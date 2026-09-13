#!/usr/bin/env python3
"""Historical OOF full-path economic replay for conditional P3 quote mapping."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import tempfile
import time
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from research.families.f02_empirical_p3_touch.audit.p3_touch_conditional_quote_mapping import (
    build_or_load_overlay,
    canonical_sha256,
    sha256_file,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_quote_path_comparison import (
    _trace_frame,
    compare_quote_frames,
)
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[4]
SPEC_SCHEMA_VERSION = "narrowgate_conditional_p3_quote_path.v1.spec"
REPORT_SCHEMA_VERSION = "narrowgate_conditional_p3_quote_path.v1"
ARMS = ("current_v2", "conditional_v4_1_oof")
SIDES = ("BUY", "SELL")


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = resolve_research_path(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    observed = sha256_file(path)
    expected = str(identity["sha256"])
    if observed != expected:
        raise ValueError(
            f"{label} hash mismatch: observed={observed} expected={expected}"
        )
    return path


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("unsupported conditional P3 quote-path spec schema")
    normalized = dict(spec)
    normalized.pop("canonical_spec_identity_sha256", None)
    if canonical_sha256(normalized) != spec.get("canonical_spec_identity_sha256"):
        raise ValueError("conditional P3 quote-path canonical spec hash mismatch")
    if tuple(spec.get("arms", ())) != ARMS:
        raise ValueError(f"conditional P3 arms must be exactly {ARMS}")
    if spec.get("only_changed_component") != "p3_curve_to_quote_mapping":
        raise ValueError("conditional P3 replay may only change the P3 quote mapping")
    mapping = spec["mapping"]
    expected_mapping = {
        "side_aggregation": "equal_opportunity_arithmetic_mean",
        "delta_star": "smallest_grid_argmax_distance_times_pair_touch_probability",
        "kappa_eff": "adjacent_grid_central_log_probability_slope",
        "invalid_or_missing_context": "fallback_current_v2_for_that_10s_bucket",
        "hold_rule": "sample_and_hold_until_next_10s_bucket",
        "parameter_search": False,
    }
    for key, value in expected_mapping.items():
        if mapping.get(key) != value:
            raise ValueError(f"conditional P3 mapping contract mismatch: {key}")
    for forbidden in (
        "independent_confirmation",
        "prediction_authority",
        "quote_mapping_authority",
        "operational_p3_replacement_authorized",
        "action_authority",
        "live_authority",
    ):
        if bool(spec["permissions"].get(forbidden, False)):
            raise ValueError(f"historical conditional P3 replay cannot grant {forbidden}")
    if not bool(spec["permissions"].get("historical_panels_previously_read")):
        raise ValueError("conditional P3 replay must acknowledge previously read panels")
    for label, identity in spec["identities"].items():
        _require_identity(identity, label)
    days: list[str] = []
    for panel in spec["panels"]:
        panel_days = [str(day) for day in panel["days"]]
        if panel_days != sorted(panel_days) or len(panel_days) != len(set(panel_days)):
            raise ValueError(f"panel {panel['role']} days must be sorted and unique")
        if bool(panel.get("independent_confirmation", False)):
            raise ValueError("conditional P3 quote-path panels are historical")
        days.extend(panel_days)
    if len(days) != len(set(days)):
        raise ValueError("conditional P3 quote-path panels overlap")
    day_inputs = spec["day_inputs"]
    if set(day_inputs) != set(days):
        raise ValueError("conditional P3 day_inputs must exactly match panel days")
    fold_artifacts = spec["fold_artifacts"]
    if set(fold_artifacts) != set(spec["fold_ids"]):
        raise ValueError("conditional P3 fold artifacts must match fold_ids")
    for fold_id, payload in fold_artifacts.items():
        for label in ("model", "calibration"):
            _require_identity(payload[label], f"{fold_id}:{label}")
    for day in days:
        payload = day_inputs[day]
        _require_identity(payload["context"], f"{day}:context")
        if payload.get("fold_id") not in spec["fold_ids"]:
            raise ValueError(f"{day} has an unsupported OOF fold")
    replay = spec["replay"]
    if replay.get("engine") != "cpp" or replay.get("queue_ahead_mode") != "exact_level":
        raise ValueError("conditional P3 economic path requires C++ exact-level replay")
    if bool(replay.get("q90_action_enabled_both_arms")):
        raise ValueError("q90 action must remain off in both arms")
    if bool(replay.get("buy_fill_selection_action_enabled_both_arms")):
        raise ValueError("BUY fill-selection action must remain off in both arms")
    return spec


def _overlay_values_at(
    overlay: Mapping[str, np.ndarray],
    timestamps: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    source_ts = np.asarray(overlay["ts_ms"], dtype=np.int64)
    target_ts = np.asarray(timestamps, dtype=np.int64)
    indices = np.searchsorted(source_ts, target_ts, side="right") - 1
    indices = np.clip(indices, 0, source_ts.size - 1)
    return (
        np.asarray(overlay["delta_star"], dtype=np.float64)[indices],
        np.asarray(overlay["kappa_eff"], dtype=np.float64)[indices],
    )


def _quote_side_metrics(
    frame: pd.DataFrame,
    *,
    overlay: Mapping[str, np.ndarray] | None,
    static_delta_star: float,
    static_kappa_eff: float,
) -> list[dict[str, Any]]:
    if frame.empty:
        frame = frame.copy()
        frame["p3_delta_star"] = pd.Series(dtype=float)
        frame["p3_kappa_eff"] = pd.Series(dtype=float)
    elif overlay is None:
        frame = frame.assign(
            p3_delta_star=float(static_delta_star),
            p3_kappa_eff=float(static_kappa_eff),
        )
    else:
        delta, kappa = _overlay_values_at(
            overlay,
            frame["quote_ts"].to_numpy(dtype=np.int64),
        )
        frame = frame.assign(p3_delta_star=delta, p3_kappa_eff=kappa)
    rows = []
    for side in (*SIDES, "POOLED"):
        group = frame if side == "POOLED" else frame[frame["side"].eq(side)]
        raw_half = group["raw_half_spread"].to_numpy(dtype=float)
        floor = group["p3_delta_star"].to_numpy(dtype=float)
        kappa = group["p3_kappa_eff"].to_numpy(dtype=float)
        rows.append(
            {
                "side": side,
                "quote_orders": int(len(group)),
                "p3_floor_binding_rate": float(
                    np.mean(np.isclose(raw_half, floor, rtol=0.0, atol=1e-8))
                    if len(group)
                    else 0.0
                ),
                "mean_raw_half_spread_usdc_per_btc": float(
                    np.mean(raw_half) if len(group) else 0.0
                ),
                "mean_p3_delta_star_usdc_per_btc": float(
                    np.mean(floor) if len(group) else 0.0
                ),
                "mean_p3_kappa_eff": float(
                    np.mean(kappa) if len(group) else 0.0
                ),
            }
        )
    return rows


def _day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import add_fill_probability_params, load_tick_base_params
    from models.data_windows import load_tick_window

    day = str(payload["day"])
    panel_role = str(payload["panel_role"])
    inputs = payload["day_inputs"]
    current_payload = json.loads(
        resolve_portable_path(
            str(payload["current_v2_p3_path"]), root=ROOT
        ).read_text(encoding="utf-8")
    )
    fallback_delta = float(current_payload["delta_star"])
    fallback_kappa = float(current_payload["kappa_eff"])
    grid_contract = payload["distance_grid"]
    fold_artifacts = payload["fold_artifacts"][str(inputs["fold_id"])]
    distance_grid = np.arange(
        float(grid_contract["minimum"]),
        float(grid_contract["maximum"]) + 0.5 * float(grid_contract["step"]),
        float(grid_contract["step"]),
        dtype=np.float64,
    )
    overlay, mapping_summary, overlay_cache_path, cache_hit = build_or_load_overlay(
        day=day,
        context_path=resolve_portable_path(
            str(inputs["context"]["path"]), root=ROOT
        ).resolve(),
        model_path=resolve_portable_path(
            str(fold_artifacts["model"]["path"]), root=ROOT
        ).resolve(),
        calibration_path=resolve_portable_path(
            str(fold_artifacts["calibration"]["path"]), root=ROOT
        ).resolve(),
        feature_contract=payload["feature_contract"],
        distance_grid=distance_grid,
        fallback_delta_star=fallback_delta,
        fallback_kappa_eff=fallback_kappa,
        mapping_contract=payload["mapping"],
        cache_dir=resolve_portable_path(
            str(payload["overlay_cache_dir"]), root=ROOT
        ).resolve(),
    )

    book_root = resolve_portable_path(str(payload["book_root"]), root=ROOT).resolve()
    feature_dir = resolve_portable_path(
        str(payload["feature_dir"]), root=ROOT
    ).resolve()
    model_dir = resolve_portable_path(str(payload["model_dir"]), root=ROOT).resolve()
    config_path = resolve_portable_path(
        str(payload["config_path"]), root=ROOT
    ).resolve()
    cache_dir = resolve_portable_path(
        str(payload["window_cache_dir"]), root=ROOT
    ).resolve()
    bt.BBO_DIR = book_root / "bbo"
    bt.L2_DIR = book_root / "l2"
    bt.configure_symbol("BTCUSDC")
    base = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
    )
    base.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "queue_ahead_mode": "exact_level",
            "queue_l2_cancel_ahead_enabled": False,
            "_formal_quality_allowed_days": [
                (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
                day,
            ],
            "collect_curves": False,
            "trace_quotes_max": int(payload["trace_quotes_max"]),
            "trace_fills_max": 0,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "sync_adjust_replay_mode": "disabled",
            "markout_side_asymmetry_sign": 1.0,
            "window_cache_write_enabled": False,
            "model_dir": str(model_dir),
            "resolved_model_dir": str(model_dir),
            "ml_enabled": True,
        }
    )
    bt.configure_symbol("BTCUSDC", model_dir_override=model_dir)
    window = load_tick_window(
        day,
        base,
        load_ml=True,
        require_ml=True,
        run_ml_inference=True,
        feature_dir=feature_dir,
        require_target_feature_files=True,
        cross_market_enabled=True,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=cache_dir,
        refresh_cache=False,
    )
    if window.book_source_authority != "native_formal_lifecycle":
        raise ValueError(
            f"{day} is not native_formal_lifecycle: {window.book_source_authority}"
        )

    daily_rows = []
    side_rows = []
    frames: dict[str, pd.DataFrame] = {}
    for arm in ARMS:
        params = dict(base)
        add_fill_probability_params(
            params,
            model_path=resolve_portable_path(
                str(payload["current_v2_p3_path"]), root=ROOT
            ).resolve(),
            label=f"P3 {arm}",
            strict=True,
        )
        arm_overlay = None
        if arm == "conditional_v4_1_oof":
            params["_conditional_p3_ts_ms"] = overlay["ts_ms"]
            params["_conditional_p3_delta_star"] = overlay["delta_star"]
            params["_conditional_p3_kappa_eff"] = overlay["kappa_eff"]
            arm_overlay = overlay
        started = time.perf_counter()
        result = bt._simulate_tick_with_engine(
            "cpp",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            params,
            ml_data=window.ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
        )
        trace = list(result.get("_quote_trace") or [])
        if len(trace) >= int(payload["trace_quotes_max"]):
            raise RuntimeError(f"quote trace limit bound on {day} {arm}: {len(trace)}")
        frame = _trace_frame(trace)
        frames[arm] = frame
        for row in _quote_side_metrics(
            frame,
            overlay=arm_overlay,
            static_delta_star=float(params["p3_delta_star"]),
            static_kappa_eff=float(params["p3_kappa_eff"]),
        ):
            side_rows.append(
                {"day": day, "panel_role": panel_role, "arm": arm, **row}
            )
        daily_rows.append(
            {
                "day": day,
                "panel_role": panel_role,
                "fold_id": str(inputs["fold_id"]),
                "arm": arm,
                "source_authority": window.book_source_authority,
                "terminal_mtm_pnl_usdc": float(result["terminal_mtm_pnl"]),
                "fills_bid": int(result["fills_bid"]),
                "fills_ask": int(result["fills_ask"]),
                "fills_total": int(result["fills_total"]),
                "n_requotes": int(result["n_requotes"]),
                "avg_markout": float(result["avg_markout"]),
                "avg_markout_bid": float(result["avg_markout_bid"]),
                "avg_markout_ask": float(result["avg_markout_ask"]),
                "final_inventory_btc": float(result["final_inventory"]),
                "max_inventory_btc": float(result["max_inventory"]),
                "abs_inventory_time_btc_s": float(result["abs_inventory_time_s"]),
                "runtime_s": float(time.perf_counter() - started),
            }
        )

    pair_rows = [
        {"day": day, "panel_role": panel_role, **row}
        for row in compare_quote_frames(
            frames["current_v2"],
            frames["conditional_v4_1_oof"],
            tick_size=float(payload["tick_size"]),
        )
    ]
    return {
        "daily": daily_rows,
        "side": side_rows,
        "pair": pair_rows,
        "mapping": {
            "day": day,
            "panel_role": panel_role,
            "fold_id": str(inputs["fold_id"]),
            "overlay_cache_path": str(overlay_cache_path),
            "overlay_cache_hit": bool(cache_hit),
            **mapping_summary,
        },
    }


def _bootstrap(values: np.ndarray, *, draws: int, seed: int) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64)
    if values.size == 0:
        raise ValueError("cannot bootstrap an empty P3 economic panel")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(int(draws), values.size), replace=True).mean(axis=1)
    return {
        "days": int(values.size),
        "sum": float(np.sum(values)),
        "mean_daily": float(np.mean(values)),
        "positive_day_rate": float(np.mean(values > 0.0)),
        "ci95_day_cluster_bootstrap": [
            float(np.quantile(sampled, 0.025)),
            float(np.quantile(sampled, 0.975)),
        ],
    }


def _panel_evidence(
    daily: pd.DataFrame,
    *,
    role: str | None,
    bootstrap_draws: int,
    seed: int,
) -> dict[str, Any]:
    subset = daily if role is None else daily[daily["panel_role"].eq(role)]
    wide = subset.pivot(index="day", columns="arm")
    pnl_delta = (
        wide["terminal_mtm_pnl_usdc"]["conditional_v4_1_oof"]
        - wide["terminal_mtm_pnl_usdc"]["current_v2"]
    ).to_numpy(dtype=float)
    control_pnl = float(wide["terminal_mtm_pnl_usdc"]["current_v2"].sum())
    candidate_pnl = float(
        wide["terminal_mtm_pnl_usdc"]["conditional_v4_1_oof"].sum()
    )
    control_fills = float(wide["fills_total"]["current_v2"].sum())
    candidate_fills = float(wide["fills_total"]["conditional_v4_1_oof"].sum())
    control_inv_time = float(wide["abs_inventory_time_btc_s"]["current_v2"].sum())
    candidate_inv_time = float(
        wide["abs_inventory_time_btc_s"]["conditional_v4_1_oof"].sum()
    )
    relative_pnl_improvement = (candidate_pnl - control_pnl) / max(abs(control_pnl), 1e-12)
    relative_fill_change = (candidate_fills - control_fills) / max(control_fills, 1.0)
    return {
        "role": "pooled" if role is None else role,
        "terminal_mtm_pnl_delta": _bootstrap(
            pnl_delta,
            draws=bootstrap_draws,
            seed=seed,
        ),
        "terminal_mtm_pnl_usdc": {
            "current_v2": control_pnl,
            "conditional_v4_1_oof": candidate_pnl,
        },
        "daily_pnl_q10_usdc": {
            arm: float(np.quantile(wide["terminal_mtm_pnl_usdc"][arm], 0.10))
            for arm in ARMS
        },
        "fills": {
            "current_v2": int(control_fills),
            "conditional_v4_1_oof": int(candidate_fills),
            "retention": float(candidate_fills / max(control_fills, 1.0)),
            "relative_change": float(relative_fill_change),
        },
        "abs_inventory_time": {
            "current_v2": control_inv_time,
            "conditional_v4_1_oof": candidate_inv_time,
            "ratio": float(candidate_inv_time / max(control_inv_time, 1e-12)),
        },
        "relative_pnl_improvement": float(relative_pnl_improvement),
        "pnl_improvement_per_absolute_fill_change": float(
            relative_pnl_improvement / max(abs(relative_fill_change), 1e-12)
        ),
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    spec_path = args.spec.expanduser().resolve()
    spec = load_spec(spec_path)
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise FileExistsError(f"conditional P3 output directory must be empty: {output_dir}")

    tasks = []
    for panel in spec["panels"]:
        for day in panel["days"]:
            tasks.append(
                {
                    "day": str(day),
                    "panel_role": str(panel["role"]),
                    "day_inputs": spec["day_inputs"][str(day)],
                    "fold_artifacts": spec["fold_artifacts"],
                    "current_v2_p3_path": spec["identities"]["current_v2_p3"]["path"],
                    "book_root": spec["paths"]["book_root"],
                    "feature_dir": spec["paths"]["feature_dir"],
                    "model_dir": spec["paths"]["model_dir"],
                    "window_cache_dir": spec["paths"]["window_cache_dir"],
                    "overlay_cache_dir": spec["paths"]["overlay_cache_dir"],
                    "config_path": spec["identities"]["operational_config"]["path"],
                    "distance_grid": spec["mapping"]["distance_grid"],
                    "feature_contract": spec["mapping"]["feature_contract"],
                    "mapping": spec["mapping"],
                    "trace_quotes_max": spec["replay"]["trace_quotes_max_per_arm_day"],
                    "tick_size": spec["replay"]["tick_size"],
                }
            )

    daily_rows: list[dict[str, Any]] = []
    side_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    mapping_rows: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=int(args.workers)) as pool:
        futures = {pool.submit(_day_task, task): task["day"] for task in tasks}
        for completed, future in enumerate(concurrent.futures.as_completed(futures), 1):
            result = future.result()
            daily_rows.extend(result["daily"])
            side_rows.extend(result["side"])
            pair_rows.extend(result["pair"])
            mapping_rows.append(result["mapping"])
            print(f"conditional P3 quote path: {completed}/{len(tasks)} days", flush=True)

    daily = pd.DataFrame(daily_rows).sort_values(["panel_role", "day", "arm"])
    side = pd.DataFrame(side_rows).sort_values(["panel_role", "day", "arm", "side"])
    pair = pd.DataFrame(pair_rows).sort_values(["panel_role", "day", "side"])
    mapping = pd.DataFrame(mapping_rows).sort_values(["panel_role", "day"])
    daily.to_csv(output_dir / "daily.csv", index=False)
    side.to_csv(output_dir / "quote_side_metrics.csv", index=False)
    pair.to_csv(output_dir / "paired_quote_coordinates.csv", index=False)
    mapping.to_csv(output_dir / "mapping_daily.csv", index=False)

    evaluation = spec["evaluation"]
    pooled = _panel_evidence(
        daily,
        role=None,
        bootstrap_draws=int(evaluation["bootstrap_draws"]),
        seed=int(evaluation["bootstrap_seed"]),
    )
    panels = {
        str(panel["role"]): _panel_evidence(
            daily,
            role=str(panel["role"]),
            bootstrap_draws=int(evaluation["bootstrap_draws"]),
            seed=int(evaluation["bootstrap_seed"]) + index + 1,
        )
        for index, panel in enumerate(spec["panels"])
    }
    q10 = pooled["daily_pnl_q10_usdc"]
    q10_margin = float(evaluation["daily_q10_relative_noninferiority_margin"])
    q10_floor = float(q10["current_v2"] - q10_margin * abs(q10["current_v2"]))
    pooled_pairs = pair[pair["side"].eq("POOLED")]
    matched_orders = pooled_pairs["matched_orders"].to_numpy(dtype=float)
    weighted_quote_change_rate = float(
        np.average(
            pooled_pairs["matched_price_change_rate"].to_numpy(dtype=float),
            weights=matched_orders,
        )
        if matched_orders.sum() > 0.0
        else 0.0
    )
    mechanics = {
        "minimum_daily_context_coverage": float(mapping["context_coverage"].min()),
        "minimum_daily_mapping_valid_fraction": float(
            mapping["mapping_valid_fraction"].min()
        ),
        "weighted_matched_quote_change_rate": weighted_quote_change_rate,
    }
    hard_gates = {
        "causal_context_coverage_at_least_95_percent_each_day": bool(
            mechanics["minimum_daily_context_coverage"]
            >= float(evaluation["minimum_daily_context_coverage"])
        ),
        "valid_mapping_coverage_at_least_95_percent_each_day": bool(
            mechanics["minimum_daily_mapping_valid_fraction"]
            >= float(evaluation["minimum_daily_mapping_valid_fraction"])
        ),
        "quote_mapping_changes_executable_coordinates": bool(
            weighted_quote_change_rate
            >= float(evaluation["minimum_matched_quote_change_rate"])
        ),
        "pooled_mean_daily_pnl_delta_positive": bool(
            pooled["terminal_mtm_pnl_delta"]["mean_daily"] > 0.0
        ),
        "both_temporal_panels_mean_daily_pnl_delta_positive": bool(
            all(
                evidence["terminal_mtm_pnl_delta"]["mean_daily"] > 0.0
                for evidence in panels.values()
            )
        ),
        "fill_retention_within_20_percent": bool(
            float(evaluation["fill_retention_minimum"])
            <= pooled["fills"]["retention"]
            <= float(evaluation["fill_retention_maximum"])
        ),
        "inventory_time_ratio_not_worse": bool(
            pooled["abs_inventory_time"]["ratio"]
            <= float(evaluation["maximum_abs_inventory_time_ratio"])
        ),
        "daily_q10_noninferior": bool(
            q10["conditional_v4_1_oof"] >= q10_floor
        ),
    }
    evidence_strength = {
        "pooled_ci95_lower_positive": bool(
            pooled["terminal_mtm_pnl_delta"]["ci95_day_cluster_bootstrap"][0] > 0.0
        ),
        "positive_day_rate_at_least_55_percent": bool(
            pooled["terminal_mtm_pnl_delta"]["positive_day_rate"]
            >= float(evaluation["minimum_positive_day_rate"])
        ),
    }
    directional_supported = bool(all(hard_gates.values()))
    decision = (
        "historical_oof_quote_mapping_economic_direction_supported_not_authorized"
        if directional_supported
        else "conditional_p3_quote_mapping_closed_historical_oof_economic_gate"
    )
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": spec["identity"],
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "implementation": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
        },
        "mapping_implementation": spec["identities"]["mapping_implementation"],
        "only_changed_component": spec["only_changed_component"],
        "days": int(daily["day"].nunique()),
        "pooled_evidence": pooled,
        "panel_evidence": panels,
        "mechanics": mechanics,
        "hard_gates": hard_gates,
        "evidence_strength": evidence_strength,
        "decision": decision,
        "permissions": spec["permissions"],
        "outputs": {
            "daily": str(output_dir / "daily.csv"),
            "quote_side_metrics": str(output_dir / "quote_side_metrics.csv"),
            "paired_quote_coordinates": str(output_dir / "paired_quote_coordinates.csv"),
            "mapping_daily": str(output_dir / "mapping_daily.csv"),
        },
    }
    _atomic_json(output_dir / "report.json", report)
    files = {
        path.name: {"size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
        for path in sorted(output_dir.iterdir())
        if path.is_file() and path.name != "manifest.json"
    }
    _atomic_json(
        output_dir / "manifest.json",
        {
            "schema_version": "narrowgate_conditional_p3_quote_path_output.v1",
            "identity": spec["identity"],
            "created_at_utc": report["created_at_utc"],
            "spec_sha256": report["spec"]["sha256"],
            "implementation_sha256": report["implementation"]["sha256"],
            "files": files,
            "permissions": spec["permissions"],
        },
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.workers <= 0:
        parser.error("--workers must be positive")
    report = run_audit(args)
    print(json.dumps({"identity": report["identity"], "decision": report["decision"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
