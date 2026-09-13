#!/usr/bin/env python3
"""Historical native full-path ML-OFF versus causal-v12 ML-ON audit.

The two historical native panels used here have already been read by earlier
research.  This audit can therefore diagnose economic transport, but it can
never create independent confirmation, action authority, or live authority.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from models.prewarm_tick_cache import _quality_authorities
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "causal_v12_native_full_path_ml_ab.v1"
ARMS = ("ml_off", "ml_on")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_identity_sha256", None)
    return canonical_sha256(normalized)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


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
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported causal-v12 full-path A/B schema")
    if canonical_spec_sha256(spec) != spec.get("canonical_spec_identity_sha256"):
        raise ValueError("causal-v12 full-path A/B canonical spec hash mismatch")
    if sha256_file(Path(__file__).resolve()) != spec["implementation_sha256"]:
        raise ValueError("causal-v12 full-path A/B implementation hash mismatch")

    permissions = spec["permissions"]
    if not bool(permissions.get("historical_panels_previously_read", False)):
        raise ValueError("the two native panels must be marked previously read")
    for forbidden in (
        "independent_confirmation",
        "prediction_authority",
        "action_authority",
        "live_authority",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"historical A/B cannot grant {forbidden}")

    if tuple(spec["arms"]) != ARMS:
        raise ValueError(f"arms must be exactly {ARMS}")
    panels = spec["panels"]
    all_days: list[str] = []
    for panel in panels:
        days = [str(day) for day in panel["days"]]
        if days != sorted(days) or len(days) != len(set(days)):
            raise ValueError(f"panel {panel['role']} days are not unique/ordered")
        if bool(panel.get("independent_confirmation", False)):
            raise ValueError("historical native panel cannot be confirmation")
        all_days.extend(days)
    if len(all_days) != len(set(all_days)):
        raise ValueError("native A/B panels overlap")

    for label, identity in spec["identities"].items():
        _require_identity(identity, label)
    return spec


def storage_gate(output_dir: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    gate = spec["storage_gate"]
    probe = output_dir.parent if output_dir.parent.exists() else ROOT
    usage = shutil.disk_usage(probe)
    free_gib = float(usage.free) / (1024.0**3)
    estimated = float(gate["estimated_new_output_gib"])
    required = max(
        float(gate["absolute_minimum_free_gib"]),
        float(gate["reserve_free_gib"])
        + float(gate["output_multiple"]) * estimated,
    )
    if free_gib < required:
        raise RuntimeError(
            f"storage gate failed: free={free_gib:.2f} GiB required={required:.2f} GiB"
        )
    return {
        "free_gib_before": free_gib,
        "estimated_new_output_gib": estimated,
        "required_free_gib": required,
        "passed": True,
    }


def _fee_usdc(row: Mapping[str, Any]) -> float:
    quantity = float(row["fill_qty"])
    markout = float(row["markout_30s"])
    value = float(row["ev_30s"])
    return quantity * (markout - value)


def reconstruct_campaigns(
    fill_trace: Sequence[Mapping[str, Any]],
    *,
    day: str,
    panel_role: str,
    arm: str,
    terminal_mark_price: float,
    order_size: float,
    inventory_tolerance: float = 1e-9,
) -> list[dict[str, Any]]:
    """Reconstruct flat-to-flat campaigns and one day-end MTM campaign."""

    active: dict[str, Any] | None = None
    campaigns: list[dict[str, Any]] = []
    rows = sorted(fill_trace, key=lambda row: int(row["fill_ts"]))
    previous_after = 0.0

    for fill_index, row in enumerate(rows):
        before = float(row["inventory_before_fill"])
        after = float(row["inventory_after_fill"])
        if abs(before - previous_after) > inventory_tolerance:
            raise ValueError(
                f"inventory trace discontinuity on {day} {arm} fill {fill_index}: "
                f"before={before} previous_after={previous_after}"
            )
        quantity = float(row["fill_qty"])
        price = float(row["quote_px"])
        if quantity <= 0.0 or price <= 0.0:
            raise ValueError("fill trace contains non-positive quantity or price")
        side = str(row["side"]).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported fill side: {side}")
        fee_usdc = _fee_usdc(row)
        cash_delta = (quantity * price if side == "SELL" else -quantity * price)
        cash_delta -= fee_usdc

        if active is None:
            if abs(before) > inventory_tolerance or abs(after) <= inventory_tolerance:
                raise ValueError("campaign must start with a flat-to-nonflat fill")
            active = {
                "day": day,
                "panel_role": panel_role,
                "arm": arm,
                "campaign_index": len(campaigns),
                "campaign_id": f"{day}:{arm}:{len(campaigns)}",
                "inventory_side": "LONG" if after > 0.0 else "SHORT",
                "start_ts_ms": int(row["fill_ts"]),
                "end_ts_ms": int(row["fill_ts"]),
                "cash_usdc": 0.0,
                "fees_usdc": 0.0,
                "fill_count": 0,
                "filled_quantity_btc": 0.0,
                "max_abs_inventory_btc": 0.0,
            }

        active["end_ts_ms"] = int(row["fill_ts"])
        active["cash_usdc"] += cash_delta
        active["fees_usdc"] += fee_usdc
        active["fill_count"] += 1
        active["filled_quantity_btc"] += quantity
        active["max_abs_inventory_btc"] = max(
            float(active["max_abs_inventory_btc"]), abs(after)
        )

        if abs(after) <= inventory_tolerance:
            active["closed"] = True
            active["terminal_reason"] = "flat_fill"
            active["terminal_inventory_btc"] = 0.0
            active["terminal_mark_price"] = math.nan
            active["terminal_value_usdc"] = float(active["cash_usdc"])
            active["duration_s"] = (
                int(active["end_ts_ms"]) - int(active["start_ts_ms"])
            ) / 1000.0
            active["max_inventory_units"] = float(
                active["max_abs_inventory_btc"]
            ) / order_size
            active["multi_level"] = bool(
                float(active["max_abs_inventory_btc"])
                >= 2.0 * order_size - inventory_tolerance
            )
            campaigns.append(active)
            active = None
        previous_after = after

    if active is not None:
        active["closed"] = False
        active["terminal_reason"] = "day_end_mtm"
        active["terminal_inventory_btc"] = previous_after
        active["terminal_mark_price"] = float(terminal_mark_price)
        active["terminal_value_usdc"] = float(active["cash_usdc"])
        active["terminal_value_usdc"] += previous_after * terminal_mark_price
        active["duration_s"] = (
            int(active["end_ts_ms"]) - int(active["start_ts_ms"])
        ) / 1000.0
        active["max_inventory_units"] = float(
            active["max_abs_inventory_btc"]
        ) / order_size
        active["multi_level"] = bool(
            float(active["max_abs_inventory_btc"])
            >= 2.0 * order_size - inventory_tolerance
        )
        campaigns.append(active)

    return campaigns


def _campaign_day_metrics(campaigns: pd.DataFrame) -> dict[str, float | int]:
    if campaigns.empty:
        return {
            "campaign_count": 0,
            "campaign_closed_count": 0,
            "campaign_terminal_value_usdc": 0.0,
            "campaign_q10_usdc": 0.0,
            "campaign_cvar10_usdc": 0.0,
            "multi_level_long_terminal_value_usdc": 0.0,
            "multi_level_short_terminal_value_usdc": 0.0,
            "multi_level_long_negative_value_usdc": 0.0,
            "multi_level_short_negative_value_usdc": 0.0,
        }
    value = campaigns["terminal_value_usdc"].to_numpy(dtype=float)
    q10 = float(np.quantile(value, 0.10))
    cvar = float(np.mean(value[value <= q10]))
    multi = campaigns[campaigns["multi_level"].astype(bool)]
    long_value = multi.loc[
        multi["inventory_side"].eq("LONG"), "terminal_value_usdc"
    ].to_numpy(dtype=float)
    short_value = multi.loc[
        multi["inventory_side"].eq("SHORT"), "terminal_value_usdc"
    ].to_numpy(dtype=float)
    return {
        "campaign_count": int(len(campaigns)),
        "campaign_closed_count": int(campaigns["closed"].astype(bool).sum()),
        "campaign_terminal_value_usdc": float(np.sum(value)),
        "campaign_q10_usdc": q10,
        "campaign_cvar10_usdc": cvar,
        "multi_level_long_terminal_value_usdc": float(np.sum(long_value)),
        "multi_level_short_terminal_value_usdc": float(np.sum(short_value)),
        "multi_level_long_negative_value_usdc": float(
            np.sum(long_value[long_value < 0.0])
        ),
        "multi_level_short_negative_value_usdc": float(
            np.sum(short_value[short_value < 0.0])
        ),
    }


def _side_trace_metrics(fill_trace: Sequence[Mapping[str, Any]], side: str) -> dict[str, float]:
    rows = [row for row in fill_trace if str(row["side"]).upper() == side]
    if not rows:
        return {
            "fill_qty_btc": 0.0,
            "fill_notional_usdc": 0.0,
            "maker_value_30s_usdc": 0.0,
            "maker_value_30s_bps": 0.0,
        }
    quantity = np.asarray([float(row["fill_qty"]) for row in rows])
    price = np.asarray([float(row["quote_px"]) for row in rows])
    value_per_btc = np.asarray([float(row["ev_30s"]) for row in rows])
    notional = float(np.sum(quantity * price))
    value = float(np.sum(quantity * value_per_btc))
    return {
        "fill_qty_btc": float(np.sum(quantity)),
        "fill_notional_usdc": notional,
        "maker_value_30s_usdc": value,
        "maker_value_30s_bps": 1e4 * value / notional if notional > 0.0 else 0.0,
    }


def _day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import disable_ml_params, load_tick_base_params
    from models.data_windows import load_tick_window

    day = str(payload["day"])
    panel_role = str(payload["panel_role"])
    book_root = resolve_portable_path(str(payload["book_root"]), root=ROOT).resolve()
    cache_dir = resolve_portable_path(str(payload["cache_dir"]), root=ROOT).resolve()
    feature_dir = resolve_portable_path(
        str(payload["feature_dir"]), root=ROOT
    ).resolve()
    model_dir = resolve_portable_path(str(payload["model_dir"]), root=ROOT).resolve()
    config_path = resolve_portable_path(
        str(payload["config_path"]), root=ROOT
    ).resolve()
    trace_fills_max = int(payload["trace_fills_max"])

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
            "trace_fills_max": trace_fills_max,
            "trace_fills_window_s": 30.0,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "sync_adjust_replay_mode": "disabled",
            "markout_side_asymmetry_sign": 1.0,
            "window_cache_write_enabled": bool(
                payload["window_cache_write_enabled"]
            ),
            "model_dir": str(model_dir),
            "resolved_model_dir": str(model_dir),
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

    daily_rows: list[dict[str, Any]] = []
    campaign_rows: list[dict[str, Any]] = []
    for arm in ARMS:
        params = dict(base)
        if arm == "ml_off":
            params["ml_enabled"] = False
            disable_ml_params(params)
            ml_data = None
        else:
            params["ml_enabled"] = True
            ml_data = window.ml_data

        started = time.perf_counter()
        result = bt._simulate_tick_with_engine(
            "cpp",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            params,
            ml_data=ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
        )
        fill_trace = list(result.get("_fill_trace") or [])
        if len(fill_trace) != int(result["fills_total"]):
            raise RuntimeError(
                f"fill trace truncated on {day} {arm}: "
                f"trace={len(fill_trace)} fills={result['fills_total']}"
            )
        campaigns = reconstruct_campaigns(
            fill_trace,
            day=day,
            panel_role=panel_role,
            arm=arm,
            terminal_mark_price=float(result["terminal_mark_price"]),
            order_size=float(params["order_size"]),
        )
        campaign_frame = pd.DataFrame(campaigns)
        campaign_metrics = _campaign_day_metrics(campaign_frame)
        accounting_error = (
            float(campaign_metrics["campaign_terminal_value_usdc"])
            - float(result["terminal_mtm_pnl"])
        )
        if abs(accounting_error) > 1e-6:
            raise RuntimeError(
                f"campaign accounting mismatch on {day} {arm}: {accounting_error}"
            )
        buy = _side_trace_metrics(fill_trace, "BUY")
        sell = _side_trace_metrics(fill_trace, "SELL")
        daily_rows.append(
            {
                "day": day,
                "panel_role": panel_role,
                "arm": arm,
                "source_authority": window.book_source_authority,
                "pnl_usdc": float(result["pnl"]),
                "terminal_mtm_pnl_usdc": float(result["terminal_mtm_pnl"]),
                "inventory_adjusted_pnl_usdc": float(
                    result["inventory_adjusted_pnl"]
                ),
                "fills_bid": int(result["fills_bid"]),
                "fills_ask": int(result["fills_ask"]),
                "fills_total": int(result["fills_total"]),
                "abs_inventory_time_btc_s": float(result["abs_inventory_time_s"]),
                "max_inventory_btc": float(result["max_inventory"]),
                "final_inventory_btc": float(result["final_inventory"]),
                "buy_markout_10s_price": float(result["avg_markout_bid"]),
                "sell_markout_10s_price": float(result["avg_markout_ask"]),
                "buy_maker_value_30s_bps": buy["maker_value_30s_bps"],
                "sell_maker_value_30s_bps": sell["maker_value_30s_bps"],
                "buy_maker_value_30s_usdc": buy["maker_value_30s_usdc"],
                "sell_maker_value_30s_usdc": sell["maker_value_30s_usdc"],
                "campaign_accounting_error_usdc": accounting_error,
                "runtime_s": time.perf_counter() - started,
                **campaign_metrics,
            }
        )
        campaign_rows.extend(campaigns)

    return {"daily": daily_rows, "campaigns": campaign_rows}


def _bootstrap_delta(
    values: np.ndarray,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    if values.ndim != 1 or len(values) == 0:
        raise ValueError("paired bootstrap needs a non-empty one-dimensional vector")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(draws, len(values)), replace=True).mean(axis=1)
    return {
        "days": int(len(values)),
        "mean_daily_delta": float(np.mean(values)),
        "sum_delta": float(np.sum(values)),
        "median_daily_delta": float(np.median(values)),
        "positive_day_rate": float(np.mean(values > 0.0)),
        "ci95_day_cluster_bootstrap": [
            float(np.quantile(sampled, 0.025)),
            float(np.quantile(sampled, 0.975)),
        ],
    }


def _paired_metric(
    daily: pd.DataFrame,
    metric: str,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    wide = daily.pivot(index="day", columns="arm", values=metric).dropna()
    if list(wide.columns.sort_values()) != ["ml_off", "ml_on"]:
        raise ValueError(f"metric {metric} lacks paired arms")
    delta = (
        wide["ml_on"].to_numpy(dtype=float)
        - wide["ml_off"].to_numpy(dtype=float)
    )
    return _bootstrap_delta(delta, draws=draws, seed=seed)


def panel_evidence(
    daily: pd.DataFrame,
    campaigns: pd.DataFrame,
    *,
    gates: Mapping[str, Any],
    bootstrap_draws: int,
    bootstrap_seed: int,
) -> dict[str, Any]:
    metrics = (
        "terminal_mtm_pnl_usdc",
        "pnl_usdc",
        "fills_total",
        "abs_inventory_time_btc_s",
        "buy_maker_value_30s_bps",
        "sell_maker_value_30s_bps",
        "campaign_q10_usdc",
        "campaign_cvar10_usdc",
        "multi_level_long_terminal_value_usdc",
        "multi_level_short_terminal_value_usdc",
        "multi_level_long_negative_value_usdc",
        "multi_level_short_negative_value_usdc",
    )
    paired = {
        metric: _paired_metric(
            daily,
            metric,
            draws=bootstrap_draws,
            seed=bootstrap_seed + index,
        )
        for index, metric in enumerate(metrics)
    }
    totals = daily.groupby("arm", sort=True).sum(numeric_only=True)
    fill_retention = float(
        totals.loc["ml_on", "fills_total"]
        / max(float(totals.loc["ml_off", "fills_total"]), 1.0)
    )
    inventory_time_ratio = float(
        totals.loc["ml_on", "abs_inventory_time_btc_s"]
        / max(float(totals.loc["ml_off", "abs_inventory_time_btc_s"]), 1e-12)
    )
    primary = paired["terminal_mtm_pnl_usdc"]
    gate_results = {
        "primary_pnl_lcb_positive": bool(
            primary["ci95_day_cluster_bootstrap"][0] > 0.0
        ),
        "fill_retention": bool(
            fill_retention >= float(gates["minimum_fill_retention"])
        ),
        "inventory_time_nonworse": bool(
            inventory_time_ratio <= float(gates["maximum_inventory_time_ratio"])
        ),
        "campaign_q10_nonworse": bool(
            paired["campaign_q10_usdc"]["mean_daily_delta"] >= 0.0
        ),
        "campaign_cvar10_nonworse": bool(
            paired["campaign_cvar10_usdc"]["mean_daily_delta"] >= 0.0
        ),
        "buy_maker_value_nonworse": bool(
            paired["buy_maker_value_30s_bps"]["mean_daily_delta"]
            >= -float(gates["side_maker_value_tolerance_bps"])
        ),
        "sell_maker_value_nonworse": bool(
            paired["sell_maker_value_30s_bps"]["mean_daily_delta"]
            >= -float(gates["side_maker_value_tolerance_bps"])
        ),
    }
    all_passed = all(gate_results.values())
    primary_delta = daily.pivot(
        index="day", columns="arm", values="terminal_mtm_pnl_usdc"
    )
    primary_delta = (
        primary_delta["ml_on"] - primary_delta["ml_off"]
    ).to_numpy(dtype=float)
    sample_std = float(np.std(primary_delta, ddof=1)) if len(primary_delta) > 1 else 0.0
    ranking_score = (
        float(np.mean(primary_delta) / max(sample_std, 1e-12))
        if all_passed
        else None
    )

    campaign_summary: dict[str, Any] = {}
    for arm, group in campaigns.groupby("arm", sort=True):
        value = group["terminal_value_usdc"].to_numpy(dtype=float)
        q10 = float(np.quantile(value, 0.10)) if len(value) else 0.0
        campaign_summary[str(arm)] = {
            "campaigns": int(len(group)),
            "closed_campaigns": int(group["closed"].astype(bool).sum()),
            "terminal_value_usdc": float(np.sum(value)),
            "mean_terminal_value_usdc": float(np.mean(value)) if len(value) else 0.0,
            "q10_usdc": q10,
            "cvar10_usdc": (
                float(np.mean(value[value <= q10])) if len(value) else 0.0
            ),
            "multi_level_long_terminal_value_usdc": float(
                group.loc[
                    group["multi_level"].astype(bool)
                    & group["inventory_side"].eq("LONG"),
                    "terminal_value_usdc",
                ].sum()
            ),
            "multi_level_short_terminal_value_usdc": float(
                group.loc[
                    group["multi_level"].astype(bool)
                    & group["inventory_side"].eq("SHORT"),
                    "terminal_value_usdc",
                ].sum()
            ),
        }

    return {
        "comparison": "ml_on_minus_ml_off",
        "cluster_unit": "UTC_day",
        "bootstrap_draws": int(bootstrap_draws),
        "bootstrap_seed": int(bootstrap_seed),
        "days": int(daily["day"].nunique()),
        "paired_metrics": paired,
        "fill_retention": fill_retention,
        "inventory_time_ratio": inventory_time_ratio,
        "campaign_summary": campaign_summary,
        "hard_gates": gate_results,
        "all_hard_gates_passed": all_passed,
        "ranking_score": ranking_score,
    }


def run(spec: Mapping[str, Any], output_dir: Path, *, workers: int) -> dict[str, Any]:
    if workers not in {1, 2}:
        raise ValueError("workers must be 1 or 2")
    identities = spec["identities"]
    book_root = resolve_portable_path(
        str(spec["paths"]["book_root"]), root=ROOT
    ).resolve()
    feature_dir = resolve_portable_path(
        str(spec["paths"]["feature_dir"]), root=ROOT
    ).resolve()
    model_dir = resolve_portable_path(
        str(spec["paths"]["model_dir"]), root=ROOT
    ).resolve()
    config_path = resolve_portable_path(
        str(identities["operational_config"]["path"]), root=ROOT
    ).resolve()
    cache_dir = resolve_portable_path(
        str(spec["paths"]["cache_dir"]), root=ROOT
    ).resolve()
    output_dir = output_dir.expanduser().resolve()
    storage = storage_gate(output_dir, spec)

    authorities = _quality_authorities(book_root)
    payloads: list[dict[str, Any]] = []
    for panel in spec["panels"]:
        for day in panel["days"]:
            if authorities.get(day) != "native_formal_lifecycle":
                raise ValueError(
                    f"{day} source authority is not native_formal_lifecycle: "
                    f"{authorities.get(day)}"
                )
            payloads.append(
                {
                    "day": day,
                    "panel_role": panel["role"],
                    "book_root": str(book_root),
                    "cache_dir": str(cache_dir),
                    "feature_dir": str(feature_dir),
                    "model_dir": str(model_dir),
                    "config_path": str(config_path),
                    "trace_fills_max": int(spec["replay"]["trace_fills_max_per_arm_day"]),
                    "window_cache_write_enabled": bool(
                        spec["replay"]["window_cache_write_enabled"]
                    ),
                }
            )

    if workers == 1:
        results = [_day_task(payload) for payload in payloads]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_day_task, payloads))

    daily = pd.DataFrame(
        [row for result in results for row in result["daily"]]
    ).sort_values(["panel_role", "day", "arm"])
    campaigns = pd.DataFrame(
        [row for result in results for row in result["campaigns"]]
    ).sort_values(["panel_role", "day", "arm", "campaign_index"])
    expected_rows = 2 * sum(len(panel["days"]) for panel in spec["panels"])
    if len(daily) != expected_rows:
        raise RuntimeError(f"daily denominator mismatch: {len(daily)} != {expected_rows}")
    if daily["campaign_accounting_error_usdc"].abs().max() > 1e-6:
        raise RuntimeError("campaign accounting identity failed")

    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / "daily.csv"
    campaigns_path = output_dir / "campaigns.parquet"
    daily.to_csv(daily_path, index=False)
    campaigns.to_parquet(campaigns_path, index=False)

    panel_results: dict[str, Any] = {}
    for index, panel in enumerate(spec["panels"]):
        role = str(panel["role"])
        panel_results[role] = panel_evidence(
            daily[daily["panel_role"].eq(role)].copy(),
            campaigns[campaigns["panel_role"].eq(role)].copy(),
            gates=spec["gates"],
            bootstrap_draws=int(spec["bootstrap"]["draws"]),
            bootstrap_seed=int(spec["bootstrap"]["seed"]) + 100 * index,
        )
        panel_results[role]["independent_confirmation"] = False

    all_panel_gates = all(
        bool(result["all_hard_gates_passed"])
        for result in panel_results.values()
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": spec["experiment_id"],
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": (
            "historical_native_economic_screen_passed_without_confirmation_authority"
            if all_panel_gates
            else "close_causal_v12_economic_screen_on_historical_native_panels"
        ),
        "panel_results": panel_results,
        "all_historical_panel_hard_gates_passed": all_panel_gates,
        "ranking_score": (
            min(
                float(result["ranking_score"])
                for result in panel_results.values()
                if result["ranking_score"] is not None
            )
            if all_panel_gates
            else None
        ),
        "transport_context": {
            "ranking_transport_observed": True,
            "absolute_probability_or_scale_transport_supported": False,
            "native_transport_report": identities["native_transport_report"],
        },
        "permissions": dict(spec["permissions"]),
        "storage_gate": storage,
        "value_provenance": spec["value_provenance"],
    }
    report_path = output_dir / "report.json"
    _atomic_json(report_path, report)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.manifest",
        "experiment_id": spec["experiment_id"],
        "spec_identity_sha256": spec["canonical_spec_identity_sha256"],
        "implementation_sha256": spec["implementation_sha256"],
        "daily": {"path": str(daily_path), "sha256": sha256_file(daily_path)},
        "campaigns": {
            "path": str(campaigns_path),
            "sha256": sha256_file(campaigns_path),
            "rows": int(len(campaigns)),
        },
        "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        "identities": identities,
        "permissions": dict(spec["permissions"]),
    }
    _atomic_json(output_dir / "manifest.json", manifest)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    spec = load_spec(args.spec.expanduser().resolve())
    report = run(spec, args.output_dir, workers=args.workers)
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
