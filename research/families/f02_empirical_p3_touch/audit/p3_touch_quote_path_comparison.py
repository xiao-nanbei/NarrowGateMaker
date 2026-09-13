#!/usr/bin/env python3
"""Historical full-path current-v2 versus expanded-v3 P3 comparison.

The calibration gate for expanded v3 is already closed. This audit therefore
measures quote-path blast radius only; it cannot rescue the candidate or grant
prediction, action, operational, or live authority.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import tempfile
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[4]
SPEC_SCHEMA_VERSION = "narrowgate_p3_touch_quote_path_comparison.v1.spec"
REPORT_SCHEMA_VERSION = "narrowgate_p3_touch_quote_path_comparison.v1"
ARMS = ("current_v2", "expanded_v3")
SIDES = ("BUY", "SELL")


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
        raise ValueError("unsupported P3 quote-path spec schema")
    normalized = dict(spec)
    normalized.pop("canonical_spec_identity_sha256", None)
    if canonical_sha256(normalized) != spec.get("canonical_spec_identity_sha256"):
        raise ValueError("P3 quote-path canonical spec hash mismatch")
    if tuple(spec.get("arms", ())) != ARMS:
        raise ValueError(f"P3 quote-path arms must be exactly {ARMS}")
    if spec.get("only_changed_input") != "empirical_p3_touch_artifact":
        raise ValueError("P3 quote-path audit may only change the P3 artifact")
    if not bool(spec["decision_contract"].get("calibration_gate_already_failed")):
        raise ValueError("quote-path diagnostic requires the failed calibration gate")
    if bool(spec["decision_contract"].get("quote_path_can_rescue_static_successor")):
        raise ValueError("quote-path evidence cannot rescue expanded v3")
    for forbidden in (
        "independent_confirmation",
        "prediction_authority",
        "action_authority",
        "live_authority",
        "operational_p3_replacement_authorized",
    ):
        if bool(spec["permissions"].get(forbidden, False)):
            raise ValueError(f"P3 quote-path audit cannot grant {forbidden}")
    all_days: list[str] = []
    for panel in spec["panels"]:
        days = [str(day) for day in panel["days"]]
        if days != sorted(days) or len(days) != len(set(days)):
            raise ValueError(f"panel {panel['role']} days must be ordered and unique")
        if bool(panel.get("independent_confirmation", False)):
            raise ValueError("quote-path panels are historical diagnostics")
        all_days.extend(days)
    if len(all_days) != len(set(all_days)):
        raise ValueError("P3 quote-path panels overlap")
    for label, identity in spec["identities"].items():
        _require_identity(identity, label)
    calibration = json.loads(
        resolve_research_path(
            str(spec["identities"]["calibration_report"]["path"])
        ).read_text(encoding="utf-8")
    )
    if bool(calibration["calibration_gate_before_quote_path"]["all_passed"]):
        raise ValueError("bound calibration report no longer records a failed gate")
    return spec


def _normalize_side(value: Any) -> str:
    text = str(value).upper()
    if "BUY" in text or text.endswith("BID"):
        return "BUY"
    if "SELL" in text or text.endswith("ASK"):
        return "SELL"
    if text in {"0", "SIDE.BUY"}:
        return "BUY"
    if text in {"1", "SIDE.SELL"}:
        return "SELL"
    raise ValueError(f"unsupported quote-trace side: {value!r}")


def _enum_text(value: Any) -> str:
    text = str(value)
    return text.split(".")[-1]


def _trace_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    required = (
        "order_id",
        "side",
        "quote_ts",
        "final_price",
        "raw_half_spread",
        "final_pair_spread",
        "outcome",
        "cancel_reason",
    )
    frame = pd.DataFrame(
        [{name: row.get(name) for name in required} for row in rows]
    )
    if frame.empty:
        return frame.assign(ordinal=pd.Series(dtype=np.int64))
    frame["side"] = frame["side"].map(_normalize_side)
    frame["quote_ts"] = pd.to_numeric(frame["quote_ts"], errors="raise").astype(
        np.int64
    )
    frame["order_id"] = pd.to_numeric(frame["order_id"], errors="raise").astype(
        np.int64
    )
    for field in ("final_price", "raw_half_spread", "final_pair_spread"):
        frame[field] = pd.to_numeric(frame[field], errors="raise").astype(float)
    frame["outcome"] = frame["outcome"].map(_enum_text)
    frame["cancel_reason"] = frame["cancel_reason"].map(_enum_text)
    frame.sort_values(["quote_ts", "side", "order_id"], inplace=True)
    frame["ordinal"] = frame.groupby(["quote_ts", "side"], sort=False).cumcount()
    return frame


def compare_quote_frames(
    current: pd.DataFrame,
    expanded: pd.DataFrame,
    *,
    tick_size: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for side in (*SIDES, "POOLED"):
        left = current if side == "POOLED" else current[current["side"].eq(side)]
        right = expanded if side == "POOLED" else expanded[expanded["side"].eq(side)]
        keys = ["quote_ts", "side", "ordinal"]
        merged = left[keys + ["final_price"]].merge(
            right[keys + ["final_price"]],
            on=keys,
            how="outer",
            suffixes=("_current", "_expanded"),
            indicator=True,
        )
        matched = merged[merged["_merge"].eq("both")].copy()
        ticks = (
            matched["final_price_expanded"] - matched["final_price_current"]
        ) / float(tick_size)
        abs_ticks = np.abs(ticks.to_numpy(dtype=float))
        signed_ticks = ticks.to_numpy(dtype=float)
        union = len(merged)
        rows.append(
            {
                "side": side,
                "current_orders": int(len(left)),
                "expanded_orders": int(len(right)),
                "union_orders": int(union),
                "matched_orders": int(len(matched)),
                "current_only_orders": int((merged["_merge"] == "left_only").sum()),
                "expanded_only_orders": int((merged["_merge"] == "right_only").sum()),
                "matched_fraction_of_union": float(len(matched) / max(union, 1)),
                "matched_price_change_rate": float(
                    np.mean(abs_ticks > 0.5) if abs_ticks.size else 0.0
                ),
                "mean_signed_price_change_ticks": float(
                    np.mean(signed_ticks) if signed_ticks.size else 0.0
                ),
                "mean_abs_price_change_ticks": float(
                    np.mean(abs_ticks) if abs_ticks.size else 0.0
                ),
                "p50_abs_price_change_ticks": float(
                    np.quantile(abs_ticks, 0.50) if abs_ticks.size else 0.0
                ),
                "p90_abs_price_change_ticks": float(
                    np.quantile(abs_ticks, 0.90) if abs_ticks.size else 0.0
                ),
                "max_abs_price_change_ticks": float(
                    np.max(abs_ticks) if abs_ticks.size else 0.0
                ),
            }
        )
    return rows


def _arm_trace_metrics(
    frame: pd.DataFrame,
    *,
    delta_star: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summaries: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    for side in (*SIDES, "POOLED"):
        group = frame if side == "POOLED" else frame[frame["side"].eq(side)]
        raw_half = group["raw_half_spread"].to_numpy(dtype=float)
        final_pair = group["final_pair_spread"].to_numpy(dtype=float)
        floor_binding = np.isclose(
            raw_half,
            float(delta_star),
            rtol=0.0,
            atol=1e-8,
        )
        summaries.append(
            {
                "side": side,
                "quote_orders": int(len(group)),
                "p3_floor_binding_rate": float(
                    np.mean(floor_binding) if len(group) else 0.0
                ),
                "mean_raw_half_spread_usdc_per_btc": float(
                    np.mean(raw_half) if len(group) else 0.0
                ),
                "mean_final_pair_spread_usdc_per_btc": float(
                    np.mean(final_pair) if len(group) else 0.0
                ),
            }
        )
        for field in ("outcome", "cancel_reason"):
            for value, count in Counter(group[field].tolist()).items():
                event_rows.append(
                    {
                        "side": side,
                        "field": field,
                        "value": str(value),
                        "count": int(count),
                    }
                )
    return summaries, event_rows


def _day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import (
        add_fill_probability_params,
        load_tick_base_params,
    )
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
    trace_quotes_max = int(payload["trace_quotes_max"])
    tick_size = float(payload["tick_size"])

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
            "trace_quotes_max": trace_quotes_max,
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

    arm_results: dict[str, dict[str, Any]] = {}
    frames: dict[str, pd.DataFrame] = {}
    event_rows: list[dict[str, Any]] = []
    side_rows: list[dict[str, Any]] = []
    for arm in ARMS:
        params = dict(base)
        p3_path = resolve_portable_path(
            str(payload[f"{arm}_p3_path"]), root=ROOT
        ).resolve()
        add_fill_probability_params(
            params,
            model_path=p3_path,
            label=f"P3 {arm}",
            strict=True,
        )
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
        if len(trace) >= trace_quotes_max:
            raise RuntimeError(
                f"quote trace limit bound on {day} {arm}: {len(trace)}"
            )
        frame = _trace_frame(trace)
        frames[arm] = frame
        summaries, counts = _arm_trace_metrics(
            frame,
            delta_star=float(params["p3_delta_star"]),
        )
        for row in summaries:
            side_rows.append(
                {"day": day, "panel_role": panel_role, "arm": arm, **row}
            )
        for row in counts:
            event_rows.append(
                {"day": day, "panel_role": panel_role, "arm": arm, **row}
            )
        arm_results[arm] = {
            "day": day,
            "panel_role": panel_role,
            "arm": arm,
            "source_authority": window.book_source_authority,
            "p3_artifact_sha256": str(
                params["fill_probability_artifact_sha256"]
            ),
            "p3_delta_star": float(params["p3_delta_star"]),
            "p3_kappa_eff": float(params["p3_kappa_eff"]),
            "terminal_mtm_pnl_usdc": float(result["terminal_mtm_pnl"]),
            "fills_bid": int(result["fills_bid"]),
            "fills_ask": int(result["fills_ask"]),
            "fills_total": int(result["fills_total"]),
            "n_requotes": int(result["n_requotes"]),
            "final_inventory_btc": float(result["final_inventory"]),
            "max_inventory_btc": float(result["max_inventory"]),
            "abs_inventory_time_btc_s": float(result["abs_inventory_time_s"]),
            "quote_trace_rows": int(len(frame)),
            "runtime_s": float(time.perf_counter() - started),
        }

    pair_rows = compare_quote_frames(
        frames["current_v2"], frames["expanded_v3"], tick_size=tick_size
    )
    return {
        "daily": list(arm_results.values()),
        "side": side_rows,
        "events": event_rows,
        "pair": [
            {"day": day, "panel_role": panel_role, **row} for row in pair_rows
        ],
    }


def _bootstrap_delta(
    values: np.ndarray,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64)
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(draws, values.size), replace=True).mean(axis=1)
    return {
        "days": int(values.size),
        "sum_delta": float(np.sum(values)),
        "mean_daily_delta": float(np.mean(values)),
        "positive_day_rate": float(np.mean(values > 0.0)),
        "ci95_day_cluster_bootstrap": [
            float(np.quantile(sampled, 0.025)),
            float(np.quantile(sampled, 0.975)),
        ],
    }


def _paired_metric(
    daily: pd.DataFrame,
    panel_role: str,
    metric: str,
    *,
    seed: int,
) -> dict[str, Any]:
    subset = daily[daily["panel_role"].eq(panel_role)]
    wide = subset.pivot(index="day", columns="arm", values=metric).dropna()
    delta = (
        wide["expanded_v3"].to_numpy(dtype=float)
        - wide["current_v2"].to_numpy(dtype=float)
    )
    return _bootstrap_delta(delta, draws=20000, seed=seed)


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
        raise FileExistsError(f"quote-path output directory must be empty: {output_dir}")

    tasks = []
    for panel in spec["panels"]:
        for day in panel["days"]:
            tasks.append(
                {
                    "day": str(day),
                    "panel_role": str(panel["role"]),
                    "book_root": spec["paths"]["book_root"],
                    "cache_dir": spec["paths"]["cache_dir"],
                    "feature_dir": spec["paths"]["feature_dir"],
                    "model_dir": spec["paths"]["model_dir"],
                    "config_path": spec["identities"]["operational_config"]["path"],
                    "current_v2_p3_path": spec["identities"]["current_v2_p3"]["path"],
                    "expanded_v3_p3_path": spec["identities"]["expanded_v3_p3"]["path"],
                    "trace_quotes_max": int(
                        spec["replay"]["trace_quotes_max_per_arm_day"]
                    ),
                    "tick_size": float(spec["replay"]["tick_size"]),
                }
            )

    daily_rows: list[dict[str, Any]] = []
    side_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=int(args.workers)) as pool:
        futures = {pool.submit(_day_task, task): task["day"] for task in tasks}
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            daily_rows.extend(result["daily"])
            side_rows.extend(result["side"])
            event_rows.extend(result["events"])
            pair_rows.extend(result["pair"])
            completed += 1
            print(
                f"P3 quote path: {completed}/{len(tasks)} days",
                flush=True,
            )

    daily = pd.DataFrame(daily_rows).sort_values(["panel_role", "day", "arm"])
    side = pd.DataFrame(side_rows).sort_values(
        ["panel_role", "day", "arm", "side"]
    )
    events = pd.DataFrame(event_rows).sort_values(
        ["panel_role", "day", "arm", "side", "field", "value"]
    )
    pairs = pd.DataFrame(pair_rows).sort_values(["panel_role", "day", "side"])
    daily.to_csv(output_dir / "daily.csv", index=False)
    side.to_csv(output_dir / "quote_side_metrics.csv", index=False)
    events.to_csv(output_dir / "quote_event_counts.csv", index=False)
    pairs.to_csv(output_dir / "paired_quote_coordinates.csv", index=False)

    panel_evidence: dict[str, Any] = {}
    for panel_index, panel in enumerate(spec["panels"]):
        role = str(panel["role"])
        subset_side = side[side["panel_role"].eq(role)]
        subset_pair = pairs[pairs["panel_role"].eq(role)]
        arm_totals = (
            daily[daily["panel_role"].eq(role)]
            .groupby("arm", sort=True)
            .sum(numeric_only=True)
        )
        side_totals = (
            subset_side.groupby(["arm", "side"], sort=True)
            .agg(
                quote_orders=("quote_orders", "sum"),
                mean_floor_binding_rate=("p3_floor_binding_rate", "mean"),
                mean_raw_half_spread=(
                    "mean_raw_half_spread_usdc_per_btc",
                    "mean",
                ),
                mean_final_pair_spread=(
                    "mean_final_pair_spread_usdc_per_btc",
                    "mean",
                ),
            )
            .reset_index()
            .to_dict(orient="records")
        )
        pair_summary = (
            subset_pair.groupby("side", sort=True)
            .agg(
                current_orders=("current_orders", "sum"),
                expanded_orders=("expanded_orders", "sum"),
                matched_orders=("matched_orders", "sum"),
                current_only_orders=("current_only_orders", "sum"),
                expanded_only_orders=("expanded_only_orders", "sum"),
                mean_daily_matched_price_change_rate=(
                    "matched_price_change_rate",
                    "mean",
                ),
                mean_daily_abs_price_change_ticks=(
                    "mean_abs_price_change_ticks",
                    "mean",
                ),
                p90_daily_abs_price_change_ticks=(
                    "p90_abs_price_change_ticks",
                    "mean",
                ),
            )
            .reset_index()
            .to_dict(orient="records")
        )
        panel_evidence[role] = {
            "days": len(panel["days"]),
            "terminal_mtm_pnl_delta": _paired_metric(
                daily,
                role,
                "terminal_mtm_pnl_usdc",
                seed=20260803 + panel_index,
            ),
            "fills_delta": _paired_metric(
                daily,
                role,
                "fills_total",
                seed=20260903 + panel_index,
            ),
            "requotes_delta": _paired_metric(
                daily,
                role,
                "n_requotes",
                seed=20261003 + panel_index,
            ),
            "arm_totals": {
                arm: {
                    key: float(value)
                    for key, value in arm_totals.loc[arm].items()
                }
                for arm in ARMS
            },
            "quote_side_metrics": side_totals,
            "paired_quote_coordinates": pair_summary,
        }

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": spec["identity"],
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "implementation": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
        },
        "only_changed_input": spec["only_changed_input"],
        "panel_evidence": panel_evidence,
        "decision": "expanded_v3_static_replacement_remains_closed",
        "decision_reason": "calibration_gate_failed_before_quote_path_and_quote_path_cannot_rescue_candidate",
        "permissions": spec["permissions"],
        "outputs": {
            "daily": str(output_dir / "daily.csv"),
            "quote_side_metrics": str(output_dir / "quote_side_metrics.csv"),
            "quote_event_counts": str(output_dir / "quote_event_counts.csv"),
            "paired_quote_coordinates": str(
                output_dir / "paired_quote_coordinates.csv"
            ),
        },
    }
    _atomic_json(output_dir / "report.json", report)
    files = {}
    for path in sorted(output_dir.iterdir()):
        if path.name == "manifest.json" or not path.is_file():
            continue
        files[path.name] = {
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    _atomic_json(
        output_dir / "manifest.json",
        {
            "schema_version": "narrowgate_p3_touch_quote_path_output.v1",
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
    parser.add_argument(
        "--spec",
        type=Path,
        default=ROOT
        / "research/families/f02_empirical_p3_touch/docs/"
        "p3_touch_source_aware_expanded_v3_quote_path_spec_20260803.json",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.workers <= 0:
        parser.error("--workers must be positive")
    report = run_audit(args)
    print(json.dumps({"identity": report["identity"], "decision": report["decision"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
