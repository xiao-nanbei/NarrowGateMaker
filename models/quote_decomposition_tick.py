#!/usr/bin/env python3
"""Quote-level decomposition diagnostics for tick replay.

The output is intentionally file-first and terminal-light.  It runs the current
live-style tick replay with historical BBO/L2, then aggregates each order into:

- fill hazard: distance/lifetime/fill-rate by side
- adverse selection: 1s/5s/30s markout and EV by fill side
- inventory control: inventory state at quote/fill
- quote constraint: cap/post-only/mid/final guard intervention rates
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from models.backtest_config import (  # noqa: E402
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from models.replay_contract import (  # noqa: E402
    configure_fixed_latency_distribution,
    freeze_replay_contract,
    validate_frozen_replay_contract,
    write_replay_contract,
)
from models.symbol_paths import DEFAULT_SYMBOL  # noqa: E402
from models.tick_ab import (  # noqa: E402
    base_params as _base_params,
    load_window as _load_window,
    parse_bound as _parse_bound,
    slice_window as _slice_window,
)


def _clean_result(result: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in result.items() if not k.startswith("_")}


def _mean_bool(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float(series.astype(bool).mean())


def _safe_mean(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").mean())


def _run_day(
    symbol: str,
    day: str,
    trace_quotes_max: int,
    trace_fills_max: int,
    trace_decisions_max: int,
    trace_queue_events_max: int,
    *,
    engine: str = "python",
    window_cache_dir: str | None = None,
    refresh_window_cache: bool = False,
    start_ms: int | None = None,
    end_ms: int | None = None,
    queue_regime_calibration: bool = False,
    trace_queue_order_ids: str = "",
    trace_queue_start_ms: int | None = None,
    trace_queue_end_ms: int | None = None,
    l2_refill_cancel_lookback_s: float | None = None,
    l2_refill_cancel_near_levels: int | None = None,
    config_path: str | None = None,
    strict_calibration: bool = False,
    live_perf_telemetry: str | None = None,
    live_perf_latency_mode: str = "avg",
    disable_buy_fill_selection: bool = False,
    execution_trade_source: str = "trades",
    market_context_warmup_days: int = 1,
    require_formal_l2: bool = False,
    verify_formal_l2_hashes: bool = False,
    queue_calibration_path: str | None = None,
    individual_trades_manifest_path: str | None = None,
    individual_trades_integrity_report_path: str | None = None,
    individual_trades_manifest_sha256: str = "",
    individual_trades_integrity_report_sha256: str = "",
    replay_purpose: str = "exploratory",
    initial_state_mode: str = "fresh_start",
    rng_seed: int = 42,
    latency_seed: int = 59,
    latency_profile_id: str = "",
    latency_environment: str = "",
    latency_scenario: str = "baseline",
) -> tuple[
    dict[str, Any],
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    if strict_calibration and not config_path:
        raise RuntimeError("strict quote decomposition requires an explicit --config")
    if config_path:
        # Evidence runners must use the same rolling live baseline as replay
        # candidate tests.  Falling back to _base_params() can silently load the
        # public template model/config and invalidate null/random comparisons.
        base = load_tick_base_params(
            symbol=symbol,
            config_path=config_path,
            configure_symbol=bt.configure_symbol,
            require_historical_bbo=True,
            queue_calibration_path=queue_calibration_path,
            strict_calibration=strict_calibration,
        )
    else:
        base = _base_params(symbol)
    if live_perf_telemetry:
        samples = bt._load_live_perf_latency_samples(
            Path(live_perf_telemetry),
            mode=live_perf_latency_mode,
        )
        base["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
        base["_cancel_order_latency_samples_ms"] = samples["cancel_order_latency_samples_ms"]
        base["live_perf_telemetry_path"] = str(live_perf_telemetry)
        base["live_perf_latency_mode"] = live_perf_latency_mode
    if disable_buy_fill_selection:
        base["buy_fill_selection_live_enabled"] = False
        base["buy_fill_selection_disabled_for_denominator"] = True
    base["execution_trade_source"] = str(execution_trade_source)
    if individual_trades_manifest_path:
        base["individual_trades_manifest_path"] = str(
            Path(individual_trades_manifest_path).expanduser().resolve()
        )
    if individual_trades_integrity_report_path:
        base["individual_trades_integrity_report_path"] = str(
            Path(individual_trades_integrity_report_path).expanduser().resolve()
        )
    if individual_trades_manifest_sha256:
        base["individual_trades_manifest_sha256"] = str(individual_trades_manifest_sha256).lower()
    if individual_trades_integrity_report_sha256:
        base["individual_trades_integrity_report_sha256"] = str(
            individual_trades_integrity_report_sha256
        ).lower()
    base["market_context_warmup_days"] = max(
        0,
        int(market_context_warmup_days),
    )
    base["require_formal_l2"] = bool(require_formal_l2)
    base["verify_formal_l2_hashes"] = bool(verify_formal_l2_hashes)
    if require_formal_l2:
        formal_l2_root = bt.BBO_DIR.parent.resolve()
        base["formal_l2_dataset_root"] = str(formal_l2_root)
        base["formal_l2_manifest_path"] = str(formal_l2_root / "manifest.json")
    base["rng_seed"] = int(rng_seed)
    base["latency_seed"] = int(latency_seed)
    if replay_purpose == "formal":
        configure_fixed_latency_distribution(
            base,
            scenario=latency_scenario,
            profile_id=latency_profile_id,
            environment=latency_environment,
        )
    replay_contract: dict[str, Any] = {}
    if strict_calibration:
        validate_formal_replay_calibration(base, require_latency=True)
        if replay_purpose == "formal":
            replay_contract = freeze_replay_contract(
                base,
                purpose="formal",
                initial_state_mode=initial_state_mode,
                root=ROOT,
            )
            validate_frozen_replay_contract(base)
    base["trace_quotes_max"] = trace_quotes_max
    base["trace_fills_max"] = trace_fills_max
    base["trace_decisions_max"] = trace_decisions_max
    base["trace_queue_events_max"] = trace_queue_events_max
    base["trace_queue_order_ids"] = trace_queue_order_ids
    if l2_refill_cancel_lookback_s is not None:
        base["l2_refill_cancel_lookback_s"] = float(l2_refill_cancel_lookback_s)
    if l2_refill_cancel_near_levels is not None:
        base["l2_refill_cancel_near_levels"] = int(l2_refill_cancel_near_levels)
    if trace_queue_start_ms is not None:
        base["trace_queue_start_ms"] = int(trace_queue_start_ms)
    if trace_queue_end_ms is not None:
        base["trace_queue_end_ms"] = int(trace_queue_end_ms)
    if queue_regime_calibration:
        # 诊断入口：用于 OOS queue/fill-selection parity。
        # 不要把同一 fit 窗口的结果当成 live 参数选择依据；先看机制量和 SELL markout。
        base["queue_regime_calibration_enabled"] = True
    if window_cache_dir:
        base["_window_cache_dir"] = window_cache_dir
    if refresh_window_cache:
        base["_refresh_window_cache"] = True
    load_key = day
    window = _load_window(load_key, base)
    window = _slice_window(window, start_ms, end_ms)
    result = bt._simulate_tick_with_engine(
        engine,
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        base,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
    )
    orders = pd.DataFrame(result.get("_quote_trace", []))
    fills = pd.DataFrame(result.get("_fill_trace", []))
    decisions = pd.DataFrame(result.get("_decision_trace", []))
    queue_events = pd.DataFrame(result.get("_queue_event_trace", []))
    if not orders.empty:
        orders.insert(0, "day", day)
    if not fills.empty:
        fills.insert(0, "day", day)
    if not decisions.empty:
        decisions.insert(0, "day", day)
    if not queue_events.empty:
        queue_events.insert(0, "day", day)
    summary = _clean_result(result)
    summary["day"] = day
    summary["trace_orders"] = int(len(orders))
    summary["trace_fills"] = int(len(fills))
    summary["trace_decisions"] = int(len(decisions))
    summary["trace_queue_events"] = int(len(queue_events))
    summary["trace_quotes_truncated"] = bool(len(orders) >= trace_quotes_max)
    summary["trace_fills_truncated"] = bool(len(fills) >= trace_fills_max)
    summary["trace_decisions_truncated"] = bool(
        trace_decisions_max > 0 and len(decisions) >= trace_decisions_max
    )
    summary["trace_queue_events_truncated"] = bool(
        trace_queue_events_max > 0 and len(queue_events) >= trace_queue_events_max
    )
    summary["replay_purpose"] = str(base.get("replay_purpose", replay_purpose))
    summary["replay_promotion_eligible"] = bool(base.get("replay_promotion_eligible", False))
    summary["replay_contract_sha256"] = str(base.get("replay_contract_sha256", ""))
    if replay_contract:
        summary["_replay_contract"] = replay_contract
    return summary, orders, fills, decisions, queue_events


def _side_summary(orders: pd.DataFrame, fills: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (day, side), side_orders in orders.groupby(["day", "side"], dropna=False):
        side_fills = (
            fills[(fills["day"] == day) & (fills["side"] == side)]
            if not fills.empty
            else pd.DataFrame()
        )
        filled_order_ids = (
            set(side_fills["order_id"].dropna().astype(int))
            if not side_fills.empty and "order_id" in side_fills
            else set()
        )
        unique_orders = side_orders["order_id"].nunique()
        canceled = side_orders[side_orders["outcome"] == "cancel"]
        fill_outcomes = side_orders[side_orders["outcome"] == "fill"]
        rows.append(
            {
                "day": day,
                "side": side,
                "orders": int(unique_orders),
                "filled_orders": int(len(filled_order_ids)),
                "fill_rate_per_order": len(filled_order_ids) / max(unique_orders, 1),
                "fill_events": int(len(side_fills)),
                "cancel_events": int(len(canceled)),
                "avg_distance_to_bbo": _safe_mean(side_orders, "final_quote_delta_to_bbo"),
                "avg_raw_distance_to_bbo": _safe_mean(side_orders, "raw_quote_delta_to_bbo"),
                "avg_distance_to_mid": _safe_mean(side_orders, "final_distance_to_mid"),
                "avg_raw_half_spread": _safe_mean(side_orders, "raw_half_spread"),
                "avg_raw_mid_shift": _safe_mean(side_orders, "raw_mid_shift"),
                "avg_abs_raw_mid_shift": float(
                    pd.to_numeric(side_orders["raw_mid_shift"], errors="coerce").abs().mean()
                ),
                "avg_fill_lifetime_ms": _safe_mean(fill_outcomes, "lifetime_ms"),
                "avg_cancel_lifetime_ms": _safe_mean(canceled, "lifetime_ms"),
                "final_guard_changed_rate": _mean_bool(side_orders["final_guard_changed"]),
                "any_constraint_changed_rate": _mean_bool(side_orders["any_constraint_changed"]),
                "delta_cap_rate": _mean_bool(side_orders["delta_cap"]),
                "post_only_rate": _mean_bool(side_orders["post_only"]),
                "mid_guard_rate": _mean_bool(side_orders["mid_guard"]),
                "final_compressed_rate": _mean_bool(side_orders["final_compressed"]),
                "avg_markout_1s": _safe_mean(side_fills, "markout_1s"),
                "avg_markout_5s": _safe_mean(side_fills, "markout_5s"),
                "avg_markout_30s": _safe_mean(side_fills, "markout_30s"),
                "avg_ev_30s": _safe_mean(side_fills, "ev_30s"),
                "toxic_30s_rate": _mean_bool(side_fills["toxic_30s"])
                if not side_fills.empty
                else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _module_summary(
    day_summaries: pd.DataFrame, orders: pd.DataFrame, fills: pd.DataFrame
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day, day_orders in orders.groupby("day"):
        day_fills = fills[fills["day"] == day] if not fills.empty else pd.DataFrame()
        filled_ids = (
            set(day_fills["order_id"].dropna().astype(int))
            if not day_fills.empty and "order_id" in day_fills
            else set()
        )
        unique_orders = day_orders["order_id"].nunique()
        canceled = day_orders[day_orders["outcome"] == "cancel"]
        summary_row = day_summaries[day_summaries["day"] == day].iloc[0]
        rows.extend(
            [
                {
                    "day": day,
                    "module": "fill_hazard",
                    "primary_metric": "fill_rate_per_order",
                    "primary_value": len(filled_ids) / max(unique_orders, 1),
                    "support_1": "avg_distance_to_bbo",
                    "support_value_1": _safe_mean(day_orders, "final_quote_delta_to_bbo"),
                    "support_2": "avg_fill_lifetime_ms",
                    "support_value_2": _safe_mean(
                        day_orders[day_orders["outcome"] == "fill"], "lifetime_ms"
                    ),
                    "note": "成交概率是否主要由最终盘口距离和挂单寿命解释。",
                },
                {
                    "day": day,
                    "module": "adverse_selection",
                    "primary_metric": "avg_ev_30s",
                    "primary_value": _safe_mean(day_fills, "ev_30s"),
                    "support_1": "toxic_30s_rate",
                    "support_value_1": _mean_bool(day_fills["toxic_30s"])
                    if not day_fills.empty
                    else 0.0,
                    "support_2": "avg_markout_5s",
                    "support_value_2": _safe_mean(day_fills, "markout_5s"),
                    "note": "成交后 1/5/30s 是否覆盖 adverse markout。",
                },
                {
                    "day": day,
                    "module": "inventory_control",
                    "primary_metric": "inventory_adjusted_pnl",
                    "primary_value": float(summary_row.get("inventory_adjusted_pnl", 0.0)),
                    "support_1": "max_inventory",
                    "support_value_1": float(summary_row.get("max_inventory", 0.0)),
                    "support_2": "avg_abs_inventory_at_quote",
                    "support_value_2": float(
                        pd.to_numeric(day_orders["inventory"], errors="coerce").abs().mean()
                    ),
                    "note": "库存控制是否用收益代价换来了风险下降。",
                },
                {
                    "day": day,
                    "module": "quote_constraint",
                    "primary_metric": "any_constraint_changed_rate",
                    "primary_value": _mean_bool(day_orders["any_constraint_changed"]),
                    "support_1": "cancel_events",
                    "support_value_1": float(len(canceled)),
                    "support_2": "stale_book_skip_rate",
                    "support_value_2": float(summary_row.get("stale_book_skip_rate", 0.0)),
                    "note": "cap/post-only/mid/final/stale 是否压扁上层报价意图。",
                },
            ]
        )
    return pd.DataFrame(rows)


def _cancel_reasons(orders: pd.DataFrame) -> pd.DataFrame:
    canceled = orders[orders["outcome"] == "cancel"].copy()
    if canceled.empty:
        return pd.DataFrame()
    return (
        canceled.groupby(["day", "side", "cancel_reason"], dropna=False)
        .agg(
            count=("order_id", "count"),
            avg_lifetime_ms=("lifetime_ms", "mean"),
            avg_distance_to_bbo=("final_quote_delta_to_bbo", "mean"),
            final_guard_changed_rate=("final_guard_changed", "mean"),
            any_constraint_changed_rate=("any_constraint_changed", "mean"),
        )
        .reset_index()
        .sort_values(["day", "side", "count"], ascending=[True, True, False])
    )


def _bias_toxicity(fills: pd.DataFrame) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame()
    by_cols = ["day", "side", "favored_by_raw_shift", "any_constraint_changed"]
    return (
        fills.groupby(by_cols, dropna=False)
        .agg(
            fills=("order_id", "count"),
            avg_raw_mid_shift=("raw_mid_shift", "mean"),
            avg_final_distance_to_bbo=("final_quote_delta_to_bbo", "mean"),
            avg_markout_1s=("markout_1s", "mean"),
            avg_markout_5s=("markout_5s", "mean"),
            avg_markout_30s=("markout_30s", "mean"),
            avg_ev_30s=("ev_30s", "mean"),
            toxic_30s_rate=("toxic_30s", "mean"),
        )
        .reset_index()
        .sort_values(["day", "side", "favored_by_raw_shift", "any_constraint_changed"])
    )


def _hazard_by_distance(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame()
    work = orders[
        [
            "day",
            "side",
            "order_id",
            "outcome",
            "lifetime_ms",
            "final_quote_delta_to_bbo",
            "final_distance_to_mid",
            "raw_mid_shift",
            "any_constraint_changed",
        ]
    ].copy()
    work["filled_event"] = work["outcome"].eq("fill")
    level = work.groupby(["day", "side", "order_id"], as_index=False).agg(
        filled=("filled_event", "max"),
        lifetime_ms=("lifetime_ms", "max"),
        final_quote_delta_to_bbo=("final_quote_delta_to_bbo", "first"),
        final_distance_to_mid=("final_distance_to_mid", "first"),
        raw_mid_shift=("raw_mid_shift", "first"),
        any_constraint_changed=("any_constraint_changed", "first"),
    )
    level["distance_bucket"] = pd.cut(
        level["final_quote_delta_to_bbo"],
        [-1e12, 0, 10, 20, 30, 40, 60, 1e12],
        labels=["inside_or_best", "0_10", "10_20", "20_30", "30_40", "40_60", "gt60"],
    )
    return (
        level.groupby(["day", "side", "distance_bucket"], observed=True)
        .agg(
            orders=("order_id", "count"),
            filled_orders=("filled", "sum"),
            fill_rate=("filled", "mean"),
            avg_lifetime_ms=("lifetime_ms", "mean"),
            avg_distance_to_bbo=("final_quote_delta_to_bbo", "mean"),
            avg_distance_to_mid=("final_distance_to_mid", "mean"),
            avg_raw_mid_shift=("raw_mid_shift", "mean"),
            constraint_rate=("any_constraint_changed", "mean"),
        )
        .reset_index()
        .sort_values(["day", "side", "distance_bucket"])
    )


def _df_to_md(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    shown = frame.copy()
    for col in shown.columns:
        if pd.api.types.is_float_dtype(shown[col]):
            shown[col] = shown[col].map(lambda v: "" if pd.isna(v) else f"{v:.6g}")
    cols = list(shown.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for _, row in shown.iterrows():
        vals = [str(row[col]) if not pd.isna(row[col]) else "" for col in cols]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def _write_markdown(
    path: Path,
    summaries: pd.DataFrame,
    side: pd.DataFrame,
    modules: pd.DataFrame,
    cancels: pd.DataFrame,
    bias: pd.DataFrame,
    hazard: pd.DataFrame,
    outputs: dict[str, str],
) -> None:
    lines = [
        "# Tick Quote Decomposition",
        "",
        "## Run Summary",
        "",
        _df_to_md(
            summaries[
                [
                    "day",
                    "pnl",
                    "inventory_adjusted_pnl",
                    "avg_markout",
                    "fills_bid",
                    "fills_ask",
                    "avg_final_spread",
                    "cap_hit_rate",
                    "stale_book_skip_rate",
                    "trace_orders",
                    "trace_fills",
                ]
            ].round(6)
        ),
        "",
        "## Four Modules",
        "",
        _df_to_md(modules.round(6)),
        "",
        "## Side Fill Hazard / EV",
        "",
        _df_to_md(side.round(6)),
        "",
        "## Top Cancel Reasons",
        "",
        _df_to_md(cancels.head(20).round(6)) if not cancels.empty else "_No cancels traced._",
        "",
        "## Raw Shift vs Toxic Fills",
        "",
        _df_to_md(bias.round(6)) if not bias.empty else "_No fills traced._",
        "",
        "## Fill Hazard By Distance",
        "",
        _df_to_md(hazard.round(6)) if not hazard.empty else "_No order trace._",
        "",
        "## Files",
        "",
    ]
    for name, out_path in outputs.items():
        lines.append(f"- {name}: `{out_path}`")
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument(
        "--days",
        nargs="+",
        required=True,
        help="UTC days to replay as independent period rows, e.g. 2026-05-15",
    )
    parser.add_argument(
        "--start-date", default=None, help="Optional UTC slice start, e.g. 2026-05-15 00:00"
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional UTC slice end; date-only values are exclusive next-day",
    )
    parser.add_argument("--tag", default=time.strftime("%Y%m%d_%H%M%S"))
    parser.add_argument(
        "--config",
        default=None,
        help="Optional live config YAML to map into replay parameters; use for current-baseline evidence.",
    )
    parser.add_argument("--trace-quotes-max", type=int, default=2_000_000)
    parser.add_argument("--trace-fills-max", type=int, default=200_000)
    parser.add_argument(
        "--trace-decisions-max",
        type=int,
        default=2_000_000,
        help="Maximum replay quote-decision rows to trace, including pause/keep actions.",
    )
    parser.add_argument(
        "--trace-queue-events-max",
        type=int,
        default=0,
        help="Maximum per-trade queue/fill debug rows to trace. Default is disabled.",
    )
    parser.add_argument(
        "--trace-queue-order-ids",
        default="",
        help="Comma-separated replay order ids to include in queue debug trace.",
    )
    parser.add_argument(
        "--trace-queue-start-date",
        default=None,
        help="Optional UTC queue-debug start, e.g. 2026-06-27 13:36:25.",
    )
    parser.add_argument(
        "--trace-queue-end-date",
        default=None,
        help="Optional UTC queue-debug end.",
    )
    parser.add_argument(
        "--engine",
        choices=("python", "cpp"),
        default="python",
        help="Replay engine used to generate quote/fill traces.",
    )
    parser.add_argument(
        "--window-cache-dir",
        default=None,
        help=(
            "Optional tick-window cache directory. Caches tick-window trades, variance, "
            "BBO/L2, and ML prediction arrays after quality filtering."
        ),
    )
    parser.add_argument(
        "--refresh-window-cache",
        action="store_true",
        help="Rebuild cached tick windows before generating traces.",
    )
    parser.add_argument(
        "--queue-regime-calibration",
        action="store_true",
        help="Apply live-log side/regime queue multipliers to fallback queue estimates.",
    )
    parser.add_argument(
        "--strict-calibration",
        action="store_true",
        help=(
            "Require formal P3, queue, latency, BBO, merged-clock, and causal-ML "
            "identity. Empirical P3 artifact values override research YAML knobs."
        ),
    )
    parser.add_argument(
        "--live-perf-telemetry",
        default=None,
        help="Frozen live telemetry CSV/CSV.GZ used for empirical REST new/cancel latency.",
    )
    parser.add_argument(
        "--live-perf-latency-mode",
        choices=("avg", "max", "sum"),
        default="avg",
    )
    parser.add_argument(
        "--execution-trade-source",
        choices=("aggTrades", "trades"),
        default="trades",
        help="Execution event tape; formal order-level evidence uses individual trades.",
    )
    parser.add_argument("--individual-trades-manifest-path", type=Path)
    parser.add_argument("--individual-trades-integrity-report-path", type=Path)
    parser.add_argument("--individual-trades-manifest-sha256", default="")
    parser.add_argument(
        "--individual-trades-integrity-report-sha256",
        default="",
    )
    parser.add_argument(
        "--market-context-warmup-days",
        type=int,
        default=1,
        help="Causal BBO/L2/bar context loaded before the target UTC day.",
    )
    parser.add_argument(
        "--require-formal-l2",
        action="store_true",
        help="Reject dates whose normalized BBO/L2 or warmup context is not formal eligible.",
    )
    parser.add_argument(
        "--verify-formal-l2-hashes",
        action="store_true",
        help="Rehash normalized BBO/L2 inputs before replay.",
    )
    parser.add_argument(
        "--queue-calibration-path",
        default=None,
        help="Explicit queue-v3 artifact used by strict replay.",
    )
    parser.add_argument(
        "--replay-purpose",
        choices=("exploratory", "formal"),
        default="exploratory",
    )
    parser.add_argument(
        "--initial-state-mode",
        choices=("fresh_start",),
        default="fresh_start",
    )
    parser.add_argument("--rng-seed", type=int, default=42)
    parser.add_argument("--latency-seed", type=int, default=59)
    parser.add_argument("--latency-profile-id", default="")
    parser.add_argument("--latency-environment", default="")
    parser.add_argument(
        "--latency-scenario",
        choices=("baseline", "stress"),
        default="baseline",
    )
    parser.add_argument(
        "--disable-buy-fill-selection",
        action="store_true",
        help="Build an unconditioned order denominator without an existing BUY scorer.",
    )
    parser.add_argument(
        "--l2-refill-cancel-lookback-s",
        type=float,
        default=None,
        help="Quote-time lookback window for exact-L2 refill/cancel trace fields (default: replay default 10s).",
    )
    parser.add_argument(
        "--l2-refill-cancel-near-levels",
        type=int,
        default=None,
        help="Near-book L2 levels used for refill/cancel trace fields (default: replay default 5).",
    )
    args = parser.parse_args()
    if args.strict_calibration and not args.require_formal_l2:
        raise SystemExit(
            "--strict-calibration order-level evidence also requires --require-formal-l2"
        )
    if args.engine == "cpp" and (
        args.l2_refill_cancel_lookback_s is not None
        or args.l2_refill_cancel_near_levels is not None
    ):
        raise SystemExit(
            "Exact-L2 refill/cancel trace fields are Python-engine authoritative; "
            "rerun with --engine python until C++ trace parity is implemented."
        )

    bt.configure_symbol(args.symbol)
    start_ms = _parse_bound(args.start_date, is_end=False)
    end_ms = _parse_bound(args.end_date, is_end=True)
    trace_queue_start_ms = _parse_bound(args.trace_queue_start_date, is_end=False)
    trace_queue_end_ms = _parse_bound(args.trace_queue_end_date, is_end=True)
    periods: list[tuple[str, int | None, int | None]] = []
    for day in args.days:
        day_start = _parse_bound(f"{day} 00:00", is_end=False)
        day_end = _parse_bound(day, is_end=True)
        seg_start = max(x for x in (start_ms, day_start) if x is not None)
        seg_end = min(x for x in (end_ms, day_end) if x is not None)
        periods.append((day, seg_start, seg_end))

    summaries: list[dict[str, Any]] = []
    order_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    decision_frames: list[pd.DataFrame] = []
    queue_event_frames: list[pd.DataFrame] = []
    replay_contracts: list[dict[str, Any]] = []
    for day, period_start_ms, period_end_ms in periods:
        print(f"Running quote decomposition {args.symbol} {day} ...")
        summary, orders, fills, decisions, queue_events = _run_day(
            args.symbol,
            day,
            trace_quotes_max=args.trace_quotes_max,
            trace_fills_max=args.trace_fills_max,
            trace_decisions_max=args.trace_decisions_max,
            trace_queue_events_max=args.trace_queue_events_max,
            engine=args.engine,
            window_cache_dir=args.window_cache_dir,
            refresh_window_cache=args.refresh_window_cache,
            start_ms=period_start_ms,
            end_ms=period_end_ms,
            queue_regime_calibration=args.queue_regime_calibration,
            trace_queue_order_ids=args.trace_queue_order_ids,
            trace_queue_start_ms=trace_queue_start_ms,
            trace_queue_end_ms=trace_queue_end_ms,
            l2_refill_cancel_lookback_s=args.l2_refill_cancel_lookback_s,
            l2_refill_cancel_near_levels=args.l2_refill_cancel_near_levels,
            config_path=args.config,
            strict_calibration=args.strict_calibration,
            live_perf_telemetry=args.live_perf_telemetry,
            live_perf_latency_mode=args.live_perf_latency_mode,
            disable_buy_fill_selection=args.disable_buy_fill_selection,
            execution_trade_source=args.execution_trade_source,
            market_context_warmup_days=args.market_context_warmup_days,
            require_formal_l2=args.require_formal_l2,
            verify_formal_l2_hashes=args.verify_formal_l2_hashes,
            queue_calibration_path=args.queue_calibration_path,
            individual_trades_manifest_path=args.individual_trades_manifest_path,
            individual_trades_integrity_report_path=(args.individual_trades_integrity_report_path),
            individual_trades_manifest_sha256=(args.individual_trades_manifest_sha256),
            individual_trades_integrity_report_sha256=(
                args.individual_trades_integrity_report_sha256
            ),
            replay_purpose=args.replay_purpose,
            initial_state_mode=args.initial_state_mode,
            rng_seed=args.rng_seed,
            latency_seed=args.latency_seed,
            latency_profile_id=args.latency_profile_id,
            latency_environment=args.latency_environment,
            latency_scenario=args.latency_scenario,
        )
        replay_contract = summary.pop("_replay_contract", {})
        if replay_contract:
            replay_contracts.append(replay_contract)
        summaries.append(summary)
        if not orders.empty:
            order_frames.append(orders)
        if not fills.empty:
            fill_frames.append(fills)
        if not decisions.empty:
            decision_frames.append(decisions)
        if not queue_events.empty:
            queue_event_frames.append(queue_events)
        print(
            f"  done {day}: pnl={summary['pnl']:.4f} "
            f"fills={summary['fills_bid']}/{summary['fills_ask']} "
            f"orders={summary['trace_orders']} decisions={summary['trace_decisions']} "
            f"queue_events={summary['trace_queue_events']}"
        )

    summary_df = pd.DataFrame(summaries)
    orders_df = pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame()
    fills_df = pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame()
    decisions_df = (
        pd.concat(decision_frames, ignore_index=True) if decision_frames else pd.DataFrame()
    )
    queue_events_df = (
        pd.concat(queue_event_frames, ignore_index=True) if queue_event_frames else pd.DataFrame()
    )
    if orders_df.empty:
        raise SystemExit("No quote trace produced; increase --trace-quotes-max or check params.")

    side_df = _side_summary(orders_df, fills_df)
    module_df = _module_summary(summary_df, orders_df, fills_df)
    cancel_df = _cancel_reasons(orders_df)
    bias_df = _bias_toxicity(fills_df)
    hazard_df = _hazard_by_distance(orders_df)

    base = bt.RESULTS_DIR / f"tick_quote_decomposition_{args.tag}_{args.symbol.lower()}"
    outputs = {
        "summary": str(base.with_suffix(".summary.csv")),
        "orders": str(base.with_suffix(".orders.csv")),
        "fills": str(base.with_suffix(".fills.csv")),
        "decisions": str(base.with_suffix(".decisions.csv")),
        "queue_events": str(base.with_suffix(".queue_events.csv")),
        "side": str(base.with_suffix(".side.csv")),
        "modules": str(base.with_suffix(".modules.csv")),
        "cancels": str(base.with_suffix(".cancel_reasons.csv")),
        "bias_toxicity": str(base.with_suffix(".bias_toxicity.csv")),
        "hazard_by_distance": str(base.with_suffix(".hazard_by_distance.csv")),
        "json": str(base.with_suffix(".summary.json")),
        "markdown": str(base.with_suffix(".md")),
    }
    if replay_contracts:
        contract_hashes = {
            str(contract.get("contract_sha256", "")) for contract in replay_contracts
        }
        if len(contract_hashes) != 1:
            raise RuntimeError("quote-decomposition days produced different replay contracts")
        outputs["replay_contract"] = str(base.with_suffix(".replay_contract.json"))
        write_replay_contract(
            replay_contracts[0],
            outputs["replay_contract"],
        )

    summary_df.to_csv(outputs["summary"], index=False)
    orders_df.to_csv(outputs["orders"], index=False)
    fills_df.to_csv(outputs["fills"], index=False)
    decisions_df.to_csv(outputs["decisions"], index=False)
    queue_events_df.to_csv(outputs["queue_events"], index=False)
    side_df.to_csv(outputs["side"], index=False)
    module_df.to_csv(outputs["modules"], index=False)
    cancel_df.to_csv(outputs["cancels"], index=False)
    bias_df.to_csv(outputs["bias_toxicity"], index=False)
    hazard_df.to_csv(outputs["hazard_by_distance"], index=False)
    Path(outputs["json"]).write_text(
        json.dumps(
            {
                "symbol": args.symbol,
                "days": [p for p, _, _ in periods],
                "period_grain": "day",
                "outputs": outputs,
                "summary": summaries,
                "replay_contract": replay_contracts[0] if replay_contracts else None,
            },
            indent=2,
            default=float,
        )
    )
    _write_markdown(
        Path(outputs["markdown"]),
        summary_df,
        side_df,
        module_df,
        cancel_df,
        bias_df,
        hazard_df,
        outputs,
    )

    for out_path in outputs.values():
        print(f"Saved {out_path}")


if __name__ == "__main__":
    main()
