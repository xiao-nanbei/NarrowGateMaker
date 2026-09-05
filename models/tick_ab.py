#!/usr/bin/env python3
"""Unified tick-replay A/B framework and experiment registry.

Every arm-sweep A/B used to live in its own ~330-line ``*_ab_tick.py`` file that
re-implemented the same scaffolding (load window -> fork workers -> run each arm
-> aggregate deltas -> write csv/summary/json/md). This module factors that
scaffolding out once and turns each experiment into a small declarative
``Experiment`` entry in ``EXPERIMENTS``.

Adding a new arm-sweep is now a registry edit, not a new file:

    EXPERIMENTS["my_lever"] = Experiment(
        key="my_lever",
        title="My Lever",
        stem="tick_my_lever_ab",
        arms=[Arm("baseline", "baseline", {}), Arm("on", "treat", {"my_flag": True})],
    )

Usage:
    python3 models/tick_ab.py --list
    python3 models/tick_ab.py spread_cap   --days 2026-05-01 2026-05-02 --workers 6
    python3 models/tick_ab.py spread_cap   --days 2026-05-15 --engine cpp
    python3 models/tick_ab.py noise_guard  --symbol BTCUSDC --days 2026-05-15

Research conclusions are day-only. Every retained UTC day is replayed from cold
state. All arms hold the current live params fixed and change only the listed
knobs.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from models.backtest_config import disable_ml_params, load_tick_base_params  # noqa: E402
from models.backtest_utils import default_backtest_workers  # noqa: E402
from models.data_windows import (  # noqa: E402
    load_tick_window_dict,
    parse_bound,
    slice_window,
)
from models.symbol_paths import DEFAULT_SYMBOL  # noqa: E402


def clean_result(result: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in result.items() if not key.startswith("_")}


def base_params(
    symbol: str,
    *,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    return load_tick_base_params(
        symbol=symbol,
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
    )


def load_window(day: str, params: dict[str, Any]) -> dict[str, Any]:
    return load_tick_window_dict(
        day,
        params,
        load_ml=True,
        require_ml=True,
        cross_market_enabled=True,
        require_historical_bbo=True,
        cache_dir=params.get("_window_cache_dir"),
        refresh_cache=bool(params.get("_refresh_window_cache", False)),
    )


# Internal aliases keep the registry implementation concise while callers use
# the public names above.
_base_params = base_params
_clean_result = clean_result
_load_window = load_window
_parse_bound = parse_bound
_slice_window = slice_window

# Metrics that always get a per-day ``delta_<metric>`` column (vs the
# ``baseline`` arm). Experiments may extend this with their own metrics.
CORE_DELTA_METRICS = (
    "pnl",
    "inventory_adjusted_pnl",
    "avg_markout",
    "fills_per_day",
    "abs_inventory_time_s",
    "time_avg_abs_inventory",
    "pnl_per_abs_inventory_hour",
    "inv_adj_pnl_per_abs_inventory_hour",
)

# Columns rendered (when present) in the summary csv / md, in order.
CORE_SUMMARY_COLUMNS = (
    "day",
    "arm",
    "group",
    "control",
    "pnl",
    "delta_pnl",
    "inventory_adjusted_pnl",
    "delta_inventory_adjusted_pnl",
    "avg_markout",
    "delta_avg_markout",
    "fills_bid",
    "fills_ask",
    "fills_total",
    "fills_per_day",
    "delta_fills_per_day",
    "abs_inventory_time_s",
    "delta_abs_inventory_time_s",
    "time_avg_abs_inventory",
    "delta_time_avg_abs_inventory",
    "pnl_per_abs_inventory_hour",
    "delta_pnl_per_abs_inventory_hour",
    "inv_adj_pnl_per_abs_inventory_hour",
    "delta_inv_adj_pnl_per_abs_inventory_hour",
)
TAIL_SUMMARY_COLUMNS = (
    "max_inventory",
    "final_inventory",
    "notional_inventory_time_s",
    "runtime_s",
    "note",
)


@dataclass(frozen=True)
class Arm:
    """A single A/B arm: a named set of param overrides on top of live config."""

    name: str
    group: str
    overrides: dict[str, Any] = field(default_factory=dict)
    note: str = ""
    needs_liq_baseline: bool = False
    control: bool = False
    ml_enabled: bool | None = None
    cross_market_enabled: bool | None = None


@dataclass(frozen=True)
class Experiment:
    """Declarative description of one arm-sweep A/B."""

    key: str
    title: str
    stem: str
    arms: list[Arm]
    default_tag: str = "20260530"
    # Extra metrics (beyond CORE_DELTA_METRICS) to add delta_ columns for.
    delta_metrics: tuple[str, ...] = ()
    # Extra result columns to surface in the summary (after the core block).
    summary_extra: tuple[str, ...] = ()
    # Keys printed in the one-line "Base params:" banner.
    base_summary_keys: tuple[str, ...] = (
        "maker_fill_prob",
        "use_bar_pricing",
        "fill_cooldown",
        "rq_min",
        "rq_max",
    )
    # Optional hook: mutate ``params`` in place per arm (e.g. inject calibrated
    # liquidity baseline, bump trace buffers). ``ctx`` is the per-window context.
    param_hook: Callable[[dict[str, Any], Arm, dict[str, Any]], None] | None = None
    # Optional hook: derive extra row metrics from the raw simulate result.
    extra_metrics: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    # Optional hook: build a per-window context (e.g. calibrate liq baseline).
    window_hook: Callable[[dict[str, Any], dict[str, Any], argparse.Namespace], dict[str, Any]] | None = None
    # Optional hook: adjust base params once before running (e.g. force a flag).
    prepare_base: Callable[[dict[str, Any], argparse.Namespace], dict[str, Any]] | None = None
    # Optional hook: register experiment-specific CLI args.
    extra_cli: Callable[[argparse.ArgumentParser], None] | None = None
    # Optional dynamic arm grid built from experiment-specific CLI arguments.
    arm_factory: Callable[[argparse.Namespace], list[Arm]] | None = None
    # Optional loader for experiments with different ML/cache requirements.
    window_loader: Callable[
        [str, dict[str, Any], argparse.Namespace], dict[str, Any]
    ] | None = None
    # Optional per-arm ML selection (boolean ML/source-wiring tests).
    ml_data_hook: Callable[[dict[str, Any], Arm], Any] | None = None

    @property
    def arms_by_name(self) -> dict[str, Arm]:
        return {arm.name: arm for arm in self.arms}


# Worker globals (fork copy-on-write); set in the parent before the pool starts.
_WORKER_BASE: dict[str, Any] | None = None
_WORKER_WINDOW: dict[str, Any] | None = None
_WORKER_CTX: dict[str, Any] | None = None
_WORKER_EXP: Experiment | None = None
_WORKER_ENGINE: str = "python"


def _run_case(day: str, arm: Arm) -> dict[str, Any]:
    base, window, ctx, exp = _WORKER_BASE, _WORKER_WINDOW, _WORKER_CTX, _WORKER_EXP
    if base is None or window is None or exp is None:
        raise RuntimeError("Worker state not initialized")
    ctx = ctx or {}

    params = dict(base)
    params.update(arm.overrides)
    if exp.param_hook is not None:
        exp.param_hook(params, arm, ctx)

    started = time.perf_counter()
    result = bt._simulate_tick_with_engine(
        _WORKER_ENGINE,
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        params,
        ml_data=(
            exp.ml_data_hook(window, arm)
            if exp.ml_data_hook is not None
            else window["ml_data"]
        ),
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
    )
    row = _clean_result(result)
    if exp.extra_metrics is not None:
        row.update(exp.extra_metrics(result))
    row.update(
        {
            "day": day,
            "arm": arm.name,
            "group": arm.group,
            "control": arm.control,
            "override": ";".join(f"{k}={v}" for k, v in sorted(arm.overrides.items())) or "baseline",
            "runtime_s": round(time.perf_counter() - started, 3),
            "note": arm.note,
        }
    )
    return row


def _run_job(job: tuple[str, Arm]) -> dict[str, Any]:
    return _run_case(job[0], job[1])


def _segment_bounds(
    window: dict[str, Any], window_days: int | None
) -> list[tuple[str | None, int | None, int | None]]:
    """Return ``(label, start_ms, end_ms)`` fresh-start segments for a window.

    Return one-day fresh-start segments aligned to UTC midnight; this avoids
    cross-day state contamination (e.g. the markout-EMA pause latch that freezes
    a side after a toxic regime).
    """
    if not window_days or window_days <= 0:
        return [(None, None, None)]
    trades = window["trades"]
    t0 = int(trades["transact_time"].iloc[0])
    t1 = int(trades["transact_time"].iloc[-1])
    day_ms = 86_400_000
    step = window_days * day_ms
    segments: list[tuple[str | None, int | None, int | None]] = []
    seg_start = (t0 // day_ms) * day_ms
    while seg_start <= t1:
        seg_end = seg_start + step
        label = pd.Timestamp(seg_start, unit="ms", tz="UTC").strftime("%Y-%m-%d")
        segments.append((label, seg_start, seg_end))
        seg_start = seg_end
    return segments


def _normalize_days(days: list[str] | None) -> list[str]:
    out: list[str] = []
    for item in days or []:
        token = str(item).strip()
        if not token:
            continue
        if "/" in token or len(token) != 10:
            raise ValueError(f"Use explicit UTC daily dates YYYY-MM-DD, not ranges/months: {token}")
        ts = pd.Timestamp(token)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        out.append(ts.strftime("%Y-%m-%d"))
    return sorted(set(out))


def _add_deltas(frame: pd.DataFrame, metrics: tuple[str, ...]) -> pd.DataFrame:
    out = frame.copy()
    for _, index in out.groupby("day").groups.items():
        day_frame = out.loc[index]
        control_mask = (
            day_frame["control"].astype(bool)
            if "control" in day_frame
            else day_frame["arm"].eq("baseline")
        )
        global_controls = day_frame[control_mask | day_frame["arm"].eq("baseline")]
        for _, group_index in day_frame.groupby("group").groups.items():
            group = out.loc[group_index]
            group_controls = (
                group[group["control"].astype(bool)]
                if "control" in group
                else group.iloc[0:0]
            )
            if group_controls.empty:
                group_controls = global_controls[global_controls["arm"].eq("baseline")]
            if group_controls.empty:
                continue
            base_row = group_controls.iloc[0]
            for metric in metrics:
                if metric in out.columns:
                    out.loc[group_index, f"delta_{metric}"] = (
                        out.loc[group_index, metric] - base_row[metric]
                    )
    return out


def _summary_columns(exp: Experiment, frame: pd.DataFrame) -> list[str]:
    ordered: list[str] = []
    for col in CORE_SUMMARY_COLUMNS:
        if col in frame.columns:
            ordered.append(col)
    for col in exp.summary_extra:
        if col in frame.columns and col not in ordered:
            ordered.append(col)
        delta = f"delta_{col}"
        if delta in frame.columns and delta not in ordered:
            ordered.append(delta)
    for col in TAIL_SUMMARY_COLUMNS:
        if col in frame.columns and col not in ordered:
            ordered.append(col)
    return ordered


def _fmt(value: Any) -> str:
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return ""
        return f"{value:.4f}"
    return str(value)


def _write_outputs(exp: Experiment, rows: list[dict[str, Any]], tag: str, symbol: str) -> None:
    out_dir = bt.RESULTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{exp.stem}_{tag}_{symbol.lower()}.csv"

    delta_metrics = tuple(dict.fromkeys(CORE_DELTA_METRICS + exp.delta_metrics))
    frame = _add_deltas(pd.DataFrame(rows), delta_metrics)
    frame.to_csv(out_csv, index=False)

    cols = _summary_columns(exp, frame)
    summary = frame[cols].sort_values(["day", "group", "arm"])
    summary_csv = out_csv.with_name(out_csv.stem + "_summary.csv")
    summary.to_csv(summary_csv, index=False)

    json_path = out_csv.with_suffix(".summary.json")
    json_path.write_text(
        json.dumps({"tag": tag, "rows": json.loads(summary.to_json(orient="records"))}, indent=2),
        encoding="utf-8",
    )

    md_path = out_csv.with_suffix(".md")
    header = "| " + " | ".join(cols) + " |"
    sep = "|" + "|".join("---" for _ in cols) + "|"
    lines = [f"# {exp.title} A/B {tag}", "",
             "Fixed current live continuous params; each arm changes only the listed knobs.",
             "", header, sep]
    for _, row in summary.iterrows():
        lines.append("| " + " | ".join(_fmt(row[col]) for col in cols) + " |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    for path in (out_csv, summary_csv, json_path, md_path):
        print(f"Saved {path}")
    print(summary.to_string(index=False))


def _write_segment_rollup(
    exp: Experiment, rows: list[dict[str, Any]], tag: str, symbol: str
) -> None:
    """Aggregate independent fresh-start segments into a per-arm roll-up.

    Each segment is one independent sample, so the mean/median/win-rate of the
    per-segment ``delta_pnl`` is a far more trustworthy verdict than a single
    continuous-window delta."""
    delta_metrics = tuple(dict.fromkeys(CORE_DELTA_METRICS + exp.delta_metrics))
    frame = _add_deltas(pd.DataFrame(rows), delta_metrics)
    agg_rows: list[dict[str, Any]] = []
    for arm, grp in frame.groupby("arm"):
        agg: dict[str, Any] = {
            "day": "ALL",
            "arm": arm,
            "group": grp["group"].iloc[0],
            "n_segments": int(len(grp)),
        }
        if "fills_total" in grp:
            agg["fills_total"] = int(grp["fills_total"].sum())
        if "fills_per_day" in grp:
            agg["mean_fills_per_day"] = round(float(grp["fills_per_day"].mean()), 4)
        if "abs_inventory_time_s" in grp:
            agg["sum_abs_inventory_time_s"] = round(float(grp["abs_inventory_time_s"].sum()), 4)
        if "time_avg_abs_inventory" in grp:
            agg["mean_time_avg_abs_inventory"] = round(float(grp["time_avg_abs_inventory"].mean()), 6)
        if "pnl_per_abs_inventory_hour" in grp:
            agg["mean_pnl_per_abs_inventory_hour"] = round(
                float(grp["pnl_per_abs_inventory_hour"].mean()), 4
            )
        agg["sum_pnl"] = round(float(grp["pnl"].sum()), 4)
        if "delta_pnl" in grp:
            agg["mean_delta_pnl"] = round(float(grp["delta_pnl"].mean()), 4)
            agg["median_delta_pnl"] = round(float(grp["delta_pnl"].median()), 4)
            agg["win_rate_delta_pnl"] = round(float((grp["delta_pnl"] > 0).mean()), 4)
        if "delta_inventory_adjusted_pnl" in grp:
            agg["mean_delta_inv_adj_pnl"] = round(
                float(grp["delta_inventory_adjusted_pnl"].mean()), 4
            )
        if "delta_abs_inventory_time_s" in grp:
            agg["mean_delta_abs_inventory_time_s"] = round(
                float(grp["delta_abs_inventory_time_s"].mean()), 4
            )
        if "delta_time_avg_abs_inventory" in grp:
            agg["mean_delta_time_avg_abs_inventory"] = round(
                float(grp["delta_time_avg_abs_inventory"].mean()), 6
            )
        agg_rows.append(agg)

    agg_frame = pd.DataFrame(agg_rows).sort_values(["day", "group", "arm"])
    rollup_csv = bt.RESULTS_DIR / f"{exp.stem}_{tag}_{symbol.lower()}_rollup.csv"
    agg_frame.to_csv(rollup_csv, index=False)
    print(f"Saved {rollup_csv}")
    print("Segment roll-up (independent fresh-start windows):")
    print(agg_frame.to_string(index=False))


def run_experiment(exp: Experiment, argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=f"Tick A/B: {exp.title}")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "Baseline config. Default resolves the hash-bound current "
            "operational baseline when available."
        ),
    )
    parser.add_argument(
        "--days",
        nargs="+",
        default=None,
        help="UTC dates to replay as independent fresh-start samples, e.g. 2026-05-15 2026-05-16",
    )
    parser.add_argument("--start-date", default=None, help="Optional UTC slice start, e.g. 2026-05-24")
    parser.add_argument("--end-date", default=None, help="Optional UTC slice end; date-only values are exclusive next-day")
    parser.add_argument("--arms", nargs="+", default=None)
    parser.add_argument("--tag", default=exp.default_tag)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument(
        "--engine",
        choices=("python", "cpp"),
        default="python",
        help="Replay engine for each arm. C++ is experimental and may reject unsupported params.",
    )
    parser.add_argument(
        "--window-cache-dir",
        default=None,
        help=(
            "Optional tick-window cache directory. Caches trades, variance, "
            "BBO/L2, and ML prediction arrays after quality filtering. Also "
            "available via NARROWGATE_TICK_WINDOW_CACHE_DIR."
        ),
    )
    parser.add_argument(
        "--refresh-window-cache",
        action="store_true",
        help="Rebuild cached tick windows before running arms.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=1,
        help="Replay independent fresh-start segments of N UTC days each. Default: 1 day.",
    )
    if exp.extra_cli is not None:
        exp.extra_cli(parser)
    args = parser.parse_args(argv)

    selected_days = _normalize_days(args.days)
    if not selected_days:
        raise SystemExit("Provide --days YYYY-MM-DD [...]")
    args.window_days = 1

    available_arms = exp.arm_factory(args) if exp.arm_factory is not None else exp.arms
    by_name = {arm.name: arm for arm in available_arms}
    arm_names = args.arms or [arm.name for arm in available_arms]
    unknown_arms = sorted(set(arm_names) - set(by_name))
    if unknown_arms:
        raise SystemExit(
            f"Unknown arms for {exp.key}: {unknown_arms}; choices: {sorted(by_name)}"
        )
    selected_arms = [by_name[name] for name in arm_names]
    bt.configure_symbol(args.symbol)
    base = _base_params(args.symbol, config_path=args.config)
    if args.engine == "cpp":
        base["collect_curves"] = False
    if exp.prepare_base is not None:
        base = exp.prepare_base(base, args)
    if args.window_cache_dir:
        base["_window_cache_dir"] = args.window_cache_dir
    if args.refresh_window_cache:
        base["_refresh_window_cache"] = True

    banner = " ".join(f"{key}={base.get(key)}" for key in exp.base_summary_keys)
    print(f"Base params: symbol={args.symbol.upper()} engine={args.engine} {banner}")

    rows: list[dict[str, Any]] = []
    start_ms = _parse_bound(args.start_date, is_end=False)
    end_ms = _parse_bound(args.end_date, is_end=True)
    window_days = getattr(args, "window_days", None)
    # Daily-only: load each UTC day independently.  Do not collapse to day[:7],
    # because that reintroduces cross-day cache/state coupling.
    for load_key in selected_days:
        load_window = (
            exp.window_loader(load_key, base, args)
            if exp.window_loader is not None
            else _load_window(load_key, base)
        )
        load_window = _slice_window(load_window, start_ms, end_ms)
        for seg_label, seg_start, seg_end in _segment_bounds(load_window, window_days):
            if seg_label is None:
                continue
            else:
                if selected_days and seg_label not in selected_days:
                    continue
                try:
                    window = _slice_window(load_window, seg_start, seg_end)
                except ValueError:
                    continue  # empty segment (e.g. data gap); skip
                seg_day = seg_label
            ctx = exp.window_hook(window, base, args) if exp.window_hook is not None else {}
            jobs = [(seg_day, arm) for arm in selected_arms]

            global _WORKER_BASE, _WORKER_WINDOW, _WORKER_CTX, _WORKER_EXP, _WORKER_ENGINE
            _WORKER_BASE, _WORKER_WINDOW, _WORKER_CTX, _WORKER_EXP, _WORKER_ENGINE = base, window, ctx, exp, args.engine

            workers = max(1, min(args.workers or default_backtest_workers(), len(jobs)))
            if workers == 1:
                iterator: Any = (_run_job(job) for job in jobs)
                pool = None
            else:
                print(f"Running {len(jobs)} variants for {seg_day} with {workers} workers ...")
                pool = mp.get_context("fork").Pool(workers)
                iterator = pool.imap_unordered(_run_job, jobs)

            try:
                for index, row in enumerate(iterator, 1):
                    rows.append(row)
                    print(
                        f"  [{index:02d}/{len(jobs):02d}] {row['day']} {row['arm']} "
                        f"pnl={row['pnl']:.4f} invAdj={row['inventory_adjusted_pnl']:.4f} "
                        f"avgMo={row['avg_markout']:.3f} fills/day={row['fills_per_day']:.2f} "
                        f"absInvTime={row.get('abs_inventory_time_s', 0.0):.2f}s "
                        f"avgAbsInv={row.get('time_avg_abs_inventory', 0.0):.6f} "
                        f"pnl/InvHr={row.get('pnl_per_abs_inventory_hour', 0.0):.4f} "
                        f"maxInv={row['max_inventory']:.4f}"
                    )
            except ValueError as exc:
                # In segmented mode one bad window (e.g. low historical book
                # coverage) should not abort the whole independent-sample sweep.
                if not window_days:
                    raise
                print(f"  [skip] {seg_day}: {exc}")
                continue
            finally:
                if pool is not None:
                    pool.close()
                    pool.join()

    _write_outputs(exp, rows, args.tag, args.symbol)
    if window_days:
        _write_segment_rollup(exp, rows, args.tag, args.symbol)


# ───────────────────────── experiment hooks ─────────────────────────


def _noise_metrics(result: dict[str, Any]) -> dict[str, Any]:
    fills = pd.DataFrame(result.get("_fill_trace") or [])
    empty = {
        "noise_count": 0,
        "noise_rate": 0.0,
        "buy_high_noise": 0,
        "sell_low_noise": 0,
        "avg_noise_markout_30s": 0.0,
        "avg_all_trace_markout_30s": 0.0,
        "avg_trace_age_ms": 0.0,
    }
    if fills.empty:
        return empty
    required = {"side", "window120_rank", "markout_30s", "age_ms"}
    if not required.issubset(fills.columns):
        out = dict(empty)
        out["avg_all_trace_markout_30s"] = float(fills.get("markout_30s", pd.Series(dtype=float)).mean() or 0.0)
        out["avg_trace_age_ms"] = float(fills.get("age_ms", pd.Series(dtype=float)).mean() or 0.0)
        return out
    rank = pd.to_numeric(fills["window120_rank"], errors="coerce")
    markout = pd.to_numeric(fills["markout_30s"], errors="coerce")
    buy_high = fills["side"].eq("BUY") & rank.ge(0.80) & markout.lt(0.0)
    sell_low = fills["side"].eq("SELL") & rank.le(0.20) & markout.lt(0.0)
    noise = buy_high | sell_low
    return {
        "noise_count": int(noise.sum()),
        "noise_rate": float(noise.mean()),
        "buy_high_noise": int(buy_high.sum()),
        "sell_low_noise": int(sell_low.sum()),
        "avg_noise_markout_30s": float(markout[noise].mean() if noise.any() else 0.0),
        "avg_all_trace_markout_30s": float(markout.mean() if len(markout) else 0.0),
        "avg_trace_age_ms": float(pd.to_numeric(fills["age_ms"], errors="coerce").mean()),
    }


def _noise_param_hook(params: dict[str, Any], arm: Arm, ctx: dict[str, Any]) -> None:
    params["trace_fills_max"] = max(int(params.get("trace_fills_max", 0) or 0), 50000)
    params["trace_fills_window_s"] = max(float(params.get("trace_fills_window_s", 10.0) or 10.0), 30.0)


def _calibrate_liq_baseline(window: dict[str, Any], levels: int) -> float:
    """Median near-book depth (top ``levels`` bid+ask sizes) over the L2 window.

    Matches ``strategy.quote_core._near_depth_total`` so the resulting ``liq_ref``
    is on the same scale as the runtime liquidity proxy used by the dynamic cap.
    """
    l2 = window.get("l2_data")
    if l2 is None:
        return 0.0
    bid_qty = getattr(l2, "bid_qty", None)
    ask_qty = getattr(l2, "ask_qty", None)
    if bid_qty is None or ask_qty is None:
        return 0.0
    bid_qty = np.asarray(bid_qty, dtype=float)
    ask_qty = np.asarray(ask_qty, dtype=float)
    if bid_qty.ndim != 2 or bid_qty.size == 0:
        return 0.0
    n = max(1, min(int(levels), bid_qty.shape[1], ask_qty.shape[1]))
    near = bid_qty[:, :n].sum(axis=1) + ask_qty[:, :n].sum(axis=1)
    near = near[near > 0.0]
    if near.size == 0:
        return 0.0
    return float(np.median(near))


def _spread_cap_window_hook(window: dict[str, Any], base: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    trace_levels = max(1, int(base.get("trace_book_imb_levels", 10)))
    override = getattr(args, "liq_baseline", None)
    if override is not None and override > 0.0:
        liq_baseline = float(override)
        print(f"  using provided liq_ref={liq_baseline:.3f}")
    else:
        liq_baseline = _calibrate_liq_baseline(window, trace_levels)
        print(f"  auto-calibrated liq_ref={liq_baseline:.3f} (median near-book depth)")
    if liq_baseline <= 0.0:
        print("  WARNING liq_ref<=0; liquidity arms degrade to baseline behavior.")
    return {"liq_baseline": liq_baseline}


def _spread_cap_param_hook(params: dict[str, Any], arm: Arm, ctx: dict[str, Any]) -> None:
    if arm.needs_liq_baseline:
        params["dynamic_cap_liq_baseline"] = ctx.get("liq_baseline", 0.0)


def _spread_cap_prepare_base(base: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    if not bool(base.get("dynamic_cap_enabled", False)):
        print(
            "WARNING: dynamic_cap_enabled is False in base params; the liquidity "
            "term only applies when the dynamic cap is enabled. Enabling it for this A/B."
        )
        base = dict(base)
        base["dynamic_cap_enabled"] = True
    return base


def _spread_cap_cli(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--liq-baseline",
        type=float,
        default=None,
        help="Override liq_ref; default auto-calibrates to median near-book depth per window.",
    )


def _boolean_cli(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tests",
        nargs="+",
        default=[
            "dynamic_cap_enabled",
            "symmetric_size",
            "regime.enabled",
            "ml.enabled",
        ],
        choices=[
            "dynamic_cap_enabled",
            "symmetric_size",
            "regime.enabled",
            "ml.enabled",
            "multi_market.enabled",
        ],
        help="Boolean mechanism groups to include.",
    )
    parser.add_argument(
        "--include-source-wiring-tests",
        action="store_true",
        help="Include diagnostic multi-market source wiring; not a policy arm.",
    )


def _boolean_arm_factory(args: argparse.Namespace) -> list[Arm]:
    specs = (
        ("dynamic_cap_enabled", "dynamic_cap_enabled", "param"),
        ("symmetric_size", "symmetric_size", "param"),
        ("regime.enabled", "regime_enabled", "param"),
        ("ml.enabled", "", "ml"),
        ("multi_market.enabled", "", "cross_market"),
    )
    selected = set(args.tests)
    if "multi_market.enabled" in selected and not args.include_source_wiring_tests:
        raise SystemExit(
            "multi_market.enabled is a source-wiring diagnostic. Pass "
            "--include-source-wiring-tests explicitly."
        )
    arms: list[Arm] = []
    for name, param_key, kind in specs:
        if name not in selected:
            continue
        note = (
            "Source-wiring diagnostic only; not an expected-PnL switch."
            if kind == "cross_market"
            else ""
        )
        for enabled in (False, True):
            arms.append(
                Arm(
                    name=f"{name}_{str(enabled).lower()}",
                    group=name,
                    overrides={param_key: enabled} if kind == "param" else {},
                    note=note,
                    control=not enabled,
                    ml_enabled=enabled if kind == "ml" else True,
                    cross_market_enabled=(
                        enabled if kind == "cross_market" else True
                    ),
                )
            )
    return arms


def _boolean_window_loader(
    day: str,
    params: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    window = load_tick_window_dict(
        day,
        params,
        load_ml=False,
        with_ml_cache=True,
        require_historical_bbo=True,
        cache_dir=params.get("_window_cache_dir"),
        refresh_cache=bool(params.get("_refresh_window_cache", False)),
    )
    cache = window.setdefault("ml_cache", {})
    for cross_market_enabled in (True, False):
        cache[cross_market_enabled] = bt.load_ml_predictions(
            window["trades"],
            toxicity_horizon_s=window["toxicity_horizon_s"],
            cross_market_enabled=cross_market_enabled,
        )
    return window


def _boolean_ml_data(window: dict[str, Any], arm: Arm):
    if arm.ml_enabled is False:
        return None
    return window["ml_cache"][bool(arm.cross_market_enabled)]


def _dynamic_skew_cli(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tests",
        nargs="+",
        default=[
            "book_imb_strength",
            "imbalance_asym.strength",
            "skew_strength",
            "ret_skew",
        ],
        choices=[
            "book_imb_strength",
            "imbalance_asym.strength",
            "skew_strength",
            "ret_skew",
        ],
        help="Numeric mechanism groups to include.",
    )
    parser.add_argument(
        "--ret-skew-values",
        nargs="*",
        type=float,
        default=None,
        help="Override the ret_skew grid.",
    )


def _dynamic_skew_arm_factory(args: argparse.Namespace) -> list[Arm]:
    tests = (
        (
            "book_imb_strength",
            "book_imb_strength",
            (0.05, 0.10, 0.20),
            {"book_imb_source": "depth_top"},
        ),
        (
            "imbalance_asym.strength",
            "book_imb_strength",
            (0.05, 0.10, 0.20),
            {"book_imb_source": "l2", "book_imb_levels": 20},
        ),
        ("skew_strength", "skew_strength", (0.10, 0.30, 0.50), {}),
        (
            "ret_skew",
            "ret_skew",
            tuple(args.ret_skew_values or (25.0, 100.0, 200.0)),
            {},
        ),
    )
    arms = [Arm("baseline", "baseline", {}, "Current live config.", control=True)]
    for name, param_key, values, extras in tests:
        if name not in set(args.tests):
            continue
        for value in values:
            overrides = dict(extras)
            overrides[param_key] = float(value)
            arms.append(
                Arm(
                    f"{name}_{value:g}",
                    name,
                    overrides,
                    f"Change only {name}.",
                )
            )
    return arms


def _inventory_cli(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-time", default=None)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--time-zone", default="UTC")
    parser.add_argument(
        "--ml",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable ML; default inherits the baseline config.",
    )
    parser.add_argument("--queue-base", type=float, default=5.0)
    parser.add_argument("--queue-decay", type=float, default=0.1)
    parser.add_argument(
        "--queue-ahead-mode",
        choices=["exact_level", "provider_visible_level"],
        default="exact_level",
    )
    parser.add_argument("--maker-fill-prob", type=float, default=None)
    parser.add_argument("--require-historical-bbo", action="store_true")
    parser.add_argument("--min-historical-book-coverage", type=float, default=0.90)
    parser.add_argument(
        "--asym-values",
        type=float,
        nargs="+",
        default=[0.03, 0.05, 0.08, 0.10],
    )
    parser.add_argument(
        "--fade-values",
        type=float,
        nargs="+",
        default=[0.0, 0.25, 0.50],
    )


def _inventory_prepare_base(
    base: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    prepared = load_tick_base_params(
        symbol=args.symbol,
        config_path=args.config,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=bool(args.require_historical_bbo),
        queue_base=args.queue_base,
        queue_decay=args.queue_decay,
        queue_ahead_mode=args.queue_ahead_mode,
        min_historical_book_coverage=args.min_historical_book_coverage,
        maker_fill_prob=args.maker_fill_prob,
    )
    run_ml = bool(prepared.get("ml_enabled", False)) if args.ml is None else bool(args.ml)
    prepared["ml_enabled"] = run_ml
    args._run_ml = run_ml
    if not run_ml:
        disable_ml_params(prepared)
    return prepared


def _inventory_window_loader(
    day: str,
    params: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    window = load_tick_window_dict(
        day,
        params,
        load_ml=bool(getattr(args, "_run_ml", False)),
        require_ml=False,
        require_historical_bbo=bool(args.require_historical_bbo),
        cache_dir=params.get("_window_cache_dir"),
        refresh_cache=bool(params.get("_refresh_window_cache", False)),
    )
    start_ms = bt._parse_time_filter_ms(args.start_time, args.time_zone)
    end_ms = bt._parse_time_filter_ms(args.end_time, args.time_zone)
    return slice_window(window, start_ms, end_ms)


def _inventory_arm_factory(args: argparse.Namespace) -> list[Arm]:
    arms = [
        Arm(
            "baseline",
            "inventory_control",
            {
                "inventory_asym_strength": 0.0,
                "inventory_signal_fade_strength": 0.0,
            },
            "No inventory asymmetry/fade.",
            control=True,
        )
    ]
    for asym, fade in product(args.asym_values, args.fade_values):
        if asym == 0.0 and fade == 0.0:
            continue
        arms.append(
            Arm(
                f"asym_{asym:g}_fade_{fade:g}".replace(".", "p"),
                "inventory_control",
                {
                    "inventory_asym_strength": float(asym),
                    "inventory_signal_fade_strength": float(fade),
                },
            )
        )
    return arms


# ───────────────────────── experiment registry ─────────────────────────

EXPERIMENTS: dict[str, Experiment] = {}


def _register(exp: Experiment) -> None:
    EXPERIMENTS[exp.key] = exp


_register(Experiment(
    key="spread_cap",
    title="Liquidity-Aware Spread Cap",
    stem="tick_spread_cap_ab",
    base_summary_keys=("dynamic_cap_enabled", "max_spread_bps", "dynamic_cap_alpha", "dynamic_cap_max_mult"),
    summary_extra=("cap_liq_beta", "cap_liq_baseline", "cap_min_mult"),
    window_hook=_spread_cap_window_hook,
    param_hook=_spread_cap_param_hook,
    prepare_base=_spread_cap_prepare_base,
    extra_cli=_spread_cap_cli,
    arms=[
        Arm("baseline", "baseline", {}, "Current live dynamic cap (variance-only)."),
        Arm("liq_beta_050", "liq_cap", {"dynamic_cap_liq_beta": 0.5},
            "Add liquidity term, beta=0.5 (widen-only, min_mult=1.0).", needs_liq_baseline=True),
        Arm("liq_beta_100", "liq_cap", {"dynamic_cap_liq_beta": 1.0},
            "Add liquidity term, beta=1.0 (widen-only, min_mult=1.0).", needs_liq_baseline=True),
        Arm("liq_beta_150", "liq_cap", {"dynamic_cap_liq_beta": 1.5},
            "Add liquidity term, beta=1.5 (widen-only, min_mult=1.0).", needs_liq_baseline=True),
        Arm("liq_beta_100_tighten", "liq_cap",
            {"dynamic_cap_liq_beta": 1.0, "dynamic_cap_min_mult": 0.7},
            "Liquidity term beta=1.0, allow thick-book tighten to 0.7x base.", needs_liq_baseline=True),
        Arm("liq_beta_150_tighten", "liq_cap",
            {"dynamic_cap_liq_beta": 1.5, "dynamic_cap_min_mult": 0.6},
            "Liquidity term beta=1.5, allow thick-book tighten to 0.6x base.", needs_liq_baseline=True),
    ],
))

_register(Experiment(
    key="boolean",
    title="Strict Boolean Mechanisms",
    stem="tick_boolean_ab",
    default_tag="20260519",
    arms=[],
    base_summary_keys=(
        "maker_fill_prob",
        "kappa_ratio",
        "depth_kappa_ratio",
        "max_spread_bps",
    ),
    summary_extra=(
        "avg_spread",
        "avg_final_spread",
        "final_spread_lt_100_rate",
        "cap_hit_rate",
        "gtx_rejects",
    ),
    extra_cli=_boolean_cli,
    arm_factory=_boolean_arm_factory,
    window_loader=_boolean_window_loader,
    ml_data_hook=_boolean_ml_data,
))

_register(Experiment(
    key="dynamic_skew",
    title="Dynamic Quote-Side Skew",
    stem="tick_dynamic_skew_ab",
    default_tag="20260520",
    arms=[],
    summary_extra=(
        "avg_markout_bid",
        "avg_markout_ask",
        "avg_final_spread",
        "final_spread_lt_100_rate",
        "cap_hit_rate",
        "avg_quote_skew_pct",
        "avg_abs_quote_skew_pct",
        "quote_abs_skew_gt_10_rate",
        "quote_abs_skew_gt_25_rate",
        "bid_tighter_rate",
        "ask_tighter_rate",
        "quote_balanced_rate",
        "avg_inventory",
    ),
    extra_cli=_dynamic_skew_cli,
    arm_factory=_dynamic_skew_arm_factory,
))

_register(Experiment(
    key="inventory_control",
    title="Inventory Asymmetry And Signal Fade",
    stem="tick_inventory_control_ab",
    arms=[],
    extra_cli=_inventory_cli,
    arm_factory=_inventory_arm_factory,
    prepare_base=_inventory_prepare_base,
    window_loader=_inventory_window_loader,
))

_register(Experiment(
    key="noise_guard",
    title="Noise Guard",
    stem="tick_noise_guard_ab",
    default_tag="20260527",
    base_summary_keys=("maker_fill_prob", "use_bar_pricing", "max_spread_bps",
                       "adverse_markout_threshold", "flat_unilateral_max_s"),
    delta_metrics=("noise_count", "noise_rate", "avg_noise_markout_30s"),
    summary_extra=("noise_count", "noise_rate", "buy_high_noise", "sell_low_noise",
                   "avg_noise_markout_30s", "local_extreme_guard_count", "local_extreme_pause_count",
                   "fragile_ttl_cancel_count", "adverse_pause_count"),
    extra_metrics=_noise_metrics,
    param_hook=_noise_param_hook,
    arms=[
        Arm("baseline", "baseline", {}, "Current live config."),
        Arm("thin_extreme_widen_1p8", "thin_local_extreme",
            {"local_extreme_guard_enabled": True, "local_extreme_window_s": 120.0,
             "local_extreme_rank_threshold": 0.80, "local_extreme_require_thin_depth": True,
             "local_extreme_spread_mult": 1.80, "local_extreme_pause": False},
            "Thin depth + recent high BUY / recent low SELL: widen only."),
        Arm("thin_extreme_pause", "thin_local_extreme",
            {"local_extreme_guard_enabled": True, "local_extreme_window_s": 120.0,
             "local_extreme_rank_threshold": 0.80, "local_extreme_require_thin_depth": True,
             "local_extreme_spread_mult": 1.80, "local_extreme_pause": True},
            "Thin depth + local extreme: pause the exposed side."),
        Arm("adverse_markout_4", "adverse_threshold", {"adverse_markout_threshold": 4.0},
            "Lower single adverse pause threshold from 5 to 4."),
        Arm("adverse_markout_3", "adverse_threshold", {"adverse_markout_threshold": 3.0},
            "Lower single adverse pause threshold from 5 to 3."),
        Arm("adverse_tier_2_widen_5_pause", "adverse_threshold",
            {"adverse_markout_threshold": 2.0, "adverse_markout_pause_threshold": 5.0,
             "adverse_spread_mult": 1.35},
            "Tiered guard: widen from -2 markout, pause only from -5."),
        Arm("thin_ttl_5s", "fragile_ttl",
            {"local_extreme_guard_enabled": False, "fragile_order_ttl_s": 5.0},
            "Thin-depth quotes get 5s TTL without local-extreme widening/pause."),
        Arm("thin_ttl_3s", "fragile_ttl",
            {"local_extreme_guard_enabled": False, "fragile_order_ttl_s": 3.0},
            "Thin-depth quotes get 3s TTL without local-extreme widening/pause."),
    ],
))


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in {"-h", "--help", "--list"}:
        print("Unified tick-replay A/B runner.\n")
        print("Usage: python3 models/tick_ab.py <experiment> [--days YYYY-MM-DD ...] [--arms ...] [--tag ...]\n")
        print("Available experiments:")
        for key, exp in EXPERIMENTS.items():
            arm_count = (
                "dynamic" if exp.arm_factory is not None else str(len(exp.arms))
            )
            print(f"  {key:16s} {exp.title} ({arm_count} arms)")
        return
    key = argv[0]
    if key not in EXPERIMENTS:
        raise SystemExit(f"Unknown experiment '{key}'. Choices: {', '.join(EXPERIMENTS)}")
    run_experiment(EXPERIMENTS[key], argv[1:])


if __name__ == "__main__":
    main()
