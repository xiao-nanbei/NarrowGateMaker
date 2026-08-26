#!/usr/bin/env python3
"""Run daily single-source or multi-venue consensus shadow evidence.

This panel consumes reusable per-day order-level denominator CSVs.  It keeps
external data strictly in the evidence layer: no replay outcome, quote, cancel,
or live policy is changed.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.audit.support import read_csv_table, write_csv
from research.families.f10_live_replay_attribution.audit.metrics import BboMidSeries, xmarket_ref_shadow_tables
from research.families.f10_live_replay_attribution.audit.runner import _load_bbo_mid_series


def _float(row: dict[str, Any], key: str) -> float:
    try:
        value = float(row.get(key, ""))
        return value if math.isfinite(value) else math.nan
    except (TypeError, ValueError):
        return math.nan


def _int(row: dict[str, Any], key: str) -> int:
    try:
        return int(float(row.get(key, 0) or 0))
    except (TypeError, ValueError):
        return 0


def _external_bucket(value: Any) -> str:
    text = str(value or "")
    if text.startswith("adverse_"):
        return "adverse"
    if text.startswith("favorable_"):
        return "favorable"
    if text == "neutral":
        return "neutral"
    return "missing"


def _local_absorption_state(row: dict[str, Any]) -> tuple[str, int]:
    """Low-dimensional quote-time proxy; no terminal/future fields allowed."""
    depth = _float(row, "near_depth_total")
    refresh = _float(row, "l2_book_refresh_ratio")
    cancel = _float(row, "l2_book_cancel_ratio")
    reversion = _float(row, "micro_reversion_score")
    score = sum((
        int(math.isfinite(depth) and depth >= 1.0),
        int(math.isfinite(refresh) and refresh >= 1.0 and math.isfinite(cancel) and cancel <= 0.75),
        int(math.isfinite(reversion) and reversion >= 0.35),
    ))
    return ("strong" if score == 3 else "weak" if score == 0 else "mixed"), score


def _rebuild_fill_markouts(
    orders: list[dict[str, Any]], local_bbo: BboMidSeries | None
) -> int:
    """Rebuild all fill horizons from local BBO instead of trusting stale labels."""
    if local_bbo is None or local_bbo.empty:
        return 0
    rebuilt = 0
    for row in orders:
        if not _int(row, "filled"):
            continue
        fill_ts = _float(row, "fill_ts")
        if fill_ts > 10_000_000_000:
            fill_ts /= 1000.0
        fill_px = _float(row, "avg_fill_price")
        if not (fill_ts > 0.0 and fill_px > 0.0):
            continue
        side = str(row.get("side", "")).upper()
        if side not in {"BUY", "SELL"}:
            continue
        wrote = False
        for horizon_s in (1, 5, 20, 30):
            target_ts = fill_ts + horizon_s
            idx = bisect.bisect_right(local_bbo.ts, target_ts) - 1
            if idx < 0 or target_ts - local_bbo.ts[idx] > 5.0:
                row[f"markout_{horizon_s}s_bps"] = ""
                continue
            future_mid = local_bbo.mid[idx]
            value = (
                (future_mid - fill_px) / fill_px * 10_000.0
                if side == "BUY"
                else (fill_px - future_mid) / fill_px * 10_000.0
            )
            row[f"markout_{horizon_s}s_bps"] = f"{value:.6f}"
            wrote = True
        rebuilt += int(wrote)
    return rebuilt


def _interaction_tables(enriched: list[dict[str, Any]], day: str) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in enriched:
        side = str(row.get("side", ""))
        if side not in {"BUY", "SELL"}:
            continue
        local_state, local_score = _local_absorption_state(row)
        row["local_absorption_state"] = local_state
        row["local_absorption_score"] = local_score
        for horizon_ms in (1000, 3000, 5000):
            submit_bucket = _external_bucket(row.get(f"pending_ref_{horizon_ms}ms_side_bucket"))
            grouped[(side, horizon_ms, "submit", local_state, submit_bucket)].append(row)
            if _int(row, "filled"):
                fill_bucket = _external_bucket(row.get(f"fill_pending_ref_{horizon_ms}ms_side_bucket"))
                grouped[(side, horizon_ms, "fill", local_state, fill_bucket)].append(row)

    out: list[dict[str, Any]] = []
    for (side, horizon_ms, sample_time, local_state, external_bucket), rows in sorted(grouped.items()):
        fills = [row for row in rows if _int(row, "filled")]
        labeled = [row for row in rows if math.isfinite(_float(row, "terminal_final_total_pnl_delta"))]
        item: dict[str, Any] = {
            "day": day,
            "side": side,
            "horizon_ms": horizon_ms,
            "sample_time": sample_time,
            "local_absorption_state": local_state,
            "external_side_bucket": external_bucket,
            "orders": len(rows),
            "fills": len(fills),
            "fill_rate": f"{len(fills) / len(rows) if rows else 0.0:.6f}",
            "terminal_labeled": len(labeled),
            "tail_m50_30s": sum(
                1 for row in fills if _float(row, "markout_30s_bps") <= -50.0
            ),
        }
        for horizon_s in (1, 5, 20, 30):
            values = [_float(row, f"markout_{horizon_s}s_bps") for row in fills]
            values = [value for value in values if math.isfinite(value)]
            item[f"avg_markout_{horizon_s}s_bps"] = (
                f"{sum(values) / len(values):.6f}" if values else ""
            )
        terminal = [_float(row, "terminal_final_total_pnl_delta") for row in labeled]
        item["avg_campaign_terminal_pnl"] = (
            f"{sum(terminal) / len(terminal):.6f}" if terminal else ""
        )
        repair = [_float(row, "terminal_campaign_repaired") for row in labeled]
        repair = [value for value in repair if math.isfinite(value)]
        item["campaign_repair_rate"] = f"{sum(repair) / len(repair):.6f}" if repair else ""
        out.append(item)
    return out


def _normalize_order_ts(row: dict[str, Any], key: str) -> float:
    value = _float(row, key)
    if value > 10_000_000_000:
        value /= 1000.0
    return value


def _cross_instrument_tables(
    orders: list[dict[str, Any]], day: str, state_path: Path, max_lag_s: float
) -> list[dict[str, Any]]:
    state = pd.read_parquet(state_path).sort_values("timestamp")
    state_ts = pd.to_numeric(state["timestamp"], errors="coerce") / 1000.0
    state = state.assign(_ts_s=state_ts).dropna(subset=["_ts_s"])
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    feature_cols = (
        "perp_minus_spot_bps",
        "spot_perp_agreement",
        "venue_divergence_bps",
        "spot_ret_1s_bps",
        "perp_ret_1s_bps",
    )
    timestamps = state["_ts_s"].tolist()
    records = state.to_dict("records")
    for row in orders:
        side = str(row.get("side", "")).upper()
        if side not in {"BUY", "SELL"}:
            continue
        for sample_time, key in (("submit", "timestamp"), ("fill", "fill_ts")):
            if sample_time == "fill" and not _int(row, "filled"):
                continue
            sample_ts = _normalize_order_ts(row, key)
            index = bisect.bisect_right(timestamps, sample_ts) - 1
            if index < 0 or sample_ts - timestamps[index] > max_lag_s:
                continue
            snapshot = records[index]
            state_name = str(snapshot.get("cross_instrument_state", "missing"))
            enriched = dict(row)
            for column in feature_cols:
                enriched[column] = snapshot.get(column)
            grouped[(side, sample_time, state_name)].append(enriched)

    out: list[dict[str, Any]] = []
    for (side, sample_time, state_name), rows in sorted(grouped.items()):
        fills = [row for row in rows if _int(row, "filled")]
        terminal = [
            row for row in rows if math.isfinite(_float(row, "terminal_final_total_pnl_delta"))
        ]
        item: dict[str, Any] = {
            "day": day,
            "side": side,
            "sample_time": sample_time,
            "cross_instrument_state": state_name,
            "orders": len(rows),
            "fills": len(fills),
            "fill_rate": f"{len(fills) / len(rows) if rows else 0.0:.6f}",
            "terminal_labeled": len(terminal),
            "tail_m50_30s": sum(
                1 for row in fills if _float(row, "markout_30s_bps") <= -50.0
            ),
        }
        for column in feature_cols:
            values = [_float(row, column) for row in rows]
            values = [value for value in values if math.isfinite(value)]
            item[f"avg_{column}"] = f"{sum(values) / len(values):.6f}" if values else ""
        for horizon_s in (1, 5, 20, 30):
            values = [_float(row, f"markout_{horizon_s}s_bps") for row in fills]
            values = [value for value in values if math.isfinite(value)]
            item[f"avg_markout_{horizon_s}s_bps"] = (
                f"{sum(values) / len(values):.6f}" if values else ""
            )
        pnl = [_float(row, "terminal_final_total_pnl_delta") for row in terminal]
        item["avg_campaign_terminal_pnl"] = f"{sum(pnl) / len(pnl):.6f}" if pnl else ""
        repair = [_float(row, "terminal_campaign_repaired") for row in terminal]
        repair = [value for value in repair if math.isfinite(value)]
        item["campaign_repair_rate"] = f"{sum(repair) / len(repair):.6f}" if repair else ""
        out.append(item)
    return out


def aggregate_cross_instrument(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ("side", "sample_time", "cross_instrument_state")
    neutral_by_day = {
        (str(row.get("day", "")), str(row.get("side", "")), str(row.get("sample_time", ""))): row
        for row in rows
        if str(row.get("cross_instrument_state", "")) == "neutral"
    }
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row.get(key, "")) for key in keys)].append(row)
    out: list[dict[str, Any]] = []
    for group_key, daily in sorted(grouped.items()):
        item = dict(zip(keys, group_key, strict=True))
        item.update({
            "days": len(daily),
            "orders": sum(_int(row, "orders") for row in daily),
            "fills": sum(_int(row, "fills") for row in daily),
            "terminal_labeled": sum(_int(row, "terminal_labeled") for row in daily),
            "tail_m50_30s": sum(_int(row, "tail_m50_30s") for row in daily),
        })
        item["fill_rate"] = f"{item['fills'] / item['orders'] if item['orders'] else 0.0:.6f}"
        for horizon_s in (1, 5, 20, 30):
            value = _weighted(daily, f"avg_markout_{horizon_s}s_bps", "fills")
            item[f"avg_markout_{horizon_s}s_bps"] = f"{value:.6f}" if math.isfinite(value) else ""
            paired = []
            for row in daily:
                neutral = neutral_by_day.get(
                    (str(row.get("day", "")), str(row.get("side", "")), str(row.get("sample_time", "")))
                )
                current_value = _float(row, f"avg_markout_{horizon_s}s_bps")
                neutral_value = _float(neutral or {}, f"avg_markout_{horizon_s}s_bps")
                weight = _int(row, "fills")
                if math.isfinite(current_value) and math.isfinite(neutral_value) and weight > 0:
                    paired.append((current_value - neutral_value, weight))
            total_weight = sum(weight for _, weight in paired)
            delta = (
                sum(value * weight for value, weight in paired) / total_weight
                if total_weight else math.nan
            )
            item[f"vs_neutral_markout_{horizon_s}s_bps"] = (
                f"{delta:.6f}" if math.isfinite(delta) else ""
            )
            item[f"vs_neutral_positive_days_{horizon_s}s"] = sum(
                value > 0.0 for value, _ in paired
            )
            item[f"vs_neutral_paired_days_{horizon_s}s"] = len(paired)
        terminal = _weighted(daily, "avg_campaign_terminal_pnl", "terminal_labeled")
        repair = _weighted(daily, "campaign_repair_rate", "terminal_labeled")
        item["avg_campaign_terminal_pnl"] = f"{terminal:.6f}" if math.isfinite(terminal) else ""
        item["campaign_repair_rate"] = f"{repair:.6f}" if math.isfinite(repair) else ""
        terminal_deltas = []
        repair_deltas = []
        for row in daily:
            neutral = neutral_by_day.get(
                (str(row.get("day", "")), str(row.get("side", "")), str(row.get("sample_time", "")))
            )
            current_terminal = _float(row, "avg_campaign_terminal_pnl")
            neutral_terminal = _float(neutral or {}, "avg_campaign_terminal_pnl")
            current_repair = _float(row, "campaign_repair_rate")
            neutral_repair = _float(neutral or {}, "campaign_repair_rate")
            weight = _int(row, "terminal_labeled")
            if math.isfinite(current_terminal) and math.isfinite(neutral_terminal) and weight > 0:
                terminal_deltas.append((current_terminal - neutral_terminal, weight))
            if math.isfinite(current_repair) and math.isfinite(neutral_repair) and weight > 0:
                repair_deltas.append((current_repair - neutral_repair, weight))
        for name, values in (
            ("vs_neutral_campaign_terminal_pnl", terminal_deltas),
            ("vs_neutral_campaign_repair_rate", repair_deltas),
        ):
            total_weight = sum(weight for _, weight in values)
            value = (
                sum(delta * weight for delta, weight in values) / total_weight
                if total_weight else math.nan
            )
            item[name] = f"{value:.6f}" if math.isfinite(value) else ""
            item[f"{name}_positive_days"] = sum(delta > 0.0 for delta, _ in values)
            item[f"{name}_paired_days"] = len(values)
        for horizon_s in (1, 5, 20, 30):
            item[f"positive_markout_days_{horizon_s}s"] = sum(
                _float(row, f"avg_markout_{horizon_s}s_bps") > 0.0 for row in daily
            )
        out.append(item)
    return out


def _load_filelist(path: Path) -> list[tuple[str, Path]]:
    rows: list[tuple[str, Path]] = []
    with path.expanduser().open(newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            day = str(row.get("day", ""))[:10]
            order_path = Path(str(row.get("order_level_csv", ""))).expanduser()
            if day and order_path.exists():
                rows.append((day, order_path))
    return sorted(rows)


def _run_day(
    day: str,
    order_path: Path,
    external_dir: Path,
    local_bbo_dir: Path,
    symbol: str,
    ref_symbol: str,
    threshold_bps: float,
    max_lag_s: float,
    cross_instrument_dir: Path | None,
    cross_instrument_symbol: str,
) -> dict[str, Any]:
    orders = read_csv_table(order_path)
    external = _load_bbo_mid_series(
        bbo_dir=external_dir,
        symbol=ref_symbol,
        days=[day],
        fallback_resolution="external_trade_1s_causal",
    )
    local = _load_bbo_mid_series(
        bbo_dir=local_bbo_dir,
        symbol=symbol,
        days=[day],
        fallback_resolution="local_bbo",
    )
    rebuilt_markouts = _rebuild_fill_markouts(orders, local)
    tables = xmarket_ref_shadow_tables(
        orders,
        ref_bbo=external,
        local_bbo=local,
        spot_bbo=None,
        threshold_bps=threshold_bps,
        max_lag_s=max_lag_s,
        enable_event_cancel=False,
        include_orders=True,
    )

    def daily_rows(name: str, sample_time: str) -> list[dict[str, Any]]:
        out = []
        for row in tables.get(name, []):
            if str(row.get("scope", "")) != "daily":
                continue
            item = dict(row)
            item["day"] = day
            item["sample_time"] = sample_time
            out.append(item)
        return out

    summary = dict((tables.get("summary") or [{}])[0])
    summary.update({
        "day": day,
        "order_level_csv": str(order_path),
        "external_available": int(external is not None and not external.empty),
        "local_available": int(local is not None and not local.empty),
        "fill_markouts_rebuilt_from_local_bbo": rebuilt_markouts,
    })
    cross_rows: list[dict[str, Any]] = []
    if cross_instrument_dir is not None:
        matches = sorted(
            cross_instrument_dir.glob(f"{cross_instrument_symbol}*{day}*.parquet")
        )
        if len(matches) != 1:
            raise FileNotFoundError(
                f"{cross_instrument_dir}: expected one cross-instrument file for {day}, got {len(matches)}"
            )
        cross_rows = _cross_instrument_tables(orders, day, matches[0], max_lag_s)
    return {
        "summary": summary,
        "submit_sorting": daily_rows("pending_sorting", "submit"),
        "fill_sorting": daily_rows("fill_pending_sorting", "fill"),
        "state_rollup": [{"day": day, **row} for row in tables.get("state_rollup", [])],
        "pending_rollup": [{"day": day, "sample_time": "submit", **row} for row in tables.get("pending_rollup", [])],
        "fill_pending_rollup": [
            {"day": day, "sample_time": "fill", **row}
            for row in tables.get("fill_pending_rollup", [])
        ],
        "interaction": _interaction_tables(tables.get("orders", []), day),
        "cross_instrument": cross_rows,
    }


def _weighted(rows: list[dict[str, Any]], value_col: str, weight_col: str) -> float:
    pairs = [(_float(row, value_col), _int(row, weight_col)) for row in rows]
    pairs = [(value, weight) for value, weight in pairs if math.isfinite(value) and weight > 0]
    total = sum(weight for _, weight in pairs)
    return sum(value * weight for value, weight in pairs) / total if total else math.nan


def aggregate_sorting(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("side", "")), _int(row, "horizon_ms"), str(row.get("sample_time", "")))].append(row)
    out: list[dict[str, Any]] = []
    for (side, horizon_ms, sample_time), daily in sorted(grouped.items()):
        item: dict[str, Any] = {
            "side": side,
            "horizon_ms": horizon_ms,
            "sample_time": sample_time,
            "days": len(daily),
            "support_days": sum(_int(row, "support_pass") for row in daily),
        }
        for bucket in ("adverse", "neutral", "favorable"):
            item[f"{bucket}_orders"] = sum(_int(row, f"{bucket}_orders") for row in daily)
            item[f"{bucket}_fills"] = sum(_int(row, f"{bucket}_fills") for row in daily)
        for horizon_s in (1, 5, 20, 30):
            for bucket in ("adverse", "neutral", "favorable"):
                value = _weighted(
                    daily,
                    f"{bucket}_avg_markout_{horizon_s}s_bps",
                    f"{bucket}_fills",
                )
                item[f"{bucket}_avg_markout_{horizon_s}s_bps"] = (
                    f"{value:.6f}" if math.isfinite(value) else ""
                )
            adverse = _float(item, f"adverse_avg_markout_{horizon_s}s_bps")
            neutral = _float(item, f"neutral_avg_markout_{horizon_s}s_bps")
            favorable = _float(item, f"favorable_avg_markout_{horizon_s}s_bps")
            item[f"fav_minus_adv_markout_{horizon_s}s_bps"] = (
                f"{favorable - adverse:.6f}" if math.isfinite(favorable) and math.isfinite(adverse) else ""
            )
            item[f"fav_minus_neutral_markout_{horizon_s}s_bps"] = (
                f"{favorable - neutral:.6f}" if math.isfinite(favorable) and math.isfinite(neutral) else ""
            )
            item[f"sorting_pass_days_{horizon_s}s"] = sum(
                _int(row, f"sorting_pass_{horizon_s}s") for row in daily
            )
        for bucket in ("adverse", "favorable"):
            value = _weighted(daily, f"{bucket}_avg_campaign_terminal_pnl", f"{bucket}_orders")
            item[f"{bucket}_avg_campaign_terminal_pnl"] = f"{value:.6f}" if math.isfinite(value) else ""
        out.append(item)
    return out


def aggregate_interactions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ("side", "horizon_ms", "sample_time", "local_absorption_state", "external_side_bucket")
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row.get(key, "")) for key in keys)].append(row)
    out: list[dict[str, Any]] = []
    for group_key, daily in sorted(grouped.items()):
        item = dict(zip(keys, group_key, strict=True))
        item.update({
            "days": len(daily),
            "orders": sum(_int(row, "orders") for row in daily),
            "fills": sum(_int(row, "fills") for row in daily),
            "terminal_labeled": sum(_int(row, "terminal_labeled") for row in daily),
            "tail_m50_30s": sum(_int(row, "tail_m50_30s") for row in daily),
        })
        item["fill_rate"] = f"{item['fills'] / item['orders'] if item['orders'] else 0.0:.6f}"
        for horizon_s in (1, 5, 20, 30):
            value = _weighted(daily, f"avg_markout_{horizon_s}s_bps", "fills")
            item[f"avg_markout_{horizon_s}s_bps"] = f"{value:.6f}" if math.isfinite(value) else ""
        terminal = _weighted(daily, "avg_campaign_terminal_pnl", "terminal_labeled")
        repair = _weighted(daily, "campaign_repair_rate", "terminal_labeled")
        item["avg_campaign_terminal_pnl"] = f"{terminal:.6f}" if math.isfinite(terminal) else ""
        item["campaign_repair_rate"] = f"{repair:.6f}" if math.isfinite(repair) else ""
        out.append(item)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filelist", type=Path, required=True)
    parser.add_argument("--external-dir", type=Path, required=True)
    parser.add_argument("--external-label", default="external")
    parser.add_argument("--cross-instrument-dir", type=Path)
    parser.add_argument(
        "--cross-instrument-symbol",
        default=None,
        help="Symbol prefix inside --cross-instrument-dir; defaults to --ref-symbol",
    )
    parser.add_argument("--local-bbo-dir", type=Path, required=True)
    parser.add_argument("--out-prefix", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--ref-symbol", default="BTCUSDT")
    parser.add_argument("--threshold-bps", type=float, default=1.0)
    parser.add_argument("--max-lag-s", type=float, default=5.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-days", type=int, default=0)
    args = parser.parse_args()

    entries = _load_filelist(args.filelist)
    if args.max_days > 0:
        entries = entries[: args.max_days]
    if not entries:
        raise SystemExit("empty order-level filelist")
    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                _run_day,
                day,
                order_path,
                args.external_dir,
                args.local_bbo_dir,
                args.symbol,
                args.ref_symbol,
                args.threshold_bps,
                args.max_lag_s,
                args.cross_instrument_dir,
                args.cross_instrument_symbol or args.ref_symbol,
            ): day
            for day, order_path in entries
        }
        for index, future in enumerate(as_completed(futures), 1):
            day = futures[future]
            try:
                result = future.result()
                results.append(result)
                status = (result.get("summary") or {}).get("status", "unknown")
                print(f"[{index:03d}/{len(entries):03d}] {day} {status}", flush=True)
            except Exception as exc:
                failures.append({"day": day, "error": str(exc)})
                print(f"[{index:03d}/{len(entries):03d}] {day} ERROR {exc}", flush=True)

    summaries = sorted((result["summary"] for result in results), key=lambda row: str(row.get("day", "")))
    submit = [row for result in results for row in result["submit_sorting"]]
    fill = [row for result in results for row in result["fill_sorting"]]
    state = [row for result in results for row in result["state_rollup"]]
    pending = [row for result in results for row in result["pending_rollup"]]
    fill_pending = [row for result in results for row in result["fill_pending_rollup"]]
    interaction_daily = [row for result in results for row in result["interaction"]]
    cross_daily = [row for result in results for row in result["cross_instrument"]]
    aggregate = aggregate_sorting(submit + fill)
    interaction_aggregate = aggregate_interactions(interaction_daily)
    cross_aggregate = aggregate_cross_instrument(cross_daily)

    args.out_prefix.parent.mkdir(parents=True, exist_ok=True)
    write_csv(args.out_prefix.with_suffix(".source_daily.csv"), summaries)
    write_csv(args.out_prefix.with_suffix(".submit_sorting_daily.csv"), sorted(submit, key=lambda r: (r["day"], r["side"], int(r["horizon_ms"]))))
    write_csv(args.out_prefix.with_suffix(".fill_sorting_daily.csv"), sorted(fill, key=lambda r: (r["day"], r["side"], int(r["horizon_ms"]))))
    write_csv(args.out_prefix.with_suffix(".sorting_aggregate.csv"), aggregate)
    write_csv(args.out_prefix.with_suffix(".state_daily.csv"), state)
    write_csv(args.out_prefix.with_suffix(".pending_daily.csv"), pending)
    write_csv(args.out_prefix.with_suffix(".fill_pending_daily.csv"), fill_pending)
    write_csv(args.out_prefix.with_suffix(".interaction_daily.csv"), interaction_daily)
    write_csv(args.out_prefix.with_suffix(".interaction_aggregate.csv"), interaction_aggregate)
    write_csv(args.out_prefix.with_suffix(".cross_instrument_daily.csv"), cross_daily)
    write_csv(args.out_prefix.with_suffix(".cross_instrument_aggregate.csv"), cross_aggregate)
    write_csv(args.out_prefix.with_suffix(".failures.csv"), failures)
    metadata = {
        "status": "ok" if not failures and len(results) == len(entries) else "incomplete",
        "days_requested": len(entries),
        "days_completed": len(results),
        "failures": len(failures),
        "external_dir": str(args.external_dir),
        "local_bbo_dir": str(args.local_bbo_dir),
        "threshold_bps": args.threshold_bps,
        "max_lag_s": args.max_lag_s,
        "event_cancel_enabled": False,
        "external_label": args.external_label,
        "cross_instrument_dir": str(args.cross_instrument_dir or ""),
        "cross_instrument_symbol": args.cross_instrument_symbol or args.ref_symbol,
        "note": (
            f"{args.external_label} trade-time shadow evidence only; "
            "no receive-time cancel or policy inference."
        ),
    }
    args.out_prefix.with_suffix(".json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, sort_keys=True))
    return 0 if metadata["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
