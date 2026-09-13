#!/usr/bin/env python3
"""Unified audit runner for live/replay evidence.

This is the canonical entry for routine evidence reports.  Prototype scripts
may still exist for deep research, but shared timestamp/side/session/campaign
and gate metrics should be added here first.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Optional, Union

from models.audit.support import (
    default_live_paths,
    parse_ts,
    read_csv_rows,
    read_csv_table,
    render_report,
    utc_text,
    write_csv,
)
from research.families.f10_live_replay_attribution.audit.metrics import (
    BboMidSeries,
    bucket_evidence_rows,
    bucket_evidence_summary,
    attach_campaign_labels_to_orders,
    build_campaigns,
    campaign_label_rows,
    campaign_label_summary,
    campaign_rows,
    campaign_policy_blocked_fill_rows,
    campaign_policy_blocked_fill_summary,
    campaign_policy_replay_rows,
    campaign_policy_replay_summary,
    campaign_summary,
    daily_gate_rows,
    fill_summary,
    inventory_shadow_summary,
    live_order_daily_rows,
    live_replay_baseline_compare_rows,
    live_replay_baseline_compare_summary,
    local_liquidity_mechanism_summary,
    local_liquidity_mechanism_tables,
    null_baseline_summary,
    null_baseline_tables,
    order_level_rows,
    order_level_knob_shadow_rows,
    order_level_score_audit_summary,
    order_level_score_daily_rows,
    order_level_score_sanity_rows,
    order_level_score_summary,
    order_level_summary,
    quote_decision_summary,
    reducing_cooldown_replay_rows,
    reducing_cooldown_replay_summary,
    replay_order_level_rows,
    shadow_avoidance_evidence_rows,
    shadow_avoidance_summary,
    spot_pending_shadow_summary,
    spot_pending_shadow_tables,
    toxic_risk_evidence_rows,
    toxic_risk_summary,
    trades_from_rows,
    xmarket_ref_shadow_summary,
    xmarket_ref_shadow_tables,
)
from research.system_engineering.audit.receive_time_tape import (
    DEFAULT_HORIZONS_MS,
    expand_inputs as expand_market_tape_inputs,
    latency_distribution as market_tape_latency_distribution,
    leader_survival as market_tape_leader_survival,
    load_book_series as load_market_tape_book_series,
)


def _read_optional(path: Optional[Path], start_ts: float, end_ts: float) -> list[dict[str, Any]]:
    if path is None:
        return []
    return read_csv_rows(path, start_ts=start_ts, end_ts=end_ts)


def _days_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    days = sorted({str(r.get("day", "")).strip() for r in rows if str(r.get("day", "")).strip()})
    if days:
        return days
    out: set[str] = set()
    for row in rows:
        ts_text = row.get("timestamp") or row.get("submit_ts") or row.get("quote_ts") or row.get("fill_ts")
        ts = parse_ts(ts_text)
        if ts > 10_000_000_000:
            ts /= 1000.0
        if ts > 0:
            from datetime import datetime, timezone

            out.add(datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d"))
    return sorted(out)


def _first_existing_column(columns: set[str], candidates: tuple[str, ...]) -> str:
    for col in candidates:
        if col in columns:
            return col
    return ""


def _load_bbo_mid_series(
    *,
    bbo_dir: Optional[Path],
    symbol: str,
    days: list[str],
    fallback_resolution: str,
) -> Optional[BboMidSeries]:
    if bbo_dir is None:
        return None
    bbo_dir = bbo_dir.expanduser().resolve()
    if not bbo_dir.exists():
        return None
    try:
        import numpy as np
        import pandas as pd
    except Exception as exc:  # pragma: no cover - dependency failure is reported as missing data
        print(f"[WARN] pandas/numpy unavailable for BBO loading: {exc}")
        return None

    frames = []
    for day in days:
        candidates = [
            bbo_dir / f"{symbol}-bbo-{day}.parquet",
            bbo_dir / f"{symbol}-bookTicker-{day}.parquet",
            bbo_dir / f"{symbol}-bookticker-{day}.parquet",
            bbo_dir / f"{symbol}-1s-{day}.parquet",
            bbo_dir / f"{symbol}-bars-1s-{day}.parquet",
        ]
        if not any(p.exists() for p in candidates):
            candidates.extend(sorted(bbo_dir.glob(f"{symbol}*{day}*.parquet")))
        path = next((p for p in candidates if p.exists()), None)
        if path is None:
            continue
        try:
            frame = pd.read_parquet(path)
        except Exception as exc:
            print(f"[WARN] failed to read BBO parquet {path}: {exc}")
            continue
        if frame.empty:
            continue
        cols = set(map(str, frame.columns))
        bid_col = _first_existing_column(cols, ("best_bid", "bid_price", "bid", "b"))
        ask_col = _first_existing_column(cols, ("best_ask", "ask_price", "ask", "a"))
        price_col = _first_existing_column(cols, ("close", "vwap", "mid", "price"))
        if (not bid_col or not ask_col) and not price_col:
            print(f"[WARN] price parquet missing bid/ask or close-like columns: {path}")
            continue
        if "timestamp" in frame.columns:
            raw_ts = frame["timestamp"]
        else:
            raw_ts = frame.index.to_series(index=frame.index)
        ts = pd.to_numeric(raw_ts, errors="coerce")
        if ts.isna().all():
            ts = pd.to_datetime(raw_ts, utc=True, errors="coerce").astype("int64") / 1e9
        ts_arr = ts.to_numpy(dtype="float64", copy=False)
        finite = np.isfinite(ts_arr)
        if not finite.any():
            continue
        # Timestamp columns in this project are usually milliseconds.  Convert
        # only when the magnitude clearly exceeds Unix seconds.
        if np.nanmedian(ts_arr[finite]) > 10_000_000_000:
            ts_arr = ts_arr / 1000.0
        if bid_col and ask_col:
            bid = pd.to_numeric(frame[bid_col], errors="coerce").to_numpy(dtype="float64", copy=False)
            ask = pd.to_numeric(frame[ask_col], errors="coerce").to_numpy(dtype="float64", copy=False)
            mid = (bid + ask) / 2.0
            keep = finite & np.isfinite(mid) & (bid > 0.0) & (ask > 0.0) & (ask >= bid)
        else:
            mid = pd.to_numeric(frame[price_col], errors="coerce").to_numpy(dtype="float64", copy=False)
            keep = finite & np.isfinite(mid) & (mid > 0.0)
        if keep.any():
            frames.append((ts_arr[keep], mid[keep]))
    if not frames:
        return None
    ts_all = np.concatenate([x[0] for x in frames])
    mid_all = np.concatenate([x[1] for x in frames])
    order = np.argsort(ts_all, kind="mergesort")
    ts_sorted = ts_all[order]
    mid_sorted = mid_all[order]
    unique_keep = np.ones(len(ts_sorted), dtype=bool)
    unique_keep[1:] = ts_sorted[1:] != ts_sorted[:-1]
    ts_sorted = ts_sorted[unique_keep]
    mid_sorted = mid_sorted[unique_keep]
    if len(ts_sorted) > 2:
        diffs = np.diff(ts_sorted)
        median_step = float(np.nanmedian(diffs[np.isfinite(diffs)])) if np.isfinite(diffs).any() else 0.0
    else:
        median_step = 0.0
    resolution = fallback_resolution
    if median_step >= 0.8:
        resolution = "1s_snapshot"
    elif median_step >= 0.08:
        resolution = "100ms_snapshot"
    elif median_step > 0.0:
        resolution = f"{median_step:.3f}s_snapshot"
    return BboMidSeries(tuple(float(x) for x in ts_sorted), tuple(float(x) for x in mid_sorted), resolution=resolution)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--log-dir", type=Path, default=None)
    parser.add_argument("--trades", type=Path, default=None)
    parser.add_argument("--order-outcomes", type=Path, default=None)
    parser.add_argument("--quote-decisions", type=Path, default=None)
    parser.add_argument("--inventory-shadow", type=Path, default=None)
    parser.add_argument("--summary-csv", type=Path, default=None)
    parser.add_argument("--replay-daily-csv", type=Path, default=None)
    parser.add_argument("--toxic-model-compare", type=Path, default=None)
    parser.add_argument("--toxic-order-aggregate", type=Path, default=None)
    parser.add_argument("--toxic-shadow-avoidance", type=Path, default=None)
    parser.add_argument("--shadow-candidates", type=Path, default=None)
    parser.add_argument("--shadow-daily", type=Path, default=None)
    parser.add_argument("--bucket-research-clues", type=Path, default=None)
    parser.add_argument("--bucket-daily-support", type=Path, default=None)
    parser.add_argument("--order-level-csv", type=Path, default=None)
    parser.add_argument("--replay-orders-csv", type=Path, default=None)
    parser.add_argument("--replay-fills-csv", type=Path, default=None)
    parser.add_argument("--local-liquidity-min-fills", type=int, default=30)
    parser.add_argument("--local-liquidity-min-daily-fills", type=int, default=5)
    parser.add_argument("--local-liquidity-holding-budget-s", type=float, default=20.0)
    parser.add_argument("--local-liquidity-max-xmarket-adverse-rate", type=float, default=0.25)
    parser.add_argument("--ref-symbol", default="BTCUSDT")
    parser.add_argument("--ref-bbo-dir", type=Path, default=None)
    parser.add_argument("--local-bbo-dir", type=Path, default=None)
    parser.add_argument("--spot-bbo-dir", type=Path, default=None)
    parser.add_argument("--xmarket-threshold-bps", type=float, default=1.0)
    parser.add_argument("--xmarket-cancel-threshold-bps", type=float, default=1.0)
    parser.add_argument("--xmarket-max-lag-s", type=float, default=5.0)
    parser.add_argument("--null-random-trials", type=int, default=64)
    parser.add_argument("--null-random-seed", type=int, default=20260706)
    parser.add_argument("--market-tape-input", action="append", default=[])
    parser.add_argument("--market-tape-local-market-id", default="binance:perp:BTCUSDT")
    parser.add_argument("--market-tape-external-market-id", action="append", default=[])
    parser.add_argument("--market-tape-lookback-ms", type=int, default=100)
    parser.add_argument(
        "--market-tape-horizons-ms",
        default=",".join(map(str, DEFAULT_HORIZONS_MS)),
    )
    parser.add_argument("--market-tape-shock-threshold-bps", type=float, default=0.25)
    parser.add_argument("--market-tape-max-book-age-ms", type=int, default=2_000)
    parser.add_argument(
        "--xmarket-ref-write-orders",
        action="store_true",
        help="Also write per-order xmarket tags; this can be very large on retained-all panels.",
    )
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument(
        "--reports",
        default="campaign,fill_selection,quote_decisions,inventory_shadow,daily_gate",
        help="Comma-separated report names.",
    )
    parser.add_argument("--out-prefix", type=Path, required=True)
    args = parser.parse_args()

    start_ts = parse_ts(args.start) if args.start else 0.0
    end_ts = parse_ts(args.end) if args.end else 0.0
    paths = default_live_paths(args.log_dir) if args.log_dir else {}
    trades_path = args.trades or paths.get("trades")
    order_path = args.order_outcomes or paths.get("order_outcomes")
    quote_path = args.quote_decisions or paths.get("quote_decisions")
    shadow_path = args.inventory_shadow or paths.get("inventory_campaign_shadow")
    sell_resiliency_path = paths.get("sell_resiliency_shadow")
    requested = {x.strip() for x in args.reports.split(",") if x.strip()}

    trades_raw = _read_optional(trades_path, start_ts, end_ts)
    order_rows = _read_optional(order_path, start_ts, end_ts)
    quote_rows = _read_optional(quote_path, start_ts, end_ts)
    shadow_rows = _read_optional(shadow_path, start_ts, end_ts)
    sell_resiliency_rows = _read_optional(sell_resiliency_path, start_ts, end_ts)
    summary_rows = _read_optional(args.summary_csv, 0.0, 0.0) if args.summary_csv else []
    replay_daily_rows = read_csv_table(args.replay_daily_csv) if args.replay_daily_csv else []
    toxic_model_rows = read_csv_table(args.toxic_model_compare) if args.toxic_model_compare else []
    toxic_order_rows = read_csv_table(args.toxic_order_aggregate) if args.toxic_order_aggregate else []
    toxic_shadow_rows = read_csv_table(args.toxic_shadow_avoidance) if args.toxic_shadow_avoidance else []
    shadow_candidate_rows = read_csv_table(args.shadow_candidates) if args.shadow_candidates else []
    shadow_daily_rows = read_csv_table(args.shadow_daily) if args.shadow_daily else []
    bucket_research_rows = read_csv_table(args.bucket_research_clues) if args.bucket_research_clues else []
    bucket_daily_rows = read_csv_table(args.bucket_daily_support) if args.bucket_daily_support else []
    order_level_input_rows = read_csv_table(args.order_level_csv) if args.order_level_csv else []
    replay_order_rows = read_csv_table(args.replay_orders_csv) if args.replay_orders_csv else []
    replay_fill_rows = read_csv_table(args.replay_fills_csv) if args.replay_fills_csv else []
    trades = trades_from_rows(trades_raw)

    sections: list[tuple[str, Union[dict[str, Any], list[dict[str, Any]]]]] = []
    outputs: dict[str, str] = {}
    order_table: list[dict[str, Any]] = []
    campaign_label_table: list[dict[str, Any]] = []

    if "campaign" in requested or "campaign_labels" in requested:
        campaigns = build_campaigns(trades)
        if "campaign" in requested:
            camp_rows = campaign_rows(campaigns)
            sections.append(("Campaign Summary", campaign_summary(campaigns)))
            sections.append(("Campaign Rows", camp_rows[-20:]))
            csv_path = args.out_prefix.with_suffix(".campaigns.csv")
            write_csv(csv_path, camp_rows)
            outputs["campaigns_csv"] = str(csv_path)
        if "campaign_labels" in requested:
            campaign_label_table = campaign_label_rows(campaigns)
            sections.append(("Campaign Label Summary", campaign_label_summary(campaign_label_table)))
            sections.append(("Campaign Label Rows", campaign_label_table[-40:]))
            label_path = args.out_prefix.with_suffix(".campaign_labels.csv")
            write_csv(label_path, campaign_label_table)
            outputs["campaign_labels_csv"] = str(label_path)

    if "campaign_policy_replay" in requested:
        replay_rows = campaign_policy_replay_rows(trades)
        blocked_rows = campaign_policy_blocked_fill_rows(trades)
        sections.append(("Campaign Policy Replay Summary", campaign_policy_replay_summary(replay_rows)))
        sections.append(("Campaign Policy Replay Rows", replay_rows))
        sections.append(("Campaign Policy Blocked Fill Summary", campaign_policy_blocked_fill_summary(blocked_rows)))
        sections.append(("Campaign Policy Blocked Fill Rows", blocked_rows[-80:]))
        csv_path = args.out_prefix.with_suffix(".campaign_policy_replay.csv")
        blocked_path = args.out_prefix.with_suffix(".campaign_policy_blocked_fills.csv")
        write_csv(csv_path, replay_rows)
        write_csv(blocked_path, blocked_rows)
        outputs["campaign_policy_replay_csv"] = str(csv_path)
        outputs["campaign_policy_blocked_fills_csv"] = str(blocked_path)

    if "reducing_cooldown_replay" in requested:
        cooldown_rows = reducing_cooldown_replay_rows(trades)
        sections.append(("Reducing Cooldown Replay Summary", reducing_cooldown_replay_summary(cooldown_rows)))
        sections.append(("Reducing Cooldown Replay Rows", cooldown_rows))
        csv_path = args.out_prefix.with_suffix(".reducing_cooldown_replay.csv")
        write_csv(csv_path, cooldown_rows)
        outputs["reducing_cooldown_replay_csv"] = str(csv_path)

    if "fill_selection" in requested:
        sections.append(("Fill / Order Summary", fill_summary(trades, order_rows)))

    if "quote_decisions" in requested:
        sections.append(("Quote Decision Summary", quote_decision_summary(quote_rows)))

    if "inventory_shadow" in requested:
        sections.append(("Inventory Campaign Shadow Summary", inventory_shadow_summary(shadow_rows)))

    if "order_level" in requested:
        if replay_order_rows:
            order_table = replay_order_level_rows(
                replay_order_rows=replay_order_rows,
                replay_fill_rows=replay_fill_rows,
            )
        else:
            order_table = order_level_rows(
                order_rows=order_rows,
                quote_rows=quote_rows,
                inventory_shadow_rows=shadow_rows,
                sell_resiliency_rows=sell_resiliency_rows,
            )
        if campaign_label_table:
            order_table = attach_campaign_labels_to_orders(order_table, campaign_label_table)
        score_rows = order_level_score_summary(order_table)
        sections.append(("Order-Level Training Table Summary", order_level_summary(order_table)))
        sections.append(("Order-Level Explainable Score Summary", score_rows))
        csv_path = args.out_prefix.with_suffix(".order_level.csv")
        score_path = args.out_prefix.with_suffix(".order_level_scores.csv")
        write_csv(csv_path, order_table)
        write_csv(score_path, score_rows)
        outputs["order_level_csv"] = str(csv_path)
        outputs["order_level_scores_csv"] = str(score_path)

    if "order_level_score_audit" in requested:
        if not order_table:
            order_table = order_level_input_rows
        if not order_table and replay_order_rows:
            order_table = replay_order_level_rows(
                replay_order_rows=replay_order_rows,
                replay_fill_rows=replay_fill_rows,
            )
        if not order_table and order_rows:
            order_table = order_level_rows(
                order_rows=order_rows,
                quote_rows=quote_rows,
                inventory_shadow_rows=shadow_rows,
                sell_resiliency_rows=sell_resiliency_rows,
            )
        if campaign_label_table:
            order_table = attach_campaign_labels_to_orders(order_table, campaign_label_table)
        sanity_rows = order_level_score_sanity_rows(order_table)
        daily_rows = order_level_score_daily_rows(order_table)
        knob_rows = order_level_knob_shadow_rows(order_table)
        sections.append(("Order-Level Score Audit Summary", order_level_score_audit_summary(sanity_rows, daily_rows, knob_rows)))
        sections.append(("Order-Level Score Sanity", sanity_rows))
        sections.append(("Order-Level Knob Shadow Rules", knob_rows[:80]))
        sanity_path = args.out_prefix.with_suffix(".order_level_score_sanity.csv")
        daily_path = args.out_prefix.with_suffix(".order_level_score_daily.csv")
        knob_path = args.out_prefix.with_suffix(".order_level_knob_shadow.csv")
        write_csv(sanity_path, sanity_rows)
        write_csv(daily_path, daily_rows)
        write_csv(knob_path, knob_rows)
        outputs["order_level_score_sanity_csv"] = str(sanity_path)
        outputs["order_level_score_daily_csv"] = str(daily_path)
        outputs["order_level_knob_shadow_csv"] = str(knob_path)

    if "daily_gate" in requested and summary_rows:
        gate_rows = daily_gate_rows(summary_rows)
        sections.append(("Daily Gate Rows", gate_rows))
        csv_path = args.out_prefix.with_suffix(".daily_gate.csv")
        write_csv(csv_path, gate_rows)
        outputs["daily_gate_csv"] = str(csv_path)

    if "live_replay_baseline_compare" in requested:
        live_daily = live_order_daily_rows(order_rows=order_rows, quote_rows=quote_rows)
        compare_rows = live_replay_baseline_compare_rows(
            live_daily_rows=live_daily,
            replay_daily_rows=replay_daily_rows,
        )
        sections.append(("Live Maker Daily Summary", live_daily))
        sections.append(("Live/Replay Baseline Compare Summary", live_replay_baseline_compare_summary(compare_rows)))
        sections.append(("Live/Replay Baseline Compare Rows", compare_rows))
        live_path = args.out_prefix.with_suffix(".live_daily_orders.csv")
        compare_path = args.out_prefix.with_suffix(".live_replay_baseline_compare.csv")
        write_csv(live_path, live_daily)
        write_csv(compare_path, compare_rows)
        outputs["live_daily_orders_csv"] = str(live_path)
        outputs["live_replay_baseline_compare_csv"] = str(compare_path)

    if "toxic_risk" in requested:
        toxic_rows = toxic_risk_evidence_rows(
            model_compare_rows=toxic_model_rows,
            order_aggregate_rows=toxic_order_rows,
            shadow_avoidance_rows=toxic_shadow_rows,
        )
        sections.append(("Toxic-Risk Evidence Summary", toxic_risk_summary(toxic_rows)))
        sections.append(("Toxic-Risk Evidence Rows", toxic_rows[:80]))
        csv_path = args.out_prefix.with_suffix(".toxic_risk.csv")
        write_csv(csv_path, toxic_rows)
        outputs["toxic_risk_csv"] = str(csv_path)

    if "shadow_avoidance" in requested:
        shadow_evidence = shadow_avoidance_evidence_rows(
            candidates_rows=shadow_candidate_rows,
            daily_rows=shadow_daily_rows,
        )
        sections.append(("Shadow Avoidance Evidence Summary", shadow_avoidance_summary(shadow_evidence)))
        sections.append(("Shadow Avoidance Evidence Rows", shadow_evidence[:80]))
        csv_path = args.out_prefix.with_suffix(".shadow_avoidance.csv")
        write_csv(csv_path, shadow_evidence)
        outputs["shadow_avoidance_csv"] = str(csv_path)

    if "bucket_evidence" in requested:
        bucket_rows = bucket_evidence_rows(
            research_clue_rows=bucket_research_rows,
            daily_support_rows=bucket_daily_rows,
        )
        sections.append(("Bucket Evidence Summary", bucket_evidence_summary(bucket_rows)))
        sections.append(("Bucket Evidence Rows", bucket_rows[:80]))
        csv_path = args.out_prefix.with_suffix(".bucket_evidence.csv")
        write_csv(csv_path, bucket_rows)
        outputs["bucket_evidence_csv"] = str(csv_path)

    if "local_liquidity_mechanism" in requested:
        if not order_table:
            order_table = order_level_input_rows
        if not order_table and replay_order_rows:
            order_table = replay_order_level_rows(
                replay_order_rows=replay_order_rows,
                replay_fill_rows=replay_fill_rows,
            )
        if not order_table and order_rows:
            order_table = order_level_rows(
                order_rows=order_rows,
                quote_rows=quote_rows,
                inventory_shadow_rows=shadow_rows,
                sell_resiliency_rows=sell_resiliency_rows,
            )
        liquidity_tables = local_liquidity_mechanism_tables(
            order_table,
            min_fills=args.local_liquidity_min_fills,
            min_daily_fills=args.local_liquidity_min_daily_fills,
            holding_budget_s=args.local_liquidity_holding_budget_s,
            max_xmarket_adverse_rate=args.local_liquidity_max_xmarket_adverse_rate,
        )
        sections.append(("Local Liquidity Mechanism Summary", local_liquidity_mechanism_summary(liquidity_tables)))
        sections.append(("Local Liquidity Candidates", liquidity_tables["candidates"][:80]))
        for suffix, rows in liquidity_tables.items():
            csv_path = args.out_prefix.with_suffix(f".local_liquidity_{suffix}.csv")
            write_csv(csv_path, rows)
            outputs[f"local_liquidity_{suffix}_csv"] = str(csv_path)

    if "null_baseline" in requested:
        if not order_table:
            order_table = order_level_input_rows
        if not order_table and replay_order_rows:
            order_table = replay_order_level_rows(
                replay_order_rows=replay_order_rows,
                replay_fill_rows=replay_fill_rows,
            )
        if not order_table and order_rows:
            order_table = order_level_rows(
                order_rows=order_rows,
                quote_rows=quote_rows,
                inventory_shadow_rows=shadow_rows,
                sell_resiliency_rows=sell_resiliency_rows,
            )
        null_tables = null_baseline_tables(
            order_table,
            random_trials=args.null_random_trials,
            random_seed=args.null_random_seed,
        )
        sections.append(("Null Baseline Summary", null_baseline_summary(null_tables)))
        sections.append(("Null Baseline Aggregate", null_tables.get("aggregate", [])[:80]))
        sections.append(("Null Baseline Condition Summary", null_tables.get("condition_summary", [])[:120]))
        sections.append(("Current Actual Daily", null_tables.get("current_daily", [])[:80]))
        sections.append(("Random Opportunity Null Daily", null_tables.get("random_daily", [])[:80]))
        sections.append(("Oracle Upper Bound Daily", null_tables.get("oracle_daily", [])[:80]))
        sections.append(("Positive Intersection Daily", null_tables.get("positive_intersection_daily", [])[:80]))
        sections.append(("Condition Daily", null_tables.get("condition_daily", [])[:120]))
        for suffix, rows in null_tables.items():
            csv_path = args.out_prefix.with_suffix(f".null_baseline_{suffix}.csv")
            write_csv(csv_path, rows)
            outputs[f"null_baseline_{suffix}_csv"] = str(csv_path)

    if "xmarket_ref_shadow" in requested:
        if not order_table:
            order_table = order_level_input_rows
        if not order_table and replay_order_rows:
            order_table = replay_order_level_rows(
                replay_order_rows=replay_order_rows,
                replay_fill_rows=replay_fill_rows,
            )
        if not order_table and order_rows:
            order_table = order_level_rows(
                order_rows=order_rows,
                quote_rows=quote_rows,
                inventory_shadow_rows=shadow_rows,
                sell_resiliency_rows=sell_resiliency_rows,
            )
        days = _days_from_rows(order_table)
        ref_bbo = _load_bbo_mid_series(
            bbo_dir=args.ref_bbo_dir,
            symbol=args.ref_symbol,
            days=days,
            fallback_resolution="unknown_ref_bbo",
        )
        local_bbo = _load_bbo_mid_series(
            bbo_dir=args.local_bbo_dir,
            symbol=args.symbol,
            days=days,
            fallback_resolution="unknown_local_bbo",
        )
        spot_bbo = _load_bbo_mid_series(
            bbo_dir=args.spot_bbo_dir,
            symbol=args.ref_symbol,
            days=days,
            fallback_resolution="unknown_spot_bbo",
        )
        xmarket_tables = xmarket_ref_shadow_tables(
            order_table,
            ref_bbo=ref_bbo,
            local_bbo=local_bbo,
            spot_bbo=spot_bbo,
            threshold_bps=args.xmarket_threshold_bps,
            cancel_threshold_bps=args.xmarket_cancel_threshold_bps,
            max_lag_s=args.xmarket_max_lag_s,
            include_orders=args.xmarket_ref_write_orders,
        )
        sections.append(("XMarket Reference Shadow Summary", xmarket_ref_shadow_summary(xmarket_tables)))
        sections.append(("XMarket Reference State Rollup", xmarket_tables.get("state_rollup", [])[:80]))
        sections.append(("XMarket Pending Residual Rollup", xmarket_tables.get("pending_rollup", [])[:80]))
        sections.append(("XMarket Pending Residual Sorting", xmarket_tables.get("pending_sorting", [])[:80]))
        sections.append(("XMarket Fill-Time Pending Residual Rollup", xmarket_tables.get("fill_pending_rollup", [])[:80]))
        sections.append(("XMarket Fill-Time Pending Residual Sorting", xmarket_tables.get("fill_pending_sorting", [])[:80]))
        sections.append(("XMarket Event-Cancel Counterfactual", xmarket_tables.get("event_cancel", [])[:80]))
        for suffix, rows in xmarket_tables.items():
            if suffix == "orders" and not args.xmarket_ref_write_orders:
                continue
            csv_path = args.out_prefix.with_suffix(f".xmarket_ref_{suffix}.csv")
            write_csv(csv_path, rows)
            outputs[f"xmarket_ref_{suffix}_csv"] = str(csv_path)

    if "spot_pending_shadow" in requested:
        if not order_table:
            order_table = order_level_input_rows
        if not order_table and replay_order_rows:
            order_table = replay_order_level_rows(
                replay_order_rows=replay_order_rows,
                replay_fill_rows=replay_fill_rows,
            )
        if not order_table and order_rows:
            order_table = order_level_rows(
                order_rows=order_rows,
                quote_rows=quote_rows,
                inventory_shadow_rows=shadow_rows,
                sell_resiliency_rows=sell_resiliency_rows,
            )
        days = _days_from_rows(order_table)
        local_bbo = _load_bbo_mid_series(
            bbo_dir=args.local_bbo_dir,
            symbol=args.symbol,
            days=days,
            fallback_resolution="unknown_local_bbo",
        )
        exec_spot_bbo = _load_bbo_mid_series(
            bbo_dir=args.spot_bbo_dir,
            symbol=args.symbol,
            days=days,
            fallback_resolution="spot_1s_bar",
        )
        ref_spot_bbo = _load_bbo_mid_series(
            bbo_dir=args.spot_bbo_dir,
            symbol=args.ref_symbol,
            days=days,
            fallback_resolution="spot_1s_bar",
        )
        spot_tables = spot_pending_shadow_tables(
            order_table,
            local_bbo=local_bbo,
            exec_spot_bbo=exec_spot_bbo,
            ref_spot_bbo=ref_spot_bbo,
            threshold_bps=args.xmarket_threshold_bps,
            max_lag_s=args.xmarket_max_lag_s,
            include_orders=args.xmarket_ref_write_orders,
        )
        sections.append(("Spot Pending Shadow Summary", spot_pending_shadow_summary(spot_tables)))
        sections.append(("Spot Pending Residual Rollup", spot_tables.get("pending_rollup", [])[:120]))
        sections.append(("Spot Pending Residual Sorting", spot_tables.get("pending_sorting", [])[:120]))
        sections.append(("Spot Fill-Time Pending Residual Rollup", spot_tables.get("fill_pending_rollup", [])[:120]))
        sections.append(("Spot Fill-Time Pending Residual Sorting", spot_tables.get("fill_pending_sorting", [])[:120]))
        for suffix, rows in spot_tables.items():
            if suffix == "orders" and not args.xmarket_ref_write_orders:
                continue
            csv_path = args.out_prefix.with_suffix(f".spot_pending_{suffix}.csv")
            write_csv(csv_path, rows)
            outputs[f"spot_pending_{suffix}_csv"] = str(csv_path)

    if "receive_time_tape" in requested:
        tape_paths = expand_market_tape_inputs(args.market_tape_input)
        if not tape_paths:
            raise FileNotFoundError(
                "receive_time_tape report requires at least one --market-tape-input"
            )
        latency_rows = market_tape_latency_distribution(tape_paths)
        sections.append(
            (
                "Receive-Time Tape Summary",
                {
                    "input_files": len(tape_paths),
                    "latency_groups": len(latency_rows),
                    "schema": "market_tape.v1",
                    "policy_effect": "none",
                    "warning": (
                        "transport lag is exchange-clock sensitive; REST/poll cadence cannot "
                        "support horizons below its empirical event cadence"
                    ),
                },
            )
        )
        sections.append(("Venue Latency Distribution", latency_rows))
        latency_path = args.out_prefix.with_suffix(".receive_time_latency.csv")
        write_csv(latency_path, latency_rows)
        outputs["receive_time_latency_csv"] = str(latency_path)

        if args.market_tape_external_market_id:
            horizons = tuple(
                int(value)
                for value in args.market_tape_horizons_ms.split(",")
                if value.strip()
            )
            market_ids = {
                args.market_tape_local_market_id,
                *args.market_tape_external_market_id,
            }
            series = load_market_tape_book_series(tape_paths, market_ids)
            leader_rows = market_tape_leader_survival(
                series,
                local_market_id=args.market_tape_local_market_id,
                external_market_ids=args.market_tape_external_market_id,
                horizons_ms=horizons,
                lookback_ms=args.market_tape_lookback_ms,
                shock_threshold_bps=args.market_tape_shock_threshold_bps,
                max_book_age_ms=args.market_tape_max_book_age_ms,
            )
            sections.append(("Single-Source Leader Survival Diagnostic", leader_rows))
            leader_path = args.out_prefix.with_suffix(".receive_time_leader_survival.csv")
            write_csv(leader_path, leader_rows)
            outputs["receive_time_leader_survival_csv"] = str(leader_path)

    metadata = {
        "symbol": args.symbol,
        "start_utc": utc_text(start_ts) if start_ts else "",
        "end_utc": utc_text(end_ts) if end_ts else "",
        "trades": str(trades_path or ""),
        "order_outcomes": str(order_path or ""),
        "quote_decisions": str(quote_path or ""),
        "inventory_shadow": str(shadow_path or ""),
        "sell_resiliency_shadow": str(sell_resiliency_path or ""),
        "summary_csv": str(args.summary_csv or ""),
        "replay_daily_csv": str(args.replay_daily_csv or ""),
        "toxic_model_compare": str(args.toxic_model_compare or ""),
        "toxic_order_aggregate": str(args.toxic_order_aggregate or ""),
        "toxic_shadow_avoidance": str(args.toxic_shadow_avoidance or ""),
        "shadow_candidates": str(args.shadow_candidates or ""),
        "shadow_daily": str(args.shadow_daily or ""),
        "bucket_research_clues": str(args.bucket_research_clues or ""),
        "bucket_daily_support": str(args.bucket_daily_support or ""),
        "order_level_csv": str(args.order_level_csv or ""),
        "replay_orders_csv": str(args.replay_orders_csv or ""),
        "replay_fills_csv": str(args.replay_fills_csv or ""),
        "market_tape_inputs": args.market_tape_input,
        "ref_symbol": args.ref_symbol,
        "ref_bbo_dir": str(args.ref_bbo_dir or ""),
        "local_bbo_dir": str(args.local_bbo_dir or ""),
        "spot_bbo_dir": str(args.spot_bbo_dir or ""),
        "outputs": outputs,
        "note": "InvAdj is inventory-path drift decomposition, not a standalone risk-adjusted alpha metric.",
    }
    report = render_report(
        title=f"{args.symbol} Unified Audit",
        metadata=metadata,
        sections=sections,
    )
    md_path = args.out_prefix.with_suffix(".md")
    json_path = args.out_prefix.with_suffix(".json")
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(report, encoding="utf-8")
    json_path.write_text(json.dumps({"metadata": metadata}, indent=2), encoding="utf-8")
    print(f"report={md_path}")
    print(f"metadata={json_path}")
    for key, value in outputs.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
