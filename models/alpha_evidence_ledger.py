#!/usr/bin/env python3
"""Build a fill-level alpha evidence ledger from daily tick replay traces.

This script is deliberately not a parameter optimizer.  It answers a narrower
question: after data-quality and mechanism gates, which maker fills look like
spread compensation, and which fills look like toxic flow?

Inputs can be existing ``tick_quote_decomposition_*.{orders,fills,decisions}``
files, or the script can run a Python quote-decomposition replay for UTC days.
The output is a set of bucketed evidence tables that should be reviewed before
any new parameter or live-candidate discussion.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from models.quote_decomposition_tick import _run_day  # noqa: E402
from models.symbol_paths import DEFAULT_SYMBOL  # noqa: E402
from models.tick_ab import parse_bound as _parse_bound  # noqa: E402


@dataclass(frozen=True)
class TraceInputs:
    summary: pd.DataFrame
    orders: pd.DataFrame
    fills: pd.DataFrame
    decisions: pd.DataFrame
    source: str


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _path_with_suffix(stem: Path, suffix: str) -> Path:
    """Append quote-decomposition style suffix to an arbitrary stem path."""
    name = stem.name
    if name.endswith(suffix):
        return stem
    return stem.parent / f"{name}{suffix}"


def _load_from_files(args: argparse.Namespace) -> TraceInputs:
    stem = Path(args.from_stem).expanduser() if args.from_stem else None
    summary_path = Path(args.summary_csv).expanduser() if args.summary_csv else None
    orders_path = Path(args.orders_csv).expanduser() if args.orders_csv else None
    fills_path = Path(args.fills_csv).expanduser() if args.fills_csv else None
    decisions_path = Path(args.decisions_csv).expanduser() if args.decisions_csv else None
    if stem is not None:
        summary_path = summary_path or _path_with_suffix(stem, ".summary.csv")
        orders_path = orders_path or _path_with_suffix(stem, ".orders.csv")
        fills_path = fills_path or _path_with_suffix(stem, ".fills.csv")
        decisions_path = decisions_path or _path_with_suffix(stem, ".decisions.csv")
    return TraceInputs(
        summary=_read_csv(summary_path),
        orders=_read_csv(orders_path),
        fills=_read_csv(fills_path),
        decisions=_read_csv(decisions_path),
        source=str(stem or "explicit_csv"),
    )


def _run_replay_inputs(args: argparse.Namespace) -> TraceInputs:
    bt.configure_symbol(args.symbol)
    start_ms = _parse_bound(args.start_date, is_end=False)
    end_ms = _parse_bound(args.end_date, is_end=True)
    summaries: list[dict[str, Any]] = []
    order_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    decision_frames: list[pd.DataFrame] = []
    for day in args.days or []:
        day_start = _parse_bound(f"{day} 00:00", is_end=False)
        day_end = _parse_bound(day, is_end=True)
        seg_start = max(x for x in (start_ms, day_start) if x is not None)
        seg_end = min(x for x in (end_ms, day_end) if x is not None)
        print(f"Running Python quote trace for {args.symbol} {day} ...")
        summary, orders, fills, decisions, _queue_events = _run_day(
            args.symbol,
            day,
            trace_quotes_max=args.trace_quotes_max,
            trace_fills_max=args.trace_fills_max,
            trace_decisions_max=args.trace_decisions_max,
            trace_queue_events_max=0,
            engine="python",
            window_cache_dir=args.window_cache_dir,
            refresh_window_cache=args.refresh_window_cache,
            start_ms=seg_start,
            end_ms=seg_end,
            queue_regime_calibration=args.queue_regime_calibration,
        )
        summaries.append(summary)
        if not orders.empty:
            order_frames.append(orders)
        if not fills.empty:
            fill_frames.append(fills)
        if not decisions.empty:
            decision_frames.append(decisions)
        print(
            f"  done {day}: fills={summary.get('fills_bid', 0)}/"
            f"{summary.get('fills_ask', 0)} orders={len(orders)} decisions={len(decisions)}"
        )
    return TraceInputs(
        summary=pd.DataFrame(summaries),
        orders=pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame(),
        fills=pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame(),
        decisions=pd.concat(decision_frames, ignore_index=True) if decision_frames else pd.DataFrame(),
        source="python_replay",
    )


def _bool_col(frame: pd.DataFrame, name: str) -> pd.Series:
    if frame.empty or name not in frame:
        return pd.Series(False, index=frame.index)
    raw = frame[name]
    if pd.api.types.is_bool_dtype(raw):
        return raw.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(raw):
        return pd.to_numeric(raw, errors="coerce").fillna(0.0).ne(0.0)
    text = raw.fillna("").astype(str).str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y"})


def _num_col(frame: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if frame.empty or name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce").fillna(default)


def _side_value(frame: pd.DataFrame, bid_col: str, ask_col: str, default: float = 0.0) -> pd.Series:
    side = frame.get("side", pd.Series("", index=frame.index)).astype(str).str.upper()
    bid = _num_col(frame, bid_col, default)
    ask = _num_col(frame, ask_col, default)
    return pd.Series(np.where(side.eq("BUY"), bid, ask), index=frame.index)


def _guard_state(frame: pd.DataFrame) -> pd.Series:
    adverse = (
        _bool_col(frame, "side_adverse")
        | _bool_col(frame, "side_adverse_pause")
        | _bool_col(frame, "adverse_toxicity")
        | _bool_col(frame, "adverse_markout")
        | _bool_col(frame, "adverse_thin_depth")
    )
    defense = (
        _bool_col(frame, "defense_guard")
        | _bool_col(frame, "defense_pause")
        | _bool_col(frame, "defense_markout")
        | _bool_col(frame, "defense_direction")
        | _bool_col(frame, "defense_microprice")
    )
    local = _bool_col(frame, "local_extreme_guard") | _bool_col(frame, "local_extreme_pause")
    out = pd.Series("none", index=frame.index, dtype=object)
    out.loc[local] = "local_extreme"
    out.loc[defense] = "defense"
    out.loc[adverse] = "adverse"
    both = adverse & defense
    out.loc[both] = "adverse+defense"
    return out


def _bucket_fixed(series: pd.Series, bins: list[float], labels: list[str]) -> pd.Series:
    bucket = pd.cut(pd.to_numeric(series, errors="coerce"), bins=bins, labels=labels, include_lowest=True)
    out = bucket.astype("object")
    out[pd.isna(out)] = "missing"
    return out.astype(str)


def _bucket_signed(series: pd.Series, eps: float = 1e-12) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    out = pd.Series("missing", index=series.index, dtype=object)
    out.loc[vals > eps] = "positive"
    out.loc[vals < -eps] = "negative"
    out.loc[vals.abs() <= eps] = "zero"
    return out.astype(str)


def _side_position_sign(frame: pd.DataFrame) -> pd.Series:
    side = frame.get("side", pd.Series("", index=frame.index)).astype(str).str.upper()
    return side.map({"BUY": 1.0, "BID": 1.0, "SELL": -1.0, "ASK": -1.0}).fillna(0.0)


def _ensure_xmarket_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach reference/spot shock buckets when xmarket columns are present.

    The sign convention follows maker inventory: BUY fills create long exposure,
    so reference/spot down moves are adverse; SELL fills create short exposure,
    so reference/spot up moves are adverse.
    """
    out = frame.copy()
    pos = _side_position_sign(out)
    ref_ret_cols: list[str] = []
    for horizon in (10, 30, 60):
        raw_col = f"cv_ref_perp_ret_{horizon}s"
        adv_col = f"ref_adverse_ret_{horizon}s"
        if adv_col not in out:
            out[adv_col] = -pos * _num_col(out, raw_col, 0.0)
        ref_ret_cols.append(adv_col)
    if "ref_adverse_ret_max" not in out:
        out["ref_adverse_ret_max"] = out[ref_ret_cols].max(axis=1)
    if "ref_adverse_flow" not in out:
        out["ref_adverse_flow"] = -pos * _num_col(out, "cv_ref_perp_volume_imbalance", 0.0)
    if "reference_confirmation" not in out:
        out["reference_confirmation"] = (
            _num_col(out, "ref_adverse_ret_max", 0.0).ge(2e-5)
            | (
                _num_col(out, "ref_adverse_ret_max", 0.0).gt(0.0)
                & _num_col(out, "ref_adverse_flow", 0.0).ge(0.35)
            )
        )

    spot_ret_cols: list[str] = []
    spot_flow_cols: list[str] = []
    for prefix in ("cv_exec_spot", "cv_ref_spot"):
        for horizon in (10, 30, 60):
            raw_col = f"{prefix}_ret_{horizon}s"
            adv_col = f"{prefix}_adverse_ret_{horizon}s"
            if adv_col not in out:
                out[adv_col] = -pos * _num_col(out, raw_col, 0.0)
            spot_ret_cols.append(adv_col)
        flow_col = f"{prefix}_adverse_flow"
        if flow_col not in out:
            out[flow_col] = -pos * _num_col(out, f"{prefix}_volume_imbalance", 0.0)
        spot_flow_cols.append(flow_col)
    if "spot_adverse_ret_max" not in out:
        out["spot_adverse_ret_max"] = out[spot_ret_cols].max(axis=1) if spot_ret_cols else 0.0
    if "spot_confirmation" not in out:
        spot_ret_max = out[spot_ret_cols].max(axis=1) if spot_ret_cols else pd.Series(0.0, index=out.index)
        spot_flow_max = out[spot_flow_cols].max(axis=1) if spot_flow_cols else pd.Series(0.0, index=out.index)
        out["spot_confirmation"] = spot_ret_max.ge(2e-5) | spot_flow_max.ge(0.35)
    if "spot_available" not in out:
        spot_raw_cols = [
            col
            for col in out.columns
            if col.startswith(("cv_exec_spot_", "cv_ref_spot_"))
        ]
        if spot_raw_cols:
            out["spot_available"] = out[spot_raw_cols].apply(pd.to_numeric, errors="coerce").abs().max(axis=1).fillna(0.0).gt(0.0)
        else:
            out["spot_available"] = False
    if "shock_label" not in out:
        out["shock_label"] = "missing"

    ref_confirm = _bool_col(out, "reference_confirmation")
    spot_confirm = _bool_col(out, "spot_confirmation")
    spot_available = _bool_col(out, "spot_available")
    out["reference_bucket"] = np.where(ref_confirm, "ref_confirmed", "ref_unconfirmed")
    out["spot_bucket"] = np.where(
        ~spot_available,
        "spot_missing",
        np.where(spot_confirm, "spot_confirmed", "spot_unconfirmed"),
    )
    out["xmarket_confirm_bucket"] = np.select(
        [
            ref_confirm & spot_confirm,
            ref_confirm & ~spot_confirm,
            ~ref_confirm & spot_confirm,
        ],
        ["ref+spot_confirmed", "ref_only_confirmed", "spot_only_confirmed"],
        default="none_confirmed",
    )
    out["ref_adverse_ret_bucket"] = _bucket_fixed(
        _num_col(out, "ref_adverse_ret_max", default=np.nan),
        [-np.inf, -2e-5, 0.0, 2e-5, 5e-5, np.inf],
        ["ref_favorable_gt2e-5", "ref_favorable_0_2e-5", "ref_neutral_0_2e-5", "ref_adverse_2e-5_5e-5", "ref_adverse_gt5e-5"],
    )
    out["spot_adverse_ret_bucket"] = _bucket_fixed(
        _num_col(out, "spot_adverse_ret_max", default=np.nan),
        [-np.inf, -2e-5, 0.0, 2e-5, 5e-5, np.inf],
        ["spot_favorable_gt2e-5", "spot_favorable_0_2e-5", "spot_neutral_0_2e-5", "spot_adverse_2e-5_5e-5", "spot_adverse_gt5e-5"],
    )
    return out


def _add_common_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.copy()
    out["side"] = out["side"].astype(str).str.upper()
    out = _ensure_xmarket_columns(out)
    out["side_toxicity"] = _side_value(out, "tox_bid", "tox_ask")
    out["side_quote_ev_30s"] = _side_value(out, "bid_quote_ev_30s", "ask_quote_ev_30s")
    out["side_quote_fill_prob"] = _side_value(out, "bid_quote_fill_prob", "ask_quote_fill_prob")
    out["side_quote_fill_markout_30s"] = _side_value(
        out, "bid_quote_fill_markout_30s", "ask_quote_fill_markout_30s"
    )
    out["side_markout_ema"] = _side_value(out, "mo_ema_bid", "mo_ema_ask")
    if "filled_qty" in out:
        qty = _num_col(out, "filled_qty")
    elif "fill_qty" in out:
        qty = _num_col(out, "fill_qty")
    elif "quantity" in out:
        qty = _num_col(out, "quantity")
    else:
        qty = pd.Series(1.0, index=out.index, dtype=float)
    # Old one-lot traces omitted quantity.  Preserve their event-weighted
    # behavior while quantity-weighting every trace that carries partial fills.
    out["fill_qty_btc"] = qty.where(qty.gt(0.0), 1.0)
    for column in ("markout_1s", "markout_5s", "markout_30s", "ev_30s"):
        out[f"{column}_qty"] = _num_col(out, column, default=np.nan) * out["fill_qty_btc"]
    out["ev_30s_usdc"] = out["ev_30s_qty"]
    out["guard_state"] = _guard_state(out)
    out["distance_bucket"] = _bucket_fixed(
        _num_col(out, "final_quote_delta_to_bbo", default=np.nan),
        [-np.inf, 0, 10, 20, 30, 40, 60, np.inf],
        ["inside_or_best", "0_10", "10_20", "20_30", "30_40", "40_60", "gt60"],
    )
    out["depth_bucket"] = _bucket_fixed(
        _num_col(out, "near_depth_total", default=np.nan),
        [-np.inf, 0.5, 1, 2, 5, 10, 25, np.inf],
        ["lt0p5", "0p5_1", "1_2", "2_5", "5_10", "10_25", "gt25"],
    )
    out["queue_rank_bucket"] = _bucket_fixed(
        _num_col(out, "queue_local_rank", default=np.nan),
        [-np.inf, 0.1, 0.25, 0.5, 0.75, 0.9, np.inf],
        ["lt0p10", "0p10_0p25", "0p25_0p50", "0p50_0p75", "0p75_0p90", "gt0p90"],
    )
    out["toxicity_bucket"] = _bucket_fixed(
        _num_col(out, "side_toxicity", default=np.nan),
        [-np.inf, 0.45, 0.5, 0.55, 0.6, 0.7, np.inf],
        ["lt45", "45_50", "50_55", "55_60", "60_70", "gt70"],
    )
    out["quote_ev_bucket"] = _bucket_signed(_num_col(out, "side_quote_ev_30s", default=np.nan))
    out["raw_bias_match"] = np.where(
        out.get("raw_bias_side", pd.Series("", index=out.index)).astype(str).str.upper().eq(out["side"]),
        "bias_matches_side",
        "bias_opposes_side",
    )
    return out


BUCKET_COLUMNS: tuple[tuple[str, str], ...] = (
    ("distance", "distance_bucket"),
    ("near_depth", "depth_bucket"),
    ("queue_rank", "queue_rank_bucket"),
    ("toxicity", "toxicity_bucket"),
    ("quote_ev_pred", "quote_ev_bucket"),
    ("guard_state", "guard_state"),
    ("raw_bias", "raw_bias_match"),
    ("reason_bucket", "queue_reason_bucket"),
    ("shock_label", "shock_label"),
    ("reference_confirm", "reference_bucket"),
    ("spot_confirm", "spot_bucket"),
    ("ref_adverse_ret", "ref_adverse_ret_bucket"),
    ("spot_adverse_ret", "spot_adverse_ret_bucket"),
    ("xmarket_confirm", "xmarket_confirm_bucket"),
)


def _iter_bucket_frames(frame: pd.DataFrame, columns: Iterable[tuple[str, str]]) -> Iterable[pd.DataFrame]:
    for family, column in columns:
        if column not in frame:
            continue
        work = frame.copy()
        work["bucket_family"] = family
        work["bucket"] = work[column].fillna("missing").astype(str)
        yield work


def _order_evidence(orders: pd.DataFrame, fills: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if orders.empty:
        return pd.DataFrame(), pd.DataFrame()
    orders = _add_common_columns(orders)
    fills = _add_common_columns(fills)
    if not fills.empty and "order_id" in fills:
        fill_mo = (
            fills.groupby(["day", "side", "order_id"], as_index=False)
            .agg(
                fill_events=("order_id", "count"),
                markout_1s=("markout_1s", "mean"),
                markout_5s=("markout_5s", "mean"),
                markout_30s=("markout_30s", "mean"),
                ev_30s=("ev_30s", "mean"),
                toxic_30s=("toxic_30s", "mean"),
            )
        )
        orders = orders.merge(fill_mo, on=["day", "side", "order_id"], how="left", suffixes=("", "_fill"))
    else:
        orders["fill_events"] = 0
    orders["filled_order"] = orders["outcome"].astype(str).eq("fill") | _num_col(orders, "fill_events").gt(0)
    bucketed = pd.concat(list(_iter_bucket_frames(orders, BUCKET_COLUMNS)), ignore_index=True)

    def _agg(group_cols: list[str]) -> pd.DataFrame:
        return (
            bucketed.groupby(group_cols, dropna=False)
            .agg(
                orders=("order_id", "nunique"),
                filled_orders=("filled_order", "sum"),
                fill_rate=("filled_order", "mean"),
                avg_lifetime_ms=("lifetime_ms", "mean"),
                avg_distance_to_bbo=("final_quote_delta_to_bbo", "mean"),
                avg_near_depth=("near_depth_total", "mean"),
                avg_queue_rank=("queue_local_rank", "mean"),
                avg_side_toxicity=("side_toxicity", "mean"),
                avg_quote_ev_pred=("side_quote_ev_30s", "mean"),
                avg_fill_markout_30s=("markout_30s", "mean"),
                avg_fill_ev_30s=("ev_30s", "mean"),
            )
            .reset_index()
            .sort_values(group_cols)
        )

    daily = _agg(["day", "side", "bucket_family", "bucket"])
    rollup = _agg(["side", "bucket_family", "bucket"])
    return daily, rollup


def _fill_evidence(fills: pd.DataFrame, *, min_fills: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if fills.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    fills = _add_common_columns(fills)
    fills["positive_30s"] = _num_col(fills, "ev_30s").gt(0)
    bucketed = pd.concat(list(_iter_bucket_frames(fills, BUCKET_COLUMNS)), ignore_index=True)

    def _agg(group_cols: list[str]) -> pd.DataFrame:
        out = (
            bucketed.groupby(group_cols, dropna=False)
            .agg(
                fills=("order_id", "count"),
                filled_qty_btc=("fill_qty_btc", "sum"),
                avg_age_ms=("age_ms", "mean"),
                avg_quote_dist=("quote_dist", "mean"),
                avg_distance_to_bbo=("final_quote_delta_to_bbo", "mean"),
                avg_near_depth=("near_depth_total", "mean"),
                avg_queue_rank=("queue_local_rank", "mean"),
                avg_side_toxicity=("side_toxicity", "mean"),
                avg_quote_ev_pred=("side_quote_ev_30s", "mean"),
                avg_quote_fill_prob=("side_quote_fill_prob", "mean"),
                markout_1s_qty_sum=("markout_1s_qty", "sum"),
                markout_5s_qty_sum=("markout_5s_qty", "sum"),
                markout_30s_qty_sum=("markout_30s_qty", "sum"),
                median_markout_30s=("markout_30s", "median"),
                p25_markout_30s=("markout_30s", lambda s: float(pd.to_numeric(s, errors="coerce").quantile(0.25))),
                p75_markout_30s=("markout_30s", lambda s: float(pd.to_numeric(s, errors="coerce").quantile(0.75))),
                ev_30s_qty_sum=("ev_30s_qty", "sum"),
                sum_ev_30s_usdc=("ev_30s_usdc", "sum"),
                positive_30s_rate=("positive_30s", "mean"),
                toxic_30s_rate=("toxic_30s", "mean"),
            )
            .reset_index()
        )
        denom = pd.to_numeric(out["filled_qty_btc"], errors="coerce").replace(0.0, np.nan)
        out["avg_markout_1s"] = out.pop("markout_1s_qty_sum") / denom
        out["avg_markout_5s"] = out.pop("markout_5s_qty_sum") / denom
        out["avg_markout_30s"] = out.pop("markout_30s_qty_sum") / denom
        out["avg_ev_30s_usdc_per_btc"] = out.pop("ev_30s_qty_sum") / denom
        # Compatibility alias.  Unlike the historical value, this is now
        # quantity-weighted; new consumers should use the unit-bearing name.
        out["avg_ev_30s"] = out["avg_ev_30s_usdc_per_btc"]
        out["sample_quality"] = np.where(out["fills"] >= min_fills, "ok", "sparse")
        out["alpha_evidence"] = np.where(
            (out["fills"] >= min_fills)
            & (out["avg_ev_30s"] > 0.0)
            & (out["avg_markout_30s"] > 0.0)
            & (out["positive_30s_rate"] >= 0.5),
            "positive_bucket",
            np.where(out["fills"] >= min_fills, "negative_or_mixed", "sparse"),
        )
        return out.sort_values(group_cols)

    daily = _agg(["day", "side", "bucket_family", "bucket"])
    rollup = _agg(["side", "bucket_family", "bucket"])
    positives = (
        rollup[rollup["alpha_evidence"].eq("positive_bucket")]
        .sort_values(["avg_ev_30s", "positive_30s_rate", "fills"], ascending=[False, False, False])
        .reset_index(drop=True)
    )
    return daily, rollup, positives


def _bucket_stability(fill_daily: pd.DataFrame, *, min_fills: int) -> pd.DataFrame:
    if fill_daily.empty:
        return pd.DataFrame()
    work = fill_daily.copy()
    work["support_day"] = pd.to_numeric(work["fills"], errors="coerce").fillna(0).ge(min_fills)
    work["positive_day"] = pd.to_numeric(work["avg_ev_30s"], errors="coerce").fillna(0.0).gt(0.0)
    work["weighted_ev_part"] = (
        pd.to_numeric(work["avg_ev_30s"], errors="coerce").fillna(0.0)
        * pd.to_numeric(work["fills"], errors="coerce").fillna(0.0)
    )
    work["weighted_pos_part"] = (
        pd.to_numeric(work["positive_30s_rate"], errors="coerce").fillna(0.0)
        * pd.to_numeric(work["fills"], errors="coerce").fillna(0.0)
    )
    rows: list[dict[str, Any]] = []
    for keys, group in work.groupby(["side", "bucket_family", "bucket"], dropna=False):
        side, family, bucket = keys
        fills = float(group["fills"].sum())
        days = int(group["day"].nunique())
        support = int(group["support_day"].sum())
        pos_days = int(group["positive_day"].sum())
        pos_support = int((group["support_day"] & group["positive_day"]).sum())
        weighted_ev = float(group["weighted_ev_part"].sum() / fills) if fills > 0 else 0.0
        weighted_pos = float(group["weighted_pos_part"].sum() / fills) if fills > 0 else 0.0
        support_ratio = pos_support / support if support else 0.0
        if support >= 3 and support_ratio >= 2 / 3 and weighted_ev > 0:
            verdict = "stable_positive"
        elif fills >= min_fills and weighted_ev > 0 and support == 0:
            verdict = "sparse_cross_day_positive"
        elif fills >= min_fills and weighted_ev > 0:
            verdict = "mixed_positive"
        elif fills >= min_fills:
            verdict = "negative_or_mixed"
        else:
            verdict = "sparse"
        rows.append({
            "side": side,
            "bucket_family": family,
            "bucket": bucket,
            "days": days,
            "total_fills": int(fills),
            "support_days": support,
            "positive_days": pos_days,
            "positive_support_days": pos_support,
            "positive_support_ratio": support_ratio,
            "weighted_avg_ev_30s": weighted_ev,
            "weighted_positive_30s_rate": weighted_pos,
            "best_day_ev_30s": float(group["avg_ev_30s"].max()),
            "worst_day_ev_30s": float(group["avg_ev_30s"].min()),
            "verdict": verdict,
        })
    return pd.DataFrame(rows).sort_values(
        ["verdict", "weighted_avg_ev_30s", "total_fills"],
        ascending=[True, False, False],
    )


def _side_regime_evidence(orders: pd.DataFrame, fills: pd.DataFrame, *, min_fills: int) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame()
    fills = _add_common_columns(fills)
    group_cols = ["side", "distance_bucket", "depth_bucket", "queue_rank_bucket", "guard_state"]
    out = (
        fills.groupby(group_cols, dropna=False)
        .agg(
            fills=("order_id", "count"),
            filled_qty_btc=("fill_qty_btc", "sum"),
            ev_30s_qty_sum=("ev_30s_qty", "sum"),
            sum_ev_30s_usdc=("ev_30s_usdc", "sum"),
            markout_1s_qty_sum=("markout_1s_qty", "sum"),
            markout_5s_qty_sum=("markout_5s_qty", "sum"),
            markout_30s_qty_sum=("markout_30s_qty", "sum"),
            positive_30s_rate=("ev_30s", lambda s: float((pd.to_numeric(s, errors="coerce") > 0).mean())),
            avg_side_toxicity=("side_toxicity", "mean"),
            avg_quote_fill_prob=("side_quote_fill_prob", "mean"),
            avg_age_ms=("age_ms", "mean"),
        )
        .reset_index()
    )
    denom = pd.to_numeric(out["filled_qty_btc"], errors="coerce").replace(0.0, np.nan)
    out["avg_ev_30s_usdc_per_btc"] = out.pop("ev_30s_qty_sum") / denom
    out["avg_ev_30s"] = out["avg_ev_30s_usdc_per_btc"]
    out["avg_markout_1s"] = out.pop("markout_1s_qty_sum") / denom
    out["avg_markout_5s"] = out.pop("markout_5s_qty_sum") / denom
    out["avg_markout_30s"] = out.pop("markout_30s_qty_sum") / denom
    out["sample_quality"] = np.where(out["fills"] >= min_fills, "ok", "sparse")
    return out.sort_values(["sample_quality", "avg_ev_30s", "fills"], ascending=[True, False, False])


def _decision_evidence(decisions: pd.DataFrame) -> pd.DataFrame:
    if decisions.empty:
        return pd.DataFrame()
    work = decisions.copy()
    work["side"] = work["side"].astype(str).str.upper()
    work["reason_text"] = work.get("reason_text", "missing").fillna("missing").astype(str)
    work["action"] = work.get("action", "missing").fillna("missing").astype(str)
    work["allow_post_bool"] = _bool_col(work, "allow_post")
    return (
        work.groupby(["side", "action", "reason_text"], dropna=False)
        .agg(
            decisions=("side", "count"),
            allow_post_rate=("allow_post_bool", "mean"),
            avg_final_distance_to_mid=("final_distance_to_mid", "mean"),
            avg_final_delta_to_bbo=("final_quote_delta_to_bbo", "mean"),
            avg_spread_mult=("spread_mult", "mean"),
            avg_toxicity=("toxicity", "mean"),
            avg_markout_ema=("markout_ema", "mean"),
            avg_l2_near_depth_total=("l2_near_depth_total", "mean"),
        )
        .reset_index()
        .sort_values(["side", "decisions"], ascending=[True, False])
    )


def _summary_evidence(summary: pd.DataFrame, orders: pd.DataFrame, fills: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    all_days = sorted(set(summary.get("day", pd.Series(dtype=str)).astype(str)) | set(orders.get("day", pd.Series(dtype=str)).astype(str)))
    for day in all_days:
        day_summary = summary[summary["day"].astype(str).eq(day)].iloc[0].to_dict() if not summary.empty and "day" in summary and summary["day"].astype(str).eq(day).any() else {}
        day_orders = orders[orders["day"].astype(str).eq(day)] if not orders.empty and "day" in orders else pd.DataFrame()
        day_fills = fills[fills["day"].astype(str).eq(day)] if not fills.empty and "day" in fills else pd.DataFrame()
        rows.append({
            "day": day,
            "orders": int(day_orders["order_id"].nunique()) if "order_id" in day_orders else 0,
            "fills": int(len(day_fills)),
            "fills_buy": int(day_fills["side"].astype(str).str.upper().eq("BUY").sum()) if not day_fills.empty else 0,
            "fills_sell": int(day_fills["side"].astype(str).str.upper().eq("SELL").sum()) if not day_fills.empty else 0,
            "fill_rate_per_order": float(len(day_fills) / max(day_orders["order_id"].nunique(), 1)) if "order_id" in day_orders else 0.0,
            "avg_ev_30s": float(day_fills["ev_30s_qty"].sum() / day_fills["fill_qty_btc"].sum()) if not day_fills.empty and day_fills["fill_qty_btc"].sum() > 0 else 0.0,
            "avg_markout_30s": float(day_fills["markout_30s_qty"].sum() / day_fills["fill_qty_btc"].sum()) if not day_fills.empty and day_fills["fill_qty_btc"].sum() > 0 else 0.0,
            "sum_ev_30s_usdc": float(day_fills["ev_30s_usdc"].sum()) if not day_fills.empty else 0.0,
            "toxic_30s_rate": float(day_fills.get("toxic_30s", pd.Series(dtype=float)).astype(bool).mean()) if not day_fills.empty and "toxic_30s" in day_fills else 0.0,
            "pnl": float(day_summary.get("pnl", 0.0) or 0.0),
            "inventory_adjusted_pnl": float(day_summary.get("inventory_adjusted_pnl", 0.0) or 0.0),
            "abs_inventory_time_s": float(day_summary.get("abs_inventory_time_s", 0.0) or 0.0),
        })
    return pd.DataFrame(rows)


def _df_to_md(frame: pd.DataFrame, *, max_rows: int = 30) -> str:
    if frame.empty:
        return "_No rows._"
    shown = frame.head(max_rows).copy()
    for col in shown.columns:
        if pd.api.types.is_float_dtype(shown[col]):
            shown[col] = shown[col].map(lambda v: "" if pd.isna(v) else f"{v:.6g}")
    lines = [
        "| " + " | ".join(shown.columns) + " |",
        "|" + "|".join("---" for _ in shown.columns) + "|",
    ]
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[col]) if not pd.isna(row[col]) else "" for col in shown.columns) + " |")
    return "\n".join(lines)


def _write_markdown(
    path: Path,
    *,
    summary: pd.DataFrame,
    fill_rollup: pd.DataFrame,
    bucket_stability: pd.DataFrame,
    order_rollup: pd.DataFrame,
    positives: pd.DataFrame,
    side_regime: pd.DataFrame,
    decision: pd.DataFrame,
    outputs: dict[str, str],
    min_fills: int,
) -> None:
    lines = [
        "# Alpha Evidence Ledger",
        "",
        "This report is not a parameter sweep.  Read it as fill-selection evidence:",
        "which side/regime buckets produce positive fill-after markout, and which",
        "buckets only increase toxic flow.",
        "",
        f"Minimum filled rows for a non-sparse bucket: `{min_fills}`.",
        "",
        "## Daily Overview",
        "",
        _df_to_md(summary.round(6)),
        "",
        "## Positive Buckets",
        "",
        _df_to_md(positives.round(6), max_rows=50),
        "",
        "## Bucket Stability",
        "",
        _df_to_md(
            bucket_stability.sort_values(
                ["verdict", "weighted_avg_ev_30s", "total_fills"],
                ascending=[True, False, False],
            ).round(6),
            max_rows=80,
        ),
        "",
        "## Fill Evidence Rollup",
        "",
        _df_to_md(
            fill_rollup.sort_values(["sample_quality", "avg_ev_30s", "fills"], ascending=[True, False, False]).round(6),
            max_rows=80,
        ),
        "",
        "## Order Fill-Probability Rollup",
        "",
        _df_to_md(
            order_rollup.sort_values(["fill_rate", "orders"], ascending=[False, False]).round(6),
            max_rows=80,
        ),
        "",
        "## Side/Regime Cross Buckets",
        "",
        _df_to_md(side_regime.round(6), max_rows=80),
        "",
        "## Decision Gate Evidence",
        "",
        _df_to_md(decision.round(6), max_rows=80),
        "",
        "## Files",
        "",
    ]
    for name, out_path in outputs.items():
        lines.append(f"- {name}: `{out_path}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_outputs(args: argparse.Namespace, inputs: TraceInputs) -> None:
    if inputs.orders.empty and inputs.fills.empty:
        raise SystemExit("No orders/fills found. Provide quote-decomposition CSVs or replay days with trace enabled.")

    orders = _add_common_columns(inputs.orders) if not inputs.orders.empty else pd.DataFrame()
    fills = _add_common_columns(inputs.fills) if not inputs.fills.empty else pd.DataFrame()
    order_daily, order_rollup = _order_evidence(orders, fills)
    fill_daily, fill_rollup, positives = _fill_evidence(fills, min_fills=args.min_fills)
    bucket_stability = _bucket_stability(fill_daily, min_fills=args.min_fills)
    side_regime = _side_regime_evidence(orders, fills, min_fills=args.min_fills)
    decision = _decision_evidence(inputs.decisions)
    summary = _summary_evidence(inputs.summary, orders, fills)

    out_dir = bt.RESULTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = out_dir / f"alpha_evidence_ledger_{args.tag}_{args.symbol.lower()}"
    outputs = {
        "daily_summary": str(stem.with_suffix(".daily_summary.csv")),
        "order_daily": str(stem.with_suffix(".order_daily.csv")),
        "order_rollup": str(stem.with_suffix(".order_rollup.csv")),
        "fill_daily": str(stem.with_suffix(".fill_daily.csv")),
        "fill_rollup": str(stem.with_suffix(".fill_rollup.csv")),
        "positive_buckets": str(stem.with_suffix(".positive_buckets.csv")),
        "bucket_stability": str(stem.with_suffix(".bucket_stability.csv")),
        "side_regime": str(stem.with_suffix(".side_regime.csv")),
        "decision": str(stem.with_suffix(".decision.csv")),
        "json": str(stem.with_suffix(".json")),
        "markdown": str(stem.with_suffix(".md")),
    }
    summary.to_csv(outputs["daily_summary"], index=False)
    order_daily.to_csv(outputs["order_daily"], index=False)
    order_rollup.to_csv(outputs["order_rollup"], index=False)
    fill_daily.to_csv(outputs["fill_daily"], index=False)
    fill_rollup.to_csv(outputs["fill_rollup"], index=False)
    positives.to_csv(outputs["positive_buckets"], index=False)
    bucket_stability.to_csv(outputs["bucket_stability"], index=False)
    side_regime.to_csv(outputs["side_regime"], index=False)
    decision.to_csv(outputs["decision"], index=False)
    Path(outputs["json"]).write_text(
        json.dumps(
            {
                "symbol": args.symbol.upper(),
                "tag": args.tag,
                "source": inputs.source,
                "min_fills": args.min_fills,
                "outputs": outputs,
                "daily_summary": json.loads(summary.to_json(orient="records")) if not summary.empty else [],
                "positive_bucket_count": int(len(positives)),
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_markdown(
        Path(outputs["markdown"]),
        summary=summary,
        fill_rollup=fill_rollup,
        bucket_stability=bucket_stability,
        order_rollup=order_rollup,
        positives=positives,
        side_regime=side_regime,
        decision=decision,
        outputs=outputs,
        min_fills=args.min_fills,
    )
    for path in outputs.values():
        print(f"Saved {path}")
    print("\nDaily overview:")
    print(summary.to_string(index=False))
    if positives.empty:
        print("\nNo non-sparse positive buckets under current gates.")
    else:
        print("\nPositive bucket candidates:")
        print(positives.head(20).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--tag", default=time.strftime("%Y%m%d_%H%M%S"))
    parser.add_argument("--from-stem", default=None, help="Path stem for tick_quote_decomposition output files.")
    parser.add_argument("--summary-csv", default=None)
    parser.add_argument("--orders-csv", default=None)
    parser.add_argument("--fills-csv", default=None)
    parser.add_argument("--decisions-csv", default=None)
    parser.add_argument("--days", nargs="*", help="UTC days to replay as independent fresh-start rows.")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--trace-quotes-max", type=int, default=2_000_000)
    parser.add_argument("--trace-fills-max", type=int, default=200_000)
    parser.add_argument("--trace-decisions-max", type=int, default=2_000_000)
    parser.add_argument("--window-cache-dir", default=None)
    parser.add_argument("--refresh-window-cache", action="store_true")
    parser.add_argument("--queue-regime-calibration", action="store_true")
    parser.add_argument("--min-fills", type=int, default=30, help="Minimum fill count before a bucket is non-sparse.")
    args = parser.parse_args()

    bt.configure_symbol(args.symbol)
    has_files = any([args.from_stem, args.summary_csv, args.orders_csv, args.fills_csv, args.decisions_csv])
    has_days = bool(args.days)
    if has_files and has_days:
        raise SystemExit("Use either existing CSV inputs or --days replay, not both.")
    if not has_files and not has_days:
        raise SystemExit("Provide --from-stem/CSV inputs, or provide --days to run Python replay.")
    inputs = _load_from_files(args) if has_files else _run_replay_inputs(args)
    _write_outputs(args, inputs)


if __name__ == "__main__":
    main()
