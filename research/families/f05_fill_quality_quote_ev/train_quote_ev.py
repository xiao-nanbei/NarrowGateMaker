#!/usr/bin/env python3
"""Train quote-level bid EV/toxicity models from tick quote traces."""

from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.families.f05_fill_quality_quote_ev.quote_ev import (  # noqa: E402
    DEFAULT_BID_QUOTE_FEATURES,
    DEFAULT_MARKOUT_BUCKET_EDGES,
    DEFAULT_MARKOUT_BUCKET_VALUES,
    clean_feature_value,
    quote_side_model_names,
    quote_side_prefix,
)
from models.backtest_config import load_live_config_as_params  # noqa: E402
from models.symbol_paths import DEFAULT_SYMBOL, paths_for  # noqa: E402
from models.quote_context_batch import enrich_orders_with_cpp_quote_context  # noqa: E402
from data_quality import allowed_timestamp_mask, mask_valid_horizon  # noqa: E402
from calendar_features import add_calendar_features, calendar_feature_names  # noqa: E402


MARKOUT_HORIZONS = (1, 5, 30)
HORIZON_VALID_COL = "_label_horizon_valid_30s"
FEATURE_HORIZON_VALID_COL = "_feature_horizon_valid_30s"
EXTREME_ADVERSE_THRESHOLD = float(DEFAULT_MARKOUT_BUCKET_EDGES[0])
TRACE_TS_COLS = ("quote_ts", "submit_ts", "activate_ts", "fill_ts", "outcome_ts", "quote_dt")

XMARKET_QUOTE_FEATURES = [
    "volume_imbalance",
    "volume_imbalance_30s",
    "volume_imbalance_60s",
    "trade_intensity_60s",
    "vpin_60s",
    "taker_quote_imbalance_10s",
    "taker_quote_imbalance_30s",
    "taker_quote_imbalance_60s",
    "taker_buy_sweep_score_10s",
    "taker_buy_sweep_score_30s",
    "taker_sell_sweep_score_10s",
    "taker_sell_sweep_score_30s",
    "l2_spread_bps",
    "l2_microprice_offset_bps",
    "l2_imbalance_l1",
    "l2_imbalance_l3",
    "l2_imbalance_l5",
    "l2_near_depth_total",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "cv_ref_perp_basis_bps",
    "cv_ref_perp_basis_residual_bps",
    "cv_ref_perp_ret_10s",
    "cv_ref_perp_ret_30s",
    "cv_ref_perp_ret_60s",
    "cv_ref_perp_volatility_60s",
    "cv_ref_perp_volume_imbalance",
    "cv_ref_perp_trade_intensity_60s",
    "cv_ref_perp_vpin_60s",
    "cv_ref_perp_age_s",
    "cv_ref_perp_available",
    "cv_exec_spot_basis_bps",
    "cv_exec_spot_basis_residual_bps",
    "cv_exec_spot_ret_10s",
    "cv_exec_spot_ret_30s",
    "cv_exec_spot_ret_60s",
    "cv_exec_spot_volume_imbalance",
    "cv_exec_spot_volatility_60s",
    "cv_exec_spot_trade_intensity_60s",
    "cv_exec_spot_vpin_60s",
    "cv_exec_spot_age_s",
    "cv_exec_spot_available",
    "cv_ref_spot_basis_bps",
    "cv_ref_spot_basis_residual_bps",
    "cv_ref_spot_ret_10s",
    "cv_ref_spot_ret_30s",
    "cv_ref_spot_ret_60s",
    "cv_ref_spot_volume_imbalance",
    "cv_ref_spot_volatility_60s",
    "cv_ref_spot_trade_intensity_60s",
    "cv_ref_spot_vpin_60s",
    "cv_ref_spot_age_s",
    "cv_ref_spot_available",
]

SHOCK_QUOTE_FEATURES = [
    "local_adverse_flow",
    "local_adverse_taker_30s",
    "local_adverse_microprice",
    "local_pressure_score",
    "ref_adverse_ret_10s",
    "ref_adverse_ret_30s",
    "ref_adverse_ret_60s",
    "ref_adverse_ret_max",
    "ref_adverse_flow",
    "spot_adverse_ret_max",
    "queue_ahead_ratio",
]

CPP_CONTEXT_QUOTE_FEATURES = [
    "cpp_near_depth_total",
    "cpp_book_imb",
    "cpp_microprice_shift_bps",
    "cpp_side_final_quote_delta_to_bbo",
    "cpp_side_side_adverse",
    "cpp_side_defense_guard",
]

XMARKET_INTERACTION_QUOTE_FEATURES = [
    "quote_ref_adverse_ret_10s",
    "quote_ref_adverse_ret_30s",
    "quote_ref_adverse_ret_60s",
    "quote_ref_adverse_ret_max",
    "quote_ref_favorable_gt2e5",
    "quote_ref_adverse_gt2e5",
    "quote_ref_low_abs_lt2e5",
    "quote_depth_ge1",
    "quote_depth_ge2",
    "quote_depth_ge5",
    "quote_dist_ge30",
    "quote_dist_ge40",
    "quote_dist_ge60",
    "quote_dist_40_60",
    "quote_rank_front_0_25",
    "quote_rank_front_0p1_0p5",
    "quote_rank_mid_0p25_0p75",
    "quote_rank_back_0p25_0p9",
    "quote_rank_back_0p5_0p9",
    "quote_ref_fav_x_depth_ge1",
    "quote_ref_fav_x_depth_ge2",
    "quote_ref_fav_x_depth_ge5",
    "quote_ref_fav_x_dist_ge30",
    "quote_ref_fav_x_dist_ge40",
    "quote_ref_fav_x_dist_ge60",
    "quote_ref_fav_x_dist_40_60",
    "quote_ref_fav_x_rank_front_0_25",
    "quote_ref_fav_x_rank_front_0p1_0p5",
    "quote_ref_fav_x_rank_mid_0p25_0p75",
    "quote_ref_fav_x_rank_back_0p25_0p9",
    "quote_ref_fav_x_rank_back_0p5_0p9",
    "quote_ref_fav_x_depth_ge1_dist_ge30",
    "quote_ref_fav_x_depth_ge1_dist_ge40",
    "quote_ref_fav_x_depth_ge2_dist_ge30",
    "quote_ref_fav_x_depth_ge2_dist_ge40",
]

LOCAL_FLOW_QUOTE_FEATURES = [
    "quote_local_adverse_flow_5s",
    "quote_local_adverse_flow_10s",
    "quote_local_adverse_flow_30s",
    "quote_local_adverse_flow_60s",
    "quote_local_flow_deceleration_30s_5s",
    "quote_local_flow_deceleration_60s_10s",
    "quote_local_flow_reversal_score",
    "quote_local_pressure_absent",
    "quote_local_pressure_reversing",
    "quote_local_pressure_decelerating",
    "quote_local_pressure_mild_decelerating",
    "quote_local_pressure_persistent",
    "quote_local_favorable_persistent",
    "quote_local_depth_refill_edge",
    "quote_local_refill_dominant",
    "quote_local_cancel_dominant",
    "quote_local_depth_balanced",
    "quote_local_adverse_microprice_bps",
    "quote_local_micro_favorable",
    "quote_local_micro_neutral",
    "quote_local_micro_adverse",
    "quote_local_fav_persistent_x_refill",
    "quote_local_fav_persistent_x_front_rank",
    "quote_local_fav_persistent_x_dist30_40",
    "quote_local_fav_persistent_x_sell",
    "quote_local_fav_persistent_x_sell_refill",
    "quote_local_mild_decel_x_refill",
    "quote_local_mild_decel_x_refill_back_rank",
]

LOCAL_RESILIENCY_QUOTE_FEATURES = [
    "quote_local_resiliency_score",
    "quote_local_resiliency_bucket_code",
    "quote_resil_depth_component",
    "quote_resil_refill_component",
    "quote_resil_queue_component",
    "quote_resil_flow_decay_component",
    "quote_resil_persistent_adverse_penalty",
    "quote_resil_brittle",
    "quote_resil_weak",
    "quote_resil_mid",
    "quote_resil_strong",
    "quote_resil_depth_low",
    "quote_resil_depth_mid",
    "quote_resil_depth_high",
    "quote_resil_refill_low",
    "quote_resil_refill_mid",
    "quote_resil_refill_high",
    "quote_resil_queue_low",
    "quote_resil_queue_mid",
    "quote_resil_queue_high",
    "quote_resil_flow_decay_low",
    "quote_resil_flow_decay_mid",
    "quote_resil_flow_decay_high",
    "quote_sell_x_resil_strong",
    "quote_sell_x_resil_queue_high_flow_high",
]

TOXIC_RISK_QUOTE_FEATURES = [
    "quote_toxic_risk_score",
    "quote_sell_toxic_risk_score",
    "quote_toxic_guard_flow_score",
    "quote_toxic_xmarket_score",
    "quote_toxic_queue_depth_score",
    "quote_toxic_side_mo_ema",
    "quote_toxic_adverse_flow_30s",
    "quote_toxic_ref_adverse_ret_max",
    "quote_toxic_spot_adverse_ret_max",
    "quote_toxic_guard_adverse_defense",
    "quote_toxic_refill_dominant",
    "quote_toxic_flow_weak_adverse",
    "quote_toxic_flow_strong_adverse",
    "quote_toxic_mo_neg10_neg3",
    "quote_toxic_mo_lt_neg10",
    "quote_toxic_ref_adverse",
    "quote_toxic_spot_adverse",
    "quote_toxic_ref_spot_adverse",
    "quote_toxic_depth_0p5_1",
    "quote_toxic_depth_1_2",
    "quote_toxic_depth_0p5_2",
    "quote_toxic_rank_front_0_25",
    "quote_toxic_rank_back_0_75_1",
    "quote_toxic_dist_30_40",
    "quote_toxic_dist_40_60",
    "quote_toxic_guard_flow_flag",
    "quote_toxic_xmarket_local_flag",
    "quote_sell_x_toxic_guard_flow",
    "quote_sell_x_toxic_ref_spot_adverse",
    "quote_sell_x_toxic_xmarket_shallow_rank",
]

MICRO_MACRO_QUOTE_FEATURES = [
    "quote_range_5s_bps",
    "quote_range_10s_bps",
    "quote_range_20s_bps",
    "quote_range_60s_bps",
    "quote_range_300s_bps",
    "quote_rv_10s_bps",
    "quote_rv_300s_bps",
    "quote_distance_micro_5s",
    "quote_distance_micro_10s",
    "quote_distance_micro",
    "quote_micro_fill_reach_score",
    "quote_micro_macro_range_ratio",
    "quote_micro_macro_vol_ratio",
    "quote_inventory_horizon_range_ratio",
    "quote_trend_efficiency_60s",
    "quote_trend_efficiency_300s",
    "quote_side_trend_adverse_60s_bps",
    "quote_side_trend_adverse_300s_bps",
    "quote_micro_macro_regime_code",
    "quote_micro_macro_dead_water",
    "quote_micro_macro_local_noise_macro_flat",
    "quote_micro_macro_macro_trend_dominant",
    "quote_micro_macro_shock_transition",
    "quote_micro_reversion_score",
    "quote_trend_inventory_risk_score",
    "quote_sell_x_trend_inventory_risk",
    "quote_sell_x_micro_reversion",
]

CALENDAR_QUOTE_FEATURES = calendar_feature_names("quote_cal_")


def _default_trace_prefix(symbol: str, tag: str) -> Path:
    paths = paths_for(symbol)
    return paths.results_dir / f"tick_quote_decomposition_{tag}_{symbol.lower()}"


def _read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _read_csv(path: Path) -> pd.DataFrame:
    return _read_table(path)


def _to_epoch_ms(series: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric.to_numpy(dtype=np.float64, copy=False)

    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    values = parsed.astype("int64").to_numpy(dtype=np.float64, copy=False) / 1_000_000.0
    values[parsed.isna().to_numpy()] = np.nan
    return values


def _filter_trace_for_orderbook_quality(frame: pd.DataFrame, symbol: str, label: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame

    for col in TRACE_TS_COLS:
        if col not in frame.columns:
            continue
        values = _to_epoch_ms(frame[col])
        if not np.isfinite(values).any():
            continue
        keep = allowed_timestamp_mask(values, symbol, label=label)
        if not keep.all():
            return frame.loc[keep].copy()
        return frame
    return frame


def _label_horizon_valid(labels: pd.DataFrame, max_gap_s: float) -> pd.Series:
    if max_gap_s <= 0.0:
        raise ValueError("max_label_gap_s must be positive")
    if labels.empty:
        return pd.Series(dtype=bool, index=labels.index)

    valid: pd.Series | None = None
    if FEATURE_HORIZON_VALID_COL in labels:
        valid = labels[FEATURE_HORIZON_VALID_COL].fillna(False).astype(bool)

    if "feature_dt" in labels:
        feature_dt = pd.to_datetime(labels["feature_dt"], utc=True, errors="coerce")
        feature_grid = pd.DatetimeIndex(feature_dt.dropna().unique()).sort_values()
        grid_valid = pd.Series(
            mask_valid_horizon(
                feature_grid,
                horizon_s=30.0,
                max_gap_s=float(max_gap_s),
            ),
            index=feature_grid,
            dtype=bool,
        )
        derived = pd.Series(
            grid_valid.reindex(pd.DatetimeIndex(feature_dt)).fillna(False).to_numpy(dtype=bool),
            index=labels.index,
        )
        valid = derived if valid is None else (valid & derived)

    if valid is None:
        raise ValueError(
            "quote EV labels require feature_dt or "
            f"{FEATURE_HORIZON_VALID_COL}; refusing to treat every horizon as valid"
        )
    return valid.astype(bool)


def _markout_col(horizon_s: int) -> str:
    return f"markout_{horizon_s}s"


def _label_markout_col(horizon_s: int, prefix: str = "bid") -> str:
    return f"label_{prefix}_fill_markout_{horizon_s}s"


def _label_bucket_col(horizon_s: int, prefix: str = "bid") -> str:
    return f"label_{prefix}_markout_bucket_{horizon_s}s"


def _label_toxic_col(horizon_s: int, prefix: str = "bid") -> str:
    return f"label_{prefix}_toxic_{horizon_s}s"


def _label_extreme_col(horizon_s: int, prefix: str = "bid") -> str:
    return f"label_{prefix}_extreme_adverse_{horizon_s}s"


def _bucketize_markout(values: pd.Series | np.ndarray) -> np.ndarray:
    arr = pd.to_numeric(values, errors="coerce").fillna(0.0) if isinstance(values, pd.Series) else values
    return np.digitize(np.asarray(arr, dtype=np.float64), DEFAULT_MARKOUT_BUCKET_EDGES).astype(np.int16)


def _empty_label_frame(orders: pd.DataFrame, prefix: str = "bid") -> pd.DataFrame:
    orders = orders.copy()
    orders[f"label_{prefix}_filled"] = 0.0
    orders[f"label_{prefix}_fill_ev_30s"] = 0.0
    orders[f"label_{prefix}_toxic_30s"] = 0.0
    orders[f"label_{prefix}_toxic_given_fill_30s"] = 0.0
    orders[f"label_{prefix}_extreme_adverse_any"] = 0.0
    orders["fill_age_ms"] = 0.0
    for horizon in MARKOUT_HORIZONS:
        orders[_label_markout_col(horizon, prefix)] = 0.0
        orders[_label_bucket_col(horizon, prefix)] = 0
        orders[_label_toxic_col(horizon, prefix)] = 0.0
        orders[_label_extreme_col(horizon, prefix)] = 0.0
    return orders


def build_labels(
    orders: pd.DataFrame,
    fills: pd.DataFrame,
    max_inventory: float | None = None,
    *,
    side: str = "BUY",
) -> pd.DataFrame:
    side_upper = "BUY" if quote_side_prefix(side) == "bid" else "SELL"
    prefix = quote_side_prefix(side_upper)
    orders = orders.loc[orders["side"].astype(str).str.upper() == side_upper].copy()
    if orders.empty:
        raise ValueError(f"No {side_upper} quote rows found in orders trace")

    if "day" not in orders.columns or "day" not in fills.columns:
        raise ValueError("quote EV training requires day-labelled quote/fill traces")
    key_cols = ["day", "order_id"]
    side_fills = fills.loc[fills["side"].astype(str).str.upper() == side_upper].copy()
    if side_fills.empty:
        return _empty_label_frame(orders, prefix)

    if "fill_qty" not in side_fills.columns:
        side_fills["fill_qty"] = 1.0
    side_fills["fill_qty"] = pd.to_numeric(side_fills["fill_qty"], errors="coerce").fillna(0.0).clip(lower=0.0)
    side_fills["_weight"] = side_fills["fill_qty"].where(side_fills["fill_qty"] > 0.0, 1.0)
    for horizon in MARKOUT_HORIZONS:
        markout_col = _markout_col(horizon)
        side_fills[markout_col] = pd.to_numeric(
            side_fills.get(markout_col, side_fills.get("ev_30s", 0.0)),
            errors="coerce",
        ).fillna(0.0)
        toxic_col = f"toxic_{horizon}s"
        if toxic_col in side_fills.columns:
            side_fills[toxic_col] = side_fills[toxic_col].astype(bool)
        else:
            side_fills[toxic_col] = side_fills[markout_col] < 0.0
    side_fills["age_ms"] = pd.to_numeric(side_fills.get("age_ms", 0.0), errors="coerce").fillna(0.0)

    def _agg(group: pd.DataFrame) -> pd.Series:
        w = group["_weight"].to_numpy(dtype=np.float64)
        total_w = float(w.sum())
        out: dict[str, Any] = {
            f"label_{prefix}_filled": 1.0,
            "fill_age_ms": float(np.average(group["age_ms"], weights=w)) if total_w > 0 else float(group["age_ms"].mean()),
        }
        any_extreme = False
        for horizon in MARKOUT_HORIZONS:
            markout_col = _markout_col(horizon)
            label_col = _label_markout_col(horizon, prefix)
            vals = group[markout_col].to_numpy(dtype=np.float64)
            avg = float(np.average(vals, weights=w)) if total_w > 0 else float(vals.mean())
            extreme = bool((group[markout_col] <= EXTREME_ADVERSE_THRESHOLD).any())
            any_extreme = any_extreme or extreme
            out[label_col] = avg
            out[_label_toxic_col(horizon, prefix)] = float(group[f"toxic_{horizon}s"].any())
            out[_label_extreme_col(horizon, prefix)] = float(extreme)
        out[f"label_{prefix}_fill_ev_30s"] = out[_label_markout_col(30, prefix)]
        out[f"label_{prefix}_toxic_30s"] = out[_label_toxic_col(30, prefix)]
        out[f"label_{prefix}_toxic_given_fill_30s"] = out[_label_toxic_col(30, prefix)]
        out[f"label_{prefix}_extreme_adverse_any"] = float(any_extreme)
        return pd.Series(out)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="DataFrameGroupBy.apply operated", category=FutureWarning)
        fill_labels = side_fills.groupby(key_cols, dropna=False).apply(_agg).reset_index()
    labelled = orders.merge(fill_labels, on=key_cols, how="left")
    for col in [
        f"label_{prefix}_filled",
        f"label_{prefix}_fill_ev_30s",
        f"label_{prefix}_toxic_30s",
        f"label_{prefix}_toxic_given_fill_30s",
        f"label_{prefix}_extreme_adverse_any",
        "fill_age_ms",
        *[_label_markout_col(h, prefix) for h in MARKOUT_HORIZONS],
        *[_label_toxic_col(h, prefix) for h in MARKOUT_HORIZONS],
        *[_label_extreme_col(h, prefix) for h in MARKOUT_HORIZONS],
    ]:
        labelled[col] = pd.to_numeric(labelled[col], errors="coerce").fillna(0.0)
    for horizon in MARKOUT_HORIZONS:
        bucket_col = _label_bucket_col(horizon, prefix)
        markout_col = _label_markout_col(horizon, prefix)
        labelled[bucket_col] = _bucketize_markout(labelled[markout_col])
        labelled.loc[labelled[f"label_{prefix}_filled"] <= 0.0, bucket_col] = 0
        labelled[bucket_col] = labelled[bucket_col].astype(np.int16)

    if max_inventory is None or max_inventory <= 0:
        max_inv = pd.to_numeric(labelled.get("inventory", 0.0), errors="coerce").abs().max()
        max_inv = float(max(max_inv, 1e-9))
    else:
        max_inv = float(max(max_inventory, 1e-9))
    labelled["inventory_ratio"] = pd.to_numeric(labelled.get("inventory", 0.0), errors="coerce").fillna(0.0) / max_inv
    labelled["inventory_ratio"] = labelled["inventory_ratio"].clip(-1.0, 1.0)
    return labelled


def _clean_feature_frame(frame: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    data: dict[str, np.ndarray] = {}
    for col in feature_cols:
        if col in frame:
            series = frame[col]
            if pd.api.types.is_bool_dtype(series):
                values = series.fillna(False).astype(np.float32).to_numpy(copy=False)
            elif pd.api.types.is_numeric_dtype(series):
                values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=np.float32, copy=False)
                values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
            else:
                values = series.map(clean_feature_value).to_numpy(dtype=np.float32, copy=False)
        else:
            values = np.zeros(len(frame), dtype=np.float32)
        data[col] = values
    return pd.DataFrame(data, index=frame.index, dtype=np.float32)


def _side_position_sign(side: pd.Series) -> pd.Series:
    norm = side.astype(str).str.upper()
    return pd.Series(np.where(norm.isin(["BUY", "BID"]), 1.0, -1.0), index=side.index)


def add_quote_time_interaction_features(
    frame: pd.DataFrame,
    *,
    threshold: float = 2e-5,
) -> pd.DataFrame:
    """Add quote-time xmarket interaction features.

    These are safe for quote EV training because they depend only on columns
    already present on the quote/order row.  Do not use fill-time shock labels or
    any previous quote EV prediction head here.
    """

    out = frame.copy()
    pos = _side_position_sign(out["side"]) if "side" in out.columns else pd.Series(1.0, index=out.index)
    adv_cols: list[str] = []
    for horizon in (10, 30, 60):
        src = f"cv_ref_perp_ret_{horizon}s"
        dst = f"quote_ref_adverse_ret_{horizon}s"
        if src in out.columns:
            # 和 live scalar path 保持同一套符号：BUY 怕 ref 跌，SELL 怕 ref 涨。
            out[dst] = -pos * pd.to_numeric(out[src], errors="coerce")
        else:
            out[dst] = np.nan
        adv_cols.append(dst)
    out["quote_ref_adverse_ret_max"] = out[adv_cols].max(axis=1, skipna=True)
    ref_available = out[adv_cols].notna().all(axis=1)
    ref_adv = pd.to_numeric(out["quote_ref_adverse_ret_max"], errors="coerce")
    out["quote_ref_favorable_gt2e5"] = (ref_available & (ref_adv < -threshold)).astype(float)
    out["quote_ref_adverse_gt2e5"] = (ref_available & (ref_adv > threshold)).astype(float)
    out["quote_ref_low_abs_lt2e5"] = (ref_available & (ref_adv.abs() <= threshold)).astype(float)

    if "l2_near_depth_total" in out.columns:
        depth = pd.to_numeric(out["l2_near_depth_total"], errors="coerce")
    else:
        depth = _first_existing_feature_series(out, ("near_depth_total",), np.nan)
    if "final_distance_to_mid" in out.columns:
        dist = pd.to_numeric(out["final_distance_to_mid"], errors="coerce").abs()
    elif "raw_distance_to_mid" in out.columns:
        dist = pd.to_numeric(out["raw_distance_to_mid"], errors="coerce").abs()
    else:
        dist = pd.Series(np.nan, index=out.index, dtype=np.float64)
    rank = _first_existing_feature_series(out, ("queue_local_rank",), np.nan)

    out["quote_depth_ge1"] = (depth >= 1.0).astype(float)
    out["quote_depth_ge2"] = (depth >= 2.0).astype(float)
    out["quote_depth_ge5"] = (depth >= 5.0).astype(float)
    out["quote_dist_ge30"] = (dist >= 30.0).astype(float)
    out["quote_dist_ge40"] = (dist >= 40.0).astype(float)
    out["quote_dist_ge60"] = (dist >= 60.0).astype(float)
    out["quote_dist_40_60"] = ((dist >= 40.0) & (dist < 60.0)).astype(float)
    out["quote_rank_front_0_25"] = ((rank >= 0.0) & (rank < 0.25)).astype(float)
    out["quote_rank_front_0p1_0p5"] = ((rank >= 0.10) & (rank < 0.50)).astype(float)
    out["quote_rank_mid_0p25_0p75"] = ((rank >= 0.25) & (rank < 0.75)).astype(float)
    out["quote_rank_back_0p25_0p9"] = ((rank >= 0.25) & (rank < 0.90)).astype(float)
    out["quote_rank_back_0p5_0p9"] = ((rank >= 0.50) & (rank < 0.90)).astype(float)

    fav = out["quote_ref_favorable_gt2e5"].astype(float)
    for suffix in [
        "depth_ge1",
        "depth_ge2",
        "depth_ge5",
        "dist_ge30",
        "dist_ge40",
        "dist_ge60",
        "dist_40_60",
        "rank_front_0_25",
        "rank_front_0p1_0p5",
        "rank_mid_0p25_0p75",
        "rank_back_0p25_0p9",
        "rank_back_0p5_0p9",
    ]:
        out[f"quote_ref_fav_x_{suffix}"] = fav * out[f"quote_{suffix}"].astype(float)
    out["quote_ref_fav_x_depth_ge1_dist_ge30"] = fav * out["quote_depth_ge1"] * out["quote_dist_ge30"]
    out["quote_ref_fav_x_depth_ge1_dist_ge40"] = fav * out["quote_depth_ge1"] * out["quote_dist_ge40"]
    out["quote_ref_fav_x_depth_ge2_dist_ge30"] = fav * out["quote_depth_ge2"] * out["quote_dist_ge30"]
    out["quote_ref_fav_x_depth_ge2_dist_ge40"] = fav * out["quote_depth_ge2"] * out["quote_dist_ge40"]
    return out


def _first_existing_feature_series(
    frame: pd.DataFrame,
    names: tuple[str, ...],
    default: float = 0.0,
) -> pd.Series:
    for name in names:
        if name in frame:
            return pd.to_numeric(frame[name], errors="coerce")
    return pd.Series(default, index=frame.index, dtype=np.float64)


def _quote_ts_seconds(frame: pd.DataFrame) -> pd.Series:
    for col in ("quote_ts", "submit_ts", "activate_ts", "timestamp_ms", "ts_ms", "timestamp", "quote_dt"):
        if col not in frame.columns:
            continue
        raw = frame[col]
        if pd.api.types.is_numeric_dtype(raw):
            vals = pd.to_numeric(raw, errors="coerce")
            scale = np.where(vals.abs() > 10_000_000_000, 1000.0, 1.0)
            return pd.Series(vals.to_numpy(dtype=np.float64) / scale, index=frame.index)
        dt = pd.to_datetime(raw, errors="coerce", utc=True)
        return pd.Series(dt.astype("int64").to_numpy(dtype=np.float64) / 1e9, index=frame.index).where(dt.notna())
    return pd.Series(np.nan, index=frame.index, dtype=np.float64)


def _clip01_series(value: pd.Series | np.ndarray | float, index: pd.Index) -> pd.Series:
    if isinstance(value, pd.Series):
        series = pd.to_numeric(value, errors="coerce")
    else:
        series = pd.Series(value, index=index, dtype=np.float64)
    return series.fillna(0.0).clip(lower=0.0, upper=1.0)


def _safe_ratio_series(num: pd.Series, den: pd.Series, eps: float = 0.05) -> pd.Series:
    n = pd.to_numeric(num, errors="coerce")
    d = pd.to_numeric(den, errors="coerce").abs().clip(lower=eps)
    return (n / d).replace([np.inf, -np.inf], np.nan)


def _micro_macro_regime_code(ratio: pd.Series, trend_eff: pd.Series) -> pd.Series:
    out = pd.Series(4.0, index=ratio.index, dtype=np.float64)  # mixed / missing
    valid = ratio.notna() & trend_eff.notna()
    micro_high = ratio >= 0.30
    micro_low = ratio < 0.20
    trend_high = trend_eff >= 0.55
    trend_low = trend_eff < 0.35
    out.loc[valid & micro_high & trend_low] = 1.0  # local_noise_macro_flat
    out.loc[valid & micro_low & trend_high] = 2.0  # macro_trend_dominant
    out.loc[valid & micro_high & trend_high] = 3.0  # shock_transition
    out.loc[valid & micro_low & trend_low] = 0.0  # dead_water
    return out


def add_micro_macro_quote_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add quote-time micro/macro path-ratio features for shadow calibration.

    中文说明：这些字段只使用报价时刻之前的 mid path。它们回答：
    1) quote 是否落在短窗自然振幅里，因此更容易成交；
    2) 1m/5m 趋势是否会把成交后的库存带离均值。
    它们是 quote EV / campaign outcome risk 的 shadow 特征，不是 live
    policy 开关。
    """

    if frame.empty:
        return frame.copy()
    out = frame.copy()
    idx = out.index
    side = out.get("side", pd.Series("", index=idx)).astype(str).str.upper()
    ts_s = _quote_ts_seconds(out)
    mid = _first_existing_feature_series(out, ("mid", "quote_mid", "current_mid"), np.nan)
    if mid.isna().all() and {"best_bid", "best_ask"}.issubset(out.columns):
        bid = pd.to_numeric(out["best_bid"], errors="coerce")
        ask = pd.to_numeric(out["best_ask"], errors="coerce")
        mid = (bid + ask) / 2.0
    if "quote_distance_bps" in out.columns:
        quote_dist_bps = pd.to_numeric(out["quote_distance_bps"], errors="coerce").abs()
    else:
        dist = _first_existing_feature_series(out, ("final_distance_to_mid", "raw_distance_to_mid"), np.nan).abs()
        quote_dist_bps = (dist / mid.replace(0.0, np.nan) * 10_000.0).replace([np.inf, -np.inf], np.nan)

    windows = (5, 10, 20, 60, 300)
    for window_s in windows:
        out[f"quote_range_{window_s}s_bps"] = np.nan
        if window_s in (10, 300):
            out[f"quote_rv_{window_s}s_bps"] = np.nan
        out[f"quote_ret_{window_s}s_bps"] = np.nan

    group_keys = out["day"].astype(str) if "day" in out.columns else pd.Series("all", index=idx)
    work = pd.DataFrame({"_ts": ts_s, "_mid": mid, "_group": group_keys}, index=idx)
    for _, locs in work.groupby("_group", sort=False).groups.items():
        part = work.loc[locs].dropna(subset=["_ts", "_mid"]).sort_values("_ts")
        if part.empty:
            continue
        part_index = part.index
        dt_index = pd.to_datetime(part["_ts"], unit="s", utc=True)
        mid_series = pd.Series(part["_mid"].to_numpy(dtype=np.float64), index=dt_index)
        ret1 = mid_series.pct_change().fillna(0.0) * 10_000.0
        for window_s in windows:
            roll = mid_series.rolling(f"{window_s}s", min_periods=1)
            high = roll.max().to_numpy(dtype=np.float64)
            low = roll.min().to_numpy(dtype=np.float64)
            rng = (high - low) / mid_series.to_numpy(dtype=np.float64) * 10_000.0
            out.loc[part_index, f"quote_range_{window_s}s_bps"] = rng
            ts_vals = part["_ts"].to_numpy(dtype=np.float64)
            mid_vals = mid_series.to_numpy(dtype=np.float64)
            left = np.searchsorted(ts_vals, ts_vals - float(window_s), side="left")
            first_mid = mid_vals[left]
            ret = np.where(first_mid > 0.0, (mid_vals - first_mid) / first_mid * 10_000.0, np.nan)
            out.loc[part_index, f"quote_ret_{window_s}s_bps"] = ret
            if window_s in (10, 300):
                rv = ret1.rolling(f"{window_s}s", min_periods=1).std().fillna(0.0).to_numpy(dtype=np.float64)
                out.loc[part_index, f"quote_rv_{window_s}s_bps"] = rv

    range_5 = pd.to_numeric(out["quote_range_5s_bps"], errors="coerce")
    range_10 = pd.to_numeric(out["quote_range_10s_bps"], errors="coerce")
    range_20 = pd.to_numeric(out["quote_range_20s_bps"], errors="coerce")
    range_60 = pd.to_numeric(out["quote_range_60s_bps"], errors="coerce")
    range_300 = pd.to_numeric(out["quote_range_300s_bps"], errors="coerce")
    rv_10 = pd.to_numeric(out["quote_rv_10s_bps"], errors="coerce")
    rv_300 = pd.to_numeric(out["quote_rv_300s_bps"], errors="coerce")
    ret_60 = pd.to_numeric(out["quote_ret_60s_bps"], errors="coerce")
    ret_300 = pd.to_numeric(out["quote_ret_300s_bps"], errors="coerce")

    out["quote_distance_micro_5s"] = _safe_ratio_series(quote_dist_bps, range_5)
    out["quote_distance_micro_10s"] = _safe_ratio_series(quote_dist_bps, range_10)
    out["quote_distance_micro"] = out["quote_distance_micro_10s"]
    out["quote_micro_fill_reach_score"] = (1.0 / (1.0 + out["quote_distance_micro"].clip(lower=0.0) / 3.0)).fillna(0.0)
    out["quote_micro_macro_range_ratio"] = _safe_ratio_series(range_10, range_300)
    out["quote_micro_macro_vol_ratio"] = _safe_ratio_series(rv_10, rv_300)
    out["quote_inventory_horizon_range_ratio"] = _safe_ratio_series(range_20, range_300)
    out["quote_trend_efficiency_60s"] = _safe_ratio_series(ret_60.abs(), range_60)
    out["quote_trend_efficiency_300s"] = _safe_ratio_series(ret_300.abs(), range_300)
    out["quote_side_trend_adverse_60s_bps"] = np.where(side.eq("BUY"), (-ret_60).clip(lower=0.0), ret_60.clip(lower=0.0))
    out["quote_side_trend_adverse_300s_bps"] = np.where(side.eq("BUY"), (-ret_300).clip(lower=0.0), ret_300.clip(lower=0.0))
    regime = _micro_macro_regime_code(out["quote_micro_macro_range_ratio"], out["quote_trend_efficiency_300s"])
    out["quote_micro_macro_regime_code"] = regime
    out["quote_micro_macro_dead_water"] = (regime == 0.0).astype(float)
    out["quote_micro_macro_local_noise_macro_flat"] = (regime == 1.0).astype(float)
    out["quote_micro_macro_macro_trend_dominant"] = (regime == 2.0).astype(float)
    out["quote_micro_macro_shock_transition"] = (regime == 3.0).astype(float)

    xmarket = _first_existing_feature_series(out, ("quote_toxic_xmarket_score",), 0.0).clip(0.0, 1.0)
    trend_adverse_score = pd.concat(
        [
            _clip01_series(out["quote_side_trend_adverse_60s_bps"] / 4.0, idx),
            _clip01_series(out["quote_side_trend_adverse_300s_bps"] / 12.0, idx),
        ],
        axis=1,
    ).max(axis=1)
    trend_eff_score = pd.concat(
        [
            _clip01_series((out["quote_trend_efficiency_60s"] - 0.35) / 0.45, idx),
            _clip01_series((out["quote_trend_efficiency_300s"] - 0.35) / 0.45, idx),
        ],
        axis=1,
    ).max(axis=1)
    macro_dom = _clip01_series((0.30 - out["quote_micro_macro_range_ratio"]) / 0.30, idx) * _clip01_series((out["quote_trend_efficiency_300s"] - 0.35) / 0.45, idx)
    out["quote_trend_inventory_risk_score"] = (
        0.45 * trend_adverse_score
        + 0.25 * trend_eff_score
        + 0.15 * macro_dom
        + 0.15 * xmarket
    ).clip(0.0, 1.0)

    micro_noise = _clip01_series((out["quote_micro_macro_range_ratio"] - 0.15) / 0.35, idx) * _clip01_series(1.0 - out["quote_trend_efficiency_300s"] / 0.55, idx)
    micro_vol = _clip01_series((out["quote_micro_macro_vol_ratio"] - 0.15) / 0.35, idx)
    refill = _first_existing_feature_series(out, ("quote_resil_refill_component",), np.nan)
    if refill.isna().all():
        refill_edge = _first_existing_feature_series(out, ("quote_local_depth_refill_edge", "depth_refill_edge"), 0.0)
        refill = ((refill_edge + 0.02) / 0.10).clip(0.0, 1.0)
    flow = _first_existing_feature_series(out, ("quote_resil_flow_decay_component",), np.nan)
    if flow.isna().all():
        flow_decel = _first_existing_feature_series(out, ("quote_local_flow_deceleration_30s_5s", "flow_deceleration_30s_5s"), 0.0)
        flow = (flow_decel / 0.50).clip(0.0, 1.0)
    out["quote_micro_reversion_score"] = (
        0.35 * micro_noise
        + 0.20 * micro_vol
        + 0.20 * refill.fillna(0.0)
        + 0.15 * flow.fillna(0.0)
        + 0.10 * (1.0 - xmarket)
    ).clip(0.0, 1.0)
    sell = side.eq("SELL").astype(float)
    out["quote_sell_x_trend_inventory_risk"] = sell * out["quote_trend_inventory_risk_score"]
    out["quote_sell_x_micro_reversion"] = sell * out["quote_micro_reversion_score"]
    for col in MICRO_MACRO_QUOTE_FEATURES:
        if col not in out.columns:
            out[col] = 0.0
    return out


def add_local_flow_quote_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add local-flow/replenishment features for quote EV research.

    这些字段只依赖 quote/order 行在报价时刻已经可见的本市场 flow、L2
    refill/cancel 和 microprice 状态；不使用成交后 markout 或 shock label。
    """

    out = frame.copy()
    side = out.get("side", pd.Series("", index=out.index)).astype(str).str.upper()
    pos = _side_position_sign(side)

    adverse: dict[int, pd.Series] = {}
    for horizon in (5, 10, 30, 60):
        raw = _first_existing_feature_series(out, (f"taker_quote_imbalance_{horizon}s",), 0.0)
        adv = -pos * raw.fillna(0.0)
        adverse[horizon] = adv
        out[f"quote_local_adverse_flow_{horizon}s"] = adv

    out["quote_local_flow_deceleration_30s_5s"] = adverse[30] - adverse[5]
    out["quote_local_flow_deceleration_60s_10s"] = adverse[60] - adverse[10]
    out["quote_local_flow_reversal_score"] = (-adverse[5]).where(adverse[30] > 0.10, 0.0)

    out["quote_local_pressure_absent"] = (adverse[30].abs() < 0.10).astype(float)
    out["quote_local_pressure_reversing"] = ((adverse[30] >= 0.10) & (adverse[5] <= -0.10)).astype(float)
    out["quote_local_pressure_decelerating"] = (
        (adverse[30] >= 0.35) & ((adverse[30] - adverse[5]) >= 0.25)
    ).astype(float)
    out["quote_local_pressure_mild_decelerating"] = (
        (adverse[30] >= 0.10) & ((adverse[30] - adverse[5]) >= 0.15)
    ).astype(float)
    out["quote_local_pressure_persistent"] = ((adverse[30] >= 0.35) & (adverse[5] >= 0.35)).astype(float)
    out["quote_local_favorable_persistent"] = ((adverse[30] <= -0.35) & (adverse[5] <= -0.10)).astype(float)

    refresh = _first_existing_feature_series(
        out,
        ("l2_book_refresh_ratio_y", "l2_book_refresh_ratio_x", "l2_book_refresh_ratio"),
        0.0,
    ).fillna(0.0)
    cancel = _first_existing_feature_series(
        out,
        ("l2_book_cancel_ratio_y", "l2_book_cancel_ratio_x", "l2_book_cancel_ratio"),
        0.0,
    ).fillna(0.0)
    refill_edge = refresh - cancel
    out["quote_local_depth_refill_edge"] = refill_edge
    out["quote_local_refill_dominant"] = (refill_edge > 0.10).astype(float)
    out["quote_local_cancel_dominant"] = (refill_edge < -0.10).astype(float)
    out["quote_local_depth_balanced"] = ((refill_edge >= -0.10) & (refill_edge <= 0.10)).astype(float)

    micro = _first_existing_feature_series(out, ("local_adverse_microprice",), np.nan)
    if micro.isna().all():
        raw_micro = _first_existing_feature_series(
            out,
            ("l2_microprice_offset_bps", "microprice_shift_bps"),
            0.0,
        ).fillna(0.0)
        micro = -pos * raw_micro
    out["quote_local_adverse_microprice_bps"] = micro.fillna(0.0)
    out["quote_local_micro_favorable"] = (out["quote_local_adverse_microprice_bps"] < -0.25).astype(float)
    out["quote_local_micro_neutral"] = (out["quote_local_adverse_microprice_bps"].abs() <= 0.25).astype(float)
    out["quote_local_micro_adverse"] = (out["quote_local_adverse_microprice_bps"] > 0.25).astype(float)

    if "final_distance_to_mid" in out.columns:
        dist = pd.to_numeric(out["final_distance_to_mid"], errors="coerce").abs()
    elif "raw_distance_to_mid" in out.columns:
        dist = pd.to_numeric(out["raw_distance_to_mid"], errors="coerce").abs()
    else:
        dist = pd.Series(0.0, index=out.index, dtype=np.float64)
    rank = pd.to_numeric(out.get("queue_local_rank", 0.0), errors="coerce").fillna(0.0)
    sell = side.eq("SELL").astype(float)
    fav = out["quote_local_favorable_persistent"].astype(float)
    mild_decel = out["quote_local_pressure_mild_decelerating"].astype(float)
    refill = out["quote_local_refill_dominant"].astype(float)
    front_rank = ((rank >= 0.0) & (rank < 0.25)).astype(float)
    back_rank = ((rank >= 0.50) & (rank < 0.90)).astype(float)
    dist30_40 = ((dist >= 30.0) & (dist < 40.0)).astype(float)

    out["quote_local_fav_persistent_x_refill"] = fav * refill
    out["quote_local_fav_persistent_x_front_rank"] = fav * front_rank
    out["quote_local_fav_persistent_x_dist30_40"] = fav * dist30_40
    out["quote_local_fav_persistent_x_sell"] = fav * sell
    out["quote_local_fav_persistent_x_sell_refill"] = fav * sell * refill
    out["quote_local_mild_decel_x_refill"] = mild_decel * refill
    out["quote_local_mild_decel_x_refill_back_rank"] = mild_decel * refill * back_rank
    return out


def add_local_resiliency_quote_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add the Stage-S quote-time local-resiliency score and low-cardinality buckets.

    中文说明：这和 `session_conditioned_evidence.py` 的 Stage-S score 使用同
    一套口径，但字段加 quote_ 前缀，只作为 quote EV / shadow calibration
    特征。它不读取成交后 markout，也不直接改变 policy。
    """

    out = add_local_flow_quote_features(frame)
    side = out.get("side", pd.Series("", index=out.index)).astype(str).str.upper()
    depth = _first_existing_feature_series(
        out,
        ("near_depth_for_bucket", "l2_near_depth_total", "near_depth_total", "cpp_near_depth_total"),
        0.0,
    ).clip(lower=0.0)
    rank = _first_existing_feature_series(
        out,
        ("queue_rank_for_bucket", "queue_local_rank", "queue_ahead_ratio"),
        1.0,
    ).clip(lower=0.0, upper=1.0)
    refill_edge = _first_existing_feature_series(
        out,
        ("depth_refill_edge", "quote_local_depth_refill_edge"),
        0.0,
    )
    flow_decel = _first_existing_feature_series(
        out,
        ("flow_deceleration_30s_5s", "quote_local_flow_deceleration_30s_5s"),
        0.0,
    )
    adverse_5s = _first_existing_feature_series(
        out,
        ("adverse_flow_5s", "quote_local_adverse_flow_5s"),
        0.0,
    )

    depth_score = np.log1p(depth.clip(upper=10.0)) / np.log1p(10.0)
    refill_score = ((refill_edge + 0.10) / 0.35).clip(lower=0.0, upper=1.0)
    queue_score = (1.0 - rank).clip(lower=0.0, upper=1.0)
    flow_decay_score = ((flow_decel + 0.10) / 0.45).clip(lower=0.0, upper=1.0)
    persistent_penalty = (adverse_5s.clip(lower=0.0, upper=0.60) / 0.60) * 0.20
    score = (
        0.30 * depth_score
        + 0.25 * refill_score
        + 0.20 * queue_score
        + 0.25 * flow_decay_score
        - persistent_penalty
    ).clip(lower=0.0, upper=1.0)

    out["quote_resil_depth_component"] = depth_score
    out["quote_resil_refill_component"] = refill_score
    out["quote_resil_queue_component"] = queue_score
    out["quote_resil_flow_decay_component"] = flow_decay_score
    out["quote_resil_persistent_adverse_penalty"] = persistent_penalty
    out["quote_local_resiliency_score"] = score
    out["quote_local_resiliency_bucket_code"] = pd.cut(
        score,
        bins=[-np.inf, 0.25, 0.45, 0.65, np.inf],
        labels=[0.0, 1.0, 2.0, 3.0],
        include_lowest=True,
    ).astype(float).fillna(-1.0)

    out["quote_resil_brittle"] = (score <= 0.25).astype(float)
    out["quote_resil_weak"] = ((score > 0.25) & (score <= 0.45)).astype(float)
    out["quote_resil_mid"] = ((score > 0.45) & (score <= 0.65)).astype(float)
    out["quote_resil_strong"] = (score > 0.65).astype(float)

    out["quote_resil_depth_low"] = (depth_score <= 0.35).astype(float)
    out["quote_resil_depth_mid"] = ((depth_score > 0.35) & (depth_score <= 0.65)).astype(float)
    out["quote_resil_depth_high"] = (depth_score > 0.65).astype(float)
    out["quote_resil_refill_low"] = (refill_score <= 0.35).astype(float)
    out["quote_resil_refill_mid"] = ((refill_score > 0.35) & (refill_score <= 0.65)).astype(float)
    out["quote_resil_refill_high"] = (refill_score > 0.65).astype(float)
    out["quote_resil_queue_low"] = (queue_score <= 0.35).astype(float)
    out["quote_resil_queue_mid"] = ((queue_score > 0.35) & (queue_score <= 0.65)).astype(float)
    out["quote_resil_queue_high"] = (queue_score > 0.65).astype(float)
    out["quote_resil_flow_decay_low"] = (flow_decay_score <= 0.35).astype(float)
    out["quote_resil_flow_decay_mid"] = ((flow_decay_score > 0.35) & (flow_decay_score <= 0.65)).astype(float)
    out["quote_resil_flow_decay_high"] = (flow_decay_score > 0.65).astype(float)

    sell = side.eq("SELL").astype(float)
    out["quote_sell_x_resil_strong"] = sell * out["quote_resil_strong"]
    out["quote_sell_x_resil_queue_high_flow_high"] = (
        sell * out["quote_resil_queue_high"] * out["quote_resil_flow_decay_high"]
    )
    return out


def add_toxic_risk_quote_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add Stage-T toxic SELL risk labels as continuous quote-time features.

    中文说明：这些字段来自历史 Stage T toxic-risk evidence，但这里只用报价
    时刻已经可见的状态重建，不读取成交后 markout。它们的定位是 quote EV /
    shadow calibration 的连续风险特征，不是 hard veto。
    """

    out = add_local_flow_quote_features(frame)
    side = out.get("side", pd.Series("", index=out.index)).astype(str).str.upper()
    pos = _side_position_sign(side)
    sell = side.eq("SELL").astype(float)

    adverse_guard = (
        _first_existing_feature_series(out, ("side_adverse",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("side_adverse_pause",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("adverse_toxicity",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("adverse_markout",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("adverse_thin_depth",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("bid_adverse",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("ask_adverse",), 0.0).fillna(0.0).gt(0.5)
    )
    defense_guard = (
        _first_existing_feature_series(out, ("defense_guard",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("defense_pause",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("defense_markout",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("defense_direction",), 0.0).fillna(0.0).gt(0.5)
        | _first_existing_feature_series(out, ("defense_microprice",), 0.0).fillna(0.0).gt(0.5)
    )
    guard_adverse_defense = (adverse_guard & defense_guard).astype(float)

    if "quote_local_adverse_flow_30s" in out.columns:
        adverse_flow_30s = pd.to_numeric(out["quote_local_adverse_flow_30s"], errors="coerce").fillna(0.0)
    else:
        raw_flow = _first_existing_feature_series(out, ("taker_quote_imbalance_30s",), 0.0).fillna(0.0)
        adverse_flow_30s = -pos * raw_flow
    flow_score = ((adverse_flow_30s - 0.10) / 0.40).clip(lower=0.0, upper=1.0)
    flow_weak_adverse = ((adverse_flow_30s >= 0.10) & (adverse_flow_30s < 0.35)).astype(float)
    flow_strong_adverse = (adverse_flow_30s >= 0.35).astype(float)

    if "quote_local_depth_refill_edge" in out.columns:
        refill_edge = pd.to_numeric(out["quote_local_depth_refill_edge"], errors="coerce").fillna(0.0)
    else:
        refresh = _first_existing_feature_series(
            out,
            ("l2_book_refresh_ratio_y", "l2_book_refresh_ratio_x", "l2_book_refresh_ratio"),
            0.0,
        ).fillna(0.0)
        cancel = _first_existing_feature_series(
            out,
            ("l2_book_cancel_ratio_y", "l2_book_cancel_ratio_x", "l2_book_cancel_ratio"),
            0.0,
        ).fillna(0.0)
        refill_edge = refresh - cancel
    refill_dominant = (refill_edge > 0.10).astype(float)

    bid_mo = _first_existing_feature_series(out, ("mo_ema_bid",), 0.0).fillna(0.0)
    ask_mo = _first_existing_feature_series(out, ("mo_ema_ask",), 0.0).fillna(0.0)
    side_mo = pd.Series(np.where(side.eq("BUY"), bid_mo, ask_mo), index=out.index, dtype=np.float64)
    mo_score = ((-side_mo - 3.0) / 17.0).clip(lower=0.0, upper=1.0)
    mo_neg10_neg3 = ((side_mo >= -10.0) & (side_mo < -3.0)).astype(float)
    mo_lt_neg10 = (side_mo < -10.0).astype(float)

    ref_cols: list[pd.Series] = []
    for horizon in (10, 30, 60):
        col = f"quote_ref_adverse_ret_{horizon}s"
        if col in out.columns:
            ref_cols.append(pd.to_numeric(out[col], errors="coerce"))
        else:
            raw = _first_existing_feature_series(out, (f"cv_ref_perp_ret_{horizon}s",), np.nan)
            ref_cols.append(-pos * raw)
    ref_adv = pd.concat(ref_cols, axis=1).max(axis=1, skipna=True).fillna(0.0)

    spot_cols: list[pd.Series] = []
    for prefix in ("cv_exec_spot", "cv_ref_spot"):
        for horizon in (10, 30, 60):
            raw = _first_existing_feature_series(out, (f"{prefix}_ret_{horizon}s",), np.nan)
            spot_cols.append(-pos * raw)
    spot_adv = pd.concat(spot_cols, axis=1).max(axis=1, skipna=True).fillna(0.0) if spot_cols else pd.Series(0.0, index=out.index)
    # Toxic risk is derived only from causal quote-time returns. Historical
    # xmarket_*_adverse fields represented a retired direct-policy action, not
    # an independently observed market state.
    ref_adverse = (ref_adv > 2e-5).astype(float)
    if "quote_ref_adverse_gt2e5" in out.columns:
        quote_ref_flag = pd.to_numeric(out["quote_ref_adverse_gt2e5"], errors="coerce").fillna(0.0).gt(0.5).astype(float)
        ref_adverse = np.maximum(ref_adverse, quote_ref_flag)

    spot_adverse = (spot_adv > 2e-5).astype(float)
    if "quote_spot_adverse_gt2e5" in out.columns:
        quote_spot_flag = pd.to_numeric(out["quote_spot_adverse_gt2e5"], errors="coerce").fillna(0.0).gt(0.5).astype(float)
        spot_adverse = np.maximum(spot_adverse, quote_spot_flag)
    ref_spot_adverse = ref_adverse * spot_adverse

    depth = _first_existing_feature_series(
        out,
        ("near_depth_for_bucket", "l2_near_depth_total", "near_depth_total", "cpp_near_depth_total"),
        np.nan,
    )
    rank = _first_existing_feature_series(
        out,
        ("queue_rank_for_bucket", "queue_local_rank", "queue_ahead_ratio"),
        np.nan,
    ).clip(lower=0.0, upper=1.0)
    if "final_distance_to_mid" in out.columns:
        dist = pd.to_numeric(out["final_distance_to_mid"], errors="coerce").abs()
    elif "raw_distance_to_mid" in out.columns:
        dist = pd.to_numeric(out["raw_distance_to_mid"], errors="coerce").abs()
    else:
        dist = pd.Series(np.nan, index=out.index, dtype=np.float64)

    depth_0p5_1 = ((depth >= 0.5) & (depth < 1.0)).astype(float)
    depth_1_2 = ((depth >= 1.0) & (depth < 2.0)).astype(float)
    depth_0p5_2 = ((depth >= 0.5) & (depth < 2.0)).astype(float)
    rank_front = ((rank >= 0.0) & (rank < 0.25)).astype(float)
    rank_back = (rank >= 0.75).astype(float)
    dist_30_40 = ((dist >= 30.0) & (dist < 40.0)).astype(float)
    dist_40_60 = ((dist >= 40.0) & (dist < 60.0)).astype(float)
    queue_depth_score = (
        0.45 * depth_0p5_2
        + 0.25 * rank_front
        + 0.15 * rank_back
        + 0.15 * ((dist >= 30.0) & (dist < 60.0)).astype(float)
    ).clip(lower=0.0, upper=1.0)

    guard_flow_score = (guard_adverse_defense * refill_dominant * (0.55 * flow_score + 0.45 * mo_score)).clip(0.0, 1.0)
    xmarket_score = (ref_spot_adverse * (0.50 + 0.30 * depth_0p5_2 + 0.20 * ((dist >= 30.0) & (dist < 60.0)).astype(float))).clip(0.0, 1.0)
    toxic_score = (
        0.42 * guard_flow_score
        + 0.28 * xmarket_score
        + 0.18 * queue_depth_score
        + 0.12 * mo_score
    ).clip(0.0, 1.0)

    guard_flow_flag = ((guard_adverse_defense > 0.5) & (refill_dominant > 0.5) & ((flow_weak_adverse > 0.5) | (flow_strong_adverse > 0.5)) & ((mo_neg10_neg3 > 0.5) | (mo_lt_neg10 > 0.5))).astype(float)
    xmarket_local_flag = ((ref_spot_adverse > 0.5) & (((depth_0p5_1 > 0.5) | (depth_1_2 > 0.5)) | ((dist_30_40 > 0.5) | (dist_40_60 > 0.5)))).astype(float)

    out["quote_toxic_risk_score"] = toxic_score
    out["quote_sell_toxic_risk_score"] = sell * toxic_score
    out["quote_toxic_guard_flow_score"] = guard_flow_score
    out["quote_toxic_xmarket_score"] = xmarket_score
    out["quote_toxic_queue_depth_score"] = queue_depth_score
    out["quote_toxic_side_mo_ema"] = side_mo
    out["quote_toxic_adverse_flow_30s"] = adverse_flow_30s
    out["quote_toxic_ref_adverse_ret_max"] = ref_adv
    out["quote_toxic_spot_adverse_ret_max"] = spot_adv
    out["quote_toxic_guard_adverse_defense"] = guard_adverse_defense
    out["quote_toxic_refill_dominant"] = refill_dominant
    out["quote_toxic_flow_weak_adverse"] = flow_weak_adverse
    out["quote_toxic_flow_strong_adverse"] = flow_strong_adverse
    out["quote_toxic_mo_neg10_neg3"] = mo_neg10_neg3
    out["quote_toxic_mo_lt_neg10"] = mo_lt_neg10
    out["quote_toxic_ref_adverse"] = ref_adverse
    out["quote_toxic_spot_adverse"] = spot_adverse
    out["quote_toxic_ref_spot_adverse"] = ref_spot_adverse
    out["quote_toxic_depth_0p5_1"] = depth_0p5_1
    out["quote_toxic_depth_1_2"] = depth_1_2
    out["quote_toxic_depth_0p5_2"] = depth_0p5_2
    out["quote_toxic_rank_front_0_25"] = rank_front
    out["quote_toxic_rank_back_0_75_1"] = rank_back
    out["quote_toxic_dist_30_40"] = dist_30_40
    out["quote_toxic_dist_40_60"] = dist_40_60
    out["quote_toxic_guard_flow_flag"] = guard_flow_flag
    out["quote_toxic_xmarket_local_flag"] = xmarket_local_flag
    out["quote_sell_x_toxic_guard_flow"] = sell * guard_flow_flag
    out["quote_sell_x_toxic_ref_spot_adverse"] = sell * ref_spot_adverse
    out["quote_sell_x_toxic_xmarket_shallow_rank"] = sell * xmarket_local_flag * (rank_front + rank_back).clip(0.0, 1.0)
    return out


def add_calendar_quote_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add quote-time calendar/session features for quote EV evidence.

    中文说明：这里显式使用报价时刻 quote_ts/submit_ts，而不是运行脚本时的机器时间。
    所有字段以 quote_cal_* 开头，避免和主模型 legacy 时间特征混淆。
    """

    out = frame.copy()
    for col in ("quote_ts", "submit_ts", "activate_ts", "timestamp_ms", "ts_ms", "timestamp", "quote_dt"):
        if col in out.columns:
            return add_calendar_features(out, ts_col=col, prefix="quote_cal_", include_legacy=False)
    for col in CALENDAR_QUOTE_FEATURES:
        out[col] = 0.0
    return out


def _select_feature_cols(frame: pd.DataFrame, feature_mode: str = "base") -> list[str]:
    cols = [col for col in DEFAULT_BID_QUOTE_FEATURES if col in frame.columns]
    if "inventory_ratio" not in cols and "inventory_ratio" in frame.columns:
        cols.append("inventory_ratio")
    mode = str(feature_mode or "base").lower()
    # feature-mode 是证据边界：xmarket/cpp-context 只使用 quote-time 可见字段；
    # shock 是离线归因用，不能直接当 live quote EV promotion 特征。
    xmarket_modes = {
        "xmarket",
        "shock",
        "xmarket_cpp",
        "xmarket_interaction",
        "xmarket_interaction_cpp",
        "local_flow_xmarket",
        "local_flow_xmarket_interaction",
        "local_flow_xmarket_interaction_cpp",
        "calendar_local_flow_xmarket",
        "calendar_local_flow_xmarket_interaction",
        "calendar_local_flow_xmarket_interaction_cpp",
    }
    uses_xmarket = mode in xmarket_modes or "xmarket" in mode
    uses_interaction = mode in {
        "xmarket_interaction",
        "xmarket_interaction_cpp",
        "local_flow_xmarket_interaction",
        "local_flow_xmarket_interaction_cpp",
        "calendar_local_flow_xmarket_interaction",
        "calendar_local_flow_xmarket_interaction_cpp",
    } or "xmarket_interaction" in mode
    uses_cpp = mode in {
        "cpp-context",
        "xmarket_cpp",
        "xmarket_interaction_cpp",
        "local_flow_xmarket_interaction_cpp",
        "calendar_local_flow_xmarket_interaction_cpp",
    } or mode.endswith("_cpp") or "_cpp" in mode
    uses_local_flow = mode in {
        "local_flow",
        "local_flow_xmarket",
        "local_flow_xmarket_interaction",
        "local_flow_xmarket_interaction_cpp",
        "calendar_local_flow",
        "calendar_local_flow_xmarket",
        "calendar_local_flow_xmarket_interaction",
        "calendar_local_flow_xmarket_interaction_cpp",
    } or "local_resiliency" in mode
    uses_calendar = mode in {
        "calendar",
        "calendar_local_flow",
        "calendar_local_flow_xmarket",
        "calendar_local_flow_xmarket_interaction",
        "calendar_local_flow_xmarket_interaction_cpp",
    } or mode.startswith("calendar_")
    uses_resiliency = "local_resiliency" in mode
    uses_toxic_risk = "toxic_risk" in mode
    uses_micro_macro = "micro_macro" in mode
    if uses_xmarket:
        for col in XMARKET_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if uses_cpp:
        for col in CPP_CONTEXT_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if uses_interaction:
        for col in XMARKET_INTERACTION_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if uses_local_flow:
        for col in LOCAL_FLOW_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if uses_resiliency:
        for col in LOCAL_RESILIENCY_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if uses_toxic_risk:
        for col in TOXIC_RISK_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if uses_micro_macro:
        for col in MICRO_MACRO_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if uses_calendar:
        for col in CALENDAR_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    if mode == "shock":
        for col in SHOCK_QUOTE_FEATURES:
            if col in frame.columns and col not in cols:
                cols.append(col)
    return cols


def _day_filter(frame: pd.DataFrame, days: list[str]) -> pd.DataFrame:
    if not days:
        return frame
    return frame.loc[frame["day"].astype(str).isin(days)].copy()


def _binary_calibration_bins(
    y_true: pd.Series,
    prob: np.ndarray,
    n_bins: int = 10,
) -> list[dict[str, float | int]]:
    y = pd.to_numeric(y_true, errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    p = np.asarray(prob, dtype=np.float64)
    if len(y) == 0:
        return []
    bins = np.minimum(np.floor(np.clip(p, 0.0, 1.0) * n_bins).astype(int), n_bins - 1)
    rows: list[dict[str, float | int]] = []
    for idx in range(n_bins):
        mask = bins == idx
        if not np.any(mask):
            continue
        pred_mean = float(p[mask].mean())
        actual_rate = float(y[mask].mean())
        rows.append({
            "bin": int(idx),
            "count": int(mask.sum()),
            "pred_mean": pred_mean,
            "actual_rate": actual_rate,
            "abs_error": float(abs(pred_mean - actual_rate)),
        })
    return rows


def _numeric_calibration_bins(
    y_true: pd.Series | np.ndarray,
    pred: np.ndarray,
    n_bins: int = 10,
) -> list[dict[str, float | int]]:
    y = pd.to_numeric(pd.Series(y_true), errors="coerce").to_numpy(dtype=np.float64)
    p = np.asarray(pred, dtype=np.float64).reshape(-1)
    mask = np.isfinite(y) & np.isfinite(p)
    y = y[mask]
    p = p[mask]
    if len(y) == 0:
        return []
    order = np.argsort(p)
    rows: list[dict[str, float | int]] = []
    for idx, subset in enumerate(np.array_split(order, min(n_bins, len(order)))):
        if len(subset) == 0:
            continue
        pred_mean = float(p[subset].mean())
        actual_mean = float(y[subset].mean())
        rows.append({
            "bin": int(idx),
            "count": int(len(subset)),
            "pred_mean": pred_mean,
            "actual_mean": actual_mean,
            "abs_error": float(abs(pred_mean - actual_mean)),
        })
    return rows


def _train_lgbm(
    name: str,
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    label_col: str,
    is_classifier: bool,
    model_dir: Path,
    *,
    min_child_samples: int = 200,
    model_kind: str | None = None,
    extra_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    import lightgbm as lgb

    if train_df.empty or valid_df.empty:
        raise ValueError(f"{name}: empty split train={len(train_df)} valid={len(valid_df)}")
    X_train = _clean_feature_frame(train_df, feature_cols)
    y_train = pd.to_numeric(train_df[label_col], errors="coerce").fillna(0.0)
    X_valid = _clean_feature_frame(valid_df, feature_cols)
    y_valid = pd.to_numeric(valid_df[label_col], errors="coerce").fillna(0.0)
    kind = model_kind or ("binary" if is_classifier else "regression")
    if kind in {"binary", "multiclass"} and y_train.nunique() < 2:
        raise ValueError(f"{name}: classifier needs both classes in train split")
    sample_weight = None
    if kind == "multiclass":
        y_train = y_train.astype(np.int32)
        y_valid = y_valid.astype(np.int32)
        required_classes = sorted(set(int(v) for v in y_train.unique()) | set(int(v) for v in y_valid.unique()))
        missing_classes = [cls for cls in required_classes if cls not in set(int(v) for v in y_train.unique())]
        if missing_classes:
            pad_x = X_train.iloc[[0] * len(missing_classes)].copy()
            X_train = pd.concat([X_train.reset_index(drop=True), pad_x.reset_index(drop=True)], ignore_index=True)
            y_train = pd.concat(
                [y_train.reset_index(drop=True), pd.Series(missing_classes, dtype=np.int32)],
                ignore_index=True,
            )
            sample_weight = np.concatenate([
                np.ones(len(train_df), dtype=np.float64),
                np.zeros(len(missing_classes), dtype=np.float64),
            ])

    base_params: dict[str, Any] = {
        "n_estimators": 1200,
        "learning_rate": 0.03,
        "num_leaves": 63,
        "max_depth": 7,
        "min_child_samples": min_child_samples,
        "subsample": 0.8,
        "subsample_freq": 1,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "random_state": 42,
        "n_jobs": 4,
        "verbose": -1,
        "force_col_wise": True,
    }
    if kind == "binary":
        base_params.update({"objective": "binary", "metric": "auc"})
        model = lgb.LGBMClassifier(**base_params)
    elif kind == "multiclass":
        base_params.update({"objective": "multiclass", "metric": "multi_logloss"})
        model = lgb.LGBMClassifier(**base_params)
    elif kind == "regression":
        base_params.update({"objective": "regression", "metric": "l1"})
        model = lgb.LGBMRegressor(**base_params)
    else:
        raise ValueError(f"Unknown model_kind={kind!r}")

    callbacks = [
        lgb.early_stopping(stopping_rounds=80, verbose=False),
        lgb.log_evaluation(period=0),
    ]
    fit_kw: dict[str, Any] = {
        "X": X_train,
        "y": y_train,
        "eval_set": [(X_valid, y_valid)],
        "callbacks": callbacks,
    }
    if sample_weight is not None:
        fit_kw["sample_weight"] = sample_weight
    model.fit(**fit_kw)

    metrics: dict[str, Any] = {
        "name": name,
        "label_col": label_col,
        "model_kind": kind,
        "feature_cols": feature_cols,
        "n_features": len(feature_cols),
        "train_rows": int(len(train_df)),
        "valid_rows": int(len(valid_df)),
        "train_label_mean": float(y_train.mean()),
        "valid_label_mean": float(y_valid.mean()),
        "best_iteration": int(model.best_iteration_ or 0),
    }
    if extra_meta:
        metrics.update(extra_meta)
    if kind == "binary":
        prob = model.predict_proba(X_valid)[:, 1]
        metrics["valid_positive_rate"] = float(y_valid.mean())
        metrics["valid_pred_mean"] = float(np.mean(prob))
        metrics["valid_calibration_bins"] = _binary_calibration_bins(y_valid, prob)
        if metrics["valid_calibration_bins"]:
            metrics["valid_calibration_mae"] = float(np.mean([
                float(row["abs_error"]) for row in metrics["valid_calibration_bins"]
            ]))
        else:
            metrics["valid_calibration_mae"] = None
        try:
            from sklearn.metrics import brier_score_loss

            metrics["valid_brier"] = float(brier_score_loss(y_valid, prob))
        except Exception:
            metrics["valid_brier"] = None
        if y_valid.nunique() > 1:
            from sklearn.metrics import roc_auc_score, log_loss

            metrics["valid_auc"] = float(roc_auc_score(y_valid, prob))
            metrics["valid_logloss"] = float(log_loss(y_valid, prob, labels=[0, 1]))
        else:
            metrics["valid_auc"] = None
            metrics["valid_logloss"] = None
        metrics["classes"] = [int(c) for c in getattr(model, "classes_", [0, 1])]
    elif kind == "multiclass":
        from sklearn.metrics import accuracy_score, balanced_accuracy_score, log_loss

        prob = model.predict_proba(X_valid)
        classes = [int(c) for c in getattr(model, "classes_", [])]
        pred_idx = np.argmax(prob, axis=1)
        pred = np.array([classes[i] if i < len(classes) else int(i) for i in pred_idx])
        all_classes = sorted(set(classes) | set(int(v) for v in y_valid.unique()))
        aligned = np.full((len(y_valid), len(all_classes)), 1e-15, dtype=np.float64)
        class_to_idx = {cls: i for i, cls in enumerate(all_classes)}
        for idx, cls in enumerate(classes):
            aligned[:, class_to_idx[cls]] = prob[:, idx]
        aligned = aligned / aligned.sum(axis=1, keepdims=True)
        metrics["classes"] = classes
        metrics["valid_accuracy"] = float(accuracy_score(y_valid, pred))
        metrics["valid_balanced_accuracy"] = float(balanced_accuracy_score(y_valid, pred))
        metrics["valid_logloss"] = float(log_loss(y_valid, aligned, labels=all_classes))
        value_col = metrics.get("realized_markout_col")
        bucket_values = metrics.get("bucket_values") or DEFAULT_MARKOUT_BUCKET_VALUES
        if value_col and str(value_col) in valid_df.columns:
            class_values = np.zeros(len(all_classes), dtype=np.float64)
            for idx, cls in enumerate(all_classes):
                class_values[idx] = float(
                    bucket_values[cls] if 0 <= cls < len(bucket_values) else bucket_values[-1]
                )
            aligned = np.nan_to_num(aligned, nan=0.0, posinf=0.0, neginf=0.0)
            aligned = np.clip(aligned, 0.0, None)
            row_sum = aligned.sum(axis=1)
            valid_sum = np.isfinite(row_sum) & (row_sum > 0.0)
            normalized = np.zeros_like(aligned)
            if bool(valid_sum.any()):
                normalized[valid_sum] = aligned[valid_sum] / row_sum[valid_sum, None]
            with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
                expected_markout = normalized @ class_values
            expected_markout = np.nan_to_num(expected_markout, nan=0.0, posinf=0.0, neginf=0.0)
            actual_markout = pd.to_numeric(valid_df[str(value_col)], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
            markout_err = np.abs(actual_markout - expected_markout)
            metrics["valid_expected_markout_mae"] = float(markout_err.mean())
            metrics["valid_expected_markout_pred_mean"] = float(expected_markout.mean())
            metrics["valid_expected_markout_actual_mean"] = float(actual_markout.mean())
            if np.std(expected_markout) > 1e-12 and np.std(actual_markout) > 1e-12:
                metrics["valid_expected_markout_corr"] = float(np.corrcoef(expected_markout, actual_markout)[0, 1])
            else:
                metrics["valid_expected_markout_corr"] = 0.0
            bins = _numeric_calibration_bins(actual_markout, expected_markout)
            metrics["valid_expected_markout_calibration_bins"] = bins
            metrics["valid_expected_markout_calibration_mae"] = (
                float(np.mean([float(row["abs_error"]) for row in bins])) if bins else None
            )
    else:
        pred = model.predict(X_valid)
        err = np.abs(y_valid.to_numpy(dtype=np.float64) - pred)
        metrics["valid_mae"] = float(err.mean())
        metrics["valid_pred_mean"] = float(np.mean(pred))
        if np.std(pred) > 1e-12 and np.std(y_valid) > 1e-12:
            metrics["valid_corr"] = float(np.corrcoef(pred, y_valid)[0, 1])
        else:
            metrics["valid_corr"] = 0.0

    model_dir.mkdir(parents=True, exist_ok=True)
    model.booster_.save_model(str(model_dir / f"{name}.txt"))
    with open(model_dir / f"{name}_meta.json", "w") as f:
        json.dump(metrics, f, indent=2)
    return metrics


def _empirical_bucket_values(frame: pd.DataFrame, bucket_col: str, value_col: str) -> list[float]:
    values = list(DEFAULT_MARKOUT_BUCKET_VALUES)
    if frame.empty or bucket_col not in frame or value_col not in frame:
        return values
    grouped = frame.groupby(bucket_col)[value_col].mean()
    for bucket, value in grouped.items():
        bucket_idx = int(bucket)
        if 0 <= bucket_idx < len(values) and pd.notna(value):
            values[bucket_idx] = float(value)
    return values


def _write_summary(path: Path, metrics: list[dict[str, Any]], label_df: pd.DataFrame, prefix: str):
    def _to_md(frame: pd.DataFrame) -> str:
        if frame.empty:
            return "_empty_"
        cols = list(frame.columns)
        lines = [
            "| " + " | ".join(cols) + " |",
            "| " + " | ".join("---" for _ in cols) + " |",
        ]
        for _, row in frame.iterrows():
            lines.append("| " + " | ".join(str(row[col]) for col in cols) + " |")
        return "\n".join(lines)

    filled_col = f"label_{prefix}_filled"
    toxic_col = f"label_{prefix}_toxic_30s"
    extreme_col = f"label_{prefix}_extreme_adverse_any"
    ev_col = f"label_{prefix}_fill_ev_30s"
    lines = [f"# {prefix.title()} Quote EV Training", ""]
    lines.append("## Label Summary")
    lines.append("")
    summary = label_df.groupby("day").agg(
        rows=("order_id", "count"),
        filled=(filled_col, "sum"),
        fill_rate=(filled_col, "mean"),
        toxic=(toxic_col, "sum"),
        extreme_adverse=(extreme_col, "sum"),
        avg_ev=(ev_col, "mean"),
        avg_ev_filled=(ev_col, lambda s: float(s[label_df.loc[s.index, filled_col] > 0].mean()) if (label_df.loc[s.index, filled_col] > 0).any() else 0.0),
        avg_markout_1s_filled=(_label_markout_col(1, prefix), lambda s: float(s[label_df.loc[s.index, filled_col] > 0].mean()) if (label_df.loc[s.index, filled_col] > 0).any() else 0.0),
        avg_markout_5s_filled=(_label_markout_col(5, prefix), lambda s: float(s[label_df.loc[s.index, filled_col] > 0].mean()) if (label_df.loc[s.index, filled_col] > 0).any() else 0.0),
        avg_markout_30s_filled=(_label_markout_col(30, prefix), lambda s: float(s[label_df.loc[s.index, filled_col] > 0].mean()) if (label_df.loc[s.index, filled_col] > 0).any() else 0.0),
    ).reset_index()
    lines.append(_to_md(summary))
    lines.append("")
    lines.append("## Model Metrics")
    lines.append("")
    lines.append(_to_md(pd.DataFrame(metrics)))
    lines.append("")
    path.write_text("\n".join(lines))


def add_quote_ev_training_args(ap: argparse.ArgumentParser) -> argparse.ArgumentParser:
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL)
    ap.add_argument("--trace-tag", default="20260521_baseline")
    ap.add_argument("--orders", type=Path, default=None)
    ap.add_argument("--fills", type=Path, default=None)
    ap.add_argument("--train-days", nargs="+", required=True, help="UTC training days, e.g. 2026-05-14 2026-05-15")
    ap.add_argument("--valid-days", nargs="+", required=True, help="UTC validation days, e.g. 2026-05-16")
    ap.add_argument("--model-dir", type=Path, default=None)
    ap.add_argument("--out-tag", default="20260521")
    ap.add_argument(
        "--feature-mode",
        choices=[
            "base",
            "xmarket",
            "shock",
            "cpp-context",
            "xmarket_cpp",
            "xmarket_interaction",
            "xmarket_interaction_cpp",
            "local_flow",
            "local_flow_xmarket",
            "local_flow_xmarket_interaction",
            "local_flow_xmarket_interaction_cpp",
            "calendar",
            "calendar_local_flow",
            "calendar_local_flow_xmarket",
            "calendar_local_flow_xmarket_interaction",
            "calendar_local_flow_xmarket_interaction_cpp",
            "local_resiliency",
            "local_resiliency_xmarket_interaction",
            "calendar_local_resiliency",
            "calendar_local_resiliency_xmarket_interaction",
            "calendar_local_resiliency_xmarket_interaction_cpp",
            "toxic_risk",
            "toxic_risk_xmarket",
            "local_flow_toxic_risk",
            "local_flow_xmarket_toxic_risk",
            "local_flow_xmarket_interaction_toxic_risk",
            "calendar_local_flow_toxic_risk",
            "calendar_local_flow_xmarket_toxic_risk",
            "calendar_local_flow_xmarket_interaction_toxic_risk",
            "local_resiliency_toxic_risk",
            "local_resiliency_xmarket_interaction_toxic_risk",
            "calendar_local_resiliency_toxic_risk",
            "calendar_local_resiliency_xmarket_interaction_toxic_risk",
            "calendar_local_resiliency_xmarket_interaction_cpp_toxic_risk",
            "micro_macro",
            "calendar_micro_macro",
            "calendar_local_flow_xmarket_interaction_toxic_risk_micro_macro",
            "calendar_local_resiliency_xmarket_interaction_toxic_risk_micro_macro",
            "calendar_local_resiliency_xmarket_interaction_cpp_toxic_risk_micro_macro",
        ],
        default="base",
        help=(
            "base keeps current quote features; xmarket adds live-computable cross-market features; "
            "xmarket_interaction adds quote-time ref/depth/distance/rank interactions; "
            "cpp-context adds a small C++ quote-context set; xmarket_cpp combines xmarket+cpp; "
            "xmarket_interaction_cpp combines xmarket+interaction+cpp; local_flow adds quote-time "
            "本市场 taker-flow/replenishment interaction features; calendar* adds unified UTC/CN/US "
            "session and holiday flags; local_resiliency* adds the Stage-S depth/refill/queue/flow-decay "
            "research score; *toxic_risk adds Stage-T continuous SELL toxic-risk labels for quote EV/"
            "shadow calibration; *micro_macro adds quote-time short-window reach and 1m/5m "
            "trend-inventory risk features; shock is offline-only."
        ),
    )
    ap.add_argument(
        "--side",
        choices=["BUY", "SELL", "bid", "ask"],
        default="BUY",
        help="Quote side to train. BUY/bid writes bid_* models; SELL/ask writes ask_* models.",
    )
    ap.add_argument(
        "--max-inventory",
        type=float,
        default=None,
        help="Inventory normalizer; default loads strategy.max_inventory from live/config.yaml.",
    )
    ap.add_argument(
        "--max-label-gap-s",
        type=float,
        default=15.0,
        help="Max allowed gap on the 10s feature grid when validating filled markout horizons.",
    )
    ap.add_argument(
        "--quote-context",
        choices=["existing", "cpp-batch", "off"],
        default="existing",
        help="Optionally refresh order quote context with the C++ depth-aware batch core before label generation.",
    )
    ap.add_argument("--quote-context-cache-dir", default=None)
    ap.add_argument("--refresh-quote-context-cache", action="store_true")
    ap.add_argument("--strict-cpp-quote-context", action="store_true")
    ap.add_argument(
        "--min-valid-filled-rows",
        type=int,
        default=100,
        help=(
            "Promotion gate only: minimum validation filled quote rows required for "
            "filled-markout heads. Artifacts are still written unless "
            "--strict-promotion-gates is set."
        ),
    )
    ap.add_argument(
        "--max-fill-calibration-mae",
        type=float,
        default=0.05,
        help="Promotion gate only: max mean absolute fill-prob calibration error on validation bins.",
    )
    ap.add_argument(
        "--strict-promotion-gates",
        action="store_true",
        help="Exit non-zero after writing artifacts if sample/calibration promotion gates fail.",
    )
    ap.add_argument(
        "--skip-quality-filter",
        action="store_true",
        help=(
            "Skip the secondary data_quality timestamp filter. Use only for trace parquet files "
            "already produced by a quality-audited cross_market_shock_audit run."
        ),
    )
    return ap


def parse_quote_ev_training_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Train quote-level EV/toxicity models from tick traces")
    add_quote_ev_training_args(ap)
    return ap.parse_args(argv)


def run_quote_ev_training(args: argparse.Namespace) -> dict[str, Any]:
    prefix = _default_trace_prefix(args.symbol, args.trace_tag)
    orders_path = args.orders or prefix.with_suffix(".orders.csv")
    fills_path = args.fills or prefix.with_suffix(".fills.csv")
    paths = paths_for(args.symbol)
    model_dir = args.model_dir or paths.model_dir
    side_prefix = quote_side_prefix(args.side)
    side_upper = "BUY" if side_prefix == "bid" else "SELL"
    model_names = quote_side_model_names(side_prefix)
    out_prefix = paths.results_dir / f"{side_prefix}_quote_ev_labels_{args.out_tag}_{args.symbol.lower()}"

    t0 = time.perf_counter()
    if args.max_inventory is None:
        try:
            max_inventory = float(load_live_config_as_params().get("max_inventory", 0.0))
        except Exception:
            max_inventory = None
    else:
        max_inventory = args.max_inventory

    orders = _read_csv(orders_path)
    fills = _read_csv(fills_path)
    if not bool(getattr(args, "skip_quality_filter", False)):
        orders = _filter_trace_for_orderbook_quality(orders, args.symbol, "quote EV orders")
        fills = _filter_trace_for_orderbook_quality(fills, args.symbol, "quote EV fills")
    train_periods = args.train_days
    valid_periods = args.valid_days
    quote_context = getattr(args, "quote_context", "existing")
    if quote_context == "cpp-batch":
        days = sorted(set(train_periods or []) | set(valid_periods or []))
        orders = enrich_orders_with_cpp_quote_context(
            orders,
            symbol=args.symbol,
            days=days,
            cache_dir=getattr(args, "quote_context_cache_dir", None),
            refresh_cache=bool(getattr(args, "refresh_quote_context_cache", False)),
            strict=bool(getattr(args, "strict_cpp_quote_context", False)),
        )
    labels = build_labels(orders, fills, max_inventory=max_inventory, side=side_upper)
    if not bool(getattr(args, "skip_quality_filter", False)):
        labels = _filter_trace_for_orderbook_quality(labels, args.symbol, f"{side_prefix} quote EV labels")
    labels[HORIZON_VALID_COL] = _label_horizon_valid(labels, args.max_label_gap_s)
    feature_mode = str(args.feature_mode or "").lower()
    if feature_mode in {
        "xmarket_interaction",
        "xmarket_interaction_cpp",
        "local_flow_xmarket_interaction",
        "local_flow_xmarket_interaction_cpp",
        "calendar_local_flow_xmarket_interaction",
        "calendar_local_flow_xmarket_interaction_cpp",
        "local_resiliency_xmarket_interaction",
        "calendar_local_resiliency_xmarket_interaction",
        "calendar_local_resiliency_xmarket_interaction_cpp",
    } or "xmarket_interaction" in feature_mode:
        labels = add_quote_time_interaction_features(labels)
    if feature_mode in {
        "calendar",
        "calendar_local_flow",
        "calendar_local_flow_xmarket",
        "calendar_local_flow_xmarket_interaction",
        "calendar_local_flow_xmarket_interaction_cpp",
        "calendar_local_resiliency",
        "calendar_local_resiliency_xmarket_interaction",
        "calendar_local_resiliency_xmarket_interaction_cpp",
    } or feature_mode.startswith("calendar_"):
        labels = add_calendar_quote_features(labels)
    if feature_mode in {
        "local_flow",
        "local_flow_xmarket",
        "local_flow_xmarket_interaction",
        "local_flow_xmarket_interaction_cpp",
        "calendar_local_flow",
        "calendar_local_flow_xmarket",
        "calendar_local_flow_xmarket_interaction",
        "calendar_local_flow_xmarket_interaction_cpp",
        "local_resiliency",
        "local_resiliency_xmarket_interaction",
        "calendar_local_resiliency",
        "calendar_local_resiliency_xmarket_interaction",
        "calendar_local_resiliency_xmarket_interaction_cpp",
    } or "local_flow" in feature_mode or "local_resiliency" in feature_mode:
        labels = add_local_flow_quote_features(labels)
    if "local_resiliency" in feature_mode:
        labels = add_local_resiliency_quote_features(labels)
    if "toxic_risk" in feature_mode:
        labels = add_toxic_risk_quote_features(labels)
    if "micro_macro" in feature_mode:
        labels = add_micro_macro_quote_features(labels)
    feature_cols = _select_feature_cols(labels, args.feature_mode)
    if not feature_cols:
        raise SystemExit(f"No usable {side_prefix} quote EV features found")

    train_df = _day_filter(labels, train_periods)
    valid_df = _day_filter(labels, valid_periods)
    if train_df.empty or valid_df.empty:
        raise SystemExit(f"Empty split: train={len(train_df)} valid={len(valid_df)}")

    labels.to_parquet(out_prefix.with_suffix(".parquet"), index=False)
    labels.to_csv(out_prefix.with_suffix(".csv"), index=False)

    filled_col = f"label_{side_prefix}_filled"
    train_filled_df = train_df.loc[(train_df[filled_col] > 0) & train_df[HORIZON_VALID_COL].astype(bool)].copy()
    valid_filled_df = valid_df.loc[(valid_df[filled_col] > 0) & valid_df[HORIZON_VALID_COL].astype(bool)].copy()
    train_filled_by_day = (
        train_filled_df.groupby("day").size().astype(int).to_dict()
        if "day" in train_filled_df else {}
    )
    valid_filled_by_day = (
        valid_filled_df.groupby("day").size().astype(int).to_dict()
        if "day" in valid_filled_df else {}
    )

    metrics = [
        _train_lgbm(
            model_names["fill_prob"],
            train_df,
            valid_df,
            feature_cols,
            filled_col,
            True,
            model_dir,
            min_child_samples=200,
        )
    ]
    for horizon in MARKOUT_HORIZONS:
        model_name = model_names["markout_buckets"][horizon]
        bucket_col = _label_bucket_col(horizon, side_prefix)
        markout_col = _label_markout_col(horizon, side_prefix)
        metrics.append(
            _train_lgbm(
                model_name,
                train_filled_df,
                valid_filled_df,
                feature_cols,
                bucket_col,
                True,
                model_dir,
                min_child_samples=10,
                model_kind="multiclass",
                extra_meta={
                    "horizon_s": horizon,
                    "horizon_valid_col": HORIZON_VALID_COL,
                    "bucket_edges": DEFAULT_MARKOUT_BUCKET_EDGES,
                    "bucket_values": _empirical_bucket_values(
                        train_filled_df,
                        bucket_col,
                        markout_col,
                    ),
                    "realized_markout_col": markout_col,
                    "bucket_value_source": "train_empirical_mean_with_default_fallback",
                },
            )
        )
    metrics.append(
        _train_lgbm(
            model_names["extreme_adverse"],
            train_filled_df,
            valid_filled_df,
            feature_cols,
            f"label_{side_prefix}_extreme_adverse_any",
            True,
            model_dir,
            min_child_samples=10,
            extra_meta={
                "threshold": EXTREME_ADVERSE_THRESHOLD,
                "definition": f"any filled {side_prefix} markout_1s/5s/30s <= threshold",
                "horizon_valid_col": HORIZON_VALID_COL,
            },
        )
    )
    for metric in metrics:
        metric["runtime_s_total"] = round(time.perf_counter() - t0, 3)

    fill_prob_metric = metrics[0] if metrics else {}
    calibration_mae = fill_prob_metric.get("valid_calibration_mae")
    promotion_blockers: list[str] = []
    if len(valid_filled_df) < int(args.min_valid_filled_rows):
        promotion_blockers.append(
            f"valid_filled_rows={len(valid_filled_df)} < min_valid_filled_rows={args.min_valid_filled_rows}"
        )
    if calibration_mae is None:
        promotion_blockers.append("fill_prob valid_calibration_mae is unavailable")
    elif float(calibration_mae) > float(args.max_fill_calibration_mae):
        promotion_blockers.append(
            f"fill_prob valid_calibration_mae={float(calibration_mae):.6f} "
            f"> max_fill_calibration_mae={args.max_fill_calibration_mae:.6f}"
        )
    sample_quality = {
        "promotion_ready": not promotion_blockers,
        "promotion_blockers": promotion_blockers,
        "min_valid_filled_rows": int(args.min_valid_filled_rows),
        "max_fill_calibration_mae": float(args.max_fill_calibration_mae),
        "train_rows": int(len(train_df)),
        "valid_rows": int(len(valid_df)),
        "train_filled_rows_horizon_valid": int(len(train_filled_df)),
        "valid_filled_rows_horizon_valid": int(len(valid_filled_df)),
        "train_periods": list(train_periods or []),
        "valid_periods": list(valid_periods or []),
        "period_grain": "day",
        "train_filled_rows_by_day": {str(k): int(v) for k, v in train_filled_by_day.items()},
        "valid_filled_rows_by_day": {str(k): int(v) for k, v in valid_filled_by_day.items()},
        "fill_prob_valid_calibration_mae": None if calibration_mae is None else float(calibration_mae),
        "fill_prob_valid_brier": fill_prob_metric.get("valid_brier"),
        "fill_prob_valid_auc": fill_prob_metric.get("valid_auc"),
        "fill_prob_valid_logloss": fill_prob_metric.get("valid_logloss"),
    }

    with open(out_prefix.with_suffix(".summary.json"), "w") as f:
        json.dump({
            "feature_mode": args.feature_mode,
            "features": feature_cols,
            "max_inventory": max_inventory,
            "max_label_gap_s": args.max_label_gap_s,
            "horizon_valid_col": HORIZON_VALID_COL,
            "filled_rows": int((labels[filled_col] > 0).sum()),
            "filled_rows_horizon_valid": int(((labels[filled_col] > 0) & labels[HORIZON_VALID_COL].astype(bool)).sum()),
            "sample_quality": sample_quality,
            "metrics": metrics,
        }, f, indent=2)
    try:
        _write_summary(out_prefix.with_suffix(".md"), metrics, labels, side_prefix)
    except Exception:
        pd.DataFrame(metrics).to_csv(out_prefix.with_suffix(".metrics.csv"), index=False)

    print(
        f"Saved {side_prefix} quote EV models to {model_dir} "
        f"({len(labels):,} labelled {side_prefix} quotes, {len(feature_cols)} features)"
    )
    if promotion_blockers:
        print(
            f"[WARN] {side_prefix} quote EV promotion gates failed: "
            + "; ".join(promotion_blockers)
        )
        if bool(getattr(args, "strict_promotion_gates", False)):
            raise SystemExit(2)
    return {
        "symbol": args.symbol.upper(),
        "side": side_prefix,
        "orders_path": str(orders_path),
        "fills_path": str(fills_path),
        "model_dir": str(model_dir),
        "labels_path": str(out_prefix.with_suffix(".parquet")),
        "label_rows": int(len(labels)),
        "n_features": int(len(feature_cols)),
        "feature_mode": args.feature_mode,
        "sample_quality": sample_quality,
        "metrics": metrics,
    }


def main(argv: list[str] | None = None) -> None:
    run_quote_ev_training(parse_quote_ev_training_args(argv))


if __name__ == "__main__":
    main()
