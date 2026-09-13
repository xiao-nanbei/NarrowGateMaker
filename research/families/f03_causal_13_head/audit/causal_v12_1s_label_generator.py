#!/usr/bin/env python3
"""Strict 1s label and overlap-weight generator for the causal-v12 successor.

The feature panel is already frozen at ``decision_ts_ms``.  This module keeps
that timestamp as the label origin; it must never inherit the legacy 10-second
``RESAMPLE_SEC`` offset from ``features.feature_engineer.add_labels``.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_quality import continuous_segment_ids
from features import feature_engineer as legacy_labels
from research.families.f03_causal_13_head.audit.causal_v12_1s_schema import (
    TRAINABLE_FEATURE_ORDER,
)
from research.families.f03_causal_13_head.audit.causal_v12_1s_training_contract import (
    HEAD_MAXIMUM_FUTURE_DEPENDENCY_S,
    NS_PER_SECOND,
    TrainingContractError,
    overlap_adjusted_sample_weights,
)

SCHEMA_VERSION = "causal_v12_1s_label_generator.v1"
IDENTITY = "causal_v12_cadence_1s_source_aware_semantics_successor_v1"
DECISION_CADENCE_MS = 1_000
SECONDS_PER_DAY = 86_400
BASE_WEIGHT_LAMBDA = 0.1
BASE_WEIGHT_REFERENCE_DATE = "2026-07-23"
LABEL_SOURCE_MAX_GAP_S = 1.5

LABEL_COLUMN_BY_HEAD = {
    "dir_10s": "label_dir_10s",
    "ret_10s": "label_ret_10s",
    "vol_10s": "label_vol_10s",
    "dir_30s": "label_dir_30s",
    "ret_30s": "label_ret_30s",
    "vol_30s": "label_vol_30s",
    "dir_60s": "label_dir_60s",
    "ret_60s": "label_ret_60s",
    "vol_60s": "label_vol_60s",
    "tox_bid_5s": "label_tox_bid_5s",
    "tox_ask_5s": "label_tox_ask_5s",
    "tox_bid_10s": "label_tox_bid_10s",
    "tox_ask_10s": "label_tox_ask_10s",
}


class LabelGenerationError(ValueError):
    """Raised when a 1s label artifact violates its frozen time semantics."""


def _utc_day_bounds_ms(target_utc_day: str) -> tuple[int, int]:
    try:
        start = pd.Timestamp(target_utc_day, tz="UTC")
    except (TypeError, ValueError) as exc:
        raise LabelGenerationError(f"invalid target UTC day: {target_utc_day}") from exc
    if start.strftime("%Y-%m-%d") != target_utc_day:
        raise LabelGenerationError(f"target UTC day is not canonical: {target_utc_day}")
    start_ms = int(start.timestamp() * 1_000)
    return start_ms, start_ms + SECONDS_PER_DAY * DECISION_CADENCE_MS


def _validate_feature_panel(
    feature_panel: pd.DataFrame,
    *,
    target_utc_day: str,
) -> tuple[np.ndarray, pd.DatetimeIndex]:
    required = {
        "cutoff_exclusive_ms",
        "decision_ts_ms",
        "feature_ready_ts_ms",
        "feature_row_fingerprint_sha256",
        "close",
    }
    missing = sorted(required - set(feature_panel.columns))
    if missing:
        raise LabelGenerationError(f"feature panel is missing columns: {missing}")
    missing_features = sorted(set(TRAINABLE_FEATURE_ORDER) - set(feature_panel.columns))
    if missing_features:
        raise LabelGenerationError(
            f"feature panel is missing trainable fields: {missing_features[:5]}"
        )
    leaked = sorted(
        column
        for column in feature_panel.columns
        if column.startswith("label_")
        or column.startswith("sample_weight__")
        or column.startswith("overlap_uniqueness__")
    )
    if leaked:
        raise LabelGenerationError(
            f"input feature panel already contains label/weight columns: {leaked[:5]}"
        )

    decision_ms = feature_panel["decision_ts_ms"].to_numpy(dtype=np.int64)
    cutoff_ms = feature_panel["cutoff_exclusive_ms"].to_numpy(dtype=np.int64)
    ready_ms = feature_panel["feature_ready_ts_ms"].to_numpy(dtype=np.int64)
    start_ms, end_ms = _utc_day_bounds_ms(target_utc_day)
    expected = np.arange(start_ms, end_ms, DECISION_CADENCE_MS, dtype=np.int64)
    if not np.array_equal(decision_ms, expected):
        raise LabelGenerationError(
            "training labels require exactly one canonical decision for every "
            "second of the target UTC day"
        )
    if not np.array_equal(cutoff_ms, decision_ms):
        raise LabelGenerationError("decision_ts_ms must equal cutoff_exclusive_ms")
    if np.any(ready_ms > decision_ms):
        raise LabelGenerationError("feature_ready_ts_ms exceeds the decision clock")
    if np.any(~np.isfinite(feature_panel["close"].to_numpy(dtype=np.float64))):
        raise LabelGenerationError("close must be finite at every 1s decision")
    fingerprints = feature_panel["feature_row_fingerprint_sha256"].astype(str)
    if not bool(fingerprints.str.fullmatch(r"[0-9a-f]{64}").all()):
        raise LabelGenerationError(
            "feature_row_fingerprint_sha256 must contain lowercase SHA256 values"
        )

    decision_index = pd.to_datetime(decision_ms, unit="ms", utc=True)
    return decision_ms * 1_000_000, decision_index


def _validate_label_bars(bars_1s: pd.DataFrame) -> tuple[pd.DatetimeIndex, np.ndarray]:
    missing = sorted({"close", "high", "low"} - set(bars_1s.columns))
    if missing:
        raise LabelGenerationError(f"label bars are missing columns: {missing}")
    index = pd.to_datetime(bars_1s.index, utc=True, errors="coerce")
    if index.isna().any():
        raise LabelGenerationError("label bars require finite UTC timestamps")
    timestamp_ns = index.as_unit("ns").asi8
    if timestamp_ns.size == 0:
        raise LabelGenerationError("label bars are empty")
    if np.any(timestamp_ns % NS_PER_SECOND != 0) or np.any(np.diff(timestamp_ns) <= 0):
        raise LabelGenerationError(
            "label bars must be unique, increasing, canonical left-labelled 1s bars"
        )
    values = bars_1s[["close", "high", "low"]].to_numpy(dtype=np.float64)
    if np.any(~np.isfinite(values)):
        raise LabelGenerationError("label OHLC values must be finite")
    if np.any(values[:, 1] < values[:, 2]):
        raise LabelGenerationError("label bar high is below low")
    return index, timestamp_ns


def _bar_future_support_masks(
    decision_ts_ns: np.ndarray,
    bars_index: pd.DatetimeIndex,
    bars_ts_ns: np.ndarray,
) -> dict[int, np.ndarray]:
    """Return exact, same-segment future support for each dependency horizon."""

    segments = continuous_segment_ids(
        bars_index,
        max_gap_s=LABEL_SOURCE_MAX_GAP_S,
    )
    start_idx = np.searchsorted(bars_ts_ns, decision_ts_ns, side="left")
    exact_start = start_idx < bars_ts_ns.size
    exact_rows = np.flatnonzero(exact_start)
    exact_start[exact_rows] &= bars_ts_ns[start_idx[exact_rows]] == decision_ts_ns[exact_rows]

    masks: dict[int, np.ndarray] = {}
    for dependency_s in sorted(set(HEAD_MAXIMUM_FUTURE_DEPENDENCY_S.values())):
        target_ns = decision_ts_ns + dependency_s * NS_PER_SECOND
        future_idx = np.searchsorted(bars_ts_ns, target_ns, side="left")
        valid = exact_start & (future_idx < bars_ts_ns.size)
        rows = np.flatnonzero(valid)
        if rows.size:
            start_rows = start_idx[rows]
            future_rows = future_idx[rows]
            valid[rows] &= (
                (segments[start_rows] == segments[future_rows])
                & (
                    bars_ts_ns[future_rows] - target_ns[rows]
                    <= int(LABEL_SOURCE_MAX_GAP_S * NS_PER_SECOND)
                )
            )
        # Training ownership is a UTC-day unit.  Labels may not cross midnight,
        # even when D+1 market data happens to be available.
        position = np.arange(decision_ts_ns.size, dtype=np.int64)
        valid &= position + dependency_s < decision_ts_ns.size
        masks[dependency_s] = valid
    return masks


def inherited_base_sample_weights(decision_index: pd.DatetimeIndex) -> np.ndarray:
    """Reproduce the causal-v12 base time-decay weight without changing scale."""

    decision_index = pd.DatetimeIndex(pd.to_datetime(decision_index, utc=True))
    reference = pd.Timestamp(BASE_WEIGHT_REFERENCE_DATE, tz="UTC")
    days_ago = np.maximum(
        (reference - decision_index).total_seconds().to_numpy(dtype=np.float64)
        / SECONDS_PER_DAY,
        0.0,
    )
    return np.exp(-BASE_WEIGHT_LAMBDA * days_ago / 30.44)


def _quote_parameters(
    *,
    symbol: str,
    config_path: Path | None,
    quote_params: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if quote_params is None:
        return legacy_labels._load_label_quote_params(symbol, config_path=config_path)
    return dict(quote_params)


def generate_daily_1s_labels(
    feature_panel: pd.DataFrame,
    bars_1s: pd.DataFrame,
    *,
    target_utc_day: str,
    symbol: str = legacy_labels.DEFAULT_SYMBOL,
    config_path: Path | None = None,
    quote_params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Generate all 13 labels and head-specific overlap-adjusted weights.

    ``bars_1s`` may contain warmup or D+1 observations, but the frozen training
    unit remains one complete UTC day.  Cross-midnight labels are censored.
    """

    decision_ts_ns, decision_index = _validate_feature_panel(
        feature_panel,
        target_utc_day=target_utc_day,
    )
    bars_index, bars_ts_ns = _validate_label_bars(bars_1s)
    support_masks = _bar_future_support_masks(
        decision_ts_ns,
        bars_index,
        bars_ts_ns,
    )

    context = legacy_labels._prepare_1s_label_context(bars_1s)
    ts_1s_ns, close_1s, high_1s, low_1s, diff_1s, sigma_sq_1s = context
    start_idx = np.searchsorted(ts_1s_ns, decision_ts_ns, side="left")
    last_hist_idx = np.clip(start_idx - 1, 0, len(sigma_sq_1s) - 1)
    sigma_sq_now = sigma_sq_1s[last_hist_idx]
    close_ref = feature_panel["close"].to_numpy(dtype=np.float64)
    params = _quote_parameters(
        symbol=symbol,
        config_path=config_path,
        quote_params=quote_params,
    )
    half_spread = legacy_labels._quote_half_spread(
        feature_panel,
        close_ref,
        sigma_sq_now,
        params,
    )
    bid_quote = close_ref - half_spread
    ask_quote = close_ref + half_spread

    output = feature_panel[
        [
            "cutoff_exclusive_ms",
            "decision_ts_ms",
            "feature_ready_ts_ms",
            "feature_row_fingerprint_sha256",
        ]
    ].copy(deep=True)
    computed: dict[str, np.ndarray] = {}
    for horizon_s in legacy_labels.LABEL_HORIZONS:
        ret_label, dir_label, vol_label = legacy_labels._compute_label_triplet(
            ts_1s_ns,
            close_1s,
            high_1s,
            low_1s,
            diff_1s,
            decision_ts_ns,
            start_idx,
            bid_quote,
            ask_quote,
            close_ref,
            horizon_s * NS_PER_SECOND,
        )
        computed[f"ret_{horizon_s}s"] = ret_label
        computed[f"dir_{horizon_s}s"] = dir_label
        computed[f"vol_{horizon_s}s"] = vol_label

    for horizon_s in legacy_labels.TOXICITY_HORIZONS:
        tox_bid, tox_ask = legacy_labels._compute_toxicity_pair(
            ts_1s_ns,
            close_1s,
            high_1s,
            low_1s,
            decision_ts_ns,
            start_idx,
            bid_quote,
            ask_quote,
            horizon_s * NS_PER_SECOND,
        )
        computed[f"tox_bid_{horizon_s}s"] = tox_bid
        computed[f"tox_ask_{horizon_s}s"] = tox_ask

    base_weight = inherited_base_sample_weights(decision_index)
    for head, dependency_s in HEAD_MAXIMUM_FUTURE_DEPENDENCY_S.items():
        values = np.asarray(computed[head], dtype=np.float64).copy()
        values[~support_masks[dependency_s]] = np.nan
        valid = np.isfinite(values)
        weights, uniqueness = overlap_adjusted_sample_weights(
            decision_ts_ns,
            valid,
            maximum_future_dependency_s=dependency_s,
            base_weight=base_weight,
        )
        output[LABEL_COLUMN_BY_HEAD[head]] = values
        output[f"label_valid__{head}"] = valid
        output[f"sample_weight__{head}"] = weights
        output[f"overlap_uniqueness__{head}"] = uniqueness

    if set(LABEL_COLUMN_BY_HEAD.values()) & set(TRAINABLE_FEATURE_ORDER):
        raise TrainingContractError("label namespace leaked into the feature schema")
    output.attrs.update(
        {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "target_utc_day": target_utc_day,
            "decision_clock": "cutoff_exclusive_ms",
            "legacy_resample_offset_applied": False,
            "base_weight_lambda": BASE_WEIGHT_LAMBDA,
            "base_weight_reference_date": BASE_WEIGHT_REFERENCE_DATE,
            "head_count": len(HEAD_MAXIMUM_FUTURE_DEPENDENCY_S),
            "materialization_role": "label_overlay_no_feature_column_duplication",
        }
    )
    return output
