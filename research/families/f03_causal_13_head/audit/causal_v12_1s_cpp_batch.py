#!/usr/bin/env python3
"""Packed Python bridge for the F03 native daily 173-feature batch kernel."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_full_schema as full
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

CPP_BATCH_ABI_VERSION = "causal_v12_1s_cpp_daily_batch.v1"


def _pack_bars(
    bars: Sequence[base.OneSecondBar],
) -> tuple[np.ndarray[Any, np.dtype[np.int64]], np.ndarray[Any, np.dtype[np.float64]]]:
    integers = np.empty((len(bars), 6), dtype=np.int64)
    floating = np.empty((len(bars), 13), dtype=np.float64)
    for index, bar in enumerate(bars):
        integers[index] = (
            int(bar.start_ts_ms),
            int(bar.finalized_ts_ms),
            int(bar.trade_count),
            int(bar.buy_count),
            int(bar.sell_count),
            int(bar.max_same_side_run),
        )
        floating[index] = (
            float(bar.open),
            float(bar.high),
            float(bar.low),
            float(bar.close),
            float(bar.volume),
            float(bar.buy_volume),
            float(bar.sell_volume),
            float(bar.buy_quote_qty),
            float(bar.sell_quote_qty),
            float(bar.buy_price_high),
            float(bar.buy_price_low),
            float(bar.sell_price_high),
            float(bar.sell_price_low),
        )
    return integers, floating


def _pack_l2(
    observations: Sequence[full.ExecutionL2Observation],
) -> tuple[np.ndarray[Any, np.dtype[np.int64]], np.ndarray[Any, np.dtype[np.float64]]]:
    clocks = np.empty((len(observations), 2), dtype=np.int64)
    values = np.empty((len(observations), len(schema.EXECUTION_L2_FEATURES)), dtype=np.float64)
    for index, item in enumerate(observations):
        clocks[index] = (int(item.bucket_start_ts_ms), int(item.feature_ready_ts_ms))
        values[index] = tuple(float(item.values[name]) for name in schema.EXECUTION_L2_FEATURES)
    return clocks, values


def _pack_metrics(
    observations: Sequence[full.MetricObservation],
) -> tuple[np.ndarray[Any, np.dtype[np.int64]], np.ndarray[Any, np.dtype[np.float64]]]:
    clocks = np.empty((len(observations), 2), dtype=np.int64)
    values = np.empty((len(observations), 4), dtype=np.float64)
    for index, item in enumerate(observations):
        clocks[index] = (int(item.source_ts_ms), int(item.feature_ready_ts_ms))
        values[index] = (
            float(item.sum_open_interest),
            float(item.toptrader_ls_ratio),
            float(item.crowd_ls_ratio),
            float(item.taker_ls_ratio),
        )
    return clocks, values


def validate_cpp_batch_module(cpp: Any) -> None:
    if getattr(cpp, "F03_CAUSAL_V12_1S_BATCH_ABI_VERSION", None) != CPP_BATCH_ABI_VERSION:
        raise base.FeatureContractError("F03 C++ daily batch ABI mismatch")
    if tuple(cpp.F03_CAUSAL_V12_1S_FEATURE_NAMES) != schema.TRAINABLE_FEATURE_ORDER:
        raise base.FeatureContractError("F03 C++ daily batch feature order mismatch")
    if cpp.F03_CAUSAL_V12_1S_FEATURE_ORDER_SHA256 != schema.feature_order_sha256():
        raise base.FeatureContractError("F03 C++ daily batch feature-order SHA256 mismatch")
    if len(tuple(cpp.F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY)) != 7:
        raise base.FeatureContractError("F03 C++ daily batch lag-state vocabulary mismatch")


def create_engine(
    cpp: Any,
    *,
    local_bars: Sequence[base.OneSecondBar],
    execution_l2: Sequence[full.ExecutionL2Observation],
    metrics: Sequence[full.MetricObservation],
    reference_bars: Sequence[base.OneSecondBar],
) -> Any:
    """Pack raw observations once; no Python feature value enters the native kernel."""

    validate_cpp_batch_module(cpp)
    local_integers, local_floating = _pack_bars(local_bars)
    reference_integers, reference_floating = _pack_bars(reference_bars)
    l2_clocks, l2_values = _pack_l2(execution_l2)
    metric_clocks, metric_values = _pack_metrics(metrics)
    return cpp.F03CausalV12OneSecondBatchEngine(
        local_integers,
        local_floating,
        l2_clocks,
        l2_values,
        metric_clocks,
        metric_values,
        reference_integers,
        reference_floating,
    )


def compute_batch(
    engine: Any,
    cutoffs_ms: Sequence[int],
    *,
    decision_ts_ms: Sequence[int] | None = None,
) -> Mapping[str, Any]:
    cutoffs = np.asarray(cutoffs_ms, dtype=np.int64)
    decisions = None if decision_ts_ms is None else np.asarray(decision_ts_ms, dtype=np.int64)
    output = engine.compute(cutoffs, decisions)
    expected_shape = (len(cutoffs), len(schema.TRAINABLE_FEATURE_ORDER))
    for name in (
        "values",
        "valid",
        "source_latest_ts_ms",
        "feature_ready_ts_ms_by_feature",
        "observation_count",
        "lag_state_code",
    ):
        if np.asarray(output[name]).shape != expected_shape:
            raise base.FeatureContractError(f"F03 C++ batch output shape mismatch: {name}")
    if output["schema_version"] != CPP_BATCH_ABI_VERSION:
        raise base.FeatureContractError("F03 C++ batch output ABI mismatch")
    return output


def feature_row_fingerprint(
    *,
    cutoff_exclusive_ms: int,
    values: np.ndarray[Any, np.dtype[np.float64]],
    valid: np.ndarray[Any, np.dtype[np.uint8]],
    source_latest_ts_ms: np.ndarray[Any, np.dtype[np.int64]],
    feature_ready_ts_ms: np.ndarray[Any, np.dtype[np.int64]],
    observation_count: np.ndarray[Any, np.dtype[np.int64]],
    lag_state_code: np.ndarray[Any, np.dtype[np.uint8]],
    lag_state_vocabulary: Sequence[str],
) -> str:
    """Reproduce the frozen Python fingerprint without recomputing feature values."""

    payload = {
        "cutoff_exclusive_ms": int(cutoff_exclusive_ms),
        "feature_contract_sha256": full.full_feature_contract_fingerprint(),
        "feature_order_sha256": schema.feature_order_sha256(),
        "source_manifest_sha256": schema.canonical_sha256(schema.source_manifest_payload()),
        "values": [
            {
                "name": name,
                "value_hex": float(values[index]).hex() if bool(valid[index]) else None,
                "source_latest_ts_ms": (
                    None if int(source_latest_ts_ms[index]) < 0 else int(source_latest_ts_ms[index])
                ),
                "feature_ready_ts_ms": (
                    None if int(feature_ready_ts_ms[index]) < 0 else int(feature_ready_ts_ms[index])
                ),
                "observation_count": int(observation_count[index]),
                "lag_state": lag_state_vocabulary[int(lag_state_code[index])],
            }
            for index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER)
        ],
    }
    return schema.canonical_sha256(payload)


def engine_identity(cpp: Any) -> dict[str, Any]:
    validate_cpp_batch_module(cpp)
    return {
        "engine": "cpp_batch",
        "engine_abi": str(cpp.F03_CAUSAL_V12_1S_BATCH_ABI_VERSION),
        "feature_order_sha256": str(cpp.F03_CAUSAL_V12_1S_FEATURE_ORDER_SHA256),
        "lag_state_vocabulary": list(cpp.F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY),
        "raw_inputs_only": True,
        "python_precomputed_feature_values_accepted": False,
        "row_fingerprint": "engine_native_canonical_feature_row.v1",
        "python_bitwise_row_fingerprint_claimed": False,
    }
