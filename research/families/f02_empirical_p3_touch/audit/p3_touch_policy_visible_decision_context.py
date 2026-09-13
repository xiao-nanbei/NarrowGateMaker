#!/usr/bin/env python3
"""Policy-visible decision contexts for conditional P3 v4.1.

The F06 placement denominator was generated from normalized 100ms BBO data
after applying a frozen AWS Tokyo visibility-age profile.  The earlier raw-BBO
transport audit omitted that latency layer.  This successor reconstructs the
same sampled visibility cutoff at every decision and one-second history point.

The profile is a historical latency sensitivity, not an AWS receive-time tape.
No fill, queue, markout, reward, PnL, or action outcome is read here.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.feature_dag import P3_TOUCH_CONDITIONAL_GRAPH
from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
    BASELINE_ACTIONS,
    DECISION_CONTEXT_FIELDS,
    OVERLAPPING_LABEL_CLUSTER_CONTRACT,
    PLACEMENT_DECISION_COLUMNS,
    DecisionCadenceContextBatch,
    DecisionCadenceContextError,
    FrozenCausalBboSource,
    _load_causal_bbo,
    _price_to_tick,
    _utc_days,
    _verify_sha256,
    load_f06_baseline_eligible_decisions,
)

IDENTITY = "p3_touch_policy_visible_decision_context.v1"
SCHEMA_VERSION = "narrowgate_p3_touch_policy_visible_decision_context.v1"
SOURCE_TIMESTAMP_SEMANTICS = "normalized_v2_source_time_plus_sampled_visibility_age"
VISIBILITY_SAMPLER_VERSION = "decision_keyed_splitmix64_v1"
SIDES = ("BUY", "SELL")

PERMISSIONS = {
    "transport_research_only": True,
    "training_authority": False,
    "prediction_authority": False,
    "quote_mapping_authority": False,
    "action_authority": False,
    "live_authority": False,
    "economic_outcomes_read": False,
    "aws_receive_time_transport_supported": False,
}


@dataclass(frozen=True)
class FrozenPolicyVisibleBboSource:
    """Hash-bound BBO and sampled visibility profile used by F06 replay."""

    path: Path
    sha256: str
    source_identity: str
    visibility_profile_path: Path
    visibility_profile_sha256: str
    visibility_profile_id: str
    visibility_seed: int
    timestamp_semantics: str = SOURCE_TIMESTAMP_SEMANTICS


@dataclass(frozen=True)
class VisibilityProfile:
    samples_ms: np.ndarray
    age_column: str
    timestamp_semantics: str


def _load_visibility_profile(
    source: FrozenPolicyVisibleBboSource,
) -> VisibilityProfile:
    path = source.visibility_profile_path.expanduser().resolve()
    _verify_sha256(
        path,
        source.visibility_profile_sha256,
        label="execution-book visibility profile",
    )
    if not str(source.visibility_profile_id).strip():
        raise DecisionCadenceContextError("visibility profile identity must be non-empty")
    frame = pd.read_csv(path)
    if "event" in frame:
        frame = frame.loc[frame["event"].astype(str).str.lower().eq("requote")].copy()
    if "status" in frame:
        frame = frame.loc[frame["status"].astype(str).str.lower().eq("ok")].copy()
    age_column = next(
        (name for name in ("depth_age_s", "exec_book_age_s") if name in frame),
        None,
    )
    if age_column is None:
        raise DecisionCadenceContextError("visibility profile lacks depth_age_s/exec_book_age_s")
    stage_lag_ms = (
        pd.to_numeric(frame["update_orders_us"], errors="coerce") / 1_000.0
        if "update_orders_us" in frame
        else pd.Series(0.0, index=frame.index, dtype=np.float64)
    )
    stage_lag_ms = stage_lag_ms.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
    samples = pd.to_numeric(frame[age_column], errors="coerce") * 1_000.0 - stage_lag_ms
    samples_ms = (
        samples.replace([np.inf, -np.inf], np.nan)
        .dropna()
        .clip(lower=0.0)
        .to_numpy(dtype=np.float64)
    )
    if samples_ms.size == 0:
        raise DecisionCadenceContextError("visibility profile contains no finite age samples")
    return VisibilityProfile(
        samples_ms=np.ascontiguousarray(samples_ms),
        age_column=str(age_column),
        timestamp_semantics=("pre_order_update" if "update_orders_us" in frame else "observed"),
    )


def _splitmix64_array(values: np.ndarray) -> np.ndarray:
    """Vectorized form of the replay's stable SplitMix64 sampler."""

    z = np.asarray(values, dtype=np.uint64).copy()
    with np.errstate(over="ignore"):
        z += np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        z ^= z >> np.uint64(31)
    return z


def visibility_delay_ms(
    query_ts_ms: np.ndarray,
    *,
    samples_ms: np.ndarray,
    seed: int,
) -> np.ndarray:
    """Return the exact decision-keyed sampled visibility age in milliseconds."""

    timestamps = np.asarray(query_ts_ms, dtype=np.int64)
    samples = np.asarray(samples_ms, dtype=np.float64).ravel()
    samples = samples[np.isfinite(samples) & (samples >= 0.0)]
    if samples.size == 0:
        raise DecisionCadenceContextError(
            "visibility sampler requires at least one finite nonnegative sample"
        )
    mixed = _splitmix64_array(timestamps.astype(np.uint64) ^ np.uint64(int(seed) & ((1 << 64) - 1)))
    selected = samples[(mixed % np.uint64(samples.size)).astype(np.int64)]
    return np.maximum(0, np.rint(selected).astype(np.int64))


def _validate_decisions(decisions: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    required = set(PLACEMENT_DECISION_COLUMNS).union({"decision_ts_ms"})
    missing = sorted(required.difference(decisions.columns))
    if missing:
        raise DecisionCadenceContextError(f"decision denominator lacks fields: {missing}")
    if decisions.empty:
        raise DecisionCadenceContextError("decision denominator is empty")
    identifiers = decisions["decision_id"].astype(str)
    if (
        identifiers.str.strip().eq("").any()
        or identifiers.str.lower().eq("nan").any()
        or identifiers.duplicated().any()
    ):
        raise DecisionCadenceContextError(
            "decision denominator requires unique non-empty decision_id values"
        )
    if not decisions["side"].astype(str).isin(SIDES).all():
        raise DecisionCadenceContextError("decision denominator has unsupported side")
    if not decisions["baseline_action"].astype(str).isin(BASELINE_ACTIONS).all():
        raise DecisionCadenceContextError(
            "decision denominator contains a non-posting baseline action"
        )
    if not pd.to_numeric(decisions["allow_post"], errors="coerce").eq(1).all():
        raise DecisionCadenceContextError("decision denominator contains a baseline-ineligible row")
    submit = pd.to_numeric(decisions["submit_ts_ns"], errors="coerce")
    ready = pd.to_numeric(decisions["feature_ready_ts_ns"], errors="coerce")
    if submit.isna().any() or ready.isna().any():
        raise DecisionCadenceContextError("decision clocks must be numeric")
    submit_values = submit.to_numpy(dtype=np.int64)
    ready_values = ready.to_numpy(dtype=np.int64)
    if (
        np.any(submit_values <= 0)
        or np.any(ready_values > submit_values)
        or np.any(submit_values % 1_000_000 != 0)
    ):
        raise DecisionCadenceContextError(
            "decision denominator violates the causal millisecond clock contract"
        )
    decision_ts = pd.to_numeric(decisions["decision_ts_ms"], errors="coerce").to_numpy(
        dtype=np.int64
    )
    if not np.array_equal(decision_ts, submit_values // 1_000_000):
        raise DecisionCadenceContextError("decision_ts_ms does not match submit_ts_ns")
    return decision_ts, ready_values


def extract_policy_visible_decision_context(
    decisions: pd.DataFrame,
    *,
    source: FrozenPolicyVisibleBboSource,
    tick_size: float = 0.1,
    max_bbo_age_ms: int = 5_000,
    fast_window_s: int = 10,
    slow_window_s: int = 60,
    variance_floor: float = 1e-6,
    chunk_rows: int = 25_000,
) -> DecisionCadenceContextBatch:
    """Rebuild v4.1 state on F06's sampled policy-visible BBO clock."""

    if source.timestamp_semantics != SOURCE_TIMESTAMP_SEMANTICS:
        raise DecisionCadenceContextError(
            "BBO source does not use the frozen sampled visibility semantics"
        )
    if not str(source.source_identity).strip():
        raise DecisionCadenceContextError("BBO source identity must be non-empty")
    if not math.isfinite(float(tick_size)) or float(tick_size) <= 0.0:
        raise DecisionCadenceContextError("tick_size must be positive")
    if int(fast_window_s) < 2 or int(slow_window_s) < int(fast_window_s):
        raise DecisionCadenceContextError("invalid fast/slow variance windows")
    if float(variance_floor) <= 0.0 or not math.isfinite(float(variance_floor)):
        raise DecisionCadenceContextError("variance_floor must be positive")
    if int(max_bbo_age_ms) < 0 or int(chunk_rows) <= 0:
        raise DecisionCadenceContextError("invalid BBO age or chunk size")

    decision_ts, _ = _validate_decisions(decisions)
    profile = _load_visibility_profile(source)
    bbo_ts, bids, asks = _load_causal_bbo(
        FrozenCausalBboSource(
            path=source.path,
            sha256=source.sha256,
            source_identity=source.source_identity,
        )
    )

    result = decisions.reset_index(drop=True).copy()
    row_count = len(result)
    reasons = np.full(row_count, "", dtype=object)
    context: dict[str, np.ndarray] = {
        field: np.full(row_count, np.nan, dtype=np.float64) for field in DECISION_CONTEXT_FIELDS
    }
    context["start_ts_ms"] = decision_ts.copy()
    context["feature_ready_ts_ms"] = np.full(row_count, -1, dtype=np.int64)

    declared_days = result["day"].astype(str).to_numpy()
    reasons[_utc_days(decision_ts) != declared_days] = "decision_timestamp_day_mismatch"

    current_delay = visibility_delay_ms(
        decision_ts,
        samples_ms=profile.samples_ms,
        seed=int(source.visibility_seed),
    )
    current_cutoff = decision_ts - current_delay
    current_idx = np.searchsorted(bbo_ts, current_cutoff, side="right") - 1
    current_safe = np.clip(current_idx, 0, len(bbo_ts) - 1)
    current_age = decision_ts - bbo_ts[current_safe]
    valid_current = (
        (current_idx >= 0)
        & np.isfinite(bids[current_safe])
        & np.isfinite(asks[current_safe])
        & (bids[current_safe] > 0.0)
        & (asks[current_safe] > bids[current_safe])
        & (current_age >= 0)
        & (current_age <= int(max_bbo_age_ms))
    )
    reasons[(reasons == "") & ~valid_current] = "policy_visible_bbo_unavailable_or_stale"

    placement_bid = result["best_bid"].to_numpy(dtype=np.float64)
    placement_ask = result["best_ask"].to_numpy(dtype=np.float64)
    source_bid_tick = np.asarray(
        [_price_to_tick(value, float(tick_size)) for value in bids[current_safe]],
        dtype=object,
    )
    source_ask_tick = np.asarray(
        [_price_to_tick(value, float(tick_size)) for value in asks[current_safe]],
        dtype=object,
    )
    placement_bid_tick = np.asarray(
        [_price_to_tick(value, float(tick_size)) for value in placement_bid],
        dtype=object,
    )
    placement_ask_tick = np.asarray(
        [_price_to_tick(value, float(tick_size)) for value in placement_ask],
        dtype=object,
    )
    valid_ticks = np.asarray(
        [
            source_bid is not None
            and source_ask is not None
            and placement_bid_value is not None
            and placement_ask_value is not None
            and int(source_bid) < int(source_ask)
            and int(placement_bid_value) < int(placement_ask_value)
            for source_bid, source_ask, placement_bid_value, placement_ask_value in zip(
                source_bid_tick,
                source_ask_tick,
                placement_bid_tick,
                placement_ask_tick,
                strict=True,
            )
        ],
        dtype=bool,
    )
    reasons[(reasons == "") & ~valid_ticks] = "bbo_not_valid_integer_ticks"
    bbo_matches = np.asarray(
        [
            valid
            and int(source_bid) == int(placement_bid_value)
            and int(source_ask) == int(placement_ask_value)
            for valid, source_bid, source_ask, placement_bid_value, placement_ask_value in zip(
                valid_ticks,
                source_bid_tick,
                source_ask_tick,
                placement_bid_tick,
                placement_ask_tick,
                strict=True,
            )
        ],
        dtype=bool,
    )
    reasons[(reasons == "") & ~bbo_matches] = "policy_visible_decision_bbo_tick_mismatch"

    offsets = np.arange(int(slow_window_s), -1, -1, dtype=np.int64)
    for lower in range(0, row_count, int(chunk_rows)):
        upper = min(lower + int(chunk_rows), row_count)
        rows = np.arange(lower, upper, dtype=np.int64)
        selected = rows[reasons[rows] == ""]
        if selected.size == 0:
            continue
        queries = decision_ts[selected, None] - offsets[None, :] * 1_000
        delays = visibility_delay_ms(
            queries,
            samples_ms=profile.samples_ms,
            seed=int(source.visibility_seed),
        )
        visible_queries = queries - delays
        history_idx = (
            np.searchsorted(bbo_ts, visible_queries.ravel(), side="right").reshape(queries.shape)
            - 1
        )
        history_safe = np.clip(history_idx, 0, len(bbo_ts) - 1)
        history_age = queries - bbo_ts[history_safe]
        history_bids = bids[history_safe]
        history_asks = asks[history_safe]
        valid_history = np.all(
            (history_idx >= 0)
            & np.isfinite(history_bids)
            & np.isfinite(history_asks)
            & (history_bids > 0.0)
            & (history_asks > history_bids)
            & (history_age >= 0)
            & (history_age <= int(max_bbo_age_ms)),
            axis=1,
        )
        reasons[selected[~valid_history]] = "policy_visible_60s_bbo_history_incomplete"
        retained = selected[valid_history]
        if retained.size == 0:
            continue
        history_mid = 0.5 * (history_bids[valid_history] + history_asks[valid_history])
        differences = np.diff(history_mid, axis=1)
        with np.errstate(invalid="ignore"):
            fast_variance = np.var(differences[:, -int(fast_window_s) :], axis=1, ddof=1)
            slow_variance = np.var(differences, axis=1, ddof=1)
        fast_variance = np.maximum(fast_variance, float(variance_floor))
        slow_variance = np.maximum(slow_variance, float(variance_floor))
        valid_variance = (
            np.isfinite(fast_variance)
            & np.isfinite(slow_variance)
            & (fast_variance > 0.0)
            & (slow_variance > 0.0)
        )
        reasons[retained[~valid_variance]] = "causal_variance_invalid"
        retained = retained[valid_variance]
        if retained.size == 0:
            continue
        fast_variance = fast_variance[valid_variance]
        slow_variance = slow_variance[valid_variance]
        fast_sigma = np.sqrt(fast_variance)
        slow_sigma = np.sqrt(slow_variance)
        current = current_safe[retained]
        ready_ts = bbo_ts[current] + current_delay[retained]

        context["feature_ready_ts_ms"][retained] = ready_ts
        context["best_bid"][retained] = bids[current]
        context["best_ask"][retained] = asks[current]
        context["mid"][retained] = 0.5 * (bids[current] + asks[current])
        context["spread"][retained] = asks[current] - bids[current]
        context["fast_variance"][retained] = fast_variance
        context["slow_variance"][retained] = slow_variance
        context["fast_sigma"][retained] = fast_sigma
        context["slow_sigma"][retained] = slow_sigma
        context["volatility_ratio"][retained] = fast_sigma / slow_sigma
        context["book_age_ms"][retained] = decision_ts[retained] - bbo_ts[current]

    supported = reasons == ""
    late = supported & (context["feature_ready_ts_ms"] > decision_ts)
    reasons[late] = "source_feature_ready_after_decision"
    supported = reasons == ""

    for field in DECISION_CONTEXT_FIELDS:
        result[field] = context[field]
    result["visibility_delay_ms"] = current_delay
    result["visible_bbo_cutoff_ts_ms"] = current_cutoff
    result["supported"] = supported
    result["unsupported_reason"] = np.where(supported, None, reasons)

    metadata: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "feature_semantics_source": "p3_touch_volatility_conditioned.v4",
        "feature_graph_id": P3_TOUCH_CONDITIONAL_GRAPH.graph_id,
        "feature_graph_sha256": P3_TOUCH_CONDITIONAL_GRAPH.sha256(),
        "source_identity": source.source_identity,
        "source_path": str(source.path.expanduser().resolve()),
        "source_sha256": source.sha256,
        "source_timestamp_semantics": source.timestamp_semantics,
        "visibility_profile_path": str(source.visibility_profile_path.expanduser().resolve()),
        "visibility_profile_sha256": source.visibility_profile_sha256,
        "visibility_profile_id": source.visibility_profile_id,
        "visibility_profile_age_column": profile.age_column,
        "visibility_profile_timestamp_semantics": profile.timestamp_semantics,
        "visibility_profile_sample_count": int(profile.samples_ms.size),
        "visibility_sampler_version": VISIBILITY_SAMPLER_VERSION,
        "visibility_seed": int(source.visibility_seed),
        "visibility_delay_mean_ms": float(np.mean(current_delay)),
        "visibility_delay_p50_ms": float(np.quantile(current_delay, 0.50)),
        "visibility_delay_p90_ms": float(np.quantile(current_delay, 0.90)),
        "visibility_delay_p99_ms": float(np.quantile(current_delay, 0.99)),
        "visibility_delay_max_ms": int(np.max(current_delay)),
        "aws_receive_time_transport_supported": False,
        "decision_cadence": "baseline_eligible_quote_decision",
        "canonical_10s_context_reuse_allowed": False,
        "decision_bbo_must_equal_context_bbo_in_integer_ticks": True,
        "rows": int(row_count),
        "supported_rows": int(np.sum(supported)),
        "unsupported_rows": int(np.sum(~supported)),
        "unsupported_reason_counts": {
            str(reason): int(count)
            for reason, count in pd.Series(reasons[~supported]).value_counts().items()
        },
        "decision_cadence_transport_supported": False,
        "overlapping_label_cluster_contract": dict(OVERLAPPING_LABEL_CLUSTER_CONTRACT),
        "permissions": dict(PERMISSIONS),
    }
    return DecisionCadenceContextBatch(frame=result, metadata=metadata)


__all__ = [
    "FrozenPolicyVisibleBboSource",
    "IDENTITY",
    "PERMISSIONS",
    "SCHEMA_VERSION",
    "SOURCE_TIMESTAMP_SEMANTICS",
    "VISIBILITY_SAMPLER_VERSION",
    "VisibilityProfile",
    "extract_policy_visible_decision_context",
    "load_f06_baseline_eligible_decisions",
    "visibility_delay_ms",
]
