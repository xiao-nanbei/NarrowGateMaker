#!/usr/bin/env python3
"""Research-only decision-cadence contexts for conditional P3 v4.1.

The frozen v4.1 model was trained on non-overlapping 10-second window starts.
This module does not claim that the model transports to arbitrary quote
decisions.  It only reconstructs the same causal BBO/volatility feature
semantics at baseline-eligible F06 decision timestamps so that a later
transport audit can test that claim.

The placement parquet is an opportunity index, not a feature source.  Every
context is rebuilt from one hash-bound causal BBO tape, and the BBO observed by
the placement row must match that source view in integer ticks.  No touch
label, fill outcome, markout, PnL, model fit, or action is read or produced.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.feature_dag import P3_TOUCH_CONDITIONAL_GRAPH
from research.families.f02_empirical_p3_touch.audit.p3_touch_calibration import (
    _timestamp_ms,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_window_context import (
    _book_validity,
    _last_known_indices,
)

IDENTITY = "p3_touch_decision_cadence_context.v1"
SCHEMA_VERSION = "narrowgate_p3_touch_decision_cadence_context.v1"
FEATURE_SEMANTICS_SOURCE = "p3_touch_volatility_conditioned.v4"
SOURCE_TIMESTAMP_SEMANTICS = "normalized_v2_causal_observation_time_ms"
SIDES = ("BUY", "SELL")
BASELINE_ACTIONS = frozenset({"place", "replace", "keep"})

# These are the only placement columns this module is allowed to read.  In
# particular, lifecycle outcomes, fills, labels, markouts, reward, and PnL are
# deliberately absent even when the source parquet contains them.
PLACEMENT_DECISION_COLUMNS = (
    "decision_id",
    "day",
    "side",
    "inventory_role",
    "campaign_id",
    "submit_ts_ns",
    "feature_ready_ts_ns",
    "best_bid",
    "best_ask",
    "baseline_price_tick",
    "baseline_action",
    "allow_post",
)

DECISION_CONTEXT_FIELDS = (
    "start_ts_ms",
    "feature_ready_ts_ms",
    "best_bid",
    "best_ask",
    "mid",
    "spread",
    "fast_variance",
    "slow_variance",
    "fast_sigma",
    "slow_sigma",
    "volatility_ratio",
    "book_age_ms",
)

PERMISSIONS = {
    "transport_research_only": True,
    "training_authority": False,
    "prediction_authority": False,
    "quote_mapping_authority": False,
    "action_authority": False,
    "live_authority": False,
    "economic_outcomes_read": False,
}

OVERLAPPING_LABEL_CLUSTER_CONTRACT = {
    "label_horizon_s": 10.0,
    "labels_extracted_by_this_module": False,
    "arbitrary_decision_windows_may_overlap": True,
    "overlapping_rows_must_not_be_treated_as_independent": True,
    "minimum_cluster_keys": ["day", "campaign_id"],
    "assignment_episode_cluster_required_for_action_research": True,
}


class DecisionCadenceContextError(ValueError):
    """Raised when an input artifact or denominator violates the contract."""


@dataclass(frozen=True)
class FrozenCausalBboSource:
    """Hash-bound historical BBO source with a causal observation timestamp."""

    path: Path
    sha256: str
    source_identity: str
    timestamp_semantics: str = SOURCE_TIMESTAMP_SEMANTICS


@dataclass(frozen=True)
class DecisionCadenceContextBatch:
    """Aligned decision rows, contexts, fail-closed reasons, and metadata."""

    frame: pd.DataFrame
    metadata: Mapping[str, Any]

    @property
    def supported(self) -> pd.DataFrame:
        return self.frame.loc[self.frame["supported"]].reset_index(drop=True)

    def model_context(self) -> dict[str, np.ndarray]:
        """Return only supported, v4 feature-compatible causal arrays."""

        supported = self.supported
        return {field: supported[field].to_numpy(copy=True) for field in DECISION_CONTEXT_FIELDS}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_sha256(path: Path, expected: str, *, label: str) -> str:
    expected = str(expected).lower()
    if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
        raise DecisionCadenceContextError(f"{label} has an invalid SHA256")
    if not path.is_file():
        raise DecisionCadenceContextError(f"{label} is missing: {path}")
    observed = _sha256_file(path)
    if observed != expected:
        raise DecisionCadenceContextError(
            f"{label} hash mismatch: observed={observed} expected={expected}"
        )
    return observed


def _price_to_tick(price: Any, tick_size: float) -> int | None:
    try:
        numeric = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0.0:
        return None
    ratio = numeric / float(tick_size)
    nearest = round(ratio)
    if abs(ratio - nearest) > 1e-9:
        return None
    return int(nearest)


def load_f06_baseline_eligible_decisions(
    placement_path: Path,
    *,
    expected_sha256: str,
) -> pd.DataFrame:
    """Load only decision-visible fields from one frozen F06 partition.

    The function validates rather than invents baseline eligibility.  Every
    admitted row must already be a postable baseline place/replace/keep
    opportunity with a unique side-specific decision identity.
    """

    path = placement_path.expanduser().resolve()
    _verify_sha256(path, expected_sha256, label="F06 placement parquet")
    try:
        frame = pd.read_parquet(path, columns=list(PLACEMENT_DECISION_COLUMNS))
    except (OSError, ValueError, KeyError) as exc:
        raise DecisionCadenceContextError(
            "F06 placement parquet lacks the frozen decision-visible schema"
        ) from exc
    if frame.empty:
        raise DecisionCadenceContextError("F06 placement denominator is empty")
    if frame.isna().any().any():
        missing = sorted(frame.columns[frame.isna().any()].tolist())
        raise DecisionCadenceContextError(f"F06 placement decision fields contain nulls: {missing}")

    decision_id = frame["decision_id"].astype(str)
    if decision_id.str.strip().eq("").any() or decision_id.str.lower().eq("nan").any():
        raise DecisionCadenceContextError("F06 decision_id must be non-empty")
    if decision_id.duplicated().any():
        raise DecisionCadenceContextError("F06 decision_id must be unique")
    if not frame["side"].astype(str).isin(SIDES).all():
        raise DecisionCadenceContextError("F06 placement side is unsupported")
    actions = frame["baseline_action"].astype(str)
    if not actions.isin(BASELINE_ACTIONS).all():
        raise DecisionCadenceContextError("F06 denominator includes a non-posting baseline action")
    allow_post = pd.to_numeric(frame["allow_post"], errors="coerce")
    if not allow_post.eq(1).all():
        raise DecisionCadenceContextError(
            "F06 denominator includes a baseline-ineligible non-postable row"
        )

    submit = pd.to_numeric(frame["submit_ts_ns"], errors="coerce")
    ready = pd.to_numeric(frame["feature_ready_ts_ns"], errors="coerce")
    if submit.isna().any() or ready.isna().any():
        raise DecisionCadenceContextError("F06 decision clocks must be numeric")
    if (submit <= 0).any() or (ready > submit).any():
        raise DecisionCadenceContextError(
            "F06 feature-ready clock exceeds the baseline decision clock"
        )
    if (submit.astype(np.int64) % 1_000_000 != 0).any():
        raise DecisionCadenceContextError(
            "F06 decision timestamp is not exactly representable in milliseconds"
        )

    baseline_price_tick = pd.to_numeric(
        frame["baseline_price_tick"], errors="coerce"
    )
    if baseline_price_tick.isna().any() or (baseline_price_tick <= 0).any():
        raise DecisionCadenceContextError(
            "F06 baseline executable price tick must be positive"
        )

    result = frame.copy()
    result["decision_id"] = decision_id
    result["submit_ts_ns"] = submit.astype(np.int64)
    result["feature_ready_ts_ns"] = ready.astype(np.int64)
    result["baseline_price_tick"] = baseline_price_tick.astype(np.int64)
    result["decision_ts_ms"] = result["submit_ts_ns"] // 1_000_000
    return result


def _load_causal_bbo(
    source: FrozenCausalBboSource,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    path = source.path.expanduser().resolve()
    _verify_sha256(path, source.sha256, label="causal BBO source")
    if source.timestamp_semantics != SOURCE_TIMESTAMP_SEMANTICS:
        raise DecisionCadenceContextError(
            "BBO timestamp must use the frozen normalized-v2 causal observation clock"
        )
    if not str(source.source_identity).strip():
        raise DecisionCadenceContextError("BBO source identity must be non-empty")
    try:
        bbo = pd.read_parquet(
            path,
            columns=["timestamp", "best_bid", "best_ask"],
        ).dropna()
    except (OSError, ValueError, KeyError) as exc:
        raise DecisionCadenceContextError(
            "causal BBO source lacks timestamp/best_bid/best_ask"
        ) from exc
    if bbo.empty:
        raise DecisionCadenceContextError("causal BBO source is empty")
    timestamps = _timestamp_ms(bbo["timestamp"])
    order = np.argsort(timestamps, kind="stable")
    timestamps = timestamps[order]
    bids = pd.to_numeric(bbo["best_bid"], errors="coerce").to_numpy(dtype=np.float64)[order]
    asks = pd.to_numeric(bbo["best_ask"], errors="coerce").to_numpy(dtype=np.float64)[order]
    if not np.all(np.diff(timestamps) >= 0):
        raise DecisionCadenceContextError("causal BBO timestamps are not sortable")
    return timestamps, bids, asks


def _utc_days(timestamp_ms: np.ndarray) -> np.ndarray:
    return pd.to_datetime(timestamp_ms, unit="ms", utc=True).strftime("%Y-%m-%d").to_numpy()


def extract_decision_cadence_context(
    decisions: pd.DataFrame,
    *,
    source: FrozenCausalBboSource,
    tick_size: float = 0.1,
    max_bbo_age_ms: int = 5_000,
    fast_window_s: int = 10,
    slow_window_s: int = 60,
    variance_floor: float = 1e-6,
    chunk_rows: int = 50_000,
) -> DecisionCadenceContextBatch:
    """Rebuild v4.1 feature semantics at arbitrary decision timestamps.

    Unsupported rows remain in the denominator with an explicit reason.  The
    caller must not substitute a canonical 10-second context, interpolate a
    nearby context, or combine these features with a different BBO.
    """

    required = set(PLACEMENT_DECISION_COLUMNS).union({"decision_ts_ms"})
    missing = sorted(required.difference(decisions.columns))
    if missing:
        raise DecisionCadenceContextError(f"decision denominator lacks fields: {missing}")
    if decisions.empty:
        raise DecisionCadenceContextError("decision denominator is empty")
    decision_ids = decisions["decision_id"].astype(str)
    if (
        decision_ids.str.strip().eq("").any()
        or decision_ids.str.lower().eq("nan").any()
        or decision_ids.duplicated().any()
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
    submit_ns = pd.to_numeric(decisions["submit_ts_ns"], errors="coerce")
    ready_ns = pd.to_numeric(decisions["feature_ready_ts_ns"], errors="coerce")
    if submit_ns.isna().any() or ready_ns.isna().any():
        raise DecisionCadenceContextError("decision clocks must be numeric")
    submit_ns_values = submit_ns.to_numpy(dtype=np.int64)
    ready_ns_values = ready_ns.to_numpy(dtype=np.int64)
    if (
        np.any(submit_ns_values <= 0)
        or np.any(ready_ns_values > submit_ns_values)
        or np.any(submit_ns_values % 1_000_000 != 0)
    ):
        raise DecisionCadenceContextError(
            "decision denominator violates the causal millisecond clock contract"
        )
    if not math.isfinite(float(tick_size)) or float(tick_size) <= 0.0:
        raise DecisionCadenceContextError("tick_size must be positive")
    if int(fast_window_s) < 2 or int(slow_window_s) < int(fast_window_s):
        raise DecisionCadenceContextError("invalid fast/slow variance windows")
    if float(variance_floor) <= 0.0 or not math.isfinite(float(variance_floor)):
        raise DecisionCadenceContextError("variance_floor must be positive")
    if int(max_bbo_age_ms) < 0 or int(chunk_rows) <= 0:
        raise DecisionCadenceContextError("invalid BBO age or chunk size")

    bbo_ts, bids, asks = _load_causal_bbo(source)
    result = decisions.reset_index(drop=True).copy()
    decision_ts = pd.to_numeric(result["decision_ts_ms"], errors="coerce")
    if decision_ts.isna().any():
        raise DecisionCadenceContextError("decision_ts_ms must be numeric")
    decision_ts_np = decision_ts.to_numpy(dtype=np.int64)
    if not np.array_equal(decision_ts_np, submit_ns_values // 1_000_000):
        raise DecisionCadenceContextError("decision_ts_ms does not match submit_ts_ns")
    row_count = len(result)

    context = {
        field: np.full(row_count, np.nan, dtype=np.float64) for field in DECISION_CONTEXT_FIELDS
    }
    context["start_ts_ms"] = decision_ts_np.copy()
    context["feature_ready_ts_ms"] = np.full(row_count, -1, dtype=np.int64)
    reasons = np.full(row_count, "", dtype=object)

    decision_days = _utc_days(decision_ts_np)
    declared_days = result["day"].astype(str).to_numpy()
    reasons[decision_days != declared_days] = "decision_timestamp_day_mismatch"

    placement_ready = result["feature_ready_ts_ns"].to_numpy(dtype=np.int64)
    placement_submit = result["submit_ts_ns"].to_numpy(dtype=np.int64)
    reasons[(reasons == "") & (placement_ready > placement_submit)] = (
        "placement_feature_ready_after_decision"
    )

    current_idx = _last_known_indices(bbo_ts, decision_ts_np)
    valid_current, current_safe = _book_validity(
        source_ts_ms=bbo_ts,
        bids=bids,
        asks=asks,
        query_ts_ms=decision_ts_np,
        indices=current_idx,
        max_bbo_age_ms=int(max_bbo_age_ms),
    )
    reasons[(reasons == "") & ~valid_current] = "current_bbo_unavailable_or_stale"

    source_bid = bids[current_safe]
    source_ask = asks[current_safe]
    placement_bid = result["best_bid"].to_numpy(dtype=np.float64)
    placement_ask = result["best_ask"].to_numpy(dtype=np.float64)
    source_bid_tick = np.asarray(
        [_price_to_tick(value, float(tick_size)) for value in source_bid],
        dtype=object,
    )
    source_ask_tick = np.asarray(
        [_price_to_tick(value, float(tick_size)) for value in source_ask],
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
            bid is not None
            and ask is not None
            and placement_b is not None
            and placement_a is not None
            and int(bid) < int(ask)
            and int(placement_b) < int(placement_a)
            for bid, ask, placement_b, placement_a in zip(
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
            valid and int(source_b) == int(placement_b) and int(source_a) == int(placement_a)
            for valid, source_b, source_a, placement_b, placement_a in zip(
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
    reasons[(reasons == "") & ~bbo_matches] = "decision_bbo_source_tick_mismatch"

    offsets = np.arange(int(slow_window_s), -1, -1, dtype=np.int64)
    for lower in range(0, row_count, int(chunk_rows)):
        upper = min(lower + int(chunk_rows), row_count)
        rows = np.arange(lower, upper, dtype=np.int64)
        eligible = reasons[rows] == ""
        if not np.any(eligible):
            continue
        selected = rows[eligible]
        queries = decision_ts_np[selected, None] - offsets[None, :] * 1_000
        history_idx = _last_known_indices(bbo_ts, queries.ravel()).reshape(queries.shape)
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
        failed = selected[~valid_history]
        reasons[failed] = "causal_60s_bbo_history_incomplete"
        if not np.any(valid_history):
            continue

        retained = selected[valid_history]
        history_mid = 0.5 * (history_bids[valid_history] + history_asks[valid_history])
        differences = np.diff(history_mid, axis=1)
        with np.errstate(invalid="ignore"):
            fast_variance = np.var(
                differences[:, -int(fast_window_s) :],
                axis=1,
                ddof=1,
            )
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

        context["feature_ready_ts_ms"][retained] = bbo_ts[current]
        context["best_bid"][retained] = bids[current]
        context["best_ask"][retained] = asks[current]
        context["mid"][retained] = 0.5 * (bids[current] + asks[current])
        context["spread"][retained] = asks[current] - bids[current]
        context["fast_variance"][retained] = fast_variance
        context["slow_variance"][retained] = slow_variance
        context["fast_sigma"][retained] = fast_sigma
        context["slow_sigma"][retained] = slow_sigma
        context["volatility_ratio"][retained] = fast_sigma / slow_sigma
        context["book_age_ms"][retained] = decision_ts_np[retained] - bbo_ts[current]

    supported = reasons == ""
    source_ready = context["feature_ready_ts_ms"]
    late = supported & (source_ready > decision_ts_np)
    reasons[late] = "source_feature_ready_after_decision"
    supported = reasons == ""

    for field in DECISION_CONTEXT_FIELDS:
        result[field] = context[field]
    result["supported"] = supported
    result["unsupported_reason"] = np.where(supported, None, reasons)

    metadata = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "feature_semantics_source": FEATURE_SEMANTICS_SOURCE,
        "feature_graph_id": P3_TOUCH_CONDITIONAL_GRAPH.graph_id,
        "feature_graph_sha256": P3_TOUCH_CONDITIONAL_GRAPH.sha256(),
        "source_identity": source.source_identity,
        "source_path": str(source.path.expanduser().resolve()),
        "source_sha256": source.sha256,
        "source_timestamp_semantics": source.timestamp_semantics,
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
    "BASELINE_ACTIONS",
    "DECISION_CONTEXT_FIELDS",
    "DecisionCadenceContextBatch",
    "DecisionCadenceContextError",
    "FrozenCausalBboSource",
    "IDENTITY",
    "OVERLAPPING_LABEL_CLUSTER_CONTRACT",
    "PERMISSIONS",
    "PLACEMENT_DECISION_COLUMNS",
    "SCHEMA_VERSION",
    "SOURCE_TIMESTAMP_SEMANTICS",
    "extract_decision_cadence_context",
    "load_f06_baseline_eligible_decisions",
]
