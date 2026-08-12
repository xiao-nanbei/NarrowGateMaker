"""Pure state machine for a persistent conditional-P3 reach budget.

The independent toxicity signal decides whether protection is requested.
Conditional P3 only chooses the smallest outward tick penalty that satisfies
the frozen reach-reduction budget and its precomputed simultaneous band.  The
selected penalty is held for the complete canonical 10-second epoch.

This module performs no I/O and owns no orders.  Callers retain responsibility
for tick/GTX/spread-cap validation, cancel/ACK routing, and hard-safety actions.
Prices are represented as integer ticks so the transition surface can be
mirrored directly in C++.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import IntEnum

CANONICAL_BUCKET_NS = 10_000_000_000
DEFAULT_REACH_REDUCTION = 0.01
MAX_OUTWARD_TICKS = 16


class QuoteSide(IntEnum):
    BUY = 0
    SELL = 1


class QuoteOperation(IntEnum):
    PLACE = 0
    KEEP = 1
    REPLACE = 2


class SimultaneousBandStatus(IntEnum):
    UNSUPPORTED = 0
    NOT_CONFIRMED = 1
    CONFIRMED = 2


class BucketEvaluationCode(IntEnum):
    NEVER_EVALUATED = 0
    ACTIVATED = 1
    DUPLICATE_BUCKET = 2
    TOXICITY_NOT_TRIGGERED = 3
    P3_UNSUPPORTED = 4
    INVALID_CURVE = 5
    REACH_BUDGET_NOT_CONFIRMED = 6
    FLAT_ENDED = 7


class QuoteDecisionCode(IntEnum):
    APPLIED = 0
    EPISODE_INACTIVE = 1
    OUTSIDE_ACTIVE_BUCKET = 2
    SIDE_MISMATCH = 3
    REDUCING_UNCHANGED = 4
    HARD_SAFETY_OVERRIDE = 5
    INVALID_EFFECTIVE_PRICE = 6


@dataclass(frozen=True)
class ReachBudgetPolicyConfig:
    reach_reduction: float = DEFAULT_REACH_REDUCTION
    max_outward_ticks: int = MAX_OUTWARD_TICKS


_DEFAULT_POLICY_CONFIG = ReachBudgetPolicyConfig()


@dataclass(frozen=True)
class ConditionalReachCurve:
    """Side-specific paired curve indexed by outward ticks from baseline.

    Index zero is the baseline reach probability.  Index ``k`` is the reach
    probability at ``baseline_distance_ticks + k``.  A CONFIRMED status means
    the precomputed simultaneous-band artifact confirms the frozen budget for
    that candidate; this module does not construct confidence intervals.
    """

    side: QuoteSide
    bucket_start_ns: int
    baseline_distance_ticks: int
    reach_probabilities: Sequence[float]
    simultaneous_band_statuses: Sequence[int]
    support_valid: bool = True


@dataclass(frozen=True)
class ReachBudgetPolicyState:
    side: QuoteSide
    last_evaluated_bucket_start_ns: int = -1
    active_bucket_start_ns: int = -1
    active: bool = False
    penalty_ticks: int = 0
    baseline_distance_ticks: int = -1
    baseline_reach_probability: float | None = None
    candidate_reach_probability: float | None = None
    evaluation_code: BucketEvaluationCode = BucketEvaluationCode.NEVER_EVALUATED


@dataclass(frozen=True)
class BucketEvaluation:
    state: ReachBudgetPolicyState
    bucket_start_ns: int
    p3_query_performed: bool
    evaluation_code: BucketEvaluationCode


@dataclass(frozen=True)
class QuotePolicyDecision:
    side: QuoteSide
    operation: QuoteOperation
    baseline_price_tick: int
    effective_price_tick: int
    penalty_ticks: int
    applied: bool
    decision_code: QuoteDecisionCode


def canonical_bucket_start_ns(decision_ts_ns: int) -> int:
    timestamp = int(decision_ts_ns)
    if timestamp < 0:
        raise ValueError("decision_ts_ns must be nonnegative")
    return (timestamp // CANONICAL_BUCKET_NS) * CANONICAL_BUCKET_NS


def initial_policy_state(side: QuoteSide | int) -> ReachBudgetPolicyState:
    return ReachBudgetPolicyState(side=_as_side(side))


def advance_canonical_bucket(
    state: ReachBudgetPolicyState,
    *,
    decision_ts_ns: int,
    toxicity_triggered: bool,
    curve: ConditionalReachCurve | None,
    config: ReachBudgetPolicyConfig = _DEFAULT_POLICY_CONFIG,
) -> BucketEvaluation:
    """Evaluate P3 at most once per canonical bucket and freeze its penalty."""

    side = _as_side(state.side)
    reach_reduction, max_ticks = _validated_config(config)
    bucket_start = canonical_bucket_start_ns(decision_ts_ns)
    last_bucket = int(state.last_evaluated_bucket_start_ns)
    if last_bucket > bucket_start:
        raise ValueError("canonical bucket clock regressed")
    if last_bucket == bucket_start:
        return BucketEvaluation(
            state=state,
            bucket_start_ns=bucket_start,
            p3_query_performed=False,
            evaluation_code=BucketEvaluationCode.DUPLICATE_BUCKET,
        )

    inactive = ReachBudgetPolicyState(
        side=side,
        last_evaluated_bucket_start_ns=bucket_start,
        evaluation_code=BucketEvaluationCode.TOXICITY_NOT_TRIGGERED,
    )
    if not bool(toxicity_triggered):
        return BucketEvaluation(
            state=inactive,
            bucket_start_ns=bucket_start,
            p3_query_performed=False,
            evaluation_code=BucketEvaluationCode.TOXICITY_NOT_TRIGGERED,
        )

    if curve is None or not bool(curve.support_valid):
        unsupported = replace(
            inactive, evaluation_code=BucketEvaluationCode.P3_UNSUPPORTED
        )
        return BucketEvaluation(
            state=unsupported,
            bucket_start_ns=bucket_start,
            p3_query_performed=True,
            evaluation_code=BucketEvaluationCode.P3_UNSUPPORTED,
        )

    validated = _validated_curve(
        curve,
        side=side,
        bucket_start_ns=bucket_start,
        max_outward_ticks=max_ticks,
    )
    if validated is None:
        invalid = replace(inactive, evaluation_code=BucketEvaluationCode.INVALID_CURVE)
        return BucketEvaluation(
            state=invalid,
            bucket_start_ns=bucket_start,
            p3_query_performed=True,
            evaluation_code=BucketEvaluationCode.INVALID_CURVE,
        )

    probabilities, statuses, baseline_distance = validated
    baseline_reach = probabilities[0]
    selected_ticks = 0
    for outward_ticks in range(max_ticks + 1):
        reach_drop = baseline_reach - probabilities[outward_ticks]
        if (
            statuses[outward_ticks] == SimultaneousBandStatus.CONFIRMED
            and reach_drop + 1e-12 >= reach_reduction
        ):
            selected_ticks = outward_ticks
            break

    if selected_ticks <= 0:
        no_budget = replace(
            inactive,
            baseline_distance_ticks=baseline_distance,
            baseline_reach_probability=baseline_reach,
            evaluation_code=BucketEvaluationCode.REACH_BUDGET_NOT_CONFIRMED,
        )
        return BucketEvaluation(
            state=no_budget,
            bucket_start_ns=bucket_start,
            p3_query_performed=True,
            evaluation_code=BucketEvaluationCode.REACH_BUDGET_NOT_CONFIRMED,
        )

    activated = ReachBudgetPolicyState(
        side=side,
        last_evaluated_bucket_start_ns=bucket_start,
        active_bucket_start_ns=bucket_start,
        active=True,
        penalty_ticks=selected_ticks,
        baseline_distance_ticks=baseline_distance,
        baseline_reach_probability=baseline_reach,
        candidate_reach_probability=probabilities[selected_ticks],
        evaluation_code=BucketEvaluationCode.ACTIVATED,
    )
    return BucketEvaluation(
        state=activated,
        bucket_start_ns=bucket_start,
        p3_query_performed=True,
        evaluation_code=BucketEvaluationCode.ACTIVATED,
    )


def end_episode_on_flat(
    state: ReachBudgetPolicyState,
    *,
    decision_ts_ns: int,
) -> ReachBudgetPolicyState:
    """End an active episode without making the current bucket queryable again."""

    bucket_start = canonical_bucket_start_ns(decision_ts_ns)
    if int(state.last_evaluated_bucket_start_ns) > bucket_start:
        raise ValueError("flat event precedes the last evaluated bucket")
    if not state.active:
        return state
    return ReachBudgetPolicyState(
        side=_as_side(state.side),
        last_evaluated_bucket_start_ns=state.last_evaluated_bucket_start_ns,
        evaluation_code=BucketEvaluationCode.FLAT_ENDED,
    )


def apply_to_quote(
    state: ReachBudgetPolicyState,
    *,
    decision_ts_ns: int,
    side: QuoteSide | int,
    operation: QuoteOperation | int,
    exposure_increasing: bool,
    baseline_price_tick: int,
    hard_safety_override: bool = False,
) -> QuotePolicyDecision:
    """Apply the frozen penalty to one quote without mutating policy state."""

    quote_side = _as_side(side)
    quote_operation = _as_operation(operation)
    baseline_tick = int(baseline_price_tick)
    if baseline_tick <= 0:
        raise ValueError("baseline_price_tick must be positive")

    def unchanged(code: QuoteDecisionCode) -> QuotePolicyDecision:
        return QuotePolicyDecision(
            side=quote_side,
            operation=quote_operation,
            baseline_price_tick=baseline_tick,
            effective_price_tick=baseline_tick,
            penalty_ticks=0,
            applied=False,
            decision_code=code,
        )

    if bool(hard_safety_override):
        return unchanged(QuoteDecisionCode.HARD_SAFETY_OVERRIDE)
    if not state.active or int(state.penalty_ticks) <= 0:
        return unchanged(QuoteDecisionCode.EPISODE_INACTIVE)
    if canonical_bucket_start_ns(decision_ts_ns) != int(
        state.active_bucket_start_ns
    ):
        return unchanged(QuoteDecisionCode.OUTSIDE_ACTIVE_BUCKET)
    if quote_side != _as_side(state.side):
        return unchanged(QuoteDecisionCode.SIDE_MISMATCH)
    if not bool(exposure_increasing):
        return unchanged(QuoteDecisionCode.REDUCING_UNCHANGED)

    penalty = int(state.penalty_ticks)
    effective_tick = (
        baseline_tick - penalty
        if quote_side == QuoteSide.BUY
        else baseline_tick + penalty
    )
    if effective_tick <= 0:
        return unchanged(QuoteDecisionCode.INVALID_EFFECTIVE_PRICE)
    return QuotePolicyDecision(
        side=quote_side,
        operation=quote_operation,
        baseline_price_tick=baseline_tick,
        effective_price_tick=effective_tick,
        penalty_ticks=penalty,
        applied=True,
        decision_code=QuoteDecisionCode.APPLIED,
    )


def _validated_config(config: ReachBudgetPolicyConfig) -> tuple[float, int]:
    reach_reduction = float(config.reach_reduction)
    max_ticks = int(config.max_outward_ticks)
    if not math.isfinite(reach_reduction) or not 0.0 < reach_reduction < 1.0:
        raise ValueError("reach_reduction must lie strictly inside (0, 1)")
    if not 1 <= max_ticks <= MAX_OUTWARD_TICKS:
        raise ValueError(f"max_outward_ticks must lie in [1, {MAX_OUTWARD_TICKS}]")
    return reach_reduction, max_ticks


def _validated_curve(
    curve: ConditionalReachCurve,
    *,
    side: QuoteSide,
    bucket_start_ns: int,
    max_outward_ticks: int,
) -> tuple[tuple[float, ...], tuple[SimultaneousBandStatus, ...], int] | None:
    try:
        if _as_side(curve.side) != side:
            return None
        if int(curve.bucket_start_ns) != bucket_start_ns:
            return None
        baseline_distance = int(curve.baseline_distance_ticks)
        if baseline_distance < 0:
            return None
        expected = max_outward_ticks + 1
        if len(curve.reach_probabilities) != expected:
            return None
        if len(curve.simultaneous_band_statuses) != expected:
            return None
        probabilities = tuple(float(curve.reach_probabilities[i]) for i in range(expected))
        if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in probabilities):
            return None
        if any(
            probabilities[index] > probabilities[index - 1] + 1e-12
            for index in range(1, expected)
        ):
            return None
        statuses = tuple(
            SimultaneousBandStatus(int(curve.simultaneous_band_statuses[i]))
            for i in range(expected)
        )
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    return probabilities, statuses, baseline_distance


def _as_side(side: QuoteSide | int) -> QuoteSide:
    try:
        return QuoteSide(int(side))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("side must be QuoteSide.BUY or QuoteSide.SELL") from exc


def _as_operation(operation: QuoteOperation | int) -> QuoteOperation:
    try:
        return QuoteOperation(int(operation))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("operation must be PLACE, KEEP, or REPLACE") from exc


__all__ = [
    "BucketEvaluation",
    "BucketEvaluationCode",
    "CANONICAL_BUCKET_NS",
    "ConditionalReachCurve",
    "DEFAULT_REACH_REDUCTION",
    "MAX_OUTWARD_TICKS",
    "QuoteDecisionCode",
    "QuoteOperation",
    "QuotePolicyDecision",
    "QuoteSide",
    "ReachBudgetPolicyConfig",
    "ReachBudgetPolicyState",
    "SimultaneousBandStatus",
    "advance_canonical_bucket",
    "apply_to_quote",
    "canonical_bucket_start_ns",
    "end_episode_on_flat",
    "initial_policy_state",
]
