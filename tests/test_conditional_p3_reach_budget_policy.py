from __future__ import annotations

from collections.abc import Sequence

import pytest

from strategy.conditional_p3_reach_budget_policy import (
    CANONICAL_BUCKET_NS,
    BucketEvaluationCode,
    ConditionalReachCurve,
    QuoteDecisionCode,
    QuoteOperation,
    QuoteSide,
    SimultaneousBandStatus,
    advance_canonical_bucket,
    apply_to_quote,
    canonical_bucket_start_ns,
    end_episode_on_flat,
    initial_policy_state,
)

BUCKET_START_NS = 30 * CANONICAL_BUCKET_NS


def _curve(
    *,
    side: QuoteSide = QuoteSide.BUY,
    bucket_start_ns: int = BUCKET_START_NS,
    probabilities: Sequence[float] | None = None,
    statuses: Sequence[int] | None = None,
    support_valid: bool = True,
) -> ConditionalReachCurve:
    values = probabilities or tuple(0.20 - 0.003 * index for index in range(17))
    band = statuses or (SimultaneousBandStatus.CONFIRMED,) * 17
    return ConditionalReachCurve(
        side=side,
        bucket_start_ns=bucket_start_ns,
        baseline_distance_ticks=5,
        reach_probabilities=values,
        simultaneous_band_statuses=band,
        support_valid=support_valid,
    )


def _activate(
    *,
    side: QuoteSide = QuoteSide.BUY,
    curve: ConditionalReachCurve | None = None,
):
    return advance_canonical_bucket(
        initial_policy_state(side),
        decision_ts_ns=BUCKET_START_NS + 123_000_000,
        toxicity_triggered=True,
        curve=curve or _curve(side=side),
    )


def test_selects_minimum_confirmed_tick_for_one_point_reach_budget() -> None:
    result = _activate()

    assert result.p3_query_performed is True
    assert result.evaluation_code == BucketEvaluationCode.ACTIVATED
    assert result.state.active is True
    assert result.state.penalty_ticks == 4
    assert result.state.baseline_reach_probability == pytest.approx(0.20)
    assert result.state.candidate_reach_probability == pytest.approx(0.188)


def test_unconfirmed_earlier_candidate_is_skipped() -> None:
    probabilities = tuple(0.20 - 0.003 * index for index in range(17))
    statuses = [SimultaneousBandStatus.CONFIRMED] * 17
    statuses[4] = SimultaneousBandStatus.NOT_CONFIRMED

    result = _activate(
        curve=_curve(probabilities=probabilities, statuses=tuple(statuses))
    )

    assert result.state.penalty_ticks == 5
    assert result.state.candidate_reach_probability == pytest.approx(0.185)


@pytest.mark.parametrize(
    ("curve", "expected_code"),
    [
        (None, BucketEvaluationCode.P3_UNSUPPORTED),
        (_curve(support_valid=False), BucketEvaluationCode.P3_UNSUPPORTED),
        (
            _curve(probabilities=tuple(0.20 - 0.0001 * i for i in range(17))),
            BucketEvaluationCode.REACH_BUDGET_NOT_CONFIRMED,
        ),
    ],
)
def test_unsupported_or_insufficient_curve_falls_back_to_baseline(
    curve: ConditionalReachCurve | None,
    expected_code: BucketEvaluationCode,
) -> None:
    result = advance_canonical_bucket(
        initial_policy_state(QuoteSide.BUY),
        decision_ts_ns=BUCKET_START_NS,
        toxicity_triggered=True,
        curve=curve,
    )

    assert result.state.active is False
    assert result.state.penalty_ticks == 0
    assert result.evaluation_code == expected_code


class _ExplodingSequence(Sequence[float]):
    def __getitem__(self, index):
        raise AssertionError("P3 curve was queried again inside the same bucket")

    def __len__(self) -> int:
        raise AssertionError("P3 curve was queried again inside the same bucket")


def test_same_bucket_reuses_frozen_k_without_requerying_p3() -> None:
    first = _activate()
    exploding = ConditionalReachCurve(
        side=QuoteSide.BUY,
        bucket_start_ns=BUCKET_START_NS,
        baseline_distance_ticks=99,
        reach_probabilities=_ExplodingSequence(),
        simultaneous_band_statuses=_ExplodingSequence(),
    )

    duplicate = advance_canonical_bucket(
        first.state,
        decision_ts_ns=BUCKET_START_NS + 9_999_999_999,
        toxicity_triggered=True,
        curve=exploding,
    )

    assert duplicate.p3_query_performed is False
    assert duplicate.evaluation_code == BucketEvaluationCode.DUPLICATE_BUCKET
    assert duplicate.state is first.state
    assert duplicate.state.penalty_ticks == 4


def test_independent_toxicity_trigger_prevents_query_for_complete_bucket() -> None:
    exploding = ConditionalReachCurve(
        side=QuoteSide.BUY,
        bucket_start_ns=BUCKET_START_NS,
        baseline_distance_ticks=5,
        reach_probabilities=_ExplodingSequence(),
        simultaneous_band_statuses=_ExplodingSequence(),
    )
    state = initial_policy_state(QuoteSide.BUY)

    no_trigger = advance_canonical_bucket(
        state,
        decision_ts_ns=BUCKET_START_NS,
        toxicity_triggered=False,
        curve=exploding,
    )
    later_trigger = advance_canonical_bucket(
        no_trigger.state,
        decision_ts_ns=BUCKET_START_NS + 5_000_000_000,
        toxicity_triggered=True,
        curve=exploding,
    )

    assert no_trigger.p3_query_performed is False
    assert no_trigger.state.active is False
    assert later_trigger.evaluation_code == BucketEvaluationCode.DUPLICATE_BUCKET
    assert later_trigger.p3_query_performed is False


@pytest.mark.parametrize(
    "operation",
    [QuoteOperation.PLACE, QuoteOperation.KEEP, QuoteOperation.REPLACE],
)
def test_frozen_buy_penalty_applies_to_all_exposure_quote_operations(
    operation: QuoteOperation,
) -> None:
    state = _activate().state

    decision = apply_to_quote(
        state,
        decision_ts_ns=BUCKET_START_NS + 7_000_000_000,
        side=QuoteSide.BUY,
        operation=operation,
        exposure_increasing=True,
        baseline_price_tick=650_000,
    )

    assert decision.applied is True
    assert decision.penalty_ticks == 4
    assert decision.effective_price_tick == 649_996


def test_sell_moves_outward_while_opposite_and_reducing_quotes_are_unchanged() -> None:
    sell_state = _activate(
        side=QuoteSide.SELL,
        curve=_curve(side=QuoteSide.SELL),
    ).state

    sell = apply_to_quote(
        sell_state,
        decision_ts_ns=BUCKET_START_NS + 1,
        side=QuoteSide.SELL,
        operation=QuoteOperation.PLACE,
        exposure_increasing=True,
        baseline_price_tick=650_100,
    )
    opposite = apply_to_quote(
        sell_state,
        decision_ts_ns=BUCKET_START_NS + 1,
        side=QuoteSide.BUY,
        operation=QuoteOperation.REPLACE,
        exposure_increasing=True,
        baseline_price_tick=650_000,
    )
    reducing = apply_to_quote(
        sell_state,
        decision_ts_ns=BUCKET_START_NS + 1,
        side=QuoteSide.SELL,
        operation=QuoteOperation.KEEP,
        exposure_increasing=False,
        baseline_price_tick=650_100,
    )

    assert sell.effective_price_tick == 650_104
    assert opposite.effective_price_tick == 650_000
    assert opposite.decision_code == QuoteDecisionCode.SIDE_MISMATCH
    assert reducing.effective_price_tick == 650_100
    assert reducing.decision_code == QuoteDecisionCode.REDUCING_UNCHANGED


def test_flat_ends_episode_without_reopening_same_bucket() -> None:
    active = _activate().state
    ended = end_episode_on_flat(
        active,
        decision_ts_ns=BUCKET_START_NS + 2_000_000_000,
    )

    assert ended.active is False
    assert ended.penalty_ticks == 0
    assert ended.evaluation_code == BucketEvaluationCode.FLAT_ENDED

    duplicate = advance_canonical_bucket(
        ended,
        decision_ts_ns=BUCKET_START_NS + 3_000_000_000,
        toxicity_triggered=True,
        curve=ConditionalReachCurve(
            side=QuoteSide.BUY,
            bucket_start_ns=BUCKET_START_NS,
            baseline_distance_ticks=5,
            reach_probabilities=_ExplodingSequence(),
            simultaneous_band_statuses=_ExplodingSequence(),
        ),
    )
    assert duplicate.evaluation_code == BucketEvaluationCode.DUPLICATE_BUCKET
    assert duplicate.p3_query_performed is False

    next_bucket = advance_canonical_bucket(
        duplicate.state,
        decision_ts_ns=BUCKET_START_NS + CANONICAL_BUCKET_NS,
        toxicity_triggered=True,
        curve=_curve(bucket_start_ns=BUCKET_START_NS + CANONICAL_BUCKET_NS),
    )
    assert next_bucket.state.active is True


def test_hard_safety_bypasses_policy_without_mutating_episode() -> None:
    state = _activate().state

    blocked = apply_to_quote(
        state,
        decision_ts_ns=BUCKET_START_NS + 1,
        side=QuoteSide.BUY,
        operation=QuoteOperation.REPLACE,
        exposure_increasing=True,
        baseline_price_tick=650_000,
        hard_safety_override=True,
    )
    resumed = apply_to_quote(
        state,
        decision_ts_ns=BUCKET_START_NS + 2,
        side=QuoteSide.BUY,
        operation=QuoteOperation.REPLACE,
        exposure_increasing=True,
        baseline_price_tick=650_000,
    )

    assert blocked.effective_price_tick == 650_000
    assert blocked.decision_code == QuoteDecisionCode.HARD_SAFETY_OVERRIDE
    assert state.active is True
    assert state.penalty_ticks == 4
    assert resumed.effective_price_tick == 649_996


def test_invalid_or_wrong_identity_curve_fails_closed() -> None:
    non_monotone = list(0.20 - 0.003 * index for index in range(17))
    non_monotone[8] = non_monotone[7] + 0.01

    for curve in (
        _curve(side=QuoteSide.SELL),
        _curve(bucket_start_ns=BUCKET_START_NS + CANONICAL_BUCKET_NS),
        _curve(probabilities=tuple(non_monotone)),
    ):
        result = advance_canonical_bucket(
            initial_policy_state(QuoteSide.BUY),
            decision_ts_ns=BUCKET_START_NS,
            toxicity_triggered=True,
            curve=curve,
        )
        assert result.state.active is False
        assert result.evaluation_code == BucketEvaluationCode.INVALID_CURVE


def test_canonical_bucket_boundary_and_clock_regression_are_explicit() -> None:
    assert canonical_bucket_start_ns(BUCKET_START_NS + CANONICAL_BUCKET_NS - 1) == (
        BUCKET_START_NS
    )
    assert canonical_bucket_start_ns(BUCKET_START_NS + CANONICAL_BUCKET_NS) == (
        BUCKET_START_NS + CANONICAL_BUCKET_NS
    )

    active = _activate().state
    with pytest.raises(ValueError, match="clock regressed"):
        advance_canonical_bucket(
            active,
            decision_ts_ns=BUCKET_START_NS - 1,
            toxicity_triggered=True,
            curve=_curve(bucket_start_ns=BUCKET_START_NS - CANONICAL_BUCKET_NS),
        )
