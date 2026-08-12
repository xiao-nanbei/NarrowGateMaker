from __future__ import annotations

from datetime import datetime, timezone

import pytest

from execution.order_lifecycle import OrderLifecyclePhase
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    CANDIDATE_ACTION,
    GuardState,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    FROZEN_RANDOM_SEEDS,
    AdapterContractViolation,
    BaselineShadowSnapshot,
    CanonicalPredictionBucket,
    RankedToxicityGuardFullPathAdapterV11,
    lifecycle_branch_contract,
)

MODEL_SHA256 = "a" * 64
THRESHOLD_SHA256 = "b" * 64
DAY = "2026-08-03"
NEXT_DAY = "2026-08-04"


def _day_start_ms(day: str) -> int:
    return int(
        datetime.fromisoformat(day)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


def _prediction(
    day: str,
    offset_s: int,
    score: float,
    *,
    ready_delay_ms: int = 100,
    decision_delay_ms: int = 200,
    model_sha256: str = MODEL_SHA256,
) -> CanonicalPredictionBucket:
    bucket = _day_start_ms(day) + int(offset_s) * 1000
    return CanonicalPredictionBucket(
        utc_day=day,
        prediction_bucket_ts_ms=bucket,
        feature_ready_ts_ms=bucket + ready_delay_ms,
        decision_ts_ms=bucket + decision_delay_ms,
        score=score,
        model_sha256=model_sha256,
    )


def _shadow(
    prediction: CanonicalPredictionBucket,
    *,
    decision_id: str,
    side: str = "BUY",
    role: str = "opener",
    eligible: bool = True,
    exposure_increasing: bool = True,
    active_order_id: str = "",
    blocker: str = "none",
) -> BaselineShadowSnapshot:
    return BaselineShadowSnapshot(
        decision_id=decision_id,
        utc_day=prediction.utc_day,
        decision_ts_ns=prediction.decision_ts_ms * 1_000_000,
        side=side,
        role=role,
        baseline_eligible=eligible,
        exposure_increasing=exposure_increasing,
        can_post=eligible,
        allow_exposure_increase=eligible and exposure_increasing,
        active_exposure_order_id=active_order_id,
        quote_price=100.0,
        quote_quantity=0.001,
        blocker_fingerprint=blocker,
        policy_fingerprint="baseline-v5",
    )


def _candidate_adapter(*, side: str = "BUY") -> RankedToxicityGuardFullPathAdapterV11:
    return RankedToxicityGuardFullPathAdapterV11(
        side=side,
        random_seed=FROZEN_RANDOM_SEEDS[side],
        frozen_model_sha256=MODEL_SHA256,
    )


def _register_thresholds(
    adapter: RankedToxicityGuardFullPathAdapterV11,
    *days: str,
) -> None:
    for day in days:
        adapter.register_daily_threshold(
            utc_day=day,
            threshold=0.80 if day == DAY else 0.85,
            source_identity_sha256=THRESHOLD_SHA256,
        )


def _active_order(
    adapter: RankedToxicityGuardFullPathAdapterV11,
    *,
    order_id: str = "buy-1",
) -> None:
    start = _day_start_ms(DAY) * 1_000_000
    adapter.on_order_submitted(
        order_id=order_id,
        initial_quantity=0.001,
        visibility_ts_ns=start + 1_000_000,
        exposure_increasing=True,
    )
    adapter.on_order_activated(
        order_id=order_id,
        visibility_ts_ns=start + 2_000_000,
        exchange_ts_ns=start + 1_500_000,
    )


def test_assignment_precedes_treatment_and_survives_utc_threshold_update() -> None:
    adapter = _candidate_adapter()
    _register_thresholds(adapter, DAY, NEXT_DAY)

    first = _prediction(DAY, 10, 0.90)
    shadow = _shadow(first, decision_id="d-1")
    directive = adapter.observe_prediction_decision(
        prediction=first,
        control_shadow=shadow,
        candidate_shadow=shadow,
        prospective_campaign_side_id="prospective-buy-1",
    )
    assert directive.action == CANDIDATE_ACTION
    assert not directive.allow_exposure_submission
    adapter.observe_final_quote_action(
        decision_id="d-1",
        role="opener",
        exposure_increasing=True,
        baseline_action="place",
        candidate_action="pause",
        baseline_price=100.0,
        candidate_price=0.0,
        baseline_quantity=0.001,
        candidate_quantity=0.0,
        event_ts_ns=first.decision_ts_ms * 1_000_000,
    )
    assert adapter.current_assignment is not None
    assignment = adapter.current_assignment
    assert assignment.assignment_utc_day == DAY
    assert adapter.journal()[0]["event_type"] == "prospective_campaign_side_assignment"

    recovery = _prediction(DAY, 20, 0.70)
    recovery_shadow = _shadow(
        recovery,
        decision_id="d-2",
        eligible=False,
    )
    recovery_directive = adapter.observe_prediction_decision(
        prediction=recovery,
        control_shadow=recovery_shadow,
        candidate_shadow=recovery_shadow,
        prospective_campaign_side_id="prospective-buy-1",
    )
    assert adapter.state == GuardState.RELEASED
    assert recovery_directive.allow_exposure_submission
    adapter.observe_final_quote_action(
        decision_id="d-2",
        role="opener",
        exposure_increasing=True,
        baseline_action="place",
        candidate_action="place",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.001,
        candidate_quantity=0.001,
        event_ts_ns=recovery.decision_ts_ms * 1_000_000,
    )

    next_day = _prediction(NEXT_DAY, 10, 0.90)
    next_shadow = _shadow(next_day, decision_id="d-3")
    next_directive = adapter.observe_prediction_decision(
        prediction=next_day,
        control_shadow=next_shadow,
        candidate_shadow=next_shadow,
        prospective_campaign_side_id="prospective-buy-1",
    )
    assert next_directive.threshold == pytest.approx(0.85)
    assert adapter.current_assignment == assignment
    assert adapter.current_assignment.assignment_utc_day == DAY
    assert adapter.current_assignment.assignment_sequence == 1
    adapter.observe_final_quote_action(
        decision_id="d-3",
        role="opener",
        exposure_increasing=True,
        baseline_action="place",
        candidate_action="pause",
        baseline_price=100.0,
        candidate_price=0.0,
        baseline_quantity=0.001,
        candidate_quantity=0.0,
        event_ts_ns=next_day.decision_ts_ms * 1_000_000,
    )
    adapter.end_prospective_campaign_side(
        prospective_campaign_side_id="prospective-buy-1",
        event_ts_ns=next_day.decision_ts_ms * 1_000_000 + 1,
        reason="baseline_shadow_campaign_terminal_without_candidate_fill",
    )
    complete = adapter.assert_execution_complete()
    assert complete["zero_tolerance_passed"]
    assert complete["execution_complete"]


def test_cancel_pending_partial_fill_then_terminal_release_clears_old_risk() -> None:
    adapter = _candidate_adapter()
    _register_thresholds(adapter, DAY)
    _active_order(adapter)

    crossing = _prediction(DAY, 10, 0.90)
    crossing_shadow = _shadow(
        crossing,
        decision_id="d-1",
        active_order_id="buy-1",
    )
    directive = adapter.observe_prediction_decision(
        prediction=crossing,
        control_shadow=crossing_shadow,
        candidate_shadow=crossing_shadow,
        candidate_active_exposure_order_id="buy-1",
        prospective_campaign_side_id="prospective-buy-1",
    )
    assert directive.request_cancel_once
    adapter.observe_final_quote_action(
        decision_id="d-1",
        role="opener",
        exposure_increasing=True,
        baseline_action="keep",
        candidate_action="cancel",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.001,
        candidate_quantity=0.001,
        event_ts_ns=crossing.decision_ts_ms * 1_000_000,
        baseline_order_id="buy-1",
        candidate_order_id="buy-1",
    )
    assert adapter.order_snapshot("buy-1")["phase"] == (
        OrderLifecyclePhase.CANCEL_PENDING.value
    )

    pending_fill_ns = crossing.decision_ts_ms * 1_000_000 + 10_000_000
    adapter.on_order_fill(
        order_id="buy-1",
        remaining_after=0.0004,
        visibility_ts_ns=pending_fill_ns,
        exchange_ts_ns=pending_fill_ns - 1_000_000,
    )
    pending = adapter.order_snapshot("buy-1")
    assert pending["phase"] == OrderLifecyclePhase.CANCEL_PENDING.value
    assert pending["remaining_quantity"] == pytest.approx(0.0004)
    assert pending["fill_risk_active"] is True

    recovery = _prediction(DAY, 20, 0.70)
    recovery_shadow = _shadow(recovery, decision_id="d-2", eligible=False)
    recovery_directive = adapter.observe_prediction_decision(
        prediction=recovery,
        control_shadow=recovery_shadow,
        candidate_shadow=recovery_shadow,
        prospective_campaign_side_id="prospective-buy-1",
    )
    assert adapter.state == GuardState.CANCEL_PENDING
    assert not recovery_directive.allow_exposure_submission
    adapter.observe_final_quote_action(
        decision_id="d-2",
        role="opener",
        exposure_increasing=True,
        baseline_action="keep",
        candidate_action="cancel_pending",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.0004,
        candidate_quantity=0.0004,
        event_ts_ns=recovery.decision_ts_ms * 1_000_000,
        baseline_order_id="buy-1",
        candidate_order_id="buy-1",
    )

    terminal_ns = recovery.decision_ts_ms * 1_000_000 + 10_000_000
    adapter.on_order_fill(
        order_id="buy-1",
        remaining_after=0.0,
        visibility_ts_ns=terminal_ns,
        exchange_ts_ns=terminal_ns - 1_000_000,
        full_fill=True,
    )
    assert adapter.state == GuardState.RELEASED
    terminal = adapter.order_snapshot("buy-1")
    assert terminal["phase"] == OrderLifecyclePhase.EXCHANGE_TERMINAL.value
    assert terminal["fill_risk_active"] is False
    assert terminal["hazard_attached"] is False
    assert terminal["cursor_attached"] is False

    with pytest.raises(AdapterContractViolation, match="hazard evaluated"):
        adapter.observe_active_order_hazard(
            order_id="buy-1",
            event_ts_ns=terminal_ns + 1,
        )
    assert adapter.contract_audit()["zero_tolerance_counts"][
        "post_terminal_hazard_or_cursor_reuse"
    ] == 1


def test_cancel_reject_restores_partial_risk_without_terminalizing_guard() -> None:
    adapter = _candidate_adapter()
    _register_thresholds(adapter, DAY)
    _active_order(adapter)

    crossing = _prediction(DAY, 10, 0.90)
    shadow = _shadow(crossing, decision_id="d-1", active_order_id="buy-1")
    first_directive = adapter.observe_prediction_decision(
        prediction=crossing,
        control_shadow=shadow,
        candidate_shadow=shadow,
        candidate_active_exposure_order_id="buy-1",
        prospective_campaign_side_id="prospective-buy-1",
    )
    assert first_directive.request_cancel_once
    adapter.observe_final_quote_action(
        decision_id="d-1",
        role="opener",
        exposure_increasing=True,
        baseline_action="keep",
        candidate_action="cancel",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.001,
        candidate_quantity=0.001,
        event_ts_ns=crossing.decision_ts_ms * 1_000_000,
        baseline_order_id="buy-1",
        candidate_order_id="buy-1",
    )
    fill_ns = crossing.decision_ts_ms * 1_000_000 + 1_000_000
    adapter.on_order_fill(
        order_id="buy-1",
        remaining_after=0.0004,
        visibility_ts_ns=fill_ns,
        exchange_ts_ns=fill_ns - 100_000,
    )
    assert adapter.on_cancel_rejected(
        order_id="buy-1",
        visibility_ts_ns=fill_ns + 1_000_000,
        exchange_ts_ns=fill_ns + 900_000,
    ) == GuardState.GUARD_ACTIVE
    snapshot = adapter.order_snapshot("buy-1")
    assert snapshot["phase"] == OrderLifecyclePhase.PARTIALLY_FILLED.value
    assert snapshot["fill_risk_active"] is True
    assert snapshot["hazard_attached"] is True
    assert snapshot["cursor_attached"] is True

    high_again = _prediction(DAY, 20, 0.90)
    high_shadow = _shadow(
        high_again,
        decision_id="d-2",
        active_order_id="buy-1",
    )
    keep_directive = adapter.observe_prediction_decision(
        prediction=high_again,
        control_shadow=high_shadow,
        candidate_shadow=high_shadow,
        candidate_active_exposure_order_id="buy-1",
        prospective_campaign_side_id="prospective-buy-1",
    )
    assert not keep_directive.allow_exposure_submission
    adapter.observe_final_quote_action(
        decision_id="d-2",
        role="opener",
        exposure_increasing=True,
        baseline_action="keep",
        candidate_action="keep_after_cancel_reject",
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.0004,
        candidate_quantity=0.0004,
        event_ts_ns=high_again.decision_ts_ms * 1_000_000,
        baseline_order_id="buy-1",
        candidate_order_id="buy-1",
    )
    adapter.observe_active_order_hazard(
        order_id="buy-1",
        event_ts_ns=fill_ns + 2_000_000,
    )
    adapter.observe_active_depth_cursor(
        order_id="buy-1",
        event_ts_ns=fill_ns + 2_000_001,
    )
    assert adapter.assert_zero_tolerance()["zero_tolerance_passed"]


@pytest.mark.parametrize("reason", ["cancel_ack", "expired", "rejected", "shutdown"])
def test_all_exchange_terminal_branches_clear_old_order_and_suppress_when_high(
    reason: str,
) -> None:
    adapter = _candidate_adapter()
    _register_thresholds(adapter, DAY)
    _active_order(adapter)
    crossing = _prediction(DAY, 10, 0.90)
    shadow = _shadow(crossing, decision_id="d-1", active_order_id="buy-1")
    adapter.observe_prediction_decision(
        prediction=crossing,
        control_shadow=shadow,
        candidate_shadow=shadow,
        candidate_active_exposure_order_id="buy-1",
        prospective_campaign_side_id="prospective-buy-1",
    )
    event_ns = crossing.decision_ts_ms * 1_000_000 + 1_000_000
    assert adapter.on_exchange_terminal(
        order_id="buy-1",
        reason=reason,
        visibility_ts_ns=event_ns,
        exchange_ts_ns=event_ns - 100_000,
    ) == GuardState.SUPPRESSING
    snapshot = adapter.order_snapshot("buy-1")
    assert snapshot["fill_risk_active"] is False
    assert snapshot["hazard_attached"] is False
    assert snapshot["cursor_attached"] is False
    assert adapter.assert_zero_tolerance()["zero_tolerance_passed"]


def test_duplicate_bucket_and_shadow_mismatch_fail_closed() -> None:
    adapter = _candidate_adapter()
    _register_thresholds(adapter, DAY)
    first = _prediction(DAY, 10, 0.90)
    shadow = _shadow(first, decision_id="d-1")
    adapter.observe_prediction_decision(
        prediction=first,
        control_shadow=shadow,
        candidate_shadow=shadow,
        prospective_campaign_side_id="prospective-buy-1",
    )
    with pytest.raises(AdapterContractViolation, match="duplicate canonical"):
        adapter.observe_prediction_decision(
            prediction=first,
            control_shadow=shadow,
            candidate_shadow=shadow,
            prospective_campaign_side_id="prospective-buy-1",
        )

    second_adapter = _candidate_adapter()
    _register_thresholds(second_adapter, DAY)
    changed_shadow = _shadow(first, decision_id="d-1", blocker="fill_cd")
    with pytest.raises(AdapterContractViolation, match="baseline-shadow snapshots differ"):
        second_adapter.observe_prediction_decision(
            prediction=first,
            control_shadow=shadow,
            candidate_shadow=changed_shadow,
        )


def test_reducing_quote_and_campaign_rerandomization_are_zero_tolerance() -> None:
    adapter = _candidate_adapter()
    _register_thresholds(adapter, DAY)
    first = _prediction(DAY, 10, 0.90)
    shadow = _shadow(first, decision_id="d-1")
    adapter.observe_prediction_decision(
        prediction=first,
        control_shadow=shadow,
        candidate_shadow=shadow,
        prospective_campaign_side_id="prospective-buy-1",
    )

    reducing = _prediction(DAY, 20, 0.70)
    reducing_shadow = _shadow(
        reducing,
        decision_id="d-2",
        role="reducing",
        eligible=True,
        exposure_increasing=False,
    )
    adapter.observe_prediction_decision(
        prediction=reducing,
        control_shadow=reducing_shadow,
        candidate_shadow=reducing_shadow,
        prospective_campaign_side_id="prospective-buy-1",
    )
    with pytest.raises(AdapterContractViolation, match="reducing quote"):
        adapter.observe_final_quote_action(
            decision_id="d-2",
            role="reducing",
            exposure_increasing=False,
            baseline_action="place",
            candidate_action="pause",
            baseline_price=100.0,
            candidate_price=0.0,
            baseline_quantity=0.001,
            candidate_quantity=0.0,
            event_ts_ns=reducing.decision_ts_ms * 1_000_000,
        )

    third = _prediction(DAY, 30, 0.90)
    third_shadow = _shadow(third, decision_id="d-3")
    with pytest.raises(AdapterContractViolation, match="identity changed"):
        adapter.observe_prediction_decision(
            prediction=third,
            control_shadow=third_shadow,
            candidate_shadow=third_shadow,
            prospective_campaign_side_id="prospective-buy-2",
        )


def test_lifecycle_branch_contract_names_all_required_routes() -> None:
    contract = lifecycle_branch_contract()
    assert contract["cancel_reject"]["fill_risk_set_ends"] is False
    assert contract["cancel_pending_partial_fill"]["remaining_quantity_updated"]
    assert set(contract["exchange_terminal"]["reasons"]) == {
        "cancel_ack",
        "full_fill",
        "expired",
        "rejected",
        "shutdown",
    }
    assert contract["exchange_terminal"]["recovered_while_waiting"] == "RELEASED"
    assert contract["exchange_terminal"]["not_recovered_while_waiting"] == (
        "SUPPRESSING"
    )


def test_adapter_rejects_frozen_seed_or_propensity_drift() -> None:
    with pytest.raises(ValueError, match="frozen random seed"):
        RankedToxicityGuardFullPathAdapterV11(
            side="BUY",
            random_seed=2026080202,
            frozen_model_sha256=MODEL_SHA256,
        )
    with pytest.raises(ValueError, match="frozen at 0.5"):
        RankedToxicityGuardFullPathAdapterV11(
            side="BUY",
            random_seed=FROZEN_RANDOM_SEEDS["BUY"],
            frozen_model_sha256=MODEL_SHA256,
            candidate_probability=0.6,
        )
