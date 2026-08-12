from __future__ import annotations

from datetime import datetime, timezone

from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    deterministic_campaign_side_assignment,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    FROZEN_RANDOM_SEEDS,
    BaselineShadowSnapshot,
    CanonicalPredictionBucket,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v2 import (
    RankedToxicityGuardFullPathAdapterV2,
    stable_assignment_episode_id,
    stable_assignment_episode_integer,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v1_5 import (
    RankedToxicityBaselineShadowCaptureV15,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v2 import (
    RankedToxicityGuardAuthoritativeReplayV2,
)

DAY = "2026-08-03"
MODEL_SHA256 = "a" * 64
THRESHOLD_SHA256 = "b" * 64


def _day_start_ms() -> int:
    return int(
        datetime.fromisoformat(DAY)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


def _prediction(ordinal: int, score: float) -> CanonicalPredictionBucket:
    bucket = _day_start_ms() + int(ordinal) * 10_000
    return CanonicalPredictionBucket(
        utc_day=DAY,
        prediction_bucket_ts_ms=bucket,
        feature_ready_ts_ms=bucket + 100,
        decision_ts_ms=bucket + 200,
        score=score,
        model_sha256=MODEL_SHA256,
    )


def _shadow(
    decision_id: str,
    decision_ts_ns: int,
    *,
    active_order_id: str = "",
) -> BaselineShadowSnapshot:
    return BaselineShadowSnapshot(
        decision_id=decision_id,
        utc_day=DAY,
        decision_ts_ns=decision_ts_ns,
        side="BUY",
        role="opener",
        baseline_eligible=True,
        exposure_increasing=True,
        can_post=True,
        allow_exposure_increase=True,
        active_exposure_order_id=active_order_id,
        quote_price=100.0,
        quote_quantity=0.001,
        blocker_fingerprint="none",
        policy_fingerprint="baseline-v5",
    )


def _adapter() -> RankedToxicityGuardFullPathAdapterV2:
    adapter = RankedToxicityGuardFullPathAdapterV2(
        side="BUY",
        random_seed=FROZEN_RANDOM_SEEDS["BUY"],
        frozen_model_sha256=MODEL_SHA256,
    )
    adapter.register_daily_threshold(
        utc_day=DAY,
        threshold=0.8,
        source_identity_sha256=THRESHOLD_SHA256,
    )
    return adapter


def _decision_for_action(
    action: str,
    *,
    lineage_id: str,
    decision_ts_ns: int,
) -> str:
    for ordinal in range(1, 20_000):
        decision_id = f"decision-{action}-{ordinal}"
        episode_id = stable_assignment_episode_id(
            side="BUY",
            initial_untreated_lineage_id=lineage_id,
            first_opportunity_decision_id=decision_id,
            assignment_ts_ns=decision_ts_ns,
        )
        assignment = deterministic_campaign_side_assignment(
            seed=FROZEN_RANDOM_SEEDS["BUY"],
            utc_day=DAY,
            side="BUY",
            campaign_opportunity_id=stable_assignment_episode_integer(episode_id),
            candidate_probability=0.5,
        )
        if assignment.action == action:
            return decision_id
    raise AssertionError(f"failed to find deterministic {action} assignment")


def _observe_final(
    adapter: RankedToxicityGuardFullPathAdapterV2,
    *,
    decision_id: str,
    event_ts_ns: int,
    candidate_action: str,
    candidate_order_id: str = "",
) -> None:
    adapter.observe_final_quote_action(
        decision_id=decision_id,
        role="opener",
        exposure_increasing=True,
        baseline_action="place" if candidate_action != "keep" else "keep",
        candidate_action=candidate_action,
        baseline_price=100.0,
        candidate_price=100.0,
        baseline_quantity=0.001,
        candidate_quantity=0.001,
        event_ts_ns=event_ts_ns,
        baseline_order_id=candidate_order_id,
        candidate_order_id=candidate_order_id,
    )


def test_active_order_carries_arm_across_inventory_campaign_boundary() -> None:
    adapter = _adapter()
    prediction = _prediction(1, 0.1)
    adapter.on_prediction_bucket(prediction)
    first_ts = prediction.decision_ts_ms * 1_000_000
    first_id = _decision_for_action(
        CONTROL_ACTION,
        lineage_id="lineage-1",
        decision_ts_ns=first_ts,
    )
    first = _shadow(first_id, first_ts)
    first_directive = adapter.on_quote_decision(
        control_shadow=first,
        candidate_shadow=first,
        prospective_campaign_side_id="lineage-1",
    )
    first_episode = first_directive.prospective_campaign_side_id
    _observe_final(
        adapter,
        decision_id=first_id,
        event_ts_ns=first_ts,
        candidate_action="place",
        candidate_order_id="order-1",
    )
    adapter.on_order_submitted(
        order_id="order-1",
        initial_quantity=0.001,
        visibility_ts_ns=first_ts + 1,
        exposure_increasing=True,
    )
    adapter.on_order_activated(
        order_id="order-1",
        visibility_ts_ns=first_ts + 2,
        exchange_ts_ns=first_ts + 2,
    )

    second = _shadow(
        "lineage-2-decision",
        first_ts + 100_000_000,
        active_order_id="order-1",
    )
    second_directive = adapter.on_quote_decision(
        control_shadow=second,
        candidate_shadow=second,
        candidate_active_exposure_order_id="order-1",
        prospective_campaign_side_id="lineage-2",
    )
    assert second_directive.prospective_campaign_side_id == first_episode
    _observe_final(
        adapter,
        decision_id=second.decision_id,
        event_ts_ns=second.decision_ts_ns,
        candidate_action="keep",
        candidate_order_id="order-1",
    )
    adapter.on_order_fill(
        order_id="order-1",
        remaining_after=0.0,
        visibility_ts_ns=second.decision_ts_ns + 1,
        exchange_ts_ns=second.decision_ts_ns + 1,
        full_fill=True,
    )

    same_campaign = _shadow(
        "lineage-2-after-terminal",
        second.decision_ts_ns + 100_000_000,
    )
    same_directive = adapter.on_quote_decision(
        control_shadow=same_campaign,
        candidate_shadow=same_campaign,
        prospective_campaign_side_id="lineage-2",
    )
    assert same_directive.prospective_campaign_side_id == first_episode
    _observe_final(
        adapter,
        decision_id=same_campaign.decision_id,
        event_ts_ns=same_campaign.decision_ts_ns,
        candidate_action="place",
    )

    third = _shadow(
        "lineage-3-decision",
        same_campaign.decision_ts_ns + 100_000_000,
    )
    third_directive = adapter.on_quote_decision(
        control_shadow=third,
        candidate_shadow=third,
        prospective_campaign_side_id="lineage-3",
    )
    assert third_directive.prospective_campaign_side_id != first_episode
    _observe_final(
        adapter,
        decision_id=third.decision_id,
        event_ts_ns=third.decision_ts_ns,
        candidate_action=(
            "pause" if not third_directive.allow_exposure_submission else "place"
        ),
    )
    adapter.censor_assignment_episode(
        event_ts_ns=third.decision_ts_ns + 1,
        reason="unit_test_panel_end",
    )

    audit = adapter.assert_execution_complete()
    assert audit["assignment_count"] == 2
    assert audit["completed_assignment_episode_count"] == 1
    assert audit["censored_assignment_episode_count"] == 1
    assert audit["carryover_transition_count"] == 1
    assert audit["clean_washout_count"] == 1
    assert audit["cross_arm_order_ownership_count"] == 0
    assert audit["forced_washout_cancel_count"] == 0
    assert audit["episode_campaign_membership"][first_episode] == [
        "lineage-1",
        "lineage-2",
    ]


def test_suppressing_guard_state_also_carries_until_natural_release() -> None:
    adapter = _adapter()
    high = _prediction(1, 0.95)
    adapter.on_prediction_bucket(high)
    first_ts = high.decision_ts_ms * 1_000_000
    first_id = _decision_for_action(
        CANDIDATE_ACTION,
        lineage_id="lineage-1",
        decision_ts_ns=first_ts,
    )
    first = _shadow(first_id, first_ts)
    first_directive = adapter.on_quote_decision(
        control_shadow=first,
        candidate_shadow=first,
        prospective_campaign_side_id="lineage-1",
    )
    first_episode = first_directive.prospective_campaign_side_id
    assert first_directive.action == CANDIDATE_ACTION
    assert not first_directive.allow_exposure_submission
    _observe_final(
        adapter,
        decision_id=first_id,
        event_ts_ns=first_ts,
        candidate_action="pause",
    )

    second = _shadow("lineage-2-decision", first_ts + 100_000_000)
    second_directive = adapter.on_quote_decision(
        control_shadow=second,
        candidate_shadow=second,
        prospective_campaign_side_id="lineage-2",
    )
    assert second_directive.prospective_campaign_side_id == first_episode
    _observe_final(
        adapter,
        decision_id=second.decision_id,
        event_ts_ns=second.decision_ts_ns,
        candidate_action="pause",
    )

    low = _prediction(2, 0.1)
    adapter.on_prediction_bucket(low)
    released = _shadow(
        "lineage-2-released",
        low.decision_ts_ms * 1_000_000,
    )
    released_directive = adapter.on_quote_decision(
        control_shadow=released,
        candidate_shadow=released,
        prospective_campaign_side_id="lineage-2",
    )
    assert released_directive.allow_exposure_submission
    _observe_final(
        adapter,
        decision_id=released.decision_id,
        event_ts_ns=released.decision_ts_ns,
        candidate_action="place",
    )

    third = _shadow("lineage-3-decision", released.decision_ts_ns + 100_000_000)
    third_directive = adapter.on_quote_decision(
        control_shadow=third,
        candidate_shadow=third,
        prospective_campaign_side_id="lineage-3",
    )
    assert third_directive.prospective_campaign_side_id != first_episode
    _observe_final(
        adapter,
        decision_id=third.decision_id,
        event_ts_ns=third.decision_ts_ns,
        candidate_action=(
            "pause" if not third_directive.allow_exposure_submission else "place"
        ),
    )
    adapter.censor_assignment_episode(
        event_ts_ns=third.decision_ts_ns + 1,
        reason="unit_test_panel_end",
    )
    audit = adapter.assert_execution_complete()
    assert audit["carryover_transition_count"] == 1
    assert audit["clean_washout_count"] == 1
    assert audit["carryover_contract_valid"] is True


def test_authoritative_v2_binding_right_censors_last_episode(tmp_path) -> None:
    prediction = _prediction(1, 0.1)
    quote = {
        "decision_id": "BTCUSDC:decision-1:BUY",
        "decision_ts_ns": prediction.decision_ts_ms * 1_000_000,
        "side": "BUY",
        "role": "opener",
        "baseline_eligible": True,
        "exposure_increasing": True,
        "can_post": True,
        "allow_exposure_increase": True,
        "active_exposure_order_id": "",
        "quote_price": 100.0,
        "quote_quantity": 0.001,
        "blocker_reasons": (),
        "policy_fingerprint": "baseline-v5",
        "untreated_lineage_ordinal": 1,
    }
    baseline_dir = tmp_path / "baseline"
    capture = RankedToxicityBaselineShadowCaptureV15(
        output_dir=baseline_dir,
        lineage_namespace=f"{DAY}|panel",
        sides=("BUY",),
        chunk_rows=1,
    )
    capture.on_prediction_bucket(
        prediction_bucket_ts_ms=prediction.prediction_bucket_ts_ms,
        feature_ready_ts_ms=prediction.feature_ready_ts_ms,
        observation_ts_ms=prediction.decision_ts_ms,
        tox_bid=prediction.score,
        tox_ask=prediction.score,
    )
    capture.on_quote_decision(**quote)
    capture.on_final_quote_action(
        decision_id=quote["decision_id"],
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action="place",
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=quote["decision_ts_ns"],
    )
    capture.finish_replay(event_ts_ns=quote["decision_ts_ns"])

    candidate = RankedToxicityGuardAuthoritativeReplayV2(
        baseline_manifest_path=baseline_dir / "manifest.json",
        output_root=tmp_path / "candidate",
        frozen_model_sha256=MODEL_SHA256,
        threshold_schedule={"BUY": {DAY: (0.8, THRESHOLD_SHA256)}},
        sides=("BUY",),
        chunk_rows=1,
    )
    candidate.on_prediction_bucket(
        prediction_bucket_ts_ms=prediction.prediction_bucket_ts_ms,
        feature_ready_ts_ms=prediction.feature_ready_ts_ms,
        observation_ts_ms=prediction.decision_ts_ms,
        tox_bid=prediction.score,
        tox_ask=prediction.score,
    )
    directive = candidate.on_quote_decision(**quote)
    candidate.on_final_quote_action(
        decision_id=quote["decision_id"],
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action=(
            "place" if directive.allow_exposure_submission else "pause"
        ),
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=quote["decision_ts_ns"],
    )
    audit = candidate.finish_replay(event_ts_ns=quote["decision_ts_ns"] + 1)

    assert audit["baseline_shadow"]["complete"] is True
    assert audit["adapters"]["BUY"]["assignment_count"] == 1
    assert audit["adapters"]["BUY"]["censored_assignment_episode_count"] == 1
    assert audit["adapters"]["BUY"]["carryover_contract_valid"] is True
    assert audit["economic_outcomes_read"] is False


def test_live_reducing_order_can_become_exposure_increasing() -> None:
    adapter = _adapter()
    prediction = _prediction(1, 0.1)
    adapter.on_prediction_bucket(prediction)
    event_ts = prediction.decision_ts_ms * 1_000_000

    adapter.on_order_submitted(
        order_id="role-transition-order",
        initial_quantity=0.001,
        visibility_ts_ns=event_ts - 2,
        exposure_increasing=False,
    )
    adapter.on_order_activated(
        order_id="role-transition-order",
        visibility_ts_ns=event_ts - 1,
        exchange_ts_ns=event_ts - 1,
    )

    decision_id = _decision_for_action(
        CONTROL_ACTION,
        lineage_id="lineage-role-transition",
        decision_ts_ns=event_ts,
    )
    current = _shadow(
        decision_id,
        event_ts,
        active_order_id="role-transition-order",
    )
    directive = adapter.on_quote_decision(
        control_shadow=current,
        candidate_shadow=current,
        candidate_active_exposure_order_id="role-transition-order",
        prospective_campaign_side_id="lineage-role-transition",
    )
    assert directive.allow_exposure_submission is True
    _observe_final(
        adapter,
        decision_id=decision_id,
        event_ts_ns=event_ts,
        candidate_action="keep",
        candidate_order_id="role-transition-order",
    )
    adapter.on_order_fill(
        order_id="role-transition-order",
        remaining_after=0.0,
        visibility_ts_ns=event_ts + 1,
        exchange_ts_ns=event_ts + 1,
        full_fill=True,
    )
    adapter.censor_assignment_episode(
        event_ts_ns=event_ts + 2,
        reason="unit_test_panel_end",
    )

    audit = adapter.assert_execution_complete()
    assert audit["active_order_role_transition_to_exposure_count"] == 1
    transitions = [
        row
        for row in adapter.journal()
        if row["event_type"] == "active_order_role_transition_to_exposure"
    ]
    assert len(transitions) == 1
    assert transitions[0]["order_id"] == "role-transition-order"
    assert transitions[0]["fill_risk_active"] is True
