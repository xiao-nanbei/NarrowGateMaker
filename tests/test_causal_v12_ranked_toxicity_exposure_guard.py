from __future__ import annotations

import math

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    GuardState,
    RankedToxicityGuardRuntime,
    build_past_only_quantile_schedule,
    collapse_eligible_prediction_buckets,
    deterministic_campaign_side_assignment,
    summarize_mechanics_journal,
)


def test_collapse_eligible_prediction_buckets_deduplicates_100ms_loop() -> None:
    rows = pd.DataFrame(
        [
            {
                "day": "2026-04-17",
                "side": "BUY",
                "decision_ts_ms": 10_100,
                "prediction_bucket_ts_ms": 10_000,
                "feature_ready_ts_ms": 10_050,
                "toxicity_score": 0.81,
                "baseline_eligible": 1,
                "exposure_increasing": 1,
                "role": "opener",
            },
            {
                "day": "2026-04-17",
                "side": "BUY",
                "decision_ts_ms": 10_200,
                "prediction_bucket_ts_ms": 10_000,
                "feature_ready_ts_ms": 10_050,
                "toxicity_score": 0.81,
                "baseline_eligible": 1,
                "exposure_increasing": 1,
                "role": "opener",
            },
            {
                "day": "2026-04-17",
                "side": "SELL",
                "decision_ts_ms": 10_100,
                "prediction_bucket_ts_ms": 10_000,
                "feature_ready_ts_ms": 10_050,
                "toxicity_score": 0.72,
                "baseline_eligible": 1,
                "exposure_increasing": 1,
                "role": "opener",
            },
        ]
    )

    collapsed = collapse_eligible_prediction_buckets(rows, side="BUY")

    assert len(collapsed) == 1
    assert collapsed.iloc[0]["decision_ts_ms"] == 10_100
    assert collapsed.iloc[0]["toxicity_score"] == pytest.approx(0.81)


def test_collapse_fails_when_sample_and_hold_score_changes_in_one_bucket() -> None:
    rows = pd.DataFrame(
        [
            {
                "day": "2026-04-17",
                "side": "BUY",
                "decision_ts_ms": 10_100,
                "prediction_bucket_ts_ms": 10_000,
                "feature_ready_ts_ms": 10_050,
                "toxicity_score": 0.81,
                "baseline_eligible": 1,
                "exposure_increasing": 1,
                "role": "opener",
            },
            {
                "day": "2026-04-17",
                "side": "BUY",
                "decision_ts_ms": 10_200,
                "prediction_bucket_ts_ms": 10_000,
                "feature_ready_ts_ms": 10_050,
                "toxicity_score": 0.82,
                "baseline_eligible": 1,
                "exposure_increasing": 1,
                "role": "opener",
            },
        ]
    )

    with pytest.raises(ValueError, match="multiple toxicity scores"):
        collapse_eligible_prediction_buckets(rows, side="BUY")


def test_past_only_schedule_never_uses_current_day() -> None:
    opportunities = pd.DataFrame(
        {
            "day": ["2026-04-17"] * 2 + ["2026-04-18"] * 2 + ["2026-04-19"],
            "toxicity_score": [0.10, 0.20, 0.30, 0.40, 0.99],
        }
    )
    schedule = build_past_only_quantile_schedule(
        opportunities,
        quantile=0.90,
        minimum_prior_days=2,
        minimum_prior_buckets=4,
    )

    first = schedule.set_index("day").loc["2026-04-19"]
    assert bool(first["threshold_ready"])
    assert first["threshold"] == pytest.approx(0.40)
    assert first["latest_history_day"] == "2026-04-18"

    changed = opportunities.copy()
    changed.loc[changed["day"] == "2026-04-19", "toxicity_score"] = 0.01
    changed_schedule = build_past_only_quantile_schedule(
        changed,
        quantile=0.90,
        minimum_prior_days=2,
        minimum_prior_buckets=4,
    )
    changed_value = changed_schedule.set_index("day").loc["2026-04-19", "threshold"]
    assert changed_value == pytest.approx(first["threshold"])


def test_guard_cancel_ack_release_and_reactivation_contract() -> None:
    runtime = RankedToxicityGuardRuntime(
        side="BUY",
        action=CANDIDATE_ACTION,
        threshold=0.80,
    )
    crossing = runtime.on_completed_prediction(
        prediction_bucket_ts_ms=10_000,
        score=0.90,
        baseline_eligible=True,
        exposure_increasing=True,
        active_exposure_order=True,
    )
    assert crossing.request_cancel_once
    assert runtime.state == GuardState.CANCEL_PENDING
    assert not runtime.permission(exposure_increasing=True)
    assert runtime.permission(exposure_increasing=False)

    duplicate = runtime.on_completed_prediction(
        prediction_bucket_ts_ms=10_000,
        score=0.90,
        baseline_eligible=True,
        exposure_increasing=True,
        active_exposure_order=True,
    )
    assert duplicate.duplicate_bucket
    assert not duplicate.request_cancel_once
    assert runtime.cancel_request_count == 1

    below_while_pending = runtime.on_completed_prediction(
        prediction_bucket_ts_ms=20_000,
        score=0.70,
        baseline_eligible=True,
        exposure_increasing=True,
        active_exposure_order=False,
    )
    assert below_while_pending.release_waiting_for_cancel_ack
    assert runtime.state == GuardState.CANCEL_PENDING
    assert runtime.on_cancel_ack() == GuardState.RELEASED
    assert runtime.permission(exposure_increasing=True)

    second_crossing = runtime.on_completed_prediction(
        prediction_bucket_ts_ms=30_000,
        score=0.85,
        baseline_eligible=True,
        exposure_increasing=True,
        active_exposure_order=False,
    )
    assert second_crossing.activated
    assert not second_crossing.request_cancel_once
    assert runtime.state == GuardState.SUPPRESSING
    assert runtime.guard_episode_count == 2


def test_control_never_suppresses_and_assignment_is_stable() -> None:
    first = deterministic_campaign_side_assignment(
        seed=20260802,
        utc_day="2026-04-17",
        side="SELL",
        campaign_opportunity_id=17,
    )
    second = deterministic_campaign_side_assignment(
        seed=20260802,
        utc_day="2026-04-17",
        side="SELL",
        campaign_opportunity_id=17,
    )
    assert first == second
    assert first.behavior_propensity == pytest.approx(0.5)

    runtime = RankedToxicityGuardRuntime(
        side="SELL",
        action=CONTROL_ACTION,
        threshold=0.80,
    )
    transition = runtime.on_completed_prediction(
        prediction_bucket_ts_ms=10_000,
        score=0.99,
        baseline_eligible=True,
        exposure_increasing=True,
        active_exposure_order=True,
    )
    assert not transition.activated
    assert runtime.state == GuardState.BASELINE
    assert runtime.permission(exposure_increasing=True)


def test_mechanics_summary_keeps_four_denominators_separate() -> None:
    journal = pd.DataFrame(
        [
            {
                "day": "2026-04-17",
                "action": CONTROL_ACTION,
                "prediction_bucket_observed": 1,
                "prediction_bucket_exceeded": 0,
                "eligible_decision": 1,
                "eligible_decision_exceeded": 0,
                "campaign_assigned": 1,
                "campaign_activated": 0,
                "final_quote_action_changed": 0,
                "behavior_propensity": 0.5,
            },
            {
                "day": "2026-04-17",
                "action": CANDIDATE_ACTION,
                "prediction_bucket_observed": 1,
                "prediction_bucket_exceeded": 1,
                "eligible_decision": 1,
                "eligible_decision_exceeded": 1,
                "campaign_assigned": 1,
                "campaign_activated": 1,
                "final_quote_action_changed": 1,
                "behavior_propensity": 0.5,
            },
            {
                "day": "2026-04-17",
                "action": CANDIDATE_ACTION,
                "prediction_bucket_observed": 0,
                "prediction_bucket_exceeded": 0,
                "eligible_decision": 0,
                "eligible_decision_exceeded": 0,
                "campaign_assigned": 0,
                "campaign_activated": 0,
                "final_quote_action_changed": 0,
                "behavior_propensity": 0.5,
            },
        ]
    )

    result = summarize_mechanics_journal(journal)

    assert result["prediction_bucket_exceedance_rate"] == pytest.approx(0.5)
    assert result["eligible_decision_exceedance_rate"] == pytest.approx(0.5)
    assert result["campaign_activation_rate"] == pytest.approx(0.5)
    assert result["final_quote_action_change_rate"] == pytest.approx(0.5)
    assert result["effective_sample_size"] == pytest.approx(2.0)
    assert result["economic_outcome_columns_read"] == []


def test_ready_clock_must_be_causal() -> None:
    frame = pd.DataFrame(
        [
            {
                "day": "2026-04-17",
                "side": "BUY",
                "decision_ts_ms": 10_100,
                "prediction_bucket_ts_ms": 10_000,
                "feature_ready_ts_ms": 10_200,
                "toxicity_score": 0.8,
                "baseline_eligible": 1,
                "exposure_increasing": 1,
                "role": "opener",
            }
        ]
    )
    with pytest.raises(ValueError, match="exceeds decision"):
        collapse_eligible_prediction_buckets(frame, side="BUY")


def test_unready_schedule_uses_nan_not_current_day_fallback() -> None:
    schedule = build_past_only_quantile_schedule(
        pd.DataFrame(
            {"day": ["2026-04-17"], "toxicity_score": [0.9]}
        ),
        minimum_prior_days=1,
        minimum_prior_buckets=1,
    )
    assert not bool(schedule.iloc[0]["threshold_ready"])
    assert math.isnan(float(schedule.iloc[0]["threshold"]))
