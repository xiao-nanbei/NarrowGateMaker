from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.sell_campaign_add_permission_cate import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    _add_outcomes,
    _dr_components,
    _promotion_gate,
)


def test_campaign_permission_dr_components_recover_known_effect() -> None:
    frame = pd.DataFrame(
        {
            "action": [CONTROL_ACTION, CANDIDATE_ACTION] * 4,
            "target_reward": [1.0, 1.4] * 4,
            f"behavior_prob_{CONTROL_ACTION}": [0.5] * 8,
            f"behavior_prob_{CANDIDATE_ACTION}": [0.5] * 8,
        }
    )

    dr0, dr1, tau = _dr_components(
        frame,
        target="reward",
        mu0=np.full(len(frame), 1.0),
        mu1=np.full(len(frame), 1.4),
    )

    assert np.allclose(dr0, 1.0)
    assert np.allclose(dr1, 1.4)
    assert np.allclose(tau, 0.4)


def test_campaign_permission_outcomes_have_higher_is_better_direction() -> None:
    frame = pd.DataFrame(
        {
            "terminal_campaign_pnl": [-3.0, 1.0],
            "reward": [-2.0, 0.5],
            "campaign_cost": [2.0, -0.5],
            "campaign_mae": [-4.0, -0.2],
            "campaign_closed": [0, 1],
            "campaign_censored": [1, 0],
            "decision_to_terminal_s": [5_000.0, 120.0],
            "intervention_fill_count": [3, 0],
        }
    )

    result = _add_outcomes(frame, development_q10=-1.0)

    assert result["target_campaign_cost_avoidance"].tolist() == [-2.0, 0.5]
    assert result["target_negative_terminal_protection"].tolist() == [-3.0, 0.0]
    assert result["target_development_q10_shortfall_protection"].tolist() == [
        -2.0,
        0.0,
    ]
    assert result["target_campaign_mae_avoidance"].tolist() == [-4.0, -0.2]
    assert result["target_restricted_time_to_repair"].tolist() == [-3_600.0, -120.0]
    assert result["target_day_end_censoring_avoidance"].tolist() == [-1.0, 0.0]
    assert result["target_intervention_add_fills"].tolist() == [3.0, 0.0]


def _passing_summary() -> dict:
    interval_positive = {"p025": 0.01}
    interval_nonnegative = {"p025": 0.0}
    return {
        "reward": {
            "interval": interval_positive.copy(),
            "daily_positive_rate": 0.60,
        },
        "campaign_cost_avoidance": {"interval": interval_nonnegative.copy()},
        "negative_terminal_protection": {"interval": interval_nonnegative.copy()},
        "development_q10_shortfall_protection": {
            "interval": interval_nonnegative.copy()
        },
        "campaign_mae_avoidance": {"uplift": 0.0},
        "repair_event": {"uplift": 0.0},
        "restricted_time_to_repair": {"uplift": 0.0},
        "day_end_censoring_avoidance": {"uplift": 0.0},
        "support": {
            "failures": [],
            "candidate_rate": 0.20,
            "policy_ess": 200.0,
            "oof_days": 20,
        },
        "activity": {"fills_retention": 0.90},
    }


def test_campaign_permission_gate_enforces_value_activity_and_tail_budget() -> None:
    passing = _passing_summary()
    passed, failures = _promotion_gate(passing)
    assert passed
    assert failures == []

    low_retention = _passing_summary()
    low_retention["activity"]["fills_retention"] = 0.849
    passed, failures = _promotion_gate(low_retention)
    assert not passed
    assert "fills_retention_below_0_85" in failures

    weak_daily_sign = _passing_summary()
    weak_daily_sign["reward"]["daily_positive_rate"] = 0.54
    passed, failures = _promotion_gate(weak_daily_sign)
    assert not passed
    assert "reward_daily_positive_rate_below_0_55" in failures

    worse_censoring = _passing_summary()
    worse_censoring["day_end_censoring_avoidance"]["uplift"] = -0.01
    passed, failures = _promotion_gate(worse_censoring)
    assert not passed
    assert "day_end_censoring_avoidance_point_estimate_negative" in failures
