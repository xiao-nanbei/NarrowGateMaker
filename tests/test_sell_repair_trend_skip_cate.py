from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.sell_repair_trend_skip_cate import (
    CANDIDATE_ACTION,
    COMPETING_HORIZON_S,
    CONTROL_ACTION,
    _add_outcomes,
    _dr_tau,
    _promotion_gate,
)


def test_competing_risk_targets_preserve_event_order_and_direction() -> None:
    frame = pd.DataFrame(
        {
            "terminal_campaign_pnl": [1.0, -2.0, -0.5],
            "competing_event": ["repair", "trend_through", "censored"],
            "competing_event_time_s": [300.0, 600.0, COMPETING_HORIZON_S],
            "reward": [0.2, -0.3, -0.1],
            "campaign_cost": [-0.1, 0.4, 0.2],
            "intervention_fill_count": [0, 1, 0],
        }
    )

    result = _add_outcomes(frame, development_q10=-1.0)

    assert result["target_repair_first_30m"].tolist() == [1.0, 0.0, 0.0]
    assert result["target_trend_through_avoidance_30m"].tolist() == [
        0.0,
        -1.0,
        0.0,
    ]
    assert result.loc[0, "target_competing_risk_utility_30m"] > 0.0
    assert result.loc[1, "target_competing_risk_utility_30m"] < 0.0
    assert result.loc[2, "target_competing_risk_utility_30m"] == 0.0


def test_binary_sell_skip_dr_recovers_known_effect() -> None:
    frame = pd.DataFrame(
        {
            "action": [CONTROL_ACTION, CANDIDATE_ACTION] * 4,
            "target_reward": [1.0, 1.5] * 4,
            f"behavior_prob_{CONTROL_ACTION}": [0.5] * 8,
            f"behavior_prob_{CANDIDATE_ACTION}": [0.5] * 8,
        }
    )

    tau = _dr_tau(
        frame,
        target="reward",
        mu0=np.full(len(frame), 1.0),
        mu1=np.full(len(frame), 1.5),
    )

    assert np.allclose(tau, 0.5)


def test_sell_skip_gate_requires_value_and_competing_risk_lower_bounds() -> None:
    passing = {
        "reward": {"interval": {"p025": 0.01}},
        "competing_risk_utility_30m": {"interval": {"p025": 0.01}},
        "campaign_cost_avoidance": {"interval": {"p025": 0.0}},
        "trend_through_avoidance_30m": {"interval": {"p025": 0.0}},
        "negative_terminal_mtm": {"interval": {"p025": 0.0}},
        "development_q10_shortfall": {"interval": {"p025": 0.0}},
        "repair_first_30m": {"uplift": 0.0},
        "support": {"candidate_rate": 0.2, "policy_ess": 200.0, "days": 20},
    }

    passed, failures = _promotion_gate(passing)
    assert passed
    assert failures == []

    passing["competing_risk_utility_30m"]["interval"]["p025"] = 0.0
    passed, failures = _promotion_gate(passing)
    assert not passed
    assert "competing_risk_utility_30m_lower_bound_not_positive" in failures
