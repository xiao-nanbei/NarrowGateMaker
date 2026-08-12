from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.buy_conditional_widen_cate import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    _dr_tau,
    _promotion_gate,
)


def test_binary_dr_pseudo_outcome_recovers_known_effect() -> None:
    frame = pd.DataFrame(
        {
            "action": [CONTROL_ACTION, CANDIDATE_ACTION] * 4,
            "target_reward": [2.0, 3.0] * 4,
            f"behavior_prob_{CONTROL_ACTION}": [0.5] * 8,
            f"behavior_prob_{CANDIDATE_ACTION}": [0.5] * 8,
        }
    )

    tau = _dr_tau(
        frame,
        target="reward",
        mu0=np.full(len(frame), 2.0),
        mu1=np.full(len(frame), 3.0),
    )

    assert np.allclose(tau, 1.0)


def test_development_gate_rejects_nonpositive_reward_lower_bound() -> None:
    passing = {
        "reward": {"interval": {"p025": 0.01}},
        "campaign_cost_avoidance": {"interval": {"p025": 0.0}},
        "negative_terminal_mtm": {"interval": {"p025": 0.0}},
        "development_q10_shortfall": {"interval": {"p025": 0.0}},
        "repair_event": {"uplift": 0.0},
        "restricted_time_to_repair": {"uplift": 0.0},
        "support": {"candidate_rate": 0.5, "policy_ess": 200.0, "days": 20},
    }
    passed, failures = _promotion_gate(passing)
    assert passed
    assert failures == []

    passing["reward"]["interval"]["p025"] = 0.0
    passed, failures = _promotion_gate(passing)
    assert not passed
    assert "reward_lower_bound_not_positive" in failures
