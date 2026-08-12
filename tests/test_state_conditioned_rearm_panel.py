from __future__ import annotations

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.state_conditioned_rearm_ope import (
    BASELINE_ACTION,
    CANDIDATE_ACTION,
    _candidate_policy,
    _derive_outcomes,
)
from research.families.f09_campaign_action_uplift.audit.state_conditioned_rearm_randomized import validate_panel


def _row(*, action: str, active: int, campaign_id: int = 1) -> dict:
    return {
        "day": "2026-06-01",
        "decision_id": f"decision-{campaign_id}",
        "campaign_id": campaign_id,
        "side": "SELL",
        "inventory_role": "add",
        "action": action,
        "behavior_propensity": 0.5,
        "behavior_prob_baseline_rearm": 0.5,
        "behavior_prob_continue_block_until_recovery": 0.5,
        "reward": 1.0,
        "fill_value": 0.25,
        "campaign_cost": -0.75,
        "queue_cost": 0.0,
        "reward_identity_error": 0.0,
        "entry_state_active": active,
        "entry_state_data_valid": 1,
        "action_effective": int(action == CANDIDATE_ACTION and active == 1),
        "baseline_cooldown_total_ms": 85_000.0,
        "baseline_rearm_elapsed_ms": 85_100.0,
        "blocked_quote_cycles": int(action == CANDIDATE_ACTION and active == 1),
        "external_reference_used": 0,
        "terminal_campaign_pnl": -2.0,
        "campaign_mae": -1.0,
        "decision_to_outcome_s": 120.0,
        "time_to_repair_s": 120.0,
        "campaign_closed": 1,
        "campaign_censored": 0,
        "intervention_fill_count": 0,
    }


def test_panel_contract_and_conditional_candidate_policy() -> None:
    frame = pd.DataFrame(
        [
            _row(action=BASELINE_ACTION, active=1, campaign_id=1),
            _row(action=CANDIDATE_ACTION, active=0, campaign_id=2),
            _row(action=CANDIDATE_ACTION, active=1, campaign_id=3),
        ]
    )
    validate_panel(frame, side="SELL")
    assert _candidate_policy(frame).tolist() == [
        CANDIDATE_ACTION,
        BASELINE_ACTION,
        CANDIDATE_ACTION,
    ]
    outcomes = _derive_outcomes(frame, q10=-3.0)
    assert outcomes["campaign_cost_avoidance"].tolist() == [0.75] * 3
    assert outcomes["tail_avoidance_5usdc"].tolist() == [0.0] * 3
    assert outcomes["repair_30m"].tolist() == [1.0] * 3
    assert outcomes["repair_time_avoidance_s"].tolist() == [-120.0] * 3
    assert outcomes["censoring_avoidance"].tolist() == [0.0] * 3


def test_panel_rejects_pre_cooldown_intervention() -> None:
    frame = pd.DataFrame([_row(action=BASELINE_ACTION, active=1)])
    frame.loc[0, "baseline_rearm_elapsed_ms"] = 84_999.0
    with pytest.raises(ValueError, match="before the baseline cooldown"):
        validate_panel(frame, side="SELL")


def test_panel_rejects_duplicate_campaign_reward_units() -> None:
    first = _row(action=BASELINE_ACTION, active=1, campaign_id=1)
    second = dict(first, decision_id="decision-duplicate")
    frame = pd.DataFrame([first, second])
    with pytest.raises(ValueError, match="only one randomized intervention"):
        validate_panel(frame, side="SELL")


def test_panel_rejects_candidate_effect_outside_entry_state() -> None:
    row = _row(action=CANDIDATE_ACTION, active=0)
    row["action_effective"] = 1
    row["blocked_quote_cycles"] = 1
    frame = pd.DataFrame([row])
    with pytest.raises(ValueError, match="outside the frozen entry state"):
        validate_panel(frame, side="SELL")
