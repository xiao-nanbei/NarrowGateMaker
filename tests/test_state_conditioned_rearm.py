from __future__ import annotations

import numpy as np

from models.replay_policies import (
    STATE_CONDITIONED_REARM_ACTIONS,
    StateConditionedRearmSpec,
    choose_state_conditioned_rearm_action,
    evaluate_state_conditioned_rearm,
    normalize_state_conditioned_rearm_probabilities,
)


def _entry_state() -> dict[str, float]:
    return {
        "path_feature_valid": 1.0,
        "path_l2_snapshot_count": 100.0,
        "path_book_age_ms": 200.0,
        "recovery_current_adverse_bps": 4.0,
        "shock_adverse_flow_imbalance_1s": 0.4,
        "shock_adverse_flow_imbalance_5s": 0.3,
        "shock_adverse_flow_imbalance_since_fill": 0.2,
        "refill_recovery_ratio": 0.4,
        "refill_current_vs_start_ratio": 0.8,
        "recovery_price_ratio": 0.2,
        "recovery_microprice_ratio": 0.3,
    }


def test_state_conditioned_rearm_has_exact_overlap() -> None:
    probabilities = normalize_state_conditioned_rearm_probabilities()
    assert probabilities == {
        "baseline_rearm": 0.5,
        "continue_block_until_recovery": 0.5,
    }
    rng = np.random.default_rng(7)
    actions = {
        choose_state_conditioned_rearm_action(rng, probabilities)[0]
        for _ in range(200)
    }
    assert actions == set(STATE_CONDITIONED_REARM_ACTIONS)


def test_entry_requires_all_causal_risk_components() -> None:
    state = _entry_state()
    decision = evaluate_state_conditioned_rearm(
        state, StateConditionedRearmSpec()
    )
    assert decision.entry_active
    assert not decision.exit_active

    for feature, replacement in (
        ("recovery_current_adverse_bps", 0.0),
        ("shock_adverse_flow_imbalance_5s", -0.1),
        ("refill_recovery_ratio", 1.2),
        ("recovery_price_ratio", 0.9),
    ):
        changed = dict(state)
        changed[feature] = replacement
        assert not evaluate_state_conditioned_rearm(changed).entry_active


def test_exit_uses_hysteresis_and_not_a_single_one_second_flow_flip() -> None:
    transient = _entry_state()
    transient["shock_adverse_flow_imbalance_1s"] = -0.8
    decision = evaluate_state_conditioned_rearm(transient)
    assert not decision.entry_active
    assert not decision.exit_active

    persistent_flow_exit = dict(transient)
    persistent_flow_exit["shock_adverse_flow_imbalance_5s"] = -0.1
    persistent_flow_exit["shock_adverse_flow_imbalance_since_fill"] = -0.1
    decision = evaluate_state_conditioned_rearm(persistent_flow_exit)
    assert decision.exit_active
    assert decision.exit_reason == "adverse_flow_dissipated"


def test_stale_or_invalid_path_never_rearms_candidate() -> None:
    state = _entry_state()
    state["path_book_age_ms"] = 5_000.0
    decision = evaluate_state_conditioned_rearm(state)
    assert not decision.data_valid
    assert not decision.entry_active
    assert not decision.exit_active
