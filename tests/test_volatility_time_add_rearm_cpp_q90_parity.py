from __future__ import annotations

import pandas as pd

from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_cpp_q90_parity as parity,
)


def _spec() -> dict:
    return {
        "panels": {"parity_days": ["2026-04-17"], "development_days": ["2026-04-17"]},
        "gates": {
            "minimum_native_book_events": 100,
            "minimum_activations": 2,
            "minimum_evaluation_calls": 10,
            "minimum_cancel_requests": 1,
            "minimum_cancel_acks": 1,
            "minimum_recoveries": 1,
            "minimum_reentries": 1,
        },
        "mechanism_coverage": {
            "historical_development_pre_ack_fill_count": 0,
            "pre_ack_fill_synthetic_test": True,
        },
    }


def test_parity_result_keeps_full_cpp_replay_authority_false() -> None:
    row = parity._parity_result(
        {
            "dynamic_fill_hazard_cpp_parity_passed": True,
            "dynamic_fill_hazard_full_cpp_tick_replay_authority": False,
            "dynamic_fill_hazard_cpp_identity": {
                "scope": "native_book_and_buy_q90_parity_kernel_only",
            },
        }
    )

    assert row["parity_passed"]
    assert not row["full_cpp_tick_replay_authority"]
    assert row["scope"] == "native_book_and_buy_q90_parity_kernel_only"


def test_decision_requires_both_arms_and_lifecycle_branch_coverage() -> None:
    base = {
        "day": "2026-04-17",
        "parity_passed": True,
        "mismatch_count": 0,
        "book_event_count": 100,
        "activation_count": 2,
        "evaluation_call_count": 10,
        "lifecycle_call_count": 2,
        "cancel_request_count": 1,
        "cancel_ack_count": 1,
        "pre_ack_fill_count": 0,
        "recovery_count": 1,
        "reentry_count": 1,
        "full_cpp_tick_replay_authority": False,
    }
    rows = pd.DataFrame(
        [
            {**base, "arm": "control_wall_time"},
            {**base, "arm": "candidate_variance_time"},
        ]
    )

    decision, gates = parity._decision(rows, _spec())
    assert gates["parity_gate_passed"]
    assert decision == (
        "cpp_q90_native_parity_pass_register_randomized_replay_identity"
    )

    missing_arm = rows.iloc[:1].copy()
    decision, gates = parity._decision(missing_arm, _spec())
    assert not gates["parity_gate_passed"]
    assert decision == (
        "cpp_q90_native_parity_failed_keep_action_identity_blocked"
    )
