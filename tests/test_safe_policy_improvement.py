from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.safe_policy_improvement import (
    SpiBBConfig,
    evaluate_local_m0_external_m1_gate,
    fit_spibb_baseline_fallback,
)


def _ope_rows(uplift: float = 1.0) -> pd.DataFrame:
    rows = []
    for day_index in range(10):
        day = f"2026-01-{day_index + 1:02d}"
        for row_index in range(20):
            action = "keep" if row_index % 2 == 0 else "cancel_until_state_exit"
            rows.append(
                {
                    "day": day,
                    "side": "BUY",
                    "queue_state_key": "q0",
                    "microprice_state_key": "m0",
                    "action": action,
                    "behavior_propensity": 0.5,
                    "ope_dr_uplift": uplift + (row_index % 3) * 0.01,
                }
            )
    return pd.DataFrame(rows)


def test_spibb_uses_candidate_only_with_support_ess_and_positive_lcb() -> None:
    rows, artifact, summary = fit_spibb_baseline_fallback(
        _ope_rows(),
        config=SpiBBConfig(
            minimum_candidate_rows=50,
            minimum_baseline_rows=50,
            minimum_effective_sample_size=50.0,
            bootstrap_trials=100,
        ),
    )

    assert summary["spibb_gate_passed"]
    assert artifact.accepted_state_ids == ("BUY|q0|m0",)
    assert set(rows["spibb_executed_action"]) == {"cancel_until_state_exit"}


def test_spibb_falls_back_to_baseline_when_lower_bound_is_negative() -> None:
    rows, artifact, summary = fit_spibb_baseline_fallback(
        _ope_rows(-1.0),
        config=SpiBBConfig(
            minimum_candidate_rows=10,
            minimum_baseline_rows=10,
            minimum_effective_sample_size=10.0,
            bootstrap_trials=50,
        ),
    )

    assert not summary["spibb_gate_passed"]
    assert not artifact.accepted_state_ids
    assert set(rows["spibb_executed_action"]) == {"keep"}


def test_spibb_policy_identity_includes_direct_uplift_evidence() -> None:
    _, positive, _ = fit_spibb_baseline_fallback(
        _ope_rows(1.0),
        config=SpiBBConfig(bootstrap_trials=0),
    )
    _, negative, _ = fit_spibb_baseline_fallback(
        _ope_rows(-1.0),
        config=SpiBBConfig(bootstrap_trials=0),
    )

    assert positive.evidence_sha256 != negative.evidence_sha256
    assert positive.policy_id != negative.policy_id


def test_external_m1_cannot_advance_before_local_m0() -> None:
    m0 = {
        "spibb_gate_passed": False,
        "dr_uplift": 0.1,
        "uplift_p025": -0.1,
    }
    m1 = {
        "spibb_gate_passed": True,
        "dr_uplift": 1.0,
        "uplift_p025": 0.5,
    }
    result = evaluate_local_m0_external_m1_gate(m0, m1)

    assert not result["passed"]
    assert "local_m0_not_passed" in result["reasons"]


def test_external_m1_requires_leave_one_venue_out_stability() -> None:
    m0 = {
        "spibb_gate_passed": True,
        "dr_uplift": 0.2,
        "uplift_p025": 0.1,
    }
    m1 = {
        "spibb_gate_passed": True,
        "dr_uplift": 0.5,
        "uplift_p025": 0.2,
    }
    result = evaluate_local_m0_external_m1_gate(
        m0,
        m1,
        leave_one_venue_out={
            "without_okx": {"dr_uplift": 0.3, "uplift_p025": 0.1},
            "without_bybit": {"dr_uplift": 0.2, "uplift_p025": 0.05},
        },
    )

    assert result["passed"]
    assert np.isclose(result["incremental_m1_minus_m0"], 0.3)
