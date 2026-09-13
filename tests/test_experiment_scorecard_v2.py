from __future__ import annotations

import pytest

from models.audit.experiment_scorecard import (
    score_profile as legacy_score_profile,
    score_profile_contract as legacy_score_profile_contract,
)
from models.audit.experiment_scorecard_v2 import (
    CANONICAL_EVIDENCE_SCHEMA_VERSION,
    CONTINUOUS_PATH_SCHEMA_VERSION,
    PROFILES,
    score_canonical_evidence,
    score_profile_contract,
)

def _metric(
    estimate: float,
    lower: float,
    upper: float = 0.10,
    daily_positive_rate: float = 0.65,
) -> dict[str, float]:
    return {
        "estimate": estimate,
        "lower_bound": lower,
        "upper_bound": upper,
        "daily_positive_rate": daily_positive_rate,
    }


def _continuous_evidence(
    profile_id: str = "action_execution_selective_v3",
) -> dict:
    return {
        "schema_version": CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": "continuous_path_synthetic_v1",
        "family_id": "continuous_path_synthetic_v1",
        "panel_role": "development",
        "score_profile_contract": score_profile_contract(profile_id),
        "input_identity": {"config_sha256": "a" * 64},
        "validity_failures": [],
        "support": {
            "n_rows": 1000,
            "n_days": 20,
            "effective_sample_size": 400.0,
            "minimum_behavior_propensity": 0.5,
            "importance_weight_clipped_rows": 0,
            "unsupported_mass": 0.0,
            "overlap_violations": 0,
            "failures": [],
        },
        "candidate_rate": 0.10,
        "invariant_violations": [],
        "family_gate_failures": [],
        "metrics": {
            "conditional_net_value": _metric(0.03, 0.01),
            "closed_campaign_value": _metric(0.02, 0.005),
            "full_panel_continuous_mtm": {"estimate": 1.25},
            "negative_terminal_protection": _metric(0.02, 0.005),
            "q10_shortfall_protection": _metric(0.03, 0.005),
            "campaign_cvar10_protection": _metric(0.03, 0.0),
            "campaign_mae_avoidance": _metric(0.08, 0.02),
            "maximum_inventory_avoidance": _metric(0.001, 0.0),
            "inventory_time_avoidance": _metric(10.0, 0.0),
            "repair_event": _metric(0.02, 0.001),
            "repair_time_avoidance_s": _metric(180.0, 30.0, 300.0),
            "fills_retention": {"estimate": 0.95},
            "queue_reset_value": _metric(0.01, 0.002),
            "latency_adjusted_value": _metric(0.01, 0.002),
            "toxic_fill_selectivity_log_ratio": _metric(0.80, 0.15, 1.20),
            "toxic_reduction_surplus": _metric(0.18, 0.04, 0.30),
            "day_end_inventory_btc": {"estimate": -0.002},
            "day_end_open_campaign_mtm_usdc": {"estimate": -2.0},
            "censoring_avoidance": _metric(-1.0, -2.0),
        },
        "continuous_path_accounting": {
            "schema_version": CONTINUOUS_PATH_SCHEMA_VERSION,
            "utc_day_role": "bootstrap_cluster_only",
            "cash_carried_across_utc_days": True,
            "inventory_carried_across_utc_days": True,
            "campaign_state_carried_across_utc_days": True,
            "forced_day_end_liquidations": 0,
            "day_end_state_resets": 0,
            "day_end_campaign_terminals": 0,
            "daily_pnl_sum_usdc": 1.25,
            "continuous_panel_pnl_usdc": 1.25,
            "daily_accounting_identity_max_abs_error_usdc": 1e-10,
            "panel_final_inventory_btc": 0.001,
            "panel_final_mark_price_usdc_per_btc": 65000.0,
            "panel_final_inventory_mtm_usdc": 65.0,
            "panel_final_inventory_mtm_included": True,
        },
    }


def test_legacy_selective_v2_contract_remains_semantically_available() -> None:
    profile = legacy_score_profile("action_execution_selective_v2")
    contract = legacy_score_profile_contract("action_execution_selective_v2")

    assert profile.minimum_fills_retention == pytest.approx(0.0)
    assert profile.require_positive_toxic_selectivity is True
    assert sum(metric.weight for metric in profile.metrics) == pytest.approx(1.0)
    assert contract["schema_version"] == "narrowgate_score_profile.v1"
    assert contract["profile_id"] == profile.profile_id
    assert len(contract["profile_sha256"]) == 64
    assert contract == legacy_score_profile_contract(profile.profile_id)


def test_v2_profiles_have_unit_weight_and_versioned_contracts() -> None:
    expected_value_weights = {
        "action_alpha_v2": (0.50, 0.04),
        "action_defense_v2": (0.35, 0.04),
        "action_execution_v2": (0.40, 0.10 * (4.0 / 15.0)),
        "action_execution_selective_v3": (0.35, 0.03),
    }
    for profile_id, profile in PROFILES.items():
        assert sum(rule.weight for rule in profile.metrics) == pytest.approx(1.0)
        assert not any(rule.name == "censoring_avoidance" for rule in profile.metrics)
        closed_campaign = next(
            rule for rule in profile.metrics if rule.name == "closed_campaign_value"
        )
        conditional = next(
            rule for rule in profile.metrics if rule.name == "conditional_net_value"
        )
        expected_closed, expected_conditional = expected_value_weights[profile_id]
        assert closed_campaign.weight == pytest.approx(expected_closed)
        assert conditional.weight == pytest.approx(expected_conditional)
        contract = score_profile_contract(profile_id)
        assert contract["schema_version"] == "narrowgate_score_profile.v2"
        assert len(contract["profile_sha256"]) == 64


def test_day_end_metrics_are_diagnostic_only_and_cannot_fail_ranking() -> None:
    evidence = _continuous_evidence()
    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v3",
    )

    assert result["validity"]["passed"]
    assert result["hard_gates"]["passed"]
    assert result["ranking_eligible"]
    scored_names = {row["name"] for row in result["metrics"]}
    assert "censoring_avoidance" not in scored_names
    assert result["diagnostic_only_metrics"]["censoring_avoidance"][
        "estimate"
    ] == -1.0
    assert result["diagnostic_only_metrics"]["day_end_inventory_btc"][
        "estimate"
    ] == -0.002


def test_continuous_accounting_and_final_inventory_mtm_are_fail_closed() -> None:
    evidence = _continuous_evidence()
    evidence["continuous_path_accounting"]["continuous_panel_pnl_usdc"] = 1.0
    evidence["continuous_path_accounting"]["panel_final_inventory_mtm_usdc"] = 0.0

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v3",
    )

    assert not result["validity"]["passed"]
    assert not result["ranking_eligible"]
    assert "daily_sum_continuous_panel_pnl_mismatch" in result["validity"][
        "failures"
    ]
    assert "panel_final_inventory_mtm_mismatch" in result["validity"][
        "failures"
    ]


def test_utc_midnight_cannot_liquidate_reset_or_terminalize_campaign() -> None:
    evidence = _continuous_evidence()
    evidence["continuous_path_accounting"]["forced_day_end_liquidations"] = 1

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v3",
    )

    assert not result["validity"]["passed"]
    assert (
        "continuous_path_forbidden_nonzero:forced_day_end_liquidations"
        in result["validity"]["failures"]
    )


@pytest.mark.parametrize(
    "metric_name",
    [
        "campaign_cvar10_protection",
        "maximum_inventory_avoidance",
        "inventory_time_avoidance",
    ],
)
def test_continuous_campaign_risk_remains_a_noncompensable_gate(
    metric_name: str,
) -> None:
    evidence = _continuous_evidence()
    evidence["metrics"][metric_name] = _metric(0.1, -0.001)

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v3",
    )

    assert not result["hard_gates"]["passed"]
    assert not result["ranking_eligible"]
    assert f"{metric_name}_lower_bound_negative" in result["hard_gates"][
        "failures"
    ]


def test_closed_campaign_value_is_primary_noncompensable_value_gate() -> None:
    evidence = _continuous_evidence()
    evidence["metrics"]["closed_campaign_value"] = _metric(0.01, 0.0)

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v3",
    )

    assert not result["hard_gates"]["passed"]
    assert "closed_campaign_value_lower_bound_not_positive" in result[
        "hard_gates"
    ]["failures"]


def test_unfrozen_v2_profile_cannot_be_ranked() -> None:
    evidence = _continuous_evidence()
    evidence["score_profile_contract"] = {}

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v3",
    )

    assert not result["validity"]["passed"]
    assert not result["ranking_eligible"]
    assert result["promotion_status"] == "invalid_unfrozen_score_profile"
