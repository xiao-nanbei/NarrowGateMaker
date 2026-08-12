from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from models.audit.experiment_scorecard import (
    CANONICAL_EVIDENCE_SCHEMA_VERSION,
    PROFILES,
    action_family_score_evidence,
    paired_screen_v2_score_evidence,
    paired_selection_score_evidence,
    score_canonical_evidence,
    score_profile_contract,
)


def _metric(
    estimate: float,
    lower: float,
    upper: float = 0.1,
    daily_positive_rate: float = 0.60,
) -> dict[str, float]:
    return {
        "estimate": estimate,
        "lower_bound": lower,
        "upper_bound": upper,
        "daily_positive_rate": daily_positive_rate,
    }


def _action_evidence(*, profile_id: str = "action_alpha_v1") -> dict:
    return {
        "schema_version": CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": "synthetic_action_v1",
        "family_id": "synthetic_action_v1",
        "panel_role": "development",
        "score_profile_contract": score_profile_contract(profile_id),
        "input_identity": {"config_sha256": "a" * 64},
        "validity_failures": [],
        "support": {
            "n_rows": 500,
            "n_days": 20,
            "effective_sample_size": 200.0,
            "minimum_behavior_propensity": 0.5,
            "unsupported_mass": 0.0,
            "overlap_violations": 0,
            "failures": [],
        },
        "candidate_rate": 0.25,
        "invariant_violations": [],
        "family_gate_failures": [],
        "metrics": {
            "conditional_net_value": _metric(0.03, 0.01),
            "negative_terminal_protection": _metric(0.02, 0.005),
            "q10_shortfall_protection": _metric(0.03, 0.005),
            "campaign_mae_avoidance": _metric(0.08, 0.02),
            "repair_event": _metric(0.02, 0.001),
            "repair_time_avoidance_s": _metric(180.0, 30.0, 300.0),
            "censoring_avoidance": _metric(0.01, 0.001),
            "fills_retention": {"estimate": 0.95},
        },
    }


def test_profiles_have_fixed_unit_weight_and_stable_contract() -> None:
    contracts = {}
    for profile_id, profile in PROFILES.items():
        assert sum(metric.weight for metric in profile.metrics) == pytest.approx(1.0)
        contracts[profile_id] = score_profile_contract(profile_id)
        assert len(contracts[profile_id]["profile_sha256"]) == 64
    assert contracts == {
        profile_id: score_profile_contract(profile_id) for profile_id in PROFILES
    }


def test_passing_action_evidence_is_rankable_but_not_live_promoted() -> None:
    result = score_canonical_evidence(
        _action_evidence(), profile_id="action_alpha_v1"
    )

    assert result["validity"]["passed"]
    assert result["support"]["passed"]
    assert result["hard_gates"]["passed"]
    assert result["ranking_eligible"]
    assert result["candidate_class"] == "development_candidate"
    assert result["promotion_status"] == "development_passed_validation_locked"
    assert result["total_score"] > 0.0
    assert result["weight_coverage"] == pytest.approx(1.0)
    json.dumps(result, allow_nan=False)


def test_tail_improvement_cannot_buy_through_reward_and_fill_collapse() -> None:
    evidence = _action_evidence(profile_id="action_defense_v1")
    evidence["metrics"]["conditional_net_value"] = _metric(
        0.0081547, -0.021603, 0.037109, 0.56
    )
    evidence["metrics"]["negative_terminal_protection"] = _metric(
        0.031331, 0.005839, 0.060769, 0.72
    )
    evidence["metrics"]["q10_shortfall_protection"] = _metric(
        0.029593, 0.008964, 0.055937, 0.84
    )
    evidence["metrics"]["campaign_mae_avoidance"] = _metric(
        0.084335, 0.049160, 0.127284, 0.92
    )
    evidence["metrics"]["repair_event"] = _metric(
        -0.000187, -0.001164, 0.000860, 0.40
    )
    evidence["metrics"]["repair_time_avoidance_s"] = _metric(
        257.152, 161.354, 353.157, 0.84
    )
    evidence["metrics"]["censoring_avoidance"] = _metric(
        -0.000187, -0.001188, 0.000805, 0.40
    )
    evidence["metrics"]["fills_retention"] = {"estimate": 0.107762}
    evidence["candidate_rate"] = 0.85733
    evidence["family_gate_failures"] = [
        "reward_lower_bound_not_positive",
        "fills_retention_below_0_85",
    ]

    result = score_canonical_evidence(
        evidence, profile_id="action_defense_v1"
    )

    assert not result["hard_gates"]["passed"]
    assert not result["ranking_eligible"]
    assert result["ranking_score"] is None
    assert result["economic_classification"] == "overbroad_risk_control"
    assert "fills_retention_below_0.85" in result["hard_gates"]["failures"]
    assert result["components"]["tail"]["score"] > 0.0
    assert result["components"]["mechanism"]["score"] == -1.0


def test_selective_execution_allows_fill_loss_only_when_toxicity_falls_faster() -> None:
    evidence = _action_evidence(profile_id="action_execution_selective_v1")
    evidence["metrics"].update(
        {
            "fills_retention": {"estimate": 0.72},
            "queue_reset_value": _metric(0.01, 0.002),
            "latency_adjusted_value": _metric(0.01, 0.002),
            "toxic_fill_selectivity_log_ratio": _metric(
                math.log(2.0), 0.10, 1.2
            ),
            "toxic_reduction_surplus": _metric(0.20, 0.03, 0.35),
        }
    )

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v1",
    )

    assert result["hard_gates"]["passed"]
    assert result["ranking_eligible"]
    assert result["components"]["selectivity"]["score"] > 0.0


def test_selective_execution_rejects_proportional_fill_collapse() -> None:
    evidence = _action_evidence(profile_id="action_execution_selective_v1")
    evidence["metrics"].update(
        {
            "fills_retention": {"estimate": 0.72},
            "queue_reset_value": _metric(0.01, 0.002),
            "latency_adjusted_value": _metric(0.01, 0.002),
            "toxic_fill_selectivity_log_ratio": _metric(0.0, -0.05, 0.05),
            "toxic_reduction_surplus": _metric(0.0, -0.05, 0.05),
        }
    )

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v1",
    )

    assert not result["hard_gates"]["passed"]
    assert not result["ranking_eligible"]
    assert "toxic_fill_selectivity_lower_bound_not_positive" in result[
        "hard_gates"
    ]["failures"]
    assert any(
        failure.startswith("fills_retention_below_0.85_without")
        for failure in result["hard_gates"]["failures"]
    )


def test_selective_v2_allows_large_volume_loss_when_toxicity_falls_faster() -> None:
    evidence = _action_evidence(profile_id="action_execution_selective_v2")
    evidence["metrics"].update(
        {
            "fills_retention": {"estimate": 0.08},
            "queue_reset_value": _metric(0.01, 0.002),
            "latency_adjusted_value": _metric(0.01, 0.002),
            "toxic_fill_selectivity_log_ratio": _metric(0.80, 0.15, 1.20),
            "toxic_reduction_surplus": _metric(0.18, 0.04, 0.30),
        }
    )

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v2",
    )

    assert result["hard_gates"]["passed"]
    assert result["ranking_eligible"]
    assert not any(
        "fills_retention_below" in failure
        for failure in result["hard_gates"]["failures"]
    )


def test_selective_v2_still_rejects_cancel_all_degeneracy() -> None:
    evidence = _action_evidence(profile_id="action_execution_selective_v2")
    evidence["metrics"].update(
        {
            "fills_retention": {"estimate": 0.0},
            "queue_reset_value": _metric(0.01, 0.002),
            "latency_adjusted_value": _metric(0.01, 0.002),
            "toxic_fill_selectivity_log_ratio": _metric(0.0, -0.01, 0.01),
            "toxic_reduction_surplus": _metric(0.0, -0.01, 0.01),
        }
    )

    result = score_canonical_evidence(
        evidence,
        profile_id="action_execution_selective_v2",
    )

    assert not result["hard_gates"]["passed"]
    assert "toxic_fill_selectivity_lower_bound_not_positive" in result[
        "hard_gates"
    ]["failures"]
    assert "toxic_reduction_surplus_lower_bound_not_positive" in result[
        "hard_gates"
    ]["failures"]


def test_unfrozen_profile_is_invalid_unless_explicitly_retrospective() -> None:
    evidence = _action_evidence()
    evidence["score_profile_contract"] = {}

    strict = score_canonical_evidence(
        evidence,
        profile_id="action_alpha_v1",
        require_frozen_profile=True,
    )
    retrospective = score_canonical_evidence(
        evidence,
        profile_id="action_alpha_v1",
        require_frozen_profile=False,
    )

    assert not strict["validity"]["passed"]
    assert strict["promotion_status"] == "invalid_unfrozen_score_profile"
    assert retrospective["validity"]["passed"]
    assert not retrospective["ranking_eligible"]
    assert retrospective["promotion_status"] == "retrospective_score_only"


def test_action_family_adapter_verifies_identity_and_maps_sell_metrics(
    tmp_path: Path,
) -> None:
    family_spec = {
        "schema_version": "synthetic.v1",
        "family_id": "synthetic_sell",
        "status": "frozen_before_outcome_replay",
        "behavior_probabilities": {"baseline": 0.5, "candidate": 0.5},
        "baseline": {
            "config_sha256": "a" * 64,
            "p3_sha256": "b" * 64,
            "queue_sha256": "c" * 64,
            "latency_sha256": "d" * 64,
        },
        "invariants": {
            "order_size_modified": False,
            "reducing_side_modified": False,
            "inventory_limit_modified": False,
            "taker_order_added": False,
        },
        "scorecard_profile": score_profile_contract("action_defense_v1"),
    }
    evidence_path = tmp_path / "evidence_split.json"
    evidence_path.write_text('{"split":"frozen"}', encoding="utf-8")
    import hashlib

    family_spec["evidence_split"] = {
        "path": str(evidence_path),
        "sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
    }
    spec_path = tmp_path / "family_spec.json"
    spec_path.write_text(json.dumps(family_spec), encoding="utf-8")
    spec_sha = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    panel_path = tmp_path / "development.csv"
    panel_path.write_text("decision_id\n1\n", encoding="utf-8")
    panel_sha = hashlib.sha256(panel_path.read_bytes()).hexdigest()
    panel = _action_evidence(profile_id="action_defense_v1")["metrics"]
    result = {
        "schema_version": "synthetic_result.v1",
        "family_id": "synthetic_sell",
        "family_spec": {"path": str(spec_path), "sha256": spec_sha},
        "development_panel": {"path": str(panel_path), "sha256": panel_sha},
        "development_gate_passed": True,
        "development_failures": [],
        "development": {
            "reward": {
                "uplift": panel["conditional_net_value"]["estimate"],
                "interval": {"p025": 0.01, "p975": 0.05},
                "daily_positive_rate": 0.60,
            },
            "negative_terminal_protection": {
                "uplift": 0.02,
                "interval": {"p025": 0.005, "p975": 0.04},
                "daily_positive_rate": 0.60,
            },
            "development_q10_shortfall_protection": {
                "uplift": 0.03,
                "interval": {"p025": 0.005, "p975": 0.05},
                "daily_positive_rate": 0.60,
            },
            "campaign_mae_avoidance": {
                "uplift": 0.08,
                "interval": {"p025": 0.02, "p975": 0.10},
                "daily_positive_rate": 0.60,
            },
            "repair_event": {
                "uplift": 0.02,
                "interval": {"p025": 0.001, "p975": 0.04},
                "daily_positive_rate": 0.60,
            },
            "restricted_time_to_repair": {
                "uplift": 180.0,
                "interval": {"p025": 30.0, "p975": 300.0},
                "daily_positive_rate": 0.60,
            },
            "day_end_censoring_avoidance": {
                "uplift": 0.01,
                "interval": {"p025": 0.001, "p975": 0.02},
                "daily_positive_rate": 0.60,
            },
            "support": {
                "oof_rows": 500,
                "oof_days": 20,
                "policy_ess": 200.0,
                "min_behavior_propensity": 0.5,
                "candidate_rate": 0.25,
                "unsupported_candidate_rows": 0,
                "overlap_violations": 0,
                "failures": [],
            },
            "activity": {"fills_retention": 0.95},
        },
    }

    evidence = action_family_score_evidence(
        result,
        family_spec,
        panel_role="development",
        profile_id="action_defense_v1",
    )

    assert evidence["validity_failures"] == []
    assert evidence["metrics"]["conditional_net_value"]["lower_bound"] == 0.01
    assert evidence["metrics"]["fills_retention"]["estimate"] == 0.95


def test_paired_screen_adapter_remains_screening_only() -> None:
    row = {
        "arm": "better",
        "group": "screen",
        "baseline_arm": "baseline",
        "n_days": 30,
        "coverage": 1.0,
        "raw_t_stat": 2.0,
        "terminal_t_stat": 1.8,
        "inv_adj_t_stat": 1.0,
        "tail_better_days": 8,
        "tail_worse_days": 2,
        "bad_campaign_rate_delta": -0.01,
        "campaign_mae_ratio": 0.95,
        "repair_rate_delta": 0.01,
        "campaign_duration_ratio": 0.95,
        "fills_ratio": 0.95,
        "inventory_time_ratio": 0.95,
        "pause_rate_delta": 0.01,
        "keep_rate_delta": 0.01,
        "place_replace_rate_delta": 0.01,
        "mechanism_pass": True,
        "mechanism_notes": "pass",
    }
    evidence = paired_selection_score_evidence(
        row,
        score_profile_contract_value=score_profile_contract("paired_screen_v1"),
    )
    result = score_canonical_evidence(
        evidence, profile_id="paired_screen_v1"
    )

    assert result["validity"]["passed"]
    assert result["support"]["passed"]
    assert result["hard_gates"]["passed"]
    assert not result["ranking_eligible"]
    assert result["promotion_status"] == "screening_rank_only"
    assert result["total_score"] > 0.0


def test_paired_screen_v2_is_rankable_but_has_no_promotion_authority() -> None:
    row = {
        "arm": "better",
        "group": "screen",
        "baseline_arm": "baseline",
        "n_days": 30,
        "coverage": 1.0,
        "raw_delta_sum": 3.0,
        "terminal_delta_sum": 2.0,
        "activity_adjusted_raw_delta": 1.0,
        "campaign_adjusted_terminal_delta": 1.0,
        "raw_t_stat": 2.0,
        "terminal_t_stat": 1.8,
        "inv_adj_t_stat": 1.0,
        "tail_campaign_delta": 0,
        "tail_better_days": 8,
        "tail_worse_days": 2,
        "bad_campaign_rate_delta": -0.01,
        "campaign_mae_ratio": 0.95,
        "repair_rate_delta": 0.01,
        "campaign_duration_ratio": 0.95,
        "fills_ratio": 0.95,
        "campaign_ratio": 1.0,
        "inventory_time_ratio": 0.95,
        "pause_rate_delta": 0.01,
        "keep_rate_delta": 0.01,
        "place_replace_rate_delta": 0.01,
        "final_spread_delta": 1.0,
        "side_min_fill_share": 0.48,
    }
    evidence = paired_screen_v2_score_evidence(
        row,
        score_profile_contract_value=score_profile_contract("paired_screen_v2"),
    )
    result = score_canonical_evidence(evidence, profile_id="paired_screen_v2")

    assert result["validity"]["passed"]
    assert result["support"]["passed"]
    assert result["hard_gates"]["passed"]
    assert result["ranking_eligible"]
    assert result["ranking_score"] == result["total_score"]
    assert result["candidate_class"] == "screening_alpha_candidate"
    assert result["promotion_status"] == "screening_rank_only"
