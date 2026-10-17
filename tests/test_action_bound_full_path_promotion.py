from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from models.audit.action_bound_full_path_promotion import (
    IDENTITY,
    OWNER_PROMOTION,
    P3_LEVERAGE_FIELDS,
    REQUIRED_ECONOMIC_GATES,
    REQUIRED_FULL_PATH_COMPONENTS,
    REQUIRED_PRODUCTION_GATES,
    SCHEMA_VERSION,
    STANDARD_PROMOTION,
    build_action_embedded_p3_leverage,
    validate_action_embedded_p3_leverage,
    validate_governance_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / (
    "research/shared/experiment_governance/docs/"
    "action_bound_full_path_direct_promotion_contract_v1.json"
)
HASH_A = "12" * 32
HASH_B = "34" * 32
HASH_C = "56" * 32
HASH_D = "78" * 32
HASH_E = "9a" * 32


def _leverage(**overrides) -> dict:
    values = {
        "parent_action_identity_hash": HASH_A,
        "baseline_identity_hash": HASH_B,
        "candidate_policy_hash": HASH_A,
        "candidate_universe_sha256": HASH_C,
        "p3_artifact_hash": HASH_D,
        "quote_snapshot_id": "snapshot-1",
        "quote_snapshot_market_generation": 8,
        "quote_snapshot_depth_generation": 8,
        "side": "SELL",
        "role": "add",
        "candidate_source": "toxicity_guard_v1",
        "candidate_action": "widen_exposure_quote",
        "baseline_effective_price": 100.2,
        "candidate_effective_price": 100.4,
        "tick_size": 0.1,
        "support_valid": True,
        "baseline_reach_probability": 0.10,
        "candidate_reach_probability": 0.08,
        "probability_denominator_epsilon": 0.001,
        "delta_reach_lcb_simultaneous": -0.03,
        "delta_reach_ucb_simultaneous": -0.01,
        "reach_near_noop_abs_delta": 0.001,
        "reach_retention_floor": 0.20,
        "simultaneous_band_family_id": "toxicity_guard_v1.side-role-action-band",
        "simultaneous_band_artifact_sha256": HASH_E,
        "simultaneous_band_method": "paired_day_cluster_bootstrap",
    }
    values.update(overrides)
    return build_action_embedded_p3_leverage(**values)


def _contract() -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "stage_order": [
            "freeze_concrete_action",
            "authoritative_full_path_replay",
            "promotion_controller",
            "direct_active_live_with_rollback",
        ],
        "shadow_stage": {
            "mandatory_for_promotion": False,
            "grants_action_authority": False,
            "engineering_exception_requires_budget_and_expiry": True,
        },
        "action_binding": {
            "identity_hash_required": True,
            "baseline_hash_required": True,
            "candidate_policy_hash_required": True,
            "side_scope_frozen_before_replay": True,
            "candidate_rate_frozen_before_replay": True,
            "rate_limit_frozen_before_replay": True,
            "deployed_parameters_must_equal_replay": True,
        },
        "conditional_p3_leverage": {
            "role": "action_embedded_mechanics_input_only",
            "standalone_research_identity_allowed": False,
            "generates_quote": False,
            "grants_action_authority": False,
            "fields": list(P3_LEVERAGE_FIELDS),
            "interval_scope": (
                "paired_simultaneous_band_over_frozen_action_candidate_family"
            ),
        },
        "authoritative_full_path_replay": {
            "shared_quote_snapshot_between_arms": True,
            "required_components": list(REQUIRED_FULL_PATH_COMPONENTS),
            "economic_gates": list(REQUIRED_ECONOMIC_GATES),
        },
        "production_promotion": {
            "required_gates": list(REQUIRED_PRODUCTION_GATES),
            "routes": {
                "hard_gate_path": STANDARD_PROMOTION,
                "owner_progression_path": OWNER_PROMOTION,
            },
            "owner_label_is_permanent": True,
            "shadow_can_substitute_for_full_path": False,
        },
        "contract_permissions": {
            "action_authorized": False,
            "live_authorized": False,
        },
    }


def test_contract_allows_direct_active_route_without_mandatory_shadow():
    validate_governance_contract(_contract())


def test_embedded_p3_leverage_preserves_reach_not_fill_semantics():
    payload = _leverage()

    assert payload["effective_tick_delta"] == 2
    assert payload["price_action_noop"] is False
    assert payload["delta_reach_probability"] == pytest.approx(-0.02)
    assert payload["relative_reach_ratio"] == pytest.approx(0.8)
    assert payload["reach_near_noop"] is False
    assert payload["reach_collapse_risk"] is False
    assert payload["p3_generates_quote"] is False
    assert payload["p3_grants_action_authority"] is False
    assert "activity_collapse_risk" not in payload


def test_relative_reach_ratio_is_null_below_frozen_epsilon():
    payload = _leverage(
        baseline_reach_probability=0.0005,
        candidate_reach_probability=0.0002,
        probability_denominator_epsilon=0.001,
        delta_reach_lcb_simultaneous=-0.0005,
        delta_reach_ucb_simultaneous=-0.0001,
    )

    assert payload["delta_reach_probability"] == pytest.approx(-0.0003)
    assert payload["relative_reach_ratio"] is None
    assert payload["relative_reach_ratio_valid"] is False
    assert payload["reach_collapse_risk"] is None


def test_unsupported_pair_falls_back_without_fabricated_reach_values():
    payload = _leverage(
        support_valid=False,
        unsupported_reason="distance_outside_v4_1_support",
        baseline_reach_probability=None,
        candidate_reach_probability=None,
        delta_reach_lcb_simultaneous=None,
        delta_reach_ucb_simultaneous=None,
    )

    assert payload["support_valid"] is False
    assert payload["delta_reach_probability"] is None
    assert payload["relative_reach_ratio"] is None
    assert payload["reach_near_noop"] is None
    assert payload["reach_collapse_risk"] is None


def test_price_noop_and_reach_near_noop_remain_distinct():
    price_noop = _leverage(
        candidate_effective_price=100.2,
        candidate_reach_probability=0.10,
        delta_reach_lcb_simultaneous=-0.0002,
        delta_reach_ucb_simultaneous=0.0002,
    )
    assert price_noop["price_action_noop"] is True
    assert price_noop["reach_near_noop"] is False

    reach_noop = _leverage(
        candidate_effective_price=100.3,
        candidate_reach_probability=0.1001,
        delta_reach_lcb_simultaneous=-0.0002,
        delta_reach_ucb_simultaneous=0.0003,
    )
    assert reach_noop["price_action_noop"] is False
    assert reach_noop["reach_near_noop"] is True


def test_embedded_schema_rejects_activity_proxy_and_inverted_joint_band():
    payload = _leverage()
    payload["activity_collapse_risk"] = payload["reach_collapse_risk"]
    with pytest.raises(ValueError, match="schema mismatch"):
        validate_action_embedded_p3_leverage(payload)

    with pytest.raises(ValueError, match="paired simultaneous band"):
        _leverage(
            delta_reach_lcb_simultaneous=-0.01,
            delta_reach_ucb_simultaneous=-0.03,
        )


def test_standalone_p3_leverage_identity_is_rejected():
    contract = _contract()
    contract["conditional_p3_leverage"]["standalone_research_identity_allowed"] = True

    with pytest.raises(ValueError, match="standalone P3 leverage identity"):
        validate_governance_contract(contract)


def test_p3_cannot_generate_quote_or_grant_action_authority():
    for field in ("generates_quote", "grants_action_authority"):
        contract = _contract()
        contract["conditional_p3_leverage"][field] = True
        with pytest.raises(ValueError, match="P3 leverage"):
            validate_governance_contract(contract)


def test_shadow_cannot_be_mandatory_or_replace_full_path():
    mandatory = _contract()
    mandatory["shadow_stage"]["mandatory_for_promotion"] = True
    with pytest.raises(ValueError, match="mandatory promotion stage"):
        validate_governance_contract(mandatory)

    substitute = _contract()
    substitute["production_promotion"]["shadow_can_substitute_for_full_path"] = True
    with pytest.raises(ValueError, match="cannot substitute"):
        validate_governance_contract(substitute)


def test_full_path_components_and_economic_gates_are_exact():
    missing_path = _contract()
    missing_path["authoritative_full_path_replay"]["required_components"].remove(
        "cancel_request_ack_race"
    )
    with pytest.raises(ValueError, match="required_components"):
        validate_governance_contract(missing_path)

    missing_gate = _contract()
    missing_gate["authoritative_full_path_replay"]["economic_gates"].remove(
        "assignment_to_terminal_pnl_lcb_positive"
    )
    with pytest.raises(ValueError, match="economic_gates"):
        validate_governance_contract(missing_gate)


def test_owner_route_label_and_deployed_parameter_identity_are_mandatory():
    wrong_label = _contract()
    wrong_label["production_promotion"]["routes"]["owner_progression_path"] = (
        "research_supported_promotion"
    )
    with pytest.raises(ValueError, match="owner route label"):
        validate_governance_contract(wrong_label)

    drift = deepcopy(_contract())
    drift["action_binding"]["deployed_parameters_must_equal_replay"] = False
    with pytest.raises(ValueError, match="deployed_parameters"):
        validate_governance_contract(drift)


def test_frozen_contract_is_valid_and_has_no_current_permissions():
    contract = json.loads(CONTRACT_PATH.read_text())

    validate_governance_contract(contract)
    assert set(contract["contract_permissions"].values()) == {False}
    assert contract["shadow_stage"]["mandatory_for_promotion"] is False


def test_frozen_contract_canonical_identity_and_file_bindings_match():
    contract = json.loads(CONTRACT_PATH.read_text())
    expected_identity = contract.pop("canonical_contract_sha256")
    raw = json.dumps(
        contract,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    assert hashlib.sha256(raw).hexdigest() == expected_identity

    for binding_name in ("validator", "contract_tests"):
        binding = contract["artifact_bindings"][binding_name]
        artifact_path = ROOT / binding["path"]
        assert hashlib.sha256(artifact_path.read_bytes()).hexdigest() == binding["sha256"]
