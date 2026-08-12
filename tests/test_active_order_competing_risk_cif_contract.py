from __future__ import annotations

import hashlib
import json
from pathlib import Path

from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif import (
    CAUSES,
    GRID_INTERVAL_MS,
    IDENTITY,
    STATE_SCHEMA_VERSION,
    ActiveOrderCompetingRiskCIF,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "active_order_competing_risk_cif_100ms_v1_spec_20260804.json"
)
DOC_PATH = SPEC_PATH.with_suffix(".md")
KERNEL_PATH = ROOT / (
    "research/families/f07_active_order_continuation/audit/"
    "active_order_competing_risk_cif.py"
)
KERNEL_SHA256 = "9c9c00902b2ff495895d76c119f3095b1213bd53954588fa186ff33ec80ffa72"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _spec() -> dict[str, object]:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def test_contract_binds_current_kernel_identity_and_grid() -> None:
    spec = _spec()
    implementation = spec["implementation_identity"]
    grid = spec["state_grid_contract"]

    assert spec["identity"] == IDENTITY
    assert spec["last_materially_modified"] == "2026-08-04"
    assert implementation["path"] == str(KERNEL_PATH.relative_to(ROOT))
    assert implementation["sha256"] == KERNEL_SHA256
    assert _sha256(KERNEL_PATH) == KERNEL_SHA256
    assert grid["interval_ms"] == GRID_INTERVAL_MS == 100
    assert grid["duplicate_edge_policy"] == "fail_closed"
    assert grid["missed_edge_policy"] == "fail_closed_no_backfill"
    assert grid["terminal_evaluation_allowed"] is False


def test_contract_freezes_joint_competing_risk_semantics() -> None:
    spec = _spec()
    competing = spec["competing_risk_contract"]

    assert tuple(competing["causes_in_canonical_order"]) == CAUSES == (
        "favorable_fill",
        "adverse_fill",
        "cancel_ack",
        "other_terminal",
    )
    assert competing["cause_specific_rate_unit"] == "events_per_second"
    assert competing["joint_normalization_required"] is True
    assert competing["survival_and_all_cause_cifs_sum_to_one"] is True
    assert competing["independent_binary_hazard_normalization_prohibited"] is True


def test_contract_freezes_lifecycle_transition_and_terminal_boundaries() -> None:
    lifecycle = _spec()["lifecycle_contract"]

    assert lifecycle["risk_phases"] == [
        "ACTIVE",
        "PARTIALLY_FILLED",
        "CANCEL_PENDING",
    ]
    assert lifecycle["partial_fill"] == {
        "classification": "observed_risk_spell_boundary",
        "ends_current_remaining_quantity_risk_spell": True,
        "starts_new_risk_spell_when_remaining_quantity_is_positive": True,
        "new_spell_id_required": True,
        "last_evaluated_grid_edge_preserved": True,
        "survival_and_cifs_reset_for_new_spell": True,
        "order_terminal": False,
    }
    assert lifecycle["cancel_request"]["classification"] == "state_transition"
    assert lifecycle["cancel_request"]["risk_set_ends"] is False
    assert lifecycle["cancel_reject"]["classification"] == "state_transition"
    assert lifecycle["cancel_reject"]["risk_set_ends"] is False
    assert lifecycle["cancel_ack"]["cause"] == "cancel_ack"
    assert lifecycle["cancel_ack"]["risk_set_ends"] is True
    assert lifecycle["post_terminal_hazard_evaluations_allowed"] is False


def test_checkpoint_contract_matches_kernel_payload_and_roundtrips() -> None:
    checkpoint = _spec()["checkpoint_contract"]
    state = ActiveOrderCompetingRiskCIF.start(
        spell_id="contract-order:spell-0",
        phase="ACTIVE",
        remaining_qty=0.001,
        last_edge=100,
    ).transition_phase("CANCEL_PENDING")
    payload = state.checkpoint()

    assert checkpoint["schema_version"] == STATE_SCHEMA_VERSION
    assert set(checkpoint["required_state"]) == set(payload)
    assert checkpoint["checkpoint_restore_parity_required"] is True
    assert ActiveOrderCompetingRiskCIF.restore(payload) == state


def test_training_execution_economics_and_q90_action_remain_closed() -> None:
    spec = _spec()
    prerequisites = spec["data_and_clock_prerequisites"]
    gate = spec["training_and_execution_gate"]
    outcomes = spec["outcome_access"]
    runtime = spec["runtime_contract"]
    permissions = spec["permissions"]

    assert prerequisites["authoritative_lifecycle_schema_required"] == (
        "lifecycle_events_v2"
    )
    assert prerequisites["lifecycle_events_v2_available"] is False
    assert prerequisites["chronological_lockstep_days_required"] == 40
    assert prerequisites["chronological_lockstep_complete"] is False
    assert prerequisites["aws_receive_time_transport_complete"] is False
    assert gate["training_eligible"] is False
    assert gate["execution_eligible"] is False
    assert all(value is False for value in outcomes.values())
    assert runtime == {
        "q90_action_enabled": False,
        "q90_action_must_remain_off": True,
        "q90_threshold_change_authorized": False,
        "live_configuration_change_authorized": False,
    }
    assert permissions == {
        "mechanics_contract_frozen": True,
        "lifecycle_panel_generation_authorized": False,
        "model_training_authorized": False,
        "prediction_evaluation_authorized": False,
        "execution_replay_authorized": False,
        "economic_outcome_read_authorized": False,
        "validation_read_authorized": False,
        "sealed_holdout_read_authorized": False,
        "q90_action_enable_authorized": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
        "baseline_update_authorized": False,
    }


def test_markdown_matches_machine_readable_contract() -> None:
    text = DOC_PATH.read_text(encoding="utf-8")

    assert "Last materially modified: 2026-08-04" in text
    assert KERNEL_SHA256 in text
    assert "`favorable_fill`" in text
    assert "`adverse_fill`" in text
    assert "`cancel_ack`" in text
    assert "`other_terminal`" in text
    assert "100 ms" in text
    assert "partial fill ends the current remaining-quantity risk spell" in text
    assert "cancel request moves the order into `CANCEL_PENDING`" in text
    assert "cancel reject returns the order" in text
    assert "q90 action remains OFF" in text
    assert "chronological\n40-day Python/C++ event lockstep" in text
    assert "AWS\nreceive-time transport" in text
    assert "PnL, reward, markout" in text
