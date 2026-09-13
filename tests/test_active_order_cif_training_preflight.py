from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
)
from models.replay.baseline_epoch_manifest import (
    REQUIRED_IDENTITY_FIELDS,
    SCHEMA_VERSION,
    canonical_sha256,
    epoch_identity_sha256,
    finalize_manifest,
)
from research.families.f07_active_order_continuation.audit.active_order_cif_training_preflight import (
    METADATA_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
    ActiveOrderCIFPreflightError,
    current_unauthoritative_live_journal_metadata,
    lifecycle_journal_v2_schema_sha256,
    run_active_order_cif_training_preflight,
)
from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif import (
    CAUSES,
    GRID_INTERVAL_MS,
)

ROOT = Path(__file__).resolve().parents[1]
KERNEL_PATH = ROOT / (
    "research/families/f07_active_order_continuation/audit/active_order_competing_risk_cif.py"
)
PREFLIGHT_PATH = ROOT / (
    "research/families/f07_active_order_continuation/audit/active_order_cif_training_preflight.py"
)
DESIGN_JSON_PATH = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "active_order_cif_training_preflight_v1_design_20260804.json"
)
DESIGN_MD_PATH = DESIGN_JSON_PATH.with_suffix(".md")


def _sha(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _ready_manifest() -> dict[str, object]:
    identity = {name: _sha(name) for name in REQUIRED_IDENTITY_FIELDS}
    return finalize_manifest(
        {
            "schema_version": SCHEMA_VERSION,
            "manifest_id": "f07-preflight-test",
            "source_clock": "utc_ns",
            "scope_start_ts_ns": 1_700_000_000_000_000_000,
            "scope_end_ts_ns": 1_700_086_400_000_000_000,
            "utc_midnight_splits_epoch": False,
            "pooled_estimation_authorized": False,
            "required_identity_fields": list(REQUIRED_IDENTITY_FIELDS),
            "restart_audit_complete": True,
            "epochs": [
                {
                    "epoch_id": "epoch-1",
                    "start_ts_ns": 1_700_000_000_000_000_000,
                    "end_ts_ns": 1_700_086_400_000_000_000,
                    "start_reason": "scope_start",
                    "boundary_status": "first_decision_bound",
                    "identity": identity,
                    "identity_sha256": epoch_identity_sha256(identity),
                    "binding_status": "fully_bound",
                    "initial_economic_state_complete": False,
                    "lifecycle_estimation_authorized": True,
                    "continuous_economic_estimation_authorized": False,
                    "pooling_authorized": False,
                }
            ],
            "unbound_intervals": [],
        }
    )


def _ready_metadata(manifest: dict[str, object]) -> dict[str, object]:
    days = [f"2026-06-{day:02d}" for day in range(1, 31)] + [
        f"2026-07-{day:02d}" for day in range(1, 11)
    ]
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "dataset_id": "f07-lifecycle-v2-test",
        "journal_schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
        "journal_columns": list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS),
        "scope_start_ts_ns": manifest["scope_start_ts_ns"],
        "scope_end_ts_ns": manifest["scope_end_ts_ns"],
        "row_count": 1_000,
        "order_count": 200,
        "chronological_day_ids": days,
        "runtime_integration": {
            "live_v2_batch_emitter_integrated": True,
            "replay_v2_batch_emitter_integrated": True,
            "all_unseen_events_emitted": True,
            "callback_batch_commit_atomic": True,
            "durable_cursor_checkpoint": True,
            "writer_health_manifest_present": True,
            "writer_drop_count": 0,
            "writer_error_count": 0,
            "live_producer_sha256": _sha("live-producer"),
            "replay_producer_sha256": _sha("replay-producer"),
        },
        "artifact_identity": {
            "lifecycle_tape_path": "/authority/lifecycle-v2.parquet",
            "lifecycle_tape_sha256": _sha("tape"),
            "admission_manifest_path": "/authority/lifecycle-v2.manifest.json",
            "admission_manifest_sha256": _sha("admission"),
            "health_manifest_path": "/authority/lifecycle-v2.health.json",
            "health_manifest_sha256": _sha("health"),
            "journal_schema_sha256": lifecycle_journal_v2_schema_sha256(),
            "atomic_admission": True,
            "payload_integrity_valid": True,
            "row_count_agreement": True,
        },
        "epoch_binding": {
            "baseline_manifest_sha256": manifest["canonical_manifest_sha256"],
            "every_row_bound_to_epoch": True,
            "unbound_row_count": 0,
            "cross_epoch_order_count": 0,
            "epoch_ids": ["epoch-1"],
        },
        "grid_contract": {
            "interval_ms": GRID_INTERVAL_MS,
            "clock": "causal_visibility_clock",
            "contiguous_edges": True,
            "duplicate_edge_count": 0,
            "missed_edge_count": 0,
        },
        "competing_risks": {
            "causes": list(CAUSES),
            "cause_counts": {
                "favorable_fill": 20,
                "adverse_fill": 30,
                "cancel_ack": 100,
                "other_terminal": 50,
            },
            "full_fill_classifier_identity": "frozen-upstream-fill-cause-v1",
            "full_fill_classifier_sha256": _sha("fill-classifier"),
            "unknown_terminal_count": 0,
            "post_terminal_risk_row_count": 0,
            "cancel_request_terminal_count": 0,
            "cancel_reject_terminal_count": 0,
        },
        "dual_clock_contract": {
            "visibility_clock_present": True,
            "exchange_clock_present": True,
            "exchange_timestamp_after_visibility_count": 0,
            "exchange_timestamp_regression_count": 0,
            "exchange_clock_valid_row_count": 950,
            "exchange_clock_invalid_row_count": 50,
            "exchange_clock_invalid_reason_counts": {"missing_exchange_timestamp:submit": 50},
        },
        "partial_fill_contract": {
            "partial_fill_count": 25,
            "spell_boundary_count": 25,
            "positive_remaining_new_spell_count": 25,
            "missing_new_spell_count": 0,
            "invalid_remaining_quantity_transition_count": 0,
            "last_grid_edge_preserved_on_reset": True,
        },
        "censoring_contract": {
            "explicit_local_shutdown_censor_count": 10,
            "local_shutdown_right_censored_count": 10,
            "legacy_shutdown_as_exchange_terminal_count": 0,
            "events_after_local_shutdown_censor_count": 0,
        },
        "left_truncation_contract": {
            "left_truncated_order_count": 4,
            "delayed_entry_order_count": 4,
            "missing_reason_count": 0,
            "missing_entry_timestamp_count": 0,
            "entry_rule": "delayed_entry_at_first_observation",
        },
        "lockstep_contract": {
            "chronological_panel_manifest_path": "/authority/f07-panel.json",
            "chronological_panel_manifest_sha256": _sha("panel"),
            "chronological_days_required": 40,
            "python_panel_builder_sha256": _sha("py-panel"),
            "cpp_panel_builder_sha256": _sha("cpp-panel"),
            "python_cif_kernel_sha256": hashlib.sha256(KERNEL_PATH.read_bytes()).hexdigest(),
            "cpp_cif_kernel_sha256": _sha("cpp-cif"),
            "event_lockstep_runner_sha256": _sha("event-lockstep"),
            "checkpoint_resume_runner_sha256": _sha("checkpoint-lockstep"),
        },
        "outcome_access": {
            "pnl_read": False,
            "reward_read": False,
            "markout_read": False,
            "campaign_terminal_value_read": False,
            "q90_action_enabled": False,
        },
    }


def test_fully_bound_metadata_is_ready_but_grants_no_authority() -> None:
    manifest = _ready_manifest()
    report = run_active_order_cif_training_preflight(
        manifest,
        _ready_metadata(manifest),
    )

    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["blockers"] == []
    assert all(report["gates"].values())
    assert all(value is False for value in report["permissions"].values())
    assert report["bounded_inputs"] == {
        "baseline_epoch_manifest_only": True,
        "lifecycle_journal_schema_metadata_only": True,
        "raw_lifecycle_rows_read": False,
        "economic_outcomes_read": False,
        "chronological_day_limit": 400,
        "baseline_epoch_limit": 512,
    }


def test_current_live_v1_fails_closed_with_exact_runtime_and_artifact_gaps() -> None:
    manifest = _ready_manifest()
    metadata = current_unauthoritative_live_journal_metadata(manifest)
    report = run_active_order_cif_training_preflight(manifest, metadata)

    assert report["gates"]["baseline_epoch_ready"] is True
    assert report["gates"]["lifecycle_data_admission_ready"] is False
    assert report["gates"]["training_identity_registration_ready"] is False
    assert report["gates"]["chronological_python_cpp_lockstep_execution_ready"] is False
    assert "journal.schema_version_is_not_v2" in report["blockers"]
    assert "journal.columns_do_not_match_v2" in report["blockers"]
    assert report["missing_runtime_integrations"] == [
        "all_unseen_lifecycle_event_batch_emission",
        "atomic_callback_batch_commit",
        "durable_lifecycle_cursor_checkpoint",
        "lifecycle_writer_health_accounting",
        "live_order_manager_order_lifecycle_journal_v2_batch_emitter",
        "replay_order_manager_order_lifecycle_journal_v2_batch_emitter",
    ]
    assert {
        "authoritative_order_lifecycle_journal_v2_tape",
        "order_lifecycle_journal_v2_admission_manifest",
        "order_lifecycle_journal_v2_health_manifest",
        "frozen_f07_chronological_40_day_panel_manifest",
        "frozen_full_fill_cause_classifier",
        "cpp_f07_cif_panel_builder",
        "cpp_active_order_competing_risk_cif_kernel",
        "python_cpp_event_lockstep_runner",
        "python_cpp_checkpoint_resume_lockstep_runner",
    }.issubset(report["missing_artifacts"])


def test_partial_fill_spell_and_local_shutdown_semantics_fail_closed() -> None:
    manifest = _ready_manifest()
    metadata = _ready_metadata(manifest)
    metadata["partial_fill_contract"]["positive_remaining_new_spell_count"] = 24
    metadata["censoring_contract"]["legacy_shutdown_as_exchange_terminal_count"] = 1

    report = run_active_order_cif_training_preflight(manifest, metadata)

    assert "partial_fill.new_spell_count_mismatch" in report["blockers"]
    assert (
        "censoring.legacy_shutdown_as_exchange_terminal_count_not_zero_or_unknown"
        in report["blockers"]
    )
    assert report["gates"]["lifecycle_data_admission_ready"] is False


def test_dual_clock_left_truncation_and_grid_are_explicit_gates() -> None:
    manifest = _ready_manifest()
    metadata = _ready_metadata(manifest)
    metadata["grid_contract"]["interval_ms"] = 200
    metadata["dual_clock_contract"]["exchange_timestamp_regression_count"] = 1
    metadata["left_truncation_contract"]["entry_rule"] = "treat_as_native_submit"

    report = run_active_order_cif_training_preflight(manifest, metadata)

    assert "grid.interval_is_not_100ms" in report["blockers"]
    assert "clocks.exchange_timestamp_regression_count_not_zero_or_unknown" in report["blockers"]
    assert "left_truncation.entry_rule_unsupported" in report["blockers"]


def test_competing_causes_and_lockstep_artifacts_are_not_optional() -> None:
    manifest = _ready_manifest()
    metadata = _ready_metadata(manifest)
    metadata["competing_risks"]["causes"] = ["fill", "cancel_ack"]
    metadata["lockstep_contract"]["cpp_cif_kernel_sha256"] = None

    report = run_active_order_cif_training_preflight(manifest, metadata)

    assert "causes.canonical_competing_risks_mismatch" in report["blockers"]
    assert "lockstep.cpp_cif_kernel_sha256_missing" in report["blockers"]
    assert report["gates"]["chronological_python_cpp_lockstep_execution_ready"] is False


def test_python_kernel_identity_must_match_current_frozen_implementation() -> None:
    manifest = _ready_manifest()
    metadata = _ready_metadata(manifest)
    metadata["lockstep_contract"]["python_cif_kernel_sha256"] = "0" * 64

    report = run_active_order_cif_training_preflight(manifest, metadata)

    assert "lockstep.python_cif_kernel_sha256_mismatch" in report["blockers"]
    assert report["gates"]["chronological_python_cpp_lockstep_execution_ready"] is False


def test_outcome_access_and_q90_action_are_always_rejected() -> None:
    manifest = _ready_manifest()
    metadata = _ready_metadata(manifest)
    metadata["outcome_access"]["markout_read"] = True
    metadata["outcome_access"]["q90_action_enabled"] = True

    report = run_active_order_cif_training_preflight(manifest, metadata)

    assert "permissions.outcome_or_q90_access_detected" in report["blockers"]
    assert report["gates"]["lifecycle_data_admission_ready"] is False


def test_exact_metadata_schema_rejects_hidden_label_or_outcome_surface() -> None:
    manifest = _ready_manifest()
    metadata = _ready_metadata(manifest)
    metadata["hidden_target"] = "future_fill_quality"

    with pytest.raises(ActiveOrderCIFPreflightError, match="schema mismatch"):
        run_active_order_cif_training_preflight(manifest, metadata)


def test_partial_baseline_epoch_manifest_blocks_data_admission() -> None:
    manifest = _ready_manifest()
    epoch = manifest["epochs"][0]
    epoch["identity"]["clock_semantics_sha256"] = None
    epoch["identity_sha256"] = epoch_identity_sha256(epoch["identity"])
    epoch["binding_status"] = "partially_bound"
    epoch["lifecycle_estimation_authorized"] = False
    manifest.pop("canonical_manifest_sha256")
    manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
    metadata = _ready_metadata(manifest)

    report = run_active_order_cif_training_preflight(manifest, metadata)

    assert "baseline.no_lifecycle_authorized_epochs" in report["blockers"]
    assert "baseline.partially_bound_epochs_present" in report["blockers"]
    assert report["gates"]["baseline_epoch_ready"] is False
    assert report["gates"]["lifecycle_data_admission_ready"] is False


def test_report_hash_is_deterministic_and_sensitive_to_metadata() -> None:
    manifest = _ready_manifest()
    metadata = _ready_metadata(manifest)
    first = run_active_order_cif_training_preflight(manifest, metadata)
    second = run_active_order_cif_training_preflight(manifest, copy.deepcopy(metadata))
    assert first["canonical_report_sha256"] == second["canonical_report_sha256"]

    changed = copy.deepcopy(metadata)
    changed["row_count"] = 1_001
    third = run_active_order_cif_training_preflight(manifest, changed)
    assert first["canonical_report_sha256"] != third["canonical_report_sha256"]


def test_design_contract_binds_implementation_and_keeps_authority_closed() -> None:
    design = json.loads(DESIGN_JSON_PATH.read_text(encoding="utf-8"))
    implementation = design["implementation_identity"]

    assert design["identity"] == "active_order_cif_training_preflight_v1"
    assert design["last_materially_modified"] == "2026-08-04"
    assert implementation["sha256"] == hashlib.sha256(PREFLIGHT_PATH.read_bytes()).hexdigest()
    assert design["current_live_v1_audit"]["lifecycle_data_admission_ready"] is False
    assert all(value is False for value in design["permissions"].values())

    text = DESIGN_MD_PATH.read_text(encoding="utf-8")
    assert "Last materially modified: 2026-08-12" in text
    assert implementation["sha256"] in text
    assert "current live v1 journal" in text
    assert "grants no panel-generation, model-training, q90 action" in text
