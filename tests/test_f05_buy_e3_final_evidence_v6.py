from __future__ import annotations

import subprocess
import sys
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_final_evidence_v6 as subject


def _content(
    schema: str,
    status: str | None,
    file_marker: str,
    canonical_marker: str,
    *,
    canonical_field: str = "canonical_fixture_sha256",
    size_bytes: int = 100,
    mode: str = "0600",
) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "status": status,
        "file_sha256": file_marker * 64,
        "canonical_field": canonical_field,
        "canonical_sha256": canonical_marker * 64,
        "size_bytes": size_bytes,
        "mode": mode,
    }


@pytest.fixture(autouse=True)
def _freeze_scaffold(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    admission = _content(
        subject.transport_v6.ADMISSION_SCHEMA,
        subject.transport_v6.ADMISSION_STATUS,
        "7",
        "8",
        canonical_field=subject.transport_v6.ADMISSION_CANONICAL_FIELD,
    )
    lifecycle = _content(
        subject.base.LIFECYCLE_SCHEMA,
        None,
        "b",
        "c",
        canonical_field="admission_identity_sha256",
        mode="0644",
    )
    post = _content(
        subject.POST_LIFECYCLE_RECEIPT_SCHEMA,
        subject.POST_LIFECYCLE_RECEIPT_STATUS,
        "d",
        "e",
        canonical_field=subject.POST_LIFECYCLE_RECEIPT_CANONICAL_FIELD,
    )
    context = _content(
        subject.lifecycle_context_v1.SCHEMA_VERSION,
        subject.lifecycle_context_v1.STATUS,
        "f",
        "a",
        canonical_field=subject.lifecycle_context_v1.CANONICAL_FIELD,
    )
    monkeypatch.setattr(subject, "FROZEN_CROSS_HOST_ADMISSION_CONTENT", admission)
    monkeypatch.setattr(
        subject,
        "FROZEN_CROSS_HOST_ADMISSION_PATH_PROVENANCE",
        str(tmp_path / "cross_host_admission.json"),
    )
    monkeypatch.setattr(subject, "FROZEN_CURRENT_LIFECYCLE_CONTENT", lifecycle)
    monkeypatch.setattr(
        subject,
        "FROZEN_CURRENT_LIFECYCLE_PATH_PROVENANCE",
        str(tmp_path / "lifecycle_admission.json"),
    )
    monkeypatch.setattr(subject, "FROZEN_CURRENT_LIFECYCLE_EPOCH_ID", "prospective-current-v6")
    monkeypatch.setattr(subject, "FROZEN_LIFECYCLE_CONTEXT_CONTENT", context)
    monkeypatch.setattr(
        subject,
        "FROZEN_LIFECYCLE_CONTEXT_PATH_PROVENANCE",
        str(tmp_path / "lifecycle_context.json"),
    )
    monkeypatch.setattr(subject, "FROZEN_POST_LIFECYCLE_HEALTH_CONTENT", post)
    monkeypatch.setattr(
        subject,
        "FROZEN_POST_LIFECYCLE_HEALTH_PATH_PROVENANCE",
        str(tmp_path / "post_lifecycle_health.json"),
    )


def _release_bundle() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    execution = subject._module_contract()  # noqa: SLF001
    roles = {
        role: _content(
            f"fixture.{role}.v1",
            None if role == "predicate_bundle" else "frozen",
            str(index + 1),
            str(index + 4),
            canonical_field=(
                "artifact_sha256" if role == "manifest" else f"canonical_{role}_sha256"
            ),
            size_bytes=200 + index,
        )
        for index, role in enumerate(("manifest", "policy", "predicate_bundle"))
    }
    release = {
        "action_authorized": True,
        "live_authorized": True,
        "scope": {"side": "BUY", "sell_owner_policy_unchanged": True},
        "rollback": {"buy_e3_enabled": False, "buy_deadline_identity": "B0"},
        "exact_artifact": {
            "artifact_sha256": subject.transport_v6.FROZEN_FINAL_ARTIFACT_SHA256,
            "roles": roles,
        },
    }
    binding = _content(
        subject.resource_v8.DIRECT_SUCCESSOR_RELEASE_SCHEMA,
        subject.resource_v8.DIRECT_SUCCESSOR_RELEASE_STATUS,
        subject.resource_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256[0],
        subject.resource_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256[0],
        canonical_field="canonical_active_release_sha256",
        size_bytes=777,
    )
    binding["file_sha256"] = subject.resource_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
    binding["canonical_sha256"] = subject.resource_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
    artifact = subject.transport_v6._artifact_projection(release)  # noqa: SLF001
    return release, binding, execution, artifact


def _shadow_row() -> dict[str, Any]:
    numeric = {
        "externalSources",
        *subject.resource_v8.GLOBAL_FLOW_STATE_ZERO_FIELDS,
        *subject.resource_v8.GLOBAL_FLOW_VALUE_ZERO_FIELDS,
        *subject.resource_v8.GLOBAL_REFERENCE_ZERO_FIELDS,
        *subject.resource_v8.GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
        *subject.resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
    }
    return {
        **{name: 0 for name in numeric},
        "globalFlowReason": subject.resource_v8.SHADOW_DISABLED_REASON,
        "globalRefReason": subject.resource_v8.SHADOW_DISABLED_REASON,
    }


def _startup_shadow() -> dict[str, Any]:
    backend = {
        name: 0
        for name in (
            "native",
            "market_count",
            "trade_batches",
            "trade_events_seen",
            "trade_events_accepted",
            "book_events_seen",
            "book_events_accepted",
            "out_of_order_events",
            "stale_trade_events",
            "trade_overflow_events",
            "book_overflow_events",
        )
    }
    identity = {
        "schema_version": "narrowgate_shadow_runtime_identity.v1",
        "global_flow_shadow_enabled": False,
        "global_reference_shadow_enabled": False,
        "global_flow_native_requested": True,
        "global_flow_native_effective": False,
        "global_flow_backend": backend,
        "global_reference_bridge_basis_sample_count": 0,
        "state_restore_contract": "shadow_state_never_restored",
        "global_flow_shadow_config_explicit": True,
        "global_reference_shadow_config_explicit": True,
    }
    return {
        "identity": identity,
        "identity_sha256": subject._canonical_sha256(identity),  # noqa: SLF001
        "all_shadow_evaluators_disabled": True,
        "all_global_flow_backend_fields_absolute_zero": True,
        "global_reference_basis_samples_absolute_zero": True,
    }


def _health_window(
    *,
    start_s: float = 1_787_543_000.0,
    updates: tuple[int, int] = (10, 20),
    post_lifecycle: bool = False,
) -> dict[str, Any]:
    counters = {name: 0 for name in subject.resource_v8.WINDOW_ZERO_COUNTERS[:-2]}
    rows = []
    for index, (wall_s, update_count) in enumerate(
        zip((start_s, start_s + 60.0), updates, strict=True), start=1
    ):
        row = {
            "fresh_generation": index,
            "line_offset_bytes": 100 * index,
            "line_size_bytes": 50,
            "line_sha256": str(index) * 64,
            "main_wall_timestamp_s": wall_s,
            "projection": {
                "boolean_cooldown_enabled": 1,
                "boolean_cooldown_updates": update_count,
                "buy_e3_enabled": 1,
                "deep_book_buffer": 0,
                "shadow_disabled_state": _shadow_row(),
                "counter_values": dict(counters),
            },
        }
        if post_lifecycle:
            row["readiness"] = {
                "runtime_loaded": True,
                "warmup_time_admitted": True,
                "completed_windows": update_count,
                "gap_resets": 0,
                "resets": 0,
                "invalid_updates": 0,
                "economic_outcome_claimed": False,
            }
        rows.append(row)
    return {
        "schema_version": subject.ACTIVE_HEALTH_WINDOW_SCHEMA,
        "status": subject.ACTIVE_HEALTH_WINDOW_STATUS,
        "boundary_offset_bytes": 0,
        "active_pid": 20,
        "active_pid_start_ticks": 200,
        "active_process_stable_identity_sha256": "f" * 64,
        "rows": rows,
        "checks": {
            "constructor_boundary_only": True,
            "two_consecutive_fresh_main_health_rows": True,
            (
                "same_pid_and_start_ticks_before_after_poll_and_each_health_row"
                if post_lifecycle
                else "same_pid_and_start_ticks_before_between_after"
            ): True,
            "sell_owner_enabled_both_rows": True,
            "buy_e3_enabled_both_rows": True,
            "external_sources_absolute_zero_both_rows": True,
            "global_flow_explicit_disabled_error_and_backend_zero_both_rows": True,
            "global_reference_explicit_disabled_error_and_state_zero_both_rows": True,
        },
    }


def _portable() -> dict[str, Any]:
    release, release_binding, execution, artifact = _release_bundle()
    del release
    shadow = _shadow_row()
    resource_shadow = {
        "baseline": shadow,
        "final": deepcopy(shadow),
        "baseline_manifest_sha256": subject._canonical_sha256(shadow),  # noqa: SLF001
        "final_manifest_sha256": subject._canonical_sha256(shadow),  # noqa: SLF001
        "all_numeric_fields_absolute_zero": True,
        "disabled_reason_exact": True,
    }
    receipts = {
        "config_correction": {
            **_content(
                subject.resource_v8.config_successor.SCHEMA_VERSION,
                subject.resource_v8.config_successor.STATUS,
                "1",
                "2",
                canonical_field=subject.resource_v8.config_successor.CANONICAL_FIELD,
            ),
            "local_filename": subject.transport_v6.CONFIG_CORRECTION_FILENAME,
        },
        "current_host_resource_gate": {
            **_content(
                subject.resource_v8.RESOURCE_SCHEMA,
                subject.resource_v8.RESOURCE_STATUS,
                "3",
                "4",
                canonical_field=subject.resource_v8.RESOURCE_CANONICAL_FIELD,
            ),
            "local_filename": subject.transport_v6.RESOURCE_FILENAME,
        },
        "active_process_capture": {
            **_content(
                subject.ACTIVE_CAPTURE_SCHEMA_V7,
                subject.ACTIVE_CAPTURE_STATUS_V7,
                "5",
                "6",
                canonical_field=subject.active_capture_v8.CANONICAL_FIELD,
            ),
            "local_filename": subject.transport_v6.ACTIVE_CAPTURE_FILENAME,
        },
        "remote_active_attestation": {
            **_content(
                subject.transport_v6.REMOTE_ATTESTATION_SCHEMA,
                subject.transport_v6.REMOTE_ATTESTATION_STATUS,
                "7",
                "8",
                canonical_field=subject.transport_v6.REMOTE_ATTESTATION_CANONICAL_FIELD,
            ),
            "local_filename": subject.transport_v6.REMOTE_ATTESTATION_FILENAME,
        },
    }
    receipts["config_correction"]["file_sha256"] = (
        subject.transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256
    )
    receipts["config_correction"]["canonical_sha256"] = (
        subject.transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256
    )
    receipts["current_host_resource_gate"]["file_sha256"] = (
        subject.transport_v6.FROZEN_FINAL_RESOURCE_FILE_SHA256
    )
    receipts["current_host_resource_gate"]["canonical_sha256"] = (
        subject.transport_v6.FROZEN_FINAL_RESOURCE_CANONICAL_SHA256
    )
    receipts["active_process_capture"]["file_sha256"] = (
        subject.transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256
    )
    receipts["active_process_capture"]["canonical_sha256"] = (
        subject.transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256
    )
    return {
        "host": {
            "provider": subject.transport_v6.CURRENT_PROVIDER,
            "region": subject.transport_v6.CURRENT_REGION,
            "instance_id": subject.transport_v6.CURRENT_INSTANCE_ID,
            "instance_type": subject.transport_v6.CURRENT_INSTANCE_TYPE,
            "public_ipv4": subject.transport_v6.CURRENT_PUBLIC_IPV4_PROVENANCE,
            "public_ipv4_role": "network_locator_provenance_only_not_host_authority",
            "resource_host_identity": {"fixture": "host"},
        },
        "runtime_execution": execution,
        "runtime_authority": {
            **release_binding,
            "execution": execution,
            "runtime_authority": True,
        },
        "exact_artifact": artifact,
        "resource_disabled_process": {
            "pid": 10,
            "pid_start_ticks": 100,
            "process_identity_sha256": "e" * 64,
            "config_sha256": subject.transport_v6.FROZEN_FINAL_DISABLED_CONFIG_SHA256,
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
            "runtime_source_manifest_sha256": (
                subject.EXPECTED_RESOURCE_RUNTIME_SOURCE_MANIFEST_SHA256
            ),
            "runtime_source_files": dict(subject.EXPECTED_RESOURCE_RUNTIME_SOURCE_SHA256),
            "shadow_runtime": resource_shadow,
        },
        "transition": {
            "disabled_pid": 10,
            "disabled_pid_start_ticks": 100,
            "active_pid": 20,
            "active_pid_start_ticks": 200,
            "active_process_identity_sha256": "d" * 64,
            "fresh_disabled_to_active_restart": True,
        },
        "active_runtime": {
            "config_sha256": subject.transport_v6.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
            "runtime_identity": {
                "schema_version": "narrowgate_live_runtime_identity.v1",
                "file_sha256": "b" * 64,
                "canonical_sha256": "c" * 64,
            },
            "startup_attestation": {
                "schema_version": "narrowgate_buy_e3_startup_attestation.v5",
                "status": "accepted",
                "canonical_sha256": "a" * 64,
            },
            "runtime_source_manifest_sha256": (
                subject.EXPECTED_ACTIVE_RUNTIME_SOURCE_MANIFEST_SHA256
            ),
            "runtime_source_files": dict(subject.EXPECTED_ACTIVE_RUNTIME_SOURCE_SHA256),
            "artifact_sha256": subject.transport_v6.FROZEN_FINAL_ARTIFACT_SHA256,
            "buy_e3_enabled": True,
            "owner_override_effective": True,
            "startup_semantics": {
                "startup_status": "accepted",
                "running_checkout_commit": subject.CURRENT_EXECUTION_COMMIT,
                "running_checkout_tree": subject.CURRENT_EXECUTION_TREE,
                "shadow_runtime": _startup_shadow(),
            },
            "active_health_window": _health_window(),
        },
        "source_receipts": receipts,
    }


def _history_payload() -> dict[str, Any]:
    attempts = subject.failed_history_v1._attempts(  # noqa: SLF001
        v6_binding=dict(subject.failed_history_v1.V6_WRONG_ROUTE_BENCHMARK),
        v7_attempt2_binding=dict(subject.failed_history_v1.V7_ATTEMPT2_BENCHMARK),
    )
    payload = {
        "schema_version": subject.FAILED_ACTIVATION_ATTEMPT_HISTORY_SCHEMA,
        "identity": subject.OWNER,
        "status": subject.FAILED_ACTIVATION_ATTEMPT_HISTORY_STATUS,
        "generated_utc": "2026-08-24T10:00:00Z",
        "failed_activation_source": dict(subject.failed_history_v1.FAILED_ACTIVATION_SOURCE),
        "failed_activation_projection": {
            "source_reported_unadmitted_session_token": (
                subject.failed_history_v1.FAILED_SESSION_TOKEN
            ),
            "attempted_runtime": {
                "execution_commit": "1" * 40,
                "execution_tree": "2" * 40,
                "config_sha256": "3" * 64,
                "pid": 57_696,
                "pid_start_ticks": 3_071_624,
            },
            "rejection": {
                "error_count": 1,
                "drop_count": 0,
                "exchange_error_code": -5022,
                "formal_collection_valid": False,
                "formal_admission_allowed": False,
            },
            "epoch_established": False,
            "runtime_authority": False,
            "evidence_authority": False,
            "reusable_for_current": False,
        },
        "resource_gate_attempts": attempts,
        "summary": {
            "failed_attempt_count": 5,
            "admitted_epoch_count": 0,
            "resource_receipt_count": 0,
            "active_process_started_in_resource_attempts": False,
            "fail_closed_without_retry_or_relaxation": True,
            "current_runtime_authority_derived_from_history": False,
        },
        "checks": {
            "misnamed_epoch_source_reclassified_as_unadmitted_session_token": True,
            "failed_activation_source_exact_file_and_canonical": True,
            "v6_wrong_route_benchmark_exact_file_and_canonical": True,
            "v7_attempt1_not_misrepresented_as_exact7": True,
            "v7_attempt2_benchmark_exact_file_and_canonical": True,
            "all_failed_resource_receipts_absent": True,
            "no_failed_attempt_reused_for_current": True,
        },
        "authority_design": dict(subject.failed_history_v1.AUTHORITY_DESIGN),
        "permissions": dict(subject.failed_history_v1.PERMISSIONS),
        "evidence_boundary": dict(subject.failed_history_v1.EVIDENCE_BOUNDARY),
    }
    field = subject.RESOURCE_ATTEMPT_REJECTION_HISTORY_CONTENT["canonical_field"]
    payload[field] = subject._document_sha256(payload, field)  # noqa: SLF001
    return payload


def _lifecycle_bundle() -> tuple[dict[str, Any], dict[str, Any]]:
    payload = {"admitted_ts_ns": 1_787_547_000_000_000_000}
    frozen = subject.FROZEN_CURRENT_LIFECYCLE_CONTENT
    binding = {
        **dict(frozen),
        "path": subject.FROZEN_CURRENT_LIFECYCLE_PATH_PROVENANCE,
        "baseline_epoch_id": subject.FROZEN_CURRENT_LIFECYCLE_EPOCH_ID,
        "config_sha256": subject.transport_v6.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
        "runtime_code_sha256": subject.lifecycle_context_v1.RUNTIME_CODE_SHA256,
        "runtime_code_files": dict(subject.EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256),
        "required_action_state": {
            "strategy.buy_e3_cooldown_policy_enabled": True,
            "strategy.buy_fill_selection_live_enabled": False,
            "strategy.buy_fill_selection_shadow_enabled": False,
            "strategy.dynamic_fill_hazard_action_enabled": False,
            "strategy.dynamic_fill_hazard_shadow_enabled": False,
            "logging.inventory_campaign_shadow_enabled": False,
        },
        "epoch_start_ts_ns": 1_787_542_000_000_000_000,
        "session_id": "fixture-session",
        "action_enablement_sha256": "2" * 64,
        "writer_runtime_identity_sha256": "3" * 64,
        "writer_identity_file_sha256": "4" * 64,
        "epoch_manifest_file_sha256": "5" * 64,
        "identity_evidence_file_sha256": "6" * 64,
    }
    return payload, binding


def _portable_lifecycle_context_payload(
    lifecycle_payload: dict[str, Any],
    lifecycle_binding: dict[str, Any],
) -> dict[str, Any]:
    projection = {
        "admitted_ts_ns": lifecycle_payload["admitted_ts_ns"],
        "session_id": lifecycle_binding["session_id"],
        "baseline_epoch_id": lifecycle_binding["baseline_epoch_id"],
        "config_sha256": lifecycle_binding["config_sha256"],
        "runtime_code_sha256": subject.lifecycle_context_v1.RUNTIME_CODE_SHA256,
        "runtime_code_schema_version": subject.lifecycle_context_v1.RUNTIME_CODE_SCHEMA,
        "runtime_source_files": dict(subject.EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256),
        "runtime_source_file_count": 65,
        "runtime_source_files_canonical_sha256": (
            subject.lifecycle_context_v1.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
        ),
        "action_enablement_sha256": lifecycle_binding["action_enablement_sha256"],
        "epoch_start_ts_ns": lifecycle_binding["epoch_start_ts_ns"],
        "writer_runtime_identity_sha256": lifecycle_binding["writer_runtime_identity_sha256"],
        "writer_identity_file_sha256": lifecycle_binding["writer_identity_file_sha256"],
        "epoch_manifest_file_sha256": lifecycle_binding["epoch_manifest_file_sha256"],
        "identity_evidence_file_sha256": lifecycle_binding["identity_evidence_file_sha256"],
        "safe_action_state": dict(subject.lifecycle_context_v1.SAFE_ACTION_STATE),
        "action_shadow_enabled_state": dict(
            subject.lifecycle_context_v1.SAFE_ACTION_SHADOW_ENABLED_STATE
        ),
        "external_shadow_only_inert": True,
        "data_source_identity_sha256": "7" * 64,
        "external_source_recording_state": deepcopy(
            subject.lifecycle_context_v1.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        ),
        "external_source_count": len(
            subject.lifecycle_context_v1.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        ),
        "source_settings_inert_because_external_master_false": True,
        "record_trades_inert_because_master_false_and_record_enabled_false": True,
        "external_effective_stream_and_recording_disabled": True,
    }
    payload = {
        "schema_version": subject.lifecycle_context_v1.SCHEMA_VERSION,
        "identity": subject.OWNER,
        "status": subject.lifecycle_context_v1.STATUS,
        "generated_utc": _iso(1_787_547_010.0),
        "lifecycle_admission": {
            field: lifecycle_binding[field] for field in subject.CONTENT_BINDING_FIELDS
        },
        "lifecycle_projection": projection,
        "runtime_execution": dict(subject.lifecycle_context_v1.RUNTIME_EXECUTION),
        "checks": dict(subject.lifecycle_context_v1.CHECKS),
        "permissions": dict(subject.lifecycle_context_v1.PERMISSIONS),
        "evidence_boundary": dict(subject.lifecycle_context_v1.EVIDENCE_BOUNDARY),
    }
    payload[subject.lifecycle_context_v1.CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload,
        subject.lifecycle_context_v1.CANONICAL_FIELD,
    )
    return payload


def _iso(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat().replace("+00:00", "Z")


def _post_lifecycle_payload() -> dict[str, Any]:
    portable = _portable()
    lifecycle_payload, lifecycle_binding = _lifecycle_bundle()
    del lifecycle_payload
    active = portable["active_runtime"]
    transition = portable["transition"]
    runtime_identity = active["runtime_identity"]
    activation_health = active["active_health_window"]
    payload = {
        "schema_version": subject.POST_LIFECYCLE_HEALTH_SCHEMA,
        "status": subject.POST_LIFECYCLE_HEALTH_STATUS,
        "generated_utc": _iso(1_787_547_300.0),
        "runtime_execution": dict(portable["runtime_execution"]),
        "runtime_authority": dict(portable["runtime_authority"]),
        "active_process": {
            "pid": transition["active_pid"],
            "pid_start_ticks": transition["active_pid_start_ticks"],
            "process_identity_sha256": transition["active_process_identity_sha256"],
            "stable_process_identity_sha256": activation_health[
                "active_process_stable_identity_sha256"
            ],
            "config_sha256": active["config_sha256"],
            "runtime_identity_file_sha256": runtime_identity["file_sha256"],
            "runtime_identity_canonical_sha256": runtime_identity["canonical_sha256"],
            "runtime_source_manifest_sha256": active["runtime_source_manifest_sha256"],
            "runtime_source_files": dict(active["runtime_source_files"]),
            "release_file_sha256": portable["runtime_authority"]["file_sha256"],
            "release_canonical_sha256": portable["runtime_authority"]["canonical_sha256"],
        },
        "lifecycle_admission": {
            field: lifecycle_binding[field] for field in subject.CONTENT_BINDING_FIELDS
        },
        "lifecycle_epoch_id": lifecycle_binding["baseline_epoch_id"],
        "main_health_window": _health_window(
            start_s=1_787_547_100.0,
            updates=(30, 40),
            post_lifecycle=True,
        ),
        "lifecycle_health": {
            "observed_utc": _iso(1_787_547_200.0),
            "line_sha256": "9" * 64,
            "order_lifecycle_v2_drops": 0,
            "order_lifecycle_v2_errors": 0,
        },
        "operational_aggregates": {
            "resource": {
                "sample_count": 2,
                "min_mem_available_mib": 1024.0,
                "max_live_rss_mib": 100.0,
                "oom_window_delta": 0,
                "swap_in_window_delta": 0,
                "swap_out_window_delta": 0,
            },
            "latency": {
                "decision_sample_count": 1,
                "decision_p99_us": 1234.0,
                "lifecycle_enqueue_p99_us": 50.0,
                "lifecycle_write_p99_ms": 0.2,
                "small_sample_disclosed": True,
                "strategy_result_authority": False,
                "formal_performance_authority": False,
                "resource_v8_formal_gate_unchanged": True,
                "economic_outcome_claimed": False,
            },
            "position": {
                "main_health_position_projection_completed": True,
                "reported_aggregate_position_flat": False,
                "reported_open_order_count": 1,
                "economic_values_persisted": False,
            },
        },
        "lifecycle_process_cross_binding": dict(
            subject.post_lifecycle_v1.LIFECYCLE_PROCESS_CROSS_BINDING
        ),
        "checks": {
            "same_active_pid_start_config_release_runtime": True,
            "snapshot_after_lifecycle_admission": True,
            "two_fresh_post_lifecycle_main_health_rows": True,
            "buy_e3_and_sell_owner_enabled": True,
            "buy_e3_runtime_loaded_and_warmup_time_admitted": True,
            "buy_e3_completed_windows_and_updates_strictly_increase": True,
            "buy_e3_gap_resets_resets_invalid_absolute_zero": True,
            "decision_count_and_latency_disclosed_without_promotion_authority": True,
            "resource_v8_formal_gate_unchanged": True,
            "economic_outcome_claimed": False,
            "external_sources_absolute_zero": True,
            "global_flow_explicit_disabled_error_and_backend_zero": True,
            "global_reference_explicit_disabled_error_and_state_zero": True,
            "lifecycle_drop_error_zero": True,
            "operational_aggregates_only": True,
            "economic_values_persisted": False,
        },
        "permissions": dict(subject.NO_NEW_AUTHORITY),
        "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
    }
    payload[subject.POST_LIFECYCLE_HEALTH_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.POST_LIFECYCLE_HEALTH_CANONICAL_FIELD
    )
    return payload


def test_five_current_receipt_layers_are_v6_and_module_routes_are_literal() -> None:
    assert {
        subject.ENVELOPE_SCHEMA,
        subject.COMPLETION_SCHEMA,
        subject.COMPOSITION_SCHEMA,
        subject.ATTEMPT_FINAL_SCHEMA,
        subject.EVIDENCE_RELEASE_SCHEMA,
    } == {
        f"{subject.OWNER}.cross_host_activation_envelope.v6",
        f"{subject.OWNER}.cross_host_operational_evidence_completion.v6",
        f"{subject.OWNER}.cross_host_final_composition_receipt.v6",
        f"{subject.OWNER}.cross_host_operational_attempt_final_receipt.v6",
        f"{subject.OWNER}.cross_host_proof_evidence_release.v6",
    }
    assert subject.transport_v6.__name__ == subject.TRANSPORT_MODULE
    assert subject.resource_v8.__name__ == subject.RESOURCE_MODULE
    assert subject.active_capture_v8.__name__ == subject.ACTIVE_CAPTURE_MODULE
    assert subject.lifecycle_context_v1.__name__ == subject.LIFECYCLE_CONTEXT_MODULE
    assert subject.ACTIVE_CAPTURE_SCHEMA_V7.endswith(".v7")
    assert subject._module_contract()["execution_commit"] == subject.CURRENT_EXECUTION_COMMIT  # noqa: SLF001


def test_module_contract_cold_subprocess_without_monkeypatch() -> None:
    completed = subprocess.run(
        (
            sys.executable,
            "-c",
            (
                "from scripts import f05_buy_e3_final_evidence_v6 as subject; "
                "print(subject._module_contract()['execution_commit'])"
            ),
        ),
        cwd=Path.cwd(),
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == subject.CURRENT_EXECUTION_COMMIT


def test_wrong_lifecycle_context_module_route_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subject.lifecycle_context_v1,
        "__name__",
        "scripts.f05_buy_e3_lifecycle_context_v0",
    )
    with pytest.raises(subject.FinalEvidenceV6Error, match="wrong module"):
        subject._module_contract()  # noqa: SLF001


def test_actual_hash_and_path_placeholders_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    missing = dict(subject.FROZEN_POST_LIFECYCLE_HEALTH_CONTENT)
    missing["file_sha256"] = ""
    monkeypatch.setattr(subject, "FROZEN_POST_LIFECYCLE_HEALTH_CONTENT", missing)
    with pytest.raises(subject.FinalEvidenceV6Error, match="file SHA256"):
        subject._frozen_content(missing, "post lifecycle")  # noqa: SLF001
    monkeypatch.setattr(subject, "FROZEN_POST_LIFECYCLE_HEALTH_PATH_PROVENANCE", "")
    with pytest.raises(subject.FinalEvidenceV6Error, match="not absolute"):
        subject._frozen_path("", "post lifecycle")  # noqa: SLF001


@pytest.mark.parametrize(
    ("target", "name", "value", "message"),
    (
        ("transport", "__name__", "scripts.f05_buy_e3_cross_host_transport_v5", "wrong module"),
        ("transport", "resource_v8", object(), "wrong module"),
        (
            "resource",
            "RESOURCE_SCHEMA",
            "fixture.current_host_concurrent_resource_gate.v7",
            "resource-v8",
        ),
        ("active", "SCHEMA_VERSION", "fixture.active_process_capture.v6", "active schema-v7"),
        (
            "transport",
            "FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA",
            "fixture.active.v6",
            "active schema-v7",
        ),
        ("transport", "FROZEN_FINAL_EXECUTION_COMMIT", "0" * 40, "not eacb"),
    ),
)
def test_wrong_transport_resource_or_active_route_rejected(
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    name: str,
    value: Any,
    message: str,
) -> None:
    module = {
        "transport": subject.transport_v6,
        "resource": subject.resource_v8,
        "active": subject.active_capture_v8,
    }[target]
    monkeypatch.setattr(module, name, value)
    with pytest.raises(subject.FinalEvidenceV6Error, match=message):
        subject._module_contract()  # noqa: SLF001


def _validate_portable(
    portable: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    release, release_binding, execution, artifact = _release_bundle()
    return subject._validate_portable_v6(  # noqa: SLF001
        portable,
        release=release,
        release_binding=release_binding,
        execution=execution,
        artifact=artifact,
    )


def test_portable_v6_accepts_only_exact_current_runtime_and_two_shadow_proofs() -> None:
    portable, no_shadow = _validate_portable(_portable())
    assert portable["runtime_execution"]["execution_commit"] == subject.CURRENT_EXECUTION_COMMIT
    assert portable["active_runtime"]["runtime_source_files"] == (
        subject.EXPECTED_ACTIVE_RUNTIME_SOURCE_SHA256
    )
    assert no_shadow["two_explicit_evaluators"] == {
        "global_flow": {
            "explicit_disabled": True,
            "state_error_zero": True,
            "backend_and_counters_absolute_zero": True,
        },
        "global_reference": {
            "explicit_disabled": True,
            "state_error_zero": True,
            "state_and_basis_absolute_zero": True,
        },
    }
    assert no_shadow["shadow_or_companion_collection_enabled"] is False


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("old_runtime", "runtime execution"),
        ("old_resource", "current_host_resource_gate identity"),
        ("old_active", "active_process_capture identity"),
        ("runtime_extra", "active runtime"),
        ("resource_manifest", "process transition"),
        ("active_manifest", "active runtime"),
        ("health_check_missing", "activation health identity"),
        ("health_update_reused", "row semantics"),
        ("global_flow_enabled", "explicit-disabled/error0/absolute0"),
        ("global_reference_error", "explicit-disabled/error0/absolute0"),
        ("external_source_nonzero", "explicit-disabled/error0/absolute0"),
    ),
)
def test_portable_v6_rejects_old_routes_reused_health_and_shadow_tamper(
    mutation: str,
    message: str,
) -> None:
    portable = _portable()
    if mutation == "old_runtime":
        portable["runtime_execution"]["execution_commit"] = "0" * 40
    elif mutation == "old_resource":
        portable["source_receipts"]["current_host_resource_gate"]["schema_version"] = (
            f"{subject.OWNER}.current_host_concurrent_resource_gate.v7"
        )
    elif mutation == "old_active":
        portable["source_receipts"]["active_process_capture"]["schema_version"] = (
            f"{subject.OWNER}.fresh_all_shadow_evaluators_disabled_active_process_capture.v6"
        )
    elif mutation == "runtime_extra":
        portable["active_runtime"]["runtime_source_files"]["unexpected.py"] = "0" * 64
    elif mutation == "resource_manifest":
        portable["resource_disabled_process"]["runtime_source_manifest_sha256"] = "0" * 64
    elif mutation == "active_manifest":
        portable["active_runtime"]["runtime_source_manifest_sha256"] = "0" * 64
    elif mutation == "health_check_missing":
        portable["active_runtime"]["active_health_window"]["checks"].pop(
            "constructor_boundary_only"
        )
    elif mutation == "health_update_reused":
        portable["active_runtime"]["active_health_window"]["rows"][1]["projection"][
            "boolean_cooldown_updates"
        ] = 10
    elif mutation == "global_flow_enabled":
        portable["active_runtime"]["active_health_window"]["rows"][1]["projection"][
            "shadow_disabled_state"
        ]["globalFlowShadowEnabled"] = 1
    elif mutation == "global_reference_error":
        portable["active_runtime"]["active_health_window"]["rows"][1]["projection"][
            "shadow_disabled_state"
        ]["globalRefStateError"] = 1
    else:
        portable["active_runtime"]["active_health_window"]["rows"][1]["projection"][
            "shadow_disabled_state"
        ]["externalSources"] = 1
    with pytest.raises(subject.FinalEvidenceV6Error, match=message):
        _validate_portable(portable)


def _recanonicalize_history(payload: dict[str, Any]) -> None:
    field = subject.RESOURCE_ATTEMPT_REJECTION_HISTORY_CONTENT["canonical_field"]
    payload[field] = subject._document_sha256(payload, field)  # noqa: SLF001


def test_failed_activation_history_is_one_exact_nonauthoritative_aggregate() -> None:
    observed = subject._validate_resource_attempt_rejection_history_payload(  # noqa: SLF001
        _history_payload()
    )
    assert set(observed["resource_gate_attempts"]) == {
        "resource_v5",
        "resource_v6",
        "resource_v7_attempt1",
        "resource_v7_attempt2",
    }
    assert observed["failed_activation_projection"]["epoch_established"] is False
    assert observed["failed_activation_projection"]["runtime_authority"] is False
    assert observed["failed_activation_projection"]["reusable_for_current"] is False


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("fake_epoch", "nonauthority"),
        ("fake_runtime_authority", "nonauthority"),
        ("v6_wrong_schema", "nonauthority"),
        ("v7_wrong_counter_name", "nonauthority"),
        ("old_misnamed_as_current", "nonauthority"),
        ("benchmark_tamper", "nonauthority"),
    ),
)
def test_failed_activation_history_rejects_fake_epochs_authority_and_tamper(
    mutation: str,
    message: str,
) -> None:
    payload = _history_payload()
    if mutation == "fake_epoch":
        payload["failed_activation_projection"]["epoch_established"] = True
    elif mutation == "fake_runtime_authority":
        payload["failed_activation_projection"]["runtime_authority"] = True
    elif mutation == "v6_wrong_schema":
        payload["resource_gate_attempts"]["resource_v6"]["benchmark"]["schema_version"] = (
            f"{subject.OWNER}.exact_four_file_host_benchmark.v5"
        )
    elif mutation == "v7_wrong_counter_name":
        payload["resource_gate_attempts"]["resource_v7_attempt1"]["failure_counter"] = (
            "globalFlowOutOfOrder"
        )
    elif mutation == "old_misnamed_as_current":
        payload["summary"]["current_runtime_authority_derived_from_history"] = True
    else:
        payload["resource_gate_attempts"]["resource_v7_attempt2"]["benchmark"]["file_sha256"] = (
            "0" * 64
        )
    _recanonicalize_history(payload)
    with pytest.raises(subject.FinalEvidenceV6Error, match=message):
        subject._validate_resource_attempt_rejection_history_payload(payload)  # noqa: SLF001


def _lifecycle_context(
    monkeypatch: pytest.MonkeyPatch,
    *,
    active_runtime: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    binding: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    default_payload, default_binding = _lifecycle_bundle()
    lifecycle_payload = default_payload if payload is None else payload
    lifecycle_binding = default_binding if binding is None else binding
    default_context_payload = _portable_lifecycle_context_payload(
        lifecycle_payload,
        lifecycle_binding,
    )
    context_payload = default_context_payload if context is None else context
    context_binding = {
        **dict(subject.FROZEN_LIFECYCLE_CONTEXT_CONTENT),
        "path": subject.FROZEN_LIFECYCLE_CONTEXT_PATH_PROVENANCE,
    }
    monkeypatch.setattr(
        subject.base,
        "_validate_lifecycle_admission",
        lambda _path: (lifecycle_payload, lifecycle_binding),
    )
    monkeypatch.setattr(
        subject.lifecycle_context_v1,
        "validate_lifecycle_context_against_admission",
        lambda *_args, **_kwargs: context_payload,
    )
    monkeypatch.setattr(
        subject,
        "_receipt_binding",
        lambda *_args, **_kwargs: context_binding,
    )
    runtime = _portable()["active_runtime"] if active_runtime is None else active_runtime
    return subject._lifecycle_context(  # noqa: SLF001
        Path(subject.FROZEN_CURRENT_LIFECYCLE_PATH_PROVENANCE),
        lifecycle_context_path=Path(subject.FROZEN_LIFECYCLE_CONTEXT_PATH_PROVENANCE),
        current_runtime_root=Path("/fixture/runtime"),
        active_runtime=runtime,
    )


def test_lifecycle_requires_new_epoch_0644_exact_config_and_runtime_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, binding, context, context_binding = _lifecycle_context(monkeypatch)
    assert payload["admitted_ts_ns"] > binding["epoch_start_ts_ns"]
    assert binding["mode"] == "0644"
    assert binding["baseline_epoch_id"] != subject.SUPERSEDED_V4_EPOCH_ID
    assert context_binding["mode"] == "0600"
    assert context["lifecycle_projection"]["runtime_source_file_count"] == 65
    assert (
        context["lifecycle_projection"]["external_effective_stream_and_recording_disabled"] is True
    )
    assert all(
        binding["runtime_code_files"][path] == digest
        for path, digest in subject.EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256.items()
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("old_epoch", "binding drifted or was reused"),
        ("wrong_config", "binding drifted or was reused"),
        ("runtime_source_tamper", "binding drifted or was reused"),
        ("active_source_extra", "binding drifted or was reused"),
        ("lifecycle_before_activation_health", "binding drifted or was reused"),
        ("mode_0600", "malformed"),
    ),
)
def test_lifecycle_rejects_old_reused_or_inexact_current_bindings(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    payload, binding = _lifecycle_bundle()
    active = _portable()["active_runtime"]
    if mutation == "old_epoch":
        binding["baseline_epoch_id"] = subject.SUPERSEDED_V4_EPOCH_ID
    elif mutation == "wrong_config":
        binding["config_sha256"] = "0" * 64
    elif mutation == "runtime_source_tamper":
        first_path = next(iter(subject.EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256))
        binding["runtime_code_files"][first_path] = "0" * 64
    elif mutation == "active_source_extra":
        active["runtime_source_files"]["unexpected.py"] = "0" * 64
    elif mutation == "lifecycle_before_activation_health":
        payload["admitted_ts_ns"] = 1_787_542_500_000_000_000
    else:
        frozen = dict(subject.FROZEN_CURRENT_LIFECYCLE_CONTENT)
        frozen["mode"] = "0600"
        binding["mode"] = "0600"
        monkeypatch.setattr(subject, "FROZEN_CURRENT_LIFECYCLE_CONTENT", frozen)
    with pytest.raises(subject.FinalEvidenceV6Error, match=message):
        _lifecycle_context(
            monkeypatch,
            active_runtime=active,
            payload=payload,
            binding=binding,
        )


@pytest.mark.parametrize(
    "mutation",
    ("context_source_extra", "shadow_flag_true", "record_enabled", "record_trades_hidden"),
)
def test_lifecycle_context_rejects_full65_shadow_or_recording_tamper(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    payload, binding = _lifecycle_bundle()
    context = _portable_lifecycle_context_payload(payload, binding)
    projection = context["lifecycle_projection"]
    if mutation == "context_source_extra":
        projection["runtime_source_files"]["unexpected.py"] = "0" * 64
    elif mutation == "shadow_flag_true":
        projection["action_shadow_enabled_state"]["depth_execution.shadow_enabled"] = True
    elif mutation == "record_enabled":
        projection["external_source_recording_state"][0]["record_enabled"] = True
    else:
        projection["external_source_recording_state"][0]["record_trades"] = False
    with pytest.raises(subject.FinalEvidenceV6Error, match="binding drifted or was reused"):
        _lifecycle_context(
            monkeypatch,
            payload=payload,
            binding=binding,
            context=context,
        )


def _validate_post_lifecycle(payload: dict[str, Any]) -> dict[str, Any]:
    portable = _portable()
    lifecycle_payload, lifecycle_binding = _lifecycle_bundle()
    return subject._validate_post_lifecycle_health_payload(  # noqa: SLF001
        payload,
        lifecycle_payload=lifecycle_payload,
        lifecycle_binding=lifecycle_binding,
        runtime_execution=portable["runtime_execution"],
        runtime_authority=portable["runtime_authority"],
        transition=portable["transition"],
        active_runtime=portable["active_runtime"],
    )


def _recanonicalize_post_lifecycle(payload: dict[str, Any]) -> None:
    field = subject.POST_LIFECYCLE_HEALTH_CANONICAL_FIELD
    payload[field] = subject._document_sha256(payload, field)  # noqa: SLF001


def test_post_lifecycle_health_is_distinct_same_process_current_no_shadow_evidence() -> None:
    observed = _validate_post_lifecycle(_post_lifecycle_payload())
    assert observed["checks"]["snapshot_after_lifecycle_admission"] is True
    assert observed["main_health_window"]["rows"][0]["main_wall_timestamp_s"] > (
        _lifecycle_bundle()[0]["admitted_ts_ns"] / 1_000_000_000
    )
    assert observed["operational_aggregates"]["latency"]["strategy_result_authority"] is False
    assert observed["operational_aggregates"]["position"] == {
        "main_health_position_projection_completed": True,
        "reported_aggregate_position_flat": False,
        "reported_open_order_count": 1,
        "economic_values_persisted": False,
    }


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("different_pid", "producer projection is invalid"),
        ("before_lifecycle", "predates lifecycle admission"),
        ("reused_update", "producer projection is invalid"),
        ("extra_check", "producer projection is invalid"),
        ("lifecycle_error", "producer projection is invalid"),
        ("economic_position_value", "producer projection is invalid"),
        ("economic_persisted", "producer projection is invalid"),
        ("fake_strategy_authority", "producer projection is invalid"),
    ),
)
def test_post_lifecycle_health_rejects_process_reuse_tamper_and_economic_values(
    mutation: str,
    message: str,
) -> None:
    payload = _post_lifecycle_payload()
    if mutation == "different_pid":
        payload["active_process"]["pid"] += 1
    elif mutation == "before_lifecycle":
        payload["main_health_window"]["rows"][0]["main_wall_timestamp_s"] = 1_787_546_999.0
    elif mutation == "reused_update":
        payload["main_health_window"]["rows"][1]["projection"]["boolean_cooldown_updates"] = 30
    elif mutation == "extra_check":
        payload["main_health_window"]["checks"]["invented_true"] = True
    elif mutation == "lifecycle_error":
        payload["lifecycle_health"]["order_lifecycle_v2_errors"] = 1
    elif mutation == "economic_position_value":
        payload["operational_aggregates"]["position"]["positionAmt"] = 0.001
    elif mutation == "economic_persisted":
        payload["operational_aggregates"]["position"]["economic_values_persisted"] = True
    else:
        payload["operational_aggregates"]["latency"]["strategy_result_authority"] = True
    _recanonicalize_post_lifecycle(payload)
    with pytest.raises(subject.FinalEvidenceV6Error, match=message):
        _validate_post_lifecycle(payload)


def test_final_proof_consumes_release_v3_authority_without_replacing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, release_binding, execution, artifact = _release_bundle()
    authority = {
        **dict(release_binding),
        "execution": dict(execution),
        "runtime_authority": True,
    }
    evidence_items = {
        "config_correction": {"role": "config-correction"},
        "current_resource_v8": {"role": "resource-v8"},
        "current_active_schema_v7": {"role": "active-schema-v7"},
        "current_remote_attestation_transport_v6": {"role": "transport-v6-attestation"},
        "current_lifecycle_admission": {"role": "new-lifecycle"},
        "current_lifecycle_context": {"role": "portable-context-transport-only"},
        "current_activation_no_shadow_evidence": {"role": "activation-health"},
        "current_post_lifecycle_live_health": {"role": "post-lifecycle-health"},
        "current_post_lifecycle_no_shadow_evidence": {"role": "post-lifecycle-no-shadow"},
        "failed_activation_attempt_history": {"role": "history-only"},
        "superseded_v4_proof": {"role": "historical-only"},
    }
    attempt = {
        "runtime_execution": execution,
        "runtime_authority": authority,
        "exact_artifact": artifact,
        "composition_root_sha256": "f" * 64,
        **evidence_items,
    }
    attempt_binding = {
        **_content(
            subject.ATTEMPT_FINAL_SCHEMA,
            subject.ATTEMPT_FINAL_STATUS,
            "1",
            "2",
            canonical_field=subject.ATTEMPT_FINAL_CANONICAL_FIELD,
        ),
        "path": "/fixture/attempt-final.json",
    }
    monkeypatch.setattr(
        subject,
        "_current_authority_context",
        lambda _root, _release: (release, release_binding, execution, artifact),
    )
    monkeypatch.setattr(subject, "validate_attempt_final", lambda _path, **_roots: attempt)
    monkeypatch.setattr(subject, "_receipt_binding", lambda *_args, **_kwargs: attempt_binding)
    payload = subject.build_evidence_release(
        attempt_final_path=Path("/fixture/attempt-final.json"),
        current_runtime_root=Path("/fixture/current"),
        current_release_v3=Path("/fixture/current/release-v3.json"),
        historical_v4_root=Path("/fixture/historical-v4"),
        historical_v4_release_v2=Path("/fixture/historical-v4/release-v2.json"),
        generated_utc="2026-08-24T11:00:00Z",
    )
    assert payload["runtime_authority"] == authority
    assert payload["authority_provenance"] == {
        "source": "source_frozen_direct_owner_release_v3",
        "current_runtime_evidence_source": (
            "transport_v6_resource_v8_active_schema_v7_post_health_and_new_lifecycle"
        ),
        "release_v3_file_sha256": release_binding["file_sha256"],
        "release_v3_canonical_sha256": release_binding["canonical_sha256"],
        "proof_release_replaces_runtime_authority": False,
        "new_authority_granted": False,
        "superseded_v4_proof_used_as_authority": False,
        "failed_activation_attempt_history_used_as_authority": False,
    }
    assert payload["evidence_state"]["runtime_authority_replaced"] is False
    assert payload["evidence_state"]["does_not_replace_runtime_active_release"] is True


def test_completion_cli_requires_distinct_post_lifecycle_health_receipt() -> None:
    common = [
        "completion",
        "--current-runtime-root",
        "/current",
        "--current-release-v3",
        "/current/release-v3.json",
        "--historical-v4-root",
        "/historical",
        "--historical-v4-release-v2",
        "/historical/release-v2.json",
        "--activation-envelope",
        "/receipts/envelope.json",
        "--lifecycle-admission",
        "/receipts/admission.json",
        "--output",
        "/receipts/completion.json",
    ]
    with pytest.raises(SystemExit):
        subject._parser().parse_args(common)  # noqa: SLF001
    parsed = subject._parser().parse_args(  # noqa: SLF001
        [
            *common,
            "--lifecycle-context",
            "/receipts/lifecycle-context.json",
            "--post-lifecycle-live-health",
            "/receipts/post-health.json",
        ]
    )
    assert parsed.lifecycle_context == Path("/receipts/lifecycle-context.json")
    assert parsed.post_lifecycle_live_health == Path("/receipts/post-health.json")
