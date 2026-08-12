"""Bounded mechanics-only preflight for F07 lifecycle-CIF training data.

The preflight reads exactly two control-plane objects: a baseline-epoch
manifest and lifecycle-journal dataset metadata. It never opens the lifecycle
tape and rejects any metadata surface that can carry economic outcomes.

Passing this preflight means that a successor training identity may be frozen.
It does not authorize model fitting, q90 action, economic evaluation, or live
deployment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path

from execution.order_lifecycle_journal import (
    ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION as LEGACY_JOURNAL_SCHEMA_VERSION,
)
from execution.order_lifecycle_journal import (
    OrderLifecycleJournalRow,
)
from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
)
from models.replay.baseline_epoch_manifest import (
    canonical_sha256,
    validate_baseline_epoch_manifest,
)
from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif import (
    CAUSES,
    GRID_INTERVAL_MS,
)
from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif import (
    IDENTITY as CIF_KERNEL_IDENTITY,
)

IDENTITY = "active_order_cif_training_preflight_v1"
METADATA_SCHEMA_VERSION = "order_lifecycle_journal_v2_dataset_metadata.v1"
REPORT_SCHEMA_VERSION = "active_order_cif_training_preflight_report.v1"
REQUIRED_CHRONOLOGICAL_DAYS = 40
MAX_METADATA_DAYS = 400
MAX_BASELINE_EPOCHS = 512

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_KEY_FRAGMENTS = (
    "pnl",
    "reward",
    "markout",
    "campaign_terminal_value",
)

_TOP_LEVEL_KEYS = (
    "schema_version",
    "dataset_id",
    "journal_schema_version",
    "journal_columns",
    "scope_start_ts_ns",
    "scope_end_ts_ns",
    "row_count",
    "order_count",
    "chronological_day_ids",
    "runtime_integration",
    "artifact_identity",
    "epoch_binding",
    "grid_contract",
    "competing_risks",
    "dual_clock_contract",
    "partial_fill_contract",
    "censoring_contract",
    "left_truncation_contract",
    "lockstep_contract",
    "outcome_access",
)

_NESTED_KEYS = {
    "runtime_integration": (
        "live_v2_batch_emitter_integrated",
        "replay_v2_batch_emitter_integrated",
        "all_unseen_events_emitted",
        "callback_batch_commit_atomic",
        "durable_cursor_checkpoint",
        "writer_health_manifest_present",
        "writer_drop_count",
        "writer_error_count",
        "live_producer_sha256",
        "replay_producer_sha256",
    ),
    "artifact_identity": (
        "lifecycle_tape_path",
        "lifecycle_tape_sha256",
        "admission_manifest_path",
        "admission_manifest_sha256",
        "health_manifest_path",
        "health_manifest_sha256",
        "journal_schema_sha256",
        "atomic_admission",
        "payload_integrity_valid",
        "row_count_agreement",
    ),
    "epoch_binding": (
        "baseline_manifest_sha256",
        "every_row_bound_to_epoch",
        "unbound_row_count",
        "cross_epoch_order_count",
        "epoch_ids",
    ),
    "grid_contract": (
        "interval_ms",
        "clock",
        "contiguous_edges",
        "duplicate_edge_count",
        "missed_edge_count",
    ),
    "competing_risks": (
        "causes",
        "cause_counts",
        "full_fill_classifier_identity",
        "full_fill_classifier_sha256",
        "unknown_terminal_count",
        "post_terminal_risk_row_count",
        "cancel_request_terminal_count",
        "cancel_reject_terminal_count",
    ),
    "dual_clock_contract": (
        "visibility_clock_present",
        "exchange_clock_present",
        "exchange_timestamp_after_visibility_count",
        "exchange_timestamp_regression_count",
        "exchange_clock_valid_row_count",
        "exchange_clock_invalid_row_count",
        "exchange_clock_invalid_reason_counts",
    ),
    "partial_fill_contract": (
        "partial_fill_count",
        "spell_boundary_count",
        "positive_remaining_new_spell_count",
        "missing_new_spell_count",
        "invalid_remaining_quantity_transition_count",
        "last_grid_edge_preserved_on_reset",
    ),
    "censoring_contract": (
        "explicit_local_shutdown_censor_count",
        "local_shutdown_right_censored_count",
        "legacy_shutdown_as_exchange_terminal_count",
        "events_after_local_shutdown_censor_count",
    ),
    "left_truncation_contract": (
        "left_truncated_order_count",
        "delayed_entry_order_count",
        "missing_reason_count",
        "missing_entry_timestamp_count",
        "entry_rule",
    ),
    "lockstep_contract": (
        "chronological_panel_manifest_path",
        "chronological_panel_manifest_sha256",
        "chronological_days_required",
        "python_panel_builder_sha256",
        "cpp_panel_builder_sha256",
        "python_cif_kernel_sha256",
        "cpp_cif_kernel_sha256",
        "event_lockstep_runner_sha256",
        "checkpoint_resume_runner_sha256",
    ),
    "outcome_access": (
        "pnl_read",
        "reward_read",
        "markout_read",
        "campaign_terminal_value_read",
        "q90_action_enabled",
    ),
}

_RUNTIME_INTEGRATION_NAMES = {
    "live_v2_batch_emitter_integrated": (
        "live_order_manager_order_lifecycle_journal_v2_batch_emitter"
    ),
    "replay_v2_batch_emitter_integrated": (
        "replay_order_manager_order_lifecycle_journal_v2_batch_emitter"
    ),
    "all_unseen_events_emitted": "all_unseen_lifecycle_event_batch_emission",
    "callback_batch_commit_atomic": "atomic_callback_batch_commit",
    "durable_cursor_checkpoint": "durable_lifecycle_cursor_checkpoint",
    "writer_health_manifest_present": "lifecycle_writer_health_accounting",
}

_ARTIFACT_FIELDS = {
    "lifecycle_tape_sha256": "authoritative_order_lifecycle_journal_v2_tape",
    "admission_manifest_sha256": "order_lifecycle_journal_v2_admission_manifest",
    "health_manifest_sha256": "order_lifecycle_journal_v2_health_manifest",
}

_LOCKSTEP_ARTIFACT_FIELDS = {
    "chronological_panel_manifest_sha256": "frozen_f07_chronological_40_day_panel_manifest",
    "python_panel_builder_sha256": "python_f07_cif_panel_builder",
    "cpp_panel_builder_sha256": "cpp_f07_cif_panel_builder",
    "python_cif_kernel_sha256": "python_active_order_competing_risk_cif_kernel",
    "cpp_cif_kernel_sha256": "cpp_active_order_competing_risk_cif_kernel",
    "event_lockstep_runner_sha256": "python_cpp_event_lockstep_runner",
    "checkpoint_resume_runner_sha256": "python_cpp_checkpoint_resume_lockstep_runner",
}


class ActiveOrderCIFPreflightError(ValueError):
    """Raised for metadata/schema drift rather than ordinary missing evidence."""


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def _exact_keys(value: Mapping[str, object], expected: Sequence[str], *, label: str) -> None:
    actual = tuple(value)
    if actual != tuple(expected):
        raise ActiveOrderCIFPreflightError(
            f"{label} schema mismatch: expected={tuple(expected)!r} actual={actual!r}"
        )


def _required_id(value: object, *, label: str) -> str:
    normalized = str(value).strip()
    if not normalized or normalized.lower() in {"nan", "none", "null"}:
        raise ActiveOrderCIFPreflightError(f"{label} must be non-empty")
    return normalized


def _nonnegative_int(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise ActiveOrderCIFPreflightError(f"{label} must be an integer")
    number = int(value)
    if number < 0 or float(value) != float(number):
        raise ActiveOrderCIFPreflightError(f"{label} must be a non-negative integer")
    return number


def _optional_nonnegative_int(value: object, *, label: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_int(value, label=label)


def _walk_keys(value: object) -> tuple[str, ...]:
    keys: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            keys.append(str(key).lower())
            keys.extend(_walk_keys(item))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            keys.extend(_walk_keys(item))
    return tuple(keys)


def lifecycle_journal_v2_schema_sha256() -> str:
    """Return the canonical schema identity without inspecting any tape rows."""

    return canonical_sha256(
        {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
            "columns": list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS),
        }
    )


def python_cif_kernel_sha256() -> str:
    """Bind lockstep readiness to the current frozen Python CIF kernel."""

    return hashlib.sha256(
        Path(__file__).with_name("active_order_competing_risk_cif.py").read_bytes()
    ).hexdigest()


def validate_lifecycle_dataset_metadata(metadata: Mapping[str, object]) -> None:
    """Validate the bounded metadata envelope, not data readiness."""

    _exact_keys(metadata, _TOP_LEVEL_KEYS, label="lifecycle dataset metadata")
    if metadata["schema_version"] != METADATA_SCHEMA_VERSION:
        raise ActiveOrderCIFPreflightError("unsupported lifecycle dataset metadata schema")
    _required_id(metadata["dataset_id"], label="dataset_id")

    for key in _walk_keys(metadata):
        if any(fragment in key for fragment in _FORBIDDEN_KEY_FRAGMENTS):
            if key not in {
                "pnl_read",
                "reward_read",
                "markout_read",
                "campaign_terminal_value_read",
            }:
                raise ActiveOrderCIFPreflightError(
                    f"economic outcome metadata field is forbidden: {key}"
                )

    columns = metadata["journal_columns"]
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)):
        raise ActiveOrderCIFPreflightError("journal_columns must be an ordered array")
    if len(columns) > 128 or len(set(map(str, columns))) != len(columns):
        raise ActiveOrderCIFPreflightError("journal_columns are duplicated or unbounded")

    start = _nonnegative_int(metadata["scope_start_ts_ns"], label="scope_start_ts_ns")
    end = _nonnegative_int(metadata["scope_end_ts_ns"], label="scope_end_ts_ns")
    if start <= 0 or start >= end:
        raise ActiveOrderCIFPreflightError("lifecycle metadata scope must have positive duration")
    _optional_nonnegative_int(metadata["row_count"], label="row_count")
    _optional_nonnegative_int(metadata["order_count"], label="order_count")

    days = metadata["chronological_day_ids"]
    if not isinstance(days, Sequence) or isinstance(days, (str, bytes)):
        raise ActiveOrderCIFPreflightError("chronological_day_ids must be an array")
    normalized_days = tuple(map(str, days))
    if len(normalized_days) > MAX_METADATA_DAYS:
        raise ActiveOrderCIFPreflightError("chronological day metadata exceeds bounded scope")
    if normalized_days != tuple(sorted(set(normalized_days))):
        raise ActiveOrderCIFPreflightError("chronological_day_ids must be unique and ordered")

    for section, expected in _NESTED_KEYS.items():
        value = metadata[section]
        if not isinstance(value, Mapping):
            raise ActiveOrderCIFPreflightError(f"{section} must be an object")
        _exact_keys(value, expected, label=section)

    runtime = metadata["runtime_integration"]
    for field in _RUNTIME_INTEGRATION_NAMES:
        if not isinstance(runtime[field], bool):
            raise ActiveOrderCIFPreflightError(f"runtime_integration.{field} must be boolean")
    for field in ("writer_drop_count", "writer_error_count"):
        _optional_nonnegative_int(runtime[field], label=f"runtime_integration.{field}")
    for field in ("live_producer_sha256", "replay_producer_sha256"):
        if runtime[field] is not None and not _is_sha256(runtime[field]):
            raise ActiveOrderCIFPreflightError(f"runtime_integration.{field} is not SHA256")

    artifacts = metadata["artifact_identity"]
    for field in (
        "lifecycle_tape_path",
        "admission_manifest_path",
        "health_manifest_path",
    ):
        if artifacts[field] is not None:
            _required_id(artifacts[field], label=f"artifact_identity.{field}")
    for field in (
        "lifecycle_tape_sha256",
        "admission_manifest_sha256",
        "health_manifest_sha256",
        "journal_schema_sha256",
    ):
        if artifacts[field] is not None and not _is_sha256(artifacts[field]):
            raise ActiveOrderCIFPreflightError(f"artifact_identity.{field} is not SHA256")
    for field in ("atomic_admission", "payload_integrity_valid", "row_count_agreement"):
        if not isinstance(artifacts[field], bool):
            raise ActiveOrderCIFPreflightError(f"artifact_identity.{field} must be boolean")

    binding = metadata["epoch_binding"]
    if binding["baseline_manifest_sha256"] is not None and not _is_sha256(
        binding["baseline_manifest_sha256"]
    ):
        raise ActiveOrderCIFPreflightError("epoch_binding baseline hash is not SHA256")
    if not isinstance(binding["every_row_bound_to_epoch"], bool):
        raise ActiveOrderCIFPreflightError("epoch row binding flag must be boolean")
    for field in ("unbound_row_count", "cross_epoch_order_count"):
        _optional_nonnegative_int(binding[field], label=f"epoch_binding.{field}")
    epoch_ids = binding["epoch_ids"]
    if not isinstance(epoch_ids, Sequence) or isinstance(epoch_ids, (str, bytes)):
        raise ActiveOrderCIFPreflightError("epoch_binding.epoch_ids must be an array")
    if len(epoch_ids) > MAX_BASELINE_EPOCHS or len(set(map(str, epoch_ids))) != len(epoch_ids):
        raise ActiveOrderCIFPreflightError("epoch ids are duplicated or unbounded")

    grid = metadata["grid_contract"]
    for field in ("interval_ms", "duplicate_edge_count", "missed_edge_count"):
        _optional_nonnegative_int(grid[field], label=f"grid_contract.{field}")
    if not isinstance(grid["contiguous_edges"], bool):
        raise ActiveOrderCIFPreflightError("grid contiguous flag must be boolean")
    _required_id(grid["clock"], label="grid_contract.clock")

    risks = metadata["competing_risks"]
    if not isinstance(risks["causes"], Sequence) or isinstance(risks["causes"], (str, bytes)):
        raise ActiveOrderCIFPreflightError("competing causes must be an array")
    cause_counts = risks["cause_counts"]
    if not isinstance(cause_counts, Mapping):
        raise ActiveOrderCIFPreflightError("cause_counts must be an object")
    if tuple(cause_counts) not in {(), tuple(CAUSES)}:
        raise ActiveOrderCIFPreflightError("cause_counts must be empty or use canonical order")
    for cause, count in cause_counts.items():
        _optional_nonnegative_int(count, label=f"cause_counts.{cause}")
    if risks["full_fill_classifier_sha256"] is not None and not _is_sha256(
        risks["full_fill_classifier_sha256"]
    ):
        raise ActiveOrderCIFPreflightError("full-fill classifier hash is not SHA256")
    if risks["full_fill_classifier_identity"] is not None:
        _required_id(
            risks["full_fill_classifier_identity"],
            label="full_fill_classifier_identity",
        )
    for field in (
        "unknown_terminal_count",
        "post_terminal_risk_row_count",
        "cancel_request_terminal_count",
        "cancel_reject_terminal_count",
    ):
        _optional_nonnegative_int(risks[field], label=f"competing_risks.{field}")

    clocks = metadata["dual_clock_contract"]
    for field in ("visibility_clock_present", "exchange_clock_present"):
        if not isinstance(clocks[field], bool):
            raise ActiveOrderCIFPreflightError(f"dual_clock_contract.{field} must be boolean")
    for field in (
        "exchange_timestamp_after_visibility_count",
        "exchange_timestamp_regression_count",
        "exchange_clock_valid_row_count",
        "exchange_clock_invalid_row_count",
    ):
        _optional_nonnegative_int(clocks[field], label=f"dual_clock_contract.{field}")
    if not isinstance(clocks["exchange_clock_invalid_reason_counts"], Mapping):
        raise ActiveOrderCIFPreflightError("exchange invalid reason counts must be an object")

    partial = metadata["partial_fill_contract"]
    for field in (
        "partial_fill_count",
        "spell_boundary_count",
        "positive_remaining_new_spell_count",
        "missing_new_spell_count",
        "invalid_remaining_quantity_transition_count",
    ):
        _optional_nonnegative_int(partial[field], label=f"partial_fill_contract.{field}")
    if not isinstance(partial["last_grid_edge_preserved_on_reset"], bool):
        raise ActiveOrderCIFPreflightError("partial-fill edge preservation must be boolean")

    censoring = metadata["censoring_contract"]
    for field in _NESTED_KEYS["censoring_contract"]:
        _optional_nonnegative_int(censoring[field], label=f"censoring_contract.{field}")
    truncation = metadata["left_truncation_contract"]
    for field in (
        "left_truncated_order_count",
        "delayed_entry_order_count",
        "missing_reason_count",
        "missing_entry_timestamp_count",
    ):
        _optional_nonnegative_int(truncation[field], label=f"left_truncation_contract.{field}")
    _required_id(truncation["entry_rule"], label="left_truncation_contract.entry_rule")

    lockstep = metadata["lockstep_contract"]
    if lockstep["chronological_panel_manifest_path"] is not None:
        _required_id(
            lockstep["chronological_panel_manifest_path"],
            label="chronological_panel_manifest_path",
        )
    _nonnegative_int(
        lockstep["chronological_days_required"],
        label="lockstep_contract.chronological_days_required",
    )
    for field in _LOCKSTEP_ARTIFACT_FIELDS:
        if lockstep[field] is not None and not _is_sha256(lockstep[field]):
            raise ActiveOrderCIFPreflightError(f"lockstep_contract.{field} is not SHA256")

    access = metadata["outcome_access"]
    for field in _NESTED_KEYS["outcome_access"]:
        if not isinstance(access[field], bool):
            raise ActiveOrderCIFPreflightError(f"outcome_access.{field} must be boolean")


def current_unauthoritative_live_journal_metadata(
    baseline_manifest: Mapping[str, object],
) -> dict[str, object]:
    """Describe the current v1 live journal without pretending it is v2 evidence."""

    scope_start = int(baseline_manifest["scope_start_ts_ns"])
    scope_end = int(baseline_manifest["scope_end_ts_ns"])
    return {
        "schema_version": METADATA_SCHEMA_VERSION,
        "dataset_id": "current_live_order_lifecycle_journal_v1_unauthoritative",
        "journal_schema_version": LEGACY_JOURNAL_SCHEMA_VERSION,
        "journal_columns": list(OrderLifecycleJournalRow.__dataclass_fields__),
        "scope_start_ts_ns": scope_start,
        "scope_end_ts_ns": scope_end,
        "row_count": None,
        "order_count": None,
        "chronological_day_ids": [],
        "runtime_integration": {
            "live_v2_batch_emitter_integrated": False,
            "replay_v2_batch_emitter_integrated": False,
            "all_unseen_events_emitted": False,
            "callback_batch_commit_atomic": False,
            "durable_cursor_checkpoint": False,
            "writer_health_manifest_present": False,
            "writer_drop_count": None,
            "writer_error_count": None,
            "live_producer_sha256": None,
            "replay_producer_sha256": None,
        },
        "artifact_identity": {
            "lifecycle_tape_path": None,
            "lifecycle_tape_sha256": None,
            "admission_manifest_path": None,
            "admission_manifest_sha256": None,
            "health_manifest_path": None,
            "health_manifest_sha256": None,
            "journal_schema_sha256": None,
            "atomic_admission": False,
            "payload_integrity_valid": False,
            "row_count_agreement": False,
        },
        "epoch_binding": {
            "baseline_manifest_sha256": baseline_manifest.get("canonical_manifest_sha256"),
            "every_row_bound_to_epoch": False,
            "unbound_row_count": None,
            "cross_epoch_order_count": None,
            "epoch_ids": [],
        },
        "grid_contract": {
            "interval_ms": GRID_INTERVAL_MS,
            "clock": "causal_visibility_clock",
            "contiguous_edges": False,
            "duplicate_edge_count": None,
            "missed_edge_count": None,
        },
        "competing_risks": {
            "causes": list(CAUSES),
            "cause_counts": {},
            "full_fill_classifier_identity": None,
            "full_fill_classifier_sha256": None,
            "unknown_terminal_count": None,
            "post_terminal_risk_row_count": None,
            "cancel_request_terminal_count": None,
            "cancel_reject_terminal_count": None,
        },
        "dual_clock_contract": {
            "visibility_clock_present": True,
            "exchange_clock_present": True,
            "exchange_timestamp_after_visibility_count": None,
            "exchange_timestamp_regression_count": None,
            "exchange_clock_valid_row_count": None,
            "exchange_clock_invalid_row_count": None,
            "exchange_clock_invalid_reason_counts": {},
        },
        "partial_fill_contract": {
            "partial_fill_count": None,
            "spell_boundary_count": None,
            "positive_remaining_new_spell_count": None,
            "missing_new_spell_count": None,
            "invalid_remaining_quantity_transition_count": None,
            "last_grid_edge_preserved_on_reset": False,
        },
        "censoring_contract": {
            "explicit_local_shutdown_censor_count": 0,
            "local_shutdown_right_censored_count": 0,
            "legacy_shutdown_as_exchange_terminal_count": None,
            "events_after_local_shutdown_censor_count": None,
        },
        "left_truncation_contract": {
            "left_truncated_order_count": None,
            "delayed_entry_order_count": None,
            "missing_reason_count": None,
            "missing_entry_timestamp_count": None,
            "entry_rule": "unsupported_in_current_v1_journal",
        },
        "lockstep_contract": {
            "chronological_panel_manifest_path": None,
            "chronological_panel_manifest_sha256": None,
            "chronological_days_required": REQUIRED_CHRONOLOGICAL_DAYS,
            "python_panel_builder_sha256": None,
            "cpp_panel_builder_sha256": None,
            "python_cif_kernel_sha256": python_cif_kernel_sha256(),
            "cpp_cif_kernel_sha256": None,
            "event_lockstep_runner_sha256": None,
            "checkpoint_resume_runner_sha256": None,
        },
        "outcome_access": {
            "pnl_read": False,
            "reward_read": False,
            "markout_read": False,
            "campaign_terminal_value_read": False,
            "q90_action_enabled": False,
        },
    }


def _zero_or_known(value: object) -> bool:
    return value is not None and int(value) == 0


def _matching_count(left: object, right: object) -> bool:
    return left is not None and right is not None and int(left) == int(right)


def run_active_order_cif_training_preflight(
    baseline_manifest: Mapping[str, object],
    lifecycle_metadata: Mapping[str, object],
) -> dict[str, object]:
    """Evaluate bounded readiness and return deterministic blockers."""

    validate_baseline_epoch_manifest(baseline_manifest)
    validate_lifecycle_dataset_metadata(lifecycle_metadata)

    epochs = list(baseline_manifest["epochs"])
    if len(epochs) > MAX_BASELINE_EPOCHS:
        raise ActiveOrderCIFPreflightError("baseline epoch manifest exceeds bounded scope")

    blockers: set[str] = set()
    missing_integrations: set[str] = set()
    missing_artifacts: set[str] = set()

    if not bool(baseline_manifest.get("restart_audit_complete")):
        blockers.add("baseline.restart_audit_incomplete")
    if baseline_manifest.get("unbound_intervals"):
        blockers.add("baseline.unbound_intervals_present")
    authorized_epochs = {
        str(epoch["epoch_id"])
        for epoch in epochs
        if bool(epoch.get("lifecycle_estimation_authorized"))
    }
    if not authorized_epochs:
        blockers.add("baseline.no_lifecycle_authorized_epochs")
    if any(epoch.get("binding_status") != "fully_bound" for epoch in epochs):
        blockers.add("baseline.partially_bound_epochs_present")
    if any(epoch.get("boundary_status") != "first_decision_bound" for epoch in epochs):
        blockers.add("baseline.non_first_decision_bound_epochs_present")
    baseline_ready = not any(item.startswith("baseline.") for item in blockers)

    if lifecycle_metadata["journal_schema_version"] != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
        blockers.add("journal.schema_version_is_not_v2")
    if tuple(lifecycle_metadata["journal_columns"]) != ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS:
        blockers.add("journal.columns_do_not_match_v2")
    artifacts = lifecycle_metadata["artifact_identity"]
    if artifacts["journal_schema_sha256"] != lifecycle_journal_v2_schema_sha256():
        blockers.add("journal.schema_identity_unbound")

    runtime = lifecycle_metadata["runtime_integration"]
    for field, integration in _RUNTIME_INTEGRATION_NAMES.items():
        if not bool(runtime[field]):
            missing_integrations.add(integration)
            blockers.add(f"runtime.{field}_missing")
    for field in ("live_producer_sha256", "replay_producer_sha256"):
        if not _is_sha256(runtime[field]):
            missing_artifacts.add(field)
            blockers.add(f"runtime.{field}_missing")
    if not _zero_or_known(runtime["writer_drop_count"]):
        blockers.add("runtime.writer_drop_count_not_zero_or_unknown")
    if not _zero_or_known(runtime["writer_error_count"]):
        blockers.add("runtime.writer_error_count_not_zero_or_unknown")

    for field, artifact_name in _ARTIFACT_FIELDS.items():
        if not _is_sha256(artifacts[field]):
            missing_artifacts.add(artifact_name)
            blockers.add(f"artifact.{field}_missing")
    for field in ("atomic_admission", "payload_integrity_valid", "row_count_agreement"):
        if not bool(artifacts[field]):
            blockers.add(f"artifact.{field}_failed")

    if lifecycle_metadata["row_count"] is None or lifecycle_metadata["order_count"] is None:
        blockers.add("artifact.dataset_counts_unknown")
    elif int(lifecycle_metadata["row_count"]) < int(lifecycle_metadata["order_count"]):
        blockers.add("artifact.row_count_below_order_count")

    binding = lifecycle_metadata["epoch_binding"]
    if binding["baseline_manifest_sha256"] != baseline_manifest["canonical_manifest_sha256"]:
        blockers.add("epoch.baseline_manifest_hash_mismatch")
    if not bool(binding["every_row_bound_to_epoch"]):
        blockers.add("epoch.rows_not_fully_bound")
    if not _zero_or_known(binding["unbound_row_count"]):
        blockers.add("epoch.unbound_row_count_not_zero_or_unknown")
    if not _zero_or_known(binding["cross_epoch_order_count"]):
        blockers.add("epoch.cross_epoch_order_count_not_zero_or_unknown")
    metadata_epoch_ids = set(map(str, binding["epoch_ids"]))
    if not metadata_epoch_ids:
        blockers.add("epoch.dataset_epoch_ids_empty")
    if not metadata_epoch_ids.issubset(authorized_epochs):
        blockers.add("epoch.dataset_uses_unauthorized_epochs")

    scope_start = int(lifecycle_metadata["scope_start_ts_ns"])
    scope_end = int(lifecycle_metadata["scope_end_ts_ns"])
    if scope_start < int(baseline_manifest["scope_start_ts_ns"]) or scope_end > int(
        baseline_manifest["scope_end_ts_ns"]
    ):
        blockers.add("epoch.dataset_scope_outside_manifest")

    grid = lifecycle_metadata["grid_contract"]
    if grid["interval_ms"] != GRID_INTERVAL_MS:
        blockers.add("grid.interval_is_not_100ms")
    if grid["clock"] != "causal_visibility_clock":
        blockers.add("grid.clock_is_not_causal_visibility")
    if not bool(grid["contiguous_edges"]):
        blockers.add("grid.edges_not_contiguous")
    if not _zero_or_known(grid["duplicate_edge_count"]):
        blockers.add("grid.duplicate_edges_not_zero_or_unknown")
    if not _zero_or_known(grid["missed_edge_count"]):
        blockers.add("grid.missed_edges_not_zero_or_unknown")

    risks = lifecycle_metadata["competing_risks"]
    if tuple(risks["causes"]) != tuple(CAUSES):
        blockers.add("causes.canonical_competing_risks_mismatch")
    if tuple(risks["cause_counts"]) != tuple(CAUSES):
        blockers.add("causes.canonical_counts_unavailable")
    if not risks["full_fill_classifier_identity"] or not _is_sha256(
        risks["full_fill_classifier_sha256"]
    ):
        blockers.add("causes.full_fill_classifier_unbound")
        missing_artifacts.add("frozen_full_fill_cause_classifier")
    for field in (
        "unknown_terminal_count",
        "post_terminal_risk_row_count",
        "cancel_request_terminal_count",
        "cancel_reject_terminal_count",
    ):
        if not _zero_or_known(risks[field]):
            blockers.add(f"causes.{field}_not_zero_or_unknown")

    clocks = lifecycle_metadata["dual_clock_contract"]
    if not bool(clocks["visibility_clock_present"]):
        blockers.add("clocks.visibility_clock_missing")
    if not bool(clocks["exchange_clock_present"]):
        blockers.add("clocks.exchange_clock_missing")
    for field in (
        "exchange_timestamp_after_visibility_count",
        "exchange_timestamp_regression_count",
    ):
        if not _zero_or_known(clocks[field]):
            blockers.add(f"clocks.{field}_not_zero_or_unknown")
    valid_clock_rows = clocks["exchange_clock_valid_row_count"]
    invalid_clock_rows = clocks["exchange_clock_invalid_row_count"]
    if valid_clock_rows is None or invalid_clock_rows is None:
        blockers.add("clocks.exchange_clock_coverage_unknown")
    elif lifecycle_metadata["row_count"] is not None and (
        int(valid_clock_rows) + int(invalid_clock_rows) != int(lifecycle_metadata["row_count"])
    ):
        blockers.add("clocks.exchange_clock_coverage_count_mismatch")

    partial = lifecycle_metadata["partial_fill_contract"]
    if partial["partial_fill_count"] is None:
        blockers.add("partial_fill.count_unknown")
    if not _matching_count(partial["partial_fill_count"], partial["spell_boundary_count"]):
        blockers.add("partial_fill.spell_boundary_count_mismatch")
    if not _matching_count(
        partial["partial_fill_count"],
        partial["positive_remaining_new_spell_count"],
    ):
        blockers.add("partial_fill.new_spell_count_mismatch")
    for field in (
        "missing_new_spell_count",
        "invalid_remaining_quantity_transition_count",
    ):
        if not _zero_or_known(partial[field]):
            blockers.add(f"partial_fill.{field}_not_zero_or_unknown")
    if not bool(partial["last_grid_edge_preserved_on_reset"]):
        blockers.add("partial_fill.last_grid_edge_not_preserved")

    censoring = lifecycle_metadata["censoring_contract"]
    if not _matching_count(
        censoring["explicit_local_shutdown_censor_count"],
        censoring["local_shutdown_right_censored_count"],
    ):
        blockers.add("censoring.local_shutdown_right_censor_count_mismatch")
    for field in (
        "legacy_shutdown_as_exchange_terminal_count",
        "events_after_local_shutdown_censor_count",
    ):
        if not _zero_or_known(censoring[field]):
            blockers.add(f"censoring.{field}_not_zero_or_unknown")

    truncation = lifecycle_metadata["left_truncation_contract"]
    if not _matching_count(
        truncation["left_truncated_order_count"],
        truncation["delayed_entry_order_count"],
    ):
        blockers.add("left_truncation.delayed_entry_count_mismatch")
    for field in ("missing_reason_count", "missing_entry_timestamp_count"):
        if not _zero_or_known(truncation[field]):
            blockers.add(f"left_truncation.{field}_not_zero_or_unknown")
    if truncation["entry_rule"] != "delayed_entry_at_first_observation":
        blockers.add("left_truncation.entry_rule_unsupported")

    access = lifecycle_metadata["outcome_access"]
    if any(bool(access[field]) for field in _NESTED_KEYS["outcome_access"]):
        blockers.add("permissions.outcome_or_q90_access_detected")

    day_count = len(lifecycle_metadata["chronological_day_ids"])
    lockstep = lifecycle_metadata["lockstep_contract"]
    if lockstep["chronological_days_required"] != REQUIRED_CHRONOLOGICAL_DAYS:
        blockers.add("lockstep.required_day_count_changed")
    if day_count < REQUIRED_CHRONOLOGICAL_DAYS:
        blockers.add("lockstep.chronological_day_support_below_40")
    for field, artifact_name in _LOCKSTEP_ARTIFACT_FIELDS.items():
        if not _is_sha256(lockstep[field]):
            missing_artifacts.add(artifact_name)
            blockers.add(f"lockstep.{field}_missing")
    if (
        _is_sha256(lockstep["python_cif_kernel_sha256"])
        and lockstep["python_cif_kernel_sha256"] != python_cif_kernel_sha256()
    ):
        blockers.add("lockstep.python_cif_kernel_sha256_mismatch")

    data_prefixes = (
        "baseline.",
        "journal.",
        "runtime.",
        "artifact.",
        "epoch.",
        "grid.",
        "causes.",
        "clocks.",
        "partial_fill.",
        "censoring.",
        "left_truncation.",
        "permissions.",
    )
    data_ready = not any(item.startswith(data_prefixes) for item in blockers)
    training_registration_ready = data_ready and (
        day_count >= REQUIRED_CHRONOLOGICAL_DAYS
        and _is_sha256(lockstep["chronological_panel_manifest_sha256"])
        and _is_sha256(lockstep["python_panel_builder_sha256"])
        and _is_sha256(lockstep["python_cif_kernel_sha256"])
    )
    lockstep_ready = (
        training_registration_ready
        and not any(item.startswith("lockstep.") for item in blockers)
        and all(_is_sha256(lockstep[field]) for field in _LOCKSTEP_ARTIFACT_FIELDS)
    )

    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "cif_kernel_identity": CIF_KERNEL_IDENTITY,
        "bounded_inputs": {
            "baseline_epoch_manifest_only": True,
            "lifecycle_journal_schema_metadata_only": True,
            "raw_lifecycle_rows_read": False,
            "economic_outcomes_read": False,
            "chronological_day_limit": MAX_METADATA_DAYS,
            "baseline_epoch_limit": MAX_BASELINE_EPOCHS,
        },
        "input_identities": {
            "baseline_manifest_sha256": baseline_manifest["canonical_manifest_sha256"],
            "lifecycle_dataset_id": lifecycle_metadata["dataset_id"],
            "lifecycle_metadata_sha256": canonical_sha256(lifecycle_metadata),
            "journal_schema_version": lifecycle_metadata["journal_schema_version"],
            "journal_v2_schema_sha256": lifecycle_journal_v2_schema_sha256(),
        },
        "gates": {
            "baseline_epoch_ready": baseline_ready,
            "lifecycle_data_admission_ready": data_ready,
            "training_identity_registration_ready": training_registration_ready,
            "chronological_python_cpp_lockstep_execution_ready": lockstep_ready,
        },
        "blockers": sorted(blockers),
        "missing_runtime_integrations": sorted(missing_integrations),
        "missing_artifacts": sorted(missing_artifacts),
        "permissions": {
            "lifecycle_panel_generation_authorized": False,
            "model_training_authorized": False,
            "prediction_evaluation_authorized": False,
            "economic_outcome_read_authorized": False,
            "q90_action_authorized": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
            "baseline_update_authorized": False,
        },
    }
    report["canonical_report_sha256"] = canonical_sha256(report)
    return report


def _load_json(path: Path) -> Mapping[str, object]:
    payload = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ActiveOrderCIFPreflightError(f"JSON root must be an object: {path}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-epoch-manifest", type=Path, required=True)
    parser.add_argument("--lifecycle-metadata", type=Path)
    parser.add_argument(
        "--audit-current-live-v1",
        action="store_true",
        help="Use a mechanics-only metadata description of the current v1 live journal.",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if bool(args.lifecycle_metadata) == bool(args.audit_current_live_v1):
        parser.error("choose exactly one of --lifecycle-metadata or --audit-current-live-v1")

    baseline = _load_json(args.baseline_epoch_manifest)
    metadata = (
        current_unauthoritative_live_journal_metadata(baseline)
        if args.audit_current_live_v1
        else _load_json(args.lifecycle_metadata)
    )
    report = run_active_order_cif_training_preflight(baseline, metadata)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = args.output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(output)
    else:
        print(rendered, end="")
    return 0 if report["gates"]["chronological_python_cpp_lockstep_execution_ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
