#!/usr/bin/env python3
"""Transport fully no-shadow BUY E3 evidence across hosts, fail closed.

This module is additive evidence plumbing only.  It neither changes the live
runtime nor reads economic outcomes, Validation, or sealed holdout data.  The
remote step proves that the exact active process was still alive; the local
step admits four immutable 0600 files into one create-only directory and
exposes a path/inode-free projection for downstream evidence composition.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v8 as resource_v8,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
)
from scripts import f05_buy_e3_active_capture_v8 as active_capture_v8
from scripts import f05_buy_e3_active_release as release_io
from scripts import f05_buy_e3_direct_owner_release_v3 as direct_release_v3
from scripts import f05_buy_e3_evidence_completion as completion

OWNER: Final = completion.OWNER
FORMAL_MODULE_ROUTE: Final = "scripts.f05_buy_e3_cross_host_transport_v6"

# Final operational authority is deliberately source-frozen, never supplied by
# a permissive CLI manifest.  These values remain empty until the no-shadow
# release/resource/active epoch is complete.  Every authority validator fails
# closed while any required value is empty.
FROZEN_FINAL_EXECUTION_COMMIT: Final = "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de"
FROZEN_FINAL_EXECUTION_TREE: Final = "0343bd5586b337385cf2aa0d7a643f5c32b0da77"
FROZEN_FINAL_ANNOTATED_TAG: Final = "f05-owner-buy-e3-no-shadow-runtime-v3-20260824"
FROZEN_FINAL_TAG_OBJECT: Final = "3878ea05252ef8f274b6f74ee7a984431c53b892"
FROZEN_FINAL_RELEASE_SCHEMA: Final = resource_v8.DIRECT_SUCCESSOR_RELEASE_SCHEMA
FROZEN_FINAL_RELEASE_STATUS: Final = resource_v8.DIRECT_SUCCESSOR_RELEASE_STATUS
FROZEN_FINAL_RELEASE_FILE_SHA256: Final = (
    "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49"
)
FROZEN_FINAL_RELEASE_CANONICAL_SHA256: Final = (
    "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f"
)
FROZEN_FINAL_RESOURCE_SCHEMA: Final = resource_v8.RESOURCE_SCHEMA
FROZEN_FINAL_RESOURCE_STATUS: Final = resource_v8.RESOURCE_STATUS
# Frozen in a second additive commit after the fresh remote gate is complete.
FROZEN_FINAL_RESOURCE_FILE_SHA256: Final = ""
FROZEN_FINAL_RESOURCE_CANONICAL_SHA256: Final = ""
FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA: Final = active_capture_v8.SCHEMA_VERSION
FROZEN_FINAL_ACTIVE_CAPTURE_STATUS: Final = active_capture_v8.STATUS
FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256: Final = ""
FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256: Final = ""
FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256: Final = ""
FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256: Final = ""
FROZEN_FINAL_DISABLED_CONFIG_SHA256: Final = (
    "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204"
)
FROZEN_FINAL_ACTIVE_CONFIG_SHA256: Final = (
    "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
)
FROZEN_FINAL_ARTIFACT_SHA256: Final = completion.ARTIFACT_SHA256
FROZEN_FINAL_RESOURCE_PATH_PROVENANCE: Final = ""
FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE: Final = ""
FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE: Final = ""
# Filled only after the no-shadow runtime supplement is emitted and reviewed.
# Keeping this unset makes every final-authority entry point fail closed.
FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT: Final[dict[str, Any] | None] = {
    "schema_version": "f05_buy_e3_no_global_flow_shadow_runtime_fix_supplement.v1",
    "status": "runtime_no_shadow_fix_verified_no_e3_or_sell_semantic_change",
    "file_sha256": "4dc5a379e927380fe282d8dd5167291f3ca3caba3699dbf4457cedb5e3b4ebb7",
    "canonical_field": "canonical_supplement_sha256",
    "canonical_sha256": "bd157ac169d0158ce19c6caf8e4686faf4b47a8a44a8179039a7223d1484393e",
    "size_bytes": 11_880,
    "mode": "0600",
}

REMOTE_ATTESTATION_SCHEMA: Final = f"{OWNER}.remote_active_attestation.v4"
REMOTE_ATTESTATION_STATUS: Final = "remote_active_process_attested_for_cross_host_transport"
REMOTE_ATTESTATION_CANONICAL_FIELD: Final = "canonical_remote_active_attestation_sha256"
ADMISSION_SCHEMA: Final = f"{OWNER}.cross_host_admission.v3"
ADMISSION_STATUS: Final = "cross_host_operational_evidence_admitted"
ADMISSION_CANONICAL_FIELD: Final = "canonical_cross_host_admission_sha256"

RESOURCE_FILENAME: Final = "current_host_resource_gate.json"
ACTIVE_CAPTURE_FILENAME: Final = "active_process_capture.json"
CONFIG_CORRECTION_FILENAME: Final = "config_correction.json"
REMOTE_ATTESTATION_FILENAME: Final = "remote_active_attestation.json"
ADMISSION_FILENAME: Final = "cross_host_admission.json"
SOURCE_FILENAMES: Final = {
    "config_correction": CONFIG_CORRECTION_FILENAME,
    "current_host_resource_gate": RESOURCE_FILENAME,
    "active_process_capture": ACTIVE_CAPTURE_FILENAME,
    "remote_active_attestation": REMOTE_ATTESTATION_FILENAME,
}

CURRENT_PROVIDER: Final = "aws"
CURRENT_REGION: Final = "ap-northeast-1"
CURRENT_PUBLIC_IPV4_PROVENANCE: Final = "13.158.101.253"
CURRENT_INSTANCE_ID: Final = "i-00fe03a8b2fb49a31"
CURRENT_INSTANCE_TYPE: Final = "c7i-flex.large"

CONTENT_BINDING_FIELDS: Final = (
    "schema_version",
    "status",
    "file_sha256",
    "canonical_field",
    "canonical_sha256",
    "size_bytes",
    "mode",
)
PORTABLE_EVIDENCE_FIELDS: Final = (
    "host",
    "runtime_execution",
    "runtime_authority",
    "exact_artifact",
    "resource_disabled_process",
    "transition",
    "active_runtime",
    "source_receipts",
)
REMOTE_REFERENCE_ROLES: Final = (
    "config_correction",
    "current_host_resource_gate",
    "active_process_capture",
    "direct_active_release",
)
_PROCESS_STABLE_FIELDS: Final = (
    "schema_version",
    "pid",
    "pid_start_ticks",
    "cmdline",
    "cmdline_sha256",
    "cwd",
    "config_path",
    "config_sha256",
    "python_executable",
    "python_binary_resolved",
    "venv_root",
    "runtime_identity",
)
_REMOTE_ATTESTATION_FIELDS: Final = {
    "schema_version",
    "identity",
    "status",
    "generated_utc",
    "host",
    "runtime_execution",
    "runtime_authority",
    "exact_artifact",
    "resource_disabled_process",
    "transition",
    "active_runtime",
    "project_references",
    "live_process_attestation",
    "checks",
    "authority_design",
    "permissions",
    "evidence_boundary",
    REMOTE_ATTESTATION_CANONICAL_FIELD,
}
_LIVE_PROCESS_ATTESTATION_FIELDS: Final = {
    "pid",
    "pid_start_ticks",
    "active_capture_process_identity_sha256",
    "recaptured_process_identity_sha256",
    "active_capture_stable_identity_sha256",
    "recaptured_stable_identity_sha256",
    "recaptured_utc",
    "cmdline_sha256",
    "runtime_identity_file_sha256",
    "alive_at_attestation",
    "stable_identity_equal",
}
_ACTIVE_HEALTH_WINDOW_FIELDS: Final = {
    "schema_version",
    "status",
    "log_path_provenance",
    "boundary_offset_bytes",
    "active_pid",
    "active_pid_start_ticks",
    "active_process_stable_identity_sha256",
    "rows",
    "checks",
}
_PORTABLE_ACTIVE_HEALTH_WINDOW_FIELDS: Final = _ACTIVE_HEALTH_WINDOW_FIELDS - {
    "log_path_provenance"
}
_ACTIVE_HEALTH_ROW_FIELDS: Final = {
    "fresh_generation",
    "line_offset_bytes",
    "line_size_bytes",
    "line_sha256",
    "main_wall_timestamp_s",
    "projection",
}
_ACTIVE_HEALTH_WINDOW_CHECKS: Final = {
    "constructor_boundary_only": True,
    "two_consecutive_fresh_main_health_rows": True,
    "same_pid_and_start_ticks_before_between_after": True,
    "sell_owner_enabled_both_rows": True,
    "buy_e3_enabled_both_rows": True,
    "external_sources_absolute_zero_both_rows": True,
    "global_flow_explicit_disabled_error_and_backend_zero_both_rows": True,
    "global_reference_explicit_disabled_error_and_state_zero_both_rows": True,
}
_ADMISSION_FIELDS: Final = {
    "schema_version",
    "identity",
    "status",
    "admitted_utc",
    "portable_evidence",
    "admitted_files",
    "transfer_manifest",
    "checks",
    "authority_design",
    "permissions",
    "evidence_boundary",
    ADMISSION_CANONICAL_FIELD,
}
_FINAL_RELEASE_V3_FIELDS: Final = set(direct_release_v3.TOP_LEVEL_FIELDS)
_PORTABLE_FORBIDDEN_KEYS: Final = frozenset(
    {
        "absolute_path",
        "device",
        "inode",
        "path",
        "remote_path_provenance",
        "repository_root",
    }
)

NO_AUTHORITY: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "shadow_or_companion_collection_enabled": False,
    "hypothetical_live_actions_scored": False,
}
TRANSPORT_AUTHORITY_DESIGN: Final = {
    "runtime_authority": "source_frozen_final_owner_release",
    "runtime_authority_replaced": False,
    "runtime_consumed": False,
    "does_not_replace_runtime_active_release": True,
    "retrospective_authority_created": False,
    "evidence_is_additive_only": True,
    "cross_host_transport_only": True,
    "remote_paths_are_provenance_only": True,
    "remote_inode_is_not_authority": True,
    "local_admission_is_not_runtime_authority": True,
}


class CrossHostTransportError(RuntimeError):
    """Raised when exact BUY E3 transport evidence cannot be proven safely."""


@dataclass(frozen=True)
class _SourceSet:
    correction: dict[str, Any]
    resource: dict[str, Any]
    active: dict[str, Any]
    attestation: dict[str, Any] | None
    release: dict[str, Any]
    bindings: dict[str, dict[str, Any]]


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _timestamp(value: Any, label: str) -> str:
    try:
        normalized = str(value)
        parsed = datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise CrossHostTransportError(f"{label} is not canonical UTC") from exc
    if not normalized.endswith("Z") or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise CrossHostTransportError(f"{label} is not canonical UTC")
    return normalized


def _utc_datetime(value: Any, label: str) -> datetime:
    normalized = _timestamp(value, label)
    return datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return _canonical_sha256(body)


def _require_sha256(value: Any, label: str) -> str:
    try:
        return completion._require_sha256(value, label)  # noqa: SLF001
    except Exception as exc:
        raise CrossHostTransportError(f"{label} is not a lowercase SHA256") from exc


def _require_git_sha(value: Any, label: str) -> str:
    try:
        return completion._require_git_sha(value, label)  # noqa: SLF001
    except Exception as exc:
        raise CrossHostTransportError(f"{label} is not a lowercase git SHA") from exc


def _frozen_final_execution() -> dict[str, str]:
    if not all(
        (
            FROZEN_FINAL_ANNOTATED_TAG,
            FROZEN_FINAL_RELEASE_SCHEMA,
            FROZEN_FINAL_RELEASE_STATUS,
            FROZEN_FINAL_RESOURCE_SCHEMA,
            FROZEN_FINAL_RESOURCE_STATUS,
            FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
            FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
            FROZEN_FINAL_RESOURCE_PATH_PROVENANCE,
            FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE,
            FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE,
        )
    ):
        raise CrossHostTransportError("final no-shadow authority is not source-frozen")
    if FROZEN_FINAL_RELEASE_SCHEMA.endswith(".v1"):
        raise CrossHostTransportError("stale direct-owner release v1 cannot be final authority")
    if (
        not isinstance(FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT, Mapping)
        or set(FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT) != set(CONTENT_BINDING_FIELDS)
        or _content_from_mapping(
            FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT,
            "final no-shadow runtime supplement",
        )
        != dict(FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT)
    ):
        raise CrossHostTransportError("final no-shadow runtime supplement is not source-frozen")
    for value, label in (
        (FROZEN_FINAL_EXECUTION_COMMIT, "final execution commit"),
        (FROZEN_FINAL_EXECUTION_TREE, "final execution tree"),
        (FROZEN_FINAL_TAG_OBJECT, "final annotated tag object"),
    ):
        _require_git_sha(value, label)
    for value, label in (
        (FROZEN_FINAL_RELEASE_FILE_SHA256, "final release file"),
        (FROZEN_FINAL_RELEASE_CANONICAL_SHA256, "final release canonical"),
        (FROZEN_FINAL_RESOURCE_FILE_SHA256, "final resource file"),
        (FROZEN_FINAL_RESOURCE_CANONICAL_SHA256, "final resource canonical"),
        (FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256, "final active capture file"),
        (FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256, "final active capture canonical"),
        (FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256, "final config correction file"),
        (
            FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256,
            "final config correction canonical",
        ),
        (FROZEN_FINAL_DISABLED_CONFIG_SHA256, "final disabled config"),
        (FROZEN_FINAL_ACTIVE_CONFIG_SHA256, "final active config"),
        (FROZEN_FINAL_ARTIFACT_SHA256, "final E3 artifact"),
    ):
        _require_sha256(value, label)
    for value, label in (
        (FROZEN_FINAL_RESOURCE_PATH_PROVENANCE, "final resource path provenance"),
        (FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE, "final active path provenance"),
        (FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE, "config correction path provenance"),
    ):
        if not PurePosixPath(value).is_absolute():
            raise CrossHostTransportError(f"{label} is not absolute")
    return {
        "execution_commit": FROZEN_FINAL_EXECUTION_COMMIT,
        "execution_tree": FROZEN_FINAL_EXECUTION_TREE,
        "annotated_operational_tag": FROZEN_FINAL_ANNOTATED_TAG,
        "annotated_operational_tag_object": FROZEN_FINAL_TAG_OBJECT,
        "tag_peeled_commit": FROZEN_FINAL_EXECUTION_COMMIT,
    }


def _open_private_json(path: Path, label: str) -> Any:
    try:
        return release_io._open_document(path, label)  # noqa: SLF001
    except Exception as exc:
        raise CrossHostTransportError(
            f"{label} is not a private 0600 single-link JSON file"
        ) from exc


def _content_binding(
    opened: Any,
    *,
    canonical_field: str,
    expected_schema: str,
    expected_status: str,
) -> dict[str, Any]:
    payload = opened.payload
    canonical = _require_sha256(payload.get(canonical_field), "receipt canonical identity")
    if (
        payload.get("schema_version") != expected_schema
        or payload.get("status") != expected_status
        or canonical != _document_sha256(payload, canonical_field)
    ):
        raise CrossHostTransportError("receipt schema/status/canonical identity drifted")
    return {
        "schema_version": expected_schema,
        "status": expected_status,
        "file_sha256": hashlib.sha256(opened.raw).hexdigest(),
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
        "size_bytes": len(opened.raw),
        "mode": "0600",
    }


def _content_from_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(CONTENT_BINDING_FIELDS):
        raise CrossHostTransportError(f"{label} content binding fields drifted")
    result = {field: value.get(field) for field in CONTENT_BINDING_FIELDS}
    if set(result) != set(CONTENT_BINDING_FIELDS):
        raise CrossHostTransportError(f"{label} content binding fields drifted")
    _require_sha256(result["file_sha256"], f"{label} file SHA256")
    _require_sha256(result["canonical_sha256"], f"{label} canonical SHA256")
    if (
        not isinstance(result["schema_version"], str)
        or not isinstance(result["status"], (str, type(None)))
        or not isinstance(result["canonical_field"], str)
        or not isinstance(result["size_bytes"], int)
        or isinstance(result["size_bytes"], bool)
        or result["size_bytes"] <= 0
        or result["mode"] != "0600"
    ):
        raise CrossHostTransportError(f"{label} content binding is malformed")
    return result


def _artifact_projection(release: Mapping[str, Any]) -> dict[str, Any]:
    artifact = release.get("exact_artifact")
    roles = artifact.get("roles") if isinstance(artifact, Mapping) else None
    if (
        not isinstance(artifact, Mapping)
        or artifact.get("artifact_sha256") != FROZEN_FINAL_ARTIFACT_SHA256
        or not isinstance(roles, Mapping)
        or set(roles) != {"manifest", "policy", "predicate_bundle"}
    ):
        raise CrossHostTransportError("frozen final exact artifact projection drifted")
    projected: dict[str, Any] = {}
    for role in ("manifest", "policy", "predicate_bundle"):
        row = roles[role]
        if not isinstance(row, Mapping):
            raise CrossHostTransportError(f"artifact role is malformed: {role}")
        projected[role] = {
            "schema_version": row.get("schema_version"),
            "status": row.get("status"),
            "file_sha256": _require_sha256(row.get("file_sha256"), f"{role} file SHA256"),
            "canonical_field": row.get("canonical_field"),
            "canonical_sha256": _require_sha256(
                row.get("canonical_sha256"), f"{role} canonical SHA256"
            ),
            "size_bytes": row.get("size_bytes"),
            "mode": row.get("mode"),
        }
        _content_from_mapping(projected[role], f"artifact {role}")
    return {"artifact_sha256": FROZEN_FINAL_ARTIFACT_SHA256, "roles": projected}


def _contains_mapping_key(value: Any, forbidden: str) -> bool:
    if isinstance(value, Mapping):
        return forbidden in value or any(
            _contains_mapping_key(nested, forbidden) for nested in value.values()
        )
    if isinstance(value, list):
        return any(_contains_mapping_key(nested, forbidden) for nested in value)
    return False


def validate_runtime_authority(root: Path, release: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the one source-frozen final owner release and clean checkout."""

    expected_execution = _frozen_final_execution()
    repository = root.expanduser().resolve(strict=True)
    try:
        observed_execution = release_io._operational_git_identity(  # noqa: SLF001
            repository, FROZEN_FINAL_ANNOTATED_TAG
        )
    except Exception as exc:
        raise CrossHostTransportError("final runtime repository identity is invalid") from exc
    if observed_execution != expected_execution:
        raise CrossHostTransportError("final runtime repository is not the frozen authority")
    opened = _open_private_json(release, "final immutable owner release")
    binding = _content_binding(
        opened,
        canonical_field="canonical_active_release_sha256",
        expected_schema=FROZEN_FINAL_RELEASE_SCHEMA,
        expected_status=FROZEN_FINAL_RELEASE_STATUS,
    )
    payload = dict(opened.payload)
    parent_authority = {
        "release": dict(direct_release_v3.PARENT_RELEASE_V2_BINDING),
        "execution": dict(direct_release_v3.PARENT_EXECUTION),
    }
    config_pair = payload.get("config_pair")
    if not isinstance(config_pair, Mapping):
        raise CrossHostTransportError("final owner release config pair is missing")
    disabled_config = config_pair.get("disabled")
    active_config = config_pair.get("active")
    expected_config_binding = {
        "disabled": {
            "file_sha256": FROZEN_FINAL_DISABLED_CONFIG_SHA256,
            "semantic_sha256": direct_release_v3.NEW_DISABLED_CONFIG_SEMANTIC_SHA256,
            "size_bytes": direct_release_v3.NEW_DISABLED_CONFIG_SIZE,
            "mode": "0600",
        },
        "active": {
            "file_sha256": FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
            "semantic_sha256": direct_release_v3.NEW_ACTIVE_CONFIG_SEMANTIC_SHA256,
            "size_bytes": direct_release_v3.NEW_ACTIVE_CONFIG_SIZE,
            "mode": "0600",
        },
    }
    if (
        disabled_config != expected_config_binding["disabled"]
        or active_config != (expected_config_binding["active"])
    ):
        raise CrossHostTransportError("final owner release config bytes drifted")
    if (
        set(payload) != _FINAL_RELEASE_V3_FIELDS
        or binding["file_sha256"] != FROZEN_FINAL_RELEASE_FILE_SHA256
        or binding["canonical_sha256"] != FROZEN_FINAL_RELEASE_CANONICAL_SHA256
        or payload.get("identity") != FROZEN_FINAL_RELEASE_SCHEMA
        or payload.get("execution") != expected_execution
        or payload.get("research_supported") is not False
        or payload.get("formal_hierarchy_passed") is not False
        or payload.get("formal_hard_gates_passed") is not False
        or payload.get("owner_risk_accepted") is not True
        or payload.get("outcome_informed_owner_override") is not True
        or payload.get("action_authorized") is not True
        or payload.get("live_authorized") is not True
        or payload.get("authorization_basis") != direct_release_v3.AUTHORIZATION_BASIS
        or payload.get("parent_runtime_authority") != parent_authority
        or payload.get("historical_evidence") != direct_release_v3.HISTORICAL_EVIDENCE
        or payload.get("pending_current_runtime_evidence")
        != direct_release_v3.PENDING_CURRENT_RUNTIME_EVIDENCE
        or payload.get("runtime_fix_contract") != direct_release_v3.RUNTIME_FIX_CONTRACT
        or payload.get("runtime_fix_supplement") != FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT
        or payload.get("no_shadow_runtime_contract") != direct_release_v3.NO_SHADOW_RUNTIME_CONTRACT
        or payload.get("evidence_boundary") != direct_release_v3.EVIDENCE_BOUNDARY
        or _contains_mapping_key(payload, "incomplete_evidence")
        or _contains_mapping_key(payload, "panel_rebuild_continues")
        or payload.get("scope") != direct_release_v3.SCOPE
        or payload.get("rollback") != direct_release_v3.ROLLBACK
        or config_pair.get("schema_version") != "f05_buy_e3_no_shadow_config_pair.v1"
        or config_pair.get("status") != "exact_no_shadow_config_pair_frozen"
        or config_pair.get("predecessor")
        != {
            "disabled_file_sha256": direct_release_v3.OLD_DISABLED_CONFIG_SHA256,
            "active_file_sha256": direct_release_v3.OLD_ACTIVE_CONFIG_SHA256,
        }
        or config_pair.get("old_to_new_semantic_additions")
        != list(direct_release_v3.NEW_CONFIG_ADDITIONS)
        or config_pair.get("active_disabled_only_difference")
        != direct_release_v3.CONFIG_PAIR_DIFFERENCE
        or config_pair.get("required_false_paths")
        != list(direct_release_v3.REQUIRED_FALSE_CONFIG_PATHS)
        or config_pair.get("external_shadow_only_marker_inert") is not True
        or config_pair.get("release_fields_present_in_yaml") is not False
    ):
        raise CrossHostTransportError("final owner release semantic authority drifted")
    _timestamp(payload.get("generated_utc"), "final owner release timestamp")
    _artifact_projection(payload)
    return payload, binding


def _direct_authority(
    direct_release_path: Path, *, direct_repository_root: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    return validate_runtime_authority(direct_repository_root, direct_release_path)


def _reject_forbidden_evidence(value: Any, label: str) -> None:
    if not isinstance(value, Mapping):
        raise CrossHostTransportError(f"{label} evidence boundary is missing")
    forbidden = (
        "economic_outcomes_read",
        "economic_values_persisted",
        "validation_read",
        "sealed_holdout_read",
        "new_economic_arm_run",
        "shadow_created",
        "companion_created",
        "hypothetical_live_actions_scored",
        "connected_to_live_market_stream",
        "benchmark_action_rows_persisted",
        "action_authorized_by_resource_receipt",
        "live_authorized_by_resource_receipt",
    )
    if any(value.get(name) is not False for name in forbidden if name in value):
        raise CrossHostTransportError(f"{label} exceeds the transport evidence boundary")


def _disabled_process(resource: Mapping[str, Any]) -> dict[str, Any]:
    process = resource.get("fresh_disabled_process")
    if not isinstance(process, Mapping):
        raise CrossHostTransportError("resource receipt lacks a disabled process")
    pid = process.get("pid", process.get("disabled_pid"))
    start = process.get("pid_start_ticks", process.get("disabled_pid_start_ticks"))
    identity = process.get(
        "canonical_process_identity_sha256",
        process.get("disabled_process_identity_sha256"),
    )
    config_sha = process.get("config_sha256", process.get("disabled_config_sha256"))
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or not isinstance(start, int)
        or isinstance(start, bool)
        or start <= 0
        or _require_sha256(identity, "disabled process identity") != identity
        or _require_sha256(config_sha, "disabled config") != config_sha
        or config_sha != FROZEN_FINAL_DISABLED_CONFIG_SHA256
        or any(
            process.get(name) is not True
            for name in ("fresh_pid", "fresh_start_ticks", "same_pid_pre_post")
        )
    ):
        raise CrossHostTransportError("resource disabled process identity drifted")
    return {
        "pid": pid,
        "pid_start_ticks": start,
        "canonical_process_identity_sha256": identity,
        "config_sha256": config_sha,
        "fresh_pid": True,
        "fresh_start_ticks": True,
        "same_pid_pre_post": True,
    }


def _resource_runtime_sources(resource: Mapping[str, Any]) -> tuple[str, dict[str, str]]:
    authority = resource.get("runtime_sources")
    rows = authority.get("files") if isinstance(authority, Mapping) else None
    manifest = (
        authority.get("runtime_source_manifest_sha256") if isinstance(authority, Mapping) else None
    )
    if not isinstance(rows, Mapping) or set(rows) != set(
        resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256
    ):
        raise CrossHostTransportError("resource runtime-source authority fields drifted")
    projected: dict[str, str] = {}
    for role, frozen in resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items():
        row = rows.get(role)
        if (
            not isinstance(row, Mapping)
            or row.get("role") != role
            or row.get("repository_relative_path") != frozen["path"]
            or row.get("sha256") != frozen["sha256"]
            or any(
                row.get(name) is not True
                for name in (
                    "runtime_working_matches_direct_successor",
                    "collector_working_matches_direct_successor",
                    "collector_head_matches_direct_successor",
                )
            )
            or frozen["path"] in projected
        ):
            raise CrossHostTransportError(f"resource runtime source drifted: {role}")
        projected[str(frozen["path"])] = _require_sha256(
            frozen["sha256"], f"resource runtime source {role}"
        )
    return _require_sha256(manifest, "resource runtime-source manifest"), projected


def _validate_resource_execution(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise CrossHostTransportError("resource runtime execution is missing")
    expected = _frozen_final_execution()
    if (
        value.get("execution_commit") != expected["execution_commit"]
        or value.get("execution_tree") != expected["execution_tree"]
        or value.get("annotated_tag", value.get("annotated_operational_tag"))
        != expected["annotated_operational_tag"]
        or value.get("annotated_tag_object", value.get("annotated_operational_tag_object"))
        != expected["annotated_operational_tag_object"]
        or value.get("tag_peeled_commit") != expected["tag_peeled_commit"]
    ):
        raise CrossHostTransportError("resource runtime execution is not the frozen final release")


def _validate_config_correction(path: Path) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    try:
        payload, binding = resource_v8.config_successor.validate_content_receipt(path)
    except Exception as exc:
        raise CrossHostTransportError("config correction receipt is invalid") from exc
    opened = _open_private_json(path, "config correction receipt")
    if payload != opened.payload or binding != _content_binding(
        opened,
        canonical_field=resource_v8.config_successor.CANONICAL_FIELD,
        expected_schema=resource_v8.config_successor.SCHEMA_VERSION,
        expected_status=resource_v8.config_successor.STATUS,
    ):
        raise CrossHostTransportError("config correction bytes changed during validation")
    if (
        binding["file_sha256"] != FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256
        or binding["canonical_sha256"] != FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256
    ):
        raise CrossHostTransportError("config correction frozen identity drifted")
    return payload, binding, opened.raw


def _validate_resource(
    path: Path,
    *,
    config_correction_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    opened = _open_private_json(path, "current-host resource receipt")
    binding = _content_binding(
        opened,
        canonical_field="canonical_resource_receipt_sha256",
        expected_schema=FROZEN_FINAL_RESOURCE_SCHEMA,
        expected_status=FROZEN_FINAL_RESOURCE_STATUS,
    )
    resource = dict(opened.payload)
    try:
        semantically_validated = resource_v8.validate_resource_receipt(
            path,
            config_correction_path=config_correction_path,
        )
    except Exception as exc:
        raise CrossHostTransportError(
            "current-host resource-v8 semantic validation failed"
        ) from exc
    if semantically_validated != resource:
        raise CrossHostTransportError("current-host resource-v8 bytes changed during validation")
    host = resource.get("host")
    authority = resource.get("authority_design")
    deployed = resource.get("exact_deployed_files")
    _correction_payload, correction_binding, _correction_raw = _validate_config_correction(
        config_correction_path
    )
    checks = resource.get("checks")
    _validate_resource_execution(resource.get("runtime_execution"))
    _disabled_process(resource)
    if (
        binding["file_sha256"] != FROZEN_FINAL_RESOURCE_FILE_SHA256
        or binding["canonical_sha256"] != FROZEN_FINAL_RESOURCE_CANONICAL_SHA256
        or not isinstance(host, Mapping)
        or host.get("instance_id") != CURRENT_INSTANCE_ID
        or host.get("instance_type") != CURRENT_INSTANCE_TYPE
        or not isinstance(authority, Mapping)
        or authority.get("runtime_authority_release_file_sha256")
        != FROZEN_FINAL_RELEASE_FILE_SHA256
        or authority.get("runtime_authority_release_canonical_sha256")
        != FROZEN_FINAL_RELEASE_CANONICAL_SHA256
        or not isinstance(deployed, Mapping)
        or resource.get("config_correction") != correction_binding
        or deployed.get("artifact_sha256") != FROZEN_FINAL_ARTIFACT_SHA256
        or not isinstance(checks, Mapping)
        or not checks
        or any(value is not True for value in checks.values())
    ):
        raise CrossHostTransportError("resource receipt frozen final identity drifted")
    _reject_forbidden_evidence(resource.get("evidence_boundary"), "resource receipt")
    return resource, binding, opened.raw


def _startup_runtime_sources(runtime: Mapping[str, Any]) -> tuple[str, dict[str, str]]:
    startup = runtime.get("startup_attestation")
    checkout = startup.get("running_checkout") if isinstance(startup, Mapping) else None
    rows = checkout.get("runtime_source_files") if isinstance(checkout, Mapping) else None
    manifest = (
        checkout.get("runtime_source_manifest_sha256") if isinstance(checkout, Mapping) else None
    )
    if not isinstance(rows, list) or not rows:
        raise CrossHostTransportError("active runtime source rows are missing")
    files: dict[str, str] = {}
    for row in rows:
        if (
            not isinstance(row, Mapping)
            or not isinstance(row.get("path"), str)
            or not row["path"]
            or row.get("matches_head_blob") is not True
            or row["path"] in files
        ):
            raise CrossHostTransportError("active runtime source row is malformed")
        files[row["path"]] = _require_sha256(
            row.get("working_file_sha256"), f"active runtime source {row['path']}"
        )
    return _require_sha256(manifest, "active runtime source manifest"), files


def _active_runtime_semantics(
    runtime: Mapping[str, Any],
    *,
    active_pid: int,
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
) -> dict[str, Any]:
    roles = release["exact_artifact"]["roles"]
    startup = runtime.get("startup_attestation")
    if not isinstance(startup, Mapping):
        raise CrossHostTransportError("active startup attestation is missing")
    gates = startup.get("gates")
    checkout = startup.get("running_checkout")
    startup_release = startup.get("buy_e3_active_release")
    state = startup.get("fill_cooldown_state")
    expected_execution = _frozen_final_execution()
    try:
        shadow_semantics = active_capture_v8._shadow_runtime_semantics(  # noqa: SLF001
            startup, runtime
        )
    except Exception as exc:
        raise CrossHostTransportError("active no-shadow runtime semantics drifted") from exc
    if (
        runtime.get("schema_version") != "narrowgate_live_runtime_identity.v1"
        or runtime.get("pid") != active_pid
        or runtime.get("config_sha256") != FROZEN_FINAL_ACTIVE_CONFIG_SHA256
        or runtime.get("f05_buy_e3_enabled") is not True
        or runtime.get("f05_buy_e3_owner_override_effective") is not True
        or runtime.get("f05_buy_e3_artifact_sha256") != FROZEN_FINAL_ARTIFACT_SHA256
        or runtime.get("f05_buy_e3_artifact_manifest_sha256") != roles["manifest"]["file_sha256"]
        or runtime.get("f05_buy_e3_policy_sha256") != roles["policy"]["file_sha256"]
        or runtime.get("f05_buy_e3_predicate_bundle_sha256")
        != roles["predicate_bundle"]["file_sha256"]
        or runtime.get("f05_buy_e3_required") is not True
        or runtime.get("f05_buy_e3_active_release_file_sha256") != release_binding["file_sha256"]
        or runtime.get("f05_buy_e3_active_release_canonical_sha256")
        != release_binding["canonical_sha256"]
        or runtime.get("global_flow_shadow_enabled") is not False
        or runtime.get("global_reference_shadow_enabled") is not False
        or runtime.get("global_flow_shadow_config_explicit") is not True
        or runtime.get("global_reference_shadow_config_explicit") is not True
        or startup.get("schema_version") != active_capture_v8.STARTUP_ATTESTATION_SCHEMA
        or startup.get("status") != "accepted"
        or startup.get("errors") != []
        or not isinstance(gates, Mapping)
        or not gates
        or any(value is not True for value in gates.values())
        or not isinstance(checkout, Mapping)
        or checkout.get("git_commit") != expected_execution["execution_commit"]
        or checkout.get("git_tree") != expected_execution["execution_tree"]
        or checkout.get("git_worktree_clean") is not True
        or not isinstance(startup_release, Mapping)
        or startup_release.get("file_sha256") != release_binding["file_sha256"]
        or startup_release.get("file_canonical_sha256") != release_binding["canonical_sha256"]
        or startup_release.get("execution_commit") != expected_execution["execution_commit"]
        or startup_release.get("execution_tree") != expected_execution["execution_tree"]
        or startup_release.get("annotated_operational_tag")
        != expected_execution["annotated_operational_tag"]
        or startup_release.get("annotated_operational_tag_object")
        != expected_execution["annotated_operational_tag_object"]
        or startup_release.get("active_config_file_sha256") != FROZEN_FINAL_ACTIVE_CONFIG_SHA256
        or startup_release.get("disabled_config_file_sha256") != FROZEN_FINAL_DISABLED_CONFIG_SHA256
        or gates.get("buy_e3_active_release_matches_running_config") is not True
        or not isinstance(state, Mapping)
    ):
        raise CrossHostTransportError("active startup/runtime identity drifted")
    buy_identity = str(state.get("buy_deadline_identity", ""))
    if buy_identity not in {"B0", f"BUY_E3:{FROZEN_FINAL_ARTIFACT_SHA256}"}:
        raise CrossHostTransportError("active startup imported an unknown BUY deadline")
    if state.get("e3_deadline_imported") is True and buy_identity == "B0":
        raise CrossHostTransportError("active startup relabeled an E3 deadline as B0")
    _runtime_source_manifest, runtime_source_files = _startup_runtime_sources(runtime)
    expected_startup_sources = {
        str(binding["path"]): str(binding["sha256"])
        for role, binding in resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items()
        if role in set(active_capture_v8.STARTUP_SOURCE_ROLE_MAP.values())
    }
    if runtime_source_files != expected_startup_sources:
        raise CrossHostTransportError(
            "active startup runtime-source bytes drifted from no-shadow successor"
        )
    return {
        "startup_attestation_sha256": _canonical_sha256(startup),
        "startup_status": "accepted",
        "running_checkout_commit": expected_execution["execution_commit"],
        "running_checkout_tree": expected_execution["execution_tree"],
        "buy_deadline_identity": buy_identity,
        "fill_cooldown_restore_mode": state.get("restore_mode"),
        "buy_remaining_ms": state.get("buy_remaining_ms"),
        "e3_deadline_imported": state.get("e3_deadline_imported"),
        "shadow_runtime": shadow_semantics,
    }


def _validate_active_health_window_content(
    raw: Any,
    *,
    process: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the path-free semantics available after cross-host transfer.

    The remote attestation separately proves that the exact source receipt was
    revalidated against the live log while that log was still available.  A
    local admission never pretends it can reopen that remote inode.
    """

    if not isinstance(raw, Mapping) or set(raw) != _ACTIVE_HEALTH_WINDOW_FIELDS:
        raise CrossHostTransportError("active HEALTH window fields drifted")
    window = dict(raw)
    remote_log = str(window.get("log_path_provenance", ""))
    boundary = window.get("boundary_offset_bytes")
    rows = window.get("rows")
    stable_sha = _canonical_sha256(_stable_process_projection(process))
    if (
        window.get("schema_version") != active_capture_v8.HEALTH_WINDOW_SCHEMA
        or window.get("status") != active_capture_v8.HEALTH_WINDOW_STATUS
        or not PurePosixPath(remote_log).is_absolute()
        or type(boundary) is not int
        or boundary < 0
        or window.get("active_pid") != process.get("pid")
        or window.get("active_pid_start_ticks") != process.get("pid_start_ticks")
        or window.get("active_process_stable_identity_sha256") != stable_sha
        or not isinstance(rows, list)
        or len(rows) != 2
        or window.get("checks") != _ACTIVE_HEALTH_WINDOW_CHECKS
    ):
        raise CrossHostTransportError("active HEALTH window identity drifted")

    projected_rows: list[dict[str, Any]] = []
    previous_end = boundary
    previous_timestamp = -math.inf
    previous_updates = -1
    for generation, raw_row in enumerate(rows, start=1):
        if not isinstance(raw_row, Mapping) or set(raw_row) != _ACTIVE_HEALTH_ROW_FIELDS:
            raise CrossHostTransportError("active HEALTH row fields drifted")
        row = dict(raw_row)
        offset = row.get("line_offset_bytes")
        size = row.get("line_size_bytes")
        timestamp = row.get("main_wall_timestamp_s")
        if (
            row.get("fresh_generation") != generation
            or type(offset) is not int
            or offset < previous_end
            or type(size) is not int
            or size <= 0
            or isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not math.isfinite(float(timestamp))
            or float(timestamp) <= previous_timestamp
        ):
            raise CrossHostTransportError("active HEALTH row chronology drifted")
        _require_sha256(row.get("line_sha256"), "active HEALTH line")
        try:
            projection = active_capture_v8._validate_health_projection(  # noqa: SLF001
                row.get("projection")
            )
        except Exception as exc:
            raise CrossHostTransportError("active HEALTH no-shadow projection drifted") from exc
        updates = projection["boolean_cooldown_updates"]
        if updates <= previous_updates:
            raise CrossHostTransportError("active HEALTH callback progress drifted")
        row["projection"] = projection
        projected_rows.append(row)
        previous_end = offset + size
        previous_timestamp = float(timestamp)
        previous_updates = updates

    portable = {key: value for key, value in window.items() if key != "log_path_provenance"}
    portable["rows"] = projected_rows
    if set(portable) != _PORTABLE_ACTIVE_HEALTH_WINDOW_FIELDS:
        raise CrossHostTransportError("portable active HEALTH fields drifted")
    _assert_portable(portable, location="active_runtime.active_health_window")
    return portable


def _validate_active_payload(
    payload: Mapping[str, Any],
    *,
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
    resource: Mapping[str, Any],
    resource_binding: Mapping[str, Any],
) -> dict[str, Any]:
    fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "runtime_authority",
        "resource_receipt",
        "config_correction",
        "host",
        "disabled_predecessor",
        "active_process",
        "runtime_identity",
        "runtime_identity_file_sha256",
        "startup_semantics",
        "active_health_window",
        "checks",
        "authority_design",
        "permissions",
        "evidence_boundary",
        "canonical_active_capture_sha256",
    }
    process = payload.get("active_process")
    predecessor = payload.get("disabled_predecessor")
    runtime = payload.get("runtime_identity")
    if (
        set(payload) != fields
        or payload.get("schema_version") != FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA
        or payload.get("identity") != OWNER
        or payload.get("status") != FROZEN_FINAL_ACTIVE_CAPTURE_STATUS
        or not isinstance(process, Mapping)
        or not isinstance(predecessor, Mapping)
        or not isinstance(runtime, Mapping)
    ):
        raise CrossHostTransportError("active capture structure drifted")
    if _content_from_mapping(payload.get("runtime_authority"), "active runtime authority") != dict(
        release_binding
    ):
        raise CrossHostTransportError("active capture final release content drifted")
    if _content_from_mapping(payload.get("resource_receipt"), "active resource receipt") != dict(
        resource_binding
    ):
        raise CrossHostTransportError("active capture resource content drifted")
    if _content_from_mapping(payload.get("config_correction"), "active config correction") != dict(
        resource.get("config_correction", {})
    ):
        raise CrossHostTransportError("active capture config correction content drifted")
    disabled = _disabled_process(resource)
    expected_predecessor = {
        "pid": int(disabled["pid"]),
        "pid_start_ticks": int(disabled["pid_start_ticks"]),
        "process_identity_sha256": disabled.get("canonical_process_identity_sha256"),
        "quiescent_before_active_capture": True,
    }
    runtime_file_sha = _require_sha256(
        payload.get("runtime_identity_file_sha256"), "active runtime identity file"
    )
    if process.get("runtime_identity_file_sha256") != runtime_file_sha:
        raise CrossHostTransportError("active process lost its runtime identity binding")
    semantics = _active_runtime_semantics(
        runtime,
        active_pid=int(process.get("pid", -1)),
        release=release,
        release_binding=release_binding,
    )
    process_body = dict(process)
    process_canonical = _require_sha256(
        process_body.pop("canonical_process_identity_sha256", None),
        "active process identity",
    )
    if process_canonical != _canonical_sha256(process_body):
        raise CrossHostTransportError("active process canonical identity drifted")
    health_window = _validate_active_health_window_content(
        payload.get("active_health_window"),
        process=process,
    )
    checks = payload.get("checks")
    authority_design = payload.get("authority_design")
    if not isinstance(checks, Mapping) or not isinstance(authority_design, Mapping):
        raise CrossHostTransportError("active capture checks/authority design are missing")
    if (
        payload.get("host") != resource.get("host")
        or predecessor != expected_predecessor
        or int(process.get("pid", -1)) == int(disabled["pid"])
        or int(process.get("pid_start_ticks", -1)) <= int(disabled["pid_start_ticks"])
        or process.get("execution_commit") != FROZEN_FINAL_EXECUTION_COMMIT
        or process.get("execution_tree") != FROZEN_FINAL_EXECUTION_TREE
        or process.get("artifact_sha256") != FROZEN_FINAL_ARTIFACT_SHA256
        or process.get("buy_e3_enabled") is not True
        or process.get("owner_override_effective") is not True
        or process.get("startup_attestation_sha256") != semantics["startup_attestation_sha256"]
        or payload.get("startup_semantics") != semantics
        or checks != active_capture_v8.CHECKS
        or authority_design != active_capture_v8.AUTHORITY_DESIGN
        or payload.get("permissions") != active_capture_v8.NO_AUTHORITY
        or payload.get("evidence_boundary") != active_capture_v8.EVIDENCE_BOUNDARY
    ):
        raise CrossHostTransportError("active capture semantic identity drifted")
    generated = _utc_datetime(payload.get("generated_utc"), "active capture timestamp")
    health_observed = datetime.fromtimestamp(
        float(health_window["rows"][1]["main_wall_timestamp_s"]),
        tz=UTC,
    )
    if generated < health_observed:
        raise CrossHostTransportError("active capture predates its portable HEALTH window")
    return semantics


def _validate_active(
    path: Path,
    *,
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
    resource: Mapping[str, Any],
    resource_binding: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], bytes, dict[str, Any]]:
    opened = _open_private_json(path, "active process capture")
    binding = _content_binding(
        opened,
        canonical_field="canonical_active_capture_sha256",
        expected_schema=FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
        expected_status=FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
    )
    if (
        binding["file_sha256"] != FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256
        or binding["canonical_sha256"] != FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256
    ):
        raise CrossHostTransportError("active capture exact byte identity drifted")
    payload = dict(opened.payload)
    semantics = _validate_active_payload(
        payload,
        release=release,
        release_binding=release_binding,
        resource=resource,
        resource_binding=resource_binding,
    )
    return payload, binding, opened.raw, semantics


def _validate_active_against_remote_log(
    path: Path,
    *,
    live_log_path: Path,
    direct_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    config_correction_path: Path,
) -> dict[str, Any]:
    try:
        return active_capture_v8.validate_active_capture(
            path,
            runtime_repository_root=direct_repository_root,
            direct_release_path=direct_release_path,
            resource_receipt_path=resource_receipt_path,
            config_correction_path=config_correction_path,
            live_log_path=live_log_path,
        )
    except Exception as exc:
        raise CrossHostTransportError(
            "active capture failed remote live-log semantic validation"
        ) from exc


def _runtime_projection(active: Mapping[str, Any], semantics: Mapping[str, Any]) -> dict[str, Any]:
    runtime = active["runtime_identity"]
    runtime_source_manifest, runtime_source_files = _startup_runtime_sources(runtime)
    startup = runtime["startup_attestation"]
    return {
        "config_sha256": _require_sha256(runtime.get("config_sha256"), "runtime config"),
        "runtime_identity": {
            "schema_version": runtime.get("schema_version"),
            "file_sha256": _require_sha256(
                active.get("runtime_identity_file_sha256"), "runtime identity file"
            ),
            "canonical_sha256": _canonical_sha256(runtime),
        },
        "startup_attestation": {
            "schema_version": startup.get("schema_version"),
            "status": startup.get("status"),
            "canonical_sha256": _canonical_sha256(startup),
        },
        "runtime_source_manifest_sha256": runtime_source_manifest,
        "runtime_source_files": runtime_source_files,
        "artifact_sha256": FROZEN_FINAL_ARTIFACT_SHA256,
        "buy_e3_enabled": True,
        "owner_override_effective": True,
        "startup_semantics": dict(semantics),
        "active_health_window": _validate_active_health_window_content(
            active.get("active_health_window"),
            process=active["active_process"],
        ),
    }


def _portable_components(
    *,
    resource: Mapping[str, Any],
    active: Mapping[str, Any],
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
    semantics: Mapping[str, Any],
) -> dict[str, Any]:
    disabled = _disabled_process(resource)
    resource_source_manifest, resource_source_files = _resource_runtime_sources(resource)
    process = active["active_process"]
    return {
        "host": {
            "provider": CURRENT_PROVIDER,
            "region": CURRENT_REGION,
            "instance_id": resource["host"]["instance_id"],
            "instance_type": resource["host"]["instance_type"],
            "public_ipv4": CURRENT_PUBLIC_IPV4_PROVENANCE,
            "public_ipv4_role": "network_locator_provenance_only_not_host_authority",
            "resource_host_identity": dict(resource["host"]),
        },
        "runtime_execution": _frozen_final_execution(),
        "runtime_authority": {
            **dict(release_binding),
            "execution": _frozen_final_execution(),
            "runtime_authority": True,
        },
        "exact_artifact": _artifact_projection(release),
        "resource_disabled_process": {
            "pid": int(disabled["pid"]),
            "pid_start_ticks": int(disabled["pid_start_ticks"]),
            "process_identity_sha256": _require_sha256(
                disabled.get("canonical_process_identity_sha256"),
                "disabled process identity",
            ),
            "config_sha256": _require_sha256(disabled.get("config_sha256"), "disabled config"),
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
            "shadow_runtime": dict(resource["shadow_runtime"]),
            "runtime_source_manifest_sha256": resource_source_manifest,
            "runtime_source_files": resource_source_files,
        },
        "transition": {
            "disabled_pid": int(disabled["pid"]),
            "disabled_pid_start_ticks": int(disabled["pid_start_ticks"]),
            "active_pid": int(process["pid"]),
            "active_pid_start_ticks": int(process["pid_start_ticks"]),
            "active_process_identity_sha256": _require_sha256(
                process.get("canonical_process_identity_sha256"),
                "active process identity",
            ),
            "fresh_disabled_to_active_restart": True,
        },
        "active_runtime": _runtime_projection(active, semantics),
    }


def _assert_portable(value: Any, *, location: str = "portable_evidence") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key) in _PORTABLE_FORBIDDEN_KEYS:
                raise CrossHostTransportError(f"{location} contains non-portable key: {key}")
            _assert_portable(nested, location=f"{location}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _assert_portable(nested, location=f"{location}[{index}]")


def _reference(binding: Mapping[str, Any], remote_path: str) -> dict[str, Any]:
    path = PurePosixPath(remote_path)
    if not path.is_absolute():
        raise CrossHostTransportError("remote project-reference path is not absolute provenance")
    return {**dict(binding), "remote_path_provenance": str(path)}


def _stable_process_projection(process: Mapping[str, Any]) -> dict[str, Any]:
    return {field: process.get(field) for field in _PROCESS_STABLE_FIELDS}


def _active_remote_release_path(active: Mapping[str, Any]) -> str:
    runtime = active.get("runtime_identity")
    if not isinstance(runtime, Mapping):
        raise CrossHostTransportError("active runtime identity is missing")
    value = str(runtime.get("f05_buy_e3_active_release_path", ""))
    if not PurePosixPath(value).is_absolute():
        raise CrossHostTransportError("active runtime release path provenance drifted")
    return value


def _validate_remote_content_at_path(value: Any, path: Path, label: str) -> None:
    """Reopen a v2 content-only binding at its separately frozen remote path."""
    if not isinstance(value, Mapping) or set(value) != set(CONTENT_BINDING_FIELDS):
        raise CrossHostTransportError(f"{label} remote content binding fields drifted")
    expected = _content_from_mapping(value, label)
    if not isinstance(expected["status"], str):
        raise CrossHostTransportError(f"{label} remote content binding is malformed")
    opened = _open_private_json(path, label)
    observed = _content_binding(
        opened,
        canonical_field=expected["canonical_field"],
        expected_schema=expected["schema_version"],
        expected_status=expected["status"],
    )
    if expected != observed:
        raise CrossHostTransportError(f"{label} remote content binding drifted")


def build_remote_active_attestation(
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    config_correction_path: Path,
    resource_receipt_path: Path,
    active_capture_path: Path,
    live_log_path: Path,
    proc_root: Path = Path("/proc"),
    dmi_root: Path = Path("/sys/devices/virtual/dmi/id"),
    generated_utc: str | None = None,
) -> dict[str, Any]:
    if (
        str(config_correction_path.expanduser().resolve(strict=True))
        != FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE
    ):
        raise CrossHostTransportError("config correction remote path provenance drifted")
    if (
        str(resource_receipt_path.expanduser().resolve(strict=True))
        != FROZEN_FINAL_RESOURCE_PATH_PROVENANCE
    ):
        raise CrossHostTransportError("resource receipt remote path provenance drifted")
    if (
        str(active_capture_path.expanduser().resolve(strict=True))
        != FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE
    ):
        raise CrossHostTransportError("active capture remote path provenance drifted")
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=direct_repository_root
    )
    _correction, correction_binding, _correction_raw = _validate_config_correction(
        config_correction_path
    )
    resource, resource_binding, _resource_raw = _validate_resource(
        resource_receipt_path,
        config_correction_path=config_correction_path,
    )
    if resource.get("config_correction") != correction_binding:
        raise CrossHostTransportError("resource/config correction cross-binding drifted")
    semantically_validated_active = _validate_active_against_remote_log(
        active_capture_path,
        live_log_path=live_log_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
        config_correction_path=config_correction_path,
    )
    active, active_binding, _active_raw, semantics = _validate_active(
        active_capture_path,
        release=release,
        release_binding=release_binding,
        resource=resource,
        resource_binding=resource_binding,
    )
    if semantically_validated_active != active:
        raise CrossHostTransportError("active capture changed after remote log validation")
    _validate_remote_content_at_path(
        active.get("resource_receipt"), resource_receipt_path, "resource receipt"
    )
    _validate_remote_content_at_path(
        active.get("config_correction"), config_correction_path, "config correction receipt"
    )
    _validate_remote_content_at_path(
        active.get("runtime_authority"), direct_release_path, "final runtime authority"
    )
    observed_host = resource_v8.host_identity(
        instance_id=CURRENT_INSTANCE_ID,
        instance_type=CURRENT_INSTANCE_TYPE,
        proc_root=proc_root,
        dmi_root=dmi_root,
    )
    if observed_host != resource.get("host"):
        raise CrossHostTransportError("attesting host is not the resource-gate host")
    process = active["active_process"]
    runtime = active["runtime_identity"]
    process_runtime = process.get("runtime_identity")
    if not isinstance(process_runtime, Mapping) or process_runtime.get("present") is not True:
        raise CrossHostTransportError("active process lacks runtime identity provenance")
    runtime_identity_path = Path(str(process_runtime.get("path", "")))
    try:
        recaptured = gate_v2.capture_actual_process_identity(
            pid=int(process["pid"]),
            expected_repository_root=direct_repository_root,
            expected_config_path=Path(str(runtime["config_path"])),
            expected_config_sha256=str(runtime["config_sha256"]),
            expected_python_executable=Path(str(process["python_executable"])),
            expected_venv_root=Path(str(process["venv_root"])),
            proc_root=proc_root,
            runtime_identity_path=runtime_identity_path,
        )
    except Exception as exc:
        raise CrossHostTransportError("active live process could not be recaptured") from exc
    if _stable_process_projection(recaptured) != _stable_process_projection(process):
        raise CrossHostTransportError("active live process changed after its capture")
    components = _portable_components(
        resource=resource,
        active=active,
        release=release,
        release_binding=release_binding,
        semantics=semantics,
    )
    release_remote_path = _active_remote_release_path(active)
    if release_remote_path != str(direct_release_path.expanduser().resolve(strict=True)):
        raise CrossHostTransportError("active runtime release path provenance drifted")
    timestamp = generated_utc or _now()
    active_process_utc = _utc_datetime(
        process.get("captured_utc"), "active process capture timestamp"
    )
    active_receipt_utc = _utc_datetime(
        active.get("generated_utc"), "active capture receipt timestamp"
    )
    recaptured_utc = _utc_datetime(
        recaptured.get("captured_utc"), "live process recapture timestamp"
    )
    attested_utc = _utc_datetime(timestamp, "remote attestation timestamp")
    if (
        max(active_process_utc, active_receipt_utc) > recaptured_utc
        or recaptured_utc > attested_utc
    ):
        raise CrossHostTransportError("remote live-process attestation chronology drifted")
    active_stable_sha = _canonical_sha256(_stable_process_projection(process))
    recaptured_stable_sha = _canonical_sha256(_stable_process_projection(recaptured))
    payload = {
        "schema_version": REMOTE_ATTESTATION_SCHEMA,
        "identity": OWNER,
        "status": REMOTE_ATTESTATION_STATUS,
        "generated_utc": timestamp,
        **components,
        "project_references": {
            "config_correction": _reference(
                correction_binding, FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE
            ),
            "current_host_resource_gate": _reference(
                resource_binding, FROZEN_FINAL_RESOURCE_PATH_PROVENANCE
            ),
            "active_process_capture": _reference(
                active_binding, FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE
            ),
            "direct_active_release": _reference(release_binding, release_remote_path),
        },
        "live_process_attestation": {
            "pid": int(process["pid"]),
            "pid_start_ticks": int(process["pid_start_ticks"]),
            "active_capture_process_identity_sha256": process["canonical_process_identity_sha256"],
            "recaptured_process_identity_sha256": recaptured["canonical_process_identity_sha256"],
            "active_capture_stable_identity_sha256": active_stable_sha,
            "recaptured_stable_identity_sha256": recaptured_stable_sha,
            "recaptured_utc": recaptured["captured_utc"],
            "cmdline_sha256": process["cmdline_sha256"],
            "runtime_identity_file_sha256": active["runtime_identity_file_sha256"],
            "alive_at_attestation": True,
            "stable_identity_equal": True,
        },
        "checks": {
            "config_correction_exact_and_resource_bound": True,
            "resource_exact_file_and_canonical": True,
            "active_capture_exact_file_and_canonical": True,
            "frozen_final_runtime_authority_exact": True,
            "current_instance_and_type_exact": True,
            "startup_runtime_artifact_release_exact": True,
            "active_pid_alive_and_stable": True,
            "active_health_window_revalidated_against_remote_log": True,
            "captured_live_not_retroactive": True,
            "project_references_content_only": True,
            "remote_path_provenance_not_authority": True,
        },
        "authority_design": dict(TRANSPORT_AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[REMOTE_ATTESTATION_CANONICAL_FIELD] = _document_sha256(
        payload, REMOTE_ATTESTATION_CANONICAL_FIELD
    )
    return payload


def validate_remote_active_attestation(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    config_correction_path: Path,
    resource_receipt_path: Path,
    active_capture_path: Path,
    live_log_path: Path | None = None,
) -> dict[str, Any]:
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=direct_repository_root
    )
    _correction, correction_binding, _correction_raw = _validate_config_correction(
        config_correction_path
    )
    resource, resource_binding, _resource_raw = _validate_resource(
        resource_receipt_path,
        config_correction_path=config_correction_path,
    )
    if resource.get("config_correction") != correction_binding:
        raise CrossHostTransportError("resource/config correction cross-binding drifted")
    semantically_validated_active = (
        _validate_active_against_remote_log(
            active_capture_path,
            live_log_path=live_log_path,
            direct_repository_root=direct_repository_root,
            direct_release_path=direct_release_path,
            resource_receipt_path=resource_receipt_path,
            config_correction_path=config_correction_path,
        )
        if live_log_path is not None
        else None
    )
    active, active_binding, _active_raw, semantics = _validate_active(
        active_capture_path,
        release=release,
        release_binding=release_binding,
        resource=resource,
        resource_binding=resource_binding,
    )
    if semantically_validated_active is not None and semantically_validated_active != active:
        raise CrossHostTransportError("active capture changed after remote log validation")
    opened = _open_private_json(path, "remote active attestation")
    binding = _content_binding(
        opened,
        canonical_field=REMOTE_ATTESTATION_CANONICAL_FIELD,
        expected_schema=REMOTE_ATTESTATION_SCHEMA,
        expected_status=REMOTE_ATTESTATION_STATUS,
    )
    del binding
    payload = dict(opened.payload)
    components = _portable_components(
        resource=resource,
        active=active,
        release=release,
        release_binding=release_binding,
        semantics=semantics,
    )
    references = payload.get("project_references")
    live = payload.get("live_process_attestation")
    if not isinstance(references, Mapping) or set(references) != set(REMOTE_REFERENCE_ROLES):
        raise CrossHostTransportError("remote project references drifted")
    expected_reference_content = {
        "config_correction": correction_binding,
        "current_host_resource_gate": resource_binding,
        "active_process_capture": active_binding,
        "direct_active_release": release_binding,
    }
    for role, expected in expected_reference_content.items():
        if set(references[role]) != {*CONTENT_BINDING_FIELDS, "remote_path_provenance"}:
            raise CrossHostTransportError(f"remote project reference fields drifted: {role}")
        content = {field: references[role][field] for field in CONTENT_BINDING_FIELDS}
        if _content_from_mapping(content, role) != expected:
            raise CrossHostTransportError(f"remote project reference content drifted: {role}")
        provenance = str(references[role].get("remote_path_provenance", ""))
        if not PurePosixPath(provenance).is_absolute():
            raise CrossHostTransportError(f"remote project provenance is not absolute: {role}")
    if (
        references["config_correction"].get("remote_path_provenance")
        != FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE
        or references["current_host_resource_gate"].get("remote_path_provenance")
        != FROZEN_FINAL_RESOURCE_PATH_PROVENANCE
        or references["active_process_capture"].get("remote_path_provenance")
        != FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE
        or references["direct_active_release"].get("remote_path_provenance")
        != _active_remote_release_path(active)
    ):
        raise CrossHostTransportError("frozen remote path provenance drifted")
    process = active["active_process"]
    if not isinstance(live, Mapping) or set(live) != _LIVE_PROCESS_ATTESTATION_FIELDS:
        raise CrossHostTransportError("live process attestation is missing")
    for name in (
        "active_capture_process_identity_sha256",
        "recaptured_process_identity_sha256",
        "active_capture_stable_identity_sha256",
        "recaptured_stable_identity_sha256",
        "cmdline_sha256",
        "runtime_identity_file_sha256",
    ):
        _require_sha256(live.get(name), f"live process {name}")
    active_process_utc = _utc_datetime(
        process.get("captured_utc"), "active process capture timestamp"
    )
    active_receipt_utc = _utc_datetime(
        active.get("generated_utc"), "active capture receipt timestamp"
    )
    recaptured_utc = _utc_datetime(live.get("recaptured_utc"), "live process recapture timestamp")
    attested_utc = _utc_datetime(payload.get("generated_utc"), "remote attestation timestamp")
    active_stable_sha = _canonical_sha256(_stable_process_projection(process))
    if (
        set(payload) != _REMOTE_ATTESTATION_FIELDS
        or payload.get("identity") != OWNER
        or any(payload.get(field) != value for field, value in components.items())
        or live.get("pid") != process.get("pid")
        or live.get("pid_start_ticks") != process.get("pid_start_ticks")
        or live.get("active_capture_process_identity_sha256")
        != process.get("canonical_process_identity_sha256")
        or live.get("active_capture_stable_identity_sha256") != active_stable_sha
        or live.get("recaptured_stable_identity_sha256") != active_stable_sha
        or live.get("cmdline_sha256") != process.get("cmdline_sha256")
        or live.get("runtime_identity_file_sha256") != active.get("runtime_identity_file_sha256")
        or live.get("alive_at_attestation") is not True
        or live.get("stable_identity_equal") is not True
        or max(active_process_utc, active_receipt_utc) > recaptured_utc
        or recaptured_utc > attested_utc
        or payload.get("checks")
        != {
            "config_correction_exact_and_resource_bound": True,
            "resource_exact_file_and_canonical": True,
            "active_capture_exact_file_and_canonical": True,
            "frozen_final_runtime_authority_exact": True,
            "current_instance_and_type_exact": True,
            "startup_runtime_artifact_release_exact": True,
            "active_pid_alive_and_stable": True,
            "active_health_window_revalidated_against_remote_log": True,
            "captured_live_not_retroactive": True,
            "project_references_content_only": True,
            "remote_path_provenance_not_authority": True,
        }
        or payload.get("authority_design") != TRANSPORT_AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise CrossHostTransportError("remote active attestation identity drifted")
    return payload


def finalize_remote_active_attestation(
    *, output_path: Path, **kwargs: Any
) -> tuple[dict[str, Any], str]:
    if output_path.name != REMOTE_ATTESTATION_FILENAME:
        raise CrossHostTransportError("remote attestation output filename is not allowlisted")
    payload = build_remote_active_attestation(**kwargs)
    try:
        file_sha = release_io._write_exclusive(output_path, payload)  # noqa: SLF001
    except Exception as exc:
        raise CrossHostTransportError("remote attestation create-only write failed") from exc
    observed = validate_remote_active_attestation(
        output_path,
        **{
            key: kwargs[key]
            for key in (
                "direct_repository_root",
                "direct_release_path",
                "config_correction_path",
                "resource_receipt_path",
                "active_capture_path",
                "live_log_path",
            )
        },
    )
    if observed != payload:
        raise CrossHostTransportError("remote attestation changed after write")
    return payload, file_sha


def _safe_directory(path: Path, label: str, *, exact_mode: int = 0o700) -> Path:
    candidate = path.expanduser().absolute()
    if ".." in candidate.parts:
        raise CrossHostTransportError(f"{label} contains a path-escape component")
    try:
        release_io._reject_symlink_components(candidate, label)  # noqa: SLF001
        metadata = candidate.lstat()
    except Exception as exc:
        raise CrossHostTransportError(f"{label} is not safely accessible") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != exact_mode
        or metadata.st_uid != os.geteuid()
    ):
        raise CrossHostTransportError(f"{label} is not an owned {exact_mode:04o} directory")
    return candidate


def _incoming_paths(root: Path) -> dict[str, Path]:
    admitted = _safe_directory(root, "incoming transport root")
    observed = {row.name for row in admitted.iterdir()}
    expected = set(SOURCE_FILENAMES.values())
    if observed != expected:
        raise CrossHostTransportError("incoming transport filenames are not exactly allowlisted")
    paths = {role: admitted / filename for role, filename in SOURCE_FILENAMES.items()}
    identities: set[tuple[int, int]] = set()
    for role, path in paths.items():
        opened = _open_private_json(path, f"incoming {role}")
        identity = (opened.metadata.st_dev, opened.metadata.st_ino)
        if identity in identities:
            raise CrossHostTransportError("incoming transport files reuse one file identity")
        identities.add(identity)
    return paths


def _validate_source_set(
    *,
    correction_path: Path,
    resource_path: Path,
    active_path: Path,
    attestation_path: Path,
    direct_repository_root: Path,
    direct_release_path: Path,
) -> _SourceSet:
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=direct_repository_root
    )
    correction, correction_binding, _correction_raw = _validate_config_correction(correction_path)
    resource, resource_binding, _resource_raw = _validate_resource(
        resource_path,
        config_correction_path=correction_path,
    )
    if resource.get("config_correction") != correction_binding:
        raise CrossHostTransportError("resource/config correction cross-binding drifted")
    active, active_binding, _active_raw, _semantics = _validate_active(
        active_path,
        release=release,
        release_binding=release_binding,
        resource=resource,
        resource_binding=resource_binding,
    )
    attestation = validate_remote_active_attestation(
        attestation_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
        config_correction_path=correction_path,
        resource_receipt_path=resource_path,
        active_capture_path=active_path,
    )
    attestation_opened = _open_private_json(attestation_path, "remote active attestation")
    attestation_binding = _content_binding(
        attestation_opened,
        canonical_field=REMOTE_ATTESTATION_CANONICAL_FIELD,
        expected_schema=REMOTE_ATTESTATION_SCHEMA,
        expected_status=REMOTE_ATTESTATION_STATUS,
    )
    return _SourceSet(
        correction=correction,
        resource=resource,
        active=active,
        attestation=attestation,
        release=release,
        bindings={
            "config_correction": correction_binding,
            "current_host_resource_gate": resource_binding,
            "active_process_capture": active_binding,
            "remote_active_attestation": attestation_binding,
            "direct_active_release": release_binding,
        },
    )


def _open_directory_descriptor(path: Path) -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise CrossHostTransportError("create-only admission requires O_NOFOLLOW")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0) | nofollow
    try:
        return os.open(path, flags)
    except OSError as exc:
        raise CrossHostTransportError("admission root could not be opened safely") from exc


def _create_private_root(path: Path) -> Path:
    target = path.expanduser().absolute()
    if ".." in target.parts:
        raise CrossHostTransportError("admission root contains a path-escape component")
    parent = _safe_directory(target.parent, "admission parent")
    if target.parent != parent or target.name in {"", ".", ".."}:
        raise CrossHostTransportError("admission root escapes its parent")
    try:
        os.mkdir(target, 0o700)
    except FileExistsError as exc:
        raise CrossHostTransportError("immutable admission root already exists") from exc
    except OSError as exc:
        raise CrossHostTransportError("admission root creation failed") from exc
    os.chmod(target, 0o700)
    return _safe_directory(target, "admission root")


def _copy_exclusive(directory_fd: int, filename: str, raw: bytes) -> None:
    if filename != Path(filename).name or filename not in SOURCE_FILENAMES.values():
        raise CrossHostTransportError("copy target filename is not allowlisted")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(filename, flags, 0o600, dir_fd=directory_fd)
    except OSError as exc:
        raise CrossHostTransportError(f"create-only evidence copy failed: {filename}") from exc
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise CrossHostTransportError("evidence copy write did not make progress")
            offset += written
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
            or metadata.st_size != len(raw)
        ):
            raise CrossHostTransportError("written evidence copy identity drifted")
    finally:
        os.close(descriptor)


def _local_binding(path: Path) -> dict[str, Any]:
    opened = _open_private_json(path, f"admitted file {path.name}")
    return {
        "local_filename": path.name,
        "path": str(opened.path),
        "file_sha256": hashlib.sha256(opened.raw).hexdigest(),
        "size_bytes": len(opened.raw),
        "mode": "0600",
        "device": opened.metadata.st_dev,
        "inode": opened.metadata.st_ino,
        "nlink": opened.metadata.st_nlink,
    }


def _portable_from_sources(sources: _SourceSet) -> dict[str, Any]:
    if sources.attestation is None:
        raise CrossHostTransportError("remote attestation is missing")
    attestation = sources.attestation
    portable = {
        "host": dict(attestation["host"]),
        "runtime_execution": dict(attestation["runtime_execution"]),
        "runtime_authority": dict(attestation["runtime_authority"]),
        "exact_artifact": dict(attestation["exact_artifact"]),
        "resource_disabled_process": dict(attestation["resource_disabled_process"]),
        "transition": dict(attestation["transition"]),
        "active_runtime": dict(attestation["active_runtime"]),
        "source_receipts": {
            role: {
                **dict(sources.bindings[role]),
                "local_filename": SOURCE_FILENAMES[role],
            }
            for role in SOURCE_FILENAMES
        },
    }
    if set(portable) != set(PORTABLE_EVIDENCE_FIELDS):
        raise CrossHostTransportError("portable evidence fields drifted")
    _assert_portable(portable)
    return portable


def _transfer_manifest(admitted_files: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    content_manifest = {
        role: {
            "local_filename": binding["local_filename"],
            "file_sha256": binding["file_sha256"],
            "size_bytes": binding["size_bytes"],
            "mode": binding["mode"],
        }
        for role, binding in admitted_files.items()
    }
    return {
        "allowlisted_filenames": dict(SOURCE_FILENAMES),
        "same_directory": True,
        "create_only_copies": True,
        "regular_files_only": True,
        "mode_0600": True,
        "single_link_only": True,
        "o_nofollow_required": True,
        "duplicate_file_identity_rejected": True,
        "path_escape_rejected": True,
        "content_manifest_sha256": _canonical_sha256(content_manifest),
    }


def _admission_payload(
    *,
    sources: _SourceSet,
    admitted_files: Mapping[str, Mapping[str, Any]],
    admitted_utc: str,
) -> dict[str, Any]:
    _timestamp(admitted_utc, "admission timestamp")
    portable = _portable_from_sources(sources)
    payload = {
        "schema_version": ADMISSION_SCHEMA,
        "identity": OWNER,
        "status": ADMISSION_STATUS,
        "admitted_utc": admitted_utc,
        "portable_evidence": portable,
        "admitted_files": {role: dict(row) for role, row in admitted_files.items()},
        "transfer_manifest": _transfer_manifest(admitted_files),
        "checks": {
            "exact_four_source_files": True,
            "same_directory_allowlist": True,
            "create_only_regular_0600_single_link_copies": True,
            "o_nofollow_and_path_escape_guards": True,
            "duplicate_sources_rejected": True,
            "current_host_resource_receipt_semantically_valid": True,
            "remote_attestation_v4_semantically_valid": True,
            "active_capture_v7_content_and_health_projection_exact": True,
            "remote_log_not_required_for_local_admission": True,
            "frozen_final_runtime_authority_exact_and_unchanged": True,
            "portable_projection_has_no_path_or_inode_authority": True,
        },
        "authority_design": dict(TRANSPORT_AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[ADMISSION_CANONICAL_FIELD] = _document_sha256(payload, ADMISSION_CANONICAL_FIELD)
    return payload


def finalize_cross_host_admission(
    *,
    incoming_root: Path,
    admission_root: Path,
    direct_repository_root: Path,
    direct_release_path: Path,
    admitted_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    incoming = _incoming_paths(incoming_root)
    sources = _validate_source_set(
        correction_path=incoming["config_correction"],
        resource_path=incoming["current_host_resource_gate"],
        active_path=incoming["active_process_capture"],
        attestation_path=incoming["remote_active_attestation"],
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
    )
    raw_by_role = {
        role: _open_private_json(incoming[role], f"incoming {role}").raw
        for role in SOURCE_FILENAMES
    }
    target = _create_private_root(admission_root)
    descriptor = _open_directory_descriptor(target)
    try:
        for role, filename in SOURCE_FILENAMES.items():
            _copy_exclusive(descriptor, filename, raw_by_role[role])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    copied = {role: target / filename for role, filename in SOURCE_FILENAMES.items()}
    copied_sources = _validate_source_set(
        correction_path=copied["config_correction"],
        resource_path=copied["current_host_resource_gate"],
        active_path=copied["active_process_capture"],
        attestation_path=copied["remote_active_attestation"],
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
    )
    if copied_sources.bindings != sources.bindings:
        raise CrossHostTransportError("admitted evidence copy content drifted")
    admitted_files = {role: _local_binding(path) for role, path in copied.items()}
    payload = _admission_payload(
        sources=copied_sources,
        admitted_files=admitted_files,
        admitted_utc=admitted_utc or _now(),
    )
    output = target / ADMISSION_FILENAME
    try:
        file_sha = release_io._write_exclusive(output, payload)  # noqa: SLF001
    except Exception as exc:
        raise CrossHostTransportError("cross-host admission create-only write failed") from exc
    observed = validate_cross_host_admission(
        output,
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
    )
    if observed != payload:
        raise CrossHostTransportError("cross-host admission changed after write")
    return payload, file_sha


def validate_cross_host_admission(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
) -> dict[str, Any]:
    if path.name != ADMISSION_FILENAME:
        raise CrossHostTransportError("cross-host admission filename is not allowlisted")
    root = _safe_directory(path.parent, "cross-host admission root")
    observed_names = {row.name for row in root.iterdir()}
    if observed_names != {*SOURCE_FILENAMES.values(), ADMISSION_FILENAME}:
        raise CrossHostTransportError("cross-host admission directory contains unallowlisted files")
    copied = {role: root / filename for role, filename in SOURCE_FILENAMES.items()}
    sources = _validate_source_set(
        correction_path=copied["config_correction"],
        resource_path=copied["current_host_resource_gate"],
        active_path=copied["active_process_capture"],
        attestation_path=copied["remote_active_attestation"],
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
    )
    opened = _open_private_json(path, "cross-host admission")
    payload = dict(opened.payload)
    canonical = _require_sha256(payload.get(ADMISSION_CANONICAL_FIELD), "admission canonical")
    admitted_files = {role: _local_binding(file_path) for role, file_path in copied.items()}
    expected = _admission_payload(
        sources=sources,
        admitted_files=admitted_files,
        admitted_utc=_timestamp(payload.get("admitted_utc"), "admission timestamp"),
    )
    if (
        set(payload) != _ADMISSION_FIELDS
        or canonical != _document_sha256(payload, ADMISSION_CANONICAL_FIELD)
        or payload != expected
    ):
        raise CrossHostTransportError("cross-host admission identity drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    remote = commands.add_parser("remote-attest")
    remote.add_argument("--direct-repository-root", type=Path, required=True)
    remote.add_argument("--direct-release", dest="direct_release_path", type=Path, required=True)
    remote.add_argument(
        "--config-correction", dest="config_correction_path", type=Path, required=True
    )
    remote.add_argument(
        "--resource-receipt", dest="resource_receipt_path", type=Path, required=True
    )
    remote.add_argument("--active-capture", dest="active_capture_path", type=Path, required=True)
    remote.add_argument("--live-log", dest="live_log_path", type=Path, required=True)
    remote.add_argument("--output", type=Path, required=True)
    admit = commands.add_parser("admit")
    admit.add_argument("--incoming-root", type=Path, required=True)
    admit.add_argument("--admission-root", type=Path, required=True)
    admit.add_argument("--direct-repository-root", type=Path, required=True)
    admit.add_argument("--direct-release", dest="direct_release_path", type=Path, required=True)
    validate = commands.add_parser("validate-admission")
    validate.add_argument("--receipt", type=Path, required=True)
    validate.add_argument("--direct-repository-root", type=Path, required=True)
    validate.add_argument("--direct-release", dest="direct_release_path", type=Path, required=True)
    return parser


def _print_result(payload: Mapping[str, Any], file_sha: str | None = None) -> None:
    canonical_field = (
        REMOTE_ATTESTATION_CANONICAL_FIELD
        if payload.get("schema_version") == REMOTE_ATTESTATION_SCHEMA
        else ADMISSION_CANONICAL_FIELD
    )
    result = {
        "schema_version": payload["schema_version"],
        "status": payload["status"],
        "canonical_sha256": payload[canonical_field],
    }
    if file_sha is not None:
        result["file_sha256"] = file_sha
    print(json.dumps(result, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "remote-attest":
        payload, file_sha = finalize_remote_active_attestation(
            direct_repository_root=args.direct_repository_root,
            direct_release_path=args.direct_release_path,
            config_correction_path=args.config_correction_path,
            resource_receipt_path=args.resource_receipt_path,
            active_capture_path=args.active_capture_path,
            live_log_path=args.live_log_path,
            output_path=args.output,
        )
    elif args.command == "admit":
        payload, file_sha = finalize_cross_host_admission(
            incoming_root=args.incoming_root,
            admission_root=args.admission_root,
            direct_repository_root=args.direct_repository_root,
            direct_release_path=args.direct_release_path,
        )
    else:
        payload = validate_cross_host_admission(
            args.receipt,
            direct_repository_root=args.direct_repository_root,
            direct_release_path=args.direct_release_path,
        )
        file_sha = None
    _print_result(payload, file_sha)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
