#!/usr/bin/env python3
"""Compose admitted cross-host BUY E3 evidence without changing live authority.

The EC2 evidence is first admitted by ``f05_buy_e3_cross_host_transport``.
This module consumes only that admission's content projection.  It deliberately
does not attempt to revalidate remote inode or absolute-path metadata on macOS.
All receipts are additive and create-only; the immutable direct-v3 owner release
remains the sole runtime authority.
"""

from __future__ import annotations

import argparse
import importlib
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from scripts import f05_buy_e3_evidence_completion as base

OWNER: Final = base.OWNER

ENVELOPE_SCHEMA: Final = f"{OWNER}.cross_host_activation_envelope.v2"
ENVELOPE_STATUS: Final = "cross_host_active_evidence_admitted"
ENVELOPE_CANONICAL_FIELD: Final = "canonical_cross_host_activation_envelope_sha256"

COMPLETION_SCHEMA: Final = f"{OWNER}.cross_host_operational_evidence_completion.v2"
COMPLETION_STATUS: Final = "cross_host_operational_evidence_complete_authority_unchanged"
COMPLETION_CANONICAL_FIELD: Final = "canonical_cross_host_operational_completion_sha256"

COMPOSITION_SCHEMA: Final = f"{OWNER}.cross_host_final_composition_receipt.v1"
COMPOSITION_STATUS: Final = "cross_host_operational_evidence_composed"
COMPOSITION_CANONICAL_FIELD: Final = "canonical_cross_host_final_composition_sha256"

ATTEMPT_FINAL_SCHEMA: Final = f"{OWNER}.cross_host_operational_attempt_final_receipt.v1"
ATTEMPT_FINAL_STATUS: Final = "cross_host_operational_attempt_results_bound_authority_unchanged"
ATTEMPT_FINAL_CANONICAL_FIELD: Final = "canonical_cross_host_attempt_final_sha256"

EVIDENCE_RELEASE_SCHEMA: Final = f"{OWNER}.cross_host_evidence_complete_active_release.v1"
EVIDENCE_RELEASE_STATUS: Final = (
    "owner_authorized_live_cross_host_evidence_complete_runtime_authority_unchanged"
)
EVIDENCE_RELEASE_CANONICAL_FIELD: Final = "canonical_cross_host_evidence_release_sha256"

EXPECTED_HOST: Final = {
    "provider": "aws",
    "region": "ap-northeast-1",
    "instance_id": "i-00fe03a8b2fb49a31",
    "instance_type": "c7i-flex.large",
    "public_ipv4": "13.158.101.253",
}

PORTABLE_ROLES: Final = {
    "current_host_resource_gate",
    "active_process_capture",
    "remote_active_attestation",
}
CONTENT_BINDING_FIELDS: Final = {
    "schema_version",
    "status",
    "file_sha256",
    "canonical_field",
    "canonical_sha256",
    "size_bytes",
    "mode",
    "local_filename",
}

GATE_RESULTS: Final = {
    "attempt4_wrappers_and_manifest": "passed_historical_mechanics_anchor",
    "attempt4_resource_or_activation": "not_claimed",
    "exact_v5_mechanics_recovery": "passed",
    "current_full_runtime_regression": "passed",
    "direct_v3_focused_successor_regression": "passed",
    "sparse_streaming_offline_parity": "passed",
    "sell_54_case_unchanged": "passed",
    "fresh_disabled_same_pid_resource_gate": "passed",
    "fresh_disabled_to_active_restart": "passed",
    "active_startup_attestation": "passed",
    "cross_host_content_admission": "passed",
    "lifecycle_orico_admission": "passed",
}
FORMAL_RESEARCH_STATE: Final = {
    "research_supported": False,
    "formal_hierarchy_passed": False,
    "formal_hard_gates_passed": False,
    "owner_risk_accepted": True,
    "old_oof_applies_to_learning_algorithm_only": True,
    "exact_artifact_oof_available": False,
}


class CrossHostCompletionError(RuntimeError):
    """Raised when a cross-host evidence binding is not exact."""


def _transport_module() -> Any:
    try:
        return importlib.import_module("scripts.f05_buy_e3_cross_host_transport")
    except ImportError as exc:  # pragma: no cover - covered once transport lands
        raise CrossHostCompletionError("cross-host transport successor is unavailable") from exc


def _timestamp(value: Any, label: str) -> None:
    try:
        base._timestamp(value, label)  # noqa: SLF001
    except Exception as exc:
        raise CrossHostCompletionError(f"{label} is invalid") from exc


def _receipt_binding(
    path: Path, *, label: str, canonical_field: str, schema: str, status: str
) -> dict[str, Any]:
    try:
        return base._receipt_binding(  # noqa: SLF001
            path,
            label=label,
            canonical_field=canonical_field,
            schema=schema,
            status=status,
        )
    except Exception as exc:
        raise CrossHostCompletionError(f"{label} binding is invalid") from exc


def _finalize(
    output_path: Path,
    payload: dict[str, Any],
    *,
    validator: Any,
    validator_kwargs: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    try:
        file_sha = base._write(output_path, payload)  # noqa: SLF001
        observed = validator(output_path, **dict(validator_kwargs))
    except Exception as exc:
        raise CrossHostCompletionError(f"receipt creation failed: {output_path}") from exc
    if observed != payload:
        raise CrossHostCompletionError("written receipt differs after validation")
    return payload, file_sha


def _attempt_context(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        payload = base.validate_operational_attempt(
            path,
            collector_repository_root=collector_repository_root,
            direct_repository_root=direct_repository_root,
            attempt4_repository_root=attempt4_repository_root,
        )
    except Exception as exc:
        raise CrossHostCompletionError("operational attempt is invalid") from exc
    binding = _receipt_binding(
        path,
        label="operational evidence attempt",
        canonical_field="canonical_operational_attempt_sha256",
        schema=base.OPERATIONAL_ATTEMPT_SCHEMA,
        status=base.OPERATIONAL_ATTEMPT_STATUS,
    )
    return payload, binding


def _content_binding(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != CONTENT_BINDING_FIELDS:
        raise CrossHostCompletionError(f"{label} content binding fields drifted")
    row = dict(value)
    for key in ("file_sha256", "canonical_sha256"):
        try:
            base._require_sha256(row.get(key), f"{label} {key}")  # noqa: SLF001
        except Exception as exc:
            raise CrossHostCompletionError(f"{label} {key} is invalid") from exc
    if (
        not isinstance(row.get("schema_version"), str)
        or not row["schema_version"]
        or not isinstance(row.get("status"), str)
        or not row["status"]
        or not isinstance(row.get("canonical_field"), str)
        or not row["canonical_field"].startswith("canonical_")
        or not isinstance(row.get("local_filename"), str)
        or Path(row["local_filename"]).name != row["local_filename"]
        or row.get("mode") != "0600"
        or not isinstance(row.get("size_bytes"), int)
        or isinstance(row.get("size_bytes"), bool)
        or row["size_bytes"] <= 0
    ):
        raise CrossHostCompletionError(f"{label} content binding is malformed")
    return row


def _portable_authority_projection(attempt: Mapping[str, Any]) -> dict[str, Any]:
    authority = attempt.get("runtime_authority")
    if not isinstance(authority, Mapping):
        raise CrossHostCompletionError("operational attempt lacks runtime authority")
    return {
        "schema_version": authority.get("schema_version"),
        "status": authority.get("status"),
        "file_sha256": authority.get("file_sha256"),
        "canonical_field": authority.get("canonical_field"),
        "canonical_sha256": authority.get("canonical_sha256"),
        "execution": base._direct_execution(),  # noqa: SLF001
        "runtime_authority": True,
    }


def _portable_artifact_projection(attempt: Mapping[str, Any]) -> dict[str, Any]:
    artifact = attempt.get("exact_artifact")
    roles = artifact.get("roles") if isinstance(artifact, Mapping) else None
    if (
        not isinstance(artifact, Mapping)
        or artifact.get("artifact_sha256") != base.ARTIFACT_SHA256
        or not isinstance(roles, Mapping)
        or set(roles) != {"manifest", "policy", "predicate_bundle"}
    ):
        raise CrossHostCompletionError("operational attempt exact artifact is malformed")
    projected: dict[str, Any] = {}
    for role in ("manifest", "policy", "predicate_bundle"):
        row = roles[role]
        if not isinstance(row, Mapping):
            raise CrossHostCompletionError(f"operational artifact role is malformed: {role}")
        projected[role] = {
            field: row.get(field)
            for field in (
                "schema_version",
                "status",
                "file_sha256",
                "canonical_field",
                "canonical_sha256",
                "size_bytes",
                "mode",
            )
        }
        _content_binding(
            {**projected[role], "local_filename": f"{role}.json"},
            f"operational artifact {role}",
        )
    return {"artifact_sha256": base.ARTIFACT_SHA256, "roles": projected}


def _validate_portable_evidence(
    value: Any, *, attempt: Mapping[str, Any]
) -> dict[str, Any]:
    fields = {
        "host",
        "runtime_execution",
        "runtime_authority",
        "exact_artifact",
        "resource_disabled_process",
        "transition",
        "active_runtime",
        "source_receipts",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise CrossHostCompletionError("portable evidence fields drifted")
    portable = dict(value)
    host = portable["host"]
    if not isinstance(host, Mapping) or any(host.get(k) != v for k, v in EXPECTED_HOST.items()):
        raise CrossHostCompletionError("portable host identity drifted")
    if portable["runtime_execution"] != base._direct_execution():  # noqa: SLF001
        raise CrossHostCompletionError("portable runtime execution drifted")
    if portable["runtime_authority"] != _portable_authority_projection(attempt):
        raise CrossHostCompletionError("portable runtime authority drifted")
    if portable["exact_artifact"] != _portable_artifact_projection(attempt):
        raise CrossHostCompletionError("portable exact artifact drifted")

    receipts = portable["source_receipts"]
    if not isinstance(receipts, Mapping) or set(receipts) != PORTABLE_ROLES:
        raise CrossHostCompletionError("portable source receipt roles drifted")
    normalized_receipts = {
        role: _content_binding(receipts[role], f"portable {role}")
        for role in sorted(PORTABLE_ROLES)
    }
    resource = normalized_receipts["current_host_resource_gate"]
    active_capture = normalized_receipts["active_process_capture"]
    attestation = normalized_receipts["remote_active_attestation"]
    transport = _transport_module()
    if (
        resource["schema_version"] != base.RESOURCE_SCHEMA
        or resource["status"] != base.RESOURCE_STATUS
        or resource["canonical_field"] != base.RESOURCE_CANONICAL_FIELD
        or active_capture["schema_version"] != base.ACTIVE_CAPTURE_SCHEMA
        or active_capture["status"] != base.ACTIVE_CAPTURE_STATUS
        or active_capture["canonical_field"] != "canonical_active_capture_sha256"
        or attestation["schema_version"] != transport.REMOTE_ATTESTATION_SCHEMA
        or attestation["status"] != transport.REMOTE_ATTESTATION_STATUS
        or attestation["canonical_field"] != transport.REMOTE_ATTESTATION_CANONICAL_FIELD
    ):
        raise CrossHostCompletionError("portable source receipt identity drifted")

    disabled = portable["resource_disabled_process"]
    transition = portable["transition"]
    active = portable["active_runtime"]
    if not isinstance(disabled, Mapping) or not isinstance(transition, Mapping):
        raise CrossHostCompletionError("portable process transition is missing")
    disabled_pid = disabled.get("pid")
    disabled_start = disabled.get("pid_start_ticks")
    active_pid = transition.get("active_pid")
    active_start = transition.get("active_pid_start_ticks")
    if (
        not isinstance(disabled_pid, int)
        or isinstance(disabled_pid, bool)
        or disabled_pid <= 0
        or not isinstance(disabled_start, int)
        or isinstance(disabled_start, bool)
        or disabled_start <= 0
        or transition.get("disabled_pid") != disabled_pid
        or transition.get("disabled_pid_start_ticks") != disabled_start
        or not isinstance(active_pid, int)
        or isinstance(active_pid, bool)
        or active_pid <= 0
        or active_pid == disabled_pid
        or not isinstance(active_start, int)
        or isinstance(active_start, bool)
        or active_start <= disabled_start
        or transition.get("fresh_disabled_to_active_restart") is not True
    ):
        raise CrossHostCompletionError("portable disabled-to-active transition drifted")
    if not isinstance(active, Mapping):
        raise CrossHostCompletionError("portable active runtime is missing")
    source_files = active.get("runtime_source_files")
    mandatory = {"strategy/maker_engine.py", "strategy/boolean_cooldown_buy_e3.py"}
    runtime_identity = active.get("runtime_identity")
    startup_attestation = active.get("startup_attestation")
    startup_semantics = active.get("startup_semantics")
    if (
        active.get("artifact_sha256") != base.ARTIFACT_SHA256
        or active.get("buy_e3_enabled") is not True
        or active.get("owner_override_effective") is not True
        or not isinstance(runtime_identity, Mapping)
        or re.fullmatch(r"[0-9a-f]{64}", str(runtime_identity.get("file_sha256"))) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(runtime_identity.get("canonical_sha256"))) is None
        or not isinstance(startup_attestation, Mapping)
        or re.fullmatch(
            r"[0-9a-f]{64}", str(startup_attestation.get("canonical_sha256"))
        )
        is None
        or not isinstance(startup_semantics, Mapping)
        or startup_semantics.get("startup_status") != "accepted"
        or startup_semantics.get("running_checkout_commit") != base.DIRECT_COMMIT
        or startup_semantics.get("running_checkout_tree") != base.DIRECT_TREE
        or not isinstance(active.get("config_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", active["config_sha256"]) is None
        or not isinstance(active.get("runtime_source_manifest_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", active["runtime_source_manifest_sha256"]) is None
        or not isinstance(source_files, Mapping)
        or not mandatory.issubset(source_files)
        or any(re.fullmatch(r"[0-9a-f]{64}", str(v)) is None for v in source_files.values())
    ):
        raise CrossHostCompletionError("portable active runtime drifted")
    portable["source_receipts"] = normalized_receipts
    return portable


def _admission_context(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    attempt: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    transport = _transport_module()
    try:
        payload = transport.validate_cross_host_admission(
            path,
            direct_repository_root=direct_repository_root,
            direct_release_path=direct_release_path,
        )
    except Exception as exc:
        raise CrossHostCompletionError("cross-host admission is invalid") from exc
    binding = _receipt_binding(
        path,
        label="cross-host evidence admission",
        canonical_field=transport.ADMISSION_CANONICAL_FIELD,
        schema=transport.ADMISSION_SCHEMA,
        status=transport.ADMISSION_STATUS,
    )
    portable = _validate_portable_evidence(payload.get("portable_evidence"), attempt=attempt)
    return payload, binding, portable


def _envelope_context(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = validate_activation_envelope(
        path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    return payload, _receipt_binding(
        path,
        label="cross-host activation envelope",
        canonical_field=ENVELOPE_CANONICAL_FIELD,
        schema=ENVELOPE_SCHEMA,
        status=ENVELOPE_STATUS,
    )


def build_activation_envelope(
    *,
    operational_attempt_path: Path,
    cross_host_admission_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt, attempt_binding = _attempt_context(
        operational_attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    release_path = Path(str(attempt["runtime_authority"]["path"]))
    _admission, admission_binding, portable = _admission_context(
        cross_host_admission_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=release_path,
        attempt=attempt,
    )
    timestamp = generated_utc or base._now()  # noqa: SLF001
    _timestamp(timestamp, "cross-host activation envelope timestamp")
    payload = {
        "schema_version": ENVELOPE_SCHEMA,
        "identity": OWNER,
        "status": ENVELOPE_STATUS,
        "generated_utc": timestamp,
        "operational_attempt": attempt_binding,
        "cross_host_admission": admission_binding,
        "runtime_authority": dict(attempt["runtime_authority"]),
        "portable_runtime_authority": dict(portable["runtime_authority"]),
        "host": dict(portable["host"]),
        "source_receipts": dict(portable["source_receipts"]),
        "transition": dict(portable["transition"]),
        "active_runtime": dict(portable["active_runtime"]),
        "exact_artifact": dict(attempt["exact_artifact"]),
        "checks": {
            "remote_process_attested_while_live": True,
            "captured_live_not_retroactive": True,
            "cross_host_content_admitted": True,
            "remote_inode_reinterpreted_locally": False,
            "resource_gate_preceded_activation": True,
            "fresh_active_restart": True,
            "direct_v3_runtime_authority_unchanged": True,
        },
        "authority_design": dict(base.AUTHORITY_DESIGN),
        "permissions": dict(base.NO_AUTHORITY),
        "evidence_boundary": dict(base.EVIDENCE_BOUNDARY),
    }
    payload[ENVELOPE_CANONICAL_FIELD] = base._document_sha256(  # noqa: SLF001
        payload, ENVELOPE_CANONICAL_FIELD
    )
    return payload


def validate_activation_envelope(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    try:
        payload, _ = base._binding(  # noqa: SLF001
            path,
            label="cross-host activation envelope",
            canonical_field=ENVELOPE_CANONICAL_FIELD,
            expected_schema=ENVELOPE_SCHEMA,
            expected_status=ENVELOPE_STATUS,
        )
    except Exception as exc:
        raise CrossHostCompletionError("cross-host activation envelope is invalid") from exc
    attempt_path = Path(str(payload.get("operational_attempt", {}).get("path", "")))
    admission_path = Path(str(payload.get("cross_host_admission", {}).get("path", "")))
    expected = build_activation_envelope(
        operational_attempt_path=attempt_path,
        cross_host_admission_path=admission_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise CrossHostCompletionError("cross-host activation envelope identity drifted")
    return payload


def finalize_activation_envelope(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_activation_envelope(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_activation_envelope,
        validator_kwargs={
            "collector_repository_root": kwargs["collector_repository_root"],
            "direct_repository_root": kwargs["direct_repository_root"],
            "attempt4_repository_root": kwargs["attempt4_repository_root"],
        },
    )


def build_completion(
    *,
    operational_attempt_path: Path,
    activation_envelope_path: Path,
    lifecycle_admission_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt, attempt_binding = _attempt_context(
        operational_attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    envelope, envelope_binding = _envelope_context(
        activation_envelope_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    if envelope["operational_attempt"]["canonical_sha256"] != attempt_binding["canonical_sha256"]:
        raise CrossHostCompletionError("activation envelope belongs to another attempt")
    try:
        lifecycle, lifecycle_binding = base._validate_lifecycle_admission(  # noqa: SLF001
            lifecycle_admission_path
        )
        base._require_lifecycle_active_match(envelope, lifecycle_binding)  # noqa: SLF001
    except Exception as exc:
        raise CrossHostCompletionError("lifecycle admission does not match active runtime") from exc
    timestamp = generated_utc or base._now()  # noqa: SLF001
    _timestamp(timestamp, "cross-host completion timestamp")
    payload = {
        "schema_version": COMPLETION_SCHEMA,
        "identity": OWNER,
        "attempt_id": attempt["attempt_id"],
        "status": COMPLETION_STATUS,
        "generated_utc": timestamp,
        "operational_attempt": attempt_binding,
        "activation_envelope": envelope_binding,
        "cross_host_admission": dict(envelope["cross_host_admission"]),
        "runtime_authority": dict(attempt["runtime_authority"]),
        "exact_artifact": dict(attempt["exact_artifact"]),
        "historical_attempt4_anchor": dict(attempt["historical_attempt4_anchor"]),
        "exact_v5_recovery": dict(attempt["exact_v5_recovery"]),
        "current_runtime_evidence": dict(attempt["current_runtime_evidence"]),
        "current_host_resource": dict(envelope["source_receipts"]["current_host_resource_gate"]),
        "fresh_activation": envelope_binding,
        "lifecycle_orico_admission": lifecycle_binding,
        "gate_results": dict(GATE_RESULTS),
        "formal_research_state": dict(FORMAL_RESEARCH_STATE),
        "direct_release_immutable_incomplete_record_preserved": True,
        "post_release_evidence_completed": True,
        "cross_host_boundary_preserved": True,
        "authority_design": dict(base.AUTHORITY_DESIGN),
        "permissions": dict(base.NO_AUTHORITY),
        "evidence_boundary": dict(base.EVIDENCE_BOUNDARY),
        "lifecycle_admission_observed_payload_sha256": base._canonical_sha256(lifecycle),  # noqa: SLF001
    }
    payload[COMPLETION_CANONICAL_FIELD] = base._document_sha256(  # noqa: SLF001
        payload, COMPLETION_CANONICAL_FIELD
    )
    return payload


def validate_completion(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    try:
        payload, _ = base._binding(  # noqa: SLF001
            path,
            label="cross-host operational completion",
            canonical_field=COMPLETION_CANONICAL_FIELD,
            expected_schema=COMPLETION_SCHEMA,
            expected_status=COMPLETION_STATUS,
        )
    except Exception as exc:
        raise CrossHostCompletionError("cross-host operational completion is invalid") from exc
    expected = build_completion(
        operational_attempt_path=Path(str(payload.get("operational_attempt", {}).get("path", ""))),
        activation_envelope_path=Path(str(payload.get("activation_envelope", {}).get("path", ""))),
        lifecycle_admission_path=Path(str(payload.get("lifecycle_orico_admission", {}).get("path", ""))),
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise CrossHostCompletionError("cross-host operational completion identity drifted")
    return payload


def finalize_completion(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_completion(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_completion,
        validator_kwargs={
            "collector_repository_root": kwargs["collector_repository_root"],
            "direct_repository_root": kwargs["direct_repository_root"],
            "attempt4_repository_root": kwargs["attempt4_repository_root"],
        },
    )


def _completion_context(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = validate_completion(
        path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    return payload, _receipt_binding(
        path,
        label="cross-host operational completion",
        canonical_field=COMPLETION_CANONICAL_FIELD,
        schema=COMPLETION_SCHEMA,
        status=COMPLETION_STATUS,
    )


def build_composition(
    *,
    operational_attempt_path: Path,
    activation_envelope_path: Path,
    operational_completion_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt, attempt_binding = _attempt_context(
        operational_attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    envelope, envelope_binding = _envelope_context(
        activation_envelope_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    completion, completion_binding = _completion_context(
        operational_completion_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    if (
        completion["attempt_id"] != attempt["attempt_id"]
        or completion["activation_envelope"]["canonical_sha256"]
        != envelope_binding["canonical_sha256"]
    ):
        raise CrossHostCompletionError("cross-host evidence is not one operational attempt")
    evidence = {
        "operational_attempt": attempt_binding,
        "historical_attempt4_anchor": dict(completion["historical_attempt4_anchor"]),
        "exact_v5_recovery": dict(completion["exact_v5_recovery"]),
        "current_runtime_evidence": dict(completion["current_runtime_evidence"]),
        "cross_host_admission": dict(envelope["cross_host_admission"]),
        "current_host_resource": dict(completion["current_host_resource"]),
        "fresh_activation": envelope_binding,
        "lifecycle_orico_admission": dict(completion["lifecycle_orico_admission"]),
        "operational_completion": completion_binding,
    }
    timestamp = generated_utc or base._now()  # noqa: SLF001
    _timestamp(timestamp, "cross-host composition timestamp")
    payload = {
        "schema_version": COMPOSITION_SCHEMA,
        "identity": OWNER,
        "attempt_id": attempt["attempt_id"],
        "status": COMPOSITION_STATUS,
        "generated_utc": timestamp,
        "collector_execution": dict(attempt["collector_execution"]),
        "runtime_execution": base._direct_execution(),  # noqa: SLF001
        "runtime_authority": dict(attempt["runtime_authority"]),
        "exact_artifact": dict(attempt["exact_artifact"]),
        "ordered_evidence_roles": list(evidence),
        "evidence": evidence,
        "composition_root_sha256": base._canonical_sha256(evidence),  # noqa: SLF001
        "composition_truth": {
            "historical_attempt4_resource_or_activation_invented": False,
            "remote_inode_reinterpreted_locally": False,
            "direct_release_rewritten": False,
            "direct_release_renamed": False,
            "post_release_evidence_additive": True,
            "single_operational_attempt": True,
        },
        "authority_design": dict(base.AUTHORITY_DESIGN),
        "permissions": dict(base.NO_AUTHORITY),
        "evidence_boundary": dict(base.EVIDENCE_BOUNDARY),
    }
    payload[COMPOSITION_CANONICAL_FIELD] = base._document_sha256(  # noqa: SLF001
        payload, COMPOSITION_CANONICAL_FIELD
    )
    return payload


def validate_composition(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    try:
        payload, _ = base._binding(  # noqa: SLF001
            path,
            label="cross-host final composition",
            canonical_field=COMPOSITION_CANONICAL_FIELD,
            expected_schema=COMPOSITION_SCHEMA,
            expected_status=COMPOSITION_STATUS,
        )
    except Exception as exc:
        raise CrossHostCompletionError("cross-host final composition is invalid") from exc
    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        raise CrossHostCompletionError("cross-host final composition evidence is missing")
    expected = build_composition(
        operational_attempt_path=Path(str(evidence.get("operational_attempt", {}).get("path", ""))),
        activation_envelope_path=Path(str(evidence.get("fresh_activation", {}).get("path", ""))),
        operational_completion_path=Path(str(evidence.get("operational_completion", {}).get("path", ""))),
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise CrossHostCompletionError("cross-host final composition identity drifted")
    return payload


def finalize_composition(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_composition(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_composition,
        validator_kwargs={
            "collector_repository_root": kwargs["collector_repository_root"],
            "direct_repository_root": kwargs["direct_repository_root"],
            "attempt4_repository_root": kwargs["attempt4_repository_root"],
        },
    )


def build_attempt_final(
    *,
    final_composition_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    composition = validate_composition(
        final_composition_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    binding = _receipt_binding(
        final_composition_path,
        label="cross-host final composition",
        canonical_field=COMPOSITION_CANONICAL_FIELD,
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    timestamp = generated_utc or base._now()  # noqa: SLF001
    _timestamp(timestamp, "cross-host attempt-final timestamp")
    payload = {
        "schema_version": ATTEMPT_FINAL_SCHEMA,
        "identity": OWNER,
        "attempt_id": composition["attempt_id"],
        "status": ATTEMPT_FINAL_STATUS,
        "generated_utc": timestamp,
        "runtime_execution": base._direct_execution(),  # noqa: SLF001
        "runtime_authority": dict(composition["runtime_authority"]),
        "exact_artifact": dict(composition["exact_artifact"]),
        "final_composition": binding,
        "composition_root_sha256": composition["composition_root_sha256"],
        "result": {
            "operational_evidence_complete": True,
            "cross_host_evidence_admitted": True,
            "runtime_authority_unchanged": True,
            "research_supported": False,
            "owner_risk_accepted": True,
            "new_authority_granted": False,
        },
        "authority_design": dict(base.AUTHORITY_DESIGN),
        "permissions": dict(base.NO_AUTHORITY),
        "evidence_boundary": dict(base.EVIDENCE_BOUNDARY),
    }
    payload[ATTEMPT_FINAL_CANONICAL_FIELD] = base._document_sha256(  # noqa: SLF001
        payload, ATTEMPT_FINAL_CANONICAL_FIELD
    )
    return payload


def validate_attempt_final(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    try:
        payload, _ = base._binding(  # noqa: SLF001
            path,
            label="cross-host attempt-final",
            canonical_field=ATTEMPT_FINAL_CANONICAL_FIELD,
            expected_schema=ATTEMPT_FINAL_SCHEMA,
            expected_status=ATTEMPT_FINAL_STATUS,
        )
    except Exception as exc:
        raise CrossHostCompletionError("cross-host attempt-final is invalid") from exc
    expected = build_attempt_final(
        final_composition_path=Path(str(payload.get("final_composition", {}).get("path", ""))),
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise CrossHostCompletionError("cross-host attempt-final identity drifted")
    return payload


def finalize_attempt_final(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_attempt_final(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_attempt_final,
        validator_kwargs={
            "collector_repository_root": kwargs["collector_repository_root"],
            "direct_repository_root": kwargs["direct_repository_root"],
            "attempt4_repository_root": kwargs["attempt4_repository_root"],
        },
    )


def build_evidence_release(
    *,
    attempt_final_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt_final = validate_attempt_final(
        attempt_final_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    final_binding = _receipt_binding(
        attempt_final_path,
        label="cross-host attempt-final",
        canonical_field=ATTEMPT_FINAL_CANONICAL_FIELD,
        schema=ATTEMPT_FINAL_SCHEMA,
        status=ATTEMPT_FINAL_STATUS,
    )
    direct_path = Path(str(attempt_final["runtime_authority"]["path"]))
    try:
        direct, direct_binding = base._direct_authority(  # noqa: SLF001
            direct_path, direct_repository_root=direct_repository_root
        )
    except Exception as exc:
        raise CrossHostCompletionError("immutable direct-v3 authority is invalid") from exc
    timestamp = generated_utc or base._now()  # noqa: SLF001
    _timestamp(timestamp, "cross-host evidence release timestamp")
    payload = {
        "schema_version": EVIDENCE_RELEASE_SCHEMA,
        "identity": OWNER,
        "attempt_id": attempt_final["attempt_id"],
        "status": EVIDENCE_RELEASE_STATUS,
        "generated_utc": timestamp,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authority_provenance": {
            "source": "immutable_direct_v3_owner_release",
            "new_authority_granted": False,
            "direct_release_canonical_sha256": direct_binding["canonical_sha256"],
            "direct_release_incomplete_record_preserved": True,
            "proof_release_replaces_runtime_authority": False,
        },
        "runtime_execution": base._direct_execution(),  # noqa: SLF001
        "runtime_authority": direct_binding,
        "exact_artifact": base._artifact_projection(direct),  # noqa: SLF001
        "scope": dict(direct["scope"]),
        "rollback": dict(direct["rollback"]),
        "operational_attempt_final": final_binding,
        "composition_root_sha256": attempt_final["composition_root_sha256"],
        "evidence_state": {
            "post_release_evidence_complete": True,
            "cross_host_evidence_admitted": True,
            "remote_inode_reinterpreted_locally": False,
            "direct_release_was_pre_evidence_owner_override": True,
            "exact_artifact_oof_available": False,
            "old_oof_applies_to_learning_algorithm_only": True,
            "runtime_authority_replaced": False,
            "runtime_consumed": False,
            "does_not_replace_runtime_active_release": True,
        },
        "authority_design": dict(base.AUTHORITY_DESIGN),
        "evidence_boundary": dict(base.EVIDENCE_BOUNDARY),
    }
    payload[EVIDENCE_RELEASE_CANONICAL_FIELD] = base._document_sha256(  # noqa: SLF001
        payload, EVIDENCE_RELEASE_CANONICAL_FIELD
    )
    return payload


def validate_evidence_release(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    try:
        payload, _ = base._binding(  # noqa: SLF001
            path,
            label="cross-host evidence-complete proof release",
            canonical_field=EVIDENCE_RELEASE_CANONICAL_FIELD,
            expected_schema=EVIDENCE_RELEASE_SCHEMA,
            expected_status=EVIDENCE_RELEASE_STATUS,
        )
    except Exception as exc:
        raise CrossHostCompletionError("cross-host evidence release is invalid") from exc
    expected = build_evidence_release(
        attempt_final_path=Path(str(payload.get("operational_attempt_final", {}).get("path", ""))),
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise CrossHostCompletionError("cross-host evidence release identity drifted")
    return payload


def finalize_evidence_release(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_evidence_release(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_evidence_release,
        validator_kwargs={
            "collector_repository_root": kwargs["collector_repository_root"],
            "direct_repository_root": kwargs["direct_repository_root"],
            "attempt4_repository_root": kwargs["attempt4_repository_root"],
        },
    )


def _roots(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--collector-repository-root", type=Path, required=True)
    parser.add_argument("--direct-repository-root", type=Path, required=True)
    parser.add_argument("--attempt4-repository-root", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    envelope = sub.add_parser("activation-envelope")
    _roots(envelope)
    envelope.add_argument("--operational-attempt", type=Path, required=True)
    envelope.add_argument("--cross-host-admission", type=Path, required=True)
    envelope.add_argument("--output", type=Path, required=True)
    completion = sub.add_parser("completion")
    _roots(completion)
    completion.add_argument("--operational-attempt", type=Path, required=True)
    completion.add_argument("--activation-envelope", type=Path, required=True)
    completion.add_argument("--lifecycle-admission", type=Path, required=True)
    completion.add_argument("--output", type=Path, required=True)
    composition = sub.add_parser("composition")
    _roots(composition)
    composition.add_argument("--operational-attempt", type=Path, required=True)
    composition.add_argument("--activation-envelope", type=Path, required=True)
    composition.add_argument("--operational-completion", type=Path, required=True)
    composition.add_argument("--output", type=Path, required=True)
    attempt_final = sub.add_parser("attempt-final")
    _roots(attempt_final)
    attempt_final.add_argument("--final-composition", type=Path, required=True)
    attempt_final.add_argument("--output", type=Path, required=True)
    evidence_release = sub.add_parser("evidence-release")
    _roots(evidence_release)
    evidence_release.add_argument("--attempt-final", type=Path, required=True)
    evidence_release.add_argument("--output", type=Path, required=True)
    validate = sub.add_parser("validate")
    _roots(validate)
    validate.add_argument(
        "--kind",
        choices=("activation-envelope", "completion", "composition", "attempt-final", "evidence-release"),
        required=True,
    )
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def _print(payload: Mapping[str, Any], file_sha: str | None) -> None:
    canonical = next(
        value
        for key, value in payload.items()
        if key.startswith("canonical_") and key.endswith("sha256")
    )
    row = {
        "schema_version": payload["schema_version"],
        "status": payload["status"],
        "canonical_sha256": canonical,
    }
    if file_sha is not None:
        row["file_sha256"] = file_sha
    print(base.json.dumps(row, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    roots = {
        "collector_repository_root": args.collector_repository_root,
        "direct_repository_root": args.direct_repository_root,
        "attempt4_repository_root": args.attempt4_repository_root,
    }
    if args.command == "activation-envelope":
        payload, file_sha = finalize_activation_envelope(
            operational_attempt_path=args.operational_attempt,
            cross_host_admission_path=args.cross_host_admission,
            output_path=args.output,
            **roots,
        )
    elif args.command == "completion":
        payload, file_sha = finalize_completion(
            operational_attempt_path=args.operational_attempt,
            activation_envelope_path=args.activation_envelope,
            lifecycle_admission_path=args.lifecycle_admission,
            output_path=args.output,
            **roots,
        )
    elif args.command == "composition":
        payload, file_sha = finalize_composition(
            operational_attempt_path=args.operational_attempt,
            activation_envelope_path=args.activation_envelope,
            operational_completion_path=args.operational_completion,
            output_path=args.output,
            **roots,
        )
    elif args.command == "attempt-final":
        payload, file_sha = finalize_attempt_final(
            final_composition_path=args.final_composition,
            output_path=args.output,
            **roots,
        )
    elif args.command == "evidence-release":
        payload, file_sha = finalize_evidence_release(
            attempt_final_path=args.attempt_final,
            output_path=args.output,
            **roots,
        )
    else:
        validators = {
            "activation-envelope": validate_activation_envelope,
            "completion": validate_completion,
            "composition": validate_composition,
            "attempt-final": validate_attempt_final,
            "evidence-release": validate_evidence_release,
        }
        payload = validators[args.kind](args.receipt, **roots)
        file_sha = None
    _print(payload, file_sha)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
