#!/usr/bin/env python3
"""Complete the external-venues-disabled direct-v4 BUY E3 evidence chain.

The historical operational-attempt-v10 receipt remains useful only for its
Attempt4, exact-V5, and direct-v3 regression evidence.  Runtime execution,
runtime authority, and exact-artifact identity in this chain come exclusively
from the source-frozen direct-v4 cross-host admission.  The final proof receipt
revalidates that direct-v4 owner release; it never promotes the historical v3
attempt into current runtime authority.

This module reads no economics, Validation, or sealed holdout data and creates
no strategy, shadow, companion, or hypothetical-action path.  Every output is
an immutable create-only 0600 JSON receipt with a canonical content identity.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from scripts import f05_buy_e3_cross_host_transport_v5 as transport
from scripts import f05_buy_e3_evidence_completion as base
from scripts import f05_buy_e3_final_evidence_v4 as historical_final_v4

OWNER: Final = base.OWNER

ENVELOPE_SCHEMA: Final = f"{OWNER}.cross_host_activation_envelope.v5"
ENVELOPE_STATUS: Final = (
    "direct_v4_external_venues_disabled_portable_activation_evidence_admitted"
)
ENVELOPE_CANONICAL_FIELD: Final = (
    "canonical_direct_v4_external_venues_disabled_activation_envelope_sha256"
)

COMPLETION_SCHEMA: Final = f"{OWNER}.cross_host_operational_evidence_completion.v5"
COMPLETION_STATUS: Final = "direct_v4_external_venues_disabled_operational_evidence_complete"
COMPLETION_CANONICAL_FIELD: Final = (
    "canonical_direct_v4_external_venues_disabled_operational_completion_sha256"
)

COMPOSITION_SCHEMA: Final = f"{OWNER}.cross_host_final_composition_receipt.v5"
COMPOSITION_STATUS: Final = "direct_v4_external_venues_disabled_operational_evidence_composed"
COMPOSITION_CANONICAL_FIELD: Final = (
    "canonical_direct_v4_external_venues_disabled_final_composition_sha256"
)

ATTEMPT_FINAL_SCHEMA: Final = f"{OWNER}.cross_host_operational_attempt_final_receipt.v5"
ATTEMPT_FINAL_STATUS: Final = (
    "direct_v4_external_venues_disabled_operational_attempt_results_bound"
)
ATTEMPT_FINAL_CANONICAL_FIELD: Final = (
    "canonical_direct_v4_external_venues_disabled_attempt_final_sha256"
)

EVIDENCE_RELEASE_SCHEMA: Final = f"{OWNER}.cross_host_proof_evidence_release.v5"
EVIDENCE_RELEASE_STATUS: Final = (
    "direct_v4_external_venues_disabled_active_authority_evidence_complete"
)
EVIDENCE_RELEASE_CANONICAL_FIELD: Final = (
    "canonical_direct_v4_external_venues_disabled_evidence_release_sha256"
)

CONTENT_BINDING_FIELDS: Final = tuple(transport.CONTENT_BINDING_FIELDS)
PORTABLE_SOURCE_ROLES: Final = tuple(transport.SOURCE_FILENAMES)
REQUIRED_ACTIVE_RUNTIME_SOURCES: Final = frozenset(
    {
        "live/config.py",
        "live/main.py",
        "strategy/boolean_cooldown_buy_e3.py",
        "strategy/maker_engine.py",
    }
)
V4_LIFECYCLE_RUNTIME_SOURCE_SHA256: Final = {
    "execution/order_lifecycle.py": (
        "9d97b7178fa64af0878d5c21efba6c334490d6cfdd8c4d1badf77d708a456817"
    ),
    "execution/order_lifecycle_journal_v2.py": (
        "b8536b3bce6fba34f4fdebc3063a967668b3254174eb3c46d1d33a604436b46b"
    ),
    "execution/order_lifecycle_journal_v2_strict_native.py": (
        "f97e47a2fd753116381bab807a9b96cfdcbda97646992f239bfd50c015a6c1a1"
    ),
    "execution/order_lifecycle_live_writer_v2.py": (
        "bf5382ebf0922653f9edf85728ee1eaee41f35070de9b6f7101f3cce12fdd4ae"
    ),
    "strategy/boolean_cooldown_buy_e3.py": (
        "643423fd04ff44aada8cbc1967a96df6180af87a1d8a02130acb8ab3a85c0cfa"
    ),
}
REQUIRED_V4_LIFECYCLE_SOURCES: Final = frozenset(
    {
        "strategy/boolean_cooldown_buy_e3.py",
        "execution/order_lifecycle.py",
        "execution/order_lifecycle_journal_v2.py",
        "execution/order_lifecycle_journal_v2_strict_native.py",
        "execution/order_lifecycle_live_writer_v2.py",
    }
)

LIFECYCLE_FIX_SUPPLEMENT_CONTENT: Final = {
    "schema_version": "f05_buy_e3_lifecycle_reject_fix_supplement.v1",
    "status": "lifecycle_only_runtime_fix_verified_no_economic_change",
    "file_sha256": "c7a83f37f679ab94f7c0c670d53a43d894295d94cc74927e3a83fd3313336e87",
    "canonical_field": "canonical_supplement_sha256",
    "canonical_sha256": "e69c4edb2025937a8569cbedd3163f3ec3b953a17fc904218e4df332dc1f221d",
    "size_bytes": 43428,
    "mode": "0600",
}

REJECTED_PREDECESSOR_EPOCH_ID: Final = "prospective-1787532118813602859-5382e2bcdaeb"
SUPERSEDED_V4_EPOCH_ID: Final = "prospective-1787542261153620067-48e29ea0b22c"
HISTORICAL_OPERATIONAL_ATTEMPT_V10_ID: Final = "operational-attempt-direct-v3-evidence-v10-20260824"
REJECTED_PREDECESSOR_CONTENT: Final = {
    "schema_version": "f05_buy_e3_rejected_predecessor_epoch_receipt.v1",
    "status": "rejected_not_admitted",
    "file_sha256": "c44f3f32ae61635ce683e5711f19fd59863e4235996a9401f48d62bc1af4d80b",
    "canonical_field": "canonical_rejected_epoch_receipt_sha256",
    "canonical_sha256": "4a3c01f7f178fa2d3f573a1696c637074fd74b51e846bd785886689ba44613d1",
    "size_bytes": 7124,
    "mode": "0600",
}
SUPERSEDED_V4_PROOF_CONTENT: Final = {
    "schema_version": historical_final_v4.EVIDENCE_RELEASE_SCHEMA,
    "status": historical_final_v4.EVIDENCE_RELEASE_STATUS,
    "file_sha256": "0f85849289cb9e42de7333117c7719e1a95a1561cf42e8a00366e0e8500df28f",
    "canonical_field": historical_final_v4.EVIDENCE_RELEASE_CANONICAL_FIELD,
    "canonical_sha256": "21ca796d12c0df733e3c1daba2fe8e326979ec0bb80b8b653275e14a6af97880",
    "size_bytes": 8031,
    "mode": "0600",
}
SUPERSEDED_V4_ACTIVE_CONFIG_SHA256: Final = (
    transport.resource_v6.config_successor.OLD_ACTIVE_CONFIG_SHA256
)

NO_NEW_AUTHORITY: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "hypothetical_live_actions_scored": False,
}
FORMAL_RESEARCH_STATE: Final = {
    "research_supported": False,
    "formal_hierarchy_passed": False,
    "formal_hard_gates_passed": False,
    "owner_risk_accepted": True,
    "old_oof_applies_to_learning_algorithm_only": True,
    "exact_artifact_oof_available": False,
}
AUTHORITY_DESIGN: Final = {
    "runtime_authority": "transport_source_frozen_direct_v4_owner_release",
    "runtime_authority_source": "validated_cross_host_admission_portable_evidence",
    "historical_operational_attempt_v10_is_runtime_authority": False,
    "historical_direct_v3_is_runtime_authority": False,
    "superseded_v4_proof_is_runtime_authority": False,
    "proof_release_replaces_direct_v4_runtime_authority": False,
    "runtime_authority_replaced": False,
    "runtime_consumed": True,
    "runtime_consumed_authority": "direct_v4_owner_release_v2",
    "does_not_replace_runtime_active_release": True,
    "retrospective_authority_created": False,
    "evidence_is_additive_only": True,
    "external_venues_disabled_config_correction_bound": True,
}

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")


class FinalEvidenceV5Error(RuntimeError):
    """Raised when any direct-v4 evidence identity fails closed."""


def _timestamp(value: Any, label: str) -> str:
    try:
        return base._timestamp(value, label)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV5Error(f"{label} is invalid") from exc


def _now() -> str:
    return base._now()  # noqa: SLF001


def _canonical_sha256(value: Any) -> str:
    return base._canonical_sha256(value)  # noqa: SLF001


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    return base._document_sha256(payload, field)  # noqa: SLF001


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise FinalEvidenceV5Error(f"{label} is not a lowercase SHA256")
    return normalized


def _content_projection(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FinalEvidenceV5Error(f"{label} content binding is missing")
    projected = {field: value.get(field) for field in CONTENT_BINDING_FIELDS}
    _require_sha256(projected["file_sha256"], f"{label} file SHA256")
    _require_sha256(projected["canonical_sha256"], f"{label} canonical SHA256")
    if (
        not isinstance(projected["schema_version"], str)
        or not projected["schema_version"]
        or not isinstance(projected["status"], (str, type(None)))
        or not isinstance(projected["canonical_field"], str)
        or not (
            projected["canonical_field"] == "artifact_sha256"
            or (
                projected["canonical_field"].startswith("canonical_")
                and projected["canonical_field"].endswith("sha256")
            )
        )
        or not isinstance(projected["size_bytes"], int)
        or isinstance(projected["size_bytes"], bool)
        or projected["size_bytes"] <= 0
        or projected["mode"] != "0600"
    ):
        raise FinalEvidenceV5Error(f"{label} content binding is malformed")
    return projected


def _exact_content(value: Any, expected: Mapping[str, Any], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(CONTENT_BINDING_FIELDS):
        raise FinalEvidenceV5Error(f"{label} exact7 fields drifted")
    observed = _content_projection(value, label)
    if observed != dict(expected):
        raise FinalEvidenceV5Error(f"{label} exact7 identity drifted")
    return observed


def _receipt_binding(
    path: Path,
    *,
    label: str,
    canonical_field: str,
    schema: str,
    status: str,
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
        raise FinalEvidenceV5Error(f"{label} binding is invalid") from exc


def _read_own_receipt(
    path: Path,
    *,
    label: str,
    canonical_field: str,
    schema: str,
    status: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        payload, binding = base._binding(  # noqa: SLF001
            path,
            label=label,
            canonical_field=canonical_field,
            expected_schema=schema,
            expected_status=status,
        )
    except Exception as exc:
        raise FinalEvidenceV5Error(f"{label} is invalid") from exc
    return payload, binding


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
        raise FinalEvidenceV5Error(f"receipt creation failed: {output_path}") from exc
    if observed != payload:
        raise FinalEvidenceV5Error("written receipt differs after validation")
    return payload, file_sha


def _artifact_content_projection(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FinalEvidenceV5Error(f"{label} artifact is missing")
    roles = value.get("roles")
    if not isinstance(roles, Mapping) or set(roles) != {"manifest", "policy", "predicate_bundle"}:
        raise FinalEvidenceV5Error(f"{label} artifact roles drifted")
    artifact_sha = _require_sha256(value.get("artifact_sha256"), f"{label} artifact")
    return {
        "artifact_sha256": artifact_sha,
        "roles": {
            role: _content_projection(roles[role], f"{label} {role}")
            for role in ("manifest", "policy", "predicate_bundle")
        },
    }


def _validate_config_correction_content(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(CONTENT_BINDING_FIELDS):
        raise FinalEvidenceV5Error("config correction exact7 fields drifted")
    content = _content_projection(value, "config correction")
    successor = transport.resource_v6.config_successor
    if (
        content["schema_version"] != successor.SCHEMA_VERSION
        or content["status"] != successor.STATUS
        or content["canonical_field"] != successor.CANONICAL_FIELD
        or content["file_sha256"] != transport.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256
        or content["canonical_sha256"]
        != transport.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256
    ):
        raise FinalEvidenceV5Error("config correction exact7 identity drifted")
    return content


def _validate_superseded_v4_historical(value: Any) -> dict[str, Any]:
    expected_fields = {
        "role",
        "epoch_id",
        "active_config_sha256",
        "proof_evidence_release",
        "current_runtime_authority",
        "current_cross_host_evidence",
        "current_lifecycle_evidence",
        "reused_as_current_evidence",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise FinalEvidenceV5Error("superseded direct-v4 historical fields drifted")
    proof = _exact_content(
        value.get("proof_evidence_release"),
        SUPERSEDED_V4_PROOF_CONTENT,
        "superseded direct-v4 proof",
    )
    if (
        value.get("role")
        != "superseded_historical_due_external_venue_shadow_enabled"
        or value.get("epoch_id") != SUPERSEDED_V4_EPOCH_ID
        or value.get("active_config_sha256") != SUPERSEDED_V4_ACTIVE_CONFIG_SHA256
        or any(
            value.get(name) is not False
            for name in (
                "current_runtime_authority",
                "current_cross_host_evidence",
                "current_lifecycle_evidence",
                "reused_as_current_evidence",
            )
        )
    ):
        raise FinalEvidenceV5Error("superseded direct-v4 historical identity drifted")
    return {**dict(value), "proof_evidence_release": proof}


def _historical_attempt_context(
    path: Path,
    *,
    historical_collector_v10_root: Path,
    historical_direct_v3_root: Path,
    attempt4_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        attempt = base.validate_operational_attempt(
            path,
            collector_repository_root=historical_collector_v10_root,
            direct_repository_root=historical_direct_v3_root,
            attempt4_repository_root=attempt4_root,
        )
    except Exception as exc:
        raise FinalEvidenceV5Error("historical operational-attempt-v10 is invalid") from exc
    binding = _receipt_binding(
        path,
        label="historical operational-attempt-v10",
        canonical_field="canonical_operational_attempt_sha256",
        schema=base.OPERATIONAL_ATTEMPT_SCHEMA,
        status=base.OPERATIONAL_ATTEMPT_STATUS,
    )
    if attempt.get("attempt_id") != HISTORICAL_OPERATIONAL_ATTEMPT_V10_ID:
        raise FinalEvidenceV5Error("historical operational attempt is not v10")
    current = attempt.get("current_runtime_evidence")
    if not isinstance(current, Mapping) or set(current) != {
        "full_regression",
        "focused_successor_regression",
        "sell54_parity",
    }:
        raise FinalEvidenceV5Error("historical direct-v3 regression evidence drifted")
    projection = {
        "role": "historical_mechanics_and_regression_anchor_only",
        "attempt_id": attempt["attempt_id"],
        "operational_attempt": binding,
        "attempt4_mechanics_anchor": _content_projection(
            attempt.get("historical_attempt4_anchor"), "historical Attempt4 anchor"
        ),
        "exact_v5_mechanics_recovery": _content_projection(
            attempt.get("exact_v5_recovery"), "historical exact V5 recovery"
        ),
        "direct_v3_runtime_execution": dict(attempt.get("runtime_execution", {})),
        "direct_v3_runtime_authority": _content_projection(
            attempt.get("runtime_authority"), "historical direct-v3 authority"
        ),
        "direct_v3_exact_artifact": _artifact_content_projection(
            attempt.get("exact_artifact"), "historical direct-v3"
        ),
        "direct_v3_regressions": {
            role: _content_projection(current[role], f"historical direct-v3 {role}")
            for role in ("full_regression", "focused_successor_regression", "sell54_parity")
        },
        "attempt4_resource_or_activation_claimed": False,
        "final_runtime_authority": False,
        "final_exact_artifact_authority": False,
    }
    return attempt, binding, projection


def _validate_supplement_payload(payload: Any) -> None:
    if not isinstance(payload, Mapping):
        raise FinalEvidenceV5Error("lifecycle fix supplement payload is missing")
    unchanged = payload.get("e3_unchanged")
    permissions = payload.get("permissions")
    focused = payload.get("focused_regression")
    full = payload.get("full_regression")
    changed_runtime = payload.get("changed_runtime_files")
    expected_execution = {
        "execution_commit": transport.FROZEN_FINAL_EXECUTION_COMMIT,
        "execution_tree": transport.FROZEN_FINAL_EXECUTION_TREE,
        "annotated_operational_tag": transport.FROZEN_FINAL_ANNOTATED_TAG,
        "annotated_operational_tag_object": transport.FROZEN_FINAL_TAG_OBJECT,
        "tag_peeled_commit": transport.FROZEN_FINAL_EXECUTION_COMMIT,
    }
    if (
        payload.get("schema_version") != LIFECYCLE_FIX_SUPPLEMENT_CONTENT["schema_version"]
        or payload.get("status") != LIFECYCLE_FIX_SUPPLEMENT_CONTENT["status"]
        or payload.get("v4_execution") != expected_execution
        or not isinstance(unchanged, Mapping)
        or unchanged.get("verified") is not True
        or unchanged.get("artifact_sha256") != transport.FROZEN_FINAL_ARTIFACT_SHA256
        or unchanged.get("action_vocabulary_seconds") != [79, 173, 223, 356, 640, 709, 2048]
        or not isinstance(changed_runtime, Mapping)
        or set(changed_runtime) != REQUIRED_V4_LIFECYCLE_SOURCES
        or any(
            not isinstance(changed_runtime.get(path), Mapping)
            or changed_runtime[path].get("file_sha256") != digest
            for path, digest in V4_LIFECYCLE_RUNTIME_SOURCE_SHA256.items()
        )
        or not isinstance(permissions, Mapping)
        or any(value is not False for value in permissions.values())
        or not isinstance(focused, Mapping)
        or focused.get("passed") != 12
        or focused.get("failed") != 0
        or not isinstance(full, Mapping)
        or full.get("passed") != 157
        or full.get("failed") != 0
    ):
        raise FinalEvidenceV5Error("lifecycle fix supplement semantics drifted")


def _supplement_context(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    frozen = transport.FROZEN_FINAL_LIFECYCLE_FIX_SUPPLEMENT
    exact_frozen = _exact_content(
        frozen, LIFECYCLE_FIX_SUPPLEMENT_CONTENT, "transport lifecycle fix supplement"
    )
    try:
        payload, binding = base._binding(  # noqa: SLF001
            path,
            label="lifecycle fix supplement",
            canonical_field=str(exact_frozen["canonical_field"]),
            expected_schema=str(exact_frozen["schema_version"]),
            expected_status=str(exact_frozen["status"]),
        )
    except Exception as exc:
        raise FinalEvidenceV5Error("lifecycle fix supplement is invalid") from exc
    content = _content_projection(binding, "lifecycle fix supplement")
    if content != exact_frozen:
        raise FinalEvidenceV5Error("lifecycle fix supplement byte identity drifted")
    _validate_supplement_payload(payload)
    return payload, content


def _validate_rejected_predecessor_payload(payload: Any) -> None:
    if not isinstance(payload, Mapping):
        raise FinalEvidenceV5Error("rejected predecessor payload is missing")
    epoch = payload.get("epoch")
    rejection = payload.get("rejection")
    boundary = payload.get("authority_boundary")
    if (
        payload.get("schema_version") != REJECTED_PREDECESSOR_CONTENT["schema_version"]
        or payload.get("status") != "rejected_not_admitted"
        or not isinstance(epoch, Mapping)
        or epoch.get("baseline_epoch_id") != REJECTED_PREDECESSOR_EPOCH_ID
        or not isinstance(rejection, Mapping)
        or rejection.get("error_count") != 1
        or rejection.get("drop_count") != 0
        or rejection.get("exchange_error_code") != -5022
        or rejection.get("formal_collection_valid") is not False
        or rejection.get("formal_admission_allowed") is not False
        or not isinstance(boundary, Mapping)
        or boundary.get("final_active_capture") is not False
        or boundary.get("lifecycle_admission") is not False
        or boundary.get("successor_runtime_authority") is not False
        or boundary.get("research_supported") is not False
        or boundary.get("shadow_created") is not False
        or boundary.get("companion_created") is not False
    ):
        raise FinalEvidenceV5Error("rejected predecessor lifecycle semantics drifted")


def _rejected_predecessor_context(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        payload, binding = base._binding(  # noqa: SLF001
            path,
            label="rejected predecessor epoch receipt",
            canonical_field=str(REJECTED_PREDECESSOR_CONTENT["canonical_field"]),
            expected_schema=str(REJECTED_PREDECESSOR_CONTENT["schema_version"]),
            expected_status=str(REJECTED_PREDECESSOR_CONTENT["status"]),
        )
    except Exception as exc:
        raise FinalEvidenceV5Error("rejected predecessor epoch receipt is invalid") from exc
    content = _content_projection(binding, "rejected predecessor epoch receipt")
    if content != REJECTED_PREDECESSOR_CONTENT:
        raise FinalEvidenceV5Error("rejected predecessor epoch exact7 identity drifted")
    _validate_rejected_predecessor_payload(payload)
    return payload, content


def _superseded_v4_proof_context(
    path: Path,
    *,
    final_v4_root: Path,
    final_v4_release: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Reopen the old proof, then retain only its immutable historical exact7."""

    try:
        proof = historical_final_v4.validate_evidence_release(
            path,
            final_v4_root=final_v4_root,
            final_v4_release=final_v4_release,
        )
        proof_binding = _receipt_binding(
            path,
            label="superseded direct-v4 proof evidence-release",
            canonical_field=historical_final_v4.EVIDENCE_RELEASE_CANONICAL_FIELD,
            schema=historical_final_v4.EVIDENCE_RELEASE_SCHEMA,
            status=historical_final_v4.EVIDENCE_RELEASE_STATUS,
        )
        exact_proof = _content_projection(proof_binding, "superseded direct-v4 proof")
        if exact_proof != SUPERSEDED_V4_PROOF_CONTENT:
            raise FinalEvidenceV5Error("superseded direct-v4 proof exact7 identity drifted")
        attempt_path = Path(str(proof.get("operational_attempt_final", {}).get("path", "")))
        attempt, attempt_binding = historical_final_v4._read_own_receipt(  # noqa: SLF001
            attempt_path,
            label="superseded direct-v4 attempt-final",
            canonical_field=historical_final_v4.ATTEMPT_FINAL_CANONICAL_FIELD,
            schema=historical_final_v4.ATTEMPT_FINAL_SCHEMA,
            status=historical_final_v4.ATTEMPT_FINAL_STATUS,
        )
        composition_path = Path(str(attempt.get("final_composition", {}).get("path", "")))
        composition, composition_binding = historical_final_v4._read_own_receipt(  # noqa: SLF001
            composition_path,
            label="superseded direct-v4 composition",
            canonical_field=historical_final_v4.COMPOSITION_CANONICAL_FIELD,
            schema=historical_final_v4.COMPOSITION_SCHEMA,
            status=historical_final_v4.COMPOSITION_STATUS,
        )
    except Exception as exc:
        raise FinalEvidenceV5Error("superseded direct-v4 proof is invalid") from exc

    evidence = composition.get("evidence")
    lifecycle = (
        evidence.get("final_v4_lifecycle_admission") if isinstance(evidence, Mapping) else None
    )
    expected_execution = {
        "execution_commit": transport.FROZEN_FINAL_EXECUTION_COMMIT,
        "execution_tree": transport.FROZEN_FINAL_EXECUTION_TREE,
        "annotated_operational_tag": transport.FROZEN_FINAL_ANNOTATED_TAG,
        "annotated_operational_tag_object": transport.FROZEN_FINAL_TAG_OBJECT,
        "tag_peeled_commit": transport.FROZEN_FINAL_EXECUTION_COMMIT,
    }
    if (
        proof.get("operational_attempt_final") != attempt_binding
        or attempt.get("final_composition") != composition_binding
        or proof.get("runtime_execution") != expected_execution
        or composition.get("runtime_execution") != expected_execution
        or not isinstance(lifecycle, Mapping)
        or lifecycle.get("baseline_epoch_id") != SUPERSEDED_V4_EPOCH_ID
        or lifecycle.get("config_sha256") != SUPERSEDED_V4_ACTIVE_CONFIG_SHA256
    ):
        raise FinalEvidenceV5Error("superseded direct-v4 proof epoch/config identity drifted")
    historical = {
        "role": "superseded_historical_due_external_venue_shadow_enabled",
        "epoch_id": SUPERSEDED_V4_EPOCH_ID,
        "active_config_sha256": SUPERSEDED_V4_ACTIVE_CONFIG_SHA256,
        "proof_evidence_release": exact_proof,
        "current_runtime_authority": False,
        "current_cross_host_evidence": False,
        "current_lifecycle_evidence": False,
        "reused_as_current_evidence": False,
    }
    return proof, exact_proof, historical


def _final_authority_context(
    final_v4_root: Path, final_v4_release: Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        release, binding = transport.validate_runtime_authority(final_v4_root, final_v4_release)
        execution = transport._frozen_final_execution()  # noqa: SLF001
        artifact = transport._artifact_projection(release)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV5Error("source-frozen direct-v4 runtime authority is invalid") from exc
    exact_binding = _content_projection(binding, "direct-v4 runtime authority")
    if (
        exact_binding.get("schema_version") != transport.FROZEN_FINAL_RELEASE_SCHEMA
        or exact_binding.get("status") != transport.FROZEN_FINAL_RELEASE_STATUS
        or exact_binding.get("file_sha256") != transport.FROZEN_FINAL_RELEASE_FILE_SHA256
        or exact_binding.get("canonical_sha256") != transport.FROZEN_FINAL_RELEASE_CANONICAL_SHA256
        or artifact.get("artifact_sha256") != transport.FROZEN_FINAL_ARTIFACT_SHA256
    ):
        raise FinalEvidenceV5Error("direct-v4 runtime authority constants drifted")
    return release, exact_binding, execution, artifact


def _validate_portable_v5(
    value: Any,
    *,
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
    execution: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(transport.PORTABLE_EVIDENCE_FIELDS):
        raise FinalEvidenceV5Error("portable direct-v4 evidence fields drifted")
    portable = dict(value)
    try:
        transport._assert_portable(portable)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV5Error("portable direct-v4 evidence contains local authority") from exc
    expected_authority = {
        **dict(release_binding),
        "execution": dict(execution),
        "runtime_authority": True,
    }
    if portable.get("runtime_execution") != execution:
        raise FinalEvidenceV5Error("portable runtime execution is not direct-v4")
    if portable.get("runtime_authority") != expected_authority:
        raise FinalEvidenceV5Error("portable runtime authority is not direct-v4 release-v2")
    if portable.get("exact_artifact") != artifact:
        raise FinalEvidenceV5Error("portable exact artifact is not direct-v4 authority artifact")

    host = portable.get("host")
    if (
        not isinstance(host, Mapping)
        or host.get("provider") != transport.CURRENT_PROVIDER
        or host.get("region") != transport.CURRENT_REGION
        or host.get("instance_id") != transport.CURRENT_INSTANCE_ID
        or host.get("instance_type") != transport.CURRENT_INSTANCE_TYPE
        or host.get("public_ipv4") != transport.CURRENT_PUBLIC_IPV4_PROVENANCE
        or host.get("public_ipv4_role") != "network_locator_provenance_only_not_host_authority"
    ):
        raise FinalEvidenceV5Error("portable current-host identity drifted")

    disabled = portable.get("resource_disabled_process")
    transition = portable.get("transition")
    if not isinstance(disabled, Mapping) or not isinstance(transition, Mapping):
        raise FinalEvidenceV5Error("portable process transition is missing")
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
        or disabled.get("config_sha256") != transport.FROZEN_FINAL_DISABLED_CONFIG_SHA256
        or disabled.get("fresh_pid") is not True
        or disabled.get("fresh_start_ticks") is not True
        or disabled.get("same_pid_pre_post") is not True
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
        raise FinalEvidenceV5Error("portable direct-v4 process transition drifted")

    active = portable.get("active_runtime")
    if not isinstance(active, Mapping):
        raise FinalEvidenceV5Error("portable direct-v4 active runtime is missing")
    runtime_files = active.get("runtime_source_files")
    startup = active.get("startup_semantics")
    if (
        active.get("config_sha256") != transport.FROZEN_FINAL_ACTIVE_CONFIG_SHA256
        or active.get("artifact_sha256") != transport.FROZEN_FINAL_ARTIFACT_SHA256
        or active.get("buy_e3_enabled") is not True
        or active.get("owner_override_effective") is not True
        or not isinstance(runtime_files, Mapping)
        or not REQUIRED_ACTIVE_RUNTIME_SOURCES.issubset(runtime_files)
        or any(_SHA256_RE.fullmatch(str(digest)) is None for digest in runtime_files.values())
        or not isinstance(startup, Mapping)
        or startup.get("startup_status") != "accepted"
        or startup.get("running_checkout_commit") != execution["execution_commit"]
        or startup.get("running_checkout_tree") != execution["execution_tree"]
    ):
        raise FinalEvidenceV5Error("portable direct-v4 active runtime drifted")

    receipts = portable.get("source_receipts")
    if not isinstance(receipts, Mapping) or set(receipts) != set(PORTABLE_SOURCE_ROLES):
        raise FinalEvidenceV5Error("portable source receipt roles drifted")
    expected_receipt_identity = {
        "config_correction": (
            transport.resource_v6.config_successor.SCHEMA_VERSION,
            transport.resource_v6.config_successor.STATUS,
            transport.resource_v6.config_successor.CANONICAL_FIELD,
        ),
        "current_host_resource_gate": (
            transport.FROZEN_FINAL_RESOURCE_SCHEMA,
            transport.FROZEN_FINAL_RESOURCE_STATUS,
            "canonical_resource_receipt_sha256",
        ),
        "active_process_capture": (
            transport.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
            transport.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
            "canonical_active_capture_sha256",
        ),
        "remote_active_attestation": (
            transport.REMOTE_ATTESTATION_SCHEMA,
            transport.REMOTE_ATTESTATION_STATUS,
            transport.REMOTE_ATTESTATION_CANONICAL_FIELD,
        ),
    }
    normalized_receipts: dict[str, Any] = {}
    for role in PORTABLE_SOURCE_ROLES:
        row = receipts[role]
        if not isinstance(row, Mapping) or set(row) != {
            *CONTENT_BINDING_FIELDS,
            "local_filename",
        }:
            raise FinalEvidenceV5Error(f"portable {role} content fields drifted")
        content = _content_projection(row, f"portable {role}")
        schema, status, canonical_field = expected_receipt_identity[role]
        if (
            content["schema_version"] != schema
            or content["status"] != status
            or content["canonical_field"] != canonical_field
            or row.get("local_filename") != transport.SOURCE_FILENAMES[role]
        ):
            raise FinalEvidenceV5Error(f"portable {role} identity drifted")
        if role == "config_correction" and (
            content["file_sha256"]
            != transport.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256
            or content["canonical_sha256"]
            != transport.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256
        ):
            raise FinalEvidenceV5Error("portable config_correction exact7 identity drifted")
        normalized_receipts[role] = dict(row)
    portable["source_receipts"] = normalized_receipts
    # Keep the release argument semantically live: this projection must be the
    # exact artifact derived from the release validator, never from history.
    if transport._artifact_projection(release) != portable["exact_artifact"]:  # noqa: SLF001
        raise FinalEvidenceV5Error("portable artifact/release cross-binding drifted")
    return portable


def _admission_context(
    path: Path,
    *,
    final_v4_root: Path,
    final_v4_release: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    release, release_binding, execution, artifact = _final_authority_context(
        final_v4_root, final_v4_release
    )
    try:
        admission = transport.validate_cross_host_admission(
            path,
            direct_repository_root=final_v4_root,
            direct_release_path=final_v4_release,
        )
    except Exception as exc:
        raise FinalEvidenceV5Error("direct-v4 cross-host admission is invalid") from exc
    binding = _receipt_binding(
        path,
        label="direct-v4 cross-host admission",
        canonical_field=transport.ADMISSION_CANONICAL_FIELD,
        schema=transport.ADMISSION_SCHEMA,
        status=transport.ADMISSION_STATUS,
    )
    portable = _validate_portable_v5(
        admission.get("portable_evidence"),
        release=release,
        release_binding=release_binding,
        execution=execution,
        artifact=artifact,
    )
    return admission, binding, portable


def _assert_history_is_not_final(history: Mapping[str, Any], portable: Mapping[str, Any]) -> None:
    historical_execution = history.get("direct_v3_runtime_execution")
    historical_authority = history.get("direct_v3_runtime_authority")
    final_authority = portable.get("runtime_authority")
    historical_artifact = history.get("direct_v3_exact_artifact")
    if (
        historical_execution == portable.get("runtime_execution")
        or not isinstance(historical_authority, Mapping)
        or not isinstance(final_authority, Mapping)
        or historical_authority.get("canonical_sha256") == final_authority.get("canonical_sha256")
        or not isinstance(historical_artifact, Mapping)
        or historical_artifact.get("artifact_sha256")
        != portable.get("exact_artifact", {}).get("artifact_sha256")
    ):
        raise FinalEvidenceV5Error("historical direct-v3 and final direct-v4 roles were conflated")


def build_activation_envelope(
    *,
    historical_operational_attempt_v10_path: Path,
    superseded_v4_proof_path: Path,
    cross_host_admission_path: Path,
    lifecycle_fix_supplement_path: Path,
    rejected_predecessor_path: Path,
    historical_collector_v10_root: Path,
    historical_direct_v3_root: Path,
    attempt4_root: Path,
    final_v4_root: Path,
    final_v4_release: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    _attempt, attempt_binding, history = _historical_attempt_context(
        historical_operational_attempt_v10_path,
        historical_collector_v10_root=historical_collector_v10_root,
        historical_direct_v3_root=historical_direct_v3_root,
        attempt4_root=attempt4_root,
    )
    _admission, admission_binding, portable = _admission_context(
        cross_host_admission_path,
        final_v4_root=final_v4_root,
        final_v4_release=final_v4_release,
    )
    _supplement, supplement_content = _supplement_context(lifecycle_fix_supplement_path)
    _predecessor, predecessor_content = _rejected_predecessor_context(rejected_predecessor_path)
    _old_proof, _old_proof_content, superseded_v4 = _superseded_v4_proof_context(
        superseded_v4_proof_path,
        final_v4_root=final_v4_root,
        final_v4_release=final_v4_release,
    )
    _assert_history_is_not_final(history, portable)
    config_correction = _validate_config_correction_content(
        _content_projection(
            portable["source_receipts"]["config_correction"],
            "portable config correction",
        ),
    )
    superseded_v4 = _validate_superseded_v4_historical(superseded_v4)
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "direct-v4 activation envelope timestamp")
    payload = {
        "schema_version": ENVELOPE_SCHEMA,
        "identity": OWNER,
        "status": ENVELOPE_STATUS,
        "generated_utc": timestamp,
        "historical_operational_attempt_v10": attempt_binding,
        "historical_evidence": history,
        "superseded_v4_proof": superseded_v4,
        "cross_host_admission": admission_binding,
        "config_correction": config_correction,
        "lifecycle_fix_supplement": supplement_content,
        "rejected_predecessor_epoch": {
            "epoch_id": REJECTED_PREDECESSOR_EPOCH_ID,
            "evidence": predecessor_content,
            "status": "rejected_not_admitted",
            "error_count": 1,
            "drop_count": 0,
            "exchange_error_code": -5022,
            "formal_collection_valid": False,
            "admitted": False,
            "reused_for_final_v4": False,
        },
        "host": dict(portable["host"]),
        "runtime_execution": dict(portable["runtime_execution"]),
        "runtime_authority": dict(portable["runtime_authority"]),
        "exact_artifact": dict(portable["exact_artifact"]),
        "resource_disabled_process": dict(portable["resource_disabled_process"]),
        "transition": dict(portable["transition"]),
        "active_runtime": dict(portable["active_runtime"]),
        "source_receipts": dict(portable["source_receipts"]),
        "checks": {
            "historical_v10_validated_as_history_only": True,
            "historical_v3_differs_from_final_v4": True,
            "superseded_v4_proof_exact7": True,
            "superseded_v4_epoch_is_history_only": True,
            "superseded_v4_proof_is_current_authority": False,
            "direct_v4_portable_admission_validated": True,
            "direct_v4_release_v2_is_only_runtime_authority": True,
            "external_venues_disabled_config_correction_exact7": True,
            "disabled_and_active_config_hashes_are_corrected_pair": True,
            "lifecycle_fix_supplement_exact7": True,
            "failed_predecessor_exact7": True,
            "failed_predecessor_rejected_not_admitted": True,
            "failed_predecessor_reused": False,
            "remote_inode_reinterpreted_locally": False,
        },
        "formal_research_state": dict(FORMAL_RESEARCH_STATE),
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_NEW_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[ENVELOPE_CANONICAL_FIELD] = _document_sha256(payload, ENVELOPE_CANONICAL_FIELD)
    return payload


def validate_activation_envelope(
    path: Path,
    *,
    lifecycle_fix_supplement_path: Path,
    rejected_predecessor_path: Path,
    historical_collector_v10_root: Path,
    historical_direct_v3_root: Path,
    attempt4_root: Path,
    final_v4_root: Path,
    final_v4_release: Path,
    superseded_v4_proof_path: Path,
) -> dict[str, Any]:
    payload, _binding = _read_own_receipt(
        path,
        label="direct-v4 activation envelope",
        canonical_field=ENVELOPE_CANONICAL_FIELD,
        schema=ENVELOPE_SCHEMA,
        status=ENVELOPE_STATUS,
    )
    expected = build_activation_envelope(
        historical_operational_attempt_v10_path=Path(
            str(payload.get("historical_operational_attempt_v10", {}).get("path", ""))
        ),
        superseded_v4_proof_path=superseded_v4_proof_path,
        cross_host_admission_path=Path(
            str(payload.get("cross_host_admission", {}).get("path", ""))
        ),
        lifecycle_fix_supplement_path=lifecycle_fix_supplement_path,
        rejected_predecessor_path=rejected_predecessor_path,
        historical_collector_v10_root=historical_collector_v10_root,
        historical_direct_v3_root=historical_direct_v3_root,
        attempt4_root=attempt4_root,
        final_v4_root=final_v4_root,
        final_v4_release=final_v4_release,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise FinalEvidenceV5Error("direct-v4 activation envelope identity drifted")
    return payload


def finalize_activation_envelope(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_activation_envelope(**kwargs)
    validator_kwargs = _validation_roots(kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_activation_envelope,
        validator_kwargs=validator_kwargs,
    )


def _activation_context(path: Path, **roots: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = validate_activation_envelope(path, **roots)
    binding = _receipt_binding(
        path,
        label="direct-v4 activation envelope",
        canonical_field=ENVELOPE_CANONICAL_FIELD,
        schema=ENVELOPE_SCHEMA,
        status=ENVELOPE_STATUS,
    )
    return payload, binding


def _lifecycle_context(
    path: Path,
    *,
    active_runtime: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        payload, binding = base._validate_lifecycle_admission(path)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV5Error("new direct-v4 lifecycle admission is invalid") from exc
    active_files = active_runtime.get("runtime_source_files")
    admitted_files = binding.get("runtime_code_files")
    baseline_epoch = str(binding.get("baseline_epoch_id", ""))
    if (
        baseline_epoch in {REJECTED_PREDECESSOR_EPOCH_ID, SUPERSEDED_V4_EPOCH_ID}
        or not baseline_epoch.startswith("prospective-")
        or active_runtime.get("config_sha256") != transport.FROZEN_FINAL_ACTIVE_CONFIG_SHA256
        or binding.get("config_sha256") != active_runtime.get("config_sha256")
        or not isinstance(active_files, Mapping)
        or not isinstance(admitted_files, Mapping)
        or any(admitted_files.get(path) != digest for path, digest in active_files.items())
        or not REQUIRED_ACTIVE_RUNTIME_SOURCES.issubset(active_files)
        or not REQUIRED_V4_LIFECYCLE_SOURCES.issubset(admitted_files)
        or any(
            admitted_files.get(path) != digest
            for path, digest in V4_LIFECYCLE_RUNTIME_SOURCE_SHA256.items()
        )
    ):
        raise FinalEvidenceV5Error(
            "lifecycle admission epoch/config/runtime files do not match active direct-v4"
        )
    return payload, dict(binding)


def build_completion(
    *,
    activation_envelope_path: Path,
    lifecycle_admission_path: Path,
    lifecycle_fix_supplement_path: Path,
    rejected_predecessor_path: Path,
    historical_collector_v10_root: Path,
    historical_direct_v3_root: Path,
    attempt4_root: Path,
    final_v4_root: Path,
    final_v4_release: Path,
    superseded_v4_proof_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    roots = {
        "lifecycle_fix_supplement_path": lifecycle_fix_supplement_path,
        "rejected_predecessor_path": rejected_predecessor_path,
        "historical_collector_v10_root": historical_collector_v10_root,
        "historical_direct_v3_root": historical_direct_v3_root,
        "attempt4_root": attempt4_root,
        "final_v4_root": final_v4_root,
        "final_v4_release": final_v4_release,
        "superseded_v4_proof_path": superseded_v4_proof_path,
    }
    envelope, envelope_binding = _activation_context(activation_envelope_path, **roots)
    lifecycle, lifecycle_binding = _lifecycle_context(
        lifecycle_admission_path, active_runtime=envelope["active_runtime"]
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "direct-v4 operational completion timestamp")
    payload = {
        "schema_version": COMPLETION_SCHEMA,
        "identity": OWNER,
        "attempt_id": envelope["historical_evidence"]["attempt_id"],
        "status": COMPLETION_STATUS,
        "generated_utc": timestamp,
        "activation_envelope": envelope_binding,
        "historical_operational_attempt_v10": dict(envelope["historical_operational_attempt_v10"]),
        "historical_evidence": dict(envelope["historical_evidence"]),
        "superseded_v4_proof": dict(envelope["superseded_v4_proof"]),
        "cross_host_admission": dict(envelope["cross_host_admission"]),
        "config_correction": dict(envelope["config_correction"]),
        "lifecycle_fix_supplement": dict(envelope["lifecycle_fix_supplement"]),
        "rejected_predecessor_epoch": dict(envelope["rejected_predecessor_epoch"]),
        "runtime_execution": dict(envelope["runtime_execution"]),
        "runtime_authority": dict(envelope["runtime_authority"]),
        "exact_artifact": dict(envelope["exact_artifact"]),
        "current_host_resource": dict(envelope["source_receipts"]["current_host_resource_gate"]),
        "active_process_capture": dict(envelope["source_receipts"]["active_process_capture"]),
        "lifecycle_orico_admission": lifecycle_binding,
        "lifecycle_admission_observed_payload_sha256": _canonical_sha256(lifecycle),
        "lifecycle_cross_binding": {
            "baseline_epoch_id": lifecycle_binding["baseline_epoch_id"],
            "rejected_predecessor_epoch_id": REJECTED_PREDECESSOR_EPOCH_ID,
            "superseded_v4_epoch_id": SUPERSEDED_V4_EPOCH_ID,
            "new_epoch_differs_from_rejected_predecessor": True,
            "new_epoch_differs_from_superseded_v4": True,
            "config_sha256": lifecycle_binding["config_sha256"],
            "external_venues_disabled_active_config_sha256": (
                transport.FROZEN_FINAL_ACTIVE_CONFIG_SHA256
            ),
            "config_correction": dict(envelope["config_correction"]),
            "runtime_code_sha256": lifecycle_binding["runtime_code_sha256"],
            "active_runtime_source_files": dict(envelope["active_runtime"]["runtime_source_files"]),
            "admitted_runtime_code_files": dict(lifecycle_binding["runtime_code_files"]),
            "all_active_runtime_files_match_admission": True,
            "failed_predecessor_reused": False,
        },
        "gate_results": {
            "attempt4_mechanics_history": "passed_historical_only",
            "exact_v5_mechanics_history": "passed_historical_only",
            "direct_v3_regressions_history": "passed_historical_only",
            "direct_v4_resource_gate": "passed",
            "direct_v4_active_capture": "passed",
            "direct_v4_cross_host_admission": "passed",
            "direct_v4_lifecycle_orico_admission": "passed",
            "external_venues_disabled_config_correction": "passed",
            "superseded_v4_proof_history_only": "passed",
            "rejected_predecessor_not_reused": "passed",
        },
        "formal_research_state": dict(FORMAL_RESEARCH_STATE),
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_NEW_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[COMPLETION_CANONICAL_FIELD] = _document_sha256(payload, COMPLETION_CANONICAL_FIELD)
    return payload


def validate_completion(path: Path, **roots: Any) -> dict[str, Any]:
    payload, _binding = _read_own_receipt(
        path,
        label="direct-v4 operational completion",
        canonical_field=COMPLETION_CANONICAL_FIELD,
        schema=COMPLETION_SCHEMA,
        status=COMPLETION_STATUS,
    )
    expected = build_completion(
        activation_envelope_path=Path(str(payload.get("activation_envelope", {}).get("path", ""))),
        lifecycle_admission_path=Path(
            str(payload.get("lifecycle_orico_admission", {}).get("path", ""))
        ),
        generated_utc=str(payload.get("generated_utc", "")),
        **roots,
    )
    if payload != expected:
        raise FinalEvidenceV5Error("direct-v4 operational completion identity drifted")
    return payload


def finalize_completion(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_completion(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_completion,
        validator_kwargs=_validation_roots(kwargs),
    )


def _completion_context(path: Path, **roots: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = validate_completion(path, **roots)
    binding = _receipt_binding(
        path,
        label="direct-v4 operational completion",
        canonical_field=COMPLETION_CANONICAL_FIELD,
        schema=COMPLETION_SCHEMA,
        status=COMPLETION_STATUS,
    )
    return payload, binding


def build_composition(
    *,
    historical_operational_attempt_v10_path: Path,
    activation_envelope_path: Path,
    operational_completion_path: Path,
    lifecycle_fix_supplement_path: Path,
    rejected_predecessor_path: Path,
    historical_collector_v10_root: Path,
    historical_direct_v3_root: Path,
    attempt4_root: Path,
    final_v4_root: Path,
    final_v4_release: Path,
    superseded_v4_proof_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    roots = {
        "lifecycle_fix_supplement_path": lifecycle_fix_supplement_path,
        "rejected_predecessor_path": rejected_predecessor_path,
        "historical_collector_v10_root": historical_collector_v10_root,
        "historical_direct_v3_root": historical_direct_v3_root,
        "attempt4_root": attempt4_root,
        "final_v4_root": final_v4_root,
        "final_v4_release": final_v4_release,
        "superseded_v4_proof_path": superseded_v4_proof_path,
    }
    _attempt, attempt_binding, history = _historical_attempt_context(
        historical_operational_attempt_v10_path,
        historical_collector_v10_root=historical_collector_v10_root,
        historical_direct_v3_root=historical_direct_v3_root,
        attempt4_root=attempt4_root,
    )
    envelope, envelope_binding = _activation_context(activation_envelope_path, **roots)
    completion, completion_binding = _completion_context(operational_completion_path, **roots)
    if (
        envelope["historical_operational_attempt_v10"]["canonical_sha256"]
        != attempt_binding["canonical_sha256"]
        or completion["activation_envelope"]["canonical_sha256"]
        != envelope_binding["canonical_sha256"]
        or completion["historical_evidence"] != history
        or completion["superseded_v4_proof"] != envelope["superseded_v4_proof"]
        or completion["config_correction"] != envelope["config_correction"]
    ):
        raise FinalEvidenceV5Error("composition inputs are not one evidence chain")
    evidence = {
        "historical_operational_attempt_v10": attempt_binding,
        "historical_attempt4_mechanics": dict(history["attempt4_mechanics_anchor"]),
        "historical_exact_v5_mechanics": dict(history["exact_v5_mechanics_recovery"]),
        "historical_direct_v3_regressions": dict(history["direct_v3_regressions"]),
        "superseded_v4_proof": dict(envelope["superseded_v4_proof"]),
        "external_venues_disabled_config_correction": dict(envelope["config_correction"]),
        "lifecycle_fix_supplement": dict(envelope["lifecycle_fix_supplement"]),
        "rejected_predecessor_epoch": dict(envelope["rejected_predecessor_epoch"]["evidence"]),
        "final_v5_cross_host_admission": dict(envelope["cross_host_admission"]),
        "final_v5_resource_gate": dict(completion["current_host_resource"]),
        "final_v5_active_capture": dict(completion["active_process_capture"]),
        "final_v5_activation_envelope": envelope_binding,
        "final_v5_lifecycle_admission": dict(completion["lifecycle_orico_admission"]),
        "final_v5_operational_completion": completion_binding,
    }
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "direct-v4 final composition timestamp")
    payload = {
        "schema_version": COMPOSITION_SCHEMA,
        "identity": OWNER,
        "attempt_id": history["attempt_id"],
        "status": COMPOSITION_STATUS,
        "generated_utc": timestamp,
        "runtime_execution": dict(completion["runtime_execution"]),
        "runtime_authority": dict(completion["runtime_authority"]),
        "exact_artifact": dict(completion["exact_artifact"]),
        "ordered_evidence_roles": list(evidence),
        "evidence": evidence,
        "composition_root_sha256": _canonical_sha256(evidence),
        "composition_truth": {
            "historical_attempt4_resource_or_activation_invented": False,
            "historical_direct_v3_used_as_final_authority": False,
            "final_direct_v4_authority_from_portable_admission_only": True,
            "superseded_v4_proof_used_as_current_authority": False,
            "superseded_v4_proof_role": (
                "superseded_historical_due_external_venue_shadow_enabled"
            ),
            "external_venues_disabled_config_correction_bound": True,
            "rejected_predecessor_admitted": False,
            "rejected_predecessor_reused": False,
            "proof_release_will_replace_direct_v4_authority": False,
            "post_release_evidence_additive": True,
        },
        "formal_research_state": dict(FORMAL_RESEARCH_STATE),
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_NEW_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[COMPOSITION_CANONICAL_FIELD] = _document_sha256(payload, COMPOSITION_CANONICAL_FIELD)
    return payload


def validate_composition(path: Path, **roots: Any) -> dict[str, Any]:
    payload, _binding = _read_own_receipt(
        path,
        label="direct-v4 final composition",
        canonical_field=COMPOSITION_CANONICAL_FIELD,
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        raise FinalEvidenceV5Error("direct-v4 composition evidence is missing")
    expected = build_composition(
        historical_operational_attempt_v10_path=Path(
            str(evidence.get("historical_operational_attempt_v10", {}).get("path", ""))
        ),
        activation_envelope_path=Path(
            str(evidence.get("final_v5_activation_envelope", {}).get("path", ""))
        ),
        operational_completion_path=Path(
            str(evidence.get("final_v5_operational_completion", {}).get("path", ""))
        ),
        generated_utc=str(payload.get("generated_utc", "")),
        **roots,
    )
    if payload != expected:
        raise FinalEvidenceV5Error("direct-v4 final composition identity drifted")
    return payload


def finalize_composition(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_composition(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_composition,
        validator_kwargs=_validation_roots(kwargs),
    )


def build_attempt_final(
    *,
    final_composition_path: Path,
    lifecycle_fix_supplement_path: Path,
    rejected_predecessor_path: Path,
    historical_collector_v10_root: Path,
    historical_direct_v3_root: Path,
    attempt4_root: Path,
    final_v4_root: Path,
    final_v4_release: Path,
    superseded_v4_proof_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    roots = {
        "lifecycle_fix_supplement_path": lifecycle_fix_supplement_path,
        "rejected_predecessor_path": rejected_predecessor_path,
        "historical_collector_v10_root": historical_collector_v10_root,
        "historical_direct_v3_root": historical_direct_v3_root,
        "attempt4_root": attempt4_root,
        "final_v4_root": final_v4_root,
        "final_v4_release": final_v4_release,
        "superseded_v4_proof_path": superseded_v4_proof_path,
    }
    composition = validate_composition(final_composition_path, **roots)
    composition_binding = _receipt_binding(
        final_composition_path,
        label="direct-v4 final composition",
        canonical_field=COMPOSITION_CANONICAL_FIELD,
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "direct-v4 attempt-final timestamp")
    payload = {
        "schema_version": ATTEMPT_FINAL_SCHEMA,
        "identity": OWNER,
        "attempt_id": composition["attempt_id"],
        "status": ATTEMPT_FINAL_STATUS,
        "generated_utc": timestamp,
        "runtime_execution": dict(composition["runtime_execution"]),
        "runtime_authority": dict(composition["runtime_authority"]),
        "exact_artifact": dict(composition["exact_artifact"]),
        "final_composition": composition_binding,
        "composition_root_sha256": composition["composition_root_sha256"],
        "lifecycle_fix_supplement": dict(composition["evidence"]["lifecycle_fix_supplement"]),
        "config_correction": dict(
            composition["evidence"]["external_venues_disabled_config_correction"]
        ),
        "superseded_v4_proof": dict(composition["evidence"]["superseded_v4_proof"]),
        "rejected_predecessor_epoch": {
            "epoch_id": REJECTED_PREDECESSOR_EPOCH_ID,
            "evidence": dict(composition["evidence"]["rejected_predecessor_epoch"]),
            "rejected": True,
            "admitted": False,
            "reused": False,
        },
        "result": {
            "operational_evidence_complete": True,
            "cross_host_evidence_admitted": True,
            "lifecycle_evidence_admitted": True,
            "direct_v4_runtime_authority_unchanged": True,
            "historical_v3_is_history_only": True,
            "superseded_v4_proof_is_history_only": True,
            "external_venues_disabled_config_correction_bound": True,
            "research_supported": False,
            "owner_risk_accepted": True,
            "new_authority_granted": False,
        },
        "formal_research_state": dict(FORMAL_RESEARCH_STATE),
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_NEW_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[ATTEMPT_FINAL_CANONICAL_FIELD] = _document_sha256(
        payload, ATTEMPT_FINAL_CANONICAL_FIELD
    )
    return payload


def validate_attempt_final(path: Path, **roots: Any) -> dict[str, Any]:
    payload, _binding = _read_own_receipt(
        path,
        label="direct-v4 attempt-final",
        canonical_field=ATTEMPT_FINAL_CANONICAL_FIELD,
        schema=ATTEMPT_FINAL_SCHEMA,
        status=ATTEMPT_FINAL_STATUS,
    )
    expected = build_attempt_final(
        final_composition_path=Path(str(payload.get("final_composition", {}).get("path", ""))),
        generated_utc=str(payload.get("generated_utc", "")),
        **roots,
    )
    if payload != expected:
        raise FinalEvidenceV5Error("direct-v4 attempt-final identity drifted")
    return payload


def finalize_attempt_final(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_attempt_final(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_attempt_final,
        validator_kwargs=_validation_roots(kwargs),
    )


def _shallow_attempt_final_context(
    path: Path,
    *,
    release_binding: Mapping[str, Any],
    execution: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the final layer without reopening historical direct-v3 helpers."""

    payload, binding = _read_own_receipt(
        path,
        label="direct-v4 attempt-final",
        canonical_field=ATTEMPT_FINAL_CANONICAL_FIELD,
        schema=ATTEMPT_FINAL_SCHEMA,
        status=ATTEMPT_FINAL_STATUS,
    )
    fields = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "runtime_execution",
        "runtime_authority",
        "exact_artifact",
        "final_composition",
        "composition_root_sha256",
        "lifecycle_fix_supplement",
        "config_correction",
        "superseded_v4_proof",
        "rejected_predecessor_epoch",
        "result",
        "formal_research_state",
        "authority_design",
        "permissions",
        "evidence_boundary",
        ATTEMPT_FINAL_CANONICAL_FIELD,
    }
    expected_authority = {
        **dict(release_binding),
        "execution": dict(execution),
        "runtime_authority": True,
    }
    predecessor = payload.get("rejected_predecessor_epoch")
    result = payload.get("result")
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or payload.get("runtime_execution") != execution
        or payload.get("runtime_authority") != expected_authority
        or payload.get("exact_artifact") != artifact
        or _exact_content(
            payload.get("lifecycle_fix_supplement"),
            LIFECYCLE_FIX_SUPPLEMENT_CONTENT,
            "attempt-final lifecycle supplement",
        )
        != LIFECYCLE_FIX_SUPPLEMENT_CONTENT
        or _validate_config_correction_content(payload.get("config_correction"))
        != payload.get("config_correction")
        or _validate_superseded_v4_historical(payload.get("superseded_v4_proof"))
        != payload.get("superseded_v4_proof")
        or not isinstance(predecessor, Mapping)
        or predecessor.get("epoch_id") != REJECTED_PREDECESSOR_EPOCH_ID
        or _exact_content(
            predecessor.get("evidence"),
            REJECTED_PREDECESSOR_CONTENT,
            "attempt-final rejected predecessor",
        )
        != REJECTED_PREDECESSOR_CONTENT
        or predecessor.get("rejected") is not True
        or predecessor.get("admitted") is not False
        or predecessor.get("reused") is not False
        or not isinstance(result, Mapping)
        or result.get("operational_evidence_complete") is not True
        or result.get("direct_v4_runtime_authority_unchanged") is not True
        or result.get("historical_v3_is_history_only") is not True
        or result.get("superseded_v4_proof_is_history_only") is not True
        or result.get("external_venues_disabled_config_correction_bound") is not True
        or result.get("research_supported") is not False
        or result.get("owner_risk_accepted") is not True
        or result.get("new_authority_granted") is not False
        or payload.get("formal_research_state") != FORMAL_RESEARCH_STATE
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_NEW_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise FinalEvidenceV5Error("direct-v4 attempt-final shallow authority binding drifted")
    _require_sha256(payload.get("composition_root_sha256"), "composition root")
    composition_ref = payload.get("final_composition")
    if not isinstance(composition_ref, Mapping):
        raise FinalEvidenceV5Error("attempt-final composition reference is missing")
    composition_path = Path(str(composition_ref.get("path", "")))
    composition, composition_binding = _read_own_receipt(
        composition_path,
        label="direct-v4 final composition",
        canonical_field=COMPOSITION_CANONICAL_FIELD,
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    if (
        composition_ref != composition_binding
        or composition.get("runtime_execution") != execution
        or composition.get("runtime_authority") != expected_authority
        or composition.get("exact_artifact") != artifact
        or composition.get("composition_root_sha256") != payload["composition_root_sha256"]
        or composition.get("evidence", {}).get("external_venues_disabled_config_correction")
        != payload.get("config_correction")
        or composition.get("evidence", {}).get("superseded_v4_proof")
        != payload.get("superseded_v4_proof")
        or composition.get("authority_design") != AUTHORITY_DESIGN
    ):
        raise FinalEvidenceV5Error("attempt-final composition authority binding drifted")
    return payload, binding


def build_evidence_release(
    *,
    attempt_final_path: Path,
    final_v4_root: Path,
    final_v4_release: Path,
    generated_utc: str | None = None,
    **_historical_roots: Any,
) -> dict[str, Any]:
    # This fresh call is deliberate: the proof receipt revalidates the actual
    # source-frozen direct-v4 release and never calls base direct-v3 helpers.
    release, release_binding, execution, artifact = _final_authority_context(
        final_v4_root, final_v4_release
    )
    attempt_final, final_binding = _shallow_attempt_final_context(
        attempt_final_path,
        release_binding=release_binding,
        execution=execution,
        artifact=artifact,
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "direct-v4 proof evidence-release timestamp")
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
        "action_authorized": release["action_authorized"],
        "live_authorized": release["live_authorized"],
        "authority_provenance": {
            "source": "transport_source_frozen_direct_v4_owner_release_v2",
            "current_runtime_evidence_source": (
                "external_venues_disabled_cross_host_admission_v2_only"
            ),
            "historical_operational_attempt_v10_used_as_authority": False,
            "historical_direct_v3_used_as_authority": False,
            "superseded_v4_proof_used_as_authority": False,
            "new_authority_granted": False,
            "direct_v4_release_file_sha256": release_binding["file_sha256"],
            "direct_v4_release_canonical_sha256": release_binding["canonical_sha256"],
            "proof_release_replaces_direct_v4_runtime_authority": False,
        },
        "runtime_execution": dict(execution),
        "runtime_authority": {
            **dict(release_binding),
            "execution": dict(execution),
            "runtime_authority": True,
        },
        "exact_artifact": dict(artifact),
        "scope": dict(release["scope"]),
        "rollback": dict(release["rollback"]),
        "operational_attempt_final": final_binding,
        "composition_root_sha256": attempt_final["composition_root_sha256"],
        "lifecycle_fix_supplement": dict(attempt_final["lifecycle_fix_supplement"]),
        "config_correction": dict(attempt_final["config_correction"]),
        "superseded_v4_proof": dict(attempt_final["superseded_v4_proof"]),
        "rejected_predecessor_epoch": dict(attempt_final["rejected_predecessor_epoch"]),
        "evidence_state": {
            "post_release_evidence_complete": True,
            "cross_host_evidence_admitted": True,
            "lifecycle_evidence_admitted": True,
            "failed_predecessor_rejected_not_admitted": True,
            "failed_predecessor_reused": False,
            "historical_v3_is_history_only": True,
            "superseded_v4_proof_is_history_only": True,
            "superseded_v4_epoch_id": SUPERSEDED_V4_EPOCH_ID,
            "superseded_v4_proof_role": (
                "superseded_historical_due_external_venue_shadow_enabled"
            ),
            "external_venues_disabled_config_correction_bound": True,
            "external_venues_disabled_active_config_exact": True,
            "exact_artifact_oof_available": False,
            "old_oof_applies_to_learning_algorithm_only": True,
            "runtime_authority_replaced": False,
            "runtime_consumed": True,
            "runtime_consumed_authority": "direct_v4_owner_release_v2",
            "does_not_replace_runtime_active_release": True,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[EVIDENCE_RELEASE_CANONICAL_FIELD] = _document_sha256(
        payload, EVIDENCE_RELEASE_CANONICAL_FIELD
    )
    return payload


def validate_evidence_release(
    path: Path,
    *,
    final_v4_root: Path,
    final_v4_release: Path,
    **historical_roots: Any,
) -> dict[str, Any]:
    payload, _binding = _read_own_receipt(
        path,
        label="direct-v4 proof evidence-release",
        canonical_field=EVIDENCE_RELEASE_CANONICAL_FIELD,
        schema=EVIDENCE_RELEASE_SCHEMA,
        status=EVIDENCE_RELEASE_STATUS,
    )
    expected = build_evidence_release(
        attempt_final_path=Path(str(payload.get("operational_attempt_final", {}).get("path", ""))),
        final_v4_root=final_v4_root,
        final_v4_release=final_v4_release,
        generated_utc=str(payload.get("generated_utc", "")),
        **historical_roots,
    )
    if payload != expected:
        raise FinalEvidenceV5Error("direct-v4 proof evidence-release identity drifted")
    return payload


def finalize_evidence_release(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_evidence_release(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_evidence_release,
        validator_kwargs=_validation_roots(kwargs),
    )


def _validation_roots(values: Mapping[str, Any]) -> dict[str, Any]:
    names = (
        "lifecycle_fix_supplement_path",
        "rejected_predecessor_path",
        "historical_collector_v10_root",
        "historical_direct_v3_root",
        "attempt4_root",
        "final_v4_root",
        "final_v4_release",
        "superseded_v4_proof_path",
    )
    return {name: values[name] for name in names if name in values}


def _roots(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--historical-collector-v10-root", type=Path, required=True)
    parser.add_argument("--historical-direct-v3-root", type=Path, required=True)
    parser.add_argument("--attempt4-root", type=Path, required=True)
    parser.add_argument("--final-v4-root", type=Path, required=True)
    parser.add_argument("--final-v4-release", type=Path, required=True)
    parser.add_argument("--superseded-v4-proof", type=Path, required=True)
    parser.add_argument("--lifecycle-fix-supplement", type=Path, required=True)
    parser.add_argument("--rejected-predecessor", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    envelope = commands.add_parser("activation-envelope")
    _roots(envelope)
    envelope.add_argument("--historical-operational-attempt-v10", type=Path, required=True)
    envelope.add_argument("--cross-host-admission", type=Path, required=True)
    envelope.add_argument("--output", type=Path, required=True)

    completion = commands.add_parser("completion")
    _roots(completion)
    completion.add_argument("--activation-envelope", type=Path, required=True)
    completion.add_argument("--lifecycle-admission", type=Path, required=True)
    completion.add_argument("--output", type=Path, required=True)

    composition = commands.add_parser("composition")
    _roots(composition)
    composition.add_argument("--historical-operational-attempt-v10", type=Path, required=True)
    composition.add_argument("--activation-envelope", type=Path, required=True)
    composition.add_argument("--operational-completion", type=Path, required=True)
    composition.add_argument("--output", type=Path, required=True)

    attempt_final = commands.add_parser("attempt-final")
    _roots(attempt_final)
    attempt_final.add_argument("--final-composition", type=Path, required=True)
    attempt_final.add_argument("--output", type=Path, required=True)

    evidence_release = commands.add_parser("evidence-release")
    _roots(evidence_release)
    evidence_release.add_argument("--attempt-final", type=Path, required=True)
    evidence_release.add_argument("--output", type=Path, required=True)

    validate = commands.add_parser("validate")
    _roots(validate)
    validate.add_argument(
        "--kind",
        choices=(
            "activation-envelope",
            "completion",
            "composition",
            "attempt-final",
            "evidence-release",
        ),
        required=True,
    )
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def _root_args(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "historical_collector_v10_root": args.historical_collector_v10_root,
        "historical_direct_v3_root": args.historical_direct_v3_root,
        "attempt4_root": args.attempt4_root,
        "final_v4_root": args.final_v4_root,
        "final_v4_release": args.final_v4_release,
        "superseded_v4_proof_path": args.superseded_v4_proof,
        "lifecycle_fix_supplement_path": args.lifecycle_fix_supplement,
        "rejected_predecessor_path": args.rejected_predecessor,
    }


def _print_result(payload: Mapping[str, Any], file_sha: str | None) -> None:
    canonical = next(
        value
        for key, value in payload.items()
        if key.startswith("canonical_") and key.endswith("sha256")
    )
    result = {
        "schema_version": payload["schema_version"],
        "status": payload["status"],
        "canonical_sha256": canonical,
    }
    if file_sha is not None:
        result["file_sha256"] = file_sha
    print(base.json.dumps(result, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    roots = _root_args(args)
    if args.command == "activation-envelope":
        payload, file_sha = finalize_activation_envelope(
            historical_operational_attempt_v10_path=args.historical_operational_attempt_v10,
            cross_host_admission_path=args.cross_host_admission,
            output_path=args.output,
            **roots,
        )
    elif args.command == "completion":
        payload, file_sha = finalize_completion(
            activation_envelope_path=args.activation_envelope,
            lifecycle_admission_path=args.lifecycle_admission,
            output_path=args.output,
            **roots,
        )
    elif args.command == "composition":
        payload, file_sha = finalize_composition(
            historical_operational_attempt_v10_path=args.historical_operational_attempt_v10,
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
    _print_result(payload, file_sha)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
