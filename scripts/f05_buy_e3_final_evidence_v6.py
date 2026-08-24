#!/usr/bin/env python3
"""Compose the fully no-shadow BUY E3 operational evidence, fail closed.

This is additive evidence plumbing only.  Runtime and live/action authority
remain the immutable direct owner release v3.  The five receipts produced by
this module revalidate that authority; they never replace it, create research
authority, or reinterpret rejected resource attempts as live epochs.

No economic outcome, Validation, or sealed-holdout input is accepted.  No
shadow, companion, hypothetical-action, or live collection path is created.
Every output is a create-only private JSON receipt.
"""

from __future__ import annotations

import argparse
import math
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v8 as resource_v8,
)
from scripts import f05_buy_e3_active_capture_v8 as active_capture_v8
from scripts import f05_buy_e3_cross_host_transport_v6 as transport_v6
from scripts import f05_buy_e3_evidence_completion as base
from scripts import f05_buy_e3_failed_activation_attempt_history as failed_history_v1
from scripts import f05_buy_e3_final_evidence_v4 as historical_final_v4
from scripts import f05_buy_e3_lifecycle_context_v1 as lifecycle_context_v1
from scripts import f05_buy_e3_post_lifecycle_live_health_v1 as post_lifecycle_v1

OWNER: Final = base.OWNER
ATTEMPT_ID: Final = "operational-attempt-no-shadow-release-v3-evidence-v6-20260824"

TRANSPORT_MODULE: Final = "scripts.f05_buy_e3_cross_host_transport_v6"
RESOURCE_MODULE: Final = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v8"
)
ACTIVE_CAPTURE_MODULE: Final = "scripts.f05_buy_e3_active_capture_v8"
FAILED_HISTORY_MODULE: Final = "scripts.f05_buy_e3_failed_activation_attempt_history"
POST_LIFECYCLE_MODULE: Final = "scripts.f05_buy_e3_post_lifecycle_live_health_v1"
LIFECYCLE_CONTEXT_MODULE: Final = "scripts.f05_buy_e3_lifecycle_context_v1"
ACTIVE_CAPTURE_SCHEMA_V7: Final = (
    f"{OWNER}.fresh_all_shadow_evaluators_disabled_active_process_capture.v7"
)
ACTIVE_CAPTURE_STATUS_V7: Final = "fresh_active_health_proven_all_shadow_evaluators_disabled"
ACTIVE_HEALTH_WINDOW_SCHEMA: Final = f"{OWNER}.fresh_active_main_health_window.v1"
ACTIVE_HEALTH_WINDOW_STATUS: Final = "two_consecutive_fresh_active_main_health_rows_verified"

CURRENT_EXECUTION_COMMIT: Final = "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de"
CURRENT_EXECUTION_TREE: Final = "0343bd5586b337385cf2aa0d7a643f5c32b0da77"
CURRENT_ANNOTATED_TAG: Final = "f05-owner-buy-e3-no-shadow-runtime-v3-20260824"
CURRENT_TAG_OBJECT: Final = "3878ea05252ef8f274b6f74ee7a984431c53b892"

ENVELOPE_SCHEMA: Final = f"{OWNER}.cross_host_activation_envelope.v6"
ENVELOPE_STATUS: Final = "release_v3_current_no_shadow_activation_evidence_admitted"
ENVELOPE_CANONICAL_FIELD: Final = "canonical_no_shadow_activation_envelope_sha256"

COMPLETION_SCHEMA: Final = f"{OWNER}.cross_host_operational_evidence_completion.v6"
COMPLETION_STATUS: Final = "release_v3_current_no_shadow_operational_evidence_complete"
COMPLETION_CANONICAL_FIELD: Final = "canonical_no_shadow_operational_completion_sha256"

COMPOSITION_SCHEMA: Final = f"{OWNER}.cross_host_final_composition_receipt.v6"
COMPOSITION_STATUS: Final = "release_v3_current_no_shadow_operational_evidence_composed"
COMPOSITION_CANONICAL_FIELD: Final = "canonical_no_shadow_final_composition_sha256"

ATTEMPT_FINAL_SCHEMA: Final = f"{OWNER}.cross_host_operational_attempt_final_receipt.v6"
ATTEMPT_FINAL_STATUS: Final = "release_v3_current_no_shadow_attempt_results_bound"
ATTEMPT_FINAL_CANONICAL_FIELD: Final = "canonical_no_shadow_attempt_final_sha256"

EVIDENCE_RELEASE_SCHEMA: Final = f"{OWNER}.cross_host_proof_evidence_release.v6"
EVIDENCE_RELEASE_STATUS: Final = "release_v3_current_no_shadow_evidence_complete"
EVIDENCE_RELEASE_CANONICAL_FIELD: Final = "canonical_no_shadow_evidence_release_sha256"

CONTENT_BINDING_FIELDS: Final = tuple(transport_v6.CONTENT_BINDING_FIELDS)
PORTABLE_SOURCE_ROLES: Final = tuple(transport_v6.SOURCE_FILENAMES)

FROZEN_CROSS_HOST_ADMISSION_PATH_PROVENANCE: Final = (
    "/Volumes/ORICO/MarketData/NarrowGate_BTCUSDC/reports/f05_owner_buy_e3_v1/"
    "direct_no_shadow_live_evidence_v6_20260824/cross_host_admission/"
    "cross_host_admission.json"
)
FROZEN_CROSS_HOST_ADMISSION_CONTENT: Final = {
    "schema_version": transport_v6.ADMISSION_SCHEMA,
    "status": transport_v6.ADMISSION_STATUS,
    "file_sha256": "78cff62bab68ead22fcc21ba40b4a69d96c9f3d452db4f1a6f7dc24bdaba00fd",
    "canonical_field": transport_v6.ADMISSION_CANONICAL_FIELD,
    "canonical_sha256": "24f9e2e7f92f29e35fc86692e53a1dd0e899ecd5b78d5e160e7ccb5a2bdfdb64",
    "size_bytes": 27_860,
    "mode": "0600",
}

FAILED_ACTIVATION_HISTORY_DURABLE_ROOT: Final = (
    "/Volumes/ORICO/MarketData/NarrowGate_BTCUSDC/reports/f05_owner_buy_e3_v1/"
    "direct_no_shadow_live_evidence_v6_20260824/failed_activation_attempt_history"
)
RESOURCE_ATTEMPT_REJECTION_HISTORY_PATH_PROVENANCE: Final = (
    f"{FAILED_ACTIVATION_HISTORY_DURABLE_ROOT}/failed_activation_attempt_history.json"
)
FAILED_ACTIVATION_SOURCE_PATH_PROVENANCE: Final = (
    f"{FAILED_ACTIVATION_HISTORY_DURABLE_ROOT}/failed_activation_source.json"
)
FAILED_V6_BENCHMARK_PATH_PROVENANCE: Final = (
    f"{FAILED_ACTIVATION_HISTORY_DURABLE_ROOT}/resource_v6_wrong_route_benchmark.json"
)
FAILED_V7_ATTEMPT2_BENCHMARK_PATH_PROVENANCE: Final = (
    f"{FAILED_ACTIVATION_HISTORY_DURABLE_ROOT}/resource_v7_attempt2_benchmark.json"
)
FAILED_ACTIVATION_ATTEMPT_HISTORY_SCHEMA: Final = failed_history_v1.SCHEMA_VERSION
FAILED_ACTIVATION_ATTEMPT_HISTORY_STATUS: Final = failed_history_v1.STATUS
RESOURCE_ATTEMPT_REJECTION_HISTORY_CONTENT: Final = {
    "schema_version": failed_history_v1.SCHEMA_VERSION,
    "status": failed_history_v1.STATUS,
    "file_sha256": "165c02da26cb1f32ff6ef8549620dc1c54d1c5082a50ebaabeeee4c2622f73d0",
    "canonical_field": failed_history_v1.CANONICAL_FIELD,
    "canonical_sha256": "914b6b3018946fe145e67dd9a0bdd1bbbe38b75b4da53e26521d55fedc7cde5a",
    "size_bytes": 6_413,
    "mode": "0600",
}

FROZEN_CURRENT_LIFECYCLE_PATH_PROVENANCE: Final = (
    "/Volumes/ORICO/MarketData/NarrowGate_BTCUSDC/formal_collection/"
    "prospective_lifecycle_journal_v2/"
    "session-prospective-1787568574639266387-ac669869e7ed/admission_manifest.json"
)
FROZEN_CURRENT_LIFECYCLE_EPOCH_ID: Final = "prospective-1787568574639266387-ac669869e7ed"
FROZEN_CURRENT_LIFECYCLE_CONTENT: Final = {
    "schema_version": base.LIFECYCLE_SCHEMA,
    "status": None,
    "file_sha256": "8b2c08b49bb2f4c272b958b3f3ed3e7f47c914577267fec45c48fe6052a17aaf",
    "canonical_field": "admission_identity_sha256",
    "canonical_sha256": "50afb8064a43a81a92388766b5b4c0e31ae8d768e017da11d7ccdcf12507878d",
    "size_bytes": 2_469,
    "mode": "0644",
}
FROZEN_LIFECYCLE_CONTEXT_PATH_PROVENANCE: Final = ""
FROZEN_LIFECYCLE_CONTEXT_CONTENT: Final = {
    "schema_version": lifecycle_context_v1.SCHEMA_VERSION,
    "status": lifecycle_context_v1.STATUS,
    "file_sha256": "",
    "canonical_field": lifecycle_context_v1.CANONICAL_FIELD,
    "canonical_sha256": "",
    "size_bytes": 0,
    "mode": "0600",
}

SUPERSEDED_V4_EPOCH_ID: Final = "prospective-1787542261153620067-48e29ea0b22c"
SUPERSEDED_V4_ACTIVE_CONFIG_SHA256: Final = (
    "2f61532126cbe633424476cb093c6c978bab1f935f69a30e06677d677008cae6"
)
SUPERSEDED_V4_PROOF_CONTENT: Final = {
    "schema_version": historical_final_v4.EVIDENCE_RELEASE_SCHEMA,
    "status": historical_final_v4.EVIDENCE_RELEASE_STATUS,
    "file_sha256": "0f85849289cb9e42de7333117c7719e1a95a1561cf42e8a00366e0e8500df28f",
    "canonical_field": historical_final_v4.EVIDENCE_RELEASE_CANONICAL_FIELD,
    "canonical_sha256": "21ca796d12c0df733e3c1daba2fe8e326979ec0bb80b8b653275e14a6af97880",
    "size_bytes": 8031,
    "mode": "0600",
}

EXPECTED_RESOURCE_RUNTIME_SOURCE_SHA256: Final = {
    str(binding["path"]): str(binding["sha256"])
    for binding in resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.values()
}
EXPECTED_RESOURCE_RUNTIME_SOURCE_MANIFEST_SHA256: Final = (
    "a486ed94a60b144aec88e23c5bc01e045356ca1143e28a94090746c38a0476f0"
)
EXPECTED_ACTIVE_RUNTIME_SOURCE_SHA256: Final = {
    str(resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[role]["path"]): str(
        resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[role]["sha256"]
    )
    for role in active_capture_v8.STARTUP_SOURCE_ROLE_MAP.values()
}
EXPECTED_ACTIVE_RUNTIME_SOURCE_MANIFEST_SHA256: Final = (
    "8fd9babfd5e596b29a662a94ddee36c60d2b1de36504978b913e9d73d1fd3b84"
)
EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256: Final = {
    **lifecycle_context_v1.EXPECTED_RUNTIME_SOURCE_SHA256
}

POST_LIFECYCLE_RECEIPT_SCHEMA: Final = post_lifecycle_v1.SCHEMA_VERSION
POST_LIFECYCLE_RECEIPT_STATUS: Final = post_lifecycle_v1.STATUS
POST_LIFECYCLE_RECEIPT_CANONICAL_FIELD: Final = post_lifecycle_v1.CANONICAL_FIELD
POST_LIFECYCLE_HEALTH_SCHEMA: Final = post_lifecycle_v1.PORTABLE_SCHEMA_VERSION
POST_LIFECYCLE_HEALTH_STATUS: Final = post_lifecycle_v1.PORTABLE_STATUS
POST_LIFECYCLE_HEALTH_CANONICAL_FIELD: Final = post_lifecycle_v1.PORTABLE_CANONICAL_FIELD
FROZEN_POST_LIFECYCLE_HEALTH_PATH_PROVENANCE: Final = ""
FROZEN_POST_LIFECYCLE_HEALTH_CONTENT: Final = {
    "schema_version": POST_LIFECYCLE_RECEIPT_SCHEMA,
    "status": POST_LIFECYCLE_RECEIPT_STATUS,
    "file_sha256": "",
    "canonical_field": POST_LIFECYCLE_RECEIPT_CANONICAL_FIELD,
    "canonical_sha256": "",
    "size_bytes": 0,
    "mode": "0600",
}

NO_NEW_AUTHORITY: Final = {"research": False, "action": False, "live": False}
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
FORMAL_RESEARCH_STATE: Final = {
    "research_supported": False,
    "formal_hierarchy_passed": False,
    "formal_hard_gates_passed": False,
    "owner_risk_accepted": True,
    "old_oof_applies_to_learning_algorithm_only": True,
    "exact_artifact_oof_available": False,
}
AUTHORITY_DESIGN: Final = {
    "runtime_authority": "direct_owner_release_v3",
    "runtime_authority_source": "transport_v6_validated_current_admission",
    "proof_release_replaces_runtime_authority": False,
    "runtime_authority_replaced": False,
    "runtime_consumed": True,
    "runtime_consumed_authority": "direct_owner_release_v3",
    "does_not_replace_runtime_active_release": True,
    "retrospective_authority_created": False,
    "evidence_is_additive_only": True,
    "superseded_v4_proof_is_authority": False,
    "rejected_resource_attempts_are_authority": False,
}

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")


class FinalEvidenceV6Error(RuntimeError):
    """Raised when the current fully no-shadow chain cannot be proven."""


def _timestamp(value: Any, label: str) -> str:
    try:
        return base._timestamp(value, label)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV6Error(f"{label} is invalid") from exc


def _now() -> str:
    return base._now()  # noqa: SLF001


def _canonical_sha256(value: Any) -> str:
    return base._canonical_sha256(value)  # noqa: SLF001


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    return base._document_sha256(payload, field)  # noqa: SLF001


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise FinalEvidenceV6Error(f"{label} is not a lowercase SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value)
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise FinalEvidenceV6Error(f"{label} is not a lowercase git SHA")
    return normalized


def _content_projection(
    value: Any,
    label: str,
    *,
    allowed_modes: frozenset[str] = frozenset({"0600"}),
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(CONTENT_BINDING_FIELDS):
        raise FinalEvidenceV6Error(f"{label} exact7 fields drifted")
    projected = {field: value.get(field) for field in CONTENT_BINDING_FIELDS}
    _require_sha256(projected["file_sha256"], f"{label} file SHA256")
    _require_sha256(projected["canonical_sha256"], f"{label} canonical SHA256")
    canonical_field = projected["canonical_field"]
    if (
        not isinstance(projected["schema_version"], str)
        or not projected["schema_version"]
        or not isinstance(projected["status"], (str, type(None)))
        or not isinstance(canonical_field, str)
        or not canonical_field
        or not (
            canonical_field == "artifact_sha256"
            or canonical_field == "admission_identity_sha256"
            or (canonical_field.startswith("canonical_") and canonical_field.endswith("sha256"))
        )
        or not isinstance(projected["size_bytes"], int)
        or isinstance(projected["size_bytes"], bool)
        or projected["size_bytes"] <= 0
        or projected["mode"] not in allowed_modes
    ):
        raise FinalEvidenceV6Error(f"{label} exact7 identity is malformed")
    return projected


def _frozen_content(
    value: Any,
    label: str,
    *,
    allowed_modes: frozenset[str] = frozenset({"0600"}),
) -> dict[str, Any]:
    """Require that a source scaffold has been replaced by an actual exact7."""

    return _content_projection(value, f"frozen {label}", allowed_modes=allowed_modes)


def _exact_content(
    value: Any,
    expected: Any,
    label: str,
    *,
    allowed_modes: frozenset[str] = frozenset({"0600"}),
) -> dict[str, Any]:
    frozen = _frozen_content(expected, label, allowed_modes=allowed_modes)
    observed = _content_projection(value, label, allowed_modes=allowed_modes)
    if observed != frozen:
        raise FinalEvidenceV6Error(f"{label} exact7 identity drifted")
    return observed


def _frozen_path(value: str, label: str) -> Path:
    candidate = PurePosixPath(value)
    if not value or not candidate.is_absolute():
        raise FinalEvidenceV6Error(f"frozen {label} path is not absolute")
    return Path(value)


def _require_exact_path(path: Path, frozen: str, label: str) -> Path:
    expected = _frozen_path(frozen, label)
    observed = path.expanduser().absolute()
    if observed != expected:
        raise FinalEvidenceV6Error(f"{label} path differs from frozen provenance")
    return observed


def _receipt_binding(
    path: Path,
    *,
    label: str,
    canonical_field: str,
    schema: str,
    status: str | None,
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
        raise FinalEvidenceV6Error(f"{label} binding is invalid") from exc


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
        raise FinalEvidenceV6Error(f"{label} is invalid") from exc
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
        raise FinalEvidenceV6Error(f"receipt creation failed: {output_path}") from exc
    if observed != payload:
        raise FinalEvidenceV6Error("written receipt differs after validation")
    return payload, file_sha


def _module_contract() -> dict[str, str]:
    if (
        transport_v6.__name__ != TRANSPORT_MODULE
        or resource_v8.__name__ != RESOURCE_MODULE
        or active_capture_v8.__name__ != ACTIVE_CAPTURE_MODULE
        or failed_history_v1.__name__ != FAILED_HISTORY_MODULE
        or lifecycle_context_v1.__name__ != LIFECYCLE_CONTEXT_MODULE
        or post_lifecycle_v1.__name__ != POST_LIFECYCLE_MODULE
        or getattr(transport_v6, "resource_v8", None) is not resource_v8
        or getattr(transport_v6, "active_capture_v8", None) is not active_capture_v8
        or getattr(post_lifecycle_v1, "resource_v8", None) is not resource_v8
        or getattr(post_lifecycle_v1, "active_capture_v8", None) is not active_capture_v8
        or getattr(post_lifecycle_v1, "lifecycle_context_v1", None) is not lifecycle_context_v1
    ):
        raise FinalEvidenceV6Error("final evidence imported the wrong module route")
    if (
        resource_v8.RESOURCE_SCHEMA != f"{OWNER}.current_host_concurrent_resource_gate.v8"
        or transport_v6.FROZEN_FINAL_RESOURCE_SCHEMA != resource_v8.RESOURCE_SCHEMA
        or transport_v6.FROZEN_FINAL_RESOURCE_STATUS != resource_v8.RESOURCE_STATUS
    ):
        raise FinalEvidenceV6Error("final evidence is not bound to resource-v8")
    if (
        active_capture_v8.SCHEMA_VERSION != ACTIVE_CAPTURE_SCHEMA_V7
        or active_capture_v8.STATUS != ACTIVE_CAPTURE_STATUS_V7
        or transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA != ACTIVE_CAPTURE_SCHEMA_V7
        or transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS != ACTIVE_CAPTURE_STATUS_V7
    ):
        raise FinalEvidenceV6Error("final evidence is not bound to active schema-v7 via module-v8")
    if (
        failed_history_v1.SCHEMA_VERSION != FAILED_ACTIVATION_ATTEMPT_HISTORY_SCHEMA
        or failed_history_v1.STATUS != FAILED_ACTIVATION_ATTEMPT_HISTORY_STATUS
        or failed_history_v1.CANONICAL_FIELD
        != RESOURCE_ATTEMPT_REJECTION_HISTORY_CONTENT["canonical_field"]
        or post_lifecycle_v1.SCHEMA_VERSION != POST_LIFECYCLE_RECEIPT_SCHEMA
        or post_lifecycle_v1.STATUS != POST_LIFECYCLE_RECEIPT_STATUS
        or post_lifecycle_v1.CANONICAL_FIELD != POST_LIFECYCLE_RECEIPT_CANONICAL_FIELD
        or post_lifecycle_v1.PORTABLE_SCHEMA_VERSION != POST_LIFECYCLE_HEALTH_SCHEMA
        or post_lifecycle_v1.PORTABLE_STATUS != POST_LIFECYCLE_HEALTH_STATUS
        or post_lifecycle_v1.PORTABLE_CANONICAL_FIELD != POST_LIFECYCLE_HEALTH_CANONICAL_FIELD
        or lifecycle_context_v1.SCHEMA_VERSION != FROZEN_LIFECYCLE_CONTEXT_CONTENT["schema_version"]
        or lifecycle_context_v1.STATUS != FROZEN_LIFECYCLE_CONTEXT_CONTENT["status"]
        or lifecycle_context_v1.CANONICAL_FIELD
        != FROZEN_LIFECYCLE_CONTEXT_CONTENT["canonical_field"]
    ):
        raise FinalEvidenceV6Error("final evidence successor producer contracts drifted")
    expected_execution = {
        "execution_commit": CURRENT_EXECUTION_COMMIT,
        "execution_tree": CURRENT_EXECUTION_TREE,
        "annotated_operational_tag": CURRENT_ANNOTATED_TAG,
        "annotated_operational_tag_object": CURRENT_TAG_OBJECT,
        "tag_peeled_commit": CURRENT_EXECUTION_COMMIT,
    }
    for value, label in (
        (CURRENT_EXECUTION_COMMIT, "current execution commit"),
        (CURRENT_EXECUTION_TREE, "current execution tree"),
        (CURRENT_TAG_OBJECT, "current tag object"),
    ):
        _require_git_sha(value, label)
    if (
        transport_v6.FROZEN_FINAL_EXECUTION_COMMIT != CURRENT_EXECUTION_COMMIT
        or transport_v6.FROZEN_FINAL_EXECUTION_TREE != CURRENT_EXECUTION_TREE
        or transport_v6.FROZEN_FINAL_ANNOTATED_TAG != CURRENT_ANNOTATED_TAG
        or transport_v6.FROZEN_FINAL_TAG_OBJECT != CURRENT_TAG_OBJECT
        or transport_v6._frozen_final_execution() != expected_execution  # noqa: SLF001
        or post_lifecycle_v1.RUNTIME_EXECUTION != expected_execution
        or post_lifecycle_v1.EXPECTED_ALL_RUNTIME_SOURCE_SHA256
        != EXPECTED_RESOURCE_RUNTIME_SOURCE_SHA256
        or post_lifecycle_v1.EXPECTED_STARTUP_SOURCE_SHA256 != EXPECTED_ACTIVE_RUNTIME_SOURCE_SHA256
        or post_lifecycle_v1.EXPECTED_LIFECYCLE_SOURCE_SHA256
        != EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256
        or lifecycle_context_v1.RUNTIME_EXECUTION != expected_execution
        or lifecycle_context_v1.EXPECTED_RUNTIME_SOURCE_SHA256
        != EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256
        or lifecycle_context_v1.RUNTIME_SOURCE_FILE_COUNT != 65
        or lifecycle_context_v1.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
        != "ffb3b0a50189b13010b05511ae1b11fe0a785b4f93bedccae620bac85759b20d"
        or lifecycle_context_v1.RUNTIME_CODE_SHA256
        != "aaf1dd51ce43db4ec2239901198e3ed6333ca4736bb11395d84f5baa64416b74"
    ):
        raise FinalEvidenceV6Error("transport-v6 runtime execution is not eacb release-v3")
    if (
        transport_v6.FROZEN_FINAL_RELEASE_SCHEMA != resource_v8.DIRECT_SUCCESSOR_RELEASE_SCHEMA
        or transport_v6.FROZEN_FINAL_RELEASE_STATUS != resource_v8.DIRECT_SUCCESSOR_RELEASE_STATUS
        or transport_v6.FROZEN_FINAL_RELEASE_FILE_SHA256
        != resource_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or transport_v6.FROZEN_FINAL_RELEASE_CANONICAL_SHA256
        != resource_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
    ):
        raise FinalEvidenceV6Error("transport-v6 runtime authority is not release-v3")
    for value, label in (
        (transport_v6.FROZEN_FINAL_RELEASE_FILE_SHA256, "release-v3 file"),
        (transport_v6.FROZEN_FINAL_RELEASE_CANONICAL_SHA256, "release-v3 canonical"),
        (transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256, "config correction file"),
        (
            transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256,
            "config correction canonical",
        ),
        (transport_v6.FROZEN_FINAL_RESOURCE_FILE_SHA256, "resource-v8 file"),
        (transport_v6.FROZEN_FINAL_RESOURCE_CANONICAL_SHA256, "resource-v8 canonical"),
        (transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256, "active-v7 file"),
        (transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256, "active-v7 canonical"),
    ):
        _require_sha256(value, label)
    return expected_execution


def _artifact_projection(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FinalEvidenceV6Error(f"{label} artifact is missing")
    roles = value.get("roles")
    if not isinstance(roles, Mapping) or set(roles) != {"manifest", "policy", "predicate_bundle"}:
        raise FinalEvidenceV6Error(f"{label} artifact roles drifted")
    return {
        "artifact_sha256": _require_sha256(value.get("artifact_sha256"), f"{label} artifact"),
        "roles": {
            role: _content_projection(roles[role], f"{label} {role}")
            for role in ("manifest", "policy", "predicate_bundle")
        },
    }


def _current_authority_context(
    current_runtime_root: Path,
    current_release_v3: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    execution = _module_contract()
    try:
        release, binding = transport_v6.validate_runtime_authority(
            current_runtime_root, current_release_v3
        )
        artifact = transport_v6._artifact_projection(release)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV6Error("current release-v3 runtime authority is invalid") from exc
    exact_binding = _content_projection(binding, "current release-v3 authority")
    if (
        exact_binding["schema_version"] != resource_v8.DIRECT_SUCCESSOR_RELEASE_SCHEMA
        or exact_binding["status"] != resource_v8.DIRECT_SUCCESSOR_RELEASE_STATUS
        or exact_binding["file_sha256"] != resource_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or exact_binding["canonical_sha256"]
        != resource_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or artifact.get("artifact_sha256") != transport_v6.FROZEN_FINAL_ARTIFACT_SHA256
    ):
        raise FinalEvidenceV6Error("current release-v3 constants drifted")
    return release, exact_binding, execution, artifact


def _shadow_health_row(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FinalEvidenceV6Error(f"{label} shadow state is missing")
    row = dict(value)
    numeric_fields = {
        "externalSources",
        *resource_v8.GLOBAL_FLOW_STATE_ZERO_FIELDS,
        *resource_v8.GLOBAL_FLOW_VALUE_ZERO_FIELDS,
        *resource_v8.GLOBAL_REFERENCE_ZERO_FIELDS,
        *resource_v8.GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
        *resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
    }
    expected_fields = {*numeric_fields, "globalFlowReason", "globalRefReason"}
    if (
        set(row) != expected_fields
        or any(type(row.get(name)) is not int or row[name] != 0 for name in numeric_fields)
        or row.get("globalFlowReason") != resource_v8.SHADOW_DISABLED_REASON
        or row.get("globalRefReason") != resource_v8.SHADOW_DISABLED_REASON
    ):
        raise FinalEvidenceV6Error(f"{label} is not explicit-disabled/error0/absolute0")
    return row


def _active_startup_shadow(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FinalEvidenceV6Error("active startup shadow evidence is missing")
    identity = value.get("identity")
    backend = identity.get("global_flow_backend") if isinstance(identity, Mapping) else None
    expected_value_fields = {
        "identity",
        "identity_sha256",
        "all_shadow_evaluators_disabled",
        "all_global_flow_backend_fields_absolute_zero",
        "global_reference_basis_samples_absolute_zero",
    }
    expected_identity_fields = {
        "schema_version",
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
        "global_flow_native_requested",
        "global_flow_native_effective",
        "global_flow_backend",
        "global_reference_bridge_basis_sample_count",
        "state_restore_contract",
        "global_flow_shadow_config_explicit",
        "global_reference_shadow_config_explicit",
    }
    expected_backend_fields = {
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
    }
    if (
        set(value) != expected_value_fields
        or not isinstance(identity, Mapping)
        or set(identity) != expected_identity_fields
        or not isinstance(backend, Mapping)
        or set(backend) != expected_backend_fields
        or identity.get("schema_version") != "narrowgate_shadow_runtime_identity.v1"
        or identity.get("global_flow_shadow_enabled") is not False
        or identity.get("global_reference_shadow_enabled") is not False
        or identity.get("global_flow_shadow_config_explicit") is not True
        or identity.get("global_reference_shadow_config_explicit") is not True
        or identity.get("global_flow_native_requested") is not True
        or identity.get("global_flow_native_effective") is not False
        or identity.get("global_reference_bridge_basis_sample_count") != 0
        or identity.get("state_restore_contract") != "shadow_state_never_restored"
        or any(type(item) is not int or item != 0 for item in backend.values())
        or value.get("all_shadow_evaluators_disabled") is not True
        or value.get("all_global_flow_backend_fields_absolute_zero") is not True
        or value.get("global_reference_basis_samples_absolute_zero") is not True
    ):
        raise FinalEvidenceV6Error("active startup is not explicit-disabled/error0/absolute0")
    if _require_sha256(
        value.get("identity_sha256"), "active startup shadow identity"
    ) != _canonical_sha256(identity):
        raise FinalEvidenceV6Error("active startup shadow canonical identity drifted")
    return dict(value)


def _active_health_window(
    value: Any,
    *,
    transition: Mapping[str, Any],
    phase_label: str,
    post_lifecycle: bool = False,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FinalEvidenceV6Error(f"portable {phase_label} window is missing")
    window = dict(value)
    rows = window.get("rows")
    checks = window.get("checks")
    expected_window_fields = {
        "schema_version",
        "status",
        "boundary_offset_bytes",
        "active_pid",
        "active_pid_start_ticks",
        "active_process_stable_identity_sha256",
        "rows",
        "checks",
    }
    expected_checks = {
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
    }
    if (
        set(window) != expected_window_fields
        or window.get("schema_version") != ACTIVE_HEALTH_WINDOW_SCHEMA
        or window.get("status") != ACTIVE_HEALTH_WINDOW_STATUS
        or window.get("active_pid") != transition.get("active_pid")
        or window.get("active_pid_start_ticks") != transition.get("active_pid_start_ticks")
        or type(window.get("boundary_offset_bytes")) is not int
        or window["boundary_offset_bytes"] < 0
        or _SHA256_RE.fullmatch(str(window.get("active_process_stable_identity_sha256"))) is None
        or not isinstance(rows, list)
        or len(rows) != 2
        or checks != expected_checks
    ):
        raise FinalEvidenceV6Error(f"portable {phase_label} identity drifted")
    previous_wall = float("-inf")
    previous_updates = -1
    previous_windows = -1
    normalized_rows: list[dict[str, Any]] = []
    for index, raw in enumerate(rows, start=1):
        expected_row_fields = {
            "fresh_generation",
            "line_offset_bytes",
            "line_size_bytes",
            "line_sha256",
            "main_wall_timestamp_s",
            "projection",
            *({"readiness"} if post_lifecycle else set()),
        }
        if not isinstance(raw, Mapping) or set(raw) != expected_row_fields:
            raise FinalEvidenceV6Error(f"portable {phase_label} row is missing")
        projection = raw.get("projection")
        shadow = (
            projection.get("shadow_disabled_state") if isinstance(projection, Mapping) else None
        )
        if (
            raw.get("fresh_generation") != index
            or type(raw.get("line_offset_bytes")) is not int
            or raw["line_offset_bytes"] < window["boundary_offset_bytes"]
            or type(raw.get("line_size_bytes")) is not int
            or raw["line_size_bytes"] <= 0
            or _SHA256_RE.fullmatch(str(raw.get("line_sha256"))) is None
            or not isinstance(raw.get("main_wall_timestamp_s"), (int, float))
            or isinstance(raw.get("main_wall_timestamp_s"), bool)
            or not isinstance(projection, Mapping)
            or set(projection)
            != {
                "boolean_cooldown_enabled",
                "boolean_cooldown_updates",
                "buy_e3_enabled",
                "deep_book_buffer",
                "shadow_disabled_state",
                "counter_values",
            }
            or projection.get("boolean_cooldown_enabled") != 1
            or projection.get("buy_e3_enabled") != 1
            or projection.get("deep_book_buffer") != 0
            or type(projection.get("boolean_cooldown_updates")) is not int
            or projection["boolean_cooldown_updates"] <= previous_updates
        ):
            raise FinalEvidenceV6Error(f"portable {phase_label} row semantics drifted")
        wall = float(raw["main_wall_timestamp_s"])
        if wall <= previous_wall:
            raise FinalEvidenceV6Error(f"portable {phase_label} rows are not chronological")
        _shadow_health_row(shadow, f"{phase_label} row {index}")
        counters = projection.get("counter_values")
        if (
            not isinstance(counters, Mapping)
            or set(counters) != set(resource_v8.WINDOW_ZERO_COUNTERS[:-2])
            or any(type(item) is not int or item < 0 for item in counters.values())
            or any(
                counters.get(name) != 0
                for name in (
                    "buyE3CooldownInvalid",
                    "buyE3CooldownResets",
                    *resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
                )
            )
        ):
            raise FinalEvidenceV6Error(f"portable {phase_label} counters drifted")
        if post_lifecycle:
            readiness = raw.get("readiness")
            if not isinstance(readiness, Mapping) or set(readiness) != {
                "runtime_loaded",
                "warmup_time_admitted",
                "completed_windows",
                "gap_resets",
                "resets",
                "invalid_updates",
                "economic_outcome_claimed",
            }:
                raise FinalEvidenceV6Error(f"portable {phase_label} readiness drifted")
            completed_windows = readiness.get("completed_windows")
            if (
                readiness.get("runtime_loaded") is not True
                or readiness.get("warmup_time_admitted") is not True
                or type(completed_windows) is not int
                or completed_windows <= previous_windows
                or readiness.get("gap_resets") != 0
                or readiness.get("resets") != 0
                or readiness.get("invalid_updates") != 0
                or readiness.get("economic_outcome_claimed") is not False
            ):
                raise FinalEvidenceV6Error(f"portable {phase_label} readiness semantics drifted")
            previous_windows = completed_windows
        previous_wall = wall
        previous_updates = int(projection["boolean_cooldown_updates"])
        normalized_rows.append(dict(raw))
    window["rows"] = normalized_rows
    return window


def _current_no_shadow_evidence(
    portable: Mapping[str, Any],
) -> dict[str, Any]:
    disabled = portable.get("resource_disabled_process")
    active = portable.get("active_runtime")
    transition = portable.get("transition")
    if (
        not isinstance(disabled, Mapping)
        or not isinstance(active, Mapping)
        or not isinstance(transition, Mapping)
    ):
        raise FinalEvidenceV6Error("current no-shadow process evidence is incomplete")
    resource_shadow = disabled.get("shadow_runtime")
    if not isinstance(resource_shadow, Mapping):
        raise FinalEvidenceV6Error("resource-v8 disabled shadow window is missing")
    baseline = _shadow_health_row(resource_shadow.get("baseline"), "resource-v8 baseline")
    final = _shadow_health_row(resource_shadow.get("final"), "resource-v8 final")
    if (
        set(resource_shadow)
        != {
            "baseline",
            "final",
            "baseline_manifest_sha256",
            "final_manifest_sha256",
            "all_numeric_fields_absolute_zero",
            "disabled_reason_exact",
        }
        or resource_shadow.get("baseline_manifest_sha256") != _canonical_sha256(baseline)
        or resource_shadow.get("final_manifest_sha256") != _canonical_sha256(final)
        or resource_shadow.get("all_numeric_fields_absolute_zero") is not True
        or resource_shadow.get("disabled_reason_exact") is not True
    ):
        raise FinalEvidenceV6Error("resource-v8 shadow window flags drifted")
    startup = active.get("startup_semantics")
    startup_shadow = (
        _active_startup_shadow(startup.get("shadow_runtime"))
        if isinstance(startup, Mapping)
        else _active_startup_shadow(None)
    )
    health = _active_health_window(
        active.get("active_health_window"),
        transition=transition,
        phase_label="activation health",
    )
    return {
        "proof_basis": [
            "resource_v8_two_explicit_disabled_evaluators_error0_absolute0",
            "active_schema_v7_two_fresh_activation_health_rows_error0_absolute0",
        ],
        "resource_disabled_window": {
            "baseline": baseline,
            "final": final,
            "all_numeric_fields_absolute_zero": True,
            "disabled_reason_exact": True,
        },
        "active_startup": startup_shadow,
        "activation_health_window": health,
        "two_explicit_evaluators": {
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
        },
        "external_sources_absolute_zero": True,
        "shadow_or_companion_collection_enabled": False,
    }


def _validate_config_correction_content(value: Any) -> dict[str, Any]:
    content = _content_projection(value, "config correction")
    successor = resource_v8.config_successor
    if (
        content["schema_version"] != successor.SCHEMA_VERSION
        or content["status"] != successor.STATUS
        or content["canonical_field"] != successor.CANONICAL_FIELD
        or content["file_sha256"] != transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256
        or content["canonical_sha256"]
        != transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256
    ):
        raise FinalEvidenceV6Error("config correction exact7 identity drifted")
    return content


def _validate_portable_v6(
    value: Any,
    *,
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
    execution: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    _module_contract()
    if not isinstance(value, Mapping) or set(value) != set(transport_v6.PORTABLE_EVIDENCE_FIELDS):
        raise FinalEvidenceV6Error("transport-v6 portable evidence fields drifted")
    portable = dict(value)
    try:
        transport_v6._assert_portable(portable)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV6Error(
            "transport-v6 portable evidence contains local authority"
        ) from exc
    expected_authority = {
        **dict(release_binding),
        "execution": dict(execution),
        "runtime_authority": True,
    }
    if portable.get("runtime_execution") != execution:
        raise FinalEvidenceV6Error("portable runtime execution is not eacb release-v3")
    if portable.get("runtime_authority") != expected_authority:
        raise FinalEvidenceV6Error("portable runtime authority is not release-v3")
    if portable.get("exact_artifact") != artifact:
        raise FinalEvidenceV6Error("portable exact artifact is not current release-v3 artifact")
    if transport_v6._artifact_projection(release) != artifact:  # noqa: SLF001
        raise FinalEvidenceV6Error("portable artifact/release-v3 cross-binding drifted")

    host = portable.get("host")
    if (
        not isinstance(host, Mapping)
        or host.get("provider") != transport_v6.CURRENT_PROVIDER
        or host.get("region") != transport_v6.CURRENT_REGION
        or host.get("instance_id") != transport_v6.CURRENT_INSTANCE_ID
        or host.get("instance_type") != transport_v6.CURRENT_INSTANCE_TYPE
        or host.get("public_ipv4") != transport_v6.CURRENT_PUBLIC_IPV4_PROVENANCE
        or host.get("public_ipv4_role") != "network_locator_provenance_only_not_host_authority"
    ):
        raise FinalEvidenceV6Error("portable current-host identity drifted")

    disabled = portable.get("resource_disabled_process")
    transition = portable.get("transition")
    active = portable.get("active_runtime")
    if (
        not isinstance(disabled, Mapping)
        or not isinstance(transition, Mapping)
        or not isinstance(active, Mapping)
    ):
        raise FinalEvidenceV6Error("portable current process chain is incomplete")
    disabled_pid = disabled.get("pid")
    disabled_start = disabled.get("pid_start_ticks")
    active_pid = transition.get("active_pid")
    active_start = transition.get("active_pid_start_ticks")
    resource_runtime_files = disabled.get("runtime_source_files")
    resource_runtime_manifest = _require_sha256(
        disabled.get("runtime_source_manifest_sha256"),
        "portable resource runtime-source manifest",
    )
    if (
        type(disabled_pid) is not int
        or disabled_pid <= 0
        or type(disabled_start) is not int
        or disabled_start <= 0
        or disabled.get("config_sha256") != transport_v6.FROZEN_FINAL_DISABLED_CONFIG_SHA256
        or disabled.get("fresh_pid") is not True
        or disabled.get("fresh_start_ticks") is not True
        or disabled.get("same_pid_pre_post") is not True
        or not isinstance(resource_runtime_files, Mapping)
        or dict(resource_runtime_files) != EXPECTED_RESOURCE_RUNTIME_SOURCE_SHA256
        or resource_runtime_manifest != EXPECTED_RESOURCE_RUNTIME_SOURCE_MANIFEST_SHA256
        or transition.get("disabled_pid") != disabled_pid
        or transition.get("disabled_pid_start_ticks") != disabled_start
        or type(active_pid) is not int
        or active_pid <= 0
        or active_pid == disabled_pid
        or type(active_start) is not int
        or active_start <= disabled_start
        or transition.get("fresh_disabled_to_active_restart") is not True
    ):
        raise FinalEvidenceV6Error("portable disabled-to-active process transition drifted")

    runtime_files = active.get("runtime_source_files")
    startup = active.get("startup_semantics")
    active_runtime_manifest = _require_sha256(
        active.get("runtime_source_manifest_sha256"),
        "portable active runtime-source manifest",
    )
    if (
        active.get("config_sha256") != transport_v6.FROZEN_FINAL_ACTIVE_CONFIG_SHA256
        or active.get("artifact_sha256") != transport_v6.FROZEN_FINAL_ARTIFACT_SHA256
        or active.get("buy_e3_enabled") is not True
        or active.get("owner_override_effective") is not True
        or not isinstance(runtime_files, Mapping)
        or dict(runtime_files) != EXPECTED_ACTIVE_RUNTIME_SOURCE_SHA256
        or active_runtime_manifest != EXPECTED_ACTIVE_RUNTIME_SOURCE_MANIFEST_SHA256
        or not isinstance(startup, Mapping)
        or startup.get("startup_status") != "accepted"
        or startup.get("running_checkout_commit") != CURRENT_EXECUTION_COMMIT
        or startup.get("running_checkout_tree") != CURRENT_EXECUTION_TREE
    ):
        raise FinalEvidenceV6Error("portable current active runtime drifted")

    receipts = portable.get("source_receipts")
    if not isinstance(receipts, Mapping) or set(receipts) != set(PORTABLE_SOURCE_ROLES):
        raise FinalEvidenceV6Error("portable source receipt roles drifted")
    expected_receipt_identity = {
        "config_correction": (
            resource_v8.config_successor.SCHEMA_VERSION,
            resource_v8.config_successor.STATUS,
            resource_v8.config_successor.CANONICAL_FIELD,
            transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256,
            transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256,
        ),
        "current_host_resource_gate": (
            resource_v8.RESOURCE_SCHEMA,
            resource_v8.RESOURCE_STATUS,
            resource_v8.RESOURCE_CANONICAL_FIELD,
            transport_v6.FROZEN_FINAL_RESOURCE_FILE_SHA256,
            transport_v6.FROZEN_FINAL_RESOURCE_CANONICAL_SHA256,
        ),
        "active_process_capture": (
            ACTIVE_CAPTURE_SCHEMA_V7,
            ACTIVE_CAPTURE_STATUS_V7,
            active_capture_v8.CANONICAL_FIELD,
            transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256,
            transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256,
        ),
        "remote_active_attestation": (
            transport_v6.REMOTE_ATTESTATION_SCHEMA,
            transport_v6.REMOTE_ATTESTATION_STATUS,
            transport_v6.REMOTE_ATTESTATION_CANONICAL_FIELD,
            None,
            None,
        ),
    }
    normalized_receipts: dict[str, dict[str, Any]] = {}
    for role in PORTABLE_SOURCE_ROLES:
        row = receipts[role]
        if not isinstance(row, Mapping) or set(row) != {
            *CONTENT_BINDING_FIELDS,
            "local_filename",
        }:
            raise FinalEvidenceV6Error(f"portable {role} content fields drifted")
        content = _content_projection(
            {field: row.get(field) for field in CONTENT_BINDING_FIELDS},
            f"portable {role}",
        )
        schema, status, canonical_field, file_sha, canonical_sha = expected_receipt_identity[role]
        if (
            content["schema_version"] != schema
            or content["status"] != status
            or content["canonical_field"] != canonical_field
            or row.get("local_filename") != transport_v6.SOURCE_FILENAMES[role]
            or (file_sha is not None and content["file_sha256"] != file_sha)
            or (canonical_sha is not None and content["canonical_sha256"] != canonical_sha)
        ):
            raise FinalEvidenceV6Error(f"portable {role} identity drifted")
        normalized_receipts[role] = dict(row)
    portable["source_receipts"] = normalized_receipts
    no_shadow = _current_no_shadow_evidence(portable)
    return portable, no_shadow


def _admission_context(
    path: Path,
    *,
    current_runtime_root: Path,
    current_release_v3: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    _require_exact_path(
        path,
        FROZEN_CROSS_HOST_ADMISSION_PATH_PROVENANCE,
        "cross-host admission",
    )
    release, release_binding, execution, artifact = _current_authority_context(
        current_runtime_root, current_release_v3
    )
    try:
        admission = transport_v6.validate_cross_host_admission(
            path,
            direct_repository_root=current_runtime_root,
            direct_release_path=current_release_v3,
        )
    except Exception as exc:
        raise FinalEvidenceV6Error("transport-v6 cross-host admission is invalid") from exc
    binding = _receipt_binding(
        path,
        label="transport-v6 cross-host admission",
        canonical_field=transport_v6.ADMISSION_CANONICAL_FIELD,
        schema=transport_v6.ADMISSION_SCHEMA,
        status=transport_v6.ADMISSION_STATUS,
    )
    exact_binding = _exact_content(
        {field: binding.get(field) for field in CONTENT_BINDING_FIELDS},
        FROZEN_CROSS_HOST_ADMISSION_CONTENT,
        "cross-host admission",
    )
    portable, no_shadow = _validate_portable_v6(
        admission.get("portable_evidence"),
        release=release,
        release_binding=release_binding,
        execution=execution,
        artifact=artifact,
    )
    return admission, {**binding, **exact_binding}, portable, no_shadow


def _validate_resource_attempt_rejection_history_payload(payload: Any) -> dict[str, Any]:
    """Reassert the producer-owned historical-only authority boundary."""

    if not isinstance(payload, Mapping):
        raise FinalEvidenceV6Error("resource rejection-history payload is missing")
    projection = payload.get("failed_activation_projection")
    rejection = projection.get("rejection") if isinstance(projection, Mapping) else None
    attempts = payload.get("resource_gate_attempts")
    summary = payload.get("summary")
    expected_attempts = failed_history_v1._attempts(  # noqa: SLF001
        v6_binding=failed_history_v1.V6_WRONG_ROUTE_BENCHMARK,
        v7_attempt2_binding=failed_history_v1.V7_ATTEMPT2_BENCHMARK,
    )
    if (
        payload.get("schema_version") != failed_history_v1.SCHEMA_VERSION
        or payload.get("status") != failed_history_v1.STATUS
        or payload.get("failed_activation_source") != failed_history_v1.FAILED_ACTIVATION_SOURCE
        or not isinstance(projection, Mapping)
        or projection.get("source_reported_unadmitted_session_token")
        != failed_history_v1.FAILED_SESSION_TOKEN
        or not isinstance(rejection, Mapping)
        or rejection.get("exchange_error_code") != -5022
        or rejection.get("formal_collection_valid") is not False
        or rejection.get("formal_admission_allowed") is not False
        or any(
            projection.get(name) is not False
            for name in (
                "epoch_established",
                "runtime_authority",
                "evidence_authority",
                "reusable_for_current",
            )
        )
        or not isinstance(attempts, Mapping)
        or attempts != expected_attempts
        or any(
            not isinstance(attempt, Mapping)
            or attempt.get("formal_resource_receipt_created") is not False
            or attempt.get("active_process_started") is not False
            for attempt in attempts.values()
        )
        or attempts["resource_v7_attempt1"].get("benchmark", {}).get("exact7_binding_claimed")
        is not False
        or not isinstance(summary, Mapping)
        or summary.get("admitted_epoch_count") != 0
        or summary.get("resource_receipt_count") != 0
        or summary.get("active_process_started_in_resource_attempts") is not False
        or summary.get("current_runtime_authority_derived_from_history") is not False
        or payload.get("authority_design") != failed_history_v1.AUTHORITY_DESIGN
        or payload.get("permissions") != failed_history_v1.PERMISSIONS
        or payload.get("evidence_boundary") != failed_history_v1.EVIDENCE_BOUNDARY
    ):
        raise FinalEvidenceV6Error("resource rejection-history nonauthority semantics drifted")
    return dict(payload)


def _resource_attempt_rejection_history_context(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _require_exact_path(
        path,
        RESOURCE_ATTEMPT_REJECTION_HISTORY_PATH_PROVENANCE,
        "resource attempt rejection history",
    )
    frozen = _frozen_content(
        RESOURCE_ATTEMPT_REJECTION_HISTORY_CONTENT,
        "resource attempt rejection history",
    )
    failed_source = _require_exact_path(
        Path(FAILED_ACTIVATION_SOURCE_PATH_PROVENANCE),
        FAILED_ACTIVATION_SOURCE_PATH_PROVENANCE,
        "failed activation source",
    )
    v6_benchmark = _require_exact_path(
        Path(FAILED_V6_BENCHMARK_PATH_PROVENANCE),
        FAILED_V6_BENCHMARK_PATH_PROVENANCE,
        "failed resource-v6 benchmark",
    )
    v7_attempt2_benchmark = _require_exact_path(
        Path(FAILED_V7_ATTEMPT2_BENCHMARK_PATH_PROVENANCE),
        FAILED_V7_ATTEMPT2_BENCHMARK_PATH_PROVENANCE,
        "failed resource-v7 attempt2 benchmark",
    )
    try:
        payload = failed_history_v1.validate_failed_activation_attempt_history(
            path,
            failed_activation_source_path=failed_source,
            v6_wrong_route_benchmark_path=v6_benchmark,
            v7_attempt2_benchmark_path=v7_attempt2_benchmark,
        )
        binding = _receipt_binding(
            path,
            label="resource attempt rejection history",
            canonical_field=failed_history_v1.CANONICAL_FIELD,
            schema=failed_history_v1.SCHEMA_VERSION,
            status=failed_history_v1.STATUS,
        )
    except Exception as exc:
        raise FinalEvidenceV6Error("resource attempt rejection history is invalid") from exc
    exact = _exact_content(
        {field: binding.get(field) for field in CONTENT_BINDING_FIELDS},
        frozen,
        "resource attempt rejection history",
    )
    observed = _validate_resource_attempt_rejection_history_payload(payload)
    return observed, {**binding, **exact}


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
        raise FinalEvidenceV6Error("superseded v4 historical fields drifted")
    proof = _exact_content(
        value.get("proof_evidence_release"),
        SUPERSEDED_V4_PROOF_CONTENT,
        "superseded v4 proof",
    )
    if (
        value.get("role") != "superseded_historical_due_global_shadow_runtime_enabled"
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
        raise FinalEvidenceV6Error("superseded v4 historical nonauthority drifted")
    return {**dict(value), "proof_evidence_release": proof}


def _superseded_v4_proof_context(
    path: Path,
    *,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Revalidate the old proof and retain it only as exact historical evidence."""

    try:
        proof = historical_final_v4.validate_evidence_release(
            path,
            final_v4_root=historical_v4_root,
            final_v4_release=historical_v4_release_v2,
        )
        proof_binding = _receipt_binding(
            path,
            label="superseded v4 proof evidence-release",
            canonical_field=historical_final_v4.EVIDENCE_RELEASE_CANONICAL_FIELD,
            schema=historical_final_v4.EVIDENCE_RELEASE_SCHEMA,
            status=historical_final_v4.EVIDENCE_RELEASE_STATUS,
        )
        exact = _exact_content(
            {field: proof_binding.get(field) for field in CONTENT_BINDING_FIELDS},
            SUPERSEDED_V4_PROOF_CONTENT,
            "superseded v4 proof",
        )
        attempt_path = Path(str(proof.get("operational_attempt_final", {}).get("path", "")))
        attempt, attempt_binding = historical_final_v4._read_own_receipt(  # noqa: SLF001
            attempt_path,
            label="superseded v4 attempt-final",
            canonical_field=historical_final_v4.ATTEMPT_FINAL_CANONICAL_FIELD,
            schema=historical_final_v4.ATTEMPT_FINAL_SCHEMA,
            status=historical_final_v4.ATTEMPT_FINAL_STATUS,
        )
        composition_path = Path(str(attempt.get("final_composition", {}).get("path", "")))
        composition, composition_binding = historical_final_v4._read_own_receipt(  # noqa: SLF001
            composition_path,
            label="superseded v4 composition",
            canonical_field=historical_final_v4.COMPOSITION_CANONICAL_FIELD,
            schema=historical_final_v4.COMPOSITION_SCHEMA,
            status=historical_final_v4.COMPOSITION_STATUS,
        )
    except Exception as exc:
        raise FinalEvidenceV6Error("superseded v4 proof is invalid") from exc
    evidence = composition.get("evidence")
    lifecycle = (
        evidence.get("final_v4_lifecycle_admission") if isinstance(evidence, Mapping) else None
    )
    parent_execution = dict(transport_v6.direct_release_v3.PARENT_EXECUTION)
    if (
        proof.get("operational_attempt_final") != attempt_binding
        or attempt.get("final_composition") != composition_binding
        or proof.get("runtime_execution") != parent_execution
        or composition.get("runtime_execution") != parent_execution
        or not isinstance(lifecycle, Mapping)
        or lifecycle.get("baseline_epoch_id") != SUPERSEDED_V4_EPOCH_ID
        or lifecycle.get("config_sha256") != SUPERSEDED_V4_ACTIVE_CONFIG_SHA256
    ):
        raise FinalEvidenceV6Error("superseded v4 epoch/config identity drifted")
    historical = _validate_superseded_v4_historical(
        {
            "role": "superseded_historical_due_global_shadow_runtime_enabled",
            "epoch_id": SUPERSEDED_V4_EPOCH_ID,
            "active_config_sha256": SUPERSEDED_V4_ACTIVE_CONFIG_SHA256,
            "proof_evidence_release": exact,
            "current_runtime_authority": False,
            "current_cross_host_evidence": False,
            "current_lifecycle_evidence": False,
            "reused_as_current_evidence": False,
        }
    )
    return proof, {**proof_binding, **exact}, historical


def _lifecycle_context(
    path: Path,
    *,
    lifecycle_context_path: Path,
    current_runtime_root: Path,
    active_runtime: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    _require_exact_path(
        path,
        FROZEN_CURRENT_LIFECYCLE_PATH_PROVENANCE,
        "current lifecycle admission",
    )
    frozen = _frozen_content(
        FROZEN_CURRENT_LIFECYCLE_CONTENT,
        "current lifecycle admission",
        allowed_modes=frozenset({"0644"}),
    )
    _require_exact_path(
        lifecycle_context_path,
        FROZEN_LIFECYCLE_CONTEXT_PATH_PROVENANCE,
        "portable lifecycle admission context",
    )
    frozen_context = _frozen_content(
        FROZEN_LIFECYCLE_CONTEXT_CONTENT,
        "portable lifecycle admission context",
    )
    if not FROZEN_CURRENT_LIFECYCLE_EPOCH_ID.startswith("prospective-"):
        raise FinalEvidenceV6Error("current lifecycle epoch is not source-frozen")
    try:
        context = lifecycle_context_v1.validate_lifecycle_context_against_admission(
            lifecycle_context_path,
            lifecycle_admission_path=path,
            runtime_repository_root=current_runtime_root,
        )
        context_binding = _receipt_binding(
            lifecycle_context_path,
            label="portable lifecycle admission context",
            canonical_field=lifecycle_context_v1.CANONICAL_FIELD,
            schema=lifecycle_context_v1.SCHEMA_VERSION,
            status=lifecycle_context_v1.STATUS,
        )
        payload, binding = base._validate_lifecycle_admission(path)  # noqa: SLF001
    except Exception as exc:
        raise FinalEvidenceV6Error("current release-v3 lifecycle admission is invalid") from exc
    exact = _exact_content(
        {field: binding.get(field) for field in CONTENT_BINDING_FIELDS},
        frozen,
        "current lifecycle admission",
        allowed_modes=frozenset({"0644"}),
    )
    exact_context = _exact_content(
        {field: context_binding.get(field) for field in CONTENT_BINDING_FIELDS},
        frozen_context,
        "portable lifecycle admission context",
    )
    active_files = active_runtime.get("runtime_source_files")
    admitted_files = binding.get("runtime_code_files")
    context_projection = context.get("lifecycle_projection")
    baseline_epoch = str(binding.get("baseline_epoch_id", ""))
    health = active_runtime.get("active_health_window")
    rows = health.get("rows") if isinstance(health, Mapping) else None
    if not isinstance(rows, list) or len(rows) != 2:
        raise FinalEvidenceV6Error("current lifecycle lacks activation-health binding")
    first_health_s = float(rows[0].get("main_wall_timestamp_s", -1.0))
    second_health_s = float(rows[1].get("main_wall_timestamp_s", -1.0))
    epoch_start_s = int(binding.get("epoch_start_ts_ns", -1)) / 1_000_000_000
    admitted_s = int(payload.get("admitted_ts_ns", -1)) / 1_000_000_000
    if (
        baseline_epoch != FROZEN_CURRENT_LIFECYCLE_EPOCH_ID
        or baseline_epoch == SUPERSEDED_V4_EPOCH_ID
        or active_runtime.get("config_sha256") != transport_v6.FROZEN_FINAL_ACTIVE_CONFIG_SHA256
        or binding.get("config_sha256") != active_runtime.get("config_sha256")
        or not isinstance(active_files, Mapping)
        or not isinstance(admitted_files, Mapping)
        or dict(active_files) != EXPECTED_ACTIVE_RUNTIME_SOURCE_SHA256
        or dict(admitted_files) != EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256
        or any(admitted_files.get(name) != digest for name, digest in active_files.items())
        or not isinstance(context_projection, Mapping)
        or context.get("lifecycle_admission") != exact
        or context_projection.get("admitted_ts_ns") != payload.get("admitted_ts_ns")
        or context_projection.get("session_id") != binding.get("session_id")
        or context_projection.get("baseline_epoch_id") != baseline_epoch
        or context_projection.get("config_sha256") != binding.get("config_sha256")
        or context_projection.get("runtime_code_sha256") != lifecycle_context_v1.RUNTIME_CODE_SHA256
        or context_projection.get("runtime_code_schema_version")
        != lifecycle_context_v1.RUNTIME_CODE_SCHEMA
        or context_projection.get("runtime_source_files")
        != EXPECTED_LIFECYCLE_RUNTIME_SOURCE_SHA256
        or context_projection.get("runtime_source_file_count") != 65
        or context_projection.get("runtime_source_files_canonical_sha256")
        != lifecycle_context_v1.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
        or context_projection.get("action_enablement_sha256")
        != binding.get("action_enablement_sha256")
        or context_projection.get("epoch_start_ts_ns") != binding.get("epoch_start_ts_ns")
        or context_projection.get("writer_runtime_identity_sha256")
        != binding.get("writer_runtime_identity_sha256")
        or context_projection.get("writer_identity_file_sha256")
        != binding.get("writer_identity_file_sha256")
        or context_projection.get("epoch_manifest_file_sha256")
        != binding.get("epoch_manifest_file_sha256")
        or context_projection.get("identity_evidence_file_sha256")
        != binding.get("identity_evidence_file_sha256")
        or context_projection.get("safe_action_state") != lifecycle_context_v1.SAFE_ACTION_STATE
        or context_projection.get("action_shadow_enabled_state")
        != lifecycle_context_v1.SAFE_ACTION_SHADOW_ENABLED_STATE
        or context_projection.get("external_shadow_only_inert") is not True
        or context_projection.get("external_source_recording_state")
        != lifecycle_context_v1.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        or context_projection.get("external_source_count")
        != len(lifecycle_context_v1.SAFE_EXTERNAL_SOURCE_RECORDING_STATE)
        or context_projection.get("source_settings_inert_because_external_master_false") is not True
        or context_projection.get(
            "record_trades_inert_because_master_false_and_record_enabled_false"
        )
        is not True
        or context_projection.get("external_effective_stream_and_recording_disabled") is not True
        or not (0.0 < epoch_start_s <= first_health_s < second_health_s < admitted_s)
    ):
        raise FinalEvidenceV6Error(
            "current lifecycle epoch/config/runtime/activation-health binding drifted or was reused"
        )
    required_action_state = binding.get("required_action_state")
    if not isinstance(required_action_state, Mapping) or any(
        required_action_state.get(name) != value
        for name, value in {
            "strategy.buy_e3_cooldown_policy_enabled": True,
            "strategy.buy_fill_selection_live_enabled": False,
            "strategy.buy_fill_selection_shadow_enabled": False,
            "strategy.dynamic_fill_hazard_action_enabled": False,
            "strategy.dynamic_fill_hazard_shadow_enabled": False,
            "logging.inventory_campaign_shadow_enabled": False,
        }.items()
    ):
        raise FinalEvidenceV6Error("current lifecycle action/shadow state drifted")
    return (
        payload,
        {**binding, **exact},
        context,
        {**context_binding, **exact_context},
    )


def _finite_nonnegative(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise FinalEvidenceV6Error(f"{label} is not numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise FinalEvidenceV6Error(f"{label} is not numeric") from exc
    if not math.isfinite(result) or result < 0.0:
        raise FinalEvidenceV6Error(f"{label} is not finite and nonnegative")
    return result


def _post_lifecycle_aggregates(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"resource", "latency", "position"}:
        raise FinalEvidenceV6Error("post-lifecycle operational aggregates drifted")
    resource = value.get("resource")
    latency = value.get("latency")
    position = value.get("position")
    if not isinstance(resource, Mapping) or set(resource) != {
        "sample_count",
        "min_mem_available_mib",
        "max_live_rss_mib",
        "oom_window_delta",
        "swap_in_window_delta",
        "swap_out_window_delta",
    }:
        raise FinalEvidenceV6Error("post-lifecycle resource aggregates drifted")
    if (
        type(resource.get("sample_count")) is not int
        or resource["sample_count"] <= 0
        or _finite_nonnegative(resource.get("min_mem_available_mib"), "MemAvailable") < 0.0
        or _finite_nonnegative(resource.get("max_live_rss_mib"), "live RSS") < 0.0
        or resource.get("oom_window_delta") != 0
        or resource.get("swap_in_window_delta") != 0
        or resource.get("swap_out_window_delta") != 0
    ):
        raise FinalEvidenceV6Error("post-lifecycle resource safety aggregates drifted")
    if not isinstance(latency, Mapping) or set(latency) != {
        "decision_sample_count",
        "decision_p99_us",
        "lifecycle_enqueue_p99_us",
        "lifecycle_write_p99_ms",
        "small_sample_disclosed",
        "strategy_result_authority",
        "formal_performance_authority",
        "resource_v8_formal_gate_unchanged",
        "economic_outcome_claimed",
    }:
        raise FinalEvidenceV6Error("post-lifecycle latency aggregates drifted")
    decision_count = latency.get("decision_sample_count")
    decision_p99 = latency.get("decision_p99_us")
    if type(decision_count) is not int or decision_count < 0:
        raise FinalEvidenceV6Error("post-lifecycle decision_sample_count drifted")
    if (decision_count == 0 and decision_p99 is not None) or (
        decision_count > 0
        and _finite_nonnegative(decision_p99, "post-lifecycle decision p99") < 0.0
    ):
        raise FinalEvidenceV6Error("post-lifecycle decision p99 drifted")
    if (
        _finite_nonnegative(latency.get("lifecycle_enqueue_p99_us"), "post-lifecycle enqueue p99")
        < 0.0
        or _finite_nonnegative(latency.get("lifecycle_write_p99_ms"), "post-lifecycle write p99")
        < 0.0
        or latency.get("small_sample_disclosed") is not True
        or latency.get("strategy_result_authority") is not False
        or latency.get("formal_performance_authority") is not False
        or latency.get("resource_v8_formal_gate_unchanged") is not True
        or latency.get("economic_outcome_claimed") is not False
    ):
        raise FinalEvidenceV6Error("post-lifecycle latency authority drifted")
    if not isinstance(position, Mapping) or set(position) != {
        "main_health_position_projection_completed",
        "reported_aggregate_position_flat",
        "reported_open_order_count",
        "economic_values_persisted",
    }:
        raise FinalEvidenceV6Error("post-lifecycle position safety projection drifted")
    if (
        position.get("main_health_position_projection_completed") is not True
        or type(position.get("reported_aggregate_position_flat")) is not bool
        or type(position.get("reported_open_order_count")) is not int
        or position["reported_open_order_count"] < 0
        or position.get("economic_values_persisted") is not False
    ):
        raise FinalEvidenceV6Error("post-lifecycle position safety semantics drifted")
    return {
        "resource": dict(resource),
        "latency": dict(latency),
        "position": dict(position),
    }


def _validate_post_lifecycle_health_payload(
    payload: Any,
    *,
    lifecycle_payload: Mapping[str, Any],
    lifecycle_binding: Mapping[str, Any],
    runtime_execution: Mapping[str, Any],
    runtime_authority: Mapping[str, Any],
    transition: Mapping[str, Any],
    active_runtime: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        payload = post_lifecycle_v1.validate_portable_projection(payload)
    except Exception as exc:
        raise FinalEvidenceV6Error("post-lifecycle producer projection is invalid") from exc
    fields = {
        "schema_version",
        "status",
        "generated_utc",
        "runtime_execution",
        "runtime_authority",
        "active_process",
        "lifecycle_admission",
        "lifecycle_epoch_id",
        "main_health_window",
        "lifecycle_health",
        "operational_aggregates",
        "lifecycle_process_cross_binding",
        "checks",
        "permissions",
        "evidence_boundary",
        POST_LIFECYCLE_HEALTH_CANONICAL_FIELD,
    }
    if not isinstance(payload, Mapping) or set(payload) != fields:
        raise FinalEvidenceV6Error("post-lifecycle live-health fields drifted")
    generated = _timestamp(payload.get("generated_utc"), "post-lifecycle health timestamp")
    if (
        payload.get("schema_version") != POST_LIFECYCLE_HEALTH_SCHEMA
        or payload.get("status") != POST_LIFECYCLE_HEALTH_STATUS
        or payload.get(POST_LIFECYCLE_HEALTH_CANONICAL_FIELD)
        != _document_sha256(payload, POST_LIFECYCLE_HEALTH_CANONICAL_FIELD)
        or payload.get("runtime_execution") != runtime_execution
        or payload.get("runtime_authority") != runtime_authority
        or payload.get("lifecycle_epoch_id") != lifecycle_binding.get("baseline_epoch_id")
        or payload.get("lifecycle_process_cross_binding")
        != post_lifecycle_v1.LIFECYCLE_PROCESS_CROSS_BINDING
    ):
        raise FinalEvidenceV6Error("post-lifecycle live-health authority drifted")
    lifecycle_content = {field: lifecycle_binding.get(field) for field in CONTENT_BINDING_FIELDS}
    if payload.get("lifecycle_admission") != lifecycle_content:
        raise FinalEvidenceV6Error("post-lifecycle health lost lifecycle exact7 binding")
    process = payload.get("active_process")
    runtime_identity = active_runtime.get("runtime_identity")
    activation_health = active_runtime.get("active_health_window")
    if not isinstance(runtime_identity, Mapping) or not isinstance(activation_health, Mapping):
        raise FinalEvidenceV6Error("post-lifecycle health lacks active runtime identity")
    expected_process = {
        "pid": transition.get("active_pid"),
        "pid_start_ticks": transition.get("active_pid_start_ticks"),
        "process_identity_sha256": transition.get("active_process_identity_sha256"),
        "stable_process_identity_sha256": activation_health.get(
            "active_process_stable_identity_sha256"
        ),
        "config_sha256": active_runtime.get("config_sha256"),
        "runtime_identity_file_sha256": runtime_identity.get("file_sha256"),
        "runtime_identity_canonical_sha256": runtime_identity.get("canonical_sha256"),
        "runtime_source_manifest_sha256": active_runtime.get("runtime_source_manifest_sha256"),
        "runtime_source_files": active_runtime.get("runtime_source_files"),
        "release_file_sha256": runtime_authority.get("file_sha256"),
        "release_canonical_sha256": runtime_authority.get("canonical_sha256"),
    }
    for name in (
        "process_identity_sha256",
        "stable_process_identity_sha256",
        "config_sha256",
        "runtime_identity_file_sha256",
        "runtime_identity_canonical_sha256",
        "runtime_source_manifest_sha256",
        "release_file_sha256",
        "release_canonical_sha256",
    ):
        _require_sha256(expected_process[name], f"post-lifecycle active {name}")
    if process != expected_process:
        raise FinalEvidenceV6Error("post-lifecycle health active process changed")
    main_health = _active_health_window(
        payload.get("main_health_window"),
        transition=transition,
        phase_label="post-lifecycle health",
        post_lifecycle=True,
    )
    lifecycle_health = payload.get("lifecycle_health")
    if not isinstance(lifecycle_health, Mapping) or set(lifecycle_health) != {
        "observed_utc",
        "line_sha256",
        "order_lifecycle_v2_drops",
        "order_lifecycle_v2_errors",
    }:
        raise FinalEvidenceV6Error("post-lifecycle lifecycle-health projection drifted")
    observed = _timestamp(
        lifecycle_health.get("observed_utc"), "post-lifecycle lifecycle-health timestamp"
    )
    _require_sha256(lifecycle_health.get("line_sha256"), "post-lifecycle lifecycle-health line")
    if (
        lifecycle_health.get("order_lifecycle_v2_drops") != 0
        or lifecycle_health.get("order_lifecycle_v2_errors") != 0
    ):
        raise FinalEvidenceV6Error("post-lifecycle lifecycle health has drops/errors")
    admitted_ns = int(lifecycle_payload.get("admitted_ts_ns", -1))
    first_health_s = float(main_health["rows"][0]["main_wall_timestamp_s"])
    second_health_s = float(main_health["rows"][1]["main_wall_timestamp_s"])
    generated_s = datetime.fromisoformat(generated.replace("Z", "+00:00")).timestamp()
    lifecycle_observed_s = datetime.fromisoformat(observed.replace("Z", "+00:00")).timestamp()
    if not (
        admitted_ns > 0
        and admitted_ns / 1_000_000_000 < first_health_s < second_health_s <= generated_s
        and admitted_ns / 1_000_000_000 < lifecycle_observed_s <= generated_s
    ):
        raise FinalEvidenceV6Error("post-lifecycle health predates lifecycle admission")
    aggregates = _post_lifecycle_aggregates(payload.get("operational_aggregates"))
    expected_checks = {
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
    }
    if (
        payload.get("checks") != expected_checks
        or payload.get("permissions") != NO_NEW_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise FinalEvidenceV6Error("post-lifecycle health checks/authority drifted")
    result = dict(payload)
    result["main_health_window"] = main_health
    result["operational_aggregates"] = aggregates
    return result


def _post_lifecycle_health_context(
    path: Path,
    *,
    lifecycle_payload: Mapping[str, Any],
    lifecycle_binding: Mapping[str, Any],
    lifecycle_context_payload: Mapping[str, Any],
    lifecycle_context_binding: Mapping[str, Any],
    runtime_execution: Mapping[str, Any],
    runtime_authority: Mapping[str, Any],
    transition: Mapping[str, Any],
    active_runtime: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    _require_exact_path(
        path,
        FROZEN_POST_LIFECYCLE_HEALTH_PATH_PROVENANCE,
        "post-lifecycle live health",
    )
    frozen = _frozen_content(
        FROZEN_POST_LIFECYCLE_HEALTH_CONTENT,
        "post-lifecycle live health",
    )
    try:
        payload, binding = base._binding(  # noqa: SLF001
            path,
            label="post-lifecycle live health",
            canonical_field=POST_LIFECYCLE_RECEIPT_CANONICAL_FIELD,
            expected_schema=POST_LIFECYCLE_RECEIPT_SCHEMA,
            expected_status=POST_LIFECYCLE_RECEIPT_STATUS,
        )
    except Exception as exc:
        raise FinalEvidenceV6Error("post-lifecycle live-health receipt is invalid") from exc
    exact = _exact_content(
        {field: binding.get(field) for field in CONTENT_BINDING_FIELDS},
        frozen,
        "post-lifecycle live health",
    )
    try:
        raw = post_lifecycle_v1.validate_content_projection(payload)
        portable = post_lifecycle_v1.portable_projection(raw)
    except Exception as exc:
        raise FinalEvidenceV6Error(
            "post-lifecycle source-frozen content projection is invalid"
        ) from exc
    expected_context_content = {
        field: lifecycle_context_binding.get(field) for field in CONTENT_BINDING_FIELDS
    }
    if (
        raw.get("lifecycle_context_receipt") != expected_context_content
        or raw.get("lifecycle_context") != lifecycle_context_payload.get("lifecycle_projection")
        or raw.get("lifecycle_admission") != lifecycle_context_payload.get("lifecycle_admission")
    ):
        raise FinalEvidenceV6Error(
            "post-lifecycle receipt lost durable lifecycle-context cross-binding"
        )
    observed = _validate_post_lifecycle_health_payload(
        portable,
        lifecycle_payload=lifecycle_payload,
        lifecycle_binding=lifecycle_binding,
        runtime_execution=runtime_execution,
        runtime_authority=runtime_authority,
        transition=transition,
        active_runtime=active_runtime,
    )
    return observed, {**binding, **exact}


def build_activation_envelope(
    *,
    cross_host_admission_path: Path,
    resource_attempt_rejection_history_path: Path,
    superseded_v4_proof_path: Path,
    current_runtime_root: Path,
    current_release_v3: Path,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    _admission, admission_binding, portable, no_shadow = _admission_context(
        cross_host_admission_path,
        current_runtime_root=current_runtime_root,
        current_release_v3=current_release_v3,
    )
    _rejections, rejection_binding = _resource_attempt_rejection_history_context(
        resource_attempt_rejection_history_path
    )
    _old_proof, old_proof_binding, old_v4 = _superseded_v4_proof_context(
        superseded_v4_proof_path,
        historical_v4_root=historical_v4_root,
        historical_v4_release_v2=historical_v4_release_v2,
    )
    correction = _validate_config_correction_content(
        {
            field: portable["source_receipts"]["config_correction"].get(field)
            for field in CONTENT_BINDING_FIELDS
        }
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "release-v3 activation envelope timestamp")
    payload = {
        "schema_version": ENVELOPE_SCHEMA,
        "identity": OWNER,
        "attempt_id": ATTEMPT_ID,
        "status": ENVELOPE_STATUS,
        "generated_utc": timestamp,
        "cross_host_admission": admission_binding,
        "resource_attempt_rejection_history": rejection_binding,
        "superseded_v4_proof_receipt": old_proof_binding,
        "superseded_v4_proof": old_v4,
        "host": dict(portable["host"]),
        "runtime_execution": dict(portable["runtime_execution"]),
        "runtime_authority": dict(portable["runtime_authority"]),
        "exact_artifact": dict(portable["exact_artifact"]),
        "config_correction": correction,
        "resource_disabled_process": dict(portable["resource_disabled_process"]),
        "transition": dict(portable["transition"]),
        "active_runtime": dict(portable["active_runtime"]),
        "source_receipts": dict(portable["source_receipts"]),
        "current_no_shadow_evidence": no_shadow,
        "checks": {
            "runtime_eacb_release_v3_exact": True,
            "transport_module_v6_exact": True,
            "resource_module_v8_exact": True,
            "active_module_v8_schema_v7_exact": True,
            "config_correction_exact": True,
            "resource_v8_exact": True,
            "active_v7_exact": True,
            "remote_attestation_transport_v6_exact": True,
            "two_current_explicit_disabled_evaluators_error0_absolute0": True,
            "post_active_health_two_fresh_rows": True,
            "shadow_or_companion_collection_enabled": False,
            "superseded_v4_proof_history_only": True,
            "rejected_resource_attempts_history_only": True,
            "all_failed_resource_receipts_absent_claim_scope": (
                "historical_operational_report_only"
            ),
            "failed_resource_receipt_absence_is_formal_or_current_authority": False,
            "persistent_absence_witness_for_v5_or_v7_claimed": False,
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
    current_runtime_root: Path,
    current_release_v3: Path,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
) -> dict[str, Any]:
    payload, _binding = _read_own_receipt(
        path,
        label="release-v3 activation envelope",
        canonical_field=ENVELOPE_CANONICAL_FIELD,
        schema=ENVELOPE_SCHEMA,
        status=ENVELOPE_STATUS,
    )
    expected = build_activation_envelope(
        cross_host_admission_path=Path(
            str(payload.get("cross_host_admission", {}).get("path", ""))
        ),
        resource_attempt_rejection_history_path=Path(
            str(payload.get("resource_attempt_rejection_history", {}).get("path", ""))
        ),
        superseded_v4_proof_path=Path(
            str(payload.get("superseded_v4_proof_receipt", {}).get("path", ""))
        ),
        current_runtime_root=current_runtime_root,
        current_release_v3=current_release_v3,
        historical_v4_root=historical_v4_root,
        historical_v4_release_v2=historical_v4_release_v2,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise FinalEvidenceV6Error("release-v3 activation envelope identity drifted")
    return payload


def finalize_activation_envelope(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_activation_envelope(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_activation_envelope,
        validator_kwargs=_validation_roots(kwargs),
    )


def _activation_context(path: Path, **roots: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = validate_activation_envelope(path, **roots)
    binding = _receipt_binding(
        path,
        label="release-v3 activation envelope",
        canonical_field=ENVELOPE_CANONICAL_FIELD,
        schema=ENVELOPE_SCHEMA,
        status=ENVELOPE_STATUS,
    )
    return payload, binding


def build_completion(
    *,
    activation_envelope_path: Path,
    lifecycle_admission_path: Path,
    lifecycle_context_path: Path,
    post_lifecycle_health_path: Path,
    current_runtime_root: Path,
    current_release_v3: Path,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    roots = {
        "current_runtime_root": current_runtime_root,
        "current_release_v3": current_release_v3,
        "historical_v4_root": historical_v4_root,
        "historical_v4_release_v2": historical_v4_release_v2,
    }
    envelope, envelope_binding = _activation_context(activation_envelope_path, **roots)
    (
        lifecycle,
        lifecycle_binding,
        lifecycle_context,
        lifecycle_context_binding,
    ) = _lifecycle_context(
        lifecycle_admission_path,
        lifecycle_context_path=lifecycle_context_path,
        current_runtime_root=current_runtime_root,
        active_runtime=envelope["active_runtime"],
    )
    post_health, post_health_binding = _post_lifecycle_health_context(
        post_lifecycle_health_path,
        lifecycle_payload=lifecycle,
        lifecycle_binding=lifecycle_binding,
        lifecycle_context_payload=lifecycle_context,
        lifecycle_context_binding=lifecycle_context_binding,
        runtime_execution=envelope["runtime_execution"],
        runtime_authority=envelope["runtime_authority"],
        transition=envelope["transition"],
        active_runtime=envelope["active_runtime"],
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "release-v3 operational completion timestamp")
    payload = {
        "schema_version": COMPLETION_SCHEMA,
        "identity": OWNER,
        "attempt_id": ATTEMPT_ID,
        "status": COMPLETION_STATUS,
        "generated_utc": timestamp,
        "activation_envelope": envelope_binding,
        "cross_host_admission": dict(envelope["cross_host_admission"]),
        "resource_attempt_rejection_history": dict(envelope["resource_attempt_rejection_history"]),
        "superseded_v4_proof": dict(envelope["superseded_v4_proof"]),
        "runtime_execution": dict(envelope["runtime_execution"]),
        "runtime_authority": dict(envelope["runtime_authority"]),
        "exact_artifact": dict(envelope["exact_artifact"]),
        "transition": dict(envelope["transition"]),
        "config_correction": dict(envelope["config_correction"]),
        "current_host_resource": dict(envelope["source_receipts"]["current_host_resource_gate"]),
        "active_process_capture": dict(envelope["source_receipts"]["active_process_capture"]),
        "remote_active_attestation": dict(envelope["source_receipts"]["remote_active_attestation"]),
        "current_no_shadow_evidence": dict(envelope["current_no_shadow_evidence"]),
        "current_lifecycle_admission": lifecycle_binding,
        "current_lifecycle_context": lifecycle_context_binding,
        "post_lifecycle_live_health": post_health_binding,
        "post_lifecycle_no_shadow_evidence": {
            "main_health_window": dict(post_health["main_health_window"]),
            "lifecycle_health": dict(post_health["lifecycle_health"]),
            "operational_aggregates": dict(post_health["operational_aggregates"]),
            "lifecycle_process_cross_binding": dict(post_health["lifecycle_process_cross_binding"]),
            "checks": dict(post_health["checks"]),
        },
        "current_lifecycle_observed_payload_sha256": _canonical_sha256(lifecycle),
        "current_lifecycle_context_observed_payload_sha256": _canonical_sha256(lifecycle_context),
        "lifecycle_cross_binding": {
            "baseline_epoch_id": lifecycle_binding["baseline_epoch_id"],
            "superseded_v4_epoch_id": SUPERSEDED_V4_EPOCH_ID,
            "new_epoch_differs_from_superseded_v4": True,
            "config_sha256": lifecycle_binding["config_sha256"],
            "active_config_sha256": transport_v6.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
            "runtime_code_sha256": lifecycle_binding["runtime_code_sha256"],
            "runtime_source_file_count": 65,
            "runtime_source_files_canonical_sha256": (
                lifecycle_context_v1.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
            ),
            "active_runtime_source_files": dict(envelope["active_runtime"]["runtime_source_files"]),
            "admitted_runtime_code_files": dict(lifecycle_binding["runtime_code_files"]),
            "no_shadow_config_binding": {
                "active_config_sha256_exact": True,
                "global_flow_shadow_disabled_bound_by_exact_config": True,
                "global_reference_shadow_disabled_bound_by_exact_config": True,
            },
            "safe_action_state": dict(lifecycle_context_v1.SAFE_ACTION_STATE),
            "action_shadow_enabled_state": dict(
                lifecycle_context_v1.SAFE_ACTION_SHADOW_ENABLED_STATE
            ),
            "external_source_recording_state": [
                dict(row) for row in lifecycle_context_v1.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
            ],
            "external_shadow_only_inert_because_master_disabled": True,
            "external_source_settings_inert_because_master_disabled": True,
            "record_trades_true_but_inert_because_master_and_record_enabled_false": True,
            "external_stream_and_recording_effectively_disabled": True,
            "runtime_source_binding": {
                "active_startup_source_map_exact": True,
                "lifecycle_source_map_exact65": True,
            },
            "action_enablement_and_data_source_projection_reopened": True,
            "lifecycle_admission_contains_pid_or_pid_start_ticks": False,
            "direct_lifecycle_admission_to_active_process_binding_claimed": False,
            "lifecycle_to_active_process_binding_basis": (
                "exact_config_runtime_source_and_post_admission_chronology_only"
            ),
            "activation_health_window": dict(envelope["active_runtime"]["active_health_window"]),
            "post_lifecycle_health": post_health_binding,
            "health_drop_count": 0,
            "health_error_count": 0,
            "old_lifecycle_reused": False,
        },
        "gate_results": {
            "runtime_eacb_release_v3": "passed",
            "config_correction": "passed",
            "current_resource_v8": "passed",
            "current_active_schema_v7": "passed",
            "transport_v6_remote_attestation": "passed",
            "transport_v6_cross_host_admission": "passed",
            "two_fresh_activation_health_rows": "passed",
            "two_fresh_post_lifecycle_health_rows": "passed",
            "two_explicit_disabled_evaluators_error0_absolute0_before_and_after_lifecycle": (
                "passed"
            ),
            "new_lifecycle_error0_drop0": "passed",
            "portable_lifecycle_context_reopened_against_orico": "passed",
            "superseded_v4_proof_history_only": "passed",
            "failed_5022_and_v5_v6_v7_attempts_history_only": "passed",
            "failed_resource_receipt_absence_current_authority": "not_claimed",
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
        label="release-v3 operational completion",
        canonical_field=COMPLETION_CANONICAL_FIELD,
        schema=COMPLETION_SCHEMA,
        status=COMPLETION_STATUS,
    )
    expected = build_completion(
        activation_envelope_path=Path(str(payload.get("activation_envelope", {}).get("path", ""))),
        lifecycle_admission_path=Path(
            str(payload.get("current_lifecycle_admission", {}).get("path", ""))
        ),
        lifecycle_context_path=Path(
            str(payload.get("current_lifecycle_context", {}).get("path", ""))
        ),
        post_lifecycle_health_path=Path(
            str(payload.get("post_lifecycle_live_health", {}).get("path", ""))
        ),
        generated_utc=str(payload.get("generated_utc", "")),
        **roots,
    )
    if payload != expected:
        raise FinalEvidenceV6Error("release-v3 operational completion identity drifted")
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
        label="release-v3 operational completion",
        canonical_field=COMPLETION_CANONICAL_FIELD,
        schema=COMPLETION_SCHEMA,
        status=COMPLETION_STATUS,
    )
    return payload, binding


def build_composition(
    *,
    activation_envelope_path: Path,
    operational_completion_path: Path,
    current_runtime_root: Path,
    current_release_v3: Path,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    roots = {
        "current_runtime_root": current_runtime_root,
        "current_release_v3": current_release_v3,
        "historical_v4_root": historical_v4_root,
        "historical_v4_release_v2": historical_v4_release_v2,
    }
    envelope, envelope_binding = _activation_context(activation_envelope_path, **roots)
    completion, completion_binding = _completion_context(operational_completion_path, **roots)
    if (
        completion.get("activation_envelope") != envelope_binding
        or completion.get("cross_host_admission") != envelope.get("cross_host_admission")
        or completion.get("resource_attempt_rejection_history")
        != envelope.get("resource_attempt_rejection_history")
        or completion.get("superseded_v4_proof") != envelope.get("superseded_v4_proof")
        or completion.get("current_no_shadow_evidence")
        != envelope.get("current_no_shadow_evidence")
    ):
        raise FinalEvidenceV6Error("final-v6 composition inputs are not one evidence chain")
    evidence = {
        "current_release_v3_runtime_authority": {
            field: completion["runtime_authority"].get(field) for field in CONTENT_BINDING_FIELDS
        },
        "current_config_correction": dict(completion["config_correction"]),
        "current_resource_v8": dict(completion["current_host_resource"]),
        "current_active_schema_v7": dict(completion["active_process_capture"]),
        "current_remote_attestation_transport_v6": dict(completion["remote_active_attestation"]),
        "current_cross_host_admission_transport_v6": dict(completion["cross_host_admission"]),
        "current_activation_no_shadow_health": dict(completion["current_no_shadow_evidence"]),
        "current_lifecycle_admission": dict(completion["current_lifecycle_admission"]),
        "current_lifecycle_context": dict(completion["current_lifecycle_context"]),
        "current_post_lifecycle_live_health": dict(completion["post_lifecycle_live_health"]),
        "current_post_lifecycle_no_shadow_health": dict(
            completion["post_lifecycle_no_shadow_evidence"]
        ),
        "failed_activation_attempt_history": dict(completion["resource_attempt_rejection_history"]),
        "superseded_v4_proof_history": dict(completion["superseded_v4_proof"]),
        "activation_envelope_v6": envelope_binding,
        "operational_completion_v6": completion_binding,
    }
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "release-v3 final composition timestamp")
    payload = {
        "schema_version": COMPOSITION_SCHEMA,
        "identity": OWNER,
        "attempt_id": ATTEMPT_ID,
        "status": COMPOSITION_STATUS,
        "generated_utc": timestamp,
        "runtime_execution": dict(completion["runtime_execution"]),
        "runtime_authority": dict(completion["runtime_authority"]),
        "exact_artifact": dict(completion["exact_artifact"]),
        "ordered_evidence_roles": list(evidence),
        "evidence": evidence,
        "composition_root_sha256": _canonical_sha256(evidence),
        "composition_truth": {
            "current_runtime_authority_is_release_v3": True,
            "proof_release_will_replace_runtime_authority": False,
            "resource_v8_active_v7_transport_v6_only": True,
            "activation_and_post_lifecycle_health_are_distinct": True,
            "no_shadow_claim_uses_only_current_explicit_disabled_error0_absolute0": True,
            "new_lifecycle_only": True,
            "portable_lifecycle_context_is_transport_only": True,
            "portable_lifecycle_context_reopened_against_orico": True,
            "direct_lifecycle_admission_to_active_pid_binding_claimed": False,
            "lifecycle_process_cross_binding_basis_is_limited_and_explicit": True,
            "old_lifecycle_reused": False,
            "superseded_v4_proof_used_as_current_authority": False,
            "failed_activation_attempts_used_as_current_authority": False,
            "failed_resource_receipt_absence_used_as_current_authority": False,
            "failed_resource_receipt_absence_scope": "historical_operational_report_only",
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
        label="release-v3 final composition",
        canonical_field=COMPOSITION_CANONICAL_FIELD,
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        raise FinalEvidenceV6Error("release-v3 composition evidence is missing")
    expected = build_composition(
        activation_envelope_path=Path(
            str(evidence.get("activation_envelope_v6", {}).get("path", ""))
        ),
        operational_completion_path=Path(
            str(evidence.get("operational_completion_v6", {}).get("path", ""))
        ),
        generated_utc=str(payload.get("generated_utc", "")),
        **roots,
    )
    if payload != expected:
        raise FinalEvidenceV6Error("release-v3 final composition identity drifted")
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
    current_runtime_root: Path,
    current_release_v3: Path,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    roots = {
        "current_runtime_root": current_runtime_root,
        "current_release_v3": current_release_v3,
        "historical_v4_root": historical_v4_root,
        "historical_v4_release_v2": historical_v4_release_v2,
    }
    composition = validate_composition(final_composition_path, **roots)
    composition_binding = _receipt_binding(
        final_composition_path,
        label="release-v3 final composition",
        canonical_field=COMPOSITION_CANONICAL_FIELD,
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    evidence = composition["evidence"]
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "release-v3 attempt-final timestamp")
    payload = {
        "schema_version": ATTEMPT_FINAL_SCHEMA,
        "identity": OWNER,
        "attempt_id": ATTEMPT_ID,
        "status": ATTEMPT_FINAL_STATUS,
        "generated_utc": timestamp,
        "runtime_execution": dict(composition["runtime_execution"]),
        "runtime_authority": dict(composition["runtime_authority"]),
        "exact_artifact": dict(composition["exact_artifact"]),
        "final_composition": composition_binding,
        "composition_root_sha256": composition["composition_root_sha256"],
        "config_correction": dict(evidence["current_config_correction"]),
        "current_resource_v8": dict(evidence["current_resource_v8"]),
        "current_active_schema_v7": dict(evidence["current_active_schema_v7"]),
        "current_remote_attestation_transport_v6": dict(
            evidence["current_remote_attestation_transport_v6"]
        ),
        "current_lifecycle_admission": dict(evidence["current_lifecycle_admission"]),
        "current_lifecycle_context": dict(evidence["current_lifecycle_context"]),
        "current_activation_no_shadow_evidence": dict(
            evidence["current_activation_no_shadow_health"]
        ),
        "current_post_lifecycle_live_health": dict(evidence["current_post_lifecycle_live_health"]),
        "current_post_lifecycle_no_shadow_evidence": dict(
            evidence["current_post_lifecycle_no_shadow_health"]
        ),
        "failed_activation_attempt_history": dict(evidence["failed_activation_attempt_history"]),
        "superseded_v4_proof": dict(evidence["superseded_v4_proof_history"]),
        "result": {
            "operational_evidence_complete": True,
            "cross_host_evidence_admitted": True,
            "lifecycle_evidence_admitted": True,
            "lifecycle_context_reopened_against_orico": True,
            "activation_health_evidence_admitted": True,
            "post_lifecycle_health_evidence_admitted": True,
            "no_shadow_or_companion_current_evidence": True,
            "release_v3_runtime_authority_unchanged": True,
            "superseded_v4_proof_is_history_only": True,
            "failed_activation_attempts_are_history_only": True,
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
        label="release-v3 attempt-final",
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
        raise FinalEvidenceV6Error("release-v3 attempt-final identity drifted")
    return payload


def finalize_attempt_final(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_attempt_final(**kwargs)
    return _finalize(
        output_path,
        payload,
        validator=validate_attempt_final,
        validator_kwargs=_validation_roots(kwargs),
    )


def build_evidence_release(
    *,
    attempt_final_path: Path,
    current_runtime_root: Path,
    current_release_v3: Path,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    # Deliberately re-open the source-frozen current release.  The proof is a
    # consumer of that authority and cannot mint or replace it.
    release, release_binding, execution, artifact = _current_authority_context(
        current_runtime_root, current_release_v3
    )
    roots = {
        "current_runtime_root": current_runtime_root,
        "current_release_v3": current_release_v3,
        "historical_v4_root": historical_v4_root,
        "historical_v4_release_v2": historical_v4_release_v2,
    }
    attempt = validate_attempt_final(attempt_final_path, **roots)
    attempt_binding = _receipt_binding(
        attempt_final_path,
        label="release-v3 attempt-final",
        canonical_field=ATTEMPT_FINAL_CANONICAL_FIELD,
        schema=ATTEMPT_FINAL_SCHEMA,
        status=ATTEMPT_FINAL_STATUS,
    )
    expected_authority = {
        **dict(release_binding),
        "execution": dict(execution),
        "runtime_authority": True,
    }
    if (
        attempt.get("runtime_execution") != execution
        or attempt.get("runtime_authority") != expected_authority
        or attempt.get("exact_artifact") != artifact
    ):
        raise FinalEvidenceV6Error("attempt-final is not bound to current release-v3 authority")
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "release-v3 proof evidence-release timestamp")
    payload = {
        "schema_version": EVIDENCE_RELEASE_SCHEMA,
        "identity": OWNER,
        "attempt_id": ATTEMPT_ID,
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
        },
        "runtime_execution": dict(execution),
        "runtime_authority": expected_authority,
        "exact_artifact": dict(artifact),
        "scope": dict(release["scope"]),
        "rollback": dict(release["rollback"]),
        "operational_attempt_final": attempt_binding,
        "composition_root_sha256": attempt["composition_root_sha256"],
        "config_correction": dict(attempt["config_correction"]),
        "current_resource_v8": dict(attempt["current_resource_v8"]),
        "current_active_schema_v7": dict(attempt["current_active_schema_v7"]),
        "current_remote_attestation_transport_v6": dict(
            attempt["current_remote_attestation_transport_v6"]
        ),
        "current_lifecycle_admission": dict(attempt["current_lifecycle_admission"]),
        "current_lifecycle_context": dict(attempt["current_lifecycle_context"]),
        "current_activation_no_shadow_evidence": dict(
            attempt["current_activation_no_shadow_evidence"]
        ),
        "current_post_lifecycle_live_health": dict(attempt["current_post_lifecycle_live_health"]),
        "current_post_lifecycle_no_shadow_evidence": dict(
            attempt["current_post_lifecycle_no_shadow_evidence"]
        ),
        "failed_activation_attempt_history": dict(attempt["failed_activation_attempt_history"]),
        "superseded_v4_proof": dict(attempt["superseded_v4_proof"]),
        "evidence_state": {
            "post_release_evidence_complete": True,
            "cross_host_evidence_admitted": True,
            "lifecycle_evidence_admitted": True,
            "lifecycle_context_reopened_against_orico": True,
            "activation_health_evidence_admitted": True,
            "post_lifecycle_health_evidence_admitted": True,
            "two_explicit_disabled_evaluators_error0_absolute0": True,
            "shadow_or_companion_collection_enabled": False,
            "old_lifecycle_reused": False,
            "failed_activation_attempts_are_nonauthoritative_history": True,
            "superseded_v4_proof_is_nonauthoritative_history": True,
            "runtime_authority_replaced": False,
            "runtime_consumed": True,
            "runtime_consumed_authority": "direct_owner_release_v3",
            "does_not_replace_runtime_active_release": True,
            "exact_artifact_oof_available": False,
            "old_oof_applies_to_learning_algorithm_only": True,
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
    current_runtime_root: Path,
    current_release_v3: Path,
    historical_v4_root: Path,
    historical_v4_release_v2: Path,
) -> dict[str, Any]:
    payload, _binding = _read_own_receipt(
        path,
        label="release-v3 proof evidence-release",
        canonical_field=EVIDENCE_RELEASE_CANONICAL_FIELD,
        schema=EVIDENCE_RELEASE_SCHEMA,
        status=EVIDENCE_RELEASE_STATUS,
    )
    expected = build_evidence_release(
        attempt_final_path=Path(str(payload.get("operational_attempt_final", {}).get("path", ""))),
        current_runtime_root=current_runtime_root,
        current_release_v3=current_release_v3,
        historical_v4_root=historical_v4_root,
        historical_v4_release_v2=historical_v4_release_v2,
        generated_utc=str(payload.get("generated_utc", "")),
    )
    if payload != expected:
        raise FinalEvidenceV6Error("release-v3 proof evidence-release identity drifted")
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
        "current_runtime_root",
        "current_release_v3",
        "historical_v4_root",
        "historical_v4_release_v2",
    )
    return {name: values[name] for name in names if name in values}


def _roots(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--current-runtime-root", type=Path, required=True)
    parser.add_argument("--current-release-v3", type=Path, required=True)
    parser.add_argument("--historical-v4-root", type=Path, required=True)
    parser.add_argument("--historical-v4-release-v2", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    envelope = commands.add_parser("activation-envelope")
    _roots(envelope)
    envelope.add_argument("--cross-host-admission", type=Path, required=True)
    envelope.add_argument("--resource-attempt-rejection-history", type=Path, required=True)
    envelope.add_argument("--superseded-v4-proof", type=Path, required=True)
    envelope.add_argument("--output", type=Path, required=True)

    completion = commands.add_parser("completion")
    _roots(completion)
    completion.add_argument("--activation-envelope", type=Path, required=True)
    completion.add_argument("--lifecycle-admission", type=Path, required=True)
    completion.add_argument("--lifecycle-context", type=Path, required=True)
    completion.add_argument("--post-lifecycle-live-health", type=Path, required=True)
    completion.add_argument("--output", type=Path, required=True)

    composition = commands.add_parser("composition")
    _roots(composition)
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
        "current_runtime_root": args.current_runtime_root,
        "current_release_v3": args.current_release_v3,
        "historical_v4_root": args.historical_v4_root,
        "historical_v4_release_v2": args.historical_v4_release_v2,
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
            cross_host_admission_path=args.cross_host_admission,
            resource_attempt_rejection_history_path=args.resource_attempt_rejection_history,
            superseded_v4_proof_path=args.superseded_v4_proof,
            output_path=args.output,
            **roots,
        )
    elif args.command == "completion":
        payload, file_sha = finalize_completion(
            activation_envelope_path=args.activation_envelope,
            lifecycle_admission_path=args.lifecycle_admission,
            lifecycle_context_path=args.lifecycle_context,
            post_lifecycle_health_path=args.post_lifecycle_live_health,
            output_path=args.output,
            **roots,
        )
    elif args.command == "composition":
        payload, file_sha = finalize_composition(
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
