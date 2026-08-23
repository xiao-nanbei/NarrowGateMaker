#!/usr/bin/env python3
"""Shared structural contract for the immutable BUY E3 final composition v2.

This leaf module has no research, deployment, or runtime imports.  Producers
and consumers use it to agree on one exact document schema without creating an
import cycle or reopening economic evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any, Final

OWNER_IDENTITY: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
SCHEMA_VERSION: Final = f"{OWNER_IDENTITY}.final_composition_receipt.v2"
COMPOSITION_IDENTITY: Final = f"{OWNER_IDENTITY}.final_composition"
STATUS: Final = "owner_buy_e3_final_evidence_composed"
CANONICAL_FIELD: Final = "canonical_final_composition_receipt_sha256"
EXPECTED_DAY_COUNT: Final = 30

LAYER4_DAY_PREFIX: Final = "layer4_day::"
WRAPPER_PREFIX: Final = "stability_wrapper::"

BASE_ROLE_ORDER: Final[tuple[str, ...]] = (
    "formal_buy_component_manifest",
    "formal_buy_component_validation",
    "joint_closeout_manifest",
    "owner_decision",
    "attempt_execution_manifest",
    "source_execution_manifest",
    "cpp_builder_preflight",
    "cpp_quick_preflight",
    "cpp_qualification",
    "owner_execution_preflight",
    "label_materialization",
    "refit_receipt",
    "exact_artifact_manifest",
    "exact_policy",
    "exact_predicate_bundle",
    "parity_research_compiled",
    "parity_development_snapshot",
    "parity_streaming_offline",
    "layer4_mechanics",
    "layer4_contract",
)

PRE_ADMISSION_RECEIPT_ROLES: Final[tuple[str, ...]] = (
    "single_day",
    "all_fold_zero_economic",
    "durability_concurrency_cache",
    "parity_layer1",
    "parity_layer2",
    "parity_layer3",
    "parity_layer4",
    "sell54",
    "regression",
)

CURRENT_ROLE_ORDER: Final[tuple[str, ...]] = (
    "compatible_execution_attempt",
    *(f"{WRAPPER_PREFIX}{role}" for role in PRE_ADMISSION_RECEIPT_ROLES),
    "compatible_runtime_regression",
    "compatible_concurrent_resource",
    "sell_54_case",
    "compatible_activation_envelope",
)

TOP_LEVEL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "research_identity",
        "amendment",
        "formal_learning_algorithm",
        "producer_chain",
        "exact_artifact",
        "compatible_execution_attempt",
        "stability_wrapper_source_canonical_sha256",
        "current_authority_evidence",
        "evidence_interpretation",
        "authority",
        "evidence_boundary",
        "permissions",
        "ordered_evidence",
        "ordered_evidence_sha256",
        CANONICAL_FIELD,
    }
)

ORDERED_EVIDENCE_BINDING_FIELDS: Final = frozenset(
    {
        "role",
        "path",
        "file_sha256",
        "size_bytes",
        "mode",
        "device",
        "inode",
        "schema_version",
        "identity",
        "status",
        "status_source",
        "canonical_field",
        "canonical_sha256",
    }
)

OUTPUT_EVIDENCE_BOUNDARY: Final = {
    "panel_role": "Development",
    "validation_read": False,
    "sealed_holdout_read": False,
    "economic_values_read": False,
    "economic_values_exposed": False,
    "economic_values_persisted": False,
    "economic_values_used_for_selection": False,
    "new_economic_arm_run": False,
    "shadow_or_companion_created": False,
    "hypothetical_actions_scored": False,
    "hypothetical_live_actions_scored": False,
}

OUTPUT_PERMISSIONS: Final = {
    "research_authorized": False,
    "action_authorized": False,
    "live_authorized": False,
}

_AMENDMENT: Final = {
    "preserves_v1_history": True,
    "research_identity_changed": False,
    "historical_v1_runtime_authority_reused": False,
    "ordinary_bugfix_attempt_only": True,
}
_EVIDENCE_INTERPRETATION: Final = {
    "research_supported": False,
    "owner_risk_accepted": True,
    "formal_hierarchy_passed": False,
    "formal_hard_gates_passed": False,
    "old_oof_applies_to_learning_algorithm_only": True,
    "exact_artifact_oof_available": False,
}
_AUTHORITY: Final = {"research": False, "action": False, "live": False}

_SHA_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_DAY_RE: Final = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ATTEMPT_ID_RE: Final = re.compile(r"^attempt-[a-z0-9][a-z0-9._-]*$")
_FORMAL_VERSION_ALIAS_RE: Final = re.compile(r"formal[-_]v\d+", re.IGNORECASE)
_CANONICAL_FIELD_RE: Final = re.compile(r"^[a-z][a-z0-9_]*sha256$")

_FALSE_BOUNDARY_KEYS: Final = frozenset(
    {
        "validation_read",
        "sealed_holdout_read",
        "research_authorized",
        "action_authorized",
        "live_authorized",
        "economic_values_read",
        "economic_values_exposed",
        "economic_values_persisted",
        "economic_values_persisted_in_receipt",
        "economic_values_used_for_selection",
        "new_economic_arm_run",
        "shadow_or_companion_created",
        "hypothetical_actions_scored",
        "hypothetical_live_actions_scored",
        "hypothetical_live_scoring",
        "exact_artifact_oof_available",
        "exact_final_artifact_oof_available",
        "old_oof_estimate_applies_to_exact_artifact",
        "old_oof_estimate_applies_to_exact_owner_artifact",
    }
)


class FinalCompositionContractError(RuntimeError):
    """Raised when the portable v2 composition document drifts."""


def canonical_sha256(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise FinalCompositionContractError("document is not canonical ASCII JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def document_sha256(payload: Mapping[str, Any], field: str = CANONICAL_FIELD) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def ordered_roles(day_count: int = EXPECTED_DAY_COUNT) -> tuple[str, ...]:
    if type(day_count) is not int or day_count != EXPECTED_DAY_COUNT:
        raise FinalCompositionContractError("final composition requires exactly 30 days")
    return (
        *BASE_ROLE_ORDER,
        *(f"{LAYER4_DAY_PREFIX}{index:02d}" for index in range(day_count)),
        "layer4_final",
        *CURRENT_ROLE_ORDER,
    )


def _exact_mapping(value: Any, fields: set[str] | frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(fields):
        raise FinalCompositionContractError(f"{label} fields drifted")
    return value


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or _SHA_RE.fullmatch(value) is None:
        raise FinalCompositionContractError(f"{label} is not a SHA256")
    return value


def _git_sha(value: Any, label: str) -> str:
    if type(value) is not str or _GIT_SHA_RE.fullmatch(value) is None:
        raise FinalCompositionContractError(f"{label} is not a Git object id")
    return value


def _sha_map(value: Any, roles: Sequence[str], label: str) -> Mapping[str, Any]:
    mapping = _exact_mapping(value, set(roles), label)
    for role in roles:
        _sha(mapping[role], f"{label}.{role}")
    return mapping


def _walk_mappings(value: Any) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []

    def visit(node: Any) -> None:
        if isinstance(node, Mapping):
            result.append(node)
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    return result


def _validate_boundaries(payload: Mapping[str, Any]) -> None:
    for mapping in _walk_mappings(payload):
        for key in _FALSE_BOUNDARY_KEYS:
            if key in mapping and mapping[key] is not False:
                raise FinalCompositionContractError(f"final composition {key} must remain false")
        if mapping.get("research_supported") not in {None, False}:
            raise FinalCompositionContractError(
                "final composition research_supported must remain false"
            )


def _validate_training_days(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) != EXPECTED_DAY_COUNT:
        raise FinalCompositionContractError("final composition training days drifted")
    days = tuple(value)
    if any(type(day) is not str or _DAY_RE.fullmatch(day) is None for day in days) or days != tuple(
        sorted(set(days))
    ):
        raise FinalCompositionContractError("final composition training days drifted")
    return days


def validate_training_days(value: Any) -> tuple[str, ...]:
    """Validate and normalize the frozen 30-day Development panel identity."""

    return _validate_training_days(value)


def _validate_attempt(value: Any) -> Mapping[str, Any]:
    attempt = _exact_mapping(
        value,
        {
            "schema_version",
            "identity",
            "attempt_id",
            "canonical_execution_attempt_sha256",
            "execution_commit",
            "execution_tree",
            "annotated_tag",
            "annotated_tag_object",
            "pre_admission_wrapper_canonical_sha256",
        },
        "final composition compatible attempt",
    )
    attempt_id = attempt.get("attempt_id")
    tag = attempt.get("annotated_tag")
    if (
        attempt.get("schema_version") != f"{OWNER_IDENTITY}.compatible_execution_attempt.v2"
        or attempt.get("identity") != OWNER_IDENTITY
        or type(attempt_id) is not str
        or _ATTEMPT_ID_RE.fullmatch(attempt_id) is None
        or _FORMAL_VERSION_ALIAS_RE.search(attempt_id) is not None
        or type(tag) is not str
        or not tag
        or any(character.isspace() for character in tag)
    ):
        raise FinalCompositionContractError("final composition compatible attempt drifted")
    _sha(attempt.get("canonical_execution_attempt_sha256"), "compatible attempt canonical")
    _git_sha(attempt.get("execution_commit"), "compatible attempt commit")
    _git_sha(attempt.get("execution_tree"), "compatible attempt tree")
    _git_sha(attempt.get("annotated_tag_object"), "compatible attempt tag object")
    _sha_map(
        attempt.get("pre_admission_wrapper_canonical_sha256"),
        PRE_ADMISSION_RECEIPT_ROLES,
        "compatible attempt wrapper identities",
    )
    return attempt


def _validate_ordered_evidence(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise FinalCompositionContractError("ordered evidence is not a list")
    expected_roles = ordered_roles()
    if len(value) != len(expected_roles):
        raise FinalCompositionContractError("ordered evidence role set or count drifted")
    observed: list[Mapping[str, Any]] = []
    seen_paths: set[str] = set()
    seen_inodes: set[tuple[int, int]] = set()
    for expected_role, raw in zip(expected_roles, value, strict=True):
        binding = _exact_mapping(
            raw,
            ORDERED_EVIDENCE_BINDING_FIELDS,
            f"ordered evidence {expected_role}",
        )
        path = binding.get("path")
        parsed_path = PurePosixPath(path) if type(path) is str else PurePosixPath("/")
        device = binding.get("device")
        inode = binding.get("inode")
        if (
            binding.get("role") != expected_role
            or type(path) is not str
            or not path
            or parsed_path.is_absolute()
            or any(part in {"", ".", ".."} for part in parsed_path.parts)
            or path in seen_paths
            or binding.get("mode") != "0600"
            or type(binding.get("size_bytes")) is not int
            or binding.get("size_bytes") <= 0
            or type(device) is not int
            or device < 0
            or type(inode) is not int
            or inode <= 0
            or (device, inode) in seen_inodes
            or type(binding.get("schema_version")) is not str
            or not binding.get("schema_version")
            or type(binding.get("identity")) is not str
            or not binding.get("identity")
            or type(binding.get("status")) is not str
            or not binding.get("status")
            or binding.get("status_source") not in {"source_field", "role_contract"}
            or type(binding.get("canonical_field")) is not str
            or _CANONICAL_FIELD_RE.fullmatch(binding["canonical_field"]) is None
        ):
            raise FinalCompositionContractError(
                f"ordered evidence binding drifted: {expected_role}"
            )
        _sha(binding.get("file_sha256"), f"ordered evidence {expected_role} file")
        _sha(binding.get("canonical_sha256"), f"ordered evidence {expected_role} canonical")
        seen_paths.add(path)
        seen_inodes.add((device, inode))
        observed.append(binding)
    return tuple(observed)


def validate_final_composition_v2(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the exact portable v2 document without opening bound evidence."""

    composition = _exact_mapping(payload, TOP_LEVEL_FIELDS, "final composition v2")
    if (
        composition.get("schema_version") != SCHEMA_VERSION
        or composition.get("identity") != COMPOSITION_IDENTITY
        or composition.get("status") != STATUS
        or composition.get("research_identity") != OWNER_IDENTITY
        or composition.get("amendment") != _AMENDMENT
        or composition.get("evidence_interpretation") != _EVIDENCE_INTERPRETATION
        or composition.get("authority") != _AUTHORITY
        or composition.get("evidence_boundary") != OUTPUT_EVIDENCE_BOUNDARY
        or composition.get("permissions") != OUTPUT_PERMISSIONS
    ):
        raise FinalCompositionContractError("final composition v2 identity or authority drifted")

    formal = _exact_mapping(
        composition.get("formal_learning_algorithm"),
        {
            "learning_algorithm_artifact_sha256",
            "old_oof_applies_to_learning_algorithm_only",
            "exact_artifact_oof_available",
        },
        "formal learning algorithm",
    )
    _sha(formal.get("learning_algorithm_artifact_sha256"), "learning algorithm artifact")
    if (
        formal.get("old_oof_applies_to_learning_algorithm_only") is not True
        or formal.get("exact_artifact_oof_available") is not False
    ):
        raise FinalCompositionContractError("formal learning algorithm interpretation drifted")

    producer = _exact_mapping(
        composition.get("producer_chain"),
        {
            "execution_manifest_sha256",
            "layer4_contract_sha256",
            "layer4_final_sha256",
            "ordered_day_count",
        },
        "producer chain",
    )
    for field in ("execution_manifest_sha256", "layer4_contract_sha256", "layer4_final_sha256"):
        _sha(producer.get(field), f"producer chain {field}")
    if producer.get("ordered_day_count") != EXPECTED_DAY_COUNT:
        raise FinalCompositionContractError("producer chain day count drifted")

    artifact = _exact_mapping(
        composition.get("exact_artifact"),
        {
            "artifact_sha256",
            "manifest_file_sha256",
            "policy_file_sha256",
            "predicate_bundle_file_sha256",
            "training_days",
            "exact_artifact_oof_available",
        },
        "final composition exact artifact",
    )
    for field in (
        "artifact_sha256",
        "manifest_file_sha256",
        "policy_file_sha256",
        "predicate_bundle_file_sha256",
    ):
        _sha(artifact.get(field), f"exact artifact {field}")
    _validate_training_days(artifact.get("training_days"))
    if artifact.get("exact_artifact_oof_available") is not False:
        raise FinalCompositionContractError("exact artifact OOF authority drifted")

    _validate_attempt(composition.get("compatible_execution_attempt"))
    _sha_map(
        composition.get("stability_wrapper_source_canonical_sha256"),
        PRE_ADMISSION_RECEIPT_ROLES,
        "stability wrapper source identities",
    )
    _sha_map(
        composition.get("current_authority_evidence"),
        (
            "runtime_regression_sha256",
            "concurrent_resource_sha256",
            "sell_54_case_sha256",
            "activation_envelope_sha256",
            "plan_sha256",
            "disabled_process_identity_sha256",
        ),
        "current authority evidence",
    )
    ordered = _validate_ordered_evidence(composition.get("ordered_evidence"))
    if composition.get("ordered_evidence_sha256") != canonical_sha256(list(ordered)):
        raise FinalCompositionContractError("ordered evidence canonical identity drifted")
    canonical = _sha(composition.get(CANONICAL_FIELD), "final composition canonical identity")
    if canonical != document_sha256(composition):
        raise FinalCompositionContractError("final composition canonical identity drifted")
    _validate_boundaries(composition)
    return dict(composition)


def ordered_evidence_by_role(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    validated = validate_final_composition_v2(payload)
    return {str(row["role"]): row for row in validated["ordered_evidence"]}
