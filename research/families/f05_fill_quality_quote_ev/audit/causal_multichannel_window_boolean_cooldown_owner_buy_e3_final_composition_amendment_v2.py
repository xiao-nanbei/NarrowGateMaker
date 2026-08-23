#!/usr/bin/env python3
"""Compose the repaired BUY E3 evidence chain without rewriting historical v1.

This amendment keeps the immutable formal/refit/artifact/parity producer chain
separate from the compatible runtime attempt that may later receive an owner
active release.  It never opens economic row files, Validation, or holdout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 as gate,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_v1 as v1,
)
from scripts import deploy_f05_buy_e3_owner_v1 as deploy
from scripts import f05_buy_e3_execution_attempt as attempt
from scripts import f05_buy_e3_final_composition_contract as composition_contract
from scripts import f05_buy_e3_stability_receipts as stability

IDENTITY: Final = composition_contract.OWNER_IDENTITY
SCHEMA_VERSION: Final = composition_contract.SCHEMA_VERSION
COMPOSITION_IDENTITY: Final = composition_contract.COMPOSITION_IDENTITY
STATUS: Final = composition_contract.STATUS
CANONICAL_FIELD: Final = composition_contract.CANONICAL_FIELD
EXPECTED_DAY_COUNT: Final = composition_contract.EXPECTED_DAY_COUNT

LAYER4_DAY_PREFIX: Final = composition_contract.LAYER4_DAY_PREFIX
WRAPPER_PREFIX: Final = composition_contract.WRAPPER_PREFIX

BASE_ROLE_ORDER: Final[tuple[str, ...]] = composition_contract.BASE_ROLE_ORDER

CURRENT_ROLE_ORDER: Final[tuple[str, ...]] = composition_contract.CURRENT_ROLE_ORDER

if tuple(attempt.PRE_ADMISSION_RECEIPT_ROLES) != composition_contract.PRE_ADMISSION_RECEIPT_ROLES:
    raise RuntimeError("final composition stability role contract drifted")

EXACT_SCHEMA_BY_ROLE: Final[dict[str, str]] = {
    "formal_buy_component_manifest": (
        "f05_full_multiscale_successor_formal_component_closeout_v1.component_artifact_manifest.v1"
    ),
    "formal_buy_component_validation": (
        "f05_full_multiscale_successor_formal_component_closeout_v1.component_validation.v1"
    ),
    "joint_closeout_manifest": f"{IDENTITY}.closeout_manifest.v1",
    "owner_decision": f"{IDENTITY}.owner_decision.v1",
    "attempt_execution_manifest": f"{IDENTITY}.execution_manifest.v1",
    "source_execution_manifest": (
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1."
        "formal_sell_only_orchestrator_v1.execution_manifest.v1"
    ),
    "cpp_builder_preflight": "f05_cpp_one_shot_real_day_all_arm_lockstep_v26.builder_preflight.v1",
    "cpp_quick_preflight": "f05_cpp_first_opportunity_all_arm_lockstep_v26.receipt.v1",
    "cpp_qualification": "f05_cpp_one_shot_real_day_all_arm_lockstep_v26.receipt.v1",
    "owner_execution_preflight": f"{IDENTITY}.execution_preflight_receipt.v1",
    "label_materialization": f"{IDENTITY}.full_development_label_materialization.v1",
    "refit_receipt": f"{IDENTITY}.refit_run_receipt.v1",
    "exact_artifact_manifest": f"{IDENTITY}.full_development_refit.v1",
    "exact_policy": f"{IDENTITY}.artifact.v1",
    "exact_predicate_bundle": f"{IDENTITY}.selected_predicate_bundle.v1",
    "parity_research_compiled": f"{IDENTITY}.parity_receipt.v1",
    "parity_development_snapshot": f"{IDENTITY}.parity_receipt.v1",
    "parity_streaming_offline": f"{IDENTITY}.parity_receipt.v1",
    "layer4_mechanics": f"{IDENTITY}.outcome_blind_mechanics_identity_receipt.v1",
    "layer4_contract": f"{IDENTITY}.layer4_lockstep_contract.v1",
    "layer4_final": f"{IDENTITY}.parity_receipt.v2",
    "compatible_execution_attempt": attempt.SCHEMA_VERSION,
    "compatible_runtime_regression": gate.COMPATIBLE_REGRESSION_SCHEMA,
    "compatible_concurrent_resource": gate.CONCURRENT_RESOURCE_SCHEMA,
    "sell_54_case": gate.SELL_PARITY_SCHEMA,
    "compatible_activation_envelope": deploy.COMPATIBLE_ACTIVATION_ENVELOPE_SCHEMA,
}

EXACT_STATUS_BY_ROLE: Final[dict[str, str]] = {
    "formal_buy_component_manifest": "formal_buy_component_manifest_bound",
    "formal_buy_component_validation": "passed_exact_component_result_report_scorecards_and_cache",
    "joint_closeout_manifest": "formal_statistics_rebuilt_owner_override_recorded",
    "owner_decision": "owner_override_recorded_artifact_not_yet_frozen",
    "attempt_execution_manifest": "pre_refit_owner_execution_bound",
    "source_execution_manifest": "pre_economic_formal_execution_bound",
    "cpp_builder_preflight": "passed_all_3516_zero_economic_builder_walk",
    "cpp_quick_preflight": "passed_first_opportunity_all_side_specific_arms_lockstep",
    "cpp_qualification": "passed_real_day_all_opportunity_all_arm_lockstep",
    "owner_execution_preflight": "owner_execution_preflight_complete",
    "label_materialization": "full_development_buy_labels_materialized",
    "refit_receipt": "owner_buy_e3_full_development_refit_complete",
    "exact_artifact_manifest": "exact_buy_e3_artifact_frozen",
    "exact_policy": "owner_refit_frozen_not_self_confirmed",
    "exact_predicate_bundle": "exact_predicate_bundle_bound",
    "parity_research_compiled": "parity_complete",
    "parity_development_snapshot": "parity_complete",
    "parity_streaming_offline": "parity_complete",
    "layer4_mechanics": "outcome_blind_mechanics_identity_materialized",
    "layer4_contract": "layer4_lockstep_contract_frozen",
    "layer4_final": "parity_complete",
    "compatible_execution_attempt": "compatible_runtime_frozen_not_activated",
    "compatible_runtime_regression": "passed",
    "compatible_concurrent_resource": "concurrent_disabled_live_benchmark_passed",
    "sell_54_case": "parity_complete",
    "compatible_activation_envelope": "compatible_activation_evidence_complete",
}

EXACT_IDENTITY_BY_ROLE: Final[dict[str, str]] = {
    "formal_buy_component_manifest": (
        "f05_full_multiscale_successor_formal_component_closeout_v1:"
        "formal_v24_buy_component_artifacts"
    ),
    "formal_buy_component_validation": (
        "f05_full_multiscale_successor_formal_component_closeout_v1:buy_component_validation"
    ),
    "joint_closeout_manifest": IDENTITY,
    "owner_decision": IDENTITY,
    "attempt_execution_manifest": IDENTITY,
    "source_execution_manifest": (
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1."
        "formal_sell_only_orchestrator_v1"
    ),
    "cpp_builder_preflight": "f05_cpp_target_predicate_builder_all_opportunity_zero_economic_v26",
    "cpp_quick_preflight": "f05_cpp_first_opportunity_all_arm_lockstep_v26",
    "cpp_qualification": "f05_cpp_one_shot_real_day_all_arm_lockstep_v26",
    "owner_execution_preflight": IDENTITY,
    "label_materialization": f"{IDENTITY}.full_development_label_materializer_v1",
    "refit_receipt": IDENTITY,
    "exact_artifact_manifest": IDENTITY,
    "exact_policy": IDENTITY,
    "exact_predicate_bundle": IDENTITY,
    "parity_research_compiled": IDENTITY,
    "parity_development_snapshot": IDENTITY,
    "parity_streaming_offline": IDENTITY,
    "layer4_mechanics": IDENTITY,
    "layer4_contract": IDENTITY,
    "layer4_final": IDENTITY,
    "compatible_execution_attempt": IDENTITY,
    "compatible_runtime_regression": IDENTITY,
    "compatible_concurrent_resource": IDENTITY,
    "sell_54_case": IDENTITY,
}

CANONICAL_FIELD_BY_ROLE: Final[dict[str, str]] = {
    "formal_buy_component_manifest": "canonical_artifact_manifest_sha256",
    "formal_buy_component_validation": "canonical_validation_receipt_sha256",
    "joint_closeout_manifest": "canonical_manifest_sha256",
    "owner_decision": "canonical_owner_decision_sha256",
    "attempt_execution_manifest": "canonical_execution_manifest_sha256",
    "source_execution_manifest": "canonical_execution_manifest_sha256",
    "cpp_builder_preflight": "canonical_receipt_sha256",
    "cpp_quick_preflight": "canonical_receipt_sha256",
    "cpp_qualification": "canonical_receipt_sha256",
    "owner_execution_preflight": "canonical_preflight_receipt_sha256",
    "label_materialization": "canonical_materialization_receipt_sha256",
    "refit_receipt": "canonical_refit_run_receipt_sha256",
    "exact_artifact_manifest": "artifact_sha256",
    "exact_policy": "canonical_sha256",
    "exact_predicate_bundle": "canonical_sha256",
    "parity_research_compiled": "canonical_receipt_sha256",
    "parity_development_snapshot": "canonical_receipt_sha256",
    "parity_streaming_offline": "canonical_receipt_sha256",
    "layer4_mechanics": "canonical_mechanics_identity_receipt_sha256",
    "layer4_contract": "canonical_contract_sha256",
    "layer4_final": "canonical_receipt_sha256",
    "compatible_execution_attempt": "canonical_execution_attempt_sha256",
    "compatible_runtime_regression": "canonical_receipt_sha256",
    "compatible_concurrent_resource": "canonical_resource_receipt_sha256",
    "sell_54_case": "canonical_receipt_sha256",
    "compatible_activation_envelope": "canonical_activation_envelope_sha256",
}

STATIC_STATUS_ROLES: Final = frozenset({"formal_buy_component_manifest", "exact_predicate_bundle"})

FALSE_BOUNDARY_KEYS: Final = frozenset(
    {
        "research_authorized",
        "action_authorized",
        "live_authorized",
        "validation_read",
        "validation_accessed",
        "sealed_holdout_read",
        "sealed_holdout_accessed",
        "economic_values_read",
        "economic_outcomes_present",
        "economic_outcomes_read",
        "economic_values_materialized_by_replay",
        "economic_values_exposed",
        "economic_values_persisted",
        "economic_values_persisted_in_receipt",
        "economic_values_used_for_selection",
        "new_economic_arm_run",
        "economic_arms_run",
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

OUTPUT_EVIDENCE_BOUNDARY: Final = dict(composition_contract.OUTPUT_EVIDENCE_BOUNDARY)

OUTPUT_PERMISSIONS: Final = dict(composition_contract.OUTPUT_PERMISSIONS)

_SHA_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_DAY_RE: Final = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MAX_JSON_BYTES: Final = 64 << 20


class FinalCompositionAmendmentError(RuntimeError):
    """Raised when any v2 composition dependency fails closed."""


# The execution-attempt validator historically catches this public name.  Keep
# the alias while the v2 amendment retains the more specific internal class.
FinalCompositionError = FinalCompositionAmendmentError


@dataclass(frozen=True, slots=True)
class CompositionInputs:
    formal_buy_component_manifest: Path
    formal_buy_component_validation: Path
    joint_closeout_manifest: Path
    owner_decision: Path
    attempt_execution_manifest: Path
    source_execution_manifest: Path
    cpp_builder_preflight: Path
    cpp_quick_preflight: Path
    cpp_qualification: Path
    owner_execution_preflight: Path
    label_materialization: Path
    refit_receipt: Path
    exact_artifact_manifest: Path
    exact_policy: Path
    exact_predicate_bundle: Path
    parity_research_compiled: Path
    parity_development_snapshot: Path
    parity_streaming_offline: Path
    layer4_mechanics: Path
    layer4_contract: Path
    layer4_day_receipts: tuple[Path, ...]
    layer4_final: Path
    compatible_execution_attempt: Path
    stability_wrappers: Mapping[str, Path]
    compatible_runtime_regression: Path
    compatible_concurrent_resource: Path
    sell_54_case: Path
    compatible_activation_envelope: Path


@dataclass(frozen=True, slots=True)
class _JsonRecord:
    path: Path
    payload: dict[str, Any]
    raw: bytes
    metadata: os.stat_result

    @property
    def file_sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()

    @property
    def file_identity(self) -> tuple[int, int]:
        return (self.metadata.st_dev, self.metadata.st_ino)


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
        raise FinalCompositionAmendmentError("document is not canonical ASCII JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def _reject_constant(value: str) -> Any:
    raise FinalCompositionAmendmentError(f"non-finite JSON constant is forbidden: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FinalCompositionAmendmentError(f"duplicate JSON key is forbidden: {key}")
        result[key] = value
    return result


def _parse_json_bytes(raw: bytes, label: str) -> dict[str, Any]:
    try:
        decoded = raw.decode("utf-8")
        payload = json.loads(
            decoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FinalCompositionAmendmentError(f"{label} is not strict JSON") from exc
    if not isinstance(payload, dict):
        raise FinalCompositionAmendmentError(f"{label} must be a JSON object")
    return payload


def _stable_stat(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _canonical_root(path: Path, label: str) -> Path:
    candidate = path.expanduser().absolute()
    try:
        metadata = candidate.lstat()
    except OSError as exc:
        raise FinalCompositionAmendmentError(f"{label} is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise FinalCompositionAmendmentError(f"{label} must be a real directory")
    if candidate.resolve(strict=True) != candidate:
        raise FinalCompositionAmendmentError(f"{label} is not a canonical path")
    return candidate


def _relative_parts(path: Path, root: Path, label: str) -> tuple[str, ...]:
    candidate = path.expanduser().absolute()
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise FinalCompositionAmendmentError(f"{label} escapes the evidence root") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise FinalCompositionAmendmentError(f"{label} path is not canonical")
    return relative.parts


def _read_private_json(path: Path, *, root: Path, label: str) -> _JsonRecord:
    parts = _relative_parts(path, root, label)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(root, directory_flags)
    descriptor = -1
    try:
        for part in parts[:-1]:
            next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        descriptor = os.open(parts[-1], file_flags, dir_fd=directory_fd)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > _MAX_JSON_BYTES
        ):
            raise FinalCompositionAmendmentError(
                f"{label} must be an owner-only 0600 single-link regular file"
            )
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1 << 20))
            if not chunk:
                raise FinalCompositionAmendmentError(f"{label} was truncated during read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise FinalCompositionAmendmentError(f"{label} grew during read")
        raw = b"".join(chunks)
        payload = _parse_json_bytes(raw, label)
        after = os.fstat(descriptor)
        if _stable_stat(before) != _stable_stat(after):
            raise FinalCompositionAmendmentError(f"{label} changed during read")
        canonical_path = root.joinpath(*parts)
        return _JsonRecord(canonical_path, payload, raw, after)
    except OSError as exc:
        raise FinalCompositionAmendmentError(f"{label} cannot be opened safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory_fd)


def _require_sha(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA_RE.fullmatch(normalized) is None:
        raise FinalCompositionAmendmentError(f"{label} is not a SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value)
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise FinalCompositionAmendmentError(f"{label} is not a Git object id")
    return normalized


def _require_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise FinalCompositionAmendmentError(f"{label} drifted")


def _field(payload: Mapping[str, Any], *path: str) -> Any:
    current: Any = payload
    for part in path:
        if not isinstance(current, Mapping) or part not in current:
            raise FinalCompositionAmendmentError(f"missing field: {'.'.join(path)}")
        current = current[part]
    return current


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


def _validate_boundaries(payload: Mapping[str, Any], role: str) -> None:
    for mapping in _walk_mappings(payload):
        for key in FALSE_BOUNDARY_KEYS:
            if key in mapping and mapping[key] is not False:
                raise FinalCompositionAmendmentError(f"{role}.{key} must remain false")
        if mapping.get("research_supported") not in {None, False}:
            raise FinalCompositionAmendmentError(f"{role}.research_supported must remain false")


def _role_schema(role: str) -> str:
    if role.startswith(LAYER4_DAY_PREFIX):
        return f"{IDENTITY}.repeated_policy_lockstep_day.v2"
    if role.startswith(WRAPPER_PREFIX):
        return attempt.PRE_ADMISSION_RECEIPT_WRAPPER_SCHEMA
    try:
        return EXACT_SCHEMA_BY_ROLE[role]
    except KeyError as exc:
        raise FinalCompositionAmendmentError(f"unknown evidence role: {role}") from exc


def _role_status(role: str, payload: Mapping[str, Any]) -> tuple[str, str]:
    if role.startswith(LAYER4_DAY_PREFIX):
        expected = "day_lockstep_complete"
    elif role.startswith(WRAPPER_PREFIX):
        expected = attempt.PRE_ADMISSION_RECEIPT_WRAPPER_STATUS
    else:
        expected = EXACT_STATUS_BY_ROLE[role]
    if role in STATIC_STATUS_ROLES:
        if "status" in payload:
            raise FinalCompositionAmendmentError(f"{role} must use its role-contract status")
        return expected, "role_contract"
    if payload.get("status") != expected:
        raise FinalCompositionAmendmentError(f"{role} status is not exact")
    return expected, "source_field"


def _canonical_field(role: str) -> str:
    if role.startswith(LAYER4_DAY_PREFIX):
        return "canonical_day_receipt_sha256"
    if role.startswith(WRAPPER_PREFIX):
        return "canonical_receipt_sha256"
    return CANONICAL_FIELD_BY_ROLE[role]


def _validate_role_record(role: str, record: _JsonRecord) -> dict[str, Any]:
    payload = record.payload
    if payload.get("schema_version") != _role_schema(role):
        raise FinalCompositionAmendmentError(f"{role} schema is not exact")
    status, status_source = _role_status(role, payload)
    field = _canonical_field(role)
    if set(
        name for name in payload if name.startswith("canonical_") and name.endswith("sha256")
    ) - {field}:
        raise FinalCompositionAmendmentError(f"{role} contains an unadmitted canonical field")
    canonical = _require_sha(payload.get(field), f"{role}.{field}")
    if document_sha256(payload, field) != canonical:
        raise FinalCompositionAmendmentError(f"{role} canonical identity drifted")
    _validate_boundaries(payload, role)
    identity = payload.get("identity")
    if role == "compatible_activation_envelope":
        if identity is not None:
            raise FinalCompositionAmendmentError("activation envelope must not invent an identity")
        identity = "f05_buy_e3_compatible_activation_envelope"
    else:
        expected_identity = (
            IDENTITY
            if role.startswith(LAYER4_DAY_PREFIX) or role.startswith(WRAPPER_PREFIX)
            else EXACT_IDENTITY_BY_ROLE[role]
        )
        if identity != expected_identity:
            raise FinalCompositionAmendmentError(f"{role} identity is not exact")
    return {
        "role": role,
        "path": record.path,
        "file_sha256": record.file_sha256,
        "size_bytes": len(record.raw),
        "mode": "0600",
        "device": record.metadata.st_dev,
        "inode": record.metadata.st_ino,
        "schema_version": payload["schema_version"],
        "identity": identity,
        "status": status,
        "status_source": status_source,
        "canonical_field": field,
        "canonical_sha256": canonical,
    }


def _ordered_roles(days: tuple[str, ...]) -> tuple[str, ...]:
    return (
        *BASE_ROLE_ORDER,
        *(f"{LAYER4_DAY_PREFIX}{index:02d}" for index in range(len(days))),
        "layer4_final",
        *CURRENT_ROLE_ORDER,
    )


def _input_role_paths(inputs: CompositionInputs) -> dict[str, Path]:
    if len(inputs.layer4_day_receipts) != EXPECTED_DAY_COUNT:
        raise FinalCompositionAmendmentError("Layer4 requires exactly 30 ordered day receipts")
    if set(inputs.stability_wrappers) != set(attempt.PRE_ADMISSION_RECEIPT_ROLES):
        raise FinalCompositionAmendmentError("the nine stability wrapper roles are not exact")
    paths = {role: Path(getattr(inputs, role)) for role in BASE_ROLE_ORDER}
    for index, path in enumerate(inputs.layer4_day_receipts):
        paths[f"{LAYER4_DAY_PREFIX}{index:02d}"] = Path(path)
    paths["layer4_final"] = Path(inputs.layer4_final)
    paths["compatible_execution_attempt"] = Path(inputs.compatible_execution_attempt)
    for role in attempt.PRE_ADMISSION_RECEIPT_ROLES:
        paths[f"{WRAPPER_PREFIX}{role}"] = Path(inputs.stability_wrappers[role])
    paths["compatible_runtime_regression"] = Path(inputs.compatible_runtime_regression)
    paths["compatible_concurrent_resource"] = Path(inputs.compatible_concurrent_resource)
    paths["sell_54_case"] = Path(inputs.sell_54_case)
    paths["compatible_activation_envelope"] = Path(inputs.compatible_activation_envelope)
    expected = set(_ordered_roles(tuple("x" for _ in range(EXPECTED_DAY_COUNT))))
    if set(paths) != expected:
        raise FinalCompositionAmendmentError("composition role set is missing or contains extras")
    return paths


def _load_role_records(
    *, root: Path, role_paths: Mapping[str, Path]
) -> tuple[dict[str, _JsonRecord], dict[str, dict[str, Any]]]:
    records: dict[str, _JsonRecord] = {}
    bindings: dict[str, dict[str, Any]] = {}
    seen: set[tuple[int, int]] = set()
    for role, path in role_paths.items():
        record = _read_private_json(path, root=root, label=role)
        if record.file_identity in seen:
            raise FinalCompositionAmendmentError("one inode was assigned to multiple roles")
        seen.add(record.file_identity)
        records[role] = record
        bindings[role] = _validate_role_record(role, record)
    return records, bindings


def _legacy_bindings(
    bindings: Mapping[str, Mapping[str, Any]], *, root: Path
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for role, binding in bindings.items():
        if (
            role not in BASE_ROLE_ORDER
            and not role.startswith(LAYER4_DAY_PREFIX)
            and role != ("layer4_final")
        ):
            continue
        result[role] = {
            "role": role,
            "path": Path(binding["path"]).relative_to(root).as_posix(),
            "file_sha256": binding["file_sha256"],
            "size_bytes": binding["size_bytes"],
            "mode": "0600",
            "schema_version": binding["schema_version"],
            "identity": binding["identity"],
            "status": binding["status"],
            "status_source": binding["status_source"],
            "canonical_field": binding["canonical_field"],
            "canonical_sha256": binding["canonical_sha256"],
        }
    return result


def _legacy_documents(records: Mapping[str, _JsonRecord]) -> dict[str, dict[str, Any]]:
    return {
        role: record.payload
        for role, record in records.items()
        if role in BASE_ROLE_ORDER or role.startswith(LAYER4_DAY_PREFIX) or role == "layer4_final"
    }


def _validate_formal_artifact_chain(
    records: Mapping[str, _JsonRecord],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    root: Path,
) -> tuple[str, str, str, str, str, tuple[str, ...]]:
    documents = _legacy_documents(records)
    legacy_bindings = _legacy_bindings(bindings, root=root)
    try:
        learning_sha = v1._validate_formal_chain(documents, legacy_bindings)  # noqa: SLF001
        execution_sha, qualification_sha, preflight_sha = v1._validate_execution_chain(  # noqa: SLF001
            documents,
            legacy_bindings,
        )
        artifact_sha, days = v1._validate_refit_and_artifact_chain(  # noqa: SLF001
            documents,
            legacy_bindings,
            execution_sha=execution_sha,
            qualification_sha=qualification_sha,
            preflight_sha=preflight_sha,
        )
    except v1.FinalCompositionError as exc:
        raise FinalCompositionAmendmentError(f"historical formal chain failed: {exc}") from exc
    if len(days) != EXPECTED_DAY_COUNT:
        raise FinalCompositionAmendmentError("artifact training-day count drifted")
    return learning_sha, execution_sha, qualification_sha, preflight_sha, artifact_sha, days


def _binding_file_record(
    binding: Any,
    *,
    root: Path,
    label: str,
    seen_nested: set[tuple[int, int]],
) -> _JsonRecord:
    if not isinstance(binding, Mapping) or set(binding) != {
        "path",
        "file_sha256",
        "size_bytes",
        "mode",
    }:
        raise FinalCompositionAmendmentError(f"{label} file binding is malformed")
    if binding.get("mode") != "0600":
        raise FinalCompositionAmendmentError(f"{label} mode drifted")
    record = _read_private_json(Path(str(binding.get("path", ""))), root=root, label=label)
    if (
        binding.get("file_sha256") != record.file_sha256
        or type(binding.get("size_bytes")) is not int
        or binding.get("size_bytes") != len(record.raw)
    ):
        raise FinalCompositionAmendmentError(f"{label} file identity drifted")
    if record.file_identity in seen_nested:
        raise FinalCompositionAmendmentError("one nested evidence inode was reused")
    seen_nested.add(record.file_identity)
    return record


def _referenced_document(
    binding: Any,
    *,
    root: Path,
    label: str,
    seen_nested: set[tuple[int, int]],
) -> _JsonRecord:
    if not isinstance(binding, Mapping) or set(binding) != {
        "file",
        "schema_version",
        "identity",
        "canonical_field",
        "canonical_sha256",
    }:
        raise FinalCompositionAmendmentError(f"{label} document binding is malformed")
    record = _binding_file_record(binding["file"], root=root, label=label, seen_nested=seen_nested)
    field = str(binding.get("canonical_field", ""))
    if (
        record.payload.get("schema_version") != binding.get("schema_version")
        or record.payload.get("identity") != binding.get("identity")
        or record.payload.get(field) != _require_sha(binding.get("canonical_sha256"), label)
        or document_sha256(record.payload, field) != record.payload.get(field)
    ):
        raise FinalCompositionAmendmentError(f"{label} document identity drifted")
    return record


def _validate_layer4_chain(
    records: Mapping[str, _JsonRecord],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    root: Path,
    learning_sha: str,
    producer_execution_sha: str,
    artifact_sha: str,
    days: tuple[str, ...],
) -> str:
    artifact_files = {
        "manifest": bindings["exact_artifact_manifest"]["file_sha256"],
        "policy": bindings["exact_policy"]["file_sha256"],
        "predicate_bundle": bindings["exact_predicate_bundle"]["file_sha256"],
    }
    for role, layer, mismatch_fields in (
        ("parity_research_compiled", "research_compiled", ("mismatch_count",)),
        (
            "parity_development_snapshot",
            "development_snapshot",
            ("predicate_projection_mismatch_count", "action_duration_mismatch_count"),
        ),
        ("parity_streaming_offline", "streaming_offline", ("feature_mismatch_count",)),
    ):
        payload = records[role].payload
        _require_equal(payload.get("layer"), layer, f"{role} layer")
        _require_equal(payload.get("artifact_sha256"), artifact_sha, f"{role} artifact")
        _require_equal(
            payload.get("artifact_manifest_file_sha256"), artifact_files["manifest"], role
        )
        _require_equal(payload.get("policy_file_sha256"), artifact_files["policy"], role)
        _require_equal(
            payload.get("predicate_bundle_file_sha256"), artifact_files["predicate_bundle"], role
        )
        for field in mismatch_fields:
            _require_equal(_field(payload, "evidence", field), 0, f"{role}.{field}")

    mechanics = records["layer4_mechanics"].payload
    _require_equal(
        mechanics.get("schema_amendment"),
        f"{IDENTITY}.layer4_receipt_binding_amendment.v2",
        "Layer4 mechanics amendment",
    )
    _require_equal(mechanics.get("economic_outcomes_present"), False, "mechanics outcomes")
    body = mechanics.get("mechanics_body")
    if not isinstance(body, Mapping) or set(body) != v1._MECHANICS_BODY_FIELDS:  # noqa: SLF001
        raise FinalCompositionAmendmentError("Layer4 mechanics body is malformed")
    _require_equal(body.get("economic_outcomes_present"), False, "mechanics body outcomes")
    mechanics_body_sha = _require_sha(
        mechanics.get("mechanics_receipt_sha256"), "Layer4 mechanics body"
    )
    _require_equal(canonical_sha256(body), mechanics_body_sha, "Layer4 mechanics body")
    _require_equal(tuple(body.get("selected_days", ())), days, "Layer4 mechanics days")
    seen_nested: set[tuple[int, int]] = set()
    owner_binding = mechanics.get("owner_execution_attempt")
    if not isinstance(owner_binding, Mapping):
        raise FinalCompositionAmendmentError("mechanics producer binding is missing")
    owner_record = _binding_file_record(
        owner_binding.get("manifest"),
        root=root,
        label="mechanics producer manifest",
        seen_nested=seen_nested,
    )
    _require_equal(
        owner_record.payload,
        records["attempt_execution_manifest"].payload,
        "mechanics producer document",
    )
    _require_equal(
        owner_binding.get("canonical_execution_manifest_sha256"),
        producer_execution_sha,
        "mechanics producer canonical identity",
    )
    _require_equal(
        owner_binding.get("execution_commit"),
        owner_record.payload.get("public_base_commit"),
        "mechanics producer commit",
    )
    _require_equal(
        owner_binding.get("annotated_tag"),
        owner_record.payload.get("annotated_tag"),
        "mechanics producer tag",
    )

    source_identity = mechanics.get("source_identity")
    expected_source_keys = {
        "source_execution_manifest",
        "source_manifest",
        "panel_manifest",
        "outcome_blind_predicate_bundle",
        "panel_file_sha256",
        "fold_manifest_sha256",
        "nested_fold_manifest_sha256",
    }
    if not isinstance(source_identity, Mapping) or set(source_identity) != expected_source_keys:
        raise FinalCompositionAmendmentError("mechanics source identity is malformed")
    source_execution = _referenced_document(
        source_identity["source_execution_manifest"],
        root=root,
        label="mechanics source execution",
        seen_nested=seen_nested,
    )
    source_manifest = _referenced_document(
        source_identity["source_manifest"],
        root=root,
        label="mechanics source manifest",
        seen_nested=seen_nested,
    )
    panel_manifest = _referenced_document(
        source_identity["panel_manifest"],
        root=root,
        label="mechanics panel manifest",
        seen_nested=seen_nested,
    )
    predicate_source = _referenced_document(
        source_identity["outcome_blind_predicate_bundle"],
        root=root,
        label="mechanics predicate source",
        seen_nested=seen_nested,
    )
    _require_equal(
        source_execution.payload,
        records["source_execution_manifest"].payload,
        "mechanics source execution document",
    )
    producer_bindings = owner_record.payload.get("bindings")
    if not isinstance(producer_bindings, Mapping):
        raise FinalCompositionAmendmentError("producer source bindings are missing")
    for producer_role, source_role in (
        ("source_execution_manifest", "source_execution_manifest"),
        ("source_manifest", "source_manifest"),
        ("panel_manifest", "panel_manifest"),
        ("outcome_blind_2025_predicate_bundle", "outcome_blind_predicate_bundle"),
    ):
        producer_file = producer_bindings.get(producer_role)
        source_file = source_identity[source_role].get("file")
        if not isinstance(producer_file, Mapping) or not isinstance(source_file, Mapping):
            raise FinalCompositionAmendmentError("mechanics producer/source binding is missing")
        _require_equal(
            producer_file.get("sha256"),
            source_file.get("file_sha256"),
            f"mechanics source {producer_role}",
        )
    fold_sha = _require_sha(source_identity.get("fold_manifest_sha256"), "fold manifest")
    nested_fold_sha = _require_sha(
        source_identity.get("nested_fold_manifest_sha256"), "nested-fold manifest"
    )
    for payload, label in (
        (owner_record.payload, "producer"),
        (source_execution.payload, "source execution"),
    ):
        _require_equal(payload.get("fold_manifest_sha256"), fold_sha, f"{label} fold")
        _require_equal(
            payload.get("nested_fold_manifest_sha256"), nested_fold_sha, f"{label} nested fold"
        )
    _require_equal(tuple(source_manifest.payload.get("selected_days", ())), days, "source days")
    _require_equal(tuple(panel_manifest.payload.get("selected_days", ())), days, "panel days")
    _require_equal(panel_manifest.payload.get("economic_outcomes_present"), False, "panel outcomes")
    panel_files = panel_manifest.payload.get("files")
    body_files = body.get("file_sha256")
    if not isinstance(panel_files, Mapping) or not isinstance(body_files, Mapping):
        raise FinalCompositionAmendmentError("mechanics panel files are missing")
    expected_panel_files = {
        str(role): _require_sha(row.get("sha256"), f"panel {role}")
        for role, row in panel_files.items()
        if isinstance(row, Mapping)
    }
    _require_equal(dict(body_files), expected_panel_files, "mechanics body panel files")
    _require_equal(
        source_identity.get("panel_file_sha256"), expected_panel_files, "mechanics panel files"
    )
    formal_bindings = body.get("bindings")
    expected_formal_keys = {
        "execution_manifest_sha256",
        "source_manifest_sha256",
        "panel_manifest_sha256",
        "fold_manifest_sha256",
        "nested_fold_manifest_sha256",
        "exact_owner_policy_sha256",
        "exact_owner_predicate_bundle_sha256",
        "exact_owner_private_config_sha256",
    }
    if not isinstance(formal_bindings, Mapping) or set(formal_bindings) != expected_formal_keys:
        raise FinalCompositionAmendmentError("mechanics formal bindings are malformed")
    expected_formal = {
        "execution_manifest_sha256": producer_execution_sha,
        "source_manifest_sha256": source_identity["source_manifest"]["canonical_sha256"],
        "panel_manifest_sha256": source_identity["panel_manifest"]["canonical_sha256"],
        "fold_manifest_sha256": fold_sha,
        "nested_fold_manifest_sha256": nested_fold_sha,
        "exact_owner_policy_sha256": panel_manifest.payload.get(
            "exact_current_owner_policy_sha256"
        ),
        "exact_owner_predicate_bundle_sha256": panel_manifest.payload.get(
            "exact_current_predicate_bundle_sha256"
        ),
        "exact_owner_private_config_sha256": panel_manifest.payload.get(
            "exact_current_private_config_sha256"
        ),
    }
    for key, value in expected_formal.items():
        expected_formal[key] = _require_sha(value, f"mechanics {key}")
    _require_equal(dict(formal_bindings), expected_formal, "mechanics formal bindings")
    _require_equal(
        expected_formal["exact_owner_predicate_bundle_sha256"],
        predicate_source.file_sha256,
        "mechanics predicate source file",
    )

    contract = records["layer4_contract"].payload
    _require_equal(
        contract.get("schema_amendment"),
        f"{IDENTITY}.layer4_receipt_binding_amendment.v2",
        "Layer4 contract amendment",
    )
    mechanics_binding = contract.get("mechanics_identity_receipt")
    if not isinstance(mechanics_binding, Mapping) or set(mechanics_binding) != {
        "receipt",
        "schema_version",
        "canonical_receipt_sha256",
        "mechanics_receipt_sha256",
    }:
        raise FinalCompositionAmendmentError("Layer4 mechanics binding is malformed")
    mechanics_file = mechanics_binding.get("receipt")
    if not isinstance(mechanics_file, Mapping) or set(mechanics_file) != {
        "path",
        "file_sha256",
        "size_bytes",
        "mode",
    }:
        raise FinalCompositionAmendmentError("Layer4 mechanics file binding is missing")
    _require_equal(
        Path(str(mechanics_file.get("path", ""))).expanduser().absolute(),
        records["layer4_mechanics"].path,
        "mechanics path",
    )
    _require_equal(mechanics_file.get("mode"), "0600", "mechanics mode")
    _require_equal(
        mechanics_file.get("file_sha256"), records["layer4_mechanics"].file_sha256, "mechanics file"
    )
    _require_equal(
        mechanics_file.get("size_bytes"), len(records["layer4_mechanics"].raw), "mechanics size"
    )
    _require_equal(
        mechanics_binding.get("canonical_receipt_sha256"),
        bindings["layer4_mechanics"]["canonical_sha256"],
        "mechanics canonical receipt",
    )
    _require_equal(
        mechanics_binding.get("mechanics_receipt_sha256"), mechanics_body_sha, "mechanics body"
    )
    contract_sha = bindings["layer4_contract"]["canonical_sha256"]
    _require_equal(
        _field(contract, "execution_attempt", "canonical_execution_manifest_sha256"),
        producer_execution_sha,
        "Layer4 producer execution",
    )
    _require_equal(
        contract.get("learning_algorithm_artifact_sha256"), learning_sha, "Layer4 learning"
    )
    formal_component = records["formal_buy_component_manifest"].payload
    _require_equal(
        _field(contract, "formal_learning_algorithm", "manifest", "file_sha256"),
        records["formal_buy_component_manifest"].file_sha256,
        "Layer4 formal manifest file",
    )
    _require_equal(
        _field(
            contract,
            "formal_learning_algorithm",
            "formal_v24_execution_manifest_sha256",
        ),
        formal_component.get("source_execution_manifest_sha256"),
        "Layer4 formal execution",
    )
    _require_equal(
        _field(contract, "formal_learning_algorithm", "component_result_canonical_sha256"),
        formal_component.get("component_result_canonical_sha256"),
        "Layer4 component result",
    )
    _require_equal(
        _field(
            contract,
            "formal_learning_algorithm",
            "nested_oof_artifact_manifest_canonical_sha256",
        ),
        formal_component.get("nested_oof_artifact_manifest_canonical_sha256"),
        "Layer4 nested OOF artifact",
    )
    for path in (
        ("source_predicate_bundle", "bundle", "file_sha256"),
        ("parity_source", "amendment_file_sha256"),
        ("parity_source", "v1_parity_file_sha256"),
    ):
        _require_sha(_field(contract, *path), f"Layer4 {'.'.join(path)}")
    _require_equal(
        _field(contract, "exact_artifact", "artifact_sha256"), artifact_sha, "Layer4 artifact"
    )
    for path, expected in (
        (
            ("exact_artifact", "artifact_manifest", "file_sha256"),
            artifact_files["manifest"],
        ),
        (("exact_artifact", "policy", "file_sha256"), artifact_files["policy"]),
        (
            ("exact_artifact", "predicate_bundle", "file_sha256"),
            artifact_files["predicate_bundle"],
        ),
    ):
        _require_equal(_field(contract, *path), expected, f"Layer4 {'.'.join(path)}")
    _require_equal(tuple(contract.get("ordered_development_days", ())), days, "Layer4 days")
    final_day_bindings: list[dict[str, str]] = []
    for index, day in enumerate(days):
        role = f"{LAYER4_DAY_PREFIX}{index:02d}"
        payload = records[role].payload
        _require_equal(
            payload.get("schema_amendment"),
            f"{IDENTITY}.layer4_receipt_binding_amendment.v2",
            f"Layer4 {day} amendment",
        )
        for field, expected in (
            ("utc_day", day),
            ("layer4_lockstep_contract_sha256", contract_sha),
            ("layer4_lockstep_contract_file_sha256", records["layer4_contract"].file_sha256),
            ("learning_algorithm_artifact_sha256", learning_sha),
            ("artifact_sha256", artifact_sha),
            ("artifact_manifest_file_sha256", artifact_files["manifest"]),
            ("policy_file_sha256", artifact_files["policy"]),
            ("predicate_bundle_file_sha256", artifact_files["predicate_bundle"]),
        ):
            _require_equal(payload.get(field), expected, f"Layer4 {day} {field}")
        _require_equal(
            payload.get("mechanics_identity_receipt"), mechanics_binding, "day mechanics"
        )
        _require_equal(_field(payload, "lockstep", "mismatch_count"), 0, "day mismatch")
        final_day_bindings.append(
            {
                "utc_day": day,
                "file_name": f"{day}.json",
                "file_sha256": records[role].file_sha256,
                "canonical_day_receipt_sha256": bindings[role]["canonical_sha256"],
            }
        )
    final = records["layer4_final"].payload
    _require_equal(
        final.get("schema_amendment"),
        f"{IDENTITY}.layer4_receipt_binding_amendment.v2",
        "Layer4 final amendment",
    )
    for field, expected in (
        ("layer", "repeated_policy_lockstep"),
        ("layer4_lockstep_contract_sha256", contract_sha),
        ("layer4_lockstep_contract_file_sha256", records["layer4_contract"].file_sha256),
        ("learning_algorithm_artifact_sha256", learning_sha),
        ("artifact_sha256", artifact_sha),
        ("artifact_manifest_file_sha256", artifact_files["manifest"]),
        ("policy_file_sha256", artifact_files["policy"]),
        ("predicate_bundle_file_sha256", artifact_files["predicate_bundle"]),
    ):
        _require_equal(final.get(field), expected, f"Layer4 final {field}")
    _require_equal(final.get("mechanics_identity_receipt"), mechanics_binding, "final mechanics")
    _require_equal(_field(final, "evidence", "day_count"), EXPECTED_DAY_COUNT, "final day count")
    _require_equal(_field(final, "evidence", "day_receipts"), final_day_bindings, "final days")
    _require_equal(_field(final, "evidence", "mismatch_count"), 0, "final mismatch")
    return str(contract_sha)


def _record_unchanged(
    before: _JsonRecord,
    *,
    root: Path,
    label: str,
    returned_payload: Mapping[str, Any] | None,
) -> None:
    after = _read_private_json(before.path, root=root, label=label)
    if (
        before.raw != after.raw
        or before.file_identity != after.file_identity
        or _stable_stat(before.metadata) != _stable_stat(after.metadata)
        or (returned_payload is not None and dict(returned_payload) != before.payload)
    ):
        raise FinalCompositionAmendmentError(f"{label} changed during independent validation")


def _attempt_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    runtime = payload.get("runtime_execution")
    evidence = payload.get("pre_admission_evidence")
    if not isinstance(runtime, Mapping) or not isinstance(evidence, Mapping):
        raise FinalCompositionAmendmentError("compatible attempt identity is incomplete")
    if set(evidence) != set(attempt.PRE_ADMISSION_RECEIPT_ROLES):
        raise FinalCompositionAmendmentError("compatible attempt wrapper role set drifted")
    wrapper_map = {
        role: _require_sha(evidence[role].get("canonical_sha256"), f"attempt wrapper {role}")
        for role in attempt.PRE_ADMISSION_RECEIPT_ROLES
        if isinstance(evidence.get(role), Mapping)
    }
    if set(wrapper_map) != set(attempt.PRE_ADMISSION_RECEIPT_ROLES):
        raise FinalCompositionAmendmentError("compatible attempt wrapper binding is missing")
    tag = runtime.get("annotated_tag")
    if not isinstance(tag, str) or not tag or any(character.isspace() for character in tag):
        raise FinalCompositionAmendmentError("compatible attempt tag is malformed")
    return {
        "schema_version": attempt.SCHEMA_VERSION,
        "identity": IDENTITY,
        "attempt_id": str(payload.get("attempt_id", "")),
        "canonical_execution_attempt_sha256": _require_sha(
            payload.get("canonical_execution_attempt_sha256"), "compatible attempt"
        ),
        "execution_commit": _require_git_sha(runtime.get("execution_commit"), "execution commit"),
        "execution_tree": _require_git_sha(runtime.get("execution_tree"), "execution tree"),
        "annotated_tag": tag,
        "annotated_tag_object": _require_git_sha(
            runtime.get("annotated_tag_object"), "execution tag object"
        ),
        "pre_admission_wrapper_canonical_sha256": wrapper_map,
    }


def _compare_attempt_wrapper_binding(
    raw: Any, record: _JsonRecord, binding: Mapping[str, Any], role: str
) -> None:
    if not isinstance(raw, Mapping):
        raise FinalCompositionAmendmentError(f"attempt wrapper is missing: {role}")
    expected_fields = {
        "path",
        "file_sha256",
        "size_bytes",
        "mode",
        "schema_version",
        "identity",
        "status",
        "canonical_field",
        "canonical_sha256",
    }
    if set(raw) != expected_fields:
        raise FinalCompositionAmendmentError(f"attempt wrapper fields drifted: {role}")
    if (
        Path(str(raw.get("path", ""))).expanduser().absolute() != record.path
        or raw.get("file_sha256") != record.file_sha256
        or raw.get("size_bytes") != len(record.raw)
        or raw.get("mode") != "0600"
        or raw.get("schema_version") != binding["schema_version"]
        or raw.get("identity") != binding["identity"]
        or raw.get("status") != binding["status"]
        or raw.get("canonical_field") != binding["canonical_field"]
        or raw.get("canonical_sha256") != binding["canonical_sha256"]
    ):
        raise FinalCompositionAmendmentError(f"attempt wrapper binding drifted: {role}")


def _source_record_from_wrapper(
    wrapper: _JsonRecord, *, root: Path, role: str
) -> tuple[_JsonRecord, Mapping[str, Any]]:
    source = wrapper.payload.get("source_receipt")
    expected_fields = {
        "path",
        "file_sha256",
        "size_bytes",
        "mode",
        "schema_version",
        "identity",
        "status",
        "canonical_field",
        "canonical_sha256",
    }
    if not isinstance(source, Mapping) or set(source) != expected_fields:
        raise FinalCompositionAmendmentError(f"wrapper source binding drifted: {role}")
    record = _read_private_json(
        Path(str(source.get("path", ""))), root=root, label=f"{role} wrapper source"
    )
    field = str(source.get("canonical_field", ""))
    if (
        source.get("file_sha256") != record.file_sha256
        or source.get("size_bytes") != len(record.raw)
        or source.get("mode") != "0600"
        or source.get("schema_version") != record.payload.get("schema_version")
        or source.get("identity") != record.payload.get("identity")
        or source.get("status") != record.payload.get("status")
        or source.get("canonical_sha256") != record.payload.get(field)
        or document_sha256(record.payload, field) != record.payload.get(field)
    ):
        raise FinalCompositionAmendmentError(f"wrapper source identity drifted: {role}")
    return record, source


def _validate_current_authority_chain(
    records: Mapping[str, _JsonRecord],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    root: Path,
    repository_root: Path,
    artifact_sha: str,
) -> tuple[dict[str, Any], dict[str, str], dict[str, Any]]:
    attempt_record = records["compatible_execution_attempt"]
    try:
        validated_attempt = attempt.validate_manifest(
            attempt_record.path,
            repository_root=repository_root,
            require_current_checkout=False,
        )
    except attempt.ExecutionAttemptError as exc:
        raise FinalCompositionAmendmentError(f"compatible attempt failed: {exc}") from exc
    _record_unchanged(
        attempt_record,
        root=root,
        label="compatible execution attempt",
        returned_payload=validated_attempt,
    )
    attempt_payload = attempt_record.payload
    attempt_identity = _attempt_identity(attempt_payload)
    _require_equal(
        _field(attempt_payload, "artifact", "artifact_sha256"), artifact_sha, "attempt artifact"
    )
    for role, source_role in (
        ("manifest", "exact_artifact_manifest"),
        ("policy", "exact_policy"),
        ("predicate_bundle", "exact_predicate_bundle"),
    ):
        source = _field(attempt_payload, "artifact", "files", role)
        if not isinstance(source, Mapping):
            raise FinalCompositionAmendmentError(f"attempt artifact file is missing: {role}")
        _require_equal(
            source.get("file_sha256"), records[source_role].file_sha256, f"attempt {role}"
        )
    formal = _field(attempt_payload, "artifact", "formal_manifest")
    if not isinstance(formal, Mapping):
        raise FinalCompositionAmendmentError("attempt producer manifest binding is missing")
    _require_equal(
        formal.get("file_sha256"),
        records["attempt_execution_manifest"].file_sha256,
        "attempt producer manifest file",
    )
    _require_equal(
        formal.get("canonical_sha256"),
        bindings["attempt_execution_manifest"]["canonical_sha256"],
        "attempt producer manifest canonical",
    )
    runtime = attempt_payload["runtime_execution"]
    context = stability.StabilityContext(
        repository_root=repository_root,
        execution_commit=str(runtime["execution_commit"]),
        execution_tag=str(runtime["annotated_tag"]),
        layer4_contract_path=records["layer4_contract"].path,
        layer4_day_receipt_dir=records[f"{LAYER4_DAY_PREFIX}00"].path.parent,
    )
    wrapper_paths = {
        role: records[f"{WRAPPER_PREFIX}{role}"].path
        for role in attempt.PRE_ADMISSION_RECEIPT_ROLES
    }
    try:
        validated_wrappers = stability.validate_stability_wrappers(
            wrappers=wrapper_paths,
            context=context,
        )
    except stability.StabilityReceiptError as exc:
        raise FinalCompositionAmendmentError(f"stability wrappers failed: {exc}") from exc
    source_canonical: dict[str, str] = {}
    source_records: dict[str, _JsonRecord] = {}
    seen_sources: set[tuple[int, int]] = set()
    for role in attempt.PRE_ADMISSION_RECEIPT_ROLES:
        wrapper_role = f"{WRAPPER_PREFIX}{role}"
        wrapper_record = records[wrapper_role]
        _record_unchanged(
            wrapper_record,
            root=root,
            label=f"{role} wrapper",
            returned_payload=validated_wrappers.get(role),
        )
        _compare_attempt_wrapper_binding(
            _field(attempt_payload, "pre_admission_evidence", role),
            wrapper_record,
            bindings[wrapper_role],
            role,
        )
        source_record, source_binding = _source_record_from_wrapper(
            wrapper_record, root=root, role=role
        )
        if source_record.file_identity == wrapper_record.file_identity:
            raise FinalCompositionAmendmentError(f"{role} wrapper aliases its source")
        if source_record.file_identity in seen_sources:
            raise FinalCompositionAmendmentError("one wrapper source inode was reused")
        seen_sources.add(source_record.file_identity)
        source_records[role] = source_record
        source_canonical[role] = _require_sha(
            source_binding.get("canonical_sha256"), f"wrapper source {role}"
        )
    direct_roles = {
        "parity_layer1": "parity_research_compiled",
        "parity_layer2": "parity_development_snapshot",
        "parity_layer3": "parity_streaming_offline",
        "parity_layer4": "layer4_final",
        "sell54": "sell_54_case",
        "regression": "compatible_runtime_regression",
    }
    for wrapper_role, direct_role in direct_roles.items():
        source_record = source_records[wrapper_role]
        direct_record = records[direct_role]
        if (
            source_record.file_identity != direct_record.file_identity
            or source_record.raw != direct_record.raw
        ):
            raise FinalCompositionAmendmentError(
                f"{wrapper_role} wrapper does not bind the direct receipt"
            )

    execution_commit = str(runtime["execution_commit"])
    execution_tag = str(runtime["annotated_tag"])
    regression_record = records["compatible_runtime_regression"]
    try:
        regression = gate.validate_runtime_regression_receipt(
            regression_record.path,
            repository_root=repository_root,
            expected_artifact_sha256=artifact_sha,
            expected_execution_commit=execution_commit,
            expected_execution_tag=execution_tag,
        )
    except gate.BuyE3DeploymentGateError as exc:
        raise FinalCompositionAmendmentError(f"compatible regression failed: {exc}") from exc
    _record_unchanged(
        regression_record,
        root=root,
        label="compatible runtime regression",
        returned_payload=regression,
    )
    sell_record = records["sell_54_case"]
    artifact_files = {
        "manifest": records["exact_artifact_manifest"].file_sha256,
        "policy": records["exact_policy"].file_sha256,
        "predicate_bundle": records["exact_predicate_bundle"].file_sha256,
    }
    try:
        sell = gate.validate_sell_owner_54_case_receipt(
            sell_record.path,
            repository_root=repository_root,
            expected_artifact_sha256=artifact_sha,
            expected_artifact_files=artifact_files,
        )
    except gate.BuyE3DeploymentGateError as exc:
        raise FinalCompositionAmendmentError(f"SELL54 failed: {exc}") from exc
    _record_unchanged(
        sell_record,
        root=root,
        label="SELL54 receipt",
        returned_payload=None,
    )
    _require_equal(sell.get("file_sha256"), sell_record.file_sha256, "SELL54 file")
    _require_equal(
        sell.get("canonical_receipt_sha256"),
        bindings["sell_54_case"]["canonical_sha256"],
        "SELL54 canonical receipt",
    )
    runtime_sources = _field(attempt_payload, "runtime_sources", "files")
    if not isinstance(runtime_sources, Mapping):
        raise FinalCompositionAmendmentError("attempt runtime sources are missing")
    source_by_path = {
        str(row.get("repository_relative_path")): row.get("file_sha256")
        for row in runtime_sources.values()
        if isinstance(row, Mapping)
    }
    for path, digest in sell.get("source_files", {}).items():
        if path in source_by_path:
            _require_equal(digest, source_by_path[path], f"SELL54 runtime source {path}")

    envelope_record = records["compatible_activation_envelope"]
    envelope = envelope_record.payload
    expected_envelope_fields = {
        "schema_version",
        "status",
        "plan_sha256",
        "plan_core_sha256",
        "transaction_contract_sha256",
        "execution",
        "artifact",
        "disabled_phase_receipt",
        "concurrent_resource_receipt",
        "runtime_regression_receipt",
        "sell_54_case_receipt",
        "checks",
        "activation_contract",
        "evidence_boundary",
        "canonical_activation_envelope_sha256",
    }
    if set(envelope) != expected_envelope_fields:
        raise FinalCompositionAmendmentError("activation envelope fields drifted")
    expected_execution = {
        "execution_commit": runtime["execution_commit"],
        "execution_tree": runtime["execution_tree"],
        "annotated_tag": runtime["annotated_tag"],
        "annotated_tag_object": runtime["annotated_tag_object"],
    }
    _require_equal(envelope.get("execution"), expected_execution, "envelope execution")
    _require_equal(
        envelope.get("artifact"),
        {"artifact_sha256": artifact_sha, "files": artifact_files},
        "envelope artifact",
    )
    for field in ("plan_sha256", "plan_core_sha256", "transaction_contract_sha256"):
        _require_sha(envelope.get(field), f"envelope {field}")
    disabled = envelope.get("disabled_phase_receipt")
    disabled_fields = {
        "path",
        "file_sha256",
        "canonical_receipt_sha256",
        "plan_sha256",
        "process_identity_sha256",
        "pid",
        "pid_start_ticks",
        "config_sha256",
        "artifact_sha256",
        "runtime_code_sha256",
        "execution_commit",
        "execution_tree",
        "runtime_identity_file_sha256",
        "startup_attestation_sha256",
    }
    if not isinstance(disabled, Mapping) or set(disabled) != disabled_fields:
        raise FinalCompositionAmendmentError("envelope disabled-phase binding drifted")
    _require_equal(disabled.get("plan_sha256"), envelope.get("plan_sha256"), "disabled plan")
    _require_equal(disabled.get("artifact_sha256"), artifact_sha, "disabled artifact")
    _require_equal(disabled.get("execution_commit"), execution_commit, "disabled commit")
    _require_equal(disabled.get("execution_tree"), runtime["execution_tree"], "disabled tree")
    for field in (
        "file_sha256",
        "canonical_receipt_sha256",
        "process_identity_sha256",
        "config_sha256",
        "runtime_code_sha256",
        "runtime_identity_file_sha256",
        "startup_attestation_sha256",
    ):
        _require_sha(disabled.get(field), f"disabled {field}")
    if type(disabled.get("pid")) is not int or type(disabled.get("pid_start_ticks")) is not int:
        raise FinalCompositionAmendmentError("disabled process identity is malformed")
    resource_record = records["compatible_concurrent_resource"]
    expected_disabled_process = {
        "canonical_process_identity_sha256": disabled["process_identity_sha256"],
        "pid": disabled["pid"],
        "pid_start_ticks": disabled["pid_start_ticks"],
    }
    try:
        resource = gate.validate_concurrent_resource_receipt(
            resource_record.path,
            expected_artifact_sha256=artifact_sha,
            expected_execution_commit=execution_commit,
            expected_execution_tag=execution_tag,
            expected_disabled_process_identity=expected_disabled_process,
        )
    except gate.BuyE3DeploymentGateError as exc:
        raise FinalCompositionAmendmentError(f"concurrent resource failed: {exc}") from exc
    _record_unchanged(
        resource_record,
        root=root,
        label="concurrent resource receipt",
        returned_payload=resource,
    )

    def expected_receipt_binding(
        record: _JsonRecord, canonical: str, extra: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            "path": str(record.path),
            "file_sha256": record.file_sha256,
            "canonical_sha256": canonical,
            **dict(extra),
        }

    _require_equal(
        envelope.get("concurrent_resource_receipt"),
        expected_receipt_binding(
            resource_record,
            bindings["compatible_concurrent_resource"]["canonical_sha256"],
            {
                "disabled_process_identity_sha256": disabled["process_identity_sha256"],
                "live_pid": disabled["pid"],
            },
        ),
        "envelope resource binding",
    )
    regression_source_manifest = canonical_sha256(
        {
            "test_files": regression["test_files"],
            "runtime_sources": regression["runtime_sources"],
        }
    )
    _require_equal(
        envelope.get("runtime_regression_receipt"),
        expected_receipt_binding(
            regression_record,
            bindings["compatible_runtime_regression"]["canonical_sha256"],
            {
                "nodeid_manifest_sha256": regression["nodeid_manifest_sha256"],
                "test_source_manifest_sha256": regression_source_manifest,
            },
        ),
        "envelope regression binding",
    )
    expected_sell_binding = {
        key: value for key, value in sell.items() if key != "canonical_receipt_sha256"
    }
    expected_sell_binding["canonical_sha256"] = sell["canonical_receipt_sha256"]
    _require_equal(envelope.get("sell_54_case_receipt"), expected_sell_binding, "envelope SELL54")
    _require_equal(
        envelope.get("checks"),
        {
            "disabled_phase_complete_and_same_plan": True,
            "b0_fill_cooldown_exact_in_both_configs": True,
            "concurrent_2vcpu_2gib_resource_window_passed": True,
            "frozen_regression_nodeid_and_sources_passed": True,
            "real_sell_54_case_and_sources_passed": True,
            "no_locked_or_economic_evidence_read": True,
        },
        "envelope checks",
    )
    _require_equal(
        envelope.get("activation_contract"),
        {
            "restart_only": True,
            "same_disabled_process_required": True,
            "phase_token_still_required": True,
            "envelope_does_not_authorize_remote_mutation_by_itself": True,
        },
        "envelope activation contract",
    )
    _require_equal(
        envelope.get("evidence_boundary"),
        deploy.ACTIVATION_ENVELOPE_EVIDENCE_BOUNDARY,
        "envelope evidence boundary",
    )
    _record_unchanged(
        envelope_record,
        root=root,
        label="compatible activation envelope",
        returned_payload=envelope,
    )
    return (
        attempt_identity,
        source_canonical,
        {
            "runtime_regression_sha256": bindings["compatible_runtime_regression"][
                "canonical_sha256"
            ],
            "concurrent_resource_sha256": bindings["compatible_concurrent_resource"][
                "canonical_sha256"
            ],
            "sell_54_case_sha256": bindings["sell_54_case"]["canonical_sha256"],
            "activation_envelope_sha256": bindings["compatible_activation_envelope"][
                "canonical_sha256"
            ],
            "plan_sha256": envelope["plan_sha256"],
            "disabled_process_identity_sha256": disabled["process_identity_sha256"],
        },
    )


def _portable_binding(binding: Mapping[str, Any], *, root: Path) -> dict[str, Any]:
    result = dict(binding)
    result["path"] = Path(binding["path"]).relative_to(root).as_posix()
    return result


def _build_payload(
    *,
    records: Mapping[str, _JsonRecord],
    bindings: Mapping[str, Mapping[str, Any]],
    root: Path,
    days: tuple[str, ...],
    learning_sha: str,
    producer_execution_sha: str,
    artifact_sha: str,
    layer4_contract_sha: str,
    compatible_attempt: Mapping[str, Any],
    wrapper_source_canonical: Mapping[str, str],
    current_authority: Mapping[str, Any],
) -> dict[str, Any]:
    roles = _ordered_roles(days)
    ordered_evidence = [_portable_binding(bindings[role], root=root) for role in roles]
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": COMPOSITION_IDENTITY,
        "status": STATUS,
        "research_identity": IDENTITY,
        "amendment": {
            "preserves_v1_history": True,
            "research_identity_changed": False,
            "historical_v1_runtime_authority_reused": False,
            "ordinary_bugfix_attempt_only": True,
        },
        "formal_learning_algorithm": {
            "learning_algorithm_artifact_sha256": learning_sha,
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
        },
        "producer_chain": {
            "execution_manifest_sha256": producer_execution_sha,
            "layer4_contract_sha256": layer4_contract_sha,
            "layer4_final_sha256": bindings["layer4_final"]["canonical_sha256"],
            "ordered_day_count": EXPECTED_DAY_COUNT,
        },
        "exact_artifact": {
            "artifact_sha256": artifact_sha,
            "manifest_file_sha256": records["exact_artifact_manifest"].file_sha256,
            "policy_file_sha256": records["exact_policy"].file_sha256,
            "predicate_bundle_file_sha256": records["exact_predicate_bundle"].file_sha256,
            "training_days": list(days),
            "exact_artifact_oof_available": False,
        },
        "compatible_execution_attempt": dict(compatible_attempt),
        "stability_wrapper_source_canonical_sha256": dict(wrapper_source_canonical),
        "current_authority_evidence": dict(current_authority),
        "evidence_interpretation": {
            "research_supported": False,
            "owner_risk_accepted": True,
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
        },
        "authority": {"research": False, "action": False, "live": False},
        "evidence_boundary": dict(OUTPUT_EVIDENCE_BOUNDARY),
        "permissions": dict(OUTPUT_PERMISSIONS),
        "ordered_evidence": ordered_evidence,
        "ordered_evidence_sha256": canonical_sha256(ordered_evidence),
    }
    payload[CANONICAL_FIELD] = document_sha256(payload, CANONICAL_FIELD)
    try:
        composition_contract.validate_final_composition_v2(payload)
    except composition_contract.FinalCompositionContractError as exc:
        raise FinalCompositionAmendmentError(
            f"final composition v2 structural contract failed: {exc}"
        ) from exc
    return payload


def _validate_all(
    *,
    records: Mapping[str, _JsonRecord],
    bindings: Mapping[str, Mapping[str, Any]],
    root: Path,
    repository_root: Path,
) -> dict[str, Any]:
    (
        learning_sha,
        producer_execution_sha,
        _qualification_sha,
        _preflight_sha,
        artifact_sha,
        days,
    ) = _validate_formal_artifact_chain(records, bindings, root=root)
    layer4_contract_sha = _validate_layer4_chain(
        records,
        bindings,
        root=root,
        learning_sha=learning_sha,
        producer_execution_sha=producer_execution_sha,
        artifact_sha=artifact_sha,
        days=days,
    )
    compatible_identity, wrapper_sources, current_authority = _validate_current_authority_chain(
        records,
        bindings,
        root=root,
        repository_root=repository_root,
        artifact_sha=artifact_sha,
    )
    return _build_payload(
        records=records,
        bindings=bindings,
        root=root,
        days=days,
        learning_sha=learning_sha,
        producer_execution_sha=producer_execution_sha,
        artifact_sha=artifact_sha,
        layer4_contract_sha=layer4_contract_sha,
        compatible_attempt=compatible_identity,
        wrapper_source_canonical=wrapper_sources,
        current_authority=current_authority,
    )


def _ensure_output_parent(path: Path, *, root: Path) -> tuple[int, str, Path]:
    parts = _relative_parts(path, root, "composition output")
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    directory_fd = os.open(root, directory_flags)
    current = root
    try:
        for part in parts[:-1]:
            current = current / part
            try:
                next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            except FileNotFoundError:
                os.mkdir(part, 0o700, dir_fd=directory_fd)
                os.fsync(directory_fd)
                next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        return directory_fd, parts[-1], current / parts[-1]
    except Exception:
        os.close(directory_fd)
        raise


def _write_no_replace(path: Path, payload: Mapping[str, Any], *, root: Path) -> None:
    directory_fd, name, target = _ensure_output_parent(path, root=root)
    encoded = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    ).encode("ascii")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    created = False
    try:
        try:
            descriptor = os.open(name, flags, 0o600, dir_fd=directory_fd)
        except FileExistsError as exc:
            raise FinalCompositionAmendmentError(
                "immutable composition output already exists"
            ) from exc
        created = True
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(encoded):
            written = os.write(descriptor, encoded[offset:])
            if written <= 0:
                raise FinalCompositionAmendmentError("composition write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.fsync(directory_fd)
        observed = _read_private_json(target, root=root, label="written composition")
        if observed.raw != encoded or observed.payload != dict(payload):
            raise FinalCompositionAmendmentError("written composition bytes drifted")
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        if created:
            try:
                os.unlink(name, dir_fd=directory_fd)
                os.fsync(directory_fd)
            except FileNotFoundError:
                pass
        raise
    finally:
        os.close(directory_fd)


def compose_final_composition(
    *,
    evidence_root: Path,
    repository_root: Path,
    inputs: CompositionInputs,
    output: Path,
) -> dict[str, Any]:
    root = _canonical_root(evidence_root, "evidence root")
    repository = _canonical_root(repository_root, "repository root")
    role_paths = _input_role_paths(inputs)
    records, bindings = _load_role_records(root=root, role_paths=role_paths)
    payload = _validate_all(
        records=records,
        bindings=bindings,
        root=root,
        repository_root=repository,
    )
    _write_no_replace(output.expanduser().absolute(), payload, root=root)
    return validate_final_composition(
        evidence_root=root,
        repository_root=repository,
        receipt_path=output,
    )


def _role_paths_from_receipt(receipt: Mapping[str, Any], *, root: Path) -> dict[str, Path]:
    ordered = receipt.get("ordered_evidence")
    if not isinstance(ordered, list) or receipt.get("ordered_evidence_sha256") != canonical_sha256(
        ordered
    ):
        raise FinalCompositionAmendmentError("ordered evidence manifest drifted")
    days = _field(receipt, "exact_artifact", "training_days")
    if (
        not isinstance(days, list)
        or len(days) != EXPECTED_DAY_COUNT
        or tuple(days) != tuple(sorted(set(days)))
        or any(_DAY_RE.fullmatch(str(day)) is None for day in days)
    ):
        raise FinalCompositionAmendmentError("receipt training days drifted")
    expected_roles = _ordered_roles(tuple(str(day) for day in days))
    observed_roles = tuple(
        str(row.get("role", "")) if isinstance(row, Mapping) else "" for row in ordered
    )
    if observed_roles != expected_roles:
        raise FinalCompositionAmendmentError("receipt role set, order, or count drifted")
    paths: dict[str, Path] = {}
    seen_relative: set[str] = set()
    for row in ordered:
        if not isinstance(row, Mapping):
            raise FinalCompositionAmendmentError("receipt evidence binding is malformed")
        relative = str(row.get("path", ""))
        if not relative or Path(relative).is_absolute() or relative in seen_relative:
            raise FinalCompositionAmendmentError("receipt evidence path is invalid")
        seen_relative.add(relative)
        paths[str(row["role"])] = root / relative
    return paths


def validate_final_composition(
    *,
    evidence_root: Path,
    repository_root: Path | None = None,
    receipt_path: Path,
) -> dict[str, Any]:
    root = _canonical_root(evidence_root, "evidence root")
    repository_candidate = (
        repository_root
        if repository_root is not None
        else Path(__file__).resolve(strict=True).parents[4]
    )
    repository = _canonical_root(repository_candidate, "repository root")
    receipt_record = _read_private_json(
        receipt_path.expanduser().absolute(), root=root, label="final composition v2"
    )
    receipt = receipt_record.payload
    if (
        receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("identity") != COMPOSITION_IDENTITY
        or receipt.get("status") != STATUS
        or receipt.get(CANONICAL_FIELD) != document_sha256(receipt, CANONICAL_FIELD)
        or receipt.get("evidence_boundary") != OUTPUT_EVIDENCE_BOUNDARY
        or receipt.get("permissions") != OUTPUT_PERMISSIONS
    ):
        raise FinalCompositionAmendmentError("final composition v2 identity drifted")
    try:
        composition_contract.validate_final_composition_v2(receipt)
    except composition_contract.FinalCompositionContractError as exc:
        raise FinalCompositionAmendmentError(
            f"final composition v2 structural contract failed: {exc}"
        ) from exc
    _validate_boundaries(receipt, "final composition v2")
    role_paths = _role_paths_from_receipt(receipt, root=root)
    records, bindings = _load_role_records(root=root, role_paths=role_paths)
    expected = _validate_all(
        records=records,
        bindings=bindings,
        root=root,
        repository_root=repository,
    )
    _require_equal(receipt, expected, "final composition v2")
    return dict(receipt)


def _path(value: str) -> Path:
    return Path(value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    compose = subparsers.add_parser("compose")
    compose.add_argument("--evidence-root", type=_path, required=True)
    compose.add_argument("--repository-root", type=_path, required=True)
    compose.add_argument("--output", type=_path, required=True)
    for role in BASE_ROLE_ORDER:
        compose.add_argument(f"--{role.replace('_', '-')}", type=_path, required=True)
    compose.add_argument("--layer4-day-receipt", type=_path, action="append", required=True)
    compose.add_argument("--layer4-final", type=_path, required=True)
    compose.add_argument("--compatible-execution-attempt", type=_path, required=True)
    compose.add_argument("--stability-wrapper", action="append", required=True)
    compose.add_argument("--compatible-runtime-regression", type=_path, required=True)
    compose.add_argument("--compatible-concurrent-resource", type=_path, required=True)
    compose.add_argument("--sell-54-case", type=_path, required=True)
    compose.add_argument("--compatible-activation-envelope", type=_path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--evidence-root", type=_path, required=True)
    validate.add_argument("--repository-root", type=_path, required=True)
    validate.add_argument("--receipt", type=_path, required=True)
    return parser


def _parse_wrappers(values: Sequence[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for raw in values:
        role, separator, path = raw.partition("=")
        if not separator or role in result or role not in attempt.PRE_ADMISSION_RECEIPT_ROLES:
            raise FinalCompositionAmendmentError(f"invalid stability wrapper: {raw!r}")
        result[role] = Path(path)
    if set(result) != set(attempt.PRE_ADMISSION_RECEIPT_ROLES):
        raise FinalCompositionAmendmentError("all nine stability wrappers are required")
    return result


def _inputs_from_args(args: argparse.Namespace) -> CompositionInputs:
    values = {role: getattr(args, role) for role in BASE_ROLE_ORDER}
    return CompositionInputs(
        **values,
        layer4_day_receipts=tuple(args.layer4_day_receipt),
        layer4_final=args.layer4_final,
        compatible_execution_attempt=args.compatible_execution_attempt,
        stability_wrappers=_parse_wrappers(args.stability_wrapper),
        compatible_runtime_regression=args.compatible_runtime_regression,
        compatible_concurrent_resource=args.compatible_concurrent_resource,
        sell_54_case=args.sell_54_case,
        compatible_activation_envelope=args.compatible_activation_envelope,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "compose":
            payload = compose_final_composition(
                evidence_root=args.evidence_root,
                repository_root=args.repository_root,
                inputs=_inputs_from_args(args),
                output=args.output,
            )
        else:
            payload = validate_final_composition(
                evidence_root=args.evidence_root,
                repository_root=args.repository_root,
                receipt_path=args.receipt,
            )
    except FinalCompositionAmendmentError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(payload, sort_keys=True, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
