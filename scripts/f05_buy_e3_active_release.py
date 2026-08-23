#!/usr/bin/env python3
"""Build, finalize, or validate the owner-authorized BUY E3 active release.

This tool is deliberately independent from the research finalizer and the live
runtime. It reads only machine identity receipts, never economic rows, and it
does not perform local or remote deployment. Every input is opened with
``O_NOFOLLOW`` and is parsed, hashed, and checked between two ``fstat`` calls
on the same file descriptor.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

try:
    from scripts import f05_buy_e3_final_composition_contract as composition_contract
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import f05_buy_e3_final_composition_contract as composition_contract

OWNER_IDENTITY: Final = composition_contract.OWNER_IDENTITY
ACTIVE_RELEASE_SCHEMA: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_active_release.v1"
)
ACTIVE_RELEASE_IDENTITY: Final = ACTIVE_RELEASE_SCHEMA
ACTIVE_RELEASE_STATUS: Final = "owner_authorized_active_release"
FINAL_COMPOSITION_V2_SCHEMA: Final = composition_contract.SCHEMA_VERSION
FINAL_COMPOSITION_IDENTITY: Final = composition_contract.COMPOSITION_IDENTITY
COMPATIBLE_ATTEMPT_FINAL_SCHEMA: Final = (
    f"{OWNER_IDENTITY}.compatible_execution_attempt_final_receipt.v1"
)
CONCURRENT_RESOURCE_SCHEMA: Final = f"{OWNER_IDENTITY}.compatible_concurrent_resource_window.v2"
COMPATIBLE_REGRESSION_SCHEMA: Final = (
    f"{OWNER_IDENTITY}.compatible_runtime_regression_test_receipt.v2"
)
SELL54_SCHEMA: Final = f"{OWNER_IDENTITY}.parity_receipt.v1"
ACTIVATION_ENVELOPE_SCHEMA: Final = "f05_buy_e3_compatible_activation_envelope.v1"

ARTIFACT_ROLES: Final = ("manifest", "policy", "predicate_bundle")
EVIDENCE_ROLES: Final = (
    "final_composition",
    "compatible_attempt_final",
    "concurrent_resource",
    "runtime_regression",
    "sell54",
    "activation_envelope",
)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_UTC_RE: Final = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
_MAX_INPUT_BYTES: Final = 64 << 20

_ARTIFACT_PERMISSIONS: Final = {
    "research_authorized": False,
    "action_authorized": False,
    "live_authorized": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
_ATTEMPT_PERMISSIONS: Final = {"research": False, "action": False, "live": False}
_ATTEMPT_BOUNDARY: Final = {
    "new_economic_arm_run": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "shadow_or_companion_created": False,
}
_ACTIVATION_BOUNDARY: Final = {
    "economic_values_read": False,
    "economic_values_persisted": False,
    "hypothetical_live_actions_scored": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
_GATE_BOUNDARY: Final = {
    "economic_values_persisted": False,
    "hypothetical_live_actions_scored": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "action_authorized": False,
    "live_authorized": False,
}

_ARTIFACT_MANIFEST_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "policy_file",
        "policy_file_sha256",
        "policy_canonical_sha256",
        "predicate_bundle_file",
        "predicate_bundle_file_sha256",
        "predicate_bundle_canonical_sha256",
        "fitted_candidate_sha256",
        "label_materialization_receipt_sha256",
        "cpp_one_shot_qualification_receipt_sha256",
        "execution_preflight_receipt_sha256",
        "implementation_sha256",
        "training_days",
        "training_row_sha256",
        "duration_vocabulary",
        "default_action",
        "exact_final_artifact_oof_available",
        "research_supported",
        "owner_risk_accepted",
        "permissions",
        "artifact_sha256",
    }
)
_POLICY_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "side",
        "selected_profile",
        "selected_candidate",
        "random_seed",
        "training_days",
        "training_row_sha256",
        "training_label_request_sha256",
        "training_label_receipt_sha256",
        "training_label_payload_sha256",
        "fitted_candidate_sha256",
        "implementation_sha256",
        "predicate_bundle_file_sha256",
        "predicate_bundle_canonical_sha256",
        "policy_semantic_sha256",
        "policy",
        "feature_pool_audit",
        "fit_audit",
        "semantic_audit",
        "runtime_contract",
        "bindings",
        "permissions",
        "evidence_boundary",
        "canonical_sha256",
    }
)
_PREDICATE_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "side",
        "selected_profile",
        "selected_candidate",
        "ema_half_lives_s",
        "ema_pairs_s",
        "ema_pair_count",
        "direct_predicates",
        "predicate_columns",
        "definitions",
        "normalization_source",
        "uses_trade_predicates",
        "uses_depth_predicates",
        "uses_m2_incremental_features",
        "validation_read",
        "sealed_holdout_read",
        "canonical_sha256",
    }
)
_FINAL_COMPOSITION_FIELDS: Final = composition_contract.TOP_LEVEL_FIELDS
_ATTEMPT_FINAL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "attempt_manifest",
        "runtime_execution",
        "artifact",
        "composition_evidence_root",
        "result_receipts",
        "permissions",
        "evidence_boundary",
        "canonical_final_receipt_sha256",
    }
)
_RESOURCE_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "artifact_sha256",
        "execution_commit",
        "execution_tag",
        "host",
        "sample_count",
        "live_pid",
        "live_pid_start_ticks",
        "pre_process_identity_sha256",
        "post_process_identity_sha256",
        "benchmark_receipt_sha256",
        "thresholds",
        "observed",
        "capture",
        "samples",
        "checks",
        "sample_series_sha256",
        *_GATE_BOUNDARY,
        "canonical_resource_receipt_sha256",
    }
)
_REGRESSION_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "artifact_sha256",
        "execution_commit",
        "execution_tag",
        "python_executable",
        "python_file_sha256",
        "collect_command",
        "run_command",
        "nodeids",
        "nodeid_manifest_sha256",
        "nodeid_source_counts",
        "collected",
        "executed",
        "passed",
        "failed",
        "errors",
        "skipped",
        "collection_return_code",
        "return_code",
        "collection_stdout_sha256",
        "collection_stderr_sha256",
        "run_stdout_sha256",
        "run_stderr_sha256",
        "test_files",
        "runtime_sources",
        "coverage",
        *_GATE_BOUNDARY,
        "canonical_receipt_sha256",
    }
)
_SELL54_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "layer",
        "artifact_sha256",
        "artifact_manifest_file_sha256",
        "policy_file_sha256",
        "predicate_bundle_file_sha256",
        "evidence",
        "economic_values_materialized_by_replay",
        "economic_values_exposed",
        "economic_values_used_for_selection",
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
        "canonical_receipt_sha256",
    }
)
_ACTIVATION_FIELDS: Final = frozenset(
    {
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
)
_RELEASE_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "research_supported",
        "formal_hierarchy_passed",
        "formal_hard_gates_passed",
        "owner_risk_accepted",
        "action_authorized",
        "live_authorized",
        "scope",
        "execution",
        "exact_artifact",
        "evidence",
        "rollback",
        "evidence_boundary",
        "canonical_active_release_sha256",
    }
)
_PORTABLE_BINDING_FIELDS: Final = frozenset(
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
        "canonical_field",
        "canonical_sha256",
    }
)


class ActiveReleaseError(RuntimeError):
    """Raised when the owner active-release evidence does not fail closed."""


@dataclass(frozen=True)
class OpenedDocument:
    path: Path
    payload: dict[str, Any]
    raw: bytes
    metadata: os.stat_result

    @property
    def inode_identity(self) -> tuple[int, int]:
        return (self.metadata.st_dev, self.metadata.st_ino)


@dataclass(frozen=True)
class DocumentContract:
    schema: str
    identity: str | None
    status: str | None
    canonical_field: str
    fields: frozenset[str]
    require_identity_absent: bool = False
    require_status_absent: bool = False


_CONTRACTS: Final = {
    "manifest": DocumentContract(
        f"{OWNER_IDENTITY}.full_development_refit.v1",
        OWNER_IDENTITY,
        "exact_buy_e3_artifact_frozen",
        "artifact_sha256",
        _ARTIFACT_MANIFEST_FIELDS,
    ),
    "policy": DocumentContract(
        f"{OWNER_IDENTITY}.artifact.v1",
        OWNER_IDENTITY,
        "owner_refit_frozen_not_self_confirmed",
        "canonical_sha256",
        _POLICY_FIELDS,
    ),
    "predicate_bundle": DocumentContract(
        f"{OWNER_IDENTITY}.selected_predicate_bundle.v1",
        OWNER_IDENTITY,
        None,
        "canonical_sha256",
        _PREDICATE_FIELDS,
        require_status_absent=True,
    ),
    "final_composition": DocumentContract(
        FINAL_COMPOSITION_V2_SCHEMA,
        FINAL_COMPOSITION_IDENTITY,
        "owner_buy_e3_final_evidence_composed",
        "canonical_final_composition_receipt_sha256",
        _FINAL_COMPOSITION_FIELDS,
    ),
    "compatible_attempt_final": DocumentContract(
        COMPATIBLE_ATTEMPT_FINAL_SCHEMA,
        OWNER_IDENTITY,
        "compatible_runtime_results_bound",
        "canonical_final_receipt_sha256",
        _ATTEMPT_FINAL_FIELDS,
    ),
    "concurrent_resource": DocumentContract(
        CONCURRENT_RESOURCE_SCHEMA,
        OWNER_IDENTITY,
        "concurrent_disabled_live_benchmark_passed",
        "canonical_resource_receipt_sha256",
        _RESOURCE_FIELDS,
    ),
    "runtime_regression": DocumentContract(
        COMPATIBLE_REGRESSION_SCHEMA,
        OWNER_IDENTITY,
        "passed",
        "canonical_receipt_sha256",
        _REGRESSION_FIELDS,
    ),
    "sell54": DocumentContract(
        SELL54_SCHEMA,
        OWNER_IDENTITY,
        "parity_complete",
        "canonical_receipt_sha256",
        _SELL54_FIELDS,
    ),
    "activation_envelope": DocumentContract(
        ACTIVATION_ENVELOPE_SCHEMA,
        None,
        "compatible_activation_evidence_complete",
        "canonical_activation_envelope_sha256",
        _ACTIVATION_FIELDS,
        require_identity_absent=True,
    ),
}


def canonical_sha256(value: Any) -> str:
    """Return the canonical ASCII JSON SHA256 used by all release documents."""

    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def document_sha256(payload: Mapping[str, Any], canonical_field: str) -> str:
    body = dict(payload)
    body.pop(canonical_field, None)
    return canonical_sha256(body)


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ActiveReleaseError(f"{label} is not a SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise ActiveReleaseError(f"{label} is not a Git object id")
    return normalized


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ActiveReleaseError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_non_finite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ActiveReleaseError("JSON contains a non-finite number")
    if isinstance(value, Mapping):
        for child in value.values():
            _reject_non_finite(child)
    elif isinstance(value, list):
        for child in value:
            _reject_non_finite(child)


def _parse_json(raw: bytes, label: str) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
        payload = json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ActiveReleaseError(f"non-finite JSON token: {token}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ActiveReleaseError(f"{label} is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ActiveReleaseError(f"{label} root is not an object")
    _reject_non_finite(payload)
    return payload


def _read_all(descriptor: int, expected_size: int, label: str) -> bytes:
    if expected_size < 0 or expected_size > _MAX_INPUT_BYTES:
        raise ActiveReleaseError(f"{label} exceeds the immutable input size limit")
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, min(1 << 20, _MAX_INPUT_BYTES + 1 - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > _MAX_INPUT_BYTES:
            raise ActiveReleaseError(f"{label} exceeds the immutable input size limit")
    return b"".join(chunks)


def _same_file_state(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_uid,
        before.st_nlink,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) == (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_uid,
        after.st_nlink,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )


def _reject_symlink_components(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            metadata = current.lstat()
        except FileNotFoundError as exc:
            raise ActiveReleaseError(f"{label} path component is missing") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise ActiveReleaseError(f"{label} path must not traverse a symbolic link")


def _open_document(path: Path, label: str) -> OpenedDocument:
    candidate = path.expanduser().absolute()
    _reject_symlink_components(candidate, label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ActiveReleaseError(f"{label} could not be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
        ):
            raise ActiveReleaseError(f"{label} is not a private 0600 single-link file")
        raw = _read_all(descriptor, before.st_size, label)
        after = os.fstat(descriptor)
        if not _same_file_state(before, after) or len(raw) != before.st_size:
            raise ActiveReleaseError(f"{label} changed while it was read")
        payload = _parse_json(raw, label)
        final_state = os.fstat(descriptor)
        if not _same_file_state(after, final_state):
            raise ActiveReleaseError(f"{label} changed while it was parsed or hashed")
    finally:
        os.close(descriptor)
    try:
        _reject_symlink_components(candidate, label)
        lexical_after = candidate.lstat()
    except FileNotFoundError as exc:
        raise ActiveReleaseError(f"{label} path disappeared after reading") from exc
    if not _same_file_state(final_state, lexical_after):
        raise ActiveReleaseError(f"{label} path was replaced while it was read")
    return OpenedDocument(candidate, payload, raw, final_state)


def _validate_contract(role: str, document: OpenedDocument) -> str:
    contract = _CONTRACTS[role]
    payload = document.payload
    if set(payload) != contract.fields:
        raise ActiveReleaseError(f"{role} schema fields drifted")
    if payload.get("schema_version") != contract.schema:
        raise ActiveReleaseError(f"{role} schema version drifted")
    if contract.require_identity_absent:
        if "identity" in payload:
            raise ActiveReleaseError(f"{role} identity field must be absent")
    elif payload.get("identity") != contract.identity:
        raise ActiveReleaseError(f"{role} identity drifted")
    if contract.require_status_absent:
        if "status" in payload:
            raise ActiveReleaseError(f"{role} status field must be absent")
    elif payload.get("status") != contract.status:
        raise ActiveReleaseError(f"{role} exact status drifted")
    embedded = _require_sha256(payload.get(contract.canonical_field), f"{role} canonical hash")
    if embedded != document_sha256(payload, contract.canonical_field):
        raise ActiveReleaseError(f"{role} canonical hash drifted")
    if role == "final_composition":
        try:
            composition_contract.validate_final_composition_v2(payload)
        except composition_contract.FinalCompositionContractError as exc:
            raise ActiveReleaseError(
                f"final_composition structural contract failed: {exc}"
            ) from exc
    return embedded


def _binding(role: str, document: OpenedDocument, canonical: str) -> dict[str, Any]:
    contract = _CONTRACTS[role]
    file_sha256 = hashlib.sha256(document.raw).hexdigest()
    namespace = "artifact" if role in ARTIFACT_ROLES else "evidence"
    return {
        "role": role,
        "path": f"{namespace}/{role}-{file_sha256}.json",
        "file_sha256": file_sha256,
        "size_bytes": len(document.raw),
        "mode": "0600",
        "device": None,
        "inode": None,
        "schema_version": contract.schema,
        "identity": contract.identity,
        "status": contract.status,
        "canonical_field": contract.canonical_field,
        "canonical_sha256": canonical,
    }


def _exact_mapping(value: Any, fields: set[str] | frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(fields):
        raise ActiveReleaseError(f"{label} fields drifted")
    return value


def _git(root: Path, *args: str) -> str:
    try:
        completed = subprocess.run(
            ("git", *args),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=20.0,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ActiveReleaseError(f"Git identity command failed: {' '.join(args)}") from exc
    return completed.stdout.strip()


def _operational_git_identity(root: Path, annotated_tag: str) -> dict[str, str]:
    repository = root.expanduser().resolve(strict=True)
    tag = str(annotated_tag).strip()
    if not tag or any(character.isspace() for character in tag):
        raise ActiveReleaseError("annotated operational tag name is invalid")
    if _git(repository, "status", "--porcelain=v1", "--untracked-files=all"):
        raise ActiveReleaseError("active release requires a completely clean worktree")
    reference = f"refs/tags/{tag}"
    if _git(repository, "cat-file", "-t", reference) != "tag":
        raise ActiveReleaseError("operational tag is not annotated")
    tag_object = _require_git_sha(_git(repository, "rev-parse", reference), "tag object")
    commit = _require_git_sha(_git(repository, "rev-parse", f"{reference}^{{}}"), "tag commit")
    head = _require_git_sha(_git(repository, "rev-parse", "HEAD"), "HEAD commit")
    if commit != head:
        raise ActiveReleaseError("operational tag does not peel to HEAD")
    tree = _require_git_sha(_git(repository, "rev-parse", f"{commit}^{{tree}}"), "tag tree")
    return {
        "execution_commit": commit,
        "execution_tree": tree,
        "annotated_operational_tag": tag,
        "annotated_operational_tag_object": tag_object,
        "tag_peeled_commit": commit,
    }


def _timestamp(value: Any, label: str) -> str:
    normalized = str(value)
    if _UTC_RE.fullmatch(normalized) is None:
        raise ActiveReleaseError(f"{label} is not a canonical UTC timestamp")
    try:
        datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ActiveReleaseError(f"{label} is not a valid UTC timestamp") from exc
    return normalized


def _validate_artifact_documents(documents: Mapping[str, OpenedDocument]) -> dict[str, Any]:
    manifest = documents["manifest"].payload
    policy = documents["policy"].payload
    predicates = documents["predicate_bundle"].payload
    artifact_sha = _require_sha256(manifest.get("artifact_sha256"), "artifact SHA256")
    policy_file_sha = hashlib.sha256(documents["policy"].raw).hexdigest()
    predicate_file_sha = hashlib.sha256(documents["predicate_bundle"].raw).hexdigest()
    policy_canonical = _require_sha256(policy.get("canonical_sha256"), "policy canonical SHA256")
    predicate_canonical = _require_sha256(
        predicates.get("canonical_sha256"), "predicate canonical SHA256"
    )
    try:
        training_days = composition_contract.validate_training_days(manifest.get("training_days"))
    except composition_contract.FinalCompositionContractError as exc:
        raise ActiveReleaseError(f"exact artifact training days drifted: {exc}") from exc
    expected_actions = [
        "CONTROL_85N",
        "FIXED_79S",
        "FIXED_173S",
        "FIXED_223S",
        "FIXED_356S",
        "FIXED_640S",
        "FIXED_709S",
        "FIXED_2048S",
    ]
    if (
        manifest.get("policy_file") != "policy.json"
        or manifest.get("predicate_bundle_file") != "predicate_bundle.json"
        or manifest.get("policy_file_sha256") != policy_file_sha
        or manifest.get("predicate_bundle_file_sha256") != predicate_file_sha
        or manifest.get("policy_canonical_sha256") != policy_canonical
        or manifest.get("predicate_bundle_canonical_sha256") != predicate_canonical
        or manifest.get("duration_vocabulary") != expected_actions
        or manifest.get("default_action") != "CONTROL_85N"
        or manifest.get("exact_final_artifact_oof_available") is not False
        or manifest.get("research_supported") is not False
        or manifest.get("owner_risk_accepted") is not True
        or manifest.get("permissions") != _ARTIFACT_PERMISSIONS
        or policy.get("training_days") != list(training_days)
    ):
        raise ActiveReleaseError("exact artifact manifest contract drifted")
    runtime_contract = _exact_mapping(
        policy.get("runtime_contract"),
        {
            "surface",
            "fixed_action_is_total_cooldown",
            "control_is_85_seconds_times_consecutive_fill_units",
            "fallback_action",
            "reducing_buy_unchanged",
            "sell_owner_policy_unchanged",
            "warmup_requires_elapsed_time_and_all_selected_states_identified",
        },
        "policy runtime contract",
    )
    policy_boundary = _exact_mapping(
        policy.get("evidence_boundary"),
        {
            "formal_hierarchy_passed",
            "formal_hard_gates_passed",
            "research_supported",
            "owner_risk_accepted",
            "outcome_informed_owner_override",
            "learning_algorithm_oof_evidence_only",
            "old_oof_estimate_applies_to_exact_artifact",
            "exact_artifact_oof_available",
        },
        "policy evidence boundary",
    )
    if (
        policy.get("side") != "BUY"
        or policy.get("predicate_bundle_file_sha256") != predicate_file_sha
        or policy.get("predicate_bundle_canonical_sha256") != predicate_canonical
        or policy.get("permissions") != _ARTIFACT_PERMISSIONS
        or runtime_contract
        != {
            "surface": "BUY_exposure_increasing_fill_callback_only",
            "fixed_action_is_total_cooldown": True,
            "control_is_85_seconds_times_consecutive_fill_units": True,
            "fallback_action": "CONTROL_85N",
            "reducing_buy_unchanged": True,
            "sell_owner_policy_unchanged": True,
            "warmup_requires_elapsed_time_and_all_selected_states_identified": True,
        }
        or policy_boundary
        != {
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "research_supported": False,
            "owner_risk_accepted": True,
            "outcome_informed_owner_override": True,
            "learning_algorithm_oof_evidence_only": True,
            "old_oof_estimate_applies_to_exact_artifact": False,
            "exact_artifact_oof_available": False,
        }
    ):
        raise ActiveReleaseError("exact policy authority or runtime contract drifted")
    if (
        predicates.get("side") != "BUY"
        or predicates.get("ema_pair_count") != 45
        or predicates.get("uses_trade_predicates") is not False
        or predicates.get("uses_depth_predicates") is not False
        or predicates.get("uses_m2_incremental_features") is not False
        or predicates.get("validation_read") is not False
        or predicates.get("sealed_holdout_read") is not False
    ):
        raise ActiveReleaseError("predicate bundle evidence boundary drifted")
    return {
        "artifact_sha256": artifact_sha,
        "manifest_file_sha256": hashlib.sha256(documents["manifest"].raw).hexdigest(),
        "policy_file_sha256": policy_file_sha,
        "policy_canonical_sha256": policy_canonical,
        "predicate_bundle_file_sha256": predicate_file_sha,
        "predicate_bundle_canonical_sha256": predicate_canonical,
        "training_days": list(training_days),
    }


def _attempt_runtime(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _exact_mapping(
        payload.get("runtime_execution"),
        {
            "execution_commit",
            "execution_tree",
            "annotated_tag",
            "annotated_tag_object",
            "tag_peeled_commit",
        },
        "compatible attempt runtime execution",
    )


def _validate_evidence_documents(
    documents: Mapping[str, OpenedDocument],
    artifact: Mapping[str, Any],
    execution: Mapping[str, str],
) -> dict[str, Any]:
    composition = documents["final_composition"].payload
    attempt = documents["compatible_attempt_final"].payload
    resource = documents["concurrent_resource"].payload
    regression = documents["runtime_regression"].payload
    sell = documents["sell54"].payload
    activation = documents["activation_envelope"].payload
    artifact_sha = artifact["artifact_sha256"]

    _timestamp(attempt.get("generated_utc"), "compatible attempt timestamp")
    if (
        attempt.get("permissions") != _ATTEMPT_PERMISSIONS
        or attempt.get("evidence_boundary") != _ATTEMPT_BOUNDARY
    ):
        raise ActiveReleaseError("compatible attempt permission boundary drifted")
    attempt_runtime = _attempt_runtime(attempt)
    if (
        attempt_runtime.get("execution_commit") != execution["execution_commit"]
        or attempt_runtime.get("execution_tree") != execution["execution_tree"]
        or attempt_runtime.get("tag_peeled_commit") != execution["execution_commit"]
    ):
        raise ActiveReleaseError("compatible attempt execution commit or tree drifted")
    attempt_artifact = _exact_mapping(
        attempt.get("artifact"), {"binding", "canonical_sha256"}, "attempt artifact"
    )
    attempt_artifact_binding = _exact_mapping(
        attempt_artifact.get("binding"),
        {"artifact_sha256", "files", "formal_manifest"},
        "attempt artifact binding",
    )
    attempt_files = _exact_mapping(
        attempt_artifact_binding.get("files"), set(ARTIFACT_ROLES), "attempt artifact files"
    )
    for role, expected_sha in (
        ("manifest", artifact["manifest_file_sha256"]),
        ("policy", artifact["policy_file_sha256"]),
        ("predicate_bundle", artifact["predicate_bundle_file_sha256"]),
    ):
        item = _exact_mapping(
            attempt_files.get(role),
            {"path", "file_sha256", "size_bytes", "device", "inode"},
            f"attempt artifact {role}",
        )
        if (
            type(item.get("path")) is not str
            or not Path(item["path"]).is_absolute()
            or item.get("file_sha256") != expected_sha
            or type(item.get("size_bytes")) is not int
            or item.get("size_bytes") <= 0
            or type(item.get("device")) is not int
            or item.get("device") < 0
            or type(item.get("inode")) is not int
            or item.get("inode") <= 0
        ):
            raise ActiveReleaseError(f"compatible attempt artifact {role} drifted")
    formal_manifest = _exact_mapping(
        attempt_artifact_binding.get("formal_manifest"),
        {"path", "file_sha256", "size_bytes", "device", "inode", "canonical_sha256"},
        "attempt artifact producer manifest",
    )
    if (
        type(formal_manifest.get("path")) is not str
        or not Path(formal_manifest["path"]).is_absolute()
        or type(formal_manifest.get("size_bytes")) is not int
        or formal_manifest.get("size_bytes") <= 0
        or type(formal_manifest.get("device")) is not int
        or formal_manifest.get("device") < 0
        or type(formal_manifest.get("inode")) is not int
        or formal_manifest.get("inode") <= 0
    ):
        raise ActiveReleaseError("compatible attempt producer manifest drifted")
    _require_sha256(formal_manifest.get("file_sha256"), "attempt producer manifest file")
    _require_sha256(formal_manifest.get("canonical_sha256"), "attempt producer manifest canonical")
    if attempt_artifact_binding.get("artifact_sha256") != artifact_sha or attempt_artifact.get(
        "canonical_sha256"
    ) != canonical_sha256(attempt_artifact_binding):
        raise ActiveReleaseError("compatible attempt artifact canonical identity drifted")

    result_receipts = _exact_mapping(
        attempt.get("result_receipts"), {"final_composition"}, "attempt result roles"
    )
    composition_result = _exact_mapping(
        result_receipts.get("final_composition"),
        {
            "path",
            "file_sha256",
            "size_bytes",
            "mode",
            "schema_version",
            "identity",
            "status",
            "canonical_field",
            "canonical_sha256",
        },
        "attempt final composition binding",
    )
    composition_canonical = composition["canonical_final_composition_receipt_sha256"]
    if (
        composition_result.get("file_sha256")
        != hashlib.sha256(documents["final_composition"].raw).hexdigest()
        or composition_result.get("schema_version") != FINAL_COMPOSITION_V2_SCHEMA
        or composition_result.get("identity") != FINAL_COMPOSITION_IDENTITY
        or composition_result.get("status") != "owner_buy_e3_final_evidence_composed"
        or composition_result.get("canonical_field") != "canonical_final_composition_receipt_sha256"
        or composition_result.get("canonical_sha256") != composition_canonical
    ):
        raise ActiveReleaseError("compatible attempt does not bind final composition v2")

    try:
        composition = composition_contract.validate_final_composition_v2(composition)
        composition_ordered = composition_contract.ordered_evidence_by_role(composition)
    except composition_contract.FinalCompositionContractError as exc:
        raise ActiveReleaseError(f"final composition v2 contract failed: {exc}") from exc
    final_artifact = composition["exact_artifact"]
    interpretation = composition["evidence_interpretation"]
    if (
        final_artifact.get("artifact_sha256") != artifact_sha
        or final_artifact.get("manifest_file_sha256") != artifact["manifest_file_sha256"]
        or final_artifact.get("policy_file_sha256") != artifact["policy_file_sha256"]
        or final_artifact.get("predicate_bundle_file_sha256")
        != artifact["predicate_bundle_file_sha256"]
        or final_artifact.get("training_days") != artifact["training_days"]
        or final_artifact.get("exact_artifact_oof_available") is not False
        or interpretation.get("research_supported") is not False
        or interpretation.get("owner_risk_accepted") is not True
        or interpretation.get("formal_hierarchy_passed") is not False
        or interpretation.get("formal_hard_gates_passed") is not False
        or interpretation.get("old_oof_applies_to_learning_algorithm_only") is not True
        or interpretation.get("exact_artifact_oof_available") is not False
    ):
        raise ActiveReleaseError("final composition owner or artifact boundary drifted")

    attempt_manifest = _exact_mapping(
        attempt.get("attempt_manifest"),
        {"path", "file_sha256", "size_bytes", "canonical_sha256"},
        "attempt manifest binding",
    )
    compatible = _exact_mapping(
        composition.get("compatible_execution_attempt"),
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
        "composition compatible attempt",
    )
    if (
        compatible.get("schema_version") != f"{OWNER_IDENTITY}.compatible_execution_attempt.v2"
        or compatible.get("identity") != OWNER_IDENTITY
        or compatible.get("attempt_id") != attempt.get("attempt_id")
        or compatible.get("canonical_execution_attempt_sha256")
        != attempt_manifest.get("canonical_sha256")
        or compatible.get("execution_commit") != attempt_runtime.get("execution_commit")
        or compatible.get("execution_tree") != attempt_runtime.get("execution_tree")
        or compatible.get("annotated_tag") != attempt_runtime.get("annotated_tag")
        or compatible.get("annotated_tag_object") != attempt_runtime.get("annotated_tag_object")
    ):
        raise ActiveReleaseError("composition compatible execution attempt drifted")

    ordered_expectations = {
        "exact_artifact_manifest": (
            artifact["manifest_file_sha256"],
            artifact["artifact_sha256"],
        ),
        "exact_policy": (
            artifact["policy_file_sha256"],
            artifact["policy_canonical_sha256"],
        ),
        "exact_predicate_bundle": (
            artifact["predicate_bundle_file_sha256"],
            artifact["predicate_bundle_canonical_sha256"],
        ),
        "compatible_execution_attempt": (
            attempt_manifest["file_sha256"],
            attempt_manifest["canonical_sha256"],
        ),
        "compatible_runtime_regression": (
            hashlib.sha256(documents["runtime_regression"].raw).hexdigest(),
            regression["canonical_receipt_sha256"],
        ),
        "compatible_concurrent_resource": (
            hashlib.sha256(documents["concurrent_resource"].raw).hexdigest(),
            resource["canonical_resource_receipt_sha256"],
        ),
        "sell_54_case": (
            hashlib.sha256(documents["sell54"].raw).hexdigest(),
            sell["canonical_receipt_sha256"],
        ),
        "compatible_activation_envelope": (
            hashlib.sha256(documents["activation_envelope"].raw).hexdigest(),
            activation["canonical_activation_envelope_sha256"],
        ),
    }
    for role, (expected_file, expected_canonical) in ordered_expectations.items():
        binding = composition_ordered[role]
        if (
            binding.get("file_sha256") != expected_file
            or binding.get("canonical_sha256") != expected_canonical
        ):
            raise ActiveReleaseError(f"final composition ordered binding drifted: {role}")

    attempt_tag = str(attempt_runtime["annotated_tag"])
    _timestamp(resource.get("generated_utc"), "resource timestamp")
    resource_checks = resource.get("checks")
    if (
        resource.get("artifact_sha256") != artifact_sha
        or resource.get("execution_commit") != execution["execution_commit"]
        or resource.get("execution_tag") != attempt_tag
        or not isinstance(resource_checks, Mapping)
        or not resource_checks
        or any(value is not True for value in resource_checks.values())
        or any(resource.get(key) is not value for key, value in _GATE_BOUNDARY.items())
    ):
        raise ActiveReleaseError("concurrent resource authority drifted")

    _timestamp(regression.get("generated_utc"), "regression timestamp")
    coverage = regression.get("coverage")
    if (
        regression.get("artifact_sha256") != artifact_sha
        or regression.get("execution_commit") != execution["execution_commit"]
        or regression.get("execution_tag") != attempt_tag
        or any(regression.get(field) != 0 for field in ("failed", "errors", "skipped"))
        or regression.get("return_code") != 0
        or regression.get("collection_return_code") != 0
        or regression.get("passed") != regression.get("executed")
        or regression.get("executed") != regression.get("collected")
        or not isinstance(coverage, Mapping)
        or not coverage
        or any(value is not True for value in coverage.values())
        or any(regression.get(key) is not value for key, value in _GATE_BOUNDARY.items())
    ):
        raise ActiveReleaseError("runtime regression authority drifted")

    sell_evidence = _exact_mapping(
        sell.get("evidence"),
        {
            "policy_sha256",
            "predicate_bundle_sha256",
            "predicate_columns",
            "sell_tri_state_cases",
            "buy_tri_state_cases",
            "mismatch_count",
            "documented_semantics_equal",
            "runtime_binding_valid",
        },
        "SELL54 evidence",
    )
    if (
        sell.get("layer") != "sell_owner_54_case_unchanged"
        or sell.get("artifact_sha256") != artifact_sha
        or sell.get("artifact_manifest_file_sha256") != artifact["manifest_file_sha256"]
        or sell.get("policy_file_sha256") != artifact["policy_file_sha256"]
        or sell.get("predicate_bundle_file_sha256") != artifact["predicate_bundle_file_sha256"]
        or sell_evidence.get("sell_tri_state_cases") != 27
        or sell_evidence.get("buy_tri_state_cases") != 27
        or sell_evidence.get("mismatch_count") != 0
        or sell_evidence.get("documented_semantics_equal") is not True
        or sell_evidence.get("runtime_binding_valid") is not True
        or any(
            sell.get(field) is not False
            for field in (
                "economic_values_materialized_by_replay",
                "economic_values_exposed",
                "economic_values_used_for_selection",
                "validation_read",
                "sealed_holdout_read",
                "action_authorized",
                "live_authorized",
            )
        )
    ):
        raise ActiveReleaseError("SELL54 safeguard drifted")

    expected_activation_execution = {
        "execution_commit": execution["execution_commit"],
        "execution_tree": execution["execution_tree"],
        "annotated_tag": attempt_runtime["annotated_tag"],
        "annotated_tag_object": attempt_runtime["annotated_tag_object"],
    }
    expected_activation_artifact = {
        "artifact_sha256": artifact_sha,
        "files": {
            "manifest": artifact["manifest_file_sha256"],
            "policy": artifact["policy_file_sha256"],
            "predicate_bundle": artifact["predicate_bundle_file_sha256"],
        },
    }
    activation_contract = _exact_mapping(
        activation.get("activation_contract"),
        {
            "restart_only",
            "same_disabled_process_required",
            "phase_token_still_required",
            "envelope_does_not_authorize_remote_mutation_by_itself",
        },
        "activation contract",
    )
    activation_checks = activation.get("checks")
    if (
        activation.get("execution") != expected_activation_execution
        or activation.get("artifact") != expected_activation_artifact
        or not isinstance(activation_checks, Mapping)
        or not activation_checks
        or any(value is not True for value in activation_checks.values())
        or activation_contract
        != {
            "restart_only": True,
            "same_disabled_process_required": True,
            "phase_token_still_required": True,
            "envelope_does_not_authorize_remote_mutation_by_itself": True,
        }
        or activation.get("evidence_boundary") != _ACTIVATION_BOUNDARY
    ):
        raise ActiveReleaseError("activation prerequisite boundary drifted")

    direct = {
        "concurrent_resource": (
            resource["canonical_resource_receipt_sha256"],
            hashlib.sha256(documents["concurrent_resource"].raw).hexdigest(),
            activation.get("concurrent_resource_receipt"),
        ),
        "runtime_regression": (
            regression["canonical_receipt_sha256"],
            hashlib.sha256(documents["runtime_regression"].raw).hexdigest(),
            activation.get("runtime_regression_receipt"),
        ),
        "sell54": (
            sell["canonical_receipt_sha256"],
            hashlib.sha256(documents["sell54"].raw).hexdigest(),
            activation.get("sell_54_case_receipt"),
        ),
    }
    for role, (canonical, file_hash, raw_binding) in direct.items():
        if not isinstance(raw_binding, Mapping):
            raise ActiveReleaseError(f"activation {role} binding is missing")
        if raw_binding.get("file_sha256") != file_hash:
            raise ActiveReleaseError(f"activation {role} file binding drifted")
        bound_canonical = raw_binding.get("canonical_sha256")
        if bound_canonical is None:
            bound_canonical = raw_binding.get("canonical_receipt_sha256")
        if bound_canonical != canonical:
            raise ActiveReleaseError(f"activation {role} canonical binding drifted")

    disabled_phase = activation.get("disabled_phase_receipt")
    current_authority = composition["current_authority_evidence"]
    if not isinstance(disabled_phase, Mapping) or current_authority != {
        "runtime_regression_sha256": regression["canonical_receipt_sha256"],
        "concurrent_resource_sha256": resource["canonical_resource_receipt_sha256"],
        "sell_54_case_sha256": sell["canonical_receipt_sha256"],
        "activation_envelope_sha256": activation["canonical_activation_envelope_sha256"],
        "plan_sha256": activation["plan_sha256"],
        "disabled_process_identity_sha256": disabled_phase.get("process_identity_sha256"),
    }:
        raise ActiveReleaseError("final composition current authority binding drifted")

    return {
        "attempt_tag": attempt_tag,
        "attempt_manifest_canonical_sha256": attempt_manifest["canonical_sha256"],
        "final_composition_canonical_sha256": composition_canonical,
    }


def _read_role_documents(
    paths: Mapping[str, Path], required_roles: Sequence[str]
) -> dict[str, OpenedDocument]:
    normalized = {str(role): Path(path) for role, path in paths.items()}
    expected = set(required_roles)
    if set(normalized) != expected:
        missing = sorted(expected - set(normalized))
        extra = sorted(set(normalized) - expected)
        raise ActiveReleaseError(f"role set drifted; missing={missing}, extra={extra}")
    documents: dict[str, OpenedDocument] = {}
    for role in required_roles:
        document = _open_document(normalized[role], role)
        _validate_contract(role, document)
        documents[role] = document
    return documents


def _assert_inode_uniqueness(documents: Sequence[OpenedDocument]) -> None:
    identities = [document.inode_identity for document in documents]
    if len(identities) != len(set(identities)):
        raise ActiveReleaseError("artifact and evidence roles must use unique inodes")


def _release_binding_map(
    documents: Mapping[str, OpenedDocument], roles: Sequence[str]
) -> dict[str, dict[str, Any]]:
    return {
        role: _binding(
            role,
            documents[role],
            _require_sha256(
                documents[role].payload[_CONTRACTS[role].canonical_field],
                f"{role} canonical SHA256",
            ),
        )
        for role in roles
    }


def build_active_release(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    evidence_paths: Mapping[str, Path],
    generated_utc: str | None = None,
) -> dict[str, Any]:
    """Build an active release in memory without writing or deploying it."""

    execution = _operational_git_identity(repository_root, annotated_operational_tag)
    artifact_documents = _read_role_documents(artifact_paths, ARTIFACT_ROLES)
    evidence_documents = _read_role_documents(evidence_paths, EVIDENCE_ROLES)
    _assert_inode_uniqueness([*artifact_documents.values(), *evidence_documents.values()])
    artifact = _validate_artifact_documents(artifact_documents)
    _validate_evidence_documents(evidence_documents, artifact, execution)
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    _timestamp(timestamp, "active release timestamp")
    payload: dict[str, Any] = {
        "schema_version": ACTIVE_RELEASE_SCHEMA,
        "identity": ACTIVE_RELEASE_IDENTITY,
        "status": ACTIVE_RELEASE_STATUS,
        "generated_utc": timestamp,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "action_authorized": True,
        "live_authorized": True,
        "scope": {
            "side": "BUY",
            "trigger": "exposure_increasing_executed_fill",
            "output": "total_cooldown",
            "reducing_buy_unchanged": True,
            "sell_owner_policy_unchanged": True,
        },
        "execution": execution,
        "exact_artifact": {
            "artifact_sha256": artifact["artifact_sha256"],
            "roles": _release_binding_map(artifact_documents, ARTIFACT_ROLES),
        },
        "evidence": _release_binding_map(evidence_documents, EVIDENCE_ROLES),
        "rollback": {
            "buy_e3_enabled": False,
            "buy_deadline_identity": "B0",
            "e3_deadline_imported": False,
            "b0_seconds": 85,
            "b0_multiplier": "consecutive_fill_units",
            "b0_contract": "85s_x_consecutive_fill_units",
        },
        "evidence_boundary": {
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "shadow_created": False,
            "companion_created": False,
            "new_economic_arm_run": False,
        },
    }
    payload["canonical_active_release_sha256"] = document_sha256(
        payload, "canonical_active_release_sha256"
    )
    return payload


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False) + "\n"
    ).encode("ascii")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_exclusive(path: Path, payload: Mapping[str, Any]) -> str:
    target = path.expanduser().absolute()
    _reject_symlink_components(target.parent, "active release output parent")
    encoded = _json_bytes(payload)
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ActiveReleaseError("active release output requires O_NOFOLLOW")
    directory_flags = (
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0) | nofollow
    )
    try:
        parent_descriptor = os.open(target.parent, directory_flags)
    except OSError as exc:
        raise ActiveReleaseError("active release output parent could not be opened safely") from exc
    try:
        parent_identity = os.fstat(parent_descriptor)
        if not stat.S_ISDIR(parent_identity.st_mode):
            raise ActiveReleaseError("active release output parent is not a directory")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | nofollow
        try:
            descriptor = os.open(
                target.name,
                flags,
                0o600,
                dir_fd=parent_descriptor,
            )
        except FileExistsError as exc:
            raise ActiveReleaseError(f"immutable active release already exists: {target}") from exc
        except OSError as exc:
            raise ActiveReleaseError("active release output could not be created safely") from exc
        try:
            os.fchmod(descriptor, 0o600)
            offset = 0
            while offset < len(encoded):
                written = os.write(descriptor, encoded[offset:])
                if written <= 0:
                    raise ActiveReleaseError("active release write did not make progress")
                offset += written
            os.fsync(descriptor)
            written_identity = os.fstat(descriptor)
            path_identity = os.stat(
                target.name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(written_identity.st_mode)
                or stat.S_IMODE(written_identity.st_mode) != 0o600
                or written_identity.st_uid != os.geteuid()
                or written_identity.st_nlink != 1
                or written_identity.st_size != len(encoded)
                or not _same_file_state(written_identity, path_identity)
            ):
                raise ActiveReleaseError("active release output identity drifted during write")
        finally:
            os.close(descriptor)
        os.fsync(parent_descriptor)
        _reject_symlink_components(target.parent, "active release output parent")
        lexical_parent = target.parent.lstat()
        if (lexical_parent.st_dev, lexical_parent.st_ino) != (
            parent_identity.st_dev,
            parent_identity.st_ino,
        ):
            raise ActiveReleaseError("active release output parent changed during write")
    finally:
        os.close(parent_descriptor)
    return hashlib.sha256(encoded).hexdigest()


def finalize_active_release(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    evidence_paths: Mapping[str, Path],
    output_path: Path,
    generated_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    payload = build_active_release(
        repository_root=repository_root,
        annotated_operational_tag=annotated_operational_tag,
        artifact_paths=artifact_paths,
        evidence_paths=evidence_paths,
        generated_utc=generated_utc,
    )
    file_hash = _write_exclusive(output_path, payload)
    validated = validate_active_release(output_path, repository_root=repository_root)
    if validated != payload:
        raise ActiveReleaseError("written active release changed during finalization")
    return payload, file_hash


def _validate_portable_release_bindings(
    raw: Any, required_roles: Sequence[str], label: str
) -> dict[str, Mapping[str, Any]]:
    bindings = _exact_mapping(raw, set(required_roles), label)
    validated: dict[str, Mapping[str, Any]] = {}
    for role in required_roles:
        binding = _exact_mapping(bindings.get(role), _PORTABLE_BINDING_FIELDS, f"{label}.{role}")
        contract = _CONTRACTS[role]
        file_sha256 = _require_sha256(binding.get("file_sha256"), f"{label}.{role} file")
        _require_sha256(binding.get("canonical_sha256"), f"{label}.{role} canonical")
        namespace = "artifact" if role in ARTIFACT_ROLES else "evidence"
        expected_path = f"{namespace}/{role}-{file_sha256}.json"
        path = binding.get("path")
        parsed = PurePosixPath(path) if type(path) is str else PurePosixPath("/")
        if (
            binding.get("role") != role
            or path != expected_path
            or parsed.is_absolute()
            or any(part in {"", ".", ".."} for part in parsed.parts)
            or type(binding.get("size_bytes")) is not int
            or binding.get("size_bytes") <= 0
            or binding.get("mode") != "0600"
            or binding.get("device") is not None
            or binding.get("inode") is not None
            or binding.get("schema_version") != contract.schema
            or binding.get("identity") != contract.identity
            or binding.get("status") != contract.status
            or binding.get("canonical_field") != contract.canonical_field
        ):
            raise ActiveReleaseError(f"{label}.{role} portable binding drifted")
        validated[role] = binding
    return validated


def validate_active_release(path: Path, *, repository_root: Path) -> dict[str, Any]:
    """Independently re-read and validate an immutable active release."""

    release_document = _open_document(path, "active release")
    payload = release_document.payload
    if set(payload) != _RELEASE_FIELDS:
        raise ActiveReleaseError("active release schema fields drifted")
    if (
        payload.get("schema_version") != ACTIVE_RELEASE_SCHEMA
        or payload.get("identity") != ACTIVE_RELEASE_IDENTITY
        or payload.get("status") != ACTIVE_RELEASE_STATUS
        or payload.get("research_supported") is not False
        or payload.get("formal_hierarchy_passed") is not False
        or payload.get("formal_hard_gates_passed") is not False
        or payload.get("owner_risk_accepted") is not True
        or payload.get("action_authorized") is not True
        or payload.get("live_authorized") is not True
    ):
        raise ActiveReleaseError("active release exact authority drifted")
    _timestamp(payload.get("generated_utc"), "active release timestamp")
    canonical = _require_sha256(
        payload.get("canonical_active_release_sha256"), "active release canonical SHA256"
    )
    if canonical != document_sha256(payload, "canonical_active_release_sha256"):
        raise ActiveReleaseError("active release canonical SHA256 drifted")
    expected_scope = {
        "side": "BUY",
        "trigger": "exposure_increasing_executed_fill",
        "output": "total_cooldown",
        "reducing_buy_unchanged": True,
        "sell_owner_policy_unchanged": True,
    }
    expected_rollback = {
        "buy_e3_enabled": False,
        "buy_deadline_identity": "B0",
        "e3_deadline_imported": False,
        "b0_seconds": 85,
        "b0_multiplier": "consecutive_fill_units",
        "b0_contract": "85s_x_consecutive_fill_units",
    }
    expected_boundary = {
        "old_oof_applies_to_learning_algorithm_only": True,
        "exact_artifact_oof_available": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "shadow_created": False,
        "companion_created": False,
        "new_economic_arm_run": False,
    }
    if (
        payload.get("scope") != expected_scope
        or payload.get("rollback") != expected_rollback
        or payload.get("evidence_boundary") != expected_boundary
    ):
        raise ActiveReleaseError("active release scope, rollback, or evidence boundary drifted")
    execution_raw = _exact_mapping(
        payload.get("execution"),
        {
            "execution_commit",
            "execution_tree",
            "annotated_operational_tag",
            "annotated_operational_tag_object",
            "tag_peeled_commit",
        },
        "active release execution",
    )
    observed_execution = _operational_git_identity(
        repository_root, str(execution_raw.get("annotated_operational_tag", ""))
    )
    if dict(execution_raw) != observed_execution:
        raise ActiveReleaseError("active release commit, tree, or operational tag drifted")

    exact_artifact = _exact_mapping(
        payload.get("exact_artifact"), {"artifact_sha256", "roles"}, "active artifact"
    )
    _require_sha256(exact_artifact.get("artifact_sha256"), "active artifact SHA256")
    artifact_bindings = _validate_portable_release_bindings(
        exact_artifact.get("roles"), ARTIFACT_ROLES, "active artifact roles"
    )
    _validate_portable_release_bindings(
        payload.get("evidence"), EVIDENCE_ROLES, "active evidence roles"
    )
    if exact_artifact.get("artifact_sha256") != artifact_bindings["manifest"].get(
        "canonical_sha256"
    ):
        raise ActiveReleaseError("active release exact artifact canonical identity drifted")
    return dict(payload)


def _add_build_inputs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--annotated-operational-tag", required=True)
    parser.add_argument("--artifact-manifest", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--predicate-bundle", type=Path, required=True)
    parser.add_argument("--final-composition", type=Path, required=True)
    parser.add_argument("--compatible-attempt-final", type=Path, required=True)
    parser.add_argument("--concurrent-resource", type=Path, required=True)
    parser.add_argument("--runtime-regression", type=Path, required=True)
    parser.add_argument("--sell54", type=Path, required=True)
    parser.add_argument("--activation-envelope", type=Path, required=True)


def _artifact_paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "manifest": args.artifact_manifest,
        "policy": args.policy,
        "predicate_bundle": args.predicate_bundle,
    }


def _evidence_paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "final_composition": args.final_composition,
        "compatible_attempt_final": args.compatible_attempt_final,
        "concurrent_resource": args.concurrent_resource,
        "runtime_regression": args.runtime_regression,
        "sell54": args.sell54,
        "activation_envelope": args.activation_envelope,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    _add_build_inputs(build)
    finalize = subparsers.add_parser("finalize")
    _add_build_inputs(finalize)
    finalize.add_argument("--output", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--repository-root", type=Path, required=True)
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "validate":
        payload = validate_active_release(args.receipt, repository_root=args.repository_root)
    elif args.command == "build":
        payload = build_active_release(
            repository_root=args.repository_root,
            annotated_operational_tag=args.annotated_operational_tag,
            artifact_paths=_artifact_paths(args),
            evidence_paths=_evidence_paths(args),
        )
        print(_json_bytes(payload).decode("ascii"), end="")
        return 0
    else:
        payload, file_hash = finalize_active_release(
            repository_root=args.repository_root,
            annotated_operational_tag=args.annotated_operational_tag,
            artifact_paths=_artifact_paths(args),
            evidence_paths=_evidence_paths(args),
            output_path=args.output,
        )
        print(file_hash)
    print(payload["canonical_active_release_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
