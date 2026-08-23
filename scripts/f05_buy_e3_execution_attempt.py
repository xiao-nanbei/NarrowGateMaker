#!/usr/bin/env python3
"""Freeze or validate a bug-fix execution attempt for the immutable BUY E3 artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from strategy.boolean_cooldown_buy_e3 import OWNER_IDENTITY, LiveBuyE3CooldownPolicy

SCHEMA_VERSION = f"{OWNER_IDENTITY}.compatible_execution_attempt.v2"
FINAL_RECEIPT_SCHEMA_VERSION = f"{OWNER_IDENTITY}.compatible_execution_attempt_final_receipt.v1"
PRODUCER_COMMIT = "c170493ea5838b6e3a715006db352c0a484d3943"
PRODUCER_TREE = "52fe1cde0e0c789acb9e4b0dbac95572ca61d483"
PRODUCER_TAG = "f05-owner-buy-e3-live-attempt2-20260821"
PRODUCER_TAG_OBJECT = "cda11b7700e3fec21464401a391133f129be74c1"
ARTIFACT_SHA256 = "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
ARTIFACT_FILE_SHA256 = {
    "manifest": "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929",
    "policy": "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b",
    "predicate_bundle": "4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915",
}
FORMAL_MANIFEST_CANONICAL_SHA256 = (
    "3d016578fb31acc6850e3032fb96a1e45c54ead55c1f6d7a1102e9be27a9133d"
)
RUNTIME_SOURCE_PATHS = {
    "live_main": "live/main.py",
    "live_config": "live/config.py",
    "live_run": "live/run.sh",
    "live_runtime_policy": "live/runtime_policy.py",
    "live_ws_handler": "live/ws_handler.py",
    "maker_engine": "strategy/maker_engine.py",
    "sell_owner_runtime": "strategy/boolean_cooldown_live.py",
    "buy_e3_runtime": "strategy/boolean_cooldown_buy_e3.py",
    "deployment_gate": (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1.py"
    ),
    "deployment_tool": "scripts/deploy_f05_buy_e3_owner_v1.py",
    "attempt_tool": "scripts/f05_buy_e3_execution_attempt.py",
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_RECEIPT_ROLE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_CANONICAL_FIELD_RE = re.compile(r"^canonical_[a-z0-9_]*sha256$")

PRE_ADMISSION_RECEIPT_ROLES = (
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
PRE_ADMISSION_RECEIPT_WRAPPER_SCHEMA = (
    f"{OWNER_IDENTITY}.pre_admission_stability_receipt_wrapper.v1"
)
PRE_ADMISSION_RECEIPT_WRAPPER_STATUS = "passed"
PRE_ADMISSION_EVIDENCE_BOUNDARY = {
    "economic_values_exposed": False,
    "economic_values_used_for_selection": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
PRE_ADMISSION_PERMISSIONS = {
    "research": False,
    "action": False,
    "live": False,
}
PRE_ADMISSION_WRAPPER_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "role",
        "status",
        "source_receipt",
        "evidence_boundary",
        "permissions",
        "canonical_receipt_sha256",
    }
)
FINAL_RESULT_RECEIPT_ROLE = "final_composition"
FINAL_RESULT_RECEIPT_ROLES = (FINAL_RESULT_RECEIPT_ROLE,)
FINAL_COMPOSITION_STATUS = "owner_buy_e3_final_evidence_composed"
FINAL_COMPOSITION_CANONICAL_FIELD = "canonical_final_composition_receipt_sha256"
FINAL_COMPOSITION_SCHEMA = f"{OWNER_IDENTITY}.final_composition_receipt.v2"
FINAL_COMPOSITION_IDENTITY = f"{OWNER_IDENTITY}.final_composition"


def _final_composition_module() -> Any:
    """Load v2 lazily because its validator also binds this attempt module."""

    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_amendment_v2,
    )

    return causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_amendment_v2


class _FinalCompositionProxy:
    """Preserve the validator seam while resolving the v2 module lazily."""

    def __getattr__(self, name: str) -> Any:
        module = _final_composition_module()
        return getattr(module, name)


final_composition = _FinalCompositionProxy()


STABILITY_VALIDATOR_IDENTITY = (
    "scripts.f05_buy_e3_stability_receipts.validate_stability_wrappers.v1"
)
RESEARCH_UNCHANGED_FIELDS = (
    "sample",
    "baseline",
    "candidate_ladder",
    "folds",
    "estimand",
    "statistics",
    "full_development_refit_artifact",
)
ATTEMPT_PERMISSIONS = {
    "research": False,
    "action": False,
    "live": False,
}
ATTEMPT_EVIDENCE_BOUNDARY = {
    "economic_values_read": False,
    "new_economic_arm_run": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "shadow_or_companion_created": False,
}

_DIRECT_RECEIPT_METADATA_BY_ROLE = {
    "single_day_source": (
        f"{OWNER_IDENTITY}.single_day_stability_receipt.v1",
        OWNER_IDENTITY,
        "exact_owner_one_day_mechanics_complete",
    ),
    "all_fold_zero_economic_source": (
        f"{OWNER_IDENTITY}.all_fold_zero_economic_stability_receipt.v1",
        OWNER_IDENTITY,
        "all_fold_zero_economic_contract_walk_complete",
    ),
    "durability_concurrency_cache_source": (
        f"{OWNER_IDENTITY}.durability_concurrency_cache_stability_receipt.v1",
        OWNER_IDENTITY,
        "durability_concurrency_cache_complete",
    ),
    "parity_layer1_source": (
        f"{OWNER_IDENTITY}.parity_receipt.v1",
        OWNER_IDENTITY,
        "parity_complete",
    ),
    "parity_layer2_source": (
        f"{OWNER_IDENTITY}.parity_receipt.v1",
        OWNER_IDENTITY,
        "parity_complete",
    ),
    "parity_layer3_source": (
        f"{OWNER_IDENTITY}.parity_receipt.v1",
        OWNER_IDENTITY,
        "parity_complete",
    ),
    "parity_layer4_source": (
        f"{OWNER_IDENTITY}.parity_receipt.v2",
        OWNER_IDENTITY,
        "parity_complete",
    ),
    "sell54_source": (
        f"{OWNER_IDENTITY}.parity_receipt.v1",
        OWNER_IDENTITY,
        "parity_complete",
    ),
    "regression_source": (
        f"{OWNER_IDENTITY}.compatible_runtime_regression_test_receipt.v2",
        OWNER_IDENTITY,
        "passed",
    ),
    FINAL_RESULT_RECEIPT_ROLE: (
        FINAL_COMPOSITION_SCHEMA,
        FINAL_COMPOSITION_IDENTITY,
        FINAL_COMPOSITION_STATUS,
    ),
}

_ATTEMPT_ID_RE = re.compile(r"^attempt-[a-z0-9][a-z0-9._-]*$")
_FORMAL_VERSION_ALIAS_RE = re.compile(r"formal[-_]v\d+", re.IGNORECASE)


class ExecutionAttemptError(RuntimeError):
    """Raised when producer and repaired runtime identities cannot be separated."""


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _research_contract() -> dict[str, Any]:
    return {
        "changed": False,
        "unchanged_fields": list(RESEARCH_UNCHANGED_FIELDS),
        "ordinary_bugfix_attempt": True,
    }


def _stable_stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
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


def _read_stable_file_record(
    path: Path,
    label: str,
    *,
    require_private: bool,
) -> tuple[bytes, os.stat_result]:
    candidate = path.expanduser().absolute()
    try:
        lexical_before = candidate.lstat()
    except FileNotFoundError as exc:
        raise ExecutionAttemptError(f"{label} does not exist") from exc
    if stat.S_ISLNK(lexical_before.st_mode):
        raise ExecutionAttemptError(f"{label} must not be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ExecutionAttemptError(f"{label} could not be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if (lexical_before.st_dev, lexical_before.st_ino) != (before.st_dev, before.st_ino):
            raise ExecutionAttemptError(f"{label} changed while it was opened")
        if not stat.S_ISREG(before.st_mode):
            raise ExecutionAttemptError(f"{label} is not a regular file")
        if require_private and (
            stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
        ):
            raise ExecutionAttemptError(f"{label} is not a private single-link file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        lexical_after = candidate.lstat()
    except FileNotFoundError as exc:
        raise ExecutionAttemptError(f"{label} pathname changed during read") from exc
    identity = _stable_stat_identity(before)
    if (
        identity != _stable_stat_identity(after)
        or identity != _stable_stat_identity(lexical_after)
        or len(raw) != before.st_size
    ):
        raise ExecutionAttemptError(f"{label} inode or bytes changed during read")
    return raw, before


def file_sha256(path: Path) -> str:
    raw, _metadata = _read_stable_file_record(
        path,
        "file SHA256 input",
        require_private=False,
    )
    return hashlib.sha256(raw).hexdigest()


def _git(root: Path, *args: str, binary: bool = False) -> str | bytes:
    completed = subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=not binary,
        timeout=20.0,
    )
    return completed.stdout if binary else str(completed.stdout).strip()


def _git_is_ancestor(root: Path, ancestor: str, descendant: str) -> bool:
    completed = subprocess.run(
        ("git", "merge-base", "--is-ancestor", ancestor, descendant),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=20.0,
    )
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    detail = completed.stderr.strip() or completed.stdout.strip() or "unknown Git error"
    raise ExecutionAttemptError(f"could not verify execution ancestry: {detail}")


def _require_clean_worktree(root: Path) -> None:
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise ExecutionAttemptError("execution freeze requires a completely clean worktree")


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ExecutionAttemptError(f"{label} is not a SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise ExecutionAttemptError(f"{label} is not a Git object id")
    return normalized


def _require_attempt_id(value: Any) -> str:
    if (
        type(value) is not str
        or _ATTEMPT_ID_RE.fullmatch(value) is None
        or _FORMAL_VERSION_ALIAS_RE.search(value) is not None
    ):
        raise ExecutionAttemptError("attempt id does not match the frozen contract")
    return value


def _read_private_json_record(
    path: Path, label: str
) -> tuple[dict[str, Any], bytes, os.stat_result]:
    raw, observed = _read_stable_file_record(
        path,
        label,
        require_private=True,
    )
    try:
        payload = json.loads(raw.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExecutionAttemptError(f"{label} is not valid canonical JSON") from exc
    if not isinstance(payload, dict):
        raise ExecutionAttemptError(f"{label} root is not an object")
    return payload, raw, observed


def _read_private_json(path: Path, label: str) -> dict[str, Any]:
    payload, _raw, _metadata = _read_private_json_record(path, label)
    return payload


def _document_sha256(payload: Mapping[str, Any], canonical_field: str) -> str:
    body = dict(payload)
    body.pop(canonical_field, None)
    return canonical_sha256(body)


def _validate_formal_manifest_identity(payload: Mapping[str, Any]) -> str:
    field = "canonical_execution_manifest_sha256"
    embedded = _require_sha256(payload.get(field), "formal manifest embedded hash")
    recomputed = _document_sha256(payload, field)
    fixed = _require_sha256(
        FORMAL_MANIFEST_CANONICAL_SHA256,
        "fixed formal manifest hash",
    )
    if payload.get("identity") != OWNER_IDENTITY:
        raise ExecutionAttemptError("formal attempt2 manifest owner identity drifted")
    if not (embedded == recomputed == fixed):
        raise ExecutionAttemptError(
            "formal attempt2 manifest embedded, recomputed, and fixed identities differ"
        )
    return fixed


def _receipt_canonical_field(payload: Mapping[str, Any], label: str) -> tuple[str, str]:
    matches: list[tuple[str, str]] = []
    for field, raw in payload.items():
        if not isinstance(field, str) or _CANONICAL_FIELD_RE.fullmatch(field) is None:
            continue
        try:
            embedded = _require_sha256(raw, f"{label}.{field}")
        except ExecutionAttemptError:
            continue
        if embedded == _document_sha256(payload, field):
            matches.append((field, embedded))
    if len(matches) != 1:
        raise ExecutionAttemptError(
            f"{label} must contain exactly one self-verifying canonical SHA256 field"
        )
    return matches[0]


def _validate_receipt_metadata(
    payload: Mapping[str, Any],
    label: str,
    *,
    role: str,
    require_owner_wrapper: bool,
) -> tuple[str, str, str]:
    schema = str(payload.get("schema_version", "")).strip()
    identity = str(payload.get("identity", "")).strip()
    status = str(payload.get("status", "")).strip()
    if not schema or not identity:
        raise ExecutionAttemptError(f"{label} receipt metadata is incomplete")
    if require_owner_wrapper:
        if set(payload) != PRE_ADMISSION_WRAPPER_FIELDS:
            raise ExecutionAttemptError(f"{label} owner wrapper fields drifted")
        if (
            schema != PRE_ADMISSION_RECEIPT_WRAPPER_SCHEMA
            or identity != OWNER_IDENTITY
            or payload.get("role") != role
            or status != PRE_ADMISSION_RECEIPT_WRAPPER_STATUS
        ):
            raise ExecutionAttemptError(f"{label} owner wrapper identity drifted")
        boundary = payload.get("evidence_boundary")
        permissions = payload.get("permissions")
        if not isinstance(boundary, Mapping) or dict(boundary) != PRE_ADMISSION_EVIDENCE_BOUNDARY:
            raise ExecutionAttemptError(f"{label} evidence boundary is incomplete or drifted")
        if not isinstance(permissions, Mapping) or dict(permissions) != PRE_ADMISSION_PERMISSIONS:
            raise ExecutionAttemptError(f"{label} permissions are incomplete or drifted")
        return schema, identity, status
    expected = _DIRECT_RECEIPT_METADATA_BY_ROLE.get(role)
    if expected is None:
        raise ExecutionAttemptError(f"{label} receipt role is not admitted")
    if (schema, identity, status) != expected:
        raise ExecutionAttemptError(f"{label} receipt schema, identity, or exact status drifted")
    return schema, identity, status


def _receipt_binding(
    path: Path,
    role: str,
    *,
    require_owner_wrapper: bool,
) -> tuple[dict[str, Any], set[tuple[int, int]]]:
    if _RECEIPT_ROLE_RE.fullmatch(role) is None:
        raise ExecutionAttemptError(f"invalid receipt role: {role!r}")
    source = path.expanduser().absolute()
    payload, raw, metadata = _read_private_json_record(source, f"{role} receipt")
    schema, identity, status = _validate_receipt_metadata(
        payload,
        role,
        role=role,
        require_owner_wrapper=require_owner_wrapper,
    )
    canonical_field, canonical = _receipt_canonical_field(payload, role)
    file_identities = {(metadata.st_dev, metadata.st_ino)}
    if require_owner_wrapper:
        raw_source_binding = payload.get("source_receipt")
        if not isinstance(raw_source_binding, Mapping):
            raise ExecutionAttemptError(f"{role} source receipt binding is missing")
        source_fields = {
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
        if set(raw_source_binding) != source_fields:
            raise ExecutionAttemptError(f"{role} source receipt binding fields drifted")
        observed_source, source_file_identities = _receipt_binding(
            Path(str(raw_source_binding.get("path", ""))),
            f"{role}_source",
            require_owner_wrapper=False,
        )
        if dict(raw_source_binding) != observed_source:
            raise ExecutionAttemptError(f"{role} source receipt bytes or identity drifted")
        if file_identities.intersection(source_file_identities):
            raise ExecutionAttemptError(f"{role} wrapper aliases its source receipt")
        file_identities.update(source_file_identities)
    binding = {
        "path": str(source),
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "mode": "0600",
        "schema_version": schema,
        "identity": identity,
        "status": status,
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
    }
    return binding, file_identities


def _receipt_bindings(
    paths: Mapping[str, Path],
    *,
    required_roles: Sequence[str] | None,
) -> dict[str, dict[str, Any]]:
    normalized = {str(role): Path(path) for role, path in paths.items()}
    if required_roles is not None and set(normalized) != set(required_roles):
        missing = sorted(set(required_roles) - set(normalized))
        extra = sorted(set(normalized) - set(required_roles))
        raise ExecutionAttemptError(
            f"receipt role set is incomplete; missing={missing}, extra={extra}"
        )
    if not normalized:
        raise ExecutionAttemptError("at least one result receipt is required")
    bindings: dict[str, dict[str, Any]] = {}
    seen_files: set[tuple[int, int]] = set()
    require_owner_wrapper = required_roles is not None and set(required_roles) == set(
        PRE_ADMISSION_RECEIPT_ROLES
    )
    for role in sorted(normalized):
        binding, file_identities = _receipt_binding(
            normalized[role],
            role,
            require_owner_wrapper=require_owner_wrapper,
        )
        if seen_files.intersection(file_identities):
            raise ExecutionAttemptError("one receipt file was assigned to multiple roles")
        seen_files.update(file_identities)
        bindings[role] = binding
    return bindings


def _revalidate_receipt_bindings(
    raw: Any,
    *,
    required_roles: Sequence[str] | None,
) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, Mapping):
        raise ExecutionAttemptError("receipt bindings are missing")
    observed = {
        str(role): dict(binding) for role, binding in raw.items() if isinstance(binding, Mapping)
    }
    if len(observed) != len(raw):
        raise ExecutionAttemptError("receipt binding entry is malformed")
    paths: dict[str, Path] = {}
    for role, binding in observed.items():
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
        if set(binding) != expected_fields:
            raise ExecutionAttemptError(f"{role} receipt binding fields drifted")
        paths[role] = Path(str(binding.get("path", "")))
    rebound = _receipt_bindings(paths, required_roles=required_roles)
    if observed != rebound:
        raise ExecutionAttemptError("receipt bytes or canonical identity drifted")
    return rebound


def _annotated_tag_identity(root: Path, tag: str, *, require_head: bool) -> dict[str, str]:
    normalized_tag = str(tag).strip()
    if not normalized_tag or any(char.isspace() for char in normalized_tag):
        raise ExecutionAttemptError("annotated tag name is invalid")
    tag_ref = f"refs/tags/{normalized_tag}"
    if _git(root, "cat-file", "-t", tag_ref) != "tag":
        raise ExecutionAttemptError("execution tag is not annotated")
    tag_object = _require_git_sha(_git(root, "rev-parse", tag_ref), "tag object")
    commit = _require_git_sha(_git(root, "rev-parse", f"{tag_ref}^{{}}"), "tag commit")
    tree = _require_git_sha(_git(root, "rev-parse", f"{commit}^{{tree}}"), "tag tree")
    if require_head and commit != _git(root, "rev-parse", "HEAD"):
        raise ExecutionAttemptError("execution tag does not peel to HEAD")
    return {
        "execution_commit": commit,
        "execution_tree": tree,
        "annotated_tag": normalized_tag,
        "annotated_tag_object": tag_object,
        "tag_peeled_commit": commit,
    }


def _producer_identity(root: Path) -> dict[str, str]:
    observed = _annotated_tag_identity(root, PRODUCER_TAG, require_head=False)
    expected = {
        "execution_commit": PRODUCER_COMMIT,
        "execution_tree": PRODUCER_TREE,
        "annotated_tag": PRODUCER_TAG,
        "annotated_tag_object": PRODUCER_TAG_OBJECT,
        "tag_peeled_commit": PRODUCER_COMMIT,
    }
    if observed != expected:
        raise ExecutionAttemptError("immutable artifact producer Git identity drifted")
    return expected


def _source_bindings(root: Path, commit: str) -> dict[str, dict[str, Any]]:
    bindings = {}
    for role, relative in RUNTIME_SOURCE_PATHS.items():
        working_path = root / relative
        working, metadata = _read_stable_file_record(
            working_path,
            f"runtime source {relative}",
            require_private=False,
        )
        committed = bytes(_git(root, "show", f"{commit}:{relative}", binary=True))
        if working != committed:
            raise ExecutionAttemptError(f"runtime source differs from tagged commit: {relative}")
        bindings[role] = {
            "repository_relative_path": relative,
            "file_sha256": hashlib.sha256(working).hexdigest(),
            "size_bytes": len(working),
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
        }
    return bindings


def _artifact_binding(
    *,
    root: Path,
    manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    formal_manifest_path: Path,
) -> dict[str, Any]:
    paths = {
        "manifest": manifest_path.expanduser().absolute(),
        "policy": policy_path.expanduser().absolute(),
        "predicate_bundle": predicate_bundle_path.expanduser().absolute(),
    }
    records: dict[str, tuple[dict[str, Any], bytes, os.stat_result]] = {}
    for role, path in paths.items():
        record = _read_private_json_record(path, f"artifact {role}")
        if hashlib.sha256(record[1]).hexdigest() != ARTIFACT_FILE_SHA256[role]:
            raise ExecutionAttemptError(f"immutable artifact file drifted: {role}")
        records[role] = record
    manifest = records["manifest"][0]
    policy = records["policy"][0]
    if manifest.get("artifact_sha256") != ARTIFACT_SHA256:
        raise ExecutionAttemptError("immutable artifact canonical identity drifted")
    if (
        policy.get("bindings", {}).get("owner_execution_commit") != PRODUCER_COMMIT
        or policy.get("bindings", {}).get("owner_execution_tag") != PRODUCER_TAG
        or policy.get("bindings", {}).get("owner_execution_manifest_canonical_sha256")
        != FORMAL_MANIFEST_CANONICAL_SHA256
    ):
        raise ExecutionAttemptError("policy no longer binds immutable attempt2 producer")
    runtime = LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=paths["manifest"],
        artifact_manifest_sha256=ARTIFACT_FILE_SHA256["manifest"],
        expected_artifact_sha256=ARTIFACT_SHA256,
        policy_path=paths["policy"],
        policy_sha256=ARTIFACT_FILE_SHA256["policy"],
        predicate_bundle_path=paths["predicate_bundle"],
        predicate_bundle_sha256=ARTIFACT_FILE_SHA256["predicate_bundle"],
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    if runtime.artifact_sha256 != ARTIFACT_SHA256:
        raise ExecutionAttemptError("runtime did not load the immutable artifact exactly")
    for role, path in paths.items():
        after = _read_private_json_record(path, f"artifact {role}")
        before = records[role]
        if (
            after[0] != before[0]
            or after[1] != before[1]
            or _stable_stat_identity(after[2]) != _stable_stat_identity(before[2])
        ):
            raise ExecutionAttemptError(f"immutable artifact changed during load: {role}")
    formal_path = formal_manifest_path.expanduser().absolute()
    formal, formal_raw, formal_metadata = _read_private_json_record(
        formal_path, "formal attempt2 manifest"
    )
    formal_canonical = _validate_formal_manifest_identity(formal)
    return {
        "artifact_sha256": ARTIFACT_SHA256,
        "files": {
            role: {
                "path": str(path),
                "file_sha256": ARTIFACT_FILE_SHA256[role],
                "size_bytes": len(records[role][1]),
                "device": records[role][2].st_dev,
                "inode": records[role][2].st_ino,
            }
            for role, path in paths.items()
        },
        "formal_manifest": {
            "path": str(formal_path),
            "file_sha256": hashlib.sha256(formal_raw).hexdigest(),
            "size_bytes": len(formal_raw),
            "device": formal_metadata.st_dev,
            "inode": formal_metadata.st_ino,
            "canonical_sha256": formal_canonical,
        },
    }


def _stability_context_payload(
    *,
    repository_root: Path,
    runtime_execution: Mapping[str, Any],
    layer4_contract_path: Path,
    layer4_day_receipt_dir: Path,
) -> dict[str, Any]:
    try:
        contract = layer4_contract_path.expanduser().resolve(strict=True)
        day_receipts = layer4_day_receipt_dir.expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        raise ExecutionAttemptError("stability validation context path is missing") from exc
    if not contract.is_file() or not day_receipts.is_dir():
        raise ExecutionAttemptError("stability validation context shape drifted")
    return {
        "validator": STABILITY_VALIDATOR_IDENTITY,
        "repository_root": str(repository_root),
        "execution_commit": runtime_execution["execution_commit"],
        "execution_tag": runtime_execution["annotated_tag"],
        "layer4_contract_path": str(contract),
        "layer4_day_receipt_dir": str(day_receipts),
    }


def _validate_stability_wrappers_substantively(
    *,
    wrappers: Mapping[str, Path],
    context_payload: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    expected_fields = {
        "validator",
        "repository_root",
        "execution_commit",
        "execution_tag",
        "layer4_contract_path",
        "layer4_day_receipt_dir",
    }
    if (
        set(context_payload) != expected_fields
        or context_payload.get("validator") != STABILITY_VALIDATOR_IDENTITY
    ):
        raise ExecutionAttemptError("stability validation context fields drifted")
    from scripts import f05_buy_e3_stability_receipts as stability

    try:
        context = stability.StabilityContext(
            repository_root=Path(str(context_payload["repository_root"])).resolve(strict=True),
            execution_commit=_require_git_sha(
                context_payload["execution_commit"],
                "stability execution commit",
            ),
            execution_tag=str(context_payload["execution_tag"]),
            layer4_contract_path=Path(str(context_payload["layer4_contract_path"])).resolve(
                strict=True
            ),
            layer4_day_receipt_dir=Path(str(context_payload["layer4_day_receipt_dir"])).resolve(
                strict=True
            ),
        )
        validated = stability.validate_stability_wrappers(
            wrappers=wrappers,
            context=context,
        )
    except Exception as exc:
        raise ExecutionAttemptError(
            f"substantive stability wrapper validation failed: {exc}"
        ) from exc
    if not isinstance(validated, Mapping) or set(validated) != set(PRE_ADMISSION_RECEIPT_ROLES):
        raise ExecutionAttemptError("substantive stability validator role set drifted")
    normalized: dict[str, dict[str, Any]] = {}
    for role in PRE_ADMISSION_RECEIPT_ROLES:
        payload = validated[role]
        if not isinstance(payload, Mapping):
            raise ExecutionAttemptError(
                f"substantive stability validator returned a malformed role: {role}"
            )
        normalized[role] = dict(payload)
    return normalized


def build_manifest(
    *,
    repository_root: Path,
    attempt_id: str,
    annotated_tag: str,
    manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    formal_manifest_path: Path,
    pre_admission_receipt_paths: Mapping[str, Path],
    layer4_contract_path: Path,
    layer4_day_receipt_dir: Path,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    normalized_attempt_id = _require_attempt_id(attempt_id)
    _require_clean_worktree(root)
    runtime_execution = _annotated_tag_identity(root, annotated_tag, require_head=True)
    if not _git_is_ancestor(root, PRODUCER_COMMIT, runtime_execution["execution_commit"]):
        raise ExecutionAttemptError("repaired runtime is not a descendant of the producer")
    artifact = _artifact_binding(
        root=root,
        manifest_path=manifest_path,
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
        formal_manifest_path=formal_manifest_path,
    )
    sources = _source_bindings(root, runtime_execution["execution_commit"])
    if set(pre_admission_receipt_paths) != set(PRE_ADMISSION_RECEIPT_ROLES):
        missing = sorted(set(PRE_ADMISSION_RECEIPT_ROLES) - set(pre_admission_receipt_paths))
        extra = sorted(set(pre_admission_receipt_paths) - set(PRE_ADMISSION_RECEIPT_ROLES))
        raise ExecutionAttemptError(
            f"receipt role set is incomplete; missing={missing}, extra={extra}"
        )
    stability_context = _stability_context_payload(
        repository_root=root,
        runtime_execution=runtime_execution,
        layer4_contract_path=layer4_contract_path,
        layer4_day_receipt_dir=layer4_day_receipt_dir,
    )
    substantive_stability = _validate_stability_wrappers_substantively(
        wrappers=pre_admission_receipt_paths,
        context_payload=stability_context,
    )
    pre_admission_evidence = _receipt_bindings(
        pre_admission_receipt_paths,
        required_roles=PRE_ADMISSION_RECEIPT_ROLES,
    )
    if any(
        substantive_stability[role].get("canonical_receipt_sha256")
        != pre_admission_evidence[role]["canonical_sha256"]
        for role in PRE_ADMISSION_RECEIPT_ROLES
    ):
        raise ExecutionAttemptError("substantive stability validation and wrapper bindings differ")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER_IDENTITY,
        "attempt_id": normalized_attempt_id,
        "status": "compatible_runtime_frozen_not_activated",
        "generated_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "research_contract": _research_contract(),
        "artifact_producer_execution": _producer_identity(root),
        "runtime_execution": runtime_execution,
        "runtime_sources": {
            "files": sources,
            "canonical_sha256": canonical_sha256(sources),
        },
        "artifact": artifact,
        "stability_validation_context": stability_context,
        "pre_admission_evidence": pre_admission_evidence,
        "permissions": dict(ATTEMPT_PERMISSIONS),
        "evidence_boundary": dict(ATTEMPT_EVIDENCE_BOUNDARY),
    }
    payload["canonical_execution_attempt_sha256"] = canonical_sha256(payload)
    return payload


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_private_json_exclusive(
    path: Path,
    payload: Mapping[str, Any],
    *,
    label: str,
) -> str:
    destination = path.expanduser().absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
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
            descriptor = os.open(destination, flags, 0o600)
        except FileExistsError as exc:
            raise ExecutionAttemptError(f"{label} already exists") from exc
        created = True
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(destination.parent)
        observed, observed_raw, _metadata = _read_private_json_record(destination, label)
        if observed != dict(payload) or observed_raw != encoded:
            raise ExecutionAttemptError(f"{label} bytes drifted during creation")
        return hashlib.sha256(observed_raw).hexdigest()
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        if created:
            destination.unlink(missing_ok=True)
            _fsync_directory(destination.parent)
        raise


def atomic_write(path: Path, payload: Mapping[str, Any]) -> str:
    """Create an admitted execution manifest exactly once."""

    return _write_private_json_exclusive(
        path,
        payload,
        label="execution attempt manifest",
    )


def validate_manifest(
    path: Path,
    *,
    repository_root: Path,
    require_current_checkout: bool = True,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    payload = _read_private_json(path, "execution attempt manifest")
    canonical = _require_sha256(
        payload.get("canonical_execution_attempt_sha256"), "manifest canonical hash"
    )
    body = dict(payload)
    body.pop("canonical_execution_attempt_sha256", None)
    if canonical_sha256(body) != canonical:
        raise ExecutionAttemptError("execution attempt canonical hash drifted")
    required = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "research_contract",
        "artifact_producer_execution",
        "runtime_execution",
        "runtime_sources",
        "artifact",
        "stability_validation_context",
        "pre_admission_evidence",
        "permissions",
        "evidence_boundary",
        "canonical_execution_attempt_sha256",
    }
    if set(payload) != required:
        raise ExecutionAttemptError("execution attempt manifest fields drifted")
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "compatible_runtime_frozen_not_activated"
        or _require_attempt_id(payload.get("attempt_id")) != payload.get("attempt_id")
        or payload.get("artifact_producer_execution") != _producer_identity(root)
        or payload.get("research_contract") != _research_contract()
        or payload.get("permissions") != ATTEMPT_PERMISSIONS
        or payload.get("evidence_boundary") != ATTEMPT_EVIDENCE_BOUNDARY
    ):
        raise ExecutionAttemptError("execution attempt exact semantic contract drifted")
    if require_current_checkout:
        _require_clean_worktree(root)
    runtime = payload.get("runtime_execution")
    if not isinstance(runtime, Mapping):
        raise ExecutionAttemptError("runtime execution binding is missing")
    observed_runtime = _annotated_tag_identity(
        root,
        str(runtime.get("annotated_tag", "")),
        require_head=require_current_checkout,
    )
    if dict(runtime) != observed_runtime:
        raise ExecutionAttemptError("runtime execution Git identity drifted")
    if not _git_is_ancestor(root, PRODUCER_COMMIT, observed_runtime["execution_commit"]):
        raise ExecutionAttemptError("repaired runtime is not a descendant of the producer")
    sources = _source_bindings(root, observed_runtime["execution_commit"])
    if payload.get("runtime_sources") != {
        "files": sources,
        "canonical_sha256": canonical_sha256(sources),
    }:
        raise ExecutionAttemptError("runtime source binding drifted")
    artifact = payload.get("artifact")
    if not isinstance(artifact, Mapping):
        raise ExecutionAttemptError("artifact binding is missing")
    observed_artifact = _artifact_binding(
        root=root,
        manifest_path=Path(str(artifact.get("files", {}).get("manifest", {}).get("path", ""))),
        policy_path=Path(str(artifact.get("files", {}).get("policy", {}).get("path", ""))),
        predicate_bundle_path=Path(
            str(artifact.get("files", {}).get("predicate_bundle", {}).get("path", ""))
        ),
        formal_manifest_path=Path(str(artifact.get("formal_manifest", {}).get("path", ""))),
    )
    if dict(artifact) != observed_artifact:
        raise ExecutionAttemptError("artifact producer binding drifted")
    raw_stability_context = payload.get("stability_validation_context")
    if not isinstance(raw_stability_context, Mapping):
        raise ExecutionAttemptError("stability validation context is missing")
    observed_stability_context = _stability_context_payload(
        repository_root=root,
        runtime_execution=observed_runtime,
        layer4_contract_path=Path(str(raw_stability_context.get("layer4_contract_path", ""))),
        layer4_day_receipt_dir=Path(str(raw_stability_context.get("layer4_day_receipt_dir", ""))),
    )
    if dict(raw_stability_context) != observed_stability_context:
        raise ExecutionAttemptError("stability validation context identity drifted")
    rebound_evidence = _revalidate_receipt_bindings(
        payload.get("pre_admission_evidence"),
        required_roles=PRE_ADMISSION_RECEIPT_ROLES,
    )
    substantive_stability = _validate_stability_wrappers_substantively(
        wrappers={
            role: Path(rebound_evidence[role]["path"]) for role in PRE_ADMISSION_RECEIPT_ROLES
        },
        context_payload=observed_stability_context,
    )
    if any(
        substantive_stability[role].get("canonical_receipt_sha256")
        != rebound_evidence[role]["canonical_sha256"]
        for role in PRE_ADMISSION_RECEIPT_ROLES
    ):
        raise ExecutionAttemptError("substantive stability validation and manifest bindings differ")
    return payload


def _attempt_manifest_binding(
    path: Path,
    *,
    repository_root: Path,
    require_current_checkout: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    source = path.expanduser().absolute()
    before, before_raw, _before_metadata = _read_private_json_record(
        source, "execution attempt manifest"
    )
    validated = validate_manifest(
        source,
        repository_root=repository_root,
        require_current_checkout=require_current_checkout,
    )
    after, after_raw, _after_metadata = _read_private_json_record(
        source, "execution attempt manifest"
    )
    if before != validated or after != validated or before_raw != after_raw:
        raise ExecutionAttemptError("execution attempt manifest changed during validation")
    return validated, {
        "path": str(source),
        "file_sha256": hashlib.sha256(after_raw).hexdigest(),
        "size_bytes": len(after_raw),
        "canonical_sha256": validated["canonical_execution_attempt_sha256"],
    }


def _final_artifact_binding(attempt: Mapping[str, Any]) -> dict[str, Any]:
    artifact = attempt.get("artifact")
    if not isinstance(artifact, Mapping):
        raise ExecutionAttemptError("attempt artifact binding is missing")
    normalized = json.loads(
        json.dumps(artifact, sort_keys=True, separators=(",", ":"), allow_nan=False)
    )
    return {
        "binding": normalized,
        "canonical_sha256": canonical_sha256(normalized),
    }


def _compatible_execution_attempt_identity(
    attempt: Mapping[str, Any],
) -> dict[str, Any]:
    runtime = attempt.get("runtime_execution")
    evidence = attempt.get("pre_admission_evidence")
    if not isinstance(runtime, Mapping) or not isinstance(evidence, Mapping):
        raise ExecutionAttemptError("compatible execution attempt identity is incomplete")
    if set(evidence) != set(PRE_ADMISSION_RECEIPT_ROLES):
        raise ExecutionAttemptError("compatible execution attempt wrapper role set drifted")
    wrapper_identities: dict[str, str] = {}
    for role in PRE_ADMISSION_RECEIPT_ROLES:
        binding = evidence.get(role)
        if not isinstance(binding, Mapping):
            raise ExecutionAttemptError(f"compatible execution wrapper is missing: {role}")
        wrapper_identities[role] = _require_sha256(
            binding.get("canonical_sha256"),
            f"compatible execution wrapper {role}",
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER_IDENTITY,
        "attempt_id": _require_attempt_id(attempt.get("attempt_id")),
        "canonical_execution_attempt_sha256": _require_sha256(
            attempt.get("canonical_execution_attempt_sha256"),
            "compatible execution attempt canonical SHA256",
        ),
        "execution_commit": _require_git_sha(
            runtime.get("execution_commit"),
            "compatible execution commit",
        ),
        "execution_tree": _require_git_sha(
            runtime.get("execution_tree"),
            "compatible execution tree",
        ),
        "annotated_tag": str(runtime.get("annotated_tag", "")),
        "annotated_tag_object": _require_git_sha(
            runtime.get("annotated_tag_object"),
            "compatible execution tag object",
        ),
        "pre_admission_wrapper_canonical_sha256": wrapper_identities,
    }


def _validated_final_composition_bindings(
    paths: Mapping[str, Path],
    *,
    evidence_root: Path,
    attempt: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], Path]:
    normalized = {str(role): Path(path) for role, path in paths.items()}
    if set(normalized) != set(FINAL_RESULT_RECEIPT_ROLES):
        missing = sorted(set(FINAL_RESULT_RECEIPT_ROLES) - set(normalized))
        extra = sorted(set(normalized) - set(FINAL_RESULT_RECEIPT_ROLES))
        raise ExecutionAttemptError(
            f"final result role set is incomplete; missing={missing}, extra={extra}"
        )
    try:
        root = evidence_root.expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        raise ExecutionAttemptError("final composition evidence root is missing") from exc
    if not root.is_dir():
        raise ExecutionAttemptError("final composition evidence root is not a directory")
    source = normalized[FINAL_RESULT_RECEIPT_ROLE].expanduser().absolute()
    try:
        resolved_target = source.resolve(strict=True)
        resolved_target.relative_to(root)
    except (FileNotFoundError, ValueError) as exc:
        raise ExecutionAttemptError("final composition receipt escapes its evidence root") from exc
    if source != resolved_target:
        raise ExecutionAttemptError(
            "final composition receipt path is not canonical or traverses a symbolic link"
        )
    target = source
    first, raw, metadata = _read_private_json_record(
        target,
        "final composition receipt",
    )
    schema, identity, status = _validate_receipt_metadata(
        first,
        "final composition",
        role=FINAL_RESULT_RECEIPT_ROLE,
        require_owner_wrapper=False,
    )
    canonical_field, canonical = _receipt_canonical_field(
        first,
        "final composition",
    )
    if canonical_field != FINAL_COMPOSITION_CANONICAL_FIELD:
        raise ExecutionAttemptError("final composition canonical field drifted")
    try:
        validated = final_composition.validate_final_composition(
            evidence_root=root,
            receipt_path=target,
        )
    except final_composition.FinalCompositionError as exc:
        raise ExecutionAttemptError(
            f"final composition independent validation failed: {exc}"
        ) from exc
    second, second_raw, second_metadata = _read_private_json_record(
        target,
        "final composition receipt",
    )
    if (
        validated != first
        or second != first
        or second_raw != raw
        or (second_metadata.st_dev, second_metadata.st_ino) != (metadata.st_dev, metadata.st_ino)
    ):
        raise ExecutionAttemptError("final composition changed during validation")
    exact_artifact = validated.get("exact_artifact")
    attempt_artifact = attempt.get("artifact")
    if (
        not isinstance(exact_artifact, Mapping)
        or not isinstance(attempt_artifact, Mapping)
        or exact_artifact.get("artifact_sha256") != attempt_artifact.get("artifact_sha256")
    ):
        raise ExecutionAttemptError(
            "final composition exact artifact does not match the execution attempt"
        )
    compatible_attempt = validated.get("compatible_execution_attempt")
    expected_compatible_attempt = _compatible_execution_attempt_identity(attempt)
    if (
        not isinstance(compatible_attempt, Mapping)
        or dict(compatible_attempt) != expected_compatible_attempt
    ):
        raise ExecutionAttemptError(
            "final composition compatible execution attempt identity drifted"
        )
    binding = {
        "path": str(target),
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "mode": "0600",
        "schema_version": schema,
        "identity": identity,
        "status": status,
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
    }
    return {FINAL_RESULT_RECEIPT_ROLE: binding}, root


def _revalidate_final_composition_bindings(
    raw: Any,
    *,
    evidence_root: Path,
    attempt: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, Mapping) or set(raw) != set(FINAL_RESULT_RECEIPT_ROLES):
        raise ExecutionAttemptError("final composition binding role set drifted")
    binding = raw.get(FINAL_RESULT_RECEIPT_ROLE)
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
    if not isinstance(binding, Mapping) or set(binding) != expected_fields:
        raise ExecutionAttemptError("final composition binding fields drifted")
    observed, _root = _validated_final_composition_bindings(
        {FINAL_RESULT_RECEIPT_ROLE: Path(str(binding.get("path", "")))},
        evidence_root=evidence_root,
        attempt=attempt,
    )
    if dict(raw) != observed:
        raise ExecutionAttemptError("final composition bytes, path, or canonical identity drifted")
    return observed


def build_final_receipt(
    *,
    repository_root: Path,
    attempt_manifest_path: Path,
    result_receipt_paths: Mapping[str, Path],
    composition_evidence_root: Path,
    require_current_checkout: bool = True,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    attempt, attempt_binding = _attempt_manifest_binding(
        attempt_manifest_path,
        repository_root=root,
        require_current_checkout=require_current_checkout,
    )
    results, evidence_root = _validated_final_composition_bindings(
        result_receipt_paths,
        evidence_root=composition_evidence_root,
        attempt=attempt,
    )
    reserved_paths = {
        Path(attempt_binding["path"]).absolute(),
        *(
            Path(str(item.get("path", ""))).absolute()
            for item in attempt["artifact"]["files"].values()
        ),
        Path(str(attempt["artifact"]["formal_manifest"]["path"])).absolute(),
    }
    if any(Path(binding["path"]).absolute() in reserved_paths for binding in results.values()):
        raise ExecutionAttemptError("a result receipt aliases an attempt or artifact input")
    payload: dict[str, Any] = {
        "schema_version": FINAL_RECEIPT_SCHEMA_VERSION,
        "identity": OWNER_IDENTITY,
        "attempt_id": attempt["attempt_id"],
        "status": "compatible_runtime_results_bound",
        "generated_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "attempt_manifest": attempt_binding,
        "runtime_execution": dict(attempt["runtime_execution"]),
        "artifact": _final_artifact_binding(attempt),
        "composition_evidence_root": str(evidence_root),
        "result_receipts": results,
        "permissions": {
            "research": False,
            "action": False,
            "live": False,
        },
        "evidence_boundary": {
            "new_economic_arm_run": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "shadow_or_companion_created": False,
        },
    }
    payload["canonical_final_receipt_sha256"] = _document_sha256(
        payload, "canonical_final_receipt_sha256"
    )
    return payload


def write_final_receipt_exclusive(path: Path, payload: Mapping[str, Any]) -> str:
    return _write_private_json_exclusive(
        path,
        payload,
        label="final receipt",
    )


def finalize_attempt(
    *,
    repository_root: Path,
    attempt_manifest_path: Path,
    result_receipt_paths: Mapping[str, Path],
    composition_evidence_root: Path,
    output_path: Path,
    require_current_checkout: bool = True,
) -> tuple[dict[str, Any], str]:
    payload = build_final_receipt(
        repository_root=repository_root,
        attempt_manifest_path=attempt_manifest_path,
        result_receipt_paths=result_receipt_paths,
        composition_evidence_root=composition_evidence_root,
        require_current_checkout=require_current_checkout,
    )
    file_hash = write_final_receipt_exclusive(output_path, payload)
    return payload, file_hash


def validate_final_receipt(
    path: Path,
    *,
    repository_root: Path,
    require_current_checkout: bool = True,
) -> dict[str, Any]:
    payload = _read_private_json(path, "final receipt")
    canonical = _require_sha256(
        payload.get("canonical_final_receipt_sha256"), "final receipt canonical hash"
    )
    if canonical != _document_sha256(payload, "canonical_final_receipt_sha256"):
        raise ExecutionAttemptError("final receipt canonical hash drifted")
    required = {
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
    if set(payload) != required:
        raise ExecutionAttemptError("final receipt fields drifted")
    if (
        payload.get("schema_version") != FINAL_RECEIPT_SCHEMA_VERSION
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "compatible_runtime_results_bound"
        or payload.get("permissions") != {"research": False, "action": False, "live": False}
        or payload.get("evidence_boundary")
        != {
            "new_economic_arm_run": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "shadow_or_companion_created": False,
        }
    ):
        raise ExecutionAttemptError("final receipt evidence boundary drifted")
    attempt_binding = payload.get("attempt_manifest")
    if not isinstance(attempt_binding, Mapping) or set(attempt_binding) != {
        "path",
        "file_sha256",
        "size_bytes",
        "canonical_sha256",
    }:
        raise ExecutionAttemptError("final receipt attempt binding is malformed")
    attempt, observed_attempt_binding = _attempt_manifest_binding(
        Path(str(attempt_binding.get("path", ""))),
        repository_root=repository_root,
        require_current_checkout=require_current_checkout,
    )
    if dict(attempt_binding) != observed_attempt_binding:
        raise ExecutionAttemptError("final receipt attempt manifest binding drifted")
    if (
        payload.get("attempt_id") != attempt["attempt_id"]
        or payload.get("runtime_execution") != attempt["runtime_execution"]
        or payload.get("artifact") != _final_artifact_binding(attempt)
    ):
        raise ExecutionAttemptError("final receipt Git or artifact binding drifted")
    results = _revalidate_final_composition_bindings(
        payload.get("result_receipts"),
        evidence_root=Path(str(payload.get("composition_evidence_root", ""))),
        attempt=attempt,
    )
    reserved_paths = {
        Path(observed_attempt_binding["path"]).absolute(),
        *(
            Path(str(item.get("path", ""))).absolute()
            for item in attempt["artifact"]["files"].values()
        ),
        Path(str(attempt["artifact"]["formal_manifest"]["path"])).absolute(),
    }
    if any(Path(binding["path"]).absolute() in reserved_paths for binding in results.values()):
        raise ExecutionAttemptError("final receipt result aliases a frozen input")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--repository-root", type=Path, required=True)
    freeze.add_argument("--attempt-id", required=True)
    freeze.add_argument("--annotated-tag", required=True)
    freeze.add_argument("--artifact-manifest", type=Path, required=True)
    freeze.add_argument("--policy", type=Path, required=True)
    freeze.add_argument("--predicate-bundle", type=Path, required=True)
    freeze.add_argument("--formal-manifest", type=Path, required=True)
    freeze.add_argument("--layer4-contract", type=Path, required=True)
    freeze.add_argument("--layer4-day-receipt-dir", type=Path, required=True)
    for role in PRE_ADMISSION_RECEIPT_ROLES:
        freeze.add_argument(
            f"--{role.replace('_', '-')}-receipt",
            dest=f"{role}_receipt",
            type=Path,
            required=True,
        )
    freeze.add_argument("--output", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--repository-root", type=Path, required=True)
    validate.add_argument("--manifest", type=Path, required=True)
    validate.add_argument("--allow-historical-checkout", action="store_true")
    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--repository-root", type=Path, required=True)
    finalize.add_argument("--manifest", type=Path, required=True)
    finalize.add_argument(
        "--result-receipt",
        action="append",
        default=[],
        metavar="ROLE=PATH",
        required=True,
    )
    finalize.add_argument(
        "--composition-evidence-root",
        type=Path,
        required=True,
    )
    finalize.add_argument("--output", type=Path, required=True)
    finalize.add_argument("--allow-historical-checkout", action="store_true")
    validate_final = subparsers.add_parser("validate-final")
    validate_final.add_argument("--repository-root", type=Path, required=True)
    validate_final.add_argument("--receipt", type=Path, required=True)
    validate_final.add_argument("--allow-historical-checkout", action="store_true")
    return parser


def _parse_role_path_arguments(values: Sequence[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for value in values:
        role, separator, raw_path = str(value).partition("=")
        role = role.strip()
        raw_path = raw_path.strip()
        if not separator or _RECEIPT_ROLE_RE.fullmatch(role) is None or not raw_path:
            raise ExecutionAttemptError(f"invalid ROLE=PATH receipt binding: {value!r}")
        if role in parsed:
            raise ExecutionAttemptError(f"duplicate result receipt role: {role}")
        parsed[role] = Path(raw_path)
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "freeze":
        payload = build_manifest(
            repository_root=args.repository_root,
            attempt_id=args.attempt_id,
            annotated_tag=args.annotated_tag,
            manifest_path=args.artifact_manifest,
            policy_path=args.policy,
            predicate_bundle_path=args.predicate_bundle,
            formal_manifest_path=args.formal_manifest,
            pre_admission_receipt_paths={
                role: getattr(args, f"{role}_receipt") for role in PRE_ADMISSION_RECEIPT_ROLES
            },
            layer4_contract_path=args.layer4_contract,
            layer4_day_receipt_dir=args.layer4_day_receipt_dir,
        )
        file_hash = atomic_write(args.output, payload)
        print(
            json.dumps(
                {
                    "path": str(args.output.expanduser().absolute()),
                    "file_sha256": file_hash,
                    "canonical_sha256": payload["canonical_execution_attempt_sha256"],
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "validate":
        payload = validate_manifest(
            args.manifest,
            repository_root=args.repository_root,
            require_current_checkout=not args.allow_historical_checkout,
        )
        print(payload["canonical_execution_attempt_sha256"])
        return 0
    if args.command == "finalize":
        payload, file_hash = finalize_attempt(
            repository_root=args.repository_root,
            attempt_manifest_path=args.manifest,
            result_receipt_paths=_parse_role_path_arguments(args.result_receipt),
            composition_evidence_root=args.composition_evidence_root,
            output_path=args.output,
            require_current_checkout=not args.allow_historical_checkout,
        )
        print(
            json.dumps(
                {
                    "path": str(args.output.expanduser().absolute()),
                    "file_sha256": file_hash,
                    "canonical_sha256": payload["canonical_final_receipt_sha256"],
                },
                sort_keys=True,
            )
        )
        return 0
    payload = validate_final_receipt(
        args.receipt,
        repository_root=args.repository_root,
        require_current_checkout=not args.allow_historical_checkout,
    )
    print(payload["canonical_final_receipt_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
