#!/usr/bin/env python3
"""Materialize, validate, and wrap nine pre-admission BUY E3 receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: E402
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: E402
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 as gate_v1,
)
from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: E402
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_amendment_v2 as parity_v2,
)
from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: E402
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1 as parity_v1,
)
from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: E402
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_refit_v1 as refit,
)
from scripts import f05_buy_e3_execution_attempt as attempt  # noqa: E402

OWNER_IDENTITY = attempt.OWNER_IDENTITY
REQUIRED_ROLES = attempt.PRE_ADMISSION_RECEIPT_ROLES
WRAPPER_SCHEMA = attempt.PRE_ADMISSION_RECEIPT_WRAPPER_SCHEMA
WRAPPER_STATUS = attempt.PRE_ADMISSION_RECEIPT_WRAPPER_STATUS
EVIDENCE_BOUNDARY = attempt.PRE_ADMISSION_EVIDENCE_BOUNDARY
PERMISSIONS = attempt.PRE_ADMISSION_PERMISSIONS
ARTIFACT_SHA256 = attempt.ARTIFACT_SHA256
ARTIFACT_FILE_SHA256 = dict(attempt.ARTIFACT_FILE_SHA256)

SINGLE_DAY_SOURCE_SCHEMA = f"{OWNER_IDENTITY}.single_day_stability_receipt.v1"
ZERO_ECONOMIC_SOURCE_SCHEMA = f"{OWNER_IDENTITY}.all_fold_zero_economic_stability_receipt.v1"
DURABILITY_SOURCE_SCHEMA = f"{OWNER_IDENTITY}.durability_concurrency_cache_stability_receipt.v1"
DURABILITY_HARNESS_SCHEMA = f"{OWNER_IDENTITY}.executor_durability_harness_receipt.v1"
DURABILITY_MEASUREMENT_SCHEMA = f"{OWNER_IDENTITY}.durability_gate_measurements.v2"
DURABILITY_TESTED_SOURCE_MANIFEST_SCHEMA = f"{OWNER_IDENTITY}.durability_tested_source_manifest.v1"
DURABILITY_PROBE_RUN_MANIFEST_SCHEMA = f"{OWNER_IDENTITY}.durability_probe_run_manifest.v1"
DURABILITY_PROBE_CACHE_NAMESPACE_SCHEMA = f"{OWNER_IDENTITY}.durability_probe_cache_namespace.v1"
DURABILITY_PROBE_SCHEMA = f"{OWNER_IDENTITY}.durability_gate_mmap_probe.v2"
DURABILITY_CACHE_PROBE_SCHEMA = f"{OWNER_IDENTITY}.durability_gate_cache_probe.v2"

MATERIALIZED_SOURCE_ROLES = (
    "single_day",
    "all_fold_zero_economic",
    "durability_concurrency_cache",
)
DIRECT_SOURCE_ROLES = tuple(
    role for role in REQUIRED_ROLES if role not in MATERIALIZED_SOURCE_ROLES
)

EXPECTED_ONE_DAY_OPPORTUNITY_COUNT = 81
EXPECTED_OUTER_FOLD_COUNT = 4
EXPECTED_INNER_FOLD_COUNT = 12
EXPECTED_DEVELOPMENT_DAY_COUNT = 30
EXPECTED_WORKER_COUNT = 10
EXPECTED_EMA_HALF_LIFE_COUNT = 10
EXPECTED_EMA_PAIR_COUNT = 45

DURABILITY_CHECKS = (
    "exact_worker_count",
    "intended_concurrency_reached",
    "all_tasks_terminal_before_mmap_close",
    "exception_path_cancels_and_joins_workers",
    "persistent_pool_shutdown_complete",
    "atomic_cache_publish",
    "partial_cache_entries_invisible",
    "exact_probe_cache_namespace_only",
    "interruption_resume_complete",
    "repeated_run_deterministic",
    "mmap_open_close_balanced",
    "zero_native_memory_faults",
)
DURABILITY_FAILURE_COUNTS = (
    "worker_exception_count",
    "unterminated_future_count",
    "cache_mismatch_count",
    "partial_cache_visibility_count",
    "atomic_publish_failure_count",
    "mmap_close_before_terminal_count",
    "mmap_use_after_close_count",
    "segmentation_fault_count",
    "bad_access_count",
)
DURABILITY_HARNESS_GATES = (
    "worker_concurrency",
    "mmap_lifetime",
    "exception_join",
    "atomic_cache",
    "cache_namespace",
    "interruption_resume",
    "repeated_run",
)
DURABILITY_HARNESS_TEST_FILE = "tests/test_f05_buy_e3_durability_gate.py"
DURABILITY_RUNTIME_SOURCE_FILES = (
    "scripts/f05_buy_e3_durability_gate.py",
    "scripts/f05_buy_e3_execution_attempt.py",
    "scripts/f05_buy_e3_stability_receipts.py",
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_"
        "successor_offline_replay_adapter_v1.py"
    ),
)
DURABILITY_GATE_NODEIDS = {
    "worker_concurrency": (
        f"{DURABILITY_HARNESS_TEST_FILE}::test_worker_concurrency_reaches_exact_ten"
    ),
    "mmap_lifetime": (
        f"{DURABILITY_HARNESS_TEST_FILE}::test_mmap_lifetime_waits_for_all_tasks_before_close"
    ),
    "exception_join": (
        f"{DURABILITY_HARNESS_TEST_FILE}::test_injected_exception_cancels_and_joins_before_close"
    ),
    "atomic_cache": (f"{DURABILITY_HARNESS_TEST_FILE}::test_atomic_cache_publish_hides_staging"),
    "cache_namespace": (f"{DURABILITY_HARNESS_TEST_FILE}::test_cache_namespace_is_exact"),
    "interruption_resume": (
        f"{DURABILITY_HARNESS_TEST_FILE}::test_cache_interruption_resume_is_complete"
    ),
    "repeated_run": (f"{DURABILITY_HARNESS_TEST_FILE}::test_repeated_run_hashes_are_deterministic"),
}
DURABILITY_HARNESS_NODEIDS = tuple(sorted(DURABILITY_GATE_NODEIDS.values()))
DURABILITY_SYNTHETIC_FIXTURE_SHA256 = (
    "197f7a314b356f70296099420b30d0beddb9fe80e95054af72e1c382cdf1eb9b"
)
DURABILITY_OBSERVATION_FIELDS = (
    "configured_worker_count",
    "peak_concurrent_worker_count",
    "submitted_task_count",
    "terminal_task_count",
    "pool_shutdown_count",
    "pool_shutdown_complete_count",
    "repeated_run_count",
    "deterministic_repeat_match_count",
    "interruption_resume_count",
    "cache_entry_count",
    "cache_hit_count",
    "atomic_cache_publish_count",
    "mmap_open_count",
    "mmap_close_count",
)
DURABILITY_TEST_COUNT_FIELDS = (
    "collected",
    "executed",
    "passed",
    "failed",
    "errors",
    "skipped",
    "return_code",
)

LEGACY_SINGLE_DAY_STATUS = "exact_owner_one_day_mechanics_complete"
LEGACY_ZERO_ECONOMIC_STATUS = "formal_offline_replay_mechanics_ready"
# Historical attempt-3 stage receipts have no schema, identity, or canonical field.
# Their admitted shape is therefore frozen byte-for-byte through these exact fields.
_LEGACY_SINGLE_DAY_FIELDS = frozenset(
    {
        "status",
        "opportunity_count",
        "exact_owner_noop_parity_count",
        "economic_values_persisted",
        "economic_values_used_for_selection",
        "validation_read",
        "sealed_holdout_read",
    }
)
_LEGACY_ZERO_ECONOMIC_FIELDS = frozenset(
    {
        "status",
        "economic_outcomes_read",
        "outer_fold_count",
        "inner_fold_count",
        "exact_owner_day_count",
        "exact_owner_mismatch_count",
        "validation_read",
        "sealed_holdout_read",
    }
)
_DURABILITY_HARNESS_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "python_executable",
        "python_file_sha256",
        "run_command",
        "nodeids",
        "nodeid_manifest_sha256",
        "gate_nodeids",
        "counts",
        "test_files",
        "runtime_sources",
        "tested_source_manifest_sha256",
        "measurement",
        "measurement_sha256",
        "observations",
        "failure_counts",
        "probe_cache_namespace_sha256",
        "probe_run_manifest_sha256",
        "event_series_sha256",
        "evidence_boundary",
        "permissions",
        "canonical_receipt_sha256",
    }
)
_DURABILITY_MEASUREMENT_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "configured_worker_count",
        "peak_concurrent_worker_count",
        "submitted_task_count",
        "terminal_task_count",
        "repeated_run_count",
        "interruption_resume_count",
        "cache_entry_count",
        "cache_hit_count",
        "mmap_open_count",
        "mmap_close_count",
        "checks",
        "failure_counts",
        "tested_source_manifest",
        "tested_source_manifest_sha256",
        "probe_run_manifest",
        "probe_run_manifest_sha256",
        "probe_cache_namespace",
        "probe_cache_namespace_sha256",
        "event_series_sha256",
        "probe_measurements",
        "cache_measurements",
        "evidence_boundary",
        "permissions",
        "economic_outcomes_read",
        "economic_values_exposed",
        "economic_values_used_for_selection",
        "validation_read",
        "sealed_holdout_read",
    }
)
_MMAP_CASE_FIELDS = frozenset(
    {
        "case",
        "configured_worker_count",
        "submitted_task_count",
        "terminal_task_count",
        "terminal_before_pool_shutdown_count",
        "peak_concurrent_worker_count",
        "cancel_request_count",
        "consumed_result_count",
        "produced_result_count",
        "task_results",
        "task_result_set_sha256",
        "expected_exception_observed",
        "unexpected_worker_exception_count",
        "pool_shutdown_call_count",
        "pool_shutdown_complete",
        "mmap_mode",
        "mmap_open_count",
        "mmap_close_count",
        "mmap_close_before_terminal_count",
        "mmap_use_after_close_count",
        "lifecycle_events",
    }
)
_CACHE_OBSERVATION_FIELDS = frozenset(
    {
        "schema_version",
        "probe_cache_namespace_sha256",
        "probe_run_manifest_sha256",
        "cache_key_sha256",
        "cache_key_probe_namespace_sha256",
        "cache_root_namespace_count",
        "cache_entry_count",
        "cache_hit_count",
        "interruption_resume_count",
        "interrupted_entry_visible_count",
        "stale_partial_after_interruption_count",
        "remaining_partial_entry_count",
        "staging_observed_before_publish",
        "final_complete_observed",
        "public_partial_load_attempt_count",
        "public_partial_load_none_count",
        "public_partial_load_visible_count",
        "public_partial_load_exception_count",
        "observer_join_failure_count",
        "partial_cache_visibility_count",
        "atomic_publish_failure_count",
        "repeated_run_count",
        "repeated_run_result_sha256s",
        "economic_outcomes_read",
        "validation_read",
        "sealed_holdout_read",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_CANONICAL_FIELD_RE = re.compile(r"^canonical_[a-z0-9_]*sha256$")
_MAX_RECEIPT_SIZE_BYTES = 32 << 20
_WRAPPER_FIELDS = frozenset(
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
_SOURCE_BINDING_FIELDS = frozenset(
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
    }
)
_UNDERLYING_BINDING_FIELDS = frozenset(
    {
        *_SOURCE_BINDING_FIELDS,
        "device",
        "inode",
    }
)


class StabilityReceiptError(RuntimeError):
    """Raised when one pre-admission stability identity is not exact."""


@dataclass(frozen=True, slots=True)
class StabilityContext:
    repository_root: Path
    execution_commit: str
    execution_tag: str
    layer4_contract_path: Path
    layer4_day_receipt_dir: Path


@dataclass(frozen=True, slots=True)
class _PrivateJsonRecord:
    path: Path
    payload: dict[str, Any]
    raw: bytes
    device: int
    inode: int


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


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise StabilityReceiptError(f"{label} is not a lowercase SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise StabilityReceiptError(f"{label} is not a Git commit id")
    return normalized


def _require_exact_mapping(value: Any, fields: Sequence[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(fields):
        raise StabilityReceiptError(f"{label} fields drifted")
    return dict(value)


def _read_private_json(path: Path, label: str) -> _PrivateJsonRecord:
    candidate = path.expanduser().absolute()
    try:
        lexical = candidate.lstat()
    except FileNotFoundError as exc:
        raise StabilityReceiptError(f"{label} does not exist") from exc
    if stat.S_ISLNK(lexical.st_mode):
        raise StabilityReceiptError(f"{label} must not be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise StabilityReceiptError(f"{label} could not be opened safely") from exc
    try:
        observed = os.fstat(descriptor)
        if (lexical.st_dev, lexical.st_ino) != (observed.st_dev, observed.st_ino):
            raise StabilityReceiptError(f"{label} changed while it was opened")
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_size <= 0
            or observed.st_size > _MAX_RECEIPT_SIZE_BYTES
        ):
            raise StabilityReceiptError(f"{label} is not an admitted private receipt")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(_MAX_RECEIPT_SIZE_BYTES + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) != observed.st_size or len(raw) > _MAX_RECEIPT_SIZE_BYTES:
        raise StabilityReceiptError(f"{label} size changed while it was read")
    try:
        payload = json.loads(raw.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StabilityReceiptError(f"{label} is not valid ASCII JSON") from exc
    if not isinstance(payload, dict):
        raise StabilityReceiptError(f"{label} root is not an object")
    return _PrivateJsonRecord(
        path=candidate,
        payload=payload,
        raw=raw,
        device=observed.st_dev,
        inode=observed.st_ino,
    )


def _canonical_field(payload: Mapping[str, Any], label: str) -> tuple[str, str]:
    matches: list[tuple[str, str]] = []
    for field, raw in payload.items():
        if not isinstance(field, str) or _CANONICAL_FIELD_RE.fullmatch(field) is None:
            continue
        try:
            embedded = _require_sha256(raw, f"{label}.{field}")
        except StabilityReceiptError:
            continue
        if embedded == _document_sha256(payload, field):
            matches.append((field, embedded))
    if len(matches) != 1:
        raise StabilityReceiptError(
            f"{label} must have exactly one self-verifying canonical SHA256 field"
        )
    return matches[0]


def _optional_canonical_field(
    payload: Mapping[str, Any],
    label: str,
) -> tuple[str | None, str | None]:
    fields = [
        field
        for field in payload
        if isinstance(field, str) and _CANONICAL_FIELD_RE.fullmatch(field) is not None
    ]
    if not fields:
        return None, None
    if len(fields) != 1:
        raise StabilityReceiptError(f"{label} canonical field set drifted")
    field = fields[0]
    embedded = _require_sha256(payload.get(field), f"{label}.{field}")
    if embedded != _document_sha256(payload, field):
        raise StabilityReceiptError(f"{label} canonical SHA256 drifted")
    return field, embedded


def _source_stat_identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _file_sha256(path: Path, *, label: str = "source file") -> str:
    """Hash one regular file through one O_NOFOLLOW descriptor without races."""

    candidate = path.expanduser().absolute()
    try:
        lexical_before = candidate.lstat()
    except FileNotFoundError as exc:
        raise StabilityReceiptError(f"{label} is missing") from exc
    if stat.S_ISLNK(lexical_before.st_mode):
        raise StabilityReceiptError(f"{label} must not be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise StabilityReceiptError(f"{label} could not be opened without following links") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or (lexical_before.st_dev, lexical_before.st_ino) != (
            before.st_dev,
            before.st_ino,
        ):
            raise StabilityReceiptError(f"{label} is not one stable regular file")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        lexical_after = candidate.lstat()
    except FileNotFoundError as exc:
        raise StabilityReceiptError(f"{label} disappeared while hashing") from exc
    if (
        _source_stat_identity(before) != _source_stat_identity(after)
        or (lexical_after.st_dev, lexical_after.st_ino) != (after.st_dev, after.st_ino)
        or stat.S_ISLNK(lexical_after.st_mode)
    ):
        raise StabilityReceiptError(f"{label} changed while hashing")
    return digest.hexdigest()


def _source_binding(record: _PrivateJsonRecord, label: str) -> dict[str, Any]:
    schema = str(record.payload.get("schema_version", "")).strip()
    identity = str(record.payload.get("identity", "")).strip()
    status = str(record.payload.get("status", "")).strip()
    if not schema or not identity or not status:
        raise StabilityReceiptError(f"{label} source metadata is incomplete")
    canonical_field, canonical = _canonical_field(record.payload, label)
    return {
        "path": str(record.path),
        "file_sha256": hashlib.sha256(record.raw).hexdigest(),
        "size_bytes": len(record.raw),
        "mode": "0600",
        "schema_version": schema,
        "identity": identity,
        "status": status,
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
    }


def _underlying_binding(record: _PrivateJsonRecord, label: str) -> dict[str, Any]:
    status_raw = record.payload.get("status")
    if not isinstance(status_raw, str) or not status_raw.strip():
        raise StabilityReceiptError(f"{label} status is missing")
    schema_raw = record.payload.get("schema_version")
    identity_raw = record.payload.get("identity")
    if schema_raw is not None and (not isinstance(schema_raw, str) or not schema_raw.strip()):
        raise StabilityReceiptError(f"{label} schema is malformed")
    if identity_raw is not None and (not isinstance(identity_raw, str) or not identity_raw.strip()):
        raise StabilityReceiptError(f"{label} identity is malformed")
    canonical_field, canonical = _optional_canonical_field(record.payload, label)
    return {
        "path": str(record.path),
        "file_sha256": hashlib.sha256(record.raw).hexdigest(),
        "size_bytes": len(record.raw),
        "mode": "0600",
        "device": record.device,
        "inode": record.inode,
        "schema_version": schema_raw.strip() if isinstance(schema_raw, str) else None,
        "identity": identity_raw.strip() if isinstance(identity_raw, str) else None,
        "status": status_raw.strip(),
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
    }


def _revalidate_underlying_binding(
    raw_binding: Any,
    label: str,
) -> _PrivateJsonRecord:
    binding = _require_exact_mapping(
        raw_binding,
        _UNDERLYING_BINDING_FIELDS,
        f"{label} binding",
    )
    path_raw = binding.get("path")
    if not isinstance(path_raw, str) or not path_raw:
        raise StabilityReceiptError(f"{label} path is missing")
    record = _read_private_json(Path(path_raw), label)
    if binding != _underlying_binding(record, label):
        raise StabilityReceiptError(f"{label} file identity or bytes drifted")
    return record


def _repository_file_map(
    raw: Any,
    *,
    repository_root: Path,
    label: str,
    expected_paths: Sequence[str] | None = None,
) -> dict[str, str]:
    if not isinstance(raw, Mapping) or not raw:
        raise StabilityReceiptError(f"{label} source map is missing")
    root = repository_root.expanduser().resolve(strict=True)
    if expected_paths is not None and set(raw) != set(expected_paths):
        raise StabilityReceiptError(f"{label} source set drifted")
    observed: dict[str, str] = {}
    for raw_name, raw_sha in raw.items():
        if not isinstance(raw_name, str) or not raw_name:
            raise StabilityReceiptError(f"{label} source path is malformed")
        relative = Path(raw_name)
        if relative.is_absolute() or ".." in relative.parts:
            raise StabilityReceiptError(f"{label} source path escapes the repository")
        lexical = root / relative
        if lexical.is_symlink():
            raise StabilityReceiptError(f"{label} source path must not be a symbolic link")
        try:
            source = lexical.resolve(strict=True)
        except FileNotFoundError as exc:
            raise StabilityReceiptError(f"{label} source is missing: {raw_name}") from exc
        if not source.is_file() or (source != root and root not in source.parents):
            raise StabilityReceiptError(f"{label} source is outside the repository")
        expected_sha = _require_sha256(raw_sha, f"{label} source {raw_name}")
        if _file_sha256(lexical, label=f"{label} source {raw_name}") != expected_sha:
            raise StabilityReceiptError(f"{label} source hash drifted: {raw_name}")
        observed[raw_name] = expected_sha
    return observed


def _validate_legacy_single_day(record: _PrivateJsonRecord) -> None:
    payload = record.payload
    if (
        set(payload) != _LEGACY_SINGLE_DAY_FIELDS
        or payload.get("status") != LEGACY_SINGLE_DAY_STATUS
        or type(payload.get("opportunity_count")) is not int
        or payload.get("opportunity_count") != EXPECTED_ONE_DAY_OPPORTUNITY_COUNT
        or type(payload.get("exact_owner_noop_parity_count")) is not int
        or payload.get("exact_owner_noop_parity_count") != EXPECTED_ONE_DAY_OPPORTUNITY_COUNT
        or payload.get("economic_values_persisted") is not False
        or payload.get("economic_values_used_for_selection") is not False
        or payload.get("validation_read") is not False
        or payload.get("sealed_holdout_read") is not False
    ):
        raise StabilityReceiptError("legacy single-day mechanics receipt drifted")


def _validate_legacy_zero_economic(record: _PrivateJsonRecord) -> None:
    payload = record.payload
    if (
        set(payload) != _LEGACY_ZERO_ECONOMIC_FIELDS
        or payload.get("status") != LEGACY_ZERO_ECONOMIC_STATUS
        or payload.get("economic_outcomes_read") is not False
        or type(payload.get("outer_fold_count")) is not int
        or payload.get("outer_fold_count") != EXPECTED_OUTER_FOLD_COUNT
        or type(payload.get("inner_fold_count")) is not int
        or payload.get("inner_fold_count") != EXPECTED_INNER_FOLD_COUNT
        or type(payload.get("exact_owner_day_count")) is not int
        or payload.get("exact_owner_day_count") != EXPECTED_DEVELOPMENT_DAY_COUNT
        or type(payload.get("exact_owner_mismatch_count")) is not int
        or payload.get("exact_owner_mismatch_count") != 0
        or payload.get("validation_read") is not False
        or payload.get("sealed_holdout_read") is not False
    ):
        raise StabilityReceiptError("legacy all-fold zero-economic receipt drifted")


def _with_canonical(payload: Mapping[str, Any]) -> dict[str, Any]:
    completed = dict(payload)
    completed["canonical_receipt_sha256"] = _document_sha256(
        completed,
        "canonical_receipt_sha256",
    )
    return completed


def _single_day_source_payload(record: _PrivateJsonRecord) -> dict[str, Any]:
    _validate_legacy_single_day(record)
    return _with_canonical(
        {
            "schema_version": SINGLE_DAY_SOURCE_SCHEMA,
            "identity": OWNER_IDENTITY,
            "status": LEGACY_SINGLE_DAY_STATUS,
            "underlying_receipt": _underlying_binding(record, "single-day mechanics receipt"),
            "opportunity_count": record.payload["opportunity_count"],
            "exact_owner_noop_parity_count": record.payload["exact_owner_noop_parity_count"],
            "economic_values_persisted": False,
            "economic_values_used_for_selection": False,
            "evidence_boundary": dict(EVIDENCE_BOUNDARY),
            "permissions": dict(PERMISSIONS),
        }
    )


def _zero_economic_source_payload(record: _PrivateJsonRecord) -> dict[str, Any]:
    _validate_legacy_zero_economic(record)
    return _with_canonical(
        {
            "schema_version": ZERO_ECONOMIC_SOURCE_SCHEMA,
            "identity": OWNER_IDENTITY,
            "status": "all_fold_zero_economic_contract_walk_complete",
            "underlying_receipt": _underlying_binding(
                record,
                "all-fold zero-economic stage receipt",
            ),
            "outer_fold_count": record.payload["outer_fold_count"],
            "inner_fold_count": record.payload["inner_fold_count"],
            "day_count": record.payload["exact_owner_day_count"],
            "mismatch_count": record.payload["exact_owner_mismatch_count"],
            "economic_outcomes_read": False,
            "evidence_boundary": dict(EVIDENCE_BOUNDARY),
            "permissions": dict(PERMISSIONS),
        }
    )


def _validate_single_day(payload: Mapping[str, Any]) -> None:
    record = _revalidate_underlying_binding(
        payload.get("underlying_receipt"),
        "single-day mechanics receipt",
    )
    if dict(payload) != _single_day_source_payload(record):
        raise StabilityReceiptError("single-day provenance or no-persist contract drifted")


def _validate_zero_economic(payload: Mapping[str, Any]) -> None:
    record = _revalidate_underlying_binding(
        payload.get("underlying_receipt"),
        "all-fold zero-economic stage receipt",
    )
    if dict(payload) != _zero_economic_source_payload(record):
        raise StabilityReceiptError("all-fold zero-economic provenance contract drifted")


def durability_tested_source_manifest(repository_root: Path) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)

    def source_map(paths: Sequence[str], label: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for relative_name in paths:
            relative = Path(relative_name)
            if relative.is_absolute() or ".." in relative.parts:
                raise StabilityReceiptError(f"{label} path escapes the repository")
            source = root / relative
            result[relative_name] = _file_sha256(
                source,
                label=f"{label} source {relative_name}",
            )
        return result

    return {
        "schema_version": DURABILITY_TESTED_SOURCE_MANIFEST_SCHEMA,
        "test_files": source_map((DURABILITY_HARNESS_TEST_FILE,), "durability test"),
        "runtime_sources": source_map(
            DURABILITY_RUNTIME_SOURCE_FILES,
            "durability runtime",
        ),
    }


def durability_probe_run_manifest(
    *,
    tested_source_manifest_sha256: str,
    synthetic_fixture_sha256: str,
) -> dict[str, Any]:
    fixture_sha256 = _require_sha256(
        synthetic_fixture_sha256,
        "durability synthetic fixture",
    )
    if fixture_sha256 != DURABILITY_SYNTHETIC_FIXTURE_SHA256:
        raise StabilityReceiptError("durability synthetic fixture identity drifted")
    return {
        "schema_version": DURABILITY_PROBE_RUN_MANIFEST_SCHEMA,
        "identity": OWNER_IDENTITY,
        "identity_kind": "pre_admission_zero_economic_probe",
        "tested_source_manifest_sha256": _require_sha256(
            tested_source_manifest_sha256,
            "durability tested source manifest",
        ),
        "configured_worker_count": EXPECTED_WORKER_COUNT,
        "tasks_per_case": EXPECTED_WORKER_COUNT,
        "case_ids": ["success", "injected_exception"],
        "synthetic_fixture_sha256": fixture_sha256,
        "cache_contract_schema": replay_adapter.DAY_CACHE_SCHEMA,
        "final_execution_manifest_bound": False,
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
        "permissions": dict(PERMISSIONS),
    }


def durability_probe_cache_namespace(
    *,
    tested_source_manifest_sha256: str,
    probe_run_manifest_sha256: str,
) -> dict[str, Any]:
    return {
        "schema_version": DURABILITY_PROBE_CACHE_NAMESPACE_SCHEMA,
        "identity": OWNER_IDENTITY,
        "identity_kind": "pre_admission_zero_economic_probe_cache",
        "tested_source_manifest_sha256": _require_sha256(
            tested_source_manifest_sha256,
            "durability tested source manifest",
        ),
        "probe_run_manifest_sha256": _require_sha256(
            probe_run_manifest_sha256,
            "durability probe run manifest",
        ),
        "cache_contract_schema": replay_adapter.DAY_CACHE_SCHEMA,
        "cross_probe_cache_reuse_allowed": False,
        "final_execution_manifest_bound": False,
    }


def durability_event_series(measurement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{OWNER_IDENTITY}.durability_event_series.v1",
        "probe_measurements": measurement.get("probe_measurements"),
        "cache_measurements": measurement.get("cache_measurements"),
    }


def _expected_mmap_lifecycle(case: str) -> list[dict[str, Any]]:
    helper_event = "helper_returned" if case == "success" else "helper_raised_expected"
    return [
        {"sequence": 0, "event": "pool_created"},
        {"sequence": 1, "event": "tasks_submitted"},
        {"sequence": 2, "event": helper_event},
        {"sequence": 3, "event": "all_futures_terminal_before_pool_shutdown"},
        {"sequence": 4, "event": "mmap_closed"},
        {"sequence": 5, "event": "pool_shutdown_complete"},
    ]


def _expected_task_results(case: str) -> list[dict[str, Any]]:
    task_ids = (
        range(EXPECTED_WORKER_COUNT) if case == "success" else range(1, EXPECTED_WORKER_COUNT)
    )
    results: list[dict[str, Any]] = []
    for task_id in task_ids:
        compared_count = 3584
        result_sha256 = canonical_sha256(
            {
                "schema_version": DURABILITY_PROBE_SCHEMA,
                "case": case,
                "task_id": task_id,
                "compared_count": compared_count,
                "economic_outcome": None,
            }
        )
        results.append(
            {
                "task_id": task_id,
                "compared_count": compared_count,
                "result_sha256": result_sha256,
            }
        )
    return results


def derive_atomic_publish_failure_count(cache: Mapping[str, Any]) -> int:
    attempts = int(cache.get("public_partial_load_attempt_count", -1))
    none_count = int(cache.get("public_partial_load_none_count", -1))
    visible = int(cache.get("public_partial_load_visible_count", -1))
    exceptions = int(cache.get("public_partial_load_exception_count", -1))
    observer_join_failures = int(cache.get("observer_join_failure_count", -1))
    return sum(
        (
            int(cache.get("staging_observed_before_publish") is not True),
            int(attempts != 1),
            int(none_count != attempts),
            max(0, visible),
            max(0, exceptions),
            max(0, observer_join_failures),
            int(cache.get("final_complete_observed") is not True),
        )
    )


def durability_measurement_contract(
    probe: Mapping[str, Any],
    cache: Mapping[str, Any],
) -> tuple[dict[str, bool], dict[str, int], dict[str, int]]:
    probe_fields = {
        "schema_version",
        "configured_worker_count",
        "tasks_per_case",
        "fixture_sha256",
        "cases",
        "economic_outcomes_read",
        "validation_read",
        "sealed_holdout_read",
        "subprocess_returncode",
    }
    if (
        set(probe) != probe_fields
        or probe.get("schema_version") != DURABILITY_PROBE_SCHEMA
        or probe.get("fixture_sha256") != DURABILITY_SYNTHETIC_FIXTURE_SHA256
    ):
        raise StabilityReceiptError("durability mmap probe fields drifted")
    cases = probe.get("cases")
    if not isinstance(cases, Mapping) or set(cases) != {"success", "injected_exception"}:
        raise StabilityReceiptError("durability mmap probe case census drifted")
    normalized_cases: dict[str, Mapping[str, Any]] = {}
    for case_name in ("success", "injected_exception"):
        case = cases.get(case_name)
        if not isinstance(case, Mapping) or set(case) != _MMAP_CASE_FIELDS:
            raise StabilityReceiptError(f"durability {case_name} mmap case fields drifted")
        if (
            case.get("case") != case_name
            or case.get("lifecycle_events") != _expected_mmap_lifecycle(case_name)
            or case.get("mmap_mode") != "read_only"
            or type(case.get("expected_exception_observed")) is not bool
            or type(case.get("pool_shutdown_complete")) is not bool
            or case.get("expected_exception_observed") != (case_name == "injected_exception")
            or case.get("consumed_result_count")
            != (0 if case_name == "injected_exception" else EXPECTED_WORKER_COUNT)
        ):
            raise StabilityReceiptError(f"durability {case_name} mmap lifecycle drifted")
        expected_task_results = _expected_task_results(case_name)
        expected_result_hashes = [item["result_sha256"] for item in expected_task_results]
        if (
            case.get("task_results") != expected_task_results
            or case.get("produced_result_count") != len(expected_task_results)
            or case.get("task_result_set_sha256") != canonical_sha256(expected_result_hashes)
        ):
            raise StabilityReceiptError(f"durability {case_name} task measurements drifted")
        for field in _MMAP_CASE_FIELDS - {
            "case",
            "task_results",
            "task_result_set_sha256",
            "expected_exception_observed",
            "pool_shutdown_complete",
            "mmap_mode",
            "lifecycle_events",
        }:
            if type(case.get(field)) is not int:
                raise StabilityReceiptError(f"durability {case_name}.{field} is not an integer")
        _require_sha256(case.get("task_result_set_sha256"), f"durability {case_name} result set")
        normalized_cases[case_name] = case
    if set(cache) != _CACHE_OBSERVATION_FIELDS:
        raise StabilityReceiptError("durability cache observation fields drifted")
    if cache.get("schema_version") != DURABILITY_CACHE_PROBE_SCHEMA:
        raise StabilityReceiptError("durability cache observation schema drifted")
    cache_integer_fields = _CACHE_OBSERVATION_FIELDS - {
        "schema_version",
        "probe_cache_namespace_sha256",
        "probe_run_manifest_sha256",
        "cache_key_sha256",
        "cache_key_probe_namespace_sha256",
        "staging_observed_before_publish",
        "final_complete_observed",
        "repeated_run_result_sha256s",
        "economic_outcomes_read",
        "validation_read",
        "sealed_holdout_read",
    }
    if any(
        type(cache.get(field)) is not int or int(cache[field]) < 0 for field in cache_integer_fields
    ) or any(
        type(cache.get(field)) is not bool
        for field in (
            "staging_observed_before_publish",
            "final_complete_observed",
            "economic_outcomes_read",
            "validation_read",
            "sealed_holdout_read",
        )
    ):
        raise StabilityReceiptError("durability cache observation types drifted")
    for field in (
        "probe_cache_namespace_sha256",
        "probe_run_manifest_sha256",
        "cache_key_sha256",
        "cache_key_probe_namespace_sha256",
    ):
        _require_sha256(cache.get(field), f"durability cache {field}")
    repeated_hashes = cache.get("repeated_run_result_sha256s")
    repeated_deterministic = (
        isinstance(repeated_hashes, list)
        and len(repeated_hashes) >= 2
        and cache.get("repeated_run_count") == len(repeated_hashes)
        and len(set(repeated_hashes)) == 1
        and all(_SHA256_RE.fullmatch(str(value)) is not None for value in repeated_hashes)
    )
    success = normalized_cases["success"]
    exception = normalized_cases["injected_exception"]
    submitted = int(success["submitted_task_count"]) + int(exception["submitted_task_count"])
    terminal = int(success["terminal_task_count"]) + int(exception["terminal_task_count"])
    terminal_before_shutdown = int(success["terminal_before_pool_shutdown_count"]) + int(
        exception["terminal_before_pool_shutdown_count"]
    )
    mmap_open = int(success["mmap_open_count"]) + int(exception["mmap_open_count"])
    mmap_close = int(success["mmap_close_count"]) + int(exception["mmap_close_count"])
    partial_visibility = int(cache.get("public_partial_load_visible_count", -1)) + int(
        cache.get("public_partial_load_exception_count", -1)
    )
    derived_atomic_failures = derive_atomic_publish_failure_count(cache)
    namespace_mismatch = int(
        cache.get("cache_key_probe_namespace_sha256") != cache.get("probe_cache_namespace_sha256")
    )
    failures = {
        "worker_exception_count": int(success["unexpected_worker_exception_count"])
        + int(exception["unexpected_worker_exception_count"]),
        "unterminated_future_count": max(0, submitted - terminal_before_shutdown),
        "cache_mismatch_count": int(not repeated_deterministic) + namespace_mismatch,
        "partial_cache_visibility_count": partial_visibility
        + int(cache.get("interrupted_entry_visible_count", 1)),
        "atomic_publish_failure_count": derived_atomic_failures,
        "mmap_close_before_terminal_count": int(success["mmap_close_before_terminal_count"])
        + int(exception["mmap_close_before_terminal_count"]),
        "mmap_use_after_close_count": int(success["mmap_use_after_close_count"])
        + int(exception["mmap_use_after_close_count"]),
        "segmentation_fault_count": int(probe.get("subprocess_returncode") != 0),
        "bad_access_count": int(probe.get("subprocess_returncode") != 0),
    }
    checks = {
        "exact_worker_count": (
            probe.get("configured_worker_count") == EXPECTED_WORKER_COUNT
            and probe.get("tasks_per_case") == EXPECTED_WORKER_COUNT
            and success["configured_worker_count"] == EXPECTED_WORKER_COUNT
            and exception["configured_worker_count"] == EXPECTED_WORKER_COUNT
            and success["submitted_task_count"] == EXPECTED_WORKER_COUNT
            and exception["submitted_task_count"] == EXPECTED_WORKER_COUNT
        ),
        "intended_concurrency_reached": (
            success["peak_concurrent_worker_count"] == EXPECTED_WORKER_COUNT
            and exception["peak_concurrent_worker_count"] == EXPECTED_WORKER_COUNT
        ),
        "all_tasks_terminal_before_mmap_close": (
            terminal == submitted
            and terminal_before_shutdown == submitted
            and failures["mmap_close_before_terminal_count"] == 0
            and failures["mmap_use_after_close_count"] == 0
        ),
        "exception_path_cancels_and_joins_workers": (
            exception["expected_exception_observed"] is True
            and exception["cancel_request_count"] == EXPECTED_WORKER_COUNT
            and exception["terminal_before_pool_shutdown_count"] == EXPECTED_WORKER_COUNT
        ),
        "persistent_pool_shutdown_complete": (
            success["pool_shutdown_call_count"] == 1
            and exception["pool_shutdown_call_count"] == 1
            and success["pool_shutdown_complete"] is True
            and exception["pool_shutdown_complete"] is True
        ),
        "atomic_cache_publish": (
            cache.get("staging_observed_before_publish") is True
            and cache.get("public_partial_load_attempt_count") == 1
            and cache.get("public_partial_load_none_count")
            == cache.get("public_partial_load_attempt_count")
            and cache.get("final_complete_observed") is True
            and cache.get("atomic_publish_failure_count") == derived_atomic_failures == 0
        ),
        "partial_cache_entries_invisible": (
            cache.get("partial_cache_visibility_count") == partial_visibility == 0
            and cache.get("stale_partial_after_interruption_count") == 0
            and cache.get("remaining_partial_entry_count") == 0
        ),
        "exact_probe_cache_namespace_only": (
            cache.get("cache_root_namespace_count") == 2 and namespace_mismatch == 0
        ),
        "interruption_resume_complete": (
            cache.get("interruption_resume_count") == 1 and cache.get("cache_hit_count", 0) >= 1
        ),
        "repeated_run_deterministic": repeated_deterministic,
        "mmap_open_close_balanced": mmap_open == mmap_close == 2,
        "zero_native_memory_faults": (
            probe.get("subprocess_returncode") == 0
            and failures["segmentation_fault_count"] == 0
            and failures["bad_access_count"] == 0
        ),
    }
    counts = {
        "configured_worker_count": EXPECTED_WORKER_COUNT,
        "peak_concurrent_worker_count": min(
            int(success["peak_concurrent_worker_count"]),
            int(exception["peak_concurrent_worker_count"]),
        ),
        "submitted_task_count": submitted,
        "terminal_task_count": terminal,
        "repeated_run_count": int(cache.get("repeated_run_count", 0)),
        "interruption_resume_count": int(cache.get("interruption_resume_count", 0)),
        "cache_entry_count": int(cache.get("cache_entry_count", 0)),
        "cache_hit_count": int(cache.get("cache_hit_count", 0)),
        "mmap_open_count": mmap_open,
        "mmap_close_count": mmap_close,
    }
    return checks, failures, counts


def durability_measurement_observations(measurement: Mapping[str, Any]) -> dict[str, int]:
    probe = measurement.get("probe_measurements")
    cache = measurement.get("cache_measurements")
    if not isinstance(probe, Mapping) or not isinstance(cache, Mapping):
        raise StabilityReceiptError("durability measurement events are missing")
    cases = probe.get("cases")
    if not isinstance(cases, Mapping):
        raise StabilityReceiptError("durability mmap cases are missing")
    shutdown_count = sum(int(case["pool_shutdown_call_count"]) for case in cases.values())
    shutdown_complete = sum(int(case["pool_shutdown_complete"] is True) for case in cases.values())
    repeated = int(measurement["repeated_run_count"])
    hashes = cache.get("repeated_run_result_sha256s")
    deterministic_matches = (
        repeated - 1
        if isinstance(hashes, list) and len(hashes) == repeated and len(set(hashes)) == 1
        else 0
    )
    return {
        "configured_worker_count": int(measurement["configured_worker_count"]),
        "peak_concurrent_worker_count": int(measurement["peak_concurrent_worker_count"]),
        "submitted_task_count": int(measurement["submitted_task_count"]),
        "terminal_task_count": int(measurement["terminal_task_count"]),
        "pool_shutdown_count": shutdown_count,
        "pool_shutdown_complete_count": shutdown_complete,
        "repeated_run_count": repeated,
        "deterministic_repeat_match_count": deterministic_matches,
        "interruption_resume_count": int(measurement["interruption_resume_count"]),
        "cache_entry_count": int(measurement["cache_entry_count"]),
        "cache_hit_count": int(measurement["cache_hit_count"]),
        "atomic_cache_publish_count": int(
            cache.get("staging_observed_before_publish") is True
            and cache.get("final_complete_observed") is True
            and cache.get("public_partial_load_none_count")
            == cache.get("public_partial_load_attempt_count")
        ),
        "mmap_open_count": int(measurement["mmap_open_count"]),
        "mmap_close_count": int(measurement["mmap_close_count"]),
    }


def validate_durability_measurement(
    measurement: Mapping[str, Any],
    *,
    repository_root: Path,
) -> dict[str, Any]:
    if set(measurement) != _DURABILITY_MEASUREMENT_FIELDS:
        raise StabilityReceiptError("durability measurement fields drifted")
    if (
        measurement.get("schema_version") != DURABILITY_MEASUREMENT_SCHEMA
        or measurement.get("identity") != OWNER_IDENTITY
        or measurement.get("status") != "durability_measurements_complete"
        or measurement.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or measurement.get("permissions") != PERMISSIONS
        or measurement.get("economic_outcomes_read") is not False
        or measurement.get("economic_values_exposed") is not False
        or measurement.get("economic_values_used_for_selection") is not False
        or measurement.get("validation_read") is not False
        or measurement.get("sealed_holdout_read") is not False
    ):
        raise StabilityReceiptError("durability measurement identity or boundary drifted")
    tested_sources = durability_tested_source_manifest(repository_root)
    tested_source_sha = canonical_sha256(tested_sources)
    if (
        measurement.get("tested_source_manifest") != tested_sources
        or measurement.get("tested_source_manifest_sha256") != tested_source_sha
    ):
        raise StabilityReceiptError("durability tested source manifest drifted")
    probe = measurement.get("probe_measurements")
    cache = measurement.get("cache_measurements")
    if not isinstance(probe, Mapping) or not isinstance(cache, Mapping):
        raise StabilityReceiptError("durability raw measurements are missing")
    for label, raw in (("probe", probe), ("cache", cache)):
        if (
            raw.get("economic_outcomes_read") is not False
            or raw.get("validation_read") is not False
            or raw.get("sealed_holdout_read") is not False
        ):
            raise StabilityReceiptError(f"durability {label} crossed the evidence boundary")
    run_manifest = durability_probe_run_manifest(
        tested_source_manifest_sha256=tested_source_sha,
        synthetic_fixture_sha256=probe.get("fixture_sha256"),
    )
    run_manifest_sha = canonical_sha256(run_manifest)
    if (
        measurement.get("probe_run_manifest") != run_manifest
        or measurement.get("probe_run_manifest_sha256") != run_manifest_sha
    ):
        raise StabilityReceiptError("durability probe run manifest drifted")
    cache_namespace = durability_probe_cache_namespace(
        tested_source_manifest_sha256=tested_source_sha,
        probe_run_manifest_sha256=run_manifest_sha,
    )
    cache_namespace_sha = canonical_sha256(cache_namespace)
    if (
        measurement.get("probe_cache_namespace") != cache_namespace
        or measurement.get("probe_cache_namespace_sha256") != cache_namespace_sha
        or cache.get("probe_run_manifest_sha256") != run_manifest_sha
        or cache.get("probe_cache_namespace_sha256") != cache_namespace_sha
    ):
        raise StabilityReceiptError("durability probe cache namespace drifted")
    checks, failures, counts = durability_measurement_contract(probe, cache)
    event_sha = canonical_sha256(durability_event_series(measurement))
    if (
        measurement.get("checks") != checks
        or measurement.get("failure_counts") != failures
        or any(measurement.get(name) != value for name, value in counts.items())
        or measurement.get("event_series_sha256") != event_sha
        or not all(checks.values())
        or any(failures.values())
    ):
        raise StabilityReceiptError("durability measurement contract drifted")
    return dict(measurement)


def _validate_durability_harness_payload(
    payload: Mapping[str, Any],
    repository_root: Path,
) -> None:
    nodeids = payload.get("nodeids")
    gate_nodeids = _require_exact_mapping(
        payload.get("gate_nodeids"),
        DURABILITY_HARNESS_GATES,
        "durability harness gate nodeids",
    )
    counts = _require_exact_mapping(
        payload.get("counts"),
        DURABILITY_TEST_COUNT_FIELDS,
        "durability harness test counts",
    )
    observations = _require_exact_mapping(
        payload.get("observations"),
        DURABILITY_OBSERVATION_FIELDS,
        "durability harness observations",
    )
    failures = _require_exact_mapping(
        payload.get("failure_counts"),
        DURABILITY_FAILURE_COUNTS,
        "durability harness failure counts",
    )
    if (
        not isinstance(nodeids, list)
        or nodeids != list(DURABILITY_HARNESS_NODEIDS)
        or gate_nodeids != DURABILITY_GATE_NODEIDS
        or any(type(value) is not int for value in counts.values())
        or any(type(value) is not int for value in observations.values())
        or any(type(value) is not int for value in failures.values())
    ):
        raise StabilityReceiptError("durability harness counts or nodeids are malformed")
    if set(gate_nodeids.values()) - set(nodeids) or len(set(gate_nodeids.values())) != len(
        DURABILITY_HARNESS_GATES
    ):
        raise StabilityReceiptError("durability harness gate coverage drifted")
    test_files = _repository_file_map(
        payload.get("test_files"),
        repository_root=repository_root,
        label="durability test",
        expected_paths=(DURABILITY_HARNESS_TEST_FILE,),
    )
    runtime_sources = _repository_file_map(
        payload.get("runtime_sources"),
        repository_root=repository_root,
        label="durability runtime",
        expected_paths=DURABILITY_RUNTIME_SOURCE_FILES,
    )
    nodeid_files = {nodeid.partition("::")[0] for nodeid in nodeids}
    if not nodeid_files.issubset(test_files):
        raise StabilityReceiptError("durability nodeids are not bound to their test files")
    executable_raw = payload.get("python_executable")
    if not isinstance(executable_raw, str) or not executable_raw:
        raise StabilityReceiptError("durability Python executable is missing")
    try:
        executable = Path(executable_raw).expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        raise StabilityReceiptError("durability Python executable is missing") from exc
    run_command = payload.get("run_command")
    expected_command = [str(executable), "-m", "pytest", "-q", *nodeids]
    source_manifest = {
        "schema_version": DURABILITY_TESTED_SOURCE_MANIFEST_SCHEMA,
        "test_files": test_files,
        "runtime_sources": runtime_sources,
    }
    measurement_raw = payload.get("measurement")
    if not isinstance(measurement_raw, Mapping):
        raise StabilityReceiptError("durability harness measurement is missing")
    measurement = validate_durability_measurement(
        measurement_raw,
        repository_root=repository_root,
    )
    derived_observations = durability_measurement_observations(measurement)
    derived_failures = measurement["failure_counts"]
    if (
        set(payload) != _DURABILITY_HARNESS_FIELDS
        or payload.get("schema_version") != DURABILITY_HARNESS_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "durability_harness_passed"
        or payload.get("python_executable") != str(executable)
        or not executable.is_file()
        or not os.access(executable, os.X_OK)
        or payload.get("python_file_sha256")
        != _file_sha256(executable, label="durability Python executable")
        or run_command != expected_command
        or payload.get("nodeid_manifest_sha256") != canonical_sha256(nodeids)
        or payload.get("tested_source_manifest_sha256") != canonical_sha256(source_manifest)
        or source_manifest != measurement["tested_source_manifest"]
        or payload.get("measurement_sha256") != canonical_sha256(measurement)
        or observations != derived_observations
        or failures != derived_failures
        or counts["collected"] != len(nodeids)
        or counts["executed"] != len(nodeids)
        or counts["passed"] != len(nodeids)
        or any(counts[name] != 0 for name in ("failed", "errors", "skipped"))
        or counts["return_code"] != 0
        or any(derived_failures[name] != 0 for name in DURABILITY_FAILURE_COUNTS)
        or payload.get("probe_cache_namespace_sha256")
        != measurement["probe_cache_namespace_sha256"]
        or payload.get("probe_run_manifest_sha256") != measurement["probe_run_manifest_sha256"]
        or payload.get("event_series_sha256") != measurement["event_series_sha256"]
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get("permissions") != PERMISSIONS
        or payload.get("canonical_receipt_sha256")
        != _document_sha256(payload, "canonical_receipt_sha256")
    ):
        raise StabilityReceiptError("durability harness identity or execution evidence drifted")
    for field in (
        "tested_source_manifest_sha256",
        "measurement_sha256",
        "probe_cache_namespace_sha256",
        "probe_run_manifest_sha256",
        "event_series_sha256",
    ):
        _require_sha256(payload.get(field), f"durability harness {field}")


def _git_bytes(repository_root: Path, *arguments: str, label: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(repository_root), *arguments],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise StabilityReceiptError(f"{label} failed: {detail or 'no diagnostic'}")
    return completed.stdout


def _validate_durability_sources_at_freeze(
    payload: Mapping[str, Any],
    context: StabilityContext,
) -> None:
    root = context.repository_root.expanduser().resolve(strict=True)
    commit = (
        _git_bytes(
            root,
            "rev-parse",
            "--verify",
            f"{context.execution_commit}^{{commit}}",
            label="durability execution commit resolution",
        )
        .decode("ascii")
        .strip()
    )
    if commit != context.execution_commit:
        raise StabilityReceiptError("durability execution commit resolution drifted")
    tag_ref = f"refs/tags/{context.execution_tag}"
    tag_type = (
        _git_bytes(
            root,
            "cat-file",
            "-t",
            tag_ref,
            label="durability annotated tag type",
        )
        .decode("ascii")
        .strip()
    )
    if tag_type != "tag":
        raise StabilityReceiptError("durability execution tag is not annotated")
    tag_commit = (
        _git_bytes(
            root,
            "rev-parse",
            "--verify",
            f"{tag_ref}^{{commit}}",
            label="durability annotated tag commit",
        )
        .decode("ascii")
        .strip()
    )
    if tag_commit != commit:
        raise StabilityReceiptError("durability annotated tag does not bind the execution commit")
    source_maps = (payload["test_files"], payload["runtime_sources"])
    for source_map in source_maps:
        for relative_name, expected_sha in source_map.items():
            blob = _git_bytes(
                root,
                "cat-file",
                "blob",
                f"{commit}:{relative_name}",
                label=f"durability frozen source {relative_name}",
            )
            if hashlib.sha256(blob).hexdigest() != expected_sha:
                raise StabilityReceiptError(
                    f"durability frozen source hash drifted: {relative_name}"
                )


def _stable_durability_harness_record(
    path: Path,
    context: StabilityContext,
) -> _PrivateJsonRecord:
    first = _read_private_json(path, "durability harness receipt")
    _validate_durability_harness_payload(first.payload, context.repository_root)
    _validate_durability_sources_at_freeze(first.payload, context)
    second = _read_private_json(first.path, "durability harness receipt")
    if (
        first.raw != second.raw
        or first.payload != second.payload
        or (first.device, first.inode) != (second.device, second.inode)
    ):
        raise StabilityReceiptError("durability harness changed during validation")
    return second


def _stable_regression_record(
    path: Path,
    context: StabilityContext,
) -> _PrivateJsonRecord:
    first = _read_private_json(path, "runtime regression receipt")
    _validate_regression(first.path, first.payload, context)
    second = _read_private_json(first.path, "runtime regression receipt")
    if (
        first.raw != second.raw
        or first.payload != second.payload
        or (first.device, first.inode) != (second.device, second.inode)
    ):
        raise StabilityReceiptError("runtime regression receipt changed during validation")
    return second


def _validate_harness_regression_alignment(
    harness: Mapping[str, Any],
    regression: Mapping[str, Any],
) -> None:
    regression_nodeids = regression.get("nodeids")
    regression_tests = regression.get("test_files")
    regression_sources = regression.get("runtime_sources")
    if (
        not isinstance(regression_nodeids, list)
        or not set(harness["nodeids"]).issubset(regression_nodeids)
        or not isinstance(regression_tests, Mapping)
        or any(regression_tests.get(name) != sha for name, sha in harness["test_files"].items())
        or not isinstance(regression_sources, Mapping)
        or any(
            regression_sources.get(name) != sha for name, sha in harness["runtime_sources"].items()
        )
        or regression.get("python_executable") != harness.get("python_executable")
        or regression.get("python_file_sha256") != harness.get("python_file_sha256")
    ):
        raise StabilityReceiptError("durability harness is not covered by regression evidence")


def _derived_durability_checks(
    observations: Mapping[str, Any],
    failures: Mapping[str, Any],
) -> dict[str, bool]:
    submitted = observations["submitted_task_count"]
    mmap_open = observations["mmap_open_count"]
    repeated = observations["repeated_run_count"]
    return {
        "exact_worker_count": observations["configured_worker_count"] == EXPECTED_WORKER_COUNT,
        "intended_concurrency_reached": observations["peak_concurrent_worker_count"]
        == EXPECTED_WORKER_COUNT,
        "all_tasks_terminal_before_mmap_close": (
            submitted >= EXPECTED_WORKER_COUNT
            and observations["terminal_task_count"] == submitted
            and failures["mmap_close_before_terminal_count"] == 0
        ),
        "exception_path_cancels_and_joins_workers": (
            failures["worker_exception_count"] == 0 and failures["unterminated_future_count"] == 0
        ),
        "persistent_pool_shutdown_complete": (
            observations["pool_shutdown_count"] >= 1
            and observations["pool_shutdown_complete_count"] == observations["pool_shutdown_count"]
        ),
        "atomic_cache_publish": (
            observations["atomic_cache_publish_count"] >= 1
            and failures["atomic_publish_failure_count"] == 0
        ),
        "partial_cache_entries_invisible": (
            observations["cache_entry_count"] >= 1
            and observations["cache_hit_count"] >= 1
            and failures["partial_cache_visibility_count"] == 0
        ),
        "exact_probe_cache_namespace_only": failures["cache_mismatch_count"] == 0,
        "interruption_resume_complete": observations["interruption_resume_count"] >= 1,
        "repeated_run_deterministic": (
            repeated >= 2 and observations["deterministic_repeat_match_count"] >= repeated - 1
        ),
        "mmap_open_close_balanced": (
            mmap_open >= 1
            and observations["mmap_close_count"] == mmap_open
            and failures["mmap_use_after_close_count"] == 0
        ),
        "zero_native_memory_faults": (
            failures["segmentation_fault_count"] == 0 and failures["bad_access_count"] == 0
        ),
    }


def _durability_source_payload(
    harness_record: _PrivateJsonRecord,
    regression_record: _PrivateJsonRecord,
) -> dict[str, Any]:
    harness = harness_record.payload
    regression = regression_record.payload
    _validate_harness_regression_alignment(harness, regression)
    observations = dict(harness["observations"])
    failures = dict(harness["failure_counts"])
    checks = _derived_durability_checks(observations, failures)
    if any(checks[name] is not True for name in DURABILITY_CHECKS):
        raise StabilityReceiptError("durability harness observations fail a required gate")
    return _with_canonical(
        {
            "schema_version": DURABILITY_SOURCE_SCHEMA,
            "identity": OWNER_IDENTITY,
            "status": "durability_concurrency_cache_complete",
            "underlying_receipts": {
                "durability_harness": _underlying_binding(
                    harness_record,
                    "durability harness receipt",
                ),
                "regression": _underlying_binding(
                    regression_record,
                    "runtime regression receipt",
                ),
            },
            "nodeid_manifest_sha256": harness["nodeid_manifest_sha256"],
            "tested_source_manifest_sha256": harness["tested_source_manifest_sha256"],
            "regression_nodeid_manifest_sha256": regression["nodeid_manifest_sha256"],
            "observations": observations,
            "checks": checks,
            "failure_counts": failures,
            "measurement_sha256": harness["measurement_sha256"],
            "probe_cache_namespace_sha256": harness["probe_cache_namespace_sha256"],
            "probe_run_manifest_sha256": harness["probe_run_manifest_sha256"],
            "event_series_sha256": harness["event_series_sha256"],
            "evidence_boundary": dict(EVIDENCE_BOUNDARY),
            "permissions": dict(PERMISSIONS),
        }
    )


def _validate_durability(
    payload: Mapping[str, Any],
    context: StabilityContext,
) -> None:
    receipts = _require_exact_mapping(
        payload.get("underlying_receipts"),
        ("durability_harness", "regression"),
        "durability underlying receipts",
    )
    harness_record = _revalidate_underlying_binding(
        receipts["durability_harness"],
        "durability harness receipt",
    )
    regression_record = _revalidate_underlying_binding(
        receipts["regression"],
        "runtime regression receipt",
    )
    _validate_durability_harness_payload(harness_record.payload, context.repository_root)
    _validate_durability_sources_at_freeze(harness_record.payload, context)
    _validate_regression(regression_record.path, regression_record.payload, context)
    if dict(payload) != _durability_source_payload(harness_record, regression_record):
        raise StabilityReceiptError("durability provenance contract drifted")


def _validate_exact_artifact_binding(payload: Mapping[str, Any], label: str) -> None:
    if (
        payload.get("artifact_sha256") != ARTIFACT_SHA256
        or payload.get("artifact_manifest_file_sha256") != ARTIFACT_FILE_SHA256["manifest"]
        or payload.get("policy_file_sha256") != ARTIFACT_FILE_SHA256["policy"]
        or payload.get("predicate_bundle_file_sha256") != ARTIFACT_FILE_SHA256["predicate_bundle"]
    ):
        raise StabilityReceiptError(f"{label} exact artifact binding drifted")


def _validate_parity_layer(
    path: Path,
    payload: Mapping[str, Any],
    *,
    expected_layer: str,
) -> None:
    validated = parity_v1.validate_parity_receipt(
        path,
        expected_layer=expected_layer,
        expected_artifact_sha256=ARTIFACT_SHA256,
    )
    if dict(validated) != dict(payload):
        raise StabilityReceiptError("parity validator returned different receipt bytes")
    _validate_exact_artifact_binding(payload, expected_layer)
    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        raise StabilityReceiptError(f"{expected_layer} evidence is missing")
    if expected_layer == parity_v1.RESEARCH_COMPILED_LAYER:
        if (
            evidence.get("structural_rule_tree_equal") is not True
            or evidence.get("logical_vector_count") != parity_v1.DEFAULT_VECTOR_LIMIT
            or evidence.get("mismatch_count") != 0
            or evidence.get("predicate_count", 0) < 1
            or evidence.get("rule_count", 0) < 1
        ):
            raise StabilityReceiptError("Layer1 research/compiled parity drifted")
        _require_sha256(evidence.get("logical_vector_sha256"), "Layer1 vector identity")
        _require_sha256(evidence.get("decision_signature_sha256"), "Layer1 decision identity")
    elif expected_layer == parity_v1.DEVELOPMENT_SNAPSHOT_LAYER:
        buy_count = evidence.get("buy_snapshot_count")
        sell_count = evidence.get("sell_snapshot_count")
        if (
            evidence.get("opportunity_count") != refit.EXPECTED_OPPORTUNITY_COUNT
            or not isinstance(buy_count, int)
            or not isinstance(sell_count, int)
            or buy_count <= 0
            or sell_count <= 0
            or buy_count + sell_count != refit.EXPECTED_OPPORTUNITY_COUNT
            or evidence.get("selected_predicate_count", 0) < 1
            or evidence.get("predicate_projection_mismatch_count") != 0
            or evidence.get("action_duration_mismatch_count") != 0
        ):
            raise StabilityReceiptError("Layer2 Development snapshot parity drifted")
        for field in (
            "snapshot_signature_sha256",
            "mechanics_receipt_sha256",
            "frozen_source_predicate_bundle_sha256",
        ):
            _require_sha256(evidence.get(field), f"Layer2 {field}")
    elif expected_layer == parity_v1.STREAMING_OFFLINE_LAYER:
        callback_count = parity_v1.DEFAULT_STREAMING_CALLBACK_COUNT
        if (
            evidence.get("callback_count") != callback_count
            or evidence.get("completed_window_count") != callback_count - 1
            or evidence.get("ema_half_life_count") != EXPECTED_EMA_HALF_LIFE_COUNT
            or evidence.get("ema_pair_count") != EXPECTED_EMA_PAIR_COUNT
            or evidence.get("feature_count", 0) < 1
            or evidence.get("feature_mismatch_count") != 0
            or evidence.get("gap_reset_count") != 0
            or evidence.get("out_of_order_count") != 0
        ):
            raise StabilityReceiptError("Layer3 streaming/offline parity drifted")
        _require_sha256(evidence.get("feature_signature_sha256"), "Layer3 feature identity")
    else:
        raise StabilityReceiptError("unsupported parity layer")


def _validate_layer4(
    path: Path,
    payload: Mapping[str, Any],
    context: StabilityContext,
) -> None:
    validated = parity_v2.validate_layer4_receipt_v2(
        path,
        contract_path=context.layer4_contract_path,
        day_receipt_dir=context.layer4_day_receipt_dir,
    )
    if dict(validated) != dict(payload):
        raise StabilityReceiptError("Layer4 validator returned different receipt bytes")
    _validate_exact_artifact_binding(payload, "Layer4")
    evidence = payload.get("evidence")
    day_receipts = evidence.get("day_receipts") if isinstance(evidence, Mapping) else None
    if (
        payload.get("schema_version") != parity_v2.LAYER4_RECEIPT_SCHEMA_V2
        or payload.get("layer") != parity_v2.LAYER4_LAYER
        or not isinstance(evidence, Mapping)
        or evidence.get("day_count") != EXPECTED_DEVELOPMENT_DAY_COUNT
        or not isinstance(day_receipts, list)
        or len(day_receipts) != EXPECTED_DEVELOPMENT_DAY_COUNT
        or evidence.get("mismatch_count") != 0
        or evidence.get("deadline_lockstep") is not True
        or evidence.get("fill_lockstep") is not True
        or evidence.get("campaign_lockstep") is not True
    ):
        raise StabilityReceiptError("Layer4 30-day aggregate drifted")
    for index, binding in enumerate(day_receipts):
        if not isinstance(binding, Mapping):
            raise StabilityReceiptError(f"Layer4 day binding {index} is malformed")
        _require_sha256(binding.get("file_sha256"), f"Layer4 day {index} file")
        _require_sha256(
            binding.get("canonical_day_receipt_sha256"),
            f"Layer4 day {index} canonical receipt",
        )


def _validate_sell54(path: Path, payload: Mapping[str, Any], context: StabilityContext) -> None:
    validated = gate_v1.validate_sell_owner_54_case_receipt(
        path,
        repository_root=context.repository_root,
        expected_artifact_sha256=ARTIFACT_SHA256,
        expected_artifact_files=ARTIFACT_FILE_SHA256,
    )
    if not isinstance(validated, Mapping):
        raise StabilityReceiptError("SELL54 validator returned no source binding")
    _validate_exact_artifact_binding(payload, "SELL54")
    evidence = payload.get("evidence")
    if (
        not isinstance(evidence, Mapping)
        or evidence.get("sell_tri_state_cases") != 27
        or evidence.get("buy_tri_state_cases") != 27
        or evidence.get("mismatch_count") != 0
        or evidence.get("documented_semantics_equal") is not True
        or evidence.get("runtime_binding_valid") is not True
    ):
        raise StabilityReceiptError("SELL54 exact parity drifted")
    _require_sha256(validated.get("source_manifest_sha256"), "SELL54 source manifest")


def _validate_regression(
    path: Path,
    payload: Mapping[str, Any],
    context: StabilityContext,
) -> None:
    validated = gate_v1.validate_runtime_regression_receipt(
        path,
        repository_root=context.repository_root,
        expected_artifact_sha256=ARTIFACT_SHA256,
        expected_execution_commit=context.execution_commit,
        expected_execution_tag=context.execution_tag,
    )
    if dict(validated) != dict(payload):
        raise StabilityReceiptError("regression validator returned different receipt bytes")
    nodeids = payload.get("nodeids")
    test_files = payload.get("test_files")
    runtime_sources = payload.get("runtime_sources")
    if (
        payload.get("status") != "passed"
        or not isinstance(nodeids, list)
        or not nodeids
        or payload.get("nodeid_manifest_sha256") != canonical_sha256(nodeids)
        or not isinstance(test_files, Mapping)
        or not test_files
        or not isinstance(runtime_sources, Mapping)
        or not runtime_sources
        or payload.get("collected") != len(nodeids)
        or payload.get("executed") != len(nodeids)
        or payload.get("passed") != len(nodeids)
        or any(payload.get(field) != 0 for field in ("failed", "errors", "skipped"))
        or payload.get("collection_return_code") != 0
        or payload.get("return_code") != 0
    ):
        raise StabilityReceiptError("regression nodeid/source or zero-failure contract drifted")
    for group_name, group in (("test", test_files), ("runtime", runtime_sources)):
        for source_name, source_sha in group.items():
            _require_sha256(source_sha, f"regression {group_name} source {source_name}")


def validate_source_receipt(
    role: str,
    path: Path,
    context: StabilityContext,
) -> tuple[dict[str, Any], dict[str, Any], tuple[int, int]]:
    if role not in REQUIRED_ROLES:
        raise StabilityReceiptError(f"unknown stability role: {role}")
    first = _read_private_json(path, f"{role} source receipt")
    payload = first.payload
    if role == "single_day":
        _validate_single_day(payload)
    elif role == "all_fold_zero_economic":
        _validate_zero_economic(payload)
    elif role == "durability_concurrency_cache":
        _validate_durability(payload, context)
    elif role == "parity_layer1":
        _validate_parity_layer(
            first.path,
            payload,
            expected_layer=parity_v1.RESEARCH_COMPILED_LAYER,
        )
    elif role == "parity_layer2":
        _validate_parity_layer(
            first.path,
            payload,
            expected_layer=parity_v1.DEVELOPMENT_SNAPSHOT_LAYER,
        )
    elif role == "parity_layer3":
        _validate_parity_layer(
            first.path,
            payload,
            expected_layer=parity_v1.STREAMING_OFFLINE_LAYER,
        )
    elif role == "parity_layer4":
        _validate_layer4(first.path, payload, context)
    elif role == "sell54":
        _validate_sell54(first.path, payload, context)
    elif role == "regression":
        _validate_regression(first.path, payload, context)
    else:  # pragma: no cover - REQUIRED_ROLES and dispatch are frozen together.
        raise StabilityReceiptError(f"stability role has no validator: {role}")
    second = _read_private_json(first.path, f"{role} source receipt")
    if (
        second.raw != first.raw
        or (second.device, second.inode) != (first.device, first.inode)
        or second.payload != first.payload
    ):
        raise StabilityReceiptError(f"{role} source receipt changed during validation")
    return second.payload, _source_binding(second, role), (second.device, second.inode)


def _wrapper_payload(role: str, source_binding: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": WRAPPER_SCHEMA,
        "identity": OWNER_IDENTITY,
        "role": role,
        "status": WRAPPER_STATUS,
        "source_receipt": dict(source_binding),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
        "permissions": dict(PERMISSIONS),
    }
    payload["canonical_receipt_sha256"] = _document_sha256(payload, "canonical_receipt_sha256")
    return payload


def _exclusive_private_json(path: Path, payload: Mapping[str, Any]) -> None:
    destination = path.expanduser().absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as exc:
            raise StabilityReceiptError(
                f"immutable private receipt already exists: {destination}"
            ) from exc
        directory_fd = os.open(destination.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(directory_fd)
            temporary.unlink()
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _stable_legacy_record(
    path: Path,
    *,
    label: str,
    validator: Any,
) -> _PrivateJsonRecord:
    first = _read_private_json(path, label)
    validator(first)
    second = _read_private_json(first.path, label)
    if (
        first.raw != second.raw
        or first.payload != second.payload
        or (first.device, first.inode) != (second.device, second.inode)
    ):
        raise StabilityReceiptError(f"{label} changed during validation")
    return second


def materialize_strict_source_receipts(
    *,
    single_day_stage_path: Path,
    zero_economic_stage_path: Path,
    durability_harness_path: Path,
    regression_receipt_path: Path,
    output_dir: Path,
    context: StabilityContext,
) -> dict[str, Path]:
    """Bind real mechanics, preflight, harness, and regression evidence."""

    destination = output_dir.expanduser().absolute()
    paths = {role: destination / f"{role}.json" for role in MATERIALIZED_SOURCE_ROLES}
    if any(path.exists() or path.is_symlink() for path in paths.values()):
        raise StabilityReceiptError("one or more immutable strict source paths already exist")
    single_day = _stable_legacy_record(
        single_day_stage_path,
        label="single-day mechanics stage receipt",
        validator=_validate_legacy_single_day,
    )
    zero_economic = _stable_legacy_record(
        zero_economic_stage_path,
        label="all-fold zero-economic stage receipt",
        validator=_validate_legacy_zero_economic,
    )
    harness = _stable_durability_harness_record(durability_harness_path, context)
    regression = _stable_regression_record(regression_receipt_path, context)
    identities = {
        (record.device, record.inode) for record in (single_day, zero_economic, harness, regression)
    }
    if len(identities) != 4:
        raise StabilityReceiptError("one underlying receipt was assigned multiple evidence roles")
    payloads = {
        "single_day": _single_day_source_payload(single_day),
        "all_fold_zero_economic": _zero_economic_source_payload(zero_economic),
        "durability_concurrency_cache": _durability_source_payload(
            harness,
            regression,
        ),
    }
    for role in MATERIALIZED_SOURCE_ROLES:
        _exclusive_private_json(paths[role], payloads[role])
    for role in MATERIALIZED_SOURCE_ROLES:
        validate_source_receipt(role, paths[role], context)
    return paths


def _normalized_role_paths(
    values: Mapping[str, Path],
    *,
    label: str,
    required_roles: Sequence[str] = REQUIRED_ROLES,
) -> dict[str, Path]:
    normalized = {str(role): Path(path) for role, path in values.items()}
    missing = sorted(set(required_roles) - set(normalized))
    extra = sorted(set(normalized) - set(required_roles))
    if missing or extra:
        raise StabilityReceiptError(f"{label} role set drifted; missing={missing}, extra={extra}")
    return normalized


def build_stability_wrappers(
    *,
    source_receipts: Mapping[str, Path],
    output_dir: Path,
    context: StabilityContext,
) -> dict[str, dict[str, Any]]:
    sources = _normalized_role_paths(source_receipts, label="source receipt")
    destination = output_dir.expanduser().absolute()
    output_paths = {role: destination / f"{role}.json" for role in REQUIRED_ROLES}
    if any(path.exists() or path.is_symlink() for path in output_paths.values()):
        raise StabilityReceiptError("one or more immutable wrapper paths already exist")
    validated: dict[str, tuple[dict[str, Any], dict[str, Any], tuple[int, int]]] = {}
    seen_sources: set[tuple[int, int]] = set()
    for role in REQUIRED_ROLES:
        result = validate_source_receipt(role, sources[role], context)
        source_identity = result[2]
        if source_identity in seen_sources:
            raise StabilityReceiptError("one source receipt was assigned to multiple roles")
        seen_sources.add(source_identity)
        validated[role] = result
    wrappers = {role: _wrapper_payload(role, validated[role][1]) for role in REQUIRED_ROLES}
    for role in REQUIRED_ROLES:
        _exclusive_private_json(output_paths[role], wrappers[role])
    return wrappers


def materialize_and_build_stability_wrappers(
    *,
    direct_source_receipts: Mapping[str, Path],
    single_day_stage_path: Path,
    zero_economic_stage_path: Path,
    durability_harness_path: Path,
    strict_source_dir: Path,
    output_dir: Path,
    context: StabilityContext,
) -> dict[str, dict[str, Any]]:
    direct = _normalized_role_paths(
        direct_source_receipts,
        label="direct source receipt",
        required_roles=DIRECT_SOURCE_ROLES,
    )
    strict_destination = strict_source_dir.expanduser().absolute()
    wrapper_destination = output_dir.expanduser().absolute()
    if strict_destination == wrapper_destination:
        raise StabilityReceiptError("strict source and wrapper directories must differ")
    wrapper_paths = {role: wrapper_destination / f"{role}.json" for role in REQUIRED_ROLES}
    if any(path.exists() or path.is_symlink() for path in wrapper_paths.values()):
        raise StabilityReceiptError("one or more immutable wrapper paths already exist")
    seen_direct: set[tuple[int, int]] = set()
    for role in DIRECT_SOURCE_ROLES:
        _payload, _binding, identity = validate_source_receipt(
            role,
            direct[role],
            context,
        )
        if identity in seen_direct:
            raise StabilityReceiptError("one direct source receipt was assigned multiple roles")
        seen_direct.add(identity)
    materialized = materialize_strict_source_receipts(
        single_day_stage_path=single_day_stage_path,
        zero_economic_stage_path=zero_economic_stage_path,
        durability_harness_path=durability_harness_path,
        regression_receipt_path=direct["regression"],
        output_dir=strict_destination,
        context=context,
    )
    sources = {**materialized, **direct}
    return build_stability_wrappers(
        source_receipts=sources,
        output_dir=wrapper_destination,
        context=context,
    )


def validate_stability_wrapper(
    path: Path,
    *,
    expected_role: str,
    context: StabilityContext,
) -> dict[str, Any]:
    if expected_role not in REQUIRED_ROLES:
        raise StabilityReceiptError(f"unknown stability role: {expected_role}")
    wrapper = _read_private_json(path, f"{expected_role} wrapper")
    payload = wrapper.payload
    if (
        set(payload) != _WRAPPER_FIELDS
        or payload.get("schema_version") != WRAPPER_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("role") != expected_role
        or payload.get("status") != WRAPPER_STATUS
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get("permissions") != PERMISSIONS
        or payload.get("canonical_receipt_sha256")
        != _document_sha256(payload, "canonical_receipt_sha256")
    ):
        raise StabilityReceiptError(f"{expected_role} wrapper identity drifted")
    source_binding = payload.get("source_receipt")
    if not isinstance(source_binding, Mapping) or set(source_binding) != _SOURCE_BINDING_FIELDS:
        raise StabilityReceiptError(f"{expected_role} source binding fields drifted")
    source_path = Path(str(source_binding.get("path", "")))
    _source_payload, observed_binding, source_identity = validate_source_receipt(
        expected_role,
        source_path,
        context,
    )
    if dict(source_binding) != observed_binding:
        raise StabilityReceiptError(f"{expected_role} source receipt bytes drifted")
    if source_identity == (wrapper.device, wrapper.inode):
        raise StabilityReceiptError(f"{expected_role} wrapper aliases its source receipt")
    return payload


def validate_stability_wrappers(
    *,
    wrappers: Mapping[str, Path],
    context: StabilityContext,
) -> dict[str, dict[str, Any]]:
    paths = _normalized_role_paths(wrappers, label="wrapper")
    validated: dict[str, dict[str, Any]] = {}
    seen_files: set[tuple[int, int]] = set()
    for role in REQUIRED_ROLES:
        record = _read_private_json(paths[role], f"{role} wrapper")
        identity = (record.device, record.inode)
        if identity in seen_files:
            raise StabilityReceiptError("one wrapper file was assigned to multiple roles")
        seen_files.add(identity)
        validated[role] = validate_stability_wrapper(
            paths[role],
            expected_role=role,
            context=context,
        )
    return validated


def _role_paths(
    values: Sequence[str],
    label: str,
    *,
    required_roles: Sequence[str] = REQUIRED_ROLES,
) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for raw in values:
        role, separator, path = raw.partition("=")
        if not separator or not role or not path or role in parsed:
            raise StabilityReceiptError(f"invalid {label} binding: {raw!r}")
        parsed[role] = Path(path)
    return _normalized_role_paths(
        parsed,
        label=label,
        required_roles=required_roles,
    )


def _context(args: argparse.Namespace) -> StabilityContext:
    execution_tag = str(args.execution_tag).strip()
    if not execution_tag:
        raise StabilityReceiptError("execution tag is empty")
    return StabilityContext(
        repository_root=Path(args.repository_root).expanduser().resolve(strict=True),
        execution_commit=_require_git_sha(args.execution_commit, "execution commit"),
        execution_tag=execution_tag,
        layer4_contract_path=Path(args.layer4_contract),
        layer4_day_receipt_dir=Path(args.layer4_day_receipt_dir),
    )


def _add_context_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--execution-commit", required=True)
    parser.add_argument("--execution-tag", required=True)
    parser.add_argument("--layer4-contract", type=Path, required=True)
    parser.add_argument("--layer4-day-receipt-dir", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    _add_context_arguments(build)
    build.add_argument("--source", action="append", required=True)
    build.add_argument("--output-dir", type=Path, required=True)
    materialize = subparsers.add_parser("materialize-and-build")
    _add_context_arguments(materialize)
    materialize.add_argument("--source", action="append", required=True)
    materialize.add_argument("--single-day-stage", type=Path, required=True)
    materialize.add_argument("--zero-economic-stage", type=Path, required=True)
    materialize.add_argument("--durability-harness", type=Path, required=True)
    materialize.add_argument("--strict-source-dir", type=Path, required=True)
    materialize.add_argument("--output-dir", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    _add_context_arguments(validate)
    validate.add_argument("--wrapper", action="append", required=True)
    return parser


def _safe_summary(
    paths: Mapping[str, Path],
    payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "identity": OWNER_IDENTITY,
        "status": "stability_wrappers_verified",
        "roles": {
            role: {
                "path": str(paths[role].expanduser().absolute()),
                "canonical_receipt_sha256": payloads[role]["canonical_receipt_sha256"],
            }
            for role in REQUIRED_ROLES
        },
        "economic_values_exposed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    context = _context(args)
    if args.command == "build":
        sources = _role_paths(args.source, "source")
        output_dir = Path(args.output_dir)
        payloads = build_stability_wrappers(
            source_receipts=sources,
            output_dir=output_dir,
            context=context,
        )
        paths = {role: output_dir / f"{role}.json" for role in REQUIRED_ROLES}
    elif args.command == "materialize-and-build":
        direct = _role_paths(
            args.source,
            "direct source",
            required_roles=DIRECT_SOURCE_ROLES,
        )
        output_dir = Path(args.output_dir)
        payloads = materialize_and_build_stability_wrappers(
            direct_source_receipts=direct,
            single_day_stage_path=Path(args.single_day_stage),
            zero_economic_stage_path=Path(args.zero_economic_stage),
            durability_harness_path=Path(args.durability_harness),
            strict_source_dir=Path(args.strict_source_dir),
            output_dir=output_dir,
            context=context,
        )
        paths = {role: output_dir / f"{role}.json" for role in REQUIRED_ROLES}
    else:
        paths = _role_paths(args.wrapper, "wrapper")
        payloads = validate_stability_wrappers(wrappers=paths, context=context)
    print(json.dumps(_safe_summary(paths, payloads), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
