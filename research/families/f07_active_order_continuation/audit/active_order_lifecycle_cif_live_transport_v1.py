#!/usr/bin/env python3
"""Outcome-blind prospective live/AWS transport audit for F07 lifecycle CIF."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from execution.order_lifecycle import FILL_RISK_PHASES
from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    validate_order_lifecycle_journal_v2_payload,
)

IDENTITY = "active_order_lifecycle_cif_live_transport_v1"
SPEC_SCHEMA_VERSION = "active_order_lifecycle_cif_live_transport_spec.v1"
REPORT_SCHEMA_VERSION = "active_order_lifecycle_cif_live_transport_report.v1"
ADMISSION_SCHEMA_VERSION = "prospective_lifecycle_journal_transport_admission.v1"
FORMAL_ADMISSION_SCHEMA_VERSION = "prospective_lifecycle_remote_session_admission.v1"
FEATURE_CONTEXT_SCHEMA_VERSION = "lifecycle_feature_visibility_context.v1"

CAUSES = ("full_fill", "cancel_ack", "other_terminal")
FEATURE_CONTEXT_COLUMNS = (
    "lifecycle_id",
    "feature_source_exchange_ts_ns",
    "feature_ready_ts_ns",
    "decision_ts_ns",
)
FORBIDDEN_OUTCOME_FRAGMENTS = (
    "pnl",
    "reward",
    "markout",
    "campaign_outcome",
    "terminal_value",
)
REQUIRED_FILE_ROLES = frozenset(
    {
        "runtime_identity",
        "live_health",
        "core_health",
        "epoch_manifest",
        "epoch_identity_evidence",
        "epoch_initial_runtime_state",
        "journal_part_manifest",
        "journal_part_data",
        "journal_cursor",
        "feature_visibility_context",
    }
)
REQUIRED_EXCHANGE_CLOCK_EVENTS = frozenset(
    {"activate", "cancel_rejected", "partial_fill", "full_fill", "exchange_terminal"}
)
_NS_PER_S = 1_000_000_000


class LiveTransportAuditError(ValueError):
    """Raised when a frozen contract or prospective input fails closed."""


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    unresolved = path.expanduser()
    if unresolved.is_symlink():
        raise LiveTransportAuditError(f"{label} must not be a symlink: {unresolved}")
    try:
        resolved = unresolved.resolve(strict=True)
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LiveTransportAuditError(f"invalid {label}: {unresolved}") from exc
    if not isinstance(payload, dict):
        raise LiveTransportAuditError(f"{label} must be a JSON object")
    return payload


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _require_sha256(label: str, value: object) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise LiveTransportAuditError(f"{label} must be a lowercase SHA256")
    return normalized


def _validate_sealed_hash(payload: Mapping[str, Any], field: str, *, label: str) -> None:
    expected = _require_sha256(f"{label} {field}", payload.get(field, ""))
    unsigned = dict(payload)
    unsigned.pop(field, None)
    if canonical_sha256(unsigned) != expected:
        raise LiveTransportAuditError(f"{label} canonical hash mismatch")


def _contains_forbidden_key(value: object, *, prefix: str = "") -> str | None:
    if isinstance(value, Mapping):
        for raw_key, nested in value.items():
            key = str(raw_key)
            lowered = key.lower()
            if any(fragment in lowered for fragment in FORBIDDEN_OUTCOME_FRAGMENTS):
                return f"{prefix}.{key}" if prefix else key
            found = _contains_forbidden_key(
                nested,
                prefix=f"{prefix}.{key}" if prefix else key,
            )
            if found:
                return found
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, nested in enumerate(value):
            found = _contains_forbidden_key(nested, prefix=f"{prefix}[{index}]")
            if found:
                return found
    return None


def validate_spec(spec_path: Path) -> dict[str, Any]:
    spec = _read_json(spec_path, label="frozen transport spec")
    expected_keys = {
        "schema_version",
        "identity",
        "status",
        "frozen_at_utc",
        "last_materially_modified",
        "canonical_spec_sha256",
        "purpose",
        "scope",
        "input_contract",
        "reference_artifacts",
        "clock_contract",
        "support_contract",
        "transport_gates",
        "output_contract",
        "implementation_identity",
        "permissions",
    }
    if set(spec) != expected_keys:
        raise LiveTransportAuditError("frozen transport spec schema mismatch")
    if spec["schema_version"] != SPEC_SCHEMA_VERSION or spec["identity"] != IDENTITY:
        raise LiveTransportAuditError("frozen transport spec identity mismatch")
    if spec["status"] != "frozen_before_prospective_tape_read":
        raise LiveTransportAuditError("transport spec was not frozen before tape read")
    _validate_sealed_hash(spec, "canonical_spec_sha256", label="transport spec")
    if _contains_forbidden_key(spec) is not None:
        raise LiveTransportAuditError("transport spec contains an economic outcome field")

    implementation = spec["implementation_identity"]
    script_hash = _require_sha256(
        "audit implementation SHA256", implementation.get("audit_sha256", "")
    )
    if file_sha256(Path(__file__).resolve()) != script_hash:
        raise LiveTransportAuditError("audit implementation differs from frozen spec")
    gates = spec["transport_gates"]
    if float(gates.get("valid_fraction_abs_delta_lte", -1.0)) != 0.05:
        raise LiveTransportAuditError("valid-fraction threshold drifted")
    if float(gates.get("composition_total_variation_lte", -1.0)) != 0.15:
        raise LiveTransportAuditError("composition-TV threshold drifted")
    permissions = spec["permissions"]
    if permissions != {
        "action_authorized": False,
        "economic_evaluation_authorized": False,
        "live_policy_authorized": False,
    }:
        raise LiveTransportAuditError("frozen permission boundary drifted")
    return spec


def _validate_reference_file(
    path: Path,
    expected: Mapping[str, Any],
    *,
    label: str,
    canonical_field: str,
) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    if file_sha256(resolved) != _require_sha256(f"{label} SHA256", expected["sha256"]):
        raise LiveTransportAuditError(f"{label} file SHA256 differs from frozen spec")
    payload = _read_json(resolved, label=label)
    _validate_sealed_hash(payload, canonical_field, label=label)
    if payload.get("schema_version") != expected["schema_version"]:
        raise LiveTransportAuditError(f"{label} schema version mismatch")
    if payload.get("identity") != expected["identity"]:
        raise LiveTransportAuditError(f"{label} identity mismatch")
    return payload


def validate_reference_chain(
    *,
    spec: Mapping[str, Any],
    cif_artifact_path: Path,
    training_report_path: Path,
    lockstep_report_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    refs = spec["reference_artifacts"]
    lockstep = _validate_reference_file(
        lockstep_report_path,
        refs["lockstep_report"],
        label="40-day lockstep report",
        canonical_field="canonical_report_sha256",
    )
    artifact = _validate_reference_file(
        cif_artifact_path,
        refs["cif_artifact"],
        label="40-day CIF artifact",
        canonical_field="canonical_artifact_sha256",
    )
    training = _validate_reference_file(
        training_report_path,
        refs["training_report"],
        label="40-day CIF training report",
        canonical_field="canonical_report_sha256",
    )
    if lockstep.get("status") != "passed" or not bool(
        lockstep.get("formal_40day_lockstep_passed")
    ):
        raise LiveTransportAuditError("40-day lockstep reference did not pass")
    if training.get("status") != "passed" or not all(training.get("gates", {}).values()):
        raise LiveTransportAuditError("40-day training reference did not pass")
    artifact_reference = training.get("model_artifact", {})
    if artifact_reference.get("sha256") != refs["cif_artifact"]["sha256"]:
        raise LiveTransportAuditError("training report binds a different CIF artifact")
    lockstep_reference = training.get("input_artifacts", {}).get("python_cpp_lockstep", {})
    if lockstep_reference.get("sha256") != refs["lockstep_report"]["sha256"]:
        raise LiveTransportAuditError("training report binds a different lockstep report")
    if artifact.get("input_artifacts", {}).get("python_cpp_lockstep", {}).get(
        "sha256"
    ) != refs["lockstep_report"]["sha256"]:
        raise LiveTransportAuditError("CIF artifact binds a different lockstep report")
    if artifact.get("permissions", {}).get("live_transport") is not False:
        raise LiveTransportAuditError("reference CIF artifact permission boundary drifted")
    for label, payload in (
        ("40-day lockstep report", lockstep),
        ("40-day CIF artifact", artifact),
        ("40-day CIF training report", training),
    ):
        scope = payload.get("scope", {})
        if not isinstance(scope, Mapping) or scope.get("economic_outcomes_read") is not False:
            raise LiveTransportAuditError(f"{label} is not outcome-blind")
        for field in ("markout_read", "pnl_read", "reward_read", "campaign_outcome_read"):
            if field in scope and scope[field] is not False:
                raise LiveTransportAuditError(f"{label} permits {field}")
    return artifact, training, lockstep


def _resolve_listed_file(root: Path, relative_path: object) -> Path:
    raw = str(relative_path)
    pure = PurePosixPath(raw)
    if not raw or pure.is_absolute() or ".." in pure.parts or pure.as_posix() != raw:
        raise LiveTransportAuditError(f"unsafe admission relative path: {raw!r}")
    path = root.joinpath(*pure.parts)
    if path.is_symlink():
        raise LiveTransportAuditError(f"admission file must not be a symlink: {raw}")
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise LiveTransportAuditError(f"admission file escaped root: {raw}") from exc
    if not resolved.is_file():
        raise LiveTransportAuditError(f"admission file is not regular: {raw}")
    return resolved


def _validate_admission_files(
    root: Path, manifest: Mapping[str, Any]
) -> tuple[dict[str, list[Path]], dict[str, Mapping[str, Any]]]:
    records = manifest.get("files")
    if not isinstance(records, list) or not records:
        raise LiveTransportAuditError("admission file inventory is missing")
    by_role: defaultdict[str, list[Path]] = defaultdict(list)
    metadata: dict[str, Mapping[str, Any]] = {}
    listed: set[str] = set()
    for raw_record in records:
        if not isinstance(raw_record, Mapping) or set(raw_record) != {
            "role",
            "relative_path",
            "size_bytes",
            "sha256",
        }:
            raise LiveTransportAuditError("admission file record schema mismatch")
        role = str(raw_record["role"])
        if role not in REQUIRED_FILE_ROLES:
            raise LiveTransportAuditError(f"unsupported admission file role: {role}")
        relative = str(raw_record["relative_path"])
        if relative in listed:
            raise LiveTransportAuditError("admission file inventory contains duplicates")
        listed.add(relative)
        path = _resolve_listed_file(root, relative)
        if path.stat().st_size != int(raw_record["size_bytes"]):
            raise LiveTransportAuditError(f"admission size mismatch: {relative}")
        if file_sha256(path) != _require_sha256(
            f"admission file SHA256 {relative}", raw_record["sha256"]
        ):
            raise LiveTransportAuditError(f"admission SHA256 mismatch: {relative}")
        by_role[role].append(path)
        metadata[relative] = raw_record
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name != "admission_manifest.json"
    }
    if actual != listed:
        raise LiveTransportAuditError(
            "admission file inventory is not exact: "
            f"missing={sorted(actual - listed)} extra={sorted(listed - actual)}"
        )
    required_singletons = {
        "runtime_identity",
        "live_health",
        "core_health",
        "epoch_manifest",
        "epoch_identity_evidence",
        "epoch_initial_runtime_state",
        "feature_visibility_context",
    }
    for role in required_singletons:
        if len(by_role[role]) != 1:
            raise LiveTransportAuditError(f"admission requires exactly one {role} file")
    if not by_role["journal_part_manifest"] or len(by_role["journal_part_manifest"]) != len(
        by_role["journal_part_data"]
    ):
        raise LiveTransportAuditError("journal part manifest/data inventory is incomplete")
    return dict(by_role), metadata


def validate_atomic_admission(admission_dir: Path) -> dict[str, Any]:
    unresolved = admission_dir.expanduser()
    if unresolved.is_symlink():
        raise LiveTransportAuditError("admission directory must not be a symlink")
    root = unresolved.resolve(strict=True)
    if not root.is_dir():
        raise LiveTransportAuditError("admission path must be a directory")
    if any(".partial-" in path.name for path in root.rglob("*")):
        raise LiveTransportAuditError("admission contains a partial file")
    manifest_path = root / "admission_manifest.json"
    manifest = _read_json(manifest_path, label="atomic admission manifest")
    if manifest.get("schema_version") == FORMAL_ADMISSION_SCHEMA_VERSION:
        return _validate_formal_remote_session_admission(root, manifest)
    if manifest.get("schema_version") != ADMISSION_SCHEMA_VERSION:
        raise LiveTransportAuditError("atomic admission schema mismatch")
    if manifest.get("identity") != IDENTITY:
        raise LiveTransportAuditError("atomic admission identity mismatch")
    if manifest.get("feature_context_schema_version") != FEATURE_CONTEXT_SCHEMA_VERSION:
        raise LiveTransportAuditError("feature visibility context version mismatch")
    _validate_sealed_hash(manifest, "manifest_sha256", label="atomic admission")
    if not bool(manifest.get("atomic_admission")) or not bool(
        manifest.get("admission_complete")
    ):
        raise LiveTransportAuditError("admission is not complete and atomic")
    if bool(manifest.get("economic_outcomes_read")):
        raise LiveTransportAuditError("admission claims economic outcomes")
    forbidden = _contains_forbidden_key(manifest)
    if forbidden:
        raise LiveTransportAuditError(f"admission contains forbidden field: {forbidden}")
    by_role, metadata = _validate_admission_files(root, manifest)
    return {
        "root": root,
        "manifest_path": manifest_path,
        "manifest": manifest,
        "by_role": by_role,
        "metadata": metadata,
    }


def _validate_formal_remote_session_admission(
    root: Path, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    """Adapt the deeply validated remote-session admission to transport roles."""

    unsigned = dict(manifest)
    admission_identity = _require_sha256(
        "formal admission identity", unsigned.pop("admission_identity_sha256", "")
    )
    if canonical_sha256(unsigned) != admission_identity:
        raise LiveTransportAuditError("formal admission canonical identity mismatch")
    for field in (
        "single_rsync_files_from_session",
        "atomic_rename_admission",
    ):
        if manifest.get(field) is not True:
            raise LiveTransportAuditError(f"formal admission lacks {field}")
    for field in (
        "remote_payload_deleted",
        "economic_outcomes_read",
        "action_authorized",
        "live_policy_authorized",
    ):
        if bool(manifest.get(field)):
            raise LiveTransportAuditError(f"formal admission exceeds authority at {field}")
    validation = manifest.get("validation")
    if not isinstance(validation, Mapping):
        raise LiveTransportAuditError("formal admission validation summary is missing")
    required_validation = {
        "epoch_fully_bound": True,
        "stable_double_read_passed": True,
        "health_drop_count": 0,
        "health_error_count": 0,
        "storage_format": "parquet",
    }
    if any(validation.get(key) != value for key, value in required_validation.items()):
        raise LiveTransportAuditError("formal admission validation summary failed")

    source = (root / "source").resolve(strict=True)
    allowlisted_root = str(manifest.get("remote_allowlisted_root", "")).rstrip("/")
    remote_seal = str(manifest.get("remote_seal_path", ""))
    prefix = allowlisted_root + "/"
    if not remote_seal.startswith(prefix):
        raise LiveTransportAuditError("formal admission seal escaped allowlisted root")
    seal_path = _resolve_listed_file(source, remote_seal[len(prefix) :])
    if file_sha256(seal_path) != _require_sha256(
        "formal admission seal SHA256", manifest.get("remote_seal_sha256", "")
    ):
        raise LiveTransportAuditError("formal admission seal SHA256 mismatch")
    seal = _read_json(seal_path, label="formal remote session seal")
    seal_unsigned = dict(seal)
    seal_identity = _require_sha256(
        "formal seal identity", seal_unsigned.pop("seal_identity_sha256", "")
    )
    if canonical_sha256(seal_unsigned) != seal_identity or seal_identity != manifest.get(
        "remote_seal_identity_sha256"
    ):
        raise LiveTransportAuditError("formal admission seal identity mismatch")
    if seal.get("collection_bound_reached") is not True or seal.get(
        "stable_double_read_passed"
    ) is not True:
        raise LiveTransportAuditError("formal admission seal is not stable and bounded")

    sealed_records = seal.get("files")
    if not isinstance(sealed_records, list) or not sealed_records:
        raise LiveTransportAuditError("formal seal file inventory is missing")
    listed: set[str] = set()
    by_role: defaultdict[str, list[Path]] = defaultdict(list)
    metadata: dict[str, Mapping[str, Any]] = {}
    session_prefix = str(seal["session_root_relative"]).rstrip("/") + "/"
    epoch_prefix = str(seal["epoch_root_relative"]).rstrip("/") + "/"
    health_relative = str(seal["health_snapshot_relative_path"])
    for record in sealed_records:
        if not isinstance(record, Mapping) or set(record) != {"path", "bytes", "sha256"}:
            raise LiveTransportAuditError("formal seal file record schema mismatch")
        relative = str(record["path"])
        if relative in listed:
            raise LiveTransportAuditError("formal seal file inventory contains duplicates")
        listed.add(relative)
        path = _resolve_listed_file(source, relative)
        if path.stat().st_size != int(record["bytes"]):
            raise LiveTransportAuditError(f"formal sealed file size mismatch: {relative}")
        if file_sha256(path) != _require_sha256(
            f"formal sealed file SHA256 {relative}", record["sha256"]
        ):
            raise LiveTransportAuditError(f"formal sealed file SHA256 mismatch: {relative}")
        role: str | None = None
        if relative == health_relative:
            role = "health_snapshot"
        elif relative == f"{session_prefix}runtime_identity.json":
            role = "runtime_identity"
        elif relative == f"{epoch_prefix}epoch_manifest.json":
            role = "epoch_manifest"
        elif relative == f"{epoch_prefix}identity_evidence.json":
            role = "epoch_identity_evidence"
        elif relative == f"{epoch_prefix}initial_runtime_state.json":
            role = "epoch_initial_runtime_state"
        elif relative.startswith(f"{session_prefix}parts/") and relative.endswith(
            ".manifest.json"
        ):
            role = "journal_part_manifest"
        elif relative.startswith(f"{session_prefix}parts/") and relative.endswith(".parquet"):
            role = "journal_part_data"
        elif relative.startswith(f"{session_prefix}cursors/") and relative.endswith(".json"):
            role = "journal_cursor"
        if role is not None:
            by_role[role].append(path)
            metadata[relative] = record

    actual = {
        path.relative_to(source).as_posix()
        for path in source.rglob("*")
        if path.is_file() and path != seal_path
    }
    if actual != listed:
        raise LiveTransportAuditError("formal admission source inventory differs from seal")
    for role in (
        "health_snapshot",
        "runtime_identity",
        "epoch_manifest",
        "epoch_identity_evidence",
        "epoch_initial_runtime_state",
    ):
        if len(by_role[role]) != 1:
            raise LiveTransportAuditError(f"formal admission requires exactly one {role}")
    if not by_role["journal_part_manifest"] or len(by_role["journal_part_manifest"]) != len(
        by_role["journal_part_data"]
    ):
        raise LiveTransportAuditError("formal admission journal inventory is incomplete")
    return {
        "root": root,
        "manifest_path": root / "admission_manifest.json",
        "manifest": dict(manifest),
        "by_role": dict(by_role),
        "metadata": metadata,
        "formal_remote_session_admission": True,
        "seal": seal,
    }


def _runtime_and_epoch(admission: Mapping[str, Any]) -> dict[str, Any]:
    by_role = admission["by_role"]
    writer = _read_json(by_role["runtime_identity"][0], label="writer runtime identity")
    runtime = writer.get("runtime_identity")
    if not isinstance(runtime, Mapping):
        raise LiveTransportAuditError("writer runtime identity payload is missing")
    runtime_hash = canonical_sha256(runtime)
    if runtime_hash != _require_sha256(
        "writer runtime identity SHA256", writer.get("runtime_identity_sha256", "")
    ):
        raise LiveTransportAuditError("writer runtime identity canonical hash mismatch")
    epoch = _read_json(by_role["epoch_manifest"][0], label="prospective epoch manifest")
    if epoch.get("binding_status") != "fully_bound":
        raise LiveTransportAuditError("prospective epoch is not fully_bound")
    if epoch.get("epoch_id") != runtime.get("baseline_epoch_id"):
        raise LiveTransportAuditError("epoch and writer lifecycle identities differ")
    identity = epoch.get("identity")
    if not isinstance(identity, Mapping):
        raise LiveTransportAuditError("prospective epoch identity is missing")
    if canonical_sha256(identity) != _require_sha256(
        "epoch identity SHA256", epoch.get("identity_sha256", "")
    ):
        raise LiveTransportAuditError("prospective epoch identity hash mismatch")
    if runtime.get("baseline_epoch_identity_sha256") != epoch.get("identity_sha256"):
        raise LiveTransportAuditError("writer does not bind the prospective epoch identity")

    evidence = _read_json(
        by_role["epoch_identity_evidence"][0], label="epoch identity evidence"
    )
    evidence_ref = epoch.get("identity_evidence")
    if not isinstance(evidence_ref, Mapping) or canonical_sha256(evidence) != _require_sha256(
        "epoch identity evidence canonical SHA256",
        evidence_ref.get("canonical_sha256", ""),
    ):
        raise LiveTransportAuditError("epoch identity evidence hash mismatch")
    clocks = evidence.get("clock_semantics")
    if not isinstance(clocks, Mapping):
        raise LiveTransportAuditError("epoch clock semantics are missing")
    required_clocks = {
        "exchange_clock": "exchange_event_time_ns",
        "visibility_clock": "local_callback_receive_time_ns",
        "feature_visibility_rule": "feature_ready_ts_ns<=decision_ts_ns",
        "missing_exchange_clock_policy": (
            "null_physical_exposure_and_invalidate_tape_row"
        ),
    }
    if any(clocks.get(key) != value for key, value in required_clocks.items()):
        raise LiveTransportAuditError("epoch clock semantics differ from AWS causal contract")
    if canonical_sha256(clocks) != identity.get("clock_semantics_sha256"):
        raise LiveTransportAuditError("epoch clock semantics identity mismatch")
    initial_state = _read_json(
        by_role["epoch_initial_runtime_state"][0], label="epoch initial runtime state"
    )
    initial_ref = epoch.get("initial_runtime_state")
    if not isinstance(initial_ref, Mapping) or canonical_sha256(initial_state) != _require_sha256(
        "epoch initial runtime state canonical SHA256",
        initial_ref.get("canonical_sha256", ""),
    ):
        raise LiveTransportAuditError("epoch initial runtime state hash mismatch")
    return {
        "runtime_file": writer,
        "runtime_identity": dict(runtime),
        "runtime_identity_sha256": runtime_hash,
        "epoch_manifest": epoch,
        "clock_semantics": dict(clocks),
    }


def _validate_health(
    admission: Mapping[str, Any],
    *,
    runtime: Mapping[str, Any],
    expected_rows: int,
) -> dict[str, Any]:
    by_role = admission["by_role"]
    if by_role.get("health_snapshot"):
        snapshot = _read_json(by_role["health_snapshot"][0], label="sealed writer health")
        live = snapshot.get("live_health")
        core = snapshot.get("core_health")
        if not isinstance(live, Mapping) or not isinstance(core, Mapping):
            raise LiveTransportAuditError("sealed writer health payload is incomplete")
    else:
        live = _read_json(by_role["live_health"][0], label="live writer health")
        core = _read_json(by_role["core_health"][0], label="core writer health")
    epoch_id = runtime["epoch_manifest"]["epoch_id"]
    runtime_hash = runtime["runtime_identity_sha256"]
    bounded_complete = (
        live.get("state") == "bounded_complete"
        and live.get("collection_bound_reached") is True
    )
    checks = {
        "session_identity": live.get("session_id") == epoch_id
        and live.get("baseline_epoch_id") == epoch_id,
        "bounded_or_closed": bounded_complete
        or (live.get("state") == "closed" and bool(core.get("closed"))),
        "remote_or_local_writer_valid": bool(live.get("remote_spool_valid"))
        or bool(live.get("formal_collection_valid")),
        "zero_drop": int(live.get("drop_count", -1)) == 0
        and int(core.get("rows_dropped", -1)) == 0,
        "zero_error": int(live.get("error_count", -1)) == 0
        and int(core.get("error_count", -1)) == 0,
        "queue_drained": int(live.get("queue_depth", -1)) == 0,
        "callback_accounting": int(live.get("callbacks_enqueued", -1))
        == int(live.get("callbacks_processed", -2)),
        "row_accounting": int(live.get("rows_committed", -1)) == expected_rows
        and int(core.get("rows_committed", -1)) == expected_rows,
        "core_formal_valid": bool(core.get("formal_collection_valid")),
        "runtime_identity": core.get("runtime_identity_sha256") == runtime_hash,
        "no_orphan_payload": int(core.get("orphan_payload_count", -1)) == 0,
    }
    if not all(checks.values()):
        failed = sorted(key for key, passed in checks.items() if not passed)
        raise LiveTransportAuditError("writer health failed: " + ",".join(failed))
    return {"live": live, "core": core, "checks": checks}


def _read_journal_parts(admission: Mapping[str, Any]) -> list[dict[str, Any]]:
    by_role = admission["by_role"]
    manifests: dict[str, dict[str, Any]] = {}
    data_by_name = {path.name: path for path in by_role["journal_part_data"]}
    rows: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    for manifest_path in sorted(by_role["journal_part_manifest"]):
        manifest = _read_json(manifest_path, label="journal part manifest")
        if manifest.get("economic_outcomes_read") is not False:
            raise LiveTransportAuditError("journal part economic boundary differs")
        data_name = str(manifest.get("data_file", ""))
        data_path = data_by_name.get(data_name)
        if data_path is None:
            raise LiveTransportAuditError("journal part data file is missing")
        if file_sha256(data_path) != manifest.get("data_sha256"):
            raise LiveTransportAuditError("journal part data SHA256 mismatch")
        if manifest.get("storage_format") != "parquet":
            raise LiveTransportAuditError("live transport requires exact Parquet parts")
        if manifest.get("journal_schema_version") != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
            raise LiveTransportAuditError("journal part schema version mismatch")
        runtime_identity_path = by_role["runtime_identity"][0]
        runtime_identity = _read_json(
            runtime_identity_path, label="writer runtime identity for journal part"
        )
        if manifest.get("runtime_identity_sha256") != runtime_identity.get(
            "runtime_identity_sha256"
        ):
            raise LiveTransportAuditError("journal part runtime identity mismatch")
        import pyarrow.parquet as pq

        table = pq.read_table(data_path)
        if tuple(table.column_names) != ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS:
            raise LiveTransportAuditError("journal Parquet schema is not exact v2")
        part_rows = table.to_pylist()
        if len(part_rows) != int(manifest.get("row_count", -1)) or not part_rows:
            raise LiveTransportAuditError("journal part row count mismatch")
        for row in part_rows:
            normalized = {column: row[column] for column in ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS}
            validate_order_lifecycle_journal_v2_payload(normalized)
            event_id = str(normalized["event_id"])
            if event_id in event_ids:
                raise LiveTransportAuditError("journal contains duplicate event IDs")
            event_ids.add(event_id)
            rows.append(normalized)
        if [row["event_id"] for row in part_rows] != list(manifest.get("event_ids", [])):
            raise LiveTransportAuditError("journal part event identities differ")
        manifests[str(manifest.get("batch_id", ""))] = manifest
    expected_names = {str(manifest["data_file"]) for manifest in manifests.values()}
    if expected_names != set(data_by_name):
        raise LiveTransportAuditError("journal data inventory has unbound files")
    return rows


def _read_feature_context(path: Path) -> dict[str, dict[str, Any]]:
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    if tuple(table.column_names) != FEATURE_CONTEXT_COLUMNS:
        raise LiveTransportAuditError("feature visibility context schema is not exact")
    if _contains_forbidden_key({column: None for column in table.column_names}):
        raise LiveTransportAuditError("feature visibility context contains outcomes")
    contexts: dict[str, dict[str, Any]] = {}
    for row in table.to_pylist():
        lifecycle_id = str(row["lifecycle_id"]).strip()
        if not lifecycle_id or lifecycle_id.lower() in {"nan", "none", "null"}:
            raise LiveTransportAuditError("feature context lifecycle ID is empty")
        if lifecycle_id in contexts:
            raise LiveTransportAuditError("feature context lifecycle ID is duplicated")
        source = int(row["feature_source_exchange_ts_ns"])
        ready = int(row["feature_ready_ts_ns"])
        decision = int(row["decision_ts_ns"])
        if source <= 0 or not source <= ready <= decision:
            raise LiveTransportAuditError("feature-ready causal ordering failed")
        contexts[lifecycle_id] = {
            "lifecycle_id": lifecycle_id,
            "feature_source_exchange_ts_ns": source,
            "feature_ready_ts_ns": ready,
            "decision_ts_ns": decision,
        }
    return contexts


def _terminal_cause(row: Mapping[str, Any]) -> str | None:
    event = str(row["lifecycle_event"])
    if event == "full_fill":
        return "full_fill"
    if str(row["terminal_observation"]) != "EXCHANGE_TERMINAL":
        return None
    reason = str(row["exchange_terminal_reason"] or row["event_reason"])
    if reason in {"cancel_ack", "cancel_ack_reconciled"}:
        return "cancel_ack"
    if reason in {"filled_before_cancel_ack", "full_fill"}:
        return "full_fill"
    return "other_terminal"


def _quantiles(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p90": None, "p99": None, "max": None}
    ordered = sorted(float(value) for value in values)

    def select(q: float) -> float:
        index = int(math.ceil(q * len(ordered))) - 1
        return ordered[max(0, min(index, len(ordered) - 1))]

    return {"p50": select(0.50), "p90": select(0.90), "p99": select(0.99), "max": ordered[-1]}


def _distribution_tv(left: Mapping[str, int], right: Mapping[str, int]) -> float | None:
    keys = set(left) | set(right)
    left_total = sum(max(0, int(left.get(key, 0))) for key in keys)
    right_total = sum(max(0, int(right.get(key, 0))) for key in keys)
    if left_total <= 0 or right_total <= 0:
        return None
    return 0.5 * sum(
        abs(int(left.get(key, 0)) / left_total - int(right.get(key, 0)) / right_total)
        for key in keys
    )


def _cell_key(
    *,
    artifact: Mapping[str, Any],
    side: str,
    phase: str,
    age_s: float,
    initial: float,
    remaining: float,
    ts_ns: int,
) -> tuple[str, str, int, str, int]:
    conditioning = artifact["conditioning"]
    edges = [
        math.inf if value == "inf" else float(value)
        for value in conditioning["risk_age_bin_edges_s"]
    ]
    if not math.isfinite(age_s) or age_s < 0.0:
        raise LiveTransportAuditError("live risk age is invalid")
    age_bin = next((index for index, upper in enumerate(edges[1:]) if age_s < upper), len(edges) - 2)
    if initial <= 0.0 or remaining <= 0.0 or remaining > initial + 1e-12:
        raise LiveTransportAuditError("live remaining quantity leaves support")
    remaining_class = "full" if remaining >= initial - 1e-12 else "partial"
    hour = datetime.fromtimestamp(ts_ns / _NS_PER_S, tz=timezone.utc).hour
    hour_bin = hour // int(conditioning["utc_hour_bin_width"])
    return str(side), str(phase), age_bin, remaining_class, hour_bin


def _next_age_boundary_ns(
    artifact: Mapping[str, Any], activation_ts_ns: int, age_s: float
) -> int | None:
    edges = [
        math.inf if value == "inf" else float(value)
        for value in artifact["conditioning"]["risk_age_bin_edges_s"]
    ]
    index = next((i for i, upper in enumerate(edges[1:]) if age_s < upper), len(edges) - 2)
    upper = edges[index + 1]
    return None if not math.isfinite(upper) else activation_ts_ns + int(round(upper * _NS_PER_S))


def _next_hour_boundary_ns(artifact: Mapping[str, Any], ts_ns: int) -> int:
    width = int(artifact["conditioning"]["utc_hour_bin_width"])
    seconds = ts_ns // _NS_PER_S
    current_hour = seconds // 3600
    return ((current_hour // width) + 1) * width * 3600 * _NS_PER_S


def _audit_lifecycles(
    *,
    rows: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]] | None,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    by_lifecycle: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_lifecycle[str(row["lifecycle_id"])].append(row)
    if contexts is not None and set(by_lifecycle) != set(contexts):
        raise LiveTransportAuditError("feature context coverage is not exactly one per lifecycle")

    exposures: defaultdict[tuple[str, str, int, str, int], float] = defaultdict(float)
    events: defaultdict[tuple[str, str, int, str, int], Counter[str]] = defaultdict(Counter)
    invalid_reasons: Counter[str] = Counter()
    cause_composition: Counter[str] = Counter()
    cancel_composition: Counter[str] = Counter()
    exchange_lags_ms: list[float] = []
    feature_lags_ms: list[float] = []
    decision_ages_ms: list[float] = []
    required_exchange_rows = 0
    required_exchange_valid_rows = 0
    post_terminal_reuse = 0
    unknown_terminal = 0
    eligible = 0
    censored = 0

    for lifecycle_id, raw_rows in by_lifecycle.items():
        ordered = sorted(raw_rows, key=lambda row: int(row["lifecycle_sequence"]))
        sequences = [int(row["lifecycle_sequence"]) for row in ordered]
        if sequences != list(range(1, len(sequences) + 1)):
            raise LiveTransportAuditError("lifecycle sequence is not contiguous from one")
        if contexts is not None:
            context = contexts[lifecycle_id]
            first_visibility = int(ordered[0]["event_visibility_ts_ns"])
            if int(context["decision_ts_ns"]) > first_visibility:
                raise LiveTransportAuditError("feature decision occurs after lifecycle submit")
            feature_lags_ms.append(
                (
                    int(context["feature_ready_ts_ns"])
                    - int(context["feature_source_exchange_ts_ns"])
                )
                / 1_000_000.0
            )
            decision_ages_ms.append(
                (int(context["decision_ts_ns"]) - int(context["feature_ready_ts_ns"]))
                / 1_000_000.0
            )
        terminal_index: int | None = None
        for index, row in enumerate(ordered):
            event = str(row["lifecycle_event"])
            if event in REQUIRED_EXCHANGE_CLOCK_EVENTS:
                required_exchange_rows += 1
                if bool(row["event_exchange_clock_valid"]) and row["event_exchange_ts_ns"] is not None:
                    required_exchange_valid_rows += 1
            if row["event_exchange_ts_ns"] is not None:
                exchange_lags_ms.append(
                    (int(row["event_visibility_ts_ns"]) - int(row["event_exchange_ts_ns"]))
                    / 1_000_000.0
                )
            if str(row["terminal_observation"]) in {
                "EXCHANGE_TERMINAL",
                "LOCAL_SHUTDOWN_CENSOR",
            }:
                if terminal_index is not None:
                    unknown_terminal += 1
                terminal_index = index
        if terminal_index is not None and terminal_index != len(ordered) - 1:
            for row in ordered[terminal_index + 1 :]:
                if (
                    str(row["phase_before"]) in FILL_RISK_PHASES
                    or str(row["phase_after"]) in FILL_RISK_PHASES
                    or bool(row["fill_risk_active_after"])
                ):
                    post_terminal_reuse += 1
        if any(bool(row["left_truncated"]) for row in ordered):
            invalid_reasons["left_truncated"] += 1
            censored += 1
            continue
        activation_indices = [
            index for index, row in enumerate(ordered) if row["lifecycle_event"] == "activate"
        ]
        if len(activation_indices) != 1:
            invalid_reasons["activation_count"] += 1
            censored += 1
            continue
        if terminal_index is None:
            invalid_reasons["missing_terminal_or_censor"] += 1
            censored += 1
            continue
        risk_rows = [
            row
            for row in ordered
            if str(row["phase_before"]) in FILL_RISK_PHASES
            or str(row["phase_after"]) in FILL_RISK_PHASES
        ]
        if not risk_rows or any(
            not bool(row["visible_exposure_valid"])
            or not bool(row["exchange_exposure_valid"])
            for row in risk_rows
        ):
            invalid_reasons["risk_clock_or_exposure_invalid"] += 1
            censored += 1
            continue
        if any(
            row["lifecycle_event"] in REQUIRED_EXCHANGE_CLOCK_EVENTS
            and (
                not bool(row["event_exchange_clock_valid"])
                or row["event_exchange_ts_ns"] is None
            )
            for row in ordered
        ):
            invalid_reasons["required_exchange_clock_missing"] += 1
            censored += 1
            continue
        if str(ordered[terminal_index]["terminal_observation"]) == "LOCAL_SHUTDOWN_CENSOR":
            invalid_reasons["local_shutdown_censor"] += 1
            censored += 1
            continue

        activation_index = activation_indices[0]
        activation = ordered[activation_index]
        activation_ts = int(activation["event_visibility_ts_ns"])
        current_ts = activation_ts
        current_phase = str(activation["phase_after"])
        initial = float(activation["initial_quantity"])
        remaining = float(activation["remaining_quantity_after"])
        last_cell: tuple[str, str, int, str, int] | None = None
        terminal_seen = False
        for row in ordered[activation_index + 1 :]:
            event_ts = int(row["event_visibility_ts_ns"])
            if event_ts < current_ts:
                raise LiveTransportAuditError("visibility clock regressed within lifecycle")
            cursor = current_ts
            while cursor < event_ts and current_phase in FILL_RISK_PHASES and remaining > 0.0:
                age_s = max(0.0, (cursor - activation_ts) / _NS_PER_S)
                segment_end = min(event_ts, _next_hour_boundary_ns(artifact, cursor))
                age_boundary = _next_age_boundary_ns(artifact, activation_ts, age_s)
                if age_boundary is not None and age_boundary > cursor:
                    segment_end = min(segment_end, age_boundary)
                if segment_end <= cursor:
                    raise LiveTransportAuditError("risk exposure segmentation did not advance")
                midpoint = cursor + (segment_end - cursor) // 2
                last_cell = _cell_key(
                    artifact=artifact,
                    side=str(row["side"]),
                    phase=current_phase,
                    age_s=max(0.0, (midpoint - activation_ts) / _NS_PER_S),
                    initial=initial,
                    remaining=remaining,
                    ts_ns=midpoint,
                )
                exposures[last_cell] += (segment_end - cursor) / _NS_PER_S
                cursor = segment_end
            cause = _terminal_cause(row)
            if cause is not None:
                if last_cell is None:
                    probe_ts = max(activation_ts, event_ts - 1)
                    last_cell = _cell_key(
                        artifact=artifact,
                        side=str(row["side"]),
                        phase=str(row["phase_before"]),
                        age_s=max(0.0, (probe_ts - activation_ts) / _NS_PER_S),
                        initial=initial,
                        remaining=float(row["remaining_quantity_before"]),
                        ts_ns=probe_ts,
                    )
                events[last_cell][cause] += 1
                composition_key = f"{row['side']}|{row['phase_before']}|{cause}"
                cause_composition[composition_key] += 1
                if cause == "cancel_ack":
                    cancel_composition[f"{row['side']}|{row['phase_before']}"] += 1
                terminal_seen = True
                break
            current_ts = event_ts
            current_phase = str(row["phase_after"])
            remaining = float(row["remaining_quantity_after"])
        if not terminal_seen:
            invalid_reasons["unsupported_terminal"] += 1
            censored += 1
            continue
        eligible += 1

    return {
        "lifecycle_count": len(by_lifecycle),
        "eligible_lifecycle_count": eligible,
        "censored_lifecycle_count": censored,
        "valid_fraction": eligible / len(by_lifecycle) if by_lifecycle else None,
        "invalid_reasons": dict(sorted(invalid_reasons.items())),
        "exposures": exposures,
        "events": events,
        "cause_composition": dict(sorted(cause_composition.items())),
        "cancel_composition": dict(sorted(cancel_composition.items())),
        "required_exchange_clock_row_count": required_exchange_rows,
        "required_exchange_clock_valid_row_count": required_exchange_valid_rows,
        "required_exchange_clock_coverage": (
            required_exchange_valid_rows / required_exchange_rows
            if required_exchange_rows
            else None
        ),
        "post_terminal_reuse_count": post_terminal_reuse,
        "unknown_terminal_count": unknown_terminal,
        "exchange_to_visibility_lag_ms": _quantiles(exchange_lags_ms),
        "feature_source_to_ready_lag_ms": _quantiles(feature_lags_ms),
        "feature_ready_to_decision_age_ms": _quantiles(decision_ages_ms),
        "feature_visibility_context_available": contexts is not None,
    }


def _reference_support(artifact: Mapping[str, Any], training: Mapping[str, Any]) -> dict[str, Any]:
    exact_cells = {
        (
            str(row["side"]),
            str(row["phase"]),
            int(row["risk_age_bin"]),
            str(row["remaining_class"]),
            int(row["utc_hour_bin"]),
        )
        for row in artifact["cells"]
    }
    parents = {
        (str(row["side"]), str(row["phase"]), int(row["risk_age_bin"]))
        for row in artifact["parent_rates"]
    }
    cause_composition: Counter[str] = Counter()
    cancel_composition: Counter[str] = Counter()
    for row in artifact["cells"]:
        for cause in CAUSES:
            count = int(row["event_counts"].get(cause, 0))
            key = f"{row['side']}|{row['phase']}|{cause}"
            cause_composition[key] += count
            if cause == "cancel_ack":
                cancel_composition[f"{row['side']}|{row['phase']}"] += count
    counts = training["training_counts"]
    eligible = int(counts["eligible_lifecycle_count"])
    censored = int(counts["censored_lifecycle_count"])
    return {
        "exact_cells": exact_cells,
        "parents": parents,
        "valid_fraction": eligible / (eligible + censored),
        "cause_composition": dict(cause_composition),
        "cancel_composition": dict(cancel_composition),
        "eligible_lifecycle_count": eligible,
        "censored_lifecycle_count": censored,
        "risk_exposure_s": float(counts["risk_exposure_s"]),
    }


def _serialize_cells(
    exposures: Mapping[tuple[str, str, int, str, int], float],
    events: Mapping[tuple[str, str, int, str, int], Mapping[str, int]],
    reference: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    exact_exposure = 0.0
    fallback_exposure = 0.0
    unsupported_exposure = 0.0
    rows: list[dict[str, Any]] = []
    for cell in sorted(exposures):
        exposure = float(exposures[cell])
        parent = cell[:3]
        support = (
            "exact"
            if cell in reference["exact_cells"]
            else "fallback"
            if parent in reference["parents"]
            else "unsupported"
        )
        if support == "exact":
            exact_exposure += exposure
        elif support == "fallback":
            fallback_exposure += exposure
        else:
            unsupported_exposure += exposure
        rows.append(
            {
                "side": cell[0],
                "phase": cell[1],
                "risk_age_bin": cell[2],
                "remaining_class": cell[3],
                "utc_hour_bin": cell[4],
                "risk_exposure_s": exposure,
                "event_counts": {
                    cause: int(events.get(cell, {}).get(cause, 0)) for cause in CAUSES
                },
                "reference_support": support,
            }
        )
    total = math.fsum((exact_exposure, fallback_exposure, unsupported_exposure))
    support = {
        "risk_exposure_s": total,
        "exact_exposure_s": exact_exposure,
        "fallback_exposure_s": fallback_exposure,
        "unsupported_exposure_s": unsupported_exposure,
        "exact_fraction": exact_exposure / total if total else None,
        "fallback_fraction": fallback_exposure / total if total else None,
        "unsupported_fraction": unsupported_exposure / total if total else None,
        "cell_count": len(rows),
    }
    return rows, support


def _observed_hours(rows: Sequence[Mapping[str, Any]]) -> float:
    timestamps = [int(row["event_visibility_ts_ns"]) for row in rows]
    return (max(timestamps) - min(timestamps)) / (3600.0 * _NS_PER_S) if len(timestamps) > 1 else 0.0


def run_transport_audit(
    *,
    spec_path: Path,
    admission_dir: Path,
    cif_artifact_path: Path,
    training_report_path: Path,
    lockstep_report_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    spec = validate_spec(spec_path)
    artifact, training, lockstep = validate_reference_chain(
        spec=spec,
        cif_artifact_path=cif_artifact_path,
        training_report_path=training_report_path,
        lockstep_report_path=lockstep_report_path,
    )
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path.resolve()), "sha256": file_sha256(spec_path.resolve())},
        "reference_artifacts": {
            "cif_artifact": {"path": str(cif_artifact_path.resolve()), "sha256": file_sha256(cif_artifact_path.resolve())},
            "training_report": {"path": str(training_report_path.resolve()), "sha256": file_sha256(training_report_path.resolve())},
            "lockstep_report": {"path": str(lockstep_report_path.resolve()), "sha256": file_sha256(lockstep_report_path.resolve())},
        },
        "scope": {
            "outcome_blind": True,
            "economic_outcomes_read": False,
            "pnl_read": False,
            "reward_read": False,
            "markout_read": False,
            "campaign_outcome_read": False,
        },
        "permissions": {
            "transport_supported": False,
            "action_authorized": False,
            "economic_evaluation_authorized": False,
            "live_policy_authorized": False,
        },
    }
    try:
        admission = validate_atomic_admission(admission_dir)
        input_contract = spec["input_contract"]
        frozen_admission_sha = input_contract.get("formal_admission_manifest_sha256")
        if frozen_admission_sha is not None and file_sha256(
            admission["manifest_path"]
        ) != _require_sha256("frozen formal admission SHA256", frozen_admission_sha):
            raise LiveTransportAuditError("formal admission differs from frozen spec")
        frozen_admission_identity = input_contract.get(
            "formal_admission_identity_sha256"
        )
        if frozen_admission_identity is not None and admission["manifest"].get(
            "admission_identity_sha256"
        ) != _require_sha256(
            "frozen formal admission identity", frozen_admission_identity
        ):
            raise LiveTransportAuditError("formal admission identity differs from frozen spec")
        runtime = _runtime_and_epoch(admission)
        rows = _read_journal_parts(admission)
        health = _validate_health(
            admission,
            runtime=runtime,
            expected_rows=len(rows),
        )
        feature_context_paths = admission["by_role"].get("feature_visibility_context", [])
        contexts = (
            _read_feature_context(feature_context_paths[0])
            if len(feature_context_paths) == 1
            else None
        )
        live = _audit_lifecycles(
            rows=rows,
            contexts=contexts,
            artifact=artifact,
        )
        reference = _reference_support(artifact, training)
        cells, support = _serialize_cells(
            live["exposures"], live["events"], reference
        )
        valid_delta = (
            abs(float(live["valid_fraction"]) - float(reference["valid_fraction"]))
            if live["valid_fraction"] is not None
            else None
        )
        cancel_tv = _distribution_tv(
            live["cancel_composition"], reference["cancel_composition"]
        )
        cause_tv = _distribution_tv(
            live["cause_composition"], reference["cause_composition"]
        )
        gates_contract = spec["transport_gates"]
        observed_hours = _observed_hours(rows)
        side_terminal_counts = Counter()
        for key, count in live["cause_composition"].items():
            side_terminal_counts[key.split("|", 1)[0]] += int(count)
        lifecycle_gates = {
            "atomic_admission_hash_chain": True,
            "epoch_fully_bound": True,
            "writer_zero_drop_error": True,
            "journal_exact_schema": True,
            "economic_fields_absent": True,
            "required_exchange_clock_coverage": live[
                "required_exchange_clock_coverage"
            ]
            == 1.0,
            "zero_unknown_terminal": live["unknown_terminal_count"] == 0,
            "zero_post_terminal_reuse": live["post_terminal_reuse_count"] == 0,
            "minimum_observed_hours": observed_hours
            >= float(gates_contract["minimum_observed_hours"]),
            "minimum_lifecycle_count": live["lifecycle_count"]
            >= int(gates_contract["minimum_lifecycle_count"]),
            "minimum_terminal_count_per_side": all(
                side_terminal_counts[side]
                >= int(gates_contract["minimum_terminal_count_per_side"])
                for side in ("BUY", "SELL")
            ),
            "positive_risk_exposure": support["risk_exposure_s"]
            >= float(gates_contract["minimum_risk_exposure_s"]),
            "unsupported_mass": support["unsupported_fraction"] is not None
            and float(support["unsupported_fraction"])
            <= float(gates_contract["unsupported_exposure_fraction_lte"]),
            "valid_fraction_transport": valid_delta is not None
            and valid_delta <= float(gates_contract["valid_fraction_abs_delta_lte"]),
            "cancel_role_composition_transport": cancel_tv is not None
            and cancel_tv <= float(gates_contract["composition_total_variation_lte"]),
            "side_phase_cause_composition_transport": cause_tv is not None
            and cause_tv <= float(gates_contract["composition_total_variation_lte"]),
            "reference_lockstep_passed": bool(lockstep["formal_40day_lockstep_passed"]),
        }
        feature_visibility_gates = {
            "feature_visibility_context_present": contexts is not None,
            "feature_ready_causal_ordering": contexts is not None,
            "feature_context_coverage_complete": contexts is not None
            and len(contexts) == live["lifecycle_count"],
        }
        lifecycle_transport_supported = all(lifecycle_gates.values())
        feature_visibility_transport_supported = all(feature_visibility_gates.values())
        transport_supported = (
            lifecycle_transport_supported and feature_visibility_transport_supported
        )
        status = (
            "passed"
            if transport_supported
            else "lifecycle_transport_passed_feature_visibility_blocked"
            if lifecycle_transport_supported and not feature_visibility_transport_supported
            else "failed_closed"
        )
        report.update(
            {
                "status": status,
                "admission": {
                    "path": str(admission["root"]),
                    "manifest_sha256": file_sha256(admission["manifest_path"]),
                    "admission_identity_sha256": admission["manifest"].get(
                        "admission_identity_sha256",
                        admission["manifest"].get("manifest_sha256"),
                    ),
                    "epoch_id": runtime["epoch_manifest"]["epoch_id"],
                    "epoch_identity_sha256": runtime["epoch_manifest"]["identity_sha256"],
                    "runtime_identity_sha256": runtime["runtime_identity_sha256"],
                    "journal_row_count": len(rows),
                    "health": {
                        "queue_hwm": int(health["live"].get("queue_hwm", 0)),
                        "enqueue_latency_p99_us": health["live"].get("enqueue_latency_p99_us"),
                        "write_latency_p99_ms": health["live"].get("write_latency_p99_ms"),
                        "drop_count": int(health["live"].get("drop_count", 0)),
                        "error_count": int(health["live"].get("error_count", 0)),
                    },
                },
                "denominator": {
                    "observed_hours": observed_hours,
                    "lifecycle_count": live["lifecycle_count"],
                    "eligible_lifecycle_count": live["eligible_lifecycle_count"],
                    "censored_lifecycle_count": live["censored_lifecycle_count"],
                    "side_terminal_counts": dict(sorted(side_terminal_counts.items())),
                    "invalid_reasons": live["invalid_reasons"],
                },
                "live_support": support,
                "cells": cells,
                "cause_shares": {
                    "live_side_phase_cause_counts": live["cause_composition"],
                    "reference_side_phase_cause_counts": reference["cause_composition"],
                    "live_cancel_role_counts": live["cancel_composition"],
                    "reference_cancel_role_counts": reference["cancel_composition"],
                    "cancel_role_definition": "side|phase_before_at_cancel_ack",
                },
                "clock_transport": {
                    "required_exchange_clock_row_count": live[
                        "required_exchange_clock_row_count"
                    ],
                    "required_exchange_clock_valid_row_count": live[
                        "required_exchange_clock_valid_row_count"
                    ],
                    "required_exchange_clock_coverage": live[
                        "required_exchange_clock_coverage"
                    ],
                    "exchange_to_visibility_lag_ms": live[
                        "exchange_to_visibility_lag_ms"
                    ],
                    "feature_source_to_ready_lag_ms": live[
                        "feature_source_to_ready_lag_ms"
                    ],
                    "feature_ready_to_decision_age_ms": live[
                        "feature_ready_to_decision_age_ms"
                    ],
                    "feature_visibility_context_available": live[
                        "feature_visibility_context_available"
                    ],
                },
                "reference_comparison": {
                    "live_valid_fraction": live["valid_fraction"],
                    "reference_valid_fraction": reference["valid_fraction"],
                    "valid_fraction_abs_delta": valid_delta,
                    "cancel_role_composition_total_variation": cancel_tv,
                    "side_phase_cause_composition_total_variation": cause_tv,
                    "valid_fraction_abs_delta_limit": 0.05,
                    "composition_total_variation_limit": 0.15,
                },
                "gates": {**lifecycle_gates, **feature_visibility_gates},
                "gate_groups": {
                    "lifecycle_cif_transport": lifecycle_gates,
                    "q90_feature_visibility_transport": feature_visibility_gates,
                },
                "lifecycle_cif_transport_supported": lifecycle_transport_supported,
                "q90_feature_visibility_transport_supported": (
                    feature_visibility_transport_supported
                ),
                "transport_supported": transport_supported,
            }
        )
        if not feature_visibility_transport_supported:
            report["transport_blocker"] = (
                "admitted lifecycle journal lacks the exact feature visibility companion "
                "(feature_source_exchange_ts_ns, feature_ready_ts_ns, decision_ts_ns); "
                "these timestamps were not inferred or fabricated"
            )
        report["permissions"]["transport_supported"] = transport_supported
    except (LiveTransportAuditError, OSError, ValueError, KeyError, TypeError) as exc:
        report.update(
            {
                "status": "failed_closed",
                "failure_reason": f"{type(exc).__name__}:{exc}",
                "gates": {"input_contract_complete": False},
                "transport_supported": False,
            }
        )
    report["canonical_report_sha256"] = canonical_sha256(report)
    _atomic_write_json(output_path, report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--admission-dir", type=Path, required=True)
    parser.add_argument("--cif-artifact", type=Path, required=True)
    parser.add_argument("--training-report", type=Path, required=True)
    parser.add_argument("--lockstep-report", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = run_transport_audit(
        spec_path=args.spec,
        admission_dir=args.admission_dir,
        cif_artifact_path=args.cif_artifact,
        training_report_path=args.training_report,
        lockstep_report_path=args.lockstep_report,
        output_path=args.out,
    )
    print(json.dumps(report, sort_keys=True))
    return 0 if report["transport_supported"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
