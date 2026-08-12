#!/usr/bin/env python3
"""Inspect or atomically admit one sealed bounded remote journal-v2 session."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root, resolve_portable_path  # noqa: E402
from execution.order_lifecycle_journal_storage_v2 import BOUNDED_REMOTE_SPOOL  # noqa: E402
from execution.order_lifecycle_journal_v2 import (  # noqa: E402
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    validate_order_lifecycle_journal_v2_payload,
)
from execution.order_lifecycle_journal_writer_v2 import (  # noqa: E402
    ORDER_LIFECYCLE_JOURNAL_WRITER_V2_IDENTITY_VERSION,
    ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION,
    _journal_schema_sha256,
    _pyarrow_schema,
)
from execution.order_lifecycle_live_writer_v2 import (  # noqa: E402
    ORDER_LIFECYCLE_LIVE_WRITER_V2_HEALTH_VERSION,
)
from execution.order_lifecycle_remote_spool_v2 import (  # noqa: E402
    REMOTE_SPOOL_HEALTH_SNAPSHOT_VERSION,
    REMOTE_SPOOL_SEAL_VERSION,
    _validate_health,
)
from models.replay.baseline_epoch_manifest import (  # noqa: E402
    REQUIRED_IDENTITY_FIELDS,
    canonical_sha256,
    epoch_identity_sha256,
)
from models.replay.prospective_baseline_epoch import (  # noqa: E402
    PROSPECTIVE_BASELINE_EPOCH_SCHEMA_VERSION,
    PROSPECTIVE_BASELINE_INITIAL_STATE_SCHEMA_VERSION,
    validate_initial_runtime_state_completeness,
)
from scripts.live_remote_pointer import active_live_remote_fields  # noqa: E402

ADMISSION_SCHEMA_VERSION = "prospective_lifecycle_remote_session_admission.v1"
PUBLICATION_SCHEMA_VERSION = "order_lifecycle_remote_spool_seal_publication.v2"
_ACTIVE_REMOTE = active_live_remote_fields(ROOT)
DEFAULT_REMOTE = _ACTIVE_REMOTE.get("ssh_target", "")
DEFAULT_REMOTE_REPO_ROOT = os.environ.get(
    "NARROWGATE_REMOTE_ROOT",
    _ACTIVE_REMOTE.get("repo_root", str(Path.home() / ROOT.name)),
)
DEFAULT_REMOTE_PYTHON = ".venv-active/bin/python3"
DEFAULT_REMOTE_ROOT = str(PurePosixPath(DEFAULT_REMOTE_REPO_ROOT) / "formal_collection")
DEFAULT_LOCAL_ROOT = (
    data_root(ROOT)
    / "formal_collection"
    / "prospective_lifecycle_journal_v2"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SESSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_PART_MANIFEST_FIELDS = {
    "schema_version",
    "batch_id",
    "runtime_identity_sha256",
    "journal_schema_version",
    "journal_schema_sha256",
    "storage_format",
    "data_file",
    "data_sha256",
    "row_count",
    "lifecycle_id",
    "client_order_id",
    "source_callback_id",
    "source_callback_type",
    "first_lifecycle_sequence",
    "last_lifecycle_sequence",
    "first_event_id",
    "last_event_id",
    "event_ids",
    "checkpoint_before",
    "checkpoint_after",
    "contains_local_shutdown_censor",
    "committed_ts_ns",
    "economic_outcomes_read",
}
_SEAL_FIELDS = {
    "schema_version",
    "sealed_ts_ns",
    "seal_nonce",
    "storage_profile",
    "allowlisted_root",
    "session_id",
    "baseline_epoch_id",
    "session_root_relative",
    "epoch_root_relative",
    "seal_relative_path",
    "health_snapshot_relative_path",
    "collection_state",
    "collection_bound_reached",
    "collection_duration_s",
    "session_max_duration_s",
    "session_max_bytes",
    "epoch_identity_sha256",
    "runtime_identity_sha256",
    "part_cursor_summary",
    "source_payload_file_count",
    "file_count",
    "payload_bytes",
    "files",
    "rsync_files_from",
    "excluded_remote_control_paths",
    "raw_health_transferred",
    "writer_lock_transferred",
    "stable_double_read_passed",
    "transfer_executed",
    "local_orico_admission_complete",
    "remote_payload_deleted",
    "economic_outcomes_read",
    "action_authorized",
    "live_policy_authorized",
    "seal_identity_sha256",
}


class RemoteSessionAdmissionError(ValueError):
    """Raised when a bounded remote session cannot be admitted exactly."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _require_sha256(value: Any, *, field_name: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise RemoteSessionAdmissionError(f"{field_name} must be a lowercase SHA256")
    return normalized


def _read_json(path: Path, *, role: str) -> dict[str, Any]:
    unresolved = path.expanduser()
    if unresolved.is_symlink():
        raise RemoteSessionAdmissionError(f"{role} must not be a symlink")
    try:
        resolved = unresolved.resolve(strict=True)
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RemoteSessionAdmissionError(f"invalid {role}: {unresolved}") from exc
    if not isinstance(payload, dict):
        raise RemoteSessionAdmissionError(f"{role} must be a JSON object")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.parent / f".{path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    directories: set[Path] = {root}
    for path in root.rglob("*"):
        if path.is_symlink():
            raise RemoteSessionAdmissionError(f"staging contains a symlink: {path}")
        if path.is_file():
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
            directories.add(path.parent)
        elif path.is_dir():
            directories.add(path)
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        _fsync_directory(directory)


def _safe_relative(value: Any, *, field_name: str) -> str:
    normalized = str(value)
    path = PurePosixPath(normalized)
    if (
        not normalized
        or path.is_absolute()
        or ".." in path.parts
        or any(char in normalized for char in ("\n", "\r", "\x00"))
    ):
        raise RemoteSessionAdmissionError(f"unsafe relative path: {field_name}")
    return normalized


def _normalize_remote_path(root: str, value: str, *, field_name: str) -> str:
    base = PurePosixPath(root)
    candidate = PurePosixPath(value)
    if not base.is_absolute():
        raise RemoteSessionAdmissionError("remote spool root must be absolute")
    if not candidate.is_absolute():
        candidate = base / candidate
    if ".." in candidate.parts or base not in candidate.parents:
        raise RemoteSessionAdmissionError(f"{field_name} escaped the remote spool root")
    return str(candidate)


def _validate_remote_targets(*, root: str, session: str, epoch: str) -> tuple[str, str]:
    normalized_session = _normalize_remote_path(root, session, field_name="session")
    normalized_epoch = _normalize_remote_path(root, epoch, field_name="epoch")
    session_path = PurePosixPath(normalized_session)
    epoch_path = PurePosixPath(normalized_epoch)
    if not session_path.name.startswith("session-"):
        raise RemoteSessionAdmissionError("remote session must use session-<epoch_id>")
    session_id = session_path.name.removeprefix("session-")
    if _SESSION_RE.fullmatch(session_id) is None or epoch_path.name != session_id:
        raise RemoteSessionAdmissionError("remote session and epoch identities differ")
    return normalized_session, normalized_epoch


def _remote_call(
    *,
    mode: str,
    remote: str,
    remote_repo_root: str,
    remote_python: str,
    root: str,
    session: str,
    epoch: str,
    stability_interval_s: float,
) -> dict[str, Any]:
    if mode not in {"inspect", "seal"}:
        raise ValueError("unsupported remote admission mode")
    session, epoch = _validate_remote_targets(root=root, session=session, epoch=epoch)
    helper_path = ROOT / "execution/order_lifecycle_remote_spool_v2.py"
    helper_b64 = base64.b64encode(helper_path.read_bytes()).decode("ascii")
    remote_argv = [
        str(helper_path.name),
        mode,
        "--session-root",
        session,
        "--epoch-root",
        epoch,
        "--allowlisted-root",
        root,
        "--stability-interval-s",
        str(float(stability_interval_s)),
    ]
    launcher = (
        "import base64,sys;"
        f"sys.argv={remote_argv!r};"
        f"exec(compile(base64.b64decode({helper_b64!r}),sys.argv[0],'exec'))"
    )
    command = " ".join(
        (
            "cd",
            shlex.quote(remote_repo_root),
            "&&",
            shlex.quote(remote_python),
            "-c",
            shlex.quote(launcher),
        )
    )
    completed = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", remote, command],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"remote journal-v2 {mode} failed: {completed.stdout}\n{completed.stderr}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("remote journal-v2 command returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("remote journal-v2 command returned a non-object")
    return payload


def inspect_remote_session(**kwargs: Any) -> dict[str, Any]:
    """Perform a side-effect-free remote double-read inspection."""

    payload = _remote_call(mode="inspect", **kwargs)
    if payload.get("transfer_executed") is not False:
        raise RemoteSessionAdmissionError("remote inspect unexpectedly claims transfer")
    if payload.get("formal_collection_valid") is not False:
        raise RemoteSessionAdmissionError("remote inspect unexpectedly claims admission")
    return payload


def _validate_publication(publication: Mapping[str, Any], *, expected_root: str) -> dict:
    required = {
        "schema_version",
        "session_id",
        "baseline_epoch_id",
        "allowlisted_root",
        "seal_path",
        "seal_relative_path",
        "seal_bytes",
        "seal_sha256",
        "seal_identity_sha256",
        "file_count",
        "payload_bytes",
        "rsync_files_from",
        "transfer_executed",
        "remote_payload_deleted",
        "economic_outcomes_read",
    }
    if set(publication) != required:
        raise RemoteSessionAdmissionError("remote seal publication schema mismatch")
    if publication.get("schema_version") != PUBLICATION_SCHEMA_VERSION:
        raise RemoteSessionAdmissionError("unsupported remote seal publication schema")
    session_id = str(publication.get("session_id", ""))
    if _SESSION_RE.fullmatch(session_id) is None:
        raise RemoteSessionAdmissionError("remote seal session identity is invalid")
    if publication.get("baseline_epoch_id") != session_id:
        raise RemoteSessionAdmissionError("remote seal epoch identity differs")
    if publication.get("allowlisted_root") != expected_root:
        raise RemoteSessionAdmissionError("remote seal allowlisted root differs")
    _safe_relative(publication.get("seal_relative_path"), field_name="seal_relative_path")
    _require_sha256(publication.get("seal_sha256"), field_name="seal file SHA256")
    _require_sha256(
        publication.get("seal_identity_sha256"), field_name="seal identity SHA256"
    )
    if int(publication.get("seal_bytes", 0)) <= 0:
        raise RemoteSessionAdmissionError("remote seal byte size is invalid")
    if int(publication.get("file_count", 0)) <= 0 or int(
        publication.get("payload_bytes", 0)
    ) <= 0:
        raise RemoteSessionAdmissionError("remote seal payload accounting is invalid")
    paths = publication.get("rsync_files_from")
    if not isinstance(paths, list) or not paths:
        raise RemoteSessionAdmissionError("remote seal has no rsync file list")
    normalized = [_safe_relative(value, field_name="rsync_files_from") for value in paths]
    if len(set(normalized)) != len(normalized):
        raise RemoteSessionAdmissionError("remote seal rsync file list contains duplicates")
    if publication["seal_relative_path"] not in normalized:
        raise RemoteSessionAdmissionError("remote seal is absent from rsync file list")
    if any(
        bool(publication.get(field))
        for field in ("transfer_executed", "remote_payload_deleted", "economic_outcomes_read")
    ):
        raise RemoteSessionAdmissionError("remote seal publication exceeds its authority")
    return dict(publication)


def _copy_remote_files_once(
    *,
    remote: str,
    root: str,
    publication: Mapping[str, Any],
    destination: Path,
) -> None:
    paths = [str(value) for value in publication["rsync_files_from"]]
    files_from = destination.parent / f".journal-v2-files-from-{uuid.uuid4().hex}"
    files_from.write_text("\n".join(paths) + "\n", encoding="ascii")
    try:
        completed = subprocess.run(
            [
                "rsync",
                "-a",
                "--files-from",
                str(files_from),
                f"{remote}:{root.rstrip('/')}/",
                f"{destination}/",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"journal-v2 session rsync failed: {completed.stdout}\n{completed.stderr}"
            )
    finally:
        files_from.unlink(missing_ok=True)


def _validate_seal(
    *, source_root: Path, publication: Mapping[str, Any]
) -> dict[str, Any]:
    seal_relative = _safe_relative(
        publication["seal_relative_path"], field_name="seal_relative_path"
    )
    seal_path = source_root / seal_relative
    seal = _read_json(seal_path, role="remote session seal")
    if set(seal) != _SEAL_FIELDS:
        raise RemoteSessionAdmissionError("remote session seal schema mismatch")
    if seal.get("schema_version") != REMOTE_SPOOL_SEAL_VERSION:
        raise RemoteSessionAdmissionError("unsupported remote session seal schema")
    if seal.get("seal_identity_sha256") != publication["seal_identity_sha256"]:
        raise RemoteSessionAdmissionError("remote seal identity differs from publication")
    identity_body = dict(seal)
    identity_sha = str(identity_body.pop("seal_identity_sha256"))
    if _canonical_sha256(identity_body) != identity_sha:
        raise RemoteSessionAdmissionError("remote seal canonical identity mismatch")
    if _sha256_file(seal_path) != publication["seal_sha256"]:
        raise RemoteSessionAdmissionError("downloaded remote seal SHA256 differs")
    if seal_path.stat().st_size != int(publication["seal_bytes"]):
        raise RemoteSessionAdmissionError("downloaded remote seal size differs")
    for field in ("session_id", "baseline_epoch_id", "allowlisted_root"):
        if seal.get(field) != publication.get(field):
            raise RemoteSessionAdmissionError(f"remote seal publication differs at {field}")
    if seal.get("seal_relative_path") != seal_relative:
        raise RemoteSessionAdmissionError("remote seal path identity differs")
    if seal.get("storage_profile") != BOUNDED_REMOTE_SPOOL:
        raise RemoteSessionAdmissionError("remote seal storage profile differs")
    if seal.get("collection_state") not in {"bounded_complete", "closed"}:
        raise RemoteSessionAdmissionError("remote seal collection state is invalid")
    if seal.get("collection_bound_reached") is not True:
        raise RemoteSessionAdmissionError("remote seal lacks the bounded completion proof")
    for field in (
        "raw_health_transferred",
        "writer_lock_transferred",
        "transfer_executed",
        "local_orico_admission_complete",
        "remote_payload_deleted",
        "economic_outcomes_read",
        "action_authorized",
        "live_policy_authorized",
    ):
        if bool(seal.get(field)):
            raise RemoteSessionAdmissionError(f"remote seal exceeds authority at {field}")
    if seal.get("stable_double_read_passed") is not True:
        raise RemoteSessionAdmissionError("remote seal lacks a stable double read")
    records = seal.get("files")
    if not isinstance(records, list) or not records:
        raise RemoteSessionAdmissionError("remote seal contains no payload files")
    normalized_records: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict) or set(record) != {"path", "bytes", "sha256"}:
            raise RemoteSessionAdmissionError("remote seal file record schema mismatch")
        relative = _safe_relative(record["path"], field_name="sealed file path")
        size = int(record["bytes"])
        if size < 0:
            raise RemoteSessionAdmissionError("remote seal file size is negative")
        digest = _require_sha256(record["sha256"], field_name="sealed file SHA256")
        normalized_records.append({"path": relative, "bytes": size, "sha256": digest})
    if len({record["path"] for record in normalized_records}) != len(normalized_records):
        raise RemoteSessionAdmissionError("remote seal file paths are duplicated")
    if int(seal.get("file_count", -1)) != len(normalized_records):
        raise RemoteSessionAdmissionError("remote seal file count differs")
    if int(seal.get("payload_bytes", -1)) != sum(
        record["bytes"] for record in normalized_records
    ):
        raise RemoteSessionAdmissionError("remote seal payload byte total differs")
    expected_rsync = [record["path"] for record in normalized_records] + [seal_relative]
    if seal.get("rsync_files_from") != expected_rsync:
        raise RemoteSessionAdmissionError("remote seal rsync list is not exact")
    if publication.get("rsync_files_from") != expected_rsync:
        raise RemoteSessionAdmissionError("publication and seal rsync lists differ")
    expected_paths = set(expected_rsync)
    actual_paths: set[str] = set()
    for path in source_root.rglob("*"):
        if path.is_symlink():
            raise RemoteSessionAdmissionError(f"downloaded payload contains a symlink: {path}")
        if path.is_file():
            actual_paths.add(path.relative_to(source_root).as_posix())
    if actual_paths != expected_paths:
        raise RemoteSessionAdmissionError("downloaded payload file set differs from seal")
    for record in normalized_records:
        path = source_root / record["path"]
        if not path.is_file() or path.is_symlink():
            raise RemoteSessionAdmissionError(f"sealed payload is missing: {record['path']}")
        if path.stat().st_size != record["bytes"]:
            raise RemoteSessionAdmissionError(f"sealed payload size differs: {record['path']}")
        if _sha256_file(path) != record["sha256"]:
            raise RemoteSessionAdmissionError(f"sealed payload SHA256 differs: {record['path']}")
    forbidden_suffixes = {"/writer.lock", "/health.json", "/live_health.json"}
    if any(any(path.endswith(suffix) for suffix in forbidden_suffixes) for path in expected_paths):
        raise RemoteSessionAdmissionError("mutable remote health or writer lock was transferred")
    return {**seal, "files": normalized_records}


def _validate_health_snapshot(
    *, source_root: Path, seal: Mapping[str, Any]
) -> dict[str, Any]:
    path = source_root / _safe_relative(
        seal["health_snapshot_relative_path"], field_name="health snapshot path"
    )
    snapshot = _read_json(path, role="sealed health snapshot")
    required = {
        "schema_version",
        "sealed_ts_ns",
        "session_id",
        "baseline_epoch_id",
        "live_health_source_path",
        "core_health_source_path",
        "live_health",
        "core_health",
        "health_projection",
        "part_cursor_summary",
        "economic_outcomes_read",
        "action_authorized",
        "live_policy_authorized",
        "snapshot_identity_sha256",
    }
    if set(snapshot) != required:
        raise RemoteSessionAdmissionError("sealed health snapshot schema mismatch")
    if snapshot.get("schema_version") != REMOTE_SPOOL_HEALTH_SNAPSHOT_VERSION:
        raise RemoteSessionAdmissionError("unsupported sealed health snapshot schema")
    identity_body = dict(snapshot)
    identity_sha = str(identity_body.pop("snapshot_identity_sha256"))
    if _canonical_sha256(identity_body) != identity_sha:
        raise RemoteSessionAdmissionError("sealed health snapshot identity mismatch")
    if snapshot.get("session_id") != seal["session_id"] or snapshot.get(
        "baseline_epoch_id"
    ) != seal["baseline_epoch_id"]:
        raise RemoteSessionAdmissionError("sealed health session identity differs")
    live_health = snapshot.get("live_health")
    core_health = snapshot.get("core_health")
    if not isinstance(live_health, dict) or not isinstance(core_health, dict):
        raise RemoteSessionAdmissionError("sealed health payload is invalid")
    if live_health.get("schema_version") != ORDER_LIFECYCLE_LIVE_WRITER_V2_HEALTH_VERSION:
        raise RemoteSessionAdmissionError("sealed live health schema is unsupported")
    try:
        projection = _validate_health(
            live_health=live_health,
            core_health=core_health,
            session_id=str(seal["session_id"]),
        )
    except ValueError as exc:
        raise RemoteSessionAdmissionError("sealed health mechanics are invalid") from exc
    if projection != snapshot.get("health_projection"):
        raise RemoteSessionAdmissionError("sealed health projection differs")
    if snapshot.get("part_cursor_summary") != seal.get("part_cursor_summary"):
        raise RemoteSessionAdmissionError("sealed health part summary differs")
    if any(
        bool(snapshot.get(field))
        for field in ("economic_outcomes_read", "action_authorized", "live_policy_authorized")
    ):
        raise RemoteSessionAdmissionError("sealed health snapshot exceeds authority")
    return snapshot


def _validate_epoch_and_identity(
    *, source_root: Path, seal: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    epoch_root = source_root / _safe_relative(
        seal["epoch_root_relative"], field_name="epoch_root_relative"
    )
    session_root = source_root / _safe_relative(
        seal["session_root_relative"], field_name="session_root_relative"
    )
    manifest = _read_json(epoch_root / "epoch_manifest.json", role="epoch manifest")
    required_epoch = {
        "schema_version",
        "epoch_id",
        "start_ts_ns",
        "end_ts_ns",
        "start_reason",
        "binding_status",
        "storage_profile",
        "remote_spool_only",
        "local_admission_complete",
        "collection_bounds",
        "identity",
        "identity_sha256",
        "initial_runtime_state",
        "identity_evidence",
        "historical_epochs_backfilled",
        "formal_collection_valid",
        "formal_collection_valid_reason",
        "permissions",
    }
    if set(manifest) != required_epoch:
        raise RemoteSessionAdmissionError("prospective epoch manifest schema mismatch")
    if manifest.get("schema_version") != PROSPECTIVE_BASELINE_EPOCH_SCHEMA_VERSION:
        raise RemoteSessionAdmissionError("unsupported prospective epoch schema")
    if manifest.get("epoch_id") != seal["session_id"]:
        raise RemoteSessionAdmissionError("prospective epoch ID differs from seal")
    if manifest.get("binding_status") != "fully_bound":
        raise RemoteSessionAdmissionError("prospective epoch is not fully_bound")
    if manifest.get("storage_profile") != BOUNDED_REMOTE_SPOOL or not bool(
        manifest.get("remote_spool_only")
    ):
        raise RemoteSessionAdmissionError("prospective epoch storage profile differs")
    if bool(manifest.get("local_admission_complete")) or bool(
        manifest.get("formal_collection_valid")
    ):
        raise RemoteSessionAdmissionError("remote prospective epoch claims local admission")
    permissions = manifest.get("permissions")
    if not isinstance(permissions, dict) or any(bool(value) for value in permissions.values()):
        raise RemoteSessionAdmissionError("remote prospective epoch grants authority")
    identity = manifest.get("identity")
    if not isinstance(identity, dict) or set(identity) != set(REQUIRED_IDENTITY_FIELDS):
        raise RemoteSessionAdmissionError("prospective epoch identity schema differs")
    for field in REQUIRED_IDENTITY_FIELDS:
        _require_sha256(identity[field], field_name=f"epoch identity {field}")
    epoch_identity = epoch_identity_sha256(identity)
    if manifest.get("identity_sha256") != epoch_identity or seal.get(
        "epoch_identity_sha256"
    ) != epoch_identity:
        raise RemoteSessionAdmissionError("prospective epoch identity hash differs")
    bounds = manifest.get("collection_bounds")
    if not isinstance(bounds, dict) or set(bounds) != {"max_duration_s", "max_bytes"}:
        raise RemoteSessionAdmissionError("prospective epoch collection bounds differ")
    if float(bounds["max_duration_s"]) != float(seal["session_max_duration_s"]) or int(
        bounds["max_bytes"]
    ) != int(seal["session_max_bytes"]):
        raise RemoteSessionAdmissionError("epoch and seal collection bounds differ")

    initial_reference = manifest.get("initial_runtime_state")
    evidence_reference = manifest.get("identity_evidence")
    if not isinstance(initial_reference, dict) or set(initial_reference) != {
        "path",
        "canonical_sha256",
    }:
        raise RemoteSessionAdmissionError("epoch initial-state reference schema differs")
    if not isinstance(evidence_reference, dict) or set(evidence_reference) != {
        "path",
        "canonical_sha256",
    }:
        raise RemoteSessionAdmissionError("epoch evidence reference schema differs")
    initial_path = epoch_root / _safe_relative(
        initial_reference["path"], field_name="initial runtime state path"
    )
    evidence_path = epoch_root / _safe_relative(
        evidence_reference["path"], field_name="identity evidence path"
    )
    initial = _read_json(initial_path, role="initial runtime state")
    evidence = _read_json(evidence_path, role="epoch identity evidence")
    if initial.get("schema_version") != PROSPECTIVE_BASELINE_INITIAL_STATE_SCHEMA_VERSION:
        raise RemoteSessionAdmissionError("initial runtime state schema differs")
    state = initial.get("state")
    if not isinstance(state, dict):
        raise RemoteSessionAdmissionError("initial runtime state payload is invalid")
    try:
        validate_initial_runtime_state_completeness(state)
    except ValueError as exc:
        raise RemoteSessionAdmissionError("initial runtime state is not fully bound") from exc
    if canonical_sha256(initial) != initial_reference["canonical_sha256"]:
        raise RemoteSessionAdmissionError("initial runtime state canonical hash differs")
    if identity["initial_runtime_state_sha256"] != initial_reference["canonical_sha256"]:
        raise RemoteSessionAdmissionError("epoch identity binds another initial state")
    if canonical_sha256(evidence) != evidence_reference["canonical_sha256"]:
        raise RemoteSessionAdmissionError("epoch identity evidence canonical hash differs")

    writer_identity = _read_json(
        session_root / "runtime_identity.json", role="journal runtime identity"
    )
    if writer_identity.get("schema_version") != ORDER_LIFECYCLE_JOURNAL_WRITER_V2_IDENTITY_VERSION:
        raise RemoteSessionAdmissionError("journal runtime identity schema differs")
    runtime_identity = writer_identity.get("runtime_identity")
    if not isinstance(runtime_identity, dict):
        raise RemoteSessionAdmissionError("journal runtime identity payload is invalid")
    runtime_sha = _canonical_sha256(runtime_identity)
    if runtime_sha != writer_identity.get("runtime_identity_sha256") or runtime_sha != seal.get(
        "runtime_identity_sha256"
    ):
        raise RemoteSessionAdmissionError("journal runtime identity hash differs")
    if runtime_identity.get("baseline_epoch_id") != seal["session_id"]:
        raise RemoteSessionAdmissionError("journal runtime epoch ID differs")
    if runtime_identity.get("baseline_epoch_identity_sha256") != epoch_identity:
        raise RemoteSessionAdmissionError("journal runtime binds another epoch")
    if runtime_identity.get("storage_profile") != BOUNDED_REMOTE_SPOOL:
        raise RemoteSessionAdmissionError("journal runtime storage profile differs")
    for field in REQUIRED_IDENTITY_FIELDS:
        if runtime_identity.get(field) != identity[field]:
            raise RemoteSessionAdmissionError(f"journal runtime identity differs at {field}")
    if writer_identity.get("journal_schema_version") != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
        raise RemoteSessionAdmissionError("journal schema version differs")
    if writer_identity.get("journal_schema_sha256") != _journal_schema_sha256():
        raise RemoteSessionAdmissionError("journal schema hash differs")
    if writer_identity.get("storage_format") != "parquet":
        raise RemoteSessionAdmissionError("formal remote session admission requires Parquet")
    if bool(writer_identity.get("economic_outcomes_read")) or bool(
        writer_identity.get("q90_action_authorized")
    ):
        raise RemoteSessionAdmissionError("journal runtime identity exceeds authority")
    return manifest, writer_identity


def _checkpoint(value: Any, *, role: str) -> dict[str, Any]:
    required = {
        "schema_version",
        "lifecycle_id",
        "client_order_id",
        "last_emitted_sequence",
        "last_event_id",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise RemoteSessionAdmissionError(f"{role} checkpoint schema mismatch")
    return value


def _validate_parts_and_cursors(
    *, source_root: Path, seal: Mapping[str, Any], writer_identity: Mapping[str, Any]
) -> dict[str, Any]:
    session_root = source_root / str(seal["session_root_relative"])
    parts_root = session_root / "parts"
    cursors_root = session_root / "cursors"
    manifests = sorted(parts_root.glob("part-*.manifest.json"))
    data_files = sorted(parts_root.glob("part-*.parquet"))
    if not manifests or len(manifests) != len(data_files):
        raise RemoteSessionAdmissionError("journal part manifest/data counts differ")
    runtime_sha = str(writer_identity["runtime_identity_sha256"])
    records: list[dict[str, Any]] = []
    referenced_data: set[Path] = set()
    all_event_ids: set[str] = set()
    by_lifecycle: dict[str, list[dict[str, Any]]] = {}
    total_rows = 0
    for manifest_path in manifests:
        manifest = _read_json(manifest_path, role="journal part manifest")
        if set(manifest) != _PART_MANIFEST_FIELDS:
            raise RemoteSessionAdmissionError("journal part manifest schema mismatch")
        if manifest.get("schema_version") != ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION:
            raise RemoteSessionAdmissionError("journal part manifest version differs")
        batch_id = _require_sha256(manifest.get("batch_id"), field_name="batch id")
        if manifest_path.name != f"part-{batch_id}.manifest.json":
            raise RemoteSessionAdmissionError("journal part manifest filename differs")
        data_path = parts_root / str(manifest.get("data_file", ""))
        if data_path != parts_root / f"part-{batch_id}.parquet":
            raise RemoteSessionAdmissionError("journal part data filename differs")
        if manifest.get("runtime_identity_sha256") != runtime_sha:
            raise RemoteSessionAdmissionError("journal part runtime identity differs")
        if manifest.get("journal_schema_version") != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
            raise RemoteSessionAdmissionError("journal part schema version differs")
        if manifest.get("journal_schema_sha256") != _journal_schema_sha256():
            raise RemoteSessionAdmissionError("journal part schema hash differs")
        if manifest.get("storage_format") != "parquet":
            raise RemoteSessionAdmissionError("journal part is not Parquet")
        if _sha256_file(data_path) != manifest.get("data_sha256"):
            raise RemoteSessionAdmissionError("journal part data hash differs")
        if bool(manifest.get("economic_outcomes_read")):
            raise RemoteSessionAdmissionError("journal part claims economic outcomes")

        import pyarrow.parquet as pq

        table = pq.read_table(data_path)
        if not table.schema.equals(_pyarrow_schema()):
            raise RemoteSessionAdmissionError("journal Parquet schema differs")
        if table.column_names != list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS):
            raise RemoteSessionAdmissionError("journal Parquet column order differs")
        rows = table.to_pylist()
        row_count = int(manifest.get("row_count", -1))
        if row_count <= 0 or len(rows) != row_count:
            raise RemoteSessionAdmissionError("journal Parquet row count differs")
        for row in rows:
            if set(row) != set(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS):
                raise RemoteSessionAdmissionError("journal row schema differs")
            try:
                validate_order_lifecycle_journal_v2_payload(row)
            except ValueError as exc:
                raise RemoteSessionAdmissionError("journal row semantics are invalid") from exc
        event_ids = [str(row["event_id"]) for row in rows]
        if event_ids != list(manifest.get("event_ids", ())):
            raise RemoteSessionAdmissionError("journal part event identities differ")
        if all_event_ids.intersection(event_ids):
            raise RemoteSessionAdmissionError("journal event identity is duplicated")
        all_event_ids.update(event_ids)
        first = rows[0]
        last = rows[-1]
        if (
            first["lifecycle_id"] != manifest["lifecycle_id"]
            or first["client_order_id"] != manifest["client_order_id"]
            or first["source_callback_id"] != manifest["source_callback_id"]
            or first["source_callback_type"] != manifest["source_callback_type"]
            or int(first["lifecycle_sequence"])
            != int(manifest["first_lifecycle_sequence"])
            or int(last["lifecycle_sequence"])
            != int(manifest["last_lifecycle_sequence"])
            or first["event_id"] != manifest["first_event_id"]
            or last["event_id"] != manifest["last_event_id"]
        ):
            raise RemoteSessionAdmissionError("journal part manifest/data identity differs")
        before = _checkpoint(manifest["checkpoint_before"], role="before")
        after = _checkpoint(manifest["checkpoint_after"], role="after")
        expected_batch = _canonical_sha256(
            {
                "schema_version": ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION,
                "lifecycle_id": first["lifecycle_id"],
                "event_ids": event_ids,
                "source_callback_id": first["source_callback_id"],
                "checkpoint_after": after,
            }
        )
        if expected_batch != batch_id:
            raise RemoteSessionAdmissionError("journal part content address differs")
        records.append(manifest)
        referenced_data.add(data_path)
        total_rows += row_count
        by_lifecycle.setdefault(str(manifest["lifecycle_id"]), []).append(manifest)
    if referenced_data != set(data_files):
        raise RemoteSessionAdmissionError("journal manifests do not cover every Parquet file")

    expected_cursors: dict[str, dict[str, Any]] = {}
    for lifecycle_id, lifecycle_records in by_lifecycle.items():
        lifecycle_records.sort(key=lambda item: int(item["first_lifecycle_sequence"]))
        expected_sequence = 1
        prior_event = ""
        client_order_id = str(lifecycle_records[0]["client_order_id"])
        for manifest in lifecycle_records:
            before = _checkpoint(manifest["checkpoint_before"], role="before")
            after = _checkpoint(manifest["checkpoint_after"], role="after")
            if (
                before["lifecycle_id"] != lifecycle_id
                or before["client_order_id"] != client_order_id
                or int(before["last_emitted_sequence"]) != expected_sequence - 1
                or str(before["last_event_id"]) != prior_event
                or int(manifest["first_lifecycle_sequence"]) != expected_sequence
            ):
                raise RemoteSessionAdmissionError("journal lifecycle sequence chain differs")
            expected_sequence = int(after["last_emitted_sequence"]) + 1
            prior_event = str(after["last_event_id"])
        expected_cursors[lifecycle_id] = _checkpoint(
            lifecycle_records[-1]["checkpoint_after"], role="final"
        )
    cursor_files = sorted(cursors_root.glob("cursor-*.json"))
    observed_cursors: dict[str, dict[str, Any]] = {}
    for path in cursor_files:
        cursor = _checkpoint(_read_json(path, role="durable cursor"), role="durable")
        lifecycle_id = str(cursor["lifecycle_id"])
        expected_name = f"cursor-{hashlib.sha256(lifecycle_id.encode()).hexdigest()}.json"
        if path.name != expected_name or lifecycle_id in observed_cursors:
            raise RemoteSessionAdmissionError("durable cursor path or identity differs")
        observed_cursors[lifecycle_id] = cursor
    if observed_cursors != expected_cursors:
        raise RemoteSessionAdmissionError("durable cursors do not match final part boundaries")
    summary = {
        "storage_format": "parquet",
        "part_count": len(records),
        "cursor_count": len(observed_cursors),
        "row_count": total_rows,
        "lifecycle_count": len(by_lifecycle),
        "event_id_count": len(all_event_ids),
    }
    if summary != seal.get("part_cursor_summary"):
        raise RemoteSessionAdmissionError("local part/cursor summary differs from seal")
    return summary


def validate_downloaded_session(
    *, staging_source_root: Path, publication: Mapping[str, Any]
) -> dict[str, Any]:
    """Deeply validate every transferred payload without mutating the staging tree."""

    source_root = staging_source_root.expanduser().resolve(strict=True)
    seal = _validate_seal(source_root=source_root, publication=publication)
    health = _validate_health_snapshot(source_root=source_root, seal=seal)
    epoch, writer_identity = _validate_epoch_and_identity(
        source_root=source_root,
        seal=seal,
    )
    summary = _validate_parts_and_cursors(
        source_root=source_root,
        seal=seal,
        writer_identity=writer_identity,
    )
    live_health = health["live_health"]
    core_health = health["core_health"]
    if int(live_health["rows_committed"]) != summary["row_count"] or int(
        core_health["rows_committed"]
    ) != summary["row_count"]:
        raise RemoteSessionAdmissionError("sealed health and Parquet row counts differ")
    if int(core_health["callbacks_committed"]) != summary["part_count"]:
        raise RemoteSessionAdmissionError("sealed health and part counts differ")
    if int(core_health["durable_cursor_count"]) != summary["cursor_count"]:
        raise RemoteSessionAdmissionError("sealed health and cursor counts differ")
    return {
        "session_id": str(seal["session_id"]),
        "baseline_epoch_id": str(seal["baseline_epoch_id"]),
        "seal_identity_sha256": str(seal["seal_identity_sha256"]),
        "seal_file_sha256": str(publication["seal_sha256"]),
        "epoch_identity_sha256": str(epoch["identity_sha256"]),
        "runtime_identity_sha256": str(writer_identity["runtime_identity_sha256"]),
        "journal_schema_sha256": _journal_schema_sha256(),
        "file_count": int(seal["file_count"]),
        "payload_bytes": int(seal["payload_bytes"]),
        **summary,
        "health_drop_count": 0,
        "health_error_count": 0,
        "epoch_fully_bound": True,
        "stable_double_read_passed": True,
        "remote_payload_deleted": False,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }


def admit_remote_session(
    *,
    remote: str,
    remote_repo_root: str,
    remote_python: str,
    root: str,
    session: str,
    epoch: str,
    local_root: Path,
    stability_interval_s: float = 0.25,
    require_orico: bool = True,
    remote_call: Callable[..., dict[str, Any]] = _remote_call,
    copy_remote_files: Callable[..., None] = _copy_remote_files_once,
) -> dict[str, Any]:
    """Seal remotely, transfer once, validate, and atomically admit locally."""

    session, epoch = _validate_remote_targets(root=root, session=session, epoch=epoch)
    publication = remote_call(
        mode="seal",
        remote=remote,
        remote_repo_root=remote_repo_root,
        remote_python=remote_python,
        root=root,
        session=session,
        epoch=epoch,
        stability_interval_s=stability_interval_s,
    )
    publication = _validate_publication(publication, expected_root=root)
    if publication["session_id"] != PurePosixPath(epoch).name:
        raise RemoteSessionAdmissionError("remote seal belongs to another requested epoch")

    unresolved_local = local_root.expanduser()
    if unresolved_local.is_symlink():
        raise RemoteSessionAdmissionError("local admission root must not be a symlink")
    local = unresolved_local.resolve()
    if require_orico:
        storage_root = resolve_portable_path(
            "${NARROWGATE_STORAGE_ROOT}", root=ROOT
        )
        if not storage_root.exists() or not os.path.ismount(storage_root):
            raise RemoteSessionAdmissionError("configured storage root is not mounted")
        if storage_root != local and storage_root not in local.parents:
            raise RemoteSessionAdmissionError(
                "local admission root must be on the configured storage root"
            )
    local.mkdir(parents=True, exist_ok=True)
    destination = local / f"session-{publication['session_id']}"
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"journal-v2 session admission already exists: {destination}")
    free_bytes = shutil.disk_usage(local).free
    # Atomic rename does not duplicate the staging payload, but retain room for
    # filesystem metadata and the admission manifest.
    minimum_free = int(publication["payload_bytes"]) + (64 << 20)
    if free_bytes < minimum_free:
        raise RemoteSessionAdmissionError("insufficient local space for atomic admission")

    staging = local / f".session-{publication['session_id']}.partial-{uuid.uuid4().hex}"
    source = staging / "source"
    staging.mkdir()
    source.mkdir()
    try:
        copy_remote_files(
            remote=remote,
            root=root,
            publication=publication,
            destination=source,
        )
        validation = validate_downloaded_session(
            staging_source_root=source,
            publication=publication,
        )
        manifest: dict[str, Any] = {
            "schema_version": ADMISSION_SCHEMA_VERSION,
            "admitted_ts_ns": time.time_ns(),
            "remote": remote,
            "remote_repo_root": remote_repo_root,
            "remote_allowlisted_root": root,
            "remote_session_root": session,
            "remote_epoch_root": epoch,
            "remote_seal_path": publication["seal_path"],
            "remote_seal_sha256": publication["seal_sha256"],
            "remote_seal_identity_sha256": publication["seal_identity_sha256"],
            "single_rsync_files_from_session": True,
            "atomic_rename_admission": True,
            "remote_payload_deleted": False,
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_policy_authorized": False,
            "validation": validation,
        }
        manifest["admission_identity_sha256"] = _canonical_sha256(manifest)
        _atomic_json(staging / "admission_manifest.json", manifest)
        _fsync_tree(staging)
        os.replace(staging, destination)
        _fsync_directory(local)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    admitted_manifest = destination / "admission_manifest.json"
    return {
        "destination": str(destination),
        "admission_manifest": str(admitted_manifest),
        "admission_manifest_sha256": _sha256_file(admitted_manifest),
        "admission_identity_sha256": manifest["admission_identity_sha256"],
        "validation": validation,
        "remote_payload_deleted": False,
        "economic_outcomes_read": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inspect", "dry-run", "execute"))
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--remote-repo-root", default=DEFAULT_REMOTE_REPO_ROOT)
    parser.add_argument("--remote-python", default=DEFAULT_REMOTE_PYTHON)
    parser.add_argument("--root", default=DEFAULT_REMOTE_ROOT)
    parser.add_argument("--session", required=True)
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--local-root", type=Path, default=DEFAULT_LOCAL_ROOT)
    parser.add_argument("--stability-interval-s", type=float, default=0.25)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    common = {
        "remote": args.remote,
        "remote_repo_root": args.remote_repo_root,
        "remote_python": args.remote_python,
        "root": args.root,
        "session": args.session,
        "epoch": args.epoch,
        "stability_interval_s": args.stability_interval_s,
    }
    if args.mode in {"inspect", "dry-run"}:
        payload = inspect_remote_session(**common)
    else:
        payload = admit_remote_session(local_root=args.local_root, **common)
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
