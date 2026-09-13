"""Seal a bounded remote journal-v2 session without stopping its live writer.

The producer stops accepting callbacks after its collection bound, but its
health heartbeat and process remain live.  This module proves that the durable
parts and cursors have stopped changing, snapshots the two mutable health
documents, and atomically publishes an immutable, rsync-ready seal.  It never
performs transport, local admission, economic evaluation, or remote deletion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import time
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from execution.order_lifecycle_journal_storage_v2 import (
    BOUNDED_REMOTE_SPOOL,
    validate_remote_spool_path,
)

REMOTE_SPOOL_TRANSFER_MANIFEST_VERSION = "order_lifecycle_remote_spool_transfer.v2"
REMOTE_SPOOL_TRANSFER_MANIFEST_NAME = "remote_spool_transfer_manifest.json"
REMOTE_SPOOL_SEAL_VERSION = "order_lifecycle_remote_spool_seal.v2"
REMOTE_SPOOL_HEALTH_SNAPSHOT_VERSION = "order_lifecycle_remote_spool_health_snapshot.v2"
REMOTE_SPOOL_SEAL_DIRECTORY = ".journal-v2-session-seals"
REMOTE_SPOOL_SEAL_NAME = "remote_session_seal.json"
REMOTE_SPOOL_HEALTH_SNAPSHOT_NAME = "health_snapshot.json"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MUTABLE_SESSION_FILES = frozenset({"writer.lock", "health.json", "live_health.json"})
_CONTROL_SESSION_FILES = frozenset({REMOTE_SPOOL_TRANSFER_MANIFEST_NAME})


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
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
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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
        raise ValueError(f"{field_name} must be a lowercase SHA256")
    return normalized


def _relative_path(path: Path, root: Path) -> str:
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(f"remote spool path escaped its allowlist: {path}") from exc
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        raise ValueError(f"unsafe allowlist-relative spool path: {relative!r}")
    return relative


def _files_under(
    root: Path,
    *,
    excluded: set[Path],
    excluded_directory_names: set[str] | None = None,
) -> tuple[Path, ...]:
    excluded_names = excluded_directory_names or set()
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"remote spool contains a symlink: {path}")
        if any(part in excluded_names for part in path.relative_to(root).parts[:-1]):
            continue
        if not path.is_file() or path in excluded:
            continue
        if ".partial-" in path.name:
            raise ValueError(f"remote spool contains a partial file: {path}")
        files.append(path)
    return tuple(files)


def _file_record(path: Path, *, allowlisted_root: Path) -> dict[str, Any]:
    before = path.stat()
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"remote spool payload is not a regular file: {path}")
    digest = _sha256_file(path)
    after = path.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise ValueError(f"remote spool payload changed while hashing: {path}")
    return {
        "path": _relative_path(path, allowlisted_root),
        "bytes": int(after.st_size),
        "sha256": digest,
    }


def _health_projection(live_health: Mapping[str, Any], core_health: Mapping[str, Any]) -> dict:
    live_fields = (
        "schema_version",
        "session_id",
        "baseline_epoch_id",
        "state",
        "storage_profile",
        "remote_spool_only",
        "local_admission_complete",
        "session_max_duration_s",
        "session_max_bytes",
        "queue_capacity",
        "queue_depth",
        "queue_hwm",
        "callbacks_enqueued",
        "callbacks_processed",
        "rows_committed",
        "drop_count",
        "error_count",
        "collection_bound_reached",
        "collection_bound_reason",
        "collection_stopped_ts_ns",
        "collection_duration_s",
        "callbacks_ignored_after_bound",
        "session_byte_limit_exceeded",
        "remote_spool_valid",
        "formal_collection_valid",
        "formal_collection_valid_reason",
    )
    core_fields = (
        "schema_version",
        "session_id",
        "runtime_identity_sha256",
        "storage_format",
        "state",
        "closed",
        "restart_count",
        "last_flush_ts_ns",
        "last_flush_batch_id",
        "callbacks_committed",
        "rows_committed",
        "rows_dropped",
        "callbacks_quarantined",
        "error_count",
        "quarantine_order_ids",
        "excluded_lifecycle_ids",
        "excluded_client_order_ids",
        "durable_cursor_count",
        "local_shutdown_censor_count",
        "orphan_payload_count",
        "orphan_payload_files",
        "formal_collection_valid",
        "economic_outcomes_read",
        "q90_action_authorized",
    )
    return {
        "live": {field: live_health.get(field) for field in live_fields},
        "core": {field: core_health.get(field) for field in core_fields},
    }


def _stable_health_projection(
    live_health: Mapping[str, Any], core_health: Mapping[str, Any]
) -> dict[str, Any]:
    """Return only fields that must stop changing once the payload is bounded."""

    projection = _health_projection(live_health, core_health)
    # The running producer intentionally counts callbacks ignored after the
    # duration bound. That heartbeat diagnostic may continue increasing even
    # though parts, cursors, identities, and committed accounting are frozen.
    projection["live"].pop("callbacks_ignored_after_bound", None)
    return projection


def _validate_health(
    *,
    live_health: Mapping[str, Any],
    core_health: Mapping[str, Any],
    session_id: str,
) -> dict[str, Any]:
    if live_health.get("session_id") != session_id:
        raise ValueError("live health session identity mismatch")
    if live_health.get("baseline_epoch_id") != session_id:
        raise ValueError("live health baseline epoch identity mismatch")
    if core_health.get("session_id") != session_id:
        raise ValueError("core health session identity mismatch")
    if live_health.get("storage_profile") != BOUNDED_REMOTE_SPOOL:
        raise ValueError("live health is not a bounded remote spool")
    state = live_health.get("state")
    if state not in {"bounded_complete", "closed"}:
        raise ValueError("remote spool has not reached bounded_complete")
    if state == "bounded_complete" and not bool(
        live_health.get("collection_bound_reached")
    ):
        raise ValueError("remote spool collection bound has not been reached")
    if int(live_health.get("queue_depth", -1)) != 0:
        raise ValueError("remote spool queue is not drained")
    callbacks_enqueued = int(live_health.get("callbacks_enqueued", -1))
    callbacks_processed = int(live_health.get("callbacks_processed", -2))
    if callbacks_enqueued < 0 or callbacks_processed != callbacks_enqueued:
        raise ValueError("remote spool callback counts have not converged")
    rows_committed = int(live_health.get("rows_committed", -1))
    if rows_committed <= 0 or rows_committed != int(core_health.get("rows_committed", -2)):
        raise ValueError("remote spool live/core committed row counts disagree")
    if int(live_health.get("drop_count", -1)) != 0:
        raise ValueError("remote spool has producer drops")
    if int(live_health.get("error_count", -1)) != 0:
        raise ValueError("remote spool has writer errors")
    if int(core_health.get("rows_dropped", -1)) != 0:
        raise ValueError("remote spool core writer dropped rows")
    if int(core_health.get("error_count", -1)) != 0:
        raise ValueError("remote spool core writer has errors")
    if int(core_health.get("callbacks_quarantined", -1)) != 0:
        raise ValueError("remote spool contains quarantined callbacks")
    if core_health.get("quarantine_order_ids") not in ([], ()):
        raise ValueError("remote spool remains in hot-start quarantine")
    if int(core_health.get("orphan_payload_count", -1)) != 0:
        raise ValueError("remote spool contains orphan payloads")
    if core_health.get("orphan_payload_files") not in ([], ()):
        raise ValueError("remote spool reports orphan payload files")
    if not bool(live_health.get("remote_spool_valid")):
        raise ValueError("remote spool health is invalid")
    if bool(live_health.get("formal_collection_valid")):
        raise ValueError("remote spool must not claim local formal admission")
    if bool(live_health.get("local_admission_complete")):
        raise ValueError("remote spool cannot complete local admission")
    if not bool(core_health.get("formal_collection_valid")):
        raise ValueError("journal writer mechanics health is invalid")
    if bool(core_health.get("economic_outcomes_read")):
        raise ValueError("journal writer health claims economic outcome access")
    if bool(core_health.get("q90_action_authorized")):
        raise ValueError("journal writer health claims q90 action authority")
    if bool(live_health.get("session_byte_limit_exceeded")):
        raise ValueError("remote spool byte limit was exceeded")
    embedded = live_health.get("core_health")
    if not isinstance(embedded, Mapping):
        raise ValueError("live health is missing its core health snapshot")
    if _health_projection(live_health, embedded)["core"] != _health_projection(
        live_health, core_health
    )["core"]:
        raise ValueError("live health embeds a different core writer state")
    return _health_projection(live_health, core_health)


def _validate_epoch_and_writer_identity(
    *,
    epoch_manifest: Mapping[str, Any],
    writer_identity: Mapping[str, Any],
    session_id: str,
    live_health: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    from models.replay.baseline_epoch_manifest import (
        REQUIRED_IDENTITY_FIELDS,
        epoch_identity_sha256,
    )
    from models.replay.prospective_baseline_epoch import (
        PROSPECTIVE_BASELINE_EPOCH_SCHEMA_VERSION,
    )

    if epoch_manifest.get("schema_version") != PROSPECTIVE_BASELINE_EPOCH_SCHEMA_VERSION:
        raise ValueError("unsupported prospective epoch manifest schema")
    if epoch_manifest.get("epoch_id") != session_id:
        raise ValueError("epoch manifest disagrees with session id")
    if epoch_manifest.get("binding_status") != "fully_bound":
        raise ValueError("prospective epoch is not fully_bound")
    if epoch_manifest.get("storage_profile") != BOUNDED_REMOTE_SPOOL:
        raise ValueError("epoch manifest is not a bounded remote spool")
    if not bool(epoch_manifest.get("remote_spool_only")):
        raise ValueError("epoch manifest is not marked remote-spool-only")
    if bool(epoch_manifest.get("local_admission_complete")):
        raise ValueError("prospective epoch cannot claim local admission on remote")
    identity = epoch_manifest.get("identity")
    if not isinstance(identity, Mapping) or set(identity) != set(REQUIRED_IDENTITY_FIELDS):
        raise ValueError("prospective epoch identity schema mismatch")
    for field in REQUIRED_IDENTITY_FIELDS:
        _require_sha256(identity.get(field), field_name=f"epoch identity {field}")
    expected_epoch_identity = epoch_identity_sha256(identity)
    if epoch_manifest.get("identity_sha256") != expected_epoch_identity:
        raise ValueError("prospective epoch canonical identity mismatch")

    epoch_bounds = epoch_manifest.get("collection_bounds")
    if not isinstance(epoch_bounds, Mapping) or set(epoch_bounds) != {
        "max_duration_s",
        "max_bytes",
    }:
        raise ValueError("epoch manifest is missing exact remote collection bounds")
    max_duration_s = float(live_health.get("session_max_duration_s", 0.0))
    max_bytes = int(live_health.get("session_max_bytes", 0))
    if max_duration_s <= 0.0 or max_bytes <= 0:
        raise ValueError("remote collection bounds are not positive")
    if float(epoch_bounds["max_duration_s"]) != max_duration_s or int(
        epoch_bounds["max_bytes"]
    ) != max_bytes:
        raise ValueError("epoch and writer remote collection bounds disagree")

    runtime_identity = writer_identity.get("runtime_identity")
    if not isinstance(runtime_identity, Mapping):
        raise ValueError("writer runtime identity is missing")
    expected_runtime_hash = _canonical_sha256(runtime_identity)
    if writer_identity.get("runtime_identity_sha256") != expected_runtime_hash:
        raise ValueError("writer runtime identity canonical hash mismatch")
    if runtime_identity.get("baseline_epoch_id") != session_id:
        raise ValueError("writer runtime identity disagrees with session id")
    if runtime_identity.get("baseline_epoch_identity_sha256") != expected_epoch_identity:
        raise ValueError("writer and epoch canonical identities disagree")
    if runtime_identity.get("storage_profile") != BOUNDED_REMOTE_SPOOL:
        raise ValueError("writer identity is not a bounded remote spool")
    if bool(runtime_identity.get("local_admission_complete")):
        raise ValueError("writer runtime identity claims local admission")
    runtime_bounds = runtime_identity.get("collection_bounds")
    if not isinstance(runtime_bounds, Mapping) or dict(runtime_bounds) != dict(epoch_bounds):
        raise ValueError("writer and epoch collection bounds disagree")
    for field in REQUIRED_IDENTITY_FIELDS:
        if runtime_identity.get(field) != identity[field]:
            raise ValueError(f"writer and epoch identity differ at {field}")
    if bool(writer_identity.get("economic_outcomes_read")):
        raise ValueError("writer identity claims economic outcome access")
    if bool(writer_identity.get("q90_action_authorized")):
        raise ValueError("writer identity claims q90 action authority")
    return dict(identity), dict(runtime_identity)


def _validate_part_cursor_summary(
    *,
    session: Path,
    writer_identity: Mapping[str, Any],
    core_health: Mapping[str, Any],
) -> dict[str, Any]:
    parts_root = session / "parts"
    cursors_root = session / "cursors"
    if not parts_root.is_dir() or parts_root.is_symlink():
        raise ValueError("remote spool parts directory is missing or unsafe")
    if not cursors_root.is_dir() or cursors_root.is_symlink():
        raise ValueError("remote spool cursor directory is missing or unsafe")
    storage_format = str(writer_identity.get("storage_format", ""))
    if storage_format not in {"parquet", "jsonl"}:
        raise ValueError("writer storage format is unsupported")
    suffix = ".parquet" if storage_format == "parquet" else ".jsonl"
    manifests = sorted(parts_root.glob("part-*.manifest.json"))
    data_files = sorted(parts_root.glob(f"part-*{suffix}"))
    if not manifests or not data_files:
        raise ValueError("remote spool has no committed journal parts")
    records: list[dict[str, Any]] = []
    referenced_data: set[Path] = set()
    total_rows = 0
    by_lifecycle: dict[str, list[dict[str, Any]]] = {}
    for manifest_path in manifests:
        if manifest_path.is_symlink():
            raise ValueError("journal part manifest must not be a symlink")
        record = _read_json(manifest_path)
        batch_id = _require_sha256(record.get("batch_id"), field_name="journal batch id")
        if manifest_path.name != f"part-{batch_id}.manifest.json":
            raise ValueError("journal part manifest filename mismatch")
        data_path = parts_root / str(record.get("data_file", ""))
        if data_path != parts_root / f"part-{batch_id}{suffix}":
            raise ValueError("journal part data filename mismatch")
        if not data_path.is_file() or data_path.is_symlink():
            raise ValueError("journal part data file is missing or unsafe")
        _require_sha256(record.get("data_sha256"), field_name="journal data SHA256")
        row_count = int(record.get("row_count", 0))
        if row_count <= 0:
            raise ValueError("journal part row count must be positive")
        if bool(record.get("economic_outcomes_read")):
            raise ValueError("journal part claims economic outcome access")
        first = int(record.get("first_lifecycle_sequence", 0))
        last = int(record.get("last_lifecycle_sequence", 0))
        event_ids = record.get("event_ids")
        if not isinstance(event_ids, list) or len(event_ids) != row_count:
            raise ValueError("journal part event identity count mismatch")
        if first <= 0 or last != first + row_count - 1:
            raise ValueError("journal part lifecycle sequence range mismatch")
        lifecycle_id = str(record.get("lifecycle_id", ""))
        client_order_id = str(record.get("client_order_id", ""))
        before = record.get("checkpoint_before")
        after = record.get("checkpoint_after")
        if not lifecycle_id or not client_order_id or not isinstance(before, Mapping) or not isinstance(after, Mapping):
            raise ValueError("journal part lifecycle or checkpoint identity is invalid")
        records.append(record)
        referenced_data.add(data_path)
        total_rows += row_count
        by_lifecycle.setdefault(lifecycle_id, []).append(record)
    if referenced_data != set(data_files):
        raise ValueError("journal part manifests and data files are not one-to-one")
    if total_rows != int(core_health.get("rows_committed", -1)):
        raise ValueError("part manifest rows disagree with core health")
    if len(records) != int(core_health.get("callbacks_committed", -1)):
        raise ValueError("part manifest count disagrees with core health")

    expected_cursors: dict[str, Mapping[str, Any]] = {}
    event_ids_seen: set[str] = set()
    for lifecycle_id, lifecycle_records in by_lifecycle.items():
        lifecycle_records.sort(key=lambda item: int(item["first_lifecycle_sequence"]))
        expected_sequence = 1
        expected_prior_event = ""
        expected_client = str(lifecycle_records[0]["client_order_id"])
        for record in lifecycle_records:
            if str(record["client_order_id"]) != expected_client:
                raise ValueError("journal lifecycle client identity changed")
            before = record["checkpoint_before"]
            after = record["checkpoint_after"]
            if (
                before.get("lifecycle_id") != lifecycle_id
                or before.get("client_order_id") != expected_client
                or int(before.get("last_emitted_sequence", -1)) != expected_sequence - 1
                or str(before.get("last_event_id", "")) != expected_prior_event
            ):
                raise ValueError("journal lifecycle checkpoint chain is broken")
            if int(record["first_lifecycle_sequence"]) != expected_sequence:
                raise ValueError("journal lifecycle sequence has a gap or overlap")
            part_event_ids = [str(value) for value in record["event_ids"]]
            if event_ids_seen.intersection(part_event_ids):
                raise ValueError("journal event id is duplicated across parts")
            event_ids_seen.update(part_event_ids)
            if (
                after.get("lifecycle_id") != lifecycle_id
                or after.get("client_order_id") != expected_client
                or int(after.get("last_emitted_sequence", -1))
                != int(record["last_lifecycle_sequence"])
                or str(after.get("last_event_id", "")) != str(record.get("last_event_id", ""))
            ):
                raise ValueError("journal checkpoint-after disagrees with part")
            expected_sequence = int(after["last_emitted_sequence"]) + 1
            expected_prior_event = str(after["last_event_id"])
        expected_cursors[lifecycle_id] = lifecycle_records[-1]["checkpoint_after"]

    cursor_files = sorted(cursors_root.glob("cursor-*.json"))
    if len(cursor_files) != int(core_health.get("durable_cursor_count", -1)):
        raise ValueError("durable cursor count disagrees with core health")
    observed_cursors: dict[str, Mapping[str, Any]] = {}
    for cursor_path in cursor_files:
        if cursor_path.is_symlink():
            raise ValueError("durable cursor must not be a symlink")
        cursor = _read_json(cursor_path)
        lifecycle_id = str(cursor.get("lifecycle_id", ""))
        expected_name = f"cursor-{hashlib.sha256(lifecycle_id.encode()).hexdigest()}.json"
        if not lifecycle_id or cursor_path.name != expected_name:
            raise ValueError("durable cursor filename mismatch")
        if lifecycle_id in observed_cursors:
            raise ValueError("duplicate durable lifecycle cursor")
        observed_cursors[lifecycle_id] = cursor
    if observed_cursors != expected_cursors:
        raise ValueError("durable cursors do not end at immutable part boundaries")
    return {
        "storage_format": storage_format,
        "part_count": len(records),
        "cursor_count": len(observed_cursors),
        "row_count": total_rows,
        "lifecycle_count": len(by_lifecycle),
        "event_id_count": len(event_ids_seen),
    }


def _source_payload_files(*, session: Path, epoch: Path) -> tuple[Path, ...]:
    session_excluded = {
        (session / name).resolve() for name in (_MUTABLE_SESSION_FILES | _CONTROL_SESSION_FILES)
    }
    session_files = _files_under(
        session,
        excluded=session_excluded,
        excluded_directory_names={REMOTE_SPOOL_SEAL_DIRECTORY},
    )
    epoch_files = _files_under(epoch, excluded=set())
    files = tuple(sorted((*session_files, *epoch_files)))
    if len(set(files)) != len(files):
        raise ValueError("session and epoch payload trees overlap")
    return files


def _capture_remote_state(
    *,
    session: Path,
    epoch: Path,
    allowlisted_root: Path,
) -> dict[str, Any]:
    live_health = _read_json(session / "live_health.json")
    core_health = _read_json(session / "health.json")
    writer_identity = _read_json(session / "runtime_identity.json")
    epoch_manifest = _read_json(epoch / "epoch_manifest.json")
    health_projection = _validate_health(
        live_health=live_health,
        core_health=core_health,
        session_id=session.name.removeprefix("session-"),
    )
    epoch_identity, runtime_identity = _validate_epoch_and_writer_identity(
        epoch_manifest=epoch_manifest,
        writer_identity=writer_identity,
        session_id=session.name.removeprefix("session-"),
        live_health=live_health,
    )
    part_summary = _validate_part_cursor_summary(
        session=session,
        writer_identity=writer_identity,
        core_health=core_health,
    )
    files = _source_payload_files(session=session, epoch=epoch)
    records = tuple(
        _file_record(path, allowlisted_root=allowlisted_root) for path in files
    )
    if len({record["path"] for record in records}) != len(records):
        raise ValueError("remote spool transfer paths are not unique")
    total_bytes = sum(int(record["bytes"]) for record in records)
    max_bytes = int(live_health["session_max_bytes"])
    if total_bytes > max_bytes:
        raise ValueError("remote spool payload exceeds its frozen byte bound")
    duration_s = float(live_health.get("collection_duration_s", -1.0))
    max_duration_s = float(live_health["session_max_duration_s"])
    if duration_s < 0.0 or duration_s > max_duration_s:
        raise ValueError("remote spool session duration exceeded its frozen bound")
    return {
        "live_health": live_health,
        "core_health": core_health,
        "writer_identity": writer_identity,
        "epoch_manifest": epoch_manifest,
        "health_projection": health_projection,
        "epoch_identity": epoch_identity,
        "runtime_identity": runtime_identity,
        "part_summary": part_summary,
        "records": records,
        "payload_bytes": total_bytes,
        "collection_duration_s": duration_s,
        "session_max_duration_s": max_duration_s,
        "session_max_bytes": max_bytes,
    }


def _capture_stable_remote_state(
    *,
    session: Path,
    epoch: Path,
    allowlisted_root: Path,
    stability_interval_s: float,
) -> dict[str, Any]:
    interval = float(stability_interval_s)
    if interval < 0.0 or interval > 30.0:
        raise ValueError("stability interval must be between 0 and 30 seconds")
    first = _capture_remote_state(
        session=session,
        epoch=epoch,
        allowlisted_root=allowlisted_root,
    )
    if interval:
        time.sleep(interval)
    second = _capture_remote_state(
        session=session,
        epoch=epoch,
        allowlisted_root=allowlisted_root,
    )
    if _stable_health_projection(
        first["live_health"], first["core_health"]
    ) != _stable_health_projection(second["live_health"], second["core_health"]):
        raise ValueError("remote spool changed during stability check: health_projection")
    for field in (
        "epoch_identity",
        "runtime_identity",
        "part_summary",
        "records",
        "payload_bytes",
        "collection_duration_s",
        "session_max_duration_s",
        "session_max_bytes",
    ):
        if first[field] != second[field]:
            raise ValueError(f"remote spool changed during stability check: {field}")
    return second


def inspect_bounded_remote_spool(
    *,
    session_root: str | Path,
    epoch_root: str | Path,
    allowlisted_roots: Sequence[str | Path],
    stability_interval_s: float = 0.25,
) -> dict[str, Any]:
    """Inspect one drained bounded session without publishing a remote seal."""

    session, session_allowlist = validate_remote_spool_path(
        session_root,
        allowlisted_roots=allowlisted_roots,
        field_name="session_root",
    )
    epoch, epoch_allowlist = validate_remote_spool_path(
        epoch_root,
        allowlisted_roots=allowlisted_roots,
        field_name="epoch_root",
    )
    if session_allowlist != epoch_allowlist:
        raise ValueError("session and epoch roots do not share an allowlisted root")
    if session_allowlist.is_symlink() or session.is_symlink() or epoch.is_symlink():
        raise ValueError("remote spool roots must not be symlinks")
    if not session.is_dir() or not epoch.is_dir():
        raise ValueError("session and epoch roots must already exist as directories")
    if not session.name.startswith("session-"):
        raise ValueError("remote spool session directory must use session-<epoch_id>")
    session_id = session.name.removeprefix("session-")
    if not session_id or epoch.name != session_id:
        raise ValueError("remote spool session and epoch identities do not match")

    state = _capture_stable_remote_state(
        session=session,
        epoch=epoch,
        allowlisted_root=session_allowlist,
        stability_interval_s=stability_interval_s,
    )
    records = list(state["records"])
    return {
        "schema_version": REMOTE_SPOOL_TRANSFER_MANIFEST_VERSION,
        "created_ts_ns": time.time_ns(),
        "storage_profile": BOUNDED_REMOTE_SPOOL,
        "session_id": session_id,
        "baseline_epoch_id": session_id,
        "allowlisted_root": str(session_allowlist),
        "session_root": str(session),
        "epoch_root": str(epoch),
        "collection_state": state["health_projection"]["live"]["state"],
        "collection_bound_reached": bool(
            state["live_health"].get("collection_bound_reached")
        ),
        "collection_duration_s": state["collection_duration_s"],
        "session_max_duration_s": state["session_max_duration_s"],
        "payload_bytes": state["payload_bytes"],
        "session_max_bytes": state["session_max_bytes"],
        "file_count": len(records),
        "files": records,
        "rsync_files_from": [record["path"] for record in records],
        "health_projection": state["health_projection"],
        "part_cursor_summary": state["part_summary"],
        "epoch_identity_sha256": state["epoch_manifest"]["identity_sha256"],
        "stable_double_read_passed": True,
        "transfer_executed": False,
        "local_orico_admission_complete": False,
        "formal_collection_valid": False,
        "seven_tape_capture_contract_modified": False,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }


def publish_bounded_remote_spool_manifest(
    *,
    session_root: str | Path,
    epoch_root: str | Path,
    allowlisted_roots: Sequence[str | Path],
    stability_interval_s: float = 0.25,
) -> Path:
    """Legacy manifest publication; prefer :func:`seal_bounded_remote_spool`."""

    payload = inspect_bounded_remote_spool(
        session_root=session_root,
        epoch_root=epoch_root,
        allowlisted_roots=allowlisted_roots,
        stability_interval_s=stability_interval_s,
    )
    output = Path(session_root).expanduser().resolve() / REMOTE_SPOOL_TRANSFER_MANIFEST_NAME
    _atomic_write_json(output, payload)
    return output


def seal_bounded_remote_spool(
    *,
    session_root: str | Path,
    epoch_root: str | Path,
    allowlisted_roots: Sequence[str | Path],
    stability_interval_s: float = 0.25,
) -> dict[str, Any]:
    """Publish an immutable health snapshot and content-addressed session seal."""

    session, session_allowlist = validate_remote_spool_path(
        session_root,
        allowlisted_roots=allowlisted_roots,
        field_name="session_root",
    )
    epoch, epoch_allowlist = validate_remote_spool_path(
        epoch_root,
        allowlisted_roots=allowlisted_roots,
        field_name="epoch_root",
    )
    if session_allowlist != epoch_allowlist:
        raise ValueError("session and epoch roots do not share an allowlisted root")
    if session_allowlist.is_symlink() or session.is_symlink() or epoch.is_symlink():
        raise ValueError("remote spool roots must not be symlinks")
    if not session.is_dir() or not epoch.is_dir():
        raise ValueError("session and epoch roots must already exist as directories")
    if not session.name.startswith("session-"):
        raise ValueError("remote spool session directory must use session-<epoch_id>")
    session_id = session.name.removeprefix("session-")
    if not session_id or epoch.name != session_id:
        raise ValueError("remote spool session and epoch identities do not match")

    state = _capture_stable_remote_state(
        session=session,
        epoch=epoch,
        allowlisted_root=session_allowlist,
        stability_interval_s=stability_interval_s,
    )
    if not bool(state["live_health"].get("collection_bound_reached")):
        raise ValueError("remote spool seal requires a completed collection bound")
    created_ts_ns = time.time_ns()
    seal_nonce = f"{created_ts_ns}-{uuid.uuid4().hex}"
    seals_root = session_allowlist / REMOTE_SPOOL_SEAL_DIRECTORY / session_id
    seals_root.mkdir(parents=True, exist_ok=True)
    if seals_root.is_symlink():
        raise ValueError("remote spool seals directory must not be a symlink")
    temporary = seals_root / f".{seal_nonce}.partial-{os.getpid()}"
    final = seals_root / seal_nonce
    temporary.mkdir()
    try:
        health_snapshot_path = temporary / REMOTE_SPOOL_HEALTH_SNAPSHOT_NAME
        health_snapshot = {
            "schema_version": REMOTE_SPOOL_HEALTH_SNAPSHOT_VERSION,
            "sealed_ts_ns": created_ts_ns,
            "session_id": session_id,
            "baseline_epoch_id": session_id,
            "live_health_source_path": _relative_path(
                session / "live_health.json", session_allowlist
            ),
            "core_health_source_path": _relative_path(
                session / "health.json", session_allowlist
            ),
            "live_health": state["live_health"],
            "core_health": state["core_health"],
            "health_projection": state["health_projection"],
            "part_cursor_summary": state["part_summary"],
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_policy_authorized": False,
        }
        health_snapshot["snapshot_identity_sha256"] = _canonical_sha256(
            health_snapshot
        )
        _atomic_write_json(health_snapshot_path, health_snapshot)

        final_health_path = final / REMOTE_SPOOL_HEALTH_SNAPSHOT_NAME
        health_record = {
            "path": _relative_path(final_health_path, session_allowlist),
            "bytes": health_snapshot_path.stat().st_size,
            "sha256": _sha256_file(health_snapshot_path),
        }
        payload_records = [*state["records"], health_record]
        payload_records.sort(key=lambda item: str(item["path"]))
        seal_path = final / REMOTE_SPOOL_SEAL_NAME
        seal_relative_path = _relative_path(seal_path, session_allowlist)
        seal = {
            "schema_version": REMOTE_SPOOL_SEAL_VERSION,
            "sealed_ts_ns": created_ts_ns,
            "seal_nonce": seal_nonce,
            "storage_profile": BOUNDED_REMOTE_SPOOL,
            "allowlisted_root": str(session_allowlist),
            "session_id": session_id,
            "baseline_epoch_id": session_id,
            "session_root_relative": _relative_path(session, session_allowlist),
            "epoch_root_relative": _relative_path(epoch, session_allowlist),
            "seal_relative_path": seal_relative_path,
            "health_snapshot_relative_path": health_record["path"],
            "collection_state": state["health_projection"]["live"]["state"],
            "collection_bound_reached": True,
            "collection_duration_s": state["collection_duration_s"],
            "session_max_duration_s": state["session_max_duration_s"],
            "session_max_bytes": state["session_max_bytes"],
            "epoch_identity_sha256": state["epoch_manifest"]["identity_sha256"],
            "runtime_identity_sha256": state["writer_identity"][
                "runtime_identity_sha256"
            ],
            "part_cursor_summary": state["part_summary"],
            "source_payload_file_count": len(state["records"]),
            "file_count": len(payload_records),
            "payload_bytes": sum(int(record["bytes"]) for record in payload_records),
            "files": payload_records,
            "rsync_files_from": [
                *[str(record["path"]) for record in payload_records],
                seal_relative_path,
            ],
            "excluded_remote_control_paths": [
                _relative_path(session / "writer.lock", session_allowlist),
                _relative_path(session / "health.json", session_allowlist),
                _relative_path(session / "live_health.json", session_allowlist),
                _relative_path(
                    session / REMOTE_SPOOL_TRANSFER_MANIFEST_NAME,
                    session_allowlist,
                ),
                _relative_path(seals_root, session_allowlist) + "/<prior-seals>",
            ],
            "raw_health_transferred": False,
            "writer_lock_transferred": False,
            "stable_double_read_passed": True,
            "transfer_executed": False,
            "local_orico_admission_complete": False,
            "remote_payload_deleted": False,
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_policy_authorized": False,
        }
        seal["seal_identity_sha256"] = _canonical_sha256(seal)
        temporary_seal_path = temporary / REMOTE_SPOOL_SEAL_NAME
        _atomic_write_json(temporary_seal_path, seal)
        _fsync_directory(temporary)

        current = _capture_remote_state(
            session=session,
            epoch=epoch,
            allowlisted_root=session_allowlist,
        )
        if _stable_health_projection(
            current["live_health"], current["core_health"]
        ) != _stable_health_projection(state["live_health"], state["core_health"]):
            raise ValueError("remote spool changed before seal publication: health_projection")
        for field in (
            "epoch_identity",
            "runtime_identity",
            "part_summary",
            "records",
            "payload_bytes",
        ):
            if current[field] != state[field]:
                raise ValueError(f"remote spool changed before seal publication: {field}")
        os.replace(temporary, final)
        _fsync_directory(seals_root)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise

    published_seal_path = final / REMOTE_SPOOL_SEAL_NAME
    return {
        "schema_version": "order_lifecycle_remote_spool_seal_publication.v2",
        "session_id": session_id,
        "baseline_epoch_id": session_id,
        "allowlisted_root": str(session_allowlist),
        "seal_path": str(published_seal_path),
        "seal_relative_path": seal["seal_relative_path"],
        "seal_bytes": published_seal_path.stat().st_size,
        "seal_sha256": _sha256_file(published_seal_path),
        "seal_identity_sha256": seal["seal_identity_sha256"],
        "file_count": seal["file_count"],
        "payload_bytes": seal["payload_bytes"],
        "rsync_files_from": seal["rsync_files_from"],
        "transfer_executed": False,
        "remote_payload_deleted": False,
        "economic_outcomes_read": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inspect", "seal"))
    parser.add_argument("--session-root", required=True)
    parser.add_argument("--epoch-root", required=True)
    parser.add_argument("--allowlisted-root", action="append", required=True)
    parser.add_argument("--stability-interval-s", type=float, default=0.25)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    kwargs = {
        "session_root": args.session_root,
        "epoch_root": args.epoch_root,
        "allowlisted_roots": tuple(args.allowlisted_root),
        "stability_interval_s": args.stability_interval_s,
    }
    if args.mode == "inspect":
        payload = inspect_bounded_remote_spool(**kwargs)
    else:
        payload = seal_bounded_remote_spool(**kwargs)
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
