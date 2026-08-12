#!/usr/bin/env python3
"""Validate and atomically admit F04 exact-opportunity v2.2 chunks."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import math
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_paths import data_root, resolve_portable_path
from execution.exact_opportunity_tape import (
    EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION,
    ExactQuoteOpportunityTapeRow,
)
from execution.exact_opportunity_tape_runtime import (
    EXACT_OPPORTUNITY_CHUNK_MANIFEST_SCHEMA_VERSION,
    canonical_sha256,
    copy_file_fsync,
    exact_tape_schema_sha256,
    sha256_file,
)

ADMISSION_MANIFEST_SCHEMA_VERSION = "exact_opportunity_admission_manifest.v2.2"
ADMISSION_ROW_SCHEMA_VERSION = "exact_opportunity_admission_row.v2.2"

_INT_FIELDS = frozenset(
    {
        "event_ts_ns",
        "exchange_ts_ns",
        "visibility_ts_ns",
        "decision_start_ts_ns",
        "feature_ready_ts_ns",
        "exposure_increasing",
        "baseline_eligible",
        "guard_valid",
        "requested_outward_ticks",
        "effective_outward_ticks",
        "queue_reset",
        "lifecycle_sequence",
    }
)
_FLOAT_FIELDS = frozenset(
    {
        "signed_inventory_before",
        "baseline_quote_price",
        "candidate_quote_price",
        "order_quantity",
        "remaining_quantity",
        "fill_quantity",
        "fill_price",
    }
)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    with temporary.open("w", encoding="utf-8") as handle:
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
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _typed_row(row: Mapping[str, str]) -> dict[str, Any]:
    typed: dict[str, Any] = {}
    for field in ExactQuoteOpportunityTapeRow.__dataclass_fields__:
        value = row[field]
        if field in _INT_FIELDS:
            typed[field] = int(value)
        elif field in _FLOAT_FIELDS:
            parsed = float(value)
            if not math.isfinite(parsed):
                raise ValueError(f"non-finite exact tape value: {field}")
            typed[field] = parsed
        else:
            typed[field] = str(value)
    return typed


def _event_day(event_ts_ns: int) -> str:
    return datetime.fromtimestamp(
        int(event_ts_ns) / 1_000_000_000.0,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d")


def validate_ready_chunk(manifest_path: str | Path) -> dict[str, Any]:
    """Validate one complete chunk without joining any economic outcome."""

    manifest_file = Path(manifest_path).expanduser().resolve()
    if not manifest_file.name.endswith(".ready.manifest.json"):
        raise ValueError("only a ready exact-opportunity manifest is admissible")
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != (
        EXACT_OPPORTUNITY_CHUNK_MANIFEST_SCHEMA_VERSION
    ):
        raise ValueError("unexpected exact-opportunity chunk manifest schema")
    if not bool(manifest.get("complete")) or not bool(manifest.get("valid")):
        raise ValueError("exact-opportunity chunk is not complete and valid")
    if int(manifest.get("rows_dropped", -1)) != 0:
        raise ValueError("exact-opportunity chunk contains writer drops")
    if int(manifest.get("error_count", -1)) != 0:
        raise ValueError("exact-opportunity chunk contains writer errors")
    if str(manifest.get("schema_sha256", "")) != exact_tape_schema_sha256():
        raise ValueError("exact-opportunity chunk schema hash mismatch")

    file_name = str(manifest.get("file_name", ""))
    if not file_name or Path(file_name).name != file_name:
        raise ValueError("unsafe exact-opportunity chunk file name")
    data_path = manifest_file.parent / file_name
    if not data_path.is_file() or data_path.suffix != ".csv":
        raise ValueError("exact-opportunity ready CSV is missing")
    file_sha256 = sha256_file(data_path)
    if file_sha256 != str(manifest.get("file_sha256", "")):
        raise ValueError("exact-opportunity ready CSV hash mismatch")
    if data_path.stat().st_size != int(manifest.get("file_bytes", -1)):
        raise ValueError("exact-opportunity ready CSV byte count mismatch")

    identity_relative = str(manifest.get("runtime_identity_file", ""))
    identity_path = (manifest_file.parent / identity_relative).resolve()
    session_root = manifest_file.parent.parent.resolve()
    if identity_path.parent != session_root:
        raise ValueError("runtime identity escaped the exact-opportunity session")
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    identity_sha256 = str(identity.get("runtime_identity_sha256", ""))
    normalized_identity = dict(identity)
    normalized_identity.pop("runtime_identity_sha256", None)
    if canonical_sha256(normalized_identity) != identity_sha256:
        raise ValueError("runtime identity canonical hash mismatch")
    if identity_sha256 != str(manifest.get("runtime_identity_sha256", "")):
        raise ValueError("chunk and runtime identity disagree")

    expected_columns = list(ExactQuoteOpportunityTapeRow.__dataclass_fields__)
    row_digest = hashlib.sha256()
    row_count = 0
    first_event_ts_ns = 0
    last_event_ts_ns = 0
    utc_day = str(manifest.get("utc_day", ""))
    with data_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != expected_columns:
            raise ValueError("exact-opportunity CSV header drifted")
        for raw_row in reader:
            row = _typed_row(raw_row)
            if row["schema_version"] != EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION:
                raise ValueError("exact-opportunity row schema version drifted")
            if _event_day(row["event_ts_ns"]) != utc_day:
                raise ValueError("exact-opportunity row crossed its UTC day chunk")
            event_ts_ns = int(row["event_ts_ns"])
            if first_event_ts_ns == 0 or event_ts_ns < first_event_ts_ns:
                first_event_ts_ns = event_ts_ns
            last_event_ts_ns = max(last_event_ts_ns, event_ts_ns)
            row_digest.update(
                json.dumps(
                    row,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("utf-8")
            )
            row_count += 1
    if row_count != int(manifest.get("row_count", -1)):
        raise ValueError("exact-opportunity row count mismatch")
    if row_digest.hexdigest() != str(manifest.get("row_sha256", "")):
        raise ValueError("exact-opportunity canonical row hash mismatch")
    if first_event_ts_ns != int(manifest.get("first_event_ts_ns", 0)):
        raise ValueError("exact-opportunity first event timestamp mismatch")
    if last_event_ts_ns != int(manifest.get("last_event_ts_ns", 0)):
        raise ValueError("exact-opportunity last event timestamp mismatch")
    expected_chunk_id = f"{utc_day}:{manifest.get('session_id', '')}"
    if str(manifest.get("chunk_id", "")) != expected_chunk_id:
        raise ValueError("exact-opportunity chunk identity mismatch")
    return {
        "schema_version": "exact_opportunity_ready_chunk_validation.v2.2",
        "valid": True,
        "economic_outcomes_read": False,
        "operational_lifecycle_outcomes_read": True,
        "chunk_id": expected_chunk_id,
        "utc_day": utc_day,
        "session_id": str(manifest["session_id"]),
        "row_count": row_count,
        "first_event_ts_ns": first_event_ts_ns,
        "last_event_ts_ns": last_event_ts_ns,
        "row_sha256": row_digest.hexdigest(),
        "schema_sha256": exact_tape_schema_sha256(),
        "file_sha256": file_sha256,
        "file_bytes": data_path.stat().st_size,
        "data_path": str(data_path),
        "chunk_manifest_path": str(manifest_file),
        "chunk_manifest_sha256": sha256_file(manifest_file),
        "runtime_identity_sha256": identity_sha256,
        "runtime_identity_path": str(identity_path),
        "runtime_identity_file_sha256": sha256_file(identity_path),
    }


def scan_staging(staging_root: str | Path) -> dict[str, Any]:
    """Report ready, invalid, and crash-left partial chunks without mutation."""

    root = Path(staging_root).expanduser().resolve()
    return {
        "schema_version": "exact_opportunity_staging_scan.v2.2",
        "staging_root": str(root),
        "ready_manifests": sorted(
            str(path) for path in root.glob("session-*/utc_day=*/*.ready.manifest.json")
        ),
        "invalid_manifests": sorted(
            str(path) for path in root.glob("session-*/utc_day=*/*.invalid.manifest.json")
        ),
        "orphan_partials": sorted(
            str(path) for path in root.glob("session-*/utc_day=*/*.partial")
        ),
    }


def admit_ready_chunk(
    manifest_path: str | Path,
    destination_root: str | Path,
    *,
    require_orico: bool = True,
) -> dict[str, Any]:
    """Atomically admit one immutable chunk; repeated calls are idempotent."""

    validation = validate_ready_chunk(manifest_path)
    destination = Path(destination_root).expanduser().resolve()
    if require_orico:
        authoritative_root = data_root().resolve()
        try:
            destination.relative_to(authoritative_root)
        except ValueError as exc:
            raise ValueError(
                "formal exact-opportunity admission must target the configured data root"
            ) from exc
        storage_mount = resolve_portable_path("${NARROWGATE_STORAGE_ROOT}").resolve()
        if not storage_mount.is_mount():
            raise ValueError(
                "configured storage root is not mounted; formal admission is fail-closed"
            )
    destination.mkdir(parents=True, exist_ok=True)
    lock_path = destination / ".admission.lock"
    lock_path.touch(exist_ok=True)
    with lock_path.open("r+") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        manifest_registry = destination / "admission_manifest.json"
        if manifest_registry.exists():
            registry = json.loads(manifest_registry.read_text(encoding="utf-8"))
        else:
            registry = {
                "schema_version": ADMISSION_MANIFEST_SCHEMA_VERSION,
                "rows": [],
            }
        if registry.get("schema_version") != ADMISSION_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unexpected exact-opportunity admission manifest schema")
        existing = {
            str(row["chunk_id"]): row for row in registry.get("rows", [])
        }
        if validation["chunk_id"] in existing:
            row = existing[validation["chunk_id"]]
            if row.get("file_sha256") != validation["file_sha256"]:
                raise ValueError("admission chunk identity collided with new bytes")
            admitted_path = destination / str(row["relative_path"])
            if not admitted_path.is_file() or sha256_file(admitted_path) != row[
                "file_sha256"
            ]:
                raise ValueError("previously admitted exact-opportunity file drifted")
            return {**row, "idempotent_replay": True}

        for previous in registry.get("rows", []):
            if previous.get("utc_day") != validation["utc_day"]:
                continue
            separated = (
                int(validation["last_event_ts_ns"])
                < int(previous["first_event_ts_ns"])
                or int(validation["first_event_ts_ns"])
                > int(previous["last_event_ts_ns"])
            )
            if not separated:
                raise ValueError(
                    "exact-opportunity chunks overlap; half-window/session "
                    "splicing is forbidden"
                )

        day_root = destination / f"utc_day={validation['utc_day']}"
        day_root.mkdir(parents=True, exist_ok=True)
        target_name = (
            f"exact-opportunity-{validation['utc_day']}-"
            f"{validation['session_id']}-{validation['file_sha256'][:16]}.csv"
        )
        target = day_root / target_name
        partial = day_root / f".{target_name}.{os.getpid()}.partial"
        source = Path(validation["data_path"])
        if target.exists():
            if sha256_file(target) != validation["file_sha256"]:
                raise ValueError("admission destination exists with different bytes")
        else:
            if partial.exists():
                partial.unlink()
            copy_file_fsync(source, partial)
            if sha256_file(partial) != validation["file_sha256"]:
                partial.unlink(missing_ok=True)
                raise ValueError("admission copy hash mismatch")
            os.replace(partial, target)
            descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        sidecar = target.with_suffix(".manifest.json")
        sidecar_payload = {
            "schema_version": ADMISSION_ROW_SCHEMA_VERSION,
            **{
                key: validation[key]
                for key in (
                    "chunk_id",
                    "utc_day",
                    "session_id",
                    "row_count",
                    "first_event_ts_ns",
                    "last_event_ts_ns",
                    "row_sha256",
                    "schema_sha256",
                    "file_sha256",
                    "file_bytes",
                    "chunk_manifest_sha256",
                    "runtime_identity_sha256",
                    "runtime_identity_file_sha256",
                )
            },
            "relative_path": str(target.relative_to(destination)),
            "integrity_valid": True,
            "economic_outcomes_read": False,
            "admitted_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_json(sidecar, sidecar_payload)
        row = dict(sidecar_payload)
        row["sidecar_sha256"] = sha256_file(sidecar)
        registry["rows"].append(row)
        registry["rows"] = sorted(
            registry["rows"],
            key=lambda value: (value["utc_day"], value["session_id"]),
        )
        registry["row_count"] = len(registry["rows"])
        registry["manifest_rows_sha256"] = canonical_sha256(registry["rows"])
        _atomic_json(manifest_registry, registry)
        return {**row, "idempotent_replay": False}
