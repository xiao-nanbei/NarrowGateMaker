#!/usr/bin/env python3
"""Finalize, transfer, and validate bounded receive-time captures.

The workflow is intentionally two-stage:

1. The current deployment host records a bounded window and writes a
   host-bound, self-contained remote summary.
2. A later task transfers all seven tapes in one rsync session, validates the
   local copy, appends the ledger atomically, and only then removes remote
   payloads.

Neither stage changes strategy parameters or enables a strategy action.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402
from scripts.live_remote_pointer import (  # noqa: E402
    active_live_remote_fields,
    require_remote_matches_source,
)

REMOTE_SUMMARY_SCHEMA = "bounded_receive_time_capture.v3"
LOCAL_VALIDATION_SCHEMA = "bounded_receive_time_capture_local_validation.v3"
LEDGER_SCHEMA = "bounded_receive_time_capture_ledger.v2"
SOURCE_IDENTITY_SCHEMA = "bounded_receive_time_capture_source.v1"
EXPECTED_TAPE_COUNT = 7
SAFE_REMOTE_PREFIXES = ("logs/market_tape/", "logs/external_venues/")
CAPTURE_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")
DEFAULT_LOCAL_ROOT = data_root(ROOT) / "receive_time_tape"
LEGACY_LEDGER_FILENAME = "capture_ledger.v1.jsonl"
CURRENT_LEDGER_FILENAME = "capture_ledger.v2.jsonl"
LEGACY_AWS_SOURCE_KEY = "legacy_aws_tokyo_v1_unbound"
_ACTIVE_REMOTE = active_live_remote_fields(ROOT)
DEFAULT_REMOTE = _ACTIVE_REMOTE.get("ssh_target", "")
DEFAULT_REMOTE_ROOT = os.environ.get(
    "NARROWGATE_REMOTE_ROOT",
    _ACTIVE_REMOTE.get("repo_root", str(Path.home() / ROOT.name)),
)
DEFAULT_SOURCE_PROVIDER = os.environ.get(
    "NARROWGATE_CAPTURE_SOURCE_PROVIDER", _ACTIVE_REMOTE.get("provider", "")
)
DEFAULT_SOURCE_REGION = os.environ.get(
    "NARROWGATE_CAPTURE_SOURCE_REGION", _ACTIVE_REMOTE.get("region", "")
)
DEFAULT_SOURCE_CITY = os.environ.get(
    "NARROWGATE_CAPTURE_SOURCE_CITY", _ACTIVE_REMOTE.get("city", "")
)
DEFAULT_SOURCE_PUBLIC_IPV4 = os.environ.get(
    "NARROWGATE_CAPTURE_SOURCE_PUBLIC_IPV4",
    _ACTIVE_REMOTE.get("public_ipv4", ""),
)
DEFAULT_SOURCE_SSH_TARGET = os.environ.get(
    "NARROWGATE_CAPTURE_SOURCE_SSH_TARGET",
    _ACTIVE_REMOTE.get("ssh_target", ""),
)


def _source_component(value: str, *, field: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if not normalized:
        raise ValueError(f"capture source {field} is required")
    return normalized


def capture_source_identity(
    *,
    provider: str,
    region: str,
    city: str,
    public_ipv4: str,
    ssh_target: str,
) -> dict[str, str]:
    provider_key = _source_component(provider, field="provider")
    region_key = _source_component(region, field="region")
    city_key = _source_component(city, field="city")
    ipv4 = public_ipv4.strip()
    target = ssh_target.strip()
    if not ipv4 or not target:
        raise ValueError("capture source public_ipv4 and ssh_target are required")
    host_key = _source_component(ipv4, field="public_ipv4")
    return {
        "schema_version": SOURCE_IDENTITY_SCHEMA,
        "provider": provider_key,
        "region": region_key,
        "city": city_key,
        "public_ipv4": ipv4,
        "ssh_target": target,
        "source_key": f"{provider_key}:{region_key}:{ipv4}",
        "storage_prefix": f"{provider_key}_{city_key}_{host_key}",
    }


def _validated_source_identity(value: Mapping[str, Any]) -> dict[str, str]:
    identity = capture_source_identity(
        provider=str(value.get("provider", "")),
        region=str(value.get("region", "")),
        city=str(value.get("city", "")),
        public_ipv4=str(value.get("public_ipv4", "")),
        ssh_target=str(value.get("ssh_target", "")),
    )
    if str(value.get("schema_version", "")) != SOURCE_IDENTITY_SCHEMA:
        raise ValueError("unsupported or missing capture source identity schema")
    for field in ("source_key", "storage_prefix"):
        if str(value.get(field, "")) != identity[field]:
            raise ValueError(f"capture source {field} does not match its fields")
    return identity


def _capture_destination_name(
    source_identity: Mapping[str, Any], capture_id: str
) -> str:
    identity = _validated_source_identity(source_identity)
    if not CAPTURE_ID_PATTERN.fullmatch(capture_id):
        raise ValueError(f"invalid capture id: {capture_id}")
    return f"{identity['storage_prefix']}_{capture_id}"


def _legacy_safe_ledger_source_key(row: Mapping[str, Any]) -> str:
    """Read old AWS ledger rows without mutating or relabelling them."""
    source = row.get("source_identity")
    if not isinstance(source, Mapping):
        return LEGACY_AWS_SOURCE_KEY
    try:
        return _validated_source_identity(source)["source_key"]
    except (TypeError, ValueError):
        return LEGACY_AWS_SOURCE_KEY


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _safe_relative_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe capture path: {value}")
    text = path.as_posix()
    if not text.startswith(SAFE_REMOTE_PREFIXES):
        raise ValueError(f"capture path is outside tape roots: {value}")
    return path


def _inspect_event_tape(path: Path) -> dict[str, Any]:
    event_counts: Counter[str] = Counter()
    market_counts: Counter[str] = Counter()
    first_receive_ts_ns: int | None = None
    last_receive_ts_ns: int | None = None
    event_count = 0
    error = ""
    valid = True
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                event_count += 1
                event_counts[str(row.get("event_type", "unknown"))] += 1
                market_counts[str(row.get("market_id", "unknown"))] += 1
                timestamp = int(row.get("local_receive_ts_ns") or 0)
                if timestamp <= 0:
                    continue
                first_receive_ts_ns = (
                    timestamp
                    if first_receive_ts_ns is None
                    else min(first_receive_ts_ns, timestamp)
                )
                last_receive_ts_ns = (
                    timestamp
                    if last_receive_ts_ns is None
                    else max(last_receive_ts_ns, timestamp)
                )
    except (EOFError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        valid = False
        error = repr(exc)
    return {
        "gzip_valid": valid,
        "validation_error": error,
        "event_count": event_count,
        "event_counts": dict(sorted(event_counts.items())),
        "market_counts": dict(sorted(market_counts.items())),
        "first_receive_ts_ns": first_receive_ts_ns,
        "last_receive_ts_ns": last_receive_ts_ns,
    }


def _merge_counts(
    rows: Iterable[dict[str, int]],
) -> dict[str, int]:
    result: Counter[str] = Counter()
    for row in rows:
        result.update({str(key): int(value) for key, value in row.items()})
    return dict(sorted(result.items()))


def _timestamp_from_marker(marker: dict[str, Any]) -> float:
    return datetime.fromisoformat(str(marker["timestamp_utc"])).timestamp()


def _extract_window_logs(
    *,
    root: Path,
    marker_dir: Path,
    start_s: float,
    end_s: float,
) -> tuple[list[str], list[str], list[dict[str, str]]]:
    health: list[tuple[float, str]] = []
    severe: list[tuple[float, str]] = []
    for path in sorted((root / "logs").glob("maker.log*")):
        if not path.is_file():
            continue
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if len(line) < 19:
                    continue
                try:
                    timestamp = (
                        datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
                        .replace(tzinfo=timezone.utc)
                        .timestamp()
                    )
                except ValueError:
                    continue
                if not start_s <= timestamp <= end_s:
                    continue
                if " INFO HEALTH " in line:
                    health.append((timestamp, line))
                if " ERROR " in line or " CRITICAL " in line:
                    severe.append((timestamp, line))
    health_lines = [line for _, line in sorted(set(health))]
    severe_lines = [line for _, line in sorted(set(severe))]

    trade_rows: list[dict[str, str]] = []
    trade_path = root / "logs" / "trades.csv"
    fieldnames: list[str] = []
    if trade_path.exists():
        with trade_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or [])
            for row in reader:
                try:
                    timestamp = float(row["timestamp"])
                except (KeyError, TypeError, ValueError):
                    continue
                if start_s <= timestamp <= end_s:
                    trade_rows.append(dict(row))

    capture_id = marker_dir.name
    _atomic_write_text(
        marker_dir / f"health_{capture_id}.log",
        "".join(health_lines),
    )
    _atomic_write_text(
        marker_dir / f"severe_{capture_id}.log",
        "".join(severe_lines),
    )
    trades_output = marker_dir / f"trades_{capture_id}.csv"
    temporary = trades_output.with_name(
        f".{trades_output.name}.{os.getpid()}.tmp"
    )
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(trade_rows)
    os.replace(temporary, trades_output)
    return health_lines, severe_lines, trade_rows


def _health_metric(lines: Sequence[str], name: str) -> float:
    pattern = re.compile(rf"(?:^| ){re.escape(name)}=([^ ]+)")
    values: list[float] = []
    for line in lines:
        match = pattern.search(line)
        if match is None:
            continue
        try:
            values.append(float(match.group(1)))
        except ValueError:
            continue
    return max(values) if values else 0.0


def _queue_metrics(health_lines: Sequence[str]) -> dict[str, float]:
    return {
        "external_record_dropped_max": _health_metric(
            health_lines, "externalRecordDropped"
        ),
        "external_record_hwm_max": _health_metric(
            health_lines, "externalRecordHwm"
        ),
        "external_record_max_age_ms": _health_metric(
            health_lines, "externalRecordMaxAgeMs"
        ),
        "market_tape_dropped_max": _health_metric(
            health_lines, "marketTapeDropped"
        ),
        "market_tape_invalid_max": _health_metric(
            health_lines, "marketTapeInvalid"
        ),
        "market_tape_hwm_max": _health_metric(
            health_lines, "marketTapeQueueHwm"
        ),
        "market_tape_max_age_ms": _health_metric(
            health_lines, "marketTapeMaxQueueAgeMs"
        ),
    }


def _interval(
    first_receive_ts_ns: int | None,
    last_receive_ts_ns: int | None,
) -> dict[str, Any]:
    if first_receive_ts_ns is None or last_receive_ts_ns is None:
        return {"start": None, "end": None, "duration_s": 0.0}
    start = datetime.fromtimestamp(
        first_receive_ts_ns / 1_000_000_000,
        tz=timezone.utc,
    )
    end = datetime.fromtimestamp(
        last_receive_ts_ns / 1_000_000_000,
        tz=timezone.utc,
    )
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "duration_s": (last_receive_ts_ns - first_receive_ts_ns)
        / 1_000_000_000,
    }


def finalize_capture(
    *,
    root: Path,
    config_path: Path,
    marker_dir: Path,
    sentinel: Path,
    duration_s: int,
    source_identity: Mapping[str, Any],
) -> dict[str, Any]:
    source = _validated_source_identity(source_identity)
    files: list[dict[str, Any]] = []
    for relative_root in ("logs/market_tape", "logs/external_venues"):
        directory = root / relative_root
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*.jsonl.gz")):
            if path.stat().st_mtime < sentinel.stat().st_mtime:
                continue
            inspection = _inspect_event_tape(path)
            files.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                    **inspection,
                }
            )

    enable = json.loads((marker_dir / "enable.json").read_text(encoding="utf-8"))
    disable = json.loads(
        (marker_dir / "disable.json").read_text(encoding="utf-8")
    )
    health, severe, trades = _extract_window_logs(
        root=root,
        marker_dir=marker_dir,
        start_s=_timestamp_from_marker(enable),
        end_s=_timestamp_from_marker(disable),
    )
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    external = config.get("external_venues", {})
    logging = config.get("logging", {})
    first_receive_ts_ns = min(
        (
            int(row["first_receive_ts_ns"])
            for row in files
            if row["first_receive_ts_ns"] is not None
        ),
        default=None,
    )
    last_receive_ts_ns = max(
        (
            int(row["last_receive_ts_ns"])
            for row in files
            if row["last_receive_ts_ns"] is not None
        ),
        default=None,
    )
    paths = [str(row["path"]) for row in files]
    queue = _queue_metrics(health)
    capture_disabled = not bool(logging.get("market_tape_enabled", False)) and not any(
        bool(source.get("record_enabled", False))
        for source in external.get("sources", [])
    )
    strategy_hash_unchanged = (
        enable.get("strategy_hash") == disable.get("strategy_hash")
    )
    config_sha256_baseline = str(enable.get("config_sha256_before") or "")
    config_sha256_capture_enabled = str(
        enable.get("config_sha256_after") or ""
    )
    config_sha256_before_disable = str(
        disable.get("config_sha256_before") or ""
    )
    config_sha256_restored = str(disable.get("config_sha256_after") or "")
    config_enable_disable_chain_valid = bool(
        config_sha256_capture_enabled
        and config_sha256_capture_enabled == config_sha256_before_disable
    )
    config_restored_to_baseline = bool(
        config_sha256_baseline
        and config_sha256_baseline == config_sha256_restored
        and config_sha256_restored == _sha256(config_path)
    )
    all_files_valid = (
        len(files) == EXPECTED_TAPE_COUNT
        and len(set(paths)) == EXPECTED_TAPE_COUNT
        and all(bool(row["gzip_valid"]) for row in files)
    )
    summary = {
        "schema_version": REMOTE_SUMMARY_SCHEMA,
        "capture_id": marker_dir.name,
        "source_identity": source,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_duration_s": int(duration_s),
        "capture_disabled_after_window": capture_disabled,
        "strategy_hash_enable": enable.get("strategy_hash"),
        "strategy_hash_disable": disable.get("strategy_hash"),
        "strategy_hash_unchanged": strategy_hash_unchanged,
        "config_sha256_baseline_before_enable": config_sha256_baseline,
        "config_sha256_capture_enabled": config_sha256_capture_enabled,
        "config_sha256_before_disable": config_sha256_before_disable,
        "config_sha256_restored_after_disable": config_sha256_restored,
        "config_enable_disable_chain_valid": config_enable_disable_chain_valid,
        "config_restored_to_baseline": config_restored_to_baseline,
        "files": files,
        "file_count": len(files),
        "unique_file_count": len(set(paths)),
        "total_bytes": sum(int(row["size_bytes"]) for row in files),
        "total_events": sum(int(row["event_count"]) for row in files),
        "event_counts": _merge_counts(row["event_counts"] for row in files),
        "market_counts": _merge_counts(row["market_counts"] for row in files),
        "interval_utc": _interval(first_receive_ts_ns, last_receive_ts_ns),
        "health_rows": len(health),
        "maker_fills": len(trades),
        "severe_log_count": len(severe),
        "severe_logs": severe,
        "queue": queue,
        "all_files_valid": all_files_valid,
        "valid": (
            all_files_valid
            and capture_disabled
            and strategy_hash_unchanged
            and config_enable_disable_chain_valid
            and config_restored_to_baseline
            and queue["external_record_dropped_max"] == 0.0
            and queue["market_tape_dropped_max"] == 0.0
        ),
    }
    _atomic_write_text(
        marker_dir / "summary.json",
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
    )
    return summary


def _marker_lines(marker: Path, prefix: str, capture_id: str) -> list[str]:
    path = marker / f"{prefix}_{capture_id}.log"
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def _marker_trade_count(marker: Path, capture_id: str) -> int:
    path = marker / f"trades_{capture_id}.csv"
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _append_ledger(ledger_path: Path, row: dict[str, Any]) -> bool:
    if ledger_path.name == LEGACY_LEDGER_FILENAME:
        raise ValueError(
            f"{LEGACY_LEDGER_FILENAME} is immutable; write "
            f"{CURRENT_LEDGER_FILENAME} instead"
        )
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        existing = [
            json.loads(line)
            for line in handle
            if line.strip()
        ]
        if any(item.get("capture_id") == row["capture_id"] for item in existing):
            return False
        handle.seek(0, os.SEEK_END)
        handle.write(json.dumps(row, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return True


def validate_local_capture(
    *,
    capture_dir: Path,
    ledger_path: Path,
) -> dict[str, Any]:
    marker = capture_dir / "marker"
    summary_path = marker / "summary.json"
    if not summary_path.exists():
        summary_path = capture_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    capture_id = str(summary["capture_id"])
    if not CAPTURE_ID_PATTERN.fullmatch(capture_id):
        raise ValueError(f"invalid capture id: {capture_id}")
    source_identity = _validated_source_identity(
        summary.get("source_identity") or {}
    )

    files: list[dict[str, Any]] = []
    first_receive_ts_ns: int | None = None
    last_receive_ts_ns: int | None = None
    for remote in summary.get("files", []):
        relative = _safe_relative_path(str(remote["path"]))
        path = capture_dir / relative
        if not path.exists():
            raise FileNotFoundError(path)
        inspection = _inspect_event_tape(path)
        local_sha256 = _sha256(path)
        remote_event_count = remote.get("event_count")
        files.append(
            {
                "path": relative.as_posix(),
                "size_bytes": path.stat().st_size,
                "event_count": int(inspection["event_count"]),
                "gzip_valid": bool(inspection["gzip_valid"]),
                "validation_error": inspection["validation_error"],
                "remote_gzip_valid": bool(remote.get("gzip_valid", False)),
                "remote_sha256": str(remote["sha256"]),
                "local_sha256": local_sha256,
                "sha256_match": local_sha256 == str(remote["sha256"]),
                "event_count_match": (
                    True
                    if remote_event_count is None
                    else int(remote_event_count) == int(inspection["event_count"])
                ),
                "event_counts": inspection["event_counts"],
                "market_counts": inspection["market_counts"],
            }
        )
        timestamp = inspection["first_receive_ts_ns"]
        if timestamp is not None:
            first_receive_ts_ns = (
                int(timestamp)
                if first_receive_ts_ns is None
                else min(first_receive_ts_ns, int(timestamp))
            )
        timestamp = inspection["last_receive_ts_ns"]
        if timestamp is not None:
            last_receive_ts_ns = (
                int(timestamp)
                if last_receive_ts_ns is None
                else max(last_receive_ts_ns, int(timestamp))
            )

    enable = json.loads((marker / "enable.json").read_text(encoding="utf-8"))
    disable = json.loads((marker / "disable.json").read_text(encoding="utf-8"))
    health = _marker_lines(marker, "health", capture_id)
    severe = _marker_lines(marker, "severe", capture_id)
    queue = dict(summary.get("queue") or _queue_metrics(health))
    interval = _interval(first_receive_ts_ns, last_receive_ts_ns)
    paths = [row["path"] for row in files]
    strategy_hash_enable = str(
        summary.get("strategy_hash_enable") or enable.get("strategy_hash")
    )
    strategy_hash_disable = str(
        summary.get("strategy_hash_disable") or disable.get("strategy_hash")
    )
    strategy_hash_unchanged = strategy_hash_enable == strategy_hash_disable
    config_sha256_baseline = str(
        summary.get("config_sha256_baseline_before_enable")
        or enable.get("config_sha256_before")
        or ""
    )
    config_sha256_capture_enabled = str(
        summary.get("config_sha256_capture_enabled")
        or enable.get("config_sha256_after")
        or ""
    )
    config_sha256_before_disable = str(
        summary.get("config_sha256_before_disable")
        or disable.get("config_sha256_before")
        or ""
    )
    config_sha256_restored = str(
        summary.get("config_sha256_restored_after_disable")
        or disable.get("config_sha256_after")
        or ""
    )
    config_enable_disable_chain_valid = bool(
        config_sha256_capture_enabled
        and config_sha256_capture_enabled == config_sha256_before_disable
    )
    config_restored_to_baseline = bool(
        config_sha256_baseline
        and config_sha256_baseline == config_sha256_restored
    )
    capture_disabled = bool(summary.get("capture_disabled_after_window", False))
    valid = (
        capture_disabled
        and len(files) == EXPECTED_TAPE_COUNT
        and len(set(paths)) == EXPECTED_TAPE_COUNT
        and all(
            row["gzip_valid"]
            and row["remote_gzip_valid"]
            and row["sha256_match"]
            and row["event_count_match"]
            for row in files
        )
        and strategy_hash_unchanged
        and config_enable_disable_chain_valid
        and config_restored_to_baseline
        and float(queue.get("external_record_dropped_max", 0.0)) == 0.0
        and float(queue.get("market_tape_dropped_max", 0.0)) == 0.0
    )
    validation = {
        "schema_version": LOCAL_VALIDATION_SCHEMA,
        "capture_id": capture_id,
        "source_identity": source_identity,
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_duration_s": int(summary.get("requested_duration_s", 0)),
        "utc_day": (
            str(interval["start"])[:10] if interval["start"] is not None else ""
        ),
        "interval_utc": interval,
        "capture_disabled_after_window": capture_disabled,
        "file_count": len(files),
        "unique_file_count": len(set(paths)),
        "files": files,
        "event_counts": _merge_counts(row["event_counts"] for row in files),
        "market_counts": _merge_counts(row["market_counts"] for row in files),
        "total_events": sum(int(row["event_count"]) for row in files),
        "health_rows": int(summary.get("health_rows", len(health))),
        "maker_fills": int(
            summary.get("maker_fills", _marker_trade_count(marker, capture_id))
        ),
        "severe_log_count": int(
            summary.get("severe_log_count", len(severe))
        ),
        "severe_logs": list(summary.get("severe_logs", severe)),
        "queue": queue,
        "strategy_hash_enable": strategy_hash_enable,
        "strategy_hash_disable": strategy_hash_disable,
        "strategy_hash_unchanged": strategy_hash_unchanged,
        "config_sha256_baseline_before_enable": config_sha256_baseline,
        "config_sha256_capture_enabled": config_sha256_capture_enabled,
        "config_sha256_before_disable": config_sha256_before_disable,
        "config_sha256_restored_after_disable": config_sha256_restored,
        "config_enable_disable_chain_valid": config_enable_disable_chain_valid,
        "config_restored_to_baseline": config_restored_to_baseline,
        "valid": valid,
    }
    _atomic_write_text(
        marker / "local_integrity_validation.json",
        json.dumps(validation, indent=2, sort_keys=True) + "\n",
    )
    manifest_path = marker / "capture_manifest.csv"
    temporary = manifest_path.with_name(
        f".{manifest_path.name}.{os.getpid()}.tmp"
    )
    manifest_columns = (
        "path",
        "size_bytes",
        "event_count",
        "remote_sha256",
        "local_sha256",
        "sha256_match",
        "gzip_valid",
        "event_count_match",
    )
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=manifest_columns)
        writer.writeheader()
        writer.writerows(
            {column: row[column] for column in manifest_columns}
            for row in files
        )
    os.replace(temporary, manifest_path)

    ledger_row = {
        key: validation[key]
        for key in (
            "capture_id",
            "source_identity",
            "requested_duration_s",
            "utc_day",
            "interval_utc",
            "capture_disabled_after_window",
            "file_count",
            "total_events",
            "maker_fills",
            "health_rows",
            "severe_log_count",
            "queue",
            "strategy_hash_enable",
            "strategy_hash_disable",
            "strategy_hash_unchanged",
            "config_sha256_baseline_before_enable",
            "config_sha256_capture_enabled",
            "config_sha256_before_disable",
            "config_sha256_restored_after_disable",
            "config_enable_disable_chain_valid",
            "config_restored_to_baseline",
            "valid",
            "validated_at_utc",
        )
    }
    ledger_row["schema_version"] = LEDGER_SCHEMA
    validation["ledger_appended"] = (
        _append_ledger(ledger_path, ledger_row) if valid else False
    )
    return validation


def _run(
    command: Sequence[str],
    *,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        input=input_text,
        check=True,
        text=True,
        capture_output=True,
    )


def _ledger_ids(path: Path) -> set[str]:
    return {str(row["capture_id"]) for row in _ledger_rows(path)}


def _ledger_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _combined_ledger_rows(
    *, current_path: Path, legacy_path: Path | None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    current = _ledger_rows(current_path)
    legacy = (
        _ledger_rows(legacy_path)
        if legacy_path is not None and legacy_path != current_path
        else []
    )
    return current, legacy


def _full_window_completed_on_day(
    rows: Iterable[Mapping[str, Any]], utc_day: str
) -> bool:
    return any(
        str(row.get("utc_day") or "") == utc_day and _is_full_window(dict(row))
        for row in rows
    )


def _is_full_window(row: dict[str, Any]) -> bool:
    requested = float(row.get("requested_duration_s") or 0.0)
    observed = float((row.get("interval_utc") or {}).get("duration_s") or 0.0)
    return bool(row.get("valid", True)) and max(requested, observed) >= 3500.0


def _try_lock(path: Path) -> tuple[Any, bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return handle, False
    return handle, True


def discover_remote_captures(
    *,
    remote: str,
    remote_root: str,
) -> list[str]:
    status = remote_status(remote=remote, remote_root=remote_root)
    return sorted(
        str(row["capture_id"])
        for row in status["captures"]
        if bool(row.get("eligible_for_sync", False))
        and CAPTURE_ID_PATTERN.fullmatch(str(row["capture_id"]))
    )


def _delete_remote_payloads(
    *,
    remote: str,
    remote_root: str,
    paths: Sequence[str],
) -> None:
    script = (
        "import json,sys\n"
        "from pathlib import Path\n"
        "paths=json.load(sys.stdin)\n"
        "for value in paths:\n"
        " p=Path(value)\n"
        " text=p.as_posix()\n"
        " if p.is_absolute() or '..' in p.parts or not "
        "(text.startswith('logs/market_tape/') or "
        "text.startswith('logs/external_venues/')):\n"
        "  raise SystemExit(f'unsafe path: {value}')\n"
        " if p.exists(): p.unlink()\n"
    )
    command = (
        f"cd {shlex.quote(remote_root)} && "
        f"python3 -c {shlex.quote(script)}"
    )
    _run(
        ["ssh", "-o", "BatchMode=yes", remote, command],
        input_text=json.dumps(list(paths)),
    )


def sync_capture(
    *,
    remote: str,
    remote_root: str,
    local_root: Path,
    ledger_path: Path,
    capture_id: str,
    delete_remote: bool,
    source_identity: Mapping[str, Any],
) -> dict[str, Any]:
    if not CAPTURE_ID_PATTERN.fullmatch(capture_id):
        raise ValueError(f"invalid capture id: {capture_id}")
    expected_source = _validated_source_identity(source_identity)
    destination = local_root / _capture_destination_name(
        expected_source, capture_id
    )
    marker = destination / "marker"
    marker.mkdir(parents=True, exist_ok=True)
    remote_marker = (
        f"{remote}:{remote_root}/logs/receive_time_capture/{capture_id}/"
    )
    _run(["rsync", "-a", "--partial", remote_marker, f"{marker}/"])
    summary = json.loads((marker / "summary.json").read_text(encoding="utf-8"))
    observed_source = _validated_source_identity(
        summary.get("source_identity") or {}
    )
    if observed_source != expected_source:
        raise RuntimeError(
            "remote capture source identity does not match requested source"
        )
    paths = [
        _safe_relative_path(str(row["path"])).as_posix()
        for row in summary.get("files", [])
    ]
    if len(paths) != EXPECTED_TAPE_COUNT or len(set(paths)) != EXPECTED_TAPE_COUNT:
        raise RuntimeError(
            f"{capture_id} does not contain seven unique tape paths"
        )
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8") as handle:
        handle.write("\n".join(paths) + "\n")
        handle.flush()
        _run(
            [
                "rsync",
                "-a",
                "--partial",
                f"--files-from={handle.name}",
                f"{remote}:{remote_root}/",
                f"{destination}/",
            ]
        )
    shutil.copy2(marker / "summary.json", destination / "summary.json")
    validation = validate_local_capture(
        capture_dir=destination,
        ledger_path=ledger_path,
    )
    if validation["valid"] and delete_remote:
        _delete_remote_payloads(
            remote=remote,
            remote_root=remote_root,
            paths=paths,
        )
        validation["remote_payloads_deleted"] = True
    else:
        validation["remote_payloads_deleted"] = False
    return validation


def remote_status(
    *,
    remote: str,
    remote_root: str,
) -> dict[str, Any]:
    script = (
        "import fcntl,json\n"
        "from datetime import datetime,timezone\n"
        "from pathlib import Path\n"
        "root=Path('.')\n"
        "lock=root/'logs/receive_time_capture/capture.lock'\n"
        "lock.parent.mkdir(parents=True,exist_ok=True)\n"
        "lock_handle=lock.open('a+')\n"
        "try:\n"
        " fcntl.flock(lock_handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
        " capture_active=False\n"
        "except BlockingIOError:\n"
        " capture_active=True\n"
        "captures=[]\n"
        "for path in sorted((root/'logs/receive_time_capture').glob("
        "'*/summary.json')):\n"
        " try: row=json.loads(path.read_text())\n"
        " except Exception: continue\n"
        " files=row.get('files') or []\n"
        " paths=[str(item.get('path','')) for item in files]\n"
        " source=row.get('source_identity') or {}\n"
        " source_bound=(row.get('schema_version')=="
        f"'{REMOTE_SUMMARY_SCHEMA}' and source.get('schema_version')=="
        f"'{SOURCE_IDENTITY_SCHEMA}' and bool(source.get('source_key')) and "
        "bool(source.get('storage_prefix')))\n"
        " payloads_present=(len(paths)==7 and "
        "all((root/Path(value)).is_file() for value in paths))\n"
        " eligible=(bool(row.get('valid',False)) and "
        "bool(row.get('all_files_valid',False)) and "
        "bool(row.get('capture_disabled_after_window',False)) and "
        "len(paths)==7 and len(set(paths))==7 and payloads_present and "
        "source_bound)\n"
        " captures.append({'capture_id':path.parent.name,"
        "'requested_duration_s':row.get('requested_duration_s',0),"
        "'valid':row.get('valid',False),"
        "'all_files_valid':row.get('all_files_valid',False),"
        "'capture_disabled_after_window':"
        "row.get('capture_disabled_after_window',False),"
        "'payloads_present':payloads_present,"
        "'source_identity':source,'source_bound':source_bound,"
        "'eligible_for_sync':eligible,"
        "'completed_at_utc':row.get('completed_at_utc')})\n"
        "print(json.dumps({'capture_active':capture_active,'captures':captures,"
        "'utc_day':datetime.now(timezone.utc).date().isoformat()}))\n"
    )
    command = (
        f"cd {shlex.quote(remote_root)} && "
        f"python3 -c {shlex.quote(script)}"
    )
    return json.loads(
        _run(["ssh", "-o", "BatchMode=yes", remote, command]).stdout
    )


def start_remote_capture(
    *,
    remote: str,
    remote_root: str,
    duration_s: int,
    allow_duplicate_day: bool,
    source_identity: Mapping[str, Any],
) -> dict[str, Any]:
    source = _validated_source_identity(source_identity)
    status = remote_status(remote=remote, remote_root=remote_root)
    if status["capture_active"]:
        return {"started": False, "reason": "capture_active", **status}
    if not allow_duplicate_day:
        today = str(status["utc_day"]).replace("-", "")
        for capture in status["captures"]:
            if (
                str(capture["capture_id"]).startswith(today)
                and int(capture.get("requested_duration_s", 0)) >= duration_s
                and bool(capture.get("valid", False))
            ):
                return {
                    "started": False,
                    "reason": "full_window_already_completed_today",
                    **status,
                }
    command = (
        f"cd {shlex.quote(remote_root)} && "
        "mkdir -p logs/receive_time_capture && "
        "stamp=$(date -u +%Y%m%dT%H%M%SZ) && "
        "{ "
        "nohup env "
        "NARROWGATE_CAPTURE_ID=\"$stamp\" "
        f"NARROWGATE_CAPTURE_SOURCE_PROVIDER={shlex.quote(source['provider'])} "
        f"NARROWGATE_CAPTURE_SOURCE_REGION={shlex.quote(source['region'])} "
        f"NARROWGATE_CAPTURE_SOURCE_CITY={shlex.quote(source['city'])} "
        "NARROWGATE_CAPTURE_SOURCE_PUBLIC_IPV4="
        f"{shlex.quote(source['public_ipv4'])} "
        f"NARROWGATE_CAPTURE_SOURCE_SSH_TARGET={shlex.quote(source['ssh_target'])} "
        f"scripts/run_bounded_receive_time_capture.sh {int(duration_s)} "
        "> \"logs/receive_time_capture/launcher_${stamp}.log\" 2>&1 "
        "< /dev/null & "
        "pid=$!; printf '%s %s\\n' \"$pid\" \"$stamp\"; "
        "}"
    )
    output = _run(
        ["ssh", "-o", "BatchMode=yes", remote, command]
    ).stdout.strip()
    pid_text, stamp = output.split(maxsplit=1)
    return {
        "started": True,
        "pid": int(pid_text),
        "capture_id": stamp,
        "duration_s": int(duration_s),
    }


def _launch_background(argv: Sequence[str], log_path: Path) -> int:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        *[arg for arg in argv if arg != "--background"],
    ]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as handle:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    print(
        json.dumps(
            {
                "started": True,
                "background_pid": process.pid,
                "log_path": str(log_path),
            },
            sort_keys=True,
        )
    )
    return 0


def _pending_capture_ids(
    status: dict[str, Any],
    ledger_ids: set[str],
) -> list[str]:
    return sorted(
        str(row["capture_id"])
        for row in status["captures"]
        if bool(row.get("eligible_for_sync", False))
        and str(row["capture_id"]) not in ledger_ids
    )


def _full_window_completed_today(
    status: dict[str, Any],
    *,
    duration_s: int,
) -> bool:
    today = str(status["utc_day"]).replace("-", "")
    return any(
        str(row["capture_id"]).startswith(today)
        and int(row.get("requested_duration_s", 0)) >= duration_s
        and bool(row.get("valid", False))
        and bool(row.get("capture_disabled_after_window", False))
        for row in status["captures"]
    )


def _sync_one_with_lock(
    *,
    remote: str,
    remote_root: str,
    local_root: Path,
    ledger_path: Path,
    capture_id: str,
    delete_remote: bool,
    source_identity: Mapping[str, Any],
) -> dict[str, Any]:
    lock_handle, acquired = _try_lock(local_root / "sync_jobs" / "sync.lock")
    if not acquired:
        lock_handle.close()
        return {
            "capture_id": capture_id,
            "valid": False,
            "skipped": True,
            "reason": "sync_active",
        }
    try:
        return sync_capture(
            remote=remote,
            remote_root=remote_root,
            local_root=local_root,
            ledger_path=ledger_path,
            capture_id=capture_id,
            delete_remote=delete_remote,
            source_identity=source_identity,
        )
    finally:
        lock_handle.close()


def collect_capture_cycle(
    *,
    remote: str,
    remote_root: str,
    local_root: Path,
    ledger_path: Path,
    legacy_ledger_path: Path | None,
    duration_s: int,
    poll_interval_s: float,
    timeout_s: float,
    delete_remote: bool,
    source_identity: Mapping[str, Any],
) -> dict[str, Any]:
    source = _validated_source_identity(source_identity)
    cycle_lock, acquired = _try_lock(
        local_root / "sync_jobs" / "collect_cycle.lock"
    )
    if not acquired:
        cycle_lock.close()
        return {"skipped": True, "reason": "collect_cycle_active"}
    try:
        status = remote_status(remote=remote, remote_root=remote_root)
        current_rows, legacy_rows = _combined_ledger_rows(
            current_path=ledger_path, legacy_path=legacy_ledger_path
        )
        combined_rows = [*legacy_rows, *current_rows]
        combined_ids = {
            str(row["capture_id"])
            for row in combined_rows
            if row.get("capture_id")
        }
        if _full_window_completed_on_day(
            combined_rows, str(status.get("utc_day") or "")
        ):
            return {
                "skipped": True,
                "reason": "full_window_already_admitted_today",
            }
        pending = _pending_capture_ids(status, combined_ids)
        if pending:
            return {
                "started_capture": False,
                "sync": _sync_one_with_lock(
                    remote=remote,
                    remote_root=remote_root,
                    local_root=local_root,
                    ledger_path=ledger_path,
                    capture_id=pending[0],
                    delete_remote=delete_remote,
                    source_identity=source,
                ),
            }
        if _full_window_completed_today(status, duration_s=duration_s):
            return {
                "skipped": True,
                "reason": "full_window_already_completed_today",
            }

        start_result: dict[str, Any] | None = None
        if not bool(status["capture_active"]):
            start_result = start_remote_capture(
                remote=remote,
                remote_root=remote_root,
                duration_s=duration_s,
                allow_duplicate_day=False,
                source_identity=source,
            )

        deadline = time.monotonic() + max(0.0, timeout_s)
        while time.monotonic() <= deadline:
            if poll_interval_s > 0:
                time.sleep(poll_interval_s)
            status = remote_status(remote=remote, remote_root=remote_root)
            current_rows, legacy_rows = _combined_ledger_rows(
                current_path=ledger_path,
                legacy_path=legacy_ledger_path,
            )
            combined_ids = {
                str(row["capture_id"])
                for row in [*legacy_rows, *current_rows]
                if row.get("capture_id")
            }
            pending = _pending_capture_ids(status, combined_ids)
            if pending:
                return {
                    "started_capture": bool(
                        start_result and start_result.get("started")
                    ),
                    "start": start_result,
                    "sync": _sync_one_with_lock(
                        remote=remote,
                        remote_root=remote_root,
                        local_root=local_root,
                        ledger_path=ledger_path,
                        capture_id=pending[0],
                        delete_remote=delete_remote,
                        source_identity=source,
                    ),
                }
            if not bool(status["capture_active"]):
                return {
                    "started_capture": bool(
                        start_result and start_result.get("started")
                    ),
                    "start": start_result,
                    "valid": False,
                    "reason": "capture_finished_without_eligible_summary",
                    "status": status,
                }
        return {
            "started_capture": bool(start_result and start_result.get("started")),
            "start": start_result,
            "valid": False,
            "reason": "capture_wait_timeout",
        }
    finally:
        cycle_lock.close()


def _add_source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-provider", default=DEFAULT_SOURCE_PROVIDER
    )
    parser.add_argument("--source-region", default=DEFAULT_SOURCE_REGION)
    parser.add_argument("--source-city", default=DEFAULT_SOURCE_CITY)
    parser.add_argument(
        "--source-public-ipv4", default=DEFAULT_SOURCE_PUBLIC_IPV4
    )
    parser.add_argument(
        "--source-ssh-target",
        default=DEFAULT_SOURCE_SSH_TARGET,
    )


def _source_identity_from_args(
    args: argparse.Namespace,
) -> dict[str, str]:
    identity = capture_source_identity(
        provider=args.source_provider,
        region=args.source_region,
        city=args.source_city,
        public_ipv4=args.source_public_ipv4,
        ssh_target=args.source_ssh_target or getattr(args, "remote", ""),
    )
    if hasattr(args, "remote"):
        require_remote_matches_source(args.remote, identity)
    return identity


def _ledger_paths_from_args(
    args: argparse.Namespace, local_root: Path
) -> tuple[Path, Path]:
    current = (
        args.ledger.expanduser().resolve()
        if args.ledger is not None
        else local_root / CURRENT_LEDGER_FILENAME
    )
    legacy = (
        args.legacy_ledger.expanduser().resolve()
        if args.legacy_ledger is not None
        else local_root / LEGACY_LEDGER_FILENAME
    )
    if current.name == LEGACY_LEDGER_FILENAME:
        raise ValueError(
            f"{LEGACY_LEDGER_FILENAME} is immutable; select "
            f"{CURRENT_LEDGER_FILENAME} for writes"
        )
    return current, legacy


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--root", type=Path, required=True)
    finalize.add_argument("--config", type=Path, required=True)
    finalize.add_argument("--marker-dir", type=Path, required=True)
    finalize.add_argument("--sentinel", type=Path, required=True)
    finalize.add_argument("--duration-s", type=int, required=True)
    _add_source_arguments(finalize)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--capture-dir", type=Path, required=True)
    validate.add_argument("--ledger", type=Path, required=True)

    sync = subparsers.add_parser("sync")
    sync.add_argument(
        "--remote",
        default=DEFAULT_REMOTE,
    )
    sync.add_argument(
        "--remote-root",
        default=DEFAULT_REMOTE_ROOT,
    )
    sync.add_argument(
        "--local-root",
        type=Path,
        default=DEFAULT_LOCAL_ROOT,
    )
    sync.add_argument("--ledger", type=Path)
    sync.add_argument("--legacy-ledger", type=Path)
    sync.add_argument("--capture-id")
    sync.add_argument("--max-captures", type=int, default=1)
    sync.add_argument("--keep-remote", action="store_true")
    sync.add_argument("--background", action="store_true")
    sync.add_argument("--background-log", type=Path)
    _add_source_arguments(sync)

    status = subparsers.add_parser("status")
    status.add_argument(
        "--remote",
        default=DEFAULT_REMOTE,
    )
    status.add_argument(
        "--remote-root",
        default=DEFAULT_REMOTE_ROOT,
    )
    status.add_argument(
        "--local-root",
        type=Path,
        default=DEFAULT_LOCAL_ROOT,
    )
    status.add_argument("--ledger", type=Path)
    status.add_argument("--legacy-ledger", type=Path)
    _add_source_arguments(status)

    start = subparsers.add_parser("start-remote")
    start.add_argument(
        "--remote",
        default=DEFAULT_REMOTE,
    )
    start.add_argument(
        "--remote-root",
        default=DEFAULT_REMOTE_ROOT,
    )
    start.add_argument("--duration-s", type=int, default=3600)
    start.add_argument("--allow-duplicate-day", action="store_true")
    _add_source_arguments(start)

    collect = subparsers.add_parser("collect")
    collect.add_argument(
        "--remote",
        default=DEFAULT_REMOTE,
    )
    collect.add_argument(
        "--remote-root",
        default=DEFAULT_REMOTE_ROOT,
    )
    collect.add_argument(
        "--local-root",
        type=Path,
        default=DEFAULT_LOCAL_ROOT,
    )
    collect.add_argument("--ledger", type=Path)
    collect.add_argument("--legacy-ledger", type=Path)
    collect.add_argument("--duration-s", type=int, default=3600)
    collect.add_argument("--poll-interval-s", type=float, default=30.0)
    collect.add_argument("--timeout-s", type=float, default=5400.0)
    collect.add_argument("--keep-remote", action="store_true")
    collect.add_argument("--background", action="store_true")
    collect.add_argument("--background-log", type=Path)
    _add_source_arguments(collect)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "finalize":
        summary = finalize_capture(
            root=args.root.expanduser().resolve(),
            config_path=args.config.expanduser().resolve(),
            marker_dir=args.marker_dir.expanduser().resolve(),
            sentinel=args.sentinel.expanduser().resolve(),
            duration_s=args.duration_s,
            source_identity=_source_identity_from_args(args),
        )
        print(json.dumps(summary, sort_keys=True))
        return 0 if summary["valid"] else 2
    if args.command == "validate":
        validation = validate_local_capture(
            capture_dir=args.capture_dir.expanduser().resolve(),
            ledger_path=args.ledger.expanduser().resolve(),
        )
        print(json.dumps(validation, sort_keys=True))
        return 0 if validation["valid"] else 2
    if args.command == "sync":
        source_identity = _source_identity_from_args(args)
        local_root = args.local_root.expanduser().resolve()
        ledger, legacy_ledger = _ledger_paths_from_args(args, local_root)
        if args.background:
            log_path = (
                args.background_log.expanduser().resolve()
                if args.background_log is not None
                else local_root
                / "sync_jobs"
                / f"sync_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.log"
            )
            return _launch_background(
                list(argv if argv is not None else sys.argv[1:]),
                log_path,
            )
        lock_handle, acquired = _try_lock(
            local_root / "sync_jobs" / "sync.lock"
        )
        if not acquired:
            lock_handle.close()
            print(
                json.dumps(
                    {"captures": [], "skipped": True, "reason": "sync_active"},
                    sort_keys=True,
                )
            )
            return 0
        try:
            current_rows, legacy_rows = _combined_ledger_rows(
                current_path=ledger,
                legacy_path=legacy_ledger,
            )
            admitted_ids = {
                str(row["capture_id"])
                for row in [*legacy_rows, *current_rows]
                if row.get("capture_id")
            }
            capture_ids = (
                (
                    [args.capture_id]
                    if args.capture_id not in admitted_ids
                    else []
                )
                if args.capture_id
                else [
                    capture_id
                    for capture_id in discover_remote_captures(
                        remote=args.remote,
                        remote_root=args.remote_root,
                    )
                    if capture_id not in admitted_ids
                ][: max(0, int(args.max_captures))]
            )
            results = [
                sync_capture(
                    remote=args.remote,
                    remote_root=args.remote_root,
                    local_root=local_root,
                    ledger_path=ledger,
                    capture_id=capture_id,
                    delete_remote=not args.keep_remote,
                    source_identity=source_identity,
                )
                for capture_id in capture_ids
            ]
        finally:
            lock_handle.close()
        print(json.dumps({"captures": results}, sort_keys=True))
        return 0 if all(row["valid"] for row in results) else 2
    if args.command == "status":
        source_identity = _source_identity_from_args(args)
        local_root = args.local_root.expanduser().resolve()
        ledger, legacy_ledger = _ledger_paths_from_args(args, local_root)
        status = remote_status(
            remote=args.remote,
            remote_root=args.remote_root,
        )
        sync_lock, available = _try_lock(
            local_root / "sync_jobs" / "sync.lock"
        )
        if available:
            fcntl.flock(sync_lock.fileno(), fcntl.LOCK_UN)
        sync_lock.close()
        status["local_sync_active"] = not available
        collect_lock, available = _try_lock(
            local_root / "sync_jobs" / "collect_cycle.lock"
        )
        if available:
            fcntl.flock(collect_lock.fileno(), fcntl.LOCK_UN)
        collect_lock.close()
        status["local_collect_active"] = not available
        status["requested_source_identity"] = source_identity
        current_ledger_rows, legacy_ledger_rows = _combined_ledger_rows(
            current_path=ledger,
            legacy_path=legacy_ledger,
        )
        ledger_rows = [*legacy_ledger_rows, *current_ledger_rows]
        status["current_ledger_path"] = str(ledger)
        status["legacy_ledger_path"] = str(legacy_ledger)
        status["current_ledger_capture_ids"] = sorted(
            str(row["capture_id"])
            for row in current_ledger_rows
            if row.get("capture_id")
        )
        status["legacy_ledger_capture_ids"] = sorted(
            str(row["capture_id"])
            for row in legacy_ledger_rows
            if row.get("capture_id")
        )
        status["ledger_capture_ids"] = sorted(
            str(row["capture_id"]) for row in ledger_rows
        )
        full_window_days = {
            str(row.get("utc_day") or "")
            for row in ledger_rows
            if _is_full_window(row) and row.get("utc_day")
        }
        status["valid_full_window_capture_count"] = sum(
            _is_full_window(row) for row in ledger_rows
        )
        status["valid_full_window_utc_days"] = sorted(full_window_days)
        full_windows_by_source: dict[str, set[str]] = {}
        for row in legacy_ledger_rows:
            if not _is_full_window(row) or not row.get("utc_day"):
                continue
            full_windows_by_source.setdefault(
                LEGACY_AWS_SOURCE_KEY, set()
            ).add(str(row["utc_day"]))
        for row in current_ledger_rows:
            if not _is_full_window(row) or not row.get("utc_day"):
                continue
            source_key = _legacy_safe_ledger_source_key(row)
            full_windows_by_source.setdefault(source_key, set()).add(
                str(row["utc_day"])
            )
        status["valid_full_window_utc_days_by_source"] = {
            key: sorted(days)
            for key, days in sorted(full_windows_by_source.items())
        }
        ledger_ids = set(status["ledger_capture_ids"])
        status["pending_capture_ids"] = [
            str(row["capture_id"])
            for row in status["captures"]
            if bool(row.get("eligible_for_sync", False))
            and str(row["capture_id"]) not in ledger_ids
        ]
        print(json.dumps(status, indent=2, sort_keys=True))
        return 0
    if args.command == "start-remote":
        result = start_remote_capture(
            remote=args.remote,
            remote_root=args.remote_root,
            duration_s=args.duration_s,
            allow_duplicate_day=args.allow_duplicate_day,
            source_identity=_source_identity_from_args(args),
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.command == "collect":
        source_identity = _source_identity_from_args(args)
        local_root = args.local_root.expanduser().resolve()
        ledger, legacy_ledger = _ledger_paths_from_args(args, local_root)
        if args.background:
            log_path = (
                args.background_log.expanduser().resolve()
                if args.background_log is not None
                else local_root
                / "sync_jobs"
                / f"collect_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.log"
            )
            return _launch_background(
                list(argv if argv is not None else sys.argv[1:]),
                log_path,
            )
        result = collect_capture_cycle(
            remote=args.remote,
            remote_root=args.remote_root,
            local_root=local_root,
            ledger_path=ledger,
            legacy_ledger_path=legacy_ledger,
            duration_s=args.duration_s,
            poll_interval_s=args.poll_interval_s,
            timeout_s=args.timeout_s,
            delete_remote=not args.keep_remote,
            source_identity=source_identity,
        )
        print(json.dumps(result, sort_keys=True))
        sync = result.get("sync")
        if sync is not None:
            return 0 if bool(sync.get("valid", False)) else 2
        return 2 if result.get("valid") is False else 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
