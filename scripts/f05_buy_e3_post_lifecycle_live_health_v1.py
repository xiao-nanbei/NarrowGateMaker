#!/usr/bin/env python3
"""Capture and revalidate post-lifecycle live health without new authority.

The producer is intended to run beside the already-active maker after the new
lifecycle admission exists.  It reads only ordinary live-health and process
safety state.  It never creates a shadow/companion evaluator and never
persists positions, prices, sizes, PnL, order identifiers, or raw API payloads.

The raw private receipt retains the lexical log path and inode/offset evidence
needed for same-host self-validation.  ``portable_projection`` deliberately
removes those host-local fields so a final evidence consumer can bind the
source-frozen exact-seven receipt without pretending to reopen the remote log.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import re
import stat
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v8 as resource_v8,
)
from scripts import f05_buy_e3_active_capture_v8 as active_capture_v8
from scripts import f05_buy_e3_evidence_completion as lifecycle_io

OWNER: Final = active_capture_v8.OWNER
SCHEMA_VERSION: Final = f"{OWNER}.post_lifecycle_live_health_receipt.v1"
STATUS: Final = "same_active_process_post_lifecycle_live_health_self_validated"
CANONICAL_FIELD: Final = "canonical_post_lifecycle_live_health_receipt_sha256"

PORTABLE_SCHEMA_VERSION: Final = f"{OWNER}.post_lifecycle_live_health.v1"
PORTABLE_STATUS: Final = (
    "same_active_process_post_lifecycle_health_and_operational_aggregates_verified"
)
PORTABLE_CANONICAL_FIELD: Final = "canonical_post_lifecycle_live_health_sha256"

CONTENT_BINDING_FIELDS: Final = frozenset(
    {
        "schema_version",
        "status",
        "file_sha256",
        "canonical_field",
        "canonical_sha256",
        "size_bytes",
        "mode",
    }
)
RUNTIME_EXECUTION: Final = {
    "execution_commit": active_capture_v8.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
    "execution_tree": active_capture_v8.DIRECT_SUCCESSOR_EXECUTION_TREE,
    "annotated_operational_tag": active_capture_v8.DIRECT_SUCCESSOR_ANNOTATED_TAG,
    "annotated_operational_tag_object": active_capture_v8.DIRECT_SUCCESSOR_TAG_OBJECT,
    "tag_peeled_commit": active_capture_v8.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
}
EXPECTED_STARTUP_SOURCE_SHA256: Final = {
    str(resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[role]["path"]): str(
        resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[role]["sha256"]
    )
    for role in active_capture_v8.STARTUP_SOURCE_ROLE_MAP.values()
}
EXPECTED_LIFECYCLE_SOURCE_SHA256: Final = {
    str(resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[role]["path"]): str(
        resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[role]["sha256"]
    )
    for role in active_capture_v8.REQUIRED_ACTIVE_SOURCE_ROLES
}
EXPECTED_ALL_RUNTIME_SOURCE_SHA256: Final = {
    str(binding["path"]): str(binding["sha256"])
    for binding in resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.values()
}

NO_AUTHORITY: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "shadow_or_companion_collection_enabled": False,
    "hypothetical_live_actions_scored": False,
}
CHECKS: Final = {
    "constructor_eof_boundary_only": True,
    "lexical_regular_non_symlink_log": True,
    "log_device_and_inode_stable": True,
    "first_two_fresh_main_health_rows": True,
    "first_fresh_lifecycle_health_row": True,
    "pid_and_start_ticks_checked_before_and_after_every_poll": True,
    "final_full_process_recapture_exact": True,
    "activation_capture_content_exact": True,
    "release_v3_and_runtime_execution_exact": True,
    "startup_runtime_source_exact10": True,
    "lifecycle_runtime_source_exact5": True,
    "lifecycle_exact7_bound": True,
    "generated_after_lifecycle_admission": True,
    "buy_e3_and_sell_owner_enabled_both_main_rows": True,
    "external_sources_and_errors_absolute_zero": True,
    "global_flow_explicit_disabled_error_state_value_backend_absolute_zero": True,
    "global_reference_explicit_disabled_error_state_value_absolute_zero": True,
    "lifecycle_drop_error_zero": True,
    "operational_aggregates_only": True,
    "economic_values_persisted": False,
}
PORTABLE_CHECKS: Final = {
    "same_active_pid_start_config_release_runtime": True,
    "snapshot_after_lifecycle_admission": True,
    "two_fresh_post_lifecycle_main_health_rows": True,
    "buy_e3_and_sell_owner_enabled": True,
    "external_sources_absolute_zero": True,
    "global_flow_explicit_disabled_error_and_backend_zero": True,
    "global_reference_explicit_disabled_error_and_state_zero": True,
    "lifecycle_drop_error_zero": True,
    "operational_aggregates_only": True,
    "economic_values_persisted": False,
}
TOP_LEVEL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "activation_capture",
        "lifecycle_admission",
        "lifecycle_context",
        "runtime_execution",
        "runtime_authority",
        "active_process",
        "log_capture",
        "main_health_window",
        "lifecycle_health",
        "operational_aggregates",
        "portable_projection",
        "checks",
        "permissions",
        "evidence_boundary",
        CANONICAL_FIELD,
    }
)
PORTABLE_FIELDS: Final = frozenset(
    {
        "schema_version",
        "status",
        "generated_utc",
        "runtime_execution",
        "runtime_authority",
        "active_process",
        "lifecycle_admission",
        "lifecycle_epoch_id",
        "main_health_window",
        "lifecycle_health",
        "operational_aggregates",
        "checks",
        "permissions",
        "evidence_boundary",
        PORTABLE_CANONICAL_FIELD,
    }
)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_TOKEN_RE_TEMPLATE: Final = r"(?:^|\s){name}=([^\s]+)"
MAX_RECEIPT_BYTES: Final = 8 << 20


class PostLifecycleLiveHealthError(RuntimeError):
    """Raised when post-lifecycle live health is not exact and self-validating."""


def _now() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _utc_datetime(value: Any, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PostLifecycleLiveHealthError(f"{label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PostLifecycleLiveHealthError(f"{label} is not timezone-aware")
    return parsed.astimezone(UTC)


def _iso_from_seconds(value: Any, label: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PostLifecycleLiveHealthError(f"{label} is not numeric")
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0.0:
        raise PostLifecycleLiveHealthError(f"{label} is invalid")
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat().replace("+00:00", "Z")


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise PostLifecycleLiveHealthError(f"{label} is not a lowercase SHA256")
    return normalized


def _strict_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise PostLifecycleLiveHealthError(f"{label} is not an integer >= {minimum}")
    return value


def _finite_nonnegative(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise PostLifecycleLiveHealthError(f"{label} is not numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PostLifecycleLiveHealthError(f"{label} is not numeric") from exc
    if not math.isfinite(result) or result < 0.0:
        raise PostLifecycleLiveHealthError(f"{label} is not finite and nonnegative")
    return result


def _document_sha256(payload: Mapping[str, Any], canonical_field: str) -> str:
    return resource_v8.document_sha256(payload, canonical_field)


def _content_projection(
    value: Any,
    label: str,
    *,
    allowed_modes: frozenset[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != CONTENT_BINDING_FIELDS:
        raise PostLifecycleLiveHealthError(f"{label} exact-seven fields drifted")
    result = {name: value.get(name) for name in CONTENT_BINDING_FIELDS}
    _require_sha256(result["file_sha256"], f"{label} file")
    _require_sha256(result["canonical_sha256"], f"{label} canonical")
    canonical_field = result["canonical_field"]
    if (
        not isinstance(result["schema_version"], str)
        or not result["schema_version"]
        or not isinstance(result["status"], (str, type(None)))
        or not isinstance(canonical_field, str)
        or not canonical_field
        or not isinstance(result["size_bytes"], int)
        or isinstance(result["size_bytes"], bool)
        or result["size_bytes"] <= 0
        or result["mode"] not in allowed_modes
    ):
        raise PostLifecycleLiveHealthError(f"{label} exact-seven identity is malformed")
    return result


def _private_binding(
    path: Path,
    *,
    label: str,
    canonical_field: str,
    schema: str,
    status: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        opened = active_capture_v8._open_private_json(path, label)  # noqa: SLF001
    except Exception as exc:
        raise PostLifecycleLiveHealthError(f"{label} could not be opened") from exc
    payload = dict(opened.payload)
    canonical = _require_sha256(payload.get(canonical_field), f"{label} canonical")
    if (
        payload.get("schema_version") != schema
        or payload.get("status") != status
        or canonical != _document_sha256(payload, canonical_field)
    ):
        raise PostLifecycleLiveHealthError(f"{label} content identity drifted")
    return payload, {
        "schema_version": schema,
        "status": status,
        "file_sha256": hashlib.sha256(opened.raw).hexdigest(),
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
        "size_bytes": len(opened.raw),
        "mode": "0600",
    }


def _activation_context(
    *,
    active_capture_path: Path,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    config_correction_path: Path,
    live_log_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        validated = active_capture_v8.validate_active_capture(
            active_capture_path,
            runtime_repository_root=runtime_repository_root,
            direct_release_path=direct_release_path,
            resource_receipt_path=resource_receipt_path,
            config_correction_path=config_correction_path,
            live_log_path=live_log_path,
        )
    except Exception as exc:
        raise PostLifecycleLiveHealthError("activation capture is invalid") from exc
    reopened, binding = _private_binding(
        active_capture_path,
        label="activation capture",
        canonical_field=active_capture_v8.CANONICAL_FIELD,
        schema=active_capture_v8.SCHEMA_VERSION,
        status=active_capture_v8.STATUS,
    )
    if reopened != validated:
        raise PostLifecycleLiveHealthError("activation capture changed during validation")
    return validated, binding


def _lifecycle_context(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        payload, binding = lifecycle_io._validate_lifecycle_admission(path)  # noqa: SLF001
    except Exception as exc:
        raise PostLifecycleLiveHealthError("lifecycle admission is invalid") from exc
    content = _content_projection(
        {name: binding.get(name) for name in CONTENT_BINDING_FIELDS},
        "lifecycle admission",
        allowed_modes=frozenset({"0644"}),
    )
    sources = binding.get("runtime_code_files")
    if (
        content["schema_version"] != lifecycle_io.LIFECYCLE_SCHEMA
        or content["status"] is not None
        or content["canonical_field"] != "admission_identity_sha256"
        or not isinstance(sources, Mapping)
        or dict(sources) != EXPECTED_LIFECYCLE_SOURCE_SHA256
        or binding.get("config_sha256") != active_capture_v8.ACTIVE_CONFIG_SHA256
        or not str(binding.get("baseline_epoch_id", "")).startswith("prospective-")
        or _strict_int(payload.get("admitted_ts_ns"), "lifecycle admitted timestamp", minimum=1)
        <= 0
    ):
        raise PostLifecycleLiveHealthError("lifecycle admission identity/runtime sources drifted")
    return payload, {**dict(binding), **content}


def _startup_sources(runtime: Any) -> tuple[str, dict[str, str]]:
    if not isinstance(runtime, Mapping):
        raise PostLifecycleLiveHealthError("activation runtime identity is missing")
    startup = runtime.get("startup_attestation")
    checkout = startup.get("running_checkout") if isinstance(startup, Mapping) else None
    rows = checkout.get("runtime_source_files") if isinstance(checkout, Mapping) else None
    manifest = (
        checkout.get("runtime_source_manifest_sha256") if isinstance(checkout, Mapping) else None
    )
    if not isinstance(rows, list) or not rows:
        raise PostLifecycleLiveHealthError("activation startup source rows are missing")
    files: dict[str, str] = {}
    for row in rows:
        if (
            not isinstance(row, Mapping)
            or not isinstance(row.get("path"), str)
            or not row["path"]
            or row.get("matches_head_blob") is not True
            or row["path"] in files
        ):
            raise PostLifecycleLiveHealthError("activation startup source row drifted")
        files[row["path"]] = _require_sha256(
            row.get("working_file_sha256"), f"startup source {row['path']}"
        )
    if files != EXPECTED_STARTUP_SOURCE_SHA256:
        raise PostLifecycleLiveHealthError("activation startup source exact10 drifted")
    return _require_sha256(manifest, "startup source manifest"), files


def _all_active_sources(process: Mapping[str, Any]) -> None:
    rows = process.get("runtime_source_files")
    if not isinstance(rows, Mapping) or set(rows) != set(
        resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256
    ):
        raise PostLifecycleLiveHealthError("activation all-source exact15 roles drifted")
    observed: dict[str, str] = {}
    for role, frozen in resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items():
        row = rows.get(role)
        relative = str(frozen["path"])
        digest = str(frozen["sha256"])
        if (
            not isinstance(row, Mapping)
            or row.get("role") != role
            or row.get("repository_relative_path") != relative
            or row.get("sha256") != digest
            or row.get("active_working_matches_direct_successor") is not True
            or row.get("direct_successor_commit_blob_matches") is not True
            or row.get("resource_v8_binding_matches") is not True
        ):
            raise PostLifecycleLiveHealthError(f"activation source role drifted: {role}")
        observed[relative] = digest
    if observed != EXPECTED_ALL_RUNTIME_SOURCE_SHA256:
        raise PostLifecycleLiveHealthError("activation all-source exact15 map drifted")


def _active_projection(
    active: Mapping[str, Any], lifecycle_binding: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    process = active.get("active_process")
    runtime = active.get("runtime_identity")
    health = active.get("active_health_window")
    if (
        not isinstance(process, Mapping)
        or not isinstance(runtime, Mapping)
        or not isinstance(health, Mapping)
    ):
        raise PostLifecycleLiveHealthError("activation process/runtime/health is missing")
    _all_active_sources(process)
    startup_manifest, startup_sources = _startup_sources(runtime)
    process_identity = _require_sha256(
        process.get("canonical_process_identity_sha256"), "activation process identity"
    )
    stable_identity = _require_sha256(
        health.get("active_process_stable_identity_sha256"), "activation stable process"
    )
    expected_stable = resource_v8.canonical_sha256(
        active_capture_v8._stable_process_projection(process)  # noqa: SLF001
    )
    runtime_file = _require_sha256(
        active.get("runtime_identity_file_sha256"), "activation runtime identity file"
    )
    runtime_canonical = resource_v8.canonical_sha256(runtime)
    authority_content = _content_projection(
        active.get("runtime_authority"),
        "activation release-v3 authority",
        allowed_modes=frozenset({"0600"}),
    )
    pid = _strict_int(process.get("pid"), "activation PID", minimum=1)
    start = _strict_int(process.get("pid_start_ticks"), "activation start ticks", minimum=1)
    activation_rows = health.get("rows")
    admitted_s = int(lifecycle_binding["admitted_ts_ns"]) / 1_000_000_000
    if (
        stable_identity != expected_stable
        or process.get("config_sha256") != active_capture_v8.ACTIVE_CONFIG_SHA256
        or lifecycle_binding.get("config_sha256") != process.get("config_sha256")
        or process.get("runtime_identity_file_sha256") != runtime_file
        or authority_content["schema_version"] != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_SCHEMA
        or authority_content["status"] != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_STATUS
        or authority_content["file_sha256"]
        != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or authority_content["canonical_sha256"]
        != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or not isinstance(activation_rows, list)
        or len(activation_rows) != 2
        or float(activation_rows[1].get("main_wall_timestamp_s", math.inf)) >= admitted_s
    ):
        raise PostLifecycleLiveHealthError(
            "activation/release/lifecycle chronology cross-binding drifted"
        )
    projected_process = {
        "pid": pid,
        "pid_start_ticks": start,
        "process_identity_sha256": process_identity,
        "stable_process_identity_sha256": stable_identity,
        "config_sha256": active_capture_v8.ACTIVE_CONFIG_SHA256,
        "runtime_identity_file_sha256": runtime_file,
        "runtime_identity_canonical_sha256": runtime_canonical,
        "runtime_source_manifest_sha256": startup_manifest,
        "runtime_source_files": startup_sources,
        "release_file_sha256": authority_content["file_sha256"],
        "release_canonical_sha256": authority_content["canonical_sha256"],
    }
    runtime_authority = {
        **authority_content,
        "execution": dict(RUNTIME_EXECUTION),
        "runtime_authority": True,
    }
    return projected_process, runtime_authority


def _same_stable_process(observed: Mapping[str, Any], activation: Mapping[str, Any]) -> bool:
    return active_capture_v8._stable_process_projection(  # noqa: SLF001
        observed
    ) == active_capture_v8._stable_process_projection(activation)  # noqa: SLF001


def _open_regular_log(path: Path) -> tuple[Path, int, os.stat_result]:
    candidate = path.expanduser().absolute()
    try:
        lexical = os.lstat(candidate)
    except OSError as exc:
        raise PostLifecycleLiveHealthError("live log lexical path could not be stated") from exc
    if stat.S_ISLNK(lexical.st_mode) or not stat.S_ISREG(lexical.st_mode):
        raise PostLifecycleLiveHealthError("live log is not a lexical regular non-symlink")
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise PostLifecycleLiveHealthError(
            "live log could not be opened without following"
        ) from exc
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or (opened.st_dev, opened.st_ino) != (lexical.st_dev, lexical.st_ino)
    ):
        os.close(descriptor)
        raise PostLifecycleLiveHealthError("live log lexical/open identity drifted")
    return candidate, descriptor, opened


def _assert_log_identity(path: Path, descriptor: int, device: int, inode: int) -> os.stat_result:
    try:
        lexical = os.lstat(path)
        opened = os.fstat(descriptor)
    except OSError as exc:
        raise PostLifecycleLiveHealthError("live log identity disappeared") from exc
    if (
        stat.S_ISLNK(lexical.st_mode)
        or not stat.S_ISREG(lexical.st_mode)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or (lexical.st_dev, lexical.st_ino) != (device, inode)
        or (opened.st_dev, opened.st_ino) != (device, inode)
    ):
        raise PostLifecycleLiveHealthError("live log device/inode changed")
    return opened


def _token(line: str, name: str, label: str) -> str:
    match = re.search(_TOKEN_RE_TEMPLATE.format(name=re.escape(name)), line)
    if match is None:
        raise PostLifecycleLiveHealthError(f"{label} token is missing")
    return match.group(1)


def _numeric_token(line: str, name: str, label: str) -> float:
    try:
        value = float(_token(line, name, label))
    except ValueError as exc:
        raise PostLifecycleLiveHealthError(f"{label} token is not numeric") from exc
    if not math.isfinite(value):
        raise PostLifecycleLiveHealthError(f"{label} token is non-finite")
    return value


def _integer_token(line: str, name: str, label: str, *, minimum: int = 0) -> int:
    raw = _token(line, name, label)
    if re.fullmatch(r"-?\d+", raw) is None:
        raise PostLifecycleLiveHealthError(f"{label} token is not an integer")
    value = int(raw)
    if value < minimum:
        raise PostLifecycleLiveHealthError(f"{label} token is below {minimum}")
    return value


def _main_event(line_bytes: bytes, *, offset: int, generation: int) -> dict[str, Any]:
    try:
        line = line_bytes.decode("utf-8", errors="strict")
        parsed = resource_v8._parse_main_health(line, generation=generation)  # noqa: SLF001
        projection = active_capture_v8._validate_health_projection(  # noqa: SLF001
            active_capture_v8._health_projection(parsed)  # noqa: SLF001
        )
    except Exception as exc:
        raise PostLifecycleLiveHealthError("fresh main HEALTH line is not exact") from exc
    position = _numeric_token(line, "pos", "main position")
    orders = _integer_token(line, "orders", "main open-order count")
    evaluations = _integer_token(line, "buyE3CooldownEval", "BUY E3 evaluation count")
    decision_p99 = _numeric_token(line, "buyE3CooldownDecisionP99Us", "BUY E3 decision p99")
    if decision_p99 < 0.0:
        raise PostLifecycleLiveHealthError("BUY E3 decision p99 is negative")
    return {
        "kind": "main",
        "row": {
            "fresh_generation": generation,
            "line_offset_bytes": offset,
            "line_size_bytes": len(line_bytes),
            "line_sha256": hashlib.sha256(line_bytes).hexdigest(),
            "main_wall_timestamp_s": parsed["wall_timestamp_s"],
            "projection": projection,
        },
        "safe_operational": {
            "position_flat": position == 0.0,
            "open_order_count": orders,
            "buy_e3_evaluation_count": evaluations,
            "buy_e3_decision_p99_us": decision_p99,
        },
    }


def _lifecycle_event(line_bytes: bytes, *, offset: int, generation: int) -> dict[str, Any]:
    try:
        line = line_bytes.decode("utf-8", errors="strict")
        parsed = resource_v8._parse_lifecycle_health(  # noqa: SLF001
            line,
            generation=generation,
        )
        timestamp = resource_v8._LOG_TS_RE.match(line)  # noqa: SLF001
    except Exception as exc:
        raise PostLifecycleLiveHealthError("fresh lifecycle HEALTH line is not exact") from exc
    if timestamp is None:
        raise PostLifecycleLiveHealthError("fresh lifecycle HEALTH timestamp is missing")
    wall_s = (
        datetime.strptime(timestamp.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).timestamp()
    )
    enqueue_p99 = _numeric_token(line, "enqueueP99Us", "lifecycle enqueue p99")
    write_p99 = _numeric_token(line, "writeP99Ms", "lifecycle write p99")
    max_rss = _numeric_token(line, "maxRssMb", "lifecycle max RSS")
    if min(enqueue_p99, write_p99, max_rss) < 0.0:
        raise PostLifecycleLiveHealthError("lifecycle operational counters are negative")
    counters = parsed["counter_values"]
    return {
        "kind": "lifecycle",
        "row": {
            "fresh_generation": generation,
            "line_offset_bytes": offset,
            "line_size_bytes": len(line_bytes),
            "line_sha256": hashlib.sha256(line_bytes).hexdigest(),
            "wall_timestamp_s": wall_s,
            "order_lifecycle_v2_drops": counters["orderLifecycleV2Drops"],
            "order_lifecycle_v2_errors": counters["orderLifecycleV2Errors"],
            "enqueue_latency_p99_us": enqueue_p99,
            "write_latency_p99_ms": write_p99,
            "process_max_rss_mib": max_rss,
        },
    }


def _scan_complete_lines(
    payload: bytes,
    *,
    start_offset: int,
    main_generation: int = 0,
    lifecycle_generation: int = 0,
) -> tuple[list[dict[str, Any]], bytes, int, int, int]:
    events: list[dict[str, Any]] = []
    cursor = start_offset
    pending = b""
    for line_bytes in payload.splitlines(keepends=True):
        if not line_bytes.endswith(b"\n"):
            pending = line_bytes
            break
        if resource_v8._HEALTH_MARKER.encode("ascii") in line_bytes:  # noqa: SLF001
            main_generation += 1
            events.append(_main_event(line_bytes, offset=cursor, generation=main_generation))
        elif resource_v8._LIFECYCLE_MARKER.encode("ascii") in line_bytes:  # noqa: SLF001
            lifecycle_generation += 1
            events.append(
                _lifecycle_event(
                    line_bytes,
                    offset=cursor,
                    generation=lifecycle_generation,
                )
            )
        cursor += len(line_bytes)
    return events, pending, cursor, main_generation, lifecycle_generation


class FreshHealthTail:
    """Read only complete lines appended after a constructor EOF boundary."""

    def __init__(self, path: Path) -> None:
        self.path, self._descriptor, metadata = _open_regular_log(path)
        self.device = metadata.st_dev
        self.inode = metadata.st_ino
        self.boundary_offset_bytes = metadata.st_size
        self.boundary_captured_utc = _now()
        if metadata.st_size and os.pread(self._descriptor, 1, metadata.st_size - 1) != b"\n":
            self.close()
            raise PostLifecycleLiveHealthError("live log EOF is not a complete-line boundary")
        self._read_offset = metadata.st_size
        self._buffer = b""
        self._buffer_start = metadata.st_size
        self._main_generation = 0
        self._lifecycle_generation = 0

    def close(self) -> None:
        descriptor = getattr(self, "_descriptor", -1)
        if descriptor >= 0:
            os.close(descriptor)
            self._descriptor = -1

    def __enter__(self) -> FreshHealthTail:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def poll(self) -> list[dict[str, Any]]:
        metadata = _assert_log_identity(
            self.path,
            self._descriptor,
            self.device,
            self.inode,
        )
        if metadata.st_size < self._read_offset:
            raise PostLifecycleLiveHealthError("live log truncated during post-lifecycle capture")
        if metadata.st_size > self._read_offset:
            chunk = os.pread(
                self._descriptor,
                metadata.st_size - self._read_offset,
                self._read_offset,
            )
            if len(chunk) != metadata.st_size - self._read_offset:
                raise PostLifecycleLiveHealthError("live log short-read during capture")
            self._read_offset = metadata.st_size
            self._buffer += chunk
        events, pending, cursor, main_generation, lifecycle_generation = _scan_complete_lines(
            self._buffer,
            start_offset=self._buffer_start,
            main_generation=self._main_generation,
            lifecycle_generation=self._lifecycle_generation,
        )
        consumed = cursor - self._buffer_start
        self._buffer = pending
        self._buffer_start += consumed
        self._main_generation = main_generation
        self._lifecycle_generation = lifecycle_generation
        return events

    def interval_sha256(self, end_offset: int) -> str:
        if end_offset <= self.boundary_offset_bytes:
            raise PostLifecycleLiveHealthError("post-lifecycle log interval is empty")
        _assert_log_identity(self.path, self._descriptor, self.device, self.inode)
        payload = os.pread(
            self._descriptor,
            end_offset - self.boundary_offset_bytes,
            self.boundary_offset_bytes,
        )
        if len(payload) != end_offset - self.boundary_offset_bytes or not payload.endswith(b"\n"):
            raise PostLifecycleLiveHealthError("post-lifecycle log interval is incomplete")
        return hashlib.sha256(payload).hexdigest()


class ProcSafetySampler:
    """Collect only host/process safety counters; never read economic state."""

    def __init__(self, proc_root: Path, pid: int) -> None:
        self.proc_root = proc_root
        self.pid = pid

    @staticmethod
    def _key_values(path: Path, label: str) -> dict[str, str]:
        try:
            rows = path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise PostLifecycleLiveHealthError(f"{label} could not be read") from exc
        result: dict[str, str] = {}
        for row in rows:
            parts = row.replace(":", " ").split()
            if len(parts) >= 2:
                result[parts[0]] = parts[1]
        return result

    def sample(self) -> dict[str, Any]:
        meminfo = self._key_values(self.proc_root / "meminfo", "proc meminfo")
        status_values = self._key_values(
            self.proc_root / str(self.pid) / "status", "live process status"
        )
        vmstat = self._key_values(self.proc_root / "vmstat", "proc vmstat")
        try:
            mem_mib = int(meminfo["MemAvailable"]) / 1024.0
            rss_mib = int(status_values["VmRSS"]) / 1024.0
            oom_kill = int(vmstat["oom_kill"])
            swap_in = int(vmstat["pswpin"])
            swap_out = int(vmstat["pswpout"])
        except (KeyError, ValueError) as exc:
            raise PostLifecycleLiveHealthError("proc safety counters are malformed") from exc
        if min(mem_mib, rss_mib, oom_kill, swap_in, swap_out) < 0:
            raise PostLifecycleLiveHealthError("proc safety counters are negative")
        return {
            "mem_available_mib": mem_mib,
            "live_rss_mib": rss_mib,
            "oom_kill": oom_kill,
            "swap_in": swap_in,
            "swap_out": swap_out,
        }


def _resource_aggregates(samples: list[Mapping[str, Any]]) -> dict[str, Any]:
    if len(samples) < 2:
        raise PostLifecycleLiveHealthError("post-lifecycle resource samples are incomplete")
    first = samples[0]
    last = samples[-1]
    result = {
        "sample_count": len(samples),
        "min_mem_available_mib": min(float(row["mem_available_mib"]) for row in samples),
        "max_live_rss_mib": max(float(row["live_rss_mib"]) for row in samples),
        "oom_window_delta": int(last["oom_kill"]) - int(first["oom_kill"]),
        "swap_in_window_delta": int(last["swap_in"]) - int(first["swap_in"]),
        "swap_out_window_delta": int(last["swap_out"]) - int(first["swap_out"]),
    }
    if (
        result["min_mem_available_mib"] <= 0.0
        or result["max_live_rss_mib"] <= 0.0
        or result["oom_window_delta"] != 0
        or result["swap_in_window_delta"] != 0
        or result["swap_out_window_delta"] != 0
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle resource safety counters failed")
    return result


def _selected_events(
    events: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    main: list[dict[str, Any]] = []
    lifecycle: dict[str, Any] | None = None
    for event in events:
        if event.get("kind") == "main" and len(main) < 2:
            main.append(dict(event))
        elif event.get("kind") == "lifecycle" and lifecycle is None:
            lifecycle = dict(event)
    if len(main) != 2 or lifecycle is None:
        raise PostLifecycleLiveHealthError("fresh main/lifecycle HEALTH set is incomplete")
    return main, lifecycle


def _health_aggregates(
    main_events: Sequence[Mapping[str, Any]], lifecycle_event: Mapping[str, Any]
) -> dict[str, Any]:
    first = main_events[0]["safe_operational"]
    second = main_events[1]["safe_operational"]
    lifecycle = lifecycle_event["row"]
    decision_count = int(second["buy_e3_evaluation_count"]) - int(first["buy_e3_evaluation_count"])
    if decision_count < 0:
        raise PostLifecycleLiveHealthError("BUY E3 evaluation count regressed")
    return {
        "latency": {
            "decision_sample_count": decision_count,
            "decision_p99_us": (
                float(second["buy_e3_decision_p99_us"]) if decision_count > 0 else None
            ),
            "callback_sample_count": 0,
            "callback_p99_us": None,
            "lifecycle_enqueue_p99_us": float(lifecycle["enqueue_latency_p99_us"]),
            "lifecycle_write_p99_ms": float(lifecycle["write_latency_p99_ms"]),
            "small_sample_disclosed": True,
            "strategy_result_authority": False,
        },
        "position": {
            "position_probe_completed": True,
            "aggregate_position_flat": bool(first["position_flat"] and second["position_flat"]),
            "open_order_count": int(second["open_order_count"]),
            "economic_values_persisted": False,
        },
    }


def _capture_fresh_health(
    *,
    tail: FreshHealthTail,
    expected_pid: int,
    expected_start_ticks: int,
    stable_process_identity_sha256: str,
    identity_supplier: Callable[[], tuple[int, int]],
    sample_supplier: Callable[[], Mapping[str, Any]],
    timeout_s: float,
    poll_interval_s: float,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    if timeout_s <= 0.0 or poll_interval_s <= 0.0:
        raise PostLifecycleLiveHealthError("post-lifecycle timeout/poll interval is invalid")
    expected_key = (expected_pid, expected_start_ticks)

    def assert_identity() -> None:
        if identity_supplier() != expected_key:
            raise PostLifecycleLiveHealthError(
                "active PID/start ticks changed during post-lifecycle polling"
            )

    assert_identity()
    samples: list[Mapping[str, Any]] = [dict(sample_supplier())]
    observed_events: list[dict[str, Any]] = []
    selected_main: list[dict[str, Any]] = []
    selected_lifecycle: dict[str, Any] | None = None
    deadline = monotonic() + timeout_s
    while monotonic() <= deadline and (len(selected_main) < 2 or selected_lifecycle is None):
        assert_identity()
        samples.append(dict(sample_supplier()))
        new_events = tail.poll()
        assert_identity()
        samples.append(dict(sample_supplier()))
        for event in new_events:
            observed_events.append(event)
            if event["kind"] == "main" and len(selected_main) < 2:
                selected_main.append(event)
            elif event["kind"] == "lifecycle" and selected_lifecycle is None:
                selected_lifecycle = event
        if len(selected_main) < 2 or selected_lifecycle is None:
            sleep(min(poll_interval_s, max(0.0, deadline - monotonic())))
    assert_identity()
    samples.append(dict(sample_supplier()))
    if len(selected_main) != 2 or selected_lifecycle is None:
        raise PostLifecycleLiveHealthError(
            "two fresh main HEALTH rows and one lifecycle HEALTH row were not observed"
        )
    first_row = selected_main[0]["row"]
    second_row = selected_main[1]["row"]
    if (
        first_row["fresh_generation"] != 1
        or second_row["fresh_generation"] != 2
        or float(second_row["main_wall_timestamp_s"]) <= float(first_row["main_wall_timestamp_s"])
        or int(second_row["projection"]["boolean_cooldown_updates"])
        <= int(first_row["projection"]["boolean_cooldown_updates"])
        or selected_lifecycle["row"]["fresh_generation"] != 1
        or selected_lifecycle["row"]["order_lifecycle_v2_drops"] != 0
        or selected_lifecycle["row"]["order_lifecycle_v2_errors"] != 0
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle HEALTH chronology/safety drifted")
    selected_end = max(
        int(event["row"]["line_offset_bytes"]) + int(event["row"]["line_size_bytes"])
        for event in (*selected_main, selected_lifecycle)
    )
    interval_sha = tail.interval_sha256(selected_end)
    main_window = {
        "schema_version": active_capture_v8.HEALTH_WINDOW_SCHEMA,
        "status": active_capture_v8.HEALTH_WINDOW_STATUS,
        "boundary_offset_bytes": tail.boundary_offset_bytes,
        "active_pid": expected_pid,
        "active_pid_start_ticks": expected_start_ticks,
        "active_process_stable_identity_sha256": _require_sha256(
            stable_process_identity_sha256, "active stable process identity"
        ),
        "rows": [dict(first_row), dict(second_row)],
        "checks": {
            "constructor_boundary_only": True,
            "two_consecutive_fresh_main_health_rows": True,
            "same_pid_and_start_ticks_before_between_after": True,
            "sell_owner_enabled_both_rows": True,
            "buy_e3_enabled_both_rows": True,
            "external_sources_absolute_zero_both_rows": True,
            "global_flow_explicit_disabled_error_and_backend_zero_both_rows": True,
            "global_reference_explicit_disabled_error_and_state_zero_both_rows": True,
        },
    }
    log_capture = {
        "log_path_provenance": str(tail.path),
        "device": tail.device,
        "inode": tail.inode,
        "boundary_offset_bytes": tail.boundary_offset_bytes,
        "boundary_captured_utc": tail.boundary_captured_utc,
        "scan_end_offset_bytes": selected_end,
        "scan_interval_sha256": interval_sha,
        "scan_completed_utc": _now(),
    }
    health_aggregates = _health_aggregates(selected_main, selected_lifecycle)
    operational = {
        "resource": _resource_aggregates(samples),
        **health_aggregates,
    }
    return log_capture, main_window, dict(selected_lifecycle["row"]), operational


def _validate_main_window_content(raw: Any, active_process: Mapping[str, Any]) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "status",
        "boundary_offset_bytes",
        "active_pid",
        "active_pid_start_ticks",
        "active_process_stable_identity_sha256",
        "rows",
        "checks",
    }
    if not isinstance(raw, Mapping) or set(raw) != expected_fields:
        raise PostLifecycleLiveHealthError("post-lifecycle main-health fields drifted")
    window = dict(raw)
    rows = window.get("rows")
    expected_checks = {
        "constructor_boundary_only": True,
        "two_consecutive_fresh_main_health_rows": True,
        "same_pid_and_start_ticks_before_between_after": True,
        "sell_owner_enabled_both_rows": True,
        "buy_e3_enabled_both_rows": True,
        "external_sources_absolute_zero_both_rows": True,
        "global_flow_explicit_disabled_error_and_backend_zero_both_rows": True,
        "global_reference_explicit_disabled_error_and_state_zero_both_rows": True,
    }
    if (
        window.get("schema_version") != active_capture_v8.HEALTH_WINDOW_SCHEMA
        or window.get("status") != active_capture_v8.HEALTH_WINDOW_STATUS
        or window.get("active_pid") != active_process.get("pid")
        or window.get("active_pid_start_ticks") != active_process.get("pid_start_ticks")
        or window.get("active_process_stable_identity_sha256")
        != active_process.get("stable_process_identity_sha256")
        or type(window.get("boundary_offset_bytes")) is not int
        or window["boundary_offset_bytes"] < 0
        or not isinstance(rows, list)
        or len(rows) != 2
        or window.get("checks") != expected_checks
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle main-health identity drifted")
    previous_wall = float("-inf")
    previous_updates = -1
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping) or set(row) != {
            "fresh_generation",
            "line_offset_bytes",
            "line_size_bytes",
            "line_sha256",
            "main_wall_timestamp_s",
            "projection",
        }:
            raise PostLifecycleLiveHealthError("post-lifecycle main-health row fields drifted")
        projection = row.get("projection")
        try:
            normalized = active_capture_v8._validate_health_projection(projection)  # noqa: SLF001
        except Exception as exc:
            raise PostLifecycleLiveHealthError(
                "post-lifecycle main-health no-shadow semantics drifted"
            ) from exc
        wall = _finite_nonnegative(row.get("main_wall_timestamp_s"), "main-health wall time")
        updates = _strict_int(
            normalized.get("boolean_cooldown_updates"),
            "main-health BUY/SELL update count",
        )
        if (
            row.get("fresh_generation") != index
            or _strict_int(row.get("line_offset_bytes"), "main-health offset")
            < window["boundary_offset_bytes"]
            or _strict_int(row.get("line_size_bytes"), "main-health size", minimum=1) <= 0
            or _require_sha256(row.get("line_sha256"), "main-health line") != row.get("line_sha256")
            or wall <= previous_wall
            or updates <= previous_updates
        ):
            raise PostLifecycleLiveHealthError("post-lifecycle main-health row drifted")
        previous_wall = wall
        previous_updates = updates
    return window


def _validate_lifecycle_health_content(raw: Any, boundary: int) -> dict[str, Any]:
    fields = {
        "fresh_generation",
        "line_offset_bytes",
        "line_size_bytes",
        "line_sha256",
        "wall_timestamp_s",
        "order_lifecycle_v2_drops",
        "order_lifecycle_v2_errors",
        "enqueue_latency_p99_us",
        "write_latency_p99_ms",
        "process_max_rss_mib",
    }
    if not isinstance(raw, Mapping) or set(raw) != fields:
        raise PostLifecycleLiveHealthError("post-lifecycle lifecycle-health fields drifted")
    row = dict(raw)
    if (
        row.get("fresh_generation") != 1
        or _strict_int(row.get("line_offset_bytes"), "lifecycle-health offset") < boundary
        or _strict_int(row.get("line_size_bytes"), "lifecycle-health size", minimum=1) <= 0
        or _require_sha256(row.get("line_sha256"), "lifecycle-health line")
        != row.get("line_sha256")
        or _finite_nonnegative(row.get("wall_timestamp_s"), "lifecycle-health wall time") <= 0
        or row.get("order_lifecycle_v2_drops") != 0
        or row.get("order_lifecycle_v2_errors") != 0
        or _finite_nonnegative(row.get("enqueue_latency_p99_us"), "lifecycle enqueue p99") < 0
        or _finite_nonnegative(row.get("write_latency_p99_ms"), "lifecycle write p99") < 0
        or _finite_nonnegative(row.get("process_max_rss_mib"), "lifecycle max RSS") < 0
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle lifecycle-health semantics drifted")
    return row


def _validate_operational_aggregates(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != {"resource", "latency", "position"}:
        raise PostLifecycleLiveHealthError("post-lifecycle operational aggregates drifted")
    resource = raw.get("resource")
    latency = raw.get("latency")
    position = raw.get("position")
    if not isinstance(resource, Mapping) or set(resource) != {
        "sample_count",
        "min_mem_available_mib",
        "max_live_rss_mib",
        "oom_window_delta",
        "swap_in_window_delta",
        "swap_out_window_delta",
    }:
        raise PostLifecycleLiveHealthError("post-lifecycle resource aggregates drifted")
    if (
        _strict_int(resource.get("sample_count"), "resource sample count", minimum=2) < 2
        or _finite_nonnegative(resource.get("min_mem_available_mib"), "MemAvailable") <= 0
        or _finite_nonnegative(resource.get("max_live_rss_mib"), "live RSS") <= 0
        or resource.get("oom_window_delta") != 0
        or resource.get("swap_in_window_delta") != 0
        or resource.get("swap_out_window_delta") != 0
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle resource safety aggregates failed")
    if not isinstance(latency, Mapping) or set(latency) != {
        "decision_sample_count",
        "decision_p99_us",
        "callback_sample_count",
        "callback_p99_us",
        "lifecycle_enqueue_p99_us",
        "lifecycle_write_p99_ms",
        "small_sample_disclosed",
        "strategy_result_authority",
    }:
        raise PostLifecycleLiveHealthError("post-lifecycle latency aggregates drifted")
    for prefix in ("decision", "callback"):
        count = _strict_int(latency.get(f"{prefix}_sample_count"), f"{prefix} sample count")
        p99 = latency.get(f"{prefix}_p99_us")
        if (count == 0 and p99 is not None) or (
            count > 0 and _finite_nonnegative(p99, f"{prefix} p99") < 0
        ):
            raise PostLifecycleLiveHealthError(f"post-lifecycle {prefix} p99 drifted")
    if (
        _finite_nonnegative(latency.get("lifecycle_enqueue_p99_us"), "lifecycle enqueue p99") < 0
        or _finite_nonnegative(latency.get("lifecycle_write_p99_ms"), "lifecycle write p99") < 0
        or latency.get("small_sample_disclosed") is not True
        or latency.get("strategy_result_authority") is not False
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle latency authority drifted")
    if not isinstance(position, Mapping) or set(position) != {
        "position_probe_completed",
        "aggregate_position_flat",
        "open_order_count",
        "economic_values_persisted",
    }:
        raise PostLifecycleLiveHealthError("post-lifecycle position safety projection drifted")
    if (
        position.get("position_probe_completed") is not True
        or type(position.get("aggregate_position_flat")) is not bool
        or _strict_int(position.get("open_order_count"), "open-order count") < 0
        or position.get("economic_values_persisted") is not False
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle position safety semantics drifted")
    return {
        "resource": dict(resource),
        "latency": dict(latency),
        "position": dict(position),
    }


def _portable_payload(
    *,
    generated_utc: str,
    runtime_authority: Mapping[str, Any],
    active_process: Mapping[str, Any],
    lifecycle_admission: Mapping[str, Any],
    lifecycle_epoch_id: str,
    main_health_window: Mapping[str, Any],
    lifecycle_health: Mapping[str, Any],
    operational_aggregates: Mapping[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": PORTABLE_SCHEMA_VERSION,
        "status": PORTABLE_STATUS,
        "generated_utc": generated_utc,
        "runtime_execution": dict(RUNTIME_EXECUTION),
        "runtime_authority": dict(runtime_authority),
        "active_process": dict(active_process),
        "lifecycle_admission": dict(lifecycle_admission),
        "lifecycle_epoch_id": lifecycle_epoch_id,
        "main_health_window": dict(main_health_window),
        "lifecycle_health": {
            "observed_utc": _iso_from_seconds(
                lifecycle_health["wall_timestamp_s"], "lifecycle-health wall time"
            ),
            "line_sha256": lifecycle_health["line_sha256"],
            "order_lifecycle_v2_drops": lifecycle_health["order_lifecycle_v2_drops"],
            "order_lifecycle_v2_errors": lifecycle_health["order_lifecycle_v2_errors"],
        },
        "operational_aggregates": dict(operational_aggregates),
        "checks": dict(PORTABLE_CHECKS),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[PORTABLE_CANONICAL_FIELD] = _document_sha256(payload, PORTABLE_CANONICAL_FIELD)
    return payload


def _validate_active_process_projection(raw: Any) -> dict[str, Any]:
    fields = {
        "pid",
        "pid_start_ticks",
        "process_identity_sha256",
        "stable_process_identity_sha256",
        "config_sha256",
        "runtime_identity_file_sha256",
        "runtime_identity_canonical_sha256",
        "runtime_source_manifest_sha256",
        "runtime_source_files",
        "release_file_sha256",
        "release_canonical_sha256",
    }
    if not isinstance(raw, Mapping) or set(raw) != fields:
        raise PostLifecycleLiveHealthError("post-lifecycle active process fields drifted")
    process = dict(raw)
    for name in fields - {"pid", "pid_start_ticks", "runtime_source_files"}:
        _require_sha256(process.get(name), f"post-lifecycle active {name}")
    if (
        _strict_int(process.get("pid"), "post-lifecycle active PID", minimum=1) <= 0
        or _strict_int(
            process.get("pid_start_ticks"), "post-lifecycle active start ticks", minimum=1
        )
        <= 0
        or process.get("config_sha256") != active_capture_v8.ACTIVE_CONFIG_SHA256
        or process.get("release_file_sha256")
        != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or process.get("release_canonical_sha256")
        != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or not isinstance(process.get("runtime_source_files"), Mapping)
        or dict(process["runtime_source_files"]) != EXPECTED_STARTUP_SOURCE_SHA256
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle active process identity drifted")
    return process


def _validate_runtime_authority(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != {
        *CONTENT_BINDING_FIELDS,
        "execution",
        "runtime_authority",
    }:
        raise PostLifecycleLiveHealthError("post-lifecycle runtime authority fields drifted")
    content = _content_projection(
        {name: raw.get(name) for name in CONTENT_BINDING_FIELDS},
        "post-lifecycle runtime authority",
        allowed_modes=frozenset({"0600"}),
    )
    if (
        content["schema_version"] != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_SCHEMA
        or content["status"] != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_STATUS
        or content["file_sha256"] != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or content["canonical_sha256"]
        != active_capture_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or raw.get("execution") != RUNTIME_EXECUTION
        or raw.get("runtime_authority") is not True
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle runtime authority drifted")
    return dict(raw)


def validate_portable_projection(raw: Any) -> dict[str, Any]:
    """Validate only the host-independent projection; never open a live source."""

    if not isinstance(raw, Mapping) or set(raw) != PORTABLE_FIELDS:
        raise PostLifecycleLiveHealthError("portable post-lifecycle projection fields drifted")
    payload = dict(raw)
    generated = _utc_datetime(payload.get("generated_utc"), "portable generated timestamp")
    process = _validate_active_process_projection(payload.get("active_process"))
    authority = _validate_runtime_authority(payload.get("runtime_authority"))
    lifecycle = _content_projection(
        payload.get("lifecycle_admission"),
        "portable lifecycle admission",
        allowed_modes=frozenset({"0644"}),
    )
    main = _validate_main_window_content(payload.get("main_health_window"), process)
    lifecycle_health = payload.get("lifecycle_health")
    if not isinstance(lifecycle_health, Mapping) or set(lifecycle_health) != {
        "observed_utc",
        "line_sha256",
        "order_lifecycle_v2_drops",
        "order_lifecycle_v2_errors",
    }:
        raise PostLifecycleLiveHealthError("portable lifecycle-health fields drifted")
    observed = _utc_datetime(
        lifecycle_health.get("observed_utc"), "portable lifecycle-health timestamp"
    )
    _require_sha256(lifecycle_health.get("line_sha256"), "portable lifecycle-health line")
    if (
        payload.get("schema_version") != PORTABLE_SCHEMA_VERSION
        or payload.get("status") != PORTABLE_STATUS
        or payload.get("runtime_execution") != RUNTIME_EXECUTION
        or lifecycle["schema_version"] != lifecycle_io.LIFECYCLE_SCHEMA
        or lifecycle["status"] is not None
        or lifecycle["canonical_field"] != "admission_identity_sha256"
        or not str(payload.get("lifecycle_epoch_id", "")).startswith("prospective-")
        or lifecycle_health.get("order_lifecycle_v2_drops") != 0
        or lifecycle_health.get("order_lifecycle_v2_errors") != 0
        or payload.get("checks") != PORTABLE_CHECKS
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(PORTABLE_CANONICAL_FIELD)
        != _document_sha256(payload, PORTABLE_CANONICAL_FIELD)
    ):
        raise PostLifecycleLiveHealthError("portable post-lifecycle identity drifted")
    aggregates = _validate_operational_aggregates(payload.get("operational_aggregates"))
    second_health = datetime.fromtimestamp(float(main["rows"][1]["main_wall_timestamp_s"]), tz=UTC)
    if generated < max(second_health, observed):
        raise PostLifecycleLiveHealthError("portable receipt predates its health rows")
    payload["runtime_authority"] = authority
    payload["active_process"] = process
    payload["main_health_window"] = main
    payload["operational_aggregates"] = aggregates
    return payload


def validate_content_projection(raw: Any) -> dict[str, Any]:
    """Validate receipt bytes semantically without reopening host-local sources."""

    if not isinstance(raw, Mapping) or set(raw) != TOP_LEVEL_FIELDS:
        raise PostLifecycleLiveHealthError("post-lifecycle receipt fields drifted")
    payload = dict(raw)
    generated = _utc_datetime(payload.get("generated_utc"), "post-lifecycle generated timestamp")
    activation = _content_projection(
        payload.get("activation_capture"),
        "post-lifecycle activation capture",
        allowed_modes=frozenset({"0600"}),
    )
    lifecycle = _content_projection(
        payload.get("lifecycle_admission"),
        "post-lifecycle lifecycle admission",
        allowed_modes=frozenset({"0644"}),
    )
    context = payload.get("lifecycle_context")
    if not isinstance(context, Mapping) or set(context) != {
        "admitted_ts_ns",
        "baseline_epoch_id",
        "config_sha256",
        "runtime_code_sha256",
        "runtime_source_files",
    }:
        raise PostLifecycleLiveHealthError("post-lifecycle lifecycle context drifted")
    admitted_ns = _strict_int(
        context.get("admitted_ts_ns"), "lifecycle admitted timestamp", minimum=1
    )
    process = _validate_active_process_projection(payload.get("active_process"))
    authority = _validate_runtime_authority(payload.get("runtime_authority"))
    main = _validate_main_window_content(payload.get("main_health_window"), process)
    log_capture = payload.get("log_capture")
    if not isinstance(log_capture, Mapping) or set(log_capture) != {
        "log_path_provenance",
        "device",
        "inode",
        "boundary_offset_bytes",
        "boundary_captured_utc",
        "scan_end_offset_bytes",
        "scan_interval_sha256",
        "scan_completed_utc",
    }:
        raise PostLifecycleLiveHealthError("post-lifecycle log capture fields drifted")
    log_path = str(log_capture.get("log_path_provenance", ""))
    boundary = _strict_int(log_capture.get("boundary_offset_bytes"), "log boundary")
    scan_end = _strict_int(log_capture.get("scan_end_offset_bytes"), "log scan end", minimum=1)
    boundary_captured = _utc_datetime(
        log_capture.get("boundary_captured_utc"), "log boundary timestamp"
    )
    scan_completed = _utc_datetime(log_capture.get("scan_completed_utc"), "log scan timestamp")
    if (
        not PurePosixPath(log_path).is_absolute()
        or _strict_int(log_capture.get("device"), "log device") < 0
        or _strict_int(log_capture.get("inode"), "log inode", minimum=1) <= 0
        or scan_end <= boundary
        or _require_sha256(log_capture.get("scan_interval_sha256"), "log scan interval")
        != log_capture.get("scan_interval_sha256")
        or main.get("boundary_offset_bytes") != boundary
        or scan_completed < boundary_captured
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle log capture identity drifted")
    lifecycle_health = _validate_lifecycle_health_content(payload.get("lifecycle_health"), boundary)
    aggregates = _validate_operational_aggregates(payload.get("operational_aggregates"))
    selected_rows = [*main["rows"], lifecycle_health]
    expected_scan_end = max(
        int(row["line_offset_bytes"]) + int(row["line_size_bytes"])
        for row in selected_rows
    )
    expected_portable = _portable_payload(
        generated_utc=str(payload["generated_utc"]),
        runtime_authority=authority,
        active_process=process,
        lifecycle_admission=lifecycle,
        lifecycle_epoch_id=str(context["baseline_epoch_id"]),
        main_health_window=main,
        lifecycle_health=lifecycle_health,
        operational_aggregates=aggregates,
    )
    observed_portable = validate_portable_projection(payload.get("portable_projection"))
    admitted = datetime.fromtimestamp(admitted_ns / 1_000_000_000, tz=UTC)
    first_main = datetime.fromtimestamp(float(main["rows"][0]["main_wall_timestamp_s"]), tz=UTC)
    second_main = datetime.fromtimestamp(float(main["rows"][1]["main_wall_timestamp_s"]), tz=UTC)
    lifecycle_observed = datetime.fromtimestamp(float(lifecycle_health["wall_timestamp_s"]), tz=UTC)
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != OWNER
        or payload.get("status") != STATUS
        or activation["schema_version"] != active_capture_v8.SCHEMA_VERSION
        or activation["status"] != active_capture_v8.STATUS
        or activation["canonical_field"] != active_capture_v8.CANONICAL_FIELD
        or lifecycle["schema_version"] != lifecycle_io.LIFECYCLE_SCHEMA
        or lifecycle["status"] is not None
        or lifecycle["canonical_field"] != "admission_identity_sha256"
        or payload.get("runtime_execution") != RUNTIME_EXECUTION
        or context.get("baseline_epoch_id") != observed_portable.get("lifecycle_epoch_id")
        or not str(context.get("baseline_epoch_id", "")).startswith("prospective-")
        or context.get("config_sha256") != active_capture_v8.ACTIVE_CONFIG_SHA256
        or _require_sha256(context.get("runtime_code_sha256"), "lifecycle runtime code")
        != context.get("runtime_code_sha256")
        or process.get("config_sha256") != context.get("config_sha256")
        or not isinstance(context.get("runtime_source_files"), Mapping)
        or dict(context["runtime_source_files"]) != EXPECTED_LIFECYCLE_SOURCE_SHA256
        or observed_portable != expected_portable
        or scan_end != expected_scan_end
        or payload.get("checks") != CHECKS
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(CANONICAL_FIELD) != _document_sha256(payload, CANONICAL_FIELD)
        or not (admitted < boundary_captured <= scan_completed <= generated)
        or not (admitted < first_main < second_main <= generated)
        or not (admitted < lifecycle_observed <= generated)
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle receipt content identity drifted")
    payload["runtime_authority"] = authority
    payload["active_process"] = process
    payload["main_health_window"] = main
    payload["lifecycle_health"] = lifecycle_health
    payload["operational_aggregates"] = aggregates
    payload["portable_projection"] = observed_portable
    return payload


def portable_projection(payload: Any) -> dict[str, Any]:
    """Return the validated path-free projection for a source-frozen consumer."""

    return dict(validate_content_projection(payload)["portable_projection"])


def _revalidate_log_bytes(
    *,
    live_log_path: Path,
    log_capture: Mapping[str, Any],
    main_health_window: Mapping[str, Any],
    lifecycle_health: Mapping[str, Any],
    operational_aggregates: Mapping[str, Any],
) -> None:
    path, descriptor, metadata = _open_regular_log(live_log_path)
    try:
        if (
            str(path) != log_capture.get("log_path_provenance")
            or metadata.st_dev != log_capture.get("device")
            or metadata.st_ino != log_capture.get("inode")
        ):
            raise PostLifecycleLiveHealthError("live log differs from receipt provenance")
        boundary = int(log_capture["boundary_offset_bytes"])
        end = int(log_capture["scan_end_offset_bytes"])
        if metadata.st_size < end:
            raise PostLifecycleLiveHealthError("live log no longer covers receipt interval")
        interval = os.pread(descriptor, end - boundary, boundary)
        if (
            len(interval) != end - boundary
            or not interval.endswith(b"\n")
            or hashlib.sha256(interval).hexdigest() != log_capture.get("scan_interval_sha256")
        ):
            raise PostLifecycleLiveHealthError("live log interval bytes drifted")
        events, pending, cursor, _main_generation, _lifecycle_generation = _scan_complete_lines(
            interval,
            start_offset=boundary,
        )
        if pending or cursor != end:
            raise PostLifecycleLiveHealthError("live log interval is not complete lines")
        selected_main, selected_lifecycle = _selected_events(events)
        recomputed_main = [event["row"] for event in selected_main]
        if recomputed_main != main_health_window.get("rows"):
            raise PostLifecycleLiveHealthError("live main HEALTH bytes/offsets/SHA drifted")
        if selected_lifecycle["row"] != lifecycle_health:
            raise PostLifecycleLiveHealthError("live lifecycle HEALTH bytes/offsets/SHA drifted")
        recomputed = _health_aggregates(selected_main, selected_lifecycle)
        if recomputed["latency"] != operational_aggregates.get("latency") or recomputed[
            "position"
        ] != operational_aggregates.get("position"):
            raise PostLifecycleLiveHealthError("live health operational safe projection drifted")
    finally:
        os.close(descriptor)


def _capture_process(
    *,
    pid: int,
    runtime_repository_root: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    proc_root: Path,
) -> dict[str, Any]:
    try:
        return active_capture_v8._capture_process(  # noqa: SLF001
            pid=pid,
            repository=runtime_repository_root.expanduser().resolve(strict=True),
            config_path=config_path.expanduser().resolve(strict=True),
            python_executable=python_executable.expanduser().absolute(),
            venv_root=venv_root.expanduser().absolute(),
            runtime_identity_path=runtime_identity_path.expanduser().absolute(),
            proc_root=proc_root,
        )
    except Exception as exc:
        raise PostLifecycleLiveHealthError("full active process recapture failed") from exc


def _pid_start_key(*, pid_file: Path, proc_root: Path) -> tuple[int, int]:
    try:
        return active_capture_v8._pid_start_key(  # noqa: SLF001
            pid_file=pid_file,
            proc_root=proc_root,
        )
    except Exception as exc:
        raise PostLifecycleLiveHealthError("active PID/start key disappeared") from exc


def build_post_lifecycle_live_health(
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    config_correction_path: Path,
    active_capture_path: Path,
    lifecycle_admission_path: Path,
    pid_file: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    live_log_path: Path,
    proc_root: Path = Path("/proc"),
    health_timeout_s: float = 180.0,
    health_poll_interval_s: float = 0.25,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    active, active_binding = _activation_context(
        active_capture_path=active_capture_path,
        runtime_repository_root=runtime_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
        config_correction_path=config_correction_path,
        live_log_path=live_log_path,
    )
    lifecycle_payload, lifecycle_binding = _lifecycle_context(lifecycle_admission_path)
    lifecycle_binding["admitted_ts_ns"] = lifecycle_payload["admitted_ts_ns"]
    projected_process, runtime_authority = _active_projection(active, lifecycle_binding)
    activation_process = active["active_process"]
    pid = int(projected_process["pid"])
    start_ticks = int(projected_process["pid_start_ticks"])
    initial = _capture_process(
        pid=pid,
        runtime_repository_root=runtime_repository_root,
        config_path=config_path,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        proc_root=proc_root,
    )
    if not _same_stable_process(initial, activation_process):
        raise PostLifecycleLiveHealthError("active process changed before post-lifecycle boundary")
    sampler = ProcSafetySampler(proc_root, pid)
    with FreshHealthTail(live_log_path) as tail:
        log_capture, main_health, lifecycle_health, aggregates = _capture_fresh_health(
            tail=tail,
            expected_pid=pid,
            expected_start_ticks=start_ticks,
            stable_process_identity_sha256=str(projected_process["stable_process_identity_sha256"]),
            identity_supplier=lambda: _pid_start_key(pid_file=pid_file, proc_root=proc_root),
            sample_supplier=sampler.sample,
            timeout_s=health_timeout_s,
            poll_interval_s=health_poll_interval_s,
        )
    final = _capture_process(
        pid=pid,
        runtime_repository_root=runtime_repository_root,
        config_path=config_path,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        proc_root=proc_root,
    )
    if not _same_stable_process(final, activation_process):
        raise PostLifecycleLiveHealthError("active process changed after post-lifecycle capture")
    timestamp = generated_utc or _now()
    lifecycle_content = {name: lifecycle_binding[name] for name in CONTENT_BINDING_FIELDS}
    lifecycle_context = {
        "admitted_ts_ns": lifecycle_payload["admitted_ts_ns"],
        "baseline_epoch_id": lifecycle_binding["baseline_epoch_id"],
        "config_sha256": lifecycle_binding["config_sha256"],
        "runtime_code_sha256": lifecycle_binding["runtime_code_sha256"],
        "runtime_source_files": dict(lifecycle_binding["runtime_code_files"]),
    }
    portable = _portable_payload(
        generated_utc=timestamp,
        runtime_authority=runtime_authority,
        active_process=projected_process,
        lifecycle_admission=lifecycle_content,
        lifecycle_epoch_id=str(lifecycle_binding["baseline_epoch_id"]),
        main_health_window=main_health,
        lifecycle_health=lifecycle_health,
        operational_aggregates=aggregates,
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "generated_utc": timestamp,
        "activation_capture": active_binding,
        "lifecycle_admission": lifecycle_content,
        "lifecycle_context": lifecycle_context,
        "runtime_execution": dict(RUNTIME_EXECUTION),
        "runtime_authority": runtime_authority,
        "active_process": projected_process,
        "log_capture": log_capture,
        "main_health_window": main_health,
        "lifecycle_health": lifecycle_health,
        "operational_aggregates": aggregates,
        "portable_projection": portable,
        "checks": dict(CHECKS),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = _document_sha256(payload, CANONICAL_FIELD)
    return validate_content_projection(payload)


def validate_post_lifecycle_live_health(
    path: Path,
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    config_correction_path: Path,
    active_capture_path: Path,
    lifecycle_admission_path: Path,
    pid_file: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    live_log_path: Path,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    payload, _binding = _private_binding(
        path,
        label="post-lifecycle live-health receipt",
        canonical_field=CANONICAL_FIELD,
        schema=SCHEMA_VERSION,
        status=STATUS,
    )
    observed = validate_content_projection(payload)
    active, active_binding = _activation_context(
        active_capture_path=active_capture_path,
        runtime_repository_root=runtime_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
        config_correction_path=config_correction_path,
        live_log_path=live_log_path,
    )
    lifecycle_payload, lifecycle_binding = _lifecycle_context(lifecycle_admission_path)
    lifecycle_binding["admitted_ts_ns"] = lifecycle_payload["admitted_ts_ns"]
    expected_process, expected_authority = _active_projection(active, lifecycle_binding)
    lifecycle_content = {name: lifecycle_binding[name] for name in CONTENT_BINDING_FIELDS}
    expected_lifecycle_context = {
        "admitted_ts_ns": lifecycle_payload["admitted_ts_ns"],
        "baseline_epoch_id": lifecycle_binding["baseline_epoch_id"],
        "config_sha256": lifecycle_binding["config_sha256"],
        "runtime_code_sha256": lifecycle_binding["runtime_code_sha256"],
        "runtime_source_files": dict(lifecycle_binding["runtime_code_files"]),
    }
    if (
        observed.get("activation_capture") != active_binding
        or observed.get("lifecycle_admission") != lifecycle_content
        or observed.get("lifecycle_context") != expected_lifecycle_context
        or observed.get("active_process") != expected_process
        or observed.get("runtime_authority") != expected_authority
    ):
        raise PostLifecycleLiveHealthError("post-lifecycle source cross-binding drifted")
    expected_pid_key = (
        int(expected_process["pid"]),
        int(expected_process["pid_start_ticks"]),
    )
    if _pid_start_key(pid_file=pid_file, proc_root=proc_root) != expected_pid_key:
        raise PostLifecycleLiveHealthError("active PID/start changed before receipt validation")
    recaptured = _capture_process(
        pid=int(expected_process["pid"]),
        runtime_repository_root=runtime_repository_root,
        config_path=config_path,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        proc_root=proc_root,
    )
    if not _same_stable_process(recaptured, active["active_process"]):
        raise PostLifecycleLiveHealthError("active process changed before receipt validation")
    _revalidate_log_bytes(
        live_log_path=live_log_path,
        log_capture=observed["log_capture"],
        main_health_window=observed["main_health_window"],
        lifecycle_health=observed["lifecycle_health"],
        operational_aggregates=observed["operational_aggregates"],
    )
    final_recaptured = _capture_process(
        pid=int(expected_process["pid"]),
        runtime_repository_root=runtime_repository_root,
        config_path=config_path,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        proc_root=proc_root,
    )
    if not _same_stable_process(final_recaptured, active["active_process"]):
        raise PostLifecycleLiveHealthError("active process changed during receipt validation")
    if _pid_start_key(pid_file=pid_file, proc_root=proc_root) != expected_pid_key:
        raise PostLifecycleLiveHealthError("active PID/start changed during receipt validation")
    return observed


def finalize_post_lifecycle_live_health(
    *, output_path: Path, **kwargs: Any
) -> tuple[dict[str, Any], str]:
    payload = build_post_lifecycle_live_health(**kwargs)
    try:
        file_sha = resource_v8.atomic_write_receipt(output_path, payload)
    except Exception as exc:
        raise PostLifecycleLiveHealthError(
            "create-only post-lifecycle receipt write failed"
        ) from exc
    validator_kwargs = {
        name: value
        for name, value in kwargs.items()
        if name
        in {
            "runtime_repository_root",
            "direct_release_path",
            "resource_receipt_path",
            "config_correction_path",
            "active_capture_path",
            "lifecycle_admission_path",
            "pid_file",
            "config_path",
            "python_executable",
            "venv_root",
            "runtime_identity_path",
            "live_log_path",
            "proc_root",
        }
    }
    observed = validate_post_lifecycle_live_health(output_path, **validator_kwargs)
    if observed != payload:
        raise PostLifecycleLiveHealthError("post-lifecycle receipt changed after write")
    return payload, file_sha


def _source_args(parser: argparse.ArgumentParser, *, include_pid: bool) -> None:
    parser.add_argument("--runtime-repository-root", type=Path, required=True)
    parser.add_argument("--direct-release", type=Path, required=True)
    parser.add_argument("--resource-receipt", type=Path, required=True)
    parser.add_argument("--config-correction", type=Path, required=True)
    parser.add_argument("--active-capture", type=Path, required=True)
    parser.add_argument("--lifecycle-admission", type=Path, required=True)
    if include_pid:
        parser.add_argument("--pid-file", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--python-executable", type=Path, required=True)
    parser.add_argument("--venv-root", type=Path, required=True)
    parser.add_argument("--runtime-identity", type=Path, required=True)
    parser.add_argument("--live-log", type=Path, required=True)
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("capture")
    _source_args(capture, include_pid=True)
    capture.add_argument("--health-timeout-s", type=float, default=180.0)
    capture.add_argument("--health-poll-interval-s", type=float, default=0.25)
    capture.add_argument("--output", type=Path, required=True)
    validate = commands.add_parser("validate")
    _source_args(validate, include_pid=True)
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def _kwargs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "runtime_repository_root": args.runtime_repository_root,
        "direct_release_path": args.direct_release,
        "resource_receipt_path": args.resource_receipt,
        "config_correction_path": args.config_correction,
        "active_capture_path": args.active_capture,
        "lifecycle_admission_path": args.lifecycle_admission,
        "pid_file": args.pid_file,
        "config_path": args.config,
        "python_executable": args.python_executable,
        "venv_root": args.venv_root,
        "runtime_identity_path": args.runtime_identity,
        "live_log_path": args.live_log,
        "proc_root": args.proc_root,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    values = _kwargs(args)
    if args.command == "capture":
        payload, file_sha = finalize_post_lifecycle_live_health(
            health_timeout_s=args.health_timeout_s,
            health_poll_interval_s=args.health_poll_interval_s,
            output_path=args.output,
            **values,
        )
        result = {
            "schema_version": payload["schema_version"],
            "status": payload["status"],
            "file_sha256": file_sha,
            "canonical_sha256": payload[CANONICAL_FIELD],
        }
    else:
        payload = validate_post_lifecycle_live_health(args.receipt, **values)
        result = {
            "schema_version": payload["schema_version"],
            "status": payload["status"],
            "canonical_sha256": payload[CANONICAL_FIELD],
        }
    print(resource_v8.json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
