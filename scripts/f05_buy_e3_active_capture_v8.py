#!/usr/bin/env python3
"""Capture the fresh fully no-shadow BUY E3 process without authority.

This additive receipt succeeds the historical direct-successor active-capture shapes.
It binds the immutable direct-successor release-v3, the independently collected v8
resource receipt, and the actual post-restart live process.  It reads no
economic outcomes and creates no shadow, companion, or hypothetical action.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v8 as resource_v8,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
)
from scripts import deploy_f05_buy_e3_owner_v1 as deploy

OWNER: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
SCHEMA_VERSION: Final = f"{OWNER}.fresh_all_shadow_evaluators_disabled_active_process_capture.v7"
STATUS: Final = "fresh_active_health_proven_all_shadow_evaluators_disabled"
CANONICAL_FIELD: Final = "canonical_active_capture_sha256"
HEALTH_WINDOW_SCHEMA: Final = f"{OWNER}.fresh_active_main_health_window.v1"
HEALTH_WINDOW_STATUS: Final = "two_consecutive_fresh_active_main_health_rows_verified"

DIRECT_SUCCESSOR_EXECUTION_COMMIT: Final = "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de"
DIRECT_SUCCESSOR_EXECUTION_TREE: Final = "0343bd5586b337385cf2aa0d7a643f5c32b0da77"
DIRECT_SUCCESSOR_ANNOTATED_TAG: Final = "f05-owner-buy-e3-no-shadow-runtime-v3-20260824"
DIRECT_SUCCESSOR_TAG_OBJECT: Final = "3878ea05252ef8f274b6f74ee7a984431c53b892"
DIRECT_SUCCESSOR_RELEASE_FILE_SHA256: Final = (
    "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49"
)
DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256: Final = (
    "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f"
)
DIRECT_SUCCESSOR_RELEASE_SCHEMA: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_direct_owner_active_release.v3"
)
DIRECT_SUCCESSOR_RELEASE_STATUS: Final = (
    "owner_authorized_direct_live_no_shadow_runtime_pending_evidence"
)
ACTIVE_CONFIG_SHA256: Final = "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
EXACT_ARTIFACT_SHA256: Final = "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
RUNTIME_IDENTITY_SCHEMA: Final = "narrowgate_live_runtime_identity.v1"
STARTUP_ATTESTATION_SCHEMA: Final = "narrowgate_buy_e3_startup_attestation.v5"
ACTIVE_RUNTIME_AUTHORITY_SCHEMA: Final = "narrowgate_f05_buy_e3_active_release_runtime_authority.v1"

REQUIRED_ACTIVE_SOURCE_ROLES: Final = frozenset(
    {
        "buy_e3_runtime",
        "order_lifecycle",
        "order_lifecycle_journal_v2",
        "order_lifecycle_journal_v2_strict_native",
        "order_lifecycle_live_writer_v2",
    }
)
STARTUP_SOURCE_ROLE_MAP: Final = {
    "live_main": "live_main",
    "live_config": "live_config",
    "live_runtime_policy": "live_runtime_policy",
    "live_ws_handler": "live_ws_handler",
    "maker_engine": "maker_engine",
    "signal_engine": "signal_engine",
    "global_flow": "global_flow",
    "global_reference": "global_reference",
    "sell_owner_runtime": "sell_runtime",
    "live_buy_runtime": "buy_e3_runtime",
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
AUTHORITY_DESIGN: Final = {
    "runtime_authority": "immutable_direct_successor_owner_release_v3",
    "runtime_authority_replaced": False,
    "runtime_consumed": True,
    "does_not_replace_runtime_active_release": True,
    "retrospective_authority_created": False,
    "evidence_is_additive_only": True,
}
CHECKS: Final = {
    "fresh_pid": True,
    "fresh_start_ticks": True,
    "disabled_predecessor_quiescent": True,
    "direct_successor_checkout_exact": True,
    "direct_successor_release_v3_exact": True,
    "resource_v8_semantic_exact": True,
    "active_config_exact": True,
    "actual_process_identity_exact": True,
    "runtime_identity_exact": True,
    "startup_attestation_accepted": True,
    "buy_e3_enabled": True,
    "owner_override_effective": True,
    "shadow_flags_disabled": True,
    "external_venues_disabled": True,
    "global_flow_shadow_explicitly_disabled": True,
    "global_reference_shadow_explicitly_disabled": True,
    "global_flow_backend_absolute_zero": True,
    "global_reference_state_absolute_zero": True,
    "artifact_exact": True,
    "buy_and_lifecycle_runtime_sources_exact": True,
    "active_process_stable_during_capture": True,
    "two_consecutive_fresh_active_main_health_rows": True,
    "active_health_same_pid_and_start_ticks": True,
    "active_health_e3_and_sell_enabled": True,
    "active_health_external_sources_absolute_zero": True,
    "active_health_shadow_state_absolute_zero": True,
    "active_health_line_bytes_revalidated": True,
    "restart_only_activation": True,
    "retroactive_signature": False,
}
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
TOP_LEVEL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "runtime_authority",
        "resource_receipt",
        "config_correction",
        "host",
        "disabled_predecessor",
        "active_process",
        "runtime_identity",
        "runtime_identity_file_sha256",
        "startup_semantics",
        "active_health_window",
        "checks",
        "authority_design",
        "permissions",
        "evidence_boundary",
        CANONICAL_FIELD,
    }
)
MAX_JSON_BYTES: Final = 64 << 20


class ActiveCaptureV7Error(RuntimeError):
    """Raised when the additive health-proven active capture is not exact."""


# Compatibility only for focused tests and callers of the never-materialized
# v6 candidate.  The actual exception class and receipt schema are v7.
ActiveCaptureV6Error = ActiveCaptureV7Error


@dataclass(frozen=True)
class OpenedJson:
    path: Path
    payload: dict[str, Any]
    raw: bytes
    metadata: os.stat_result


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise ActiveCaptureV6Error(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _open_private_json(path: Path, label: str) -> OpenedJson:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_file():
        raise ActiveCaptureV6Error(f"{label} is not a regular file")
    target = candidate.resolve(strict=True)
    before = target.stat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_size <= 0
        or before.st_size > MAX_JSON_BYTES
    ):
        raise ActiveCaptureV6Error(f"{label} has unsafe identity or permissions")
    try:
        raw = target.read_bytes()
        payload = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ActiveCaptureV6Error(f"non-finite JSON value: {value}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ActiveCaptureV6Error(f"{label} is unreadable JSON") from exc
    after = target.stat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ActiveCaptureV6Error(f"{label} changed while read")
    if not isinstance(payload, dict):
        raise ActiveCaptureV6Error(f"{label} root is not an object")
    return OpenedJson(target, payload, raw, before)


def _require_sha256(value: Any, label: str) -> str:
    try:
        return resource_v8._require_sha256(value, label)  # noqa: SLF001
    except Exception as exc:
        raise ActiveCaptureV6Error(f"{label} is not SHA256") from exc


def _strict_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ActiveCaptureV6Error(f"{label} is not a positive integer")
    return value


def _timestamp(value: Any, label: str) -> str:
    normalized = str(value)
    if not normalized.endswith("Z"):
        raise ActiveCaptureV6Error(f"{label} is not UTC")
    try:
        observed = datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ActiveCaptureV6Error(f"{label} is invalid") from exc
    if observed.utcoffset() != UTC.utcoffset(observed):
        raise ActiveCaptureV6Error(f"{label} is not UTC")
    return normalized


def _utc_datetime(value: Any, label: str) -> datetime:
    normalized = _timestamp(value, label)
    return datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _content_binding(
    opened: OpenedJson,
    *,
    canonical_field: str,
    expected_schema: str,
    expected_status: str,
) -> dict[str, Any]:
    canonical = _require_sha256(opened.payload.get(canonical_field), canonical_field)
    if (
        opened.payload.get("schema_version") != expected_schema
        or opened.payload.get("status") != expected_status
        or canonical != resource_v8.document_sha256(opened.payload, canonical_field)
    ):
        raise ActiveCaptureV6Error("content binding semantic identity drifted")
    return {
        "schema_version": expected_schema,
        "status": expected_status,
        "file_sha256": hashlib.sha256(opened.raw).hexdigest(),
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
        "size_bytes": len(opened.raw),
        "mode": "0600",
    }


def _validate_runtime_repository(root: Path) -> tuple[Path, dict[str, Any]]:
    try:
        repository = root.expanduser().resolve(strict=True)
        execution = resource_v8.capture_git_execution(
            repository,
            annotated_tag=DIRECT_SUCCESSOR_ANNOTATED_TAG,
            runtime_authority=True,
        )
    except Exception as exc:
        raise ActiveCaptureV6Error(
            "active runtime checkout is not exact clean direct-successor"
        ) from exc
    expected = {
        "execution_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "execution_tree": DIRECT_SUCCESSOR_EXECUTION_TREE,
        "annotated_tag": DIRECT_SUCCESSOR_ANNOTATED_TAG,
        "annotated_tag_object": DIRECT_SUCCESSOR_TAG_OBJECT,
        "tag_peeled_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
    }
    if any(execution.get(field) != value for field, value in expected.items()):
        raise ActiveCaptureV6Error("active runtime Git identity drifted")
    return repository, execution


def _validate_release(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    opened = _open_private_json(path, "direct-successor release-v2")
    binding = _content_binding(
        opened,
        canonical_field="canonical_active_release_sha256",
        expected_schema=DIRECT_SUCCESSOR_RELEASE_SCHEMA,
        expected_status=DIRECT_SUCCESSOR_RELEASE_STATUS,
    )
    try:
        resource_v8._validate_direct_release_payload(opened.payload)  # noqa: SLF001
    except Exception as exc:
        raise ActiveCaptureV6Error("direct-successor release-v2 semantics drifted") from exc
    if (
        binding["file_sha256"] != DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or binding["canonical_sha256"] != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
    ):
        raise ActiveCaptureV6Error("direct-successor release-v2 byte identity drifted")
    return dict(opened.payload), binding


def _validate_resource(
    path: Path,
    *,
    config_correction_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        validated = resource_v8.validate_resource_receipt(
            path,
            config_correction_path=config_correction_path,
        )
    except Exception as exc:
        raise ActiveCaptureV6Error("resource-v4 receipt is invalid") from exc
    opened = _open_private_json(path, "resource-v4 receipt")
    binding = _content_binding(
        opened,
        canonical_field=resource_v8.RESOURCE_CANONICAL_FIELD,
        expected_schema=resource_v8.RESOURCE_SCHEMA,
        expected_status=resource_v8.RESOURCE_STATUS,
    )
    execution = validated.get("runtime_execution")
    authority = validated.get("authority_design")
    sources = validated.get("runtime_sources")
    deployed = validated.get("exact_deployed_files")
    if validated != opened.payload or not all(
        isinstance(value, Mapping) for value in (execution, authority, sources, deployed)
    ):
        raise ActiveCaptureV6Error("resource-v4 receipt changed or is incomplete")
    if (
        execution.get("execution_commit") != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or execution.get("execution_tree") != DIRECT_SUCCESSOR_EXECUTION_TREE
        or execution.get("annotated_tag") != DIRECT_SUCCESSOR_ANNOTATED_TAG
        or execution.get("annotated_tag_object") != DIRECT_SUCCESSOR_TAG_OBJECT
        or authority.get("runtime_authority_release_file_sha256")
        != DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or authority.get("runtime_authority_release_canonical_sha256")
        != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or authority.get("direct_successor_release_does_not_depend_on_resource_receipt") is not True
        or deployed.get("artifact_sha256") != EXACT_ARTIFACT_SHA256
        or sources.get("direct_successor_execution_commit") != DIRECT_SUCCESSOR_EXECUTION_COMMIT
    ):
        raise ActiveCaptureV6Error("resource-v4 authority semantics drifted")
    return dict(validated), binding


def _disabled_process(resource: Mapping[str, Any]) -> dict[str, Any]:
    raw = resource.get("fresh_disabled_process")
    if not isinstance(raw, Mapping):
        raise ActiveCaptureV6Error("resource-v4 lacks its disabled process")
    process = {
        "pid": raw.get("disabled_pid"),
        "pid_start_ticks": raw.get("disabled_pid_start_ticks"),
        "canonical_process_identity_sha256": raw.get("disabled_process_identity_sha256"),
        "config_path": raw.get("disabled_config_path"),
        "config_sha256": raw.get("disabled_config_sha256"),
        "fresh_pid": raw.get("fresh_pid"),
        "fresh_start_ticks": raw.get("fresh_start_ticks"),
        "same_pid_pre_post": raw.get("same_pid_pre_post"),
    }
    _strict_positive_int(process["pid"], "disabled PID")
    _strict_positive_int(process["pid_start_ticks"], "disabled PID start ticks")
    _require_sha256(process["canonical_process_identity_sha256"], "disabled process identity")
    if process["config_sha256"] != resource_v8.EXPECTED_DISABLED_CONFIG_SHA256 or any(
        process[name] is not True
        for name in ("fresh_pid", "fresh_start_ticks", "same_pid_pre_post")
    ):
        raise ActiveCaptureV6Error("resource-v4 disabled process semantics drifted")
    return process


def _predecessor_is_quiescent(pid: int, *, proc_root: Path) -> bool:
    root = proc_root.expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ActiveCaptureV6Error("active capture requires a process filesystem")
    return not (root / str(pid)).exists()


def _read_pid(path: Path) -> int:
    candidate = path.expanduser().resolve(strict=True)
    try:
        return _strict_positive_int(
            int(candidate.read_text(encoding="ascii").strip()), "active PID"
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ActiveCaptureV6Error("active PID file is invalid") from exc


def _git_blob_sha256(repository: Path, relative: str) -> str:
    completed = subprocess.run(
        ("git", "show", f"{DIRECT_SUCCESSOR_EXECUTION_COMMIT}:{relative}"),
        cwd=repository,
        check=True,
        capture_output=True,
    )
    return hashlib.sha256(completed.stdout).hexdigest()


def _capture_active_runtime_sources(
    repository: Path,
    resource: Mapping[str, Any],
) -> dict[str, Any]:
    resource_sources = resource.get("runtime_sources")
    resource_files = (
        resource_sources.get("files") if isinstance(resource_sources, Mapping) else None
    )
    if not isinstance(resource_files, Mapping):
        raise ActiveCaptureV6Error("resource-v4 runtime source map is missing")
    rows: dict[str, dict[str, Any]] = {}
    for role, frozen in resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items():
        relative = str(frozen["path"])
        expected = str(frozen["sha256"])
        resource_row = resource_files.get(role)
        candidate = repository / relative
        if (
            not isinstance(resource_row, Mapping)
            or resource_row.get("repository_relative_path") != relative
            or resource_row.get("sha256") != expected
            or candidate.is_symlink()
            or not candidate.is_file()
            or resource_v8.file_sha256(candidate) != expected
            or _git_blob_sha256(repository, relative) != expected
        ):
            raise ActiveCaptureV6Error(f"active runtime source drifted: {role}")
        rows[role] = {
            "role": role,
            "repository_relative_path": relative,
            "sha256": expected,
            "active_working_matches_direct_successor": True,
            "direct_successor_commit_blob_matches": True,
            "resource_v8_binding_matches": True,
        }
    if not REQUIRED_ACTIVE_SOURCE_ROLES.issubset(rows):
        raise ActiveCaptureV6Error("BUY and lifecycle source roles are incomplete")
    return {
        "execution_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "files": rows,
        "runtime_source_manifest_sha256": resource_v8.canonical_sha256(rows),
        "buy_and_four_lifecycle_sources_exact": True,
    }


def _startup_source_plan() -> dict[str, Any]:
    files: dict[str, dict[str, str]] = {}
    for startup_role, resource_role in STARTUP_SOURCE_ROLE_MAP.items():
        frozen = resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[resource_role]
        sha = str(frozen["sha256"])
        files[startup_role] = {
            "repository_relative_path": str(frozen["path"]),
            "execution_commit_blob_sha256": sha,
            "working_file_sha256": sha,
            "authority_basis": deploy._CURRENT_RUNTIME_SOURCE_AUTHORITY_BASIS,  # noqa: SLF001
        }
    plan = {
        "files": files,
        "runtime_code_sha256": resource_v8.canonical_sha256(files),
    }
    try:
        expected = deploy._validated_expected_runtime_source_hashes(plan)  # noqa: SLF001
    except Exception as exc:
        raise ActiveCaptureV7Error("startup runtime source authority is malformed") from exc
    if expected != {
        str(binding["path"]): str(binding["sha256"])
        for binding in resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.values()
        if str(binding["path"]) in set(deploy._CURRENT_RUNTIME_SOURCE_PATHS.values())  # noqa: SLF001
    }:
        raise ActiveCaptureV7Error("startup runtime source authority is incomplete")
    return plan


def _release_phase_binding(
    release_binding: Mapping[str, Any],
    *,
    runtime_release_path: str,
) -> dict[str, Any]:
    return {
        "local_path": runtime_release_path,
        "remote_path": runtime_release_path,
        "file_sha256": release_binding["file_sha256"],
        "canonical_active_release_sha256": release_binding["canonical_sha256"],
        "schema_version": release_binding["schema_version"],
        "status": release_binding["status"],
        "active_config_file_sha256": ACTIVE_CONFIG_SHA256,
        "disabled_config_file_sha256": resource_v8.EXPECTED_DISABLED_CONFIG_SHA256,
    }


def _shadow_runtime_semantics(
    startup: Mapping[str, Any], runtime: Mapping[str, Any]
) -> dict[str, Any]:
    raw = startup.get("shadow_runtime_identity")
    expected_fields = {
        "schema_version",
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
        "global_flow_native_requested",
        "global_flow_native_effective",
        "global_flow_backend",
        "global_reference_bridge_basis_sample_count",
        "state_restore_contract",
        "global_flow_shadow_config_explicit",
        "global_reference_shadow_config_explicit",
    }
    if not isinstance(raw, Mapping) or set(raw) != expected_fields:
        raise ActiveCaptureV6Error("startup shadow runtime identity fields drifted")
    backend_fields = {
        "native",
        "market_count",
        "trade_batches",
        "trade_events_seen",
        "trade_events_accepted",
        "book_events_seen",
        "book_events_accepted",
        "out_of_order_events",
        "stale_trade_events",
        "trade_overflow_events",
        "book_overflow_events",
    }
    backend = raw.get("global_flow_backend")
    if (
        not isinstance(backend, Mapping)
        or set(backend) != backend_fields
        or any(type(backend[name]) is not int or backend[name] != 0 for name in backend_fields)
    ):
        raise ActiveCaptureV6Error("disabled global-flow backend is not exact absolute zero")
    gates = startup.get("gates")
    if (
        raw.get("schema_version") != "narrowgate_shadow_runtime_identity.v1"
        or raw.get("global_flow_shadow_enabled") is not False
        or raw.get("global_reference_shadow_enabled") is not False
        or raw.get("global_flow_shadow_config_explicit") is not True
        or raw.get("global_reference_shadow_config_explicit") is not True
        or raw.get("global_flow_native_requested") is not True
        or raw.get("global_flow_native_effective") is not False
        or raw.get("global_reference_bridge_basis_sample_count") != 0
        or raw.get("state_restore_contract") != "shadow_state_never_restored"
        or runtime.get("global_flow_shadow_enabled") is not False
        or runtime.get("global_reference_shadow_enabled") is not False
        or runtime.get("global_flow_shadow_config_explicit") is not True
        or runtime.get("global_reference_shadow_config_explicit") is not True
        or not isinstance(gates, Mapping)
        or gates.get("shadow_config_explicit") is not True
        or gates.get("global_flow_shadow_backend_contract_valid") is not True
        or gates.get("global_reference_shadow_state_contract_valid") is not True
    ):
        raise ActiveCaptureV6Error("disabled shadow runtime authority drifted")
    projection = {
        **{name: raw[name] for name in sorted(expected_fields - {"global_flow_backend"})},
        "global_flow_backend": {name: backend[name] for name in sorted(backend_fields)},
    }
    return {
        "identity": projection,
        "identity_sha256": resource_v8.canonical_sha256(projection),
        "all_shadow_evaluators_disabled": True,
        "all_global_flow_backend_fields_absolute_zero": True,
        "global_reference_basis_samples_absolute_zero": True,
    }


def _runtime_semantics(
    runtime: Mapping[str, Any],
    *,
    process: Mapping[str, Any],
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
    expected_repository_root: Path,
    expected_release_path: str | None,
) -> dict[str, Any]:
    active_pid = _strict_positive_int(process.get("pid"), "active PID")
    repository = expected_repository_root.expanduser().absolute()
    if Path(str(process.get("cwd", ""))).expanduser().absolute() != repository:
        raise ActiveCaptureV6Error("active process cwd differs from runtime authority")
    runtime_release_path = str(runtime.get("f05_buy_e3_active_release_path", ""))
    if not PurePosixPath(runtime_release_path).is_absolute() or (
        expected_release_path is not None and runtime_release_path != expected_release_path
    ):
        raise ActiveCaptureV6Error("active runtime release path drifted")
    try:
        startup = deploy._validate_runtime_identity_authority(  # noqa: SLF001
            runtime,
            expected_pid=active_pid,
            expected_config_path=str(process["config_path"]),
            expected_config_sha256=ACTIVE_CONFIG_SHA256,
            expected_python_executable=str(process["python_executable"]),
            expected_python_binary_resolved=str(process["python_binary_resolved"]),
            expected_enabled=True,
            expected_artifact_sha256=EXACT_ARTIFACT_SHA256,
            expected_execution_commit=DIRECT_SUCCESSOR_EXECUTION_COMMIT,
            expected_execution_tree=DIRECT_SUCCESSOR_EXECUTION_TREE,
            expected_runtime_sources=_startup_source_plan(),
            expected_repository_root=str(repository),
            expected_startup_attestation_schema_version=STARTUP_ATTESTATION_SCHEMA,
            expected_active_release=_release_phase_binding(
                release_binding,
                runtime_release_path=runtime_release_path,
            ),
        )
    except Exception as exc:
        raise ActiveCaptureV6Error(
            "active runtime/startup attestation is not exact successor"
        ) from exc
    roles = release.get("exact_artifact", {}).get("roles", {})
    startup_release = startup.get("buy_e3_active_release")
    gates = startup.get("gates")
    shadow_semantics = _shadow_runtime_semantics(startup, runtime)
    if (
        runtime.get("schema_version") != RUNTIME_IDENTITY_SCHEMA
        or runtime.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or runtime.get("buy_fill_selection_shadow_enabled") is not False
        or runtime.get("dynamic_fill_hazard_shadow_enabled") is not False
        or runtime.get("f05_buy_e3_enabled") is not True
        or runtime.get("f05_buy_e3_owner_override_effective") is not True
        or runtime.get("f05_buy_e3_required") is not True
        or runtime.get("f05_buy_e3_artifact_sha256") != EXACT_ARTIFACT_SHA256
        or runtime.get("f05_buy_e3_artifact_manifest_sha256")
        != roles.get("manifest", {}).get("file_sha256")
        or runtime.get("f05_buy_e3_policy_sha256") != roles.get("policy", {}).get("file_sha256")
        or runtime.get("f05_buy_e3_predicate_bundle_sha256")
        != roles.get("predicate_bundle", {}).get("file_sha256")
        or runtime.get("f05_buy_e3_active_release_authority_schema_version")
        != ACTIVE_RUNTIME_AUTHORITY_SCHEMA
        or runtime.get("f05_buy_e3_active_release_file_sha256")
        != DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
        or runtime.get("f05_buy_e3_active_release_canonical_sha256")
        != DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        or startup.get("schema_version") != STARTUP_ATTESTATION_SCHEMA
        or startup.get("status") != "accepted"
        or startup.get("errors") != []
        or not isinstance(gates, Mapping)
        or not gates
        or any(value is not True for value in gates.values())
        or not isinstance(startup_release, Mapping)
        or startup_release.get("execution_commit") != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or startup_release.get("execution_tree") != DIRECT_SUCCESSOR_EXECUTION_TREE
        or startup_release.get("annotated_operational_tag") != DIRECT_SUCCESSOR_ANNOTATED_TAG
        or startup_release.get("annotated_operational_tag_object") != DIRECT_SUCCESSOR_TAG_OBJECT
        or startup_release.get("active_config_file_sha256") != ACTIVE_CONFIG_SHA256
        or startup_release.get("disabled_config_file_sha256")
        != resource_v8.EXPECTED_DISABLED_CONFIG_SHA256
        or gates.get("buy_e3_active_release_matches_running_config") is not True
    ):
        raise ActiveCaptureV6Error("active runtime authority, artifact, or shadow state drifted")
    state = startup.get("fill_cooldown_state")
    if not isinstance(state, Mapping):
        raise ActiveCaptureV6Error("active startup cooldown state is missing")
    buy_identity = str(state.get("buy_deadline_identity", ""))
    if buy_identity not in {"B0", f"BUY_E3:{EXACT_ARTIFACT_SHA256}"} or (
        state.get("e3_deadline_imported") is True and buy_identity == "B0"
    ):
        raise ActiveCaptureV6Error("active startup imported an unsafe BUY deadline")
    return {
        "startup_attestation_sha256": resource_v8.canonical_sha256(startup),
        "startup_status": "accepted",
        "running_checkout_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "running_checkout_tree": DIRECT_SUCCESSOR_EXECUTION_TREE,
        "buy_deadline_identity": buy_identity,
        "fill_cooldown_restore_mode": state.get("restore_mode"),
        "buy_remaining_ms": state.get("buy_remaining_ms"),
        "e3_deadline_imported": state.get("e3_deadline_imported"),
        "shadow_runtime": shadow_semantics,
    }


def _stable_process_projection(process: Mapping[str, Any]) -> dict[str, Any]:
    return {
        name: process.get(name)
        for name in (
            "schema_version",
            "pid",
            "pid_start_ticks",
            "cmdline",
            "cmdline_sha256",
            "cwd",
            "config_path",
            "config_sha256",
            "python_executable",
            "python_binary_resolved",
            "venv_root",
            "runtime_identity",
        )
    }


def _capture_process(
    *,
    pid: int,
    repository: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    proc_root: Path,
) -> dict[str, Any]:
    try:
        return gate_v2.capture_actual_process_identity(
            pid=pid,
            expected_repository_root=repository,
            expected_config_path=config_path,
            expected_config_sha256=ACTIVE_CONFIG_SHA256,
            expected_python_executable=python_executable,
            expected_venv_root=venv_root,
            proc_root=proc_root,
            runtime_identity_path=runtime_identity_path,
        )
    except Exception as exc:
        raise ActiveCaptureV6Error("actual active process identity capture failed") from exc


def _health_projection(parsed: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "boolean_cooldown_enabled": parsed.get("boolean_cooldown_enabled"),
        "boolean_cooldown_updates": parsed.get("boolean_cooldown_updates"),
        "buy_e3_enabled": parsed.get("buy_e3_enabled"),
        "deep_book_buffer": parsed.get("deep_book_buffer"),
        "shadow_disabled_state": dict(parsed.get("shadow_disabled_state", {})),
        "counter_values": dict(parsed.get("counter_values", {})),
    }


def _validate_health_projection(raw: Any) -> dict[str, Any]:
    expected_fields = {
        "boolean_cooldown_enabled",
        "boolean_cooldown_updates",
        "buy_e3_enabled",
        "deep_book_buffer",
        "shadow_disabled_state",
        "counter_values",
    }
    if not isinstance(raw, Mapping) or set(raw) != expected_fields:
        raise ActiveCaptureV6Error("active HEALTH projection fields drifted")
    projection = dict(raw)
    shadow = projection.get("shadow_disabled_state")
    counters = projection.get("counter_values")
    expected_shadow_fields = {
        "externalSources",
        *resource_v8.GLOBAL_FLOW_STATE_ZERO_FIELDS,
        *resource_v8.GLOBAL_FLOW_VALUE_ZERO_FIELDS,
        *resource_v8.GLOBAL_REFERENCE_ZERO_FIELDS,
        *resource_v8.GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
        *resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
        "globalFlowReason",
        "globalRefReason",
    }
    if (
        projection.get("boolean_cooldown_enabled") != 1
        or projection.get("buy_e3_enabled") != 1
        or type(projection.get("boolean_cooldown_updates")) is not int
        or int(projection["boolean_cooldown_updates"]) < 0
        or projection.get("deep_book_buffer") != 0
        or not isinstance(shadow, Mapping)
        or set(shadow) != expected_shadow_fields
        or shadow.get("globalFlowReason") != resource_v8.SHADOW_DISABLED_REASON
        or shadow.get("globalRefReason") != resource_v8.SHADOW_DISABLED_REASON
        or any(
            type(value) is not int or value != 0
            for key, value in shadow.items()
            if key not in {"globalFlowReason", "globalRefReason"}
        )
        or not isinstance(counters, Mapping)
        or set(counters) != set(resource_v8.WINDOW_ZERO_COUNTERS[:-2])
        or any(type(value) is not int or value < 0 for value in counters.values())
        or any(
            counters.get(name) != 0
            for name in (
                "buyE3CooldownInvalid",
                "buyE3CooldownResets",
                "externalErrors",
                "externalRecordDropped",
                *resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
            )
        )
    ):
        raise ActiveCaptureV6Error("active HEALTH no-shadow semantics drifted")
    return projection


class ActiveMainHealthTail:
    """Read only main HEALTH lines appended after an exact EOF boundary."""

    def __init__(self, path: Path) -> None:
        candidate = path.expanduser().absolute()
        if candidate.is_symlink() or not candidate.is_file():
            raise ActiveCaptureV6Error("active live log is not a regular non-symlink file")
        self.path = candidate.resolve(strict=True)
        metadata = self.path.stat()
        self._device = metadata.st_dev
        self._inode = metadata.st_ino
        if metadata.st_size:
            with self.path.open("rb") as handle:
                handle.seek(-1, os.SEEK_END)
                if handle.read(1) != b"\n":
                    raise ActiveCaptureV7Error(
                        "active live log EOF is not a complete-line boundary"
                    )
        self.boundary_offset_bytes = metadata.st_size
        self._read_offset = metadata.st_size
        self._buffer = b""
        self._buffer_start = metadata.st_size
        self._main_generation = 0

    def poll(self) -> list[dict[str, Any]]:
        metadata = self.path.stat()
        if (
            metadata.st_dev != self._device
            or metadata.st_ino != self._inode
            or metadata.st_size < self._read_offset
        ):
            raise ActiveCaptureV6Error("active live log rotated during HEALTH capture")
        if metadata.st_size > self._read_offset:
            with self.path.open("rb") as handle:
                handle.seek(self._read_offset)
                chunk = handle.read()
                self._read_offset = handle.tell()
            self._buffer += chunk
        observed: list[dict[str, Any]] = []
        while True:
            newline = self._buffer.find(b"\n")
            if newline < 0:
                break
            line_bytes = self._buffer[: newline + 1]
            raw = line_bytes[:-1]
            start = self._buffer_start
            self._buffer = self._buffer[newline + 1 :]
            self._buffer_start += newline + 1
            if resource_v8._HEALTH_MARKER.encode("ascii") not in raw:  # noqa: SLF001
                continue
            try:
                line = line_bytes.decode("utf-8", errors="strict")
            except UnicodeError as exc:
                raise ActiveCaptureV6Error("active HEALTH line is not UTF-8") from exc
            self._main_generation += 1
            try:
                parsed = resource_v8._parse_main_health(  # noqa: SLF001
                    line,
                    generation=self._main_generation,
                )
            except Exception as exc:
                raise ActiveCaptureV6Error("active main HEALTH line is not exact") from exc
            projection = _validate_health_projection(_health_projection(parsed))
            observed.append(
                {
                    "fresh_generation": self._main_generation,
                    "line_offset_bytes": start,
                    "line_size_bytes": len(line_bytes),
                    "line_sha256": hashlib.sha256(line_bytes).hexdigest(),
                    "main_wall_timestamp_s": parsed["wall_timestamp_s"],
                    "projection": projection,
                }
            )
        return observed


def _pid_start_key(*, pid_file: Path, proc_root: Path) -> tuple[int, int]:
    pid = _read_pid(pid_file)
    try:
        start_ticks = resource_v8._proc_start_ticks(proc_root, pid)  # noqa: SLF001
    except Exception as exc:
        raise ActiveCaptureV6Error("active PID/start ticks disappeared") from exc
    return pid, int(start_ticks)


def _capture_fresh_active_health_window(
    *,
    tail: ActiveMainHealthTail,
    expected_pid: int,
    expected_start_ticks: int,
    process_stable_identity_sha256: str,
    identity_supplier: Callable[[], tuple[int, int]],
    timeout_s: float = 150.0,
    poll_interval_s: float = 0.25,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    expected_key = (expected_pid, expected_start_ticks)

    def assert_identity() -> None:
        if identity_supplier() != expected_key:
            raise ActiveCaptureV6Error("active PID/start ticks changed during HEALTH capture")

    if timeout_s <= 0.0 or poll_interval_s <= 0.0:
        raise ActiveCaptureV6Error("active HEALTH timeout/poll interval is invalid")
    assert_identity()
    rows: list[dict[str, Any]] = []
    deadline = monotonic() + timeout_s
    while monotonic() <= deadline and len(rows) < 2:
        assert_identity()
        for row in tail.poll():
            assert_identity()
            rows.append(row)
            if len(rows) == 2:
                break
        assert_identity()
        if len(rows) < 2:
            sleep(min(poll_interval_s, max(0.0, deadline - monotonic())))
    if len(rows) < 2:
        raise ActiveCaptureV6Error("two fresh active main HEALTH rows were not observed")
    first, second = rows[:2]
    if (
        first.get("fresh_generation") != 1
        or second.get("fresh_generation") != 2
        or float(second.get("main_wall_timestamp_s", 0.0))
        <= float(first.get("main_wall_timestamp_s", 0.0))
        or int(second["projection"]["boolean_cooldown_updates"])
        <= int(first["projection"]["boolean_cooldown_updates"])
    ):
        raise ActiveCaptureV6Error("active main HEALTH pair is not consecutive and monotonic")
    _require_sha256(process_stable_identity_sha256, "active process stable identity")
    return {
        "schema_version": HEALTH_WINDOW_SCHEMA,
        "status": HEALTH_WINDOW_STATUS,
        "log_path_provenance": str(tail.path),
        "boundary_offset_bytes": tail.boundary_offset_bytes,
        "active_pid": expected_pid,
        "active_pid_start_ticks": expected_start_ticks,
        "active_process_stable_identity_sha256": process_stable_identity_sha256,
        "rows": [first, second],
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


def _validate_active_health_window(
    raw: Any,
    *,
    live_log_path: Path,
    expected_pid: int,
    expected_start_ticks: int,
    expected_process_stable_identity_sha256: str,
) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "status",
        "log_path_provenance",
        "boundary_offset_bytes",
        "active_pid",
        "active_pid_start_ticks",
        "active_process_stable_identity_sha256",
        "rows",
        "checks",
    }
    if not isinstance(raw, Mapping) or set(raw) != expected_fields:
        raise ActiveCaptureV6Error("active HEALTH window fields drifted")
    window = dict(raw)
    log = live_log_path.expanduser().absolute()
    if log.is_symlink() or not log.is_file():
        raise ActiveCaptureV6Error("active live log is not a regular non-symlink file")
    resolved = log.resolve(strict=True)
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
        window.get("schema_version") != HEALTH_WINDOW_SCHEMA
        or window.get("status") != HEALTH_WINDOW_STATUS
        or window.get("log_path_provenance") != str(resolved)
        or type(window.get("boundary_offset_bytes")) is not int
        or int(window["boundary_offset_bytes"]) < 0
        or window.get("active_pid") != expected_pid
        or window.get("active_pid_start_ticks") != expected_start_ticks
        or window.get("active_process_stable_identity_sha256")
        != expected_process_stable_identity_sha256
        or not isinstance(rows, list)
        or len(rows) != 2
        or window.get("checks") != expected_checks
    ):
        raise ActiveCaptureV6Error("active HEALTH window identity drifted")
    expected_row_fields = {
        "fresh_generation",
        "line_offset_bytes",
        "line_size_bytes",
        "line_sha256",
        "main_wall_timestamp_s",
        "projection",
    }
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != expected_row_fields:
            raise ActiveCaptureV7Error("active HEALTH row fields drifted")
    boundary = int(window["boundary_offset_bytes"])
    last = rows[-1]
    end = last.get("line_offset_bytes")
    last_size = last.get("line_size_bytes")
    if type(end) is not int or type(last_size) is not int or last_size <= 0:
        raise ActiveCaptureV7Error("active HEALTH row location drifted")
    end += last_size
    if end <= boundary:
        raise ActiveCaptureV7Error("active HEALTH byte interval drifted")
    with resolved.open("rb") as handle:
        handle.seek(boundary)
        interval = handle.read(end - boundary)
    if len(interval) != end - boundary or not interval.endswith(b"\n"):
        raise ActiveCaptureV7Error("active HEALTH byte interval is incomplete")
    recomputed: list[dict[str, Any]] = []
    offset = boundary
    for line_bytes in interval.splitlines(keepends=True):
        if not line_bytes.endswith(b"\n"):
            raise ActiveCaptureV7Error("active HEALTH interval has an incomplete line")
        if resource_v8._HEALTH_MARKER.encode("ascii") not in line_bytes:  # noqa: SLF001
            offset += len(line_bytes)
            continue
        index = len(recomputed) + 1
        try:
            line = line_bytes.decode("utf-8", errors="strict")
            parsed = resource_v8._parse_main_health(  # noqa: SLF001
                line,
                generation=index,
            )
        except Exception as exc:
            raise ActiveCaptureV7Error("active HEALTH line could not be recomputed") from exc
        recomputed.append(
            {
                "fresh_generation": index,
                "line_offset_bytes": offset,
                "line_size_bytes": len(line_bytes),
                "line_sha256": hashlib.sha256(line_bytes).hexdigest(),
                "main_wall_timestamp_s": parsed["wall_timestamp_s"],
                "projection": _validate_health_projection(_health_projection(parsed)),
            }
        )
        offset += len(line_bytes)
    if recomputed != [dict(row) for row in rows]:
        raise ActiveCaptureV7Error("active HEALTH rows or intervening bytes drifted")
    if float(recomputed[1]["main_wall_timestamp_s"]) <= float(
        recomputed[0]["main_wall_timestamp_s"]
    ) or int(recomputed[1]["projection"]["boolean_cooldown_updates"]) <= int(
        recomputed[0]["projection"]["boolean_cooldown_updates"]
    ):
        raise ActiveCaptureV6Error("active HEALTH row chronology drifted")
    return window


def build_active_capture(
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    config_correction_path: Path,
    pid_file: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    live_log_path: Path,
    proc_root: Path = Path("/proc"),
    health_timeout_s: float = 150.0,
    health_poll_interval_s: float = 0.25,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    repository, _execution = _validate_runtime_repository(runtime_repository_root)
    release, release_binding = _validate_release(direct_release_path)
    resource, resource_binding = _validate_resource(
        resource_receipt_path,
        config_correction_path=config_correction_path,
    )
    disabled = _disabled_process(resource)
    disabled_pid = int(disabled["pid"])
    if not _predecessor_is_quiescent(disabled_pid, proc_root=proc_root):
        raise ActiveCaptureV6Error("disabled predecessor PID is still running")
    config = config_path.expanduser().resolve(strict=True)
    if resource_v8.file_sha256(config) != ACTIVE_CONFIG_SHA256:
        raise ActiveCaptureV6Error("active config bytes drifted")
    config_payload = resource_v8._load_yaml(config)  # noqa: SLF001
    external = resource_v8._mapping(  # noqa: SLF001
        config_payload.get("external_venues"), "external_venues config"
    )
    if external.get("enabled") is not False:
        raise ActiveCaptureV6Error("external venue shadow input is enabled")
    multi_market = resource_v8._mapping(  # noqa: SLF001
        config_payload.get("multi_market"), "multi_market config"
    )
    for name in (
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
    ):
        if name not in multi_market or multi_market.get(name) is not False:
            raise ActiveCaptureV6Error(f"multi_market.{name} is not explicitly disabled")
    active_pid = _read_pid(pid_file)
    if active_pid == disabled_pid:
        raise ActiveCaptureV6Error("active restart reused the disabled PID")
    process = _capture_process(
        pid=active_pid,
        repository=repository,
        config_path=config,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        proc_root=proc_root,
    )
    active_start = _strict_positive_int(process.get("pid_start_ticks"), "active PID start ticks")
    if active_start <= int(disabled["pid_start_ticks"]):
        raise ActiveCaptureV6Error("active process did not start after its disabled predecessor")
    runtime_opened = _open_private_json(runtime_identity_path, "active runtime identity")
    runtime_file_sha = hashlib.sha256(runtime_opened.raw).hexdigest()
    process_runtime = process.get("runtime_identity")
    if (
        not isinstance(process_runtime, Mapping)
        or process_runtime.get("present") is not True
        or process_runtime.get("file_sha256") != runtime_file_sha
    ):
        raise ActiveCaptureV6Error("actual process and runtime identity bytes differ")
    active_sources = _capture_active_runtime_sources(repository, resource)
    release_resolved = str(direct_release_path.expanduser().resolve(strict=True))
    semantics = _runtime_semantics(
        runtime_opened.payload,
        process=process,
        release=release,
        release_binding=release_binding,
        expected_repository_root=repository,
        expected_release_path=release_resolved,
    )
    stable_process_sha = resource_v8.canonical_sha256(_stable_process_projection(process))
    health_tail = ActiveMainHealthTail(live_log_path)
    health_window = _capture_fresh_active_health_window(
        tail=health_tail,
        expected_pid=active_pid,
        expected_start_ticks=active_start,
        process_stable_identity_sha256=stable_process_sha,
        identity_supplier=lambda: _pid_start_key(pid_file=pid_file, proc_root=proc_root),
        timeout_s=health_timeout_s,
        poll_interval_s=health_poll_interval_s,
    )
    recaptured = _capture_process(
        pid=active_pid,
        repository=repository,
        config_path=config,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        proc_root=proc_root,
    )
    if _stable_process_projection(recaptured) != _stable_process_projection(process):
        raise ActiveCaptureV6Error("active process changed during capture")
    _validate_active_health_window(
        health_window,
        live_log_path=live_log_path,
        expected_pid=active_pid,
        expected_start_ticks=active_start,
        expected_process_stable_identity_sha256=stable_process_sha,
    )
    process_row = dict(process)
    process_row.pop("canonical_process_identity_sha256", None)
    process_row.update(
        {
            "execution_commit": DIRECT_SUCCESSOR_EXECUTION_COMMIT,
            "execution_tree": DIRECT_SUCCESSOR_EXECUTION_TREE,
            "artifact_sha256": EXACT_ARTIFACT_SHA256,
            "buy_e3_enabled": True,
            "owner_override_effective": True,
            "runtime_identity_file_sha256": runtime_file_sha,
            "startup_attestation_sha256": semantics["startup_attestation_sha256"],
            "runtime_source_files": active_sources["files"],
            "runtime_source_manifest_sha256": active_sources["runtime_source_manifest_sha256"],
            "buy_and_four_lifecycle_sources_exact": True,
        }
    )
    process_row["canonical_process_identity_sha256"] = resource_v8.document_sha256(
        process_row, "canonical_process_identity_sha256"
    )
    timestamp = generated_utc or _now()
    captured_at = max(
        _utc_datetime(process.get("captured_utc"), "active process capture timestamp"),
        _utc_datetime(recaptured.get("captured_utc"), "active process recapture timestamp"),
        datetime.fromtimestamp(
            float(health_window["rows"][1]["main_wall_timestamp_s"]),
            tz=UTC,
        ),
    )
    if _utc_datetime(timestamp, "active capture timestamp") < captured_at:
        raise ActiveCaptureV6Error("active capture receipt predates its process observation")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "generated_utc": timestamp,
        "runtime_authority": release_binding,
        "resource_receipt": resource_binding,
        "config_correction": dict(resource["config_correction"]),
        "host": dict(resource["host"]),
        "disabled_predecessor": {
            "pid": disabled_pid,
            "pid_start_ticks": int(disabled["pid_start_ticks"]),
            "process_identity_sha256": disabled["canonical_process_identity_sha256"],
            "quiescent_before_active_capture": True,
        },
        "active_process": process_row,
        "runtime_identity": dict(runtime_opened.payload),
        "runtime_identity_file_sha256": runtime_file_sha,
        "startup_semantics": semantics,
        "active_health_window": health_window,
        "checks": dict(CHECKS),
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = resource_v8.document_sha256(payload, CANONICAL_FIELD)
    return payload


def validate_active_capture(
    path: Path,
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    config_correction_path: Path,
    live_log_path: Path,
) -> dict[str, Any]:
    repository, _execution = _validate_runtime_repository(runtime_repository_root)
    release, release_binding = _validate_release(direct_release_path)
    resource, resource_binding = _validate_resource(
        resource_receipt_path,
        config_correction_path=config_correction_path,
    )
    active_sources = _capture_active_runtime_sources(repository, resource)
    disabled = _disabled_process(resource)
    opened = _open_private_json(path, "active-capture-v6 receipt")
    payload = opened.payload
    process = payload.get("active_process")
    predecessor = payload.get("disabled_predecessor")
    runtime = payload.get("runtime_identity")
    if (
        set(payload) != TOP_LEVEL_FIELDS
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != OWNER
        or payload.get("status") != STATUS
        or not isinstance(process, Mapping)
        or not isinstance(predecessor, Mapping)
        or not isinstance(runtime, Mapping)
        or payload.get("runtime_authority") != release_binding
        or payload.get("resource_receipt") != resource_binding
        or payload.get("config_correction") != resource.get("config_correction")
        or payload.get("host") != resource.get("host")
    ):
        raise ActiveCaptureV6Error("active-capture-v6 structure or content binding drifted")
    if (
        set(payload["runtime_authority"]) != CONTENT_BINDING_FIELDS
        or set(payload["resource_receipt"]) != CONTENT_BINDING_FIELDS
        or set(payload["config_correction"]) != CONTENT_BINDING_FIELDS
    ):
        raise ActiveCaptureV6Error("active capture contains non-content authority bindings")
    expected_predecessor = {
        "pid": int(disabled["pid"]),
        "pid_start_ticks": int(disabled["pid_start_ticks"]),
        "process_identity_sha256": disabled["canonical_process_identity_sha256"],
        "quiescent_before_active_capture": True,
    }
    active_pid = _strict_positive_int(process.get("pid"), "active PID")
    active_start = _strict_positive_int(process.get("pid_start_ticks"), "active start ticks")
    if (
        predecessor != expected_predecessor
        or active_pid == int(disabled["pid"])
        or active_start <= int(disabled["pid_start_ticks"])
        or process.get("execution_commit") != DIRECT_SUCCESSOR_EXECUTION_COMMIT
        or process.get("execution_tree") != DIRECT_SUCCESSOR_EXECUTION_TREE
        or process.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or process.get("artifact_sha256") != EXACT_ARTIFACT_SHA256
        or process.get("buy_e3_enabled") is not True
        or process.get("owner_override_effective") is not True
        or process.get("runtime_source_files") != active_sources["files"]
        or process.get("runtime_source_manifest_sha256")
        != active_sources["runtime_source_manifest_sha256"]
        or process.get("buy_and_four_lifecycle_sources_exact") is not True
    ):
        raise ActiveCaptureV6Error("active process transition or successor source identity drifted")
    process_body = dict(process)
    process_canonical = _require_sha256(
        process_body.pop("canonical_process_identity_sha256", None),
        "active process canonical identity",
    )
    if process_canonical != resource_v8.canonical_sha256(process_body):
        raise ActiveCaptureV6Error("active process canonical identity drifted")
    stable_process_sha = resource_v8.canonical_sha256(_stable_process_projection(process))
    health_window = _validate_active_health_window(
        payload.get("active_health_window"),
        live_log_path=live_log_path,
        expected_pid=active_pid,
        expected_start_ticks=active_start,
        expected_process_stable_identity_sha256=stable_process_sha,
    )
    runtime_file_sha = _require_sha256(
        payload.get("runtime_identity_file_sha256"), "runtime identity file"
    )
    process_runtime = process.get("runtime_identity")
    if (
        process.get("runtime_identity_file_sha256") != runtime_file_sha
        or not isinstance(process_runtime, Mapping)
        or process_runtime.get("file_sha256") != runtime_file_sha
    ):
        raise ActiveCaptureV6Error("active runtime identity file binding drifted")
    semantics = _runtime_semantics(
        runtime,
        process=process,
        release=release,
        release_binding=release_binding,
        expected_repository_root=repository,
        expected_release_path=None,
    )
    if (
        process.get("startup_attestation_sha256") != semantics["startup_attestation_sha256"]
        or payload.get("startup_semantics") != semantics
        or payload.get("checks") != CHECKS
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(CANONICAL_FIELD) != resource_v8.document_sha256(payload, CANONICAL_FIELD)
    ):
        raise ActiveCaptureV6Error("active-capture-v6 semantic identity drifted")
    generated = _utc_datetime(payload.get("generated_utc"), "active capture timestamp")
    health_observed = datetime.fromtimestamp(
        float(health_window["rows"][1]["main_wall_timestamp_s"]),
        tz=UTC,
    )
    if generated < health_observed:
        raise ActiveCaptureV6Error("active capture predates its fresh HEALTH window")
    return dict(payload)


def finalize_active_capture(
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    config_correction_path: Path,
    pid_file: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    live_log_path: Path,
    output_path: Path,
    proc_root: Path = Path("/proc"),
    generated_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    payload = build_active_capture(
        runtime_repository_root=runtime_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
        config_correction_path=config_correction_path,
        pid_file=pid_file,
        config_path=config_path,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        live_log_path=live_log_path,
        proc_root=proc_root,
        generated_utc=generated_utc,
    )
    try:
        file_sha = resource_v8.atomic_write_receipt(output_path, payload)
    except Exception as exc:
        raise ActiveCaptureV6Error("create-only active capture write failed") from exc
    observed = validate_active_capture(
        output_path,
        runtime_repository_root=runtime_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
        config_correction_path=config_correction_path,
        live_log_path=live_log_path,
    )
    if observed != payload:
        raise ActiveCaptureV6Error("active capture changed after write")
    return payload, file_sha


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture = subparsers.add_parser("capture")
    capture.add_argument("--runtime-repository-root", type=Path, required=True)
    capture.add_argument("--direct-release", type=Path, required=True)
    capture.add_argument("--resource-receipt", type=Path, required=True)
    capture.add_argument("--config-correction", type=Path, required=True)
    capture.add_argument("--pid-file", type=Path, required=True)
    capture.add_argument("--config", type=Path, required=True)
    capture.add_argument("--python", type=Path, required=True)
    capture.add_argument("--venv-root", type=Path, required=True)
    capture.add_argument("--runtime-identity", type=Path, required=True)
    capture.add_argument("--live-log", type=Path, required=True)
    capture.add_argument("--output", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--receipt", type=Path, required=True)
    validate.add_argument("--runtime-repository-root", type=Path, required=True)
    validate.add_argument("--direct-release", type=Path, required=True)
    validate.add_argument("--resource-receipt", type=Path, required=True)
    validate.add_argument("--config-correction", type=Path, required=True)
    validate.add_argument("--live-log", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "capture":
        payload, file_sha = finalize_active_capture(
            runtime_repository_root=args.runtime_repository_root,
            direct_release_path=args.direct_release,
            resource_receipt_path=args.resource_receipt,
            config_correction_path=args.config_correction,
            pid_file=args.pid_file,
            config_path=args.config,
            python_executable=args.python,
            venv_root=args.venv_root,
            runtime_identity_path=args.runtime_identity,
            live_log_path=args.live_log,
            output_path=args.output,
        )
    else:
        payload = validate_active_capture(
            args.receipt,
            runtime_repository_root=args.runtime_repository_root,
            direct_release_path=args.direct_release,
            resource_receipt_path=args.resource_receipt,
            config_correction_path=args.config_correction,
            live_log_path=args.live_log,
        )
        file_sha = resource_v8.file_sha256(args.receipt)
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "status": payload["status"],
                "canonical_sha256": payload[CANONICAL_FIELD],
                "file_sha256": file_sha,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
