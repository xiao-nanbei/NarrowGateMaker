#!/usr/bin/env python3
"""Capture the fresh external-venues-disabled BUY E3 process without authority.

This additive receipt succeeds the historical direct-v4 active-capture shape.
It binds the immutable direct-v4 release-v2, the independently collected v4
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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v5 as resource_v5,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
)
from scripts import deploy_f05_buy_e3_owner_v1 as deploy

OWNER: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
SCHEMA_VERSION: Final = f"{OWNER}.fresh_external_venues_disabled_active_process_capture.v3"
STATUS: Final = "fresh_external_venues_disabled_active_process_captured"
CANONICAL_FIELD: Final = "canonical_active_capture_sha256"

DIRECT_V4_EXECUTION_COMMIT: Final = "07ef93733a3a685caba945c7761a48473e403072"
DIRECT_V4_EXECUTION_TREE: Final = "ff505cd81a8eb11f2087d2ae27e7986fd99b0444"
DIRECT_V4_ANNOTATED_TAG: Final = "f05-owner-buy-e3-direct-live-v4-20260824"
DIRECT_V4_TAG_OBJECT: Final = "da83fa0b4aed00e4d04ea3faa212b2fb27a81f0d"
DIRECT_V4_RELEASE_FILE_SHA256: Final = (
    "ff888f4b5973563275c2b97e1554d45c9d686ef15d686440bf096521aab17fc2"
)
DIRECT_V4_RELEASE_CANONICAL_SHA256: Final = (
    "823ca1e4d53e968eb0afc53c4d2cad99cc17aac696548baa1700e800a4579702"
)
DIRECT_V4_RELEASE_SCHEMA: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_direct_owner_active_release.v2"
)
DIRECT_V4_RELEASE_STATUS: Final = "owner_authorized_direct_live_lifecycle_repair_pending_evidence"
ACTIVE_CONFIG_SHA256: Final = "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
EXACT_ARTIFACT_SHA256: Final = "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
RUNTIME_IDENTITY_SCHEMA: Final = "narrowgate_live_runtime_identity.v1"
STARTUP_ATTESTATION_SCHEMA: Final = "narrowgate_buy_e3_startup_attestation.v4"
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
STARTUP_SOURCE_ROLES: Final = (
    "buy_e3_runtime",
    "maker_engine",
    "live_config",
    "live_main",
    "live_runtime_policy",
    "live_ws_handler",
    "sell_runtime",
)

NO_AUTHORITY: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "hypothetical_live_actions_scored": False,
}
AUTHORITY_DESIGN: Final = {
    "runtime_authority": "immutable_direct_v4_owner_release_v2",
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
    "direct_v4_checkout_exact": True,
    "direct_v4_release_v2_exact": True,
    "resource_v5_semantic_exact": True,
    "active_config_exact": True,
    "actual_process_identity_exact": True,
    "runtime_identity_exact": True,
    "startup_attestation_accepted": True,
    "buy_e3_enabled": True,
    "owner_override_effective": True,
    "shadow_flags_disabled": True,
    "external_venues_disabled": True,
    "artifact_exact": True,
    "buy_and_lifecycle_runtime_sources_exact": True,
    "active_process_stable_during_capture": True,
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
        "host",
        "disabled_predecessor",
        "active_process",
        "runtime_identity",
        "runtime_identity_file_sha256",
        "startup_semantics",
        "checks",
        "authority_design",
        "permissions",
        "evidence_boundary",
        CANONICAL_FIELD,
    }
)
MAX_JSON_BYTES: Final = 64 << 20


class ActiveCaptureV5Error(RuntimeError):
    """Raised when the additive direct-v4 capture cannot prove exact identity."""


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
            raise ActiveCaptureV5Error(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _open_private_json(path: Path, label: str) -> OpenedJson:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_file():
        raise ActiveCaptureV5Error(f"{label} is not a regular file")
    target = candidate.resolve(strict=True)
    before = target.stat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_size <= 0
        or before.st_size > MAX_JSON_BYTES
    ):
        raise ActiveCaptureV5Error(f"{label} has unsafe identity or permissions")
    try:
        raw = target.read_bytes()
        payload = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ActiveCaptureV5Error(f"non-finite JSON value: {value}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ActiveCaptureV5Error(f"{label} is unreadable JSON") from exc
    after = target.stat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ActiveCaptureV5Error(f"{label} changed while read")
    if not isinstance(payload, dict):
        raise ActiveCaptureV5Error(f"{label} root is not an object")
    return OpenedJson(target, payload, raw, before)


def _require_sha256(value: Any, label: str) -> str:
    try:
        return resource_v5._require_sha256(value, label)  # noqa: SLF001
    except Exception as exc:
        raise ActiveCaptureV5Error(f"{label} is not SHA256") from exc


def _strict_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ActiveCaptureV5Error(f"{label} is not a positive integer")
    return value


def _timestamp(value: Any, label: str) -> str:
    normalized = str(value)
    if not normalized.endswith("Z"):
        raise ActiveCaptureV5Error(f"{label} is not UTC")
    try:
        observed = datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ActiveCaptureV5Error(f"{label} is invalid") from exc
    if observed.utcoffset() != UTC.utcoffset(observed):
        raise ActiveCaptureV5Error(f"{label} is not UTC")
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
        or canonical != resource_v5.document_sha256(opened.payload, canonical_field)
    ):
        raise ActiveCaptureV5Error("content binding semantic identity drifted")
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
        execution = resource_v5.capture_git_execution(
            repository,
            annotated_tag=DIRECT_V4_ANNOTATED_TAG,
            runtime_authority=True,
        )
    except Exception as exc:
        raise ActiveCaptureV5Error("active runtime checkout is not exact clean direct-v4") from exc
    expected = {
        "execution_commit": DIRECT_V4_EXECUTION_COMMIT,
        "execution_tree": DIRECT_V4_EXECUTION_TREE,
        "annotated_tag": DIRECT_V4_ANNOTATED_TAG,
        "annotated_tag_object": DIRECT_V4_TAG_OBJECT,
        "tag_peeled_commit": DIRECT_V4_EXECUTION_COMMIT,
    }
    if any(execution.get(field) != value for field, value in expected.items()):
        raise ActiveCaptureV5Error("active runtime Git identity drifted")
    return repository, execution


def _validate_release(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    opened = _open_private_json(path, "direct-v4 release-v2")
    binding = _content_binding(
        opened,
        canonical_field="canonical_active_release_sha256",
        expected_schema=DIRECT_V4_RELEASE_SCHEMA,
        expected_status=DIRECT_V4_RELEASE_STATUS,
    )
    try:
        resource_v5._validate_direct_release_payload(opened.payload)  # noqa: SLF001
    except Exception as exc:
        raise ActiveCaptureV5Error("direct-v4 release-v2 semantics drifted") from exc
    if (
        binding["file_sha256"] != DIRECT_V4_RELEASE_FILE_SHA256
        or binding["canonical_sha256"] != DIRECT_V4_RELEASE_CANONICAL_SHA256
    ):
        raise ActiveCaptureV5Error("direct-v4 release-v2 byte identity drifted")
    return dict(opened.payload), binding


def _validate_resource(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        validated = resource_v5.validate_resource_receipt(path)
    except Exception as exc:
        raise ActiveCaptureV5Error("resource-v4 receipt is invalid") from exc
    opened = _open_private_json(path, "resource-v4 receipt")
    binding = _content_binding(
        opened,
        canonical_field=resource_v5.RESOURCE_CANONICAL_FIELD,
        expected_schema=resource_v5.RESOURCE_SCHEMA,
        expected_status=resource_v5.RESOURCE_STATUS,
    )
    execution = validated.get("runtime_execution")
    authority = validated.get("authority_design")
    sources = validated.get("runtime_sources")
    deployed = validated.get("exact_deployed_files")
    if validated != opened.payload or not all(
        isinstance(value, Mapping) for value in (execution, authority, sources, deployed)
    ):
        raise ActiveCaptureV5Error("resource-v4 receipt changed or is incomplete")
    if (
        execution.get("execution_commit") != DIRECT_V4_EXECUTION_COMMIT
        or execution.get("execution_tree") != DIRECT_V4_EXECUTION_TREE
        or execution.get("annotated_tag") != DIRECT_V4_ANNOTATED_TAG
        or execution.get("annotated_tag_object") != DIRECT_V4_TAG_OBJECT
        or authority.get("runtime_authority_release_file_sha256") != DIRECT_V4_RELEASE_FILE_SHA256
        or authority.get("runtime_authority_release_canonical_sha256")
        != DIRECT_V4_RELEASE_CANONICAL_SHA256
        or authority.get("direct_v4_release_does_not_depend_on_resource_receipt") is not True
        or deployed.get("artifact_sha256") != EXACT_ARTIFACT_SHA256
        or sources.get("direct_v4_execution_commit") != DIRECT_V4_EXECUTION_COMMIT
    ):
        raise ActiveCaptureV5Error("resource-v4 authority semantics drifted")
    return dict(validated), binding


def _disabled_process(resource: Mapping[str, Any]) -> dict[str, Any]:
    raw = resource.get("fresh_disabled_process")
    if not isinstance(raw, Mapping):
        raise ActiveCaptureV5Error("resource-v4 lacks its disabled process")
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
    if process["config_sha256"] != resource_v5.EXPECTED_DISABLED_CONFIG_SHA256 or any(
        process[name] is not True
        for name in ("fresh_pid", "fresh_start_ticks", "same_pid_pre_post")
    ):
        raise ActiveCaptureV5Error("resource-v4 disabled process semantics drifted")
    return process


def _predecessor_is_quiescent(pid: int, *, proc_root: Path) -> bool:
    root = proc_root.expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ActiveCaptureV5Error("active capture requires a process filesystem")
    return not (root / str(pid)).exists()


def _read_pid(path: Path) -> int:
    candidate = path.expanduser().resolve(strict=True)
    try:
        return _strict_positive_int(
            int(candidate.read_text(encoding="ascii").strip()), "active PID"
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ActiveCaptureV5Error("active PID file is invalid") from exc


def _git_blob_sha256(repository: Path, relative: str) -> str:
    completed = subprocess.run(
        ("git", "show", f"{DIRECT_V4_EXECUTION_COMMIT}:{relative}"),
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
        raise ActiveCaptureV5Error("resource-v4 runtime source map is missing")
    rows: dict[str, dict[str, Any]] = {}
    for role, frozen in resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256.items():
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
            or resource_v5.file_sha256(candidate) != expected
            or _git_blob_sha256(repository, relative) != expected
        ):
            raise ActiveCaptureV5Error(f"active runtime source drifted: {role}")
        rows[role] = {
            "role": role,
            "repository_relative_path": relative,
            "sha256": expected,
            "active_working_matches_direct_v4": True,
            "direct_v4_commit_blob_matches": True,
            "resource_v5_binding_matches": True,
        }
    if not REQUIRED_ACTIVE_SOURCE_ROLES.issubset(rows):
        raise ActiveCaptureV5Error("BUY and lifecycle source roles are incomplete")
    return {
        "execution_commit": DIRECT_V4_EXECUTION_COMMIT,
        "files": rows,
        "runtime_source_manifest_sha256": resource_v5.canonical_sha256(rows),
        "buy_and_four_lifecycle_sources_exact": True,
    }


def _startup_source_plan() -> dict[str, Any]:
    files: dict[str, dict[str, str]] = {}
    for role in STARTUP_SOURCE_ROLES:
        frozen = resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256[role]
        sha = str(frozen["sha256"])
        files[role] = {
            "repository_relative_path": str(frozen["path"]),
            "artifact_manifest_sha256": sha,
            "execution_commit_blob_sha256": sha,
            "working_file_sha256": sha,
        }
    return {
        "files": files,
        "runtime_code_sha256": resource_v5.canonical_sha256(files),
    }


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
    }


def _runtime_semantics(
    runtime: Mapping[str, Any],
    *,
    process: Mapping[str, Any],
    release: Mapping[str, Any],
    release_binding: Mapping[str, Any],
    expected_release_path: str | None,
) -> dict[str, Any]:
    active_pid = _strict_positive_int(process.get("pid"), "active PID")
    runtime_release_path = str(runtime.get("f05_buy_e3_active_release_path", ""))
    if not PurePosixPath(runtime_release_path).is_absolute() or (
        expected_release_path is not None and runtime_release_path != expected_release_path
    ):
        raise ActiveCaptureV5Error("active runtime release path drifted")
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
            expected_execution_commit=DIRECT_V4_EXECUTION_COMMIT,
            expected_execution_tree=DIRECT_V4_EXECUTION_TREE,
            expected_runtime_sources=_startup_source_plan(),
            expected_active_release=_release_phase_binding(
                release_binding,
                runtime_release_path=runtime_release_path,
            ),
        )
    except Exception as exc:
        raise ActiveCaptureV5Error("active runtime/startup attestation is not exact v4") from exc
    roles = release.get("exact_artifact", {}).get("roles", {})
    startup_release = startup.get("buy_e3_active_release")
    gates = startup.get("gates")
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
        or runtime.get("f05_buy_e3_active_release_file_sha256") != DIRECT_V4_RELEASE_FILE_SHA256
        or runtime.get("f05_buy_e3_active_release_canonical_sha256")
        != DIRECT_V4_RELEASE_CANONICAL_SHA256
        or startup.get("schema_version") != STARTUP_ATTESTATION_SCHEMA
        or startup.get("status") != "accepted"
        or startup.get("errors") != []
        or not isinstance(gates, Mapping)
        or not gates
        or any(value is not True for value in gates.values())
        or not isinstance(startup_release, Mapping)
        or startup_release.get("execution_commit") != DIRECT_V4_EXECUTION_COMMIT
        or startup_release.get("execution_tree") != DIRECT_V4_EXECUTION_TREE
        or startup_release.get("annotated_operational_tag") != DIRECT_V4_ANNOTATED_TAG
        or startup_release.get("annotated_operational_tag_object") != DIRECT_V4_TAG_OBJECT
    ):
        raise ActiveCaptureV5Error("active runtime authority, artifact, or shadow state drifted")
    state = startup.get("fill_cooldown_state")
    if not isinstance(state, Mapping):
        raise ActiveCaptureV5Error("active startup cooldown state is missing")
    buy_identity = str(state.get("buy_deadline_identity", ""))
    if buy_identity not in {"B0", f"BUY_E3:{EXACT_ARTIFACT_SHA256}"} or (
        state.get("e3_deadline_imported") is True and buy_identity == "B0"
    ):
        raise ActiveCaptureV5Error("active startup imported an unsafe BUY deadline")
    return {
        "startup_attestation_sha256": resource_v5.canonical_sha256(startup),
        "startup_status": "accepted",
        "running_checkout_commit": DIRECT_V4_EXECUTION_COMMIT,
        "running_checkout_tree": DIRECT_V4_EXECUTION_TREE,
        "buy_deadline_identity": buy_identity,
        "fill_cooldown_restore_mode": state.get("restore_mode"),
        "buy_remaining_ms": state.get("buy_remaining_ms"),
        "e3_deadline_imported": state.get("e3_deadline_imported"),
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
        raise ActiveCaptureV5Error("actual active process identity capture failed") from exc


def build_active_capture(
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    pid_file: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    proc_root: Path = Path("/proc"),
    generated_utc: str | None = None,
) -> dict[str, Any]:
    repository, _execution = _validate_runtime_repository(runtime_repository_root)
    release, release_binding = _validate_release(direct_release_path)
    resource, resource_binding = _validate_resource(resource_receipt_path)
    disabled = _disabled_process(resource)
    disabled_pid = int(disabled["pid"])
    if not _predecessor_is_quiescent(disabled_pid, proc_root=proc_root):
        raise ActiveCaptureV5Error("disabled predecessor PID is still running")
    config = config_path.expanduser().resolve(strict=True)
    if resource_v5.file_sha256(config) != ACTIVE_CONFIG_SHA256:
        raise ActiveCaptureV5Error("active config bytes drifted")
    config_payload = resource_v5._load_yaml(config)  # noqa: SLF001
    external = resource_v5._mapping(  # noqa: SLF001
        config_payload.get("external_venues"), "external_venues config"
    )
    if external.get("enabled") is not False:
        raise ActiveCaptureV5Error("external venue shadow input is enabled")
    active_pid = _read_pid(pid_file)
    if active_pid == disabled_pid:
        raise ActiveCaptureV5Error("active restart reused the disabled PID")
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
        raise ActiveCaptureV5Error("active process did not start after its disabled predecessor")
    runtime_opened = _open_private_json(runtime_identity_path, "active runtime identity")
    runtime_file_sha = hashlib.sha256(runtime_opened.raw).hexdigest()
    process_runtime = process.get("runtime_identity")
    if (
        not isinstance(process_runtime, Mapping)
        or process_runtime.get("present") is not True
        or process_runtime.get("file_sha256") != runtime_file_sha
    ):
        raise ActiveCaptureV5Error("actual process and runtime identity bytes differ")
    active_sources = _capture_active_runtime_sources(repository, resource)
    release_resolved = str(direct_release_path.expanduser().resolve(strict=True))
    semantics = _runtime_semantics(
        runtime_opened.payload,
        process=process,
        release=release,
        release_binding=release_binding,
        expected_release_path=release_resolved,
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
        raise ActiveCaptureV5Error("active process changed during capture")
    process_row = dict(process)
    process_row.pop("canonical_process_identity_sha256", None)
    process_row.update(
        {
            "execution_commit": DIRECT_V4_EXECUTION_COMMIT,
            "execution_tree": DIRECT_V4_EXECUTION_TREE,
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
    process_row["canonical_process_identity_sha256"] = resource_v5.document_sha256(
        process_row, "canonical_process_identity_sha256"
    )
    timestamp = generated_utc or _now()
    captured_at = max(
        _utc_datetime(process.get("captured_utc"), "active process capture timestamp"),
        _utc_datetime(recaptured.get("captured_utc"), "active process recapture timestamp"),
    )
    if _utc_datetime(timestamp, "active capture timestamp") < captured_at:
        raise ActiveCaptureV5Error("active capture receipt predates its process observation")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "generated_utc": timestamp,
        "runtime_authority": release_binding,
        "resource_receipt": resource_binding,
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
        "checks": dict(CHECKS),
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = resource_v5.document_sha256(payload, CANONICAL_FIELD)
    return payload


def validate_active_capture(
    path: Path,
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
) -> dict[str, Any]:
    repository, _execution = _validate_runtime_repository(runtime_repository_root)
    release, release_binding = _validate_release(direct_release_path)
    resource, resource_binding = _validate_resource(resource_receipt_path)
    active_sources = _capture_active_runtime_sources(repository, resource)
    disabled = _disabled_process(resource)
    opened = _open_private_json(path, "active-capture-v4 receipt")
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
        or payload.get("host") != resource.get("host")
    ):
        raise ActiveCaptureV5Error("active-capture-v4 structure or content binding drifted")
    if (
        set(payload["runtime_authority"]) != CONTENT_BINDING_FIELDS
        or set(payload["resource_receipt"]) != CONTENT_BINDING_FIELDS
    ):
        raise ActiveCaptureV5Error("active capture contains non-content authority bindings")
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
        or process.get("execution_commit") != DIRECT_V4_EXECUTION_COMMIT
        or process.get("execution_tree") != DIRECT_V4_EXECUTION_TREE
        or process.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or process.get("artifact_sha256") != EXACT_ARTIFACT_SHA256
        or process.get("buy_e3_enabled") is not True
        or process.get("owner_override_effective") is not True
        or process.get("runtime_source_files") != active_sources["files"]
        or process.get("runtime_source_manifest_sha256")
        != active_sources["runtime_source_manifest_sha256"]
        or process.get("buy_and_four_lifecycle_sources_exact") is not True
    ):
        raise ActiveCaptureV5Error("active process transition or v4 source identity drifted")
    process_body = dict(process)
    process_canonical = _require_sha256(
        process_body.pop("canonical_process_identity_sha256", None),
        "active process canonical identity",
    )
    if process_canonical != resource_v5.canonical_sha256(process_body):
        raise ActiveCaptureV5Error("active process canonical identity drifted")
    runtime_file_sha = _require_sha256(
        payload.get("runtime_identity_file_sha256"), "runtime identity file"
    )
    process_runtime = process.get("runtime_identity")
    if (
        process.get("runtime_identity_file_sha256") != runtime_file_sha
        or not isinstance(process_runtime, Mapping)
        or process_runtime.get("file_sha256") != runtime_file_sha
    ):
        raise ActiveCaptureV5Error("active runtime identity file binding drifted")
    semantics = _runtime_semantics(
        runtime,
        process=process,
        release=release,
        release_binding=release_binding,
        expected_release_path=None,
    )
    if (
        process.get("startup_attestation_sha256") != semantics["startup_attestation_sha256"]
        or payload.get("startup_semantics") != semantics
        or payload.get("checks") != CHECKS
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(CANONICAL_FIELD) != resource_v5.document_sha256(payload, CANONICAL_FIELD)
    ):
        raise ActiveCaptureV5Error("active-capture-v4 semantic identity drifted")
    _timestamp(payload.get("generated_utc"), "active capture timestamp")
    return dict(payload)


def finalize_active_capture(
    *,
    runtime_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    pid_file: Path,
    config_path: Path,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    output_path: Path,
    proc_root: Path = Path("/proc"),
    generated_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    payload = build_active_capture(
        runtime_repository_root=runtime_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
        pid_file=pid_file,
        config_path=config_path,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        proc_root=proc_root,
        generated_utc=generated_utc,
    )
    try:
        file_sha = resource_v5.atomic_write_receipt(output_path, payload)
    except Exception as exc:
        raise ActiveCaptureV5Error("create-only active capture write failed") from exc
    observed = validate_active_capture(
        output_path,
        runtime_repository_root=runtime_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
    )
    if observed != payload:
        raise ActiveCaptureV5Error("active capture changed after write")
    return payload, file_sha


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture = subparsers.add_parser("capture")
    capture.add_argument("--runtime-repository-root", type=Path, required=True)
    capture.add_argument("--direct-release", type=Path, required=True)
    capture.add_argument("--resource-receipt", type=Path, required=True)
    capture.add_argument("--pid-file", type=Path, required=True)
    capture.add_argument("--config", type=Path, required=True)
    capture.add_argument("--python", type=Path, required=True)
    capture.add_argument("--venv-root", type=Path, required=True)
    capture.add_argument("--runtime-identity", type=Path, required=True)
    capture.add_argument("--output", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--receipt", type=Path, required=True)
    validate.add_argument("--runtime-repository-root", type=Path, required=True)
    validate.add_argument("--direct-release", type=Path, required=True)
    validate.add_argument("--resource-receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "capture":
        payload, file_sha = finalize_active_capture(
            runtime_repository_root=args.runtime_repository_root,
            direct_release_path=args.direct_release,
            resource_receipt_path=args.resource_receipt,
            pid_file=args.pid_file,
            config_path=args.config,
            python_executable=args.python,
            venv_root=args.venv_root,
            runtime_identity_path=args.runtime_identity,
            output_path=args.output,
        )
    else:
        payload = validate_active_capture(
            args.receipt,
            runtime_repository_root=args.runtime_repository_root,
            direct_release_path=args.direct_release,
            resource_receipt_path=args.resource_receipt,
        )
        file_sha = resource_v5.file_sha256(args.receipt)
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
