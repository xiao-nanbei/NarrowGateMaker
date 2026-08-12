"""Build the local-only attempt7 journal successor from the safe baseline.

The safe predecessor already enforces ``warmup_before_websocket.v1`` and has
the lifecycle journal disabled.  This builder layers the frozen attempt6
journal-v2 implementation over that predecessor, except that the live writer
must come from the current optimized repository source.  It never invokes
SSH, deploys files, mutates a current pointer, or grants strategy authority.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import shutil
import sys
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "prospective_lifecycle_from_safe_baseline.v1"
RELEASE_ID = "prospective_lifecycle_journal_v2_safe_baseline_attempt7_20260805"
MANIFEST_NAME = "release_manifest.json"
STARTUP_CONTRACT = "warmup_before_websocket.v1"

EPHEMERAL_ROOT = Path(
    os.environ.get("NARROWGATE_EPHEMERAL_ROOT", tempfile.gettempdir())
).expanduser()
SAFE_BASELINE_ROOT_DEFAULT = (
    EPHEMERAL_ROOT / "narrowgate_warmup_before_websocket_baseline_successor_v1_20260805"
)
ATTEMPT6_ROOT_DEFAULT = (
    EPHEMERAL_ROOT
    / "narrowgate_prospective_lifecycle_narrow_release_v1_20260805_attempt6"
)

SAFE_MANIFEST_NAME = "baseline_successor_manifest.json"
SAFE_MANIFEST_SCHEMA = "warmup_before_websocket_baseline_successor.v1"
SAFE_MANIFEST_CANONICAL_SHA256 = "864f11f8cc40de06505e1b6121ca8dbcbb1d315cd0bf7a3aaeb7a6742d23a57b"
SAFE_MANIFEST_FILE_SHA256 = "0bb093655e4125dbfceb2e874acdda9856e153ed9e182dcc89f400428ce4e8f4"

ATTEMPT6_MANIFEST_SCHEMA = "prospective_lifecycle_narrow_release.v1"
ATTEMPT6_MANIFEST_CANONICAL_SHA256 = (
    "58335e942d2fed1dc0169d100c05ba7335fcd09981b6dd6e9a605e816ac0770c"
)
ATTEMPT6_MANIFEST_FILE_SHA256 = "7cb518a2f546930f26c45e168c609ac73788927e9f253aa9c4a0a1410ec3f207"

LIVE_WRITER_PATH = "execution/order_lifecycle_live_writer_v2.py"
ATTEMPT6_WRITER_SHA256 = "b66c923bf1a2f76afbde62c91ac7b3bc45448b629563a2d549e5029cbe71d450"
OPTIMIZED_WRITER_SHA256 = "48daae0a60f4d5cf3909d544abd85d5f6393a5d1a582dfc0793a407b3b213749"

SAFE_RUNTIME_PATHS = (
    "live/config.py",
    "live/config.yaml",
    "live/main.py",
    "live/ws_handler.py",
    "strategy/maker_engine.py",
    "strategy/order_manager.py",
)

EXPECTED_ACTION_ENABLEMENT = {
    "ml_enabled": True,
    "dynamic_fill_hazard_shadow_enabled": True,
    "dynamic_fill_hazard_action_enabled": False,
    "buy_fill_selection_shadow_enabled": False,
    "buy_fill_selection_live_enabled": False,
    "buy_fill_selection_live_model_path": "",
}

PERFORMANCE_LIMITS = {
    "minimum_collection_duration_s": 3500.0,
    "maximum_collection_duration_s": 3700.0,
    "producer_enqueue_p99_us": 100.0,
    "producer_enqueue_max_us": 1000.0,
    "quote_loop_p99_regression_pct": 5.0,
    "writer_queue_hwm": 2048,
    "writer_cpu_pct_one_core": 10.0,
    "writer_rss_delta_mib": 256.0,
    "writer_write_p99_ms": 250.0,
}

REQUIRED_RUNTIME_EVIDENCE = (
    "remote_python_3_12_13_verified",
    "remote_pyarrow_24_0_0_verified",
    "native_extension_path_and_sha256_bound",
    "staged_overlay_import_smoke_passed",
    "targeted_lifecycle_tests_passed_on_remote_venv",
    "initial_state_13_domain_completeness_passed",
    "zero_pre_epoch_native_events",
    "one_hour_zero_drop_zero_error",
    "producer_enqueue_p99_le_100us",
    "producer_enqueue_max_le_1000us",
    "quote_loop_p99_regression_le_5pct",
    "writer_queue_hwm_le_2048",
    "writer_cpu_le_10pct_one_core",
    "writer_rss_delta_le_256mib",
    "writer_write_p99_le_250ms",
    "maker_thread_filesystem_calls_zero",
    "bounded_spool_admission_roundtrip_passed",
    "rollback_restart_rehearsed",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _read_object(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _require_regular_file(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{label} must be a regular file: {path}")
    observed = _sha256(path)
    if observed != expected_sha256:
        raise ValueError(
            f"{label} SHA256 mismatch: expected={expected_sha256} observed={observed} path={path}"
        )


def _validate_canonical_manifest(
    path: Path,
    *,
    label: str,
    schema_version: str,
    canonical_sha256: str,
    file_sha256: str,
) -> dict[str, Any]:
    _require_regular_file(path, file_sha256, f"{label} file")
    payload = _read_object(path, label)
    if payload.get("schema_version") != schema_version:
        raise ValueError(f"{label} schema drifted")
    claimed = str(payload.get("manifest_sha256", ""))
    without_hash = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    actual = _canonical_sha256(without_hash)
    if claimed != canonical_sha256 or actual != canonical_sha256:
        raise ValueError(
            f"{label} canonical hash mismatch: expected={canonical_sha256} "
            f"claimed={claimed} actual={actual}"
        )
    return payload


def _records_by_path(manifest: Mapping[str, Any], label: str) -> dict[str, dict[str, Any]]:
    records = manifest.get("files")
    if not isinstance(records, list):
        raise ValueError(f"{label} files must be a list")
    result: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise ValueError(f"{label} file record must be an object")
        logical_path = str(record.get("path", ""))
        if not logical_path or logical_path in result:
            raise ValueError(f"{label} file paths must be non-empty and unique")
        result[logical_path] = record
    return result


def _validate_stage_files(
    root: Path,
    *,
    manifest_name: str,
    records: Mapping[str, Mapping[str, Any]],
    label: str,
) -> None:
    expected_paths = {manifest_name, *records}
    actual_paths = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if actual_paths != expected_paths:
        raise ValueError(
            f"{label} file set drifted: missing={sorted(expected_paths - actual_paths)} "
            f"extra={sorted(actual_paths - expected_paths)}"
        )
    for logical_path, record in records.items():
        path = (root / logical_path).resolve(strict=True)
        if root not in path.parents or path.is_symlink() or not path.is_file():
            raise ValueError(f"{label} file escaped or is not regular: {logical_path}")
        if _sha256(path) != record.get("sha256"):
            raise ValueError(f"{label} file hash mismatch: {logical_path}")
        if path.stat().st_size != record.get("size_bytes"):
            raise ValueError(f"{label} file size mismatch: {logical_path}")
        if logical_path.endswith(".py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _action_enablement(config: Mapping[str, Any]) -> dict[str, Any]:
    strategy = config.get("strategy")
    ml = config.get("ml")
    if not isinstance(strategy, Mapping) or not isinstance(ml, Mapping):
        raise ValueError("config lacks strategy or ml mapping")
    return {
        "ml_enabled": ml.get("enabled"),
        "dynamic_fill_hazard_shadow_enabled": strategy.get("dynamic_fill_hazard_shadow_enabled"),
        "dynamic_fill_hazard_action_enabled": strategy.get("dynamic_fill_hazard_action_enabled"),
        "buy_fill_selection_shadow_enabled": strategy.get("buy_fill_selection_shadow_enabled"),
        "buy_fill_selection_live_enabled": strategy.get("buy_fill_selection_live_enabled"),
        "buy_fill_selection_live_model_path": strategy.get("buy_fill_selection_live_model_path"),
    }


def _validate_safe_baseline(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    resolved = root.expanduser().resolve(strict=True)
    if not resolved.is_dir() or resolved.is_symlink():
        raise ValueError("safe baseline root must be a non-symlink directory")
    manifest = _validate_canonical_manifest(
        resolved / SAFE_MANIFEST_NAME,
        label="safe baseline manifest",
        schema_version=SAFE_MANIFEST_SCHEMA,
        canonical_sha256=SAFE_MANIFEST_CANONICAL_SHA256,
        file_sha256=SAFE_MANIFEST_FILE_SHA256,
    )
    records = _records_by_path(manifest, "safe baseline manifest")
    if set(records) != set(SAFE_RUNTIME_PATHS):
        raise ValueError("safe baseline must bind exactly the six runtime files")
    _validate_stage_files(
        resolved,
        manifest_name=SAFE_MANIFEST_NAME,
        records=records,
        label="safe baseline stage",
    )
    if manifest.get("startup_contract") != STARTUP_CONTRACT:
        raise ValueError("safe baseline startup contract drifted")
    expected_journal = {
        "lifecycle_journal_enabled": False,
        "lifecycle_journal_config_present": False,
        "lifecycle_journal_runtime_imported": False,
        "journal_payload_files_included": False,
        "economic_outcomes_read": False,
    }
    if manifest.get("journal_boundary") != expected_journal:
        raise ValueError("safe baseline is not explicitly journal-OFF")
    equality = manifest.get("strategy_config_semantic_equality")
    if not isinstance(equality, Mapping) or equality.get("passed") is not True:
        raise ValueError("safe baseline semantic equality is not bound")
    if equality.get("action_enablement") != EXPECTED_ACTION_ENABLEMENT:
        raise ValueError("safe baseline action enablement drifted")
    permissions = manifest.get("permissions")
    if not isinstance(permissions, Mapping) or any(
        permissions.get(field) is not False
        for field in (
            "deployment_executed",
            "deployment_authorized",
            "rollback_authorized",
            "strategy_action_authorized",
            "economic_research_authorized",
        )
    ):
        raise ValueError("safe baseline permission boundary drifted")
    return manifest, records


def _validate_attempt6(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    resolved = root.expanduser().resolve(strict=True)
    if not resolved.is_dir() or resolved.is_symlink():
        raise ValueError("attempt6 root must be a non-symlink directory")
    manifest = _validate_canonical_manifest(
        resolved / MANIFEST_NAME,
        label="attempt6 manifest",
        schema_version=ATTEMPT6_MANIFEST_SCHEMA,
        canonical_sha256=ATTEMPT6_MANIFEST_CANONICAL_SHA256,
        file_sha256=ATTEMPT6_MANIFEST_FILE_SHA256,
    )
    records = _records_by_path(manifest, "attempt6 manifest")
    _validate_stage_files(
        resolved,
        manifest_name=MANIFEST_NAME,
        records=records,
        label="attempt6 stage",
    )
    writer = records.get(LIVE_WRITER_PATH)
    if writer is None or writer.get("sha256") != ATTEMPT6_WRITER_SHA256:
        raise ValueError("attempt6 writer identity drifted")
    lifecycle = manifest.get("config_semantics", {}).get("lifecycle_journal_v2")
    if not isinstance(lifecycle, Mapping):
        raise ValueError("attempt6 lifecycle config is missing")
    if lifecycle.get("enabled") is not True:
        raise ValueError("attempt6 lifecycle journal is not enabled")
    if lifecycle.get("storage_profile") != "bounded_remote_spool":
        raise ValueError("attempt6 lifecycle storage is not bounded_remote_spool")
    if lifecycle.get("remote_session_max_duration_s") != 3600:
        raise ValueError("attempt6 bounded duration drifted")
    if tuple(manifest.get("runtime_evidence_required", ())) != REQUIRED_RUNTIME_EVIDENCE:
        raise ValueError("attempt6 runtime evidence gates drifted")
    if manifest.get("deployment_authorized") is not False:
        raise ValueError("attempt6 unexpectedly authorizes deployment")
    return manifest, records


def _validate_writer_source(path: Path) -> Path:
    resolved = path.expanduser().resolve(strict=True)
    _require_regular_file(resolved, OPTIMIZED_WRITER_SHA256, "optimized live writer")
    if OPTIMIZED_WRITER_SHA256 == ATTEMPT6_WRITER_SHA256:
        raise AssertionError("optimized writer unexpectedly equals the attempt6 writer")
    ast.parse(resolved.read_text(encoding="utf-8"), filename=str(resolved))
    return resolved


def _write_exclusive(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    path.chmod(0o644)


def _safe_file_hashes(records: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    return {path: str(records[path]["sha256"]) for path in sorted(records)}


def _candidate_record(
    *,
    logical_path: str,
    candidate_path: Path,
    safe_records: Mapping[str, Mapping[str, Any]],
    attempt6_records: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": logical_path,
        "sha256": _sha256(candidate_path),
        "size_bytes": candidate_path.stat().st_size,
        "attempt6_template_sha256": attempt6_records[logical_path]["sha256"],
    }
    if logical_path == LIVE_WRITER_PATH:
        record.update(
            {
                "mode": "add_current_optimized_source",
                "source_sha256": OPTIMIZED_WRITER_SHA256,
                "predecessor_state": "absent_journal_off",
            }
        )
    elif logical_path in safe_records:
        record.update(
            {
                "mode": "patch_safe_baseline_journal_v2_only",
                "predecessor_sha256": safe_records[logical_path]["sha256"],
            }
        )
    else:
        record.update(
            {
                "mode": "add_attempt6_journal_v2_payload",
                "predecessor_state": "absent_journal_off",
            }
        )
    return record


def _stable_start_probe(safe_manifest: Mapping[str, Any]) -> dict[str, Any]:
    rollback = safe_manifest.get("rollback_binding")
    if not isinstance(rollback, Mapping):
        raise ValueError("safe baseline rollback binding is missing")
    probe = rollback.get("stable_start_probe")
    if not isinstance(probe, dict):
        raise ValueError("safe baseline stable-start probe is missing")
    return dict(probe)


def _build_payload(
    *,
    repo_root: Path,
    stage_root: Path,
    safe_root: Path,
    attempt6_root: Path,
    safe_manifest: Mapping[str, Any],
    safe_records: Mapping[str, Mapping[str, Any]],
    attempt6_manifest: Mapping[str, Any],
    attempt6_records: Mapping[str, Mapping[str, Any]],
    writer_source: Path,
) -> dict[str, Any]:
    del repo_root
    records: list[dict[str, Any]] = []
    for logical_path in sorted(attempt6_records):
        source = writer_source if logical_path == LIVE_WRITER_PATH else attempt6_root / logical_path
        output = stage_root / logical_path
        _write_exclusive(output, source.read_bytes())
        records.append(
            _candidate_record(
                logical_path=logical_path,
                candidate_path=output,
                safe_records=safe_records,
                attempt6_records=attempt6_records,
            )
        )

    safe_config = yaml.safe_load((safe_root / "live/config.yaml").read_text(encoding="utf-8"))
    candidate_config = yaml.safe_load((stage_root / "live/config.yaml").read_text(encoding="utf-8"))
    if not isinstance(safe_config, dict) or not isinstance(candidate_config, dict):
        raise ValueError("safe and candidate configs must be mappings")
    lifecycle = candidate_config.pop("lifecycle_journal_v2", None)
    if candidate_config != safe_config:
        raise ValueError("candidate changes safe baseline config beyond lifecycle_journal_v2")
    attempt6_lifecycle = attempt6_manifest["config_semantics"]["lifecycle_journal_v2"]
    if lifecycle != attempt6_lifecycle:
        raise ValueError("candidate lifecycle config differs from attempt6 semantics")
    actions = _action_enablement(candidate_config)
    if actions != EXPECTED_ACTION_ENABLEMENT:
        raise ValueError("candidate action enablement differs from safe baseline")

    payload_paths = set(attempt6_records)
    added_paths = sorted(payload_paths - set(SAFE_RUNTIME_PATHS))
    safe_hashes = _safe_file_hashes(safe_records)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "build_mode": "deterministic_local_stage_only_no_ssh_no_deploy",
        "startup_contract": STARTUP_CONTRACT,
        "predecessor_binding": {
            "role": "safe_rollback_baseline",
            "release_id": safe_manifest["release_id"],
            "successor_baseline_id": safe_manifest["successor_baseline_id"],
            "manifest_schema": SAFE_MANIFEST_SCHEMA,
            "manifest_canonical_sha256": SAFE_MANIFEST_CANONICAL_SHA256,
            "manifest_file_sha256": SAFE_MANIFEST_FILE_SHA256,
            "startup_contract": STARTUP_CONTRACT,
            "runtime_file_sha256": safe_hashes,
            "journal_state": {
                "enabled": False,
                "config_present": False,
                "runtime_imported": False,
                "payload_files_present": False,
            },
            "journal_payload_absent_paths": added_paths,
        },
        "attempt6_semantics_binding": {
            "release_id": attempt6_manifest["release_id"],
            "manifest_schema": ATTEMPT6_MANIFEST_SCHEMA,
            "manifest_canonical_sha256": ATTEMPT6_MANIFEST_CANONICAL_SHA256,
            "manifest_file_sha256": ATTEMPT6_MANIFEST_FILE_SHA256,
            "payload_paths": sorted(attempt6_records),
            "journal_config_sha256": _canonical_sha256(attempt6_lifecycle),
            "old_writer_sha256": ATTEMPT6_WRITER_SHA256,
            "candidate_payload_equal_except": [LIVE_WRITER_PATH],
        },
        "source_bindings": {
            LIVE_WRITER_PATH: {
                "role": "current_optimized_repository_source",
                "sha256": OPTIMIZED_WRITER_SHA256,
                "attempt6_sha256": ATTEMPT6_WRITER_SHA256,
                "attempt6_writer_reused": False,
            }
        },
        "files": records,
        "config_semantics": {
            "safe_config_sha256": safe_hashes["live/config.yaml"],
            "all_safe_fields_unchanged": True,
            "only_new_top_level_section": "lifecycle_journal_v2",
            "lifecycle_journal_v2": lifecycle,
        },
        "strategy_semantics": {
            "unchanged_from_safe_baseline": True,
            "strategy_or_quote_parameters_changed": False,
            "model_binding_equal": True,
            "p3_binding_equal": True,
            "action_enablement_equal": True,
            "action_enablement": actions,
            "model": safe_manifest["strategy_config_semantic_equality"]["model"],
            "p3": safe_manifest["strategy_config_semantic_equality"]["p3"],
            "only_runtime_delta": "bounded lifecycle journal v2 collection",
        },
        "journal_boundary": {
            "predecessor_enabled": False,
            "candidate_enabled": True,
            "storage_profile": "bounded_remote_spool",
            "max_session_duration_s": 3600,
            "economic_outcomes_read": False,
            "orders_mutated_by_journal": False,
            "strategy_actions_changed": False,
        },
        "performance_limits": dict(PERFORMANCE_LIMITS),
        "runtime_evidence_required": list(REQUIRED_RUNTIME_EVIDENCE),
        "rollback_binding": {
            "target_role": "safe_rollback_baseline",
            "target_baseline_id": safe_manifest["successor_baseline_id"],
            "target_manifest_canonical_sha256": SAFE_MANIFEST_CANONICAL_SHA256,
            "startup_contract": STARTUP_CONTRACT,
            "restore_runtime_file_sha256": safe_hashes,
            "remove_journal_payload_paths": added_paths,
            "resulting_journal_enabled": False,
            "stable_start_probe": _stable_start_probe(safe_manifest),
            "rollback_authorized": False,
            "deployment_evidence_bound": False,
        },
        "permissions": {
            "local_stage_built": True,
            "deployment_executed": False,
            "deployment_authorized": False,
            "rollback_authorized": False,
            "registry_modified": False,
            "current_pointer_modified": False,
            "economic_research_authorized": False,
            "strategy_action_authorized": False,
            "live_policy_authorized": False,
            "q90_action_authorized": False,
            "buy_fill_selection_action_authorized": False,
        },
        "blockers": [
            {
                "id": "runtime_evidence_not_yet_bound",
                "detail": list(REQUIRED_RUNTIME_EVIDENCE),
            }
        ],
    }
    manifest["manifest_sha256"] = _canonical_sha256(manifest)
    _write_exclusive(
        stage_root / MANIFEST_NAME,
        (
            json.dumps(
                manifest,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8"),
    )
    return manifest


def validate_staging(
    candidate_root: Path,
    *,
    repo_root: Path | None = None,
    safe_baseline_root: Path = SAFE_BASELINE_ROOT_DEFAULT,
    attempt6_root: Path = ATTEMPT6_ROOT_DEFAULT,
    writer_source: Path | None = None,
) -> dict[str, Any]:
    repo = (repo_root or Path(__file__).resolve().parents[1]).expanduser().resolve(strict=True)
    safe_root = safe_baseline_root.expanduser().resolve(strict=True)
    attempt_root = attempt6_root.expanduser().resolve(strict=True)
    writer = _validate_writer_source(writer_source or repo / LIVE_WRITER_PATH)
    safe_manifest, safe_records = _validate_safe_baseline(safe_root)
    attempt6_manifest, attempt6_records = _validate_attempt6(attempt_root)

    root = candidate_root.expanduser().resolve(strict=True)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("candidate root must be a non-symlink directory")
    manifest = _read_object(root / MANIFEST_NAME, "attempt7 release manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("attempt7 release manifest schema drifted")
    claimed = str(manifest.get("manifest_sha256", ""))
    without_hash = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if claimed != _canonical_sha256(without_hash):
        raise ValueError("attempt7 release manifest canonical hash mismatch")

    records = _records_by_path(manifest, "attempt7 release manifest")
    if set(records) != set(attempt6_records):
        raise ValueError("attempt7 payload paths must equal attempt6 payload paths")
    _validate_stage_files(
        root,
        manifest_name=MANIFEST_NAME,
        records=records,
        label="attempt7 stage",
    )

    differing_from_attempt6: list[str] = []
    for logical_path in sorted(attempt6_records):
        candidate_path = root / logical_path
        attempt6_path = attempt_root / logical_path
        if candidate_path.read_bytes() != attempt6_path.read_bytes():
            differing_from_attempt6.append(logical_path)
        record = records[logical_path]
        if record.get("attempt6_template_sha256") != attempt6_records[logical_path]["sha256"]:
            raise ValueError(f"attempt6 template binding drifted: {logical_path}")
        if logical_path == LIVE_WRITER_PATH:
            if record.get("mode") != "add_current_optimized_source":
                raise ValueError("optimized writer mode drifted")
            if record.get("source_sha256") != OPTIMIZED_WRITER_SHA256:
                raise ValueError("optimized writer source binding drifted")
            if candidate_path.read_bytes() != writer.read_bytes():
                raise ValueError("candidate writer is not the current optimized source")
            if _sha256(candidate_path) == ATTEMPT6_WRITER_SHA256:
                raise ValueError("candidate silently reused the old attempt6 writer")
        elif logical_path in safe_records:
            if record.get("mode") != "patch_safe_baseline_journal_v2_only":
                raise ValueError(f"safe baseline patch mode drifted: {logical_path}")
            if record.get("predecessor_sha256") != safe_records[logical_path]["sha256"]:
                raise ValueError(f"safe predecessor hash drifted: {logical_path}")
        elif record.get("mode") != "add_attempt6_journal_v2_payload":
            raise ValueError(f"journal payload mode drifted: {logical_path}")
    if differing_from_attempt6 != [LIVE_WRITER_PATH]:
        raise ValueError(
            "attempt7 payload must differ from attempt6 only at the optimized writer: "
            f"observed={differing_from_attempt6}"
        )

    predecessor = manifest.get("predecessor_binding")
    if not isinstance(predecessor, Mapping):
        raise ValueError("safe predecessor binding is missing")
    expected_safe_hashes = _safe_file_hashes(safe_records)
    if predecessor.get("manifest_canonical_sha256") != SAFE_MANIFEST_CANONICAL_SHA256:
        raise ValueError("safe predecessor manifest binding drifted")
    if predecessor.get("runtime_file_sha256") != expected_safe_hashes:
        raise ValueError("safe predecessor six-file hashes drifted")
    if predecessor.get("startup_contract") != STARTUP_CONTRACT:
        raise ValueError("safe predecessor startup contract drifted")
    if predecessor.get("journal_state") != {
        "enabled": False,
        "config_present": False,
        "runtime_imported": False,
        "payload_files_present": False,
    }:
        raise ValueError("safe predecessor journal absence is not explicit")

    attempt_binding = manifest.get("attempt6_semantics_binding")
    if not isinstance(attempt_binding, Mapping):
        raise ValueError("attempt6 semantics binding is missing")
    if attempt_binding.get("manifest_canonical_sha256") != ATTEMPT6_MANIFEST_CANONICAL_SHA256:
        raise ValueError("attempt6 semantics manifest binding drifted")
    if attempt_binding.get("candidate_payload_equal_except") != [LIVE_WRITER_PATH]:
        raise ValueError("attempt6 payload difference allowlist drifted")

    safe_config = yaml.safe_load((safe_root / "live/config.yaml").read_text(encoding="utf-8"))
    candidate_config = yaml.safe_load((root / "live/config.yaml").read_text(encoding="utf-8"))
    if not isinstance(safe_config, dict) or not isinstance(candidate_config, dict):
        raise ValueError("safe and candidate configs must be mappings")
    lifecycle = candidate_config.pop("lifecycle_journal_v2", None)
    if candidate_config != safe_config:
        raise ValueError("candidate changed strategy config beyond lifecycle_journal_v2")
    expected_lifecycle = attempt6_manifest["config_semantics"]["lifecycle_journal_v2"]
    if lifecycle != expected_lifecycle or lifecycle.get("enabled") is not True:
        raise ValueError("candidate journal is not the frozen enabled attempt6 journal")
    if _action_enablement(candidate_config) != EXPECTED_ACTION_ENABLEMENT:
        raise ValueError("candidate action enablement drifted")

    if manifest.get("startup_contract") != STARTUP_CONTRACT:
        raise ValueError("attempt7 startup contract drifted")
    strategy = manifest.get("strategy_semantics")
    if not isinstance(strategy, Mapping):
        raise ValueError("strategy semantics are missing")
    if strategy.get("action_enablement") != EXPECTED_ACTION_ENABLEMENT:
        raise ValueError("manifest action semantics drifted")
    if strategy.get("strategy_or_quote_parameters_changed") is not False:
        raise ValueError("attempt7 cannot change strategy or quote parameters")
    journal = manifest.get("journal_boundary")
    if not isinstance(journal, Mapping) or journal.get("candidate_enabled") is not True:
        raise ValueError("attempt7 candidate journal is not enabled")
    if journal.get("economic_outcomes_read") is not False:
        raise ValueError("attempt7 cannot read economic outcomes")
    if journal.get("strategy_actions_changed") is not False:
        raise ValueError("journal cannot mutate strategy actions")

    if manifest.get("performance_limits") != PERFORMANCE_LIMITS:
        raise ValueError("attempt7 performance limits were relaxed or changed")
    if tuple(manifest.get("runtime_evidence_required", ())) != REQUIRED_RUNTIME_EVIDENCE:
        raise ValueError("attempt7 runtime evidence gates drifted")

    rollback = manifest.get("rollback_binding")
    if not isinstance(rollback, Mapping):
        raise ValueError("safe rollback binding is missing")
    if rollback.get("target_manifest_canonical_sha256") != SAFE_MANIFEST_CANONICAL_SHA256:
        raise ValueError("rollback does not target the safe baseline")
    if rollback.get("restore_runtime_file_sha256") != expected_safe_hashes:
        raise ValueError("rollback six-file restore hashes drifted")
    if rollback.get("startup_contract") != STARTUP_CONTRACT:
        raise ValueError("rollback startup contract is unsafe")
    if rollback.get("resulting_journal_enabled") is not False:
        raise ValueError("rollback must return to journal-OFF")
    if (
        rollback.get("stable_start_probe")
        != safe_manifest["rollback_binding"]["stable_start_probe"]
    ):
        raise ValueError("rollback stable-start probe drifted")

    permissions = manifest.get("permissions")
    if not isinstance(permissions, Mapping) or any(
        permissions.get(field) is not False
        for field in (
            "deployment_executed",
            "deployment_authorized",
            "rollback_authorized",
            "registry_modified",
            "current_pointer_modified",
            "economic_research_authorized",
            "strategy_action_authorized",
            "live_policy_authorized",
            "q90_action_authorized",
            "buy_fill_selection_action_authorized",
        )
    ):
        raise ValueError("attempt7 local-only or action permission boundary drifted")

    return {
        "schema_version": "prospective_lifecycle_from_safe_baseline_validation.v1",
        "release_id": RELEASE_ID,
        "manifest_sha256": claimed,
        "file_count": len(records),
        "safe_predecessor_bound": True,
        "safe_startup_contract_bound": True,
        "journal_enabled": True,
        "journal_bounded": True,
        "attempt6_semantics_preserved": True,
        "optimized_writer_sha256": OPTIMIZED_WRITER_SHA256,
        "old_attempt6_writer_reused": False,
        "performance_limits_unchanged": True,
        "economic_outcomes_read": False,
        "strategy_action_authorized": False,
        "live_policy_authorized": False,
        "deployment_authorized": False,
        "payload_valid": True,
    }


def build_release(
    *,
    repo_root: Path,
    output_root: Path,
    safe_baseline_root: Path = SAFE_BASELINE_ROOT_DEFAULT,
    attempt6_root: Path = ATTEMPT6_ROOT_DEFAULT,
    writer_source: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    repo = repo_root.expanduser().resolve(strict=True)
    expected_python = (repo / ".venv/bin/python").resolve(strict=True)
    if Path(sys.executable).resolve() != expected_python or sys.version_info < (3, 10):
        raise RuntimeError("builder must run with repository .venv Python >=3.10")
    safe_root = safe_baseline_root.expanduser().resolve(strict=True)
    attempt_root = attempt6_root.expanduser().resolve(strict=True)
    writer = _validate_writer_source(writer_source or repo / LIVE_WRITER_PATH)
    safe_manifest, safe_records = _validate_safe_baseline(safe_root)
    attempt6_manifest, attempt6_records = _validate_attempt6(attempt_root)

    output = output_root.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"attempt7 output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.parent / f".{output.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    temporary.mkdir()
    try:
        manifest = _build_payload(
            repo_root=repo,
            stage_root=temporary,
            safe_root=safe_root,
            attempt6_root=attempt_root,
            safe_manifest=safe_manifest,
            safe_records=safe_records,
            attempt6_manifest=attempt6_manifest,
            attempt6_records=attempt6_records,
            writer_source=writer,
        )
        validation = validate_staging(
            temporary,
            repo_root=repo,
            safe_baseline_root=safe_root,
            attempt6_root=attempt_root,
            writer_source=writer,
        )
        os.replace(temporary, output)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    final_validation = validate_staging(
        output,
        repo_root=repo,
        safe_baseline_root=safe_root,
        attempt6_root=attempt_root,
        writer_source=writer,
    )
    if final_validation != validation:
        raise AssertionError("attempt7 validation changed after atomic publication")
    return manifest, validation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    build.add_argument("--safe-baseline-root", type=Path, default=SAFE_BASELINE_ROOT_DEFAULT)
    build.add_argument("--attempt6-root", type=Path, default=ATTEMPT6_ROOT_DEFAULT)
    build.add_argument("--output-root", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--candidate-root", type=Path, required=True)
    validate.add_argument("--safe-baseline-root", type=Path, default=SAFE_BASELINE_ROOT_DEFAULT)
    validate.add_argument("--attempt6-root", type=Path, default=ATTEMPT6_ROOT_DEFAULT)
    args = parser.parse_args()
    if args.command == "build":
        manifest, validation = build_release(
            repo_root=args.repo_root,
            output_root=args.output_root,
            safe_baseline_root=args.safe_baseline_root,
            attempt6_root=args.attempt6_root,
        )
        print(
            json.dumps(
                {"manifest_sha256": manifest["manifest_sha256"], "validation": validation},
                sort_keys=True,
            )
        )
        return 0
    result = validate_staging(
        args.candidate_root,
        safe_baseline_root=args.safe_baseline_root,
        attempt6_root=args.attempt6_root,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
