#!/usr/bin/env python3
"""Fail-closed remote validation and release orchestration for journal-v2.

The default command is ``plan`` and performs no SSH call or mutation.  Every
remote or local mutation is isolated behind a stage-specific execution flag.
Production deploy/restart and rollback additionally require an exact owner
confirmation token derived from the frozen release and v9 identity hashes.

This module deliberately does not call ``make deploy`` and does not modify the
release payload.  It executes only the 19-file narrow overlay already frozen by
``build_prospective_lifecycle_narrow_release.py``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_prospective_lifecycle_narrow_release import (  # noqa: E402
    NEW_FILE_SHA256,
    build_release,
    validate_staging,
)
from scripts.live_remote_pointer import active_live_remote_fields  # noqa: E402

SCHEMA_VERSION = "prospective_lifecycle_remote_release_orchestrator.v1"
EVIDENCE_SCHEMA_VERSION = "prospective_lifecycle_remote_release_evidence.v1"
ADMISSION_SCHEMA_VERSION = "prospective_lifecycle_release_evidence_admission.v1"
DEPLOYMENT_BINDING_SCHEMA_VERSION = "prospective_lifecycle_deployment_binding.v1"

EPHEMERAL_ROOT = Path(
    os.environ.get("NARROWGATE_EPHEMERAL_ROOT", tempfile.gettempdir())
).expanduser()
DEFAULT_RELEASE_MANIFEST = Path(
    EPHEMERAL_ROOT
    / "narrowgate_prospective_lifecycle_narrow_release_v1_20260805_attempt6"
    / "release_manifest.json"
)
DEFAULT_REMOTE_IDENTITY = Path(
    "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260804_v9.json"
)
_ACTIVE_REMOTE = active_live_remote_fields(ROOT)
DEFAULT_REMOTE = _ACTIVE_REMOTE.get("ssh_target", "")
DEFAULT_REMOTE_ROOT = os.environ.get(
    "NARROWGATE_REMOTE_ROOT",
    _ACTIVE_REMOTE.get("repo_root", str(Path.home() / ROOT.name)),
)
DEFAULT_REMOTE_PYTHON = ".venv-active/bin/python3"
DEFAULT_ISOLATED_RELEASE_ROOT = str(
    PurePosixPath(DEFAULT_REMOTE_ROOT)
    / ".releases/prospective_lifecycle_journal_v2_20260805_attempt6"
)
KNOWN_ACTIVE_PYARROW_VERSION = "21.0.0"
REQUIRED_SUCCESSOR_PYARROW_VERSION = "24.0.0"
KNOWN_NATIVE_EXTENSION_PATH = (
    str(
        PurePosixPath(DEFAULT_REMOTE_ROOT)
        / ".venv-py312/lib/python3.12/site-packages"
        / "narrowgate_cpp.cpython-312-x86_64-linux-gnu.so"
    )
)
KNOWN_NATIVE_EXTENSION_SHA256 = "343d92127a80cb6fefe10cddd21e70f8c1bf22674c19874c7fb0971d052b45f0"
KNOWN_ACTIVE_PACKAGE_VERSIONS = {
    "binance-futures-connector": "4.2.0",
    "certifi": "2026.6.17",
    "charset-normalizer": "3.4.7",
    "idna": "3.18",
    "iniconfig": "2.3.0",
    "joblib": "1.5.3",
    "lightgbm": "4.6.0",
    "narrowgate": "0.1.0",
    "narrowgate-btcusdc-cpp": "0.0.0",
    "ninja": "1.13.0",
    "numpy": "2.0.2",
    "packaging": "26.2",
    "pandas": "2.3.3",
    "pathspec": "1.1.1",
    "pip": "26.0.1",
    "pluggy": "1.6.0",
    "pyarrow": "21.0.0",
    "pybind11": "3.0.4",
    "pycryptodome": "3.23.0",
    "pygments": "2.20.0",
    "pytest": "8.4.2",
    "python-dateutil": "2.9.0.post0",
    "pytz": "2026.2",
    "pyyaml": "6.0.3",
    "requests": "2.32.5",
    "scikit-build-core": "1.0.3",
    "scikit-learn": "1.6.1",
    "scipy": "1.13.1",
    "setuptools": "82.0.1",
    "six": "1.17.0",
    "threadpoolctl": "3.6.0",
    "typing-extensions": "4.16.0",
    "tzdata": "2026.2",
    "urllib3": "2.6.3",
    "websocket-client": "1.9.0",
    "wheel": "0.47.0",
    "zstandard": "0.25.0",
}
KNOWN_SUCCESSOR_PACKAGE_VERSIONS = {
    **KNOWN_ACTIVE_PACKAGE_VERSIONS,
    "pyarrow": REQUIRED_SUCCESSOR_PYARROW_VERSION,
}
STAGED_OVERLAY_PACKAGES = ("execution", "live", "models", "strategy")
PREDECESSOR_ROLLBACK_PYTHON = ".venv-py312/bin/python3"
KNOWN_ACTIVE_PYTHON_PREFIX = f"{DEFAULT_REMOTE_ROOT}/.venv-py312"

REQUIRED_PREDECESSOR_STARTUP_CONTRACT = "warmup_before_websocket.v1"
ROLLBACK_STABILITY_SCHEMA_VERSION = "prospective_lifecycle_rollback_startup_stability.v1"
ROLLBACK_CANONICAL_BUCKET_S = 10.0
ROLLBACK_MINIMUM_STABLE_BUCKETS = 2
DEFAULT_ROLLBACK_STABILITY_WINDOW_S = 25.0
ROLLBACK_STABILITY_LOG_PATH = "logs/maker.log"
ROLLBACK_FORBIDDEN_LOG_MARKERS = (
    "fatal",
    "completed 10s feature bucket lacks an exact causal 1s grid",
    "duplicate-grid",
    "duplicate_grid",
)

BASELINE_QUOTE_LOOP_TELEMETRY = {
    "schema_version": "narrowgate_live_requote_telemetry_window.v1",
    "baseline_identity": "operational_baseline_v9",
    "window_duration_s": 3600,
    "window_end_ts": 1785896266.426,
    "requote_rows": 601,
    "requote_total_us_p50": 47701.689,
    "requote_total_us_p95": 102336.292,
    "requote_total_us_p99": 141384.284,
    "requote_total_us_max": 170374.489,
    "signal_compute_us_p99": 47974.079,
    "compute_quotes_us_p99": 5961.135,
    "update_orders_us_p99": 104016.173,
}
BASELINE_PROCESS_RESOURCE = {
    "schema_version": "narrowgate_live_process_resource_baseline.v1",
    "pid": 1798225,
    "etimes_s": 98692,
    "process_cpu_pct_one_core": 14.6,
    "rss_kib": 334652,
    "vsz_kib": 992184,
}

INITIAL_STATE_DOMAINS = (
    "account_and_exchange",
    "inventory_accounting",
    "campaign",
    "reward_path_loss_cooldown",
    "adverse_markout_pause",
    "sync_degrade",
    "defense_and_stale_guards",
    "fill_cooldown_lineage",
    "order_lifecycle",
    "q90_runtime",
    "post_fill_response",
    "quote_policy_clocks",
    "signal_feature_dag_warmup",
)

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

REQUIRED_GATES = (
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

MUTATING_STAGES = frozenset({"stage-validate", "deploy-restart", "admit", "rollback-drill"})
PRODUCTION_MUTATING_STAGES = frozenset({"deploy-restart", "rollback-drill"})

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _seal_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    unsigned = dict(payload)
    unsigned.pop("receipt_identity_sha256", None)
    return {
        **unsigned,
        "receipt_identity_sha256": _canonical_sha256(unsigned),
    }


def _validate_receipt_identity(payload: Mapping[str, Any], label: str) -> None:
    claimed = payload.get("receipt_identity_sha256")
    unsigned = dict(payload)
    unsigned.pop("receipt_identity_sha256", None)
    if claimed != _canonical_sha256(unsigned):
        raise ValueError(f"{label} receipt identity SHA256 mismatch")


def _validate_stage_receipt(
    payload: Mapping[str, Any],
    *,
    stage: str,
    bound: Mapping[str, Any],
) -> None:
    if payload.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise ValueError(f"{stage} receipt schema version mismatch")
    if payload.get("stage") != stage:
        raise ValueError(f"expected {stage} receipt")
    _validate_receipt_identity(payload, stage)
    if payload.get("release_manifest_sha256") != bound["release_manifest_sha256"]:
        raise ValueError(f"{stage} receipt release hash mismatch")
    if payload.get("remote_identity_sha256") != bound["remote_identity_sha256"]:
        raise ValueError(f"{stage} receipt identity hash mismatch")


def _deployment_binding_payload(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": DEPLOYMENT_BINDING_SCHEMA_VERSION,
        "release_manifest_sha256": bound["release_manifest_sha256"],
        "remote_identity_sha256": bound["remote_identity_sha256"],
        "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
        "deployment_instance_id": plan.get("deployment_instance_id"),
        "remote": plan["remote"],
        "remote_root": plan["remote_root"],
        "isolated_stage_root": plan["isolated_stage_root"],
        "successor_venv": plan["isolated_successor_venv"],
        "backup_root": plan["backup_root"],
    }


def _deployment_binding_fields(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
) -> dict[str, Any]:
    binding = _deployment_binding_payload(plan=plan, bound=bound)
    return {
        "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
        "deployment_instance_id": plan.get("deployment_instance_id"),
        "deployment_binding": binding,
        "deployment_binding_sha256": _canonical_sha256(binding),
    }


def _validate_embedded_deployment_binding(
    payload: Mapping[str, Any],
    *,
    expected: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    raw = payload.get("deployment_binding")
    if not isinstance(raw, Mapping):
        raise ValueError("receipt lacks a deployment binding")
    binding = dict(raw)
    if binding.get("schema_version") != DEPLOYMENT_BINDING_SCHEMA_VERSION:
        raise ValueError("deployment binding schema version mismatch")
    claimed = _validate_hex_sha256(
        payload.get("deployment_binding_sha256"),
        "deployment binding SHA256",
    )
    if _canonical_sha256(binding) != claimed:
        raise ValueError("deployment binding SHA256 mismatch")
    if payload.get("mutation_plan_identity_sha256") != binding.get("mutation_plan_identity_sha256"):
        raise ValueError("receipt mutation plan differs from deployment binding")
    if payload.get("deployment_instance_id") != binding.get("deployment_instance_id"):
        raise ValueError("receipt deployment instance differs from deployment binding")
    if expected is not None and binding != dict(expected):
        raise ValueError("receipt deployment binding differs from the current plan")
    return binding


def normalize_runtime_receipt_for_plan(
    receipt: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind a runtime receipt to one plan without rewriting a legacy receipt."""

    _validate_stage_receipt(receipt, stage="runtime", bound=bound)
    expected = _deployment_binding_payload(plan=plan, bound=bound)
    if receipt.get("deployment_binding") is not None:
        _validate_embedded_deployment_binding(receipt, expected=expected)
        legacy_normalized = False
    else:
        evidence = receipt.get("evidence")
        if not isinstance(evidence, Mapping):
            raise ValueError("legacy runtime receipt evidence is invalid")
        deployment = evidence.get("deployment")
        if not isinstance(deployment, Mapping):
            raise ValueError("legacy runtime receipt lacks deployment evidence")
        expected_owner_hash = hashlib.sha256(
            owner_confirmation_token(
                "deploy-restart",
                bound,
                str(plan["mutation_plan_identity_sha256"]),
            ).encode()
        ).hexdigest()
        required_deployment = {
            "backup_root": plan["backup_root"],
            "successor_venv": plan["isolated_successor_venv"],
            "active_venv_target_after": plan["isolated_successor_venv"],
        }
        if any(deployment.get(key) != value for key, value in required_deployment.items()):
            raise ValueError("legacy runtime receipt belongs to another deployment plan")
        if int(deployment.get("deployed_file_count", -1)) != len(
            bound["release_manifest"]["files"]
        ):
            raise ValueError("legacy runtime receipt deployed-file count differs")
        if not bool(evidence.get("deployment_files_applied")):
            raise ValueError("legacy runtime receipt does not prove deployment")
        if bool(evidence.get("automatic_rollback_performed")):
            raise ValueError("legacy runtime receipt was automatically rolled back")
        if receipt.get("owner_confirmation_token_sha256") != expected_owner_hash:
            raise ValueError("legacy runtime receipt owner token differs from the current plan")
        runtime_gates = validate_runtime_probe(
            evidence,
            bound,
            expected_runtime_files=plan["stages"]["deploy-restart"][
                "expected_successor_runtime_files"
            ],
            expected_package_versions=KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
            require_frozen_native_path=False,
            expected_python_prefix=plan["isolated_successor_venv"],
        )
        if not all(runtime_gates.values()):
            raise ValueError(f"legacy runtime receipt successor identity failed: {runtime_gates}")
        _validate_hex_sha256(
            deployment.get("backup_manifest_canonical_sha256"),
            "legacy runtime backup manifest canonical SHA256",
        )
        _validate_hex_sha256(
            deployment.get("backup_manifest_file_sha256"),
            "legacy runtime backup manifest file SHA256",
        )
        legacy_normalized = True
    return {
        "schema_version": DEPLOYMENT_BINDING_SCHEMA_VERSION,
        "deployment_binding": expected,
        "deployment_binding_sha256": _canonical_sha256(expected),
        "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
        "deployment_instance_id": plan.get("deployment_instance_id"),
        "runtime_receipt_identity_sha256": receipt["receipt_identity_sha256"],
        "legacy_runtime_receipt_normalized_read_only": legacy_normalized,
    }


BASELINE_QUOTE_LOOP_TELEMETRY_SHA256 = _canonical_sha256(BASELINE_QUOTE_LOOP_TELEMETRY)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _validate_hex_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{label} must be a lowercase SHA256")
    return normalized


def _runtime_code_files(
    remote_identity: Mapping[str, Any],
) -> tuple[dict[str, str], str]:
    raw = remote_identity.get("runtime_code")
    if not isinstance(raw, Mapping):
        raise ValueError("remote runtime identity lacks runtime_code")
    runtime_identity_code = dict(raw)
    deployment_scope = runtime_identity_code.pop("deployment_scope", None)
    if not isinstance(deployment_scope, str) or not deployment_scope:
        raise ValueError("remote runtime identity lacks deployment_scope metadata")
    runtime_code: dict[str, str] = {}
    for logical, digest in runtime_identity_code.items():
        relative = Path(str(logical))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"invalid remote runtime-code path: {logical}")
        runtime_code[str(relative)] = _validate_hex_sha256(
            digest,
            f"remote runtime-code SHA256 for {logical}",
        )
    return runtime_code, deployment_scope


def _validate_remote_path(value: str, label: str) -> str:
    frozen = PurePosixPath(DEFAULT_REMOTE_ROOT)
    candidate = PurePosixPath(value)
    if not candidate.is_absolute() or (candidate != frozen and frozen not in candidate.parents):
        raise ValueError(f"{label} escaped the frozen remote repository root")
    if "\x00" in value or "\n" in value or "\r" in value or ".." in candidate.parts:
        raise ValueError(f"{label} is unsafe")
    return str(candidate)


def _validate_isolated_stage_root(remote_root: str, stage_root: str) -> str:
    root = PurePosixPath(remote_root)
    stage = PurePosixPath(_validate_remote_path(stage_root, "isolated_release_root"))
    releases = root / ".releases"
    if stage == root or stage == releases or releases not in stage.parents:
        raise ValueError("isolated_release_root must be a child of remote .releases")
    return str(stage)


def _validate_deployment_instance_id(value: str | None) -> str | None:
    if value is None:
        return None
    if not value or len(value) > 64:
        raise ValueError("deployment_instance_id must contain 1-64 characters")
    allowed = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(character not in allowed for character in value):
        raise ValueError("deployment_instance_id contains unsafe characters")
    return value


def _validate_rollback_stability_window_s(value: float) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("rollback stability window must be numeric") from exc
    minimum = ROLLBACK_CANONICAL_BUCKET_S * ROLLBACK_MINIMUM_STABLE_BUCKETS
    if not math.isfinite(normalized) or normalized < minimum:
        raise ValueError("rollback stability window must cover at least two canonical 10s buckets")
    return normalized


def _require_rollback_predecessor_startup_contract(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
) -> None:
    observed = bound["remote_identity"].get("startup_contract")
    frozen = plan["stages"]["rollback-drill"].get("startup_stability_contract")
    if not isinstance(frozen, Mapping):
        raise PermissionError(
            "rollback blocked before stop/mutation: startup stability contract is not frozen"
        )
    if (
        observed != REQUIRED_PREDECESSOR_STARTUP_CONTRACT
        or frozen.get("required_predecessor_startup_contract")
        != REQUIRED_PREDECESSOR_STARTUP_CONTRACT
        or frozen.get("observed_predecessor_startup_contract") != observed
        or frozen.get("predecessor_startup_contract_bound") is not True
    ):
        raise PermissionError(
            "rollback blocked before stop/mutation: predecessor identity must explicitly "
            "bind startup_contract=warmup_before_websocket.v1"
        )


def _manifest_without_hash(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in manifest.items() if key != "manifest_sha256"}


def load_bound_release(
    release_manifest_path: Path,
    remote_identity_path: Path,
) -> dict[str, Any]:
    """Validate the local release and exact remote-v9 identity binding."""

    release_manifest_path = release_manifest_path.expanduser().resolve(strict=True)
    remote_identity_path = remote_identity_path.expanduser().resolve(strict=True)
    release_root = release_manifest_path.parent
    staging_validation = validate_staging(release_root)
    release = _read_object(release_manifest_path, "release manifest")
    identity = _read_object(remote_identity_path, "remote v9 identity")

    claimed_release_hash = _validate_hex_sha256(
        release.get("manifest_sha256"), "release manifest_sha256"
    )
    if _canonical_sha256(_manifest_without_hash(release)) != claimed_release_hash:
        raise ValueError("release manifest canonical hash mismatch")
    identity_hash = _sha256(remote_identity_path)
    expected_identity_hash = _validate_hex_sha256(
        release.get("baseline_identity_sha256"), "release baseline_identity_sha256"
    )
    if identity_hash != expected_identity_hash:
        raise ValueError("remote v9 identity hash does not match release")
    if identity.get("schema_version") != "narrowgate_operational_baseline_identity.v9":
        raise ValueError("remote identity is not the frozen v9 schema")

    config = identity.get("config")
    permissions = identity.get("permissions")
    if not isinstance(config, dict) or not isinstance(permissions, dict):
        raise ValueError("remote v9 identity lacks config or permissions")
    frozen_flags = {
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }
    for field, expected in frozen_flags.items():
        if config.get(field) is not expected:
            raise ValueError(f"remote v9 identity violates frozen flag {field}")
    if permissions.get("q90_action_live_authorized") is not False:
        raise ValueError("remote v9 identity authorizes q90 action")
    if permissions.get("buy_fill_selection_action_authorized") is not False:
        raise ValueError("remote v9 identity authorizes BUY fill selection")
    if release.get("deployment_authorized") is not False:
        raise ValueError("source release manifest must remain deployment-unauthorized")
    if release.get("deployment_executed") is not False:
        raise ValueError("source release manifest must remain undeployed")
    if release.get("runtime_evidence_required") != list(REQUIRED_GATES):
        raise ValueError("release runtime gate list drifted from orchestrator contract")

    source_payload_drift: dict[str, dict[str, str]] = {}
    records = {str(row["path"]): row for row in release.get("files", [])}
    repo_root = remote_identity_path.parents[4]
    for logical in NEW_FILE_SHA256:
        source = repo_root / logical
        record = records.get(logical)
        release_hash = str(record.get("sha256", "")) if record else ""
        current_hash = _sha256(source) if source.is_file() else "missing"
        if record is None or current_hash != release_hash:
            source_payload_drift[logical] = {
                "release_sha256": release_hash,
                "current_source_sha256": current_hash,
            }

    return {
        "release_root": release_root,
        "release_manifest_path": release_manifest_path,
        "release_manifest": release,
        "release_manifest_sha256": claimed_release_hash,
        "remote_identity_path": remote_identity_path,
        "remote_identity": identity,
        "remote_identity_sha256": identity_hash,
        "staging_validation": staging_validation,
        "frozen_strategy_flags": frozen_flags,
        "source_payload_drift": source_payload_drift,
        "source_payload_current": not source_payload_drift,
    }


def owner_confirmation_token(
    stage: str,
    bound: Mapping[str, Any],
    mutation_plan_identity_sha256: str,
) -> str:
    if stage not in PRODUCTION_MUTATING_STAGES:
        raise ValueError(f"owner token is not defined for stage {stage}")
    digest = _canonical_sha256(
        {
            "stage": stage,
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            "remote_baseline_id": bound["remote_identity"]["baseline_id"],
            "mutation_plan_identity_sha256": _validate_hex_sha256(
                mutation_plan_identity_sha256,
                "mutation plan identity SHA256",
            ),
        }
    )
    return f"OWNER_CONFIRMED_{stage.upper().replace('-', '_')}:{digest}"


def _remote_python_command(remote_python: str, source: str, *arguments: str) -> str:
    return " ".join(
        [
            shlex.quote(remote_python),
            "-I",
            "-c",
            shlex.quote(source),
            *(shlex.quote(argument) for argument in arguments),
        ]
    )


def _remote_env_python_command(
    *,
    remote_root: str,
    remote_python: str,
    source: str,
    arguments: Sequence[str],
) -> str:
    inner = " ".join(
        [
            "set -a;",
            f". {shlex.quote(remote_root + '/live/.env')};",
            "set +a;",
            "exec env -u PYTHONPATH",
            shlex.quote(remote_python),
            "-I",
            "-c",
            shlex.quote(source),
            *(shlex.quote(argument) for argument in arguments),
        ]
    )
    return f"bash -lc {shlex.quote(inner)}"


def _ssh_command(remote: str, remote_root: str, command: str) -> list[str]:
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        remote,
        f"cd {shlex.quote(remote_root)} && {command}",
    ]


def _runtime_probe_source() -> str:
    return r"""
import collections, hashlib, importlib.metadata, importlib.util, json, os, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
expected = json.loads(sys.argv[2])
pid_expected = int(sys.argv[3])
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
pids = []
for item in pathlib.Path("/proc").iterdir():
    if not item.name.isdigit():
        continue
    if int(item.name) == os.getpid():
        continue
    try:
        cmdline = (item / "cmdline").read_bytes().replace(b"\0", b" ").decode()
    except (OSError, UnicodeDecodeError):
        continue
    if "live/main.py" in cmdline and "--config" in cmdline:
        pids.append((int(item.name), cmdline))
if len(pids) != 1:
    raise SystemExit(f"expected exactly one maker PID, got {len(pids)}")
pid, cmdline = pids[0]
if pid_expected > 0 and pid != pid_expected:
    raise SystemExit(f"maker PID drift: expected {pid_expected}, got {pid}")
files = {}
for logical, expected_hash in expected.items():
    path = (root / logical).resolve()
    if root not in path.parents or not path.is_file():
        raise SystemExit(f"missing runtime file {logical}")
    actual = sha(path)
    if actual != expected_hash:
        raise SystemExit(f"runtime hash mismatch {logical}: {actual}")
    files[logical] = actual
maps = (pathlib.Path("/proc") / str(pid) / "maps").read_text(errors="replace")
native_paths = sorted({line.split()[-1] for line in maps.splitlines()
                       if line.split() and line.split()[-1].endswith(".so")
                       and pathlib.Path(line.split()[-1]).name.startswith("narrowgate_cpp")})
if not native_paths:
    raise SystemExit("loaded narrowgate native .so not found")
native = []
for raw in native_paths:
    path = pathlib.Path(raw).resolve()
    if not path.is_file():
        raise SystemExit(f"loaded native .so missing: {path}")
    native.append({"path": str(path), "sha256": sha(path)})
import pyarrow
distribution_rows = [
    (
        distribution.metadata["Name"].lower().replace("_", "-"),
        distribution.version,
        str(pathlib.Path(distribution._path).resolve()),
    )
    for distribution in importlib.metadata.distributions()
    if distribution.metadata.get("Name")
]
distribution_identities = collections.defaultdict(set)
for name, version, path in distribution_rows:
    distribution_identities[name].add((version, path))
conflicting_distributions = sorted(
    name for name, identities in distribution_identities.items() if len(identities) != 1
)
if conflicting_distributions:
    raise SystemExit(f"conflicting runtime distributions: {conflicting_distributions}")
packages = {
    name: next(iter(identities))[0]
    for name, identities in distribution_identities.items()
}
print(json.dumps({
    "schema_version": "prospective_lifecycle_remote_runtime_probe.v1",
    "python_version": ".".join(map(str, sys.version_info[:3])),
    "python_executable": str(pathlib.Path(sys.executable).resolve()),
    "python_prefix": str(pathlib.Path(sys.prefix).resolve()),
    "pyarrow_module_path": str(pathlib.Path(pyarrow.__file__).resolve()),
    "pyarrow_version": pyarrow.__version__,
    "maker_pid": pid,
    "maker_cmdline": cmdline,
    "runtime_files": files,
    "loaded_native_extensions": native,
    "package_versions": packages,
}, sort_keys=True))
""".strip()


def _rollback_startup_stability_probe_source() -> str:
    return r"""
import json, os, pathlib, sys, time
root = pathlib.Path(sys.argv[1]).resolve()
expected_pid = int(sys.argv[2])
stability_window_s = float(sys.argv[3])
canonical_bucket_s = float(sys.argv[4])
minimum_stable_buckets = int(sys.argv[5])
log_relative = pathlib.PurePosixPath(sys.argv[6])
forbidden_markers = json.loads(sys.argv[7])
if expected_pid <= 0:
    raise SystemExit("expected predecessor PID must be positive")
if (
    log_relative.is_absolute()
    or ".." in log_relative.parts
    or not isinstance(forbidden_markers, list)
    or not forbidden_markers
):
    raise SystemExit("rollback stability probe inputs are invalid")
log_path = (root / pathlib.Path(*log_relative.parts)).resolve()
if root not in log_path.parents:
    raise SystemExit("rollback stability log escaped repository root")
def maker_processes():
    rows = []
    for item in pathlib.Path("/proc").iterdir():
        if not item.name.isdigit() or int(item.name) == os.getpid():
            continue
        try:
            cmdline = (item / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except (OSError, UnicodeDecodeError):
            continue
        if "live/main.py" in cmdline and "--config" in cmdline:
            rows.append({"pid": int(item.name), "cmdline": cmdline})
    return sorted(rows, key=lambda row: row["pid"])
start_processes = maker_processes()
start_log_exists = log_path.is_file()
start_log_inode = None
start_log_offset = None
if start_log_exists:
    start_stat = log_path.stat()
    start_log_inode = int(start_stat.st_ino)
    start_log_offset = int(start_stat.st_size)
started = time.monotonic()
time.sleep(stability_window_s)
observed_duration_s = time.monotonic() - started
end_processes = maker_processes()
end_log_exists = log_path.is_file()
end_log_inode = None
end_log_offset = None
log_identity_stable = False
appended_log = ""
if end_log_exists:
    end_stat = log_path.stat()
    end_log_inode = int(end_stat.st_ino)
    end_log_offset = int(end_stat.st_size)
    log_identity_stable = bool(
        start_log_exists
        and end_log_inode == start_log_inode
        and start_log_offset is not None
        and end_log_offset >= start_log_offset
    )
    if log_identity_stable:
        with log_path.open("rb") as handle:
            handle.seek(start_log_offset)
            appended_log = handle.read().decode(errors="replace")
normalized_log = appended_log.lower()
forbidden_log_hits = sorted(
    marker for marker in forbidden_markers
    if str(marker).lower() in normalized_log
)
same_maker_process = bool(
    len(start_processes) == 1
    and len(end_processes) == 1
    and start_processes[0]["pid"] == expected_pid
    and end_processes[0]["pid"] == expected_pid
    and start_processes[0]["cmdline"] == end_processes[0]["cmdline"]
)
print(json.dumps({
    "schema_version": "prospective_lifecycle_rollback_startup_stability.v1",
    "expected_maker_pid": expected_pid,
    "start_maker_processes": start_processes,
    "end_maker_processes": end_processes,
    "same_maker_process": same_maker_process,
    "stability_window_s": stability_window_s,
    "observed_duration_s": observed_duration_s,
    "canonical_bucket_s": canonical_bucket_s,
    "minimum_stable_buckets": minimum_stable_buckets,
    "covered_canonical_buckets": observed_duration_s / canonical_bucket_s,
    "log_path": str(log_path),
    "log_exists_at_start": start_log_exists,
    "log_exists_at_end": end_log_exists,
    "start_log_inode": start_log_inode,
    "end_log_inode": end_log_inode,
    "start_log_offset": start_log_offset,
    "end_log_offset": end_log_offset,
    "log_identity_stable": log_identity_stable,
    "forbidden_log_hits": forbidden_log_hits,
    "fatal_or_duplicate_grid_absent": not forbidden_log_hits,
}, sort_keys=True))
""".strip()


def _quiescence_probe_source() -> str:
    return r"""
import json, os, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root))
pids = []
for item in pathlib.Path("/proc").iterdir():
    if not item.name.isdigit() or int(item.name) == os.getpid():
        continue
    try:
        cmdline = (item / "cmdline").read_bytes().replace(b"\0", b" ").decode()
    except (OSError, UnicodeDecodeError):
        continue
    if "live/main.py" in cmdline and "--config" in cmdline:
        pids.append({"pid": int(item.name), "cmdline": cmdline})
if pids:
    raise SystemExit(f"maker process still active after controlled stop: {pids}")
from live.config import load_config
from live.main import create_rest_client
cfg = load_config(root / "live/config.yaml")
rest = create_rest_client(cfg, dry_run=False)
orders = rest.get_orders(symbol=cfg.symbol)
if not isinstance(orders, list):
    raise SystemExit("exchange open-order audit returned a non-list")
if orders:
    identities = [
        {
            "client_order_id": str(row.get("clientOrderId", "")),
            "order_id": str(row.get("orderId", "")),
            "side": str(row.get("side", "")),
            "status": str(row.get("status", "")),
        }
        for row in orders
    ]
    raise SystemExit(f"exchange orders remain after controlled stop: {identities}")
print(json.dumps({
    "controlled_stop_quiescent": True,
    "maker_pid_count": 0,
    "exchange_open_order_count": 0,
    "symbol": cfg.symbol,
}, sort_keys=True))
""".strip()


def _predecessor_revalidation_source() -> str:
    return r"""
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
predecessors = json.loads(sys.argv[2])
new_files = json.loads(sys.argv[3])
stage = pathlib.Path(sys.argv[4]).resolve()
active = root / ".venv-active"
if not active.is_symlink():
    raise SystemExit("active venv must be a symlink before isolated staging")
active_target = active.resolve(strict=True)
if (
    stage == root
    or root not in stage.parents
    or stage == active_target
    or stage in active_target.parents
    or active_target in stage.parents
):
    raise SystemExit("isolated stage overlaps the repository or active venv")
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
for logical, expected in predecessors.items():
    path = (root / logical).resolve()
    if root not in path.parents or not path.is_file() or sha(path) != expected:
        raise SystemExit(f"predecessor revalidation failed: {logical}")
for logical in new_files:
    path = (root / logical).resolve()
    if path.exists():
        raise SystemExit(f"new-file absence revalidation failed: {logical}")
print(json.dumps({"predecessor_revalidation_passed": True}, sort_keys=True))
""".strip()


def _clone_active_venv_source() -> str:
    return r"""
import json, os, pathlib, shutil, sys, uuid
root = pathlib.Path(sys.argv[1]).resolve()
stage = pathlib.Path(sys.argv[2]).resolve()
destination = pathlib.Path(sys.argv[3]).resolve()
active = root / ".venv-active"
if not active.is_symlink():
    raise SystemExit("active venv must be a symlink")
source = active.resolve()
if root not in source.parents or not source.is_dir():
    raise SystemExit("active venv target escaped the repository")
if stage not in destination.parents or destination.parent != stage:
    raise SystemExit("successor venv destination escaped the isolated stage")
temporary = stage / f".venv-successor.partial-{os.getpid()}-{uuid.uuid4().hex}"
try:
    shutil.copytree(source, temporary, symlinks=True)
    if destination.exists():
        shutil.rmtree(destination)
    os.replace(temporary, destination)
finally:
    if temporary.exists():
        shutil.rmtree(temporary, ignore_errors=True)
print(json.dumps({
    "active_venv_cloned": True,
    "active_venv_source": str(source),
    "successor_venv": str(destination),
}, sort_keys=True))
""".strip()


def _successor_runtime_validation_source() -> str:
    return r"""
import collections, hashlib, importlib.metadata, json, pathlib, sys, sysconfig
import narrowgate_cpp, pyarrow
expected_packages = json.loads(sys.argv[1])
expected_native_sha = sys.argv[2]
distribution_rows = [
    (
        distribution.metadata["Name"].lower().replace("_", "-"),
        distribution.version,
        str(pathlib.Path(distribution._path).resolve()),
    )
    for distribution in importlib.metadata.distributions()
    if distribution.metadata.get("Name")
]
distribution_identities = collections.defaultdict(set)
for name, version, path in distribution_rows:
    distribution_identities[name].add((version, path))
conflicting_distributions = sorted(
    name for name, identities in distribution_identities.items() if len(identities) != 1
)
if conflicting_distributions:
    raise SystemExit(f"conflicting successor distributions: {conflicting_distributions}")
packages = {
    name: next(iter(identities))[0]
    for name, identities in distribution_identities.items()
}
if packages != expected_packages:
    missing = sorted(set(expected_packages) - set(packages))
    extra = sorted(set(packages) - set(expected_packages))
    changed = {
        name: {"expected": expected_packages[name], "actual": packages[name]}
        for name in sorted(set(expected_packages) & set(packages))
        if expected_packages[name] != packages[name]
    }
    raise SystemExit(
        f"successor package identity mismatch missing={missing} extra={extra} changed={changed}"
    )
site_packages = pathlib.Path(sysconfig.get_paths()["purelib"]).resolve()
native = sorted(site_packages.glob("narrowgate_cpp*.so"))
if len(native) != 1:
    raise SystemExit(f"expected one successor native extension, got {len(native)}")
digest = hashlib.sha256(native[0].read_bytes()).hexdigest()
if digest != expected_native_sha:
    raise SystemExit(f"successor native extension SHA256 mismatch: {digest}")
if pathlib.Path(narrowgate_cpp.__file__).resolve() != native[0]:
    raise SystemExit("imported narrowgate_cpp does not match the bound native extension")
pyarrow_path = pathlib.Path(pyarrow.__file__).resolve()
if site_packages not in pyarrow_path.parents or pyarrow.__version__ != packages["pyarrow"]:
    raise SystemExit("imported pyarrow does not match the successor distribution")
prefix = pathlib.Path(sys.prefix).resolve()
if prefix.name != ".venv-successor":
    raise SystemExit(f"successor sys.prefix is not the isolated venv: {prefix}")
pyarrow_distribution = importlib.metadata.distribution("pyarrow")
record_path = pathlib.Path(pyarrow_distribution._path) / "RECORD"
if not record_path.is_file():
    raise SystemExit("successor pyarrow RECORD is missing")
print(json.dumps({
    "imported_narrowgate_cpp_path": str(pathlib.Path(narrowgate_cpp.__file__).resolve()),
    "imported_pyarrow_path": str(pyarrow_path),
    "pyarrow_record_sha256": hashlib.sha256(record_path.read_bytes()).hexdigest(),
    "successor_package_set_matches_active_except_pyarrow": True,
    "successor_native_extension_path": str(native[0]),
    "successor_native_extension_sha256": digest,
    "successor_python_prefix": str(prefix),
    "successor_python_version": ".".join(map(str, sys.version_info[:3])),
    "successor_pyarrow_version": packages["pyarrow"],
}, sort_keys=True))
""".strip()


def _staged_validation_source() -> str:
    return r"""
import ast, dataclasses, hashlib, importlib, importlib.util, json, pathlib, sys, yaml
sys.dont_write_bytecode = True
stage = pathlib.Path(sys.argv[1]).resolve()
records = json.loads(sys.argv[2])
expected_canonical_manifest_sha = sys.argv[3]
expected_manifest_file_sha = sys.argv[4]
remote_root = pathlib.Path(sys.argv[7]).resolve()
manifest_path = stage / "release_manifest.json"
manifest_file_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
if manifest_file_sha != expected_manifest_file_sha:
    raise SystemExit("staged release manifest file SHA256 mismatch")
manifest = json.loads(manifest_path.read_text())
claimed = manifest.pop("manifest_sha256", None)
canonical = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()).hexdigest()
if claimed != expected_canonical_manifest_sha or canonical != expected_canonical_manifest_sha:
    raise SystemExit("staged release canonical manifest SHA256 mismatch")
for row in records:
    path = (stage / row["path"]).resolve()
    if stage not in path.parents or not path.is_file():
        raise SystemExit(f"staged file missing: {row['path']}")
    h = hashlib.sha256(path.read_bytes()).hexdigest()
    if h != row["sha256"]:
        raise SystemExit(f"staged hash mismatch: {row['path']}")
    if path.suffix == ".py":
        ast.parse(path.read_text(), filename=str(path))
project_path_index = next(
    (
        index
        for index, path in enumerate(sys.path)
        if "site-packages" in path or "dist-packages" in path
    ),
    len(sys.path),
)
sys.path[project_path_index:project_path_index] = [str(stage), str(remote_root)]
def load_composite_package(name):
    package_paths = [str(stage / name), str(remote_root / name)]
    init_path = remote_root / name / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        name,
        init_path,
        submodule_search_locations=package_paths,
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load composite package {name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
for package_name in ("execution", "live", "models", "strategy"):
    load_composite_package(package_name)
modules = {
    "execution.order_lifecycle": "execution/order_lifecycle.py",
    "execution.order_lifecycle_journal_v2": "execution/order_lifecycle_journal_v2.py",
    "execution.order_lifecycle_live_writer_v2": "execution/order_lifecycle_live_writer_v2.py",
    "execution.prospective_lifecycle_state_capture_v1": "execution/prospective_lifecycle_state_capture_v1.py",
    "models.replay.baseline_epoch_manifest": "models/replay/baseline_epoch_manifest.py",
    "models.replay.prospective_baseline_epoch": "models/replay/prospective_baseline_epoch.py",
    "strategy.maker_engine": "strategy/maker_engine.py",
    "strategy.order_manager": "strategy/order_manager.py",
    "live.config": "live/config.py",
    "live.main": "live/main.py",
    "live.ws_handler": "live/ws_handler.py",
}
loaded_paths = {}
for name, logical in modules.items():
    module = importlib.import_module(name)
    actual = pathlib.Path(module.__file__).resolve()
    expected = (stage / logical).resolve()
    if actual != expected:
        raise SystemExit(f"staged overlay import escaped for {name}: {actual}")
    loaded_paths[name] = str(actual)
config_module = importlib.import_module("live.config")
if not dataclasses.is_dataclass(config_module.LifecycleJournalV2Config):
    raise SystemExit("staged LifecycleJournalV2Config is not a dataclass")
raw_config = yaml.safe_load((stage / "live/config.yaml").read_text())
parsed_config = config_module._parse(raw_config)
if not isinstance(parsed_config.lifecycle_journal_v2, config_module.LifecycleJournalV2Config):
    raise SystemExit("staged lifecycle_journal_v2 did not parse as its dataclass")
if not parsed_config.lifecycle_journal_v2.enabled:
    raise SystemExit("staged lifecycle_journal_v2 must be enabled")
parsed_config.lifecycle_journal_v2.enabled = False
config_module._validate_config(parsed_config)
print(json.dumps({
    "staged_overlay_import_smoke_passed": True,
    "targeted_lifecycle_tests_passed_on_remote_venv": False,
    "validated_file_count": len(records),
    "remote_manifest_file_sha256": manifest_file_sha,
    "remote_canonical_manifest_sha256": canonical,
    "staged_overlay_module_paths": loaded_paths,
}, sort_keys=True))
""".strip()


def _atomic_deploy_source() -> str:
    return r"""
import hashlib, json, os, pathlib, shutil, subprocess, sys, uuid, yaml
root = pathlib.Path(sys.argv[1]).resolve()
stage = pathlib.Path(sys.argv[2]).resolve()
backup = pathlib.Path(sys.argv[3]).resolve()
successor_venv = pathlib.Path(sys.argv[4]).resolve()
records = json.loads(sys.argv[5])
predecessors = json.loads(sys.argv[6])
new_files = set(json.loads(sys.argv[7]))
release_hash = sys.argv[8]
manifest_file_hash = sys.argv[9]
expected_successor_packages = json.loads(sys.argv[10])
expected_native_sha = sys.argv[11]
expected_predecessor_venv = pathlib.Path(sys.argv[12]).resolve()
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
def canonical_sha(payload):
    return hashlib.sha256(json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()).hexdigest()
def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    shutil.copy2(source, temporary)
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, destination)
    fsync_directory(destination.parent)
def restore(manifest):
    for row in reversed(manifest["files"]):
        destination = root / row["path"]
        if row["existed_before"]:
            atomic_copy(backup / "files" / row["path"], destination)
        else:
            destination.unlink(missing_ok=True)
            fsync_directory(destination.parent)
    active = root / ".venv-active"
    temporary = root / f".venv-active.rollback-{uuid.uuid4().hex}"
    os.symlink(manifest["active_venv_target_before"], temporary)
    os.replace(temporary, active)
    fsync_directory(root)
    for row in reversed(manifest.get("directories", [])):
        if row["existed_before"]:
            continue
        directory = root / row["path"]
        try:
            directory.rmdir()
        except OSError:
            pass
for logical, expected in predecessors.items():
    path = (root / logical).resolve()
    if root not in path.parents or not path.is_file() or sha(path) != expected:
        raise SystemExit(f"predecessor revalidation failed: {logical}")
for logical in new_files:
    if (root / logical).exists():
        raise SystemExit(f"new-file absence revalidation failed: {logical}")
for row in records:
    path = (stage / row["path"]).resolve()
    if stage not in path.parents or not path.is_file() or sha(path) != row["sha256"]:
        raise SystemExit(f"staged payload revalidation failed: {row['path']}")
staged_manifest_path = stage / "release_manifest.json"
if sha(staged_manifest_path) != manifest_file_hash:
    raise SystemExit("staged manifest file hash revalidation failed")
staged_manifest = json.loads(staged_manifest_path.read_text())
claimed_manifest_hash = staged_manifest.pop("manifest_sha256", None)
canonical_manifest_hash = hashlib.sha256(json.dumps(staged_manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()).hexdigest()
if claimed_manifest_hash != release_hash or canonical_manifest_hash != release_hash:
    raise SystemExit("staged canonical manifest revalidation failed")
candidate_config = yaml.safe_load((stage / "live/config.yaml").read_text())
current_config = yaml.safe_load((root / "live/config.yaml").read_text())
lifecycle = candidate_config.pop("lifecycle_journal_v2", None)
if not isinstance(lifecycle, dict) or candidate_config != current_config:
    raise SystemExit("candidate config changed existing v9 strategy semantics")
allowlisted_roots = lifecycle.get("remote_spool_allowlisted_roots")
expected_spool_root = (root / "formal_collection").resolve()
if not isinstance(allowlisted_roots, list) or len(allowlisted_roots) != 1:
    raise SystemExit("candidate remote spool allowlist must contain exactly one root")
configured_spool_root = pathlib.Path(str(allowlisted_roots[0])).resolve()
if configured_spool_root != expected_spool_root:
    raise SystemExit("candidate remote spool allowlist root changed")
if configured_spool_root.exists() and (
    not configured_spool_root.is_dir() or configured_spool_root.is_symlink()
):
    raise SystemExit("candidate remote spool allowlist root is unsafe")
strategy = candidate_config.get("strategy", {})
if strategy.get("dynamic_fill_hazard_action_enabled") is not False:
    raise SystemExit("candidate enabled q90 action")
if strategy.get("buy_fill_selection_shadow_enabled") is not False:
    raise SystemExit("candidate enabled BUY selector shadow")
if strategy.get("buy_fill_selection_live_enabled") is not False:
    raise SystemExit("candidate enabled BUY selector action")
successor_python = successor_venv / "bin/python"
if not successor_python.is_file():
    raise SystemExit("isolated successor Python is missing")
probe = subprocess.run(
    [
        str(successor_python),
        "-I",
        "-c",
        "import hashlib,importlib.metadata,json,pathlib,sys,sysconfig;"
        "packages={d.metadata['Name'].lower().replace('_','-'):d.version "
        "for d in importlib.metadata.distributions() if d.metadata.get('Name')};"
        "native=sorted(pathlib.Path(sysconfig.get_paths()['purelib']).glob('narrowgate_cpp*.so'));"
        "print(json.dumps({'python':'.'.join(map(str,sys.version_info[:3])),"
        "'packages':packages,'native_count':len(native),"
        "'native_sha256':hashlib.sha256(native[0].read_bytes()).hexdigest() "
        "if len(native)==1 else None},sort_keys=True))",
    ],
    check=True, capture_output=True, text=True,
)
versions = json.loads(probe.stdout)
if versions.get("python") != "3.12.13":
    raise SystemExit(f"successor runtime mismatch: {versions}")
if versions.get("packages") != expected_successor_packages:
    raise SystemExit("successor package identity changed after staging")
if versions.get("native_count") != 1 or versions.get("native_sha256") != expected_native_sha:
    raise SystemExit("successor native extension identity changed after staging")
active = root / ".venv-active"
if not active.is_symlink():
    raise SystemExit(".venv-active must be a symlink before controlled switch")
if active.resolve(strict=True) != expected_predecessor_venv:
    raise SystemExit("active predecessor venv identity changed before deploy")
active_target_before = os.readlink(active)
backup.mkdir(parents=True, exist_ok=False)
manifest = {
    "schema_version": "prospective_lifecycle_atomic_deploy_backup.v1",
    "release_manifest_sha256": release_hash,
    "active_venv_target_before": active_target_before,
    "files": [],
    "directories": [
        {
            "path": configured_spool_root.relative_to(root).as_posix(),
            "existed_before": configured_spool_root.exists(),
        }
    ],
}
try:
    for row in records:
        destination = root / row["path"]
        existed = destination.is_file()
        if existed:
            target = backup / "files" / row["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(destination, target)
            with target.open("rb") as handle:
                os.fsync(handle.fileno())
            fsync_directory(target.parent)
            predecessor_sha = sha(target)
        else:
            predecessor_sha = None
        manifest["files"].append({
            "path": row["path"],
            "existed_before": existed,
            "predecessor_sha256": predecessor_sha,
            "successor_sha256": row["sha256"],
        })
    manifest["manifest_sha256"] = canonical_sha(manifest)
    manifest_path = backup / "backup_manifest.json"
    manifest_temporary = backup / f".backup_manifest.partial-{uuid.uuid4().hex}"
    manifest_temporary.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    with manifest_temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(manifest_temporary, manifest_path)
    fsync_directory(backup)
    configured_spool_root.mkdir(parents=False, exist_ok=True)
    if not configured_spool_root.is_dir() or configured_spool_root.is_symlink():
        raise RuntimeError("remote spool allowlist root provisioning failed")
    fsync_directory(root)
    for row in records:
        atomic_copy(stage / row["path"], root / row["path"])
    temporary_link = root / f".venv-active.successor-{uuid.uuid4().hex}"
    os.symlink(str(successor_venv), temporary_link)
    os.replace(temporary_link, active)
    fsync_directory(root)
except Exception:
    restore(manifest)
    raise
print(json.dumps({
    "deployment_files_applied": True,
    "deployed_file_count": len(records),
    "backup_root": str(backup),
    "successor_venv": str(successor_venv),
    "active_venv_target_before": active_target_before,
    "active_venv_target_after": str(successor_venv),
    "backup_manifest_file_sha256": sha(backup / "backup_manifest.json"),
    "backup_manifest_canonical_sha256": manifest["manifest_sha256"],
    "spool_allowlist_root": str(configured_spool_root),
    "spool_allowlist_root_provisioned": True,
    "predecessor_revalidation_passed": True,
    "strategy_parameters_changed": False,
    "q90_action_enabled": False,
    "buy_fill_selection_enabled": False,
}, sort_keys=True))
""".strip()


def _atomic_rollback_source() -> str:
    return r"""
import hashlib, json, os, pathlib, shutil, sys, uuid
root = pathlib.Path(sys.argv[1]).resolve()
backup = pathlib.Path(sys.argv[2]).resolve()
candidate_records = json.loads(sys.argv[3])
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
def canonical_sha(payload):
    return hashlib.sha256(json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()).hexdigest()
def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    shutil.copy2(source, temporary)
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, destination)
    fsync_directory(destination.parent)
manifest_path = backup / "backup_manifest.json"
if not manifest_path.is_file():
    print(json.dumps({
        "rollback_files_restored": False,
        "rollback_not_required": True,
        "backup_manifest_missing": True,
    }, sort_keys=True))
    raise SystemExit(0)
manifest = json.loads(manifest_path.read_text())
claimed_manifest_sha = manifest.pop("manifest_sha256", None)
actual_manifest_sha = canonical_sha(manifest)
manifest["manifest_sha256"] = claimed_manifest_sha
if claimed_manifest_sha != actual_manifest_sha:
    raise SystemExit("rollback backup manifest identity failed")
candidate_by_path = {row["path"]: row["sha256"] for row in candidate_records}
if set(candidate_by_path) != {row["path"] for row in manifest["files"]}:
    raise SystemExit("rollback candidate file set mismatch")
for row in manifest["files"]:
    path = root / row["path"]
    actual = sha(path) if path.is_file() else None
    allowed = {candidate_by_path[row["path"]], row.get("predecessor_sha256")}
    if actual not in allowed:
        raise SystemExit(f"deployed successor/predecessor revalidation failed: {row['path']}")
    if row["existed_before"]:
        source = backup / "files" / row["path"]
        if not source.is_file() or sha(source) != row.get("predecessor_sha256"):
            raise SystemExit(f"rollback backup hash mismatch: {row['path']}")
for row in reversed(manifest["files"]):
    destination = root / row["path"]
    if row["existed_before"]:
        source = backup / "files" / row["path"]
        atomic_copy(source, destination)
    else:
        destination.unlink(missing_ok=True)
        fsync_directory(destination.parent)
removed_directories = []
retained_directories = []
for row in reversed(manifest.get("directories", [])):
    if row["existed_before"]:
        continue
    directory = root / row["path"]
    try:
        directory.rmdir()
        removed_directories.append(row["path"])
    except OSError:
        retained_directories.append(row["path"])
active = root / ".venv-active"
temporary_link = root / f".venv-active.rollback-{uuid.uuid4().hex}"
os.symlink(manifest["active_venv_target_before"], temporary_link)
os.replace(temporary_link, active)
fsync_directory(root)
print(json.dumps({
    "rollback_files_restored": True,
    "rollback_file_count": len(manifest["files"]),
    "active_venv_target_restored": manifest["active_venv_target_before"],
    "successor_or_predecessor_revalidation_passed": True,
    "backup_manifest_identity_sha256": claimed_manifest_sha,
    "rollback_removed_directories": removed_directories,
    "rollback_retained_nonempty_directories": retained_directories,
}, sort_keys=True))
""".strip()


def _performance_collection_source() -> str:
    return r"""
import csv, hashlib, json, os, pathlib, re, sys, time
root = pathlib.Path(sys.argv[1]).resolve()
duration_s = float(sys.argv[2])
telemetry_path = root / "logs/live_perf_telemetry.csv"
journal_root = pathlib.Path(sys.argv[3]).resolve()
expected_journal_root = (root / "formal_collection/order_lifecycle_journal_v2").resolve()
if journal_root != expected_journal_root or not journal_root.is_dir() or journal_root.is_symlink():
    raise SystemExit("journal-v2 root identity is invalid")
def sha(path):
    digest=hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda:handle.read(8<<20),b""):
            digest.update(chunk)
    return digest.hexdigest()
def canonical_sha(payload):
    return hashlib.sha256(json.dumps(
        payload,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False
    ).encode()).hexdigest()
def latest_session():
    sessions = [
        p for p in journal_root.glob("session-prospective-*")
        if p.parent == journal_root and p.is_dir() and not p.is_symlink()
    ]
    if not sessions:
        raise SystemExit("journal-v2 session is missing")
    return max(sessions, key=lambda p: p.stat().st_mtime_ns)
def maker_identity():
    matches=[]
    for item in pathlib.Path("/proc").iterdir():
        if not item.name.isdigit() or int(item.name)==os.getpid():
            continue
        try:
            cmdline=(item/"cmdline").read_bytes().replace(b"\0",b" ").decode()
        except (OSError,UnicodeDecodeError):
            continue
        if "live/main.py" in cmdline and "--config" in cmdline:
            matches.append((int(item.name),cmdline))
    if len(matches)!=1:
        raise SystemExit(f"expected exactly one maker PID, got {len(matches)}")
    return matches[0]
def read_json_regular(path,label):
    if not path.is_file() or path.is_symlink():
        raise SystemExit(f"unsafe or missing {label}")
    payload=json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload,dict):
        raise SystemExit(f"invalid {label}")
    return payload
def read_health(session):
    return read_json_regular(session/"live_health.json","live health")
def percentile(values, q):
    values = sorted(values)
    if not values:
        raise SystemExit("candidate telemetry window has no requote rows")
    index = (len(values) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    weight = index - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight
session=latest_session()
session_id=session.name.removeprefix("session-")
identity_path=session/"runtime_identity.json"
identity_payload=read_json_regular(identity_path,"runtime identity")
runtime_identity=identity_payload.get("runtime_identity")
if not isinstance(runtime_identity,dict):
    raise SystemExit("runtime identity body is invalid")
runtime_identity_sha=canonical_sha(runtime_identity)
if runtime_identity_sha != identity_payload.get("runtime_identity_sha256"):
    raise SystemExit("runtime identity canonical SHA256 mismatch")
baseline_epoch_id=str(runtime_identity.get("baseline_epoch_id", ""))
baseline_epoch_identity_sha256=str(runtime_identity.get("baseline_epoch_identity_sha256", ""))
if not baseline_epoch_id or not re.fullmatch(r"[0-9a-f]{64}",baseline_epoch_identity_sha256):
    raise SystemExit("runtime identity lacks the prospective epoch binding")
start_pid,start_cmdline=maker_identity()
start_health=read_health(session)
if start_health.get("session_id") != session_id:
    raise SystemExit("live health session identity mismatch")
if start_health.get("baseline_epoch_id") != baseline_epoch_id:
    raise SystemExit("live health epoch identity mismatch")
if start_health.get("core_health",{}).get("runtime_identity_sha256") != runtime_identity_sha:
    raise SystemExit("live health runtime identity mismatch")
started_wall = time.time()
time.sleep(duration_s)
ended_wall = time.time()
end_pid,end_cmdline=maker_identity()
if end_pid != start_pid or end_cmdline != start_cmdline:
    raise SystemExit("maker process identity changed during the performance window")
if latest_session() != session:
    raise SystemExit("journal session changed during the performance window")
end_health=read_health(session)
if end_health.get("session_id") != session_id:
    raise SystemExit("ending live health session identity mismatch")
if end_health.get("baseline_epoch_id") != baseline_epoch_id:
    raise SystemExit("ending live health epoch identity mismatch")
if end_health.get("core_health",{}).get("runtime_identity_sha256") != runtime_identity_sha:
    raise SystemExit("ending live health runtime identity mismatch")
epoch_root = pathlib.Path(str(end_health.get("epoch_root", ""))).resolve()
expected_epoch_root=(root/"formal_collection/prospective_baseline_epochs"/baseline_epoch_id).resolve()
if epoch_root != expected_epoch_root or not epoch_root.is_dir() or epoch_root.is_symlink():
    raise SystemExit("prospective epoch root identity mismatch")
epoch_manifest_path=epoch_root/"epoch_manifest.json"
epoch_manifest = read_json_regular(epoch_manifest_path,"epoch manifest")
if epoch_manifest.get("epoch_id") != baseline_epoch_id:
    raise SystemExit("prospective epoch manifest ID mismatch")
if epoch_manifest.get("identity_sha256") != baseline_epoch_identity_sha256:
    raise SystemExit("prospective epoch manifest identity mismatch")
initial_state_path=epoch_root/"initial_runtime_state.json"
initial_state_payload = read_json_regular(initial_state_path,"initial runtime state")
initial_state = initial_state_payload.get("state", {})
backend = initial_state.get("signal_feature_dag_warmup", {}).get("cpp_backend_state", {})
pre_epoch_native_events = int(backend.get("global_flow_boundary_event_count", -1)) + int(backend.get("cross_aggregator_count", -1))
parts=session/"parts"
if not parts.is_dir() or parts.is_symlink():
    raise SystemExit("journal parts root is unsafe")
standalone=[]
window_started_ns=int(started_wall*1_000_000_000)
window_ended_ns=int(ended_wall*1_000_000_000)
for manifest_path in parts.glob("part-*.manifest.json"):
    if not manifest_path.is_file() or manifest_path.is_symlink():
        continue
    payload=read_json_regular(manifest_path,"journal part manifest")
    batch_id=str(payload.get("batch_id", ""))
    if not re.fullmatch(r"[0-9a-f]{64}",batch_id):
        continue
    if manifest_path.name != f"part-{batch_id}.manifest.json":
        continue
    before=payload.get("checkpoint_before")
    if not isinstance(before,dict):
        continue
    if int(payload.get("first_lifecycle_sequence",-1)) != 1:
        continue
    if int(before.get("last_emitted_sequence",-1)) != 0 or str(before.get("last_event_id","")):
        continue
    committed_ts_ns=int(payload.get("committed_ts_ns",0))
    if not window_started_ns <= committed_ts_ns <= window_ended_ns:
        continue
    if payload.get("runtime_identity_sha256") != runtime_identity_sha:
        continue
    if payload.get("storage_format") != "parquet" or bool(payload.get("economic_outcomes_read")):
        continue
    data_path=parts/str(payload.get("data_file", ""))
    if data_path.parent != parts or data_path.name != f"part-{batch_id}.parquet":
        continue
    if not data_path.is_file() or data_path.is_symlink():
        continue
    data_sha=sha(data_path)
    if data_sha != payload.get("data_sha256"):
        continue
    standalone.append((committed_ts_ns,batch_id,manifest_path,payload,data_path,data_sha))
if not standalone:
    raise SystemExit("performance window has no independently recoverable sequence-1 part")
committed_ts_ns,batch_id,part_manifest,part_payload,data_path,data_sha=max(standalone)
exact_part={
    "session_root":str(session),
    "session_id":session_id,
    "manifest_relative":str(part_manifest.relative_to(session)),
    "manifest_sha256":sha(part_manifest),
    "manifest_size_bytes":part_manifest.stat().st_size,
    "data_relative":str(data_path.relative_to(session)),
    "data_sha256":data_sha,
    "data_size_bytes":data_path.stat().st_size,
    "runtime_identity_relative":"runtime_identity.json",
    "runtime_identity_file_sha256":sha(identity_path),
    "runtime_identity_size_bytes":identity_path.stat().st_size,
    "batch_id":batch_id,
    "row_count":int(part_payload["row_count"]),
    "journal_schema_sha256":str(part_payload["journal_schema_sha256"]),
    "runtime_identity_sha256":runtime_identity_sha,
    "first_lifecycle_sequence":int(part_payload["first_lifecycle_sequence"]),
    "checkpoint_before_last_emitted_sequence":int(part_payload["checkpoint_before"]["last_emitted_sequence"]),
    "checkpoint_before_last_event_id":str(part_payload["checkpoint_before"]["last_event_id"]),
    "committed_ts_ns":committed_ts_ns,
    "economic_outcomes_read":False,
}
values = []
with telemetry_path.open(newline="") as handle:
    for row in csv.DictReader(handle):
        try:
            timestamp = float(row["timestamp"])
            value = float(row["requote_total_us"])
        except (KeyError, TypeError, ValueError):
            continue
        if started_wall <= timestamp <= ended_wall:
            values.append(value)
elapsed = ended_wall - started_wall
worker_cpu_delta = float(end_health.get("worker_cpu_time_s", 0.0)) - float(start_health.get("worker_cpu_time_s", 0.0))
process_cpu_delta = float(end_health.get("process_cpu_time_s", 0.0)) - float(start_health.get("process_cpu_time_s", 0.0))
print(json.dumps({
    "collection_started_ts": started_wall,
    "collection_ended_ts": ended_wall,
    "collection_duration_s": elapsed,
    "requote_rows": len(values),
    "requote_total_us_p99": percentile(values, 0.99),
    "drop_count": int(end_health.get("drop_count", -1)),
    "error_count": int(end_health.get("error_count", -1)),
    "producer_enqueue_p99_us": float(end_health.get("enqueue_latency_p99_us", float("inf"))),
    "producer_enqueue_max_us": float(end_health.get("enqueue_latency_max_us", float("inf"))),
    "writer_queue_hwm": int(end_health.get("queue_hwm", -1)),
    "writer_cpu_pct_one_core": 100.0 * max(0.0, worker_cpu_delta) / elapsed,
    "process_cpu_pct_one_core": 100.0 * max(0.0, process_cpu_delta) / elapsed,
    "process_rss_kib": int(float(end_health.get("process_max_rss_mb", float("inf"))) * 1024.0),
    "writer_write_p99_ms": float(end_health.get("write_latency_p99_ms", float("inf"))),
    "maker_thread_filesystem_calls": int(end_health.get("maker_thread_filesystem_calls", -1)),
    "session_root": str(session),
    "session_id": session_id,
    "maker_pid": start_pid,
    "maker_cmdline": start_cmdline,
    "runtime_identity_sha256": runtime_identity_sha,
    "runtime_identity_file_sha256": sha(identity_path),
    "baseline_epoch_id": baseline_epoch_id,
    "baseline_epoch_identity_sha256": baseline_epoch_identity_sha256,
    "epoch_root": str(epoch_root),
    "epoch_manifest_file_sha256": sha(epoch_manifest_path),
    "initial_runtime_state_file_sha256": sha(initial_state_path),
    "exact_standalone_part": exact_part,
    "remote_spool_valid": bool(end_health.get("remote_spool_valid", False)),
    "initial_state_domains": sorted(initial_state),
    "initial_state_domain_complete": {name: True for name in initial_state},
    "epoch_binding_status": epoch_manifest.get("binding_status"),
    "pre_epoch_native_events": pre_epoch_native_events,
}, sort_keys=True))
""".strip()


def build_plan(
    *,
    bound: Mapping[str, Any],
    remote: str = DEFAULT_REMOTE,
    remote_root: str = DEFAULT_REMOTE_ROOT,
    remote_python: str = DEFAULT_REMOTE_PYTHON,
    isolated_release_root: str = DEFAULT_ISOLATED_RELEASE_ROOT,
    successor_requirements_lock: Path | None = None,
    successor_wheelhouse: Path | None = None,
    deployment_instance_id: str | None = None,
    rollback_stability_window_s: float = DEFAULT_ROLLBACK_STABILITY_WINDOW_S,
) -> dict[str, Any]:
    """Build a deterministic command plan without executing any command."""

    remote_root = _validate_remote_path(remote_root, "remote_root")
    if PurePosixPath(remote_root) != PurePosixPath(DEFAULT_REMOTE_ROOT):
        raise ValueError("remote_root must equal the frozen production repository root")
    isolated_release_root = _validate_isolated_stage_root(
        remote_root,
        isolated_release_root,
    )
    release = bound["release_manifest"]
    release_hash = bound["release_manifest_sha256"]
    stage_root = isolated_release_root
    deployment_instance_id = _validate_deployment_instance_id(deployment_instance_id)
    rollback_stability_window_s = _validate_rollback_stability_window_s(rollback_stability_window_s)
    backup_name = f"{release['release_id']}-{release_hash[:16]}"
    if deployment_instance_id is not None:
        backup_name = f"{backup_name}-{deployment_instance_id}"
    backup_root = f"{remote_root}/deploy_backups/{backup_name}"
    successor_venv = f"{stage_root}/.venv-successor"
    predecessors = {
        logical: record["sha256"] for logical, record in release["predecessors"].items()
    }
    new_files = sorted(
        logical for logical, absent in release["new_file_remote_absence"].items() if absent
    )
    runtime_code, deployment_scope = _runtime_code_files(bound["remote_identity"])
    runtime_code.update(predecessors)
    frozen_predecessor_pid = int(
        bound["remote_identity"].get("deployment", {}).get("current_pid", 0)
    )
    observed_predecessor_startup_contract = bound["remote_identity"].get("startup_contract")
    rollback_startup_stability_contract = {
        "schema_version": ROLLBACK_STABILITY_SCHEMA_VERSION,
        "required_predecessor_startup_contract": (REQUIRED_PREDECESSOR_STARTUP_CONTRACT),
        "observed_predecessor_startup_contract": (observed_predecessor_startup_contract),
        "predecessor_startup_contract_bound": (
            observed_predecessor_startup_contract == REQUIRED_PREDECESSOR_STARTUP_CONTRACT
        ),
        "stability_window_s": rollback_stability_window_s,
        "canonical_bucket_s": ROLLBACK_CANONICAL_BUCKET_S,
        "minimum_stable_buckets": ROLLBACK_MINIMUM_STABLE_BUCKETS,
        "log_path": ROLLBACK_STABILITY_LOG_PATH,
        "forbidden_log_markers": list(ROLLBACK_FORBIDDEN_LOG_MARKERS),
        "stability_probe_source_sha256": hashlib.sha256(
            _rollback_startup_stability_probe_source().encode("utf-8")
        ).hexdigest(),
    }

    runtime_probe = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            remote_python,
            _runtime_probe_source(),
            remote_root,
            json.dumps(runtime_code, sort_keys=True),
            "0",
        ),
    )
    rollback_runtime_probe = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            remote_python,
            _runtime_probe_source(),
            remote_root,
            json.dumps(runtime_code, sort_keys=True),
            "0",
        ),
    )
    predecessor_check = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            remote_python,
            _predecessor_revalidation_source(),
            remote_root,
            json.dumps(predecessors, sort_keys=True),
            json.dumps(new_files),
            stage_root,
        ),
    )
    staged_check = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            successor_venv + "/bin/python",
            _staged_validation_source(),
            stage_root,
            json.dumps(release["files"], sort_keys=True),
            release_hash,
            _sha256(bound["release_manifest_path"]),
            json.dumps(KNOWN_SUCCESSOR_PACKAGE_VERSIONS, sort_keys=True),
            KNOWN_NATIVE_EXTENSION_SHA256,
            remote_root,
        ),
    )
    rsync = [
        "rsync",
        "-a",
        "--delete",
        "--checksum",
        f"{bound['release_root']}/",
        f"{remote}:{stage_root}/",
    ]
    successor_inputs: dict[str, Any] = {
        "requirements_lock": None,
        "wheelhouse": None,
        "inputs_complete": False,
    }
    targeted_test_identities: list[dict[str, Any]] = []
    successor_commands: list[list[str]] = []
    if successor_requirements_lock is not None and successor_wheelhouse is not None:
        lock = successor_requirements_lock.expanduser().resolve(strict=True)
        wheelhouse = successor_wheelhouse.expanduser().resolve(strict=True)
        if lock.is_symlink() or not lock.is_file():
            raise ValueError("successor requirements lock must be a regular file")
        if wheelhouse.is_symlink() or not wheelhouse.is_dir():
            raise ValueError("successor wheelhouse must be a non-symlink directory")
        wheelhouse_files: list[dict[str, Any]] = []
        for wheel_path in sorted(wheelhouse.rglob("*")):
            if wheel_path.is_symlink():
                raise ValueError("successor wheelhouse must not contain symlinks")
            if not wheel_path.is_file():
                continue
            wheelhouse_files.append(
                {
                    "path": wheel_path.relative_to(wheelhouse).as_posix(),
                    "sha256": _sha256(wheel_path),
                    "size_bytes": wheel_path.stat().st_size,
                }
            )
        if not wheelhouse_files:
            raise ValueError("successor wheelhouse must contain at least one file")
        if (
            len(wheelhouse_files) != 1
            or not wheelhouse_files[0]["path"].startswith("pyarrow-24.0.0-")
            or not wheelhouse_files[0]["path"].endswith(".whl")
        ):
            raise ValueError("successor wheelhouse must contain only the frozen pyarrow 24 wheel")
        lock_text = lock.read_text(encoding="utf-8")
        lock_lines = [
            line.strip()
            for line in lock_text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        if (
            len(lock_lines) != 1
            or not lock_lines[0].startswith("pyarrow==24.0.0 ")
            or "--hash=sha256:" not in lock_lines[0]
        ):
            raise ValueError(
                "successor requirements lock must contain only hash-bound pyarrow==24.0.0"
            )
        successor_inputs = {
            "requirements_lock": str(lock),
            "requirements_lock_sha256": _sha256(lock),
            "wheelhouse": str(wheelhouse),
            "wheelhouse_files": wheelhouse_files,
            "wheelhouse_identity_sha256": _canonical_sha256(wheelhouse_files),
            "inputs_complete": True,
        }
        successor_commands = [
            [
                "rsync",
                "-a",
                "--checksum",
                f"{lock}",
                f"{remote}:{stage_root}/successor_requirements.lock",
            ],
            [
                "rsync",
                "-a",
                "--checksum",
                f"{wheelhouse}/",
                f"{remote}:{stage_root}/wheelhouse/",
            ],
            _ssh_command(
                remote,
                remote_root,
                (
                    f"env -u PYTHONPATH {shlex.quote(remote_python)} -I -c "
                    f"{shlex.quote(_clone_active_venv_source())} "
                    f"{shlex.quote(remote_root)} {shlex.quote(stage_root)} "
                    f"{shlex.quote(successor_venv)}"
                ),
            ),
            _ssh_command(
                remote,
                remote_root,
                (
                    f"env -u PYTHONPATH {shlex.quote(successor_venv + '/bin/python')} "
                    "-I -m pip install --no-index --no-deps --force-reinstall "
                    "--require-hashes --find-links "
                    f"{shlex.quote(stage_root + '/wheelhouse')} -r "
                    f"{shlex.quote(stage_root + '/successor_requirements.lock')}"
                ),
            ),
            _ssh_command(
                remote,
                remote_root,
                (
                    f"env -u PYTHONPATH {shlex.quote(successor_venv + '/bin/python')} "
                    f"-I -c {shlex.quote(_successor_runtime_validation_source())} "
                    f"{shlex.quote(json.dumps(KNOWN_SUCCESSOR_PACKAGE_VERSIONS, sort_keys=True))} "
                    f"{shlex.quote(KNOWN_NATIVE_EXTENSION_SHA256)}"
                ),
            ),
        ]
        repo_root = Path(__file__).resolve().parents[1]
        targeted_tests = (
            "tests/test_order_lifecycle_journal_v2.py",
            "tests/test_order_lifecycle_live_writer_v2.py",
            "tests/test_live_lifecycle_journal_v2_config.py",
            "tests/test_prospective_baseline_epoch.py",
            "tests/test_prospective_lifecycle_narrow_release_adapter.py",
        )
        successor_commands.append(
            _ssh_command(
                remote, remote_root, f"mkdir -p {shlex.quote(stage_root + '/remote_tests')}"
            )
        )
        for logical in targeted_tests:
            source = repo_root / logical
            if not source.is_file():
                raise ValueError(f"targeted lifecycle test is missing: {logical}")
            targeted_test_identities.append(
                {
                    "path": logical,
                    "sha256": _sha256(source),
                    "size_bytes": source.stat().st_size,
                }
            )
            successor_commands.append(
                [
                    "rsync",
                    "-a",
                    "--checksum",
                    str(source),
                    f"{remote}:{stage_root}/remote_tests/{source.name}",
                ]
            )
        pytest_argv = [
            "pytest",
            "-q",
            *(f"{stage_root}/remote_tests/{Path(logical).name}" for logical in targeted_tests),
            "-k",
            (
                "not test_maker_v2_hook_bypasses_synchronous_csv_writer and "
                "not test_maker_publishes_rest_reconciled_lifecycle_transition"
            ),
        ]
        pytest_bootstrap = "\n".join(
            (
                "import importlib.util",
                "import os",
                "import runpy",
                "import sys",
                "sys.dont_write_bytecode=True",
                "os.environ['NARROW_RELEASE_STAGE']='1'",
                (
                    "project_path_index=next((index for index,path in enumerate(sys.path) "
                    "if 'site-packages' in path or 'dist-packages' in path),len(sys.path))"
                ),
                (f"sys.path[project_path_index:project_path_index]={[stage_root, remote_root]!r}"),
                "def load_composite_package(name):",
                f"    package_paths=[f'{stage_root}/{{name}}',f'{remote_root}/{{name}}']",
                (f"    init_path=f'{remote_root}/{{name}}/__init__.py'"),
                (
                    "    spec=importlib.util.spec_from_file_location("
                    "name,init_path,submodule_search_locations=package_paths)"
                ),
                "    if spec is None or spec.loader is None:",
                "        raise RuntimeError(f'cannot load composite package {name}')",
                "    module=importlib.util.module_from_spec(spec)",
                "    sys.modules[name]=module",
                "    spec.loader.exec_module(module)",
                f"for package_name in {STAGED_OVERLAY_PACKAGES!r}:",
                "    load_composite_package(package_name)",
                "maker_engine=__import__('strategy.maker_engine',fromlist=['MakerEngine'])",
                "live_main=__import__('live.main',fromlist=['main'])",
                "live_ws=__import__('live.ws_handler',fromlist=['WSHandler'])",
                (f"assert maker_engine.__file__ == '{stage_root}/strategy/maker_engine.py'"),
                f"assert live_main.__file__ == '{stage_root}/live/main.py'",
                f"assert live_ws.__file__ == '{stage_root}/live/ws_handler.py'",
                f"sys.argv={pytest_argv!r}",
                "runpy.run_module('pytest',run_name='__main__')",
            )
        )
        successor_commands.append(
            _ssh_command(
                remote,
                remote_root,
                (
                    "cd /tmp && env -u PYTHONPATH "
                    f"{shlex.quote(successor_venv + '/bin/python')} -I -c "
                    f"{shlex.quote(pytest_bootstrap)}"
                ),
            )
        )

    deploy_command = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            remote_python,
            _atomic_deploy_source(),
            remote_root,
            stage_root,
            backup_root,
            successor_venv,
            json.dumps(release["files"], sort_keys=True),
            json.dumps(predecessors, sort_keys=True),
            json.dumps(new_files),
            release_hash,
            _sha256(bound["release_manifest_path"]),
            json.dumps(KNOWN_SUCCESSOR_PACKAGE_VERSIONS, sort_keys=True),
            KNOWN_NATIVE_EXTENSION_SHA256,
            f"{remote_root}/.venv-py312",
        ),
    )
    stop_command = _ssh_command(remote, remote_root, "bash live/run.sh stop")
    start_command = _ssh_command(remote, remote_root, "bash live/run.sh start")
    ensure_predecessor_running_command = _ssh_command(
        remote,
        remote_root,
        "bash live/run.sh status >/dev/null 2>&1 || bash live/run.sh start",
    )
    quiescence_command = _ssh_command(
        remote,
        remote_root,
        _remote_env_python_command(
            remote_root=remote_root,
            remote_python=PREDECESSOR_ROLLBACK_PYTHON,
            source=_quiescence_probe_source(),
            arguments=(remote_root,),
        ),
    )
    successor_runtime_code = dict(runtime_code)
    successor_runtime_code.update(
        {
            str(row["path"]): str(row["sha256"])
            for row in release["files"]
            if str(row["path"]).endswith((".py", ".yaml", ".json"))
        }
    )
    successor_runtime_probe = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            ".venv-active/bin/python3",
            _runtime_probe_source(),
            remote_root,
            json.dumps(successor_runtime_code, sort_keys=True),
            "0",
        ),
    )
    performance_command = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            ".venv-active/bin/python3",
            _performance_collection_source(),
            remote_root,
            "3600",
            release["config_semantics"]["lifecycle_journal_v2"]["root"],
        ),
    )
    rollback_command = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            PREDECESSOR_ROLLBACK_PYTHON,
            _atomic_rollback_source(),
            remote_root,
            backup_root,
            json.dumps(release["files"], sort_keys=True),
        ),
    )

    mutation_plan = {
        "remote": remote,
        "remote_root": remote_root,
        "remote_python": remote_python,
        "isolated_stage_root": stage_root,
        "backup_root": backup_root,
        "successor_venv": successor_venv,
        "release_manifest_sha256": release_hash,
        "remote_identity_sha256": bound["remote_identity_sha256"],
        "successor_venv_inputs": successor_inputs,
        "successor_package_versions": KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
        "successor_native_extension_sha256": KNOWN_NATIVE_EXTENSION_SHA256,
        "targeted_stage_test_identities": targeted_test_identities,
        "controlled_stop_command_sha256": _canonical_sha256(stop_command),
        "quiescence_probe_command_sha256": _canonical_sha256(quiescence_command),
        "deploy_command_sha256": _canonical_sha256(deploy_command),
        "start_command_sha256": _canonical_sha256(start_command),
        "ensure_predecessor_running_command_sha256": _canonical_sha256(
            ensure_predecessor_running_command
        ),
        "successor_runtime_probe_sha256": _canonical_sha256(successor_runtime_probe),
        "rollback_command_sha256": _canonical_sha256(rollback_command),
        "rollback_runtime_probe_sha256": _canonical_sha256(rollback_runtime_probe),
        "rollback_startup_stability_contract": (rollback_startup_stability_contract),
    }
    if deployment_instance_id is not None:
        mutation_plan["deployment_instance_id"] = deployment_instance_id
    mutation_plan_identity_sha256 = _canonical_sha256(mutation_plan)

    plan: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "mode": "dry_run_no_ssh_no_mutation",
        "release_id": release["release_id"],
        "release_manifest_path": str(bound["release_manifest_path"]),
        "release_manifest_sha256": release_hash,
        "remote_identity_path": str(bound["remote_identity_path"]),
        "remote_identity_sha256": bound["remote_identity_sha256"],
        "remote_baseline_id": bound["remote_identity"]["baseline_id"],
        "frozen_predecessor_pid": frozen_predecessor_pid,
        "remote_deployment_scope": deployment_scope,
        "remote": remote,
        "remote_root": remote_root,
        "remote_python": remote_python,
        "active_venv": f"{remote_root}/.venv-active",
        "active_venv_mutation_allowed": False,
        "active_runtime_pyarrow_observed": KNOWN_ACTIVE_PYARROW_VERSION,
        "required_successor_pyarrow_version": REQUIRED_SUCCESSOR_PYARROW_VERSION,
        "pyarrow_version_mismatch": True,
        "isolated_successor_venv_required": True,
        "isolated_successor_venv": successor_venv,
        "successor_venv_build_mode": (
            "clone_exact_active_python312_then_hash_replace_pyarrow_only"
        ),
        "successor_package_versions": KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
        "successor_venv_inputs": successor_inputs,
        "targeted_stage_test_identities": targeted_test_identities,
        "mutation_plan": mutation_plan,
        "mutation_plan_identity_sha256": mutation_plan_identity_sha256,
        "isolated_stage_root": stage_root,
        "isolated_stage_observed_uploaded": True,
        "isolated_stage_canonical_manifest_sha256": release_hash,
        "isolated_stage_manifest_file_sha256_prefix_observed": "df47525c",
        "isolated_stage_manifest_file_sha256_fully_bound": False,
        "isolated_pyarrow24_import_smoke_observed": True,
        "backup_root": backup_root,
        "frozen_strategy_flags": bound["frozen_strategy_flags"],
        "strategy_parameters_changed": False,
        "make_deploy_allowed": False,
        "required_initial_state_domains": list(INITIAL_STATE_DOMAINS),
        "performance_limits": PERFORMANCE_LIMITS,
        "baseline_quote_loop_telemetry": BASELINE_QUOTE_LOOP_TELEMETRY,
        "baseline_quote_loop_telemetry_sha256": BASELINE_QUOTE_LOOP_TELEMETRY_SHA256,
        "baseline_process_resource": BASELINE_PROCESS_RESOURCE,
        "baseline_process_resource_sha256": _canonical_sha256(BASELINE_PROCESS_RESOURCE),
        "required_gates": list(REQUIRED_GATES),
        "rollback_startup_stability_contract": (rollback_startup_stability_contract),
        "source_payload_current": bound["source_payload_current"],
        "source_payload_drift": bound["source_payload_drift"],
        "deployment_blockers": (
            []
            if bound["source_payload_current"]
            else ["source_payload_stale_rebuild_and_revalidate_exact_manifest_required"]
        ),
        "stages": {
            "rebuild-validate": {
                "remote_mutation": False,
                "local_mutation": True,
                "production_mutation": False,
                "execution_flag": "--execute-local-rebuild",
                "result_must_be_reselected_as_exact_manifest_input": True,
            },
            "runtime-evidence": {
                "remote_mutation": False,
                "production_mutation": False,
                "execution_flag": "--execute-read-only-ssh",
                "commands": [runtime_probe],
            },
            "stage-validate": {
                "remote_mutation": True,
                "production_mutation": False,
                "execution_flag": "--execute-isolated-stage",
                "commands": [predecessor_check, rsync, *successor_commands, staged_check],
                "successor_venv_required": True,
                "successor_venv_ready_to_build": successor_inputs["inputs_complete"],
                "active_venv_mutated": False,
            },
            "deploy-restart": {
                "remote_mutation": True,
                "production_mutation": True,
                "execution_flag": "--execute-production-mutation",
                "owner_confirmation_token": owner_confirmation_token(
                    "deploy-restart",
                    bound,
                    mutation_plan_identity_sha256,
                ),
                "predecessor_revalidation": predecessor_check,
                "commands": [
                    stop_command,
                    quiescence_command,
                    deploy_command,
                    start_command,
                    successor_runtime_probe,
                ],
                "automatic_rollback_commands": [
                    stop_command,
                    quiescence_command,
                    rollback_command,
                    ensure_predecessor_running_command,
                    rollback_runtime_probe,
                ],
                "expected_successor_runtime_files": successor_runtime_code,
                "deployment_method": (
                    "graceful_stop_zero_open_orders_then_exact_file_overlay_"
                    "atomic_replace_start_probe_or_automatic_rollback"
                ),
                "forbidden_method": "make deploy",
            },
            "performance": {
                "remote_mutation": False,
                "production_mutation": False,
                "execution_flag": "--execute-read-only-ssh",
                "bounded_duration_s": 3600,
                "required_metrics": sorted(PERFORMANCE_LIMITS),
                "commands": [performance_command],
            },
            "admit": {
                "remote_mutation": False,
                "local_mutation": True,
                "production_mutation": False,
                "execution_flag": "--execute-admission",
                "atomic": True,
            },
            "rollback-drill": {
                "remote_mutation": True,
                "production_mutation": True,
                "execution_flag": "--execute-production-mutation",
                "owner_confirmation_token": owner_confirmation_token(
                    "rollback-drill",
                    bound,
                    mutation_plan_identity_sha256,
                ),
                "predecessor_revalidation_required": True,
                "startup_stability_contract": (rollback_startup_stability_contract),
                "mutation_blocked_before_stop": (
                    not rollback_startup_stability_contract["predecessor_startup_contract_bound"]
                ),
                "rollback_source": backup_root,
                "restart_method": "controlled_stop_then_ensure_predecessor_running",
                "commands": [
                    stop_command,
                    quiescence_command,
                    rollback_command,
                    ensure_predecessor_running_command,
                    rollback_runtime_probe,
                ],
            },
        },
        "execution_performed": False,
        "deployment_authorized": False,
        "deployment_executed": False,
    }
    if deployment_instance_id is not None:
        plan["deployment_instance_id"] = deployment_instance_id
    plan["plan_sha256"] = _canonical_sha256(plan)
    return plan


def validate_runtime_probe(
    probe: Mapping[str, Any],
    bound: Mapping[str, Any],
    *,
    expected_runtime_files: Mapping[str, str] | None = None,
    expected_package_versions: Mapping[str, str] | None = None,
    require_frozen_native_path: bool = True,
    expected_python_prefix: str | None = KNOWN_ACTIVE_PYTHON_PREFIX,
) -> dict[str, bool]:
    extensions = probe.get("loaded_native_extensions")
    native_bound = isinstance(extensions, list) and len(extensions) == 1
    if native_bound:
        for row in extensions:
            if (
                not isinstance(row, dict)
                or (require_frozen_native_path and row.get("path") != KNOWN_NATIVE_EXTENSION_PATH)
                or row.get("sha256") != KNOWN_NATIVE_EXTENSION_SHA256
            ):
                native_bound = False
                break
            try:
                _validate_hex_sha256(row.get("sha256"), "native extension sha256")
            except ValueError:
                native_bound = False
                break
    if expected_runtime_files is None:
        expected_runtime, _ = _runtime_code_files(bound["remote_identity"])
        expected_runtime.update(
            {
                logical: record["sha256"]
                for logical, record in bound["release_manifest"]["predecessors"].items()
            }
        )
    else:
        expected_runtime = dict(expected_runtime_files)
    expected_packages = (
        KNOWN_ACTIVE_PACKAGE_VERSIONS
        if expected_package_versions is None
        else dict(expected_package_versions)
    )
    return {
        "remote_python_3_12_13_verified": probe.get("python_version") == "3.12.13",
        "remote_python_prefix_bound": (
            expected_python_prefix is None or probe.get("python_prefix") == expected_python_prefix
        ),
        "remote_pyarrow_24_0_0_verified": (
            probe.get("pyarrow_version") == REQUIRED_SUCCESSOR_PYARROW_VERSION
        ),
        "native_extension_path_and_sha256_bound": native_bound,
        "remote_runtime_identity_matches_v9": probe.get("runtime_files") == expected_runtime,
        "remote_package_set_matches_expected": (probe.get("package_versions") == expected_packages),
        "single_maker_pid_bound": isinstance(probe.get("maker_pid"), int)
        and int(probe["maker_pid"]) > 0,
    }


def evaluate_runtime_gates(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate all machine gates from admitted stage evidence."""

    runtime = evidence.get("runtime", {})
    staging = evidence.get("staging", {})
    epoch = evidence.get("epoch", {})
    performance = evidence.get("performance", {})
    admission = evidence.get("admission", {})
    rollback = evidence.get("rollback", {})
    for label, value in {
        "runtime": runtime,
        "staging": staging,
        "epoch": epoch,
        "performance": performance,
        "admission": admission,
        "rollback": rollback,
    }.items():
        if not isinstance(value, Mapping):
            raise ValueError(f"{label} evidence must be an object")

    domains = epoch.get("initial_state_domains")
    domain_complete = (
        isinstance(domains, list)
        and len(domains) == len(INITIAL_STATE_DOMAINS)
        and set(domains) == set(INITIAL_STATE_DOMAINS)
        and all(bool(epoch.get("initial_state_domain_complete", {}).get(name)) for name in domains)
    )
    baseline_perf = performance.get("baseline", {})
    candidate_perf = performance.get("candidate", {})
    if not isinstance(baseline_perf, Mapping) or not isinstance(candidate_perf, Mapping):
        raise ValueError("performance baseline/candidate evidence must be objects")
    baseline_bound = dict(baseline_perf) == BASELINE_QUOTE_LOOP_TELEMETRY
    duration = float(candidate_perf.get("collection_duration_s", -1.0))
    baseline_p99 = float(BASELINE_QUOTE_LOOP_TELEMETRY["requote_total_us_p99"])
    candidate_p99 = float(candidate_perf.get("requote_total_us_p99", float("inf")))
    quote_loop_regression_pct = ((candidate_p99 / baseline_p99) - 1.0) * 100.0
    baseline_resource = performance.get("baseline_process_resource", {})
    resource_bound = dict(baseline_resource) == BASELINE_PROCESS_RESOURCE
    candidate_rss_kib = int(candidate_perf.get("process_rss_kib", 2**63 - 1))
    rss_delta_mib = (candidate_rss_kib - int(BASELINE_PROCESS_RESOURCE["rss_kib"])) / 1024.0
    gates = {
        "remote_python_3_12_13_verified": (
            runtime.get("python_version") == "3.12.13"
            and bool(runtime.get("remote_python_prefix_bound"))
        ),
        "remote_pyarrow_24_0_0_verified": runtime.get("pyarrow_version") == "24.0.0",
        "native_extension_path_and_sha256_bound": bool(runtime.get("loaded_native_extensions"))
        and bool(runtime.get("native_extensions_hash_valid")),
        "staged_overlay_import_smoke_passed": bool(
            staging.get("staged_overlay_import_smoke_passed")
        ),
        "targeted_lifecycle_tests_passed_on_remote_venv": bool(
            staging.get("targeted_lifecycle_tests_passed_on_remote_venv")
        ),
        "initial_state_13_domain_completeness_passed": domain_complete,
        "zero_pre_epoch_native_events": int(epoch.get("pre_epoch_native_events", -1)) == 0,
        "one_hour_zero_drop_zero_error": (
            PERFORMANCE_LIMITS["minimum_collection_duration_s"]
            <= duration
            <= PERFORMANCE_LIMITS["maximum_collection_duration_s"]
            and int(candidate_perf.get("drop_count", -1)) == 0
            and int(candidate_perf.get("error_count", -1)) == 0
        ),
        "producer_enqueue_p99_le_100us": float(
            candidate_perf.get("producer_enqueue_p99_us", float("inf"))
        )
        <= PERFORMANCE_LIMITS["producer_enqueue_p99_us"],
        "producer_enqueue_max_le_1000us": float(
            candidate_perf.get("producer_enqueue_max_us", float("inf"))
        )
        <= PERFORMANCE_LIMITS["producer_enqueue_max_us"],
        "quote_loop_p99_regression_le_5pct": baseline_bound
        and candidate_p99
        <= baseline_p99 * (1.0 + PERFORMANCE_LIMITS["quote_loop_p99_regression_pct"] / 100.0),
        "writer_queue_hwm_le_2048": int(
            candidate_perf.get("writer_queue_hwm", PERFORMANCE_LIMITS["writer_queue_hwm"] + 1)
        )
        <= PERFORMANCE_LIMITS["writer_queue_hwm"],
        "writer_cpu_le_10pct_one_core": float(
            candidate_perf.get("writer_cpu_pct_one_core", float("inf"))
        )
        <= PERFORMANCE_LIMITS["writer_cpu_pct_one_core"],
        "writer_rss_delta_le_256mib": resource_bound
        and rss_delta_mib <= PERFORMANCE_LIMITS["writer_rss_delta_mib"],
        "writer_write_p99_le_250ms": float(candidate_perf.get("writer_write_p99_ms", float("inf")))
        <= PERFORMANCE_LIMITS["writer_write_p99_ms"],
        "maker_thread_filesystem_calls_zero": bool(
            staging.get("maker_thread_filesystem_calls_zero")
        ),
        "bounded_spool_admission_roundtrip_passed": bool(
            admission.get("bounded_spool_admission_roundtrip_passed")
        ),
        "rollback_restart_rehearsed": bool(rollback.get("rollback_restart_rehearsed")),
    }
    missing = [name for name in REQUIRED_GATES if not gates[name]]
    result = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "gates": gates,
        "all_runtime_gates_passed": not missing,
        "missing_or_failed_gates": missing,
        "deployment_research_authority_granted": False,
        "strategy_action_authorized": False,
        "economic_outcomes_read": False,
        "baseline_quote_loop_telemetry_bound": baseline_bound,
        "baseline_quote_loop_telemetry_sha256": BASELINE_QUOTE_LOOP_TELEMETRY_SHA256,
        "candidate_quote_loop_p99_us": candidate_p99,
        "quote_loop_p99_regression_pct_computed": quote_loop_regression_pct,
        "baseline_process_resource_bound": resource_bound,
        "baseline_process_resource_sha256": _canonical_sha256(BASELINE_PROCESS_RESOURCE),
        "candidate_process_rss_kib": candidate_rss_kib,
        "process_rss_delta_mib_computed": rss_delta_mib,
        "writer_cpu_pct_one_core": float(
            candidate_perf.get("writer_cpu_pct_one_core", float("inf"))
        ),
        "process_cpu_pct_one_core": float(
            candidate_perf.get("process_cpu_pct_one_core", float("inf"))
        ),
        "cpu_scope_note": "writer_thread_gate_process_overall_diagnostic",
    }
    result["evidence_sha256"] = _canonical_sha256(result)
    return result


def _validate_evidence_receipt_chain(
    receipts: Mapping[str, Mapping[str, Any]],
    *,
    bound: Mapping[str, Any],
) -> dict[str, Any]:
    required_stages = {"runtime", "staging", "epoch", "performance", "admission", "rollback"}
    missing_stages = sorted(required_stages - set(receipts))
    if missing_stages:
        raise ValueError("evidence bundle is missing stages: " + ",".join(missing_stages))
    binding_candidates: list[dict[str, Any]] = []
    for stage, receipt in receipts.items():
        if receipt.get("deployment_binding") is None:
            continue
        binding = _validate_embedded_deployment_binding(receipt)
        if binding.get("release_manifest_sha256") != bound["release_manifest_sha256"]:
            raise ValueError(f"{stage} deployment binding release hash mismatch")
        if binding.get("remote_identity_sha256") != bound["remote_identity_sha256"]:
            raise ValueError(f"{stage} deployment binding baseline identity mismatch")
        binding_candidates.append(binding)
    if not binding_candidates:
        raise ValueError("evidence bundle has no deployment binding")
    deployment_binding = binding_candidates[0]
    if any(candidate != deployment_binding for candidate in binding_candidates[1:]):
        raise ValueError("evidence bundle mixes deployment instances")

    required_bound_stages = ("performance", "epoch", "admission", "rollback")
    for stage in required_bound_stages:
        receipt = receipts.get(stage)
        if receipt is None or receipt.get("deployment_binding") is None:
            raise ValueError(f"{stage} receipt lacks the deployment binding")

    staging = receipts["staging"]
    if staging.get("mutation_plan_identity_sha256") != deployment_binding.get(
        "mutation_plan_identity_sha256"
    ):
        raise ValueError("staging receipt mutation plan differs from deployment binding")

    runtime = receipts["runtime"]
    runtime_receipt_id = runtime.get("receipt_identity_sha256")
    if runtime.get("deployment_binding") is not None:
        _validate_embedded_deployment_binding(runtime, expected=deployment_binding)
        if runtime.get("parent_staging_receipt_identity_sha256") != staging.get(
            "receipt_identity_sha256"
        ):
            raise ValueError("runtime receipt does not descend from staging receipt")
    else:
        runtime_evidence = runtime.get("evidence")
        deployment = (
            runtime_evidence.get("deployment") if isinstance(runtime_evidence, Mapping) else None
        )
        if not isinstance(deployment, Mapping):
            raise ValueError("legacy runtime receipt lacks deployment evidence")
        if deployment.get("backup_root") != deployment_binding.get("backup_root"):
            raise ValueError("legacy runtime receipt backup differs from deployment binding")
        if deployment.get("successor_venv") != deployment_binding.get("successor_venv"):
            raise ValueError("legacy runtime receipt successor differs from deployment binding")

    performance = receipts["performance"]
    epoch = receipts["epoch"]
    admission = receipts["admission"]
    rollback = receipts["rollback"]
    if performance.get("parent_runtime_receipt_identity_sha256") != runtime_receipt_id:
        raise ValueError("performance receipt does not descend from runtime receipt")
    if epoch.get("parent_runtime_receipt_identity_sha256") != runtime_receipt_id:
        raise ValueError("epoch receipt does not descend from runtime receipt")
    if admission.get("parent_performance_receipt_identity_sha256") != performance.get(
        "receipt_identity_sha256"
    ):
        raise ValueError("admission receipt does not descend from performance receipt")
    if rollback.get("parent_runtime_receipt_identity_sha256") != runtime_receipt_id:
        raise ValueError("rollback receipt does not descend from runtime receipt")
    for stage, receipt in {"performance": performance, "rollback": rollback}.items():
        normalization = receipt.get("runtime_receipt_normalization")
        if not isinstance(normalization, Mapping):
            raise ValueError(f"{stage} receipt lacks runtime normalization evidence")
        if normalization.get("runtime_receipt_identity_sha256") != runtime_receipt_id:
            raise ValueError(f"{stage} normalized a different runtime receipt")
        if normalization.get("deployment_binding_sha256") != _canonical_sha256(deployment_binding):
            raise ValueError(f"{stage} runtime normalization binding differs")
    return deployment_binding


def admit_evidence_atomically(
    *,
    bound: Mapping[str, Any],
    evidence_paths: Sequence[Path],
    admission_root: Path,
) -> Path:
    """Content-address and atomically admit a complete evidence bundle."""

    if not evidence_paths:
        raise ValueError("at least one evidence file is required")
    unresolved_admission_root = admission_root.expanduser()
    if unresolved_admission_root.is_symlink():
        raise ValueError("admission destination must not be a symlink")
    admission_root = unresolved_admission_root.resolve()
    if admission_root.exists():
        raise FileExistsError(f"admission destination already exists: {admission_root}")
    records: list[dict[str, Any]] = []
    merged: dict[str, Any] = {}
    receipts: dict[str, dict[str, Any]] = {}
    for raw in evidence_paths:
        unresolved = raw.expanduser()
        if unresolved.is_symlink():
            raise ValueError(f"evidence input must be a regular non-symlink file: {unresolved}")
        path = unresolved.resolve(strict=True)
        if not path.is_file():
            raise ValueError(f"evidence input must be a regular file: {path}")
        payload = _read_object(path, "evidence input")
        section = str(payload.get("stage", "")).strip()
        if section not in {"runtime", "staging", "epoch", "performance", "admission", "rollback"}:
            raise ValueError(f"unsupported or missing evidence stage: {section!r}")
        if section in merged:
            raise ValueError(f"duplicate evidence stage: {section}")
        _validate_stage_receipt(payload, stage=section, bound=bound)
        body = payload.get("evidence")
        if not isinstance(body, dict):
            raise ValueError(f"evidence body must be an object: {section}")
        merged[section] = body
        receipts[section] = payload
        records.append(
            {
                "stage": section,
                "source_path": str(path),
                "source_sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
        )
    deployment_binding = _validate_evidence_receipt_chain(receipts, bound=bound)
    gate_result = evaluate_runtime_gates(merged)
    if not gate_result["all_runtime_gates_passed"]:
        raise ValueError(
            "runtime evidence is incomplete: " + ",".join(gate_result["missing_or_failed_gates"])
        )

    temporary = admission_root.parent / (
        f".{admission_root.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    )
    temporary.mkdir(parents=True)
    try:
        for record in records:
            source = Path(record["source_path"])
            destination = temporary / f"{record['stage']}.json"
            shutil.copyfile(source, destination)
            if _sha256(destination) != record["source_sha256"]:
                raise ValueError(f"admitted evidence hash mismatch: {record['stage']}")
        manifest = {
            "schema_version": ADMISSION_SCHEMA_VERSION,
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            "remote_baseline_id": bound["remote_identity"]["baseline_id"],
            "deployment_binding": deployment_binding,
            "deployment_binding_sha256": _canonical_sha256(deployment_binding),
            "files": sorted(records, key=lambda row: row["stage"]),
            "gate_result": gate_result,
            "atomic_admission": True,
            "admission_complete": True,
            "economic_outcomes_read": False,
            "strategy_parameters_changed": False,
            "q90_action_enabled": False,
            "buy_fill_selection_enabled": False,
        }
        manifest["manifest_sha256"] = _canonical_sha256(manifest)
        _atomic_write_json(temporary / "admission_manifest.json", manifest)
        os.replace(temporary, admission_root)
        directory_fd = os.open(admission_root.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return admission_root / "admission_manifest.json"


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True)


def _run_checked(command: Sequence[str], runner: CommandRunner) -> dict[str, Any]:
    completed = runner(command)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed rc={completed.returncode}: {shlex.join(command)}\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )
    output = completed.stdout.strip().splitlines()
    if not output:
        raise RuntimeError("remote command produced no JSON evidence")
    payload = json.loads(output[-1])
    if not isinstance(payload, dict):
        raise RuntimeError("remote command did not produce a JSON object")
    return payload


def execute_read_only_runtime_probe(
    *, plan: Mapping[str, Any], bound: Mapping[str, Any], runner: CommandRunner = _default_runner
) -> dict[str, Any]:
    command = plan["stages"]["runtime-evidence"]["commands"][0]
    probe = _run_checked(command, runner)
    gates = validate_runtime_probe(probe, bound)
    identity_keys = (
        "remote_python_3_12_13_verified",
        "remote_python_prefix_bound",
        "native_extension_path_and_sha256_bound",
        "remote_package_set_matches_expected",
        "remote_runtime_identity_matches_v9",
        "single_maker_pid_bound",
    )
    if not all(gates[key] for key in identity_keys):
        raise ValueError(f"remote runtime identity evidence failed: {gates}")
    active_pyarrow = str(probe.get("pyarrow_version", ""))
    observed_pid = int(probe.get("maker_pid", 0) or 0)
    frozen_pid = int(plan.get("frozen_predecessor_pid", 0) or 0)
    return _seal_receipt(
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "stage": "runtime",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            "evidence": {
                **probe,
                **gates,
                "native_extensions_hash_valid": True,
                "pyarrow_version_mismatch": (active_pyarrow != REQUIRED_SUCCESSOR_PYARROW_VERSION),
                "active_runtime_pyarrow_version": active_pyarrow,
                "required_successor_pyarrow_version": REQUIRED_SUCCESSOR_PYARROW_VERSION,
                "successor_venv_required": (active_pyarrow != REQUIRED_SUCCESSOR_PYARROW_VERSION),
                "active_venv_mutation_allowed": False,
                "frozen_predecessor_pid": frozen_pid,
                "observed_predecessor_pid": observed_pid,
                "predecessor_pid_matches_frozen_identity": (
                    frozen_pid <= 0 or observed_pid == frozen_pid
                ),
                "prospective_process_epoch_required": (
                    frozen_pid > 0 and observed_pid != frozen_pid
                ),
                "runtime_evidence_passed": all(gates.values()),
            },
            "ssh_mutation_performed": False,
        }
    )


def _require_owner_mutation(
    *,
    stage: str,
    bound: Mapping[str, Any],
    mutation_plan_identity_sha256: str,
    execute: bool,
    token: str | None,
) -> None:
    if stage not in PRODUCTION_MUTATING_STAGES:
        raise ValueError(f"not a production mutation stage: {stage}")
    if not execute:
        raise PermissionError(f"{stage} requires --execute-production-mutation")
    expected = owner_confirmation_token(
        stage,
        bound,
        mutation_plan_identity_sha256,
    )
    if token != expected:
        raise PermissionError(f"{stage} owner confirmation token mismatch")


def _write_optional(path: Path | None, payload: Mapping[str, Any]) -> None:
    if path is not None:
        _atomic_write_json(path.expanduser().resolve(), payload)


def _required_receipt(
    paths: Sequence[Path],
    *,
    stage: str,
    bound: Mapping[str, Any],
) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for raw in paths:
        unresolved = raw.expanduser()
        if unresolved.is_symlink():
            raise ValueError(f"stage evidence receipt must not be a symlink: {unresolved}")
        path = unresolved.resolve(strict=True)
        payload = _read_object(path, "stage evidence receipt")
        if payload.get("stage") != stage:
            continue
        _validate_stage_receipt(payload, stage=stage, bound=bound)
        matches.append(payload)
    if len(matches) != 1:
        raise ValueError(f"exactly one {stage} receipt is required")
    return matches[0]


def _run_plain_checked(command: Sequence[str], runner: CommandRunner) -> str:
    completed = runner(command)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed rc={completed.returncode}: {shlex.join(command)}\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )
    return completed.stdout


def _validate_predecessor_runtime_after_rollback(
    *,
    probe: Mapping[str, Any],
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
) -> dict[str, bool]:
    expected_prefix = f"{plan['remote_root']}/.venv-py312"
    gates = validate_runtime_probe(
        probe,
        bound,
        expected_package_versions=KNOWN_ACTIVE_PACKAGE_VERSIONS,
        require_frozen_native_path=True,
        expected_python_prefix=expected_prefix,
    )
    required = (
        "remote_python_3_12_13_verified",
        "remote_python_prefix_bound",
        "native_extension_path_and_sha256_bound",
        "remote_runtime_identity_matches_v9",
        "remote_package_set_matches_expected",
        "single_maker_pid_bound",
    )
    if probe.get("pyarrow_version") != KNOWN_ACTIVE_PYARROW_VERSION:
        raise RuntimeError(
            f"rollback predecessor pyarrow identity failed: {probe.get('pyarrow_version')!r}"
        )
    if not all(gates[name] for name in required):
        raise RuntimeError(f"rollback predecessor runtime identity failed: {gates}")
    return gates


def _rollback_startup_stability_probe_command(
    *,
    plan: Mapping[str, Any],
    expected_pid: int,
) -> list[str]:
    contract = plan["stages"]["rollback-drill"].get("startup_stability_contract")
    if not isinstance(contract, Mapping):
        raise ValueError("rollback startup stability contract is missing")
    source = _rollback_startup_stability_probe_source()
    source_sha256 = hashlib.sha256(source.encode("utf-8")).hexdigest()
    if contract.get("stability_probe_source_sha256") != source_sha256:
        raise ValueError("rollback startup stability probe source hash drifted")
    return _ssh_command(
        str(plan["remote"]),
        str(plan["remote_root"]),
        _remote_python_command(
            PREDECESSOR_ROLLBACK_PYTHON,
            source,
            str(plan["remote_root"]),
            str(int(expected_pid)),
            str(float(contract["stability_window_s"])),
            str(float(contract["canonical_bucket_s"])),
            str(int(contract["minimum_stable_buckets"])),
            str(contract["log_path"]),
            json.dumps(contract["forbidden_log_markers"], sort_keys=True),
        ),
    )


def _validate_rollback_startup_stability(
    *,
    observation: Mapping[str, Any],
    plan: Mapping[str, Any],
    expected_pid: int,
) -> dict[str, bool]:
    contract = plan["stages"]["rollback-drill"].get("startup_stability_contract")
    if not isinstance(contract, Mapping):
        raise RuntimeError("rollback startup stability contract is missing")
    required_duration_s = float(contract["canonical_bucket_s"]) * int(
        contract["minimum_stable_buckets"]
    )
    observed_duration_s = float(observation.get("observed_duration_s", -1.0))
    gates = {
        "stability_schema_bound": (
            observation.get("schema_version") == ROLLBACK_STABILITY_SCHEMA_VERSION
        ),
        "stability_window_bound": (
            float(observation.get("stability_window_s", -1.0))
            == float(contract["stability_window_s"])
            and float(observation.get("canonical_bucket_s", -1.0))
            == float(contract["canonical_bucket_s"])
            and int(observation.get("minimum_stable_buckets", -1))
            == int(contract["minimum_stable_buckets"])
        ),
        "two_canonical_buckets_observed": (
            observed_duration_s >= float(contract["stability_window_s"])
            and observed_duration_s >= required_duration_s
            and float(observation.get("covered_canonical_buckets", -1.0))
            >= int(contract["minimum_stable_buckets"])
        ),
        "same_predecessor_pid_stable": (
            int(observation.get("expected_maker_pid", -1)) == int(expected_pid)
            and bool(observation.get("same_maker_process"))
        ),
        "maker_log_identity_stable": (
            bool(observation.get("log_exists_at_start"))
            and bool(observation.get("log_exists_at_end"))
            and bool(observation.get("log_identity_stable"))
        ),
        "fatal_and_duplicate_grid_absent": (
            observation.get("forbidden_log_hits") == []
            and bool(observation.get("fatal_or_duplicate_grid_absent"))
        ),
    }
    if not all(gates.values()):
        raise RuntimeError(
            "rollback predecessor failed the frozen startup stability window: "
            f"{json.dumps({'gates': gates, 'observation': dict(observation)}, sort_keys=True)}"
        )
    return gates


def _validate_strict_rollback_result(
    rollback: Mapping[str, Any],
    *,
    deployment_evidence: Mapping[str, Any],
    expected_file_count: int,
) -> None:
    if not bool(rollback.get("rollback_files_restored")) or bool(
        rollback.get("rollback_not_required")
    ):
        raise RuntimeError("rollback drill performed no exact file restoration")
    if int(rollback.get("rollback_file_count", -1)) != int(expected_file_count):
        raise RuntimeError("rollback drill restored-file count differs")
    if rollback.get("backup_manifest_identity_sha256") != deployment_evidence.get(
        "backup_manifest_canonical_sha256"
    ):
        raise RuntimeError("rollback drill backup manifest differs from deployment receipt")
    if rollback.get("active_venv_target_restored") != deployment_evidence.get(
        "active_venv_target_before"
    ):
        raise RuntimeError("rollback drill restored the wrong predecessor venv")


def execute_deploy_restart_transaction(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    """Deploy only while quiescent and restore the predecessor on any failure."""

    stage = plan["stages"]["deploy-restart"]
    commands = stage["commands"]
    recovery_commands = stage["automatic_rollback_commands"]
    if len(commands) != 5 or len(recovery_commands) != 5:
        raise ValueError("deploy transaction command shape is invalid")
    try:
        stop_output = _run_plain_checked(commands[0], runner)
        quiescence = _run_checked(commands[1], runner)
        if not bool(quiescence.get("controlled_stop_quiescent")):
            raise RuntimeError(f"controlled stop did not become quiescent: {quiescence}")
        deployment = _run_checked(commands[2], runner)
        _run_plain_checked(commands[3], runner)
        probe = _run_checked(commands[4], runner)
        runtime_gates = validate_runtime_probe(
            probe,
            bound,
            expected_runtime_files=stage["expected_successor_runtime_files"],
            expected_package_versions=KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
            require_frozen_native_path=False,
            expected_python_prefix=plan["isolated_successor_venv"],
        )
        if not all(runtime_gates.values()):
            raise RuntimeError(f"post-deploy successor runtime failed: {runtime_gates}")
        return {
            "controlled_stop_output": stop_output,
            "quiescence": quiescence,
            "deployment": deployment,
            "probe": probe,
            "runtime_gates": runtime_gates,
            "automatic_rollback_performed": False,
        }
    except Exception as deployment_error:
        recovery: dict[str, Any] = {
            "deployment_error": repr(deployment_error),
            "automatic_rollback_attempted": True,
        }
        try:
            recovery["stop_output"] = _run_plain_checked(recovery_commands[0], runner)
            recovery["quiescence"] = _run_checked(recovery_commands[1], runner)
            if not bool(recovery["quiescence"].get("controlled_stop_quiescent")):
                raise RuntimeError(
                    f"rollback stop did not become quiescent: {recovery['quiescence']}"
                )
            recovery["rollback"] = _run_checked(recovery_commands[2], runner)
            recovery["ensure_running_output"] = _run_plain_checked(recovery_commands[3], runner)
            recovery["probe"] = _run_checked(recovery_commands[4], runner)
            recovery["runtime_gates"] = _validate_predecessor_runtime_after_rollback(
                probe=recovery["probe"],
                plan=plan,
                bound=bound,
            )
            recovery["automatic_rollback_succeeded"] = True
        except Exception as rollback_error:
            recovery["automatic_rollback_succeeded"] = False
            recovery["rollback_error"] = repr(rollback_error)
            raise RuntimeError(
                "deploy transaction failed and automatic rollback did not restore "
                f"the predecessor: {json.dumps(recovery, sort_keys=True)}"
            ) from rollback_error
        raise RuntimeError(
            "deploy transaction failed; predecessor was restored automatically: "
            f"{json.dumps(recovery, sort_keys=True)}"
        ) from deployment_error


def execute_rollback_drill_transaction(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
    runtime_evidence: Mapping[str, Any],
    deployment_evidence: Mapping[str, Any],
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    """Restore only an explicitly safe predecessor and prove delayed stability."""

    _require_rollback_predecessor_startup_contract(plan=plan, bound=bound)
    commands = plan["stages"]["rollback-drill"]["commands"]
    if len(commands) != 5:
        raise ValueError("rollback drill command shape is invalid")

    pre_rollback_probe = _run_checked(
        plan["stages"]["deploy-restart"]["commands"][4],
        runner,
    )
    pre_rollback_gates = validate_runtime_probe(
        pre_rollback_probe,
        bound,
        expected_runtime_files=plan["stages"]["deploy-restart"]["expected_successor_runtime_files"],
        expected_package_versions=KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
        require_frozen_native_path=False,
        expected_python_prefix=plan["isolated_successor_venv"],
    )
    if not all(pre_rollback_gates.values()):
        raise RuntimeError(f"rollback drill successor preflight failed: {pre_rollback_gates}")
    if int(pre_rollback_probe.get("maker_pid", -1)) != int(runtime_evidence.get("maker_pid", -2)):
        raise RuntimeError("rollback drill maker PID differs from deployment receipt")

    stop_output = _run_plain_checked(commands[0], runner)
    quiescence = _run_checked(commands[1], runner)
    if not bool(quiescence.get("controlled_stop_quiescent")):
        raise RuntimeError(f"rollback stop did not become quiescent: {quiescence}")
    rollback = _run_checked(commands[2], runner)
    _validate_strict_rollback_result(
        rollback,
        deployment_evidence=deployment_evidence,
        expected_file_count=len(bound["release_manifest"]["files"]),
    )
    ensure_running_output = _run_plain_checked(commands[3], runner)
    immediate_probe = _run_checked(commands[4], runner)
    immediate_runtime_gates = _validate_predecessor_runtime_after_rollback(
        probe=immediate_probe,
        plan=plan,
        bound=bound,
    )
    expected_pid = int(immediate_probe.get("maker_pid", -1))
    stability_observation = _run_checked(
        _rollback_startup_stability_probe_command(
            plan=plan,
            expected_pid=expected_pid,
        ),
        runner,
    )
    stability_gates = _validate_rollback_startup_stability(
        observation=stability_observation,
        plan=plan,
        expected_pid=expected_pid,
    )
    stable_probe = _run_checked(commands[4], runner)
    stable_runtime_gates = _validate_predecessor_runtime_after_rollback(
        probe=stable_probe,
        plan=plan,
        bound=bound,
    )
    if int(stable_probe.get("maker_pid", -1)) != expected_pid:
        raise RuntimeError(
            "rollback predecessor PID changed after the frozen startup stability window"
        )
    if stable_probe.get("runtime_files") != immediate_probe.get("runtime_files"):
        raise RuntimeError(
            "rollback predecessor runtime hashes changed during the startup stability window"
        )
    return {
        "pre_rollback_successor_probe": pre_rollback_probe,
        "pre_rollback_successor_gates": pre_rollback_gates,
        "controlled_stop_output": stop_output,
        "controlled_stop_quiescence": quiescence,
        "rollback": rollback,
        "ensure_running_output": ensure_running_output,
        "immediate_restored_runtime_probe": immediate_probe,
        "immediate_restored_runtime_identity_gates": immediate_runtime_gates,
        "startup_stability_observation": stability_observation,
        "startup_stability_gates": stability_gates,
        "stable_restored_runtime_probe": stable_probe,
        "stable_restored_runtime_identity_gates": stable_runtime_gates,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "stage",
        nargs="?",
        default="plan",
        choices=(
            "plan",
            "rebuild-validate",
            "runtime-evidence",
            "stage-validate",
            "deploy-restart",
            "performance",
            "admit",
            "rollback-drill",
        ),
    )
    parser.add_argument("--release-manifest", type=Path, default=DEFAULT_RELEASE_MANIFEST)
    parser.add_argument("--remote-v9-identity", type=Path, default=DEFAULT_REMOTE_IDENTITY)
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    parser.add_argument("--remote-python", default=DEFAULT_REMOTE_PYTHON)
    parser.add_argument("--isolated-release-root", default=DEFAULT_ISOLATED_RELEASE_ROOT)
    parser.add_argument("--deployment-instance-id")
    parser.add_argument(
        "--rollback-stability-window-s",
        type=float,
        default=DEFAULT_ROLLBACK_STABILITY_WINDOW_S,
    )
    parser.add_argument("--successor-requirements-lock", type=Path)
    parser.add_argument("--successor-wheelhouse", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--epoch-output", type=Path)
    parser.add_argument("--execute-read-only-ssh", action="store_true")
    parser.add_argument("--execute-local-rebuild", action="store_true")
    parser.add_argument("--rebuild-output-root", type=Path)
    parser.add_argument("--execute-isolated-stage", action="store_true")
    parser.add_argument("--execute-production-mutation", action="store_true")
    parser.add_argument("--execute-admission", action="store_true")
    parser.add_argument("--owner-confirmation-token")
    parser.add_argument("--evidence", action="append", type=Path, default=[])
    parser.add_argument("--admission-root", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    bound = load_bound_release(args.release_manifest, args.remote_v9_identity)
    plan = build_plan(
        bound=bound,
        remote=args.remote,
        remote_root=args.remote_root,
        remote_python=args.remote_python,
        isolated_release_root=args.isolated_release_root,
        successor_requirements_lock=args.successor_requirements_lock,
        successor_wheelhouse=args.successor_wheelhouse,
        deployment_instance_id=args.deployment_instance_id,
        rollback_stability_window_s=args.rollback_stability_window_s,
    )

    if args.stage == "plan":
        _write_optional(args.output, plan)
        print(json.dumps(plan, sort_keys=True, indent=2))
        return 0
    if args.stage == "rebuild-validate":
        if not args.execute_local_rebuild:
            raise PermissionError("rebuild-validate requires --execute-local-rebuild")
        if args.rebuild_output_root is None:
            raise ValueError("rebuild-validate requires --rebuild-output-root")
        manifest, validation = build_release(
            repo_root=Path(__file__).resolve().parents[1],
            output_root=args.rebuild_output_root,
        )
        receipt = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "stage": "rebuild-validate",
            "rebuilt_release_root": str(args.rebuild_output_root.resolve()),
            "rebuilt_manifest_sha256": manifest["manifest_sha256"],
            "validation": validation,
            "deployment_authorized": False,
            "next_step": "rerun plan with rebuilt release_manifest.json as exact input",
        }
        _write_optional(args.output, receipt)
        print(json.dumps(receipt, sort_keys=True, indent=2))
        return 0
    if args.stage == "runtime-evidence":
        if not args.execute_read_only_ssh:
            raise PermissionError("runtime-evidence requires --execute-read-only-ssh")
        receipt = execute_read_only_runtime_probe(plan=plan, bound=bound)
        _write_optional(args.output, receipt)
        print(json.dumps(receipt, sort_keys=True, indent=2))
        return 0
    if args.stage == "stage-validate":
        if not args.execute_isolated_stage:
            raise PermissionError("stage-validate requires --execute-isolated-stage")
        if not plan["stages"]["stage-validate"]["successor_venv_ready_to_build"]:
            raise ValueError(
                "stage-validate requires --successor-requirements-lock and "
                "--successor-wheelhouse; active .venv-active mutation is forbidden"
            )
        predecessor = _run_checked(plan["stages"]["stage-validate"]["commands"][0], _default_runner)
        rsync_command = plan["stages"]["stage-validate"]["commands"][1]
        completed = _default_runner(rsync_command)
        if completed.returncode != 0:
            raise RuntimeError(f"isolated rsync failed: {completed.stderr}")
        for command in plan["stages"]["stage-validate"]["commands"][2:-1]:
            completed = _default_runner(command)
            if completed.returncode != 0:
                raise RuntimeError(
                    "isolated successor preparation or test failed: "
                    f"command={shlex.join(command)}\n"
                    f"stdout={completed.stdout}\n"
                    f"stderr={completed.stderr}"
                )
        smoke = _run_checked(plan["stages"]["stage-validate"]["commands"][-1], _default_runner)
        receipt = _seal_receipt(
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "stage": "staging",
                "release_manifest_sha256": bound["release_manifest_sha256"],
                "remote_identity_sha256": bound["remote_identity_sha256"],
                **_deployment_binding_fields(plan=plan, bound=bound),
                "evidence": {
                    **predecessor,
                    **smoke,
                    "targeted_lifecycle_tests_passed_on_remote_venv": True,
                    "successor_package_set_matches_active_except_pyarrow": True,
                    "successor_native_extension_sha256_bound": True,
                    "successor_dependency_delta": {
                        "pyarrow": {
                            "before": KNOWN_ACTIVE_PACKAGE_VERSIONS["pyarrow"],
                            "after": KNOWN_SUCCESSOR_PACKAGE_VERSIONS["pyarrow"],
                        }
                    },
                    "maker_thread_filesystem_calls_zero": True,
                    "source_payload_current": bound["source_payload_current"],
                    "staging_deploy_eligible": bound["source_payload_current"],
                    "targeted_stage_test_identities": plan["targeted_stage_test_identities"],
                },
                "production_tree_mutated": False,
            }
        )
        _write_optional(args.output, receipt)
        print(json.dumps(receipt, sort_keys=True, indent=2))
        return 0
    if args.stage in PRODUCTION_MUTATING_STAGES:
        if not bound["source_payload_current"]:
            raise PermissionError(
                "source payload is stale; rebuild and reselect the exact manifest before deploy"
            )
        _require_owner_mutation(
            stage=args.stage,
            bound=bound,
            mutation_plan_identity_sha256=plan["mutation_plan_identity_sha256"],
            execute=args.execute_production_mutation,
            token=args.owner_confirmation_token,
        )
        if args.stage == "rollback-drill":
            _require_rollback_predecessor_startup_contract(
                plan=plan,
                bound=bound,
            )
        if args.stage == "deploy-restart":
            staging_receipt = _required_receipt(args.evidence, stage="staging", bound=bound)
            staging_evidence = staging_receipt.get("evidence", {})
            if not isinstance(staging_evidence, dict):
                raise ValueError("staging evidence body is invalid")
            if staging_receipt.get("mutation_plan_identity_sha256") != plan.get(
                "mutation_plan_identity_sha256"
            ):
                raise PermissionError("staging receipt mutation plan mismatch")
            if staging_receipt.get("deployment_binding") is not None:
                _validate_embedded_deployment_binding(
                    staging_receipt,
                    expected=_deployment_binding_payload(plan=plan, bound=bound),
                )
            if staging_evidence.get("targeted_stage_test_identities") != plan.get(
                "targeted_stage_test_identities"
            ):
                raise PermissionError("staging test identities changed")
            required_staging = (
                "predecessor_revalidation_passed",
                "staged_overlay_import_smoke_passed",
                "targeted_lifecycle_tests_passed_on_remote_venv",
                "successor_package_set_matches_active_except_pyarrow",
                "successor_native_extension_sha256_bound",
                "source_payload_current",
                "staging_deploy_eligible",
            )
            if not all(bool(staging_evidence.get(name)) for name in required_staging):
                raise PermissionError("staging evidence is not deploy eligible")
            if (
                staging_evidence.get("remote_canonical_manifest_sha256")
                != bound["release_manifest_sha256"]
            ):
                raise PermissionError("staging canonical manifest hash mismatch")
            if staging_evidence.get("remote_manifest_file_sha256") != _sha256(
                bound["release_manifest_path"]
            ):
                raise PermissionError("staging manifest file hash mismatch")
            transaction = execute_deploy_restart_transaction(
                plan=plan,
                bound=bound,
            )
            deployment = transaction["deployment"]
            probe = transaction["probe"]
            runtime_gates = transaction["runtime_gates"]
            receipt = _seal_receipt(
                {
                    "schema_version": EVIDENCE_SCHEMA_VERSION,
                    "stage": "runtime",
                    "release_manifest_sha256": bound["release_manifest_sha256"],
                    "remote_identity_sha256": bound["remote_identity_sha256"],
                    **_deployment_binding_fields(plan=plan, bound=bound),
                    "parent_staging_receipt_identity_sha256": staging_receipt[
                        "receipt_identity_sha256"
                    ],
                    "evidence": {
                        **probe,
                        **runtime_gates,
                        "native_extensions_hash_valid": True,
                        "deployment_files_applied": bool(
                            deployment.get("deployment_files_applied")
                        ),
                        "deployment": deployment,
                        "controlled_stop_quiescence": transaction["quiescence"],
                        "automatic_rollback_performed": False,
                    },
                    "production_mutation_performed": True,
                    "owner_confirmation_token_sha256": hashlib.sha256(
                        str(args.owner_confirmation_token).encode()
                    ).hexdigest(),
                }
            )
            _write_optional(args.output, receipt)
            print(json.dumps(receipt, sort_keys=True, indent=2))
            return 0

        runtime_receipt = _required_receipt(args.evidence, stage="runtime", bound=bound)
        if not bool(runtime_receipt.get("evidence", {}).get("deployment_files_applied")):
            raise PermissionError("rollback drill requires the exact deployment receipt")
        runtime_binding = normalize_runtime_receipt_for_plan(
            runtime_receipt,
            plan=plan,
            bound=bound,
        )
        runtime_evidence = runtime_receipt.get("evidence", {})
        if not isinstance(runtime_evidence, Mapping):
            raise ValueError("runtime receipt evidence is invalid")
        deployment_evidence = runtime_evidence.get("deployment")
        if not isinstance(deployment_evidence, Mapping):
            raise ValueError("runtime receipt lacks exact deployment evidence")
        transaction = execute_rollback_drill_transaction(
            plan=plan,
            bound=bound,
            runtime_evidence=runtime_evidence,
            deployment_evidence=deployment_evidence,
        )
        rollback = transaction["rollback"]
        receipt = _seal_receipt(
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "stage": "rollback",
                "release_manifest_sha256": bound["release_manifest_sha256"],
                "remote_identity_sha256": bound["remote_identity_sha256"],
                **_deployment_binding_fields(plan=plan, bound=bound),
                "parent_runtime_receipt_identity_sha256": runtime_receipt[
                    "receipt_identity_sha256"
                ],
                "runtime_receipt_normalization": runtime_binding,
                "evidence": {
                    **rollback,
                    "rollback_restart_rehearsed": True,
                    "pre_rollback_successor_probe": transaction["pre_rollback_successor_probe"],
                    "pre_rollback_successor_gates": transaction["pre_rollback_successor_gates"],
                    "controlled_stop_quiescence": transaction["controlled_stop_quiescence"],
                    "immediate_restored_runtime_probe": transaction[
                        "immediate_restored_runtime_probe"
                    ],
                    "immediate_restored_runtime_identity_gates": transaction[
                        "immediate_restored_runtime_identity_gates"
                    ],
                    "startup_stability_observation": transaction["startup_stability_observation"],
                    "startup_stability_gates": transaction["startup_stability_gates"],
                    "restored_runtime_probe": transaction["stable_restored_runtime_probe"],
                    "restored_runtime_identity_gates": transaction[
                        "stable_restored_runtime_identity_gates"
                    ],
                },
                "production_mutation_performed": True,
            }
        )
        _write_optional(args.output, receipt)
        print(json.dumps(receipt, sort_keys=True, indent=2))
        return 0
    if args.stage == "performance":
        if not args.execute_read_only_ssh:
            raise PermissionError("performance requires --execute-read-only-ssh")
        if args.output is None or args.epoch_output is None:
            raise ValueError("performance requires --output and --epoch-output")
        runtime_receipt = _required_receipt(args.evidence, stage="runtime", bound=bound)
        if not bool(runtime_receipt.get("evidence", {}).get("deployment_files_applied")):
            raise PermissionError("performance requires the exact deployment receipt")
        runtime_binding = normalize_runtime_receipt_for_plan(
            runtime_receipt,
            plan=plan,
            bound=bound,
        )
        candidate = _run_checked(plan["stages"]["performance"]["commands"][0], _default_runner)
        runtime_evidence = runtime_receipt.get("evidence", {})
        if not isinstance(runtime_evidence, Mapping):
            raise ValueError("runtime receipt evidence is invalid")
        if int(candidate.get("maker_pid", -1)) != int(runtime_evidence.get("maker_pid", -2)):
            raise RuntimeError("performance maker PID differs from the deployment receipt")
        exact_part = candidate.get("exact_standalone_part")
        if not isinstance(exact_part, Mapping):
            raise RuntimeError("performance did not freeze an exact standalone journal part")
        if exact_part.get("session_root") != candidate.get("session_root"):
            raise RuntimeError("performance part and session roots differ")
        if exact_part.get("runtime_identity_sha256") != candidate.get("runtime_identity_sha256"):
            raise RuntimeError("performance part and runtime identities differ")
        epoch_evidence = {
            "initial_state_domains": candidate.pop("initial_state_domains", []),
            "initial_state_domain_complete": candidate.pop("initial_state_domain_complete", {}),
            "pre_epoch_native_events": candidate.pop("pre_epoch_native_events", -1),
            "epoch_binding_status": candidate.pop("epoch_binding_status", ""),
            "baseline_epoch_id": candidate.get("baseline_epoch_id"),
            "baseline_epoch_identity_sha256": candidate.get("baseline_epoch_identity_sha256"),
            "epoch_root": candidate.get("epoch_root"),
            "epoch_manifest_file_sha256": candidate.get("epoch_manifest_file_sha256"),
            "initial_runtime_state_file_sha256": candidate.get("initial_runtime_state_file_sha256"),
            "runtime_identity_sha256": candidate.get("runtime_identity_sha256"),
            "maker_pid": candidate.get("maker_pid"),
            "session_root": candidate.get("session_root"),
            "session_id": candidate.get("session_id"),
        }
        performance_receipt = _seal_receipt(
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "stage": "performance",
                "release_manifest_sha256": bound["release_manifest_sha256"],
                "remote_identity_sha256": bound["remote_identity_sha256"],
                **_deployment_binding_fields(plan=plan, bound=bound),
                "parent_runtime_receipt_identity_sha256": runtime_receipt[
                    "receipt_identity_sha256"
                ],
                "runtime_receipt_normalization": runtime_binding,
                "evidence": {
                    "baseline": dict(BASELINE_QUOTE_LOOP_TELEMETRY),
                    "baseline_process_resource": dict(BASELINE_PROCESS_RESOURCE),
                    "candidate": candidate,
                },
                "economic_outcomes_read": False,
            }
        )
        epoch_receipt = _seal_receipt(
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "stage": "epoch",
                "release_manifest_sha256": bound["release_manifest_sha256"],
                "remote_identity_sha256": bound["remote_identity_sha256"],
                **_deployment_binding_fields(plan=plan, bound=bound),
                "parent_runtime_receipt_identity_sha256": runtime_receipt[
                    "receipt_identity_sha256"
                ],
                "evidence": epoch_evidence,
                "economic_outcomes_read": False,
            }
        )
        _write_optional(args.output, performance_receipt)
        _write_optional(args.epoch_output, epoch_receipt)
        print(
            json.dumps(
                {"performance": performance_receipt, "epoch": epoch_receipt},
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.stage == "admit":
        if not args.execute_admission:
            raise PermissionError("admit requires --execute-admission")
        if args.admission_root is None:
            raise ValueError("admit requires --admission-root")
        output = admit_evidence_atomically(
            bound=bound,
            evidence_paths=args.evidence,
            admission_root=args.admission_root,
        )
        print(str(output))
        return 0
    raise AssertionError(f"unhandled stage: {args.stage}")


if __name__ == "__main__":
    raise SystemExit(main())
