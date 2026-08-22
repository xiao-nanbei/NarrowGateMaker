"""External deployment-gate amendment for the frozen BUY E3 runtime.

This module does not change the artifact-bound runtime or the v1 deployment
gate.  It closes the operational identities that v1 cannot observe: the Git
tag object, the actual live PID/config/cwd, a post-start log checkpoint, and a
concurrent live-plus-benchmark resource window.  It reads no research outcome
and never scores a hypothetical live action.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import time
from base64 import b64decode, urlsafe_b64encode
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 import (
    REGRESSION_SCHEMA as SUPERSEDED_REGRESSION_SCHEMA_V1,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 import (
    RUNTIME_REGRESSION_SOURCES,
    RUNTIME_REGRESSION_TESTS,
)
from strategy.boolean_cooldown_buy_e3 import OWNER_IDENTITY, LiveBuyE3CooldownPolicy

SCHEMA_VERSION = f"{OWNER_IDENTITY}.deployment_gate_amendment.v2"
PROCESS_IDENTITY_SCHEMA = f"{OWNER_IDENTITY}.actual_process_identity.v2"
RESOURCE_WINDOW_SCHEMA = f"{OWNER_IDENTITY}.concurrent_resource_window.v2"
STARTUP_LOG_SCHEMA = f"{OWNER_IDENTITY}.startup_log_checkpoint.v2"
RUNTIME_REGRESSION_SCHEMA = f"{OWNER_IDENTITY}.runtime_regression_test_receipt.v2"
FROZEN_EXECUTION_COMMIT = "c170493ea5838b6e3a715006db352c0a484d3943"
FROZEN_EXECUTION_TREE = "52fe1cde0e0c789acb9e4b0dbac95572ca61d483"
FROZEN_EXECUTION_TAG = "f05-owner-buy-e3-live-attempt2-20260821"
FROZEN_EXECUTION_TAG_OBJECT = "cda11b7700e3fec21464401a391133f129be74c1"
EXPECTED_RUNTIME_REGRESSION_PASSED = 67

MIB = 1024 * 1024
MIN_MEM_AVAILABLE_MIB = 512.0
MAX_LIVE_RSS_MIB = 512.0
MAX_BENCHMARK_RSS_MIB = 256.0
MAX_COMBINED_RSS_MIB = 768.0
MIN_RATE_MULTIPLIER = 2.0
MAX_CALLBACK_P99_US = 2_000.0
MAX_DECISION_P99_US = 10_000.0

REQUIRED_RUNTIME_PATHS = {
    "live_buy_runtime": "strategy/boolean_cooldown_buy_e3.py",
    "maker_engine": "strategy/maker_engine.py",
    "live_config": "live/config.py",
    "live_runtime_policy": "live/runtime_policy.py",
    "live_main": "live/main.py",
}
REQUIRED_ZERO_COUNTERS = (
    "marketTapeDropped",
    "marketTapeInvalid",
    "externalRecordDropped",
    "globalFlowTradeOverflow",
    "globalFlowBookOverflow",
    "booleanCooldownInvalid",
    "buyE3CooldownInvalid",
)
FATAL_STARTUP_PATTERNS = (
    "traceback (most recent call last)",
    "sigsegv",
    "exc_bad_access",
    "out of memory",
    "oom-kill",
    "artifact_file_hash_drift",
    "identity_drift",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False):
    output: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in output:
            raise BuyE3DeploymentGateAmendmentError(f"duplicate YAML key: {key}")
        output[key] = loader.construct_object(value_node, deep=deep)
    return output


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


class BuyE3DeploymentGateAmendmentError(RuntimeError):
    """Raised when external deployment evidence cannot be proven exactly."""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise BuyE3DeploymentGateAmendmentError(f"{label} is not a SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise BuyE3DeploymentGateAmendmentError(f"{label} is not a Git object id")
    return normalized


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise BuyE3DeploymentGateAmendmentError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def read_json(path: Path) -> dict[str, Any]:
    candidate = path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3DeploymentGateAmendmentError(f"JSON input is not a regular file: {path}")
    target = candidate.resolve(strict=True)
    try:
        payload = json.loads(
            target.read_text(encoding="ascii"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                BuyE3DeploymentGateAmendmentError(f"non-finite JSON value: {value}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BuyE3DeploymentGateAmendmentError(f"unreadable JSON: {path.name}") from exc
    if not isinstance(payload, dict):
        raise BuyE3DeploymentGateAmendmentError(f"JSON root is not an object: {path.name}")
    return payload


def atomic_write_receipt(path: Path, payload: Mapping[str, Any]) -> str:
    target = path.expanduser().absolute()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.parent.is_symlink():
        raise BuyE3DeploymentGateAmendmentError("receipt parent cannot be a symlink")
    if target.exists() or target.is_symlink():
        raise BuyE3DeploymentGateAmendmentError(f"immutable receipt already exists: {target.name}")
    temporary = target.with_name(f".{target.name}.tmp.{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            json.dump(dict(payload), handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()
    if stat.S_IMODE(target.stat().st_mode) != 0o600:
        raise BuyE3DeploymentGateAmendmentError("receipt permission drifted from 0600")
    return file_sha256(target)


def _run_git(repository_root: Path, *arguments: str, binary: bool = False) -> str | bytes:
    completed = subprocess.run(
        ("git", *arguments),
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=not binary,
    )
    if binary:
        return completed.stdout
    return str(completed.stdout).strip()


def verify_execution_git_identity(
    *,
    repository_root: Path,
    expected_commit: str,
    expected_tree: str,
    annotated_tag: str,
    expected_tag_object: str,
) -> dict[str, Any]:
    """Bind a historical execution commit without requiring it to be HEAD."""

    root = repository_root.expanduser().resolve(strict=True)
    commit = _require_git_sha(expected_commit, "execution commit")
    if commit != FROZEN_EXECUTION_COMMIT:
        raise BuyE3DeploymentGateAmendmentError("execution commit is not frozen c170493e")
    tree = _require_git_sha(expected_tree, "execution tree")
    tag_object = _require_git_sha(expected_tag_object, "annotated tag object")
    tag = str(annotated_tag).strip()
    if not tag or any(char.isspace() for char in tag):
        raise BuyE3DeploymentGateAmendmentError("annotated tag name is invalid")
    observed_tag_object = str(_run_git(root, "rev-parse", f"refs/tags/{tag}"))
    observed_type = str(_run_git(root, "cat-file", "-t", f"refs/tags/{tag}"))
    peeled_commit = str(_run_git(root, "rev-parse", f"refs/tags/{tag}^{{}}"))
    observed_tree = str(_run_git(root, "rev-parse", f"{commit}^{{tree}}"))
    if observed_type != "tag":
        raise BuyE3DeploymentGateAmendmentError("execution tag is not annotated")
    if observed_tag_object != tag_object or peeled_commit != commit or observed_tree != tree:
        raise BuyE3DeploymentGateAmendmentError("execution Git identity drifted")
    return {
        "execution_commit": commit,
        "execution_tree": tree,
        "annotated_tag": tag,
        "annotated_tag_object": tag_object,
        "tag_peeled_commit": peeled_commit,
    }


def verify_runtime_sources(
    *,
    repository_root: Path,
    execution_commit: str,
    artifact_manifest: Mapping[str, Any],
    runtime_paths: Mapping[str, str] = REQUIRED_RUNTIME_PATHS,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    commit = _require_git_sha(execution_commit, "execution commit")
    expected = artifact_manifest.get("implementation_sha256")
    if not isinstance(expected, Mapping):
        raise BuyE3DeploymentGateAmendmentError("artifact manifest lacks runtime hashes")
    bindings: dict[str, Any] = {}
    for role, relative in runtime_paths.items():
        expected_sha = _require_sha256(expected.get(role), f"runtime hash {role}")
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise BuyE3DeploymentGateAmendmentError(f"runtime source unavailable: {relative}")
        working_sha = file_sha256(path)
        blob = _run_git(root, "show", f"{commit}:{relative}", binary=True)
        blob_sha = hashlib.sha256(bytes(blob)).hexdigest()
        if working_sha != expected_sha or blob_sha != expected_sha:
            raise BuyE3DeploymentGateAmendmentError(f"runtime source hash drifted: {role}")
        bindings[role] = {
            "repository_relative_path": relative,
            "artifact_manifest_sha256": expected_sha,
            "execution_commit_blob_sha256": blob_sha,
            "working_file_sha256": working_sha,
        }
    return {
        "files": bindings,
        "runtime_code_sha256": canonical_sha256(bindings),
    }


def _bind_execution_files(
    *,
    repository_root: Path,
    execution_commit: str,
    relative_paths: Sequence[str],
    label: str,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    commit = _require_git_sha(execution_commit, "execution commit")
    if len(set(relative_paths)) != len(relative_paths):
        raise BuyE3DeploymentGateAmendmentError(f"duplicate {label} path")
    bindings: dict[str, Any] = {}
    for raw_path in relative_paths:
        relative = Path(str(raw_path))
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise BuyE3DeploymentGateAmendmentError(f"unsafe {label} path: {raw_path}")
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise BuyE3DeploymentGateAmendmentError(f"{label} unavailable: {raw_path}")
        try:
            path.resolve(strict=True).relative_to(root)
        except ValueError as exc:
            raise BuyE3DeploymentGateAmendmentError(
                f"{label} parent path escaped: {raw_path}"
            ) from exc
        working_sha = file_sha256(path)
        blob = _run_git(root, "show", f"{commit}:{relative.as_posix()}", binary=True)
        blob_sha = hashlib.sha256(bytes(blob)).hexdigest()
        if working_sha != blob_sha:
            raise BuyE3DeploymentGateAmendmentError(
                f"{label} differs from frozen execution: {raw_path}"
            )
        bindings[relative.as_posix()] = {
            "working_file_sha256": working_sha,
            "execution_commit_blob_sha256": blob_sha,
        }
    return bindings


def _symlink_chain(path: Path) -> list[dict[str, Any]]:
    chain: list[dict[str, Any]] = []
    current = path
    seen: set[Path] = set()
    for _ in range(32):
        if current in seen:
            raise BuyE3DeploymentGateAmendmentError("Python executable symlink loop detected")
        seen.add(current)
        if not current.is_symlink():
            return chain
        raw_target = os.readlink(current)
        next_path = Path(raw_target)
        if not next_path.is_absolute():
            next_path = current.parent / next_path
        next_path = next_path.absolute()
        chain.append(
            {
                "link_path": str(current),
                "link_target": raw_target,
                "next_path": str(next_path),
            }
        )
        current = next_path
    raise BuyE3DeploymentGateAmendmentError("Python executable symlink chain is too deep")


def bind_lexical_venv_python(
    *,
    python_executable: Path,
    venv_root: Path,
    expected_resolved_target: Path,
    expected_resolved_target_sha256: str,
) -> dict[str, Any]:
    """Bind the venv entrypoint without replacing it with its physical target."""

    lexical_venv = venv_root.expanduser().absolute()
    if lexical_venv.is_symlink() or not lexical_venv.is_dir():
        raise BuyE3DeploymentGateAmendmentError("bound venv root is not a real directory")
    lexical_python = python_executable.expanduser().absolute()
    try:
        relative = lexical_python.relative_to(lexical_venv)
    except ValueError as exc:
        raise BuyE3DeploymentGateAmendmentError(
            "lexical Python executable is outside the bound venv"
        ) from exc
    if not relative.parts:
        raise BuyE3DeploymentGateAmendmentError("lexical Python executable is the venv root")
    parent = lexical_venv
    for component in relative.parts[:-1]:
        parent /= component
        if parent.is_symlink() or not parent.is_dir():
            raise BuyE3DeploymentGateAmendmentError(
                "lexical Python executable has an unsafe parent path"
            )
    if not lexical_python.exists() or not lexical_python.is_file():
        raise BuyE3DeploymentGateAmendmentError("lexical Python executable is unavailable")
    if not os.access(lexical_python, os.X_OK):
        raise BuyE3DeploymentGateAmendmentError("lexical Python executable is not executable")
    observed_target = lexical_python.resolve(strict=True)
    expected_target_path = expected_resolved_target.expanduser().absolute()
    if expected_target_path.is_symlink() or not expected_target_path.is_file():
        raise BuyE3DeploymentGateAmendmentError(
            "expected Python target is not a physical executable"
        )
    expected_target = expected_target_path.resolve(strict=True)
    if observed_target != expected_target:
        raise BuyE3DeploymentGateAmendmentError("Python executable symlink target drifted")
    expected_target_sha = _require_sha256(
        expected_resolved_target_sha256, "expected Python target hash"
    )
    observed_target_sha = file_sha256(observed_target)
    if observed_target_sha != expected_target_sha:
        raise BuyE3DeploymentGateAmendmentError("Python executable target bytes drifted")
    chain = _symlink_chain(lexical_python)
    binding: dict[str, Any] = {
        "lexical_executable_path": str(lexical_python),
        "lexical_venv_root": str(lexical_venv),
        "lexical_path_preserved": True,
        "lexical_path_under_bound_venv": True,
        "lexical_entrypoint_is_symlink": lexical_python.is_symlink(),
        "symlink_chain": chain,
        "symlink_chain_sha256": canonical_sha256(chain),
        "resolved_target_path": str(observed_target),
        "resolved_target_sha256": observed_target_sha,
        "expected_target_path": str(expected_target),
        "expected_target_sha256": expected_target_sha,
        "resolved_target_matches_frozen_identity": True,
        "resolved_target_outside_venv": lexical_venv not in observed_target.parents,
    }
    binding["canonical_python_binding_sha256"] = document_sha256(
        binding, "canonical_python_binding_sha256"
    )
    return binding


def _bind_superseded_v1_regression_receipt(
    *,
    receipt_path: Path | None,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
    expected_execution_tag: str,
) -> dict[str, Any]:
    if receipt_path is None:
        return {"present": False}
    path = receipt_path.expanduser()
    if path.is_symlink() or not path.is_file():
        raise BuyE3DeploymentGateAmendmentError("v1 failed-attempt receipt is unavailable")
    path = path.resolve(strict=True)
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise BuyE3DeploymentGateAmendmentError("v1 failed-attempt receipt is not mode 0600")
    receipt = read_json(path)
    return_code = receipt.get("return_code")
    if (
        receipt.get("schema_version") != SUPERSEDED_REGRESSION_SCHEMA_V1
        or receipt.get("identity") != OWNER_IDENTITY
        or receipt.get("status") != "failed"
        or receipt.get("artifact_sha256") != expected_artifact_sha256
        or receipt.get("execution_commit") != expected_execution_commit
        or receipt.get("execution_tag") != expected_execution_tag
        or receipt.get("passed") != 0
        or not isinstance(return_code, int)
        or return_code == 0
        or receipt.get("canonical_receipt_sha256")
        != document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise BuyE3DeploymentGateAmendmentError("v1 failed-attempt receipt identity drifted")
    if set(receipt.get("test_files", {})) != set(RUNTIME_REGRESSION_TESTS) or set(
        receipt.get("runtime_sources", {})
    ) != set(RUNTIME_REGRESSION_SOURCES):
        raise BuyE3DeploymentGateAmendmentError("v1 failed-attempt suite identity drifted")
    return {
        "present": True,
        "role": "superseded_failed_attempt_only",
        "supersession_reason": "v1_resolved_lexical_venv_entrypoint_before_execution",
        "path": str(path),
        "file_sha256": file_sha256(path),
        "schema_version": receipt["schema_version"],
        "canonical_receipt_sha256": receipt["canonical_receipt_sha256"],
        "status": receipt["status"],
        "passed": receipt["passed"],
        "failed": receipt.get("failed"),
        "return_code": return_code,
        "eligible_for_gate_satisfaction": False,
    }


def _default_runtime_regression_runner(
    command: Sequence[str], repository_root: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=repository_root,
        capture_output=True,
        text=True,
        check=False,
    )


def _safe_process_output(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return ""


def _pytest_counts(stdout: str, stderr: str) -> dict[str, int]:
    combined = f"{stdout}\n{stderr}"

    def final_count(pattern: str) -> int:
        matches = re.findall(pattern, combined, flags=re.IGNORECASE)
        return int(matches[-1]) if matches else 0

    return {
        "passed": final_count(r"\b(\d+)\s+passed\b"),
        "failed": final_count(r"\b(\d+)\s+failed\b"),
        "errors": final_count(r"\b(\d+)\s+errors?\b"),
    }


def run_runtime_regression_tests_v2(
    *,
    repository_root: Path,
    expected_artifact_sha256: str,
    output_path: Path,
    python_executable: Path,
    venv_root: Path,
    expected_python_target: Path,
    expected_python_target_sha256: str,
    expected_execution_commit: str = FROZEN_EXECUTION_COMMIT,
    expected_execution_tree: str = FROZEN_EXECUTION_TREE,
    expected_execution_tag: str = FROZEN_EXECUTION_TAG,
    expected_tag_object: str = FROZEN_EXECUTION_TAG_OBJECT,
    superseded_v1_failed_receipt_path: Path | None = None,
    process_runner: Callable[
        [Sequence[str], Path], subprocess.CompletedProcess[str]
    ] = _default_runtime_regression_runner,
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    """Run the frozen suite through the lexical venv entrypoint and preserve evidence."""

    root = repository_root.expanduser().resolve(strict=True)
    artifact_sha = _require_sha256(expected_artifact_sha256, "artifact hash")
    execution = verify_execution_git_identity(
        repository_root=root,
        expected_commit=expected_execution_commit,
        expected_tree=expected_execution_tree,
        annotated_tag=expected_execution_tag,
        expected_tag_object=expected_tag_object,
    )
    python_binding = bind_lexical_venv_python(
        python_executable=python_executable,
        venv_root=venv_root,
        expected_resolved_target=expected_python_target,
        expected_resolved_target_sha256=expected_python_target_sha256,
    )
    test_files = _bind_execution_files(
        repository_root=root,
        execution_commit=execution["execution_commit"],
        relative_paths=RUNTIME_REGRESSION_TESTS,
        label="runtime regression test",
    )
    runtime_sources = _bind_execution_files(
        repository_root=root,
        execution_commit=execution["execution_commit"],
        relative_paths=RUNTIME_REGRESSION_SOURCES,
        label="runtime regression source",
    )
    superseded_v1 = _bind_superseded_v1_regression_receipt(
        receipt_path=superseded_v1_failed_receipt_path,
        expected_artifact_sha256=artifact_sha,
        expected_execution_commit=execution["execution_commit"],
        expected_execution_tag=execution["annotated_tag"],
    )
    lexical_python = python_binding["lexical_executable_path"]
    command = (str(lexical_python), "-m", "pytest", "-q", *RUNTIME_REGRESSION_TESTS)
    stdout = ""
    stderr = ""
    return_code: int | None = None
    failure_reason: str | None = None
    try:
        completed = process_runner(command, root)
        return_code = int(completed.returncode)
        stdout = _safe_process_output(completed.stdout)
        stderr = _safe_process_output(completed.stderr)
    except Exception as exc:
        failure_reason = f"process_launch_failed:{type(exc).__name__}"
    counts = _pytest_counts(stdout, stderr)
    if failure_reason is None:
        if return_code != 0 and counts["passed"] == 0:
            failure_reason = f"pytest_returncode_{return_code}_no_tests_completed"
        elif return_code != 0:
            failure_reason = f"pytest_returncode_{return_code}"
        elif counts["failed"] or counts["errors"]:
            failure_reason = "pytest_reported_failures_or_errors"
        elif counts["passed"] != EXPECTED_RUNTIME_REGRESSION_PASSED:
            failure_reason = "pytest_pass_count_mismatch"
    status = "passed" if failure_reason is None else "failed"
    receipt: dict[str, Any] = {
        "schema_version": RUNTIME_REGRESSION_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": status,
        "generated_utc": _utc_now(),
        "artifact_sha256": artifact_sha,
        "execution_identity": execution,
        "python_identity": python_binding,
        "command": {
            "argv0": command[0],
            "argv0_is_lexical_venv_entrypoint": command[0] == lexical_python,
            "arguments_sha256": canonical_sha256(list(command)),
            "pytest_module_invocation": True,
            "test_paths": list(RUNTIME_REGRESSION_TESTS),
        },
        "expected_passed": EXPECTED_RUNTIME_REGRESSION_PASSED,
        "passed": counts["passed"],
        "failed": counts["failed"],
        "errors": counts["errors"],
        "return_code": return_code,
        "failure_reason": failure_reason,
        "stdout": {
            "sha256": hashlib.sha256(stdout.encode("utf-8")).hexdigest(),
            "size_bytes": len(stdout.encode("utf-8")),
            "content_stored": False,
        },
        "stderr": {
            "sha256": hashlib.sha256(stderr.encode("utf-8")).hexdigest(),
            "size_bytes": len(stderr.encode("utf-8")),
            "content_stored": False,
        },
        "test_files": test_files,
        "test_suite_sha256": canonical_sha256(test_files),
        "runtime_sources": runtime_sources,
        "runtime_sources_sha256": canonical_sha256(runtime_sources),
        "superseded_v1_failed_attempt": superseded_v1,
        "coverage": {
            "lexical_venv_entrypoint_preserved": True,
            "physical_interpreter_bound_separately": True,
            "buy_disabled_equals_b0": True,
            "restart_and_rollback": True,
            "sell_integration_unchanged": True,
        },
        "evidence_boundary": {
            "economic_values_read": False,
            "economic_arms_run": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    receipt["canonical_receipt_sha256"] = document_sha256(receipt, "canonical_receipt_sha256")
    atomic_write_receipt(output_path, receipt)
    if status != "passed" and raise_on_failure:
        raise BuyE3DeploymentGateAmendmentError(
            "runtime regression v2 failed; immutable receipt was preserved"
        )
    return receipt


def validate_runtime_regression_receipt_v2(
    path: Path,
    *,
    expected_artifact_sha256: str,
    require_passed: bool = True,
) -> dict[str, Any]:
    candidate = path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3DeploymentGateAmendmentError("runtime regression receipt is unavailable")
    if stat.S_IMODE(candidate.stat().st_mode) != 0o600:
        raise BuyE3DeploymentGateAmendmentError("runtime regression receipt is not mode 0600")
    receipt = read_json(candidate)
    execution = receipt.get("execution_identity")
    python_identity = receipt.get("python_identity")
    command = receipt.get("command")
    stdout = receipt.get("stdout")
    stderr = receipt.get("stderr")
    if (
        receipt.get("schema_version") != RUNTIME_REGRESSION_SCHEMA
        or receipt.get("identity") != OWNER_IDENTITY
        or receipt.get("artifact_sha256")
        != _require_sha256(expected_artifact_sha256, "artifact hash")
        or receipt.get("canonical_receipt_sha256")
        != document_sha256(receipt, "canonical_receipt_sha256")
        or not isinstance(execution, Mapping)
        or execution.get("execution_commit") != FROZEN_EXECUTION_COMMIT
        or execution.get("execution_tree") != FROZEN_EXECUTION_TREE
        or execution.get("annotated_tag") != FROZEN_EXECUTION_TAG
        or execution.get("annotated_tag_object") != FROZEN_EXECUTION_TAG_OBJECT
        or execution.get("tag_peeled_commit") != FROZEN_EXECUTION_COMMIT
    ):
        raise BuyE3DeploymentGateAmendmentError("runtime regression receipt identity drifted")
    if not isinstance(python_identity, Mapping) or (
        python_identity.get("lexical_path_preserved") is not True
        or python_identity.get("lexical_path_under_bound_venv") is not True
        or python_identity.get("resolved_target_matches_frozen_identity") is not True
        or python_identity.get("symlink_chain_sha256")
        != canonical_sha256(python_identity.get("symlink_chain"))
        or python_identity.get("canonical_python_binding_sha256")
        != document_sha256(python_identity, "canonical_python_binding_sha256")
        or _SHA256_RE.fullmatch(str(python_identity.get("resolved_target_sha256", ""))) is None
        or python_identity.get("resolved_target_sha256")
        != python_identity.get("expected_target_sha256")
    ):
        raise BuyE3DeploymentGateAmendmentError("runtime regression Python identity drifted")
    lexical_receipt_path = Path(str(python_identity.get("lexical_executable_path", "")))
    lexical_receipt_venv = Path(str(python_identity.get("lexical_venv_root", "")))
    try:
        lexical_receipt_path.relative_to(lexical_receipt_venv)
    except (KeyError, ValueError) as exc:
        raise BuyE3DeploymentGateAmendmentError(
            "runtime regression lexical Python escaped its venv"
        ) from exc
    if (
        not lexical_receipt_path.is_absolute()
        or not lexical_receipt_venv.is_absolute()
        or python_identity.get("resolved_target_path")
        != python_identity.get("expected_target_path")
    ):
        raise BuyE3DeploymentGateAmendmentError(
            "runtime regression lexical/physical Python paths drifted"
        )
    expected_command = [
        str(python_identity["lexical_executable_path"]),
        "-m",
        "pytest",
        "-q",
        *RUNTIME_REGRESSION_TESTS,
    ]
    if not isinstance(command, Mapping) or (
        command.get("argv0") != python_identity.get("lexical_executable_path")
        or command.get("argv0_is_lexical_venv_entrypoint") is not True
        or command.get("pytest_module_invocation") is not True
        or command.get("test_paths") != list(RUNTIME_REGRESSION_TESTS)
        or command.get("arguments_sha256") != canonical_sha256(expected_command)
    ):
        raise BuyE3DeploymentGateAmendmentError("runtime regression command identity drifted")
    for label, stream in (("stdout", stdout), ("stderr", stderr)):
        if not isinstance(stream, Mapping) or (
            _SHA256_RE.fullmatch(str(stream.get("sha256", ""))) is None
            or not isinstance(stream.get("size_bytes"), int)
            or int(stream["size_bytes"]) < 0
            or stream.get("content_stored") is not False
        ):
            raise BuyE3DeploymentGateAmendmentError(f"runtime regression {label} evidence drifted")
    test_files = receipt.get("test_files")
    runtime_sources = receipt.get("runtime_sources")
    if (
        not isinstance(test_files, Mapping)
        or set(test_files) != set(RUNTIME_REGRESSION_TESTS)
        or receipt.get("test_suite_sha256") != canonical_sha256(test_files)
        or not isinstance(runtime_sources, Mapping)
        or set(runtime_sources) != set(RUNTIME_REGRESSION_SOURCES)
        or receipt.get("runtime_sources_sha256") != canonical_sha256(runtime_sources)
    ):
        raise BuyE3DeploymentGateAmendmentError("runtime regression file identity drifted")
    for group in (test_files, runtime_sources):
        for binding in group.values():
            if not isinstance(binding, Mapping) or (
                binding.get("working_file_sha256") != binding.get("execution_commit_blob_sha256")
                or _SHA256_RE.fullmatch(str(binding.get("working_file_sha256", ""))) is None
            ):
                raise BuyE3DeploymentGateAmendmentError(
                    "runtime regression frozen file bytes drifted"
                )
    superseded = receipt.get("superseded_v1_failed_attempt")
    if not isinstance(superseded, Mapping):
        raise BuyE3DeploymentGateAmendmentError("v1 supersession evidence is malformed")
    if (superseded.get("present") is not True and superseded.get("present") is not False) or (
        superseded.get("present") is False and set(superseded) != {"present"}
    ):
        raise BuyE3DeploymentGateAmendmentError("v1 supersession presence drifted")
    if superseded.get("present") is True and (
        superseded.get("role") != "superseded_failed_attempt_only"
        or superseded.get("schema_version") != SUPERSEDED_REGRESSION_SCHEMA_V1
        or superseded.get("status") != "failed"
        or superseded.get("passed") != 0
        or not isinstance(superseded.get("return_code"), int)
        or superseded.get("return_code") == 0
        or superseded.get("eligible_for_gate_satisfaction") is not False
        or _SHA256_RE.fullmatch(str(superseded.get("file_sha256", ""))) is None
        or _SHA256_RE.fullmatch(str(superseded.get("canonical_receipt_sha256", ""))) is None
    ):
        raise BuyE3DeploymentGateAmendmentError("v1 supersession evidence drifted")
    boundary = receipt.get("evidence_boundary")
    if not isinstance(boundary, Mapping) or any(boundary.values()):
        raise BuyE3DeploymentGateAmendmentError("runtime regression evidence boundary drifted")
    passed_exactly = (
        receipt.get("status") == "passed"
        and receipt.get("return_code") == 0
        and receipt.get("passed") == EXPECTED_RUNTIME_REGRESSION_PASSED
        and receipt.get("expected_passed") == EXPECTED_RUNTIME_REGRESSION_PASSED
        and receipt.get("failed") == 0
        and receipt.get("errors") == 0
        and receipt.get("failure_reason") is None
    )
    failed_consistently = (
        receipt.get("status") == "failed"
        and isinstance(receipt.get("failure_reason"), str)
        and bool(receipt.get("failure_reason"))
        and receipt.get("expected_passed") == EXPECTED_RUNTIME_REGRESSION_PASSED
    )
    if not passed_exactly and not failed_consistently:
        raise BuyE3DeploymentGateAmendmentError("runtime regression result is inconsistent")
    if require_passed and not passed_exactly:
        raise BuyE3DeploymentGateAmendmentError("runtime regression v2 did not pass exactly")
    return receipt


def _resolve_artifact_path(repository_root: Path, raw: Any, label: str) -> Path:
    value = str(raw).strip()
    if not value:
        raise BuyE3DeploymentGateAmendmentError(f"missing {label}")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = repository_root / path
    if path.is_symlink() or not path.is_file():
        raise BuyE3DeploymentGateAmendmentError(f"{label} is not a regular file")
    return path.resolve(strict=True)


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    candidate = path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3DeploymentGateAmendmentError("private config is not a regular file")
    target = candidate.resolve(strict=True)
    payload = yaml.load(target.read_text(encoding="utf-8"), Loader=_UniqueKeySafeLoader)
    if not isinstance(payload, dict):
        raise BuyE3DeploymentGateAmendmentError("private config root is not a mapping")
    return payload


def _strategy_and_risk(config: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    strategy = config.get("strategy")
    risk = config.get("risk")
    if not isinstance(strategy, dict) or not isinstance(risk, dict):
        raise BuyE3DeploymentGateAmendmentError("config lacks strategy/risk mappings")
    return strategy, risk


def validate_config_artifact(
    *,
    config_path: Path,
    repository_root: Path,
    expected_enabled: bool,
) -> dict[str, Any]:
    """Load the exact E3 artifact even when the live feature is disabled."""

    root = repository_root.expanduser().resolve(strict=True)
    config = _load_yaml_mapping(config_path)
    strategy, risk = _strategy_and_risk(config)
    enabled = bool(strategy.get("buy_e3_cooldown_policy_enabled", False))
    if enabled is not bool(expected_enabled):
        raise BuyE3DeploymentGateAmendmentError("BUY E3 config enablement drifted")
    manifest_path = _resolve_artifact_path(
        root,
        strategy.get("buy_e3_cooldown_artifact_manifest_path"),
        "artifact manifest",
    )
    policy_path = _resolve_artifact_path(
        root, strategy.get("buy_e3_cooldown_policy_path"), "policy"
    )
    bundle_path = _resolve_artifact_path(
        root,
        strategy.get("buy_e3_cooldown_predicate_bundle_path"),
        "predicate bundle",
    )
    manifest_file_sha = _require_sha256(
        strategy.get("buy_e3_cooldown_artifact_manifest_sha256"),
        "artifact manifest file hash",
    )
    artifact_sha = _require_sha256(
        strategy.get("buy_e3_cooldown_artifact_sha256"), "artifact canonical hash"
    )
    policy_sha = _require_sha256(strategy.get("buy_e3_cooldown_policy_sha256"), "policy file hash")
    bundle_sha = _require_sha256(
        strategy.get("buy_e3_cooldown_predicate_bundle_sha256"),
        "predicate bundle file hash",
    )
    if (
        file_sha256(manifest_path) != manifest_file_sha
        or file_sha256(policy_path) != policy_sha
        or file_sha256(bundle_path) != bundle_sha
    ):
        raise BuyE3DeploymentGateAmendmentError("artifact triple file hash drifted")
    runtime = LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=manifest_path,
        artifact_manifest_sha256=manifest_file_sha,
        expected_artifact_sha256=artifact_sha,
        policy_path=policy_path,
        policy_sha256=policy_sha,
        predicate_bundle_path=bundle_path,
        predicate_bundle_sha256=bundle_sha,
        warmup_s=float(strategy.get("buy_e3_cooldown_ema_warmup_s", 0.0)),
        max_feature_age_s=float(risk.get("max_exec_book_visible_age_s", 0.0)),
    )
    del runtime
    return {
        "enabled": enabled,
        "config_path": str(config_path.expanduser().resolve(strict=True)),
        "config_sha256": file_sha256(config_path.expanduser().resolve(strict=True)),
        "artifact_sha256": artifact_sha,
        "artifact_files": {
            "manifest": {"path": str(manifest_path), "sha256": manifest_file_sha},
            "policy": {"path": str(policy_path), "sha256": policy_sha},
            "predicate_bundle": {"path": str(bundle_path), "sha256": bundle_sha},
        },
        "artifact_loaded_with_from_files": True,
    }


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for key in sorted(value):
            name = f"{prefix}.{key}" if prefix else str(key)
            output.update(_flatten(value[key], name))
        return output
    return {prefix: value}


def validate_private_config_pair(
    *,
    disabled_config_path: Path,
    active_config_path: Path,
    repository_root: Path,
    allowed_diff: Sequence[str] = ("strategy.buy_e3_cooldown_policy_enabled",),
) -> dict[str, Any]:
    disabled = _load_yaml_mapping(disabled_config_path)
    active = _load_yaml_mapping(active_config_path)
    disabled_flat = _flatten(disabled)
    active_flat = _flatten(active)
    observed_diff = sorted(
        key
        for key in set(disabled_flat) | set(active_flat)
        if disabled_flat.get(key) != active_flat.get(key)
    )
    expected_diff = sorted(str(value) for value in allowed_diff)
    if observed_diff != expected_diff:
        raise BuyE3DeploymentGateAmendmentError(
            f"private config diff is not exactly allowlisted: {observed_diff}"
        )
    disabled_binding = validate_config_artifact(
        config_path=disabled_config_path,
        repository_root=repository_root,
        expected_enabled=False,
    )
    active_binding = validate_config_artifact(
        config_path=active_config_path,
        repository_root=repository_root,
        expected_enabled=True,
    )
    if disabled_binding["artifact_sha256"] != active_binding["artifact_sha256"]:
        raise BuyE3DeploymentGateAmendmentError("disabled/active artifact identity differs")
    if disabled_binding["artifact_files"] != active_binding["artifact_files"]:
        raise BuyE3DeploymentGateAmendmentError("disabled/active artifact files differ")
    return {
        "disabled": disabled_binding,
        "active": active_binding,
        "allowlisted_diff": expected_diff,
        "allowlisted_diff_sha256": canonical_sha256(expected_diff),
        "observed_diff": observed_diff,
    }


def ssh_host_key_fingerprints(known_hosts_path: Path) -> list[str]:
    candidate = known_hosts_path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3DeploymentGateAmendmentError("known-hosts path is not a regular file")
    path = candidate.resolve(strict=True)
    fingerprints: set[str] = set()
    for line in path.read_text(encoding="ascii").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        fields = stripped.split()
        if len(fields) < 3:
            raise BuyE3DeploymentGateAmendmentError("known-hosts line is malformed")
        try:
            key = b64decode(fields[2], validate=True)
        except ValueError as exc:
            raise BuyE3DeploymentGateAmendmentError("known-hosts key is malformed") from exc
        encoded = urlsafe_b64encode(hashlib.sha256(key).digest()).decode("ascii").rstrip("=")
        fingerprints.add(f"SHA256:{encoded}")
    if not fingerprints:
        raise BuyE3DeploymentGateAmendmentError("known-hosts has no keys")
    return sorted(fingerprints)


def capture_startup_log_checkpoint(log_path: Path) -> dict[str, Any]:
    candidate = log_path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3DeploymentGateAmendmentError("startup log is not a regular file")
    path = candidate.resolve(strict=True)
    metadata = path.stat()
    payload = {
        "schema_version": STARTUP_LOG_SCHEMA,
        "captured_utc": _utc_now(),
        "path": str(path),
        "device": int(metadata.st_dev),
        "inode": int(metadata.st_ino),
        "offset": int(metadata.st_size),
    }
    payload["canonical_checkpoint_sha256"] = document_sha256(payload, "canonical_checkpoint_sha256")
    return payload


def validate_startup_log_after_checkpoint(
    *,
    log_path: Path,
    checkpoint: Mapping[str, Any],
    required_markers: Sequence[str],
) -> dict[str, Any]:
    if checkpoint.get("schema_version") != STARTUP_LOG_SCHEMA or checkpoint.get(
        "canonical_checkpoint_sha256"
    ) != document_sha256(checkpoint, "canonical_checkpoint_sha256"):
        raise BuyE3DeploymentGateAmendmentError("startup log checkpoint identity drifted")
    path = log_path.expanduser().resolve(strict=True)
    metadata = path.stat()
    if (
        str(path) != checkpoint.get("path")
        or int(metadata.st_dev) != int(checkpoint.get("device", -1))
        or int(metadata.st_ino) != int(checkpoint.get("inode", -1))
        or int(metadata.st_size) < int(checkpoint.get("offset", -1))
    ):
        raise BuyE3DeploymentGateAmendmentError("startup log rotated or truncated")
    with path.open("rb") as handle:
        handle.seek(int(checkpoint["offset"]))
        segment = handle.read().decode("utf-8", errors="replace")
    lowered = segment.lower()
    fatal_counts = {pattern: lowered.count(pattern) for pattern in FATAL_STARTUP_PATTERNS}
    if any(fatal_counts.values()):
        raise BuyE3DeploymentGateAmendmentError("fatal marker occurred after startup checkpoint")
    positions = [segment.find(marker) for marker in required_markers]
    if any(position < 0 for position in positions) or positions != sorted(positions):
        raise BuyE3DeploymentGateAmendmentError("startup markers are missing or out of order")
    return {
        "checkpoint_sha256": checkpoint["canonical_checkpoint_sha256"],
        "segment_sha256": hashlib.sha256(segment.encode("utf-8")).hexdigest(),
        "segment_size_bytes": len(segment.encode("utf-8")),
        "required_markers_sha256": canonical_sha256(list(required_markers)),
        "fatal_pattern_counts": fatal_counts,
    }


def _proc_file(proc_root: Path, pid: int, name: str) -> Path:
    path = proc_root / str(int(pid)) / name
    if not path.exists() and not path.is_symlink():
        raise BuyE3DeploymentGateAmendmentError(f"process evidence missing: {name}")
    return path


def _cmdline_config_path(arguments: Sequence[str]) -> str:
    for index, value in enumerate(arguments):
        if value == "--config" and index + 1 < len(arguments):
            return arguments[index + 1]
        if value.startswith("--config="):
            return value.split("=", 1)[1]
    raise BuyE3DeploymentGateAmendmentError("live PID command lacks explicit --config")


def capture_actual_process_identity(
    *,
    pid: int,
    expected_repository_root: Path,
    expected_config_path: Path,
    expected_config_sha256: str,
    expected_python_executable: Path,
    expected_venv_root: Path,
    proc_root: Path = Path("/proc"),
    runtime_identity_path: Path | None = None,
) -> dict[str, Any]:
    process_root = proc_root.expanduser().resolve(strict=True)
    cmdline = _proc_file(process_root, pid, "cmdline").read_bytes().split(b"\0")
    arguments = [value.decode("utf-8", errors="strict") for value in cmdline if value]
    if not arguments or not any(value.endswith("live/main.py") for value in arguments):
        raise BuyE3DeploymentGateAmendmentError("PID is not live/main.py")
    cwd = _proc_file(process_root, pid, "cwd").resolve(strict=True)
    executable = _proc_file(process_root, pid, "exe").resolve(strict=True)
    stat_fields = _proc_file(process_root, pid, "stat").read_text(encoding="ascii").split()
    if len(stat_fields) < 22:
        raise BuyE3DeploymentGateAmendmentError("process stat is malformed")
    repository_root = expected_repository_root.expanduser().resolve(strict=True)
    config_candidate = expected_config_path.expanduser()
    if config_candidate.is_symlink() or not config_candidate.is_file():
        raise BuyE3DeploymentGateAmendmentError("actual PID config is not a regular file")
    config_path = config_candidate.resolve(strict=True)
    expected_python_path = expected_python_executable.expanduser().absolute()
    if not expected_python_path.exists():
        raise BuyE3DeploymentGateAmendmentError("expected Python executable is unavailable")
    expected_python_binary = expected_python_path.resolve(strict=True)
    venv_root = expected_venv_root.expanduser().absolute()
    if venv_root not in expected_python_path.parents:
        raise BuyE3DeploymentGateAmendmentError("expected Python path is outside the venv")
    config_sha256 = _require_sha256(expected_config_sha256, "config hash")
    command_config = Path(_cmdline_config_path(arguments)).expanduser()
    if not command_config.is_absolute():
        command_config = cwd / command_config
    command_config = command_config.resolve(strict=True)
    if cwd != repository_root or command_config != config_path:
        raise BuyE3DeploymentGateAmendmentError("actual PID cwd/config path drifted")
    if file_sha256(config_path) != config_sha256:
        raise BuyE3DeploymentGateAmendmentError("actual PID config bytes drifted")
    if executable != expected_python_binary:
        raise BuyE3DeploymentGateAmendmentError("actual PID Python/venv identity drifted")
    runtime_binding: dict[str, Any] = {"present": False}
    if runtime_identity_path is not None:
        runtime_path = runtime_identity_path.expanduser().resolve(strict=True)
        runtime = read_json(runtime_path)
        if (
            int(runtime.get("pid", -1)) != int(pid)
            or Path(str(runtime.get("config_path", ""))).resolve() != config_path
            or runtime.get("config_sha256") != config_sha256
            or Path(str(runtime.get("python_executable", ""))).resolve() != executable
        ):
            raise BuyE3DeploymentGateAmendmentError("runtime identity does not match PID")
        runtime_binding = {
            "present": True,
            "path": str(runtime_path),
            "file_sha256": file_sha256(runtime_path),
            "schema_version": runtime.get("schema_version"),
        }
    payload: dict[str, Any] = {
        "schema_version": PROCESS_IDENTITY_SCHEMA,
        "captured_utc": _utc_now(),
        "pid": int(pid),
        "pid_start_ticks": int(stat_fields[21]),
        "cmdline": arguments,
        "cmdline_sha256": canonical_sha256(arguments),
        "cwd": str(cwd),
        "config_path": str(config_path),
        "config_sha256": config_sha256,
        "python_executable": str(expected_python_path),
        "python_binary_resolved": str(executable),
        "venv_root": str(venv_root),
        "runtime_identity": runtime_binding,
    }
    payload["canonical_process_identity_sha256"] = document_sha256(
        payload, "canonical_process_identity_sha256"
    )
    return payload


def require_fresh_pid(before: Mapping[str, Any], after: Mapping[str, Any]) -> None:
    if before.get("schema_version") != PROCESS_IDENTITY_SCHEMA:
        raise BuyE3DeploymentGateAmendmentError("pre-restart PID identity is malformed")
    if after.get("schema_version") != PROCESS_IDENTITY_SCHEMA:
        raise BuyE3DeploymentGateAmendmentError("post-restart PID identity is malformed")
    if int(before.get("pid", -1)) == int(after.get("pid", -1)):
        raise BuyE3DeploymentGateAmendmentError("restart reused the old live PID")
    if before.get("canonical_process_identity_sha256") == after.get(
        "canonical_process_identity_sha256"
    ):
        raise BuyE3DeploymentGateAmendmentError("post-restart process identity was reused")


def _finite_number(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise BuyE3DeploymentGateAmendmentError(f"{label} is not numeric") from exc
    if not math.isfinite(number):
        raise BuyE3DeploymentGateAmendmentError(f"{label} is non-finite")
    return number


def _counter_map(sample: Mapping[str, Any]) -> dict[str, int]:
    raw = sample.get("counters")
    if not isinstance(raw, Mapping):
        raise BuyE3DeploymentGateAmendmentError("resource sample lacks counters")
    output: dict[str, int] = {}
    for key in REQUIRED_ZERO_COUNTERS:
        if key not in raw:
            raise BuyE3DeploymentGateAmendmentError(f"resource sample lacks {key}")
        output[key] = int(raw[key])
    return output


def validate_concurrent_resource_evidence(
    *,
    samples: Sequence[Mapping[str, Any]],
    benchmark_receipt: Mapping[str, Any],
    pre_health: Mapping[str, Any],
    post_health: Mapping[str, Any],
) -> dict[str, Any]:
    if len(samples) < 2:
        raise BuyE3DeploymentGateAmendmentError("concurrent resource window is too short")
    pre_pid = int(pre_health.get("pid", -1))
    post_pid = int(post_health.get("pid", -1))
    if pre_pid <= 0 or post_pid != pre_pid:
        raise BuyE3DeploymentGateAmendmentError("post-benchmark health is not same-PID")
    for field in (
        "canonical_process_identity_sha256",
        "config_sha256",
        "runtime_code_sha256",
        "artifact_sha256",
    ):
        if not pre_health.get(field) or pre_health.get(field) != post_health.get(field):
            raise BuyE3DeploymentGateAmendmentError(
                f"post-benchmark health identity drifted: {field}"
            )
    callback = benchmark_receipt.get("callback_benchmark")
    decision = benchmark_receipt.get("decision_benchmark")
    if not isinstance(callback, Mapping) or not isinstance(decision, Mapping):
        raise BuyE3DeploymentGateAmendmentError("benchmark receipt is malformed")
    observed_rate = _finite_number(callback.get("observed_live_rate_hz"), "observed rate")
    achieved_rate = _finite_number(callback.get("achieved_rate_hz"), "achieved rate")
    callback_p99 = _finite_number(callback.get("latency_p99_us"), "callback p99")
    decision_p99 = _finite_number(decision.get("latency_p99_us"), "decision p99")
    first_counters = _counter_map(samples[0])
    all_counters = [_counter_map(sample) for sample in samples]
    mem_available = [
        _finite_number(row.get("mem_available_mib"), "MemAvailable") for row in samples
    ]
    live_rss = [_finite_number(row.get("live_rss_mib"), "live RSS") for row in samples]
    benchmark_rss = [
        _finite_number(row.get("benchmark_rss_mib"), "benchmark RSS") for row in samples
    ]
    combined = [live + bench for live, bench in zip(live_rss, benchmark_rss, strict=True)]
    deep_buffers = [int(row.get("deep_book_buffer", -1)) for row in samples]
    oom_events = [int(row.get("oom_events", -1)) for row in samples]
    swap_in = [int(row.get("swap_in_kib", -1)) for row in samples]
    swap_out = [int(row.get("swap_out_kib", -1)) for row in samples]
    checks = {
        "concurrent_live_and_benchmark_observed": any(value > 0.0 for value in benchmark_rss),
        "min_mem_available_at_least_512mib": min(mem_available) >= MIN_MEM_AVAILABLE_MIB,
        "live_rss_at_most_512mib": max(live_rss) <= MAX_LIVE_RSS_MIB,
        "benchmark_rss_at_most_256mib": max(benchmark_rss) <= MAX_BENCHMARK_RSS_MIB,
        "combined_rss_at_most_768mib": max(combined) <= MAX_COMBINED_RSS_MIB,
        "no_oom_events": min(oom_events) >= 0 and max(oom_events) == min(oom_events),
        "no_swap_activity": (
            min(swap_in) >= 0
            and min(swap_out) >= 0
            and max(swap_in) == min(swap_in)
            and max(swap_out) == min(swap_out)
        ),
        "zero_drop_invalid_overflow_delta": all(
            counters[key] == first_counters[key]
            for counters in all_counters
            for key in REQUIRED_ZERO_COUNTERS
        ),
        "deep_book_buffer_zero": all(value == 0 for value in deep_buffers),
        "true_2x_observed_rate": observed_rate > 0.0
        and achieved_rate >= MIN_RATE_MULTIPLIER * observed_rate,
        "callback_p99_at_most_2ms": callback_p99 <= MAX_CALLBACK_P99_US,
        "decision_p99_at_most_10ms": decision_p99 <= MAX_DECISION_P99_US,
        "post_benchmark_same_pid_health": True,
    }
    if not all(checks.values()):
        failed = sorted(key for key, value in checks.items() if not value)
        raise BuyE3DeploymentGateAmendmentError(
            "concurrent resource gate failed: " + ", ".join(failed)
        )
    payload: dict[str, Any] = {
        "schema_version": RESOURCE_WINDOW_SCHEMA,
        "status": "concurrent_disabled_live_benchmark_passed",
        "sample_count": len(samples),
        "live_pid": pre_pid,
        "pre_health_sha256": pre_health["canonical_process_identity_sha256"],
        "post_health_sha256": post_health["canonical_process_identity_sha256"],
        "thresholds": {
            "min_mem_available_mib": MIN_MEM_AVAILABLE_MIB,
            "max_live_rss_mib": MAX_LIVE_RSS_MIB,
            "max_benchmark_rss_mib": MAX_BENCHMARK_RSS_MIB,
            "max_combined_rss_mib": MAX_COMBINED_RSS_MIB,
            "min_achieved_to_observed_rate": MIN_RATE_MULTIPLIER,
            "max_callback_p99_us": MAX_CALLBACK_P99_US,
            "max_decision_p99_us": MAX_DECISION_P99_US,
        },
        "observed": {
            "min_mem_available_mib": min(mem_available),
            "max_live_rss_mib": max(live_rss),
            "max_benchmark_rss_mib": max(benchmark_rss),
            "max_combined_rss_mib": max(combined),
            "achieved_to_observed_rate": achieved_rate / observed_rate,
            "callback_p99_us": callback_p99,
            "decision_p99_us": decision_p99,
        },
        "checks": checks,
        "sample_series_sha256": canonical_sha256(list(samples)),
        "economic_values_persisted": False,
        "hypothetical_live_actions_scored": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    payload["canonical_resource_window_sha256"] = document_sha256(
        payload, "canonical_resource_window_sha256"
    )
    return payload


def capture_concurrent_disabled_live_benchmark(
    *,
    launch_benchmark: Callable[[], Any],
    sample_provider: Callable[[int], Mapping[str, Any]],
    benchmark_receipt_provider: Callable[[], Mapping[str, Any]],
    pre_health: Mapping[str, Any],
    post_health_provider: Callable[[], Mapping[str, Any]],
    sample_interval_s: float = 0.1,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Sample disabled live and an isolated benchmark in the same time window.

    ``launch_benchmark`` must return a Popen-like object with ``pid``, ``poll``
    and ``wait``.  Providers are injectable so tests never need a real live
    process, while the production caller can read Linux proc/health counters.
    """

    interval = float(sample_interval_s)
    if not math.isfinite(interval) or interval <= 0.0:
        raise BuyE3DeploymentGateAmendmentError("sample interval must be positive")
    live_pid = int(pre_health.get("pid", -1))
    if live_pid <= 0:
        raise BuyE3DeploymentGateAmendmentError("pre-health lacks live PID")
    benchmark = launch_benchmark()
    benchmark_pid = int(getattr(benchmark, "pid", -1))
    if benchmark_pid <= 0 or benchmark_pid == live_pid:
        raise BuyE3DeploymentGateAmendmentError("benchmark process identity is invalid")
    samples: list[Mapping[str, Any]] = []
    while benchmark.poll() is None:
        samples.append(dict(sample_provider(benchmark_pid)))
        sleep(interval)
    returncode = int(benchmark.wait())
    samples.append(dict(sample_provider(benchmark_pid)))
    if returncode != 0:
        raise BuyE3DeploymentGateAmendmentError("concurrent benchmark process failed")
    return validate_concurrent_resource_evidence(
        samples=samples,
        benchmark_receipt=dict(benchmark_receipt_provider()),
        pre_health=pre_health,
        post_health=dict(post_health_provider()),
    )


def build_amended_gate_receipt(
    *,
    execution_identity: Mapping[str, Any],
    runtime_sources: Mapping[str, Any],
    artifact_binding: Mapping[str, Any],
    config_binding: Mapping[str, Any],
    host_binding: Mapping[str, Any],
    disabled_process_identity: Mapping[str, Any],
    startup_log_binding: Mapping[str, Any],
    resource_window: Mapping[str, Any],
    rollback_identities: Mapping[str, Any],
) -> dict[str, Any]:
    if execution_identity.get("execution_commit") != FROZEN_EXECUTION_COMMIT:
        raise BuyE3DeploymentGateAmendmentError("amendment is not bound to frozen execution")
    if config_binding.get("disabled", {}).get("enabled") is not False:
        raise BuyE3DeploymentGateAmendmentError("deployment gate did not run disabled")
    if resource_window.get("schema_version") != RESOURCE_WINDOW_SCHEMA:
        raise BuyE3DeploymentGateAmendmentError("resource window identity drifted")
    if resource_window.get("canonical_resource_window_sha256") != document_sha256(
        resource_window, "canonical_resource_window_sha256"
    ):
        raise BuyE3DeploymentGateAmendmentError("resource window hash drifted")
    if set(rollback_identities) != {"primary_disabled", "deep_predecessor"}:
        raise BuyE3DeploymentGateAmendmentError("dual rollback identities are incomplete")
    for name, identity in rollback_identities.items():
        if not isinstance(identity, Mapping) or identity.get("buy_e3_enabled") is not False:
            raise BuyE3DeploymentGateAmendmentError(f"rollback identity is unsafe: {name}")
        if identity.get("buy_deadline_identity") != "B0":
            raise BuyE3DeploymentGateAmendmentError(
                f"rollback identity can import an E3 deadline: {name}"
            )
    required_execution = (
        "execution_commit",
        "execution_tree",
        "annotated_tag",
        "annotated_tag_object",
        "tag_peeled_commit",
    )
    if any(not execution_identity.get(field) for field in required_execution):
        raise BuyE3DeploymentGateAmendmentError("execution Git binding is incomplete")
    if execution_identity.get("tag_peeled_commit") != execution_identity.get("execution_commit"):
        raise BuyE3DeploymentGateAmendmentError("annotated tag peel is inconsistent")
    runtime_files = runtime_sources.get("files")
    if (
        not isinstance(runtime_files, Mapping)
        or set(REQUIRED_RUNTIME_PATHS) - set(runtime_files)
        or _SHA256_RE.fullmatch(str(runtime_sources.get("runtime_code_sha256", ""))) is None
    ):
        raise BuyE3DeploymentGateAmendmentError("runtime source binding is incomplete")
    artifact_sha = _require_sha256(artifact_binding.get("artifact_sha256"), "artifact hash")
    artifact_files = artifact_binding.get("artifact_files")
    if not isinstance(artifact_files, Mapping) or set(artifact_files) != {
        "manifest",
        "policy",
        "predicate_bundle",
    }:
        raise BuyE3DeploymentGateAmendmentError("artifact triple binding is incomplete")
    for role, binding in artifact_files.items():
        if not isinstance(binding, Mapping):
            raise BuyE3DeploymentGateAmendmentError(f"artifact binding is malformed: {role}")
        _require_sha256(binding.get("sha256"), f"artifact file {role}")
    disabled_config = config_binding.get("disabled")
    active_config = config_binding.get("active")
    if not isinstance(disabled_config, Mapping) or not isinstance(active_config, Mapping):
        raise BuyE3DeploymentGateAmendmentError("private config pair binding is incomplete")
    if (
        disabled_config.get("enabled") is not False
        or active_config.get("enabled") is not True
        or disabled_config.get("artifact_loaded_with_from_files") is not True
        or active_config.get("artifact_loaded_with_from_files") is not True
        or disabled_config.get("artifact_sha256") != artifact_sha
        or active_config.get("artifact_sha256") != artifact_sha
    ):
        raise BuyE3DeploymentGateAmendmentError("private config pair was not isolated-validated")
    required_host = (
        "active_pointer_file_sha256",
        "known_hosts_file_sha256",
        "host_key_fingerprint",
        "repo_root",
        "python_executable",
        "venv_root",
    )
    if any(not host_binding.get(field) for field in required_host):
        raise BuyE3DeploymentGateAmendmentError("host/SSH identity binding is incomplete")
    for field in ("active_pointer_file_sha256", "known_hosts_file_sha256"):
        _require_sha256(host_binding[field], field)
    if (
        disabled_process_identity.get("schema_version") != PROCESS_IDENTITY_SCHEMA
        or disabled_process_identity.get("canonical_process_identity_sha256")
        != document_sha256(disabled_process_identity, "canonical_process_identity_sha256")
        or disabled_process_identity.get("config_sha256") != disabled_config.get("config_sha256")
        or disabled_process_identity.get("cwd") != host_binding.get("repo_root")
        or disabled_process_identity.get("artifact_sha256") != artifact_sha
        or disabled_process_identity.get("runtime_code_sha256")
        != runtime_sources.get("runtime_code_sha256")
    ):
        raise BuyE3DeploymentGateAmendmentError("actual disabled PID identity is inconsistent")
    if (
        _SHA256_RE.fullmatch(str(startup_log_binding.get("checkpoint_sha256", ""))) is None
        or _SHA256_RE.fullmatch(str(startup_log_binding.get("segment_sha256", ""))) is None
        or any(startup_log_binding.get("fatal_pattern_counts", {}).values())
    ):
        raise BuyE3DeploymentGateAmendmentError("startup log binding is incomplete")
    if int(resource_window.get("live_pid", -1)) != int(disabled_process_identity.get("pid", -2)):
        raise BuyE3DeploymentGateAmendmentError("resource window used another live PID")
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER_IDENTITY,
        "status": "disabled_deploy_gate_passed_activation_not_yet_authorized",
        "generated_utc": _utc_now(),
        "execution_identity": dict(execution_identity),
        "runtime_sources": dict(runtime_sources),
        "artifact_binding": dict(artifact_binding),
        "config_binding": dict(config_binding),
        "host_binding": dict(host_binding),
        "disabled_process_identity": dict(disabled_process_identity),
        "startup_log_binding": dict(startup_log_binding),
        "resource_window": dict(resource_window),
        "rollback_identities": dict(rollback_identities),
        "activation_contract": {
            "restart_only": True,
            "sighup_allowed": False,
            "fresh_pid_required": True,
            "external_narrowgate_live_config_required": True,
            "warmup_executes_natural_b0": True,
            "hypothetical_scorer_allowed": False,
        },
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    receipt["canonical_amendment_receipt_sha256"] = document_sha256(
        receipt, "canonical_amendment_receipt_sha256"
    )
    return receipt


def validate_amended_gate_receipt(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("status") != "disabled_deploy_gate_passed_activation_not_yet_authorized"
        or payload.get("canonical_amendment_receipt_sha256")
        != document_sha256(payload, "canonical_amendment_receipt_sha256")
        or payload.get("permissions", {}).get("live_authorized") is not False
    ):
        raise BuyE3DeploymentGateAmendmentError("amended gate receipt identity drifted")
    if (
        payload.get("execution_identity", {}).get("execution_commit") != FROZEN_EXECUTION_COMMIT
        or payload.get("activation_contract", {}).get("restart_only") is not True
        or payload.get("activation_contract", {}).get("sighup_allowed") is not False
        or payload.get("activation_contract", {}).get("fresh_pid_required") is not True
        or payload.get("config_binding", {}).get("disabled", {}).get("enabled") is not False
        or payload.get("config_binding", {}).get("active", {}).get("enabled") is not True
        or set(payload.get("rollback_identities", {})) != {"primary_disabled", "deep_predecessor"}
    ):
        raise BuyE3DeploymentGateAmendmentError("amended gate semantic contract drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    return parser


def main() -> int:
    payload = validate_amended_gate_receipt(_parser().parse_args().receipt)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "canonical_amendment_receipt_sha256": payload["canonical_amendment_receipt_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BuyE3DeploymentGateAmendmentError",
    "EXPECTED_RUNTIME_REGRESSION_PASSED",
    "FROZEN_EXECUTION_COMMIT",
    "FROZEN_EXECUTION_TAG",
    "FROZEN_EXECUTION_TAG_OBJECT",
    "FROZEN_EXECUTION_TREE",
    "PROCESS_IDENTITY_SCHEMA",
    "RESOURCE_WINDOW_SCHEMA",
    "RUNTIME_REGRESSION_SCHEMA",
    "RUNTIME_REGRESSION_SOURCES",
    "RUNTIME_REGRESSION_TESTS",
    "SCHEMA_VERSION",
    "STARTUP_LOG_SCHEMA",
    "SUPERSEDED_REGRESSION_SCHEMA_V1",
    "atomic_write_receipt",
    "bind_lexical_venv_python",
    "build_amended_gate_receipt",
    "canonical_sha256",
    "capture_concurrent_disabled_live_benchmark",
    "capture_actual_process_identity",
    "capture_startup_log_checkpoint",
    "document_sha256",
    "file_sha256",
    "require_fresh_pid",
    "run_runtime_regression_tests_v2",
    "ssh_host_key_fingerprints",
    "validate_amended_gate_receipt",
    "validate_concurrent_resource_evidence",
    "validate_config_artifact",
    "validate_private_config_pair",
    "validate_runtime_regression_receipt_v2",
    "validate_startup_log_after_checkpoint",
    "verify_execution_git_identity",
    "verify_runtime_sources",
]
