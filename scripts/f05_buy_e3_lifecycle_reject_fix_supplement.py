#!/usr/bin/env python3
"""Freeze the non-economic lifecycle repair carried by BUY E3 direct-v4."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

try:
    from scripts import f05_buy_e3_active_release as legacy_release
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import f05_buy_e3_active_release as legacy_release

SCHEMA_VERSION: Final = "f05_buy_e3_lifecycle_reject_fix_supplement.v1"
IDENTITY: Final = SCHEMA_VERSION
STATUS: Final = "lifecycle_only_runtime_fix_verified_no_economic_change"
CANONICAL_FIELD: Final = "canonical_supplement_sha256"

PARENT_EXECUTION: Final = {
    "execution_commit": "1be0e062fe2c8ac12a34d5fc2193ca166898105a",
    "execution_tree": "ec54a9fbe5a4e476af4d6e58cc323804f0a2f275",
    "annotated_operational_tag": "f05-owner-buy-e3-direct-live-v3-20260824",
    "annotated_operational_tag_object": "00b5d8bb9078a04dee7e2ae2b3ecdec332698106",
    "tag_peeled_commit": "1be0e062fe2c8ac12a34d5fc2193ca166898105a",
}
RUNTIME_CHANGED_FILES: Final = frozenset(
    {
        "execution/order_lifecycle.py",
        "execution/order_lifecycle_journal_v2.py",
        "execution/order_lifecycle_journal_v2_strict_native.py",
        "execution/order_lifecycle_live_writer_v2.py",
        "strategy/boolean_cooldown_buy_e3.py",
    }
)
EXPECTED_CHANGED_REPOSITORY_FILES: Final = frozenset(
    {
        *RUNTIME_CHANGED_FILES,
        "scripts/f05_buy_e3_direct_owner_release_v2.py",
        "scripts/f05_buy_e3_lifecycle_reject_fix_supplement.py",
        "tests/test_execution_order_lifecycle.py",
        "tests/test_f05_buy_e3_direct_owner_release_v2.py",
        "tests/test_f05_buy_e3_lifecycle_reject_fix_supplement.py",
        "tests/test_order_lifecycle_journal_v2.py",
        "tests/test_order_lifecycle_live_writer_v2.py",
    }
)
CRITICAL_UNCHANGED_REPOSITORY_FILES: Final = (
    "live/config.py",
    "live/main.py",
    "live/runtime_policy.py",
    "live/ws_handler.py",
    "strategy/boolean_cooldown_live.py",
    "strategy/maker_engine.py",
)
BUY_E3_DECISION_AST_NODES: Final = (
    "BuyE3CooldownDecision",
    "_PairState",
    "_label",
    "_pair_key",
    "_tri_not",
    "_literal_state",
    "_and_state",
    "_or_state",
    "_CompiledBuyE3Evaluator",
    "_FullMidEmaState",
    "ReceiveTimeFullMidEmaWindows",
    "_definition_value",
    "LiveBuyE3CooldownPolicy",
)
EXACT_ARTIFACT_SHA256: Final = (
    "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
)
ARTIFACT_FILE_SHA256: Final = {
    "manifest": "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929",
    "policy": "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b",
    "predicate_bundle": "4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915",
}
CONFIG_FILE_SHA256: Final = {
    "active": "2f61532126cbe633424476cb093c6c978bab1f935f69a30e06677d677008cae6",
    "disabled": "d08df3958f4243109036555ba60d58c2599d88560990305f176744d62959c7ef",
}
ACTION_VOCABULARY_SECONDS: Final = [79, 173, 223, 356, 640, 709, 2048]
PERMISSIONS: Final = {
    "research_authorized": False,
    "action_authorized": False,
    "live_authorized": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "economic_values_read": False,
    "new_economic_arm_run": False,
    "shadow_or_companion_created": False,
    "remote_mutation_authorized": False,
}
FOCUSED_REGRESSION_TARGETS: Final = (
    "tests/test_execution_order_lifecycle.py::"
    "test_preactivation_rejection_records_complete_zero_exchange_exposure",
    "tests/test_order_lifecycle_journal_v2.py::"
    "test_null_exchange_id_exception_requires_complete_valid_visible_exposure",
    "tests/test_order_lifecycle_live_writer_v2.py::"
    "test_preactivation_gtx_reject_commits_complete_zero_exchange_exposure",
    "tests/test_f05_buy_e3_direct_owner_release_v2.py",
)
FULL_REGRESSION_TARGETS: Final = (
    "tests/test_execution_order_lifecycle.py",
    "tests/test_order_lifecycle_live_writer_v2.py",
    "tests/test_order_lifecycle_journal_v2.py",
    "tests/test_order_lifecycle_v2_event_lockstep.py",
    "tests/test_order_lifecycle_v2_cpp_event_stream_binding.py",
    "tests/test_order_lifecycle_v2_40day_cpp_lockstep.py",
    "tests/test_order_lifecycle_journal_writer_v2.py",
    "tests/test_order_lifecycle_journal_writer_v2_replay_day_buffered.py",
    "tests/test_order_lifecycle_journal_writer_v2_replay_single_owner.py",
    "tests/test_f05_buy_e3_direct_owner_release.py",
    "tests/test_live_buy_e3_startup_attestation.py",
    "tests/test_live_fill_cooldown_policy.py",
    "tests/test_f05_buy_e3_direct_owner_release_v2.py",
)
SAFE_IMPORT_MODULES: Final = {
    "execution.order_lifecycle": "execution/order_lifecycle.py",
    "execution.order_lifecycle_journal_v2": "execution/order_lifecycle_journal_v2.py",
    "execution.order_lifecycle_journal_v2_strict_native": (
        "execution/order_lifecycle_journal_v2_strict_native.py"
    ),
    "execution.order_lifecycle_live_writer_v2": (
        "execution/order_lifecycle_live_writer_v2.py"
    ),
    "strategy.boolean_cooldown_buy_e3": "strategy/boolean_cooldown_buy_e3.py",
}
TOP_LEVEL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "v3_parent_execution",
        "v4_execution",
        "changed_repository_files",
        "changed_runtime_files",
        "focused_regression",
        "full_regression",
        "e3_unchanged",
        "permissions",
        CANONICAL_FIELD,
    }
)
REGRESSION_FIELDS: Final = frozenset(
    {
        "execution_commit",
        "targets",
        "collect_command",
        "run_command",
        "collect_returncode",
        "run_returncode",
        "nodeids",
        "passed",
        "failed",
        "skipped",
        "junit_xml_sha256",
        "collect_stdout_sha256",
        "collect_stderr_sha256",
        "run_stdout_sha256",
        "run_stderr_sha256",
        "interpreter",
        "sanitized_python_environment",
        "safe_import_files",
        "safe_import_stdout_sha256",
        "test_source_files",
        "collector_source",
        "canonical_regression_sha256",
    }
)
INTERPRETER_FIELDS: Final = frozenset(
    {
        "lexical_executable",
        "resolved_executable",
        "executable_sha256",
        "python_version",
        "pyvenv_cfg_path",
        "pyvenv_cfg_sha256",
    }
)


class LifecycleFixSupplementError(RuntimeError):
    """Raised when the lifecycle-only supplement cannot be proven."""


def _git_bytes(root: Path, *args: str) -> bytes:
    try:
        completed = subprocess.run(
            ("git", *args),
            cwd=root,
            check=True,
            capture_output=True,
            timeout=20.0,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise LifecycleFixSupplementError(
            f"Git content command failed: {' '.join(args)}"
        ) from exc
    return completed.stdout


def _file_binding(root: Path, commit: str, relative_path: str) -> dict[str, str]:
    blob = legacy_release._git(  # noqa: SLF001
        root,
        "rev-parse",
        f"{commit}:{relative_path}",
    )
    if len(blob) != 40 or any(character not in "0123456789abcdef" for character in blob):
        raise LifecycleFixSupplementError(f"invalid Git blob for {relative_path}")
    raw = _git_bytes(root, "show", f"{commit}:{relative_path}")
    return {
        "git_blob_sha1": blob,
        "file_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _changed_repository_files(
    root: Path,
    v4_commit: str,
) -> dict[str, dict[str, str]]:
    raw = legacy_release._git(  # noqa: SLF001
        root,
        "diff",
        "--name-status",
        PARENT_EXECUTION["execution_commit"],
        v4_commit,
    )
    observed: set[str] = set()
    for line in raw.splitlines():
        fields = line.split("\t")
        if len(fields) != 2 or fields[0] not in {"A", "M"}:
            raise LifecycleFixSupplementError(
                "direct-v4 diff must contain only added or modified files"
            )
        observed.add(fields[1])
    if observed != set(EXPECTED_CHANGED_REPOSITORY_FILES):
        raise LifecycleFixSupplementError("direct-v4 changed-file allowlist drifted")
    return {
        path: _file_binding(root, v4_commit, path)
        for path in sorted(observed)
    }


def _unchanged_critical_files(
    root: Path,
    v4_commit: str,
) -> dict[str, dict[str, str]]:
    bindings: dict[str, dict[str, str]] = {}
    for path in CRITICAL_UNCHANGED_REPOSITORY_FILES:
        parent = _file_binding(root, PARENT_EXECUTION["execution_commit"], path)
        current = _file_binding(root, v4_commit, path)
        if parent != current:
            raise LifecycleFixSupplementError(f"E3 critical source drifted: {path}")
        bindings[path] = current
    return bindings


def _semantic_ast_hashes(source: bytes) -> dict[str, str]:
    try:
        tree = ast.parse(source.decode("utf-8"))
    except (SyntaxError, UnicodeDecodeError) as exc:
        raise LifecycleFixSupplementError("BUY E3 strategy AST cannot be parsed") from exc
    by_name = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }
    if not set(BUY_E3_DECISION_AST_NODES).issubset(by_name):
        raise LifecycleFixSupplementError("BUY E3 decision AST node set drifted")
    return {
        name: hashlib.sha256(
            ast.dump(
                by_name[name],
                annotate_fields=True,
                include_attributes=False,
            ).encode("utf-8")
        ).hexdigest()
        for name in BUY_E3_DECISION_AST_NODES
    }


def _unchanged_decision_ast(root: Path, v4_commit: str) -> dict[str, str]:
    path = "strategy/boolean_cooldown_buy_e3.py"
    parent = _semantic_ast_hashes(
        _git_bytes(root, "show", f"{PARENT_EXECUTION['execution_commit']}:{path}")
    )
    current = _semantic_ast_hashes(_git_bytes(root, "show", f"{v4_commit}:{path}"))
    if parent != current:
        raise LifecycleFixSupplementError("BUY E3 decision or EMA semantics drifted")
    return current


def _sha256_file(path: Path, label: str) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise LifecycleFixSupplementError(f"cannot hash {label}") from exc


def _interpreter_binding() -> dict[str, str]:
    lexical = Path(sys.executable).absolute()
    resolved = lexical.resolve(strict=True)
    pyvenv = lexical.parent.parent / "pyvenv.cfg"
    if not pyvenv.is_file():
        raise LifecycleFixSupplementError("collector interpreter is not a bound venv")
    return {
        "lexical_executable": str(lexical),
        "resolved_executable": str(resolved),
        "executable_sha256": _sha256_file(resolved, "resolved Python executable"),
        "python_version": sys.version,
        "pyvenv_cfg_path": str(pyvenv.absolute()),
        "pyvenv_cfg_sha256": _sha256_file(pyvenv, "pyvenv.cfg"),
    }


def _sanitized_python_environment(root: Path) -> dict[str, str | None]:
    return {
        "PYTHONPATH": str(root),
        "PYTHONSAFEPATH": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHOME": None,
        "PYTHONUSERBASE": None,
        "NARROWGATE_ROOT": str(root),
    }


def _subprocess_environment(root: Path) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "PYTHONPATH",
            "PYTHONSAFEPATH",
            "PYTHONNOUSERSITE",
            "PYTHONDONTWRITEBYTECODE",
            "PYTHONHOME",
            "PYTHONUSERBASE",
            "NARROWGATE_ROOT",
        }
    }
    environment.update(
        {
            key: value
            for key, value in _sanitized_python_environment(root).items()
            if value is not None
        }
    )
    return environment


def _assert_exact_checkout(root: Path, v4_commit: str) -> None:
    head = legacy_release._git(root, "rev-parse", "HEAD")  # noqa: SLF001
    status = legacy_release._git(  # noqa: SLF001
        root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if head != v4_commit or status:
        raise LifecycleFixSupplementError(
            "regression checkout must remain clean at the exact direct-v4 commit"
        )


def _run_command(root: Path, command: Sequence[str], label: str) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            tuple(command),
            cwd=root,
            check=False,
            capture_output=True,
            env=_subprocess_environment(root),
            timeout=120.0,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LifecycleFixSupplementError(f"{label} command failed to execute") from exc


def _collect_nodeids(
    root: Path,
    v4_commit: str,
    targets: Sequence[str],
) -> tuple[list[str], subprocess.CompletedProcess]:
    _assert_exact_checkout(root, v4_commit)
    command = (sys.executable, "-m", "pytest", "--collect-only", "-q", *targets)
    completed = _run_command(root, command, "pytest collection")
    _assert_exact_checkout(root, v4_commit)
    try:
        lines = completed.stdout.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise LifecycleFixSupplementError("pytest collection output is not UTF-8") from exc
    nodeids = [line.strip() for line in lines if line.strip().startswith("tests/")]
    if completed.returncode != 0 or not nodeids or len(nodeids) != len(set(nodeids)):
        raise LifecycleFixSupplementError("fixed pytest nodeid collection failed")
    return nodeids, completed


def _safe_import_files(root: Path, v4_commit: str) -> tuple[dict[str, str], str]:
    modules = json.dumps(sorted(SAFE_IMPORT_MODULES), separators=(",", ":"))
    program = (
        "import importlib,json,pathlib;"
        f"mods={modules};"
        "print(json.dumps({m:str(pathlib.Path(importlib.import_module(m).__file__)."
        "resolve()) for m in mods},sort_keys=True))"
    )
    _assert_exact_checkout(root, v4_commit)
    completed = _run_command(
        root,
        (sys.executable, "-c", program),
        "safe import",
    )
    _assert_exact_checkout(root, v4_commit)
    if completed.returncode != 0:
        raise LifecycleFixSupplementError("safe import verification failed")
    try:
        observed = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LifecycleFixSupplementError("safe import output is malformed") from exc
    expected = {
        module: str((root / relative).resolve(strict=True))
        for module, relative in SAFE_IMPORT_MODULES.items()
    }
    if observed != expected:
        raise LifecycleFixSupplementError("runtime imports did not resolve from direct-v4")
    return dict(SAFE_IMPORT_MODULES), hashlib.sha256(completed.stdout).hexdigest()


def _test_source_bindings(
    root: Path,
    v4_commit: str,
    targets: Sequence[str],
) -> dict[str, dict[str, str]]:
    paths = sorted({target.split("::", 1)[0] for target in targets})
    return {path: _file_binding(root, v4_commit, path) for path in paths}


def _junit_counts(raw: bytes) -> tuple[int, int, int]:
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise LifecycleFixSupplementError("pytest JUnit XML is malformed") from exc
    suites = [root] if root.tag == "testsuite" else list(root.findall("./testsuite"))
    if not suites:
        raise LifecycleFixSupplementError("pytest JUnit XML has no test suite")
    tests = sum(int(suite.attrib.get("tests", "0")) for suite in suites)
    failures = sum(int(suite.attrib.get("failures", "0")) for suite in suites)
    errors = sum(int(suite.attrib.get("errors", "0")) for suite in suites)
    skipped = sum(int(suite.attrib.get("skipped", "0")) for suite in suites)
    return tests - failures - errors - skipped, failures + errors, skipped


def _collect_regression(
    root: Path,
    v4_commit: str,
    targets: Sequence[str],
    label: str,
) -> dict[str, Any]:
    _assert_exact_checkout(root, v4_commit)
    safe_imports, safe_import_stdout_sha256 = _safe_import_files(root, v4_commit)
    nodeids, collection = _collect_nodeids(root, v4_commit, targets)
    with tempfile.TemporaryDirectory(prefix="f05-lifecycle-regression-") as directory:
        junit = Path(directory) / "pytest-junit.xml"
        actual_command = (
            sys.executable,
            "-m",
            "pytest",
            "-q",
            f"--junitxml={junit}",
            *targets,
        )
        _assert_exact_checkout(root, v4_commit)
        completed = _run_command(root, actual_command, label)
        _assert_exact_checkout(root, v4_commit)
        try:
            junit_raw = junit.read_bytes()
        except OSError as exc:
            raise LifecycleFixSupplementError(f"{label} did not write JUnit XML") from exc
    passed, failed, skipped = _junit_counts(junit_raw)
    if (
        completed.returncode != 0
        or failed != 0
        or passed <= 0
        or passed + skipped != len(nodeids)
    ):
        raise LifecycleFixSupplementError(f"{label} did not pass exact nodeids")
    payload: dict[str, Any] = {
        "execution_commit": v4_commit,
        "targets": list(targets),
        "collect_command": [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            *targets,
        ],
        "run_command": [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--junitxml=<CREATE_ONLY_TEMPFILE>",
            *targets,
        ],
        "collect_returncode": collection.returncode,
        "run_returncode": completed.returncode,
        "nodeids": nodeids,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "junit_xml_sha256": hashlib.sha256(junit_raw).hexdigest(),
        "collect_stdout_sha256": hashlib.sha256(collection.stdout).hexdigest(),
        "collect_stderr_sha256": hashlib.sha256(collection.stderr).hexdigest(),
        "run_stdout_sha256": hashlib.sha256(completed.stdout).hexdigest(),
        "run_stderr_sha256": hashlib.sha256(completed.stderr).hexdigest(),
        "interpreter": _interpreter_binding(),
        "sanitized_python_environment": _sanitized_python_environment(root),
        "safe_import_files": safe_imports,
        "safe_import_stdout_sha256": safe_import_stdout_sha256,
        "test_source_files": _test_source_bindings(root, v4_commit, targets),
        "collector_source": _file_binding(
            root,
            v4_commit,
            "scripts/f05_buy_e3_lifecycle_reject_fix_supplement.py",
        ),
    }
    payload["canonical_regression_sha256"] = legacy_release.document_sha256(
        payload,
        "canonical_regression_sha256",
    )
    _assert_exact_checkout(root, v4_commit)
    return payload


def _validate_regression(
    value: object,
    *,
    root: Path,
    v4_commit: str,
    targets: Sequence[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(REGRESSION_FIELDS):
        raise LifecycleFixSupplementError(f"{label} fields drifted")
    payload = dict(value)
    canonical = legacy_release._require_sha256(  # noqa: SLF001
        payload.get("canonical_regression_sha256"),
        f"{label} canonical SHA256",
    )
    if canonical != legacy_release.document_sha256(
        payload,
        "canonical_regression_sha256",
    ):
        raise LifecycleFixSupplementError(f"{label} canonical drifted")
    expected_collect = [
        sys.executable,
        "-m",
        "pytest",
        "--collect-only",
        "-q",
        *targets,
    ]
    expected_run = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "--junitxml=<CREATE_ONLY_TEMPFILE>",
        *targets,
    ]
    nodeids = payload.get("nodeids")
    passed = payload.get("passed")
    skipped = payload.get("skipped")
    if (
        payload.get("execution_commit") != v4_commit
        or payload.get("targets") != list(targets)
        or payload.get("collect_command") != expected_collect
        or payload.get("run_command") != expected_run
        or payload.get("collect_returncode") != 0
        or payload.get("run_returncode") != 0
        or not isinstance(nodeids, list)
        or not nodeids
        or len(nodeids) != len(set(nodeids))
        or payload.get("failed") != 0
        or isinstance(passed, bool)
        or not isinstance(passed, int)
        or isinstance(skipped, bool)
        or not isinstance(skipped, int)
        or passed <= 0
        or skipped < 0
        or passed + skipped != len(nodeids)
        or payload.get("interpreter") != _interpreter_binding()
        or payload.get("sanitized_python_environment")
        != _sanitized_python_environment(root)
        or payload.get("safe_import_files") != SAFE_IMPORT_MODULES
        or payload.get("test_source_files")
        != _test_source_bindings(root, v4_commit, targets)
        or payload.get("collector_source")
        != _file_binding(
            root,
            v4_commit,
            "scripts/f05_buy_e3_lifecycle_reject_fix_supplement.py",
        )
    ):
        raise LifecycleFixSupplementError(f"{label} execution evidence drifted")
    for field in (
        "junit_xml_sha256",
        "collect_stdout_sha256",
        "collect_stderr_sha256",
        "run_stdout_sha256",
        "run_stderr_sha256",
        "safe_import_stdout_sha256",
    ):
        legacy_release._require_sha256(payload.get(field), f"{label} {field}")  # noqa: SLF001
    rerun = _collect_regression(root, v4_commit, targets, f"{label} independent rerun")
    stable_fields = (
        "execution_commit",
        "targets",
        "collect_command",
        "run_command",
        "collect_returncode",
        "run_returncode",
        "nodeids",
        "passed",
        "failed",
        "skipped",
        "interpreter",
        "sanitized_python_environment",
        "safe_import_files",
        "test_source_files",
        "collector_source",
    )
    if any(payload.get(field) != rerun.get(field) for field in stable_fields):
        raise LifecycleFixSupplementError(f"{label} independent rerun drifted")
    return payload


def build_supplement(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    """Build the exact direct-v4 lifecycle-only evidence in memory."""

    root = repository_root.expanduser().resolve(strict=True)
    v4_execution = legacy_release._operational_git_identity(  # noqa: SLF001
        root,
        annotated_operational_tag,
    )
    changed = _changed_repository_files(root, v4_execution["execution_commit"])
    runtime = {path: changed[path] for path in sorted(RUNTIME_CHANGED_FILES)}
    critical = _unchanged_critical_files(root, v4_execution["execution_commit"])
    decision_ast = _unchanged_decision_ast(root, v4_execution["execution_commit"])
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    legacy_release._timestamp(timestamp, "lifecycle fix supplement timestamp")  # noqa: SLF001
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": STATUS,
        "generated_utc": timestamp,
        "v3_parent_execution": dict(PARENT_EXECUTION),
        "v4_execution": v4_execution,
        "changed_repository_files": changed,
        "changed_runtime_files": runtime,
        "focused_regression": _collect_regression(
            root,
            v4_execution["execution_commit"],
            FOCUSED_REGRESSION_TARGETS,
            "focused regression",
        ),
        "full_regression": _collect_regression(
            root,
            v4_execution["execution_commit"],
            FULL_REGRESSION_TARGETS,
            "full regression",
        ),
        "e3_unchanged": {
            "artifact_sha256": EXACT_ARTIFACT_SHA256,
            "artifact_file_sha256": dict(ARTIFACT_FILE_SHA256),
            "config_file_sha256": dict(CONFIG_FILE_SHA256),
            "critical_unchanged_repository_files": critical,
            "buy_e3_decision_ast_sha256": decision_ast,
            "action_vocabulary_seconds": list(ACTION_VOCABULARY_SECONDS),
            "verified": True,
        },
        "permissions": dict(PERMISSIONS),
    }
    payload[CANONICAL_FIELD] = legacy_release.document_sha256(
        payload,
        CANONICAL_FIELD,
    )
    return payload


def validate_supplement(path: Path, *, repository_root: Path) -> dict[str, Any]:
    """Validate a private create-only lifecycle fix supplement."""

    document = legacy_release._open_document(path, "lifecycle fix supplement")  # noqa: SLF001
    payload = document.payload
    if set(payload) != set(TOP_LEVEL_FIELDS):
        raise LifecycleFixSupplementError("lifecycle fix supplement fields drifted")
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != IDENTITY
        or payload.get("status") != STATUS
        or payload.get("v3_parent_execution") != PARENT_EXECUTION
        or payload.get("permissions") != PERMISSIONS
    ):
        raise LifecycleFixSupplementError("lifecycle fix supplement authority drifted")
    canonical = legacy_release._require_sha256(  # noqa: SLF001
        payload.get(CANONICAL_FIELD),
        "lifecycle fix supplement canonical SHA256",
    )
    if canonical != legacy_release.document_sha256(payload, CANONICAL_FIELD):
        raise LifecycleFixSupplementError("lifecycle fix supplement canonical drifted")
    execution = payload.get("v4_execution")
    if not isinstance(execution, Mapping):
        raise LifecycleFixSupplementError("lifecycle fix supplement execution is missing")
    root = repository_root.expanduser().resolve(strict=True)
    observed_execution = legacy_release._operational_git_identity(  # noqa: SLF001
        root,
        str(execution.get("annotated_operational_tag", "")),
    )
    if dict(execution) != observed_execution:
        raise LifecycleFixSupplementError("lifecycle fix supplement execution drifted")
    v4_commit = observed_execution["execution_commit"]
    changed = _changed_repository_files(root, v4_commit)
    runtime = {path: changed[path] for path in sorted(RUNTIME_CHANGED_FILES)}
    if (
        payload.get("changed_repository_files") != changed
        or payload.get("changed_runtime_files") != runtime
    ):
        raise LifecycleFixSupplementError("lifecycle fix supplement diff drifted")
    expected_e3 = {
        "artifact_sha256": EXACT_ARTIFACT_SHA256,
        "artifact_file_sha256": dict(ARTIFACT_FILE_SHA256),
        "config_file_sha256": dict(CONFIG_FILE_SHA256),
        "critical_unchanged_repository_files": _unchanged_critical_files(
            root,
            v4_commit,
        ),
        "buy_e3_decision_ast_sha256": _unchanged_decision_ast(root, v4_commit),
        "action_vocabulary_seconds": list(ACTION_VOCABULARY_SECONDS),
        "verified": True,
    }
    if payload.get("e3_unchanged") != expected_e3:
        raise LifecycleFixSupplementError("lifecycle fix supplement E3 boundary drifted")
    _validate_regression(
        payload.get("focused_regression"),
        root=root,
        v4_commit=v4_commit,
        targets=FOCUSED_REGRESSION_TARGETS,
        label="focused regression",
    )
    _validate_regression(
        payload.get("full_regression"),
        root=root,
        v4_commit=v4_commit,
        targets=FULL_REGRESSION_TARGETS,
        label="full regression",
    )
    legacy_release._timestamp(  # noqa: SLF001
        payload.get("generated_utc"),
        "lifecycle fix supplement timestamp",
    )
    return dict(payload)


def finalize_supplement(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    output_path: Path,
    generated_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    payload = build_supplement(
        repository_root=repository_root,
        annotated_operational_tag=annotated_operational_tag,
        generated_utc=generated_utc,
    )
    file_hash = legacy_release._write_exclusive(output_path, payload)  # noqa: SLF001
    validated = validate_supplement(output_path, repository_root=repository_root)
    if validated != payload:
        raise LifecycleFixSupplementError("written lifecycle fix supplement changed")
    return payload, file_hash


def supplement_binding(path: Path, *, repository_root: Path) -> dict[str, Any]:
    payload = validate_supplement(path, repository_root=repository_root)
    document = legacy_release._open_document(path, "lifecycle fix supplement")  # noqa: SLF001
    return {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "file_sha256": hashlib.sha256(document.raw).hexdigest(),
        "canonical_field": CANONICAL_FIELD,
        "canonical_sha256": payload[CANONICAL_FIELD],
        "size_bytes": len(document.raw),
        "mode": "0600",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    finalize = commands.add_parser("finalize")
    finalize.add_argument("--repository-root", type=Path, required=True)
    finalize.add_argument("--annotated-operational-tag", required=True)
    finalize.add_argument("--output", type=Path, required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--repository-root", type=Path, required=True)
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "validate":
        payload = validate_supplement(args.receipt, repository_root=args.repository_root)
        print(payload[CANONICAL_FIELD])
        return 0
    payload, file_hash = finalize_supplement(
        repository_root=args.repository_root,
        annotated_operational_tag=args.annotated_operational_tag,
        output_path=args.output,
    )
    print(f"file_sha256={file_hash}")
    print(f"canonical_sha256={payload[CANONICAL_FIELD]}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
