#!/usr/bin/env python3
"""Build the single owner BUY E3 live-safety operational successor.

The predecessor release-v3 remains immutable.  This successor preserves its
exact E3 artifact, E1/E2 non-deployment, SELL owner policy, B0 fallback, exact
no-shadow posture, and research boundary while admitting only the reviewed
execution-safety changes in its descendant source tree.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import f05_buy_e3_active_release as release_io  # noqa: E402
from scripts import f05_buy_e3_direct_owner_release_v3 as release_v3  # noqa: E402
from scripts import f05_buy_e3_lifecycle_reject_fix_supplement as semantic_guard  # noqa: E402
from scripts import f05_live_safety_locked_runtime as locked_runtime  # noqa: E402
from scripts import f05_live_safety_native_build_receipt as native_receipt_io  # noqa: E402
from strategy import boolean_cooldown_buy_e3 as runtime  # noqa: E402

SCHEMA_VERSION: Final = runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
IDENTITY: Final = runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_IDENTITY
STATUS: Final = runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_STATUS
CANONICAL_FIELD: Final = "canonical_active_release_sha256"
EXACT_ARTIFACT_SHA256: Final = runtime.DIRECT_OWNER_EXACT_ARTIFACT_SHA256
PREDECESSOR_EXECUTION: Final = runtime._LIVE_SAFETY_SUCCESSOR_PREDECESSOR_EXECUTION  # noqa: SLF001
PREDECESSOR_RELEASE: Final = runtime._LIVE_SAFETY_SUCCESSOR_PREDECESSOR_RELEASE  # noqa: SLF001
AUTHORIZATION_BASIS: Final = runtime._LIVE_SAFETY_SUCCESSOR_AUTHORIZATION_BASIS  # noqa: SLF001
CANDIDATE_SEMANTICS: Final = runtime._LIVE_SAFETY_SUCCESSOR_CANDIDATE_SEMANTICS  # noqa: SLF001
OPERATIONAL_SAFETY_CONTRACT: Final = runtime._LIVE_SAFETY_SUCCESSOR_OPERATIONAL_CONTRACT  # noqa: SLF001
RUNTIME_SOURCE_CONTRACT: Final = runtime._LIVE_SAFETY_SUCCESSOR_RUNTIME_SOURCE_CONTRACT  # noqa: SLF001
NATIVE_ABI_CONTRACT: Final = runtime._LIVE_SAFETY_SUCCESSOR_NATIVE_ABI_CONTRACT  # noqa: SLF001
NO_SHADOW_RUNTIME_CONTRACT: Final = runtime._DIRECT_OWNER_V3_NO_SHADOW_RUNTIME_CONTRACT  # noqa: SLF001
PENDING_CURRENT_RUNTIME_EVIDENCE: Final = runtime._LIVE_SAFETY_SUCCESSOR_PENDING_RUNTIME_EVIDENCE  # noqa: SLF001
ROLLBACK: Final = runtime._LIVE_SAFETY_SUCCESSOR_ROLLBACK  # noqa: SLF001
EVIDENCE_BOUNDARY: Final = runtime._LIVE_SAFETY_SUCCESSOR_EVIDENCE_BOUNDARY  # noqa: SLF001
SCOPE: Final = {
    "strategy_action_scope": {
        "side": "BUY",
        "candidate": "E3",
        "trigger": "exposure_increasing_executed_fill",
        "output": "total_cooldown",
        "artifact_and_action_authority_unchanged": True,
    },
    "operational_safety_scope": {
        "sides": ["BUY", "SELL"],
        "shared_execution_state_corrections": True,
        "new_economic_strategy_arm": False,
        "authorized_by_current_owner_deploy_fix_directive": True,
    },
}

# A deleted tracked path has no blob in the successor tree.  Keep the existing
# two-digest receipt schema by binding deletions to the canonical empty-content
# tombstone; the exact execution tree and the predecessor-to-successor diff
# independently prove that the path is absent.
_DELETED_GIT_BLOB_SHA1: Final = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
_DELETED_FILE_SHA256: Final = (
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)
B0_FALLBACK_PATH: Final = "strategy/maker_engine.py"
B0_FALLBACK_ALLOWED_METHODS: Final = frozenset({("MakerEngine", "sync_position")})


def _config_pair(
    *,
    predecessor_disabled_path: Path,
    predecessor_active_path: Path,
    disabled_path: Path,
    active_path: Path,
) -> dict[str, Any]:
    predecessor_disabled = release_v3._open_config(  # noqa: SLF001
        predecessor_disabled_path, "predecessor disabled config"
    )
    predecessor_active = release_v3._open_config(  # noqa: SLF001
        predecessor_active_path, "predecessor active config"
    )
    disabled = release_v3._open_config(disabled_path, "successor disabled config")  # noqa: SLF001
    active = release_v3._open_config(active_path, "successor active config")  # noqa: SLF001
    if hashlib.sha256(predecessor_disabled.raw).hexdigest() != (
        "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204"
    ) or hashlib.sha256(predecessor_active.raw).hexdigest() != (
        "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
    ):
        raise LiveSafetySuccessorError("predecessor no-shadow config pair drifted")
    expected_changes = ["api.timeout_s", "strategy.spread_cap_mode"]
    if release_v3._leaf_diff(predecessor_disabled.payload, disabled.payload) != expected_changes:  # noqa: SLF001
        raise LiveSafetySuccessorError("successor disabled config changed outside safety fields")
    if release_v3._leaf_diff(predecessor_active.payload, active.payload) != expected_changes:  # noqa: SLF001
        raise LiveSafetySuccessorError("successor active config changed outside safety fields")
    if release_v3._leaf_diff(disabled.payload, active.payload) != [  # noqa: SLF001
        "strategy.buy_e3_cooldown_policy_enabled"
    ]:
        raise LiveSafetySuccessorError("successor pair differs outside BUY E3 enablement")
    for label, document in (("disabled", disabled), ("active", active)):
        if release_v3._path_value(document.payload, "api.timeout_s") != 5.0:  # noqa: SLF001
            raise LiveSafetySuccessorError(f"{label} config lacks explicit REST timeout")
        if release_v3._path_value(  # noqa: SLF001
            document.payload, "strategy.spread_cap_mode"
        ) != "pause_exposure":
            raise LiveSafetySuccessorError(f"{label} config lacks pause_exposure")
        for path in release_v3.REQUIRED_FALSE_CONFIG_PATHS:
            if release_v3._path_value(document.payload, path) is not False:  # noqa: SLF001
                raise LiveSafetySuccessorError(f"{label} config did not disable {path}")
    return {
        "schema_version": "f05_buy_e3_live_safety_successor_config_pair.v1",
        "status": "explicit_timeout_pause_exposure_no_shadow_pair",
        "predecessor": {
            "disabled_file_sha256": hashlib.sha256(predecessor_disabled.raw).hexdigest(),
            "active_file_sha256": hashlib.sha256(predecessor_active.raw).hexdigest(),
        },
        "disabled": release_v3._config_file_binding(disabled),  # noqa: SLF001
        "active": release_v3._config_file_binding(active),  # noqa: SLF001
        "predecessor_to_successor_semantic_changes": expected_changes,
        "explicit_safety_values": {
            "api.timeout_s": 5.0,
            "strategy.spread_cap_mode": "pause_exposure",
        },
        "active_disabled_only_difference": "strategy.buy_e3_cooldown_policy_enabled",
        "required_false_paths": list(release_v3.REQUIRED_FALSE_CONFIG_PATHS),
        "external_shadow_only_marker_inert": True,
        "release_fields_present_in_yaml": False,
    }


class LiveSafetySuccessorError(RuntimeError):
    """Raised when the operational successor cannot be frozen exactly."""


def _git_file_binding(root: Path, commit: str, path: str) -> dict[str, str]:
    try:
        blob = subprocess.run(
            ("git", "rev-parse", f"{commit}:{path}"),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=20.0,
        ).stdout.strip()
        raw = subprocess.run(
            ("git", "show", f"{commit}:{path}"),
            cwd=root,
            check=True,
            capture_output=True,
            timeout=20.0,
        ).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise LiveSafetySuccessorError(f"cannot bind successor source: {path}") from exc
    return {"git_blob_sha1": blob, "file_sha256": hashlib.sha256(raw).hexdigest()}


def _changed_repository_files(root: Path, execution_commit: str) -> dict[str, Any]:
    try:
        rows = subprocess.run(
            (
                "git",
                "diff",
                "--name-status",
                PREDECESSOR_EXECUTION["execution_commit"],
                execution_commit,
            ),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=30.0,
        ).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise LiveSafetySuccessorError("cannot enumerate successor source delta") from exc
    statuses: dict[str, str] = {}
    for row in rows:
        fields = row.split("\t")
        if len(fields) != 2 or fields[0] not in {"A", "M", "D"}:
            raise LiveSafetySuccessorError(
                "successor delta must contain only added, modified, or deleted tracked files"
            )
        statuses[fields[1]] = fields[0]
    paths = set(statuses)
    required = set(RUNTIME_SOURCE_CONTRACT["required_repository_paths"])
    safety_runtime = {
        "strategy/inventory_manager.py",
        "strategy/order_manager.py",
        "live/main.py",
        "live/run.sh",
    }
    protected_changed = (required | safety_runtime).intersection(paths)
    if (
        not required.intersection(paths)
        or not safety_runtime.issubset(paths)
        or any(statuses[path] == "D" for path in protected_changed)
    ):
        raise LiveSafetySuccessorError("successor delta lacks required safety runtime changes")
    return {
        path: (
            {
                "git_blob_sha1": _DELETED_GIT_BLOB_SHA1,
                "file_sha256": _DELETED_FILE_SHA256,
            }
            if statuses[path] == "D"
            else _git_file_binding(root, execution_commit, path)
        )
        for path in sorted(paths)
    }


def _semantic_ast_sha256_redacting_method_bodies(
    source: bytes,
    *,
    allowed_methods: frozenset[tuple[str, str]],
) -> str:
    """Hash the full AST while redacting only allowlisted method bodies."""

    try:
        tree = ast.parse(source.decode("utf-8"))
    except (SyntaxError, UnicodeDecodeError) as exc:
        raise LiveSafetySuccessorError("B0 fallback AST cannot be parsed") from exc

    classes = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
    }
    for class_name, method_name in sorted(allowed_methods):
        class_node = classes.get(class_name)
        if class_node is None:
            raise LiveSafetySuccessorError(
                f"allowlisted B0 fallback class is missing: {class_name}"
            )
        matches = [
            node
            for node in class_node.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == method_name
        ]
        if len(matches) != 1:
            raise LiveSafetySuccessorError(
                "allowlisted B0 fallback method must exist exactly once: "
                f"{class_name}.{method_name}"
            )
        # Keep the method itself, including decorators, sync/async kind, complete
        # argument signature, return annotation, and type comment.  Only the
        # reviewed reconciliation implementation body is allowed to advance.
        matches[0].body = [ast.Pass()]

    canonical_ast = ast.dump(
        tree,
        annotate_fields=True,
        include_attributes=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical_ast).hexdigest()


def _protected_semantics(root: Path, execution_commit: str) -> dict[str, Any]:
    mechanics_commit = "e0804e1dd8b199e2dc04d36c0dcd5f27e9fc83d5"
    changed_after_mechanics = subprocess.run(
        ("git", "diff", "--name-only", mechanics_commit, execution_commit),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=20.0,
    ).stdout.splitlines()
    forbidden_prefixes = (
        "research/families/f03_causal_13_head/",
        "research/families/f05_fill_quality_quote_ev/docs/causal_multichannel_window_boolean_cooldown_owner_buy_e3_backtest_mechanics_",
        "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_backtest_mechanics_",
    )
    if any(path.startswith(forbidden_prefixes) for path in changed_after_mechanics):
        raise LiveSafetySuccessorError("successor modified frozen mechanics or F03 after e080")

    def committed(path: str, commit: str = execution_commit) -> bytes:
        return subprocess.run(
            ("git", "show", f"{commit}:{path}"),
            cwd=root,
            check=True,
            capture_output=True,
            timeout=20.0,
        ).stdout

    e3_current = committed("strategy/boolean_cooldown_buy_e3.py")
    e3_mechanics = committed("strategy/boolean_cooldown_buy_e3.py", mechanics_commit)
    current_ast = semantic_guard._semantic_ast_hashes(e3_current)  # noqa: SLF001
    mechanics_ast = semantic_guard._semantic_ast_hashes(e3_mechanics)  # noqa: SLF001
    if current_ast != mechanics_ast:
        raise LiveSafetySuccessorError("protected BUY E3 decision AST changed after e080")
    b0_current = committed(B0_FALLBACK_PATH)
    b0_mechanics = committed(B0_FALLBACK_PATH, mechanics_commit)
    if _semantic_ast_sha256_redacting_method_bodies(
        b0_current,
        allowed_methods=B0_FALLBACK_ALLOWED_METHODS,
    ) != _semantic_ast_sha256_redacting_method_bodies(
        b0_mechanics,
        allowed_methods=B0_FALLBACK_ALLOWED_METHODS,
    ):
        raise LiveSafetySuccessorError(
            "protected B0 fallback AST changed outside allowlisted reconciliation methods"
        )
    paths = {
        "sell_owner_policy_file_sha256": "strategy/boolean_cooldown_live.py",
        "e1_e2_definition_file_sha256": (
            "research/families/f05_fill_quality_quote_ev/audit/"
            "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1.py"
        ),
    }
    output = {
        "e3_decision_ast_sha256": hashlib.sha256(
            json.dumps(current_ast, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest(),
        "b0_fallback_file_sha256": hashlib.sha256(b0_current).hexdigest(),
        "f03_sources_unchanged_after_e080": True,
        "frozen_mechanics_evidence_bytes_referenced_not_modified": True,
        "current_head_not_mechanics_authority": True,
    }
    for field, path in paths.items():
        current = committed(path)
        if current != committed(path, mechanics_commit):
            raise LiveSafetySuccessorError(f"protected semantics changed after e080: {path}")
        output[field] = hashlib.sha256(current).hexdigest()
    return output


def _predecessor(path: Path, *, artifact_paths: Mapping[str, Path]) -> dict[str, Any]:
    document = release_io._open_document(path, "owner BUY E3 release-v3")  # noqa: SLF001
    binding = {
        "schema_version": document.payload.get("schema_version"),
        "status": document.payload.get("status"),
        "file_sha256": hashlib.sha256(document.raw).hexdigest(),
        "canonical_field": CANONICAL_FIELD,
        "canonical_sha256": document.payload.get(CANONICAL_FIELD),
        "size_bytes": len(document.raw),
        "mode": "0600",
    }
    if binding != PREDECESSOR_RELEASE:
        raise LiveSafetySuccessorError("immutable release-v3 predecessor identity drifted")
    roles = document.payload.get("exact_artifact", {}).get("roles", {})
    expected_files = {
        role: hashlib.sha256(Path(path_value).read_bytes()).hexdigest()
        for role, path_value in artifact_paths.items()
    }
    runtime._validate_active_release(  # noqa: SLF001
        document.payload,
        expected_canonical_sha256=str(binding["canonical_sha256"]),
        expected_artifact_sha256=EXACT_ARTIFACT_SHA256,
        expected_manifest_file_sha256=expected_files["manifest"],
        expected_policy_file_sha256=expected_files["policy"],
        expected_predicate_bundle_file_sha256=expected_files["predicate_bundle"],
    )
    if any(roles.get(role, {}).get("file_sha256") != digest for role, digest in expected_files.items()):
        raise LiveSafetySuccessorError("exact E3 artifact differs from release-v3")
    return dict(document.payload)


def build_live_safety_successor(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    predecessor_release_path: Path,
    predecessor_disabled_config_path: Path,
    predecessor_active_config_path: Path,
    disabled_config_path: Path,
    active_config_path: Path,
    native_build_receipt_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    execution = release_io._operational_git_identity(  # noqa: SLF001
        root, annotated_operational_tag
    )
    if execution["execution_commit"] == PREDECESSOR_EXECUTION["execution_commit"]:
        raise LiveSafetySuccessorError("successor execution did not advance")
    ancestry = subprocess.run(
        (
            "git",
            "merge-base",
            "--is-ancestor",
            PREDECESSOR_EXECUTION["execution_commit"],
            execution["execution_commit"],
        ),
        cwd=root,
        check=False,
        capture_output=True,
        timeout=20.0,
    )
    if ancestry.returncode != 0:
        raise LiveSafetySuccessorError("successor does not descend from release-v3")
    predecessor = _predecessor(predecessor_release_path, artifact_paths=artifact_paths)
    exact_artifact = {
        "artifact_sha256": EXACT_ARTIFACT_SHA256,
        "roles": dict(predecessor["exact_artifact"]["roles"]),
    }
    config_pair = _config_pair(
        predecessor_disabled_path=predecessor_disabled_config_path,
        predecessor_active_path=predecessor_active_config_path,
        disabled_path=disabled_config_path,
        active_path=active_config_path,
    )
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    release_io._timestamp(timestamp, "live safety successor timestamp")  # noqa: SLF001
    native_document = release_io._open_document(  # noqa: SLF001
        native_build_receipt_path, "Linux x86_64 native build receipt"
    )
    native_receipt = native_document.payload
    native_fields = {
        "schema_version",
        "status",
        "generated_utc",
        "execution",
        "platform",
        "python_minor",
        "python",
        "soabi",
        "compiler",
        "pybind11_version",
        "dependency_lock",
        "installed_distribution_lock",
        "native_sources",
        "wheel",
        "module",
        "abi_contract",
        "parity_tests",
        "parity_smoke_passed",
        "canonical_native_build_sha256",
    }
    byte_fields = {"path", "sha256", "size_bytes"}
    python_fields = byte_fields | {"version"}
    module = native_receipt.get("module")
    wheel = native_receipt.get("wheel")
    python_identity = native_receipt.get("python")
    abi_contract = native_receipt.get("abi_contract")
    native_sources = native_receipt.get("native_sources")
    dependency_lock = native_receipt.get("dependency_lock")
    installed_lock = native_receipt.get("installed_distribution_lock")
    dependency_fields = {
        "runtime_lock_path",
        "runtime_lock_file_sha256",
        "runtime_lock_canonical_sha256",
        "wheelhouse_path",
        "wheelhouse_manifest_path",
        "wheelhouse_manifest_file_sha256",
        "wheelhouse_canonical_sha256",
    }
    installed_fields = {
        "install_receipt_path",
        "install_receipt_file_sha256",
        "install_receipt_canonical_sha256",
        "root_wheel_path",
        "root_wheel_sha256",
        "native_wheel_path",
        "native_wheel_sha256",
        "interpreter",
        "installed_distributions",
        "installed_record_aggregate_sha256",
    }
    interpreter_fields = {
        "implementation",
        "version",
        "version_info",
        "cache_tag",
        "soabi",
        "abiflags",
        "sysconfig_platform",
        "system",
        "machine",
        "compiler",
        "openssl_runtime",
        "openssl_version_number",
        "executable_sha256",
        "executable_size_bytes",
        "base_executable_sha256",
        "base_executable_size_bytes",
        "is_virtual_environment",
    }
    interpreter = installed_lock.get("interpreter") if isinstance(installed_lock, Mapping) else None
    sha_fields = (
        (dependency_lock, "runtime_lock_file_sha256"),
        (dependency_lock, "runtime_lock_canonical_sha256"),
        (dependency_lock, "wheelhouse_manifest_file_sha256"),
        (dependency_lock, "wheelhouse_canonical_sha256"),
        (installed_lock, "install_receipt_file_sha256"),
        (installed_lock, "install_receipt_canonical_sha256"),
        (installed_lock, "root_wheel_sha256"),
        (installed_lock, "native_wheel_sha256"),
        (installed_lock, "installed_record_aggregate_sha256"),
    )
    if (
        set(native_receipt) != native_fields
        or native_receipt.get("schema_version") != native_receipt_io.SCHEMA
        or native_receipt.get("status") != native_receipt_io.STATUS
        or native_receipt.get("execution") != execution
        or native_receipt.get("platform") != "linux_x86_64"
        or native_receipt.get("python_minor") != "3.12"
        or not str(native_receipt.get("soabi", "")).startswith("cpython-312-")
        or not isinstance(module, Mapping)
        or set(module) != byte_fields
        or not isinstance(wheel, Mapping)
        or set(wheel) != byte_fields
        or not isinstance(python_identity, Mapping)
        or set(python_identity) != python_fields
        or not str(python_identity.get("version", "")).startswith("3.12.")
        or not isinstance(native_sources, Mapping)
        or set(native_sources) != set(native_receipt_io.NATIVE_SOURCES)
        or any(
            not isinstance(binding, Mapping)
            or set(binding) != {"sha256", "size_bytes"}
            or len(str(binding.get("sha256", ""))) != 64
            or type(binding.get("size_bytes")) is not int
            or binding.get("size_bytes", 0) <= 0
            for binding in native_sources.values()
        )
        or abi_contract
        != {
            "schema_version": "narrowgate_native_live_safety_abi.v1",
            "required_apis": [
                "compute_quote_core_live",
                "compute_live_routing_decision",
                "SignalFeatureEngine",
                "SIGNAL_FEATURE_NAMES",
                "TradeBarAggregator",
            ],
            "required_quote_fields": {
                "QuoteFlags": ["delta_cap", "final_compressed", "cap_exposure_block"],
                "SideQuoteContext": ["cap_exposure_block"],
            },
            "validated": True,
        }
        or native_receipt.get("parity_tests") != list(native_receipt_io.PARITY_TESTS)
        or native_receipt.get("parity_smoke_passed") is not True
        or native_receipt.get("canonical_native_build_sha256")
        != release_io.document_sha256(native_receipt, "canonical_native_build_sha256")
        or not isinstance(dependency_lock, Mapping)
        or set(dependency_lock) != dependency_fields
        or not isinstance(installed_lock, Mapping)
        or set(installed_lock) != installed_fields
        or not isinstance(interpreter, Mapping)
        or set(interpreter) != interpreter_fields
        or interpreter.get("implementation") != "cpython"
        or interpreter.get("version_info", [])[:2] != list(locked_runtime.REQUIRED_PYTHON)
        or interpreter.get("is_virtual_environment") is not True
        or interpreter.get("soabi") != native_receipt.get("soabi")
        or interpreter.get("version") != python_identity.get("version")
        or interpreter.get("compiler") != native_receipt.get("compiler")
        or interpreter.get("executable_sha256") != python_identity.get("sha256")
        or interpreter.get("executable_size_bytes")
        != python_identity.get("size_bytes")
        or any(
            not isinstance(mapping, Mapping)
            or len(str(mapping.get(field, ""))) != 64
            for mapping, field in sha_fields
        )
        or installed_lock.get("native_wheel_sha256") != wheel.get("sha256")
        or not isinstance(installed_lock.get("installed_distributions"), list)
        or not installed_lock.get("installed_distributions")
    ):
        raise LiveSafetySuccessorError("native build receipt is not exact-tag authority")
    release_io._timestamp(str(native_receipt["generated_utc"]), "native build timestamp")  # noqa: SLF001
    for label, binding in (
        ("native module", module),
        ("native wheel", wheel),
        ("native Python", python_identity),
    ):
        if (
            len(str(binding["sha256"])) != 64
            or type(binding["size_bytes"]) is not int
            or binding["size_bytes"] <= 0
            or not str(binding["path"]).startswith("/")
        ):
            raise LiveSafetySuccessorError(f"{label} byte identity drifted")
    native_build = {
        "schema_version": native_receipt["schema_version"],
        "status": native_receipt["status"],
        "file_sha256": hashlib.sha256(native_document.raw).hexdigest(),
        "canonical_sha256": native_receipt["canonical_native_build_sha256"],
        "module_sha256": native_receipt["module"]["sha256"],
        "wheel_sha256": native_receipt["wheel"]["sha256"],
        "soabi": native_receipt["soabi"],
        "python_minor": native_receipt["python_minor"],
        "platform": native_receipt["platform"],
        "runtime_lock_file_sha256": dependency_lock["runtime_lock_file_sha256"],
        "runtime_lock_path": dependency_lock["runtime_lock_path"],
        "runtime_lock_canonical_sha256": dependency_lock[
            "runtime_lock_canonical_sha256"
        ],
        "wheelhouse_manifest_file_sha256": dependency_lock[
            "wheelhouse_manifest_file_sha256"
        ],
        "wheelhouse_path": dependency_lock["wheelhouse_path"],
        "wheelhouse_canonical_sha256": dependency_lock[
            "wheelhouse_canonical_sha256"
        ],
        "install_receipt_path": installed_lock["install_receipt_path"],
        "install_receipt_file_sha256": installed_lock[
            "install_receipt_file_sha256"
        ],
        "install_receipt_canonical_sha256": installed_lock[
            "install_receipt_canonical_sha256"
        ],
        "root_wheel_sha256": installed_lock["root_wheel_sha256"],
        "root_wheel_path": installed_lock["root_wheel_path"],
        "native_wheel_path": installed_lock["native_wheel_path"],
        "installed_record_aggregate_sha256": installed_lock[
            "installed_record_aggregate_sha256"
        ],
        "interpreter": dict(interpreter),
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": STATUS,
        "generated_utc": timestamp,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": dict(AUTHORIZATION_BASIS),
        "scope": dict(SCOPE),
        "execution": dict(execution),
        "predecessor_runtime_authority": {
            "release": dict(PREDECESSOR_RELEASE),
            "execution": dict(PREDECESSOR_EXECUTION),
        },
        "exact_artifact": exact_artifact,
        "candidate_semantics": dict(CANDIDATE_SEMANTICS),
        "protected_semantics": _protected_semantics(
            root, str(execution["execution_commit"])
        ),
        "config_pair": config_pair,
        "operational_safety_contract": dict(OPERATIONAL_SAFETY_CONTRACT),
        "runtime_source_contract": dict(RUNTIME_SOURCE_CONTRACT),
        "changed_repository_files": _changed_repository_files(
            root, str(execution["execution_commit"])
        ),
        "native_abi_contract": dict(NATIVE_ABI_CONTRACT),
        "native_build": native_build,
        "no_shadow_runtime_contract": dict(NO_SHADOW_RUNTIME_CONTRACT),
        "pending_current_runtime_evidence": dict(PENDING_CURRENT_RUNTIME_EVIDENCE),
        "rollback": dict(ROLLBACK),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = release_io.document_sha256(payload, CANONICAL_FIELD)
    return payload


def finalize_live_safety_successor(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_live_safety_successor(**kwargs)
    file_sha256 = release_io._write_exclusive(output_path, payload)  # noqa: SLF001
    return payload, file_sha256


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--annotated-operational-tag", required=True)
    parser.add_argument("--predecessor-release", type=Path, required=True)
    parser.add_argument("--artifact-manifest", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--predicate-bundle", type=Path, required=True)
    parser.add_argument("--predecessor-disabled-config", type=Path, required=True)
    parser.add_argument("--predecessor-active-config", type=Path, required=True)
    parser.add_argument("--disabled-config", type=Path, required=True)
    parser.add_argument("--active-config", type=Path, required=True)
    parser.add_argument("--native-build-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload, file_sha256 = finalize_live_safety_successor(
        output_path=args.output,
        repository_root=args.repository_root,
        annotated_operational_tag=args.annotated_operational_tag,
        artifact_paths={
            "manifest": args.artifact_manifest,
            "policy": args.policy,
            "predicate_bundle": args.predicate_bundle,
        },
        predecessor_release_path=args.predecessor_release,
        predecessor_disabled_config_path=args.predecessor_disabled_config,
        predecessor_active_config_path=args.predecessor_active_config,
        disabled_config_path=args.disabled_config,
        active_config_path=args.active_config,
        native_build_receipt_path=args.native_build_receipt,
    )
    print(
        f"{payload[CANONICAL_FIELD]} {file_sha256} {args.output.expanduser().absolute()}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
