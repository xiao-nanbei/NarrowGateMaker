"""Transactional planner for the frozen owner-selected BUY E3 runtime.

The default command only writes a deterministic plan.  Any SSH or remote
mutation requires a named phase, an explicit authorization flag, and a secret
whose SHA256 was frozen in the plan.  The planner is external to the c170493e
runtime and never changes the E3 algorithm or the v1 deployment gate.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import importlib.util
import json
import os
import shlex
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live.runtime_policy import (  # noqa: E402
    F05_BUY_E3_OWNER_OVERRIDE_ENV,
    f05_buy_e3_runtime_policy,
)

try:  # noqa: E402
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
    )
except ImportError:
    external_gate = os.environ.get("NARROWGATE_BUY_E3_GATE_V2_PATH", "").strip()
    if not external_gate:
        raise
    specification = importlib.util.spec_from_file_location(
        "narrowgate_buy_e3_gate_v2", external_gate
    )
    if specification is None or specification.loader is None:
        raise ImportError("cannot load external BUY E3 deployment gate amendment") from None
    gate_v2 = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(gate_v2)


PLAN_SCHEMA = "f05_buy_e3_owner_transactional_deploy_plan.v1"
RECEIPT_SCHEMA = "f05_buy_e3_owner_transactional_deploy_receipt.v3"
PREFLIGHT_SCHEMA = "f05_buy_e3_owner_isolated_config_preflight.v2"
RUNTIME_IDENTITY_SCHEMA = "narrowgate_live_runtime_identity.v1"
STARTUP_ATTESTATION_SCHEMA = "narrowgate_buy_e3_startup_attestation.v2"
RUNNING_CHECKOUT_SCHEMA = "narrowgate_running_checkout_identity.v2"
FILL_COOLDOWN_STATE_SCHEMA = "narrowgate_fill_cooldown_state.v2"
INTERPRETER_IDENTITY_SCHEMA = "narrowgate_interpreter_identity.v1"
NATIVE_RUNTIME_IDENTITY_SCHEMA = "narrowgate_native_runtime_identity.v1"
RUNTIME_IDENTITY_BINDING_SCHEMA = "narrowgate_runtime_identity_binding.v1"
RUNTIME_ATTESTATION_CONTRACT_SCHEMA = (
    "narrowgate_runtime_written_startup_attestation_contract.v1"
)
POINTER_SCHEMA = "narrowgate_live_remote_pointer.v1"
ACTIVE_POINTER_STATUS = "current_active"

PHASES = ("disabled-deploy", "activate", "rollback-primary", "rollback-deep")
MUTATING_PHASES = frozenset(PHASES)
PHASE_COMPLETE = "phase_complete"
PHASE_FAILED_CLOSED = "phase_failed_closed"
FAILURE_CLASSES = frozenset(
    {
        "command_returncode_nonzero",
        "command_runner_exception",
        "old_pid_probe_invalid",
        "process_probe_invalid",
        "process_identity_invalid",
        "process_authority_or_deadline_mismatch",
        "fresh_pid_required",
        "disabled_phase_receipt_invalid",
        "disabled_process_handoff_mismatch",
        "runtime_identity_invalid",
        "receipt_write_failed",
        "receipt_validation_failed",
        "phase_contract_validation_failed",
    }
)
RECEIPT_EVIDENCE_BOUNDARY = {
    "validation_read": False,
    "sealed_holdout_read": False,
    "economic_arms_run": False,
    "economic_values_read": False,
    "stdout_or_stderr_embedded": False,
}
RECEIPT_PERMISSIONS = {
    "required_mode": "0600",
    "immutable_create_only": True,
}
RECEIPT_AUTHORITY = {
    "classification": "local_unsigned_structural_receipt",
    "cryptographic_signature_present": False,
    "standalone_activation_evidence": False,
}
TRANSACTION_CONTRACT = {
    "default_mode": "dry_run_plan",
    "remote_mutation_requires_explicit_phase": True,
    "remote_mutation_requires_token": True,
    "token_transport": "stdin_or_owner_only_file_descriptor",
    "plaintext_cli_token_allowed": False,
    "external_narrowgate_live_config_required": True,
    "activation_restart_only": True,
    "sighup_activation_allowed": False,
    "activation_requires_same_plan_disabled_receipt": True,
    "activation_requires_runtime_written_startup_attestation": True,
    "runtime_identity_file_read_after_process_probe": True,
    "startup_attestation_expected_value_echo_allowed": False,
    "local_phase_receipts_structural_only": True,
    "receipt_output_reserved_before_remote_mutation": True,
    "receipt_commit_no_replace": True,
    "receipt_failure_requires_automatic_rollback": True,
    "mutation_marked_before_remote_command": True,
    "stop_failure_requires_probe_and_rollback": True,
    "rollback_requires_fresh_pid": True,
    "rollback_buy_deadline_identity": "B0",
    "rollback_imports_e3_deadline": False,
    "pre_stop_isolated_disabled_and_active_preflight": True,
    "staged_tools_content_addressed_read_only": True,
    "staged_tool_hash_and_exec_same_shell": True,
    "planner_host_bound_spool_existence_deferred": True,
    "remote_host_bound_spool_preflight_mandatory": True,
}
PLAN_EVIDENCE_BOUNDARY = {
    "validation_read": False,
    "sealed_holdout_read": False,
    "economic_arms_run": False,
    "hypothetical_live_actions_scored": False,
}
REMOTE_OVERRIDE_ENV = (
    "NARROWGATE_LIVE_REMOTE",
    "NARROWGATE_LIVE_REMOTE_POINTER",
)
STRICT_SSH_OPTIONS = (
    "BatchMode=yes",
    "StrictHostKeyChecking=yes",
)
HOST_BOUND_SPOOL_REMOTE_CHECKS = (
    "allowlisted_root_exists",
    "allowlisted_root_is_directory",
    "allowlisted_root_is_not_symlink",
    "journal_and_epoch_roots_are_strict_children_of_same_allowlisted_root",
)


class BuyE3TransactionalDeployError(RuntimeError):
    """Raised when a deployment plan or transaction cannot fail closed."""


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
PreflightRunner = Callable[[Path, Path, bool], Mapping[str, Any]]

_PROCESS_IDENTITY_FIELDS = frozenset(
    {
        "schema_version",
        "captured_utc",
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
        "execution_commit",
        "execution_tree",
        "runtime_identity_file_sha256",
        "startup_attestation_sha256",
        "artifact_sha256",
        "runtime_code_sha256",
        "buy_e3_enabled",
        "owner_override_effective",
        "initial_buy_deadline_identity",
        "e3_deadline_imported",
        "canonical_process_identity_sha256",
    }
)
_RESULT_FIELDS = frozenset(
    {
        "label",
        "command_sha256",
        "returncode",
        "stdout_sha256",
        "stderr_sha256",
        "observed_pid",
        "process_identity_sha256",
        "runtime_identity_file_sha256",
        "startup_attestation_sha256",
    }
)
_PHASE_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "plan_sha256",
        "phase",
        "status",
        "remote_mutation_authorized",
        "phase_authorization_token_sha256",
        "transaction_contract_sha256",
        "expected_commands",
        "expected_automatic_rollback_commands",
        "results",
        "mutation_started",
        "disabled_phase_receipt_binding",
        "pre_stop_disabled_process_identity",
        "pre_stop_disabled_startup_attestation",
        "actual_startup_attestation",
        "actual_process_identity",
        "stop_failure_probe_result",
        "rollback_attempted",
        "rollback_status",
        "rollback_failure_class",
        "rollback_process_identity",
        "failure_class",
        "permissions",
        "evidence_boundary",
        "evidence_authority",
        "canonical_receipt_sha256",
    }
)
_RUNTIME_IDENTITY_BINDING_FIELDS = frozenset(
    {
        "schema_version",
        "authority",
        "evidence_classification",
        "cryptographic_signature_present",
        "runtime_identity_path",
        "runtime_identity_file_sha256",
        "runtime_identity_schema_version",
        "pid",
        "pid_start_ticks",
        "process_identity_sha256",
        "config_path",
        "config_sha256",
        "artifact_sha256",
        "buy_e3_enabled",
        "owner_override_effective",
        "startup_attestation",
        "startup_attestation_sha256",
        "canonical_runtime_identity_binding_sha256",
    }
)
_STARTUP_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "attested_at_utc",
        "fill_cooldown_state",
        "running_checkout",
        "loaded_module_origins",
        "interpreter_identity",
        "native_runtime_identity",
        "gates",
        "errors",
    }
)
_STARTUP_GATE_FIELDS = frozenset(
    {
        "fill_cooldown_state_available",
        "fill_cooldown_state_schema_v2",
        "buy_deadline_identity_is_b0",
        "buy_remaining_ms_is_zero",
        "buy_e3_deadline_not_imported",
        "git_toplevel_matches_repo",
        "git_pre_snapshot_available",
        "git_pre_snapshot_stable",
        "git_pre_worktree_clean",
        "runtime_source_manifest_available",
        "runtime_files_match_head",
        "loaded_module_origins_available",
        "loaded_module_origins_under_repo",
        "loaded_module_origins_match_runtime_sources",
        "interpreter_identity_available",
        "interpreter_identity_stable",
        "native_runtime_matches_initial_identity",
        "native_runtime_contract_valid",
        "native_runtime_identity_available",
        "native_runtime_identity_stable",
        "git_post_snapshot_available",
        "git_post_snapshot_stable",
        "git_post_worktree_clean",
        "git_snapshot_stable",
        "safe_to_start_live_loops",
    }
)
_RUNNING_CHECKOUT_FIELDS = frozenset(
    {
        "schema_version",
        "git_commit",
        "git_tree",
        "git_worktree_clean",
        "pre_snapshot",
        "post_snapshot",
        "stable_snapshot",
        "runtime_source_file_count",
        "runtime_source_manifest_sha256",
        "runtime_source_files",
    }
)
_GIT_SNAPSHOT_FIELDS = frozenset(
    {
        "commit",
        "tree",
        "status_porcelain_sha256",
        "status_entry_count",
        "worktree_clean",
        "snapshot_internally_stable",
    }
)
_STABLE_GIT_SNAPSHOT_FIELDS = frozenset(
    {
        "pre_snapshot_internally_stable",
        "post_snapshot_internally_stable",
        "commit_identical",
        "tree_identical",
        "status_identical",
        "runtime_files_match_head",
        "stable",
    }
)
_RUNTIME_SOURCE_FILE_FIELDS = frozenset(
    {
        "path",
        "working_file_sha256",
        "head_blob_sha256",
        "working_size_bytes",
        "head_blob_size_bytes",
        "matches_head_blob",
    }
)
_LOADED_RUNTIME_MODULE_ROLES = frozenset(
    {
        "live_main",
        "live_config",
        "live_runtime_policy",
        "live_ws_handler",
        "maker_engine",
        "boolean_cooldown_live",
        "boolean_cooldown_buy_e3",
    }
)
_LOADED_RUNTIME_MODULE_FIELDS = frozenset(
    {
        "module_name",
        "origin_path",
        "repository_relative_path",
        "source_sha256",
    }
)
_FILE_BYTE_IDENTITY_FIELDS = frozenset(
    {"reported_path", "resolved_path", "sha256", "size_bytes"}
)
_INTERPRETER_IDENTITY_FIELDS = frozenset(
    {"schema_version", "version", "before", "after", "stable"}
)
_NATIVE_RUNTIME_IDENTITY_FIELDS = frozenset(
    {
        "schema_version",
        "profile",
        "platform",
        "enabled",
        "reported_module_path",
        "loaded_module_origin_path",
        "before",
        "after",
        "stable",
    }
)
_HOST_BOUND_STORAGE_GATE_FIELDS = frozenset(
    {
        "profile",
        "status",
        "deferred_on_planner_host",
        "mandatory_remote_preflight",
        "allowlisted_root",
        "journal_root",
        "prospective_epoch_root",
        "required_remote_checks",
    }
)
_COMMAND_FIELDS = frozenset(
    {"label", "argv", "command_sha256", "mutates_remote", "after_stop"}
)
_EXTERNAL_PACKAGE_ROLES = (
    "deploy_script",
    "gate_amendment",
    "artifact_manifest",
    "policy",
    "predicate_bundle",
    "disabled_config",
    "active_config",
)
_PLAN_CORE_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "planner_repository_root",
        "execution",
        "runtime_sources",
        "artifact",
        "external_tools_and_package",
        "configs",
        "isolated_preflights",
        "active_pointer",
        "ssh",
        "host",
        "remote",
        "rollback_identities",
        "phase_token_sha256",
        "phases",
        "transaction_contract",
        "runtime_attestation_contract",
        "evidence_boundary",
    }
)
_PLAN_BASE_FIELDS = _PLAN_CORE_FIELDS | frozenset(
    {"plan_core_sha256", "canonical_plan_sha256"}
)
_PLAN_ACTIVATION_FIELDS = frozenset(
    {"activation_gate", "activation_gate_receipt_sha256"}
)
_ACTIVATION_GATE_BINDING_FIELDS = frozenset(
    {
        "path",
        "file_sha256",
        "canonical_receipt_sha256",
        "cross_binding_sha256",
        "plan_core_sha256",
        "transaction_contract_sha256",
        "canonical_activation_binding_sha256",
    }
)
_DISABLED_PHASE_BINDING_FIELDS = frozenset(
    {
        "path",
        "file_sha256",
        "canonical_receipt_sha256",
        "plan_sha256",
        "process_identity_sha256",
        "pid",
        "pid_start_ticks",
        "config_sha256",
        "artifact_sha256",
        "runtime_code_sha256",
        "execution_commit",
        "execution_tree",
        "runtime_identity_file_sha256",
        "startup_attestation_sha256",
    }
)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise BuyE3TransactionalDeployError(f"{label} is not a SHA256")
    return normalized


def _plan_core_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    missing = _PLAN_CORE_FIELDS - set(plan)
    if missing:
        raise BuyE3TransactionalDeployError(
            "deployment plan core lacks: " + ", ".join(sorted(missing))
        )
    return {field: plan[field] for field in sorted(_PLAN_CORE_FIELDS)}


def _plan_core_sha256(plan: Mapping[str, Any]) -> str:
    return gate_v2.canonical_sha256(_plan_core_payload(plan))


def _runtime_attestation_contract(remote: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_ATTESTATION_CONTRACT_SCHEMA,
        "required_for_activation": True,
        "required_for_disabled_deploy_completion": True,
        "remote_path": str(remote["runtime_identity_path"]),
        "runtime_identity_schema_version": RUNTIME_IDENTITY_SCHEMA,
        "startup_attestation_schema_version": STARTUP_ATTESTATION_SCHEMA,
        "authority": "runtime_written_startup_attestation",
        "evidence_classification": "runtime_identity_file_unsigned_structural_evidence",
        "cryptographic_signature_present": False,
        "local_receipt_standalone_activation_evidence": False,
        "expected_value_echo_is_evidence": False,
    }


def _read_json(path: Path) -> dict[str, Any]:
    return gate_v2.read_json(path)


def _reject_remote_environment_override() -> None:
    present = [name for name in REMOTE_OVERRIDE_ENV if os.environ.get(name, "").strip()]
    if present:
        raise BuyE3TransactionalDeployError(
            "environment remote override is forbidden: " + ", ".join(present)
        )


def load_sha_bound_active_pointer(
    *,
    pointer_path: Path,
    expected_file_sha256: str,
) -> dict[str, Any]:
    """Resolve only the explicit pointer bytes; environment overrides are rejected."""

    _reject_remote_environment_override()
    path = pointer_path.expanduser()
    if path.is_symlink() or not path.is_file():
        raise BuyE3TransactionalDeployError("active pointer is not a regular file")
    path = path.resolve(strict=True)
    expected = _require_sha256(expected_file_sha256, "active pointer file hash")
    if gate_v2.file_sha256(path) != expected:
        raise BuyE3TransactionalDeployError("active pointer file hash drifted")
    payload = _read_json(path)
    if (
        payload.get("schema_version") != POINTER_SCHEMA
        or payload.get("status") != ACTIVE_POINTER_STATUS
    ):
        raise BuyE3TransactionalDeployError("active pointer identity drifted")
    required = ("ssh_target", "repo_root", "provider", "region", "public_ipv4")
    fields = {key: str(payload.get(key, "")).strip() for key in required}
    if not all(fields.values()):
        raise BuyE3TransactionalDeployError("active pointer lacks required host fields")
    return {
        "path": str(path),
        "file_sha256": expected,
        **fields,
    }


def bind_known_hosts(
    *,
    known_hosts_path: Path,
    expected_file_sha256: str,
    expected_fingerprint: str,
) -> dict[str, Any]:
    path = known_hosts_path.expanduser()
    if path.is_symlink() or not path.is_file():
        raise BuyE3TransactionalDeployError("known-hosts is not a regular file")
    path = path.resolve(strict=True)
    expected_sha = _require_sha256(expected_file_sha256, "known-hosts file hash")
    if gate_v2.file_sha256(path) != expected_sha:
        raise BuyE3TransactionalDeployError("known-hosts file hash drifted")
    fingerprints = gate_v2.ssh_host_key_fingerprints(path)
    fingerprint = str(expected_fingerprint).strip()
    if fingerprint not in fingerprints:
        raise BuyE3TransactionalDeployError("expected host-key fingerprint is absent")
    return {
        "path": str(path),
        "file_sha256": expected_sha,
        "expected_fingerprint": fingerprint,
        "observed_fingerprints": fingerprints,
    }


def _config_mapping(config_path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise BuyE3TransactionalDeployError("private config is not a mapping")
    return payload


def _strategy_mapping(config_path: Path) -> dict[str, Any]:
    payload = _config_mapping(config_path)
    if not isinstance(payload.get("strategy"), dict):
        raise BuyE3TransactionalDeployError("private config lacks strategy mapping")
    return payload["strategy"]


def _host_bound_spool_gate(
    config_path: Path, *, defer_host_bound_spool: bool
) -> dict[str, Any]:
    from execution.order_lifecycle_journal_storage_v2 import (
        BOUNDED_REMOTE_SPOOL,
        validate_lifecycle_journal_storage,
    )

    lifecycle = _config_mapping(config_path).get("lifecycle_journal_v2")
    if not isinstance(lifecycle, Mapping) or not bool(lifecycle.get("enabled", False)):
        return {
            "profile": None,
            "status": "not_applicable",
            "deferred_on_planner_host": False,
            "mandatory_remote_preflight": False,
            "allowlisted_root": None,
            "journal_root": None,
            "prospective_epoch_root": None,
            "required_remote_checks": [],
        }
    profile = str(lifecycle.get("storage_profile", "")).strip()
    if profile != BOUNDED_REMOTE_SPOOL:
        return {
            "profile": profile,
            "status": "not_applicable",
            "deferred_on_planner_host": False,
            "mandatory_remote_preflight": False,
            "allowlisted_root": None,
            "journal_root": None,
            "prospective_epoch_root": None,
            "required_remote_checks": [],
        }
    try:
        resolution = validate_lifecycle_journal_storage(
            profile=profile,
            journal_root=str(lifecycle["root"]),
            prospective_epoch_root=str(lifecycle["prospective_epoch_root"]),
            required_mount=str(lifecycle["required_mount"]),
            remote_spool_allowlisted_roots=lifecycle[
                "remote_spool_allowlisted_roots"
            ],
            enabled=False,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise BuyE3TransactionalDeployError(
            "bounded remote spool lexical/allowlist contract is invalid"
        ) from exc
    return {
        "profile": profile,
        "status": (
            "deferred_to_mandatory_remote_preflight"
            if defer_host_bound_spool
            else "validated_on_execution_host"
        ),
        "deferred_on_planner_host": bool(defer_host_bound_spool),
        "mandatory_remote_preflight": True,
        "allowlisted_root": str(resolution.allowlisted_root),
        "journal_root": str(resolution.journal_root),
        "prospective_epoch_root": str(resolution.prospective_epoch_root),
        "required_remote_checks": list(HOST_BOUND_SPOOL_REMOTE_CHECKS),
    }


@contextmanager
def _host_bound_spool_validation_scope(
    config_path: Path, *, defer_host_bound_spool: bool
):
    import live.config as live_config
    from execution.order_lifecycle_journal_storage_v2 import BOUNDED_REMOTE_SPOOL

    gate = _host_bound_spool_gate(
        config_path, defer_host_bound_spool=defer_host_bound_spool
    )
    if gate["status"] != "deferred_to_mandatory_remote_preflight":
        yield gate
        return
    original = live_config.validate_lifecycle_journal_storage

    def defer_only_host_existence(**kwargs):
        if (
            str(kwargs.get("profile", "")).strip() == BOUNDED_REMOTE_SPOOL
            and kwargs.get("enabled") is True
        ):
            return original(**{**kwargs, "enabled": False})
        return original(**kwargs)

    live_config.validate_lifecycle_journal_storage = defer_only_host_existence
    try:
        yield gate
    finally:
        live_config.validate_lifecycle_journal_storage = original


def isolated_config_preflight(
    repository_root: Path,
    config_path: Path,
    expected_enabled: bool,
    *,
    defer_host_bound_spool: bool = False,
) -> dict[str, Any]:
    """Validate one config in a short-lived process before any live stop.

    The function itself is also useful for the internal child command.  The
    parent planner invokes it twice through ``run_isolated_preflight``.
    """

    root = repository_root.expanduser().resolve(strict=True)
    config = config_path.expanduser().resolve(strict=True)
    previous = os.environ.get(F05_BUY_E3_OWNER_OVERRIDE_ENV)
    try:
        if expected_enabled:
            os.environ[F05_BUY_E3_OWNER_OVERRIDE_ENV] = "1"
        else:
            os.environ.pop(F05_BUY_E3_OWNER_OVERRIDE_ENV, None)
        from live.config import load_config
        from scripts.preflight_live_deploy import validate_deploy_config

        with _host_bound_spool_validation_scope(
            config, defer_host_bound_spool=defer_host_bound_spool
        ) as host_bound_storage_gate:
            generic = validate_deploy_config(config, root)
            loaded = load_config(config)
            artifact = gate_v2.validate_config_artifact(
                config_path=config,
                repository_root=root,
                expected_enabled=expected_enabled,
            )
            policy = f05_buy_e3_runtime_policy(
                bool(loaded.strategy.buy_e3_cooldown_policy_enabled),
                evidence_route=loaded.strategy.buy_e3_cooldown_evidence_route,
            )
    finally:
        if previous is None:
            os.environ.pop(F05_BUY_E3_OWNER_OVERRIDE_ENV, None)
        else:
            os.environ[F05_BUY_E3_OWNER_OVERRIDE_ENV] = previous
    if bool(policy["f05_buy_e3_owner_override_effective"]) is not bool(expected_enabled):
        raise BuyE3TransactionalDeployError("isolated owner override was not exact")
    receipt: dict[str, Any] = {
        "schema_version": PREFLIGHT_SCHEMA,
        "status": "isolated_config_preflight_passed",
        "expected_enabled": bool(expected_enabled),
        "config_sha256": artifact["config_sha256"],
        "artifact_sha256": artifact["artifact_sha256"],
        "artifact_files": artifact["artifact_files"],
        "artifact_loaded_with_from_files": artifact["artifact_loaded_with_from_files"],
        "generic_preflight_sha256": gate_v2.canonical_sha256(generic),
        "owner_override_requested": policy["f05_buy_e3_owner_override_requested"],
        "owner_override_effective": policy["f05_buy_e3_owner_override_effective"],
        "host_bound_storage_gate": host_bound_storage_gate,
        "validation_read": False,
        "sealed_holdout_read": False,
        "economic_values_read": False,
    }
    receipt["canonical_preflight_sha256"] = gate_v2.document_sha256(
        receipt, "canonical_preflight_sha256"
    )
    return receipt


def run_isolated_preflight(
    repository_root: Path,
    config_path: Path,
    expected_enabled: bool,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    python = repository_root / ".venv/bin/python"
    if not python.is_file() or not os.access(python, os.X_OK):
        raise BuyE3TransactionalDeployError("repository virtualenv Python is unavailable")
    command = (
        str(python),
        str(Path(__file__).resolve()),
        "isolated-preflight",
        "--repository-root",
        str(repository_root),
        "--config",
        str(config_path),
        "--expected-enabled",
        "1" if expected_enabled else "0",
        "--defer-host-bound-spool",
    )
    environment = dict(os.environ)
    environment.pop(F05_BUY_E3_OWNER_OVERRIDE_ENV, None)
    completed = runner(
        command,
        cwd=repository_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise BuyE3TransactionalDeployError("isolated preflight output is not JSON") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != PREFLIGHT_SCHEMA
        or payload.get("status") != "isolated_config_preflight_passed"
        or payload.get("expected_enabled") is not bool(expected_enabled)
        or payload.get("canonical_preflight_sha256")
        != gate_v2.document_sha256(payload, "canonical_preflight_sha256")
    ):
        raise BuyE3TransactionalDeployError("isolated preflight receipt drifted")
    storage_gate = payload.get("host_bound_storage_gate")
    if (
        not isinstance(storage_gate, Mapping)
        or set(storage_gate) != _HOST_BOUND_STORAGE_GATE_FIELDS
        or (
            storage_gate.get("status")
            not in {"not_applicable", "deferred_to_mandatory_remote_preflight"}
        )
        or (
            storage_gate.get("status") == "deferred_to_mandatory_remote_preflight"
            and (
                storage_gate.get("deferred_on_planner_host") is not True
                or storage_gate.get("mandatory_remote_preflight") is not True
                or storage_gate.get("required_remote_checks")
                != list(HOST_BOUND_SPOOL_REMOTE_CHECKS)
            )
        )
    ):
        raise BuyE3TransactionalDeployError("isolated host-bound storage gate drifted")
    return payload


def _runtime_source_manifest_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(
        list(rows),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validated_expected_runtime_source_hashes(
    runtime_sources: Mapping[str, Any],
) -> dict[str, str]:
    files = runtime_sources.get("files")
    if not isinstance(files, Mapping):
        raise BuyE3TransactionalDeployError("runtime source bindings are malformed")
    if runtime_sources.get("runtime_code_sha256") != gate_v2.canonical_sha256(files):
        raise BuyE3TransactionalDeployError("runtime source aggregate is malformed")
    expected: dict[str, str] = {}
    for role, raw in files.items():
        if not isinstance(raw, Mapping):
            raise BuyE3TransactionalDeployError(
                f"runtime source binding is malformed: {role}"
            )
        path = str(raw.get("repository_relative_path", "")).strip()
        relative = PurePosixPath(path)
        if not path or relative.is_absolute() or ".." in relative.parts:
            raise BuyE3TransactionalDeployError(
                f"runtime source path is unsafe: {role}"
            )
        hashes = {
            _require_sha256(raw.get(field), f"runtime source {role} {field}")
            for field in (
                "artifact_manifest_sha256",
                "execution_commit_blob_sha256",
                "working_file_sha256",
            )
        }
        if len(hashes) != 1 or path in expected:
            raise BuyE3TransactionalDeployError(
                f"runtime source binding disagrees: {role}"
            )
        expected[path] = hashes.pop()
    return expected


def _validate_file_byte_identity(
    raw: Any,
    *,
    label: str,
    expected_reported_path: str | None = None,
    expected_resolved_path: str | None = None,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _FILE_BYTE_IDENTITY_FIELDS:
        raise BuyE3TransactionalDeployError(f"{label} fields drifted")
    identity = dict(raw)
    reported_path = str(identity.get("reported_path", "")).strip()
    resolved_path = str(identity.get("resolved_path", "")).strip()
    size_bytes = identity.get("size_bytes")
    if (
        not reported_path
        or not resolved_path
        or not PurePosixPath(reported_path).is_absolute()
        or not PurePosixPath(resolved_path).is_absolute()
        or not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or size_bytes <= 0
        or (
            expected_reported_path is not None
            and reported_path != expected_reported_path
        )
        or (
            expected_resolved_path is not None
            and resolved_path != expected_resolved_path
        )
    ):
        raise BuyE3TransactionalDeployError(f"{label} is malformed")
    _require_sha256(identity.get("sha256"), f"{label} hash")
    return identity


def _validate_startup_attestation(
    raw: Any,
    *,
    expected_execution_commit: str,
    expected_execution_tree: str,
    expected_runtime_sources: Mapping[str, Any],
    expected_python_executable: str,
    expected_python_binary_resolved: str,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _STARTUP_ATTESTATION_FIELDS:
        raise BuyE3TransactionalDeployError("runtime startup attestation fields drifted")
    attestation = dict(raw)
    gates = attestation.get("gates")
    state = attestation.get("fill_cooldown_state")
    checkout = attestation.get("running_checkout")
    expected_gates = {name: True for name in _STARTUP_GATE_FIELDS}
    if (
        attestation.get("schema_version") != STARTUP_ATTESTATION_SCHEMA
        or attestation.get("status") != "accepted"
        or not str(attestation.get("attested_at_utc", "")).strip()
        or attestation.get("errors") != []
        or not isinstance(gates, Mapping)
        or set(gates) != _STARTUP_GATE_FIELDS
        or dict(gates) != expected_gates
        or not isinstance(state, Mapping)
        or state.get("schema_version") != FILL_COOLDOWN_STATE_SCHEMA
        or state.get("buy_deadline_identity") != "B0"
        or not isinstance(state.get("buy_remaining_ms"), int)
        or isinstance(state.get("buy_remaining_ms"), bool)
        or state.get("buy_remaining_ms") != 0
        or not isinstance(checkout, Mapping)
        or set(checkout) != _RUNNING_CHECKOUT_FIELDS
    ):
        raise BuyE3TransactionalDeployError(
            "runtime startup attestation is rejected or deadline-unsafe"
        )
    if (
        checkout.get("schema_version") != RUNNING_CHECKOUT_SCHEMA
        or checkout.get("git_commit") != expected_execution_commit
        or checkout.get("git_tree") != expected_execution_tree
        or checkout.get("git_worktree_clean") is not True
    ):
        raise BuyE3TransactionalDeployError("runtime startup checkout identity drifted")
    expected_snapshot = {
        "commit": expected_execution_commit,
        "tree": expected_execution_tree,
        "status_porcelain_sha256": hashlib.sha256(b"").hexdigest(),
        "status_entry_count": 0,
        "worktree_clean": True,
        "snapshot_internally_stable": True,
    }
    for label in ("pre_snapshot", "post_snapshot"):
        snapshot = checkout.get(label)
        if (
            not isinstance(snapshot, Mapping)
            or set(snapshot) != _GIT_SNAPSHOT_FIELDS
            or dict(snapshot) != expected_snapshot
        ):
            raise BuyE3TransactionalDeployError(
                f"runtime startup {label} drifted"
            )
    stable_snapshot = checkout.get("stable_snapshot")
    if (
        not isinstance(stable_snapshot, Mapping)
        or set(stable_snapshot) != _STABLE_GIT_SNAPSHOT_FIELDS
        or any(value is not True for value in stable_snapshot.values())
    ):
        raise BuyE3TransactionalDeployError(
            "runtime startup stable Git snapshot drifted"
        )
    source_rows = checkout.get("runtime_source_files")
    if not isinstance(source_rows, list) or not source_rows:
        raise BuyE3TransactionalDeployError("runtime startup source manifest is empty")
    normalized_rows: list[dict[str, Any]] = []
    observed: dict[str, str] = {}
    for raw_row in source_rows:
        if not isinstance(raw_row, Mapping) or set(raw_row) != _RUNTIME_SOURCE_FILE_FIELDS:
            raise BuyE3TransactionalDeployError(
                "runtime startup source file binding drifted"
            )
        row = dict(raw_row)
        path = str(row.get("path", "")).strip()
        relative = PurePosixPath(path)
        working_size = row.get("working_size_bytes")
        head_size = row.get("head_blob_size_bytes")
        if (
            not path
            or relative.is_absolute()
            or ".." in relative.parts
            or path in observed
            or not isinstance(working_size, int)
            or isinstance(working_size, bool)
            or working_size < 0
            or not isinstance(head_size, int)
            or isinstance(head_size, bool)
            or head_size != working_size
            or row.get("matches_head_blob") is not True
        ):
            raise BuyE3TransactionalDeployError(
                "runtime startup source file binding is malformed"
            )
        working_sha256 = _require_sha256(
            row.get("working_file_sha256"),
            f"runtime startup working source {path}",
        )
        head_sha256 = _require_sha256(
            row.get("head_blob_sha256"),
            f"runtime startup HEAD source {path}",
        )
        if working_sha256 != head_sha256:
            raise BuyE3TransactionalDeployError(
                "runtime startup source differs from its HEAD blob"
            )
        observed[path] = working_sha256
        normalized_rows.append(row)
    if [row["path"] for row in normalized_rows] != sorted(observed):
        raise BuyE3TransactionalDeployError("runtime startup source manifest is not ordered")
    if (
        checkout.get("runtime_source_file_count") != len(normalized_rows)
        or checkout.get("runtime_source_manifest_sha256")
        != _runtime_source_manifest_sha256(normalized_rows)
    ):
        raise BuyE3TransactionalDeployError(
            "runtime startup source manifest aggregate drifted"
        )
    expected_sources = _validated_expected_runtime_source_hashes(
        expected_runtime_sources
    )
    if any(observed.get(path) != sha256 for path, sha256 in expected_sources.items()):
        raise BuyE3TransactionalDeployError(
            "runtime startup source bytes differ from the frozen plan"
        )
    loaded_origins = attestation.get("loaded_module_origins")
    if (
        not isinstance(loaded_origins, Mapping)
        or set(loaded_origins) != _LOADED_RUNTIME_MODULE_ROLES
    ):
        raise BuyE3TransactionalDeployError(
            "runtime loaded module origin set drifted"
        )
    for role, raw_module in loaded_origins.items():
        if (
            not isinstance(raw_module, Mapping)
            or set(raw_module) != _LOADED_RUNTIME_MODULE_FIELDS
        ):
            raise BuyE3TransactionalDeployError(
                f"runtime loaded module origin fields drifted: {role}"
            )
        relative_path = str(
            raw_module.get("repository_relative_path", "")
        ).strip()
        origin_path = str(raw_module.get("origin_path", "")).strip()
        module_name = str(raw_module.get("module_name", "")).strip()
        if (
            not module_name
            or not origin_path
            or not PurePosixPath(origin_path).is_absolute()
            or observed.get(relative_path)
            != _require_sha256(
                raw_module.get("source_sha256"),
                f"runtime loaded module source {role}",
            )
        ):
            raise BuyE3TransactionalDeployError(
                f"runtime loaded module origin is not source-bound: {role}"
            )
    interpreter = attestation.get("interpreter_identity")
    if (
        not isinstance(interpreter, Mapping)
        or set(interpreter) != _INTERPRETER_IDENTITY_FIELDS
        or interpreter.get("schema_version") != INTERPRETER_IDENTITY_SCHEMA
        or not str(interpreter.get("version", "")).strip()
        or interpreter.get("stable") is not True
    ):
        raise BuyE3TransactionalDeployError("runtime interpreter identity drifted")
    interpreter_before = _validate_file_byte_identity(
        interpreter.get("before"),
        label="runtime interpreter before",
        expected_reported_path=expected_python_executable,
        expected_resolved_path=expected_python_binary_resolved,
    )
    interpreter_after = _validate_file_byte_identity(
        interpreter.get("after"),
        label="runtime interpreter after",
        expected_reported_path=expected_python_executable,
        expected_resolved_path=expected_python_binary_resolved,
    )
    if interpreter_before != interpreter_after:
        raise BuyE3TransactionalDeployError(
            "runtime interpreter bytes changed during attestation"
        )
    native = attestation.get("native_runtime_identity")
    if (
        not isinstance(native, Mapping)
        or set(native) != _NATIVE_RUNTIME_IDENTITY_FIELDS
        or native.get("schema_version") != NATIVE_RUNTIME_IDENTITY_SCHEMA
        or not str(native.get("platform", "")).strip()
        or native.get("stable") is not True
        or not isinstance(native.get("enabled"), bool)
    ):
        raise BuyE3TransactionalDeployError("native runtime identity drifted")
    if native["enabled"]:
        native_before = _validate_file_byte_identity(
            native.get("before"), label="native runtime before"
        )
        native_after = _validate_file_byte_identity(
            native.get("after"), label="native runtime after"
        )
        if (
            native_before != native_after
            or native.get("reported_module_path")
            != native_before["reported_path"]
            or native.get("loaded_module_origin_path")
            != native_before["resolved_path"]
        ):
            raise BuyE3TransactionalDeployError(
                "native runtime module bytes or origin drifted"
            )
    elif (
        native.get("reported_module_path") != "disabled"
        or native.get("loaded_module_origin_path") is not None
        or native.get("before") is not None
        or native.get("after") is not None
    ):
        raise BuyE3TransactionalDeployError(
            "disabled native runtime identity is malformed"
        )
    return attestation


def _validate_runtime_identity_authority(
    runtime: Any,
    *,
    expected_pid: int,
    expected_config_path: str,
    expected_config_sha256: str,
    expected_python_executable: str,
    expected_python_binary_resolved: str,
    expected_enabled: bool,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
    expected_execution_tree: str,
    expected_runtime_sources: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(runtime, Mapping):
        raise BuyE3TransactionalDeployError("runtime identity file is not a mapping")
    if (
        runtime.get("schema_version") != RUNTIME_IDENTITY_SCHEMA
        or int(runtime.get("pid", -1)) != int(expected_pid)
        or runtime.get("config_path") != expected_config_path
        or runtime.get("config_sha256") != expected_config_sha256
        or runtime.get("python_executable") != expected_python_executable
        or runtime.get("f05_buy_e3_enabled") is not bool(expected_enabled)
        or runtime.get("f05_buy_e3_owner_override_effective")
        is not bool(expected_enabled)
        or (
            expected_artifact_sha256
            and runtime.get("f05_buy_e3_artifact_sha256")
            != expected_artifact_sha256
        )
        or not str(runtime.get("recorded_at_utc", "")).strip()
    ):
        raise BuyE3TransactionalDeployError(
            "runtime identity process/config/artifact authority drifted"
        )
    attestation = _validate_startup_attestation(
        runtime.get("startup_attestation"),
        expected_execution_commit=expected_execution_commit,
        expected_execution_tree=expected_execution_tree,
        expected_runtime_sources=expected_runtime_sources,
        expected_python_executable=expected_python_executable,
        expected_python_binary_resolved=expected_python_binary_resolved,
    )
    native_runtime = runtime.get("native_runtime")
    native_identity = attestation["native_runtime_identity"]
    native_enable_flag_names = (
        "NARROWGATE_CPP_QUOTE_CORE",
        "NARROWGATE_CPP_SIGNAL_FEATURES",
        "NARROWGATE_CPP_GLOBAL_FLOW",
        "NARROWGATE_CPP_LIVE_ROUTING",
    )
    native_flag_names = native_enable_flag_names + ("NARROWGATE_CPP_STRICT",)
    if (
        not isinstance(native_runtime, Mapping)
        or set(native_runtime) != {"profile", "module", *native_flag_names}
        or any(not isinstance(native_runtime.get(name), bool) for name in native_flag_names)
        or native_identity.get("profile") != native_runtime.get("profile")
        or native_identity.get("reported_module_path")
        != native_runtime.get("module")
        or native_identity.get("enabled")
        is not any(bool(native_runtime[name]) for name in native_enable_flag_names)
    ):
        raise BuyE3TransactionalDeployError(
            "runtime native attestation differs from runtime-owned identity"
        )
    return attestation


def capture_runtime_process_probe(
    *,
    repository_root: Path,
    pid_file: Path,
    config_path: Path,
    config_sha256: str,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    expected_buy_e3_enabled: bool,
    expected_execution_commit: str,
    expected_execution_tree: str,
    expected_artifact_sha256: str | None,
    expected_runtime_code_sha256: str,
    artifact_manifest_path: Path | None,
) -> dict[str, Any]:
    pid = int(pid_file.expanduser().resolve(strict=True).read_text(encoding="ascii").strip())
    process = gate_v2.capture_actual_process_identity(
        pid=pid,
        expected_repository_root=repository_root,
        expected_config_path=config_path,
        expected_config_sha256=config_sha256,
        expected_python_executable=python_executable,
        expected_venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
    )
    runtime_candidate = runtime_identity_path.expanduser()
    if runtime_candidate.is_symlink() or not runtime_candidate.is_file():
        raise BuyE3TransactionalDeployError(
            "runtime identity is not a non-symlink regular file"
        )
    runtime_path = runtime_candidate.resolve(strict=True)
    runtime_bytes = runtime_path.read_bytes()
    runtime_file_sha256 = hashlib.sha256(runtime_bytes).hexdigest()
    if process.get("runtime_identity", {}).get("file_sha256") != runtime_file_sha256:
        raise BuyE3TransactionalDeployError("runtime identity changed during process probe")
    try:
        runtime = json.loads(runtime_bytes)
    except json.JSONDecodeError as exc:
        raise BuyE3TransactionalDeployError("runtime identity file is not JSON") from exc
    enabled = bool(runtime.get("f05_buy_e3_enabled", False))
    effective = bool(runtime.get("f05_buy_e3_owner_override_effective", False))
    if enabled is not expected_buy_e3_enabled or effective is not expected_buy_e3_enabled:
        raise BuyE3TransactionalDeployError("actual process BUY E3 authority drifted")
    artifact_sha = (
        _require_sha256(expected_artifact_sha256, "expected artifact hash")
        if str(expected_artifact_sha256 or "").strip()
        else ""
    )
    if bool(artifact_sha) is not bool(artifact_manifest_path is not None):
        raise BuyE3TransactionalDeployError(
            "artifact hash and manifest must be supplied together"
        )
    runtime_code_sha = _require_sha256(expected_runtime_code_sha256, "expected runtime code hash")
    if artifact_sha and runtime.get("f05_buy_e3_artifact_sha256") != artifact_sha:
        raise BuyE3TransactionalDeployError("actual process artifact identity drifted")
    completed_commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    completed_tree = subprocess.run(
        ("git", "rev-parse", "HEAD^{tree}"),
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if completed_commit != expected_execution_commit or completed_tree != expected_execution_tree:
        raise BuyE3TransactionalDeployError("actual process checkout identity drifted")
    if artifact_manifest_path is not None:
        manifest = gate_v2.read_json(
            artifact_manifest_path.expanduser().resolve(strict=True)
        )
        actual_runtime_sources = gate_v2.verify_runtime_sources(
            repository_root=repository_root,
            execution_commit=completed_commit,
            artifact_manifest=manifest,
        )
        if actual_runtime_sources.get("runtime_code_sha256") != runtime_code_sha:
            raise BuyE3TransactionalDeployError("actual runtime source aggregate drifted")
    else:
        empty_sources: dict[str, Any] = {}
        actual_runtime_sources = {
            "files": empty_sources,
            "runtime_code_sha256": gate_v2.canonical_sha256(empty_sources),
        }
    startup_attestation = _validate_runtime_identity_authority(
        runtime,
        expected_pid=pid,
        expected_config_path=str(config_path.expanduser().resolve(strict=True)),
        expected_config_sha256=_require_sha256(config_sha256, "config hash"),
        expected_python_executable=str(python_executable.expanduser().absolute()),
        expected_python_binary_resolved=str(process["python_binary_resolved"]),
        expected_enabled=expected_buy_e3_enabled,
        expected_artifact_sha256=artifact_sha,
        expected_execution_commit=completed_commit,
        expected_execution_tree=completed_tree,
        expected_runtime_sources=actual_runtime_sources,
    )
    startup_state = startup_attestation["fill_cooldown_state"]
    startup_gates = startup_attestation["gates"]
    process.update(
        {
            "execution_commit": completed_commit,
            "execution_tree": completed_tree,
            "runtime_identity_file_sha256": runtime_file_sha256,
            "startup_attestation_sha256": gate_v2.canonical_sha256(
                startup_attestation
            ),
            "artifact_sha256": artifact_sha,
            "runtime_code_sha256": runtime_code_sha,
            "buy_e3_enabled": enabled,
            "owner_override_effective": effective,
            "initial_buy_deadline_identity": startup_state[
                "buy_deadline_identity"
            ],
            "e3_deadline_imported": startup_gates[
                "buy_e3_deadline_not_imported"
            ]
            is not True,
        }
    )
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )
    return process


def _validate_rollback_identity(name: str, raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise BuyE3TransactionalDeployError(f"rollback identity is malformed: {name}")
    required = (
        "identity",
        "execution_commit",
        "execution_tree",
        "config_path",
        "config_sha256",
        "python_executable",
        "venv_root",
        "runtime_code_sha256",
    )
    missing = [field for field in required if not str(raw.get(field, "")).strip()]
    if missing:
        raise BuyE3TransactionalDeployError(f"rollback identity {name} lacks: {', '.join(missing)}")
    if raw.get("buy_e3_enabled") is not False:
        raise BuyE3TransactionalDeployError(f"rollback identity {name} enables BUY E3")
    if raw.get("buy_deadline_identity") != "B0":
        raise BuyE3TransactionalDeployError(f"rollback identity {name} can retain E3 deadline")
    if raw.get("imports_e3_deadline") is not False:
        raise BuyE3TransactionalDeployError(f"rollback identity {name} imports E3 state")
    normalized = dict(raw)
    for field in ("config_sha256", "runtime_code_sha256"):
        normalized[field] = _require_sha256(raw[field], f"rollback {name} {field}")
    return normalized


def _activation_gate_cross_binding(
    *,
    execution: Mapping[str, Any],
    runtime_sources: Mapping[str, Any],
    artifact: Mapping[str, Any],
    configs: Mapping[str, Any],
    pointer: Mapping[str, Any],
    known_hosts: Mapping[str, Any],
    host: Mapping[str, Any],
    rollback: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "execution": {
            field: execution.get(field)
            for field in (
                "execution_commit",
                "execution_tree",
                "annotated_tag",
                "annotated_tag_object",
                "tag_peeled_commit",
            )
        },
        "runtime_code_sha256": runtime_sources.get("runtime_code_sha256"),
        "artifact_sha256": artifact.get("artifact_sha256"),
        "configs": {
            name: configs.get(name, {}).get("config_sha256")
            for name in ("disabled", "active")
        },
        "host": {
            "active_pointer_file_sha256": pointer.get("file_sha256"),
            "known_hosts_file_sha256": known_hosts.get("file_sha256"),
            "host_key_fingerprint": known_hosts.get("expected_fingerprint"),
            "repo_root": pointer.get("repo_root"),
            "python_executable": host.get("python_executable"),
            "venv_root": host.get("venv_root"),
        },
        "rollback_identities": {
            name: dict(rollback.get(name, {}))
            for name in ("primary_disabled", "deep_predecessor")
        },
    }


def _activation_gate_receipt_cross_binding(receipt: Mapping[str, Any]) -> dict[str, Any]:
    execution = receipt.get("execution_identity")
    runtime_sources = receipt.get("runtime_sources")
    artifact = receipt.get("artifact_binding")
    configs = receipt.get("config_binding")
    host = receipt.get("host_binding")
    rollback = receipt.get("rollback_identities")
    if not all(
        isinstance(value, Mapping)
        for value in (execution, runtime_sources, artifact, configs, host, rollback)
    ):
        raise BuyE3TransactionalDeployError("activation gate cross-binding is incomplete")
    return {
        "execution": {
            field: execution.get(field)
            for field in (
                "execution_commit",
                "execution_tree",
                "annotated_tag",
                "annotated_tag_object",
                "tag_peeled_commit",
            )
        },
        "runtime_code_sha256": runtime_sources.get("runtime_code_sha256"),
        "artifact_sha256": artifact.get("artifact_sha256"),
        "configs": {
            name: configs.get(name, {}).get("config_sha256")
            for name in ("disabled", "active")
        },
        "host": {
            field: host.get(field)
            for field in (
                "active_pointer_file_sha256",
                "known_hosts_file_sha256",
                "host_key_fingerprint",
                "repo_root",
                "python_executable",
                "venv_root",
            )
        },
        "rollback_identities": {
            name: dict(rollback.get(name, {}))
            for name in ("primary_disabled", "deep_predecessor")
        },
    }


def _require_activation_gate_cross_binding(
    receipt: Mapping[str, Any], expected: Mapping[str, Any]
) -> str:
    observed = _activation_gate_receipt_cross_binding(receipt)
    if observed != dict(expected):
        raise BuyE3TransactionalDeployError("activation gate cross-binding drifted")
    return gate_v2.canonical_sha256(observed)


def _ssh_base(known_hosts: str) -> list[str]:
    return [
        "ssh",
        "-o",
        STRICT_SSH_OPTIONS[0],
        "-o",
        STRICT_SSH_OPTIONS[1],
        "-o",
        f"UserKnownHostsFile={known_hosts}",
    ]


def _ssh_command(*, target: str, known_hosts: str, remote_command: str) -> list[str]:
    return [*_ssh_base(known_hosts), "--", target, remote_command]


def _rsync_command(*, source: str, target: str, known_hosts: str, destination: str) -> list[str]:
    transport = shlex.join(_ssh_base(known_hosts))
    return [
        "rsync",
        "--archive",
        "--checksum",
        "--ignore-existing",
        "--protect-args",
        "-e",
        transport,
        source,
        f"{target}:{destination}",
    ]


def _remote_external_config_start(repo_root: str, config_path: str, *, owner_override: bool) -> str:
    authority = (
        f"{F05_BUY_E3_OWNER_OVERRIDE_ENV}=1"
        if owner_override
        else f"-u {F05_BUY_E3_OWNER_OVERRIDE_ENV}"
    )
    return (
        f"cd {shlex.quote(repo_root)} && "
        f"env {authority} NARROWGATE_LIVE_CONFIG={shlex.quote(config_path)} "
        "bash live/run.sh start"
    )


def _remote_external_config_stop(repo_root: str, config_path: str) -> str:
    return (
        f"cd {shlex.quote(repo_root)} && "
        f"env -u {F05_BUY_E3_OWNER_OVERRIDE_ENV} "
        f"NARROWGATE_LIVE_CONFIG={shlex.quote(config_path)} bash live/run.sh stop"
    )


def _remote_preflight(
    *,
    repo_root: str,
    external_script: str,
    config_path: str,
    expected_enabled: bool,
    python: str,
    external_gate: str,
    external_script_sha256: str,
    external_gate_sha256: str,
) -> str:
    override = f"{F05_BUY_E3_OWNER_OVERRIDE_ENV}=1 " if expected_enabled else ""
    command = (
        f"cd {shlex.quote(repo_root)} && env PYTHONPATH={shlex.quote(repo_root)} "
        f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
        f"{override}{shlex.quote(python)} {shlex.quote(external_script)} isolated-preflight "
        f"--repository-root {shlex.quote(repo_root)} --config {shlex.quote(config_path)} "
        f"--expected-enabled {1 if expected_enabled else 0}"
    )
    return _verified_external_exec(
        command=command,
        external_script=external_script,
        external_script_sha256=external_script_sha256,
        external_gate=external_gate,
        external_gate_sha256=external_gate_sha256,
    )


def _verified_external_exec(
    *,
    command: str,
    external_script: str,
    external_script_sha256: str,
    external_gate: str,
    external_gate_sha256: str,
) -> str:
    return (
        f"test \"$(sha256sum {shlex.quote(external_script)} | awk '{{print $1}}')\" = "
        f"{shlex.quote(external_script_sha256)} && "
        f"test \"$(sha256sum {shlex.quote(external_gate)} | awk '{{print $1}}')\" = "
        f"{shlex.quote(external_gate_sha256)} && {command}"
    )


def _command(
    label: str, argv: Sequence[str], *, mutates: bool, after_stop: bool = False
) -> dict[str, Any]:
    return {
        "label": label,
        "argv": list(argv),
        "command_sha256": gate_v2.canonical_sha256(list(argv)),
        "mutates_remote": bool(mutates),
        "after_stop": bool(after_stop),
    }


def _phase_commands(
    *,
    pointer: Mapping[str, Any],
    known_hosts: Mapping[str, Any],
    host: Mapping[str, Any],
    configs: Mapping[str, Any],
    remote: Mapping[str, Any],
    execution: Mapping[str, Any],
    rollback: Mapping[str, Any],
    runtime_sources: Mapping[str, Any],
    artifact: Mapping[str, Any],
    local_package: Mapping[str, str],
) -> dict[str, list[dict[str, Any]]]:
    target = str(pointer["ssh_target"])
    repo_root = str(pointer["repo_root"])
    known = str(known_hosts["path"])
    python = str(host["python_executable"])
    stage_root = str(remote["stage_root"])
    local_hashes = {
        role: gate_v2.file_sha256(Path(local_package[role]))
        for role in _EXTERNAL_PACKAGE_ROLES
    }
    package_sha256 = gate_v2.canonical_sha256(local_hashes)
    stage = f"{stage_root}/package-{package_sha256}"
    staged_names = {
        role: f"{role}-{local_hashes[role]}{Path(local_package[role]).suffix}"
        for role in _EXTERNAL_PACKAGE_ROLES
    }
    staged_paths = {role: f"{stage}/{name}" for role, name in staged_names.items()}
    external_script = staged_paths["deploy_script"]
    external_gate = staged_paths["gate_amendment"]
    external_script_sha256 = local_hashes["deploy_script"]
    external_gate_sha256 = local_hashes["gate_amendment"]
    disabled_config = str(remote["disabled_config_path"])
    active_config = str(remote["active_config_path"])
    pid_file = str(remote["pid_file"])
    checkpoint_base = str(remote["startup_checkpoint_path"])
    disabled_checkpoint = f"{checkpoint_base}.disabled"
    active_checkpoint = f"{checkpoint_base}.active"
    runtime_identity_path = str(remote["runtime_identity_path"])
    prepare_stage = _command(
        "prepare-isolated-stage",
        _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=(
                f"test ! -L {shlex.quote(stage_root)} && "
                f"(test -d {shlex.quote(stage_root)} || "
                f"install -d -m 700 {shlex.quote(stage_root)}) && "
                f"test ! -L {shlex.quote(stage)} && "
                f"(test -d {shlex.quote(stage)} || install -d -m 700 {shlex.quote(stage)})"
            ),
        ),
        mutates=True,
    )
    transfers = [
        _command(
            f"stage-{role}",
            _rsync_command(
                source=str(local_package[role]),
                target=target,
                known_hosts=known,
                destination=staged_paths[role],
            ),
            mutates=True,
        )
        for role in _EXTERNAL_PACKAGE_ROLES
    ]
    staged_checks = [
        f"test ! -L {shlex.quote(staged_paths[role])} && "
        f"test -f {shlex.quote(staged_paths[role])} && "
        f"test \"$(sha256sum {shlex.quote(staged_paths[role])} | awk '{{print $1}}')\" = "
        f"{shlex.quote(local_hashes[role])}"
        for role in _EXTERNAL_PACKAGE_ROLES
    ]
    freeze_stage = _command(
        "validate-and-freeze-content-addressed-stage",
        _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=(
                " && ".join(staged_checks)
                + " && chmod 400 "
                + " ".join(shlex.quote(staged_paths[role]) for role in _EXTERNAL_PACKAGE_ROLES)
                + f" && chmod 500 {shlex.quote(stage)}"
            ),
        ),
        mutates=True,
    )
    installs = (
        ("artifact_manifest", str(remote["artifact_manifest_path"])),
        ("policy", str(remote["policy_path"])),
        ("predicate_bundle", str(remote["predicate_bundle_path"])),
        ("disabled_config", disabled_config),
        ("active_config", active_config),
    )
    install_fragments: list[str] = []
    for role, destination in installs:
        source = staged_paths[role]
        parent = str(Path(destination).parent)
        expected_sha = local_hashes[role]
        install_fragments.append(
            f"test \"$(sha256sum {shlex.quote(source)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(expected_sha)} && test ! -L {shlex.quote(destination)} && "
            f"mkdir -p {shlex.quote(parent)} && "
            f"chmod 700 {shlex.quote(parent)} && "
            f"(test ! -e {shlex.quote(destination)} || "
            f"cmp -s {shlex.quote(source)} {shlex.quote(destination)}) && "
            f"install -m 600 {shlex.quote(source)} {shlex.quote(destination)} && "
            f"test \"$(sha256sum {shlex.quote(destination)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(expected_sha)}"
        )
    install_bytes = _command(
        "install-private-artifact-and-config-bytes",
        _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=" && ".join(install_fragments),
        ),
        mutates=True,
    )
    checkout = (
        f"cd {shlex.quote(repo_root)} && "
        f'test "$(git cat-file -t refs/tags/{shlex.quote(str(execution["annotated_tag"]))})" '
        f'= tag && test "$(git rev-parse refs/tags/{shlex.quote(str(execution["annotated_tag"]))})" '
        f"= {shlex.quote(str(execution['annotated_tag_object']))} && "
        f'test "$(git rev-parse refs/tags/{shlex.quote(str(execution["annotated_tag"]))}^{{}})" '
        f"= {shlex.quote(str(execution['execution_commit']))} && git checkout --detach "
        f"{shlex.quote(str(execution['execution_commit']))} && "
        f'test "$(git rev-parse HEAD^{{tree}})" = {shlex.quote(str(execution["execution_tree"]))}'
    )
    disabled_preflight = _remote_preflight(
        repo_root=repo_root,
        external_script=external_script,
        config_path=disabled_config,
        expected_enabled=False,
        python=python,
        external_gate=external_gate,
        external_script_sha256=external_script_sha256,
        external_gate_sha256=external_gate_sha256,
    )
    active_preflight = _remote_preflight(
        repo_root=repo_root,
        external_script=external_script,
        config_path=active_config,
        expected_enabled=True,
        python=python,
        external_gate=external_gate,
        external_script_sha256=external_script_sha256,
        external_gate_sha256=external_gate_sha256,
    )

    def common_pre_stop(checkpoint_path: str) -> list[dict[str, Any]]:
        log_checkpoint = _verified_external_exec(
            command=(
            f"env NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
            f"PYTHONPATH={shlex.quote(repo_root)} {shlex.quote(python)} "
            f"{shlex.quote(external_script)} log-checkpoint --log "
            f"{shlex.quote(str(remote['log_path']))} --output "
            f"{shlex.quote(checkpoint_path)}"
            ),
            external_script=external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
        )
        return [
            prepare_stage,
            *transfers,
            freeze_stage,
            install_bytes,
            _command(
                "isolated-disabled-preflight",
                _ssh_command(target=target, known_hosts=known, remote_command=disabled_preflight),
                mutates=False,
            ),
            _command(
                "isolated-active-preflight",
                _ssh_command(target=target, known_hosts=known, remote_command=active_preflight),
                mutates=False,
            ),
            _command(
                "capture-old-pid",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=(
                        f"test -s {shlex.quote(pid_file)} && "
                        f"printf '%s\\n' \"$(cat {shlex.quote(pid_file)})\""
                    ),
                ),
                mutates=False,
            ),
            _command(
                "startup-log-checkpoint",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=log_checkpoint,
                ),
                mutates=True,
            ),
        ]

    stop_disabled = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_stop(repo_root, disabled_config),
    )
    stop_active = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_stop(repo_root, active_config),
    )
    quiescent = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=("test -z \"$(pgrep -f '[l]ive/main.py' || true)\""),
    )
    checkout_command = _ssh_command(target=target, known_hosts=known, remote_command=checkout)
    start_disabled = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_start(
            repo_root, disabled_config, owner_override=False
        ),
    )
    start_active = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_start(repo_root, active_config, owner_override=True),
    )

    def process_probe(
        config_path: str,
        config_sha: str,
        enabled: bool,
        *,
        expected_execution: Mapping[str, Any] = execution,
        expected_runtime_code_sha256: str = str(runtime_sources["runtime_code_sha256"]),
        expected_artifact_sha256: str = str(artifact["artifact_sha256"]),
    ) -> list[str]:
        command = _verified_external_exec(
            command=(
            f"cd {shlex.quote(repo_root)} && env "
            f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
            f"PYTHONPATH={shlex.quote(repo_root)} {shlex.quote(python)} "
            f"{shlex.quote(external_script)} process-probe --repository-root "
            f"{shlex.quote(repo_root)} --pid-file {shlex.quote(pid_file)} --config "
            f"{shlex.quote(config_path)} --config-sha256 {shlex.quote(config_sha)} "
            f"--python-executable {shlex.quote(python)} --venv-root "
            f"{shlex.quote(str(host['venv_root']))} --runtime-identity "
            f"{shlex.quote(str(remote['runtime_identity_path']))} --expected-enabled "
            f"{1 if enabled else 0} --execution-commit "
            f"{shlex.quote(str(expected_execution['execution_commit']))} --execution-tree "
            f"{shlex.quote(str(expected_execution['execution_tree']))} "
            f"--runtime-code-sha256 {shlex.quote(expected_runtime_code_sha256)}"
            ),
            external_script=external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
        )
        if expected_artifact_sha256:
            command += (
                f" --artifact-sha256 {shlex.quote(expected_artifact_sha256)}"
                f" --artifact-manifest {shlex.quote(str(remote['artifact_manifest_path']))}"
            )
        return _ssh_command(target=target, known_hosts=known, remote_command=command)

    runtime_identity_read = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=(
            f"test ! -L {shlex.quote(runtime_identity_path)} && "
            f"test -f {shlex.quote(runtime_identity_path)} && "
            f"cat {shlex.quote(runtime_identity_path)}"
        ),
    )

    def log_validate(checkpoint_path: str) -> list[str]:
        markers = " ".join(
            f"--marker {shlex.quote(str(marker))}" for marker in remote["startup_markers"]
        )
        command = _verified_external_exec(
            command=(
            f"env NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
            f"PYTHONPATH={shlex.quote(repo_root)} {shlex.quote(python)} "
            f"{shlex.quote(external_script)} log-validate --log "
            f"{shlex.quote(str(remote['log_path']))} --checkpoint "
            f"{shlex.quote(checkpoint_path)} {markers}"
            ),
            external_script=external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
        )
        return _ssh_command(target=target, known_hosts=known, remote_command=command)

    disabled = [
        *common_pre_stop(disabled_checkpoint),
        _command("stop-live", stop_disabled, mutates=True, after_stop=True),
        _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
        _command("checkout-frozen-runtime", checkout_command, mutates=True, after_stop=True),
        _command("start-disabled", start_disabled, mutates=True, after_stop=True),
        _command(
            "fresh-disabled-process-probe",
            process_probe(disabled_config, str(configs["disabled"]["config_sha256"]), False),
            mutates=False,
            after_stop=True,
        ),
        _command(
            "read-disabled-runtime-identity",
            runtime_identity_read,
            mutates=False,
            after_stop=True,
        ),
        _command(
            "validate-disabled-startup-log",
            log_validate(disabled_checkpoint),
            mutates=False,
            after_stop=True,
        ),
    ]
    activate = [
        *common_pre_stop(active_checkpoint),
        _command(
            "reprobe-disabled-process-before-stop",
            process_probe(disabled_config, str(configs["disabled"]["config_sha256"]), False),
            mutates=False,
        ),
        _command(
            "read-pre-stop-disabled-runtime-identity",
            runtime_identity_read,
            mutates=False,
        ),
        _command("stop-live", stop_disabled, mutates=True, after_stop=True),
        _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
        _command("start-active-restart-only", start_active, mutates=True, after_stop=True),
        _command(
            "fresh-active-process-probe",
            process_probe(active_config, str(configs["active"]["config_sha256"]), True),
            mutates=False,
            after_stop=True,
        ),
        _command(
            "read-active-runtime-identity",
            runtime_identity_read,
            mutates=False,
            after_stop=True,
        ),
        _command(
            "validate-active-startup-log",
            log_validate(active_checkpoint),
            mutates=False,
            after_stop=True,
        ),
    ]

    def rollback_commands(name: str, stop_command: Sequence[str]) -> list[dict[str, Any]]:
        identity = rollback[name]
        rollback_checkout = _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=(
                f"cd {shlex.quote(repo_root)} && git checkout --detach "
                f"{shlex.quote(str(identity['execution_commit']))} && "
                f'test "$(git rev-parse HEAD^{{tree}})" = '
                f"{shlex.quote(str(identity['execution_tree']))}"
            ),
        )
        rollback_start = _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=_remote_external_config_start(
                repo_root, str(identity["config_path"]), owner_override=False
            ),
        )
        return [
            _command(
                "capture-old-pid",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=(
                        f"test -s {shlex.quote(pid_file)} && cat {shlex.quote(pid_file)}"
                    ),
                ),
                mutates=False,
            ),
            _command("stop-live", stop_command, mutates=True, after_stop=True),
            _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
            _command("checkout-rollback-runtime", rollback_checkout, mutates=True, after_stop=True),
            _command("start-rollback-fresh-b0", rollback_start, mutates=True, after_stop=True),
            _command(
                "fresh-rollback-process-probe",
                process_probe(
                    str(identity["config_path"]),
                    str(identity["config_sha256"]),
                    False,
                    expected_execution=identity,
                    expected_runtime_code_sha256=str(identity["runtime_code_sha256"]),
                    expected_artifact_sha256=str(identity.get("artifact_sha256", "")),
                ),
                mutates=False,
                after_stop=True,
            ),
        ]

    return {
        "disabled-deploy": disabled,
        "activate": activate,
        "rollback-primary": rollback_commands("primary_disabled", stop_active),
        "rollback-deep": rollback_commands("deep_predecessor", stop_active),
    }


def build_plan(
    *,
    specification: Mapping[str, Any],
    repository_root: Path,
    preflight_runner: PreflightRunner | None = None,
) -> dict[str, Any]:
    """Build and validate a deterministic plan without any remote command."""

    _reject_remote_environment_override()
    root = repository_root.expanduser().resolve(strict=True)
    execution_raw = specification.get("execution")
    artifact_raw = specification.get("artifact")
    configs_raw = specification.get("configs")
    pointer_raw = specification.get("active_pointer")
    ssh_raw = specification.get("ssh")
    host_raw = specification.get("host")
    remote_raw = specification.get("remote")
    rollback_raw = specification.get("rollback_identities")
    token_raw = specification.get("phase_token_sha256")
    for label, value in (
        ("execution", execution_raw),
        ("artifact", artifact_raw),
        ("configs", configs_raw),
        ("active_pointer", pointer_raw),
        ("ssh", ssh_raw),
        ("host", host_raw),
        ("remote", remote_raw),
        ("rollback_identities", rollback_raw),
        ("phase_token_sha256", token_raw),
    ):
        if not isinstance(value, Mapping):
            raise BuyE3TransactionalDeployError(f"specification lacks {label}")
    execution = gate_v2.verify_execution_git_identity(
        repository_root=root,
        expected_commit=str(execution_raw["commit"]),
        expected_tree=str(execution_raw["tree"]),
        annotated_tag=str(execution_raw["annotated_tag"]),
        expected_tag_object=str(execution_raw["annotated_tag_object"]),
    )
    manifest_path = Path(str(artifact_raw["manifest_path"])).expanduser().resolve(strict=True)
    policy_path = Path(str(artifact_raw["policy_path"])).expanduser().resolve(strict=True)
    bundle_path = Path(str(artifact_raw["predicate_bundle_path"])).expanduser().resolve(strict=True)
    artifact_manifest = _read_json(manifest_path)
    policy_payload = _read_json(policy_path)
    if (
        policy_payload.get("bindings", {}).get("owner_execution_commit")
        != execution["execution_commit"]
    ):
        raise BuyE3TransactionalDeployError("policy artifact binds another execution commit")
    runtime_sources = gate_v2.verify_runtime_sources(
        repository_root=root,
        execution_commit=execution["execution_commit"],
        artifact_manifest=artifact_manifest,
    )
    disabled_config = Path(str(configs_raw["disabled_path"])).expanduser().resolve(strict=True)
    active_config = Path(str(configs_raw["active_path"])).expanduser().resolve(strict=True)
    config_binding = gate_v2.validate_private_config_pair(
        disabled_config_path=disabled_config,
        active_config_path=active_config,
        repository_root=root,
        allowed_diff=tuple(configs_raw.get("allowed_diff", ())),
    )
    if (
        config_binding["disabled"]["artifact_files"]["manifest"]["path"] != str(manifest_path)
        or config_binding["disabled"]["artifact_files"]["policy"]["path"] != str(policy_path)
        or config_binding["disabled"]["artifact_files"]["predicate_bundle"]["path"]
        != str(bundle_path)
    ):
        raise BuyE3TransactionalDeployError("specification artifact paths differ from config")
    pointer = load_sha_bound_active_pointer(
        pointer_path=Path(str(pointer_raw["path"])),
        expected_file_sha256=str(pointer_raw["file_sha256"]),
    )
    known_hosts = bind_known_hosts(
        known_hosts_path=Path(str(ssh_raw["known_hosts_path"])),
        expected_file_sha256=str(ssh_raw["known_hosts_file_sha256"]),
        expected_fingerprint=str(ssh_raw["host_key_fingerprint"]),
    )
    host = dict(host_raw)
    for field in ("logical_host", "repo_root", "python_executable", "venv_root"):
        if not str(host.get(field, "")).strip():
            raise BuyE3TransactionalDeployError(f"host identity lacks {field}")
    if str(host["repo_root"]) != pointer["repo_root"]:
        raise BuyE3TransactionalDeployError("host repo root differs from active pointer")
    remote = dict(remote_raw)
    for field in (
        "stage_root",
        "disabled_config_path",
        "active_config_path",
        "artifact_manifest_path",
        "policy_path",
        "predicate_bundle_path",
        "pid_file",
        "log_path",
        "runtime_identity_path",
        "startup_checkpoint_path",
    ):
        if not str(remote.get(field, "")).startswith("/"):
            raise BuyE3TransactionalDeployError(f"remote path is not absolute: {field}")
    startup_markers = remote.get("startup_markers")
    if (
        not isinstance(startup_markers, list)
        or not startup_markers
        or any(not str(marker).strip() for marker in startup_markers)
    ):
        raise BuyE3TransactionalDeployError("remote startup markers are not frozen")
    disabled_strategy = _strategy_mapping(disabled_config)
    active_strategy = _strategy_mapping(active_config)
    remote_artifact_fields = {
        "buy_e3_cooldown_artifact_manifest_path": "artifact_manifest_path",
        "buy_e3_cooldown_policy_path": "policy_path",
        "buy_e3_cooldown_predicate_bundle_path": "predicate_bundle_path",
    }
    for config_field, remote_field in remote_artifact_fields.items():
        disabled_value = str(disabled_strategy.get(config_field, "")).strip()
        active_value = str(active_strategy.get(config_field, "")).strip()
        if not disabled_value or disabled_value != active_value:
            raise BuyE3TransactionalDeployError(
                f"disabled/active remote artifact path differs: {config_field}"
            )
        relative = PurePosixPath(disabled_value)
        if relative.is_absolute() or ".." in relative.parts:
            raise BuyE3TransactionalDeployError(
                f"deploy config artifact path is not repository-relative: {config_field}"
            )
        expected_remote = str(PurePosixPath(pointer["repo_root"]) / relative)
        if str(remote[remote_field]) != expected_remote:
            raise BuyE3TransactionalDeployError(
                f"remote artifact destination differs from config: {config_field}"
            )
    rollback = {
        "primary_disabled": _validate_rollback_identity(
            "primary_disabled", rollback_raw.get("primary_disabled")
        ),
        "deep_predecessor": _validate_rollback_identity(
            "deep_predecessor", rollback_raw.get("deep_predecessor")
        ),
    }
    if rollback["primary_disabled"]["identity"] == rollback["deep_predecessor"]["identity"]:
        raise BuyE3TransactionalDeployError("dual rollback identities are not distinct")
    primary = rollback["primary_disabled"]
    if (
        primary["execution_commit"] != execution["execution_commit"]
        or primary["execution_tree"] != execution["execution_tree"]
        or primary["config_path"] != remote["disabled_config_path"]
        or primary["config_sha256"] != config_binding["disabled"]["config_sha256"]
        or primary["runtime_code_sha256"] != runtime_sources["runtime_code_sha256"]
        or primary["python_executable"] != host["python_executable"]
        or primary["venv_root"] != host["venv_root"]
    ):
        raise BuyE3TransactionalDeployError("primary disabled rollback is not exact attempt2")
    phase_tokens = {
        phase: _require_sha256(token_raw.get(phase), f"token hash {phase}") for phase in PHASES
    }
    runner = preflight_runner or (
        lambda repo, config, enabled: run_isolated_preflight(repo, config, enabled)
    )
    disabled_preflight = dict(runner(root, disabled_config, False))
    active_preflight = dict(runner(root, active_config, True))
    expected_storage_gates = {
        False: _host_bound_spool_gate(
            disabled_config, defer_host_bound_spool=True
        ),
        True: _host_bound_spool_gate(
            active_config, defer_host_bound_spool=True
        ),
    }
    for payload, enabled in ((disabled_preflight, False), (active_preflight, True)):
        storage_gate = payload.get("host_bound_storage_gate")
        if (
            payload.get("schema_version") != PREFLIGHT_SCHEMA
            or payload.get("status") != "isolated_config_preflight_passed"
            or payload.get("expected_enabled") is not enabled
            or payload.get("artifact_loaded_with_from_files") is not True
            or not isinstance(storage_gate, Mapping)
            or set(storage_gate) != _HOST_BOUND_STORAGE_GATE_FIELDS
            or dict(storage_gate) != expected_storage_gates[enabled]
            or payload.get("canonical_preflight_sha256")
            != gate_v2.document_sha256(payload, "canonical_preflight_sha256")
        ):
            raise BuyE3TransactionalDeployError("isolated preflight did not pass exactly")
    artifact_binding = {
        "artifact_sha256": config_binding["disabled"]["artifact_sha256"],
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": gate_v2.file_sha256(manifest_path),
        "policy_path": str(policy_path),
        "policy_file_sha256": gate_v2.file_sha256(policy_path),
        "predicate_bundle_path": str(bundle_path),
        "predicate_bundle_file_sha256": gate_v2.file_sha256(bundle_path),
    }
    local_package = {
        "deploy_script": str(Path(__file__).resolve()),
        "gate_amendment": str(Path(gate_v2.__file__).resolve()),
        "artifact_manifest": str(manifest_path),
        "policy": str(policy_path),
        "predicate_bundle": str(bundle_path),
        "disabled_config": str(disabled_config),
        "active_config": str(active_config),
    }
    external_files = {
        role: {"path": path, "file_sha256": gate_v2.file_sha256(Path(path))}
        for role, path in local_package.items()
    }
    external_tools = {
        "files": external_files,
        "content_package_sha256": gate_v2.canonical_sha256(
            {role: binding["file_sha256"] for role, binding in external_files.items()}
        ),
    }
    commands = _phase_commands(
        pointer=pointer,
        known_hosts=known_hosts,
        host=host,
        configs=config_binding,
        remote=remote,
        execution=execution,
        rollback=rollback,
        runtime_sources=runtime_sources,
        artifact=artifact_binding,
        local_package=local_package,
    )
    for phase, rows in commands.items():
        stop_positions = [index for index, row in enumerate(rows) if row["label"] == "stop-live"]
        preflights = [index for index, row in enumerate(rows) if "preflight" in row["label"]]
        if phase in {"disabled-deploy", "activate"} and (
            len(stop_positions) != 1 or len(preflights) != 2 or max(preflights) >= stop_positions[0]
        ):
            raise BuyE3TransactionalDeployError("both isolated preflights must precede stop")
        for row in rows:
            argv = row["argv"]
            if argv[0] not in {"ssh", "rsync"} or "StrictHostKeyChecking=yes" not in " ".join(argv):
                raise BuyE3TransactionalDeployError("remote command lacks strict SSH")
    plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA,
        "status": "plan_only_no_remote_command_executed",
        "planner_repository_root": str(root),
        "execution": execution,
        "runtime_sources": runtime_sources,
        "artifact": artifact_binding,
        "external_tools_and_package": external_tools,
        "configs": config_binding,
        "isolated_preflights": {
            "disabled": disabled_preflight,
            "active": active_preflight,
        },
        "active_pointer": pointer,
        "ssh": {
            **known_hosts,
            "strict_options": list(STRICT_SSH_OPTIONS),
            "environment_pointer_override_allowed": False,
        },
        "host": host,
        "remote": remote,
        "rollback_identities": rollback,
        "phase_token_sha256": phase_tokens,
        "phases": commands,
        "transaction_contract": dict(TRANSACTION_CONTRACT),
        "runtime_attestation_contract": _runtime_attestation_contract(remote),
        "evidence_boundary": dict(PLAN_EVIDENCE_BOUNDARY),
    }
    plan["plan_core_sha256"] = _plan_core_sha256(plan)
    activation_gate_raw = specification.get("activation_gate")
    if activation_gate_raw is not None:
        if not isinstance(activation_gate_raw, Mapping):
            raise BuyE3TransactionalDeployError("activation gate binding is malformed")
        activation_path = Path(str(activation_gate_raw.get("path", ""))).expanduser()
        expected_file_sha = _require_sha256(
            activation_gate_raw.get("file_sha256"), "activation gate file hash"
        )
        if gate_v2.file_sha256(activation_path.resolve(strict=True)) != expected_file_sha:
            raise BuyE3TransactionalDeployError("activation gate file hash drifted")
        activation_receipt = gate_v2.validate_amended_gate_receipt(activation_path)
        expected_cross_binding = _activation_gate_cross_binding(
            execution=execution,
            runtime_sources=runtime_sources,
            artifact=artifact_binding,
            configs=config_binding,
            pointer=pointer,
            known_hosts=known_hosts,
            host=host,
            rollback=rollback,
        )
        cross_binding_sha256 = _require_activation_gate_cross_binding(
            activation_receipt, expected_cross_binding
        )
        activation_gate_binding: dict[str, Any] = {
            "path": str(activation_path.resolve(strict=True)),
            "file_sha256": expected_file_sha,
            "canonical_receipt_sha256": activation_receipt[
                "canonical_amendment_receipt_sha256"
            ],
            "cross_binding_sha256": cross_binding_sha256,
            "plan_core_sha256": plan["plan_core_sha256"],
            "transaction_contract_sha256": gate_v2.canonical_sha256(
                plan["transaction_contract"]
            ),
        }
        activation_gate_binding["canonical_activation_binding_sha256"] = (
            gate_v2.document_sha256(
                activation_gate_binding, "canonical_activation_binding_sha256"
            )
        )
        plan["activation_gate"] = activation_gate_binding
        plan["activation_gate_receipt_sha256"] = activation_gate_binding["canonical_receipt_sha256"]
    plan["canonical_plan_sha256"] = gate_v2.document_sha256(plan, "canonical_plan_sha256")
    validate_plan(plan)
    return plan


def validate_plan(plan: Mapping[str, Any]) -> None:
    if not isinstance(plan, Mapping):
        raise BuyE3TransactionalDeployError("deployment plan is malformed")
    has_activation = "activation_gate" in plan or "activation_gate_receipt_sha256" in plan
    expected_fields = _PLAN_BASE_FIELDS | (_PLAN_ACTIVATION_FIELDS if has_activation else set())
    if set(plan) != expected_fields:
        raise BuyE3TransactionalDeployError("deployment plan fields drifted")
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("status") != "plan_only_no_remote_command_executed"
        or plan.get("execution", {}).get("execution_commit")
        != gate_v2.FROZEN_EXECUTION_COMMIT
        or plan.get("transaction_contract") != TRANSACTION_CONTRACT
        or plan.get("evidence_boundary") != PLAN_EVIDENCE_BOUNDARY
        or plan.get("runtime_attestation_contract")
        != _runtime_attestation_contract(plan.get("remote", {}))
        or plan.get("plan_core_sha256") != _plan_core_sha256(plan)
    ):
        raise BuyE3TransactionalDeployError("deployment plan core identity drifted")
    token_hashes = plan.get("phase_token_sha256")
    if not isinstance(token_hashes, Mapping) or set(token_hashes) != set(PHASES):
        raise BuyE3TransactionalDeployError("deployment plan phase token fields drifted")
    for phase in PHASES:
        _require_sha256(token_hashes[phase], f"plan token hash {phase}")
    package = plan.get("external_tools_and_package")
    if not isinstance(package, Mapping) or set(package) != {
        "files",
        "content_package_sha256",
    }:
        raise BuyE3TransactionalDeployError("deployment package fields drifted")
    files = package.get("files")
    if not isinstance(files, Mapping) or set(files) != set(_EXTERNAL_PACKAGE_ROLES):
        raise BuyE3TransactionalDeployError("deployment package roles drifted")
    local_package: dict[str, str] = {}
    package_hashes: dict[str, str] = {}
    for role in _EXTERNAL_PACKAGE_ROLES:
        binding = files.get(role)
        if not isinstance(binding, Mapping) or set(binding) != {"path", "file_sha256"}:
            raise BuyE3TransactionalDeployError(f"deployment package binding drifted: {role}")
        local_package[role] = str(binding["path"])
        package_hashes[role] = _require_sha256(
            binding["file_sha256"], f"deployment package hash {role}"
        )
    if package.get("content_package_sha256") != gate_v2.canonical_sha256(package_hashes):
        raise BuyE3TransactionalDeployError("deployment package aggregate drifted")
    expected_commands = _phase_commands(
        pointer=plan["active_pointer"],
        known_hosts=plan["ssh"],
        host=plan["host"],
        configs=plan["configs"],
        remote=plan["remote"],
        execution=plan["execution"],
        rollback=plan["rollback_identities"],
        runtime_sources=plan["runtime_sources"],
        artifact=plan["artifact"],
        local_package=local_package,
    )
    if plan.get("phases") != expected_commands:
        raise BuyE3TransactionalDeployError("deployment phase commands drifted")
    for rows in expected_commands.values():
        for row in rows:
            if set(row) != _COMMAND_FIELDS or row["command_sha256"] != gate_v2.canonical_sha256(
                row["argv"]
            ):
                raise BuyE3TransactionalDeployError("deployment command core drifted")
    if has_activation:
        activation = plan.get("activation_gate")
        if not isinstance(activation, Mapping) or set(activation) != _ACTIVATION_GATE_BINDING_FIELDS:
            raise BuyE3TransactionalDeployError("activation gate plan binding is incomplete")
        if (
            activation.get("plan_core_sha256") != plan["plan_core_sha256"]
            or activation.get("transaction_contract_sha256")
            != gate_v2.canonical_sha256(plan["transaction_contract"])
            or activation.get("canonical_receipt_sha256")
            != plan.get("activation_gate_receipt_sha256")
            or activation.get("canonical_activation_binding_sha256")
            != gate_v2.document_sha256(
                activation, "canonical_activation_binding_sha256"
            )
        ):
            raise BuyE3TransactionalDeployError("activation gate plan/core binding drifted")
    if plan.get("canonical_plan_sha256") != gate_v2.document_sha256(
        plan, "canonical_plan_sha256"
    ):
        raise BuyE3TransactionalDeployError("deployment plan canonical identity drifted")


def _revalidate_plan_inputs(plan: Mapping[str, Any]) -> None:
    validate_plan(plan)
    _reject_remote_environment_override()
    tools = plan.get("external_tools_and_package")
    if not isinstance(tools, Mapping):
        raise BuyE3TransactionalDeployError("plan lacks external package bindings")
    files = tools.get("files")
    if not isinstance(files, Mapping):
        raise BuyE3TransactionalDeployError("plan lacks external package file bindings")
    observed_hashes: dict[str, str] = {}
    for role, binding in files.items():
        if not isinstance(binding, Mapping):
            raise BuyE3TransactionalDeployError(f"external package binding malformed: {role}")
        path = Path(str(binding.get("path", ""))).expanduser()
        if path.is_symlink() or not path.is_file():
            raise BuyE3TransactionalDeployError(f"external package file unavailable: {role}")
        observed_hash = gate_v2.file_sha256(path.resolve(strict=True))
        if observed_hash != binding.get("file_sha256"):
            raise BuyE3TransactionalDeployError(f"external package file drifted: {role}")
        observed_hashes[str(role)] = observed_hash
    if gate_v2.canonical_sha256(observed_hashes) != tools.get("content_package_sha256"):
        raise BuyE3TransactionalDeployError("external package aggregate drifted")
    pointer = plan["active_pointer"]
    load_sha_bound_active_pointer(
        pointer_path=Path(str(pointer["path"])),
        expected_file_sha256=str(pointer["file_sha256"]),
    )
    ssh = plan["ssh"]
    bind_known_hosts(
        known_hosts_path=Path(str(ssh["path"])),
        expected_file_sha256=str(ssh["file_sha256"]),
        expected_fingerprint=str(ssh["expected_fingerprint"]),
    )
    root = Path(str(plan["planner_repository_root"])).expanduser().resolve(strict=True)
    execution = plan["execution"]
    gate_v2.verify_execution_git_identity(
        repository_root=root,
        expected_commit=str(execution["execution_commit"]),
        expected_tree=str(execution["execution_tree"]),
        annotated_tag=str(execution["annotated_tag"]),
        expected_tag_object=str(execution["annotated_tag_object"]),
    )
    manifest = _read_json(Path(str(plan["artifact"]["manifest_path"])))
    runtime = gate_v2.verify_runtime_sources(
        repository_root=root,
        execution_commit=str(execution["execution_commit"]),
        artifact_manifest=manifest,
    )
    if runtime.get("runtime_code_sha256") != plan["runtime_sources"].get("runtime_code_sha256"):
        raise BuyE3TransactionalDeployError("runtime source aggregate drifted")
    activation = plan.get("activation_gate")
    activation_receipt_sha256 = plan.get("activation_gate_receipt_sha256")
    if (activation is None) != (activation_receipt_sha256 is None):
        raise BuyE3TransactionalDeployError("activation gate plan binding is incomplete")
    if activation is not None:
        if not isinstance(activation, Mapping):
            raise BuyE3TransactionalDeployError("activation gate plan binding is malformed")
        path = Path(str(activation["path"]))
        if gate_v2.file_sha256(path.resolve(strict=True)) != activation["file_sha256"]:
            raise BuyE3TransactionalDeployError("activation gate bytes drifted")
        receipt = gate_v2.validate_amended_gate_receipt(path)
        expected_cross_binding = _activation_gate_cross_binding(
            execution=plan["execution"],
            runtime_sources=plan["runtime_sources"],
            artifact=plan["artifact"],
            configs=plan["configs"],
            pointer=plan["active_pointer"],
            known_hosts=plan["ssh"],
            host=plan["host"],
            rollback=plan["rollback_identities"],
        )
        cross_binding_sha256 = _require_activation_gate_cross_binding(
            receipt, expected_cross_binding
        )
        if cross_binding_sha256 != activation.get("cross_binding_sha256"):
            raise BuyE3TransactionalDeployError("activation gate plan binding drifted")
        if receipt.get("canonical_amendment_receipt_sha256") != activation_receipt_sha256:
            raise BuyE3TransactionalDeployError("activation gate canonical binding drifted")
        if (
            activation.get("plan_core_sha256") != plan["plan_core_sha256"]
            or activation.get("transaction_contract_sha256")
            != gate_v2.canonical_sha256(plan["transaction_contract"])
        ):
            raise BuyE3TransactionalDeployError("activation gate transactional binding drifted")


def phase_authorization_token_sha256(token: str) -> str:
    if not token:
        raise BuyE3TransactionalDeployError("empty phase token")
    return _sha256_text(token)


def _read_phase_token(*, token_file: Path | None, token_fd: int | None) -> str:
    if (token_file is None) == (token_fd is None):
        raise BuyE3TransactionalDeployError(
            "exactly one token file or token file descriptor is required"
        )
    close_fd = False
    if token_file is not None:
        path = token_file.expanduser()
        if path.is_symlink() or not path.is_file():
            raise BuyE3TransactionalDeployError("phase token file is not a regular file")
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        close_fd = True
    else:
        descriptor = int(token_fd)
        if descriptor < 0:
            raise BuyE3TransactionalDeployError("phase token file descriptor is invalid")
    try:
        metadata = os.fstat(descriptor)
        if stat.S_ISREG(metadata.st_mode) and (
            stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.getuid()
        ):
            raise BuyE3TransactionalDeployError(
                "phase token regular file descriptor must be owner-only 0600"
            )
        chunks: list[bytes] = []
        total = 0
        while total <= 4096:
            chunk = os.read(descriptor, min(4097 - total, 4096))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > 4096:
            raise BuyE3TransactionalDeployError("phase token exceeds the bounded size")
        try:
            token = b"".join(chunks).decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError as exc:
            raise BuyE3TransactionalDeployError("phase token is not UTF-8") from exc
        if not token or "\n" in token or "\r" in token or "\x00" in token:
            raise BuyE3TransactionalDeployError("phase token content is malformed")
        return token
    finally:
        if close_fd:
            os.close(descriptor)


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True)


def _expected_commands(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "label": str(row["label"]),
            "command_sha256": _require_sha256(row["command_sha256"], "command hash"),
        }
        for row in rows
    ]


def _automatic_rollback_rows(plan: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        row
        for row in plan["phases"]["rollback-primary"]
        if row["label"] != "capture-old-pid"
    ]


def _expected_process_binding(plan: Mapping[str, Any], phase: str) -> dict[str, Any]:
    if phase in {"disabled-deploy", "activate"}:
        config_name = "active" if phase == "activate" else "disabled"
        return {
            "enabled": phase == "activate",
            "config_path": plan["remote"][f"{config_name}_config_path"],
            "config_sha256": plan["configs"][config_name]["config_sha256"],
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "artifact_sha256": plan["artifact"]["artifact_sha256"],
            "runtime_code_sha256": plan["runtime_sources"]["runtime_code_sha256"],
            "repo_root": plan["active_pointer"]["repo_root"],
            "python_executable": plan["host"]["python_executable"],
            "venv_root": plan["host"]["venv_root"],
        }
    rollback_name = {
        "rollback-primary": "primary_disabled",
        "rollback-deep": "deep_predecessor",
    }.get(phase)
    if rollback_name is None:
        raise BuyE3TransactionalDeployError("process identity phase is unknown")
    identity = plan["rollback_identities"][rollback_name]
    return {
        "enabled": False,
        "config_path": identity["config_path"],
        "config_sha256": identity["config_sha256"],
        "execution_commit": identity["execution_commit"],
        "execution_tree": identity["execution_tree"],
        "artifact_sha256": str(identity.get("artifact_sha256", "")),
        "runtime_code_sha256": identity["runtime_code_sha256"],
        "repo_root": plan["active_pointer"]["repo_root"],
        "python_executable": identity["python_executable"],
        "venv_root": identity["venv_root"],
    }


def _validate_actual_process_identity(
    process: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    phase: str,
    old_pid: int | None,
    require_fresh: bool = True,
) -> dict[str, Any]:
    if set(process) != _PROCESS_IDENTITY_FIELDS:
        raise BuyE3TransactionalDeployError("actual process identity fields drifted")
    if (
        process.get("schema_version") != gate_v2.PROCESS_IDENTITY_SCHEMA
        or process.get("canonical_process_identity_sha256")
        != gate_v2.document_sha256(process, "canonical_process_identity_sha256")
    ):
        raise BuyE3TransactionalDeployError("actual process identity hash drifted")
    expected = _expected_process_binding(plan, phase)
    try:
        pid = int(process.get("pid", -1))
        start_ticks = int(process.get("pid_start_ticks", -1))
    except (TypeError, ValueError) as exc:
        raise BuyE3TransactionalDeployError("actual process PID identity is malformed") from exc
    if pid <= 0 or start_ticks <= 0:
        raise BuyE3TransactionalDeployError("actual process PID identity is malformed")
    if require_fresh and (old_pid is None or pid == old_pid):
        raise BuyE3TransactionalDeployError("actual process PID is not fresh")
    cmdline = process.get("cmdline")
    runtime_identity = process.get("runtime_identity")
    if (
        not isinstance(cmdline, list)
        or not cmdline
        or process.get("cmdline_sha256") != gate_v2.canonical_sha256(cmdline)
        or not isinstance(runtime_identity, Mapping)
        or runtime_identity.get("present") is not True
        or runtime_identity.get("path") != plan["remote"]["runtime_identity_path"]
        or runtime_identity.get("schema_version") != RUNTIME_IDENTITY_SCHEMA
    ):
        raise BuyE3TransactionalDeployError("actual runtime process binding is malformed")
    runtime_identity_file_sha256 = _require_sha256(
        runtime_identity.get("file_sha256"), "runtime identity file hash"
    )
    if (
        _require_sha256(
            process.get("runtime_identity_file_sha256"),
            "process-bound runtime identity file hash",
        )
        != runtime_identity_file_sha256
    ):
        raise BuyE3TransactionalDeployError(
            "actual process runtime identity file hash drifted"
        )
    _require_sha256(
        process.get("startup_attestation_sha256"),
        "process-bound startup attestation hash",
    )
    exact_fields = {
        "cwd": expected["repo_root"],
        "config_path": expected["config_path"],
        "config_sha256": expected["config_sha256"],
        "python_executable": expected["python_executable"],
        "venv_root": expected["venv_root"],
        "execution_commit": expected["execution_commit"],
        "execution_tree": expected["execution_tree"],
        "artifact_sha256": expected["artifact_sha256"],
        "runtime_code_sha256": expected["runtime_code_sha256"],
    }
    if any(process.get(field) != value for field, value in exact_fields.items()):
        raise BuyE3TransactionalDeployError("actual process artifact/runtime/config identity drifted")
    if (
        process.get("buy_e3_enabled") is not expected["enabled"]
        or process.get("owner_override_effective") is not expected["enabled"]
        or process.get("initial_buy_deadline_identity") != "B0"
        or process.get("e3_deadline_imported") is not False
    ):
        raise BuyE3TransactionalDeployError("actual process authority/deadline identity drifted")
    if not str(process.get("captured_utc", "")).strip() or not str(
        process.get("python_binary_resolved", "")
    ).strip():
        raise BuyE3TransactionalDeployError("actual process capture identity is incomplete")
    return dict(process)


def _classify_process_error(exc: Exception) -> str:
    message = str(exc)
    if "probe" in message or "not JSON" in message:
        return "process_probe_invalid"
    if "fresh" in message or "PID" in message:
        return "fresh_pid_required"
    if "authority/deadline" in message:
        return "process_authority_or_deadline_mismatch"
    return "process_identity_invalid"


def _parse_process_probe(
    stdout: str,
    *,
    plan: Mapping[str, Any],
    phase: str,
    old_pid: int | None,
    require_fresh: bool = True,
) -> dict[str, Any]:
    try:
        process = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise BuyE3TransactionalDeployError("fresh process probe is not JSON") from exc
    if not isinstance(process, dict):
        raise BuyE3TransactionalDeployError("fresh process probe is malformed")
    return _validate_actual_process_identity(
        process,
        plan=plan,
        phase=phase,
        old_pid=old_pid,
        require_fresh=require_fresh,
    )


def _process_handoff_identity(process: Mapping[str, Any]) -> dict[str, Any]:
    runtime_identity = process.get("runtime_identity")
    if not isinstance(runtime_identity, Mapping):
        raise BuyE3TransactionalDeployError("disabled process runtime identity is malformed")
    return {
        "pid": int(process["pid"]),
        "pid_start_ticks": int(process["pid_start_ticks"]),
        "cmdline": process["cmdline"],
        "cmdline_sha256": process["cmdline_sha256"],
        "cwd": process["cwd"],
        "config_path": process["config_path"],
        "config_sha256": process["config_sha256"],
        "python_executable": process["python_executable"],
        "python_binary_resolved": process["python_binary_resolved"],
        "venv_root": process["venv_root"],
        "runtime_identity_path": runtime_identity["path"],
        "runtime_identity_file_sha256": process[
            "runtime_identity_file_sha256"
        ],
        "startup_attestation_sha256": process[
            "startup_attestation_sha256"
        ],
        "execution_commit": process["execution_commit"],
        "execution_tree": process["execution_tree"],
        "artifact_sha256": process["artifact_sha256"],
        "runtime_code_sha256": process["runtime_code_sha256"],
        "buy_e3_enabled": process["buy_e3_enabled"],
        "owner_override_effective": process["owner_override_effective"],
        "initial_buy_deadline_identity": process["initial_buy_deadline_identity"],
        "e3_deadline_imported": process["e3_deadline_imported"],
    }


def _require_same_disabled_process(
    prior: Mapping[str, Any], current: Mapping[str, Any]
) -> None:
    if _process_handoff_identity(prior) != _process_handoff_identity(current):
        raise BuyE3TransactionalDeployError("disabled process handoff identity drifted")


def _load_disabled_phase_receipt_binding(
    receipt_path: Path, *, plan: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    target = receipt_path.expanduser().absolute()
    try:
        receipt = validate_phase_receipt(
            target, plan=plan, expected_phase="disabled-deploy"
        )
    except Exception as exc:
        raise BuyE3TransactionalDeployError(
            "activation disabled phase receipt is invalid"
        ) from exc
    if receipt.get("status") != PHASE_COMPLETE:
        raise BuyE3TransactionalDeployError(
            "activation disabled phase receipt is not complete"
        )
    process = receipt.get("actual_process_identity")
    startup_binding = receipt.get("actual_startup_attestation")
    if not isinstance(process, Mapping):
        raise BuyE3TransactionalDeployError(
            "activation disabled phase receipt lacks a process"
        )
    if not isinstance(startup_binding, Mapping):
        raise BuyE3TransactionalDeployError(
            "activation disabled phase receipt lacks runtime startup evidence"
        )
    binding = {
        "path": str(target.resolve(strict=True)),
        "file_sha256": gate_v2.file_sha256(target),
        "canonical_receipt_sha256": receipt["canonical_receipt_sha256"],
        "plan_sha256": receipt["plan_sha256"],
        "process_identity_sha256": process["canonical_process_identity_sha256"],
        "pid": int(process["pid"]),
        "pid_start_ticks": int(process["pid_start_ticks"]),
        "config_sha256": process["config_sha256"],
        "artifact_sha256": process["artifact_sha256"],
        "runtime_code_sha256": process["runtime_code_sha256"],
        "execution_commit": process["execution_commit"],
        "execution_tree": process["execution_tree"],
        "runtime_identity_file_sha256": startup_binding[
            "runtime_identity_file_sha256"
        ],
        "startup_attestation_sha256": startup_binding[
            "startup_attestation_sha256"
        ],
    }
    return binding, dict(process)


def _validate_runtime_identity_stdout(
    stdout: str,
    *,
    plan: Mapping[str, Any],
    process: Mapping[str, Any],
    process_phase: str,
) -> dict[str, Any]:
    try:
        runtime = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise BuyE3TransactionalDeployError("runtime identity file is not JSON") from exc
    runtime_identity = process.get("runtime_identity")
    if not isinstance(runtime_identity, Mapping):
        raise BuyE3TransactionalDeployError("runtime identity process binding is malformed")
    file_sha256 = _sha256_text(stdout)
    if (
        runtime_identity.get("path")
        != plan["runtime_attestation_contract"]["remote_path"]
        or runtime_identity.get("file_sha256") != file_sha256
        or process.get("runtime_identity_file_sha256") != file_sha256
    ):
        raise BuyE3TransactionalDeployError(
            "runtime identity stdout differs from process-bound file bytes"
        )
    expected = _expected_process_binding(plan, process_phase)
    attestation = _validate_runtime_identity_authority(
        runtime,
        expected_pid=int(process["pid"]),
        expected_config_path=expected["config_path"],
        expected_config_sha256=expected["config_sha256"],
        expected_python_executable=expected["python_executable"],
        expected_python_binary_resolved=str(process["python_binary_resolved"]),
        expected_enabled=bool(expected["enabled"]),
        expected_artifact_sha256=expected["artifact_sha256"],
        expected_execution_commit=expected["execution_commit"],
        expected_execution_tree=expected["execution_tree"],
        expected_runtime_sources=plan["runtime_sources"],
    )
    startup_attestation_sha256 = gate_v2.canonical_sha256(attestation)
    if process.get("startup_attestation_sha256") != startup_attestation_sha256:
        raise BuyE3TransactionalDeployError(
            "runtime startup attestation differs from process-bound hash"
        )
    binding: dict[str, Any] = {
        "schema_version": RUNTIME_IDENTITY_BINDING_SCHEMA,
        "authority": "runtime_written_startup_attestation",
        "evidence_classification": (
            "runtime_identity_file_unsigned_structural_evidence"
        ),
        "cryptographic_signature_present": False,
        "runtime_identity_path": runtime_identity["path"],
        "runtime_identity_file_sha256": file_sha256,
        "runtime_identity_schema_version": runtime["schema_version"],
        "pid": int(process["pid"]),
        "pid_start_ticks": int(process["pid_start_ticks"]),
        "process_identity_sha256": process[
            "canonical_process_identity_sha256"
        ],
        "config_path": runtime["config_path"],
        "config_sha256": runtime["config_sha256"],
        "artifact_sha256": process["artifact_sha256"],
        "buy_e3_enabled": runtime["f05_buy_e3_enabled"],
        "owner_override_effective": runtime[
            "f05_buy_e3_owner_override_effective"
        ],
        "startup_attestation": attestation,
        "startup_attestation_sha256": startup_attestation_sha256,
    }
    binding["canonical_runtime_identity_binding_sha256"] = (
        gate_v2.document_sha256(
            binding, "canonical_runtime_identity_binding_sha256"
        )
    )
    return binding


def _validate_runtime_identity_binding(
    raw: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    process: Mapping[str, Any],
    process_phase: str,
) -> dict[str, Any]:
    if set(raw) != _RUNTIME_IDENTITY_BINDING_FIELDS:
        raise BuyE3TransactionalDeployError("runtime identity binding fields drifted")
    binding = dict(raw)
    runtime_identity = process.get("runtime_identity")
    if not isinstance(runtime_identity, Mapping):
        raise BuyE3TransactionalDeployError("runtime identity process binding is malformed")
    if (
        runtime_identity.get("file_sha256")
        != process.get("runtime_identity_file_sha256")
    ):
        raise BuyE3TransactionalDeployError(
            "runtime identity process file hashes disagree"
        )
    expected = _expected_process_binding(plan, process_phase)
    exact = {
        "schema_version": RUNTIME_IDENTITY_BINDING_SCHEMA,
        "authority": "runtime_written_startup_attestation",
        "evidence_classification": (
            "runtime_identity_file_unsigned_structural_evidence"
        ),
        "cryptographic_signature_present": False,
        "runtime_identity_path": plan["runtime_attestation_contract"]["remote_path"],
        "runtime_identity_file_sha256": process[
            "runtime_identity_file_sha256"
        ],
        "runtime_identity_schema_version": RUNTIME_IDENTITY_SCHEMA,
        "pid": int(process["pid"]),
        "pid_start_ticks": int(process["pid_start_ticks"]),
        "process_identity_sha256": process[
            "canonical_process_identity_sha256"
        ],
        "config_path": expected["config_path"],
        "config_sha256": expected["config_sha256"],
        "artifact_sha256": expected["artifact_sha256"],
        "buy_e3_enabled": bool(expected["enabled"]),
        "owner_override_effective": bool(expected["enabled"]),
    }
    if any(binding.get(field) != value for field, value in exact.items()):
        raise BuyE3TransactionalDeployError("runtime identity binding drifted")
    attestation = _validate_startup_attestation(
        binding.get("startup_attestation"),
        expected_execution_commit=expected["execution_commit"],
        expected_execution_tree=expected["execution_tree"],
        expected_runtime_sources=plan["runtime_sources"],
        expected_python_executable=expected["python_executable"],
        expected_python_binary_resolved=str(process["python_binary_resolved"]),
    )
    if (
        binding.get("startup_attestation_sha256")
        != gate_v2.canonical_sha256(attestation)
        or binding.get("startup_attestation_sha256")
        != process.get("startup_attestation_sha256")
        or binding.get("canonical_runtime_identity_binding_sha256")
        != gate_v2.document_sha256(
            binding, "canonical_runtime_identity_binding_sha256"
        )
    ):
        raise BuyE3TransactionalDeployError("runtime identity binding hash drifted")
    return binding


def _reserve_receipt_output(output_path: Path) -> dict[str, Any]:
    target = output_path.expanduser().absolute()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.parent.is_symlink() or not target.parent.is_dir():
        raise BuyE3TransactionalDeployError("receipt parent is not a stable directory")
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(target.parent, directory_flags)
    reservation_name = f".{target.name}.reserve"
    try:
        try:
            os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise BuyE3TransactionalDeployError("immutable phase receipt already exists")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(reservation_name, flags, 0o600, dir_fd=directory_fd)
        os.close(descriptor)
        parent_stat = os.fstat(directory_fd)
        return {
            "target": target,
            "directory_fd": directory_fd,
            "directory_device": parent_stat.st_dev,
            "directory_inode": parent_stat.st_ino,
            "reservation_name": reservation_name,
            "committed": False,
        }
    except Exception:
        os.close(directory_fd)
        raise


def _assert_receipt_parent_stable(reservation: Mapping[str, Any]) -> None:
    parent_stat = os.stat(Path(reservation["target"]).parent, follow_symlinks=False)
    if (
        parent_stat.st_dev != reservation["directory_device"]
        or parent_stat.st_ino != reservation["directory_inode"]
        or not stat.S_ISDIR(parent_stat.st_mode)
    ):
        raise BuyE3TransactionalDeployError("receipt parent identity drifted")


def _commit_reserved_receipt(
    reservation: dict[str, Any],
    payload: Mapping[str, Any],
    *,
    validator: Callable[[Path], Any],
) -> None:
    _assert_receipt_parent_stable(reservation)
    directory_fd = int(reservation["directory_fd"])
    target = Path(reservation["target"])
    temporary_name = f".{target.name}.tmp.{os.getpid()}.{os.urandom(8).hex()}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary_name, flags, 0o600, dir_fd=directory_fd)
    temporary_path = target.parent / temporary_name
    try:
        try:
            encoded = (
                json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True)
                + "\n"
            ).encode("ascii")
            offset = 0
            while offset < len(encoded):
                written = os.write(descriptor, encoded[offset:])
                if written <= 0:
                    raise OSError("receipt write made no progress")
                offset += written
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        validator(temporary_path)
        _assert_receipt_parent_stable(reservation)
        os.link(
            temporary_name,
            target.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
            follow_symlinks=False,
        )
        os.fsync(directory_fd)
        validator(target)
        reservation["committed"] = True
    finally:
        try:
            os.unlink(temporary_name, dir_fd=directory_fd)
        except FileNotFoundError:
            pass


def _release_receipt_reservation(reservation: Mapping[str, Any]) -> None:
    directory_fd = int(reservation["directory_fd"])
    try:
        try:
            os.unlink(str(reservation["reservation_name"]), dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _command_result(
    row: Mapping[str, Any], completed: subprocess.CompletedProcess[str] | None
) -> dict[str, Any]:
    if completed is None:
        return {
            "label": row["label"],
            "command_sha256": row["command_sha256"],
            "returncode": None,
            "stdout_sha256": None,
            "stderr_sha256": None,
        }
    return {
        "label": row["label"],
        "command_sha256": row["command_sha256"],
        "returncode": int(completed.returncode),
        "stdout_sha256": _sha256_text(completed.stdout or ""),
        "stderr_sha256": _sha256_text(completed.stderr or ""),
    }


def _build_phase_receipt(
    *,
    plan: Mapping[str, Any],
    phase: str,
    status: str,
    results: Sequence[Mapping[str, Any]],
    mutation_started: bool,
    disabled_phase_receipt_binding: Mapping[str, Any] | None,
    pre_stop_disabled_process_identity: Mapping[str, Any] | None,
    pre_stop_disabled_startup_attestation: Mapping[str, Any] | None,
    actual_startup_attestation: Mapping[str, Any] | None,
    actual_process_identity: Mapping[str, Any] | None,
    stop_failure_probe_result: Mapping[str, Any] | None,
    rollback_attempted: bool,
    rollback_status: str,
    rollback_failure_class: str | None,
    rollback_process_identity: Mapping[str, Any] | None,
    failure_class: str | None,
) -> dict[str, Any]:
    rollback_rows = _automatic_rollback_rows(plan) if rollback_attempted else []
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "plan_sha256": plan["canonical_plan_sha256"],
        "phase": phase,
        "status": status,
        "remote_mutation_authorized": True,
        "phase_authorization_token_sha256": plan["phase_token_sha256"][phase],
        "transaction_contract_sha256": gate_v2.canonical_sha256(
            plan["transaction_contract"]
        ),
        "expected_commands": _expected_commands(plan["phases"][phase]),
        "expected_automatic_rollback_commands": _expected_commands(rollback_rows),
        "results": [dict(result) for result in results],
        "mutation_started": bool(mutation_started),
        "disabled_phase_receipt_binding": (
            dict(disabled_phase_receipt_binding)
            if disabled_phase_receipt_binding is not None
            else None
        ),
        "pre_stop_disabled_process_identity": (
            dict(pre_stop_disabled_process_identity)
            if pre_stop_disabled_process_identity is not None
            else None
        ),
        "pre_stop_disabled_startup_attestation": (
            dict(pre_stop_disabled_startup_attestation)
            if pre_stop_disabled_startup_attestation is not None
            else None
        ),
        "actual_startup_attestation": (
            dict(actual_startup_attestation)
            if actual_startup_attestation is not None
            else None
        ),
        "actual_process_identity": (
            dict(actual_process_identity) if actual_process_identity is not None else None
        ),
        "stop_failure_probe_result": (
            dict(stop_failure_probe_result)
            if stop_failure_probe_result is not None
            else None
        ),
        "rollback_attempted": bool(rollback_attempted),
        "rollback_status": rollback_status,
        "rollback_failure_class": rollback_failure_class,
        "rollback_process_identity": (
            dict(rollback_process_identity) if rollback_process_identity is not None else None
        ),
        "failure_class": failure_class,
        "permissions": dict(RECEIPT_PERMISSIONS),
        "evidence_boundary": dict(RECEIPT_EVIDENCE_BOUNDARY),
        "evidence_authority": dict(RECEIPT_AUTHORITY),
    }
    receipt["canonical_receipt_sha256"] = gate_v2.document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    return receipt


def _validate_result_shape(result: Mapping[str, Any]) -> None:
    if set(result) - _RESULT_FIELDS:
        raise BuyE3TransactionalDeployError("phase result embeds forbidden fields")
    label = result.get("label")
    if not isinstance(label, str) or not label:
        raise BuyE3TransactionalDeployError("phase result label is malformed")
    _require_sha256(result.get("command_sha256"), "phase result command hash")
    returncode = result.get("returncode")
    if returncode is None:
        if result.get("stdout_sha256") is not None or result.get("stderr_sha256") is not None:
            raise BuyE3TransactionalDeployError("runner failure output binding is malformed")
        if set(result) != {
            "label",
            "command_sha256",
            "returncode",
            "stdout_sha256",
            "stderr_sha256",
        }:
            raise BuyE3TransactionalDeployError("runner failure carries fabricated identity")
        return
    else:
        if not isinstance(returncode, int):
            raise BuyE3TransactionalDeployError("phase return code is malformed")
        _require_sha256(result.get("stdout_sha256"), "phase stdout hash")
        _require_sha256(result.get("stderr_sha256"), "phase stderr hash")
    identity_fields = set(result) - {
        "label",
        "command_sha256",
        "returncode",
        "stdout_sha256",
        "stderr_sha256",
    }
    bare_label = label.removeprefix("automatic-rollback:")
    if bare_label == "capture-old-pid":
        if identity_fields and (
            identity_fields != {"observed_pid"} or int(result["observed_pid"]) <= 0
        ):
            raise BuyE3TransactionalDeployError("old PID result binding is malformed")
    elif (
        "process-probe" in bare_label
        or bare_label == "reprobe-disabled-process-before-stop"
    ):
        if identity_fields and identity_fields != {
            "observed_pid",
            "process_identity_sha256",
        }:
            raise BuyE3TransactionalDeployError("process result binding is malformed")
        if identity_fields and int(result["observed_pid"]) <= 0:
            raise BuyE3TransactionalDeployError("process result PID is malformed")
        if identity_fields:
            _require_sha256(result["process_identity_sha256"], "process identity hash")
    elif bare_label in {
        "read-disabled-runtime-identity",
        "read-pre-stop-disabled-runtime-identity",
        "read-active-runtime-identity",
    }:
        if identity_fields and identity_fields != {
            "runtime_identity_file_sha256",
            "startup_attestation_sha256",
        }:
            raise BuyE3TransactionalDeployError(
                "runtime identity result binding is malformed"
            )
        if identity_fields:
            _require_sha256(
                result["runtime_identity_file_sha256"],
                "runtime identity file hash",
            )
            _require_sha256(
                result["startup_attestation_sha256"],
                "runtime startup attestation hash",
            )
    elif identity_fields:
        raise BuyE3TransactionalDeployError("non-probe result carries process identity")


def validate_phase_receipt(
    receipt_path: Path,
    *,
    plan: Mapping[str, Any],
    expected_phase: str | None = None,
) -> dict[str, Any]:
    """Validate one immutable phase receipt against its complete frozen plan."""

    validate_plan(plan)
    _revalidate_plan_inputs(plan)
    candidate = receipt_path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3TransactionalDeployError("phase receipt is not an immutable regular file")
    target = candidate.resolve(strict=True)
    if stat.S_IMODE(target.stat().st_mode) != 0o600:
        raise BuyE3TransactionalDeployError("phase receipt permission drifted from 0600")
    receipt = gate_v2.read_json(target)
    if set(receipt) != _PHASE_RECEIPT_FIELDS:
        raise BuyE3TransactionalDeployError("phase receipt fields drifted")
    phase = str(receipt.get("phase", ""))
    if phase not in MUTATING_PHASES or (expected_phase is not None and phase != expected_phase):
        raise BuyE3TransactionalDeployError("phase receipt phase drifted")
    if (
        receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("plan_sha256") != plan["canonical_plan_sha256"]
        or receipt.get("canonical_receipt_sha256")
        != gate_v2.document_sha256(receipt, "canonical_receipt_sha256")
        or receipt.get("remote_mutation_authorized") is not True
        or receipt.get("phase_authorization_token_sha256")
        != plan["phase_token_sha256"][phase]
        or receipt.get("transaction_contract_sha256")
        != gate_v2.canonical_sha256(plan["transaction_contract"])
        or receipt.get("permissions") != RECEIPT_PERMISSIONS
        or receipt.get("evidence_boundary") != RECEIPT_EVIDENCE_BOUNDARY
        or receipt.get("evidence_authority") != RECEIPT_AUTHORITY
    ):
        raise BuyE3TransactionalDeployError("phase receipt identity drifted")
    expected = _expected_commands(plan["phases"][phase])
    if receipt.get("expected_commands") != expected:
        raise BuyE3TransactionalDeployError("phase receipt expected command binding drifted")
    raw_results = receipt.get("results")
    if not isinstance(raw_results, list) or not raw_results:
        raise BuyE3TransactionalDeployError("phase receipt lacks command results")
    results: list[dict[str, Any]] = []
    for raw in raw_results:
        if not isinstance(raw, Mapping):
            raise BuyE3TransactionalDeployError("phase result is malformed")
        result = dict(raw)
        _validate_result_shape(result)
        results.append(result)
    first_rollback = next(
        (index for index, result in enumerate(results) if result["label"].startswith("automatic-rollback:")),
        len(results),
    )
    main_results = results[:first_rollback]
    rollback_results = results[first_rollback:]
    if any(result["label"].startswith("automatic-rollback:") for result in main_results) or any(
        not result["label"].startswith("automatic-rollback:") for result in rollback_results
    ):
        raise BuyE3TransactionalDeployError("phase result ordering drifted")
    if len(main_results) > len(expected):
        raise BuyE3TransactionalDeployError("phase result count exceeds frozen commands")
    for index, result in enumerate(main_results):
        if {
            "label": result["label"],
            "command_sha256": result["command_sha256"],
        } != expected[index]:
            raise BuyE3TransactionalDeployError("phase command order/hash drifted")
    attempted_rows = plan["phases"][phase][: len(main_results)]
    mutation_started = any(bool(row["mutates_remote"]) for row in attempted_rows)
    if receipt.get("mutation_started") is not mutation_started:
        raise BuyE3TransactionalDeployError("phase mutation-start binding drifted")
    status = receipt.get("status")
    failure_class = receipt.get("failure_class")
    if status == PHASE_COMPLETE:
        if (
            len(main_results) != len(expected)
            or rollback_results
            or any(result["returncode"] != 0 for result in main_results)
            or failure_class is not None
        ):
            raise BuyE3TransactionalDeployError("completed phase did not run every command")
    elif status == PHASE_FAILED_CLOSED:
        if failure_class not in FAILURE_CLASSES:
            raise BuyE3TransactionalDeployError("failed phase lacks a safe failure class")
        if any(result["returncode"] not in {0, None} for result in main_results[:-1]):
            raise BuyE3TransactionalDeployError("phase continued after a failed command")
        last_returncode = main_results[-1]["returncode"]
        if failure_class == "command_returncode_nonzero" and (
            not isinstance(last_returncode, int) or last_returncode == 0
        ):
            raise BuyE3TransactionalDeployError("failed command return code was not preserved")
        if failure_class == "command_runner_exception" and last_returncode is not None:
            raise BuyE3TransactionalDeployError("runner failure return code was fabricated")
    else:
        raise BuyE3TransactionalDeployError("phase receipt status drifted")
    old_pid = next(
        (
            int(result["observed_pid"])
            for result in main_results
            if result["label"] == "capture-old-pid" and "observed_pid" in result
        ),
        None,
    )
    disabled_binding = receipt.get("disabled_phase_receipt_binding")
    pre_stop_disabled_process = receipt.get("pre_stop_disabled_process_identity")
    pre_stop_startup = receipt.get("pre_stop_disabled_startup_attestation")
    actual_startup = receipt.get("actual_startup_attestation")
    prior_disabled_process: dict[str, Any] | None = None
    validated_pre_stop: dict[str, Any] | None = None
    if phase == "activate":
        if (
            not isinstance(disabled_binding, Mapping)
            or set(disabled_binding) != _DISABLED_PHASE_BINDING_FIELDS
        ):
            raise BuyE3TransactionalDeployError(
                "activation disabled phase receipt binding drifted"
            )
        rebound, prior_disabled_process = _load_disabled_phase_receipt_binding(
            Path(str(disabled_binding["path"])), plan=plan
        )
        if dict(disabled_binding) != rebound:
            raise BuyE3TransactionalDeployError(
                "activation disabled phase receipt bytes drifted"
            )
        if old_pid is not None and old_pid != int(prior_disabled_process["pid"]):
            raise BuyE3TransactionalDeployError(
                "activation old PID differs from disabled receipt"
            )
        if pre_stop_disabled_process is not None:
            if not isinstance(pre_stop_disabled_process, Mapping):
                raise BuyE3TransactionalDeployError(
                    "pre-stop disabled process binding is malformed"
                )
            validated_pre_stop = _validate_actual_process_identity(
                pre_stop_disabled_process,
                plan=plan,
                phase="disabled-deploy",
                old_pid=None,
                require_fresh=False,
            )
            try:
                _require_same_disabled_process(prior_disabled_process, validated_pre_stop)
            except BuyE3TransactionalDeployError:
                if not (
                    status == PHASE_FAILED_CLOSED
                    and failure_class == "disabled_process_handoff_mismatch"
                ):
                    raise
            else:
                if (
                    status == PHASE_FAILED_CLOSED
                    and failure_class == "disabled_process_handoff_mismatch"
                ):
                    raise BuyE3TransactionalDeployError(
                        "failed handoff receipt does not preserve the mismatch"
                    )
            pre_stop_hash = validated_pre_stop["canonical_process_identity_sha256"]
            if sum(
                result["label"] == "reprobe-disabled-process-before-stop"
                and result.get("process_identity_sha256") == pre_stop_hash
                for result in main_results
            ) != 1:
                raise BuyE3TransactionalDeployError(
                    "pre-stop disabled process probe is not rebound"
                )
        if pre_stop_startup is not None:
            if (
                not isinstance(pre_stop_startup, Mapping)
                or validated_pre_stop is None
            ):
                raise BuyE3TransactionalDeployError(
                    "pre-stop runtime startup binding is malformed"
                )
            validated_pre_stop_startup = _validate_runtime_identity_binding(
                pre_stop_startup,
                plan=plan,
                process=validated_pre_stop,
                process_phase="disabled-deploy",
            )
            if (
                validated_pre_stop_startup["runtime_identity_file_sha256"]
                != disabled_binding["runtime_identity_file_sha256"]
                or validated_pre_stop_startup["startup_attestation_sha256"]
                != disabled_binding["startup_attestation_sha256"]
            ):
                raise BuyE3TransactionalDeployError(
                    "pre-stop runtime identity differs from disabled receipt"
                )
            if sum(
                result["label"] == "read-pre-stop-disabled-runtime-identity"
                and result.get("stdout_sha256")
                == validated_pre_stop_startup["runtime_identity_file_sha256"]
                and result.get("runtime_identity_file_sha256")
                == validated_pre_stop_startup["runtime_identity_file_sha256"]
                and result.get("startup_attestation_sha256")
                == validated_pre_stop_startup["startup_attestation_sha256"]
                for result in main_results
            ) != 1:
                raise BuyE3TransactionalDeployError(
                    "pre-stop runtime identity result is not rebound"
                )
        if status == PHASE_COMPLETE and (
            old_pid is None
            or pre_stop_disabled_process is None
            or pre_stop_startup is None
        ):
            raise BuyE3TransactionalDeployError(
                "activation completion lacks disabled handoff evidence"
            )
    elif any(
        value is not None
        for value in (disabled_binding, pre_stop_disabled_process, pre_stop_startup)
    ):
        raise BuyE3TransactionalDeployError(
            "non-activation receipt carries activation handoff evidence"
        )
    process = receipt.get("actual_process_identity")
    if process is not None:
        if not isinstance(process, Mapping):
            raise BuyE3TransactionalDeployError("actual process receipt binding is malformed")
        validated_process = _validate_actual_process_identity(
            process, plan=plan, phase=phase, old_pid=old_pid
        )
        process_hash = validated_process["canonical_process_identity_sha256"]
        matching = [
            result
            for result in main_results
            if result.get("process_identity_sha256") == process_hash
        ]
        if len(matching) != 1:
            raise BuyE3TransactionalDeployError("process probe result is not rebound")
    elif status == PHASE_COMPLETE:
        raise BuyE3TransactionalDeployError("completed phase lacks actual process identity")
    if actual_startup is not None:
        if not isinstance(actual_startup, Mapping) or not isinstance(process, Mapping):
            raise BuyE3TransactionalDeployError(
                "actual runtime startup binding is malformed"
            )
        if phase not in {"disabled-deploy", "activate"}:
            raise BuyE3TransactionalDeployError(
                "rollback receipt carries unsupported runtime startup evidence"
            )
        validated_actual_startup = _validate_runtime_identity_binding(
            actual_startup,
            plan=plan,
            process=process,
            process_phase=phase,
        )
        expected_label = (
            "read-disabled-runtime-identity"
            if phase == "disabled-deploy"
            else "read-active-runtime-identity"
        )
        if sum(
            result["label"] == expected_label
            and result.get("stdout_sha256")
            == validated_actual_startup["runtime_identity_file_sha256"]
            and result.get("runtime_identity_file_sha256")
            == validated_actual_startup["runtime_identity_file_sha256"]
            and result.get("startup_attestation_sha256")
            == validated_actual_startup["startup_attestation_sha256"]
            for result in main_results
        ) != 1:
            raise BuyE3TransactionalDeployError(
                "actual runtime identity result is not rebound"
            )
    elif status == PHASE_COMPLETE and phase in {"disabled-deploy", "activate"}:
        raise BuyE3TransactionalDeployError(
            "completed deployment phase lacks runtime-written startup evidence"
        )
    failed_stop = next(
        (
            result
            for result in main_results
            if result["label"] == "stop-live" and result["returncode"] != 0
        ),
        None,
    )
    stop_failure_probe = receipt.get("stop_failure_probe_result")
    if failed_stop is None:
        if stop_failure_probe is not None:
            raise BuyE3TransactionalDeployError("unexpected stop failure probe evidence")
    else:
        if not isinstance(stop_failure_probe, Mapping):
            raise BuyE3TransactionalDeployError("stop failure lacks post-stop probe")
        _validate_result_shape(stop_failure_probe)
        expected_probe_row = next(
            row for row in plan["phases"][phase] if row["label"] == "confirm-quiescent"
        )
        if (
            stop_failure_probe.get("label") != "stop-failure-probe:confirm-quiescent"
            or stop_failure_probe.get("command_sha256")
            != expected_probe_row["command_sha256"]
        ):
            raise BuyE3TransactionalDeployError("stop failure probe command drifted")
    expected_rollback_attempt = (
        status == PHASE_FAILED_CLOSED
        and mutation_started
        and phase not in {"rollback-primary", "rollback-deep"}
    )
    if receipt.get("rollback_attempted") is not expected_rollback_attempt:
        raise BuyE3TransactionalDeployError("automatic rollback behavior drifted")
    expected_rollback = (
        _expected_commands(_automatic_rollback_rows(plan)) if expected_rollback_attempt else []
    )
    if receipt.get("expected_automatic_rollback_commands") != expected_rollback:
        raise BuyE3TransactionalDeployError("automatic rollback command binding drifted")
    rollback_status = receipt.get("rollback_status")
    rollback_failure_class = receipt.get("rollback_failure_class")
    rollback_process = receipt.get("rollback_process_identity")
    if not expected_rollback_attempt:
        if (
            rollback_results
            or rollback_status != "not_required"
            or rollback_failure_class is not None
            or rollback_process is not None
        ):
            raise BuyE3TransactionalDeployError("unexpected automatic rollback evidence")
    else:
        if not rollback_results or len(rollback_results) > len(expected_rollback):
            raise BuyE3TransactionalDeployError("automatic rollback results are incomplete")
        for index, result in enumerate(rollback_results):
            expected_result = expected_rollback[index]
            if {
                "label": result["label"].removeprefix("automatic-rollback:"),
                "command_sha256": result["command_sha256"],
            } != expected_result:
                raise BuyE3TransactionalDeployError("automatic rollback order/hash drifted")
        if rollback_status == "rollback_complete":
            if (
                len(rollback_results) != len(expected_rollback)
                or any(result["returncode"] != 0 for result in rollback_results)
                or rollback_failure_class is not None
                or not isinstance(rollback_process, Mapping)
            ):
                raise BuyE3TransactionalDeployError("automatic rollback completion is unproven")
            validated_rollback = _validate_actual_process_identity(
                rollback_process,
                plan=plan,
                phase="rollback-primary",
                old_pid=old_pid,
            )
            rollback_hash = validated_rollback["canonical_process_identity_sha256"]
            if sum(
                result.get("process_identity_sha256") == rollback_hash
                for result in rollback_results
            ) != 1:
                raise BuyE3TransactionalDeployError("rollback process probe is not rebound")
        elif rollback_status == "rollback_failed_closed":
            if rollback_failure_class not in FAILURE_CLASSES:
                raise BuyE3TransactionalDeployError("rollback failure class is unsafe")
            if rollback_process is not None:
                if not isinstance(rollback_process, Mapping):
                    raise BuyE3TransactionalDeployError(
                        "failed rollback process binding is malformed"
                    )
                _validate_actual_process_identity(
                    rollback_process,
                    plan=plan,
                    phase="rollback-primary",
                    old_pid=old_pid,
                )
        else:
            raise BuyE3TransactionalDeployError("automatic rollback status drifted")
    return receipt


def _run_stop_failure_probe(
    *,
    plan: Mapping[str, Any],
    phase: str,
    runner: CommandRunner,
) -> dict[str, Any]:
    row = next(
        row for row in plan["phases"][phase] if row["label"] == "confirm-quiescent"
    )
    probe_row = dict(row)
    probe_row["label"] = "stop-failure-probe:confirm-quiescent"
    try:
        completed = runner(tuple(str(value) for value in row["argv"]))
    except Exception:
        return _command_result(probe_row, None)
    return _command_result(probe_row, completed)


def _run_automatic_rollback(
    *,
    plan: Mapping[str, Any],
    runner: CommandRunner,
    results: list[dict[str, Any]],
    old_pid: int | None,
) -> tuple[str, str | None, dict[str, Any] | None]:
    rollback_failure_class: str | None = None
    rollback_process_identity: dict[str, Any] | None = None
    completed_rows = 0
    rows = _automatic_rollback_rows(plan)
    for row in rows:
        automatic_row = dict(row)
        automatic_row["label"] = f"automatic-rollback:{row['label']}"
        try:
            completed = runner(tuple(str(value) for value in row["argv"]))
        except Exception:
            results.append(_command_result(automatic_row, None))
            rollback_failure_class = "command_runner_exception"
            break
        result = _command_result(automatic_row, completed)
        results.append(result)
        completed_rows += 1
        if completed.returncode != 0:
            rollback_failure_class = "command_returncode_nonzero"
            break
        if "process-probe" in row["label"]:
            try:
                rollback_process_identity = _parse_process_probe(
                    completed.stdout or "",
                    plan=plan,
                    phase="rollback-primary",
                    old_pid=old_pid,
                )
            except BuyE3TransactionalDeployError as exc:
                rollback_failure_class = _classify_process_error(exc)
                break
            result["observed_pid"] = rollback_process_identity["pid"]
            result["process_identity_sha256"] = rollback_process_identity[
                "canonical_process_identity_sha256"
            ]
    if (
        rollback_failure_class is None
        and completed_rows == len(rows)
        and rollback_process_identity is not None
    ):
        return "rollback_complete", None, rollback_process_identity
    return "rollback_failed_closed", rollback_failure_class, rollback_process_identity


def execute_phase(
    *,
    plan: Mapping[str, Any],
    phase: str,
    token: str,
    authorize_remote_mutation: bool,
    runner: CommandRunner = _default_runner,
    output_path: Path | None = None,
    disabled_phase_receipt_path: Path | None = None,
) -> dict[str, Any]:
    """Execute one explicit transaction phase; default callers cannot mutate."""

    validate_plan(plan)
    _revalidate_plan_inputs(plan)
    if phase not in MUTATING_PHASES:
        raise BuyE3TransactionalDeployError("unknown deployment phase")
    if not authorize_remote_mutation:
        raise PermissionError("remote mutation requires --authorize-remote-mutation")
    expected_token = str(plan["phase_token_sha256"][phase])
    if not hmac.compare_digest(phase_authorization_token_sha256(token), expected_token):
        raise PermissionError("phase token does not match the frozen plan")
    if phase == "activate" and not plan.get("activation_gate_receipt_sha256"):
        raise PermissionError("activation requires a separately bound amended gate receipt")
    disabled_phase_receipt_binding: dict[str, Any] | None = None
    prior_disabled_process: dict[str, Any] | None = None
    if phase == "activate":
        if disabled_phase_receipt_path is None:
            raise PermissionError(
                "activation requires the same-plan successful disabled phase receipt"
            )
        disabled_phase_receipt_binding, prior_disabled_process = (
            _load_disabled_phase_receipt_binding(
                disabled_phase_receipt_path, plan=plan
            )
        )
    elif disabled_phase_receipt_path is not None:
        raise BuyE3TransactionalDeployError(
            "disabled phase receipt is accepted only for activation"
        )
    if output_path is None:
        raise BuyE3TransactionalDeployError("remote phase requires an immutable receipt output")
    reservation = _reserve_receipt_output(output_path)
    rows = plan["phases"][phase]
    results: list[dict[str, Any]] = []
    mutation_started = False
    rollback_attempted = False
    rollback_status = "not_required"
    rollback_failure_class: str | None = None
    pre_stop_disabled_process_identity: dict[str, Any] | None = None
    pre_stop_disabled_startup_attestation: dict[str, Any] | None = None
    actual_startup_attestation: dict[str, Any] | None = None
    actual_process_identity: dict[str, Any] | None = None
    stop_failure_probe_result: dict[str, Any] | None = None
    rollback_process_identity: dict[str, Any] | None = None
    phase_complete = False
    failure_class: str | None = None
    old_pid: int | None = None
    phase_error: BaseException | None = None
    try:
        for row in rows:
            if row["mutates_remote"]:
                mutation_started = True
            try:
                completed = runner(tuple(str(value) for value in row["argv"]))
            except Exception:
                results.append(_command_result(row, None))
                failure_class = "command_runner_exception"
                if row["label"] == "stop-live":
                    stop_failure_probe_result = _run_stop_failure_probe(
                        plan=plan, phase=phase, runner=runner
                    )
                raise
            result = _command_result(row, completed)
            results.append(result)
            if completed.returncode != 0:
                failure_class = "command_returncode_nonzero"
                if row["label"] == "stop-live":
                    stop_failure_probe_result = _run_stop_failure_probe(
                        plan=plan, phase=phase, runner=runner
                    )
                raise BuyE3TransactionalDeployError(
                    f"remote phase failed closed at {row['label']}"
                )
            if completed.returncode == 0 and row["label"] == "capture-old-pid":
                try:
                    old_pid = int((completed.stdout or "").strip())
                except ValueError as exc:
                    failure_class = "old_pid_probe_invalid"
                    raise BuyE3TransactionalDeployError("old PID probe is malformed") from exc
                if old_pid <= 0:
                    failure_class = "old_pid_probe_invalid"
                    raise BuyE3TransactionalDeployError("old PID probe is invalid")
                result["observed_pid"] = old_pid
                if (
                    phase == "activate"
                    and prior_disabled_process is not None
                    and old_pid != int(prior_disabled_process["pid"])
                ):
                    failure_class = "disabled_process_handoff_mismatch"
                    raise BuyE3TransactionalDeployError(
                        "captured process differs from disabled phase receipt"
                    )
            if row["label"] == "reprobe-disabled-process-before-stop":
                try:
                    pre_stop_disabled_process_identity = _parse_process_probe(
                        completed.stdout or "",
                        plan=plan,
                        phase="disabled-deploy",
                        old_pid=None,
                        require_fresh=False,
                    )
                    result["observed_pid"] = pre_stop_disabled_process_identity["pid"]
                    result["process_identity_sha256"] = pre_stop_disabled_process_identity[
                        "canonical_process_identity_sha256"
                    ]
                    if prior_disabled_process is None:
                        raise BuyE3TransactionalDeployError(
                            "activation lacks prior disabled process"
                        )
                    _require_same_disabled_process(
                        prior_disabled_process, pre_stop_disabled_process_identity
                    )
                except BuyE3TransactionalDeployError:
                    failure_class = "disabled_process_handoff_mismatch"
                    raise
            elif completed.returncode == 0 and "process-probe" in row["label"]:
                try:
                    actual_process_identity = _parse_process_probe(
                        completed.stdout or "", plan=plan, phase=phase, old_pid=old_pid
                    )
                except BuyE3TransactionalDeployError as exc:
                    failure_class = _classify_process_error(exc)
                    raise
                result["observed_pid"] = actual_process_identity["pid"]
                result["process_identity_sha256"] = actual_process_identity[
                    "canonical_process_identity_sha256"
                ]
            if row["label"] in {
                "read-disabled-runtime-identity",
                "read-pre-stop-disabled-runtime-identity",
                "read-active-runtime-identity",
            }:
                try:
                    if row["label"] == "read-pre-stop-disabled-runtime-identity":
                        identity_process = pre_stop_disabled_process_identity
                        identity_phase = "disabled-deploy"
                    else:
                        identity_process = actual_process_identity
                        identity_phase = phase
                    if identity_process is None:
                        raise BuyE3TransactionalDeployError(
                            "runtime identity read lacks its process probe"
                        )
                    startup_binding = _validate_runtime_identity_stdout(
                        completed.stdout or "",
                        plan=plan,
                        process=identity_process,
                        process_phase=identity_phase,
                    )
                except BuyE3TransactionalDeployError:
                    failure_class = "runtime_identity_invalid"
                    raise
                result["runtime_identity_file_sha256"] = startup_binding[
                    "runtime_identity_file_sha256"
                ]
                result["startup_attestation_sha256"] = startup_binding[
                    "startup_attestation_sha256"
                ]
                if row["label"] == "read-pre-stop-disabled-runtime-identity":
                    pre_stop_disabled_startup_attestation = startup_binding
                    if disabled_phase_receipt_binding is None or (
                        startup_binding["runtime_identity_file_sha256"]
                        != disabled_phase_receipt_binding[
                            "runtime_identity_file_sha256"
                        ]
                        or startup_binding["startup_attestation_sha256"]
                        != disabled_phase_receipt_binding[
                            "startup_attestation_sha256"
                        ]
                    ):
                        failure_class = "disabled_process_handoff_mismatch"
                        raise BuyE3TransactionalDeployError(
                            "runtime identity changed since disabled phase"
                        )
                else:
                    actual_startup_attestation = startup_binding
        if actual_process_identity is None:
            failure_class = "phase_contract_validation_failed"
            raise BuyE3TransactionalDeployError("phase did not return actual process identity")
        if phase in {"disabled-deploy", "activate"} and actual_startup_attestation is None:
            failure_class = "phase_contract_validation_failed"
            raise BuyE3TransactionalDeployError(
                "phase did not validate runtime-written startup evidence"
            )
        if phase == "activate" and pre_stop_disabled_startup_attestation is None:
            failure_class = "phase_contract_validation_failed"
            raise BuyE3TransactionalDeployError(
                "activation did not preserve disabled startup evidence"
            )
        if len(results) != len(rows) or any(result["returncode"] != 0 for result in results):
            failure_class = "phase_contract_validation_failed"
            raise BuyE3TransactionalDeployError("phase command completion is incomplete")
        _revalidate_plan_inputs(plan)
        phase_complete = True
    except BaseException as exc:
        phase_error = exc
        if failure_class is None:
            failure_class = "phase_contract_validation_failed"
        if mutation_started and phase not in {"rollback-primary", "rollback-deep"}:
            rollback_attempted = True
            (
                rollback_status,
                rollback_failure_class,
                rollback_process_identity,
            ) = _run_automatic_rollback(
                plan=plan,
                runner=runner,
                results=results,
                old_pid=old_pid,
            )
    receipt = _build_phase_receipt(
        plan=plan,
        phase=phase,
        status=PHASE_COMPLETE if phase_complete else PHASE_FAILED_CLOSED,
        results=results,
        mutation_started=mutation_started,
        disabled_phase_receipt_binding=disabled_phase_receipt_binding,
        pre_stop_disabled_process_identity=pre_stop_disabled_process_identity,
        pre_stop_disabled_startup_attestation=(
            pre_stop_disabled_startup_attestation
        ),
        actual_startup_attestation=actual_startup_attestation,
        actual_process_identity=actual_process_identity,
        stop_failure_probe_result=stop_failure_probe_result,
        rollback_attempted=rollback_attempted,
        rollback_status=rollback_status,
        rollback_failure_class=rollback_failure_class,
        rollback_process_identity=rollback_process_identity,
        failure_class=None if phase_complete else failure_class,
    )
    try:
        _commit_reserved_receipt(
            reservation,
            receipt,
            validator=lambda path: validate_phase_receipt(
                path, plan=plan, expected_phase=phase
            ),
        )
    except Exception as exc:
        receipt_failure_class = (
            "receipt_validation_failed"
            if isinstance(exc, BuyE3TransactionalDeployError)
            else "receipt_write_failed"
        )
        if (
            mutation_started
            and not rollback_attempted
            and phase not in {"rollback-primary", "rollback-deep"}
        ):
            rollback_attempted = True
            _run_automatic_rollback(
                plan=plan,
                runner=runner,
                results=results,
                old_pid=old_pid,
            )
        raise BuyE3TransactionalDeployError(
            f"phase receipt failed closed: {receipt_failure_class}"
        ) from exc
    finally:
        _release_receipt_reservation(reservation)
    if phase_error is not None:
        raise phase_error
    return receipt


def _build_spec_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--specification", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, default=REPO_ROOT)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    _build_spec_parser(plan)
    plan.add_argument("--output", type=Path, required=True)
    preflight = subparsers.add_parser("isolated-preflight")
    preflight.add_argument("--repository-root", type=Path, required=True)
    preflight.add_argument("--config", type=Path, required=True)
    preflight.add_argument("--expected-enabled", type=int, choices=(0, 1), required=True)
    preflight.add_argument("--defer-host-bound-spool", action="store_true")
    process = subparsers.add_parser("process-probe")
    process.add_argument("--repository-root", type=Path, required=True)
    process.add_argument("--pid-file", type=Path, required=True)
    process.add_argument("--config", type=Path, required=True)
    process.add_argument("--config-sha256", required=True)
    process.add_argument("--python-executable", type=Path, required=True)
    process.add_argument("--venv-root", type=Path, required=True)
    process.add_argument("--runtime-identity", type=Path, required=True)
    process.add_argument("--expected-enabled", type=int, choices=(0, 1), required=True)
    process.add_argument("--execution-commit", required=True)
    process.add_argument("--execution-tree", required=True)
    process.add_argument("--artifact-sha256", default="")
    process.add_argument("--artifact-manifest", type=Path)
    process.add_argument("--runtime-code-sha256", required=True)
    checkpoint = subparsers.add_parser("log-checkpoint")
    checkpoint.add_argument("--log", type=Path, required=True)
    checkpoint.add_argument("--output", type=Path, required=True)
    log_validate = subparsers.add_parser("log-validate")
    log_validate.add_argument("--log", type=Path, required=True)
    log_validate.add_argument("--checkpoint", type=Path, required=True)
    log_validate.add_argument("--marker", action="append", required=True)
    execute = subparsers.add_parser("execute")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--phase", choices=PHASES, required=True)
    token_source = execute.add_mutually_exclusive_group(required=True)
    token_source.add_argument("--token-file", type=Path)
    token_source.add_argument("--token-fd", type=int)
    execute.add_argument("--disabled-phase-receipt", type=Path)
    execute.add_argument("--authorize-remote-mutation", action="store_true")
    execute.add_argument("--output", type=Path, required=True)
    return parser


def _reject_plaintext_cli_token(argv: Sequence[str]) -> None:
    if any(value == "--token" or value.startswith("--token=") for value in argv):
        raise BuyE3TransactionalDeployError(
            "plaintext --token is forbidden; use stdin, --token-fd, or a 0600 token file"
        )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    _reject_plaintext_cli_token(arguments)
    parser = _parser()
    args = parser.parse_args(arguments)
    command = args.command
    if command == "isolated-preflight":
        payload = isolated_config_preflight(
            args.repository_root,
            args.config,
            bool(args.expected_enabled),
            defer_host_bound_spool=bool(args.defer_host_bound_spool),
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "process-probe":
        payload = capture_runtime_process_probe(
            repository_root=args.repository_root,
            pid_file=args.pid_file,
            config_path=args.config,
            config_sha256=args.config_sha256,
            python_executable=args.python_executable,
            venv_root=args.venv_root,
            runtime_identity_path=args.runtime_identity,
            expected_buy_e3_enabled=bool(args.expected_enabled),
            expected_execution_commit=args.execution_commit,
            expected_execution_tree=args.execution_tree,
            expected_artifact_sha256=args.artifact_sha256,
            expected_runtime_code_sha256=args.runtime_code_sha256,
            artifact_manifest_path=args.artifact_manifest,
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "log-checkpoint":
        payload = gate_v2.capture_startup_log_checkpoint(args.log)
        gate_v2.atomic_write_receipt(args.output, payload)
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "log-validate":
        checkpoint = gate_v2.read_json(args.checkpoint)
        payload = gate_v2.validate_startup_log_after_checkpoint(
            log_path=args.log,
            checkpoint=checkpoint,
            required_markers=tuple(args.marker),
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "execute":
        plan = _read_json(args.plan)
        token = _read_phase_token(token_file=args.token_file, token_fd=args.token_fd)
        execute_phase(
            plan=plan,
            phase=args.phase,
            token=token,
            authorize_remote_mutation=args.authorize_remote_mutation,
            output_path=args.output,
            disabled_phase_receipt_path=args.disabled_phase_receipt,
        )
        return 0
    if command != "plan":
        parser.error("a command is required")
    specification = _read_json(args.specification)
    payload = build_plan(specification=specification, repository_root=args.repository_root)
    gate_v2.atomic_write_receipt(args.output, payload)
    print(json.dumps({"status": payload["status"], "plan": payload["canonical_plan_sha256"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACTIVE_POINTER_STATUS",
    "BuyE3TransactionalDeployError",
    "FAILURE_CLASSES",
    "PHASES",
    "PHASE_COMPLETE",
    "PHASE_FAILED_CLOSED",
    "PLAN_SCHEMA",
    "PREFLIGHT_SCHEMA",
    "RECEIPT_SCHEMA",
    "bind_known_hosts",
    "build_plan",
    "execute_phase",
    "capture_runtime_process_probe",
    "isolated_config_preflight",
    "load_sha_bound_active_pointer",
    "phase_authorization_token_sha256",
    "run_isolated_preflight",
    "validate_phase_receipt",
    "validate_plan",
]
