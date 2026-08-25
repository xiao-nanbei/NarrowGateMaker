"""Transactional planner for the frozen owner-selected BUY E3 runtime.

The default command only writes a deterministic plan.  Any SSH or remote
mutation requires a named phase, an explicit authorization flag, and a secret
whose SHA256 was frozen in the plan.  The planner is external to the c170493e
runtime and never changes the E3 algorithm or the v1 deployment gate.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import importlib.util
import json
import math
import os
import platform
import shlex
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live.runtime_policy import (  # noqa: E402
    F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV,
    F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
    F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
    F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
    F05_BUY_E3_OWNER_OVERRIDE_ENV,
    LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256_ENV,
    LIVE_SAFETY_SUCCESSOR_FILE_SHA256_ENV,
    LIVE_SAFETY_SUCCESSOR_PATH_ENV,
    f05_buy_e3_runtime_policy,
)
from scripts import f05_buy_e3_active_release as active_release  # noqa: E402
from scripts import f05_buy_e3_execution_attempt as execution_attempt  # noqa: E402
from scripts import f05_live_safety_locked_runtime as locked_runtime  # noqa: E402
from scripts import (  # noqa: E402
    f05_live_safety_startup_static_authority as startup_static_authority,
)
from strategy import boolean_cooldown_buy_e3 as buy_e3_runtime  # noqa: E402

STARTUP_EXCHANGE_RECONCILIATION_PATH_ENV = (
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH"
)
STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256_ENV = (
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256"
)
STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256_ENV = (
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256"
)
STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256_ENV = (
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256"
)
STARTUP_STATIC_RUNTIME_AUTHORITY_PATH_ENV = (
    "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH"
)
STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256_ENV = (
    "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256"
)
STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256_ENV = (
    "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256"
)
STARTUP_STATIC_RUNTIME_VERIFIER_PATH_ENV = (
    "NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH"
)
STARTUP_STATIC_RUNTIME_VERIFIER_SHA256_ENV = (
    "NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256"
)
STARTUP_STATIC_TRUSTED_PYTHON_PATH_ENV = (
    "NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH"
)
STARTUP_STATIC_TRUSTED_PYTHON_SHA256_ENV = (
    "NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256"
)
_ACTIVE_RELEASE_PROBE_ARGS_PLACEHOLDER = (
    "__NARROWGATE_ACTIVE_RELEASE_PROBE_ARGS__"
)

try:  # noqa: E402
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 as gate_v1,
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
COMPATIBLE_PLAN_SCHEMA = "f05_buy_e3_owner_transactional_deploy_plan.v2"
SUCCESSOR_PLAN_SCHEMA = "f05_buy_e3_operational_safety_successor_deploy_plan.v1"
SUCCESSOR_ANNOTATED_TAG = "f05-owner-buy-e3-live-safety-successor-v1-final-r4-20260825"
LEGACY_RECEIPT_SCHEMA = "f05_buy_e3_owner_transactional_deploy_receipt.v3"
HISTORICAL_RECEIPT_SCHEMA = "f05_buy_e3_owner_transactional_deploy_receipt.v4"
RECEIPT_SCHEMA = "f05_buy_e3_owner_transactional_deploy_receipt.v5"
PREFLIGHT_SCHEMA = "f05_buy_e3_owner_isolated_config_preflight.v2"
RUNTIME_IDENTITY_SCHEMA = "narrowgate_live_runtime_identity.v1"
LEGACY_STARTUP_ATTESTATION_SCHEMA = "narrowgate_buy_e3_startup_attestation.v3"
HISTORICAL_STARTUP_ATTESTATION_SCHEMA = "narrowgate_buy_e3_startup_attestation.v4"
STARTUP_ATTESTATION_SCHEMA = "narrowgate_buy_e3_startup_attestation.v5"
PREDECESSOR_STARTUP_ATTESTATION_SCHEMA = STARTUP_ATTESTATION_SCHEMA
SUCCESSOR_STARTUP_ATTESTATION_SCHEMA = "narrowgate_buy_e3_startup_attestation.v6"
SUCCESSOR_STOP_FAILURE_RECOVERY_SCHEMA = (
    "f05_buy_e3_successor_stop_failure_reconciliation.v1"
)
SUCCESSOR_READINESS_MARKERS = frozenset(
    {
        "RUNTIME_IDENTITY path=",
        "STARTUP_CANCEL_AND_OPEN_ORDERS_COMPLETE open_orders=0",
        "POSITION_RECONCILIATION_COMPLETE seed=1",
        "STARTUP_EXCHANGE_RECONCILIATION_LINEAGE position_sha256=",
        "Entering main loop...",
    }
)
FROZEN_07EF_EXECUTION_COMMIT = "07ef93733a3a685caba945c7761a48473e403072"
FROZEN_07EF_EXECUTION_TREE = "ff505cd81a8eb11f2087d2ae27e7986fd99b0444"
FROZEN_07EF_DISABLED_CONFIG_SHA256 = (
    "10158a92177cd87b77fdb24a2a477dcab4b41cfb29208cf96c19953edafe166f"
)
FROZEN_07EF_RUNTIME_CODE_SHA256 = (
    "00b7b1b4b9d7b51b8bc90a857381de27be0cee45eff7e3fccb2060409abcc0cc"
)
FROZEN_07EF_ACTIVE_CONFIG_SHA256 = (
    "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
)
_FROZEN_07EF_RUNTIME_SOURCE_SHA256 = {
    "live_buy_runtime": "643423fd04ff44aada8cbc1967a96df6180af87a1d8a02130acb8ab3a85c0cfa",
    "maker_engine": "9ab3dea5c9e7830b1a85030f1dc33d88fd403fb98bb03b2837b0e131926e546f",
    "live_config": "115c57eae1cf413ae2a27851df2f543fbef66e3aa97448301781279b7b7cae73",
    "live_runtime_policy": (
        "23bf62c1e0bfdd0bcc94ef203d39e22f61f9296bf3545157c373ca4f45912964"
    ),
    "live_main": "2a23505ba54630265df168c568c56eebc449ae1bcf42217a308478b7a998b6fe",
}
RUNNING_CHECKOUT_SCHEMA = "narrowgate_running_checkout_identity.v2"
FILL_COOLDOWN_STATE_SCHEMA = "narrowgate_fill_cooldown_state.v2"
INTERPRETER_IDENTITY_SCHEMA = "narrowgate_interpreter_identity.v1"
NATIVE_RUNTIME_IDENTITY_SCHEMA = "narrowgate_native_runtime_identity.v1"
RUNTIME_IDENTITY_BINDING_SCHEMA = "narrowgate_runtime_identity_binding.v1"
RUNTIME_ATTESTATION_CONTRACT_SCHEMA = "narrowgate_runtime_written_startup_attestation_contract.v1"
COMPATIBLE_ACTIVATION_ENVELOPE_SCHEMA = "f05_buy_e3_compatible_activation_envelope.v1"
ACTIVE_RELEASE_RUNTIME_AUTHORITY_SCHEMA = (
    "narrowgate_f05_buy_e3_active_release_runtime_authority.v1"
)
POINTER_SCHEMA = "narrowgate_live_remote_pointer.v1"
ACTIVE_POINTER_STATUS = "current_active"
B0_FILL_COOLDOWN_SECONDS = 85.0
REMOTE_ACTIVE_RELEASE_RELATIVE_DIRECTORY = PurePosixPath(
    "live/private/f05_buy_e3_owner_v1"
)

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
        "phase_timeout",
        "stop_execution_state_uncertain",
        "exchange_reconciliation_failed",
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
    "per_command_timeout_s": 180,
    "whole_phase_timeout_s": 1_800,
    "successor_pre_stop_staging_is_not_live_state_mutation": True,
    "successor_uncertain_stop_forbids_automatic_restart": True,
    "successor_exchange_reconciliation_failure_forbids_restart": True,
    "successor_rollback_requires_signed_exchange_reconciliation": True,
}
PLAN_EVIDENCE_BOUNDARY = {
    "validation_read": False,
    "sealed_holdout_read": False,
    "economic_arms_run": False,
    "hypothetical_live_actions_scored": False,
}
ACTIVATION_ENVELOPE_EVIDENCE_BOUNDARY = {
    "economic_values_read": False,
    "economic_values_persisted": False,
    "hypothetical_live_actions_scored": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
REMOTE_OVERRIDE_ENV = (
    "NARROWGATE_LIVE_REMOTE",
    "NARROWGATE_LIVE_REMOTE_POINTER",
)
STRICT_SSH_OPTIONS = (
    "BatchMode=yes",
    "StrictHostKeyChecking=yes",
    "ConnectTimeout=15",
    "ServerAliveInterval=10",
    "ServerAliveCountMax=3",
)
COMMAND_TIMEOUT_S = 180.0
PHASE_TIMEOUT_S = 1_800.0
MAX_ACTIVE_RELEASE_BYTES = 64 << 20
HOST_BOUND_SPOOL_REMOTE_CHECKS = (
    "allowlisted_root_exists",
    "allowlisted_root_is_directory",
    "allowlisted_root_is_not_symlink",
    "journal_and_epoch_roots_are_strict_children_of_same_allowlisted_root",
)


class BuyE3TransactionalDeployError(RuntimeError):
    """Raised when a deployment plan or transaction cannot fail closed."""


def _is_successor_execution(execution: Mapping[str, Any]) -> bool:
    return str(execution.get("annotated_tag", "")) == SUCCESSOR_ANNOTATED_TAG


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
PreflightRunner = Callable[[Path, Path, bool], Mapping[str, Any]]

_LEGACY_PROCESS_IDENTITY_FIELDS = frozenset(
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
_PROCESS_IDENTITY_FIELDS = _LEGACY_PROCESS_IDENTITY_FIELDS | frozenset(
    {
        "fill_cooldown_restore_mode",
        "initial_buy_remaining_ms",
        "active_release_path",
        "active_release_file_sha256",
        "active_release_canonical_sha256",
        "active_release_execution_commit",
        "active_release_execution_tree",
    }
)
_SUCCESSOR_PROCESS_IDENTITY_FIELDS = _PROCESS_IDENTITY_FIELDS | frozenset(
    {"startup_exchange_reconciliation"}
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
_LEGACY_PHASE_RECEIPT_FIELDS = frozenset(
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
_PHASE_RECEIPT_FIELDS = _LEGACY_PHASE_RECEIPT_FIELDS | frozenset({"active_release_binding"})
_ACTIVATION_ENVELOPE_PHASE_BINDING_FIELDS = frozenset(
    {
        "path",
        "file_sha256",
        "canonical_activation_envelope_sha256",
        "concurrent_resource_receipt_sha256",
        "runtime_regression_receipt_sha256",
        "sell_54_case_receipt_sha256",
    }
)
_LEGACY_ACTIVE_RELEASE_PHASE_BINDING_FIELDS = frozenset(
    {
        "local_path",
        "remote_path",
        "file_sha256",
        "canonical_active_release_sha256",
        "schema_version",
        "status",
    }
)
_ACTIVE_RELEASE_PHASE_BINDING_FIELDS = (
    _LEGACY_ACTIVE_RELEASE_PHASE_BINDING_FIELDS
    | frozenset(
        {
            "active_config_file_sha256",
            "disabled_config_file_sha256",
        }
    )
)
_SUCCESSOR_ACTIVE_RELEASE_PHASE_BINDING_FIELDS = (
    _ACTIVE_RELEASE_PHASE_BINDING_FIELDS
    | frozenset(
        {
            "native_build_receipt_sha256",
            "native_build_receipt_canonical_sha256",
            "native_module_sha256",
            "native_wheel_sha256",
            "native_soabi",
            "runtime_lock_file_sha256",
            "runtime_lock_path",
            "runtime_lock_canonical_sha256",
            "wheelhouse_manifest_file_sha256",
            "wheelhouse_path",
            "wheelhouse_canonical_sha256",
            "install_receipt_path",
            "install_receipt_file_sha256",
            "install_receipt_canonical_sha256",
            "root_wheel_sha256",
            "root_wheel_path",
            "native_wheel_path",
            "installed_record_aggregate_sha256",
            "locked_runtime_interpreter",
        }
    )
)
_LEGACY_RUNTIME_IDENTITY_BINDING_FIELDS = frozenset(
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
_RUNTIME_IDENTITY_BINDING_FIELDS = _LEGACY_RUNTIME_IDENTITY_BINDING_FIELDS | frozenset(
    {
        "active_release_file_sha256",
        "active_release_canonical_sha256",
        "active_release_execution_commit",
        "active_release_execution_tree",
    }
)
_SUCCESSOR_RUNTIME_IDENTITY_BINDING_FIELDS = _RUNTIME_IDENTITY_BINDING_FIELDS | {
    "startup_exchange_reconciliation"
}
_STARTUP_EXCHANGE_RECONCILIATION_BINDING_FIELDS = frozenset(
    {
        "path",
        "file_sha256",
        "canonical_sha256",
        "account_key_sha256",
        "position_lineage_sha256",
    }
)
_LEGACY_STARTUP_ATTESTATION_FIELDS = frozenset(
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
_HISTORICAL_STARTUP_ATTESTATION_FIELDS = (
    _LEGACY_STARTUP_ATTESTATION_FIELDS | frozenset({"buy_e3_active_release"})
)
_STARTUP_ATTESTATION_FIELDS = _HISTORICAL_STARTUP_ATTESTATION_FIELDS | frozenset(
    {"shadow_runtime_identity"}
)
_PREDECESSOR_STARTUP_ATTESTATION_FIELDS = _STARTUP_ATTESTATION_FIELDS
_SUCCESSOR_STARTUP_ATTESTATION_FIELDS = _STARTUP_ATTESTATION_FIELDS | frozenset(
    {"loaded_repository_module_closure", "live_safety_successor"}
)
_LEGACY_STARTUP_GATE_FIELDS = frozenset(
    {
        "fill_cooldown_state_available",
        "fill_cooldown_state_schema_v2",
        "fill_cooldown_restore_mode_valid",
        "fill_cooldown_checkpoint_binding_valid",
        "fill_cooldown_deadline_contract_valid",
        "fill_cooldown_artifact_contract_valid",
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
_HISTORICAL_STARTUP_GATE_FIELDS = _LEGACY_STARTUP_GATE_FIELDS | frozenset(
    {
        "buy_e3_active_release_contract_valid",
        "buy_e3_active_release_matches_checkout",
    }
)
_STARTUP_GATE_FIELDS = _HISTORICAL_STARTUP_GATE_FIELDS | frozenset(
    {
        "buy_e3_active_release_matches_running_config",
        "shadow_config_explicit",
        "global_flow_shadow_backend_contract_valid",
        "global_reference_shadow_state_contract_valid",
    }
)
_PREDECESSOR_STARTUP_GATE_FIELDS = _STARTUP_GATE_FIELDS
_SUCCESSOR_STARTUP_GATE_FIELDS = _STARTUP_GATE_FIELDS | frozenset(
    {
        "repository_module_closure_available",
        "repository_module_closure_complete",
        "mandatory_safety_modules_loaded",
        "live_safety_successor_authority_valid",
    }
)
_LIVE_SAFETY_SUCCESSOR_IDENTITY_FIELDS = frozenset(
    {
        "path",
        "file_sha256",
        "canonical_sha256",
        "execution_commit",
        "execution_tree",
        "active_config_file_sha256",
        "disabled_config_file_sha256",
        "native_module_sha256",
        "native_wheel_sha256",
        "native_soabi",
        "native_build_receipt_sha256",
        "native_build_receipt_canonical_sha256",
        "runtime_lock_file_sha256",
        "runtime_lock_path",
        "runtime_lock_canonical_sha256",
        "wheelhouse_manifest_file_sha256",
        "wheelhouse_path",
        "wheelhouse_canonical_sha256",
        "install_receipt_path",
        "install_receipt_file_sha256",
        "install_receipt_canonical_sha256",
        "root_wheel_sha256",
        "root_wheel_path",
        "native_wheel_path",
        "installed_record_aggregate_sha256",
        "locked_runtime_interpreter",
    }
)
_HISTORICAL_STARTUP_ACTIVE_RELEASE_FIELDS = frozenset(
    {
        "path",
        "file_sha256",
        "file_canonical_sha256",
        "execution_commit",
        "execution_tree",
        "annotated_operational_tag",
        "annotated_operational_tag_object",
    }
)
_STARTUP_ACTIVE_RELEASE_FIELDS = _HISTORICAL_STARTUP_ACTIVE_RELEASE_FIELDS | frozenset(
    {"active_config_file_sha256", "disabled_config_file_sha256"}
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
_LOADED_RUNTIME_MODULE_IDENTITIES = {
    "live_main": ("live.main", "live/main.py"),
    "live_config": ("live.config", "live/config.py"),
    "live_runtime_policy": ("live.runtime_policy", "live/runtime_policy.py"),
    "live_ws_handler": ("live.ws_handler", "live/ws_handler.py"),
    "maker_engine": ("strategy.maker_engine", "strategy/maker_engine.py"),
    "signal_engine": ("strategy.signal", "strategy/signal.py"),
    "global_flow": ("strategy.global_flow", "strategy/global_flow.py"),
    "global_reference": (
        "strategy.global_reference",
        "strategy/global_reference.py",
    ),
    "boolean_cooldown_live": (
        "strategy.boolean_cooldown_live",
        "strategy/boolean_cooldown_live.py",
    ),
    "boolean_cooldown_buy_e3": (
        "strategy.boolean_cooldown_buy_e3",
        "strategy/boolean_cooldown_buy_e3.py",
    ),
}
_SUCCESSOR_LOADED_RUNTIME_MODULE_IDENTITIES = {
    **_LOADED_RUNTIME_MODULE_IDENTITIES,
    "inventory_manager": ("strategy.inventory_manager", "strategy/inventory_manager.py"),
    "order_manager": ("strategy.order_manager", "strategy/order_manager.py"),
    "quote_core": ("strategy.quote_core", "strategy/quote_core.py"),
    "replay_controls": ("strategy.replay_controls", "strategy/replay_controls.py"),
    "continuous_accounting": (
        "models.replay.continuous_accounting",
        "models/replay/continuous_accounting.py",
    ),
    "order_lifecycle_live_writer": (
        "execution.order_lifecycle_live_writer_v2",
        "execution/order_lifecycle_live_writer_v2.py",
    ),
}
_LOADED_RUNTIME_MODULE_ROLES = frozenset(_LOADED_RUNTIME_MODULE_IDENTITIES)
_PREDECESSOR_LOADED_RUNTIME_MODULE_IDENTITIES = _LOADED_RUNTIME_MODULE_IDENTITIES
_HISTORICAL_LOADED_RUNTIME_MODULE_IDENTITIES = {
    "live_main": ("live.main", "live/main.py"),
    "live_config": ("live.config", "live/config.py"),
    "live_runtime_policy": ("live.runtime_policy", "live/runtime_policy.py"),
    "live_ws_handler": ("live.ws_handler", "live/ws_handler.py"),
    "maker_engine": ("strategy.maker_engine", "strategy/maker_engine.py"),
    "boolean_cooldown_live": (
        "strategy.boolean_cooldown_live",
        "strategy/boolean_cooldown_live.py",
    ),
    "boolean_cooldown_buy_e3": (
        "strategy.boolean_cooldown_buy_e3",
        "strategy/boolean_cooldown_buy_e3.py",
    ),
}
_HISTORICAL_LOADED_RUNTIME_MODULE_ROLES = frozenset(
    _HISTORICAL_LOADED_RUNTIME_MODULE_IDENTITIES
)
_CURRENT_RUNTIME_SOURCE_PATHS = {
    "live_buy_runtime": "strategy/boolean_cooldown_buy_e3.py",
    "maker_engine": "strategy/maker_engine.py",
    "live_config": "live/config.py",
    "live_runtime_policy": "live/runtime_policy.py",
    "live_main": "live/main.py",
    "live_ws_handler": "live/ws_handler.py",
    "sell_owner_runtime": "strategy/boolean_cooldown_live.py",
    "signal_engine": "strategy/signal.py",
    "global_flow": "strategy/global_flow.py",
    "global_reference": "strategy/global_reference.py",
}
if set(_CURRENT_RUNTIME_SOURCE_PATHS.values()) != {
    relative for _module, relative in _LOADED_RUNTIME_MODULE_IDENTITIES.values()
}:  # pragma: no cover - import-time invariant
    raise RuntimeError("current runtime source roles differ from startup loaded modules")


def _successor_runtime_source_paths(repository_root: Path) -> dict[str, str]:
    root = repository_root.expanduser().resolve(strict=True)
    paths = {
        candidate.relative_to(root).as_posix()
        for root_name in ("live", "strategy", "execution", "features")
        for candidate in (root / root_name).rglob("*.py")
        if "__pycache__" not in candidate.parts
    }
    paths.update(
        {
            "market_fusion.py",
            "models/replay/baseline_epoch_manifest.py",
            "models/replay/continuous_accounting.py",
            "models/replay/prospective_baseline_epoch.py",
            "data_paths.py",
            "scripts/f05_live_safety_locked_runtime.py",
            "scripts/f05_live_safety_startup_static_authority.py",
            "live/run.sh",
            "live/profiles/native.env",
        }
    )
    missing = sorted(path for path in paths if not (root / path).is_file())
    if missing:
        raise RuntimeError("successor runtime source closure is incomplete: " + ", ".join(missing))
    return {path: path for path in sorted(paths)}
_CURRENT_RUNTIME_SOURCE_AUTHORITY_BASIS = (
    "validated_current_execution_commit_and_working_tree"
)
_LOADED_RUNTIME_MODULE_FIELDS = frozenset(
    {
        "module_name",
        "origin_path",
        "repository_relative_path",
        "source_sha256",
    }
)
_FILE_BYTE_IDENTITY_FIELDS = frozenset({"reported_path", "resolved_path", "sha256", "size_bytes"})
_INTERPRETER_IDENTITY_FIELDS = frozenset({"schema_version", "version", "before", "after", "stable"})
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
_PREDECESSOR_NATIVE_RUNTIME_IDENTITY_FIELDS = _NATIVE_RUNTIME_IDENTITY_FIELDS
_SUCCESSOR_NATIVE_RUNTIME_IDENTITY_FIELDS = _NATIVE_RUNTIME_IDENTITY_FIELDS | {
    "abi_contract",
    "locked_runtime",
}
_LOCKED_RUNTIME_INTERPRETER_FIELDS = frozenset(
    {
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
)
_LOCKED_RUNTIME_STARTUP_FIELDS = frozenset(
    {
        "validated",
        "venv_selector_path",
        "venv_selector_target",
        "venv_real_path",
        "python_real_path",
        "install_receipt_path",
        "install_receipt_file_sha256",
        "install_receipt_canonical_sha256",
        "runtime_lock_canonical_sha256",
        "wheelhouse_canonical_sha256",
        "installed_record_aggregate_sha256",
        "interpreter",
    }
)
_SUCCESSOR_NATIVE_BUILD_PROJECTED_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "file_sha256",
        "canonical_sha256",
        "module_sha256",
        "wheel_sha256",
        "soabi",
        "python_minor",
        "platform",
        "runtime_lock_file_sha256",
        "runtime_lock_path",
        "runtime_lock_canonical_sha256",
        "wheelhouse_manifest_file_sha256",
        "wheelhouse_path",
        "wheelhouse_canonical_sha256",
        "install_receipt_path",
        "install_receipt_file_sha256",
        "install_receipt_canonical_sha256",
        "root_wheel_sha256",
        "root_wheel_path",
        "native_wheel_path",
        "installed_record_aggregate_sha256",
        "interpreter",
    }
)
_SHADOW_RUNTIME_IDENTITY_FIELDS = frozenset(
    {
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
)
_GLOBAL_FLOW_BACKEND_FIELDS = frozenset(
    {
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
_COMMAND_FIELDS = frozenset({"label", "argv", "command_sha256", "mutates_remote", "after_stop"})
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
_PLAN_BASE_FIELDS = _PLAN_CORE_FIELDS | frozenset({"plan_core_sha256", "canonical_plan_sha256"})
_PLAN_ACTIVATION_FIELDS = frozenset({"activation_gate", "activation_gate_receipt_sha256"})
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
_COMPATIBLE_ACTIVATION_GATE_BINDING_FIELDS = _ACTIVATION_GATE_BINDING_FIELDS | {"kind"}
_COMPATIBLE_ACTIVATION_GATE_KIND = "compatible_execution_generic_v1"
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


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 40 or any(char not in "0123456789abcdef" for char in normalized):
        raise BuyE3TransactionalDeployError(f"{label} is not a Git SHA")
    return normalized


def _remote_active_release_path(repo_root: str, file_sha256: str) -> str:
    digest = _require_sha256(file_sha256, "active release file hash")
    return str(
        PurePosixPath(str(repo_root))
        / REMOTE_ACTIVE_RELEASE_RELATIVE_DIRECTORY
        / f"active_release-{digest}.json"
    )


def _startup_static_authority_binding_from_payload(
    payload: Mapping[str, Any], *, stage_root: str
) -> dict[str, str]:
    canonical = _require_sha256(
        payload.get(startup_static_authority.CANONICAL_FIELD),
        "startup static authority canonical hash",
    )
    raw = startup_static_authority.file_bytes(payload)
    file_sha256 = hashlib.sha256(raw).hexdigest()
    return {
        "remote_path": str(
            PurePosixPath(stage_root)
            / f"startup-static-runtime-authority-{file_sha256}.json"
        ),
        "file_sha256": file_sha256,
        "canonical_sha256": canonical,
    }


def _startup_static_authority_from_release(
    *,
    repo_root: str,
    execution: Mapping[str, Any],
    runtime_sources: Mapping[str, Any],
    host: Mapping[str, Any],
    remote: Mapping[str, Any],
    release_binding: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    commit = str(execution["execution_commit"])
    stage_root = str(remote["stage_root"])
    repo_root = str(repo_root)
    runtime_root = f"{stage_root}/runtime-{commit}"
    venv_path = f"{stage_root}/venv-{commit}"
    static_relative = "scripts/f05_live_safety_startup_static_authority.py"
    locked_relative = "scripts/f05_live_safety_locked_runtime.py"
    sources = runtime_sources.get("files")
    if not isinstance(sources, Mapping):
        raise BuyE3TransactionalDeployError(
            "startup static authority lacks runtime source bindings"
        )

    def source_hash(relative: str) -> str:
        binding = sources.get(relative)
        if not isinstance(binding, Mapping):
            raise BuyE3TransactionalDeployError(
                f"startup static authority lacks source: {relative}"
            )
        return _require_sha256(
            binding.get("working_file_sha256"),
            f"startup static authority source {relative}",
        )

    interpreter = release_binding.get("locked_runtime_interpreter")
    if not isinstance(interpreter, Mapping):
        raise BuyE3TransactionalDeployError(
            "startup static authority lacks locked interpreter"
        )
    payload = startup_static_authority.build_authority(
        execution_commit=commit,
        execution_tree=str(execution["execution_tree"]),
        repository_path=repo_root,
        runtime_root=runtime_root,
        selector_path=f"{repo_root}/.venv-active",
        selector_target=venv_path,
        trusted_python_path=str(host["trusted_static_python_path"]),
        trusted_python_sha256=str(host["trusted_static_python_sha256"]),
        authority_verifier_path=f"{runtime_root}/{static_relative}",
        authority_verifier_sha256=source_hash(static_relative),
        locked_runtime_verifier_path=f"{runtime_root}/{locked_relative}",
        locked_runtime_verifier_sha256=source_hash(locked_relative),
        venv_path=venv_path,
        target_python_path=f"{venv_path}/bin/python3",
        target_python_sha256=str(interpreter["executable_sha256"]),
        install_receipt_path=str(release_binding["install_receipt_path"]),
        install_receipt_file_sha256=str(
            release_binding["install_receipt_file_sha256"]
        ),
        install_receipt_canonical_sha256=str(
            release_binding["install_receipt_canonical_sha256"]
        ),
        installed_record_aggregate_sha256=str(
            release_binding["installed_record_aggregate_sha256"]
        ),
        safety_release_path=str(release_binding["remote_path"]),
        safety_release_file_sha256=str(release_binding["file_sha256"]),
        safety_release_canonical_sha256=str(
            release_binding["canonical_active_release_sha256"]
        ),
    )
    return payload, _startup_static_authority_binding_from_payload(
        payload, stage_root=stage_root
    )


def _startup_static_authority_env(
    authority: Mapping[str, Any], binding: Mapping[str, Any]
) -> str:
    return " ".join(
        (
            f"{STARTUP_STATIC_RUNTIME_AUTHORITY_PATH_ENV}="
            f"{shlex.quote(str(binding['remote_path']))}",
            f"{STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256_ENV}="
            f"{shlex.quote(str(binding['file_sha256']))}",
            f"{STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256_ENV}="
            f"{shlex.quote(str(binding['canonical_sha256']))}",
            f"{STARTUP_STATIC_RUNTIME_VERIFIER_PATH_ENV}="
            f"{shlex.quote(str(authority['authority_verifier']['path']))}",
            f"{STARTUP_STATIC_RUNTIME_VERIFIER_SHA256_ENV}="
            f"{shlex.quote(str(authority['authority_verifier']['sha256']))}",
            f"{STARTUP_STATIC_TRUSTED_PYTHON_PATH_ENV}="
            f"{shlex.quote(str(authority['trusted_python']['path']))}",
            f"{STARTUP_STATIC_TRUSTED_PYTHON_SHA256_ENV}="
            f"{shlex.quote(str(authority['trusted_python']['sha256']))}",
        )
    )


def _clean_remote_shell_command(command: str) -> str:
    if not command.strip():
        raise BuyE3TransactionalDeployError("clean remote command is empty")
    return (
        '/usr/bin/env -i HOME="$HOME" PATH=/usr/bin:/bin '
        "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
        f"/bin/bash --noprofile --norc -c {shlex.quote(command)}"
    )


def _clean_static_gate_command(command: str) -> str:
    return _clean_remote_shell_command(command)


def _plan_core_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    missing = _PLAN_CORE_FIELDS - set(plan)
    if missing:
        raise BuyE3TransactionalDeployError(
            "deployment plan core lacks: " + ", ".join(sorted(missing))
        )
    return {field: plan[field] for field in sorted(_PLAN_CORE_FIELDS)}


def _plan_core_sha256(plan: Mapping[str, Any]) -> str:
    return gate_v2.canonical_sha256(_plan_core_payload(plan))


def _runtime_attestation_contract(
    remote: Mapping[str, Any],
    *,
    startup_schema_version: str = STARTUP_ATTESTATION_SCHEMA,
) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_ATTESTATION_CONTRACT_SCHEMA,
        "required_for_activation": True,
        "required_for_disabled_deploy_completion": True,
        "remote_path": str(remote["runtime_identity_path"]),
        "runtime_identity_schema_version": RUNTIME_IDENTITY_SCHEMA,
        "startup_attestation_schema_version": startup_schema_version,
        "authority": "runtime_written_startup_attestation",
        "evidence_classification": "runtime_identity_file_unsigned_structural_evidence",
        "cryptographic_signature_present": False,
        "local_receipt_standalone_activation_evidence": False,
        "expected_value_echo_is_evidence": False,
    }


def _read_json(path: Path) -> dict[str, Any]:
    return gate_v2.read_json(path)


def _load_compatible_attempt(
    *, repository_root: Path, raw: Any
) -> tuple[dict[str, Any], dict[str, str]] | None:
    if raw is None:
        return None
    allowed_fields = {"path", "file_sha256"}
    if not isinstance(raw, Mapping) or set(raw) not in (
        allowed_fields,
        allowed_fields | {"canonical_sha256"},
    ):
        raise BuyE3TransactionalDeployError("compatible execution attempt binding is malformed")
    path = Path(str(raw["path"])).expanduser().absolute()
    expected_file_sha256 = _require_sha256(
        raw["file_sha256"], "compatible execution attempt file hash"
    )
    if path.is_symlink() or not path.is_file():
        raise BuyE3TransactionalDeployError("compatible execution attempt manifest is unavailable")
    if gate_v2.file_sha256(path) != expected_file_sha256:
        raise BuyE3TransactionalDeployError("compatible execution attempt file hash drifted")
    try:
        payload = execution_attempt.validate_manifest(
            path,
            repository_root=repository_root,
            require_current_checkout=True,
        )
    except execution_attempt.ExecutionAttemptError as exc:
        raise BuyE3TransactionalDeployError(
            f"compatible execution attempt is invalid: {exc}"
        ) from exc
    binding = {
        "path": str(path),
        "file_sha256": expected_file_sha256,
        "canonical_sha256": str(payload["canonical_execution_attempt_sha256"]),
    }
    if (
        "canonical_sha256" in raw
        and _require_sha256(raw["canonical_sha256"], "compatible execution attempt canonical hash")
        != binding["canonical_sha256"]
    ):
        raise BuyE3TransactionalDeployError("compatible execution attempt canonical hash drifted")
    return payload, binding


def _compatible_execution_identity(
    *,
    specification_execution: Mapping[str, Any],
    attempt_payload: Mapping[str, Any],
    attempt_binding: Mapping[str, str],
) -> dict[str, Any]:
    runtime = attempt_payload.get("runtime_execution")
    if not isinstance(runtime, Mapping):
        raise BuyE3TransactionalDeployError("compatible execution attempt lacks runtime identity")
    expected_specification = {
        "commit": runtime.get("execution_commit"),
        "tree": runtime.get("execution_tree"),
        "annotated_tag": runtime.get("annotated_tag"),
        "annotated_tag_object": runtime.get("annotated_tag_object"),
    }
    if dict(specification_execution) != expected_specification:
        raise BuyE3TransactionalDeployError(
            "deployment specification differs from compatible runtime identity"
        )
    return {
        **dict(runtime),
        "compatible_attempt_manifest": dict(attempt_binding),
    }


def _current_runtime_sources(
    *,
    repository_root: Path,
    execution_commit: str,
    attempt_payload: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    execution_commit = _require_git_sha(
        execution_commit,
        "current runtime execution commit",
    )
    by_path: dict[str, Mapping[str, Any]] = {}
    if attempt_payload is not None:
        runtime_sources = attempt_payload.get("runtime_sources")
        files = runtime_sources.get("files") if isinstance(runtime_sources, Mapping) else None
        if not isinstance(files, Mapping):
            raise BuyE3TransactionalDeployError(
                "compatible execution attempt lacks runtime source bindings"
            )
        for raw in files.values():
            if not isinstance(raw, Mapping):
                raise BuyE3TransactionalDeployError(
                    "compatible runtime source binding is malformed"
                )
            relative = str(raw.get("repository_relative_path", "")).strip()
            if not relative or relative in by_path:
                raise BuyE3TransactionalDeployError(
                    "compatible runtime source paths are incomplete or duplicated"
                )
            by_path[relative] = raw
        runtime = attempt_payload.get("runtime_execution")
        if (
            not isinstance(runtime, Mapping)
            or _require_git_sha(
                runtime.get("execution_commit"),
                "compatible runtime execution commit",
            )
            != execution_commit
        ):
            raise BuyE3TransactionalDeployError(
                "compatible execution attempt runtime identity drifted"
            )
    root = repository_root.expanduser().resolve(strict=True)
    selected_paths = (
        _CURRENT_RUNTIME_SOURCE_PATHS if source_paths is None else dict(source_paths)
    )
    bindings: dict[str, Any] = {}
    for role, relative in selected_paths.items():
        candidate = root / relative
        if candidate.is_symlink() or not candidate.is_file():
            raise BuyE3TransactionalDeployError(
                f"current runtime source is unavailable: {relative}"
            )
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(root)
            committed = subprocess.run(
                ("git", "show", f"{execution_commit}:{relative}"),
                cwd=root,
                check=True,
                capture_output=True,
                timeout=20.0,
            ).stdout
        except (ValueError, OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise BuyE3TransactionalDeployError(
                f"cannot bind current runtime source: {relative}"
            ) from exc
        working = resolved.read_bytes()
        if working != committed:
            raise BuyE3TransactionalDeployError(
                f"current runtime source differs from validated execution: {relative}"
            )
        sha256 = hashlib.sha256(working).hexdigest()
        source = by_path.get(relative)
        if source is not None and _require_sha256(
            source.get("file_sha256"), f"compatible runtime source {role}"
        ) != sha256:
            raise BuyE3TransactionalDeployError(
                f"compatible attempt runtime source drifted: {relative}"
            )
        bindings[role] = {
            "repository_relative_path": relative,
            "execution_commit_blob_sha256": sha256,
            "working_file_sha256": sha256,
            "authority_basis": _CURRENT_RUNTIME_SOURCE_AUTHORITY_BASIS,
        }
    return {
        "files": bindings,
        "runtime_code_sha256": gate_v2.canonical_sha256(bindings),
    }


def _successor_runtime_sources(
    *, repository_root: Path, execution_commit: str
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    return _current_runtime_sources(
        repository_root=root,
        execution_commit=execution_commit,
        source_paths=_successor_runtime_source_paths(root),
    )


def _compatible_runtime_sources(
    attempt_payload: Mapping[str, Any],
    *,
    repository_root: Path,
) -> dict[str, Any]:
    runtime = attempt_payload.get("runtime_execution")
    if not isinstance(runtime, Mapping):
        raise BuyE3TransactionalDeployError(
            "compatible execution attempt lacks runtime identity"
        )
    return _current_runtime_sources(
        repository_root=repository_root,
        execution_commit=_require_git_sha(
            runtime.get("execution_commit"), "compatible runtime execution commit"
        ),
        attempt_payload=attempt_payload,
    )


def _revalidate_compatible_execution(
    *, repository_root: Path, execution: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = execution.get("compatible_attempt_manifest")
    loaded = _load_compatible_attempt(repository_root=repository_root, raw=binding)
    if loaded is None:
        raise BuyE3TransactionalDeployError(
            "compatible deployment plan lacks its execution attempt manifest"
        )
    payload, observed_binding = loaded
    runtime = payload.get("runtime_execution")
    expected = {
        **(dict(runtime) if isinstance(runtime, Mapping) else {}),
        "compatible_attempt_manifest": observed_binding,
    }
    if dict(execution) != expected:
        raise BuyE3TransactionalDeployError("compatible deployment execution identity drifted")
    return payload, _compatible_runtime_sources(
        payload,
        repository_root=repository_root,
    )


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


def validate_b0_config_contract(
    config_path: Path,
    *,
    expected_fill_cooldown_s: float = B0_FILL_COOLDOWN_SECONDS,
) -> dict[str, Any]:
    """Require an exact numeric B0 cooldown; no coercion or tolerance is allowed."""

    expected = float(expected_fill_cooldown_s)
    if not math.isfinite(expected) or expected != B0_FILL_COOLDOWN_SECONDS:
        raise BuyE3TransactionalDeployError("B0 expected cooldown identity drifted")
    value = _strategy_mapping(config_path).get("fill_cooldown")
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) != expected
    ):
        raise BuyE3TransactionalDeployError(
            "strategy.fill_cooldown must be the exact numeric B0 value 85"
        )
    return {
        "field": "strategy.fill_cooldown",
        "seconds": expected,
        "exact_numeric": True,
    }


def _host_bound_spool_gate(config_path: Path, *, defer_host_bound_spool: bool) -> dict[str, Any]:
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
            remote_spool_allowlisted_roots=lifecycle["remote_spool_allowlisted_roots"],
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
def _host_bound_spool_validation_scope(config_path: Path, *, defer_host_bound_spool: bool):
    import live.config as live_config
    from execution.order_lifecycle_journal_storage_v2 import BOUNDED_REMOTE_SPOOL

    gate = _host_bound_spool_gate(config_path, defer_host_bound_spool=defer_host_bound_spool)
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
    validate_b0_config_contract(config)
    previous_buy = os.environ.get(F05_BUY_E3_OWNER_OVERRIDE_ENV)
    previous_sell = os.environ.get(F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV)
    release_env_names = (
        F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
        F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
        F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
    )
    previous_release = {name: os.environ.get(name) for name in release_env_names}
    try:
        for name in release_env_names:
            os.environ.pop(name, None)
        os.environ[F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV] = "1"
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
        if previous_buy is None:
            os.environ.pop(F05_BUY_E3_OWNER_OVERRIDE_ENV, None)
        else:
            os.environ[F05_BUY_E3_OWNER_OVERRIDE_ENV] = previous_buy
        if previous_sell is None:
            os.environ.pop(F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV, None)
        else:
            os.environ[F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV] = previous_sell
        for name, value in previous_release.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
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
        "-B",
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
    for name in (
        F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
        F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
        F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
    ):
        environment.pop(name, None)
    completed = runner(
        command,
        cwd=repository_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_S,
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


def _encode_runtime_source_authority(runtime_sources: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(runtime_sources),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return base64.b64encode(encoded).decode("ascii")


def _decode_runtime_source_authority(value: str) -> dict[str, Any]:
    try:
        decoded = base64.b64decode(str(value).encode("ascii"), validate=True)
        payload = json.loads(decoded.decode("ascii"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise BuyE3TransactionalDeployError(
            "runtime source authority is not canonical base64 JSON"
        ) from exc
    if not isinstance(payload, Mapping) or _encode_runtime_source_authority(payload) != value:
        raise BuyE3TransactionalDeployError("runtime source authority encoding drifted")
    return dict(payload)


def _validate_checkout_runtime_source_authority(
    *,
    repository_root: Path,
    execution_commit: str,
    runtime_sources: Mapping[str, Any],
    expected_runtime_code_sha256: str,
    expected_startup_attestation_schema_version: str,
) -> dict[str, Any]:
    files = runtime_sources.get("files") if isinstance(runtime_sources, Mapping) else None
    if expected_startup_attestation_schema_version not in {
        SUCCESSOR_STARTUP_ATTESTATION_SCHEMA,
        STARTUP_ATTESTATION_SCHEMA,
        HISTORICAL_STARTUP_ATTESTATION_SCHEMA,
        LEGACY_STARTUP_ATTESTATION_SCHEMA,
    }:
        raise BuyE3TransactionalDeployError("runtime source startup schema is unsupported")
    successor = (
        expected_startup_attestation_schema_version
        == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
    )
    current = expected_startup_attestation_schema_version == STARTUP_ATTESTATION_SCHEMA
    expected_paths = _successor_runtime_source_paths(repository_root) if successor else (
        _CURRENT_RUNTIME_SOURCE_PATHS
        if current
        else gate_v2.REQUIRED_RUNTIME_PATHS
    )
    if (
        not isinstance(files, Mapping)
        or set(files) != set(expected_paths)
        or set(runtime_sources) != {"files", "runtime_code_sha256"}
    ):
        raise BuyE3TransactionalDeployError("runtime source authority role set drifted")
    root = repository_root.expanduser().resolve(strict=True)
    normalized: dict[str, Any] = {}
    expected_fields = (
        {
            "repository_relative_path",
            "execution_commit_blob_sha256",
            "working_file_sha256",
            "authority_basis",
        }
        if current or successor
        else {
            "repository_relative_path",
            "artifact_manifest_sha256",
            "execution_commit_blob_sha256",
            "working_file_sha256",
        }
    )
    for role, expected_relative in expected_paths.items():
        raw = files.get(role)
        if not isinstance(raw, Mapping) or set(raw) != expected_fields:
            raise BuyE3TransactionalDeployError(
                f"runtime source authority fields drifted: {role}"
            )
        relative = str(raw.get("repository_relative_path", ""))
        expected_sha = _require_sha256(
            raw.get("working_file_sha256"), f"runtime source authority {role}"
        )
        if (
            relative != expected_relative
            or raw.get("execution_commit_blob_sha256") != expected_sha
            or (
                (current or successor)
                and raw.get("authority_basis")
                != _CURRENT_RUNTIME_SOURCE_AUTHORITY_BASIS
            )
            or (
                not (current or successor)
                and raw.get("artifact_manifest_sha256") != expected_sha
            )
        ):
            raise BuyE3TransactionalDeployError(
                f"runtime source authority identity drifted: {role}"
            )
        candidate = root / relative
        if candidate.is_symlink() or not candidate.is_file():
            raise BuyE3TransactionalDeployError(
                f"runtime source authority path is unavailable: {role}"
            )
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(root)
        except ValueError as exc:
            raise BuyE3TransactionalDeployError(
                f"runtime source authority path escaped: {role}"
            ) from exc
        working_sha = gate_v2.file_sha256(resolved)
        completed = subprocess.run(
            ("git", "show", f"{execution_commit}:{relative}"),
            cwd=root,
            check=True,
            capture_output=True,
        )
        blob_sha = hashlib.sha256(completed.stdout).hexdigest()
        if working_sha != expected_sha or blob_sha != expected_sha:
            raise BuyE3TransactionalDeployError(
                f"runtime source authority bytes drifted: {role}"
            )
        normalized[role] = dict(raw)
    aggregate = _require_sha256(
        expected_runtime_code_sha256, "expected runtime source aggregate"
    )
    if (
        runtime_sources.get("runtime_code_sha256") != aggregate
        or gate_v2.canonical_sha256(normalized) != aggregate
    ):
        raise BuyE3TransactionalDeployError("runtime source authority aggregate drifted")
    return {"files": normalized, "runtime_code_sha256": aggregate}


def _validated_expected_runtime_source_hashes(
    runtime_sources: Mapping[str, Any],
) -> dict[str, Any]:
    files = runtime_sources.get("files")
    if not isinstance(files, Mapping):
        raise BuyE3TransactionalDeployError("runtime source bindings are malformed")
    if runtime_sources.get("runtime_code_sha256") != gate_v2.canonical_sha256(files):
        raise BuyE3TransactionalDeployError("runtime source aggregate is malformed")
    current = bool(files) and all(
        isinstance(raw, Mapping)
        and str(role) == str(raw.get("repository_relative_path", ""))
        and raw.get("authority_basis") == _CURRENT_RUNTIME_SOURCE_AUTHORITY_BASIS
        for role, raw in files.items()
    )
    predecessor = set(files) == set(_CURRENT_RUNTIME_SOURCE_PATHS)
    historical = set(files) == set(gate_v2.REQUIRED_RUNTIME_PATHS)
    if not current and not predecessor and not historical:
        raise BuyE3TransactionalDeployError("runtime source role set is malformed")
    expected_paths = (
        {str(role): str(role) for role in files}
        if current
        else _CURRENT_RUNTIME_SOURCE_PATHS
        if predecessor
        else gate_v2.REQUIRED_RUNTIME_PATHS
    )
    expected_fields = (
        {
            "repository_relative_path",
            "execution_commit_blob_sha256",
            "working_file_sha256",
            "authority_basis",
        }
        if current or predecessor
        else {
            "repository_relative_path",
            "artifact_manifest_sha256",
            "execution_commit_blob_sha256",
            "working_file_sha256",
        }
    )
    expected: dict[str, str] = {}
    for role, raw in files.items():
        if not isinstance(raw, Mapping) or set(raw) != expected_fields:
            raise BuyE3TransactionalDeployError(f"runtime source binding is malformed: {role}")
        path = str(raw.get("repository_relative_path", "")).strip()
        relative = PurePosixPath(path)
        if not path or relative.is_absolute() or ".." in relative.parts:
            raise BuyE3TransactionalDeployError(f"runtime source path is unsafe: {role}")
        hash_fields = (
            ("execution_commit_blob_sha256", "working_file_sha256")
            if current or predecessor
            else (
                "artifact_manifest_sha256",
                "execution_commit_blob_sha256",
                "working_file_sha256",
            )
        )
        hashes = {
            _require_sha256(raw.get(field), f"runtime source {role} {field}")
            for field in hash_fields
        }
        if (
            len(hashes) != 1
            or path in expected
            or path != expected_paths[role]
            or (
                (current or predecessor)
                and raw.get("authority_basis")
                != _CURRENT_RUNTIME_SOURCE_AUTHORITY_BASIS
            )
        ):
            raise BuyE3TransactionalDeployError(f"runtime source binding disagrees: {role}")
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
        or (expected_reported_path is not None and reported_path != expected_reported_path)
        or (expected_resolved_path is not None and resolved_path != expected_resolved_path)
    ):
        raise BuyE3TransactionalDeployError(f"{label} is malformed")
    _require_sha256(identity.get("sha256"), f"{label} hash")
    return identity


def _empty_active_release_identity() -> dict[str, str]:
    return {field: "" for field in _STARTUP_ACTIVE_RELEASE_FIELDS}


def _active_release_contract(schema_version: Any) -> tuple[str, str]:
    contracts = {
        buy_e3_runtime.ACTIVE_RELEASE_SCHEMA: (
            buy_e3_runtime.ACTIVE_RELEASE_IDENTITY,
            buy_e3_runtime.ACTIVE_RELEASE_STATUS,
        ),
        buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_SCHEMA: (
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_IDENTITY,
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_STATUS,
        ),
        buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V2_SCHEMA: (
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V2_IDENTITY,
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V2_STATUS,
        ),
        buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA: (
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_IDENTITY,
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_STATUS,
        ),
        buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA: (
            buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_IDENTITY,
            buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_STATUS,
        ),
    }
    try:
        return contracts[str(schema_version)]
    except KeyError as exc:
        raise BuyE3TransactionalDeployError(
            "active release schema is not supported"
        ) from exc


def _successor_native_build_binding(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _SUCCESSOR_NATIVE_BUILD_PROJECTED_FIELDS:
        raise BuyE3TransactionalDeployError(
            "successor native build projection fields drifted"
        )
    native = dict(raw)
    if (
        native.get("schema_version")
        != "narrowgate_linux_x86_64_native_build_receipt.v2"
        or native.get("status")
        != "exact_tag_native_build_dependency_lock_and_parity_passed"
        or native.get("platform") != "linux_x86_64"
        or native.get("python_minor") != "3.12"
        or not str(native.get("soabi", "")).startswith("cpython-312-")
        or any(
            not PurePosixPath(str(native.get(field, ""))).is_absolute()
            for field in (
                "runtime_lock_path",
                "wheelhouse_path",
                "install_receipt_path",
                "root_wheel_path",
                "native_wheel_path",
            )
        )
    ):
        raise BuyE3TransactionalDeployError(
            "successor native build projection identity drifted"
        )
    for field in (
        "file_sha256",
        "canonical_sha256",
        "module_sha256",
        "wheel_sha256",
        "runtime_lock_file_sha256",
        "runtime_lock_canonical_sha256",
        "wheelhouse_manifest_file_sha256",
        "wheelhouse_canonical_sha256",
        "install_receipt_file_sha256",
        "install_receipt_canonical_sha256",
        "root_wheel_sha256",
        "installed_record_aggregate_sha256",
    ):
        native[field] = _require_sha256(native.get(field), field)
    interpreter = native.get("interpreter")
    if (
        not isinstance(interpreter, Mapping)
        or set(interpreter) != _LOCKED_RUNTIME_INTERPRETER_FIELDS
        or interpreter.get("implementation") != "cpython"
        or interpreter.get("version_info", [])[:2] != [3, 12]
        or interpreter.get("is_virtual_environment") is not True
        or interpreter.get("soabi") != native["soabi"]
    ):
        raise BuyE3TransactionalDeployError(
            "successor native build interpreter authority drifted"
        )
    _require_sha256(
        interpreter.get("executable_sha256"),
        "successor interpreter executable",
    )
    _require_sha256(
        interpreter.get("base_executable_sha256"),
        "successor interpreter base executable",
    )
    return {
        "native_build_receipt_sha256": native["file_sha256"],
        "native_build_receipt_canonical_sha256": native["canonical_sha256"],
        "native_module_sha256": native["module_sha256"],
        "native_wheel_sha256": native["wheel_sha256"],
        "native_soabi": native["soabi"],
        "runtime_lock_file_sha256": native["runtime_lock_file_sha256"],
        "runtime_lock_path": native["runtime_lock_path"],
        "runtime_lock_canonical_sha256": native[
            "runtime_lock_canonical_sha256"
        ],
        "wheelhouse_manifest_file_sha256": native[
            "wheelhouse_manifest_file_sha256"
        ],
        "wheelhouse_path": native["wheelhouse_path"],
        "wheelhouse_canonical_sha256": native["wheelhouse_canonical_sha256"],
        "install_receipt_path": native["install_receipt_path"],
        "install_receipt_file_sha256": native["install_receipt_file_sha256"],
        "install_receipt_canonical_sha256": native[
            "install_receipt_canonical_sha256"
        ],
        "root_wheel_sha256": native["root_wheel_sha256"],
        "root_wheel_path": native["root_wheel_path"],
        "native_wheel_path": native["native_wheel_path"],
        "installed_record_aggregate_sha256": native[
            "installed_record_aggregate_sha256"
        ],
        "locked_runtime_interpreter": dict(interpreter),
    }


def _expected_active_release_identity(
    raw: Mapping[str, Any] | None,
    *,
    expected_execution_commit: str,
    expected_execution_tree: str,
) -> dict[str, str]:
    if raw is None:
        return _empty_active_release_identity()
    schema_version = str(raw.get("schema_version", ""))
    config_bound = schema_version in {
        buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
        buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
    }
    expected_fields = (
        _SUCCESSOR_ACTIVE_RELEASE_PHASE_BINDING_FIELDS
        if schema_version == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
        else _ACTIVE_RELEASE_PHASE_BINDING_FIELDS
        if config_bound
        else _LEGACY_ACTIVE_RELEASE_PHASE_BINDING_FIELDS
    )
    if set(raw) != expected_fields:
        raise BuyE3TransactionalDeployError("active release phase binding fields drifted")
    identity = {
        "path": str(raw["remote_path"]),
        "file_sha256": _require_sha256(raw["file_sha256"], "active release file hash"),
        "file_canonical_sha256": _require_sha256(
            raw["canonical_active_release_sha256"],
            "active release canonical hash",
        ),
        "execution_commit": str(expected_execution_commit),
        "execution_tree": str(expected_execution_tree),
        "active_config_file_sha256": "",
        "disabled_config_file_sha256": "",
    }
    if config_bound:
        identity["active_config_file_sha256"] = _require_sha256(
            raw.get("active_config_file_sha256"),
            "active release embedded active config hash",
        )
        identity["disabled_config_file_sha256"] = _require_sha256(
            raw.get("disabled_config_file_sha256"),
            "active release embedded disabled config hash",
        )
    if schema_version == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA:
        for field in (
            "native_build_receipt_sha256",
            "native_build_receipt_canonical_sha256",
            "native_module_sha256",
            "native_wheel_sha256",
            "runtime_lock_file_sha256",
            "runtime_lock_canonical_sha256",
            "wheelhouse_manifest_file_sha256",
            "wheelhouse_canonical_sha256",
            "install_receipt_file_sha256",
            "install_receipt_canonical_sha256",
            "root_wheel_sha256",
            "installed_record_aggregate_sha256",
        ):
            identity[field] = _require_sha256(
                raw.get(field), f"active release {field}"
            )
        interpreter = raw.get("locked_runtime_interpreter")
        if (
            not str(raw.get("native_soabi", "")).startswith("cpython-312-")
            or any(
                not PurePosixPath(str(raw.get(field, ""))).is_absolute()
                for field in (
                    "runtime_lock_path",
                    "wheelhouse_path",
                    "install_receipt_path",
                    "root_wheel_path",
                    "native_wheel_path",
                )
            )
            or not isinstance(interpreter, Mapping)
            or set(interpreter) != _LOCKED_RUNTIME_INTERPRETER_FIELDS
            or interpreter.get("implementation") != "cpython"
            or interpreter.get("version_info", [])[:2] != [3, 12]
            or interpreter.get("is_virtual_environment") is not True
            or interpreter.get("soabi") != raw.get("native_soabi")
        ):
            raise BuyE3TransactionalDeployError("active release native SOABI drifted")
        identity["native_soabi"] = str(raw["native_soabi"])
        for field in (
            "runtime_lock_path",
            "wheelhouse_path",
            "install_receipt_path",
            "root_wheel_path",
            "native_wheel_path",
        ):
            identity[field] = str(raw[field])
        identity["locked_runtime_interpreter"] = dict(interpreter)
    return identity


def _validate_restore_contract(
    state: Mapping[str, Any],
    *,
    expected_enabled: bool,
    expected_artifact_sha256: str,
) -> tuple[str, str, int, bool]:
    restore_mode = str(state.get("restore_mode", ""))
    checkpoint_loaded = state.get("checkpoint_loaded")
    checkpoint_sequence = state.get("checkpoint_sequence")
    buy_identity = str(state.get("buy_deadline_identity", ""))
    remaining = state.get("buy_remaining_ms")
    if not isinstance(remaining, int) or isinstance(remaining, bool) or remaining < 0:
        raise BuyE3TransactionalDeployError("runtime BUY cooldown remaining time is malformed")
    checkpoint_valid = (
        restore_mode == "fresh_b0_no_checkpoint"
        and checkpoint_loaded is False
        and type(checkpoint_sequence) is int
        and checkpoint_sequence == 0
    ) or (
        restore_mode != "fresh_b0_no_checkpoint"
        and checkpoint_loaded is True
        and type(checkpoint_sequence) is int
        and checkpoint_sequence > 0
    )
    enabled_modes = {
        "fresh_b0_no_checkpoint",
        "expired_to_b0",
        "b0_checkpoint_resume",
        "exact_same_artifact_resume",
        "artifact_identity_changed_to_b0",
    }
    disabled_modes = {
        "fresh_b0_no_checkpoint",
        "expired_to_b0",
        "b0_checkpoint_resume",
        "rollback_to_b0",
    }
    admitted = enabled_modes if expected_enabled else disabled_modes
    if restore_mode not in admitted or not checkpoint_valid:
        raise BuyE3TransactionalDeployError("runtime cooldown restore mode is not admitted")
    if restore_mode == "exact_same_artifact_resume":
        deadline_valid = (
            remaining > 0
            and buy_identity
            == f"BUY_E3:{_require_sha256(expected_artifact_sha256, 'expected artifact hash')}"
        )
        imported_e3 = True
    elif restore_mode in {
        "artifact_identity_changed_to_b0",
        "rollback_to_b0",
        "b0_checkpoint_resume",
    }:
        deadline_valid = buy_identity == "B0"
        imported_e3 = False
    else:
        deadline_valid = buy_identity == "B0" and remaining == 0
        imported_e3 = False
    if not deadline_valid:
        raise BuyE3TransactionalDeployError(
            "runtime startup attestation is rejected or deadline-unsafe"
        )
    return restore_mode, buy_identity, remaining, imported_e3


def _validate_startup_attestation(
    raw: Any,
    *,
    expected_schema_version: str,
    expected_execution_commit: str,
    expected_execution_tree: str,
    expected_artifact_sha256: str,
    expected_runtime_sources: Mapping[str, Any],
    expected_repository_root: str,
    expected_python_executable: str,
    expected_python_binary_resolved: str,
    expected_config_sha256: str,
    expected_enabled: bool,
    expected_active_release: Mapping[str, Any] | None,
    expected_safety_release: Mapping[str, Any] | None = None,
    allow_legacy: bool = False,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise BuyE3TransactionalDeployError("runtime startup attestation fields drifted")
    attestation = dict(raw)
    schema_version = attestation.get("schema_version")
    if schema_version != expected_schema_version:
        raise BuyE3TransactionalDeployError(
            "runtime startup attestation schema differs from the frozen probe target"
        )
    legacy_v3 = schema_version == LEGACY_STARTUP_ATTESTATION_SCHEMA
    historical_v4 = schema_version == HISTORICAL_STARTUP_ATTESTATION_SCHEMA
    predecessor_v5 = schema_version == STARTUP_ATTESTATION_SCHEMA
    successor_v6 = schema_version == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
    historical = legacy_v3 or historical_v4
    if legacy_v3:
        expected_fields = _LEGACY_STARTUP_ATTESTATION_FIELDS
        expected_gate_fields = _LEGACY_STARTUP_GATE_FIELDS
    elif historical_v4:
        expected_fields = _HISTORICAL_STARTUP_ATTESTATION_FIELDS
        expected_gate_fields = _HISTORICAL_STARTUP_GATE_FIELDS
    elif predecessor_v5:
        expected_fields = _PREDECESSOR_STARTUP_ATTESTATION_FIELDS
        expected_gate_fields = _PREDECESSOR_STARTUP_GATE_FIELDS
    elif successor_v6:
        expected_fields = _SUCCESSOR_STARTUP_ATTESTATION_FIELDS
        expected_gate_fields = _SUCCESSOR_STARTUP_GATE_FIELDS
    else:
        raise BuyE3TransactionalDeployError(
            "runtime startup attestation schema is unsupported"
        )
    if set(attestation) != expected_fields or (historical and not allow_legacy):
        raise BuyE3TransactionalDeployError("runtime startup attestation fields drifted")
    gates = attestation.get("gates")
    state = attestation.get("fill_cooldown_state")
    checkout = attestation.get("running_checkout")
    expected_gates = {name: True for name in expected_gate_fields}
    if (
        schema_version
        != (
            LEGACY_STARTUP_ATTESTATION_SCHEMA
            if legacy_v3
            else HISTORICAL_STARTUP_ATTESTATION_SCHEMA
            if historical_v4
            else STARTUP_ATTESTATION_SCHEMA
            if predecessor_v5
            else SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
        )
        or attestation.get("status") != "accepted"
        or not str(attestation.get("attested_at_utc", "")).strip()
        or attestation.get("errors") != []
        or not isinstance(gates, Mapping)
        or set(gates) != expected_gate_fields
        or dict(gates) != expected_gates
        or not isinstance(state, Mapping)
        or state.get("schema_version") != FILL_COOLDOWN_STATE_SCHEMA
        or not isinstance(checkout, Mapping)
        or set(checkout) != _RUNNING_CHECKOUT_FIELDS
    ):
        raise BuyE3TransactionalDeployError(
            "runtime startup attestation is rejected or deadline-unsafe"
        )
    shadow_runtime = attestation.get("shadow_runtime_identity")
    if not historical:
        if (
            not isinstance(shadow_runtime, Mapping)
            or set(shadow_runtime) != _SHADOW_RUNTIME_IDENTITY_FIELDS
            or shadow_runtime.get("schema_version")
            != "narrowgate_shadow_runtime_identity.v1"
            or shadow_runtime.get("global_flow_shadow_enabled") is not False
            or shadow_runtime.get("global_reference_shadow_enabled") is not False
            or type(shadow_runtime.get("global_flow_native_requested")) is not bool
            or shadow_runtime.get("global_flow_native_effective") is not False
            or type(
                shadow_runtime.get("global_reference_bridge_basis_sample_count")
            ) is not int
            or shadow_runtime.get("global_reference_bridge_basis_sample_count") != 0
            or shadow_runtime.get("state_restore_contract")
            != "shadow_state_never_restored"
            or shadow_runtime.get("global_flow_shadow_config_explicit") is not True
            or shadow_runtime.get("global_reference_shadow_config_explicit") is not True
        ):
            raise BuyE3TransactionalDeployError(
                "runtime no-shadow startup identity drifted"
            )
        backend = shadow_runtime.get("global_flow_backend")
        if (
            not isinstance(backend, Mapping)
            or set(backend) != _GLOBAL_FLOW_BACKEND_FIELDS
            or any(type(value) is not int or value != 0 for value in backend.values())
        ):
            raise BuyE3TransactionalDeployError(
                "runtime global-flow backend is not absolute zero"
            )
    if legacy_v3:
        restore_mode = state.get("restore_mode")
        checkpoint_loaded = state.get("checkpoint_loaded")
        checkpoint_sequence = state.get("checkpoint_sequence")
        if (
            expected_active_release is not None
            or restore_mode not in {"fresh_b0_no_checkpoint", "expired_to_b0"}
            or not (
                (
                    restore_mode == "fresh_b0_no_checkpoint"
                    and checkpoint_loaded is False
                    and type(checkpoint_sequence) is int
                    and checkpoint_sequence == 0
                )
                or (
                    restore_mode == "expired_to_b0"
                    and checkpoint_loaded is True
                    and type(checkpoint_sequence) is int
                    and checkpoint_sequence > 0
                )
            )
            or state.get("buy_deadline_identity") != "B0"
            or state.get("buy_remaining_ms") != 0
        ):
            raise BuyE3TransactionalDeployError(
                "legacy runtime startup attestation is deadline-unsafe"
            )
    else:
        _validate_restore_contract(
            state,
            expected_enabled=expected_enabled,
            expected_artifact_sha256=expected_artifact_sha256,
        )
        release = attestation.get("buy_e3_active_release")
        expected_release_fields = (
            _HISTORICAL_STARTUP_ACTIVE_RELEASE_FIELDS
            if historical_v4
            else _STARTUP_ACTIVE_RELEASE_FIELDS
        )
        if not isinstance(release, Mapping) or set(release) != expected_release_fields:
            raise BuyE3TransactionalDeployError("runtime active release identity fields drifted")
        expected_release = _expected_active_release_identity(
            expected_active_release,
            expected_execution_commit=expected_execution_commit,
            expected_execution_tree=expected_execution_tree,
        )
        if historical_v4:
            expected_release = {
                field: value
                for field, value in expected_release.items()
                if field in _HISTORICAL_STARTUP_ACTIVE_RELEASE_FIELDS
            }
        if expected_enabled:
            if expected_active_release is None:
                raise BuyE3TransactionalDeployError(
                    "enabled runtime startup lacks post-envelope active release authority"
                )
            for field, value in expected_release.items():
                if field not in expected_release_fields:
                    continue
                if release.get(field) != value:
                    raise BuyE3TransactionalDeployError("runtime active release identity drifted")
            if (
                not historical_v4
                and release.get("active_config_file_sha256")
                != expected_config_sha256
            ):
                raise BuyE3TransactionalDeployError(
                    "runtime active release does not bind the running config"
                )
            if (
                not str(release.get("annotated_operational_tag", "")).strip()
                or len(str(release.get("annotated_operational_tag_object", ""))) != 40
            ):
                raise BuyE3TransactionalDeployError(
                    "runtime active release operational tag identity is incomplete"
                )
        else:
            empty_release = {
                field: ""
                for field in (
                    _HISTORICAL_STARTUP_ACTIVE_RELEASE_FIELDS
                    if historical_v4
                    else _STARTUP_ACTIVE_RELEASE_FIELDS
                )
            }
            if dict(release) != empty_release:
                raise BuyE3TransactionalDeployError(
                    "disabled runtime retained active release identity"
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
            raise BuyE3TransactionalDeployError(f"runtime startup {label} drifted")
    stable_snapshot = checkout.get("stable_snapshot")
    if (
        not isinstance(stable_snapshot, Mapping)
        or set(stable_snapshot) != _STABLE_GIT_SNAPSHOT_FIELDS
        or any(value is not True for value in stable_snapshot.values())
    ):
        raise BuyE3TransactionalDeployError("runtime startup stable Git snapshot drifted")
    source_rows = checkout.get("runtime_source_files")
    if not isinstance(source_rows, list) or not source_rows:
        raise BuyE3TransactionalDeployError("runtime startup source manifest is empty")
    normalized_rows: list[dict[str, Any]] = []
    observed: dict[str, str] = {}
    for raw_row in source_rows:
        if not isinstance(raw_row, Mapping) or set(raw_row) != _RUNTIME_SOURCE_FILE_FIELDS:
            raise BuyE3TransactionalDeployError("runtime startup source file binding drifted")
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
            raise BuyE3TransactionalDeployError("runtime startup source file binding is malformed")
        working_sha256 = _require_sha256(
            row.get("working_file_sha256"),
            f"runtime startup working source {path}",
        )
        head_sha256 = _require_sha256(
            row.get("head_blob_sha256"),
            f"runtime startup HEAD source {path}",
        )
        if working_sha256 != head_sha256:
            raise BuyE3TransactionalDeployError("runtime startup source differs from its HEAD blob")
        observed[path] = working_sha256
        normalized_rows.append(row)
    if [row["path"] for row in normalized_rows] != sorted(observed):
        raise BuyE3TransactionalDeployError("runtime startup source manifest is not ordered")
    if checkout.get("runtime_source_file_count") != len(normalized_rows) or checkout.get(
        "runtime_source_manifest_sha256"
    ) != _runtime_source_manifest_sha256(normalized_rows):
        raise BuyE3TransactionalDeployError("runtime startup source manifest aggregate drifted")
    expected_loaded_modules = (
        _HISTORICAL_LOADED_RUNTIME_MODULE_IDENTITIES
        if historical
        else _LOADED_RUNTIME_MODULE_IDENTITIES
        if predecessor_v5
        else _SUCCESSOR_LOADED_RUNTIME_MODULE_IDENTITIES
    )
    expected_loaded_paths = {
        relative for _module, relative in expected_loaded_modules.values()
    }
    if not expected_loaded_paths.issubset(observed) or (
        not successor_v6
        and set(observed) != expected_loaded_paths
    ):
        raise BuyE3TransactionalDeployError(
            "runtime startup source manifest path set drifted"
        )
    expected_sources = _validated_expected_runtime_source_hashes(expected_runtime_sources)
    if historical:
        historical_required_paths = set(gate_v2.REQUIRED_RUNTIME_PATHS.values())
        if not historical_required_paths.issubset(expected_sources):
            raise BuyE3TransactionalDeployError(
                "historical runtime source authority is incomplete"
            )
        expected_sources = {
            path: sha256
            for path, sha256 in expected_sources.items()
            if path in expected_loaded_paths
        }
    elif predecessor_v5 and set(expected_sources) != expected_loaded_paths:
        raise BuyE3TransactionalDeployError(
            "predecessor runtime source authority does not cover every loaded module"
        )
    elif successor_v6 and not set(expected_sources).issubset(observed):
        raise BuyE3TransactionalDeployError(
            "current runtime source authority does not cover every loaded module"
        )
    if any(observed.get(path) != sha256 for path, sha256 in expected_sources.items()):
        raise BuyE3TransactionalDeployError(
            "runtime startup source bytes differ from the frozen plan"
        )
    loaded_origins = attestation.get("loaded_module_origins")
    if (
        not isinstance(loaded_origins, Mapping)
        or set(loaded_origins) != set(expected_loaded_modules)
    ):
        raise BuyE3TransactionalDeployError("runtime loaded module origin set drifted")
    for role, raw_module in loaded_origins.items():
        if not isinstance(raw_module, Mapping) or set(raw_module) != _LOADED_RUNTIME_MODULE_FIELDS:
            raise BuyE3TransactionalDeployError(
                f"runtime loaded module origin fields drifted: {role}"
            )
        relative_path = str(raw_module.get("repository_relative_path", "")).strip()
        origin_path = str(raw_module.get("origin_path", "")).strip()
        module_name = str(raw_module.get("module_name", "")).strip()
        expected_module_name, expected_relative_path = expected_loaded_modules[role]
        expected_origin_path = str(
            PurePosixPath(expected_repository_root) / expected_relative_path
        )
        if (
            module_name != expected_module_name
            or relative_path != expected_relative_path
            or origin_path != expected_origin_path
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
    if successor_v6:
        closure = attestation.get("loaded_repository_module_closure")
        if not isinstance(closure, list) or not closure:
            raise BuyE3TransactionalDeployError("runtime repository module closure is empty")
        closure_pairs: list[tuple[str, str]] = []
        for raw_module in closure:
            if not isinstance(raw_module, Mapping) or set(raw_module) != _LOADED_RUNTIME_MODULE_FIELDS:
                raise BuyE3TransactionalDeployError(
                    "runtime repository module closure fields drifted"
                )
            path = str(raw_module.get("repository_relative_path", ""))
            pair = (str(raw_module.get("module_name", "")), path)
            if pair in closure_pairs or observed.get(path) != raw_module.get("source_sha256"):
                raise BuyE3TransactionalDeployError(
                    "runtime repository module closure is not source-bound"
                )
            expected_origin = str(PurePosixPath(expected_repository_root) / path)
            if raw_module.get("origin_path") != expected_origin:
                raise BuyE3TransactionalDeployError(
                    "runtime repository module closure escaped the checkout"
                )
            closure_pairs.append(pair)
        if closure_pairs != sorted(closure_pairs) or not expected_loaded_paths.issubset(
            {path for _module, path in closure_pairs}
        ):
            raise BuyE3TransactionalDeployError(
                "runtime repository module closure is incomplete or unordered"
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
        raise BuyE3TransactionalDeployError("runtime interpreter bytes changed during attestation")
    native = attestation.get("native_runtime_identity")
    if (
        not isinstance(native, Mapping)
        or set(native)
        != (
            _SUCCESSOR_NATIVE_RUNTIME_IDENTITY_FIELDS
            if successor_v6
            else _NATIVE_RUNTIME_IDENTITY_FIELDS
        )
        or native.get("schema_version") != NATIVE_RUNTIME_IDENTITY_SCHEMA
        or not str(native.get("platform", "")).strip()
        or native.get("stable") is not True
        or not isinstance(native.get("enabled"), bool)
    ):
        raise BuyE3TransactionalDeployError("native runtime identity drifted")
    if successor_v6:
        abi = native.get("abi_contract")
        locked = native.get("locked_runtime")
        expected_venv = (
            str(
                PurePosixPath(
                    str(expected_safety_release.get("install_receipt_path", ""))
                ).parent
                / f"venv-{expected_execution_commit}"
            )
            if expected_safety_release is not None
            else ""
        )
        expected_selector = str(
            PurePosixPath(expected_repository_root) / ".venv-active"
        )
        if (
            not isinstance(abi, Mapping)
            or abi.get("schema_version") != "narrowgate_native_live_safety_abi.v1"
            or abi.get("validated") is not True
            or not isinstance(abi.get("required_apis"), list)
            or abi.get("required_quote_fields")
            != {
                "QuoteFlags": ["delta_cap", "final_compressed", "cap_exposure_block"],
                "SideQuoteContext": ["cap_exposure_block"],
            }
        ):
            raise BuyE3TransactionalDeployError("native runtime ABI contract drifted")
        if (
            expected_safety_release is None
            or not isinstance(locked, Mapping)
            or set(locked) != _LOCKED_RUNTIME_STARTUP_FIELDS
            or locked.get("validated") is not True
            or locked.get("venv_selector_path") != expected_selector
            or locked.get("venv_selector_target") != expected_venv
            or locked.get("venv_real_path") != expected_venv
            or locked.get("python_real_path")
            != str(PurePosixPath(expected_venv) / "bin/python3")
            or locked.get("install_receipt_path")
            != expected_safety_release.get("install_receipt_path")
            or locked.get("install_receipt_file_sha256")
            != expected_safety_release.get("install_receipt_file_sha256")
            or locked.get("install_receipt_canonical_sha256")
            != expected_safety_release.get("install_receipt_canonical_sha256")
            or locked.get("runtime_lock_canonical_sha256")
            != expected_safety_release.get("runtime_lock_canonical_sha256")
            or locked.get("wheelhouse_canonical_sha256")
            != expected_safety_release.get("wheelhouse_canonical_sha256")
            or locked.get("installed_record_aggregate_sha256")
            != expected_safety_release.get(
                "installed_record_aggregate_sha256"
            )
            or locked.get("interpreter")
            != expected_safety_release.get("locked_runtime_interpreter")
        ):
            raise BuyE3TransactionalDeployError(
                "locked successor runtime startup authority drifted"
            )
    if native["enabled"]:
        native_before = _validate_file_byte_identity(
            native.get("before"), label="native runtime before"
        )
        native_after = _validate_file_byte_identity(
            native.get("after"), label="native runtime after"
        )
        if (
            native_before != native_after
            or native.get("reported_module_path") != native_before["reported_path"]
            or native.get("loaded_module_origin_path") != native_before["resolved_path"]
        ):
            raise BuyE3TransactionalDeployError("native runtime module bytes or origin drifted")
        if (
            expected_safety_release is not None
            and expected_safety_release.get("schema_version")
            == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
            and native_before["sha256"]
            != _require_sha256(
                expected_safety_release.get("native_module_sha256"),
                "successor expected native module hash",
            )
        ):
            raise BuyE3TransactionalDeployError(
                "native runtime module differs from successor build receipt"
            )
    elif (
        native.get("reported_module_path") != "disabled"
        or native.get("loaded_module_origin_path") is not None
        or native.get("before") is not None
        or native.get("after") is not None
    ):
        raise BuyE3TransactionalDeployError("disabled native runtime identity is malformed")
    if successor_v6:
        safety = attestation.get("live_safety_successor")
        expected_safety_identity = None
        if expected_safety_release is not None:
            expected_safety_identity = {
                "path": expected_safety_release["remote_path"],
                "file_sha256": expected_safety_release["file_sha256"],
                "canonical_sha256": expected_safety_release[
                    "canonical_active_release_sha256"
                ],
                "execution_commit": expected_execution_commit,
                "execution_tree": expected_execution_tree,
                "active_config_file_sha256": expected_safety_release[
                    "active_config_file_sha256"
                ],
                "disabled_config_file_sha256": expected_safety_release[
                    "disabled_config_file_sha256"
                ],
                **{
                    field: expected_safety_release[field]
                    for field in _SUCCESSOR_ACTIVE_RELEASE_PHASE_BINDING_FIELDS
                    - _ACTIVE_RELEASE_PHASE_BINDING_FIELDS
                },
            }
        if (
            expected_safety_release is None
            or expected_safety_release.get("schema_version")
            != buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
            or not isinstance(safety, Mapping)
            or set(safety) != _LIVE_SAFETY_SUCCESSOR_IDENTITY_FIELDS
            or safety != expected_safety_identity
        ):
            raise BuyE3TransactionalDeployError(
                "runtime live safety successor identity drifted"
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
    expected_repository_root: str,
    expected_startup_attestation_schema_version: str,
    expected_active_release: Mapping[str, Any] | None,
    expected_safety_release: Mapping[str, Any] | None = None,
    expected_exchange_reconciliation_path: str | None = None,
    allow_legacy_startup: bool = False,
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
        or runtime.get("f05_buy_e3_owner_override_effective") is not bool(expected_enabled)
        or (
            expected_artifact_sha256
            and runtime.get("f05_buy_e3_artifact_sha256") != expected_artifact_sha256
        )
        or not str(runtime.get("recorded_at_utc", "")).strip()
    ):
        raise BuyE3TransactionalDeployError(
            "runtime identity process/config/artifact authority drifted"
        )
    successor_runtime = (
        expected_startup_attestation_schema_version
        == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
    )
    exchange_binding = runtime.get("startup_exchange_reconciliation")
    if successor_runtime:
        if (
            not isinstance(exchange_binding, Mapping)
            or set(exchange_binding)
            != _STARTUP_EXCHANGE_RECONCILIATION_BINDING_FIELDS
        ):
            raise BuyE3TransactionalDeployError(
                "runtime startup exchange reconciliation binding drifted"
            )
        exchange_path = Path(str(exchange_binding.get("path", ""))).expanduser()
        try:
            exchange_raw = exchange_path.read_bytes()
            exchange_payload = json.loads(exchange_raw)
        except (OSError, json.JSONDecodeError) as exc:
            raise BuyE3TransactionalDeployError(
                "runtime startup exchange reconciliation receipt is unavailable"
            ) from exc
        exchange_canonical = dict(exchange_payload)
        observed_canonical = exchange_canonical.pop(
            "canonical_exchange_reconciliation_sha256", None
        )
        if (
            not exchange_path.is_absolute()
            or exchange_path.is_symlink()
            or not exchange_path.is_file()
            or exchange_path.stat().st_nlink != 1
            or stat.S_IMODE(exchange_path.stat().st_mode) != 0o600
            or (
                expected_exchange_reconciliation_path is not None
                and str(exchange_path.resolve(strict=True))
                != expected_exchange_reconciliation_path
            )
            or hashlib.sha256(exchange_raw).hexdigest()
            != exchange_binding.get("file_sha256")
            or observed_canonical != exchange_binding.get("canonical_sha256")
            or gate_v2.canonical_sha256(exchange_canonical) != observed_canonical
            or exchange_payload.get("account_key_sha256")
            != exchange_binding.get("account_key_sha256")
            or exchange_payload.get("position_lineage_sha256")
            != exchange_binding.get("position_lineage_sha256")
        ):
            raise BuyE3TransactionalDeployError(
                "runtime startup exchange reconciliation receipt drifted"
            )
        for field in (
            "file_sha256",
            "canonical_sha256",
            "account_key_sha256",
            "position_lineage_sha256",
        ):
            _require_sha256(exchange_binding.get(field), f"startup exchange {field}")
    elif exchange_binding is not None:
        raise BuyE3TransactionalDeployError(
            "predecessor runtime carries successor exchange authority"
        )
    expected_release = _expected_active_release_identity(
        expected_active_release,
        expected_execution_commit=expected_execution_commit,
        expected_execution_tree=expected_execution_tree,
    )
    if expected_enabled:
        if expected_active_release is None or (
            runtime.get("f05_buy_e3_active_release_authority_schema_version")
            != ACTIVE_RELEASE_RUNTIME_AUTHORITY_SCHEMA
            or runtime.get("f05_buy_e3_required") is not True
            or runtime.get("f05_buy_e3_active_release_path") != expected_release["path"]
            or runtime.get("f05_buy_e3_active_release_file_sha256")
            != expected_release["file_sha256"]
            or runtime.get("f05_buy_e3_active_release_canonical_sha256")
            != expected_release["file_canonical_sha256"]
        ):
            raise BuyE3TransactionalDeployError("runtime identity active release authority drifted")
    else:
        startup_raw = runtime.get("startup_attestation")
        legacy_startup = (
            isinstance(startup_raw, Mapping)
            and startup_raw.get("schema_version") == LEGACY_STARTUP_ATTESTATION_SCHEMA
        )
        if legacy_startup and allow_legacy_startup:
            invalid_disabled_release = any(
                (
                    runtime.get("f05_buy_e3_active_release_authority_schema_version")
                    not in {None, ACTIVE_RELEASE_RUNTIME_AUTHORITY_SCHEMA},
                    runtime.get("f05_buy_e3_required") not in {None, False},
                    runtime.get("f05_buy_e3_active_release_path") not in {None, ""},
                    runtime.get("f05_buy_e3_active_release_file_sha256") not in {None, ""},
                    runtime.get("f05_buy_e3_active_release_canonical_sha256") not in {None, ""},
                )
            )
        else:
            invalid_disabled_release = (
                runtime.get("f05_buy_e3_active_release_authority_schema_version")
                != ACTIVE_RELEASE_RUNTIME_AUTHORITY_SCHEMA
                or runtime.get("f05_buy_e3_required") is not False
                or runtime.get("f05_buy_e3_active_release_path") != ""
                or runtime.get("f05_buy_e3_active_release_file_sha256") != ""
                or runtime.get("f05_buy_e3_active_release_canonical_sha256") != ""
            )
        if invalid_disabled_release:
            raise BuyE3TransactionalDeployError(
                "disabled runtime retained active release authority"
            )
    attestation = _validate_startup_attestation(
        runtime.get("startup_attestation"),
        expected_schema_version=expected_startup_attestation_schema_version,
        expected_execution_commit=expected_execution_commit,
        expected_execution_tree=expected_execution_tree,
        expected_artifact_sha256=expected_artifact_sha256,
        expected_runtime_sources=expected_runtime_sources,
        expected_repository_root=expected_repository_root,
        expected_python_executable=expected_python_executable,
        expected_python_binary_resolved=expected_python_binary_resolved,
        expected_config_sha256=expected_config_sha256,
        expected_enabled=expected_enabled,
        expected_active_release=expected_active_release,
        expected_safety_release=expected_safety_release,
        allow_legacy=allow_legacy_startup,
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
    historical_startup = attestation.get("schema_version") in {
        LEGACY_STARTUP_ATTESTATION_SCHEMA,
        HISTORICAL_STARTUP_ATTESTATION_SCHEMA,
    }
    native_flow_requested_field = "NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED"
    native_flow_effective_field = "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE"
    expected_native_fields = {"profile", "module", *native_flag_names}
    if not historical_startup:
        expected_native_fields.update(
            {native_flow_requested_field, native_flow_effective_field}
        )
    if attestation.get("schema_version") == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA:
        expected_native_fields.update({"abi_contract", "locked_runtime"})
    shadow_runtime = attestation.get("shadow_runtime_identity", {})
    if (
        not isinstance(native_runtime, Mapping)
        or set(native_runtime) != expected_native_fields
        or any(type(native_runtime.get(name)) is not bool for name in native_flag_names)
        or (
            not historical_startup
            and (
                native_runtime.get(native_flow_requested_field)
                is not native_runtime.get("NARROWGATE_CPP_GLOBAL_FLOW")
                or native_runtime.get(native_flow_effective_field) is not False
                or native_runtime.get(native_flow_requested_field)
                is not shadow_runtime.get("global_flow_native_requested")
                or native_runtime.get(native_flow_effective_field)
                is not shadow_runtime.get("global_flow_native_effective")
            )
        )
        or native_identity.get("profile") != native_runtime.get("profile")
        or native_identity.get("reported_module_path") != native_runtime.get("module")
        or (
            attestation.get("schema_version") == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
            and native_identity.get("abi_contract") != native_runtime.get("abi_contract")
        )
        or (
            attestation.get("schema_version") == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
            and native_identity.get("locked_runtime")
            != native_runtime.get("locked_runtime")
        )
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
    expected_artifact_manifest_file_sha256: str,
    expected_policy_file_sha256: str,
    expected_predicate_bundle_file_sha256: str,
    expected_runtime_code_sha256: str,
    runtime_source_authority_base64: str,
    expected_startup_attestation_schema_version: str,
    artifact_manifest_path: Path | None,
    policy_path: Path | None = None,
    predicate_bundle_path: Path | None = None,
    active_release_path: Path | None = None,
    expected_active_release_file_sha256: str = "",
    expected_active_release_canonical_sha256: str = "",
    safety_release_path: Path | None = None,
    expected_safety_release_file_sha256: str = "",
    expected_safety_release_canonical_sha256: str = "",
    expected_safety_active_config_file_sha256: str = "",
    expected_safety_disabled_config_file_sha256: str = "",
    expected_exchange_reconciliation_path: Path | None = None,
) -> dict[str, Any]:
    if expected_startup_attestation_schema_version not in {
        LEGACY_STARTUP_ATTESTATION_SCHEMA,
        HISTORICAL_STARTUP_ATTESTATION_SCHEMA,
        STARTUP_ATTESTATION_SCHEMA,
        SUCCESSOR_STARTUP_ATTESTATION_SCHEMA,
    }:
        raise BuyE3TransactionalDeployError(
            "expected startup attestation schema is not an admitted frozen writer"
        )
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
        raise BuyE3TransactionalDeployError("runtime identity is not a non-symlink regular file")
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
    artifact_paths = (artifact_manifest_path, policy_path, predicate_bundle_path)
    any_artifact_path = any(path is not None for path in artifact_paths)
    all_artifact_paths = all(path is not None for path in artifact_paths)
    if bool(artifact_sha) is not any_artifact_path or (
        any_artifact_path and not all_artifact_paths
    ):
        raise BuyE3TransactionalDeployError(
            "artifact hash and all three artifact paths must be supplied together"
        )
    artifact_file_hashes = {
        "manifest": str(expected_artifact_manifest_file_sha256).strip(),
        "policy": str(expected_policy_file_sha256).strip(),
        "predicate_bundle": str(expected_predicate_bundle_file_sha256).strip(),
    }
    if artifact_sha:
        artifact_file_hashes = {
            role: _require_sha256(value, f"expected {role} file hash")
            for role, value in artifact_file_hashes.items()
        }
    elif any(artifact_file_hashes.values()):
        raise BuyE3TransactionalDeployError(
            "artifact file hashes require an expected artifact"
        )
    runtime_code_sha = _require_sha256(expected_runtime_code_sha256, "expected runtime code hash")
    if artifact_sha and runtime.get("f05_buy_e3_artifact_sha256") != artifact_sha:
        raise BuyE3TransactionalDeployError("actual process artifact identity drifted")
    active_release_binding: dict[str, Any] | None = None
    supplied_release = any(
        (
            active_release_path is not None,
            bool(str(expected_active_release_file_sha256).strip()),
            bool(str(expected_active_release_canonical_sha256).strip()),
        )
    )
    if supplied_release:
        if (
            not expected_buy_e3_enabled
            or active_release_path is None
            or not str(expected_active_release_file_sha256).strip()
            or not str(expected_active_release_canonical_sha256).strip()
        ):
            raise BuyE3TransactionalDeployError(
                "active release process-probe binding is incomplete"
            )
        installed_release = _validate_installed_active_release_file(
            active_release_path,
            expected_file_sha256=_require_sha256(
                expected_active_release_file_sha256,
                "expected active release file hash",
            ),
            expected_canonical_sha256=_require_sha256(
                expected_active_release_canonical_sha256,
                "expected active release canonical hash",
            ),
            expected_execution_commit=expected_execution_commit,
            expected_execution_tree=expected_execution_tree,
            expected_artifact_sha256=artifact_sha,
            expected_manifest_file_sha256=artifact_file_hashes["manifest"],
            expected_policy_file_sha256=artifact_file_hashes["policy"],
            expected_predicate_bundle_file_sha256=artifact_file_hashes[
                "predicate_bundle"
            ],
            expected_active_config_file_sha256=_require_sha256(
                config_sha256,
                "running active config hash",
            ),
        )
        schema_version = str(installed_release.get("schema_version", ""))
        _identity, release_status = _active_release_contract(schema_version)
        active_release_binding = {
            "local_path": "not_transferred_to_probe_host",
            "remote_path": str(active_release_path.expanduser().absolute()),
            "file_sha256": _require_sha256(
                expected_active_release_file_sha256,
                "expected active release file hash",
            ),
            "canonical_active_release_sha256": _require_sha256(
                expected_active_release_canonical_sha256,
                "expected active release canonical hash",
            ),
            "schema_version": schema_version,
            "status": release_status,
        }
        if schema_version in {
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
            buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
        }:
            config_pair = installed_release["config_pair"]
            active_release_binding.update(
                {
                    "active_config_file_sha256": str(
                        config_pair["active"]["file_sha256"]
                    ),
                    "disabled_config_file_sha256": str(
                        config_pair["disabled"]["file_sha256"]
                    ),
                }
            )
        if schema_version == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA:
            native_build = installed_release["native_build"]
            active_release_binding.update(
                _successor_native_build_binding(native_build)
            )
    elif expected_buy_e3_enabled:
        raise BuyE3TransactionalDeployError(
            "enabled process probe requires post-envelope active release authority"
        )
    safety_release_binding: dict[str, Any] | None = None
    supplied_safety_release = any(
        (
            safety_release_path is not None,
            bool(str(expected_safety_release_file_sha256).strip()),
            bool(str(expected_safety_release_canonical_sha256).strip()),
            bool(str(expected_safety_active_config_file_sha256).strip()),
            bool(str(expected_safety_disabled_config_file_sha256).strip()),
        )
    )
    if supplied_safety_release:
        if (
            safety_release_path is None
            or not all(
                str(value).strip()
                for value in (
                    expected_safety_release_file_sha256,
                    expected_safety_release_canonical_sha256,
                    expected_safety_active_config_file_sha256,
                    expected_safety_disabled_config_file_sha256,
                )
            )
        ):
            raise BuyE3TransactionalDeployError(
                "successor safety release process-probe binding is incomplete"
            )
        installed_safety = _validate_installed_active_release_file(
            safety_release_path,
            expected_file_sha256=_require_sha256(
                expected_safety_release_file_sha256,
                "expected successor safety release file hash",
            ),
            expected_canonical_sha256=_require_sha256(
                expected_safety_release_canonical_sha256,
                "expected successor safety release canonical hash",
            ),
            expected_execution_commit=expected_execution_commit,
            expected_execution_tree=expected_execution_tree,
            expected_artifact_sha256=artifact_sha,
            expected_manifest_file_sha256=artifact_file_hashes["manifest"],
            expected_policy_file_sha256=artifact_file_hashes["policy"],
            expected_predicate_bundle_file_sha256=artifact_file_hashes[
                "predicate_bundle"
            ],
            expected_active_config_file_sha256=_require_sha256(
                expected_safety_active_config_file_sha256,
                "expected successor active config hash",
            ),
            expected_disabled_config_file_sha256=_require_sha256(
                expected_safety_disabled_config_file_sha256,
                "expected successor disabled config hash",
            ),
        )
        if (
            installed_safety.get("schema_version")
            != buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
        ):
            raise BuyE3TransactionalDeployError(
                "safety release process probe requires successor schema"
            )
        config_pair = installed_safety["config_pair"]
        native_build = installed_safety["native_build"]
        safety_release_binding = {
            "local_path": "not_transferred_to_probe_host",
            "remote_path": str(safety_release_path.expanduser().absolute()),
            "file_sha256": _require_sha256(
                expected_safety_release_file_sha256,
                "expected successor safety release file hash",
            ),
            "canonical_active_release_sha256": _require_sha256(
                expected_safety_release_canonical_sha256,
                "expected successor safety release canonical hash",
            ),
            "schema_version": buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
            "status": buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_STATUS,
            "active_config_file_sha256": str(config_pair["active"]["file_sha256"]),
            "disabled_config_file_sha256": str(config_pair["disabled"]["file_sha256"]),
        }
        safety_release_binding.update(_successor_native_build_binding(native_build))
    if (
        expected_startup_attestation_schema_version
        == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
        and safety_release_binding is None
    ):
        raise BuyE3TransactionalDeployError(
            "successor process probe requires always-on safety release authority"
        )
    if (
        expected_startup_attestation_schema_version
        != SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
        and safety_release_binding is not None
    ):
        raise BuyE3TransactionalDeployError(
            "predecessor process probe cannot carry successor safety authority"
        )
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
    actual_runtime_sources = _validate_checkout_runtime_source_authority(
        repository_root=repository_root,
        execution_commit=completed_commit,
        runtime_sources=_decode_runtime_source_authority(runtime_source_authority_base64),
        expected_runtime_code_sha256=runtime_code_sha,
        expected_startup_attestation_schema_version=(
            expected_startup_attestation_schema_version
        ),
    )
    if artifact_sha:
        exact_artifact_paths = {
            "manifest": artifact_manifest_path,
            "policy": policy_path,
            "predicate_bundle": predicate_bundle_path,
        }
        for role, raw_path in exact_artifact_paths.items():
            assert raw_path is not None
            candidate = raw_path.expanduser()
            if candidate.is_symlink() or not candidate.is_file():
                raise BuyE3TransactionalDeployError(
                    f"artifact role is not a non-symlink regular file: {role}"
                )
            if gate_v2.file_sha256(candidate.resolve(strict=True)) != artifact_file_hashes[role]:
                raise BuyE3TransactionalDeployError(f"artifact role bytes drifted: {role}")
        manifest = gate_v2.read_json(artifact_manifest_path.resolve(strict=True))
        if manifest.get("artifact_sha256") != artifact_sha:
            raise BuyE3TransactionalDeployError("artifact manifest identity drifted")
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
        expected_repository_root=str(repository_root.resolve(strict=True)),
        expected_startup_attestation_schema_version=(
            expected_startup_attestation_schema_version
        ),
        expected_active_release=active_release_binding,
        expected_safety_release=safety_release_binding,
        expected_exchange_reconciliation_path=(
            str(expected_exchange_reconciliation_path.expanduser().absolute())
            if expected_exchange_reconciliation_path is not None
            else None
        ),
        allow_legacy_startup=expected_startup_attestation_schema_version
        in {LEGACY_STARTUP_ATTESTATION_SCHEMA, HISTORICAL_STARTUP_ATTESTATION_SCHEMA},
    )
    startup_state = startup_attestation["fill_cooldown_state"]
    startup_release = startup_attestation.get("buy_e3_active_release")
    if not isinstance(startup_release, Mapping):
        startup_release = _empty_active_release_identity()
    restore_mode, _buy_identity, buy_remaining_ms, imported_e3 = _validate_restore_contract(
        startup_state,
        expected_enabled=expected_buy_e3_enabled,
        expected_artifact_sha256=artifact_sha,
    )
    process.update(
        {
            "execution_commit": completed_commit,
            "execution_tree": completed_tree,
            "runtime_identity_file_sha256": runtime_file_sha256,
            "startup_attestation_sha256": gate_v2.canonical_sha256(startup_attestation),
            "artifact_sha256": artifact_sha,
            "runtime_code_sha256": runtime_code_sha,
            "buy_e3_enabled": enabled,
            "owner_override_effective": effective,
            "initial_buy_deadline_identity": startup_state["buy_deadline_identity"],
            "fill_cooldown_restore_mode": restore_mode,
            "initial_buy_remaining_ms": buy_remaining_ms,
            "e3_deadline_imported": imported_e3,
            "active_release_path": str(startup_release.get("path", "")),
            "active_release_file_sha256": str(startup_release.get("file_sha256", "")),
            "active_release_canonical_sha256": str(
                startup_release.get("file_canonical_sha256", "")
            ),
            "active_release_execution_commit": str(startup_release.get("execution_commit", "")),
            "active_release_execution_tree": str(startup_release.get("execution_tree", "")),
        }
    )
    if (
        expected_startup_attestation_schema_version
        == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
    ):
        exchange = runtime.get("startup_exchange_reconciliation")
        if not isinstance(exchange, Mapping):
            raise BuyE3TransactionalDeployError(
                "successor process probe lacks exchange reconciliation binding"
            )
        process["startup_exchange_reconciliation"] = dict(exchange)
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )
    return process


def _validate_rollback_identity(name: str, raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise BuyE3TransactionalDeployError(f"rollback identity is malformed: {name}")
    if name == "deep_predecessor" and raw.get("mode") == "stop_cancel_reconcile_only":
        expected = {
            "identity",
            "mode",
            "historical_execution_commit",
            "historical_execution_tree",
            "automatic_historical_restart_authorized",
            "requires_manual_exchange_reconciliation",
        }
        if (
            set(raw) != expected
            or not str(raw.get("identity", "")).strip()
            or raw.get("automatic_historical_restart_authorized") is not False
            or raw.get("requires_manual_exchange_reconciliation") is not True
        ):
            raise BuyE3TransactionalDeployError(
                "deep predecessor must be an exact stop/cancel/reconcile-only identity"
            )
        normalized = dict(raw)
        normalized["historical_execution_commit"] = _require_git_sha(
            raw.get("historical_execution_commit"),
            "deep rollback historical commit",
        )
        normalized["historical_execution_tree"] = _require_git_sha(
            raw.get("historical_execution_tree"),
            "deep rollback historical tree",
        )
        return normalized
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


def _rollback_startup_attestation_schema(
    *,
    identity: Mapping[str, Any],
    current_execution: Mapping[str, Any],
) -> str:
    """Resolve a rollback writer schema from an exact frozen target identity."""

    if (
        identity.get("execution_commit") == current_execution.get("execution_commit")
        and identity.get("execution_tree") == current_execution.get("execution_tree")
    ):
        return (
            SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
            if _is_successor_execution(current_execution)
            else STARTUP_ATTESTATION_SCHEMA
        )
    if (
        identity.get("execution_commit") == FROZEN_07EF_EXECUTION_COMMIT
        and identity.get("execution_tree") == FROZEN_07EF_EXECUTION_TREE
        and identity.get("config_sha256") == FROZEN_07EF_DISABLED_CONFIG_SHA256
        and identity.get("buy_e3_enabled") is False
        and not str(identity.get("artifact_sha256", "")).strip()
    ):
        return HISTORICAL_STARTUP_ATTESTATION_SCHEMA
    raise BuyE3TransactionalDeployError(
        "rollback startup schema is not frozen for the exact runtime/config target"
    )


def _frozen_07ef_runtime_sources() -> dict[str, Any]:
    bindings = {
        role: {
            "repository_relative_path": relative,
            "artifact_manifest_sha256": _FROZEN_07EF_RUNTIME_SOURCE_SHA256[role],
            "execution_commit_blob_sha256": _FROZEN_07EF_RUNTIME_SOURCE_SHA256[role],
            "working_file_sha256": _FROZEN_07EF_RUNTIME_SOURCE_SHA256[role],
        }
        for role, relative in gate_v2.REQUIRED_RUNTIME_PATHS.items()
    }
    if gate_v2.canonical_sha256(bindings) != FROZEN_07EF_RUNTIME_CODE_SHA256:
        raise BuyE3TransactionalDeployError("frozen 07ef runtime source map is inconsistent")
    return {"files": bindings, "runtime_code_sha256": FROZEN_07EF_RUNTIME_CODE_SHA256}


def _rollback_runtime_source_authority(
    *,
    identity: Mapping[str, Any],
    current_execution: Mapping[str, Any],
    current_runtime_sources: Mapping[str, Any],
) -> dict[str, Any]:
    if (
        identity.get("execution_commit") == current_execution.get("execution_commit")
        and identity.get("execution_tree") == current_execution.get("execution_tree")
        and identity.get("runtime_code_sha256")
        == current_runtime_sources.get("runtime_code_sha256")
    ):
        return dict(current_runtime_sources)
    if (
        _rollback_startup_attestation_schema(
            identity=identity, current_execution=current_execution
        )
        == HISTORICAL_STARTUP_ATTESTATION_SCHEMA
        and identity.get("runtime_code_sha256") == FROZEN_07EF_RUNTIME_CODE_SHA256
    ):
        return _frozen_07ef_runtime_sources()
    raise BuyE3TransactionalDeployError(
        "rollback runtime source authority is not frozen for the exact target"
    )


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
    host_binding = {
        "active_pointer_file_sha256": pointer.get("file_sha256"),
        "known_hosts_file_sha256": known_hosts.get("file_sha256"),
        "host_key_fingerprint": known_hosts.get("expected_fingerprint"),
        "repo_root": pointer.get("repo_root"),
        "python_executable": host.get("python_executable"),
        "venv_root": host.get("venv_root"),
    }
    if _is_successor_execution(execution):
        host_binding["current_venv_selector_target"] = host.get(
            "current_venv_selector_target"
        )
        host_binding["trusted_static_python_path"] = host.get(
            "trusted_static_python_path"
        )
        host_binding["trusted_static_python_sha256"] = host.get(
            "trusted_static_python_sha256"
        )
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
            name: configs.get(name, {}).get("config_sha256") for name in ("disabled", "active")
        },
        "host": host_binding,
        "rollback_identities": {
            name: dict(rollback.get(name, {})) for name in ("primary_disabled", "deep_predecessor")
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
    host_fields = [
        "active_pointer_file_sha256",
        "known_hosts_file_sha256",
        "host_key_fingerprint",
        "repo_root",
        "python_executable",
        "venv_root",
    ]
    if _is_successor_execution(execution):
        host_fields.extend(
            (
                "current_venv_selector_target",
                "trusted_static_python_path",
                "trusted_static_python_sha256",
            )
        )
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
            name: configs.get(name, {}).get("config_sha256") for name in ("disabled", "active")
        },
        "host": {field: host.get(field) for field in host_fields},
        "rollback_identities": {
            name: dict(rollback.get(name, {})) for name in ("primary_disabled", "deep_predecessor")
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
    command = ["ssh"]
    for option in STRICT_SSH_OPTIONS:
        command.extend(("-o", option))
    command.extend(("-o", f"UserKnownHostsFile={known_hosts}"))
    return command


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


def _release_env_unsets() -> str:
    return " ".join(
        f"-u {name}"
        for name in (
            F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
            F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
            F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
        )
    )


def _safety_env_unsets() -> str:
    return " ".join(
        f"-u {name}"
        for name in (
            LIVE_SAFETY_SUCCESSOR_PATH_ENV,
            LIVE_SAFETY_SUCCESSOR_FILE_SHA256_ENV,
            LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256_ENV,
        )
    )


def _remote_external_config_start(
    repo_root: str,
    config_path: str,
    *,
    owner_override: bool,
    active_release_binding: Mapping[str, Any] | None = None,
    safety_release_binding: Mapping[str, Any] | None = None,
    exchange_reconciliation_path: str | None = None,
    dynamic_exchange_authority: bool = False,
    startup_static_authority_env: str = "",
) -> str:
    authority_unset_fragments: list[str] = []
    if not owner_override:
        authority_unset_fragments.append(f"-u {F05_BUY_E3_OWNER_OVERRIDE_ENV}")
    if active_release_binding is None:
        authority_unset_fragments.append(_release_env_unsets())
    if safety_release_binding is None:
        authority_unset_fragments.append(_safety_env_unsets())
    if exchange_reconciliation_path is None:
        authority_unset_fragments.extend(
            f"-u {name}"
            for name in (
                STARTUP_EXCHANGE_RECONCILIATION_PATH_ENV,
                STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256_ENV,
                STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256_ENV,
                STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256_ENV,
            )
        )
    authority_unsets = " ".join(authority_unset_fragments)
    buy_authority = f"{F05_BUY_E3_OWNER_OVERRIDE_ENV}=1" if owner_override else ""
    if active_release_binding is not None:
        if not owner_override:
            raise BuyE3TransactionalDeployError(
                "disabled start cannot carry active release authority"
            )
        release_authority = " ".join(
            (
                f"{F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV}="
                f"{shlex.quote(str(active_release_binding['remote_path']))}",
                f"{F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV}="
                f"{shlex.quote(str(active_release_binding['file_sha256']))}",
                f"{F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV}="
                f"{shlex.quote(str(active_release_binding['canonical_active_release_sha256']))}",
            )
        )
    else:
        release_authority = ""
    if safety_release_binding is None:
        safety_authority = ""
    else:
        safety_authority = " ".join(
            (
                f"{LIVE_SAFETY_SUCCESSOR_PATH_ENV}="
                f"{shlex.quote(str(safety_release_binding['remote_path']))}",
                f"{LIVE_SAFETY_SUCCESSOR_FILE_SHA256_ENV}="
                f"{shlex.quote(str(safety_release_binding['file_sha256']))}",
                f"{LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256_ENV}="
                f"{shlex.quote(str(safety_release_binding['canonical_active_release_sha256']))}",
            )
        )
    if exchange_reconciliation_path is None:
        exchange_authority = ""
    elif dynamic_exchange_authority:
        exchange_authority = " ".join(
            (
                f"{STARTUP_EXCHANGE_RECONCILIATION_PATH_ENV}="
                f"{shlex.quote(exchange_reconciliation_path)}",
                f'{STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256_ENV}="$exchange_file_sha256"',
                f'{STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256_ENV}="$exchange_canonical_sha256"',
                f'{STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256_ENV}="$exchange_account_key_sha256"',
            )
        )
    else:
        raise BuyE3TransactionalDeployError(
            "successor exchange receipt requires same-transaction hash authority"
        )
    return (
        f"/usr/bin/env -i {authority_unsets} HOME=\"$HOME\" PATH=/usr/bin:/bin "
        f"{buy_authority} {release_authority} {safety_authority} {exchange_authority} "
        f"{startup_static_authority_env} "
        f"{F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV}=1 "
        "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
        "NARROWGATE_LIVE_PROFILE=native "
        f"NARROWGATE_LIVE_PROFILE_FILE={shlex.quote(repo_root + '/live/profiles/native.env')} "
        f"NARROWGATE_LIVE_CONFIG={shlex.quote(config_path)} "
        f"/bin/bash --noprofile --norc {shlex.quote(repo_root + '/live/run.sh')} start"
    )


def _remote_external_config_stop(repo_root: str, config_path: str) -> str:
    return (
        f"cd {shlex.quote(repo_root)} && "
        f"env -u {F05_BUY_E3_OWNER_OVERRIDE_ENV} {_release_env_unsets()} "
        f"{_safety_env_unsets()} "
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
    clean_isolated_execution: bool = False,
) -> str:
    buy_authority = (
        f"{F05_BUY_E3_OWNER_OVERRIDE_ENV}=1"
        if expected_enabled
        else f"-u {F05_BUY_E3_OWNER_OVERRIDE_ENV}"
    )
    command = (
        f"cd {shlex.quote(repo_root)} && env {buy_authority} {_release_env_unsets()} "
        f"{F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV}=1 "
        "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
        f"PYTHONPATH={shlex.quote(repo_root)} "
        f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
        f"{shlex.quote(python)} "
        f"{'-I -B ' if clean_isolated_execution else ''}"
        f"{shlex.quote(external_script)} isolated-preflight "
        f"--repository-root {shlex.quote(repo_root)} --config {shlex.quote(config_path)} "
        f"--expected-enabled {1 if expected_enabled else 0}"
    )
    verified = _verified_external_exec(
        command=command,
        external_script=external_script,
        external_script_sha256=external_script_sha256,
        external_gate=external_gate,
        external_gate_sha256=external_gate_sha256,
    )
    return (
        _clean_remote_shell_command(verified)
        if clean_isolated_execution
        else verified
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


def _remote_exchange_reconciliation_command(
    *,
    repo_root: str,
    external_tool_root: str,
    external_tool_python: str,
    external_script: str,
    external_script_sha256: str,
    external_gate: str,
    external_gate_sha256: str,
    config_path: str,
    output_path: str,
    startup_static_gate: str,
) -> str:
    if not startup_static_gate.strip():
        raise BuyE3TransactionalDeployError(
            "exchange reconciliation requires a trusted static runtime gate"
        )
    inner = (
        "set -eu; "
        f"{startup_static_gate}; "
        "safe_home=$HOME; "
        "set -a; "
        f". {shlex.quote(repo_root + '/live/.env')}; "
        "set +a; "
        "api_key=${BINANCE_API_KEY-}; api_secret=${BINANCE_API_SECRET-}; "
        "builtin unset BASH_ENV ENV LD_AUDIT LD_LIBRARY_PATH LD_PRELOAD "
        "PYTHONHOME PYTHONINSPECT PYTHONPATH PYTHONSTARTUP PYTHONUSERBASE; "
        "/usr/bin/env -i HOME=\"$safe_home\" PATH=/usr/bin:/bin "
        'BINANCE_API_KEY="$api_key" BINANCE_API_SECRET="$api_secret" '
        f"{F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV}=1 "
        "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
        f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
        f"{shlex.quote(external_tool_python)} -I -B {shlex.quote(external_script)} "
        f"exchange-reconcile --config {shlex.quote(config_path)} "
        f"--output {shlex.quote(output_path)}"
    )
    verified = _verified_external_exec(
        command=f"{{ {inner}; }}",
        external_script=external_script,
        external_script_sha256=external_script_sha256,
        external_gate=external_gate,
        external_gate_sha256=external_gate_sha256,
    )
    return (
        '/usr/bin/env -i HOME="$HOME" PATH=/usr/bin:/bin '
        "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
        f"/bin/bash --noprofile --norc -c {shlex.quote(verified)}"
    )


def _remote_bound_exchange_config_start(
    *,
    repo_root: str,
    runtime_config_path: str,
    reconciliation_config_path: str,
    reconciliation_output_path: str,
    owner_override: bool,
    external_tool_root: str,
    external_tool_python: str,
    external_script: str,
    external_script_sha256: str,
    external_gate: str,
    external_gate_sha256: str,
    active_release_binding: Mapping[str, Any] | None = None,
    safety_release_binding: Mapping[str, Any] | None = None,
    startup_static_authority_env: str = "",
    startup_static_gate: str = "",
    trusted_static_python_path: str = "",
) -> str:
    """Reconcile, freeze its authority, and start in one remote shell."""

    if not PurePosixPath(trusted_static_python_path).is_absolute():
        raise BuyE3TransactionalDeployError(
            "bound exchange start requires an absolute trusted Python"
        )

    reconcile = _remote_exchange_reconciliation_command(
        repo_root=repo_root,
        external_tool_root=external_tool_root,
        external_tool_python=external_tool_python,
        external_script=external_script,
        external_script_sha256=external_script_sha256,
        external_gate=external_gate,
        external_gate_sha256=external_gate_sha256,
        config_path=reconciliation_config_path,
        output_path=reconciliation_output_path,
        startup_static_gate=startup_static_gate,
    )
    canonical_field = "canonical_exchange_reconciliation_sha256"
    account_field = "account_key_sha256"
    json_field_reader = (
        'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))'
        "[sys.argv[2]])"
    )
    start = _remote_external_config_start(
        repo_root,
        runtime_config_path,
        owner_override=owner_override,
        active_release_binding=active_release_binding,
        safety_release_binding=safety_release_binding,
        exchange_reconciliation_path=reconciliation_output_path,
        dynamic_exchange_authority=True,
        startup_static_authority_env=startup_static_authority_env,
    )
    transaction = (
        f"{reconcile} && "
        f"test ! -L {shlex.quote(reconciliation_output_path)} && "
        f"test -f {shlex.quote(reconciliation_output_path)} && "
        f"test \"$(stat -c '%a' {shlex.quote(reconciliation_output_path)})\" = 600 && "
        f"test \"$(stat -c '%h' {shlex.quote(reconciliation_output_path)})\" = 1 && "
        f"exchange_file_sha256=$(sha256sum {shlex.quote(reconciliation_output_path)} | awk '{{print $1}}') && "
        f"exchange_canonical_sha256=$({shlex.quote(trusted_static_python_path)} -I -B -S -c "
        f"{shlex.quote(json_field_reader)} {shlex.quote(reconciliation_output_path)} "
        f"{shlex.quote(canonical_field)}) && "
        f"exchange_account_key_sha256=$({shlex.quote(trusted_static_python_path)} -I -B -S -c "
        f"{shlex.quote(json_field_reader)} {shlex.quote(reconciliation_output_path)} "
        f"{shlex.quote(account_field)}) && "
        "case \"$exchange_file_sha256$exchange_canonical_sha256"
        "$exchange_account_key_sha256\" in "
        "*[!0-9a-f]*|'') exit 86 ;; esac && "
        "test \"${#exchange_file_sha256}\" = 64 && "
        "test \"${#exchange_canonical_sha256}\" = 64 && "
        "test \"${#exchange_account_key_sha256}\" = 64 && "
        f"{start}"
    )
    return (
        '/usr/bin/env -i HOME="$HOME" PATH=/usr/bin:/bin '
        "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
        f"/bin/bash --noprofile --norc -c {shlex.quote(transaction)}"
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
        role: gate_v2.file_sha256(Path(local_package[role])) for role in _EXTERNAL_PACKAGE_ROLES
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
    try:
        committed_tool_relatives = {
            role: Path(local_package[role]).resolve().relative_to(REPO_ROOT.resolve())
            for role in ("deploy_script", "gate_amendment")
        }
    except ValueError as exc:
        raise BuyE3TransactionalDeployError(
            "successor external tools must be repository-owned files"
        ) from exc
    disabled_config = str(remote["disabled_config_path"])
    active_config = str(remote["active_config_path"])
    successor_process_contract = _is_successor_execution(execution)
    legacy_pid_file = str(remote["pid_file"])
    supervisor_pid_file = str(
        remote.get("supervisor_pid_file", legacy_pid_file)
    )
    maker_child_pid_file = str(
        remote.get("maker_child_pid_file", legacy_pid_file)
    )
    if successor_process_contract and (
        supervisor_pid_file == maker_child_pid_file
        or supervisor_pid_file == legacy_pid_file == maker_child_pid_file
    ):
        raise BuyE3TransactionalDeployError(
            "successor deploy requires distinct supervisor and maker child PID files"
        )
    pid_file = maker_child_pid_file
    successor_safety_binding = (
        {
            "remote_path": str(remote["safety_release_path"]),
            "file_sha256": str(remote["safety_release_file_sha256"]),
            "canonical_active_release_sha256": str(
                remote["safety_release_canonical_sha256"]
            ),
        }
        if successor_process_contract
        else None
    )
    successor_static_authority_binding: dict[str, str] | None = None
    successor_static_authority_env = ""
    successor_static_gate = ""
    successor_candidate_static_gate = ""
    if successor_process_contract:
        static_relative = "scripts/f05_live_safety_startup_static_authority.py"
        static_source = runtime_sources.get("files", {}).get(static_relative)
        if not isinstance(static_source, Mapping):
            raise BuyE3TransactionalDeployError(
                "successor runtime sources lack the startup static verifier"
            )
        static_source_sha256 = _require_sha256(
            static_source.get("working_file_sha256"),
            "successor startup static verifier source",
        )
        successor_static_authority_binding = {
            "remote_path": str(remote["startup_static_runtime_authority_path"]),
            "file_sha256": _require_sha256(
                remote["startup_static_runtime_authority_file_sha256"],
                "successor startup static authority file",
            ),
            "canonical_sha256": _require_sha256(
                remote["startup_static_runtime_authority_canonical_sha256"],
                "successor startup static authority canonical",
            ),
        }
        static_authority_projection = {
            "authority_verifier": {
                "path": (
                    f"{stage_root}/runtime-{execution['execution_commit']}/"
                    f"{static_relative}"
                ),
                "sha256": static_source_sha256,
            },
            "trusted_python": {
                "path": str(host["trusted_static_python_path"]),
                "sha256": _require_sha256(
                    host["trusted_static_python_sha256"],
                    "successor trusted static Python",
                ),
            },
        }
        successor_static_authority_env = _startup_static_authority_env(
            static_authority_projection,
            successor_static_authority_binding,
        )
        static_verifier_path = str(
            static_authority_projection["authority_verifier"]["path"]
        )
        trusted_python_path = str(
            static_authority_projection["trusted_python"]["path"]
        )
        authority_path = str(successor_static_authority_binding["remote_path"])
        raw_successor_static_gate = (
            f"test ! -L {shlex.quote(trusted_python_path)} && "
            f"test -f {shlex.quote(trusted_python_path)} && "
            f"test \"$(stat -c '%h' {shlex.quote(trusted_python_path)})\" = 1 && "
            f"test \"$(sha256sum {shlex.quote(trusted_python_path)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(static_authority_projection['trusted_python']['sha256']))} && "
            f"test ! -L {shlex.quote(static_verifier_path)} && "
            f"test -f {shlex.quote(static_verifier_path)} && "
            f"test \"$(stat -c '%h' {shlex.quote(static_verifier_path)})\" = 1 && "
            f"test \"$(sha256sum {shlex.quote(static_verifier_path)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(static_source_sha256)} && "
            f"test ! -L {shlex.quote(authority_path)} && "
            f"test -f {shlex.quote(authority_path)} && "
            f"test \"$(stat -c '%h' {shlex.quote(authority_path)})\" = 1 && "
            f"test \"$(sha256sum {shlex.quote(authority_path)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(successor_static_authority_binding['file_sha256'])} && "
            "env PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
            f"{shlex.quote(trusted_python_path)} -I -B -S "
            f"{shlex.quote(static_verifier_path)} --authority "
            f"{shlex.quote(authority_path)} --expected-file-sha256 "
            f"{shlex.quote(successor_static_authority_binding['file_sha256'])} "
            f"--expected-canonical-sha256 "
            f"{shlex.quote(successor_static_authority_binding['canonical_sha256'])}"
        )
        successor_static_gate = _clean_static_gate_command(
            raw_successor_static_gate
        )
        successor_candidate_static_gate = _clean_static_gate_command(
            f"{raw_successor_static_gate} --candidate-only"
        )
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
    compatible = execution.get("compatible_attempt_manifest") is not None
    isolated_required = compatible or successor_process_contract

    def install_command(label: str, destinations: Sequence[tuple[str, str]]) -> dict[str, Any]:
        fragments: list[str] = []
        for role, destination in destinations:
            source = staged_paths[role]
            parent = str(PurePosixPath(destination).parent)
            expected_sha = local_hashes[role]
            fragments.append(
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
        return _command(
            label,
            _ssh_command(
                target=target,
                known_hosts=known,
                remote_command=" && ".join(fragments),
            ),
            mutates=True,
        )

    install_bytes = install_command("install-private-artifact-and-config-bytes", installs)
    isolated_root = f"{stage_root}/runtime-{execution['execution_commit']}"
    isolated_venv = f"{stage_root}/venv-{execution['execution_commit']}"
    isolated_python = f"{isolated_venv}/bin/python3"
    isolated_wheel_dir = f"{stage_root}/native-wheel-{execution['execution_commit']}"
    selected_python = f"{repo_root}/.venv-active/bin/python3"
    selected_venv = f"{repo_root}/.venv-active"
    external_tool_root = isolated_root if successor_process_contract else repo_root
    external_tool_python = isolated_python if successor_process_contract else python
    exchange_external_script = (
        f"{external_tool_root}/scripts/deploy_f05_buy_e3_owner_v1.py"
        if successor_process_contract
        else external_script
    )
    operational_external_script = exchange_external_script
    selector_temp = (
        f"{repo_root}/.venv-active.next-{package_sha256[:12]}-"
        f"{str(execution['execution_commit'])[:12]}"
    )
    if successor_process_contract and (
        python != selected_python or str(host["venv_root"]) != selected_venv
    ):
        raise BuyE3TransactionalDeployError(
            "successor host identity must select the atomic .venv-active runtime"
        )
    predecessor_selector_target = str(
        host.get("current_venv_selector_target", "")
    ).strip()
    trusted_static_python = str(host.get("trusted_static_python_path", "")).strip()
    trusted_static_python_sha256 = str(
        host.get("trusted_static_python_sha256", "")
    ).strip().lower()
    if successor_process_contract and (
        not predecessor_selector_target
        or "\x00" in predecessor_selector_target
        or not PurePosixPath(trusted_static_python).is_absolute()
    ):
        raise BuyE3TransactionalDeployError(
            "successor host identity lacks the frozen predecessor selector/interpreter"
        )
    if successor_process_contract:
        _require_sha256(
            trusted_static_python_sha256,
            "successor trusted static Python hash",
        )
    isolated_installs: list[tuple[str, str]] = []
    prepare_isolated_runtime: dict[str, Any] | None = None
    install_isolated_bytes: dict[str, Any] | None = None
    if isolated_required:
        repo_prefix = PurePosixPath(repo_root)
        for role, destination in installs:
            try:
                relative = PurePosixPath(destination).relative_to(repo_prefix)
            except ValueError as exc:
                raise BuyE3TransactionalDeployError(
                    "compatible preflight paths must live below the remote repository"
                ) from exc
            isolated_installs.append((role, str(PurePosixPath(isolated_root) / relative)))
        tag = str(execution["annotated_tag"])
        commit = str(execution["execution_commit"])
        tree = str(execution["execution_tree"])
        tag_object = str(execution["annotated_tag_object"])
        native_receipt = f"{stage_root}/native-build-{commit}.json"
        if successor_process_contract:
            prepare_label = "validate-prebuilt-successor-runtime"
            prepare_remote_command = (
                "export PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 && "
                f"cd {shlex.quote(repo_root)} && "
                'test -z "$(git status --porcelain=v1 --untracked-files=all)" && '
                f'test "$(git cat-file -t refs/tags/{shlex.quote(tag)})" = tag && '
                f'test "$(git rev-parse refs/tags/{shlex.quote(tag)})" = '
                f"{shlex.quote(tag_object)} && "
                f'test "$(git rev-parse refs/tags/{shlex.quote(tag)}^{{}})" = '
                f"{shlex.quote(commit)} && "
                f'test "$(git rev-parse {shlex.quote(commit)}^{{tree}})" = '
                f"{shlex.quote(tree)} && "
                f"test ! -L {shlex.quote(isolated_root)} && "
                f"test -d {shlex.quote(isolated_root)} && "
                f"cd {shlex.quote(isolated_root)} && "
                f"test \"$(sha256sum {shlex.quote(str(committed_tool_relatives['deploy_script']))} | awk '{{print $1}}')\" = {shlex.quote(external_script_sha256)} && "
                f"test \"$(sha256sum {shlex.quote(str(committed_tool_relatives['gate_amendment']))} | awk '{{print $1}}')\" = {shlex.quote(external_gate_sha256)} && "
                f'test "$(git cat-file -t refs/tags/{shlex.quote(tag)})" = tag && '
                f'test "$(git rev-parse refs/tags/{shlex.quote(tag)})" = '
                f"{shlex.quote(tag_object)} && "
                f'test "$(git rev-parse refs/tags/{shlex.quote(tag)}^{{}})" = '
                f"{shlex.quote(commit)} && "
                f'test "$(git rev-parse HEAD)" = {shlex.quote(commit)} && '
                f'test "$(git rev-parse HEAD^{{tree}})" = {shlex.quote(tree)} && '
                'test -z "$(git status --porcelain=v1 --untracked-files=all)" && '
                'test "$(uname -s)" = Linux && test "$(uname -m)" = x86_64 && '
                f"test ! -L {shlex.quote(isolated_venv)} && "
                f"test -x {shlex.quote(isolated_python)} && "
                f"test ! -e {shlex.quote(selector_temp)} && "
                f'test "$({shlex.quote(isolated_python)} -B -c '
                "'import sys; print(f\"{sys.version_info.major}.{sys.version_info.minor}\")')"
                "= 3.12 && "
                f"{shlex.quote(isolated_python)} -B -m pip check && "
                f"PYTHONPATH={shlex.quote(isolated_root)} {shlex.quote(isolated_python)} -B -c \"import Crypto, binance, importlib.machinery, live.main, live.ws_handler, narrowgate_cpp, numpy, pandas, pytest, requests, strategy.maker_engine, websocket, yaml, pathlib, platform, sys, sysconfig; "
                "assert platform.system() == 'Linux' and platform.machine() == 'x86_64'; "
                "assert str(sysconfig.get_config_var('SOABI')).startswith('cpython-312-'); "
                "assert pathlib.Path(narrowgate_cpp.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve()); "
                "assert any(str(narrowgate_cpp.__file__).endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES)\" && "
                f"test ! -L {shlex.quote(isolated_wheel_dir)} && "
                f"test -d {shlex.quote(isolated_wheel_dir)} && "
                f"test \"$(find {shlex.quote(isolated_wheel_dir)} -maxdepth 1 -type f -name '*.whl' | wc -l)\" = 1 && "
                f"test ! -L {shlex.quote(native_receipt)} && "
                f"test -f {shlex.quote(native_receipt)} && "
                f"test \"$(stat -c '%a' {shlex.quote(native_receipt)})\" = 600 && "
                f"test \"$(stat -c '%h' {shlex.quote(native_receipt)})\" = 1"
            )
            prepare_mutates = False
        else:
            prepare_label = "fetch-and-prepare-isolated-runtime"
            prepare_remote_command = (
                f"cd {shlex.quote(repo_root)} && "
                'test -z "$(git status --porcelain=v1 --untracked-files=all)" && '
                "git fetch --no-tags origin "
                "refs/heads/main:refs/remotes/origin/main && "
                f"git fetch --no-tags origin refs/tags/{shlex.quote(tag)}:"
                f"refs/tags/{shlex.quote(tag)} && "
                f'test "$(git cat-file -t refs/tags/{shlex.quote(tag)})" = tag && '
                f'test "$(git rev-parse refs/tags/{shlex.quote(tag)})" = '
                f"{shlex.quote(tag_object)} && "
                f'test "$(git rev-parse refs/tags/{shlex.quote(tag)}^{{}})" = '
                f"{shlex.quote(commit)} && "
                f"test ! -L {shlex.quote(isolated_root)} && "
                f"(test -d {shlex.quote(isolated_root)} || "
                f"git worktree add --detach {shlex.quote(isolated_root)} "
                f"{shlex.quote(commit)}) && "
                f"cd {shlex.quote(isolated_root)} && "
                f'test "$(git rev-parse HEAD)" = {shlex.quote(commit)} && '
                f'test "$(git rev-parse HEAD^{{tree}})" = {shlex.quote(tree)} && '
                'test -z "$(git status --porcelain=v1 --untracked-files=all)" && '
                'test "$(uname -s)" = Linux && test "$(uname -m)" = x86_64 && '
                'test "$(python3.12 -B -c \'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")\')" = 3.12 && '
                f"(test -x {shlex.quote(isolated_python)} || ("
                f"test ! -e {shlex.quote(isolated_venv)} && "
                f"python3.12 -B -m venv {shlex.quote(isolated_venv)} && "
                f"mkdir -p {shlex.quote(isolated_wheel_dir)} && "
                f"{shlex.quote(isolated_python)} -B -m pip wheel --no-deps "
                f"--no-build-isolation --wheel-dir {shlex.quote(isolated_wheel_dir)} cpp && "
                f"wheel=$(find {shlex.quote(isolated_wheel_dir)} -maxdepth 1 -type f "
                "-name '*.whl' -print -quit) && test -n \"$wheel\" && "
                f"{shlex.quote(isolated_python)} -B -m pip install --force-reinstall "
                "--no-deps \"$wheel\")) && "
                f"{shlex.quote(isolated_python)} -B -c \"import importlib.machinery, narrowgate_cpp, pathlib, platform, sys, sysconfig; "
                "assert sys.version_info[:2] == (3, 12); "
                "assert platform.system() == 'Linux' and platform.machine() == 'x86_64'; "
                "assert str(sysconfig.get_config_var('SOABI')).startswith('cpython-312-'); "
                "required=('compute_quote_core_live','compute_live_routing_decision',"
                "'SignalFeatureEngine','SIGNAL_FEATURE_NAMES','TradeBarAggregator'); "
                "assert all(hasattr(narrowgate_cpp,name) for name in required); "
                "q=narrowgate_cpp.QuoteFlags(); s=narrowgate_cpp.SideQuoteContext(); "
                "assert all(hasattr(q,name) for name in ('delta_cap','final_compressed','cap_exposure_block')); "
                "assert hasattr(s,'cap_exposure_block'); "
                "assert pathlib.Path(narrowgate_cpp.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve()); "
                "assert any(str(narrowgate_cpp.__file__).endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES)\" && "
                f"site=$({shlex.quote(isolated_python)} -B -c "
                "'import site; print(site.getsitepackages()[0])') && "
                f"PYTHONPATH=\"$site:{shlex.quote(isolated_root)}\" python3.12 -B "
                "-m pytest -q tests/test_cpp_quote_core_parity.py "
                "tests/test_cpp_tick_replay_golden_parity.py "
                "tests/test_conditional_p3_cpp_overlay.py"
            )
            prepare_mutates = True
        prepare_isolated_runtime = _command(
            prepare_label,
            _ssh_command(
                target=target,
                known_hosts=known,
                remote_command=prepare_remote_command,
            ),
            mutates=prepare_mutates,
        )
        install_isolated_bytes = install_command(
            "install-isolated-preflight-artifact-and-config-bytes",
            isolated_installs,
        )
    checkout = (
        f"cd {shlex.quote(repo_root)} && "
        f'test "$(git cat-file -t refs/tags/{shlex.quote(str(execution["annotated_tag"]))})" '
        f'= tag && test "$(git rev-parse refs/tags/{shlex.quote(str(execution["annotated_tag"]))})" '
        f"= {shlex.quote(str(execution['annotated_tag_object']))} && "
        f'test "$(git rev-parse refs/tags/{shlex.quote(str(execution["annotated_tag"]))}^{{}})" '
        f"= {shlex.quote(str(execution['execution_commit']))} && git checkout --detach "
        f"{shlex.quote(str(execution['execution_commit']))} && "
        f'test "$(git rev-parse HEAD^{{tree}})" = {shlex.quote(str(execution["execution_tree"]))} '
        '&& test -z "$(git status --porcelain=v1 --untracked-files=all)"'
    )
    preflight_root = isolated_root if isolated_required else repo_root
    preflight_disabled_config = (
        next(destination for role, destination in isolated_installs if role == "disabled_config")
        if isolated_required
        else disabled_config
    )
    preflight_active_config = (
        next(destination for role, destination in isolated_installs if role == "active_config")
        if isolated_required
        else active_config
    )
    preflight_python = isolated_python if successor_process_contract else python
    disabled_preflight = _remote_preflight(
        repo_root=preflight_root,
        external_script=operational_external_script,
        config_path=preflight_disabled_config,
        expected_enabled=False,
        python=preflight_python,
        external_gate=external_gate,
        external_script_sha256=external_script_sha256,
        external_gate_sha256=external_gate_sha256,
        clean_isolated_execution=successor_process_contract,
    )
    active_preflight = _remote_preflight(
        repo_root=preflight_root,
        external_script=operational_external_script,
        config_path=preflight_active_config,
        expected_enabled=True,
        python=preflight_python,
        external_gate=external_gate,
        external_script_sha256=external_script_sha256,
        external_gate_sha256=external_gate_sha256,
        clean_isolated_execution=successor_process_contract,
    )

    def common_pre_stop(
        checkpoint_path: str, *, existing_successor_family: bool
    ) -> list[dict[str, Any]]:
        log_checkpoint = _verified_external_exec(
            command=(
                "env PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
                f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
                f"PYTHONPATH={shlex.quote(external_tool_root)} "
                f"{shlex.quote(external_tool_python)} "
                f"{'-I -B ' if successor_process_contract else ''}"
                f"{shlex.quote(operational_external_script)} log-checkpoint --log "
                f"{shlex.quote(str(remote['log_path']))} --output "
                f"{shlex.quote(checkpoint_path)}"
            ),
            external_script=operational_external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
        )
        if successor_process_contract:
            log_checkpoint = _clean_remote_shell_command(log_checkpoint)
        if isolated_required:
            if prepare_isolated_runtime is None or install_isolated_bytes is None:
                raise BuyE3TransactionalDeployError(
                    "compatible isolated preflight commands are incomplete"
                )
            preflight_setup = [prepare_isolated_runtime, install_isolated_bytes]
            if successor_process_contract:
                # Make rollback B0 dependencies ready while the predecessor is
                # still untouched.  Post-stop work is then only atomic runtime
                # selection/checkout/start plus read-only verification.
                preflight_setup.append(install_bytes)
        else:
            preflight_setup = [install_bytes]
        existing_child_pid_file = (
            maker_child_pid_file if existing_successor_family else legacy_pid_file
        )
        pid_capture_rows = [
            _command(
                "capture-old-pid",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=(
                        (
                            f"test -s {shlex.quote(existing_child_pid_file)} && "
                            f"p=$(cat {shlex.quote(existing_child_pid_file)}) && "
                            "test -r /proc/$p/stat && printf '%s %s\\n' \"$p\" "
                            "\"$(awk '{print $22}' /proc/$p/stat)\""
                        )
                        if successor_process_contract
                        else (
                            f"test -s {shlex.quote(existing_child_pid_file)} && "
                            f"printf '%s\\n' \"$(cat {shlex.quote(existing_child_pid_file)})\""
                        )
                    ),
                ),
                mutates=False,
            )
        ]
        selector_target = (
            isolated_venv
            if existing_successor_family
            else predecessor_selector_target
        )
        selector_check = _command(
            "validate-current-venv-selector-before-stop",
            _ssh_command(
                target=target,
                known_hosts=known,
                remote_command=(
                    f"test -L {shlex.quote(selected_venv)} && "
                    f"test \"$(readlink {shlex.quote(selected_venv)})\" = "
                    f"{shlex.quote(selector_target)} && "
                    f"test ! -e {shlex.quote(selector_temp)}"
                ),
            ),
            mutates=False,
        )
        if existing_successor_family:
            pid_capture_rows.append(
                _command(
                    "capture-old-supervisor-pid",
                    _ssh_command(
                        target=target,
                        known_hosts=known,
                        remote_command=(
                            f"test -s {shlex.quote(supervisor_pid_file)} && "
                            f"p=$(cat {shlex.quote(supervisor_pid_file)}) && "
                            "test -r /proc/$p/stat && printf '%s %s\\n' \"$p\" "
                            "\"$(awk '{print $22}' /proc/$p/stat)\""
                        ),
                    ),
                    mutates=False,
                )
            )
        return [
            prepare_stage,
            *transfers,
            freeze_stage,
            *preflight_setup,
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
            *([selector_check] if successor_process_contract else []),
            *pid_capture_rows,
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

    def stop_with_identity_quiescence(
        config_path: str, *, existing_successor_family: bool
    ) -> list[str]:
        stop = _remote_external_config_stop(repo_root, config_path)
        if not successor_process_contract:
            remote_stop = stop
        else:
            child_file = (
                maker_child_pid_file if existing_successor_family else legacy_pid_file
            )
            capture = (
                f"cp=$(cat {shlex.quote(child_file)}) && "
                "test \"$cp\" -gt 1 && test -r /proc/$cp/stat && "
                "ct=$(awk '{print $22}' /proc/$cp/stat) && test \"$ct\" -gt 0 && "
            )
            supervisor_check = ""
            stop_state_check = ""
            if existing_successor_family:
                capture += (
                    f"sp=$(cat {shlex.quote(supervisor_pid_file)}) && "
                    "test \"$sp\" -gt 1 && test \"$sp\" != \"$cp\" && "
                    "test -r /proc/$sp/stat && "
                    "st=$(awk '{print $22}' /proc/$sp/stat) && test \"$st\" -gt 0 && "
                )
                supervisor_check = (
                    "if test -r /proc/$sp/stat && "
                    "test \"$(awk '{print $22}' /proc/$sp/stat)\" = \"$st\"; "
                    "then exit 72; fi && "
                )
                stop_state = f"{repo_root}/logs/maker.stop.state"
                stop_state_check = (
                    f"test ! -L {shlex.quote(stop_state)} && "
                    f"test -f {shlex.quote(stop_state)} && "
                    f"test \"$(awk -F= '$1==\"schema\" {{print $2}}' {shlex.quote(stop_state)})\" = narrowgate_live_stop_state.v1 && "
                    f"test \"$(awk -F= '$1==\"supervisor_pid\" {{print $2}}' {shlex.quote(stop_state)})\" = \"$sp\" && "
                    f"test \"$(awk -F= '$1==\"child_pid\" {{print $2}}' {shlex.quote(stop_state)})\" = \"$cp\" && "
                    f"test \"$(awk -F= '$1==\"supervisor_state\" {{print $2}}' {shlex.quote(stop_state)})\" = stopped_by_operator && "
                    f"test \"$(awk -F= '$1==\"child_exit_code\" {{print $2}}' {shlex.quote(stop_state)})\" = 0 && "
                    f"test \"$(awk -F= '$1==\"kill_escalation\" {{print $2}}' {shlex.quote(stop_state)})\" = 0 && "
                    f"test \"$(awk -F= '$1==\"orphan_count\" {{print $2}}' {shlex.quote(stop_state)})\" = 0 && "
                    f"test \"$(awk -F= '$1==\"clean\" {{print $2}}' {shlex.quote(stop_state)})\" = 1 && "
                )
            remote_stop = (
                "{ "
                + capture
                + "true; } || exit 70; stop_rc=0; ("
                + stop
                + ") || stop_rc=$?; "
                + "if test -r /proc/$cp/stat && "
                + "test \"$(awk '{print $22}' /proc/$cp/stat)\" = \"$ct\"; "
                + "then exit 71; fi && "
                + supervisor_check
                + stop_state_check
                + "test -z \"$(pgrep -f '[l]ive/main.py' || true)\" && "
                + "test \"$stop_rc\" = 0"
            )
        return _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=remote_stop,
        )

    stop_disabled_initial = stop_with_identity_quiescence(
        disabled_config, existing_successor_family=False
    )
    stop_disabled_successor = stop_with_identity_quiescence(
        disabled_config, existing_successor_family=successor_process_contract
    )
    stop_active = stop_with_identity_quiescence(
        active_config, existing_successor_family=successor_process_contract
    )
    quiescent = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=("test -z \"$(pgrep -f '[l]ive/main.py' || true)\""),
    )

    def exchange_reconciliation_remote_command(
        config_path: str, output_path: str
    ) -> str:
        return _remote_exchange_reconciliation_command(
            repo_root=repo_root,
            external_tool_root=external_tool_root,
            external_tool_python=external_tool_python,
            external_script=exchange_external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
            config_path=config_path,
            output_path=output_path,
            startup_static_gate=successor_candidate_static_gate,
        )
    def exchange_reconciliation(config_path: str, output_path: str) -> list[str]:
        return _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=exchange_reconciliation_remote_command(
                config_path, output_path
            ),
        )

    disabled_exchange_checkpoint = f"{disabled_checkpoint}.exchange"
    active_exchange_checkpoint = f"{active_checkpoint}.exchange"
    rollback_exchange_checkpoint = f"{checkpoint_base}.rollback.exchange"
    checkout_command = _ssh_command(target=target, known_hosts=known, remote_command=checkout)
    select_successor_venv = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=(
            f"test -x {shlex.quote(isolated_python)} && "
            f"(test ! -e {shlex.quote(selected_venv)} || "
            f"test -L {shlex.quote(selected_venv)}) && "
            f"test ! -e {shlex.quote(selector_temp)} && "
            f"tmp={shlex.quote(selector_temp)} && "
            "cleanup() { test ! -L \"$tmp\" || rm -f -- \"$tmp\"; } && "
            "trap cleanup EXIT HUP INT TERM && "
            f"ln -s {shlex.quote(isolated_venv)} "
            "\"$tmp\" && "
            f"test \"$(readlink \"$tmp\")\" = {shlex.quote(isolated_venv)} && "
            f"mv -Tf \"$tmp\" {shlex.quote(repo_root + '/.venv-active')} && "
            "trap - EXIT HUP INT TERM"
        ),
    )
    start_disabled = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=(
            _remote_bound_exchange_config_start(
                repo_root=repo_root,
                runtime_config_path=disabled_config,
                reconciliation_config_path=disabled_config,
                reconciliation_output_path=f"{disabled_exchange_checkpoint}.startup",
                owner_override=False,
                external_tool_root=external_tool_root,
                external_tool_python=external_tool_python,
                external_script=exchange_external_script,
                external_script_sha256=external_script_sha256,
                external_gate=external_gate,
                external_gate_sha256=external_gate_sha256,
                safety_release_binding=successor_safety_binding,
                startup_static_authority_env=successor_static_authority_env,
                startup_static_gate=successor_static_gate,
                trusted_static_python_path=str(host["trusted_static_python_path"]),
            )
            if successor_process_contract
            else _remote_external_config_start(
                repo_root,
                disabled_config,
                owner_override=False,
            )
        ),
    )
    start_active = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=(
            _remote_bound_exchange_config_start(
                repo_root=repo_root,
                runtime_config_path=active_config,
                reconciliation_config_path=disabled_config,
                reconciliation_output_path=f"{active_exchange_checkpoint}.startup",
                owner_override=True,
                external_tool_root=external_tool_root,
                external_tool_python=external_tool_python,
                external_script=exchange_external_script,
                external_script_sha256=external_script_sha256,
                external_gate=external_gate,
                external_gate_sha256=external_gate_sha256,
                safety_release_binding=successor_safety_binding,
                startup_static_authority_env=successor_static_authority_env,
                startup_static_gate=successor_static_gate,
                trusted_static_python_path=str(host["trusted_static_python_path"]),
            )
            if successor_process_contract
            else _remote_external_config_start(
                repo_root,
                active_config,
                owner_override=True,
            )
        ),
    )

    def process_probe(
        config_path: str,
        config_sha: str,
        enabled: bool,
        *,
        expected_execution: Mapping[str, Any] = execution,
        expected_runtime_code_sha256: str = str(runtime_sources["runtime_code_sha256"]),
        expected_runtime_sources: Mapping[str, Any] = runtime_sources,
        expected_artifact_sha256: str = str(artifact["artifact_sha256"]),
        expected_startup_attestation_schema_version: str = STARTUP_ATTESTATION_SCHEMA,
        expected_exchange_reconciliation_path: str | None = None,
    ) -> list[str]:
        command = _verified_external_exec(
            command=(
                f"cd {shlex.quote(external_tool_root)} && env "
                "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
                f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
                f"PYTHONPATH={shlex.quote(external_tool_root)} "
                f"{shlex.quote(external_tool_python)} "
                f"{'-I -B ' if successor_process_contract else ''}"
                f"{shlex.quote(operational_external_script)} process-probe --repository-root "
                f"{shlex.quote(repo_root)} --pid-file {shlex.quote(pid_file)} --config "
                f"{shlex.quote(config_path)} --config-sha256 {shlex.quote(config_sha)} "
                f"--python-executable {shlex.quote(python)} --venv-root "
                f"{shlex.quote(str(host['venv_root']))} --runtime-identity "
                f"{shlex.quote(str(remote['runtime_identity_path']))} --expected-enabled "
                f"{1 if enabled else 0} --execution-commit "
                f"{shlex.quote(str(expected_execution['execution_commit']))} --execution-tree "
                f"{shlex.quote(str(expected_execution['execution_tree']))} "
                f"--runtime-code-sha256 {shlex.quote(expected_runtime_code_sha256)} "
                f"--runtime-source-authority-base64 "
                f"{shlex.quote(_encode_runtime_source_authority(expected_runtime_sources))} "
                f"--expected-startup-attestation-schema-version "
                f"{shlex.quote(expected_startup_attestation_schema_version)}"
            ),
            external_script=operational_external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
        )
        if expected_artifact_sha256:
            command += (
                f" --artifact-sha256 {shlex.quote(expected_artifact_sha256)}"
                f" --artifact-manifest {shlex.quote(str(remote['artifact_manifest_path']))}"
                f" --policy {shlex.quote(str(remote['policy_path']))}"
                f" --predicate-bundle {shlex.quote(str(remote['predicate_bundle_path']))}"
                f" --artifact-manifest-file-sha256 "
                f"{shlex.quote(str(artifact['manifest_file_sha256']))}"
                f" --policy-file-sha256 "
                f"{shlex.quote(str(artifact['policy_file_sha256']))}"
                f" --predicate-bundle-file-sha256 "
                f"{shlex.quote(str(artifact['predicate_bundle_file_sha256']))}"
            )
        if successor_process_contract:
            if expected_exchange_reconciliation_path is None:
                raise BuyE3TransactionalDeployError(
                    "successor process probe requires exact exchange reconciliation path"
                )
            assert successor_safety_binding is not None
            command += (
                f" --safety-release "
                f"{shlex.quote(str(successor_safety_binding['remote_path']))}"
                f" --safety-release-file-sha256 "
                f"{shlex.quote(str(successor_safety_binding['file_sha256']))}"
                f" --safety-release-canonical-sha256 "
                f"{shlex.quote(str(successor_safety_binding['canonical_active_release_sha256']))}"
                f" --safety-active-config-sha256 "
                f"{shlex.quote(str(configs['active']['config_sha256']))}"
                f" --safety-disabled-config-sha256 "
                f"{shlex.quote(str(configs['disabled']['config_sha256']))}"
                f" --expected-exchange-reconciliation-path "
                f"{shlex.quote(expected_exchange_reconciliation_path)}"
            )
            if enabled:
                command += f" {_ACTIVE_RELEASE_PROBE_ARGS_PLACEHOLDER}"
            command = _clean_remote_shell_command(command)
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

    def process_family_probe(label: str) -> dict[str, Any] | None:
        if not successor_process_contract:
            return None
        command = (
            f"sp=$(cat {shlex.quote(supervisor_pid_file)}) && "
            f"cp=$(cat {shlex.quote(maker_child_pid_file)}) && "
            "test \"$sp\" -gt 1 && test \"$cp\" -gt 1 && test \"$sp\" != \"$cp\" && "
            "test -r /proc/$sp/stat && test -r /proc/$cp/stat && "
            "test \"$(awk '{print $4}' /proc/$cp/stat)\" = \"$sp\" && "
            "ss1=$(awk '{print $22}' /proc/$sp/stat) && "
            "cs1=$(awk '{print $22}' /proc/$cp/stat) && "
            "test \"$ss1\" -gt 0 && test \"$cs1\" -gt 0 && "
            "ss2=$(awk '{print $22}' /proc/$sp/stat) && "
            "cs2=$(awk '{print $22}' /proc/$cp/stat) && "
            "test \"$ss1\" = \"$ss2\" && test \"$cs1\" = \"$cs2\" && "
            "printf '%s %s %s %s\\n' \"$sp\" \"$ss1\" \"$cp\" \"$cs1\""
        )
        return _command(
            label,
            _ssh_command(target=target, known_hosts=known, remote_command=command),
            mutates=False,
            after_stop=True,
        )

    def log_validate(checkpoint_path: str) -> list[str]:
        markers = " ".join(
            f"--marker {shlex.quote(str(marker))}" for marker in remote["startup_markers"]
        )
        command = _verified_external_exec(
            command=(
                "env PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
                f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
                f"PYTHONPATH={shlex.quote(external_tool_root)} "
                f"{shlex.quote(external_tool_python)} "
                f"{'-I -B ' if successor_process_contract else ''}"
                f"{shlex.quote(operational_external_script)} log-validate --log "
                f"{shlex.quote(str(remote['log_path']))} --checkpoint "
                f"{shlex.quote(checkpoint_path)} {markers}"
            ),
            external_script=operational_external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
        )
        if successor_process_contract:
            command = _clean_remote_shell_command(command)
        if successor_process_contract:
            stable_family = (
                f"test \"$(cat {shlex.quote(supervisor_pid_file)})\" = \"$sp\" && "
                f"test \"$(cat {shlex.quote(maker_child_pid_file)})\" = \"$cp\" && "
                "test -r /proc/$sp/stat && test -r /proc/$cp/stat && "
                "test \"$(awk '{print $22}' /proc/$sp/stat)\" = \"$st\" && "
                "test \"$(awk '{print $22}' /proc/$cp/stat)\" = \"$ct\" && "
                "test \"$(awk '{print $4}' /proc/$cp/stat)\" = \"$sp\""
            )
            command = (
                f"sp=$(cat {shlex.quote(supervisor_pid_file)}) && "
                f"cp=$(cat {shlex.quote(maker_child_pid_file)}) && "
                "test \"$sp\" -gt 1 && test \"$cp\" -gt 1 && "
                "test \"$sp\" != \"$cp\" && "
                "st=$(awk '{print $22}' /proc/$sp/stat) && "
                "ct=$(awk '{print $22}' /proc/$cp/stat) && "
                "test \"$st\" -gt 0 && test \"$ct\" -gt 0 && "
                "deadline=$((SECONDS + 120)) && "
                f"until ({command}); do {stable_family} && "
                "test \"$SECONDS\" -lt \"$deadline\" && sleep 1; done && "
                f"{stable_family}"
            )
        return _ssh_command(target=target, known_hosts=known, remote_command=command)

    current_startup_schema = (
        HISTORICAL_STARTUP_ATTESTATION_SCHEMA
        if (
            execution.get("execution_commit") == FROZEN_07EF_EXECUTION_COMMIT
            and execution.get("execution_tree") == FROZEN_07EF_EXECUTION_TREE
            and configs.get("disabled", {}).get("config_sha256")
            == FROZEN_07EF_DISABLED_CONFIG_SHA256
            and configs.get("active", {}).get("config_sha256")
            == FROZEN_07EF_ACTIVE_CONFIG_SHA256
        )
        else PREDECESSOR_STARTUP_ATTESTATION_SCHEMA
        if (
            execution.get("execution_commit") == gate_v2.FROZEN_EXECUTION_COMMIT
            and execution.get("execution_tree") == gate_v2.FROZEN_EXECUTION_TREE
        )
        else SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
        if _is_successor_execution(execution)
        else STARTUP_ATTESTATION_SCHEMA
    )
    disabled = [
        *common_pre_stop(disabled_checkpoint, existing_successor_family=False),
        _command("stop-live", stop_disabled_initial, mutates=True, after_stop=True),
        _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
        *(
            [
                _command(
                    "signed-exchange-open-orders-position-reconciliation",
                    exchange_reconciliation(
                        disabled_config, disabled_exchange_checkpoint
                    ),
                    mutates=True,
                    after_stop=True,
                )
            ]
            if successor_process_contract
            else []
        ),
        _command("checkout-frozen-runtime", checkout_command, mutates=True, after_stop=True),
        *(
            [
                _command(
                    "select-successor-native-venv",
                    select_successor_venv,
                    mutates=True,
                    after_stop=True,
                )
            ]
            if successor_process_contract
            else []
        ),
        *(
            [install_bytes]
            if isolated_required and not successor_process_contract
            else []
        ),
        _command("start-disabled", start_disabled, mutates=True, after_stop=True),
        *(
            [
                _command(
                    "wait-disabled-exchange-ready",
                    log_validate(disabled_checkpoint),
                    mutates=False,
                    after_stop=True,
                )
            ]
            if successor_process_contract
            else []
        ),
        *(
            [process_family_probe("fresh-disabled-supervisor-child-probe")]
            if successor_process_contract
            else []
        ),
        _command(
            "fresh-disabled-process-probe",
            process_probe(
                disabled_config,
                str(configs["disabled"]["config_sha256"]),
                False,
                expected_startup_attestation_schema_version=current_startup_schema,
                expected_exchange_reconciliation_path=(
                    f"{disabled_exchange_checkpoint}.startup"
                    if successor_process_contract
                    else None
                ),
            ),
            mutates=False,
            after_stop=True,
        ),
        _command(
            "read-disabled-runtime-identity",
            runtime_identity_read,
            mutates=False,
            after_stop=True,
        ),
        *(
            [
                _command(
                    "validate-disabled-startup-log",
                    log_validate(disabled_checkpoint),
                    mutates=False,
                    after_stop=True,
                )
            ]
            if not successor_process_contract
            else []
        ),
    ]
    activate = [
        *common_pre_stop(
            active_checkpoint,
            existing_successor_family=successor_process_contract,
        ),
        _command(
            "reprobe-disabled-process-before-stop",
            process_probe(
                disabled_config,
                str(configs["disabled"]["config_sha256"]),
                False,
                expected_startup_attestation_schema_version=current_startup_schema,
                expected_exchange_reconciliation_path=(
                    f"{disabled_exchange_checkpoint}.startup"
                    if successor_process_contract
                    else None
                ),
            ),
            mutates=False,
        ),
        _command(
            "read-pre-stop-disabled-runtime-identity",
            runtime_identity_read,
            mutates=False,
        ),
        _command("stop-live", stop_disabled_successor, mutates=True, after_stop=True),
        _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
        *(
            [
                _command(
                    "signed-exchange-open-orders-position-reconciliation",
                    exchange_reconciliation(disabled_config, active_exchange_checkpoint),
                    mutates=True,
                    after_stop=True,
                )
            ]
            if successor_process_contract
            else []
        ),
        *(
            [
                _command(
                    "reselect-successor-native-venv",
                    select_successor_venv,
                    mutates=True,
                    after_stop=True,
                )
            ]
            if successor_process_contract
            else []
        ),
        *(
            [install_bytes]
            if isolated_required and not successor_process_contract
            else []
        ),
        _command("start-active-restart-only", start_active, mutates=True, after_stop=True),
        *(
            [
                _command(
                    "wait-active-exchange-ready",
                    log_validate(active_checkpoint),
                    mutates=False,
                    after_stop=True,
                )
            ]
            if successor_process_contract
            else []
        ),
        *(
            [process_family_probe("fresh-active-supervisor-child-probe")]
            if successor_process_contract
            else []
        ),
        _command(
            "fresh-active-process-probe",
            process_probe(
                active_config,
                str(configs["active"]["config_sha256"]),
                True,
                expected_startup_attestation_schema_version=current_startup_schema,
                expected_exchange_reconciliation_path=(
                    f"{active_exchange_checkpoint}.startup"
                    if successor_process_contract
                    else None
                ),
            ),
            mutates=False,
            after_stop=True,
        ),
        _command(
            "read-active-runtime-identity",
            runtime_identity_read,
            mutates=False,
            after_stop=True,
        ),
        *(
            [
                _command(
                    "validate-active-startup-log",
                    log_validate(active_checkpoint),
                    mutates=False,
                    after_stop=True,
                )
            ]
            if not successor_process_contract
            else []
        ),
    ]

    def rollback_commands(name: str, stop_command: Sequence[str]) -> list[dict[str, Any]]:
        identity = rollback[name]
        if identity.get("mode") == "stop_cancel_reconcile_only":
            return [
                _command(
                    "capture-old-pid",
                    _ssh_command(
                        target=target,
                        known_hosts=known,
                        remote_command=(
                            f"test -s {shlex.quote(pid_file)} && "
                            f"p=$(cat {shlex.quote(pid_file)}) && test -r /proc/$p/stat && "
                            "printf '%s %s\\n' \"$p\" \"$(awk '{print $22}' /proc/$p/stat)\""
                        ),
                    ),
                    mutates=False,
                ),
                _command("stop-live", stop_command, mutates=True, after_stop=True),
                _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
                *(
                    [
                        _command(
                            "signed-exchange-open-orders-position-reconciliation",
                            exchange_reconciliation(
                                disabled_config,
                                f"{checkpoint_base}.deep-rollback.exchange",
                            ),
                            mutates=True,
                            after_stop=True,
                        )
                    ]
                    if successor_process_contract
                    else []
                ),
                _command(
                    "deep-stop-reconciliation-required",
                    _ssh_command(
                        target=target,
                        known_hosts=known,
                        remote_command=(
                            "printf '%s\\n' 'live stopped; manual exchange reconciliation required'"
                        ),
                    ),
                    mutates=False,
                    after_stop=True,
                ),
            ]
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
            remote_command=(
                _remote_bound_exchange_config_start(
                    repo_root=repo_root,
                    runtime_config_path=str(identity["config_path"]),
                    reconciliation_config_path=disabled_config,
                    reconciliation_output_path=(
                        f"{rollback_exchange_checkpoint}.startup"
                    ),
                    owner_override=False,
                    external_tool_root=external_tool_root,
                    external_tool_python=external_tool_python,
                    external_script=exchange_external_script,
                    external_script_sha256=external_script_sha256,
                    external_gate=external_gate,
                    external_gate_sha256=external_gate_sha256,
                    safety_release_binding=successor_safety_binding,
                    startup_static_authority_env=successor_static_authority_env,
                    startup_static_gate=successor_static_gate,
                    trusted_static_python_path=str(host["trusted_static_python_path"]),
                )
                if successor_process_contract
                else _remote_external_config_start(
                    repo_root,
                    str(identity["config_path"]),
                    owner_override=False,
                )
            ),
        )
        rollback_successor_prerequisite: dict[str, Any] | None = None
        if successor_process_contract:
            assert successor_safety_binding is not None
            prerequisite = (
                f"{successor_static_gate} && "
                f"test -x {shlex.quote(isolated_python)} && "
                f"test -L {shlex.quote(selected_venv)} && "
                f"test \"$(readlink {shlex.quote(selected_venv)})\" = "
                f"{shlex.quote(isolated_venv)} && "
                f"test ! -e {shlex.quote(selector_temp)} && "
                f"test ! -L {shlex.quote(str(successor_safety_binding['remote_path']))} && "
                f"test -f {shlex.quote(str(successor_safety_binding['remote_path']))} && "
                f"test \"$(sha256sum {shlex.quote(str(successor_safety_binding['remote_path']))} | awk '{{print $1}}')\" = "
                f"{shlex.quote(str(successor_safety_binding['file_sha256']))} && "
                f"test \"$(sha256sum {shlex.quote(str(identity['config_path']))} | awk '{{print $1}}')\" = "
                f"{shlex.quote(str(identity['config_sha256']))}"
            )
            rollback_successor_prerequisite = _command(
                "validate-rollback-successor-prerequisites-before-stop",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=prerequisite,
                ),
                mutates=False,
            )
        return [
            *(
                [rollback_successor_prerequisite]
                if rollback_successor_prerequisite is not None
                else []
            ),
            _command(
                "capture-old-pid",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=(
                        f"test -s {shlex.quote(pid_file)} && "
                        f"p=$(cat {shlex.quote(pid_file)}) && test -r /proc/$p/stat && "
                        "printf '%s %s\\n' \"$p\" \"$(awk '{print $22}' /proc/$p/stat)\""
                    ),
                ),
                mutates=False,
            ),
            _command("stop-live", stop_command, mutates=True, after_stop=True),
            _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
            *(
                [
                    _command(
                        "signed-exchange-open-orders-position-reconciliation",
                        exchange_reconciliation(
                            str(identity["config_path"]),
                            rollback_exchange_checkpoint,
                        ),
                        mutates=True,
                        after_stop=True,
                    )
                ]
                if successor_process_contract
                else []
            ),
            _command("checkout-rollback-runtime", rollback_checkout, mutates=True, after_stop=True),
            *(
                [
                    _command(
                        "select-rollback-successor-native-venv",
                        select_successor_venv,
                        mutates=True,
                        after_stop=True,
                    )
                ]
                if successor_process_contract
                else []
            ),
            _command("start-rollback-fresh-b0", rollback_start, mutates=True, after_stop=True),
            *(
                [process_family_probe("fresh-rollback-supervisor-child-probe")]
                if successor_process_contract
                else []
            ),
            _command(
                "fresh-rollback-process-probe",
                process_probe(
                    str(identity["config_path"]),
                    str(identity["config_sha256"]),
                    False,
                    expected_execution=identity,
                    expected_runtime_code_sha256=str(identity["runtime_code_sha256"]),
                    expected_runtime_sources=_rollback_runtime_source_authority(
                        identity=identity,
                        current_execution=execution,
                        current_runtime_sources=runtime_sources,
                    ),
                    expected_artifact_sha256=str(identity.get("artifact_sha256", "")),
                    expected_startup_attestation_schema_version=(
                        _rollback_startup_attestation_schema(
                            identity=identity,
                            current_execution=execution,
                        )
                    ),
                    expected_exchange_reconciliation_path=(
                        f"{rollback_exchange_checkpoint}.startup"
                        if successor_process_contract
                        else None
                    ),
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


def _validate_active_release_phase_binding(
    raw: Mapping[str, Any], *, plan: Mapping[str, Any]
) -> dict[str, Any]:
    schema_version = str(raw.get("schema_version", ""))
    expected_identity, expected_status = _active_release_contract(schema_version)
    config_bound = schema_version in {
        buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
        buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
    }
    expected_fields = (
        _SUCCESSOR_ACTIVE_RELEASE_PHASE_BINDING_FIELDS
        if schema_version == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
        else _ACTIVE_RELEASE_PHASE_BINDING_FIELDS
        if config_bound
        else _LEGACY_ACTIVE_RELEASE_PHASE_BINDING_FIELDS
    )
    if set(raw) != expected_fields:
        raise BuyE3TransactionalDeployError("active release phase binding fields drifted")
    binding = dict(raw)
    expected_remote_path = _remote_active_release_path(
        str(plan["active_pointer"]["repo_root"]),
        str(binding.get("file_sha256", "")),
    )
    local_path = Path(str(binding.get("local_path", ""))).expanduser().absolute()
    if (
        not local_path.is_absolute()
        or binding.get("remote_path") != expected_remote_path
        or binding.get("schema_version") != schema_version
        or binding.get("status") != expected_status
    ):
        raise BuyE3TransactionalDeployError("active release phase binding identity drifted")
    if expected_identity != schema_version:
        raise BuyE3TransactionalDeployError("active release phase identity drifted")
    binding["file_sha256"] = _require_sha256(binding.get("file_sha256"), "active release file hash")
    binding["canonical_active_release_sha256"] = _require_sha256(
        binding.get("canonical_active_release_sha256"),
        "active release canonical hash",
    )
    if config_bound:
        binding["active_config_file_sha256"] = _require_sha256(
            binding.get("active_config_file_sha256"),
            "active release embedded active config hash",
        )
        binding["disabled_config_file_sha256"] = _require_sha256(
            binding.get("disabled_config_file_sha256"),
            "active release embedded disabled config hash",
        )
        if (
            binding["active_config_file_sha256"]
            != plan["configs"]["active"]["config_sha256"]
            or binding["disabled_config_file_sha256"]
            != plan["configs"]["disabled"]["config_sha256"]
        ):
            raise BuyE3TransactionalDeployError(
                "active release phase config binding drifted"
            )
    if schema_version == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA:
        for field in (
            "native_build_receipt_sha256",
            "native_build_receipt_canonical_sha256",
            "native_module_sha256",
            "native_wheel_sha256",
            "runtime_lock_file_sha256",
            "runtime_lock_canonical_sha256",
            "wheelhouse_manifest_file_sha256",
            "wheelhouse_canonical_sha256",
            "install_receipt_file_sha256",
            "install_receipt_canonical_sha256",
            "root_wheel_sha256",
            "installed_record_aggregate_sha256",
        ):
            binding[field] = _require_sha256(binding.get(field), field)
        interpreter = binding.get("locked_runtime_interpreter")
        stage_root = PurePosixPath(str(plan["remote"]["stage_root"]))
        commit = str(plan["execution"]["execution_commit"])
        runtime_lock_path = PurePosixPath(str(binding.get("runtime_lock_path", "")))
        wheelhouse_path = PurePosixPath(str(binding.get("wheelhouse_path", "")))
        install_receipt_path = PurePosixPath(
            str(binding.get("install_receipt_path", ""))
        )
        root_wheel_path = PurePosixPath(str(binding.get("root_wheel_path", "")))
        native_wheel_path = PurePosixPath(
            str(binding.get("native_wheel_path", ""))
        )
        if (
            not str(binding.get("native_soabi", "")).startswith("cpython-312-")
            or any(
                not PurePosixPath(str(binding.get(field, ""))).is_absolute()
                for field in (
                    "runtime_lock_path",
                    "wheelhouse_path",
                    "install_receipt_path",
                    "root_wheel_path",
                    "native_wheel_path",
                )
            )
            or not isinstance(interpreter, Mapping)
            or set(interpreter) != _LOCKED_RUNTIME_INTERPRETER_FIELDS
            or interpreter.get("soabi") != binding.get("native_soabi")
            or plan["host"].get("trusted_static_python_sha256")
            not in {
                interpreter.get("executable_sha256"),
                interpreter.get("base_executable_sha256"),
            }
            or runtime_lock_path != stage_root / f"runtime-lock-{commit}.json"
            or wheelhouse_path
            != stage_root / f"wheelhouse-{binding['runtime_lock_canonical_sha256']}"
            or install_receipt_path
            != stage_root / f"locked-runtime-install-{commit}.json"
            or root_wheel_path.parent != stage_root / f"root-wheel-{commit}"
            or root_wheel_path.suffix != ".whl"
            or native_wheel_path.parent != stage_root / f"native-wheel-{commit}"
            or native_wheel_path.suffix != ".whl"
        ):
            raise BuyE3TransactionalDeployError("active release phase native SOABI drifted")
        binding["locked_runtime_interpreter"] = dict(interpreter)
        if (
            binding["remote_path"] != plan["remote"].get("safety_release_path")
            or binding["file_sha256"]
            != plan["remote"].get("safety_release_file_sha256")
            or binding["canonical_active_release_sha256"]
            != plan["remote"].get("safety_release_canonical_sha256")
        ):
            raise BuyE3TransactionalDeployError(
                "successor release differs from the plan-time safety authority"
            )
    return binding


def _activation_rows_with_active_release(
    plan: Mapping[str, Any],
    active_release_binding: Mapping[str, Any],
    *,
    phase: str = "activate",
) -> list[dict[str, Any]]:
    """Derive post-envelope authority commands without changing the frozen plan."""

    binding = _validate_active_release_phase_binding(active_release_binding, plan=plan)
    if phase not in {"disabled-deploy", "activate"}:
        raise BuyE3TransactionalDeployError("safety release can bind only deployment phases")
    base_rows = [dict(row) for row in plan["phases"][phase]]
    package = plan["external_tools_and_package"]
    package_files = package["files"]
    stage = f"{plan['remote']['stage_root']}/package-{package['content_package_sha256']}"
    release_stage_path = (
        f"{plan['remote']['stage_root']}/active-release-{binding['file_sha256']}.json"
    )
    target = str(plan["active_pointer"]["ssh_target"])
    known_hosts = str(plan["ssh"]["path"])
    repo_root = str(plan["active_pointer"]["repo_root"])
    python = str(plan["host"]["python_executable"])
    successor_release = (
        binding["schema_version"]
        == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
    )
    external_tool_root = (
        f"{plan['remote']['stage_root']}/runtime-"
        f"{plan['execution']['execution_commit']}"
        if successor_release
        else repo_root
    )
    external_tool_python = (
        f"{plan['remote']['stage_root']}/venv-"
        f"{plan['execution']['execution_commit']}/bin/python3"
        if successor_release
        else python
    )
    exchange_external_script = (
        f"{external_tool_root}/scripts/deploy_f05_buy_e3_owner_v1.py"
        if successor_release
        else ""
    )
    startup_static_payload: dict[str, Any] | None = None
    startup_static_binding: dict[str, str] | None = None
    startup_static_env = ""
    if successor_release:
        startup_static_payload, startup_static_binding = (
            _startup_static_authority_from_release(
                repo_root=repo_root,
                execution=plan["execution"],
                runtime_sources=plan["runtime_sources"],
                host=plan["host"],
                remote=plan["remote"],
                release_binding=binding,
            )
        )
        planned_static_binding = {
            "remote_path": str(
                plan["remote"]["startup_static_runtime_authority_path"]
            ),
            "file_sha256": str(
                plan["remote"]["startup_static_runtime_authority_file_sha256"]
            ),
            "canonical_sha256": str(
                plan["remote"][
                    "startup_static_runtime_authority_canonical_sha256"
                ]
            ),
        }
        if startup_static_binding != planned_static_binding:
            raise BuyE3TransactionalDeployError(
                "startup static authority differs from the frozen plan/release"
            )
        startup_static_env = _startup_static_authority_env(
            startup_static_payload,
            startup_static_binding,
        )

    def staged_path(role: str) -> str:
        local_path = Path(str(package_files[role]["path"]))
        return f"{stage}/{role}-{package_files[role]['file_sha256']}{local_path.suffix}"

    external_script = staged_path("deploy_script")
    external_gate = staged_path("gate_amendment")
    external_script_sha256 = str(package_files["deploy_script"]["file_sha256"])
    external_gate_sha256 = str(package_files["gate_amendment"]["file_sha256"])
    transfer = _command(
        "stage-active-release",
        _rsync_command(
            source=str(binding["local_path"]),
            target=target,
            known_hosts=known_hosts,
            destination=release_stage_path,
        ),
        mutates=True,
    )
    validate_stage = _command(
        "validate-and-freeze-active-release-stage",
        _ssh_command(
            target=target,
            known_hosts=known_hosts,
            remote_command=(
                f"test ! -L {shlex.quote(release_stage_path)} && "
                f"test -f {shlex.quote(release_stage_path)} && "
                f"(test \"$(stat -c '%a' {shlex.quote(release_stage_path)})\" = 600 || "
                f"test \"$(stat -c '%a' {shlex.quote(release_stage_path)})\" = 400) && "
                f"test \"$(stat -c '%h' {shlex.quote(release_stage_path)})\" = 1 && "
                f'test "$(sha256sum {shlex.quote(release_stage_path)} | '
                f"awk '{{print $1}}')\" = {shlex.quote(str(binding['file_sha256']))} && "
                f"chmod 400 {shlex.quote(release_stage_path)}"
            ),
        ),
        mutates=True,
    )
    native_build_validate: dict[str, Any] | None = None
    static_tree_validate: dict[str, Any] | None = None
    startup_static_install: dict[str, Any] | None = None
    stale_bytecode_cleanup: dict[str, Any] | None = None
    full_static_tree_command = ""
    if binding["schema_version"] == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA:
        isolated_python = (
            f"{plan['remote']['stage_root']}/venv-"
            f"{plan['execution']['execution_commit']}/bin/python3"
        )
        native_receipt = (
            f"{plan['remote']['stage_root']}/native-build-"
            f"{plan['execution']['execution_commit']}.json"
        )
        locked_script_relative = "scripts/f05_live_safety_locked_runtime.py"
        locked_source = plan["runtime_sources"]["files"].get(
            locked_script_relative
        )
        if not isinstance(locked_source, Mapping):
            raise BuyE3TransactionalDeployError(
                "successor plan lacks locked runtime verifier source authority"
            )
        locked_script_sha256 = _require_sha256(
            locked_source.get("working_file_sha256"),
            "locked runtime verifier source",
        )
        locked_script = f"{external_tool_root}/{locked_script_relative}"
        if startup_static_payload is None or startup_static_binding is None:
            raise BuyE3TransactionalDeployError(
                "successor startup static authority was not constructed"
            )
        static_verifier = str(
            startup_static_payload["authority_verifier"]["path"]
        )
        static_verifier_sha256 = str(
            startup_static_payload["authority_verifier"]["sha256"]
        )
        static_authority_path = str(startup_static_binding["remote_path"])
        static_authority_raw = startup_static_authority.file_bytes(
            startup_static_payload
        )
        static_authority_base64 = base64.b64encode(static_authority_raw).decode(
            "ascii"
        )
        static_authority_temp = f"{static_authority_path}.next-{str(binding['file_sha256'])[:12]}"
        install_static_command = (
            "set -eu; "
            f"test ! -L {shlex.quote(static_verifier)}; "
            f"test -f {shlex.quote(static_verifier)}; "
            f"test \"$(sha256sum {shlex.quote(static_verifier)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(static_verifier_sha256)}; "
            f"test ! -L {shlex.quote(locked_script)}; "
            f"test -f {shlex.quote(locked_script)}; "
            f"test \"$(sha256sum {shlex.quote(locked_script)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(locked_script_sha256)}; "
            f"chmod 444 {shlex.quote(static_verifier)} {shlex.quote(locked_script)}; "
            f"test \"$(stat -c '%a' {shlex.quote(static_verifier)})\" = 444; "
            f"test \"$(stat -c '%a' {shlex.quote(locked_script)})\" = 444; "
            f"test ! -e {shlex.quote(static_authority_temp)}; "
            f"if test ! -e {shlex.quote(static_authority_path)}; then "
            "umask 077; "
            f"trap 'rm -f {shlex.quote(static_authority_temp)}' EXIT HUP INT TERM; "
            f"printf '%s' {shlex.quote(static_authority_base64)} | base64 -d > "
            f"{shlex.quote(static_authority_temp)}; "
            f"test \"$(sha256sum {shlex.quote(static_authority_temp)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(startup_static_binding['file_sha256']))}; "
            f"chmod 400 {shlex.quote(static_authority_temp)}; "
            f"mv -n {shlex.quote(static_authority_temp)} {shlex.quote(static_authority_path)}; "
            f"if test -e {shlex.quote(static_authority_temp)}; then "
            f"cmp -s {shlex.quote(static_authority_temp)} {shlex.quote(static_authority_path)}; "
            f"rm -f {shlex.quote(static_authority_temp)}; fi; "
            "trap - EXIT HUP INT TERM; "
            "fi; "
            f"test ! -L {shlex.quote(static_authority_path)}; "
            f"test -f {shlex.quote(static_authority_path)}; "
            f"test \"$(stat -c '%a' {shlex.quote(static_authority_path)})\" = 400; "
            f"test \"$(stat -c '%h' {shlex.quote(static_authority_path)})\" = 1; "
            f"test \"$(sha256sum {shlex.quote(static_authority_path)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(startup_static_binding['file_sha256']))}"
        )
        startup_static_install = _command(
            "install-successor-startup-static-runtime-authority",
            _ssh_command(
                target=target,
                known_hosts=known_hosts,
                remote_command=install_static_command,
            ),
            mutates=True,
            after_stop=False,
        )
        wheelhouse_manifest = (
            f"{binding['wheelhouse_path']}/{locked_runtime.WHEELHOUSE_MANIFEST}"
        )
        frozen_python_sha256 = str(
            binding["locked_runtime_interpreter"]["executable_sha256"]
        )
        command = (
            "export PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 && "
            f"test \"$(sha256sum {shlex.quote(locked_script)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(locked_script_sha256)} && "
            f"test ! -L {shlex.quote(isolated_python)} && "
            f"test -f {shlex.quote(isolated_python)} && "
            f"test \"$(stat -c '%h' {shlex.quote(isolated_python)})\" = 1 && "
            f"test \"$(sha256sum {shlex.quote(isolated_python)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(frozen_python_sha256)} && "
            f"test ! -L {shlex.quote(native_receipt)} && "
            f"test -f {shlex.quote(native_receipt)} && "
            f"test \"$(sha256sum {shlex.quote(native_receipt)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(binding['native_build_receipt_sha256']))} && "
            f"test \"$(sha256sum {shlex.quote(str(binding['runtime_lock_path']))} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(binding['runtime_lock_file_sha256']))} && "
            f"test \"$(sha256sum {shlex.quote(wheelhouse_manifest)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(binding['wheelhouse_manifest_file_sha256']))} && "
            f"test \"$(sha256sum {shlex.quote(str(binding['install_receipt_path']))} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(binding['install_receipt_file_sha256']))} && "
            f"test \"$(sha256sum {shlex.quote(str(binding['root_wheel_path']))} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(binding['root_wheel_sha256']))} && "
            f"test \"$(sha256sum {shlex.quote(str(binding['native_wheel_path']))} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(binding['native_wheel_sha256']))} && "
            f"{shlex.quote(isolated_python)} -B {shlex.quote(locked_script)} verify-install "
            f"--builder-python {shlex.quote(isolated_python)} "
            f"--venv {shlex.quote(str(PurePosixPath(isolated_python).parent.parent))} "
            f"--lock {shlex.quote(str(binding['runtime_lock_path']))} "
            f"--expected-lock-sha256 {shlex.quote(str(binding['runtime_lock_canonical_sha256']))} "
            f"--wheelhouse {shlex.quote(str(binding['wheelhouse_path']))} "
            f"--expected-wheelhouse-sha256 {shlex.quote(str(binding['wheelhouse_canonical_sha256']))} "
            f"--root-wheel {shlex.quote(str(binding['root_wheel_path']))} "
            f"--root-wheel-sha256 {shlex.quote(str(binding['root_wheel_sha256']))} "
            f"--native-wheel {shlex.quote(str(binding['native_wheel_path']))} "
            f"--native-wheel-sha256 {shlex.quote(str(binding['native_wheel_sha256']))} "
            f"--receipt {shlex.quote(str(binding['install_receipt_path']))} "
            f"--expected-receipt-sha256 {shlex.quote(str(binding['install_receipt_canonical_sha256']))} && "
            f"module=$({shlex.quote(isolated_python)} -B -c "
            "'import narrowgate_cpp; print(narrowgate_cpp.__file__)') && "
            "test -f \"$module\" && "
            "test \"$(sha256sum \"$module\" | awk '{print $1}')\" = "
            f"{shlex.quote(str(binding['native_module_sha256']))} && "
            f"test \"$({shlex.quote(isolated_python)} -B -c "
            "'import sysconfig; print(sysconfig.get_config_var(\"SOABI\"))')\" = "
            f"{shlex.quote(str(binding['native_soabi']))}"
        )
        native_build_validate = _command(
            "validate-successor-native-build-before-stop",
            _ssh_command(
                target=target,
                known_hosts=known_hosts,
                remote_command=command,
            ),
            mutates=False,
        )
        static_tree_command = (
            f"test \"$(sha256sum {shlex.quote(static_verifier)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(static_verifier_sha256)} && "
            f"test \"$(stat -c '%a' {shlex.quote(static_verifier)})\" = 444 && "
            f"test \"$(sha256sum {shlex.quote(locked_script)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(locked_script_sha256)} && "
            f"test \"$(stat -c '%a' {shlex.quote(locked_script)})\" = 444 && "
            f"test ! -L {shlex.quote(str(plan['host']['trusted_static_python_path']))} && "
            f"test -f {shlex.quote(str(plan['host']['trusted_static_python_path']))} && "
            f"test \"$(stat -c '%h' {shlex.quote(str(plan['host']['trusted_static_python_path']))})\" = 1 && "
            f"test \"$(sha256sum {shlex.quote(str(plan['host']['trusted_static_python_path']))} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(plan['host']['trusted_static_python_sha256']))} && "
            f"test ! -L {shlex.quote(static_authority_path)} && "
            f"test -f {shlex.quote(static_authority_path)} && "
            f"test \"$(stat -c '%a' {shlex.quote(static_authority_path)})\" = 400 && "
            f"test \"$(stat -c '%h' {shlex.quote(static_authority_path)})\" = 1 && "
            f"test \"$(sha256sum {shlex.quote(static_authority_path)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(str(startup_static_binding['file_sha256']))} && "
            "export PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 && "
            f"{shlex.quote(str(plan['host']['trusted_static_python_path']))} "
            f"-I -B -S {shlex.quote(static_verifier)} "
            f"--authority {shlex.quote(static_authority_path)} "
            f"--expected-file-sha256 {shlex.quote(str(startup_static_binding['file_sha256']))} "
            f"--expected-canonical-sha256 "
            f"{shlex.quote(str(startup_static_binding['canonical_sha256']))}"
        )
        full_static_tree_command = _clean_static_gate_command(static_tree_command)
        static_tree_command = _clean_static_gate_command(
            f"{static_tree_command} --candidate-only"
        )
        static_tree_validate = _command(
            "validate-successor-static-runtime-tree-before-target-python",
            _ssh_command(
                target=target,
                known_hosts=known_hosts,
                remote_command=static_tree_command,
            ),
            mutates=False,
        )
        cleanup_fragments = []
        for cleanup_root in (repo_root, external_tool_root):
            cleanup_fragments.append(
                f"test ! -L {shlex.quote(cleanup_root)} && "
                f"/usr/bin/find {shlex.quote(cleanup_root)} -type f "
                "\\( -name '*.pyc' -o -name '*.pyo' \\) -delete && "
                f"/usr/bin/find {shlex.quote(cleanup_root)} -depth -type d "
                "-name __pycache__ -empty -delete"
            )
        stale_bytecode_cleanup = _command(
            "remove-stale-successor-bytecode-caches",
            _ssh_command(
                target=target,
                known_hosts=known_hosts,
                remote_command=" && ".join(cleanup_fragments),
            ),
            mutates=True,
        )
    if successor_release:
        release_destination = str(binding["remote_path"])
        release_parent = str(PurePosixPath(release_destination).parent)
        release_temp = (
            f"{release_destination}.next-{str(binding['file_sha256'])[:16]}"
        )
        install_command = _clean_remote_shell_command(
            "set -eu; umask 077; "
            f"test ! -L {shlex.quote(release_stage_path)}; "
            f"test -f {shlex.quote(release_stage_path)}; "
            f"test \"$(/usr/bin/sha256sum {shlex.quote(release_stage_path)} | "
            f"/usr/bin/awk '{{print $1}}')\" = {shlex.quote(str(binding['file_sha256']))}; "
            f"/usr/bin/install -d -m 700 {shlex.quote(release_parent)}; "
            f"test ! -e {shlex.quote(release_temp)}; "
            f"trap '/usr/bin/rm -f -- {shlex.quote(release_temp)}' EXIT HUP INT TERM; "
            f"/usr/bin/install -m 600 {shlex.quote(release_stage_path)} "
            f"{shlex.quote(release_temp)}; "
            f"if /usr/bin/ln {shlex.quote(release_temp)} "
            f"{shlex.quote(release_destination)} 2>/dev/null; then :; "
            f"else test ! -L {shlex.quote(release_destination)} && "
            f"test -f {shlex.quote(release_destination)} && "
            f"/usr/bin/cmp -s {shlex.quote(release_temp)} "
            f"{shlex.quote(release_destination)}; fi; "
            f"/usr/bin/rm -f -- {shlex.quote(release_temp)}; "
            "trap - EXIT HUP INT TERM; "
            f"test ! -L {shlex.quote(release_destination)}; "
            f"test -f {shlex.quote(release_destination)}; "
            f"test \"$(/usr/bin/stat -c '%a' {shlex.quote(release_destination)})\" = 600; "
            f"test \"$(/usr/bin/stat -c '%h' {shlex.quote(release_destination)})\" = 1; "
            f"test \"$(/usr/bin/sha256sum {shlex.quote(release_destination)} | "
            f"/usr/bin/awk '{{print $1}}')\" = {shlex.quote(str(binding['file_sha256']))}"
        )
    else:
        install_command = _verified_external_exec(
            command=(
                f"cd {shlex.quote(external_tool_root)} && env {_release_env_unsets()} "
                "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "
                f"PYTHONPATH={shlex.quote(external_tool_root)} "
                f"{shlex.quote(external_tool_python)} "
                f"{shlex.quote(external_script)} install-active-release --source "
                f"{shlex.quote(release_stage_path)} --destination "
                f"{shlex.quote(str(binding['remote_path']))} --file-sha256 "
                f"{shlex.quote(str(binding['file_sha256']))}"
            ),
            external_script=external_script,
            external_script_sha256=external_script_sha256,
            external_gate=external_gate,
            external_gate_sha256=external_gate_sha256,
        )
    install = _command(
        "install-private-active-release",
        _ssh_command(
            target=target,
            known_hosts=known_hosts,
            remote_command=install_command,
        ),
        mutates=True,
        after_stop=False,
    )
    start = _command(
        "start-active-restart-only" if phase == "activate" else "start-disabled",
        _ssh_command(
            target=target,
            known_hosts=known_hosts,
            remote_command=(
                _remote_bound_exchange_config_start(
                    repo_root=repo_root,
                    runtime_config_path=str(
                        plan["remote"][
                            "active_config_path"
                            if phase == "activate"
                            else "disabled_config_path"
                        ]
                    ),
                    reconciliation_config_path=str(
                        plan["remote"]["disabled_config_path"]
                    ),
                    reconciliation_output_path=(
                        f"{plan['remote']['startup_checkpoint_path']}."
                        f"{'active' if phase == 'activate' else 'disabled'}."
                        "exchange.startup"
                    ),
                    owner_override=phase == "activate",
                    external_tool_root=external_tool_root,
                    external_tool_python=external_tool_python,
                    external_script=exchange_external_script,
                    external_script_sha256=external_script_sha256,
                    external_gate=external_gate,
                    external_gate_sha256=external_gate_sha256,
                    active_release_binding=(binding if phase == "activate" else None),
                    safety_release_binding=binding,
                    startup_static_authority_env=startup_static_env,
                    startup_static_gate=full_static_tree_command,
                    trusted_static_python_path=str(
                        plan["host"]["trusted_static_python_path"]
                    ),
                )
                if successor_release
                else _remote_external_config_start(
                    repo_root,
                    str(
                        plan["remote"][
                            "active_config_path"
                            if phase == "activate"
                            else "disabled_config_path"
                        ]
                    ),
                    owner_override=phase == "activate",
                    active_release_binding=(binding if phase == "activate" else None),
                    safety_release_binding=binding,
                    startup_static_authority_env=startup_static_env,
                )
            ),
        ),
        mutates=True,
        after_stop=True,
    )

    rows: list[dict[str, Any]] = []
    for row in base_rows:
        label = str(row["label"])
        if label == "validate-prebuilt-successor-runtime" and static_tree_validate is not None:
            if startup_static_install is None:
                raise BuyE3TransactionalDeployError(
                    "successor startup static authority install is unavailable"
                )
            rows.extend((transfer, validate_stage, install))
            rows.append(startup_static_install)
            if stale_bytecode_cleanup is not None:
                rows.append(stale_bytecode_cleanup)
            rows.append(static_tree_validate)
        if label == "startup-log-checkpoint":
            if not successor_release:
                rows.extend((transfer, validate_stage))
            if native_build_validate is not None:
                rows.append(native_build_validate)
            if not successor_release:
                rows.append(install)
        if label == ("start-active-restart-only" if phase == "activate" else "start-disabled"):
            rows.append(start)
            continue
        if label == "fresh-active-process-probe" and phase == "activate":
            argv = list(row["argv"])
            active_args = (
                f"--active-release {shlex.quote(str(binding['remote_path']))} "
                f"--active-release-file-sha256 "
                f"{shlex.quote(str(binding['file_sha256']))} "
                f"--active-release-canonical-sha256 "
                f"{shlex.quote(str(binding['canonical_active_release_sha256']))}"
            )
            if successor_release:
                if argv[-1].count(_ACTIVE_RELEASE_PROBE_ARGS_PLACEHOLDER) != 1:
                    raise BuyE3TransactionalDeployError(
                        "successor active process probe placeholder drifted"
                    )
                argv[-1] = argv[-1].replace(
                    _ACTIVE_RELEASE_PROBE_ARGS_PLACEHOLDER,
                    active_args,
                )
            else:
                argv[-1] = f"{argv[-1]} {active_args}"
            rows.append(
                _command(
                    label,
                    argv,
                    mutates=bool(row["mutates_remote"]),
                    after_stop=bool(row["after_stop"]),
                )
            )
            continue
        rows.append(row)
    return rows


def _execution_rows(
    plan: Mapping[str, Any],
    phase: str,
    active_release_binding: Mapping[str, Any] | None,
) -> list[Mapping[str, Any]]:
    if phase in {"disabled-deploy", "activate"} and active_release_binding is not None:
        return _activation_rows_with_active_release(
            plan, active_release_binding, phase=phase
        )
    return list(plan["phases"][phase])


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
    compatible_loaded = _load_compatible_attempt(
        repository_root=root,
        raw=specification.get("compatible_execution_attempt"),
    )
    if compatible_loaded is None:
        attempt_payload = None
        execution = gate_v2.verify_execution_git_identity(
            repository_root=root,
            expected_commit=str(execution_raw["commit"]),
            expected_tree=str(execution_raw["tree"]),
            annotated_tag=str(execution_raw["annotated_tag"]),
            expected_tag_object=str(execution_raw["annotated_tag_object"]),
        )
        runtime_sources_from_attempt = None
    else:
        attempt_payload, attempt_binding = compatible_loaded
        execution = _compatible_execution_identity(
            specification_execution=execution_raw,
            attempt_payload=attempt_payload,
            attempt_binding=attempt_binding,
        )
        runtime_sources_from_attempt = _compatible_runtime_sources(
            attempt_payload,
            repository_root=root,
        )
    manifest_path = Path(str(artifact_raw["manifest_path"])).expanduser().resolve(strict=True)
    policy_path = Path(str(artifact_raw["policy_path"])).expanduser().resolve(strict=True)
    bundle_path = Path(str(artifact_raw["predicate_bundle_path"])).expanduser().resolve(strict=True)
    _read_json(manifest_path)
    policy_payload = _read_json(policy_path)
    expected_producer_commit = (
        gate_v2.FROZEN_EXECUTION_COMMIT
        if _is_successor_execution(execution)
        else execution["execution_commit"]
        if attempt_payload is None
        else attempt_payload["artifact_producer_execution"]["execution_commit"]
    )
    if policy_payload.get("bindings", {}).get("owner_execution_commit") != expected_producer_commit:
        raise BuyE3TransactionalDeployError("policy artifact binds another execution commit")
    runtime_sources = (
        (
            _successor_runtime_sources(
                repository_root=root,
                execution_commit=execution["execution_commit"],
            )
            if _is_successor_execution(execution)
            else _current_runtime_sources(
                repository_root=root,
                execution_commit=execution["execution_commit"],
            )
        )
        if runtime_sources_from_attempt is None
        else runtime_sources_from_attempt
    )
    disabled_config = Path(str(configs_raw["disabled_path"])).expanduser().resolve(strict=True)
    active_config = Path(str(configs_raw["active_path"])).expanduser().resolve(strict=True)
    validate_b0_config_contract(disabled_config)
    validate_b0_config_contract(active_config)
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
    successor_process_contract = _is_successor_execution(execution)
    if successor_process_contract:
        if (
            not str(host.get("current_venv_selector_target", "")).strip()
            or not PurePosixPath(
                str(host.get("trusted_static_python_path", ""))
            ).is_absolute()
        ):
            raise BuyE3TransactionalDeployError(
                "successor host identity lacks current selector/static interpreter"
            )
        _require_sha256(
            host.get("trusted_static_python_sha256"),
            "successor trusted static Python hash",
        )
        for field in ("supervisor_pid_file", "maker_child_pid_file"):
            if not str(remote.get(field, "")).startswith("/"):
                raise BuyE3TransactionalDeployError(
                    f"successor remote path is not absolute: {field}"
                )
        if remote["supervisor_pid_file"] == remote["maker_child_pid_file"]:
            raise BuyE3TransactionalDeployError(
                "successor supervisor and child PID files must differ"
            )
        if not str(remote.get("safety_release_path", "")).startswith("/"):
            raise BuyE3TransactionalDeployError(
                "successor remote safety release path is not absolute"
            )
        _require_sha256(
            remote.get("safety_release_file_sha256"),
            "successor remote safety release file hash",
        )
        _require_sha256(
            remote.get("safety_release_canonical_sha256"),
            "successor remote safety release canonical hash",
        )
        authority_path = PurePosixPath(
            str(remote.get("startup_static_runtime_authority_path", ""))
        )
        authority_file_sha256 = _require_sha256(
            remote.get("startup_static_runtime_authority_file_sha256"),
            "successor startup static authority file hash",
        )
        _require_sha256(
            remote.get("startup_static_runtime_authority_canonical_sha256"),
            "successor startup static authority canonical hash",
        )
        if authority_path != (
            PurePosixPath(str(remote["stage_root"]))
            / f"startup-static-runtime-authority-{authority_file_sha256}.json"
        ):
            raise BuyE3TransactionalDeployError(
                "successor startup static authority path drifted"
            )
    startup_markers = remote.get("startup_markers")
    if (
        not isinstance(startup_markers, list)
        or not startup_markers
        or any(not str(marker).strip() for marker in startup_markers)
    ):
        raise BuyE3TransactionalDeployError("remote startup markers are not frozen")
    if successor_process_contract and not SUCCESSOR_READINESS_MARKERS.issubset(
        {str(marker) for marker in startup_markers}
    ):
        raise BuyE3TransactionalDeployError(
            "successor startup markers lack the fixed exchange/readiness contract"
        )
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
        False: _host_bound_spool_gate(disabled_config, defer_host_bound_spool=True),
        True: _host_bound_spool_gate(active_config, defer_host_bound_spool=True),
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
    if attempt_payload is not None:
        admitted_artifact = attempt_payload.get("artifact")
        admitted_files = (
            admitted_artifact.get("files") if isinstance(admitted_artifact, Mapping) else None
        )
        observed_files = {
            "manifest": artifact_binding["manifest_file_sha256"],
            "policy": artifact_binding["policy_file_sha256"],
            "predicate_bundle": artifact_binding["predicate_bundle_file_sha256"],
        }
        if (
            not isinstance(admitted_files, Mapping)
            or admitted_artifact.get("artifact_sha256") != artifact_binding["artifact_sha256"]
            or any(
                admitted_files.get(role, {}).get("file_sha256") != sha256
                for role, sha256 in observed_files.items()
            )
        ):
            raise BuyE3TransactionalDeployError(
                "deployment artifact differs from compatible attempt binding"
            )
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
        preflights = [
            index
            for index, row in enumerate(rows)
            if row["label"] in {"isolated-disabled-preflight", "isolated-active-preflight"}
        ]
        if phase in {"disabled-deploy", "activate"} and (
            len(stop_positions) != 1 or len(preflights) != 2 or max(preflights) >= stop_positions[0]
        ):
            raise BuyE3TransactionalDeployError("both isolated preflights must precede stop")
        for row in rows:
            argv = row["argv"]
            if argv[0] not in {"ssh", "rsync"} or "StrictHostKeyChecking=yes" not in " ".join(argv):
                raise BuyE3TransactionalDeployError("remote command lacks strict SSH")
    plan: dict[str, Any] = {
        "schema_version": (
            SUCCESSOR_PLAN_SCHEMA
            if _is_successor_execution(execution)
            else PLAN_SCHEMA
            if attempt_payload is None
            else COMPATIBLE_PLAN_SCHEMA
        ),
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
        "runtime_attestation_contract": _runtime_attestation_contract(
            remote,
            startup_schema_version=(
                SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
                if _is_successor_execution(execution)
                else STARTUP_ATTESTATION_SCHEMA
            ),
        ),
        "evidence_boundary": dict(PLAN_EVIDENCE_BOUNDARY),
    }
    plan["plan_core_sha256"] = _plan_core_sha256(plan)
    activation_gate_raw = specification.get("activation_gate")
    if activation_gate_raw is not None:
        if attempt_payload is not None:
            raise BuyE3TransactionalDeployError(
                "compatible execution activation must use a post-disabled activation envelope"
            )
        if not isinstance(activation_gate_raw, Mapping):
            raise BuyE3TransactionalDeployError("activation gate binding is malformed")
        activation_path = Path(str(activation_gate_raw.get("path", ""))).expanduser()
        expected_file_sha = _require_sha256(
            activation_gate_raw.get("file_sha256"), "activation gate file hash"
        )
        if gate_v2.file_sha256(activation_path.resolve(strict=True)) != expected_file_sha:
            raise BuyE3TransactionalDeployError("activation gate file hash drifted")
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
        activation_kind = str(activation_gate_raw.get("kind", "")).strip()
        if activation_kind:
            if attempt_payload is None or activation_kind != _COMPATIBLE_ACTIVATION_GATE_KIND:
                raise BuyE3TransactionalDeployError(
                    "activation gate kind is incompatible with this execution"
                )
            activation_receipt = gate_v1.validate_deployment_gate_receipt(
                activation_path,
                expected_artifact_sha256=str(artifact_binding["artifact_sha256"]),
            )
            if (
                activation_receipt.get("execution_commit") != execution["execution_commit"]
                or activation_receipt.get("execution_tag") != execution["annotated_tag"]
            ):
                raise BuyE3TransactionalDeployError(
                    "generic deployment gate binds another execution"
                )
            canonical_receipt_sha256 = activation_receipt[
                "canonical_deployment_gate_receipt_sha256"
            ]
            cross_binding_sha256 = gate_v2.canonical_sha256(expected_cross_binding)
        else:
            if attempt_payload is not None:
                raise BuyE3TransactionalDeployError(
                    "compatible execution requires a generic v1 activation gate"
                )
            activation_receipt = gate_v2.validate_amended_gate_receipt(activation_path)
            canonical_receipt_sha256 = activation_receipt["canonical_amendment_receipt_sha256"]
            cross_binding_sha256 = _require_activation_gate_cross_binding(
                activation_receipt, expected_cross_binding
            )
        activation_gate_binding: dict[str, Any] = {
            "path": str(activation_path.resolve(strict=True)),
            "file_sha256": expected_file_sha,
            "canonical_receipt_sha256": canonical_receipt_sha256,
            "cross_binding_sha256": cross_binding_sha256,
            "plan_core_sha256": plan["plan_core_sha256"],
            "transaction_contract_sha256": gate_v2.canonical_sha256(plan["transaction_contract"]),
        }
        if activation_kind:
            activation_gate_binding["kind"] = activation_kind
        activation_gate_binding["canonical_activation_binding_sha256"] = gate_v2.document_sha256(
            activation_gate_binding, "canonical_activation_binding_sha256"
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
    execution = plan.get("execution")
    if not isinstance(execution, Mapping):
        raise BuyE3TransactionalDeployError("deployment execution identity is malformed")
    compatible_binding = execution.get("compatible_attempt_manifest")
    compatible = compatible_binding is not None
    successor = _is_successor_execution(execution)
    if successor and compatible:
        raise BuyE3TransactionalDeployError(
            "operational safety successor cannot use a compatible-attempt plan"
        )
    if compatible and has_activation:
        raise BuyE3TransactionalDeployError(
            "compatible deployment plans cannot embed a plan-time activation gate"
        )
    if compatible:
        if (
            not isinstance(compatible_binding, Mapping)
            or set(compatible_binding) != {"path", "file_sha256", "canonical_sha256"}
            or not str(compatible_binding.get("path", "")).strip()
        ):
            raise BuyE3TransactionalDeployError("compatible execution attempt plan binding drifted")
        _require_sha256(
            compatible_binding.get("file_sha256"),
            "compatible execution attempt plan file hash",
        )
        _require_sha256(
            compatible_binding.get("canonical_sha256"),
            "compatible execution attempt plan canonical hash",
        )
    if (
        plan.get("schema_version")
        != (
            SUCCESSOR_PLAN_SCHEMA
            if successor
            else COMPATIBLE_PLAN_SCHEMA
            if compatible
            else PLAN_SCHEMA
        )
        or plan.get("status") != "plan_only_no_remote_command_executed"
        or (
            not compatible
            and not successor
            and execution.get("execution_commit") != gate_v2.FROZEN_EXECUTION_COMMIT
        )
        or plan.get("transaction_contract") != TRANSACTION_CONTRACT
        or plan.get("evidence_boundary") != PLAN_EVIDENCE_BOUNDARY
        or plan.get("runtime_attestation_contract")
        != _runtime_attestation_contract(
            plan.get("remote", {}),
            startup_schema_version=(
                SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
                if successor
                else STARTUP_ATTESTATION_SCHEMA
            ),
        )
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
        compatible_activation = (
            isinstance(activation, Mapping)
            and activation.get("kind") == _COMPATIBLE_ACTIVATION_GATE_KIND
        )
        expected_activation_fields = (
            _COMPATIBLE_ACTIVATION_GATE_BINDING_FIELDS
            if compatible_activation
            else _ACTIVATION_GATE_BINDING_FIELDS
        )
        if (
            not isinstance(activation, Mapping)
            or set(activation) != expected_activation_fields
            or compatible_activation is not compatible
        ):
            raise BuyE3TransactionalDeployError("activation gate plan binding is incomplete")
        if (
            activation.get("plan_core_sha256") != plan["plan_core_sha256"]
            or activation.get("transaction_contract_sha256")
            != gate_v2.canonical_sha256(plan["transaction_contract"])
            or activation.get("canonical_receipt_sha256")
            != plan.get("activation_gate_receipt_sha256")
            or activation.get("canonical_activation_binding_sha256")
            != gate_v2.document_sha256(activation, "canonical_activation_binding_sha256")
        ):
            raise BuyE3TransactionalDeployError("activation gate plan/core binding drifted")
        if compatible_activation:
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
            if activation.get("cross_binding_sha256") != gate_v2.canonical_sha256(
                expected_cross_binding
            ):
                raise BuyE3TransactionalDeployError(
                    "compatible activation gate cross-binding drifted"
                )
    if plan.get("canonical_plan_sha256") != gate_v2.document_sha256(plan, "canonical_plan_sha256"):
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
    validate_b0_config_contract(Path(str(files["disabled_config"]["path"])))
    validate_b0_config_contract(Path(str(files["active_config"]["path"])))
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
    compatible_payload: dict[str, Any] | None
    if execution.get("compatible_attempt_manifest") is None:
        compatible_payload = None
        gate_v2.verify_execution_git_identity(
            repository_root=root,
            expected_commit=str(execution["execution_commit"]),
            expected_tree=str(execution["execution_tree"]),
            annotated_tag=str(execution["annotated_tag"]),
            expected_tag_object=str(execution["annotated_tag_object"]),
        )
        runtime = (
            _successor_runtime_sources(
                repository_root=root,
                execution_commit=str(execution["execution_commit"]),
            )
            if _is_successor_execution(execution)
            else _current_runtime_sources(
                repository_root=root,
                execution_commit=str(execution["execution_commit"]),
            )
        )
    else:
        compatible_payload, runtime = _revalidate_compatible_execution(
            repository_root=root,
            execution=execution,
        )
    if runtime.get("runtime_code_sha256") != plan["runtime_sources"].get("runtime_code_sha256"):
        raise BuyE3TransactionalDeployError("runtime source aggregate drifted")
    if runtime != plan["runtime_sources"]:
        raise BuyE3TransactionalDeployError("runtime source binding drifted")
    if compatible_payload is not None:
        admitted_artifact = compatible_payload.get("artifact")
        admitted_files = (
            admitted_artifact.get("files") if isinstance(admitted_artifact, Mapping) else None
        )
        plan_artifact = plan["artifact"]
        observed_files = {
            "manifest": plan_artifact["manifest_file_sha256"],
            "policy": plan_artifact["policy_file_sha256"],
            "predicate_bundle": plan_artifact["predicate_bundle_file_sha256"],
        }
        if (
            not isinstance(admitted_files, Mapping)
            or admitted_artifact.get("artifact_sha256") != plan_artifact["artifact_sha256"]
            or any(
                admitted_files.get(role, {}).get("file_sha256") != sha256
                for role, sha256 in observed_files.items()
            )
        ):
            raise BuyE3TransactionalDeployError("compatible deployment artifact binding drifted")
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
        if activation.get("kind") == _COMPATIBLE_ACTIVATION_GATE_KIND:
            receipt = gate_v1.validate_deployment_gate_receipt(
                path,
                expected_artifact_sha256=str(plan["artifact"]["artifact_sha256"]),
            )
            if (
                receipt.get("execution_commit") != execution["execution_commit"]
                or receipt.get("execution_tag") != execution["annotated_tag"]
            ):
                raise BuyE3TransactionalDeployError("generic deployment gate execution drifted")
            cross_binding_sha256 = gate_v2.canonical_sha256(expected_cross_binding)
            canonical_receipt_sha256 = receipt["canonical_deployment_gate_receipt_sha256"]
        else:
            receipt = gate_v2.validate_amended_gate_receipt(path)
            cross_binding_sha256 = _require_activation_gate_cross_binding(
                receipt, expected_cross_binding
            )
            canonical_receipt_sha256 = receipt["canonical_amendment_receipt_sha256"]
        if cross_binding_sha256 != activation.get("cross_binding_sha256"):
            raise BuyE3TransactionalDeployError("activation gate plan binding drifted")
        if canonical_receipt_sha256 != activation_receipt_sha256:
            raise BuyE3TransactionalDeployError("activation gate canonical binding drifted")
        if activation.get("plan_core_sha256") != plan["plan_core_sha256"] or activation.get(
            "transaction_contract_sha256"
        ) != gate_v2.canonical_sha256(plan["transaction_contract"]):
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


def _reject_symlink_components(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            metadata = current.lstat()
        except FileNotFoundError as exc:
            raise BuyE3TransactionalDeployError(f"{label} path component is missing") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise BuyE3TransactionalDeployError(f"{label} path traverses a symbolic link")


def _read_bound_release_file(
    path: Path,
    *,
    label: str,
    admitted_modes: frozenset[int],
) -> tuple[bytes, os.stat_result]:
    candidate = path.expanduser().absolute()
    _reject_symlink_components(candidate, label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise BuyE3TransactionalDeployError(f"{label} could not be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) not in admitted_modes
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > MAX_ACTIVE_RELEASE_BYTES
        ):
            raise BuyE3TransactionalDeployError(
                f"{label} is not an owner-only single-link immutable file"
            )
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1 << 20, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if remaining or not _same_file_state(before, after):
            raise BuyE3TransactionalDeployError(f"{label} changed while it was read")
    finally:
        os.close(descriptor)
    lexical_after = candidate.lstat()
    if not _same_file_state(before, lexical_after):
        raise BuyE3TransactionalDeployError(f"{label} path was replaced while it was read")
    return raw, before


def install_private_active_release(
    *, source_path: Path, destination_path: Path, expected_file_sha256: str
) -> dict[str, Any]:
    """Install immutable release bytes without following or replacing a path."""

    expected_sha256 = _require_sha256(expected_file_sha256, "active release file hash")
    source, _source_state = _read_bound_release_file(
        source_path,
        label="staged active release",
        admitted_modes=frozenset({0o400, 0o600}),
    )
    if hashlib.sha256(source).hexdigest() != expected_sha256:
        raise BuyE3TransactionalDeployError("staged active release file hash drifted")

    destination = destination_path.expanduser().absolute()
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _reject_symlink_components(destination.parent, "active release destination parent")
    os.chmod(destination.parent, 0o700)
    directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(destination.parent, directory_flags)
    try:
        create_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        create_flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(destination.name, create_flags, 0o600, dir_fd=directory_fd)
        except FileExistsError as exc:
            existing, _state = _read_bound_release_file(
                destination,
                label="installed active release",
                admitted_modes=frozenset({0o600}),
            )
            if hashlib.sha256(existing).hexdigest() != expected_sha256 or existing != source:
                raise BuyE3TransactionalDeployError(
                    "active release destination already exists with different bytes"
                ) from exc
        else:
            try:
                os.fchmod(descriptor, 0o600)
                offset = 0
                while offset < len(source):
                    written = os.write(descriptor, source[offset:])
                    if written <= 0:
                        raise BuyE3TransactionalDeployError(
                            "active release install did not make progress"
                        )
                    offset += written
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.fsync(directory_fd)
        installed, state = _read_bound_release_file(
            destination,
            label="installed active release",
            admitted_modes=frozenset({0o600}),
        )
        if installed != source or hashlib.sha256(installed).hexdigest() != expected_sha256:
            raise BuyE3TransactionalDeployError("installed active release bytes drifted")
        return {
            "path": str(destination),
            "file_sha256": expected_sha256,
            "mode": "0600",
            "nlink": state.st_nlink,
        }
    finally:
        os.close(directory_fd)


def _validate_installed_active_release_file(
    path: Path,
    *,
    expected_file_sha256: str,
    expected_canonical_sha256: str,
    expected_execution_commit: str,
    expected_execution_tree: str,
    expected_artifact_sha256: str,
    expected_manifest_file_sha256: str = "",
    expected_policy_file_sha256: str = "",
    expected_predicate_bundle_file_sha256: str = "",
    expected_active_config_file_sha256: str = "",
    expected_disabled_config_file_sha256: str = "",
) -> dict[str, Any]:
    """Independently bind the installed private release to process-probe inputs."""

    raw, _state = _read_bound_release_file(
        path,
        label="installed active release process authority",
        admitted_modes=frozenset({0o600}),
    )
    file_sha256 = _require_sha256(
        expected_file_sha256,
        "installed active release expected file hash",
    )
    canonical_sha256 = _require_sha256(
        expected_canonical_sha256,
        "installed active release expected canonical hash",
    )
    if hashlib.sha256(raw).hexdigest() != file_sha256:
        raise BuyE3TransactionalDeployError(
            "installed active release process authority file hash drifted"
        )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BuyE3TransactionalDeployError(
            "installed active release process authority is not JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise BuyE3TransactionalDeployError(
            "installed active release process authority identity drifted"
        )
    try:
        if payload.get("schema_version") == buy_e3_runtime.ACTIVE_RELEASE_SCHEMA:
            execution = payload.get("execution", {})
            artifact = payload.get("exact_artifact", {})
            if (
                payload.get("identity") != active_release.ACTIVE_RELEASE_IDENTITY
                or payload.get("status") != active_release.ACTIVE_RELEASE_STATUS
                or payload.get("canonical_active_release_sha256") != canonical_sha256
                or active_release.document_sha256(
                    payload,
                    "canonical_active_release_sha256",
                )
                != canonical_sha256
                or not isinstance(execution, Mapping)
                or not isinstance(artifact, Mapping)
                or artifact.get("artifact_sha256")
                != _require_sha256(
                    expected_artifact_sha256,
                    "installed active release expected artifact hash",
                )
            ):
                raise ValueError("legacy active release identity drifted")
            release_identity = {
                "execution_commit": execution.get("execution_commit"),
                "execution_tree": execution.get("execution_tree"),
            }
        else:
            release_identity = buy_e3_runtime._validate_active_release(  # noqa: SLF001
                payload,
                expected_canonical_sha256=canonical_sha256,
                expected_artifact_sha256=_require_sha256(
                    expected_artifact_sha256,
                    "installed active release expected artifact hash",
                ),
                expected_manifest_file_sha256=_require_sha256(
                    expected_manifest_file_sha256,
                    "installed active release expected manifest file hash",
                ),
                expected_policy_file_sha256=_require_sha256(
                    expected_policy_file_sha256,
                    "installed active release expected policy file hash",
                ),
                expected_predicate_bundle_file_sha256=_require_sha256(
                    expected_predicate_bundle_file_sha256,
                    "installed active release expected predicate bundle file hash",
                ),
            )
    except (active_release.ActiveReleaseError, TypeError, ValueError) as exc:
        raise BuyE3TransactionalDeployError(
            "installed active release process authority identity drifted"
        ) from exc
    schema_version = str(payload.get("schema_version", ""))
    expected_identity, expected_status = _active_release_contract(schema_version)
    if (
        payload.get("identity") != expected_identity
        or payload.get("status") != expected_status
        or release_identity.get("execution_commit") != expected_execution_commit
        or release_identity.get("execution_tree") != expected_execution_tree
    ):
        raise BuyE3TransactionalDeployError(
            "installed active release process authority identity drifted"
        )
    if schema_version in {
        buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
        buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
    }:
        active_config_sha256 = _require_sha256(
            expected_active_config_file_sha256,
            "installed active release expected active config hash",
        )
        if release_identity.get("active_config_file_sha256") != active_config_sha256:
            raise BuyE3TransactionalDeployError(
                "installed active release binds another active config"
            )
        if expected_disabled_config_file_sha256:
            disabled_config_sha256 = _require_sha256(
                expected_disabled_config_file_sha256,
                "installed active release expected disabled config hash",
            )
            if (
                release_identity.get("disabled_config_file_sha256")
                != disabled_config_sha256
            ):
                raise BuyE3TransactionalDeployError(
                    "installed active release binds another disabled config"
                )
    return payload


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_S,
    )


def _expected_commands(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "label": str(row["label"]),
            "command_sha256": _require_sha256(row["command_sha256"], "command hash"),
        }
        for row in rows
    ]


def _automatic_rollback_from_proven_quiescence(
    results: Sequence[Mapping[str, Any]],
) -> bool:
    """Return whether rollback may skip a second process capture/stop.

    A successful quiescence probe is useful only until a start command is
    attempted.  A failed or disconnected start can still have spawned a
    process, so any observed start row forces the full stop path.
    """

    main_results = [
        result
        for result in results
        if not str(result.get("label", "")).startswith("automatic-rollback:")
    ]
    quiescent_index = next(
        (
            index
            for index in range(len(main_results) - 1, -1, -1)
            if main_results[index].get("label") == "confirm-quiescent"
            and main_results[index].get("returncode") == 0
        ),
        None,
    )
    if quiescent_index is None:
        return False
    return not any(
        str(result.get("label", "")).startswith("start-")
        for result in main_results[quiescent_index + 1 :]
    )


def _automatic_rollback_rows(
    plan: Mapping[str, Any], *, already_quiescent: bool = False
) -> list[Mapping[str, Any]]:
    rows = list(plan["phases"]["rollback-primary"])
    if already_quiescent:
        if not _is_successor_execution(plan["execution"]):
            raise BuyE3TransactionalDeployError(
                "legacy automatic rollback cannot use successor quiescence"
            )
        start_index = next(
            (
                index
                for index, row in enumerate(rows)
                if row["label"]
                == "signed-exchange-open-orders-position-reconciliation"
            ),
            None,
        )
        if start_index is None:
            raise BuyE3TransactionalDeployError(
                "quiescent rollback lacks signed exchange reconciliation"
            )
        return rows[start_index:]
    without_capture = [row for row in rows if row["label"] != "capture-old-pid"]
    if not _is_successor_execution(plan["execution"]):
        return without_capture
    by_label = {str(row["label"]): row for row in without_capture}
    prerequisite_label = "validate-rollback-successor-prerequisites-before-stop"
    stop = by_label.get("stop-live")
    quiescent = by_label.get("confirm-quiescent")
    prerequisite = by_label.get(prerequisite_label)
    if stop is None or quiescent is None or prerequisite is None:
        raise BuyE3TransactionalDeployError(
            "successor automatic rollback lacks stop-first prerequisites"
        )
    tail = [
        row
        for row in without_capture
        if row["label"]
        not in {"stop-live", "confirm-quiescent", prerequisite_label}
    ]
    return [stop, quiescent, prerequisite, *tail]


def _same_file_state(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_uid,
        before.st_nlink,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) == (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_uid,
        after.st_nlink,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )


@contextmanager
def _stable_private_active_release(path: Path):
    """Hold the caller-supplied release inode stable across independent validation."""

    candidate = path.expanduser().absolute()
    _reject_symlink_components(candidate, "active release")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise BuyE3TransactionalDeployError(
            "active release could not be opened with O_NOFOLLOW"
        ) from exc
    before: os.stat_result | None = None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > MAX_ACTIVE_RELEASE_BYTES
        ):
            raise BuyE3TransactionalDeployError(
                "active release is not an owner-only 0600 single-link immutable receipt"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                descriptor,
                min(1 << 20, MAX_ACTIVE_RELEASE_BYTES + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_ACTIVE_RELEASE_BYTES:
                raise BuyE3TransactionalDeployError("active release exceeds the size limit")
        raw = b"".join(chunks)
        after_read = os.fstat(descriptor)
        if not _same_file_state(before, after_read) or len(raw) != before.st_size:
            raise BuyE3TransactionalDeployError("active release changed while it was read")
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BuyE3TransactionalDeployError("active release is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise BuyE3TransactionalDeployError("active release root is not an object")
        yield candidate, raw, payload
        after_validation = os.fstat(descriptor)
        if not _same_file_state(before, after_validation):
            raise BuyE3TransactionalDeployError(
                "active release changed during independent validation"
            )
    finally:
        os.close(descriptor)
    if before is None:
        raise BuyE3TransactionalDeployError("active release identity was not captured")
    try:
        lexical_after = candidate.lstat()
    except FileNotFoundError as exc:
        raise BuyE3TransactionalDeployError(
            "active release path disappeared during validation"
        ) from exc
    if not _same_file_state(before, lexical_after):
        raise BuyE3TransactionalDeployError("active release path was replaced during validation")


def _release_role_binding(payload: Mapping[str, Any], section: str, role: str) -> Mapping[str, Any]:
    raw_section = payload.get(section)
    if section == "exact_artifact" and isinstance(raw_section, Mapping):
        raw_section = raw_section.get("roles")
    if not isinstance(raw_section, Mapping) or not isinstance(raw_section.get(role), Mapping):
        raise BuyE3TransactionalDeployError(f"active release lacks {section}.{role}")
    return raw_section[role]


def _validate_active_release_for_activation(
    path: Path,
    *,
    plan: Mapping[str, Any],
    activation_envelope_binding: Mapping[str, Any] | None,
) -> dict[str, Any]:
    attempt_plan_binding = plan["execution"].get("compatible_attempt_manifest")
    repository_root = Path(str(plan["planner_repository_root"]))
    with _stable_private_active_release(path) as (candidate, raw, raw_payload):
        schema_version = str(raw_payload.get("schema_version", ""))
        try:
            if schema_version == buy_e3_runtime.ACTIVE_RELEASE_SCHEMA:
                payload = active_release.validate_active_release(
                    candidate,
                    repository_root=repository_root,
                )
                release_identity = {
                    "execution_commit": payload.get("execution", {}).get(
                        "execution_commit"
                    ),
                    "execution_tree": payload.get("execution", {}).get(
                        "execution_tree"
                    ),
                }
            else:
                payload = dict(raw_payload)
                release_identity = buy_e3_runtime._validate_active_release(  # noqa: SLF001
                    payload,
                    expected_canonical_sha256=str(
                        payload.get("canonical_active_release_sha256", "")
                    ),
                    expected_artifact_sha256=str(
                        plan["artifact"]["artifact_sha256"]
                    ),
                    expected_manifest_file_sha256=str(
                        plan["artifact"]["manifest_file_sha256"]
                    ),
                    expected_policy_file_sha256=str(
                        plan["artifact"]["policy_file_sha256"]
                    ),
                    expected_predicate_bundle_file_sha256=str(
                        plan["artifact"]["predicate_bundle_file_sha256"]
                    ),
                )
        except (active_release.ActiveReleaseError, TypeError, ValueError) as exc:
            raise BuyE3TransactionalDeployError(f"active release validation failed: {exc}") from exc
        if payload != raw_payload:
            raise BuyE3TransactionalDeployError(
                "active release bytes differ from independently validated payload"
            )
        expected_identity, expected_status = _active_release_contract(schema_version)
        if (
            payload.get("identity") != expected_identity
            or payload.get("status") != expected_status
        ):
            raise BuyE3TransactionalDeployError(
                "active release authority identity drifted"
            )

        execution = payload.get("execution")
        expected_execution = {
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "annotated_operational_tag": plan["execution"]["annotated_tag"],
            "annotated_operational_tag_object": plan["execution"]["annotated_tag_object"],
            "tag_peeled_commit": plan["execution"]["execution_commit"],
        }
        if execution != expected_execution:
            raise BuyE3TransactionalDeployError(
                "active release execution commit/tree/tag differs from the plan"
            )
        exact_artifact = payload.get("exact_artifact")
        if (
            not isinstance(exact_artifact, Mapping)
            or exact_artifact.get("artifact_sha256") != plan["artifact"]["artifact_sha256"]
        ):
            raise BuyE3TransactionalDeployError(
                "active release exact artifact differs from the plan"
            )
        artifact_expectations = {
            "manifest": (
                plan["artifact"]["manifest_path"],
                plan["artifact"]["manifest_file_sha256"],
            ),
            "policy": (
                plan["artifact"]["policy_path"],
                plan["artifact"]["policy_file_sha256"],
            ),
            "predicate_bundle": (
                plan["artifact"]["predicate_bundle_path"],
                plan["artifact"]["predicate_bundle_file_sha256"],
            ),
        }
        for role, (expected_path, expected_sha256) in artifact_expectations.items():
            role_binding = _release_role_binding(payload, "exact_artifact", role)
            if (
                role_binding.get("file_sha256") != expected_sha256
                or (
                    schema_version == buy_e3_runtime.ACTIVE_RELEASE_SCHEMA
                    and Path(str(role_binding.get("path", ""))).resolve(strict=True)
                    != Path(str(expected_path)).resolve(strict=True)
                )
            ):
                raise BuyE3TransactionalDeployError(
                    f"active release artifact role differs from the plan: {role}"
                )

        if schema_version in {
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_SCHEMA,
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V2_SCHEMA,
        }:
            raise BuyE3TransactionalDeployError(
                "historical direct-owner release cannot authorize the current activation"
            )
        if schema_version == buy_e3_runtime.ACTIVE_RELEASE_SCHEMA:
            if (
                not isinstance(attempt_plan_binding, Mapping)
                or not isinstance(activation_envelope_binding, Mapping)
            ):
                raise BuyE3TransactionalDeployError(
                    "legacy active release requires its compatible activation envelope"
                )
            envelope_release_binding = _release_role_binding(
                payload, "evidence", "activation_envelope"
            )
            envelope_path = Path(
                str(activation_envelope_binding["path"])
            ).resolve(strict=True)
            if (
                Path(str(envelope_release_binding.get("path", ""))).resolve(
                    strict=True
                )
                != envelope_path
                or envelope_release_binding.get("file_sha256")
                != activation_envelope_binding["file_sha256"]
                or envelope_release_binding.get("canonical_sha256")
                != activation_envelope_binding[
                    "canonical_activation_envelope_sha256"
                ]
            ):
                raise BuyE3TransactionalDeployError(
                    "active release binds another activation envelope"
                )

            attempt_final_binding = _release_role_binding(
                payload, "evidence", "compatible_attempt_final"
            )
            attempt_final_path = Path(str(attempt_final_binding.get("path", "")))
            try:
                attempt_final = execution_attempt.validate_final_receipt(
                    attempt_final_path,
                    repository_root=repository_root,
                    require_current_checkout=True,
                )
            except execution_attempt.ExecutionAttemptError as exc:
                raise BuyE3TransactionalDeployError(
                    f"active release compatible attempt is invalid: {exc}"
                ) from exc
            if (
                attempt_final_binding.get("file_sha256")
                != gate_v2.file_sha256(attempt_final_path.resolve(strict=True))
                or attempt_final_binding.get("canonical_sha256")
                != attempt_final.get("canonical_final_receipt_sha256")
                or attempt_final.get("runtime_execution")
                != {
                    "execution_commit": plan["execution"]["execution_commit"],
                    "execution_tree": plan["execution"]["execution_tree"],
                    "annotated_tag": plan["execution"]["annotated_tag"],
                    "annotated_tag_object": plan["execution"]["annotated_tag_object"],
                    "tag_peeled_commit": plan["execution"]["execution_commit"],
                }
            ):
                raise BuyE3TransactionalDeployError(
                    "active release compatible attempt final differs from the plan"
                )
            attempt_manifest = attempt_final.get("attempt_manifest")
            if (
                not isinstance(attempt_manifest, Mapping)
                or Path(str(attempt_manifest.get("path", ""))).resolve(strict=True)
                != Path(str(attempt_plan_binding["path"])).resolve(strict=True)
                or attempt_manifest.get("file_sha256")
                != attempt_plan_binding["file_sha256"]
                or attempt_manifest.get("canonical_sha256")
                != attempt_plan_binding["canonical_sha256"]
            ):
                raise BuyE3TransactionalDeployError(
                    "active release attempt manifest differs from the plan"
                )
        elif schema_version in {
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
            buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
        }:
            if activation_envelope_binding is not None:
                raise BuyE3TransactionalDeployError(
                    "direct-owner v3 cannot borrow a historical activation envelope"
                )
            if (
                release_identity.get("active_config_file_sha256")
                != plan["configs"]["active"]["config_sha256"]
                or release_identity.get("disabled_config_file_sha256")
                != plan["configs"]["disabled"]["config_sha256"]
            ):
                raise BuyE3TransactionalDeployError(
                    "active release exact config pair differs from the plan"
                )

        file_sha256 = hashlib.sha256(raw).hexdigest()
        canonical_sha256 = _require_sha256(
            payload.get("canonical_active_release_sha256"),
            "active release canonical hash",
        )
        remote_path = _remote_active_release_path(
            str(plan["active_pointer"]["repo_root"]),
            file_sha256,
        )
        binding = {
            "local_path": str(candidate.resolve(strict=True)),
            "remote_path": remote_path,
            "file_sha256": file_sha256,
            "canonical_active_release_sha256": canonical_sha256,
            "schema_version": schema_version,
            "status": expected_status,
        }
        if schema_version in {
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
            buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
        }:
            binding.update(
                {
                    "active_config_file_sha256": release_identity[
                        "active_config_file_sha256"
                    ],
                    "disabled_config_file_sha256": release_identity[
                        "disabled_config_file_sha256"
                    ],
                }
            )
        if schema_version == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA:
            native_build = payload["native_build"]
            binding.update(_successor_native_build_binding(native_build))
        return binding


def _expected_process_binding(
    plan: Mapping[str, Any],
    phase: str,
    active_release_binding: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if phase in {"disabled-deploy", "activate"}:
        config_name = "active" if phase == "activate" else "disabled"
        if phase != "activate" and active_release_binding is not None and (
            active_release_binding.get("schema_version")
            != buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
        ):
            raise BuyE3TransactionalDeployError(
                "disabled process cannot carry active release authority"
            )
        active_binding = active_release_binding if phase == "activate" else None
        safety_binding = (
            active_release_binding
            if active_release_binding is not None
            and active_release_binding.get("schema_version")
            == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
            else None
        )
        return {
            "enabled": phase == "activate",
            "startup_attestation_schema_version": str(
                plan["runtime_attestation_contract"][
                    "startup_attestation_schema_version"
                ]
            ),
            "config_path": plan["remote"][f"{config_name}_config_path"],
            "config_sha256": plan["configs"][config_name]["config_sha256"],
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "artifact_sha256": plan["artifact"]["artifact_sha256"],
            "runtime_code_sha256": plan["runtime_sources"]["runtime_code_sha256"],
            "repo_root": plan["active_pointer"]["repo_root"],
            "python_executable": plan["host"]["python_executable"],
            "venv_root": plan["host"]["venv_root"],
            "active_release": _expected_active_release_identity(
                active_binding,
                expected_execution_commit=plan["execution"]["execution_commit"],
                expected_execution_tree=plan["execution"]["execution_tree"],
            ),
            "active_release_binding": active_binding,
            "safety_release_binding": safety_binding,
            "startup_exchange_reconciliation_path": (
                f"{plan['remote']['startup_checkpoint_path']}.{config_name}."
                "exchange.startup"
                if _is_successor_execution(plan["execution"])
                else None
            ),
        }
    rollback_name = {
        "rollback-primary": "primary_disabled",
        "rollback-deep": "deep_predecessor",
    }.get(phase)
    if rollback_name is None:
        raise BuyE3TransactionalDeployError("process identity phase is unknown")
    identity = plan["rollback_identities"][rollback_name]
    safety_binding = (
        active_release_binding
        if active_release_binding is not None
        and active_release_binding.get("schema_version")
        == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
        else None
    )
    return {
        "enabled": False,
        "startup_attestation_schema_version": _rollback_startup_attestation_schema(
            identity=identity,
            current_execution=plan["execution"],
        ),
        "config_path": identity["config_path"],
        "config_sha256": identity["config_sha256"],
        "execution_commit": identity["execution_commit"],
        "execution_tree": identity["execution_tree"],
        "artifact_sha256": str(identity.get("artifact_sha256", "")),
        "runtime_code_sha256": identity["runtime_code_sha256"],
        "repo_root": plan["active_pointer"]["repo_root"],
        "python_executable": identity["python_executable"],
        "venv_root": identity["venv_root"],
        "active_release": _empty_active_release_identity(),
        "active_release_binding": None,
        "safety_release_binding": safety_binding,
        "startup_exchange_reconciliation_path": (
            f"{plan['remote']['startup_checkpoint_path']}.rollback.exchange.startup"
            if _is_successor_execution(plan["execution"])
            else None
        ),
    }


def _expected_runtime_sources_for_phase(
    plan: Mapping[str, Any], phase: str
) -> dict[str, Any]:
    if phase in {"disabled-deploy", "activate"}:
        return dict(plan["runtime_sources"])
    rollback_name = {
        "rollback-primary": "primary_disabled",
        "rollback-deep": "deep_predecessor",
    }.get(phase)
    if rollback_name is None:
        raise BuyE3TransactionalDeployError("runtime source phase is unknown")
    return _rollback_runtime_source_authority(
        identity=plan["rollback_identities"][rollback_name],
        current_execution=plan["execution"],
        current_runtime_sources=plan["runtime_sources"],
    )


def _validate_actual_process_identity(
    process: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    phase: str,
    old_pid: int | None,
    require_fresh: bool = True,
    active_release_binding: Mapping[str, Any] | None = None,
    allow_legacy: bool = False,
) -> dict[str, Any]:
    expected = _expected_process_binding(plan, phase, active_release_binding)
    successor = (
        expected["startup_attestation_schema_version"]
        == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
    )
    legacy = set(process) == _LEGACY_PROCESS_IDENTITY_FIELDS
    expected_fields = (
        _LEGACY_PROCESS_IDENTITY_FIELDS
        if legacy
        else _SUCCESSOR_PROCESS_IDENTITY_FIELDS
        if successor
        else _PROCESS_IDENTITY_FIELDS
    )
    if (
        set(process) != expected_fields
        or (legacy and not allow_legacy)
        or (successor and legacy)
    ):
        raise BuyE3TransactionalDeployError("actual process identity fields drifted")
    if process.get("schema_version") != gate_v2.PROCESS_IDENTITY_SCHEMA or process.get(
        "canonical_process_identity_sha256"
    ) != gate_v2.document_sha256(process, "canonical_process_identity_sha256"):
        raise BuyE3TransactionalDeployError("actual process identity hash drifted")
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
        raise BuyE3TransactionalDeployError("actual process runtime identity file hash drifted")
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
        raise BuyE3TransactionalDeployError(
            "actual process artifact/runtime/config identity drifted"
        )
    if (
        process.get("buy_e3_enabled") is not expected["enabled"]
        or process.get("owner_override_effective") is not expected["enabled"]
    ):
        raise BuyE3TransactionalDeployError("actual process authority/deadline identity drifted")
    if legacy:
        if (
            process.get("initial_buy_deadline_identity") != "B0"
            or process.get("e3_deadline_imported") is not False
        ):
            raise BuyE3TransactionalDeployError(
                "actual process authority/deadline identity drifted"
            )
    else:
        mode = str(process.get("fill_cooldown_restore_mode", ""))
        identity = str(process.get("initial_buy_deadline_identity", ""))
        remaining = process.get("initial_buy_remaining_ms")
        imported = process.get("e3_deadline_imported")
        if not isinstance(remaining, int) or isinstance(remaining, bool) or remaining < 0:
            raise BuyE3TransactionalDeployError(
                "actual process authority/deadline identity drifted"
            )
        enabled_modes = {
            "fresh_b0_no_checkpoint",
            "expired_to_b0",
            "b0_checkpoint_resume",
            "exact_same_artifact_resume",
            "artifact_identity_changed_to_b0",
        }
        disabled_modes = {
            "fresh_b0_no_checkpoint",
            "expired_to_b0",
            "b0_checkpoint_resume",
            "rollback_to_b0",
        }
        if mode not in (enabled_modes if expected["enabled"] else disabled_modes):
            raise BuyE3TransactionalDeployError(
                "actual process authority/deadline identity drifted"
            )
        if mode == "exact_same_artifact_resume":
            deadline_valid = (
                identity == f"BUY_E3:{expected['artifact_sha256']}"
                and remaining > 0
                and imported is True
            )
        elif mode in {
            "artifact_identity_changed_to_b0",
            "rollback_to_b0",
            "b0_checkpoint_resume",
        }:
            deadline_valid = identity == "B0" and imported is False
        else:
            deadline_valid = identity == "B0" and remaining == 0 and imported is False
        expected_release = expected["active_release"]
        release_fields = {
            "active_release_path": expected_release["path"],
            "active_release_file_sha256": expected_release["file_sha256"],
            "active_release_canonical_sha256": expected_release["file_canonical_sha256"],
            "active_release_execution_commit": expected_release["execution_commit"],
            "active_release_execution_tree": expected_release["execution_tree"],
        }
        if not deadline_valid or any(
            process.get(field) != value for field, value in release_fields.items()
        ):
            raise BuyE3TransactionalDeployError(
                "actual process authority/deadline identity drifted"
            )
    if (
        not str(process.get("captured_utc", "")).strip()
        or not str(process.get("python_binary_resolved", "")).strip()
    ):
        raise BuyE3TransactionalDeployError("actual process capture identity is incomplete")
    if successor:
        exchange = process.get("startup_exchange_reconciliation")
        if (
            not isinstance(exchange, Mapping)
            or set(exchange) != _STARTUP_EXCHANGE_RECONCILIATION_BINDING_FIELDS
            or exchange.get("path")
            != expected["startup_exchange_reconciliation_path"]
        ):
            raise BuyE3TransactionalDeployError(
                "actual process exchange reconciliation binding drifted"
            )
        for field in (
            "file_sha256",
            "canonical_sha256",
            "account_key_sha256",
            "position_lineage_sha256",
        ):
            _require_sha256(exchange.get(field), f"process exchange {field}")
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
    active_release_binding: Mapping[str, Any] | None = None,
    allow_legacy: bool = False,
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
        active_release_binding=active_release_binding,
        allow_legacy=allow_legacy,
    )


def _parse_process_family_probe(stdout: str) -> dict[str, int | str]:
    tokens = stdout.split()
    if len(tokens) != 4:
        raise BuyE3TransactionalDeployError(
            "supervisor/child process family probe is malformed"
        )
    try:
        supervisor_pid, supervisor_ticks, child_pid, child_ticks = (
            int(value) for value in tokens
        )
    except (TypeError, ValueError) as exc:
        raise BuyE3TransactionalDeployError(
            "supervisor/child process family probe is malformed"
        ) from exc
    if (
        min(supervisor_pid, supervisor_ticks, child_pid, child_ticks) <= 0
        or supervisor_pid == child_pid
    ):
        raise BuyE3TransactionalDeployError(
            "supervisor/child process family probe is malformed"
        )
    family: dict[str, int | str] = {
        "supervisor_pid": supervisor_pid,
        "supervisor_start_ticks": supervisor_ticks,
        "child_pid": child_pid,
        "child_start_ticks": child_ticks,
        "child_ppid": supervisor_pid,
    }
    family["process_family_identity_sha256"] = gate_v2.canonical_sha256(
        family
    )
    return family


def _process_handoff_identity(process: Mapping[str, Any]) -> dict[str, Any]:
    runtime_identity = process.get("runtime_identity")
    if not isinstance(runtime_identity, Mapping):
        raise BuyE3TransactionalDeployError("disabled process runtime identity is malformed")
    handoff = {
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
        "runtime_identity_file_sha256": process["runtime_identity_file_sha256"],
        "startup_attestation_sha256": process["startup_attestation_sha256"],
        "execution_commit": process["execution_commit"],
        "execution_tree": process["execution_tree"],
        "artifact_sha256": process["artifact_sha256"],
        "runtime_code_sha256": process["runtime_code_sha256"],
        "buy_e3_enabled": process["buy_e3_enabled"],
        "owner_override_effective": process["owner_override_effective"],
        "initial_buy_deadline_identity": process["initial_buy_deadline_identity"],
        "e3_deadline_imported": process["e3_deadline_imported"],
        "fill_cooldown_restore_mode": process.get("fill_cooldown_restore_mode"),
        "initial_buy_remaining_ms": process.get("initial_buy_remaining_ms"),
        "active_release_path": process.get("active_release_path"),
        "active_release_file_sha256": process.get("active_release_file_sha256"),
        "active_release_canonical_sha256": process.get("active_release_canonical_sha256"),
        "active_release_execution_commit": process.get("active_release_execution_commit"),
        "active_release_execution_tree": process.get("active_release_execution_tree"),
    }
    if "startup_exchange_reconciliation" in process:
        exchange = process.get("startup_exchange_reconciliation")
        if not isinstance(exchange, Mapping):
            raise BuyE3TransactionalDeployError(
                "disabled process exchange reconciliation binding is malformed"
            )
        handoff["startup_exchange_reconciliation"] = dict(exchange)
    return handoff


def _require_same_disabled_process(prior: Mapping[str, Any], current: Mapping[str, Any]) -> None:
    if _process_handoff_identity(prior) != _process_handoff_identity(current):
        raise BuyE3TransactionalDeployError("disabled process handoff identity drifted")


def _load_disabled_phase_receipt_binding(
    receipt_path: Path, *, plan: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    target = receipt_path.expanduser().absolute()
    try:
        receipt = validate_phase_receipt(target, plan=plan, expected_phase="disabled-deploy")
    except Exception as exc:
        raise BuyE3TransactionalDeployError("activation disabled phase receipt is invalid") from exc
    if receipt.get("status") != PHASE_COMPLETE:
        raise BuyE3TransactionalDeployError("activation disabled phase receipt is not complete")
    process = receipt.get("actual_process_identity")
    startup_binding = receipt.get("actual_startup_attestation")
    if not isinstance(process, Mapping):
        raise BuyE3TransactionalDeployError("activation disabled phase receipt lacks a process")
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
        "runtime_identity_file_sha256": startup_binding["runtime_identity_file_sha256"],
        "startup_attestation_sha256": startup_binding["startup_attestation_sha256"],
    }
    return binding, dict(process)


def _validate_runtime_identity_stdout(
    stdout: str,
    *,
    plan: Mapping[str, Any],
    process: Mapping[str, Any],
    process_phase: str,
    active_release_binding: Mapping[str, Any] | None = None,
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
        runtime_identity.get("path") != plan["runtime_attestation_contract"]["remote_path"]
        or runtime_identity.get("file_sha256") != file_sha256
        or process.get("runtime_identity_file_sha256") != file_sha256
    ):
        raise BuyE3TransactionalDeployError(
            "runtime identity stdout differs from process-bound file bytes"
        )
    expected = _expected_process_binding(plan, process_phase, active_release_binding)
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
        expected_runtime_sources=_expected_runtime_sources_for_phase(plan, process_phase),
        expected_repository_root=expected["repo_root"],
        expected_startup_attestation_schema_version=expected[
            "startup_attestation_schema_version"
        ],
        expected_active_release=expected.get("active_release_binding"),
        expected_safety_release=expected.get("safety_release_binding"),
        expected_exchange_reconciliation_path=expected.get(
            "startup_exchange_reconciliation_path"
        ),
        allow_legacy_startup=expected["startup_attestation_schema_version"]
        in {LEGACY_STARTUP_ATTESTATION_SCHEMA, HISTORICAL_STARTUP_ATTESTATION_SCHEMA},
    )
    startup_attestation_sha256 = gate_v2.canonical_sha256(attestation)
    if process.get("startup_attestation_sha256") != startup_attestation_sha256:
        raise BuyE3TransactionalDeployError(
            "runtime startup attestation differs from process-bound hash"
        )
    binding: dict[str, Any] = {
        "schema_version": RUNTIME_IDENTITY_BINDING_SCHEMA,
        "authority": "runtime_written_startup_attestation",
        "evidence_classification": ("runtime_identity_file_unsigned_structural_evidence"),
        "cryptographic_signature_present": False,
        "runtime_identity_path": runtime_identity["path"],
        "runtime_identity_file_sha256": file_sha256,
        "runtime_identity_schema_version": runtime["schema_version"],
        "pid": int(process["pid"]),
        "pid_start_ticks": int(process["pid_start_ticks"]),
        "process_identity_sha256": process["canonical_process_identity_sha256"],
        "config_path": runtime["config_path"],
        "config_sha256": runtime["config_sha256"],
        "artifact_sha256": process["artifact_sha256"],
        "buy_e3_enabled": runtime["f05_buy_e3_enabled"],
        "owner_override_effective": runtime["f05_buy_e3_owner_override_effective"],
        "active_release_file_sha256": process["active_release_file_sha256"],
        "active_release_canonical_sha256": process["active_release_canonical_sha256"],
        "active_release_execution_commit": process["active_release_execution_commit"],
        "active_release_execution_tree": process["active_release_execution_tree"],
        "startup_attestation": attestation,
        "startup_attestation_sha256": startup_attestation_sha256,
    }
    if (
        expected["startup_attestation_schema_version"]
        == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
    ):
        binding["startup_exchange_reconciliation"] = dict(
            runtime["startup_exchange_reconciliation"]
        )
    binding["canonical_runtime_identity_binding_sha256"] = gate_v2.document_sha256(
        binding, "canonical_runtime_identity_binding_sha256"
    )
    return binding


def _validate_runtime_identity_binding(
    raw: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    process: Mapping[str, Any],
    process_phase: str,
    active_release_binding: Mapping[str, Any] | None = None,
    allow_legacy: bool = False,
    expected_startup_schema_version: str | None = None,
) -> dict[str, Any]:
    expected = _expected_process_binding(plan, process_phase, active_release_binding)
    successor = (
        expected["startup_attestation_schema_version"]
        == SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
    )
    legacy = set(raw) == _LEGACY_RUNTIME_IDENTITY_BINDING_FIELDS
    expected_fields = (
        _LEGACY_RUNTIME_IDENTITY_BINDING_FIELDS
        if legacy
        else _SUCCESSOR_RUNTIME_IDENTITY_BINDING_FIELDS
        if successor
        else _RUNTIME_IDENTITY_BINDING_FIELDS
    )
    if set(raw) != expected_fields or (legacy and not allow_legacy):
        raise BuyE3TransactionalDeployError("runtime identity binding fields drifted")
    binding = dict(raw)
    runtime_identity = process.get("runtime_identity")
    if not isinstance(runtime_identity, Mapping):
        raise BuyE3TransactionalDeployError("runtime identity process binding is malformed")
    if runtime_identity.get("file_sha256") != process.get("runtime_identity_file_sha256"):
        raise BuyE3TransactionalDeployError("runtime identity process file hashes disagree")
    exact = {
        "schema_version": RUNTIME_IDENTITY_BINDING_SCHEMA,
        "authority": "runtime_written_startup_attestation",
        "evidence_classification": ("runtime_identity_file_unsigned_structural_evidence"),
        "cryptographic_signature_present": False,
        "runtime_identity_path": plan["runtime_attestation_contract"]["remote_path"],
        "runtime_identity_file_sha256": process["runtime_identity_file_sha256"],
        "runtime_identity_schema_version": RUNTIME_IDENTITY_SCHEMA,
        "pid": int(process["pid"]),
        "pid_start_ticks": int(process["pid_start_ticks"]),
        "process_identity_sha256": process["canonical_process_identity_sha256"],
        "config_path": expected["config_path"],
        "config_sha256": expected["config_sha256"],
        "artifact_sha256": expected["artifact_sha256"],
        "buy_e3_enabled": bool(expected["enabled"]),
        "owner_override_effective": bool(expected["enabled"]),
    }
    if any(binding.get(field) != value for field, value in exact.items()):
        raise BuyE3TransactionalDeployError("runtime identity binding drifted")
    if not legacy:
        release = expected["active_release"]
        expected_release_fields = {
            "active_release_file_sha256": release["file_sha256"],
            "active_release_canonical_sha256": release["file_canonical_sha256"],
            "active_release_execution_commit": release["execution_commit"],
            "active_release_execution_tree": release["execution_tree"],
        }
        if any(binding.get(field) != value for field, value in expected_release_fields.items()):
            raise BuyE3TransactionalDeployError("runtime identity active release binding drifted")
    if successor:
        exchange = binding.get("startup_exchange_reconciliation")
        if (
            not isinstance(exchange, Mapping)
            or set(exchange) != _STARTUP_EXCHANGE_RECONCILIATION_BINDING_FIELDS
            or exchange.get("path")
            != expected["startup_exchange_reconciliation_path"]
        ):
            raise BuyE3TransactionalDeployError(
                "runtime identity exchange reconciliation binding drifted"
            )
        for field in (
            "file_sha256",
            "canonical_sha256",
            "account_key_sha256",
            "position_lineage_sha256",
        ):
            _require_sha256(exchange.get(field), f"runtime binding exchange {field}")
    attestation = _validate_startup_attestation(
        binding.get("startup_attestation"),
        expected_schema_version=(
            expected_startup_schema_version
            or expected["startup_attestation_schema_version"]
        ),
        expected_execution_commit=expected["execution_commit"],
        expected_execution_tree=expected["execution_tree"],
        expected_artifact_sha256=expected["artifact_sha256"],
        expected_runtime_sources=_expected_runtime_sources_for_phase(plan, process_phase),
        expected_repository_root=expected["repo_root"],
        expected_python_executable=expected["python_executable"],
        expected_python_binary_resolved=str(process["python_binary_resolved"]),
        expected_config_sha256=expected["config_sha256"],
        expected_enabled=bool(expected["enabled"]),
        expected_active_release=expected.get("active_release_binding"),
        expected_safety_release=expected.get("safety_release_binding"),
        allow_legacy=allow_legacy,
    )
    if (
        binding.get("startup_attestation_sha256") != gate_v2.canonical_sha256(attestation)
        or binding.get("startup_attestation_sha256") != process.get("startup_attestation_sha256")
        or binding.get("canonical_runtime_identity_binding_sha256")
        != gate_v2.document_sha256(binding, "canonical_runtime_identity_binding_sha256")
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
                json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
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


def _artifact_file_hashes(plan: Mapping[str, Any]) -> dict[str, str]:
    artifact = plan["artifact"]
    return {
        "manifest": str(artifact["manifest_file_sha256"]),
        "policy": str(artifact["policy_file_sha256"]),
        "predicate_bundle": str(artifact["predicate_bundle_file_sha256"]),
    }


def _compatible_activation_receipt_binding(
    *,
    path: Path,
    canonical_sha256: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    target = path.expanduser().absolute().resolve(strict=True)
    binding = {
        "path": str(target),
        "file_sha256": gate_v2.file_sha256(target),
        "canonical_sha256": _require_sha256(canonical_sha256, "receipt canonical hash"),
    }
    if extra is not None:
        binding.update(dict(extra))
    return binding


def build_compatible_activation_envelope(
    *,
    plan: Mapping[str, Any],
    disabled_phase_receipt_path: Path,
    concurrent_resource_receipt_path: Path,
    runtime_regression_receipt_path: Path,
    sell_54_case_receipt_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    """Bind post-disabled safety evidence without changing the research identity."""

    validate_plan(plan)
    if plan["execution"].get("compatible_attempt_manifest") is None:
        raise BuyE3TransactionalDeployError(
            "compatible activation envelope requires a compatible execution plan"
        )
    if plan.get("activation_gate") is not None:
        raise BuyE3TransactionalDeployError(
            "compatible activation envelope cannot wrap a legacy plan-time gate"
        )
    disabled_binding, disabled_process = _load_disabled_phase_receipt_binding(
        disabled_phase_receipt_path,
        plan=plan,
    )
    execution = plan["execution"]
    artifact = plan["artifact"]
    resource = gate_v1.validate_concurrent_resource_receipt(
        concurrent_resource_receipt_path,
        expected_artifact_sha256=str(artifact["artifact_sha256"]),
        expected_execution_commit=str(execution["execution_commit"]),
        expected_execution_tag=str(execution["annotated_tag"]),
        expected_disabled_process_identity=disabled_process,
    )
    regression = gate_v1.validate_runtime_regression_receipt(
        runtime_regression_receipt_path,
        repository_root=Path(str(plan["planner_repository_root"])),
        expected_artifact_sha256=str(artifact["artifact_sha256"]),
        expected_execution_commit=str(execution["execution_commit"]),
        expected_execution_tag=str(execution["annotated_tag"]),
    )
    sell = gate_v1.validate_sell_owner_54_case_receipt(
        sell_54_case_receipt_path,
        repository_root=Path(str(plan["planner_repository_root"])),
        expected_artifact_sha256=str(artifact["artifact_sha256"]),
        expected_artifact_files=_artifact_file_hashes(plan),
    )
    external_files = plan["external_tools_and_package"]["files"]
    validate_b0_config_contract(Path(str(external_files["disabled_config"]["path"])))
    validate_b0_config_contract(Path(str(external_files["active_config"]["path"])))
    resource_binding = _compatible_activation_receipt_binding(
        path=concurrent_resource_receipt_path,
        canonical_sha256=str(resource["canonical_resource_receipt_sha256"]),
        extra={
            "disabled_process_identity_sha256": disabled_binding["process_identity_sha256"],
            "live_pid": disabled_binding["pid"],
        },
    )
    regression_binding = _compatible_activation_receipt_binding(
        path=runtime_regression_receipt_path,
        canonical_sha256=str(regression["canonical_receipt_sha256"]),
        extra={
            "nodeid_manifest_sha256": regression["nodeid_manifest_sha256"],
            "test_source_manifest_sha256": gate_v2.canonical_sha256(
                {
                    "test_files": regression["test_files"],
                    "runtime_sources": regression["runtime_sources"],
                }
            ),
        },
    )
    sell_binding = {key: value for key, value in sell.items() if key != "canonical_receipt_sha256"}
    sell_binding["canonical_sha256"] = sell["canonical_receipt_sha256"]
    checks = {
        "disabled_phase_complete_and_same_plan": True,
        "b0_fill_cooldown_exact_in_both_configs": True,
        "concurrent_2vcpu_2gib_resource_window_passed": True,
        "frozen_regression_nodeid_and_sources_passed": True,
        "real_sell_54_case_and_sources_passed": True,
        "no_locked_or_economic_evidence_read": True,
    }
    envelope: dict[str, Any] = {
        "schema_version": COMPATIBLE_ACTIVATION_ENVELOPE_SCHEMA,
        "status": "compatible_activation_evidence_complete",
        "plan_sha256": plan["canonical_plan_sha256"],
        "plan_core_sha256": plan["plan_core_sha256"],
        "transaction_contract_sha256": gate_v2.canonical_sha256(plan["transaction_contract"]),
        "execution": {
            "execution_commit": execution["execution_commit"],
            "execution_tree": execution["execution_tree"],
            "annotated_tag": execution["annotated_tag"],
            "annotated_tag_object": execution["annotated_tag_object"],
        },
        "artifact": {
            "artifact_sha256": artifact["artifact_sha256"],
            "files": _artifact_file_hashes(plan),
        },
        "disabled_phase_receipt": disabled_binding,
        "concurrent_resource_receipt": resource_binding,
        "runtime_regression_receipt": regression_binding,
        "sell_54_case_receipt": sell_binding,
        "checks": checks,
        "activation_contract": {
            "restart_only": True,
            "same_disabled_process_required": True,
            "phase_token_still_required": True,
            "envelope_does_not_authorize_remote_mutation_by_itself": True,
        },
        "evidence_boundary": dict(ACTIVATION_ENVELOPE_EVIDENCE_BOUNDARY),
    }
    envelope["canonical_activation_envelope_sha256"] = gate_v2.document_sha256(
        envelope, "canonical_activation_envelope_sha256"
    )
    reservation = _reserve_receipt_output(output_path)
    try:
        _commit_reserved_receipt(
            reservation,
            envelope,
            validator=lambda candidate: validate_compatible_activation_envelope(
                candidate,
                plan=plan,
                disabled_phase_receipt_path=disabled_phase_receipt_path,
            ),
        )
    finally:
        _release_receipt_reservation(reservation)
    return envelope


def validate_compatible_activation_envelope(
    path: Path,
    *,
    plan: Mapping[str, Any],
    disabled_phase_receipt_path: Path,
) -> dict[str, Any]:
    validate_plan(plan)
    if plan["execution"].get("compatible_attempt_manifest") is None:
        raise BuyE3TransactionalDeployError("activation envelope is not valid for this plan")
    target = path.expanduser().absolute()
    metadata = target.lstat()
    if target.is_symlink() or not target.is_file() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise BuyE3TransactionalDeployError("activation envelope must be a 0600 regular file")
    envelope = gate_v2.read_json(target)
    fields = {
        "schema_version",
        "status",
        "plan_sha256",
        "plan_core_sha256",
        "transaction_contract_sha256",
        "execution",
        "artifact",
        "disabled_phase_receipt",
        "concurrent_resource_receipt",
        "runtime_regression_receipt",
        "sell_54_case_receipt",
        "checks",
        "activation_contract",
        "evidence_boundary",
        "canonical_activation_envelope_sha256",
    }
    if set(envelope) != fields:
        raise BuyE3TransactionalDeployError("activation envelope fields drifted")
    disabled_binding, disabled_process = _load_disabled_phase_receipt_binding(
        disabled_phase_receipt_path,
        plan=plan,
    )
    if envelope.get("disabled_phase_receipt") != disabled_binding:
        raise BuyE3TransactionalDeployError("activation envelope disabled receipt drifted")
    execution = plan["execution"]
    artifact = plan["artifact"]
    expected_execution = {
        "execution_commit": execution["execution_commit"],
        "execution_tree": execution["execution_tree"],
        "annotated_tag": execution["annotated_tag"],
        "annotated_tag_object": execution["annotated_tag_object"],
    }
    expected_artifact = {
        "artifact_sha256": artifact["artifact_sha256"],
        "files": _artifact_file_hashes(plan),
    }
    resource_binding = envelope.get("concurrent_resource_receipt")
    regression_binding = envelope.get("runtime_regression_receipt")
    sell_binding = envelope.get("sell_54_case_receipt")
    if not all(
        isinstance(binding, Mapping)
        for binding in (resource_binding, regression_binding, sell_binding)
    ):
        raise BuyE3TransactionalDeployError("activation envelope receipt binding is malformed")
    resource_path = Path(str(resource_binding["path"]))
    if gate_v2.file_sha256(resource_path) != resource_binding.get("file_sha256"):
        raise BuyE3TransactionalDeployError("activation resource receipt bytes drifted")
    resource = gate_v1.validate_concurrent_resource_receipt(
        resource_path,
        expected_artifact_sha256=str(artifact["artifact_sha256"]),
        expected_execution_commit=str(execution["execution_commit"]),
        expected_execution_tag=str(execution["annotated_tag"]),
        expected_disabled_process_identity=disabled_process,
    )
    expected_resource_binding = _compatible_activation_receipt_binding(
        path=resource_path,
        canonical_sha256=str(resource["canonical_resource_receipt_sha256"]),
        extra={
            "disabled_process_identity_sha256": disabled_binding["process_identity_sha256"],
            "live_pid": disabled_binding["pid"],
        },
    )
    regression_path = Path(str(regression_binding["path"]))
    if gate_v2.file_sha256(regression_path) != regression_binding.get("file_sha256"):
        raise BuyE3TransactionalDeployError("activation regression receipt bytes drifted")
    regression = gate_v1.validate_runtime_regression_receipt(
        regression_path,
        repository_root=Path(str(plan["planner_repository_root"])),
        expected_artifact_sha256=str(artifact["artifact_sha256"]),
        expected_execution_commit=str(execution["execution_commit"]),
        expected_execution_tag=str(execution["annotated_tag"]),
    )
    expected_regression_binding = _compatible_activation_receipt_binding(
        path=regression_path,
        canonical_sha256=str(regression["canonical_receipt_sha256"]),
        extra={
            "nodeid_manifest_sha256": regression["nodeid_manifest_sha256"],
            "test_source_manifest_sha256": gate_v2.canonical_sha256(
                {
                    "test_files": regression["test_files"],
                    "runtime_sources": regression["runtime_sources"],
                }
            ),
        },
    )
    sell_path = Path(str(sell_binding["path"]))
    if gate_v2.file_sha256(sell_path) != sell_binding.get("file_sha256"):
        raise BuyE3TransactionalDeployError("activation SELL receipt bytes drifted")
    sell = gate_v1.validate_sell_owner_54_case_receipt(
        sell_path,
        repository_root=Path(str(plan["planner_repository_root"])),
        expected_artifact_sha256=str(artifact["artifact_sha256"]),
        expected_artifact_files=_artifact_file_hashes(plan),
    )
    expected_sell_binding = {
        key: value for key, value in sell.items() if key != "canonical_receipt_sha256"
    }
    expected_sell_binding["canonical_sha256"] = sell["canonical_receipt_sha256"]
    external_files = plan["external_tools_and_package"]["files"]
    validate_b0_config_contract(Path(str(external_files["disabled_config"]["path"])))
    validate_b0_config_contract(Path(str(external_files["active_config"]["path"])))
    expected_checks = {
        "disabled_phase_complete_and_same_plan": True,
        "b0_fill_cooldown_exact_in_both_configs": True,
        "concurrent_2vcpu_2gib_resource_window_passed": True,
        "frozen_regression_nodeid_and_sources_passed": True,
        "real_sell_54_case_and_sources_passed": True,
        "no_locked_or_economic_evidence_read": True,
    }
    expected_contract = {
        "restart_only": True,
        "same_disabled_process_required": True,
        "phase_token_still_required": True,
        "envelope_does_not_authorize_remote_mutation_by_itself": True,
    }
    if (
        envelope.get("schema_version") != COMPATIBLE_ACTIVATION_ENVELOPE_SCHEMA
        or envelope.get("status") != "compatible_activation_evidence_complete"
        or envelope.get("plan_sha256") != plan["canonical_plan_sha256"]
        or envelope.get("plan_core_sha256") != plan["plan_core_sha256"]
        or envelope.get("transaction_contract_sha256")
        != gate_v2.canonical_sha256(plan["transaction_contract"])
        or envelope.get("execution") != expected_execution
        or envelope.get("artifact") != expected_artifact
        or resource_binding != expected_resource_binding
        or regression_binding != expected_regression_binding
        or sell_binding != expected_sell_binding
        or envelope.get("checks") != expected_checks
        or envelope.get("activation_contract") != expected_contract
        or envelope.get("evidence_boundary") != ACTIVATION_ENVELOPE_EVIDENCE_BOUNDARY
        or envelope.get("canonical_activation_envelope_sha256")
        != gate_v2.document_sha256(envelope, "canonical_activation_envelope_sha256")
    ):
        raise BuyE3TransactionalDeployError("activation envelope identity drifted")
    return envelope


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
    execution_rows: Sequence[Mapping[str, Any]],
    mutation_started: bool,
    activation_envelope_binding: Mapping[str, Any] | None,
    active_release_binding: Mapping[str, Any] | None,
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
    rollback_rows = (
        _automatic_rollback_rows(
            plan,
            already_quiescent=_automatic_rollback_from_proven_quiescence(results),
        )
        if rollback_attempted
        else []
    )
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "plan_sha256": plan["canonical_plan_sha256"],
        "phase": phase,
        "status": status,
        "remote_mutation_authorized": True,
        "phase_authorization_token_sha256": plan["phase_token_sha256"][phase],
        "transaction_contract_sha256": gate_v2.canonical_sha256(plan["transaction_contract"]),
        "expected_commands": _expected_commands(execution_rows),
        "expected_automatic_rollback_commands": _expected_commands(rollback_rows),
        "results": [dict(result) for result in results],
        "mutation_started": bool(mutation_started),
        **(
            {"activation_envelope_binding": dict(activation_envelope_binding)}
            if activation_envelope_binding is not None
            else {}
        ),
        "active_release_binding": (
            dict(active_release_binding) if active_release_binding is not None else None
        ),
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
            dict(actual_startup_attestation) if actual_startup_attestation is not None else None
        ),
        "actual_process_identity": (
            dict(actual_process_identity) if actual_process_identity is not None else None
        ),
        "stop_failure_probe_result": (
            dict(stop_failure_probe_result) if stop_failure_probe_result is not None else None
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
            identity_fields
            not in ({"observed_pid"}, {"observed_pid", "observed_start_ticks"})
            or int(result["observed_pid"]) <= 0
            or (
                "observed_start_ticks" in result
                and int(result["observed_start_ticks"]) <= 0
            )
        ):
            raise BuyE3TransactionalDeployError("old PID result binding is malformed")
    elif bare_label == "capture-old-supervisor-pid":
        if identity_fields and (
            identity_fields != {"observed_pid", "observed_start_ticks"}
            or int(result["observed_pid"]) <= 0
            or int(result["observed_start_ticks"]) <= 0
        ):
            raise BuyE3TransactionalDeployError(
                "old supervisor PID result binding is malformed"
            )
    elif "supervisor-child-probe" in bare_label:
        family_fields = {
            "supervisor_pid",
            "supervisor_start_ticks",
            "child_pid",
            "child_start_ticks",
            "child_ppid",
            "process_family_identity_sha256",
        }
        if identity_fields and (
            identity_fields != family_fields
            or any(int(result[field]) <= 0 for field in family_fields - {"process_family_identity_sha256"})
            or int(result["child_ppid"]) != int(result["supervisor_pid"])
        ):
            raise BuyE3TransactionalDeployError("process family result binding is malformed")
        if identity_fields:
            observed_family_sha = _require_sha256(
                result["process_family_identity_sha256"],
                "process family identity hash",
            )
            family_identity = {
                field: result[field]
                for field in family_fields - {"process_family_identity_sha256"}
            }
            if observed_family_sha != gate_v2.canonical_sha256(family_identity):
                raise BuyE3TransactionalDeployError(
                    "process family canonical identity drifted"
                )
    elif "process-probe" in bare_label or bare_label == "reprobe-disabled-process-before-stop":
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
            raise BuyE3TransactionalDeployError("runtime identity result binding is malformed")
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

    candidate = receipt_path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise BuyE3TransactionalDeployError("phase receipt is not an immutable regular file")
    target = candidate.resolve(strict=True)
    if stat.S_IMODE(target.stat().st_mode) != 0o600:
        raise BuyE3TransactionalDeployError("phase receipt permission drifted from 0600")
    receipt = gate_v2.read_json(target)
    phase = str(receipt.get("phase", ""))
    if phase not in MUTATING_PHASES or (expected_phase is not None and phase != expected_phase):
        raise BuyE3TransactionalDeployError("phase receipt phase drifted")
    raw_active_release_binding = receipt.get("active_release_binding")
    direct_v3_activation = bool(
        phase == "activate"
        and isinstance(raw_active_release_binding, Mapping)
        and raw_active_release_binding.get("schema_version")
        in {
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
            buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
        }
    )
    successor_disabled = bool(
        phase == "disabled-deploy"
        and isinstance(raw_active_release_binding, Mapping)
        and raw_active_release_binding.get("schema_version")
        == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
    )
    compatible_activation = bool(
        phase == "activate"
        and plan["execution"].get("compatible_attempt_manifest") is not None
        and not direct_v3_activation
    )
    receipt_schema = receipt.get("schema_version")
    legacy_v3_receipt = receipt_schema == LEGACY_RECEIPT_SCHEMA
    historical_v4_receipt = receipt_schema == HISTORICAL_RECEIPT_SCHEMA
    current_v5_receipt = receipt_schema == RECEIPT_SCHEMA
    if historical_v4_receipt:
        raise BuyE3TransactionalDeployError(
            "historical receipt-v4 is read-only provenance and is not accepted by the current validator"
        )
    if not (legacy_v3_receipt or current_v5_receipt):
        raise BuyE3TransactionalDeployError("phase receipt schema drifted")
    historical_receipt = legacy_v3_receipt
    expected_receipt_startup_schema = (
        LEGACY_STARTUP_ATTESTATION_SCHEMA
        if legacy_v3_receipt
        else SUCCESSOR_STARTUP_ATTESTATION_SCHEMA
        if _is_successor_execution(plan["execution"])
        else STARTUP_ATTESTATION_SCHEMA
    )
    validate_plan(plan)
    _revalidate_plan_inputs(plan)
    expected_fields = (
        _LEGACY_PHASE_RECEIPT_FIELDS if legacy_v3_receipt else _PHASE_RECEIPT_FIELDS
    ) | ({"activation_envelope_binding"} if compatible_activation else set())
    if set(receipt) != expected_fields:
        raise BuyE3TransactionalDeployError("phase receipt fields drifted")
    if (
        receipt.get("schema_version") != receipt_schema
        or receipt.get("plan_sha256") != plan["canonical_plan_sha256"]
        or receipt.get("canonical_receipt_sha256")
        != gate_v2.document_sha256(receipt, "canonical_receipt_sha256")
        or receipt.get("remote_mutation_authorized") is not True
        or receipt.get("phase_authorization_token_sha256") != plan["phase_token_sha256"][phase]
        or receipt.get("transaction_contract_sha256")
        != gate_v2.canonical_sha256(plan["transaction_contract"])
        or receipt.get("permissions") != RECEIPT_PERMISSIONS
        or receipt.get("evidence_boundary") != RECEIPT_EVIDENCE_BOUNDARY
        or receipt.get("evidence_authority") != RECEIPT_AUTHORITY
    ):
        raise BuyE3TransactionalDeployError("phase receipt identity drifted")
    active_release_binding = raw_active_release_binding
    if legacy_v3_receipt:
        if active_release_binding is not None:
            raise BuyE3TransactionalDeployError(
                "historical phase receipt carries future active release authority"
            )
    elif compatible_activation or direct_v3_activation or successor_disabled:
        if not isinstance(active_release_binding, Mapping):
            raise BuyE3TransactionalDeployError(
                "activation receipt lacks active release binding"
            )
        active_release_binding = _validate_active_release_phase_binding(
            active_release_binding,
            plan=plan,
        )
    elif active_release_binding is not None:
        raise BuyE3TransactionalDeployError(
            "non-compatible phase receipt carries active release binding"
        )
    execution_rows = _execution_rows(plan, phase, active_release_binding)
    expected = _expected_commands(execution_rows)
    if receipt.get("expected_commands") != expected:
        raise BuyE3TransactionalDeployError("phase receipt expected command binding drifted")
    raw_results = receipt.get("results")
    if not isinstance(raw_results, list):
        raise BuyE3TransactionalDeployError("phase receipt command results are malformed")
    results: list[dict[str, Any]] = []
    for raw in raw_results:
        if not isinstance(raw, Mapping):
            raise BuyE3TransactionalDeployError("phase result is malformed")
        result = dict(raw)
        _validate_result_shape(result)
        results.append(result)
    first_rollback = next(
        (
            index
            for index, result in enumerate(results)
            if result["label"].startswith("automatic-rollback:")
        ),
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
    attempted_rows = execution_rows[: len(main_results)]
    successor_transaction = plan.get("schema_version") == SUCCESSOR_PLAN_SCHEMA
    mutation_started = any(
        bool(row["mutates_remote"])
        and (not successor_transaction or bool(row["after_stop"]))
        for row in attempted_rows
    )
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
        if not main_results and (
            failure_class != "phase_timeout"
            or receipt.get("mutation_started") is not False
            or rollback_results
        ):
            raise BuyE3TransactionalDeployError("empty failed phase is not a pre-mutation timeout")
        if any(result["returncode"] not in {0, None} for result in main_results[:-1]):
            raise BuyE3TransactionalDeployError("phase continued after a failed command")
        if main_results:
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
    envelope_binding = receipt.get("activation_envelope_binding")
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
            raise BuyE3TransactionalDeployError("activation disabled phase receipt binding drifted")
        rebound, prior_disabled_process = _load_disabled_phase_receipt_binding(
            Path(str(disabled_binding["path"])), plan=plan
        )
        if dict(disabled_binding) != rebound:
            raise BuyE3TransactionalDeployError("activation disabled phase receipt bytes drifted")
        if compatible_activation:
            if (
                not isinstance(envelope_binding, Mapping)
                or set(envelope_binding) != _ACTIVATION_ENVELOPE_PHASE_BINDING_FIELDS
            ):
                raise BuyE3TransactionalDeployError(
                    "compatible activation envelope binding drifted"
                )
            envelope = validate_compatible_activation_envelope(
                Path(str(envelope_binding["path"])),
                plan=plan,
                disabled_phase_receipt_path=Path(str(disabled_binding["path"])),
            )
            expected_envelope_binding = {
                "path": str(Path(str(envelope_binding["path"])).resolve(strict=True)),
                "file_sha256": gate_v2.file_sha256(
                    Path(str(envelope_binding["path"])).resolve(strict=True)
                ),
                "canonical_activation_envelope_sha256": envelope[
                    "canonical_activation_envelope_sha256"
                ],
                "concurrent_resource_receipt_sha256": envelope["concurrent_resource_receipt"][
                    "canonical_sha256"
                ],
                "runtime_regression_receipt_sha256": envelope["runtime_regression_receipt"][
                    "canonical_sha256"
                ],
                "sell_54_case_receipt_sha256": envelope["sell_54_case_receipt"]["canonical_sha256"],
            }
            if dict(envelope_binding) != expected_envelope_binding:
                raise BuyE3TransactionalDeployError("compatible activation envelope bytes drifted")
            if not historical_receipt:
                observed_release_binding = _validate_active_release_for_activation(
                    Path(str(active_release_binding["local_path"])),
                    plan=plan,
                    activation_envelope_binding=expected_envelope_binding,
                )
                if dict(active_release_binding) != observed_release_binding:
                    raise BuyE3TransactionalDeployError(
                        "active release bytes or activation binding drifted"
                    )
        elif direct_v3_activation:
            if envelope_binding is not None:
                raise BuyE3TransactionalDeployError(
                    "direct-owner v3 receipt borrowed a historical activation envelope"
                )
            if not historical_receipt:
                observed_release_binding = _validate_active_release_for_activation(
                    Path(str(active_release_binding["local_path"])),
                    plan=plan,
                    activation_envelope_binding=None,
                )
                if dict(active_release_binding) != observed_release_binding:
                    raise BuyE3TransactionalDeployError(
                        "direct-owner v3 active release bytes drifted"
                    )
        if old_pid is not None and old_pid != int(prior_disabled_process["pid"]):
            raise BuyE3TransactionalDeployError("activation old PID differs from disabled receipt")
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
                allow_legacy=legacy_v3_receipt,
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
            if (
                sum(
                    result["label"] == "reprobe-disabled-process-before-stop"
                    and result.get("process_identity_sha256") == pre_stop_hash
                    for result in main_results
                )
                != 1
            ):
                raise BuyE3TransactionalDeployError(
                    "pre-stop disabled process probe is not rebound"
                )
        if pre_stop_startup is not None:
            if not isinstance(pre_stop_startup, Mapping) or validated_pre_stop is None:
                raise BuyE3TransactionalDeployError("pre-stop runtime startup binding is malformed")
            validated_pre_stop_startup = _validate_runtime_identity_binding(
                pre_stop_startup,
                plan=plan,
                process=validated_pre_stop,
                process_phase="disabled-deploy",
                allow_legacy=historical_receipt,
                expected_startup_schema_version=expected_receipt_startup_schema,
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
            if (
                sum(
                    result["label"] == "read-pre-stop-disabled-runtime-identity"
                    and result.get("stdout_sha256")
                    == validated_pre_stop_startup["runtime_identity_file_sha256"]
                    and result.get("runtime_identity_file_sha256")
                    == validated_pre_stop_startup["runtime_identity_file_sha256"]
                    and result.get("startup_attestation_sha256")
                    == validated_pre_stop_startup["startup_attestation_sha256"]
                    for result in main_results
                )
                != 1
            ):
                raise BuyE3TransactionalDeployError(
                    "pre-stop runtime identity result is not rebound"
                )
        if status == PHASE_COMPLETE and (
            old_pid is None or pre_stop_disabled_process is None or pre_stop_startup is None
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
            process,
            plan=plan,
            phase=phase,
            old_pid=old_pid,
            active_release_binding=active_release_binding,
            allow_legacy=legacy_v3_receipt,
        )
        process_hash = validated_process["canonical_process_identity_sha256"]
        matching = [
            result
            for result in main_results
            if result.get("process_identity_sha256") == process_hash
        ]
        if len(matching) != 1:
            raise BuyE3TransactionalDeployError("process probe result is not rebound")
        if _is_successor_execution(plan["execution"]):
            family_results = [
                result
                for result in main_results
                if "supervisor-child-probe" in str(result.get("label", ""))
            ]
            if (
                len(family_results) != 1
                or int(family_results[0].get("child_pid", -1))
                != int(validated_process["pid"])
                or int(family_results[0].get("child_start_ticks", -1))
                != int(validated_process["pid_start_ticks"])
                or int(family_results[0].get("child_ppid", -1))
                != int(family_results[0].get("supervisor_pid", -2))
            ):
                raise BuyE3TransactionalDeployError(
                    "successor process receipt lacks an exact supervisor/child binding"
                )
    elif status == PHASE_COMPLETE and phase != "rollback-deep":
        raise BuyE3TransactionalDeployError("completed phase lacks actual process identity")
    elif status == PHASE_COMPLETE:
        deep_labels = {result["label"] for result in main_results}
        if (
            "confirm-quiescent" not in deep_labels
            or "deep-stop-reconciliation-required" not in deep_labels
            or any("start" in label or "process-probe" in label for label in deep_labels)
        ):
            raise BuyE3TransactionalDeployError(
                "deep rollback receipt is not exact stopped reconciliation evidence"
            )
    if actual_startup is not None:
        if not isinstance(actual_startup, Mapping) or not isinstance(process, Mapping):
            raise BuyE3TransactionalDeployError("actual runtime startup binding is malformed")
        if phase not in {"disabled-deploy", "activate"}:
            raise BuyE3TransactionalDeployError(
                "rollback receipt carries unsupported runtime startup evidence"
            )
        validated_actual_startup = _validate_runtime_identity_binding(
            actual_startup,
            plan=plan,
            process=process,
            process_phase=phase,
            active_release_binding=active_release_binding,
            allow_legacy=historical_receipt,
            expected_startup_schema_version=expected_receipt_startup_schema,
        )
        expected_label = (
            "read-disabled-runtime-identity"
            if phase == "disabled-deploy"
            else "read-active-runtime-identity"
        )
        if (
            sum(
                result["label"] == expected_label
                and result.get("stdout_sha256")
                == validated_actual_startup["runtime_identity_file_sha256"]
                and result.get("runtime_identity_file_sha256")
                == validated_actual_startup["runtime_identity_file_sha256"]
                and result.get("startup_attestation_sha256")
                == validated_actual_startup["startup_attestation_sha256"]
                for result in main_results
            )
            != 1
        ):
            raise BuyE3TransactionalDeployError("actual runtime identity result is not rebound")
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
        expected_probe_row = next(
            row for row in execution_rows if row["label"] == "confirm-quiescent"
        )
        if successor_transaction:
            if set(stop_failure_probe) != {"schema_version", "results"} or (
                stop_failure_probe.get("schema_version")
                != SUCCESSOR_STOP_FAILURE_RECOVERY_SCHEMA
            ):
                raise BuyE3TransactionalDeployError(
                    "successor stop failure recovery evidence drifted"
                )
            recovery_results = stop_failure_probe.get("results")
            if not isinstance(recovery_results, list) or not recovery_results:
                raise BuyE3TransactionalDeployError(
                    "successor stop failure lacks a quiescence result"
                )
            for recovery_result in recovery_results:
                if not isinstance(recovery_result, Mapping):
                    raise BuyE3TransactionalDeployError(
                        "successor stop failure recovery result is malformed"
                    )
                _validate_result_shape(recovery_result)
            first_recovery = recovery_results[0]
            if (
                first_recovery.get("label")
                != "stop-failure-probe:confirm-quiescent"
                or first_recovery.get("command_sha256")
                != expected_probe_row["command_sha256"]
            ):
                raise BuyE3TransactionalDeployError(
                    "stop failure quiescence command drifted"
                )
            quiescent = first_recovery.get("returncode") == 0
            expected_reconciliation_row = next(
                (
                    row
                    for row in execution_rows
                    if row["label"]
                    == "signed-exchange-open-orders-position-reconciliation"
                ),
                None,
            )
            if quiescent:
                if expected_reconciliation_row is None or len(recovery_results) != 2:
                    raise BuyE3TransactionalDeployError(
                        "quiescent stop failure lacks independent exchange reconciliation"
                    )
                reconciliation_result = recovery_results[1]
                if (
                    reconciliation_result.get("label")
                    != "stop-failure-probe:signed-exchange-open-orders-position-reconciliation"
                    or reconciliation_result.get("command_sha256")
                    != expected_reconciliation_row["command_sha256"]
                ):
                    raise BuyE3TransactionalDeployError(
                        "stop failure exchange reconciliation command drifted"
                    )
            elif len(recovery_results) != 1:
                raise BuyE3TransactionalDeployError(
                    "non-quiescent stop failure ran exchange reconciliation"
                )
        else:
            _validate_result_shape(stop_failure_probe)
            if (
                stop_failure_probe.get("label")
                != "stop-failure-probe:confirm-quiescent"
                or stop_failure_probe.get("command_sha256")
                != expected_probe_row["command_sha256"]
            ):
                raise BuyE3TransactionalDeployError("stop failure probe command drifted")
    expected_rollback_attempt = (
        status == PHASE_FAILED_CLOSED
        and mutation_started
        and phase not in {"rollback-primary", "rollback-deep"}
        and not (
            successor_transaction
            and (
                failed_stop is not None
                or failure_class == "exchange_reconciliation_failed"
            )
        )
    )
    if receipt.get("rollback_attempted") is not expected_rollback_attempt:
        raise BuyE3TransactionalDeployError("automatic rollback behavior drifted")
    expected_rollback = (
        _expected_commands(
            _automatic_rollback_rows(
                plan,
                already_quiescent=_automatic_rollback_from_proven_quiescence(
                    main_results
                ),
            )
        )
        if expected_rollback_attempt
        else []
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
                allow_legacy=legacy_v3_receipt,
            )
            rollback_hash = validated_rollback["canonical_process_identity_sha256"]
            if (
                sum(
                    result.get("process_identity_sha256") == rollback_hash
                    for result in rollback_results
                )
                != 1
            ):
                raise BuyE3TransactionalDeployError("rollback process probe is not rebound")
            if _is_successor_execution(plan["execution"]):
                family_results = [
                    result
                    for result in rollback_results
                    if "supervisor-child-probe"
                    in str(result.get("label", ""))
                ]
                if (
                    len(family_results) != 1
                    or int(family_results[0].get("child_pid", -1))
                    != int(validated_rollback["pid"])
                    or int(family_results[0].get("child_start_ticks", -1))
                    != int(validated_rollback["pid_start_ticks"])
                    or int(family_results[0].get("child_ppid", -1))
                    != int(family_results[0].get("supervisor_pid", -2))
                ):
                    raise BuyE3TransactionalDeployError(
                        "automatic rollback lacks an exact supervisor/child binding"
                    )
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
                    allow_legacy=legacy_v3_receipt,
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
    rows = plan["phases"][phase]
    row = next(row for row in rows if row["label"] == "confirm-quiescent")
    probe_row = dict(row)
    probe_row["label"] = "stop-failure-probe:confirm-quiescent"
    try:
        completed = runner(tuple(str(value) for value in row["argv"]))
    except Exception:
        completed = None
    quiescence_result = _command_result(probe_row, completed)
    if not _is_successor_execution(plan["execution"]):
        return quiescence_result

    recovery_results = [quiescence_result]
    if completed is not None and completed.returncode == 0:
        reconciliation_row = next(
            row
            for row in rows
            if row["label"]
            == "signed-exchange-open-orders-position-reconciliation"
        )
        reconciliation_probe = dict(reconciliation_row)
        reconciliation_probe["label"] = (
            "stop-failure-probe:"
            "signed-exchange-open-orders-position-reconciliation"
        )
        try:
            reconciled = runner(
                tuple(str(value) for value in reconciliation_row["argv"])
            )
        except Exception:
            reconciled = None
        recovery_results.append(
            _command_result(reconciliation_probe, reconciled)
        )
    return {
        "schema_version": SUCCESSOR_STOP_FAILURE_RECOVERY_SCHEMA,
        "results": recovery_results,
    }


def _run_automatic_rollback(
    *,
    plan: Mapping[str, Any],
    runner: CommandRunner,
    results: list[dict[str, Any]],
    old_pid: int | None,
    already_quiescent: bool,
) -> tuple[str, str | None, dict[str, Any] | None]:
    rollback_failure_class: str | None = None
    rollback_process_identity: dict[str, Any] | None = None
    rollback_process_family: dict[str, int | str] | None = None
    completed_rows = 0
    rows = _automatic_rollback_rows(plan, already_quiescent=already_quiescent)
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
        if "supervisor-child-probe" in row["label"]:
            try:
                rollback_process_family = _parse_process_family_probe(
                    completed.stdout or ""
                )
            except BuyE3TransactionalDeployError:
                rollback_failure_class = "process_probe_invalid"
                break
            result.update(rollback_process_family)
        elif "process-probe" in row["label"]:
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
            if _is_successor_execution(plan["execution"]) and (
                rollback_process_family is None
                or int(rollback_process_identity["pid"])
                != int(rollback_process_family["child_pid"])
                or int(rollback_process_identity["pid_start_ticks"])
                != int(rollback_process_family["child_start_ticks"])
            ):
                rollback_failure_class = "process_identity_invalid"
                break
    if (
        rollback_failure_class is None
        and completed_rows == len(rows)
        and rollback_process_identity is not None
        and (
            rollback_process_family is not None
            or not _is_successor_execution(plan["execution"])
        )
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
    activation_envelope_path: Path | None = None,
    active_release_path: Path | None = None,
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
    compatible = plan["execution"].get("compatible_attempt_manifest") is not None
    release_schema = ""
    successor_plan = _is_successor_execution(plan["execution"])
    if phase in {"disabled-deploy", "activate"} and active_release_path is not None:
        with _stable_private_active_release(
            active_release_path
        ) as (_candidate, _raw, release_payload):
            release_schema = str(release_payload.get("schema_version", ""))
        _active_release_contract(release_schema)
        if release_schema == buy_e3_runtime.ACTIVE_RELEASE_SCHEMA:
            raise BuyE3TransactionalDeployError(
                "historical standalone release is not current live activation authority"
            )
        if release_schema in {
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_SCHEMA,
            buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V2_SCHEMA,
        }:
            raise BuyE3TransactionalDeployError(
                "historical direct-owner release cannot authorize current activation"
            )
        if phase == "disabled-deploy" and (
            release_schema
            != buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
        ):
            raise BuyE3TransactionalDeployError(
                "disabled successor phase requires operational safety successor authority"
            )
    direct_v3_activation = release_schema in {
        buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
        buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
    }
    activation_envelope_binding: dict[str, Any] | None = None
    if phase == "activate" and direct_v3_activation:
        if activation_envelope_path is not None:
            raise BuyE3TransactionalDeployError(
                "direct-owner v3 activation forbids a historical activation envelope"
            )
    elif phase == "activate" and compatible:
        if activation_envelope_path is None or disabled_phase_receipt_path is None:
            raise PermissionError(
                "compatible activation requires a post-disabled activation envelope"
            )
        envelope = validate_compatible_activation_envelope(
            activation_envelope_path,
            plan=plan,
            disabled_phase_receipt_path=disabled_phase_receipt_path,
        )
        envelope_target = activation_envelope_path.expanduser().absolute().resolve(strict=True)
        activation_envelope_binding = {
            "path": str(envelope_target),
            "file_sha256": gate_v2.file_sha256(envelope_target),
            "canonical_activation_envelope_sha256": envelope[
                "canonical_activation_envelope_sha256"
            ],
            "concurrent_resource_receipt_sha256": envelope["concurrent_resource_receipt"][
                "canonical_sha256"
            ],
            "runtime_regression_receipt_sha256": envelope["runtime_regression_receipt"][
                "canonical_sha256"
            ],
            "sell_54_case_receipt_sha256": envelope["sell_54_case_receipt"]["canonical_sha256"],
        }
    elif phase == "activate" and not plan.get("activation_gate_receipt_sha256"):
        raise PermissionError("activation requires a separately bound amended gate receipt")
    elif activation_envelope_path is not None:
        raise BuyE3TransactionalDeployError(
            "activation envelope is accepted only for compatible activation"
        )
    disabled_phase_receipt_binding: dict[str, Any] | None = None
    prior_disabled_process: dict[str, Any] | None = None
    if phase == "activate":
        if disabled_phase_receipt_path is None:
            raise PermissionError(
                "activation requires the same-plan successful disabled phase receipt"
            )
        disabled_phase_receipt_binding, prior_disabled_process = (
            _load_disabled_phase_receipt_binding(disabled_phase_receipt_path, plan=plan)
        )
    elif disabled_phase_receipt_path is not None:
        raise BuyE3TransactionalDeployError(
            "disabled phase receipt is accepted only for activation"
        )
    active_release_binding: dict[str, Any] | None = None
    if phase in {"disabled-deploy", "activate"} and active_release_path is not None:
        if not direct_v3_activation and activation_envelope_binding is None:
            raise PermissionError(
                "legacy compatible activation requires a post-envelope active release"
            )
        active_release_binding = _validate_active_release_for_activation(
            active_release_path,
            plan=plan,
            activation_envelope_binding=activation_envelope_binding,
        )
    elif phase == "activate":
        if successor_plan:
            raise PermissionError(
                "successor deployment requires an exact live safety successor release"
            )
        raise PermissionError(
            "current activation requires an exact direct-owner v3 active release"
        )
    elif phase == "disabled-deploy" and successor_plan:
        raise PermissionError(
            "successor deployment requires an exact live safety successor release"
        )
    elif active_release_path is not None:
        raise BuyE3TransactionalDeployError(
            "active release is accepted only for compatible activation"
        )
    if output_path is None:
        raise BuyE3TransactionalDeployError("remote phase requires an immutable receipt output")
    reservation = _reserve_receipt_output(output_path)
    rows = _execution_rows(plan, phase, active_release_binding)
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
    old_pid_start_ticks: int | None = None
    old_supervisor_pid: int | None = None
    old_supervisor_start_ticks: int | None = None
    current_process_family: dict[str, int | str] | None = None
    phase_error: BaseException | None = None
    stop_execution_state_uncertain = False
    phase_started = time.monotonic()

    def require_phase_time() -> None:
        nonlocal failure_class
        if time.monotonic() - phase_started > PHASE_TIMEOUT_S:
            failure_class = "phase_timeout"
            raise TimeoutError(f"remote phase exceeded {int(PHASE_TIMEOUT_S)} seconds")

    try:
        for row in rows:
            require_phase_time()
            if row["mutates_remote"] and (
                not successor_plan or bool(row["after_stop"])
            ):
                mutation_started = True
            try:
                completed = runner(tuple(str(value) for value in row["argv"]))
            except Exception:
                results.append(_command_result(row, None))
                if row["label"] == "stop-live":
                    stop_execution_state_uncertain = successor_plan
                    failure_class = (
                        "stop_execution_state_uncertain"
                        if successor_plan
                        else "command_runner_exception"
                    )
                    stop_failure_probe_result = _run_stop_failure_probe(
                        plan=plan, phase=phase, runner=runner
                    )
                else:
                    stop_execution_state_uncertain = (
                        successor_plan
                        and row["label"]
                        == "signed-exchange-open-orders-position-reconciliation"
                    )
                    failure_class = (
                        "exchange_reconciliation_failed"
                        if stop_execution_state_uncertain
                        else "command_runner_exception"
                    )
                raise
            result = _command_result(row, completed)
            results.append(result)
            if completed.returncode != 0:
                if row["label"] == "stop-live":
                    stop_execution_state_uncertain = successor_plan
                    failure_class = (
                        "stop_execution_state_uncertain"
                        if successor_plan
                        else "command_returncode_nonzero"
                    )
                    stop_failure_probe_result = _run_stop_failure_probe(
                        plan=plan, phase=phase, runner=runner
                    )
                else:
                    stop_execution_state_uncertain = (
                        successor_plan
                        and row["label"]
                        == "signed-exchange-open-orders-position-reconciliation"
                    )
                    failure_class = (
                        "exchange_reconciliation_failed"
                        if stop_execution_state_uncertain
                        else "command_returncode_nonzero"
                    )
                raise BuyE3TransactionalDeployError(f"remote phase failed closed at {row['label']}")
            if completed.returncode == 0 and row["label"] == "capture-old-pid":
                try:
                    tokens = (completed.stdout or "").split()
                    old_pid = int(tokens[0])
                    old_pid_start_ticks = int(tokens[1]) if successor_plan else None
                except (IndexError, ValueError) as exc:
                    failure_class = "old_pid_probe_invalid"
                    raise BuyE3TransactionalDeployError("old PID probe is malformed") from exc
                if old_pid <= 0:
                    failure_class = "old_pid_probe_invalid"
                    raise BuyE3TransactionalDeployError("old PID probe is invalid")
                result["observed_pid"] = old_pid
                if old_pid_start_ticks is not None:
                    result["observed_start_ticks"] = old_pid_start_ticks
                if (
                    phase == "activate"
                    and prior_disabled_process is not None
                    and old_pid != int(prior_disabled_process["pid"])
                ):
                    failure_class = "disabled_process_handoff_mismatch"
                    raise BuyE3TransactionalDeployError(
                        "captured process differs from disabled phase receipt"
                    )
            elif completed.returncode == 0 and row["label"] == "capture-old-supervisor-pid":
                try:
                    tokens = (completed.stdout or "").split()
                    old_supervisor_pid = int(tokens[0])
                    old_supervisor_start_ticks = int(tokens[1])
                except (IndexError, ValueError) as exc:
                    failure_class = "old_pid_probe_invalid"
                    raise BuyE3TransactionalDeployError(
                        "old supervisor PID probe is malformed"
                    ) from exc
                if old_supervisor_pid <= 0 or old_supervisor_start_ticks <= 0:
                    raise BuyE3TransactionalDeployError(
                        "old supervisor PID probe is invalid"
                    )
                result["observed_pid"] = old_supervisor_pid
                result["observed_start_ticks"] = old_supervisor_start_ticks
            elif completed.returncode == 0 and "supervisor-child-probe" in row["label"]:
                current_process_family = _parse_process_family_probe(
                    completed.stdout or ""
                )
                sp = int(current_process_family["supervisor_pid"])
                ss = int(current_process_family["supervisor_start_ticks"])
                cp = int(current_process_family["child_pid"])
                cs = int(current_process_family["child_start_ticks"])
                if (
                    (old_pid == cp and old_pid_start_ticks == cs)
                    or (
                        old_supervisor_pid == sp
                        and old_supervisor_start_ticks == ss
                    )
                ):
                    raise BuyE3TransactionalDeployError(
                        "supervisor/child process family is stale or reused"
                    )
                result.update(current_process_family)
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
                        completed.stdout or "",
                        plan=plan,
                        phase=phase,
                        old_pid=old_pid,
                        active_release_binding=active_release_binding,
                    )
                except BuyE3TransactionalDeployError as exc:
                    failure_class = _classify_process_error(exc)
                    raise
                result["observed_pid"] = actual_process_identity["pid"]
                result["process_identity_sha256"] = actual_process_identity[
                    "canonical_process_identity_sha256"
                ]
                if (
                    current_process_family is not None
                    and int(actual_process_identity["pid"])
                    != int(current_process_family["child_pid"])
                ):
                    raise BuyE3TransactionalDeployError(
                        "runtime identity process is not the attested supervisor child"
                    )
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
                        active_release_binding=(
                            active_release_binding
                            if identity_phase == "activate"
                            or (
                                active_release_binding is not None
                                and active_release_binding.get("schema_version")
                                == buy_e3_runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA
                            )
                            else None
                        ),
                    )
                except BuyE3TransactionalDeployError:
                    failure_class = "runtime_identity_invalid"
                    raise
                result["runtime_identity_file_sha256"] = startup_binding[
                    "runtime_identity_file_sha256"
                ]
                result["startup_attestation_sha256"] = startup_binding["startup_attestation_sha256"]
                if row["label"] == "read-pre-stop-disabled-runtime-identity":
                    pre_stop_disabled_startup_attestation = startup_binding
                    if disabled_phase_receipt_binding is None or (
                        startup_binding["runtime_identity_file_sha256"]
                        != disabled_phase_receipt_binding["runtime_identity_file_sha256"]
                        or startup_binding["startup_attestation_sha256"]
                        != disabled_phase_receipt_binding["startup_attestation_sha256"]
                    ):
                        failure_class = "disabled_process_handoff_mismatch"
                        raise BuyE3TransactionalDeployError(
                            "runtime identity changed since disabled phase"
                        )
                else:
                    actual_startup_attestation = startup_binding
        if actual_process_identity is None and phase != "rollback-deep":
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
        require_phase_time()
        _revalidate_plan_inputs(plan)
        phase_complete = True
    except BaseException as exc:
        phase_error = exc
        if failure_class is None:
            failure_class = "phase_contract_validation_failed"
        if (
            mutation_started
            and not stop_execution_state_uncertain
            and phase not in {"rollback-primary", "rollback-deep"}
        ):
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
                already_quiescent=(
                    _automatic_rollback_from_proven_quiescence(results)
                ),
            )
    receipt = _build_phase_receipt(
        plan=plan,
        phase=phase,
        status=PHASE_COMPLETE if phase_complete else PHASE_FAILED_CLOSED,
        results=results,
        execution_rows=rows,
        mutation_started=mutation_started,
        activation_envelope_binding=activation_envelope_binding,
        active_release_binding=active_release_binding,
        disabled_phase_receipt_binding=disabled_phase_receipt_binding,
        pre_stop_disabled_process_identity=pre_stop_disabled_process_identity,
        pre_stop_disabled_startup_attestation=(pre_stop_disabled_startup_attestation),
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
            validator=lambda path: validate_phase_receipt(path, plan=plan, expected_phase=phase),
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
            and not stop_execution_state_uncertain
            and phase not in {"rollback-primary", "rollback-deep"}
        ):
            rollback_attempted = True
            _run_automatic_rollback(
                plan=plan,
                runner=runner,
                results=results,
                old_pid=old_pid,
                already_quiescent=(
                    _automatic_rollback_from_proven_quiescence(results)
                ),
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


def prepare_successor_native_runtime(
    *,
    repository_root: Path,
    stage_root: Path,
    annotated_tag: str,
    annotated_tag_object: str,
    execution_commit: str,
    execution_tree: str,
    seed_python: Path,
) -> dict[str, Any]:
    """Prepare native bytes before a release or deployment plan can exist.

    This is deliberately a separate, pre-deploy operation.  It is safe to run
    while the predecessor remains live and it never changes the active checkout,
    selector, config, PID files, or process.  The resulting create-only receipt
    is an input to the private successor release; deployment only validates it.
    """

    root = repository_root.expanduser().resolve(strict=True)
    stage = stage_root.expanduser().absolute()
    commit = _require_git_sha(execution_commit, "native preparation commit")
    tree = _require_git_sha(execution_tree, "native preparation tree")
    tag_object = _require_git_sha(
        annotated_tag_object, "native preparation annotated tag object"
    )
    tag = str(annotated_tag).strip()
    if (
        not tag
        or "/" in tag
        or ".." in tag
        or platform.system() != "Linux"
        or platform.machine() != "x86_64"
        or sys.version_info[:2] != (3, 12)
    ):
        raise BuyE3TransactionalDeployError(
            "native preparation requires a simple annotated tag on Linux x86_64 Python 3.12"
        )
    if stage.is_symlink():
        raise BuyE3TransactionalDeployError("native preparation stage root is a symlink")
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(stage.stat().st_mode) != 0o700:
        raise BuyE3TransactionalDeployError("native preparation stage mode drifted")
    runtime_root = stage / f"runtime-{commit}"
    venv_root = stage / f"venv-{commit}"
    python = venv_root / "bin" / "python3"
    lock_path = stage / f"runtime-lock-{commit}.json"
    root_wheel_dir = stage / f"root-wheel-{commit}"
    wheel_dir = stage / f"native-wheel-{commit}"
    install_receipt = stage / f"locked-runtime-install-{commit}.json"
    receipt = stage / f"native-build-{commit}.json"
    seed = Path(os.path.abspath(os.path.expanduser(str(seed_python))))
    try:
        resolved_seed = seed.resolve(strict=True)
    except OSError as exc:
        raise BuyE3TransactionalDeployError(
            "native preparation seed Python does not resolve"
        ) from exc
    if not resolved_seed.is_file() or not seed.is_file():
        raise BuyE3TransactionalDeployError(
            "native preparation seed Python is not an executable file"
        )

    def run(argv: Sequence[str], *, cwd: Path = root, timeout: float = 900.0) -> None:
        safe_env = os.environ.copy()
        safe_env.update(
            {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1"}
        )
        completed = subprocess.run(
            tuple(str(value) for value in argv),
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=safe_env,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()[-1000:]
            raise BuyE3TransactionalDeployError(
                f"native preparation command failed: {argv[0]}: {detail}"
            )

    run(("git", "fetch", "--no-tags", "origin", f"refs/tags/{tag}:refs/tags/{tag}"))
    git_checks = {
        "type": subprocess.run(
            ("git", "cat-file", "-t", f"refs/tags/{tag}"),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "tag_object": subprocess.run(
            ("git", "rev-parse", f"refs/tags/{tag}"),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "commit": subprocess.run(
            ("git", "rev-parse", f"refs/tags/{tag}^{{}}"),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
    }
    if git_checks != {"type": "tag", "tag_object": tag_object, "commit": commit}:
        raise BuyE3TransactionalDeployError("native preparation tag identity drifted")
    if runtime_root.is_symlink():
        raise BuyE3TransactionalDeployError("native preparation worktree is a symlink")
    if not runtime_root.exists():
        run(("git", "worktree", "add", "--detach", str(runtime_root), commit))
    checkout = (
        subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=runtime_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        subprocess.run(
            ("git", "rev-parse", "HEAD^{tree}"),
            cwd=runtime_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        subprocess.run(
            ("git", "status", "--porcelain=v1", "--untracked-files=all"),
            cwd=runtime_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
    )
    if checkout != (commit, tree, ""):
        raise BuyE3TransactionalDeployError("native preparation worktree identity drifted")
    if receipt.exists() or receipt.is_symlink():
        raise BuyE3TransactionalDeployError(
            "native preparation is create-only; an existing receipt requires a "
            "separate externally frozen read-only validation workflow"
        )

    for path, label in (
        (venv_root, "venv"),
        (lock_path, "runtime lock"),
        (install_receipt, "install receipt"),
    ):
        if path.exists() or path.is_symlink():
            raise BuyE3TransactionalDeployError(
                f"native preparation {label} must be create-only"
            )
    for directory, label in (
        (root_wheel_dir, "root wheel directory"),
        (wheel_dir, "native wheel directory"),
    ):
        if directory.is_symlink() or (directory.exists() and any(directory.iterdir())):
            raise BuyE3TransactionalDeployError(
                f"native preparation {label} must be absent or empty"
            )
        directory.mkdir(mode=0o700, exist_ok=True)

    try:
        lock_result = locked_runtime.generate_lock(
            seed_python=seed,
            output_path=lock_path,
        )
        lock_sha256 = str(
            lock_result["lock"][locked_runtime.LOCK_CANONICAL_FIELD]
        )
        wheelhouse = stage / f"wheelhouse-{lock_sha256}"
        if wheelhouse.exists() or wheelhouse.is_symlink():
            raise BuyE3TransactionalDeployError(
                "dependency wheelhouse must be create-only for a new native receipt"
            )
        wheelhouse_result = locked_runtime.download_wheelhouse(
            lock_path=lock_path,
            expected_lock_sha256=lock_sha256,
            pip_python=seed,
            output_dir=wheelhouse,
        )
        wheelhouse_sha256 = str(
            wheelhouse_result["manifest"][locked_runtime.WHEELHOUSE_CANONICAL_FIELD]
        )
    except locked_runtime.LockedRuntimeError as exc:
        raise BuyE3TransactionalDeployError(
            f"locked dependency preparation failed: {exc}"
        ) from exc

    wheel_build = (
        "-B",
        "-m",
        "pip",
        "wheel",
        "--no-deps",
        "--no-build-isolation",
        "--disable-pip-version-check",
        "--no-input",
    )
    run(
        (str(seed), *wheel_build, "--wheel-dir", str(root_wheel_dir), str(runtime_root)),
        cwd=runtime_root,
    )
    run(
        (
            str(seed),
            *wheel_build,
            "--wheel-dir",
            str(wheel_dir),
            str(runtime_root / "cpp"),
        ),
        cwd=runtime_root,
    )
    root_wheels = sorted(root_wheel_dir.glob("*.whl"))
    wheels = sorted(wheel_dir.glob("*.whl"))
    if (
        len(root_wheels) != 1
        or len(wheels) != 1
        or any(
            wheel.is_symlink() or not wheel.is_file()
            for wheel in (*root_wheels, *wheels)
        )
    ):
        raise BuyE3TransactionalDeployError(
            "native preparation requires exactly one root and one native wheel"
        )
    try:
        root_binding = locked_runtime.inspect_wheel(root_wheels[0])
        native_binding = locked_runtime.inspect_wheel(wheels[0])
        install_result = locked_runtime.install_locked_runtime(
            builder_python=seed,
            venv_dir=venv_root,
            lock_path=lock_path,
            expected_lock_sha256=lock_sha256,
            wheelhouse_dir=wheelhouse,
            expected_wheelhouse_sha256=wheelhouse_sha256,
            root_wheel_path=root_wheels[0],
            root_wheel_sha256=str(root_binding["sha256"]),
            native_wheel_path=wheels[0],
            native_wheel_sha256=str(native_binding["sha256"]),
            receipt_path=install_receipt,
        )
        install_sha256 = str(
            install_result["receipt"][locked_runtime.INSTALL_CANONICAL_FIELD]
        )
    except locked_runtime.LockedRuntimeError as exc:
        raise BuyE3TransactionalDeployError(
            f"offline locked runtime installation failed: {exc}"
        ) from exc
    run(
        (
            str(python),
            "-B",
            str(runtime_root / "scripts" / "f05_live_safety_native_build_receipt.py"),
            "--repository-root",
            str(runtime_root),
            "--annotated-tag",
            tag,
            "--wheel",
            str(wheels[0]),
            "--builder-python",
            str(seed),
            "--runtime-lock",
            str(lock_path),
            "--runtime-lock-sha256",
            lock_sha256,
            "--dependency-wheelhouse",
            str(wheelhouse),
            "--dependency-wheelhouse-sha256",
            wheelhouse_sha256,
            "--root-wheel",
            str(root_wheels[0]),
            "--root-wheel-sha256",
            str(root_binding["sha256"]),
            "--install-receipt",
            str(install_receipt),
            "--install-receipt-sha256",
            install_sha256,
            "--output",
            str(receipt),
        ),
        cwd=runtime_root,
    )
    return {
        "status": "successor_native_prepared_before_deployment",
        "runtime_root": str(runtime_root),
        "venv_root": str(venv_root),
        "python_executable": str(python),
        "wheel_path": str(wheels[0]),
        "wheel_sha256": gate_v2.file_sha256(wheels[0]),
        "runtime_lock_path": str(lock_path),
        "runtime_lock_canonical_sha256": lock_sha256,
        "wheelhouse_path": str(wheelhouse),
        "wheelhouse_canonical_sha256": wheelhouse_sha256,
        "locked_runtime_install_receipt_path": str(install_receipt),
        "locked_runtime_install_receipt_canonical_sha256": install_sha256,
        "native_build_receipt_path": str(receipt),
        "native_build_receipt_sha256": gate_v2.file_sha256(receipt),
    }


def signed_exchange_reconciliation(config_path: Path, output_path: Path) -> dict[str, Any]:
    """Cancel residual orders and freeze one stable signed position snapshot."""

    from live.config import load_config
    from live.main import create_rest_client

    cfg = load_config(config_path)
    if not str(cfg.api.key).strip() or not str(cfg.api.secret).strip():
        raise BuyE3TransactionalDeployError(
            "signed exchange reconciliation requires API credentials"
        )
    rest = create_rest_client(cfg, dry_run=False)
    deadline = time.monotonic() + 30.0
    orders = rest.get_orders(symbol=cfg.symbol)
    if not isinstance(orders, list):
        raise BuyE3TransactionalDeployError("signed openOrders returned a non-list")
    if orders:
        rest.cancel_open_orders(symbol=cfg.symbol)
    while True:
        orders = rest.get_orders(symbol=cfg.symbol)
        if isinstance(orders, list) and not orders:
            break
        if time.monotonic() >= deadline:
            raise BuyE3TransactionalDeployError(
                "signed exchange openOrders did not converge to zero"
            )
        time.sleep(0.25)

    def position_snapshot() -> list[dict[str, str | int]]:
        raw = rest.get_position_risk(symbol=cfg.symbol)
        if not isinstance(raw, list) or not raw:
            raise BuyE3TransactionalDeployError(
                "signed positionRisk returned no position rows"
            )
        rows: list[dict[str, str | int]] = []
        for item in raw:
            if not isinstance(item, Mapping) or str(item.get("symbol", "")) != cfg.symbol:
                continue
            normalized: dict[str, str | int] = {
                "symbol": cfg.symbol,
                "position_side": str(item.get("positionSide", "BOTH")),
                "position_amt": str(item.get("positionAmt", "")),
                "entry_price": str(item.get("entryPrice", "")),
                "update_time_ms": int(item.get("updateTime", 0) or 0),
            }
            try:
                if not Decimal(str(normalized["position_amt"])).is_finite() or not Decimal(
                    str(normalized["entry_price"])
                ).is_finite():
                    raise InvalidOperation
            except (InvalidOperation, ValueError) as exc:
                raise BuyE3TransactionalDeployError(
                    "signed positionRisk contains a non-finite quantity or price"
                ) from exc
            rows.append(normalized)
        rows.sort(key=lambda row: str(row["position_side"]))
        if len(rows) != 1 or rows[0]["position_side"] != "BOTH":
            raise BuyE3TransactionalDeployError(
                "signed positionRisk must contain exactly one BOTH row"
            )
        return rows

    first = position_snapshot()
    if rest.get_orders(symbol=cfg.symbol) != []:
        raise BuyE3TransactionalDeployError(
            "exchange orders appeared during position reconciliation"
        )
    second = position_snapshot()
    if first != second:
        raise BuyE3TransactionalDeployError(
            "signed position changed during the stopped reconciliation barrier"
        )
    payload: dict[str, Any] = {
        "schema_version": "narrowgate_stopped_exchange_reconciliation.v1",
        "status": "signed_open_orders_zero_exact_position_stable",
        "symbol": cfg.symbol,
        "signed_endpoints": ["openOrders", "positionRisk"],
        "open_order_count": 0,
        "position_rows": second,
        "account_key_sha256": hashlib.sha256(
            str(cfg.api.key).encode("utf-8")
        ).hexdigest(),
    }
    payload["position_lineage_sha256"] = gate_v2.canonical_sha256(second)
    payload["canonical_exchange_reconciliation_sha256"] = gate_v2.document_sha256(
        payload, "canonical_exchange_reconciliation_sha256"
    )
    output = output_path.expanduser().absolute()
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    if stat.S_IMODE(output.stat().st_mode) != 0o600 or output.stat().st_nlink != 1:
        raise BuyE3TransactionalDeployError("exchange reconciliation receipt inode drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    _build_spec_parser(plan)
    plan.add_argument("--output", type=Path, required=True)
    prepare_native = subparsers.add_parser("prepare-successor-native")
    prepare_native.add_argument("--repository-root", type=Path, required=True)
    prepare_native.add_argument("--stage-root", type=Path, required=True)
    prepare_native.add_argument("--annotated-tag", required=True)
    prepare_native.add_argument("--annotated-tag-object", required=True)
    prepare_native.add_argument("--execution-commit", required=True)
    prepare_native.add_argument("--execution-tree", required=True)
    prepare_native.add_argument("--seed-python", type=Path, required=True)
    exchange = subparsers.add_parser("exchange-reconcile")
    exchange.add_argument("--config", type=Path, required=True)
    exchange.add_argument("--output", type=Path, required=True)
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
    process.add_argument("--policy", type=Path)
    process.add_argument("--predicate-bundle", type=Path)
    process.add_argument("--artifact-manifest-file-sha256", default="")
    process.add_argument("--policy-file-sha256", default="")
    process.add_argument("--predicate-bundle-file-sha256", default="")
    process.add_argument("--runtime-code-sha256", required=True)
    process.add_argument("--runtime-source-authority-base64", required=True)
    process.add_argument(
        "--expected-startup-attestation-schema-version",
        choices=(
            LEGACY_STARTUP_ATTESTATION_SCHEMA,
            HISTORICAL_STARTUP_ATTESTATION_SCHEMA,
            STARTUP_ATTESTATION_SCHEMA,
            SUCCESSOR_STARTUP_ATTESTATION_SCHEMA,
        ),
        required=True,
    )
    process.add_argument("--active-release", type=Path)
    process.add_argument("--active-release-file-sha256", default="")
    process.add_argument("--active-release-canonical-sha256", default="")
    process.add_argument("--safety-release", type=Path)
    process.add_argument("--safety-release-file-sha256", default="")
    process.add_argument("--safety-release-canonical-sha256", default="")
    process.add_argument("--safety-active-config-sha256", default="")
    process.add_argument("--safety-disabled-config-sha256", default="")
    process.add_argument("--expected-exchange-reconciliation-path", type=Path)
    install_release = subparsers.add_parser("install-active-release")
    install_release.add_argument("--source", type=Path, required=True)
    install_release.add_argument("--destination", type=Path, required=True)
    install_release.add_argument("--file-sha256", required=True)
    checkpoint = subparsers.add_parser("log-checkpoint")
    checkpoint.add_argument("--log", type=Path, required=True)
    checkpoint.add_argument("--output", type=Path, required=True)
    log_validate = subparsers.add_parser("log-validate")
    log_validate.add_argument("--log", type=Path, required=True)
    log_validate.add_argument("--checkpoint", type=Path, required=True)
    log_validate.add_argument("--marker", action="append", required=True)
    bind_activation = subparsers.add_parser("bind-activation")
    bind_activation.add_argument("--plan", type=Path, required=True)
    bind_activation.add_argument("--disabled-phase-receipt", type=Path, required=True)
    bind_activation.add_argument("--concurrent-resource-receipt", type=Path, required=True)
    bind_activation.add_argument("--runtime-regression-receipt", type=Path, required=True)
    bind_activation.add_argument("--sell-54-case-receipt", type=Path, required=True)
    bind_activation.add_argument("--output", type=Path, required=True)
    execute = subparsers.add_parser("execute")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--phase", choices=PHASES, required=True)
    token_source = execute.add_mutually_exclusive_group(required=True)
    token_source.add_argument("--token-file", type=Path)
    token_source.add_argument("--token-fd", type=int)
    execute.add_argument("--disabled-phase-receipt", type=Path)
    execute.add_argument("--activation-envelope", type=Path)
    execute.add_argument("--active-release", type=Path)
    execute.add_argument("--authorize-remote-mutation", action="store_true")
    execute.add_argument("--output", type=Path, required=True)
    return parser


def _reject_plaintext_cli_token(argv: Sequence[str]) -> None:
    if any(value == "--token" or value.startswith("--token=") for value in argv):
        raise BuyE3TransactionalDeployError(
            "plaintext --token is forbidden; use stdin, --token-fd, or a 0600 token file"
        )


def _reject_duplicate_cli_options(argv: Sequence[str]) -> None:
    repeatable = {"--marker"}
    seen: set[str] = set()
    for value in argv:
        if not value.startswith("--"):
            continue
        option = value.split("=", 1)[0]
        if option in seen and option not in repeatable:
            raise BuyE3TransactionalDeployError(f"duplicate CLI option is forbidden: {option}")
        seen.add(option)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    _reject_plaintext_cli_token(arguments)
    _reject_duplicate_cli_options(arguments)
    parser = _parser()
    args = parser.parse_args(arguments)
    command = args.command
    if command == "exchange-reconcile":
        payload = signed_exchange_reconciliation(args.config, args.output)
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "prepare-successor-native":
        payload = prepare_successor_native_runtime(
            repository_root=args.repository_root,
            stage_root=args.stage_root,
            annotated_tag=args.annotated_tag,
            annotated_tag_object=args.annotated_tag_object,
            execution_commit=args.execution_commit,
            execution_tree=args.execution_tree,
            seed_python=args.seed_python,
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
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
            expected_artifact_manifest_file_sha256=(
                args.artifact_manifest_file_sha256
            ),
            expected_policy_file_sha256=args.policy_file_sha256,
            expected_predicate_bundle_file_sha256=(
                args.predicate_bundle_file_sha256
            ),
            expected_runtime_code_sha256=args.runtime_code_sha256,
            runtime_source_authority_base64=args.runtime_source_authority_base64,
            expected_startup_attestation_schema_version=(
                args.expected_startup_attestation_schema_version
            ),
            artifact_manifest_path=args.artifact_manifest,
            policy_path=args.policy,
            predicate_bundle_path=args.predicate_bundle,
            active_release_path=args.active_release,
            expected_active_release_file_sha256=args.active_release_file_sha256,
            expected_active_release_canonical_sha256=(args.active_release_canonical_sha256),
            safety_release_path=args.safety_release,
            expected_safety_release_file_sha256=args.safety_release_file_sha256,
            expected_safety_release_canonical_sha256=(
                args.safety_release_canonical_sha256
            ),
            expected_safety_active_config_file_sha256=(
                args.safety_active_config_sha256
            ),
            expected_safety_disabled_config_file_sha256=(
                args.safety_disabled_config_sha256
            ),
            expected_exchange_reconciliation_path=(
                args.expected_exchange_reconciliation_path
            ),
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "install-active-release":
        payload = install_private_active_release(
            source_path=args.source,
            destination_path=args.destination,
            expected_file_sha256=args.file_sha256,
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
    if command == "bind-activation":
        payload = build_compatible_activation_envelope(
            plan=_read_json(args.plan),
            disabled_phase_receipt_path=args.disabled_phase_receipt,
            concurrent_resource_receipt_path=args.concurrent_resource_receipt,
            runtime_regression_receipt_path=args.runtime_regression_receipt,
            sell_54_case_receipt_path=args.sell_54_case_receipt,
            output_path=args.output,
        )
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "activation_envelope": payload["canonical_activation_envelope_sha256"],
                },
                sort_keys=True,
            )
        )
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
            activation_envelope_path=args.activation_envelope,
            active_release_path=args.active_release,
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
    "build_compatible_activation_envelope",
    "build_plan",
    "execute_phase",
    "capture_runtime_process_probe",
    "install_private_active_release",
    "isolated_config_preflight",
    "load_sha_bound_active_pointer",
    "phase_authorization_token_sha256",
    "prepare_successor_native_runtime",
    "run_isolated_preflight",
    "signed_exchange_reconciliation",
    "validate_b0_config_contract",
    "validate_compatible_activation_envelope",
    "validate_phase_receipt",
    "validate_plan",
]
