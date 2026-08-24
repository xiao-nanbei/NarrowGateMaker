from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import shlex
import stat
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
)
from scripts import deploy_f05_buy_e3_owner_v1 as subject

_RUNTIME_SOURCE_PATHS = {
    "live_buy_runtime": "strategy/boolean_cooldown_buy_e3.py",
    "maker_engine": "strategy/maker_engine.py",
    "live_config": "live/config.py",
    "live_runtime_policy": "live/runtime_policy.py",
    "live_main": "live/main.py",
}
_RUNTIME_SOURCE_FILES = {
    role: {
        "repository_relative_path": path,
        "artifact_manifest_sha256": hashlib.sha256(path.encode()).hexdigest(),
        "execution_commit_blob_sha256": hashlib.sha256(path.encode()).hexdigest(),
        "working_file_sha256": hashlib.sha256(path.encode()).hexdigest(),
    }
    for role, path in _RUNTIME_SOURCE_PATHS.items()
}
_RUNTIME_CODE_SHA256 = gate_v2.canonical_sha256(_RUNTIME_SOURCE_FILES)


def _token_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _preflight(enabled: bool) -> dict:
    payload = {
        "schema_version": subject.PREFLIGHT_SCHEMA,
        "status": "isolated_config_preflight_passed",
        "expected_enabled": enabled,
        "artifact_loaded_with_from_files": True,
        "artifact_sha256": "a" * 64,
        "host_bound_storage_gate": {
            "profile": "bounded_remote_spool",
            "status": "deferred_to_mandatory_remote_preflight",
            "deferred_on_planner_host": True,
            "mandatory_remote_preflight": True,
            "allowlisted_root": "/home/remote/formal_collection",
            "journal_root": "/home/remote/formal_collection/journal",
            "prospective_epoch_root": "/home/remote/formal_collection/epochs",
            "required_remote_checks": list(subject.HOST_BOUND_SPOOL_REMOTE_CHECKS),
        },
    }
    payload["canonical_preflight_sha256"] = gate_v2.document_sha256(
        payload, "canonical_preflight_sha256"
    )
    return payload


def _process_probe(
    plan: dict,
    phase: str,
    pid: int = 101,
    active_release_binding: dict | None = None,
    **overrides,
) -> str:
    expected = subject._expected_process_binding(plan, phase, active_release_binding)
    cmdline = [expected["python_executable"], "live/main.py", "--config", expected["config_path"]]
    runtime_identity_payload = _runtime_identity_payload(
        plan,
        phase,
        pid=pid,
        active_release_binding=active_release_binding,
    )
    runtime_identity_text = _runtime_identity(
        plan,
        phase,
        pid=pid,
        active_release_binding=active_release_binding,
    )
    runtime_identity_file_sha256 = hashlib.sha256(runtime_identity_text.encode()).hexdigest()
    payload = {
        "schema_version": gate_v2.PROCESS_IDENTITY_SCHEMA,
        "captured_utc": "2026-08-22T00:00:00Z",
        "pid": pid,
        "pid_start_ticks": 12345 + pid,
        "cmdline": cmdline,
        "cmdline_sha256": gate_v2.canonical_sha256(cmdline),
        "cwd": expected["repo_root"],
        "config_path": expected["config_path"],
        "config_sha256": expected["config_sha256"],
        "python_executable": expected["python_executable"],
        "python_binary_resolved": expected["python_executable"],
        "venv_root": expected["venv_root"],
        "runtime_identity": {
            "present": True,
            "path": "/remote/repo/logs/runtime_identity.json",
            "file_sha256": runtime_identity_file_sha256,
            "schema_version": subject.RUNTIME_IDENTITY_SCHEMA,
        },
        "execution_commit": expected["execution_commit"],
        "execution_tree": expected["execution_tree"],
        "runtime_identity_file_sha256": runtime_identity_file_sha256,
        "startup_attestation_sha256": gate_v2.canonical_sha256(
            runtime_identity_payload["startup_attestation"]
        ),
        "artifact_sha256": expected["artifact_sha256"],
        "runtime_code_sha256": expected["runtime_code_sha256"],
        "buy_e3_enabled": expected["enabled"],
        "owner_override_effective": expected["enabled"],
        "initial_buy_deadline_identity": "B0",
        "fill_cooldown_restore_mode": "fresh_b0_no_checkpoint",
        "initial_buy_remaining_ms": 0,
        "e3_deadline_imported": False,
        "active_release_path": expected["active_release"]["path"],
        "active_release_file_sha256": expected["active_release"]["file_sha256"],
        "active_release_canonical_sha256": expected["active_release"]["file_canonical_sha256"],
        "active_release_execution_commit": expected["active_release"]["execution_commit"],
        "active_release_execution_tree": expected["active_release"]["execution_tree"],
    }
    payload.update(overrides)
    payload["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        payload, "canonical_process_identity_sha256"
    )
    return json.dumps(payload)


def _runtime_identity_payload(
    plan: dict,
    phase: str,
    *,
    pid: int,
    active_release_binding: dict | None = None,
    repository_root: str | None = None,
) -> dict:
    expected = subject._expected_process_binding(plan, phase, active_release_binding)
    runtime_repository_root = repository_root or expected["repo_root"]
    source_hashes = {
        binding["repository_relative_path"]: binding["working_file_sha256"]
        for binding in plan["runtime_sources"]["files"].values()
    }
    for extra_path in (
        "live/ws_handler.py",
        "strategy/boolean_cooldown_live.py",
        "strategy/signal.py",
        "strategy/global_flow.py",
        "strategy/global_reference.py",
    ):
        source_hashes[extra_path] = hashlib.sha256(extra_path.encode()).hexdigest()
    source_rows = [
        {
            "path": path,
            "working_file_sha256": sha256,
            "head_blob_sha256": sha256,
            "working_size_bytes": index + 1,
            "head_blob_size_bytes": index + 1,
            "matches_head_blob": True,
        }
        for index, (path, sha256) in enumerate(sorted(source_hashes.items()))
    ]
    git_snapshot = {
        "commit": expected["execution_commit"],
        "tree": expected["execution_tree"],
        "status_porcelain_sha256": hashlib.sha256(b"").hexdigest(),
        "status_entry_count": 0,
        "worktree_clean": True,
        "snapshot_internally_stable": True,
    }
    checkout = {
        "schema_version": subject.RUNNING_CHECKOUT_SCHEMA,
        "git_commit": expected["execution_commit"],
        "git_tree": expected["execution_tree"],
        "git_worktree_clean": True,
        "pre_snapshot": dict(git_snapshot),
        "post_snapshot": dict(git_snapshot),
        "stable_snapshot": {
            "pre_snapshot_internally_stable": True,
            "post_snapshot_internally_stable": True,
            "commit_identical": True,
            "tree_identical": True,
            "status_identical": True,
            "runtime_files_match_head": True,
            "stable": True,
        },
        "runtime_source_file_count": len(source_rows),
        "runtime_source_manifest_sha256": subject._runtime_source_manifest_sha256(source_rows),
        "runtime_source_files": source_rows,
    }
    loaded_roles = {
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
    loaded_module_origins = {
        role: {
            "module_name": module_name,
            "origin_path": str(Path(runtime_repository_root) / relative_path),
            "repository_relative_path": relative_path,
            "source_sha256": source_hashes[relative_path],
        }
        for role, (module_name, relative_path) in loaded_roles.items()
    }
    interpreter_file = {
        "reported_path": expected["python_executable"],
        "resolved_path": expected["python_executable"],
        "sha256": "9" * 64,
        "size_bytes": 123,
    }
    native_runtime = {
        "profile": "unmanaged",
        "module": "disabled",
        "NARROWGATE_CPP_QUOTE_CORE": False,
        "NARROWGATE_CPP_SIGNAL_FEATURES": False,
        "NARROWGATE_CPP_GLOBAL_FLOW": False,
        "NARROWGATE_CPP_LIVE_ROUTING": False,
        "NARROWGATE_CPP_STRICT": False,
        "NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED": False,
        "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE": False,
    }
    startup_release = subject._empty_active_release_identity()
    if expected["enabled"] and active_release_binding is not None:
        startup_release = {
            **expected["active_release"],
            "annotated_operational_tag": plan["execution"]["annotated_tag"],
            "annotated_operational_tag_object": plan["execution"]["annotated_tag_object"],
        }
    payload = {
        "schema_version": subject.RUNTIME_IDENTITY_SCHEMA,
        "recorded_at_utc": "2026-08-22T00:00:00Z",
        "pid": pid,
        "python_executable": expected["python_executable"],
        "config_path": expected["config_path"],
        "config_sha256": expected["config_sha256"],
        "f05_buy_e3_enabled": expected["enabled"],
        "f05_buy_e3_owner_override_effective": expected["enabled"],
        "f05_buy_e3_artifact_sha256": expected["artifact_sha256"],
        "f05_buy_e3_active_release_authority_schema_version": (
            subject.ACTIVE_RELEASE_RUNTIME_AUTHORITY_SCHEMA
        ),
        "f05_buy_e3_required": bool(expected["enabled"]),
        "f05_buy_e3_active_release_path": expected["active_release"]["path"],
        "f05_buy_e3_active_release_file_sha256": expected["active_release"]["file_sha256"],
        "f05_buy_e3_active_release_canonical_sha256": expected["active_release"][
            "file_canonical_sha256"
        ],
        "native_runtime": native_runtime,
        "startup_attestation": {
            "schema_version": expected["startup_attestation_schema_version"],
            "status": "accepted",
            "attested_at_utc": "2026-08-22T00:00:01Z",
            "fill_cooldown_state": {
                "schema_version": subject.FILL_COOLDOWN_STATE_SCHEMA,
                "reset_policy": "fresh_process_b0",
                "restore_mode": "fresh_b0_no_checkpoint",
                "checkpoint_loaded": False,
                "checkpoint_sequence": 0,
                "consec_buy": 0.0,
                "consec_sell": 0.0,
                "buy_remaining_ms": 0,
                "sell_remaining_ms": 0,
                "last_buy_fill_ts_ms": 0,
                "last_sell_fill_ts_ms": 0,
                "last_fill_side": "",
                "buy_deadline_identity": "B0",
                "sell_deadline_identity": "B0",
                "snapshot_ts_ms": 1,
            },
            "buy_e3_active_release": startup_release,
            "shadow_runtime_identity": {
                "schema_version": "narrowgate_shadow_runtime_identity.v1",
                "global_flow_shadow_enabled": False,
                "global_reference_shadow_enabled": False,
                "global_flow_native_requested": False,
                "global_flow_native_effective": False,
                "global_flow_backend": {
                    "native": 0,
                    "market_count": 0,
                    "trade_batches": 0,
                    "trade_events_seen": 0,
                    "trade_events_accepted": 0,
                    "book_events_seen": 0,
                    "book_events_accepted": 0,
                    "out_of_order_events": 0,
                    "stale_trade_events": 0,
                    "trade_overflow_events": 0,
                    "book_overflow_events": 0,
                },
                "global_reference_bridge_basis_sample_count": 0,
                "state_restore_contract": "shadow_state_never_restored",
                "global_flow_shadow_config_explicit": True,
                "global_reference_shadow_config_explicit": True,
            },
            "gates": {name: True for name in subject._STARTUP_GATE_FIELDS},
            "running_checkout": checkout,
            "loaded_module_origins": loaded_module_origins,
            "interpreter_identity": {
                "schema_version": subject.INTERPRETER_IDENTITY_SCHEMA,
                "version": "3.12.13",
                "before": dict(interpreter_file),
                "after": dict(interpreter_file),
                "stable": True,
            },
            "native_runtime_identity": {
                "schema_version": subject.NATIVE_RUNTIME_IDENTITY_SCHEMA,
                "profile": "unmanaged",
                "platform": "linux",
                "enabled": False,
                "reported_module_path": "disabled",
                "loaded_module_origin_path": None,
                "before": None,
                "after": None,
                "stable": True,
            },
            "errors": [],
        },
    }
    if (
        expected["startup_attestation_schema_version"]
        == subject.HISTORICAL_STARTUP_ATTESTATION_SCHEMA
    ):
        startup = payload["startup_attestation"]
        startup.pop("shadow_runtime_identity")
        startup["buy_e3_active_release"].pop("active_config_file_sha256")
        startup["buy_e3_active_release"].pop("disabled_config_file_sha256")
        for field in subject._STARTUP_GATE_FIELDS - subject._HISTORICAL_STARTUP_GATE_FIELDS:
            startup["gates"].pop(field)
        for role in (
            subject._LOADED_RUNTIME_MODULE_ROLES
            - subject._HISTORICAL_LOADED_RUNTIME_MODULE_ROLES
        ):
            startup["loaded_module_origins"].pop(role)
        payload["native_runtime"].pop("NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED")
        payload["native_runtime"].pop("NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE")
    return payload


def _runtime_identity(
    plan: dict,
    phase: str,
    *,
    pid: int,
    active_release_binding: dict | None = None,
) -> str:
    return (
        json.dumps(
            _runtime_identity_payload(
                plan,
                phase,
                pid=pid,
                active_release_binding=active_release_binding,
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def _rollback(name: str, commit: str) -> dict:
    return {
        "identity": name,
        "execution_commit": commit,
        "execution_tree": "2" * 40,
        "config_path": f"/remote/{name}.yaml",
        "config_sha256": "3" * 64,
        "python_executable": "/remote/repo/.venv/bin/python",
        "venv_root": "/remote/repo/.venv",
        "runtime_code_sha256": "4" * 64,
        "buy_e3_enabled": False,
        "buy_deadline_identity": "B0",
        "imports_e3_deadline": False,
    }


def _specification(tmp_path: Path) -> dict:
    manifest = tmp_path / "artifact_manifest.json"
    policy = tmp_path / "policy.json"
    bundle = tmp_path / "predicate_bundle.json"
    disabled = tmp_path / "disabled.yaml"
    active = tmp_path / "active.yaml"
    pointer = tmp_path / "pointer.json"
    known = tmp_path / "known_hosts"
    manifest.write_text(
        json.dumps({"artifact_sha256": "a" * 64}) + "\n",
        encoding="ascii",
    )
    policy.write_text(
        json.dumps({"bindings": {"owner_execution_commit": gate_v2.FROZEN_EXECUTION_COMMIT}})
        + "\n",
        encoding="ascii",
    )
    bundle.write_text("{}\n", encoding="ascii")
    config_strategy = {
        "fill_cooldown": 85,
        "buy_e3_cooldown_artifact_manifest_path": ("live/private/e3/artifact_manifest.json"),
        "buy_e3_cooldown_policy_path": "live/private/e3/policy.json",
        "buy_e3_cooldown_predicate_bundle_path": ("live/private/e3/predicate_bundle.json"),
    }
    lifecycle = {
        "enabled": True,
        "storage_profile": "bounded_remote_spool",
        "required_mount": "/home/remote/formal_collection",
        "root": "/home/remote/formal_collection/journal",
        "prospective_epoch_root": "/home/remote/formal_collection/epochs",
        "remote_spool_allowlisted_roots": ["/home/remote/formal_collection"],
    }
    disabled.write_text(
        yaml.safe_dump(
            {"strategy": config_strategy, "lifecycle_journal_v2": lifecycle},
            sort_keys=True,
        ),
        encoding="ascii",
    )
    active.write_text(
        yaml.safe_dump(
            {"strategy": config_strategy, "lifecycle_journal_v2": lifecycle},
            sort_keys=True,
        ),
        encoding="ascii",
    )
    pointer.write_text("{}\n", encoding="ascii")
    known.write_text("host ssh-ed25519 ZmFrZQ==\n", encoding="ascii")
    return {
        "execution": {
            "commit": gate_v2.FROZEN_EXECUTION_COMMIT,
            "tree": "5" * 40,
            "annotated_tag": "f05-owner-buy-e3-live-attempt2-20260821",
            "annotated_tag_object": "6" * 40,
        },
        "artifact": {
            "manifest_path": str(manifest),
            "policy_path": str(policy),
            "predicate_bundle_path": str(bundle),
        },
        "configs": {
            "disabled_path": str(disabled),
            "active_path": str(active),
            "allowed_diff": ["strategy.buy_e3_cooldown_policy_enabled"],
        },
        "active_pointer": {"path": str(pointer), "file_sha256": "7" * 64},
        "ssh": {
            "known_hosts_path": str(known),
            "known_hosts_file_sha256": "8" * 64,
            "host_key_fingerprint": "SHA256:fake",
        },
        "host": {
            "logical_host": "current-live",
            "repo_root": "/remote/repo",
            "python_executable": "/remote/repo/.venv/bin/python",
            "venv_root": "/remote/repo/.venv",
        },
        "remote": {
            "stage_root": "/remote/stage/e3",
            "disabled_config_path": "/remote/repo/live/private/e3/disabled.yaml",
            "active_config_path": "/remote/repo/live/private/e3/active.yaml",
            "pid_file": "/remote/repo/logs/maker.pid",
            "log_path": "/remote/repo/logs/maker.log",
            "runtime_identity_path": "/remote/repo/logs/runtime_identity.json",
            "artifact_manifest_path": "/remote/repo/live/private/e3/artifact_manifest.json",
            "policy_path": "/remote/repo/live/private/e3/policy.json",
            "predicate_bundle_path": "/remote/repo/live/private/e3/predicate_bundle.json",
            "startup_checkpoint_path": "/remote/repo/logs/e3_startup_checkpoint.json",
            "startup_markers": ["runtime identity", "HEALTH"],
        },
        "rollback_identities": {
            "primary_disabled": {
                **_rollback("primary", gate_v2.FROZEN_EXECUTION_COMMIT),
                "execution_tree": "5" * 40,
                "config_path": "/remote/repo/live/private/e3/disabled.yaml",
                "config_sha256": "c" * 64,
                "runtime_code_sha256": _RUNTIME_CODE_SHA256,
            },
            "deep_predecessor": {
                **_rollback("deep", gate_v2.FROZEN_EXECUTION_COMMIT),
                "execution_tree": "5" * 40,
                "runtime_code_sha256": _RUNTIME_CODE_SHA256,
            },
        },
        "phase_token_sha256": {phase: _token_hash(f"token-{phase}") for phase in subject.PHASES},
    }


def _activation_gate_payload(spec: dict) -> dict:
    payload = {
        "schema_version": gate_v2.SCHEMA_VERSION,
        "status": "disabled_deploy_gate_passed_activation_not_yet_authorized",
        "execution_identity": {
            "execution_commit": gate_v2.FROZEN_EXECUTION_COMMIT,
            "execution_tree": "5" * 40,
            "annotated_tag": spec["execution"]["annotated_tag"],
            "annotated_tag_object": "6" * 40,
            "tag_peeled_commit": gate_v2.FROZEN_EXECUTION_COMMIT,
        },
        "runtime_sources": {"runtime_code_sha256": _RUNTIME_CODE_SHA256},
        "artifact_binding": {"artifact_sha256": "a" * 64},
        "config_binding": {
            "disabled": {"enabled": False, "config_sha256": "c" * 64},
            "active": {"enabled": True, "config_sha256": "d" * 64},
        },
        "host_binding": {
            "active_pointer_file_sha256": "7" * 64,
            "known_hosts_file_sha256": "8" * 64,
            "host_key_fingerprint": "SHA256:fake",
            "repo_root": "/remote/repo",
            "python_executable": "/remote/repo/.venv/bin/python",
            "venv_root": "/remote/repo/.venv",
        },
        "rollback_identities": {
            name: dict(identity) for name, identity in spec["rollback_identities"].items()
        },
        "activation_contract": {
            "restart_only": True,
            "sighup_allowed": False,
            "fresh_pid_required": True,
        },
        "permissions": {"live_authorized": False},
    }
    payload["canonical_amendment_receipt_sha256"] = gate_v2.document_sha256(
        payload, "canonical_amendment_receipt_sha256"
    )
    return payload


def _bind_activation_gate(tmp_path: Path, spec: dict, payload: dict | None = None) -> Path:
    receipt = tmp_path / "activation_gate.json"
    body = payload or _activation_gate_payload(spec)
    receipt.write_text(json.dumps(body, sort_keys=True) + "\n", encoding="ascii")
    receipt.chmod(0o600)
    spec["activation_gate"] = {
        "path": str(receipt),
        "file_sha256": gate_v2.file_sha256(receipt),
    }
    return receipt


def _recanonicalize_plan(plan: dict) -> None:
    plan["plan_core_sha256"] = subject._plan_core_sha256(plan)
    activation = plan.get("activation_gate")
    if isinstance(activation, dict):
        activation["plan_core_sha256"] = plan["plan_core_sha256"]
        activation["transaction_contract_sha256"] = gate_v2.canonical_sha256(
            plan["transaction_contract"]
        )
        activation["canonical_activation_binding_sha256"] = gate_v2.document_sha256(
            activation, "canonical_activation_binding_sha256"
        )
        plan["activation_gate_receipt_sha256"] = activation["canonical_receipt_sha256"]
    plan["canonical_plan_sha256"] = gate_v2.document_sha256(plan, "canonical_plan_sha256")


def _successful_runner(
    plan: dict,
    phase: str,
    *,
    disabled_process: dict | None = None,
    active_release_binding: dict | None = None,
):
    commands: list[str] = []
    disabled_process = disabled_process or json.loads(
        _process_probe(plan, "disabled-deploy", pid=101)
    )
    main_rows = subject._execution_rows(plan, phase, active_release_binding)
    main_index = 0
    fallback_labels = {
        tuple(row["argv"]): row["label"] for rows in plan["phases"].values() for row in rows
    }
    fallback_labels.update({tuple(row["argv"]): row["label"] for row in main_rows})
    disabled_runtime = _runtime_identity(plan, "disabled-deploy", pid=disabled_process["pid"])
    active_runtime = _runtime_identity(
        plan,
        "activate",
        pid=202,
        active_release_binding=active_release_binding,
    )

    def run(command):
        nonlocal main_index
        commands.append(" ".join(command))
        if main_index < len(main_rows) and tuple(command) == tuple(main_rows[main_index]["argv"]):
            label = main_rows[main_index]["label"]
            main_index += 1
        else:
            label = fallback_labels.get(tuple(command), "")
        if label == "capture-old-pid":
            output = f"{disabled_process['pid'] if phase == 'activate' else 100}\n"
        elif label == "reprobe-disabled-process-before-stop":
            output = json.dumps(disabled_process)
        elif label in {
            "read-disabled-runtime-identity",
            "read-pre-stop-disabled-runtime-identity",
        }:
            output = disabled_runtime
        elif label == "read-active-runtime-identity":
            output = active_runtime
        elif label == "fresh-active-process-probe":
            output = _process_probe(
                plan,
                "activate",
                pid=202,
                active_release_binding=active_release_binding,
            )
        elif label == "fresh-disabled-process-probe":
            output = json.dumps(disabled_process)
        elif label == "fresh-rollback-process-probe":
            output = _process_probe(plan, "rollback-primary", pid=303)
        else:
            output = "ok"
        return subprocess.CompletedProcess(command, 0, output, "")

    return run, commands


def _successful_disabled_receipt(tmp_path: Path, plan: dict) -> tuple[Path, dict, dict]:
    disabled_process = json.loads(_process_probe(plan, "disabled-deploy", pid=101))
    runner, _commands = _successful_runner(
        plan, "disabled-deploy", disabled_process=disabled_process
    )
    path = tmp_path / "disabled-phase.json"
    receipt = subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=path,
    )
    return path, receipt, disabled_process


def _set_nested(payload: dict, path: tuple[str, ...], value) -> None:
    cursor = payload
    for field in path[:-1]:
        cursor = cursor[field]
    cursor[path[-1]] = value


def _command_label(plan: dict, command: Sequence[str]) -> str:
    matches = [
        row["label"]
        for rows in plan["phases"].values()
        for row in rows
        if tuple(row["argv"]) == tuple(command)
    ]
    return matches[0] if matches else ""


def _write_receipt(path: Path, payload: dict, *, recanonicalize: bool = True) -> Path:
    if recanonicalize:
        payload["canonical_receipt_sha256"] = gate_v2.document_sha256(
            payload, "canonical_receipt_sha256"
        )
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _patch_plan_dependencies(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, spec: dict) -> None:
    artifact_files = {
        "manifest": {
            "path": spec["artifact"]["manifest_path"],
            "sha256": gate_v2.file_sha256(Path(spec["artifact"]["manifest_path"])),
        },
        "policy": {
            "path": spec["artifact"]["policy_path"],
            "sha256": gate_v2.file_sha256(Path(spec["artifact"]["policy_path"])),
        },
        "predicate_bundle": {
            "path": spec["artifact"]["predicate_bundle_path"],
            "sha256": gate_v2.file_sha256(Path(spec["artifact"]["predicate_bundle_path"])),
        },
    }
    monkeypatch.setattr(
        gate_v2,
        "verify_execution_git_identity",
        lambda **_: {
            "execution_commit": gate_v2.FROZEN_EXECUTION_COMMIT,
            "execution_tree": "5" * 40,
            "annotated_tag": spec["execution"]["annotated_tag"],
            "annotated_tag_object": "6" * 40,
            "tag_peeled_commit": gate_v2.FROZEN_EXECUTION_COMMIT,
        },
    )
    monkeypatch.setattr(
        gate_v2,
        "verify_runtime_sources",
        lambda **_: {
            "files": copy.deepcopy(_RUNTIME_SOURCE_FILES),
            "runtime_code_sha256": _RUNTIME_CODE_SHA256,
        },
    )
    monkeypatch.setattr(
        gate_v2,
        "validate_private_config_pair",
        lambda **_: {
            "disabled": {
                "enabled": False,
                "artifact_sha256": "a" * 64,
                "artifact_files": artifact_files,
                "config_sha256": "c" * 64,
            },
            "active": {
                "enabled": True,
                "artifact_sha256": "a" * 64,
                "artifact_files": artifact_files,
                "config_sha256": "d" * 64,
            },
            "allowlisted_diff": ["strategy.buy_e3_cooldown_policy_enabled"],
            "allowlisted_diff_sha256": "e" * 64,
            "observed_diff": ["strategy.buy_e3_cooldown_policy_enabled"],
        },
    )
    monkeypatch.setattr(
        subject,
        "load_sha_bound_active_pointer",
        lambda **_: {
            "path": str(tmp_path / "pointer.json"),
            "file_sha256": "7" * 64,
            "ssh_target": "user@host",
            "repo_root": "/remote/repo",
            "provider": "fake",
            "region": "fake",
            "public_ipv4": "127.0.0.1",
        },
    )
    monkeypatch.setattr(
        subject,
        "bind_known_hosts",
        lambda **_: {
            "path": str(tmp_path / "known_hosts"),
            "file_sha256": "8" * 64,
            "expected_fingerprint": "SHA256:fake",
            "observed_fingerprints": ["SHA256:fake"],
        },
    )
    monkeypatch.setattr(
        gate_v2,
        "validate_amended_gate_receipt",
        lambda path: json.loads(Path(path).read_text(encoding="ascii")),
    )


def _plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    activation_gate: bool = False,
) -> tuple[dict, dict]:
    spec = _specification(tmp_path)
    if activation_gate:
        _bind_activation_gate(tmp_path, spec)
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)
    plan = subject.build_plan(
        specification=spec,
        repository_root=tmp_path,
        preflight_runner=lambda _repo, _config, enabled: _preflight(enabled),
    )
    return plan, spec


def test_actual_native_audit_roundtrips_current_v5_and_tamper_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from live import main as live_main

    plan, _spec = _plan(tmp_path, monkeypatch)
    for name in live_main.CPP_RUNTIME_FLAGS:
        monkeypatch.setenv(name, "0")
    monkeypatch.delenv("NARROWGATE_CPP_PROFILE", raising=False)
    native_runtime = live_main.audit_native_runtime(
        Mock(),
        cfg=SimpleNamespace(
            multi_market=SimpleNamespace(global_flow_shadow_enabled=False)
        ),
    )
    assert set(native_runtime) == {
        "profile",
        "module",
        *live_main.CPP_RUNTIME_FLAGS,
        "NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED",
        "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE",
    }
    runtime = _runtime_identity_payload(
        plan,
        "disabled-deploy",
        pid=377,
    )
    runtime["native_runtime"] = native_runtime
    expected = subject._expected_process_binding(plan, "disabled-deploy")

    def validate(candidate: dict) -> dict:
        return subject._validate_runtime_identity_authority(
            candidate,
            expected_pid=377,
            expected_config_path=expected["config_path"],
            expected_config_sha256=expected["config_sha256"],
            expected_python_executable=expected["python_executable"],
            expected_python_binary_resolved=expected["python_executable"],
            expected_enabled=False,
            expected_artifact_sha256=expected["artifact_sha256"],
            expected_execution_commit=expected["execution_commit"],
            expected_execution_tree=expected["execution_tree"],
            expected_runtime_sources=plan["runtime_sources"],
            expected_repository_root=expected["repo_root"],
            expected_startup_attestation_schema_version=(
                subject.STARTUP_ATTESTATION_SCHEMA
            ),
            expected_active_release=None,
        )

    assert validate(runtime) == runtime["startup_attestation"]
    mutations = (
        lambda value: value["native_runtime"].pop(
            "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE"
        ),
        lambda value: value["native_runtime"].__setitem__("unexpected", False),
        lambda value: value["native_runtime"].__setitem__(
            "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE", True
        ),
        lambda value: value["native_runtime"].__setitem__(
            "NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED", True
        ),
    )
    for mutation in mutations:
        tampered = copy.deepcopy(runtime)
        mutation(tampered)
        with pytest.raises(
            subject.BuyE3TransactionalDeployError,
            match="runtime native attestation",
        ):
            validate(tampered)

    origin_mutations = (
        lambda value: value["startup_attestation"]["loaded_module_origins"].pop(
            "global_reference"
        ),
        lambda value: value["startup_attestation"]["loaded_module_origins"].__setitem__(
            "unexpected",
            copy.deepcopy(
                value["startup_attestation"]["loaded_module_origins"][
                    "global_reference"
                ]
            ),
        ),
        lambda value: value["startup_attestation"]["loaded_module_origins"][
            "global_reference"
        ].__setitem__("source_sha256", "0" * 64),
        lambda value: value["startup_attestation"]["loaded_module_origins"][
            "global_reference"
        ].__setitem__("origin_path", "/outside/strategy/global_reference.py"),
        lambda value: value["startup_attestation"]["loaded_module_origins"][
            "global_reference"
        ].__setitem__("module_name", "strategy.global_flow"),
        lambda value: value["startup_attestation"]["loaded_module_origins"][
            "global_reference"
        ].__setitem__("repository_relative_path", "strategy/global_flow.py"),
    )
    for mutation in origin_mutations:
        tampered = copy.deepcopy(runtime)
        mutation(tampered)
        with pytest.raises(
            subject.BuyE3TransactionalDeployError,
            match="loaded module origin",
        ):
            validate(tampered)


def test_runtime_source_authority_validates_actual_git_checkout_and_tamper(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "runtime-repo"
    repo.mkdir()
    for index, relative in enumerate(gate_v2.REQUIRED_RUNTIME_PATHS.values()):
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"runtime-source-{index}\n", encoding="ascii")
    subprocess.run(("git", "init", "-q"), cwd=repo, check=True)
    subprocess.run(("git", "add", "."), cwd=repo, check=True)
    subprocess.run(
        (
            "git",
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ),
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    files = {}
    for role, relative in gate_v2.REQUIRED_RUNTIME_PATHS.items():
        sha256 = gate_v2.file_sha256(repo / relative)
        files[role] = {
            "repository_relative_path": relative,
            "artifact_manifest_sha256": sha256,
            "execution_commit_blob_sha256": sha256,
            "working_file_sha256": sha256,
        }
    aggregate = gate_v2.canonical_sha256(files)
    authority = {"files": files, "runtime_code_sha256": aggregate}

    assert subject._validate_checkout_runtime_source_authority(
        repository_root=repo,
        execution_commit=commit,
        runtime_sources=authority,
        expected_runtime_code_sha256=aggregate,
    ) == authority
    encoded = subject._encode_runtime_source_authority(authority)
    assert subject._decode_runtime_source_authority(encoded) == authority

    for mutate in (
        lambda value: value["files"].__setitem__("unexpected", {}),
        lambda value: value["files"]["live_main"].__setitem__(
            "working_file_sha256", "0" * 64
        ),
    ):
        tampered = copy.deepcopy(authority)
        mutate(tampered)
        with pytest.raises(
            subject.BuyE3TransactionalDeployError,
            match="runtime source authority",
        ):
            subject._validate_checkout_runtime_source_authority(
                repository_root=repo,
                execution_commit=commit,
                runtime_sources=tampered,
                expected_runtime_code_sha256=aggregate,
            )
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="base64"):
        subject._decode_runtime_source_authority("not-base64!")


def test_generated_process_probe_cli_roundtrips_parser_and_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    captured_kwargs: list[dict] = []
    monkeypatch.setattr(
        subject,
        "capture_runtime_process_probe",
        lambda **kwargs: captured_kwargs.append(kwargs) or {"status": "accepted"},
    )
    for phase in ("disabled-deploy", "rollback-deep"):
        row = next(
            item for item in plan["phases"][phase] if "process-probe" in item["label"]
        )
        remote_tokens = shlex.split(str(row["argv"][-1]))
        command_index = remote_tokens.index("process-probe")
        cli = remote_tokens[command_index:]
        parsed = subject._parser().parse_args(cli)
        assert parsed.command == "process-probe"
        assert subject.main(cli) == 0
    assert len(captured_kwargs) == 2
    assert captured_kwargs[0]["expected_startup_attestation_schema_version"] == (
        subject.STARTUP_ATTESTATION_SCHEMA
    )
    assert subject._decode_runtime_source_authority(
        captured_kwargs[0]["runtime_source_authority_base64"]
    ) == plan["runtime_sources"]
    assert captured_kwargs[1]["expected_artifact_sha256"] == ""
    assert captured_kwargs[1]["artifact_manifest_path"] is None
    assert captured_kwargs[1]["policy_path"] is None
    assert captured_kwargs[1]["predicate_bundle_path"] is None
    assert capsys.readouterr().out.count('"status": "accepted"') == 2
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="duplicate CLI option",
    ):
        subject.main(
            [
                *cli,
                "--expected-startup-attestation-schema-version",
                subject.STARTUP_ATTESTATION_SCHEMA,
            ]
        )

def test_deploy_attestation_schema_matches_runtime_writer() -> None:
    from live import main as live_main

    empty_attestation = live_main._empty_startup_attestation()
    assert subject.STARTUP_ATTESTATION_SCHEMA == live_main.STARTUP_ATTESTATION_SCHEMA
    assert subject.RUNNING_CHECKOUT_SCHEMA == live_main.RUNNING_CHECKOUT_SCHEMA
    assert subject._STARTUP_GATE_FIELDS == frozenset(live_main.STARTUP_ATTESTATION_GATE_NAMES)
    assert subject._LOADED_RUNTIME_MODULE_ROLES == frozenset(live_main.KEY_LOADED_RUNTIME_MODULES)
    # The runtime's rejected placeholder omits the release field; accepted v5
    # attestations include it and are validated strictly below.
    assert set(empty_attestation) | {"buy_e3_active_release"} == (
        subject._STARTUP_ATTESTATION_FIELDS
    )


def _capture_runtime_probe_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    runtime_mutator=None,
    gate_runtime_file_sha256: str | None = None,
) -> tuple[dict, str, int]:
    plan, spec = _plan(tmp_path, monkeypatch)
    pid = 4242
    pid_start_ticks = 987_654
    pid_file = tmp_path / "live.pid"
    pid_file.write_text(f"{pid}\n", encoding="ascii")
    config_path = Path(spec["configs"]["disabled_path"]).resolve(strict=True)
    config_sha256 = gate_v2.file_sha256(config_path)
    python_executable = tmp_path / ".venv/bin/python"
    python_executable.parent.mkdir(parents=True, exist_ok=True)
    python_executable.write_text("fixture-python\n", encoding="ascii")
    venv_root = python_executable.parents[1]
    runtime_identity_path = tmp_path / "runtime_identity.json"
    artifact_manifest_path = Path(spec["artifact"]["manifest_path"])

    runtime = _runtime_identity_payload(
        plan,
        "disabled-deploy",
        pid=pid,
        repository_root=str(tmp_path.resolve()),
    )
    runtime.update(
        {
            "python_executable": str(python_executable.absolute()),
            "config_path": str(config_path),
            "config_sha256": config_sha256,
        }
    )
    for timing in ("before", "after"):
        runtime["startup_attestation"]["interpreter_identity"][timing].update(
            {
                "reported_path": str(python_executable.absolute()),
                "resolved_path": str(python_executable.resolve(strict=True)),
            }
        )
    if runtime_mutator is not None:
        runtime_mutator(runtime)
    runtime_text = json.dumps(runtime, indent=2, sort_keys=True) + "\n"
    runtime_identity_path.write_text(runtime_text, encoding="ascii")
    runtime_file_sha256 = hashlib.sha256(runtime_text.encode()).hexdigest()

    cmdline = [
        str(python_executable.absolute()),
        "live/main.py",
        "--config",
        str(config_path),
    ]
    captured = {
        "schema_version": gate_v2.PROCESS_IDENTITY_SCHEMA,
        "captured_utc": "2026-08-22T00:00:02Z",
        "pid": pid,
        "pid_start_ticks": pid_start_ticks,
        "cmdline": cmdline,
        "cmdline_sha256": gate_v2.canonical_sha256(cmdline),
        "cwd": str(tmp_path.resolve()),
        "config_path": str(config_path),
        "config_sha256": config_sha256,
        "python_executable": str(python_executable.absolute()),
        "python_binary_resolved": str(python_executable.resolve(strict=True)),
        "venv_root": str(venv_root.absolute()),
        "runtime_identity": {
            "present": True,
            "path": str(runtime_identity_path.resolve(strict=True)),
            "file_sha256": gate_runtime_file_sha256 or runtime_file_sha256,
            "schema_version": subject.RUNTIME_IDENTITY_SCHEMA,
        },
    }
    captured["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        captured, "canonical_process_identity_sha256"
    )
    monkeypatch.setattr(
        gate_v2,
        "capture_actual_process_identity",
        lambda **_kwargs: copy.deepcopy(captured),
    )

    def fake_git_run(command, **_kwargs):
        reference = command[-1]
        stdout = (
            plan["execution"]["execution_tree"]
            if reference == "HEAD^{tree}"
            else plan["execution"]["execution_commit"]
        )
        return subprocess.CompletedProcess(command, 0, f"{stdout}\n", "")

    monkeypatch.setattr(subject.subprocess, "run", fake_git_run)
    monkeypatch.setattr(
        subject,
        "_validate_checkout_runtime_source_authority",
        lambda **_kwargs: copy.deepcopy(plan["runtime_sources"]),
    )
    process = subject.capture_runtime_process_probe(
        repository_root=tmp_path,
        pid_file=pid_file,
        config_path=config_path,
        config_sha256=config_sha256,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        expected_buy_e3_enabled=False,
        expected_execution_commit=plan["execution"]["execution_commit"],
        expected_execution_tree=plan["execution"]["execution_tree"],
        expected_artifact_sha256=plan["artifact"]["artifact_sha256"],
        expected_artifact_manifest_file_sha256=plan["artifact"][
            "manifest_file_sha256"
        ],
        expected_policy_file_sha256=plan["artifact"]["policy_file_sha256"],
        expected_predicate_bundle_file_sha256=plan["artifact"][
            "predicate_bundle_file_sha256"
        ],
        expected_runtime_code_sha256=plan["runtime_sources"]["runtime_code_sha256"],
        runtime_source_authority_base64=subject._encode_runtime_source_authority(
            plan["runtime_sources"]
        ),
        expected_startup_attestation_schema_version=subject.STARTUP_ATTESTATION_SCHEMA,
        artifact_manifest_path=artifact_manifest_path,
        policy_path=Path(spec["artifact"]["policy_path"]),
        predicate_bundle_path=Path(spec["artifact"]["predicate_bundle_path"]),
    )
    return process, runtime_text, pid_start_ticks


def test_active_pointer_rejects_environment_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pointer = tmp_path / "pointer.json"
    pointer.write_text(
        json.dumps(
            {
                "schema_version": subject.POINTER_SCHEMA,
                "status": subject.ACTIVE_POINTER_STATUS,
                "ssh_target": "user@host",
                "repo_root": "/repo",
                "provider": "fake",
                "region": "fake",
                "public_ipv4": "127.0.0.1",
            }
        ),
        encoding="ascii",
    )
    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE", "attacker@host")
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="override"):
        subject.load_sha_bound_active_pointer(
            pointer_path=pointer,
            expected_file_sha256=gate_v2.file_sha256(pointer),
        )


def test_known_hosts_binding_checks_sha_and_fingerprint(tmp_path: Path) -> None:
    key = b"local fake host key"
    encoded = base64.b64encode(key).decode()
    known = tmp_path / "known_hosts"
    known.write_text(f"host ssh-ed25519 {encoded}\n", encoding="ascii")
    digest = base64.urlsafe_b64encode(hashlib.sha256(key).digest()).decode().rstrip("=")
    binding = subject.bind_known_hosts(
        known_hosts_path=known,
        expected_file_sha256=gate_v2.file_sha256(known),
        expected_fingerprint=f"SHA256:{digest}",
    )
    assert binding["expected_fingerprint"] == f"SHA256:{digest}"
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="fingerprint"):
        subject.bind_known_hosts(
            known_hosts_path=known,
            expected_file_sha256=gate_v2.file_sha256(known),
            expected_fingerprint="SHA256:wrong",
        )


def test_plan_is_dry_run_strict_ssh_and_preflights_before_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    subject.validate_plan(plan)
    assert plan["status"] == "plan_only_no_remote_command_executed"
    assert plan["transaction_contract"]["activation_restart_only"] is True
    assert plan["transaction_contract"]["sighup_activation_allowed"] is False
    assert (
        plan["runtime_attestation_contract"]["remote_path"]
        == plan["remote"]["runtime_identity_path"]
    )
    for preflight in plan["isolated_preflights"].values():
        assert preflight["host_bound_storage_gate"]["status"] == (
            "deferred_to_mandatory_remote_preflight"
        )
    for phase, rows in plan["phases"].items():
        for row in rows:
            assert row["argv"][0] in {"ssh", "rsync"}
            assert "StrictHostKeyChecking=yes" in " ".join(row["argv"])
            assert "UserKnownHostsFile=" in " ".join(row["argv"])
            assert "reload" not in " ".join(row["argv"]).lower()
            if row["label"] in {
                "isolated-disabled-preflight",
                "isolated-active-preflight",
            }:
                assert "--defer-host-bound-spool" not in row["argv"]
                assert "--defer-host-bound-spool" not in " ".join(row["argv"])
        if phase in {"disabled-deploy", "activate"}:
            stop = next(index for index, row in enumerate(rows) if row["label"] == "stop-live")
            preflights = [index for index, row in enumerate(rows) if "preflight" in row["label"]]
            assert len(preflights) == 2
            assert max(preflights) < stop
    commands_by_label = {
        (phase, row["label"]): " ".join(row["argv"])
        for phase, rows in plan["phases"].items()
        for row in rows
    }
    starts = {
        key: command for key, command in commands_by_label.items() if key[1].startswith("start-")
    }
    assert starts
    assert all("NARROWGATE_LIVE_CONFIG=" in command for command in starts.values())
    for command in starts.values():
        assert "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY=1" in command
    assert (
        "-u NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY"
        in starts[("disabled-deploy", "start-disabled")]
    )
    assert (
        "NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY=1"
        in starts[("activate", "start-active-restart-only")]
    )
    for phase in ("rollback-primary", "rollback-deep"):
        assert (
            "-u NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY"
            in starts[(phase, "start-rollback-fresh-b0")]
        )
    disabled_preflight = commands_by_label[("disabled-deploy", "isolated-disabled-preflight")]
    active_preflight = commands_by_label[("disabled-deploy", "isolated-active-preflight")]
    assert "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY=1" in disabled_preflight
    assert "-u NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY" in disabled_preflight
    assert "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY=1" in active_preflight
    assert "NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY=1" in active_preflight
    disabled_labels = [row["label"] for row in plan["phases"]["disabled-deploy"]]
    assert disabled_labels.index("fresh-disabled-process-probe") < disabled_labels.index(
        "read-disabled-runtime-identity"
    )
    active_labels = [row["label"] for row in plan["phases"]["activate"]]
    assert (
        active_labels.index("reprobe-disabled-process-before-stop")
        < active_labels.index("read-pre-stop-disabled-runtime-identity")
        < active_labels.index("stop-live")
    )
    assert active_labels.index("fresh-active-process-probe") < active_labels.index(
        "read-active-runtime-identity"
    )


def test_validate_plan_rejects_rehashed_argv_injection_and_extra_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    injected = copy.deepcopy(plan)
    row = injected["phases"]["disabled-deploy"][0]
    row["argv"].append("--injected-after-rehash")
    row["command_sha256"] = gate_v2.canonical_sha256(row["argv"])
    _recanonicalize_plan(injected)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="phase commands"):
        subject.validate_plan(injected)

    extra = copy.deepcopy(plan)
    extra["attacker_field"] = "accepted only by a weak canonical check"
    extra["canonical_plan_sha256"] = gate_v2.document_sha256(extra, "canonical_plan_sha256")
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="fields drifted"):
        subject.validate_plan(extra)


def test_activation_gate_binds_exact_plan_core_and_transaction_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    activation = plan["activation_gate"]
    assert activation["plan_core_sha256"] == plan["plan_core_sha256"]
    assert activation["transaction_contract_sha256"] == gate_v2.canonical_sha256(
        plan["transaction_contract"]
    )

    tampered = copy.deepcopy(plan)
    tampered["activation_gate"]["plan_core_sha256"] = "0" * 64
    tampered["activation_gate"]["canonical_activation_binding_sha256"] = gate_v2.document_sha256(
        tampered["activation_gate"], "canonical_activation_binding_sha256"
    )
    tampered["canonical_plan_sha256"] = gate_v2.document_sha256(tampered, "canonical_plan_sha256")
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="plan/core"):
        subject.validate_plan(tampered)


def test_staged_tools_are_content_addressed_read_only_and_verified_at_exec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    package = plan["external_tools_and_package"]
    stage_fragment = f"package-{package['content_package_sha256']}"
    all_rows = [row for rows in plan["phases"].values() for row in rows]
    transfer_rows = [row for row in all_rows if row["label"].startswith("stage-")]
    assert transfer_rows
    for row in transfer_rows:
        role = row["label"].removeprefix("stage-")
        joined = " ".join(row["argv"])
        assert "--ignore-existing" in row["argv"]
        assert stage_fragment in joined
        assert package["files"][role]["file_sha256"] in joined
    freeze = next(
        row
        for row in plan["phases"]["disabled-deploy"]
        if row["label"] == "validate-and-freeze-content-addressed-stage"
    )
    assert "chmod 400" in " ".join(freeze["argv"])
    assert "chmod 500" in " ".join(freeze["argv"])
    tool_labels = {
        "isolated-disabled-preflight",
        "isolated-active-preflight",
        "startup-log-checkpoint",
        "fresh-disabled-process-probe",
        "reprobe-disabled-process-before-stop",
        "fresh-active-process-probe",
        "validate-disabled-startup-log",
        "validate-active-startup-log",
    }
    for row in all_rows:
        if row["label"] in tool_labels or "rollback-process-probe" in row["label"]:
            joined = " ".join(row["argv"])
            assert "sha256sum" in joined
            assert package["files"]["deploy_script"]["file_sha256"] in joined
            assert package["files"]["gate_amendment"]["file_sha256"] in joined


def test_plaintext_cli_token_is_rejected_and_secure_token_sources_work(
    tmp_path: Path,
) -> None:
    secret = "must-not-appear-in-an-argv"
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="plaintext --token") as exc:
        subject.main(["execute", "--token", secret])
    assert secret not in str(exc.value)

    token_file = tmp_path / "phase.token"
    token_file.write_text("from-file\n", encoding="ascii")
    token_file.chmod(0o644)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="0600"):
        subject._read_phase_token(token_file=token_file, token_fd=None)
    token_file.chmod(0o600)
    assert subject._read_phase_token(token_file=token_file, token_fd=None) == "from-file"

    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, b"from-stdin\n")
        os.close(write_fd)
        write_fd = -1
        assert subject._read_phase_token(token_file=None, token_fd=read_fd) == "from-stdin"
    finally:
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)


def test_plan_requires_two_distinct_b0_rollback_identities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _specification(tmp_path)
    spec["rollback_identities"]["deep_predecessor"]["identity"] = "primary"
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="not distinct"):
        subject.build_plan(
            specification=spec,
            repository_root=tmp_path,
            preflight_runner=lambda _repo, _config, enabled: _preflight(enabled),
        )


def test_plan_rejects_local_preflight_that_hides_host_bound_spool_deferral(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _specification(tmp_path)
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)

    def false_not_applicable(_repo: Path, _config: Path, enabled: bool) -> dict:
        payload = _preflight(enabled)
        payload["host_bound_storage_gate"] = {
            "profile": None,
            "status": "not_applicable",
            "deferred_on_planner_host": False,
            "mandatory_remote_preflight": False,
            "allowlisted_root": None,
            "journal_root": None,
            "prospective_epoch_root": None,
            "required_remote_checks": [],
        }
        payload["canonical_preflight_sha256"] = gate_v2.document_sha256(
            payload, "canonical_preflight_sha256"
        )
        return payload

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="preflight"):
        subject.build_plan(
            specification=spec,
            repository_root=tmp_path,
            preflight_runner=false_not_applicable,
        )


def test_host_bound_spool_deferral_keeps_lexical_allowlist_fail_closed(
    tmp_path: Path,
) -> None:
    config = tmp_path / "bad-spool.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "strategy": {},
                "lifecycle_journal_v2": {
                    "enabled": True,
                    "storage_profile": "bounded_remote_spool",
                    "required_mount": "/home/remote/allowed",
                    "root": "/home/remote/not-allowed/journal",
                    "prospective_epoch_root": "/home/remote/not-allowed/epochs",
                    "remote_spool_allowlisted_roots": ["/home/remote/allowed"],
                },
            },
            sort_keys=True,
        ),
        encoding="ascii",
    )

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="allowlist"):
        subject._host_bound_spool_gate(config, defer_host_bound_spool=True)


@pytest.mark.parametrize("make_symlink", [False, True])
def test_execution_host_spool_preflight_rejects_absent_or_symlink_root(
    tmp_path: Path,
    make_symlink: bool,
) -> None:
    import live.config as live_config

    with tempfile.TemporaryDirectory(
        prefix=".f05-spool-fixture-", dir=Path.cwd()
    ) as remote_fixture:
        remote_root = Path(remote_fixture)
        allowlisted = remote_root / "remote" / "formal_collection"
        if make_symlink:
            target = remote_root / "remote" / "actual"
            target.mkdir(parents=True)
            allowlisted.parent.mkdir(parents=True, exist_ok=True)
            allowlisted.symlink_to(target, target_is_directory=True)
        config = tmp_path / "host-bound.yaml"
        lifecycle = {
            "enabled": True,
            "storage_profile": "bounded_remote_spool",
            "required_mount": str(allowlisted),
            "root": str(allowlisted / "journal"),
            "prospective_epoch_root": str(allowlisted / "epochs"),
            "remote_spool_allowlisted_roots": [str(allowlisted)],
        }
        config.write_text(
            yaml.safe_dump(
                {"strategy": {}, "lifecycle_journal_v2": lifecycle},
                sort_keys=True,
            ),
            encoding="ascii",
        )

        with subject._host_bound_spool_validation_scope(
            config, defer_host_bound_spool=False
        ) as gate:
            assert gate["status"] == "validated_on_execution_host"
            with pytest.raises(ValueError, match="existing non-symlink"):
                live_config.validate_lifecycle_journal_storage(
                    profile=lifecycle["storage_profile"],
                    journal_root=lifecycle["root"],
                    prospective_epoch_root=lifecycle["prospective_epoch_root"],
                    required_mount=lifecycle["required_mount"],
                    remote_spool_allowlisted_roots=lifecycle["remote_spool_allowlisted_roots"],
                    enabled=True,
                )


def test_run_isolated_preflight_uses_child_and_strips_owner_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    python = tmp_path / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("fake", encoding="ascii")
    python.chmod(0o700)
    config = tmp_path / "config.yaml"
    config.write_text("strategy: {}\n", encoding="ascii")
    observed: dict = {}

    def fake_runner(command, **kwargs):
        observed["command"] = command
        observed["env"] = kwargs["env"]
        return subprocess.CompletedProcess(command, 0, json.dumps(_preflight(True)), "")

    monkeypatch.setenv("NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY", "1")
    payload = subject.run_isolated_preflight(
        tmp_path,
        config,
        True,
        runner=fake_runner,
    )
    assert payload["expected_enabled"] is True
    assert "NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY" not in observed["env"]
    assert "isolated-preflight" in observed["command"]
    assert "--defer-host-bound-spool" in observed["command"]


def test_execute_requires_flag_and_exact_phase_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    with pytest.raises(PermissionError, match="authorize"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=False,
        )
    with pytest.raises(PermissionError, match="token"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="wrong",
            authorize_remote_mutation=True,
        )


def test_activation_requires_separate_amended_gate_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    with pytest.raises(PermissionError, match="amended gate"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
        )


def test_activation_rejects_hash_without_bound_gate_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    plan["activation_gate_receipt_sha256"] = "f" * 64
    _recanonicalize_plan(plan)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="fields drifted"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            output_path=tmp_path / "must-not-exist.json",
        )


def test_activation_with_gate_still_requires_disabled_phase_receipt_before_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    calls: list[Sequence[str]] = []
    with pytest.raises(PermissionError, match="same-plan successful disabled"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            runner=lambda command: calls.append(command),
            output_path=tmp_path / "must-not-run.json",
        )
    assert calls == []


def test_authorized_execution_requires_immutable_receipt_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    calls: list[Sequence[str]] = []

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="immutable receipt"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=True,
            runner=lambda command: calls.append(command),
        )
    assert calls == []


def test_post_stop_failure_attempts_primary_rollback_and_writes_0600_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    calls: list[Sequence[str]] = []
    failed_at: list[int] = []

    def fake_runner(command):
        calls.append(command)
        joined = " ".join(command)
        if "printf '%s" in joined:
            return subprocess.CompletedProcess(command, 0, "100\n", "")
        # Fail after stop/quiescence/checkout when disabled start is attempted.
        if "bash live/run.sh start" in joined and "disabled.yaml" in joined and not failed_at:
            failed_at.append(len(calls))
            return subprocess.CompletedProcess(command, 1, "", "failed")
        if " process-probe " in joined:
            return subprocess.CompletedProcess(
                command, 0, _process_probe(plan, "rollback-primary"), ""
            )
        return subprocess.CompletedProcess(command, 0, "101\n", "")

    receipt = tmp_path / "transaction.json"
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="failed closed"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=True,
            runner=fake_runner,
            output_path=receipt,
        )
    assert failed_at and len(calls) > failed_at[0]
    payload = json.loads(receipt.read_text(encoding="ascii"))
    assert payload["status"] == subject.PHASE_FAILED_CLOSED
    assert payload["failure_class"] == "command_returncode_nonzero"
    assert payload["rollback_attempted"] is True
    assert payload["rollback_status"] == "rollback_complete"
    assert payload["rollback_process_identity"]["buy_e3_enabled"] is False
    assert any(row["label"].startswith("automatic-rollback:") for row in payload["results"])
    receipt_text = receipt.read_text(encoding="ascii")
    assert '"stdout":' not in receipt_text
    assert '"stderr":' not in receipt_text
    assert oct(receipt.stat().st_mode & 0o777) == "0o600"
    assert (
        subject.validate_phase_receipt(receipt, plan=plan, expected_phase="disabled-deploy")
        == payload
    )


def test_mutation_is_marked_before_stop_and_early_mutation_failure_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    base_runner, calls = _successful_runner(plan, "disabled-deploy")
    failed = False

    def runner(command):
        nonlocal failed
        if not failed:
            failed = True
            calls.append(" ".join(command))
            return subprocess.CompletedProcess(command, 12, "", "stage failed")
        return base_runner(command)

    output = tmp_path / "early-mutation-failed.json"
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="failed closed"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=output,
        )
    payload = subject.validate_phase_receipt(output, plan=plan, expected_phase="disabled-deploy")
    assert payload["mutation_started"] is True
    assert payload["rollback_attempted"] is True
    assert not any(
        row["label"] == "stop-live"
        for row in payload["results"]
        if not row["label"].startswith("automatic-")
    )


def test_stop_nonzero_is_probed_then_rolled_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    base_runner, _commands = _successful_runner(plan, "disabled-deploy")
    order: list[str] = []
    failed_stop = False

    def runner(command):
        nonlocal failed_stop
        label = _command_label(plan, command)
        order.append(label)
        if label == "stop-live" and not failed_stop:
            failed_stop = True
            return subprocess.CompletedProcess(command, 9, "", "stop failed")
        return base_runner(command)

    output = tmp_path / "stop-failed.json"
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="stop-live"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=output,
        )
    payload = subject.validate_phase_receipt(output, plan=plan, expected_phase="disabled-deploy")
    assert payload["stop_failure_probe_result"]["label"] == ("stop-failure-probe:confirm-quiescent")
    assert payload["rollback_attempted"] is True
    first_stop = order.index("stop-live")
    assert order[first_stop + 1] == "confirm-quiescent"
    assert "stop-live" in order[first_stop + 2 :]


def test_output_path_race_never_replaces_and_receipt_commit_failure_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    base_runner, calls = _successful_runner(plan, "disabled-deploy")
    output = tmp_path / "raced-output.json"
    injected = False

    def runner(command):
        nonlocal injected
        if not injected:
            injected = True
            output.write_text("attacker-owned\n", encoding="ascii")
        return base_runner(command)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="receipt_write_failed"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=output,
        )
    assert output.read_text(encoding="ascii") == "attacker-owned\n"
    assert len(calls) > len(plan["phases"]["disabled-deploy"])


def test_receipt_validation_failure_after_mutation_triggers_automatic_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    runner, calls = _successful_runner(plan, "disabled-deploy")
    output = tmp_path / "validator-rejected.json"

    def reject_receipt(*_args, **_kwargs):
        raise subject.BuyE3TransactionalDeployError("adversarial validator rejection")

    monkeypatch.setattr(subject, "validate_phase_receipt", reject_receipt)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="receipt_validation_failed"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=output,
        )
    assert not output.exists()
    assert len(calls) > len(plan["phases"]["disabled-deploy"])


def test_successful_disabled_phase_never_uses_sighup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    fake_runner, commands = _successful_runner(plan, "disabled-deploy")
    receipt = tmp_path / "disabled-transaction.json"

    result = subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=fake_runner,
        output_path=receipt,
    )
    assert result["status"] == subject.PHASE_COMPLETE
    assert result["rollback_attempted"] is False
    assert (
        result["actual_process_identity"]["artifact_sha256"] == plan["artifact"]["artifact_sha256"]
    )
    assert (
        result["actual_process_identity"]["runtime_code_sha256"]
        == plan["runtime_sources"]["runtime_code_sha256"]
    )
    startup = result["actual_startup_attestation"]
    assert startup["authority"] == "runtime_written_startup_attestation"
    assert (
        startup["runtime_identity_file_sha256"]
        == result["actual_process_identity"]["runtime_identity"]["file_sha256"]
    )
    runtime_result = next(
        row for row in result["results"] if row["label"] == "read-disabled-runtime-identity"
    )
    assert runtime_result["stdout_sha256"] == startup["runtime_identity_file_sha256"]
    assert (
        subject.validate_phase_receipt(receipt, plan=plan, expected_phase="disabled-deploy")
        == result
    )
    assert not any(
        "sighup" in command.lower() or " reload" in command.lower() for command in commands
    )


def test_successful_activation_receipt_embeds_exact_fresh_process_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (
        plan,
        disabled_path,
        disabled_process,
        envelope_path,
        release_path,
        release_binding,
    ) = _direct_v3_activation_context(tmp_path, monkeypatch)
    fake_runner, _commands = _successful_runner(
        plan,
        "activate",
        disabled_process=disabled_process,
        active_release_binding=release_binding,
    )
    receipt = tmp_path / "activate-transaction.json"

    result = subject.execute_phase(
        plan=plan,
        phase="activate",
        token="token-activate",
        authorize_remote_mutation=True,
        runner=fake_runner,
        output_path=receipt,
        disabled_phase_receipt_path=disabled_path,
        activation_envelope_path=None,
        active_release_path=release_path,
    )

    process = result["actual_process_identity"]
    assert result["status"] == subject.PHASE_COMPLETE
    assert process["buy_e3_enabled"] is True
    assert process["owner_override_effective"] is True
    assert process["artifact_sha256"] == plan["artifact"]["artifact_sha256"]
    assert process["runtime_code_sha256"] == plan["runtime_sources"]["runtime_code_sha256"]
    assert process["config_sha256"] == plan["configs"]["active"]["config_sha256"]
    assert process["execution_tree"] == plan["execution"]["execution_tree"]
    assert process["pid"] != next(
        row["observed_pid"] for row in result["results"] if row["label"] == "capture-old-pid"
    )
    assert process["initial_buy_deadline_identity"] == "B0"
    assert process["e3_deadline_imported"] is False
    assert process["active_release_file_sha256"] == release_binding["file_sha256"]
    assert result["active_release_binding"] == release_binding
    assert result["evidence_authority"] == subject.RECEIPT_AUTHORITY
    assert result["evidence_authority"]["standalone_activation_evidence"] is False
    assert result["actual_startup_attestation"]["authority"] == (
        "runtime_written_startup_attestation"
    )
    assert (
        result["pre_stop_disabled_startup_attestation"]["runtime_identity_file_sha256"]
        == result["disabled_phase_receipt_binding"]["runtime_identity_file_sha256"]
    )
    assert (
        result["actual_startup_attestation"]["startup_attestation"]["fill_cooldown_state"][
            "buy_deadline_identity"
        ]
        == "B0"
    )
    assert result["disabled_phase_receipt_binding"]["plan_sha256"] == plan["canonical_plan_sha256"]
    assert subject.validate_phase_receipt(receipt, plan=plan, expected_phase="activate") == result


def test_activation_requires_successful_disabled_receipt_from_the_exact_same_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    disabled_path, _receipt, _process = _successful_disabled_receipt(tmp_path, plan)
    different_plan = copy.deepcopy(plan)
    different_plan["phase_token_sha256"]["rollback-deep"] = "0" * 64
    _recanonicalize_plan(different_plan)
    subject.validate_plan(different_plan)
    calls: list[Sequence[str]] = []
    with pytest.raises(
        subject.BuyE3TransactionalDeployError, match="disabled phase receipt is invalid"
    ):
        subject.execute_phase(
            plan=different_plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            runner=lambda command: calls.append(command),
            output_path=tmp_path / "must-not-run.json",
            disabled_phase_receipt_path=disabled_path,
        )
    assert calls == []


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("pid", 909),
        ("pid_start_ticks", 999_999),
        ("config_sha256", "0" * 64),
        ("runtime_code_sha256", "1" * 64),
        ("artifact_sha256", "2" * 64),
    ],
)
def test_activation_reprobes_exact_disabled_process_before_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    replacement,
) -> None:
    (
        plan,
        disabled_path,
        disabled_process,
        _envelope_path,
        release_path,
        release_binding,
    ) = _direct_v3_activation_context(tmp_path, monkeypatch)
    base_runner, calls = _successful_runner(
        plan,
        "activate",
        disabled_process=disabled_process,
        active_release_binding=release_binding,
    )
    reprobe_argv = tuple(
        next(
            row["argv"]
            for row in plan["phases"]["activate"]
            if row["label"] == "reprobe-disabled-process-before-stop"
        )
    )

    def mismatched_runner(command):
        if tuple(command) == reprobe_argv:
            drifted = dict(disabled_process)
            drifted[field] = replacement
            drifted["canonical_process_identity_sha256"] = gate_v2.document_sha256(
                drifted, "canonical_process_identity_sha256"
            )
            calls.append(" ".join(command))
            return subprocess.CompletedProcess(command, 0, json.dumps(drifted), "")
        return base_runner(command)

    output = tmp_path / "handoff-failed.json"
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="handoff|artifact/runtime/config",
    ):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
                runner=mismatched_runner,
                output_path=output,
                disabled_phase_receipt_path=disabled_path,
                active_release_path=release_path,
            )
    payload = subject.validate_phase_receipt(output, plan=plan, expected_phase="activate")
    assert payload["status"] == subject.PHASE_FAILED_CLOSED
    assert payload["failure_class"] == "disabled_process_handoff_mismatch"
    assert payload["rollback_attempted"] is True
    stop_index = next(
        (index for index, command in enumerate(calls) if "bash live/run.sh stop" in command),
        len(calls),
    )
    reprobe_index = next(index for index, command in enumerate(calls) if "process-probe" in command)
    assert reprobe_index < stop_index


def test_activation_fails_closed_on_tampered_runtime_identity_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (
        plan,
        disabled_path,
        disabled_process,
        _envelope_path,
        release_path,
        release_binding,
    ) = _direct_v3_activation_context(tmp_path, monkeypatch)
    base_runner, _calls = _successful_runner(
        plan,
        "activate",
        disabled_process=disabled_process,
        active_release_binding=release_binding,
    )

    runtime_read = tuple(
        next(
            row["argv"]
            for row in plan["phases"]["activate"]
            if row["label"] == "read-pre-stop-disabled-runtime-identity"
        )
    )

    def wrong_attestation_runner(command):
        if tuple(command) == runtime_read:
            forged = _runtime_identity_payload(plan, "disabled-deploy", pid=disabled_process["pid"])
            forged["f05_buy_e3_artifact_sha256"] = "0" * 64
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(forged, indent=2, sort_keys=True) + "\n",
                "",
            )
        return base_runner(command)

    output = tmp_path / "attestation-failed.json"
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="runtime identity"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
                runner=wrong_attestation_runner,
                output_path=output,
                disabled_phase_receipt_path=disabled_path,
                active_release_path=release_path,
            )
    payload = subject.validate_phase_receipt(output, plan=plan, expected_phase="activate")
    assert payload["failure_class"] == "runtime_identity_invalid"
    assert payload["rollback_attempted"] is True
    assert payload["pre_stop_disabled_startup_attestation"] is None


def test_capture_runtime_process_probe_binds_runtime_written_file_and_attestation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    process, runtime_text, pid_start_ticks = _capture_runtime_probe_fixture(tmp_path, monkeypatch)
    runtime = json.loads(runtime_text)
    expected_file_sha256 = hashlib.sha256(runtime_text.encode()).hexdigest()
    expected_attestation_sha256 = gate_v2.canonical_sha256(runtime["startup_attestation"])

    assert set(process) == subject._PROCESS_IDENTITY_FIELDS
    assert process["pid"] == runtime["pid"] == 4242
    assert process["pid_start_ticks"] == pid_start_ticks
    assert process["runtime_identity_file_sha256"] == expected_file_sha256
    assert process["runtime_identity"]["file_sha256"] == expected_file_sha256
    assert process["startup_attestation_sha256"] == expected_attestation_sha256
    assert process["initial_buy_deadline_identity"] == "B0"
    assert process["e3_deadline_imported"] is False
    assert process["canonical_process_identity_sha256"] == (
        gate_v2.document_sha256(process, "canonical_process_identity_sha256")
    )


def test_capture_runtime_process_probe_accepts_expired_b0_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def expired_b0(runtime: dict) -> None:
        state = runtime["startup_attestation"]["fill_cooldown_state"]
        state["restore_mode"] = "expired_to_b0"
        state["checkpoint_loaded"] = True
        state["checkpoint_sequence"] = 7

    process, _runtime_text, _pid_start_ticks = _capture_runtime_probe_fixture(
        tmp_path,
        monkeypatch,
        runtime_mutator=expired_b0,
    )

    assert process["initial_buy_deadline_identity"] == "B0"
    assert process["e3_deadline_imported"] is False


@pytest.mark.parametrize(
    "runtime_mutation",
    [
        "missing",
        "v1-schema",
        "v4-schema",
        "missing-v5-field",
        "extra-v5-field",
        "expected-echo",
        "forged-deadline",
        "forged-source",
        "forged-interpreter",
        "native-expected-echo",
    ],
)
def test_capture_runtime_process_probe_rejects_non_authoritative_startup_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runtime_mutation: str,
) -> None:
    def mutate(runtime: dict) -> None:
        if runtime_mutation == "missing":
            runtime.pop("startup_attestation")
        elif runtime_mutation == "v1-schema":
            runtime["startup_attestation"]["schema_version"] = (
                "narrowgate_buy_e3_startup_attestation.v1"
            )
        elif runtime_mutation == "v4-schema":
            runtime["startup_attestation"]["schema_version"] = (
                subject.HISTORICAL_STARTUP_ATTESTATION_SCHEMA
            )
        elif runtime_mutation == "missing-v5-field":
            runtime["startup_attestation"].pop("shadow_runtime_identity")
        elif runtime_mutation == "extra-v5-field":
            runtime["startup_attestation"]["unversioned_extension"] = False
        elif runtime_mutation == "expected-echo":
            runtime["startup_attestation"] = {
                "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
                "status": "accepted",
                "artifact_sha256": runtime["f05_buy_e3_artifact_sha256"],
                "buy_deadline_identity": "B0",
                "buy_remaining_ms": 0,
            }
        elif runtime_mutation == "forged-deadline":
            runtime["startup_attestation"]["fill_cooldown_state"]["buy_deadline_identity"] = (
                "BUY_E3"
            )
        elif runtime_mutation == "forged-source":
            runtime["startup_attestation"]["running_checkout"]["runtime_source_files"][0][
                "working_file_sha256"
            ] = "0" * 64
        elif runtime_mutation == "forged-interpreter":
            runtime["startup_attestation"]["interpreter_identity"]["after"]["sha256"] = "0" * 64
        else:
            runtime["native_runtime"]["module"] = "/expected/native.so"

    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match=(
            "startup attestation|deadline|runtime identity|runtime startup source|"
            "runtime interpreter|native runtime|runtime native"
        ),
    ):
        _capture_runtime_probe_fixture(
            tmp_path,
            monkeypatch,
            runtime_mutator=mutate,
        )


def test_capture_runtime_process_probe_rejects_forged_file_sha256(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="changed during process probe",
    ):
        _capture_runtime_probe_fixture(
            tmp_path,
            monkeypatch,
            gate_runtime_file_sha256="0" * 64,
        )


@pytest.mark.parametrize(
    "field",
    ["runtime_identity_file_sha256", "startup_attestation_sha256"],
)
def test_process_identity_requires_top_level_runtime_hashes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    process = json.loads(_process_probe(plan, "disabled-deploy", pid=101))
    process.pop(field)
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="fields drifted"):
        subject._validate_actual_process_identity(
            process,
            plan=plan,
            phase="disabled-deploy",
            old_pid=100,
        )


def test_process_identity_rejects_forged_top_level_runtime_file_sha256(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    process = json.loads(_process_probe(plan, "disabled-deploy", pid=101))
    process["runtime_identity_file_sha256"] = "0" * 64
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="file hash drifted"):
        subject._validate_actual_process_identity(
            process,
            plan=plan,
            phase="disabled-deploy",
            old_pid=100,
        )


def test_runtime_identity_rejects_forged_top_level_attestation_sha256(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    process = json.loads(_process_probe(plan, "disabled-deploy", pid=101))
    process["startup_attestation_sha256"] = "0" * 64
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )
    validated = subject._validate_actual_process_identity(
        process,
        plan=plan,
        phase="disabled-deploy",
        old_pid=100,
    )

    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="process-bound hash",
    ):
        subject._validate_runtime_identity_stdout(
            _runtime_identity(plan, "disabled-deploy", pid=101),
            plan=plan,
            process=validated,
            process_phase="disabled-deploy",
        )


def test_expected_value_echo_is_not_runtime_owned_startup_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    process = json.loads(_process_probe(plan, "disabled-deploy", pid=101))
    forged_echo = json.dumps(
        {
            "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
            "status": "accepted",
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "artifact_sha256": plan["artifact"]["artifact_sha256"],
            "buy_deadline_identity": "B0",
            "buy_remaining_ms": 0,
        },
        sort_keys=True,
    )
    process["runtime_identity"]["file_sha256"] = hashlib.sha256(forged_echo.encode()).hexdigest()
    process["runtime_identity_file_sha256"] = process["runtime_identity"]["file_sha256"]
    process["startup_attestation_sha256"] = gate_v2.canonical_sha256(json.loads(forged_echo))
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="runtime identity"):
        subject._validate_runtime_identity_stdout(
            forged_echo,
            plan=plan,
            process=process,
            process_phase="disabled-deploy",
        )


@pytest.mark.parametrize(
    ("field_path", "replacement"),
    [
        (("execution_identity", "execution_tree"), "0" * 40),
        (("execution_identity", "annotated_tag_object"), "1" * 40),
        (("runtime_sources", "runtime_code_sha256"), "0" * 64),
        (("config_binding", "disabled", "config_sha256"), "0" * 64),
        (("config_binding", "active", "config_sha256"), "1" * 64),
        (("host_binding", "active_pointer_file_sha256"), "0" * 64),
        (("host_binding", "known_hosts_file_sha256"), "1" * 64),
        (("host_binding", "host_key_fingerprint"), "SHA256:another"),
        (("host_binding", "repo_root"), "/another/repo"),
        (("host_binding", "python_executable"), "/another/python"),
        (("host_binding", "venv_root"), "/another/venv"),
        (("rollback_identities", "primary_disabled", "config_sha256"), "0" * 64),
        (("rollback_identities", "deep_predecessor", "execution_tree"), "0" * 40),
    ],
)
def test_build_plan_rejects_activation_gate_cross_binding_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field_path: tuple[str, ...],
    replacement: str,
) -> None:
    spec = _specification(tmp_path)
    payload = _activation_gate_payload(spec)
    _set_nested(payload, field_path, replacement)
    payload["canonical_amendment_receipt_sha256"] = gate_v2.document_sha256(
        payload, "canonical_amendment_receipt_sha256"
    )
    _bind_activation_gate(tmp_path, spec, payload)
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="cross-binding"):
        subject.build_plan(
            specification=spec,
            repository_root=tmp_path,
            preflight_runner=lambda _repo, _config, enabled: _preflight(enabled),
        )


def test_revalidate_plan_rejects_rehashed_activation_gate_cross_binding_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    path = Path(plan["activation_gate"]["path"])
    payload = _activation_gate_payload(spec)
    payload["execution_identity"]["annotated_tag_object"] = "0" * 40
    payload["canonical_amendment_receipt_sha256"] = gate_v2.document_sha256(
        payload, "canonical_amendment_receipt_sha256"
    )
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    plan["activation_gate"]["file_sha256"] = gate_v2.file_sha256(path)
    plan["activation_gate"]["canonical_receipt_sha256"] = payload[
        "canonical_amendment_receipt_sha256"
    ]
    _recanonicalize_plan(plan)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="cross-binding"):
        subject._revalidate_plan_inputs(plan)


def test_validate_phase_receipt_rejects_rehashed_command_tampering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    runner, _commands = _successful_runner(plan, "disabled-deploy")
    original = tmp_path / "original.json"
    subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=original,
    )
    payload = json.loads(original.read_text(encoding="ascii"))
    payload["results"][0]["label"] = "tampered-command"
    tampered = _write_receipt(tmp_path / "tampered-command.json", payload)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="order/hash"):
        subject.validate_phase_receipt(tampered, plan=plan)


def test_validate_phase_receipt_rejects_embedded_stdout_even_when_rehashed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    runner, _commands = _successful_runner(plan, "disabled-deploy")
    original = tmp_path / "original.json"
    subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=original,
    )
    payload = json.loads(original.read_text(encoding="ascii"))
    payload["results"][0]["stdout"] = "not allowed"
    tampered = _write_receipt(tmp_path / "embedded-stdout.json", payload)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="forbidden fields"):
        subject.validate_phase_receipt(tampered, plan=plan)


def test_unsigned_local_receipt_cannot_claim_standalone_activation_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    runner, _commands = _successful_runner(plan, "disabled-deploy")
    original = tmp_path / "original-structural.json"
    subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=original,
    )
    payload = json.loads(original.read_text(encoding="ascii"))
    payload["evidence_authority"]["standalone_activation_evidence"] = True
    tampered = _write_receipt(tmp_path / "forged-authority.json", payload)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="identity drifted"):
        subject.validate_phase_receipt(tampered, plan=plan)


def test_validate_phase_receipt_rejects_wrong_phase_even_when_rehashed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    runner, _commands = _successful_runner(plan, "disabled-deploy")
    original = tmp_path / "original.json"
    subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=original,
    )
    payload = json.loads(original.read_text(encoding="ascii"))
    payload["phase"] = "rollback-primary"
    tampered = _write_receipt(tmp_path / "wrong-phase.json", payload)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="phase drifted"):
        subject.validate_phase_receipt(tampered, plan=plan, expected_phase="disabled-deploy")


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("owner_override_effective", False),
        ("runtime_code_sha256", "0" * 64),
        ("artifact_sha256", "1" * 64),
        ("initial_buy_deadline_identity", "E3"),
        ("e3_deadline_imported", True),
        ("runtime_identity_file_sha256", "2" * 64),
        ("startup_attestation_sha256", "3" * 64),
        ("pid", 101),
    ],
)
def test_validate_activation_receipt_rejects_process_artifact_and_deadline_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    replacement,
) -> None:
    (
        plan,
        disabled_path,
        disabled_process,
        envelope_path,
        release_path,
        release_binding,
    ) = _direct_v3_activation_context(tmp_path, monkeypatch)
    runner, _commands = _successful_runner(
        plan,
        "activate",
        disabled_process=disabled_process,
        active_release_binding=release_binding,
    )
    original = tmp_path / "original.json"
    subject.execute_phase(
        plan=plan,
        phase="activate",
        token="token-activate",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=original,
        disabled_phase_receipt_path=disabled_path,
        activation_envelope_path=None,
        active_release_path=release_path,
    )
    payload = json.loads(original.read_text(encoding="ascii"))
    process = payload["actual_process_identity"]
    process[field] = replacement
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )
    process_result = next(
        row for row in payload["results"] if row["label"] == "fresh-active-process-probe"
    )
    process_result["process_identity_sha256"] = process["canonical_process_identity_sha256"]
    if field == "pid":
        process_result["observed_pid"] = replacement
    tampered = _write_receipt(tmp_path / f"wrong-{field}.json", payload)

    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="process|PID|runtime identity",
    ):
        subject.validate_phase_receipt(tampered, plan=plan, expected_phase="activate")


def test_validate_phase_receipt_rejects_bad_canonical_hash_and_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    runner, _commands = _successful_runner(plan, "disabled-deploy")
    original = tmp_path / "original.json"
    subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=original,
    )
    payload = json.loads(original.read_text(encoding="ascii"))
    payload["canonical_receipt_sha256"] = "0" * 64
    bad_hash = _write_receipt(tmp_path / "bad-hash.json", payload, recanonicalize=False)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="identity drifted"):
        subject.validate_phase_receipt(bad_hash, plan=plan)

    original.chmod(0o644)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="0600"):
        subject.validate_phase_receipt(original, plan=plan)


def _compatible_attempt(
    tmp_path: Path,
    spec: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict, dict]:
    runtime_commit = "b" * 40
    runtime_tree = "c" * 40
    runtime_tag = "f05-owner-buy-e3-live-attempt3-20260823"
    runtime_tag_object = "d" * 40
    source_files = {
        role: {
            "repository_relative_path": relative,
            "file_sha256": _RUNTIME_SOURCE_FILES[role]["working_file_sha256"],
            "size_bytes": 1,
        }
        for role, relative in gate_v2.REQUIRED_RUNTIME_PATHS.items()
    }
    attempt_payload = {
        "artifact_producer_execution": {
            "execution_commit": gate_v2.FROZEN_EXECUTION_COMMIT,
        },
        "runtime_execution": {
            "execution_commit": runtime_commit,
            "execution_tree": runtime_tree,
            "annotated_tag": runtime_tag,
            "annotated_tag_object": runtime_tag_object,
            "tag_peeled_commit": runtime_commit,
        },
        "runtime_sources": {"files": source_files},
        "artifact": {
            "artifact_sha256": "a" * 64,
            "files": {
                role: {"file_sha256": gate_v2.file_sha256(Path(spec["artifact"][f"{role}_path"]))}
                for role in ("manifest", "policy", "predicate_bundle")
            },
        },
        "canonical_execution_attempt_sha256": "9" * 64,
    }
    attempt_path = tmp_path / "execution_attempt.json"
    attempt_path.write_text("{}\n", encoding="ascii")
    attempt_path.chmod(0o600)
    spec["compatible_execution_attempt"] = {
        "path": str(attempt_path),
        "file_sha256": gate_v2.file_sha256(attempt_path),
    }
    spec["execution"] = {
        "commit": runtime_commit,
        "tree": runtime_tree,
        "annotated_tag": runtime_tag,
        "annotated_tag_object": runtime_tag_object,
    }
    runtime_sources = subject._compatible_runtime_sources(attempt_payload)
    spec["rollback_identities"]["primary_disabled"].update(
        {
            "execution_commit": runtime_commit,
            "execution_tree": runtime_tree,
            "runtime_code_sha256": runtime_sources["runtime_code_sha256"],
        }
    )
    spec["rollback_identities"]["deep_predecessor"].update(
        {
            "execution_commit": runtime_commit,
            "execution_tree": runtime_tree,
            "runtime_code_sha256": runtime_sources["runtime_code_sha256"],
        }
    )
    monkeypatch.setattr(
        subject.execution_attempt,
        "validate_manifest",
        lambda *_args, **_kwargs: copy.deepcopy(attempt_payload),
    )
    return attempt_payload, runtime_sources


def test_compatible_attempt_plan_separates_artifact_producer_and_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _specification(tmp_path)
    attempt_payload, runtime_sources = _compatible_attempt(tmp_path, spec, monkeypatch)
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)

    plan = subject.build_plan(
        specification=spec,
        repository_root=tmp_path,
        preflight_runner=lambda _repo, _config, enabled: _preflight(enabled),
    )

    assert plan["schema_version"] == subject.COMPATIBLE_PLAN_SCHEMA
    assert (
        plan["execution"]["execution_commit"]
        == attempt_payload["runtime_execution"]["execution_commit"]
    )
    assert (
        plan["execution"]["execution_commit"]
        != attempt_payload["artifact_producer_execution"]["execution_commit"]
    )
    assert plan["runtime_sources"] == runtime_sources
    disabled_labels = [row["label"] for row in plan["phases"]["disabled-deploy"]]
    assert disabled_labels.index("fetch-and-prepare-isolated-runtime") < disabled_labels.index(
        "isolated-disabled-preflight"
    )
    assert disabled_labels.index("isolated-active-preflight") < disabled_labels.index("stop-live")
    assert disabled_labels.index("stop-live") < disabled_labels.index("checkout-frozen-runtime")
    assert disabled_labels.index("checkout-frozen-runtime") < disabled_labels.index(
        "install-private-artifact-and-config-bytes"
    )
    assert disabled_labels.index(
        "install-private-artifact-and-config-bytes"
    ) < disabled_labels.index("start-disabled")
    subject.validate_plan(plan)
    subject._revalidate_plan_inputs(plan)


def test_compatible_attempt_rejects_plan_time_gate_and_requires_post_disabled_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _specification(tmp_path)
    _attempt_payload, _runtime_sources = _compatible_attempt(tmp_path, spec, monkeypatch)
    gate_path = tmp_path / "generic_gate.json"
    gate_path.write_text("{}\n", encoding="ascii")
    gate_path.chmod(0o600)
    spec["activation_gate"] = {
        "kind": subject._COMPATIBLE_ACTIVATION_GATE_KIND,
        "path": str(gate_path),
        "file_sha256": gate_v2.file_sha256(gate_path),
    }
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)

    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="post-disabled activation envelope",
    ):
        subject.build_plan(
            specification=spec,
            repository_root=tmp_path,
            preflight_runner=lambda _repo, _config, enabled: _preflight(enabled),
        )


def test_compatible_activation_rejects_missing_envelope_before_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _specification(tmp_path)
    _compatible_attempt(tmp_path, spec, monkeypatch)
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)
    plan = subject.build_plan(
        specification=spec,
        repository_root=tmp_path,
        preflight_runner=lambda _repo, _config, enabled: _preflight(enabled),
    )
    disabled_path, _receipt, _process = _successful_disabled_receipt(tmp_path, plan)

    with pytest.raises(PermissionError, match="activation envelope"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            runner=lambda _command: pytest.fail("runner must not execute"),
            output_path=tmp_path / "activate-without-envelope.json",
            disabled_phase_receipt_path=disabled_path,
        )


def test_default_runner_applies_per_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict = {}

    def fake_run(command, **kwargs):
        observed.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)
    subject._default_runner(("true",))

    assert observed["timeout"] == subject.COMMAND_TIMEOUT_S


def test_phase_timeout_fails_closed_before_remote_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    output = tmp_path / "phase-timeout.json"
    monkeypatch.setattr(subject, "PHASE_TIMEOUT_S", -1.0)

    with pytest.raises(TimeoutError, match="exceeded"):
        subject.execute_phase(
            plan=plan,
            phase="disabled-deploy",
            token="token-disabled-deploy",
            authorize_remote_mutation=True,
            runner=lambda command: subprocess.CompletedProcess(command, 0, "", ""),
            output_path=output,
        )

    receipt = json.loads(output.read_text(encoding="ascii"))
    assert receipt["status"] == subject.PHASE_FAILED_CLOSED
    assert receipt["failure_class"] == "phase_timeout"
    assert receipt["mutation_started"] is False
    assert receipt["rollback_attempted"] is False


@pytest.mark.parametrize(
    "invalid_value",
    ("__missing__", True, "85", float("nan"), 84.999, 85.001),
    ids=("missing", "bool", "string", "nan", "below", "above"),
)
def test_b0_config_contract_requires_exact_numeric_85(
    tmp_path: Path,
    invalid_value,
) -> None:
    config = tmp_path / "config.yaml"
    strategy = {"fill_cooldown": 85}
    if invalid_value == "__missing__":
        strategy.pop("fill_cooldown")
    else:
        strategy["fill_cooldown"] = invalid_value
    config.write_text(yaml.safe_dump({"strategy": strategy}), encoding="ascii")

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="exact numeric B0"):
        subject.validate_b0_config_contract(config)

    strategy["fill_cooldown"] = 85.0
    config.write_text(yaml.safe_dump({"strategy": strategy}), encoding="ascii")
    assert subject.validate_b0_config_contract(config)["seconds"] == 85.0


def _write_gate_document(path: Path, payload: dict, canonical_field: str) -> Path:
    payload[canonical_field] = gate_v2.document_sha256(payload, canonical_field)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def test_immutable_receipt_reader_uses_one_fd_and_rejects_link_or_path_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_v1 = subject.gate_v1
    original = tmp_path / "receipt.json"
    original.write_text('{"identity":"original"}\n', encoding="ascii")
    original.chmod(0o600)
    payload, target = gate_v1._read_immutable_json(original)
    assert payload == {"identity": "original"}
    assert target == original.absolute()

    symlink = tmp_path / "receipt-link.json"
    symlink.symlink_to(original)
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="unavailable|symlink"):
        gate_v1._read_immutable_json(symlink)

    hardlink = tmp_path / "receipt-hardlink.json"
    os.link(original, hardlink)
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="single-link"):
        gate_v1._read_immutable_json(original)
    hardlink.unlink()

    replacement = tmp_path / "replacement.json"
    replacement.write_text('{"identity":"replacement"}\n', encoding="ascii")
    replacement.chmod(0o600)
    real_open = gate_v1.os.open
    real_fstat = gate_v1.os.fstat
    swapped = False
    target_fd = None
    target_parent = original.parent.stat()

    def swapping_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal target_fd
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if (
            path == original.name
            and dir_fd is not None
            and real_fstat(dir_fd).st_dev == target_parent.st_dev
            and real_fstat(dir_fd).st_ino == target_parent.st_ino
            and not flags & getattr(os, "O_DIRECTORY", 0)
        ):
            target_fd = descriptor
        return descriptor

    def swapping_fstat(descriptor):
        nonlocal swapped
        metadata = real_fstat(descriptor)
        if descriptor == target_fd and not swapped:
            swapped = True
            os.replace(replacement, original)
        return metadata

    monkeypatch.setattr(gate_v1.os, "open", swapping_open)
    monkeypatch.setattr(gate_v1.os, "fstat", swapping_fstat)
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="changed while it was read"):
        gate_v1._read_immutable_json(original)
    assert swapped is True


def test_generic_gate_validator_requires_exact_checks_and_boundary(tmp_path: Path) -> None:
    gate_v1 = subject.gate_v1
    payload = {
        "schema_version": gate_v1.GATE_SCHEMA,
        "identity": gate_v1.OWNER_IDENTITY,
        "status": "deployment_gate_passed",
        "generated_utc": "2026-08-23T00:00:00Z",
        "artifact_sha256": "a" * 64,
        "execution_commit": "b" * 40,
        "execution_tag": "f05-compatible",
        "health_receipt_sha256": "c" * 64,
        "benchmark_receipt_sha256": "d" * 64,
        "checks": {name: True for name in gate_v1.DEPLOYMENT_GATE_CHECK_NAMES},
        "activation_allowed": True,
        **gate_v1.EVIDENCE_BOUNDARY,
    }
    valid = _write_gate_document(
        tmp_path / "gate.json",
        payload,
        "canonical_deployment_gate_receipt_sha256",
    )
    assert (
        gate_v1.validate_deployment_gate_receipt(valid, expected_artifact_sha256="a" * 64)[
            "activation_allowed"
        ]
        is True
    )

    mutations = []
    missing_check = copy.deepcopy(payload)
    missing_check["checks"].pop(next(iter(gate_v1.DEPLOYMENT_GATE_CHECK_NAMES)))
    mutations.append(missing_check)
    extra_check = copy.deepcopy(payload)
    extra_check["checks"]["unfrozen_check"] = True
    mutations.append(extra_check)
    open_boundary = copy.deepcopy(payload)
    open_boundary["validation_read"] = True
    mutations.append(open_boundary)
    for index, mutation in enumerate(mutations):
        mutation.pop("canonical_deployment_gate_receipt_sha256", None)
        path = _write_gate_document(
            tmp_path / f"invalid-gate-{index}.json",
            mutation,
            "canonical_deployment_gate_receipt_sha256",
        )
        with pytest.raises(subject.gate_v1.BuyE3DeploymentGateError, match="drifted"):
            gate_v1.validate_deployment_gate_receipt(path, expected_artifact_sha256="a" * 64)


def _resource_sample(
    *,
    monotonic_ns: int = 110,
    health_generation: int = 2,
    live_pid: int = 321,
    live_pid_start_ticks: int = 12_345,
    benchmark_pid: int = 654,
    benchmark_pid_start_ticks: int = 67_890,
    benchmark_rss_mib: float = 120.0,
) -> dict:
    return {
        "monotonic_ns": monotonic_ns,
        "live_pid": live_pid,
        "live_pid_start_ticks": live_pid_start_ticks,
        "benchmark_pid": benchmark_pid,
        "benchmark_pid_start_ticks": benchmark_pid_start_ticks,
        "benchmark_running": True,
        "health_generation": health_generation,
        "health_line_sha256": "3" * 64,
        "mem_available_mib": 700.0,
        "live_rss_mib": 300.0,
        "benchmark_rss_mib": benchmark_rss_mib,
        "deep_book_buffer": 0,
        "oom_events": 0,
        "swap_in_kib": 0,
        "swap_out_kib": 0,
        "counter_values": {name: 0 for name in subject.gate_v1.REQUIRED_ZERO_COUNTERS},
    }


def _resource_capture(samples: Sequence[dict], *, authority: str | None = None) -> dict:
    counters = {name: 0 for name in subject.gate_v1.REQUIRED_ZERO_COUNTERS}
    return {
        "authority": authority or subject.gate_v1.CONCURRENT_CAPTURE_AUTHORITY,
        "collector_pid": 987,
        "benchmark_command_sha256": "4" * 64,
        "benchmark_pid": samples[0]["benchmark_pid"],
        "benchmark_pid_start_ticks": samples[0]["benchmark_pid_start_ticks"],
        "live_pid": samples[0]["live_pid"],
        "live_pid_start_ticks": samples[0]["live_pid_start_ticks"],
        "benchmark_launch_monotonic_ns": 100,
        "first_overlap_sample_monotonic_ns": samples[0]["monotonic_ns"],
        "last_overlap_sample_monotonic_ns": samples[-1]["monotonic_ns"],
        "benchmark_exit_monotonic_ns": 130,
        "post_health_observed_monotonic_ns": 140,
        "pre_health_generation": 1,
        "post_health_generation": 4,
        "pre_health_line_sha256": "5" * 64,
        "post_health_line_sha256": "6" * 64,
        "pre_counter_values": counters,
        "post_counter_values": copy.deepcopy(counters),
        "pre_deep_book_buffer": 0,
        "post_deep_book_buffer": 0,
        "benchmark_returncode": 0,
        "benchmark_stdout_sha256": "7" * 64,
        "benchmark_stderr_sha256": "8" * 64,
        "post_health_after_benchmark_exit": True,
    }


def test_concurrent_resource_receipt_binds_2vcpu_2gib_combined_window(
    tmp_path: Path,
) -> None:
    gate_v1 = subject.gate_v1
    process = {
        "pid": 321,
        "pid_start_ticks": 12_345,
        "canonical_process_identity_sha256": "1" * 64,
        "artifact_sha256": "a" * 64,
        "execution_commit": "b" * 40,
    }
    benchmark = {
        "artifact_sha256": "a" * 64,
        "callback_benchmark": {
            "observed_live_rate_hz": 100.0,
            "achieved_rate_hz": 220.0,
            "latency_p99_us": 900.0,
        },
        "decision_benchmark": {"latency_p99_us": 4_000.0},
    }
    benchmark["canonical_benchmark_receipt_sha256"] = gate_v1._document_sha256(
        benchmark, "canonical_benchmark_receipt_sha256"
    )
    samples = [
        _resource_sample(),
        _resource_sample(
            monotonic_ns=120,
            health_generation=3,
            benchmark_rss_mib=130.0,
        ),
    ]
    capture = _resource_capture(samples)
    output = tmp_path / "resource.json"
    receipt = gate_v1.build_concurrent_resource_receipt(
        samples=samples,
        capture_provenance=capture,
        benchmark_receipt=benchmark,
        pre_process_identity=process,
        post_process_identity=copy.deepcopy(process),
        logical_cpu_count=2,
        mem_total_mib=2_048.0,
        expected_artifact_sha256="a" * 64,
        expected_execution_commit="b" * 40,
        expected_execution_tag="f05-compatible",
        output_path=output,
    )
    assert receipt["observed"]["max_combined_rss_mib"] == 430.0
    assert (
        gate_v1.validate_concurrent_resource_receipt(
            output,
            expected_artifact_sha256="a" * 64,
            expected_execution_commit="b" * 40,
            expected_execution_tag="f05-compatible",
            expected_disabled_process_identity=process,
        )["checks"]["combined_rss_at_most_768mib"]
        is True
    )
    assert output.stat().st_mode & 0o777 == 0o600
    assert output.stat().st_nlink == 1
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="already exists"):
        gate_v1.build_concurrent_resource_receipt(
            samples=samples,
            capture_provenance=capture,
            benchmark_receipt=benchmark,
            pre_process_identity=process,
            post_process_identity=copy.deepcopy(process),
            logical_cpu_count=2,
            mem_total_mib=2_048.0,
            expected_artifact_sha256="a" * 64,
            expected_execution_commit="b" * 40,
            expected_execution_tag="f05-compatible",
            output_path=output,
        )

    tampered = copy.deepcopy(receipt)
    tampered["observed"]["max_combined_rss_mib"] = 769.0
    tampered.pop("canonical_resource_receipt_sha256")
    tampered_path = _write_gate_document(
        tmp_path / "resource-tampered.json",
        tampered,
        "canonical_resource_receipt_sha256",
    )
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="drifted"):
        gate_v1.validate_concurrent_resource_receipt(
            tampered_path,
            expected_artifact_sha256="a" * 64,
            expected_execution_commit="b" * 40,
            expected_execution_tag="f05-compatible",
            expected_disabled_process_identity=process,
        )

    fabricated_capture = _resource_capture(samples, authority="user_authored_json_v1")
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="authority"):
        gate_v1.build_concurrent_resource_receipt(
            samples=samples,
            capture_provenance=fabricated_capture,
            benchmark_receipt=benchmark,
            pre_process_identity=process,
            post_process_identity=copy.deepcopy(process),
            logical_cpu_count=2,
            mem_total_mib=2_048.0,
            expected_artifact_sha256="a" * 64,
            expected_execution_commit="b" * 40,
            expected_execution_tag="f05-compatible",
            output_path=tmp_path / "fabricated-resource.json",
        )


class _FakeBenchmarkProcess:
    def __init__(self, *, pid: int = 654, alive_polls: int = 4) -> None:
        self.pid = pid
        self.alive_polls = alive_polls
        self.poll_count = 0
        self.returncode: int | None = None

    def poll(self) -> int | None:
        self.poll_count += 1
        if self.poll_count <= self.alive_polls:
            return None
        self.returncode = 0
        return self.returncode

    def communicate(self, *, timeout: float) -> tuple[str, str]:
        assert timeout == 10.0
        if self.returncode is None:
            self.returncode = 0
        return "benchmark complete\n", ""

    def terminate(self) -> None:
        self.returncode = -15

    def wait(self, *, timeout: float) -> int:
        assert timeout == 10.0
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def kill(self) -> None:
        self.returncode = -9


def _direct_capture_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    alive_polls: int = 4,
    stale_after_live_snapshots: int | None = None,
) -> tuple[dict, list[tuple[str, ...]]]:
    gate_v1 = subject.gate_v1
    artifact_manifest = tmp_path / "artifact-manifest.json"
    policy = tmp_path / "policy.json"
    predicate_bundle = tmp_path / "predicate-bundle.json"
    health_receipt_path = tmp_path / "health.json"
    pid_file = tmp_path / "live.pid"
    live_log = tmp_path / "live.log"
    for path in (artifact_manifest, policy, predicate_bundle, health_receipt_path):
        path.write_text("{}\n", encoding="ascii")
    pid_file.write_text("321\n", encoding="ascii")
    live_log.write_text("", encoding="ascii")

    benchmark_receipt = {
        "artifact_sha256": "a" * 64,
        "callback_benchmark": {
            "observed_live_rate_hz": 100.0,
            "achieved_rate_hz": 220.0,
            "latency_p99_us": 900.0,
        },
        "decision_benchmark": {"latency_p99_us": 4_000.0},
    }
    benchmark_receipt["canonical_benchmark_receipt_sha256"] = gate_v1._document_sha256(
        benchmark_receipt, "canonical_benchmark_receipt_sha256"
    )
    monkeypatch.setattr(
        gate_v1,
        "validate_health_receipt",
        lambda _path: {
            "runtime": {"buy_e3_enabled": False},
            "repository": {"commit": "b" * 40},
        },
    )
    monkeypatch.setattr(
        gate_v1,
        "validate_benchmark_receipt",
        lambda _path, *, expected_artifact_sha256: benchmark_receipt,
    )

    monotonic_value = 100

    def monotonic_ns() -> int:
        nonlocal monotonic_value
        monotonic_value += 10
        return monotonic_value

    health_generation = 0

    def health_state() -> dict:
        nonlocal health_generation
        health_generation += 1
        return {
            "generation": health_generation,
            "line_sha256": f"{health_generation:064x}",
            "deep_book_buffer": 0,
            "counter_values": {name: 0 for name in gate_v1.REQUIRED_ZERO_COUNTERS},
        }

    live_snapshot_count = 0

    def process_snapshot(pid: int) -> dict:
        nonlocal live_snapshot_count
        if pid == 321:
            live_snapshot_count += 1
            start_ticks = 12_345
            if (
                stale_after_live_snapshots is not None
                and live_snapshot_count > stale_after_live_snapshots
            ):
                start_ticks += 1
            command = ["python", "live/main.py", "--config", "live/config.yaml"]
        else:
            start_ticks = 67_890
            command = ["python", str(Path(gate_v1.__file__)), "benchmark"]
        return {
            "pid": pid,
            "pid_start_ticks": start_ticks,
            "cmdline": command,
            "cmdline_sha256": gate_v1._canonical_sha256(command),
        }

    commands: list[tuple[str, ...]] = []

    def popen_factory(command: Sequence[str], **_kwargs) -> _FakeBenchmarkProcess:
        commands.append(tuple(command))
        return _FakeBenchmarkProcess(alive_polls=alive_polls)

    disabled_process = {
        "pid": 321,
        "pid_start_ticks": 12_345,
        "canonical_process_identity_sha256": "1" * 64,
        "artifact_sha256": "a" * 64,
        "execution_commit": "b" * 40,
    }
    kwargs = {
        "repository_root": tmp_path,
        "disabled_process_identity": disabled_process,
        "pid_file": pid_file,
        "live_log_path": live_log,
        "artifact_manifest_path": artifact_manifest,
        "artifact_manifest_file_sha256": "2" * 64,
        "expected_artifact_sha256": "a" * 64,
        "policy_path": policy,
        "policy_file_sha256": "3" * 64,
        "predicate_bundle_path": predicate_bundle,
        "predicate_bundle_file_sha256": "4" * 64,
        "health_receipt_path": health_receipt_path,
        "expected_execution_commit": "b" * 40,
        "expected_execution_tag": "f05-compatible",
        "benchmark_output_path": tmp_path / "benchmark.json",
        "output_path": tmp_path / "resource.json",
        "python_executable": Path(gate_v1.sys.executable),
        "paced_duration_s": 2.0,
        "sample_interval_s": 0.01,
        "post_health_timeout_s": 1.0,
        "_popen_factory": popen_factory,
        "_process_snapshot_provider": process_snapshot,
        "_resource_metrics_provider": lambda _live_pid, _benchmark_pid: {
            "mem_available_mib": 700.0,
            "live_rss_mib": 300.0,
            "benchmark_rss_mib": 120.0,
            "oom_events": 0,
            "swap_in_kib": 0,
            "swap_out_kib": 0,
        },
        "_health_state_provider": health_state,
        "_host_identity_provider": lambda: {
            "logical_cpu_count": 2,
            "mem_total_mib": 2_048.0,
        },
        "_git_identity_provider": lambda _root: {
            "commit": "b" * 40,
            "annotated_tags_at_head": ["f05-compatible"],
            "worktree_clean": True,
        },
        "_monotonic_ns": monotonic_ns,
        "_sleep": lambda _duration: None,
    }
    return kwargs, commands


def test_direct_concurrent_collector_proves_live_benchmark_overlap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_v1 = subject.gate_v1
    kwargs, commands = _direct_capture_inputs(tmp_path, monkeypatch)
    receipt = gate_v1.capture_concurrent_resource_receipt(**kwargs)
    capture = receipt["capture"]
    assert capture["authority"] == gate_v1.CONCURRENT_CAPTURE_AUTHORITY
    assert capture["benchmark_launch_monotonic_ns"] <= receipt["samples"][0]["monotonic_ns"]
    assert receipt["samples"][-1]["monotonic_ns"] < capture["benchmark_exit_monotonic_ns"]
    assert len(receipt["samples"]) == 2
    assert all(sample["benchmark_running"] is True for sample in receipt["samples"])
    assert commands and "benchmark" in commands[0]
    assert not any("samples" in argument for argument in commands[0])
    output = kwargs["output_path"]
    assert output.stat().st_mode & 0o777 == 0o600
    assert output.stat().st_nlink == 1


def test_direct_concurrent_collector_rejects_sequential_benchmark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_v1 = subject.gate_v1
    kwargs, _commands = _direct_capture_inputs(tmp_path, monkeypatch, alive_polls=0)
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="did not overlap"):
        gate_v1.capture_concurrent_resource_receipt(**kwargs)
    assert not kwargs["output_path"].exists()


def test_direct_concurrent_collector_rejects_stale_live_pid_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_v1 = subject.gate_v1
    kwargs, _commands = _direct_capture_inputs(
        tmp_path,
        monkeypatch,
        stale_after_live_snapshots=3,
    )
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="stale or unexpected"):
        gate_v1.capture_concurrent_resource_receipt(**kwargs)
    assert not kwargs["output_path"].exists()


def test_capture_concurrent_cli_rejects_user_sample_json(tmp_path: Path) -> None:
    gate_v1 = subject.gate_v1
    required = [
        "capture-concurrent",
        "--repository-root",
        str(tmp_path),
        "--disabled-phase-receipt",
        str(tmp_path / "disabled.json"),
        "--pid-file",
        str(tmp_path / "live.pid"),
        "--live-log",
        str(tmp_path / "live.log"),
        "--artifact-manifest",
        str(tmp_path / "manifest.json"),
        "--artifact-manifest-file-sha256",
        "1" * 64,
        "--artifact-sha256",
        "2" * 64,
        "--policy",
        str(tmp_path / "policy.json"),
        "--policy-file-sha256",
        "3" * 64,
        "--predicate-bundle",
        str(tmp_path / "predicates.json"),
        "--predicate-bundle-file-sha256",
        "4" * 64,
        "--health-receipt",
        str(tmp_path / "health.json"),
        "--execution-commit",
        "5" * 40,
        "--execution-tag",
        "f05-compatible",
        "--benchmark-output",
        str(tmp_path / "benchmark.json"),
        "--output",
        str(tmp_path / "resource.json"),
        "--samples-json",
        str(tmp_path / "fabricated.json"),
    ]
    with pytest.raises(SystemExit):
        gate_v1._parser().parse_args(required)


def test_git_identity_explicitly_includes_all_untracked_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_v1 = subject.gate_v1
    commands: list[tuple[str, ...]] = []

    def fake_run(command: Sequence[str], **_kwargs) -> subprocess.CompletedProcess[str]:
        commands.append(tuple(command))
        if command[1:3] == ("tag", "--points-at"):
            stdout = "f05-compatible\n"
        elif command[1:3] == ("cat-file", "-t"):
            stdout = "tag\n"
        elif command[1:3] == ("rev-parse", "HEAD"):
            stdout = "b" * 40 + "\n"
        else:
            stdout = ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(gate_v1.subprocess, "run", fake_run)
    identity = gate_v1._git_identity(tmp_path)
    assert identity["worktree_clean"] is True
    assert (
        "git",
        "status",
        "--porcelain",
        "--untracked-files=all",
    ) in commands


def _runtime_regression_receipt(repo: Path, path: Path) -> Path:
    gate_v1 = subject.gate_v1
    executable = Path(sys.executable).resolve()
    junit_path = path.parent / "frozen-junit.xml"
    nodeids = sorted(
        f"{relative}::test_frozen_nodeid_source_binding"
        for relative in gate_v1.RUNTIME_REGRESSION_TESTS
    )
    collect_command = [
        str(executable),
        "-m",
        "pytest",
        "--collect-only",
        "-q",
        *gate_v1.RUNTIME_REGRESSION_TESTS,
    ]
    run_command = [
        str(executable),
        "-m",
        "pytest",
        "-q",
        f"--junitxml={junit_path}",
        *nodeids,
    ]
    coverage_names = (
        "buy_disabled_equals_b0",
        "exposure_increasing_buy_only",
        "reducing_buy_unchanged",
        "fixed_action_total_cooldown",
        "b0_consecutive_units",
        "partial_fill_and_consecutive_units",
        "restart_and_rollback",
        "gap_and_out_of_order",
        "stale_unobserved_hash_drift_fallback",
        "sell_integration_unchanged",
    )
    payload = {
        "schema_version": gate_v1.COMPATIBLE_REGRESSION_SCHEMA,
        "identity": gate_v1.OWNER_IDENTITY,
        "status": "passed",
        "generated_utc": "2026-08-23T00:00:00Z",
        "artifact_sha256": "a" * 64,
        "execution_commit": "b" * 40,
        "execution_tag": "f05-compatible",
        "python_executable": str(executable),
        "python_file_sha256": gate_v1._file_sha256(executable),
        "collect_command": collect_command,
        "run_command": run_command,
        "nodeids": nodeids,
        "nodeid_manifest_sha256": gate_v2.canonical_sha256(nodeids),
        "nodeid_source_counts": {relative: 1 for relative in gate_v1.RUNTIME_REGRESSION_TESTS},
        "collected": len(nodeids),
        "executed": len(nodeids),
        "passed": len(nodeids),
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "collection_return_code": 0,
        "return_code": 0,
        "collection_stdout_sha256": "1" * 64,
        "collection_stderr_sha256": "2" * 64,
        "run_stdout_sha256": "3" * 64,
        "run_stderr_sha256": "4" * 64,
        "test_files": {
            relative: gate_v1._file_sha256(repo / relative)
            for relative in gate_v1.RUNTIME_REGRESSION_TESTS
        },
        "runtime_sources": {
            relative: gate_v1._file_sha256(repo / relative)
            for relative in gate_v1.RUNTIME_REGRESSION_SOURCES
        },
        "coverage": {name: True for name in coverage_names},
        **gate_v1.EVIDENCE_BOUNDARY,
    }
    return _write_gate_document(path, payload, "canonical_receipt_sha256")


def test_runtime_regression_receipt_freezes_nodeids_and_critical_sources(
    tmp_path: Path,
) -> None:
    gate_v1 = subject.gate_v1
    repo = Path(__file__).resolve().parents[1]
    receipt_path = _runtime_regression_receipt(repo, tmp_path / "regression.json")
    receipt = gate_v1.validate_runtime_regression_receipt(
        receipt_path,
        repository_root=repo,
        expected_artifact_sha256="a" * 64,
        expected_execution_commit="b" * 40,
        expected_execution_tag="f05-compatible",
    )
    assert {
        "tests/test_fill_cooldown_checkpoint.py",
        "tests/test_deploy_f05_buy_e3_owner_v1.py",
        "tests/test_f05_buy_e3_execution_attempt.py",
        "tests/test_f05_buy_e3_stability_receipts.py",
        "tests/test_f05_buy_e3_durability_gate.py",
        "tests/test_live_buy_e3_startup_attestation.py",
        "tests/test_live_run_authority_environment.py",
        "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_v1.py",
        "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_amendment_v2.py",
        "tests/test_f05_buy_e3_active_release.py",
        "tests/test_live_buy_e3_active_release_runtime.py",
        "tests/test_prospective_baseline_epoch.py",
        "tests/test_private_evidence_governance.py",
    }.issubset(receipt["test_files"])
    assert {
        "live/run.sh",
        "scripts/f05_buy_e3_active_release.py",
        "scripts/f05_buy_e3_final_composition_contract.py",
        "scripts/f05_buy_e3_stability_receipts.py",
        "scripts/f05_buy_e3_durability_gate.py",
        "models/replay/prospective_baseline_epoch.py",
        "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_v1.py",
        "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_amendment_v2.py",
    }.issubset(receipt["runtime_sources"])

    tampered = copy.deepcopy(receipt)
    tampered["runtime_sources"]["scripts/audit_private_evidence.py"] = "0" * 64
    tampered.pop("canonical_receipt_sha256")
    tampered_path = _write_gate_document(
        tmp_path / "regression-tampered.json", tampered, "canonical_receipt_sha256"
    )
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="drifted"):
        gate_v1.validate_runtime_regression_receipt(
            tampered_path,
            repository_root=repo,
            expected_artifact_sha256="a" * 64,
            expected_execution_commit="b" * 40,
            expected_execution_tag="f05-compatible",
        )


def test_compatible_runtime_regression_preserves_lexical_venv_python(
    tmp_path: Path,
) -> None:
    physical = tmp_path / "runtime/python3"
    physical.parent.mkdir()
    physical.write_text("physical interpreter\n", encoding="ascii")
    lexical = tmp_path / ".venv/bin/python"
    lexical.parent.mkdir(parents=True)
    lexical.symlink_to(physical)

    observed = subject.gate_v1._lexical_python_executable(lexical)

    assert observed == lexical.absolute()
    assert observed != physical.resolve()


def _sell_54_case_receipt(path: Path, artifact_files: dict[str, str]) -> Path:
    gate_v1 = subject.gate_v1
    payload = {
        "schema_version": gate_v1.SELL_PARITY_SCHEMA,
        "identity": gate_v1.OWNER_IDENTITY,
        "status": "parity_complete",
        "layer": "sell_owner_54_case_unchanged",
        "artifact_sha256": "a" * 64,
        "artifact_manifest_file_sha256": artifact_files["manifest"],
        "policy_file_sha256": artifact_files["policy"],
        "predicate_bundle_file_sha256": artifact_files["predicate_bundle"],
        "evidence": {
            "policy_sha256": "5" * 64,
            "predicate_bundle_sha256": "6" * 64,
            "predicate_columns": ["campaign_age", "h16_h256_cross_recency"],
            "sell_tri_state_cases": 27,
            "buy_tri_state_cases": 27,
            "mismatch_count": 0,
            "documented_semantics_equal": True,
            "runtime_binding_valid": True,
        },
        "economic_values_materialized_by_replay": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    return _write_gate_document(path, payload, "canonical_receipt_sha256")


def test_sell_54_case_receipt_binds_real_cases_and_evaluator_sources(tmp_path: Path) -> None:
    gate_v1 = subject.gate_v1
    repo = Path(__file__).resolve().parents[1]
    artifact_files = {
        "manifest": "1" * 64,
        "policy": "2" * 64,
        "predicate_bundle": "3" * 64,
    }
    path = _sell_54_case_receipt(tmp_path / "sell.json", artifact_files)
    binding = gate_v1.validate_sell_owner_54_case_receipt(
        path,
        repository_root=repo,
        expected_artifact_sha256="a" * 64,
        expected_artifact_files=artifact_files,
    )
    assert set(binding["source_files"]) == set(gate_v1.SELL_54_CASE_SOURCE_PATHS)

    tampered = json.loads(path.read_text(encoding="ascii"))
    tampered["evidence"]["sell_tri_state_cases"] = 26
    tampered.pop("canonical_receipt_sha256")
    tampered_path = _write_gate_document(
        tmp_path / "sell-tampered.json", tampered, "canonical_receipt_sha256"
    )
    with pytest.raises(gate_v1.BuyE3DeploymentGateError, match="drifted"):
        gate_v1.validate_sell_owner_54_case_receipt(
            tampered_path,
            repository_root=repo,
            expected_artifact_sha256="a" * 64,
            expected_artifact_files=artifact_files,
        )


def _mock_activation_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path]:
    resource_path = tmp_path / "resource-evidence.json"
    regression_path = tmp_path / "regression-evidence.json"
    sell_path = tmp_path / "sell-evidence.json"
    for path in (resource_path, regression_path, sell_path):
        path.write_text("{}\n", encoding="ascii")
        path.chmod(0o600)

    monkeypatch.setattr(
        subject.gate_v1,
        "validate_concurrent_resource_receipt",
        lambda *_args, **_kwargs: {
            "canonical_resource_receipt_sha256": "1" * 64,
        },
    )
    monkeypatch.setattr(
        subject.gate_v1,
        "validate_runtime_regression_receipt",
        lambda *_args, **_kwargs: {
            "canonical_receipt_sha256": "2" * 64,
            "nodeid_manifest_sha256": "3" * 64,
            "test_files": {"tests/frozen.py": "4" * 64},
            "runtime_sources": {"runtime/frozen.py": "5" * 64},
        },
    )

    def validate_sell(path: Path, **_kwargs):
        target = Path(path).resolve(strict=True)
        source_files = {"sell/runtime.py": "8" * 64}
        return {
            "path": str(target),
            "file_sha256": gate_v2.file_sha256(target),
            "canonical_receipt_sha256": "6" * 64,
            "sell_policy_sha256": "7" * 64,
            "sell_predicate_bundle_sha256": "9" * 64,
            "source_files": source_files,
            "source_manifest_sha256": gate_v2.canonical_sha256(source_files),
        }

    monkeypatch.setattr(
        subject.gate_v1,
        "validate_sell_owner_54_case_receipt",
        validate_sell,
    )
    return resource_path, regression_path, sell_path


def _mock_active_release(
    tmp_path: Path,
    plan: dict,
    envelope_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, dict]:
    attempt_final_path = tmp_path / "compatible-attempt-final.json"
    attempt_final_path.write_text("{}\n", encoding="ascii")
    attempt_final_path.chmod(0o600)
    attempt_final_canonical = "e" * 64
    attempt_final = {
        "canonical_final_receipt_sha256": attempt_final_canonical,
        "runtime_execution": {
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "annotated_tag": plan["execution"]["annotated_tag"],
            "annotated_tag_object": plan["execution"]["annotated_tag_object"],
            "tag_peeled_commit": plan["execution"]["execution_commit"],
        },
        "attempt_manifest": {
            **plan["execution"]["compatible_attempt_manifest"],
            "size_bytes": Path(plan["execution"]["compatible_attempt_manifest"]["path"])
            .stat()
            .st_size,
        },
    }
    envelope = json.loads(envelope_path.read_text(encoding="ascii"))
    artifact_roles = {
        "manifest": {
            "path": plan["artifact"]["manifest_path"],
            "file_sha256": plan["artifact"]["manifest_file_sha256"],
        },
        "policy": {
            "path": plan["artifact"]["policy_path"],
            "file_sha256": plan["artifact"]["policy_file_sha256"],
        },
        "predicate_bundle": {
            "path": plan["artifact"]["predicate_bundle_path"],
            "file_sha256": plan["artifact"]["predicate_bundle_file_sha256"],
        },
    }
    payload = {
        "schema_version": subject.active_release.ACTIVE_RELEASE_SCHEMA,
        "identity": subject.active_release.ACTIVE_RELEASE_IDENTITY,
        "status": subject.active_release.ACTIVE_RELEASE_STATUS,
        "execution": {
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "annotated_operational_tag": plan["execution"]["annotated_tag"],
            "annotated_operational_tag_object": plan["execution"]["annotated_tag_object"],
            "tag_peeled_commit": plan["execution"]["execution_commit"],
        },
        "exact_artifact": {
            "artifact_sha256": plan["artifact"]["artifact_sha256"],
            "roles": artifact_roles,
        },
        "evidence": {
            "compatible_attempt_final": {
                "path": str(attempt_final_path),
                "file_sha256": gate_v2.file_sha256(attempt_final_path),
                "canonical_sha256": attempt_final_canonical,
            },
            "activation_envelope": {
                "path": str(envelope_path),
                "file_sha256": gate_v2.file_sha256(envelope_path),
                "canonical_sha256": envelope["canonical_activation_envelope_sha256"],
            },
        },
    }
    payload["canonical_active_release_sha256"] = subject.active_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )
    release_path = tmp_path / "active-release.json"
    release_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    release_path.chmod(0o600)
    monkeypatch.setattr(
        subject.active_release,
        "validate_active_release",
        lambda path, **_kwargs: copy.deepcopy(json.loads(Path(path).read_text(encoding="ascii"))),
    )
    monkeypatch.setattr(
        subject.execution_attempt,
        "validate_final_receipt",
        lambda *_args, **_kwargs: copy.deepcopy(attempt_final),
    )
    binding = {
        "local_path": str(release_path.resolve(strict=True)),
        "remote_path": subject._remote_active_release_path(
            "/remote/repo", gate_v2.file_sha256(release_path)
        ),
        "file_sha256": gate_v2.file_sha256(release_path),
        "canonical_active_release_sha256": payload["canonical_active_release_sha256"],
        "schema_version": subject.active_release.ACTIVE_RELEASE_SCHEMA,
        "status": subject.active_release.ACTIVE_RELEASE_STATUS,
    }
    return release_path, binding


def _compatible_activation_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict, Path, dict, Path, Path, dict]:
    spec = _specification(tmp_path)
    _compatible_attempt(tmp_path, spec, monkeypatch)
    _patch_plan_dependencies(monkeypatch, tmp_path, spec)
    plan = subject.build_plan(
        specification=spec,
        repository_root=tmp_path,
        preflight_runner=lambda _repo, _config, enabled: _preflight(enabled),
    )
    disabled_path, _receipt, disabled_process = _successful_disabled_receipt(tmp_path, plan)
    resource_path, regression_path, sell_path = _mock_activation_evidence(tmp_path, monkeypatch)
    envelope_path = tmp_path / "activation-envelope.json"
    subject.build_compatible_activation_envelope(
        plan=plan,
        disabled_phase_receipt_path=disabled_path,
        concurrent_resource_receipt_path=resource_path,
        runtime_regression_receipt_path=regression_path,
        sell_54_case_receipt_path=sell_path,
        output_path=envelope_path,
    )
    release_path, release_binding = _mock_active_release(
        tmp_path,
        plan,
        envelope_path,
        monkeypatch,
    )
    return (
        plan,
        disabled_path,
        disabled_process,
        envelope_path,
        release_path,
        release_binding,
    )


def _direct_v3_activation_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict, Path, dict, Path, Path, dict]:
    (
        plan,
        disabled_path,
        disabled_process,
        historical_envelope_path,
        _legacy_release_path,
        _legacy_release_binding,
    ) = _compatible_activation_context(tmp_path, monkeypatch)
    payload = {
        "schema_version": subject.buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
        "identity": subject.buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_IDENTITY,
        "status": subject.buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_STATUS,
        "execution": {
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "annotated_operational_tag": plan["execution"]["annotated_tag"],
            "annotated_operational_tag_object": plan["execution"]["annotated_tag_object"],
            "tag_peeled_commit": plan["execution"]["execution_commit"],
        },
        "exact_artifact": {
            "artifact_sha256": plan["artifact"]["artifact_sha256"],
            "roles": {
                role: {"file_sha256": plan["artifact"][field]}
                for role, field in {
                    "manifest": "manifest_file_sha256",
                    "policy": "policy_file_sha256",
                    "predicate_bundle": "predicate_bundle_file_sha256",
                }.items()
            },
        },
        "config_pair": {
            "active": {
                "file_sha256": plan["configs"]["active"]["config_sha256"]
            },
            "disabled": {
                "file_sha256": plan["configs"]["disabled"]["config_sha256"]
            },
        },
    }
    payload["canonical_active_release_sha256"] = subject.active_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )
    release_path = tmp_path / "direct-v3-active-release.json"
    release_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    release_path.chmod(0o600)

    def validate_v3(observed, **expected):
        if (
            observed.get("schema_version") != payload["schema_version"]
            or observed.get("status") != payload["status"]
            or observed.get("canonical_active_release_sha256")
            != subject.active_release.document_sha256(
                observed, "canonical_active_release_sha256"
            )
            or expected["expected_canonical_sha256"]
            != observed.get("canonical_active_release_sha256")
            or expected["expected_artifact_sha256"]
            != plan["artifact"]["artifact_sha256"]
        ):
            raise ValueError("test direct-v3 release drifted")
        return {
            "file_canonical_sha256": observed["canonical_active_release_sha256"],
            "execution_commit": plan["execution"]["execution_commit"],
            "execution_tree": plan["execution"]["execution_tree"],
            "annotated_operational_tag": plan["execution"]["annotated_tag"],
            "annotated_operational_tag_object": plan["execution"][
                "annotated_tag_object"
            ],
            "active_config_file_sha256": plan["configs"]["active"][
                "config_sha256"
            ],
            "disabled_config_file_sha256": plan["configs"]["disabled"][
                "config_sha256"
            ],
        }

    monkeypatch.setattr(subject.buy_e3_runtime, "_validate_active_release", validate_v3)
    file_sha256 = gate_v2.file_sha256(release_path)
    binding = {
        "local_path": str(release_path.resolve(strict=True)),
        "remote_path": subject._remote_active_release_path(
            plan["active_pointer"]["repo_root"], file_sha256
        ),
        "file_sha256": file_sha256,
        "canonical_active_release_sha256": payload[
            "canonical_active_release_sha256"
        ],
        "schema_version": payload["schema_version"],
        "status": payload["status"],
        "active_config_file_sha256": plan["configs"]["active"]["config_sha256"],
        "disabled_config_file_sha256": plan["configs"]["disabled"][
            "config_sha256"
        ],
    }
    return (
        plan,
        disabled_path,
        disabled_process,
        historical_envelope_path,
        release_path,
        binding,
    )


def test_compatible_activation_envelope_binds_all_receipts_before_fake_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        disabled_path,
        disabled_process,
        envelope_path,
        release_path,
        release_binding,
    ) = _compatible_activation_context(tmp_path, monkeypatch)
    envelope = subject.validate_compatible_activation_envelope(
        envelope_path,
        plan=plan,
        disabled_phase_receipt_path=disabled_path,
    )

    validated = subject.validate_compatible_activation_envelope(
        envelope_path,
        plan=plan,
        disabled_phase_receipt_path=disabled_path,
    )
    assert validated == envelope
    assert all(envelope["checks"].values())

    runner = Mock(side_effect=AssertionError("historical authority must not run"))
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="historical standalone release is not current live activation authority",
    ):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=tmp_path / "historical-rejected.json",
            disabled_phase_receipt_path=disabled_path,
            activation_envelope_path=envelope_path,
            active_release_path=release_path,
        )
    runner.assert_not_called()

    for field in (
        "concurrent_resource_receipt",
        "runtime_regression_receipt",
        "sell_54_case_receipt",
    ):
        tampered = copy.deepcopy(envelope)
        tampered.pop(field)
        tampered.pop("canonical_activation_envelope_sha256")
        path = _write_gate_document(
            tmp_path / f"envelope-missing-{field}.json",
            tampered,
            "canonical_activation_envelope_sha256",
        )
        with pytest.raises(subject.BuyE3TransactionalDeployError, match="fields drifted"):
            subject.validate_compatible_activation_envelope(
                path,
                plan=plan,
                disabled_phase_receipt_path=disabled_path,
            )


def test_direct_v3_activation_rejects_historical_envelope_before_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        disabled_path,
        _disabled_process,
        envelope_path,
        release_path,
        _release_binding,
    ) = _direct_v3_activation_context(tmp_path, monkeypatch)
    runner = Mock(side_effect=AssertionError("v3 envelope rejection must precede runner"))
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="forbids a historical activation envelope",
    ):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=tmp_path / "v3-envelope-rejected.json",
            disabled_phase_receipt_path=disabled_path,
            activation_envelope_path=envelope_path,
            active_release_path=release_path,
        )
    runner.assert_not_called()


def _activation_envelope_binding(
    plan: dict,
    envelope_path: Path,
    disabled_path: Path,
) -> dict:
    envelope = subject.validate_compatible_activation_envelope(
        envelope_path,
        plan=plan,
        disabled_phase_receipt_path=disabled_path,
    )
    target = envelope_path.resolve(strict=True)
    return {
        "path": str(target),
        "file_sha256": gate_v2.file_sha256(target),
        "canonical_activation_envelope_sha256": envelope["canonical_activation_envelope_sha256"],
        "concurrent_resource_receipt_sha256": envelope["concurrent_resource_receipt"][
            "canonical_sha256"
        ],
        "runtime_regression_receipt_sha256": envelope["runtime_regression_receipt"][
            "canonical_sha256"
        ],
        "sell_54_case_receipt_sha256": envelope["sell_54_case_receipt"]["canonical_sha256"],
    }


def _rewrite_active_release(path: Path, mutator) -> dict:
    payload = json.loads(path.read_text(encoding="ascii"))
    mutator(payload)
    payload["canonical_active_release_sha256"] = subject.active_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return payload


def _synthetic_active_release_binding(plan: dict) -> dict:
    file_sha256 = "d" * 64
    return {
        "local_path": "/private/local-active-release.json",
        "remote_path": subject._remote_active_release_path(
            plan["active_pointer"]["repo_root"], file_sha256
        ),
        "file_sha256": file_sha256,
        "canonical_active_release_sha256": "e" * 64,
        "schema_version": subject.buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA,
        "status": subject.buy_e3_runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_STATUS,
        "active_config_file_sha256": plan["configs"]["active"]["config_sha256"],
        "disabled_config_file_sha256": plan["configs"]["disabled"]["config_sha256"],
    }


def test_active_release_phase_binding_requires_content_addressed_remote_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    binding = _synthetic_active_release_binding(plan)
    assert subject._validate_active_release_phase_binding(binding, plan=plan) == binding

    drifted = dict(binding)
    drifted["remote_path"] = (
        f"{plan['active_pointer']['repo_root']}/"
        "live/private/f05_buy_e3_owner_v1/active_release.json"
    )
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="phase binding identity drifted",
    ):
        subject._validate_active_release_phase_binding(drifted, plan=plan)


def _validate_fixture_startup(
    plan: dict,
    attestation: dict,
    *,
    expected_enabled: bool,
    active_release_binding: dict | None,
    allow_legacy: bool = False,
) -> dict:
    expected = subject._expected_process_binding(
        plan,
        "activate" if expected_enabled else "disabled-deploy",
        active_release_binding,
    )
    return subject._validate_startup_attestation(
        attestation,
        expected_schema_version=(
            str(attestation.get("schema_version"))
            if allow_legacy
            else subject.STARTUP_ATTESTATION_SCHEMA
        ),
        expected_execution_commit=expected["execution_commit"],
        expected_execution_tree=expected["execution_tree"],
        expected_artifact_sha256=expected["artifact_sha256"],
        expected_runtime_sources=plan["runtime_sources"],
        expected_repository_root=expected["repo_root"],
        expected_python_executable=expected["python_executable"],
        expected_python_binary_resolved=expected["python_executable"],
        expected_config_sha256=expected["config_sha256"],
        expected_enabled=expected_enabled,
        expected_active_release=active_release_binding,
        allow_legacy=allow_legacy,
    )


def _as_frozen_07ef_startup_v4(attestation: dict) -> dict:
    historical = copy.deepcopy(attestation)
    historical["schema_version"] = subject.HISTORICAL_STARTUP_ATTESTATION_SCHEMA
    historical.pop("shadow_runtime_identity")
    historical["buy_e3_active_release"].pop("active_config_file_sha256")
    historical["buy_e3_active_release"].pop("disabled_config_file_sha256")
    for field in subject._STARTUP_GATE_FIELDS - subject._HISTORICAL_STARTUP_GATE_FIELDS:
        historical["gates"].pop(field)
    for role in subject._LOADED_RUNTIME_MODULE_ROLES - (
        subject._HISTORICAL_LOADED_RUNTIME_MODULE_ROLES
    ):
        historical["loaded_module_origins"].pop(role)
    assert set(historical) == subject._HISTORICAL_STARTUP_ATTESTATION_FIELDS
    assert set(historical["gates"]) == subject._HISTORICAL_STARTUP_GATE_FIELDS
    return historical


def test_frozen_07ef_startup_v4_is_historical_only_and_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    current = _runtime_identity_payload(
        plan,
        "disabled-deploy",
        pid=499,
    )["startup_attestation"]
    historical = _as_frozen_07ef_startup_v4(current)

    assert _validate_fixture_startup(
        plan,
        historical,
        expected_enabled=False,
        active_release_binding=None,
        allow_legacy=True,
    ) == historical
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="startup attestation (fields|schema)",
    ):
        _validate_fixture_startup(
            plan,
            historical,
            expected_enabled=False,
            active_release_binding=None,
        )

    for mutation in (
        lambda value: value.__setitem__("shadow_runtime_identity", {}),
        lambda value: value["gates"].__setitem__("shadow_config_explicit", True),
        lambda value: value["buy_e3_active_release"].pop("path"),
    ):
        tampered = copy.deepcopy(historical)
        mutation(tampered)
        with pytest.raises(subject.BuyE3TransactionalDeployError):
            _validate_fixture_startup(
                plan,
                tampered,
                expected_enabled=False,
                active_release_binding=None,
                allow_legacy=True,
            )


def test_current_activation_requires_direct_v3_release_before_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        disabled_path,
        _disabled_process,
        envelope_path,
        _release_path,
        _release_binding,
    ) = _compatible_activation_context(tmp_path, monkeypatch)
    called = False

    def runner(_command):
        nonlocal called
        called = True
        raise AssertionError("runner must not execute without the post-envelope release")

    with pytest.raises(PermissionError, match="exact direct-owner v3 active release"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=tmp_path / "missing-release.json",
            disabled_phase_receipt_path=disabled_path,
            activation_envelope_path=envelope_path,
        )
    assert called is False


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda payload: payload["execution"].__setitem__("execution_commit", "0" * 40),
            "execution commit/tree/tag differs",
        ),
        (
            lambda payload: payload["evidence"]["activation_envelope"].__setitem__(
                "canonical_sha256", "0" * 64
            ),
            "binds another activation envelope",
        ),
        (
            lambda payload: payload["evidence"]["compatible_attempt_final"].__setitem__(
                "canonical_sha256", "0" * 64
            ),
            "compatible attempt final differs from the plan",
        ),
    ],
)
def test_active_release_rejects_wrong_plan_or_envelope_before_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation,
    message: str,
) -> None:
    (
        plan,
        disabled_path,
        _disabled_process,
        envelope_path,
        release_path,
        _release_binding,
    ) = _compatible_activation_context(tmp_path, monkeypatch)
    _rewrite_active_release(release_path, mutation)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match=message):
        subject._validate_active_release_for_activation(
            release_path,
            plan=plan,
            activation_envelope_binding=_activation_envelope_binding(
                plan,
                envelope_path,
                disabled_path,
            ),
        )


def test_active_release_validation_rejects_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        disabled_path,
        _disabled_process,
        envelope_path,
        release_path,
        _release_binding,
    ) = _compatible_activation_context(tmp_path, monkeypatch)
    envelope_binding = _activation_envelope_binding(plan, envelope_path, disabled_path)
    original_payload = json.loads(release_path.read_text(encoding="ascii"))
    replacement = tmp_path / "replacement-release.json"
    replacement.write_bytes(release_path.read_bytes())
    replacement.chmod(0o600)
    displaced = tmp_path / "displaced-release.json"

    def swapping_validator(path: Path, **_kwargs):
        Path(path).rename(displaced)
        replacement.rename(path)
        return copy.deepcopy(original_payload)

    monkeypatch.setattr(
        subject.active_release,
        "validate_active_release",
        swapping_validator,
    )
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="changed during independent validation|path was replaced",
    ):
        subject._validate_active_release_for_activation(
            release_path,
            plan=plan,
            activation_envelope_binding=envelope_binding,
        )


def test_active_release_validation_rejects_symlinked_parent(
    tmp_path: Path,
) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    release = real_parent / "release.json"
    release.write_text("{}\n", encoding="ascii")
    release.chmod(0o600)
    alias = tmp_path / "alias"
    alias.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="symbolic link"):
        with subject._stable_private_active_release(alias / release.name):
            pass


def test_activation_receipt_rejects_post_activation_release_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        disabled_path,
        disabled_process,
        envelope_path,
        release_path,
        release_binding,
    ) = _direct_v3_activation_context(tmp_path, monkeypatch)
    runner, _commands = _successful_runner(
        plan,
        "activate",
        disabled_process=disabled_process,
        active_release_binding=release_binding,
    )
    receipt_path = tmp_path / "activation-before-release-tamper.json"
    subject.execute_phase(
        plan=plan,
        phase="activate",
        token="token-activate",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=receipt_path,
        disabled_phase_receipt_path=disabled_path,
        activation_envelope_path=None,
        active_release_path=release_path,
    )

    release_path.write_bytes(release_path.read_bytes() + b" \n")
    release_path.chmod(0o600)
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="direct-owner v3 active release bytes drifted",
    ):
        subject.validate_phase_receipt(
            receipt_path,
            plan=plan,
            expected_phase="activate",
        )


def test_post_envelope_stage_failure_rolls_back_to_release_free_b0(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        disabled_path,
        disabled_process,
        envelope_path,
        release_path,
        release_binding,
    ) = _direct_v3_activation_context(tmp_path, monkeypatch)
    base_runner, _commands = _successful_runner(
        plan,
        "activate",
        disabled_process=disabled_process,
        active_release_binding=release_binding,
    )
    rows = subject._activation_rows_with_active_release(plan, release_binding)
    labels = {tuple(row["argv"]): row["label"] for row in rows}
    stage_failed = False

    def runner(command):
        nonlocal stage_failed
        if labels.get(tuple(command)) == "stage-active-release" and not stage_failed:
            stage_failed = True
            return subprocess.CompletedProcess(command, 17, "", "stage failed")
        return base_runner(command)

    receipt_path = tmp_path / "failed-post-envelope-stage.json"
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="stage-active-release"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            runner=runner,
            output_path=receipt_path,
            disabled_phase_receipt_path=disabled_path,
            activation_envelope_path=None,
            active_release_path=release_path,
        )
    receipt = subject.validate_phase_receipt(
        receipt_path,
        plan=plan,
        expected_phase="activate",
    )
    assert receipt["rollback_status"] == "rollback_complete"
    rollback = receipt["rollback_process_identity"]
    assert rollback["initial_buy_deadline_identity"] == "B0"
    assert rollback["e3_deadline_imported"] is False
    assert rollback["active_release_path"] == ""
    assert rollback["active_release_file_sha256"] == ""
    assert rollback["active_release_canonical_sha256"] == ""


def test_release_commands_are_deterministic_post_envelope_and_rollback_unsets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        disabled_path,
        _disabled_process,
        envelope_path,
        _release_path,
        release_binding,
    ) = _compatible_activation_context(tmp_path, monkeypatch)
    first = subject._activation_rows_with_active_release(plan, release_binding)
    second = subject._activation_rows_with_active_release(plan, release_binding)
    assert first == second

    plan_text = json.dumps(plan, sort_keys=True)
    envelope_text = envelope_path.read_text(encoding="ascii")
    active_config_text = Path(
        plan["external_tools_and_package"]["files"]["active_config"]["path"]
    ).read_text(encoding="utf-8")
    for digest in (
        release_binding["file_sha256"],
        release_binding["canonical_active_release_sha256"],
    ):
        assert digest not in plan_text
        assert digest not in envelope_text
        assert digest not in active_config_text

    labels = [row["label"] for row in first]
    assert labels.index("capture-old-pid") < labels.index("stage-active-release")
    assert labels.index("stage-active-release") < labels.index("stop-live")
    assert labels.index("install-private-active-release") < labels.index(
        "start-active-restart-only"
    )
    release_transfer = next(row for row in first if row["label"] == "stage-active-release")
    assert release_transfer["argv"][-1].endswith(
        f":{plan['remote']['stage_root']}/active-release-{release_binding['file_sha256']}.json"
    )
    assert "/package-" not in release_transfer["argv"][-1]
    release_freeze = " ".join(
        next(row for row in first if row["label"] == "validate-and-freeze-active-release-stage")[
            "argv"
        ]
    )
    assert "= 600 ||" in release_freeze
    assert "= 400)" in release_freeze
    active_start = " ".join(
        next(row for row in first if row["label"] == "start-active-restart-only")["argv"]
    )
    assert release_binding["remote_path"] in active_start
    assert release_binding["file_sha256"] in active_start
    assert release_binding["canonical_active_release_sha256"] in active_start
    for name in (
        subject.F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
        subject.F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
        subject.F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
    ):
        assert f"{name}=" in active_start
        assert f"-u {name}" not in active_start

    disabled_start = " ".join(
        next(row for row in plan["phases"]["disabled-deploy"] if row["label"] == "start-disabled")[
            "argv"
        ]
    )
    rollback_starts = [
        " ".join(
            next(row for row in plan["phases"][phase] if row["label"] == "start-rollback-fresh-b0")[
                "argv"
            ]
        )
        for phase in ("rollback-primary", "rollback-deep")
    ]
    automatic_rollback_authority_commands = [
        " ".join(row["argv"])
        for row in subject._automatic_rollback_rows(plan)
        if row["label"] in {"stop-live", "start-rollback-fresh-b0"}
    ]
    for command in (
        disabled_start,
        *rollback_starts,
        *automatic_rollback_authority_commands,
    ):
        assert f"-u {subject.F05_BUY_E3_OWNER_OVERRIDE_ENV}" in command
        for name in (
            subject.F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
            subject.F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
            subject.F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
        ):
            assert f"-u {name}" in command
            assert f"{name}=" not in command

    assert _activation_envelope_binding(plan, envelope_path, disabled_path)[
        "canonical_activation_envelope_sha256"
    ]


def test_private_active_release_install_is_0600_single_link_and_no_replace(
    tmp_path: Path,
) -> None:
    source = tmp_path / "stage.json"
    source.write_text('{"release":true}\n', encoding="ascii")
    source.chmod(0o600)
    expected = gate_v2.file_sha256(source)
    destination = tmp_path / "private" / "active-release.json"

    installed = subject.install_private_active_release(
        source_path=source,
        destination_path=destination,
        expected_file_sha256=expected,
    )
    assert installed == {
        "path": str(destination.absolute()),
        "file_sha256": expected,
        "mode": "0600",
        "nlink": 1,
    }
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert destination.stat().st_nlink == 1

    inode_before_retry = destination.stat().st_ino
    subject.install_private_active_release(
        source_path=source,
        destination_path=destination,
        expected_file_sha256=expected,
    )
    assert destination.stat().st_ino == inode_before_retry
    original = destination.read_bytes()
    destination.write_text('{"other":true}\n', encoding="ascii")
    destination.chmod(0o600)
    different = destination.read_bytes()
    assert different != original
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="different bytes"):
        subject.install_private_active_release(
            source_path=source,
            destination_path=destination,
            expected_file_sha256=expected,
        )
    assert destination.read_bytes() == different


def test_private_active_release_content_addressed_slots_coexist(tmp_path: Path) -> None:
    private = tmp_path / "private"
    sources = []
    destinations = []
    for index in (1, 2):
        source = tmp_path / f"stage-{index}.json"
        source.write_text(json.dumps({"release": index}) + "\n", encoding="ascii")
        source.chmod(0o600)
        digest = gate_v2.file_sha256(source)
        destination = private / f"active_release-{digest}.json"
        subject.install_private_active_release(
            source_path=source,
            destination_path=destination,
            expected_file_sha256=digest,
        )
        sources.append(source.read_bytes())
        destinations.append(destination)

    assert destinations[0] != destinations[1]
    assert [path.read_bytes() for path in destinations] == sources
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in destinations)


def test_installed_active_release_process_probe_rebinds_file_and_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan,
        _disabled_path,
        _disabled_process,
        _envelope_path,
        release_path,
        release_binding,
    ) = _compatible_activation_context(tmp_path, monkeypatch)
    payload = subject._validate_installed_active_release_file(
        release_path,
        expected_file_sha256=release_binding["file_sha256"],
        expected_canonical_sha256=release_binding["canonical_active_release_sha256"],
        expected_execution_commit=plan["execution"]["execution_commit"],
        expected_execution_tree=plan["execution"]["execution_tree"],
        expected_artifact_sha256=plan["artifact"]["artifact_sha256"],
        expected_manifest_file_sha256=plan["artifact"]["manifest_file_sha256"],
        expected_policy_file_sha256=plan["artifact"]["policy_file_sha256"],
        expected_predicate_bundle_file_sha256=plan["artifact"][
            "predicate_bundle_file_sha256"
        ],
        expected_active_config_file_sha256=plan["configs"]["active"][
            "config_sha256"
        ],
    )
    assert (
        payload["canonical_active_release_sha256"]
        == release_binding["canonical_active_release_sha256"]
    )

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="file hash drifted"):
        subject._validate_installed_active_release_file(
            release_path,
            expected_file_sha256="0" * 64,
            expected_canonical_sha256=release_binding["canonical_active_release_sha256"],
            expected_execution_commit=plan["execution"]["execution_commit"],
            expected_execution_tree=plan["execution"]["execution_tree"],
            expected_artifact_sha256=plan["artifact"]["artifact_sha256"],
            expected_manifest_file_sha256=plan["artifact"]["manifest_file_sha256"],
            expected_policy_file_sha256=plan["artifact"]["policy_file_sha256"],
            expected_predicate_bundle_file_sha256=plan["artifact"][
                "predicate_bundle_file_sha256"
            ],
            expected_active_config_file_sha256=plan["configs"]["active"][
                "config_sha256"
            ],
        )
    release_path.chmod(0o400)
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="owner-only"):
        subject._validate_installed_active_release_file(
            release_path,
            expected_file_sha256=release_binding["file_sha256"],
            expected_canonical_sha256=release_binding["canonical_active_release_sha256"],
            expected_execution_commit=plan["execution"]["execution_commit"],
            expected_execution_tree=plan["execution"]["execution_tree"],
            expected_artifact_sha256=plan["artifact"]["artifact_sha256"],
            expected_manifest_file_sha256=plan["artifact"]["manifest_file_sha256"],
            expected_policy_file_sha256=plan["artifact"]["policy_file_sha256"],
            expected_predicate_bundle_file_sha256=plan["artifact"][
                "predicate_bundle_file_sha256"
            ],
            expected_active_config_file_sha256=plan["configs"]["active"][
                "config_sha256"
            ],
        )


@pytest.mark.parametrize(
    (
        "expected_enabled",
        "restore_mode",
        "checkpoint_loaded",
        "checkpoint_sequence",
        "identity_kind",
        "remaining_ms",
    ),
    [
        (True, "fresh_b0_no_checkpoint", False, 0, "B0", 0),
        (True, "expired_to_b0", True, 1, "B0", 0),
        (True, "b0_checkpoint_resume", True, 2, "B0", 85_000),
        (True, "exact_same_artifact_resume", True, 3, "BUY_E3", 120_000),
        (True, "artifact_identity_changed_to_b0", True, 4, "B0", 85_000),
        (False, "rollback_to_b0", True, 5, "B0", 85_000),
    ],
)
def test_v5_startup_validator_accepts_admitted_restore_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    expected_enabled: bool,
    restore_mode: str,
    checkpoint_loaded: bool,
    checkpoint_sequence: int,
    identity_kind: str,
    remaining_ms: int,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    release_binding = _synthetic_active_release_binding(plan) if expected_enabled else None
    phase = "activate" if expected_enabled else "disabled-deploy"
    runtime = _runtime_identity_payload(
        plan,
        phase,
        pid=500,
        active_release_binding=release_binding,
    )
    state = runtime["startup_attestation"]["fill_cooldown_state"]
    identity = (
        f"BUY_E3:{plan['artifact']['artifact_sha256']}" if identity_kind == "BUY_E3" else "B0"
    )
    state.update(
        {
            "restore_mode": restore_mode,
            "checkpoint_loaded": checkpoint_loaded,
            "checkpoint_sequence": checkpoint_sequence,
            "buy_deadline_identity": identity,
            "buy_remaining_ms": remaining_ms,
        }
    )

    assert (
        _validate_fixture_startup(
            plan,
            runtime["startup_attestation"],
            expected_enabled=expected_enabled,
            active_release_binding=release_binding,
        )
        == runtime["startup_attestation"]
    )


def test_v5_startup_validator_strictly_rejects_mode_identity_and_release_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    release_binding = _synthetic_active_release_binding(plan)
    active = _runtime_identity_payload(
        plan,
        "activate",
        pid=501,
        active_release_binding=release_binding,
    )["startup_attestation"]

    invalid_active_modes = []
    rollback = copy.deepcopy(active)
    rollback["fill_cooldown_state"].update(
        {
            "restore_mode": "rollback_to_b0",
            "checkpoint_loaded": True,
            "checkpoint_sequence": 1,
            "buy_deadline_identity": "B0",
            "buy_remaining_ms": 85_000,
        }
    )
    invalid_active_modes.append(rollback)
    wrong_artifact = copy.deepcopy(active)
    wrong_artifact["fill_cooldown_state"].update(
        {
            "restore_mode": "exact_same_artifact_resume",
            "checkpoint_loaded": True,
            "checkpoint_sequence": 2,
            "buy_deadline_identity": f"BUY_E3:{'0' * 64}",
            "buy_remaining_ms": 85_000,
        }
    )
    invalid_active_modes.append(wrong_artifact)
    fresh_with_remaining = copy.deepcopy(active)
    fresh_with_remaining["fill_cooldown_state"]["buy_remaining_ms"] = 1
    invalid_active_modes.append(fresh_with_remaining)
    missing_release = copy.deepcopy(active)
    missing_release["buy_e3_active_release"] = subject._empty_active_release_identity()
    invalid_active_modes.append(missing_release)
    for attestation in invalid_active_modes:
        with pytest.raises(subject.BuyE3TransactionalDeployError):
            _validate_fixture_startup(
                plan,
                attestation,
                expected_enabled=True,
                active_release_binding=release_binding,
            )

    disabled = _runtime_identity_payload(
        plan,
        "disabled-deploy",
        pid=502,
    )["startup_attestation"]
    disabled["buy_e3_active_release"] = copy.deepcopy(active["buy_e3_active_release"])
    with pytest.raises(subject.BuyE3TransactionalDeployError, match="retained"):
        _validate_fixture_startup(
            plan,
            disabled,
            expected_enabled=False,
            active_release_binding=None,
        )


@pytest.mark.parametrize(
    "field",
    [
        "active_release_path",
        "active_release_file_sha256",
        "active_release_canonical_sha256",
        "active_release_execution_commit",
        "active_release_execution_tree",
    ],
)
def test_enabled_process_identity_exactly_binds_active_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    release_binding = _synthetic_active_release_binding(plan)
    process = json.loads(
        _process_probe(
            plan,
            "activate",
            active_release_binding=release_binding,
        )
    )
    process[field] = "0" * (40 if field.endswith(("commit", "tree")) else 64)
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process,
        "canonical_process_identity_sha256",
    )
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="authority/deadline identity drifted|active release",
    ):
        subject._validate_actual_process_identity(
            process,
            plan=plan,
            phase="activate",
            old_pid=100,
            active_release_binding=release_binding,
        )


def _as_legacy_v3_disabled_receipt(receipt: dict) -> dict:
    legacy = copy.deepcopy(receipt)
    legacy["schema_version"] = subject.LEGACY_RECEIPT_SCHEMA
    legacy.pop("active_release_binding")
    process = legacy["actual_process_identity"]
    for field in subject._PROCESS_IDENTITY_FIELDS - subject._LEGACY_PROCESS_IDENTITY_FIELDS:
        process.pop(field)
    startup_binding = legacy["actual_startup_attestation"]
    startup = startup_binding["startup_attestation"]
    startup["schema_version"] = subject.LEGACY_STARTUP_ATTESTATION_SCHEMA
    startup.pop("buy_e3_active_release")
    startup.pop("shadow_runtime_identity")
    for field in subject._STARTUP_GATE_FIELDS - subject._LEGACY_STARTUP_GATE_FIELDS:
        startup["gates"].pop(field)
    for role in subject._LOADED_RUNTIME_MODULE_ROLES - (
        subject._HISTORICAL_LOADED_RUNTIME_MODULE_ROLES
    ):
        startup["loaded_module_origins"].pop(role)
    startup_sha256 = gate_v2.canonical_sha256(startup)
    process["startup_attestation_sha256"] = startup_sha256
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process,
        "canonical_process_identity_sha256",
    )
    for field in (
        subject._RUNTIME_IDENTITY_BINDING_FIELDS - subject._LEGACY_RUNTIME_IDENTITY_BINDING_FIELDS
    ):
        startup_binding.pop(field)
    startup_binding["process_identity_sha256"] = process["canonical_process_identity_sha256"]
    startup_binding["startup_attestation_sha256"] = startup_sha256
    startup_binding["canonical_runtime_identity_binding_sha256"] = gate_v2.document_sha256(
        startup_binding,
        "canonical_runtime_identity_binding_sha256",
    )
    for result in legacy["results"]:
        if result["label"] == "fresh-disabled-process-probe":
            result["process_identity_sha256"] = process["canonical_process_identity_sha256"]
        elif result["label"] == "read-disabled-runtime-identity":
            result["startup_attestation_sha256"] = startup_sha256
    legacy["canonical_receipt_sha256"] = gate_v2.document_sha256(
        legacy,
        "canonical_receipt_sha256",
    )
    return legacy


def test_historical_v3_disabled_receipt_validator_remains_supported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    disabled_path, receipt, _process = _successful_disabled_receipt(tmp_path, plan)
    legacy = _as_legacy_v3_disabled_receipt(receipt)
    legacy_path = _write_receipt(tmp_path / "historical-v3.json", legacy)

    assert subject.validate_phase_receipt(
        legacy_path,
        plan=plan,
        expected_phase="disabled-deploy",
    ) == json.loads(legacy_path.read_text(encoding="ascii"))
    assert receipt["schema_version"] == subject.RECEIPT_SCHEMA
    assert receipt["active_release_binding"] is None
    assert disabled_path.stat().st_mode & 0o777 == 0o600


def test_historical_v4_receipt_is_explicitly_non_authoritative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    _disabled_path, receipt, _process = _successful_disabled_receipt(tmp_path, plan)
    historical = copy.deepcopy(receipt)
    historical["schema_version"] = subject.HISTORICAL_RECEIPT_SCHEMA
    historical_path = _write_receipt(tmp_path / "historical-v4.json", historical)

    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="historical receipt-v4.*not accepted",
    ):
        subject.validate_phase_receipt(
            historical_path,
            plan=plan,
            expected_phase="disabled-deploy",
        )
