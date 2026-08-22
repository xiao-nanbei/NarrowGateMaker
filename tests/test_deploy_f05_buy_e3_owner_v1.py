from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path

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
    **overrides,
) -> str:
    expected = subject._expected_process_binding(plan, phase)
    cmdline = [expected["python_executable"], "live/main.py", "--config", expected["config_path"]]
    runtime_identity_payload = _runtime_identity_payload(plan, phase, pid=pid)
    runtime_identity_text = _runtime_identity(plan, phase, pid=pid)
    runtime_identity_file_sha256 = hashlib.sha256(
        runtime_identity_text.encode()
    ).hexdigest()
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
        "e3_deadline_imported": False,
    }
    payload.update(overrides)
    payload["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        payload, "canonical_process_identity_sha256"
    )
    return json.dumps(payload)


def _runtime_identity_payload(plan: dict, phase: str, *, pid: int) -> dict:
    expected = subject._expected_process_binding(plan, phase)
    source_hashes = {
        binding["repository_relative_path"]: binding["working_file_sha256"]
        for binding in plan["runtime_sources"]["files"].values()
    }
    for extra_path in (
        "live/ws_handler.py",
        "strategy/boolean_cooldown_live.py",
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
        "runtime_source_manifest_sha256": subject._runtime_source_manifest_sha256(
            source_rows
        ),
        "runtime_source_files": source_rows,
    }
    loaded_roles = {
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
    loaded_module_origins = {
        role: {
            "module_name": module_name,
            "origin_path": f"/remote/repo/{relative_path}",
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
    }
    return {
        "schema_version": subject.RUNTIME_IDENTITY_SCHEMA,
        "recorded_at_utc": "2026-08-22T00:00:00Z",
        "pid": pid,
        "python_executable": expected["python_executable"],
        "config_path": expected["config_path"],
        "config_sha256": expected["config_sha256"],
        "f05_buy_e3_enabled": expected["enabled"],
        "f05_buy_e3_owner_override_effective": expected["enabled"],
        "f05_buy_e3_artifact_sha256": expected["artifact_sha256"],
        "native_runtime": native_runtime,
        "startup_attestation": {
            "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
            "status": "accepted",
            "attested_at_utc": "2026-08-22T00:00:01Z",
            "fill_cooldown_state": {
                "schema_version": subject.FILL_COOLDOWN_STATE_SCHEMA,
                "reset_policy": "fresh_process_b0",
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
            "gates": {
                name: True for name in subject._STARTUP_GATE_FIELDS
            },
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


def _runtime_identity(plan: dict, phase: str, *, pid: int) -> str:
    return json.dumps(
        _runtime_identity_payload(plan, phase, pid=pid),
        indent=2,
        sort_keys=True,
    ) + "\n"


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
    manifest.write_text("{}\n", encoding="ascii")
    policy.write_text(
        json.dumps({"bindings": {"owner_execution_commit": gate_v2.FROZEN_EXECUTION_COMMIT}})
        + "\n",
        encoding="ascii",
    )
    bundle.write_text("{}\n", encoding="ascii")
    config_strategy = {
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
            "deep_predecessor": _rollback("deep", "a" * 40),
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
            name: dict(identity)
            for name, identity in spec["rollback_identities"].items()
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
        plan["activation_gate_receipt_sha256"] = activation[
            "canonical_receipt_sha256"
        ]
    plan["canonical_plan_sha256"] = gate_v2.document_sha256(plan, "canonical_plan_sha256")


def _successful_runner(
    plan: dict,
    phase: str,
    *,
    disabled_process: dict | None = None,
):
    commands: list[str] = []
    disabled_process = disabled_process or json.loads(
        _process_probe(plan, "disabled-deploy", pid=101)
    )
    main_rows = plan["phases"][phase]
    main_index = 0
    fallback_labels = {
        tuple(row["argv"]): row["label"]
        for rows in plan["phases"].values()
        for row in rows
    }
    disabled_runtime = _runtime_identity(
        plan, "disabled-deploy", pid=disabled_process["pid"]
    )
    active_runtime = _runtime_identity(plan, "activate", pid=202)

    def run(command):
        nonlocal main_index
        commands.append(" ".join(command))
        if (
            main_index < len(main_rows)
            and tuple(command) == tuple(main_rows[main_index]["argv"])
        ):
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
            output = _process_probe(plan, "activate", pid=202)
        elif label == "fresh-disabled-process-probe":
            output = json.dumps(disabled_process)
        elif label == "fresh-rollback-process-probe":
            output = _process_probe(plan, "rollback-primary", pid=303)
        else:
            output = "ok"
        return subprocess.CompletedProcess(command, 0, output, "")

    return run, commands


def _successful_disabled_receipt(
    tmp_path: Path, plan: dict
) -> tuple[Path, dict, dict]:
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


def test_deploy_attestation_schema_matches_runtime_writer() -> None:
    from live import main as live_main

    empty_attestation = live_main._empty_startup_attestation()
    assert subject.STARTUP_ATTESTATION_SCHEMA == live_main.STARTUP_ATTESTATION_SCHEMA
    assert subject.RUNNING_CHECKOUT_SCHEMA == live_main.RUNNING_CHECKOUT_SCHEMA
    assert subject._STARTUP_GATE_FIELDS == frozenset(
        live_main.STARTUP_ATTESTATION_GATE_NAMES
    )
    assert subject._LOADED_RUNTIME_MODULE_ROLES == frozenset(
        live_main.KEY_LOADED_RUNTIME_MODULES
    )
    assert set(empty_attestation) == subject._STARTUP_ATTESTATION_FIELDS


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

    runtime = _runtime_identity_payload(plan, "disabled-deploy", pid=pid)
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
        expected_runtime_code_sha256=plan["runtime_sources"][
            "runtime_code_sha256"
        ],
        artifact_manifest_path=artifact_manifest_path,
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
    assert plan["runtime_attestation_contract"]["remote_path"] == plan["remote"][
        "runtime_identity_path"
    ]
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
    starts = [
        " ".join(row["argv"])
        for rows in plan["phases"].values()
        for row in rows
        if row["label"].startswith("start-")
    ]
    assert starts
    assert all("NARROWGATE_LIVE_CONFIG=" in command for command in starts)
    disabled_labels = [row["label"] for row in plan["phases"]["disabled-deploy"]]
    assert disabled_labels.index("fresh-disabled-process-probe") < disabled_labels.index(
        "read-disabled-runtime-identity"
    )
    active_labels = [row["label"] for row in plan["phases"]["activate"]]
    assert active_labels.index("reprobe-disabled-process-before-stop") < active_labels.index(
        "read-pre-stop-disabled-runtime-identity"
    ) < active_labels.index("stop-live")
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
    extra["canonical_plan_sha256"] = gate_v2.document_sha256(
        extra, "canonical_plan_sha256"
    )
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
    tampered["activation_gate"]["canonical_activation_binding_sha256"] = (
        gate_v2.document_sha256(
            tampered["activation_gate"], "canonical_activation_binding_sha256"
        )
    )
    tampered["canonical_plan_sha256"] = gate_v2.document_sha256(
        tampered, "canonical_plan_sha256"
    )
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
                    remote_spool_allowlisted_roots=lifecycle[
                        "remote_spool_allowlisted_roots"
                    ],
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
        if (
            "bash live/run.sh start" in joined
            and "disabled.yaml" in joined
            and not failed_at
        ):
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
    assert subject.validate_phase_receipt(
        receipt, plan=plan, expected_phase="disabled-deploy"
    ) == payload


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
    payload = subject.validate_phase_receipt(
        output, plan=plan, expected_phase="disabled-deploy"
    )
    assert payload["mutation_started"] is True
    assert payload["rollback_attempted"] is True
    assert not any(
        row["label"] == "stop-live" for row in payload["results"] if not row["label"].startswith("automatic-")
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
    payload = subject.validate_phase_receipt(
        output, plan=plan, expected_phase="disabled-deploy"
    )
    assert payload["stop_failure_probe_result"]["label"] == (
        "stop-failure-probe:confirm-quiescent"
    )
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
    with pytest.raises(
        subject.BuyE3TransactionalDeployError, match="receipt_validation_failed"
    ):
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
    assert result["actual_process_identity"]["artifact_sha256"] == plan["artifact"][
        "artifact_sha256"
    ]
    assert result["actual_process_identity"]["runtime_code_sha256"] == plan[
        "runtime_sources"
    ]["runtime_code_sha256"]
    startup = result["actual_startup_attestation"]
    assert startup["authority"] == "runtime_written_startup_attestation"
    assert startup["runtime_identity_file_sha256"] == result[
        "actual_process_identity"
    ]["runtime_identity"]["file_sha256"]
    runtime_result = next(
        row
        for row in result["results"]
        if row["label"] == "read-disabled-runtime-identity"
    )
    assert runtime_result["stdout_sha256"] == startup[
        "runtime_identity_file_sha256"
    ]
    assert subject.validate_phase_receipt(
        receipt, plan=plan, expected_phase="disabled-deploy"
    ) == result
    assert not any(
        "sighup" in command.lower() or " reload" in command.lower() for command in commands
    )


def test_successful_activation_receipt_embeds_exact_fresh_process_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    disabled_path, _disabled_receipt, disabled_process = _successful_disabled_receipt(
        tmp_path, plan
    )
    fake_runner, _commands = _successful_runner(
        plan, "activate", disabled_process=disabled_process
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
    assert result["evidence_authority"] == subject.RECEIPT_AUTHORITY
    assert result["evidence_authority"]["standalone_activation_evidence"] is False
    assert result["actual_startup_attestation"]["authority"] == (
        "runtime_written_startup_attestation"
    )
    assert result["pre_stop_disabled_startup_attestation"][
        "runtime_identity_file_sha256"
    ] == result["disabled_phase_receipt_binding"][
        "runtime_identity_file_sha256"
    ]
    assert result["actual_startup_attestation"]["startup_attestation"][
        "fill_cooldown_state"
    ]["buy_deadline_identity"] == "B0"
    assert result["disabled_phase_receipt_binding"]["plan_sha256"] == plan[
        "canonical_plan_sha256"
    ]
    assert subject.validate_phase_receipt(
        receipt, plan=plan, expected_phase="activate"
    ) == result


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
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    disabled_path, _receipt, disabled_process = _successful_disabled_receipt(tmp_path, plan)
    base_runner, calls = _successful_runner(
        plan, "activate", disabled_process=disabled_process
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
        )
    payload = subject.validate_phase_receipt(
        output, plan=plan, expected_phase="activate"
    )
    assert payload["status"] == subject.PHASE_FAILED_CLOSED
    assert payload["failure_class"] == "disabled_process_handoff_mismatch"
    assert payload["rollback_attempted"] is True
    stop_index = next(
        (index for index, command in enumerate(calls) if "bash live/run.sh stop" in command),
        len(calls),
    )
    reprobe_index = next(
        index for index, command in enumerate(calls) if "process-probe" in command
    )
    assert reprobe_index < stop_index


def test_activation_fails_closed_on_tampered_runtime_identity_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    disabled_path, _receipt, disabled_process = _successful_disabled_receipt(tmp_path, plan)
    base_runner, _calls = _successful_runner(
        plan, "activate", disabled_process=disabled_process
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
            forged = _runtime_identity_payload(
                plan, "disabled-deploy", pid=disabled_process["pid"]
            )
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
        )
    payload = subject.validate_phase_receipt(
        output, plan=plan, expected_phase="activate"
    )
    assert payload["failure_class"] == "runtime_identity_invalid"
    assert payload["rollback_attempted"] is True
    assert payload["pre_stop_disabled_startup_attestation"] is None


def test_capture_runtime_process_probe_binds_runtime_written_file_and_attestation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    process, runtime_text, pid_start_ticks = _capture_runtime_probe_fixture(
        tmp_path, monkeypatch
    )
    runtime = json.loads(runtime_text)
    expected_file_sha256 = hashlib.sha256(runtime_text.encode()).hexdigest()
    expected_attestation_sha256 = gate_v2.canonical_sha256(
        runtime["startup_attestation"]
    )

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


@pytest.mark.parametrize(
    "runtime_mutation",
    [
        "missing",
        "v1-schema",
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
        elif runtime_mutation == "expected-echo":
            runtime["startup_attestation"] = {
                "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
                "status": "accepted",
                "artifact_sha256": runtime["f05_buy_e3_artifact_sha256"],
                "buy_deadline_identity": "B0",
                "buy_remaining_ms": 0,
            }
        elif runtime_mutation == "forged-deadline":
            runtime["startup_attestation"]["fill_cooldown_state"][
                "buy_deadline_identity"
            ] = "BUY_E3"
        elif runtime_mutation == "forged-source":
            runtime["startup_attestation"]["running_checkout"][
                "runtime_source_files"
            ][0]["working_file_sha256"] = "0" * 64
        elif runtime_mutation == "forged-interpreter":
            runtime["startup_attestation"]["interpreter_identity"]["after"][
                "sha256"
            ] = "0" * 64
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
    process["runtime_identity"]["file_sha256"] = hashlib.sha256(
        forged_echo.encode()
    ).hexdigest()
    process["runtime_identity_file_sha256"] = process["runtime_identity"][
        "file_sha256"
    ]
    process["startup_attestation_sha256"] = gate_v2.canonical_sha256(
        json.loads(forged_echo)
    )
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
        subject.validate_phase_receipt(
            tampered, plan=plan, expected_phase="disabled-deploy"
        )


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
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    disabled_path, _disabled_receipt, disabled_process = _successful_disabled_receipt(
        tmp_path, plan
    )
    runner, _commands = _successful_runner(
        plan, "activate", disabled_process=disabled_process
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
    process_result["process_identity_sha256"] = process[
        "canonical_process_identity_sha256"
    ]
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
