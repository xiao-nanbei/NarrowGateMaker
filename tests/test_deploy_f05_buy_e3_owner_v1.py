from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest
import yaml

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
)
from scripts import deploy_f05_buy_e3_owner_v1 as subject


def _token_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _preflight(enabled: bool) -> dict:
    payload = {
        "schema_version": subject.PREFLIGHT_SCHEMA,
        "status": "isolated_config_preflight_passed",
        "expected_enabled": enabled,
        "artifact_loaded_with_from_files": True,
        "artifact_sha256": "a" * 64,
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
            "file_sha256": "9" * 64,
            "schema_version": "runtime_identity.v1",
        },
        "execution_commit": expected["execution_commit"],
        "execution_tree": expected["execution_tree"],
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
    disabled.write_text(
        yaml.safe_dump({"strategy": config_strategy}, sort_keys=True), encoding="ascii"
    )
    active.write_text(
        yaml.safe_dump({"strategy": config_strategy}, sort_keys=True), encoding="ascii"
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
                "runtime_code_sha256": "b" * 64,
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
        "runtime_sources": {"runtime_code_sha256": "b" * 64},
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
    plan["canonical_plan_sha256"] = gate_v2.document_sha256(plan, "canonical_plan_sha256")


def _successful_runner(plan: dict, phase: str):
    commands: list[str] = []

    def run(command):
        joined = " ".join(command)
        commands.append(joined)
        if "printf '%s" in joined or "cat /remote/repo/logs/maker.pid" in joined:
            output = "100\n"
        elif " process-probe " in joined:
            output = _process_probe(plan, phase)
        else:
            output = "ok"
        return subprocess.CompletedProcess(command, 0, output, "")

    return run, commands


def _set_nested(payload: dict, path: tuple[str, ...], value) -> None:
    cursor = payload
    for field in path[:-1]:
        cursor = cursor[field]
    cursor[path[-1]] = value


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
        lambda **_: {"files": {}, "runtime_code_sha256": "b" * 64},
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
    for phase, rows in plan["phases"].items():
        for row in rows:
            assert row["argv"][0] in {"ssh", "rsync"}
            assert "StrictHostKeyChecking=yes" in " ".join(row["argv"])
            assert "UserKnownHostsFile=" in " ".join(row["argv"])
            assert "reload" not in " ".join(row["argv"]).lower()
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

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="binding is incomplete"):
        subject.execute_phase(
            plan=plan,
            phase="activate",
            token="token-activate",
            authorize_remote_mutation=True,
            output_path=tmp_path / "must-not-exist.json",
        )


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
    fake_runner, _commands = _successful_runner(plan, "activate")
    receipt = tmp_path / "activate-transaction.json"

    result = subject.execute_phase(
        plan=plan,
        phase="activate",
        token="token-activate",
        authorize_remote_mutation=True,
        runner=fake_runner,
        output_path=receipt,
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
    assert subject.validate_phase_receipt(
        receipt, plan=plan, expected_phase="activate"
    ) == result


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
        ("pid", 100),
    ],
)
def test_validate_activation_receipt_rejects_process_artifact_and_deadline_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    replacement,
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch, activation_gate=True)
    runner, _commands = _successful_runner(plan, "activate")
    original = tmp_path / "original.json"
    subject.execute_phase(
        plan=plan,
        phase="activate",
        token="token-activate",
        authorize_remote_mutation=True,
        runner=runner,
        output_path=original,
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

    with pytest.raises(subject.BuyE3TransactionalDeployError, match="process|PID"):
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
