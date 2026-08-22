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


def _process_probe(pid: int = 101) -> str:
    payload = {
        "schema_version": gate_v2.PROCESS_IDENTITY_SCHEMA,
        "pid": pid,
        "buy_e3_enabled": False,
        "owner_override_effective": False,
        "initial_buy_deadline_identity": "B0",
        "e3_deadline_imported": False,
        "canonical_process_identity_sha256": "",
    }
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


def _plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict, dict]:
    spec = _specification(tmp_path)
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
        if "bash live/run.sh start" in joined and "disabled.yaml" in joined:
            failed_at.append(len(calls))
            return subprocess.CompletedProcess(command, 1, "", "failed")
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
    assert payload["rollback_attempted"] is True
    assert any(row["label"].startswith("automatic-rollback:") for row in payload["results"])
    assert oct(receipt.stat().st_mode & 0o777) == "0o600"


def test_successful_disabled_phase_never_uses_sighup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _spec = _plan(tmp_path, monkeypatch)
    commands: list[str] = []

    def fake_runner(command):
        commands.append(" ".join(command))
        joined = commands[-1]
        if "printf '%s" in joined:
            output = "100\n"
        elif " process-probe " in joined:
            output = _process_probe()
        else:
            output = "ok"
        return subprocess.CompletedProcess(command, 0, output, "")

    result = subject.execute_phase(
        plan=plan,
        phase="disabled-deploy",
        token="token-disabled-deploy",
        authorize_remote_mutation=True,
        runner=fake_runner,
    )
    assert result["status"] == "phase_complete"
    assert result["rollback_attempted"] is False
    assert not any(
        "sighup" in command.lower() or " reload" in command.lower() for command in commands
    )
