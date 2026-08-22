from __future__ import annotations

import json
import os
import stat
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as subject,
)


def _artifact_files(tmp_path: Path) -> tuple[Path, Path, Path]:
    manifest = tmp_path / "artifact_manifest.json"
    policy = tmp_path / "policy.json"
    bundle = tmp_path / "predicate_bundle.json"
    manifest.write_text("{}\n", encoding="ascii")
    policy.write_text("{}\n", encoding="ascii")
    bundle.write_text("{}\n", encoding="ascii")
    return manifest, policy, bundle


def _config(
    path: Path,
    *,
    enabled: bool,
    manifest: Path,
    policy: Path,
    bundle: Path,
) -> None:
    payload = {
        "strategy": {
            "buy_e3_cooldown_policy_enabled": enabled,
            "buy_e3_cooldown_artifact_manifest_path": str(manifest),
            "buy_e3_cooldown_artifact_manifest_sha256": subject.file_sha256(manifest),
            "buy_e3_cooldown_artifact_sha256": "a" * 64,
            "buy_e3_cooldown_policy_path": str(policy),
            "buy_e3_cooldown_policy_sha256": subject.file_sha256(policy),
            "buy_e3_cooldown_predicate_bundle_path": str(bundle),
            "buy_e3_cooldown_predicate_bundle_sha256": subject.file_sha256(bundle),
            "buy_e3_cooldown_ema_warmup_s": 2048.0,
        },
        "risk": {"max_exec_book_visible_age_s": 2.0},
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")


def _process_identity(pid: int, identity: str = "f" * 64) -> dict:
    return {
        "schema_version": subject.PROCESS_IDENTITY_SCHEMA,
        "pid": pid,
        "pid_start_ticks": pid * 10,
        "canonical_process_identity_sha256": identity,
        "config_sha256": "a" * 64,
        "runtime_code_sha256": "b" * 64,
        "artifact_sha256": "c" * 64,
    }


def _resource_sample(**overrides) -> dict:
    payload = {
        "mem_available_mib": 800.0,
        "live_rss_mib": 300.0,
        "benchmark_rss_mib": 150.0,
        "deep_book_buffer": 0,
        "oom_events": 4,
        "swap_in_kib": 12,
        "swap_out_kib": 7,
        "counters": {key: 10 for key in subject.REQUIRED_ZERO_COUNTERS},
    }
    payload.update(overrides)
    return payload


def _benchmark(**callback_overrides) -> dict:
    callback = {
        "observed_live_rate_hz": 100.0,
        "achieved_rate_hz": 205.0,
        "latency_p99_us": 1_900.0,
    }
    callback.update(callback_overrides)
    return {
        "callback_benchmark": callback,
        "decision_benchmark": {"latency_p99_us": 9_000.0},
    }


def _frozen_artifact_files() -> dict:
    return {
        role: {
            "path": f"/remote/repo/live/private/e3/{role}.json",
            "sha256": sha256,
        }
        for role, sha256 in subject.FROZEN_ARTIFACT_FILE_SHA256.items()
    }


def _frozen_runtime_sources() -> dict:
    files = {
        role: {
            "repository_relative_path": subject.REQUIRED_RUNTIME_PATHS[role],
            "artifact_manifest_sha256": sha256,
            "execution_commit_blob_sha256": sha256,
            "working_file_sha256": sha256,
        }
        for role, sha256 in subject.FROZEN_RUNTIME_SOURCE_SHA256.items()
    }
    return {"files": files, "runtime_code_sha256": subject.canonical_sha256(files)}


def _valid_amended_gate_payload() -> dict:
    runtime_sources = _frozen_runtime_sources()
    artifact_files = _frozen_artifact_files()
    artifact_binding = {
        "artifact_sha256": subject.FROZEN_ARTIFACT_SHA256,
        "artifact_files": deepcopy(artifact_files),
    }
    disabled_path = "/remote/repo/live/private/e3/disabled.yaml"
    active_path = "/remote/repo/live/private/e3/active.yaml"
    disabled_sha = "d" * 64
    active_sha = "e" * 64
    config_binding = {
        "disabled": {
            "enabled": False,
            "config_path": disabled_path,
            "config_sha256": disabled_sha,
            "artifact_sha256": subject.FROZEN_ARTIFACT_SHA256,
            "artifact_files": deepcopy(artifact_files),
            "artifact_loaded_with_from_files": True,
        },
        "active": {
            "enabled": True,
            "config_path": active_path,
            "config_sha256": active_sha,
            "artifact_sha256": subject.FROZEN_ARTIFACT_SHA256,
            "artifact_files": deepcopy(artifact_files),
            "artifact_loaded_with_from_files": True,
        },
        "allowlisted_diff": list(subject.EXACT_CONFIG_DIFF),
        "allowlisted_diff_sha256": subject.canonical_sha256(list(subject.EXACT_CONFIG_DIFF)),
        "observed_diff": list(subject.EXACT_CONFIG_DIFF),
    }
    host_binding = {
        "active_pointer_file_sha256": "7" * 64,
        "known_hosts_file_sha256": "8" * 64,
        "host_key_fingerprint": "SHA256:ZmFrZUZpbmdlcnByaW50",
        "repo_root": "/remote/repo",
        "python_executable": "/remote/repo/.venv/bin/python",
        "venv_root": "/remote/repo/.venv",
    }
    cmdline = [
        host_binding["python_executable"],
        "/remote/repo/live/main.py",
        "--config",
        disabled_path,
    ]
    process = {
        "schema_version": subject.PROCESS_IDENTITY_SCHEMA,
        "captured_utc": "2026-08-22T01:02:03Z",
        "pid": 4312,
        "pid_start_ticks": 998877,
        "cmdline": cmdline,
        "cmdline_sha256": subject.canonical_sha256(cmdline),
        "cwd": host_binding["repo_root"],
        "config_path": disabled_path,
        "config_sha256": disabled_sha,
        "python_executable": host_binding["python_executable"],
        "python_binary_resolved": "/usr/bin/python3.12",
        "venv_root": host_binding["venv_root"],
        "runtime_identity": {
            "present": True,
            "path": "/remote/repo/logs/runtime_identity.json",
            "file_sha256": "9" * 64,
            "schema_version": "narrowgate_live_runtime_identity.v1",
        },
        "artifact_sha256": subject.FROZEN_ARTIFACT_SHA256,
        "runtime_code_sha256": runtime_sources["runtime_code_sha256"],
    }
    process["canonical_process_identity_sha256"] = subject.document_sha256(
        process, "canonical_process_identity_sha256"
    )
    startup = {
        "checkpoint_sha256": "1" * 64,
        "segment_sha256": "2" * 64,
        "segment_size_bytes": 2048,
        "required_markers_sha256": "3" * 64,
        "fatal_pattern_counts": {pattern: 0 for pattern in subject.FATAL_STARTUP_PATTERNS},
    }
    resource = {
        "schema_version": subject.RESOURCE_WINDOW_SCHEMA,
        "status": "concurrent_disabled_live_benchmark_passed",
        "sample_count": 3,
        "live_pid": process["pid"],
        "pre_health_sha256": process["canonical_process_identity_sha256"],
        "post_health_sha256": process["canonical_process_identity_sha256"],
        "thresholds": {
            "min_mem_available_mib": subject.MIN_MEM_AVAILABLE_MIB,
            "max_live_rss_mib": subject.MAX_LIVE_RSS_MIB,
            "max_benchmark_rss_mib": subject.MAX_BENCHMARK_RSS_MIB,
            "max_combined_rss_mib": subject.MAX_COMBINED_RSS_MIB,
            "min_achieved_to_observed_rate": subject.MIN_RATE_MULTIPLIER,
            "max_callback_p99_us": subject.MAX_CALLBACK_P99_US,
            "max_decision_p99_us": subject.MAX_DECISION_P99_US,
        },
        "observed": {
            "min_mem_available_mib": 700.0,
            "max_live_rss_mib": 300.0,
            "max_benchmark_rss_mib": 150.0,
            "max_combined_rss_mib": 450.0,
            "achieved_to_observed_rate": 2.05,
            "callback_p99_us": 1_900.0,
            "decision_p99_us": 9_000.0,
        },
        "checks": {name: True for name in subject.RESOURCE_CHECK_NAMES},
        "sample_series_sha256": "4" * 64,
        "economic_values_persisted": False,
        "hypothetical_live_actions_scored": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    resource["canonical_resource_window_sha256"] = subject.document_sha256(
        resource, "canonical_resource_window_sha256"
    )
    rollback = {
        "primary_disabled": {
            "identity": "attempt2-disabled-b0",
            "execution_commit": subject.FROZEN_EXECUTION_COMMIT,
            "execution_tree": subject.FROZEN_EXECUTION_TREE,
            "config_path": disabled_path,
            "config_sha256": disabled_sha,
            "python_executable": host_binding["python_executable"],
            "venv_root": host_binding["venv_root"],
            "runtime_code_sha256": runtime_sources["runtime_code_sha256"],
            "buy_e3_enabled": False,
            "buy_deadline_identity": "B0",
            "imports_e3_deadline": False,
        },
        "deep_predecessor": {
            "identity": "deep-predecessor-b0",
            "execution_commit": "a" * 40,
            "execution_tree": "b" * 40,
            "config_path": "/rollback/repo/live/private/b0.yaml",
            "config_sha256": "5" * 64,
            "python_executable": "/rollback/repo/.venv/bin/python",
            "venv_root": "/rollback/repo/.venv",
            "runtime_code_sha256": "6" * 64,
            "buy_e3_enabled": False,
            "buy_deadline_identity": "B0",
            "imports_e3_deadline": False,
        },
    }
    return subject.build_amended_gate_receipt(
        execution_identity={
            "execution_commit": subject.FROZEN_EXECUTION_COMMIT,
            "execution_tree": subject.FROZEN_EXECUTION_TREE,
            "annotated_tag": subject.FROZEN_EXECUTION_TAG,
            "annotated_tag_object": subject.FROZEN_EXECUTION_TAG_OBJECT,
            "tag_peeled_commit": subject.FROZEN_EXECUTION_COMMIT,
        },
        runtime_sources=runtime_sources,
        artifact_binding=artifact_binding,
        config_binding=config_binding,
        host_binding=host_binding,
        disabled_process_identity=process,
        startup_log_binding=startup,
        resource_window=resource,
        rollback_identities=rollback,
    )


def _recanonicalize_amended_gate(payload: dict) -> None:
    runtime = payload["runtime_sources"]
    runtime["runtime_code_sha256"] = subject.canonical_sha256(runtime["files"])
    configs = payload["config_binding"]
    configs["allowlisted_diff_sha256"] = subject.canonical_sha256(configs["allowlisted_diff"])
    process = payload["disabled_process_identity"]
    process["cmdline_sha256"] = subject.canonical_sha256(process["cmdline"])
    process["canonical_process_identity_sha256"] = subject.document_sha256(
        process, "canonical_process_identity_sha256"
    )
    resource = payload["resource_window"]
    resource["canonical_resource_window_sha256"] = subject.document_sha256(
        resource, "canonical_resource_window_sha256"
    )
    payload["canonical_amendment_receipt_sha256"] = subject.document_sha256(
        payload, "canonical_amendment_receipt_sha256"
    )


def _set_nested(payload: dict, path: tuple[str, ...], value) -> None:
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value


def _write_gate_receipt(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "deployment-gate-amendment-v2.json"
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _runtime_regression_fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    repository = tmp_path / "repo"
    repository.mkdir()
    for relative in (*subject.RUNTIME_REGRESSION_TESTS, *subject.RUNTIME_REGRESSION_SOURCES):
        path = repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{relative}\n", encoding="ascii")
    physical_python = tmp_path / "bundled" / "python3.12"
    physical_python.parent.mkdir(parents=True)
    physical_python.write_text("physical interpreter\n", encoding="ascii")
    physical_python.chmod(0o700)
    venv = repository / ".venv"
    lexical_python = venv / "bin" / "python"
    lexical_python.parent.mkdir(parents=True)
    (venv / "bin" / "python3").symlink_to(physical_python)
    lexical_python.symlink_to("python3")
    return repository, venv, lexical_python, physical_python


def _patch_runtime_regression_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subject,
        "verify_execution_git_identity",
        lambda **_: {
            "execution_commit": subject.FROZEN_EXECUTION_COMMIT,
            "execution_tree": subject.FROZEN_EXECUTION_TREE,
            "annotated_tag": subject.FROZEN_EXECUTION_TAG,
            "annotated_tag_object": subject.FROZEN_EXECUTION_TAG_OBJECT,
            "tag_peeled_commit": subject.FROZEN_EXECUTION_COMMIT,
        },
    )

    def fake_git(root: Path, *arguments: str, binary: bool = False):
        assert binary is True
        assert arguments[0] == "show"
        relative = arguments[1].split(":", 1)[1]
        return (root / relative).read_bytes()

    monkeypatch.setattr(subject, "_run_git", fake_git)


def test_private_config_pair_loads_artifact_even_while_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, policy, bundle = _artifact_files(tmp_path)
    disabled = tmp_path / "disabled.yaml"
    active = tmp_path / "active.yaml"
    _config(disabled, enabled=False, manifest=manifest, policy=policy, bundle=bundle)
    _config(active, enabled=True, manifest=manifest, policy=policy, bundle=bundle)
    calls: list[dict] = []

    def fake_from_files(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(subject.LiveBuyE3CooldownPolicy, "from_files", fake_from_files)
    binding = subject.validate_private_config_pair(
        disabled_config_path=disabled,
        active_config_path=active,
        repository_root=tmp_path,
    )

    assert len(calls) == 2
    assert binding["disabled"]["enabled"] is False
    assert binding["active"]["enabled"] is True
    assert binding["observed_diff"] == ["strategy.buy_e3_cooldown_policy_enabled"]


def test_private_config_pair_rejects_any_unallowlisted_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, policy, bundle = _artifact_files(tmp_path)
    disabled = tmp_path / "disabled.yaml"
    active = tmp_path / "active.yaml"
    _config(disabled, enabled=False, manifest=manifest, policy=policy, bundle=bundle)
    _config(active, enabled=True, manifest=manifest, policy=policy, bundle=bundle)
    payload = yaml.safe_load(active.read_text(encoding="utf-8"))
    payload["risk"]["max_exec_book_visible_age_s"] = 3.0
    active.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
    monkeypatch.setattr(subject.LiveBuyE3CooldownPolicy, "from_files", lambda **_: object())

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="allowlisted"):
        subject.validate_private_config_pair(
            disabled_config_path=disabled,
            active_config_path=active,
            repository_root=tmp_path,
        )


def test_atomic_receipt_is_immutable_and_mode_0600(tmp_path: Path) -> None:
    path = tmp_path / "private" / "receipt.json"
    subject.atomic_write_receipt(path, {"status": "ok"})
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="already exists"):
        subject.atomic_write_receipt(path, {"status": "different"})


def test_atomic_receipt_rejects_dangling_symlink(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    redirected = tmp_path / "redirected.json"
    path.symlink_to(redirected)
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="already exists"):
        subject.atomic_write_receipt(path, {"status": "unsafe"})
    assert not redirected.exists()


def test_startup_log_checkpoint_rejects_rotation_and_post_checkpoint_fatal(
    tmp_path: Path,
) -> None:
    log = tmp_path / "maker.log"
    log.write_text("old startup text\n", encoding="utf-8")
    checkpoint = subject.capture_startup_log_checkpoint(log)
    with log.open("a", encoding="utf-8") as handle:
        handle.write("STARTED\nTraceback (most recent call last)\nHEALTHY\n")
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="fatal marker"):
        subject.validate_startup_log_after_checkpoint(
            log_path=log,
            checkpoint=checkpoint,
            required_markers=("STARTED", "HEALTHY"),
        )

    replacement = tmp_path / "replacement.log"
    replacement.write_text("STARTED\nHEALTHY\n", encoding="utf-8")
    os.replace(replacement, log)
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="rotated"):
        subject.validate_startup_log_after_checkpoint(
            log_path=log,
            checkpoint=checkpoint,
            required_markers=("STARTED", "HEALTHY"),
        )


def test_actual_process_identity_binds_cmdline_cwd_config_python_and_venv(
    tmp_path: Path,
) -> None:
    pid = 4312
    repository = tmp_path / "repo"
    repository.mkdir()
    config = repository / "private.yaml"
    config.write_text("strategy: {}\n", encoding="ascii")
    venv = repository / ".venv"
    python = venv / "bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("fake executable", encoding="ascii")
    proc = tmp_path / "proc"
    process = proc / str(pid)
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(
        (f"{python}\0{repository / 'live/main.py'}\0--config\0{config}\0").encode()
    )
    (process / "stat").write_text(" ".join([str(pid)] * 22), encoding="ascii")
    (process / "cwd").symlink_to(repository, target_is_directory=True)
    (process / "exe").symlink_to(python)

    identity = subject.capture_actual_process_identity(
        pid=pid,
        expected_repository_root=repository,
        expected_config_path=config,
        expected_config_sha256=subject.file_sha256(config),
        expected_python_executable=python,
        expected_venv_root=venv,
        proc_root=proc,
    )

    assert identity["pid"] == pid
    assert identity["cwd"] == str(repository)
    assert identity["config_path"] == str(config)
    assert identity["venv_root"] == str(venv)


def test_fresh_pid_rejects_pid_reuse() -> None:
    before = _process_identity(100)
    after = _process_identity(100, "e" * 64)
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="reused"):
        subject.require_fresh_pid(before, after)


def test_concurrent_resource_gate_requires_true_two_x_and_same_pid() -> None:
    pre = _process_identity(100)
    post = dict(pre)
    samples = [_resource_sample(), _resource_sample()]
    receipt = subject.validate_concurrent_resource_evidence(
        samples=samples,
        benchmark_receipt=_benchmark(),
        pre_health=pre,
        post_health=post,
    )
    assert receipt["checks"]["true_2x_observed_rate"] is True
    assert receipt["observed"]["achieved_to_observed_rate"] == pytest.approx(2.05)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="true_2x"):
        subject.validate_concurrent_resource_evidence(
            samples=samples,
            benchmark_receipt=_benchmark(achieved_rate_hz=199.99),
            pre_health=pre,
            post_health=post,
        )
    changed = dict(post, pid=101)
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="same-PID"):
        subject.validate_concurrent_resource_evidence(
            samples=samples,
            benchmark_receipt=_benchmark(),
            pre_health=pre,
            post_health=changed,
        )


@pytest.mark.parametrize(
    ("override", "failure"),
    (
        ({"mem_available_mib": 511.99}, "min_mem"),
        ({"live_rss_mib": 512.01}, "live_rss"),
        ({"benchmark_rss_mib": 256.01}, "benchmark_rss"),
        ({"live_rss_mib": 512.0, "benchmark_rss_mib": 256.01}, "combined_rss"),
        ({"deep_book_buffer": 1}, "deep_book_buffer"),
        ({"oom_events": 5}, "no_oom"),
        ({"swap_in_kib": 13}, "no_swap"),
    ),
)
def test_concurrent_resource_gate_rejects_each_hard_limit(override: dict, failure: str) -> None:
    pre = _process_identity(100)
    post = dict(pre)
    samples = [_resource_sample(), _resource_sample(**override)]
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match=failure):
        subject.validate_concurrent_resource_evidence(
            samples=samples,
            benchmark_receipt=_benchmark(),
            pre_health=pre,
            post_health=post,
        )


def test_concurrent_resource_gate_rejects_drop_delta() -> None:
    first = _resource_sample()
    counters = dict(first["counters"])
    counters[subject.REQUIRED_ZERO_COUNTERS[0]] += 1
    second = _resource_sample(counters=counters)
    pre = _process_identity(100)
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="drop_invalid"):
        subject.validate_concurrent_resource_evidence(
            samples=[first, second],
            benchmark_receipt=_benchmark(),
            pre_health=pre,
            post_health=dict(pre),
        )


def test_concurrent_sampler_overlaps_live_and_benchmark_without_real_processes() -> None:
    class FakeBenchmark:
        pid = 200

        def __init__(self) -> None:
            self.polls = 0

        def poll(self):
            self.polls += 1
            return None if self.polls < 3 else 0

        @staticmethod
        def wait() -> int:
            return 0

    pre = _process_identity(100)
    receipt = subject.capture_concurrent_disabled_live_benchmark(
        launch_benchmark=FakeBenchmark,
        sample_provider=lambda pid: _resource_sample(
            benchmark_rss_mib=150.0 if pid == 200 else 0.0
        ),
        benchmark_receipt_provider=_benchmark,
        pre_health=pre,
        post_health_provider=lambda: dict(pre),
        sleep=lambda _seconds: None,
    )
    assert receipt["sample_count"] == 3
    assert receipt["checks"]["concurrent_live_and_benchmark_observed"] is True


def test_amended_gate_requires_two_b0_rollback_identities() -> None:
    payload = deepcopy(_valid_amended_gate_payload())
    del payload["rollback_identities"]["deep_predecessor"]
    _recanonicalize_amended_gate(payload)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="rollback identities"):
        subject.validate_amended_gate_payload(payload)


def test_verify_runtime_sources_binds_working_and_frozen_git_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = {}
    for role, relative in subject.REQUIRED_RUNTIME_PATHS.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"{role}\n".encode())
        expected[role] = subject.file_sha256(path)

    def fake_git(_root, *arguments, binary=False):
        assert arguments[0] == "show"
        relative = arguments[1].split(":", 1)[1]
        return (tmp_path / relative).read_bytes()

    monkeypatch.setattr(subject, "_run_git", fake_git)
    binding = subject.verify_runtime_sources(
        repository_root=tmp_path,
        execution_commit=subject.FROZEN_EXECUTION_COMMIT,
        artifact_manifest={"implementation_sha256": expected},
    )
    assert set(binding["files"]) == set(subject.REQUIRED_RUNTIME_PATHS)
    assert len(binding["runtime_code_sha256"]) == 64


def test_runtime_regression_v2_preserves_lexical_venv_entrypoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, venv, lexical_python, physical_python = _runtime_regression_fixture(tmp_path)
    _patch_runtime_regression_git(monkeypatch)
    commands: list[tuple[str, ...]] = []

    def fake_runner(command, cwd):
        commands.append(tuple(command))
        assert cwd == repository
        if command[0] == str(physical_python):
            return subprocess.CompletedProcess(command, 1, "", "0 tests collected")
        assert command[0] == str(lexical_python)
        return subprocess.CompletedProcess(command, 0, "67 passed in 3.10s\n", "")

    collapsed = fake_runner((str(lexical_python.resolve()), "-m", "pytest", "-q"), repository)
    assert collapsed.returncode == 1
    assert collapsed.stderr == "0 tests collected"
    commands.clear()

    output = tmp_path / "runtime-regression-v2.json"
    receipt = subject.run_runtime_regression_tests_v2(
        repository_root=repository,
        expected_artifact_sha256="a" * 64,
        output_path=output,
        python_executable=lexical_python,
        venv_root=venv,
        expected_python_target=physical_python,
        expected_python_target_sha256=subject.file_sha256(physical_python),
        process_runner=fake_runner,
    )

    assert receipt["status"] == "passed"
    assert receipt["passed"] == 67
    assert commands[0][0] == str(lexical_python)
    assert commands[0][0] != str(physical_python)
    assert receipt["python_identity"]["resolved_target_path"] == str(physical_python)
    assert receipt["python_identity"]["lexical_executable_path"] == str(lexical_python)
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert (
        subject.validate_runtime_regression_receipt_v2(
            output,
            expected_artifact_sha256="a" * 64,
        )["status"]
        == "passed"
    )


def test_runtime_regression_v2_rejects_malicious_symlink_escape(tmp_path: Path) -> None:
    repository, venv, lexical_python, physical_python = _runtime_regression_fixture(tmp_path)
    malicious = tmp_path / "attacker" / "python"
    malicious.parent.mkdir()
    malicious.write_text("malicious interpreter\n", encoding="ascii")
    malicious.chmod(0o700)
    (venv / "bin" / "python3").unlink()
    (venv / "bin" / "python3").symlink_to(malicious)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="target drifted"):
        subject.bind_lexical_venv_python(
            python_executable=lexical_python,
            venv_root=venv,
            expected_resolved_target=physical_python,
            expected_resolved_target_sha256=subject.file_sha256(physical_python),
        )
    assert repository.exists()


def test_runtime_regression_v2_binds_superseded_v1_failed_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, venv, lexical_python, physical_python = _runtime_regression_fixture(tmp_path)
    _patch_runtime_regression_git(monkeypatch)
    v1_receipt = {
        "schema_version": subject.SUPERSEDED_REGRESSION_SCHEMA_V1,
        "identity": subject.OWNER_IDENTITY,
        "status": "failed",
        "artifact_sha256": "a" * 64,
        "execution_commit": subject.FROZEN_EXECUTION_COMMIT,
        "execution_tag": subject.FROZEN_EXECUTION_TAG,
        "passed": 0,
        "failed": 0,
        "return_code": 1,
        "test_files": {path: "b" * 64 for path in subject.RUNTIME_REGRESSION_TESTS},
        "runtime_sources": {path: "c" * 64 for path in subject.RUNTIME_REGRESSION_SOURCES},
    }
    v1_receipt["canonical_receipt_sha256"] = subject.document_sha256(
        v1_receipt, "canonical_receipt_sha256"
    )
    v1_path = tmp_path / "failed-v1.json"
    v1_path.write_text(json.dumps(v1_receipt), encoding="ascii")
    v1_path.chmod(0o600)

    output = tmp_path / "runtime-regression-v2.json"
    receipt = subject.run_runtime_regression_tests_v2(
        repository_root=repository,
        expected_artifact_sha256="a" * 64,
        output_path=output,
        python_executable=lexical_python,
        venv_root=venv,
        expected_python_target=physical_python,
        expected_python_target_sha256=subject.file_sha256(physical_python),
        superseded_v1_failed_receipt_path=v1_path,
        process_runner=lambda command, _cwd: subprocess.CompletedProcess(
            command, 0, "67 passed in 3.10s\n", ""
        ),
    )

    superseded = receipt["superseded_v1_failed_attempt"]
    assert superseded["present"] is True
    assert superseded["role"] == "superseded_failed_attempt_only"
    assert superseded["eligible_for_gate_satisfaction"] is False
    assert superseded["file_sha256"] == subject.file_sha256(v1_path)


def test_runtime_regression_v2_preserves_safe_failure_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, venv, lexical_python, physical_python = _runtime_regression_fixture(tmp_path)
    _patch_runtime_regression_git(monkeypatch)
    output = tmp_path / "failed-v2.json"
    receipt = subject.run_runtime_regression_tests_v2(
        repository_root=repository,
        expected_artifact_sha256="a" * 64,
        output_path=output,
        python_executable=lexical_python,
        venv_root=venv,
        expected_python_target=physical_python,
        expected_python_target_sha256=subject.file_sha256(physical_python),
        process_runner=lambda command, _cwd: subprocess.CompletedProcess(
            command, 1, "sensitive stdout", "sensitive stderr"
        ),
        raise_on_failure=False,
    )

    assert receipt["status"] == "failed"
    assert receipt["failure_reason"] == "pytest_returncode_1_no_tests_completed"
    assert receipt["stdout"]["content_stored"] is False
    assert receipt["stderr"]["content_stored"] is False
    serialized = output.read_text(encoding="ascii")
    assert "sensitive stdout" not in serialized
    assert "sensitive stderr" not in serialized
    assert (
        subject.validate_runtime_regression_receipt_v2(
            output,
            expected_artifact_sha256="a" * 64,
            require_passed=False,
        )["status"]
        == "failed"
    )


def test_amended_gate_payload_revalidates_complete_nested_contract(tmp_path: Path) -> None:
    payload = _valid_amended_gate_payload()
    path = _write_gate_receipt(tmp_path, payload)

    assert subject.validate_amended_gate_payload(payload) == payload
    assert subject.validate_amended_gate_receipt(path) == payload


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("schema_version",), "attacker.v1"),
        (("identity",), "attacker-owner"),
        (("status",), "passed"),
        (("generated_utc",), "not-a-time"),
        (("execution_identity", "execution_commit"), "0" * 40),
        (("execution_identity", "execution_tree"), "0" * 40),
        (("execution_identity", "annotated_tag"), "attacker-tag"),
        (("execution_identity", "annotated_tag_object"), "0" * 40),
        (("execution_identity", "tag_peeled_commit"), "0" * 40),
        (
            (
                "runtime_sources",
                "files",
                "live_main",
                "repository_relative_path",
            ),
            "live/attacker.py",
        ),
        (
            ("runtime_sources", "files", "live_main", "working_file_sha256"),
            "0" * 64,
        ),
        (("artifact_binding", "artifact_sha256"), "0" * 64),
        (
            ("artifact_binding", "artifact_files", "policy", "sha256"),
            "0" * 64,
        ),
        (("config_binding", "disabled", "enabled"), True),
        (("config_binding", "active", "enabled"), False),
        (("config_binding", "active", "artifact_loaded_with_from_files"), False),
        (("config_binding", "observed_diff"), []),
        (("config_binding", "allowlisted_diff"), []),
        (
            ("config_binding", "active", "artifact_files", "manifest", "path"),
            "/remote/repo/live/private/e3/attacker.json",
        ),
        (("config_binding", "active", "config_sha256"), "d" * 64),
        (("host_binding", "active_pointer_file_sha256"), "short"),
        (("host_binding", "known_hosts_file_sha256"), "short"),
        (("host_binding", "host_key_fingerprint"), "MD5:attacker"),
        (("host_binding", "repo_root"), "/remote/other"),
        (("host_binding", "python_executable"), "/usr/bin/python3"),
        (("disabled_process_identity", "pid"), 0),
        (("disabled_process_identity", "pid_start_ticks"), 0),
        (("disabled_process_identity", "cwd"), "/remote/other"),
        (("disabled_process_identity", "config_sha256"), "0" * 64),
        (("disabled_process_identity", "artifact_sha256"), "0" * 64),
        (("disabled_process_identity", "runtime_code_sha256"), "0" * 64),
        (("disabled_process_identity", "runtime_identity", "present"), False),
        (
            ("disabled_process_identity", "runtime_identity", "schema_version"),
            "attacker.v1",
        ),
        (("startup_log_binding", "checkpoint_sha256"), "short"),
        (("startup_log_binding", "segment_sha256"), "short"),
        (("startup_log_binding", "segment_size_bytes"), -1),
        (
            (
                "startup_log_binding",
                "fatal_pattern_counts",
                subject.FATAL_STARTUP_PATTERNS[0],
            ),
            1,
        ),
        (("resource_window", "status"), "passed"),
        (("resource_window", "sample_count"), 1),
        (("resource_window", "live_pid"), 9999),
        (("resource_window", "pre_health_sha256"), "0" * 64),
        (("resource_window", "thresholds", "max_live_rss_mib"), 999.0),
        (("resource_window", "observed", "min_mem_available_mib"), 511.0),
        (("resource_window", "checks", subject.RESOURCE_CHECK_NAMES[0]), False),
        (("resource_window", "hypothetical_live_actions_scored"), True),
        (("rollback_identities", "primary_disabled", "buy_e3_enabled"), True),
        (
            ("rollback_identities", "primary_disabled", "buy_deadline_identity"),
            "E3",
        ),
        (("rollback_identities", "primary_disabled", "imports_e3_deadline"), True),
        (
            ("rollback_identities", "primary_disabled", "runtime_code_sha256"),
            "0" * 64,
        ),
        (
            ("rollback_identities", "deep_predecessor", "identity"),
            "attempt2-disabled-b0",
        ),
        (("activation_contract", "restart_only"), False),
        (("activation_contract", "sighup_allowed"), True),
        (("activation_contract", "fresh_pid_required"), False),
        (("activation_contract", "hypothetical_scorer_allowed"), True),
        (("permissions", "research_authorized"), True),
        (("permissions", "action_authorized"), True),
        (("permissions", "live_authorized"), True),
        (("permissions", "validation_read"), True),
        (("permissions", "sealed_holdout_read"), True),
    ),
)
def test_amended_gate_rejects_self_rehashed_nested_tamper(
    tmp_path: Path, path: tuple[str, ...], value
) -> None:
    payload = deepcopy(_valid_amended_gate_payload())
    _set_nested(payload, path, value)
    _recanonicalize_amended_gate(payload)
    receipt = _write_gate_receipt(tmp_path, payload)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError):
        subject.validate_amended_gate_receipt(receipt)


@pytest.mark.parametrize(
    ("container", "key"),
    (
        (("runtime_sources", "files"), "live_main"),
        (("artifact_binding", "artifact_files"), "policy"),
        (
            ("startup_log_binding", "fatal_pattern_counts"),
            subject.FATAL_STARTUP_PATTERNS[0],
        ),
        (("resource_window", "checks"), subject.RESOURCE_CHECK_NAMES[0]),
        (("rollback_identities",), "deep_predecessor"),
        (("activation_contract",), "restart_only"),
        (("permissions",), "live_authorized"),
    ),
)
def test_amended_gate_rejects_self_rehashed_missing_nested_field(
    tmp_path: Path, container: tuple[str, ...], key: str
) -> None:
    payload = deepcopy(_valid_amended_gate_payload())
    target = payload
    for component in container:
        target = target[component]
    del target[key]
    _recanonicalize_amended_gate(payload)
    receipt = _write_gate_receipt(tmp_path, payload)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError):
        subject.validate_amended_gate_receipt(receipt)


def test_amended_gate_rejects_coordinated_rehashed_runtime_substitution(
    tmp_path: Path,
) -> None:
    payload = deepcopy(_valid_amended_gate_payload())
    substituted_sha = "0" * 64
    binding = payload["runtime_sources"]["files"]["live_main"]
    binding["artifact_manifest_sha256"] = substituted_sha
    binding["execution_commit_blob_sha256"] = substituted_sha
    binding["working_file_sha256"] = substituted_sha
    payload["runtime_sources"]["runtime_code_sha256"] = subject.canonical_sha256(
        payload["runtime_sources"]["files"]
    )
    substituted_runtime_sha = payload["runtime_sources"]["runtime_code_sha256"]
    payload["disabled_process_identity"]["runtime_code_sha256"] = substituted_runtime_sha
    payload["rollback_identities"]["primary_disabled"]["runtime_code_sha256"] = (
        substituted_runtime_sha
    )
    _recanonicalize_amended_gate(payload)
    process_sha = payload["disabled_process_identity"]["canonical_process_identity_sha256"]
    payload["resource_window"]["pre_health_sha256"] = process_sha
    payload["resource_window"]["post_health_sha256"] = process_sha
    _recanonicalize_amended_gate(payload)
    receipt = _write_gate_receipt(tmp_path, payload)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="frozen runtime source"):
        subject.validate_amended_gate_receipt(receipt)


def test_amended_gate_rejects_coordinated_rehashed_artifact_substitution(
    tmp_path: Path,
) -> None:
    payload = deepcopy(_valid_amended_gate_payload())
    substituted_artifact = "0" * 64
    payload["artifact_binding"]["artifact_sha256"] = substituted_artifact
    payload["disabled_process_identity"]["artifact_sha256"] = substituted_artifact
    for name in ("disabled", "active"):
        payload["config_binding"][name]["artifact_sha256"] = substituted_artifact
    _recanonicalize_amended_gate(payload)
    process_sha = payload["disabled_process_identity"]["canonical_process_identity_sha256"]
    payload["resource_window"]["pre_health_sha256"] = process_sha
    payload["resource_window"]["post_health_sha256"] = process_sha
    _recanonicalize_amended_gate(payload)
    receipt = _write_gate_receipt(tmp_path, payload)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="frozen artifact identity"):
        subject.validate_amended_gate_receipt(receipt)


def test_amended_gate_rejects_self_rehashed_unknown_fields(tmp_path: Path) -> None:
    payload = deepcopy(_valid_amended_gate_payload())
    payload["attacker_note"] = "looks harmless"
    _recanonicalize_amended_gate(payload)
    receipt = _write_gate_receipt(tmp_path, payload)

    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="fields drifted"):
        subject.validate_amended_gate_receipt(receipt)
