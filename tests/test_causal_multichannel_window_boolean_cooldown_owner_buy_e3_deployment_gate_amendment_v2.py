from __future__ import annotations

import json
import os
import stat
import subprocess
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
    resource = {
        "schema_version": subject.RESOURCE_WINDOW_SCHEMA,
        "canonical_resource_window_sha256": "",
    }
    resource["canonical_resource_window_sha256"] = subject.document_sha256(
        resource, "canonical_resource_window_sha256"
    )
    execution = {"execution_commit": subject.FROZEN_EXECUTION_COMMIT}
    with pytest.raises(subject.BuyE3DeploymentGateAmendmentError, match="dual rollback"):
        subject.build_amended_gate_receipt(
            execution_identity=execution,
            runtime_sources={},
            artifact_binding={},
            config_binding={"disabled": {"enabled": False}},
            host_binding={},
            disabled_process_identity={},
            startup_log_binding={},
            resource_window=resource,
            rollback_identities={},
        )


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
