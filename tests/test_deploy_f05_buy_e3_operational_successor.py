from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from scripts import deploy_f05_buy_e3_owner_v1 as subject
from strategy import boolean_cooldown_buy_e3 as runtime


def _successor_rows(tmp_path: Path):
    deploy_script = Path(subject.__file__).resolve()
    gate = Path(subject.gate_v2.__file__).resolve()
    local_package: dict[str, str] = {
        "deploy_script": str(deploy_script),
        "gate_amendment": str(gate),
    }
    for role in subject._EXTERNAL_PACKAGE_ROLES:
        if role in local_package:
            continue
        path = tmp_path / f"{role}.json"
        path.write_text(f'{{"role":"{role}"}}\n', encoding="ascii")
        local_package[role] = str(path)
    commit = "1" * 40
    tree = "2" * 40
    release_file_sha256 = "3" * 64
    repo_root = "/srv/narrowgate"
    disabled_sha = "4" * 64
    active_sha = "5" * 64
    artifact_sha = "6" * 64
    runtime_sha = "7" * 64
    locked_source_path = "scripts/f05_live_safety_locked_runtime.py"
    locked_source_sha = subject.gate_v2.file_sha256(
        Path(subject.locked_runtime.__file__).resolve()
    )
    static_source_path = "scripts/f05_live_safety_startup_static_authority.py"
    static_source_sha = subject.gate_v2.file_sha256(
        Path(subject.startup_static_authority.__file__).resolve()
    )
    runtime_sources = {
        "files": {
            locked_source_path: {
                "repository_relative_path": locked_source_path,
                "artifact_manifest_sha256": locked_source_sha,
                "execution_commit_blob_sha256": locked_source_sha,
                "working_file_sha256": locked_source_sha,
            },
            static_source_path: {
                "repository_relative_path": static_source_path,
                "artifact_manifest_sha256": static_source_sha,
                "execution_commit_blob_sha256": static_source_sha,
                "working_file_sha256": static_source_sha,
            },
        },
        "runtime_code_sha256": runtime_sha,
    }
    remote = {
        "stage_root": "/srv/stage",
        "disabled_config_path": f"{repo_root}/live/private/disabled.yaml",
        "active_config_path": f"{repo_root}/live/private/active.yaml",
        "artifact_manifest_path": f"{repo_root}/live/private/artifact.json",
        "policy_path": f"{repo_root}/live/private/policy.json",
        "predicate_bundle_path": f"{repo_root}/live/private/predicates.json",
        "pid_file": f"{repo_root}/logs/maker.pid",
        "supervisor_pid_file": f"{repo_root}/logs/maker.pid",
        "maker_child_pid_file": f"{repo_root}/logs/maker.child.pid",
        "log_path": f"{repo_root}/logs/maker.log",
        "runtime_identity_path": f"{repo_root}/logs/runtime.json",
        "startup_checkpoint_path": f"{repo_root}/logs/checkpoint",
        "startup_markers": sorted(subject.SUCCESSOR_READINESS_MARKERS),
        "safety_release_path": subject._remote_active_release_path(
            repo_root, release_file_sha256
        ),
        "safety_release_file_sha256": release_file_sha256,
        "safety_release_canonical_sha256": "8" * 64,
        "startup_static_runtime_authority_path": (
            f"/srv/stage/startup-static-runtime-authority-{'d' * 64}.json"
        ),
        "startup_static_runtime_authority_file_sha256": "d" * 64,
        "startup_static_runtime_authority_canonical_sha256": "e" * 64,
    }
    execution = {
        "annotated_tag": subject.SUCCESSOR_ANNOTATED_TAG,
        "annotated_tag_object": "9" * 40,
        "execution_commit": commit,
        "execution_tree": tree,
    }
    configs = {
        "disabled": {"config_sha256": disabled_sha},
        "active": {"config_sha256": active_sha},
    }
    artifact = {
        "artifact_sha256": artifact_sha,
        "manifest_file_sha256": "a" * 64,
        "policy_file_sha256": "b" * 64,
        "predicate_bundle_file_sha256": "c" * 64,
    }
    primary = {
        "identity": "successor-b0",
        "execution_commit": commit,
        "execution_tree": tree,
        "config_path": remote["disabled_config_path"],
        "config_sha256": disabled_sha,
        "python_executable": f"{repo_root}/.venv-active/bin/python3",
        "venv_root": f"{repo_root}/.venv-active",
        "runtime_code_sha256": runtime_sha,
        "artifact_sha256": artifact_sha,
        "buy_e3_enabled": False,
        "buy_deadline_identity": "B0",
        "imports_e3_deadline": False,
    }
    phases = subject._phase_commands(
        pointer={"ssh_target": "user@host", "repo_root": repo_root},
        known_hosts={"path": str(tmp_path / "known_hosts")},
        host={
            "python_executable": f"{repo_root}/.venv-active/bin/python3",
            "venv_root": f"{repo_root}/.venv-active",
            "current_venv_selector_target": "/srv/old-venv",
            "trusted_static_python_path": "/usr/bin/python3.12",
            "trusted_static_python_sha256": "0" * 64,
        },
        configs=configs,
        remote=remote,
        execution=execution,
        rollback={
            "primary_disabled": primary,
            "deep_predecessor": {"mode": "stop_cancel_reconcile_only"},
        },
        runtime_sources=runtime_sources,
        artifact=artifact,
        local_package=local_package,
    )
    package_hashes = {
        role: subject.gate_v2.file_sha256(Path(path))
        for role, path in local_package.items()
    }
    plan = {
        "execution": execution,
        "phases": phases,
        "remote": remote,
        "active_pointer": {"ssh_target": "user@host", "repo_root": repo_root},
        "ssh": {"path": str(tmp_path / "known_hosts")},
        "host": {
            "python_executable": f"{repo_root}/.venv-active/bin/python3",
            "venv_root": f"{repo_root}/.venv-active",
            "current_venv_selector_target": "/srv/old-venv",
            "trusted_static_python_path": "/usr/bin/python3.12",
            "trusted_static_python_sha256": "0" * 64,
        },
        "runtime_sources": runtime_sources,
        "configs": configs,
        "external_tools_and_package": {
            "content_package_sha256": subject.gate_v2.canonical_sha256(package_hashes),
            "files": {
                role: {"path": path, "file_sha256": package_hashes[role]}
                for role, path in local_package.items()
            },
        },
    }
    interpreter = {
        "implementation": "cpython",
        "version": "3.12.13",
        "version_info": [3, 12, 13, "final", 0],
        "cache_tag": "cpython-312",
        "soabi": "cpython-312-x86_64-linux-gnu",
        "abiflags": "",
        "sysconfig_platform": "linux-x86_64",
        "system": "Linux",
        "machine": "x86_64",
        "compiler": "GCC 12.2.0",
        "openssl_runtime": "OpenSSL 3.0.0",
        "openssl_version_number": 805306368,
        "executable_sha256": "0" * 64,
        "executable_size_bytes": 1,
        "base_executable_sha256": "1" * 64,
        "base_executable_size_bytes": 1,
        "is_virtual_environment": True,
    }
    binding = {
        "local_path": str(tmp_path / "release.json"),
        "remote_path": remote["safety_release_path"],
        "file_sha256": release_file_sha256,
        "canonical_active_release_sha256": "8" * 64,
        "schema_version": runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
        "status": runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_STATUS,
        "active_config_file_sha256": active_sha,
        "disabled_config_file_sha256": disabled_sha,
        "native_build_receipt_sha256": "d" * 64,
        "native_build_receipt_canonical_sha256": "e" * 64,
        "native_module_sha256": "e" * 64,
        "native_wheel_sha256": "f" * 64,
        "native_soabi": "cpython-312-x86_64-linux-gnu",
        "runtime_lock_file_sha256": "0" * 64,
        "runtime_lock_path": f"/srv/stage/runtime-lock-{commit}.json",
        "runtime_lock_canonical_sha256": "1" * 64,
        "wheelhouse_manifest_file_sha256": "2" * 64,
        "wheelhouse_path": f"/srv/stage/wheelhouse-{'1' * 64}",
        "wheelhouse_canonical_sha256": "3" * 64,
        "install_receipt_path": f"/srv/stage/locked-runtime-install-{commit}.json",
        "install_receipt_file_sha256": "4" * 64,
        "install_receipt_canonical_sha256": "5" * 64,
        "root_wheel_sha256": "6" * 64,
        "root_wheel_path": f"/srv/stage/root-wheel-{commit}/root.whl",
        "native_wheel_path": f"/srv/stage/native-wheel-{commit}/native.whl",
        "installed_record_aggregate_sha256": "7" * 64,
        "locked_runtime_interpreter": interpreter,
    }
    static_payload, static_binding = subject._startup_static_authority_from_release(
        repo_root=repo_root,
        execution=execution,
        runtime_sources=runtime_sources,
        host=plan["host"],
        remote=remote,
        release_binding=binding,
    )
    del static_payload
    remote.update(
        {
            "startup_static_runtime_authority_path": static_binding["remote_path"],
            "startup_static_runtime_authority_file_sha256": static_binding[
                "file_sha256"
            ],
            "startup_static_runtime_authority_canonical_sha256": static_binding[
                "canonical_sha256"
            ],
        }
    )
    phases = subject._phase_commands(
        pointer={"ssh_target": "user@host", "repo_root": repo_root},
        known_hosts={"path": str(tmp_path / "known_hosts")},
        host=plan["host"],
        configs=configs,
        remote=remote,
        execution=execution,
        rollback={
            "primary_disabled": primary,
            "deep_predecessor": {"mode": "stop_cancel_reconcile_only"},
        },
        runtime_sources=runtime_sources,
        artifact=artifact,
        local_package=local_package,
    )
    plan["phases"] = phases
    return phases, plan, binding


def _command_text(row: dict) -> str:
    return " ".join(str(value) for value in row["argv"])


def test_startup_static_authority_hashes_standard_0755_executable(
    tmp_path: Path,
) -> None:
    executable = tmp_path / "python3.12"
    executable.write_bytes(b"frozen executable bytes")
    executable.chmod(0o755)
    assert subject.startup_static_authority._sha256_file(executable) == hashlib.sha256(  # noqa: SLF001
        b"frozen executable bytes"
    ).hexdigest()


def test_startup_static_authority_rejects_ignored_bytecode(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(("/usr/bin/git", "init", "-q", str(repository)), check=True)
    (repository / ".gitignore").write_text("__pycache__/\n*.pyc\n", encoding="ascii")
    cache = repository / "live" / "__pycache__"
    cache.mkdir(parents=True)
    (cache / "main.cpython-312.pyc").write_bytes(b"poisoned-bytecode")

    with pytest.raises(
        subject.startup_static_authority.StartupStaticAuthorityError,
        match="ignored executable import artifact",
    ):
        subject.startup_static_authority._reject_ignored_import_artifacts(  # noqa: SLF001
            repository
        )


def test_remote_start_boundary_drops_exported_shell_functions(
    tmp_path: Path,
) -> None:
    live = tmp_path / "live"
    live.mkdir()
    run = live / "run.sh"
    run.write_text('#!/bin/bash\ndirname "$0"\n', encoding="ascii")
    command = subject._remote_external_config_start(  # noqa: SLF001
        str(tmp_path),
        str(live / "config.yaml"),
        owner_override=False,
    )
    environment = {
        "HOME": str(tmp_path),
        "PATH": "/usr/bin:/bin",
        "BASH_FUNC_dirname%%": "() { printf 'evil-function\\n'; }",
        "BASH_ENV": str(tmp_path / "missing-bash-env"),
        "LD_AUDIT": str(tmp_path / "missing-audit.so"),
    }
    completed = subprocess.run(
        ("/bin/bash", "--noprofile", "--norc", "-c", command),
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert completed.stdout.strip() == str(live)
    assert "evil-function" not in completed.stdout


def test_successor_uses_prebuilt_unified_runtime_and_safety_probe_split(
    tmp_path: Path,
) -> None:
    phases, _plan, _binding = _successor_rows(tmp_path)
    disabled = phases["disabled-deploy"]
    labels = [row["label"] for row in disabled]
    stop_index = labels.index("stop-live")
    validate = disabled[labels.index("validate-prebuilt-successor-runtime")]
    validate_text = _command_text(validate)
    disabled_probe = _command_text(disabled[labels.index("fresh-disabled-process-probe")])

    assert labels.index("validate-prebuilt-successor-runtime") < stop_index
    assert "/srv/stage/venv-" in validate_text
    assert "/runtime-" in validate_text
    assert ".venv-successor" not in validate_text
    assert "pip wheel" not in validate_text
    assert "python3.12 -m pytest" not in validate_text
    assert "--safety-release " in disabled_probe
    assert "--active-release " not in disabled_probe
    assert labels.index("signed-exchange-open-orders-position-reconciliation") < labels.index(
        "checkout-frozen-runtime"
    )
    selector = _command_text(
        disabled[labels.index("validate-current-venv-selector-before-stop")]
    )
    assert "test -L /srv/narrowgate/.venv-active" in selector
    assert 'readlink /srv/narrowgate/.venv-active)\" = /srv/old-venv' in selector
    stop = _command_text(disabled[labels.index("stop-live")])
    assert "{ cp=$(cat " in stop
    assert "true; } || exit 70; stop_rc=0" in stop


def test_successor_release_installs_before_stop_and_active_adds_only_buy_grant(
    tmp_path: Path,
) -> None:
    _phases, plan, binding = _successor_rows(tmp_path)
    rows = subject._activation_rows_with_active_release(
        plan, binding, phase="activate"
    )
    labels = [row["label"] for row in rows]
    assert labels.index("install-private-active-release") < labels.index("stop-live")
    assert labels.index("install-private-active-release") < labels.index(
        "validate-successor-static-runtime-tree-before-target-python"
    )
    assert labels.index(
        "validate-successor-static-runtime-tree-before-target-python"
    ) < labels.index("validate-prebuilt-successor-runtime")
    assert labels.index(
        "install-successor-startup-static-runtime-authority"
    ) < labels.index("validate-successor-static-runtime-tree-before-target-python")
    static_gate = _command_text(
        rows[
            labels.index(
                "validate-successor-static-runtime-tree-before-target-python"
            )
        ]
    )
    static_index = labels.index(
        "validate-successor-static-runtime-tree-before-target-python"
    )
    assert all(
        "/srv/stage/venv-" not in _command_text(row)
        for row in rows[:static_index]
    )
    assert "/usr/bin/python3.12 -I -B -S" in static_gate
    assert "sha256sum /usr/bin/python3.12" in static_gate
    assert "f05_live_safety_startup_static_authority.py" in static_gate
    assert "startup-static-runtime-authority-" in static_gate
    assert "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1" in static_gate
    probe = _command_text(rows[labels.index("fresh-active-process-probe")])
    assert "--safety-release " in probe
    assert "--active-release " in probe
    assert subject._ACTIVE_RELEASE_PROBE_ARGS_PLACEHOLDER not in probe  # noqa: SLF001
    assert "--expected-exchange-reconciliation-path " in probe
    assert ".active.exchange.startup" in probe
    native = _command_text(
        rows[labels.index("validate-successor-native-build-before-stop")]
    )
    assert "/srv/stage/venv-" in native
    assert "/runtime-" not in native.split("/bin/python3", 1)[0].rsplit(" ", 1)[-1]
    assert "test ! -L /srv/stage/venv-" in native
    assert "sha256sum /srv/stage/venv-" in native
    start = _command_text(rows[labels.index("start-active-restart-only")])
    launch = "/bin/bash --noprofile --norc /srv/narrowgate/live/run.sh start"
    assert start.index("exchange-reconcile") < start.index(launch)
    assert '/usr/bin/env -i HOME="$HOME" PATH=/usr/bin:/bin' in start
    assert "bash -lc" not in start
    assert " -I -B " in start
    assert start.index("f05_live_safety_startup_static_authority.py") < start.index(
        "exchange-reconcile"
    )
    assert "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256=" in start
    assert "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256=" in start
    assert "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256=" in start
    for name in (
        "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH",
        "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256",
        "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256",
        "NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH",
        "NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256",
        "NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH",
        "NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256",
    ):
        assert f"{name}=" in start


def test_successor_primary_rollback_reconciles_before_b0_start(
    tmp_path: Path,
) -> None:
    phases, _plan, _binding = _successor_rows(tmp_path)
    rows = phases["rollback-primary"]
    labels = [row["label"] for row in rows]
    assert labels.index("signed-exchange-open-orders-position-reconciliation") < labels.index(
        "start-rollback-fresh-b0"
    )
    assert labels.index("select-rollback-successor-native-venv") < labels.index(
        "start-rollback-fresh-b0"
    )
    start = _command_text(rows[labels.index("start-rollback-fresh-b0")])
    assert "NARROWGATE_LIVE_SAFETY_SUCCESSOR_PATH=" in start
    assert "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH=" in start
    assert "NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY=1" not in start
    rollback_probe = _command_text(
        rows[labels.index("fresh-rollback-process-probe")]
    )
    assert "--expected-exchange-reconciliation-path " in rollback_probe
    assert ".rollback.exchange.startup" in rollback_probe


def test_successor_static_gate_rejects_unfrozen_trusted_python(
    tmp_path: Path,
) -> None:
    _phases, plan, binding = _successor_rows(tmp_path)
    plan["host"]["trusted_static_python_sha256"] = "9" * 64
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="native SOABI drifted",
    ):
        subject._activation_rows_with_active_release(plan, binding, phase="activate")


def test_successor_release_paths_must_live_under_the_frozen_stage_root(
    tmp_path: Path,
) -> None:
    _phases, plan, binding = _successor_rows(tmp_path)
    binding["install_receipt_path"] = "/other/stage/locked-runtime-install.json"
    with pytest.raises(
        subject.BuyE3TransactionalDeployError,
        match="native SOABI drifted",
    ):
        subject._activation_rows_with_active_release(plan, binding, phase="activate")


def test_successor_automatic_rollback_uses_quiescent_branch_only_before_start(
    tmp_path: Path,
) -> None:
    _phases, plan, _binding = _successor_rows(tmp_path)
    before_start = [
        {"label": "confirm-quiescent", "returncode": 0},
        {"label": "checkout-frozen-runtime", "returncode": 1},
    ]
    assert subject._automatic_rollback_from_proven_quiescence(before_start)
    quiescent_rows = subject._automatic_rollback_rows(
        plan, already_quiescent=True
    )
    assert quiescent_rows[0]["label"] == (
        "signed-exchange-open-orders-position-reconciliation"
    )
    assert all(row["label"] != "stop-live" for row in quiescent_rows)

    attempted_start = [
        {"label": "confirm-quiescent", "returncode": 0},
        {"label": "start-disabled", "returncode": 1},
    ]
    assert not subject._automatic_rollback_from_proven_quiescence(attempted_start)
    full_rows = subject._automatic_rollback_rows(plan, already_quiescent=False)
    full_labels = [row["label"] for row in full_rows]
    assert full_labels[:3] == [
        "stop-live",
        "confirm-quiescent",
        "validate-rollback-successor-prerequisites-before-stop",
    ]

    command_by_argv = {tuple(row["argv"]): row["label"] for row in full_rows}

    def runner(argv):
        label = command_by_argv[tuple(argv)]
        return subprocess.CompletedProcess(
            argv,
            1
            if label == "validate-rollback-successor-prerequisites-before-stop"
            else 0,
            stdout="",
            stderr="",
        )

    results: list[dict] = []
    status, failure, process = subject._run_automatic_rollback(
        plan=plan,
        runner=runner,
        results=results,
        old_pid=123,
        already_quiescent=False,
    )
    assert status == "rollback_failed_closed"
    assert failure == "command_returncode_nonzero"
    assert process is None
    assert [result["label"] for result in results] == [
        "automatic-rollback:stop-live",
        "automatic-rollback:confirm-quiescent",
        "automatic-rollback:validate-rollback-successor-prerequisites-before-stop",
    ]

    empty_results: list[dict] = []

    def empty_runner(argv):
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    status, failure, process = subject._run_automatic_rollback(
        plan=plan,
        runner=empty_runner,
        results=empty_results,
        old_pid=123,
        already_quiescent=True,
    )
    assert status == "rollback_failed_closed"
    assert failure == "process_probe_invalid"
    assert process is None
    assert empty_results[-1]["label"] == (
        "automatic-rollback:fresh-rollback-supervisor-child-probe"
    )


def test_successor_automatic_rollback_binds_process_to_family_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _phases, plan, _binding = _successor_rows(tmp_path)
    rows = subject._automatic_rollback_rows(plan, already_quiescent=True)
    labels = {tuple(row["argv"]): row["label"] for row in rows}

    def runner(argv):
        label = labels[tuple(argv)]
        stdout = "10 20 11 21\n" if "supervisor-child-probe" in label else "{}"
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(
        subject,
        "_parse_process_probe",
        lambda *_args, **_kwargs: {
            "pid": 12,
            "pid_start_ticks": 22,
            "canonical_process_identity_sha256": "a" * 64,
        },
    )
    results: list[dict] = []
    status, failure, process = subject._run_automatic_rollback(
        plan=plan,
        runner=runner,
        results=results,
        old_pid=123,
        already_quiescent=True,
    )
    assert status == "rollback_failed_closed"
    assert failure == "process_identity_invalid"
    assert process is not None
