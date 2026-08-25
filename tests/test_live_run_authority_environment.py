from __future__ import annotations

import os
import signal
import subprocess
import textwrap
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AUTHORITY_ENV = (
    "NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY",
    "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY",
    "NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_PATH",
    "NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256",
    "NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256",
    "NARROWGATE_LIVE_SAFETY_SUCCESSOR_PATH",
    "NARROWGATE_LIVE_SAFETY_SUCCESSOR_FILE_SHA256",
    "NARROWGATE_LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256",
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH",
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256",
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256",
    "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256",
    "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH",
    "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256",
    "NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256",
    "NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH",
    "NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256",
    "NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH",
    "NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256",
)
NORMAL_ENV = (
    "NARROWGATE_TEST_NORMAL_FROM_ENV",
    "NARROWGATE_TEST_NORMAL_FROM_PROFILE",
)
FIXED_RUNTIME_ENV = (
    "PYTHONDONTWRITEBYTECODE",
    "PYTHONNOUSERSITE",
)
SCRUBBED_RUNTIME_ENV = (
    "PYTHONPATH",
    "PYTHONHOME",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "LD_AUDIT",
    "LD_PROFILE",
    "LD_DEBUG",
    "LD_DEBUG_OUTPUT",
    "LD_ORIGIN_PATH",
    "BASH_ENV",
    "ENV",
)


def _write_executable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _stage_run_sh(
    tmp_path: Path, *, require_static_runtime: bool = False
) -> tuple[Path, Path, Path, Path]:
    root = tmp_path / "runner"
    live = root / "live"
    scripts = root / "scripts"
    profile = live / "profiles" / "test.env"
    capture = root / "environment.tsv"
    args_capture = root / "arguments.txt"
    bash_env_payload = root / "malicious-bash-env.sh"

    live.mkdir(parents=True)
    scripts.mkdir(parents=True)
    runner = (ROOT / "live" / "run.sh").read_text(encoding="utf-8")
    if not require_static_runtime:
        runner = runner.replace(
            "LIVE_START_REQUIRES_STATIC_RUNTIME=1",
            "LIVE_START_REQUIRES_STATIC_RUNTIME=0",
            1,
        )
    _write_executable(live / "run.sh", runner)
    (live / "main.py").write_text("# intercepted by the test stub\n", encoding="utf-8")
    (live / "config.yaml").write_text("project_name: test\n", encoding="utf-8")
    (live / "formal_dry_run_public.yaml").write_text(
        "project_name: dry-run-test\n", encoding="utf-8"
    )
    (scripts / "preflight_live_deploy.py").write_text(
        "# intercepted by the test stub\n", encoding="utf-8"
    )
    bash_env_payload.write_text(
        'printf exploited > "$NARROWGATE_TEST_BASH_ENV_MARKER"\n',
        encoding="utf-8",
    )

    stale_env = {name: f"stale-env-authority-{index}" for index, name in enumerate(AUTHORITY_ENV)}
    stale_env.update(
        {
            NORMAL_ENV[0]: "loaded-from-env",
            "NARROWGATE_LIVE_PROFILE_FILE": "/stale/env/profile.env",
            FIXED_RUNTIME_ENV[0]: "0",
            FIXED_RUNTIME_ENV[1]: "0",
            **{name: f"unsafe-env-{name}" for name in SCRUBBED_RUNTIME_ENV},
            "BASH_ENV": str(bash_env_payload),
            "ENV": str(bash_env_payload),
        }
    )
    (live / ".env").write_text(
        "".join(f"{name}={value}\n" for name, value in stale_env.items())
        + "mkdir() { printf exploited > \"$NARROWGATE_TEST_BASH_ENV_MARKER\"; "
        + "/bin/mkdir \"$@\"; }\nexport -f mkdir\n",
        encoding="utf-8",
    )

    stale_profile = {
        name: f"stale-profile-authority-{index}"
        for index, name in enumerate(AUTHORITY_ENV)
    }
    stale_profile[NORMAL_ENV[1]] = "loaded-from-profile"
    stale_profile[FIXED_RUNTIME_ENV[0]] = "profile-disabled-bytecode-guard"
    stale_profile[FIXED_RUNTIME_ENV[1]] = "profile-disabled-user-site-guard"
    profile.parent.mkdir(parents=True)
    profile.write_text(
        "".join(f"{name}={value}\n" for name, value in stale_profile.items()),
        encoding="utf-8",
    )

    python_stub = textwrap.dedent(
        f"""\
        #!/usr/bin/env bash
        set -euo pipefail

        original_args="$*"
        if [[ "${{1:-}}" == "-I" ]]; then
            shift
        fi
        if [[ "${{1:-}}" == "-B" ]]; then
            shift
        fi
        if [[ "${{1:-}}" == "-c" ]]; then
            exit 0
        fi

        state() {{
            local value
            if value="$(printenv "$1" 2>/dev/null)"; then
                printf 'SET:%s' "$value"
            else
                printf 'UNSET'
            fi
        }}

        kind=unknown
        case "${{1:-}}" in
            *preflight_live_deploy.py) kind=preflight ;;
            *live/main.py) kind=main ;;
        esac
        printf '%s' "$kind" >> "$NARROWGATE_TEST_CAPTURE"
        for name in {' '.join(AUTHORITY_ENV)}; do
            printf '\\t%s' "$(state "$name")" >> "$NARROWGATE_TEST_CAPTURE"
        done
        for name in {' '.join(SCRUBBED_RUNTIME_ENV)}; do
            printf '\\t%s' "$(state "$name")" >> "$NARROWGATE_TEST_CAPTURE"
        done
        printf '\\t%s\\t%s\\t%s\\t%s\\n' \\
            "$(state {NORMAL_ENV[0]})" \\
            "$(state {NORMAL_ENV[1]})" \\
            "$(state {FIXED_RUNTIME_ENV[0]})" \\
            "$(state {FIXED_RUNTIME_ENV[1]})" >> "$NARROWGATE_TEST_CAPTURE"
        printf '%s\\n' "$original_args" >> "$NARROWGATE_TEST_ARGS_CAPTURE"

        if [[ "$kind" == "preflight" ]]; then
            printf '{{"status":"test-only"}}\\n'
            exit 0
        fi
        for arg in "$@"; do
            if [[ "$arg" == "--dry-run" ]]; then
                exit 0
            fi
        done

        child=""
        trap '[[ -n "$child" ]] && kill "$child" 2>/dev/null || true; exit 0' TERM INT
        sleep 30 &
        child=$!
        wait "$child"
        """
    )
    _write_executable(root / ".venv" / "bin" / "python3", python_stub)
    _write_executable(root / "test-bin" / "ps", "#!/usr/bin/env bash\nexit 0\n")
    return root, profile, capture, args_capture


def _base_environment(
    root: Path, profile: Path, capture: Path, args_capture: Path
) -> dict[str, str]:
    environment = os.environ.copy()
    for name in (
        *AUTHORITY_ENV,
        *NORMAL_ENV,
        *FIXED_RUNTIME_ENV,
        *SCRUBBED_RUNTIME_ENV,
        "NARROWGATE_LIVE_PROFILE",
        "BASH_ENV",
        "ENV",
    ):
        environment.pop(name, None)
    environment.update(
        {
            "NARROWGATE_LIVE_CONFIG": str(root / "live" / "config.yaml"),
            "NARROWGATE_LIVE_PROFILE_FILE": str(profile),
            "NARROWGATE_TEST_CAPTURE": str(capture),
            "NARROWGATE_TEST_ARGS_CAPTURE": str(args_capture),
            "NARROWGATE_TEST_BASH_ENV_MARKER": str(root / "bash-env-executed"),
            "PATH": f"{root / 'test-bin'}:{environment['PATH']}",
        }
    )
    return environment


def _terminate_staged_main(root: Path) -> None:
    pid_path = root / "logs" / "maker.pid"
    if not pid_path.exists():
        return
    pid = int(pid_path.read_text(encoding="utf-8").strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _read_records(capture: Path) -> dict[str, dict[str, str]]:
    records: dict[str, dict[str, str]] = {}
    fields = (*AUTHORITY_ENV, *SCRUBBED_RUNTIME_ENV, *NORMAL_ENV, *FIXED_RUNTIME_ENV)
    for line in capture.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        assert len(parts) == len(fields) + 1
        records[parts[0]] = dict(zip(fields, parts[1:], strict=True))
    return records


def _run_start(root: Path, environment: dict[str, str], command: list[str]) -> None:
    try:
        subprocess.run(
            command,
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    finally:
        _terminate_staged_main(root)


def test_live_start_fails_closed_when_runtime_selector_is_missing(tmp_path: Path) -> None:
    root, profile, capture, args_capture = _stage_run_sh(
        tmp_path, require_static_runtime=True
    )
    environment = _base_environment(root, profile, capture, args_capture)

    completed = subprocess.run(
        ("bash", str(root / "live" / "run.sh"), "start"),
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode != 0
    assert "Missing deployment-bound startup authority" in completed.stderr
    assert not capture.exists()
    assert not (root / "bash-env-executed").exists()


def test_run_sh_preserves_invocation_only_buy_e3_authority(tmp_path: Path) -> None:
    root, profile, capture, args_capture = _stage_run_sh(tmp_path)
    environment = _base_environment(root, profile, capture, args_capture)
    invocation = {
        name: f"invocation-authority-{index}"
        for index, name in enumerate(AUTHORITY_ENV)
    }
    environment.update(invocation)

    _run_start(root, environment, ["bash", str(root / "live" / "run.sh"), "start"])

    records = _read_records(capture)
    assert set(records) == {"preflight", "main"}
    for record in records.values():
        for name, value in invocation.items():
            assert record[name] == f"SET:{value}"
        assert record[NORMAL_ENV[0]] == "SET:loaded-from-env"
        assert record[NORMAL_ENV[1]] == "SET:loaded-from-profile"
        for name in FIXED_RUNTIME_ENV:
            assert record[name] == "SET:1"
        for name in SCRUBBED_RUNTIME_ENV:
            assert record[name] == "UNSET"
    assert not (root / "bash-env-executed").exists()
    assert all(
        invocation.startswith("-I -B ")
        for invocation in args_capture.read_text(encoding="utf-8").splitlines()
    )


def test_run_sh_rollback_env_unsets_cannot_be_reintroduced(tmp_path: Path) -> None:
    root, profile, capture, args_capture = _stage_run_sh(tmp_path)
    environment = _base_environment(root, profile, capture, args_capture)
    for name in AUTHORITY_ENV:
        environment[name] = "parent-value-that-env-u-must-remove"
    command = ["env"]
    for name in AUTHORITY_ENV:
        command.extend(("-u", name))
    command.extend(("bash", str(root / "live" / "run.sh"), "start"))

    _run_start(root, environment, command)

    records = _read_records(capture)
    assert set(records) == {"preflight", "main"}
    for record in records.values():
        for name in AUTHORITY_ENV:
            assert record[name] == "UNSET"
        assert record[NORMAL_ENV[0]] == "SET:loaded-from-env"
        assert record[NORMAL_ENV[1]] == "SET:loaded-from-profile"
        for name in FIXED_RUNTIME_ENV:
            assert record[name] == "SET:1"
        for name in SCRUBBED_RUNTIME_ENV:
            assert record[name] == "UNSET"
    assert not (root / "bash-env-executed").exists()
    assert all(
        invocation.startswith("-I -B ")
        for invocation in args_capture.read_text(encoding="utf-8").splitlines()
    )


def test_formal_dry_run_does_not_source_live_environment(tmp_path: Path) -> None:
    root, profile, capture, args_capture = _stage_run_sh(tmp_path)
    environment = _base_environment(root, profile, capture, args_capture)
    environment["NARROWGATE_DRY_RUN_TIMEOUT_S"] = "7"

    subprocess.run(
        ["bash", str(root / "live" / "run.sh"), "dry-run"],
        cwd=root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )

    records = _read_records(capture)
    assert set(records) == {"main"}
    for name in (*AUTHORITY_ENV, *NORMAL_ENV):
        assert records["main"][name] == "UNSET"
    for name in FIXED_RUNTIME_ENV:
        assert records["main"][name] == "SET:1"
    for name in SCRUBBED_RUNTIME_ENV:
        assert records["main"][name] == "UNSET"
    args = args_capture.read_text(encoding="utf-8").splitlines()
    assert len(args) == 1
    assert args[0].startswith("-I -B ")
    assert "--dry-run --dry-run-timeout-s 7" in args[0]
    assert str(root / "live" / "config.yaml") in args[0]
