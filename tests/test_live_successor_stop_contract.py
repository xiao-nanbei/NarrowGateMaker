from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_successor_stop_does_not_hide_uncertain_child_or_forced_kill() -> None:
    script = (ROOT / "live" / "run.sh").read_text(encoding="utf-8")
    supervise = script.split("supervise() {", 1)[1].split("\nstart() {", 1)[0]
    service = script.split("service() {", 1)[1].split(
        "\n_launch_manual_supervisor() {", 1
    )[0]
    stop = script.split("stop() {", 1)[1].split("\nrestart() {", 1)[0]
    restart = script.split("restart() {", 1)[1].split("\nstatus() {", 1)[0]

    assert "if [[ $child_exit -eq 0 ]]" in supervise
    assert '_wait_for_supervisor_child "$_supervisor_child_pid"' in supervise
    assert 'return "$child_exit"' in supervise
    assert "STOP_RESULT clean=0" in stop
    assert "kill_escalation" in stop
    assert "_reject_direct_maker_control" in stop
    assert 'SUPERVISOR_STATE_FILE' in stop
    assert 'return "$EXECUTION_STATE_UNCERTAIN_EXIT_CODE"' in stop
    assert 'exec "$PYTHON_BIN" -I -B "$MAIN_PY"' in service
    assert "supervise" not in service
    assert "stop 2>/dev/null || true" not in restart
    assert "\n    stop\n" in restart


def test_supervisor_term_reaps_child_and_records_its_real_clean_exit(
    tmp_path: Path,
) -> None:
    live_dir = tmp_path / "live"
    scripts_dir = tmp_path / "scripts"
    # This fixture exercises supervisor signal/reap mechanics, not the
    # deployment-bound `.venv-active` static-authority contract.
    venv_bin = tmp_path / ".venv" / "bin"
    logs_dir = tmp_path / "logs"
    live_dir.mkdir()
    scripts_dir.mkdir()
    venv_bin.mkdir(parents=True)
    logs_dir.mkdir()
    # Stage a mechanics-only launcher copy. Production start/supervise is
    # deliberately always deployment-authority gated; this test isolates the
    # TERM/wait contract and does not exercise startup authority.
    runner = (ROOT / "live" / "run.sh").read_text(encoding="utf-8")
    runner = runner.replace(
        "LIVE_START_REQUIRES_STATIC_RUNTIME=1",
        "LIVE_START_REQUIRES_STATIC_RUNTIME=0",
        1,
    )
    (live_dir / "run.sh").write_text(runner, encoding="utf-8")
    (live_dir / "run.sh").chmod(0o755)
    child_ready = tmp_path / "child.ready"
    (live_dir / "main.py").write_text(
        "import os\n"
        "import signal\n"
        "import sys\n"
        "import time\n"
        "def stop(_signum, _frame):\n"
        "    time.sleep(0.2)\n"
        "    raise SystemExit(0)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "open(os.environ['NARROWGATE_TEST_CHILD_READY'], 'x').close()\n"
        "while True:\n"
        "    time.sleep(0.05)\n",
        encoding="utf-8",
    )
    (live_dir / "config.yaml").write_text("project_name: test\n", encoding="utf-8")
    (scripts_dir / "preflight_live_deploy.py").write_text(
        'print("{}")\n', encoding="utf-8"
    )
    fake_python = venv_bin / "python3"
    fake_python.symlink_to(sys.executable)

    process = subprocess.Popen(
        ("bash", str(live_dir / "run.sh"), "__supervise"),
        cwd=tmp_path,
        env={
            **os.environ,
            "NARROWGATE_SUPERVISOR_MAX_RESTARTS": "0",
            "NARROWGATE_SUPERVISOR_BACKOFF_S": "1",
            "NARROWGATE_TEST_CHILD_READY": str(child_ready),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        child_pid_file = logs_dir / "maker.child.pid"
        deadline = time.monotonic() + 5.0
        while (
            not child_pid_file.is_file() or not child_ready.is_file()
        ) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert child_pid_file.is_file()
        assert child_ready.is_file()
        child_pid = int(child_pid_file.read_text(encoding="ascii").strip())

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5.0)
        assert process.returncode == 0, (stdout, stderr)
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
        state = dict(
            line.split("=", 1)
            for line in (logs_dir / "maker.supervisor.state")
            .read_text(encoding="utf-8")
            .splitlines()
        )
        assert state["state"] == "stopped_by_operator"
        assert state["last_exit_code"] == "0"
        assert not child_pid_file.exists()
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)
