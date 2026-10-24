"""Developer commands select an interpreter, never a stale project venv."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("script", ["x86_live_benchmark.sh", "x86_live_soak.sh"])
@pytest.mark.parametrize("explicit", [False, True])
def test_benchmark_uses_selected_python(tmp_path, script, explicit):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    selected = bin_dir / ("chosen-python" if explicit else "python3")
    capture = tmp_path / "calls"
    selected.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CAPTURE"\n')
    selected.chmod(0o755)
    stale = tmp_path / ".venv/bin/python"
    stale.parent.mkdir(parents=True)
    stale.write_text("#!/bin/sh\nexit 93\n")
    stale.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHON", "TASKSET_CPU", "RUN_PERF")}
    env.update(ROOT=str(tmp_path), CAPTURE=str(capture), PATH=f"{bin_dir}:{env['PATH']}")
    if explicit:
        env["PYTHON"] = "chosen-python"
    result = subprocess.run(["bash", str(ROOT / "scripts" / script)], env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert capture.read_text().strip()


def test_make_does_not_autoselect_old_venv(tmp_path):
    (tmp_path / "Makefile").write_bytes((ROOT / "Makefile").read_bytes())
    old = tmp_path / ".venv/bin/python"
    old.parent.mkdir(parents=True)
    old.touch()
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHON", "MAKEFLAGS")}
    result = subprocess.run(["make", "-n", "native-replay-dev"], cwd=tmp_path, env=env, capture_output=True, text=True, check=True)
    assert result.stdout.startswith("python3 scripts/build_replay_native.py")
    assert ".venv" not in result.stdout
