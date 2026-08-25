from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_live_safety_native_build_receipt as subject


def test_native_parity_smoke_forces_no_bytecode_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    def fake_run(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        observed["argv"] = argv
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    subject._run_native_parity_smoke(tmp_path)  # noqa: SLF001

    assert observed["argv"] == (
        sys.executable,
        "-B",
        "-m",
        "pytest",
        "-q",
        *subject.PARITY_TESTS,
    )
    assert observed["kwargs"]["cwd"] == tmp_path
    assert observed["kwargs"]["env"]["PYTHONDONTWRITEBYTECODE"] == "1"
    assert observed["kwargs"]["env"]["PYTHONNOUSERSITE"] == "1"
