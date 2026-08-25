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
    expected_token = f"{tmp_path.resolve()}{subject.os.sep}"
    monkeypatch.setenv("NARROWGATE_CPP_EXPECT_MODULE_TOKEN", "/hostile/inherited/")

    def fake_run(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        observed["argv"] = argv
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    subject._run_native_parity_smoke(  # noqa: SLF001
        tmp_path,
        expected_module_token=expected_token,
    )

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
    assert observed["kwargs"]["env"]["NARROWGATE_CPP_EXPECT_MODULE_TOKEN"] == expected_token


def test_locked_venv_module_token_requires_exact_commit_directory(
    tmp_path: Path,
) -> None:
    commit = "a" * 40
    expected_venv = tmp_path / f"venv-{commit}"
    expected_venv.mkdir()

    assert (
        subject._locked_venv_module_token(  # noqa: SLF001
            venv=expected_venv,
            commit=commit,
        )
        == f"{expected_venv.resolve()}{subject.os.sep}"
    )

    wrong_venv = tmp_path / "venv-wrong"
    wrong_venv.mkdir()
    with pytest.raises(
        subject.NativeBuildReceiptError,
        match="commit-bound venv",
    ):
        subject._locked_venv_module_token(  # noqa: SLF001
            venv=wrong_venv,
            commit=commit,
        )
