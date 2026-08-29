from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from live import native_build_receipt as subject


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


def test_locked_runtime_authority_uses_one_root_per_json_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert not hasattr(subject, "NATIVE_SOURCES")
    lock = tmp_path / "runtime-lock.json"
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    wheelhouse_manifest = wheelhouse / subject.locked_runtime.WHEELHOUSE_MANIFEST
    root_wheel = tmp_path / "root.whl"
    native_wheel = tmp_path / "native.whl"
    install_receipt = tmp_path / "install-receipt.json"
    for path, raw in (
        (lock, b"lock\n"),
        (wheelhouse_manifest, b"wheelhouse\n"),
        (root_wheel, b"root-wheel"),
        (native_wheel, b"native-wheel"),
        (install_receipt, b"install\n"),
    ):
        path.write_bytes(raw)
    root_sha256 = subject._sha(root_wheel.read_bytes())  # noqa: SLF001
    native_sha256 = subject._sha(native_wheel.read_bytes())  # noqa: SLF001
    interpreter = {"version": "3.12.0"}
    installed = {
        "explicit_wheels": {
            "root": {"sha256": root_sha256},
            "native": {"sha256": native_sha256},
        },
        "interpreter": interpreter,
        "installed_distributions": [],
        "installed_record_aggregate_sha256": "a" * 64,
    }
    monkeypatch.setattr(
        subject.locked_runtime,
        "validate_installed_runtime",
        lambda **_kwargs: installed,
    )

    dependency, distribution = subject._locked_runtime_authority(  # noqa: SLF001
        builder_python=tmp_path / "builder-python",
        runtime_lock_path=lock,
        runtime_lock_sha256="b" * 64,
        dependency_wheelhouse_path=wheelhouse,
        dependency_wheelhouse_sha256="c" * 64,
        root_wheel_path=root_wheel,
        root_wheel_sha256=root_sha256,
        native_wheel_path=native_wheel,
        native_wheel_sha256=native_sha256,
        install_receipt_path=install_receipt,
        install_receipt_sha256="d" * 64,
    )

    assert dependency == {
        "runtime_lock_path": str(lock.resolve()),
        "runtime_lock_sha256": "b" * 64,
        "wheelhouse_path": str(wheelhouse.resolve()),
        "wheelhouse_manifest_path": str(wheelhouse_manifest.resolve()),
        "wheelhouse_sha256": "c" * 64,
    }
    assert distribution["install_receipt_sha256"] == "d" * 64
    assert not any("file_sha256" in key or "canonical_sha256" in key for key in dependency)
    assert not any("file_sha256" in key or "canonical_sha256" in key for key in distribution)
