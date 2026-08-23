from __future__ import annotations

import hashlib
import json
import platform
import shlex
import sys
from pathlib import Path

import pytest

from scripts import f05_buy_e3_attempt4_stability_successor as subject
from scripts import f05_buy_e3_stability_receipts as stability


def _record(path: Path, payload: dict, *, inode: int) -> object:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
    return stability._PrivateJsonRecord(  # noqa: SLF001
        path=path,
        payload=payload,
        raw=raw,
        device=1,
        inode=inode,
    )


def _interpreter_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture_records(tmp_path: Path) -> tuple[object, object]:
    shared = Path(sys.executable).resolve(strict=True)
    venv_root = tmp_path / "fixture-venv"
    entrypoint = venv_root / "bin/python"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.symlink_to(shared)
    version = platform.python_version()
    (venv_root / "pyvenv.cfg").write_text(
        "\n".join(
            (
                f"home = {shared.parent}",
                "include-system-site-packages = false",
                f"version = {version}",
                f"executable = {shared}",
                (
                    "command = "
                    f"{shlex.quote(str(shared))} -m venv {shlex.quote(str(venv_root))}"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    file_sha = _interpreter_sha(shared)
    common = {
        "status": "passed",
        "python_file_sha256": file_sha,
        "nodeids": ["tests/test_gate.py::test_gate"],
        "test_files": {"tests/test_gate.py": "1" * 64},
        "runtime_sources": {"scripts/runtime.py": "2" * 64},
    }
    harness = {
        **common,
        "python_executable": str(shared),
        "run_command": [str(shared), "-m", "pytest", "-q"],
    }
    regression = {
        **common,
        "python_executable": str(entrypoint),
        "run_command": [str(entrypoint), "-m", "pytest", "-q"],
    }
    return (
        _record(tmp_path / "harness.json", harness, inode=11),
        _record(tmp_path / "regression.json", regression, inode=12),
    )


def test_interpreter_equivalence_preserves_both_lexical_paths(tmp_path: Path) -> None:
    harness, regression = _fixture_records(tmp_path)

    payload = subject._interpreter_equivalence_payload(harness, regression)  # noqa: SLF001

    lexical = payload["lexical_provenance"]
    assert lexical["durability_harness"]["receipt_python_executable"] != lexical[
        "runtime_regression"
    ]["receipt_python_executable"]
    assert payload["shared_interpreter"]["realpath"] == str(Path(sys.executable).resolve())
    assert payload["venv_identity"]["venv_root"] == str(
        Path(regression.payload["python_executable"]).parent.parent
    )
    assert all(payload["checks"].values())


def test_interpreter_equivalence_rejects_run_command_provenance_drift(
    tmp_path: Path,
) -> None:
    harness, regression = _fixture_records(tmp_path)
    regression.payload["run_command"][0] = harness.payload["python_executable"]

    with pytest.raises(subject.Attempt4SuccessorError, match="lexical provenance"):
        subject._interpreter_equivalence_payload(harness, regression)  # noqa: SLF001


def test_interpreter_equivalence_rejects_recorded_file_hash_drift(tmp_path: Path) -> None:
    harness, regression = _fixture_records(tmp_path)
    regression.payload["python_file_sha256"] = "0" * 64

    with pytest.raises(subject.Attempt4SuccessorError, match="file SHA256"):
        subject._interpreter_equivalence_payload(harness, regression)  # noqa: SLF001


@pytest.mark.parametrize("drift", ("nodeid", "test_source", "runtime_source", "python_sha"))
def test_coverage_successor_preserves_every_nonlexical_alignment_gate(drift: str) -> None:
    harness = {
        "nodeids": ["tests/test_gate.py::test_gate"],
        "test_files": {"tests/test_gate.py": "1" * 64},
        "runtime_sources": {"scripts/runtime.py": "2" * 64},
        "python_file_sha256": "3" * 64,
    }
    regression = {
        "nodeids": list(harness["nodeids"]),
        "test_files": dict(harness["test_files"]),
        "runtime_sources": dict(harness["runtime_sources"]),
        "python_file_sha256": harness["python_file_sha256"],
    }
    if drift == "nodeid":
        regression["nodeids"] = []
    elif drift == "test_source":
        regression["test_files"]["tests/test_gate.py"] = "4" * 64
    elif drift == "runtime_source":
        regression["runtime_sources"]["scripts/runtime.py"] = "5" * 64
    else:
        regression["python_file_sha256"] = "6" * 64

    with pytest.raises(subject.Attempt4SuccessorError, match="not covered"):
        subject._validate_harness_regression_coverage(harness, regression)  # noqa: SLF001


def test_coverage_successor_accepts_only_lexical_path_difference() -> None:
    harness = {
        "nodeids": ["tests/test_gate.py::test_gate"],
        "test_files": {"tests/test_gate.py": "1" * 64},
        "runtime_sources": {"scripts/runtime.py": "2" * 64},
        "python_file_sha256": "3" * 64,
        "python_executable": "/real/python",
    }
    regression = {
        **harness,
        "nodeids": list(harness["nodeids"]),
        "test_files": dict(harness["test_files"]),
        "runtime_sources": dict(harness["runtime_sources"]),
        "python_executable": "/venv/bin/python",
    }

    subject._validate_harness_regression_coverage(harness, regression)  # noqa: SLF001
