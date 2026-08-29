#!/usr/bin/env python3
"""Freeze an exact Linux native build after ABI and parity checks.

The receipt binds software artifacts only. Host, account, strategy-arm, and
release identities are private deployment inputs and are intentionally absent.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.machinery
import json
import os
import platform
import stat
import subprocess
import sys
import sysconfig
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# The target environment is required to remain bytecode-free.  Set this before
# importing any repository or installed build dependency; the deployment
# launcher also supplies PYTHONDONTWRITEBYTECODE=1 before interpreter startup.
sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live import deployment_runtime as locked_runtime  # noqa: E402

SCHEMA = "narrowgate_linux_x86_64_native_build_receipt.v2"
STATUS = "exact_tag_native_build_dependency_lock_and_parity_passed"
CANONICAL_FIELD = "canonical_native_build_sha256"
PARITY_TESTS = (
    "tests/test_cpp_quote_core_parity.py",
    "tests/test_cpp_tick_replay_golden_parity.py",
    "tests/test_conditional_p3_cpp_overlay.py",
)


class NativeBuildReceiptError(RuntimeError):
    pass


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical(payload: dict[str, Any]) -> str:
    clone = dict(payload)
    clone.pop(CANONICAL_FIELD, None)
    return _sha(json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args), cwd=root, check=True, capture_output=True, text=True, timeout=20.0
    ).stdout.strip()


def _file(path: Path) -> dict[str, Any]:
    target = path.expanduser().resolve(strict=True)
    raw = target.read_bytes()
    return {"path": str(target), "sha256": _sha(raw), "size_bytes": len(raw)}


def _locked_venv_module_token(*, venv: Path, commit: str) -> str:
    resolved = venv.expanduser().resolve(strict=True)
    if (
        len(commit) != 40
        or any(character not in "0123456789abcdef" for character in commit)
        or resolved.name != f"venv-{commit}"
    ):
        raise NativeBuildReceiptError("native module interpreter is outside the commit-bound venv")
    return f"{resolved}{os.sep}"


def _run_native_parity_smoke(
    root: Path,
    *,
    expected_module_token: str,
) -> None:
    if (
        not expected_module_token
        or "\x00" in expected_module_token
        or not expected_module_token.endswith(os.sep)
        or not Path(expected_module_token).is_absolute()
    ):
        raise NativeBuildReceiptError("native parity module token is invalid")
    parity_environment = dict(os.environ)
    parity_environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "NARROWGATE_CPP_EXPECT_MODULE_TOKEN": expected_module_token,
        }
    )
    completed = subprocess.run(
        (sys.executable, "-B", "-m", "pytest", "-q", *PARITY_TESTS),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=300.0,
        env=parity_environment,
    )
    if completed.returncode != 0:
        raise NativeBuildReceiptError("native parity smoke failed")


def _locked_runtime_authority(
    *,
    builder_python: Path,
    runtime_lock_path: Path,
    runtime_lock_sha256: str,
    dependency_wheelhouse_path: Path,
    dependency_wheelhouse_sha256: str,
    root_wheel_path: Path,
    root_wheel_sha256: str,
    native_wheel_path: Path,
    native_wheel_sha256: str,
    install_receipt_path: Path,
    install_receipt_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        installed = locked_runtime.validate_installed_runtime(
            venv_python=Path(sys.executable),
            pip_runner_python=builder_python,
            receipt_path=install_receipt_path,
            expected_receipt_sha256=install_receipt_sha256,
            lock_path=runtime_lock_path,
            expected_lock_sha256=runtime_lock_sha256,
            wheelhouse_dir=dependency_wheelhouse_path,
            expected_wheelhouse_sha256=dependency_wheelhouse_sha256,
            root_wheel_path=root_wheel_path,
            root_wheel_sha256=root_wheel_sha256,
            native_wheel_path=native_wheel_path,
            native_wheel_sha256=native_wheel_sha256,
        )
    except locked_runtime.LockedRuntimeError as exc:
        raise NativeBuildReceiptError(f"locked Python runtime validation failed: {exc}") from exc
    lock = _file(runtime_lock_path)
    manifest = _file(dependency_wheelhouse_path / locked_runtime.WHEELHOUSE_MANIFEST)
    install = _file(install_receipt_path)
    root_wheel = _file(root_wheel_path)
    native_wheel = _file(native_wheel_path)
    dependency_lock = {
        "runtime_lock_path": lock["path"],
        "runtime_lock_sha256": runtime_lock_sha256,
        "wheelhouse_path": str(dependency_wheelhouse_path.expanduser().resolve(strict=True)),
        "wheelhouse_manifest_path": manifest["path"],
        "wheelhouse_sha256": dependency_wheelhouse_sha256,
    }
    installed_distribution_lock = {
        "install_receipt_path": install["path"],
        "install_receipt_sha256": install_receipt_sha256,
        "root_wheel_path": root_wheel["path"],
        "root_wheel_sha256": installed["explicit_wheels"]["root"]["sha256"],
        "native_wheel_path": native_wheel["path"],
        "native_wheel_sha256": installed["explicit_wheels"]["native"]["sha256"],
        "interpreter": dict(installed["interpreter"]),
        "installed_distributions": list(installed["installed_distributions"]),
        "installed_record_aggregate_sha256": installed["installed_record_aggregate_sha256"],
    }
    return dependency_lock, installed_distribution_lock


def build_receipt(
    *,
    repository_root: Path,
    annotated_tag: str,
    wheel_path: Path,
    builder_python: Path,
    runtime_lock_path: Path,
    runtime_lock_sha256: str,
    dependency_wheelhouse_path: Path,
    dependency_wheelhouse_sha256: str,
    root_wheel_path: Path,
    root_wheel_sha256: str,
    install_receipt_path: Path,
    install_receipt_sha256: str,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise NativeBuildReceiptError("native receipt requires Linux x86_64")
    if sys.version_info[:2] != (3, 12):
        raise NativeBuildReceiptError("native receipt requires Python 3.12")
    commit = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")
    tag_object = _git(root, "rev-parse", f"refs/tags/{annotated_tag}")
    peeled = _git(root, "rev-parse", f"refs/tags/{annotated_tag}^{{}}")
    if _git(root, "cat-file", "-t", f"refs/tags/{annotated_tag}") != "tag" or peeled != commit:
        raise NativeBuildReceiptError("annotated tag does not peel to the build checkout")
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise NativeBuildReceiptError("native build checkout is dirty")
    import narrowgate_cpp

    module = Path(str(narrowgate_cpp.__file__)).resolve(strict=True)
    locked_venv = Path(sys.prefix).resolve(strict=True)
    expected_module_token = _locked_venv_module_token(
        venv=locked_venv,
        commit=commit,
    )
    if not any(str(module).endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES):
        raise NativeBuildReceiptError("narrowgate_cpp is not a native extension")
    if not module.is_relative_to(locked_venv):
        raise NativeBuildReceiptError("narrowgate_cpp is outside the isolated venv")
    wheel = wheel_path.expanduser().resolve(strict=True)
    with zipfile.ZipFile(wheel) as archive:
        extension_rows = [
            name
            for name in archive.namelist()
            if any(name.endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES)
        ]
        if len(extension_rows) != 1 or _sha(archive.read(extension_rows[0])) != _sha(
            module.read_bytes()
        ):
            raise NativeBuildReceiptError("installed native module does not match the frozen wheel")
    required = (
        "compute_quote_core_live",
        "compute_live_routing_decision",
        "SignalFeatureEngine",
        "SIGNAL_FEATURE_NAMES",
        "TradeBarAggregator",
    )
    if any(not hasattr(narrowgate_cpp, name) for name in required):
        raise NativeBuildReceiptError("native module lacks required live API")
    quote = narrowgate_cpp.QuoteFlags()
    side = narrowgate_cpp.SideQuoteContext()
    if any(
        not hasattr(quote, name) for name in ("delta_cap", "final_compressed", "cap_exposure_block")
    ) or not hasattr(side, "cap_exposure_block"):
        raise NativeBuildReceiptError("native module lacks successor quote ABI fields")
    _run_native_parity_smoke(
        root,
        expected_module_token=expected_module_token,
    )
    try:
        import pybind11

        pybind_version = str(pybind11.__version__)
    except ImportError:
        pybind_version = "not_importable_build_dependency"
    dependency_lock, installed_distribution_lock = _locked_runtime_authority(
        builder_python=builder_python,
        runtime_lock_path=runtime_lock_path,
        runtime_lock_sha256=runtime_lock_sha256,
        dependency_wheelhouse_path=dependency_wheelhouse_path,
        dependency_wheelhouse_sha256=dependency_wheelhouse_sha256,
        root_wheel_path=root_wheel_path,
        root_wheel_sha256=root_wheel_sha256,
        native_wheel_path=wheel,
        native_wheel_sha256=_sha(wheel.read_bytes()),
        install_receipt_path=install_receipt_path,
        install_receipt_sha256=install_receipt_sha256,
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "generated_utc": generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "execution": {
            "execution_commit": commit,
            "execution_tree": tree,
            "annotated_operational_tag": annotated_tag,
            "annotated_operational_tag_object": tag_object,
            "tag_peeled_commit": peeled,
        },
        "platform": "linux_x86_64",
        "python_minor": "3.12",
        "python": {**_file(Path(sys.executable)), "version": platform.python_version()},
        "soabi": str(sysconfig.get_config_var("SOABI")),
        "compiler": platform.python_compiler(),
        "pybind11_version": pybind_version,
        "dependency_lock": dependency_lock,
        "installed_distribution_lock": installed_distribution_lock,
        "wheel": _file(wheel),
        "module": _file(module),
        "abi_contract": {
            "schema_version": "narrowgate_native_runtime_abi.v1",
            "required_apis": list(required),
            "required_quote_fields": {
                "QuoteFlags": ["delta_cap", "final_compressed", "cap_exposure_block"],
                "SideQuoteContext": ["cap_exposure_block"],
            },
            "validated": True,
        },
        "parity_tests": list(PARITY_TESTS),
        "parity_smoke_passed": True,
    }
    payload[CANONICAL_FIELD] = _canonical(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--annotated-tag", required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--builder-python", type=Path, required=True)
    parser.add_argument("--runtime-lock", type=Path, required=True)
    parser.add_argument("--runtime-lock-sha256", required=True)
    parser.add_argument("--dependency-wheelhouse", type=Path, required=True)
    parser.add_argument("--dependency-wheelhouse-sha256", required=True)
    parser.add_argument("--root-wheel", type=Path, required=True)
    parser.add_argument("--root-wheel-sha256", required=True)
    parser.add_argument("--install-receipt", type=Path, required=True)
    parser.add_argument("--install-receipt-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = build_receipt(
        repository_root=args.repository_root,
        annotated_tag=args.annotated_tag,
        wheel_path=args.wheel,
        builder_python=args.builder_python,
        runtime_lock_path=args.runtime_lock,
        runtime_lock_sha256=args.runtime_lock_sha256,
        dependency_wheelhouse_path=args.dependency_wheelhouse,
        dependency_wheelhouse_sha256=args.dependency_wheelhouse_sha256,
        root_wheel_path=args.root_wheel,
        root_wheel_sha256=args.root_wheel_sha256,
        install_receipt_path=args.install_receipt,
        install_receipt_sha256=args.install_receipt_sha256,
    )
    output = args.output.expanduser()
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
    if stat.S_IMODE(output.stat().st_mode) != 0o600:
        raise NativeBuildReceiptError("native receipt mode drifted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
