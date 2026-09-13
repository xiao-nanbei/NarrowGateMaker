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
import tempfile
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
from strategy.model_contract import REQUIRED_MODEL_HEADS  # noqa: E402

SCHEMA = "narrowgate_linux_x86_64_native_build_receipt.v3"
STATUS = "exact_tag_native_build_dependency_lock_and_parity_passed"
CANONICAL_FIELD = "canonical_native_build_sha256"
LIVE_PARITY_TESTS = locked_runtime.NATIVE_LIVE_PARITY_TESTS
PRODUCTION_BUILD_FLAVOR = "live"
_QUALIFICATION_REPORT_ENV = "NARROWGATE_NATIVE_QUALIFICATION_REPORT"
PRODUCTION_LIVE_CPU_PROFILE = "ec2-cascadelake-avx2"
PRODUCTION_LIVE_COMPILE_OPTIONS = (
    "-O3",
    "-march=haswell",
    "-mtune=cascadelake",
    "-mprefer-vector-width=256",
    "-fno-fast-math",
    "-ffp-contract=off",
    "-fno-lto",
)


class NativeBuildReceiptError(RuntimeError):
    pass


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    """Emit machine-readable qualification counts for the receipt subprocess."""

    report_path = os.environ.get(_QUALIFICATION_REPORT_ENV, "")
    if not report_path:
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    stats = getattr(reporter, "stats", {}) if reporter is not None else {}
    def count(name: str) -> int:
        return len(stats.get(name, ()))
    payload = {
        "collected": int(session.testscollected),
        "passed": count("passed"),
        "failed": count("failed"),
        "errors": count("error"),
        "skipped": count("skipped"),
        "xfailed": count("xfailed"),
        "xpassed": count("xpassed"),
        "deselected": count("deselected"),
        "exitstatus": int(exitstatus),
    }
    Path(report_path).write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _validate_lightgbm_bundle_abi(module: Any) -> None:
    """Require the one fixed 13-head order used by Python and native inference."""

    if not bool(module.NATIVE_LIGHTGBM_BUNDLE_INFERENCE_AVAILABLE):
        raise NativeBuildReceiptError(
            "native module LightGBM bundle inference is unavailable"
        )
    if tuple(module.LIGHTGBM_BUNDLE_HEAD_NAMES) != tuple(REQUIRED_MODEL_HEADS):
        raise NativeBuildReceiptError(
            "native module LightGBM bundle head order differs from the model contract"
        )


def _production_live_cpu_build(module: Any) -> dict[str, Any]:
    """Read the compiled hot-path profile; a portable wheel is not releasable."""

    try:
        preferred_vector_width_bits = int(
            getattr(module, "NATIVE_LIVE_BUILD_VECTOR_WIDTH_BITS", 0)
        )
    except (TypeError, ValueError) as exc:
        raise NativeBuildReceiptError("native live CPU build metadata is invalid") from exc
    observed = {
        "profile": str(getattr(module, "NATIVE_LIVE_BUILD_PROFILE", "")),
        "compile_options": str(
            getattr(module, "NATIVE_LIVE_BUILD_COMPILE_OPTIONS", "")
        ),
        "production": bool(
            getattr(module, "NATIVE_LIVE_BUILD_IS_PRODUCTION", False)
        ),
        "preferred_vector_width_bits": preferred_vector_width_bits,
    }
    if observed != {
        "profile": PRODUCTION_LIVE_CPU_PROFILE,
        "compile_options": " ".join(PRODUCTION_LIVE_COMPILE_OPTIONS),
        "production": True,
        "preferred_vector_width_bits": 256,
    }:
        raise NativeBuildReceiptError(
            "native release wheel was not built with the measured EC2 Cascade Lake profile"
        )
    return observed


def _production_build_surface(module: Any) -> dict[str, Any]:
    """Require a production module that contains live APIs, not replay research."""

    observed = {
        "flavor": str(getattr(module, "NATIVE_BUILD_FLAVOR", "")),
        "tick_replay_available": bool(
            getattr(module, "NATIVE_TICK_REPLAY_AVAILABLE", True)
        ),
        "research_runtime_available": bool(
            getattr(module, "NATIVE_RESEARCH_RUNTIME_AVAILABLE", True)
        ),
    }
    expected = {
        "flavor": PRODUCTION_BUILD_FLAVOR,
        "tick_replay_available": False,
        "research_runtime_available": False,
    }
    if observed != expected:
        raise NativeBuildReceiptError(
            "native release wheel was not built with the live-only production surface"
        )
    return observed


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
) -> dict[str, int]:
    if (
        not expected_module_token
        or "\x00" in expected_module_token
        or not expected_module_token.endswith(os.sep)
        or not Path(expected_module_token).is_absolute()
    ):
        raise NativeBuildReceiptError("native parity module token is invalid")
    parity_environment = dict(os.environ)
    with tempfile.TemporaryDirectory(prefix="narrowgate-native-qualification-") as temp:
        report_path = Path(temp) / "pytest-qualification.json"
        parity_environment.update(
            {
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
                "NARROWGATE_CPP_EXPECT_MODULE_TOKEN": expected_module_token,
                _QUALIFICATION_REPORT_ENV: str(report_path),
            }
        )
        completed = subprocess.run(
            (
                sys.executable,
                "-B",
                "-m",
                "pytest",
                "-q",
                "-o",
                "xfail_strict=true",
                "-p",
                "live.native_build_receipt",
                *LIVE_PARITY_TESTS,
            ),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=300.0,
            env=parity_environment,
        )
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise NativeBuildReceiptError(
                "native parity qualification report is missing or invalid"
            ) from exc
    expected_fields = {
        "collected",
        "passed",
        "failed",
        "errors",
        "skipped",
        "xfailed",
        "xpassed",
        "deselected",
        "exitstatus",
    }
    if set(report) != expected_fields or any(
        isinstance(report[name], bool) or not isinstance(report[name], int)
        for name in expected_fields
    ):
        raise NativeBuildReceiptError("native parity qualification counts are invalid")
    disallowed = (
        report["failed"]
        + report["errors"]
        + report["skipped"]
        + report["xfailed"]
        + report["xpassed"]
        + report["deselected"]
    )
    if (
        completed.returncode != 0
        or report["exitstatus"] != 0
        or report["collected"] <= 0
        or report["passed"] != report["collected"]
        or disallowed != 0
    ):
        raise NativeBuildReceiptError(
            "native live parity qualification did not pass every collected test"
        )
    return {name: int(report[name]) for name in expected_fields if name != "exitstatus"}


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
    required = locked_runtime.NATIVE_LIVE_ABI_CONTRACT["required_apis"]
    if any(not hasattr(narrowgate_cpp, name) for name in required):
        raise NativeBuildReceiptError("native module lacks required live API")
    _validate_lightgbm_bundle_abi(narrowgate_cpp)
    if not bool(narrowgate_cpp.NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE):
        raise NativeBuildReceiptError(
            "native module order-action planner capability is unavailable"
        )
    if not bool(narrowgate_cpp.NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE):
        raise NativeBuildReceiptError(
            "native module cooldown hot-path capability is unavailable"
        )
    build_surface = _production_build_surface(narrowgate_cpp)
    live_cpu_build = _production_live_cpu_build(narrowgate_cpp)
    required_class_members = locked_runtime.NATIVE_LIVE_ABI_CONTRACT[
        "required_class_members"
    ]
    if any(
        not hasattr(getattr(narrowgate_cpp, class_name), member)
        for class_name, members in required_class_members.items()
        for member in members
    ):
        raise NativeBuildReceiptError("native module lacks required live class API")
    quote_instances = {
        "QuoteFlags": narrowgate_cpp.QuoteFlags(),
        "SideQuoteContext": narrowgate_cpp.SideQuoteContext(),
    }
    if any(
        not hasattr(quote_instances[class_name], field)
        for class_name, fields in locked_runtime.NATIVE_LIVE_ABI_CONTRACT[
            "required_quote_fields"
        ].items()
        for field in fields
    ):
        raise NativeBuildReceiptError("native module lacks successor quote ABI fields")
    parity_qualification = _run_native_parity_smoke(
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
        "build_surface": build_surface,
        "live_cpu_build": live_cpu_build,
        "abi_contract": locked_runtime.native_live_abi_contract_payload(),
        "parity_qualification": {
            "tests": list(LIVE_PARITY_TESTS),
            **parity_qualification,
            "validated": True,
        },
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
