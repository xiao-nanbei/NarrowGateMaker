from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import platform
import shlex
import stat
import subprocess
import sys
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from live import deployment_runtime as subject
from live import runtime_policy
from scripts import live_deploy_common as source_deploy

BUILDER_PYTHON = Path(sys.executable)
pytestmark = pytest.mark.skipif(
    sys.version_info[:2] != subject.REQUIRED_PYTHON,
    reason="locked live runtime is intentionally CPython 3.12-only",
)


def _digest(raw: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b"=").decode()


def _wheel(
    directory: Path,
    *,
    name: str,
    version: str,
    marker: str = "original",
    requires: tuple[str, ...] = (),
    extra_members: dict[str, bytes] | None = None,
) -> Path:
    normalized = name.replace("-", "_").replace(".", "_")
    dist_info = f"{normalized}-{version}.dist-info"
    package = f"{normalized}_fixture"
    metadata_lines = [
        "Metadata-Version: 2.1",
        f"Name: {name}",
        f"Version: {version}",
    ]
    metadata_lines.extend(f"Requires-Dist: {requirement}" for requirement in requires)
    members = {
        f"{package}/__init__.py": f"MARKER = {marker!r}\n".encode(),
        f"{dist_info}/METADATA": ("\n".join(metadata_lines) + "\n\n").encode(),
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\n"
            b"Generator: NarrowGate locked-runtime test\n"
            b"Root-Is-Purelib: true\n"
            b"Tag: py3-none-any\n"
        ),
    }
    if extra_members:
        assert not set(members) & set(extra_members)
        members.update(extra_members)
    record_path = f"{dist_info}/RECORD"
    record = io.StringIO()
    writer = csv.writer(record, lineterminator="\n")
    for member, raw in sorted(members.items()):
        writer.writerow((member, f"sha256={_digest(raw)}", len(raw)))
    writer.writerow((record_path, "", ""))
    members[record_path] = record.getvalue().encode()
    path = directory / f"{normalized}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        for member, raw in sorted(members.items()):
            info = zipfile.ZipInfo(member, date_time=(2020, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            archive.writestr(info, raw)
    return path


@pytest.fixture(scope="session", autouse=True)
def _explicit_builder_virtual_environment(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[None]:
    """Exercise the runtime builder through a real venv on every test host."""

    global BUILDER_PYTHON

    original = BUILDER_PYTHON
    root = tmp_path_factory.mktemp("locked-runtime-builder")
    venv = root / "venv"
    original_snapshot = subject.probe_interpreter(original)
    creator = subject._venv_creator_for_builder(original, original_snapshot)  # noqa: SLF001
    subprocess.run(
        (
            str(creator),
            "-I",
            "-B",
            "-m",
            "venv",
            "--copies",
            str(venv),
        ),
        check=True,
        capture_output=True,
        text=True,
        timeout=180.0,
    )
    builder = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    excluded_wheels = (
        _wheel(root, name="narrowgate", version="0.1.2.dev0"),
        _wheel(root, name="narrowgate-btcusdc-cpp", version="0.1.2.dev0"),
    )
    subprocess.run(
        (
            str(builder),
            "-I",
            "-B",
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            "--no-cache-dir",
            "--disable-pip-version-check",
            "--no-compile",
            *(str(path) for path in excluded_wheels),
        ),
        check=True,
        capture_output=True,
        text=True,
        timeout=180.0,
    )
    assert subject.probe_interpreter(builder)["is_virtual_environment"] is True
    BUILDER_PYTHON = builder
    try:
        yield
    finally:
        BUILDER_PYTHON = original


def _write_lock(
    path: Path,
    distributions: list[tuple[str, str]],
) -> dict[str, Any]:
    interpreter = subject.probe_interpreter(BUILDER_PYTHON)
    payload: dict[str, Any] = {
        "schema_version": subject.LOCK_SCHEMA,
        "status": "locked",
        "generated_utc": "2026-08-25T00:00:00Z",
        "interpreter": interpreter,
        "distributions": [
            {"name": subject.normalize_distribution_name(name), "version": version}
            for name, version in sorted(distributions)
        ],
        "excluded_distribution_names": sorted(
            {
                subject.normalize_distribution_name(name)
                for name in subject.DEFAULT_EXCLUDED_DISTRIBUTIONS
            }
        ),
        "excluded_distributions": [],
        "install_contract": {
            "dependencies": "exact_wheel_paths_only",
            "index_access": "forbidden",
            "dependency_resolution": "forbidden",
            "root_wheel": "explicit",
            "native_wheel": "explicit",
        },
    }
    payload[subject.LOCK_CANONICAL_FIELD] = subject.canonical_sha256(
        payload, subject.LOCK_CANONICAL_FIELD
    )
    subject._write_json_authority(path, payload)  # noqa: SLF001
    return payload


def _wheelhouse(
    tmp_path: Path,
    *,
    dependency_names: tuple[tuple[str, str], ...] = (("frozen-dep", "1.2.3"),),
) -> tuple[dict[str, Any], Path, dict[str, Path], Path]:
    lock_path = tmp_path / "runtime.lock.json"
    lock = _write_lock(lock_path, list(dependency_names))
    wheels = {
        name: _wheel(tmp_path, name=name, version=version) for name, version in dependency_names
    }
    output = tmp_path / "wheelhouse"
    subject.receive_wheelhouse(
        lock_path=lock_path,
        expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
        wheel_paths=list(wheels.values()),
        output_dir=output,
    )
    return lock, lock_path, wheels, output


def _install(
    tmp_path: Path,
    *,
    broken_root_requirement: bool = False,
) -> dict[str, Any]:
    lock, lock_path, _wheels, wheelhouse = _wheelhouse(tmp_path)
    root = _wheel(
        tmp_path,
        name="narrowgate",
        version="0.1.2.dev0",
        requires=("missing-runtime-dependency>=1",)
        if broken_root_requirement
        else ("frozen-dep==1.2.3",),
    )
    native = _wheel(tmp_path, name="narrowgate-btcusdc-cpp", version="0.1.2.dev0")
    root_binding = subject.inspect_wheel(root)
    native_binding = subject.inspect_wheel(native)
    manifest = subject.validate_wheelhouse(
        lock_path=lock_path,
        expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
        wheelhouse_dir=wheelhouse,
        expected_manifest_sha256=json.loads((wheelhouse / subject.WHEELHOUSE_MANIFEST).read_text())[
            subject.WHEELHOUSE_CANONICAL_FIELD
        ],
    )
    venv = tmp_path / "locked-venv"
    receipt = tmp_path / "runtime.install.json"
    result = subject.install_locked_runtime(
        builder_python=BUILDER_PYTHON,
        venv_dir=venv,
        lock_path=lock_path,
        expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
        wheelhouse_dir=wheelhouse,
        expected_wheelhouse_sha256=manifest[subject.WHEELHOUSE_CANONICAL_FIELD],
        root_wheel_path=root,
        root_wheel_sha256=root_binding["sha256"],
        native_wheel_path=native,
        native_wheel_sha256=native_binding["sha256"],
        receipt_path=receipt,
        generated_utc="2026-08-25T01:00:00Z",
    )
    return {
        "lock": lock,
        "lock_path": lock_path,
        "wheelhouse": wheelhouse,
        "manifest": manifest,
        "root": root,
        "root_binding": root_binding,
        "native": native,
        "native_binding": native_binding,
        "venv": venv,
        "receipt_path": receipt,
        "receipt": result["receipt"],
    }


def _verify_install(bundle: dict[str, Any]) -> dict[str, Any]:
    return subject.validate_installed_runtime(
        venv_python=bundle["venv"] / "bin/python",
        pip_runner_python=BUILDER_PYTHON,
        receipt_path=bundle["receipt_path"],
        expected_receipt_sha256=bundle["receipt"][subject.INSTALL_CANONICAL_FIELD],
        lock_path=bundle["lock_path"],
        expected_lock_sha256=bundle["lock"][subject.LOCK_CANONICAL_FIELD],
        wheelhouse_dir=bundle["wheelhouse"],
        expected_wheelhouse_sha256=bundle["manifest"][subject.WHEELHOUSE_CANONICAL_FIELD],
        root_wheel_path=bundle["root"],
        root_wheel_sha256=bundle["root_binding"]["sha256"],
        native_wheel_path=bundle["native"],
        native_wheel_sha256=bundle["native_binding"]["sha256"],
    )


def _deployment_envelope_fixture(
    tmp_path: Path,
) -> tuple[dict[str, Any], Path, Path, Path]:
    bundle_root = tmp_path / "runtime"
    bundle_root.mkdir()
    bundle = _install(bundle_root)
    repository = tmp_path / "repository"
    repository.mkdir()
    config = repository / "config.yaml"
    config.write_text("symbol: BTCUSDC\n", encoding="utf-8")
    for command in (
        ("git", "init", "-q"),
        ("git", "config", "user.email", "fixture@example.invalid"),
        ("git", "config", "user.name", "Fixture"),
        ("git", "add", "config.yaml"),
        ("git", "commit", "-q", "-m", "fixture"),
    ):
        subprocess.run(command, cwd=repository, check=True, timeout=30.0)
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ("git", "rev-parse", "HEAD^{tree}"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    module = bundle_root / "narrowgate_cpp.fixture.so"
    module.write_bytes(b"native module fixture")
    install = bundle["receipt"]
    native_receipt: dict[str, Any] = {
        "schema_version": subject.NATIVE_BUILD_RECEIPT_SCHEMA,
        "status": "exact_tag_native_build_dependency_lock_and_parity_passed",
        "execution": {
            "execution_commit": commit,
            "execution_tree": tree,
            # The validator ignores this redundant commit proof.
            "tag_peeled_commit": commit,
        },
        "soabi": install["interpreter"]["soabi"],
        "dependency_lock": {
            "runtime_lock_path": str(bundle["lock_path"].resolve()),
            "runtime_lock_sha256": bundle["lock"][subject.LOCK_CANONICAL_FIELD],
            "wheelhouse_path": str(bundle["wheelhouse"].resolve()),
            "wheelhouse_manifest_path": str(
                (bundle["wheelhouse"] / subject.WHEELHOUSE_MANIFEST).resolve()
            ),
            "wheelhouse_sha256": bundle["manifest"][subject.WHEELHOUSE_CANONICAL_FIELD],
        },
        "installed_distribution_lock": {
            "install_receipt_path": str(bundle["receipt_path"].resolve()),
            "install_receipt_sha256": install[subject.INSTALL_CANONICAL_FIELD],
            "root_wheel_path": str(bundle["root"].resolve()),
            "root_wheel_sha256": bundle["root_binding"]["sha256"],
            "native_wheel_path": str(bundle["native"].resolve()),
            "native_wheel_sha256": bundle["native_binding"]["sha256"],
            "interpreter": install["interpreter"],
            "installed_distributions": install["installed_distributions"],
            "installed_record_aggregate_sha256": install["installed_record_aggregate_sha256"],
        },
        "wheel": {
            "path": str(bundle["native"].resolve()),
            "sha256": bundle["native_binding"]["sha256"],
            "size_bytes": bundle["native"].stat().st_size,
        },
        "module": {
            "path": str(module.resolve()),
            "sha256": hashlib.sha256(module.read_bytes()).hexdigest(),
            "size_bytes": module.stat().st_size,
        },
        "build_surface": {
            "flavor": "live",
            "tick_replay_available": False,
            "research_runtime_available": False,
        },
        "live_cpu_build": {
            "profile": subject.NATIVE_LIVE_CPU_PROFILE,
            "compile_options": subject.NATIVE_LIVE_COMPILE_OPTIONS,
            "production": True,
            "preferred_vector_width_bits": 256,
        },
        "abi_contract": subject.native_live_abi_contract_payload(),
        "parity_qualification": {
            "tests": list(subject.NATIVE_LIVE_PARITY_TESTS),
            "collected": len(subject.NATIVE_LIVE_PARITY_TESTS),
            "passed": len(subject.NATIVE_LIVE_PARITY_TESTS),
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "xfailed": 0,
            "xpassed": 0,
            "deselected": 0,
            "validated": True,
        },
    }
    native_receipt[subject.NATIVE_BUILD_RECEIPT_CANONICAL_FIELD] = subject.canonical_sha256(
        native_receipt, subject.NATIVE_BUILD_RECEIPT_CANONICAL_FIELD
    )
    receipt_path = bundle_root / "native-build.json"
    subject._write_json_authority(receipt_path, native_receipt)  # noqa: SLF001
    model_authorization = tmp_path / "model-authorization.json"
    model_authorization.write_text(
        '{"schema_version":"fixture_model_authorization.v1"}\n',
        encoding="utf-8",
    )
    return bundle, repository, receipt_path, model_authorization


def _write_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _activation_artifacts(
    tmp_path: Path,
    *,
    envelope_sha256: str,
) -> tuple[Path, str, Path, str]:
    reconciliation_path = tmp_path / "stopped-reconciliation.json"
    position_rows = [
        {
            "symbol": "BTCUSDC",
            "position_side": "BOTH",
            "position_amt": "0.000",
            "entry_price": "0.0",
            "update_time_ms": 1,
        }
    ]
    reconciliation: dict[str, Any] = {
        "schema_version": subject.STOPPED_RECONCILIATION_SCHEMA,
        "status": subject.STOPPED_RECONCILIATION_STATUS,
        "generated_utc": "2026-08-30T00:00:00Z",
        "symbol": "BTCUSDC",
        "open_order_count": 0,
        "signed_endpoints": ["/fapi/v1/openOrders", "/fapi/v3/positionRisk"],
        "signed_read_sequence": [
            "/fapi/v1/openOrders",
            "/fapi/v3/positionRisk",
            "/fapi/v1/openOrders",
            "/fapi/v3/positionRisk",
        ],
        "account_key_sha256": "a" * 64,
        "position_rows": position_rows,
        # Published predecessor receipts may carry this redundant leaf. The
        # canonical loader must continue to admit them while new writers omit it.
        "position_lineage_sha256": hashlib.sha256(
            json.dumps(
                position_rows,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        ).hexdigest(),
    }
    reconciliation_root = subject.canonical_sha256(
        reconciliation,
        subject.STOPPED_RECONCILIATION_CANONICAL_FIELD,
    )
    reconciliation[subject.STOPPED_RECONCILIATION_CANONICAL_FIELD] = reconciliation_root
    _write_private_json(reconciliation_path, reconciliation)

    runtime_path = tmp_path / "runtime-identity.json"
    runtime_identity = {
        "schema_version": subject.LIVE_RUNTIME_IDENTITY_SCHEMA,
        "dry_run": False,
        "testnet": False,
        "startup_attestation": {
            "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
            "status": "accepted",
            "errors": [],
            "gates": {"safe_to_start_live_loops": True},
            "deployment_envelope": {"canonical_sha256": envelope_sha256},
        },
        "startup_exchange_reconciliation": {
            "path": str(reconciliation_path.resolve()),
            "canonical_sha256": reconciliation_root,
        },
    }
    _write_private_json(runtime_path, runtime_identity)
    return (
        reconciliation_path,
        reconciliation_root,
        runtime_path,
        hashlib.sha256(runtime_path.read_bytes()).hexdigest(),
    )


def _site_packages(venv: Path) -> Path:
    candidates = list((venv / "lib").glob("python3.12/site-packages"))
    assert len(candidates) == 1
    return candidates[0]


def _rewrite_record_digest(record: Path, relative_name: str, raw: bytes) -> None:
    rows = list(csv.reader(io.StringIO(record.read_text())))
    replaced = False
    for row in rows:
        if row[0] == relative_name:
            row[1] = f"sha256={_digest(raw)}"
            row[2] = str(len(raw))
            replaced = True
    assert replaced
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerows(rows)
    record.write_text(output.getvalue())


def _mock_copied_venv_interpreters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    executable_raw: bytes,
    declared_base_raw: bytes,
    versioned_base_raw: bytes,
) -> tuple[Path, Path, Path]:
    venv_prefix = tmp_path / "venv"
    base_prefix = tmp_path / "base"
    executable_name = "python.exe" if sys.platform == "win32" else "python"
    executable = venv_prefix / ("Scripts" if sys.platform == "win32" else "bin") / executable_name
    declared_base = base_prefix / ("python3.exe" if sys.platform == "win32" else "bin/python3")
    versioned_base = (
        base_prefix / executable_name
        if sys.platform == "win32"
        else base_prefix / "bin" / f"python{sys.version_info.major}.{sys.version_info.minor}"
    )
    for path, raw in (
        (executable, executable_raw),
        (declared_base, declared_base_raw),
        (versioned_base, versioned_base_raw),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    monkeypatch.setattr(sys, "executable", str(executable))
    monkeypatch.setattr(sys, "prefix", str(venv_prefix))
    monkeypatch.setattr(sys, "base_prefix", str(base_prefix))
    monkeypatch.setattr(sys, "_base_executable", str(declared_base))
    return executable, declared_base, versioned_base


def test_interpreter_snapshot_safely_corrects_wrong_unversioned_base_for_copied_venv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable_raw = b"exact Python 3.12 executable bytes"
    _executable, declared_base, _versioned_base = _mock_copied_venv_interpreters(
        tmp_path,
        monkeypatch,
        executable_raw=executable_raw,
        declared_base_raw=b"Amazon Linux unversioned Python 3.9 bytes",
        versioned_base_raw=executable_raw,
    )

    snapshot = subject._current_interpreter_snapshot()  # noqa: SLF001

    assert snapshot["executable_sha256"] == hashlib.sha256(executable_raw).hexdigest()
    assert snapshot["base_executable_sha256"] == snapshot["executable_sha256"]
    assert snapshot["base_executable_size_bytes"] == len(executable_raw)
    assert (
        snapshot["base_executable_sha256"] != hashlib.sha256(declared_base.read_bytes()).hexdigest()
    )


def test_venv_creator_ignores_wrong_unversioned_base_for_copied_venv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable_raw = b"exact Python 3.12 executable bytes"
    _executable, declared_base, versioned_base = _mock_copied_venv_interpreters(
        tmp_path,
        monkeypatch,
        executable_raw=executable_raw,
        declared_base_raw=b"Amazon Linux unversioned Python 3.9 bytes",
        versioned_base_raw=executable_raw,
    )

    creator = subject._current_venv_creator_snapshot()  # noqa: SLF001

    assert creator == {
        "path": str(versioned_base.resolve()),
        "sha256": hashlib.sha256(executable_raw).hexdigest(),
        "size_bytes": len(executable_raw),
    }
    assert creator["path"] != str(declared_base)


@pytest.mark.parametrize("candidate_state", ["missing", "different-bytes"])
def test_venv_creator_fails_closed_without_exact_versioned_base(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    candidate_state: str,
) -> None:
    executable_raw = b"exact Python 3.12 executable bytes"
    _executable, _declared_base, versioned_base = _mock_copied_venv_interpreters(
        tmp_path,
        monkeypatch,
        executable_raw=executable_raw,
        declared_base_raw=b"Amazon Linux unversioned Python 3.9 bytes",
        versioned_base_raw=(
            executable_raw if candidate_state == "missing" else b"different Python bytes"
        ),
    )
    if candidate_state == "missing":
        versioned_base.unlink()

    with pytest.raises(
        subject.LockedRuntimeError,
        match=(
            "interpreter does not resolve"
            if candidate_state == "missing"
            else "no base venv creator is byte-identical"
        ),
    ):
        subject._current_venv_creator_snapshot()  # noqa: SLF001


def test_venv_creator_uses_matching_declared_base_without_versioned_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable_raw = b"exact Python 3.12 executable bytes"
    _executable, declared_base, versioned_base = _mock_copied_venv_interpreters(
        tmp_path,
        monkeypatch,
        executable_raw=executable_raw,
        declared_base_raw=executable_raw,
        versioned_base_raw=b"unused versioned alias",
    )
    versioned_base.unlink()

    creator = subject._current_venv_creator_snapshot()  # noqa: SLF001

    assert creator["path"] == str(declared_base.resolve())
    assert creator["sha256"] == hashlib.sha256(executable_raw).hexdigest()


def test_interpreter_snapshot_refuses_mismatched_versioned_base_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    declared_raw = b"declared unversioned base bytes"
    _mock_copied_venv_interpreters(
        tmp_path,
        monkeypatch,
        executable_raw=b"copied Python 3.12 executable bytes",
        declared_base_raw=declared_raw,
        versioned_base_raw=b"different versioned candidate bytes",
    )

    snapshot = subject._current_interpreter_snapshot()  # noqa: SLF001

    assert snapshot["base_executable_sha256"] == hashlib.sha256(declared_raw).hexdigest()
    assert snapshot["base_executable_size_bytes"] == len(declared_raw)
    assert snapshot["base_executable_sha256"] != snapshot["executable_sha256"]


def test_venv_creator_is_reprobed_before_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = subject.probe_interpreter(BUILDER_PYTHON)
    creator_path = tmp_path / "python3.12"
    creator_raw = b"selected creator bytes"
    creator_path.write_bytes(creator_raw)
    binding = {
        "path": str(creator_path),
        "sha256": hashlib.sha256(creator_raw).hexdigest(),
        "size_bytes": len(creator_raw),
    }
    drifted_creator = dict(builder)
    drifted_creator["version"] = "3.12.0"
    monkeypatch.setattr(subject, "_run_python_json", lambda *_args: binding)
    monkeypatch.setattr(subject, "probe_interpreter", lambda _path: drifted_creator)

    with pytest.raises(
        subject.LockedRuntimeError,
        match=r"venv creator interpreter drift: \['version'\]",
    ):
        subject._venv_creator_for_builder(  # noqa: SLF001
            BUILDER_PYTHON,
            builder,
        )


def test_base_builder_is_used_directly_without_creator_alias_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder_path = tmp_path / "python3.12"
    builder_raw = b"verified base builder bytes"
    builder_path.write_bytes(builder_raw)
    builder = subject.probe_interpreter(BUILDER_PYTHON)
    builder.update(
        {
            "executable_sha256": hashlib.sha256(builder_raw).hexdigest(),
            "executable_size_bytes": len(builder_raw),
            "base_executable_sha256": hashlib.sha256(builder_raw).hexdigest(),
            "base_executable_size_bytes": len(builder_raw),
            "is_virtual_environment": False,
        }
    )

    def unexpected_probe(*_args: object) -> dict[str, object]:
        raise AssertionError("base builder must not require a venv creator alias probe")

    monkeypatch.setattr(subject, "_run_python_json", unexpected_probe)

    assert subject._venv_creator_for_builder(builder_path, builder) == builder_path.resolve()  # noqa: SLF001


class _SeedDistribution:
    def __init__(self, metadata_path: Path, *, name: str, version: str) -> None:
        self._path = metadata_path
        self.metadata = {"Name": name}
        self.version = version

    @staticmethod
    def read_text(filename: str) -> None:
        assert filename == "direct_url.json"
        return None


def test_seed_snapshot_deduplicates_one_metadata_inode_reached_through_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib.metadata as metadata

    real_site = tmp_path / "lib" / "python3.12" / "site-packages"
    dist_info = real_site / "binance_futures_connector-4.1.0.dist-info"
    dist_info.mkdir(parents=True)
    lib64 = tmp_path / "lib64"
    lib64.symlink_to(tmp_path / "lib", target_is_directory=True)
    alias = lib64 / "python3.12" / "site-packages" / dist_info.name
    assert (dist_info.stat().st_dev, dist_info.stat().st_ino) == (
        alias.stat().st_dev,
        alias.stat().st_ino,
    )
    monkeypatch.setattr(
        metadata,
        "distributions",
        lambda: iter(
            (
                _SeedDistribution(
                    dist_info,
                    name="binance-futures-connector",
                    version="4.1.0",
                ),
                _SeedDistribution(
                    alias,
                    name="binance-futures-connector",
                    version="4.1.0",
                ),
            )
        ),
    )

    snapshot = subject._seed_snapshot_current()  # noqa: SLF001

    assert snapshot["distributions"] == [
        {
            "name": "binance-futures-connector",
            "source_kind": "index_or_unknown",
            "version": "4.1.0",
        }
    ]


def test_build_lock_still_rejects_same_name_at_distinct_metadata_inodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib.metadata as metadata

    first = tmp_path / "first" / "duplicate-1.0.dist-info"
    second = tmp_path / "second" / "duplicate-1.0.dist-info"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    assert (first.stat().st_dev, first.stat().st_ino) != (
        second.stat().st_dev,
        second.stat().st_ino,
    )
    monkeypatch.setattr(
        metadata,
        "distributions",
        lambda: iter(
            (
                _SeedDistribution(first, name="duplicate", version="1.0"),
                _SeedDistribution(second, name="duplicate", version="1.0"),
            )
        ),
    )
    snapshot = subject._seed_snapshot_current()  # noqa: SLF001
    assert len(snapshot["distributions"]) == 2
    snapshot["interpreter"] = subject.probe_interpreter(BUILDER_PYTHON)
    monkeypatch.setattr(subject, "_run_python_json", lambda *_args: snapshot)

    with pytest.raises(subject.LockedRuntimeError, match="duplicate seed distribution: duplicate"):
        subject.build_lock(seed_python=BUILDER_PYTHON)


def test_build_lock_excludes_editable_root_and_native_and_binds_exact_interpreter() -> None:
    lock = subject.build_lock(
        seed_python=BUILDER_PYTHON,
        generated_utc="2026-08-25T00:00:00Z",
    )
    names = {row["name"] for row in lock["distributions"]}
    excluded = {row["name"] for row in lock["excluded_distributions"]}
    assert "narrowgate" not in names
    assert "narrowgate-btcusdc-cpp" not in names
    assert {"narrowgate", "narrowgate-btcusdc-cpp"} <= excluded
    interpreter = lock["interpreter"]
    assert interpreter["version_info"] == list(sys.version_info[:3])
    assert interpreter["soabi"].startswith("cpython-312")
    assert interpreter["compiler"]
    assert interpreter["openssl_runtime"].startswith("OpenSSL ")
    assert len(interpreter["executable_sha256"]) == 64
    assert lock[subject.LOCK_CANONICAL_FIELD] == subject.canonical_sha256(
        lock, subject.LOCK_CANONICAL_FIELD
    )


def test_private_probe_is_a_real_subprocess_cli() -> None:
    completed = subprocess.run(
        (
            str(BUILDER_PYTHON),
            "-I",
            "-B",
            str(Path(subject.__file__).resolve()),
            "_probe-interpreter",
        ),
        check=True,
        capture_output=True,
        text=True,
        timeout=30.0,
    )
    payload = json.loads(completed.stdout)
    assert payload["version_info"] == list(sys.version_info[:3])
    assert payload["is_virtual_environment"] is True
    assert len(payload["executable_sha256"]) == 64


def test_private_probe_runner_forces_no_bytecode_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, tuple[str, ...]] = {}

    def fake_run(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        observed["argv"] = argv
        return subprocess.CompletedProcess(argv, 0, stdout="{}\n", stderr="")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    assert subject._run_python_json(BUILDER_PYTHON, "_probe-interpreter") == {}  # noqa: SLF001
    assert observed["argv"][1:3] == ("-I", "-B")


def test_content_addressed_wheelhouse_is_private_complete_and_create_only(
    tmp_path: Path,
) -> None:
    lock, lock_path, _wheels, wheelhouse = _wheelhouse(tmp_path)
    manifest_path = wheelhouse / subject.WHEELHOUSE_MANIFEST
    manifest = json.loads(manifest_path.read_text())
    row = manifest["wheels"][0]
    artifact = wheelhouse / row["relative_path"]
    assert artifact.read_bytes()
    assert stat.S_IMODE(wheelhouse.stat().st_mode) == 0o700
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(artifact.stat().st_mode) == 0o600
    assert artifact.stat().st_nlink == 1
    assert (
        subject.validate_wheelhouse(
            lock_path=lock_path,
            expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
            wheelhouse_dir=wheelhouse,
            expected_manifest_sha256=manifest[subject.WHEELHOUSE_CANONICAL_FIELD],
        )
        == manifest
    )
    with pytest.raises(subject.LockedRuntimeError, match="create-only wheelhouse conflict"):
        subject.receive_wheelhouse(
            lock_path=lock_path,
            expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
            wheel_paths=[],
            output_dir=wheelhouse,
        )


def test_missing_locked_wheel_leaves_no_partial_wheelhouse(tmp_path: Path) -> None:
    lock_path = tmp_path / "runtime.lock.json"
    lock = _write_lock(lock_path, [("first-dep", "1.0"), ("second-dep", "2.0")])
    first = _wheel(tmp_path, name="first-dep", version="1.0")
    output = tmp_path / "incomplete"
    with pytest.raises(subject.LockedRuntimeError, match="missing locked wheels"):
        subject.receive_wheelhouse(
            lock_path=lock_path,
            expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
            wheel_paths=[first],
            output_dir=output,
        )
    assert not output.exists()
    assert not list(tmp_path.glob(".incomplete.staging.*"))


def test_symlink_wheel_input_is_rejected_before_publication(tmp_path: Path) -> None:
    lock_path = tmp_path / "runtime.lock.json"
    lock = _write_lock(lock_path, [("frozen-dep", "1.2.3")])
    wheel = _wheel(tmp_path, name="frozen-dep", version="1.2.3")
    link = tmp_path / "linked.whl"
    link.symlink_to(wheel)
    output = tmp_path / "wheelhouse"
    with pytest.raises(subject.LockedRuntimeError, match="symlink"):
        subject.receive_wheelhouse(
            lock_path=lock_path,
            expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
            wheel_paths=[link],
            output_dir=output,
        )
    assert not output.exists()


def test_wheel_nested_vendored_dist_info_authorities_are_recorded_ordinary_members(
    tmp_path: Path,
) -> None:
    nested = "setuptools/_vendor/vendored-1.0.dist-info"
    wheel = _wheel(
        tmp_path,
        name="setuptools",
        version="81.0.0",
        extra_members={
            f"{nested}/METADATA": b"vendored metadata payload\n",
            f"{nested}/WHEEL": b"vendored wheel payload\n",
            f"{nested}/RECORD": b"vendored record payload\n",
        },
    )

    assert subject.inspect_wheel(wheel) == {
        "name": "setuptools",
        "version": "81.0.0",
        "filename": wheel.name,
        "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "size_bytes": wheel.stat().st_size,
    }


def test_wheel_rejects_multiple_top_level_dist_info_authority_directories(
    tmp_path: Path,
) -> None:
    other = "unrelated-2.0.dist-info"
    wheel = _wheel(
        tmp_path,
        name="primary",
        version="1.0",
        extra_members={
            f"{other}/METADATA": b"Metadata-Version: 2.1\nName: unrelated\nVersion: 2.0\n\n",
            f"{other}/WHEEL": b"Wheel-Version: 1.0\nTag: py3-none-any\n",
            f"{other}/RECORD": b"ordinary payload for the primary RECORD to bind\n",
        },
    )

    with pytest.raises(subject.LockedRuntimeError, match="wheel authority members are ambiguous"):
        subject.inspect_wheel(wheel)


def test_wheel_tamper_and_resigned_manifest_cannot_cross_frozen_authority(
    tmp_path: Path,
) -> None:
    original_dir = tmp_path / "original"
    original_dir.mkdir()
    lock, lock_path, _wheels, wheelhouse = _wheelhouse(original_dir)
    original_manifest = json.loads((wheelhouse / subject.WHEELHOUSE_MANIFEST).read_text())
    row = original_manifest["wheels"][0]
    artifact = wheelhouse / row["relative_path"]
    artifact.write_bytes(artifact.read_bytes() + b"tamper")
    artifact.chmod(0o600)
    with pytest.raises(subject.LockedRuntimeError, match="SHA256 mismatch"):
        subject.validate_wheelhouse(
            lock_path=lock_path,
            expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
            wheelhouse_dir=wheelhouse,
            expected_manifest_sha256=original_manifest[subject.WHEELHOUSE_CANONICAL_FIELD],
        )

    forged_dir = tmp_path / "forged"
    forged_dir.mkdir()
    forged_lock_path = forged_dir / "runtime.lock.json"
    forged_lock_path.write_bytes(lock_path.read_bytes())
    forged_lock_path.chmod(0o600)
    changed = _wheel(
        forged_dir,
        name="frozen-dep",
        version="1.2.3",
        marker="different bytes with the same name and version",
    )
    forged = subject.receive_wheelhouse(
        lock_path=forged_lock_path,
        expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
        wheel_paths=[changed],
        output_dir=forged_dir / "wheelhouse",
    )["manifest"]
    assert (
        forged[subject.WHEELHOUSE_CANONICAL_FIELD]
        != original_manifest[subject.WHEELHOUSE_CANONICAL_FIELD]
    )
    with pytest.raises(subject.LockedRuntimeError, match="frozen expected hash"):
        subject.validate_wheelhouse(
            lock_path=forged_lock_path,
            expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
            wheelhouse_dir=forged_dir / "wheelhouse",
            expected_manifest_sha256=original_manifest[subject.WHEELHOUSE_CANONICAL_FIELD],
        )


def test_offline_install_receipt_binds_versions_records_and_interpreter(tmp_path: Path) -> None:
    bundle = _install(tmp_path)
    receipt = bundle["receipt"]
    assert _verify_install(bundle) == receipt
    assert stat.S_IMODE(bundle["receipt_path"].stat().st_mode) == 0o600
    assert bundle["receipt_path"].stat().st_nlink == 1
    assert receipt["pip_check"] == {"passed": True}
    assert receipt["interpreter"]["version"] == platform.python_version()
    assert receipt["interpreter"]["soabi"].startswith("cpython-312")
    assert receipt["interpreter"]["openssl_runtime"]
    assert len(receipt["installed_record_aggregate_sha256"]) == 64
    assert {row["name"] for row in receipt["installed_distributions"]} == {
        "frozen-dep",
        "narrowgate",
        "narrowgate-btcusdc-cpp",
    }
    assert receipt["install_policy"] == {
        "target_started_without_pip": True,
        "builder_pip_target_mode": True,
        "no_index": True,
        "no_dependencies": True,
        "no_cache": True,
        "exact_wheel_paths": True,
        "bytecode_files_forbidden": True,
        "record_outside_site_files_forbidden": True,
        "static_tree_verified_before_target_execution": True,
    }
    assert not list(_site_packages(bundle["venv"]).rglob("*.pyc"))
    assert not list(_site_packages(bundle["venv"]).rglob("__pycache__"))
    assert (
        subject.validate_static_installed_tree(
            venv_dir=bundle["venv"],
            receipt_path=bundle["receipt_path"],
            expected_receipt_sha256=receipt[subject.INSTALL_CANONICAL_FIELD],
        )
        == receipt
    )
    assert (
        subject.validate_startup_runtime(
            venv_python=bundle["venv"] / "bin/python",
            pip_runner_python=BUILDER_PYTHON,
            receipt_path=bundle["receipt_path"],
            expected_receipt_sha256=receipt[subject.INSTALL_CANONICAL_FIELD],
            expected_lock_sha256=bundle["lock"][subject.LOCK_CANONICAL_FIELD],
            expected_wheelhouse_sha256=bundle["manifest"][subject.WHEELHOUSE_CANONICAL_FIELD],
            expected_root_wheel_sha256=bundle["root_binding"]["sha256"],
            expected_native_wheel_sha256=bundle["native_binding"]["sha256"],
            expected_python_version=receipt["interpreter"]["version"],
            expected_soabi=receipt["interpreter"]["soabi"],
            expected_compiler=receipt["interpreter"]["compiler"],
            expected_openssl_runtime=receipt["interpreter"]["openssl_runtime"],
            expected_interpreter_executable_sha256=receipt["interpreter"]["executable_sha256"],
        )
        == receipt
    )


def test_offline_install_streams_wheels_without_whole_file_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock, lock_path, _wheels, wheelhouse = _wheelhouse(tmp_path)
    payload = b"bounded-wheel-payload" * 256
    root = _wheel(
        tmp_path,
        name="narrowgate",
        version="0.1.2.dev0",
        requires=("frozen-dep==1.2.3",),
        extra_members={"narrowgate_fixture/payload.bin": payload},
    )
    native = _wheel(
        tmp_path,
        name="narrowgate-btcusdc-cpp",
        version="0.1.2.dev0",
        extra_members={"narrowgate_cpp_fixture/payload.bin": payload},
    )
    root_binding = subject.inspect_wheel(root)
    native_binding = subject.inspect_wheel(native)
    manifest = json.loads((wheelhouse / subject.WHEELHOUSE_MANIFEST).read_text())

    original_read = subject._read_regular_file  # noqa: SLF001

    def reject_whole_wheel_read(
        path: Path,
        *,
        private_authority: bool = False,
    ) -> bytes:
        if Path(path).suffix == ".whl":
            raise AssertionError("install must not read a whole wheel into memory")
        return original_read(path, private_authority=private_authority)

    def reject_bytes_inspector(
        path: Path,
        *,
        expected_sha256: str | None = None,
    ) -> tuple[dict[str, Any], bytes]:
        del path, expected_sha256
        raise AssertionError("install must inspect wheels through the bounded path reader")

    monkeypatch.setattr(subject, "_WHEEL_IO_CHUNK_BYTES", 64)
    monkeypatch.setattr(subject, "_read_regular_file", reject_whole_wheel_read)
    monkeypatch.setattr(subject, "_inspect_wheel_bytes", reject_bytes_inspector)
    receipt_path = tmp_path / "runtime.install.json"
    result = subject.install_locked_runtime(
        builder_python=BUILDER_PYTHON,
        venv_dir=tmp_path / "locked-venv",
        lock_path=lock_path,
        expected_lock_sha256=lock[subject.LOCK_CANONICAL_FIELD],
        wheelhouse_dir=wheelhouse,
        expected_wheelhouse_sha256=manifest[subject.WHEELHOUSE_CANONICAL_FIELD],
        root_wheel_path=root,
        root_wheel_sha256=root_binding["sha256"],
        native_wheel_path=native,
        native_wheel_sha256=native_binding["sha256"],
        receipt_path=receipt_path,
        generated_utc="2026-08-25T01:00:00Z",
    )

    assert result["receipt"]["explicit_wheels"] == {
        "root": root_binding,
        "native": native_binding,
    }
    assert receipt_path.is_file()


def test_offline_install_uses_base_creator_and_keeps_builder_for_pip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = subject.probe_interpreter(BUILDER_PYTHON)
    expected_creator = subject._venv_creator_for_builder(  # noqa: SLF001
        BUILDER_PYTHON,
        builder,
    )
    calls: list[tuple[str, tuple[str, ...]]] = []
    original = subject._run_checked  # noqa: SLF001

    def recording_run_checked(
        command: tuple[str, ...] | list[str],
        *,
        timeout: float,
        env: dict[str, str],
        label: str,
    ) -> subprocess.CompletedProcess[str]:
        calls.append((label, tuple(command)))
        return original(command, timeout=timeout, env=env, label=label)

    monkeypatch.setattr(subject, "_run_checked", recording_run_checked)
    _install(tmp_path)

    by_label = {label: command for label, command in calls}
    assert by_label["fresh venv creation"][0] == str(expected_creator)
    assert by_label["offline exact-wheel install"][0] == str(BUILDER_PYTHON.absolute())
    assert by_label["pip check"][0] == str(BUILDER_PYTHON.absolute())


def test_installed_version_drift_is_detected_even_if_record_is_resigned(tmp_path: Path) -> None:
    bundle = _install(tmp_path)
    dist_info = _site_packages(bundle["venv"]) / "frozen_dep-1.2.3.dist-info"
    metadata = dist_info / "METADATA"
    changed = metadata.read_bytes().replace(b"Version: 1.2.3", b"Version: 9.9.9")
    assert changed != metadata.read_bytes()
    metadata.write_bytes(changed)
    _rewrite_record_digest(dist_info / "RECORD", "frozen_dep-1.2.3.dist-info/METADATA", changed)
    with pytest.raises(subject.LockedRuntimeError, match="distribution/version drift"):
        _verify_install(bundle)


def test_installed_record_order_drift_is_detected(tmp_path: Path) -> None:
    bundle = _install(tmp_path)
    record = _site_packages(bundle["venv"]) / "frozen_dep-1.2.3.dist-info/RECORD"
    rows = list(csv.reader(io.StringIO(record.read_text())))
    assert len(rows) >= 3
    rows[0], rows[1] = rows[1], rows[0]
    output = io.StringIO()
    csv.writer(output, lineterminator="\n").writerows(rows)
    record.write_text(output.getvalue())
    with pytest.raises(subject.LockedRuntimeError, match="RECORD detail drift"):
        _verify_install(bundle)


def test_static_gate_rejects_unmanifested_pth_before_target_can_execute(
    tmp_path: Path,
) -> None:
    bundle = _install(tmp_path)
    sentinel = tmp_path / "pth-executed"
    injected = _site_packages(bundle["venv"]) / "injected.pth"
    injected.write_text(f"import pathlib; pathlib.Path({str(sentinel)!r}).write_text('executed')\n")
    completed = subprocess.run(
        (
            str(BUILDER_PYTHON),
            "-I",
            "-B",
            str(Path(subject.__file__).resolve()),
            "verify-static-tree",
            "--venv",
            str(bundle["venv"]),
            "--receipt",
            str(bundle["receipt_path"]),
            "--expected-receipt-sha256",
            bundle["receipt"][subject.INSTALL_CANONICAL_FIELD],
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=30.0,
    )
    assert completed.returncode == 2
    assert "outside every RECORD" in completed.stderr
    assert not sentinel.exists()


def test_static_gate_rejects_unmanifested_bytecode_and_symlink(tmp_path: Path) -> None:
    bundle = _install(tmp_path)
    site_packages = _site_packages(bundle["venv"])
    bytecode = site_packages / "frozen_dep_fixture" / "__pycache__" / "bad.pyc"
    bytecode.parent.mkdir()
    bytecode.write_bytes(b"not trusted bytecode")
    with pytest.raises(subject.LockedRuntimeError, match="bytecode"):
        subject.validate_static_installed_tree(
            venv_dir=bundle["venv"],
            receipt_path=bundle["receipt_path"],
            expected_receipt_sha256=bundle["receipt"][subject.INSTALL_CANONICAL_FIELD],
        )

    bytecode.unlink()
    bytecode.parent.rmdir()
    link = site_packages / "frozen_dep_fixture" / "linked.py"
    link.symlink_to(site_packages / "frozen_dep_fixture" / "__init__.py")
    with pytest.raises(subject.LockedRuntimeError, match="unsafe"):
        subject.validate_static_installed_tree(
            venv_dir=bundle["venv"],
            receipt_path=bundle["receipt_path"],
            expected_receipt_sha256=bundle["receipt"][subject.INSTALL_CANONICAL_FIELD],
        )


def test_static_gate_rejects_record_listed_unhashed_bytecode(tmp_path: Path) -> None:
    bundle = _install(tmp_path)
    site_packages = _site_packages(bundle["venv"])
    dist_info = site_packages / "frozen_dep-1.2.3.dist-info"
    bytecode_relative = "frozen_dep_fixture/__pycache__/listed.cpython-312.pyc"
    bytecode = site_packages / bytecode_relative
    bytecode.parent.mkdir()
    bytecode.write_bytes(b"unchecked bytecode")
    record = dist_info / "RECORD"
    rows = list(csv.reader(io.StringIO(record.read_text())))
    rows.append([bytecode_relative, "", ""])
    output = io.StringIO()
    csv.writer(output, lineterminator="\n").writerows(rows)
    record.write_text(output.getvalue())
    with pytest.raises(subject.LockedRuntimeError, match="bytecode"):
        subject.validate_static_installed_tree(
            venv_dir=bundle["venv"],
            receipt_path=bundle["receipt_path"],
            expected_receipt_sha256=bundle["receipt"][subject.INSTALL_CANONICAL_FIELD],
        )


def test_pip_check_failure_removes_partial_venv_and_writes_no_receipt(
    tmp_path: Path,
) -> None:
    with pytest.raises(subject.LockedRuntimeError, match="pip check"):
        _install(tmp_path, broken_root_requirement=True)
    assert not (tmp_path / "locked-venv").exists()
    assert not (tmp_path / "runtime.install.json").exists()
    assert not list(tmp_path.glob(".locked-wheels.*"))


def test_receipt_resigning_cannot_cross_frozen_expected_hash(tmp_path: Path) -> None:
    bundle = _install(tmp_path)
    payload = json.loads(bundle["receipt_path"].read_text())
    payload["generated_utc"] = "2099-01-01T00:00:00Z"
    payload[subject.INSTALL_CANONICAL_FIELD] = subject.canonical_sha256(
        payload, subject.INSTALL_CANONICAL_FIELD
    )
    bundle["receipt_path"].write_bytes(subject._canonical_json_bytes(payload))  # noqa: SLF001
    bundle["receipt_path"].chmod(0o600)
    with pytest.raises(subject.LockedRuntimeError, match="frozen expected hash"):
        _verify_install(bundle)


def _clean_source_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "source"
    repository.mkdir()
    subprocess.run(("git", "init", "-q"), cwd=repository, check=True)
    (repository / "tracked.txt").write_text("public source\n")
    subprocess.run(("git", "add", "tracked.txt"), cwd=repository, check=True)
    subprocess.run(
        (
            "git",
            "-c",
            "user.name=NarrowGate test",
            "-c",
            "user.email=narrowgate-test@example.invalid",
            "commit",
            "-q",
            "-m",
            "initial",
        ),
        cwd=repository,
        check=True,
    )
    return repository


def _annotate_source_repository(repository: Path, tag: str) -> str:
    subprocess.run(
        (
            "git",
            "-c",
            "user.name=NarrowGate test",
            "-c",
            "user.email=narrowgate-test@example.invalid",
            "tag",
            "-a",
            tag,
            "-m",
            "public release",
        ),
        cwd=repository,
        check=True,
    )
    return subprocess.check_output(
        ("git", "rev-parse", f"refs/tags/{tag}"), cwd=repository, text=True
    ).strip()


def test_public_source_release_dry_run_binds_clean_git_identity(tmp_path: Path) -> None:
    repository = _clean_source_repository(tmp_path)
    tag_object = _annotate_source_repository(repository, "v1.2.3")
    _annotate_source_repository(repository, "owner-tag-not-selected")

    bundle_path = tmp_path / "source.bundle"
    source_deploy.create_public_source_bundle(
        repository_root=repository,
        output_path=bundle_path,
        annotated_tag="v1.2.3",
    )
    advertised_refs = subprocess.check_output(
        ("git", "bundle", "list-heads", str(bundle_path)), text=True
    )
    assert " refs/tags/v1.2.3\n" in advertised_refs
    assert "owner-tag-not-selected" not in advertised_refs
    bundle_clone = tmp_path / "bundle-clone"
    subprocess.run(
        ("git", "clone", "-q", "--no-checkout", str(bundle_path), str(bundle_clone)),
        check=True,
    )
    cloned_tags = subprocess.check_output(
        ("git", "for-each-ref", "--format=%(refname:short)", "refs/tags"),
        cwd=bundle_clone,
        text=True,
    ).splitlines()
    assert cloned_tags == ["v1.2.3"]

    result = source_deploy.deploy_public_source_release(
        repository_root=repository,
        target="operator@example.invalid",
        release_dir="/opt/narrowgate/releases/example",
        annotated_tag="v1.2.3",
        dry_run=True,
    )

    assert result["schema_version"] == source_deploy.PUBLIC_SOURCE_RELEASE_SCHEMA
    assert result["status"] == "planned"
    assert result["mode"] == "dry-run"
    assert (
        result["execution_commit"]
        == subprocess.check_output(("git", "rev-parse", "HEAD"), cwd=repository, text=True).strip()
    )
    assert (
        result["execution_tree"]
        == subprocess.check_output(
            ("git", "rev-parse", "HEAD^{tree}"), cwd=repository, text=True
        ).strip()
    )
    assert result["annotated_tag"] == "v1.2.3"
    assert result["annotated_tag_object"] == tag_object
    assert len(result["bundle_sha256"]) == 64
    assert result["bundle_size_bytes"] > 0
    assert result["private_materials_transferred"] is False
    assert result["process_restarted"] is False


def test_public_source_release_rejects_dirty_or_malformed_input(tmp_path: Path) -> None:
    repository = _clean_source_repository(tmp_path)
    (repository / "private-config.yaml").write_text("secret: do-not-copy\n")

    with pytest.raises(source_deploy.LiveDeployContractError, match="not clean"):
        source_deploy.deploy_public_source_release(
            repository_root=repository,
            target="operator@example.invalid",
            release_dir="/opt/narrowgate/releases/example",
            dry_run=True,
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="SSH target"):
        source_deploy.deploy_public_source_release(
            repository_root=repository,
            target="-oProxyCommand=unsafe",
            release_dir="/opt/narrowgate/releases/example",
            dry_run=True,
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="absolute POSIX"):
        source_deploy.render_public_source_publish_shell(
            release_dir="relative/release",
            execution_commit="1" * 40,
            execution_tree="2" * 40,
            bundle_sha256="3" * 64,
        )


def test_public_source_publish_shell_is_source_only_and_atomic() -> None:
    rendered = source_deploy.render_public_source_publish_shell(
        release_dir="/opt/narrowgate/releases/example",
        execution_commit="1" * 40,
        execution_tree="2" * 40,
        bundle_sha256="3" * 64,
        annotated_tag="v1.2.3",
        annotated_tag_object="4" * 40,
    )

    assert "git clone --no-checkout" in rendered
    assert "status --porcelain=v1 --untracked-files=all" in rendered
    assert "/bin/mv -T --" in rendered
    assert "cat-file -t" in rendered
    assert "refs/tags/$annotated_tag" in rendered
    assert "live/config.yaml" not in rendered
    assert "restart" not in rendered
    assert "model" not in rendered


def test_build_deployment_envelope_b0_omits_buy_e3_extension(
    tmp_path: Path,
) -> None:
    _bundle, repository, native_receipt, model_authorization = (
        _deployment_envelope_fixture(tmp_path)
    )
    output = tmp_path / "deployment-envelope.json"

    result = subject.build_deployment_envelope(
        repository_root=repository,
        active_config_path=repository / "config.yaml",
        native_build_receipt_path=native_receipt,
        model_authorization_path=model_authorization,
        output_path=output,
    )

    envelope = result["envelope"]
    assert output.stat().st_mode & 0o777 == 0o600
    assert output.stat().st_nlink == 1
    assert result["canonical_sha256"] == envelope["canonical_sha256"]
    assert set(envelope) == {
        "schema_version",
        "status",
        "source",
        "build_bundle",
        "config_bundle",
        "model_policy_bundle",
        "policy_approvals",
        "canonical_sha256",
    }
    assert envelope["policy_approvals"] == []
    assert set(envelope["build_bundle"]) == {"manifest_path", "root_sha256"}
    assert set(envelope["config_bundle"]) == {"member_paths", "root_sha256"}
    assert set(envelope["model_policy_bundle"]) == {
        "member_paths",
        "root_sha256",
    }
    assert envelope["model_policy_bundle"]["member_paths"] == {
        "model_authorization": str(model_authorization.resolve())
    }
    assert all(
        "file_sha256" not in key and "canonical_sha256" not in key
        for bundle in (
            envelope["build_bundle"],
            envelope["config_bundle"],
            envelope["model_policy_bundle"],
        )
        for key in bundle
    )
    assert not ({"host", "account", "credential", "current_release"} & set(envelope))

    environment = {
        runtime_policy.DEPLOYMENT_ENVELOPE_PATH_ENV: str(output),
        runtime_policy.DEPLOYMENT_ENVELOPE_CANONICAL_SHA256_ENV: result["canonical_sha256"],
    }
    assert not hasattr(runtime_policy, "DEPLOYMENT_ENVELOPE_FILE_SHA256_ENV")
    authority = runtime_policy.deployment_envelope_runtime_authority(environ=environment)
    assert authority["execution_commit"] == envelope["source"]["commit"]
    assert authority["policy_approvals"] == []


def test_deployment_envelope_binds_canonical_policy_approvals_and_loads_legacy_empty(
    tmp_path: Path,
) -> None:
    _bundle, repository, native_receipt, model_authorization = (
        _deployment_envelope_fixture(tmp_path)
    )
    output = tmp_path / "deployment-envelope.json"
    result = subject.build_deployment_envelope(
        repository_root=repository,
        active_config_path=repository / "config.yaml",
        native_build_receipt_path=native_receipt,
        model_authorization_path=model_authorization,
        policy_approvals=("q90_action", "f05_boolean_cooldown"),
        output_path=output,
    )

    assert result["envelope"]["policy_approvals"] == [
        "f05_boolean_cooldown",
        "q90_action",
    ]
    authority = subject.load_deployment_envelope(
        output,
        expected_root_sha256=result["canonical_sha256"],
    )
    assert authority["policy_approvals"] == [
        "f05_boolean_cooldown",
        "q90_action",
    ]

    noncanonical = dict(result["envelope"])
    noncanonical["policy_approvals"] = list(
        reversed(noncanonical["policy_approvals"])
    )
    noncanonical[subject.DEPLOYMENT_ENVELOPE_CANONICAL_FIELD] = (
        subject.canonical_sha256(
            noncanonical,
            subject.DEPLOYMENT_ENVELOPE_CANONICAL_FIELD,
        )
    )
    noncanonical_path = tmp_path / "noncanonical-envelope.json"
    subject._write_json_authority(  # noqa: SLF001
        noncanonical_path,
        noncanonical,
    )
    with pytest.raises(subject.LockedRuntimeError, match="not canonical"):
        subject.load_deployment_envelope(
            noncanonical_path,
            expected_root_sha256=noncanonical[
                subject.DEPLOYMENT_ENVELOPE_CANONICAL_FIELD
            ],
        )

    with pytest.raises(subject.LockedRuntimeError, match="duplicated"):
        subject.build_deployment_envelope(
            repository_root=repository,
            active_config_path=repository / "config.yaml",
            native_build_receipt_path=native_receipt,
            model_authorization_path=model_authorization,
            policy_approvals=("q90_action", "q90_action"),
            output_path=tmp_path / "duplicate.json",
        )
    with pytest.raises(subject.LockedRuntimeError, match="unknown"):
        subject.build_deployment_envelope(
            repository_root=repository,
            active_config_path=repository / "config.yaml",
            native_build_receipt_path=native_receipt,
            model_authorization_path=model_authorization,
            policy_approvals=("unregistered_policy",),
            output_path=tmp_path / "unknown.json",
        )

    legacy = dict(result["envelope"])
    legacy.pop("policy_approvals")
    legacy[subject.DEPLOYMENT_ENVELOPE_CANONICAL_FIELD] = subject.canonical_sha256(
        legacy,
        subject.DEPLOYMENT_ENVELOPE_CANONICAL_FIELD,
    )
    legacy_path = tmp_path / "legacy-envelope.json"
    subject._write_json_authority(legacy_path, legacy)  # noqa: SLF001
    legacy_authority = subject.load_deployment_envelope(
        legacy_path,
        expected_root_sha256=legacy[
            subject.DEPLOYMENT_ENVELOPE_CANONICAL_FIELD
        ],
    )
    assert legacy_authority["policy_approvals"] == []


def test_native_build_bundle_rejects_non_live_or_skipped_qualification(
    tmp_path: Path,
) -> None:
    _bundle, repository, native_receipt, _model_authorization = (
        _deployment_envelope_fixture(tmp_path)
    )
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ("git", "rev-parse", "HEAD^{tree}"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    original = json.loads(native_receipt.read_text(encoding="utf-8"))
    missing_member_contract = subject.native_live_abi_contract_payload()
    missing_member_contract["required_class_members"][
        "NativeReplaceContinuationState"
    ].remove("telemetry")

    for index, (field, value, message) in enumerate((
        (
            "schema_version",
            "narrowgate_linux_x86_64_native_build_receipt.v2",
            "schema drifted",
        ),
        ("build_surface", {**original["build_surface"], "flavor": "full"}, "surface"),
        (
            "abi_contract",
            missing_member_contract,
            "ABI qualification drifted",
        ),
        (
            "parity_qualification",
            {
                **original["parity_qualification"],
                "passed": 0,
                "skipped": 1,
            },
            "did not pass exactly",
        ),
    )):
        changed = {**original, field: value}
        changed[subject.NATIVE_BUILD_RECEIPT_CANONICAL_FIELD] = subject.canonical_sha256(
            changed,
            subject.NATIVE_BUILD_RECEIPT_CANONICAL_FIELD,
        )
        changed_path = native_receipt.with_name(f"native-build-invalid-{index}.json")
        subject._write_json_authority(changed_path, changed)  # noqa: SLF001
        with pytest.raises(subject.LockedRuntimeError, match=message):
            subject._validate_native_build_bundle(  # noqa: SLF001
                changed_path,
                execution_commit=commit,
                execution_tree=tree,
            )


def test_startup_derives_runtime_leaves_from_one_envelope_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    (repository / "tracked.txt").write_text("fixture\n", encoding="utf-8")
    for command in (
        ("git", "init", "-q"),
        ("git", "config", "user.email", "fixture@example.invalid"),
        ("git", "config", "user.name", "Fixture"),
        ("git", "add", "tracked.txt"),
        ("git", "commit", "-q", "-m", "fixture"),
    ):
        subprocess.run(command, cwd=repository, check=True, timeout=30.0)
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ("git", "rev-parse", "HEAD^{tree}"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    runtime = tmp_path / "runtime"
    runtime.mkdir()
    install_receipt = runtime / "install-receipt.json"
    expected_venv = runtime / f"venv-{commit}"
    expected_python = expected_venv / "bin" / "python3"
    expected_python.parent.mkdir(parents=True)
    expected_python.write_bytes(b"fixture interpreter")
    expected_python.chmod(0o700)
    (repository / ".git" / "info" / "exclude").write_text(
        ".venv-active\n",
        encoding="utf-8",
    )
    (repository / ".venv-active").symlink_to(expected_venv)

    authority = {
        "canonical_sha256": "a" * 64,
        "execution_commit": commit,
        "execution_tree": tree,
        "install_receipt_path": str(install_receipt),
        "install_receipt_canonical_sha256": "b" * 64,
        "runtime_lock_canonical_sha256": "c" * 64,
        "wheelhouse_canonical_sha256": "d" * 64,
        "root_wheel_sha256": "e" * 64,
        "native_wheel_sha256": "f" * 64,
        "locked_runtime_interpreter": {
            "version": "3.12.0",
            "soabi": "cpython-312-x86_64-linux-gnu",
            "compiler": "fixture",
            "openssl_runtime": "fixture",
            "executable_sha256": "1" * 64,
        },
    }
    observed: dict[str, Any] = {}

    def fake_load(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        observed["load"] = kwargs
        return authority

    def fake_validate(**kwargs: Any) -> dict[str, Any]:
        observed["validate"] = kwargs
        return {"status": "offline_exact_install_passed"}

    monkeypatch.setattr(subject, "load_deployment_envelope", fake_load)
    monkeypatch.setattr(subject, "validate_startup_runtime", fake_validate)
    result = subject.validate_deployment_envelope_startup(
        repository_root=repository,
        envelope_path=tmp_path / "deployment-envelope.json",
        expected_envelope_sha256="a" * 64,
        venv_python=repository / ".venv-active" / "bin" / "python3",
        pip_runner_python=BUILDER_PYTHON,
    )

    assert result["canonical_sha256"] == "a" * 64
    assert observed["load"] == {
        "expected_root_sha256": "a" * 64,
    }
    assert observed["validate"]["receipt_path"] == install_receipt
    assert observed["validate"]["expected_receipt_sha256"] == "b" * 64
    assert observed["validate"]["expected_lock_sha256"] == "c" * 64
    assert observed["validate"]["expected_wheelhouse_sha256"] == "d" * 64
    assert observed["validate"]["expected_root_wheel_sha256"] == "e" * 64
    assert observed["validate"]["expected_native_wheel_sha256"] == "f" * 64


def test_build_deployment_envelope_policy_extension_is_all_or_none(
    tmp_path: Path,
) -> None:
    _bundle, repository, native_receipt, model_authorization = (
        _deployment_envelope_fixture(tmp_path)
    )
    policy = tmp_path / "policy.json"
    policy.write_text("{}\n", encoding="utf-8")

    with pytest.raises(subject.LockedRuntimeError, match="all-or-none"):
        subject.build_deployment_envelope(
            repository_root=repository,
            active_config_path=repository / "config.yaml",
            native_build_receipt_path=native_receipt,
            model_authorization_path=model_authorization,
            policy_file_path=policy,
            output_path=tmp_path / "deployment-envelope.json",
        )

    boolean_policy = tmp_path / "boolean-policy.json"
    boolean_bundle = tmp_path / "boolean-predicate-bundle.json"
    boolean_policy.write_text('{"policy":"fixture"}\n', encoding="utf-8")
    boolean_bundle.write_text('{"bundle":"fixture"}\n', encoding="utf-8")
    with pytest.raises(subject.LockedRuntimeError, match="all-or-none"):
        subject.build_deployment_envelope(
            repository_root=repository,
            active_config_path=repository / "config.yaml",
            native_build_receipt_path=native_receipt,
            model_authorization_path=model_authorization,
            boolean_policy_file_path=boolean_policy,
            output_path=tmp_path / "incomplete-boolean-envelope.json",
        )

    artifact_manifest = tmp_path / "artifact-manifest.json"
    predicate_bundle = tmp_path / "predicate-bundle.json"
    artifact_manifest.write_text('{"artifact":"fixture"}\n', encoding="utf-8")
    predicate_bundle.write_text('{"predicate":"fixture"}\n', encoding="utf-8")
    output = tmp_path / "complete-deployment-envelope.json"
    result = subject.build_deployment_envelope(
        repository_root=repository,
        active_config_path=repository / "config.yaml",
        native_build_receipt_path=native_receipt,
        model_authorization_path=model_authorization,
        boolean_policy_file_path=boolean_policy,
        boolean_predicate_bundle_path=boolean_bundle,
        policy_artifact_manifest_path=artifact_manifest,
        policy_file_path=policy,
        predicate_bundle_path=predicate_bundle,
        output_path=output,
    )
    members = result["envelope"]["model_policy_bundle"]["member_paths"]
    assert set(members) == {
        "artifact_manifest",
        "boolean_policy",
        "boolean_predicate_bundle",
        "model_authorization",
        "policy",
        "predicate_bundle",
    }
    environment = {
        runtime_policy.DEPLOYMENT_ENVELOPE_PATH_ENV: str(output),
        runtime_policy.DEPLOYMENT_ENVELOPE_CANONICAL_SHA256_ENV: result["canonical_sha256"],
    }
    authority = runtime_policy.deployment_envelope_runtime_authority(environ=environment)
    assert authority["canonical_sha256"] == result["canonical_sha256"]
    assert set(authority["model_policy_member_paths"]) == set(members)
    assert set(authority["model_policy_member_sha256"]) == set(members)
    policy.write_text('{"changed":true}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="model-policy bundle root drifted"):
        runtime_policy.deployment_envelope_runtime_authority(environ=environment)


def test_compact_activation_receipt_and_current_pointer_bind_only_roots(
    tmp_path: Path,
) -> None:
    _bundle, repository, native_receipt, model_authorization = (
        _deployment_envelope_fixture(tmp_path)
    )
    envelope_path = tmp_path / "deployment-envelope.json"
    envelope = subject.build_deployment_envelope(
        repository_root=repository,
        active_config_path=repository / "config.yaml",
        native_build_receipt_path=native_receipt,
        model_authorization_path=model_authorization,
        output_path=envelope_path,
    )
    release_id = "release-20260830"
    reconciliation_path, reconciliation_root, runtime_path, runtime_sha256 = (
        _activation_artifacts(
            tmp_path,
            envelope_sha256=envelope["canonical_sha256"],
        )
    )
    receipt_path = tmp_path / "active-receipt.json"
    receipt_result = subject.build_activation_receipt(
        release_id=release_id,
        deployment_envelope_path=envelope_path,
        deployment_envelope_sha256=envelope["canonical_sha256"],
        stopped_reconciliation_path=reconciliation_path,
        stopped_reconciliation_sha256=reconciliation_root,
        runtime_identity_path=runtime_path,
        output_path=receipt_path,
    )
    receipt = receipt_result["receipt"]
    assert set(receipt) == {
        "schema_version",
        "release_id",
        "status",
        "deployment_envelope_sha256",
        "stopped_reconciliation_sha256",
        "runtime_identity_sha256",
        "canonical_sha256",
    }
    assert receipt["runtime_identity_sha256"] == runtime_sha256
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    assert receipt_path.stat().st_nlink == 1
    assert (
        subject.load_activation_receipt(
            receipt_path,
            expected_root_sha256=receipt_result["canonical_sha256"],
            expected_deployment_envelope_sha256=envelope["canonical_sha256"],
            expected_release_id=release_id,
        )
        == receipt
    )

    pointer_path = tmp_path / "live.current.json"
    published = subject.publish_current_pointer(
        release_id=release_id,
        deployment_envelope_path=envelope_path,
        deployment_envelope_sha256=envelope["canonical_sha256"],
        activation_receipt_path=receipt_path,
        activation_receipt_sha256=receipt_result["canonical_sha256"],
        stopped_reconciliation_path=reconciliation_path,
        runtime_identity_path=runtime_path,
        output_path=pointer_path,
    )
    pointer = published["pointer"]
    assert pointer == {
        "schema_version": subject.CURRENT_POINTER_SCHEMA,
        "release_id": release_id,
        "activation_receipt_sha256": receipt_result["canonical_sha256"],
        "status": subject.CURRENT_POINTER_STATUS,
    }
    assert "canonical_sha256" not in pointer
    assert pointer["status"] == "selected_activation"
    assert stat.S_IMODE(pointer_path.stat().st_mode) == 0o600
    assert pointer_path.stat().st_nlink == 1
    assert (
        subject.load_current_pointer(
            pointer_path,
            deployment_envelope_path=envelope_path,
            activation_receipt_path=receipt_path,
        )["pointer"]
        == pointer
    )
    legacy_pointer = {
        "schema_version": subject.LEGACY_CURRENT_POINTER_SCHEMA,
        "release_id": release_id,
        "deployment_envelope_sha256": envelope["canonical_sha256"],
        "activation_receipt_sha256": receipt_result["canonical_sha256"],
        "status": subject.CURRENT_POINTER_STATUS,
    }
    legacy_pointer_path = tmp_path / "legacy-live.current.json"
    subject._write_json_authority(legacy_pointer_path, legacy_pointer)  # noqa: SLF001
    assert (
        subject.load_current_pointer(
            legacy_pointer_path,
            deployment_envelope_path=envelope_path,
            activation_receipt_path=receipt_path,
        )["pointer"]
        == legacy_pointer
    )
    wrong_legacy_pointer = {
        **legacy_pointer,
        "deployment_envelope_sha256": "0" * 64,
    }
    wrong_legacy_pointer_path = tmp_path / "wrong-legacy-live.current.json"
    subject._write_json_authority(  # noqa: SLF001
        wrong_legacy_pointer_path,
        wrong_legacy_pointer,
    )
    with pytest.raises(
        subject.LockedRuntimeError,
        match="current pointer deployment envelope lineage drifted",
    ):
        subject.load_current_pointer(
            wrong_legacy_pointer_path,
            deployment_envelope_path=envelope_path,
            activation_receipt_path=receipt_path,
        )
    wrong_receipt = dict(receipt)
    wrong_receipt["deployment_envelope_sha256"] = "0" * 64
    wrong_receipt[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD] = subject.canonical_sha256(
        wrong_receipt,
        subject.ACTIVATION_RECEIPT_CANONICAL_FIELD,
    )
    wrong_receipt_path = tmp_path / "wrong-envelope-receipt.json"
    subject._write_json_authority(wrong_receipt_path, wrong_receipt)  # noqa: SLF001
    wrong_pointer = {
        **pointer,
        "activation_receipt_sha256": wrong_receipt[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD],
    }
    wrong_pointer_path = tmp_path / "wrong-envelope-current.json"
    subject._write_json_authority(wrong_pointer_path, wrong_pointer)  # noqa: SLF001
    with pytest.raises(subject.LockedRuntimeError, match="deployment release root drifted"):
        subject.load_current_pointer(
            wrong_pointer_path,
            deployment_envelope_path=envelope_path,
            activation_receipt_path=wrong_receipt_path,
        )
    model_authorization.write_text('{"changed":true}\n', encoding="utf-8")
    assert (
        subject.load_current_pointer(
            pointer_path,
            deployment_envelope_path=envelope_path,
            activation_receipt_path=receipt_path,
        )["pointer"]
        == pointer
    )
    subject._write_json_pointer_atomic(pointer_path, pointer)  # noqa: SLF001
    assert json.loads(pointer_path.read_bytes()) == pointer
    assert not list(tmp_path.glob(".live.current.json.tmp.*"))


def test_activation_receipt_rejects_unbound_or_changed_runtime_artifacts(
    tmp_path: Path,
) -> None:
    _bundle, repository, native_receipt, model_authorization = (
        _deployment_envelope_fixture(tmp_path)
    )
    envelope_path = tmp_path / "deployment-envelope.json"
    envelope = subject.build_deployment_envelope(
        repository_root=repository,
        active_config_path=repository / "config.yaml",
        native_build_receipt_path=native_receipt,
        model_authorization_path=model_authorization,
        output_path=envelope_path,
    )
    reconciliation_path, reconciliation_root, runtime_path, _runtime_sha256 = (
        _activation_artifacts(
            tmp_path,
            envelope_sha256=envelope["canonical_sha256"],
        )
    )
    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    runtime["startup_attestation"]["deployment_envelope"]["canonical_sha256"] = "f" * 64
    _write_private_json(runtime_path, runtime)
    with pytest.raises(subject.LockedRuntimeError, match="runtime identity authority drifted"):
        subject.build_activation_receipt(
            release_id="release-a",
            deployment_envelope_path=envelope_path,
            deployment_envelope_sha256=envelope["canonical_sha256"],
            stopped_reconciliation_path=reconciliation_path,
            stopped_reconciliation_sha256=reconciliation_root,
            runtime_identity_path=runtime_path,
            output_path=tmp_path / "rejected-receipt.json",
        )

    reconciliation_path, reconciliation_root, runtime_path, _runtime_sha256 = (
        _activation_artifacts(
            tmp_path,
            envelope_sha256=envelope["canonical_sha256"],
        )
    )
    receipt_path = tmp_path / "active-receipt.json"
    receipt = subject.build_activation_receipt(
        release_id="release-a",
        deployment_envelope_path=envelope_path,
        deployment_envelope_sha256=envelope["canonical_sha256"],
        stopped_reconciliation_path=reconciliation_path,
        stopped_reconciliation_sha256=reconciliation_root,
        runtime_identity_path=runtime_path,
        output_path=receipt_path,
    )
    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    runtime["pid"] = 999
    _write_private_json(runtime_path, runtime)
    with pytest.raises(subject.LockedRuntimeError, match="runtime identity file hash drifted"):
        subject.publish_current_pointer(
            release_id="release-a",
            deployment_envelope_path=envelope_path,
            deployment_envelope_sha256=envelope["canonical_sha256"],
            activation_receipt_path=receipt_path,
            activation_receipt_sha256=receipt["canonical_sha256"],
            stopped_reconciliation_path=reconciliation_path,
            runtime_identity_path=runtime_path,
            output_path=tmp_path / "current.json",
        )


def test_compact_receipt_and_pointer_reject_ambiguous_or_broken_lineage(
    tmp_path: Path,
) -> None:
    receipt_path = tmp_path / "active-receipt.json"
    payload: dict[str, Any] = {
        "schema_version": subject.ACTIVATION_RECEIPT_SCHEMA,
        "release_id": "release-a",
        "status": subject.ACTIVATION_RECEIPT_STATUS,
        "deployment_envelope_sha256": "1" * 64,
        "stopped_reconciliation_sha256": "2" * 64,
        "runtime_identity_sha256": "3" * 64,
    }
    payload[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD] = subject.canonical_sha256(
        payload,
        subject.ACTIVATION_RECEIPT_CANONICAL_FIELD,
    )
    subject._write_json_authority(receipt_path, payload)  # noqa: SLF001
    assert subject.load_activation_receipt(
        receipt_path,
        expected_root_sha256=payload[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD],
        expected_deployment_envelope_sha256="1" * 64,
        expected_release_id="release-a",
    )["runtime_identity_sha256"] == "3" * 64
    with pytest.raises(subject.LockedRuntimeError, match="deployment envelope lineage"):
        subject.load_activation_receipt(
            receipt_path,
            expected_root_sha256=payload[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD],
            expected_deployment_envelope_sha256="5" * 64,
            expected_release_id="release-a",
        )
    with pytest.raises(subject.LockedRuntimeError, match="expected deployment envelope"):
        subject.load_activation_receipt(
            receipt_path,
            expected_root_sha256=payload[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD],
            expected_deployment_envelope_sha256=None,  # type: ignore[arg-type]
            expected_release_id="release-a",
        )

    ambiguous = dict(payload)
    ambiguous["copied_leaf_sha256"] = "6" * 64
    ambiguous[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD] = subject.canonical_sha256(
        ambiguous,
        subject.ACTIVATION_RECEIPT_CANONICAL_FIELD,
    )
    ambiguous_path = tmp_path / "ambiguous-receipt.json"
    subject._write_json_authority(ambiguous_path, ambiguous)  # noqa: SLF001
    with pytest.raises(subject.LockedRuntimeError, match="fields drifted"):
        subject.load_activation_receipt(
            ambiguous_path,
            expected_root_sha256=ambiguous[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD],
            expected_deployment_envelope_sha256="1" * 64,
            expected_release_id="release-a",
        )

    pointer = {
        "schema_version": subject.CURRENT_POINTER_SCHEMA,
        "release_id": "release-a",
        "activation_receipt_sha256": payload[subject.ACTIVATION_RECEIPT_CANONICAL_FIELD],
        "status": subject.CURRENT_POINTER_STATUS,
    }
    with pytest.raises(subject.LockedRuntimeError, match="current pointer fields drifted"):
        subject._validate_current_pointer_payload(  # noqa: SLF001
            {**pointer, "deployment_envelope_sha256": "7" * 64}
        )
    with pytest.raises(subject.LockedRuntimeError, match="lowercase SHA256"):
        subject._validate_current_pointer_payload(  # noqa: SLF001
            {**pointer, "activation_receipt_sha256": int("1" * 64)}
        )
    destination = tmp_path / "current.json"
    outside = tmp_path / "outside.json"
    outside.write_text("outside\n", encoding="utf-8")
    outside.chmod(0o600)
    destination.symlink_to(outside)
    with pytest.raises(subject.LockedRuntimeError, match="symlink"):
        subject._write_json_pointer_atomic(destination, pointer)  # noqa: SLF001
    assert outside.read_text(encoding="utf-8") == "outside\n"


def _prepared_activation_args() -> dict[str, Any]:
    return {
        "release_id": "release-a",
        "release_dir": "/srv/narrowgate/releases/release-a",
        "previous_release_dir": "/srv/narrowgate/releases/release-old",
        "private_environment_file": "/srv/narrowgate/private/live.env",
        "active_config_path": "/srv/narrowgate/private/config.yaml",
        "deployment_envelope_path": "/srv/narrowgate/private/envelope.json",
        "deployment_envelope_sha256": "1" * 64,
        "trusted_python_path": "/srv/narrowgate/runtime/bin/python",
        "stopped_reconciliation_path": "/srv/narrowgate/private/reconcile.json",
        "activation_receipt_path": "/srv/narrowgate/private/activation.json",
        "current_pointer_path": "/srv/narrowgate/private/current.json",
    }


def test_prepared_activation_is_dry_run_by_default_and_has_fixed_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shell = source_deploy.render_prepared_release_activation_shell(
        **_prepared_activation_args()
    )
    rendered_argv = shlex.split(shell)
    subprocess.run(
        ("bash", "-n", "-c", rendered_argv[-1]),
        check=True,
        capture_output=True,
        text=True,
    )
    shell = rendered_argv[-1]
    monkeypatch.setattr(
        source_deploy.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("dry-run must not open SSH"),
    )
    result = source_deploy.activate_prepared_release(
        target="example.test",
        **_prepared_activation_args(),
    )
    assert result["status"] == "planned"
    assert result["phases"] == list(source_deploy.PREPARED_ACTIVATION_PHASES)
    verify = shell.index('"$release/live/run.sh" candidate-verify')
    old_pid = shell.index('old_pid="$(systemctl show', verify)
    stop = shell.index("sudo systemctl stop narrowgate.service", old_pid)
    reconcile = shell.index('"$release/live/run.sh" reconcile-stopped', stop)
    cleanup_arm = shell.index("cleanup_required=1", reconcile)
    start = shell.index("--service-type=simple --unit=narrowgate", cleanup_arm)
    admitted = shell.index('test "$observations" -ge 2', start)
    receipt = shell.index("build-activation-receipt", admitted)
    critical = shell.index("trap '' HUP INT TERM", receipt)
    publish = shell.index("publish-current-pointer", critical)
    pointer_verify = shell.index("load_current_pointer", publish)
    post_check = shell.index("post_health_values=", pointer_verify)
    final_publish = shell.index('mv -f -- "$pointer_stage" "$current"', post_check)
    committed = shell.index("pointer_committed=1", final_publish)
    cleanup_disarm = shell.index("cleanup_required=0", committed)
    parent_fsync = shell.index('if ! "$trusted" -c', cleanup_disarm)
    trap_restore = shell.index("trap 'exit 84' HUP INT TERM", parent_fsync)
    assert [
        verify,
        old_pid,
        stop,
        reconcile,
        cleanup_arm,
        start,
        admitted,
        receipt,
        critical,
        publish,
        pointer_verify,
        post_check,
        final_publish,
        committed,
        cleanup_disarm,
        parent_fsync,
        trap_restore,
    ] == sorted(
        (
            verify,
            old_pid,
            stop,
            reconcile,
            cleanup_arm,
            start,
            admitted,
            receipt,
            critical,
            publish,
            pointer_verify,
            post_check,
            final_publish,
            committed,
            cleanup_disarm,
            parent_fsync,
            trap_restore,
        )
    )
    pre_stop = shell[verify:stop]
    assert 'ActiveState --value)" = active' in pre_stop
    assert 'SubState --value)" = running' in pre_stop
    assert 'Transient --value)" = yes' in pre_stop
    assert 'WorkingDirectory --value)" = "$previous"' in pre_stop
    assert 'process_matches "$old_pid" "$previous"' in pre_stop
    assert 'test "$unit_cwd" = "$release"' in shell[:verify]
    assert 'rm -f -- "$pointer_stage"' in shell[:verify]
    assert 'test "$health" -nt "$start_marker"' in shell[start:receipt]
    assert "recordedAtNs" in shell
    assert "reconciliationPending" in shell
    assert "lastTickAge" in shell and "math.isfinite(age)" in shell
    assert "0<=age<=1.0" in shell
    assert "userStreamConnected" in shell and "userStreamGeneration" in shell
    assert 'test "$user_generation" != "$observed_user_generation"' in shell
    assert 'test "$observed_user_generation" = "$user_generation"' in shell
    assert 'test "$post_user_generation" = "$user_generation"' in shell
    assert 'test "$cleanup_required" = 1 && test "$pointer_committed" = 0' in shell
    assert "pointer commit uncertain; candidate remains running" in shell
    assert '[[ "$reconciliation_sha" =~ ^[0-9a-f]{64}$ ]]' in shell
    assert shell.count('candidate_unit_matches "$candidate_pid"') >= 4
    lock_check = shell.index('canonical_output "$current"')
    lock_open = shell.index('exec 9>>"$lock"')
    assert lock_check < shell.index('private_parent "$lock"') < lock_open
    assert shell.index("set -o noclobber", lock_check) < lock_open
    assert 'test -f "$lock"' in shell[lock_check:lock_open]
    assert "/proc/$$/fd/9" in shell[lock_open:start]
    assert 'rm -f -- "$lock"' not in shell
    marker_check = shell.index('canonical_output "$start_marker"')
    marker_open = shell.index(': >"$start_marker"')
    assert marker_check < shell.index("set -o noclobber", marker_check) < marker_open
    assert marker_check < shell.index('private_parent "$start_marker"') < marker_open
    assert '--output "$pointer_stage"' in shell[publish:post_check]
    assert "activation_receipt_sha256" in shell[pointer_verify:post_check]
    assert "os.fsync(fd)" in shell[parent_fsync:trap_restore]
    assert "load_current_pointer" not in shell[final_publish:cleanup_disarm]
    assert "start narrowgate.service" not in shell
    assert "rollback" not in shell
    assert "pgrep -f" not in shell
    assert '/proc/{entry.name}/cmdline' in shell
    assert 'arg.endswith(b"/live/main.py")' in shell
    assert "activation failed phase=%s line=%s rc=%s" in shell


@pytest.mark.parametrize(
    "metadata_case",
    ("regular", "missing", "symlink", "file_alias", "parent_alias", "sudo_denied"),
)
def test_prepared_activation_checks_root_private_environment_metadata(
    metadata_case: str,
) -> None:
    rendered = shlex.split(
        source_deploy.render_prepared_release_activation_shell(**_prepared_activation_args())
    )[-1]
    helper = rendered.split("canonical_environment_input() {", 1)[1].split("\n}", 1)[0]
    assert rendered.count('canonical_environment_input "$env_file"') == 2
    assert 'canonical_input "$env_file"' not in rendered
    assert 'test -f "$env_file"' not in rendered
    # This virtual root-private path cannot be stat'ed by the operator. Only
    # the narrow sudo metadata calls may supply its identity; no content read.
    script = r'''
set -e
metadata_case="$1"
env_file=/unavailable-root-private/live.env
sudo() {
  test "$1" = -n || return 91
  shift
  test "$metadata_case" != sudo_denied || return 1
  case "$1:$2" in
    test:-f) test "$metadata_case" != missing ;;
    test:!) test "$3" = -L && test "$metadata_case" != symlink ;;
    readlink:-f)
      test "$3" = -- || return 92
      if test "$metadata_case" = file_alias && test "$4" = "$env_file"; then
        printf '%s\n' /aliased/live.env
      elif test "$metadata_case" = parent_alias && test "$4" != "$env_file"; then
        printf '%s\n' /aliased
      else
        printf '%s\n' "$4"
      fi ;;
    *) return 93 ;;
  esac
}
canonical_environment_input() {
''' + helper + '\n}\ncanonical_environment_input "$env_file"\n'
    result = subprocess.run(
        ("bash", "-c", script, "environment-metadata-test", metadata_case),
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is (metadata_case == "regular"), result.stderr


def test_prepared_activation_can_resume_only_from_proven_stopped_previous() -> None:
    shell = source_deploy.render_prepared_release_activation_shell(
        **_prepared_activation_args(),
        resume_stopped=True,
    )
    rendered = shlex.split(shell)[-1]
    subprocess.run(
        ("bash", "-n", "-c", rendered),
        check=True,
        capture_output=True,
        text=True,
    )
    resume = rendered.index('if test "$resume_stopped" = 1')
    runtime_fatal = rendered.index(
        'elif test "$recover_runtime_fatal" = 1',
        resume,
    )
    reconcile = rendered.index('phase=fresh_reconcile', resume)
    resume_block = rendered[resume:runtime_fatal]
    assert "unit_inactive_or_absent" in resume_block
    assert 'case "$load" in loaded|not-found)' in rendered
    assert 'test "$state" = inactive' in rendered
    assert 'test "$substate" = dead' in rendered
    assert 'test "$pid" = 0' in rendered
    assert "|| true" not in rendered[
        rendered.index("unit_inactive_or_absent()") : rendered.index("cleanup()")
    ]
    assert 'canonical_input "$current"' in resume_block
    assert "narrowgate_live_current_pointer.v2" in resume_block
    assert "selected_activation" in resume_block
    assert "release_id" in resume_block
    assert '"$current" "$previous_release_id"' in resume_block
    assert "journalctl" not in resume_block
    assert "runtime_health" not in resume_block
    assert runtime_fatal < reconcile


def test_prepared_activation_recovers_only_from_bound_runtime_fatal_exit() -> None:
    shell = source_deploy.render_prepared_release_activation_shell(
        **_prepared_activation_args(),
        recover_runtime_fatal=True,
        previous_deployment_envelope_path="/srv/narrowgate/private/old-envelope.json",
        previous_activation_receipt_path="/srv/narrowgate/private/old-activation.json",
        previous_stopped_reconciliation_path="/srv/narrowgate/private/old-reconcile.json",
    )
    rendered = shlex.split(shell)[-1]
    subprocess.run(
        ("bash", "-n", "-c", rendered),
        check=True,
        capture_output=True,
        text=True,
    )
    fatal = rendered.index('elif test "$recover_runtime_fatal" = 1')
    proof = rendered.index("phase=runtime_fatal_proof", fatal)
    reconcile = rendered.index("phase=fresh_reconcile", proof)
    fatal_block = rendered[fatal:reconcile]
    assert "unit_inactive_or_absent" in fatal_block
    assert fatal_block.index("quiescent") < fatal_block.index("phase=runtime_fatal_proof")
    assert "load_current_pointer" in fatal_block
    assert "_validate_activation_artifacts" in fatal_block
    assert "fatalRuntimeLatched" in fatal_block
    assert "reconciliationRequired" in fatal_block
    assert "quoteLoopRunning" in fatal_block
    assert "runtime_health.v1" in fatal_block
    assert "operator-gated reconciliation" in fatal_block
    assert "_SYSTEMD_INVOCATION_ID" in fatal_block
    assert "INVOCATION_ID" in fatal_block
    assert "EXIT_CODE" in fatal_block
    assert "EXIT_STATUS" in fatal_block
    assert "'78'" in fatal_block
    assert '"_PID=$previous_pid" --since="$previous_since" --until=now' in fatal_block
    assert '"INVOCATION_ID=$fatal_invocation"' in fatal_block
    assert fatal_block.count('"$trusted" -I -B -c') == 3
    assert "records=[]" not in fatal_block
    absence_guard = rendered.index('test ! -e "$reconciliation" && test ! -e "$activation"')
    assert absence_guard < proof < reconcile


def test_runtime_fatal_recovery_proves_exact_lineage_health_and_journal(
    tmp_path: Path,
) -> None:
    _bundle, repository, native_receipt, model_authorization = (
        _deployment_envelope_fixture(tmp_path)
    )
    envelope_path = tmp_path / "deployment-envelope.json"
    envelope = subject.build_deployment_envelope(
        repository_root=repository,
        active_config_path=repository / "config.yaml",
        native_build_receipt_path=native_receipt,
        model_authorization_path=model_authorization,
        output_path=envelope_path,
    )
    reconciliation_path, reconciliation_root, runtime_path, _runtime_sha256 = (
        _activation_artifacts(
            tmp_path,
            envelope_sha256=envelope["canonical_sha256"],
        )
    )
    release_id = "release-old"
    release_dir = tmp_path / release_id
    (release_dir / "live").mkdir(parents=True)
    main_path = release_dir / "live" / "main.py"
    main_path.write_text("# fixture\n", encoding="utf-8")
    config_path = repository / "config.yaml"
    pid = 191735
    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    runtime.update(
        {
            "pid": pid,
            "recorded_at_utc": "2026-09-03T11:25:25.624720+00:00",
            "config_path": str(config_path.resolve()),
        }
    )
    _write_private_json(runtime_path, runtime)
    activation_path = tmp_path / "activation.json"
    activation = subject.build_activation_receipt(
        release_id=release_id,
        deployment_envelope_path=envelope_path,
        deployment_envelope_sha256=envelope["canonical_sha256"],
        stopped_reconciliation_path=reconciliation_path,
        stopped_reconciliation_sha256=reconciliation_root,
        runtime_identity_path=runtime_path,
        output_path=activation_path,
    )
    current_path = tmp_path / "current.json"
    subject.publish_current_pointer(
        release_id=release_id,
        deployment_envelope_path=envelope_path,
        deployment_envelope_sha256=envelope["canonical_sha256"],
        activation_receipt_path=activation_path,
        activation_receipt_sha256=activation["canonical_sha256"],
        stopped_reconciliation_path=reconciliation_path,
        runtime_identity_path=runtime_path,
        output_path=current_path,
    )
    health_path = tmp_path / "runtime-health.json"
    health = {
        "schemaVersion": "narrowgate.live_runtime_health.v1",
        "recordedAtNs": 1788444996775502889,
        "pid": pid,
        "quoteLoopRunning": False,
        "fatalRuntimeLatched": True,
        "reconciliationRequired": True,
        "reconciliationPending": False,
        "fatalReason": "EXACT_EXECUTION_RECONCILIATION_FAILED",
    }
    _write_private_json(health_path, health)
    lineage = subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_LINEAGE_CHECK,  # noqa: SLF001
            str(Path(subject.__file__).resolve()),
            str(current_path),
            str(envelope_path),
            str(activation_path),
            str(reconciliation_path),
            str(runtime_path),
            str(health_path),
            release_id,
        ),
        check=True,
        capture_output=True,
        text=True,
    )
    observed_pid, since, health_sha256, config_token = lineage.stdout.strip().split("\t")
    assert observed_pid == str(pid)
    assert since == "2026-09-03 11:25:25 UTC"
    assert health_sha256 == hashlib.sha256(health_path.read_bytes()).hexdigest()
    assert base64.urlsafe_b64decode(config_token).decode() == str(config_path.resolve())

    invocation = "3c493119689c4935a055481a7e673ef5"
    fatal_record = {
        "_PID": str(pid),
        "_SYSTEMD_UNIT": "narrowgate.service",
        "_SYSTEMD_INVOCATION_ID": invocation,
        "_CMDLINE": (
            f"/runtime/bin/python -I -B {main_path} --config {config_path.resolve()}"
        ),
        "__REALTIME_TIMESTAMP": "1788444996829935",
        "MESSAGE": (
            "2026-09-03 14:16:36 [main] CRITICAL Execution state is uncertain "
            "at shutdown; exiting 78 for operator-gated reconciliation "
            "(reason=EXACT_EXECUTION_RECONCILIATION_FAILED pending=0)"
        ),
    }
    fatal_input = json.dumps({"MESSAGE": "irrelevant"}) + "\n" + json.dumps(fatal_record) + "\n"
    message = subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_MESSAGE_CHECK,  # noqa: SLF001
            str(pid),
            str(main_path),
            config_token,
            str(health_path),
            health_sha256,
        ),
        input=fatal_input,
        check=True,
        capture_output=True,
        text=True,
    )
    assert message.stdout.strip() == invocation
    duplicate = subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_MESSAGE_CHECK,  # noqa: SLF001
            str(pid),
            str(main_path),
            config_token,
            str(health_path),
            health_sha256,
        ),
        input=json.dumps(fatal_record) + "\n" + json.dumps(fatal_record) + "\n",
        check=False,
        capture_output=True,
        text=True,
    )
    # CRITICAL logs also use the synchronous stderr fallback: two copies of
    # the same process/invocation/reason are not two different failures.
    assert duplicate.returncode == 0
    assert duplicate.stdout.strip() == invocation
    conflict = {**fatal_record, "_SYSTEMD_INVOCATION_ID": "b" * 32}
    conflicting = subprocess.run(
        (sys.executable, "-c", source_deploy._RUNTIME_FATAL_MESSAGE_CHECK,
         str(pid), str(main_path), config_token, str(health_path), health_sha256),
        input=json.dumps(fatal_record) + "\n" + json.dumps(conflict) + "\n",
        check=False, capture_output=True, text=True,
    )
    assert conflicting.returncode != 0

    stale_fatal_record = {
        **fatal_record,
        "__REALTIME_TIMESTAMP": str((health["recordedAtNs"] - 1) // 1000),
    }
    rejected_stale_message = subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_MESSAGE_CHECK,  # noqa: SLF001
            str(pid),
            str(main_path),
            config_token,
            str(health_path),
            health_sha256,
        ),
        input=json.dumps(stale_fatal_record) + "\n",
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected_stale_message.returncode != 0

    exit_record = {
        "UNIT": "narrowgate.service",
        "INVOCATION_ID": invocation,
        "COMMAND": "ExecStart",
        "EXIT_CODE": "exited",
        "EXIT_STATUS": "78",
        "MESSAGE_ID": "98e322203f7a4ed290d09fe03c09fe15",
        "_PID": "1",
        "_UID": "0",
        "_COMM": "systemd",
        "_EXE": "/usr/lib/systemd/systemd",
    }
    subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_EXIT_CHECK,  # noqa: SLF001
            invocation,
        ),
        input=json.dumps(exit_record) + "\n",
        check=True,
        capture_output=True,
        text=True,
    )
    wrong_exit = {**exit_record, "EXIT_STATUS": "1"}
    rejected = subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_EXIT_CHECK,  # noqa: SLF001
            invocation,
        ),
        input=json.dumps(wrong_exit) + "\n",
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0

    forged_exit = {
        **exit_record,
        "_PID": str(pid),
        "_UID": "1000",
        "_COMM": "python3",
        "_EXE": "/runtime/bin/python",
    }
    rejected_forgery = subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_EXIT_CHECK,  # noqa: SLF001
            invocation,
        ),
        input=json.dumps(forged_exit) + "\n",
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected_forgery.returncode != 0

    _write_private_json(health_path, {**health, "pid": pid + 1})
    rejected_health = subprocess.run(
        (
            sys.executable,
            "-c",
            source_deploy._RUNTIME_FATAL_LINEAGE_CHECK,  # noqa: SLF001
            str(Path(subject.__file__).resolve()),
            str(current_path),
            str(envelope_path),
            str(activation_path),
            str(reconciliation_path),
            str(runtime_path),
            str(health_path),
            release_id,
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected_health.returncode != 0

    # A failed health writer leaves the last periodic (healthy) snapshot.
    # Recovery still needs the exact process's publication-failure and exit-78
    # journal proof; a stale snapshot or unrelated journal row cannot admit it.
    _write_private_json(health_path, {
        **health, "fatalRuntimeLatched": False, "reconciliationRequired": False,
        "quoteLoopRunning": True, "fatalReason": "",
    })
    health_sha256 = hashlib.sha256(health_path.read_bytes()).hexdigest()
    publication_failure = {
        **fatal_record,
        "MESSAGE": "CRITICAL Final runtime health publication failed: writer failed",
        "__REALTIME_TIMESTAMP": "1788444996800000",
    }
    for failure, expected in (
        (None, False),
        (publication_failure, True),
        ({**publication_failure, "_PID": str(pid + 1)}, False),
        ({**publication_failure, "_SYSTEMD_INVOCATION_ID": "a" * 32}, False),
        ({**publication_failure, "__REALTIME_TIMESTAMP": "1788444996900000"}, False),
    ):
        rows = [fatal_record] if failure is None else [failure, fatal_record]
        result = subprocess.run(
            (sys.executable, "-c", source_deploy._RUNTIME_FATAL_MESSAGE_CHECK,
             str(pid), str(main_path), config_token, str(health_path), health_sha256),
            input="".join(json.dumps(row) + "\n" for row in rows),
            check=False, capture_output=True, text=True,
        )
        assert (result.returncode == 0) is expected, result.stderr
        if expected:
            assert result.stdout.strip() == invocation

    delayed_duplicate = {
        **publication_failure, "__REALTIME_TIMESTAMP": "1788444996900000",
    }
    result = subprocess.run(
        (sys.executable, "-c", source_deploy._RUNTIME_FATAL_MESSAGE_CHECK,
         str(pid), str(main_path), config_token, str(health_path), health_sha256),
        input="".join(json.dumps(row) + "\n" for row in (
            publication_failure, fatal_record, delayed_duplicate,
        )),
        check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == invocation


def test_runtime_fatal_recovery_evidence_is_mode_scoped_and_never_reused() -> None:
    args = _prepared_activation_args()
    evidence = {
        "previous_deployment_envelope_path": "/srv/narrowgate/private/old-envelope.json",
        "previous_activation_receipt_path": "/srv/narrowgate/private/old-activation.json",
        "previous_stopped_reconciliation_path": "/srv/narrowgate/private/old-reconcile.json",
    }
    planned = source_deploy.activate_prepared_release(
        target="example.test",
        **args,
        recover_runtime_fatal=True,
        **evidence,
    )
    assert planned["status"] == "planned"
    assert planned["phases"] == [
        "verify",
        "stop_quiescence",
        "runtime_fatal_proof",
        "fresh_reconcile",
        "start",
        "bounded_health",
        "activation_receipt",
        "publish_current",
    ]
    with pytest.raises(source_deploy.LiveDeployContractError, match="mutually exclusive"):
        source_deploy.render_prepared_release_activation_shell(
            **args,
            resume_stopped=True,
            recover_runtime_fatal=True,
            **evidence,
        )
    with pytest.raises(
        source_deploy.LiveDeployContractError,
        match="requires the previous deployment envelope",
    ):
        source_deploy.render_prepared_release_activation_shell(
            **args,
            recover_runtime_fatal=True,
        )
    with pytest.raises(
        source_deploy.LiveDeployContractError,
        match="only valid for runtime-fatal recovery",
    ):
        source_deploy.render_prepared_release_activation_shell(
            **args,
            **evidence,
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="cannot reuse"):
        source_deploy.render_prepared_release_activation_shell(
            **args,
            recover_runtime_fatal=True,
            **{
                **evidence,
                "previous_activation_receipt_path": args["activation_receipt_path"],
            },
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="cannot reuse"):
        source_deploy.render_prepared_release_activation_shell(
            **args,
            recover_runtime_fatal=True,
            **{
                **evidence,
                "previous_deployment_envelope_path": args[
                    "deployment_envelope_path"
                ],
            },
        )


def test_quiescence_probe_matches_process_argv_not_wrapper_text() -> None:
    probe = source_deploy.render_quiescence_probe_shell()
    assert "pgrep -f" not in probe
    assert "/proc/{entry.name}/cmdline" in probe
    assert 'arg.endswith(b"/live/main.py")' in probe
    assert 'b"__supervise" in args' in probe


def test_prepared_activation_rejects_shell_shaped_identity_and_paths() -> None:
    args = _prepared_activation_args()
    with pytest.raises(source_deploy.LiveDeployContractError, match="release ID"):
        source_deploy.render_prepared_release_activation_shell(
            **{**args, "release_id": "release-a;id"}
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="release ID"):
        source_deploy.render_prepared_release_activation_shell(
            **{**args, "release_id": "Release-A"}
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="service user"):
        source_deploy.render_prepared_release_activation_shell(
            **args, service_user="ec2-user;id"
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="absolute"):
        source_deploy.render_prepared_release_activation_shell(
            **{**args, "active_config_path": "relative.yaml"}
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="percent"):
        source_deploy.render_prepared_release_activation_shell(
            **{**args, "active_config_path": "/srv/private/config%2f.yaml"}
        )
    with pytest.raises(source_deploy.LiveDeployContractError, match="SOCKS5 proxy"):
        source_deploy.activate_prepared_release(
            target="example.test",
            execute=True,
            socks5_proxy="127.0.0.1;id:7897",
            **args,
        )
    for target in (
        "user@-host",
        "user@[127.0.0.1]",
        "user@host%eth0",
        "user@host:22",
    ):
        with pytest.raises(source_deploy.LiveDeployContractError, match="SSH target"):
            source_deploy.activate_prepared_release(
                target=target,
                socks5_proxy="127.0.0.1:7897",
                **args,
            )


def test_prepared_activation_uses_one_ssh_and_accepts_existing_roots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Any, ...]] = []

    def fake_run(argv: tuple[Any, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=f"activated release-a {'1' * 64} {'2' * 64} {'3' * 64}\n",
            stderr="",
        )

    monkeypatch.setattr(source_deploy.subprocess, "run", fake_run)
    result = source_deploy.activate_prepared_release(
        target="example.test",
        execute=True,
        socks5_proxy="127.0.0.1:7897",
        **_prepared_activation_args(),
    )
    assert len(calls) == 1
    assert calls[0][:5] == ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20")
    assert calls[0][5:7] == (
        "-o",
        "ProxyCommand=nc -x 127.0.0.1:7897 -X 5 %h %p",
    )
    assert result["status"] == "activated"
    assert result["stopped_reconciliation_sha256"] == "2" * 64
    assert result["activation_receipt_sha256"] == "3" * 64

    def drifted_run(
        argv: tuple[Any, ...], **_kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=f"activated release-a {'4' * 64} {'2' * 64} {'3' * 64}\n",
            stderr="",
        )

    monkeypatch.setattr(source_deploy.subprocess, "run", drifted_run)
    with pytest.raises(source_deploy.LiveDeployContractError, match="envelope root drifted"):
        source_deploy.activate_prepared_release(
            target="example.test",
            execute=True,
            **_prepared_activation_args(),
        )
