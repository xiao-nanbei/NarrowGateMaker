from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import platform
import stat
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_live_safety_locked_runtime as subject

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
        version="0.1.1",
        requires=("missing-runtime-dependency>=1",)
        if broken_root_requirement
        else ("frozen-dep==1.2.3",),
    )
    native = _wheel(tmp_path, name="narrowgate-btcusdc-cpp", version="0.0.0")
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
    executable = (
        venv_prefix
        / ("Scripts" if sys.platform == "win32" else "bin")
        / executable_name
    )
    declared_base = base_prefix / (
        "python3.exe" if sys.platform == "win32" else "bin/python3"
    )
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
    assert snapshot["base_executable_sha256"] != hashlib.sha256(
        declared_base.read_bytes()
    ).hexdigest()


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
    injected.write_text(
        f"import pathlib; pathlib.Path({str(sentinel)!r}).write_text('executed')\n"
    )
    completed = subprocess.run(
        (
            str(BUILDER_PYTHON),
            "-I",
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
