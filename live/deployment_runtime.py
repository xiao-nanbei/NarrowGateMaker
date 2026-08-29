#!/usr/bin/env python3
"""Build and verify an offline, content-addressed Python 3.12 runtime closure.

This is the policy-neutral public deployment primitive. Owner identity, host
selection, release hashes, and activation/rollback receipts belong in private
configuration and are intentionally absent from this module.

The module deliberately separates three authorities:

* a lock generated from an already-resolved seed virtual environment;
* a content-addressed wheelhouse whose canonical manifest hash is frozen by a
  caller; and
* an install receipt written only after an empty virtual environment has been
  populated with exact wheel paths and ``pip check`` has passed.

No install command emitted here is allowed to resolve a dependency or contact
an index.  The NarrowGate root and native wheels are excluded from the seed
lock and must be supplied explicitly at install time.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
import platform
import re
import shutil
import ssl
import stat
import subprocess
import sys
import sysconfig
import tempfile
import zipfile
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from email.parser import BytesParser
from email.policy import compat32
from pathlib import Path, PurePosixPath
from typing import Any

LOCK_SCHEMA = "narrowgate_locked_python_runtime.v1"
WHEELHOUSE_SCHEMA = "narrowgate_content_addressed_wheelhouse.v1"
INSTALL_RECEIPT_SCHEMA = "narrowgate_locked_python_runtime_install.v2"
LOCK_CANONICAL_FIELD = "canonical_lock_sha256"
WHEELHOUSE_CANONICAL_FIELD = "canonical_wheelhouse_sha256"
INSTALL_CANONICAL_FIELD = "canonical_install_receipt_sha256"
DEPLOYMENT_ENVELOPE_SCHEMA = "narrowgate_private_deployment_envelope.v1"
DEPLOYMENT_ENVELOPE_CANONICAL_FIELD = "canonical_sha256"
NATIVE_BUILD_RECEIPT_SCHEMA = "narrowgate_linux_x86_64_native_build_receipt.v2"
NATIVE_BUILD_RECEIPT_CANONICAL_FIELD = "canonical_native_build_sha256"
NATIVE_BUILD_RECEIPT_STATUS = "exact_tag_native_build_dependency_lock_and_parity_passed"
WHEELHOUSE_MANIFEST = "wheelhouse.manifest.json"
REQUIRED_PYTHON = (3, 12)
DEFAULT_EXCLUDED_DISTRIBUTIONS = (
    "narrowgate",
    "narrowgate-btcusdc-cpp",
    "narrowgate-cpp",
    "narrowgate_cpp",
)
ROOT_DISTRIBUTION_NAME = "narrowgate"
NATIVE_DISTRIBUTION_NAMES = frozenset(
    {
        "narrowgate-btcusdc-cpp",
        "narrowgate-cpp",
    }
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$")


class LockedRuntimeError(RuntimeError):
    """Fail-closed error raised for a non-authoritative runtime closure."""


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def normalize_distribution_name(name: str) -> str:
    value = re.sub(r"[-_.]+", "-", str(name).strip()).lower()
    if not value or not _NAME_RE.fullmatch(value):
        raise LockedRuntimeError(f"invalid distribution name: {name!r}")
    return value


def _require_sha256(value: str, label: str) -> str:
    candidate = str(value).strip().lower()
    if not _SHA256_RE.fullmatch(candidate):
        raise LockedRuntimeError(f"{label} is not a lowercase SHA256")
    return candidate


def canonical_sha256(payload: dict[str, Any], field: str) -> str:
    clone = dict(payload)
    clone.pop(field, None)
    raw = json.dumps(
        clone,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return _sha256(raw)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def _assert_no_symlink_components(path: Path, *, allow_missing_leaf: bool = False) -> None:
    target = _absolute(path)
    parts = target.parts
    current = Path(parts[0])
    for index, part in enumerate(parts[1:], start=1):
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            if allow_missing_leaf and index == len(parts) - 1:
                return
            # A missing ancestor is reported by the operation which needs it.
            return
        if stat.S_ISLNK(info.st_mode):
            raise LockedRuntimeError(f"symlink path component is forbidden: {current}")


def _read_regular_file(path: Path, *, private_authority: bool = False) -> bytes:
    target = _absolute(path)
    _assert_no_symlink_components(target)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(target, flags)
    except OSError as exc:
        raise LockedRuntimeError(f"cannot open regular file {target}: {exc}") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise LockedRuntimeError(f"not a regular file: {target}")
        if private_authority:
            if stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1:
                raise LockedRuntimeError(f"authority must be mode 0600 with one link: {target}")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            raw = handle.read()
        after = os.fstat(fd)
        stable = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) == (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if not stable or len(raw) != after.st_size:
            raise LockedRuntimeError(f"file changed while it was being read: {target}")
        return raw
    finally:
        os.close(fd)


def _write_create_only_private(path: Path, raw: bytes) -> None:
    target = _absolute(path)
    parent = target.parent
    _assert_no_symlink_components(parent)
    if not parent.is_dir():
        raise LockedRuntimeError(f"output parent is not a directory: {parent}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(target, flags, 0o600)
    except FileExistsError as exc:
        raise LockedRuntimeError(f"create-only conflict: {target}") from exc
    try:
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise LockedRuntimeError(f"short write: {target}")
            view = view[written:]
        os.fsync(fd)
        os.fchmod(fd, 0o600)
    except BaseException:
        os.close(fd)
        try:
            target.unlink()
        except FileNotFoundError:
            pass
        raise
    else:
        info = os.fstat(fd)
        os.close(fd)
        if stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise LockedRuntimeError(f"private output mode/link drifted: {target}")


def _load_json_bytes(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LockedRuntimeError(f"invalid JSON authority {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise LockedRuntimeError(f"JSON authority must be an object: {label}")
    if _canonical_json_bytes(value) != raw:
        raise LockedRuntimeError(f"JSON authority is not canonical: {label}")
    return value


def _write_json_authority(path: Path, payload: dict[str, Any]) -> None:
    _write_create_only_private(path, _canonical_json_bytes(payload))


def _write_json_authority_atomic(path: Path, payload: dict[str, Any]) -> bytes:
    """Publish one create-only private authority without a partial-file window."""

    target = _absolute(path)
    parent = target.parent
    _assert_no_symlink_components(parent)
    if not parent.is_dir():
        raise LockedRuntimeError(f"output parent is not a directory: {parent}")
    if target.exists() or target.is_symlink():
        raise LockedRuntimeError(f"create-only conflict: {target}")
    raw = _canonical_json_bytes(payload)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.tmp.", dir=parent)
    temporary = Path(temporary_name)
    published = False
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise LockedRuntimeError(f"short write: {temporary}")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError as exc:
            raise LockedRuntimeError(f"create-only conflict: {target}") from exc
        published = True
        temporary.unlink()
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        info = target.stat()
        if stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise LockedRuntimeError(f"private output mode/link drifted: {target}")
        return raw
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        if published:
            try:
                target.unlink()
            except FileNotFoundError:
                pass
        raise
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _resolved_executable_bytes(path: Path) -> tuple[bytes, Path]:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise LockedRuntimeError(f"interpreter does not resolve: {path}: {exc}") from exc
    if not resolved.is_file():
        raise LockedRuntimeError(f"interpreter is not a file: {resolved}")
    return resolved.read_bytes(), resolved


def _versioned_base_executable_candidate() -> Path:
    if os.name == "nt":
        return Path(sys.base_prefix) / Path(sys.executable).name
    return (
        Path(sys.base_prefix) / "bin" / f"python{sys.version_info.major}.{sys.version_info.minor}"
    )


def _bound_base_executable_bytes(executable_raw: bytes) -> bytes:
    declared_path = Path(getattr(sys, "_base_executable", sys.executable))
    declared_raw, _ = _resolved_executable_bytes(declared_path)
    if sys.prefix == sys.base_prefix or declared_raw == executable_raw:
        return declared_raw

    # Amazon Linux can give a copied Python 3.12 venv ``/usr/bin/python3`` as
    # ``sys._base_executable``, even when that unversioned path is Python 3.9.
    # Correct that declaration only when the versioned base is byte-identical
    # to the running venv executable; every other mismatch remains observable.
    candidate = _versioned_base_executable_candidate()
    try:
        candidate_raw, _ = _resolved_executable_bytes(candidate)
    except LockedRuntimeError:
        return declared_raw
    return candidate_raw if candidate_raw == executable_raw else declared_raw


def _current_interpreter_snapshot() -> dict[str, Any]:
    executable_raw, _ = _resolved_executable_bytes(Path(sys.executable))
    base_raw = _bound_base_executable_bytes(executable_raw)
    return {
        "implementation": platform.python_implementation().lower(),
        "version": platform.python_version(),
        "version_info": [sys.version_info.major, sys.version_info.minor, sys.version_info.micro],
        "cache_tag": str(sys.implementation.cache_tag),
        "soabi": str(sysconfig.get_config_var("SOABI")),
        "abiflags": str(getattr(sys, "abiflags", "")),
        "sysconfig_platform": str(sysconfig.get_platform()),
        "system": platform.system(),
        "machine": platform.machine(),
        "compiler": platform.python_compiler(),
        "openssl_runtime": ssl.OPENSSL_VERSION,
        "openssl_version_number": int(ssl.OPENSSL_VERSION_NUMBER),
        "executable_sha256": _sha256(executable_raw),
        "executable_size_bytes": len(executable_raw),
        "base_executable_sha256": _sha256(base_raw),
        "base_executable_size_bytes": len(base_raw),
        "is_virtual_environment": sys.prefix != sys.base_prefix,
    }


def _direct_source_kind(raw: str | None) -> str:
    if not raw:
        return "index_or_unknown"
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return "invalid_direct_url"
    if not isinstance(value, dict):
        return "invalid_direct_url"
    if isinstance(value.get("dir_info"), dict):
        return "editable_directory" if value["dir_info"].get("editable") else "directory"
    if isinstance(value.get("vcs_info"), dict):
        return "vcs"
    if isinstance(value.get("archive_info"), dict):
        return "archive"
    return "direct_url"


def _seed_metadata_identity(distribution: Any) -> tuple[int, int]:
    """Return the physical identity of an installed distribution's metadata.

    ``importlib.metadata`` may enumerate one ``.dist-info`` directory more than
    once when two entries on ``sys.path`` are filesystem aliases (for example,
    Amazon Linux virtual environments where ``lib64`` points to ``lib``).  Its
    public API does not expose the metadata directory, so use the path retained
    by the standard-library ``PathDistribution`` and fail closed for any other
    representation.
    """

    metadata_path = getattr(distribution, "_path", None)
    if metadata_path is None:
        raise LockedRuntimeError("seed distribution metadata path is unavailable")
    try:
        info = os.stat(metadata_path)
    except (OSError, TypeError, ValueError) as exc:
        raise LockedRuntimeError(
            f"cannot stat seed distribution metadata path: {metadata_path!r}: {exc}"
        ) from exc
    return info.st_dev, info.st_ino


def _seed_snapshot_current() -> dict[str, Any]:
    import importlib.metadata as metadata

    rows: list[dict[str, str]] = []
    rows_by_metadata_identity: dict[tuple[int, int], dict[str, str]] = {}
    for distribution in metadata.distributions():
        name = str(distribution.metadata.get("Name") or "").strip()
        version = str(distribution.version or "").strip()
        row = {
            "name": normalize_distribution_name(name),
            "version": version,
            "source_kind": _direct_source_kind(distribution.read_text("direct_url.json")),
        }
        identity = _seed_metadata_identity(distribution)
        previous = rows_by_metadata_identity.get(identity)
        if previous is not None:
            if previous != row:
                raise LockedRuntimeError(
                    "one seed metadata location produced inconsistent distribution metadata"
                )
            continue
        rows_by_metadata_identity[identity] = row
        rows.append(row)
    rows.sort(key=lambda row: (row["name"], row["version"], row["source_kind"]))
    return {"interpreter": _current_interpreter_snapshot(), "distributions": rows}


def _urlsafe_digest(raw: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b"=").decode("ascii")


def _site_packages_directory(prefix: Path, interpreter: dict[str, Any]) -> Path:
    """Locate one real site-packages directory without executing its interpreter."""

    version_info = interpreter.get("version_info")
    if (
        not isinstance(version_info, list)
        or len(version_info) != 3
        or any(type(value) is not int for value in version_info)
    ):
        raise LockedRuntimeError("installed tree interpreter version is malformed")
    major, minor, _micro = version_info
    candidates = (
        prefix / "Lib" / "site-packages",
        prefix / "lib" / f"python{major}.{minor}" / "site-packages",
        prefix / "lib64" / f"python{major}.{minor}" / "site-packages",
    )
    real_candidates: list[Path] = []
    for candidate in candidates:
        if not candidate.exists() and not candidate.is_symlink():
            continue
        try:
            _assert_no_symlink_components(candidate)
        except LockedRuntimeError:
            continue
        if candidate.is_dir():
            real_candidates.append(candidate)
    if len(real_candidates) != 1:
        raise LockedRuntimeError("installed tree requires exactly one real site-packages directory")
    return real_candidates[0]


def _installed_tree_snapshot(prefix_path: Path, *, interpreter: dict[str, Any]) -> dict[str, Any]:
    """Statically bind every installed RECORD and the complete site tree.

    This function intentionally does not execute the target interpreter or use
    ``importlib.metadata``.  A trusted builder can therefore call it before a
    target ``.pth`` file or bytecode cache has any opportunity to execute.
    """

    prefix = _absolute(prefix_path)
    _assert_no_symlink_components(prefix)
    if not prefix.is_dir():
        raise LockedRuntimeError(f"installed prefix is not a real directory: {prefix}")
    site_packages = _site_packages_directory(prefix, interpreter)
    site_relative = site_packages.relative_to(prefix).as_posix()
    dist_info_directories: list[Path] = []
    for child in sorted(site_packages.iterdir(), key=lambda path: path.name):
        info = child.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise LockedRuntimeError(f"installed site-packages symlink is forbidden: {child}")
        if child.name.endswith(".dist-info"):
            if not stat.S_ISDIR(info.st_mode):
                raise LockedRuntimeError(f"installed dist-info is not a directory: {child}")
            dist_info_directories.append(child)
    if not dist_info_directories:
        raise LockedRuntimeError("installed tree contains no dist-info directories")

    distributions: list[dict[str, Any]] = []
    seen: set[str] = set()
    claimed_site_files: dict[str, str] = {}
    for dist_info in dist_info_directories:
        metadata_path = dist_info / "METADATA"
        metadata_raw = _read_regular_file(metadata_path)
        metadata = BytesParser(policy=compat32).parsebytes(metadata_raw)
        name = normalize_distribution_name(str(metadata.get("Name") or ""))
        version = str(metadata.get("Version") or "").strip()
        if not version:
            raise LockedRuntimeError(f"installed distribution has no version: {name}")
        if name in seen:
            raise LockedRuntimeError(f"duplicate installed distribution: {name}")
        seen.add(name)
        record = dist_info / "RECORD"
        try:
            record_info = record.lstat()
        except FileNotFoundError as exc:
            raise LockedRuntimeError(f"installed distribution lacks RECORD: {name}") from exc
        if (
            stat.S_ISLNK(record_info.st_mode)
            or not stat.S_ISREG(record_info.st_mode)
            or record_info.st_nlink != 1
        ):
            raise LockedRuntimeError(f"installed RECORD is not a regular file: {name}")
        record_raw = _read_regular_file(record)
        verified_rows: list[dict[str, Any]] = []
        record_names: set[str] = set()
        try:
            rows = list(csv.reader(io.StringIO(record_raw.decode("utf-8"))))
        except (UnicodeDecodeError, csv.Error) as exc:
            raise LockedRuntimeError(f"invalid installed RECORD for {name}: {exc}") from exc
        for row in rows:
            if len(row) != 3 or not row[0] or row[0] in record_names:
                raise LockedRuntimeError(f"invalid or duplicate RECORD row for {name}")
            record_names.add(row[0])
            member = PurePosixPath(row[0])
            if member.is_absolute() or "\\" in row[0]:
                raise LockedRuntimeError(f"unsafe installed RECORD path for {name}: {row[0]}")
            if member.suffix == ".pyc" or "__pycache__" in member.parts:
                raise LockedRuntimeError(f"installed bytecode is forbidden: {name}:{row[0]}")
            located = site_packages.joinpath(*member.parts)
            try:
                _assert_no_symlink_components(located)
                resolved = located.resolve(strict=True)
            except OSError as exc:
                raise LockedRuntimeError(
                    f"missing installed RECORD member for {name}: {row[0]}"
                ) from exc
            if not resolved.is_relative_to(prefix):
                raise LockedRuntimeError(f"installed RECORD escapes venv for {name}: {row[0]}")
            resolved_info = resolved.stat()
            if located.is_symlink() or not resolved.is_file() or resolved_info.st_nlink != 1:
                raise LockedRuntimeError(f"installed RECORD member is not regular: {name}:{row[0]}")
            if resolved.is_relative_to(site_packages):
                site_name = resolved.relative_to(site_packages).as_posix()
                previous_owner = claimed_site_files.setdefault(site_name, name)
                if previous_owner != name:
                    raise LockedRuntimeError(
                        f"installed site file has multiple RECORD owners: {site_name}"
                    )
            if not row[1]:
                if resolved != record.resolve(strict=True):
                    raise LockedRuntimeError(
                        f"unhashed installed file is forbidden: {name}:{row[0]}"
                    )
                continue
            algorithm, separator, encoded = row[1].partition("=")
            if separator != "=" or algorithm != "sha256" or not encoded:
                raise LockedRuntimeError(f"non-SHA256 RECORD digest for {name}:{row[0]}")
            raw = resolved.read_bytes()
            if _urlsafe_digest(raw) != encoded:
                raise LockedRuntimeError(f"installed RECORD digest mismatch for {name}:{row[0]}")
            try:
                expected_size = int(row[2])
            except ValueError as exc:
                raise LockedRuntimeError(
                    f"invalid installed RECORD size for {name}:{row[0]}"
                ) from exc
            if expected_size != len(raw):
                raise LockedRuntimeError(f"installed RECORD size mismatch for {name}:{row[0]}")
            verified_rows.append({"path": row[0], "sha256": _sha256(raw), "size_bytes": len(raw)})
        verified_rows.sort(key=lambda row: row["path"])
        distributions.append(
            {
                "name": name,
                "version": version,
                "record_sha256": _sha256(record_raw),
                "record_size_bytes": len(record_raw),
                "record_verified_files_sha256": _sha256(
                    json.dumps(verified_rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ),
                "record_verified_file_count": len(verified_rows),
            }
        )
    distributions.sort(key=lambda row: row["name"])
    tree_directories: list[dict[str, Any]] = []
    tree_files: list[dict[str, Any]] = []
    observed_site_files: set[str] = set()
    for current, directories, files in os.walk(site_packages, followlinks=False):
        current_path = Path(current)
        current_relative = current_path.relative_to(site_packages).as_posix()
        current_info = current_path.lstat()
        if stat.S_ISLNK(current_info.st_mode) or not stat.S_ISDIR(current_info.st_mode):
            raise LockedRuntimeError(f"installed site directory is unsafe: {current_path}")
        if current_relative != ".":
            if "__pycache__" in PurePosixPath(current_relative).parts:
                raise LockedRuntimeError(
                    f"installed bytecode directory is forbidden: {current_relative}"
                )
            tree_directories.append(
                {
                    "path": current_relative,
                    "mode": stat.S_IMODE(current_info.st_mode),
                }
            )
        for directory in directories:
            child = current_path / directory
            child_info = child.lstat()
            child_relative = child.relative_to(site_packages).as_posix()
            if stat.S_ISLNK(child_info.st_mode) or not stat.S_ISDIR(child_info.st_mode):
                raise LockedRuntimeError(f"installed site directory is unsafe: {child}")
            if directory == "__pycache__":
                raise LockedRuntimeError(
                    f"installed bytecode directory is forbidden: {child_relative}"
                )
        for filename in files:
            child = current_path / filename
            child_relative = child.relative_to(site_packages).as_posix()
            child_info = child.lstat()
            if (
                stat.S_ISLNK(child_info.st_mode)
                or not stat.S_ISREG(child_info.st_mode)
                or child_info.st_nlink != 1
            ):
                raise LockedRuntimeError(f"installed site file is unsafe: {child}")
            if child.suffix == ".pyc" or "__pycache__" in PurePosixPath(child_relative).parts:
                raise LockedRuntimeError(f"installed bytecode is forbidden: {child_relative}")
            if child_relative not in claimed_site_files:
                raise LockedRuntimeError(
                    f"installed site file is outside every RECORD: {child_relative}"
                )
            raw = _read_regular_file(child)
            observed_site_files.add(child_relative)
            tree_files.append(
                {
                    "path": child_relative,
                    "owner": claimed_site_files[child_relative],
                    "sha256": _sha256(raw),
                    "size_bytes": len(raw),
                    "mode": stat.S_IMODE(child_info.st_mode),
                }
            )
    if observed_site_files != set(claimed_site_files):
        missing = sorted(set(claimed_site_files) - observed_site_files)
        raise LockedRuntimeError(f"installed RECORD site members are missing: {missing}")
    tree_directories.sort(key=lambda row: row["path"])
    tree_files.sort(key=lambda row: row["path"])
    aggregate_payload = {
        "distributions": distributions,
        "site_packages_relative_path": site_relative,
        "directories": tree_directories,
        "files": tree_files,
    }
    return {
        "distributions": distributions,
        "record_aggregate_sha256": _sha256(
            json.dumps(aggregate_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ),
        "site_packages_relative_path": site_relative,
        "site_packages_directory_count": len(tree_directories),
        "site_packages_file_count": len(tree_files),
    }


def _installed_snapshot_current() -> dict[str, Any]:
    interpreter = _current_interpreter_snapshot()
    snapshot = _installed_tree_snapshot(Path(sys.prefix), interpreter=interpreter)
    return {"interpreter": interpreter, **snapshot}


def _safe_environment() -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            "PIP_NO_CACHE_DIR": "1",
        }
    )
    return env


def _run_python_json(python: Path, private_command: str) -> dict[str, Any]:
    executable = _absolute(python)
    completed = subprocess.run(
        (
            str(executable),
            "-I",
            "-B",
            str(Path(__file__).resolve(strict=True)),
            private_command,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=180.0,
        env=_safe_environment(),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()[-2000:]
        raise LockedRuntimeError(
            f"interpreter probe failed ({private_command}, rc={completed.returncode}): {detail}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise LockedRuntimeError(f"interpreter probe returned invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise LockedRuntimeError("interpreter probe returned a non-object")
    return value


def probe_interpreter(python: Path) -> dict[str, Any]:
    return _run_python_json(python, "_probe-interpreter")


def _validate_interpreter_shape(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LockedRuntimeError(f"{label} interpreter binding is not an object")
    required = {
        "implementation",
        "version",
        "version_info",
        "cache_tag",
        "soabi",
        "abiflags",
        "sysconfig_platform",
        "system",
        "machine",
        "compiler",
        "openssl_runtime",
        "openssl_version_number",
        "executable_sha256",
        "executable_size_bytes",
        "base_executable_sha256",
        "base_executable_size_bytes",
        "is_virtual_environment",
    }
    if set(value) != required:
        raise LockedRuntimeError(f"{label} interpreter fields drifted")
    if value["implementation"] != "cpython" or value["version_info"][:2] != list(REQUIRED_PYTHON):
        raise LockedRuntimeError(f"{label} requires exact CPython 3.12.x")
    _require_sha256(value["executable_sha256"], f"{label} executable")
    _require_sha256(value["base_executable_sha256"], f"{label} base executable")
    if not isinstance(value["openssl_runtime"], str) or not value["openssl_runtime"]:
        raise LockedRuntimeError(f"{label} OpenSSL runtime is missing")
    return value


def _interpreter_binding(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "is_virtual_environment"}


def _assert_interpreter_equal(actual: dict[str, Any], expected: dict[str, Any], label: str) -> None:
    _validate_interpreter_shape(actual, f"{label} actual")
    _validate_interpreter_shape(expected, f"{label} expected")
    if _interpreter_binding(actual) != _interpreter_binding(expected):
        changed = sorted(
            key for key in _interpreter_binding(expected) if actual.get(key) != expected.get(key)
        )
        raise LockedRuntimeError(f"{label} interpreter drift: {changed}")


def _validate_version(version: Any, label: str) -> str:
    value = str(version).strip()
    if not value or any(ord(character) < 32 for character in value):
        raise LockedRuntimeError(f"invalid version for {label}: {version!r}")
    return value


def _validate_lock_payload(
    payload: dict[str, Any], *, expected_canonical_sha256: str | None = None
) -> dict[str, Any]:
    if set(payload) != {
        "schema_version",
        "status",
        "generated_utc",
        "interpreter",
        "distributions",
        "excluded_distribution_names",
        "excluded_distributions",
        "install_contract",
        LOCK_CANONICAL_FIELD,
    }:
        raise LockedRuntimeError("runtime lock fields drifted")
    if payload.get("schema_version") != LOCK_SCHEMA or payload.get("status") != "locked":
        raise LockedRuntimeError("unsupported or incomplete runtime lock")
    actual_canonical = canonical_sha256(payload, LOCK_CANONICAL_FIELD)
    if payload.get(LOCK_CANONICAL_FIELD) != actual_canonical:
        raise LockedRuntimeError("runtime lock canonical hash mismatch")
    if expected_canonical_sha256 is not None and actual_canonical != _require_sha256(
        expected_canonical_sha256, "expected lock"
    ):
        raise LockedRuntimeError("runtime lock does not match frozen expected hash")
    interpreter = _validate_interpreter_shape(payload.get("interpreter"), "lock")
    if interpreter["is_virtual_environment"] is not True:
        raise LockedRuntimeError("runtime lock was not generated from a virtual environment")
    rows = payload.get("distributions")
    if not isinstance(rows, list) or not rows:
        raise LockedRuntimeError("runtime lock has no distributions")
    names: list[str] = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"name", "version"}:
            raise LockedRuntimeError("runtime lock distribution row drifted")
        name = normalize_distribution_name(row["name"])
        if name != row["name"]:
            raise LockedRuntimeError("runtime lock name is not normalized")
        _validate_version(row["version"], name)
        names.append(name)
    if names != sorted(names) or len(names) != len(set(names)):
        raise LockedRuntimeError("runtime lock distributions are unsorted or duplicated")
    excluded = payload.get("excluded_distributions")
    if not isinstance(excluded, list):
        raise LockedRuntimeError("runtime lock excluded distribution list is missing")
    excluded_names: list[str] = []
    for row in excluded:
        if not isinstance(row, dict) or set(row) != {"name", "version", "source_kind"}:
            raise LockedRuntimeError("excluded distribution row drifted")
        excluded_name = normalize_distribution_name(row["name"])
        if excluded_name != row["name"]:
            raise LockedRuntimeError("excluded distribution name is not normalized")
        _validate_version(row["version"], excluded_name)
        excluded_names.append(excluded_name)
    if excluded_names != sorted(excluded_names) or len(excluded_names) != len(set(excluded_names)):
        raise LockedRuntimeError("excluded distributions are unsorted or duplicated")
    if set(names) & set(excluded_names):
        raise LockedRuntimeError("excluded distribution leaked into the dependency lock")
    configured_exclusions = payload.get("excluded_distribution_names")
    if not isinstance(configured_exclusions, list):
        raise LockedRuntimeError("configured lock exclusions are missing")
    normalized_exclusions = [normalize_distribution_name(name) for name in configured_exclusions]
    if (
        configured_exclusions != normalized_exclusions
        or normalized_exclusions != sorted(set(normalized_exclusions))
        or not set(excluded_names) <= set(normalized_exclusions)
    ):
        raise LockedRuntimeError("configured lock exclusions drifted")
    if payload.get("install_contract") != {
        "dependencies": "exact_wheel_paths_only",
        "index_access": "forbidden",
        "dependency_resolution": "forbidden",
        "root_wheel": "explicit",
        "native_wheel": "explicit",
    }:
        raise LockedRuntimeError("runtime lock install contract drifted")
    return payload


def build_lock(
    *,
    seed_python: Path,
    generated_utc: str | None = None,
    excluded_names: Iterable[str] = DEFAULT_EXCLUDED_DISTRIBUTIONS,
) -> dict[str, Any]:
    snapshot = _run_python_json(seed_python, "_snapshot-seed")
    interpreter = _validate_interpreter_shape(snapshot.get("interpreter"), "seed")
    if interpreter["is_virtual_environment"] is not True:
        raise LockedRuntimeError("seed interpreter must be a virtual environment")
    excluded_set = {normalize_distribution_name(name) for name in excluded_names}
    rows = snapshot.get("distributions")
    if not isinstance(rows, list):
        raise LockedRuntimeError("seed distribution snapshot is missing")
    locked: dict[str, dict[str, str]] = {}
    excluded: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise LockedRuntimeError("invalid seed distribution row")
        name = normalize_distribution_name(row.get("name", ""))
        version = _validate_version(row.get("version", ""), name)
        source_kind = str(row.get("source_kind", ""))
        target = excluded if name in excluded_set else locked
        if name in target:
            raise LockedRuntimeError(f"duplicate seed distribution: {name}")
        if target is excluded:
            target[name] = {"name": name, "version": version, "source_kind": source_kind}
        else:
            target[name] = {"name": name, "version": version}
    if not locked:
        raise LockedRuntimeError("seed dependency closure is empty")
    payload: dict[str, Any] = {
        "schema_version": LOCK_SCHEMA,
        "status": "locked",
        "generated_utc": generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "interpreter": interpreter,
        "distributions": [locked[name] for name in sorted(locked)],
        "excluded_distribution_names": sorted(excluded_set),
        "excluded_distributions": [excluded[name] for name in sorted(excluded)],
        "install_contract": {
            "dependencies": "exact_wheel_paths_only",
            "index_access": "forbidden",
            "dependency_resolution": "forbidden",
            "root_wheel": "explicit",
            "native_wheel": "explicit",
        },
    }
    payload[LOCK_CANONICAL_FIELD] = canonical_sha256(payload, LOCK_CANONICAL_FIELD)
    return _validate_lock_payload(payload)


def generate_lock(
    *,
    seed_python: Path,
    output_path: Path,
    generated_utc: str | None = None,
    excluded_names: Iterable[str] = DEFAULT_EXCLUDED_DISTRIBUTIONS,
) -> dict[str, Any]:
    payload = build_lock(
        seed_python=seed_python,
        generated_utc=generated_utc,
        excluded_names=excluded_names,
    )
    _write_json_authority(output_path, payload)
    return {
        "lock": payload,
        "publication_semantics": "first_writer",
    }


def load_lock(path: Path, *, expected_canonical_sha256: str) -> tuple[dict[str, Any], bytes]:
    raw = _read_regular_file(path, private_authority=True)
    payload = _load_json_bytes(raw, str(path))
    return (
        _validate_lock_payload(payload, expected_canonical_sha256=expected_canonical_sha256),
        raw,
    )


def _safe_wheel_member(name: str) -> None:
    member = PurePosixPath(name)
    if (
        not name
        or name.startswith("/")
        or "\\" in name
        or member.is_absolute()
        or any(part in {"", ".", ".."} for part in member.parts)
    ):
        raise LockedRuntimeError(f"unsafe wheel member path: {name!r}")


def _is_top_level_dist_info_authority(name: str, authority: str) -> bool:
    parts = name.split("/")
    return len(parts) == 2 and parts[0].endswith(".dist-info") and parts[1] == authority


def _inspect_wheel_bytes(
    path: Path, *, expected_sha256: str | None = None
) -> tuple[dict[str, Any], bytes]:
    source = _absolute(path)
    if source.suffix != ".whl" or Path(source.name).name != source.name:
        raise LockedRuntimeError(f"wheel filename is invalid: {source.name}")
    raw = _read_regular_file(source)
    digest = _sha256(raw)
    if expected_sha256 is not None and digest != _require_sha256(
        expected_sha256, f"expected wheel {source.name}"
    ):
        raise LockedRuntimeError(f"wheel SHA256 mismatch: {source.name}")
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                raise LockedRuntimeError(f"wheel has duplicate members: {source.name}")
            for name in names:
                _safe_wheel_member(name.rstrip("/"))
            metadata_names = [
                name for name in names if _is_top_level_dist_info_authority(name, "METADATA")
            ]
            wheel_names = [
                name for name in names if _is_top_level_dist_info_authority(name, "WHEEL")
            ]
            record_names = [
                name for name in names if _is_top_level_dist_info_authority(name, "RECORD")
            ]
            if len(metadata_names) != 1 or len(wheel_names) != 1 or len(record_names) != 1:
                raise LockedRuntimeError(f"wheel authority members are ambiguous: {source.name}")
            dist_info = metadata_names[0].removesuffix("/METADATA")
            if wheel_names[0] != f"{dist_info}/WHEEL" or record_names[0] != f"{dist_info}/RECORD":
                raise LockedRuntimeError(f"wheel dist-info directories disagree: {source.name}")
            metadata = BytesParser(policy=compat32).parsebytes(archive.read(metadata_names[0]))
            name = normalize_distribution_name(str(metadata.get("Name") or ""))
            version = _validate_version(metadata.get("Version") or "", name)
            try:
                record_rows = list(
                    csv.reader(io.StringIO(archive.read(record_names[0]).decode("utf-8")))
                )
            except (UnicodeDecodeError, csv.Error) as exc:
                raise LockedRuntimeError(f"invalid wheel RECORD: {source.name}: {exc}") from exc
            by_name: dict[str, tuple[str, str]] = {}
            for row in record_rows:
                if len(row) != 3 or not row[0] or row[0] in by_name:
                    raise LockedRuntimeError(
                        f"invalid or duplicate wheel RECORD row: {source.name}"
                    )
                _safe_wheel_member(row[0])
                by_name[row[0]] = (row[1], row[2])
            archive_files = {info.filename for info in infos if not info.is_dir()}
            if set(by_name) != archive_files:
                raise LockedRuntimeError(f"wheel RECORD member set mismatch: {source.name}")
            for member_name in sorted(archive_files):
                encoded, size_text = by_name[member_name]
                member_raw = archive.read(member_name)
                if member_name == record_names[0]:
                    if encoded or size_text:
                        raise LockedRuntimeError(
                            f"wheel RECORD must be self-unhashed: {source.name}"
                        )
                    continue
                algorithm, separator, value = encoded.partition("=")
                if separator != "=" or algorithm != "sha256" or not value:
                    raise LockedRuntimeError(
                        f"wheel member lacks SHA256: {source.name}:{member_name}"
                    )
                if _urlsafe_digest(member_raw) != value or size_text != str(len(member_raw)):
                    raise LockedRuntimeError(
                        f"wheel RECORD digest/size mismatch: {source.name}:{member_name}"
                    )
    except zipfile.BadZipFile as exc:
        raise LockedRuntimeError(f"invalid wheel ZIP: {source.name}") from exc
    return (
        {
            "name": name,
            "version": version,
            "filename": source.name,
            "sha256": digest,
            "size_bytes": len(raw),
        },
        raw,
    )


def inspect_wheel(path: Path, *, expected_sha256: str | None = None) -> dict[str, Any]:
    artifact, _ = _inspect_wheel_bytes(path, expected_sha256=expected_sha256)
    return artifact


def _wheelhouse_payload(
    *, lock: dict[str, Any], artifacts: Sequence[dict[str, Any]]
) -> dict[str, Any]:
    wheels = []
    for artifact in sorted(artifacts, key=lambda row: row["name"]):
        wheel = dict(artifact)
        wheel["relative_path"] = f"wheels/{wheel['sha256']}/{wheel['filename']}"
        wheels.append(wheel)
    payload: dict[str, Any] = {
        "schema_version": WHEELHOUSE_SCHEMA,
        "status": "complete",
        "lock_authority": {
            "canonical_lock_sha256": lock[LOCK_CANONICAL_FIELD],
        },
        "interpreter": lock["interpreter"],
        "wheels": wheels,
        "publication_contract": {
            "layout": "wheels/<sha256>/<filename>",
            "files": "create_only_0600",
            "directories": "0700",
            "manifest": "written_last",
        },
    }
    payload[WHEELHOUSE_CANONICAL_FIELD] = canonical_sha256(payload, WHEELHOUSE_CANONICAL_FIELD)
    return payload


def _validate_wheelhouse_payload(
    payload: dict[str, Any], *, expected_canonical_sha256: str
) -> dict[str, Any]:
    if set(payload) != {
        "schema_version",
        "status",
        "lock_authority",
        "interpreter",
        "wheels",
        "publication_contract",
        WHEELHOUSE_CANONICAL_FIELD,
    }:
        raise LockedRuntimeError("wheelhouse manifest fields drifted")
    if payload.get("schema_version") != WHEELHOUSE_SCHEMA or payload.get("status") != "complete":
        raise LockedRuntimeError("unsupported or incomplete wheelhouse manifest")
    actual = canonical_sha256(payload, WHEELHOUSE_CANONICAL_FIELD)
    if payload.get(WHEELHOUSE_CANONICAL_FIELD) != actual:
        raise LockedRuntimeError("wheelhouse canonical hash mismatch")
    if actual != _require_sha256(expected_canonical_sha256, "expected wheelhouse"):
        raise LockedRuntimeError("wheelhouse does not match frozen expected hash")
    rows = payload.get("wheels")
    if not isinstance(rows, list) or not rows:
        raise LockedRuntimeError("wheelhouse contains no wheels")
    names: list[str] = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "name",
            "version",
            "filename",
            "sha256",
            "size_bytes",
            "relative_path",
        }:
            raise LockedRuntimeError("wheelhouse wheel row drifted")
        name = normalize_distribution_name(row["name"])
        _validate_version(row["version"], name)
        digest = _require_sha256(row["sha256"], f"wheelhouse {name}")
        if row["relative_path"] != f"wheels/{digest}/{row['filename']}":
            raise LockedRuntimeError(f"wheelhouse filename/hash binding drifted: {name}")
        names.append(name)
    if names != sorted(names) or len(names) != len(set(names)):
        raise LockedRuntimeError("wheelhouse distributions are unsorted or duplicated")
    if payload.get("publication_contract") != {
        "layout": "wheels/<sha256>/<filename>",
        "files": "create_only_0600",
        "directories": "0700",
        "manifest": "written_last",
    }:
        raise LockedRuntimeError("wheelhouse publication contract drifted")
    return payload


def _publish_wheelhouse(
    *,
    lock_path: Path,
    expected_lock_sha256: str,
    wheel_paths: Sequence[Path],
    output_dir: Path,
) -> dict[str, Any]:
    output = _absolute(output_dir)
    if output.exists() or output.is_symlink():
        raise LockedRuntimeError(f"create-only wheelhouse conflict: {output}")
    lock, _ = load_lock(lock_path, expected_canonical_sha256=expected_lock_sha256)
    expected = {row["name"]: row["version"] for row in lock["distributions"]}
    artifacts: dict[str, tuple[dict[str, Any], bytes]] = {}
    for path in wheel_paths:
        artifact, raw = _inspect_wheel_bytes(path)
        name = artifact["name"]
        if name in artifacts:
            raise LockedRuntimeError(f"duplicate wheel for distribution: {name}")
        if name not in expected:
            raise LockedRuntimeError(f"wheel is not in the frozen lock: {name}")
        if artifact["version"] != expected[name]:
            raise LockedRuntimeError(
                f"wheel version drift for {name}: {artifact['version']} != {expected[name]}"
            )
        artifacts[name] = (artifact, raw)
    missing = sorted(set(expected) - set(artifacts))
    if missing:
        raise LockedRuntimeError(f"wheelhouse is missing locked wheels: {missing}")

    parent = output.parent
    _assert_no_symlink_components(parent)
    if not parent.is_dir():
        raise LockedRuntimeError(f"wheelhouse parent is not a directory: {parent}")
    if output.exists() or output.is_symlink():
        raise LockedRuntimeError(f"create-only wheelhouse conflict: {output}")
    stage = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging.", dir=parent))
    os.chmod(stage, 0o700)
    try:
        wheels_root = stage / "wheels"
        wheels_root.mkdir(mode=0o700)
        for name in sorted(artifacts):
            artifact, raw = artifacts[name]
            digest_dir = wheels_root / artifact["sha256"]
            digest_dir.mkdir(mode=0o700)
            _write_create_only_private(digest_dir / artifact["filename"], raw)
        payload = _wheelhouse_payload(
            lock=lock,
            artifacts=[artifacts[name][0] for name in sorted(artifacts)],
        )
        _write_json_authority(stage / WHEELHOUSE_MANIFEST, payload)
        _validate_wheelhouse_directory(
            lock=lock,
            wheelhouse_dir=stage,
            expected_manifest_sha256=payload[WHEELHOUSE_CANONICAL_FIELD],
        )
        if output.exists() or output.is_symlink():
            raise LockedRuntimeError(f"create-only wheelhouse conflict: {output}")
        os.rename(stage, output)
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    return {
        "manifest": payload,
        "publication_semantics": "first_writer_atomic_directory",
    }


def receive_wheelhouse(
    *,
    lock_path: Path,
    expected_lock_sha256: str,
    wheel_paths: Sequence[Path],
    output_dir: Path,
) -> dict[str, Any]:
    return _publish_wheelhouse(
        lock_path=lock_path,
        expected_lock_sha256=expected_lock_sha256,
        wheel_paths=wheel_paths,
        output_dir=output_dir,
    )


def download_wheelhouse(
    *,
    lock_path: Path,
    expected_lock_sha256: str,
    pip_python: Path,
    output_dir: Path,
) -> dict[str, Any]:
    lock, _ = load_lock(lock_path, expected_canonical_sha256=expected_lock_sha256)
    pip_interpreter = probe_interpreter(pip_python)
    _assert_interpreter_equal(pip_interpreter, lock["interpreter"], "wheel download")
    output = _absolute(output_dir)
    if output.exists() or output.is_symlink():
        raise LockedRuntimeError(f"create-only wheelhouse conflict: {output}")
    with tempfile.TemporaryDirectory(prefix="narrowgate-wheel-download-") as temporary:
        temp = Path(temporary)
        wheel_paths: list[Path] = []
        for index, row in enumerate(lock["distributions"]):
            destination = temp / f"{index:04d}"
            destination.mkdir(mode=0o700)
            command = (
                str(_absolute(pip_python)),
                "-B",
                "-m",
                "pip",
                "download",
                "--disable-pip-version-check",
                "--no-deps",
                "--only-binary=:all:",
                "--dest",
                str(destination),
                f"{row['name']}=={row['version']}",
            )
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=300.0,
                env=_safe_environment(),
            )
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout).strip()[-2000:]
                raise LockedRuntimeError(
                    f"exact wheel download failed for {row['name']}=={row['version']}: {detail}"
                )
            files = list(destination.iterdir())
            if len(files) != 1 or files[0].suffix != ".whl":
                raise LockedRuntimeError(
                    f"download did not yield exactly one wheel for {row['name']}"
                )
            artifact = inspect_wheel(files[0])
            if artifact["name"] != row["name"] or artifact["version"] != row["version"]:
                raise LockedRuntimeError(f"downloaded wheel metadata drift for {row['name']}")
            wheel_paths.append(files[0])
        return _publish_wheelhouse(
            lock_path=lock_path,
            expected_lock_sha256=expected_lock_sha256,
            wheel_paths=wheel_paths,
            output_dir=output_dir,
        )


def _validate_private_directory(path: Path, label: str) -> Path:
    target = _absolute(path)
    _assert_no_symlink_components(target)
    try:
        info = target.stat()
    except FileNotFoundError as exc:
        raise LockedRuntimeError(f"missing {label}: {target}") from exc
    if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
        raise LockedRuntimeError(f"{label} must be a real mode-0700 directory: {target}")
    return target


def _validate_wheelhouse_directory(
    *,
    lock: dict[str, Any],
    wheelhouse_dir: Path,
    expected_manifest_sha256: str,
) -> tuple[dict[str, Any], list[tuple[dict[str, Any], bytes]]]:
    root = _validate_private_directory(wheelhouse_dir, "wheelhouse")
    manifest_raw = _read_regular_file(root / WHEELHOUSE_MANIFEST, private_authority=True)
    manifest = _validate_wheelhouse_payload(
        _load_json_bytes(manifest_raw, str(root / WHEELHOUSE_MANIFEST)),
        expected_canonical_sha256=expected_manifest_sha256,
    )
    expected_lock_authority = {
        "canonical_lock_sha256": lock[LOCK_CANONICAL_FIELD],
    }
    if manifest.get("lock_authority") != expected_lock_authority:
        raise LockedRuntimeError("wheelhouse lock authority drifted")
    if manifest.get("interpreter") != lock["interpreter"]:
        raise LockedRuntimeError("wheelhouse interpreter authority drifted")
    lock_versions = {row["name"]: row["version"] for row in lock["distributions"]}
    artifacts: list[tuple[dict[str, Any], bytes]] = []
    expected_files = {WHEELHOUSE_MANIFEST}
    expected_directories = {".", "wheels"}
    for row in manifest["wheels"]:
        relative = Path(row["relative_path"])
        expected_files.add(relative.as_posix())
        expected_directories.add(relative.parent.as_posix())
        path = root / relative
        raw = _read_regular_file(path, private_authority=True)
        artifact, inspected_raw = _inspect_wheel_bytes(path, expected_sha256=row["sha256"])
        if raw != inspected_raw or artifact != {key: row[key] for key in artifact}:
            raise LockedRuntimeError(f"wheelhouse artifact binding drifted: {row['name']}")
        if row["name"] not in lock_versions or row["version"] != lock_versions[row["name"]]:
            raise LockedRuntimeError(f"wheelhouse version is outside lock: {row['name']}")
        if row["size_bytes"] != len(raw) or row["filename"] != path.name:
            raise LockedRuntimeError(f"wheelhouse filename/size drifted: {row['name']}")
        artifacts.append((artifact, raw))
    if {row["name"] for row in manifest["wheels"]} != set(lock_versions):
        raise LockedRuntimeError("wheelhouse distribution set differs from lock")
    actual_files: set[str] = set()
    actual_directories: set[str] = {"."}
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        relative_dir = current_path.relative_to(root).as_posix()
        actual_directories.add(relative_dir)
        for directory in directories:
            child = current_path / directory
            info = child.lstat()
            if stat.S_ISLNK(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
                raise LockedRuntimeError(f"wheelhouse directory is unsafe: {child}")
        for filename in files:
            actual_files.add((current_path / filename).relative_to(root).as_posix())
    if actual_files != expected_files or actual_directories != expected_directories:
        raise LockedRuntimeError("wheelhouse contains missing or unmanifested paths")
    return manifest, artifacts


def validate_wheelhouse(
    *,
    lock_path: Path,
    expected_lock_sha256: str,
    wheelhouse_dir: Path,
    expected_manifest_sha256: str,
) -> dict[str, Any]:
    lock, _ = load_lock(lock_path, expected_canonical_sha256=expected_lock_sha256)
    manifest, _ = _validate_wheelhouse_directory(
        lock=lock,
        wheelhouse_dir=wheelhouse_dir,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    return manifest


def _run_checked(
    command: Sequence[str], *, timeout: float, env: dict[str, str], label: str
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        tuple(command),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()[-3000:]
        raise LockedRuntimeError(f"{label} failed (rc={completed.returncode}): {detail}")
    return completed


def _expected_distribution_versions(
    lock: dict[str, Any], root_artifact: dict[str, Any], native_artifact: dict[str, Any]
) -> dict[str, str]:
    expected = {row["name"]: row["version"] for row in lock["distributions"]}
    for artifact in (root_artifact, native_artifact):
        if artifact["name"] in expected:
            raise LockedRuntimeError(
                f"explicit wheel distribution leaked into dependency lock: {artifact['name']}"
            )
        expected[artifact["name"]] = artifact["version"]
    return expected


def _validate_explicit_wheels(
    *,
    root_wheel_path: Path,
    root_wheel_sha256: str,
    native_wheel_path: Path,
    native_wheel_sha256: str,
) -> tuple[tuple[dict[str, Any], bytes], tuple[dict[str, Any], bytes]]:
    root = _inspect_wheel_bytes(root_wheel_path, expected_sha256=root_wheel_sha256)
    native = _inspect_wheel_bytes(native_wheel_path, expected_sha256=native_wheel_sha256)
    if root[0]["name"] != ROOT_DISTRIBUTION_NAME:
        raise LockedRuntimeError("root wheel distribution must be narrowgate")
    if native[0]["name"] not in NATIVE_DISTRIBUTION_NAMES:
        raise LockedRuntimeError(
            f"native wheel distribution is not recognized: {native[0]['name']}"
        )
    if root[0]["name"] == native[0]["name"]:
        raise LockedRuntimeError("root and native wheels have the same distribution name")
    return root, native


def _validate_installed_versions(snapshot: dict[str, Any], expected: dict[str, str]) -> None:
    rows = snapshot.get("distributions")
    if not isinstance(rows, list):
        raise LockedRuntimeError("installed distribution snapshot is missing")
    actual = {row.get("name"): row.get("version") for row in rows if isinstance(row, dict)}
    if actual != expected:
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        changed = sorted(
            name for name in set(actual) & set(expected) if actual[name] != expected[name]
        )
        raise LockedRuntimeError(
            f"installed distribution/version drift: missing={missing}, extra={extra}, changed={changed}"
        )
    aggregate = snapshot.get("record_aggregate_sha256")
    _require_sha256(aggregate, "installed RECORD aggregate")


def _validate_static_snapshot_against_receipt(
    *, target: Path, receipt: dict[str, Any]
) -> dict[str, Any]:
    interpreter = _validate_interpreter_shape(receipt.get("interpreter"), "static installed tree")
    snapshot = _installed_tree_snapshot(target, interpreter=interpreter)
    expected_versions = {row["name"]: row["version"] for row in receipt["installed_distributions"]}
    _validate_installed_versions(snapshot, expected_versions)
    if receipt["installed_distributions"] != snapshot["distributions"]:
        raise LockedRuntimeError("static installed distribution or RECORD detail drift")
    if receipt["installed_record_aggregate_sha256"] != snapshot["record_aggregate_sha256"]:
        raise LockedRuntimeError("static installed tree/RECORD aggregate drift")
    return snapshot


def _pip_target_command(builder_python: Path, target_python: Path) -> list[str]:
    return [
        str(_absolute(builder_python)),
        "-B",
        "-m",
        "pip",
        "--python",
        str(_absolute(target_python)),
    ]


def install_locked_runtime(
    *,
    builder_python: Path,
    venv_dir: Path,
    lock_path: Path,
    expected_lock_sha256: str,
    wheelhouse_dir: Path,
    expected_wheelhouse_sha256: str,
    root_wheel_path: Path,
    root_wheel_sha256: str,
    native_wheel_path: Path,
    native_wheel_sha256: str,
    receipt_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    lock, _ = load_lock(lock_path, expected_canonical_sha256=expected_lock_sha256)
    builder = probe_interpreter(builder_python)
    _assert_interpreter_equal(builder, lock["interpreter"], "runtime builder")
    manifest, dependency_artifacts = _validate_wheelhouse_directory(
        lock=lock,
        wheelhouse_dir=wheelhouse_dir,
        expected_manifest_sha256=expected_wheelhouse_sha256,
    )
    root_artifact, native_artifact = _validate_explicit_wheels(
        root_wheel_path=root_wheel_path,
        root_wheel_sha256=root_wheel_sha256,
        native_wheel_path=native_wheel_path,
        native_wheel_sha256=native_wheel_sha256,
    )
    expected_versions = _expected_distribution_versions(lock, root_artifact[0], native_artifact[0])
    target = _absolute(venv_dir)
    receipt = _absolute(receipt_path)
    _assert_no_symlink_components(target.parent)
    _assert_no_symlink_components(receipt.parent)
    if not target.parent.is_dir() or not receipt.parent.is_dir():
        raise LockedRuntimeError("venv and receipt parents must already exist")
    if target.exists() or target.is_symlink():
        raise LockedRuntimeError(f"fresh venv create-only conflict: {target}")
    if receipt.exists() or receipt.is_symlink():
        raise LockedRuntimeError(f"install receipt create-only conflict: {receipt}")
    if receipt.is_relative_to(target):
        raise LockedRuntimeError("install receipt must be outside the target venv")

    created = False
    install_stage: Path | None = None
    try:
        _run_checked(
            (
                str(_absolute(builder_python)),
                "-I",
                "-B",
                "-m",
                "venv",
                "--without-pip",
                "--copies",
                str(target),
            ),
            timeout=180.0,
            env=_safe_environment(),
            label="fresh venv creation",
        )
        created = True
        os.chmod(target, 0o700)
        target_python = target / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        if target_python.is_symlink() or not target_python.is_file():
            raise LockedRuntimeError("target venv interpreter is not an owned regular copy")
        target_before = probe_interpreter(target_python)
        if target_before["is_virtual_environment"] is not True:
            raise LockedRuntimeError("new target is not an isolated virtual environment")
        _assert_interpreter_equal(target_before, lock["interpreter"], "fresh runtime")

        install_stage = Path(tempfile.mkdtemp(prefix=".locked-wheels.", dir=target.parent))
        os.chmod(install_stage, 0o700)
        exact_paths: list[Path] = []
        seen_filenames: set[str] = set()
        all_artifacts = [*dependency_artifacts, root_artifact, native_artifact]
        for artifact, raw in all_artifacts:
            filename = artifact["filename"]
            if filename in seen_filenames:
                raise LockedRuntimeError(f"wheel filename collision: {filename}")
            seen_filenames.add(filename)
            exact_path = install_stage / filename
            _write_create_only_private(exact_path, raw)
            if (
                _sha256(_read_regular_file(exact_path, private_authority=True))
                != artifact["sha256"]
            ):
                raise LockedRuntimeError(f"private install wheel copy drifted: {filename}")
            exact_paths.append(exact_path)
        env = _safe_environment()
        env.update(
            {
                "PIP_CONFIG_FILE": os.devnull,
                "PIP_NO_INDEX": "1",
                "PIP_NO_DEPENDENCIES": "1",
            }
        )
        install_command = [
            *_pip_target_command(builder_python, target_python),
            "install",
            "--no-index",
            "--no-deps",
            "--no-cache-dir",
            "--disable-pip-version-check",
            "--no-compile",
            "--force-reinstall",
            "--no-warn-script-location",
            *(str(path) for path in exact_paths),
        ]
        _run_checked(
            install_command,
            timeout=600.0,
            env=env,
            label="offline exact-wheel install",
        )
        static_installed = _installed_tree_snapshot(target, interpreter=target_before)
        _validate_installed_versions(static_installed, expected_versions)
        pip_check = _run_checked(
            [*_pip_target_command(builder_python, target_python), "check"],
            timeout=180.0,
            env=env,
            label="pip check",
        )
        installed = _run_python_json(target_python, "_snapshot-installed")
        _assert_interpreter_equal(
            installed["interpreter"], lock["interpreter"], "installed runtime"
        )
        _validate_installed_versions(installed, expected_versions)
        if installed["distributions"] != static_installed["distributions"]:
            raise LockedRuntimeError(
                "target interpreter disagrees with the static installed RECORD snapshot"
            )
        if installed["record_aggregate_sha256"] != static_installed["record_aggregate_sha256"]:
            raise LockedRuntimeError("target interpreter changed the static installed tree")
        pyvenv_raw = _read_regular_file(target / "pyvenv.cfg")
        payload: dict[str, Any] = {
            "schema_version": INSTALL_RECEIPT_SCHEMA,
            "status": "offline_exact_install_passed",
            "generated_utc": generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "lock_authority": {
                "canonical_lock_sha256": lock[LOCK_CANONICAL_FIELD],
            },
            "wheelhouse_authority": {
                "canonical_wheelhouse_sha256": manifest[WHEELHOUSE_CANONICAL_FIELD],
            },
            "explicit_wheels": {
                "root": root_artifact[0],
                "native": native_artifact[0],
            },
            "interpreter": installed["interpreter"],
            "pyvenv_cfg_sha256": _sha256(pyvenv_raw),
            "installed_distributions": static_installed["distributions"],
            "installed_record_aggregate_sha256": static_installed["record_aggregate_sha256"],
            "pip_check": {
                "passed": True,
                "stdout_sha256": _sha256(pip_check.stdout.encode("utf-8")),
            },
            "install_policy": {
                "target_started_without_pip": True,
                "builder_pip_target_mode": True,
                "no_index": True,
                "no_dependencies": True,
                "no_cache": True,
                "exact_wheel_paths": True,
                "bytecode_files_forbidden": True,
                "record_outside_site_files_forbidden": True,
                "static_tree_verified_before_target_execution": True,
            },
        }
        payload[INSTALL_CANONICAL_FIELD] = canonical_sha256(payload, INSTALL_CANONICAL_FIELD)
        _write_json_authority(receipt, payload)
        return {
            "receipt": payload,
            "publication_semantics": "first_writer_receipt_last",
        }
    except BaseException:
        if created:
            shutil.rmtree(target, ignore_errors=True)
        raise
    finally:
        if install_stage is not None:
            shutil.rmtree(install_stage, ignore_errors=True)


def _load_install_receipt(
    path: Path, *, expected_canonical_sha256: str
) -> tuple[dict[str, Any], bytes]:
    raw = _read_regular_file(path, private_authority=True)
    payload = _load_json_bytes(raw, str(path))
    if set(payload) != {
        "schema_version",
        "status",
        "generated_utc",
        "lock_authority",
        "wheelhouse_authority",
        "explicit_wheels",
        "interpreter",
        "pyvenv_cfg_sha256",
        "installed_distributions",
        "installed_record_aggregate_sha256",
        "pip_check",
        "install_policy",
        INSTALL_CANONICAL_FIELD,
    }:
        raise LockedRuntimeError("install receipt fields drifted")
    if payload.get("schema_version") != INSTALL_RECEIPT_SCHEMA or payload.get("status") != (
        "offline_exact_install_passed"
    ):
        raise LockedRuntimeError("unsupported or incomplete install receipt")
    actual = canonical_sha256(payload, INSTALL_CANONICAL_FIELD)
    if payload.get(INSTALL_CANONICAL_FIELD) != actual:
        raise LockedRuntimeError("install receipt canonical hash mismatch")
    if actual != _require_sha256(expected_canonical_sha256, "expected install receipt"):
        raise LockedRuntimeError("install receipt does not match frozen expected hash")
    _validate_interpreter_shape(payload.get("interpreter"), "install receipt")
    _require_sha256(payload.get("pyvenv_cfg_sha256", ""), "receipt pyvenv.cfg")
    _require_sha256(
        payload.get("installed_record_aggregate_sha256", ""),
        "receipt installed RECORD aggregate",
    )
    if payload.get("install_policy") != {
        "target_started_without_pip": True,
        "builder_pip_target_mode": True,
        "no_index": True,
        "no_dependencies": True,
        "no_cache": True,
        "exact_wheel_paths": True,
        "bytecode_files_forbidden": True,
        "record_outside_site_files_forbidden": True,
        "static_tree_verified_before_target_execution": True,
    }:
        raise LockedRuntimeError("install receipt policy drifted")
    return payload, raw


def validate_static_installed_tree(
    *,
    venv_dir: Path,
    receipt_path: Path,
    expected_receipt_sha256: str,
) -> dict[str, Any]:
    """Verify the target tree without ever starting its interpreter.

    Deployment must invoke this API (or ``verify-static-tree``) with a trusted
    predecessor/builder Python before the target Python is allowed to run.  It
    rejects bytecode, symlinks, hard links, and every site-packages file which
    is not owned by an installed distribution RECORD.
    """

    receipt, _ = _load_install_receipt(
        receipt_path, expected_canonical_sha256=expected_receipt_sha256
    )
    target = _absolute(venv_dir)
    _validate_static_snapshot_against_receipt(target=target, receipt=receipt)
    pyvenv_raw = _read_regular_file(target / "pyvenv.cfg")
    if receipt["pyvenv_cfg_sha256"] != _sha256(pyvenv_raw):
        raise LockedRuntimeError("static pyvenv.cfg drift")
    return receipt


def validate_startup_runtime(
    *,
    venv_python: Path,
    receipt_path: Path,
    expected_receipt_sha256: str,
    expected_lock_sha256: str,
    expected_wheelhouse_sha256: str,
    expected_root_wheel_sha256: str,
    expected_native_wheel_sha256: str,
    expected_python_version: str,
    expected_soabi: str,
    expected_compiler: str,
    expected_openssl_runtime: str,
    expected_interpreter_executable_sha256: str,
    pip_runner_python: Path | None = None,
) -> dict[str, Any]:
    """Validate the live venv using only release-frozen receipt authorities.

    This is the startup-facing verifier.  It does not need the original lock,
    wheelhouse, or wheels to remain mounted after deployment; their externally
    frozen hashes are compared to the trusted install receipt.  The current
    interpreter, installed versions, every installed RECORD, and ``pip check``
    are then recomputed from the live venv.
    """

    receipt, _ = _load_install_receipt(
        receipt_path, expected_canonical_sha256=expected_receipt_sha256
    )
    if receipt.get("lock_authority", {}).get("canonical_lock_sha256") != _require_sha256(
        expected_lock_sha256, "startup expected lock"
    ):
        raise LockedRuntimeError("startup lock authority drifted")
    if receipt.get("wheelhouse_authority", {}).get(
        "canonical_wheelhouse_sha256"
    ) != _require_sha256(expected_wheelhouse_sha256, "startup expected wheelhouse"):
        raise LockedRuntimeError("startup wheelhouse authority drifted")
    explicit = receipt.get("explicit_wheels")
    if not isinstance(explicit, dict) or set(explicit) != {"root", "native"}:
        raise LockedRuntimeError("startup explicit-wheel receipt is malformed")
    if explicit["root"].get("sha256") != _require_sha256(
        expected_root_wheel_sha256, "startup expected root wheel"
    ):
        raise LockedRuntimeError("startup root wheel authority drifted")
    if explicit["native"].get("sha256") != _require_sha256(
        expected_native_wheel_sha256, "startup expected native wheel"
    ):
        raise LockedRuntimeError("startup native wheel authority drifted")
    interpreter = receipt["interpreter"]
    frozen_interpreter_fields = {
        "version": expected_python_version,
        "soabi": expected_soabi,
        "compiler": expected_compiler,
        "openssl_runtime": expected_openssl_runtime,
        "executable_sha256": _require_sha256(
            expected_interpreter_executable_sha256,
            "startup expected interpreter executable",
        ),
    }
    changed = sorted(
        name
        for name, expected in frozen_interpreter_fields.items()
        if interpreter.get(name) != expected
    )
    if changed:
        raise LockedRuntimeError(f"startup frozen interpreter authority drift: {changed}")
    target_python = _absolute(venv_python)
    if target_python.is_symlink() or not target_python.is_file():
        raise LockedRuntimeError("startup venv interpreter is not an owned regular copy")
    _validate_static_snapshot_against_receipt(target=target_python.parent.parent, receipt=receipt)
    snapshot = _run_python_json(target_python, "_snapshot-installed")
    _assert_interpreter_equal(snapshot["interpreter"], interpreter, "startup runtime")
    expected_versions = {row["name"]: row["version"] for row in receipt["installed_distributions"]}
    _validate_installed_versions(snapshot, expected_versions)
    if receipt["installed_distributions"] != snapshot["distributions"]:
        raise LockedRuntimeError("startup installed distribution or RECORD detail drift")
    if receipt["installed_record_aggregate_sha256"] != snapshot["record_aggregate_sha256"]:
        raise LockedRuntimeError("startup installed RECORD aggregate drift")
    pyvenv_raw = _read_regular_file(target_python.parent.parent / "pyvenv.cfg")
    if receipt["pyvenv_cfg_sha256"] != _sha256(pyvenv_raw):
        raise LockedRuntimeError("startup pyvenv.cfg drift")
    runner = target_python if pip_runner_python is None else pip_runner_python
    runner_interpreter = probe_interpreter(runner)
    _assert_interpreter_equal(runner_interpreter, interpreter, "startup pip check runner")
    env = _safe_environment()
    env.update({"PIP_CONFIG_FILE": os.devnull, "PIP_NO_INDEX": "1"})
    _run_checked(
        [*_pip_target_command(runner, target_python), "check"],
        timeout=180.0,
        env=env,
        label="startup pip check",
    )
    return receipt


def validate_installed_runtime(
    *,
    venv_python: Path,
    pip_runner_python: Path,
    receipt_path: Path,
    expected_receipt_sha256: str,
    lock_path: Path,
    expected_lock_sha256: str,
    wheelhouse_dir: Path,
    expected_wheelhouse_sha256: str,
    root_wheel_path: Path,
    root_wheel_sha256: str,
    native_wheel_path: Path,
    native_wheel_sha256: str,
) -> dict[str, Any]:
    lock, _ = load_lock(lock_path, expected_canonical_sha256=expected_lock_sha256)
    manifest, _ = _validate_wheelhouse_directory(
        lock=lock,
        wheelhouse_dir=wheelhouse_dir,
        expected_manifest_sha256=expected_wheelhouse_sha256,
    )
    root_artifact, native_artifact = _validate_explicit_wheels(
        root_wheel_path=root_wheel_path,
        root_wheel_sha256=root_wheel_sha256,
        native_wheel_path=native_wheel_path,
        native_wheel_sha256=native_wheel_sha256,
    )
    expected_versions = _expected_distribution_versions(lock, root_artifact[0], native_artifact[0])
    receipt, _ = _load_install_receipt(
        receipt_path, expected_canonical_sha256=expected_receipt_sha256
    )
    if receipt.get("lock_authority") != {
        "canonical_lock_sha256": lock[LOCK_CANONICAL_FIELD],
    }:
        raise LockedRuntimeError("install receipt lock authority drifted")
    if receipt.get("wheelhouse_authority") != {
        "canonical_wheelhouse_sha256": manifest[WHEELHOUSE_CANONICAL_FIELD],
    }:
        raise LockedRuntimeError("install receipt wheelhouse authority drifted")
    if receipt.get("explicit_wheels") != {
        "root": root_artifact[0],
        "native": native_artifact[0],
    }:
        raise LockedRuntimeError("install receipt explicit-wheel authority drifted")
    target_python = _absolute(venv_python)
    if target_python.is_symlink() or not target_python.is_file():
        raise LockedRuntimeError("installed venv interpreter is not an owned regular copy")
    _validate_static_snapshot_against_receipt(target=target_python.parent.parent, receipt=receipt)
    snapshot = _run_python_json(target_python, "_snapshot-installed")
    _assert_interpreter_equal(snapshot["interpreter"], lock["interpreter"], "verified runtime")
    _validate_installed_versions(snapshot, expected_versions)
    if receipt.get("interpreter") != snapshot["interpreter"]:
        raise LockedRuntimeError("installed interpreter receipt drift")
    if receipt.get("installed_distributions") != snapshot["distributions"]:
        raise LockedRuntimeError("installed distribution or RECORD detail drift")
    if receipt.get("installed_record_aggregate_sha256") != snapshot["record_aggregate_sha256"]:
        raise LockedRuntimeError("installed RECORD aggregate drift")
    pyvenv_raw = _read_regular_file(target_python.parent.parent / "pyvenv.cfg")
    if receipt.get("pyvenv_cfg_sha256") != _sha256(pyvenv_raw):
        raise LockedRuntimeError("pyvenv.cfg drift")
    runner = probe_interpreter(pip_runner_python)
    _assert_interpreter_equal(runner, lock["interpreter"], "pip check runner")
    env = _safe_environment()
    env.update({"PIP_CONFIG_FILE": os.devnull, "PIP_NO_INDEX": "1"})
    _run_checked(
        [*_pip_target_command(pip_runner_python, target_python), "check"],
        timeout=180.0,
        env=env,
        label="verification pip check",
    )
    return receipt


def _git_output(repository_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", *args),
        cwd=repository_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30.0,
        env=_safe_environment(),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()[-2000:]
        raise LockedRuntimeError(f"git {' '.join(args)} failed: {detail}")
    return completed.stdout.strip()


def _load_canonical_authority(
    path: Path,
    *,
    schema: str,
    canonical_field: str,
) -> tuple[dict[str, Any], bytes]:
    raw = _read_regular_file(path, private_authority=True)
    payload = _load_json_bytes(raw, str(path))
    if payload.get("schema_version") != schema:
        raise LockedRuntimeError(f"authority schema drifted: {path}")
    observed = _require_sha256(payload.get(canonical_field, ""), canonical_field)
    if observed != canonical_sha256(payload, canonical_field):
        raise LockedRuntimeError(f"authority canonical hash drifted: {path}")
    return payload, raw


def _receipt_path(value: Any, label: str, *, directory: bool = False) -> Path:
    raw = str(value or "")
    path = Path(raw).expanduser()
    if not raw or "\x00" in raw or not path.is_absolute():
        raise LockedRuntimeError(f"native receipt {label} path is invalid")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise LockedRuntimeError(f"native receipt {label} path is not canonical")
    if (directory and not resolved.is_dir()) or (not directory and not resolved.is_file()):
        raise LockedRuntimeError(f"native receipt {label} path type drifted")
    return resolved


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LockedRuntimeError(f"{label} must be an object")
    return value


def _file_binding(path: Path, expected_sha256: Any, label: str) -> tuple[Path, str]:
    resolved = _receipt_path(path, label)
    expected = _require_sha256(expected_sha256, f"native receipt {label}")
    if _sha256(_read_regular_file(resolved)) != expected:
        raise LockedRuntimeError(f"native receipt {label} file hash drifted")
    return resolved, expected


def _content_bundle_reference(
    members: Sequence[tuple[str, Path]],
) -> dict[str, Any]:
    """Bind named non-manifest files behind one bundle root.

    Leaf hashes are deliberately local to this calculation.  The release
    manifest exposes one root plus locators, rather than copying each leaf
    digest into every deployment and runtime-policy layer.
    """

    locators: dict[str, str] = {}
    identities: list[dict[str, Any]] = []
    for role, path in members:
        if role in locators:
            raise LockedRuntimeError(f"duplicate content-bundle role: {role}")
        resolved = _receipt_path(_absolute(path), f"{role} bundle member")
        raw = _read_regular_file(resolved)
        locators[role] = str(resolved)
        identities.append({"role": role, "sha256": _sha256(raw), "size_bytes": len(raw)})
    identities.sort(key=lambda row: row["role"])
    root = _sha256(
        json.dumps(
            identities,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    )
    return {"root_sha256": root, "member_paths": locators}


def _validate_content_bundle_reference(
    value: Any,
    *,
    label: str,
    required_roles: frozenset[str],
) -> tuple[dict[str, str], dict[str, str]]:
    reference = _require_mapping(value, label)
    if set(reference) != {"root_sha256", "member_paths"}:
        raise LockedRuntimeError(f"{label} fields drifted")
    expected_root = _require_sha256(reference.get("root_sha256", ""), label)
    member_paths = _require_mapping(reference.get("member_paths"), f"{label} paths")
    if set(member_paths) != set(required_roles):
        raise LockedRuntimeError(f"{label} member roles drifted")
    resolved_members: list[tuple[str, Path]] = []
    for role in sorted(required_roles):
        resolved_members.append(
            (role, _receipt_path(Path(str(member_paths[role])), f"{label} {role}"))
        )
    observed = _content_bundle_reference(resolved_members)
    if observed["root_sha256"] != expected_root:
        raise LockedRuntimeError(f"{label} root drifted")
    leaf_sha256 = {role: _sha256(_read_regular_file(path)) for role, path in resolved_members}
    return {role: str(path) for role, path in resolved_members}, leaf_sha256


def _validate_native_build_bundle(
    native_build_receipt_path: Path,
    *,
    execution_commit: str,
    execution_tree: str,
    expected_root_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate one nested build manifest and derive runtime-only leaves."""

    native_path = _receipt_path(_absolute(native_build_receipt_path), "native build receipt")
    native, _native_raw = _load_canonical_authority(
        native_path,
        schema=NATIVE_BUILD_RECEIPT_SCHEMA,
        canonical_field=NATIVE_BUILD_RECEIPT_CANONICAL_FIELD,
    )
    native_root = _require_sha256(native[NATIVE_BUILD_RECEIPT_CANONICAL_FIELD], "native build root")
    if expected_root_sha256 is not None and native_root != _require_sha256(
        expected_root_sha256, "expected native build root"
    ):
        raise LockedRuntimeError("native build bundle root drifted")
    if native.get("status") != NATIVE_BUILD_RECEIPT_STATUS:
        raise LockedRuntimeError("native build receipt status drifted")
    if "native_sources" in native:
        raise LockedRuntimeError("native build receipt repeats Git-tracked source hashes")
    execution = _require_mapping(native.get("execution"), "native execution")
    if (
        execution.get("execution_commit") != execution_commit
        or execution.get("execution_tree") != execution_tree
    ):
        raise LockedRuntimeError("native build receipt checkout binding drifted")
    dependency = _require_mapping(native.get("dependency_lock"), "native dependency lock")
    installed = _require_mapping(
        native.get("installed_distribution_lock"),
        "native installed distribution lock",
    )
    if set(dependency) != {
        "runtime_lock_path",
        "runtime_lock_sha256",
        "wheelhouse_path",
        "wheelhouse_manifest_path",
        "wheelhouse_sha256",
    }:
        raise LockedRuntimeError("native dependency-lock fields drifted")
    if set(installed) != {
        "install_receipt_path",
        "install_receipt_sha256",
        "root_wheel_path",
        "root_wheel_sha256",
        "native_wheel_path",
        "native_wheel_sha256",
        "interpreter",
        "installed_distributions",
        "installed_record_aggregate_sha256",
    }:
        raise LockedRuntimeError("native installed-distribution fields drifted")

    lock_path = _receipt_path(Path(str(dependency.get("runtime_lock_path", ""))), "runtime lock")
    lock_canonical = _require_sha256(
        dependency.get("runtime_lock_sha256", ""),
        "native receipt runtime lock root",
    )
    lock, _ = load_lock(lock_path, expected_canonical_sha256=lock_canonical)

    wheelhouse_path = _receipt_path(
        Path(str(dependency.get("wheelhouse_path", ""))),
        "wheelhouse",
        directory=True,
    )
    manifest_path = _receipt_path(
        Path(str(dependency.get("wheelhouse_manifest_path", ""))),
        "wheelhouse manifest",
    )
    if manifest_path != wheelhouse_path / WHEELHOUSE_MANIFEST:
        raise LockedRuntimeError("wheelhouse manifest path binding drifted")
    wheelhouse_canonical = _require_sha256(
        dependency.get("wheelhouse_sha256", ""),
        "native receipt wheelhouse root",
    )
    manifest = validate_wheelhouse(
        lock_path=lock_path,
        expected_lock_sha256=lock_canonical,
        wheelhouse_dir=wheelhouse_path,
        expected_manifest_sha256=wheelhouse_canonical,
    )

    install_path = _receipt_path(
        Path(str(installed.get("install_receipt_path", ""))), "install receipt"
    )
    install_canonical = _require_sha256(
        installed.get("install_receipt_sha256", ""),
        "native receipt install root",
    )
    install, _ = _load_install_receipt(
        install_path, expected_canonical_sha256=install_canonical
    )
    if install.get("lock_authority", {}).get("canonical_lock_sha256") != lock[LOCK_CANONICAL_FIELD]:
        raise LockedRuntimeError("install receipt lock root drifted")
    if (
        install.get("wheelhouse_authority", {}).get("canonical_wheelhouse_sha256")
        != manifest[WHEELHOUSE_CANONICAL_FIELD]
    ):
        raise LockedRuntimeError("install receipt wheelhouse root drifted")

    root_wheel_path, root_wheel_sha256 = _file_binding(
        Path(str(installed.get("root_wheel_path", ""))),
        installed.get("root_wheel_sha256"),
        "root wheel",
    )
    native_wheel_path, native_wheel_sha256 = _file_binding(
        Path(str(installed.get("native_wheel_path", ""))),
        installed.get("native_wheel_sha256"),
        "native wheel",
    )
    explicit = _require_mapping(install.get("explicit_wheels"), "install explicit wheels")
    root_artifact, native_artifact = _validate_explicit_wheels(
        root_wheel_path=root_wheel_path,
        root_wheel_sha256=root_wheel_sha256,
        native_wheel_path=native_wheel_path,
        native_wheel_sha256=native_wheel_sha256,
    )
    if (
        _require_mapping(explicit.get("root"), "install root wheel") != root_artifact[0]
        or _require_mapping(explicit.get("native"), "install native wheel") != native_artifact[0]
    ):
        raise LockedRuntimeError("install receipt explicit wheel identity drifted")
    native_wheel = _require_mapping(native.get("wheel"), "native wheel")
    if (
        _receipt_path(Path(str(native_wheel.get("path", ""))), "native receipt wheel")
        != native_wheel_path
        or native_wheel.get("sha256") != native_wheel_sha256
        or native_wheel.get("size_bytes") != native_wheel_path.stat().st_size
    ):
        raise LockedRuntimeError("native receipt wheel identity drifted")
    module = _require_mapping(native.get("module"), "native module")
    module_path, module_sha256 = _file_binding(
        Path(str(module.get("path", ""))), module.get("sha256"), "native module"
    )
    if module.get("size_bytes") != module_path.stat().st_size:
        raise LockedRuntimeError("native module size identity drifted")

    interpreter = _require_mapping(installed.get("interpreter"), "native interpreter")
    if (
        interpreter != install.get("interpreter")
        or interpreter != lock.get("interpreter")
        or installed.get("installed_distributions") != install.get("installed_distributions")
        or installed.get("installed_record_aggregate_sha256")
        != install.get("installed_record_aggregate_sha256")
        or native.get("soabi") != interpreter.get("soabi")
    ):
        raise LockedRuntimeError("native/install runtime identity drifted")
    _validate_installed_versions(
        {
            "distributions": install.get("installed_distributions"),
            "record_aggregate_sha256": install.get("installed_record_aggregate_sha256"),
        },
        _expected_distribution_versions(lock, root_artifact[0], native_artifact[0]),
    )
    installed_record_aggregate_sha256 = _require_sha256(
        installed.get("installed_record_aggregate_sha256", ""),
        "installed RECORD aggregate",
    )
    return {
        "root_sha256": native_root,
        "manifest_path": str(native_path),
        "native_module_sha256": module_sha256,
        "native_wheel_sha256": native_wheel_sha256,
        "runtime_lock_path": str(lock_path),
        "runtime_lock_canonical_sha256": lock_canonical,
        "wheelhouse_path": str(wheelhouse_path),
        "wheelhouse_canonical_sha256": wheelhouse_canonical,
        "install_receipt_path": str(install_path),
        "install_receipt_canonical_sha256": install_canonical,
        "root_wheel_path": str(root_wheel_path),
        "root_wheel_sha256": root_wheel_sha256,
        "native_wheel_path": str(native_wheel_path),
        "installed_record_aggregate_sha256": installed_record_aggregate_sha256,
        "locked_runtime_interpreter": interpreter,
        "native_soabi": str(native.get("soabi", "")),
    }


def build_deployment_envelope(
    *,
    repository_root: Path,
    active_config_path: Path,
    native_build_receipt_path: Path,
    output_path: Path,
    disabled_config_path: Path | None = None,
    policy_artifact_manifest_path: Path | None = None,
    policy_file_path: Path | None = None,
    predicate_bundle_path: Path | None = None,
) -> dict[str, Any]:
    """Write one compact release root over source and three bundle roots."""

    root = _absolute(repository_root).resolve(strict=True)
    if not root.is_dir():
        raise LockedRuntimeError("repository root is not a directory")
    commit = _git_output(root, "rev-parse", "HEAD")
    tree = _git_output(root, "rev-parse", "HEAD^{tree}")
    if not re.fullmatch(r"[0-9a-f]{40}", commit) or not re.fullmatch(r"[0-9a-f]{40}", tree):
        raise LockedRuntimeError("repository commit/tree identity is invalid")
    if _git_output(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise LockedRuntimeError("deployment envelope requires a clean repository")

    active = _receipt_path(_absolute(active_config_path), "active config")
    disabled = (
        active
        if disabled_config_path is None
        else _receipt_path(_absolute(disabled_config_path), "disabled config")
    )
    build = _validate_native_build_bundle(
        native_build_receipt_path,
        execution_commit=commit,
        execution_tree=tree,
    )

    policy_paths = (
        policy_artifact_manifest_path,
        policy_file_path,
        predicate_bundle_path,
    )
    if any(path is not None for path in policy_paths) and not all(
        path is not None for path in policy_paths
    ):
        raise LockedRuntimeError("BUY E3 policy artifacts must be supplied all-or-none")

    policy_members: list[tuple[str, Path]] = []
    if all(path is not None for path in policy_paths):
        policy_members = [
            ("artifact_manifest", _absolute(policy_artifact_manifest_path)),
            ("policy", _absolute(policy_file_path)),
            ("predicate_bundle", _absolute(predicate_bundle_path)),
        ]

    payload: dict[str, Any] = {
        "schema_version": DEPLOYMENT_ENVELOPE_SCHEMA,
        "status": "deployment_envelope_built",
        "source": {"commit": commit, "tree": tree},
        "build_bundle": {
            "manifest_path": build["manifest_path"],
            "root_sha256": build["root_sha256"],
        },
        "config_bundle": _content_bundle_reference((("active", active), ("disabled", disabled))),
        "model_policy_bundle": _content_bundle_reference(policy_members),
    }
    payload[DEPLOYMENT_ENVELOPE_CANONICAL_FIELD] = canonical_sha256(
        payload, DEPLOYMENT_ENVELOPE_CANONICAL_FIELD
    )
    _write_json_authority_atomic(output_path, payload)
    return {
        "envelope": payload,
        "path": str(_absolute(output_path)),
        "canonical_sha256": payload[DEPLOYMENT_ENVELOPE_CANONICAL_FIELD],
    }


def load_deployment_envelope(
    path: Path,
    *,
    expected_root_sha256: str,
    buy_e3_enabled: bool,
) -> dict[str, Any]:
    """Resolve one compact release root into runtime-only derived leaves."""

    payload, _raw = _load_canonical_authority(
        path,
        schema=DEPLOYMENT_ENVELOPE_SCHEMA,
        canonical_field=DEPLOYMENT_ENVELOPE_CANONICAL_FIELD,
    )
    observed_root = _require_sha256(payload[DEPLOYMENT_ENVELOPE_CANONICAL_FIELD], "release root")
    if observed_root != _require_sha256(expected_root_sha256, "expected release root"):
        raise LockedRuntimeError("deployment release root drifted")
    if set(payload) != {
        "schema_version",
        "status",
        "source",
        "build_bundle",
        "config_bundle",
        "model_policy_bundle",
        DEPLOYMENT_ENVELOPE_CANONICAL_FIELD,
    }:
        raise LockedRuntimeError("deployment release-root fields drifted")
    if payload.get("status") != "deployment_envelope_built":
        raise LockedRuntimeError("deployment release-root status drifted")
    source = _require_mapping(payload.get("source"), "release source")
    if set(source) != {"commit", "tree"}:
        raise LockedRuntimeError("deployment source fields drifted")
    commit = str(source.get("commit", ""))
    tree = str(source.get("tree", ""))
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None or re.fullmatch(r"[0-9a-f]{40}", tree) is None:
        raise LockedRuntimeError("deployment source identity is invalid")
    build_reference = _require_mapping(payload.get("build_bundle"), "build bundle")
    if set(build_reference) != {"manifest_path", "root_sha256"}:
        raise LockedRuntimeError("build bundle fields drifted")
    build = _validate_native_build_bundle(
        Path(str(build_reference.get("manifest_path", ""))),
        execution_commit=commit,
        execution_tree=tree,
        expected_root_sha256=str(build_reference.get("root_sha256", "")),
    )
    _config_paths, config_leaves = _validate_content_bundle_reference(
        payload.get("config_bundle"),
        label="config bundle",
        required_roles=frozenset({"active", "disabled"}),
    )
    policy_reference = _require_mapping(payload.get("model_policy_bundle"), "model-policy bundle")
    policy_member_paths = _require_mapping(
        policy_reference.get("member_paths"), "model-policy bundle paths"
    )
    complete_policy_roles = frozenset({"artifact_manifest", "policy", "predicate_bundle"})
    observed_policy_roles = frozenset(policy_member_paths)
    if observed_policy_roles not in {frozenset(), complete_policy_roles}:
        raise LockedRuntimeError("model-policy bundle member roles drifted")
    if bool(buy_e3_enabled) and observed_policy_roles != complete_policy_roles:
        raise LockedRuntimeError("model-policy bundle member roles drifted")
    _policy_paths, _policy_leaves = _validate_content_bundle_reference(
        policy_reference,
        label="model-policy bundle",
        required_roles=observed_policy_roles,
    )
    authority = {
        "path": str(_absolute(path).resolve(strict=True)),
        "canonical_sha256": observed_root,
        "execution_commit": commit,
        "execution_tree": tree,
        "active_config_file_sha256": config_leaves["active"],
        "disabled_config_file_sha256": config_leaves["disabled"],
        **{
            key: value
            for key, value in build.items()
            if key not in {"root_sha256", "manifest_path"}
        },
    }
    return authority


def validate_deployment_envelope_startup(
    *,
    repository_root: Path,
    envelope_path: Path,
    expected_envelope_sha256: str,
    venv_python: Path,
    pip_runner_python: Path,
) -> dict[str, Any]:
    """Validate a live runtime from the single canonical release root.

    The envelope and its nested manifests derive all build/runtime leaves.
    Callers supply only the release root and non-digest locators.  Repository
    source is identified by the envelope's Git commit/tree and a clean checkout,
    never by repeating hashes for individual tracked files.
    """

    root = _absolute(repository_root).resolve(strict=True)
    if not root.is_dir():
        raise LockedRuntimeError("repository root is not a directory")
    authority = load_deployment_envelope(
        envelope_path,
        expected_root_sha256=expected_envelope_sha256,
        buy_e3_enabled=False,
    )
    if (
        _git_output(root, "rev-parse", "HEAD") != authority["execution_commit"]
        or _git_output(root, "rev-parse", "HEAD^{tree}") != authority["execution_tree"]
        or _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise LockedRuntimeError("startup checkout differs from deployment release root")

    install_receipt_path = Path(authority["install_receipt_path"])
    expected_venv = install_receipt_path.parent / (
        f"venv-{authority['execution_commit']}"
    )
    selected_venv = root / ".venv-active"
    try:
        selector_target = os.readlink(selected_venv)
        resolved_selector = selected_venv.resolve(strict=True)
        resolved_python = _absolute(venv_python).resolve(strict=True)
    except OSError as exc:
        raise LockedRuntimeError("startup venv selector authority is unavailable") from exc
    expected_python = expected_venv / "bin" / "python3"
    if (
        not selected_venv.is_symlink()
        or selector_target != str(expected_venv)
        or resolved_selector != expected_venv
        or expected_venv.is_symlink()
        or not expected_venv.is_dir()
        or resolved_python != expected_python
    ):
        raise LockedRuntimeError("startup venv selector differs from deployment release root")

    interpreter = _require_mapping(
        authority.get("locked_runtime_interpreter"),
        "deployment runtime interpreter",
    )
    receipt = validate_startup_runtime(
        venv_python=resolved_python,
        pip_runner_python=pip_runner_python,
        receipt_path=install_receipt_path,
        expected_receipt_sha256=str(authority["install_receipt_canonical_sha256"]),
        expected_lock_sha256=str(authority["runtime_lock_canonical_sha256"]),
        expected_wheelhouse_sha256=str(authority["wheelhouse_canonical_sha256"]),
        expected_root_wheel_sha256=str(authority["root_wheel_sha256"]),
        expected_native_wheel_sha256=str(authority["native_wheel_sha256"]),
        expected_python_version=str(interpreter["version"]),
        expected_soabi=str(interpreter["soabi"]),
        expected_compiler=str(interpreter["compiler"]),
        expected_openssl_runtime=str(interpreter["openssl_runtime"]),
        expected_interpreter_executable_sha256=str(interpreter["executable_sha256"]),
    )
    return {
        "status": "deployment_envelope_startup_verified",
        "canonical_sha256": authority["canonical_sha256"],
        "receipt": receipt,
    }


def _summary(payload: dict[str, Any], canonical_field: str) -> None:
    print(
        json.dumps(
            {canonical_field: payload[canonical_field], "status": payload["status"]},
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Registered as real commands as well as fast-path-dispatched in __main__.
    # This keeps programmatic main([...]) and subprocess probes equivalent.
    subparsers.add_parser("_probe-interpreter", help=argparse.SUPPRESS)
    subparsers.add_parser("_snapshot-seed", help=argparse.SUPPRESS)
    subparsers.add_parser("_snapshot-installed", help=argparse.SUPPRESS)

    lock = subparsers.add_parser("lock", help="freeze a resolved seed venv")
    lock.add_argument("--seed-python", type=Path, required=True)
    lock.add_argument("--output", type=Path, required=True)
    lock.add_argument("--generated-utc")
    lock.add_argument("--exclude", action="append", default=[])

    receive = subparsers.add_parser("wheelhouse-receive", help="ingest exact wheels")
    receive.add_argument("--lock", type=Path, required=True)
    receive.add_argument("--expected-lock-sha256", required=True)
    receive.add_argument("--wheel", action="append", type=Path, required=True)
    receive.add_argument("--output-dir", type=Path, required=True)

    download = subparsers.add_parser("wheelhouse-download", help="download exact wheels")
    download.add_argument("--lock", type=Path, required=True)
    download.add_argument("--expected-lock-sha256", required=True)
    download.add_argument("--pip-python", type=Path, required=True)
    download.add_argument("--output-dir", type=Path, required=True)

    verify_wheelhouse = subparsers.add_parser("wheelhouse-verify")
    verify_wheelhouse.add_argument("--lock", type=Path, required=True)
    verify_wheelhouse.add_argument("--expected-lock-sha256", required=True)
    verify_wheelhouse.add_argument("--wheelhouse", type=Path, required=True)
    verify_wheelhouse.add_argument("--expected-wheelhouse-sha256", required=True)

    install = subparsers.add_parser("install", help="build a fresh offline venv")
    verify = subparsers.add_parser("verify-install", help="verify an installed venv")
    for command in (install, verify):
        command.add_argument("--builder-python", type=Path, required=True)
        command.add_argument("--venv", type=Path, required=True)
        command.add_argument("--lock", type=Path, required=True)
        command.add_argument("--expected-lock-sha256", required=True)
        command.add_argument("--wheelhouse", type=Path, required=True)
        command.add_argument("--expected-wheelhouse-sha256", required=True)
        command.add_argument("--root-wheel", type=Path, required=True)
        command.add_argument("--root-wheel-sha256", required=True)
        command.add_argument("--native-wheel", type=Path, required=True)
        command.add_argument("--native-wheel-sha256", required=True)
        command.add_argument("--receipt", type=Path, required=True)
    install.add_argument("--generated-utc")
    verify.add_argument("--expected-receipt-sha256", required=True)

    static_tree = subparsers.add_parser(
        "verify-static-tree",
        help="verify an installed tree without starting its interpreter",
    )
    static_tree.add_argument("--venv", type=Path, required=True)
    static_tree.add_argument("--receipt", type=Path, required=True)
    static_tree.add_argument("--expected-receipt-sha256", required=True)

    envelope = subparsers.add_parser(
        "build-envelope",
        help="derive a generic deployment envelope from frozen runtime authorities",
    )
    envelope.add_argument("--repository-root", type=Path, required=True)
    envelope.add_argument("--active-config", type=Path, required=True)
    envelope.add_argument("--disabled-config", type=Path)
    envelope.add_argument("--native-build-receipt", type=Path, required=True)
    envelope.add_argument("--policy-artifact-manifest", type=Path)
    envelope.add_argument("--policy-file", type=Path)
    envelope.add_argument("--predicate-bundle", type=Path)
    envelope.add_argument("--output", type=Path, required=True)
    startup = subparsers.add_parser(
        "verify-envelope-startup",
        help="verify live runtime from one canonical deployment envelope root",
    )
    startup.add_argument("--repository-root", type=Path, required=True)
    startup.add_argument("--envelope", type=Path, required=True)
    startup.add_argument("--expected-envelope-sha256", required=True)
    startup.add_argument("--venv-python", type=Path, required=True)
    startup.add_argument("--pip-runner-python", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "_probe-interpreter":
            print(
                json.dumps(_current_interpreter_snapshot(), sort_keys=True, separators=(",", ":"))
            )
            return 0
        if args.command == "_snapshot-seed":
            print(json.dumps(_seed_snapshot_current(), sort_keys=True, separators=(",", ":")))
            return 0
        if args.command == "_snapshot-installed":
            print(json.dumps(_installed_snapshot_current(), sort_keys=True, separators=(",", ":")))
            return 0
        if args.command == "lock":
            excluded = args.exclude or list(DEFAULT_EXCLUDED_DISTRIBUTIONS)
            result = generate_lock(
                seed_python=args.seed_python,
                output_path=args.output,
                generated_utc=args.generated_utc,
                excluded_names=excluded,
            )
            _summary(result["lock"], LOCK_CANONICAL_FIELD)
            return 0
        if args.command == "wheelhouse-receive":
            result = receive_wheelhouse(
                lock_path=args.lock,
                expected_lock_sha256=args.expected_lock_sha256,
                wheel_paths=args.wheel,
                output_dir=args.output_dir,
            )
            _summary(result["manifest"], WHEELHOUSE_CANONICAL_FIELD)
            return 0
        if args.command == "wheelhouse-download":
            result = download_wheelhouse(
                lock_path=args.lock,
                expected_lock_sha256=args.expected_lock_sha256,
                pip_python=args.pip_python,
                output_dir=args.output_dir,
            )
            _summary(result["manifest"], WHEELHOUSE_CANONICAL_FIELD)
            return 0
        if args.command == "wheelhouse-verify":
            payload = validate_wheelhouse(
                lock_path=args.lock,
                expected_lock_sha256=args.expected_lock_sha256,
                wheelhouse_dir=args.wheelhouse,
                expected_manifest_sha256=args.expected_wheelhouse_sha256,
            )
            _summary(payload, WHEELHOUSE_CANONICAL_FIELD)
            return 0
        if args.command == "install":
            result = install_locked_runtime(
                builder_python=args.builder_python,
                venv_dir=args.venv,
                lock_path=args.lock,
                expected_lock_sha256=args.expected_lock_sha256,
                wheelhouse_dir=args.wheelhouse,
                expected_wheelhouse_sha256=args.expected_wheelhouse_sha256,
                root_wheel_path=args.root_wheel,
                root_wheel_sha256=args.root_wheel_sha256,
                native_wheel_path=args.native_wheel,
                native_wheel_sha256=args.native_wheel_sha256,
                receipt_path=args.receipt,
                generated_utc=args.generated_utc,
            )
            _summary(result["receipt"], INSTALL_CANONICAL_FIELD)
            return 0
        if args.command == "verify-install":
            payload = validate_installed_runtime(
                venv_python=args.venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python"),
                pip_runner_python=args.builder_python,
                receipt_path=args.receipt,
                expected_receipt_sha256=args.expected_receipt_sha256,
                lock_path=args.lock,
                expected_lock_sha256=args.expected_lock_sha256,
                wheelhouse_dir=args.wheelhouse,
                expected_wheelhouse_sha256=args.expected_wheelhouse_sha256,
                root_wheel_path=args.root_wheel,
                root_wheel_sha256=args.root_wheel_sha256,
                native_wheel_path=args.native_wheel,
                native_wheel_sha256=args.native_wheel_sha256,
            )
            _summary(payload, INSTALL_CANONICAL_FIELD)
            return 0
        if args.command == "verify-static-tree":
            payload = validate_static_installed_tree(
                venv_dir=args.venv,
                receipt_path=args.receipt,
                expected_receipt_sha256=args.expected_receipt_sha256,
            )
            _summary(payload, INSTALL_CANONICAL_FIELD)
            return 0
        if args.command == "build-envelope":
            result = build_deployment_envelope(
                repository_root=args.repository_root,
                active_config_path=args.active_config,
                disabled_config_path=args.disabled_config,
                native_build_receipt_path=args.native_build_receipt,
                policy_artifact_manifest_path=args.policy_artifact_manifest,
                policy_file_path=args.policy_file,
                predicate_bundle_path=args.predicate_bundle,
                output_path=args.output,
            )
            print(
                json.dumps(
                    {
                        "canonical_sha256": result["canonical_sha256"],
                        "path": result["path"],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return 0
        if args.command == "verify-envelope-startup":
            result = validate_deployment_envelope_startup(
                repository_root=args.repository_root,
                envelope_path=args.envelope,
                expected_envelope_sha256=args.expected_envelope_sha256,
                venv_python=args.venv_python,
                pip_runner_python=args.pip_runner_python,
            )
            print(
                json.dumps(
                    {
                        "canonical_sha256": result["canonical_sha256"],
                        "status": result["status"],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return 0
        raise LockedRuntimeError(f"unsupported command: {args.command}")
    except LockedRuntimeError as exc:
        print(f"locked runtime error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in {
        "_probe-interpreter",
        "_snapshot-seed",
        "_snapshot-installed",
    }:
        private = sys.argv[1]
        if private == "_probe-interpreter":
            value = _current_interpreter_snapshot()
        elif private == "_snapshot-seed":
            value = _seed_snapshot_current()
        else:
            value = _installed_snapshot_current()
        print(json.dumps(value, sort_keys=True, separators=(",", ":")))
        raise SystemExit(0)
    raise SystemExit(main())
