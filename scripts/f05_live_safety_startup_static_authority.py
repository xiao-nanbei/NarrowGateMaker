#!/usr/bin/env python3
"""Verify the persistent successor runtime before target Python can execute.

This module is deliberately standard-library-only.  It must be launched by a
release-frozen trusted system interpreter with ``-I -S``.  Only after it has
validated its own bytes, the authority document, checkout/selector identity,
the safety release, and the complete installed tree may ``live/run.sh`` start
the target virtual-environment interpreter.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, Final

SCHEMA: Final = "narrowgate_startup_static_runtime_authority.v1"
STATUS: Final = "trusted_static_gate_required_before_every_target_python"
CANONICAL_FIELD: Final = "canonical_startup_static_runtime_authority_sha256"
SUCCESSOR_RELEASE_SCHEMA: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
    "direct_owner_live_safety_successor.v1"
)
SUCCESSOR_RELEASE_STATUS: Final = (
    "owner_authorized_direct_live_safety_successor_pending_runtime_evidence"
)
SUCCESSOR_RELEASE_CANONICAL_FIELD: Final = "canonical_active_release_sha256"

_TOP_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "execution",
        "repository",
        "venv_selector",
        "trusted_python",
        "authority_verifier",
        "locked_runtime_verifier",
        "target_runtime",
        "safety_release",
        CANONICAL_FIELD,
    }
)
_EXECUTION_FIELDS = frozenset({"commit", "tree"})
_REPOSITORY_FIELDS = frozenset({"path", "runtime_root"})
_SELECTOR_FIELDS = frozenset({"path", "target"})
_BYTE_FILE_FIELDS = frozenset({"path", "sha256"})
_TARGET_FIELDS = frozenset(
    {
        "venv_path",
        "python_path",
        "python_sha256",
        "install_receipt_path",
        "install_receipt_file_sha256",
        "install_receipt_canonical_sha256",
        "installed_record_aggregate_sha256",
    }
)
_RELEASE_FIELDS = frozenset({"path", "file_sha256", "canonical_sha256"})
_NATIVE_RELEASE_PROJECTION = {
    "install_receipt_path": "install_receipt_path",
    "install_receipt_file_sha256": "install_receipt_file_sha256",
    "install_receipt_canonical_sha256": "install_receipt_canonical_sha256",
    "installed_record_aggregate_sha256": "installed_record_aggregate_sha256",
}
_FORBIDDEN_BYTECODE_SUFFIXES = frozenset({".pyc", ".pyo"})
_IMPORT_EXTENSION_SUFFIXES = frozenset({".dylib", ".pyd", ".so"})
_NON_IMPORT_BUILD_ROOTS = frozenset({".venv", "build", "dist"})


class StartupStaticAuthorityError(RuntimeError):
    """Raised when the target runtime cannot be trusted before execution."""


def _canonical_bytes(payload: Mapping[str, Any], field: str) -> bytes:
    projected = dict(payload)
    projected.pop(field, None)
    return json.dumps(
        projected,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def canonical_sha256(payload: Mapping[str, Any], field: str = CANONICAL_FIELD) -> str:
    return hashlib.sha256(_canonical_bytes(payload, field)).hexdigest()


def file_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise StartupStaticAuthorityError(f"{label} is not a SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 40 or any(char not in "0123456789abcdef" for char in normalized):
        raise StartupStaticAuthorityError(f"{label} is not a Git SHA")
    return normalized


def _absolute_posix(value: Any, label: str) -> str:
    text = str(value).strip()
    path = PurePosixPath(text)
    if not text or "\x00" in text or not path.is_absolute() or ".." in path.parts:
        raise StartupStaticAuthorityError(f"{label} is not an absolute normalized path")
    return str(path)


def build_authority(
    *,
    execution_commit: str,
    execution_tree: str,
    repository_path: str,
    runtime_root: str,
    selector_path: str,
    selector_target: str,
    trusted_python_path: str,
    trusted_python_sha256: str,
    authority_verifier_path: str,
    authority_verifier_sha256: str,
    locked_runtime_verifier_path: str,
    locked_runtime_verifier_sha256: str,
    venv_path: str,
    target_python_path: str,
    target_python_sha256: str,
    install_receipt_path: str,
    install_receipt_file_sha256: str,
    install_receipt_canonical_sha256: str,
    installed_record_aggregate_sha256: str,
    safety_release_path: str,
    safety_release_file_sha256: str,
    safety_release_canonical_sha256: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "execution": {
            "commit": _require_git_sha(execution_commit, "execution commit"),
            "tree": _require_git_sha(execution_tree, "execution tree"),
        },
        "repository": {
            "path": _absolute_posix(repository_path, "repository path"),
            "runtime_root": _absolute_posix(runtime_root, "runtime root"),
        },
        "venv_selector": {
            "path": _absolute_posix(selector_path, "venv selector path"),
            "target": _absolute_posix(selector_target, "venv selector target"),
        },
        "trusted_python": {
            "path": _absolute_posix(trusted_python_path, "trusted Python path"),
            "sha256": _require_sha256(trusted_python_sha256, "trusted Python"),
        },
        "authority_verifier": {
            "path": _absolute_posix(authority_verifier_path, "authority verifier path"),
            "sha256": _require_sha256(authority_verifier_sha256, "authority verifier"),
        },
        "locked_runtime_verifier": {
            "path": _absolute_posix(
                locked_runtime_verifier_path, "locked runtime verifier path"
            ),
            "sha256": _require_sha256(
                locked_runtime_verifier_sha256, "locked runtime verifier"
            ),
        },
        "target_runtime": {
            "venv_path": _absolute_posix(venv_path, "target venv path"),
            "python_path": _absolute_posix(target_python_path, "target Python path"),
            "python_sha256": _require_sha256(target_python_sha256, "target Python"),
            "install_receipt_path": _absolute_posix(
                install_receipt_path, "install receipt path"
            ),
            "install_receipt_file_sha256": _require_sha256(
                install_receipt_file_sha256, "install receipt file"
            ),
            "install_receipt_canonical_sha256": _require_sha256(
                install_receipt_canonical_sha256, "install receipt canonical"
            ),
            "installed_record_aggregate_sha256": _require_sha256(
                installed_record_aggregate_sha256, "installed tree aggregate"
            ),
        },
        "safety_release": {
            "path": _absolute_posix(safety_release_path, "safety release path"),
            "file_sha256": _require_sha256(
                safety_release_file_sha256, "safety release file"
            ),
            "canonical_sha256": _require_sha256(
                safety_release_canonical_sha256, "safety release canonical"
            ),
        },
    }
    if (
        payload["repository"]["runtime_root"]
        != str(
            PurePosixPath(payload["target_runtime"]["venv_path"]).parent
            / f"runtime-{payload['execution']['commit']}"
        )
        or payload["venv_selector"]["path"]
        != str(PurePosixPath(payload["repository"]["path"]) / ".venv-active")
        or payload["venv_selector"]["target"]
        != payload["target_runtime"]["venv_path"]
        or payload["target_runtime"]["python_path"]
        != str(PurePosixPath(payload["target_runtime"]["venv_path"]) / "bin/python3")
        or payload["authority_verifier"]["path"]
        != str(
            PurePosixPath(payload["repository"]["runtime_root"])
            / "scripts/f05_live_safety_startup_static_authority.py"
        )
        or payload["locked_runtime_verifier"]["path"]
        != str(
            PurePosixPath(payload["repository"]["runtime_root"])
            / "scripts/f05_live_safety_locked_runtime.py"
        )
    ):
        raise StartupStaticAuthorityError("startup static authority path derivation drifted")
    payload[CANONICAL_FIELD] = canonical_sha256(payload)
    return payload


def validate_payload(
    raw: Mapping[str, Any], *, expected_canonical_sha256: str
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _TOP_FIELDS:
        raise StartupStaticAuthorityError("startup static authority fields drifted")
    payload = dict(raw)
    if payload.get("schema_version") != SCHEMA or payload.get("status") != STATUS:
        raise StartupStaticAuthorityError("startup static authority identity drifted")
    for field, expected in (
        ("execution", _EXECUTION_FIELDS),
        ("repository", _REPOSITORY_FIELDS),
        ("venv_selector", _SELECTOR_FIELDS),
        ("trusted_python", _BYTE_FILE_FIELDS),
        ("authority_verifier", _BYTE_FILE_FIELDS),
        ("locked_runtime_verifier", _BYTE_FILE_FIELDS),
        ("target_runtime", _TARGET_FIELDS),
        ("safety_release", _RELEASE_FIELDS),
    ):
        value = payload.get(field)
        if not isinstance(value, Mapping) or set(value) != expected:
            raise StartupStaticAuthorityError(
                f"startup static authority {field} fields drifted"
            )
    rebuilt = build_authority(
        execution_commit=payload["execution"]["commit"],
        execution_tree=payload["execution"]["tree"],
        repository_path=payload["repository"]["path"],
        runtime_root=payload["repository"]["runtime_root"],
        selector_path=payload["venv_selector"]["path"],
        selector_target=payload["venv_selector"]["target"],
        trusted_python_path=payload["trusted_python"]["path"],
        trusted_python_sha256=payload["trusted_python"]["sha256"],
        authority_verifier_path=payload["authority_verifier"]["path"],
        authority_verifier_sha256=payload["authority_verifier"]["sha256"],
        locked_runtime_verifier_path=payload["locked_runtime_verifier"]["path"],
        locked_runtime_verifier_sha256=payload["locked_runtime_verifier"]["sha256"],
        venv_path=payload["target_runtime"]["venv_path"],
        target_python_path=payload["target_runtime"]["python_path"],
        target_python_sha256=payload["target_runtime"]["python_sha256"],
        install_receipt_path=payload["target_runtime"]["install_receipt_path"],
        install_receipt_file_sha256=payload["target_runtime"][
            "install_receipt_file_sha256"
        ],
        install_receipt_canonical_sha256=payload["target_runtime"][
            "install_receipt_canonical_sha256"
        ],
        installed_record_aggregate_sha256=payload["target_runtime"][
            "installed_record_aggregate_sha256"
        ],
        safety_release_path=payload["safety_release"]["path"],
        safety_release_file_sha256=payload["safety_release"]["file_sha256"],
        safety_release_canonical_sha256=payload["safety_release"]["canonical_sha256"],
    )
    expected = _require_sha256(
        expected_canonical_sha256, "expected startup static authority canonical"
    )
    if payload != rebuilt or payload.get(CANONICAL_FIELD) != expected:
        raise StartupStaticAuthorityError("startup static authority canonical drifted")
    return payload


def _read_regular(path: Path, *, private: bool) -> bytes:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink():
        raise StartupStaticAuthorityError(f"authority path is a symlink: {candidate}")
    descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        metadata = os.fstat(descriptor)
        allowed_modes = (
            {0o400, 0o600}
            if private
            else {0o400, 0o444, 0o500, 0o555, 0o755}
        )
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) not in allowed_modes
        ):
            raise StartupStaticAuthorityError(
                f"authority file inode/mode is unsafe: {candidate}"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise StartupStaticAuthorityError(
                f"authority file changed while read: {candidate}"
            )
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def load_authority(
    path: Path, *, expected_file_sha256: str, expected_canonical_sha256: str
) -> dict[str, Any]:
    raw = _read_regular(path, private=True)
    if hashlib.sha256(raw).hexdigest() != _require_sha256(
        expected_file_sha256, "expected startup static authority file"
    ):
        raise StartupStaticAuthorityError("startup static authority file hash drifted")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StartupStaticAuthorityError("startup static authority is not JSON") from exc
    return validate_payload(
        payload, expected_canonical_sha256=expected_canonical_sha256
    )


def _sha256_file(path: Path, *, private: bool = False) -> str:
    return hashlib.sha256(_read_regular(path, private=private)).hexdigest()


def _git_identity(root: Path) -> tuple[str, str]:
    def output(*args: str) -> str:
        completed = subprocess.run(
            ("/usr/bin/git", "-C", str(root), *args),
            check=False,
            capture_output=True,
            text=True,
            timeout=20.0,
            env={
                "HOME": os.environ.get("HOME", ""),
                "PATH": "/usr/bin:/bin",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
            },
        )
        if completed.returncode != 0:
            raise StartupStaticAuthorityError(
                f"cannot validate repository identity: {' '.join(args)}"
            )
        return completed.stdout.strip()

    commit = output("rev-parse", "HEAD")
    tree = output("rev-parse", "HEAD^{tree}")
    if output("status", "--porcelain=v1", "--untracked-files=all"):
        raise StartupStaticAuthorityError("repository worktree is not clean")
    return commit, tree


def _reject_ignored_import_artifacts(root: Path) -> None:
    """Reject ignored executable Python artifacts outside isolated build roots.

    A clean Git status intentionally omits ignored files.  That is insufficient
    before importing from a checkout because CPython can execute an ignored
    ``__pycache__`` entry or native extension while the tracked tree still
    matches the frozen commit.  The isolated venv/build directories are not
    import roots for the live checkout and are verified by the locked-runtime
    tree contract separately.
    """

    completed = subprocess.run(
        (
            "/usr/bin/git",
            "-C",
            str(root),
            "status",
            "--porcelain=v1",
            "-z",
            "--ignored=matching",
            "--untracked-files=all",
        ),
        check=False,
        capture_output=True,
        timeout=20.0,
        env={
            "HOME": os.environ.get("HOME", ""),
            "PATH": "/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
        },
    )
    if completed.returncode != 0:
        raise StartupStaticAuthorityError(
            "cannot scan ignored repository import artifacts"
        )
    for raw_entry in completed.stdout.split(b"\0"):
        if not raw_entry.startswith(b"!! "):
            continue
        relative = raw_entry[3:].decode("utf-8", errors="surrogateescape").rstrip("/")
        parts = PurePosixPath(relative).parts
        if not parts or parts[0] in _NON_IMPORT_BUILD_ROOTS:
            continue
        suffix = PurePosixPath(relative).suffix.lower()
        if (
            "__pycache__" in parts
            or suffix in _FORBIDDEN_BYTECODE_SUFFIXES
            or suffix in _IMPORT_EXTENSION_SUFFIXES
        ):
            raise StartupStaticAuthorityError(
                f"ignored executable import artifact is forbidden: {relative}"
            )


def _load_locked_verifier(path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "narrowgate_trusted_locked_runtime_verifier", path
    )
    if specification is None or specification.loader is None:
        raise StartupStaticAuthorityError("cannot load locked runtime verifier")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def verify_startup_static_runtime(
    *,
    authority_path: Path,
    expected_authority_file_sha256: str,
    expected_authority_canonical_sha256: str,
    candidate_only: bool = False,
) -> dict[str, Any]:
    sys.dont_write_bytecode = True
    if os.environ.get("PYTHONDONTWRITEBYTECODE") != "1" or os.environ.get(
        "PYTHONNOUSERSITE"
    ) != "1":
        raise StartupStaticAuthorityError("trusted static verifier environment is unsafe")
    authority = load_authority(
        authority_path,
        expected_file_sha256=expected_authority_file_sha256,
        expected_canonical_sha256=expected_authority_canonical_sha256,
    )
    trusted = authority["trusted_python"]
    trusted_path = Path(trusted["path"])
    if Path(sys.executable).absolute() != trusted_path or _sha256_file(
        trusted_path
    ) != trusted["sha256"]:
        raise StartupStaticAuthorityError("trusted static Python identity drifted")
    self_binding = authority["authority_verifier"]
    self_path = Path(self_binding["path"])
    if Path(__file__).absolute() != self_path or _sha256_file(self_path) != self_binding[
        "sha256"
    ]:
        raise StartupStaticAuthorityError("startup authority verifier bytes drifted")

    execution = authority["execution"]
    repository = authority["repository"]
    repository_path = Path(repository["path"])
    runtime_root = Path(repository["runtime_root"])
    if _git_identity(runtime_root) != (execution["commit"], execution["tree"]):
        raise StartupStaticAuthorityError("isolated runtime repository identity drifted")
    _reject_ignored_import_artifacts(runtime_root)
    # Candidate validation deliberately does not require the active checkout to
    # have switched commits yet, but it must still reject ignored executable
    # artifacts before the old process is stopped.
    _reject_ignored_import_artifacts(repository_path)
    if not candidate_only:
        if _git_identity(repository_path) != (execution["commit"], execution["tree"]):
            raise StartupStaticAuthorityError("active repository identity drifted")

    selector = authority["venv_selector"]
    selector_path = Path(selector["path"])
    target = authority["target_runtime"]
    venv_path = Path(target["venv_path"])
    if not candidate_only:
        selector_metadata = selector_path.lstat()
        if not stat.S_ISLNK(selector_metadata.st_mode):
            raise StartupStaticAuthorityError("runtime selector is not an exact symlink")
        if os.readlink(selector_path) != selector["target"]:
            raise StartupStaticAuthorityError("runtime selector target drifted")
        if selector_path.resolve(strict=True) != venv_path:
            raise StartupStaticAuthorityError("runtime selector resolution drifted")
    python_path = Path(target["python_path"])
    if _sha256_file(python_path) != target["python_sha256"]:
        raise StartupStaticAuthorityError("target Python bytes drifted")

    release_binding = authority["safety_release"]
    release_raw = _read_regular(Path(release_binding["path"]), private=True)
    if hashlib.sha256(release_raw).hexdigest() != release_binding["file_sha256"]:
        raise StartupStaticAuthorityError("safety release file hash drifted")
    try:
        release = json.loads(release_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StartupStaticAuthorityError("safety release is not JSON") from exc
    if (
        release.get("schema_version") != SUCCESSOR_RELEASE_SCHEMA
        or release.get("status") != SUCCESSOR_RELEASE_STATUS
        or release.get(SUCCESSOR_RELEASE_CANONICAL_FIELD)
        != release_binding["canonical_sha256"]
        or canonical_sha256(release, SUCCESSOR_RELEASE_CANONICAL_FIELD)
        != release_binding["canonical_sha256"]
        or release.get("execution", {}).get("execution_commit") != execution["commit"]
        or release.get("execution", {}).get("execution_tree") != execution["tree"]
    ):
        raise StartupStaticAuthorityError("safety release authority drifted")
    native = release.get("native_build")
    if not isinstance(native, Mapping):
        raise StartupStaticAuthorityError("safety release lacks native runtime authority")
    if any(
        native.get(release_field) != target[authority_field]
        for authority_field, release_field in _NATIVE_RELEASE_PROJECTION.items()
    ) or native.get("interpreter", {}).get("executable_sha256") != target[
        "python_sha256"
    ]:
        raise StartupStaticAuthorityError("static authority differs from safety release")

    locked_binding = authority["locked_runtime_verifier"]
    locked_path = Path(locked_binding["path"])
    if _sha256_file(locked_path) != locked_binding["sha256"]:
        raise StartupStaticAuthorityError("locked runtime verifier bytes drifted")
    locked_module = _load_locked_verifier(locked_path)
    try:
        receipt = locked_module.validate_static_installed_tree(
            venv_dir=venv_path,
            receipt_path=Path(target["install_receipt_path"]),
            expected_receipt_sha256=target["install_receipt_canonical_sha256"],
        )
    except Exception as exc:
        raise StartupStaticAuthorityError("installed runtime static tree drifted") from exc
    receipt_raw = _read_regular(Path(target["install_receipt_path"]), private=True)
    if (
        hashlib.sha256(receipt_raw).hexdigest()
        != target["install_receipt_file_sha256"]
        or receipt.get("installed_record_aggregate_sha256")
        != target["installed_record_aggregate_sha256"]
    ):
        raise StartupStaticAuthorityError("installed runtime receipt authority drifted")
    return {
        "schema_version": SCHEMA,
        "status": (
            "trusted_static_runtime_candidate_verified"
            if candidate_only
            else "trusted_static_runtime_verified"
        ),
        "authority_file_sha256": expected_authority_file_sha256,
        "authority_canonical_sha256": expected_authority_canonical_sha256,
        "execution_commit": execution["commit"],
        "execution_tree": execution["tree"],
        "installed_record_aggregate_sha256": target[
            "installed_record_aggregate_sha256"
        ],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--expected-file-sha256", required=True)
    parser.add_argument("--expected-canonical-sha256", required=True)
    parser.add_argument("--candidate-only", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = verify_startup_static_runtime(
            authority_path=args.authority,
            expected_authority_file_sha256=args.expected_file_sha256,
            expected_authority_canonical_sha256=args.expected_canonical_sha256,
            candidate_only=bool(args.candidate_only),
        )
    except StartupStaticAuthorityError as exc:
        print(f"startup static authority error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
