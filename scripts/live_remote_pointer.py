"""Fail-closed resolver for the local current-live-host pointer."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

POINTER_SCHEMA_VERSION = "narrowgate_live_remote_pointer.v1"
ACTIVE_STATUS = "current_active"
CONFIG_RECONCILIATION_STATUS = "completed_release_v3_no_shadow_current_config_locator_reconciled"
CURRENT_LIVE_CONFIG_SHA256 = "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
CURRENT_RELEASE_FILE_SHA256 = "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49"
CURRENT_RELEASE_CANONICAL_SHA256 = (
    "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f"
)
CURRENT_RUNTIME_COMMIT = "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de"
CURRENT_RUNTIME_TREE = "0343bd5586b337385cf2aa0d7a643f5c32b0da77"
CURRENT_RUNTIME_TAG_OBJECT = "3878ea05252ef8f274b6f74ee7a984431c53b892"
CURRENT_ACTIVATION_RECEIPT_FILE_SHA256 = (
    "44b6482c0448dfdf773950e94b96fb9b379f4665c4ec3c41ca1241c7fc40aaa9"
)
CURRENT_ACTIVATION_RECEIPT_CANONICAL_SHA256 = (
    "6e4269fd821fa943c9888661fa99c28ec4d59eb5457e4103ad930da40f488f7e"
)
MAX_PRIVATE_AUTHORITY_BYTES = 64 << 20


class LiveRemotePointerError(ValueError):
    """Raised when no unambiguous active deployment host can be resolved."""


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LiveRemotePointerError(f"duplicate private authority JSON key: {key}")
        result[key] = value
    return result


def _open_parent_nofollow(path: Path) -> tuple[Path, int]:
    target = Path(os.path.abspath(os.fspath(path.expanduser())))
    parts = target.parts
    if not target.is_absolute() or len(parts) < 2 or target.name in {"", ".", ".."}:
        raise LiveRemotePointerError("private current authority path is malformed")
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise LiveRemotePointerError("secure no-follow private reads are unsupported")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    directory_fd: int | None = None
    try:
        directory_fd = os.open(target.anchor, flags)
        for component in parts[1:-1]:
            next_fd = os.open(component, flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
    except OSError as exc:
        if directory_fd is not None:
            os.close(directory_fd)
        raise LiveRemotePointerError(
            "private authority parent is unavailable or contains a symlink"
        ) from exc
    if directory_fd is None:
        raise LiveRemotePointerError("private authority parent is unavailable")
    return target, directory_fd


def _read_private_regular(path: Path) -> bytes:
    target, directory_fd = _open_parent_nofollow(path)
    descriptor: int | None = None
    try:
        before = os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        os.close(directory_fd)
        raise LiveRemotePointerError("private current authority file is unavailable") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
        or before.st_size > MAX_PRIVATE_AUTHORITY_BYTES
    ):
        os.close(directory_fd)
        raise LiveRemotePointerError("private current authority file identity is unsafe")
    try:
        descriptor = os.open(
            target.name,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise LiveRemotePointerError("private authority file changed while opening")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            chunks.append(chunk)
        after_fd = os.fstat(descriptor)
    except OSError as exc:
        os.close(directory_fd)
        raise LiveRemotePointerError("private current authority file is unavailable") from exc
    except LiveRemotePointerError:
        os.close(directory_fd)
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)
    try:
        after_path = os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
        parent_identity = os.fstat(directory_fd)
    finally:
        os.close(directory_fd)
    _target_again, directory_fd_again = _open_parent_nofollow(target)
    try:
        parent_identity_again = os.fstat(directory_fd_again)
    finally:
        os.close(directory_fd_again)
    identity = lambda value: (  # noqa: E731 - compact immutable stat projection
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
    if (
        identity(before) != identity(after_fd)
        or identity(before) != identity(after_path)
        or (parent_identity.st_dev, parent_identity.st_ino)
        != (parent_identity_again.st_dev, parent_identity_again.st_ino)
    ):
        raise LiveRemotePointerError("private authority file changed while reading")
    return b"".join(chunks)


def load_live_remote_pointer(root: Path) -> dict[str, Any]:
    pointer_path = Path(
        os.environ.get(
            "NARROWGATE_LIVE_REMOTE_POINTER",
            str(root / "docs/private/live_remote.current.local.json"),
        )
    ).expanduser()
    if not pointer_path.is_file():
        raise LiveRemotePointerError(
            "current live remote pointer is unavailable; pass --remote explicitly"
        )
    try:
        payload = json.loads(
            _read_private_regular(pointer_path),
            object_pairs_hook=_strict_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveRemotePointerError("current live remote pointer JSON is invalid") from exc
    if not isinstance(payload, dict):
        raise LiveRemotePointerError("current live remote pointer must be a JSON object")
    if payload.get("schema_version") != POINTER_SCHEMA_VERSION:
        raise LiveRemotePointerError("unsupported current live remote pointer schema")
    return payload


def _remote_fields_from_payload(
    payload: Mapping[str, Any],
    *,
    explicit_remote: str,
) -> dict[str, str]:
    if payload.get("status") != ACTIVE_STATUS:
        if explicit_remote:
            return {"ssh_target": explicit_remote}
        return {}

    required = ("ssh_target", "provider", "region", "city", "public_ipv4")
    resolved = {field: str(payload.get(field, "")).strip() for field in required}
    if not all(resolved.values()):
        if explicit_remote:
            return {"ssh_target": explicit_remote}
        return {}
    if explicit_remote and explicit_remote != resolved["ssh_target"]:
        return {"ssh_target": explicit_remote}
    repo_root = str(payload.get("repo_root", "")).strip()
    if repo_root:
        resolved["repo_root"] = repo_root
    return resolved


def active_live_remote_fields(root: Path) -> dict[str, str]:
    """Return one internally consistent active-host identity.

    An explicit ``NARROWGATE_LIVE_REMOTE`` is allowed for migration and recovery
    commands. Capture-source fields are not inferred from that single override;
    callers that need provenance must pass the corresponding source fields too.
    """

    explicit_remote = os.environ.get("NARROWGATE_LIVE_REMOTE", "").strip()
    try:
        payload = load_live_remote_pointer(root)
    except (OSError, json.JSONDecodeError, LiveRemotePointerError):
        if explicit_remote:
            return {"ssh_target": explicit_remote}
        return {}
    return _remote_fields_from_payload(payload, explicit_remote=explicit_remote)


def active_live_locator_fields(root: Path) -> dict[str, str]:
    """Resolve the current remote and its stable, hash-bound live config alias.

    Backtest callers must not use this helper: the mutable live alias follows
    the private current deployment, while replay defaults resolve their own
    immutable versioned config through ``models.backtest_config``.

    This is deliberately a locator resolver, not an evidence validator.  It
    cross-checks the pointer's frozen release/config projection before returning
    a path, but it does not reopen the reconciliation receipt or recursively
    grant release, activation, action-occurrence, or economic authority.
    """

    try:
        payload = load_live_remote_pointer(root)
    except (OSError, json.JSONDecodeError, LiveRemotePointerError):
        return {}
    remote = _remote_fields_from_payload(
        payload,
        explicit_remote=os.environ.get("NARROWGATE_LIVE_REMOTE", "").strip(),
    )
    if not remote or set(remote) == {"ssh_target"}:
        return {}
    reconciliation = payload.get("current_config_locator_reconciliation")
    if not isinstance(reconciliation, Mapping):
        return {}
    alias = reconciliation.get("stable_live_config_alias")
    receipt = reconciliation.get("receipt")
    if (
        reconciliation.get("status") != CONFIG_RECONCILIATION_STATUS
        or reconciliation.get("immutable_v6_activation_receipt_preserved") is not True
        or reconciliation.get("backtest_v12_config_may_resolve_to_live_alias") is not False
        or not isinstance(alias, Mapping)
        or not isinstance(receipt, Mapping)
    ):
        return {}
    raw_path = os.environ.get(
        "NARROWGATE_LIVE_CONFIG",
        str(root / "docs/private/live_config.current.local.yaml"),
    )
    config_path = Path(os.path.abspath(os.fspath(Path(raw_path).expanduser())))
    expected_path = Path(os.path.abspath(os.fspath(Path(str(alias.get("path", ""))).expanduser())))
    if config_path != expected_path or not config_path.is_file():
        return {}
    try:
        raw = _read_private_regular(config_path)
    except LiveRemotePointerError:
        return {}
    observed_sha256 = hashlib.sha256(raw).hexdigest()
    expected_sha256 = str(alias.get("sha256", ""))
    release = payload.get("current_buy_e3_release")
    activation = payload.get("current_activation_receipt")
    if (
        observed_sha256 != expected_sha256
        or observed_sha256 != str(payload.get("config_sha256", ""))
        or observed_sha256 != CURRENT_LIVE_CONFIG_SHA256
        or len(raw) != alias.get("bytes")
        or not isinstance(release, Mapping)
        or release.get("active_config_sha256") != observed_sha256
        or release.get("active_release_file_sha256") != CURRENT_RELEASE_FILE_SHA256
        or release.get("active_release_canonical_sha256") != CURRENT_RELEASE_CANONICAL_SHA256
        or release.get("execution_commit") != CURRENT_RUNTIME_COMMIT
        or release.get("execution_tree") != CURRENT_RUNTIME_TREE
        or release.get("annotated_tag_object") != CURRENT_RUNTIME_TAG_OBJECT
        or release.get("external_venues_enabled") is not False
        or release.get("global_flow_shadow_enabled") is not False
        or release.get("global_reference_shadow_enabled") is not False
        or not isinstance(activation, Mapping)
        or activation.get("sha256") != CURRENT_ACTIVATION_RECEIPT_FILE_SHA256
        or activation.get("canonical_sha256") != CURRENT_ACTIVATION_RECEIPT_CANONICAL_SHA256
        or not isinstance(receipt.get("sha256"), str)
        or len(str(receipt["sha256"])) != 64
        or any(character not in "0123456789abcdef" for character in receipt["sha256"])
    ):
        return {}
    return {
        **remote,
        "live_config_path": str(config_path),
        "live_config_sha256": observed_sha256,
        "resolution_scope": "locator_only_not_evidence_authority",
    }


def require_remote_matches_source(remote: str, source_identity: Mapping[str, Any]) -> None:
    source_remote = str(source_identity.get("ssh_target", "")).strip()
    if not remote.strip():
        raise LiveRemotePointerError(
            "no current_active remote is resolved; pass --remote explicitly"
        )
    if source_remote != remote.strip():
        raise LiveRemotePointerError(
            "remote and capture source ssh_target differ; refusing mixed-host provenance"
        )
