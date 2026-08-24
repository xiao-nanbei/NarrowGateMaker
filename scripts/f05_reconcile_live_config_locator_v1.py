#!/usr/bin/env python3
"""Reconcile the private current-live config locator without changing live state.

The immutable release-v3 receipt already identifies the active no-shadow
configuration.  This successor repairs only the local repository locator:
it preserves the old v12 bytes for replay, creates a versioned release-v3
config, and then publishes an ordered metadata transaction.

``prepare-manifest`` is the only command that may create the durable manifest.
``run`` is write-free unless ``--apply`` is supplied.  Neither command contacts
a remote host, changes a strategy, reads economic outcomes, or grants research,
action, live, or backtest-economic authority.

Publication order is a strict recoverable prefix::

    immutable snapshots/configs/receipt
      -> stable live-config alias
      -> additive current-remote pointer successor
      -> private catalog successor

The immutable v6 activation receipt is never rewritten.  All mutable targets
share the existing ``docs/private`` transaction flock.
"""

from __future__ import annotations

import argparse
import contextvars
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from scripts import audit_private_evidence
from scripts import f05_closeout_operational_metadata_v6 as metadata_v6

MANIFEST_SCHEMA: Final = "f05_buy_e3_current_config_locator_reconciliation_manifest.v1"
MANIFEST_STATUS: Final = "release_v3_no_shadow_config_locator_inputs_frozen"
MANIFEST_CANONICAL_FIELD: Final = "canonical_config_locator_reconciliation_manifest_sha256"
MANIFEST_RECEIPT_ID: Final = "f05-buy-e3-no-shadow-config-locator-reconciliation-20260825-v1"

RECEIPT_SCHEMA: Final = "narrowgate.current_live_config_locator_reconciliation_receipt.v1"
RECEIPT_STATUS: Final = "completed_release_v3_no_shadow_current_config_locator_reconciled"
RECEIPT_CANONICAL_FIELD: Final = (
    "canonical_current_live_config_locator_reconciliation_receipt_sha256"
)

POINTER_SCHEMA: Final = "narrowgate_live_remote_pointer.v1"
CATALOG_SCHEMA: Final = "narrowgate_private_artifact_catalog_v1"
PUBLISHER_MODULE_ROUTE: Final = "scripts.f05_reconcile_live_config_locator_v1"
PUBLISHER_TAG: Final = "f05-owner-buy-e3-no-shadow-governance-source-v1-20260825"

PRIVATE_EVIDENCE_ROOT_ENV: Final = "NARROWGATE_PRIVATE_EVIDENCE_ROOT"
E3_V6_EVIDENCE_RELATIVE: Final = Path(
    "f05_owner_buy_e3_v1/direct_no_shadow_live_evidence_v6_20260824"
)
FORMAL_MANIFEST_RELATIVE: Final = Path(
    E3_V6_EVIDENCE_RELATIVE / "config_locator_reconciliation/reconciliation_manifest_v1.json"
)
ACTIVE_CONFIG_SOURCE_RELATIVE: Final = Path(
    E3_V6_EVIDENCE_RELATIVE / "authority_sources/config.direct_owner_active.no_shadow.v2.yaml"
)

CURRENT_ALIAS_FILENAME: Final = "live_config.current.local.yaml"
BACKTEST_V12_ARCHIVE_FILENAME: Final = "live_config.backtest_v12.800f4c025663.local.yaml"
RELEASE_V3_CONFIG_FILENAME: Final = "live_config.owner_buy_e3_no_shadow_release_v3_20260824_v1.yaml"
PREDECESSOR_POINTER_SNAPSHOT_FILENAME: Final = (
    "live_remote.pre_config_reconciliation_v6.26b1d8bf.snapshot.json"
)
PREDECESSOR_CATALOG_SNAPSHOT_FILENAME: Final = (
    "catalog.pre_config_reconciliation_v6.0b9c9d93.snapshot.json"
)
RECEIPT_FILENAME: Final = "live_config_locator_reconciliation_v1_20260825.json"
CURRENT_CONFIG_ARTIFACT_ID: Final = "repository-live-config-current"
CURRENT_POINTER_ARTIFACT_ID: Final = "repository-live-remote-current"
V6_ACTIVATION_ARTIFACT_ID: Final = (
    "repository-live-replacement-activation-aws-tokyo-buy-e3-no-shadow-v6-20260824-v1"
)
BACKTEST_ARCHIVE_ARTIFACT_ID: Final = "repository-live-config-backtest-v12-800f4c025663-archive"
RELEASE_V3_CONFIG_ARTIFACT_ID: Final = (
    "repository-live-config-owner-buy-e3-no-shadow-release-v3-3d8463c47c1c"
)
POINTER_SNAPSHOT_ARTIFACT_ID: Final = (
    "repository-live-remote-pre-config-reconciliation-v6-26b1d8bf-snapshot"
)
CATALOG_SNAPSHOT_ARTIFACT_ID: Final = (
    "repository-private-catalog-pre-config-reconciliation-v6-0b9c9d93-snapshot"
)
RECONCILIATION_ARTIFACT_ID: Final = "repository-live-config-locator-reconciliation-v1-20260825"

OLD_CONFIG_SHA256: Final = "800f4c025663ce6b54cfcf16d02ce510ccaf52545332ca4c19b1fbdf37f0cf85"
OLD_CONFIG_SIZE: Final = 26_443
ACTIVE_CONFIG_SHA256: Final = "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
ACTIVE_CONFIG_SIZE: Final = 27_443
PREDECESSOR_POINTER_SHA256: Final = (
    "26b1d8bfe44e03fd511023e8cea2563861b369f1f04c1df8d8d39a16e4ac0906"
)
PREDECESSOR_POINTER_SIZE: Final = 44_813
PREDECESSOR_CATALOG_SHA256: Final = (
    "0b9c9d930681ad677afa3cd8de03d7582afcb4456f2013841df7fa40c777e4a7"
)
PREDECESSOR_CATALOG_SIZE: Final = 1_389_789
V6_ACTIVATION: Final = {
    "schema_version": "narrowgate.live_replacement_activation_receipt.v3",
    "status": "completed_active_release_v3_no_shadow_evidence_closed",
    "file_sha256": "44b6482c0448dfdf773950e94b96fb9b379f4665c4ec3c41ca1241c7fc40aaa9",
    "canonical_field": "canonical_replacement_activation_receipt_sha256",
    "canonical_sha256": "6e4269fd821fa943c9888661fa99c28ec4d59eb5457e4103ad930da40f488f7e",
    "size_bytes": 545_127,
    "mode": "0600",
}
RELEASE_V3: Final = {
    "file_sha256": "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49",
    "canonical_sha256": "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f",
}
RUNTIME_V3: Final = {
    "commit": "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de",
    "tree": "0343bd5586b337385cf2aa0d7a643f5c32b0da77",
    "annotated_tag_object": "3878ea05252ef8f274b6f74ee7a984431c53b892",
}

NO_NEW_AUTHORITY: Final = {
    "research": False,
    "action": False,
    "live": False,
    "backtest_economic": False,
}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "live_process_mutated": False,
    "remote_contacted": False,
    "immutable_v6_activation_receipt_rewritten": False,
}

TRACKED_SUCCESSOR_FILES: Final = (
    "docs/live_host_and_historical_data_access_20260811.md",
    "models/backtest_config.py",
    "research/families/f10_live_replay_attribution/README.md",
    "research/families/f10_live_replay_attribution/docs/operational_baseline_current.json",
    "research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260825_v13.json",
    "research/public_machine_document_projections.json",
    "research/registry.json",
    "scripts/audit_private_evidence.py",
    "scripts/f05_reconcile_live_config_locator_v1.py",
    "scripts/live_remote_pointer.py",
)

MAX_FILE_BYTES: Final = 64 << 20
MANIFEST_MAX_CLOCK_SKEW_SECONDS: Final = 5
STAGING_TOKEN_HEX_LENGTH: Final = 32
STAGING_SUFFIX: Final = "-uncommitted-config-reconciliation-v1"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_OID_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_LOCKED_PRIVATE_CONTEXT: contextvars.ContextVar[tuple[Path, tuple[int, int]] | None] = (
    contextvars.ContextVar("locked_private_context", default=None)
)


class ConfigLocatorReconciliationError(RuntimeError):
    """Raised when any source, plan, or publication state fails closed."""


class CommittedPostAuditError(ConfigLocatorReconciliationError):
    """Report a committed exact transaction whose post-audit did not pass."""

    def __init__(self, result: Mapping[str, Any]) -> None:
        self.result = deepcopy(dict(result))
        super().__init__(str(self.result.get("status", "committed post-audit failure")))


def _private_evidence_root() -> Path:
    raw = os.environ.get(PRIVATE_EVIDENCE_ROOT_ENV, "").strip()
    if not raw:
        raise ConfigLocatorReconciliationError(
            f"{PRIVATE_EVIDENCE_ROOT_ENV} is required for private evidence resolution"
        )
    root = Path(os.path.abspath(os.fspath(Path(raw).expanduser())))
    if not root.is_absolute():
        raise ConfigLocatorReconciliationError("private evidence root must be absolute")
    filesystem_root = Path("/")
    ephemeral_roots = {
        filesystem_root / "tmp",
        filesystem_root / "private" / "tmp",
        filesystem_root / "var" / "tmp",
        filesystem_root / "private" / "var" / "tmp",
        _absolute(Path(tempfile.gettempdir())),
        Path(tempfile.gettempdir()).resolve(strict=True),
    }
    if any(root == candidate or root.is_relative_to(candidate) for candidate in ephemeral_roots):
        raise ConfigLocatorReconciliationError(
            "private evidence root must be durable and non-ephemeral"
        )
    _opened_root, descriptor = _open_directory_nofollow(root)
    try:
        metadata = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if metadata.st_uid != os.getuid() or not stat.S_ISDIR(metadata.st_mode):
        raise ConfigLocatorReconciliationError("private evidence root identity is unsafe")
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise ConfigLocatorReconciliationError(
            "private evidence trust root must not be group/other writable"
        )
    return root


def _formal_manifest_path() -> Path:
    return _private_evidence_root() / FORMAL_MANIFEST_RELATIVE


def _active_config_source_path() -> Path:
    return _private_evidence_root() / ACTIVE_CONFIG_SOURCE_RELATIVE


def _require_owned_private_directory(path: Path, label: str) -> None:
    _opened, descriptor = _open_directory_nofollow(path)
    try:
        metadata = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        metadata.st_uid != os.getuid()
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise ConfigLocatorReconciliationError(f"{label} must be owner-only 0700")


def _validate_private_evidence_layout(root: Path, *, allow_manifest_leaf_missing: bool) -> None:
    unit = root / E3_V6_EVIDENCE_RELATIVE
    authority_sources = (root / ACTIVE_CONFIG_SOURCE_RELATIVE).parent
    manifest_leaf = (root / FORMAL_MANIFEST_RELATIVE).parent
    _require_owned_private_directory(unit, "private evidence unit")
    _require_owned_private_directory(authority_sources, "authority source directory")
    if _secure_exists(manifest_leaf):
        _require_owned_private_directory(manifest_leaf, "formal manifest directory")
    elif not allow_manifest_leaf_missing:
        raise ConfigLocatorReconciliationError("formal manifest directory is missing")


def _private_evidence_trust_contract(root: Path) -> dict[str, Any]:
    return {
        "environment_variable": PRIVATE_EVIDENCE_ROOT_ENV,
        "resolved_reports_root": str(root),
        "resolved_reports_root_owner_required": True,
        "resolved_reports_root_group_or_other_write_forbidden": True,
        "all_lexical_ancestors_symlink_forbidden": True,
        "mount_ancestor_mode_is_not_an_authority_claim": True,
        "e3_evidence_unit_mode": "0700",
        "authority_source_directory_mode": "0700",
        "formal_manifest_directory_mode": "0700",
        "formal_manifest_file_mode": "0600",
    }


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return _sha(encoded)


def _document_sha(payload: Mapping[str, Any], field: str) -> str:
    projected = dict(payload)
    projected.pop(field, None)
    return _canonical(projected)


def _render(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()


def _require_sha(value: Any, label: str) -> str:
    text = str(value)
    if _SHA256_RE.fullmatch(text) is None:
        raise ConfigLocatorReconciliationError(f"{label} is not a lowercase SHA256")
    return text


def _require_oid(value: Any, label: str) -> str:
    text = str(value)
    if _GIT_OID_RE.fullmatch(text) is None:
        raise ConfigLocatorReconciliationError(f"{label} is not a Git object id")
    return text


def _json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ConfigLocatorReconciliationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path.expanduser())))


def _open_directory_nofollow(path: Path) -> tuple[Path, int]:
    target = _absolute(path)
    if not target.is_absolute() or not hasattr(os, "O_NOFOLLOW"):
        raise ConfigLocatorReconciliationError("secure directory walk is unsupported")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(target.anchor, flags)
        for component in target.parts[1:]:
            next_descriptor = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise ConfigLocatorReconciliationError(
            f"directory is unavailable or contains a symlink: {target}"
        ) from exc
    if descriptor is None:
        raise ConfigLocatorReconciliationError(f"directory is unavailable: {target}")
    locked = _LOCKED_PRIVATE_CONTEXT.get()
    if locked is not None and target == locked[0]:
        observed = os.fstat(descriptor)
        if (observed.st_dev, observed.st_ino) != locked[1]:
            os.close(descriptor)
            raise ConfigLocatorReconciliationError("locked private directory identity drifted")
    return target, descriptor


def _open_parent_nofollow(path: Path) -> tuple[Path, int]:
    target = _absolute(path)
    if target.name in {"", ".", ".."}:
        raise ConfigLocatorReconciliationError("secure file path is malformed")
    _parent, descriptor = _open_directory_nofollow(target.parent)
    return target, descriptor


def _revalidate_parent(target: Path, descriptor: int) -> None:
    _parent, again = _open_directory_nofollow(target.parent)
    try:
        before = os.fstat(descriptor)
        after = os.fstat(again)
    finally:
        os.close(again)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise ConfigLocatorReconciliationError(f"file parent changed: {target}")


def _read_regular(
    path: Path,
    *,
    mode: int = 0o600,
    allowed_nlinks: frozenset[int] = frozenset({1}),
) -> tuple[bytes, os.stat_result]:
    target, directory_fd = _open_parent_nofollow(path)
    descriptor: int | None = None
    try:
        before = os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        os.close(directory_fd)
        raise ConfigLocatorReconciliationError(f"required file is unavailable: {target}") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or stat.S_IMODE(before.st_mode) != mode
        or before.st_nlink not in allowed_nlinks
        or before.st_size < 0
        or before.st_size > MAX_FILE_BYTES
    ):
        os.close(directory_fd)
        raise ConfigLocatorReconciliationError(f"unsafe file identity: {target}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(target.name, flags, dir_fd=directory_fd)
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ConfigLocatorReconciliationError(f"file changed while opening: {target}")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_FILE_BYTES:
                raise ConfigLocatorReconciliationError(f"file is too large: {target}")
            chunks.append(chunk)
        after_fd = os.fstat(descriptor)
        after_path = os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
        _revalidate_parent(target, directory_fd)
    except OSError as exc:
        raise ConfigLocatorReconciliationError(f"required file is unavailable: {target}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)

    def identity(value: os.stat_result) -> tuple[int, ...]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_uid,
            value.st_gid,
            value.st_nlink,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    if identity(before) != identity(after_fd) or identity(before) != identity(after_path):
        raise ConfigLocatorReconciliationError(f"file changed while reading: {target}")
    return b"".join(chunks), before


def _load_json(
    path: Path,
    *,
    allowed_nlinks: frozenset[int] = frozenset({1}),
) -> tuple[dict[str, Any], bytes, os.stat_result]:
    raw, metadata = _read_regular(path, allowed_nlinks=allowed_nlinks)
    try:
        payload = json.loads(raw, object_pairs_hook=_json_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfigLocatorReconciliationError(f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ConfigLocatorReconciliationError(f"JSON root is not an object: {path}")
    return payload, raw, metadata


def _binding(path: Path, raw: bytes) -> dict[str, Any]:
    return {
        "path": str(_absolute(path)),
        "sha256": _sha(raw),
        "bytes": len(raw),
        "mode": "0600",
    }


def _canonical_binding(
    path: Path,
    payload: Mapping[str, Any],
    raw: bytes,
    canonical_field: str,
) -> dict[str, Any]:
    canonical = _require_sha(payload.get(canonical_field), f"{path.name} canonical SHA256")
    if canonical != _document_sha(payload, canonical_field):
        raise ConfigLocatorReconciliationError(f"canonical recomputation drifted: {path}")
    return {
        "path": str(_absolute(path)),
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": _sha(raw),
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
        "size_bytes": len(raw),
        "mode": "0600",
    }


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    )
    if result.returncode != 0:
        raise ConfigLocatorReconciliationError(
            f"publisher git check failed ({' '.join(args)}): {result.stderr.strip()}"
        )
    return result.stdout.strip()


def _git_bytes(root: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(root), *args],
        check=False,
        capture_output=True,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    )
    if result.returncode != 0:
        raise ConfigLocatorReconciliationError(
            f"publisher git byte check failed ({' '.join(args)}): "
            f"{result.stderr.decode('utf-8', errors='replace').strip()}"
        )
    return result.stdout


def _observe_publisher(root: Path) -> dict[str, Any]:
    root = _absolute(root)
    _opened_root, root_fd = _open_directory_nofollow(root)
    root_identity = os.fstat(root_fd)
    os.close(root_fd)
    tag_object = _git(root, "rev-parse", f"refs/tags/{PUBLISHER_TAG}")
    if _git(root, "cat-file", "-t", tag_object) != "tag":
        raise ConfigLocatorReconciliationError("publisher tag is not annotated")
    observed = {
        "module_route": PUBLISHER_MODULE_ROUTE,
        "annotated_tag": PUBLISHER_TAG,
        "annotated_tag_object": tag_object,
        "commit": _git(root, "rev-parse", f"refs/tags/{PUBLISHER_TAG}^{{commit}}"),
        "tree": _git(root, "rev-parse", f"refs/tags/{PUBLISHER_TAG}^{{tree}}"),
    }
    if (
        _git(root, "rev-parse", "HEAD") != observed["commit"]
        or _git(root, "rev-parse", "HEAD^{tree}") != observed["tree"]
        or _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise ConfigLocatorReconciliationError("publisher checkout is not exact and clean")
    expected_script = root / "scripts" / "f05_reconcile_live_config_locator_v1.py"
    if _absolute(expected_script) != _absolute(Path(__file__)):
        raise ConfigLocatorReconciliationError("publisher module origin drifted")
    metadata_origin = getattr(metadata_v6, "__file__", None)
    audit_origin = getattr(audit_private_evidence, "__file__", None)
    if (
        not isinstance(metadata_origin, str)
        or _absolute(Path(metadata_origin))
        != _absolute(root / "scripts/f05_closeout_operational_metadata_v6.py")
        or not isinstance(audit_origin, str)
        or _absolute(Path(audit_origin)) != _absolute(root / "scripts/audit_private_evidence.py")
    ):
        raise ConfigLocatorReconciliationError("publisher dependency origin drifted")
    for name, module in tuple(sys.modules.items()):
        if not (name.startswith("scripts.") or name.startswith("research.")):
            continue
        origin = getattr(module, "__file__", None)
        if not isinstance(origin, str):
            continue
        try:
            _absolute(Path(origin)).relative_to(root)
        except ValueError as exc:
            raise ConfigLocatorReconciliationError(
                f"publisher transitive module origin drifted: {name}"
            ) from exc
    script_raw, _metadata = _read_regular(expected_script, mode=0o644)
    _opened_after, after_fd = _open_directory_nofollow(root)
    try:
        after_identity = os.fstat(after_fd)
    finally:
        os.close(after_fd)
    if (root_identity.st_dev, root_identity.st_ino) != (
        after_identity.st_dev,
        after_identity.st_ino,
    ):
        raise ConfigLocatorReconciliationError("publisher root changed during validation")
    observed["script_sha256"] = _sha(script_raw)
    return observed


def _validate_publisher(root: Path, expected: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "module_route",
        "annotated_tag",
        "annotated_tag_object",
        "commit",
        "tree",
        "script_sha256",
    }
    if set(expected) != required:
        raise ConfigLocatorReconciliationError("publisher source fields drifted")
    if (
        expected.get("module_route") != PUBLISHER_MODULE_ROUTE
        or expected.get("annotated_tag") != PUBLISHER_TAG
    ):
        raise ConfigLocatorReconciliationError("publisher route/tag drifted")
    for field in ("annotated_tag_object", "commit", "tree"):
        _require_oid(expected.get(field), f"publisher {field}")
    _require_sha(expected.get("script_sha256"), "publisher script SHA256")
    observed = _observe_publisher(root)
    if observed != dict(expected):
        raise ConfigLocatorReconciliationError("publisher source identity drifted")
    return observed


def _validate_metadata_tracked_source(
    root: Path,
    publisher: Mapping[str, Any],
) -> None:
    root = _absolute(root)
    _opened, before_fd = _open_directory_nofollow(root)
    before = os.fstat(before_fd)
    os.close(before_fd)
    if (
        _git(root, "rev-parse", "HEAD") != publisher.get("commit")
        or _git(root, "rev-parse", "HEAD^{tree}") != publisher.get("tree")
        or _git(root, "status", "--porcelain=v1", "--untracked-files=no")
    ):
        raise ConfigLocatorReconciliationError(
            "metadata tracked checkout is not the exact publisher integration source"
        )
    _opened_after, after_fd = _open_directory_nofollow(root)
    after = os.fstat(after_fd)
    os.close(after_fd)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise ConfigLocatorReconciliationError("metadata root changed during validation")


def _tracked_successor_files(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in TRACKED_SUCCESSOR_FILES:
        raw, _metadata = _read_regular(root / relative, mode=0o644)
        result[relative] = _sha(raw)
    return result


def _tracked_successor_tree_files(root: Path, commit: str) -> dict[str, str]:
    _require_oid(commit, "publisher commit")
    return {
        relative: _sha(_git_bytes(root, "cat-file", "blob", f"{commit}:{relative}"))
        for relative in TRACKED_SUCCESSOR_FILES
    }


def _validate_tracked_source_pair(
    *,
    publisher_root: Path,
    metadata_root: Path,
    publisher: Mapping[str, Any],
    expected_tracked: Mapping[str, Any],
) -> dict[str, str]:
    _validate_publisher(publisher_root, publisher)
    tree_tracked = _tracked_successor_tree_files(publisher_root, str(publisher.get("commit", "")))
    if tree_tracked != dict(expected_tracked):
        raise ConfigLocatorReconciliationError(
            "tracked successor identities do not match the annotated publisher tree"
        )
    if _tracked_successor_files(publisher_root) != tree_tracked:
        raise ConfigLocatorReconciliationError("publisher tracked bytes drifted")
    _validate_publisher(publisher_root, publisher)
    _validate_metadata_tracked_source(metadata_root, publisher)
    if _tracked_successor_files(metadata_root) != tree_tracked:
        raise ConfigLocatorReconciliationError(
            "metadata tracked bytes do not match the annotated publisher tree"
        )
    _validate_metadata_tracked_source(metadata_root, publisher)
    return tree_tracked


def _now_utc_ns() -> int:
    observed = datetime.now(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = observed - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + observed.microsecond * 1_000


def _nanosecond_utc(value: int) -> str:
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    whole = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{whole}.{nanoseconds:09d}Z"


def _timestamp_ns(value: Any, label: str) -> int:
    if not isinstance(value, str):
        raise ConfigLocatorReconciliationError(f"{label} must be a UTC timestamp")
    matched = re.fullmatch(
        r"(?P<whole>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(?P<fraction>\d{1,9}))?Z",
        value,
    )
    if matched is None:
        raise ConfigLocatorReconciliationError(f"{label} must be a UTC Z timestamp")
    try:
        parsed = datetime.strptime(matched.group("whole"), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ConfigLocatorReconciliationError(f"{label} is invalid") from exc
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = parsed - epoch
    fraction = (matched.group("fraction") or "").ljust(9, "0")
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + int(fraction or "0")


def _pending(path: Path, kind: str) -> Path:
    return path.with_name(f".{path.name}.{kind}-pending-config-reconciliation-v1")


def _staging_path(path: Path, kind: str, token: str) -> Path:
    if re.fullmatch(r"[a-z][a-z0-9_-]*", kind) is None:
        raise ConfigLocatorReconciliationError("staging kind is malformed")
    if re.fullmatch(rf"[0-9a-f]{{{STAGING_TOKEN_HEX_LENGTH}}}", token) is None:
        raise ConfigLocatorReconciliationError("staging token is malformed")
    return path.with_name(f".{path.name}.{kind}-staging-{token}{STAGING_SUFFIX}")


def _is_staging_path(path: Path, final_path: Path, kind: str) -> bool:
    prefix = f".{final_path.name}.{kind}-staging-"
    name = path.name
    if not name.startswith(prefix) or not name.endswith(STAGING_SUFFIX):
        return False
    token = name[len(prefix) : -len(STAGING_SUFFIX)]
    return re.fullmatch(rf"[0-9a-f]{{{STAGING_TOKEN_HEX_LENGTH}}}", token) is not None


def _staging_paths(path: Path, kind: str) -> tuple[Path, ...]:
    target, directory_fd = _open_parent_nofollow(path)
    try:
        names = os.listdir(directory_fd)
        _revalidate_parent(target, directory_fd)
    except OSError as exc:
        raise ConfigLocatorReconciliationError(
            f"staging directory is unavailable: {target.parent}"
        ) from exc
    finally:
        os.close(directory_fd)
    return tuple(
        target.parent / name
        for name in sorted(names)
        if _is_staging_path(target.parent / name, target, kind)
    )


def _manifest_staging_path(path: Path, token: str) -> Path:
    return _staging_path(path, "manifest", token)


def _is_manifest_staging_path(path: Path, formal_path: Path) -> bool:
    return _is_staging_path(path, formal_path, "manifest")


def _manifest_staging_paths(path: Path) -> tuple[Path, ...]:
    return _staging_paths(path, "manifest")


def _secure_exists(path: Path) -> bool:
    target, directory_fd = _open_parent_nofollow(path)
    try:
        try:
            os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            exists = False
        else:
            exists = True
        _revalidate_parent(target, directory_fd)
        return exists
    finally:
        os.close(directory_fd)


def _fsync_dir(path: Path) -> None:
    _target, descriptor = _open_directory_nofollow(path)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new(path: Path, data: bytes) -> None:
    target, directory_fd = _open_parent_nofollow(path)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            target.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_fd,
        )
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ConfigLocatorReconciliationError(f"short write: {path}")
            view = view[written:]
        os.fsync(descriptor)
        _revalidate_parent(target, directory_fd)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)


def _secure_unlink(path: Path) -> None:
    target, directory_fd = _open_parent_nofollow(path)
    try:
        os.unlink(target.name, dir_fd=directory_fd)
        os.fsync(directory_fd)
        _revalidate_parent(target, directory_fd)
    finally:
        os.close(directory_fd)


def _secure_link(source: Path, destination: Path) -> None:
    source_target, source_fd = _open_parent_nofollow(source)
    destination_target, destination_fd = _open_parent_nofollow(destination)
    try:
        os.link(
            source_target.name,
            destination_target.name,
            src_dir_fd=source_fd,
            dst_dir_fd=destination_fd,
            follow_symlinks=False,
        )
        os.fsync(destination_fd)
        _revalidate_parent(source_target, source_fd)
        _revalidate_parent(destination_target, destination_fd)
    finally:
        os.close(source_fd)
        os.close(destination_fd)


def _secure_replace(source: Path, destination: Path) -> None:
    source_target, source_fd = _open_parent_nofollow(source)
    destination_target, destination_fd = _open_parent_nofollow(destination)
    try:
        os.replace(
            source_target.name,
            destination_target.name,
            src_dir_fd=source_fd,
            dst_dir_fd=destination_fd,
        )
        os.fsync(destination_fd)
        _revalidate_parent(source_target, source_fd)
        _revalidate_parent(destination_target, destination_fd)
    finally:
        os.close(source_fd)
        os.close(destination_fd)


def _read_exact_any_link(path: Path, data: bytes, links: frozenset[int]) -> os.stat_result:
    observed, metadata = _read_regular(path, allowed_nlinks=links)
    if observed != data:
        raise ConfigLocatorReconciliationError(f"pending/published bytes drifted: {path}")
    return metadata


def _require_transaction_lock() -> None:
    locked = _LOCKED_PRIVATE_CONTEXT.get()
    if locked is None:
        raise ConfigLocatorReconciliationError(
            "transaction lock is required for uncommitted staging recovery"
        )
    _locked_path, descriptor = _open_directory_nofollow(locked[0])
    os.close(descriptor)


def _is_strict_prefix(observed: bytes, expected: bytes) -> bool:
    return len(observed) < len(expected) and expected.startswith(observed)


def _recovery_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _secure_unlink_bound(path: Path, expected: os.stat_result) -> None:
    target, directory_fd = _open_parent_nofollow(path)
    try:
        observed = os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
        if _recovery_identity(observed) != _recovery_identity(expected):
            raise ConfigLocatorReconciliationError(
                f"file changed before identity-bound unlink: {target}"
            )
        os.unlink(target.name, dir_fd=directory_fd)
        os.fsync(directory_fd)
        _revalidate_parent(target, directory_fd)
    except OSError as exc:
        raise ConfigLocatorReconciliationError(f"identity-bound unlink failed: {target}") from exc
    finally:
        os.close(directory_fd)


def _expected_current_metadata(
    path: Path,
    expected_current: bytes | None,
) -> os.stat_result | None:
    if expected_current is None:
        if _secure_exists(path):
            raise ConfigLocatorReconciliationError(
                "published final forbids uncommitted staging recovery"
            )
        return None
    return _read_exact_any_link(path, expected_current, frozenset({1}))


def _validate_staging_state(
    path: Path,
    data: bytes | None,
    *,
    pending_kind: str,
    staging_kind: str,
) -> str:
    staging = _staging_paths(path, staging_kind)
    if not staging:
        return "absent"
    _require_transaction_lock()
    pending = _pending(path, pending_kind)
    if _secure_exists(pending):
        if data is None or len(staging) != 1:
            raise ConfigLocatorReconciliationError("staging transfer is ambiguous")
        pending_metadata = _read_exact_any_link(pending, data, frozenset({2}))
        staging_metadata = _read_exact_any_link(staging[0], data, frozenset({2}))
        if (pending_metadata.st_dev, pending_metadata.st_ino) != (
            staging_metadata.st_dev,
            staging_metadata.st_ino,
        ):
            raise ConfigLocatorReconciliationError("staging transfer inode drifted")
        return "pending_staging_transfer_nlink2"
    for candidate in staging:
        observed, _metadata = _read_regular(candidate, allowed_nlinks=frozenset({1}))
        if data is not None and observed != data and not _is_strict_prefix(observed, data):
            raise ConfigLocatorReconciliationError(
                f"uncommitted staging bytes drifted: {candidate}"
            )
    return "staging_recoverable_uncommitted"


def _cleanup_uncommitted_staging(
    path: Path,
    data: bytes | None,
    *,
    pending_kind: str,
    staging_kind: str,
    expected_current: bytes | None,
) -> None:
    _require_transaction_lock()
    final_metadata = _expected_current_metadata(path, expected_current)
    pending = _pending(path, pending_kind)
    if _secure_exists(pending):
        raise ConfigLocatorReconciliationError(
            "deterministic pending forbids uncommitted staging cleanup"
        )
    if (
        _validate_staging_state(
            path,
            data,
            pending_kind=pending_kind,
            staging_kind=staging_kind,
        )
        != "staging_recoverable_uncommitted"
    ):
        raise ConfigLocatorReconciliationError("uncommitted staging is unavailable")
    bound: list[tuple[Path, os.stat_result]] = []
    for candidate in _staging_paths(path, staging_kind):
        observed, metadata = _read_regular(candidate, allowed_nlinks=frozenset({1}))
        if data is not None and observed != data and not _is_strict_prefix(observed, data):
            raise ConfigLocatorReconciliationError(
                f"uncommitted staging bytes drifted: {candidate}"
            )
        bound.append((candidate, metadata))
    if final_metadata is None:
        _expected_current_metadata(path, None)
    else:
        current = _expected_current_metadata(path, expected_current)
        if current is None or _recovery_identity(current) != _recovery_identity(final_metadata):
            raise ConfigLocatorReconciliationError(
                "mutable predecessor changed before staging cleanup"
            )
    if _secure_exists(pending):
        raise ConfigLocatorReconciliationError(
            "deterministic pending appeared before staging cleanup"
        )
    for candidate, metadata in bound:
        _secure_unlink_bound(candidate, metadata)
    if _staging_paths(path, staging_kind) or _secure_exists(pending):
        raise ConfigLocatorReconciliationError("publication state changed during staging cleanup")
    if final_metadata is None:
        _expected_current_metadata(path, None)
    else:
        current = _expected_current_metadata(path, expected_current)
        if current is None or _recovery_identity(current) != _recovery_identity(final_metadata):
            raise ConfigLocatorReconciliationError(
                "mutable predecessor changed during staging cleanup"
            )


def _recover_staging_transfer(
    path: Path,
    data: bytes,
    *,
    pending_kind: str,
    staging_kind: str,
    expected_current: bytes | None,
) -> None:
    _require_transaction_lock()
    final_metadata = _expected_current_metadata(path, expected_current)
    if (
        _validate_staging_state(
            path,
            data,
            pending_kind=pending_kind,
            staging_kind=staging_kind,
        )
        != "pending_staging_transfer_nlink2"
    ):
        raise ConfigLocatorReconciliationError("staging transfer is unavailable")
    staging = _staging_paths(path, staging_kind)
    staging_metadata = _read_exact_any_link(staging[0], data, frozenset({2}))
    _secure_unlink_bound(staging[0], staging_metadata)
    _read_exact_any_link(_pending(path, pending_kind), data, frozenset({1}))
    if final_metadata is None:
        _expected_current_metadata(path, None)
    else:
        current = _expected_current_metadata(path, expected_current)
        if current is None or _recovery_identity(current) != _recovery_identity(final_metadata):
            raise ConfigLocatorReconciliationError(
                "mutable predecessor changed during staging transfer recovery"
            )


def _stage_deterministic_pending(
    path: Path,
    data: bytes,
    *,
    pending_kind: str,
    staging_kind: str,
    expected_current: bytes | None,
) -> None:
    _require_transaction_lock()
    final_metadata = _expected_current_metadata(path, expected_current)
    pending = _pending(path, pending_kind)
    if _secure_exists(pending) or _staging_paths(path, staging_kind):
        raise ConfigLocatorReconciliationError("publication state changed before staged write")
    staging: Path | None = None
    for _attempt in range(16):
        candidate = _staging_path(path, staging_kind, os.urandom(16).hex())
        try:
            _write_new(candidate, data)
        except FileExistsError:
            continue
        staging = candidate
        break
    if staging is None:
        raise ConfigLocatorReconciliationError("unique staging path is unavailable")
    _fsync_dir(path.parent)
    _read_exact_any_link(staging, data, frozenset({1}))
    if final_metadata is None:
        _expected_current_metadata(path, None)
    else:
        current = _expected_current_metadata(path, expected_current)
        if current is None or _recovery_identity(current) != _recovery_identity(final_metadata):
            raise ConfigLocatorReconciliationError(
                "mutable predecessor changed before staging transfer"
            )
    if _secure_exists(pending):
        raise ConfigLocatorReconciliationError(
            "deterministic pending appeared before staging transfer"
        )
    _secure_link(staging, pending)
    staging_metadata = _read_exact_any_link(staging, data, frozenset({2}))
    pending_metadata = _read_exact_any_link(pending, data, frozenset({2}))
    if (staging_metadata.st_dev, staging_metadata.st_ino) != (
        pending_metadata.st_dev,
        pending_metadata.st_ino,
    ):
        raise ConfigLocatorReconciliationError("staging hardlink identity drifted")
    _secure_unlink_bound(staging, staging_metadata)
    _read_exact_any_link(pending, data, frozenset({1}))


def _discard_orphan_manifest_staging(path: Path, staging: Sequence[Path]) -> None:
    if tuple(staging) != _manifest_staging_paths(path):
        raise ConfigLocatorReconciliationError("manifest staging paths drifted")
    _cleanup_uncommitted_staging(
        path,
        None,
        pending_kind="create",
        staging_kind="manifest",
        expected_current=None,
    )


def _recover_manifest_staging_transfer(
    path: Path,
    pending: Path,
    staging: Sequence[Path],
) -> None:
    _require_transaction_lock()
    if _secure_exists(path) or len(staging) != 1:
        raise ConfigLocatorReconciliationError("manifest staging transfer is ambiguous")
    candidate = staging[0]
    if not _is_manifest_staging_path(candidate, path):
        raise ConfigLocatorReconciliationError("manifest staging path drifted")
    pending_payload, pending_raw, pending_metadata = _manifest_candidate(
        pending,
        formal_path=path,
        allowed_nlinks=frozenset({2}),
    )
    staging_payload, staging_raw, staging_metadata = _manifest_candidate(
        candidate,
        formal_path=path,
        allowed_nlinks=frozenset({2}),
    )
    if (
        staging_payload != pending_payload
        or staging_raw != pending_raw
        or (staging_metadata.st_dev, staging_metadata.st_ino)
        != (pending_metadata.st_dev, pending_metadata.st_ino)
    ):
        raise ConfigLocatorReconciliationError("manifest staging transfer identity drifted")
    _secure_unlink_bound(candidate, staging_metadata)
    _read_exact_any_link(pending, pending_raw, frozenset({1}))


def _stage_manifest_pending(path: Path, data: bytes) -> None:
    _stage_deterministic_pending(
        path,
        data,
        pending_kind="create",
        staging_kind="manifest",
        expected_current=None,
    )


def _publish_create_only(path: Path, data: bytes) -> None:
    pending = _pending(path, "create")
    staging_state = _validate_staging_state(
        path,
        data,
        pending_kind="create",
        staging_kind="create",
    )
    if _secure_exists(path):
        if staging_state != "absent":
            raise ConfigLocatorReconciliationError(
                "published create-only final has ambiguous staging"
            )
        metadata = _read_exact_any_link(path, data, frozenset({1, 2}))
        if metadata.st_nlink == 2:
            if not _secure_exists(pending):
                raise ConfigLocatorReconciliationError("nlink=2 final lacks recovery link")
            pending_meta = _read_exact_any_link(pending, data, frozenset({2}))
            if (metadata.st_dev, metadata.st_ino) != (
                pending_meta.st_dev,
                pending_meta.st_ino,
            ):
                raise ConfigLocatorReconciliationError("recovery hardlink inode drifted")
            _secure_unlink(pending)
            _fsync_dir(path.parent)
            _read_exact_any_link(path, data, frozenset({1}))
        elif _secure_exists(pending):
            raise ConfigLocatorReconciliationError("orphan create-only pending path")
        return
    if _secure_exists(pending):
        if staging_state != "absent":
            raise ConfigLocatorReconciliationError(
                "create-only staging transfer requires a fresh transaction restart"
            )
        _read_exact_any_link(pending, data, frozenset({1}))
    else:
        if staging_state == "staging_recoverable_uncommitted":
            raise ConfigLocatorReconciliationError(
                "uncommitted create-only staging requires a fresh transaction restart"
            )
        elif staging_state != "absent":
            raise ConfigLocatorReconciliationError("create-only staging state drifted")
        _stage_deterministic_pending(
            path,
            data,
            pending_kind="create",
            staging_kind="create",
            expected_current=None,
        )
    _secure_link(pending, path)
    _secure_unlink(pending)
    _read_exact_any_link(path, data, frozenset({1}))


def _atomic_replace(
    path: Path,
    data: bytes,
    *,
    kind: str,
    expected_current: bytes,
) -> None:
    pending = _pending(path, kind)
    _read_exact_any_link(path, expected_current, frozenset({1}))
    staging_state = _validate_staging_state(
        path,
        data,
        pending_kind=kind,
        staging_kind=kind,
    )
    if _secure_exists(pending):
        if staging_state != "absent":
            raise ConfigLocatorReconciliationError(
                f"{kind} staging transfer requires a fresh transaction restart"
            )
        _read_exact_any_link(pending, data, frozenset({1}))
    else:
        if staging_state == "staging_recoverable_uncommitted":
            raise ConfigLocatorReconciliationError(
                f"uncommitted {kind} staging requires a fresh transaction restart"
            )
        elif staging_state != "absent":
            raise ConfigLocatorReconciliationError(f"{kind} staging state drifted")
        _stage_deterministic_pending(
            path,
            data,
            pending_kind=kind,
            staging_kind=kind,
            expected_current=expected_current,
        )
    _read_exact_any_link(path, expected_current, frozenset({1}))
    _secure_replace(pending, path)
    _read_exact_any_link(path, data, frozenset({1}))


def _validate_create_state(path: Path, data: bytes) -> str:
    pending = _pending(path, "create")
    exists = _secure_exists(path)
    pending_exists = _secure_exists(pending)
    staging_state = _validate_staging_state(
        path,
        data,
        pending_kind="create",
        staging_kind="create",
    )
    if not exists and not pending_exists:
        return "missing" if staging_state == "absent" else staging_state
    if not exists:
        if staging_state == "pending_staging_transfer_nlink2":
            return staging_state
        if staging_state != "absent":
            raise ConfigLocatorReconciliationError("create-only pending has ambiguous staging")
        _read_exact_any_link(pending, data, frozenset({1}))
        return "pending_create_only"
    if staging_state != "absent":
        raise ConfigLocatorReconciliationError("published create-only final has ambiguous staging")
    metadata = _read_exact_any_link(path, data, frozenset({1, 2}))
    if metadata.st_nlink == 2:
        if not pending_exists:
            raise ConfigLocatorReconciliationError("nlink=2 final lacks recovery pending")
        pending_meta = _read_exact_any_link(pending, data, frozenset({2}))
        if (metadata.st_dev, metadata.st_ino) != (pending_meta.st_dev, pending_meta.st_ino):
            raise ConfigLocatorReconciliationError("recovery pending inode drifted")
        return "published_recoverable_nlink2"
    if pending_exists:
        raise ConfigLocatorReconciliationError("orphan create-only pending is ambiguous")
    return "published_nlink1"


def _validate_replace_pending(path: Path, data: bytes, kind: str, *, target_new: bool) -> str:
    pending = _pending(path, kind)
    pending_exists = _secure_exists(pending)
    staging_state = _validate_staging_state(
        path,
        data,
        pending_kind=kind,
        staging_kind=kind,
    )
    if pending_exists or staging_state != "absent":
        if target_new:
            raise ConfigLocatorReconciliationError(
                f"orphan {kind} pending/staging after publication"
            )
        if staging_state == "pending_staging_transfer_nlink2":
            return staging_state
        if pending_exists:
            if staging_state != "absent":
                raise ConfigLocatorReconciliationError(f"{kind} pending has ambiguous staging")
            _read_exact_any_link(pending, data, frozenset({1}))
            return "pending_exact"
        if staging_state != "staging_recoverable_uncommitted":
            raise ConfigLocatorReconciliationError(f"{kind} staging state drifted")
        return staging_state
    return "absent"


def _validate_transaction_prefix(
    *,
    immutable_states: Mapping[str, str],
    receipt_state: str,
    alias_new: bool,
    pointer_new: bool,
    catalog_new: bool,
    replace_pending: Mapping[str, str],
) -> None:
    ordered_create_states = [
        immutable_states["backtest_archive"],
        immutable_states["release_v3_config"],
        immutable_states["pointer_snapshot"],
        immutable_states["catalog_snapshot"],
        receipt_state,
    ]
    final_state = "published_nlink1"
    recoverable_states = {
        "pending_create_only",
        "pending_staging_transfer_nlink2",
        "published_recoverable_nlink2",
        "staging_recoverable_uncommitted",
    }
    incomplete_seen = False
    recoverable_count = 0
    for state in ordered_create_states:
        if state == final_state and not incomplete_seen:
            continue
        if state in recoverable_states and not incomplete_seen:
            incomplete_seen = True
            recoverable_count += 1
            continue
        if state == "missing":
            incomplete_seen = True
            continue
        raise ConfigLocatorReconciliationError("create-only publication prefix is impossible")
    if recoverable_count > 1:
        raise ConfigLocatorReconciliationError("multiple create-only recovery steps are impossible")
    if any(
        state == final_state
        for state in ordered_create_states[
            next(
                (
                    index
                    for index, state in enumerate(ordered_create_states)
                    if state != final_state
                ),
                len(ordered_create_states),
            ) :
        ]
    ):
        raise ConfigLocatorReconciliationError("create-only publication prefix has a gap")

    pending_roles = [role for role, state in replace_pending.items() if state != "absent"]
    if len(pending_roles) > 1:
        raise ConfigLocatorReconciliationError(
            "multiple mutable replacement pendings are impossible"
        )
    all_create_final = all(state == final_state for state in ordered_create_states)
    if (alias_new or pointer_new or catalog_new or pending_roles) and not all_create_final:
        raise ConfigLocatorReconciliationError(
            "mutable publication cannot precede all create-only final links"
        )
    if pointer_new and not alias_new:
        raise ConfigLocatorReconciliationError("pointer advanced before stable alias")
    if catalog_new and not pointer_new:
        raise ConfigLocatorReconciliationError("catalog advanced before pointer")
    if pending_roles == ["alias"] and (alias_new or pointer_new or catalog_new):
        raise ConfigLocatorReconciliationError("alias pending state is not an exact prefix")
    if pending_roles == ["pointer"] and (not alias_new or pointer_new or catalog_new):
        raise ConfigLocatorReconciliationError("pointer pending state is not an exact prefix")
    if pending_roles == ["catalog"] and (not alias_new or not pointer_new or catalog_new):
        raise ConfigLocatorReconciliationError("catalog pending state is not an exact prefix")


def _open_transaction_lock(metadata_root: Path) -> int:
    private_root = _absolute(metadata_root) / "docs" / "private"
    opened_path, descriptor = _open_directory_nofollow(private_root)
    before = os.fstat(descriptor)
    if not stat.S_ISDIR(before.st_mode) or before.st_uid != os.getuid() or before.st_mode & 0o077:
        os.close(descriptor)
        raise ConfigLocatorReconciliationError("metadata transaction directory is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    _LOCKED_PRIVATE_CONTEXT.set((opened_path, (before.st_dev, before.st_ino)))
    return descriptor


def _close_transaction_lock(descriptor: int) -> None:
    try:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)
        _LOCKED_PRIVATE_CONTEXT.set(None)


def _ensure_private_directory(path: Path) -> None:
    target, parent_fd = _open_parent_nofollow(path)
    try:
        try:
            metadata = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            os.mkdir(target.name, 0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
            metadata = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise ConfigLocatorReconciliationError(f"unsafe private directory: {path}")
        _revalidate_parent(target, parent_fd)
    finally:
        os.close(parent_fd)


def _paths(metadata_root: Path) -> dict[str, Path]:
    private = _absolute(metadata_root) / "docs" / "private"
    return {
        "private": private,
        "alias": private / CURRENT_ALIAS_FILENAME,
        "backtest_archive": private / BACKTEST_V12_ARCHIVE_FILENAME,
        "release_v3_config": private / RELEASE_V3_CONFIG_FILENAME,
        "pointer": private / "live_remote.current.local.json",
        "pointer_snapshot": private / PREDECESSOR_POINTER_SNAPSHOT_FILENAME,
        "catalog": private / "catalog.current.local.json",
        "catalog_snapshot": private / PREDECESSOR_CATALOG_SNAPSHOT_FILENAME,
        "receipt": private / RECEIPT_FILENAME,
    }


def _validate_v6_activation_locator(raw_path: Any, private_root: Path) -> Path:
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ConfigLocatorReconciliationError("current activation receipt locator is malformed")
    candidate = _absolute(Path(raw_path))
    private = _absolute(private_root)
    if candidate.parent != private or candidate.name in {"", ".", ".."}:
        raise ConfigLocatorReconciliationError(
            "current activation receipt locator escapes docs/private"
        )
    return candidate


def _v6_activation_path_from_pointer(pointer: Mapping[str, Any], private_root: Path) -> Path:
    projection = pointer.get("current_activation_receipt")
    if not isinstance(projection, Mapping):
        raise ConfigLocatorReconciliationError("current activation receipt locator is missing")
    return _validate_v6_activation_locator(projection.get("path"), private_root)


def _validate_exact_raw(
    path: Path,
    *,
    sha256: str,
    size_bytes: int,
) -> tuple[bytes, os.stat_result]:
    raw, metadata = _read_regular(path)
    if _sha(raw) != sha256 or len(raw) != size_bytes:
        raise ConfigLocatorReconciliationError(f"frozen source bytes drifted: {path}")
    return raw, metadata


def _validate_v6_activation(path: Path) -> tuple[dict[str, Any], bytes, dict[str, Any]]:
    payload, raw, _metadata = _load_json(path)
    binding = _canonical_binding(
        path,
        payload,
        raw,
        str(V6_ACTIVATION["canonical_field"]),
    )
    expected = {"path": str(_absolute(path)), **V6_ACTIVATION}
    if binding != expected:
        raise ConfigLocatorReconciliationError("immutable v6 activation receipt drifted")
    return payload, raw, binding


def _catalog_entry(catalog: Mapping[str, Any], artifact_id: str) -> dict[str, Any]:
    entries = catalog.get("entries")
    if catalog.get("schema_version") != CATALOG_SCHEMA or not isinstance(entries, list):
        raise ConfigLocatorReconciliationError("private catalog schema drifted")
    rows = [
        row for row in entries if isinstance(row, Mapping) and row.get("artifact_id") == artifact_id
    ]
    if len(rows) != 1:
        raise ConfigLocatorReconciliationError(f"catalog artifact is ambiguous: {artifact_id}")
    return deepcopy(dict(rows[0]))


def _validate_predecessor(
    metadata_root: Path,
) -> dict[str, Any]:
    paths = _paths(metadata_root)
    alias_raw, _alias_meta = _validate_exact_raw(
        paths["alias"], sha256=OLD_CONFIG_SHA256, size_bytes=OLD_CONFIG_SIZE
    )
    pointer, pointer_raw, _pointer_meta = _load_json(paths["pointer"])
    if (
        _sha(pointer_raw) != PREDECESSOR_POINTER_SHA256
        or len(pointer_raw) != PREDECESSOR_POINTER_SIZE
        or pointer.get("schema_version") != POINTER_SCHEMA
        or pointer.get("status") != "current_active"
        or pointer.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or pointer.get("current_config_locator_reconciliation") is not None
    ):
        raise ConfigLocatorReconciliationError("predecessor v6 pointer drifted")
    activation_path = _v6_activation_path_from_pointer(pointer, paths["private"])
    _activation, _activation_raw, activation_binding = _validate_v6_activation(activation_path)
    expected_activation_projection = {
        "path": activation_binding["path"],
        "sha256": activation_binding["file_sha256"],
        "canonical_sha256": activation_binding["canonical_sha256"],
        "bytes": activation_binding["size_bytes"],
    }
    if pointer.get("current_activation_receipt") != expected_activation_projection:
        raise ConfigLocatorReconciliationError(
            "predecessor pointer-to-v6 activation binding drifted"
        )
    release = pointer.get("current_buy_e3_release")
    if (
        not isinstance(release, Mapping)
        or release.get("active_release_file_sha256") != RELEASE_V3["file_sha256"]
        or release.get("active_release_canonical_sha256") != RELEASE_V3["canonical_sha256"]
        or release.get("external_venues_enabled") is not False
        or release.get("global_flow_shadow_enabled") is not False
        or release.get("global_reference_shadow_enabled") is not False
    ):
        raise ConfigLocatorReconciliationError("predecessor release-v3 projection drifted")
    catalog, catalog_raw, _catalog_meta = _load_json(paths["catalog"])
    if (
        _sha(catalog_raw) != PREDECESSOR_CATALOG_SHA256
        or len(catalog_raw) != PREDECESSOR_CATALOG_SIZE
    ):
        raise ConfigLocatorReconciliationError("predecessor catalog bytes drifted")
    current_config = _catalog_entry(catalog, CURRENT_CONFIG_ARTIFACT_ID)
    current_pointer = _catalog_entry(catalog, CURRENT_POINTER_ARTIFACT_ID)
    activation_entry = _catalog_entry(catalog, V6_ACTIVATION_ARTIFACT_ID)
    if (
        current_config.get("local_path") != str(paths["alias"])
        or current_config.get("sha256") != OLD_CONFIG_SHA256
        or current_config.get("bytes") != OLD_CONFIG_SIZE
        or current_pointer.get("local_path") != str(paths["pointer"])
        or current_pointer.get("sha256") != PREDECESSOR_POINTER_SHA256
        or current_pointer.get("bytes") != PREDECESSOR_POINTER_SIZE
        or activation_entry.get("local_path") != activation_binding["path"]
        or activation_entry.get("sha256") != activation_binding["file_sha256"]
        or activation_entry.get("bytes") != activation_binding["size_bytes"]
    ):
        raise ConfigLocatorReconciliationError("predecessor catalog chain drifted")
    return {
        "paths": paths,
        "alias_raw": alias_raw,
        "pointer": pointer,
        "pointer_raw": pointer_raw,
        "catalog": catalog,
        "catalog_raw": catalog_raw,
        "activation_binding": activation_binding,
        "catalog_entries": {
            "current_config": current_config,
            "current_pointer": current_pointer,
            "v6_activation": activation_entry,
        },
    }


def _validate_current_activation_crossbinding(
    metadata_root: Path,
    expected_activation_path: Path,
) -> dict[str, Any]:
    paths = _paths(metadata_root)
    expected_path = _validate_v6_activation_locator(
        str(_absolute(expected_activation_path)), paths["private"]
    )
    pointer, _pointer_raw, _pointer_metadata = _load_json(paths["pointer"])
    pointer_path = _v6_activation_path_from_pointer(pointer, paths["private"])
    if pointer_path != expected_path:
        raise ConfigLocatorReconciliationError(
            "current pointer activation locator drifted from manifest"
        )
    _activation, _activation_raw, activation_binding = _validate_v6_activation(expected_path)
    expected_projection = {
        "path": activation_binding["path"],
        "sha256": activation_binding["file_sha256"],
        "canonical_sha256": activation_binding["canonical_sha256"],
        "bytes": activation_binding["size_bytes"],
    }
    if pointer.get("current_activation_receipt") != expected_projection:
        raise ConfigLocatorReconciliationError("current pointer activation receipt binding drifted")
    catalog, _catalog_raw, _catalog_metadata = _load_json(paths["catalog"])
    activation_entry = _catalog_entry(catalog, V6_ACTIVATION_ARTIFACT_ID)
    if (
        activation_entry.get("local_path") != activation_binding["path"]
        or activation_entry.get("sha256") != activation_binding["file_sha256"]
        or activation_entry.get("bytes") != activation_binding["size_bytes"]
    ):
        raise ConfigLocatorReconciliationError("current catalog activation receipt binding drifted")
    return activation_binding


def _validate_active_config_source(path: Path) -> bytes:
    raw, _metadata = _validate_exact_raw(
        path,
        sha256=ACTIVE_CONFIG_SHA256,
        size_bytes=ACTIVE_CONFIG_SIZE,
    )
    return raw


def _transaction_contract(
    metadata_root: Path,
    active_config_source: Path,
    *,
    activation_path: Path,
) -> dict[str, Any]:
    paths = _paths(metadata_root)
    activation_path = _validate_v6_activation_locator(
        str(_absolute(activation_path)), paths["private"]
    )
    return {
        "predecessor": {
            "current_alias": {
                "path": str(paths["alias"]),
                "sha256": OLD_CONFIG_SHA256,
                "bytes": OLD_CONFIG_SIZE,
                "mode": "0600",
            },
            "current_pointer": {
                "path": str(paths["pointer"]),
                "sha256": PREDECESSOR_POINTER_SHA256,
                "bytes": PREDECESSOR_POINTER_SIZE,
                "mode": "0600",
            },
            "current_catalog": {
                "path": str(paths["catalog"]),
                "sha256": PREDECESSOR_CATALOG_SHA256,
                "bytes": PREDECESSOR_CATALOG_SIZE,
                "mode": "0600",
            },
            "v6_activation_receipt": {
                "path": str(activation_path),
                **V6_ACTIVATION,
            },
        },
        "active_config_source": {
            "path": str(_absolute(active_config_source)),
            "sha256": ACTIVE_CONFIG_SHA256,
            "bytes": ACTIVE_CONFIG_SIZE,
            "mode": "0600",
        },
        "outputs": {
            "backtest_v12_archive": str(paths["backtest_archive"]),
            "release_v3_versioned_config": str(paths["release_v3_config"]),
            "predecessor_pointer_snapshot": str(paths["pointer_snapshot"]),
            "predecessor_catalog_snapshot": str(paths["catalog_snapshot"]),
            "reconciliation_receipt": str(paths["receipt"]),
            "stable_live_config_alias": str(paths["alias"]),
            "current_remote_pointer": str(paths["pointer"]),
            "current_catalog": str(paths["catalog"]),
        },
        "artifact_ids": {
            "current_config": CURRENT_CONFIG_ARTIFACT_ID,
            "current_pointer": CURRENT_POINTER_ARTIFACT_ID,
            "v6_activation": V6_ACTIVATION_ARTIFACT_ID,
            "backtest_archive": BACKTEST_ARCHIVE_ARTIFACT_ID,
            "release_v3_config": RELEASE_V3_CONFIG_ARTIFACT_ID,
            "pointer_snapshot": POINTER_SNAPSHOT_ARTIFACT_ID,
            "catalog_snapshot": CATALOG_SNAPSHOT_ARTIFACT_ID,
            "reconciliation_receipt": RECONCILIATION_ARTIFACT_ID,
        },
        "ordered_publication": [
            "immutable_backtest_v12_archive",
            "immutable_release_v3_versioned_config",
            "immutable_predecessor_pointer_snapshot",
            "immutable_predecessor_catalog_snapshot",
            "immutable_reconciliation_receipt",
            "stable_live_config_alias",
            "current_remote_pointer",
            "current_catalog",
        ],
    }


def _activation_path_from_transaction(transaction: Mapping[str, Any], metadata_root: Path) -> Path:
    predecessor = transaction.get("predecessor")
    activation = (
        predecessor.get("v6_activation_receipt") if isinstance(predecessor, Mapping) else None
    )
    if not isinstance(activation, Mapping):
        raise ConfigLocatorReconciliationError("manifest activation receipt contract is missing")
    return _validate_v6_activation_locator(activation.get("path"), _paths(metadata_root)["private"])


def _build_manifest(
    *,
    publisher_root: Path,
    metadata_root: Path,
    active_config_source: Path,
    publisher_source: Mapping[str, Any],
    tracked_successor_files: Mapping[str, str],
    metadata_audit_baseline: Mapping[str, Any],
    activation_path: Path,
    generated_utc: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA,
        "status": MANIFEST_STATUS,
        "generated_utc": generated_utc,
        "receipt_id": MANIFEST_RECEIPT_ID,
        "publisher_root": str(_absolute(publisher_root)),
        "metadata_repository_root": str(_absolute(metadata_root)),
        "publisher_source": deepcopy(dict(publisher_source)),
        "tracked_successor_files": deepcopy(dict(tracked_successor_files)),
        "private_evidence_trust_boundary": _private_evidence_trust_contract(
            _private_evidence_root()
        ),
        "metadata_audit_baseline": deepcopy(dict(metadata_audit_baseline)),
        "transaction": _transaction_contract(
            metadata_root,
            active_config_source,
            activation_path=activation_path,
        ),
        "permissions": deepcopy(NO_NEW_AUTHORITY),
        "evidence_boundary": deepcopy(EVIDENCE_BOUNDARY),
    }
    payload[MANIFEST_CANONICAL_FIELD] = _document_sha(payload, MANIFEST_CANONICAL_FIELD)
    return payload


def _validate_manifest_payload(
    payload: Mapping[str, Any],
    path: Path,
    raw: bytes,
    *,
    recursive: bool,
) -> dict[str, Any]:
    expected_top = {
        "schema_version",
        "status",
        "generated_utc",
        "receipt_id",
        "publisher_root",
        "metadata_repository_root",
        "publisher_source",
        "tracked_successor_files",
        "private_evidence_trust_boundary",
        "metadata_audit_baseline",
        "transaction",
        "permissions",
        "evidence_boundary",
        MANIFEST_CANONICAL_FIELD,
    }
    if (
        set(payload) != expected_top
        or payload.get("schema_version") != MANIFEST_SCHEMA
        or payload.get("status") != MANIFEST_STATUS
        or payload.get("receipt_id") != MANIFEST_RECEIPT_ID
        or payload.get("permissions") != NO_NEW_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(MANIFEST_CANONICAL_FIELD) != _document_sha(payload, MANIFEST_CANONICAL_FIELD)
    ):
        raise ConfigLocatorReconciliationError("manifest identity drifted")
    formal_manifest_path = _formal_manifest_path()
    active_config_source = _active_config_source_path()
    if _absolute(path) != _absolute(formal_manifest_path):
        raise ConfigLocatorReconciliationError("formal manifest path drifted")
    generated_ns = _timestamp_ns(payload.get("generated_utc"), "manifest generated_utc")
    if generated_ns > _now_utc_ns() + MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise ConfigLocatorReconciliationError("manifest generated_utc is in the future")
    publisher_root = Path(str(payload.get("publisher_root", ""))).expanduser()
    metadata_root = Path(str(payload.get("metadata_repository_root", ""))).expanduser()
    if (
        not publisher_root.is_absolute()
        or not metadata_root.is_absolute()
        or publisher_root != _absolute(publisher_root)
        or metadata_root != _absolute(metadata_root)
    ):
        raise ConfigLocatorReconciliationError("manifest roots must be absolute")
    evidence_root = _private_evidence_root()
    _validate_private_evidence_layout(evidence_root, allow_manifest_leaf_missing=False)
    if payload.get("private_evidence_trust_boundary") != _private_evidence_trust_contract(
        evidence_root
    ):
        raise ConfigLocatorReconciliationError("private evidence trust boundary drifted")
    identities: list[tuple[int, int]] = []
    for label, root in (
        ("publisher", publisher_root),
        ("metadata", metadata_root),
        ("private evidence", evidence_root),
    ):
        _opened, descriptor = _open_directory_nofollow(root)
        try:
            root_stat = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if root_stat.st_uid != os.getuid():
            raise ConfigLocatorReconciliationError(f"{label} root owner drifted")
        identities.append((root_stat.st_dev, root_stat.st_ino))
    if len(set(identities)) != 3:
        raise ConfigLocatorReconciliationError(
            "publisher, metadata, and private evidence roots must be distinct"
        )
    roots = (publisher_root, metadata_root, evidence_root)
    if any(
        left != right and (left.is_relative_to(right) or right.is_relative_to(left))
        for index, left in enumerate(roots)
        for right in roots[index + 1 :]
    ):
        raise ConfigLocatorReconciliationError(
            "publisher, metadata, and private evidence roots must not overlap"
        )
    transaction = payload.get("transaction")
    if not isinstance(transaction, Mapping):
        raise ConfigLocatorReconciliationError("manifest transaction is missing")
    activation_path = _activation_path_from_transaction(transaction, metadata_root)
    audit_baseline = payload.get("metadata_audit_baseline")
    if not isinstance(audit_baseline, Mapping):
        raise ConfigLocatorReconciliationError("manifest metadata audit baseline is missing")
    _validate_audit_baseline_exact(audit_baseline)
    active_source = transaction.get("active_config_source")
    if (
        not isinstance(active_source, Mapping)
        or active_source
        != {
            "path": str(_absolute(active_config_source)),
            "sha256": ACTIVE_CONFIG_SHA256,
            "bytes": ACTIVE_CONFIG_SIZE,
            "mode": "0600",
        }
        or transaction
        != _transaction_contract(
            metadata_root,
            active_config_source,
            activation_path=activation_path,
        )
    ):
        raise ConfigLocatorReconciliationError("manifest transaction contract drifted")
    tracked = payload.get("tracked_successor_files")
    if not isinstance(tracked, Mapping) or set(tracked) != set(TRACKED_SUCCESSOR_FILES):
        raise ConfigLocatorReconciliationError("manifest tracked successor files drifted")
    for relative, digest in tracked.items():
        _require_sha(digest, f"tracked successor {relative}")
    publisher = payload.get("publisher_source")
    if not isinstance(publisher, Mapping):
        raise ConfigLocatorReconciliationError("manifest publisher source is missing")
    if recursive:
        _validate_tracked_source_pair(
            publisher_root=publisher_root,
            metadata_root=metadata_root,
            publisher=publisher,
            expected_tracked=tracked,
        )
        _validate_active_config_source(active_config_source)
        _validate_current_activation_crossbinding(metadata_root, activation_path)
        _assert_no_new_findings(audit_baseline, _audit_fn(metadata_root))
    binding = _canonical_binding(path, payload, raw, MANIFEST_CANONICAL_FIELD)
    return {"payload": dict(payload), "binding": binding}


def validate_manifest(path: Path, *, recursive: bool = True) -> dict[str, Any]:
    payload, raw, metadata = _load_json(path)
    validated = _validate_manifest_payload(payload, path, raw, recursive=recursive)
    generated_ns = _timestamp_ns(payload["generated_utc"], "manifest generated_utc")
    if abs(metadata.st_mtime_ns - generated_ns) > MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise ConfigLocatorReconciliationError("manifest mtime is not bound to generated_utc")
    return {
        "manifest": validated["binding"],
        "publisher_source": deepcopy(payload["publisher_source"]),
        "tracked_successor_file_count": len(payload["tracked_successor_files"]),
        "recursive_validation_passed": recursive,
    }


def _manifest_candidate(
    path: Path,
    *,
    formal_path: Path,
    allowed_nlinks: frozenset[int],
) -> tuple[dict[str, Any], bytes, os.stat_result]:
    payload, raw, metadata = _load_json(path, allowed_nlinks=allowed_nlinks)
    _validate_manifest_payload(payload, formal_path, raw, recursive=True)
    generated_ns = _timestamp_ns(payload["generated_utc"], "manifest generated_utc")
    if abs(metadata.st_mtime_ns - generated_ns) > MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise ConfigLocatorReconciliationError("manifest mtime is not bound to generated_utc")
    return payload, raw, metadata


def prepare_manifest(
    *,
    publisher_root: Path,
    metadata_repository_root: Path,
    active_config_source: Path,
    receipt_id: str,
    output_path: Path | None = None,
    recursive: bool = True,
) -> dict[str, Any]:
    if recursive is not True:
        raise ConfigLocatorReconciliationError("manifest publication requires recursion")
    if receipt_id != MANIFEST_RECEIPT_ID:
        raise ConfigLocatorReconciliationError("manifest receipt id drifted")
    formal_manifest_path = _formal_manifest_path()
    expected_active_source = _active_config_source_path()
    path = _absolute(formal_manifest_path if output_path is None else output_path)
    if path != _absolute(formal_manifest_path):
        raise ConfigLocatorReconciliationError("formal manifest output path drifted")
    if _absolute(active_config_source) != _absolute(expected_active_source):
        raise ConfigLocatorReconciliationError("active config source path drifted")
    metadata_root = _absolute(metadata_repository_root)
    evidence_root = _private_evidence_root()
    _validate_private_evidence_layout(evidence_root, allow_manifest_leaf_missing=True)
    descriptor = _open_transaction_lock(metadata_root)
    try:
        # The evidence unit root already exists and is authority-bound by the
        # configured durable root. Only this one reviewed 0700 leaf may be
        # created, before publication-state probes that require its parent.
        _ensure_private_directory(path.parent)
        pending = _pending(path, "create")
        path_exists = _secure_exists(path)
        pending_exists = _secure_exists(pending)
        staging = _manifest_staging_paths(path)
        if staging:
            if not path_exists and not pending_exists:
                _discard_orphan_manifest_staging(path, staging)
            elif not path_exists and pending_exists:
                _recover_manifest_staging_transfer(path, pending, staging)
            else:
                raise ConfigLocatorReconciliationError(
                    "manifest staging is ambiguous with published state"
                )
        if _secure_exists(path):
            payload, raw, metadata = _manifest_candidate(
                path,
                formal_path=path,
                allowed_nlinks=frozenset({1, 2}),
            )
            if metadata.st_nlink == 2:
                if not _secure_exists(pending):
                    raise ConfigLocatorReconciliationError(
                        "manifest nlink=2 lacks recovery pending"
                    )
                pending_payload, pending_raw, pending_meta = _manifest_candidate(
                    pending,
                    formal_path=path,
                    allowed_nlinks=frozenset({2}),
                )
                if (
                    pending_payload != payload
                    or pending_raw != raw
                    or (pending_meta.st_dev, pending_meta.st_ino)
                    != (metadata.st_dev, metadata.st_ino)
                ):
                    raise ConfigLocatorReconciliationError("manifest recovery hardlink drifted")
                _validate_predecessor(metadata_root)
                _require_exact_current_audit_baseline(
                    payload["metadata_audit_baseline"], metadata_root, _audit_fn
                )
                _secure_unlink(pending)
            elif _secure_exists(pending):
                raise ConfigLocatorReconciliationError("orphan manifest pending is ambiguous")
            pointer_raw, _pointer_metadata = _read_regular(_paths(metadata_root)["pointer"])
            if (_sha(pointer_raw), len(pointer_raw)) == (
                PREDECESSOR_POINTER_SHA256,
                PREDECESSOR_POINTER_SIZE,
            ):
                _validate_predecessor(metadata_root)
                _require_exact_current_audit_baseline(
                    payload["metadata_audit_baseline"], metadata_root, _audit_fn
                )
            result = validate_manifest(path, recursive=True)
            result["write_semantics"] = "create_only_idempotent_existing_exact_reused"
            return result
        if _secure_exists(pending):
            payload, raw, _metadata = _manifest_candidate(
                pending,
                formal_path=path,
                allowed_nlinks=frozenset({1}),
            )
            if (
                payload.get("publisher_root") != str(_absolute(publisher_root))
                or payload.get("metadata_repository_root") != str(metadata_root)
                or payload.get("receipt_id") != receipt_id
            ):
                raise ConfigLocatorReconciliationError("pending manifest inputs drifted")
            _validate_predecessor(metadata_root)
            _require_exact_current_audit_baseline(
                payload["metadata_audit_baseline"], metadata_root, _audit_fn
            )
            _secure_link(pending, path)
            _secure_unlink(pending)
            result = validate_manifest(path, recursive=True)
            result["write_semantics"] = "create_only_pending_recovered"
            return result

        publisher_root = _absolute(publisher_root)
        _validate_predecessor(metadata_root)
        _validate_active_config_source(active_config_source)
        publisher = _observe_publisher(publisher_root)
        tree_tracked = _tracked_successor_tree_files(publisher_root, str(publisher["commit"]))
        # All slow recursive checks finish before the final creation clock is
        # frozen. The manifest binds tag-tree blobs, not mutable worktree reads.
        _validate_tracked_source_pair(
            publisher_root=publisher_root,
            metadata_root=metadata_root,
            publisher=publisher,
            expected_tracked=tree_tracked,
        )
        if _secure_exists(path) or _secure_exists(pending) or _manifest_staging_paths(path):
            raise ConfigLocatorReconciliationError(
                "manifest publication state changed during validation"
            )
        _validate_predecessor(metadata_root)
        _validate_active_config_source(active_config_source)
        _validate_tracked_source_pair(
            publisher_root=publisher_root,
            metadata_root=metadata_root,
            publisher=publisher,
            expected_tracked=tree_tracked,
        )
        audit_observed = _audit_fn(metadata_root)
        audit_baseline = _audit_baseline(audit_observed)
        _assert_no_new_findings(audit_baseline, audit_observed)
        _validate_active_config_source(active_config_source)
        _validate_tracked_source_pair(
            publisher_root=publisher_root,
            metadata_root=metadata_root,
            publisher=publisher,
            expected_tracked=tree_tracked,
        )
        predecessor = _validate_predecessor(metadata_root)
        if _secure_exists(path) or _secure_exists(pending) or _manifest_staging_paths(path):
            raise ConfigLocatorReconciliationError(
                "manifest publication state changed before first writer"
            )
        generated_utc = _nanosecond_utc(_now_utc_ns())
        payload = _build_manifest(
            publisher_root=publisher_root,
            metadata_root=metadata_root,
            active_config_source=_absolute(active_config_source),
            publisher_source=publisher,
            tracked_successor_files=tree_tracked,
            metadata_audit_baseline=audit_baseline,
            activation_path=Path(str(predecessor["activation_binding"]["path"])),
            generated_utc=generated_utc,
        )
        raw = _render(payload)
        _validate_manifest_payload(payload, path, raw, recursive=False)
        _stage_manifest_pending(path, raw)
        _publish_create_only(path, raw)
        result = validate_manifest(path, recursive=True)
        result["write_semantics"] = "create_only_first_writer"
        return result
    finally:
        _close_transaction_lock(descriptor)


def _audit_fn(root: Path) -> Mapping[str, Any]:
    return audit_private_evidence.audit(
        root,
        mode=audit_private_evidence.METADATA_ONLY,
        deny_locked=True,
        allowlist_manifest=None,
    )


def _audit_baseline(audit: Mapping[str, Any]) -> dict[str, Any]:
    try:
        baseline = metadata_v6._audit_baseline(audit)
    except Exception as exc:
        raise ConfigLocatorReconciliationError(str(exc)) from exc
    return _validate_audit_baseline_exact(baseline)


def _require_exact_current_audit_baseline(
    expected: Mapping[str, Any],
    metadata_root: Path,
    audit_fn: AuditFn,
) -> dict[str, Any]:
    _validate_audit_baseline_exact(expected)
    observed = audit_fn(metadata_root)
    observed_baseline = _audit_baseline(observed)
    if observed_baseline != dict(expected):
        raise ConfigLocatorReconciliationError(
            "formal manifest metadata audit baseline drifted from current unfinished state"
        )
    return _assert_no_new_findings(expected, observed)


def _validate_audit_baseline_exact(
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    fingerprints = baseline.get("finding_fingerprints")
    if not isinstance(fingerprints, list) or any(
        not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None for value in fingerprints
    ):
        raise ConfigLocatorReconciliationError("metadata audit baseline fingerprints are malformed")
    if fingerprints != sorted(fingerprints) or len(fingerprints) != len(set(fingerprints)):
        raise ConfigLocatorReconciliationError(
            "metadata audit baseline fingerprints must be sorted and unique"
        )
    expected = {
        "schema_version": audit_private_evidence.AUDIT_SCHEMA,
        "mode": audit_private_evidence.METADATA_ONLY,
        "deny_locked": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "comparison_semantics": "after_finding_set_minus_before_finding_set",
        "preexisting_findings_may_remain": True,
        "required_new_finding_count": 0,
        "finding_count": len(fingerprints),
        "finding_fingerprints": list(fingerprints),
        "finding_set_sha256": _canonical(fingerprints),
    }
    if dict(baseline) != expected:
        raise ConfigLocatorReconciliationError("metadata audit baseline identity drifted")
    return deepcopy(expected)


def _assert_no_new_findings(
    baseline: Mapping[str, Any], audit: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        return metadata_v6._assert_no_new_findings(baseline, audit)
    except Exception as exc:
        raise ConfigLocatorReconciliationError(str(exc)) from exc


def _compare_audit_findings(
    baseline: Mapping[str, Any], audit: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        return metadata_v6._compare_audit_findings(baseline, audit)
    except Exception as exc:
        raise ConfigLocatorReconciliationError(str(exc)) from exc


def _immutable_bindings(
    paths: Mapping[str, Path],
    *,
    old_config_raw: bytes,
    active_config_raw: bytes,
    old_pointer_raw: bytes,
    old_catalog_raw: bytes,
) -> dict[str, dict[str, Any]]:
    return {
        "backtest_v12_archive": _binding(paths["backtest_archive"], old_config_raw),
        "release_v3_versioned_config": _binding(paths["release_v3_config"], active_config_raw),
        "predecessor_pointer_snapshot": _binding(paths["pointer_snapshot"], old_pointer_raw),
        "predecessor_catalog_snapshot": _binding(paths["catalog_snapshot"], old_catalog_raw),
    }


def _candidate_audit_contract(
    *,
    manifest: Mapping[str, Any],
    immutable_bindings: Mapping[str, Any],
    audit_baseline: Mapping[str, Any],
) -> dict[str, Any]:
    tracked = manifest.get("tracked_successor_files") or {}
    auditor_sha256 = tracked.get("scripts/audit_private_evidence.py")
    _require_sha(auditor_sha256, "candidate auditor source SHA256")
    independent_overrides = {
        "stable_live_config_alias": {
            "path": manifest["transaction"]["outputs"]["stable_live_config_alias"],
            "sha256": ACTIVE_CONFIG_SHA256,
            "bytes": ACTIVE_CONFIG_SIZE,
            "mode": "0600",
        },
        **deepcopy(dict(immutable_bindings)),
    }
    return {
        "schema_version": "narrowgate.prepublication_candidate_owner_root_audit_contract.v1",
        "auditor_module": "scripts.audit_private_evidence",
        "auditor_source_sha256": auditor_sha256,
        "mode": audit_private_evidence.METADATA_ONLY,
        "deny_locked": True,
        "catalog_path_remapping_required": True,
        "baseline_finding_set_sha256": audit_baseline.get("finding_set_sha256"),
        "required_new_finding_count": 0,
        "independent_override_bindings": independent_overrides,
        "self_referential_successor_roles": [
            "immutable_reconciliation_receipt",
            "current_remote_pointer",
            "current_catalog",
        ],
        "self_referential_hashes_embedded": False,
        "self_referential_bytes_require_structural_validation_and_full_candidate_audit": True,
    }


def _receipt_payload(
    *,
    manifest: Mapping[str, Any],
    manifest_binding: Mapping[str, Any],
    immutable_bindings: Mapping[str, Any],
    audit_baseline: Mapping[str, Any],
    candidate_audit_contract: Mapping[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "status": RECEIPT_STATUS,
        "generated_utc": manifest["generated_utc"],
        "manifest": deepcopy(dict(manifest_binding)),
        "publisher_source": deepcopy(dict(manifest["publisher_source"])),
        "tracked_successor_files": deepcopy(dict(manifest["tracked_successor_files"])),
        "predecessor": deepcopy(dict(manifest["transaction"]["predecessor"])),
        "immutable_outputs": deepcopy(dict(immutable_bindings)),
        "stable_live_config_alias": {
            "path": manifest["transaction"]["outputs"]["stable_live_config_alias"],
            "sha256": ACTIVE_CONFIG_SHA256,
            "bytes": ACTIVE_CONFIG_SIZE,
            "mode": "0600",
            "semantics": "mutable_compatibility_projection_of_versioned_release_v3_config",
        },
        "backtest_default": {
            "config": deepcopy(immutable_bindings["backtest_v12_archive"]),
            "config_sha256": OLD_CONFIG_SHA256,
            "current_live_alias_allowed": False,
            "immutable_v12_control_retained": True,
            "exact_buy_e3_replay_baseline_available": False,
            "buy_e3_economic_authority": False,
        },
        "current_live": {
            "config": deepcopy(immutable_bindings["release_v3_versioned_config"]),
            "stable_alias_sha256": ACTIVE_CONFIG_SHA256,
            "release_v3": deepcopy(RELEASE_V3),
            "runtime_v3": deepcopy(RUNTIME_V3),
            "no_shadow": True,
            "economic_outcomes_read": False,
            "economic_values_persisted": False,
            "nonbaseline_action_occurrence_proven": False,
            "economic_effect_proven": False,
        },
        "pointer_successor_contract": {
            "schema_version_preserved": POINTER_SCHEMA,
            "immutable_v6_activation_receipt_preserved": True,
            "additive_field": "current_config_locator_reconciliation",
            "current_activation_receipt_replaced": False,
        },
        "catalog_successor_contract": {
            "schema_version_preserved": CATALOG_SCHEMA,
            "current_config_role": "current_live_config_compatibility_alias",
            "new_artifact_ids": [
                BACKTEST_ARCHIVE_ARTIFACT_ID,
                RELEASE_V3_CONFIG_ARTIFACT_ID,
                POINTER_SNAPSHOT_ARTIFACT_ID,
                CATALOG_SNAPSHOT_ARTIFACT_ID,
                RECONCILIATION_ARTIFACT_ID,
            ],
        },
        "ordered_publication": deepcopy(manifest["transaction"]["ordered_publication"]),
        "metadata_audit_baseline": deepcopy(dict(audit_baseline)),
        "prepublication_candidate_owner_root_audit_contract": deepcopy(
            dict(candidate_audit_contract)
        ),
        "permissions": deepcopy(NO_NEW_AUTHORITY),
        "evidence_boundary": deepcopy(EVIDENCE_BOUNDARY),
        "receipt_is_release_authority": False,
        "receipt_is_backtest_economic_authority": False,
    }
    payload[RECEIPT_CANONICAL_FIELD] = _document_sha(payload, RECEIPT_CANONICAL_FIELD)
    return payload


def _receipt_binding(path: Path, payload: Mapping[str, Any], raw: bytes) -> dict[str, Any]:
    return _canonical_binding(path, payload, raw, RECEIPT_CANONICAL_FIELD)


def _pointer_payload(
    predecessor: Mapping[str, Any],
    *,
    receipt_binding: Mapping[str, Any],
    immutable_bindings: Mapping[str, Any],
    generated_utc: str,
    publisher_source: Mapping[str, Any],
) -> dict[str, Any]:
    pointer = deepcopy(dict(predecessor))
    original_activation = deepcopy(pointer.get("current_activation_receipt"))
    pointer["pointer_publication_status"] = (
        "completed_active_release_v3_no_shadow_evidence_closed_config_locator_reconciled"
    )
    pointer["current_config_locator_reconciliation"] = {
        "schema_version": RECEIPT_SCHEMA,
        "status": RECEIPT_STATUS,
        "generated_utc": generated_utc,
        "receipt": {
            "path": receipt_binding["path"],
            "sha256": receipt_binding["file_sha256"],
            "canonical_sha256": receipt_binding["canonical_sha256"],
            "bytes": receipt_binding["size_bytes"],
        },
        "stable_live_config_alias": {
            "path": str(_paths(Path(receipt_binding["path"]).parents[2])["alias"]),
            "sha256": ACTIVE_CONFIG_SHA256,
            "bytes": ACTIVE_CONFIG_SIZE,
        },
        "release_v3_versioned_config": deepcopy(immutable_bindings["release_v3_versioned_config"]),
        "backtest_v12_config_archive": deepcopy(immutable_bindings["backtest_v12_archive"]),
        "predecessor_pointer_snapshot": deepcopy(
            immutable_bindings["predecessor_pointer_snapshot"]
        ),
        "predecessor_catalog_snapshot": deepcopy(
            immutable_bindings["predecessor_catalog_snapshot"]
        ),
        "publisher_commit": publisher_source["commit"],
        "publisher_tree": publisher_source["tree"],
        "immutable_v6_activation_receipt_preserved": True,
        "backtest_v12_config_may_resolve_to_live_alias": False,
        "current_live_resolution": "private_remote_pointer_then_stable_live_config_alias",
        "backtest_default_resolution": "immutable_versioned_v12_config_archive",
        "economic_outcomes_read": False,
        "economic_values_persisted": False,
        "new_authority_granted": False,
    }
    query_policy = pointer.get("current_query_policy")
    if not isinstance(query_policy, dict):
        raise ConfigLocatorReconciliationError("predecessor pointer query policy is missing")
    query_policy["current_live_config_resolution"] = (
        "private_current_remote_pointer_then_stable_live_config_alias"
    )
    query_policy["backtest_default_config_resolution"] = (
        "immutable_versioned_v12_archive_never_current_live_alias"
    )
    query_policy["config_locator_reconciliation_updated_utc"] = generated_utc
    if pointer.get("current_activation_receipt") != original_activation:
        raise ConfigLocatorReconciliationError("pointer activation receipt was modified")
    return pointer


def _catalog_row(
    *,
    artifact_id: str,
    role: str,
    binding: Mapping[str, Any],
    generated_utc: str,
    notes: str,
) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "role": role,
        "local_path": binding["path"],
        "source_document": None,
        "source_line_before_migration": None,
        "sha256": binding.get("sha256", binding.get("file_sha256")),
        "bytes": binding.get("bytes", binding.get("size_bytes")),
        "availability": "private_not_distributed",
        "panel_role": "operational",
        "read_gate": "owner_only",
        "last_verified_utc": generated_utc,
        "related_public_docs": [
            "docs/live_host_and_historical_data_access_20260811.md",
            "research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260825_v13.json",
        ],
        "public_projection": None,
        "notes": notes,
    }


def _catalog_payload(
    predecessor: Mapping[str, Any],
    *,
    pointer_data: bytes,
    receipt_binding: Mapping[str, Any],
    immutable_bindings: Mapping[str, Any],
    generated_utc: str,
) -> dict[str, Any]:
    catalog = deepcopy(dict(predecessor))
    entries = catalog.get("entries")
    if catalog.get("schema_version") != CATALOG_SCHEMA or not isinstance(entries, list):
        raise ConfigLocatorReconciliationError("predecessor catalog schema drifted")
    reserved = {
        BACKTEST_ARCHIVE_ARTIFACT_ID,
        RELEASE_V3_CONFIG_ARTIFACT_ID,
        POINTER_SNAPSHOT_ARTIFACT_ID,
        CATALOG_SNAPSHOT_ARTIFACT_ID,
        RECONCILIATION_ARTIFACT_ID,
    }
    if any(row.get("artifact_id") in reserved for row in entries if isinstance(row, Mapping)):
        raise ConfigLocatorReconciliationError("reconciliation catalog id already exists")
    current_config = _catalog_entry(catalog, CURRENT_CONFIG_ARTIFACT_ID)
    current_pointer = _catalog_entry(catalog, CURRENT_POINTER_ARTIFACT_ID)
    current_config.update(
        {
            "role": "current_live_config_compatibility_alias",
            "sha256": ACTIVE_CONFIG_SHA256,
            "bytes": ACTIVE_CONFIG_SIZE,
            "read_gate": "owner_authorized_locator_resolution_only",
            "last_verified_utc": generated_utc,
            "notes": (
                "Mutable current-live compatibility alias for the exact release-v3 no-shadow "
                "config. Backtests must use the immutable versioned v12 archive."
            ),
        }
    )
    current_pointer.update(
        {
            "sha256": _sha(pointer_data),
            "bytes": len(pointer_data),
            "last_verified_utc": generated_utc,
            "notes": (
                "Mutable current remote pointer retains the immutable v6 activation receipt "
                "and adds an exact config-locator reconciliation binding."
            ),
        }
    )
    for index, row in enumerate(entries):
        if row.get("artifact_id") == CURRENT_CONFIG_ARTIFACT_ID:
            entries[index] = current_config
        elif row.get("artifact_id") == CURRENT_POINTER_ARTIFACT_ID:
            entries[index] = current_pointer
    entries.extend(
        [
            _catalog_row(
                artifact_id=BACKTEST_ARCHIVE_ARTIFACT_ID,
                role="immutable_backtest_v12_control_config_archive",
                binding=immutable_bindings["backtest_v12_archive"],
                generated_utc=generated_utc,
                notes=(
                    "Create-only predecessor v12 config retained as the backtest default; "
                    "it is not current live configuration."
                ),
            ),
            _catalog_row(
                artifact_id=RELEASE_V3_CONFIG_ARTIFACT_ID,
                role="immutable_current_live_release_v3_no_shadow_config",
                binding=immutable_bindings["release_v3_versioned_config"],
                generated_utc=generated_utc,
                notes=(
                    "Create-only exact release-v3 no-shadow configuration; live authority "
                    "remains the immutable release and final-v6 evidence chain."
                ),
            ),
            _catalog_row(
                artifact_id=POINTER_SNAPSHOT_ARTIFACT_ID,
                role="immutable_pre_config_reconciliation_v6_pointer_snapshot",
                binding=immutable_bindings["predecessor_pointer_snapshot"],
                generated_utc=generated_utc,
                notes="Create-only predecessor v6 current-remote pointer bytes.",
            ),
            _catalog_row(
                artifact_id=CATALOG_SNAPSHOT_ARTIFACT_ID,
                role="immutable_pre_config_reconciliation_catalog_snapshot",
                binding=immutable_bindings["predecessor_catalog_snapshot"],
                generated_utc=generated_utc,
                notes="Create-only predecessor private catalog bytes.",
            ),
            _catalog_row(
                artifact_id=RECONCILIATION_ARTIFACT_ID,
                role="current_live_config_locator_reconciliation_receipt",
                binding=receipt_binding,
                generated_utc=generated_utc,
                notes=(
                    "Create-only governance receipt for config locator reconciliation. It "
                    "grants no research, action, live, or backtest-economic authority."
                ),
            ),
        ]
    )
    catalog["generated_at_utc"] = generated_utc
    return catalog


def _validate_receipt(
    payload: Mapping[str, Any],
    raw: bytes,
    *,
    path: Path,
    manifest: Mapping[str, Any],
    manifest_binding: Mapping[str, Any],
    immutable_bindings: Mapping[str, Any],
) -> dict[str, Any]:
    audit_baseline = payload.get("metadata_audit_baseline", {})
    expected_candidate_contract = _candidate_audit_contract(
        manifest=manifest,
        immutable_bindings=immutable_bindings,
        audit_baseline=audit_baseline,
    )
    if (
        payload.get("schema_version") != RECEIPT_SCHEMA
        or payload.get("status") != RECEIPT_STATUS
        or payload.get("manifest") != manifest_binding
        or payload.get("permissions") != NO_NEW_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get("receipt_is_release_authority") is not False
        or payload.get("receipt_is_backtest_economic_authority") is not False
        or payload.get("prepublication_candidate_owner_root_audit_contract")
        != expected_candidate_contract
        or payload.get(RECEIPT_CANONICAL_FIELD) != _document_sha(payload, RECEIPT_CANONICAL_FIELD)
    ):
        raise ConfigLocatorReconciliationError("reconciliation receipt identity drifted")
    expected = _receipt_payload(
        manifest=manifest,
        manifest_binding=manifest_binding,
        immutable_bindings=immutable_bindings,
        audit_baseline=audit_baseline,
        candidate_audit_contract=expected_candidate_contract,
    )
    if payload != expected or raw != _render(expected):
        raise ConfigLocatorReconciliationError("reconciliation receipt differs from plan")
    try:
        metadata_v6._reject_secrets(payload)
        metadata_v6._reject_economic_fields(payload)
    except Exception as exc:
        raise ConfigLocatorReconciliationError(str(exc)) from exc
    return _receipt_binding(path, payload, raw)


def _validate_candidate(
    *,
    predecessor_pointer: Mapping[str, Any],
    predecessor_catalog: Mapping[str, Any],
    receipt: Mapping[str, Any],
    receipt_data: bytes,
    pointer: Mapping[str, Any],
    pointer_data: bytes,
    catalog: Mapping[str, Any],
    catalog_data: bytes,
    immutable_bindings: Mapping[str, Any],
) -> dict[str, Any]:
    reconciliation = pointer.get("current_config_locator_reconciliation")
    if (
        pointer.get("schema_version") != POINTER_SCHEMA
        or pointer.get("status") != "current_active"
        or pointer.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or pointer.get("current_activation_receipt")
        != predecessor_pointer.get("current_activation_receipt")
        or not isinstance(reconciliation, Mapping)
        or reconciliation.get("status") != RECEIPT_STATUS
        or reconciliation.get("immutable_v6_activation_receipt_preserved") is not True
        or reconciliation.get("backtest_v12_config_may_resolve_to_live_alias") is not False
    ):
        raise ConfigLocatorReconciliationError("pointer successor contract drifted")
    if catalog_data != _render(catalog):
        raise ConfigLocatorReconciliationError("catalog candidate bytes drifted")
    if receipt_data != _render(receipt) or pointer_data != _render(pointer):
        raise ConfigLocatorReconciliationError("receipt/pointer candidate bytes drifted")
    if len(catalog.get("entries", [])) != len(predecessor_catalog.get("entries", [])) + 5:
        raise ConfigLocatorReconciliationError("catalog candidate entry count drifted")
    current_config = _catalog_entry(catalog, CURRENT_CONFIG_ARTIFACT_ID)
    current_pointer = _catalog_entry(catalog, CURRENT_POINTER_ARTIFACT_ID)
    if (
        current_config.get("sha256") != ACTIVE_CONFIG_SHA256
        or current_config.get("bytes") != ACTIVE_CONFIG_SIZE
        or current_config.get("role") != "current_live_config_compatibility_alias"
        or current_pointer.get("sha256") != _sha(pointer_data)
        or current_pointer.get("bytes") != len(pointer_data)
    ):
        raise ConfigLocatorReconciliationError("candidate current catalog rows drifted")
    expected_rows = {
        BACKTEST_ARCHIVE_ARTIFACT_ID: immutable_bindings["backtest_v12_archive"],
        RELEASE_V3_CONFIG_ARTIFACT_ID: immutable_bindings["release_v3_versioned_config"],
        POINTER_SNAPSHOT_ARTIFACT_ID: immutable_bindings["predecessor_pointer_snapshot"],
        CATALOG_SNAPSHOT_ARTIFACT_ID: immutable_bindings["predecessor_catalog_snapshot"],
    }
    for artifact_id, binding in expected_rows.items():
        row = _catalog_entry(catalog, artifact_id)
        if (
            row.get("local_path") != binding["path"]
            or row.get("sha256") != binding["sha256"]
            or row.get("bytes") != binding["bytes"]
        ):
            raise ConfigLocatorReconciliationError(
                f"candidate catalog binding drifted: {artifact_id}"
            )
    receipt_row = _catalog_entry(catalog, RECONCILIATION_ARTIFACT_ID)
    if (
        receipt_row.get("local_path") != reconciliation["receipt"]["path"]
        or receipt_row.get("sha256") != _sha(receipt_data)
        or receipt_row.get("bytes") != len(receipt_data)
    ):
        raise ConfigLocatorReconciliationError("candidate receipt catalog row drifted")
    try:
        for candidate in (receipt, pointer, catalog):
            metadata_v6._reject_secrets(candidate)
            metadata_v6._reject_economic_fields(candidate)
    except Exception as exc:
        raise ConfigLocatorReconciliationError(str(exc)) from exc
    return {
        "proof_semantics": "deterministic_structural_candidate_cross_binding",
        "exact_immutable_outputs_validated": 5,
        "exact_mutable_successors_validated": 3,
        "catalog_entry_delta_validated": True,
        "secret_scan_passed": True,
        "economic_value_scan_passed": True,
        "immutable_v6_activation_receipt_preserved": True,
        "passed": True,
    }


def _write_skeleton_file(
    source: Path,
    destination: Path,
    metadata: os.stat_result,
    *,
    inode_targets: dict[tuple[int, int], Path],
    read_contents: bool,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISLNK(metadata.st_mode):
        destination.symlink_to(os.readlink(source))
        return
    if not stat.S_ISREG(metadata.st_mode):
        return
    identity = (metadata.st_dev, metadata.st_ino)
    prior = inode_targets.get(identity)
    if prior is not None:
        os.link(prior, destination, follow_symlinks=False)
        return
    if read_contents:
        raw, source_meta = _read_regular(
            source,
            mode=mode,
            allowed_nlinks=frozenset({metadata.st_nlink}),
        )
        if _topology_stat_identity(source_meta) != _topology_stat_identity(metadata):
            raise ConfigLocatorReconciliationError(
                f"governance metadata changed between walk and secure read: {source}"
            )
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            mode,
        )
        try:
            os.write(descriptor, raw)
        finally:
            os.close(descriptor)
    else:
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            mode,
        )
        os.close(descriptor)
    os.chmod(destination, mode, follow_symlinks=False)
    inode_targets[identity] = destination


def _topology_stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _private_topology_snapshot(root: Path) -> tuple[tuple[str, tuple[int, ...]], ...]:
    records: dict[str, tuple[int, ...]] = {}
    for directory, names, files in os.walk(root, followlinks=False):
        source_directory = Path(directory)
        relative_directory = source_directory.relative_to(root)
        directory_key = "." if relative_directory == Path(".") else relative_directory.as_posix()
        records[directory_key] = _topology_stat_identity(source_directory.lstat())
        for name in (*tuple(names), *tuple(files)):
            source_path = source_directory / name
            relative = source_path.relative_to(root).as_posix()
            if relative in records:
                raise ConfigLocatorReconciliationError(
                    f"duplicate private topology path: {relative}"
                )
            records[relative] = _topology_stat_identity(source_path.lstat())
    return tuple(sorted(records.items()))


def _copy_private_metadata_skeleton(metadata_root: Path, candidate_root: Path) -> None:
    """Mirror private metadata topology without reading payload file contents."""

    inode_targets: dict[tuple[int, int], Path] = {}
    governance_paths = {
        *(root / "README.local.md" for root in audit_private_evidence.PRIVATE_OWNER_ROOTS),
        *(
            root / "catalog.current.local.json"
            for root in audit_private_evidence.PRIVATE_OWNER_ROOTS
        ),
        audit_private_evidence.NONPUBLISHED_INDEX,
    }
    for relative_root in audit_private_evidence.PRIVATE_OWNER_ROOTS:
        source_root = metadata_root / relative_root
        destination_root = candidate_root / relative_root
        if not _secure_exists(source_root):
            continue
        _opened_source, source_fd = _open_directory_nofollow(source_root)
        source_metadata = os.fstat(source_fd)
        os.close(source_fd)
        topology_before = dict(_private_topology_snapshot(source_root))
        destination_root.mkdir(parents=True, exist_ok=True)
        os.chmod(destination_root, stat.S_IMODE(source_metadata.st_mode))
        for directory, names, files in os.walk(source_root, followlinks=False):
            source_directory = Path(directory)
            relative_directory = source_directory.relative_to(source_root)
            destination_directory = destination_root / relative_directory
            directory_metadata = source_directory.lstat()
            directory_key = (
                "." if relative_directory == Path(".") else relative_directory.as_posix()
            )
            if topology_before.get(directory_key) != _topology_stat_identity(directory_metadata):
                raise ConfigLocatorReconciliationError(
                    f"private directory topology drifted: {relative_root / relative_directory}"
                )
            destination_directory.mkdir(parents=True, exist_ok=True)
            os.chmod(destination_directory, stat.S_IMODE(directory_metadata.st_mode))
            for name in tuple(names):
                source_path = source_directory / name
                destination_path = destination_directory / name
                child_metadata = source_path.lstat()
                child_key = source_path.relative_to(source_root).as_posix()
                if topology_before.get(child_key) != _topology_stat_identity(child_metadata):
                    raise ConfigLocatorReconciliationError(
                        f"private child topology drifted: {relative_root / Path(child_key)}"
                    )
                if stat.S_ISLNK(child_metadata.st_mode):
                    destination_path.symlink_to(os.readlink(source_path), target_is_directory=True)
                    names.remove(name)
                elif stat.S_ISDIR(child_metadata.st_mode):
                    destination_path.mkdir(exist_ok=True)
                    os.chmod(destination_path, stat.S_IMODE(child_metadata.st_mode))
            for name in files:
                source_path = source_directory / name
                source_metadata_row = source_path.lstat()
                source_key = source_path.relative_to(source_root).as_posix()
                if topology_before.get(source_key) != _topology_stat_identity(source_metadata_row):
                    raise ConfigLocatorReconciliationError(
                        f"private file topology drifted: {relative_root / Path(source_key)}"
                    )
                _write_skeleton_file(
                    source_path,
                    destination_directory / name,
                    source_metadata_row,
                    inode_targets=inode_targets,
                    read_contents=source_path.relative_to(metadata_root) in governance_paths,
                )
        _opened_after, source_after_fd = _open_directory_nofollow(source_root)
        try:
            source_after = os.fstat(source_after_fd)
        finally:
            os.close(source_after_fd)
        topology_after = _private_topology_snapshot(source_root)
        if (
            _topology_stat_identity(source_metadata) != _topology_stat_identity(source_after)
            or tuple(topology_before.items()) != topology_after
        ):
            raise ConfigLocatorReconciliationError(
                f"private owner root changed during candidate audit: {relative_root}"
            )

    # The nonpublished index is a metadata-only governance input even when a
    # test or a future constrained audit uses an owner-root subset that does
    # not include its parent. Copy only this exact path, never a basename
    # match, so nested lookalikes remain topology-only.
    source_index = metadata_root / audit_private_evidence.NONPUBLISHED_INDEX
    destination_index = candidate_root / audit_private_evidence.NONPUBLISHED_INDEX
    if _secure_exists(source_index) and not destination_index.exists():
        source_metadata = source_index.lstat()
        destination_index.parent.mkdir(parents=True, exist_ok=True)
        _write_skeleton_file(
            source_index,
            destination_index,
            source_metadata,
            inode_targets=inode_targets,
            read_contents=True,
        )


def _validate_candidate_auditor_identity(
    *,
    publisher_root: Path,
    publisher_source: Mapping[str, Any],
    audit_contract: Mapping[str, Any],
    candidate_root: Path | None = None,
) -> str:
    if audit_contract.get("auditor_module") != "scripts.audit_private_evidence":
        raise ConfigLocatorReconciliationError("candidate auditor module drifted")
    expected_sha256 = _require_sha(
        audit_contract.get("auditor_source_sha256"),
        "candidate auditor source SHA256",
    )
    expected_path = _absolute(publisher_root / "scripts/audit_private_evidence.py")
    loaded_origin = getattr(audit_private_evidence, "__file__", None)
    if not isinstance(loaded_origin, str) or _absolute(Path(loaded_origin)) != expected_path:
        raise ConfigLocatorReconciliationError("candidate auditor module origin drifted")
    publisher_raw, _publisher_metadata = _read_regular(expected_path, mode=0o644)
    commit = _require_oid(publisher_source.get("commit"), "candidate publisher commit")
    tagged_raw = _git_bytes(
        publisher_root,
        "cat-file",
        "blob",
        f"{commit}:scripts/audit_private_evidence.py",
    )
    observed = _sha(publisher_raw)
    if publisher_raw != tagged_raw or observed != expected_sha256:
        raise ConfigLocatorReconciliationError("candidate auditor source identity drifted")
    if candidate_root is not None:
        candidate_raw, _candidate_metadata = _read_regular(
            candidate_root / "scripts/audit_private_evidence.py", mode=0o644
        )
        if candidate_raw != tagged_raw:
            raise ConfigLocatorReconciliationError(
                "candidate checkout auditor source identity drifted"
            )
    return observed


def _full_candidate_owner_root_audit(
    *,
    metadata_root: Path,
    publisher_root: Path,
    publisher_source: Mapping[str, Any],
    audit_baseline: Mapping[str, Any],
    audit_contract: Mapping[str, Any],
    planned_files: Mapping[str, tuple[Path, bytes]],
) -> dict[str, Any]:
    if audit_contract.get("required_new_finding_count") != 0:
        raise ConfigLocatorReconciliationError("candidate audit contract drifted")
    _validate_candidate_auditor_identity(
        publisher_root=publisher_root,
        publisher_source=publisher_source,
        audit_contract=audit_contract,
    )
    with tempfile.TemporaryDirectory(
        prefix="narrowgate-config-reconciliation-candidate-",
        dir=Path(tempfile.gettempdir()).resolve(strict=True),
    ) as temporary:
        candidate_root = Path(temporary) / "repository"
        clone = subprocess.run(
            [
                "git",
                "--no-optional-locks",
                "clone",
                "--quiet",
                "--shared",
                "--no-checkout",
                str(publisher_root),
                str(candidate_root),
            ],
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        )
        if clone.returncode != 0:
            raise ConfigLocatorReconciliationError(
                f"candidate checkout clone failed: {clone.stderr.strip()}"
            )
        _git(candidate_root, "checkout", "--quiet", "--detach", str(publisher_source["commit"]))
        _validate_candidate_auditor_identity(
            publisher_root=publisher_root,
            publisher_source=publisher_source,
            audit_contract=audit_contract,
            candidate_root=candidate_root,
        )
        _copy_private_metadata_skeleton(metadata_root, candidate_root)
        for path in (metadata_root / "docs/private").glob(".*.pending-config-reconciliation-v1"):
            relative = path.relative_to(metadata_root)
            candidate_pending = candidate_root / relative
            if candidate_pending.exists() or candidate_pending.is_symlink():
                candidate_pending.unlink()
        override_bindings: dict[str, dict[str, Any]] = {}
        for role, (official_path, raw) in planned_files.items():
            try:
                relative = official_path.relative_to(metadata_root)
            except ValueError as exc:
                raise ConfigLocatorReconciliationError(
                    f"candidate override escapes metadata root: {role}"
                ) from exc
            candidate_path = candidate_root / relative
            if candidate_path.exists() or candidate_path.is_symlink():
                candidate_path.unlink()
            candidate_path.parent.mkdir(parents=True, exist_ok=True)
            _write_new(candidate_path, raw)
            override_bindings[role] = {
                "relative_path": relative.as_posix(),
                "sha256": _sha(raw),
                "bytes": len(raw),
                "mode": "0600",
            }
        observed = audit_private_evidence.audit(
            candidate_root,
            mode=audit_private_evidence.METADATA_ONLY,
            deny_locked=True,
            allowlist_manifest=None,
            catalog_path_source_root=metadata_root,
        )
        _validate_candidate_auditor_identity(
            publisher_root=publisher_root,
            publisher_source=publisher_source,
            audit_contract=audit_contract,
            candidate_root=candidate_root,
        )
    source_root_sha256 = hashlib.sha256(str(metadata_root).encode("utf-8")).hexdigest()
    if (
        observed.get("catalog_path_remapping_enabled") is not True
        or observed.get("catalog_path_source_root_sha256") != source_root_sha256
        or observed.get("validation_read") is not False
        or observed.get("sealed_holdout_read") is not False
        or observed.get("payload_files_opened") != 0
    ):
        raise ConfigLocatorReconciliationError("candidate audit envelope drifted")
    comparison = _assert_no_new_findings(audit_baseline, observed)
    observed_baseline = _audit_baseline(observed)
    return {
        "schema_version": "narrowgate.prepublication_candidate_owner_root_audit.v1",
        "status": "full_metadata_only_candidate_owner_root_audit_passed",
        "audit_contract_sha256": _canonical(audit_contract),
        "catalog_path_remapping_enabled": True,
        "catalog_path_source_root_sha256": source_root_sha256,
        "candidate_finding_set_sha256": observed_baseline["finding_set_sha256"],
        "candidate_finding_count": observed_baseline["finding_count"],
        "new_finding_count": comparison["new_finding_count"],
        "planned_override_bindings": override_bindings,
        "validation_read": False,
        "sealed_holdout_read": False,
        "payload_files_opened": 0,
        "passed": True,
    }


AuditFn = Callable[[Path], Mapping[str, Any]]
FailureFn = Callable[[str], None]


def _load_manifest_for_staging_preflight(path: Path) -> dict[str, Any]:
    payload, raw, metadata = _load_json(path)
    validated = _validate_manifest_payload(payload, path, raw, recursive=False)
    generated_ns = _timestamp_ns(payload["generated_utc"], "manifest generated_utc")
    if abs(metadata.st_mtime_ns - generated_ns) > MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise ConfigLocatorReconciliationError("manifest mtime drifted")
    return validated["payload"]


def _load_manifest_for_execute(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload, raw, metadata = _load_json(path)
    validated = _validate_manifest_payload(payload, path, raw, recursive=True)
    generated_ns = _timestamp_ns(payload["generated_utc"], "manifest generated_utc")
    if abs(metadata.st_mtime_ns - generated_ns) > MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise ConfigLocatorReconciliationError("manifest mtime drifted")
    return validated["payload"], validated["binding"]


def _validate_execution_immutable_authorities(
    *,
    manifest_path: Path,
    expected_manifest: Mapping[str, Any],
    expected_manifest_binding: Mapping[str, Any],
    activation_path: Path,
) -> dict[str, Any]:
    evidence_root = _private_evidence_root()
    _validate_private_evidence_layout(evidence_root, allow_manifest_leaf_missing=False)
    payload, raw, metadata = _load_json(manifest_path)
    validated = _validate_manifest_payload(payload, manifest_path, raw, recursive=False)
    generated_ns = _timestamp_ns(payload["generated_utc"], "manifest generated_utc")
    if (
        abs(metadata.st_mtime_ns - generated_ns) > MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000
        or payload != dict(expected_manifest)
        or validated["binding"] != dict(expected_manifest_binding)
    ):
        raise ConfigLocatorReconciliationError("formal manifest immutable authority drifted")
    activation_binding = _validate_current_activation_crossbinding(
        Path(str(payload["metadata_repository_root"])), activation_path
    )
    transaction = payload.get("transaction")
    if not isinstance(transaction, Mapping):
        raise ConfigLocatorReconciliationError("manifest transaction contract is missing")
    predecessor = transaction.get("predecessor")
    expected_activation = (
        predecessor.get("v6_activation_receipt") if isinstance(predecessor, Mapping) else None
    )
    if not isinstance(expected_activation, Mapping) or activation_binding != dict(
        expected_activation
    ):
        raise ConfigLocatorReconciliationError("v6 activation immutable authority drifted")
    return {
        "manifest": deepcopy(dict(validated["binding"])),
        "v6_activation": deepcopy(dict(activation_binding)),
        "private_evidence_layout_exact": True,
    }


def _validate_execution_source_identity(manifest: Mapping[str, Any]) -> None:
    publisher_root = Path(str(manifest["publisher_root"]))
    metadata_root = Path(str(manifest["metadata_repository_root"]))
    publisher = manifest.get("publisher_source")
    tracked = manifest.get("tracked_successor_files")
    if not isinstance(publisher, Mapping) or not isinstance(tracked, Mapping):
        raise ConfigLocatorReconciliationError("manifest source identity is incomplete")
    _validate_tracked_source_pair(
        publisher_root=publisher_root,
        metadata_root=metadata_root,
        publisher=publisher,
        expected_tracked=tracked,
    )
    _validate_active_config_source(_active_config_source_path())


def _read_create_candidate(path: Path, expected: bytes) -> tuple[bytes, str]:
    pending = _pending(path, "create")
    if _secure_exists(path):
        state = _validate_create_state(path, expected)
        raw, _meta = _read_regular(path, allowed_nlinks=frozenset({1, 2}))
        return raw, state
    if _secure_exists(pending):
        state = _validate_create_state(path, expected)
        raw, _meta = _read_regular(pending)
        return raw, state
    return expected, "missing"


def _read_frozen_create_candidate(
    path: Path,
    *,
    sha256: str,
    size_bytes: int,
) -> tuple[bytes, str]:
    pending = _pending(path, "create")
    path_exists = _secure_exists(path)
    pending_exists = _secure_exists(pending)
    if not path_exists and not pending_exists:
        return b"", "missing"
    candidate = path if path_exists else pending
    allowed_links = frozenset({1, 2}) if path_exists else frozenset({1})
    raw, metadata = _read_regular(candidate, allowed_nlinks=allowed_links)
    if _sha(raw) != sha256 or len(raw) != size_bytes:
        raise ConfigLocatorReconciliationError(f"frozen create-only bytes drifted: {path}")
    state = _validate_create_state(path, raw)
    return raw, state


def _parse_frozen_json(raw: bytes, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw, object_pairs_hook=_json_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfigLocatorReconciliationError(f"invalid frozen JSON: {label}") from exc
    if not isinstance(payload, dict):
        raise ConfigLocatorReconciliationError(f"frozen JSON root drifted: {label}")
    return payload


def _validate_frozen_predecessor_material(
    paths: Mapping[str, Path],
    *,
    alias_raw: bytes,
    pointer_raw: bytes,
    catalog_raw: bytes,
    expected_activation_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if (
        _sha(alias_raw) != OLD_CONFIG_SHA256
        or len(alias_raw) != OLD_CONFIG_SIZE
        or _sha(pointer_raw) != PREDECESSOR_POINTER_SHA256
        or len(pointer_raw) != PREDECESSOR_POINTER_SIZE
        or _sha(catalog_raw) != PREDECESSOR_CATALOG_SHA256
        or len(catalog_raw) != PREDECESSOR_CATALOG_SIZE
    ):
        raise ConfigLocatorReconciliationError("frozen predecessor material drifted")
    pointer = _parse_frozen_json(pointer_raw, "predecessor pointer")
    catalog = _parse_frozen_json(catalog_raw, "predecessor catalog")
    activation_path = _v6_activation_path_from_pointer(pointer, paths["private"])
    if activation_path != _absolute(expected_activation_path):
        raise ConfigLocatorReconciliationError(
            "frozen predecessor activation locator drifted from manifest"
        )
    _activation, _activation_raw, activation_binding = _validate_v6_activation(activation_path)
    if (
        pointer.get("schema_version") != POINTER_SCHEMA
        or pointer.get("status") != "current_active"
        or pointer.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or pointer.get("current_config_locator_reconciliation") is not None
        or pointer.get("current_activation_receipt")
        != {
            "path": activation_binding["path"],
            "sha256": activation_binding["file_sha256"],
            "canonical_sha256": activation_binding["canonical_sha256"],
            "bytes": activation_binding["size_bytes"],
        }
    ):
        raise ConfigLocatorReconciliationError("frozen predecessor pointer semantics drifted")
    config_row = _catalog_entry(catalog, CURRENT_CONFIG_ARTIFACT_ID)
    pointer_row = _catalog_entry(catalog, CURRENT_POINTER_ARTIFACT_ID)
    activation_row = _catalog_entry(catalog, V6_ACTIVATION_ARTIFACT_ID)
    if (
        config_row.get("local_path") != str(paths["alias"])
        or config_row.get("sha256") != OLD_CONFIG_SHA256
        or config_row.get("bytes") != OLD_CONFIG_SIZE
        or pointer_row.get("local_path") != str(paths["pointer"])
        or pointer_row.get("sha256") != PREDECESSOR_POINTER_SHA256
        or pointer_row.get("bytes") != PREDECESSOR_POINTER_SIZE
        or activation_row.get("local_path") != activation_binding["path"]
        or activation_row.get("sha256") != activation_binding["file_sha256"]
        or activation_row.get("bytes") != activation_binding["size_bytes"]
    ):
        raise ConfigLocatorReconciliationError("frozen predecessor catalog chain drifted")
    return pointer, catalog, activation_binding


def _cleanup_execution_staging_preflight(manifest_path: Path) -> bool:
    manifest = _load_manifest_for_staging_preflight(manifest_path)
    metadata_root = Path(str(manifest["metadata_repository_root"]))
    paths = _paths(metadata_root)
    roles: tuple[tuple[Path, str, tuple[str, int] | None], ...] = (
        (paths["backtest_archive"], "create", None),
        (paths["release_v3_config"], "create", None),
        (paths["pointer_snapshot"], "create", None),
        (paths["catalog_snapshot"], "create", None),
        (paths["receipt"], "create", None),
        (paths["alias"], "alias", (OLD_CONFIG_SHA256, OLD_CONFIG_SIZE)),
        (
            paths["pointer"],
            "pointer",
            (PREDECESSOR_POINTER_SHA256, PREDECESSOR_POINTER_SIZE),
        ),
        (
            paths["catalog"],
            "catalog",
            (PREDECESSOR_CATALOG_SHA256, PREDECESSOR_CATALOG_SIZE),
        ),
    )
    descriptor = _open_transaction_lock(metadata_root)
    try:
        if _load_manifest_for_staging_preflight(manifest_path) != manifest:
            raise ConfigLocatorReconciliationError(
                "manifest changed before staging preflight cleanup"
            )
        actions: list[dict[str, Any]] = []
        for path, kind, predecessor_identity in roles:
            staging = _staging_paths(path, kind)
            if not staging:
                continue
            if predecessor_identity is None:
                if _secure_exists(path):
                    raise ConfigLocatorReconciliationError(
                        f"published create-only final forbids staging cleanup: {path}"
                    )
            else:
                predecessor_raw, _predecessor_metadata = _read_regular(path)
                if (_sha(predecessor_raw), len(predecessor_raw)) != predecessor_identity:
                    raise ConfigLocatorReconciliationError(
                        f"published or drifted mutable final forbids staging cleanup: {path}"
                    )
            pending = _pending(path, kind)
            if _secure_exists(pending):
                if len(staging) != 1:
                    raise ConfigLocatorReconciliationError(
                        f"deterministic pending has ambiguous staging: {pending}"
                    )
                pending_raw, pending_metadata = _read_regular(
                    pending,
                    allowed_nlinks=frozenset({2}),
                )
                staging_raw, staging_metadata = _read_regular(
                    staging[0],
                    allowed_nlinks=frozenset({2}),
                )
                if staging_raw != pending_raw or (
                    staging_metadata.st_dev,
                    staging_metadata.st_ino,
                ) != (pending_metadata.st_dev, pending_metadata.st_ino):
                    raise ConfigLocatorReconciliationError(
                        f"deterministic pending staging transfer drifted: {pending}"
                    )
                actions.append(
                    {
                        "kind": "transfer",
                        "pending": pending,
                        "pending_raw": pending_raw,
                        "staging": ((staging[0], staging_metadata),),
                    }
                )
                continue
            bound: list[tuple[Path, os.stat_result]] = []
            for candidate in staging:
                _candidate_raw, candidate_metadata = _read_regular(
                    candidate,
                    allowed_nlinks=frozenset({1}),
                )
                bound.append((candidate, candidate_metadata))
            actions.append(
                {
                    "kind": "orphan",
                    "pending": pending,
                    "pending_raw": None,
                    "staging": tuple(bound),
                }
            )

        for action in actions:
            for candidate, candidate_metadata in action["staging"]:
                _secure_unlink_bound(candidate, candidate_metadata)
            pending = action["pending"]
            if action["kind"] == "transfer":
                _read_exact_any_link(
                    pending,
                    action["pending_raw"],
                    frozenset({1}),
                )
            elif _secure_exists(pending):
                raise ConfigLocatorReconciliationError(
                    f"deterministic pending appeared during staging cleanup: {pending}"
                )
        for path, kind, _predecessor_identity in roles:
            if _staging_paths(path, kind):
                raise ConfigLocatorReconciliationError(f"staging reappeared during cleanup: {path}")
        if _load_manifest_for_staging_preflight(manifest_path) != manifest:
            raise ConfigLocatorReconciliationError(
                "manifest changed during staging preflight cleanup"
            )
        return bool(actions)
    finally:
        _close_transaction_lock(descriptor)


def execute(
    manifest_path: Path,
    *,
    apply: bool = False,
    audit_fn: AuditFn = _audit_fn,
    failure_hook: FailureFn | None = None,
) -> dict[str, Any]:
    if os.environ.get("NARROWGATE_LIVE_REMOTE") or os.environ.get("NARROWGATE_LIVE_REMOTE_POINTER"):
        raise ConfigLocatorReconciliationError("live resolver overrides are forbidden")
    if apply:
        _cleanup_execution_staging_preflight(manifest_path)
    return _execute_once(
        manifest_path,
        apply=apply,
        audit_fn=audit_fn,
        failure_hook=failure_hook,
    )


def _execute_once(
    manifest_path: Path,
    *,
    apply: bool,
    audit_fn: AuditFn,
    failure_hook: FailureFn | None,
) -> dict[str, Any]:
    manifest, manifest_binding = _load_manifest_for_execute(manifest_path)
    metadata_root = Path(str(manifest["metadata_repository_root"]))
    frozen_audit_baseline = manifest.get("metadata_audit_baseline")
    if not isinstance(frozen_audit_baseline, Mapping):
        raise ConfigLocatorReconciliationError("manifest audit baseline is missing")
    paths = _paths(metadata_root)
    active_config_source = _active_config_source_path()
    transaction = manifest.get("transaction")
    if not isinstance(transaction, Mapping):
        raise ConfigLocatorReconciliationError("execute transaction contract is missing")
    activation_path = _activation_path_from_transaction(transaction, metadata_root)
    if transaction != _transaction_contract(
        metadata_root,
        active_config_source,
        activation_path=activation_path,
    ):
        raise ConfigLocatorReconciliationError("execute transaction contract drifted")
    descriptor = _open_transaction_lock(metadata_root)
    try:
        active_config_raw = _validate_active_config_source(active_config_source)

        current_alias_raw, _alias_meta = _read_regular(paths["alias"])
        alias_old = (
            _sha(current_alias_raw),
            len(current_alias_raw),
        ) == (OLD_CONFIG_SHA256, OLD_CONFIG_SIZE)
        alias_new = (
            _sha(current_alias_raw),
            len(current_alias_raw),
        ) == (ACTIVE_CONFIG_SHA256, ACTIVE_CONFIG_SIZE)
        if not alias_old and not alias_new:
            raise ConfigLocatorReconciliationError(
                "stable config alias is neither predecessor nor successor"
            )
        current_pointer, current_pointer_raw, _pointer_meta = _load_json(paths["pointer"])
        current_catalog, current_catalog_raw, _catalog_meta = _load_json(paths["catalog"])

        old_config_raw = current_alias_raw if alias_old else b""
        if not alias_old:
            archive_raw, archive_state = _read_frozen_create_candidate(
                paths["backtest_archive"],
                sha256=OLD_CONFIG_SHA256,
                size_bytes=OLD_CONFIG_SIZE,
            )
            if archive_state == "missing":
                raise ConfigLocatorReconciliationError(
                    "live alias advanced before immutable backtest archive"
                )
            old_config_raw = archive_raw
        if _sha(old_config_raw) != OLD_CONFIG_SHA256 or len(old_config_raw) != OLD_CONFIG_SIZE:
            raise ConfigLocatorReconciliationError("backtest archive predecessor bytes drifted")

        pointer_old = (
            _sha(current_pointer_raw),
            len(current_pointer_raw),
        ) == (PREDECESSOR_POINTER_SHA256, PREDECESSOR_POINTER_SIZE)
        pointer_snapshot_expected = current_pointer_raw if pointer_old else b""
        if not pointer_old:
            snapshot_raw, snapshot_state = _read_frozen_create_candidate(
                paths["pointer_snapshot"],
                sha256=PREDECESSOR_POINTER_SHA256,
                size_bytes=PREDECESSOR_POINTER_SIZE,
            )
            if snapshot_state == "missing":
                raise ConfigLocatorReconciliationError(
                    "pointer advanced before immutable predecessor snapshot"
                )
            pointer_snapshot_expected = snapshot_raw
        if (
            _sha(pointer_snapshot_expected) != PREDECESSOR_POINTER_SHA256
            or len(pointer_snapshot_expected) != PREDECESSOR_POINTER_SIZE
        ):
            raise ConfigLocatorReconciliationError("predecessor pointer snapshot drifted")

        catalog_old = (
            _sha(current_catalog_raw),
            len(current_catalog_raw),
        ) == (PREDECESSOR_CATALOG_SHA256, PREDECESSOR_CATALOG_SIZE)
        catalog_snapshot_expected = current_catalog_raw if catalog_old else b""
        if not catalog_old:
            snapshot_raw, snapshot_state = _read_frozen_create_candidate(
                paths["catalog_snapshot"],
                sha256=PREDECESSOR_CATALOG_SHA256,
                size_bytes=PREDECESSOR_CATALOG_SIZE,
            )
            if snapshot_state == "missing":
                raise ConfigLocatorReconciliationError(
                    "catalog advanced before immutable predecessor snapshot"
                )
            catalog_snapshot_expected = snapshot_raw
        if (
            _sha(catalog_snapshot_expected) != PREDECESSOR_CATALOG_SHA256
            or len(catalog_snapshot_expected) != PREDECESSOR_CATALOG_SIZE
        ):
            raise ConfigLocatorReconciliationError("predecessor catalog snapshot drifted")

        predecessor_pointer, predecessor_catalog, _activation_binding = (
            _validate_frozen_predecessor_material(
                paths,
                alias_raw=old_config_raw,
                pointer_raw=pointer_snapshot_expected,
                catalog_raw=catalog_snapshot_expected,
                expected_activation_path=activation_path,
            )
        )
        immutable_bindings = _immutable_bindings(
            paths,
            old_config_raw=old_config_raw,
            active_config_raw=active_config_raw,
            old_pointer_raw=pointer_snapshot_expected,
            old_catalog_raw=catalog_snapshot_expected,
        )
        immutable_data = {
            "backtest_archive": old_config_raw,
            "release_v3_config": active_config_raw,
            "pointer_snapshot": pointer_snapshot_expected,
            "catalog_snapshot": catalog_snapshot_expected,
        }
        immutable_states = {
            key: _validate_create_state(paths[key], immutable_data[key]) for key in immutable_data
        }

        audit_baseline = frozen_audit_baseline
        audit_contract = _candidate_audit_contract(
            manifest=manifest,
            immutable_bindings=immutable_bindings,
            audit_baseline=audit_baseline,
        )
        receipt = _receipt_payload(
            manifest=manifest,
            manifest_binding=manifest_binding,
            immutable_bindings=immutable_bindings,
            audit_baseline=audit_baseline,
            candidate_audit_contract=audit_contract,
        )
        receipt_data = _render(receipt)
        receipt_state = _validate_create_state(paths["receipt"], receipt_data)
        if receipt_state not in {
            "published_nlink1",
            "published_recoverable_nlink2",
        } and (not alias_old or not pointer_old or not catalog_old):
            raise ConfigLocatorReconciliationError(
                "mutable targets advanced before reconciliation receipt"
            )
        receipt_binding = _validate_receipt(
            receipt,
            receipt_data,
            path=paths["receipt"],
            manifest=manifest,
            manifest_binding=manifest_binding,
            immutable_bindings=immutable_bindings,
        )

        planned_pointer = _pointer_payload(
            predecessor_pointer,
            receipt_binding=receipt_binding,
            immutable_bindings=immutable_bindings,
            generated_utc=manifest["generated_utc"],
            publisher_source=manifest["publisher_source"],
        )
        planned_pointer_data = _render(planned_pointer)
        pointer_new = current_pointer_raw == planned_pointer_data
        if not pointer_old and not pointer_new:
            raise ConfigLocatorReconciliationError(
                "current pointer is neither predecessor nor exact successor"
            )
        planned_catalog = _catalog_payload(
            predecessor_catalog,
            pointer_data=planned_pointer_data,
            receipt_binding=receipt_binding,
            immutable_bindings=immutable_bindings,
            generated_utc=manifest["generated_utc"],
        )
        planned_catalog_data = _render(planned_catalog)
        catalog_new = current_catalog_raw == planned_catalog_data
        if not catalog_old and not catalog_new:
            raise ConfigLocatorReconciliationError(
                "current catalog is neither predecessor nor exact successor"
            )
        if pointer_new and not alias_new:
            raise ConfigLocatorReconciliationError("pointer advanced before stable alias")
        if catalog_new and not pointer_new:
            raise ConfigLocatorReconciliationError("catalog advanced before pointer")

        pending_states = {
            "alias": _validate_replace_pending(
                paths["alias"], active_config_raw, "alias", target_new=alias_new
            ),
            "pointer": _validate_replace_pending(
                paths["pointer"], planned_pointer_data, "pointer", target_new=pointer_new
            ),
            "catalog": _validate_replace_pending(
                paths["catalog"], planned_catalog_data, "catalog", target_new=catalog_new
            ),
        }
        _validate_transaction_prefix(
            immutable_states=immutable_states,
            receipt_state=receipt_state,
            alias_new=alias_new,
            pointer_new=pointer_new,
            catalog_new=catalog_new,
            replace_pending=pending_states,
        )
        restart_states = {
            "pending_staging_transfer_nlink2",
            "staging_recoverable_uncommitted",
        }
        if apply and (
            any(state in restart_states for state in immutable_states.values())
            or receipt_state in restart_states
            or any(state in restart_states for state in pending_states.values())
        ):
            raise ConfigLocatorReconciliationError(
                "staging appeared after execution preflight cleanup"
            )
        transaction_already_completed = (
            all(state == "published_nlink1" for state in immutable_states.values())
            and receipt_state == "published_nlink1"
            and alias_new
            and pointer_new
            and catalog_new
            and all(state == "absent" for state in pending_states.values())
        )
        structural_candidate = _validate_candidate(
            predecessor_pointer=predecessor_pointer,
            predecessor_catalog=predecessor_catalog,
            receipt=receipt,
            receipt_data=receipt_data,
            pointer=planned_pointer,
            pointer_data=planned_pointer_data,
            catalog=planned_catalog,
            catalog_data=planned_catalog_data,
            immutable_bindings=immutable_bindings,
        )
        pre_candidate_observed = audit_fn(metadata_root)
        if not transaction_already_completed:
            pre_candidate_audit = _require_exact_current_audit_baseline(
                audit_baseline,
                metadata_root,
                lambda _root: pre_candidate_observed,
            )
        else:
            pre_candidate_audit = _assert_no_new_findings(
                audit_baseline,
                pre_candidate_observed,
            )
        audit_contract = receipt["prepublication_candidate_owner_root_audit_contract"]
        candidate_audit = _full_candidate_owner_root_audit(
            metadata_root=metadata_root,
            publisher_root=Path(str(manifest["publisher_root"])),
            publisher_source=manifest["publisher_source"],
            audit_baseline=audit_baseline,
            audit_contract=audit_contract,
            planned_files={
                "backtest_v12_archive": (paths["backtest_archive"], old_config_raw),
                "release_v3_versioned_config": (
                    paths["release_v3_config"],
                    active_config_raw,
                ),
                "predecessor_pointer_snapshot": (
                    paths["pointer_snapshot"],
                    pointer_snapshot_expected,
                ),
                "predecessor_catalog_snapshot": (
                    paths["catalog_snapshot"],
                    catalog_snapshot_expected,
                ),
                "reconciliation_receipt": (paths["receipt"], receipt_data),
                "stable_live_config_alias": (paths["alias"], active_config_raw),
                "current_remote_pointer": (paths["pointer"], planned_pointer_data),
                "current_catalog": (paths["catalog"], planned_catalog_data),
            },
        )
        # The detached candidate audit can be slow. Re-audit the real owner
        # root and then rebind every immutable/source authority before a dry
        # run is accepted or the first official byte is written.
        if not transaction_already_completed:
            prewrite_audit = _require_exact_current_audit_baseline(
                audit_baseline,
                metadata_root,
                audit_fn,
            )
        else:
            prewrite_audit = _assert_no_new_findings(
                audit_baseline,
                audit_fn(metadata_root),
            )
        prewrite_authorities = _validate_execution_immutable_authorities(
            manifest_path=manifest_path,
            expected_manifest=manifest,
            expected_manifest_binding=manifest_binding,
            activation_path=activation_path,
        )
        _validate_execution_source_identity(manifest)
        result: dict[str, Any] = {
            "schema_version": "f05_buy_e3_config_locator_reconciliation_execution.v1",
            "status": "planned_or_resumed_exact_transaction",
            "mode": "apply" if apply else "dry_run",
            "writes_performed": False,
            "transaction_committed": False,
            "state_before": {
                "immutable": immutable_states,
                "receipt": receipt_state,
                "stable_alias": "successor" if alias_new else "predecessor",
                "pointer": "successor" if pointer_new else "predecessor",
                "catalog": "successor" if catalog_new else "predecessor",
                "pending": pending_states,
            },
            "ordered_steps": manifest["transaction"]["ordered_publication"],
            "manifest": manifest_binding,
            "receipt": receipt_binding,
            "structural_candidate_validation": structural_candidate,
            "prepublication_candidate_audit": candidate_audit,
            "pre_candidate_metadata_audit": pre_candidate_audit,
            "prewrite_metadata_audit": prewrite_audit,
            "prewrite_immutable_authorities": prewrite_authorities,
            "immutable_v6_activation_receipt_preserved": True,
            "backtest_default_config_sha256": OLD_CONFIG_SHA256,
            "current_live_config_sha256": ACTIVE_CONFIG_SHA256,
            "current_live_alias_used_for_backtest": False,
            "permissions": deepcopy(NO_NEW_AUTHORITY),
        }
        if not apply:
            return result

        # Keep the final byte-authority recheck adjacent to the first write.
        _validate_execution_immutable_authorities(
            manifest_path=manifest_path,
            expected_manifest=manifest,
            expected_manifest_binding=manifest_binding,
            activation_path=activation_path,
        )
        _validate_execution_source_identity(manifest)

        for key in (
            "backtest_archive",
            "release_v3_config",
            "pointer_snapshot",
            "catalog_snapshot",
        ):
            _publish_create_only(paths[key], immutable_data[key])
            if failure_hook is not None:
                failure_hook(key)
        _publish_create_only(paths["receipt"], receipt_data)
        if failure_hook is not None:
            failure_hook("receipt")
        if not alias_new:
            _atomic_replace(
                paths["alias"],
                active_config_raw,
                kind="alias",
                expected_current=old_config_raw,
            )
        if failure_hook is not None:
            failure_hook("alias")
        if not pointer_new:
            _atomic_replace(
                paths["pointer"],
                planned_pointer_data,
                kind="pointer",
                expected_current=pointer_snapshot_expected,
            )
        if failure_hook is not None:
            failure_hook("pointer")
        if not catalog_new:
            _atomic_replace(
                paths["catalog"],
                planned_catalog_data,
                kind="catalog",
                expected_current=catalog_snapshot_expected,
            )
        if failure_hook is not None:
            failure_hook("catalog")

        for key, data in (*immutable_data.items(), ("receipt", receipt_data)):
            _read_exact_any_link(paths[key], data, frozenset({1}))
        _read_exact_any_link(paths["alias"], active_config_raw, frozenset({1}))
        _read_exact_any_link(paths["pointer"], planned_pointer_data, frozenset({1}))
        _read_exact_any_link(paths["catalog"], planned_catalog_data, frozenset({1}))
        final_pointer, final_pointer_raw, _final_pointer_meta = _load_json(paths["pointer"])
        final_catalog, final_catalog_raw, _final_catalog_meta = _load_json(paths["catalog"])
        _validate_candidate(
            predecessor_pointer=predecessor_pointer,
            predecessor_catalog=predecessor_catalog,
            receipt=receipt,
            receipt_data=receipt_data,
            pointer=final_pointer,
            pointer_data=final_pointer_raw,
            catalog=final_catalog,
            catalog_data=final_catalog_raw,
            immutable_bindings=immutable_bindings,
        )
        try:
            post_immutable_authorities = _validate_execution_immutable_authorities(
                manifest_path=manifest_path,
                expected_manifest=manifest,
                expected_manifest_binding=manifest_binding,
                activation_path=activation_path,
            )
            _validate_execution_source_identity(manifest)
        except Exception as exc:
            post_source_identity = {
                "passed": False,
                "diagnostic_error_type": type(exc).__name__,
                "diagnostic_error_message": str(exc),
            }
        else:
            post_source_identity = {
                "passed": True,
                "immutable_authorities": post_immutable_authorities,
            }
        try:
            post_audit = _compare_audit_findings(audit_baseline, audit_fn(metadata_root))
        except Exception as exc:
            post_audit = {
                "passed": None,
                "new_finding_count": None,
                "new_findings": [],
                "diagnostic_error_type": type(exc).__name__,
                "diagnostic_error_message": str(exc),
            }
        result["writes_performed"] = True
        result["transaction_committed"] = True
        result["post_write_verified"] = True
        result["post_write_source_identity"] = post_source_identity
        result["metadata_audit"] = post_audit
        if post_source_identity["passed"] is not True:
            result["status"] = (
                "committed_exact_transaction_with_post_source_identity_diagnostic_error"
            )
            result["post_source_identity_drift_detected"] = True
            result["post_source_identity_drift_attribution"] = "unattributed_after_commit"
            result["post_audit_drift_detected"] = post_audit.get("passed") is False
            result["post_audit_drift_attribution"] = (
                "unattributed_after_commit" if post_audit.get("passed") is not True else "none"
            )
        elif post_audit.get("passed") is True:
            result["status"] = "completed_exact_transaction"
            result["post_source_identity_drift_detected"] = False
            result["post_source_identity_drift_attribution"] = "none"
            result["post_audit_drift_detected"] = False
            result["post_audit_drift_attribution"] = "none"
        elif post_audit.get("passed") is False:
            result["status"] = "committed_exact_transaction_with_unattributed_post_audit_drift"
            result["post_source_identity_drift_detected"] = False
            result["post_source_identity_drift_attribution"] = "none"
            result["post_audit_drift_detected"] = True
            result["post_audit_drift_attribution"] = "unattributed_after_commit"
        else:
            result["status"] = "committed_exact_transaction_with_post_audit_diagnostic_error"
            result["post_source_identity_drift_detected"] = False
            result["post_source_identity_drift_attribution"] = "none"
            result["post_audit_drift_detected"] = None
            result["post_audit_drift_attribution"] = "unattributed_after_commit"
        result["state_after"] = {
            "immutable": "published_nlink1",
            "receipt": "published_nlink1",
            "stable_alias": "successor",
            "pointer": "successor",
            "catalog": "successor",
        }
        if post_source_identity["passed"] is not True or post_audit.get("passed") is not True:
            raise CommittedPostAuditError(result)
        return result
    finally:
        _close_transaction_lock(descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare-manifest")
    prepare.add_argument("--publisher-root", type=Path, required=True)
    prepare.add_argument("--metadata-repository-root", type=Path, required=True)
    prepare.add_argument("--active-config-source", type=Path, required=True)
    prepare.add_argument("--receipt-id", required=True)
    prepare.add_argument("--output", type=Path)
    validate = commands.add_parser("validate-manifest")
    validate.add_argument("--manifest", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "prepare-manifest":
            result = prepare_manifest(
                publisher_root=args.publisher_root,
                metadata_repository_root=args.metadata_repository_root,
                active_config_source=args.active_config_source,
                receipt_id=args.receipt_id,
                output_path=args.output,
            )
        elif args.command == "validate-manifest":
            result = validate_manifest(args.manifest)
        else:
            result = execute(args.manifest, apply=args.apply)
    except CommittedPostAuditError as exc:
        print(json.dumps(exc.result, indent=2, sort_keys=True))
        return 3
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
