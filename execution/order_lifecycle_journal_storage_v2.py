"""Storage-profile contract for prospective lifecycle journal-v2 collection.

The live producer and local replay/admission paths have different authority.
This module keeps that distinction explicit and validates every configured
path before either profile may be enabled.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

LOCAL_ORICO_REPLAY_ADMISSION = "local_orico_replay_admission"
BOUNDED_REMOTE_SPOOL = "bounded_remote_spool"
LIFECYCLE_JOURNAL_V2_STORAGE_PROFILES = frozenset(
    {LOCAL_ORICO_REPLAY_ADMISSION, BOUNDED_REMOTE_SPOOL}
)

_REMOTE_FORBIDDEN_EXACT = frozenset(
    {
        Path("/"),
        Path("/home"),
        Path("/root"),
        Path("/mnt"),
        Path("/opt"),
    }
)
_REMOTE_FORBIDDEN_TREES = (
    Path("/bin"),
    Path("/boot"),
    Path("/dev"),
    Path("/etc"),
    Path("/private"),
    Path("/proc"),
    Path("/sbin"),
    Path("/sys"),
    Path("/System"),
    Path("/tmp"),
    Path("/usr"),
    Path("/var"),
    Path("/Volumes"),
)


@dataclass(frozen=True, slots=True)
class LifecycleJournalStorageResolution:
    profile: str
    journal_root: Path
    prospective_epoch_root: Path
    allowlisted_root: Path
    local_admission_authorized: bool
    remote_spool_only: bool


def _absolute_resolved(value: str | Path, *, field_name: str) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        raise ValueError(f"lifecycle_journal_v2.{field_name} must be absolute")
    return candidate.resolve()


def _absolute_normalized(value: str | Path, *, field_name: str) -> Path:
    """Normalize a path for another host without resolving local symlinks."""

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        raise ValueError(f"lifecycle_journal_v2.{field_name} must be absolute")
    return Path(os.path.normpath(str(candidate)))


def _is_within(path: Path, parent: Path, *, allow_equal: bool = False) -> bool:
    return (allow_equal and path == parent) or parent in path.parents


def _assert_remote_allowlist_root(path: Path) -> None:
    if path in _REMOTE_FORBIDDEN_EXACT:
        raise ValueError("remote spool allowlisted root is too broad or sensitive")
    if any(_is_within(path, forbidden, allow_equal=True) for forbidden in _REMOTE_FORBIDDEN_TREES):
        raise ValueError("remote spool allowlisted root is inside a sensitive directory")
    if len(path.parts) < 4:
        raise ValueError("remote spool allowlisted root is too broad or sensitive")


def _normalize_remote_allowlist(values: Iterable[str | Path]) -> tuple[Path, ...]:
    roots = tuple(
        _absolute_normalized(value, field_name="remote_spool_allowlisted_roots")
        for value in values
    )
    if not roots:
        raise ValueError("bounded_remote_spool requires at least one allowlisted root")
    if len(set(roots)) != len(roots):
        raise ValueError("remote spool allowlisted roots must be unique")
    for root in roots:
        _assert_remote_allowlist_root(root)
    return roots


def validate_lifecycle_journal_storage(
    *,
    profile: str,
    journal_root: str | Path,
    prospective_epoch_root: str | Path,
    required_mount: str | Path,
    remote_spool_allowlisted_roots: Iterable[str | Path],
    enabled: bool,
) -> LifecycleJournalStorageResolution:
    """Validate and resolve one of the two journal-v2 storage profiles."""

    normalized_profile = str(profile).strip()
    if normalized_profile not in LIFECYCLE_JOURNAL_V2_STORAGE_PROFILES:
        raise ValueError(
            "lifecycle_journal_v2.storage_profile must be "
            "local_orico_replay_admission or bounded_remote_spool"
        )
    path_normalizer = (
        _absolute_normalized
        if normalized_profile == BOUNDED_REMOTE_SPOOL
        else _absolute_resolved
    )
    root = path_normalizer(journal_root, field_name="root")
    epoch_root = path_normalizer(
        prospective_epoch_root, field_name="prospective_epoch_root"
    )
    if root == epoch_root or root in epoch_root.parents or epoch_root in root.parents:
        raise ValueError(
            "lifecycle journal and prospective epoch roots must be distinct siblings"
        )

    if normalized_profile == LOCAL_ORICO_REPLAY_ADMISSION:
        mount = _absolute_resolved(required_mount, field_name="required_mount")
        if mount == Path(mount.anchor):
            raise ValueError("local replay admission requires a dedicated storage mount")
        for field_name, value in (("root", root), ("prospective_epoch_root", epoch_root)):
            if not _is_within(value, mount):
                raise ValueError(
                    f"lifecycle_journal_v2.{field_name} must be inside required_mount"
                )
        if enabled and (not mount.exists() or not os.path.ismount(mount)):
            raise ValueError("enabled lifecycle_journal_v2 requires its storage mount")
        return LifecycleJournalStorageResolution(
            profile=normalized_profile,
            journal_root=root,
            prospective_epoch_root=epoch_root,
            allowlisted_root=mount,
            local_admission_authorized=True,
            remote_spool_only=False,
        )

    allowlisted_roots = _normalize_remote_allowlist(remote_spool_allowlisted_roots)
    journal_parent = next(
        (candidate for candidate in allowlisted_roots if _is_within(root, candidate)),
        None,
    )
    epoch_parent = next(
        (candidate for candidate in allowlisted_roots if _is_within(epoch_root, candidate)),
        None,
    )
    if journal_parent is None:
        raise ValueError(
            "lifecycle_journal_v2.root must be a strict child of an allowlisted remote spool root"
        )
    if epoch_parent is None:
        raise ValueError(
            "lifecycle_journal_v2.prospective_epoch_root must be a strict child of an "
            "allowlisted remote spool root"
        )
    if journal_parent != epoch_parent:
        raise ValueError("journal and epoch roots must share one remote spool allowlist root")
    if enabled:
        if (
            not journal_parent.exists()
            or not journal_parent.is_dir()
            or journal_parent.is_symlink()
        ):
            raise ValueError(
                "enabled bounded_remote_spool requires an existing non-symlink "
                "allowlisted root"
            )
    return LifecycleJournalStorageResolution(
        profile=normalized_profile,
        journal_root=root,
        prospective_epoch_root=epoch_root,
        allowlisted_root=journal_parent,
        local_admission_authorized=False,
        remote_spool_only=True,
    )


def validate_remote_spool_path(
    path: str | Path,
    *,
    allowlisted_roots: Iterable[str | Path],
    field_name: str,
) -> tuple[Path, Path]:
    """Validate one absolute bounded-spool path for collector tooling."""

    resolved = _absolute_normalized(path, field_name=field_name)
    roots = _normalize_remote_allowlist(allowlisted_roots)
    parent = next((root for root in roots if _is_within(resolved, root)), None)
    if parent is None:
        raise ValueError(f"{field_name} is outside the remote spool allowlist")
    return resolved, parent
