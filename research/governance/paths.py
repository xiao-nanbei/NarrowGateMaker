"""Resolve frozen research paths across versioned hard-layout migrations.

Active code imports the canonical ``research`` package directly. Historical
specifications may retain either a pre-family path or a root ``research_*``
path; those paths are resolved through the immutable layout manifests without
recreating import aliases, duplicate source files, or symlinks.
"""

from __future__ import annotations

import hashlib
import json
import tarfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from data_paths import relocate_marketdata_path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
LAYOUT_V1_MANIFEST = REPOSITORY_ROOT / "research/governance/migrations/layout_v1.json"
LAYOUT_V2_MANIFEST = REPOSITORY_ROOT / "research/governance/migrations/layout_v2.json"
MIGRATION_MANIFESTS = (LAYOUT_V1_MANIFEST, LAYOUT_V2_MANIFEST)
SUPPORTED_SCHEMAS = {
    "narrowgate.research_path_migration.v1",
    "narrowgate.research_layout_migration.v2",
}
PRIVATE_LEGACY_ARCHIVE_ROOT = (
    REPOSITORY_ROOT
    / "research/shared/experiment_governance/private/legacy_snapshots"
)
REPOSITORY_NAME_ALIASES = frozenset(
    {
        REPOSITORY_ROOT.name,
        "NarrowGate_BTCUSDC",
        "NarrowGateMaker",
    }
)


@dataclass(frozen=True)
class PrivateLegacyArchiveIdentity:
    artifact_id: str
    logical_path: str
    filename: str
    sha256: str
    size_bytes: int
    availability: str = "private_not_distributed"


PRIVATE_LEGACY_ARCHIVES = {
    "research/governance/archive/legacy_snapshot_v1.tar.gz": PrivateLegacyArchiveIdentity(
        artifact_id="research-layout-legacy-snapshot-v1",
        logical_path="research/governance/archive/legacy_snapshot_v1.tar.gz",
        filename="legacy_snapshot_v1.tar.gz",
        sha256="6358405985e3f9bad937b0560f7fb8ed3724db528ef220f577ddc056773a7aaa",
        size_bytes=937843,
    ),
    "research/governance/archive/legacy_snapshot_v2.tar.gz": PrivateLegacyArchiveIdentity(
        artifact_id="research-layout-legacy-snapshot-v2",
        logical_path="research/governance/archive/legacy_snapshot_v2.tar.gz",
        filename="legacy_snapshot_v2.tar.gz",
        sha256="f2444e06a09c69d26eabcd75b4687982a76ed1a6189c38b44c30493846f2e034",
        size_bytes=1933702,
    ),
}


@dataclass(frozen=True)
class ResearchPathIdentity:
    migration_schema: str
    legacy_path: str
    canonical_path: str
    archived_sha256: str
    archive_member: str
    archive_path: str
    canonical_availability: str


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@lru_cache(maxsize=1)
def _manifests() -> tuple[dict[str, Any], ...]:
    manifests: list[dict[str, Any]] = []
    for path in MIGRATION_MANIFESTS:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema") not in SUPPORTED_SCHEMAS:
            raise RuntimeError(f"unsupported research path migration manifest: {path}")
        manifests.append(payload)
    return tuple(manifests)


@lru_cache(maxsize=1)
def _mapping() -> dict[str, ResearchPathIdentity]:
    identities: dict[str, ResearchPathIdentity] = {}
    for manifest in _manifests():
        schema = str(manifest["schema"])
        archive_path = str(manifest["legacy_snapshot"]["path"])
        for row in manifest["mappings"]:
            identity = ResearchPathIdentity(
                migration_schema=schema,
                legacy_path=str(row["legacy_path"]),
                canonical_path=str(row["canonical_path"]),
                archived_sha256=str(row["sha256"]),
                archive_member=str(row["archive_member"]),
                archive_path=archive_path,
                canonical_availability=str(
                    row.get("canonical_availability", "public_repository")
                ),
            )
            if identity.legacy_path in identities:
                raise RuntimeError(f"duplicate research legacy path: {identity.legacy_path}")
            identities[identity.legacy_path] = identity
    return identities


def _repository_relative(path: str | Path) -> str | None:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        try:
            return candidate.resolve(strict=False).relative_to(REPOSITORY_ROOT).as_posix()
        except ValueError:
            parts = candidate.parts
            for index in range(len(parts) - 1, -1, -1):
                if parts[index] not in REPOSITORY_NAME_ALIASES:
                    continue
                suffix = parts[index + 1 :]
                if suffix and ".." not in suffix:
                    return Path(*suffix).as_posix()
            return None
    relative = candidate.as_posix()
    while relative.startswith("./"):
        relative = relative[2:]
    return relative


def migration_identity(path: str | Path) -> ResearchPathIdentity | None:
    relative = _repository_relative(path)
    if relative is None:
        return None
    return _mapping().get(relative)


def _identity_chain(path: str | Path) -> tuple[ResearchPathIdentity, ...]:
    relative = _repository_relative(path)
    if relative is None:
        return ()
    identities: list[ResearchPathIdentity] = []
    visited: set[str] = set()
    while relative in _mapping():
        if relative in visited:
            raise RuntimeError(f"cyclic research path migration: {relative}")
        visited.add(relative)
        identity = _mapping()[relative]
        identities.append(identity)
        relative = identity.canonical_path
    return tuple(identities)


def _final_relative(path: str | Path) -> str | None:
    relative = _repository_relative(path)
    if relative is None:
        return None
    for identity in _identity_chain(relative):
        relative = identity.canonical_path
    return relative


def _private_canonical_identity(path: str | Path) -> ResearchPathIdentity | None:
    """Return a mapping whose retired canonical bytes remain private."""

    final_relative = _final_relative(path)
    if final_relative is None:
        return None
    matches = [
        identity
        for identity in _mapping().values()
        if identity.canonical_path == final_relative
        and identity.canonical_availability == "private_not_distributed"
    ]
    if len(matches) > 1:
        raise RuntimeError(f"ambiguous private canonical research path: {final_relative}")
    return matches[0] if matches else None


def private_legacy_archive_identity(
    path: str | Path,
) -> PrivateLegacyArchiveIdentity | None:
    """Return the private artifact identity for a frozen archive locator."""

    final_relative = _final_relative(path)
    if final_relative is None:
        return None
    return PRIVATE_LEGACY_ARCHIVES.get(final_relative)


def resolve_private_legacy_archive(
    path: str | Path,
    *,
    require_exists: bool = True,
) -> Path:
    """Resolve and verify an exact legacy archive retained outside public Git."""

    identity = private_legacy_archive_identity(path)
    if identity is None:
        raise KeyError(f"unknown private legacy archive: {path}")
    candidate = PRIVATE_LEGACY_ARCHIVE_ROOT / identity.filename
    if not candidate.is_file():
        if require_exists:
            raise FileNotFoundError(
                f"{identity.artifact_id} is required for historical reproduction; "
                f"availability={identity.availability}"
            )
        return candidate.resolve(strict=False)
    actual_size = candidate.stat().st_size
    if actual_size != identity.size_bytes:
        raise RuntimeError(
            f"private legacy archive size mismatch for {identity.artifact_id}: "
            f"expected={identity.size_bytes} actual={actual_size}"
        )
    actual_sha256 = file_sha256(candidate)
    if actual_sha256 != identity.sha256:
        raise RuntimeError(
            f"private legacy archive hash mismatch for {identity.artifact_id}: "
            f"expected={identity.sha256} actual={actual_sha256}"
        )
    return candidate.resolve()


def resolve_research_path(path: str | Path, *, require_exists: bool = True) -> Path:
    """Resolve any registered historical path to the current research layout."""

    if private_legacy_archive_identity(path) is not None:
        return resolve_private_legacy_archive(path, require_exists=require_exists)

    private_canonical = _private_canonical_identity(path)
    if private_canonical is not None:
        candidate = REPOSITORY_ROOT / private_canonical.canonical_path
        if require_exists:
            raise FileNotFoundError(
                f"{private_canonical.canonical_path} is retained only in private "
                f"historical evidence; availability={private_canonical.canonical_availability}; "
                "use archived_bytes() with the frozen identity"
            )
        return candidate.resolve(strict=False)

    candidate = relocate_marketdata_path(path)
    if not candidate.is_absolute():
        candidate = REPOSITORY_ROOT / candidate
    if candidate.exists():
        return candidate.resolve()

    # A public placeholder has already been expanded by
    # ``relocate_marketdata_path``.  Do not reinterpret its original literal
    # (for example ``${NARROWGATE_DATA_ROOT}/...``) as a repository-relative
    # migration key when the target is intentionally not materialized yet.
    if "${" in str(path):
        if require_exists:
            raise FileNotFoundError(candidate)
        return candidate.resolve(strict=False)

    final_relative = _final_relative(path)
    if final_relative is not None:
        canonical = REPOSITORY_ROOT / final_relative
        if canonical.exists() or not require_exists:
            return canonical.resolve(strict=False)

    if require_exists:
        raise FileNotFoundError(candidate)
    return candidate.resolve(strict=False)


def _archive_for(identity: ResearchPathIdentity) -> Path:
    return resolve_private_legacy_archive(identity.archive_path)


def archived_bytes(path: str | Path, expected_sha256: str | None = None) -> bytes:
    """Return exact bytes from the migration boundary matching ``path``."""

    identities = _identity_chain(path)
    if not identities:
        private_canonical = _private_canonical_identity(path)
        if private_canonical is not None:
            identities = (private_canonical,)
    if not identities:
        raise KeyError(f"path is not part of a research migration: {path}")
    expected = str(expected_sha256) if expected_sha256 is not None else None
    identity = next(
        (row for row in identities if expected is None or row.archived_sha256 == expected),
        None,
    )
    if identity is None:
        known = ", ".join(row.archived_sha256 for row in identities)
        raise RuntimeError(f"archived identity mismatch for {path}: known={known} expected={expected}")

    with tarfile.open(_archive_for(identity), "r:gz") as handle:
        member = handle.extractfile(identity.archive_member)
        if member is None:
            raise RuntimeError(f"archive member is missing: {identity.archive_member}")
        payload = member.read()
    actual = hashlib.sha256(payload).hexdigest()
    if actual != identity.archived_sha256:
        raise RuntimeError(
            f"archived payload changed for {identity.legacy_path}: "
            f"expected={identity.archived_sha256} actual={actual}"
        )
    return payload


def verify_path_identity(path: str | Path, expected_sha256: str) -> Path:
    """Verify current bytes and fail explicitly for a frozen archived identity."""

    resolved = resolve_research_path(path)
    actual = file_sha256(resolved)
    expected = str(expected_sha256)
    if actual == expected:
        return resolved

    identity = next(
        (row for row in _identity_chain(path) if row.archived_sha256 == expected),
        None,
    )
    if identity is not None:
        archived_bytes(path, expected)
        archive_identity = private_legacy_archive_identity(identity.archive_path)
        artifact_id = (
            archive_identity.artifact_id
            if archive_identity is not None
            else "unregistered-private-legacy-archive"
        )
        raise RuntimeError(
            f"{identity.legacy_path} belongs to a frozen pre-migration code identity. "
            f"Its exact bytes are retained as private artifact {artifact_id}; rerun "
            "under a new experiment identity with the canonical source path."
        )
    raise RuntimeError(f"identity changed for {path}: expected={expected} actual={actual}")
