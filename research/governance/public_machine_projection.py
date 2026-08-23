"""Resolve the two identities of a redacted public machine document.

Public JSON/CSV projections have their own byte hash.  The exact historical
source bytes keep a separate hash and, on an owner's machine, live only below
an ignored private evidence directory.  This module verifies both identities
without treating the source hash as the hash of the public file.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECTION_MANIFEST_PATHS = (
    Path("research/public_machine_document_projections.json"),
    Path("docs/public_machine_document_projections.json"),
)
NONPUBLISHED_PROJECTION_INDEX_PATH = Path(
    "models/private/nonpublished_machine_document_projections.current.local.json"
)
PROJECTION_SCHEMA = "narrowgate_public_machine_document_projections_v1"
NONPUBLISHED_PROJECTION_SCHEMA = "narrowgate_nonpublished_machine_document_projections_v1"
NONPUBLISHED_AVAILABILITY = "private_working_tree_projection_not_distributed"


class PublicMachineProjectionError(RuntimeError):
    """Raised when either side of a public/private projection binding drifts."""


def sha256_file(path: Path) -> str:
    """Return the SHA256 of one file."""

    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class PublicMachineProjection:
    """Verified metadata for one public projection and its historical source."""

    public_path: Path
    public_relative_path: str
    manifest_path: Path
    unit_id: str
    public_projection_sha256: str
    source_private_sha256: str
    private_source_path: Path
    materialized_identity: str

    @property
    def private_source_available(self) -> bool:
        return self.private_source_path.is_file()

    def require_private_source(self) -> Path:
        """Return the exact source only after verifying its registered hash."""

        if not self.private_source_available:
            raise PublicMachineProjectionError(
                "private evidence store; not distributed with the public repository: "
                f"{self.public_relative_path}"
            )
        observed = sha256_file(self.private_source_path)
        if observed != self.source_private_sha256:
            raise PublicMachineProjectionError(
                "private source SHA256 mismatch for "
                f"{self.public_relative_path}: expected {self.source_private_sha256}, "
                f"observed {observed}"
            )
        return self.private_source_path


def _private_source_path(entry: dict[str, Any]) -> Path:
    public_relative = Path(str(entry["public_path"]))
    unit_id = str(entry["unit_id"])
    if unit_id == "repository":
        try:
            within_unit = public_relative.relative_to("docs")
        except ValueError as exc:
            raise PublicMachineProjectionError(
                f"repository projection is outside docs/: {public_relative}"
            ) from exc
        return PROJECT_ROOT / "docs/private/original_public_machine_records" / within_unit

    unit = Path(unit_id)
    try:
        within_unit = public_relative.relative_to(unit)
    except ValueError:
        return (
            PROJECT_ROOT
            / unit
            / "private/original_public_machine_records/cross_unit"
            / public_relative
        )
    return PROJECT_ROOT / unit / "private/original_public_machine_records" / within_unit


def _projection_entries() -> dict[str, tuple[Path, dict[str, Any]]]:
    entries: dict[str, tuple[Path, dict[str, Any]]] = {}
    manifest_contracts = [
        *((relative, PROJECTION_SCHEMA) for relative in PROJECTION_MANIFEST_PATHS),
        (NONPUBLISHED_PROJECTION_INDEX_PATH, NONPUBLISHED_PROJECTION_SCHEMA),
    ]
    for relative_manifest, expected_schema in manifest_contracts:
        manifest_path = PROJECT_ROOT / relative_manifest
        if not manifest_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != expected_schema:
            raise PublicMachineProjectionError(
                f"unsupported projection manifest schema: {relative_manifest}"
            )
        for raw_entry in manifest.get("entries", []):
            entry = dict(raw_entry)
            public_relative = str(entry.get("public_path", ""))
            if not public_relative or public_relative in entries:
                raise PublicMachineProjectionError(
                    f"missing or duplicate public projection path: {public_relative!r}"
                )
            entries[public_relative] = (manifest_path, entry)
    return entries


def projection_for(
    path: str | Path,
    *,
    verify_private_if_available: bool = True,
) -> PublicMachineProjection | None:
    """Verify and return projection metadata, or ``None`` for a normal file."""

    public_path = Path(path).expanduser().resolve()
    try:
        public_relative = public_path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return None
    registered = _projection_entries().get(public_relative)
    if registered is None:
        return None
    manifest_path, entry = registered
    if not public_path.is_file():
        raise PublicMachineProjectionError(f"public projection is missing: {public_relative}")
    expected_public = str(entry["public_projection_sha256"])
    expected_source = str(entry["source_private_sha256"])
    observed_public = sha256_file(public_path)
    is_nonpublished = manifest_path == PROJECT_ROOT / NONPUBLISHED_PROJECTION_INDEX_PATH
    source_materialized_in_place = (
        is_nonpublished
        and entry.get("availability") == NONPUBLISHED_AVAILABILITY
        and observed_public == expected_source
    )
    if observed_public != expected_public and not source_materialized_in_place:
        raise PublicMachineProjectionError(
            f"public projection SHA256 mismatch for {public_relative}: "
            f"expected {expected_public}, observed {observed_public}"
        )
    private_source_path = (
        public_path if source_materialized_in_place else _private_source_path(entry)
    )
    projection = PublicMachineProjection(
        public_path=public_path,
        public_relative_path=public_relative,
        manifest_path=manifest_path,
        unit_id=str(entry["unit_id"]),
        public_projection_sha256=expected_public,
        source_private_sha256=expected_source,
        private_source_path=private_source_path,
        materialized_identity=(
            "private_source" if source_materialized_in_place else "public_projection"
        ),
    )
    if verify_private_if_available and projection.private_source_available:
        projection.require_private_source()
    return projection


def source_identity_sha256(path: str | Path) -> str:
    """Return the executed-source hash while also verifying public bytes.

    For an ordinary file this is its direct byte hash.  For a registered
    projection this is the source hash recorded in the projection manifest;
    the current public projection hash is verified first, and owner-side exact
    source bytes are additionally verified whenever they are available.
    """

    resolved = Path(path).expanduser().resolve()
    projection = projection_for(resolved)
    if projection is None:
        return sha256_file(resolved)
    return projection.source_private_sha256


def source_document_path(path: str | Path, *, require_private: bool) -> Path:
    """Resolve a document to exact historical bytes when they are available."""

    resolved = Path(path).expanduser().resolve()
    projection = projection_for(resolved)
    if projection is None:
        return resolved
    if projection.private_source_available or require_private:
        return projection.require_private_source()
    return resolved
