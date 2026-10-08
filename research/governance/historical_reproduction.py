"""Fail-closed execution contract for closed historical research runners."""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from research.governance.paths import (
    private_legacy_archive_identity,
    resolve_private_legacy_archive,
)
from research.governance.public_machine_projection import (
    projection_for,
    source_document_path,
    source_identity_sha256,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = Path(__file__).with_name("historical_reproduction_registry.v1.json")
REGISTRY_SCHEMA = "narrowgate_historical_reproduction_registry.v1"
FROZEN_SOURCE_ARCHIVE_LOGICAL_PATH = (
    "research/governance/archive/legacy_snapshot_v1.tar.gz"
)


class HistoricalReproductionError(RuntimeError):
    """Raised when a closed runner is asked to create unauthorised evidence."""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _frozen_source_archive() -> Path:
    try:
        return resolve_private_legacy_archive(FROZEN_SOURCE_ARCHIVE_LOGICAL_PATH)
    except (FileNotFoundError, RuntimeError) as exc:
        raise HistoricalReproductionError(
            "exact frozen source archive is unavailable or invalid; "
            "availability=private_not_distributed"
        ) from exc


def verify_frozen_source_identity(
    relative_path: str | Path,
    expected_sha256: str,
) -> dict[str, str]:
    """Verify frozen source bytes in-tree or in the immutable v1 archive."""
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise HistoricalReproductionError(f"invalid frozen source path: {relative}")
    current = (PROJECT_ROOT / relative).resolve()
    if current.is_file() and _sha256(current) == expected_sha256:
        return {"source": "working_tree", "path": str(relative), "sha256": expected_sha256}

    member_name = f"legacy/{relative.as_posix()}"
    archive_path = _frozen_source_archive()
    with tarfile.open(archive_path, "r:gz") as archive:
        try:
            member = archive.getmember(member_name)
        except KeyError as exc:
            raise HistoricalReproductionError(
                f"frozen source is absent from archive: {relative}"
            ) from exc
        handle = archive.extractfile(member)
        if handle is None:
            raise HistoricalReproductionError(f"frozen source is not a file: {relative}")
        actual_sha256 = hashlib.sha256(handle.read()).hexdigest()
    if actual_sha256 != expected_sha256:
        raise HistoricalReproductionError(
            f"frozen source hash mismatch for {relative}: expected {expected_sha256}, "
            f"got {actual_sha256}"
        )
    archive_identity = private_legacy_archive_identity(FROZEN_SOURCE_ARCHIVE_LOGICAL_PATH)
    if archive_identity is None:
        raise HistoricalReproductionError("frozen source archive identity is unregistered")
    return {
        "source": "legacy_snapshot_v1",
        "path": member_name,
        "sha256": actual_sha256,
        "archive_artifact_id": archive_identity.artifact_id,
        "source_availability": archive_identity.availability,
    }


def _registry() -> Mapping[str, Any]:
    payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    if payload.get("schema_version") != REGISTRY_SCHEMA:
        raise HistoricalReproductionError("historical reproduction registry schema drift")
    return payload


def add_historical_reproduction_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--historical-reproduction",
        action="store_true",
        help="Reproduce an exact frozen historical spec; never creates new authority.",
    )


def require_historical_reproduction(
    *,
    runner_id: str,
    enabled: bool,
    spec_path: str | Path | None,
) -> dict[str, Any]:
    """Authorize only a repository-owned spec whose bytes match the registry."""
    if not enabled:
        raise HistoricalReproductionError(
            f"{runner_id} is closed; pass --historical-reproduction with an exact "
            "frozen spec/hash"
        )
    entry = (_registry().get("runners") or {}).get(runner_id)
    if not isinstance(entry, dict):
        raise HistoricalReproductionError(f"unregistered historical runner: {runner_id}")
    if not bool(entry.get("supported", False)):
        reason = str(entry.get("reason") or "unsupported")
        raise HistoricalReproductionError(
            f"{runner_id} cannot run: {reason}; source is retained read-only"
        )
    if spec_path is None:
        raise HistoricalReproductionError(f"{runner_id} requires a frozen spec path")

    actual_path = Path(spec_path).expanduser().resolve()
    allowed = {
        (PROJECT_ROOT / str(item["path"])).resolve(): str(item["sha256"])
        for item in entry.get("specs") or []
    }
    if actual_path not in allowed:
        raise HistoricalReproductionError(
            f"{runner_id} spec path is not the registered frozen file: {actual_path}"
        )
    actual_sha256 = source_identity_sha256(actual_path)
    expected_sha256 = allowed[actual_path]
    if actual_sha256 != expected_sha256:
        raise HistoricalReproductionError(
            f"{runner_id} frozen spec hash mismatch: expected {expected_sha256}, "
            f"got {actual_sha256}"
        )
    projection = projection_for(actual_path)
    if projection is not None:
        source_document_path(actual_path, require_private=True)
    identity = {
        "mode": "historical_reproduction",
        "research_authority": "historical_evidence_only",
        "runner_id": runner_id,
        "family": str(entry["family"]),
        "spec_path": str(actual_path.relative_to(PROJECT_ROOT)),
        "spec_sha256": actual_sha256,
        "new_experiment_identity_allowed": False,
        "action_or_live_authorization": False,
    }
    if projection is not None:
        identity["public_projection_sha256"] = projection.public_projection_sha256
        identity["source_availability"] = "private_evidence_store_not_distributed"
    return identity


def stamp_historical_reproduction_output(
    output_dir: str | Path,
    identity: Mapping[str, Any],
) -> None:
    """Stamp successful output so downstream code cannot confuse its authority."""
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "report.json"
    if report_path.is_file():
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if bool(report.get("action_or_live_authorization", False)):
            raise HistoricalReproductionError(
                "historical report attempted to claim action/live authority"
            )
        report["historical_reproduction"] = dict(identity)
        report["research_authority"] = "historical_evidence_only"
        report["new_experiment_identity_allowed"] = False
        report["action_or_live_authorization"] = False
        temporary = report_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        temporary.replace(report_path)
    marker = output / "historical_reproduction_identity.json"
    temporary = marker.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(dict(identity), indent=2, sort_keys=True) + "\n")
    temporary.replace(marker)
