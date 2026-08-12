#!/usr/bin/env python3
"""Owner-side audit for NarrowGate private evidence boundaries and identities."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
PRIVATE_MARKER = "Local only — do not publish."
CATALOG_SCHEMA = "narrowgate_private_artifact_catalog_v1"
PROJECTION_SCHEMA = "narrowgate_public_machine_document_projections_v1"
NONPUBLISHED_SCHEMA = "narrowgate_nonpublished_machine_document_projections_v1"

RESEARCH_OWNER_ROOTS = (
    *(Path(f"research/families/f{index:02d}_{name}/private") for index, name in (
        (1, "fixed_parameter_racing"),
        (2, "empirical_p3_touch"),
        (3, "causal_13_head"),
        (4, "external_market_alpha"),
        (5, "fill_quality_quote_ev"),
        (6, "placement_fill_cif"),
        (7, "active_order_continuation"),
        (8, "side_taker_lifecycle"),
        (9, "campaign_action_uplift"),
        (10, "live_replay_attribution"),
    )),
    Path("research/shared/data_identity/private"),
    Path("research/shared/experiment_governance/private"),
    Path("research/shared/replay_lifecycle/private"),
    Path("research/shared/strategy_semantics/private"),
    Path("research/system_engineering/private"),
)
PRIVATE_OWNER_ROOTS = (
    Path("docs/private"),
    Path("live/private"),
    Path("data/private"),
    Path("models/private"),
    Path("execution/private"),
    *RESEARCH_OWNER_ROOTS,
)
PUBLIC_PROJECTION_MANIFESTS = (
    Path("docs/public_machine_document_projections.json"),
    Path("research/public_machine_document_projections.json"),
)
NONPUBLISHED_INDEX = Path(
    "models/private/nonpublished_machine_document_projections.current.local.json"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _ignored(repo_root: Path, relative_path: Path) -> bool:
    return (
        subprocess.run(
            ["git", "check-ignore", "-q", "--", relative_path.as_posix()],
            cwd=repo_root,
            check=False,
        ).returncode
        == 0
    )


def _private_source_path(repo_root: Path, entry: dict[str, Any]) -> Path:
    public_relative = Path(str(entry["public_path"]))
    unit_id = str(entry["unit_id"])
    if unit_id == "repository":
        within_unit = public_relative.relative_to("docs")
        return repo_root / "docs/private/original_public_machine_records" / within_unit
    unit = Path(unit_id)
    try:
        within_unit = public_relative.relative_to(unit)
    except ValueError:
        return (
            repo_root
            / unit
            / "private/original_public_machine_records/cross_unit"
            / public_relative
        )
    return repo_root / unit / "private/original_public_machine_records" / within_unit


def _resolve_catalog_path(repo_root: Path, raw: object) -> Path | None:
    if not isinstance(raw, str) or not raw or "..." in raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve(strict=False)


def audit(repo_root: Path) -> dict[str, object]:
    findings: list[dict[str, object]] = []
    catalog_entries = 0
    semantic_catalog_entries = 0
    verified_catalog_files = 0
    artifact_ids: dict[str, str] = {}
    private_files_seen = 0

    for relative_root in PRIVATE_OWNER_ROOTS:
        root = repo_root / relative_root
        if not root.is_dir():
            findings.append(
                {"kind": "private_owner_root_missing", "path": relative_root.as_posix()}
            )
            continue
        if not _ignored(repo_root, relative_root):
            findings.append(
                {"kind": "private_owner_root_not_ignored", "path": relative_root.as_posix()}
            )
        if root.stat().st_mode & 0o077:
            findings.append(
                {"kind": "private_owner_root_permissions", "path": relative_root.as_posix()}
            )
        marker = root / "README.local.md"
        if not marker.is_file() or marker.read_text(encoding="utf-8").splitlines()[0] != PRIVATE_MARKER:
            findings.append(
                {"kind": "private_owner_marker_invalid", "path": relative_root.as_posix()}
            )
        catalog_path = root / "catalog.current.local.json"
        if not catalog_path.is_file():
            findings.append(
                {"kind": "private_catalog_missing", "path": relative_root.as_posix()}
            )
            continue
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        unit_id = str(catalog.get("unit_id", relative_root.parent.as_posix()))
        if catalog.get("schema_version") != CATALOG_SCHEMA:
            findings.append(
                {"kind": "private_catalog_schema", "path": str(catalog_path.relative_to(repo_root))}
            )
        if catalog.get("visibility") != "local_only_do_not_publish":
            findings.append(
                {"kind": "private_catalog_visibility", "path": str(catalog_path.relative_to(repo_root))}
            )
        for entry in catalog.get("entries", []):
            catalog_entries += 1
            artifact_id = str(entry.get("artifact_id", ""))
            if not artifact_id:
                findings.append(
                    {"kind": "private_artifact_id_missing", "path": str(catalog_path.relative_to(repo_root))}
                )
                continue
            globally_qualified_id = f"{unit_id}:{artifact_id}"
            if globally_qualified_id in artifact_ids:
                findings.append(
                    {
                        "kind": "private_artifact_id_duplicate",
                        "path": str(catalog_path.relative_to(repo_root)),
                        "artifact_id": globally_qualified_id,
                        "first_path": artifact_ids[globally_qualified_id],
                    }
                )
            artifact_ids[globally_qualified_id] = str(catalog_path.relative_to(repo_root))
            panel_role = str(entry.get("panel_role", ""))
            read_gate = str(entry.get("read_gate", ""))
            if not panel_role or not read_gate:
                findings.append(
                    {
                        "kind": "private_read_governance_missing",
                        "path": str(catalog_path.relative_to(repo_root)),
                        "artifact_id": artifact_id,
                    }
                )
            expected_sha = entry.get("sha256")
            expected_bytes = entry.get("bytes")
            if expected_sha is not None or expected_bytes is not None:
                semantic_catalog_entries += 1
            resolved = _resolve_catalog_path(repo_root, entry.get("local_path"))
            if resolved is None or not resolved.is_file():
                continue
            if expected_sha is not None and _sha256(resolved) != expected_sha:
                findings.append(
                    {
                        "kind": "private_catalog_sha_mismatch",
                        "path": str(catalog_path.relative_to(repo_root)),
                        "artifact_id": artifact_id,
                    }
                )
            elif expected_bytes is not None and resolved.stat().st_size != expected_bytes:
                findings.append(
                    {
                        "kind": "private_catalog_size_mismatch",
                        "path": str(catalog_path.relative_to(repo_root)),
                        "artifact_id": artifact_id,
                    }
                )
            elif expected_sha is not None or expected_bytes is not None:
                verified_catalog_files += 1
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            private_files_seen += 1
            if path.stat().st_mode & 0o077:
                findings.append(
                    {
                        "kind": "private_file_permissions",
                        "path": str(path.relative_to(repo_root)),
                    }
                )

    projection_entries = 0
    projection_private_sources_verified = 0
    for relative_manifest in PUBLIC_PROJECTION_MANIFESTS:
        payload = json.loads((repo_root / relative_manifest).read_text(encoding="utf-8"))
        if payload.get("schema_version") != PROJECTION_SCHEMA:
            findings.append(
                {"kind": "projection_manifest_schema", "path": relative_manifest.as_posix()}
            )
        for entry in payload.get("entries", []):
            projection_entries += 1
            public_path = repo_root / str(entry["public_path"])
            if not public_path.is_file():
                findings.append(
                    {"kind": "public_projection_missing", "path": str(entry["public_path"])}
                )
                continue
            if _sha256(public_path) != entry.get("public_projection_sha256"):
                findings.append(
                    {"kind": "public_projection_sha_mismatch", "path": str(entry["public_path"])}
                )
            source_path = _private_source_path(repo_root, entry)
            if not source_path.is_file():
                findings.append(
                    {"kind": "projection_private_source_missing", "path": str(entry["public_path"])}
                )
                continue
            if _sha256(source_path) != entry.get("source_private_sha256"):
                findings.append(
                    {"kind": "projection_private_source_sha_mismatch", "path": str(entry["public_path"])}
                )
            else:
                projection_private_sources_verified += 1

    nonpublished_projection_entries = 0
    nonpublished_projection_files_verified = 0
    nonpublished_path = repo_root / NONPUBLISHED_INDEX
    if not nonpublished_path.is_file():
        findings.append({"kind": "nonpublished_projection_index_missing", "path": str(NONPUBLISHED_INDEX)})
    else:
        payload = json.loads(nonpublished_path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != NONPUBLISHED_SCHEMA:
            findings.append({"kind": "nonpublished_projection_schema", "path": str(NONPUBLISHED_INDEX)})
        for entry in payload.get("entries", []):
            nonpublished_projection_entries += 1
            path = repo_root / str(entry["public_path"])
            if not _ignored(repo_root, Path(str(entry["public_path"]))):
                findings.append(
                    {"kind": "nonpublished_projection_not_ignored", "path": str(entry["public_path"])}
                )
            if not path.is_file() or _sha256(path) != entry.get("public_projection_sha256"):
                findings.append(
                    {"kind": "nonpublished_projection_sha_mismatch", "path": str(entry["public_path"])}
                )
            else:
                nonpublished_projection_files_verified += 1

    catalog_text = "\n".join(
        (repo_root / root / "catalog.current.local.json").read_text(encoding="utf-8")
        for root in PRIVATE_OWNER_ROOTS
        if (repo_root / root / "catalog.current.local.json").is_file()
    )
    if "live/.env" in catalog_text:
        findings.append({"kind": "secret_surface_cataloged", "path": "live/.env"})

    return {
        "schema_version": "narrowgate_private_evidence_audit_v1",
        "owner_roots_expected": len(PRIVATE_OWNER_ROOTS),
        "private_files_seen": private_files_seen,
        "catalog_entries": catalog_entries,
        "semantic_catalog_entries": semantic_catalog_entries,
        "verified_catalog_files": verified_catalog_files,
        "public_projection_entries": projection_entries,
        "projection_private_sources_verified": projection_private_sources_verified,
        "nonpublished_projection_entries": nonpublished_projection_entries,
        "nonpublished_projection_files_verified": nonpublished_projection_files_verified,
        "findings": findings,
        "passed": not findings,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit(args.repo_root.resolve())
    rendered = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
