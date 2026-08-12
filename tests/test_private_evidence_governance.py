from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import scripts.audit_private_evidence as private_audit
from scripts.govern_public_machine_records import govern
from scripts.split_nonpublished_machine_projections import split


def _init_repo(path: Path) -> Path:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    return path


def _projection_manifest(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_public_machine_document_projections_v1",
                "entries": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _private_catalog(path: Path, *, unit_id: str, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_private_artifact_catalog_v1",
                "visibility": "local_only_do_not_publish",
                "documentation_scope": "local_only_do_not_publish",
                "unit_id": unit_id,
                "entries": entries,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_machine_record_governance_preserves_private_source(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("research/**/private/\n", encoding="utf-8")
    _projection_manifest(repo / "docs/public_machine_document_projections.json")
    _projection_manifest(repo / "research/public_machine_document_projections.json")
    public = repo / ("research/families/f05_fill_quality_quote_ev/docs/runtime_receipt.json")
    public.parent.mkdir(parents=True)
    source = {
        "config_path": "docs/private/live_config.current.local.yaml",
        "current_pid": 12345,
        "result": "unchanged",
    }
    source_bytes = json.dumps(source).encode("utf-8")
    public.write_bytes(source_bytes)

    changes = govern(repo, apply=True)

    assert len(changes) == 1
    projection = json.loads(public.read_text(encoding="utf-8"))
    assert projection == {
        "config_path": "${NARROWGATE_LIVE_CONFIG}",
        "current_pid": "<private-process-id>",
        "result": "unchanged",
    }
    private_source = repo / (
        "research/families/f05_fill_quality_quote_ev/private/"
        "original_public_machine_records/docs/runtime_receipt.json"
    )
    assert private_source.read_bytes() == source_bytes
    assert (
        hashlib.sha256(private_source.read_bytes()).hexdigest() == changes[0].source_private_sha256
    )
    assert govern(repo, apply=False) == []


def test_ignored_projection_is_removed_from_public_manifest(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("models/saved_*/\nmodels/private/\n", encoding="utf-8")
    projection = repo / "models/saved_bundle/report.json"
    projection.parent.mkdir(parents=True)
    projection.write_text('{"result": "local"}\n', encoding="utf-8")
    digest = hashlib.sha256(projection.read_bytes()).hexdigest()
    manifest = repo / "docs/public_machine_document_projections.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_public_machine_document_projections_v1",
                "entries": [
                    {
                        "public_path": "models/saved_bundle/report.json",
                        "unit_id": "research/families/f05_fill_quality_quote_ev",
                        "source_private_sha256": "a" * 64,
                        "public_projection_sha256": digest,
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = split(repo, apply=True)

    assert result["private_entries"] == 1
    assert json.loads(manifest.read_text(encoding="utf-8"))["entries"] == []
    private_index = json.loads(
        (
            repo / "models/private/nonpublished_machine_document_projections.current.local.json"
        ).read_text(encoding="utf-8")
    )
    assert private_index["entries"][0]["availability"] == (
        "private_working_tree_projection_not_distributed"
    )

    second = split(repo, apply=True)

    assert second["new_private_entries"] == 0
    assert second["private_entries"] == 1
    assert json.loads(
        (
            repo / "models/private/nonpublished_machine_document_projections.current.local.json"
        ).read_text(encoding="utf-8")
    ) == private_index


def test_private_audit_verifies_owner_catalog_and_projection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("unit/private/\nmodels/private/\n", encoding="utf-8")
    private_root = repo / "unit/private"
    private_root.mkdir(parents=True)
    private_root.chmod(0o700)
    marker = private_root / "README.local.md"
    marker.write_text("Local only — do not publish.\n", encoding="utf-8")
    marker.chmod(0o600)
    evidence = private_root / "evidence.json"
    evidence.write_text('{"identity": "exact"}\n', encoding="utf-8")
    evidence.chmod(0o600)
    _private_catalog(
        private_root / "catalog.current.local.json",
        unit_id="unit",
        entries=[
            {
                "artifact_id": "exact-evidence",
                "role": "audit_receipt",
                "local_path": "unit/private/evidence.json",
                "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
                "bytes": evidence.stat().st_size,
                "availability": "private_not_distributed",
                "panel_role": "historical",
                "read_gate": "owner_only",
            }
        ],
    )
    _projection_manifest(repo / "docs/public_machine_document_projections.json")
    _projection_manifest(repo / "research/public_machine_document_projections.json")
    nonpublished = repo / private_audit.NONPUBLISHED_INDEX
    nonpublished.parent.mkdir(parents=True)
    nonpublished.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_nonpublished_machine_document_projections_v1",
                "entries": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    nonpublished.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    result = private_audit.audit(repo)

    assert result["passed"] is True
    assert result["verified_catalog_files"] == 1

    evidence.write_text('{"identity": "tampered"}\n', encoding="utf-8")
    result = private_audit.audit(repo)
    assert result["passed"] is False
    assert result["findings"][0]["kind"] == "private_catalog_sha_mismatch"
