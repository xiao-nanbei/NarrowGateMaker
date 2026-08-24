from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

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


def _authorization_allowlist(
    path: Path,
    *,
    repo: Path,
    catalog_artifact_ids: list[str],
    content_roots: list[str],
    private_projection_public_paths: list[str] | None = None,
    nonpublished_projection_paths: list[str] | None = None,
) -> Path:
    payload = {
        "schema_version": private_audit.ALLOWLIST_SCHEMA,
        "authorization_id": "test-development-content-read",
        "repository_root": str(repo.resolve()),
        "catalog_artifact_ids": catalog_artifact_ids,
        "private_projection_public_paths": private_projection_public_paths or [],
        "nonpublished_projection_paths": nonpublished_projection_paths or [],
        "content_roots": content_roots,
        "allowed_read_gates": ["owner_only"],
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    payload["canonical_allowlist_sha256"] = private_audit._document_sha256(
        payload, "canonical_allowlist_sha256"
    )
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    path.chmod(0o600)
    return path


def _minimal_audit_repo(tmp_path: Path, *, entries: list[dict]) -> tuple[Path, Path]:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text(
        "unit/private/\nmodels/private/\n",
        encoding="utf-8",
    )
    private_root = repo / "unit/private"
    private_root.mkdir(parents=True)
    private_root.chmod(0o700)
    marker = private_root / "README.local.md"
    marker.write_text("Local only — do not publish.\n", encoding="utf-8")
    marker.chmod(0o600)
    _private_catalog(
        private_root / "catalog.current.local.json",
        unit_id="unit",
        entries=entries,
    )
    _projection_manifest(repo / "docs/public_machine_document_projections.json")
    _projection_manifest(repo / "research/public_machine_document_projections.json")
    nonpublished = repo / private_audit.NONPUBLISHED_INDEX
    nonpublished.parent.mkdir(parents=True)
    nonpublished.write_text(
        json.dumps(
            {
                "schema_version": private_audit.NONPUBLISHED_SCHEMA,
                "entries": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    nonpublished.chmod(0o600)
    return repo, private_root


def _swap_path_after_secure_open(
    monkeypatch: pytest.MonkeyPatch,
    *,
    target: Path,
    replacement: Path,
) -> dict[str, bool | int | None]:
    real_open = private_audit.os.open
    real_fstat = private_audit.os.fstat
    state: dict[str, bool | int | None] = {"swapped": False, "target_fd": None}
    target_parent = target.parent.stat()

    def swapping_open(path, flags, mode=0o777, *, dir_fd=None):
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if (
            os.fspath(path) == target.name
            and dir_fd is not None
            and os.fstat(dir_fd).st_dev == target_parent.st_dev
            and os.fstat(dir_fd).st_ino == target_parent.st_ino
            and not flags & getattr(os, "O_DIRECTORY", 0)
            and not state["swapped"]
        ):
            state["target_fd"] = descriptor
        return descriptor

    def swapping_fstat(descriptor):
        metadata = real_fstat(descriptor)
        if descriptor == state["target_fd"] and not state["swapped"]:
            replacement.replace(target)
            state["swapped"] = True
        return metadata

    monkeypatch.setattr(private_audit.os, "open", swapping_open)
    monkeypatch.setattr(private_audit.os, "fstat", swapping_fstat)
    return state


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
    assert (
        json.loads(
            (
                repo / "models/private/nonpublished_machine_document_projections.current.local.json"
            ).read_text(encoding="utf-8")
        )
        == private_index
    )


def test_private_audit_defaults_to_metadata_only_without_opening_payload(
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
    real_secure_file_record = private_audit._secure_file_record

    def deny_payload_open(path: Path, **kwargs):
        if Path(path) == evidence:
            pytest.fail("metadata-only audit opened payload bytes")
        return real_secure_file_record(path, **kwargs)

    monkeypatch.setattr(private_audit, "_secure_file_record", deny_payload_open)

    result = private_audit.audit(repo)

    assert result["passed"] is True
    assert result["mode"] == private_audit.METADATA_ONLY
    assert result["metadata_catalog_files_seen"] == 1
    assert result["verified_catalog_files"] == 0
    assert result["payload_files_opened"] == 0

    evidence.write_text('{"identity": "tampered"}\n', encoding="utf-8")
    result = private_audit.audit(repo)
    assert result["passed"] is True
    assert result["payload_files_opened"] == 0


def test_authorized_content_requires_allowlist_and_detects_exact_payload_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
                "local_path": "unit/private/evidence.json",
                "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
                "bytes": evidence.stat().st_size,
                "panel_role": "historical_development",
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
                "schema_version": private_audit.NONPUBLISHED_SCHEMA,
                "entries": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    nonpublished.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    with pytest.raises(private_audit.PrivateEvidenceAuditError, match="requires"):
        private_audit.audit(repo, mode=private_audit.AUTHORIZED_CONTENT)

    allowlist = _authorization_allowlist(
        repo / "authorized.local.json",
        repo=repo,
        catalog_artifact_ids=["unit:exact-evidence"],
        content_roots=["unit/private"],
    )
    result = private_audit.audit(
        repo,
        mode=private_audit.AUTHORIZED_CONTENT,
        allowlist_manifest=allowlist,
    )
    assert result["passed"] is True
    assert result["verified_catalog_files"] == 1
    assert result["payload_files_opened"] == 1

    evidence.write_text('{"identity": "tampered"}\n', encoding="utf-8")
    result = private_audit.audit(
        repo,
        mode=private_audit.AUTHORIZED_CONTENT,
        allowlist_manifest=allowlist,
    )
    assert result["passed"] is False
    assert any(row["kind"] == "private_catalog_sha_mismatch" for row in result["findings"])


def test_metadata_candidate_catalog_root_remap_is_explicit_and_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / "source"
    candidate_path = tmp_path / "candidate"
    source_path.mkdir()
    candidate_path.mkdir()
    source = _init_repo(source_path)
    candidate = _init_repo(candidate_path)
    absolute_source = source / "unit/private/evidence.json"
    entry = {
        "artifact_id": "planned-evidence",
        "local_path": str(absolute_source),
        "panel_role": "operational",
        "read_gate": "owner_authorized_locator_resolution_only",
    }
    for repo in (source, candidate):
        (repo / ".gitignore").write_text("unit/private/\nmodels/private/\n", encoding="utf-8")
        private_root = repo / "unit/private"
        private_root.mkdir(parents=True)
        private_root.chmod(0o700)
        marker = private_root / "README.local.md"
        marker.write_text("Local only — do not publish.\n", encoding="utf-8")
        marker.chmod(0o600)
        evidence = private_root / "evidence.json"
        evidence.write_text('{"identity":"exact"}\n', encoding="utf-8")
        evidence.chmod(0o600)
        _private_catalog(
            private_root / "catalog.current.local.json",
            unit_id="unit",
            entries=[entry],
        )
        _projection_manifest(repo / "docs/public_machine_document_projections.json")
        _projection_manifest(repo / "research/public_machine_document_projections.json")
        nonpublished = repo / private_audit.NONPUBLISHED_INDEX
        nonpublished.parent.mkdir(parents=True)
        nonpublished.write_text(
            json.dumps({"schema_version": private_audit.NONPUBLISHED_SCHEMA, "entries": []}) + "\n",
            encoding="utf-8",
        )
        nonpublished.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    source_result = private_audit.audit(source)
    without_remap = private_audit.audit(candidate)
    with_remap = private_audit.audit(
        candidate,
        catalog_path_source_root=source,
    )

    assert source_result["passed"] is True
    assert without_remap["passed"] is False
    assert any(row["kind"] == "private_catalog_path_escape" for row in without_remap["findings"])
    assert with_remap["passed"] is True
    assert with_remap["catalog_path_remapping_enabled"] is True
    assert (
        with_remap["catalog_path_source_root_sha256"]
        == hashlib.sha256(str(source.resolve()).encode("utf-8")).hexdigest()
    )
    assert with_remap["findings"] == source_result["findings"]


def test_catalog_root_remap_rejects_unsafe_modes_and_preserves_external_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / "source"
    candidate_path = tmp_path / "candidate"
    source_path.mkdir()
    candidate_path.mkdir()
    source, _private = _minimal_audit_repo(source_path, entries=[])
    candidate, candidate_private = _minimal_audit_repo(candidate_path, entries=[])
    outside = tmp_path / "outside.json"
    outside.write_text("{}\n", encoding="utf-8")
    _private_catalog(
        candidate_private / "catalog.current.local.json",
        unit_id="unit",
        entries=[
            {
                "artifact_id": "external",
                "local_path": str(outside),
                "panel_role": "historical",
                "read_gate": "owner_only",
            }
        ],
    )
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    result = private_audit.audit(candidate, catalog_path_source_root=source)
    assert any(row["kind"] == "private_catalog_path_escape" for row in result["findings"])
    allowlist = _authorization_allowlist(
        candidate / "authorized.local.json",
        repo=candidate,
        catalog_artifact_ids=[],
        content_roots=["unit/private"],
    )
    with pytest.raises(private_audit.PrivateEvidenceAuditError, match="restricted"):
        private_audit.audit(
            candidate,
            mode=private_audit.AUTHORIZED_CONTENT,
            allowlist_manifest=allowlist,
            catalog_path_source_root=source,
        )
    with pytest.raises(private_audit.PrivateEvidenceAuditError, match="invalid"):
        private_audit.audit(candidate, catalog_path_source_root=candidate)
    linked_source = tmp_path / "linked-source"
    linked_source.symlink_to(source, target_is_directory=True)
    with pytest.raises(private_audit.PrivateEvidenceAuditError, match="symlink"):
        private_audit.audit(candidate, catalog_path_source_root=linked_source)


@pytest.mark.parametrize(
    ("panel_role", "read_gate", "expected_kind"),
    (
        ("sealed_holdout", "owner_only", "locked_panel_denied"),
        ("historical", "anything_goes", "private_read_gate_unknown"),
    ),
)
def test_metadata_audit_rejects_locked_panels_and_unknown_read_gates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    panel_role: str,
    read_gate: str,
    expected_kind: str,
) -> None:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("unit/private/\nmodels/private/\n", encoding="utf-8")
    private_root = repo / "unit/private"
    private_root.mkdir(parents=True)
    private_root.chmod(0o700)
    marker = private_root / "README.local.md"
    marker.write_text("Local only — do not publish.\n", encoding="utf-8")
    marker.chmod(0o600)
    _private_catalog(
        private_root / "catalog.current.local.json",
        unit_id="unit",
        entries=[
            {
                "artifact_id": "denied",
                "local_path": "unit/private/missing.json",
                "panel_role": panel_role,
                "read_gate": read_gate,
            }
        ],
    )
    _projection_manifest(repo / "docs/public_machine_document_projections.json")
    _projection_manifest(repo / "research/public_machine_document_projections.json")
    nonpublished = repo / private_audit.NONPUBLISHED_INDEX
    nonpublished.parent.mkdir(parents=True)
    nonpublished.write_text(
        json.dumps({"schema_version": private_audit.NONPUBLISHED_SCHEMA, "entries": []}) + "\n",
        encoding="utf-8",
    )
    nonpublished.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    result = private_audit.audit(repo)

    assert result["passed"] is False
    assert any(row["kind"] == expected_kind for row in result["findings"])


def test_metadata_audit_rejects_catalog_path_traversal_and_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("unit/private/\nmodels/private/\n", encoding="utf-8")
    private_root = repo / "unit/private"
    private_root.mkdir(parents=True)
    private_root.chmod(0o700)
    marker = private_root / "README.local.md"
    marker.write_text("Local only — do not publish.\n", encoding="utf-8")
    marker.chmod(0o600)
    outside = repo / "outside.json"
    outside.write_text("{}\n", encoding="utf-8")
    symlink = private_root / "linked.json"
    symlink.symlink_to(outside)
    _private_catalog(
        private_root / "catalog.current.local.json",
        unit_id="unit",
        entries=[
            {
                "artifact_id": "traversal",
                "local_path": "../outside.json",
                "panel_role": "historical",
                "read_gate": "owner_only",
            },
            {
                "artifact_id": "symlink",
                "local_path": "unit/private/linked.json",
                "panel_role": "historical",
                "read_gate": "owner_only",
            },
        ],
    )
    _projection_manifest(repo / "docs/public_machine_document_projections.json")
    _projection_manifest(repo / "research/public_machine_document_projections.json")
    nonpublished = repo / private_audit.NONPUBLISHED_INDEX
    nonpublished.parent.mkdir(parents=True)
    nonpublished.write_text(
        json.dumps({"schema_version": private_audit.NONPUBLISHED_SCHEMA, "entries": []}) + "\n",
        encoding="utf-8",
    )
    nonpublished.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    result = private_audit.audit(repo)
    kinds = {row["kind"] for row in result["findings"]}

    assert "private_catalog_path_escape" in kinds
    assert "private_catalog_symlink_escape" in kinds
    assert "private_symlink_forbidden" in kinds


def test_metadata_audit_rejects_absolute_escape_and_disguised_locked_locator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    repo = _init_repo(repo_path)
    outside = tmp_path / "outside.json"
    outside.write_text('{"not": "authorized"}\n', encoding="utf-8")
    private_root = repo / "unit/private"
    private_root.mkdir(parents=True)
    private_root.chmod(0o700)
    marker = private_root / "README.local.md"
    marker.write_text("Local only — do not publish.\n", encoding="utf-8")
    marker.chmod(0o600)
    _private_catalog(
        private_root / "catalog.current.local.json",
        unit_id="unit",
        entries=[
            {
                "artifact_id": "absolute-escape",
                "local_path": str(outside),
                "panel_role": "historical_development",
                "read_gate": "owner_only",
            },
            {
                "artifact_id": "disguised-locked",
                "local_path": "unit/private/Validation/hidden.json",
                "panel_role": "historical_development",
                "read_gate": "owner_only",
            },
        ],
    )
    _projection_manifest(repo / "docs/public_machine_document_projections.json")
    _projection_manifest(repo / "research/public_machine_document_projections.json")
    nonpublished = repo / private_audit.NONPUBLISHED_INDEX
    nonpublished.parent.mkdir(parents=True)
    nonpublished.write_text(
        json.dumps(
            {
                "schema_version": ("narrowgate_nonpublished_machine_document_projections_v1"),
                "entries": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    nonpublished.chmod(0o600)
    (repo / ".gitignore").write_text("unit/private/\nmodels/private/\n", encoding="utf-8")
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    result = private_audit.audit(repo)

    kinds = {finding["kind"] for finding in result["findings"]}
    assert result["passed"] is False
    assert "private_catalog_path_escape" in kinds
    assert "locked_panel_denied" in kinds


def test_metadata_audit_rejects_symlinked_catalog_before_parsing_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("unit/private/\nmodels/private/\n", encoding="utf-8")
    private_root = repo / "unit/private"
    private_root.mkdir(parents=True)
    private_root.chmod(0o700)
    marker = private_root / "README.local.md"
    marker.write_text("Local only — do not publish.\n", encoding="utf-8")
    marker.chmod(0o600)
    outside = repo / "outside-payload.bin"
    outside.write_bytes(b"this is not governance JSON")
    (private_root / "catalog.current.local.json").symlink_to(outside)
    _projection_manifest(repo / "docs/public_machine_document_projections.json")
    _projection_manifest(repo / "research/public_machine_document_projections.json")
    nonpublished = repo / private_audit.NONPUBLISHED_INDEX
    nonpublished.parent.mkdir(parents=True)
    nonpublished.write_text(
        json.dumps(
            {
                "schema_version": ("narrowgate_nonpublished_machine_document_projections_v1"),
                "entries": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    nonpublished.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))

    result = private_audit.audit(repo)

    kinds = {finding["kind"] for finding in result["findings"]}
    assert result["passed"] is False
    assert "private_catalog_missing_or_unsafe" in kinds


def test_authorization_allowlist_rejects_path_swap_and_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _private_root = _minimal_audit_repo(tmp_path, entries=[])
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))
    allowlist = _authorization_allowlist(
        repo / "authorized.local.json",
        repo=repo,
        catalog_artifact_ids=[],
        content_roots=["unit/private"],
    )
    replacement = repo / "authorized-replacement.local.json"
    replacement.write_bytes(allowlist.read_bytes())
    replacement.chmod(0o600)
    state = _swap_path_after_secure_open(
        monkeypatch,
        target=allowlist,
        replacement=replacement,
    )

    with pytest.raises(private_audit.PrivateEvidenceAuditError, match="changed while it was read"):
        private_audit.audit(
            repo,
            mode=private_audit.AUTHORIZED_CONTENT,
            allowlist_manifest=allowlist,
        )
    assert state["swapped"] is True

    symlink = repo / "authorized-link.local.json"
    symlink.symlink_to(allowlist)
    with pytest.raises(private_audit.PrivateEvidenceAuditError, match="unsafe|symlink"):
        private_audit.audit(
            repo,
            mode=private_audit.AUTHORIZED_CONTENT,
            allowlist_manifest=symlink,
        )


@pytest.mark.parametrize(
    ("relative_target", "expected_kind"),
    (
        ("unit/private/README.local.md", "private_owner_marker_invalid"),
        ("unit/private/catalog.current.local.json", "private_catalog_missing_or_unsafe"),
        (
            "docs/public_machine_document_projections.json",
            "projection_manifest_missing_or_unsafe",
        ),
    ),
)
def test_governance_metadata_reader_rejects_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative_target: str,
    expected_kind: str,
) -> None:
    repo, _private_root = _minimal_audit_repo(tmp_path, entries=[])
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))
    target = repo / relative_target
    replacement = target.parent / f"replacement-{target.name}"
    replacement.write_bytes(target.read_bytes())
    replacement.chmod(target.stat().st_mode & 0o777)
    state = _swap_path_after_secure_open(
        monkeypatch,
        target=target,
        replacement=replacement,
    )

    result = private_audit.audit(repo)

    assert state["swapped"] is True
    assert result["passed"] is False
    assert any(finding["kind"] == expected_kind for finding in result["findings"])


def test_authorized_private_payload_reader_rejects_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_name = "authorized-evidence.json"
    repo, private_root = _minimal_audit_repo(tmp_path, entries=[])
    evidence = private_root / evidence_name
    evidence.write_text('{"identity":"original"}\n', encoding="utf-8")
    evidence.chmod(0o600)
    _private_catalog(
        private_root / "catalog.current.local.json",
        unit_id="unit",
        entries=[
            {
                "artifact_id": "authorized-evidence",
                "local_path": f"unit/private/{evidence_name}",
                "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
                "bytes": evidence.stat().st_size,
                "panel_role": "historical_development",
                "read_gate": "owner_only",
            }
        ],
    )
    allowlist = _authorization_allowlist(
        repo / "authorized.local.json",
        repo=repo,
        catalog_artifact_ids=["unit:authorized-evidence"],
        content_roots=["unit/private"],
    )
    replacement = private_root / "replacement-evidence.json"
    replacement.write_text('{"identity":"replacement"}\n', encoding="utf-8")
    replacement.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))
    state = _swap_path_after_secure_open(
        monkeypatch,
        target=evidence,
        replacement=replacement,
    )

    result = private_audit.audit(
        repo,
        mode=private_audit.AUTHORIZED_CONTENT,
        allowlist_manifest=allowlist,
    )

    assert state["swapped"] is True
    assert result["passed"] is False
    assert result["verified_catalog_files"] == 0
    assert any(
        finding["kind"] == "private_catalog_payload_unsafe" for finding in result["findings"]
    )


def test_authorized_projection_private_source_rejects_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _private_root = _minimal_audit_repo(tmp_path, entries=[])
    public_name = "docs/runtime-projection.json"
    public = repo / public_name
    public.write_text('{"visibility":"public"}\n', encoding="utf-8")
    source = repo / "docs/private/original_public_machine_records/runtime-projection.json"
    source.parent.mkdir(parents=True)
    source.parent.chmod(0o700)
    source.write_text('{"visibility":"private"}\n', encoding="utf-8")
    source.chmod(0o600)
    manifest = repo / "docs/public_machine_document_projections.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": private_audit.PROJECTION_SCHEMA,
                "entries": [
                    {
                        "public_path": public_name,
                        "unit_id": "repository",
                        "public_projection_sha256": hashlib.sha256(public.read_bytes()).hexdigest(),
                        "source_private_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    allowlist = _authorization_allowlist(
        repo / "authorized.local.json",
        repo=repo,
        catalog_artifact_ids=[],
        content_roots=["docs/private"],
        private_projection_public_paths=[public_name],
    )
    replacement = source.parent / "replacement-runtime-projection.json"
    replacement.write_text('{"visibility":"replacement"}\n', encoding="utf-8")
    replacement.chmod(0o600)
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))
    state = _swap_path_after_secure_open(
        monkeypatch,
        target=source,
        replacement=replacement,
    )

    result = private_audit.audit(
        repo,
        mode=private_audit.AUTHORIZED_CONTENT,
        allowlist_manifest=allowlist,
    )

    assert state["swapped"] is True
    assert result["passed"] is False
    assert result["projection_private_sources_verified"] == 0
    assert any(
        finding["kind"] == "projection_private_source_unsafe" for finding in result["findings"]
    )


def test_authorized_mode_never_opens_locked_or_unknown_gate_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, private_root = _minimal_audit_repo(tmp_path, entries=[])
    locked = private_root / "Validation/locked-payload.bin"
    locked.parent.mkdir()
    locked.write_bytes(b"locked bytes must never be opened")
    locked.chmod(0o600)
    unknown = private_root / "unknown-gate-payload.bin"
    unknown.write_bytes(b"unknown gate bytes must never be opened")
    unknown.chmod(0o600)
    entries = [
        {
            "artifact_id": "locked",
            "local_path": "unit/private/Validation/locked-payload.bin",
            "sha256": hashlib.sha256(locked.read_bytes()).hexdigest(),
            "bytes": locked.stat().st_size,
            "panel_role": "sealed_holdout",
            "read_gate": "owner_only",
        },
        {
            "artifact_id": "unknown-gate",
            "local_path": "unit/private/unknown-gate-payload.bin",
            "sha256": hashlib.sha256(unknown.read_bytes()).hexdigest(),
            "bytes": unknown.stat().st_size,
            "panel_role": "historical_development",
            "read_gate": "unknown_gate",
        },
    ]
    _private_catalog(
        private_root / "catalog.current.local.json",
        unit_id="unit",
        entries=entries,
    )
    allowlist = _authorization_allowlist(
        repo / "authorized.local.json",
        repo=repo,
        catalog_artifact_ids=["unit:locked", "unit:unknown-gate"],
        content_roots=["unit/private"],
    )
    monkeypatch.setattr(private_audit, "PRIVATE_OWNER_ROOTS", (Path("unit/private"),))
    real_open = private_audit.os.open

    def deny_forbidden_payload_open(path, flags, mode=0o777, *, dir_fd=None):
        if os.fspath(path) in {locked.name, unknown.name} and dir_fd is not None:
            pytest.fail("locked or unknown-gate payload was opened")
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(private_audit.os, "open", deny_forbidden_payload_open)

    result = private_audit.audit(
        repo,
        mode=private_audit.AUTHORIZED_CONTENT,
        allowlist_manifest=allowlist,
    )

    kinds = {finding["kind"] for finding in result["findings"]}
    assert result["passed"] is False
    assert result["payload_files_opened"] == 0
    assert result["validation_read"] is False
    assert result["sealed_holdout_read"] is False
    assert "locked_panel_denied" in kinds
    assert "private_read_gate_unknown" in kinds
    assert "private_read_gate_not_authorized" in kinds
