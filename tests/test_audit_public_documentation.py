import hashlib
import json
import re
import tarfile
from pathlib import Path

import pytest

from scripts import audit_public_documentation as public_audit

audit = public_audit.audit


def _init_repo(tmp_path: Path) -> Path:
    import subprocess

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    return tmp_path


def test_public_documentation_audit_accepts_portable_locators(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "README.md").write_text(
        "Use `${NARROWGATE_DATA_ROOT}/panel` and the private evidence store.\n",
        encoding="utf-8",
    )
    result = audit(repo)
    assert result["passed"] is True
    assert result["findings"] == []


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        ("personal_home", "/srv/example-personal/report.json"),
        ("physical_volume", "/srv/example-volume/report.json"),
        ("private_tmp", "/srv/example-tmp/report.json"),
        ("ssh_target", "example-ssh-target"),
        ("cloud_resource_id", "example-cloud-resource-id"),
    ],
)
def test_public_documentation_audit_rejects_private_locator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    value: str,
) -> None:
    monkeypatch.setattr(
        public_audit,
        "PRIVATE_LOCATOR_PATTERNS",
        {kind: re.compile(re.escape(value))},
    )
    repo = _init_repo(tmp_path)
    (repo / "report.md").write_text(value + "\n", encoding="utf-8")
    result = audit(repo)
    assert result["passed"] is False
    assert len(result["findings"]) == 1


def test_ignored_private_document_is_not_scanned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_locator = "/srv/example-private/report.json"
    monkeypatch.setattr(
        public_audit,
        "PRIVATE_LOCATOR_PATTERNS",
        {"test_private": re.compile(re.escape(private_locator))},
    )
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("private/\n", encoding="utf-8")
    private = repo / "private"
    private.mkdir()
    (private / "catalog.json").write_text(
        json.dumps({"path": private_locator}) + "\n", encoding="utf-8"
    )
    result = audit(repo)
    assert result["passed"] is True


def test_projection_manifest_binds_public_bytes(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    report = repo / "report.json"
    report.write_text('{"result": "public"}\n', encoding="utf-8")
    digest = hashlib.sha256(report.read_bytes()).hexdigest()
    research = repo / "research"
    research.mkdir()
    (research / "public_machine_document_projections.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "public_path": "report.json",
                        "public_projection_sha256": digest,
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    result = audit(repo)
    assert result["passed"] is True
    assert result["projection_entries_verified"] == 1

    report.write_text('{"result": "changed"}\n', encoding="utf-8")
    result = audit(repo)
    assert result["passed"] is False
    assert result["findings"][0]["kind"] == "public_projection_sha_mismatch"


def test_repository_relative_markdown_link_must_resolve(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "target.md").write_text("# Target\n", encoding="utf-8")
    source = repo / "README.md"
    source.write_text("Read the [target](target.md).\n", encoding="utf-8")
    result = audit(repo)
    assert result["passed"] is True
    assert result["repository_links_checked"] == 1

    source.write_text("Read the [missing](missing.md).\n", encoding="utf-8")
    result = audit(repo)
    assert result["passed"] is False
    assert result["findings"][0]["kind"] == "broken_repository_link"


def test_markdown_hash_requires_reader_facing_availability_notice(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    digest = "a" * 64
    report = repo / "report.md"
    report.write_text(f"Artifact SHA256: `{digest}`.\n", encoding="utf-8")
    result = audit(repo)
    assert result["passed"] is False
    assert result["findings"][0]["kind"] == "hash_availability_notice_missing"

    report.write_text(
        "Evidence availability: the named artifact is retained in the private evidence "
        "store and is not distributed with the public repository.\n\n"
        f"Artifact SHA256: `{digest}`.\n",
        encoding="utf-8",
    )
    result = audit(repo)
    assert result["passed"] is True


def test_json_markdown_binding_must_match_public_bytes(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    report = repo / "report.md"
    report.write_text("# Public report\n", encoding="utf-8")
    record = repo / "record.json"
    record.write_text(
        json.dumps(
            {
                "report": {
                    "path": "report.md",
                    "sha256": "0" * 64,
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    result = audit(repo)
    assert result["passed"] is False
    assert result["findings"][0]["kind"] == "public_markdown_sha_mismatch"

    payload = json.loads(record.read_text(encoding="utf-8"))
    payload["report"]["sha256"] = hashlib.sha256(report.read_bytes()).hexdigest()
    record.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    result = audit(repo)
    assert result["passed"] is True


def test_public_machine_record_rejects_process_identifier(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    record = repo / "runtime.json"
    record.write_text('{"deployment": {"current_pid": 12345}}\n', encoding="utf-8")

    result = audit(repo)
    assert result["passed"] is False
    assert result["findings"][0]["kind"] == "public_process_identifier"

    record.write_text(
        '{"deployment": {"current_pid": "<private-process-id>"}}\n',
        encoding="utf-8",
    )
    assert audit(repo)["passed"] is True


def test_public_machine_record_rejects_private_locator(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    record = repo / "spec.json"
    record.write_text(
        '{"config_path": "docs/private/live_config.current.local.yaml"}\n',
        encoding="utf-8",
    )
    result = audit(repo)
    assert result["passed"] is False
    assert result["findings"][0]["kind"] == "public_machine_private_locator"

    record.write_text(
        '{"config_path": "${NARROWGATE_LIVE_CONFIG}"}\n', encoding="utf-8"
    )
    assert audit(repo)["passed"] is True


def test_public_projection_must_not_be_git_ignored(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    ignored = repo / "ignored"
    ignored.mkdir()
    projection = ignored / "projection.json"
    projection.write_text('{"result": "local only"}\n', encoding="utf-8")
    digest = hashlib.sha256(projection.read_bytes()).hexdigest()
    research = repo / "research"
    research.mkdir()
    (research / "public_machine_document_projections.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "public_path": "ignored/projection.json",
                        "public_projection_sha256": digest,
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    result = audit(repo)
    assert result["passed"] is False
    assert any(
        row["kind"] == "public_projection_is_git_ignored"
        for row in result["findings"]
    )


def test_public_archive_members_are_scanned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_locator = "/srv/example-private/evidence.json"
    monkeypatch.setattr(
        public_audit,
        "PRIVATE_LOCATOR_PATTERNS",
        {"personal_home": re.compile(re.escape(private_locator))},
    )
    repo = _init_repo(tmp_path)
    payload = tmp_path / "payload.txt"
    payload.write_text(private_locator + "\n", encoding="utf-8")
    archive = repo / "evidence.tar.gz"
    with tarfile.open(archive, "w:gz") as handle:
        handle.add(payload, arcname="payload.txt")

    result = audit(repo)
    assert result["passed"] is False
    assert result["archive_files_scanned"] == 1
    assert result["archive_members_scanned"] == 1
    assert result["findings"][0]["kind"] == "archive_personal_home"


def test_public_source_rejects_known_private_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path)
    private_remote = "example-user@example-host.test via 192.0.2.206"
    monkeypatch.setattr(
        public_audit,
        "PRIVATE_LOCATOR_PATTERNS",
        {
            "ssh_target": re.compile(re.escape("example-user@example-host.test")),
            "known_private_ipv4": re.compile(re.escape("192.0.2.206")),
        },
    )
    (repo / "example.py").write_text(
        f'REMOTE = "{private_remote}"\n', encoding="utf-8"
    )
    result = audit(repo)
    assert result["passed"] is False
    kinds = {row["kind"] for row in result["findings"]}
    assert "source_ssh_target" in kinds
    assert "source_known_private_ipv4" in kinds
