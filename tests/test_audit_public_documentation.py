import hashlib
import json
import re
import tarfile
from pathlib import Path

import pytest

from scripts import audit_public_documentation as public_audit

audit = public_audit.audit
REPOSITORY_REQUIRED_BILINGUAL_DOCUMENTS = (
    public_audit.REQUIRED_BILINGUAL_DOCUMENTS
)


@pytest.fixture(autouse=True)
def _disable_repository_required_bilingual_documents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep focused temporary repositories independent of the real repo layout."""

    monkeypatch.setattr(public_audit, "REQUIRED_BILINGUAL_DOCUMENTS", ())


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


@pytest.mark.parametrize(
    "home_parts",
    [
        ("Users", "example-user", "project", "report.json"),
        ("home", "example-user", "project", "report.json"),
    ],
)
def test_public_documentation_audit_rejects_generic_absolute_home(
    tmp_path: Path,
    home_parts: tuple[str, str, str, str],
) -> None:
    repo = _init_repo(tmp_path)
    absolute_home = "/" + "/".join(home_parts)
    (repo / "report.md").write_text(absolute_home + "\n", encoding="utf-8")

    result = audit(repo)

    assert result["passed"] is False
    assert any(row["kind"] == "personal_home" for row in result["findings"])


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


def test_machine_skill_instructions_must_be_english(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    skill = repo / ".github" / "skills" / "example" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("# Example\n\nMachine instructions only.\n", encoding="utf-8")
    assert audit(repo)["passed"] is True

    skill.write_text("# Example\n\n机器说明。\n", encoding="utf-8")
    result = audit(repo)
    assert result["passed"] is False
    assert any(
        row["kind"] == "machine_skill_contains_han" for row in result["findings"]
    )


def test_chinese_translation_requires_english_counterpart(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "guide.zh-CN.md").write_text(
        "[English](guide.md)\n\n"
        "Last materially synchronized: 2026-08-29\n",
        encoding="utf-8",
    )

    result = audit(repo)

    assert result["passed"] is False
    assert any(
        row["kind"] == "bilingual_english_document_missing"
        for row in result["findings"]
    )


def test_bilingual_pair_requires_mutual_links_and_matching_sync_marker(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    english = repo / "guide.md"
    translation = repo / "guide.zh-CN.md"
    english.write_text(
        "# Guide\n\nLast materially synchronized: 2026-08-28\n",
        encoding="utf-8",
    )
    translation.write_text(
        "# 指南\n\nLast materially synchronized: 2026-08-29\n",
        encoding="utf-8",
    )

    result = audit(repo)

    kinds = {row["kind"] for row in result["findings"]}
    assert "bilingual_counterpart_link_missing" in kinds
    assert "bilingual_sync_marker_mismatch" in kinds

    english.write_text(
        '<p><a href="guide.md">English</a> | '
        '<a href="guide.zh-CN.md">Simplified Chinese</a></p>\n\n'
        "Last materially synchronized: 2026-08-29\n",
        encoding="utf-8",
    )
    translation.write_text(
        '<p><a href="guide.md">English</a> | '
        '<a href="guide.zh-CN.md">Simplified Chinese</a></p>\n\n'
        "Last materially synchronized: 2026-08-29\n",
        encoding="utf-8",
    )
    assert audit(repo)["passed"] is True


def test_bilingual_pair_requires_sync_markers_on_both_documents(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    (repo / "guide.md").write_text(
        "[简体中文](guide.zh-CN.md)\n",
        encoding="utf-8",
    )
    (repo / "guide.zh-CN.md").write_text(
        "[English](guide.md)\n",
        encoding="utf-8",
    )

    result = audit(repo)

    assert sum(
        row["kind"] == "bilingual_sync_marker_missing"
        for row in result["findings"]
    ) == 2


def test_required_bilingual_scope_is_limited_to_maintained_entrypoints() -> None:
    assert set(REPOSITORY_REQUIRED_BILINGUAL_DOCUMENTS) == {
        Path("README.md"),
        Path("CONTRIBUTING.md"),
        Path("SECURITY.md"),
        Path("docs/opensource/README.md"),
        Path("docs/dev/README.md"),
        Path("docs/ops/README.md"),
        Path("research/README.md"),
        Path("docs/public_private_documentation_contract.md"),
    }


def test_required_maintained_document_requires_bilingual_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path)
    monkeypatch.setattr(
        public_audit,
        "REQUIRED_BILINGUAL_DOCUMENTS",
        (Path("docs/ops/README.md"),),
    )
    english = repo / "docs" / "ops" / "README.md"
    english.parent.mkdir(parents=True)
    english.write_text("# Operations\n", encoding="utf-8")

    result = audit(repo)

    assert result["passed"] is False
    assert any(
        row["kind"] == "required_bilingual_document_missing"
        and row["path"] == "docs/ops/README.zh-CN.md"
        for row in result["findings"]
    )


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

    report.write_text(
        "证据可用性：该 artifact 保存在私有证据库中，不随公共仓库分发。\n\n"
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


def test_public_source_rejects_ssh_target_but_allows_rfc5737_ipv4(
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
        },
    )
    (repo / "example.py").write_text(
        f'REMOTE = "{private_remote}"\n', encoding="utf-8"
    )
    result = audit(repo)
    assert result["passed"] is False
    kinds = {row["kind"] for row in result["findings"]}
    assert "source_ssh_target" in kinds
    assert "source_public_ipv4" not in kinds


@pytest.mark.parametrize(
    "address",
    [
        "0.0.0.0",
        "10.0.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "169.254.1.1",
        "192.0.0.9",
        "192.0.2.10",
        "192.168.1.1",
        "198.18.0.1",
        "198.51.100.20",
        "203.0.113.30",
        "224.0.0.1",
        "240.0.0.1",
    ],
)
def test_public_documentation_allows_explicit_non_public_ipv4(
    tmp_path: Path,
    address: str,
) -> None:
    repo = _init_repo(tmp_path)
    (repo / "report.md").write_text(f"Address: `{address}`.\n", encoding="utf-8")

    assert audit(repo)["passed"] is True


@pytest.mark.parametrize(
    ("filename", "expected_kind"),
    [
        ("report.md", "public_ipv4"),
        ("example.py", "source_public_ipv4"),
    ],
)
@pytest.mark.parametrize(
    "octets",
    [
        ("8", "8", "8", "8"),
        ("008", "008", "008", "008"),
    ],
)
def test_public_surfaces_reject_global_ipv4_between_underscores(
    tmp_path: Path,
    filename: str,
    expected_kind: str,
    octets: tuple[str, str, str, str],
) -> None:
    repo = _init_repo(tmp_path)
    public_ipv4 = ".".join(octets)
    (repo / filename).write_text(
        f'ARCHIVE = "aws_{public_ipv4}_20260811"\n',
        encoding="utf-8",
    )

    result = audit(repo)

    assert result["passed"] is False
    assert any(row["kind"] == expected_kind for row in result["findings"])


def test_public_ipv4_scan_does_not_exempt_auditor_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path)
    public_ipv4 = ".".join(("8", "8", "4", "4"))
    auditor = repo / "audit_public_documentation.py"
    auditor.write_text(f'ENDPOINT = "{public_ipv4}"\n', encoding="utf-8")
    monkeypatch.setattr(public_audit, "__file__", str(auditor))

    result = audit(repo)

    assert result["passed"] is False
    assert any(
        row["path"] == auditor.name and row["kind"] == "source_public_ipv4"
        for row in result["findings"]
    )


def test_public_archive_rejects_global_ipv4(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    public_ipv4 = ".".join(("1", "1", "1", "1"))
    payload = tmp_path / "payload.txt"
    payload.write_text(public_ipv4 + "\n", encoding="utf-8")
    archive = repo / "evidence.tar.gz"
    with tarfile.open(archive, "w:gz") as handle:
        handle.add(payload, arcname="payload.txt")

    result = audit(repo)

    assert result["passed"] is False
    assert any(
        row["kind"] == "archive_public_ipv4" for row in result["findings"]
    )
