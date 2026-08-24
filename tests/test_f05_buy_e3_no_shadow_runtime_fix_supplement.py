from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

from scripts import f05_buy_e3_no_shadow_runtime_fix_supplement as subject


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, str, str, str, Path, Path]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "NarrowGate Test")
    _git(repo, "config", "user.email", "narrowgate@example.invalid")
    required = (
        "live/config.py",
        "live/main.py",
        "strategy/maker_engine.py",
        "strategy/signal.py",
    )
    for relative in required:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("BASE = True\n", encoding="ascii")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "parent")
    parent_commit = _git(repo, "rev-parse", "HEAD")
    parent_tree = _git(repo, "rev-parse", "HEAD^{tree}")
    _git(repo, "tag", "-a", "parent-tag", "-m", "parent")
    parent_tag_object = _git(repo, "rev-parse", "parent-tag")
    monkeypatch.setattr(
        subject,
        "PARENT_EXECUTION",
        {
            "git_commit": parent_commit,
            "git_tree": parent_tree,
            "annotated_operational_tag": "parent-tag",
            "annotated_operational_tag_object": parent_tag_object,
        },
    )
    monkeypatch.setattr(subject, "AST_BASELINE", {})
    for relative in required:
        (repo / relative).write_text("BASE = True\nFIX = True\n", encoding="ascii")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "runtime fix")
    execution_commit = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "-a", "runtime-fix-tag", "-m", "runtime fix")
    tag_object = _git(repo, "rev-parse", "runtime-fix-tag")

    disabled = tmp_path / "disabled.yaml"
    active = tmp_path / "active.yaml"
    disabled.write_bytes(b"disabled\n")
    active.write_bytes(b"active\n")
    os.chmod(disabled, 0o600)
    os.chmod(active, 0o600)
    monkeypatch.setattr(
        subject,
        "CONFIG_IDENTITIES",
        {
            "disabled": {
                "sha256": hashlib.sha256(disabled.read_bytes()).hexdigest(),
                "size_bytes": disabled.stat().st_size,
                "mode": 0o600,
            },
            "active": {
                "sha256": hashlib.sha256(active.read_bytes()).hexdigest(),
                "size_bytes": active.stat().st_size,
                "mode": 0o600,
            },
        },
    )
    return repo, execution_commit, "runtime-fix-tag", tag_object, disabled, active


def test_supplement_content_validator_returns_exact7(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, commit, tag, tag_object, disabled, active = _fixture(tmp_path, monkeypatch)
    payload = subject.build_supplement(
        repo=repo,
        execution_commit=commit,
        execution_tag=tag,
        execution_tag_object=tag_object,
        disabled_config=disabled,
        active_config=active,
    )
    output = tmp_path / "supplement.json"
    subject._publish(  # noqa: SLF001
        output,
        (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )
    admitted, exact7 = subject.validate_content_receipt(output)
    assert admitted == payload
    assert exact7 == {
        "schema_version": subject.SCHEMA,
        "status": subject.STATUS,
        "file_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "canonical_field": subject.CANONICAL_FIELD,
        "canonical_sha256": payload[subject.CANONICAL_FIELD],
        "size_bytes": output.stat().st_size,
        "mode": "0600",
    }
    assert set(payload["changed_repository_files"]) == {
        "live/config.py",
        "live/main.py",
        "strategy/maker_engine.py",
        "strategy/signal.py",
    }


def test_supplement_validator_rejects_permission_widening(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, commit, tag, tag_object, disabled, active = _fixture(tmp_path, monkeypatch)
    payload = subject.build_supplement(
        repo=repo,
        execution_commit=commit,
        execution_tag=tag,
        execution_tag_object=tag_object,
        disabled_config=disabled,
        active_config=active,
    )
    payload["permissions"]["apply_or_deploy_performed"] = True
    payload[subject.CANONICAL_FIELD] = subject._canonical_sha256(payload)  # noqa: SLF001
    output = tmp_path / "tampered.json"
    output.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(output, 0o600)
    with pytest.raises(ValueError, match="permissions widened"):
        subject.validate_content_receipt(output)


def test_supplement_publish_is_create_only(tmp_path: Path) -> None:
    output = tmp_path / "supplement.json"
    output.write_text("preserve\n", encoding="ascii")
    with pytest.raises(FileExistsError, match="refusing to replace"):
        subject._publish(output, b"replacement\n")  # noqa: SLF001
    assert output.read_text(encoding="ascii") == "preserve\n"
