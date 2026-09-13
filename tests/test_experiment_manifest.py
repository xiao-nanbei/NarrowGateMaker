from __future__ import annotations

import json
import subprocess
import tarfile
from pathlib import Path

import pytest

from models.audit.experiment_manifest import (
    SCHEMA_VERSION,
    build_manifest,
    git_workspace_identity,
    sha256_file,
    write_code_checkpoint,
    write_manifest,
)
from models.audit.experiment_scorecard import score_profile_contract


def _spec(config: Path, dataset: Path, artifact: Path) -> dict:
    return {
        "experiment_id": "unit-test-v1",
        "engine": "python",
        "config_path": str(config),
        "dataset_manifest_path": str(dataset),
        "feature_schema_version": "features.v1",
        "model_versions": {"model": "model.v1"},
        "label_versions": {"label": "label.v1"},
        "splits": {"train": ["2026-01-01"], "late": ["2026-01-03"]},
        "baseline_definition": {"name": "baseline"},
        "action_definition": {"name": "candidate"},
        "scorecard_profile": score_profile_contract("action_alpha_v1"),
        "input_paths": [],
        "artifact_paths": [str(artifact)],
    }


def _code_identity() -> dict:
    return {
        "commit": "abc123",
        "dirty": True,
        "tracked_patch_sha256": "1" * 64,
        "untracked_files": [],
        "workspace_sha256": "2" * 64,
    }


def test_build_manifest_hashes_config_dataset_and_artifact(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("strategy: {}\n", encoding="utf-8")
    dataset = tmp_path / "days.csv"
    dataset.write_text("day\n2026-01-01\n2026-01-03\n", encoding="utf-8")
    artifact = tmp_path / "result.csv"
    artifact.write_text("value\n1\n", encoding="utf-8")

    manifest = build_manifest(
        _spec(config, dataset, artifact),
        repo_root=tmp_path,
        code_identity=_code_identity(),
    )

    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["config"]["sha256"] == sha256_file(config)
    assert manifest["dataset_manifest"]["days"] == ["2026-01-01", "2026-01-03"]
    assert manifest["dataset_manifest"]["unique_days"] == 2
    assert manifest["artifacts"][0]["sha256"] == sha256_file(artifact)
    assert manifest["scorecard_profile"] == score_profile_contract("action_alpha_v1")
    assert len(manifest["manifest_identity_sha256"]) == 64


def test_manifest_identity_ignores_creation_time(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    dataset = tmp_path / "days.csv"
    artifact = tmp_path / "result.csv"
    config.write_text("x: 1\n", encoding="utf-8")
    dataset.write_text("day\n2026-01-01\n", encoding="utf-8")
    artifact.write_text("x\n", encoding="utf-8")
    spec = _spec(config, dataset, artifact)

    first = build_manifest(spec, repo_root=tmp_path, code_identity=_code_identity())
    second = build_manifest(spec, repo_root=tmp_path, code_identity=_code_identity())

    assert first["manifest_identity_sha256"] == second["manifest_identity_sha256"]


def test_manifest_deduplicates_identical_input_paths(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    dataset = tmp_path / "days.csv"
    artifact = tmp_path / "result.csv"
    source = tmp_path / "source.bin"
    config.write_text("x: 1\n", encoding="utf-8")
    dataset.write_text("day\n2026-01-01\n", encoding="utf-8")
    artifact.write_text("x\n", encoding="utf-8")
    source.write_bytes(b"source")
    spec = _spec(config, dataset, artifact)
    spec["input_paths"] = [str(source), str(source)]

    manifest = build_manifest(
        spec,
        repo_root=tmp_path,
        code_identity=_code_identity(),
    )

    assert len(manifest["inputs"]) == 1


def test_write_manifest_refuses_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "experiment_manifest.json"
    write_manifest(path, {"experiment_id": "one"})
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        write_manifest(path, {"experiment_id": "two"})
    assert json.loads(path.read_text(encoding="utf-8"))["experiment_id"] == "one"


def test_code_checkpoint_restores_tracked_patch_and_untracked_files(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    tracked = repo / "tracked.txt"
    tracked.write_text("before\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    tracked.write_text("after\n", encoding="utf-8")
    (repo / "new.txt").write_text("new\n", encoding="utf-8")

    identity = git_workspace_identity(repo)
    checkpoint = write_code_checkpoint(
        tmp_path / "checkpoint",
        repo_root=repo,
        code_identity=identity,
    )

    assert checkpoint["tracked_patch"]["size_bytes"] > 0
    with tarfile.open(checkpoint["untracked_archive"]["resolved_path"]) as archive:
        assert archive.getnames() == ["new.txt"]
    metadata = json.loads(
        Path(checkpoint["metadata"]["resolved_path"]).read_text(encoding="utf-8")
    )
    assert metadata["base_commit"] == identity["commit"]
    assert metadata["workspace_sha256"] == identity["workspace_sha256"]
