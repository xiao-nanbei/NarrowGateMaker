from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from research.governance.public_machine_projection import (
    projection_for,
    source_identity_sha256,
)
from scripts.build_prospective_lifecycle_narrow_release import build_release
from scripts.preflight_prospective_lifecycle_release import (
    DEFAULT_REMOTE_FORMAL_ROOT,
    FROZEN_V9_BASELINE_SHA256,
    PATCH_EXISTING_FILES,
    REMOTE_V9_SNAPSHOT_DEFAULTS,
    _atomic_write_json,
    _sha256,
    _validate_release_config,
    audit_release,
)

ROOT = Path(__file__).resolve().parents[1]
BASELINE = (
    ROOT / "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260804_v9.json"
)


def _audit(**overrides):
    arguments = {
        "repo_root": ROOT,
        "baseline_identity_path": BASELINE,
        "remote_snapshots": REMOTE_V9_SNAPSHOT_DEFAULTS,
    }
    arguments.update(overrides)
    return audit_release(**arguments)


def test_frozen_v9_identity_and_supplied_remote_copies_match() -> None:
    result = _audit()

    projection = projection_for(BASELINE)
    assert projection is not None
    assert source_identity_sha256(BASELINE) == FROZEN_V9_BASELINE_SHA256
    assert _sha256(BASELINE) == projection.public_projection_sha256
    assert projection.source_private_sha256 == FROZEN_V9_BASELINE_SHA256
    assert result["remote_baseline_identity_sha256"] == projection.source_private_sha256
    assert result["remote_baseline_public_projection_sha256"] == (
        projection.public_projection_sha256
    )
    assert result["remote_baseline_private_source_available"] is True
    assert all(row["matches_frozen_v9"] for row in result["remote_v9_snapshots"].values())
    assert result["safe_to_deploy_directly_from_remote_v9"] is False
    assert result["deployment_executed"] is False


def test_current_worktree_fails_closed_for_mixed_concerns_and_missing_patch() -> None:
    result = _audit()
    blockers = {row["id"] for row in result["blockers"]}

    assert "working_tree_entrypoints_mix_unrelated_concerns" in blockers
    assert "v9_successor_patch_not_frozen" in blockers
    assert "remote_dependency_hash_evidence_missing" in blockers
    assert "production_release_evidence_incomplete" in blockers
    assert "features/feature_dag.py" in result["working_tree_v9_runtime_drift"]


def test_minimal_payload_excludes_cpp_and_full_make_deploy() -> None:
    payload = _audit()["minimal_release_payload"]
    all_files = (
        payload["patch_existing_from_exact_v9"]
        + payload["add_runtime_files"]
        + payload["add_generated_runtime_files"]
        + payload["add_operations_files"]
        + payload["identity_files"]
        + payload["hash_bind_without_replacement"]
    )

    assert not any(path.startswith("cpp/") for path in all_files)
    assert "Makefile deploy target" in payload["excluded"]
    assert "live/config.yaml" in payload["patch_existing_from_exact_v9"]
    assert "execution/order_lifecycle.py" in payload["add_runtime_files"]
    assert "models/replay/__init__.py" in payload["add_generated_runtime_files"]
    assert "execution/order_lifecycle_live_writer_v2.py" in payload["add_runtime_files"]
    assert "models/replay/prospective_baseline_epoch.py" in payload["add_runtime_files"]


def test_tampered_remote_copy_is_rejected(tmp_path: Path) -> None:
    tampered = tmp_path / "main.py"
    tampered.write_bytes(REMOTE_V9_SNAPSHOT_DEFAULTS["live/main.py"].read_bytes() + b"\n")
    snapshots = dict(REMOTE_V9_SNAPSHOT_DEFAULTS)
    snapshots["live/main.py"] = tampered

    result = _audit(remote_snapshots=snapshots)
    blockers = {row["id"] for row in result["blockers"]}

    assert "remote_v9_predecessor_hash_mismatch" in blockers
    assert result["remote_v9_snapshots"]["live/main.py"]["matches_frozen_v9"] is False


def test_manifest_is_deterministic_and_atomic_output_is_valid(tmp_path: Path) -> None:
    first = _audit()
    second = _audit()
    assert first["manifest_sha256"] == second["manifest_sha256"]

    output = tmp_path / "release.json"
    _atomic_write_json(output, first)
    assert json.loads(output.read_text(encoding="utf-8")) == first
    assert not list(tmp_path.glob("*.partial-*"))


def test_candidate_patch_must_define_required_runtime_symbols(tmp_path: Path) -> None:
    for logical_path in PATCH_EXISTING_FILES:
        path = tmp_path / logical_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("value = 1\n", encoding="utf-8")

    result = _audit(candidate_root=tmp_path)
    blockers = {row["id"] for row in result["blockers"]}

    assert "v9_successor_patch_missing_required_symbol" in blockers


def test_release_config_preserves_v9_and_bounds_remote_spool(tmp_path: Path) -> None:
    config = {
        "ml": {"enabled": True},
        "strategy": {
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "use_bar_pricing": False,
        },
        "lifecycle_journal_v2": {
            "enabled": True,
            "storage_profile": "bounded_remote_spool",
            "required_mount": DEFAULT_REMOTE_FORMAL_ROOT,
            "root": f"{DEFAULT_REMOTE_FORMAL_ROOT}/order_lifecycle_journal_v2",
            "prospective_epoch_root": (
                f"{DEFAULT_REMOTE_FORMAL_ROOT}/prospective_baseline_epochs"
            ),
            "remote_spool_allowlisted_roots": [
                DEFAULT_REMOTE_FORMAL_ROOT
            ],
            "remote_session_max_duration_s": 3600,
            "remote_session_max_bytes": 4 * 1024 * 1024 * 1024,
            "baseline_identity_path": (
                "research/families/f10_live_replay_attribution/docs/"
                "operational_baseline_identity_20260804_v9.json"
            ),
            "baseline_identity_sha256": FROZEN_V9_BASELINE_SHA256,
        },
    }
    path = tmp_path / "release.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")

    identity = _validate_release_config(path)
    assert identity["lifecycle_journal_v2"]["enabled"] is True

    config["strategy"]["dynamic_fill_hazard_action_enabled"] = True
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    with pytest.raises(ValueError, match="dynamic_fill_hazard_action_enabled"):
        _validate_release_config(path)


def test_direct_cli_validates_staged_candidate_before_runtime_gates(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    build_release(repo_root=ROOT, output_root=candidate)
    command = [
        sys.executable,
        str(ROOT / "scripts/preflight_prospective_lifecycle_release.py"),
        "--candidate-root",
        str(candidate),
        "--remote-dependency-manifest",
        str(
            ROOT
            / "research/shared/replay_lifecycle/docs/"
            "prospective_lifecycle_remote_v9_dependency_evidence_20260805.json"
        ),
        "--release-config",
        str(candidate / "live/config.yaml"),
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    result = json.loads(completed.stdout)
    blockers = {row["id"] for row in result["blockers"]}

    assert completed.returncode == 2
    assert "candidate_release_manifest_invalid" not in blockers
    assert result["candidate_staging_validation"]["payload_valid"] is True
    assert blockers == {"production_release_evidence_incomplete"}
