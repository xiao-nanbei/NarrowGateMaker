from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
import yaml

from scripts.build_warmup_before_websocket_baseline_successor import (
    EXPECTED_PREDECESSOR_SHA256,
    JOURNAL_RUNTIME_MARKERS,
    MANIFEST_NAME,
    NEW_STARTUP_BLOCK,
    OLD_STARTUP_BLOCK,
    PREDECESSOR_IDENTITY_RELATIVE,
    PREDECESSOR_SOURCE_DEFAULTS,
    STARTUP_CONTRACT,
    _canonical_sha256,
    build_release,
    validate_staging,
)

ROOT = Path(__file__).resolve().parents[1]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def built_successors(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("warmup-before-websocket-successor")
    first = root / "first"
    second = root / "second"
    first_manifest, first_validation = build_release(
        repo_root=ROOT,
        output_root=first,
    )
    second_manifest, second_validation = build_release(
        repo_root=ROOT,
        output_root=second,
    )
    return (
        first,
        second,
        first_manifest,
        second_manifest,
        first_validation,
        second_validation,
    )


def _rewrite_manifest_hash(stage: Path) -> None:
    path = stage / MANIFEST_NAME
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest_without_hash = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    manifest["manifest_sha256"] = _canonical_sha256(manifest_without_hash)
    path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def test_build_is_byte_deterministic_and_local_only(built_successors) -> None:
    first, second, first_manifest, second_manifest, first_validation, second_validation = (
        built_successors
    )
    assert first_manifest == second_manifest
    assert first_validation == second_validation
    assert first_validation["payload_valid"] is True
    assert first_validation["deployment_authorized"] is False
    assert first_validation["rollback_authorized"] is False
    assert first_manifest["build_mode"] == ("deterministic_local_stage_only_no_ssh_no_deploy")
    for relative in {MANIFEST_NAME, *EXPECTED_PREDECESSOR_SHA256}:
        assert (first / relative).read_bytes() == (second / relative).read_bytes()


def test_only_semantic_difference_is_startup_order(built_successors) -> None:
    first, _, manifest, *_ = built_successors
    predecessor = PREDECESSOR_SOURCE_DEFAULTS["live/main.py"].read_text(encoding="utf-8")
    candidate = (first / "live/main.py").read_text(encoding="utf-8")
    assert candidate.count(NEW_STARTUP_BLOCK) == 1
    assert candidate.replace(NEW_STARTUP_BLOCK, OLD_STARTUP_BLOCK, 1) == predecessor
    assert candidate.index("engine.start()") < candidate.index("ws.start(rest)")
    for logical_path in EXPECTED_PREDECESSOR_SHA256:
        if logical_path == "live/main.py":
            continue
        assert (first / logical_path).read_bytes() == (
            PREDECESSOR_SOURCE_DEFAULTS[logical_path].read_bytes()
        )
    equality = manifest["strategy_config_semantic_equality"]
    assert equality["passed"] is True
    assert equality["strategy_or_quote_parameters_changed"] is False
    assert (
        equality["predecessor_semantic_identity_sha256"]
        == (equality["successor_semantic_identity_sha256"])
    )


def test_old_startup_order_is_rejected_even_with_rehashed_manifest(
    tmp_path: Path,
    built_successors,
) -> None:
    first = built_successors[0]
    tampered = tmp_path / "old-order"
    shutil.copytree(first, tampered)
    main_path = tampered / "live/main.py"
    candidate = main_path.read_text(encoding="utf-8")
    main_path.write_text(
        candidate.replace(NEW_STARTUP_BLOCK, OLD_STARTUP_BLOCK, 1),
        encoding="utf-8",
    )
    manifest_path = tampered / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    record = next(row for row in manifest["files"] if row["path"] == "live/main.py")
    record["sha256"] = _sha256(main_path)
    record["size_bytes"] = main_path.stat().st_size
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest_hash(tampered)
    with pytest.raises(
        ValueError,
        match="successor must order engine.start before ws.start",
    ):
        validate_staging(tampered, repo_root=ROOT)


def test_lifecycle_journal_is_not_enabled_or_staged(built_successors) -> None:
    first, _, manifest, *_ = built_successors
    config = yaml.safe_load((first / "live/config.yaml").read_text(encoding="utf-8"))
    assert "lifecycle_journal_v2" not in config
    assert manifest["journal_boundary"] == {
        "lifecycle_journal_enabled": False,
        "lifecycle_journal_config_present": False,
        "lifecycle_journal_runtime_imported": False,
        "journal_payload_files_included": False,
        "economic_outcomes_read": False,
    }
    for relative in EXPECTED_PREDECESSOR_SHA256:
        text = (first / relative).read_text(encoding="utf-8")
        assert not [marker for marker in JOURNAL_RUNTIME_MARKERS if marker in text]
    assert not any("journal" in path.name for path in first.rglob("*.py"))


def test_predecessor_and_safe_rollback_probe_are_exactly_bound(
    built_successors,
) -> None:
    manifest = built_successors[2]
    predecessor = manifest["predecessor_binding"]
    rollback = manifest["rollback_binding"]
    probe = rollback["stable_start_probe"]
    assert predecessor["identity_path"] == PREDECESSOR_IDENTITY_RELATIVE
    assert predecessor["startup_contract"] == "unsafe_websocket_before_warmup.v0"
    assert manifest["startup_contract"] == STARTUP_CONTRACT
    assert rollback["startup_contract"] == STARTUP_CONTRACT
    assert rollback["rollback_authorized"] is False
    assert probe["same_pid_required"] is True
    assert probe["canonical_bucket_s"] == 10.0
    assert probe["minimum_completed_canonical_buckets"] == 2
    assert probe["minimum_observed_duration_s"] == 20.0
    assert probe["required_ordered_log_markers"] == [
        "MakerEngine started",
        "All WebSocket streams started",
        "Entering main loop...",
    ]
    assert "Fatal error:" in probe["forbidden_log_markers"]
    assert (
        "completed 10s feature bucket lacks an exact causal 1s grid"
        in probe["forbidden_log_markers"]
    )


def test_build_does_not_modify_v9_identity_or_current_pointer(
    tmp_path: Path,
    built_successors,
) -> None:
    del built_successors
    guarded = [
        ROOT / PREDECESSOR_IDENTITY_RELATIVE,
        ROOT / "research/families/f10_live_replay_attribution/docs/"
        "operational_baseline_current.json",
        ROOT / "live/main.py",
    ]
    before = {path: path.read_bytes() for path in guarded}
    build_release(repo_root=ROOT, output_root=tmp_path / "successor")
    after = {path: path.read_bytes() for path in guarded}
    assert after == before


def test_builder_has_no_remote_or_deployment_execution_surface() -> None:
    source = (ROOT / "scripts/build_warmup_before_websocket_baseline_successor.py").read_text(
        encoding="utf-8"
    )
    assert "import subprocess" not in source
    assert "import socket" not in source
    assert "paramiko" not in source
    assert "ssh " not in source.lower()
    assert 'deployment_executed": True' not in source
