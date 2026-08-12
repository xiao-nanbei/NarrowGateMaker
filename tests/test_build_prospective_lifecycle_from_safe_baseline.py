from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
import yaml

from scripts.build_prospective_lifecycle_from_safe_baseline import (
    ATTEMPT6_MANIFEST_CANONICAL_SHA256,
    ATTEMPT6_ROOT_DEFAULT,
    ATTEMPT6_WRITER_SHA256,
    EXPECTED_ACTION_ENABLEMENT,
    LIVE_WRITER_PATH,
    MANIFEST_NAME,
    OPTIMIZED_WRITER_SHA256,
    PERFORMANCE_LIMITS,
    REQUIRED_RUNTIME_EVIDENCE,
    SAFE_BASELINE_ROOT_DEFAULT,
    SAFE_MANIFEST_CANONICAL_SHA256,
    SAFE_RUNTIME_PATHS,
    STARTUP_CONTRACT,
    _canonical_sha256,
    build_release,
    validate_staging,
)

ROOT = Path(__file__).resolve().parents[1]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def built_attempt7(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("prospective-lifecycle-attempt7")
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


def _rewrite_manifest(stage: Path) -> None:
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


def test_build_is_deterministic_local_only_and_unauthorized(built_attempt7) -> None:
    first, second, first_manifest, second_manifest, first_validation, second_validation = (
        built_attempt7
    )
    assert first_manifest == second_manifest
    assert first_validation == second_validation
    assert first_validation["payload_valid"] is True
    assert first_validation["journal_enabled"] is True
    assert first_validation["deployment_authorized"] is False
    assert first_validation["strategy_action_authorized"] is False
    assert first_validation["live_policy_authorized"] is False
    assert first_manifest["build_mode"] == ("deterministic_local_stage_only_no_ssh_no_deploy")
    paths = {path.relative_to(first).as_posix() for path in first.rglob("*") if path.is_file()}
    for relative in paths:
        assert (first / relative).read_bytes() == (second / relative).read_bytes()


def test_candidate_equals_attempt6_except_current_optimized_writer(built_attempt7) -> None:
    first, _, manifest, *_ = built_attempt7
    candidate_paths = {
        path.relative_to(first).as_posix()
        for path in first.rglob("*")
        if path.is_file() and path.name != MANIFEST_NAME
    }
    attempt6_paths = {
        path.relative_to(ATTEMPT6_ROOT_DEFAULT).as_posix()
        for path in ATTEMPT6_ROOT_DEFAULT.rglob("*")
        if path.is_file() and path.name != MANIFEST_NAME
    }
    assert candidate_paths == attempt6_paths
    differences = [
        relative
        for relative in sorted(candidate_paths)
        if (first / relative).read_bytes() != (ATTEMPT6_ROOT_DEFAULT / relative).read_bytes()
    ]
    assert differences == [LIVE_WRITER_PATH]
    assert (first / LIVE_WRITER_PATH).read_bytes() == (ROOT / LIVE_WRITER_PATH).read_bytes()
    assert _sha256(first / LIVE_WRITER_PATH) == OPTIMIZED_WRITER_SHA256
    assert _sha256(first / LIVE_WRITER_PATH) != ATTEMPT6_WRITER_SHA256
    assert manifest["attempt6_semantics_binding"]["candidate_payload_equal_except"] == [
        LIVE_WRITER_PATH
    ]
    assert manifest["attempt6_semantics_binding"]["manifest_canonical_sha256"] == (
        ATTEMPT6_MANIFEST_CANONICAL_SHA256
    )
    assert manifest["source_bindings"][LIVE_WRITER_PATH] == {
        "role": "current_optimized_repository_source",
        "sha256": OPTIMIZED_WRITER_SHA256,
        "attempt6_sha256": ATTEMPT6_WRITER_SHA256,
        "attempt6_writer_reused": False,
    }


def test_safe_predecessor_six_files_and_journal_absence_are_bound(built_attempt7) -> None:
    manifest = built_attempt7[2]
    predecessor = manifest["predecessor_binding"]
    safe_manifest = json.loads(
        (SAFE_BASELINE_ROOT_DEFAULT / "baseline_successor_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    safe_records = {row["path"]: row for row in safe_manifest["files"]}
    expected_hashes = {path: safe_records[path]["sha256"] for path in sorted(SAFE_RUNTIME_PATHS)}
    assert predecessor["manifest_canonical_sha256"] == SAFE_MANIFEST_CANONICAL_SHA256
    assert predecessor["runtime_file_sha256"] == expected_hashes
    assert predecessor["startup_contract"] == STARTUP_CONTRACT
    assert predecessor["journal_state"] == {
        "enabled": False,
        "config_present": False,
        "runtime_imported": False,
        "payload_files_present": False,
    }
    assert set(predecessor["journal_payload_absent_paths"]) == (
        set(manifest["attempt6_semantics_binding"]["payload_paths"]) - set(SAFE_RUNTIME_PATHS)
    )
    for relative in SAFE_RUNTIME_PATHS:
        record = next(row for row in manifest["files"] if row["path"] == relative)
        assert record["predecessor_sha256"] == expected_hashes[relative]
        assert record["mode"] == "patch_safe_baseline_journal_v2_only"


def test_safe_rollback_target_is_explicit_and_journal_off(built_attempt7) -> None:
    manifest = built_attempt7[2]
    rollback = manifest["rollback_binding"]
    safe_manifest = json.loads(
        (SAFE_BASELINE_ROOT_DEFAULT / "baseline_successor_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert rollback["target_role"] == "safe_rollback_baseline"
    assert rollback["target_baseline_id"] == safe_manifest["successor_baseline_id"]
    assert rollback["target_manifest_canonical_sha256"] == SAFE_MANIFEST_CANONICAL_SHA256
    assert rollback["startup_contract"] == STARTUP_CONTRACT
    assert rollback["resulting_journal_enabled"] is False
    assert rollback["rollback_authorized"] is False
    assert rollback["stable_start_probe"] == safe_manifest["rollback_binding"]["stable_start_probe"]
    assert set(rollback["remove_journal_payload_paths"]) == (
        set(manifest["attempt6_semantics_binding"]["payload_paths"]) - set(SAFE_RUNTIME_PATHS)
    )


def test_only_config_delta_is_enabled_bounded_journal_and_actions_stay_off(
    built_attempt7,
) -> None:
    first, _, manifest, *_ = built_attempt7
    safe = yaml.safe_load(
        (SAFE_BASELINE_ROOT_DEFAULT / "live/config.yaml").read_text(encoding="utf-8")
    )
    candidate = yaml.safe_load((first / "live/config.yaml").read_text(encoding="utf-8"))
    lifecycle = candidate.pop("lifecycle_journal_v2")
    assert candidate == safe
    assert lifecycle["enabled"] is True
    assert lifecycle["storage_profile"] == "bounded_remote_spool"
    assert lifecycle["remote_session_max_duration_s"] == 3600
    assert manifest["config_semantics"]["all_safe_fields_unchanged"] is True
    assert manifest["strategy_semantics"]["action_enablement"] == (EXPECTED_ACTION_ENABLEMENT)
    assert manifest["strategy_semantics"]["strategy_or_quote_parameters_changed"] is False
    assert manifest["journal_boundary"]["candidate_enabled"] is True
    assert manifest["journal_boundary"]["orders_mutated_by_journal"] is False
    assert manifest["journal_boundary"]["strategy_actions_changed"] is False
    permissions = manifest["permissions"]
    assert permissions["strategy_action_authorized"] is False
    assert permissions["live_policy_authorized"] is False
    assert permissions["q90_action_authorized"] is False
    assert permissions["buy_fill_selection_action_authorized"] is False


def test_original_performance_limits_are_frozen_without_relaxation(built_attempt7) -> None:
    manifest = built_attempt7[2]
    assert manifest["performance_limits"] == {
        "minimum_collection_duration_s": 3500.0,
        "maximum_collection_duration_s": 3700.0,
        "producer_enqueue_p99_us": 100.0,
        "producer_enqueue_max_us": 1000.0,
        "quote_loop_p99_regression_pct": 5.0,
        "writer_queue_hwm": 2048,
        "writer_cpu_pct_one_core": 10.0,
        "writer_rss_delta_mib": 256.0,
        "writer_write_p99_ms": 250.0,
    }
    assert manifest["performance_limits"] == PERFORMANCE_LIMITS
    assert tuple(manifest["runtime_evidence_required"]) == REQUIRED_RUNTIME_EVIDENCE


def test_old_attempt6_writer_cannot_be_selected_as_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="optimized live writer SHA256 mismatch"):
        build_release(
            repo_root=ROOT,
            output_root=tmp_path / "rejected-old-writer",
            writer_source=ATTEMPT6_ROOT_DEFAULT / LIVE_WRITER_PATH,
        )


def test_rehashed_candidate_with_old_attempt6_writer_is_rejected(
    tmp_path: Path,
    built_attempt7,
) -> None:
    first = built_attempt7[0]
    tampered = tmp_path / "old-attempt6-writer"
    shutil.copytree(first, tampered)
    writer = tampered / LIVE_WRITER_PATH
    writer.write_bytes((ATTEMPT6_ROOT_DEFAULT / LIVE_WRITER_PATH).read_bytes())
    manifest_path = tampered / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    record = next(row for row in manifest["files"] if row["path"] == LIVE_WRITER_PATH)
    record["sha256"] = _sha256(writer)
    record["size_bytes"] = writer.stat().st_size
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest(tampered)
    with pytest.raises(ValueError, match="candidate writer is not the current optimized source"):
        validate_staging(tampered, repo_root=ROOT)


def test_rehashed_manifest_cannot_relax_action_or_performance_gates(
    tmp_path: Path,
    built_attempt7,
) -> None:
    first = built_attempt7[0]
    tampered = tmp_path / "relaxed-gates"
    shutil.copytree(first, tampered)
    manifest_path = tampered / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["permissions"]["strategy_action_authorized"] = True
    manifest["performance_limits"]["producer_enqueue_p99_us"] = 150.0
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest(tampered)
    with pytest.raises(ValueError, match="performance limits were relaxed or changed"):
        validate_staging(tampered, repo_root=ROOT)


def test_builder_has_no_remote_or_deployment_execution_surface() -> None:
    source = (ROOT / "scripts/build_prospective_lifecycle_from_safe_baseline.py").read_text(
        encoding="utf-8"
    )
    assert "import subprocess" not in source
    assert "import socket" not in source
    assert "paramiko" not in source
    assert "boto" not in source
    assert "ssh " not in source.lower()
    assert 'deployment_executed": True' not in source
    assert 'strategy_action_authorized": True' not in source
