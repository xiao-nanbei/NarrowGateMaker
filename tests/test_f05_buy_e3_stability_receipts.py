from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_stability_receipts as subject

SHA = "a" * 64


def _write_private_json(
    path: Path,
    payload: dict[str, Any],
    *,
    canonical: bool = True,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    if canonical:
        for field in tuple(body):
            if field.startswith("canonical_") and field.endswith("sha256"):
                body.pop(field)
        body["canonical_receipt_sha256"] = subject._document_sha256(  # noqa: SLF001
            body,
            "canonical_receipt_sha256",
        )
    path.write_text(json.dumps(body, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="ascii"))


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _parity_receipt(layer: str, evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": subject.parity_v1.SCHEMA_VERSION,
        "identity": subject.OWNER_IDENTITY,
        "status": "parity_complete",
        "layer": layer,
        "artifact_sha256": subject.ARTIFACT_SHA256,
        "artifact_manifest_file_sha256": subject.ARTIFACT_FILE_SHA256["manifest"],
        "policy_file_sha256": subject.ARTIFACT_FILE_SHA256["policy"],
        "predicate_bundle_file_sha256": subject.ARTIFACT_FILE_SHA256["predicate_bundle"],
        "evidence": evidence,
        "economic_values_materialized_by_replay": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _durability_nodeids() -> list[str]:
    return list(subject.DURABILITY_HARNESS_NODEIDS)


def _direct_source_payloads(
    context: subject.StabilityContext,
) -> dict[str, dict[str, Any]]:
    day_bindings = [
        {
            "utc_day": f"2026-07-{index + 1:02d}",
            "file_name": f"2026-07-{index + 1:02d}.json",
            "file_sha256": f"{index + 1:064x}",
            "canonical_day_receipt_sha256": f"{index + 101:064x}",
        }
        for index in range(subject.EXPECTED_DEVELOPMENT_DAY_COUNT)
    ]
    nodeids = _durability_nodeids()
    python_executable = context.repository_root / "bin" / "python-test"
    source_manifest = subject.durability_tested_source_manifest(context.repository_root)
    test_files = dict(source_manifest["test_files"])
    runtime_sources = dict(source_manifest["runtime_sources"])
    regression = {
        "schema_version": subject.gate_v1.COMPATIBLE_REGRESSION_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "passed",
        "artifact_sha256": subject.ARTIFACT_SHA256,
        "execution_commit": context.execution_commit,
        "execution_tag": context.execution_tag,
        "python_executable": str(python_executable.resolve()),
        "python_file_sha256": _file_sha256(python_executable),
        "nodeids": nodeids,
        "nodeid_manifest_sha256": subject.canonical_sha256(nodeids),
        "collected": len(nodeids),
        "executed": len(nodeids),
        "passed": len(nodeids),
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "collection_return_code": 0,
        "return_code": 0,
        "test_files": test_files,
        "runtime_sources": runtime_sources,
    }
    return {
        "parity_layer1": _parity_receipt(
            subject.parity_v1.RESEARCH_COMPILED_LAYER,
            {
                "structural_rule_tree_equal": True,
                "predicate_count": 12,
                "rule_count": 3,
                "logical_vector_count": subject.parity_v1.DEFAULT_VECTOR_LIMIT,
                "logical_vector_sha256": SHA,
                "decision_signature_sha256": SHA,
                "mismatch_count": 0,
            },
        ),
        "parity_layer2": _parity_receipt(
            subject.parity_v1.DEVELOPMENT_SNAPSHOT_LAYER,
            {
                "opportunity_count": subject.refit.EXPECTED_OPPORTUNITY_COUNT,
                "buy_snapshot_count": 2_000,
                "sell_snapshot_count": subject.refit.EXPECTED_OPPORTUNITY_COUNT - 2_000,
                "selected_predicate_count": 12,
                "selected_state_unobserved_count": 3,
                "predicate_projection_mismatch_count": 0,
                "action_duration_mismatch_count": 0,
                "snapshot_signature_sha256": SHA,
                "mechanics_receipt_sha256": SHA,
                "frozen_source_predicate_bundle_sha256": SHA,
            },
        ),
        "parity_layer3": _parity_receipt(
            subject.parity_v1.STREAMING_OFFLINE_LAYER,
            {
                "callback_count": subject.parity_v1.DEFAULT_STREAMING_CALLBACK_COUNT,
                "completed_window_count": subject.parity_v1.DEFAULT_STREAMING_CALLBACK_COUNT - 1,
                "ema_half_life_count": 10,
                "ema_pair_count": 45,
                "feature_count": 100,
                "feature_ready_ts_ns": 1,
                "feature_age_ms": 0.0,
                "feature_signature_sha256": SHA,
                "feature_mismatch_count": 0,
                "gap_reset_count": 0,
                "out_of_order_count": 0,
            },
        ),
        "parity_layer4": {
            **_parity_receipt(
                subject.parity_v2.LAYER4_LAYER,
                {
                    "day_count": 30,
                    "day_receipts": day_bindings,
                    "mismatch_count": 0,
                    "deadline_lockstep": True,
                    "fill_lockstep": True,
                    "campaign_lockstep": True,
                },
            ),
            "schema_version": subject.parity_v2.LAYER4_RECEIPT_SCHEMA_V2,
        },
        "sell54": _parity_receipt(
            subject.parity_v1.SELL_OWNER_54_CASE_LAYER,
            {
                "policy_sha256": SHA,
                "predicate_bundle_sha256": SHA,
                "predicate_columns": ["p"],
                "sell_tri_state_cases": 27,
                "buy_tri_state_cases": 27,
                "mismatch_count": 0,
                "documented_semantics_equal": True,
                "runtime_binding_valid": True,
            },
        ),
        "regression": regression,
    }


def _durability_harness_payload(
    context: subject.StabilityContext,
    regression: dict[str, Any],
) -> dict[str, Any]:
    nodeids = list(subject.DURABILITY_HARNESS_NODEIDS)
    tested_source_manifest = subject.durability_tested_source_manifest(context.repository_root)
    tested_source_sha = subject.canonical_sha256(tested_source_manifest)
    run_manifest = subject.durability_probe_run_manifest(
        tested_source_manifest_sha256=tested_source_sha,
        synthetic_fixture_sha256=subject.DURABILITY_SYNTHETIC_FIXTURE_SHA256,
    )
    run_manifest_sha = subject.canonical_sha256(run_manifest)
    cache_namespace = subject.durability_probe_cache_namespace(
        tested_source_manifest_sha256=tested_source_sha,
        probe_run_manifest_sha256=run_manifest_sha,
    )
    cache_namespace_sha = subject.canonical_sha256(cache_namespace)

    def mmap_case(case: str) -> dict[str, Any]:
        injected = case == "injected_exception"
        task_results = subject._expected_task_results(case)  # noqa: SLF001
        result_hashes = [item["result_sha256"] for item in task_results]
        return {
            "case": case,
            "configured_worker_count": 10,
            "submitted_task_count": 10,
            "terminal_task_count": 10,
            "terminal_before_pool_shutdown_count": 10,
            "peak_concurrent_worker_count": 10,
            "cancel_request_count": 10 if injected else 0,
            "consumed_result_count": 0 if injected else 10,
            "produced_result_count": 9 if injected else 10,
            "task_results": task_results,
            "task_result_set_sha256": subject.canonical_sha256(result_hashes),
            "expected_exception_observed": injected,
            "unexpected_worker_exception_count": 0,
            "pool_shutdown_call_count": 1,
            "pool_shutdown_complete": True,
            "mmap_mode": "read_only",
            "mmap_open_count": 1,
            "mmap_close_count": 1,
            "mmap_close_before_terminal_count": 0,
            "mmap_use_after_close_count": 0,
            "lifecycle_events": subject._expected_mmap_lifecycle(case),  # noqa: SLF001
        }

    probe = {
        "schema_version": subject.DURABILITY_PROBE_SCHEMA,
        "configured_worker_count": 10,
        "tasks_per_case": 10,
        "fixture_sha256": subject.DURABILITY_SYNTHETIC_FIXTURE_SHA256,
        "cases": {
            "success": mmap_case("success"),
            "injected_exception": mmap_case("injected_exception"),
        },
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "subprocess_returncode": 0,
    }
    cache = {
        "schema_version": subject.DURABILITY_CACHE_PROBE_SCHEMA,
        "probe_cache_namespace_sha256": cache_namespace_sha,
        "probe_run_manifest_sha256": run_manifest_sha,
        "cache_key_sha256": SHA,
        "cache_key_probe_namespace_sha256": cache_namespace_sha,
        "cache_root_namespace_count": 2,
        "cache_entry_count": 2,
        "cache_hit_count": 3,
        "interruption_resume_count": 1,
        "interrupted_entry_visible_count": 0,
        "stale_partial_after_interruption_count": 0,
        "remaining_partial_entry_count": 0,
        "staging_observed_before_publish": True,
        "final_complete_observed": True,
        "public_partial_load_attempt_count": 1,
        "public_partial_load_none_count": 1,
        "public_partial_load_visible_count": 0,
        "public_partial_load_exception_count": 0,
        "observer_join_failure_count": 0,
        "partial_cache_visibility_count": 0,
        "atomic_publish_failure_count": -1,
        "repeated_run_count": 2,
        "repeated_run_result_sha256s": [SHA, SHA],
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    cache["atomic_publish_failure_count"] = subject.derive_atomic_publish_failure_count(cache)
    checks, failures, counts = subject.durability_measurement_contract(probe, cache)
    measurement = {
        "schema_version": subject.DURABILITY_MEASUREMENT_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "durability_measurements_complete",
        **counts,
        "checks": checks,
        "failure_counts": failures,
        "tested_source_manifest": tested_source_manifest,
        "tested_source_manifest_sha256": tested_source_sha,
        "probe_run_manifest": run_manifest,
        "probe_run_manifest_sha256": run_manifest_sha,
        "probe_cache_namespace": cache_namespace,
        "probe_cache_namespace_sha256": cache_namespace_sha,
        "event_series_sha256": "",
        "probe_measurements": probe,
        "cache_measurements": cache,
        "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
        "permissions": dict(subject.PERMISSIONS),
        "economic_outcomes_read": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    measurement["event_series_sha256"] = subject.canonical_sha256(
        subject.durability_event_series(measurement)
    )
    observations = subject.durability_measurement_observations(measurement)
    test_files = dict(tested_source_manifest["test_files"])
    runtime_sources = dict(tested_source_manifest["runtime_sources"])
    payload = {
        "schema_version": subject.DURABILITY_HARNESS_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "durability_harness_passed",
        "python_executable": regression["python_executable"],
        "python_file_sha256": regression["python_file_sha256"],
        "run_command": [
            regression["python_executable"],
            "-m",
            "pytest",
            "-q",
            *nodeids,
        ],
        "nodeids": nodeids,
        "nodeid_manifest_sha256": subject.canonical_sha256(nodeids),
        "gate_nodeids": dict(subject.DURABILITY_GATE_NODEIDS),
        "counts": {
            "collected": len(nodeids),
            "executed": len(nodeids),
            "passed": len(nodeids),
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "return_code": 0,
        },
        "test_files": test_files,
        "runtime_sources": runtime_sources,
        "tested_source_manifest_sha256": tested_source_sha,
        "measurement": measurement,
        "measurement_sha256": subject.canonical_sha256(measurement),
        "observations": observations,
        "failure_counts": failures,
        "probe_cache_namespace_sha256": cache_namespace_sha,
        "probe_run_manifest_sha256": run_manifest_sha,
        "event_series_sha256": measurement["event_series_sha256"],
        "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
        "permissions": dict(subject.PERMISSIONS),
    }
    payload["canonical_receipt_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload,
        "canonical_receipt_sha256",
    )
    return payload


def _context(tmp_path: Path) -> subject.StabilityContext:
    repository = tmp_path / "repo"
    repository.mkdir()
    source_names = (
        subject.DURABILITY_HARNESS_TEST_FILE,
        *subject.DURABILITY_RUNTIME_SOURCE_FILES,
    )
    for index, relative_name in enumerate(source_names):
        source = repository / relative_name
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"# frozen durability source {index}\n", encoding="ascii")
    (repository / "bin").mkdir()
    executable = repository / "bin" / "python-test"
    executable.write_bytes(b"synthetic-test-interpreter\n")
    executable.chmod(0o700)
    _git(repository, "init")
    _git(repository, "config", "user.name", "NarrowGate Test")
    _git(repository, "config", "user.email", "narrowgate@example.invalid")
    _git(repository, "add", "--all")
    _git(repository, "commit", "-m", "freeze durability sources")
    commit = _git(repository, "rev-parse", "HEAD")
    tag = "f05-owner-buy-e3-test"
    _git(repository, "tag", "-a", tag, "-m", "freeze durability sources")
    return subject.StabilityContext(
        repository_root=repository,
        execution_commit=commit,
        execution_tag=tag,
        layer4_contract_path=tmp_path / "layer4-contract.json",
        layer4_day_receipt_dir=tmp_path / "layer4-days",
    )


def _install_validator_stubs(
    monkeypatch: pytest.MonkeyPatch,
    context: subject.StabilityContext,
) -> None:
    def parity(path: Path, *, expected_layer: str, expected_artifact_sha256: str) -> dict:
        payload = _read(path)
        assert payload["layer"] == expected_layer
        assert expected_artifact_sha256 == subject.ARTIFACT_SHA256
        return payload

    def layer4(
        path: Path,
        *,
        contract_path: Path,
        day_receipt_dir: Path,
    ) -> dict:
        assert contract_path == context.layer4_contract_path
        assert day_receipt_dir == context.layer4_day_receipt_dir
        return _read(path)

    def sell54(
        path: Path,
        *,
        repository_root: Path,
        expected_artifact_sha256: str,
        expected_artifact_files: dict[str, str],
    ) -> dict:
        assert repository_root == context.repository_root
        assert expected_artifact_sha256 == subject.ARTIFACT_SHA256
        assert expected_artifact_files == subject.ARTIFACT_FILE_SHA256
        return {"source_manifest_sha256": SHA, "path": str(path)}

    def regression(
        path: Path,
        *,
        repository_root: Path,
        expected_artifact_sha256: str,
        expected_execution_commit: str,
        expected_execution_tag: str,
    ) -> dict:
        assert repository_root == context.repository_root
        assert expected_artifact_sha256 == subject.ARTIFACT_SHA256
        assert expected_execution_commit == context.execution_commit
        assert expected_execution_tag == context.execution_tag
        return _read(path)

    monkeypatch.setattr(subject.parity_v1, "validate_parity_receipt", parity)
    monkeypatch.setattr(subject.parity_v2, "validate_layer4_receipt_v2", layer4)
    monkeypatch.setattr(subject.gate_v1, "validate_sell_owner_54_case_receipt", sell54)
    monkeypatch.setattr(subject.gate_v1, "validate_runtime_regression_receipt", regression)


@dataclass(frozen=True)
class Evidence:
    sources: dict[str, Path]
    direct: dict[str, Path]
    context: subject.StabilityContext
    output: Path
    strict_dir: Path
    single_day_stage: Path
    zero_economic_stage: Path
    durability_harness: Path


@pytest.fixture
def evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Evidence:
    context = _context(tmp_path)
    _install_validator_stubs(monkeypatch, context)
    direct_payloads = _direct_source_payloads(context)
    direct_dir = tmp_path / "direct"
    direct = {
        role: _write_private_json(direct_dir / f"{role}.json", payload)
        for role, payload in direct_payloads.items()
    }
    stage_dir = tmp_path / "stages"
    single_day_stage = _write_private_json(
        stage_dir / "stage_one_day_mechanics.json",
        {
            "status": subject.LEGACY_SINGLE_DAY_STATUS,
            "opportunity_count": subject.EXPECTED_ONE_DAY_OPPORTUNITY_COUNT,
            "exact_owner_noop_parity_count": subject.EXPECTED_ONE_DAY_OPPORTUNITY_COUNT,
            "economic_values_persisted": False,
            "economic_values_used_for_selection": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        canonical=False,
    )
    zero_economic_stage = _write_private_json(
        stage_dir / "stage_zero_economic_preflight.json",
        {
            "status": subject.LEGACY_ZERO_ECONOMIC_STATUS,
            "economic_outcomes_read": False,
            "outer_fold_count": subject.EXPECTED_OUTER_FOLD_COUNT,
            "inner_fold_count": subject.EXPECTED_INNER_FOLD_COUNT,
            "exact_owner_day_count": subject.EXPECTED_DEVELOPMENT_DAY_COUNT,
            "exact_owner_mismatch_count": 0,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        canonical=False,
    )
    durability_harness = _write_private_json(
        stage_dir / "durability_harness.json",
        _durability_harness_payload(context, direct_payloads["regression"]),
    )
    strict_dir = tmp_path / "strict"
    materialized = subject.materialize_strict_source_receipts(
        single_day_stage_path=single_day_stage,
        zero_economic_stage_path=zero_economic_stage,
        durability_harness_path=durability_harness,
        regression_receipt_path=direct["regression"],
        output_dir=strict_dir,
        context=context,
    )
    return Evidence(
        sources={**materialized, **direct},
        direct=direct,
        context=context,
        output=tmp_path / "wrappers",
        strict_dir=strict_dir,
        single_day_stage=single_day_stage,
        zero_economic_stage=zero_economic_stage,
        durability_harness=durability_harness,
    )


def test_materializes_provenance_bound_private_sources(evidence: Evidence) -> None:
    single = _read(evidence.sources["single_day"])
    zero = _read(evidence.sources["all_fold_zero_economic"])
    durability = _read(evidence.sources["durability_concurrency_cache"])

    for role in subject.MATERIALIZED_SOURCE_ROLES:
        path = evidence.sources[role]
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert _read(path)["canonical_receipt_sha256"]
    legacy_binding = single["underlying_receipt"]
    assert set(legacy_binding) == subject._UNDERLYING_BINDING_FIELDS  # noqa: SLF001
    assert legacy_binding["schema_version"] is None
    assert legacy_binding["identity"] is None
    assert legacy_binding["canonical_field"] is None
    assert legacy_binding["canonical_sha256"] is None
    assert zero["outer_fold_count"] == 4
    assert zero["inner_fold_count"] == 12
    assert zero["day_count"] == 30
    assert zero["mismatch_count"] == 0
    assert set(durability["underlying_receipts"]) == {
        "durability_harness",
        "regression",
    }
    assert all(durability["checks"].values())
    assert all(value == 0 for value in durability["failure_counts"].values())
    serialized = json.dumps({"single": single, "zero": zero, "durability": durability})
    assert all(token not in serialized.lower() for token in ("pnl", "gross", "usdc", "reward"))


def test_builds_and_revalidates_all_nine_private_wrappers(evidence: Evidence) -> None:
    wrappers = subject.build_stability_wrappers(
        source_receipts=evidence.sources,
        output_dir=evidence.output,
        context=evidence.context,
    )

    assert tuple(wrappers) == subject.REQUIRED_ROLES
    paths = {role: evidence.output / f"{role}.json" for role in subject.REQUIRED_ROLES}
    assert subject.validate_stability_wrappers(wrappers=paths, context=evidence.context) == wrappers
    attempt_bindings = subject.attempt._receipt_bindings(  # noqa: SLF001
        paths,
        required_roles=subject.REQUIRED_ROLES,
    )
    assert tuple(sorted(attempt_bindings)) == tuple(sorted(subject.REQUIRED_ROLES))
    for role, path in paths.items():
        payload = _read(path)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert payload["role"] == role
        assert payload["evidence_boundary"] == subject.EVIDENCE_BOUNDARY
        assert payload["permissions"] == subject.PERMISSIONS
        assert set(payload) == subject._WRAPPER_FIELDS  # noqa: SLF001
        assert "evidence" not in payload
        assert payload["source_receipt"]["file_sha256"]
        assert payload["source_receipt"]["canonical_sha256"]


def test_existing_wrapper_path_is_never_replaced(evidence: Evidence) -> None:
    subject.build_stability_wrappers(
        source_receipts=evidence.sources,
        output_dir=evidence.output,
        context=evidence.context,
    )
    original = (evidence.output / "single_day.json").read_bytes()

    with pytest.raises(subject.StabilityReceiptError, match="already exist"):
        subject.build_stability_wrappers(
            source_receipts=evidence.sources,
            output_dir=evidence.output,
            context=evidence.context,
        )

    assert (evidence.output / "single_day.json").read_bytes() == original


def test_existing_strict_source_paths_are_never_replaced(evidence: Evidence) -> None:
    originals = {
        role: evidence.sources[role].read_bytes() for role in subject.MATERIALIZED_SOURCE_ROLES
    }
    with pytest.raises(subject.StabilityReceiptError, match="strict source paths already exist"):
        subject.materialize_strict_source_receipts(
            single_day_stage_path=evidence.single_day_stage,
            zero_economic_stage_path=evidence.zero_economic_stage,
            durability_harness_path=evidence.durability_harness,
            regression_receipt_path=evidence.direct["regression"],
            output_dir=evidence.strict_dir,
            context=evidence.context,
        )
    assert all(
        evidence.sources[role].read_bytes() == originals[role]
        for role in subject.MATERIALIZED_SOURCE_ROLES
    )


def test_direct_source_tamper_after_wrap_is_detected(evidence: Evidence) -> None:
    subject.build_stability_wrappers(
        source_receipts=evidence.sources,
        output_dir=evidence.output,
        context=evidence.context,
    )
    payload = _read(evidence.sources["parity_layer1"])
    payload["attacker_note"] = "bytes changed"
    _write_private_json(evidence.sources["parity_layer1"], payload)

    with pytest.raises(subject.StabilityReceiptError, match="bytes drifted"):
        subject.validate_stability_wrapper(
            evidence.output / "parity_layer1.json",
            expected_role="parity_layer1",
            context=evidence.context,
        )


def test_legacy_stage_tamper_after_materialization_is_detected(evidence: Evidence) -> None:
    payload = _read(evidence.single_day_stage)
    payload["opportunity_count"] -= 1
    _write_private_json(evidence.single_day_stage, payload, canonical=False)

    with pytest.raises(subject.StabilityReceiptError, match="file identity or bytes drifted"):
        subject.validate_source_receipt(
            "single_day",
            evidence.sources["single_day"],
            evidence.context,
        )


def test_legacy_stage_same_byte_path_swap_is_detected(evidence: Evidence) -> None:
    original = evidence.zero_economic_stage.read_bytes()
    replacement = evidence.zero_economic_stage.with_suffix(".replacement")
    replacement.write_bytes(original)
    replacement.chmod(0o600)
    os.replace(replacement, evidence.zero_economic_stage)

    with pytest.raises(subject.StabilityReceiptError, match="file identity or bytes drifted"):
        subject.validate_source_receipt(
            "all_fold_zero_economic",
            evidence.sources["all_fold_zero_economic"],
            evidence.context,
        )


def test_durability_rejects_failed_raw_observation(evidence: Evidence, tmp_path: Path) -> None:
    payload = _read(evidence.durability_harness)
    payload["observations"]["terminal_task_count"] -= 1
    bad_harness = _write_private_json(tmp_path / "bad-harness.json", payload)

    with pytest.raises(subject.StabilityReceiptError, match="execution evidence drifted"):
        subject.materialize_strict_source_receipts(
            single_day_stage_path=evidence.single_day_stage,
            zero_economic_stage_path=evidence.zero_economic_stage,
            durability_harness_path=bad_harness,
            regression_receipt_path=evidence.direct["regression"],
            output_dir=tmp_path / "bad-strict",
            context=evidence.context,
        )
    assert not (tmp_path / "bad-strict").exists()


def test_durability_harness_is_pre_admission_probe_not_final_manifest(
    evidence: Evidence,
) -> None:
    payload = _read(evidence.durability_harness)
    assert "execution_commit" not in payload
    assert "execution_tag" not in payload
    assert "artifact_sha256" not in payload
    serialized = json.dumps(payload)
    assert "execution_manifest_canonical_sha256" not in serialized
    assert payload["measurement"]["probe_run_manifest"]["final_execution_manifest_bound"] is False
    assert (
        payload["measurement"]["probe_cache_namespace"]["final_execution_manifest_bound"] is False
    )


def test_stability_recomputes_raw_event_series_after_adversarial_rehash(
    evidence: Evidence,
    tmp_path: Path,
) -> None:
    payload = _read(evidence.durability_harness)
    measurement = payload["measurement"]
    measurement["probe_measurements"]["cases"]["injected_exception"][
        "terminal_before_pool_shutdown_count"
    ] = 9
    measurement["event_series_sha256"] = subject.canonical_sha256(
        subject.durability_event_series(measurement)
    )
    payload["measurement_sha256"] = subject.canonical_sha256(measurement)
    payload["event_series_sha256"] = measurement["event_series_sha256"]
    bad_harness = _write_private_json(tmp_path / "raw-event-rehashed.json", payload)

    with pytest.raises(subject.StabilityReceiptError, match="measurement contract drifted"):
        subject.materialize_strict_source_receipts(
            single_day_stage_path=evidence.single_day_stage,
            zero_economic_stage_path=evidence.zero_economic_stage,
            durability_harness_path=bad_harness,
            regression_receipt_path=evidence.direct["regression"],
            output_dir=tmp_path / "raw-event-strict",
            context=evidence.context,
        )


def test_stability_recomputes_probe_cache_namespace_after_adversarial_rehash(
    evidence: Evidence,
    tmp_path: Path,
) -> None:
    payload = _read(evidence.durability_harness)
    measurement = payload["measurement"]
    namespace = measurement["probe_cache_namespace"]
    namespace["identity_kind"] = "forged_post_hoc_namespace"
    namespace_sha = subject.canonical_sha256(namespace)
    measurement["probe_cache_namespace_sha256"] = namespace_sha
    cache = measurement["cache_measurements"]
    cache["probe_cache_namespace_sha256"] = namespace_sha
    cache["cache_key_probe_namespace_sha256"] = namespace_sha
    measurement["event_series_sha256"] = subject.canonical_sha256(
        subject.durability_event_series(measurement)
    )
    payload["measurement_sha256"] = subject.canonical_sha256(measurement)
    payload["probe_cache_namespace_sha256"] = namespace_sha
    payload["event_series_sha256"] = measurement["event_series_sha256"]
    bad_harness = _write_private_json(tmp_path / "namespace-rehashed.json", payload)

    with pytest.raises(subject.StabilityReceiptError, match="probe cache namespace drifted"):
        subject.materialize_strict_source_receipts(
            single_day_stage_path=evidence.single_day_stage,
            zero_economic_stage_path=evidence.zero_economic_stage,
            durability_harness_path=bad_harness,
            regression_receipt_path=evidence.direct["regression"],
            output_dir=tmp_path / "namespace-strict",
            context=evidence.context,
        )


def test_stability_recomputes_probe_run_manifest_after_full_cascading_rehash(
    evidence: Evidence,
    tmp_path: Path,
) -> None:
    payload = _read(evidence.durability_harness)
    measurement = payload["measurement"]
    run_manifest = measurement["probe_run_manifest"]
    run_manifest["configured_worker_count"] = 9
    run_sha = subject.canonical_sha256(run_manifest)
    measurement["probe_run_manifest_sha256"] = run_sha
    namespace = measurement["probe_cache_namespace"]
    namespace["probe_run_manifest_sha256"] = run_sha
    namespace_sha = subject.canonical_sha256(namespace)
    measurement["probe_cache_namespace_sha256"] = namespace_sha
    cache = measurement["cache_measurements"]
    cache["probe_run_manifest_sha256"] = run_sha
    cache["probe_cache_namespace_sha256"] = namespace_sha
    cache["cache_key_probe_namespace_sha256"] = namespace_sha
    measurement["event_series_sha256"] = subject.canonical_sha256(
        subject.durability_event_series(measurement)
    )
    payload["probe_run_manifest_sha256"] = run_sha
    payload["probe_cache_namespace_sha256"] = namespace_sha
    payload["event_series_sha256"] = measurement["event_series_sha256"]
    payload["measurement_sha256"] = subject.canonical_sha256(measurement)
    bad_harness = _write_private_json(tmp_path / "run-manifest-rehashed.json", payload)

    with pytest.raises(subject.StabilityReceiptError, match="probe run manifest drifted"):
        subject.materialize_strict_source_receipts(
            single_day_stage_path=evidence.single_day_stage,
            zero_economic_stage_path=evidence.zero_economic_stage,
            durability_harness_path=bad_harness,
            regression_receipt_path=evidence.direct["regression"],
            output_dir=tmp_path / "run-manifest-strict",
            context=evidence.context,
        )


def test_stability_recomputes_each_task_measurement_after_rehash(
    evidence: Evidence,
    tmp_path: Path,
) -> None:
    payload = _read(evidence.durability_harness)
    measurement = payload["measurement"]
    success = measurement["probe_measurements"]["cases"]["success"]
    success["task_results"][0]["result_sha256"] = "f" * 64
    success["task_result_set_sha256"] = subject.canonical_sha256(
        [item["result_sha256"] for item in success["task_results"]]
    )
    measurement["event_series_sha256"] = subject.canonical_sha256(
        subject.durability_event_series(measurement)
    )
    payload["event_series_sha256"] = measurement["event_series_sha256"]
    payload["measurement_sha256"] = subject.canonical_sha256(measurement)
    bad_harness = _write_private_json(tmp_path / "task-measurement-rehashed.json", payload)

    with pytest.raises(subject.StabilityReceiptError, match="task measurements drifted"):
        subject.materialize_strict_source_receipts(
            single_day_stage_path=evidence.single_day_stage,
            zero_economic_stage_path=evidence.zero_economic_stage,
            durability_harness_path=bad_harness,
            regression_receipt_path=evidence.direct["regression"],
            output_dir=tmp_path / "task-measurement-strict",
            context=evidence.context,
        )


def test_stability_rejects_fictitious_durability_test_nodeid(
    evidence: Evidence,
    tmp_path: Path,
) -> None:
    payload = _read(evidence.durability_harness)
    payload["nodeids"][0] = "tests/test_durability.py::test_worker_concurrency"
    payload["nodeid_manifest_sha256"] = subject.canonical_sha256(payload["nodeids"])
    payload["run_command"][-len(payload["nodeids"]) :] = payload["nodeids"]
    bad_harness = _write_private_json(tmp_path / "fictitious-nodeid.json", payload)

    with pytest.raises(subject.StabilityReceiptError, match="counts or nodeids are malformed"):
        subject.materialize_strict_source_receipts(
            single_day_stage_path=evidence.single_day_stage,
            zero_economic_stage_path=evidence.zero_economic_stage,
            durability_harness_path=bad_harness,
            regression_receipt_path=evidence.direct["regression"],
            output_dir=tmp_path / "fictitious-nodeid-strict",
            context=evidence.context,
        )


def test_stability_freeze_rejects_commit_blob_different_from_probe_source(
    evidence: Evidence,
) -> None:
    runtime = evidence.context.repository_root / subject.DURABILITY_RUNTIME_SOURCE_FILES[0]
    original = runtime.read_bytes()
    runtime.write_bytes(b"# changed only in frozen commit\n")
    _git(
        evidence.context.repository_root,
        "add",
        str(runtime.relative_to(evidence.context.repository_root)),
    )
    _git(evidence.context.repository_root, "commit", "-m", "mutate frozen source")
    bad_commit = _git(evidence.context.repository_root, "rev-parse", "HEAD")
    bad_tag = "f05-owner-buy-e3-mutated"
    _git(evidence.context.repository_root, "tag", "-a", bad_tag, "-m", "mutated")
    runtime.write_bytes(original)
    bad_context = subject.StabilityContext(
        repository_root=evidence.context.repository_root,
        execution_commit=bad_commit,
        execution_tag=bad_tag,
        layer4_contract_path=evidence.context.layer4_contract_path,
        layer4_day_receipt_dir=evidence.context.layer4_day_receipt_dir,
    )

    with pytest.raises(subject.StabilityReceiptError, match="frozen source hash drifted"):
        subject.validate_source_receipt(
            "durability_concurrency_cache",
            evidence.sources["durability_concurrency_cache"],
            bad_context,
        )


def test_stability_source_hash_detects_same_fd_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "mutable.py"
    source.write_bytes(b"x" * (2 << 20))
    original_read = subject.os.read
    mutated = False

    def mutating_read(descriptor: int, size: int) -> bytes:
        nonlocal mutated
        chunk = original_read(descriptor, size)
        if chunk and not mutated:
            mutated = True
            with source.open("ab") as handle:
                handle.write(b"changed")
                handle.flush()
                os.fsync(handle.fileno())
        return chunk

    monkeypatch.setattr(subject.os, "read", mutating_read)
    with pytest.raises(subject.StabilityReceiptError, match="changed while hashing"):
        subject._file_sha256(source, label="mutation fixture")  # noqa: SLF001


def test_durability_rejects_runtime_source_hash_drift(evidence: Evidence) -> None:
    runtime = evidence.context.repository_root / subject.DURABILITY_RUNTIME_SOURCE_FILES[0]
    runtime.write_text("# changed runtime source\n", encoding="ascii")

    with pytest.raises(subject.StabilityReceiptError, match="source hash drifted"):
        subject.validate_source_receipt(
            "durability_concurrency_cache",
            evidence.sources["durability_concurrency_cache"],
            evidence.context,
        )


def test_handcrafted_durability_booleans_are_not_accepted(
    evidence: Evidence,
    tmp_path: Path,
) -> None:
    handcrafted = _write_private_json(
        tmp_path / "handcrafted.json",
        {
            "schema_version": subject.DURABILITY_SOURCE_SCHEMA,
            "identity": subject.OWNER_IDENTITY,
            "status": "durability_concurrency_cache_complete",
            "checks": {name: True for name in subject.DURABILITY_CHECKS},
            "failure_counts": {name: 0 for name in subject.DURABILITY_FAILURE_COUNTS},
            "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
            "permissions": dict(subject.PERMISSIONS),
        },
    )

    with pytest.raises(subject.StabilityReceiptError, match="underlying receipts"):
        subject.validate_source_receipt(
            "durability_concurrency_cache",
            handcrafted,
            evidence.context,
        )


@pytest.mark.parametrize(
    ("role", "mutate", "message"),
    (
        ("single_day", lambda value: value.update(opportunity_count=80), "single-day"),
        (
            "all_fold_zero_economic",
            lambda value: value.update(day_count=29),
            "zero-economic",
        ),
        (
            "durability_concurrency_cache",
            lambda value: value["checks"].update(atomic_cache_publish=False),
            "durability",
        ),
        (
            "parity_layer1",
            lambda value: value["evidence"].update(mismatch_count=1),
            "Layer1",
        ),
        (
            "parity_layer2",
            lambda value: value["evidence"].update(action_duration_mismatch_count=1),
            "Layer2",
        ),
        (
            "parity_layer3",
            lambda value: value["evidence"].update(gap_reset_count=1),
            "Layer3",
        ),
        (
            "parity_layer4",
            lambda value: value["evidence"].update(day_count=29),
            "Layer4",
        ),
        ("sell54", lambda value: value["evidence"].update(mismatch_count=1), "SELL54"),
        ("regression", lambda value: value.update(failed=1), "regression"),
    ),
)
def test_each_role_fails_closed_before_any_wrapper_is_written(
    evidence: Evidence,
    role: str,
    mutate: Any,
    message: str,
) -> None:
    payload = _read(evidence.sources[role])
    mutate(payload)
    _write_private_json(evidence.sources[role], payload)

    with pytest.raises(subject.StabilityReceiptError, match=message):
        subject.build_stability_wrappers(
            source_receipts=evidence.sources,
            output_dir=evidence.output,
            context=evidence.context,
        )

    assert not evidence.output.exists()


def test_missing_role_fails_closed(evidence: Evidence) -> None:
    sources = dict(evidence.sources)
    sources.pop("regression")

    with pytest.raises(subject.StabilityReceiptError, match="missing=.*regression"):
        subject.build_stability_wrappers(
            source_receipts=sources,
            output_dir=evidence.output,
            context=evidence.context,
        )


def test_source_requires_mode_0600_and_self_verifying_canonical_hash(
    evidence: Evidence,
) -> None:
    source = evidence.sources["single_day"]
    source.chmod(0o644)
    with pytest.raises(subject.StabilityReceiptError, match="private receipt"):
        subject.validate_source_receipt("single_day", source, evidence.context)

    source.chmod(0o600)
    payload = _read(source)
    payload["canonical_receipt_sha256"] = "f" * 64
    source.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    source.chmod(0o600)
    with pytest.raises(subject.StabilityReceiptError, match="single-day provenance"):
        subject.validate_source_receipt("single_day", source, evidence.context)


def test_wrapper_canonical_tamper_is_rejected(evidence: Evidence) -> None:
    subject.build_stability_wrappers(
        source_receipts=evidence.sources,
        output_dir=evidence.output,
        context=evidence.context,
    )
    wrapper = evidence.output / "single_day.json"
    payload = _read(wrapper)
    payload["status"] = "stability_rejected"
    wrapper.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    wrapper.chmod(0o600)

    with pytest.raises(subject.StabilityReceiptError, match="wrapper identity"):
        subject.validate_stability_wrapper(
            wrapper,
            expected_role="single_day",
            context=evidence.context,
        )


def _context_cli_args(context: subject.StabilityContext) -> list[str]:
    return [
        "--repository-root",
        str(context.repository_root),
        "--execution-commit",
        context.execution_commit,
        "--execution-tag",
        context.execution_tag,
        "--layer4-contract",
        str(context.layer4_contract_path),
        "--layer4-day-receipt-dir",
        str(context.layer4_day_receipt_dir),
    ]


def test_materialize_and_build_cli_then_validate(
    evidence: Evidence,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    strict = tmp_path / "cli-strict"
    wrappers = tmp_path / "cli-wrappers"
    direct_args = [
        value
        for role in subject.DIRECT_SOURCE_ROLES
        for value in ("--source", f"{role}={evidence.direct[role]}")
    ]
    result = subject.main(
        [
            "materialize-and-build",
            *_context_cli_args(evidence.context),
            *direct_args,
            "--single-day-stage",
            str(evidence.single_day_stage),
            "--zero-economic-stage",
            str(evidence.zero_economic_stage),
            "--durability-harness",
            str(evidence.durability_harness),
            "--strict-source-dir",
            str(strict),
            "--output-dir",
            str(wrappers),
        ]
    )
    assert result == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["economic_values_exposed"] is False
    assert summary["validation_read"] is False
    assert set(summary["roles"]) == set(subject.REQUIRED_ROLES)

    wrapper_args = [
        value
        for role in subject.REQUIRED_ROLES
        for value in ("--wrapper", f"{role}={wrappers / f'{role}.json'}")
    ]
    assert (
        subject.main(
            [
                "validate",
                *_context_cli_args(evidence.context),
                *wrapper_args,
            ]
        )
        == 0
    )
    validated = json.loads(capsys.readouterr().out)
    assert validated["status"] == "stability_wrappers_verified"


def test_direct_script_cli_resolves_the_current_worktree() -> None:
    script = Path(subject.__file__).resolve()
    completed = subprocess.run(
        [sys.executable, str(script), "materialize-and-build", "--help"],
        cwd=script.parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    assert "--durability-harness" in completed.stdout
    assert "--strict-source-dir" in completed.stdout
