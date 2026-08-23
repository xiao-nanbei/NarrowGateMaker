from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import pytest

from scripts import deploy_f05_buy_e3_owner_v1 as deploy
from scripts import f05_buy_e3_active_release as subject
from strategy import boolean_cooldown_buy_e3 as buy_runtime


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_document(path: Path, payload: dict, canonical_field: str) -> Path:
    payload.pop(canonical_field, None)
    payload[canonical_field] = subject.document_sha256(payload, canonical_field)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)
    return path


def _rewrite_document(path: Path, mutate) -> dict:
    payload = json.loads(path.read_text(encoding="ascii"))
    mutate(payload)
    role = next(
        role
        for role, contract in subject._CONTRACTS.items()  # noqa: SLF001
        if payload.get("schema_version") == contract.schema
        and (contract.identity is None or payload.get("identity") == contract.identity)
    )
    _write_document(path, payload, subject._CONTRACTS[role].canonical_field)  # noqa: SLF001
    return payload


def _artifact_documents(root: Path) -> tuple[dict[str, Path], dict[str, str]]:
    ordering = "predicate::test::mid_ordering"
    campaign_age = buy_runtime.DIRECT_CAMPAIGN_AGE
    training_days = [
        (date(2026, 7, 1) + timedelta(days=index)).isoformat()
        for index in range(subject.composition_contract.EXPECTED_DAY_COUNT)
    ]
    predicate = {
        "schema_version": f"{subject.OWNER_IDENTITY}.selected_predicate_bundle.v1",
        "identity": subject.OWNER_IDENTITY,
        "side": "BUY",
        "selected_profile": buy_runtime.SELECTED_PROFILE,
        "selected_candidate": buy_runtime.SELECTED_CANDIDATE,
        "ema_half_lives_s": list(buy_runtime.EMA_HALF_LIVES_S),
        "ema_pairs_s": [list(pair) for pair in buy_runtime.EMA_PAIRS_S],
        "ema_pair_count": 45,
        "direct_predicates": [
            {
                "name": campaign_age,
                "kind": "campaign_age_gt_baseline_duration",
                "source_field": "campaign_age_s",
                "clock_group": "context",
            }
        ],
        "predicate_columns": sorted((campaign_age, ordering)),
        "definitions": [
            {
                "name": ordering,
                "block": "M1",
                "clock_group": "book",
                "kind": "preserved_tri",
                "source_field": "tri::mid_usdc_per_btc__h0p5s__h1s::positive_ordering",
                "threshold": None,
                "quantile": None,
                "category": None,
            }
        ],
        "normalization_source": {
            "source_sha256": "1" * 64,
            "reference_days_are_2025": True,
        },
        "uses_trade_predicates": False,
        "uses_depth_predicates": False,
        "uses_m2_incremental_features": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    predicate_path = _write_document(root / "predicate_bundle.json", predicate, "canonical_sha256")
    predicate_payload = json.loads(predicate_path.read_text(encoding="ascii"))
    predicate_file_sha = _file_sha256(predicate_path)
    predicate_canonical = predicate_payload["canonical_sha256"]

    policy = {
        "schema_version": f"{subject.OWNER_IDENTITY}.artifact.v1",
        "identity": subject.OWNER_IDENTITY,
        "status": "owner_refit_frozen_not_self_confirmed",
        "side": "BUY",
        "selected_profile": buy_runtime.SELECTED_PROFILE,
        "selected_candidate": buy_runtime.SELECTED_CANDIDATE,
        "random_seed": 17,
        "training_days": training_days,
        "training_row_sha256": "2" * 64,
        "training_label_request_sha256": "3" * 64,
        "training_label_receipt_sha256": "4" * 64,
        "training_label_payload_sha256": "5" * 64,
        "fitted_candidate_sha256": "6" * 64,
        "implementation_sha256": "7" * 64,
        "predicate_bundle_file_sha256": predicate_file_sha,
        "predicate_bundle_canonical_sha256": predicate_canonical,
        "policy_semantic_sha256": "8" * 64,
        "policy": {
            "identity": "fixture_e3_boolean_learner",
            "side": "BUY",
            "ordered_first_match_rules": [
                {
                    "action": "FIXED_2048S",
                    "clauses": [
                        {
                            "literals": [
                                {"predicate": campaign_age, "negated": False},
                                {"predicate": ordering, "negated": False},
                            ]
                        }
                    ],
                },
                {
                    "action": "FIXED_79S",
                    "clauses": [
                        {
                            "literals": [
                                {"predicate": campaign_age, "negated": True},
                                {"predicate": ordering, "negated": False},
                            ]
                        }
                    ],
                },
            ],
            "default_action": "CONTROL_85N",
            "permissions": {"action_authorized": False, "live_authorized": False},
        },
        "feature_pool_audit": {},
        "fit_audit": {},
        "semantic_audit": {},
        "runtime_contract": {
            "surface": "BUY_exposure_increasing_fill_callback_only",
            "fixed_action_is_total_cooldown": True,
            "control_is_85_seconds_times_consecutive_fill_units": True,
            "fallback_action": "CONTROL_85N",
            "reducing_buy_unchanged": True,
            "sell_owner_policy_unchanged": True,
            "warmup_requires_elapsed_time_and_all_selected_states_identified": True,
        },
        "bindings": {
            "owner_execution_commit": "9" * 40,
            "owner_execution_tag": "artifact-producer",
        },
        "permissions": dict(subject._ARTIFACT_PERMISSIONS),  # noqa: SLF001
        "evidence_boundary": {
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "research_supported": False,
            "owner_risk_accepted": True,
            "outcome_informed_owner_override": True,
            "learning_algorithm_oof_evidence_only": True,
            "old_oof_estimate_applies_to_exact_artifact": False,
            "exact_artifact_oof_available": False,
        },
    }
    policy_path = _write_document(root / "policy.json", policy, "canonical_sha256")
    policy_payload = json.loads(policy_path.read_text(encoding="ascii"))
    policy_file_sha = _file_sha256(policy_path)

    manifest = {
        "schema_version": f"{subject.OWNER_IDENTITY}.full_development_refit.v1",
        "identity": subject.OWNER_IDENTITY,
        "status": "exact_buy_e3_artifact_frozen",
        "policy_file": "policy.json",
        "policy_file_sha256": policy_file_sha,
        "policy_canonical_sha256": policy_payload["canonical_sha256"],
        "predicate_bundle_file": "predicate_bundle.json",
        "predicate_bundle_file_sha256": predicate_file_sha,
        "predicate_bundle_canonical_sha256": predicate_canonical,
        "fitted_candidate_sha256": "6" * 64,
        "label_materialization_receipt_sha256": "a" * 64,
        "cpp_one_shot_qualification_receipt_sha256": "b" * 64,
        "execution_preflight_receipt_sha256": "c" * 64,
        "implementation_sha256": "7" * 64,
        "training_days": training_days,
        "training_row_sha256": "2" * 64,
        "duration_vocabulary": [
            "CONTROL_85N",
            "FIXED_79S",
            "FIXED_173S",
            "FIXED_223S",
            "FIXED_356S",
            "FIXED_640S",
            "FIXED_709S",
            "FIXED_2048S",
        ],
        "default_action": "CONTROL_85N",
        "exact_final_artifact_oof_available": False,
        "research_supported": False,
        "owner_risk_accepted": True,
        "permissions": dict(subject._ARTIFACT_PERMISSIONS),  # noqa: SLF001
    }
    manifest_path = _write_document(root / "artifact_manifest.json", manifest, "artifact_sha256")
    manifest_payload = json.loads(manifest_path.read_text(encoding="ascii"))
    return (
        {
            "manifest": manifest_path,
            "policy": policy_path,
            "predicate_bundle": predicate_path,
        },
        {
            "artifact_sha256": manifest_payload["artifact_sha256"],
            "training_days": training_days,
            "manifest_file_sha256": _file_sha256(manifest_path),
            "policy_file_sha256": policy_file_sha,
            "policy_canonical_sha256": policy_payload["canonical_sha256"],
            "predicate_bundle_file_sha256": predicate_file_sha,
            "predicate_bundle_canonical_sha256": predicate_canonical,
        },
    )


def _resource_document(root: Path, artifact: dict, execution: dict) -> Path:
    payload = {
        "schema_version": subject.CONCURRENT_RESOURCE_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "concurrent_disabled_live_benchmark_passed",
        "generated_utc": "2026-08-23T00:00:00Z",
        "artifact_sha256": artifact["artifact_sha256"],
        "execution_commit": execution["execution_commit"],
        "execution_tag": execution["attempt_tag"],
        "host": {"logical_cpu_count": 2, "mem_total_mib": 2048.0},
        "sample_count": 2,
        "live_pid": 101,
        "live_pid_start_ticks": 202,
        "pre_process_identity_sha256": "d" * 64,
        "post_process_identity_sha256": "d" * 64,
        "benchmark_receipt_sha256": "e" * 64,
        "thresholds": {},
        "observed": {},
        "capture": {},
        "samples": [{}, {}],
        "checks": {"concurrent_capture_passed": True},
        "sample_series_sha256": "f" * 64,
        **subject._GATE_BOUNDARY,  # noqa: SLF001
    }
    return _write_document(root / "resource.json", payload, "canonical_resource_receipt_sha256")


def _regression_document(root: Path, artifact: dict, execution: dict) -> Path:
    payload = {
        "schema_version": subject.COMPATIBLE_REGRESSION_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "passed",
        "generated_utc": "2026-08-23T00:01:00Z",
        "artifact_sha256": artifact["artifact_sha256"],
        "execution_commit": execution["execution_commit"],
        "execution_tag": execution["attempt_tag"],
        "python_executable": "/fixture/python",
        "python_file_sha256": "1" * 64,
        "collect_command": ["collect"],
        "run_command": ["run"],
        "nodeids": ["tests/test_fixture.py::test_case"],
        "nodeid_manifest_sha256": "2" * 64,
        "nodeid_source_counts": {"tests/test_fixture.py": 1},
        "collected": 1,
        "executed": 1,
        "passed": 1,
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "collection_return_code": 0,
        "return_code": 0,
        "collection_stdout_sha256": "3" * 64,
        "collection_stderr_sha256": "4" * 64,
        "run_stdout_sha256": "5" * 64,
        "run_stderr_sha256": "6" * 64,
        "test_files": {"tests/test_fixture.py": "7" * 64},
        "runtime_sources": {"fixture.py": "8" * 64},
        "coverage": {"restart_and_rollback": True},
        **subject._GATE_BOUNDARY,  # noqa: SLF001
    }
    return _write_document(root / "regression.json", payload, "canonical_receipt_sha256")


def _sell_document(root: Path, artifact: dict) -> Path:
    payload = {
        "schema_version": subject.SELL54_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "parity_complete",
        "layer": "sell_owner_54_case_unchanged",
        "artifact_sha256": artifact["artifact_sha256"],
        "artifact_manifest_file_sha256": artifact["manifest_file_sha256"],
        "policy_file_sha256": artifact["policy_file_sha256"],
        "predicate_bundle_file_sha256": artifact["predicate_bundle_file_sha256"],
        "evidence": {
            "policy_sha256": "9" * 64,
            "predicate_bundle_sha256": "a" * 64,
            "predicate_columns": ["p0"],
            "sell_tri_state_cases": 27,
            "buy_tri_state_cases": 27,
            "mismatch_count": 0,
            "documented_semantics_equal": True,
            "runtime_binding_valid": True,
        },
        "economic_values_materialized_by_replay": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    return _write_document(root / "sell54.json", payload, "canonical_receipt_sha256")


def _binding(path: Path, canonical: str) -> dict:
    return {
        "path": str(path),
        "file_sha256": _file_sha256(path),
        "canonical_sha256": canonical,
    }


def _activation_document(
    root: Path,
    artifact: dict,
    execution: dict,
    resource_path: Path,
    regression_path: Path,
    sell_path: Path,
) -> Path:
    resource = json.loads(resource_path.read_text(encoding="ascii"))
    regression = json.loads(regression_path.read_text(encoding="ascii"))
    sell = json.loads(sell_path.read_text(encoding="ascii"))
    payload = {
        "schema_version": subject.ACTIVATION_ENVELOPE_SCHEMA,
        "status": "compatible_activation_evidence_complete",
        "plan_sha256": "b" * 64,
        "plan_core_sha256": "c" * 64,
        "transaction_contract_sha256": "d" * 64,
        "execution": {
            "execution_commit": execution["execution_commit"],
            "execution_tree": execution["execution_tree"],
            "annotated_tag": execution["attempt_tag"],
            "annotated_tag_object": execution["attempt_tag_object"],
        },
        "artifact": {
            "artifact_sha256": artifact["artifact_sha256"],
            "files": {
                "manifest": artifact["manifest_file_sha256"],
                "policy": artifact["policy_file_sha256"],
                "predicate_bundle": artifact["predicate_bundle_file_sha256"],
            },
        },
        "disabled_phase_receipt": {"process_identity_sha256": "e" * 64},
        "concurrent_resource_receipt": {
            **_binding(resource_path, resource["canonical_resource_receipt_sha256"]),
            "disabled_process_identity_sha256": "e" * 64,
            "live_pid": 101,
        },
        "runtime_regression_receipt": {
            **_binding(regression_path, regression["canonical_receipt_sha256"]),
            "nodeid_manifest_sha256": regression["nodeid_manifest_sha256"],
            "test_source_manifest_sha256": "f" * 64,
        },
        "sell_54_case_receipt": {
            **_binding(sell_path, sell["canonical_receipt_sha256"]),
            "sell_policy_sha256": "9" * 64,
        },
        "checks": {"all_prerequisites_passed": True},
        "activation_contract": {
            "restart_only": True,
            "same_disabled_process_required": True,
            "phase_token_still_required": True,
            "envelope_does_not_authorize_remote_mutation_by_itself": True,
        },
        "evidence_boundary": dict(subject._ACTIVATION_BOUNDARY),  # noqa: SLF001
    }
    return _write_document(
        root / "activation.json", payload, "canonical_activation_envelope_sha256"
    )


def _composition_document(
    root: Path,
    artifact: dict,
    execution: dict,
    resource_path: Path,
    regression_path: Path,
    sell_path: Path,
    activation_path: Path,
) -> Path:
    resource = json.loads(resource_path.read_text(encoding="ascii"))
    regression = json.loads(regression_path.read_text(encoding="ascii"))
    sell = json.loads(sell_path.read_text(encoding="ascii"))
    activation = json.loads(activation_path.read_text(encoding="ascii"))
    ordered_overrides = {
        "exact_artifact_manifest": (
            artifact["manifest_file_sha256"],
            artifact["artifact_sha256"],
            "artifact_sha256",
        ),
        "exact_policy": (
            artifact["policy_file_sha256"],
            artifact["policy_canonical_sha256"],
            "canonical_sha256",
        ),
        "exact_predicate_bundle": (
            artifact["predicate_bundle_file_sha256"],
            artifact["predicate_bundle_canonical_sha256"],
            "canonical_sha256",
        ),
        "compatible_execution_attempt": (
            "2" * 64,
            execution["attempt_manifest_canonical_sha256"],
            "canonical_execution_attempt_sha256",
        ),
        "compatible_runtime_regression": (
            _file_sha256(regression_path),
            regression["canonical_receipt_sha256"],
            "canonical_receipt_sha256",
        ),
        "compatible_concurrent_resource": (
            _file_sha256(resource_path),
            resource["canonical_resource_receipt_sha256"],
            "canonical_resource_receipt_sha256",
        ),
        "sell_54_case": (
            _file_sha256(sell_path),
            sell["canonical_receipt_sha256"],
            "canonical_receipt_sha256",
        ),
        "compatible_activation_envelope": (
            _file_sha256(activation_path),
            activation["canonical_activation_envelope_sha256"],
            "canonical_activation_envelope_sha256",
        ),
    }
    ordered: list[dict] = []
    for index, role in enumerate(subject.composition_contract.ordered_roles()):
        file_sha, canonical_sha, canonical_field = ordered_overrides.get(
            role,
            (
                f"{index + 1:064x}",
                f"{index + 101:064x}",
                "canonical_receipt_sha256",
            ),
        )
        ordered.append(
            {
                "role": role,
                "path": f"inputs/{role.replace('::', '-')}.json",
                "file_sha256": file_sha,
                "size_bytes": index + 1,
                "mode": "0600",
                "device": 1,
                "inode": index + 1,
                "schema_version": f"fixture.{role.replace('::', '_')}.v1",
                "identity": f"fixture_{role.replace('::', '_')}",
                "status": "passed",
                "status_source": "source_field",
                "canonical_field": canonical_field,
                "canonical_sha256": canonical_sha,
            }
        )
    wrapper_hashes = {
        role: f"{index + 201:064x}"
        for index, role in enumerate(subject.composition_contract.PRE_ADMISSION_RECEIPT_ROLES)
    }
    payload = {
        "schema_version": subject.FINAL_COMPOSITION_V2_SCHEMA,
        "identity": subject.FINAL_COMPOSITION_IDENTITY,
        "status": "owner_buy_e3_final_evidence_composed",
        "research_identity": subject.OWNER_IDENTITY,
        "amendment": {
            "preserves_v1_history": True,
            "research_identity_changed": False,
            "historical_v1_runtime_authority_reused": False,
            "ordinary_bugfix_attempt_only": True,
        },
        "formal_learning_algorithm": {
            "learning_algorithm_artifact_sha256": "1" * 64,
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
        },
        "producer_chain": {
            "execution_manifest_sha256": "2" * 64,
            "layer4_contract_sha256": "3" * 64,
            "layer4_final_sha256": "4" * 64,
            "ordered_day_count": 30,
        },
        "exact_artifact": {
            "artifact_sha256": artifact["artifact_sha256"],
            "manifest_file_sha256": artifact["manifest_file_sha256"],
            "policy_file_sha256": artifact["policy_file_sha256"],
            "predicate_bundle_file_sha256": artifact["predicate_bundle_file_sha256"],
            "training_days": artifact["training_days"],
            "exact_artifact_oof_available": False,
        },
        "compatible_execution_attempt": {
            "schema_version": f"{subject.OWNER_IDENTITY}.compatible_execution_attempt.v2",
            "identity": subject.OWNER_IDENTITY,
            "attempt_id": "attempt-fixture",
            "canonical_execution_attempt_sha256": execution["attempt_manifest_canonical_sha256"],
            "execution_commit": execution["execution_commit"],
            "execution_tree": execution["execution_tree"],
            "annotated_tag": execution["attempt_tag"],
            "annotated_tag_object": execution["attempt_tag_object"],
            "pre_admission_wrapper_canonical_sha256": wrapper_hashes,
        },
        "stability_wrapper_source_canonical_sha256": {
            role: f"{index + 301:064x}"
            for index, role in enumerate(subject.composition_contract.PRE_ADMISSION_RECEIPT_ROLES)
        },
        "current_authority_evidence": {
            "runtime_regression_sha256": regression["canonical_receipt_sha256"],
            "concurrent_resource_sha256": resource["canonical_resource_receipt_sha256"],
            "sell_54_case_sha256": sell["canonical_receipt_sha256"],
            "activation_envelope_sha256": activation["canonical_activation_envelope_sha256"],
            "plan_sha256": activation["plan_sha256"],
            "disabled_process_identity_sha256": activation["disabled_phase_receipt"][
                "process_identity_sha256"
            ],
        },
        "evidence_interpretation": {
            "research_supported": False,
            "owner_risk_accepted": True,
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
        },
        "authority": {"research": False, "action": False, "live": False},
        "evidence_boundary": dict(subject.composition_contract.OUTPUT_EVIDENCE_BOUNDARY),
        "permissions": dict(subject.composition_contract.OUTPUT_PERMISSIONS),
        "ordered_evidence": ordered,
        "ordered_evidence_sha256": subject.canonical_sha256(ordered),
    }
    return _write_document(
        root / "composition.json",
        payload,
        "canonical_final_composition_receipt_sha256",
    )


def _attempt_final_document(
    root: Path,
    artifact_paths: dict[str, Path],
    artifact: dict,
    execution: dict,
    composition_path: Path,
) -> Path:
    composition = json.loads(composition_path.read_text(encoding="ascii"))
    artifact_binding = {
        "artifact_sha256": artifact["artifact_sha256"],
        "files": {
            role: {
                "path": str(path),
                "file_sha256": _file_sha256(path),
                "size_bytes": path.stat().st_size,
                "device": path.stat().st_dev,
                "inode": path.stat().st_ino,
            }
            for role, path in artifact_paths.items()
        },
        "formal_manifest": {
            "path": str(root / "producer-manifest.json"),
            "file_sha256": "0" * 64,
            "size_bytes": 1,
            "device": 1,
            "inode": 1,
            "canonical_sha256": "1" * 64,
        },
    }
    payload = {
        "schema_version": subject.COMPATIBLE_ATTEMPT_FINAL_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "attempt_id": "attempt-fixture",
        "status": "compatible_runtime_results_bound",
        "generated_utc": "2026-08-23T00:02:00Z",
        "attempt_manifest": {
            "path": str(root / "attempt-manifest.json"),
            "file_sha256": "2" * 64,
            "size_bytes": 1,
            "canonical_sha256": execution["attempt_manifest_canonical_sha256"],
        },
        "runtime_execution": {
            "execution_commit": execution["execution_commit"],
            "execution_tree": execution["execution_tree"],
            "annotated_tag": execution["attempt_tag"],
            "annotated_tag_object": execution["attempt_tag_object"],
            "tag_peeled_commit": execution["execution_commit"],
        },
        "artifact": {
            "binding": artifact_binding,
            "canonical_sha256": subject.canonical_sha256(artifact_binding),
        },
        "composition_evidence_root": str(root),
        "result_receipts": {
            "final_composition": {
                "path": str(composition_path),
                "file_sha256": _file_sha256(composition_path),
                "size_bytes": composition_path.stat().st_size,
                "mode": "0600",
                "schema_version": subject.FINAL_COMPOSITION_V2_SCHEMA,
                "identity": subject.FINAL_COMPOSITION_IDENTITY,
                "status": "owner_buy_e3_final_evidence_composed",
                "canonical_field": "canonical_final_composition_receipt_sha256",
                "canonical_sha256": composition["canonical_final_composition_receipt_sha256"],
            }
        },
        "permissions": dict(subject._ATTEMPT_PERMISSIONS),  # noqa: SLF001
        "evidence_boundary": dict(subject._ATTEMPT_BOUNDARY),  # noqa: SLF001
    }
    return _write_document(root / "attempt-final.json", payload, "canonical_final_receipt_sha256")


@pytest.fixture
def release_fixture(tmp_path: Path) -> dict[str, object]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "NarrowGate Test")
    _git(repo, "config", "user.email", "narrowgate@example.invalid")
    (repo / "source.py").write_text("FROZEN = True\n", encoding="ascii")
    _git(repo, "add", "source.py")
    _git(repo, "commit", "-m", "frozen runtime")
    commit = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    _git(repo, "tag", "-a", "attempt-tag", "-m", "attempt")
    attempt_tag_object = _git(repo, "rev-parse", "refs/tags/attempt-tag")
    _git(repo, "tag", "-a", "operational-tag", "-m", "operational")

    artifact_root = tmp_path / "artifact"
    artifact_root.mkdir()
    artifact_paths, artifact = _artifact_documents(artifact_root)
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    execution = {
        "execution_commit": commit,
        "execution_tree": tree,
        "attempt_tag": "attempt-tag",
        "attempt_tag_object": attempt_tag_object,
        "attempt_manifest_canonical_sha256": "a" * 64,
    }
    resource = _resource_document(evidence_root, artifact, execution)
    regression = _regression_document(evidence_root, artifact, execution)
    sell = _sell_document(evidence_root, artifact)
    activation = _activation_document(
        evidence_root,
        artifact,
        execution,
        resource,
        regression,
        sell,
    )
    composition = _composition_document(
        evidence_root,
        artifact,
        execution,
        resource,
        regression,
        sell,
        activation,
    )
    attempt_final = _attempt_final_document(
        evidence_root,
        artifact_paths,
        artifact,
        execution,
        composition,
    )
    return {
        "repo": repo,
        "tag": "operational-tag",
        "artifact_paths": artifact_paths,
        "artifact": artifact,
        "evidence_paths": {
            "final_composition": composition,
            "compatible_attempt_final": attempt_final,
            "concurrent_resource": resource,
            "runtime_regression": regression,
            "sell54": sell,
            "activation_envelope": activation,
        },
    }


def _build(fixture: dict[str, object]) -> dict:
    return subject.build_active_release(
        repository_root=fixture["repo"],
        annotated_operational_tag=fixture["tag"],
        artifact_paths=fixture["artifact_paths"],
        evidence_paths=fixture["evidence_paths"],
        generated_utc="2026-08-23T01:00:00Z",
    )


def _finalize(fixture: dict[str, object], output: Path) -> tuple[dict, str]:
    return subject.finalize_active_release(
        repository_root=fixture["repo"],
        annotated_operational_tag=fixture["tag"],
        artifact_paths=fixture["artifact_paths"],
        evidence_paths=fixture["evidence_paths"],
        output_path=output,
        generated_utc="2026-08-23T01:00:00Z",
    )


def test_finalize_and_validate_round_trip_is_private_and_owner_authorized(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    output = tmp_path / "active-release.json"
    payload, file_hash = _finalize(release_fixture, output)

    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert file_hash == _file_sha256(output)
    assert payload["schema_version"] == subject.ACTIVE_RELEASE_SCHEMA
    assert payload["identity"] == subject.ACTIVE_RELEASE_IDENTITY
    assert payload["status"] == subject.ACTIVE_RELEASE_STATUS
    assert payload["research_supported"] is False
    assert payload["owner_risk_accepted"] is True
    assert payload["action_authorized"] is True
    assert payload["live_authorized"] is True
    assert payload["scope"]["side"] == "BUY"
    assert payload["scope"]["trigger"] == "exposure_increasing_executed_fill"
    assert payload["scope"]["output"] == "total_cooldown"
    assert (
        subject.validate_active_release(output, repository_root=release_fixture["repo"]) == payload
    )


def test_portable_grant_validates_and_loads_without_source_evidence(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    source_release = tmp_path / "source-active-release.json"
    payload, _ = _finalize(release_fixture, source_release)
    for section in (payload["exact_artifact"]["roles"], payload["evidence"]):
        for binding in section.values():
            assert not Path(binding["path"]).is_absolute()
            assert binding["device"] is None
            assert binding["inode"] is None

    portable_root = tmp_path / "portable"
    portable_root.mkdir()
    portable_release = portable_root / "active-release.json"
    shutil.copyfile(source_release, portable_release)
    portable_release.chmod(0o600)
    copied_artifacts: dict[str, Path] = {}
    for role, source in release_fixture["artifact_paths"].items():
        target = portable_root / source.name
        shutil.copyfile(source, target)
        target.chmod(0o600)
        copied_artifacts[role] = target

    assert not (portable_root / "evidence").exists()
    assert (
        subject.validate_active_release(
            portable_release,
            repository_root=release_fixture["repo"],
        )
        == payload
    )
    assert (
        deploy._validate_installed_active_release_file(  # noqa: SLF001
            portable_release,
            expected_file_sha256=_file_sha256(portable_release),
            expected_canonical_sha256=payload["canonical_active_release_sha256"],
            expected_execution_commit=payload["execution"]["execution_commit"],
            expected_execution_tree=payload["execution"]["execution_tree"],
            expected_artifact_sha256=payload["exact_artifact"]["artifact_sha256"],
        )
        == payload
    )
    policy = buy_runtime.LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=copied_artifacts["manifest"],
        artifact_manifest_sha256=_file_sha256(copied_artifacts["manifest"]),
        expected_artifact_sha256=release_fixture["artifact"]["artifact_sha256"],
        policy_path=copied_artifacts["policy"],
        policy_sha256=_file_sha256(copied_artifacts["policy"]),
        predicate_bundle_path=copied_artifacts["predicate_bundle"],
        predicate_bundle_sha256=_file_sha256(copied_artifacts["predicate_bundle"]),
        active_release_path=portable_release,
        active_release_file_sha256=_file_sha256(portable_release),
        active_release_canonical_sha256=payload["canonical_active_release_sha256"],
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    assert policy.artifact_sha256 == release_fixture["artifact"]["artifact_sha256"]


@pytest.mark.parametrize(
    ("field", "value"),
    [("path", "/private/source/evidence.json"), ("device", 1), ("inode", 1)],
)
def test_portable_grant_rejects_local_filesystem_identity(
    release_fixture: dict[str, object], tmp_path: Path, field: str, value: object
) -> None:
    output = tmp_path / "active-release.json"
    payload, _ = _finalize(release_fixture, output)
    tampered = deepcopy(payload)
    tampered["evidence"]["final_composition"][field] = value
    _write_document(output, tampered, "canonical_active_release_sha256")

    with pytest.raises(subject.ActiveReleaseError, match="portable binding drifted"):
        subject.validate_active_release(output, repository_root=release_fixture["repo"])


@pytest.mark.parametrize("role", subject.EVIDENCE_ROLES)
def test_ambiguous_or_near_match_status_fails_closed(
    release_fixture: dict[str, object], role: str
) -> None:
    path = release_fixture["evidence_paths"][role]
    if role == "final_composition":
        replacement = "not_owner_buy_e3_final_evidence_composed"
    elif role == "compatible_attempt_final":
        replacement = "compatible_runtime_results_bound_extra"
    elif role == "concurrent_resource":
        replacement = "not_concurrent_disabled_live_benchmark_passed"
    elif role == "runtime_regression":
        replacement = "not_passed"
    elif role == "sell54":
        replacement = "parity_complete_bypassed"
    else:
        replacement = "compatible_activation_evidence_complete_bypassed"
    _rewrite_document(path, lambda payload: payload.update(status=replacement))

    with pytest.raises(subject.ActiveReleaseError, match="exact status"):
        _build(release_fixture)


@pytest.mark.parametrize("collection", ["artifact_paths", "evidence_paths"])
@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_missing_or_extra_roles_are_rejected(
    release_fixture: dict[str, object], collection: str, mutation: str, tmp_path: Path
) -> None:
    changed = dict(release_fixture[collection])
    if mutation == "missing":
        changed.pop(next(iter(changed)))
    else:
        extra = tmp_path / "extra.json"
        extra.write_text("{}\n", encoding="ascii")
        extra.chmod(0o600)
        changed["extra_role"] = extra
    fixture = dict(release_fixture)
    fixture[collection] = changed

    with pytest.raises(subject.ActiveReleaseError, match="role set drifted"):
        _build(fixture)


def test_artifact_file_or_canonical_drift_is_rejected(release_fixture: dict[str, object]) -> None:
    manifest = release_fixture["artifact_paths"]["manifest"]
    _rewrite_document(
        manifest,
        lambda payload: payload.update(policy_file_sha256="0" * 64),
    )

    with pytest.raises(subject.ActiveReleaseError, match="artifact manifest contract"):
        _build(release_fixture)


def test_execution_commit_drift_in_attempt_is_rejected(release_fixture: dict[str, object]) -> None:
    attempt = release_fixture["evidence_paths"]["compatible_attempt_final"]
    _rewrite_document(
        attempt,
        lambda payload: payload["runtime_execution"].update(execution_commit="0" * 40),
    )

    with pytest.raises(subject.ActiveReleaseError, match="execution commit or tree"):
        _build(release_fixture)


def test_operational_tag_must_be_annotated_and_peel_to_head(
    release_fixture: dict[str, object],
) -> None:
    repo = release_fixture["repo"]
    _git(repo, "tag", "lightweight")
    changed = dict(release_fixture)
    changed["tag"] = "lightweight"

    with pytest.raises(subject.ActiveReleaseError, match="not annotated"):
        _build(changed)


def test_operational_tag_object_drift_is_detected_after_finalization(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    output = tmp_path / "active-release.json"
    _finalize(release_fixture, output)
    repo = release_fixture["repo"]
    _git(repo, "tag", "-d", release_fixture["tag"])
    _git(repo, "tag", "-a", release_fixture["tag"], "-m", "different object")

    with pytest.raises(subject.ActiveReleaseError, match="operational tag drifted"):
        subject.validate_active_release(output, repository_root=repo)


def test_symlink_and_non_private_input_are_rejected(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    resource = release_fixture["evidence_paths"]["concurrent_resource"]
    symlink = tmp_path / "resource-link.json"
    symlink.symlink_to(resource)
    evidence = dict(release_fixture["evidence_paths"])
    evidence["concurrent_resource"] = symlink
    changed = dict(release_fixture)
    changed["evidence_paths"] = evidence
    with pytest.raises(subject.ActiveReleaseError, match="symbolic link|opened safely"):
        _build(changed)

    resource.chmod(0o644)
    with pytest.raises(subject.ActiveReleaseError, match="private 0600"):
        _build(release_fixture)


def test_symlinked_parent_directory_is_rejected(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    evidence_root = release_fixture["evidence_paths"]["concurrent_resource"].parent
    parent_link = tmp_path / "evidence-link"
    parent_link.symlink_to(evidence_root, target_is_directory=True)
    evidence = dict(release_fixture["evidence_paths"])
    evidence["concurrent_resource"] = parent_link / "resource.json"
    changed = dict(release_fixture)
    changed["evidence_paths"] = evidence

    with pytest.raises(subject.ActiveReleaseError, match="must not traverse"):
        _build(changed)


def test_symlinked_output_parent_is_rejected_before_publish(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    real_parent = tmp_path / "real-output"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-output"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    output = linked_parent / "active-release.json"

    with pytest.raises(subject.ActiveReleaseError, match="symbolic link"):
        _finalize(release_fixture, output)

    assert not (real_parent / output.name).exists()


def test_missing_output_parent_is_not_created_implicitly(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    missing_parent = tmp_path / "missing" / "nested"

    with pytest.raises(subject.ActiveReleaseError, match="path component is missing"):
        _finalize(release_fixture, missing_parent / "active-release.json")

    assert not missing_parent.exists()


def test_path_swap_between_fd_read_and_path_recheck_is_rejected(
    release_fixture: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    resource = release_fixture["evidence_paths"]["concurrent_resource"]
    replacement = resource.with_name("resource-replacement.json")
    replacement.write_bytes(resource.read_bytes())
    replacement.chmod(0o600)
    original_fstat = subject.os.fstat
    calls = 0

    def swapping_fstat(descriptor: int):
        nonlocal calls
        observed = original_fstat(descriptor)
        calls += 1
        if calls == 1:
            resource.rename(resource.with_name("resource-original.json"))
            replacement.rename(resource)
        return observed

    monkeypatch.setattr(subject.os, "fstat", swapping_fstat)
    with pytest.raises(
        subject.ActiveReleaseError,
        match="changed while it was read|path was replaced",
    ):
        subject._open_document(resource, "resource")  # noqa: SLF001


def test_hard_link_and_inode_alias_are_rejected(
    release_fixture: dict[str, object], tmp_path: Path
) -> None:
    resource = release_fixture["evidence_paths"]["concurrent_resource"]
    hardlink = tmp_path / "resource-hardlink.json"
    os.link(resource, hardlink)

    with pytest.raises(subject.ActiveReleaseError, match="single-link"):
        _build(release_fixture)


def test_finalize_is_no_replace(release_fixture: dict[str, object], tmp_path: Path) -> None:
    output = tmp_path / "active-release.json"
    first, _ = _finalize(release_fixture, output)
    with pytest.raises(subject.ActiveReleaseError, match="already exists"):
        _finalize(release_fixture, output)
    assert json.loads(output.read_text(encoding="ascii")) == first


@pytest.mark.parametrize(
    ("section", "mutate", "message"),
    [
        (
            "rollback",
            lambda payload: payload["rollback"].update(
                buy_e3_enabled=True,
                buy_deadline_identity="E3",
                e3_deadline_imported=True,
            ),
            "rollback",
        ),
        (
            "evidence_boundary",
            lambda payload: payload["evidence_boundary"].update(validation_read=True),
            "evidence boundary",
        ),
        (
            "evidence_boundary",
            lambda payload: payload["evidence_boundary"].update(shadow_created=True),
            "evidence boundary",
        ),
    ],
)
def test_rollback_and_evidence_boundaries_fail_closed_after_tamper(
    release_fixture: dict[str, object],
    tmp_path: Path,
    section: str,
    mutate,
    message: str,
) -> None:
    del section
    output = tmp_path / "active-release.json"
    payload, _ = _finalize(release_fixture, output)
    tampered = deepcopy(payload)
    mutate(tampered)
    _write_document(output, tampered, "canonical_active_release_sha256")

    with pytest.raises(subject.ActiveReleaseError, match=message):
        subject.validate_active_release(output, repository_root=release_fixture["repo"])


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_release_binding_role_set_tamper_is_rejected(
    release_fixture: dict[str, object], tmp_path: Path, mutation: str
) -> None:
    output = tmp_path / "active-release.json"
    payload, _ = _finalize(release_fixture, output)
    tampered = deepcopy(payload)
    if mutation == "missing":
        tampered["evidence"].pop("sell54")
    else:
        tampered["evidence"]["extra_role"] = deepcopy(tampered["evidence"]["sell54"])
    _write_document(output, tampered, "canonical_active_release_sha256")

    with pytest.raises(subject.ActiveReleaseError, match="fields drifted"):
        subject.validate_active_release(output, repository_root=release_fixture["repo"])


def test_composition_v2_schema_is_frozen_and_extra_field_rejected(
    release_fixture: dict[str, object],
) -> None:
    composition = release_fixture["evidence_paths"]["final_composition"]
    _rewrite_document(composition, lambda payload: payload.update(unfrozen_field=True))

    with pytest.raises(subject.ActiveReleaseError, match="schema fields drifted"):
        _build(release_fixture)


def test_cli_build_finalize_and_validate(
    release_fixture: dict[str, object], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    artifact = release_fixture["artifact_paths"]
    evidence = release_fixture["evidence_paths"]
    common = [
        "--repository-root",
        str(release_fixture["repo"]),
        "--annotated-operational-tag",
        release_fixture["tag"],
        "--artifact-manifest",
        str(artifact["manifest"]),
        "--policy",
        str(artifact["policy"]),
        "--predicate-bundle",
        str(artifact["predicate_bundle"]),
        "--final-composition",
        str(evidence["final_composition"]),
        "--compatible-attempt-final",
        str(evidence["compatible_attempt_final"]),
        "--concurrent-resource",
        str(evidence["concurrent_resource"]),
        "--runtime-regression",
        str(evidence["runtime_regression"]),
        "--sell54",
        str(evidence["sell54"]),
        "--activation-envelope",
        str(evidence["activation_envelope"]),
    ]
    assert subject.main(["build", *common]) == 0
    built = json.loads(capsys.readouterr().out)
    assert built["schema_version"] == subject.ACTIVE_RELEASE_SCHEMA

    output = tmp_path / "cli-active-release.json"
    assert subject.main(["finalize", *common, "--output", str(output)]) == 0
    finalized_hashes = capsys.readouterr().out.strip().splitlines()
    assert len(finalized_hashes) == 2
    assert all(len(value) == 64 for value in finalized_hashes)
    assert (
        subject.main(
            [
                "validate",
                "--repository-root",
                str(release_fixture["repo"]),
                "--receipt",
                str(output),
            ]
        )
        == 0
    )
    assert len(capsys.readouterr().out.strip()) == 64
