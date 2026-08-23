from __future__ import annotations

import importlib.util
import json
import stat
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_amendment_v2 as subject,
)
from scripts import f05_buy_e3_active_release as active_release
from strategy import boolean_cooldown_buy_e3 as buy_runtime


def _legacy_fixture_module() -> ModuleType:
    name = "_f05_buy_e3_legacy_composition_fixture"
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    path = Path(__file__).with_name(
        "test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_v1.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("legacy composition fixture cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


LEGACY = _legacy_fixture_module()


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


class RuntimeArtifactEvidenceFixture(LEGACY.EvidenceFixture):
    """Historical evidence fixture with the exact runtime-consumable artifact leaf."""

    def _write(
        self,
        role: str,
        payload: dict[str, Any],
        canonical_field: str,
    ) -> dict[str, Any]:
        ordering = "predicate::test::mid_ordering"
        campaign_age = buy_runtime.DIRECT_CAMPAIGN_AGE
        if role == "exact_predicate_bundle":
            payload = {
                "schema_version": f"{subject.IDENTITY}.selected_predicate_bundle.v1",
                "identity": subject.IDENTITY,
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
                        "source_field": ("tri::mid_usdc_per_btc__h0p5s__h1s::positive_ordering"),
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
        elif role == "exact_policy":
            bundle_path = self.paths["exact_predicate_bundle"]
            bundle = self.documents["exact_predicate_bundle"]
            payload = {
                "schema_version": f"{subject.IDENTITY}.artifact.v1",
                "identity": subject.IDENTITY,
                "status": "owner_refit_frozen_not_self_confirmed",
                "side": "BUY",
                "selected_profile": buy_runtime.SELECTED_PROFILE,
                "selected_candidate": buy_runtime.SELECTED_CANDIDATE,
                "random_seed": 17,
                "training_days": list(self.days),
                "training_row_sha256": "2" * 64,
                "training_label_request_sha256": "3" * 64,
                "training_label_receipt_sha256": "4" * 64,
                "training_label_payload_sha256": "5" * 64,
                "fitted_candidate_sha256": "6" * 64,
                "implementation_sha256": "7" * 64,
                "predicate_bundle_file_sha256": subject.hashlib.sha256(
                    bundle_path.read_bytes()
                ).hexdigest(),
                "predicate_bundle_canonical_sha256": bundle["canonical_sha256"],
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
                "permissions": LEGACY._permissions(),
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
        elif role == "exact_artifact_manifest":
            policy_path = self.paths["exact_policy"]
            policy = self.documents["exact_policy"]
            bundle_path = self.paths["exact_predicate_bundle"]
            bundle = self.documents["exact_predicate_bundle"]
            payload = {
                "schema_version": f"{subject.IDENTITY}.full_development_refit.v1",
                "identity": subject.IDENTITY,
                "status": "exact_buy_e3_artifact_frozen",
                "policy_file": "policy.json",
                "policy_file_sha256": subject.hashlib.sha256(policy_path.read_bytes()).hexdigest(),
                "policy_canonical_sha256": policy["canonical_sha256"],
                "predicate_bundle_file": "predicate_bundle.json",
                "predicate_bundle_file_sha256": subject.hashlib.sha256(
                    bundle_path.read_bytes()
                ).hexdigest(),
                "predicate_bundle_canonical_sha256": bundle["canonical_sha256"],
                "fitted_candidate_sha256": "6" * 64,
                "label_materialization_receipt_sha256": payload[
                    "label_materialization_receipt_sha256"
                ],
                "cpp_one_shot_qualification_receipt_sha256": payload[
                    "cpp_one_shot_qualification_receipt_sha256"
                ],
                "execution_preflight_receipt_sha256": payload["execution_preflight_receipt_sha256"],
                "implementation_sha256": "7" * 64,
                "training_days": list(self.days),
                "training_row_sha256": "2" * 64,
                "duration_vocabulary": list(buy_runtime.BUY_ACTIONS),
                "default_action": "CONTROL_85N",
                "exact_final_artifact_oof_available": False,
                "research_supported": False,
                "owner_risk_accepted": True,
                "permissions": LEGACY._permissions(),
            }
        return super()._write(role, payload, canonical_field)


def _write_document(path: Path, payload: dict[str, Any], canonical_field: str) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    document = dict(payload)
    document[canonical_field] = subject.document_sha256(document, canonical_field)
    path.write_text(
        json.dumps(document, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)
    return document


def _rewrite(path: Path, payload: dict[str, Any], canonical_field: str) -> None:
    body = dict(payload)
    body[canonical_field] = subject.document_sha256(body, canonical_field)
    path.write_text(
        json.dumps(body, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)


def _file_binding(path: Path, payload: dict[str, Any], canonical_field: str) -> dict[str, Any]:
    return {
        "path": str(path),
        "file_sha256": subject.hashlib.sha256(path.read_bytes()).hexdigest(),
        "size_bytes": path.stat().st_size,
        "mode": "0600",
        "schema_version": payload["schema_version"],
        "identity": payload["identity"],
        "status": payload["status"],
        "canonical_field": canonical_field,
        "canonical_sha256": payload[canonical_field],
    }


def _artifact_files(legacy: Any) -> dict[str, str]:
    return {
        "manifest": subject.hashlib.sha256(
            legacy.paths["exact_artifact_manifest"].read_bytes()
        ).hexdigest(),
        "policy": subject.hashlib.sha256(legacy.paths["exact_policy"].read_bytes()).hexdigest(),
        "predicate_bundle": subject.hashlib.sha256(
            legacy.paths["exact_predicate_bundle"].read_bytes()
        ).hexdigest(),
    }


@dataclass(slots=True)
class V2Evidence:
    root: Path
    repository_root: Path
    legacy: Any
    paths: dict[str, Path]
    documents: dict[str, dict[str, Any]]
    wrappers: dict[str, Path]
    output: Path
    sell_result: dict[str, Any]

    @classmethod
    def build(cls, root: Path, repository_root: Path) -> V2Evidence:
        root = root.resolve()
        legacy = RuntimeArtifactEvidenceFixture(root)
        paths: dict[str, Path] = dict(legacy.paths)
        documents: dict[str, dict[str, Any]] = dict(legacy.documents)
        current = root / "current"
        artifact_sha = documents["exact_artifact_manifest"]["artifact_sha256"]
        artifact_files = _artifact_files(legacy)
        runtime = {
            "execution_commit": _git(repository_root, "rev-parse", "HEAD"),
            "execution_tree": _git(repository_root, "rev-parse", "HEAD^{tree}"),
            "annotated_tag": "f05-owner-buy-e3-compatible-attempt-test",
            "annotated_tag_object": _git(
                repository_root,
                "rev-parse",
                "refs/tags/f05-owner-buy-e3-compatible-attempt-test",
            ),
            "tag_peeled_commit": _git(repository_root, "rev-parse", "HEAD"),
        }

        regression_path = current / "regression.json"
        regression = _write_document(
            regression_path,
            {
                "schema_version": subject.gate.COMPATIBLE_REGRESSION_SCHEMA,
                "identity": subject.IDENTITY,
                "status": "passed",
                "generated_utc": "2026-08-23T00:01:00Z",
                "artifact_sha256": artifact_sha,
                "execution_commit": runtime["execution_commit"],
                "execution_tag": runtime["annotated_tag"],
                "python_executable": "/fixture/python",
                "python_file_sha256": "0" * 64,
                "collect_command": ["collect"],
                "run_command": ["run"],
                "nodeids": ["tests/test_runtime.py::test_runtime"],
                "nodeid_manifest_sha256": "1" * 64,
                "nodeid_source_counts": {"tests/test_runtime.py": 1},
                "collected": 1,
                "executed": 1,
                "passed": 1,
                "failed": 0,
                "errors": 0,
                "skipped": 0,
                "collection_return_code": 0,
                "return_code": 0,
                "collection_stdout_sha256": "4" * 64,
                "collection_stderr_sha256": "5" * 64,
                "run_stdout_sha256": "6" * 64,
                "run_stderr_sha256": "7" * 64,
                "test_files": {"tests/test_runtime.py": "2" * 64},
                "runtime_sources": {"live/main.py": "3" * 64},
                "coverage": {"restart_and_rollback": True},
                "economic_values_persisted": False,
                "hypothetical_live_actions_scored": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
            "canonical_receipt_sha256",
        )
        paths["compatible_runtime_regression"] = regression_path
        documents["compatible_runtime_regression"] = regression

        resource_path = current / "resource.json"
        resource = _write_document(
            resource_path,
            {
                "schema_version": subject.gate.CONCURRENT_RESOURCE_SCHEMA,
                "identity": subject.IDENTITY,
                "status": "concurrent_disabled_live_benchmark_passed",
                "generated_utc": "2026-08-23T00:00:00Z",
                "artifact_sha256": artifact_sha,
                "execution_commit": runtime["execution_commit"],
                "execution_tag": runtime["annotated_tag"],
                "host": {"logical_cpu_count": 2, "mem_total_mib": 2048.0},
                "sample_count": 2,
                "live_pid": 1234,
                "live_pid_start_ticks": 5678,
                "pre_process_identity_sha256": "8" * 64,
                "post_process_identity_sha256": "8" * 64,
                "benchmark_receipt_sha256": "9" * 64,
                "thresholds": {},
                "observed": {},
                "capture": {},
                "samples": [{}, {}],
                "checks": {"concurrent_capture_passed": True},
                "sample_series_sha256": "a" * 64,
                "economic_values_persisted": False,
                "hypothetical_live_actions_scored": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
            "canonical_resource_receipt_sha256",
        )
        paths["compatible_concurrent_resource"] = resource_path
        documents["compatible_concurrent_resource"] = resource

        sell_path = current / "sell54.json"
        sell = _write_document(
            sell_path,
            {
                "schema_version": subject.gate.SELL_PARITY_SCHEMA,
                "identity": subject.IDENTITY,
                "status": "parity_complete",
                "layer": "sell_owner_54_case_unchanged",
                "artifact_sha256": artifact_sha,
                "artifact_manifest_file_sha256": artifact_files["manifest"],
                "policy_file_sha256": artifact_files["policy"],
                "predicate_bundle_file_sha256": artifact_files["predicate_bundle"],
                "evidence": {
                    "policy_sha256": "6" * 64,
                    "predicate_bundle_sha256": "7" * 64,
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
            },
            "canonical_receipt_sha256",
        )
        paths["sell_54_case"] = sell_path
        documents["sell_54_case"] = sell

        source_by_role: dict[str, tuple[Path, dict[str, Any], str]] = {}
        source_specs = {
            "single_day": (
                subject.stability.SINGLE_DAY_SOURCE_SCHEMA,
                "exact_owner_one_day_mechanics_complete",
            ),
            "all_fold_zero_economic": (
                subject.stability.ZERO_ECONOMIC_SOURCE_SCHEMA,
                "all_fold_zero_economic_contract_walk_complete",
            ),
            "durability_concurrency_cache": (
                subject.stability.DURABILITY_SOURCE_SCHEMA,
                "durability_concurrency_cache_complete",
            ),
        }
        for role, (schema, status_value) in source_specs.items():
            source_path = current / f"{role}.source.json"
            source_payload = _write_document(
                source_path,
                {
                    "schema_version": schema,
                    "identity": subject.IDENTITY,
                    "status": status_value,
                    "economic_values_exposed": False,
                    "economic_values_used_for_selection": False,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                },
                "canonical_receipt_sha256",
            )
            source_by_role[role] = (source_path, source_payload, "canonical_receipt_sha256")
        for wrapper_role, direct_role in {
            "parity_layer1": "parity_research_compiled",
            "parity_layer2": "parity_development_snapshot",
            "parity_layer3": "parity_streaming_offline",
            "parity_layer4": "layer4_final",
            "sell54": "sell_54_case",
            "regression": "compatible_runtime_regression",
        }.items():
            direct_path = paths[direct_role]
            direct_payload = documents[direct_role]
            canonical_field = subject._canonical_field(direct_role)  # noqa: SLF001
            source_by_role[wrapper_role] = (direct_path, direct_payload, canonical_field)

        wrappers: dict[str, Path] = {}
        wrapper_documents: dict[str, dict[str, Any]] = {}
        for role in subject.attempt.PRE_ADMISSION_RECEIPT_ROLES:
            source_path, source_payload, canonical_field = source_by_role[role]
            wrapper_path = current / f"{role}.wrapper.json"
            wrapper = _write_document(
                wrapper_path,
                {
                    "schema_version": subject.attempt.PRE_ADMISSION_RECEIPT_WRAPPER_SCHEMA,
                    "identity": subject.IDENTITY,
                    "role": role,
                    "status": "passed",
                    "source_receipt": _file_binding(source_path, source_payload, canonical_field),
                    "evidence_boundary": dict(subject.attempt.PRE_ADMISSION_EVIDENCE_BOUNDARY),
                    "permissions": dict(subject.attempt.PRE_ADMISSION_PERMISSIONS),
                },
                "canonical_receipt_sha256",
            )
            wrappers[role] = wrapper_path
            wrapper_documents[role] = wrapper

        runtime_source_rows = {
            "sell_owner_runtime": {
                "repository_relative_path": "strategy/boolean_cooldown_live.py",
                "file_sha256": "4" * 64,
                "size_bytes": 1,
                "device": 1,
                "inode": 1,
            },
            "parity": {
                "repository_relative_path": (
                    "research/families/f05_fill_quality_quote_ev/audit/"
                    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1.py"
                ),
                "file_sha256": "5" * 64,
                "size_bytes": 1,
                "device": 1,
                "inode": 2,
            },
        }
        pre_admission = {
            role: _file_binding(wrappers[role], wrapper_documents[role], "canonical_receipt_sha256")
            for role in subject.attempt.PRE_ADMISSION_RECEIPT_ROLES
        }
        attempt_path = current / "compatible_attempt.json"
        compatible_attempt = _write_document(
            attempt_path,
            {
                "schema_version": subject.attempt.SCHEMA_VERSION,
                "identity": subject.IDENTITY,
                "attempt_id": "attempt-composition-test",
                "status": "compatible_runtime_frozen_not_activated",
                "generated_utc": "2026-08-23T00:00:00Z",
                "research_contract": subject.attempt._research_contract(),  # noqa: SLF001
                "artifact_producer_execution": {},
                "runtime_execution": runtime,
                "runtime_sources": {
                    "files": runtime_source_rows,
                    "canonical_sha256": subject.canonical_sha256(runtime_source_rows),
                },
                "artifact": {
                    "artifact_sha256": artifact_sha,
                    "files": {
                        role: {
                            "path": str(paths[source_role]),
                            "file_sha256": artifact_files[role],
                            "size_bytes": paths[source_role].stat().st_size,
                            "device": paths[source_role].stat().st_dev,
                            "inode": paths[source_role].stat().st_ino,
                        }
                        for role, source_role in {
                            "manifest": "exact_artifact_manifest",
                            "policy": "exact_policy",
                            "predicate_bundle": "exact_predicate_bundle",
                        }.items()
                    },
                    "formal_manifest": {
                        "path": str(paths["attempt_execution_manifest"]),
                        "file_sha256": subject.hashlib.sha256(
                            paths["attempt_execution_manifest"].read_bytes()
                        ).hexdigest(),
                        "size_bytes": paths["attempt_execution_manifest"].stat().st_size,
                        "device": paths["attempt_execution_manifest"].stat().st_dev,
                        "inode": paths["attempt_execution_manifest"].stat().st_ino,
                        "canonical_sha256": documents["attempt_execution_manifest"][
                            "canonical_execution_manifest_sha256"
                        ],
                    },
                },
                "stability_validation_context": {
                    "validator": subject.attempt.STABILITY_VALIDATOR_IDENTITY,
                    "repository_root": str(repository_root),
                    "execution_commit": runtime["execution_commit"],
                    "execution_tag": runtime["annotated_tag"],
                    "layer4_contract_path": str(paths["layer4_contract"]),
                    "layer4_day_receipt_dir": str(paths["layer4_day::00"].parent),
                },
                "pre_admission_evidence": pre_admission,
                "permissions": dict(subject.attempt.ATTEMPT_PERMISSIONS),
                "evidence_boundary": dict(subject.attempt.ATTEMPT_EVIDENCE_BOUNDARY),
            },
            "canonical_execution_attempt_sha256",
        )
        paths["compatible_execution_attempt"] = attempt_path
        documents["compatible_execution_attempt"] = compatible_attempt

        sell_result = {
            "path": str(sell_path),
            "file_sha256": subject.hashlib.sha256(sell_path.read_bytes()).hexdigest(),
            "canonical_receipt_sha256": sell["canonical_receipt_sha256"],
            "sell_policy_sha256": "6" * 64,
            "sell_predicate_bundle_sha256": "7" * 64,
            "source_files": {
                "strategy/boolean_cooldown_live.py": "4" * 64,
                (
                    "research/families/f05_fill_quality_quote_ev/audit/"
                    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1.py"
                ): "5" * 64,
            },
            "source_manifest_sha256": "8" * 64,
        }
        disabled = {
            "path": str(current / "disabled-phase.json"),
            "file_sha256": "9" * 64,
            "canonical_receipt_sha256": "a" * 64,
            "plan_sha256": "b" * 64,
            "process_identity_sha256": "c" * 64,
            "pid": 1234,
            "pid_start_ticks": 5678,
            "config_sha256": "d" * 64,
            "artifact_sha256": artifact_sha,
            "runtime_code_sha256": "e" * 64,
            "execution_commit": runtime["execution_commit"],
            "execution_tree": runtime["execution_tree"],
            "runtime_identity_file_sha256": "f" * 64,
            "startup_attestation_sha256": "0" * 64,
        }
        regression_source_manifest = subject.canonical_sha256(
            {
                "test_files": regression["test_files"],
                "runtime_sources": regression["runtime_sources"],
            }
        )
        envelope_path = current / "activation-envelope.json"
        envelope = _write_document(
            envelope_path,
            {
                "schema_version": subject.deploy.COMPATIBLE_ACTIVATION_ENVELOPE_SCHEMA,
                "status": "compatible_activation_evidence_complete",
                "plan_sha256": disabled["plan_sha256"],
                "plan_core_sha256": "1" * 64,
                "transaction_contract_sha256": "2" * 64,
                "execution": {
                    key: runtime[key]
                    for key in (
                        "execution_commit",
                        "execution_tree",
                        "annotated_tag",
                        "annotated_tag_object",
                    )
                },
                "artifact": {"artifact_sha256": artifact_sha, "files": artifact_files},
                "disabled_phase_receipt": disabled,
                "concurrent_resource_receipt": {
                    "path": str(resource_path),
                    "file_sha256": subject.hashlib.sha256(resource_path.read_bytes()).hexdigest(),
                    "canonical_sha256": resource["canonical_resource_receipt_sha256"],
                    "disabled_process_identity_sha256": disabled["process_identity_sha256"],
                    "live_pid": disabled["pid"],
                },
                "runtime_regression_receipt": {
                    "path": str(regression_path),
                    "file_sha256": subject.hashlib.sha256(regression_path.read_bytes()).hexdigest(),
                    "canonical_sha256": regression["canonical_receipt_sha256"],
                    "nodeid_manifest_sha256": regression["nodeid_manifest_sha256"],
                    "test_source_manifest_sha256": regression_source_manifest,
                },
                "sell_54_case_receipt": {
                    key: value
                    for key, value in sell_result.items()
                    if key != "canonical_receipt_sha256"
                }
                | {"canonical_sha256": sell_result["canonical_receipt_sha256"]},
                "checks": {
                    "disabled_phase_complete_and_same_plan": True,
                    "b0_fill_cooldown_exact_in_both_configs": True,
                    "concurrent_2vcpu_2gib_resource_window_passed": True,
                    "frozen_regression_nodeid_and_sources_passed": True,
                    "real_sell_54_case_and_sources_passed": True,
                    "no_locked_or_economic_evidence_read": True,
                },
                "activation_contract": {
                    "restart_only": True,
                    "same_disabled_process_required": True,
                    "phase_token_still_required": True,
                    "envelope_does_not_authorize_remote_mutation_by_itself": True,
                },
                "evidence_boundary": dict(subject.deploy.ACTIVATION_ENVELOPE_EVIDENCE_BOUNDARY),
            },
            "canonical_activation_envelope_sha256",
        )
        paths["compatible_activation_envelope"] = envelope_path
        documents["compatible_activation_envelope"] = envelope
        evidence = cls(
            root=root,
            repository_root=repository_root,
            legacy=legacy,
            paths=paths,
            documents=documents,
            wrappers=wrappers,
            output=root / "receipts/final-composition-v2.json",
            sell_result=sell_result,
        )
        return evidence

    def inputs(self) -> subject.CompositionInputs:
        return subject.CompositionInputs(
            formal_buy_component_manifest=self.paths["formal_buy_component_manifest"],
            formal_buy_component_validation=self.paths["formal_buy_component_validation"],
            joint_closeout_manifest=self.paths["joint_closeout_manifest"],
            owner_decision=self.paths["owner_decision"],
            attempt_execution_manifest=self.paths["attempt_execution_manifest"],
            source_execution_manifest=self.paths["source_execution_manifest"],
            cpp_builder_preflight=self.paths["cpp_builder_preflight"],
            cpp_quick_preflight=self.paths["cpp_quick_preflight"],
            cpp_qualification=self.paths["cpp_qualification"],
            owner_execution_preflight=self.paths["owner_execution_preflight"],
            label_materialization=self.paths["label_materialization"],
            refit_receipt=self.paths["refit_receipt"],
            exact_artifact_manifest=self.paths["exact_artifact_manifest"],
            exact_policy=self.paths["exact_policy"],
            exact_predicate_bundle=self.paths["exact_predicate_bundle"],
            parity_research_compiled=self.paths["parity_research_compiled"],
            parity_development_snapshot=self.paths["parity_development_snapshot"],
            parity_streaming_offline=self.paths["parity_streaming_offline"],
            layer4_mechanics=self.paths["layer4_mechanics"],
            layer4_contract=self.paths["layer4_contract"],
            layer4_day_receipts=tuple(
                self.paths[f"layer4_day::{index:02d}"]
                for index in range(subject.EXPECTED_DAY_COUNT)
            ),
            layer4_final=self.paths["layer4_final"],
            compatible_execution_attempt=self.paths["compatible_execution_attempt"],
            stability_wrappers=self.wrappers,
            compatible_runtime_regression=self.paths["compatible_runtime_regression"],
            compatible_concurrent_resource=self.paths["compatible_concurrent_resource"],
            sell_54_case=self.paths["sell_54_case"],
            compatible_activation_envelope=self.paths["compatible_activation_envelope"],
        )


@pytest.fixture
def evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> V2Evidence:
    repository_root = tmp_path / "repo"
    repository_root.mkdir()
    _git(repository_root, "init")
    _git(repository_root, "config", "user.name", "NarrowGate Test")
    _git(repository_root, "config", "user.email", "narrowgate@example.invalid")
    (repository_root / "source.py").write_text("FROZEN = True\n", encoding="ascii")
    _git(repository_root, "add", "source.py")
    _git(repository_root, "commit", "-m", "compatible runtime")
    _git(
        repository_root,
        "tag",
        "-a",
        "f05-owner-buy-e3-compatible-attempt-test",
        "-m",
        "compatible attempt",
    )
    _git(repository_root, "tag", "-a", "operational-tag", "-m", "operational release")
    legacy_fixture_contract = {
        "formal_buy_component_manifest": (
            "f05_formal.component_artifact_manifest.v1",
            "f05_formal:formal_v24_buy_component_artifacts",
            "formal_buy_component_manifest_bound",
        ),
        "formal_buy_component_validation": (
            "f05_formal.component_validation.v1",
            "f05_formal:buy_component_validation",
            "passed_exact_component_result_report_scorecards_and_cache",
        ),
        "source_execution_manifest": (
            "f05_formal_sell_only.execution_manifest.v1",
            "f05_formal_sell_only",
            "pre_execution_bound",
        ),
        "cpp_builder_preflight": (
            "f05_cpp.builder_preflight.v1",
            "f05_cpp_builder",
            "passed_all_3516_zero_economic_builder_walk",
        ),
        "cpp_quick_preflight": (
            "f05_cpp_quick.receipt.v1",
            "f05_cpp_quick",
            "passed_first_opportunity_all_side_specific_arms_lockstep",
        ),
        "cpp_qualification": (
            "f05_cpp_qualification.receipt.v1",
            "f05_cpp_qualification",
            "passed_real_day_all_opportunity_all_arm_lockstep",
        ),
    }
    for role, (schema, identity, status_value) in legacy_fixture_contract.items():
        monkeypatch.setitem(subject.EXACT_SCHEMA_BY_ROLE, role, schema)
        monkeypatch.setitem(subject.EXACT_IDENTITY_BY_ROLE, role, identity)
        monkeypatch.setitem(subject.EXACT_STATUS_BY_ROLE, role, status_value)
    result = V2Evidence.build(tmp_path.resolve(), repository_root)

    def validate_attempt(path: Path, **_kwargs: Any) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="ascii"))

    def validate_wrappers(**kwargs: Any) -> dict[str, dict[str, Any]]:
        return {
            role: json.loads(Path(path).read_text(encoding="ascii"))
            for role, path in kwargs["wrappers"].items()
        }

    def validate_regression(path: Path, **_kwargs: Any) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="ascii"))

    def validate_resource(path: Path, **_kwargs: Any) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="ascii"))

    def validate_sell(_path: Path, **_kwargs: Any) -> dict[str, Any]:
        return dict(result.sell_result)

    monkeypatch.setattr(subject.attempt, "validate_manifest", validate_attempt)
    monkeypatch.setattr(subject.stability, "validate_stability_wrappers", validate_wrappers)
    monkeypatch.setattr(subject.gate, "validate_runtime_regression_receipt", validate_regression)
    monkeypatch.setattr(subject.gate, "validate_concurrent_resource_receipt", validate_resource)
    monkeypatch.setattr(subject.gate, "validate_sell_owner_54_case_receipt", validate_sell)
    return result


def test_historical_production_role_identities_are_frozen_exactly() -> None:
    assert subject.EXACT_SCHEMA_BY_ROLE["formal_buy_component_manifest"].startswith(
        "f05_full_multiscale_successor_formal_component_closeout_v1."
    )
    assert subject.EXACT_IDENTITY_BY_ROLE["formal_buy_component_manifest"].endswith(
        ":formal_v24_buy_component_artifacts"
    )
    assert subject.EXACT_STATUS_BY_ROLE["source_execution_manifest"] == (
        "pre_economic_formal_execution_bound"
    )
    assert (
        "formal_sell_only_orchestrator_v1"
        in subject.EXACT_SCHEMA_BY_ROLE["source_execution_manifest"]
    )
    assert subject.EXACT_SCHEMA_BY_ROLE["cpp_qualification"] == (
        "f05_cpp_one_shot_real_day_all_arm_lockstep_v26.receipt.v1"
    )


def _compose(evidence: V2Evidence) -> dict[str, Any]:
    return subject.compose_final_composition(
        evidence_root=evidence.root,
        repository_root=evidence.repository_root,
        inputs=evidence.inputs(),
        output=evidence.output,
    )


def test_compose_and_validate_v2_exact_chain(evidence: V2Evidence) -> None:
    receipt = _compose(evidence)
    assert receipt["schema_version"] == subject.SCHEMA_VERSION
    assert receipt["status"] == subject.STATUS
    assert receipt["permissions"] == subject.OUTPUT_PERMISSIONS
    assert receipt["authority"] == {"research": False, "action": False, "live": False}
    assert receipt["evidence_boundary"] == subject.OUTPUT_EVIDENCE_BOUNDARY
    assert (
        receipt["formal_learning_algorithm"]["old_oof_applies_to_learning_algorithm_only"] is True
    )
    assert receipt["exact_artifact"]["exact_artifact_oof_available"] is False
    compatible = receipt["compatible_execution_attempt"]
    assert compatible == subject.attempt._compatible_execution_attempt_identity(  # noqa: SLF001
        evidence.documents["compatible_execution_attempt"]
    )
    assert compatible["execution_commit"] == _git(evidence.repository_root, "rev-parse", "HEAD")
    assert set(compatible["pre_admission_wrapper_canonical_sha256"]) == set(
        subject.attempt.PRE_ADMISSION_RECEIPT_ROLES
    )
    assert stat.S_IMODE(evidence.output.stat().st_mode) == 0o600
    assert (
        subject.validate_final_composition(
            evidence_root=evidence.root,
            repository_root=evidence.repository_root,
            receipt_path=evidence.output,
        )
        == receipt
    )


def test_real_v2_chain_reaches_deploy_validator_and_runtime_loader(
    evidence: V2Evidence,
) -> None:
    composition = _compose(evidence)
    attempt_final_path = evidence.root / "receipts/attempt-final.json"
    attempt_final, _ = subject.attempt.finalize_attempt(
        repository_root=evidence.repository_root,
        attempt_manifest_path=evidence.paths["compatible_execution_attempt"],
        result_receipt_paths={"final_composition": evidence.output},
        composition_evidence_root=evidence.root,
        output_path=attempt_final_path,
        require_current_checkout=True,
    )
    artifact_paths = {
        "manifest": evidence.paths["exact_artifact_manifest"],
        "policy": evidence.paths["exact_policy"],
        "predicate_bundle": evidence.paths["exact_predicate_bundle"],
    }
    evidence_paths = {
        "final_composition": evidence.output,
        "compatible_attempt_final": attempt_final_path,
        "concurrent_resource": evidence.paths["compatible_concurrent_resource"],
        "runtime_regression": evidence.paths["compatible_runtime_regression"],
        "sell54": evidence.paths["sell_54_case"],
        "activation_envelope": evidence.paths["compatible_activation_envelope"],
    }
    attempt_manifest = evidence.documents["compatible_execution_attempt"]
    runtime = attempt_manifest["runtime_execution"]
    release_path = evidence.root / "receipts/active-release.json"
    release, release_file_sha256 = active_release.finalize_active_release(
        repository_root=evidence.repository_root,
        annotated_operational_tag=runtime["annotated_tag"],
        artifact_paths=artifact_paths,
        evidence_paths=evidence_paths,
        output_path=release_path,
        generated_utc="2026-08-23T01:00:00Z",
    )

    artifact = composition["exact_artifact"]
    deployed_release = subject.deploy._validate_installed_active_release_file(  # noqa: SLF001
        release_path,
        expected_file_sha256=release_file_sha256,
        expected_canonical_sha256=release["canonical_active_release_sha256"],
        expected_execution_commit=runtime["execution_commit"],
        expected_execution_tree=runtime["execution_tree"],
        expected_artifact_sha256=artifact["artifact_sha256"],
    )
    assert deployed_release == release

    loaded = buy_runtime.LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=artifact_paths["manifest"],
        artifact_manifest_sha256=artifact["manifest_file_sha256"],
        expected_artifact_sha256=artifact["artifact_sha256"],
        policy_path=artifact_paths["policy"],
        policy_sha256=artifact["policy_file_sha256"],
        predicate_bundle_path=artifact_paths["predicate_bundle"],
        predicate_bundle_sha256=artifact["predicate_bundle_file_sha256"],
        active_release_path=release_path,
        active_release_file_sha256=release_file_sha256,
        active_release_canonical_sha256=release["canonical_active_release_sha256"],
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    assert loaded.artifact_sha256 == artifact["artifact_sha256"]
    assert (
        attempt_final["result_receipts"]["final_composition"]["canonical_sha256"]
        == (composition[subject.CANONICAL_FIELD])
    )


def test_compose_is_no_replace(evidence: V2Evidence) -> None:
    _compose(evidence)
    with pytest.raises(subject.FinalCompositionAmendmentError, match="already exists"):
        _compose(evidence)


def test_old_attempt_composition_schema_is_rejected(evidence: V2Evidence) -> None:
    _compose(evidence)
    receipt = json.loads(evidence.output.read_text(encoding="ascii"))
    receipt["schema_version"] = f"{subject.IDENTITY}.final_composition_receipt.v1"
    _rewrite(evidence.output, receipt, subject.CANONICAL_FIELD)
    with pytest.raises(subject.FinalCompositionAmendmentError, match="identity drifted"):
        subject.validate_final_composition(
            evidence_root=evidence.root,
            repository_root=evidence.repository_root,
            receipt_path=evidence.output,
        )


def test_ambiguous_compatible_attempt_status_is_rejected(evidence: V2Evidence) -> None:
    path = evidence.paths["compatible_execution_attempt"]
    payload = json.loads(path.read_text(encoding="ascii"))
    payload["status"] = "not_passed_but_contains_passed"
    _rewrite(path, payload, "canonical_execution_attempt_sha256")
    with pytest.raises(subject.FinalCompositionAmendmentError, match="status is not exact"):
        _compose(evidence)


def test_missing_wrapper_role_in_final_receipt_is_rejected(evidence: V2Evidence) -> None:
    _compose(evidence)
    receipt = json.loads(evidence.output.read_text(encoding="ascii"))
    receipt["ordered_evidence"] = [
        row
        for row in receipt["ordered_evidence"]
        if row["role"] != f"{subject.WRAPPER_PREFIX}single_day"
    ]
    receipt["ordered_evidence_sha256"] = subject.canonical_sha256(receipt["ordered_evidence"])
    _rewrite(evidence.output, receipt, subject.CANONICAL_FIELD)
    with pytest.raises(subject.FinalCompositionAmendmentError, match="role set"):
        subject.validate_final_composition(
            evidence_root=evidence.root,
            repository_root=evidence.repository_root,
            receipt_path=evidence.output,
        )


def test_same_artifact_different_attempt_is_rejected(evidence: V2Evidence) -> None:
    _compose(evidence)
    receipt = json.loads(evidence.output.read_text(encoding="ascii"))
    receipt["compatible_execution_attempt"]["execution_commit"] = "f" * 40
    _rewrite(evidence.output, receipt, subject.CANONICAL_FIELD)
    with pytest.raises(subject.FinalCompositionAmendmentError, match="final composition v2"):
        subject.validate_final_composition(
            evidence_root=evidence.root,
            repository_root=evidence.repository_root,
            receipt_path=evidence.output,
        )


def test_resource_envelope_binding_drift_is_rejected(evidence: V2Evidence) -> None:
    path = evidence.paths["compatible_activation_envelope"]
    envelope = json.loads(path.read_text(encoding="ascii"))
    envelope["concurrent_resource_receipt"]["canonical_sha256"] = "f" * 64
    _rewrite(path, envelope, "canonical_activation_envelope_sha256")
    with pytest.raises(subject.FinalCompositionAmendmentError, match="resource binding"):
        _compose(evidence)


def test_symlink_role_is_rejected(evidence: V2Evidence) -> None:
    source = evidence.paths["compatible_runtime_regression"]
    copy = source.with_name("regression-copy.json")
    copy.write_bytes(source.read_bytes())
    copy.chmod(0o600)
    source.unlink()
    source.symlink_to(copy)
    with pytest.raises(subject.FinalCompositionAmendmentError, match="opened safely"):
        _compose(evidence)


def test_one_inode_cannot_satisfy_two_roles(evidence: V2Evidence) -> None:
    inputs = evidence.inputs()
    day_receipts = list(inputs.layer4_day_receipts)
    day_receipts[1] = day_receipts[0]
    duplicated = replace(inputs, layer4_day_receipts=tuple(day_receipts))
    with pytest.raises(subject.FinalCompositionAmendmentError, match="inode"):
        subject.compose_final_composition(
            evidence_root=evidence.root,
            repository_root=evidence.repository_root,
            inputs=duplicated,
            output=evidence.output,
        )


def test_path_swap_during_resource_validation_is_rejected(
    evidence: V2Evidence, monkeypatch: pytest.MonkeyPatch
) -> None:
    def swap(candidate: Path, **_kwargs: Any) -> dict[str, Any]:
        raw = candidate.read_bytes()
        payload = json.loads(raw)
        candidate.unlink()
        candidate.write_bytes(raw)
        candidate.chmod(0o600)
        return payload

    monkeypatch.setattr(subject.gate, "validate_concurrent_resource_receipt", swap)
    with pytest.raises(subject.FinalCompositionAmendmentError, match="changed during"):
        _compose(evidence)


def test_cli_validate(evidence: V2Evidence, capsys: pytest.CaptureFixture[str]) -> None:
    receipt = _compose(evidence)
    assert (
        subject.main(
            [
                "validate",
                "--evidence-root",
                str(evidence.root),
                "--repository-root",
                str(evidence.repository_root),
                "--receipt",
                str(evidence.output),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == receipt


def test_cli_compose(evidence: V2Evidence, capsys: pytest.CaptureFixture[str]) -> None:
    output = evidence.root / "receipts/cli-final-composition-v2.json"
    args = [
        "compose",
        "--evidence-root",
        str(evidence.root),
        "--repository-root",
        str(evidence.repository_root),
        "--output",
        str(output),
    ]
    for role in subject.BASE_ROLE_ORDER:
        args.extend((f"--{role.replace('_', '-')}", str(evidence.paths[role])))
    for index in range(subject.EXPECTED_DAY_COUNT):
        args.extend(("--layer4-day-receipt", str(evidence.paths[f"layer4_day::{index:02d}"])))
    args.extend(("--layer4-final", str(evidence.paths["layer4_final"])))
    args.extend(
        (
            "--compatible-execution-attempt",
            str(evidence.paths["compatible_execution_attempt"]),
        )
    )
    for role in subject.attempt.PRE_ADMISSION_RECEIPT_ROLES:
        args.extend(("--stability-wrapper", f"{role}={evidence.wrappers[role]}"))
    args.extend(
        (
            "--compatible-runtime-regression",
            str(evidence.paths["compatible_runtime_regression"]),
            "--compatible-concurrent-resource",
            str(evidence.paths["compatible_concurrent_resource"]),
            "--sell-54-case",
            str(evidence.paths["sell_54_case"]),
            "--compatible-activation-envelope",
            str(evidence.paths["compatible_activation_envelope"]),
        )
    )
    assert subject.main(args) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["schema_version"] == subject.SCHEMA_VERSION
    assert output.exists()
