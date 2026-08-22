from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_v1 as composition,
)

SHA_COMPONENT_RESULT = "1" * 64
SHA_EXECUTION_SOURCE = "2" * 64
SHA_FRESH_ADAPTER = "3" * 64


def _permissions() -> dict[str, bool]:
    return {
        "research_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }


def _boundaries() -> dict[str, bool]:
    return {
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _write_document(
    path: Path,
    payload: dict[str, Any],
    canonical_field: str,
) -> dict[str, Any]:
    document = dict(payload)
    document[canonical_field] = composition.document_sha256(document, canonical_field)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            document,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)
    return document


def _rewrite_document(path: Path, mutate: Any, canonical_field: str) -> dict[str, Any]:
    payload = composition.strict_load_json(path)
    payload.pop(canonical_field, None)
    mutate(payload)
    return _write_document(path, payload, canonical_field)


class EvidenceFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths: dict[str, Path] = {}
        self.documents: dict[str, dict[str, Any]] = {}
        self.days = tuple(
            (date(2026, 7, 1) + timedelta(days=index)).isoformat()
            for index in range(composition.EXPECTED_DAY_COUNT)
        )
        self.source_output = root / "receipts/source_role_resolution.json"
        self.final_output = root / "receipts/final_composition.json"
        self._build()

    def _path(self, role: str) -> Path:
        path = self.root / "inputs" / f"{role.replace('::', '-')}.json"
        self.paths[role] = path
        return path

    def _write(
        self,
        role: str,
        payload: dict[str, Any],
        canonical_field: str,
    ) -> dict[str, Any]:
        document = _write_document(self._path(role), payload, canonical_field)
        self.documents[role] = document
        return document

    def _build(self) -> None:
        self._write(
            "owner_decision",
            {
                "schema_version": f"{composition.IDENTITY}.owner_decision.v1",
                "identity": composition.IDENTITY,
                "status": "owner_override_recorded_artifact_not_yet_frozen",
                "selected_side": "BUY",
                "selected_learning_algorithm": "E3_HIGHER_ORDER_BOOLEAN",
                "research_supported": False,
                "owner_risk_accepted": True,
                "outcome_informed_owner_override": True,
                "formal_closeout_mutated": False,
                "formal_hierarchy_passed": False,
                "formal_hard_gates_passed": False,
                "evidence_boundary": {
                    "exact_final_artifact_oof_available": False,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                    "new_economic_arm_run": False,
                },
                "source_bindings": {"BUY": {"result_canonical_sha256": SHA_COMPONENT_RESULT}},
                "permissions": _permissions(),
            },
            "canonical_owner_decision_sha256",
        )
        source_execution = self._write(
            "source_execution_manifest",
            {
                "schema_version": "f05_formal_sell_only.execution_manifest.v1",
                "identity": "f05_formal_sell_only",
                "status": "pre_execution_bound",
                "execution_contract": {"formal_sides": ["SELL"]},
                "backend": {"identity": "dual_side_outcome_blind_mechanics"},
                "executor": {
                    "identity": "historical_sell_only_v7",
                    "formal_sides": ["SELL"],
                },
                "permissions": _permissions(),
            },
            "canonical_execution_manifest_sha256",
        )
        self._write(
            "joint_closeout_manifest",
            {
                "schema_version": f"{composition.IDENTITY}.closeout_manifest.v1",
                "identity": composition.IDENTITY,
                "status": "formal_statistics_rebuilt_owner_override_recorded",
                "files": {
                    "owner_decision.json": {
                        "sha256": composition.file_sha256(self.paths["owner_decision"]),
                        "size_bytes": self.paths["owner_decision"].stat().st_size,
                        "mode": "0600",
                    }
                },
                "permissions": _permissions(),
            },
            "canonical_manifest_sha256",
        )
        component = self._write(
            "formal_buy_component_manifest",
            {
                "schema_version": "f05_formal.component_artifact_manifest.v1",
                "identity": "f05_formal:formal_v24_buy_component_artifacts",
                "formal_side": "BUY",
                "source_execution_manifest_sha256": SHA_EXECUTION_SOURCE,
                "component_result_canonical_sha256": SHA_COMPONENT_RESULT,
                "nested_oof_artifact_manifest_canonical_sha256": "4" * 64,
                "bindings": {},
                "permissions": _permissions(),
            },
            "canonical_artifact_manifest_sha256",
        )
        self._write(
            "formal_buy_component_validation",
            {
                "schema_version": "f05_formal.component_validation.v1",
                "identity": "f05_formal:buy_component_validation",
                "status": "passed_exact_component_result_report_scorecards_and_cache",
                "formal_side": "BUY",
                "source_execution": {"execution_manifest_sha256": SHA_EXECUTION_SOURCE},
                "component_result": {"canonical_sha256": SHA_COMPONENT_RESULT},
                "permissions": _permissions(),
            },
            "canonical_validation_receipt_sha256",
        )
        execution = self._write(
            "attempt_execution_manifest",
            {
                "schema_version": f"{composition.IDENTITY}.execution_manifest.v1",
                "identity": composition.IDENTITY,
                "status": "pre_refit_owner_execution_bound",
                "public_base_commit": "c" * 40,
                "annotated_tag": "f05-owner-buy-e3-live-attempt2-20260821",
                "execution_contract": {
                    "selected_side": "BUY",
                    "training_day_count": composition.EXPECTED_DAY_COUNT,
                },
                "backend": source_execution["backend"],
                "executor": source_execution["executor"],
                "bindings": {
                    "owner_decision": {
                        "sha256": composition.file_sha256(self.paths["owner_decision"])
                    },
                    "joint_closeout_manifest": {
                        "sha256": composition.file_sha256(self.paths["joint_closeout_manifest"])
                    },
                    "source_execution_manifest": {
                        "sha256": composition.file_sha256(self.paths["source_execution_manifest"])
                    },
                },
                "evidence_boundary": {
                    "formal_hierarchy_passed": False,
                    "formal_hard_gates_passed": False,
                    "outcome_informed_owner_override": True,
                    "exact_artifact_oof_available": False,
                },
                "permissions": _permissions(),
            },
            "canonical_execution_manifest_sha256",
        )
        execution_sha = execution["canonical_execution_manifest_sha256"]
        builder = self._write(
            "cpp_builder_preflight",
            {
                "schema_version": "f05_cpp.builder_preflight.v1",
                "identity": "f05_cpp_builder",
                "status": "passed_all_3516_zero_economic_builder_walk",
                "execution_manifest_sha256": execution_sha,
                "opportunity_count": 3516,
                "economic_values_read": False,
                "economic_values_persisted": False,
                **_boundaries(),
            },
            "canonical_receipt_sha256",
        )
        quick = self._write(
            "cpp_quick_preflight",
            {
                "schema_version": "f05_cpp_quick.receipt.v1",
                "identity": "f05_cpp_quick",
                "status": "passed_first_opportunity_all_side_specific_arms_lockstep",
                "execution_manifest_sha256": execution_sha,
                "all_panel_builder_preflight_receipt_sha256": builder["canonical_receipt_sha256"],
                "arm_count": 16,
                "zero_mismatch_arm_count": 16,
                "mismatch_count": 0,
                "economic_values_persisted": False,
                "economic_values_exposed": False,
                "economic_values_used_for_selection": False,
                **_boundaries(),
            },
            "canonical_receipt_sha256",
        )
        qualification = self._write(
            "cpp_qualification",
            {
                "schema_version": "f05_cpp_qualification.receipt.v1",
                "identity": "f05_cpp_qualification",
                "status": "passed_real_day_all_opportunity_all_arm_lockstep",
                "qualification_contract": {
                    "execution_manifest_sha256": execution_sha,
                    "all_panel_builder_preflight_receipt_sha256": builder[
                        "canonical_receipt_sha256"
                    ],
                    "first_opportunity_all_arm_preflight_receipt_sha256": quick[
                        "canonical_receipt_sha256"
                    ],
                    "economic_values_persisted": False,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                },
                "arm_count": 648,
                "zero_mismatch_arm_count": 648,
                "economic_values_persisted": False,
                "economic_values_used_for_selection": False,
                **_boundaries(),
            },
            "canonical_receipt_sha256",
        )
        preflight = self._write(
            "owner_execution_preflight",
            {
                "schema_version": (f"{composition.IDENTITY}.execution_preflight_receipt.v1"),
                "identity": composition.IDENTITY,
                "status": "owner_execution_preflight_complete",
                "execution_manifest_canonical_sha256": execution_sha,
                "cpp_qualification_receipt_sha256": qualification["canonical_receipt_sha256"],
                "formal_zero_economic_preflight": {
                    "status": "formal_offline_replay_mechanics_ready"
                },
                "economic_values_exposed": False,
                **_boundaries(),
            },
            "canonical_preflight_receipt_sha256",
        )
        labels = self._write(
            "label_materialization",
            {
                "schema_version": (
                    f"{composition.IDENTITY}.full_development_label_materialization.v1"
                ),
                "identity": f"{composition.IDENTITY}.full_development_label_materializer_v1",
                "status": "full_development_buy_labels_materialized",
                "side": "BUY",
                "full_day_count": composition.EXPECTED_DAY_COUNT,
                "fresh_execution_manifest_sha256": execution_sha,
                "fresh_adapter_identity": "qualified_dual_side_canonical_adapter",
                "fresh_adapter_artifact_sha256": SHA_FRESH_ADAPTER,
                "strategy_dependent_cross_execution_cache_imported": False,
                "economic_values_persisted_in_receipt": False,
                **_boundaries(),
            },
            "canonical_materialization_receipt_sha256",
        )
        bundle = self._write(
            "exact_predicate_bundle",
            {
                "schema_version": (f"{composition.IDENTITY}.selected_predicate_bundle.v1"),
                "identity": composition.IDENTITY,
                "side": "BUY",
                "uses_trade_predicates": False,
                "uses_depth_predicates": False,
                "uses_m2_incremental_features": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
            "canonical_sha256",
        )
        policy = self._write(
            "exact_policy",
            {
                "schema_version": f"{composition.IDENTITY}.artifact.v1",
                "identity": composition.IDENTITY,
                "status": "owner_refit_frozen_not_self_confirmed",
                "side": "BUY",
                "predicate_bundle_file_sha256": composition.file_sha256(
                    self.paths["exact_predicate_bundle"]
                ),
                "evidence_boundary": {
                    "research_supported": False,
                    "owner_risk_accepted": True,
                    "outcome_informed_owner_override": True,
                    "formal_hierarchy_passed": False,
                    "formal_hard_gates_passed": False,
                    "exact_artifact_oof_available": False,
                },
                "permissions": _permissions(),
            },
            "canonical_sha256",
        )
        artifact_manifest = self._write(
            "exact_artifact_manifest",
            {
                "schema_version": f"{composition.IDENTITY}.full_development_refit.v1",
                "identity": composition.IDENTITY,
                "status": "exact_buy_e3_artifact_frozen",
                "policy_file_sha256": composition.file_sha256(self.paths["exact_policy"]),
                "policy_canonical_sha256": policy["canonical_sha256"],
                "predicate_bundle_file_sha256": composition.file_sha256(
                    self.paths["exact_predicate_bundle"]
                ),
                "predicate_bundle_canonical_sha256": bundle["canonical_sha256"],
                "label_materialization_receipt_sha256": labels[
                    "canonical_materialization_receipt_sha256"
                ],
                "cpp_one_shot_qualification_receipt_sha256": qualification[
                    "canonical_receipt_sha256"
                ],
                "execution_preflight_receipt_sha256": preflight[
                    "canonical_preflight_receipt_sha256"
                ],
                "training_days": list(self.days),
                "research_supported": False,
                "owner_risk_accepted": True,
                "exact_final_artifact_oof_available": False,
                "permissions": _permissions(),
            },
            "artifact_sha256",
        )
        artifact_sha = artifact_manifest["artifact_sha256"]
        self._write(
            "refit_receipt",
            {
                "schema_version": f"{composition.IDENTITY}.refit_run_receipt.v1",
                "identity": composition.IDENTITY,
                "status": "owner_buy_e3_full_development_refit_complete",
                "execution_manifest_canonical_sha256": execution_sha,
                "cpp_qualification_receipt_sha256": qualification["canonical_receipt_sha256"],
                "execution_preflight_receipt_sha256": preflight[
                    "canonical_preflight_receipt_sha256"
                ],
                "label_materialization_receipt_sha256": labels[
                    "canonical_materialization_receipt_sha256"
                ],
                "artifact_sha256": artifact_sha,
                "full_development_refit_count": 1,
                "outer_fold_policy_selected": False,
                "outer_fold_rules_merged": False,
                "literal_edited": False,
                "candidate_substituted": False,
                "research_supported": False,
                "owner_risk_accepted": True,
                "exact_artifact_oof_available": False,
                "permissions": _permissions(),
            },
            "canonical_refit_run_receipt_sha256",
        )
        for role, layer, mismatch_fields in (
            ("parity_research_compiled", "research_compiled", ("mismatch_count",)),
            (
                "parity_development_snapshot",
                "development_snapshot",
                (
                    "predicate_projection_mismatch_count",
                    "action_duration_mismatch_count",
                ),
            ),
            (
                "parity_streaming_offline",
                "streaming_offline",
                ("feature_mismatch_count",),
            ),
        ):
            self._write(
                role,
                {
                    "schema_version": f"{composition.IDENTITY}.parity_receipt.v1",
                    "identity": composition.IDENTITY,
                    "status": "parity_complete",
                    "layer": layer,
                    "artifact_sha256": artifact_sha,
                    "artifact_manifest_file_sha256": composition.file_sha256(
                        self.paths["exact_artifact_manifest"]
                    ),
                    "policy_file_sha256": composition.file_sha256(self.paths["exact_policy"]),
                    "predicate_bundle_file_sha256": composition.file_sha256(
                        self.paths["exact_predicate_bundle"]
                    ),
                    "evidence": {field: 0 for field in mismatch_fields},
                    "economic_values_exposed": False,
                    "economic_values_used_for_selection": False,
                    **_boundaries(),
                },
                "canonical_receipt_sha256",
            )
        contract = self._write(
            "layer4_contract",
            {
                "schema_version": f"{composition.IDENTITY}.layer4_lockstep_contract.v2",
                "identity": composition.IDENTITY,
                "status": "layer4_lockstep_contract_frozen",
                "execution_manifest_canonical_sha256": execution_sha,
                "learning_algorithm_artifact_sha256": component[
                    "canonical_artifact_manifest_sha256"
                ],
                "learning_algorithm_manifest_file_sha256": composition.file_sha256(
                    self.paths["formal_buy_component_manifest"]
                ),
                "formal_v24_execution_manifest_sha256": component[
                    "source_execution_manifest_sha256"
                ],
                "component_result_canonical_sha256": component["component_result_canonical_sha256"],
                "nested_oof_manifest_canonical_sha256": component[
                    "nested_oof_artifact_manifest_canonical_sha256"
                ],
                "mechanics_receipt_sha256": "7" * 64,
                "source_predicate_bundle_sha256": "8" * 64,
                "parity_source_file_sha256": "9" * 64,
                "exact_artifact_sha256": artifact_sha,
                "artifact_manifest_file_sha256": composition.file_sha256(
                    self.paths["exact_artifact_manifest"]
                ),
                "policy_file_sha256": composition.file_sha256(self.paths["exact_policy"]),
                "predicate_bundle_file_sha256": composition.file_sha256(
                    self.paths["exact_predicate_bundle"]
                ),
                "ordered_development_days": list(self.days),
                "economic_values_exposed": False,
                "economic_values_used_for_selection": False,
                **_boundaries(),
            },
            "canonical_contract_sha256",
        )
        admitted_days: list[dict[str, str]] = []
        for index, utc_day in enumerate(self.days):
            role = f"layer4_day::{index:02d}"
            day_receipt = self._write(
                role,
                {
                    "schema_version": (f"{composition.IDENTITY}.repeated_policy_lockstep_day.v2"),
                    "identity": composition.IDENTITY,
                    "status": "day_lockstep_complete",
                    "utc_day": utc_day,
                    "canonical_contract_sha256": contract["canonical_contract_sha256"],
                    "learning_algorithm_artifact_sha256": component[
                        "canonical_artifact_manifest_sha256"
                    ],
                    "exact_artifact_sha256": artifact_sha,
                    "artifact_manifest_file_sha256": composition.file_sha256(
                        self.paths["exact_artifact_manifest"]
                    ),
                    "policy_file_sha256": composition.file_sha256(self.paths["exact_policy"]),
                    "predicate_bundle_file_sha256": composition.file_sha256(
                        self.paths["exact_predicate_bundle"]
                    ),
                    "lockstep": {"mismatch_count": 0},
                    "economic_values_exposed": False,
                    "economic_values_used_for_selection": False,
                    **_boundaries(),
                },
                "canonical_day_receipt_sha256",
            )
            admitted_days.append(
                {
                    "utc_day": utc_day,
                    "file_sha256": composition.file_sha256(self.paths[role]),
                    "canonical_day_receipt_sha256": day_receipt["canonical_day_receipt_sha256"],
                }
            )
        self._write(
            "layer4_final",
            {
                "schema_version": f"{composition.IDENTITY}.parity_receipt.v2",
                "identity": composition.IDENTITY,
                "status": "parity_complete",
                "layer": "repeated_policy_lockstep",
                "canonical_contract_sha256": contract["canonical_contract_sha256"],
                "learning_algorithm_artifact_sha256": component[
                    "canonical_artifact_manifest_sha256"
                ],
                "exact_artifact_sha256": artifact_sha,
                "artifact_manifest_file_sha256": composition.file_sha256(
                    self.paths["exact_artifact_manifest"]
                ),
                "policy_file_sha256": composition.file_sha256(self.paths["exact_policy"]),
                "predicate_bundle_file_sha256": composition.file_sha256(
                    self.paths["exact_predicate_bundle"]
                ),
                "evidence": {
                    "day_count": composition.EXPECTED_DAY_COUNT,
                    "day_receipts": admitted_days,
                    "mismatch_count": 0,
                },
                "economic_values_exposed": False,
                "economic_values_used_for_selection": False,
                **_boundaries(),
            },
            "canonical_receipt_sha256",
        )
        self._write(
            "sell_54_case",
            {
                "schema_version": f"{composition.IDENTITY}.parity_receipt.v1",
                "identity": composition.IDENTITY,
                "status": "parity_complete",
                "layer": "sell_owner_54_case_unchanged",
                "artifact_sha256": artifact_sha,
                "artifact_manifest_file_sha256": composition.file_sha256(
                    self.paths["exact_artifact_manifest"]
                ),
                "policy_file_sha256": composition.file_sha256(self.paths["exact_policy"]),
                "predicate_bundle_file_sha256": composition.file_sha256(
                    self.paths["exact_predicate_bundle"]
                ),
                "evidence": {
                    "sell_tri_state_cases": 27,
                    "buy_tri_state_cases": 27,
                    "mismatch_count": 0,
                },
                "economic_values_exposed": False,
                "economic_values_used_for_selection": False,
                **_boundaries(),
            },
            "canonical_receipt_sha256",
        )
        self._write(
            "runtime_regression",
            {
                "schema_version": (f"{composition.IDENTITY}.runtime_regression_test_receipt.v1"),
                "identity": composition.IDENTITY,
                "status": "passed",
                "artifact_sha256": artifact_sha,
                "execution_commit": execution["public_base_commit"],
                "execution_tag": execution["annotated_tag"],
                "passed": 100,
                "failed": 0,
                "economic_values_read": False,
                **_boundaries(),
            },
            "canonical_receipt_sha256",
        )
        runtime = self._write(
            "host_runtime_identity",
            {
                "schema_version": f"{composition.IDENTITY}.host_runtime_identity_receipt.v2",
                "identity": composition.IDENTITY,
                "status": "host_runtime_identity_verified",
                "artifact_sha256": artifact_sha,
                "execution_commit": execution["public_base_commit"],
                "execution_tag": execution["annotated_tag"],
                "actual_config_file_sha256": "5" * 64,
                "runtime_code_sha256": "6" * 64,
                "economic_values_persisted": False,
                **_boundaries(),
            },
            "canonical_runtime_identity_receipt_sha256",
        )
        runtime_sha = runtime["canonical_runtime_identity_receipt_sha256"]
        health = self._write(
            "host_health",
            {
                "schema_version": f"{composition.IDENTITY}.host_health_window.v2",
                "identity": composition.IDENTITY,
                "status": "disabled_live_health_window_complete",
                "artifact_sha256": artifact_sha,
                "runtime_identity_receipt_sha256": runtime_sha,
                "runtime": {"buy_e3_enabled": False, "sell_owner_enabled": True},
                "economic_values_persisted": False,
                "hypothetical_live_actions_scored": False,
                **_boundaries(),
            },
            "canonical_health_receipt_sha256",
        )
        benchmark = self._write(
            "host_benchmark",
            {
                "schema_version": f"{composition.IDENTITY}.host_benchmark.v2",
                "identity": composition.IDENTITY,
                "status": "exact_artifact_host_benchmark_complete",
                "artifact_sha256": artifact_sha,
                "runtime_identity_receipt_sha256": runtime_sha,
                "health_receipt_sha256": health["canonical_health_receipt_sha256"],
                "economic_values_persisted": False,
                "hypothetical_live_actions_scored": False,
                **_boundaries(),
            },
            "canonical_benchmark_receipt_sha256",
        )
        post_health = self._write(
            "host_post_health",
            {
                "schema_version": f"{composition.IDENTITY}.host_health_window.v2",
                "identity": composition.IDENTITY,
                "status": "post_benchmark_live_health_window_complete",
                "artifact_sha256": artifact_sha,
                "runtime_identity_receipt_sha256": runtime_sha,
                "runtime": {"buy_e3_enabled": False, "sell_owner_enabled": True},
                "economic_values_persisted": False,
                "hypothetical_live_actions_scored": False,
                **_boundaries(),
            },
            "canonical_health_receipt_sha256",
        )
        self._write(
            "deployment_gate",
            {
                "schema_version": f"{composition.IDENTITY}.deployment_gate.v2",
                "identity": composition.IDENTITY,
                "status": "deployment_gate_passed",
                "artifact_sha256": artifact_sha,
                "execution_commit": execution["public_base_commit"],
                "execution_tag": execution["annotated_tag"],
                "runtime_identity_receipt_sha256": runtime_sha,
                "health_receipt_sha256": health["canonical_health_receipt_sha256"],
                "benchmark_receipt_sha256": benchmark["canonical_benchmark_receipt_sha256"],
                "post_health_receipt_sha256": post_health["canonical_health_receipt_sha256"],
                "checks": {"all_runtime_identity_and_resource_checks": True},
                "activation_allowed": True,
                "economic_values_persisted": False,
                "hypothetical_live_actions_scored": False,
                **_boundaries(),
            },
            "canonical_deployment_gate_receipt_sha256",
        )

    def inputs(self) -> composition.CompositionInputs:
        return composition.CompositionInputs(
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
            layer4_contract=self.paths["layer4_contract"],
            layer4_day_receipts=tuple(
                self.paths[f"layer4_day::{index:02d}"]
                for index in range(composition.EXPECTED_DAY_COUNT)
            ),
            layer4_final=self.paths["layer4_final"],
            sell_54_case=self.paths["sell_54_case"],
            runtime_regression=self.paths["runtime_regression"],
            host_runtime_identity=self.paths["host_runtime_identity"],
            host_health=self.paths["host_health"],
            host_benchmark=self.paths["host_benchmark"],
            host_post_health=self.paths["host_post_health"],
            deployment_gate=self.paths["deployment_gate"],
        )

    def compose(self) -> dict[str, Any]:
        self.source_output.parent.mkdir(parents=True, exist_ok=True)
        return composition.compose_final_composition(
            evidence_root=self.root,
            inputs=self.inputs(),
            source_role_output=self.source_output,
            output=self.final_output,
        )


@pytest.fixture
def evidence(tmp_path: Path) -> EvidenceFixture:
    return EvidenceFixture(tmp_path)


def test_compose_and_validate_complete_chain(evidence: EvidenceFixture) -> None:
    receipt = evidence.compose()
    validated = composition.validate_final_composition(
        evidence_root=evidence.root,
        receipt_path=evidence.final_output,
    )
    assert validated == receipt
    assert (
        receipt["formal_learning_algorithm"]["learning_algorithm_artifact_sha256"]
        == evidence.documents["formal_buy_component_manifest"]["canonical_artifact_manifest_sha256"]
    )
    assert receipt["formal_learning_algorithm"]["identities_are_distinct"] is True
    assert receipt["four_layer_parity"]["ordered_day_count"] == 30
    assert (evidence.final_output.stat().st_mode & 0o777) == 0o600
    assert (evidence.source_output.stat().st_mode & 0o777) == 0o600
    source = composition.strict_load_json(evidence.source_output)
    assert source["historical_source_execution"]["formal_sides"] == ["SELL"]
    assert source["owner_execution"]["label_side"] == "BUY"


def test_compose_refuses_overwrite(evidence: EvidenceFixture) -> None:
    evidence.compose()
    with pytest.raises(composition.FinalCompositionError, match="already exists"):
        evidence.compose()


def test_layer4_requires_exactly_30_ordered_v2_days(evidence: EvidenceFixture) -> None:
    inputs = evidence.inputs()
    with pytest.raises(composition.FinalCompositionError, match="exactly 30"):
        composition.compose_final_composition(
            evidence_root=evidence.root,
            inputs=replace(inputs, layer4_day_receipts=inputs.layer4_day_receipts[:-1]),
            source_role_output=evidence.source_output,
            output=evidence.final_output,
        )
    with pytest.raises(composition.FinalCompositionError, match="exactly 30"):
        composition.compose_final_composition(
            evidence_root=evidence.root,
            inputs=replace(
                inputs,
                layer4_day_receipts=(
                    *inputs.layer4_day_receipts,
                    inputs.layer4_day_receipts[-1],
                ),
            ),
            source_role_output=evidence.source_output,
            output=evidence.final_output,
        )
    reversed_inputs = replace(
        inputs,
        layer4_day_receipts=tuple(reversed(inputs.layer4_day_receipts)),
    )
    with pytest.raises(composition.FinalCompositionError, match="Layer4 day"):
        composition.compose_final_composition(
            evidence_root=evidence.root,
            inputs=reversed_inputs,
            source_role_output=evidence.source_output,
            output=evidence.final_output,
        )


def test_layer4_v1_day_receipt_is_rejected(evidence: EvidenceFixture) -> None:
    path = evidence.paths["layer4_day::00"]

    def mutate(payload: dict[str, Any]) -> None:
        payload["schema_version"] = f"{composition.IDENTITY}.repeated_policy_lockstep_day.v1"

    _rewrite_document(path, mutate, "canonical_day_receipt_sha256")
    with pytest.raises(composition.FinalCompositionError, match="schema is not admitted"):
        evidence.compose()


def test_cross_chain_parent_drift_is_rejected(evidence: EvidenceFixture) -> None:
    path = evidence.paths["cpp_quick_preflight"]

    def mutate(payload: dict[str, Any]) -> None:
        payload["all_panel_builder_preflight_receipt_sha256"] = "f" * 64

    _rewrite_document(path, mutate, "canonical_receipt_sha256")
    with pytest.raises(composition.FinalCompositionError, match="quick builder parent"):
        evidence.compose()


def test_layer4_cannot_substitute_exact_artifact_for_learning_algorithm(
    evidence: EvidenceFixture,
) -> None:
    path = evidence.paths["layer4_contract"]

    def mutate(payload: dict[str, Any]) -> None:
        payload["learning_algorithm_artifact_sha256"] = evidence.documents[
            "exact_artifact_manifest"
        ]["artifact_sha256"]

    _rewrite_document(path, mutate, "canonical_contract_sha256")
    with pytest.raises(composition.FinalCompositionError, match="learning algorithm"):
        evidence.compose()


def test_permission_drift_is_rejected(evidence: EvidenceFixture) -> None:
    evidence.paths["runtime_regression"].chmod(0o644)
    with pytest.raises(composition.FinalCompositionError, match="permission drifted"):
        evidence.compose()


def test_validation_or_action_authority_drift_is_rejected(
    evidence: EvidenceFixture,
) -> None:
    path = evidence.paths["parity_streaming_offline"]

    def mutate(payload: dict[str, Any]) -> None:
        payload["validation_read"] = True

    _rewrite_document(path, mutate, "canonical_receipt_sha256")
    with pytest.raises(composition.FinalCompositionError, match="validation_read"):
        evidence.compose()


def test_exact_artifact_oof_claim_is_rejected(evidence: EvidenceFixture) -> None:
    path = evidence.paths["exact_policy"]

    def mutate(payload: dict[str, Any]) -> None:
        payload["evidence_boundary"]["exact_artifact_oof_available"] = True

    _rewrite_document(path, mutate, "canonical_sha256")
    with pytest.raises(composition.FinalCompositionError, match="exact_artifact_oof_available"):
        evidence.compose()


def test_upstream_byte_drift_breaks_later_validation(evidence: EvidenceFixture) -> None:
    evidence.compose()
    path = evidence.paths["host_post_health"]
    raw = path.read_bytes()
    path.write_bytes(raw + b" ")
    path.chmod(0o600)
    with pytest.raises(composition.FinalCompositionError, match="binding drifted"):
        composition.validate_final_composition(
            evidence_root=evidence.root,
            receipt_path=evidence.final_output,
        )


def test_duplicate_keys_and_nonfinite_numbers_are_rejected(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"a": 1, "a": 2}\n', encoding="ascii")
    duplicate.chmod(0o600)
    with pytest.raises(composition.FinalCompositionError, match="duplicate JSON key"):
        composition.strict_load_json(duplicate)
    nonfinite = tmp_path / "nonfinite.json"
    nonfinite.write_text('{"a": NaN}\n', encoding="ascii")
    nonfinite.chmod(0o600)
    with pytest.raises(composition.FinalCompositionError, match="non-finite"):
        composition.strict_load_json(nonfinite)


def test_symlink_and_path_escape_are_rejected(
    evidence: EvidenceFixture,
    tmp_path: Path,
) -> None:
    target = evidence.paths["runtime_regression"]
    link = evidence.root / "inputs/runtime-link.json"
    link.symlink_to(target)
    with pytest.raises(composition.FinalCompositionError, match="symlink evidence"):
        composition.compose_final_composition(
            evidence_root=evidence.root,
            inputs=replace(evidence.inputs(), runtime_regression=link),
            source_role_output=evidence.source_output,
            output=evidence.final_output,
        )
    outside = tmp_path.parent / "outside-final-composition.json"
    outside.write_text("{}\n", encoding="ascii")
    outside.chmod(0o600)
    with pytest.raises(composition.FinalCompositionError, match="escapes"):
        composition.compose_final_composition(
            evidence_root=evidence.root,
            inputs=replace(evidence.inputs(), runtime_regression=outside),
            source_role_output=evidence.source_output,
            output=evidence.final_output,
        )
    outside.unlink()


def test_final_receipt_rejects_removed_or_reordered_evidence(
    evidence: EvidenceFixture,
) -> None:
    evidence.compose()
    receipt = composition.strict_load_json(evidence.final_output)
    receipt.pop("canonical_final_composition_receipt_sha256")
    receipt["ordered_evidence"][1], receipt["ordered_evidence"][2] = (
        receipt["ordered_evidence"][2],
        receipt["ordered_evidence"][1],
    )
    receipt["ordered_evidence_sha256"] = composition.canonical_sha256(receipt["ordered_evidence"])
    receipt["canonical_final_composition_receipt_sha256"] = composition.document_sha256(
        receipt,
        "canonical_final_composition_receipt_sha256",
    )
    evidence.final_output.unlink()
    _write_document(
        evidence.final_output,
        {
            key: value
            for key, value in receipt.items()
            if key != "canonical_final_composition_receipt_sha256"
        },
        "canonical_final_composition_receipt_sha256",
    )
    with pytest.raises(composition.FinalCompositionError, match="missing, extra, or reordered"):
        composition.validate_final_composition(
            evidence_root=evidence.root,
            receipt_path=evidence.final_output,
        )


def test_source_role_receipt_cannot_be_substituted(evidence: EvidenceFixture) -> None:
    evidence.compose()
    path = evidence.source_output

    def mutate(payload: dict[str, Any]) -> None:
        payload["owner_execution"]["label_side"] = "SELL"

    _rewrite_document(
        path,
        mutate,
        "canonical_source_role_resolution_receipt_sha256",
    )
    with pytest.raises(composition.FinalCompositionError, match="binding drifted"):
        composition.validate_final_composition(
            evidence_root=evidence.root,
            receipt_path=evidence.final_output,
        )


def test_cli_compose_and_validate(
    evidence: EvidenceFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    inputs = evidence.inputs()
    arguments = [
        "compose",
        "--evidence-root",
        str(evidence.root),
    ]
    for field in (
        "formal_buy_component_manifest",
        "formal_buy_component_validation",
        "joint_closeout_manifest",
        "owner_decision",
        "attempt_execution_manifest",
        "source_execution_manifest",
        "cpp_builder_preflight",
        "cpp_quick_preflight",
        "cpp_qualification",
        "owner_execution_preflight",
        "label_materialization",
        "refit_receipt",
        "exact_artifact_manifest",
        "exact_policy",
        "exact_predicate_bundle",
        "parity_research_compiled",
        "parity_development_snapshot",
        "parity_streaming_offline",
        "layer4_contract",
        "layer4_final",
        "sell_54_case",
        "runtime_regression",
        "host_runtime_identity",
        "host_health",
        "host_benchmark",
        "host_post_health",
        "deployment_gate",
    ):
        arguments.extend((f"--{field.replace('_', '-')}", str(getattr(inputs, field))))
    for path in inputs.layer4_day_receipts:
        arguments.extend(("--layer4-day-receipt", str(path)))
    arguments.extend(
        (
            "--source-role-output",
            str(evidence.source_output),
            "--output",
            str(evidence.final_output),
        )
    )
    assert composition.main(arguments) == 0
    assert json.loads(capsys.readouterr().out)["status"] == ("owner_buy_e3_final_evidence_composed")
    assert (
        composition.main(
            [
                "validate",
                "--evidence-root",
                str(evidence.root),
                "--receipt",
                str(evidence.final_output),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["identity"] == composition.COMPOSITION_IDENTITY
