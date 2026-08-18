from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as orchestrator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _binding(path: Path) -> dict[str, str]:
    return {"path": str(path), "sha256": orchestrator._file_sha256(path)}


def _bundle_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, dict[str, object], dict[str, object], dict[str, object]]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    selected = [f"2026-07-{day:02d}" for day in range(1, 31)]
    target_receipts = [
        {
            "utc_day": day,
            "day_receipt_sha256": orchestrator._canonical_sha256({"day": day}),
        }
        for day in selected
    ]
    folds = offline._fold_manifest(selected, selection_sha256="e" * 64)
    source: dict[str, object] = {
        "identity": offline.IDENTITY,
        "selected_days": selected,
        "target_day_receipts": target_receipts,
        "fold_manifest": folds,
        "selection_sha256": "e" * 64,
        "canonical_manifest_sha256": "s" * 64,
    }
    nested_folds = offline.derive_bound_nested_fold_manifest(source)
    source_path = tmp_path / "source.json"
    _write_json(source_path, source)
    monkeypatch.setattr(
        orchestrator.offline,
        "validate_canonical_manifest",
        lambda *_args, **_kwargs: source,
    )

    panel_files = {}
    for role in orchestrator._PANEL_FILES:
        path = tmp_path / f"{role}.bin"
        path.write_bytes(role.encode("ascii"))
        panel_files[role] = {
            **_binding(path),
            "rows": orchestrator.CPP_BUILDER_PREFLIGHT_OPPORTUNITIES,
            "row_key_sha256": "f" * 64,
        }
    sequential_files = {}
    for role in ("manifest", "merged_panel_manifest", "portable_replay_binding"):
        path = tmp_path / f"sequential-{role}.json"
        path.write_text("{}\n", encoding="ascii")
        sequential_files[role] = _binding(path)
    owner_files: dict[str, dict[str, str]] = {}
    owner_hashes = {
        "policy": offline.ACTIVE_OWNER_POLICY_SHA256,
        "predicate_bundle": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "private_config": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    }
    for role, digest in owner_hashes.items():
        path = tmp_path / f"owner-{role}.bin"
        path.write_bytes(role.encode("ascii"))
        owner_files[role] = {"path": str(path), "sha256": digest}
    real_file_sha256 = orchestrator._file_sha256
    owner_path_hashes = {
        Path(binding["path"]).resolve(): binding["sha256"] for binding in owner_files.values()
    }
    monkeypatch.setattr(
        orchestrator,
        "_file_sha256",
        lambda path: owner_path_hashes.get(Path(path).resolve(), real_file_sha256(path)),
    )
    panel: dict[str, object] = {
        "schema_version": orchestrator.PANEL_SCHEMA_VERSION,
        "identity": offline.IDENTITY,
        "mechanics_identity": orchestrator.mechanics.MECHANICS_IDENTITY,
        "status": "offline_outcome_blind_sequential_mechanics_panel_admitted",
        "formal_execution_eligible": True,
        "sequential_panel_builder": {
            "identity": "synthetic-sequential-panel-v2",
            "status": "outcome_blind_b0_mechanics_days_admitted",
            "selected_days": selected,
            "selected_day_count": offline.REQUIRED_DAYS,
            **sequential_files,
            "input_binding_sha256": "b" * 64,
            "sequential_replay_input_identity": "synthetic-replay-input-v2",
            "day_manifest_sha256": {day: "c" * 64 for day in selected},
            "permissions": {
                "economic_outcomes_read": False,
                "labels_read": False,
                "candidate_actions_generated": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        },
        "source_authority": orchestrator.mechanics.SOURCE_AUTHORITY,
        "source_manifest": _binding(source_path),
        "source_manifest_sha256": source["canonical_manifest_sha256"],
        "selected_days": selected,
        "panel_role": offline.PANEL_ROLE,
        "queue_identity": offline.QUEUE_IDENTITY,
        "exact_queue_policy_eligible": False,
        "same_millisecond_ambiguity_policy": "censor",
        "economic_outcomes_present": False,
        "one_shot_training_labels_precomputed": False,
        "outer_train_label_generation_required": True,
        "one_shot_effect_aggregation_used": False,
        "repeated_sequential_policy_required": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "exact_current_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_current_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "exact_current_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "owner_artifacts": owner_files,
        "permissions": {
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "day_receipt_sha256": {
            row["utc_day"]: row["day_receipt_sha256"] for row in target_receipts
        },
        "files": panel_files,
    }
    panel["canonical_panel_manifest_sha256"] = orchestrator._document_sha256(
        panel, "canonical_panel_manifest_sha256"
    )
    panel_path = tmp_path / "panel.json"
    _write_json(panel_path, panel)

    execution: dict[str, object] = {
        "schema_version": orchestrator.SCHEMA_VERSION,
        "identity": orchestrator.IDENTITY,
        "status": "pre_economic_formal_execution_bound",
        "repository_root": "${NARROWGATE_ROOT}",
        "public_base_commit": "a" * 40,
        "annotated_tag": "research/f05/offline-v1",
        "source_manifest": _binding(source_path),
        "panel_manifest": _binding(panel_path),
        "fold_manifest_sha256": folds["fold_manifest_sha256"],
        "nested_fold_manifest": nested_folds,
        "nested_fold_manifest_sha256": nested_folds["nested_fold_manifest_sha256"],
        "source_contract": {
            "panel_role": offline.PANEL_ROLE,
            "queue_identity": offline.QUEUE_IDENTITY,
            "selected_day_count": offline.REQUIRED_DAYS,
            "selection_sha256": source["selection_sha256"],
            "day_receipts_revalidated": True,
            "economic_outcomes_read": False,
        },
        "backend": {
            "module": orchestrator.CANONICAL_BACKEND_MODULE,
            "function": orchestrator.CANONICAL_BACKEND_FUNCTION,
            "custom_evaluator_allowed": False,
        },
        "execution_contract": {
            "control": "B0_CURRENT_EXACT",
            "sequential_repeated_policy": True,
            "one_shot_effect_aggregation_used": False,
            "outer_test_candidate_freeze_required": True,
            "action_alpha_v1_required": True,
        },
        "executor": orchestrator.formal_executor_contract(),
        "cpp_one_shot_qualification": orchestrator._cpp_qualification_contract(),
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    execution["canonical_execution_manifest_sha256"] = orchestrator._document_sha256(
        execution, "canonical_execution_manifest_sha256"
    )
    execution_path = tmp_path / "execution.json"
    _write_json(execution_path, execution)
    _write_cpp_qualification_receipt(execution_path, execution, source, panel)
    census_path = (
        execution_path.parent / orchestrator.COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME
    )
    census = json.loads(census_path.read_text(encoding="utf-8"))
    monkeypatch.setattr(
        orchestrator,
        "_completed_buy_cache_census",
        lambda _bundle: census,
    )
    return execution_path, execution, panel, source


def _write_cpp_qualification_receipt(
    execution_path: Path,
    execution: dict[str, object],
    source: dict[str, object],
    panel: dict[str, object],
) -> None:
    research_contract = orchestrator._research_contract_snapshot(
        manifest=execution,
        source=source,
        panel=panel,
    )
    invariance_receipt: dict[str, object] = {
        "schema_version": f"{orchestrator.V24_V26_INVARIANCE_IDENTITY}.receipt.v1",
        "identity": orchestrator.V24_V26_INVARIANCE_IDENTITY,
        "status": orchestrator.V24_V26_INVARIANCE_STATUS,
        "formal_v24": {
            "canonical_execution_manifest_sha256": (
                orchestrator.V24_EXECUTION_MANIFEST_CANONICAL_SHA256
            ),
            "manifest_file_sha256": orchestrator.V24_EXECUTION_MANIFEST_FILE_SHA256,
            "public_base_commit": orchestrator.V24_PUBLIC_BASE_COMMIT,
            "annotated_tag": orchestrator.V24_ANNOTATED_TAG,
        },
        "formal_v26": {
            "canonical_execution_manifest_sha256": execution[
                "canonical_execution_manifest_sha256"
            ],
            "manifest_file_sha256": orchestrator._file_sha256(execution_path),
            "public_base_commit": execution["public_base_commit"],
            "annotated_tag": execution["annotated_tag"],
        },
        "research_contract": research_contract,
        "research_contract_sha256": orchestrator._canonical_sha256(research_contract),
        "semantic_source_hashes": {},
        "semantic_source_count": len(
            orchestrator._V24_V26_RESEARCH_SEMANTIC_SOURCE_PATHS
        ),
        "execution_only_source_hashes": {},
        "execution_only_source_count": len(
            orchestrator._V24_V26_EXECUTION_ONLY_SOURCE_PATHS
        ),
        "completed_side_resume": orchestrator.completed_side_resume_contract(),
        "execution_only_repair": "mmap_lifetime_and_completed_side_semantic_resume",
        "data_changed": False,
        "candidate_ladder_changed": False,
        "duration_vocabulary_changed": False,
        "estimand_changed": False,
        "statistics_changed": False,
        "economic_outcomes_read": False,
        "economic_values_persisted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    invariance_receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        invariance_receipt,
        "canonical_receipt_sha256",
    )
    _write_json(
        execution_path.parent / orchestrator.V24_V26_INVARIANCE_RECEIPT_NAME,
        invariance_receipt,
    )
    census_receipt: dict[str, object] = {
        "successor_execution_manifest_sha256": execution[
            "canonical_execution_manifest_sha256"
        ],
        "completed_side_resume": orchestrator.completed_side_resume_contract(),
        "economic_values_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    census_receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        census_receipt,
        "canonical_receipt_sha256",
    )
    _write_json(
        execution_path.parent / orchestrator.COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME,
        census_receipt,
    )
    builder_receipt: dict[str, object] = {
        "schema_version": ("f05_cpp_one_shot_real_day_all_arm_lockstep_v26.builder_preflight.v1"),
        "identity": orchestrator.CPP_BUILDER_PREFLIGHT_IDENTITY,
        "status": orchestrator.CPP_BUILDER_PREFLIGHT_STATUS,
        "execution_manifest_sha256": execution["canonical_execution_manifest_sha256"],
        "source_manifest_sha256": source["canonical_manifest_sha256"],
        "panel_manifest_sha256": panel["canonical_panel_manifest_sha256"],
        "opportunity_count": orchestrator.CPP_BUILDER_PREFLIGHT_OPPORTUNITIES,
        "cpp_startup_contract_validation": True,
        "cpp_startup_validated_row_count": (
            orchestrator.CPP_BUILDER_PREFLIGHT_OPPORTUNITIES
        ),
        "formal_v24_to_v26_invariance_receipt_sha256": invariance_receipt[
            "canonical_receipt_sha256"
        ],
        "economic_evaluator_call_count": 0,
        "economic_values_read": False,
        "economic_values_persisted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    builder_receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        builder_receipt,
        "canonical_receipt_sha256",
    )
    _write_json(
        execution_path.parent / orchestrator.CPP_BUILDER_PREFLIGHT_RECEIPT_NAME,
        builder_receipt,
    )
    quick_receipt: dict[str, object] = {
        "schema_version": f"{orchestrator.CPP_QUICK_PREFLIGHT_IDENTITY}.receipt.v1",
        "identity": orchestrator.CPP_QUICK_PREFLIGHT_IDENTITY,
        "status": orchestrator.CPP_QUICK_PREFLIGHT_STATUS,
        "execution_manifest_sha256": execution["canonical_execution_manifest_sha256"],
        "opportunity_count": 1,
        "arm_count": 8,
        "zero_mismatch_arm_count": 8,
        "all_panel_builder_preflight_receipt_sha256": builder_receipt[
            "canonical_receipt_sha256"
        ],
        "formal_v24_to_v26_invariance_receipt_sha256": invariance_receipt[
            "canonical_receipt_sha256"
        ],
        "economic_values_persisted": False,
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    quick_receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        quick_receipt,
        "canonical_receipt_sha256",
    )
    _write_json(
        execution_path.parent / orchestrator.CPP_QUICK_PREFLIGHT_RECEIPT_NAME,
        quick_receipt,
    )
    qualification = {
        "schema_version": "f05_cpp_one_shot_real_day_all_arm_lockstep_v26.contract.v1",
        "execution_manifest_sha256": execution["canonical_execution_manifest_sha256"],
        "source_manifest_sha256": source["canonical_manifest_sha256"],
        "panel_manifest_sha256": panel["canonical_panel_manifest_sha256"],
        "public_base_commit": execution["public_base_commit"],
        "annotated_tag": execution["annotated_tag"],
        "qualification_day": "2026-07-01",
        "opportunity_count": 2,
        "arm_count": 16,
        "all_panel_builder_preflight_receipt_sha256": builder_receipt["canonical_receipt_sha256"],
        "all_panel_builder_preflight_opportunity_count": (
            orchestrator.CPP_BUILDER_PREFLIGHT_OPPORTUNITIES
        ),
        "formal_v24_to_v26_invariance_receipt_sha256": invariance_receipt[
            "canonical_receipt_sha256"
        ],
        "first_opportunity_all_arm_preflight_receipt_sha256": quick_receipt[
            "canonical_receipt_sha256"
        ],
        "source_hashes": {"cpp_extension": "d" * 64},
    }
    receipt: dict[str, object] = {
        "schema_version": "f05_cpp_one_shot_real_day_all_arm_lockstep_v26.receipt.v1",
        "identity": orchestrator.CPP_QUALIFICATION_IDENTITY,
        "status": "passed_real_day_all_opportunity_all_arm_lockstep",
        "qualification_contract": qualification,
        "qualification_sha256": orchestrator._canonical_sha256(qualification),
        "opportunity_count": 2,
        "arm_count": 16,
        "zero_mismatch_arm_count": 16,
        "cpp_one_shot_formal_authorized": True,
        "python_sequential_engine_remains_authoritative": True,
        "economic_values_persisted": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    _write_json(
        execution_path.parent / orchestrator.CPP_QUALIFICATION_RECEIPT_NAME,
        receipt,
    )


def _rewrite_execution(path: Path, payload: dict[str, object]) -> None:
    payload["canonical_execution_manifest_sha256"] = orchestrator._document_sha256(
        payload, "canonical_execution_manifest_sha256"
    )
    _write_json(path, payload)


def _rewrite_panel(
    execution_path: Path,
    execution: dict[str, object],
    panel: dict[str, object],
) -> None:
    panel_binding = execution["panel_manifest"]
    assert isinstance(panel_binding, dict)
    panel_path = Path(str(panel_binding["path"]))
    panel["canonical_panel_manifest_sha256"] = orchestrator._document_sha256(
        panel, "canonical_panel_manifest_sha256"
    )
    _write_json(panel_path, panel)
    execution["panel_manifest"] = _binding(panel_path)
    _rewrite_execution(execution_path, execution)


def test_only_hash_bound_manifest_enters_formal_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, _execution, _panel, source = _bundle_fixture(tmp_path, monkeypatch)
    bundle = orchestrator._load_formal_offline_bundle(
        path,
        verify_source_bytes=False,
        require_clean_tag=False,
    )
    assert bundle.source_manifest is source
    assert set(bundle.panel_files) == set(orchestrator._PANEL_FILES)
    assert "action_outcomes" not in bundle.panel_files
    assert "action_supported" not in bundle.panel_files
    with pytest.raises(SystemExit):
        orchestrator.parse_args(["run", str(path), "--evaluator", "custom"])
    with pytest.raises(SystemExit):
        orchestrator.parse_args(
            ["bind", str(path), str(tmp_path / "out.json"), "--days", "2026-07-01"]
        )
    with pytest.raises(SystemExit):
        orchestrator.parse_args(["bind", str(path), str(tmp_path / "out.json"), "--fold", "custom"])
    mechanics = orchestrator.parse_args(
        [
            "diagnose-one-day",
            str(path),
            "--output",
            str(tmp_path / "mechanics.json"),
        ]
    )
    assert mechanics.command == "diagnose-one-day"


def test_formal_loader_requires_immutable_cpp_lockstep_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, _execution, _panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.CPP_QUALIFICATION_RECEIPT_NAME
    receipt_path.unlink()

    with pytest.raises(orchestrator.OfflineOrchestratorError, match="cannot load"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_formal_loader_requires_all_panel_builder_preflight_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, *_ = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.CPP_BUILDER_PREFLIGHT_RECEIPT_NAME
    receipt_path.unlink()

    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="builder preflight receipt",
    ):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
            require_cpp_qualification=True,
        )


def test_formal_loader_requires_first_opportunity_all_arm_preflight_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, *_ = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.CPP_QUICK_PREFLIGHT_RECEIPT_NAME
    receipt_path.unlink()

    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="first-opportunity all-arm preflight receipt",
    ):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
            require_cpp_qualification=True,
        )


def test_formal_loader_rejects_quick_preflight_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, *_ = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.CPP_QUICK_PREFLIGHT_RECEIPT_NAME
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["zero_mismatch_arm_count"] = 7
    receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        receipt,
        "canonical_receipt_sha256",
    )
    _write_json(receipt_path, receipt)

    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="first-opportunity all-arm preflight receipt failed closed",
    ):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
            require_cpp_qualification=True,
        )


def test_formal_loader_requires_v24_to_v26_invariance_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, *_ = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.V24_V26_INVARIANCE_RECEIPT_NAME
    receipt_path.unlink()

    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="invariance receipt",
    ):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_formal_loader_requires_completed_buy_cache_census_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, *_ = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME
    receipt_path.unlink()

    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="completed BUY cache census",
    ):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_formal_loader_rejects_completed_buy_cache_census_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, *_ = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["completed_side_resume"]["required_complete_cache_units"] = 576
    receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        receipt,
        "canonical_receipt_sha256",
    )
    _write_json(receipt_path, receipt)

    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="census receipt failed closed",
    ):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_formal_loader_rejects_recomputed_invariance_contract_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, *_ = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.V24_V26_INVARIANCE_RECEIPT_NAME
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["research_contract"]["candidate_ladder"].append("OUTCOME_INFORMED")
    receipt["research_contract_sha256"] = orchestrator._canonical_sha256(
        receipt["research_contract"]
    )
    receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        receipt,
        "canonical_receipt_sha256",
    )
    _write_json(receipt_path, receipt)

    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="invariance receipt failed closed",
    ):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_execution_only_invariance_rejects_research_contract_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, successor, *_ = _bundle_fixture(tmp_path, monkeypatch)
    predecessor = json.loads(json.dumps(successor))
    predecessor["public_base_commit"] = orchestrator.V23_PUBLIC_BASE_COMMIT
    predecessor["annotated_tag"] = orchestrator.V23_ANNOTATED_TAG
    predecessor["canonical_execution_manifest_sha256"] = (
        orchestrator.V23_EXECUTION_MANIFEST_CANONICAL_SHA256
    )
    predecessor["executor"]["identity"] = (
        "f05_full_multiscale_offline_replay_executor_cpp_one_shot_v3"
    )
    predecessor["executor"].pop(
        "builder_instantiates_formal_cpp_runtime_required",
        None,
    )
    predecessor["cpp_one_shot_qualification"]["identity"] = (
        "f05_cpp_one_shot_real_day_all_arm_lockstep_v23"
    )
    for field in (
        "invariance_receipt_file",
        "builder_formal_runtime_instantiation_required",
    ):
        predecessor["cpp_one_shot_qualification"].pop(field, None)

    orchestrator._execution_only_manifest_invariance(predecessor, successor)
    successor["execution_contract"]["control"] = "CONTROL_85N"
    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="outside the execution-only allowance",
    ):
        orchestrator._execution_only_manifest_invariance(predecessor, successor)


def test_v24_v26_invariance_separates_research_and_execution_sources() -> None:
    backend_path = (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_repeated_policy_backend_v1.py"
    )
    assert backend_path in orchestrator._V24_V26_EXECUTION_ONLY_SOURCE_PATHS
    assert backend_path not in orchestrator._V24_V26_RESEARCH_SEMANTIC_SOURCE_PATHS
    assert len(orchestrator._V24_V26_EXECUTION_ONLY_SOURCE_PATHS) == 4


def test_v24_v26_execution_only_invariance_rejects_research_contract_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _path, successor, *_ = _bundle_fixture(tmp_path, monkeypatch)
    predecessor = json.loads(json.dumps(successor))
    predecessor["public_base_commit"] = orchestrator.V24_PUBLIC_BASE_COMMIT
    predecessor["annotated_tag"] = orchestrator.V24_ANNOTATED_TAG
    predecessor["canonical_execution_manifest_sha256"] = (
        orchestrator.V24_EXECUTION_MANIFEST_CANONICAL_SHA256
    )
    predecessor["executor"]["identity"] = (
        "f05_full_multiscale_offline_replay_executor_cpp_one_shot_v4"
    )
    predecessor["executor"].pop("completed_side_resume")
    predecessor["cpp_one_shot_qualification"]["identity"] = (
        "f05_cpp_one_shot_real_day_all_arm_lockstep_v24"
    )
    predecessor["cpp_one_shot_qualification"]["invariance_receipt_file"] = (
        orchestrator.V23_V24_INVARIANCE_RECEIPT_NAME
    )

    orchestrator._execution_only_manifest_invariance_v24_v26(
        predecessor,
        successor,
    )
    successor["execution_contract"]["control"] = "CONTROL_85N"
    with pytest.raises(
        orchestrator.OfflineOrchestratorError,
        match="outside the execution-only allowance",
    ):
        orchestrator._execution_only_manifest_invariance_v24_v26(
            predecessor,
            successor,
        )


def test_cpp_lockstep_receipt_rejects_nonzero_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, _execution, _panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    receipt_path = path.parent / orchestrator.CPP_QUALIFICATION_RECEIPT_NAME
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["zero_mismatch_arm_count"] = 15
    receipt["canonical_receipt_sha256"] = orchestrator._document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    _write_json(receipt_path, receipt)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match="failed closed"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_cpp_lockstep_receipt_rebinds_loaded_extension_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, execution, _panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        orchestrator,
        "_current_cpp_qualification_source_hashes",
        lambda: {"cpp_extension": "d" * 64},
    )
    orchestrator._validate_cpp_qualification_receipt(
        path,
        execution,
        verify_runtime_artifacts=True,
    )

    monkeypatch.setattr(
        orchestrator,
        "_current_cpp_qualification_source_hashes",
        lambda: {"cpp_extension": "e" * 64},
    )
    with pytest.raises(orchestrator.OfflineOrchestratorError, match="extension bytes"):
        orchestrator._validate_cpp_qualification_receipt(
            path,
            execution,
            verify_runtime_artifacts=True,
        )


def test_legacy_unbound_mechanics_panel_cannot_enter_formal_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    panel["schema_version"] = orchestrator.mechanics.LEGACY_PANEL_SCHEMA_VERSION
    _rewrite_panel(path, execution, panel)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match="schema"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_formal_bundle_cannot_be_constructed_by_a_caller(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        orchestrator.FormalOfflineBundle(
            execution_manifest_path=tmp_path / "execution.json",
            execution_manifest={},
            source_manifest_path=tmp_path / "source.json",
            source_manifest={},
            panel_manifest_path=tmp_path / "panel.json",
            panel_manifest={},
            panel_files={},
            repository_root=tmp_path,
        )


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    (
        ("backend", "custom_evaluator_allowed", True, "backend identity"),
        ("execution_contract", "one_shot_effect_aggregation_used", True, "execution contract"),
        ("execution_contract", "control", "CONTROL_85N", "execution contract"),
        ("permissions", "validation_read", True, "permissions"),
        ("permissions", "sealed_holdout_read", True, "permissions"),
    ),
)
def test_formal_manifest_bypasses_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    section: str,
    field: str,
    value: object,
    message: str,
) -> None:
    path, execution, _panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    nested = execution[section]
    assert isinstance(nested, dict)
    nested[field] = value
    _rewrite_execution(path, execution)
    with pytest.raises(orchestrator.OfflineOrchestratorError, match=message):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_manifest_hash_and_panel_day_order_tampering_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    execution["annotated_tag"] = "changed"
    _write_json(path, execution)
    with pytest.raises(orchestrator.OfflineOrchestratorError, match="manifest hash"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


@pytest.mark.parametrize("tamper", ("inner_day", "nested_sha"))
def test_nested_four_by_three_fold_tampering_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    path, execution, _panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    if tamper == "inner_day":
        nested_folds = execution["nested_fold_manifest"]
        assert isinstance(nested_folds, dict)
        outer_folds = nested_folds["outer_folds"]
        assert isinstance(outer_folds, list)
        inner_folds = outer_folds[0]["inner_folds"]
        inner_folds[0]["test_days"][0] = "2026-01-01"
        nested_folds["nested_fold_manifest_sha256"] = offline.canonical_document_sha256(
            nested_folds,
            "nested_fold_manifest_sha256",
        )
        execution["nested_fold_manifest_sha256"] = nested_folds["nested_fold_manifest_sha256"]
    else:
        execution["nested_fold_manifest_sha256"] = "0" * 64
    _rewrite_execution(path, execution)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match="nested-fold"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )

    path, execution, panel, _source = _bundle_fixture(tmp_path / "second", monkeypatch)
    panel["selected_days"] = list(reversed(panel["selected_days"]))
    _rewrite_panel(path, execution, panel)
    with pytest.raises(orchestrator.OfflineOrchestratorError, match="day order"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


@pytest.mark.parametrize("role", ("action_outcomes", "action_supported"))
def test_economic_label_files_are_rejected_from_mechanics_panel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    role: str,
) -> None:
    path, execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    economic_file = tmp_path / f"{role}.bin"
    economic_file.write_bytes(b"forbidden economic labels")
    files = panel["files"]
    assert isinstance(files, dict)
    files[role] = _binding(economic_file)
    _rewrite_panel(path, execution, panel)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match="economic label files"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("economic_outcomes_present", True, "economic_outcomes_present=false"),
        (
            "one_shot_training_labels_precomputed",
            True,
            "one_shot_training_labels_precomputed=false",
        ),
        (
            "outer_train_label_generation_required",
            False,
            "outer_train_label_generation_required=true",
        ),
    ),
)
def test_non_outcome_blind_panel_declarations_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: bool,
    message: str,
) -> None:
    path, execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    panel[field] = value
    _rewrite_panel(path, execution, panel)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match=message):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("source_authority", "receive_time", "source authority"),
        ("exact_queue_policy_eligible", True, "exact queue authority"),
        ("exact_current_private_config_sha256", "0" * 64, "private-config"),
    ),
)
def test_panel_cannot_drift_source_queue_or_exact_owner_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
    message: str,
) -> None:
    path, execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    panel[field] = value
    _rewrite_panel(path, execution, panel)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match=message):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_panel_cannot_claim_action_authority_or_drop_owner_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    permissions = panel["permissions"]
    assert isinstance(permissions, dict)
    permissions["action_authorized"] = True
    _rewrite_panel(path, execution, panel)
    with pytest.raises(orchestrator.OfflineOrchestratorError, match="permissions"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )

    path, execution, panel, _source = _bundle_fixture(tmp_path / "second", monkeypatch)
    owners = panel["owner_artifacts"]
    assert isinstance(owners, dict)
    owners.pop("private_config")
    _rewrite_panel(path, execution, panel)
    with pytest.raises(orchestrator.OfflineOrchestratorError, match="owner artifact census"):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


@pytest.mark.parametrize(
    "field",
    (
        "economic_outcomes_present",
        "one_shot_training_labels_precomputed",
        "outer_train_label_generation_required",
    ),
)
def test_outcome_blind_panel_declarations_must_be_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    path, execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    panel.pop(field)
    _rewrite_panel(path, execution, panel)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match=field):
        orchestrator._load_formal_offline_bundle(
            path,
            verify_source_bytes=False,
            require_clean_tag=False,
        )


def test_dirty_worktree_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        orchestrator,
        "_git",
        lambda *args, **_kwargs: (
            " M strategy/file.py" if args[:2] == ("status", "--porcelain") else ""
        ),
    )
    with pytest.raises(orchestrator.OfflineOrchestratorError, match="clean worktree"):
        orchestrator._validate_clean_annotated_tag(
            repository_root=tmp_path,
            commit_sha="a" * 40,
            tag="research/f05/offline-v1",
        )


def test_canonical_bind_derives_every_formal_field_from_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _execution_path, _execution, panel, source = _bundle_fixture(tmp_path, monkeypatch)
    panel_path = tmp_path / "panel.json"
    output = tmp_path / "formal.json"
    monkeypatch.setattr(orchestrator.mechanics, "validate_panel", lambda *_args, **_kwargs: panel)

    def fake_git(*args: str, root: Path) -> str:
        del root
        if args == ("status", "--porcelain"):
            return ""
        if args == ("rev-parse", "HEAD"):
            return "a" * 40
        if args == ("cat-file", "-t", "refs/tags/research/f05/offline-v1"):
            return "tag"
        if args == ("rev-parse", "refs/tags/research/f05/offline-v1^{}"):
            return "a" * 40
        raise AssertionError(args)

    monkeypatch.setattr(orchestrator, "_git", fake_git)
    qualification_loads: list[Path] = []
    monkeypatch.setattr(
        orchestrator,
        "load_formal_offline_bundle_for_cpp_qualification",
        lambda path, **_kwargs: qualification_loads.append(path),
    )
    monkeypatch.setattr(
        orchestrator,
        "load_formal_offline_bundle",
        lambda *_args, **_kwargs: pytest.fail(
            "bind cannot require a receipt that does not exist yet"
        ),
    )
    result = orchestrator.bind_formal_execution_manifest(
        panel_path,
        output,
        annotated_tag="research/f05/offline-v1",
        repository_root=tmp_path,
    )

    assert result["source_manifest"]["sha256"] == orchestrator._file_sha256(
        tmp_path / "source.json"
    )
    assert result["panel_manifest"]["sha256"] == orchestrator._file_sha256(panel_path)
    assert result["fold_manifest_sha256"] == source["fold_manifest"]["fold_manifest_sha256"]
    nested_folds = result["nested_fold_manifest"]
    assert len(nested_folds["outer_folds"]) == 4
    assert all(len(row["inner_folds"]) == 3 for row in nested_folds["outer_folds"])
    assert result["nested_fold_manifest_sha256"] == nested_folds["nested_fold_manifest_sha256"]
    assert result["backend"]["custom_evaluator_allowed"] is False
    assert result["execution_contract"]["one_shot_effect_aggregation_used"] is False
    assert result["executor"] == orchestrator.formal_executor_contract()
    assert result["permissions"] == {
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    assert qualification_loads == [output]
    assert json.loads(output.read_text(encoding="utf-8")) == result


def test_canonical_bind_refuses_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _execution_path, _execution, panel, _source = _bundle_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(orchestrator.mechanics, "validate_panel", lambda *_args, **_kwargs: panel)
    monkeypatch.setattr(orchestrator, "_validate_clean_annotated_tag", lambda **_kwargs: None)
    monkeypatch.setattr(
        orchestrator,
        "_git",
        lambda *args, **_kwargs: "a" * 40 if args == ("rev-parse", "HEAD") else "",
    )
    output = tmp_path / "formal.json"
    output.write_text("{}", encoding="ascii")

    with pytest.raises(orchestrator.OfflineOrchestratorError, match="already exists"):
        orchestrator.bind_formal_execution_manifest(
            tmp_path / "panel.json",
            output,
            annotated_tag="research/f05/offline-v1",
            repository_root=tmp_path,
        )


def test_cli_preflight_reports_backend_blocker_instead_of_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = tmp_path / "formal.json"
    manifest.write_text("{}", encoding="ascii")
    blocked = {
        "identity": offline.IDENTITY,
        "status": "mechanics_only_backend_incomplete",
        "economic_outcomes_read": False,
        "blocker": "canonical_backtest_tick_arm_executor_binding",
    }
    fake_backend = SimpleNamespace(
        preflight_canonical_offline_economics=lambda path: blocked if path == manifest else None
    )
    monkeypatch.setattr(orchestrator.importlib, "import_module", lambda _name: fake_backend)

    assert orchestrator.main(["preflight", str(manifest)]) == 0

    emitted = json.loads(capsys.readouterr().out)
    assert emitted == blocked
    assert emitted["status"] != "formal_offline_replay_mechanics_ready"


def test_cli_preflight_can_atomically_write_a_canonical_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = tmp_path / "formal.json"
    manifest.write_text("{}", encoding="ascii")
    output = tmp_path / "preflight.json"
    blocked = {
        "identity": offline.IDENTITY,
        "status": "blocked_missing_canonical_fields",
        "economic_outcomes_read": False,
        "missing_canonical_fields": ["campaign_id"],
    }
    fake_backend = SimpleNamespace(preflight_canonical_offline_economics=lambda _path: blocked)
    monkeypatch.setattr(orchestrator.importlib, "import_module", lambda _name: fake_backend)

    assert orchestrator.main(["preflight", str(manifest), "--output", str(output)]) == 0

    emitted = json.loads(capsys.readouterr().out)
    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert emitted == persisted
    assert persisted["canonical_preflight_sha256"] == orchestrator._document_sha256(
        persisted,
        "canonical_preflight_sha256",
    )


def test_cli_preflight_refuses_to_replace_an_immutable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "formal.json"
    manifest.write_text("{}", encoding="ascii")
    output = tmp_path / "preflight.json"
    output.write_text('{"existing":true}\n', encoding="ascii")
    fake_backend = SimpleNamespace(
        preflight_canonical_offline_economics=lambda _path: {
            "identity": offline.IDENTITY,
            "status": "blocked_missing_canonical_fields",
            "economic_outcomes_read": False,
            "missing_canonical_fields": ["campaign_id"],
        }
    )
    monkeypatch.setattr(orchestrator.importlib, "import_module", lambda _name: fake_backend)

    with pytest.raises(orchestrator.OfflineOrchestratorError, match="already exists"):
        orchestrator.main(["preflight", str(manifest), "--output", str(output)])

    assert json.loads(output.read_text(encoding="ascii")) == {"existing": True}
