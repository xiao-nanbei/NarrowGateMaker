#!/usr/bin/env python3
"""Canonical formal entry for the F05 formal-v27 SELL-only component.

The execution never inherits a cross-execution strategy-dependent cache.  Every
SELL request is either computed under the exact v27 manifest or resumed from an
exact v27 cache key.  The completed formal-v24 BUY side remains a separately
identified component and cannot be represented as a v27 replay result.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_mechanics_v1 as mechanics,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as base,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

IDENTITY = f"{offline.IDENTITY}.formal_sell_only_orchestrator_v1"
SCHEMA_VERSION = f"{IDENTITY}.execution_manifest.v1"
FORMAL_RESULT_SCHEMA = f"{IDENTITY}.formal_result.v1"
CANONICAL_BACKEND_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_sell_only_backend_v1"
)
CANONICAL_BACKEND_FUNCTION = "run_canonical_offline_sell_economics"
FORMAL_SIDES = ("SELL",)
EXECUTOR_IDENTITY = "f05_full_multiscale_offline_replay_executor_sell_only_v7"
INVARIANCE_IDENTITY = "f05_formal_v26_to_v27_sell_only_invariance_v1"
INVARIANCE_RECEIPT_NAME = "formal_v26_to_v27_sell_only_invariance_receipt.json"
INVARIANCE_STATUS = "passed_sell_only_component_research_contract_invariance"
CPP_QUALIFICATION_REUSE_IDENTITY = "f05_cpp_v26_both_side_to_v27_sell_only_reuse_v1"
CPP_QUALIFICATION_REUSE_RECEIPT_NAME = "cpp_v26_qualification_reuse_receipt.json"
PREDECESSOR_CPP_QUALIFICATION_COPY_NAME = "cpp_v26_real_day_lockstep_receipt.json"
CPP_QUALIFICATION_REUSE_STATUS = "passed_v26_both_side_superset_for_v27_sell_only"
V26_EXECUTION_MANIFEST_CANONICAL_SHA256 = (
    "e0f4d92181609a6d6b1794b4ff88850775d01538f5642018254fb004e55f85df"
)
V26_EXECUTION_MANIFEST_FILE_SHA256 = (
    "07ce367420b30e4fb21b2f41063a30785b8056240bf55beab7257e715661e1f3"
)
V26_PUBLIC_BASE_COMMIT = "20b60cd8098383b4ebda9d3e0688188987389038"
V26_ANNOTATED_TAG = (
    "research/f05/causal-multichannel-window-boolean-cooldown-full-multiscale-"
    "successor-offline/formal-semantic-safe-sell-resume-v26-20260818"
)
V26_CPP_QUALIFICATION_IDENTITY = "f05_cpp_one_shot_real_day_all_arm_lockstep_v26"
V26_CPP_QUALIFICATION_FILE_SHA256 = (
    "572fd2d5a08851087106adc202218f5c99131c57fb94821566a1dcafcc5f55b0"
)
V26_CPP_QUALIFICATION_CANONICAL_SHA256 = (
    "19072deaa984868d6e8934df63f71a7667eea49335b245903103b8f83f99d0b0"
)
V24_EXECUTION_MANIFEST_CANONICAL_SHA256 = base.V24_EXECUTION_MANIFEST_CANONICAL_SHA256
V24_PUBLIC_BASE_COMMIT = base.V24_PUBLIC_BASE_COMMIT
V24_ANNOTATED_TAG = base.V24_ANNOTATED_TAG
COMPLETED_V24_BUY_CACHE_UNITS = 577


class SellOnlyOrchestratorError(RuntimeError):
    """Raised when the formal SELL-only identity or composition boundary drifts."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return _canonical_sha256(payload)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SellOnlyOrchestratorError(f"cannot load {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise SellOnlyOrchestratorError(f"{label} root must be an object")
    return payload


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)


def formal_executor_contract() -> dict[str, Any]:
    """Return the frozen fresh-compute SELL executor contract."""

    return {
        "identity": EXECUTOR_IDENTITY,
        "formal_sides": list(FORMAL_SIDES),
        "replay_adapter_executor_identity": base.EXECUTOR_ACCELERATION_IDENTITY,
        "authoritative_engine_by_stage": {
            "outer_train_one_shot": "cpp",
            "outer_test_sequential": "python",
        },
        "global_worker_tokens": base.EXECUTOR_GLOBAL_WORKER_TOKENS,
        "nested_worker_pools_allowed": False,
        "one_shot_process_topology": dict(base.EXECUTOR_ONE_SHOT_TOPOLOGY),
        "day_input_materialization_workers": base.EXECUTOR_DAY_INPUT_MATERIALIZATION_WORKERS,
        "day_input_mmap": {
            "enabled": True,
            "identity": base.EXECUTOR_DAY_INPUT_CACHE_IDENTITY,
            "root": base.EXECUTOR_DAY_INPUT_CACHE_ROOT,
            "open_mode": "read_only",
            "content_addressed": True,
        },
        "b0_control_cache": {
            "enabled": True,
            "candidate_output_allowed": False,
            "side_excludable": False,
        },
        "cross_execution_strategy_cache_reuse_allowed": False,
        "predecessor_cache_key_transform_allowed": False,
        "same_execution_exact_key_resume_allowed": True,
        "target_request_source_coverage_required": True,
        "cpp_formal_engine_authorized": True,
        "cpp_authority_scope": "outer_train_one_shot_labels_only",
        "all_fold_zero_economic_contract_walk_required": True,
    }


def formal_execution_contract() -> dict[str, Any]:
    return {
        "control": "B0_CURRENT_EXACT",
        "formal_sides": list(FORMAL_SIDES),
        "component_scope": "sell_only_learning_algorithm_oof",
        "sequential_repeated_policy": True,
        "one_shot_effect_aggregation_used": False,
        "outer_test_candidate_freeze_required": True,
        "action_alpha_v1_required": True,
    }


def formal_composition_contract() -> dict[str, Any]:
    return {
        "status": "sell_component_only_combined_report_not_yet_authorized",
        "buy_component": {
            "source_execution_identity": "formal_v24",
            "execution_manifest_sha256": V24_EXECUTION_MANIFEST_CANONICAL_SHA256,
            "public_base_commit": V24_PUBLIC_BASE_COMMIT,
            "annotated_tag": V24_ANNOTATED_TAG,
            "required_complete_cache_units": COMPLETED_V24_BUY_CACHE_UNITS,
            "materialization_required_before_combination": True,
        },
        "sell_component": {
            "source_execution_identity": "formal_v27",
            "formal_sides": list(FORMAL_SIDES),
            "fresh_execution_only": True,
        },
        "cross_commit_composition_receipt_required": True,
        "combined_result_authorized": False,
    }


def cpp_qualification_reuse_contract() -> dict[str, Any]:
    return {
        "identity": CPP_QUALIFICATION_REUSE_IDENTITY,
        "receipt_file": CPP_QUALIFICATION_REUSE_RECEIPT_NAME,
        "predecessor_receipt_file": PREDECESSOR_CPP_QUALIFICATION_COPY_NAME,
        "predecessor_identity": V26_CPP_QUALIFICATION_IDENTITY,
        "predecessor_file_sha256": V26_CPP_QUALIFICATION_FILE_SHA256,
        "predecessor_canonical_sha256": V26_CPP_QUALIFICATION_CANONICAL_SHA256,
        "predecessor_coverage_sides": ["BUY", "SELL"],
        "formal_coverage_sides": list(FORMAL_SIDES),
        "coverage_relation": "predecessor_both_side_superset_of_sell_only",
        "zero_mismatch_arms_required": 648,
        "source_hash_equality_required": True,
        "economic_values_persisted": False,
    }


def _semantic_source_hashes(repository_root: Path) -> dict[str, dict[str, Any]]:
    hashes: dict[str, dict[str, Any]] = {}
    for relative_path in base._V24_V26_RESEARCH_SEMANTIC_SOURCE_PATHS:
        current_path = repository_root / relative_path
        if not current_path.is_file():
            raise SellOnlyOrchestratorError(
                f"research semantic source is missing: {relative_path}"
            )
        predecessor = base._git_file_bytes(
            repository_root=repository_root,
            commit=V26_PUBLIC_BASE_COMMIT,
            path=relative_path,
        )
        predecessor_sha = hashlib.sha256(predecessor).hexdigest()
        current_sha = _file_sha256(current_path)
        if predecessor_sha != current_sha:
            raise SellOnlyOrchestratorError(
                f"research semantic source changed since formal-v26: {relative_path}"
            )
        hashes[relative_path] = {
            "formal_v26_sha256": predecessor_sha,
            "formal_v27_sha256": current_sha,
            "equal": True,
        }
    return hashes


def _normalized_research_snapshot(
    *,
    manifest: Mapping[str, Any],
    source: Mapping[str, Any],
    panel: Mapping[str, Any],
) -> dict[str, Any]:
    snapshot = base._research_contract_snapshot(
        manifest=manifest,
        source=source,
        panel=panel,
    )
    snapshot["execution_contract"] = {
        "control": "B0_CURRENT_EXACT",
        "sequential_repeated_policy": True,
        "one_shot_effect_aggregation_used": False,
        "outer_test_candidate_freeze_required": True,
        "action_alpha_v1_required": True,
    }
    return snapshot


def _validate_manifest_core(
    execution_manifest_path: Path,
    *,
    verify_source_bytes: bool,
    require_clean_tag: bool,
) -> base.FormalOfflineBundle:
    path = execution_manifest_path.expanduser().resolve()
    manifest = _load_json(path, label="formal-v27 execution manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("identity") != IDENTITY:
        raise SellOnlyOrchestratorError("formal-v27 execution identity drifted")
    if manifest.get("status") != "pre_economic_formal_execution_bound":
        raise SellOnlyOrchestratorError("formal-v27 execution status drifted")
    if manifest.get("canonical_execution_manifest_sha256") != _document_sha256(
        manifest, "canonical_execution_manifest_sha256"
    ):
        raise SellOnlyOrchestratorError("formal-v27 manifest canonical hash drifted")
    if manifest.get("backend") != {
        "module": CANONICAL_BACKEND_MODULE,
        "function": CANONICAL_BACKEND_FUNCTION,
        "custom_evaluator_allowed": False,
    }:
        raise SellOnlyOrchestratorError("formal-v27 backend identity drifted")
    if manifest.get("execution_contract") != formal_execution_contract():
        raise SellOnlyOrchestratorError("formal-v27 execution contract drifted")
    executor = manifest.get("executor")
    if executor != formal_executor_contract() or "completed_side_resume" in executor:
        raise SellOnlyOrchestratorError("formal-v27 executor or no-resume contract drifted")
    if manifest.get("composition_contract") != formal_composition_contract():
        raise SellOnlyOrchestratorError("formal-v27 composition contract drifted")
    if manifest.get("cpp_one_shot_qualification") != cpp_qualification_reuse_contract():
        raise SellOnlyOrchestratorError("formal-v27 C++ qualification contract drifted")
    if manifest.get("permissions") != {
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise SellOnlyOrchestratorError("formal-v27 permissions drifted")
    try:
        repository_root = resolve_portable_path(
            str(manifest.get("repository_root")),
            root=Path(__file__).resolve().parents[4],
        ).resolve()
    except (RuntimeError, ValueError) as exc:
        raise SellOnlyOrchestratorError("repository root binding is not portable") from exc
    source_path = base._resolve_bound_file(
        manifest.get("source_manifest") or {},
        label="canonical source manifest",
        repository_root=repository_root,
    )
    source = offline.validate_canonical_manifest(
        source_path,
        rehash_sources=verify_source_bytes,
    )
    if len(source.get("selected_days", ())) != offline.REQUIRED_DAYS:
        raise SellOnlyOrchestratorError("canonical source gate lacks 30 admitted days")
    if manifest.get("source_contract") != {
        "panel_role": offline.PANEL_ROLE,
        "queue_identity": offline.QUEUE_IDENTITY,
        "selected_day_count": offline.REQUIRED_DAYS,
        "selection_sha256": source.get("selection_sha256"),
        "day_receipts_revalidated": True,
        "economic_outcomes_read": False,
    }:
        raise SellOnlyOrchestratorError("formal-v27 source contract drifted")
    panel_path = base._resolve_bound_file(
        manifest.get("panel_manifest") or {},
        label="canonical panel manifest",
        repository_root=repository_root,
    )
    if verify_source_bytes:
        try:
            panel = mechanics.validate_panel(
                panel_path,
                layout=offline.default_layout(),
                repository_root=repository_root,
            )
        except (mechanics.OfflineMechanicsError, OSError, ValueError) as exc:
            raise SellOnlyOrchestratorError("canonical mechanics panel failed admission") from exc
    else:
        panel = _load_json(panel_path, label="canonical panel manifest")
    panel_files = base._validate_panel_manifest(
        panel,
        source=source,
        repository_root=repository_root,
    )
    folds = source.get("fold_manifest")
    if not isinstance(folds, Mapping):
        raise SellOnlyOrchestratorError("source admission lacks frozen folds")
    if manifest.get("fold_manifest_sha256") != folds.get("fold_manifest_sha256"):
        raise SellOnlyOrchestratorError("formal-v27 fold binding drifted")
    expected_nested = offline.derive_bound_nested_fold_manifest(source)
    if (
        manifest.get("nested_fold_manifest") != expected_nested
        or manifest.get("nested_fold_manifest_sha256")
        != expected_nested.get("nested_fold_manifest_sha256")
    ):
        raise SellOnlyOrchestratorError("formal-v27 nested-fold binding drifted")
    if require_clean_tag:
        base._validate_clean_annotated_tag(
            repository_root=repository_root,
            commit_sha=str(manifest.get("public_base_commit", "")),
            tag=str(manifest.get("annotated_tag", "")),
        )
    return base._new_formal_offline_bundle(
        execution_manifest_path=path,
        execution_manifest=manifest,
        source_manifest_path=source_path,
        source_manifest=source,
        panel_manifest_path=panel_path,
        panel_manifest=panel,
        panel_files=panel_files,
        repository_root=repository_root,
    )


def bind_formal_sell_only_execution_manifest(
    panel_manifest_path: Path,
    output_path: Path,
    *,
    annotated_tag: str,
    repository_root: Path | None = None,
) -> Mapping[str, Any]:
    root = (repository_root or Path(__file__).resolve().parents[4]).expanduser().resolve()
    destination = output_path.expanduser().resolve()
    if destination.exists():
        raise SellOnlyOrchestratorError(f"immutable v27 manifest exists: {destination}")
    commit_sha = base._git("rev-parse", "HEAD", root=root)
    base._validate_clean_annotated_tag(
        repository_root=root,
        commit_sha=commit_sha,
        tag=annotated_tag,
    )
    panel_path = panel_manifest_path.expanduser().resolve()
    panel = mechanics.validate_panel(
        panel_path,
        layout=offline.default_layout(),
        repository_root=root,
    )
    source_binding = panel.get("source_manifest")
    if not isinstance(source_binding, Mapping):
        raise SellOnlyOrchestratorError("canonical panel lacks source binding")
    source_path = base._resolve_bound_file(
        source_binding,
        label="canonical source manifest",
        repository_root=root,
    )
    source = offline.validate_canonical_manifest(source_path, rehash_sources=True)
    base._validate_panel_manifest(panel, source=source, repository_root=root)
    folds = source.get("fold_manifest")
    if not isinstance(folds, Mapping):
        raise SellOnlyOrchestratorError("canonical source lacks fold identity")
    nested_folds = offline.derive_bound_nested_fold_manifest(source)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "pre_economic_formal_execution_bound",
        "repository_root": "${NARROWGATE_ROOT}",
        "public_base_commit": commit_sha,
        "annotated_tag": annotated_tag,
        "source_manifest": base._binding(source_path, repository_root=root),
        "panel_manifest": base._binding(panel_path, repository_root=root),
        "fold_manifest_sha256": folds["fold_manifest_sha256"],
        "nested_fold_manifest": nested_folds,
        "nested_fold_manifest_sha256": nested_folds["nested_fold_manifest_sha256"],
        "backend": {
            "module": CANONICAL_BACKEND_MODULE,
            "function": CANONICAL_BACKEND_FUNCTION,
            "custom_evaluator_allowed": False,
        },
        "execution_contract": formal_execution_contract(),
        "executor": formal_executor_contract(),
        "composition_contract": formal_composition_contract(),
        "cpp_one_shot_qualification": cpp_qualification_reuse_contract(),
        "source_contract": {
            "panel_role": offline.PANEL_ROLE,
            "queue_identity": offline.QUEUE_IDENTITY,
            "selected_day_count": offline.REQUIRED_DAYS,
            "selection_sha256": source.get("selection_sha256"),
            "day_receipts_revalidated": True,
            "economic_outcomes_read": False,
        },
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest["canonical_execution_manifest_sha256"] = _document_sha256(
        manifest, "canonical_execution_manifest_sha256"
    )
    base._atomic_json(destination, manifest)
    try:
        _validate_manifest_core(
            destination,
            verify_source_bytes=True,
            require_clean_tag=True,
        )
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    return manifest


def admit_v26_v27_invariance_receipt(
    predecessor_manifest_path: Path,
    successor_manifest_path: Path,
    output_path: Path,
    *,
    repository_root: Path | None = None,
) -> Mapping[str, Any]:
    root = (repository_root or Path(__file__).resolve().parents[4]).expanduser().resolve()
    predecessor_path = predecessor_manifest_path.expanduser().resolve()
    successor_path = successor_manifest_path.expanduser().resolve()
    destination = output_path.expanduser().resolve()
    if destination != successor_path.parent / INVARIANCE_RECEIPT_NAME:
        raise SellOnlyOrchestratorError("v26-to-v27 invariance path is not canonical")
    if destination.exists():
        raise SellOnlyOrchestratorError("immutable v26-to-v27 invariance receipt exists")
    if _file_sha256(predecessor_path) != V26_EXECUTION_MANIFEST_FILE_SHA256:
        raise SellOnlyOrchestratorError("formal-v26 manifest byte identity drifted")
    predecessor = _load_json(predecessor_path, label="formal-v26 manifest")
    if (
        predecessor.get("canonical_execution_manifest_sha256")
        != V26_EXECUTION_MANIFEST_CANONICAL_SHA256
        or predecessor.get("public_base_commit") != V26_PUBLIC_BASE_COMMIT
        or predecessor.get("annotated_tag") != V26_ANNOTATED_TAG
    ):
        raise SellOnlyOrchestratorError("formal-v26 manifest identity drifted")
    successor_bundle = _validate_manifest_core(
        successor_path,
        verify_source_bytes=True,
        require_clean_tag=True,
    )
    predecessor_source_path = base._resolve_bound_file(
        predecessor.get("source_manifest") or {},
        label="formal-v26 source manifest",
        repository_root=root,
    )
    predecessor_panel_path = base._resolve_bound_file(
        predecessor.get("panel_manifest") or {},
        label="formal-v26 panel manifest",
        repository_root=root,
    )
    predecessor_source = offline.validate_canonical_manifest(
        predecessor_source_path,
        rehash_sources=True,
    )
    predecessor_panel = mechanics.validate_panel(
        predecessor_panel_path,
        layout=offline.default_layout(),
        repository_root=root,
    )
    predecessor_snapshot = _normalized_research_snapshot(
        manifest=predecessor,
        source=predecessor_source,
        panel=predecessor_panel,
    )
    successor_snapshot = _normalized_research_snapshot(
        manifest=successor_bundle.execution_manifest,
        source=successor_bundle.source_manifest,
        panel=successor_bundle.panel_manifest,
    )
    if predecessor_snapshot != successor_snapshot:
        raise SellOnlyOrchestratorError("v27 changed the frozen research contract")
    source_hashes = _semantic_source_hashes(root)
    receipt: dict[str, Any] = {
        "schema_version": f"{INVARIANCE_IDENTITY}.receipt.v1",
        "identity": INVARIANCE_IDENTITY,
        "status": INVARIANCE_STATUS,
        "formal_v26": {
            "canonical_execution_manifest_sha256": V26_EXECUTION_MANIFEST_CANONICAL_SHA256,
            "manifest_file_sha256": V26_EXECUTION_MANIFEST_FILE_SHA256,
            "public_base_commit": V26_PUBLIC_BASE_COMMIT,
            "annotated_tag": V26_ANNOTATED_TAG,
        },
        "formal_v27": {
            "canonical_execution_manifest_sha256": successor_bundle.execution_manifest[
                "canonical_execution_manifest_sha256"
            ],
            "manifest_file_sha256": _file_sha256(successor_path),
            "public_base_commit": successor_bundle.execution_manifest["public_base_commit"],
            "annotated_tag": successor_bundle.execution_manifest["annotated_tag"],
        },
        "research_contract_sha256": _canonical_sha256(successor_snapshot),
        "research_semantic_source_hashes": source_hashes,
        "allowed_execution_change": "drop_cross_execution_buy_resume_and_run_sell_only",
        "formal_sides": list(FORMAL_SIDES),
        "candidate_ladder_changed": False,
        "data_changed": False,
        "folds_changed": False,
        "estimand_changed": False,
        "statistics_changed": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    base._atomic_json(destination, receipt)
    return receipt


def _validate_invariance_receipt(bundle: base.FormalOfflineBundle) -> Mapping[str, Any]:
    path = bundle.execution_manifest_path.parent / INVARIANCE_RECEIPT_NAME
    receipt = _load_json(path, label="formal-v26-to-v27 invariance receipt")
    current_sources = _semantic_source_hashes(Path(bundle.repository_root))
    formal_v27 = receipt.get("formal_v27")
    if (
        receipt.get("identity") != INVARIANCE_IDENTITY
        or receipt.get("status") != INVARIANCE_STATUS
        or receipt.get("formal_sides") != list(FORMAL_SIDES)
        or receipt.get("research_semantic_source_hashes") != current_sources
        or not isinstance(formal_v27, Mapping)
        or formal_v27.get("canonical_execution_manifest_sha256")
        != bundle.execution_manifest.get("canonical_execution_manifest_sha256")
        or formal_v27.get("manifest_file_sha256")
        != _file_sha256(bundle.execution_manifest_path)
        or formal_v27.get("public_base_commit")
        != bundle.execution_manifest.get("public_base_commit")
        or formal_v27.get("annotated_tag") != bundle.execution_manifest.get("annotated_tag")
        or receipt.get("economic_outcomes_read") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("action_authorized") is not False
        or receipt.get("live_authorized") is not False
        or receipt.get("canonical_receipt_sha256")
        != _document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise SellOnlyOrchestratorError("formal-v26-to-v27 invariance receipt drifted")
    return receipt


def _validate_predecessor_cpp_qualification(payload: Mapping[str, Any]) -> None:
    qualification = payload.get("qualification_contract")
    if (
        payload.get("identity") != V26_CPP_QUALIFICATION_IDENTITY
        or payload.get("status") != "passed_real_day_all_opportunity_all_arm_lockstep"
        or payload.get("canonical_receipt_sha256")
        != V26_CPP_QUALIFICATION_CANONICAL_SHA256
        or payload.get("canonical_receipt_sha256")
        != _document_sha256(payload, "canonical_receipt_sha256")
        or payload.get("opportunity_count") != 81
        or payload.get("arm_count") != 648
        or payload.get("zero_mismatch_arm_count") != 648
        or payload.get("python_sequential_engine_remains_authoritative") is not True
        or payload.get("economic_values_persisted") is not False
        or payload.get("economic_values_used_for_selection") is not False
        or payload.get("validation_read") is not False
        or payload.get("sealed_holdout_read") is not False
        or payload.get("action_authorized") is not False
        or payload.get("live_authorized") is not False
        or not isinstance(qualification, Mapping)
        or qualification.get("source_manifest_sha256")
        != "1e156e6d505b78f66a7a2d0d30eccfdb5eb97f46c0ba2f9c844baa381f32dfd7"
        or qualification.get("panel_manifest_sha256")
        != "c90930f9bdb51996af33efaeb9ef4d5386716f3ba911fcf2e1e935a65a0c8cab"
        or qualification.get("source_hashes") != base._current_cpp_qualification_source_hashes()
    ):
        raise SellOnlyOrchestratorError("formal-v26 C++ qualification is not reusable")


def admit_cpp_qualification_reuse_receipt(
    predecessor_receipt_path: Path,
    successor_manifest_path: Path,
    output_path: Path,
) -> Mapping[str, Any]:
    source_path = predecessor_receipt_path.expanduser().resolve()
    manifest_path = successor_manifest_path.expanduser().resolve()
    destination = output_path.expanduser().resolve()
    if destination != manifest_path.parent / CPP_QUALIFICATION_REUSE_RECEIPT_NAME:
        raise SellOnlyOrchestratorError("C++ qualification reuse path is not canonical")
    copied_path = manifest_path.parent / PREDECESSOR_CPP_QUALIFICATION_COPY_NAME
    if destination.exists() or copied_path.exists():
        raise SellOnlyOrchestratorError("immutable C++ qualification reuse evidence exists")
    source_bytes = source_path.read_bytes()
    if hashlib.sha256(source_bytes).hexdigest() != V26_CPP_QUALIFICATION_FILE_SHA256:
        raise SellOnlyOrchestratorError("formal-v26 C++ qualification bytes drifted")
    predecessor = json.loads(source_bytes)
    if not isinstance(predecessor, Mapping):
        raise SellOnlyOrchestratorError("formal-v26 C++ qualification is malformed")
    _validate_predecessor_cpp_qualification(predecessor)
    bundle = _validate_manifest_core(
        manifest_path,
        verify_source_bytes=True,
        require_clean_tag=True,
    )
    qualification = predecessor["qualification_contract"]
    if (
        qualification.get("source_manifest_sha256")
        != bundle.source_manifest.get("canonical_manifest_sha256")
        or qualification.get("panel_manifest_sha256")
        != bundle.panel_manifest.get("canonical_panel_manifest_sha256")
    ):
        raise SellOnlyOrchestratorError("C++ qualification denominator drifted")
    _atomic_bytes(copied_path, source_bytes)
    receipt: dict[str, Any] = {
        "schema_version": f"{CPP_QUALIFICATION_REUSE_IDENTITY}.receipt.v1",
        "identity": CPP_QUALIFICATION_REUSE_IDENTITY,
        "status": CPP_QUALIFICATION_REUSE_STATUS,
        "execution_manifest_sha256": bundle.execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "public_base_commit": bundle.execution_manifest["public_base_commit"],
        "annotated_tag": bundle.execution_manifest["annotated_tag"],
        "predecessor_receipt_file": PREDECESSOR_CPP_QUALIFICATION_COPY_NAME,
        "predecessor_receipt_file_sha256": V26_CPP_QUALIFICATION_FILE_SHA256,
        "predecessor_receipt_canonical_sha256": V26_CPP_QUALIFICATION_CANONICAL_SHA256,
        "predecessor_coverage_sides": ["BUY", "SELL"],
        "formal_coverage_sides": list(FORMAL_SIDES),
        "coverage_relation": "predecessor_both_side_superset_of_sell_only",
        "source_hashes_revalidated": True,
        "zero_mismatch_arm_count": 648,
        "python_sequential_engine_remains_authoritative": True,
        "economic_outcomes_read": False,
        "economic_values_persisted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    try:
        base._atomic_json(destination, receipt)
    except Exception:
        copied_path.unlink(missing_ok=True)
        raise
    return receipt


def _validate_cpp_qualification_reuse_receipt(
    bundle: base.FormalOfflineBundle,
) -> Mapping[str, Any]:
    root = bundle.execution_manifest_path.parent
    copied_path = root / PREDECESSOR_CPP_QUALIFICATION_COPY_NAME
    if _file_sha256(copied_path) != V26_CPP_QUALIFICATION_FILE_SHA256:
        raise SellOnlyOrchestratorError("copied formal-v26 C++ qualification drifted")
    predecessor = _load_json(copied_path, label="copied formal-v26 C++ qualification")
    _validate_predecessor_cpp_qualification(predecessor)
    receipt = _load_json(
        root / CPP_QUALIFICATION_REUSE_RECEIPT_NAME,
        label="formal-v27 C++ qualification reuse receipt",
    )
    if (
        receipt.get("identity") != CPP_QUALIFICATION_REUSE_IDENTITY
        or receipt.get("status") != CPP_QUALIFICATION_REUSE_STATUS
        or receipt.get("execution_manifest_sha256")
        != bundle.execution_manifest.get("canonical_execution_manifest_sha256")
        or receipt.get("public_base_commit") != bundle.execution_manifest.get("public_base_commit")
        or receipt.get("annotated_tag") != bundle.execution_manifest.get("annotated_tag")
        or receipt.get("predecessor_receipt_file_sha256")
        != V26_CPP_QUALIFICATION_FILE_SHA256
        or receipt.get("predecessor_receipt_canonical_sha256")
        != V26_CPP_QUALIFICATION_CANONICAL_SHA256
        or receipt.get("formal_coverage_sides") != list(FORMAL_SIDES)
        or receipt.get("source_hashes_revalidated") is not True
        or receipt.get("zero_mismatch_arm_count") != 648
        or receipt.get("economic_outcomes_read") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("action_authorized") is not False
        or receipt.get("live_authorized") is not False
        or receipt.get("canonical_receipt_sha256")
        != _document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise SellOnlyOrchestratorError("formal-v27 C++ qualification reuse receipt drifted")
    return receipt


def load_formal_sell_only_bundle_for_admission(
    execution_manifest_path: Path,
) -> base.FormalOfflineBundle:
    return _validate_manifest_core(
        execution_manifest_path,
        verify_source_bytes=True,
        require_clean_tag=True,
    )


def load_formal_sell_only_bundle(
    execution_manifest_path: Path,
) -> base.FormalOfflineBundle:
    bundle = load_formal_sell_only_bundle_for_admission(execution_manifest_path)
    _validate_invariance_receipt(bundle)
    _validate_cpp_qualification_reuse_receipt(bundle)
    return bundle


def run_formal_sell_only_economics(
    execution_manifest_path: Path,
    *,
    output_dir: Path,
) -> Mapping[str, Any]:
    bundle = load_formal_sell_only_bundle(execution_manifest_path)
    backend = importlib.import_module(CANONICAL_BACKEND_MODULE)
    runner = getattr(backend, CANONICAL_BACKEND_FUNCTION, None)
    if not callable(runner):
        raise SellOnlyOrchestratorError("canonical SELL-only backend is unavailable")
    result = runner(execution_manifest_path)
    if not isinstance(result, Mapping):
        raise SellOnlyOrchestratorError("SELL-only backend returned a custom result")
    if (
        result.get("schema_version") != FORMAL_RESULT_SCHEMA
        or result.get("execution_manifest_sha256")
        != bundle.execution_manifest.get("canonical_execution_manifest_sha256")
        or result.get("formal_sides") != list(FORMAL_SIDES)
        or result.get("component_scope") != "sell_only_learning_algorithm_oof"
        or result.get("repeated_sequential_policy") is not True
        or result.get("one_shot_effect_aggregation_used") is not False
        or result.get("exact_owner_policy_sha256") != offline.ACTIVE_OWNER_POLICY_SHA256
        or result.get("validation_read") is not False
        or result.get("sealed_holdout_read") is not False
    ):
        raise SellOnlyOrchestratorError("SELL-only formal result contract drifted")
    output = output_dir.expanduser().resolve()
    payload = dict(result)
    payload["canonical_result_sha256"] = _document_sha256(
        payload, "canonical_result_sha256"
    )
    base._atomic_json(output / "formal_result.json", payload)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bind = subparsers.add_parser("bind")
    bind.add_argument("panel_manifest", type=Path)
    bind.add_argument("output_manifest", type=Path)
    bind.add_argument("--annotated-tag", required=True)
    invariance = subparsers.add_parser("admit-v26-v27-invariance")
    invariance.add_argument("predecessor_manifest", type=Path)
    invariance.add_argument("successor_manifest", type=Path)
    invariance.add_argument("--output", type=Path, required=True)
    qualification = subparsers.add_parser("admit-cpp-qualification-reuse")
    qualification.add_argument("predecessor_receipt", type=Path)
    qualification.add_argument("successor_manifest", type=Path)
    qualification.add_argument("--output", type=Path, required=True)
    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("manifest", type=Path)
    preflight.add_argument("--output", type=Path)
    run = subparsers.add_parser("run")
    run.add_argument("manifest", type=Path)
    run.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "bind":
        payload = bind_formal_sell_only_execution_manifest(
            args.panel_manifest,
            args.output_manifest,
            annotated_tag=args.annotated_tag,
        )
    elif args.command == "admit-v26-v27-invariance":
        payload = admit_v26_v27_invariance_receipt(
            args.predecessor_manifest,
            args.successor_manifest,
            args.output,
        )
    elif args.command == "admit-cpp-qualification-reuse":
        payload = admit_cpp_qualification_reuse_receipt(
            args.predecessor_receipt,
            args.successor_manifest,
            args.output,
        )
    elif args.command == "preflight":
        backend = importlib.import_module(CANONICAL_BACKEND_MODULE)
        runner = getattr(backend, "preflight_canonical_offline_sell_economics", None)
        if not callable(runner):
            raise SellOnlyOrchestratorError("SELL-only preflight is unavailable")
        payload = dict(runner(args.manifest))
        if args.output is not None:
            destination = args.output.expanduser().resolve()
            if destination.exists():
                raise SellOnlyOrchestratorError("immutable SELL preflight receipt exists")
            payload["canonical_preflight_sha256"] = _document_sha256(
                payload, "canonical_preflight_sha256"
            )
            base._atomic_json(destination, payload)
    else:
        payload = run_formal_sell_only_economics(
            args.manifest,
            output_dir=args.output_dir,
        )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
