#!/usr/bin/env python3
"""Canonical, non-injectable formal entry for the offline F05 successor.

The command accepts one hash-bound execution manifest.  Its prebound panel is
strictly outcome-blind mechanics: economic labels must be generated inside each
outer-train fold by the fixed backend.  It never accepts a DataFrame, handwritten
fold, evaluator callback, day list, or one-shot result.  Source admission may be
audited in a dirty worktree, but formal economics is allowed only from the clean
annotated tag frozen in the execution manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data_paths import resolve_portable_path
from models import cache_tier_lru
from models.audit import dataset_governance, execution_governance
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_mechanics_v1 as mechanics,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

IDENTITY = f"{offline.IDENTITY}.formal_orchestrator_v1"
SCHEMA_VERSION = f"{IDENTITY}.execution_manifest.v1"
PANEL_SCHEMA_VERSION = mechanics.PANEL_SCHEMA_VERSION
CANONICAL_BACKEND_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_repeated_policy_backend_v1"
)
CANONICAL_BACKEND_FUNCTION = "run_canonical_offline_economics"
FORMAL_RESULT_SCHEMA = f"{IDENTITY}.formal_result.v1"
DATASET_BINDING_NAME = "formal_dataset_binding.json"
EXECUTOR_ACCELERATION_IDENTITY = "f05_full_multiscale_offline_replay_executor_cpp_one_shot_v6"
EXECUTOR_DAY_INPUT_CACHE_IDENTITY = "f05_full_multiscale_offline_replay_executor_acceleration_v2"
EXECUTOR_DAY_INPUT_CACHE_ROOT = (
    "${NARROWGATE_DATA_ROOT}/cache/replay_dag/f05_full_multiscale_offline_day_input_mmap_v2"
)
EXECUTOR_SEQUENTIAL_CACHE_ROOT = (
    "${NARROWGATE_DATA_ROOT}/cache/replay_dag/"
    "f05_full_multiscale_successor_offline_sequential_replay_cache_v2"
)
EXECUTOR_ACTIVE_CACHE_TTL_S = 14 * 24 * 60 * 60
EXECUTOR_GLOBAL_WORKER_TOKENS = 10
EXECUTOR_WORKER_GOVERNOR_ROOT = "${NARROWGATE_CACHE_ROOT}/execution_governor_v1"
EXECUTOR_DAY_INPUT_MATERIALIZATION_WORKERS = 2
EXECUTOR_ONE_SHOT_TOPOLOGY = {
    "total_worker_tokens": 10,
    "day_parent_workers": 0,
    "supervisor_workers": 0,
    "arm_workers": 10,
    "nested_process_pool": False,
    "shared_prefix_posix_fork": False,
    "global_cpp_arm_thread_pool": True,
    "shared_read_only_observation_tape": True,
}
CPP_QUALIFICATION_RECEIPT_NAME = "cpp_real_day_lockstep_receipt.json"
CPP_BUILDER_PREFLIGHT_IDENTITY = (
    "f05_cpp_target_predicate_builder_all_opportunity_zero_economic_v26"
)
CPP_BUILDER_PREFLIGHT_RECEIPT_NAME = "cpp_target_predicate_builder_walk_receipt.json"
CPP_BUILDER_PREFLIGHT_STATUS = "passed_all_3516_zero_economic_builder_walk"
CPP_BUILDER_PREFLIGHT_OPPORTUNITIES = 3_516
CPP_QUALIFICATION_IDENTITY = "f05_cpp_one_shot_real_day_all_arm_lockstep_v26"
CPP_QUICK_PREFLIGHT_IDENTITY = "f05_cpp_first_opportunity_all_arm_lockstep_v26"
CPP_QUICK_PREFLIGHT_RECEIPT_NAME = "cpp_first_opportunity_all_arm_preflight_receipt.json"
CPP_QUICK_PREFLIGHT_STATUS = "passed_first_opportunity_all_side_specific_arms_lockstep"
V23_V24_INVARIANCE_IDENTITY = "f05_formal_v23_to_v24_execution_only_invariance_v1"
V23_V24_INVARIANCE_RECEIPT_NAME = "formal_v23_to_v24_invariance_receipt.json"
V23_V24_INVARIANCE_STATUS = "passed_execution_only_research_contract_invariance"
V23_EXECUTION_MANIFEST_CANONICAL_SHA256 = (
    "0f0a999f6862f9574975f51ae6fee332515aa06547c2b3766a31b4d5e9afa4d3"
)
V23_EXECUTION_MANIFEST_FILE_SHA256 = (
    "2f28b011874c6bc892da07a98fb9b18f4d33ad001fc9ef27895b6d60174ae44b"
)
V23_PUBLIC_BASE_COMMIT = "8a1d6e739f47e0b47d4470812e00314a66673fe7"
V23_ANNOTATED_TAG = (
    "research/f05/causal-multichannel-window-boolean-cooldown-full-multiscale-"
    "successor-offline/formal-cpp-one-shot-v23-20260816"
)
V24_V26_INVARIANCE_IDENTITY = "f05_formal_v24_to_v26_execution_only_invariance_v1"
V24_V26_INVARIANCE_RECEIPT_NAME = "formal_v24_to_v26_invariance_receipt.json"
V24_V26_INVARIANCE_STATUS = "passed_execution_only_research_contract_invariance"
V24_EXECUTION_MANIFEST_CANONICAL_SHA256 = (
    "2021a70f2f15f4fff82240cdc494556413da0fc24d369be00fd60628bcf3395a"
)
V24_EXECUTION_MANIFEST_FILE_SHA256 = (
    "0c3beda45316a2211d2bc56c563b7dedef2b3133f03290a529060e034c0d18c4"
)
V24_PUBLIC_BASE_COMMIT = "8b863a49987b97c88ae8d70ec6605f081c988575"
V24_ANNOTATED_TAG = (
    "research/f05/causal-multichannel-window-boolean-cooldown-full-multiscale-"
    "successor-offline/formal-cpp-one-shot-v24-20260816"
)
V24_REPLAY_ADAPTER_ARTIFACT_SHA256 = (
    "3fe25034aef8a0149f1e67c80ae5b7d236b04790c2e2e583cdb62ebb6cf353e1"
)
COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME = (
    "formal_v24_completed_buy_cache_census_receipt.json"
)
PREDECESSOR_DAY_CACHE_ROOT = (
    "${NARROWGATE_DATA_ROOT}/cache/replay_dag/"
    "f05_full_multiscale_successor_offline_sequential_replay_cache_v2"
)
_RESEARCH_SEMANTIC_SOURCE_PATHS = (
    "models/audit/experiment_scorecard.py",
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_repeated_policy_backend_v1.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_nested_oof.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/docs/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_v1_spec_20260813.json"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/docs/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_v1_execution_contract_20260813.json"
    ),
)
_V24_V26_EXECUTION_ONLY_SOURCE_PATHS = (
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_repeated_policy_backend_v1.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_replay_adapter_v1.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_orchestrator_v1.py"
    ),
    (
        "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_cpp_real_day_lockstep_v22.py"
    ),
)
_V24_V26_RESEARCH_SEMANTIC_SOURCE_PATHS = tuple(
    path
    for path in _RESEARCH_SEMANTIC_SOURCE_PATHS
    if path not in _V24_V26_EXECUTION_ONLY_SOURCE_PATHS
)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,199}$")
_PANEL_FILES = (
    "metadata",
    "boolean_features",
    "continuous_features",
    "exact_owner_actions",
    "replay_inputs",
)
_FORBIDDEN_ECONOMIC_PANEL_FILES = frozenset(
    {
        "action_outcomes",
        "action_supported",
        "economic_outcomes",
        "one_shot_training_labels",
    }
)


class OfflineOrchestratorError(RuntimeError):
    """Raised when formal economics can be bypassed or its identity drifts."""


def completed_side_resume_contract() -> dict[str, Any]:
    """Bind completed BUY units while excluding every unfinished SELL unit."""

    return {
        "schema_version": (
            f"{offline.IDENTITY}.offline_replay_adapter_v1.completed_side_resume.v1"
        ),
        "mode": "hash_verified_completed_side_only",
        "predecessor_execution_manifest_sha256": (
            V24_EXECUTION_MANIFEST_CANONICAL_SHA256
        ),
        "predecessor_adapter_artifact_sha256": V24_REPLAY_ADAPTER_ARTIFACT_SHA256,
        "predecessor_cache_root": PREDECESSOR_DAY_CACHE_ROOT,
        "census_receipt_file": COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME,
        "inherited_sides": ["BUY"],
        "excluded_sides": ["SELL"],
        "required_stage_counts": {
            "outer_train_one_shot": 67,
            "inner_oof": 250,
            "outer_oof": 260,
        },
        "required_complete_cache_units": 577,
        "predecessor_failed_entries_reusable": False,
        "recompute_inherited_side_allowed": False,
    }


def formal_executor_contract() -> dict[str, Any]:
    """Return the only executor contract accepted by a newly bound formal run."""

    return {
        "identity": EXECUTOR_ACCELERATION_IDENTITY,
        "authoritative_engine_by_stage": {
            "outer_train_one_shot": "cpp",
            "outer_test_sequential": "python",
        },
        "global_worker_tokens": EXECUTOR_GLOBAL_WORKER_TOKENS,
        "nested_worker_pools_allowed": False,
        "host_worker_governor": {
            "identity": execution_governance.WORKER_GOVERNOR_IDENTITY,
            "root": EXECUTOR_WORKER_GOVERNOR_ROOT,
            "capacity": EXECUTOR_GLOBAL_WORKER_TOKENS,
            "requested_tokens": EXECUTOR_GLOBAL_WORKER_TOKENS,
            "nested_governed_pool_allowed": False,
        },
        "one_shot_process_topology": dict(EXECUTOR_ONE_SHOT_TOPOLOGY),
        "day_input_materialization_workers": (EXECUTOR_DAY_INPUT_MATERIALIZATION_WORKERS),
        "day_input_mmap": {
            "enabled": True,
            "identity": EXECUTOR_DAY_INPUT_CACHE_IDENTITY,
            "root": EXECUTOR_DAY_INPUT_CACHE_ROOT,
            "open_mode": "read_only",
            "content_addressed": True,
        },
        "b0_control_cache": {
            "enabled": True,
            "candidate_output_allowed": False,
            "side_excludable": False,
        },
        "completed_side_resume": completed_side_resume_contract(),
        "cpp_formal_engine_authorized": True,
        "cpp_authority_scope": "outer_train_one_shot_labels_only",
        "cpp_real_day_all_arm_lockstep_required": True,
        "cpp_first_opportunity_all_arm_preflight_required": True,
        "builder_instantiates_formal_cpp_runtime_required": True,
        "all_panel_zero_economic_builder_walk_required": True,
        "all_fold_zero_economic_contract_walk_required": True,
    }


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


def _bytes_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git_file_bytes(*, repository_root: Path, commit: str, path: str) -> bytes:
    completed = subprocess.run(
        ["git", "show", f"{commit}:{path}"],
        cwd=repository_root,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise OfflineOrchestratorError(
            f"research-contract source is absent from predecessor commit: {path}"
        )
    return bytes(completed.stdout)


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineOrchestratorError(f"cannot load {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise OfflineOrchestratorError(f"{label} root must be an object")
    return payload


def _research_contract_snapshot(
    *,
    manifest: Mapping[str, Any],
    source: Mapping[str, Any],
    panel: Mapping[str, Any],
) -> dict[str, Any]:
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
    )
    from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
        duration_vocabulary,
    )

    files = panel.get("files")
    if not isinstance(files, Mapping):
        raise OfflineOrchestratorError("invariance panel file census is missing")
    row_key_sha256_values = {
        str(binding.get("row_key_sha256", ""))
        for binding in files.values()
        if isinstance(binding, Mapping)
    }
    if len(row_key_sha256_values) != 1:
        raise OfflineOrchestratorError("invariance panel row identity drifted")
    metadata = files.get("metadata")
    if not isinstance(metadata, Mapping):
        raise OfflineOrchestratorError("invariance metadata binding is missing")
    opportunity_count = int(metadata.get("rows", 0))
    if opportunity_count != CPP_BUILDER_PREFLIGHT_OPPORTUNITIES:
        raise OfflineOrchestratorError("invariance mechanics opportunity count drifted")
    selected_days = tuple(str(value) for value in source.get("selected_days", ()))
    if len(selected_days) != offline.REQUIRED_DAYS:
        raise OfflineOrchestratorError("invariance selected-day denominator drifted")
    confirmatory = [list(value) for value in nested.CONFIRMATORY_COMPARISONS]
    snapshot = {
        "source_manifest_binding": dict(manifest.get("source_manifest") or {}),
        "panel_manifest_binding": dict(manifest.get("panel_manifest") or {}),
        "selected_day_order": list(selected_days),
        "selected_day_order_sha256": _canonical_sha256(selected_days),
        "selected_day_count": len(selected_days),
        "mechanics_opportunity_count": opportunity_count,
        "opportunity_identity_set_and_order_sha256": next(iter(row_key_sha256_values)),
        "fold_manifest_sha256": manifest.get("fold_manifest_sha256"),
        "nested_fold_manifest_sha256": manifest.get("nested_fold_manifest_sha256"),
        "four_by_three_chronological_fold_contract": manifest.get(
            "nested_fold_manifest"
        ),
        "exact_b0_policy_sha256": panel.get("exact_current_owner_policy_sha256"),
        "exact_b0_predicate_bundle_sha256": panel.get(
            "exact_current_predicate_bundle_sha256"
        ),
        "candidate_ladder_identity": successor.IDENTITY,
        "candidate_ladder": list(successor.SUCCESSOR_CANDIDATE_LADDER),
        "candidate_ladder_sha256": _canonical_sha256(
            successor.SUCCESSOR_CANDIDATE_LADDER
        ),
        "side_specific_duration_vocabulary": {
            side: list(duration_vocabulary(side)) for side in ("BUY", "SELL")
        },
        "complete_hierarchy_suffixes": list(successor.COMPLETE_HIERARCHY_SUFFIXES),
        "confirmatory_comparisons": confirmatory,
        "sequential_repeated_policy_estimand": {
            "control": "B0_CURRENT_EXACT",
            "outer_train": "one_shot_labels_only",
            "outer_test": "paired_repeated_sequential_policy",
            "one_shot_effect_aggregation_used": False,
        },
        "score_profile_contract": dict(successor.SCORE_PROFILE_CONTRACT),
        "execution_contract": dict(manifest.get("execution_contract") or {}),
        "validation_and_sealed_holdout_boundaries": {
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "action_and_live_permissions": {
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    if snapshot["exact_b0_policy_sha256"] != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineOrchestratorError("invariance exact B0 policy drifted")
    if (
        snapshot["exact_b0_predicate_bundle_sha256"]
        != offline.ACTIVE_PREDICATE_BUNDLE_SHA256
    ):
        raise OfflineOrchestratorError("invariance exact B0 predicate bundle drifted")
    if snapshot["score_profile_contract"].get("profile_id") != "action_alpha_v1":
        raise OfflineOrchestratorError("invariance score profile drifted")
    return snapshot


def _semantic_source_hashes(
    *,
    repository_root: Path,
    predecessor_commit: str,
) -> dict[str, dict[str, Any]]:
    hashes: dict[str, dict[str, Any]] = {}
    for relative_path in _RESEARCH_SEMANTIC_SOURCE_PATHS:
        current_path = repository_root / relative_path
        if not current_path.is_file():
            raise OfflineOrchestratorError(
                f"research-contract source is missing: {relative_path}"
            )
        predecessor_bytes = _git_file_bytes(
            repository_root=repository_root,
            commit=predecessor_commit,
            path=relative_path,
        )
        predecessor_sha256 = _bytes_sha256(predecessor_bytes)
        current_sha256 = _file_sha256(current_path)
        if predecessor_sha256 != current_sha256:
            raise OfflineOrchestratorError(
                f"research-contract source changed since formal-v23: {relative_path}"
            )
        hashes[relative_path] = {
            "formal_v23_sha256": predecessor_sha256,
            "formal_v24_sha256": current_sha256,
            "equal": True,
        }
    return hashes


def _execution_only_manifest_invariance(
    predecessor: Mapping[str, Any],
    successor: Mapping[str, Any],
) -> None:
    predecessor_normalized = dict(predecessor)
    successor_normalized = dict(successor)
    for field in (
        "public_base_commit",
        "annotated_tag",
        "executor",
        "cpp_one_shot_qualification",
        "canonical_execution_manifest_sha256",
    ):
        predecessor_normalized.pop(field, None)
        successor_normalized.pop(field, None)
    if predecessor_normalized != successor_normalized:
        raise OfflineOrchestratorError(
            "formal-v24 changed a manifest field outside the execution-only allowance"
        )

    predecessor_executor = dict(predecessor.get("executor") or {})
    successor_executor = dict(successor.get("executor") or {})
    if predecessor_executor.pop("identity", None) != (
        "f05_full_multiscale_offline_replay_executor_cpp_one_shot_v3"
    ):
        raise OfflineOrchestratorError("formal-v23 executor identity drifted")
    if successor_executor.pop("identity", None) != EXECUTOR_ACCELERATION_IDENTITY:
        raise OfflineOrchestratorError("formal-v24 executor identity drifted")
    if (
        successor_executor.pop(
            "builder_instantiates_formal_cpp_runtime_required",
            None,
        )
        is not True
    ):
        raise OfflineOrchestratorError(
            "formal-v24 builder runtime-instantiation gate is missing"
        )
    if predecessor_executor != successor_executor:
        raise OfflineOrchestratorError(
            "formal-v24 changed the executor beyond its preflight gate"
        )

    predecessor_qualification = dict(predecessor.get("cpp_one_shot_qualification") or {})
    successor_qualification = dict(successor.get("cpp_one_shot_qualification") or {})
    if predecessor_qualification.pop("identity", None) != (
        "f05_cpp_one_shot_real_day_all_arm_lockstep_v23"
    ):
        raise OfflineOrchestratorError("formal-v23 qualification identity drifted")
    if successor_qualification.pop("identity", None) != CPP_QUALIFICATION_IDENTITY:
        raise OfflineOrchestratorError("formal-v24 qualification identity drifted")
    for field in (
        "invariance_receipt_file",
        "builder_formal_runtime_instantiation_required",
    ):
        predecessor_qualification.pop(field, None)
        successor_qualification.pop(field, None)
    if predecessor_qualification != successor_qualification:
        raise OfflineOrchestratorError(
            "formal-v24 changed C++ qualification beyond the frozen runtime gate"
        )


def admit_v23_v24_invariance_receipt(
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
    if destination != successor_path.parent / V23_V24_INVARIANCE_RECEIPT_NAME:
        raise OfflineOrchestratorError("invariance receipt path is not canonical")
    if destination.exists():
        raise OfflineOrchestratorError("immutable v23-to-v24 invariance receipt already exists")
    if _file_sha256(predecessor_path) != V23_EXECUTION_MANIFEST_FILE_SHA256:
        raise OfflineOrchestratorError("formal-v23 execution manifest byte identity drifted")
    predecessor = _load_json(predecessor_path, label="formal-v23 execution manifest")
    if (
        predecessor.get("canonical_execution_manifest_sha256")
        != V23_EXECUTION_MANIFEST_CANONICAL_SHA256
        or predecessor.get("canonical_execution_manifest_sha256")
        != _document_sha256(predecessor, "canonical_execution_manifest_sha256")
        or predecessor.get("public_base_commit") != V23_PUBLIC_BASE_COMMIT
        or predecessor.get("annotated_tag") != V23_ANNOTATED_TAG
    ):
        raise OfflineOrchestratorError("formal-v23 execution identity drifted")
    bundle = _load_formal_offline_bundle(
        successor_path,
        verify_source_bytes=True,
        require_clean_tag=True,
        require_cpp_qualification=False,
        require_invariance=False,
        require_completed_side_census=False,
    )
    successor = bundle.execution_manifest
    _execution_only_manifest_invariance(predecessor, successor)
    research_contract = _research_contract_snapshot(
        manifest=successor,
        source=bundle.source_manifest,
        panel=bundle.panel_manifest,
    )
    semantic_sources = _semantic_source_hashes(
        repository_root=root,
        predecessor_commit=V23_PUBLIC_BASE_COMMIT,
    )
    receipt: dict[str, Any] = {
        "schema_version": f"{V23_V24_INVARIANCE_IDENTITY}.receipt.v1",
        "identity": V23_V24_INVARIANCE_IDENTITY,
        "status": V23_V24_INVARIANCE_STATUS,
        "formal_v23": {
            "canonical_execution_manifest_sha256": (
                V23_EXECUTION_MANIFEST_CANONICAL_SHA256
            ),
            "manifest_file_sha256": V23_EXECUTION_MANIFEST_FILE_SHA256,
            "public_base_commit": V23_PUBLIC_BASE_COMMIT,
            "annotated_tag": V23_ANNOTATED_TAG,
        },
        "formal_v24": {
            "canonical_execution_manifest_sha256": successor[
                "canonical_execution_manifest_sha256"
            ],
            "manifest_file_sha256": _file_sha256(successor_path),
            "public_base_commit": successor["public_base_commit"],
            "annotated_tag": successor["annotated_tag"],
        },
        "research_contract": research_contract,
        "research_contract_sha256": _canonical_sha256(research_contract),
        "semantic_source_hashes": semantic_sources,
        "semantic_source_count": len(semantic_sources),
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
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt,
        "canonical_receipt_sha256",
    )
    _atomic_json(destination, receipt)
    return receipt


def _validate_v23_v24_invariance_receipt(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    panel: Mapping[str, Any],
    repository_root: Path,
    verify_runtime_artifacts: bool,
) -> Mapping[str, Any]:
    receipt = _load_json(
        manifest_path.parent / V23_V24_INVARIANCE_RECEIPT_NAME,
        label="formal-v23-to-v24 invariance receipt",
    )
    formal_v23 = receipt.get("formal_v23")
    formal_v24 = receipt.get("formal_v24")
    expected_contract = _research_contract_snapshot(
        manifest=manifest,
        source=source,
        panel=panel,
    )
    false_fields = (
        "data_changed",
        "candidate_ladder_changed",
        "duration_vocabulary_changed",
        "estimand_changed",
        "statistics_changed",
        "economic_outcomes_read",
        "economic_values_persisted",
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
    )
    if (
        receipt.get("identity") != V23_V24_INVARIANCE_IDENTITY
        or receipt.get("status") != V23_V24_INVARIANCE_STATUS
        or not isinstance(formal_v23, Mapping)
        or not isinstance(formal_v24, Mapping)
        or formal_v23.get("canonical_execution_manifest_sha256")
        != V23_EXECUTION_MANIFEST_CANONICAL_SHA256
        or formal_v23.get("manifest_file_sha256")
        != V23_EXECUTION_MANIFEST_FILE_SHA256
        or formal_v23.get("public_base_commit") != V23_PUBLIC_BASE_COMMIT
        or formal_v23.get("annotated_tag") != V23_ANNOTATED_TAG
        or formal_v24.get("canonical_execution_manifest_sha256")
        != manifest.get("canonical_execution_manifest_sha256")
        or formal_v24.get("manifest_file_sha256") != _file_sha256(manifest_path)
        or formal_v24.get("public_base_commit") != manifest.get("public_base_commit")
        or formal_v24.get("annotated_tag") != manifest.get("annotated_tag")
        or receipt.get("research_contract") != expected_contract
        or receipt.get("research_contract_sha256") != _canonical_sha256(expected_contract)
        or int(receipt.get("semantic_source_count", 0))
        != len(_RESEARCH_SEMANTIC_SOURCE_PATHS)
        or any(receipt.get(field) is not False for field in false_fields)
        or receipt.get("canonical_receipt_sha256")
        != _document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise OfflineOrchestratorError("formal-v23-to-v24 invariance receipt failed closed")
    if verify_runtime_artifacts:
        expected_sources = _semantic_source_hashes(
            repository_root=repository_root,
            predecessor_commit=V23_PUBLIC_BASE_COMMIT,
        )
        if receipt.get("semantic_source_hashes") != expected_sources:
            raise OfflineOrchestratorError(
                "formal-v23-to-v24 semantic source identity drifted"
            )
    return receipt


def _v24_v26_semantic_source_hashes(
    *,
    repository_root: Path,
) -> dict[str, dict[str, Any]]:
    hashes: dict[str, dict[str, Any]] = {}
    for relative_path in _V24_V26_RESEARCH_SEMANTIC_SOURCE_PATHS:
        current_path = repository_root / relative_path
        if not current_path.is_file():
            raise OfflineOrchestratorError(
                f"research-contract source is missing: {relative_path}"
            )
        predecessor_sha256 = _bytes_sha256(
            _git_file_bytes(
                repository_root=repository_root,
                commit=V24_PUBLIC_BASE_COMMIT,
                path=relative_path,
            )
        )
        current_sha256 = _file_sha256(current_path)
        if predecessor_sha256 != current_sha256:
            raise OfflineOrchestratorError(
                f"research-contract source changed since formal-v24: {relative_path}"
            )
        hashes[relative_path] = {
            "formal_v24_sha256": predecessor_sha256,
            "formal_v26_sha256": current_sha256,
            "equal": True,
        }
    return hashes


def _v24_v26_execution_source_hashes(
    *,
    repository_root: Path,
) -> dict[str, dict[str, Any]]:
    hashes: dict[str, dict[str, Any]] = {}
    for relative_path in _V24_V26_EXECUTION_ONLY_SOURCE_PATHS:
        current_path = repository_root / relative_path
        if not current_path.is_file():
            raise OfflineOrchestratorError(
                f"execution-only successor source is missing: {relative_path}"
            )
        predecessor_sha256 = _bytes_sha256(
            _git_file_bytes(
                repository_root=repository_root,
                commit=V24_PUBLIC_BASE_COMMIT,
                path=relative_path,
            )
        )
        current_sha256 = _file_sha256(current_path)
        if predecessor_sha256 == current_sha256:
            raise OfflineOrchestratorError(
                f"execution-only successor source did not change: {relative_path}"
            )
        hashes[relative_path] = {
            "formal_v24_sha256": predecessor_sha256,
            "formal_v26_sha256": current_sha256,
            "equal": False,
            "change_class": "mmap_lifetime_or_completed_side_semantic_resume_only",
        }
    return hashes


def _execution_only_manifest_invariance_v24_v26(
    predecessor: Mapping[str, Any],
    successor: Mapping[str, Any],
) -> None:
    predecessor_normalized = dict(predecessor)
    successor_normalized = dict(successor)
    for field in (
        "public_base_commit",
        "annotated_tag",
        "executor",
        "cpp_one_shot_qualification",
        "canonical_execution_manifest_sha256",
    ):
        predecessor_normalized.pop(field, None)
        successor_normalized.pop(field, None)
    if predecessor_normalized != successor_normalized:
        raise OfflineOrchestratorError(
            "formal-v26 changed a manifest field outside the execution-only allowance"
        )

    predecessor_executor = dict(predecessor.get("executor") or {})
    successor_executor = dict(successor.get("executor") or {})
    if predecessor_executor.pop("identity", None) != (
        "f05_full_multiscale_offline_replay_executor_cpp_one_shot_v4"
    ):
        raise OfflineOrchestratorError("formal-v24 executor identity drifted")
    if successor_executor.pop("identity", None) != EXECUTOR_ACCELERATION_IDENTITY:
        raise OfflineOrchestratorError("formal-v26 executor identity drifted")
    if successor_executor.pop("completed_side_resume", None) != (
        completed_side_resume_contract()
    ):
        raise OfflineOrchestratorError("formal-v26 completed-side resume contract drifted")
    if predecessor_executor != successor_executor:
        raise OfflineOrchestratorError(
            "formal-v26 changed the executor beyond mmap lifetime repair and resume"
        )

    predecessor_qualification = dict(predecessor.get("cpp_one_shot_qualification") or {})
    successor_qualification = dict(successor.get("cpp_one_shot_qualification") or {})
    if predecessor_qualification.pop("identity", None) != (
        "f05_cpp_one_shot_real_day_all_arm_lockstep_v24"
    ):
        raise OfflineOrchestratorError("formal-v24 qualification identity drifted")
    if successor_qualification.pop("identity", None) != CPP_QUALIFICATION_IDENTITY:
        raise OfflineOrchestratorError("formal-v26 qualification identity drifted")
    predecessor_qualification.pop("invariance_receipt_file", None)
    successor_qualification.pop("invariance_receipt_file", None)
    if predecessor_qualification != successor_qualification:
        raise OfflineOrchestratorError(
            "formal-v26 changed C++ qualification beyond receipt rebinding"
        )


def admit_v24_v26_invariance_receipt(
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
    if destination != successor_path.parent / V24_V26_INVARIANCE_RECEIPT_NAME:
        raise OfflineOrchestratorError("v24-to-v26 invariance receipt path is not canonical")
    if destination.exists():
        raise OfflineOrchestratorError("immutable v24-to-v26 invariance receipt already exists")
    if _file_sha256(predecessor_path) != V24_EXECUTION_MANIFEST_FILE_SHA256:
        raise OfflineOrchestratorError("formal-v24 execution manifest byte identity drifted")
    predecessor = _load_json(predecessor_path, label="formal-v24 execution manifest")
    if (
        predecessor.get("canonical_execution_manifest_sha256")
        != V24_EXECUTION_MANIFEST_CANONICAL_SHA256
        or predecessor.get("canonical_execution_manifest_sha256")
        != _document_sha256(predecessor, "canonical_execution_manifest_sha256")
        or predecessor.get("public_base_commit") != V24_PUBLIC_BASE_COMMIT
        or predecessor.get("annotated_tag") != V24_ANNOTATED_TAG
    ):
        raise OfflineOrchestratorError("formal-v24 execution identity drifted")
    bundle = _load_formal_offline_bundle(
        successor_path,
        verify_source_bytes=True,
        require_clean_tag=True,
        require_cpp_qualification=False,
        require_invariance=False,
        require_completed_side_census=False,
    )
    successor = bundle.execution_manifest
    _execution_only_manifest_invariance_v24_v26(predecessor, successor)
    research_contract = _research_contract_snapshot(
        manifest=successor,
        source=bundle.source_manifest,
        panel=bundle.panel_manifest,
    )
    semantic_sources = _v24_v26_semantic_source_hashes(repository_root=root)
    execution_sources = _v24_v26_execution_source_hashes(repository_root=root)
    receipt: dict[str, Any] = {
        "schema_version": f"{V24_V26_INVARIANCE_IDENTITY}.receipt.v1",
        "identity": V24_V26_INVARIANCE_IDENTITY,
        "status": V24_V26_INVARIANCE_STATUS,
        "formal_v24": {
            "canonical_execution_manifest_sha256": (
                V24_EXECUTION_MANIFEST_CANONICAL_SHA256
            ),
            "manifest_file_sha256": V24_EXECUTION_MANIFEST_FILE_SHA256,
            "public_base_commit": V24_PUBLIC_BASE_COMMIT,
            "annotated_tag": V24_ANNOTATED_TAG,
        },
        "formal_v26": {
            "canonical_execution_manifest_sha256": successor[
                "canonical_execution_manifest_sha256"
            ],
            "manifest_file_sha256": _file_sha256(successor_path),
            "public_base_commit": successor["public_base_commit"],
            "annotated_tag": successor["annotated_tag"],
        },
        "research_contract": research_contract,
        "research_contract_sha256": _canonical_sha256(research_contract),
        "semantic_source_hashes": semantic_sources,
        "semantic_source_count": len(semantic_sources),
        "execution_only_source_hashes": execution_sources,
        "execution_only_source_count": len(execution_sources),
        "completed_side_resume": completed_side_resume_contract(),
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
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt,
        "canonical_receipt_sha256",
    )
    _atomic_json(destination, receipt)
    return receipt


def _validate_v24_v26_invariance_receipt(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    panel: Mapping[str, Any],
    repository_root: Path,
    verify_runtime_artifacts: bool,
) -> Mapping[str, Any]:
    receipt = _load_json(
        manifest_path.parent / V24_V26_INVARIANCE_RECEIPT_NAME,
        label="formal-v24-to-v26 invariance receipt",
    )
    formal_v24 = receipt.get("formal_v24")
    formal_v26 = receipt.get("formal_v26")
    expected_contract = _research_contract_snapshot(
        manifest=manifest,
        source=source,
        panel=panel,
    )
    false_fields = (
        "data_changed",
        "candidate_ladder_changed",
        "duration_vocabulary_changed",
        "estimand_changed",
        "statistics_changed",
        "economic_outcomes_read",
        "economic_values_persisted",
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
    )
    if (
        receipt.get("identity") != V24_V26_INVARIANCE_IDENTITY
        or receipt.get("status") != V24_V26_INVARIANCE_STATUS
        or receipt.get("execution_only_repair")
        != "mmap_lifetime_and_completed_side_semantic_resume"
        or receipt.get("completed_side_resume") != completed_side_resume_contract()
        or not isinstance(formal_v24, Mapping)
        or not isinstance(formal_v26, Mapping)
        or formal_v24.get("canonical_execution_manifest_sha256")
        != V24_EXECUTION_MANIFEST_CANONICAL_SHA256
        or formal_v24.get("manifest_file_sha256")
        != V24_EXECUTION_MANIFEST_FILE_SHA256
        or formal_v24.get("public_base_commit") != V24_PUBLIC_BASE_COMMIT
        or formal_v24.get("annotated_tag") != V24_ANNOTATED_TAG
        or formal_v26.get("canonical_execution_manifest_sha256")
        != manifest.get("canonical_execution_manifest_sha256")
        or formal_v26.get("manifest_file_sha256") != _file_sha256(manifest_path)
        or formal_v26.get("public_base_commit") != manifest.get("public_base_commit")
        or formal_v26.get("annotated_tag") != manifest.get("annotated_tag")
        or receipt.get("research_contract") != expected_contract
        or receipt.get("research_contract_sha256") != _canonical_sha256(expected_contract)
        or int(receipt.get("semantic_source_count", 0))
        != len(_V24_V26_RESEARCH_SEMANTIC_SOURCE_PATHS)
        or int(receipt.get("execution_only_source_count", 0))
        != len(_V24_V26_EXECUTION_ONLY_SOURCE_PATHS)
        or any(receipt.get(field) is not False for field in false_fields)
        or receipt.get("canonical_receipt_sha256")
        != _document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise OfflineOrchestratorError("formal-v24-to-v26 invariance receipt failed closed")
    if verify_runtime_artifacts:
        expected_sources = _v24_v26_semantic_source_hashes(
            repository_root=repository_root
        )
        if receipt.get("semantic_source_hashes") != expected_sources:
            raise OfflineOrchestratorError(
                "formal-v24-to-v26 semantic source identity drifted"
            )
        expected_execution_sources = _v24_v26_execution_source_hashes(
            repository_root=repository_root
        )
        if receipt.get("execution_only_source_hashes") != expected_execution_sources:
            raise OfflineOrchestratorError(
                "formal-v24-to-v26 execution source identity drifted"
            )
    return receipt


def _completed_buy_cache_census(
    bundle: FormalOfflineBundle,
) -> dict[str, Any]:
    try:
        cache_root = resolve_portable_path(
            PREDECESSOR_DAY_CACHE_ROOT,
            root=bundle.repository_root,
        ).resolve()
    except (RuntimeError, ValueError) as exc:
        raise OfflineOrchestratorError(
            "formal-v24 predecessor cache root cannot be resolved"
        ) from exc
    progress_root = cache_root / "progress"
    entries_root = cache_root / "entries"
    if not progress_root.is_dir() or not entries_root.is_dir():
        raise OfflineOrchestratorError("formal-v24 predecessor cache roots are missing")
    expected_source = bundle.source_manifest["canonical_manifest_sha256"]
    expected_panel = bundle.panel_manifest["canonical_panel_manifest_sha256"]
    expected_fold = bundle.execution_manifest["fold_manifest_sha256"]
    progress_schema = (
        f"{offline.IDENTITY}.offline_replay_adapter_v1.day_progress.v2"
    )
    cache_schema = f"{offline.IDENTITY}.offline_replay_adapter_v1.day_cache.v2"
    completed_units: list[dict[str, Any]] = []
    completed_counts: dict[str, int] = {}
    excluded_states: dict[str, int] = {}
    for progress_path in sorted(progress_root.glob("*.json")):
        progress = _load_json(progress_path, label="formal-v24 day progress receipt")
        key = progress.get("cache_key")
        if not isinstance(key, Mapping) or key.get(
            "execution_manifest_sha256"
        ) != V24_EXECUTION_MANIFEST_CANONICAL_SHA256:
            continue
        if (
            progress.get("schema_version") != progress_schema
            or progress.get("cache_key_sha256") != progress_path.stem
            or progress.get("cache_key_sha256")
            != _canonical_sha256({"schema_version": cache_schema, **dict(key)})
            or progress.get("receipt_sha256")
            != _document_sha256(progress, "receipt_sha256")
            or key.get("adapter_artifact_sha256")
            != V24_REPLAY_ADAPTER_ARTIFACT_SHA256
            or key.get("source_manifest_sha256") != expected_source
            or key.get("panel_manifest_sha256") != expected_panel
            or key.get("fold_manifest_sha256") != expected_fold
            or key.get("exact_owner_policy_sha256")
            != offline.ACTIVE_OWNER_POLICY_SHA256
        ):
            raise OfflineOrchestratorError("formal-v24 progress receipt identity drifted")
        side = str(key.get("side"))
        stage = str(key.get("stage"))
        state = str(progress.get("state"))
        if side == "SELL":
            excluded_states[state] = excluded_states.get(state, 0) + 1
            if (entries_root / progress_path.stem / "manifest.json").exists():
                raise OfflineOrchestratorError(
                    "formal-v24 unfinished SELL entry unexpectedly became admissible"
                )
            continue
        if side != "BUY" or state != "complete":
            raise OfflineOrchestratorError(
                "formal-v24 completed-side census found non-complete BUY work"
            )
        entry_path = entries_root / progress_path.stem / "manifest.json"
        entry = _load_json(entry_path, label="formal-v24 completed BUY cache manifest")
        files = entry.get("files")
        if (
            entry.get("schema_version") != cache_schema
            or entry.get("cache_key_sha256") != progress_path.stem
            or entry.get("cache_key") != dict(key)
            or entry.get("complete") is not True
            or entry.get("atomic_admission") is not True
            or entry.get("receipt_sha256") != _document_sha256(entry, "receipt_sha256")
            or not isinstance(files, Mapping)
            or not files
        ):
            raise OfflineOrchestratorError("formal-v24 completed BUY cache manifest drifted")
        file_bindings: dict[str, str] = {}
        for name, raw_binding in sorted(files.items()):
            if not isinstance(raw_binding, Mapping):
                raise OfflineOrchestratorError("formal-v24 BUY cache file binding drifted")
            file_path = entry_path.parent / str(raw_binding.get("file"))
            expected_sha = str(raw_binding.get("sha256"))
            if not file_path.is_file() or _file_sha256(file_path) != expected_sha:
                raise OfflineOrchestratorError("formal-v24 BUY cache file hash drifted")
            file_bindings[str(name)] = expected_sha
        completed_counts[stage] = completed_counts.get(stage, 0) + 1
        completed_units.append(
            {
                "cache_key_sha256": progress_path.stem,
                "stage": stage,
                "fold_id": str(key.get("fold_id")),
                "utc_day": str(key.get("utc_day")),
                "progress_receipt_sha256": progress["receipt_sha256"],
                "cache_receipt_sha256": entry["receipt_sha256"],
                "file_sha256": file_bindings,
            }
        )
    contract = completed_side_resume_contract()
    if (
        completed_counts != contract["required_stage_counts"]
        or len(completed_units) != contract["required_complete_cache_units"]
        or excluded_states != {"running": 10}
    ):
        raise OfflineOrchestratorError("formal-v24 completed-side cache census drifted")
    unit_set_sha256 = _canonical_sha256(completed_units)
    receipt: dict[str, Any] = {
        "schema_version": (
            f"{offline.IDENTITY}.formal_v26_completed_side_cache_census.v1"
        ),
        "identity": "f05_formal_v24_completed_buy_cache_census_v1",
        "status": "passed_577_completed_buy_units_sell_excluded",
        "successor_execution_manifest_sha256": bundle.execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "completed_side_resume": contract,
        "completed_stage_counts": completed_counts,
        "completed_cache_units": len(completed_units),
        "excluded_predecessor_states": {"SELL": excluded_states},
        "completed_unit_set_sha256": unit_set_sha256,
        "completed_units": completed_units,
        "parquet_payloads_parsed": False,
        "economic_values_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt,
        "canonical_receipt_sha256",
    )
    return receipt


def admit_completed_buy_cache_census_receipt(
    successor_manifest_path: Path,
    output_path: Path,
) -> Mapping[str, Any]:
    successor_path = successor_manifest_path.expanduser().resolve()
    destination = output_path.expanduser().resolve()
    if destination != successor_path.parent / COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME:
        raise OfflineOrchestratorError("completed BUY cache census path is not canonical")
    if destination.exists():
        raise OfflineOrchestratorError("immutable completed BUY cache census already exists")
    bundle = _load_formal_offline_bundle(
        successor_path,
        verify_source_bytes=True,
        require_clean_tag=True,
        require_cpp_qualification=False,
        require_invariance=True,
        require_completed_side_census=False,
    )
    receipt = _completed_buy_cache_census(bundle)
    _atomic_json(destination, receipt)
    return receipt


def _validate_completed_buy_cache_census_receipt(
    bundle: FormalOfflineBundle,
    *,
    verify_cache_artifacts: bool,
) -> Mapping[str, Any]:
    path = bundle.execution_manifest_path.parent / COMPLETED_BUY_CACHE_CENSUS_RECEIPT_NAME
    receipt = _load_json(path, label="formal-v24 completed BUY cache census")
    if (
        receipt.get("successor_execution_manifest_sha256")
        != bundle.execution_manifest.get("canonical_execution_manifest_sha256")
        or receipt.get("completed_side_resume") != completed_side_resume_contract()
        or receipt.get("economic_values_read") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("action_authorized") is not False
        or receipt.get("live_authorized") is not False
        or receipt.get("canonical_receipt_sha256")
        != _document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise OfflineOrchestratorError("completed BUY cache census receipt failed closed")
    if verify_cache_artifacts and receipt != _completed_buy_cache_census(bundle):
        raise OfflineOrchestratorError("completed BUY cache census no longer reproduces")
    return receipt


def _resolve_bound_file(
    binding: Mapping[str, Any],
    *,
    label: str,
    repository_root: Path,
) -> Path:
    if not {"path", "sha256"} <= set(binding):
        raise OfflineOrchestratorError(f"{label} binding is incomplete")
    digest = str(binding.get("sha256"))
    if _SHA_RE.fullmatch(digest) is None:
        raise OfflineOrchestratorError(f"{label} SHA256 is invalid")
    try:
        path = (
            resolve_portable_path(
                str(binding.get("path")),
                root=repository_root,
            )
            .expanduser()
            .resolve()
        )
    except (RuntimeError, ValueError) as exc:
        raise OfflineOrchestratorError(f"{label} path is not portable") from exc
    if not path.is_file() or _file_sha256(path) != digest:
        raise OfflineOrchestratorError(f"{label} file hash drifted")
    if "size_bytes" in binding and int(binding["size_bytes"]) != path.stat().st_size:
        raise OfflineOrchestratorError(f"{label} file size drifted")
    return path


def _portable_path(path: Path, *, repository_root: Path) -> str:
    """Encode a formal binding without publishing a machine-specific locator."""

    roots = mechanics.PortableRoots.from_layout(
        offline.default_layout(),
        repository_root=repository_root,
    )
    resolved = path.expanduser().resolve()
    for marker, configured_root in roots.marker_roots():
        try:
            relative = resolved.relative_to(configured_root)
        except ValueError:
            continue
        return marker if not relative.parts else f"{marker}/{relative.as_posix()}"
    raise OfflineOrchestratorError("formal binding lies outside the governed portable roots")


def _binding(path: Path, *, repository_root: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise OfflineOrchestratorError(f"formal bound file is missing: {resolved}")
    return {
        "path": _portable_path(resolved, repository_root=repository_root),
        "sha256": _file_sha256(resolved),
        "size_bytes": int(resolved.stat().st_size),
    }


def _build_dataset_binding(
    *,
    source_path: Path,
    source: Mapping[str, Any],
    nested_folds: Mapping[str, Any],
    repository_root: Path,
) -> dict[str, Any]:
    """Derive the mandatory dataset binding from admitted, outcome-blind inputs."""

    selected_days = list(source.get("selected_days") or ())
    outer_folds = nested_folds.get("outer_folds")
    if not isinstance(outer_folds, list) or not outer_folds:
        raise OfflineOrchestratorError("dataset binding lacks outer chronological folds")
    folds: list[dict[str, list[str]]] = []
    test_days: set[str] = set()
    for index, raw_fold in enumerate(outer_folds):
        if not isinstance(raw_fold, Mapping):
            raise OfflineOrchestratorError(f"dataset binding outer fold {index} is invalid")
        train = [str(day) for day in raw_fold.get("train_days") or ()]
        test = [str(day) for day in raw_fold.get("test_days") or ()]
        folds.append({"train_days": train, "test_days": test})
        test_days.update(test)
    binding: dict[str, Any] = {
        "schema_version": dataset_governance.SCHEMA_VERSION,
        "experiment_id": IDENTITY,
        "experiment_class": "chronological_policy_learning",
        "universe_manifest": {
            # This receipt is owner-private execution evidence. The enclosing
            # formal manifest remains portable and hash-binds these bytes.
            "path": str(source_path.expanduser().resolve()),
            "sha256": _file_sha256(source_path),
            "days_field": "selected_days",
            "eligibility_mode": "exact",
        },
        "required_capabilities": [
            "individual_trades",
            "native_normalized_bbo_100ms",
            "native_normalized_l2_top20_100ms",
            "causal_v12_prediction_overlay",
            "previous_natural_day_warmup",
            "d_plus_1_common_washout",
            "modeled_queue_same_millisecond_ambiguity_censoring",
        ],
        "eligible_days": selected_days,
        "excluded_days": [],
        "evidence": {
            "panels": {
                "development": {"days": selected_days, "status": "open"},
                "embargo_1": {"days": [], "status": "not_allocated"},
                "validation": {"days": [], "status": "not_allocated"},
                "embargo_2": {"days": [], "status": "not_allocated"},
                "sealed_holdout": {"days": [], "status": "not_allocated"},
            }
        },
        "training_window": {
            "mode": "expanding_all_eligible_pre_cutoff",
            "cutoff_basis": "evidence_panel_boundary",
            "source_authorities": [mechanics.SOURCE_AUTHORITY],
            "source_pooling": "single_authority",
        },
        "oof": {
            "enabled": True,
            "scope": "development_only",
            "test_day_count": len(test_days),
            "folds": folds,
        },
        "execution_denominator": {
            "role": "outer_oof_learning_algorithm",
            "days": sorted(test_days),
            "claims_current_50_day_baseline": False,
            "future_canonical_50_day_confirmation_required": True,
            "one_shot_effect_aggregation_used": False,
        },
        "binding_timing": {
            "created_before_economic_execution": True,
            "post_execution_remediation": False,
        },
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    try:
        dataset_governance.validate_dataset_binding(
            binding,
            project_root=repository_root,
        )
    except ValueError as exc:
        raise OfflineOrchestratorError("derived dataset binding failed governance") from exc
    return binding


def _git(*args: str, root: Path) -> str:
    try:
        result = subprocess.run(
            ("git", *args),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise OfflineOrchestratorError(f"git identity probe failed: {' '.join(args)}") from exc
    return result.stdout.strip()


def _validate_clean_annotated_tag(
    *,
    repository_root: Path,
    commit_sha: str,
    tag: str,
) -> None:
    # Git SHA-1 repositories still expose 40 characters.  The manifest stores the
    # exact native object id and validates it separately from artifact SHA256.
    if len(commit_sha) not in {40, 64} or re.fullmatch(r"[0-9a-f]+", commit_sha) is None:
        raise OfflineOrchestratorError("public base commit is malformed")
    if _TAG_RE.fullmatch(tag) is None:
        raise OfflineOrchestratorError("formal execution tag is malformed")
    if _git("status", "--porcelain", root=repository_root):
        raise OfflineOrchestratorError("formal economics require a clean worktree")
    head = _git("rev-parse", "HEAD", root=repository_root)
    if head != commit_sha:
        raise OfflineOrchestratorError("HEAD drifted from the formal execution commit")
    if _git("cat-file", "-t", f"refs/tags/{tag}", root=repository_root) != "tag":
        raise OfflineOrchestratorError("formal execution requires an annotated tag")
    if _git("rev-parse", f"refs/tags/{tag}^{{}}", root=repository_root) != head:
        raise OfflineOrchestratorError("formal execution tag does not identify HEAD")


@dataclass(frozen=True, slots=True, init=False)
class FormalOfflineBundle:
    execution_manifest_path: Path
    execution_manifest: Mapping[str, Any]
    source_manifest_path: Path
    source_manifest: Mapping[str, Any]
    panel_manifest_path: Path
    panel_manifest: Mapping[str, Any]
    dataset_binding_path: Path | None
    dataset_binding: Mapping[str, Any]
    panel_files: Mapping[str, Path]
    repository_root: Path


def _new_formal_offline_bundle(
    *,
    execution_manifest_path: Path,
    execution_manifest: Mapping[str, Any],
    source_manifest_path: Path,
    source_manifest: Mapping[str, Any],
    panel_manifest_path: Path,
    panel_manifest: Mapping[str, Any],
    panel_files: Mapping[str, Path],
    repository_root: Path,
    dataset_binding_path: Path | None = None,
    dataset_binding: Mapping[str, Any] | None = None,
) -> FormalOfflineBundle:
    """Construct a bundle only after the strict loader validates every binding."""

    bundle = object.__new__(FormalOfflineBundle)
    values = {
        "execution_manifest_path": execution_manifest_path,
        "execution_manifest": execution_manifest,
        "source_manifest_path": source_manifest_path,
        "source_manifest": source_manifest,
        "panel_manifest_path": panel_manifest_path,
        "panel_manifest": panel_manifest,
        "dataset_binding_path": dataset_binding_path,
        "dataset_binding": dataset_binding or {},
        "panel_files": panel_files,
        "repository_root": repository_root,
    }
    for field, value in values.items():
        object.__setattr__(bundle, field, value)
    return bundle


def _validate_panel_manifest(
    panel: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    repository_root: Path,
) -> dict[str, Path]:
    if panel.get("schema_version") != PANEL_SCHEMA_VERSION:
        raise OfflineOrchestratorError("canonical nested-OOF panel schema drifted")
    if (
        panel.get("status") != "offline_outcome_blind_sequential_mechanics_panel_admitted"
        or panel.get("formal_execution_eligible") is not True
    ):
        raise OfflineOrchestratorError(
            "canonical panel is not a formally admitted sequential-builder artifact"
        )
    if panel.get("identity") != offline.IDENTITY:
        raise OfflineOrchestratorError("canonical panel identity drifted")
    if panel.get("canonical_panel_manifest_sha256") != _document_sha256(
        panel, "canonical_panel_manifest_sha256"
    ):
        raise OfflineOrchestratorError("canonical panel manifest hash drifted")
    if panel.get("source_manifest_sha256") != source.get("canonical_manifest_sha256"):
        raise OfflineOrchestratorError("panel is not bound to the admitted source manifest")
    if tuple(panel.get("selected_days") or ()) != tuple(source.get("selected_days") or ()):
        raise OfflineOrchestratorError("panel day order drifted from source admission")
    if panel.get("panel_role") != offline.PANEL_ROLE:
        raise OfflineOrchestratorError("panel role is not family-specific historical Development")
    if panel.get("mechanics_identity") != mechanics.MECHANICS_IDENTITY:
        raise OfflineOrchestratorError("canonical panel mechanics identity drifted")
    if panel.get("source_authority") != mechanics.SOURCE_AUTHORITY:
        raise OfflineOrchestratorError("canonical panel source authority drifted")
    if panel.get("queue_identity") != offline.QUEUE_IDENTITY:
        raise OfflineOrchestratorError("panel queue identity drifted")
    if panel.get("exact_queue_policy_eligible") is not False:
        raise OfflineOrchestratorError("modeled-queue panel claimed exact queue authority")
    if panel.get("same_millisecond_ambiguity_policy") != "censor":
        raise OfflineOrchestratorError("same-millisecond ambiguity is not censored")
    if panel.get("economic_outcomes_present") is not False:
        raise OfflineOrchestratorError(
            "canonical panel must declare economic_outcomes_present=false"
        )
    if panel.get("one_shot_training_labels_precomputed") is not False:
        raise OfflineOrchestratorError(
            "canonical panel must declare one_shot_training_labels_precomputed=false"
        )
    if panel.get("outer_train_label_generation_required") is not True:
        raise OfflineOrchestratorError(
            "canonical panel must declare outer_train_label_generation_required=true"
        )
    if panel.get("one_shot_effect_aggregation_used") is not False:
        raise OfflineOrchestratorError("one-shot effects cannot enter formal policy economics")
    if panel.get("repeated_sequential_policy_required") is not True:
        raise OfflineOrchestratorError("panel does not require repeated sequential policy replay")
    if panel.get("validation_read") is not False or panel.get("sealed_holdout_read") is not False:
        raise OfflineOrchestratorError("Validation or sealed holdout entered the panel")
    if panel.get("exact_current_owner_policy_sha256") != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineOrchestratorError("panel control is not exact current owner B0")
    if panel.get("exact_current_predicate_bundle_sha256") != offline.ACTIVE_PREDICATE_BUNDLE_SHA256:
        raise OfflineOrchestratorError("panel owner predicate identity drifted")
    if panel.get("exact_current_private_config_sha256") != offline.ACTIVE_PRIVATE_CONFIG_SHA256:
        raise OfflineOrchestratorError("panel owner private-config identity drifted")
    if panel.get("permissions") != {
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflineOrchestratorError("canonical panel permissions drifted")
    sequential_builder = panel.get("sequential_panel_builder")
    if not isinstance(sequential_builder, Mapping):
        raise OfflineOrchestratorError("canonical panel lacks its sequential-builder binding")
    if (
        sequential_builder.get("status") != "outcome_blind_b0_mechanics_days_admitted"
        or sequential_builder.get("selected_days") != list(source.get("selected_days") or ())
        or sequential_builder.get("selected_day_count") != offline.REQUIRED_DAYS
        or sequential_builder.get("permissions")
        != {
            "economic_outcomes_read": False,
            "labels_read": False,
            "candidate_actions_generated": False,
            "action_authorized": False,
            "live_authorized": False,
        }
    ):
        raise OfflineOrchestratorError("sequential-builder evidence contract drifted")
    for role in ("manifest", "merged_panel_manifest", "portable_replay_binding"):
        _resolve_bound_file(
            sequential_builder.get(role) or {},
            label=f"sequential builder {role}",
            repository_root=repository_root,
        )
    owner_artifacts = panel.get("owner_artifacts")
    expected_owner = {
        "policy": offline.ACTIVE_OWNER_POLICY_SHA256,
        "predicate_bundle": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "private_config": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    }
    if not isinstance(owner_artifacts, Mapping) or set(owner_artifacts) != set(expected_owner):
        raise OfflineOrchestratorError("canonical panel owner artifact census drifted")
    for role, expected_sha256 in expected_owner.items():
        owner_path = _resolve_bound_file(
            owner_artifacts[role],
            label=f"exact owner {role}",
            repository_root=repository_root,
        )
        if _file_sha256(owner_path) != expected_sha256:
            raise OfflineOrchestratorError(f"exact owner {role} identity drifted")
    receipts = panel.get("day_receipt_sha256")
    source_receipts = {
        row["utc_day"]: row["day_receipt_sha256"]
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping) and row.get("utc_day") in set(source.get("selected_days", ()))
    }
    if not isinstance(receipts, Mapping) or dict(receipts) != source_receipts:
        raise OfflineOrchestratorError("panel day receipts drifted from source admission")
    files = panel.get("files")
    if not isinstance(files, Mapping):
        raise OfflineOrchestratorError("canonical mechanics panel files must be an object")
    forbidden_files = set(files) & _FORBIDDEN_ECONOMIC_PANEL_FILES
    if forbidden_files:
        roles = ", ".join(sorted(forbidden_files))
        raise OfflineOrchestratorError(
            f"economic label files are forbidden in the mechanics panel: {roles}"
        )
    if set(files) != set(_PANEL_FILES):
        raise OfflineOrchestratorError("canonical panel file census is incomplete")
    return {
        role: _resolve_bound_file(
            files[role],
            label=f"panel {role}",
            repository_root=repository_root,
        )
        for role in _PANEL_FILES
    }


def _cpp_qualification_contract() -> dict[str, Any]:
    return {
        "identity": CPP_QUALIFICATION_IDENTITY,
        "receipt_file": CPP_QUALIFICATION_RECEIPT_NAME,
        "invariance_receipt_file": V24_V26_INVARIANCE_RECEIPT_NAME,
        "required_status": "passed_real_day_all_opportunity_all_arm_lockstep",
        "qualification_day_index": 0,
        "all_opportunities_required": True,
        "all_side_specific_duration_arms_required": True,
        "zero_mismatches_required": True,
        "all_panel_zero_economic_builder_walk_required": True,
        "builder_preflight_receipt_file": CPP_BUILDER_PREFLIGHT_RECEIPT_NAME,
        "builder_preflight_opportunity_count": CPP_BUILDER_PREFLIGHT_OPPORTUNITIES,
        "builder_formal_runtime_instantiation_required": True,
        "first_opportunity_all_arm_preflight_required": True,
        "first_opportunity_all_arm_preflight_receipt_file": (
            CPP_QUICK_PREFLIGHT_RECEIPT_NAME
        ),
        "economic_values_persisted": False,
    }


def _current_cpp_qualification_source_hashes() -> dict[str, str]:
    """Rebind the exact source and extension bytes qualified by real-day parity."""

    root = Path(__file__).resolve().parents[4]
    audit_root = root / "research/families/f05_fill_quality_quote_ev/audit"
    try:
        cpp = importlib.import_module("narrowgate_cpp")
    except ImportError as exc:
        raise OfflineOrchestratorError("qualified C++ one-shot extension is unavailable") from exc
    extension_path_value = getattr(cpp, "__file__", None)
    if not extension_path_value:
        raise OfflineOrchestratorError("qualified C++ one-shot extension has no byte identity")
    paths = {
        "qualification_runner": audit_root
        / "causal_multichannel_window_boolean_cooldown_cpp_real_day_lockstep_v22.py",
        "observation_tape": audit_root
        / "causal_multichannel_window_boolean_cooldown_cpp_observation_tape_v21.py",
        "cpp_runtime": audit_root
        / "causal_multichannel_window_boolean_cooldown_cpp_runtime_v22.py",
        "replay_adapter": audit_root
        / (
            "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
            "offline_replay_adapter_v1.py"
        ),
        "shared_prefix": audit_root
        / "causal_multichannel_window_boolean_cooldown_shared_prefix.py",
        "study": audit_root / "multiscale_ema_boolean_cooldown_duration_policy_study.py",
        "backtest_tick": root / "models/backtest_tick.py",
        "tick_replay_cpp": root / "cpp/narrowgate_cpp/tick_replay.cpp",
        "tick_replay_hpp": root / "cpp/narrowgate_cpp/tick_replay.hpp",
        "bindings_cpp": root / "cpp/narrowgate_cpp/bindings.cpp",
        "cpp_extension": Path(str(extension_path_value)).resolve(),
    }
    if any(not path.is_file() for path in paths.values()):
        raise OfflineOrchestratorError("qualified C++ one-shot source census is incomplete")
    return {name: _file_sha256(path) for name, path in paths.items()}


def _validate_cpp_qualification_receipt(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    *,
    verify_runtime_artifacts: bool = True,
) -> Mapping[str, Any]:
    contract = manifest.get("cpp_one_shot_qualification")
    if contract != _cpp_qualification_contract():
        raise OfflineOrchestratorError("C++ one-shot qualification contract drifted")
    invariance_receipt = _load_json(
        manifest_path.parent / V24_V26_INVARIANCE_RECEIPT_NAME,
        label="formal-v24-to-v26 invariance receipt",
    )
    builder_path = manifest_path.parent / CPP_BUILDER_PREFLIGHT_RECEIPT_NAME
    builder_receipt = _load_json(
        builder_path,
        label="C++ all-panel builder preflight receipt",
    )
    if (
        builder_receipt.get("identity") != CPP_BUILDER_PREFLIGHT_IDENTITY
        or builder_receipt.get("status") != CPP_BUILDER_PREFLIGHT_STATUS
        or builder_receipt.get("execution_manifest_sha256")
        != manifest.get("canonical_execution_manifest_sha256")
        or int(builder_receipt.get("opportunity_count", 0)) != CPP_BUILDER_PREFLIGHT_OPPORTUNITIES
        or builder_receipt.get("cpp_startup_contract_validation") is not True
        or int(builder_receipt.get("cpp_startup_validated_row_count", 0))
        != CPP_BUILDER_PREFLIGHT_OPPORTUNITIES
        or int(builder_receipt.get("economic_evaluator_call_count", -1)) != 0
        or builder_receipt.get("formal_v24_to_v26_invariance_receipt_sha256")
        != invariance_receipt.get("canonical_receipt_sha256")
        or builder_receipt.get("economic_values_read") is not False
        or builder_receipt.get("economic_values_persisted") is not False
        or builder_receipt.get("validation_read") is not False
        or builder_receipt.get("sealed_holdout_read") is not False
        or builder_receipt.get("action_authorized") is not False
        or builder_receipt.get("live_authorized") is not False
        or builder_receipt.get("canonical_receipt_sha256")
        != _document_sha256(builder_receipt, "canonical_receipt_sha256")
    ):
        raise OfflineOrchestratorError("C++ all-panel builder preflight receipt failed closed")
    quick_receipt = _load_json(
        manifest_path.parent / CPP_QUICK_PREFLIGHT_RECEIPT_NAME,
        label="C++ first-opportunity all-arm preflight receipt",
    )
    if (
        quick_receipt.get("identity") != CPP_QUICK_PREFLIGHT_IDENTITY
        or quick_receipt.get("status") != CPP_QUICK_PREFLIGHT_STATUS
        or quick_receipt.get("execution_manifest_sha256")
        != manifest.get("canonical_execution_manifest_sha256")
        or int(quick_receipt.get("opportunity_count", 0)) != 1
        or int(quick_receipt.get("arm_count", 0)) != 8
        or int(quick_receipt.get("zero_mismatch_arm_count", 0)) != 8
        or quick_receipt.get("all_panel_builder_preflight_receipt_sha256")
        != builder_receipt.get("canonical_receipt_sha256")
        or quick_receipt.get("formal_v24_to_v26_invariance_receipt_sha256")
        != invariance_receipt.get("canonical_receipt_sha256")
        or quick_receipt.get("economic_values_persisted") is not False
        or quick_receipt.get("economic_values_exposed") is not False
        or quick_receipt.get("economic_values_used_for_selection") is not False
        or quick_receipt.get("validation_read") is not False
        or quick_receipt.get("sealed_holdout_read") is not False
        or quick_receipt.get("action_authorized") is not False
        or quick_receipt.get("live_authorized") is not False
        or quick_receipt.get("canonical_receipt_sha256")
        != _document_sha256(quick_receipt, "canonical_receipt_sha256")
    ):
        raise OfflineOrchestratorError(
            "C++ first-opportunity all-arm preflight receipt failed closed"
        )
    receipt_path = manifest_path.parent / CPP_QUALIFICATION_RECEIPT_NAME
    receipt = _load_json(receipt_path, label="C++ one-shot lockstep receipt")
    qualification = receipt.get("qualification_contract")
    if (
        not isinstance(qualification, Mapping)
        or receipt.get("identity") != CPP_QUALIFICATION_IDENTITY
        or receipt.get("status") != contract["required_status"]
        or receipt.get("cpp_one_shot_formal_authorized") is not True
        or receipt.get("python_sequential_engine_remains_authoritative") is not True
        or receipt.get("zero_mismatch_arm_count") != receipt.get("arm_count")
        or int(receipt.get("opportunity_count", 0)) <= 0
        or int(receipt.get("arm_count", 0)) != int(receipt.get("opportunity_count", 0)) * 8
        or qualification.get("all_panel_builder_preflight_receipt_sha256")
        != builder_receipt.get("canonical_receipt_sha256")
        or qualification.get("formal_v24_to_v26_invariance_receipt_sha256")
        != invariance_receipt.get("canonical_receipt_sha256")
        or qualification.get("first_opportunity_all_arm_preflight_receipt_sha256")
        != quick_receipt.get("canonical_receipt_sha256")
        or int(qualification.get("all_panel_builder_preflight_opportunity_count", 0))
        != CPP_BUILDER_PREFLIGHT_OPPORTUNITIES
        or receipt.get("economic_values_persisted") is not False
        or receipt.get("economic_values_used_for_selection") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("action_authorized") is not False
        or receipt.get("live_authorized") is not False
        or receipt.get("canonical_receipt_sha256")
        != _document_sha256(receipt, "canonical_receipt_sha256")
    ):
        raise OfflineOrchestratorError("C++ one-shot lockstep receipt failed closed")
    if (
        qualification.get("execution_manifest_sha256")
        != manifest.get("canonical_execution_manifest_sha256")
        or qualification.get("public_base_commit") != manifest.get("public_base_commit")
        or qualification.get("annotated_tag") != manifest.get("annotated_tag")
        or qualification.get("opportunity_count") != receipt.get("opportunity_count")
        or qualification.get("arm_count") != receipt.get("arm_count")
    ):
        raise OfflineOrchestratorError("C++ one-shot lockstep binding drifted")
    if receipt.get("qualification_sha256") != _canonical_sha256(qualification):
        raise OfflineOrchestratorError("C++ one-shot qualification SHA256 drifted")
    if verify_runtime_artifacts:
        qualified_sources = qualification.get("source_hashes")
        current_sources = _current_cpp_qualification_source_hashes()
        if not isinstance(qualified_sources, Mapping) or dict(qualified_sources) != current_sources:
            raise OfflineOrchestratorError(
                "C++ one-shot qualified source or extension bytes drifted"
            )
    return receipt


def _load_formal_offline_bundle(
    execution_manifest_path: Path,
    *,
    verify_source_bytes: bool = True,
    require_clean_tag: bool = True,
    require_cpp_qualification: bool = True,
    require_invariance: bool = True,
    require_completed_side_census: bool = True,
) -> FormalOfflineBundle:
    """Internal loader with test-only verification controls."""

    path = execution_manifest_path.expanduser().resolve()
    manifest = _load_json(path, label="formal execution manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("identity") != IDENTITY:
        raise OfflineOrchestratorError("formal execution identity drifted")
    if manifest.get("status") != "pre_economic_formal_execution_bound":
        raise OfflineOrchestratorError("formal execution status drifted")
    if manifest.get("canonical_execution_manifest_sha256") != _document_sha256(
        manifest, "canonical_execution_manifest_sha256"
    ):
        raise OfflineOrchestratorError("formal execution manifest hash drifted")
    if manifest.get("backend") != {
        "module": CANONICAL_BACKEND_MODULE,
        "function": CANONICAL_BACKEND_FUNCTION,
        "custom_evaluator_allowed": False,
    }:
        raise OfflineOrchestratorError("formal backend identity drifted")
    if manifest.get("execution_contract") != {
        "control": "B0_CURRENT_EXACT",
        "sequential_repeated_policy": True,
        "one_shot_effect_aggregation_used": False,
        "outer_test_candidate_freeze_required": True,
        "action_alpha_v1_required": True,
    }:
        raise OfflineOrchestratorError("formal execution contract drifted")
    if manifest.get("executor") != formal_executor_contract():
        raise OfflineOrchestratorError("formal executor acceleration contract drifted")
    if manifest.get("cpp_one_shot_qualification") != _cpp_qualification_contract():
        raise OfflineOrchestratorError("formal C++ qualification contract drifted")
    permissions = manifest.get("permissions")
    if permissions != {
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflineOrchestratorError("formal execution permissions drifted")
    root_value = manifest.get("repository_root")
    try:
        repository_root = resolve_portable_path(
            str(root_value),
            root=Path(__file__).resolve().parents[4],
        ).resolve()
    except (RuntimeError, ValueError) as exc:
        raise OfflineOrchestratorError("repository root binding is not portable") from exc
    source_path = _resolve_bound_file(
        manifest.get("source_manifest") or {},
        label="canonical source manifest",
        repository_root=repository_root,
    )
    source = offline.validate_canonical_manifest(
        source_path,
        rehash_sources=verify_source_bytes,
    )
    if len(source.get("selected_days", ())) != offline.REQUIRED_DAYS:
        raise OfflineOrchestratorError("canonical source gate has fewer than 30 admitted days")
    if manifest.get("source_contract") != {
        "panel_role": offline.PANEL_ROLE,
        "queue_identity": offline.QUEUE_IDENTITY,
        "selected_day_count": offline.REQUIRED_DAYS,
        "selection_sha256": source.get("selection_sha256"),
        "day_receipts_revalidated": True,
        "economic_outcomes_read": False,
    }:
        raise OfflineOrchestratorError("formal source contract drifted")
    panel_path = _resolve_bound_file(
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
            raise OfflineOrchestratorError(
                "canonical mechanics panel failed full admission validation"
            ) from exc
    else:
        panel = _load_json(panel_path, label="canonical panel manifest")
    panel_files = _validate_panel_manifest(
        panel,
        source=source,
        repository_root=repository_root,
    )
    source_folds = source.get("fold_manifest")
    if not isinstance(source_folds, Mapping):
        raise OfflineOrchestratorError("source admission lacks frozen folds")
    if manifest.get("fold_manifest_sha256") != source_folds.get("fold_manifest_sha256"):
        raise OfflineOrchestratorError("formal fold manifest drifted")
    try:
        expected_nested_folds = offline.derive_bound_nested_fold_manifest(source)
    except offline.OfflineSourceGateError as exc:
        raise OfflineOrchestratorError(
            "source admission cannot derive the frozen 4x3 nested folds"
        ) from exc
    if manifest.get("nested_fold_manifest") != expected_nested_folds:
        raise OfflineOrchestratorError("formal nested-fold manifest drifted")
    if manifest.get("nested_fold_manifest_sha256") != expected_nested_folds.get(
        "nested_fold_manifest_sha256"
    ):
        raise OfflineOrchestratorError("formal nested-fold SHA256 drifted")
    dataset_binding_path = _resolve_bound_file(
        manifest.get("dataset_binding") or {},
        label="formal dataset binding",
        repository_root=repository_root,
    )
    try:
        dataset_binding, _ = dataset_governance.load_dataset_binding(
            dataset_binding_path,
            expected_file_sha256=str(manifest["dataset_binding"]["sha256"]),
            expected_experiment_id=IDENTITY,
            project_root=repository_root,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise OfflineOrchestratorError("formal dataset binding failed closed") from exc
    expected_oof_folds = [
        {
            "train_days": list(fold["train_days"]),
            "test_days": list(fold["test_days"]),
        }
        for fold in expected_nested_folds["outer_folds"]
    ]
    if (
        dataset_binding.get("eligible_days") != list(source.get("selected_days") or ())
        or (dataset_binding.get("oof") or {}).get("folds") != expected_oof_folds
        or (dataset_binding.get("universe_manifest") or {}).get("sha256")
        != _file_sha256(source_path)
    ):
        raise OfflineOrchestratorError("formal dataset binding drifted from source/folds")
    if require_clean_tag:
        _validate_clean_annotated_tag(
            repository_root=repository_root,
            commit_sha=str(manifest.get("public_base_commit", "")),
            tag=str(manifest.get("annotated_tag", "")),
        )
    if require_invariance:
        _validate_v24_v26_invariance_receipt(
            path,
            manifest,
            source=source,
            panel=panel,
            repository_root=repository_root,
            verify_runtime_artifacts=verify_source_bytes,
        )
    bundle = _new_formal_offline_bundle(
        execution_manifest_path=path,
        execution_manifest=manifest,
        source_manifest_path=source_path,
        source_manifest=source,
        panel_manifest_path=panel_path,
        panel_manifest=panel,
        dataset_binding_path=dataset_binding_path,
        dataset_binding=dataset_binding,
        panel_files=panel_files,
        repository_root=repository_root,
    )
    if require_completed_side_census:
        _validate_completed_buy_cache_census_receipt(
            bundle,
            verify_cache_artifacts=verify_source_bytes,
        )
    if require_cpp_qualification:
        _validate_cpp_qualification_receipt(
            path,
            manifest,
            verify_runtime_artifacts=verify_source_bytes,
        )
    return bundle


def load_formal_offline_bundle(
    execution_manifest_path: Path,
) -> FormalOfflineBundle:
    """Load the sole formal input with source-byte and clean-tag checks required."""

    return _load_formal_offline_bundle(
        execution_manifest_path,
        verify_source_bytes=True,
        require_clean_tag=True,
        require_cpp_qualification=True,
        require_invariance=True,
    )


def load_formal_offline_bundle_for_cpp_qualification(
    execution_manifest_path: Path,
) -> FormalOfflineBundle:
    """Load the frozen identity before its immutable lockstep receipt exists."""

    return _load_formal_offline_bundle(
        execution_manifest_path,
        verify_source_bytes=True,
        require_clean_tag=True,
        require_cpp_qualification=False,
        require_invariance=False,
        require_completed_side_census=False,
    )


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="ascii",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def bind_formal_execution_manifest(
    panel_manifest_path: Path,
    output_path: Path,
    *,
    annotated_tag: str,
    repository_root: Path | None = None,
) -> Mapping[str, Any]:
    """Derive the sole formal input from an admitted panel at a clean tag.

    The caller cannot supply dates, folds, owner identities, backend functions, or
    an evaluator.  Every one of those fields is recovered from the revalidated
    source and panel admission chain.
    """

    root = (repository_root or Path(__file__).resolve().parents[4]).expanduser().resolve()
    destination = output_path.expanduser().resolve()
    if destination.exists():
        raise OfflineOrchestratorError(
            f"immutable formal execution manifest already exists: {destination}"
        )
    dataset_binding_path = destination.parent / DATASET_BINDING_NAME
    if dataset_binding_path.exists():
        raise OfflineOrchestratorError(
            f"immutable formal dataset binding already exists: {dataset_binding_path}"
        )
    commit_sha = _git("rev-parse", "HEAD", root=root)
    _validate_clean_annotated_tag(
        repository_root=root,
        commit_sha=commit_sha,
        tag=annotated_tag,
    )
    panel_path = panel_manifest_path.expanduser().resolve()
    try:
        panel = mechanics.validate_panel(
            panel_path,
            layout=offline.default_layout(),
            repository_root=root,
        )
    except (mechanics.OfflineMechanicsError, OSError, ValueError) as exc:
        raise OfflineOrchestratorError(
            "canonical mechanics panel failed full admission validation"
        ) from exc
    source_binding = panel.get("source_manifest")
    if not isinstance(source_binding, Mapping):
        raise OfflineOrchestratorError("canonical panel lacks its source binding")
    source_path = _resolve_bound_file(
        source_binding,
        label="canonical source manifest",
        repository_root=root,
    )
    source = offline.validate_canonical_manifest(source_path, rehash_sources=True)
    if len(source.get("selected_days", ())) != offline.REQUIRED_DAYS:
        raise OfflineOrchestratorError("canonical source gate has fewer than 30 admitted days")
    _validate_panel_manifest(panel, source=source, repository_root=root)
    folds = source.get("fold_manifest")
    if (
        not isinstance(folds, Mapping)
        or _SHA_RE.fullmatch(str(folds.get("fold_manifest_sha256", ""))) is None
    ):
        raise OfflineOrchestratorError("canonical source admission lacks frozen fold identity")
    try:
        nested_folds = offline.derive_bound_nested_fold_manifest(source)
    except offline.OfflineSourceGateError as exc:
        raise OfflineOrchestratorError(
            "canonical source admission cannot freeze the complete 4x3 fold contract"
        ) from exc
    dataset_binding = _build_dataset_binding(
        source_path=source_path,
        source=source,
        nested_folds=nested_folds,
        repository_root=root,
    )
    _atomic_json(dataset_binding_path, dataset_binding)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "pre_economic_formal_execution_bound",
        "repository_root": "${NARROWGATE_ROOT}",
        "public_base_commit": commit_sha,
        "annotated_tag": annotated_tag,
        "source_manifest": _binding(source_path, repository_root=root),
        "panel_manifest": _binding(panel_path, repository_root=root),
        "dataset_binding": _binding(dataset_binding_path, repository_root=root),
        "fold_manifest_sha256": folds["fold_manifest_sha256"],
        "nested_fold_manifest": nested_folds,
        "nested_fold_manifest_sha256": nested_folds["nested_fold_manifest_sha256"],
        "backend": {
            "module": CANONICAL_BACKEND_MODULE,
            "function": CANONICAL_BACKEND_FUNCTION,
            "custom_evaluator_allowed": False,
        },
        "execution_contract": {
            "control": "B0_CURRENT_EXACT",
            "sequential_repeated_policy": True,
            "one_shot_effect_aggregation_used": False,
            "outer_test_candidate_freeze_required": True,
            "action_alpha_v1_required": True,
        },
        "executor": formal_executor_contract(),
        "cpp_one_shot_qualification": _cpp_qualification_contract(),
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
    _atomic_json(destination, manifest)
    try:
        load_formal_offline_bundle_for_cpp_qualification(destination)
    except Exception:
        destination.unlink(missing_ok=True)
        dataset_binding_path.unlink(missing_ok=True)
        raise
    return manifest


def run_formal_offline_economics(
    execution_manifest_path: Path,
    *,
    output_dir: Path,
) -> Mapping[str, Any]:
    """Run only the repository-owned backend after complete formal admission."""

    bundle = load_formal_offline_bundle(execution_manifest_path)
    try:
        backend = importlib.import_module(CANONICAL_BACKEND_MODULE)
    except ModuleNotFoundError as exc:
        raise OfflineOrchestratorError(
            "canonical repeated-policy backend is not implemented"
        ) from exc
    runner = getattr(backend, CANONICAL_BACKEND_FUNCTION, None)
    if not callable(runner):
        raise OfflineOrchestratorError("canonical backend function is unavailable")
    cache_config = cache_tier_lru.CacheTierConfig.from_environment()
    protected_paths = [
        resolve_portable_path(EXECUTOR_DAY_INPUT_CACHE_ROOT, root=bundle.repository_root),
        resolve_portable_path(EXECUTOR_SEQUENTIAL_CACHE_ROOT, root=bundle.repository_root),
    ]
    governor_root = resolve_portable_path(
        EXECUTOR_WORKER_GOVERNOR_ROOT,
        root=bundle.repository_root,
    )
    execution_sha256 = str(
        bundle.execution_manifest["canonical_execution_manifest_sha256"]
    )
    run_id = f"f05-formal-{execution_sha256[:16]}"
    execution_governance.validate_worker_topology(
        total_worker_tokens=EXECUTOR_GLOBAL_WORKER_TOKENS,
        outer_pool_workers=EXECUTOR_GLOBAL_WORKER_TOKENS,
        nested_pool_workers=0,
    )
    with execution_governance.worker_lease(
        run_id=run_id,
        execution_identity=EXECUTOR_ACCELERATION_IDENTITY,
        requested_tokens=EXECUTOR_GLOBAL_WORKER_TOKENS,
        capacity=EXECUTOR_GLOBAL_WORKER_TOKENS,
        root=governor_root,
    ) as lease:
        with cache_tier_lru.active_cache_manifest(
            cache_config,
            run_id=run_id,
            protected_paths=protected_paths,
            ttl_s=EXECUTOR_ACTIVE_CACHE_TTL_S,
            identity_sha256=execution_sha256,
        ):
            result = runner(execution_manifest_path)
    lease_receipt = dict(lease.receipt)
    if not isinstance(result, Mapping):
        raise OfflineOrchestratorError("canonical backend did not return a result manifest")
    if result.get("schema_version") != FORMAL_RESULT_SCHEMA:
        raise OfflineOrchestratorError("canonical backend result schema drifted")
    if result.get("execution_manifest_sha256") != bundle.execution_manifest.get(
        "canonical_execution_manifest_sha256"
    ):
        raise OfflineOrchestratorError("formal result is not bound to its execution manifest")
    if (
        result.get("repeated_sequential_policy") is not True
        or result.get("one_shot_effect_aggregation_used") is not False
    ):
        raise OfflineOrchestratorError("formal result did not execute sequential policies")
    if result.get("exact_owner_policy_sha256") != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineOrchestratorError("formal result control identity drifted")
    if result.get("validation_read") is not False or result.get("sealed_holdout_read") is not False:
        raise OfflineOrchestratorError("formal result read a forbidden evidence split")
    output = output_dir.expanduser().resolve()
    payload = dict(result)
    payload["host_worker_governor"] = {
        "identity": execution_governance.WORKER_GOVERNOR_IDENTITY,
        "lease_id": lease.lease_id,
        "lease_receipt_sha256": lease_receipt["receipt_sha256"],
        "lease_state": lease_receipt["state"],
        "capacity": lease.capacity,
        "requested_tokens": lease.requested_tokens,
    }
    payload["canonical_result_sha256"] = _document_sha256(payload, "canonical_result_sha256")
    _atomic_json(output / "formal_result.json", payload)
    return payload


def run_formal_exact_owner_one_day_mechanics(
    execution_manifest_path: Path,
    *,
    output_path: Path,
) -> Mapping[str, Any]:
    """Run and immutably admit the fixed one-day exact-B0 mechanics receipt."""

    bundle = load_formal_offline_bundle(execution_manifest_path)
    backend = importlib.import_module(CANONICAL_BACKEND_MODULE)
    runner = getattr(backend, "run_exact_owner_one_day_mechanics", None)
    if not callable(runner):
        raise OfflineOrchestratorError("canonical backend one-day mechanics entry is unavailable")
    result = runner(execution_manifest_path)
    if not isinstance(result, Mapping):
        raise OfflineOrchestratorError(
            "canonical backend one-day mechanics returned a custom payload"
        )
    if (
        result.get("status") != "exact_owner_one_day_mechanics_complete"
        or result.get("execution_manifest_sha256")
        != bundle.execution_manifest["canonical_execution_manifest_sha256"]
        or result.get("worker_count") != 1
        or result.get("exact_owner_noop_parity_count") != result.get("opportunity_count")
        or result.get("economic_values_persisted") is not False
        or result.get("economic_values_used_for_selection") is not False
        or result.get("validation_read") is not False
        or result.get("sealed_holdout_read") is not False
        or result.get("action_authorized") is not False
        or result.get("live_authorized") is not False
    ):
        raise OfflineOrchestratorError(
            "canonical one-day mechanics receipt failed its hard contract"
        )
    destination = output_path.expanduser().resolve()
    if destination.exists():
        raise OfflineOrchestratorError(
            f"immutable one-day mechanics receipt already exists: {destination}"
        )
    payload = dict(result)
    payload["canonical_receipt_sha256"] = _document_sha256(
        payload,
        "canonical_receipt_sha256",
    )
    _atomic_json(destination, payload)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bind = subparsers.add_parser("bind")
    bind.add_argument("panel_manifest", type=Path)
    bind.add_argument("output_manifest", type=Path)
    bind.add_argument("--annotated-tag", required=True)
    invariance = subparsers.add_parser("admit-v23-v24-invariance")
    invariance.add_argument("predecessor_manifest", type=Path)
    invariance.add_argument("successor_manifest", type=Path)
    invariance.add_argument("--output", type=Path, required=True)
    resume_invariance = subparsers.add_parser("admit-v24-v26-invariance")
    resume_invariance.add_argument("predecessor_manifest", type=Path)
    resume_invariance.add_argument("successor_manifest", type=Path)
    resume_invariance.add_argument("--output", type=Path, required=True)
    cache_census = subparsers.add_parser("admit-completed-buy-cache-census")
    cache_census.add_argument("successor_manifest", type=Path)
    cache_census.add_argument("--output", type=Path, required=True)
    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("manifest", type=Path)
    preflight.add_argument("--output", type=Path)
    mechanics_one_day = subparsers.add_parser("diagnose-one-day")
    mechanics_one_day.add_argument("manifest", type=Path)
    mechanics_one_day.add_argument("--output", type=Path, required=True)
    run = subparsers.add_parser("run")
    run.add_argument("manifest", type=Path)
    run.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "bind":
        result = bind_formal_execution_manifest(
            args.panel_manifest,
            args.output_manifest,
            annotated_tag=args.annotated_tag,
        )
        result = {
            "identity": IDENTITY,
            "status": result["status"],
            "execution_manifest_sha256": result["canonical_execution_manifest_sha256"],
            "economic_outcomes_read": False,
        }
    elif args.command == "admit-v23-v24-invariance":
        receipt = admit_v23_v24_invariance_receipt(
            args.predecessor_manifest,
            args.successor_manifest,
            args.output,
        )
        result = {
            "identity": receipt["identity"],
            "status": receipt["status"],
            "research_contract_sha256": receipt["research_contract_sha256"],
            "economic_outcomes_read": False,
        }
    elif args.command == "admit-v24-v26-invariance":
        receipt = admit_v24_v26_invariance_receipt(
            args.predecessor_manifest,
            args.successor_manifest,
            args.output,
        )
        result = {
            "identity": receipt["identity"],
            "status": receipt["status"],
            "research_contract_sha256": receipt["research_contract_sha256"],
            "economic_outcomes_read": False,
        }
    elif args.command == "admit-completed-buy-cache-census":
        receipt = admit_completed_buy_cache_census_receipt(
            args.successor_manifest,
            args.output,
        )
        result = {
            "identity": receipt["identity"],
            "status": receipt["status"],
            "completed_cache_units": receipt["completed_cache_units"],
            "economic_outcomes_read": False,
        }
    elif args.command == "preflight":
        backend = importlib.import_module(CANONICAL_BACKEND_MODULE)
        preflight_runner = getattr(
            backend,
            "preflight_canonical_offline_economics",
            None,
        )
        if not callable(preflight_runner):
            raise OfflineOrchestratorError("canonical backend preflight is unavailable")
        result = preflight_runner(args.manifest)
        if args.output is not None:
            output_path = args.output.expanduser().resolve()
            if output_path.exists():
                raise OfflineOrchestratorError(
                    f"immutable formal preflight receipt already exists: {output_path}"
                )
            payload = dict(result)
            payload["canonical_preflight_sha256"] = _document_sha256(
                payload,
                "canonical_preflight_sha256",
            )
            _atomic_json(output_path, payload)
            result = payload
    elif args.command == "diagnose-one-day":
        result = run_formal_exact_owner_one_day_mechanics(
            args.manifest,
            output_path=args.output,
        )
    else:
        result = run_formal_offline_economics(
            args.manifest,
            output_dir=args.output_dir,
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
