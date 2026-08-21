#!/usr/bin/env python3
"""Freeze the owner-selected BUY E3 policy on all 30 Development days.

This is an owner-override refit, not a repair of the formal research result.
The formal hierarchy and hard-gate failures remain immutable.  The resulting
exact artifact has no OOF return estimate of its own.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as orchestrator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_predicate_view_v1 as predicate_view,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_closeout_v1 as closeout,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BUY_DURATION_POLICY_IDS,
    EMA_HALF_LIVES_S,
    ema_pairs,
)

IDENTITY = closeout.IDENTITY
SCHEMA_VERSION = f"{IDENTITY}.full_development_refit.v1"
EXECUTION_MANIFEST_SCHEMA = f"{IDENTITY}.execution_manifest.v1"
POLICY_ARTIFACT_SCHEMA = f"{IDENTITY}.artifact.v1"
PREDICATE_BUNDLE_SCHEMA = f"{IDENTITY}.selected_predicate_bundle.v1"
FINAL_RECEIPT_SCHEMA = f"{IDENTITY}.final_receipt.v1"
REFIT_RUN_RECEIPT_SCHEMA = f"{IDENTITY}.refit_run_receipt.v1"
PREFLIGHT_RECEIPT_SCHEMA = f"{IDENTITY}.execution_preflight_receipt.v1"
LABEL_MATERIALIZATION_SCHEMA = f"{IDENTITY}.full_development_label_materialization.v1"
LABEL_PROVIDER_IDENTITY = f"{IDENTITY}.full_development_label_materializer_v1"
OWNER_CANDIDATE = closeout.OWNER_CANDIDATE
OWNER_PROFILE = "e3_high_order_multirule_dnf_v1"
OWNER_SEED = 20260813
OWNER_FOLD_ID = "owner_buy_e3_full_development_refit"
EXPECTED_DAY_COUNT = 30
EXPECTED_OPPORTUNITY_COUNT = 3516
EXPECTED_PREDECESSOR_LABEL_DAY_COUNT = 24
EXPECTED_FRESH_LABEL_DAY_COUNT = 6
EXPECTED_ACTIONS = tuple(BUY_DURATION_POLICY_IDS)
FIXED_ACTIONS = tuple(action for action in EXPECTED_ACTIONS if action != "CONTROL_85N")
DIRECT_PREDICATES = frozenset({successor.CURRENT_CAMPAIGN_AGE})
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")

FORMAL_V24_EXECUTION_MANIFEST_SHA256 = (
    "2021a70f2f15f4fff82240cdc494556413da0fc24d369be00fd60628bcf3395a"
)
FORMAL_V24_BUY_ADAPTER_ARTIFACT_SHA256 = (
    "3fe25034aef8a0149f1e67c80ae5b7d236b04790c2e2e583cdb62ebb6cf353e1"
)
FORMAL_V24_BUY_CANDIDATE_BUNDLE_SHA256 = (
    "61c56ebb3f4a0ff58f86247188fcb54464c20705147147ee07bd88c83906ddc9"
)


class OwnerBuyE3RefitError(RuntimeError):
    """Raised when the owner refit or exact artifact identity drifts."""


@dataclass(frozen=True, slots=True)
class OwnerBuyE3ArtifactBundle:
    fitted_candidate: nested.FittedCandidate
    policy_artifact: Mapping[str, Any]
    selected_predicate_bundle: Mapping[str, Any]
    artifact_manifest: Mapping[str, Any]
    artifact_sha256: str


@dataclass(frozen=True, slots=True)
class FullDevelopmentLabelMaterialization:
    outcomes: pd.DataFrame
    supported: pd.DataFrame
    receipt: Mapping[str, Any]
    receipt_sha256: str


def canonical_sha256(value: Any) -> str:
    return closeout.canonical_sha256(value)


def document_sha256(value: Mapping[str, Any], field: str) -> str:
    return closeout.document_sha256(value, field)


def file_sha256(path: Path) -> str:
    return closeout.file_sha256(path)


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )


def _json_file_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def execution_contract() -> dict[str, Any]:
    return {
        "control": "B0_CURRENT_EXACT",
        "sequential_repeated_policy": True,
        "one_shot_effect_aggregation_used": False,
        "selected_side": "BUY",
        "selected_candidate": OWNER_CANDIDATE,
        "selected_profile": OWNER_PROFILE,
        "random_seed": OWNER_SEED,
        "training_scope": "all_30_frozen_development_days",
        "training_day_count": EXPECTED_DAY_COUNT,
        "duration_vocabulary": list(EXPECTED_ACTIONS),
        "default_action": "CONTROL_85N",
        "full_development_refit_count": 1,
        "outer_fold_policy_selection_allowed": False,
        "outer_fold_rule_merge_allowed": False,
        "literal_edit_allowed": False,
        "candidate_substitution_allowed": False,
        "new_economic_arm_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }


def build_execution_manifest_payload(
    *,
    repository_root: Path,
    public_base_commit: str,
    annotated_tag: str,
    source_manifest_path: Path,
    panel_manifest_path: Path,
    owner_decision_path: Path,
    joint_closeout_manifest_path: Path,
    source_execution_manifest_path: Path,
    predicate_bundle_path: Path,
    cpp_qualification_runner_path: Path,
) -> dict[str, Any]:
    """Build the pre-refit manifest after the tested source receives a tag."""

    root = repository_root.expanduser().resolve()
    try:
        source_execution = json.loads(
            source_execution_manifest_path.expanduser().resolve().read_text(encoding="ascii")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3RefitError("source execution manifest is unreadable") from exc
    if not isinstance(source_execution, Mapping):
        raise OwnerBuyE3RefitError("source execution manifest root is malformed")
    backend_contract = source_execution.get("backend")
    executor_contract = source_execution.get("executor")
    nested_fold_manifest = source_execution.get("nested_fold_manifest")
    if (
        not isinstance(backend_contract, Mapping)
        or not isinstance(executor_contract, Mapping)
        or not isinstance(nested_fold_manifest, Mapping)
        or _SHA_RE.fullmatch(str(source_execution.get("fold_manifest_sha256", ""))) is None
        or _SHA_RE.fullmatch(str(source_execution.get("nested_fold_manifest_sha256", ""))) is None
    ):
        raise OwnerBuyE3RefitError("source execution replay contract is malformed")
    bindings = {
        "source_manifest": orchestrator._binding(source_manifest_path, repository_root=root),
        "panel_manifest": orchestrator._binding(panel_manifest_path, repository_root=root),
        "owner_decision": orchestrator._binding(owner_decision_path, repository_root=root),
        "joint_closeout_manifest": orchestrator._binding(
            joint_closeout_manifest_path, repository_root=root
        ),
        "source_execution_manifest": orchestrator._binding(
            source_execution_manifest_path, repository_root=root
        ),
        "outcome_blind_2025_predicate_bundle": orchestrator._binding(
            predicate_bundle_path, repository_root=root
        ),
        "cpp_one_shot_qualification_runner": orchestrator._binding(
            cpp_qualification_runner_path, repository_root=root
        ),
    }
    payload: dict[str, Any] = {
        "schema_version": EXECUTION_MANIFEST_SCHEMA,
        "identity": IDENTITY,
        "status": "pre_refit_owner_execution_bound",
        "repository_root": "${NARROWGATE_ROOT}",
        "public_base_commit": str(public_base_commit),
        "annotated_tag": str(annotated_tag),
        "execution_contract": execution_contract(),
        "backend": dict(backend_contract),
        "executor": dict(executor_contract),
        "fold_manifest_sha256": source_execution["fold_manifest_sha256"],
        "nested_fold_manifest": dict(nested_fold_manifest),
        "nested_fold_manifest_sha256": source_execution["nested_fold_manifest_sha256"],
        "bindings": bindings,
        "evidence_boundary": {
            "formal_closeout_mutated": False,
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "outcome_informed_owner_override": True,
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
        },
        "cache_contract": {
            "formal_v24_buy_labels_may_be_materialized_only_after_exact_hash_audit": True,
            "missing_full_development_days": "fresh_same_contract_compute",
            "strategy_dependent_cross_execution_cache_allowed": False,
            "partial_failed_attempt_economics_reused": False,
        },
        "cpp_one_shot_qualification_contract": {
            "runner_bound_before_execution": True,
            "receipt_file": "cpp_real_day_lockstep_receipt.json",
            "all_3516_target_rows_zero_economic_walk_required": True,
            "first_opportunity_all_arm_preflight_required": True,
            "real_day_all_opportunity_all_arm_lockstep_required": True,
            "zero_mismatch_required": True,
            "economic_values_persisted": False,
            "receipt_bound_by_final_receipt_after_execution": True,
        },
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    payload["canonical_execution_manifest_sha256"] = document_sha256(
        payload, "canonical_execution_manifest_sha256"
    )
    return payload


def bind_execution_manifest(
    *,
    output_path: Path,
    repository_root: Path,
    annotated_tag: str,
    source_manifest_path: Path,
    panel_manifest_path: Path,
    owner_decision_path: Path,
    joint_closeout_manifest_path: Path,
    source_execution_manifest_path: Path,
    predicate_bundle_path: Path,
    cpp_qualification_runner_path: Path,
) -> Mapping[str, Any]:
    root = repository_root.expanduser().resolve()
    destination = output_path.expanduser().resolve()
    if destination.exists():
        raise OwnerBuyE3RefitError("immutable owner execution manifest already exists")
    commit = orchestrator._git("rev-parse", "HEAD", root=root)
    orchestrator._validate_clean_annotated_tag(
        repository_root=root,
        commit_sha=commit,
        tag=annotated_tag,
    )
    payload = build_execution_manifest_payload(
        repository_root=root,
        public_base_commit=commit,
        annotated_tag=annotated_tag,
        source_manifest_path=source_manifest_path,
        panel_manifest_path=panel_manifest_path,
        owner_decision_path=owner_decision_path,
        joint_closeout_manifest_path=joint_closeout_manifest_path,
        source_execution_manifest_path=source_execution_manifest_path,
        predicate_bundle_path=predicate_bundle_path,
        cpp_qualification_runner_path=cpp_qualification_runner_path,
    )
    _atomic_json(destination, payload)
    return payload


def load_owner_execution_bundle(
    execution_manifest_path: Path,
    *,
    repository_root: Path,
    verify_source_bytes: bool,
    require_clean_tag: bool,
) -> orchestrator.FormalOfflineBundle:
    """Load owner execution mechanics through its separately frozen source run."""

    manifest = validate_execution_manifest(
        execution_manifest_path,
        repository_root=repository_root,
        require_clean_tag=require_clean_tag,
    )
    bindings = manifest["bindings"]
    source_execution_path = orchestrator._resolve_bound_file(
        bindings["source_execution_manifest"],
        label="owner source execution manifest",
        repository_root=repository_root,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_sell_only_orchestrator_v1 as sell_orchestrator,
    )

    source_bundle = sell_orchestrator._validate_manifest_core(
        source_execution_path,
        verify_source_bytes=verify_source_bytes,
        require_clean_tag=False,
    )
    expected_source = orchestrator._resolve_bound_file(
        bindings["source_manifest"],
        label="owner source manifest",
        repository_root=repository_root,
    )
    expected_panel = orchestrator._resolve_bound_file(
        bindings["panel_manifest"],
        label="owner panel manifest",
        repository_root=repository_root,
    )
    if (
        source_bundle.source_manifest_path != expected_source
        or source_bundle.panel_manifest_path != expected_panel
    ):
        raise OwnerBuyE3RefitError("owner source execution mechanics binding drifted")
    for field in (
        "backend",
        "executor",
        "fold_manifest_sha256",
        "nested_fold_manifest",
        "nested_fold_manifest_sha256",
    ):
        if manifest.get(field) != source_bundle.execution_manifest.get(field):
            raise OwnerBuyE3RefitError(
                f"owner replay contract drifted from source execution at {field}"
            )
    return orchestrator._new_formal_offline_bundle(
        execution_manifest_path=execution_manifest_path.expanduser().resolve(),
        execution_manifest=manifest,
        source_manifest_path=source_bundle.source_manifest_path,
        source_manifest=source_bundle.source_manifest,
        panel_manifest_path=source_bundle.panel_manifest_path,
        panel_manifest=source_bundle.panel_manifest,
        panel_files=source_bundle.panel_files,
        repository_root=source_bundle.repository_root,
    )


def validate_execution_manifest(
    path: Path,
    *,
    repository_root: Path,
    require_clean_tag: bool,
) -> Mapping[str, Any]:
    manifest_path = path.expanduser().resolve()
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3RefitError("owner execution manifest is unreadable") from exc
    if not isinstance(payload, dict):
        raise OwnerBuyE3RefitError("owner execution manifest root is not an object")
    if (
        payload.get("schema_version") != EXECUTION_MANIFEST_SCHEMA
        or payload.get("identity") != IDENTITY
        or payload.get("status") != "pre_refit_owner_execution_bound"
        or payload.get("execution_contract") != execution_contract()
        or payload.get("canonical_execution_manifest_sha256")
        != document_sha256(payload, "canonical_execution_manifest_sha256")
    ):
        raise OwnerBuyE3RefitError("owner execution manifest identity drifted")
    expected_permissions = {
        "research_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    if payload.get("permissions") != expected_permissions:
        raise OwnerBuyE3RefitError("owner execution manifest permissions drifted")
    bindings = payload.get("bindings")
    if not isinstance(bindings, Mapping):
        raise OwnerBuyE3RefitError("owner execution manifest bindings are missing")
    for label, binding in bindings.items():
        resolved = orchestrator._resolve_bound_file(
            binding,
            label=str(label),
            repository_root=repository_root,
        )
        if file_sha256(resolved) != binding.get("sha256"):
            raise OwnerBuyE3RefitError(f"owner execution binding drifted: {label}")
    if require_clean_tag:
        orchestrator._validate_clean_annotated_tag(
            repository_root=repository_root,
            commit_sha=str(payload.get("public_base_commit", "")),
            tag=str(payload.get("annotated_tag", "")),
        )
    return payload


def bind_full_development_labels(
    panel: nested.NestedOofPanel,
    *,
    outcomes: pd.DataFrame,
    supported: pd.DataFrame,
    provider_identity: str,
    provider_artifact_sha256: str,
) -> tuple[nested.NestedOofPanel, nested.FoldScopedOneShotLabelBatch]:
    metadata = panel.metadata
    buy_index = metadata.index[metadata["side"].astype(str).str.upper() == "BUY"]
    days = tuple(sorted(metadata.loc[buy_index, "utc_day"].astype(str).unique()))
    if len(days) != EXPECTED_DAY_COUNT:
        raise OwnerBuyE3RefitError("full-Development BUY day count drifted")
    request = nested._build_fold_scoped_label_request(
        panel,
        side="BUY",
        outer_fold_id=OWNER_FOLD_ID,
        train_days=days,
        train_index=buy_index,
    )
    batch = nested.bind_fold_scoped_one_shot_labels(
        request,
        outcomes=outcomes,
        supported=supported,
        provider_identity=provider_identity,
        provider_artifact_sha256=provider_artifact_sha256,
    )
    materialized = replace(
        panel,
        action_outcomes=batch.outcomes,
        action_supported=batch.supported,
        learning_label_request_sha256=batch.request_sha256,
        learning_label_payload_sha256=batch.label_payload_sha256,
        learning_label_receipt_sha256=batch.receipt_sha256,
    )
    return materialized, batch


def _full_development_buy_request(
    mechanics: backend.OutcomeBlindMechanics,
) -> tuple[nested.FoldScopedOneShotLabelRequest, pd.Index, tuple[str, ...], pd.DataFrame]:
    panel = mechanics.panel
    metadata = panel.metadata
    buy_index = metadata.index[metadata["side"].astype(str).str.upper() == "BUY"]
    days = tuple(sorted(metadata.loc[buy_index, "utc_day"].astype(str).unique()))
    if len(days) != EXPECTED_DAY_COUNT:
        raise OwnerBuyE3RefitError("full-Development BUY day count drifted")
    request = nested._build_fold_scoped_label_request(
        panel,
        side="BUY",
        outer_fold_id=OWNER_FOLD_ID,
        train_days=days,
        train_index=buy_index,
    )
    replay_rows = backend._bind_outer_train_replay_scope(
        mechanics.replay_inputs.loc[buy_index],
        outer_fold_id=OWNER_FOLD_ID,
    )
    if tuple(str(value) for value in replay_rows.index) != tuple(request.row_ids):
        raise OwnerBuyE3RefitError("full-Development BUY replay row order drifted")
    return request, buy_index, days, replay_rows


def _predecessor_semantic_sources(
    *,
    cache: replay_adapter.DayReplayCache,
    replay_rows: pd.DataFrame,
    days: Sequence[str],
    bindings: backend.FormalExecutionBindings,
    predecessor_execution_manifest_sha256: str,
    predecessor_adapter_artifact_sha256: str,
    predecessor_candidate_bundle_sha256: str,
) -> tuple[dict[str, tuple[pd.DataFrame, pd.DataFrame]], list[dict[str, Any]]]:
    selected_days = {str(day) for day in days}
    candidates: dict[str, tuple[replay_adapter.OneShotSemanticCacheKey, Mapping[str, Any]]] = {}
    if not cache.semantic_one_shot.is_dir():
        raise OwnerBuyE3RefitError("predecessor semantic one-shot cache is missing")
    for path in sorted(cache.semantic_one_shot.glob("*.json")):
        try:
            manifest = json.loads(path.read_text(encoding="ascii"))
        except (OSError, json.JSONDecodeError) as exc:
            raise OwnerBuyE3RefitError("predecessor semantic manifest is unreadable") from exc
        semantic_payload = manifest.get("semantic_key")
        if not isinstance(semantic_payload, Mapping):
            continue
        if (
            str(semantic_payload.get("side", "")).upper() != "BUY"
            or semantic_payload.get("execution_manifest_sha256")
            != predecessor_execution_manifest_sha256
            or str(semantic_payload.get("utc_day", "")) not in selected_days
        ):
            continue
        try:
            key = replay_adapter.OneShotSemanticCacheKey(**dict(semantic_payload))
        except (TypeError, replay_adapter.OfflineReplayAdapterError) as exc:
            raise OwnerBuyE3RefitError("predecessor BUY semantic key is malformed") from exc
        expected_identity = {
            "adapter_artifact_sha256": predecessor_adapter_artifact_sha256,
            "source_manifest_sha256": bindings.source_manifest_sha256,
            "panel_manifest_sha256": bindings.panel_manifest_sha256,
            "fold_manifest_sha256": bindings.fold_manifest_sha256,
            "execution_manifest_sha256": predecessor_execution_manifest_sha256,
            "exact_owner_policy_sha256": bindings.exact_owner_policy_sha256,
            "candidate_policy_sha256": predecessor_candidate_bundle_sha256,
            "side": "BUY",
        }
        for field, expected in expected_identity.items():
            if getattr(key, field) != expected:
                raise OwnerBuyE3RefitError(f"predecessor BUY semantic identity drifted at {field}")
        if key.utc_day in candidates:
            raise OwnerBuyE3RefitError(
                f"duplicate predecessor BUY semantic source for {key.utc_day}"
            )
        candidates[key.utc_day] = (key, manifest)

    frames: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    audits: list[dict[str, Any]] = []
    for day, (key, manifest) in sorted(candidates.items()):
        day_rows = replay_rows.loc[replay_rows["utc_day"].astype(str) == day]
        expected_semantic_sha = replay_adapter._one_shot_semantic_day_input_sha256(day_rows)
        if key.semantic_day_input_sha256 != expected_semantic_sha:
            raise OwnerBuyE3RefitError(f"predecessor BUY semantic day input drifted for {day}")
        loaded = cache.load_semantic_one_shot(key)
        if loaded is None:
            raise OwnerBuyE3RefitError(f"predecessor BUY semantic source disappeared for {day}")
        outcomes, supported, evidence = loaded
        expected_index = pd.Index(
            tuple(str(value) for value in day_rows.index),
            name=outcomes.index.name,
        )
        nested._validate_action_label_frames(
            outcomes,
            supported,
            expected_index=expected_index,
            required_vocabulary=EXPECTED_ACTIONS,
            exact_vocabulary=True,
        )
        source_frames = manifest.get("source_frame_sha256")
        if not isinstance(source_frames, Mapping):
            raise OwnerBuyE3RefitError("predecessor semantic frame binding is missing")
        if source_frames.get("outcomes") != replay_adapter._frame_sha256(
            outcomes
        ) or source_frames.get("supported") != replay_adapter._frame_sha256(supported):
            raise OwnerBuyE3RefitError(f"predecessor semantic frame hash drifted for {day}")
        frames[day] = (outcomes, supported)
        audits.append(
            {
                "utc_day": day,
                "semantic_key_sha256": key.semantic_key_sha256,
                "semantic_cache_receipt_sha256": evidence["semantic_cache_receipt_sha256"],
                "source_cache_key_sha256": evidence["source_cache_key_sha256"],
                "source_cache_receipt_sha256": evidence["source_cache_receipt_sha256"],
                "outcomes_frame_sha256": source_frames["outcomes"],
                "supported_frame_sha256": source_frames["supported"],
            }
        )
    return frames, audits


def materialize_full_development_buy_labels(
    mechanics: backend.OutcomeBlindMechanics,
    *,
    predecessor_cache_root: Path,
    fresh_adapter: backend.CanonicalReplayAdapter,
    owner_execution_manifest_sha256: str,
    predecessor_execution_manifest_sha256: str = FORMAL_V24_EXECUTION_MANIFEST_SHA256,
    predecessor_adapter_artifact_sha256: str = FORMAL_V24_BUY_ADAPTER_ARTIFACT_SHA256,
    predecessor_candidate_bundle_sha256: str = FORMAL_V24_BUY_CANDIDATE_BUNDLE_SHA256,
    expected_predecessor_day_count: int = EXPECTED_PREDECESSOR_LABEL_DAY_COUNT,
    expected_fresh_day_count: int = EXPECTED_FRESH_LABEL_DAY_COUNT,
    expected_panel_opportunity_count: int = EXPECTED_OPPORTUNITY_COUNT,
) -> FullDevelopmentLabelMaterialization:
    """Materialize 30-day BUY labels without importing cross-execution strategy cache."""

    for value, label in (
        (owner_execution_manifest_sha256, "owner execution manifest SHA256"),
        (predecessor_execution_manifest_sha256, "predecessor execution SHA256"),
        (predecessor_adapter_artifact_sha256, "predecessor adapter SHA256"),
        (predecessor_candidate_bundle_sha256, "predecessor candidate bundle SHA256"),
    ):
        if _SHA_RE.fullmatch(str(value)) is None:
            raise OwnerBuyE3RefitError(f"{label} is invalid")
    if len(mechanics.panel.metadata) != expected_panel_opportunity_count:
        raise OwnerBuyE3RefitError("full-Development panel opportunity count drifted")
    request, buy_index, days, replay_rows = _full_development_buy_request(mechanics)
    cache = replay_adapter.DayReplayCache(predecessor_cache_root)
    predecessor_frames, predecessor_audit = _predecessor_semantic_sources(
        cache=cache,
        replay_rows=replay_rows,
        days=days,
        bindings=mechanics.bindings,
        predecessor_execution_manifest_sha256=predecessor_execution_manifest_sha256,
        predecessor_adapter_artifact_sha256=predecessor_adapter_artifact_sha256,
        predecessor_candidate_bundle_sha256=predecessor_candidate_bundle_sha256,
    )
    if len(predecessor_frames) != expected_predecessor_day_count:
        raise OwnerBuyE3RefitError(
            "predecessor BUY semantic day count drifted: "
            f"expected {expected_predecessor_day_count}, observed {len(predecessor_frames)}"
        )
    fresh_days = tuple(day for day in days if day not in predecessor_frames)
    if len(fresh_days) != expected_fresh_day_count:
        raise OwnerBuyE3RefitError(
            "fresh BUY label day count drifted: "
            f"expected {expected_fresh_day_count}, observed {len(fresh_days)}"
        )
    fresh_index = buy_index[
        mechanics.panel.metadata.loc[buy_index, "utc_day"].astype(str).isin(fresh_days)
    ]
    fresh_request = nested._build_fold_scoped_label_request(
        mechanics.panel,
        side="BUY",
        outer_fold_id=OWNER_FOLD_ID,
        train_days=fresh_days,
        train_index=fresh_index,
    )
    fresh_rows = backend._bind_outer_train_replay_scope(
        mechanics.replay_inputs.loc[fresh_index],
        outer_fold_id=OWNER_FOLD_ID,
    )
    owner_bindings = replace(
        mechanics.bindings,
        execution_manifest_sha256=owner_execution_manifest_sha256,
    )
    adapter_request = backend.CanonicalOuterTrainReplayRequest(
        label_request=fresh_request,
        replay_input_sha256=replay_adapter._frame_sha256(fresh_rows),
        bindings=owner_bindings,
    )
    fresh_result = fresh_adapter.generate_outer_train_one_shot_labels(
        adapter_request,
        fresh_rows,
    )
    if not isinstance(fresh_result, backend.CanonicalOneShotReplayResult):
        raise OwnerBuyE3RefitError("fresh BUY label adapter returned a custom payload")
    expected_adapter_receipt = backend.build_outer_train_label_replay_receipt(
        adapter_request,
        adapter_identity=fresh_adapter.identity,
        adapter_artifact_sha256=fresh_adapter.artifact_sha256,
    )
    if dict(fresh_result.receipt) != expected_adapter_receipt:
        raise OwnerBuyE3RefitError("fresh BUY label adapter receipt drifted")
    nested._validate_action_label_frames(
        fresh_result.outcomes,
        fresh_result.supported,
        expected_index=pd.Index(
            tuple(str(value) for value in fresh_index),
            name=fresh_result.outcomes.index.name,
        ),
        required_vocabulary=EXPECTED_ACTIONS,
        exact_vocabulary=True,
    )

    outcome_parts = [predecessor_frames[day][0] for day in days if day in predecessor_frames]
    support_parts = [predecessor_frames[day][1] for day in days if day in predecessor_frames]
    outcome_parts.append(fresh_result.outcomes)
    support_parts.append(fresh_result.supported)
    outcomes = pd.concat(outcome_parts, axis=0).loc[list(request.row_ids)]
    supported = pd.concat(support_parts, axis=0).loc[list(request.row_ids)]
    nested._validate_action_label_frames(
        outcomes,
        supported,
        expected_index=pd.Index(request.row_ids, name=outcomes.index.name),
        required_vocabulary=EXPECTED_ACTIONS,
        exact_vocabulary=True,
    )
    receipt: dict[str, Any] = {
        "schema_version": LABEL_MATERIALIZATION_SCHEMA,
        "identity": LABEL_PROVIDER_IDENTITY,
        "status": "full_development_buy_labels_materialized",
        "side": "BUY",
        "full_request_sha256": request.request_sha256,
        "full_row_sha256": request.row_sha256,
        "full_day_count": len(days),
        "full_row_count": len(outcomes),
        "duration_vocabulary": list(EXPECTED_ACTIONS),
        "predecessor_execution_manifest_sha256": predecessor_execution_manifest_sha256,
        "predecessor_day_count": len(predecessor_audit),
        "predecessor_sources": predecessor_audit,
        "fresh_execution_manifest_sha256": owner_execution_manifest_sha256,
        "fresh_day_count": len(fresh_days),
        "fresh_days": list(fresh_days),
        "fresh_adapter_identity": fresh_adapter.identity,
        "fresh_adapter_artifact_sha256": fresh_adapter.artifact_sha256,
        "fresh_request_sha256": fresh_request.request_sha256,
        "fresh_adapter_receipt_sha256": canonical_sha256(expected_adapter_receipt),
        "fresh_outcomes_frame_sha256": replay_adapter._frame_sha256(fresh_result.outcomes),
        "fresh_supported_frame_sha256": replay_adapter._frame_sha256(fresh_result.supported),
        "combined_outcomes_frame_sha256": replay_adapter._frame_sha256(outcomes),
        "combined_supported_frame_sha256": replay_adapter._frame_sha256(supported),
        "strategy_dependent_cross_execution_cache_imported": False,
        "economic_values_persisted_in_receipt": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    receipt["canonical_materialization_receipt_sha256"] = document_sha256(
        receipt,
        "canonical_materialization_receipt_sha256",
    )
    return FullDevelopmentLabelMaterialization(
        outcomes=outcomes,
        supported=supported,
        receipt=receipt,
        receipt_sha256=receipt["canonical_materialization_receipt_sha256"],
    )


def bind_materialized_full_development_labels(
    panel: nested.NestedOofPanel,
    materialization: FullDevelopmentLabelMaterialization,
) -> tuple[nested.NestedOofPanel, nested.FoldScopedOneShotLabelBatch]:
    return bind_full_development_labels(
        panel,
        outcomes=materialization.outcomes,
        supported=materialization.supported,
        provider_identity=LABEL_PROVIDER_IDENTITY,
        provider_artifact_sha256=materialization.receipt_sha256,
    )


def _e3_entry_and_profile(
    ladder: Sequence[nested.CandidateLadderEntry],
) -> tuple[nested.CandidateLadderEntry, successor.SuccessorSearchProfile]:
    entries = {entry.name: entry for entry in ladder}
    if OWNER_CANDIDATE not in entries:
        raise OwnerBuyE3RefitError("frozen ladder lacks BUY E3")
    entry = entries[OWNER_CANDIDATE]
    if entry.kind != "boolean" or len(entry.profiles) != 1:
        raise OwnerBuyE3RefitError("BUY E3 search contract drifted")
    profile = entry.profiles[0]
    if profile.name != OWNER_PROFILE:
        raise OwnerBuyE3RefitError("BUY E3 profile drifted")
    return entry, profile


def _definition_index(
    bundle: predicate_view.FrozenPredicateBundle,
) -> dict[str, Any]:
    definitions: dict[str, Any] = {}
    for key in ("book.BUY", "trade.BUY"):
        artifact = bundle.artifacts.get(key)
        if artifact is None:
            raise OwnerBuyE3RefitError(f"predicate artifact is missing: {key}")
        for definition in artifact.definitions:
            prior = definitions.get(definition.name)
            if prior is not None and prior != definition:
                raise OwnerBuyE3RefitError("predicate definition collision")
            definitions[definition.name] = definition
    return definitions


def _selected_predicate_payload(
    *,
    fitted: nested.FittedCandidate,
    source_bundle: predicate_view.FrozenPredicateBundle,
) -> dict[str, Any]:
    if fitted.policy is None:
        raise OwnerBuyE3RefitError("BUY E3 refit produced no Boolean policy")
    definitions = _definition_index(source_bundle)
    selected: list[dict[str, Any]] = []
    direct: list[dict[str, Any]] = []
    for name in fitted.policy.predicate_columns:
        definition = definitions.get(name)
        if definition is None:
            if name not in DIRECT_PREDICATES:
                raise OwnerBuyE3RefitError(f"selected predicate is unbound: {name}")
            direct.append(
                {
                    "name": name,
                    "kind": "campaign_age_gt_baseline_duration",
                    "source_field": "campaign_age_s",
                    "clock_group": "context",
                }
            )
            continue
        source = str(definition.source_field).lower()
        if definition.clock_group != "book" or "mid_usdc_per_btc" not in source:
            raise OwnerBuyE3RefitError(f"BUY E3 selected a forbidden trade/depth predicate: {name}")
        selected.append(asdict(definition))
    payload: dict[str, Any] = {
        "schema_version": PREDICATE_BUNDLE_SCHEMA,
        "identity": IDENTITY,
        "side": "BUY",
        "selected_candidate": OWNER_CANDIDATE,
        "selected_profile": OWNER_PROFILE,
        "predicate_columns": list(fitted.policy.predicate_columns),
        "definitions": selected,
        "direct_predicates": direct,
        "ema_half_lives_s": list(EMA_HALF_LIVES_S),
        "ema_pairs_s": [list(pair) for pair in ema_pairs()],
        "ema_pair_count": len(ema_pairs()),
        "normalization_source": source_bundle.receipt(),
        "uses_trade_predicates": False,
        "uses_depth_predicates": False,
        "uses_m2_incremental_features": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    payload["canonical_sha256"] = document_sha256(payload, "canonical_sha256")
    return payload


def fit_owner_buy_e3(
    panel: nested.NestedOofPanel,
    *,
    ladder: Sequence[nested.CandidateLadderEntry],
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    execution_manifest: Mapping[str, Any],
    label_materialization_receipt_sha256: str,
    cpp_qualification_receipt_sha256: str,
    execution_preflight_receipt_sha256: str,
) -> OwnerBuyE3ArtifactBundle:
    if execution_manifest.get("execution_contract") != execution_contract():
        raise OwnerBuyE3RefitError("owner refit execution contract drifted")
    if not panel.has_preconstructed_labels:
        raise OwnerBuyE3RefitError("owner refit lacks full-Development labels")
    if (
        _SHA_RE.fullmatch(str(label_materialization_receipt_sha256)) is None
        or _SHA_RE.fullmatch(str(cpp_qualification_receipt_sha256)) is None
        or _SHA_RE.fullmatch(str(execution_preflight_receipt_sha256)) is None
    ):
        raise OwnerBuyE3RefitError("owner refit execution receipt binding is invalid")
    buy_index = panel.metadata.index[panel.metadata["side"].astype(str).str.upper() == "BUY"]
    training_days = tuple(sorted(panel.metadata.loc[buy_index, "utc_day"].astype(str).unique()))
    if len(training_days) != EXPECTED_DAY_COUNT:
        raise OwnerBuyE3RefitError("owner refit did not receive all 30 days")
    entry, profile = _e3_entry_and_profile(ladder)
    fitted = nested._fit_boolean_candidate(
        panel,
        entry=entry,
        side="BUY",
        train_index=buy_index,
        fold_id=OWNER_FOLD_ID,
        profile=profile,
        random_seed=OWNER_SEED,
    )
    fitted = replace(fitted, learning_algorithm_fold_specific=False)
    if (
        fitted.ladder_name != OWNER_CANDIDATE
        or fitted.side != "BUY"
        or fitted.selected_profile != OWNER_PROFILE
        or fitted.training_days != training_days
        or fitted.policy is None
    ):
        raise OwnerBuyE3RefitError("frozen BUY E3 fit identity drifted")
    actions = {str(rule.action) for rule in fitted.policy.rules}
    if actions - set(FIXED_ACTIONS) or fitted.policy.default_action != "CONTROL_85N":
        raise OwnerBuyE3RefitError("BUY E3 emitted an action outside the frozen vocabulary")
    semantic = successor.audit_policy_semantics(
        fitted.policy,
        candidate_source_block=OWNER_CANDIDATE,
    )
    if semantic.uses_m2_incremental_features:
        raise OwnerBuyE3RefitError("BUY E3 artifact unexpectedly uses true M2 features")
    selected_bundle = _selected_predicate_payload(
        fitted=fitted,
        source_bundle=source_predicate_bundle,
    )
    selected_bundle_file_sha = _json_file_sha256(selected_bundle)
    policy_payload = fitted.policy.payload()
    root = Path(__file__).resolve().parents[4]
    implementation_paths = {
        "owner_refit": Path(__file__).resolve(),
        "nested_oof": Path(nested.__file__).resolve(),
        "successor_contract": Path(successor.__file__).resolve(),
        "predicate_projector": Path(predicate_view.__file__).resolve(),
        "boolean_compiler": root
        / "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_nested_oof.py",
        "live_buy_runtime": root / "strategy/boolean_cooldown_buy_e3.py",
        "owner_buy_e3_parity": root / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1.py",
        "owner_buy_e3_deployment_gate": root / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1.py",
        "maker_engine": root / "strategy/maker_engine.py",
        "live_config": root / "live/config.py",
        "live_runtime_policy": root / "live/runtime_policy.py",
        "live_main": root / "live/main.py",
    }
    implementation_sha256 = {name: file_sha256(path) for name, path in implementation_paths.items()}
    policy_artifact: dict[str, Any] = {
        "schema_version": POLICY_ARTIFACT_SCHEMA,
        "identity": IDENTITY,
        "status": "owner_refit_frozen_not_self_confirmed",
        "side": "BUY",
        "selected_candidate": OWNER_CANDIDATE,
        "selected_profile": OWNER_PROFILE,
        "random_seed": OWNER_SEED,
        "policy": policy_payload,
        "policy_semantic_sha256": canonical_sha256(policy_payload),
        "fitted_candidate_sha256": fitted.policy_sha256,
        "predicate_bundle_file_sha256": selected_bundle_file_sha,
        "predicate_bundle_canonical_sha256": selected_bundle["canonical_sha256"],
        "training_days": list(fitted.training_days),
        "training_row_sha256": fitted.training_row_sha256,
        "training_label_request_sha256": panel.learning_label_request_sha256,
        "training_label_payload_sha256": panel.learning_label_payload_sha256,
        "training_label_receipt_sha256": panel.learning_label_receipt_sha256,
        "fit_audit": dict(fitted.fit_audit),
        "feature_pool_audit": fitted.feature_pool_audit,
        "semantic_audit": asdict(semantic),
        "runtime_contract": {
            "surface": "BUY_exposure_increasing_fill_callback_only",
            "reducing_buy_unchanged": True,
            "fixed_action_is_total_cooldown": True,
            "control_is_85_seconds_times_consecutive_fill_units": True,
            "sell_owner_policy_unchanged": True,
            "fallback_action": "CONTROL_85N",
            "warmup_requires_elapsed_time_and_all_selected_states_identified": True,
        },
        "evidence_boundary": {
            "research_supported": False,
            "owner_risk_accepted": True,
            "outcome_informed_owner_override": True,
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "learning_algorithm_oof_evidence_only": True,
            "exact_artifact_oof_available": False,
            "old_oof_estimate_applies_to_exact_artifact": False,
        },
        "bindings": {
            "owner_execution_manifest_canonical_sha256": execution_manifest[
                "canonical_execution_manifest_sha256"
            ],
            "owner_execution_commit": execution_manifest["public_base_commit"],
            "owner_execution_tag": execution_manifest["annotated_tag"],
            "label_materialization_receipt_sha256": label_materialization_receipt_sha256,
            "cpp_one_shot_qualification_receipt_sha256": cpp_qualification_receipt_sha256,
            "execution_preflight_receipt_sha256": execution_preflight_receipt_sha256,
            "source_manifest_file_sha256": execution_manifest["bindings"]["source_manifest"][
                "sha256"
            ],
            "panel_manifest_file_sha256": execution_manifest["bindings"]["panel_manifest"][
                "sha256"
            ],
        },
        "implementation_sha256": implementation_sha256,
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    policy_artifact["canonical_sha256"] = document_sha256(policy_artifact, "canonical_sha256")
    policy_file_sha = _json_file_sha256(policy_artifact)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "exact_buy_e3_artifact_frozen",
        "policy_file": "policy.json",
        "policy_file_sha256": policy_file_sha,
        "policy_canonical_sha256": policy_artifact["canonical_sha256"],
        "predicate_bundle_file": "predicate_bundle.json",
        "predicate_bundle_file_sha256": selected_bundle_file_sha,
        "predicate_bundle_canonical_sha256": selected_bundle["canonical_sha256"],
        "fitted_candidate_sha256": fitted.policy_sha256,
        "label_materialization_receipt_sha256": label_materialization_receipt_sha256,
        "cpp_one_shot_qualification_receipt_sha256": cpp_qualification_receipt_sha256,
        "execution_preflight_receipt_sha256": execution_preflight_receipt_sha256,
        "implementation_sha256": implementation_sha256,
        "training_days": list(training_days),
        "training_row_sha256": fitted.training_row_sha256,
        "duration_vocabulary": list(EXPECTED_ACTIONS),
        "default_action": "CONTROL_85N",
        "exact_final_artifact_oof_available": False,
        "research_supported": False,
        "owner_risk_accepted": True,
        "permissions": policy_artifact["permissions"],
    }
    artifact_sha = canonical_sha256(manifest)
    manifest["artifact_sha256"] = artifact_sha
    return OwnerBuyE3ArtifactBundle(
        fitted_candidate=fitted,
        policy_artifact=policy_artifact,
        selected_predicate_bundle=selected_bundle,
        artifact_manifest=manifest,
        artifact_sha256=artifact_sha,
    )


def write_artifact_bundle(
    bundle: OwnerBuyE3ArtifactBundle,
    output_dir: Path,
) -> dict[str, Any]:
    destination = output_dir.expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    targets = {
        "policy": (destination / "policy.json", bundle.policy_artifact),
        "predicate_bundle": (
            destination / "predicate_bundle.json",
            bundle.selected_predicate_bundle,
        ),
        "artifact_manifest": (
            destination / "artifact_manifest.json",
            bundle.artifact_manifest,
        ),
    }
    for path, _payload in targets.values():
        if path.exists():
            raise OwnerBuyE3RefitError(f"immutable artifact already exists: {path.name}")
    hashes = {name: _atomic_json(path, payload) for name, (path, payload) in targets.items()}
    if (
        hashes["policy"] != bundle.artifact_manifest["policy_file_sha256"]
        or hashes["predicate_bundle"] != bundle.artifact_manifest["predicate_bundle_file_sha256"]
    ):
        raise OwnerBuyE3RefitError("written artifact bytes drifted")
    return {
        "identity": IDENTITY,
        "artifact_sha256": bundle.artifact_sha256,
        "files": hashes,
    }


def build_final_receipt(
    *,
    execution_manifest: Mapping[str, Any],
    artifact_manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    parity_receipt_paths: Mapping[str, Path],
    sell_54_case_receipt_path: Path,
    runtime_regression_receipt_path: Path,
    deployment_gate_receipt_path: Path,
) -> dict[str, Any]:
    expected_layers = {
        "research_compiled",
        "development_snapshot",
        "streaming_offline",
        "repeated_policy_lockstep",
    }
    if set(parity_receipt_paths) != expected_layers:
        raise OwnerBuyE3RefitError("four-layer parity receipt set is incomplete")
    try:
        artifact_manifest = json.loads(
            artifact_manifest_path.expanduser().resolve().read_text(encoding="ascii")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3RefitError("artifact manifest is unreadable") from exc
    artifact_sha256 = str(artifact_manifest.get("artifact_sha256", ""))
    if (
        _SHA_RE.fullmatch(artifact_sha256) is None
        or artifact_manifest.get("policy_file_sha256") != file_sha256(policy_path)
        or artifact_manifest.get("predicate_bundle_file_sha256")
        != file_sha256(predicate_bundle_path)
    ):
        raise OwnerBuyE3RefitError("exact artifact file binding drifted")
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 as deployment_gate,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1 as parity,
    )

    parity_receipts: dict[str, str] = {}
    for layer, path in parity_receipt_paths.items():
        receipt = parity.validate_parity_receipt(
            path,
            expected_layer=layer,
            expected_artifact_sha256=artifact_sha256,
        )
        parity_receipts[layer] = str(receipt["canonical_receipt_sha256"])
    sell_receipt = parity.validate_parity_receipt(
        sell_54_case_receipt_path,
        expected_layer=parity.SELL_OWNER_54_CASE_LAYER,
        expected_artifact_sha256=artifact_sha256,
    )
    regression = json.loads(
        runtime_regression_receipt_path.expanduser().resolve().read_text(encoding="ascii")
    )
    if (
        regression.get("schema_version") != f"{IDENTITY}.runtime_regression_test_receipt.v1"
        or regression.get("status") != "passed"
        or regression.get("failed") != 0
        or regression.get("artifact_sha256") != artifact_sha256
        or regression.get("canonical_receipt_sha256")
        != document_sha256(regression, "canonical_receipt_sha256")
    ):
        raise OwnerBuyE3RefitError("runtime regression receipt identity drifted")
    deployment_gate_receipt = deployment_gate.validate_deployment_gate_receipt(
        deployment_gate_receipt_path,
        expected_artifact_sha256=artifact_sha256,
    )
    payload: dict[str, Any] = {
        "schema_version": FINAL_RECEIPT_SCHEMA,
        "identity": IDENTITY,
        "status": "owner_artifact_and_gates_complete",
        "execution_manifest_canonical_sha256": execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "artifact_sha256": artifact_sha256,
        "artifact_manifest_file_sha256": file_sha256(artifact_manifest_path),
        "policy_file_sha256": file_sha256(policy_path),
        "predicate_bundle_file_sha256": file_sha256(predicate_bundle_path),
        "parity_receipts": dict(parity_receipts),
        "sell_54_case_receipt_sha256": sell_receipt["canonical_receipt_sha256"],
        "runtime_regression_receipt_sha256": regression["canonical_receipt_sha256"],
        "deployment_gate_receipt_sha256": deployment_gate_receipt[
            "canonical_deployment_gate_receipt_sha256"
        ],
        "research_supported": False,
        "owner_risk_accepted": True,
        "exact_artifact_oof_available": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    payload["canonical_final_receipt_sha256"] = document_sha256(
        payload, "canonical_final_receipt_sha256"
    )
    return payload


def validate_cpp_qualification_receipt(
    path: Path,
    *,
    execution_manifest: Mapping[str, Any],
    repository_root: Path,
) -> Mapping[str, Any]:
    try:
        receipt = json.loads(path.expanduser().resolve().read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3RefitError("C++ qualification receipt is unreadable") from exc
    if not isinstance(receipt, Mapping):
        raise OwnerBuyE3RefitError("C++ qualification receipt root is malformed")
    contract = receipt.get("qualification_contract")
    if not isinstance(contract, Mapping):
        raise OwnerBuyE3RefitError("C++ qualification contract is missing")
    if (
        receipt.get("identity") != "f05_cpp_one_shot_real_day_all_arm_lockstep_v26"
        or receipt.get("status") != "passed_real_day_all_opportunity_all_arm_lockstep"
        or receipt.get("canonical_receipt_sha256")
        != document_sha256(receipt, "canonical_receipt_sha256")
        or contract.get("execution_manifest_sha256")
        != execution_manifest["canonical_execution_manifest_sha256"]
        or contract.get("public_base_commit") != execution_manifest["public_base_commit"]
        or contract.get("annotated_tag") != execution_manifest["annotated_tag"]
        or contract.get("owner_buy_e3_execution_manifest_contract_sha256")
        != execution_manifest["canonical_execution_manifest_sha256"]
        or receipt.get("zero_mismatch_arm_count") != receipt.get("arm_count")
        or receipt.get("economic_values_persisted") is not False
        or receipt.get("economic_values_used_for_selection") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("action_authorized") is not False
        or receipt.get("live_authorized") is not False
    ):
        raise OwnerBuyE3RefitError("C++ qualification receipt identity drifted")
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_cpp_real_day_lockstep_v22 as qualification,
    )

    try:
        import narrowgate_cpp as cpp
    except ImportError as exc:
        raise OwnerBuyE3RefitError("C++ qualification runtime is unavailable") from exc
    expected_source_hashes = qualification._source_hashes(
        repository_root.expanduser().resolve(),
        cpp,
    )
    if contract.get("source_hashes") != expected_source_hashes:
        raise OwnerBuyE3RefitError("C++ qualification source identity drifted")
    sibling_receipts = {
        "all_panel_builder_preflight_receipt_sha256": (
            path.parent / qualification.BUILDER_PREFLIGHT_RECEIPT_NAME
        ),
        "first_opportunity_all_arm_preflight_receipt_sha256": (
            path.parent / qualification.QUICK_PREFLIGHT_RECEIPT_NAME
        ),
    }
    for field, receipt_path in sibling_receipts.items():
        try:
            sibling = json.loads(receipt_path.read_text(encoding="ascii"))
        except (OSError, json.JSONDecodeError) as exc:
            raise OwnerBuyE3RefitError(
                f"C++ qualification dependency is unreadable: {receipt_path.name}"
            ) from exc
        if (
            not isinstance(sibling, Mapping)
            or sibling.get("canonical_receipt_sha256") != contract.get(field)
            or sibling.get("canonical_receipt_sha256")
            != document_sha256(sibling, "canonical_receipt_sha256")
        ):
            raise OwnerBuyE3RefitError(f"C++ qualification dependency drifted: {receipt_path.name}")
    return dict(receipt)


def _owner_mechanics_and_adapter(
    bundle: orchestrator.FormalOfflineBundle,
    *,
    qualification: Mapping[str, Any],
    repository_root: Path,
) -> tuple[backend.OutcomeBlindMechanics, backend.CanonicalReplayAdapter]:
    execution_manifest = bundle.execution_manifest
    backend_contract = execution_manifest["backend"]
    mechanics = backend.load_outcome_blind_mechanics(
        bundle,
        expected_backend_module=str(backend_contract["module"]),
        expected_backend_function=str(backend_contract["function"]),
    )
    executor = execution_manifest.get("executor")
    mmap = executor.get("day_input_mmap") if isinstance(executor, Mapping) else None
    if not isinstance(mmap, Mapping):
        raise OwnerBuyE3RefitError("owner refit mmap executor contract is malformed")
    try:
        mmap_root = (
            resolve_portable_path(
                str(mmap.get("root")),
                root=repository_root,
            )
            .expanduser()
            .resolve()
        )
        acceleration = replay_adapter.SequentialReplayAccelerationOptions(
            day_input_cache_root=mmap_root
        )
        worker_tokens = int(executor["global_worker_tokens"])
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        raise OwnerBuyE3RefitError("owner refit executor cannot be resolved") from exc
    current_adapter = replay_adapter.build_canonical_replay_adapter(
        acceleration=acceleration,
        global_worker_tokens=worker_tokens,
        cpp_qualification_receipt_sha256=str(qualification["canonical_receipt_sha256"]),
    )
    expected_adapter_sha = qualification["qualification_contract"]["source_hashes"][
        "replay_adapter"
    ]
    if current_adapter.artifact_sha256 != expected_adapter_sha:
        raise OwnerBuyE3RefitError("owner refit adapter escaped C++ qualification")
    return mechanics, current_adapter


def _predecessor_semantic_receipt_census(
    mechanics: backend.OutcomeBlindMechanics,
    *,
    predecessor_cache_root: Path,
) -> Mapping[str, Any]:
    _request, _buy_index, days, replay_rows = _full_development_buy_request(mechanics)
    cache = replay_adapter.DayReplayCache(predecessor_cache_root)
    selected_days = set(days)
    audited: dict[str, dict[str, Any]] = {}
    for path in sorted(cache.semantic_one_shot.glob("*.json")):
        try:
            manifest = json.loads(path.read_text(encoding="ascii"))
        except (OSError, json.JSONDecodeError) as exc:
            raise OwnerBuyE3RefitError("semantic preflight manifest is unreadable") from exc
        semantic_payload = manifest.get("semantic_key")
        if not isinstance(semantic_payload, Mapping):
            continue
        if (
            str(semantic_payload.get("side", "")).upper() != "BUY"
            or semantic_payload.get("execution_manifest_sha256")
            != FORMAL_V24_EXECUTION_MANIFEST_SHA256
            or str(semantic_payload.get("utc_day", "")) not in selected_days
        ):
            continue
        key = replay_adapter.OneShotSemanticCacheKey(**dict(semantic_payload))
        expected = {
            "adapter_artifact_sha256": FORMAL_V24_BUY_ADAPTER_ARTIFACT_SHA256,
            "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
            "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
            "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
            "exact_owner_policy_sha256": mechanics.bindings.exact_owner_policy_sha256,
            "candidate_policy_sha256": FORMAL_V24_BUY_CANDIDATE_BUNDLE_SHA256,
        }
        if any(getattr(key, field) != value for field, value in expected.items()):
            raise OwnerBuyE3RefitError("semantic preflight predecessor identity drifted")
        if key.utc_day in audited:
            raise OwnerBuyE3RefitError("semantic preflight predecessor day is duplicated")
        day_rows = replay_rows.loc[replay_rows["utc_day"].astype(str) == key.utc_day]
        if key.semantic_day_input_sha256 != (
            replay_adapter._one_shot_semantic_day_input_sha256(day_rows)
        ):
            raise OwnerBuyE3RefitError("semantic preflight day input drifted")
        validated_manifest = cache._semantic_manifest(key)
        if validated_manifest is None:
            raise OwnerBuyE3RefitError("semantic preflight source disappeared")
        source_key = replay_adapter.DayReplayCacheKey(
            **dict(validated_manifest["source_cache_key"])
        )
        source_manifest = cache._manifest(source_key)
        if source_manifest is None:
            raise OwnerBuyE3RefitError("semantic preflight source receipt disappeared")
        file_bindings = source_manifest.get("files")
        if not isinstance(file_bindings, Mapping):
            raise OwnerBuyE3RefitError("semantic preflight source files are malformed")
        verified_files: dict[str, str] = {}
        source_root = cache._entry(source_key)
        for name, binding in sorted(file_bindings.items()):
            if not isinstance(binding, Mapping):
                raise OwnerBuyE3RefitError("semantic preflight frame binding is malformed")
            frame_path = source_root / str(binding.get("file", ""))
            expected_sha = str(binding.get("sha256", ""))
            if not frame_path.is_file() or file_sha256(frame_path) != expected_sha:
                raise OwnerBuyE3RefitError("semantic preflight frame bytes drifted")
            verified_files[str(name)] = expected_sha
        audited[key.utc_day] = {
            "semantic_key_sha256": key.semantic_key_sha256,
            "semantic_cache_receipt_sha256": validated_manifest["receipt_sha256"],
            "source_cache_key_sha256": source_key.cache_key_sha256,
            "source_cache_receipt_sha256": source_manifest["receipt_sha256"],
            "verified_file_sha256": verified_files,
        }
    fresh_days = tuple(day for day in days if day not in audited)
    if (
        len(audited) != EXPECTED_PREDECESSOR_LABEL_DAY_COUNT
        or len(fresh_days) != EXPECTED_FRESH_LABEL_DAY_COUNT
    ):
        raise OwnerBuyE3RefitError("semantic preflight 24+6 day contract drifted")
    return {
        "status": "predecessor_label_receipt_census_complete",
        "predecessor_execution_manifest_sha256": (FORMAL_V24_EXECUTION_MANIFEST_SHA256),
        "predecessor_day_count": len(audited),
        "fresh_day_count": len(fresh_days),
        "fresh_days": list(fresh_days),
        "day_receipts": [{"utc_day": day, **audit} for day, audit in sorted(audited.items())],
        "economic_frame_payloads_parsed": False,
        "economic_values_exposed": False,
    }


def run_owner_execution_preflight(
    execution_manifest_path: Path,
    *,
    cpp_qualification_receipt_path: Path,
    predecessor_cache_root: Path,
    output_path: Path,
    repository_root: Path,
) -> Mapping[str, Any]:
    destination = output_path.expanduser().resolve()
    if destination.exists():
        raise OwnerBuyE3RefitError("immutable owner preflight receipt already exists")
    bundle = load_owner_execution_bundle(
        execution_manifest_path,
        repository_root=repository_root,
        verify_source_bytes=True,
        require_clean_tag=True,
    )
    qualification = validate_cpp_qualification_receipt(
        cpp_qualification_receipt_path,
        execution_manifest=bundle.execution_manifest,
        repository_root=repository_root,
    )
    mechanics, current_adapter = _owner_mechanics_and_adapter(
        bundle,
        qualification=qualification,
        repository_root=repository_root,
    )
    formal = current_adapter.preflight_formal_economics(mechanics)
    if formal.get("status") != backend.MECHANICS_READY_STATUS:
        raise OwnerBuyE3RefitError("owner formal zero-economic preflight failed")
    fold_walk = formal.get("all_fold_zero_economic_contract_walk")
    owner_walk = formal.get("exact_owner_action_contract_walk")
    if (
        not isinstance(fold_walk, Mapping)
        or fold_walk.get("status") != "all_fold_zero_economic_contract_walk_complete"
        or not isinstance(owner_walk, Mapping)
        or owner_walk.get("status") != "exact_owner_action_contract_walk_complete"
        or owner_walk.get("opportunity_count") != EXPECTED_OPPORTUNITY_COUNT
        or owner_walk.get("mismatch_count") != 0
    ):
        raise OwnerBuyE3RefitError("owner zero-economic fold/action walk drifted")
    one_day = current_adapter.run_exact_owner_one_day_mechanics(mechanics)
    if one_day.get("status") != "exact_owner_one_day_mechanics_complete":
        raise OwnerBuyE3RefitError("owner one-day end-to-end mechanics failed")
    semantic_census = _predecessor_semantic_receipt_census(
        mechanics,
        predecessor_cache_root=predecessor_cache_root,
    )
    receipt: dict[str, Any] = {
        "schema_version": PREFLIGHT_RECEIPT_SCHEMA,
        "identity": IDENTITY,
        "status": "owner_execution_preflight_complete",
        "execution_manifest_canonical_sha256": bundle.execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "cpp_qualification_receipt_sha256": qualification["canonical_receipt_sha256"],
        "adapter_artifact_sha256": current_adapter.artifact_sha256,
        "formal_zero_economic_preflight": formal,
        "single_day_end_to_end_mechanics": one_day,
        "predecessor_label_receipt_census": semantic_census,
        "economic_values_exposed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_preflight_receipt_sha256"] = document_sha256(
        receipt,
        "canonical_preflight_receipt_sha256",
    )
    _atomic_json(destination, receipt)
    return receipt


def validate_owner_execution_preflight(
    path: Path,
    *,
    execution_manifest: Mapping[str, Any],
    cpp_qualification_receipt_sha256: str,
) -> Mapping[str, Any]:
    try:
        receipt = json.loads(path.expanduser().resolve().read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3RefitError("owner execution preflight is unreadable") from exc
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("schema_version") != PREFLIGHT_RECEIPT_SCHEMA
        or receipt.get("identity") != IDENTITY
        or receipt.get("status") != "owner_execution_preflight_complete"
        or receipt.get("execution_manifest_canonical_sha256")
        != execution_manifest["canonical_execution_manifest_sha256"]
        or receipt.get("cpp_qualification_receipt_sha256") != cpp_qualification_receipt_sha256
        or receipt.get("economic_values_exposed") is not False
        or receipt.get("validation_read") is not False
        or receipt.get("sealed_holdout_read") is not False
        or receipt.get("action_authorized") is not False
        or receipt.get("live_authorized") is not False
        or receipt.get("canonical_preflight_receipt_sha256")
        != document_sha256(receipt, "canonical_preflight_receipt_sha256")
    ):
        raise OwnerBuyE3RefitError("owner execution preflight identity drifted")
    return dict(receipt)


def run_owner_buy_e3_refit(
    execution_manifest_path: Path,
    *,
    cpp_qualification_receipt_path: Path,
    execution_preflight_receipt_path: Path,
    predecessor_cache_root: Path,
    output_dir: Path,
    repository_root: Path,
) -> Mapping[str, Any]:
    destination = output_dir.expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    artifact_dir = destination / "artifact"
    immutable_targets = (
        destination / "label_materialization_receipt.json",
        destination / "refit_run_receipt.json",
        artifact_dir / "policy.json",
        artifact_dir / "predicate_bundle.json",
        artifact_dir / "artifact_manifest.json",
    )
    if any(path.exists() for path in immutable_targets):
        raise OwnerBuyE3RefitError("immutable owner refit output already exists")
    bundle = load_owner_execution_bundle(
        execution_manifest_path,
        repository_root=repository_root,
        verify_source_bytes=True,
        require_clean_tag=True,
    )
    execution_manifest = bundle.execution_manifest
    qualification = validate_cpp_qualification_receipt(
        cpp_qualification_receipt_path,
        execution_manifest=execution_manifest,
        repository_root=repository_root,
    )
    preflight = validate_owner_execution_preflight(
        execution_preflight_receipt_path,
        execution_manifest=execution_manifest,
        cpp_qualification_receipt_sha256=str(qualification["canonical_receipt_sha256"]),
    )
    mechanics, current_adapter = _owner_mechanics_and_adapter(
        bundle,
        qualification=qualification,
        repository_root=repository_root,
    )
    materialization = materialize_full_development_buy_labels(
        mechanics,
        predecessor_cache_root=predecessor_cache_root,
        fresh_adapter=current_adapter,
        owner_execution_manifest_sha256=execution_manifest["canonical_execution_manifest_sha256"],
    )
    _atomic_json(
        destination / "label_materialization_receipt.json",
        materialization.receipt,
    )
    materialized_panel, _batch = bind_materialized_full_development_labels(
        mechanics.panel,
        materialization,
    )
    ladder, _continuous = current_adapter.build_search_contract(mechanics)
    if mechanics.predicate_bundle_path is None:
        raise OwnerBuyE3RefitError("owner refit lacks its frozen 2025 predicate bundle")
    source_predicates = predicate_view.load_frozen_predicate_bundle(
        mechanics.predicate_bundle_path,
        expected_file_sha256=mechanics.bindings.exact_owner_predicate_bundle_sha256,
    )
    artifact = fit_owner_buy_e3(
        materialized_panel,
        ladder=ladder,
        source_predicate_bundle=source_predicates,
        execution_manifest=execution_manifest,
        label_materialization_receipt_sha256=materialization.receipt_sha256,
        cpp_qualification_receipt_sha256=str(qualification["canonical_receipt_sha256"]),
        execution_preflight_receipt_sha256=str(preflight["canonical_preflight_receipt_sha256"]),
    )
    written = write_artifact_bundle(artifact, artifact_dir)
    run_receipt: dict[str, Any] = {
        "schema_version": REFIT_RUN_RECEIPT_SCHEMA,
        "identity": IDENTITY,
        "status": "owner_buy_e3_full_development_refit_complete",
        "execution_manifest_canonical_sha256": execution_manifest[
            "canonical_execution_manifest_sha256"
        ],
        "cpp_qualification_receipt_sha256": qualification["canonical_receipt_sha256"],
        "label_materialization_receipt_sha256": materialization.receipt_sha256,
        "execution_preflight_receipt_sha256": preflight["canonical_preflight_receipt_sha256"],
        "artifact_sha256": artifact.artifact_sha256,
        "artifact_files": written["files"],
        "full_development_refit_count": 1,
        "outer_fold_policy_selected": False,
        "outer_fold_rules_merged": False,
        "literal_edited": False,
        "candidate_substituted": False,
        "research_supported": False,
        "owner_risk_accepted": True,
        "exact_artifact_oof_available": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    run_receipt["canonical_refit_run_receipt_sha256"] = document_sha256(
        run_receipt,
        "canonical_refit_run_receipt_sha256",
    )
    _atomic_json(destination / "refit_run_receipt.json", run_receipt)
    return run_receipt


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> str:
    data = _json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(data).hexdigest()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--cpp-qualification-receipt", type=Path, required=True)
    parser.add_argument("--execution-preflight-receipt", type=Path, required=True)
    parser.add_argument("--predecessor-cache-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path(__file__).resolve().parents[4],
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_owner_buy_e3_refit(
        args.manifest,
        cpp_qualification_receipt_path=args.cpp_qualification_receipt,
        execution_preflight_receipt_path=args.execution_preflight_receipt,
        predecessor_cache_root=args.predecessor_cache_root,
        output_dir=args.output_dir,
        repository_root=args.repository_root,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


__all__ = [
    "EXECUTION_MANIFEST_SCHEMA",
    "EXPECTED_ACTIONS",
    "FINAL_RECEIPT_SCHEMA",
    "FORMAL_V24_BUY_ADAPTER_ARTIFACT_SHA256",
    "FORMAL_V24_BUY_CANDIDATE_BUNDLE_SHA256",
    "FORMAL_V24_EXECUTION_MANIFEST_SHA256",
    "FullDevelopmentLabelMaterialization",
    "IDENTITY",
    "LABEL_MATERIALIZATION_SCHEMA",
    "LABEL_PROVIDER_IDENTITY",
    "OWNER_CANDIDATE",
    "OWNER_FOLD_ID",
    "OWNER_PROFILE",
    "OWNER_SEED",
    "PREFLIGHT_RECEIPT_SCHEMA",
    "REFIT_RUN_RECEIPT_SCHEMA",
    "OwnerBuyE3ArtifactBundle",
    "OwnerBuyE3RefitError",
    "bind_execution_manifest",
    "bind_full_development_labels",
    "bind_materialized_full_development_labels",
    "build_execution_manifest_payload",
    "build_final_receipt",
    "execution_contract",
    "fit_owner_buy_e3",
    "load_owner_execution_bundle",
    "materialize_full_development_buy_labels",
    "run_owner_buy_e3_refit",
    "run_owner_execution_preflight",
    "validate_cpp_qualification_receipt",
    "validate_owner_execution_preflight",
    "validate_execution_manifest",
    "write_artifact_bundle",
]


if __name__ == "__main__":
    raise SystemExit(main())
