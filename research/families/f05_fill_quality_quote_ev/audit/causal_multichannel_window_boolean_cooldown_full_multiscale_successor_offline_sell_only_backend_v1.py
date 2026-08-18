"""Formal SELL-only backend for the F05 full-multiscale successor.

This backend deliberately has no completed-side or cross-execution strategy
cache bridge.  A SELL request can resolve only to its exact v27 cache key or to
fresh computation under the v27 execution manifest.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as base_orchestrator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_sell_only_orchestrator_v1 as orchestrator,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

IDENTITY = f"{offline.IDENTITY}.offline_sell_only_backend_v1"
FORMAL_RESULT_SCHEMA = orchestrator.FORMAL_RESULT_SCHEMA
TARGET_SOURCE_COVERAGE_IDENTITY = (
    "f05_formal_v27_sell_only_target_request_source_coverage_v1"
)


def _load_canonical_replay_adapter(
    bundle: base_orchestrator.FormalOfflineBundle,
) -> backend.CanonicalReplayAdapter:
    """Build the fixed replay adapter without a predecessor cache transform."""

    executor = bundle.execution_manifest.get("executor")
    if executor != orchestrator.formal_executor_contract():
        raise backend.OfflineRepeatedPolicyBackendError(
            "formal SELL-only executor contract drifted"
        )
    orchestrator._validate_cpp_qualification_reuse_receipt(bundle)
    try:
        module = importlib.import_module(backend.CANONICAL_REPLAY_ADAPTER_MODULE)
    except ModuleNotFoundError as exc:
        raise backend.OfflineRepeatedPolicyBackendError(
            "fixed historical replay adapter is not implemented"
        ) from exc
    factory = getattr(module, backend.CANONICAL_REPLAY_ADAPTER_FACTORY, None)
    if not callable(factory):
        raise backend.OfflineRepeatedPolicyBackendError(
            "fixed replay adapter factory is unavailable"
        )
    acceleration_type = getattr(module, "SequentialReplayAccelerationOptions", None)
    topology_type = getattr(module, "OneShotProcessTopology", None)
    if (
        not isinstance(acceleration_type, type)
        or not isinstance(topology_type, type)
        or getattr(module, "EXECUTOR_ACCELERATION_IDENTITY", None)
        != executor.get("replay_adapter_executor_identity")
        or getattr(module, "DAY_INPUT_CACHE_IDENTITY", None)
        != base_orchestrator.EXECUTOR_DAY_INPUT_CACHE_IDENTITY
        or topology_type().payload()
        != base_orchestrator.EXECUTOR_ONE_SHOT_TOPOLOGY
        or getattr(module, "DAY_INPUT_MATERIALIZATION_WORKERS", None)
        != base_orchestrator.EXECUTOR_DAY_INPUT_MATERIALIZATION_WORKERS
    ):
        raise backend.OfflineRepeatedPolicyBackendError(
            "fixed replay adapter acceleration contract drifted"
        )
    mmap = executor.get("day_input_mmap")
    if not isinstance(mmap, Mapping):
        raise backend.OfflineRepeatedPolicyBackendError(
            "formal SELL-only mmap contract is malformed"
        )
    try:
        mmap_root = resolve_portable_path(
            str(mmap.get("root")),
            root=bundle.repository_root,
        ).expanduser().resolve()
        acceleration = acceleration_type(day_input_cache_root=mmap_root)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise backend.OfflineRepeatedPolicyBackendError(
            "formal SELL-only mmap contract cannot be resolved"
        ) from exc
    module_path_value = getattr(module, "__file__", None)
    if not module_path_value:
        raise backend.OfflineRepeatedPolicyBackendError(
            "fixed replay adapter has no source artifact"
        )
    module_path = Path(module_path_value).expanduser().resolve()
    adapter = backend._validate_adapter_shape(
        factory(
            acceleration=acceleration,
            global_worker_tokens=int(executor["global_worker_tokens"]),
            cpp_qualification_receipt_sha256=(
                orchestrator.V26_CPP_QUALIFICATION_CANONICAL_SHA256
            ),
            completed_side_resume=None,
            completed_side_resume_receipt_sha256=None,
        )
    )
    if type(adapter).__module__ != backend.CANONICAL_REPLAY_ADAPTER_MODULE:
        raise backend.OfflineRepeatedPolicyBackendError(
            "custom replay adapter implementation is forbidden"
        )
    if adapter.artifact_sha256 != backend._file_sha256(module_path):
        raise backend.OfflineRepeatedPolicyBackendError(
            "fixed replay adapter source hash drifted"
        )
    _target_request_source_coverage(adapter)
    return adapter


def _target_request_source_coverage(
    adapter: backend.CanonicalReplayAdapter,
) -> dict[str, Any]:
    """Freeze the only two legal resolution paths before economics are read."""

    summary = adapter.completed_side_resume_summary(require_complete=False)
    if summary is not None:
        raise backend.OfflineRepeatedPolicyBackendError(
            "formal-v27 forbids completed-side and cross-execution cache resume"
        )
    return {
        "identity": TARGET_SOURCE_COVERAGE_IDENTITY,
        "status": "sell_only_exact_same_execution_or_fresh_compute",
        "formal_sides": list(orchestrator.FORMAL_SIDES),
        "allowed_resolution_paths": [
            "exact_v27_cache_key",
            "fresh_v27_compute",
        ],
        "cross_execution_source_count": 0,
        "predecessor_key_transform_available": False,
        "completed_side_resume_available": False,
        "strategy_dependent_request_sha_rewrite_allowed": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _adapter_preflight(
    mechanics: backend.OutcomeBlindMechanics,
    adapter: backend.CanonicalReplayAdapter,
) -> dict[str, Any]:
    result = dict(backend._preflight_adapter(mechanics, adapter))
    result["sell_only_target_request_source_coverage"] = (
        _target_request_source_coverage(adapter)
    )
    result["formal_execution_projection"] = {
        "mechanics_contract_walk_sides": ["BUY", "SELL"],
        "economic_execution_sides": list(orchestrator.FORMAL_SIDES),
        "buy_economics_read_by_v27": False,
    }
    return result


def _component_fields() -> dict[str, Any]:
    return {
        "formal_sides": list(orchestrator.FORMAL_SIDES),
        "component_scope": "sell_only_learning_algorithm_oof",
        "composition_contract": orchestrator.formal_composition_contract(),
        "cross_execution_strategy_cache_reuse_used": False,
    }


def _blocked_bundle_result(
    bundle: base_orchestrator.FormalOfflineBundle,
    *,
    blocker: str,
    status: str = backend.BLOCKED_STATUS,
    missing_canonical_fields: Sequence[str] = (),
) -> dict[str, Any]:
    result = backend._blocked_bundle_result(
        bundle,
        blocker=blocker,
        status=status,
        missing_canonical_fields=missing_canonical_fields,
    )
    result.update(
        {
            "schema_version": FORMAL_RESULT_SCHEMA,
            "identity": IDENTITY,
            **_component_fields(),
        }
    )
    return result


def _blocked_result(
    mechanics: backend.OutcomeBlindMechanics,
    *,
    blocker: str,
    status: str = backend.BLOCKED_STATUS,
    missing_canonical_fields: Sequence[str] = (),
) -> dict[str, Any]:
    result = backend._blocked_result(
        mechanics,
        blocker=blocker,
        status=status,
        missing_canonical_fields=missing_canonical_fields,
    )
    result.update(
        {
            "schema_version": FORMAL_RESULT_SCHEMA,
            "identity": IDENTITY,
            **_component_fields(),
        }
    )
    return result


def _completed_result(
    mechanics: backend.OutcomeBlindMechanics,
    *,
    adapter: backend.CanonicalReplayAdapter,
    result: nested.NestedOofExecutionResult,
    label_receipts: Sequence[Mapping[str, Any]],
    sequential_receipts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    target_coverage = _target_request_source_coverage(adapter)
    return {
        "schema_version": FORMAL_RESULT_SCHEMA,
        "identity": IDENTITY,
        "status": "sell_learning_algorithm_nested_oof_complete_no_action_or_live_authority",
        "execution_manifest_sha256": mechanics.bindings.execution_manifest_sha256,
        "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
        "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
        "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
        "nested_fold_manifest_sha256": mechanics.bindings.nested_fold_manifest_sha256,
        "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
        "exact_owner_policy_sha256": mechanics.bindings.exact_owner_policy_sha256,
        "exact_owner_predicate_bundle_sha256": (
            mechanics.bindings.exact_owner_predicate_bundle_sha256
        ),
        "exact_owner_private_config_sha256": (
            mechanics.bindings.exact_owner_private_config_sha256
        ),
        "canonical_replay_adapter_identity": adapter.identity,
        "canonical_replay_adapter_sha256": adapter.artifact_sha256,
        "sell_only_target_request_source_coverage": target_coverage,
        "label_replay_receipts": list(label_receipts),
        "sequential_replay_receipts": list(sequential_receipts),
        "nested_oof_report": result.report(),
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "economic_outcomes_read": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        **_component_fields(),
    }


def run_canonical_offline_sell_economics(
    execution_manifest_path: Path,
) -> Mapping[str, Any]:
    """Run only SELL OOF after the complete zero-economic admission chain."""

    if not isinstance(execution_manifest_path, Path):
        raise TypeError("formal SELL backend accepts only an execution-manifest Path")
    bundle = orchestrator.load_formal_sell_only_bundle(execution_manifest_path)
    try:
        adapter = _load_canonical_replay_adapter(bundle)
    except backend.OfflineRepeatedPolicyBackendError as exc:
        raise backend.OfflineRepeatedPolicyBackendIncomplete(
            _blocked_bundle_result(bundle, blocker=str(exc))
        ) from exc
    schema_preflight = backend._preflight_bound_panel_schema(bundle, adapter)
    if schema_preflight["status"] != backend.FORMAL_PANEL_SCHEMA_READY_STATUS:
        missing = tuple(
            str(value) for value in schema_preflight["missing_canonical_fields"]
        )
        blocked = _blocked_bundle_result(
            bundle,
            blocker=",".join(missing),
            status=backend.CANONICAL_FIELDS_BLOCKED_STATUS,
            missing_canonical_fields=missing,
        )
        blocked["adapter_preflight"] = schema_preflight
        raise backend.OfflineRepeatedPolicyBackendIncomplete(blocked)
    mechanics = backend.load_outcome_blind_mechanics(
        bundle,
        expected_backend_module=orchestrator.CANONICAL_BACKEND_MODULE,
        expected_backend_function=orchestrator.CANONICAL_BACKEND_FUNCTION,
    )
    preflight = _adapter_preflight(mechanics, adapter)
    if preflight["status"] != backend.MECHANICS_READY_STATUS:
        missing = tuple(str(value) for value in preflight["missing_canonical_fields"])
        blocked = _blocked_result(
            mechanics,
            blocker=",".join(missing),
            status=(
                backend.CANONICAL_FIELDS_BLOCKED_STATUS
                if preflight["status"] == backend.CANONICAL_FIELDS_BLOCKED_STATUS
                else backend.BLOCKED_STATUS
            ),
            missing_canonical_fields=missing,
        )
        blocked["adapter_preflight"] = preflight
        raise backend.OfflineRepeatedPolicyBackendIncomplete(blocked)
    ladder, continuous = adapter.build_search_contract(mechanics)
    provider = backend.CanonicalFoldScopedLabelProvider(mechanics, adapter)
    evaluator = backend.CanonicalSequentialEvaluator(mechanics, adapter)
    result = nested.run_nested_chronological_oof(
        mechanics.panel,
        fold_manifest=mechanics.fold_manifest,
        ladder=ladder,
        continuous=continuous,
        evaluator=evaluator,
        label_provider=provider,
        config=nested.NestedOofConfig(
            sides=orchestrator.FORMAL_SIDES,
            panel_role=offline.PANEL_ROLE,
            earliest_eligible_day=None,
        ),
    )
    return _completed_result(
        mechanics,
        adapter=adapter,
        result=result,
        label_receipts=provider.receipts,
        sequential_receipts=evaluator.receipts,
    )


def preflight_canonical_offline_sell_economics(
    execution_manifest_path: Path,
) -> Mapping[str, Any]:
    """Validate SELL-only execution and cache resolution without economics."""

    if not isinstance(execution_manifest_path, Path):
        raise TypeError("formal SELL preflight accepts only an execution-manifest Path")
    bundle = orchestrator.load_formal_sell_only_bundle(execution_manifest_path)
    try:
        adapter = _load_canonical_replay_adapter(bundle)
    except backend.OfflineRepeatedPolicyBackendError as exc:
        return _blocked_bundle_result(bundle, blocker=str(exc))
    schema_preflight = backend._preflight_bound_panel_schema(bundle, adapter)
    if schema_preflight["status"] != backend.FORMAL_PANEL_SCHEMA_READY_STATUS:
        missing = tuple(
            str(value) for value in schema_preflight["missing_canonical_fields"]
        )
        result = _blocked_bundle_result(
            bundle,
            blocker=",".join(missing),
            status=backend.CANONICAL_FIELDS_BLOCKED_STATUS,
            missing_canonical_fields=missing,
        )
        result["adapter_preflight"] = schema_preflight
        return result
    mechanics = backend.load_outcome_blind_mechanics(
        bundle,
        expected_backend_module=orchestrator.CANONICAL_BACKEND_MODULE,
        expected_backend_function=orchestrator.CANONICAL_BACKEND_FUNCTION,
    )
    adapter_preflight = _adapter_preflight(mechanics, adapter)
    if adapter_preflight["status"] != backend.MECHANICS_READY_STATUS:
        missing = tuple(
            str(value) for value in adapter_preflight["missing_canonical_fields"]
        )
        result = _blocked_result(
            mechanics,
            blocker=",".join(missing),
            status=(
                backend.CANONICAL_FIELDS_BLOCKED_STATUS
                if adapter_preflight["status"]
                == backend.CANONICAL_FIELDS_BLOCKED_STATUS
                else backend.BLOCKED_STATUS
            ),
            missing_canonical_fields=missing,
        )
        result["adapter_preflight"] = adapter_preflight
        return result
    return {
        "schema_version": FORMAL_RESULT_SCHEMA,
        "identity": IDENTITY,
        "status": backend.MECHANICS_READY_STATUS,
        "execution_manifest_sha256": mechanics.bindings.execution_manifest_sha256,
        "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
        "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
        "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
        "nested_fold_manifest_sha256": mechanics.bindings.nested_fold_manifest_sha256,
        "exact_owner_policy_sha256": mechanics.bindings.exact_owner_policy_sha256,
        "adapter_preflight": adapter_preflight,
        "sell_only_target_request_source_coverage": (
            _target_request_source_coverage(adapter)
        ),
        "repeated_sequential_policy": False,
        "one_shot_effect_aggregation_used": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": {
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        **_component_fields(),
    }


__all__ = [
    "FORMAL_RESULT_SCHEMA",
    "IDENTITY",
    "TARGET_SOURCE_COVERAGE_IDENTITY",
    "preflight_canonical_offline_sell_economics",
    "run_canonical_offline_sell_economics",
]
