#!/usr/bin/env python3
"""Restart-aware adapter for the owner Boolean cooldown policy.

The adapter reuses the shared F03 calendar, accounting, maintenance-drain, and
checkpoint substrate.  Online epochs may cross UTC midnight without resetting
orders, cooldowns, or EMA state.  A planned process restart drains orders,
keeps cash/inventory/economic-campaign state, resets process-local state, and
rebuilds the owner M2 EMA state from past-only observations before quoting.

This remains historical exchange-time, modeled-queue sensitivity evidence.
It cannot claim receive-time transport, strict queue, action, or live authority.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from models.replay.narrowgate_continuous_tick_adapter import (
    AdapterArmBinding,
    AuthoritativeReplayEpoch,
    NarrowGateContinuousTickReplayAdapter,
    ReplayDayInput,
    compile_authoritative_epochs,
)
from models.replay.replay_state_checkpoint import (
    RESTART_RESET_FIELDS,
    ContinuousReplayState,
)
from models.replay.restart_aware_continuous_ab import canonical_sha256, sha256_file
from models.replay.restart_aware_continuous_execution import ContinuousOperation
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_2 as f03_framework,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_v1 as daily_full_path,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_native_observation_cache import (
    NativeObservationCacheError,
    open_admitted_observation_cache,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_replay_emitter import (
    CooldownV2ReplayEmitter,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_runtime_policy import (
    load_runtime_policy_evaluator,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_restart_aware_v1"
SCHEMA_VERSION = f"{IDENTITY}.v1"
CONTROL_ARM = "control"
CANDIDATE_ARM = "candidate"
ARMS = (CONTROL_ARM, CANDIDATE_ARM)
DEFAULT_DAILY_PANEL = daily_full_path.DEFAULT_OUTPUT / "panel"
DEFAULT_FRAMEWORK_PLAN = f03_framework.DEFAULT_OUTPUT_ROOT / f03_framework.PLAN_FILENAME
DEFAULT_POLICY = daily_full_path.DEFAULT_POLICY
DEFAULT_POLICY_SHA256 = daily_full_path.DEFAULT_POLICY_SHA256
DEFAULT_OBSERVATION_CACHE = daily_full_path.DEFAULT_OBSERVATION_CACHE
MAX_OWNER_EMA_HALF_LIFE_MS = 256_000


class OwnerRestartAwareError(RuntimeError):
    """Raised before an ambiguous continuous owner-policy run can advance."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise OwnerRestartAwareError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerRestartAwareError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise OwnerRestartAwareError(f"{role} must be a JSON object")
    return payload


def _validate_daily_panel(
    panel_root: Path,
    *,
    expected_policy_sha256: str,
) -> dict[str, Any] | None:
    root = Path(panel_root).expanduser().resolve()
    if not root.is_dir():
        return None
    report_path = root / "report.json"
    manifest_path = root / "manifest.json"
    marker_path = root / daily_full_path.PANEL_SUCCESS
    if not report_path.is_file() or not manifest_path.is_file() or not marker_path.is_file():
        raise OwnerRestartAwareError("daily full-path panel is partially admitted")
    if marker_path.read_text(encoding="ascii").strip() != sha256_file(manifest_path):
        raise OwnerRestartAwareError("daily full-path panel admission marker drifted")
    manifest = _load_json(manifest_path, role="daily full-path manifest")
    report = _load_json(report_path, role="daily full-path report")
    if manifest.get("identity") != daily_full_path.IDENTITY:
        raise OwnerRestartAwareError("daily full-path manifest identity drifted")
    file_bindings = {
        str(row.get("relative_path")): str(row.get("sha256"))
        for row in manifest.get("files", ())
        if isinstance(row, Mapping)
    }
    if file_bindings.get("report.json") != sha256_file(report_path):
        raise OwnerRestartAwareError("daily full-path report is not manifest-bound")
    if (manifest.get("policy") or {}).get("sha256") != expected_policy_sha256:
        raise OwnerRestartAwareError("daily full-path policy identity drifted")
    panel = report.get("panel") or {}
    permissions = report.get("permissions") or {}
    if (
        report.get("identity") != daily_full_path.IDENTITY
        or report.get("status")
        != "owner_repeated_policy_historical_full_path_economics_complete"
        or panel.get("days") != 50
        or panel.get("prefix_days") != 40
        or panel.get("added_days") != 10
        or panel.get("daily_fresh_start") is not True
        or panel.get("continuous_replay") is not False
    ):
        raise OwnerRestartAwareError("daily full-path prerequisite semantics drifted")
    if any(
        permissions.get(name) is not False
        for name in (
            "research_supported",
            "strict_native_queue_authority",
            "receive_time_transport_authority",
            "continuous_replay_authority",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise OwnerRestartAwareError("daily full-path prerequisite exceeded its authority")
    return {
        "path": str(root),
        "report_sha256": sha256_file(report_path),
        "manifest_sha256": sha256_file(manifest_path),
        "policy_sha256": expected_policy_sha256,
        "days": 50,
        "daily_fresh_start": True,
        "continuous_replay": False,
    }


def _load_framework_binding(plan_path: Path) -> dict[str, Any]:
    resolved = Path(plan_path).expanduser().resolve()
    payload = _load_json(resolved, role="F03 continuous framework plan")
    marker = resolved.parent / f03_framework.PLAN_SUCCESS
    if not marker.is_file() or marker.read_text(encoding="ascii").strip() != sha256_file(
        resolved
    ):
        raise OwnerRestartAwareError("F03 framework plan admission marker drifted")
    if payload.get("schema_version") != f03_framework.PLAN_SCHEMA_VERSION:
        raise OwnerRestartAwareError("F03 framework plan schema drifted")
    expected_identity = str(payload.pop("plan_identity_sha256", ""))
    if canonical_sha256(payload) != expected_identity:
        raise OwnerRestartAwareError("F03 framework plan canonical identity drifted")
    payload["plan_identity_sha256"] = expected_identity
    continuous = payload.get("continuous_plan") or {}
    comparison = payload.get("comparison") or {}
    if (
        continuous.get("same_restart_manifest_both_arms") is not True
        or continuous.get("arm_economic_state_isolated") is not True
        or continuous.get("utc_midnight_policy")
        != "accounting_only_no_flatten_no_state_reset"
        or continuous.get("gap_policy")
        != "clear_orders_queue_then_past_only_warmup"
        or comparison.get("same_restart_schedule") is not True
        or comparison.get("arm_mutable_state_isolated") is not True
        or comparison.get("utc_midnight_state_reset") is not False
        or comparison.get("daily_forced_flat") is not False
    ):
        raise OwnerRestartAwareError("F03 framework continuity invariants drifted")
    operations = tuple(ContinuousOperation(**row) for row in payload.get("operations", ()))
    if not operations:
        raise OwnerRestartAwareError("F03 framework operation tape is empty")
    for operation in operations:
        operation.validate()
    if canonical_sha256([asdict(row) for row in operations]) != payload.get(
        "operation_tape_sha256"
    ):
        raise OwnerRestartAwareError("F03 framework operation tape hash drifted")
    drains = [
        row.end_ts_ms - row.start_ts_ms for row in operations if row.kind == "cancel_drain"
    ]
    if not drains or min(drains) <= 0:
        raise OwnerRestartAwareError("F03 framework lacks an observable cancel drain")
    epochs = compile_authoritative_epochs(operations, panel_cancel_drain_ms=max(drains))
    midnight_operations = tuple(row for row in operations if row.kind == "utc_accounting")
    embedded_midnights = {
        boundary
        for epoch in epochs
        for boundary in epoch.utc_boundaries_ts_ms
        if epoch.start_ts_ms < boundary <= epoch.gap_end_ts_ms
    }
    if any(row.start_ts_ms not in embedded_midnights for row in midnight_operations):
        raise OwnerRestartAwareError("UTC accounting boundary split an online engine epoch")
    warmup_ms = tuple(
        epoch.start_ts_ms - epoch.warmup_lookback_start_ts_ms
        for epoch in epochs
        if epoch.warmup_lookback_start_ts_ms < epoch.start_ts_ms
    )
    if warmup_ms and min(warmup_ms) < MAX_OWNER_EMA_HALF_LIFE_MS:
        raise OwnerRestartAwareError("restart warmup is shorter than the owner EMA support")
    return {
        "path": str(resolved),
        "file_sha256": sha256_file(resolved),
        "plan_identity_sha256": expected_identity,
        "restart_timeline_sha256": str(continuous.get("restart_timeline_sha256", "")),
        "operation_tape_sha256": str(payload["operation_tape_sha256"]),
        "operation_count": len(operations),
        "authoritative_epoch_count": len(epochs),
        "utc_accounting_boundary_count": len(midnight_operations),
        "utc_boundaries_embedded_inside_epochs": True,
        "minimum_restart_warmup_ms": min(warmup_ms) if warmup_ms else None,
        "same_restart_manifest_both_arms": True,
        "arm_economic_state_isolated": True,
        "operations": operations,
    }


def prepare_preflight(
    *,
    daily_panel: Path = DEFAULT_DAILY_PANEL,
    framework_plan: Path = DEFAULT_FRAMEWORK_PLAN,
    policy_path: Path = DEFAULT_POLICY,
    expected_policy_sha256: str = DEFAULT_POLICY_SHA256,
    runtime_loader: Callable[..., Any] = load_runtime_policy_evaluator,
) -> dict[str, Any]:
    """Bind prerequisites without reading or producing continuous economics."""

    resolved_policy = Path(policy_path).expanduser().resolve()
    blockers: list[str] = []
    if not resolved_policy.is_file():
        blockers.append("owner_policy_artifact_missing")
        policy_binding: dict[str, Any] | None = None
    elif sha256_file(resolved_policy) != expected_policy_sha256:
        raise OwnerRestartAwareError("owner policy SHA256 drifted")
    else:
        evaluator = runtime_loader(
            resolved_policy,
            expected_policy_sha256=expected_policy_sha256,
        )
        binding_error = getattr(evaluator, "_binding_error", None)
        if binding_error:
            raise OwnerRestartAwareError(
                f"owner runtime policy failed binding: {binding_error}"
            )
        if str(getattr(evaluator, "policy_sha256", "")) != expected_policy_sha256:
            raise OwnerRestartAwareError("owner runtime policy identity drifted")
        policy_binding = {
            "path": str(resolved_policy),
            "sha256": expected_policy_sha256,
            "runtime_binding_valid": True,
        }

    daily_binding = _validate_daily_panel(
        daily_panel,
        expected_policy_sha256=expected_policy_sha256,
    )
    if daily_binding is None:
        blockers.append("daily_fresh_start_50d_full_path_panel_not_admitted")

    framework_binding: dict[str, Any] | None = None
    resolved_framework = Path(framework_plan).expanduser().resolve()
    if not resolved_framework.is_file():
        blockers.append("shared_restart_aware_framework_plan_missing")
    else:
        framework_binding = _load_framework_binding(resolved_framework)
        framework_binding = {
            key: value
            for key, value in framework_binding.items()
            if key != "operations"
        }

    payload = {
        "schema_version": f"{SCHEMA_VERSION}.preflight",
        "identity": IDENTITY,
        "daily_fresh_start_prerequisite": daily_binding,
        "framework": framework_binding,
        "owner_policy": policy_binding,
        "state_contract": {
            "utc_midnight": {
                "cash_preserved": True,
                "inventory_preserved": True,
                "economic_campaign_preserved": True,
                "cooldown_preserved": True,
                "ema_preserved": True,
                "orders_and_queue_preserved": True,
                "accounting_slice_only": True,
            },
            "planned_maintenance": {
                "new_quotes_stopped": True,
                "orders_require_terminal_ack_fill_reject_or_expiry": True,
                "inventory_flattened": False,
                "cash_inventory_entry_campaign_preserved": True,
                "process_local_cooldown_and_ema_reset": True,
                "past_only_warmup_required_before_requote": True,
                "runtime_reset_fields": list(RESTART_RESET_FIELDS),
            },
        },
        "candidate_missing_m2_policy": "exact_control_fallback_for_entire_epoch",
        "engine": "python_owner_policy_over_shared_restart_substrate",
        "mechanics_execution_eligible": not blockers,
        "blockers": blockers,
        "remaining_completion_blockers": [
            "paired_continuous_economic_finalizer_not_bound",
            "python_cpp_streaming_policy_parity_not_available",
            "receive_time_transport_not_available",
            "strict_exchange_queue_authority_not_available",
        ],
        "permissions": {
            "daily_fresh_start_prerequisite_only": True,
            "continuous_economic_result_authorized": False,
            "strict_queue_authority": False,
            "receive_time_transport_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "economic_outcomes_read": False,
    }
    payload["preflight_identity_sha256"] = canonical_sha256(payload)
    return payload


def validate_preflight(payload: Mapping[str, Any], *, require_eligible: bool) -> None:
    normalized = dict(payload)
    expected = str(normalized.pop("preflight_identity_sha256", ""))
    if canonical_sha256(normalized) != expected:
        raise OwnerRestartAwareError("owner restart-aware preflight identity drifted")
    if normalized.get("identity") != IDENTITY:
        raise OwnerRestartAwareError("owner restart-aware preflight identity is invalid")
    permissions = normalized.get("permissions") or {}
    if any(
        permissions.get(name) is not False
        for name in (
            "continuous_economic_result_authorized",
            "strict_queue_authority",
            "receive_time_transport_authority",
            "research_supported",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise OwnerRestartAwareError("owner restart-aware preflight exceeded authority")
    if require_eligible and normalized.get("mechanics_execution_eligible") is not True:
        raise OwnerRestartAwareError(
            "owner restart-aware mechanics blocked: "
            + ",".join(str(value) for value in normalized.get("blockers", ()))
        )
    if normalized.get("mechanics_execution_eligible") is True and any(
        normalized.get(name) is None
        for name in (
            "daily_fresh_start_prerequisite",
            "framework",
            "owner_policy",
        )
    ):
        raise OwnerRestartAwareError("eligible preflight lacks a required binding")


class SharedReplayDayInputProvider(Protocol):
    def load_day(self, *, day: str) -> ReplayDayInput: ...


class _ArmSharedReplayInputAdapter:
    def __init__(self, provider: SharedReplayDayInputProvider) -> None:
        self._provider = provider

    def load_day(self, *, arm_id: str, day: str) -> ReplayDayInput:
        if arm_id not in ARMS:
            raise OwnerRestartAwareError(f"unknown continuous arm: {arm_id}")
        row = self._provider.load_day(day=day)
        row.validate()
        if row.exact_queue_authority or row.exact_lifecycle_authority:
            raise OwnerRestartAwareError(
                "owner modeled-queue adapter cannot inherit exact source authority"
            )
        return row


@dataclass(frozen=True, slots=True)
class OwnerEpochFeatureInput:
    observation_factory: Callable[[], Iterator[CausalWindowObservation]]
    warmup_cutoff_ts_ns: int
    warmup_identity: str
    identity_hashes: Mapping[str, str]
    source_cursor_prefixes: Mapping[str, str]
    support_binding: Mapping[str, Any]

    def validate(self) -> None:
        if self.warmup_cutoff_ts_ns <= 0 or not self.warmup_identity:
            raise OwnerRestartAwareError("owner epoch warmup identity is incomplete")
        if set(self.source_cursor_prefixes) != {"market", "depth", "trade"}:
            raise OwnerRestartAwareError("owner epoch source cursors are incomplete")
        required_hashes = {
            "config_sha256",
            "code_sha256",
            "model_sha256",
            "p3_sha256",
            "feature_dag_sha256",
            "execution_abi_sha256",
            "baseline_identity_sha256",
        }
        if set(self.identity_hashes) != required_hashes or any(
            len(str(value)) != 64 for value in self.identity_hashes.values()
        ):
            raise OwnerRestartAwareError("owner epoch execution hashes are incomplete")


class OwnerEpochFeatureProvider(Protocol):
    def load_epoch(self, epoch: AuthoritativeReplayEpoch) -> OwnerEpochFeatureInput | None: ...


def _calendar_days(start_ts_ms: int, end_ts_ms: int) -> tuple[str, ...]:
    start = datetime.fromtimestamp(start_ts_ms / 1_000.0, tz=UTC).date()
    finish = datetime.fromtimestamp(max(start_ts_ms, end_ts_ms - 1) / 1_000.0, tz=UTC).date()
    return tuple(
        (start + timedelta(days=offset)).isoformat()
        for offset in range((finish - start).days + 1)
    )


class DailyObservationCacheEpochProvider:
    """Form restart-local M2 streams from admitted outcome-blind daily caches."""

    def __init__(self, cache_root: Path, *, identity_hashes: Mapping[str, str]) -> None:
        self.cache_root = Path(cache_root).expanduser().resolve()
        self.identity_hashes = dict(identity_hashes)

    def load_epoch(self, epoch: AuthoritativeReplayEpoch) -> OwnerEpochFeatureInput | None:
        days = tuple(epoch.source_days)
        coverage_days = _calendar_days(
            epoch.warmup_lookback_start_ts_ms,
            epoch.end_ts_ms,
        )
        caches: list[tuple[Any, int, int]] = []
        bindings = []
        for index, day in enumerate(days):
            day_root = self.cache_root / day
            if not day_root.exists():
                return None
            try:
                cache = open_admitted_observation_cache(self.cache_root, day, deep=False)
            except NativeObservationCacheError as exc:
                raise OwnerRestartAwareError(
                    f"existing M2 cache is not validly admitted: {day}"
                ) from exc
            manifest = dict(cache.manifest)
            if (
                manifest.get("exact_queue_policy_eligible") is not False
                or manifest.get("action_authorized") is not False
                or manifest.get("live_policy_authorized") is not False
            ):
                raise OwnerRestartAwareError(f"M2 cache authority drifted: {day}")
            day_start_ms = int(
                datetime.strptime(day, "%Y-%m-%d")
                .replace(tzinfo=UTC)
                .timestamp()
                * 1_000
            )
            segment_start_ms = (
                epoch.warmup_lookback_start_ts_ms
                if index == 0
                else day_start_ms
            )
            segment_end_ms = min(epoch.end_ts_ms, day_start_ms + 86_400_000)
            first_left_ts_ns = int(manifest.get("first_left_ts_ns", 0) or 0)
            last_right_ts_ns = int(manifest.get("last_right_ts_ns", 0) or 0)
            segment_start_ns = segment_start_ms * 1_000_000
            segment_end_ns = segment_end_ms * 1_000_000
            if (
                segment_end_ns <= segment_start_ns
                or first_left_ts_ns > segment_start_ns
                or last_right_ts_ns < segment_end_ns
            ):
                return None
            caches.append((cache, segment_start_ns, segment_end_ns))
            bindings.append(
                {
                    "day": day,
                    "manifest_sha256": sha256_file(cache.day_root / "manifest.json"),
                    "source_binding_sha256": str(manifest["source_binding_sha256"]),
                    "segment_start_ts_ns": segment_start_ns,
                    "segment_end_ts_ns": segment_end_ns,
                }
            )
        cutoff_ns = epoch.start_ts_ms * 1_000_000

        def observations() -> Iterator[CausalWindowObservation]:
            for cache, segment_start_ns, segment_end_ns in caches:
                for row in cache.observations_between(
                    start_feature_ready_ts_ns=segment_start_ns,
                    end_feature_ready_ts_ns=segment_end_ns,
                ):
                    yield replace(
                        row,
                        warmup_admitted=bool(row.right_ts_ns > cutoff_ns),
                    )

        feature_input = OwnerEpochFeatureInput(
            observation_factory=observations,
            warmup_cutoff_ts_ns=cutoff_ns,
            warmup_identity=canonical_sha256(
                {
                    "epoch_id": epoch.epoch_id,
                    "warmup_start_ts_ms": epoch.warmup_lookback_start_ts_ms,
                    "bindings": bindings,
                }
            ),
            identity_hashes=self.identity_hashes,
            source_cursor_prefixes={
                name: f"owner-m2:{epoch.epoch_id}:{name}"
                for name in ("market", "depth", "trade")
            },
            support_binding={
                "supported": True,
                "days": list(days),
                "coverage_calendar_days": list(coverage_days),
                "bindings": bindings,
                "receive_time_transport_authority": False,
                "exact_queue_authority": False,
            },
        )
        feature_input.validate()
        return feature_input


@dataclass(slots=True)
class EpochRuntimeBinding:
    params: dict[str, Any]
    mode: str
    feature_input: OwnerEpochFeatureInput | None = None
    evaluator: Any | None = None
    emitter: Any | None = None

    def receipt(self) -> dict[str, Any]:
        policy_audit = dict(self.evaluator.audit()) if self.evaluator is not None else {}
        emitter_audit: dict[str, Any] = {}
        if self.emitter is not None:
            raw = self.emitter.audit()
            if hasattr(raw, "__dataclass_fields__"):
                emitter_audit = asdict(raw)
            elif isinstance(raw, Mapping):
                emitter_audit = dict(raw)
            else:
                emitter_audit = dict(vars(raw))
        return {
            "mode": self.mode,
            "candidate_effective_policy": (
                "owner_boolean_cooldown" if self.mode == "owner_policy" else "control"
            ),
            "missing_m2_control_fallback": self.mode == "missing_m2_control_fallback",
            "policy_audit": policy_audit,
            "emitter_audit": emitter_audit,
            "support_binding": (
                dict(self.feature_input.support_binding)
                if self.feature_input is not None
                else (
                    {
                        "supported": True,
                        "reason": "control_arm_does_not_require_m2",
                    }
                    if self.mode == "control"
                    else {"supported": False, "reason": "missing_epoch_m2_cache"}
                )
            ),
        }


def bind_epoch_runtime(
    *,
    arm: str,
    epoch: AuthoritativeReplayEpoch,
    params: Mapping[str, Any],
    feature_provider: OwnerEpochFeatureProvider,
    policy_path: Path,
    expected_policy_sha256: str,
    runtime_loader: Callable[..., Any] = load_runtime_policy_evaluator,
    emitter_factory: Callable[..., Any] = CooldownV2ReplayEmitter,
) -> EpochRuntimeBinding:
    bound = dict(params)
    bound.pop("cooldown_v2_snapshot_emitter", None)
    bound.pop("cooldown_duration_policy_evaluator", None)
    if arm == CONTROL_ARM:
        return EpochRuntimeBinding(params=bound, mode="control")
    if arm != CANDIDATE_ARM:
        raise OwnerRestartAwareError(f"unknown continuous arm: {arm}")
    feature_input = feature_provider.load_epoch(epoch)
    if feature_input is None:
        return EpochRuntimeBinding(params=bound, mode="missing_m2_control_fallback")
    feature_input.validate()
    evaluator = runtime_loader(
        policy_path,
        expected_policy_sha256=expected_policy_sha256,
    )
    binding_error = getattr(evaluator, "_binding_error", None)
    if binding_error:
        raise OwnerRestartAwareError(f"owner runtime policy failed binding: {binding_error}")
    emitter = emitter_factory(
        feature_block="M2",
        observations=feature_input.observation_factory(),
        warmup_cutoff_ts_ns=feature_input.warmup_cutoff_ts_ns,
        warmup_identity=feature_input.warmup_identity,
        identity_hashes=feature_input.identity_hashes,
        source_cursor_prefixes=feature_input.source_cursor_prefixes,
        retain_snapshots=False,
    )
    bound["cooldown_v2_snapshot_emitter"] = emitter
    bound["cooldown_duration_policy_evaluator"] = evaluator
    return EpochRuntimeBinding(
        params=bound,
        mode="owner_policy",
        feature_input=feature_input,
        evaluator=evaluator,
        emitter=emitter,
    )


class OwnerBooleanCooldownRestartAwareAdapter(NarrowGateContinuousTickReplayAdapter):
    """Python owner-policy binding over the shared restart-bounded executor."""

    def __init__(
        self,
        *,
        preflight: Mapping[str, Any],
        plan_identity_sha256: str | None = None,
        operations: Sequence[ContinuousOperation],
        arm_bindings: Mapping[str, AdapterArmBinding],
        shared_input_provider: SharedReplayDayInputProvider,
        initial_states: Mapping[str, ContinuousReplayState],
        feature_provider: OwnerEpochFeatureProvider,
        output_root: Path,
        panel_cancel_drain_ms: int,
        policy_path: Path = DEFAULT_POLICY,
        expected_policy_sha256: str = DEFAULT_POLICY_SHA256,
        runtime_identity_sha256: str,
        warmup_context_identities: Mapping[str, str] | None = None,
        simulate_python: Callable[..., Mapping[str, Any]] | None = None,
        runtime_loader: Callable[..., Any] = load_runtime_policy_evaluator,
        emitter_factory: Callable[..., Any] = CooldownV2ReplayEmitter,
    ) -> None:
        validate_preflight(preflight, require_eligible=True)
        if set(arm_bindings) != set(ARMS) or set(initial_states) != set(ARMS):
            raise OwnerRestartAwareError("owner continuous adapter requires control/candidate arms")
        control_state = initial_states[CONTROL_ARM].to_dict()
        candidate_state = initial_states[CANDIDATE_ARM].to_dict()
        control_state.pop("arm_id")
        candidate_state.pop("arm_id")
        if control_state != candidate_state:
            raise OwnerRestartAwareError("paired arms must start from common economics")
        if arm_bindings[CONTROL_ARM].cadence_ms != arm_bindings[CANDIDATE_ARM].cadence_ms:
            raise OwnerRestartAwareError("owner arms must share the same inference cadence")
        resolved_policy = Path(policy_path).expanduser().resolve()
        owner_binding = preflight.get("owner_policy") or {}
        if (
            Path(str(owner_binding.get("path", ""))).expanduser().resolve()
            != resolved_policy
            or owner_binding.get("sha256") != expected_policy_sha256
            or arm_bindings[CANDIDATE_ARM].policy_identity_sha256
            != expected_policy_sha256
        ):
            raise OwnerRestartAwareError("owner continuous policy binding drifted")
        operation_rows = tuple(operations)
        if len(runtime_identity_sha256) != 64:
            raise OwnerRestartAwareError("owner runtime identity is invalid")
        warmup_identities = {
            str(day): str(identity)
            for day, identity in sorted((warmup_context_identities or {}).items())
        }
        if any(len(identity) != 64 for identity in warmup_identities.values()):
            raise OwnerRestartAwareError("owner warmup context identity is invalid")
        operation_tape_sha256 = canonical_sha256([asdict(row) for row in operation_rows])
        if (preflight.get("framework") or {}).get(
            "operation_tape_sha256"
        ) != operation_tape_sha256:
            raise OwnerRestartAwareError("owner continuous operation tape drifted")
        derived_plan_identity = canonical_sha256(
            {
                "identity": IDENTITY,
                "preflight_identity_sha256": preflight["preflight_identity_sha256"],
                "operation_tape_sha256": operation_tape_sha256,
                "policy_sha256": expected_policy_sha256,
                "arm_policy_identities": {
                    arm: arm_bindings[arm].policy_identity_sha256 for arm in ARMS
                },
                "initial_states": {
                    arm: initial_states[arm].to_dict() for arm in ARMS
                },
                "runtime_identity_sha256": runtime_identity_sha256,
                "warmup_context_identities": warmup_identities,
            }
        )
        if plan_identity_sha256 is not None and plan_identity_sha256 != derived_plan_identity:
            raise OwnerRestartAwareError("owner continuous plan identity drifted")
        self.owner_preflight = dict(preflight)
        self.feature_provider = feature_provider
        self.policy_path = resolved_policy
        self.owner_plan_identity_sha256 = derived_plan_identity
        self.runtime_identity_sha256 = runtime_identity_sha256
        self.warmup_context_identities = warmup_identities
        self.expected_policy_sha256 = expected_policy_sha256
        self.runtime_loader = runtime_loader
        self.emitter_factory = emitter_factory
        self._simulate_python = simulate_python
        self._active_context: tuple[str, AuthoritativeReplayEpoch] | None = None
        self._runtime_receipts: dict[tuple[str, str], EpochRuntimeBinding] = {}
        super().__init__(
            plan_identity_sha256=derived_plan_identity,
            operations=operation_rows,
            arm_bindings=arm_bindings,
            input_provider=_ArmSharedReplayInputAdapter(shared_input_provider),
            initial_states=initial_states,
            output_root=output_root,
            panel_cancel_drain_ms=panel_cancel_drain_ms,
            simulate=self._dispatch_python,
        )

    def _dispatch_python(
        self,
        engine: str,
        trades_df: Any,
        var_ts_ms: Any,
        var_ssq: Any,
        params: Mapping[str, Any],
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        if engine != "cpp" or self._active_context is None:
            raise OwnerRestartAwareError("owner Python dispatch escaped its epoch context")
        arm, epoch = self._active_context
        binding = bind_epoch_runtime(
            arm=arm,
            epoch=epoch,
            params=params,
            feature_provider=self.feature_provider,
            policy_path=self.policy_path,
            expected_policy_sha256=self.expected_policy_sha256,
            runtime_loader=self.runtime_loader,
            emitter_factory=self.emitter_factory,
        )
        simulate = self._simulate_python
        if simulate is None:
            from models import backtest_tick as bt

            simulate = bt._simulate_tick_with_engine
        result = simulate(
            "python",
            trades_df,
            var_ts_ms,
            var_ssq,
            binding.params,
            **kwargs,
        )
        self._runtime_receipts[(arm, epoch.epoch_id)] = binding
        return result

    def _simulate_epoch(
        self,
        *,
        arm: str,
        epoch: AuthoritativeReplayEpoch,
        ledger: Any,
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
        if self._active_context is not None:
            raise OwnerRestartAwareError("owner continuous adapter is not re-entrant")
        self._active_context = (arm, epoch)
        try:
            mechanics, authority = super()._simulate_epoch(
                arm=arm,
                epoch=epoch,
                ledger=ledger,
            )
        finally:
            self._active_context = None
        if any(
            row[claim]
            for row in authority
            for claim in ("exact_queue_authority", "exact_lifecycle_authority")
        ):
            raise OwnerRestartAwareError("modeled owner replay emitted exact authority")
        binding = self._runtime_receipts.pop((arm, epoch.epoch_id), None)
        if binding is None:
            raise OwnerRestartAwareError("owner epoch lacks a runtime-policy receipt")
        return (
            mechanics
            | {
                "owner_runtime": binding.receipt(),
                "engine": "python",
                "utc_midnight_preserves_cooldown_and_ema": True,
                "planned_restart_flattens_inventory": False,
                "strict_queue_authority": False,
                "receive_time_transport_authority": False,
                "action_authorized": False,
                "live_authorized": False,
            },
            authority,
        )

    def run(self, *, max_epochs: int | None = None) -> dict[str, Any]:
        result = super().run(max_epochs=max_epochs)
        return result | {
            "identity": IDENTITY,
            "daily_fresh_start_prerequisite_bound": True,
            "continuous_economic_result_authorized": False,
            "strict_queue_authority": False,
            "receive_time_transport_authority": False,
            "action_authorized": False,
            "live_authorized": False,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily-panel", type=Path, default=DEFAULT_DAILY_PANEL)
    parser.add_argument("--framework-plan", type=Path, default=DEFAULT_FRAMEWORK_PLAN)
    parser.add_argument("--policy-path", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--policy-sha256", default=DEFAULT_POLICY_SHA256)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload = prepare_preflight(
        daily_panel=args.daily_panel,
        framework_plan=args.framework_plan,
        policy_path=args.policy_path,
        expected_policy_sha256=args.policy_sha256,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


__all__ = [
    "ARMS",
    "CANDIDATE_ARM",
    "CONTROL_ARM",
    "DailyObservationCacheEpochProvider",
    "EpochRuntimeBinding",
    "IDENTITY",
    "OwnerBooleanCooldownRestartAwareAdapter",
    "OwnerEpochFeatureInput",
    "OwnerRestartAwareError",
    "bind_epoch_runtime",
    "prepare_preflight",
    "validate_preflight",
]


if __name__ == "__main__":
    raise SystemExit(main())
