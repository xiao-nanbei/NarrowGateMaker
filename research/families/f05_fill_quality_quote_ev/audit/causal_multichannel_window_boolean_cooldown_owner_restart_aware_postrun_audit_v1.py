#!/usr/bin/env python3
"""Read-only strong audit for the F05 owner restart-aware execution.

This module is deliberately absent from the frozen execution plan and runtime
artifact graph. It validates admitted artifacts after the run has completed;
it never resumes execution, finalizes economics, or grants research/action/live
authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_owner_restart_aware_"
    "postrun_audit_v1"
)
SCHEMA_VERSION = f"{IDENTITY}.v1"
PLAN_IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_owner_restart_aware_execution_v1"
)
PLAN_SCHEMA_VERSION = f"{PLAN_IDENTITY}.v1.plan"
RECEIPT_SCHEMA_VERSION = (
    "narrowgate_authoritative_continuous_tick_adapter.v1.mechanics_receipt"
)
CHECKPOINT_SCHEMA_VERSION = (
    "narrowgate_authoritative_continuous_tick_adapter.v1.checkpoint"
)
ARMS = ("control", "candidate")
EXPECTED_EPOCH_COUNT = 104
EXPECTED_UTC_DAY_COUNT = 71
EXPECTED_CANDIDATE_MODE_COUNTS = {
    "owner_policy": 60,
    "missing_m2_control_fallback": 44,
}
ABS_TOLERANCE_USDC = 1e-6
PLAN_MARKER_NAME = "_PLAN_SUCCESS"

PLAN_PERMISSION_FIELDS = (
    "strict_queue_authority",
    "receive_time_transport_authority",
    "research_supported",
    "action_authorized",
    "live_authorized",
)
ARM_PERMISSION_FIELDS = (
    "strict_queue_authority",
    "receive_time_transport_authority",
    "action_authorized",
    "live_authorized",
)


class PostrunAuditError(RuntimeError):
    """Raised when an admitted post-run artifact fails the frozen audit."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PostrunAuditError(message)


def _load_json_object(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    _require(resolved.is_file(), f"missing {role}: {resolved}")
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PostrunAuditError(f"invalid {role}: {resolved}") from exc
    _require(isinstance(value, dict), f"{role} must be a JSON object")
    return value


def _verify_marker(path: Path, marker_path: Path, *, role: str) -> str:
    _require(path.is_file(), f"missing {role}: {path}")
    _require(marker_path.is_file(), f"missing {role} marker: {marker_path}")
    file_sha256 = sha256_file(path)
    try:
        marker_value = marker_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise PostrunAuditError(f"invalid {role} marker: {marker_path}") from exc
    _require(marker_value == file_sha256, f"{role} atomic marker drifted")
    return file_sha256


def _load_admitted_canonical_json(
    path: Path,
    *,
    marker_path: Path,
    hash_field: str,
    schema_version: str,
    role: str,
) -> tuple[dict[str, Any], str]:
    file_sha256 = _verify_marker(path, marker_path, role=role)
    payload = _load_json_object(path, role=role)
    _require(
        payload.get("schema_version") == schema_version,
        f"{role} schema drifted",
    )
    normalized = dict(payload)
    expected = normalized.pop(hash_field, None)
    _require(
        isinstance(expected, str) and len(expected) == 64,
        f"{role} lacks canonical identity",
    )
    _require(
        canonical_sha256(normalized) == expected,
        f"{role} canonical hash drifted",
    )
    return payload, file_sha256


def _require_false_fields(
    payload: Mapping[str, Any], fields: Sequence[str], *, role: str
) -> None:
    for field in fields:
        _require(payload.get(field) is False, f"{role} granted {field}")


def _resolve_bound_path(
    value: Any,
    *,
    expected: Path,
    role: str,
) -> Path:
    actual = Path(str(value)).expanduser().resolve()
    _require(actual == expected.resolve(), f"{role} escaped canonical path")
    return actual


def _expected_epoch_ids(count: int) -> tuple[str, ...]:
    return tuple(f"epoch-{ordinal:04d}" for ordinal in range(1, count + 1))


def _assert_exact_admission_files(
    root: Path,
    *,
    expected_ids: Sequence[str],
    role: str,
) -> None:
    expected_json = {f"{epoch_id}.json" for epoch_id in expected_ids}
    expected_markers = {f"{epoch_id}.success" for epoch_id in expected_ids}
    actual_json = {path.name for path in root.glob("epoch-*.json")}
    actual_markers = {path.name for path in root.glob("epoch-*.success")}
    _require(actual_json == expected_json, f"{role} JSON denominator drifted")
    _require(actual_markers == expected_markers, f"{role} marker denominator drifted")


def _utc_day_before_boundary(ts_ms: int) -> str:
    return datetime.fromtimestamp((int(ts_ms) - 1) / 1_000.0, tz=UTC).date().isoformat()


def _validate_epoch(
    epoch: Mapping[str, Any],
    *,
    epoch_id: str,
    previous_gap_end_ts_ms: int | None,
) -> tuple[int, tuple[int, ...]]:
    _require(epoch.get("epoch_id") == epoch_id, f"{epoch_id} identity drifted")
    start = int(epoch.get("start_ts_ms", -1))
    quote_stop = int(epoch.get("quote_stop_ts_ms", -1))
    end = int(epoch.get("end_ts_ms", -1))
    gap_end = int(epoch.get("gap_end_ts_ms", -1))
    _require(start >= 0 and start <= quote_stop < end <= gap_end, f"{epoch_id} clock drifted")
    if previous_gap_end_ts_ms is not None:
        _require(start == previous_gap_end_ts_ms, f"{epoch_id} restart chain has a gap")
    boundaries = tuple(int(value) for value in epoch.get("utc_boundaries_ts_ms", ()))
    _require(boundaries == tuple(sorted(set(boundaries))), f"{epoch_id} UTC boundaries drifted")
    _require(
        all(start < boundary <= gap_end for boundary in boundaries),
        f"{epoch_id} UTC boundary escaped epoch/gap",
    )
    random_path = epoch.get("random_path_sha256")
    _require(
        isinstance(random_path, str) and len(random_path) == 64,
        f"{epoch_id} random path identity is invalid",
    )
    return gap_end, boundaries


def _validate_warmup(
    mechanics: Mapping[str, Any],
    *,
    epoch: Mapping[str, Any],
    role: str,
) -> None:
    _require(
        int(mechanics.get("warmup_lookback_start_ts_ms", -1))
        == int(epoch["warmup_lookback_start_ts_ms"]),
        f"{role} warmup start drifted",
    )
    _require(
        int(mechanics.get("quote_resume_ts_ms", -1)) == int(epoch["start_ts_ms"]),
        f"{role} quote resume drifted",
    )
    warmup = mechanics.get("warmup")
    _require(isinstance(warmup, Mapping), f"{role} lacks warmup receipt")
    _require(warmup.get("quoting_enabled") is False, f"{role} quoted during warmup")
    if warmup.get("required") is True:
        _require(int(warmup.get("market_event_count", 0)) > 0, f"{role} warmup lacks market events")
        _require(int(warmup.get("prediction_row_count", 0)) > 0, f"{role} warmup lacks predictions")
        ready = warmup.get("latest_prediction_ready_ts_ms")
        _require(ready is not None, f"{role} warmup lacks feature-ready clock")
        _require(
            int(ready) <= int(epoch["start_ts_ms"]),
            f"{role} warmup consumed future-ready prediction",
        )
        _require(
            warmup.get("feature_ready_not_after_resume") is True,
            f"{role} warmup past-only flag failed",
        )
    else:
        _require(
            int(epoch["warmup_lookback_start_ts_ms"]) >= int(epoch["start_ts_ms"]),
            f"{role} skipped required warmup",
        )
        _require(int(warmup.get("market_event_count", 0)) == 0, f"{role} optional warmup has events")
        _require(int(warmup.get("prediction_row_count", 0)) == 0, f"{role} optional warmup has predictions")
        _require(
            warmup.get("latest_prediction_ready_ts_ms") is None,
            f"{role} optional warmup has a ready timestamp",
        )


def _validate_offline_gap(
    mechanics: Mapping[str, Any], *, epoch: Mapping[str, Any], role: str
) -> None:
    gap = mechanics.get("offline_gap")
    _require(isinstance(gap, Mapping), f"{role} lacks offline-gap receipt")
    _require(int(gap.get("fill_count", -1)) == 0, f"{role} traded during offline gap")
    _require(
        gap.get("market_event_trading_enabled") is False,
        f"{role} enabled trading during offline gap",
    )
    _require(
        int(gap.get("start_ts_ms", -1)) == int(epoch["end_ts_ms"])
        and int(gap.get("end_ts_ms", -1)) == int(epoch["gap_end_ts_ms"]),
        f"{role} offline-gap clock drifted",
    )
    if int(epoch["gap_end_ts_ms"]) > int(epoch["end_ts_ms"]):
        _require(gap.get("cash_unchanged") is True, f"{role} changed cash in offline gap")
        _require(
            gap.get("inventory_unchanged") is True,
            f"{role} changed inventory in offline gap",
        )


def _validate_mechanics(
    mechanics: Mapping[str, Any],
    *,
    arm: str,
    epoch: Mapping[str, Any],
) -> None:
    role = f"{epoch['epoch_id']} {arm} mechanics"
    _require(mechanics.get("epoch_id") == epoch["epoch_id"], f"{role} epoch drifted")
    _require(mechanics.get("arm") == arm, f"{role} arm drifted")
    _require(mechanics.get("engine") == "python", f"{role} engine drifted")
    _require(mechanics.get("actual_tick_replay_used") is True, f"{role} skipped tick replay")
    _require(
        mechanics.get("planned_quote_stop_triggered") is True,
        f"{role} did not reach quote stop",
    )
    _require(
        int(mechanics.get("planned_shutdown_remaining_orders", -1)) == 0,
        f"{role} retained exchange orders",
    )
    _require(
        mechanics.get("planned_restart_flattens_inventory") is False,
        f"{role} flattened inventory at restart",
    )
    _require(
        mechanics.get("utc_midnight_preserves_cooldown_and_ema") is True,
        f"{role} reset policy state at UTC midnight",
    )
    _require_false_fields(mechanics, ARM_PERMISSION_FIELDS, role=role)
    _require(
        mechanics.get("economic_outputs_read") is False,
        f"{role} read economic outputs during mechanics execution",
    )
    _require(
        int(mechanics.get("provider_exact_queue_claim_count", -1)) == 0,
        f"{role} emitted exact queue authority",
    )
    _require(
        int(mechanics.get("provider_exact_lifecycle_claim_count", -1)) == 0,
        f"{role} emitted exact lifecycle authority",
    )
    for source in mechanics.get("source_authority", ()):
        _require(isinstance(source, Mapping), f"{role} source authority is invalid")
        _require(
            source.get("exact_queue_authority") is False
            and source.get("exact_lifecycle_authority") is False,
            f"{role} source claimed exact authority",
        )
    _validate_warmup(mechanics, epoch=epoch, role=role)
    _validate_offline_gap(mechanics, epoch=epoch, role=role)
    _require(
        int(mechanics.get("random_seed", -1)) == int(epoch["random_seed"]),
        f"{role} random seed drifted",
    )
    _require(
        mechanics.get("random_path_sha256") == epoch["random_path_sha256"],
        f"{role} random path drifted",
    )


def _validate_runtime_mode(
    mechanics: Mapping[str, Any],
    *,
    arm: str,
    epoch_id: str,
) -> tuple[str, Mapping[str, Any]]:
    runtime = mechanics.get("owner_runtime")
    _require(isinstance(runtime, Mapping), f"{epoch_id} {arm} lacks owner runtime")
    mode = str(runtime.get("mode", ""))
    audit = runtime.get("policy_audit")
    _require(isinstance(audit, Mapping), f"{epoch_id} {arm} policy audit is invalid")
    support = runtime.get("support_binding")
    _require(isinstance(support, Mapping), f"{epoch_id} {arm} support binding is invalid")
    if arm == "control":
        _require(mode == "control", f"{epoch_id} control mode drifted")
        _require(runtime.get("candidate_effective_policy") == "control", f"{epoch_id} control policy drifted")
        _require(runtime.get("missing_m2_control_fallback") is False, f"{epoch_id} control marked fallback")
        _require(not audit, f"{epoch_id} control emitted policy actions")
        _require(support.get("supported") is True, f"{epoch_id} control lost support")
        return mode, audit
    _require(
        mode in EXPECTED_CANDIDATE_MODE_COUNTS,
        f"{epoch_id} candidate runtime mode drifted",
    )
    if mode == "owner_policy":
        _require(
            runtime.get("candidate_effective_policy") == "owner_boolean_cooldown",
            f"{epoch_id} owner policy identity drifted",
        )
        _require(
            runtime.get("missing_m2_control_fallback") is False,
            f"{epoch_id} owner policy marked fallback",
        )
        _require(support.get("supported") is True, f"{epoch_id} owner policy lacks support")
        _require(
            support.get("exact_queue_authority") is False
            and support.get("receive_time_transport_authority") is False,
            f"{epoch_id} owner support exceeded authority",
        )
        action_counts = audit.get("action_counts")
        _require(isinstance(action_counts, Mapping), f"{epoch_id} lacks nested action counts")
        numeric_counts = [int(value) for value in action_counts.values()]
        _require(all(value >= 0 for value in numeric_counts), f"{epoch_id} action count is negative")
        evaluations = int(audit.get("evaluations", -1))
        _require(sum(numeric_counts) == evaluations, f"{epoch_id} action counts do not equal evaluations")
        _require(
            int(audit.get("fallback", -1)) + int(audit.get("nonbaseline", -1))
            == evaluations,
            f"{epoch_id} policy decision denominator drifted",
        )
    else:
        _require(
            runtime.get("candidate_effective_policy") == "control",
            f"{epoch_id} fallback did not execute control",
        )
        _require(
            runtime.get("missing_m2_control_fallback") is True,
            f"{epoch_id} fallback flag drifted",
        )
        _require(not audit, f"{epoch_id} fallback emitted policy actions")
        _require(support.get("supported") is False, f"{epoch_id} fallback claimed M2 support")
    return mode, audit


def _validate_checkpoint_semantics(
    checkpoint: Mapping[str, Any],
    *,
    receipt_mechanics: Mapping[str, Any],
    arm: str,
    epoch: Mapping[str, Any],
    ordinal: int,
) -> None:
    role = f"{epoch['epoch_id']} {arm} checkpoint"
    state = checkpoint.get("state")
    ledger = checkpoint.get("ledger_state")
    engine = checkpoint.get("engine_state")
    _require(isinstance(state, Mapping), f"{role} state is invalid")
    _require(isinstance(ledger, Mapping), f"{role} ledger is invalid")
    _require(isinstance(engine, Mapping), f"{role} engine state is invalid")
    _require(canonical_sha256(dict(state)) == checkpoint.get("state_sha256"), f"{role} state hash drifted")
    _require(canonical_sha256(dict(ledger)) == checkpoint.get("ledger_state_sha256"), f"{role} ledger hash drifted")
    _require(canonical_sha256(dict(engine)) == checkpoint.get("engine_state_sha256"), f"{role} engine hash drifted")
    _require(canonical_sha256(dict(receipt_mechanics)) == checkpoint.get("mechanics_sha256"), f"{role} mechanics hash drifted")
    _require(dict(state) == ledger.get("state"), f"{role} state/ledger split brain")
    _require(
        engine.get("accounting_state_sha256") == checkpoint.get("ledger_state_sha256"),
        f"{role} engine accounting binding drifted",
    )
    _require(state.get("arm_id") == arm, f"{role} state arm drifted")
    _require(int(state.get("restart_generation", -1)) == ordinal, f"{role} restart generation drifted")
    _require(int(state.get("checkpoint_ts_ms", -1)) == int(epoch["gap_end_ts_ms"]), f"{role} checkpoint clock drifted")
    _require(state.get("orders_terminal") is True, f"{role} retained nonterminal orders")
    _require(state.get("quoting_enabled") is False, f"{role} retained quoting")
    for field in (
        "active_order_count",
        "pending_cancel_count",
        "queue_cursor_count",
        "q90_cursor_count",
    ):
        _require(int(state.get(field, -1)) == 0, f"{role} retained {field}")
    _require(engine.get("checkpoint_boundary") == "post_cancel_ack_drain", f"{role} boundary drifted")
    for field in (
        "active_orders",
        "pending_new_orders",
        "pending_cancel_orders",
        "queue_positions",
        "queue_cursors",
        "q90_cursors",
    ):
        value = engine.get(field)
        _require(isinstance(value, list) and not value, f"{role} retained {field}")
    cooldowns = engine.get("cooldown_lineages")
    _require(isinstance(cooldowns, Mapping) and not cooldowns, f"{role} retained cooldown lineages")
    _require_false_fields(
        checkpoint,
        ("economic_outcomes_read", "promotion_authorized"),
        role=role,
    )


def _validate_ledger_prefix(
    previous: Mapping[str, tuple[Any, ...]],
    ledger: Mapping[str, Any],
    *,
    role: str,
) -> dict[str, tuple[Any, ...]]:
    current: dict[str, tuple[Any, ...]] = {}
    for field in ("daily_slices", "closed_campaigns", "gap_carries"):
        rows = ledger.get(field)
        _require(isinstance(rows, list), f"{role} {field} is invalid")
        values = tuple(rows)
        prior = previous.get(field, ())
        _require(values[: len(prior)] == prior, f"{role} rewrote prior {field}")
        current[field] = values
    return current


def _aggregate_policy_audit(
    aggregate: dict[str, Any], audit: Mapping[str, Any], *, role: str
) -> None:
    for name, raw in audit.items():
        if name == "action_counts":
            _require(isinstance(raw, Mapping), f"{role} action_counts is invalid")
            target = aggregate.setdefault("action_counts", defaultdict(int))
            _require(isinstance(target, defaultdict), f"{role} aggregate action_counts drifted")
            for action, value in raw.items():
                _require(isinstance(value, int) and value >= 0, f"{role} action count is invalid")
                target[str(action)] += int(value)
            continue
        _require(
            isinstance(raw, (int, float)) and not isinstance(raw, bool),
            f"{role} policy audit field {name} is not numeric",
        )
        numeric = float(raw)
        _require(math.isfinite(numeric), f"{role} policy audit field {name} is nonfinite")
        if name == "duration_ms_max":
            aggregate[name] = max(float(aggregate.get(name, 0.0)), numeric)
        else:
            aggregate[name] = float(aggregate.get(name, 0.0)) + numeric


def _normalize_policy_aggregate(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for name, raw in sorted(value.items()):
        if name == "action_counts":
            normalized[name] = dict(sorted(raw.items()))
            continue
        number = float(raw)
        normalized[name] = int(number) if number.is_integer() else number
    return normalized


def _validate_terminal_accounting(
    terminal: Mapping[str, Mapping[str, Any]],
    *,
    expected_days: Sequence[str],
) -> tuple[dict[str, Any], float]:
    accounting: dict[str, Any] = {}
    paired_days: tuple[str, ...] | None = None
    maximum_residual = 0.0
    for arm in ARMS:
        checkpoint = terminal[arm]
        ledger = checkpoint["ledger_state"]
        state = checkpoint["state"]
        rows = ledger["daily_slices"]
        _require(len(rows) == EXPECTED_UTC_DAY_COUNT, f"{arm} UTC day count drifted")
        days = tuple(str(row.get("day", "")) for row in rows)
        _require(days == tuple(expected_days), f"{arm} UTC day denominator drifted")
        if paired_days is None:
            paired_days = days
        else:
            _require(days == paired_days, "paired arms use different UTC accounting days")
        prior_end: float | None = None
        daily_pnl: list[float] = []
        day_identity_residual = 0.0
        continuity_residual = 0.0
        for row in rows:
            start = float(row["start_equity_usdc"])
            end = float(row["end_equity_usdc"])
            pnl = float(row["pnl_usdc"])
            _require(all(math.isfinite(value) for value in (start, end, pnl)), f"{arm} daily accounting is nonfinite")
            residual = abs((end - start) - pnl)
            day_identity_residual = max(day_identity_residual, residual)
            _require(residual <= ABS_TOLERANCE_USDC, f"{arm} daily PnL identity drifted")
            if prior_end is not None:
                residual = abs(start - prior_end)
                continuity_residual = max(continuity_residual, residual)
                _require(residual <= ABS_TOLERANCE_USDC, f"{arm} inter-day equity is discontinuous")
            prior_end = end
            daily_pnl.append(pnl)
        terminal_pnl = float(state["cumulative_pnl_usdc"])
        sum_residual = abs(math.fsum(daily_pnl) - terminal_pnl)
        _require(sum_residual <= ABS_TOLERANCE_USDC, f"{arm} daily PnL sum differs from terminal PnL")
        terminal_equity = float(state["cash_usdc"]) + float(state["position_btc"]) * float(state["last_mark_price"])
        equity_residual = abs(
            terminal_equity - float(state["equity_anchor_usdc"]) - terminal_pnl
        )
        _require(equity_residual <= ABS_TOLERANCE_USDC, f"{arm} terminal equity identity drifted")
        _require(prior_end is not None, f"{arm} lacks terminal daily equity")
        terminal_day_residual = abs(prior_end - terminal_equity)
        _require(terminal_day_residual <= ABS_TOLERANCE_USDC, f"{arm} terminal daily equity drifted")
        residuals = {
            "daily_identity_max_abs_error_usdc": day_identity_residual,
            "interday_continuity_max_abs_error_usdc": continuity_residual,
            "daily_sum_vs_terminal_abs_error_usdc": sum_residual,
            "terminal_equity_identity_abs_error_usdc": equity_residual,
            "terminal_day_equity_abs_error_usdc": terminal_day_residual,
        }
        maximum_residual = max(maximum_residual, *residuals.values())
        accounting[arm] = {
            "utc_day_count": len(days),
            "first_utc_day": days[0],
            "last_utc_day": days[-1],
            "residuals": residuals,
        }
    return accounting, maximum_residual


def audit_postrun(plan_path: Path) -> dict[str, Any]:
    """Validate a complete admitted run without altering or interpreting it."""

    resolved_plan = Path(plan_path).expanduser().resolve()
    plan, plan_file_sha256 = _load_admitted_canonical_json(
        resolved_plan,
        marker_path=resolved_plan.parent / PLAN_MARKER_NAME,
        hash_field="plan_identity_sha256",
        schema_version=PLAN_SCHEMA_VERSION,
        role="owner restart-aware execution plan",
    )
    _require(plan.get("identity") == PLAN_IDENTITY, "execution plan identity drifted")
    _require(plan.get("execution_eligible") is True, "execution plan is not eligible")
    _require(not plan.get("blockers"), "execution plan retains blockers")
    _require(plan.get("economic_outcomes_read") is False, "execution plan read economics")
    _require(plan.get("economic_results_aggregated") is False, "execution plan aggregated economics")
    permissions = plan.get("permissions")
    _require(isinstance(permissions, Mapping), "execution plan lacks permissions")
    _require_false_fields(permissions, PLAN_PERMISSION_FIELDS, role="execution plan")
    adapter_identity = plan.get("adapter_plan_identity_sha256")
    _require(
        isinstance(adapter_identity, str) and len(adapter_identity) == 64,
        "execution plan lacks adapter identity",
    )
    framework = plan.get("framework")
    _require(isinstance(framework, Mapping), "execution plan lacks framework binding")
    _require(
        int(framework.get("authoritative_epoch_count", 0)) == EXPECTED_EPOCH_COUNT,
        "authoritative epoch denominator drifted",
    )
    _require(
        int(framework.get("utc_accounting_boundary_count", 0))
        == EXPECTED_UTC_DAY_COUNT,
        "UTC accounting boundary denominator drifted",
    )
    execution_root = Path(str(plan.get("execution_output_root", ""))).expanduser().resolve()
    _require(execution_root.name == adapter_identity, "execution root is not identity namespaced")
    receipt_root = execution_root / "receipts"
    checkpoint_roots = {arm: execution_root / "checkpoints" / arm for arm in ARMS}
    expected_ids = _expected_epoch_ids(EXPECTED_EPOCH_COUNT)
    _assert_exact_admission_files(receipt_root, expected_ids=expected_ids, role="receipt")
    for arm, root in checkpoint_roots.items():
        _assert_exact_admission_files(root, expected_ids=expected_ids, role=f"{arm} checkpoint")

    previous_checkpoint_sha256 = {arm: "" for arm in ARMS}
    previous_ledger_sequences: dict[str, dict[str, tuple[Any, ...]]] = {
        arm: {} for arm in ARMS
    }
    terminal_checkpoints: dict[str, Mapping[str, Any]] = {}
    candidate_mode_counts: Counter[str] = Counter()
    control_mode_counts: Counter[str] = Counter()
    policy_aggregate: dict[str, Any] = {}
    all_boundaries: list[int] = []
    previous_gap_end: int | None = None
    cumulative_boundary_count = 0
    terminal_checkpoint_sha256: dict[str, str] = {}

    for ordinal, epoch_id in enumerate(expected_ids, start=1):
        receipt_path = receipt_root / f"{epoch_id}.json"
        receipt, _ = _load_admitted_canonical_json(
            receipt_path,
            marker_path=receipt_path.with_suffix(".success"),
            hash_field="receipt_sha256",
            schema_version=RECEIPT_SCHEMA_VERSION,
            role=f"paired receipt {epoch_id}",
        )
        _require(receipt.get("plan_identity_sha256") == adapter_identity, f"{epoch_id} plan identity drifted")
        _require(receipt.get("same_random_path") is True, f"{epoch_id} paired random paths differ")
        _require_false_fields(
            receipt,
            ("economic_outcomes_read", "promotion_authorized"),
            role=f"paired receipt {epoch_id}",
        )
        arms = receipt.get("arms")
        checkpoint_refs = receipt.get("checkpoints")
        _require(set(arms or {}) == set(ARMS), f"{epoch_id} receipt lost an arm")
        _require(set(checkpoint_refs or {}) == set(ARMS), f"{epoch_id} receipt lost a checkpoint")
        epoch = receipt.get("epoch")
        _require(isinstance(epoch, Mapping), f"{epoch_id} epoch payload is invalid")
        previous_gap_end, boundaries = _validate_epoch(
            epoch,
            epoch_id=epoch_id,
            previous_gap_end_ts_ms=previous_gap_end,
        )
        all_boundaries.extend(boundaries)
        cumulative_boundary_count += len(boundaries)
        if ordinal < EXPECTED_EPOCH_COUNT:
            _require(epoch.get("terminal") is False, f"{epoch_id} became terminal early")
        else:
            _require(epoch.get("terminal") is True, "last epoch is not terminal")

        arm_random_paths: set[str] = set()
        for arm in ARMS:
            mechanics = arms[arm]
            _require(isinstance(mechanics, Mapping), f"{epoch_id} {arm} mechanics is invalid")
            _validate_mechanics(mechanics, arm=arm, epoch=epoch)
            arm_random_paths.add(str(mechanics["random_path_sha256"]))
            mode, policy_audit = _validate_runtime_mode(
                mechanics,
                arm=arm,
                epoch_id=epoch_id,
            )
            if arm == "control":
                control_mode_counts[mode] += 1
            else:
                candidate_mode_counts[mode] += 1
                if mode == "owner_policy":
                    _aggregate_policy_audit(
                        policy_aggregate,
                        policy_audit,
                        role=f"{epoch_id} candidate policy audit",
                    )
            expected_checkpoint_path = checkpoint_roots[arm] / f"{epoch_id}.json"
            ref = checkpoint_refs[arm]
            _require(isinstance(ref, Mapping), f"{epoch_id} {arm} checkpoint ref is invalid")
            checkpoint_path = _resolve_bound_path(
                ref.get("path"),
                expected=expected_checkpoint_path,
                role=f"{epoch_id} {arm} checkpoint",
            )
            checkpoint, checkpoint_file_sha256 = _load_admitted_canonical_json(
                checkpoint_path,
                marker_path=checkpoint_path.with_suffix(".success"),
                hash_field="checkpoint_sha256",
                schema_version=CHECKPOINT_SCHEMA_VERSION,
                role=f"{epoch_id} {arm} checkpoint",
            )
            _require(ref.get("file_sha256") == checkpoint_file_sha256, f"{epoch_id} {arm} checkpoint file hash drifted")
            _require(ref.get("checkpoint_sha256") == checkpoint["checkpoint_sha256"], f"{epoch_id} {arm} checkpoint ref drifted")
            _require(checkpoint.get("plan_identity_sha256") == adapter_identity, f"{epoch_id} {arm} checkpoint plan drifted")
            _require(checkpoint.get("epoch_id") == epoch_id, f"{epoch_id} {arm} checkpoint epoch drifted")
            _require(checkpoint.get("arm_id") == arm, f"{epoch_id} {arm} checkpoint arm drifted")
            _require(
                checkpoint.get("previous_checkpoint_sha256")
                == previous_checkpoint_sha256[arm],
                f"{epoch_id} {arm} checkpoint chain drifted",
            )
            _validate_checkpoint_semantics(
                checkpoint,
                receipt_mechanics=mechanics,
                arm=arm,
                epoch=epoch,
                ordinal=ordinal,
            )
            ledger = checkpoint["ledger_state"]
            _require(
                len(ledger["daily_slices"]) == cumulative_boundary_count,
                f"{epoch_id} {arm} accounting boundary count drifted",
            )
            previous_ledger_sequences[arm] = _validate_ledger_prefix(
                previous_ledger_sequences[arm],
                ledger,
                role=f"{epoch_id} {arm} ledger",
            )
            previous_checkpoint_sha256[arm] = str(checkpoint["checkpoint_sha256"])
            terminal_checkpoint_sha256[arm] = str(checkpoint["checkpoint_sha256"])
            terminal_checkpoints[arm] = checkpoint
        _require(len(arm_random_paths) == 1, f"{epoch_id} paired mechanics random paths differ")

    _require(len(all_boundaries) == EXPECTED_UTC_DAY_COUNT, "UTC boundary count drifted")
    _require(all_boundaries == sorted(set(all_boundaries)), "UTC boundary chain is not unique and ordered")
    expected_days = tuple(_utc_day_before_boundary(value) for value in all_boundaries)
    _require(len(set(expected_days)) == EXPECTED_UTC_DAY_COUNT, "UTC day labels are not unique")
    for prior, current in zip(expected_days, expected_days[1:], strict=False):
        _require(
            date.fromisoformat(current) == date.fromisoformat(prior) + timedelta(days=1),
            "UTC accounting calendar is not continuous",
        )
    _require(
        dict(control_mode_counts) == {"control": EXPECTED_EPOCH_COUNT},
        "control runtime mode denominator drifted",
    )
    _require(
        dict(candidate_mode_counts) == EXPECTED_CANDIDATE_MODE_COUNTS,
        "candidate owner-policy/fallback denominator drifted",
    )
    accounting, maximum_accounting_residual = _validate_terminal_accounting(
        terminal_checkpoints,
        expected_days=expected_days,
    )
    normalized_policy_audit = _normalize_policy_aggregate(policy_aggregate)
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "audit_passed": True,
        "read_only": True,
        "source_artifacts_modified": False,
        "accounting_values_read_for_reconciliation_only": True,
        "economic_conclusion_artifacts_read": False,
        "economic_effect_estimate_computed": False,
        "economic_conclusions_modified": False,
        "plan": {
            "path": str(resolved_plan),
            "file_sha256": plan_file_sha256,
            "plan_identity_sha256": plan["plan_identity_sha256"],
            "adapter_plan_identity_sha256": adapter_identity,
        },
        "execution": {
            "root": str(execution_root),
            "receipt_count": EXPECTED_EPOCH_COUNT,
            "first_epoch_id": expected_ids[0],
            "last_epoch_id": expected_ids[-1],
            "same_random_path_all_epochs": True,
            "quote_stop_and_zero_remaining_orders_all_epochs": True,
            "warmup_past_only_all_epochs": True,
            "strict_queue_authority_claim_count": 0,
            "receive_time_transport_authority_claim_count": 0,
        },
        "runtime_modes": {
            "control": dict(sorted(control_mode_counts.items())),
            "candidate": dict(sorted(candidate_mode_counts.items())),
        },
        "candidate_policy_audit": normalized_policy_audit,
        "accounting": {
            "utc_day_count": EXPECTED_UTC_DAY_COUNT,
            "first_utc_day": expected_days[0],
            "last_utc_day": expected_days[-1],
            "paired_dates_identical": True,
            "calendar_continuous": True,
            "tolerance_usdc": ABS_TOLERANCE_USDC,
            "maximum_abs_reconciliation_error_usdc": maximum_accounting_residual,
            "arms": accounting,
        },
        "terminal_checkpoint_sha256": terminal_checkpoint_sha256,
        "permissions": {field: False for field in PLAN_PERMISSION_FIELDS},
    }
    payload["audit_sha256"] = canonical_sha256(payload)
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def write_audit_atomically(
    output_path: Path,
    payload: Mapping[str, Any],
    *,
    plan_path: Path,
) -> Path:
    resolved_output = Path(output_path).expanduser().resolve()
    resolved_plan = Path(plan_path).expanduser().resolve()
    plan = _load_json_object(resolved_plan, role="owner restart-aware execution plan")
    execution_root = Path(str(plan.get("execution_output_root", ""))).expanduser().resolve()
    _require(resolved_output != resolved_plan, "audit output cannot replace the execution plan")
    _require(
        not resolved_output.is_relative_to(execution_root),
        "audit output cannot be written inside the execution artifact tree",
    )
    _atomic_json(resolved_output, payload)
    return resolved_output


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional audit JSON path; omitted output is written to stdout only.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        payload = audit_postrun(args.plan)
        if args.output is None:
            print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
        else:
            written = write_audit_atomically(args.output, payload, plan_path=args.plan)
            print(json.dumps({"audit_passed": True, "output": str(written)}, sort_keys=True))
    except PostrunAuditError as exc:
        print(json.dumps({"audit_passed": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    return 0


__all__ = [
    "ABS_TOLERANCE_USDC",
    "EXPECTED_CANDIDATE_MODE_COUNTS",
    "EXPECTED_EPOCH_COUNT",
    "EXPECTED_UTC_DAY_COUNT",
    "IDENTITY",
    "PostrunAuditError",
    "audit_postrun",
    "canonical_sha256",
    "main",
    "sha256_file",
    "write_audit_atomically",
]


if __name__ == "__main__":
    raise SystemExit(main())
