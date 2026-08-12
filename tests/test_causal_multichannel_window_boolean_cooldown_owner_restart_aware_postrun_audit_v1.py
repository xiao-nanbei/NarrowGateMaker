from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_restart_aware_postrun_audit_v1 as audit,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_admitted(
    path: Path,
    payload: dict[str, Any],
    *,
    hash_field: str,
    marker_path: Path | None = None,
) -> dict[str, Any]:
    value = dict(payload)
    value[hash_field] = audit.canonical_sha256(value)
    _write_json(path, value)
    marker = marker_path or path.with_suffix(".success")
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(audit.sha256_file(path) + "\n", encoding="ascii")
    return value


def _epoch_bounds(base: datetime, ordinal: int) -> tuple[int, int]:
    total_ms = audit.EXPECTED_UTC_DAY_COUNT * 86_400_000
    start = int(base.timestamp() * 1_000) + (ordinal - 1) * total_ms // audit.EXPECTED_EPOCH_COUNT
    end = int(base.timestamp() * 1_000) + ordinal * total_ms // audit.EXPECTED_EPOCH_COUNT
    return start, end


def _daily_slices(
    base: datetime,
    count: int,
    *,
    pnl_per_day: float,
    discontinuous_after_first_day: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    equity = 0.0
    for offset in range(count):
        start = equity
        if discontinuous_after_first_day and offset == 1:
            start += 0.01
        equity += pnl_per_day
        if discontinuous_after_first_day and offset == 1:
            equity += 0.01
        rows.append(
            {
                "day": (base.date() + timedelta(days=offset)).isoformat(),
                "start_equity_usdc": start,
                "end_equity_usdc": equity,
                "pnl_usdc": pnl_per_day,
                "start_inventory_btc": 0.0,
                "end_inventory_btc": 0.0,
                "end_mark_price": 60_000.0,
            }
        )
    return rows


def _mechanics(
    *,
    arm: str,
    epoch: dict[str, Any],
    owner_policy: bool,
) -> dict[str, Any]:
    if arm == "control":
        owner_runtime = {
            "mode": "control",
            "candidate_effective_policy": "control",
            "missing_m2_control_fallback": False,
            "policy_audit": {},
            "emitter_audit": {},
            "support_binding": {
                "supported": True,
                "reason": "control_arm_does_not_require_m2",
            },
        }
    elif owner_policy:
        owner_runtime = {
            "mode": "owner_policy",
            "candidate_effective_policy": "owner_boolean_cooldown",
            "missing_m2_control_fallback": False,
            "policy_audit": {
                "action_counts": {"CONTROL_85N": 1, "FIXED_211S": 1},
                "duration_ms_max": 211_000,
                "duration_ms_sum": 296_000,
                "evaluations": 2,
                "fallback": 1,
                "nonbaseline": 1,
                "supported": 2,
            },
            "emitter_audit": {"economic_outcomes_read": False},
            "support_binding": {
                "supported": True,
                "exact_queue_authority": False,
                "receive_time_transport_authority": False,
            },
        }
    else:
        owner_runtime = {
            "mode": "missing_m2_control_fallback",
            "candidate_effective_policy": "control",
            "missing_m2_control_fallback": True,
            "policy_audit": {},
            "emitter_audit": {},
            "support_binding": {
                "supported": False,
                "reason": "missing_epoch_m2_cache",
            },
        }
    return {
        "epoch_id": epoch["epoch_id"],
        "arm": arm,
        "engine": "python",
        "actual_tick_replay_used": True,
        "quote_count": 10,
        "fill_count": 1,
        "terminal_fill_count": 0,
        "cancel_request_count": 2,
        "cancel_ack_count": 10,
        "planned_quote_stop_triggered": True,
        "planned_shutdown_remaining_orders": 0,
        "planned_restart_flattens_inventory": False,
        "utc_midnight_preserves_cooldown_and_ema": True,
        "warmup_lookback_start_ts_ms": epoch["warmup_lookback_start_ts_ms"],
        "quote_resume_ts_ms": epoch["start_ts_ms"],
        "warmup": {
            "required": True,
            "market_event_count": 10,
            "prediction_row_count": 3,
            "latest_prediction_ready_ts_ms": epoch["start_ts_ms"] - 1,
            "feature_ready_not_after_resume": True,
            "quoting_enabled": False,
            "source_identity_sha256": "a" * 64,
        },
        "random_seed": epoch["random_seed"],
        "random_path_sha256": epoch["random_path_sha256"],
        "policy_identity_sha256": "b" * 64,
        "cadence_ms": 10_000,
        "source_authority": [
            {
                "source_profile": "provider_normalized",
                "exact_queue_authority": False,
                "exact_lifecycle_authority": False,
            }
        ],
        "provider_exact_queue_claim_count": 0,
        "provider_exact_lifecycle_claim_count": 0,
        "economic_outputs_read": False,
        "strict_queue_authority": False,
        "receive_time_transport_authority": False,
        "action_authorized": False,
        "live_authorized": False,
        "offline_gap": {
            "gap_id": epoch["gap_id"],
            "start_ts_ms": epoch["end_ts_ms"],
            "end_ts_ms": epoch["gap_end_ts_ms"],
            "market_event_trading_enabled": False,
            "fill_count": 0,
            "utc_boundary_count": len(epoch["utc_boundaries_ts_ms"]),
            "inventory_unchanged": True,
            "cash_unchanged": True,
        },
        "owner_runtime": owner_runtime,
    }


def _checkpoint(
    *,
    arm: str,
    epoch: dict[str, Any],
    ordinal: int,
    previous_sha256: str,
    mechanics: dict[str, Any],
    daily_slices: list[dict[str, Any]],
) -> dict[str, Any]:
    cumulative = sum(float(row["pnl_usdc"]) for row in daily_slices)
    state = {
        "schema_version": "continuous_replay_state.v1",
        "arm_id": arm,
        "checkpoint_ts_ms": epoch["gap_end_ts_ms"],
        "cash_usdc": cumulative,
        "position_btc": 0.0,
        "average_entry_price": 0.0,
        "last_mark_price": 60_000.0,
        "cumulative_realized_pnl_usdc": cumulative,
        "cumulative_fees_usdc": 0.0,
        "cumulative_pnl_usdc": cumulative,
        "equity_anchor_usdc": 0.0,
        "economic_campaign": None,
        "active_order_count": 0,
        "pending_cancel_count": 0,
        "queue_cursor_count": 0,
        "q90_cursor_count": 0,
        "orders_terminal": True,
        "quoting_enabled": False,
        "feature_warmup_ready": False,
        "restart_generation": ordinal,
        "runtime_reset_fields": ["orders", "queue"],
    }
    ledger = {
        "state": state,
        "day_start_equity_usdc": cumulative,
        "day_start_inventory_btc": 0.0,
        "day_start": daily_slices[-1]["day"] if daily_slices else "2026-01-01",
        "daily_slices": daily_slices,
        "closed_campaigns": [],
        "gap_carries": [],
    }
    ledger_sha = audit.canonical_sha256(ledger)
    engine = {
        "schema_version": "narrowgate_authoritative_continuous_tick_adapter.v1",
        "checkpoint_boundary": "post_cancel_ack_drain",
        "active_orders": [],
        "pending_new_orders": [],
        "pending_cancel_orders": [],
        "queue_positions": [],
        "queue_cursors": [],
        "q90_cursors": [],
        "cooldown_lineages": {},
        "campaign_reward_path": None,
        "feature_model_held_state": {
            "cleared_by_production_restart": True,
            "warmup_required_before_next_quote": not epoch["terminal"],
        },
        "rng_state": {
            "algorithm": "operation_keyed_seed_v1",
            "completed_epoch_random_path_sha256": epoch["random_path_sha256"],
        },
        "accounting_state_sha256": ledger_sha,
        "runtime_reset_fields": ["orders", "queue"],
    }
    return {
        "schema_version": audit.CHECKPOINT_SCHEMA_VERSION,
        "plan_identity_sha256": "c" * 64,
        "arm_id": arm,
        "epoch_id": epoch["epoch_id"],
        "state": state,
        "state_sha256": audit.canonical_sha256(state),
        "ledger_state": ledger,
        "ledger_state_sha256": ledger_sha,
        "engine_state": engine,
        "engine_state_sha256": audit.canonical_sha256(engine),
        "mechanics_sha256": audit.canonical_sha256(mechanics),
        "previous_checkpoint_sha256": previous_sha256,
        "economic_outcomes_read": False,
        "promotion_authorized": False,
    }


def _build_run(
    tmp_path: Path,
    *,
    discontinuous_after_first_day: bool = False,
) -> tuple[Path, Path]:
    root = tmp_path / "study"
    execution_root = root / "execution" / ("c" * 64)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    previous = {arm: "" for arm in audit.ARMS}
    boundary_count = 0
    total_end = int((base + timedelta(days=audit.EXPECTED_UTC_DAY_COUNT)).timestamp() * 1_000)
    for ordinal in range(1, audit.EXPECTED_EPOCH_COUNT + 1):
        epoch_id = f"epoch-{ordinal:04d}"
        start_ts_ms, end_ts_ms = _epoch_bounds(base, ordinal)
        boundaries = []
        while boundary_count < audit.EXPECTED_UTC_DAY_COUNT:
            boundary = int((base + timedelta(days=boundary_count + 1)).timestamp() * 1_000)
            if boundary > end_ts_ms:
                break
            if boundary > start_ts_ms:
                boundaries.append(boundary)
                boundary_count += 1
            else:
                boundary_count += 1
        epoch = {
            "epoch_id": epoch_id,
            "start_ts_ms": start_ts_ms,
            "quote_stop_ts_ms": end_ts_ms - 2_000,
            "end_ts_ms": end_ts_ms,
            "gap_end_ts_ms": end_ts_ms,
            "gap_id": f"gap-{ordinal:04d}",
            "warmup_lookback_start_ts_ms": start_ts_ms - 3_600_000,
            "source_days": [],
            "utc_boundaries_ts_ms": boundaries,
            "random_seed": ordinal,
            "random_path_sha256": f"{ordinal:064x}",
            "terminal": ordinal == audit.EXPECTED_EPOCH_COUNT,
        }
        slices = _daily_slices(
            base,
            boundary_count,
            pnl_per_day=1.0,
            discontinuous_after_first_day=discontinuous_after_first_day,
        )
        checkpoint_refs: dict[str, dict[str, str]] = {}
        mechanics_by_arm: dict[str, dict[str, Any]] = {}
        for arm in audit.ARMS:
            mechanics = _mechanics(
                arm=arm,
                epoch=epoch,
                owner_policy=ordinal <= 60,
            )
            mechanics_by_arm[arm] = mechanics
            checkpoint = _checkpoint(
                arm=arm,
                epoch=epoch,
                ordinal=ordinal,
                previous_sha256=previous[arm],
                mechanics=mechanics,
                daily_slices=slices,
            )
            checkpoint_path = execution_root / "checkpoints" / arm / f"{epoch_id}.json"
            checkpoint = _write_admitted(
                checkpoint_path,
                checkpoint,
                hash_field="checkpoint_sha256",
            )
            previous[arm] = checkpoint["checkpoint_sha256"]
            checkpoint_refs[arm] = {
                "path": str(checkpoint_path),
                "file_sha256": audit.sha256_file(checkpoint_path),
                "checkpoint_sha256": checkpoint["checkpoint_sha256"],
            }
        receipt = {
            "schema_version": audit.RECEIPT_SCHEMA_VERSION,
            "plan_identity_sha256": "c" * 64,
            "epoch": epoch,
            "arms": mechanics_by_arm,
            "checkpoints": checkpoint_refs,
            "same_random_path": True,
            "economic_outcomes_read": False,
            "promotion_authorized": False,
        }
        _write_admitted(
            execution_root / "receipts" / f"{epoch_id}.json",
            receipt,
            hash_field="receipt_sha256",
        )
    assert boundary_count == audit.EXPECTED_UTC_DAY_COUNT
    assert end_ts_ms == total_end
    plan = {
        "schema_version": audit.PLAN_SCHEMA_VERSION,
        "identity": audit.PLAN_IDENTITY,
        "adapter_plan_identity_sha256": "c" * 64,
        "execution_output_root": str(execution_root),
        "framework": {
            "authoritative_epoch_count": audit.EXPECTED_EPOCH_COUNT,
            "utc_accounting_boundary_count": audit.EXPECTED_UTC_DAY_COUNT,
        },
        "execution_eligible": True,
        "blockers": [],
        "economic_outcomes_read": False,
        "economic_results_aggregated": False,
        "permissions": {field: False for field in audit.PLAN_PERMISSION_FIELDS},
    }
    plan_path = root / "execution-plan.json"
    _write_admitted(
        plan_path,
        plan,
        hash_field="plan_identity_sha256",
        marker_path=root / audit.PLAN_MARKER_NAME,
    )
    return plan_path, execution_root


def _rewrite_checkpoint_and_chain(
    plan_path: Path,
    execution_root: Path,
    *,
    epoch_id: str,
    arm: str,
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    checkpoint_path = execution_root / "checkpoints" / arm / f"{epoch_id}.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint.pop("checkpoint_sha256")
    mutate(checkpoint)
    checkpoint = _write_admitted(
        checkpoint_path,
        checkpoint,
        hash_field="checkpoint_sha256",
    )
    receipt_path = execution_root / "receipts" / f"{epoch_id}.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt.pop("receipt_sha256")
    receipt["checkpoints"][arm] = {
        "path": str(checkpoint_path),
        "file_sha256": audit.sha256_file(checkpoint_path),
        "checkpoint_sha256": checkpoint["checkpoint_sha256"],
    }
    _write_admitted(receipt_path, receipt, hash_field="receipt_sha256")
    assert plan_path.is_file()


def test_postrun_audit_accepts_complete_chain_and_aggregates_nested_actions(
    tmp_path: Path,
) -> None:
    plan_path, _ = _build_run(tmp_path)

    result = audit.audit_postrun(plan_path)

    assert result["audit_passed"] is True
    assert result["execution"]["receipt_count"] == 104
    assert result["accounting"]["utc_day_count"] == 71
    assert result["runtime_modes"]["candidate"] == {
        "missing_m2_control_fallback": 44,
        "owner_policy": 60,
    }
    assert result["candidate_policy_audit"]["action_counts"] == {
        "CONTROL_85N": 60,
        "FIXED_211S": 60,
    }
    assert result["candidate_policy_audit"]["duration_ms_max"] == 211_000
    assert result["candidate_policy_audit"]["duration_ms_sum"] == 17_760_000
    assert result["accounting"]["maximum_abs_reconciliation_error_usdc"] == 0.0
    normalized = dict(result)
    expected = normalized.pop("audit_sha256")
    assert audit.canonical_sha256(normalized) == expected


def test_postrun_audit_writes_optional_output_atomically_outside_execution_tree(
    tmp_path: Path,
) -> None:
    plan_path, execution_root = _build_run(tmp_path)
    result = audit.audit_postrun(plan_path)
    output = tmp_path / "audit" / "postrun.json"

    assert audit.write_audit_atomically(output, result, plan_path=plan_path) == output
    assert json.loads(output.read_text(encoding="utf-8")) == result
    assert not list(output.parent.glob("*.partial"))
    with pytest.raises(audit.PostrunAuditError, match="execution artifact tree"):
        audit.write_audit_atomically(
            execution_root / "audit.json",
            result,
            plan_path=plan_path,
        )


def test_postrun_audit_cli_defaults_to_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    plan_path, _ = _build_run(tmp_path)

    assert audit.main(["--plan", str(plan_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["audit_passed"] is True
    assert payload["economic_effect_estimate_computed"] is False


def test_postrun_audit_rejects_semantic_state_hash_drift_even_when_readmission_is_valid(
    tmp_path: Path,
) -> None:
    plan_path, execution_root = _build_run(tmp_path)

    def mutate(checkpoint: dict[str, Any]) -> None:
        checkpoint["state"]["cash_usdc"] += 1.0

    _rewrite_checkpoint_and_chain(
        plan_path,
        execution_root,
        epoch_id="epoch-0104",
        arm="candidate",
        mutate=mutate,
    )

    with pytest.raises(audit.PostrunAuditError, match="state hash drifted"):
        audit.audit_postrun(plan_path)


def test_postrun_audit_rejects_mechanics_hash_drift(
    tmp_path: Path,
) -> None:
    plan_path, execution_root = _build_run(tmp_path)
    receipt_path = execution_root / "receipts" / "epoch-0104.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt.pop("receipt_sha256")
    receipt["arms"]["candidate"]["quote_count"] += 1
    _write_admitted(receipt_path, receipt, hash_field="receipt_sha256")

    with pytest.raises(audit.PostrunAuditError, match="mechanics hash drifted"):
        audit.audit_postrun(plan_path)


def test_postrun_audit_rejects_daily_equity_discontinuity(
    tmp_path: Path,
) -> None:
    plan_path, _ = _build_run(
        tmp_path,
        discontinuous_after_first_day=True,
    )

    with pytest.raises(audit.PostrunAuditError, match="inter-day equity is discontinuous"):
        audit.audit_postrun(plan_path)


def test_postrun_audit_rejects_authority_escalation(
    tmp_path: Path,
) -> None:
    plan_path, execution_root = _build_run(tmp_path)
    receipt_path = execution_root / "receipts" / "epoch-0001.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt.pop("receipt_sha256")
    receipt["arms"]["candidate"]["live_authorized"] = True
    _write_admitted(receipt_path, receipt, hash_field="receipt_sha256")

    with pytest.raises(audit.PostrunAuditError, match="granted live_authorized"):
        audit.audit_postrun(plan_path)
