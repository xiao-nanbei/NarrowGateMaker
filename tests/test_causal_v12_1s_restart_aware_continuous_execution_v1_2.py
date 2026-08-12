from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from models.replay import restart_aware_continuous_execution as shared
from models.replay.replay_state_checkpoint import ContinuousReplayState
from models.replay.restart_aware_continuous_ab import (
    CalendarSourceBinding,
    ContinuousABPlan,
    FrozenRestartInterval,
    SourceArtifactBinding,
    canonical_sha256,
    day_start_ms,
    ordered_days,
    restart_timeline_payload,
    source_artifact_manifest_payload,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_2 as subject,
)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _plan(tmp_path: Path) -> ContinuousABPlan:
    payload_path = tmp_path / "source.parquet"
    payload_path.write_bytes(b"source")
    artifact_sha = subject.sha256_file(payload_path)
    days = ordered_days(subject.parent.START_DAY, subject.parent.END_DAY)
    bindings = []
    for ordinal, day in enumerate(days):
        native = ordinal < 52
        bindings.append(
            CalendarSourceBinding(
                day=day,
                book_identity=("native_available" if native else "provider_normalized_sensitivity"),
                book_root=str(tmp_path),
                bbo_path=str(payload_path),
                l2_path=str(payload_path),
                feature_identity="test-feature",
                feature_path=str(payload_path),
                artifacts=tuple(
                    SourceArtifactBinding(
                        role=role,
                        path=str(payload_path),
                        size_bytes=payload_path.stat().st_size,
                        sha256=artifact_sha,
                    )
                    for role in ("bbo", "l2", "feature")
                ),
                exact_queue_authority=native,
                exact_lifecycle_authority=native,
            )
        )
    start = day_start_ms(days[0])
    second = day_start_ms(days[1])
    intervals = (
        FrozenRestartInterval(
            gap_id="initial",
            offline_start_ts_ms=start,
            resume_ts_ms=start + 1_000,
            quote_stop_ts_ms=None,
            cancel_deadline_ts_ms=None,
            warmup_lookback_start_ts_ms=start - 300_000,
        ),
        FrozenRestartInterval(
            gap_id="planned",
            offline_start_ts_ms=second + 20_000,
            resume_ts_ms=second + 30_000,
            quote_stop_ts_ms=second + 18_000,
            cancel_deadline_ts_ms=second + 19_999,
            warmup_lookback_start_ts_ms=second - 270_000,
        ),
    )
    timeline = restart_timeline_payload(calendar_days=days, intervals=intervals)
    plan = ContinuousABPlan(
        calendar_start_day=days[0],
        calendar_end_day=days[-1],
        calendar_days=days,
        source_bindings=tuple(bindings),
        restart_intervals=intervals,
        source_calendar_manifest_path=str(tmp_path / "calendar.json"),
        source_calendar_manifest_sha256="a" * 64,
        source_artifact_manifest_sha256=canonical_sha256(
            source_artifact_manifest_payload(bindings)
        ),
        restart_timeline_sha256=canonical_sha256(timeline),
    )
    plan.validate()
    return plan


def _policy_manifest(tmp_path: Path, plan: ContinuousABPlan, *, arm: str) -> Path:
    artifact = tmp_path / f"{arm}.bin"
    artifact.write_bytes(arm.encode("ascii"))
    binding = {
        "path": str(artifact),
        "sha256": subject.sha256_file(artifact),
        "size_bytes": artifact.stat().st_size,
    }
    shared_artifact = tmp_path / "shared-policy-inputs.bin"
    shared_artifact.write_bytes(b"shared-policy-inputs")
    shared_binding = {
        "path": str(shared_artifact),
        "sha256": subject.sha256_file(shared_artifact),
        "size_bytes": shared_artifact.stat().st_size,
    }
    native_days = [row.day for row in plan.source_bindings if row.exact_queue_authority]
    provider_days = [row.day for row in plan.source_bindings if not row.exact_queue_authority]
    payload = {
        "schema_version": subject.POLICY_ARTIFACT_SCHEMA_VERSION,
        "arm": arm,
        "identity": f"test-{arm}",
        "baseline_id": subject.parent.one_second_replay.EXPECTED_BASELINE_ID,
        "cadence_ms": 10_000 if arm == "control" else 1_000,
        "ml_enabled": True,
        "q90_action_enabled": False,
        "buy_fill_selection_enabled": False,
        "bundle_meta": binding,
        "feature_dag": binding,
        "operational_config": shared_binding,
        "baseline_identity": shared_binding,
        "initial_state": shared_binding,
        "latency_profile": shared_binding,
        "engine_state_schema": shared_binding,
        "execution_abi": "test-stateful-continuous-abi.v1",
        "primary_native_40day_receipt": binding if arm == "candidate" else None,
        "overlay_indices": [binding],
        "native_days": native_days,
        "provider_days": provider_days,
        "days": {
            row.day: {
                "overlay_manifest": binding,
                "overlay_data": binding,
                "overlay_identity_sha256": canonical_sha256({"arm": arm, "day": row.day}),
                "source_profile": (
                    "native" if row.exact_queue_authority else "provider_normalized"
                ),
            }
            for row in plan.source_bindings
        },
    }
    path = tmp_path / f"{arm}-policy.json"
    _write_json(path, payload)
    return path


class FakeAdapter:
    def __init__(
        self,
        *,
        gap_fill: bool = False,
        utc_reset: bool = False,
        drain_terminal_fill: bool = False,
    ) -> None:
        self.gap_fill = gap_fill
        self.utc_reset = utc_reset
        self.drain_terminal_fill = drain_terminal_fill

    def initialize_arm(
        self,
        *,
        arm_id: str,
        policy_artifacts: dict[str, Any],
        first_operation: shared.ContinuousOperation,
    ) -> shared.ArmCheckpoint:
        state = ContinuousReplayState(
            arm_id=arm_id,
            checkpoint_ts_ms=first_operation.start_ts_ms,
            cash_usdc=0.0,
            position_btc=0.0,
            average_entry_price=0.0,
            cumulative_realized_pnl_usdc=0.0,
            cumulative_fees_usdc=0.0,
            equity_anchor_usdc=0.0,
            last_mark_price=60_000.0,
            cumulative_pnl_usdc=0.0,
            feature_warmup_ready=False,
            quoting_enabled=False,
        )
        engine_state = {"initialized": True, "policy": policy_artifacts["identity"]}
        return shared.ArmCheckpoint(
            arm_id=arm_id,
            operation_sequence=0,
            operation_id="initial",
            state=state,
            engine_state=engine_state,
            engine_state_sha256=canonical_sha256(engine_state),
            previous_checkpoint_sha256="",
        )

    def execute_operation(
        self,
        *,
        operation: shared.ContinuousOperation,
        checkpoint: shared.ArmCheckpoint,
        policy_artifacts: dict[str, Any],
    ) -> shared.OperationResult:
        before = checkpoint.state
        reset = False
        panel_mtm = False
        quotes = 0
        fills = 0
        terminal_fills = 0
        cancels = 0
        acks = 0
        if operation.kind == "online":
            state = replace(
                before,
                checkpoint_ts_ms=operation.end_ts_ms,
                feature_warmup_ready=True,
                quoting_enabled=True,
            )
            quotes = 1
        elif operation.kind == "cancel_drain":
            state = before.for_planned_restart(operation.end_ts_ms)
            reset = True
            cancels = 1
            if self.drain_terminal_fill:
                fills = 1
                terminal_fills = 1
            else:
                acks = 1
        elif operation.kind == "offline_gap":
            state = before.with_mark(operation.end_ts_ms, before.last_mark_price)
            fills = int(self.gap_fill)
        elif operation.kind == "warmup_resume":
            state = ContinuousReplayState.from_dict(
                {
                    **before.to_dict(),
                    "checkpoint_ts_ms": operation.end_ts_ms,
                    "feature_warmup_ready": True,
                    "quoting_enabled": True,
                }
            )
        elif operation.kind == "utc_accounting":
            state = before.with_mark(operation.end_ts_ms, before.last_mark_price)
            if self.utc_reset:
                state = replace(state, restart_generation=state.restart_generation + 1)
        else:
            state = ContinuousReplayState.from_dict(
                {
                    **before.to_dict(),
                    "checkpoint_ts_ms": operation.end_ts_ms,
                    "orders_terminal": True,
                    "active_order_count": 0,
                    "pending_cancel_count": 0,
                    "queue_cursor_count": 0,
                    "q90_cursor_count": 0,
                    "feature_warmup_ready": False,
                    "quoting_enabled": False,
                }
            )
            panel_mtm = True
        engine_state = {
            **dict(checkpoint.engine_state),
            "last_operation": operation.operation_id,
            "policy": policy_artifacts["identity"],
        }
        return shared.OperationResult(
            operation_id=operation.operation_id,
            random_path_sha256=operation.random_path_sha256,
            state=state,
            engine_state=engine_state,
            quote_count=quotes,
            fill_count=fills,
            terminal_fill_count=terminal_fills,
            cancel_request_count=cancels,
            cancel_ack_count=acks,
            cancel_reject_count=0,
            runtime_reset_applied=reset,
            panel_terminal_mtm_applied=panel_mtm,
        )


def _patch_frozen_plan(monkeypatch: pytest.MonkeyPatch, plan: ContinuousABPlan) -> None:
    spec = {
        "schema_version": subject.parent.SCHEMA_VERSION,
        "identity": "test-parent",
        "calendar": {"days": list(plan.calendar_days)},
    }
    amendment = {
        "schema_version": subject.parent.AMENDMENT_SCHEMA_VERSION,
        "identity": "test-amendment",
    }
    monkeypatch.setattr(subject, "_build_frozen_plan", lambda **_: (plan, spec, amendment))


def test_operation_tape_preserves_midnight_and_authority_tiers(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    rows = shared.build_continuous_operations(plan, base_seed=subject.BASE_SEED)
    assert sum(row.kind == "utc_accounting" for row in rows) == 71
    assert sum(row.kind == "panel_terminal" for row in rows) == 1
    assert sum(row.kind == "offline_gap" for row in rows) == 2
    assert all(not row.exact_queue_authority for row in rows if row.kind != "online")
    assert all(
        not row.exact_lifecycle_authority
        for row in rows
        if row.kind not in {"online", "cancel_drain"}
    )
    provider = {row.day for row in plan.source_bindings if not row.exact_queue_authority}
    assert all(
        not row.exact_queue_authority and not row.exact_lifecycle_authority
        for row in rows
        if row.day in provider
    )
    assert all(a.end_ts_ms <= b.start_ts_ms for a, b in zip(rows, rows[1:], strict=False))


def test_cancel_drain_inside_previous_gap_never_overlaps(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    first = plan.restart_intervals[0]
    close_following = FrozenRestartInterval(
        gap_id="close-following",
        offline_start_ts_ms=first.resume_ts_ms + 1_000,
        resume_ts_ms=first.resume_ts_ms + 2_000,
        quote_stop_ts_ms=first.resume_ts_ms - 1_000,
        cancel_deadline_ts_ms=first.resume_ts_ms + 999,
        warmup_lookback_start_ts_ms=first.resume_ts_ms - 300_000,
    )
    intervals = (first, close_following, plan.restart_intervals[1])
    repaired = replace(
        plan,
        restart_intervals=intervals,
        restart_timeline_sha256=canonical_sha256(
            restart_timeline_payload(calendar_days=plan.calendar_days, intervals=intervals)
        ),
    )
    rows = shared.build_continuous_operations(repaired, base_seed=subject.BASE_SEED)
    assert all(a.end_ts_ms <= b.start_ts_ms for a, b in zip(rows, rows[1:], strict=False))
    drain = next(
        row for row in rows if row.gap_id == "close-following" and row.kind == "cancel_drain"
    )
    assert drain.start_ts_ms == first.resume_ts_ms


def test_restart_crossing_midnight_keeps_prior_day_cancel_drain(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    third_day = day_start_ms(plan.calendar_days[2])
    cross_midnight = FrozenRestartInterval(
        gap_id="cross-midnight",
        offline_start_ts_ms=third_day,
        resume_ts_ms=third_day + 1_000,
        quote_stop_ts_ms=third_day - 2_000,
        cancel_deadline_ts_ms=third_day - 1,
        warmup_lookback_start_ts_ms=third_day - 300_000,
    )
    intervals = (*plan.restart_intervals, cross_midnight)
    intervals = tuple(sorted(intervals, key=lambda row: row.offline_start_ts_ms))
    repaired = replace(
        plan,
        restart_intervals=intervals,
        restart_timeline_sha256=canonical_sha256(
            restart_timeline_payload(calendar_days=plan.calendar_days, intervals=intervals)
        ),
    )
    rows = shared.build_continuous_operations(repaired, base_seed=subject.BASE_SEED)
    drain = next(
        row for row in rows if row.gap_id == "cross-midnight" and row.kind == "cancel_drain"
    )
    gap = next(row for row in rows if row.gap_id == "cross-midnight" and row.kind == "offline_gap")
    assert drain.day == plan.calendar_days[1]
    assert drain.end_ts_ms == third_day
    assert gap.day == plan.calendar_days[2]
    assert gap.start_ts_ms == third_day


def test_prepare_and_validate_remain_outcome_blind_when_artifacts_unbound(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plan = _plan(tmp_path)
    _patch_frozen_plan(monkeypatch, plan)
    output = tmp_path / "output"
    payload = subject.prepare_execution_plan(output_root=output, allow_test_root=True)
    assert payload["execution_eligible"] is False
    assert payload["blockers"] == [
        "control_71_day_policy_artifacts_unbound",
        "candidate_71_day_policy_artifacts_unbound",
    ]
    assert payload["economic_outcomes_read"] is False
    assert payload["promotion_authorized"] is False
    validated = subject.validate_execution_plan(output / subject.PLAN_FILENAME)
    assert validated["plan_identity_sha256"] == payload["plan_identity_sha256"]


def test_unbound_run_fails_before_adapter_or_outcome_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plan = _plan(tmp_path)
    _patch_frozen_plan(monkeypatch, plan)
    output = tmp_path / "output"
    subject.prepare_execution_plan(output_root=output, allow_test_root=True)
    with pytest.raises(subject.F03ContinuousExecutionError, match="actual run blocked"):
        subject.run_prepared_plan(output / subject.PLAN_FILENAME, adapter=FakeAdapter())


def test_bound_plan_executes_and_atomically_resumes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plan = _plan(tmp_path)
    _patch_frozen_plan(monkeypatch, plan)
    control = _policy_manifest(tmp_path, plan, arm="control")
    candidate = _policy_manifest(tmp_path, plan, arm="candidate")
    output = tmp_path / "output"
    payload = subject.prepare_execution_plan(
        output_root=output,
        control_artifacts_path=control,
        candidate_artifacts_path=candidate,
        allow_test_root=True,
    )
    assert payload["execution_eligible"] is True
    operations = tuple(shared.ContinuousOperation(**row) for row in payload["operations"])
    policies = payload["policy_artifacts"]
    first = shared.execute_continuous_plan(
        plan_identity_sha256=payload["plan_identity_sha256"],
        operations=operations,
        policy_artifacts=policies,
        output_root=output,
        adapter=FakeAdapter(),
        max_operations=12,
        allow_test_output_root=True,
    )
    assert first["completed_operation_sequence"] == 12
    final = shared.execute_continuous_plan(
        plan_identity_sha256=payload["plan_identity_sha256"],
        operations=operations,
        policy_artifacts=policies,
        output_root=output,
        adapter=FakeAdapter(),
        allow_test_output_root=True,
    )
    assert final["completed_operation_sequence"] == len(operations)
    assert final["panel_complete"] is True
    assert final["economic_results_aggregated"] is False
    last = output / "operations" / f"{len(operations):05d}"
    assert (last / "_SUCCESS").is_file()
    assert (last / "checkpoint-control.json").is_file()
    assert (last / "checkpoint-candidate.json").is_file()


def test_gap_trading_and_midnight_reset_fail_closed(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    operations = shared.build_continuous_operations(plan, base_seed=subject.BASE_SEED)
    policies = {
        "control": {"identity": "control"},
        "candidate": {"identity": "candidate"},
    }
    with pytest.raises(shared.ContinuousExecutionError, match="offline_gap produced a fill"):
        shared.execute_continuous_plan(
            plan_identity_sha256="b" * 64,
            operations=operations,
            policy_artifacts=policies,
            output_root=tmp_path / "gap-output",
            adapter=FakeAdapter(gap_fill=True),
            allow_test_output_root=True,
        )
    with pytest.raises(shared.ContinuousExecutionError, match="UTC midnight changed"):
        shared.execute_continuous_plan(
            plan_identity_sha256="c" * 64,
            operations=operations,
            policy_artifacts=policies,
            output_root=tmp_path / "utc-output",
            adapter=FakeAdapter(utc_reset=True),
            allow_test_output_root=True,
        )


def test_cancel_drain_accepts_terminal_fill_race(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    operations = shared.build_continuous_operations(plan, base_seed=subject.BASE_SEED)
    drain_sequence = next(row.sequence for row in operations if row.kind == "cancel_drain")
    result = shared.execute_continuous_plan(
        plan_identity_sha256="e" * 64,
        operations=operations,
        policy_artifacts={
            "control": {"identity": "control"},
            "candidate": {"identity": "candidate"},
        },
        output_root=tmp_path / "drain-fill-output",
        adapter=FakeAdapter(drain_terminal_fill=True),
        max_operations=drain_sequence,
        allow_test_output_root=True,
    )
    assert result["completed_operation_sequence"] == drain_sequence
    receipt = json.loads(
        (
            tmp_path / "drain-fill-output" / "operations" / f"{drain_sequence:05d}" / "receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["arms"]["control"]["terminal_fill_count"] == 1
    assert receipt["arms"]["control"]["cancel_ack_count"] == 0


def test_policy_manifest_rejects_action_and_source_profile_drift(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    manifest = _policy_manifest(tmp_path, plan, arm="control")
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["q90_action_enabled"] = True
    _write_json(manifest, payload)
    with pytest.raises(subject.F03ContinuousExecutionError, match="q90-OFF"):
        subject.load_policy_artifacts(manifest, expected_arm="control")


def test_checkpoint_tamper_is_detected_on_resume(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    operations = shared.build_continuous_operations(plan, base_seed=subject.BASE_SEED)
    policies = {
        "control": {"identity": "control"},
        "candidate": {"identity": "candidate"},
    }
    output = tmp_path / "output"
    shared.execute_continuous_plan(
        plan_identity_sha256="d" * 64,
        operations=operations,
        policy_artifacts=policies,
        output_root=output,
        adapter=FakeAdapter(),
        max_operations=3,
        allow_test_output_root=True,
    )
    checkpoint = output / "operations" / "00003" / "checkpoint-control.json"
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    payload["engine_state"]["tampered"] = True
    _write_json(checkpoint, payload)
    with pytest.raises(shared.ContinuousExecutionError, match="checkpoint hash mismatch"):
        shared.execute_continuous_plan(
            plan_identity_sha256="d" * 64,
            operations=operations,
            policy_artifacts=policies,
            output_root=output,
            adapter=FakeAdapter(),
            allow_test_output_root=True,
        )
