from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_repeated_policy_v1 as bridge,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_successor_transport_adapter_v1 as transport,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
    duration_vocabulary,
)


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _candidate_policy(side: str = "SELL") -> BooleanCooldownPolicy:
    return BooleanCooldownPolicy(
        side=side,
        rules=(
            BooleanRule(
                action=duration_vocabulary(side)[1],
                clauses=(
                    AndClause(
                        literals=(TriLiteral(predicate="predicate::candidate"),)
                    ),
                ),
            ),
        ),
    )


def _artifact_binding(
    *,
    scope: bridge.ExecutedArtifactScope = (
        bridge.ExecutedArtifactScope.LEARNING_ALGORITHM_FOLD_POLICY
    ),
) -> bridge.ArtifactIdentityBinding:
    if scope == bridge.ExecutedArtifactScope.LEARNING_ALGORITHM_FOLD_POLICY:
        return bridge.ArtifactIdentityBinding(
            executed_artifact_scope=scope,
            executed_policy_identity="successor_outer_fold_1_sell_policy",
            executed_policy_sha256="6" * 64,
            executed_predicate_bundle_sha256="7" * 64,
            learning_algorithm_identity="successor_outer_fold_1_sell_policy",
            learning_algorithm_artifact_sha256="8" * 64,
        )
    return bridge.ArtifactIdentityBinding(
        executed_artifact_scope=scope,
        executed_policy_identity="successor_final_refit_sell_policy",
        executed_policy_sha256="a" * 64,
        executed_predicate_bundle_sha256="b" * 64,
        learning_algorithm_identity="successor_learning_algorithm",
        learning_algorithm_artifact_sha256="c" * 64,
        final_artifact_identity="successor_final_refit_sell_policy",
        final_artifact_sha256="a" * 64,
    )


def _common_identity(seed: str = "9") -> dict[str, Any]:
    return {
        "transport_common_market_source_sha256": seed * 64,
        "common_receive_clock_source_sha256": "a" * 64,
        "common_feature_ready_clock_source_sha256": "b" * 64,
        "common_random_source_sha256": "d" * 64,
        "market_manifest_sha256": "e" * 64,
        "lifecycle_manifest_sha256": "f" * 64,
        "decision_clock_contract": "exchange_receive_feature_ready_v1",
    }


def _prospective_admission(day: str):
    return successor.parse_prospective_day_admission(
        {
            "utc_day": day,
            "epoch_identity_sha256": "1" * 64,
            "session_manifest_sha256": "2" * 64,
            "utc_day_closed": True,
            "registered_treatment_interval_coverage_complete": True,
            "strategy_identity_valid": True,
            "source_complete": True,
            "receive_clock_valid": True,
            "feature_ready_clock_valid": True,
            "policy_decision_clock_valid": True,
            "lifecycle_valid": True,
            "callbacks_converged": True,
            "remote_local_admission_valid": True,
            "recorder_drops": 0,
            "severe_errors": 0,
            "eligible_events": 10,
            "feature_ready_active_treatment_events": 5,
        }
    )


class _OfflineCanonicalDayAdmission:
    def __init__(
        self,
        day: str,
        *,
        eligible: bool = True,
        receipt_sha256: str | None = None,
    ) -> None:
        self.admission_identity = "f05_offline_canonical_day_receipt_v1"
        self.utc_day = day
        self.eligible = eligible
        self._payload = {
            "admission_identity": self.admission_identity,
            "utc_day": day,
            "eligible": eligible,
            "record": {
                "canonical_source_manifest_sha256": "1" * 64,
                "day_receipt_sha256": "2" * 64,
                "source_role": "family_specific_unconsumed_historical_development",
            },
        }
        self.receipt_sha256 = receipt_sha256 or _canonical_sha256(self._payload)

    def canonical_receipt_payload(self) -> Mapping[str, Any]:
        return dict(self._payload)


def _initial_states() -> dict[str, bridge.ArmStateSnapshot]:
    states: dict[str, bridge.ArmStateSnapshot] = {}
    for arm in bridge.ARMS:
        states[arm] = bridge.ArmStateSnapshot.build(
            arm=arm,
            payload={
                "orders": {"arm": arm, "open": [], "sequence": 0},
                "inventory": {"arm": arm, "quantity": 0.0, "sequence": 0},
                "campaign": {"arm": arm, "campaign_id": None, "sequence": 0},
                "cooldown": {"arm": arm, "deadline_ns": None, "sequence": 0},
                "ema": {"arm": arm, "values": {}, "sequence": 0},
            },
        )
    return states


def _segment(
    segment_id: str,
    day: str,
    *,
    start_hour: int = 0,
    end_hour: int = 24,
    restart: bridge.FullyBoundRestartBinding | None = None,
    market_seed: str = "9",
    day_admission: bridge.FormalDayAdmissionProtocol | None = None,
) -> bridge.ReplaySegmentSpec:
    from datetime import UTC, date, datetime, time, timedelta

    parsed = date.fromisoformat(day)
    start = datetime.combine(parsed, time(), tzinfo=UTC) + timedelta(
        hours=start_hour
    )
    end = datetime.combine(parsed, time(), tzinfo=UTC) + timedelta(
        hours=end_hour
    )
    fields: dict[str, Any] = {
        "segment_id": segment_id,
        "utc_day": day,
        "segment_start_utc": start.isoformat().replace("+00:00", "Z"),
        "segment_end_utc": end.isoformat().replace("+00:00", "Z"),
        "common_input_identity": _common_identity(market_seed),
        "restart_binding": restart,
    }
    if day_admission is None:
        fields["prospective_day_admission"] = _prospective_admission(day)
    else:
        fields["day_admission"] = day_admission
    return bridge.ReplaySegmentSpec(
        **fields,
    )


class _Emitter:
    def __init__(self, arm: str) -> None:
        self.arm = arm

    def capture_exposure_fill(self, **_kwargs):
        return None

    def audit(self) -> dict[str, Any]:
        return {"arm": self.arm}


class _EmitterFactory:
    def __init__(self) -> None:
        self.instances: list[_Emitter] = []

    def __call__(self, arm, _common, _state_identity):
        emitter = _Emitter(arm)
        self.instances.append(emitter)
        return emitter


def _transport_receipt(
    arm: str,
    *,
    market_sha256: str,
    supported: bool = True,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "schema_version": transport.TRANSPORT_RECEIPT_SCHEMA_VERSION,
        "identity": transport.TRANSPORT_IDENTITY,
        "arm": arm,
        "common_market_source_sha256": market_sha256,
        "arm_fill_source_sha256": ("3" if arm == "control" else "4") * 64,
        "delay_artifact_sha256": None if arm == "control" else "5" * 64,
        "private_fill_visibility_authority": (
            "recorded_exact" if arm == "control" else "modeled_sensitivity"
        ),
        "book_event_count": 3,
        "book_visible_count": 3,
        "trade_event_count": 3,
        "trade_visible_count": 3,
        "fill_truth_count": 3,
        "private_fill_visible_count": 3 if supported else 2,
        "counterfactual_fill_censored_count": 0 if supported else 1,
        "source_gap_count": 0,
        "pre_exchange_clamp_count": 0,
        "head_of_line_clamp_count": 0,
        "clock_inversion_count": 0,
        "future_visibility_violation_count": 0,
        "ambiguous_same_timestamp_count": 0,
        "pending_private_fill_count": 0,
        "formal_replay_support_valid": supported,
        "live_equivalent": False,
        "thread_interleaving_replayed": False,
        "rest_user_stream_reconnect_replayed": False,
        "action_authorized": False,
        "live_policy_authorized": False,
        "exclusion_reasons": (
            () if supported else ("counterfactual_private_fill_delay_unsupported",)
        ),
    }
    return {**body, "transport_receipt_sha256": _canonical_sha256(body)}


_PREDICATES = {
    successor.CURRENT_SHORT_CROSS: 1,
    successor.CURRENT_LONG_CROSS: 0,
    successor.CURRENT_CAMPAIGN_AGE: 0,
    "predicate::candidate": 1,
}


class _FakeBacktestTickExecutor:
    def __init__(
        self,
        *,
        sides: tuple[str, ...] = ("BUY", "SELL", "SELL"),
        fail_once: tuple[str, str] | None = None,
        one_shot: bool = False,
        drop_last_decision: bool = False,
        random_source_drift: bool = False,
        receive_clock_drift: bool = False,
        state_identity_drift: bool = False,
        fresh_start: bool = False,
        output_state_hash_drift: bool = False,
        restart_recovery_missing: bool = False,
    ) -> None:
        self.sides = sides
        self.fail_once = fail_once
        self.one_shot = one_shot
        self.drop_last_decision = drop_last_decision
        self.random_source_drift = random_source_drift
        self.receive_clock_drift = receive_clock_drift
        self.state_identity_drift = state_identity_drift
        self.fresh_start = fresh_start
        self.output_state_hash_drift = output_state_hash_drift
        self.restart_recovery_missing = restart_recovery_missing
        self.calls: list[tuple[str, str]] = []
        self.requests: list[bridge.ArmReplayRequest] = []
        self.outputs: dict[tuple[str, str], bridge.ArmStateSnapshot] = {}

    @staticmethod
    def _transition(request: bridge.ArmReplayRequest) -> bridge.ArmStateSnapshot:
        output: dict[str, dict[str, Any]] = {}
        for component, raw in request.input_state.payload.items():
            value = dict(raw)
            value["sequence"] = int(value.get("sequence", 0)) + 1
            value["last_segment_id"] = request.segment_id
            value["arm"] = request.arm
            output[component] = value
        return bridge.ArmStateSnapshot.build(arm=request.arm, payload=output)

    def __call__(self, request: bridge.ArmReplayRequest) -> Mapping[str, Any]:
        call = (request.segment_id, request.arm)
        self.calls.append(call)
        self.requests.append(request)
        assert set(request.backtest_tick_params_overlay()) == {
            "cooldown_v2_snapshot_emitter",
            "cooldown_duration_policy_evaluator",
        }
        if self.fail_once == call:
            self.fail_once = None
            raise RuntimeError(f"synthetic {request.segment_id}/{request.arm} interruption")
        decisions: list[dict[str, Any]] = []
        snapshots: list[dict[str, Any]] = []
        for ordinal, side in enumerate(self.sides, start=1):
            snapshot_id = f"{request.segment_id}-{request.arm}-{ordinal}"
            decision = request.evaluator.evaluate_predicates(
                side=side,
                predicate_values=_PREDICATES,
                baseline_duration_ms=85_000,
                snapshot_id=snapshot_id,
            )
            decisions.append(
                {
                    "exposure_fill_ordinal": ordinal,
                    "side": side,
                    "snapshot_id": decision.snapshot_id,
                    "policy_sha256": decision.policy_sha256,
                    "action_id": decision.action_id,
                    "duration_ms": decision.duration_ms,
                }
            )
            snapshots.append(
                {"exposure_fill_ordinal": ordinal, "snapshot_id": snapshot_id}
            )
        if self.drop_last_decision:
            decisions.pop()
        backtest_result = {
            "campaign_exposure_increasing_fills": len(self.sides),
            "_cooldown_duration_policy_decisions": decisions,
            "_cooldown_v2_snapshot_receipts": snapshots,
            "_cooldown_duration_policy_audit": request.evaluator.audit(),
            "ema_add_wait_fork_enabled": False,
            "_ema_add_wait_fork_trace": {},
            "_cooldown_duration_fork_trace": (
                {"schema_version": "one_shot"} if self.one_shot else {}
            ),
        }
        state_identity = asdict(request.state_identity)
        if self.state_identity_drift:
            state_identity["order_state_id"] = "0" * 64
        output_state = self._transition(request)
        self.outputs[call] = output_state
        restart_recovery: dict[str, Any] = {"restart_detected": False}
        if request.restart_binding is not None and not self.restart_recovery_missing:
            restart_recovery = {
                "restart_detected": True,
                "fully_bound": True,
                "recovery_complete": True,
                "restart_manifest_sha256": (
                    request.restart_binding.restart_manifest_sha256
                ),
                "restored_input_state_sha256": request.input_state.state_sha256,
            }
        return {
            "engine_evaluator_abi": bridge.BACKTEST_TICK_EVALUATOR_ABI,
            "repeated_sequential_policy_executed": True,
            "one_shot_effect_aggregation_used": False,
            "execution_copied_from_other_arm": False,
            "chain_identity_sha256": request.chain_identity_sha256,
            "segment_index": request.segment_index,
            "segment_id": request.segment_id,
            "arm": request.arm,
            "policy_sha256": request.evaluator.policy_sha256,
            "common_input_identity_sha256": request.common_input_identity_sha256,
            "common_market_source_sha256": request.common_market_source_sha256,
            "common_receive_clock_source_sha256": (
                "0" * 64
                if self.receive_clock_drift
                else request.common_receive_clock_source_sha256
            ),
            "common_feature_ready_clock_source_sha256": (
                request.common_feature_ready_clock_source_sha256
            ),
            "common_random_source_sha256": (
                "0" * 64
                if self.random_source_drift
                else request.common_random_source_sha256
            ),
            "arm_local_state_identity": state_identity,
            "arm_local_state_fresh": self.fresh_start,
            "state_copied_from_other_arm": False,
            "input_state_restored": True,
            "input_state_sha256": request.input_state.state_sha256,
            "restart_recovery": restart_recovery,
            "state_transition_complete": True,
            "output_state": output_state.payload,
            "output_state_sha256": (
                "0" * 64
                if self.output_state_hash_drift
                else output_state.state_sha256
            ),
            "formal_support_valid": True,
            "formal_exclusion_reasons": [],
            "campaign_terminal_value_usdc": (
                10.0 if request.arm == bridge.CONTROL_ARM else 11.25
            ),
            "transport_receipt": _transport_receipt(
                request.arm,
                market_sha256=request.common_market_source_sha256,
            ),
            "backtest_result": backtest_result,
        }


def _run_chain(
    tmp_path: Path,
    executor: _FakeBacktestTickExecutor,
    *,
    segments: Sequence[bridge.ReplaySegmentSpec] | None = None,
    store: bridge.AtomicSegmentAdmissionStore | None = None,
    initial_states: Mapping[str, bridge.ArmStateSnapshot] | None = None,
    initial_manifest: str | None = None,
    binding: bridge.ArtifactIdentityBinding | None = None,
) -> tuple[bridge.RestartAwareStateChainReceipt, bridge.AtomicSegmentAdmissionStore]:
    frozen_states = dict(initial_states or _initial_states())
    frozen_store = store or bridge.AtomicSegmentAdmissionStore(tmp_path / "paired")
    receipt = bridge.execute_restart_aware_repeated_policy_state_chain(
        segments=segments or (_segment("day-20260813", "2026-08-13"),),
        initial_states=frozen_states,
        initial_state_manifest_sha256=(
            initial_manifest
            if initial_manifest is not None
            else bridge.build_initial_state_manifest_sha256(frozen_states)
        ),
        target_side=bridge.CandidateTargetSide.SELL,
        target_policy=_candidate_policy("SELL"),
        artifact_binding=binding or _artifact_binding(),
        arm_executor=executor,
        snapshot_emitter_factory=_EmitterFactory(),
        admission_store=frozen_store,
    )
    return receipt, frozen_store


def _admitted_receipts(
    store: bridge.AtomicSegmentAdmissionStore,
    chain_receipt: bridge.RestartAwareStateChainReceipt,
) -> list[dict[str, Any]]:
    root = store.root / "segments" / chain_receipt.chain_identity_sha256
    return [
        json.loads((path / "receipt.json").read_text(encoding="utf-8"))
        for path in sorted(root.iterdir())
        if path.is_dir() and not path.name.startswith(".")
    ]


def test_candidate_target_side_delegates_other_side_to_fresh_exact_b0() -> None:
    evaluator = bridge.build_target_side_candidate_evaluator(
        target_side=bridge.CandidateTargetSide.SELL,
        target_policy=_candidate_policy("SELL"),
        artifact_binding=_artifact_binding(),
    )
    buy = evaluator.evaluate_predicates(
        side="BUY",
        predicate_values=_PREDICATES,
        baseline_duration_ms=170_000,
        snapshot_id="buy",
    )
    sell = evaluator.evaluate_predicates(
        side="SELL",
        predicate_values=_PREDICATES,
        baseline_duration_ms=85_000,
        snapshot_id="sell",
    )
    assert buy.duration_ms == 170_000
    assert sell.duration_ms != 85_000
    assert buy.policy_sha256 == sell.policy_sha256 == "6" * 64
    audit = evaluator.audit()
    assert audit["target_side_evaluations"] == 1
    assert audit["b0_delegated_evaluations"] == 1
    assert audit["opposite_side_delegates_exact_b0"] is True
    assert audit["b0_delegate_audit"]["policy_sha256"] == (
        successor.ACTIVE_OWNER_POLICY_SHA256
    )


def test_two_arms_keep_independent_state_across_sorted_utc_days(tmp_path: Path) -> None:
    executor = _FakeBacktestTickExecutor()
    chain, store = _run_chain(
        tmp_path,
        executor,
        segments=(
            _segment("day-20260813", "2026-08-13"),
            _segment("day-20260814", "2026-08-14", market_seed="8"),
        ),
    )
    assert executor.calls == [
        ("day-20260813", bridge.CONTROL_ARM),
        ("day-20260813", bridge.CANDIDATE_ARM),
        ("day-20260814", bridge.CONTROL_ARM),
        ("day-20260814", bridge.CANDIDATE_ARM),
    ]
    for arm in bridge.ARMS:
        first_output = executor.outputs[("day-20260813", arm)]
        second_request = next(
            request
            for request in executor.requests
            if request.segment_id == "day-20260814" and request.arm == arm
        )
        assert second_request.input_state.state_sha256 == first_output.state_sha256
        assert second_request.input_state.payload == first_output.payload
        assert all(
            value["sequence"] == 1
            for value in second_request.input_state.payload.values()
        )
    assert chain.segment_count == 2
    assert chain.state_chain_contiguous is True
    assert chain.arm_local_state is True
    assert chain.fresh_start_used is False
    assert chain.one_shot_effect_aggregation_used is False
    receipts = _admitted_receipts(store, chain)
    assert receipts[0]["day_admission_identity"] == (
        f"{successor.IDENTITY}.prospective_day_admission.v1"
    )
    assert len(receipts[0]["day_admission_receipt_sha256"]) == 64
    assert receipts[1]["previous_segment_receipt_sha256"] == receipts[0][
        "receipt_sha256"
    ]
    assert receipts[1]["control_input_state_sha256"] == receipts[0][
        "control_output_state_sha256"
    ]
    assert receipts[1]["candidate_input_state_sha256"] == receipts[0][
        "candidate_output_state_sha256"
    ]
    assert receipts[0]["paired_exogenous_clock_identity_sha256"] != receipts[1][
        "paired_exogenous_clock_identity_sha256"
    ]
    assert chain.control_final_state_sha256 != chain.candidate_final_state_sha256


def test_offline_canonical_day_admission_runs_through_formal_bridge(
    tmp_path: Path,
) -> None:
    day = "2026-07-01"
    admission = _OfflineCanonicalDayAdmission(day)
    executor = _FakeBacktestTickExecutor()
    chain, store = _run_chain(
        tmp_path,
        executor,
        segments=(
            _segment(
                "offline-20260701",
                day,
                day_admission=admission,
            ),
        ),
    )
    assert executor.calls == [
        ("offline-20260701", bridge.CONTROL_ARM),
        ("offline-20260701", bridge.CANDIDATE_ARM),
    ]
    receipt = _admitted_receipts(store, chain)[0]
    assert receipt["utc_day"] == day
    assert receipt["formal_denominator_eligible"] is True
    assert receipt["day_admission_identity"] == admission.admission_identity
    assert receipt["day_admission_receipt_sha256"] == admission.receipt_sha256
    assert receipt["one_shot_effect_aggregation_used"] is False


@pytest.mark.parametrize(
    ("admission", "segment_day", "message"),
    [
        (
            _OfflineCanonicalDayAdmission("2026-07-01", receipt_sha256="0" * 64),
            "2026-07-01",
            "receipt hash mismatch",
        ),
        (
            _OfflineCanonicalDayAdmission("2026-07-02"),
            "2026-07-01",
            "does not match replay UTC day",
        ),
        (
            _OfflineCanonicalDayAdmission("2026-07-01", eligible=False),
            "2026-07-01",
            "ineligible for formal replay",
        ),
    ],
)
def test_formal_day_admission_fails_closed_before_arm_execution(
    admission: _OfflineCanonicalDayAdmission,
    segment_day: str,
    message: str,
) -> None:
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match=message):
        _segment(
            "offline-invalid",
            segment_day,
            day_admission=admission,
        )


def test_segment_rejects_ambiguous_legacy_and_protocol_admissions() -> None:
    from datetime import UTC, datetime, timedelta

    day = "2026-08-13"
    start = datetime(2026, 8, 13, tzinfo=UTC)
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="exactly one"):
        bridge.ReplaySegmentSpec(
            segment_id="ambiguous-admission",
            utc_day=day,
            segment_start_utc=start.isoformat().replace("+00:00", "Z"),
            segment_end_utc=(start + timedelta(days=1))
            .isoformat()
            .replace("+00:00", "Z"),
            common_input_identity=_common_identity(),
            prospective_day_admission=_prospective_admission(day),
            day_admission=_OfflineCanonicalDayAdmission(day),
        )


def test_segment_rejects_missing_formal_day_admission() -> None:
    from datetime import UTC, datetime, timedelta

    start = datetime(2026, 8, 13, tzinfo=UTC)
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="exactly one"):
        bridge.ReplaySegmentSpec(
            segment_id="missing-admission",
            utc_day="2026-08-13",
            segment_start_utc=start.isoformat().replace("+00:00", "Z"),
            segment_end_utc=(start + timedelta(days=1))
            .isoformat()
            .replace("+00:00", "Z"),
            common_input_identity=_common_identity(),
        )


def test_formal_day_admission_rejects_receipt_identity_drift() -> None:
    admission = _OfflineCanonicalDayAdmission("2026-07-01")
    admission._payload["admission_identity"] = "different_offline_identity"
    admission.receipt_sha256 = _canonical_sha256(admission._payload)
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="identity drifted"):
        _segment(
            "offline-identity-drift",
            "2026-07-01",
            day_admission=admission,
        )


def test_fully_bound_restart_restores_same_hash_and_continues(tmp_path: Path) -> None:
    store = bridge.AtomicSegmentAdmissionStore(tmp_path / "paired")
    first, _ = _run_chain(
        tmp_path,
        _FakeBacktestTickExecutor(),
        segments=(_segment("day-20260813", "2026-08-13"),),
        store=store,
    )
    restart = bridge.FullyBoundRestartBinding(
        restart_manifest_sha256="c" * 64,
        restored_state_sha256={
            bridge.CONTROL_ARM: first.control_final_state_sha256,
            bridge.CANDIDATE_ARM: first.candidate_final_state_sha256,
        },
    )
    resumed_executor = _FakeBacktestTickExecutor()
    resumed, _ = _run_chain(
        tmp_path,
        resumed_executor,
        segments=(
            _segment("day-20260813", "2026-08-13"),
            _segment("day-20260814", "2026-08-14", restart=restart),
        ),
        store=store,
    )
    assert resumed_executor.calls == [
        ("day-20260814", bridge.CONTROL_ARM),
        ("day-20260814", bridge.CANDIDATE_ARM),
    ]
    assert resumed.restart_count == 1
    receipts = _admitted_receipts(store, resumed)
    assert receipts[1]["fully_bound_restart_restored"] is True
    assert receipts[1]["restart_manifest_sha256"] == "c" * 64
    assert receipts[1]["control_input_state_sha256"] == (
        first.control_final_state_sha256
    )
    assert receipts[1]["candidate_input_state_sha256"] == (
        first.candidate_final_state_sha256
    )


def test_unrecoverable_restart_fails_closed_without_fresh_start(tmp_path: Path) -> None:
    store = bridge.AtomicSegmentAdmissionStore(tmp_path / "paired")
    first, _ = _run_chain(
        tmp_path,
        _FakeBacktestTickExecutor(),
        segments=(_segment("day-20260813", "2026-08-13"),),
        store=store,
    )
    restart = bridge.FullyBoundRestartBinding(
        restart_manifest_sha256="c" * 64,
        restored_state_sha256={
            bridge.CONTROL_ARM: "0" * 64,
            bridge.CANDIDATE_ARM: first.candidate_final_state_sha256,
        },
    )
    executor = _FakeBacktestTickExecutor()
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="restored a different"):
        _run_chain(
            tmp_path,
            executor,
            segments=(
                _segment("day-20260813", "2026-08-13"),
                _segment("day-20260814", "2026-08-14", restart=restart),
            ),
            store=store,
        )
    assert executor.calls == []
    assert not list(
        (store.root / "segments" / first.chain_identity_sha256).glob("*20260814*")
    )


def test_restart_executor_must_emit_exact_recovery_receipt(tmp_path: Path) -> None:
    store = bridge.AtomicSegmentAdmissionStore(tmp_path / "paired")
    first, _ = _run_chain(
        tmp_path,
        _FakeBacktestTickExecutor(),
        segments=(_segment("day-20260813", "2026-08-13"),),
        store=store,
    )
    restart = bridge.FullyBoundRestartBinding(
        restart_manifest_sha256="c" * 64,
        restored_state_sha256={
            bridge.CONTROL_ARM: first.control_final_state_sha256,
            bridge.CANDIDATE_ARM: first.candidate_final_state_sha256,
        },
    )
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="fully-bound restart"):
        _run_chain(
            tmp_path,
            _FakeBacktestTickExecutor(restart_recovery_missing=True),
            segments=(
                _segment("day-20260813", "2026-08-13"),
                _segment("day-20260814", "2026-08-14", restart=restart),
            ),
            store=store,
        )


@pytest.mark.parametrize(
    ("executor", "message"),
    [
        (_FakeBacktestTickExecutor(one_shot=True), "one-shot fork trace"),
        (_FakeBacktestTickExecutor(fresh_start=True), "fresh-start is forbidden"),
        (
            _FakeBacktestTickExecutor(drop_last_decision=True),
            "every exposure fill",
        ),
        (
            _FakeBacktestTickExecutor(random_source_drift=True),
            "common random source drifted",
        ),
        (
            _FakeBacktestTickExecutor(receive_clock_drift=True),
            "common receive clock drifted",
        ),
        (
            _FakeBacktestTickExecutor(state_identity_drift=True),
            "arm-local state identity",
        ),
        (
            _FakeBacktestTickExecutor(output_state_hash_drift=True),
            "output state hash drifted",
        ),
    ],
)
def test_bridge_rejects_nonsequential_unpaired_or_unrecoverable_execution(
    tmp_path: Path,
    executor: _FakeBacktestTickExecutor,
    message: str,
) -> None:
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match=message):
        _run_chain(tmp_path, executor)


def test_checkpoint_resume_and_atomic_segment_admission(tmp_path: Path) -> None:
    store = bridge.AtomicSegmentAdmissionStore(tmp_path / "paired")
    interrupted = _FakeBacktestTickExecutor(
        fail_once=("day-20260813", bridge.CANDIDATE_ARM)
    )
    with pytest.raises(RuntimeError, match="synthetic day-20260813/candidate"):
        _run_chain(tmp_path, interrupted, store=store)
    checkpoints = list((store.root / "checkpoints").rglob("control.json"))
    assert len(checkpoints) == 1
    assert not list((store.root / "segments").rglob(bridge.SEGMENT_SUCCESS_MARKER))

    resumed = _FakeBacktestTickExecutor()
    receipt, _ = _run_chain(tmp_path, resumed, store=store)
    assert resumed.calls == [("day-20260813", bridge.CANDIDATE_ARM)]
    segment_root = store.root / "segments" / receipt.chain_identity_sha256
    admitted = next(path for path in segment_root.iterdir() if path.is_dir())
    assert (admitted / "manifest.json").is_file()
    assert (admitted / "receipt.json").is_file()
    assert (admitted / bridge.SEGMENT_SUCCESS_MARKER).is_file()
    assert not list(segment_root.glob("*.partial"))

    never_called = _FakeBacktestTickExecutor()
    loaded, _ = _run_chain(tmp_path, never_called, store=store)
    assert never_called.calls == []
    assert loaded.segment_receipt_sha256 == receipt.segment_receipt_sha256


def test_atomic_admission_revalidates_checkpoint_state_bytes(tmp_path: Path) -> None:
    store = bridge.AtomicSegmentAdmissionStore(tmp_path / "paired")

    class _TamperingExecutor(_FakeBacktestTickExecutor):
        def __call__(self, request):
            if request.arm == bridge.CANDIDATE_ARM:
                checkpoint = next((store.root / "checkpoints").rglob("control.json"))
                payload = json.loads(checkpoint.read_text(encoding="utf-8"))
                payload["evidence"]["output_state"]["payload"]["inventory"][
                    "quantity"
                ] = 999.0
                checkpoint.write_text(json.dumps(payload), encoding="utf-8")
            return super().__call__(request)

    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="checkpoint hash drifted"):
        _run_chain(tmp_path, _TamperingExecutor(), store=store)
    assert not list((store.root / "segments").rglob(bridge.SEGMENT_SUCCESS_MARKER))


def test_initial_state_manifest_must_bind_exact_payload(tmp_path: Path) -> None:
    states = _initial_states()
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="initial state manifest"):
        _run_chain(
            tmp_path,
            _FakeBacktestTickExecutor(),
            initial_states=states,
            initial_manifest="0" * 64,
        )


def test_all_five_state_components_are_mandatory() -> None:
    payload = dict(_initial_states()[bridge.CONTROL_ARM].payload)
    payload.pop("ema")
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="orders/inventory"):
        bridge.ArmStateSnapshot.build(arm=bridge.CONTROL_ARM, payload=payload)


def test_unsorted_or_overlapping_segments_fail_before_execution(tmp_path: Path) -> None:
    executor = _FakeBacktestTickExecutor()
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="not UTC sorted"):
        _run_chain(
            tmp_path,
            executor,
            segments=(
                _segment("late", "2026-08-14"),
                _segment("early", "2026-08-13"),
            ),
        )
    assert executor.calls == []


def test_shared_emitter_is_rejected_before_any_economics(tmp_path: Path) -> None:
    states = _initial_states()
    shared = _Emitter("shared")
    store = bridge.AtomicSegmentAdmissionStore(tmp_path / "paired")
    with pytest.raises(bridge.RepeatedPolicyBridgeError, match="share one snapshot emitter"):
        bridge.execute_restart_aware_repeated_policy_state_chain(
            segments=(_segment("day-20260813", "2026-08-13"),),
            initial_states=states,
            initial_state_manifest_sha256=(
                bridge.build_initial_state_manifest_sha256(states)
            ),
            target_side=bridge.CandidateTargetSide.SELL,
            target_policy=_candidate_policy("SELL"),
            artifact_binding=_artifact_binding(),
            arm_executor=_FakeBacktestTickExecutor(),
            snapshot_emitter_factory=lambda _arm, _common, _state: shared,
            admission_store=store,
        )


def test_target_side_without_fill_is_excluded_not_zero_effect(tmp_path: Path) -> None:
    chain, store = _run_chain(
        tmp_path,
        _FakeBacktestTickExecutor(sides=("BUY", "BUY")),
    )
    paired = _admitted_receipts(store, chain)[0]
    assert paired["candidate_target_side_evaluations"] == 0
    assert paired["formal_denominator_eligible"] is False
    assert paired["terminal_value_delta_usdc"] is None
    assert "candidate:candidate_target_side_not_evaluated" in paired[
        "exclusion_reasons"
    ]


def test_final_refit_identity_stays_separate_from_exact_oof(tmp_path: Path) -> None:
    chain, store = _run_chain(
        tmp_path,
        _FakeBacktestTickExecutor(),
        binding=_artifact_binding(
            scope=bridge.ExecutedArtifactScope.FINAL_FULL_DEVELOPMENT_REFIT
        ),
    )
    paired = _admitted_receipts(store, chain)[0]
    assert paired["executed_artifact_scope"] == (
        "final_full_development_refit_artifact"
    )
    assert paired["final_artifact_identity"] == "successor_final_refit_sell_policy"
    assert paired["exact_final_artifact_oof_available"] is False
    assert paired["action_authorized"] is False
    assert paired["live_policy_authorized"] is False
