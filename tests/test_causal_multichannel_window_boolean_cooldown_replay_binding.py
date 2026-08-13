from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from models import backtest_tick as bt
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_replay_emitter import (
    CooldownV2ReplayEmitter,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    PosixCooldownSharedPrefixExecutor,
)

BASE_MS = 1_700_000_000_000
BASE_NS = BASE_MS * 1_000_000


def _params(emitter: CooldownV2ReplayEmitter) -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 0.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "fill_cooldown": 85.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "replay_initial_state_mode": "fresh_start",
        "trace_cooldown_duration_opportunities_max": 100,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
        "cooldown_v2_snapshot_emitter": emitter,
    }


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + offset for offset in range(0, 9_000, 1_000)],
                dtype=np.int64,
            ),
            "price": np.asarray(
                [100.0, 96.6, 96.6, 90.0, 90.0, 90.0, 80.0, 80.0, 80.0],
                dtype=np.float64,
            ),
            "quantity": np.asarray(
                [0.0, 0.001, 0.0, 0.001, 0.0, 0.0, 0.001, 0.0, 0.0],
                dtype=np.float64,
            ),
            "is_buyer_maker": np.asarray(
                [False, True, False, True, False, False, True, False, False],
                dtype=np.uint8,
            ),
        }
    )


def _emitter(*, gap_through_ns: int | None = None) -> CooldownV2ReplayEmitter:
    observations = (
        CausalWindowObservation(
            left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
            right_ts_ns=right,
            feature_ready_ts_ns=right,
            market_generation=index,
            depth_generation=index,
            values={"mid_usdc_per_btc": 100.0},
            source_gap=bool(
                gap_through_ns is not None and right <= gap_through_ns
            ),
            warmup_admitted=right > BASE_NS,
        )
        for index, right in enumerate(
            range(
                BASE_NS,
                BASE_NS + 9_100_000_000,
                BASE_WINDOW_WIDTH_NS,
            ),
            start=1,
        )
    )
    return CooldownV2ReplayEmitter(
        feature_block="R0",
        observations=observations,
        warmup_cutoff_ts_ns=BASE_NS,
        warmup_identity="synthetic-d-minus-1",
        identity_hashes={
            "config_sha256": "a" * 64,
            "code_sha256": "b" * 64,
            "model_sha256": "c" * 64,
            "p3_sha256": "d" * 64,
            "feature_dag_sha256": "e" * 64,
            "execution_abi_sha256": "f" * 64,
            "baseline_identity_sha256": "1" * 64,
        },
        source_cursor_prefixes={
            "market": "synthetic-market",
            "depth": "synthetic-depth",
            "trade": "synthetic-trade",
        },
        retain_snapshots=True,
    )


def test_python_replay_emits_atomic_snapshot_at_each_exposure_fill() -> None:
    emitter = _emitter()
    result = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _params(emitter),
    )

    receipts = result["_cooldown_v2_snapshot_receipts"]
    assert receipts
    assert len(receipts) == len(emitter.snapshots)
    first = emitter.snapshots[0]
    assert first.m0_context["role_at_fill"] == "opener"
    assert first.m0_context["cooldown_lineage_revision_before"] == 0
    assert first.m0_context["remaining_order_qty_after_btc"] == 0.0
    assert first.m0_context["queue_state_before_fill"] == "unknown"
    assert first.m0_context["target_price_displayed_qty_status"] == "unknown"
    assert first.m0_context["target_price_displayed_qty_btc"] is None
    assert first.m0_context["target_price_displayed_qty_known"] is False
    assert first.m0_context["target_price_displayed_qty_is_queue_ahead"] is False
    assert first.policy_input_valid is True
    assert result["_cooldown_v2_snapshot_emitter_audit"]["snapshots_emitted"] == len(
        receipts
    )


@dataclass(frozen=True)
class _Decision:
    action_id: str
    duration_ms: float
    fallback_reason: str
    matched_rule_index: int | None
    policy_sha256: str
    predicate_bundle_sha256: str
    snapshot_id: str
    support_valid: bool


class _RepeatedPolicyEvaluator:
    policy_identity = "synthetic-repeated-policy"
    policy_sha256 = "a" * 64
    predicate_bundle_sha256 = "b" * 64

    def __init__(self) -> None:
        self.snapshot_ids: list[str] = []

    def evaluate(self, snapshot, *, baseline_duration_ms: float) -> _Decision:
        self.snapshot_ids.append(str(snapshot.snapshot_id))
        return _Decision(
            action_id="FIXED_1S",
            duration_ms=1_000.0,
            fallback_reason="",
            matched_rule_index=0,
            policy_sha256="a" * 64,
            predicate_bundle_sha256="b" * 64,
            snapshot_id=str(snapshot.snapshot_id),
            support_valid=True,
        )

    def audit(self) -> dict[str, int]:
        return {"evaluations": len(self.snapshot_ids)}


def test_python_replay_executes_repeated_policy_at_every_exposure_fill() -> None:
    emitter = _emitter()
    evaluator = _RepeatedPolicyEvaluator()
    params = _params(emitter)
    params["cooldown_duration_policy_evaluator"] = evaluator

    result = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    decisions = result["_cooldown_duration_policy_decisions"]
    assert len(decisions) == len(evaluator.snapshot_ids) >= 2
    assert len(decisions) == len(result["_cooldown_v2_snapshot_receipts"])
    assert all(row["action_id"] == "FIXED_1S" for row in decisions)
    assert all(row["duration_ms"] == 1_000.0 for row in decisions)
    assert result["_cooldown_duration_policy_audit"] == {
        "evaluations": len(decisions)
    }


def test_one_shot_fork_uses_exact_owner_prefix_and_overrides_only_target() -> None:
    baseline_params = _params(_emitter())
    baseline_params["cooldown_duration_policy_evaluator"] = _RepeatedPolicyEvaluator()
    baseline_params["trace_fills_max"] = 100
    baseline = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        baseline_params,
    )
    target = baseline["_cooldown_duration_opportunity_trace"][0]

    params = _params(_emitter())
    params.update(
        {
            "cooldown_duration_policy_evaluator": _RepeatedPolicyEvaluator(),
            "cooldown_duration_fork_enabled": True,
            "cooldown_duration_fork_action": "FIXED_DURATION_MS",
            "cooldown_duration_fork_target_ordinal": int(
                target["exposure_fill_ordinal"]
            ),
            "cooldown_duration_fork_target_ts_ms": int(
                target["fill_visible_ts_ms"]
            ),
            "cooldown_duration_fork_target_side": str(target["side"]),
            "cooldown_duration_fork_target_order_id": int(target["order_id"]),
            "cooldown_duration_fork_target_campaign_id": int(
                target["campaign_id"]
            ),
            "cooldown_duration_fork_expected_baseline_ms": float(
                target["baseline_duration_ms"]
            ),
            "cooldown_duration_fork_fixed_ms": 2_000.0,
            "cooldown_duration_fork_baseline_policy_enabled": True,
            "cooldown_duration_fork_expected_owner_action": "FIXED_1S",
            "cooldown_duration_fork_expected_owner_policy_sha256": "a" * 64,
        }
    )
    fork = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    trace = fork["_cooldown_duration_fork_trace"]
    assert (
        trace["schema_version"]
        == "multiscale_ema_boolean_cooldown_duration_fork_trace.v3"
    )
    assert trace["exact_owner_baseline_policy_enabled"] is True
    assert trace["exact_owner_action"] == "FIXED_1S"
    assert trace["exact_owner_baseline_duration_ms"] == 1_000.0
    assert trace["applied_duration_ms"] == 2_000.0
    assert trace["exact_owner_policy_sha256"] == "a" * 64
    assert fork["_cooldown_duration_policy_decisions"][0]["action_id"] == "FIXED_1S"


def test_legacy_one_shot_fork_keeps_v2_schema_without_exact_owner_fields() -> None:
    baseline = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _params(_emitter()),
    )
    target = baseline["_cooldown_duration_opportunity_trace"][0]
    params = _params(_emitter())
    params.update(
        {
            "cooldown_duration_fork_enabled": True,
            "cooldown_duration_fork_action": "CONTROL_85N",
            "cooldown_duration_fork_target_ordinal": int(
                target["exposure_fill_ordinal"]
            ),
            "cooldown_duration_fork_target_ts_ms": int(
                target["fill_visible_ts_ms"]
            ),
            "cooldown_duration_fork_target_side": str(target["side"]),
            "cooldown_duration_fork_target_order_id": int(target["order_id"]),
            "cooldown_duration_fork_target_campaign_id": int(
                target["campaign_id"]
            ),
            "cooldown_duration_fork_expected_baseline_ms": float(
                target["baseline_duration_ms"]
            ),
        }
    )
    fork = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    trace = fork["_cooldown_duration_fork_trace"]
    assert (
        trace["schema_version"]
        == "multiscale_ema_boolean_cooldown_duration_fork_trace.v2"
    )
    assert "exact_owner_action" not in trace
    assert "exact_owner_policy_sha256" not in trace


def test_one_shot_fork_rejects_unbound_policy_combination() -> None:
    params = _params(_emitter())
    params.update(
        {
            "cooldown_duration_policy_evaluator": _RepeatedPolicyEvaluator(),
            "cooldown_duration_fork_enabled": True,
            "cooldown_duration_fork_action": "CONTROL_85N",
            "cooldown_duration_fork_target_ordinal": 1,
            "cooldown_duration_fork_target_ts_ms": BASE_MS + 1_000,
            "cooldown_duration_fork_target_side": "BUY",
            "cooldown_duration_fork_target_order_id": 1,
            "cooldown_duration_fork_target_campaign_id": 1,
            "cooldown_duration_fork_expected_baseline_ms": 85_000.0,
        }
    )
    with np.testing.assert_raises_regex(
        ValueError, "cannot share one-shot cooldown execution"
    ):
        bt._simulate_tick_with_engine(
            "python",
            _trades(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            params,
        )


def test_cpp_replay_rejects_python_repeated_policy() -> None:
    emitter = _emitter()
    params = _params(emitter)
    params["cooldown_duration_policy_evaluator"] = _RepeatedPolicyEvaluator()

    with np.testing.assert_raises(NotImplementedError):
        bt._simulate_tick_with_engine(
            "cpp",
            _trades(),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            params,
        )


def test_python_replay_reports_bounded_event_progress() -> None:
    emitter = _emitter()
    progress: list[dict[str, int]] = []
    params = _params(emitter)
    params["_replay_progress_callback"] = lambda row: progress.append(dict(row))
    params["_replay_progress_interval_events"] = 2

    bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    assert progress[0]["event_index"] == 0
    assert all(row["event_count"] >= len(_trades()) for row in progress)
    assert [row["event_index"] for row in progress] == sorted(
        {row["event_index"] for row in progress}
    )


def test_python_replay_executes_eight_arms_from_one_posix_prefix(
    tmp_path,
) -> None:
    emitter = _emitter()
    executor = PosixCooldownSharedPrefixExecutor(
        output_root=tmp_path / "labels",
        target_day="2023-11-14",
        source_contract_sha256="2" * 64,
        execution_identity_hashes={
            "baseline_identity_sha256": "3" * 64,
            "config_sha256": "4" * 64,
            "code_sha256": "5" * 64,
            "model_sha256": "6" * 64,
            "p3_sha256": "7" * 64,
            "feature_dag_sha256": "8" * 64,
            "execution_abi_sha256": "9" * 64,
        },
        require_strict_native=False,
    )
    params = _params(emitter)
    params["cooldown_duration_shared_prefix_executor"] = executor

    result = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    audit = result["_cooldown_duration_shared_prefix_audit"]
    assert audit["opportunities_dispatched"] == 1
    assert audit["arm_processes_completed"] == 8
    manifest_path = tmp_path / "labels" / "2023-11-14"
    admissions = [path for path in manifest_path.iterdir() if path.is_dir()]
    assert len(admissions) == 1
    admitted_files = {path.name for path in admissions[0].iterdir()}
    assert len([name for name in admitted_files if name.startswith("arm-")]) == 8
    assert {"manifest.json", "_SUCCESS"} <= admitted_files


def test_shared_prefix_parent_stops_at_boundary_but_fork_arms_continue(
    tmp_path,
) -> None:
    emitter = _emitter()
    executor = PosixCooldownSharedPrefixExecutor(
        output_root=tmp_path / "labels",
        target_day="2023-11-14",
        source_contract_sha256="2" * 64,
        execution_identity_hashes={
            "baseline_identity_sha256": "3" * 64,
            "config_sha256": "4" * 64,
            "code_sha256": "5" * 64,
            "model_sha256": "6" * 64,
            "p3_sha256": "7" * 64,
            "feature_dag_sha256": "8" * 64,
            "execution_abi_sha256": "9" * 64,
        },
        require_strict_native=False,
    )
    params = _params(emitter)
    params["cooldown_duration_shared_prefix_executor"] = executor
    params["cooldown_duration_parent_stop_ts_ms"] = BASE_MS + 4_500

    result = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    assert result["cooldown_duration_parent_stop_triggered"] is True
    assert result["cooldown_duration_parent_stop_trigger_ts_ms"] == BASE_MS + 4_500
    audit = result["_cooldown_duration_shared_prefix_audit"]
    assert audit["opportunities_dispatched"] == 1
    assert audit["arm_processes_completed"] == 8


def test_bounded_shared_prefix_parent_stops_after_frozen_opportunity(
    tmp_path,
) -> None:
    emitter = _emitter()
    executor = PosixCooldownSharedPrefixExecutor(
        output_root=tmp_path / "labels",
        target_day="2023-11-14",
        source_contract_sha256="2" * 64,
        execution_identity_hashes={
            "baseline_identity_sha256": "3" * 64,
            "config_sha256": "4" * 64,
            "code_sha256": "5" * 64,
            "model_sha256": "6" * 64,
            "p3_sha256": "7" * 64,
            "feature_dag_sha256": "8" * 64,
            "execution_abi_sha256": "9" * 64,
        },
        max_opportunities=1,
        require_strict_native=False,
    )
    params = _params(emitter)
    params["cooldown_duration_shared_prefix_executor"] = executor
    params["cooldown_duration_parent_stop_ts_ms"] = BASE_MS + 8_000

    result = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    assert result["cooldown_duration_parent_stop_triggered"] is True
    assert result["cooldown_duration_parent_stop_trigger_ts_ms"] < BASE_MS + 8_000
    audit = result["_cooldown_duration_shared_prefix_audit"]
    assert audit["opportunities_dispatched"] == 1
    assert audit["arm_processes_completed"] == 8


def test_invalid_feature_snapshot_falls_back_without_forking(
    tmp_path,
) -> None:
    emitter = _emitter(gap_through_ns=BASE_NS + 1_100_000_000)
    executor = PosixCooldownSharedPrefixExecutor(
        output_root=tmp_path / "labels",
        target_day="2023-11-14",
        source_contract_sha256="2" * 64,
        execution_identity_hashes={
            "baseline_identity_sha256": "3" * 64,
            "config_sha256": "4" * 64,
            "code_sha256": "5" * 64,
            "model_sha256": "6" * 64,
            "p3_sha256": "7" * 64,
            "feature_dag_sha256": "8" * 64,
            "execution_abi_sha256": "9" * 64,
        },
        max_opportunities=1,
        require_strict_native=False,
    )
    params = _params(emitter)
    params["cooldown_duration_shared_prefix_executor"] = executor

    result = bt._simulate_tick_with_engine(
        "python",
        _trades(),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )

    receipts = result["_cooldown_v2_snapshot_receipts"]
    assert receipts[0]["policy_input_valid"] is False
    assert result["_cooldown_duration_shared_prefix_audit"][
        "opportunities_dispatched"
    ] == 0
    assert not (tmp_path / "labels" / "2023-11-14").exists()
