from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from models.backtest_tick import simulate_tick
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard import (
    CANDIDATE_ACTION,
    deterministic_campaign_side_assignment,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    FROZEN_RANDOM_SEEDS,
    AdapterContractViolation,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4 import (
    stable_campaign_opportunity_id,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay import (
    RankedToxicityBaselineShadowCaptureV14,
    RankedToxicityGuardAuthoritativeReplayV14,
)

DAY = "2026-08-03"
MODEL_SHA256 = "a" * 64
THRESHOLD_SHA256 = "b" * 64


def _day_start_ms() -> int:
    return int(
        datetime.fromisoformat(DAY)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


def _prediction_kwargs() -> dict[str, float | int]:
    bucket = _day_start_ms() + 10_000
    return {
        "prediction_bucket_ts_ms": bucket,
        "feature_ready_ts_ms": bucket + 100,
        "observation_ts_ms": bucket + 200,
        "tox_bid": 0.9,
        "tox_ask": 0.1,
    }


def _quote_kwargs() -> dict[str, object]:
    prediction = _prediction_kwargs()
    return {
        "decision_id": "BTCUSDC:decision-1:BUY",
        "decision_ts_ns": int(prediction["observation_ts_ms"]) * 1_000_000,
        "side": "BUY",
        "role": "opener",
        "baseline_eligible": True,
        "exposure_increasing": True,
        "can_post": True,
        "allow_exposure_increase": True,
        "active_exposure_order_id": "",
        "quote_price": 100.0,
        "quote_quantity": 0.001,
        "blocker_reasons": (),
        "policy_fingerprint": "c" * 64,
        "untreated_lineage_ordinal": 1,
    }


def test_two_pass_authoritative_binding_uses_exact_untreated_denominator(tmp_path) -> None:
    baseline_dir = tmp_path / "baseline"
    capture = RankedToxicityBaselineShadowCaptureV14(
        output_dir=baseline_dir,
        lineage_namespace=f"{DAY}|panel-1",
        sides=("BUY",),
        chunk_rows=1,
    )
    capture.validate_replay_start(
        params={"dynamic_fill_hazard_action_enabled": False},
        ml_data=([], [], [], [], [], []),
    )
    capture.on_prediction_bucket(**_prediction_kwargs())
    quote = _quote_kwargs()
    capture.on_quote_decision(**quote)
    capture.on_final_quote_action(
        decision_id=str(quote["decision_id"]),
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action="place",
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=int(quote["decision_ts_ns"]),
    )
    capture_audit = capture.finish_replay(
        event_ts_ns=int(quote["decision_ts_ns"])
    )
    assert capture_audit["baseline_shadow_rows"] == 1

    candidate = RankedToxicityGuardAuthoritativeReplayV14(
        baseline_manifest_path=baseline_dir / "manifest.json",
        output_root=tmp_path / "candidate",
        frozen_model_sha256=MODEL_SHA256,
        threshold_schedule={
            "BUY": {DAY: (0.8, THRESHOLD_SHA256)},
        },
        sides=("BUY",),
        chunk_rows=1,
    )
    candidate.validate_replay_start(
        params={"dynamic_fill_hazard_action_enabled": False},
        ml_data=([], [], [], [], [], []),
    )
    candidate.on_prediction_bucket(**_prediction_kwargs())
    directive = candidate.on_quote_decision(**quote)
    candidate_action = "place" if directive.allow_exposure_submission else "pause"
    changed = candidate.on_final_quote_action(
        decision_id=str(quote["decision_id"]),
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action=candidate_action,
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=int(quote["decision_ts_ns"]),
    )
    audit = candidate.finish_replay(event_ts_ns=int(quote["decision_ts_ns"]))

    assert changed is (candidate_action != "place")
    assert audit["baseline_shadow"]["complete"] is True
    assert audit["baseline_shadow"]["rows"] == 1
    assert audit["adapters"]["BUY"]["execution_complete"] is True
    assert audit["economic_outcomes_read"] is False


def test_baseline_capture_rejects_duplicate_prediction_bucket(tmp_path) -> None:
    capture = RankedToxicityBaselineShadowCaptureV14(
        output_dir=tmp_path / "baseline",
        lineage_namespace="panel",
        sides=("BUY",),
    )
    capture.on_prediction_bucket(**_prediction_kwargs())
    with pytest.raises(AdapterContractViolation, match="duplicate"):
        capture.on_prediction_bucket(**_prediction_kwargs())


def test_assignment_boundary_uses_untreated_lineage_not_candidate_campaign(
    tmp_path,
) -> None:
    baseline_dir = tmp_path / "baseline-lineages"
    capture = RankedToxicityBaselineShadowCaptureV14(
        output_dir=baseline_dir,
        lineage_namespace=f"{DAY}|panel-lineages",
        sides=("BUY",),
        chunk_rows=2,
    )
    capture.on_prediction_bucket(**_prediction_kwargs())
    base_quote = _quote_kwargs()
    for suffix, ordinal in (("one", 1), ("same", 1), ("two", 2)):
        quote = {
            **base_quote,
            "decision_id": f"BTCUSDC:{suffix}:BUY",
            "decision_ts_ns": int(base_quote["decision_ts_ns"]) + ordinal,
            "untreated_lineage_ordinal": ordinal,
        }
        capture.on_quote_decision(**quote)
        capture.on_final_quote_action(
            decision_id=str(quote["decision_id"]),
            side="BUY",
            role="opener",
            exposure_increasing=True,
            candidate_action="place",
            candidate_price=100.0,
            candidate_quantity=0.001,
            candidate_order_id="",
            event_ts_ns=int(quote["decision_ts_ns"]),
        )
    capture.finish_replay(event_ts_ns=int(base_quote["decision_ts_ns"]) + 10)

    candidate = RankedToxicityGuardAuthoritativeReplayV14(
        baseline_manifest_path=baseline_dir / "manifest.json",
        output_root=tmp_path / "candidate-lineages",
        frozen_model_sha256=MODEL_SHA256,
        threshold_schedule={"BUY": {DAY: (0.8, THRESHOLD_SHA256)}},
        sides=("BUY",),
        chunk_rows=2,
    )
    candidate.on_prediction_bucket(**_prediction_kwargs())
    first_assignment_id = ""
    for index, (suffix, ordinal, candidate_reducing) in enumerate(
        (("one", 1, False), ("same", 1, True), ("two", 2, False))
    ):
        quote = {
            **base_quote,
            "decision_id": f"BTCUSDC:{suffix}:BUY",
            "decision_ts_ns": int(base_quote["decision_ts_ns"]) + ordinal,
            "untreated_lineage_ordinal": ordinal,
            "role": "reducing" if candidate_reducing else "opener",
            "exposure_increasing": not candidate_reducing,
        }
        directive = candidate.on_quote_decision(**quote)
        assignment_id = candidate.adapters["BUY"].current_assignment
        assert assignment_id is not None
        if index == 0:
            first_assignment_id = assignment_id.prospective_campaign_side_id
            candidate.on_campaign_terminal(
                event_ts_ns=int(quote["decision_ts_ns"]),
                candidate_campaign_ordinal=99,
            )
            assert (
                candidate.adapters["BUY"].current_assignment
                is assignment_id
            )
        elif index == 1:
            assert assignment_id.prospective_campaign_side_id == first_assignment_id
            assert directive.allow_exposure_submission is True
        else:
            assert assignment_id.prospective_campaign_side_id != first_assignment_id
        candidate.on_final_quote_action(
            decision_id=str(quote["decision_id"]),
            side="BUY",
            role=str(quote["role"]),
            exposure_increasing=bool(quote["exposure_increasing"]),
            candidate_action=(
                "place" if directive.allow_exposure_submission else "pause"
            ),
            candidate_price=100.0,
            candidate_quantity=0.001,
            candidate_order_id="",
            event_ts_ns=int(quote["decision_ts_ns"]),
        )
    audit = candidate.finish_replay(
        event_ts_ns=int(base_quote["decision_ts_ns"]) + 10
    )

    assert audit["candidate_campaign_terminal_count"] == 1
    assert audit["untreated_lineage_transition_count"] == 1
    assert audit["adapters"]["BUY"]["assignment_count"] == 2


def test_baseline_index_rejects_multi_day_memory_scope(tmp_path) -> None:
    baseline_dir = tmp_path / "baseline-multi-day"
    capture = RankedToxicityBaselineShadowCaptureV14(
        output_dir=baseline_dir,
        lineage_namespace="multi-day-panel",
        sides=("BUY",),
        chunk_rows=2,
    )
    capture.on_prediction_bucket(**_prediction_kwargs())
    base_quote = _quote_kwargs()
    for index, decision_ts_ns in enumerate(
        (
            int(base_quote["decision_ts_ns"]),
            int(base_quote["decision_ts_ns"]) + 86_400_000_000_000,
        )
    ):
        quote = {
            **base_quote,
            "decision_id": f"BTCUSDC:multi-day-{index}:BUY",
            "decision_ts_ns": decision_ts_ns,
            "untreated_lineage_ordinal": index + 1,
        }
        capture.on_quote_decision(**quote)
        capture.on_final_quote_action(
            decision_id=str(quote["decision_id"]),
            side="BUY",
            role="opener",
            exposure_increasing=True,
            candidate_action="place",
            candidate_price=100.0,
            candidate_quantity=0.001,
            candidate_order_id="",
            event_ts_ns=decision_ts_ns,
        )
    capture.finish_replay(event_ts_ns=int(base_quote["decision_ts_ns"]))

    with pytest.raises(ValueError, match="exactly one UTC-day panel"):
        RankedToxicityGuardAuthoritativeReplayV14(
            baseline_manifest_path=baseline_dir / "manifest.json",
            output_root=tmp_path / "candidate-multi-day",
            frozen_model_sha256=MODEL_SHA256,
            threshold_schedule={"BUY": {DAY: (0.8, THRESHOLD_SHA256)}},
            sides=("BUY",),
        )


def _candidate_lineage_namespace() -> str:
    prospective_suffix = "|BUY|lineage-000000000001"
    for ordinal in range(1, 10_000):
        namespace = f"{DAY}|panel-{ordinal}"
        prospective_id = namespace + prospective_suffix
        assignment = deterministic_campaign_side_assignment(
            seed=FROZEN_RANDOM_SEEDS["BUY"],
            utc_day=DAY,
            side="BUY",
            campaign_opportunity_id=stable_campaign_opportunity_id(
                side="BUY",
                prospective_campaign_side_id=prospective_id,
            ),
            candidate_probability=0.5,
        )
        if assignment.action == CANDIDATE_ACTION:
            return namespace
    raise AssertionError("failed to find deterministic candidate namespace")


def _tiny_replay_inputs():
    start = _day_start_ms() + 10_000
    timestamps = np.arange(start, start + 2_100, 100, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": timestamps,
            "price": np.full(timestamps.size, 65_000.0),
            "quantity": np.zeros(timestamps.size),
            "is_buyer_maker": np.zeros(timestamps.size, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 0.1,
        "rq_min": 0.1,
        "rq_max": 0.1,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 0.0,
        "use_bar_pricing": True,
        "replay_event_clock": "trade",
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "dynamic_fill_hazard_action_enabled": False,
        "ml_enabled": True,
        "max_exec_book_age_s": 0.0,
        "trace_decisions_max": 20,
    }
    ml_ts = np.asarray([start], dtype=np.int64)
    ml_data = (
        ml_ts,
        np.asarray([0.5]),
        np.asarray([0.0]),
        np.asarray([0.0]),
        np.asarray([0.9]),
        np.asarray([0.1]),
    )
    return trades, params, ml_data, start


def test_simulate_tick_runs_two_pass_held_score_full_path(tmp_path) -> None:
    trades, params, ml_data, start = _tiny_replay_inputs()
    namespace = _candidate_lineage_namespace()
    baseline_dir = tmp_path / "baseline"
    capture = RankedToxicityBaselineShadowCaptureV14(
        output_dir=baseline_dir,
        lineage_namespace=namespace,
        sides=("BUY",),
        chunk_rows=5,
    )
    baseline_result = simulate_tick(
        trades,
        np.asarray([start], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        ml_data=ml_data,
        ranked_toxicity_guard_binding=capture,
    )
    baseline_audit = baseline_result["ranked_toxicity_guard_binding_audit"]
    assert baseline_audit["prediction_bucket_count"] == 1
    assert baseline_audit["quote_decision_count"] > 1

    candidate = RankedToxicityGuardAuthoritativeReplayV14(
        baseline_manifest_path=baseline_dir / "manifest.json",
        output_root=tmp_path / "candidate",
        frozen_model_sha256=MODEL_SHA256,
        threshold_schedule={"BUY": {DAY: (0.8, THRESHOLD_SHA256)}},
        sides=("BUY",),
        chunk_rows=5,
    )
    candidate_result = simulate_tick(
        trades,
        np.asarray([start], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        ml_data=ml_data,
        ranked_toxicity_guard_binding=candidate,
    )
    audit = candidate_result["ranked_toxicity_guard_binding_audit"]

    assert audit["baseline_shadow"]["complete"] is True
    assert audit["adapters"]["BUY"]["quote_decision_count"] > 1
    assert audit["adapters"]["BUY"]["held_prediction_reuse_count"] > 0
    assert audit["adapters"]["BUY"]["zero_tolerance_passed"] is True


def test_simulate_tick_routes_guard_cancel_ack_through_authoritative_lifecycle(
    tmp_path,
) -> None:
    trades, params, _, start = _tiny_replay_inputs()
    timestamps = np.arange(start, start + 12_100, 100, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": timestamps,
            "price": np.full(timestamps.size, 65_000.0),
            "quantity": np.zeros(timestamps.size),
            "is_buyer_maker": np.zeros(timestamps.size, dtype=np.uint8),
        }
    )
    ml_data = (
        np.asarray([start, start + 10_000], dtype=np.int64),
        np.asarray([0.5, 0.5]),
        np.asarray([0.0, 0.0]),
        np.asarray([0.0, 0.0]),
        np.asarray([0.1, 0.9]),
        np.asarray([0.1, 0.1]),
    )
    namespace = _candidate_lineage_namespace()
    baseline_dir = tmp_path / "baseline-cancel"
    capture = RankedToxicityBaselineShadowCaptureV14(
        output_dir=baseline_dir,
        lineage_namespace=namespace,
        sides=("BUY",),
        chunk_rows=20,
    )
    baseline_result = simulate_tick(
        trades,
        np.asarray([start], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        ml_data=ml_data,
        ranked_toxicity_guard_binding=capture,
    )
    assert any(
        row["side"] == "BUY" and row["action"] in {"place", "replace", "keep"}
        for row in baseline_result["_decision_trace"]
    ), [
        (row["side"], row["action"], row.get("reason_text"))
        for row in baseline_result["_decision_trace"]
    ]

    candidate_root = tmp_path / "candidate-cancel"
    candidate = RankedToxicityGuardAuthoritativeReplayV14(
        baseline_manifest_path=baseline_dir / "manifest.json",
        output_root=candidate_root,
        frozen_model_sha256=MODEL_SHA256,
        threshold_schedule={"BUY": {DAY: (0.8, THRESHOLD_SHA256)}},
        sides=("BUY",),
        chunk_rows=20,
    )
    result = simulate_tick(
        trades,
        np.asarray([start], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        ml_data=ml_data,
        ranked_toxicity_guard_binding=candidate,
    )
    audit = result["ranked_toxicity_guard_binding_audit"]
    rows = list(
        iter_chunked_parquet_journal(
            candidate_root / "buy" / "manifest.json"
        )
    )

    assert audit["adapters"]["BUY"]["treatment_event_count"] >= 1
    assert any(
        row["event_type"] == "cancel_requested"
        and row["guard_initiated"] is True
        for row in rows
    )
    assert any(
        row["event_type"] == "exchange_terminal"
        and row["terminal_reason"] == "cancel_ack"
        for row in rows
    )
    assert audit["adapters"]["BUY"]["zero_tolerance_passed"] is True
