from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.local_action_uplift import annotate_native_action_support
from research.families.f07_active_order_continuation.audit.local_order_value_panel import add_competing_risk_labels
from research.families.f07_active_order_continuation.audit.queue_value_models import (
    EmpiricalMicropriceArtifact,
    EmpiricalMicropriceCell,
    MODEL_BUNDLE_SCHEMA_VERSION,
    QUEUE_SCHEMA_VERSION,
    QueueEventModel,
    QueueReactiveHawkesArtifact,
    QueueValueModelBundle,
    QueueValueSideModel,
    QueueValueStateConfig,
)
from models.backtest_tick import simulate_tick
from models.tick_data_types import (
    HistoricalBBOData,
    HistoricalExchangeBookEvent,
)

BASE_MS = 1_700_000_000_000


def _write_artifacts(tmp_path, *, native_events: bool = False):
    event_columns = {
        "adverse_market_order": "adverse_market_order_count",
        "cancel": (
            "exchange_book_cancel_count"
            if native_events
            else "cancel_count"
        ),
        "refill": (
            "exchange_book_refill_count"
            if native_events
            else "refill_count"
        ),
    }
    queue = QueueReactiveHawkesArtifact(
        schema_version=QUEUE_SCHEMA_VERSION,
        artifact_id="queue-test",
        input_scope="local_only",
        categorical_features=("side",),
        numeric_edges={
            "spread_ticks": (),
            "book_imbalance": (),
            "queue_fraction_left": (),
        },
        exposure_column="interval_ms",
        timestamp_column="interval_end_ts_ns",
        group_columns=("day", "side"),
        event_columns=event_columns,
        event_models={
            "adverse_market_order": QueueEventModel(
                "adverse_market_order",
                10.0,
                100.0,
                0.0,
                0.0,
                {"side=BUY|spread_ticks=b0|book_imbalance=b0|queue_fraction_left=b0": 10.0},
            ),
            "cancel": QueueEventModel(
                "cancel",
                5.0,
                100.0,
                0.0,
                0.0,
                {"side=BUY|spread_ticks=b0|book_imbalance=b0|queue_fraction_left=b0": 5.0},
            ),
            "refill": QueueEventModel(
                "refill",
                0.1,
                100.0,
                0.0,
                0.0,
                {"side=BUY|spread_ticks=b0|book_imbalance=b0|queue_fraction_left=b0": 0.1},
            ),
        },
        training_rows=100,
        training_days=("2026-01-01",),
    )
    micro = EmpiricalMicropriceArtifact(
        schema_version="empirical_microprice.v1",
        artifact_id="micro-test",
        input_scope="local_only",
        tick_size=0.1,
        horizon_ms=1_000,
        imbalance_edges=(),
        spread_edges=(),
        min_cell_rows=1,
        max_abs_ticks=2.0,
        global_cell=EmpiricalMicropriceCell(
            rows=100,
            expected_mid_delta_ticks=-1.0,
            p_up=0.05,
            p_down=0.90,
            p_flat=0.05,
        ),
        cells={},
        training_rows=100,
        training_days=("2026-01-01",),
    )
    queue_path = tmp_path / "queue.json"
    micro_path = tmp_path / "micro.json"
    queue.save(queue_path)
    micro.save(micro_path)
    return queue_path, micro_path


def _write_bundle(tmp_path, *, native_events: bool = False):
    queue_path, micro_path = _write_artifacts(
        tmp_path,
        native_events=native_events,
    )
    queue = QueueReactiveHawkesArtifact.load(queue_path)
    micro = EmpiricalMicropriceArtifact.load(micro_path)
    side_model = QueueValueSideModel(
        queue_artifact=queue,
        microprice_artifact=micro,
        state_config=QueueValueStateConfig(
            entry_expected_ticks=-0.1,
            exit_expected_ticks=0.0,
            entry_adverse_probability=0.52,
            exit_adverse_probability=0.50,
            entry_flow_ratio=1.0,
            exit_flow_ratio=0.9,
        ),
        calibration={"calibration_passed": True},
    )
    bundle = QueueValueModelBundle(
        schema_version=MODEL_BUNDLE_SCHEMA_VERSION,
        bundle_id="queue-value-test",
        input_scope="local_only",
        fit_days=("2026-01-01",),
        calibration_days=("2026-01-03",),
        internal_embargo_days=("2026-01-02",),
        sides={"BUY": side_model, "SELL": side_model},
        calibration_passed=True,
        historical_visibility="test",
    )
    bundle_path = tmp_path / "bundle.json"
    bundle.save(bundle_path)
    return bundle_path


def test_queue_value_family_intervenes_on_active_add_order(tmp_path) -> None:
    bundle_path = _write_bundle(tmp_path)
    trades = pd.DataFrame(
        {
            "transact_time": np.arange(0, 8_000, 1_000, dtype=np.int64),
            "price": np.full(8, 100.0),
            "quantity": np.zeros(8),
            "is_buyer_maker": np.ones(8, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "initial_inventory": 0.001,
        "initial_entry_price": 100.0,
        "initial_live_state": {
            "active_orders": [
                {
                    "side": "BUY",
                    "price": 99.0,
                    "quantity": 0.001,
                    "remaining": 0.001,
                    "submit_ts_ms": 0,
                    "event_ts_ms": 0,
                    "status": "OPEN",
                    "mid_at_quote": 100.0,
                }
            ],
            "campaign": {"active": True, "age_s": 10.0},
        },
        "replay_event_clock": "trade",
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "queue_value_keep_cancel_enabled": True,
        "trace_queue_value_keep_cancel_max": 10,
        "queue_value_keep_cancel_probabilities": {
            "keep": 0.5,
            "cancel_until_state_exit": 0.5,
        },
        "queue_value_keep_cancel_seed": 7,
        "queue_value_model_bundle_path": str(bundle_path),
        "queue_value_fill_horizon_ms": 1_000,
        "queue_value_price_jump_ticks": 1.0,
    }

    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
    )
    trace = result["_queue_value_keep_cancel_trace"]

    assert result["queue_value_keep_cancel_assignment_count"] == 1
    assert len(trace) == 1
    assert trace[0]["order_active_before"] == 1
    assert trace[0]["inventory_role"] == "add"
    assert trace[0]["input_scope"] == "local_only"
    assert trace[0]["action"] in {"keep", "cancel_until_state_exit"}
    assert abs(trace[0]["reward_identity_error"]) < 1e-12


def test_cancel_reenter_waits_for_ack_then_returns_to_baseline(tmp_path) -> None:
    bundle_path = _write_bundle(tmp_path)
    trades = pd.DataFrame(
        {
            "transact_time": np.arange(0, 8_000, 1_000, dtype=np.int64),
            "price": np.full(8, 100.0),
            "quantity": np.zeros(8),
            "is_buyer_maker": np.ones(8, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "initial_inventory": 0.001,
        "initial_entry_price": 100.0,
        "initial_live_state": {
            "active_orders": [
                {
                    "side": "BUY",
                    "price": 99.0,
                    "quantity": 0.001,
                    "remaining": 0.001,
                    "submit_ts_ms": 0,
                    "event_ts_ms": 0,
                    "status": "OPEN",
                    "mid_at_quote": 100.0,
                }
            ],
            "campaign": {"active": True, "age_s": 10.0},
        },
        "replay_event_clock": "trade",
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "cancel_order_latency_ms": 500,
        "queue_value_keep_cancel_enabled": True,
        "queue_value_action_family": "queue_value_cancel_reenter",
        "queue_value_keep_cancel_sides": ["BUY"],
        "trace_queue_value_keep_cancel_max": 10,
        "queue_value_keep_cancel_probabilities": {
            "keep": 0.5,
            "cancel_then_baseline_reenter": 0.5,
        },
        "queue_value_keep_cancel_seed": 7,
        "queue_value_model_bundle_path": str(bundle_path),
        "queue_value_fill_horizon_ms": 1_000,
        "queue_value_price_jump_ticks": 1.0,
    }

    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
    )
    trace = result["_queue_value_keep_cancel_trace"]

    assert result["queue_value_action_family"] == "queue_value_cancel_reenter"
    assert result["queue_value_keep_cancel_assignment_count"] == 1
    assert result[
        "queue_value_keep_cancel_cancel_then_baseline_reenter_count"
    ] == 1
    assert trace[0]["action"] == "cancel_then_baseline_reenter"
    assert trace[0]["cancel_ack_ts_ns"] >= trace[0]["cancel_request_ts_ns"]
    assert trace[0]["state_exit_reason"] == "cancel_ack_reenter"
    assert trace[0]["reentry_order_submit_count"] == 1
    assert trace[0]["reentry_order_id"] != trace[0]["order_id"]
    assert trace[0]["reentry_submit_ts_ns"] >= trace[0]["cancel_ack_ts_ns"]
    assert trace[0]["state_exit_ts_ns"] == trace[0]["reentry_submit_ts_ns"]
    assert trace[0]["reward_definition"] == "decision_to_flat_or_day_end_mtm"
    assert (
        trace[0]["campaign_cost_definition"]
        == "accounting_residual_not_separately_identified"
    )
    assert result["decision_place_count"] > 0


def test_baseline_replay_emits_one_causal_observation_per_active_order() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": np.arange(0, 8_000, 1_000, dtype=np.int64),
            "price": np.full(8, 100.0),
            "quantity": np.zeros(8),
            "is_buyer_maker": np.ones(8, dtype=np.uint8),
        }
    )
    bbo_ts = np.arange(0, 7_501, 500, dtype=np.int64)
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts,
        best_bid=np.full(bbo_ts.size, 99.9),
        best_ask=np.full(bbo_ts.size, 100.1),
        bid_qty=np.where(np.arange(bbo_ts.size) % 2 == 0, 1.0, 2.0),
        ask_qty=np.where(np.arange(bbo_ts.size) % 2 == 0, 2.0, 1.0),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "trace_local_order_value_max": 20,
        "local_order_value_fill_horizon_ms": 1_000,
        "local_order_value_price_jump_ticks": 1.0,
    }

    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        bbo_data=bbo,
    )
    source = pd.DataFrame(result["_local_order_value_trace"])
    panel = add_competing_risk_labels(source)

    assert not panel.empty
    assert panel["order_id"].is_unique
    assert (panel["feature_ready_ts_ns"] <= panel["decision_ts_ns"]).all()
    assert (panel["interval_ms"] > 0.0).all()
    assert set(panel["input_scope"]) == {"local_only"}
    assert panel["cancel_count"].sum() > 0.0
    assert panel["refill_count"].sum() > 0.0
    assert (
        panel[
            [
                "event_favorable_fill",
                "event_adverse_fill",
                "event_cancel",
                "event_adverse_price_jump",
                "event_campaign_repair",
                "event_censored",
            ]
        ].sum(axis=1)
        == 1
    ).all()


def _native_queue_action_params(bundle_path) -> dict:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "initial_inventory": 0.001,
        "initial_entry_price": 100.0,
        "initial_live_state": {
            "active_orders": [
                {
                    "side": "BUY",
                    "price": 99.0,
                    "quantity": 0.001,
                    "remaining": 0.001,
                    "submit_ts_ms": BASE_MS + 100,
                    "event_ts_ms": BASE_MS + 100,
                    "status": "OPEN",
                    "mid_at_quote": 100.0,
                }
            ],
            "campaign": {"active": True, "age_s": 10.0},
        },
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "queue_value_keep_cancel_enabled": True,
        "trace_queue_value_keep_cancel_max": 10,
        "queue_value_keep_cancel_probabilities": {
            "keep": 0.5,
            "cancel_until_state_exit": 0.5,
        },
        "queue_value_keep_cancel_seed": 7,
        "queue_value_model_bundle_path": str(bundle_path),
        "queue_value_fill_horizon_ms": 1_000,
        "queue_value_price_jump_ticks": 1.0,
        "exchange_book_queue_mode": "diagnostic",
    }


def _native_snapshot(*, include_order_level: bool) -> tuple:
    levels = [
        ("bid", 980, 3.0),
        ("ask", 1010, 4.0),
    ]
    if include_order_level:
        levels.append(("bid", 990, 5.0))
    return (
        HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="snapshot",
            exchange_ts_ns=BASE_MS * 1_000_000,
            final_update_id=100,
            last_update_id=100,
            levels=tuple(levels),
        ),
    )


def test_native_event_bundle_requires_exchange_book_scheduler(tmp_path) -> None:
    bundle_path = _write_bundle(tmp_path, native_events=True)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200],
                dtype=np.int64,
            ),
            "price": np.full(2, 100.0),
            "quantity": np.zeros(2),
            "is_buyer_maker": np.ones(2, dtype=np.uint8),
        }
    )
    params = _native_queue_action_params(bundle_path)
    params["exchange_book_queue_mode"] = "disabled"

    with pytest.raises(
        ValueError,
        match="requires the strategy-independent exchange-book scheduler",
    ):
        simulate_tick(
            trades,
            np.asarray([BASE_MS], dtype=np.int64),
            np.asarray([1.0], dtype=np.float64),
            params,
        )


def test_native_event_bundle_records_exact_level_runtime_source(
    tmp_path,
) -> None:
    bundle_path = _write_bundle(tmp_path, native_events=True)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200],
                dtype=np.int64,
            ),
            "price": np.full(2, 100.0),
            "quantity": np.zeros(2),
            "is_buyer_maker": np.ones(2, dtype=np.uint8),
        }
    )
    params = _native_queue_action_params(bundle_path)
    native_events = (
        *_native_snapshot(include_order_level=True),
        HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="delta",
            exchange_ts_ns=(BASE_MS + 300) * 1_000_000,
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            levels=(("bid", 990, 2.0),),
        ),
    )

    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=native_events,
    )

    assert result["queue_value_runtime_event_source"] == {
        "BUY": "native_exchange_exact_level",
        "SELL": "native_exchange_exact_level",
    }
    assert result["queue_value_runtime_event_counts"]["BUY"]["cancel"] == 1
    assert result["queue_value_runtime_event_counts"]["BUY"]["refill"] == 0


def test_queue_action_preserves_denominator_when_native_seed_is_unknown(
    tmp_path,
) -> None:
    bundle_path = _write_bundle(tmp_path)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200, BASE_MS + 2_200],
                dtype=np.int64,
            ),
            "price": np.full(3, 100.0),
            "quantity": np.zeros(3),
            "is_buyer_maker": np.ones(3, dtype=np.uint8),
        }
    )
    params = _native_queue_action_params(bundle_path)

    exact = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=_native_snapshot(
            include_order_level=True
        ),
    )
    missing = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=_native_snapshot(
            include_order_level=False
        ),
    )

    assert exact["queue_value_keep_cancel_assignment_count"] == 1
    assert missing["queue_value_keep_cancel_assignment_count"] == 1
    record = exact["_queue_value_keep_cancel_trace"][0]
    assert record["simulator_queue_source"] == "native_exchange_book"
    assert record["queue_source"] == "delayed_policy_topn_or_fitted"
    assert record["exchange_book_queue_status"] == "exact"
    assert record["exchange_book_queue_path_valid"] == 1
    assert record["simulator_queue_init"] == 5.0
    missing_record = missing["_queue_value_keep_cancel_trace"][0]
    assert missing_record["exchange_book_queue_status"] == "unknown"
    assert missing_record["native_exchange_seed_supported_at_decision"] == 0
    assert missing_record["native_campaign_predecision_supported"] == 0


def test_strict_native_mode_marks_unknown_seed_without_dropping_day(
    tmp_path,
) -> None:
    bundle_path = _write_bundle(tmp_path)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200],
                dtype=np.int64,
            ),
            "price": np.full(2, 100.0),
            "quantity": np.zeros(2),
            "is_buyer_maker": np.ones(2, dtype=np.uint8),
        }
    )
    params = _native_queue_action_params(bundle_path)
    params["exchange_book_queue_mode"] = "strict"

    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=_native_snapshot(
            include_order_level=False
        ),
    )

    assert result["exchange_book_queue_missing_count"] >= 1
    assert result["queue_value_keep_cancel_assignment_count"] == 1
    record = result["_queue_value_keep_cancel_trace"][0]
    assert record["native_exchange_seed_supported_at_decision"] == 0


def test_same_ms_cancel_ack_and_crossing_trade_is_censored_and_fill_first(
    tmp_path,
) -> None:
    bundle_path = _write_bundle(tmp_path)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 500, BASE_MS + 1_200],
                dtype=np.int64,
            ),
            "price": np.asarray([100.0, 99.0, 100.0]),
            "quantity": np.asarray([0.0, 10.0, 0.0]),
            "is_buyer_maker": np.ones(3, dtype=np.uint8),
        }
    )
    params = _native_queue_action_params(bundle_path)
    params.update(
        {
            "requote_interval": 0.1,
            "rq_min": 0.1,
            "rq_max": 0.1,
            "cancel_order_latency_ms": 300,
        }
    )

    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=_native_snapshot(
            include_order_level=True
        ),
    )

    assert result["queue_value_keep_cancel_assignment_count"] == 1
    assert result["exchange_book_cancel_trade_ambiguous_order_count"] == 1
    record = result["_queue_value_keep_cancel_trace"][0]
    assert record["action"] == "cancel_until_state_exit"
    assert record["exchange_book_queue_path_valid"] == 0
    assert record["exchange_book_queue_ambiguous"] == 1
    assert (
        record["exchange_book_queue_invalidated_reason"]
        == "same_ms_cancel_ack_trade_ambiguity"
    )
    assert record["intervention_fill_count"] == 1
    assert record["fill_ts_ns"] == (BASE_MS + 500) * 1_000_000


@pytest.mark.parametrize(
    ("delta_tick", "expected_ambiguous"),
    [(990, True), (980, False)],
)
def test_same_ms_cancel_ack_and_native_book_change_is_explicitly_censored(
    tmp_path,
    delta_tick: int,
    expected_ambiguous: bool,
) -> None:
    bundle_path = _write_bundle(tmp_path)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200],
                dtype=np.int64,
            ),
            "price": np.full(2, 100.0),
            "quantity": np.zeros(2),
            "is_buyer_maker": np.ones(2, dtype=np.uint8),
        }
    )
    params = _native_queue_action_params(bundle_path)
    params["cancel_order_latency_ms"] = 300
    native_events = (
        *_native_snapshot(include_order_level=True),
        HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="delta",
            exchange_ts_ns=(BASE_MS + 500) * 1_000_000,
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            levels=(("bid", delta_tick, 2.0),),
        ),
    )

    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=native_events,
    )

    record = result["_queue_value_keep_cancel_trace"][0]
    assert bool(
        result["exchange_book_cancel_book_ambiguous_order_count"]
    ) is expected_ambiguous
    assert bool(record["exchange_book_queue_ambiguous"]) is expected_ambiguous
    if expected_ambiguous:
        assert record["exchange_book_queue_invalidated_reason"] == (
            "same_ms_cancel_ack_exchange_book_ambiguity"
        )


def test_native_action_support_keeps_post_treatment_censoring_explicit() -> None:
    panel = pd.DataFrame(
        [
            {
                "simulator_queue_source": "native_exchange_book",
                "exchange_book_queue_status": "exact",
                "exchange_book_queue_path_valid": 1,
                "exchange_book_queue_ambiguous": 0,
                "exchange_book_queue_invalidated_reason": "",
            },
            {
                "simulator_queue_source": "native_exchange_book",
                "exchange_book_queue_status": "known_zero",
                "exchange_book_queue_path_valid": 0,
                "exchange_book_queue_ambiguous": 1,
                "exchange_book_queue_invalidated_reason": (
                    "same_ms_exchange_book_ambiguity"
                ),
            },
        ]
    )

    annotated, summary = annotate_native_action_support(panel)

    assert annotated["native_exchange_seed_supported"].tolist() == [1, 1]
    assert annotated["native_exchange_outcome_supported"].tolist() == [1, 0]
    assert annotated["native_exchange_support_reason"].tolist() == [
        "supported",
        "same_ms_exchange_book_ambiguity",
    ]
    assert summary["seed_support_ratio"] == pytest.approx(1.0)
    assert summary["outcome_support_ratio"] == pytest.approx(0.5)
    assert summary["seed_gate"] is True
    assert summary["path_gate"] is False
