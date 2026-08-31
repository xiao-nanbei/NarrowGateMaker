import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models.tick_data_types import HistoricalBBOData
from strategy.quote_core import SPREAD_CAP_COMPRESS

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")


def test_cpp_fails_closed_for_private_fill_visibility_mode():
    with pytest.raises(ValueError, match="Python-only"):
        bt._simulate_tick_cpp(
            None,
            None,
            None,
            {"_private_fill_visibility_latency_samples_ms": [1.0]},
        )


def _params(*, cancel_latency_ms: int = 500):
    params = narrowgate_cpp.TickReplayParams()
    params.order_size = 0.001
    params.max_inventory = 0.01
    params.requote_interval_s = 1.0
    params.requote_clock_fixed = True
    # Keep this stable quote until maintenance; replacement/ACK ownership is
    # tested separately and must not empty the book before the stop boundary.
    params.requote_threshold_bps = 1.0
    params.cancel_order_latency_ms = cancel_latency_ms
    params.maker_fill_prob = 1.0
    params.trace_quotes_max = 100
    params.trace_fills_max = 100
    params.planned_quote_stop_ts_ms = 2_000
    params.quote.gamma = 0.01
    params.quote.kappa = 1.0
    params.quote.tick_size = 0.1
    params.quote.lot_size = 0.001
    params.quote.order_size = params.order_size
    params.quote.max_inventory = params.max_inventory
    params.quote.max_spread_bps = 20.0
    params.quote.spread_cap_mode = SPREAD_CAP_COMPRESS
    return params


def test_planned_maintenance_cancels_and_stops_new_quotes():
    ts = np.arange(0, 4_001, 100, dtype=np.int64)
    price = np.full(ts.shape, 100.0, dtype=np.float64)
    qty = np.zeros(ts.shape, dtype=np.float64)
    maker = np.zeros(ts.shape, dtype=np.uint8)

    result = narrowgate_cpp.simulate_tick_arrays(
        ts, price, qty, maker, _params()
    )
    summary = result.summary

    assert summary.planned_quote_stop_triggered
    assert summary.planned_quote_stop_trigger_ts_ms == 2_000
    assert summary.planned_shutdown_orders_at_trigger > 0
    assert summary.planned_shutdown_open_order_count == 0
    assert summary.planned_shutdown_pending_new_order_count == 0
    assert summary.planned_shutdown_pending_cancel_order_count == 0
    assert summary.n_requotes == 2
    assert any(row.cancel_reason == "planned_maintenance" for row in result.quote_trace)


def test_planned_maintenance_keeps_ack_pending_order_in_fill_risk_set():
    ts = np.arange(0, 3_001, 100, dtype=np.int64)
    price = np.full(ts.shape, 100.0, dtype=np.float64)
    qty = np.zeros(ts.shape, dtype=np.float64)
    maker = np.zeros(ts.shape, dtype=np.uint8)
    fill_idx = int(np.flatnonzero(ts == 2_200)[0])
    price[fill_idx] = 99.0
    qty[fill_idx] = 0.001
    maker[fill_idx] = 1

    result = narrowgate_cpp.simulate_tick_arrays(
        ts, price, qty, maker, _params(cancel_latency_ms=500)
    )
    summary = result.summary

    assert summary.planned_quote_stop_triggered
    assert summary.fills_bid == 1
    assert summary.pending_cancel_fills == 1
    assert summary.planned_shutdown_open_order_count == 0
    assert summary.planned_shutdown_pending_new_order_count == 0
    assert summary.planned_shutdown_pending_cancel_order_count == 0


def test_split_cancel_stops_matching_before_local_ack():
    ts = np.arange(0, 3_001, 100, dtype=np.int64)
    price = np.full(ts.shape, 100.0, dtype=np.float64)
    qty = np.zeros(ts.shape, dtype=np.float64)
    maker = np.zeros(ts.shape, dtype=np.uint8)
    fill_idx = int(np.flatnonzero(ts == 2_200)[0])
    price[fill_idx] = 99.0
    qty[fill_idx] = 0.001
    maker[fill_idx] = 1
    params = _params(cancel_latency_ms=500)
    params.new_order_latency_samples_ms = [900.0]
    params.new_order_exchange_effective_latency_samples_ms = [300.0]
    params.cancel_exchange_effective_latency_samples_ms = [100.0]
    params.cancel_ack_visibility_latency_samples_ms = [400.0]

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, maker, params)
    summary = result.summary

    assert summary.planned_quote_stop_triggered
    assert summary.fills_bid == 0
    planned_cancels = [
        row
        for row in result.quote_trace
        if row.cancel_reason == "planned_maintenance"
    ]
    assert planned_cancels
    assert {row.outcome_ts for row in planned_cancels} == {2_400}
    assert {
        row.activate_ts - row.submit_ts for row in planned_cancels
    } == {300}


def test_python_cpp_split_lifecycle_latency_parity():
    ts = np.arange(0, 3_001, 100, dtype=np.int64)
    price = np.full(ts.shape, 100.0, dtype=np.float64)
    qty = np.zeros(ts.shape, dtype=np.float64)
    buyer_maker = np.ones(ts.shape, dtype=bool)
    fill_idx = int(np.flatnonzero(ts == 2_200)[0])
    price[fill_idx] = 96.0
    qty[fill_idx] = 10.0
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": price,
            "quantity": qty,
            "is_buyer_maker": buyer_maker,
        }
    )
    bbo = HistoricalBBOData(
        ts_ms=ts,
        best_bid=np.full(ts.size, 99.9),
        best_ask=np.full(ts.size, 100.1),
        bid_qty=np.ones(ts.size),
        ask_qty=np.ones(ts.size),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 0.5,
        "rq_min": 0.5,
        "rq_max": 0.5,
        "requote_clock": "fixed",
        "replace_pending_coalesce": True,
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
        # Split lifecycle samples must remain authoritative even when the
        # legacy cancel latency is disabled.
        "cancel_order_latency_ms": 0,
        "planned_quote_stop_ts_ms": 2_000,
        "replay_event_clock_end_ts_ms": 3_000,
        "_new_order_latency_samples_ms": [900.0],
        "_new_order_exchange_effective_latency_samples_ms": [300.0],
        "_cancel_exchange_effective_latency_samples_ms": [100.0],
        "_cancel_ack_visibility_latency_samples_ms": [400.0],
        "trace_quotes_max": 100,
    }

    results = {
        engine: bt._simulate_tick_with_engine(
            engine,
            trades,
            np.asarray([0], dtype=np.int64),
            np.asarray([1.0], dtype=np.float64),
            params,
            bbo_data=bbo,
        )
        for engine in ("python", "cpp")
    }

    for result in results.values():
        assert result["fills_bid"] == 0
        assert result["cancel_latency_split_enabled"] is True
        assert result["decision_pending_coalesce_count"] == 4
        planned_cancels = [
            row
            for row in result["_quote_trace"]
            if row["cancel_reason"] == "planned_maintenance"
        ]
        assert {row["outcome_ts"] for row in planned_cancels} == {2_400}
        assert {
            row["activate_ts"] - row["submit_ts"] for row in planned_cancels
        } == {300}

    pre_ack_trades = trades.copy()
    pre_ack_trades.loc[:, "quantity"] = 0.0
    pre_ack_trades.loc[:, "price"] = 100.0
    pre_ack_trades.loc[pre_ack_trades["transact_time"] == 500, "price"] = 96.0
    pre_ack_trades.loc[pre_ack_trades["transact_time"] == 500, "quantity"] = 10.0
    for engine in ("python", "cpp"):
        with pytest.raises(RuntimeError, match="pre-ACK exchange fill"):
            bt._simulate_tick_with_engine(
                engine,
                pre_ack_trades,
                np.asarray([0], dtype=np.int64),
                np.asarray([1.0], dtype=np.float64),
                params,
                bbo_data=bbo,
            )

    delayed_params = {
        **params,
        "replay_purpose": "diagnostic",
        "_decision_to_gateway_latency_samples_ms": [100.0],
        "decision_to_gateway_latency_seed": 73,
    }
    delayed_results = {
        engine: bt._simulate_tick_with_engine(
            engine,
            trades,
            np.asarray([0], dtype=np.int64),
            np.asarray([1.0], dtype=np.float64),
            delayed_params,
            bbo_data=bbo,
        )
        for engine in ("python", "cpp")
    }
    for result in delayed_results.values():
        planned_cancels = [
            row
            for row in result["_quote_trace"]
            if row["cancel_reason"] == "planned_maintenance"
        ]
        assert planned_cancels
        assert {row["outcome_ts"] for row in planned_cancels} == {2_400}
        assert {
            row["activate_ts"] - row["submit_ts"]
            for row in planned_cancels
        } == {400}
        assert result["decision_to_gateway_latency_authority"] == (
            "diagnostic_only"
        )
        assert result["decision_to_gateway_latency_seed"] == 73
        assert result["decision_market_snapshot_clock"] == (
            "decision_time_frozen"
        )
    for key in (
        "pnl",
        "final_inventory",
        "fills_bid",
        "fills_ask",
        "n_requotes",
        "decision_pending_coalesce_count",
    ):
        assert delayed_results["cpp"][key] == pytest.approx(
            delayed_results["python"][key]
        )

    # A one-element profile cannot detect compute/REST seed cross-wiring.
    # Keep the order path fixed and vary both seeds independently.
    no_fill_trades = trades.copy()
    no_fill_trades.loc[:, "quantity"] = 0.0
    compute_samples = np.asarray([17.0, 43.0, 89.0])
    effective_samples = np.asarray([31.0, 53.0, 79.0])
    rest_draws = {}
    compute_draws = {}
    for rest_seed in (59, 103):
        for compute_seed in (73, 91):
            seeded_params = {
                **delayed_params,
                "requote_interval": 10.0,
                "rq_min": 10.0,
                "rq_max": 10.0,
                "latency_seed": rest_seed,
                "decision_to_gateway_latency_seed": compute_seed,
                "_decision_to_gateway_latency_samples_ms": compute_samples,
                "_new_order_exchange_effective_latency_samples_ms": effective_samples,
                "_new_order_latency_samples_ms": [311.0, 533.0, 797.0],
                "_cancel_exchange_effective_latency_samples_ms": [23.0, 47.0, 83.0],
                "_cancel_ack_visibility_latency_samples_ms": [223.0, 447.0, 683.0],
            }
            traces = {}
            for engine in ("python", "cpp"):
                result = bt._simulate_tick_with_engine(
                    engine,
                    no_fill_trades,
                    np.asarray([0], dtype=np.int64),
                    np.asarray([1.0], dtype=np.float64),
                    seeded_params,
                    bbo_data=bbo,
                )
                traces[engine] = sorted(
                    (row["side"], row["submit_ts"], row["activate_ts"], row["outcome_ts"])
                    for row in result["_quote_trace"]
                )
                assert len(traces[engine]) == 2
            assert traces["cpp"] == traces["python"]
            for side, submit_ts, activate_ts, _ in traces["cpp"]:
                compute_delay = bt._deterministic_decision_to_gateway_latency_ms(
                    compute_samples, seed=compute_seed, decision_ts_ms=submit_ts
                )
                rest_delay = activate_ts - submit_ts - compute_delay
                expected_rest = bt.deterministic_latency_ms(
                    base_ms=0, jitter_ms=0, samples_ms=effective_samples,
                    seed=rest_seed, event_ts_ms=submit_ts, side=side,
                    operation=bt._LATENCY_NEW, order_ts_ms=submit_ts,
                )
                assert rest_delay == expected_rest
                rest_draws[rest_seed, compute_seed, side] = rest_delay
                compute_draws[rest_seed, compute_seed, side] = compute_delay
    for side in ("BUY", "SELL"):
        for rest_seed in (59, 103):
            assert rest_draws[rest_seed, 73, side] == rest_draws[rest_seed, 91, side]
        for compute_seed in (73, 91):
            assert compute_draws[59, compute_seed, side] == compute_draws[103, compute_seed, side]
