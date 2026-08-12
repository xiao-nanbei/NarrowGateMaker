import numpy as np
import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")


def _params(*, cancel_latency_ms: int = 500):
    params = narrowgate_cpp.TickReplayParams()
    params.order_size = 0.001
    params.max_inventory = 0.01
    params.requote_interval_s = 1.0
    params.requote_clock_fixed = True
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
