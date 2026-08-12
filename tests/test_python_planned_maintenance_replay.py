from __future__ import annotations

import numpy as np
import pandas as pd

from models.backtest_tick import simulate_tick
from models.tick_data_types import HistoricalBBOData


def _inputs(*, crossing_fill_ts_ms: int | None = None):
    bbo_ts_ms = np.arange(0, 4_001, 100, dtype=np.int64)
    execution_end_ms = 1_500 if crossing_fill_ts_ms is None else 3_000
    ts_ms = np.arange(0, execution_end_ms + 1, 100, dtype=np.int64)
    price = np.full(ts_ms.shape, 100.0, dtype=np.float64)
    quantity = np.zeros(ts_ms.shape, dtype=np.float64)
    buyer_maker = np.ones(ts_ms.shape, dtype=np.uint8)
    if crossing_fill_ts_ms is not None:
        index = int(np.flatnonzero(ts_ms == crossing_fill_ts_ms)[0])
        price[index] = 96.0
        quantity[index] = 10.0
    trades = pd.DataFrame(
        {
            "transact_time": ts_ms,
            "price": price,
            "quantity": quantity,
            "is_buyer_maker": buyer_maker,
        }
    )
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts_ms,
        best_bid=np.full(bbo_ts_ms.size, 99.9),
        best_ask=np.full(bbo_ts_ms.size, 100.1),
        bid_qty=np.ones(bbo_ts_ms.size),
        ask_qty=np.ones(bbo_ts_ms.size),
    )
    return trades, bbo


def _params(*, cancel_latency_ms: int = 500) -> dict[str, object]:
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
        "cancel_order_latency_ms": cancel_latency_ms,
        "planned_quote_stop_ts_ms": 2_000,
        "replay_event_clock_end_ts_ms": 4_000,
        "trace_quotes_max": 100,
        "trace_fills_max": 100,
    }


def _run(*, crossing_fill_ts_ms: int | None = None):
    trades, bbo = _inputs(crossing_fill_ts_ms=crossing_fill_ts_ms)
    return simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        _params(),
        bbo_data=bbo,
    )


def test_python_planned_maintenance_cancels_and_stops_new_quotes() -> None:
    result = _run()

    assert result["planned_quote_stop_triggered"] is True
    assert result["planned_quote_stop_trigger_ts_ms"] == 2_000
    assert result["planned_shutdown_orders_at_trigger"] == 2
    assert result["planned_shutdown_open_order_count"] == 0
    assert result["planned_shutdown_pending_new_order_count"] == 0
    assert result["planned_shutdown_pending_cancel_order_count"] == 0
    assert result["n_requotes"] == 2
    assert sum(
        row.get("cancel_reason") == "planned_maintenance"
        for row in result["_quote_trace"]
    ) == 2


def test_python_planned_maintenance_preserves_fill_risk_until_cancel_ack() -> None:
    result = _run(crossing_fill_ts_ms=2_200)

    assert result["planned_quote_stop_triggered"] is True
    assert result["fills_bid"] == 1
    assert result["fills_while_pending_cancel"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 2_200
    assert result["planned_shutdown_open_order_count"] == 0
    assert result["planned_shutdown_pending_new_order_count"] == 0
    assert result["planned_shutdown_pending_cancel_order_count"] == 0
