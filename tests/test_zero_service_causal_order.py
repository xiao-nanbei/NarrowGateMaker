"""Equal wall-clock timestamps do not erase already-established event order."""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
import pytest

from models.backtest_tick import simulate_tick
from models.replay.runtime_checkpoint_io import (
    load_trusted_runtime_checkpoint,
    save_runtime_checkpoint,
)
from models.tick_data_types import HistoricalBBOData
from tests.test_exchange_book_replay import BASE_MS, _event
from tests.test_python_planned_maintenance_replay import _async_fifo_params, _params
from tests.test_tick_runtime_checkpoint import assert_same


def _scenario(*, future_trade=False):
    offsets = np.arange(200, 3_201, 100, dtype=np.int64)
    price = np.full(len(offsets), 100.0)
    quantity = np.zeros(len(offsets))
    # This trade is consumed before the first quote wake at the same ms.
    price[0], quantity[0] = 96.0, 10.0
    if future_trade:
        later = np.flatnonzero(offsets == 1_300)[0]
        price[later], quantity[later] = 96.0, 10.0
    trades = pd.DataFrame(
        dict(
            transact_time=BASE_MS + offsets,
            price=price,
            quantity=quantity,
            is_buyer_maker=np.ones(len(offsets), dtype=np.uint8),
        )
    )
    bbo = HistoricalBBOData(
        ts_ms=BASE_MS + offsets,
        best_bid=np.full(len(offsets), 99.9),
        best_ask=np.full(len(offsets), 100.1),
        bid_qty=np.ones(len(offsets)),
        ask_qty=np.ones(len(offsets)),
    )
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(("bid", 950, 1.0), ("bid", 999, 1.0),
                    ("ask", 1001, 1.0), ("ask", 1050, 1.0)),
            last_update_id=100,
            ordinal=1,
            receive_delay_ms=0,
        )
    ]
    for index, offset in enumerate(offsets, start=1):
        events.append(
            _event(
                int(offset),
                event_type="delta",
                levels=(("bid", 999, float(index + 1)),
                        ("ask", 1001, float(index + 1))),
                first_update_id=100 + index,
                final_update_id=100 + index,
                previous_final_update_id=99 + index,
                ordinal=index + 1,
                receive_delay_ms=0,
            )
        )
    params = {
        **_params(cancel_latency_ms=0),
        **_async_fifo_params(new=(0.0, 0.0, 0.0), cancel=(0.0, 0.0, 0.0)),
        "replay_main_loop_sleep_ms": 0,
        "replay_main_loop_wait_mode": "event_deadline",
        "new_order_latency_ms": 0,
        "replay_event_clock_end_ts_ms": BASE_MS + 3_200,
        "trace_local_order_lifecycle_max": 1000,
        "trace_quotes_max": 1000,
        "exchange_book_queue_mode": "diagnostic",
        "replace_terminal_continuation": True,
        "requote_threshold_bps": 1.0,
    }
    return (trades, np.array([BASE_MS - 1000]), np.array([1.0]), params), {
        "bbo_data": bbo,
        "exchange_book_event_tape": events,
    }


def _observed_run(args, kwargs):
    observations = []

    def observe(frame, event, _result):
        name = frame.f_code.co_name
        if event == "return" and name == "_capture_zero_service_execution_context":
            order = frame.f_locals["order"]
            context = order.get("zero_service_execution_context")
            if context is not None:
                observations.append(("capture", int(order["trace_id"]), context))
        elif event == "call" and name in {
            "_record_order_lifecycle_submit", "_publish_local_new_ack",
            "_advance_async_rest_worker", "_record_order_lifecycle_reject",
        }:
            order = frame.f_locals.get("order")
            completed = frame.f_locals.get("completed")
            if name == "_advance_async_rest_worker":
                if completed is not None:
                    observations.append(
                        ("http_return", int(completed["order"]["trace_id"]),
                         int(frame.f_locals["now_ts"]))
                    )
            elif order is not None:
                observations.append((name, int(order["trace_id"]), None))

    previous = sys.getprofile()
    try:
        sys.setprofile(observe)
        result = simulate_tick(*args, **kwargs)
    finally:
        sys.setprofile(previous)
    return result, observations


def test_equal_ms_consumed_book_is_causal_predecessor_not_prior_ms():
    args, kwargs = _scenario()
    result, observations = _observed_run(args, kwargs)
    captures = [row for row in observations if row[0] == "capture"]
    assert captures, observations
    first = captures[0][2]
    assert first["activation_ts_ms"] == BASE_MS + 200
    assert first["evidence"]["asof_ns"] == (BASE_MS + 200) * 1_000_000
    assert first["book"][2:4] == (2.0, 2.0)
    assert first["evidence"]["valid"]
    assert first["book"][:2] == pytest.approx((99.9, 100.1))
    # The crossing trade at exactly the request timestamp happened before
    # the request. It cannot be reused to fill the newly-created order.
    assert result["fills_bid"] == result["fills_ask"] == 0
    assert result["exchange_book_events_consumed"] == len(kwargs["exchange_book_event_tape"])
    first_order = captures[0][1]
    labels = [row[0] for row in observations if row[1] == first_order]
    assert labels.index("capture") < labels.index("_record_order_lifecycle_submit")
    assert labels.index("_record_order_lifecycle_submit") < labels.index("_publish_local_new_ack")
    assert labels.index("_publish_local_new_ack") < labels.index("http_return")
    second_capture = observations.index(captures[1])
    first_return = next(i for i, row in enumerate(observations)
                        if row[:2] == ("http_return", first_order))
    assert first_return < second_capture


@pytest.mark.parametrize("poll100", [False, True])
def test_zero_service_later_trade_and_disk_restore(tmp_path, poll100):
    args, kwargs = _scenario(future_trade=True)
    if poll100:
        params = dict(args[3], replay_main_loop_wait_mode="poll",
                      replay_main_loop_sleep_ms=100, zero_service_causal_order=True)
        args = (*args[:3], params)
    full = simulate_tick(*args, **kwargs)
    assert full["fills_bid"] > 0
    assert all(row["fill_ts"] > BASE_MS + 200 for row in full["_fill_trace"])
    partial = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=BASE_MS + 201)
    checkpoint = partial["_replay_checkpoint"]
    assert any(
        order.get("zero_service_execution_context")
        for order in checkpoint["runtime"].bid_orders + checkpoint["runtime"].ask_orders
    )
    path = tmp_path / "zero-service-causal.checkpoint"
    save_runtime_checkpoint(path, checkpoint)
    resumed = simulate_tick(
        *args, **kwargs,
        resume_checkpoint=load_trusted_runtime_checkpoint(path),
        consume_resume_checkpoint=True,
    )
    assert_same(resumed, full)


def test_original_polling_mode_never_gets_zero_service_context():
    args, kwargs = _scenario(future_trade=True)
    params = dict(args[3], replay_main_loop_wait_mode="poll", replay_main_loop_sleep_ms=100)
    args = (*args[:3], params)
    explicit, observations = _observed_run(args, kwargs)
    assert not any(row[0] == "capture" for row in observations)
    params = dict(params)
    del params["replay_main_loop_wait_mode"]
    implicit = simulate_tick(*args[:3], params, **kwargs)
    assert_same(explicit, implicit)


def test_poll100_explicit_zero_context_preserves_causal_order():
    args, kwargs = _scenario()
    params = dict(args[3], replay_main_loop_wait_mode="poll",
                  replay_main_loop_sleep_ms=100, zero_service_causal_order=True)
    result, observations = _observed_run((*args[:3], params), kwargs)
    captures = [r for r in observations if r[0] == "capture"]
    assert captures
    assert captures[0][2]["evidence"]["valid"]
    assert captures[0][2]["evidence"]["asof_ns"] <= captures[0][2]["activation_ts_ms"] * 1_000_000
    assert result["fills_bid"] == result["fills_ask"] == 0


def test_consumed_equal_ms_book_change_governs_post_only_rejection():
    args, kwargs = _scenario()
    kwargs["exchange_book_event_tape"] = [
        kwargs["exchange_book_event_tape"][0],
        _event(
            200, event_type="snapshot",
            levels=(("bid", 950, 2.0), ("ask", 960, 2.0)),
            last_update_id=200, ordinal=2, receive_delay_ms=0,
        ),
    ]
    _, observations = _observed_run(args, kwargs)
    first_capture = next(row for row in observations if row[0] == "capture")
    first_order, context = first_capture[1:]
    assert context["book"][:2] == (95.0, 96.0)
    assert context["evidence"]["valid"]
    labels = [row[0] for row in observations if row[1] == first_order]
    assert "_record_order_lifecycle_reject" in labels
    assert "_publish_local_new_ack" not in labels


def test_missing_execution_book_is_not_promoted_to_valid_by_zero_service():
    args, kwargs = _scenario()
    # There is no initialized native snapshot when the first request starts.
    kwargs["exchange_book_event_tape"] = []
    _, observations = _observed_run(args, kwargs)
    captures = [row for row in observations if row[0] == "capture"]
    assert captures
    for _, order_id, context in captures:
        assert not context["evidence"]["valid"]
        assert context["book"][-1] == "unavailable_book"
        assert not context["queue"].strict_usable
        assert not any(row[:2] == ("_publish_local_new_ack", order_id)
                       for row in observations)
