"""Synthetic consumer tests; no market records or experimental parameters."""
import pickle

import numpy as np
import pytest

from models.backtest_tick import simulate_tick
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from strategy.quote_schedule import strict_ready_ms
from tests.test_python_planned_maintenance_replay import _inputs, _params, _async_fifo_params
from tests.test_tick_runtime_checkpoint import assert_same


def scenario(mode="state_event"):
    trades, original = _inputs(crossing_fill_ts_ms=1100)
    base = 10_000
    trades["transact_time"] += base
    bbo = HistoricalBBOData(ts_ms=original.ts_ms + base,
        best_bid=original.best_bid, best_ask=original.best_ask,
        bid_qty=original.bid_qty, ask_qty=original.ask_qty)
    depth = HistoricalL2Data(ts_ms=bbo.ts_ms,
        bid_px=bbo.best_bid[:, None], ask_px=bbo.best_ask[:, None],
        bid_qty=bbo.bid_qty[:, None], ask_qty=bbo.ask_qty[:, None])

    def clocks(ms):
        ns = np.asarray(ms, dtype=np.int64) * 1_000_000
        return dict(exchange_ts_ns=ns, receive_ts_ns=ns.copy(), feature_ready_ts_ns=ns.copy())

    variance = clocks([base - 1000])
    variance["receive_ts_ns"] += 1_000_000_000
    variance["feature_ready_ts_ns"] += 1_000_000_000
    params = {**_params(), **_async_fifo_params(),
        "quote_schedule_mode": mode, "use_bar_pricing": False,
        "planned_quote_stop_ts_ms": base + 2000,
        "replay_event_clock_end_ts_ms": base + 4000,
        "_decision_to_gateway_latency_samples_ms": [40.],
        "_pre_snapshot_compute_latency_samples_ms": [10.],
        "exec_book_visibility_mode": "message_schedule",
        "_exec_message_delivery": dict(bbo=clocks(bbo.ts_ms), depth=clocks(depth.ts_ms),
            variance=variance, trade={**clocks([base]), "last_child_row_index": np.asarray([0])}),
        "trace_decisions_max": 500,
    }
    return (trades, np.asarray([base-1000]), np.asarray([1.]), params), dict(bbo_data=bbo, l2_data=depth)


def test_strict_visibility_projection_not_epsilon():
    assert strict_ready_ms(10_000_000) == 11
    assert strict_ready_ms(10_000_001) == 11
    assert strict_ready_ms(10_999_999) == 11


def test_disabled_mode_exact_original_outputs():
    args, kw = scenario("existing")
    with_mode = simulate_tick(*args, **kw)
    del args[3]["quote_schedule_mode"]
    assert_same(simulate_tick(*args, **kw), with_mode)


@pytest.mark.parametrize("cut", [10001, 10005, 10010, 10040, 11001, 12001])
def test_state_event_business_checkpoint_and_clone(cut):
    args, kw = scenario()
    full = simulate_tick(*args, **kw)
    assert full["n_requotes"] > 0
    assert full["quote_schedule"]["counters"]["published:BOOK_VIEW"] > 0
    cp = simulate_tick(*args, **kw, checkpoint_at_ts_ms=cut)["_replay_checkpoint"]
    restored = pickle.loads(pickle.dumps(cp))
    before = pickle.dumps(restored["runtime"].quote_schedule)
    assert_same(simulate_tick(*args, **kw, resume_checkpoint=restored), full)
    assert pickle.dumps(restored["runtime"].quote_schedule) == before


def test_checkpoint_cannot_change_mode():
    args, kw = scenario()
    cp = simulate_tick(*args, **kw, checkpoint_at_ts_ms=10005)["_replay_checkpoint"]
    args[3]["quote_schedule_mode"] = "existing"
    with pytest.raises(ValueError, match="scheduling mode differs"):
        simulate_tick(*args, **kw, resume_checkpoint=cp)


@pytest.mark.parametrize("cut", [10001,10005,10040,11001])
def test_existing_source_clock_consumer_and_restore(cut):
    args, kw = scenario()
    args[3].pop("_exec_message_delivery")
    args[3].pop("exec_book_visibility_mode")
    full = simulate_tick(*args, **kw)
    assert full["n_requotes"] > 0
    assert full["quote_schedule"]["counters"]["published:BOOK_VIEW"] > 0
    cp = simulate_tick(*args, **kw, checkpoint_at_ts_ms=cut)["_replay_checkpoint"]
    assert_same(simulate_tick(*args, **kw, resume_checkpoint=cp), full)


def test_event_mode_retains_positive_rq_and_sampled_compute_phases():
    args, kw = scenario()
    args[3].update(requote_interval=60., rq_min=60., rq_max=60., target_observe_only=True,
                   _requote_tail_work_samples_ms=[5.])
    result = simulate_tick(*args, **kw)
    assert args[3]["requote_interval"] == 60.
    assert result["n_requotes"] > 2
    rows = result["target_observation"]["samples"]
    assert rows
    entries = sorted({row["entry_ts"] for row in rows})
    assert all(b-a >= 45 for a, b in zip(entries, entries[1:], strict=False))
    for row in rows:
        assert row["capture_ts"] - row["entry_ts"] == 10
        assert row["request_ready_ts"] - row["entry_ts"] == 40
    assert "published:TRADE_QUOTE_VIEW" not in result["quote_schedule"]["counters"]


@pytest.mark.parametrize("cut", [10001, 10005, 10040])
def test_event_mode_single_owner_consume(cut):
    args, kw = scenario()
    full = simulate_tick(*args, **kw)
    cp = simulate_tick(*args, **kw, checkpoint_at_ts_ms=cut)["_replay_checkpoint"]
    actual = simulate_tick(*args, **kw, resume_checkpoint=cp, consume_resume_checkpoint=True)
    assert cp["consumed"]
    assert_same(actual, full)


def test_live_age_wakeup_needs_price_and_other_eligibility():
    import threading
    from strategy.quote_schedule import QuoteSchedule, QuoteScheduleWakeup
    from strategy.order_manager import Side, OrderState
    from tests.test_live_replace_throttle import _engine, _order
    engine = _engine()
    engine.cfg.strategy.replace_min_price_change_ticks = 15
    engine.cfg.strategy.replace_min_interval_ms = 1000
    engine._quote_schedule_wakeup = wake = QuoteScheduleWakeup(QuoteSchedule(), threading.Event())
    order = _order(Side.BUY, 100., 100)
    kwargs = dict(side=Side.BUY, now_ts=order.create_time+.1, q=0., target_price=90.,
                  order=order, needs_update=True, force_update=False)
    assert not engine._apply_replace_throttle(**kwargs)
    assert not wake.state.deadlines
    assert not engine._apply_replace_throttle(**kwargs, age_wake_eligible=True)
    assert "replace_age:BUY" in wake.state.deadlines
    assert not engine._apply_replace_throttle(**{**kwargs, "target_price":100.}, age_wake_eligible=True)
    assert not wake.state.deadlines
    order.state = OrderState.PENDING_NEW
    engine._apply_replace_throttle(**kwargs, age_wake_eligible=True)
    assert not wake.state.deadlines


def test_live_terminal_survives_prepare_wait_and_two_side_route():
    import threading
    from strategy.quote_schedule import QuoteSchedule, QuoteScheduleWakeup
    wake = QuoteScheduleWakeup(QuoteSchedule(), threading.Event())
    wake.terminal("BUY", "one")
    wake.terminal("SELL", "two")
    assert not wake.prepare_wait(0)
    work = wake.claim(0)
    wake.route(("BUY", "SELL"))
    wake.terminal("SELL", "new")
    wake.finish(work.sequence, busy_until_ns=0)
    assert wake.state.terminals == {"SELL":"new"}
    assert not wake.prepare_wait(0)
