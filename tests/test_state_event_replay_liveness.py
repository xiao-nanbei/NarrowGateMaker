"""Synthetic execution of the real nested scheduling functions, not a new runner."""
import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from strategy.quote_schedule import QuoteSchedule


def functions(state):
    tree = ast.parse(Path("models/backtest_tick.py").read_text())
    names = {"_replacement_continuation_pending", "_finish_main_loop_tick",
             "_schedule_main_loop_tick", "_begin_main_loop_work", "_next_replay_event"}
    nodes = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    namespace = {"_tick_state": state, "_quote_schedule_next_visible": lambda _: None}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<actual-replay-scheduling>", "exec"), namespace)
    return namespace


def state():
    return SimpleNamespace(
        quote_schedule=QuoteSchedule(), replace_terminal_continuation=True,
        replacement_terminal_due_ts={"BUY": 990, "SELL": -1},
        bid_orders=[{"trace_id": "successor"}], ask_orders=[],
        main_loop_waiting_request=None, main_loop_work_enabled=False,
        main_loop_work_post_sum_ms=0, main_loop_after_tick_ms=0,
        main_loop_tick_complete_ms=1000, main_loop_quote_tail_ms=0,
        async_rest_gateway=True, main_loop_sleep_ms=100,
        main_loop_enabled=True, pending_quote_compute=None, serial_rest_decision=None,
        replay_event_cursor={"after_event": "wake", "index": 1, "started": True},
        main_loop_next_wake_ms=None, trade_ts=[1000, 1100], n_trades=2,
        local_lifecycle_boundary_scheduler=SimpleNamespace(next_timestamp=lambda: None),
        quote_schedule_visible_ms=1000, main_loop_tick_count=0,
    )


def test_retained_terminal_with_order_does_not_starve_market():
    s = state()
    f = functions(s)
    event = f["_next_replay_event"]()
    assert event == (1, 1100, True, False, False)
    assert s.main_loop_tick_count == 0
    assert s.replacement_terminal_due_ts["BUY"] == 990  # no invented cancellation


@pytest.mark.parametrize("busy_ns,expected", [(1050000000, 1050), (1050000001, 1051)])
def test_dirty_waits_for_actual_busy_boundary(busy_ns, expected):
    s = state()
    s.quote_schedule.publish("BOOK_VIEW", 1)
    s.quote_schedule.busy_until_ns = busy_ns
    f = functions(s)
    f["_finish_main_loop_tick"](1000)
    assert s.main_loop_next_wake_ms == expected
    assert s.quote_schedule.claim(1000000000) is None
    assert s.quote_schedule.claim(expected * 1000000) is not None


def test_executable_terminal_still_wakes_immediately():
    s = state()
    s.bid_orders.clear()
    f = functions(s)
    f["_finish_main_loop_tick"](1000)
    assert s.main_loop_next_wake_ms == 1000


def test_disabled_continuation_is_not_runnable():
    s = state()
    s.bid_orders.clear()
    s.replace_terminal_continuation = False
    functions(s)["_finish_main_loop_tick"](1000)
    assert s.main_loop_next_wake_ms == 1100


def test_existing_mode_keeps_original_scheduling():
    s = state()
    s.quote_schedule = None
    functions(s)["_finish_main_loop_tick"](1000)
    assert s.main_loop_next_wake_ms == 1000


@pytest.mark.parametrize("event_mode,expected", [(True, True), (False, False)])
def test_ttl_eligibility_does_not_follow_event_quote_last_rq(event_mode, expected):
    tree = ast.parse(Path("models/backtest_tick.py").read_text())
    func = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                and n.name == "_process_order_transitions")
    condition = next(n.test for n in ast.walk(func) if isinstance(n, ast.If))
    s = SimpleNamespace(main_loop_enabled=True, is_main_loop_wake=True,
                        quote_schedule=QuoteSchedule() if event_mode else None,
                        last_rq_ts=9900, cur_rq_ms=5000,
                        ORDER_OPEN="open", ORDER_PENDING_CANCEL="cancel")
    order = dict(state="open", cancel_ts=0, quote_ts=1000)
    assert eval(compile(ast.Expression(condition), "<actual-ttl-condition>", "eval"),
                {"_tick_state": s, "allow_ttl_initiation": True,
                 "now_ts": 10000, "ttl_ms": 1000, "order": order}) is expected
