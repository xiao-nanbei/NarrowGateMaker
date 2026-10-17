import threading
import time

import pytest

from live.config import Config
from live.main import LIVE_MAIN_LOOP_FALLBACK_WAIT_S, _LiveMainLoopWakeup
from strategy.maker_engine import MakerEngine
from strategy.order_manager import Order, Side


def _continuation_engine() -> MakerEngine:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.cfg.strategy.replace_terminal_continuation = True
    engine._replace_terminal_continuation_lock = threading.Lock()
    engine._replace_terminal_continuation_generation = {"BUY": 0, "SELL": 0}
    engine._replace_terminal_continuation_intents = {}
    engine._replace_terminal_continuation_in_flight = {}
    engine._replace_terminal_continuation_event_sequence = 0
    engine._replace_terminal_continuation_wakeup = None
    return engine


def _order(side: Side, cid: str) -> Order:
    return Order(
        client_order_id=cid,
        symbol="BTCUSDC",
        side=side,
        price=100.0,
        quantity=0.001,
        create_time=time.time(),
    )


def test_live_main_loop_wakeup_keeps_existing_100ms_fallback() -> None:
    wakeup = _LiveMainLoopWakeup()

    assert LIVE_MAIN_LOOP_FALLBACK_WAIT_S == 0.1
    assert not wakeup.wait()


def test_terminal_publish_during_iteration_shortcuts_wait_once() -> None:
    wakeup = _LiveMainLoopWakeup(fallback_wait_s=0.001)
    wakeup.notify_replacement_terminal()

    assert wakeup.wait()

    # Wait consumes the one-shot notification. With no new authoritative
    # terminal publish, the periodic fallback remains in force.
    assert not wakeup.wait()


def test_notification_after_prior_wait_is_seen_by_next_wait() -> None:
    wakeup = _LiveMainLoopWakeup(fallback_wait_s=0.001)

    assert not wakeup.wait()
    wakeup.notify_replacement_terminal()
    assert wakeup.wait()


def test_shutdown_uses_the_same_interruptible_wait_without_quote_event() -> None:
    wakeup = _LiveMainLoopWakeup(fallback_wait_s=1.0)
    waiting = threading.Event()
    finished = threading.Event()
    observed = []

    def wait_once() -> None:
        waiting.set()
        observed.append(wakeup.wait())
        finished.set()

    thread = threading.Thread(target=wait_once)
    thread.start()
    assert waiting.wait(timeout=0.1)

    wakeup.notify_shutdown()

    assert finished.wait(timeout=0.1)
    thread.join(timeout=0.1)
    assert not thread.is_alive()
    assert observed == [True]


def test_only_successful_authoritative_terminal_publish_wakes_main_loop() -> None:
    engine = _continuation_engine()
    wakeup = threading.Event()
    engine.set_replace_terminal_continuation_wakeup(wakeup.set)
    order = _order(Side.BUY, "buy_replace")

    # An unarmed or duplicate callback cannot wake the decision loop.
    assert not engine._publish_replace_terminal_continuation(order)
    assert not wakeup.is_set()

    generation = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )
    assert not wakeup.is_set()
    assert engine._publish_replace_terminal_continuation(
        order,
        generation=generation,
    )
    assert wakeup.is_set()

    wakeup.clear()
    assert not engine._publish_replace_terminal_continuation(order)
    assert not wakeup.is_set()


def test_native_authoritative_terminal_publish_wakes_main_loop_exactly_once() -> None:
    narrowgate_cpp = pytest.importorskip("narrowgate_cpp")
    engine = _continuation_engine()
    engine._replace_terminal_continuation_native_module = narrowgate_cpp
    engine._replace_terminal_continuation_native_state = (
        narrowgate_cpp.NativeReplaceContinuationState(True)
    )
    wake_count = 0

    def wake() -> None:
        nonlocal wake_count
        wake_count += 1

    engine.set_replace_terminal_continuation_wakeup(wake)
    order = _order(Side.BUY, "buy_native_replace")
    generation = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )

    assert engine._publish_replace_terminal_continuation(
        order,
        generation=generation,
    )
    assert not engine._publish_replace_terminal_continuation(
        order,
        generation=generation,
    )
    assert wake_count == 1


def test_non_cancel_terminal_clears_intent_without_waking_main_loop() -> None:
    engine = _continuation_engine()
    wakeup = threading.Event()
    engine.set_replace_terminal_continuation_wakeup(wakeup.set)
    order = _order(Side.SELL, "sell_filled_while_cancel_pending")
    engine._bid_cid = None
    engine._ask_cid = order.client_order_id
    engine._order_ref_lock = threading.RLock()
    engine._order_context_lock = threading.RLock()
    engine._order_policy_context = {}
    engine._exact_opportunity_tape_runtime = None
    engine._on_dynamic_fill_hazard_order_terminal = lambda *args, **kwargs: None
    engine._arm_replace_terminal_continuation(
        side=Side.SELL,
        cid=order.client_order_id,
    )

    engine._on_order_terminal(order, "full_fill")

    assert not wakeup.is_set()
    assert engine._replace_terminal_continuation_intents == {}


def test_wakeup_failure_keeps_published_terminal_ready(caplog) -> None:
    engine = _continuation_engine()
    order = _order(Side.BUY, "buy_broken_wakeup")

    def broken_wakeup() -> None:
        raise RuntimeError("event unavailable")

    engine.set_replace_terminal_continuation_wakeup(broken_wakeup)
    generation = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )

    assert engine._publish_replace_terminal_continuation(
        order,
        generation=generation,
    )
    ready = engine._take_ready_replace_terminal_continuations()

    assert set(ready) == {Side.BUY}
    assert "REPLACE_TERMINAL_CONTINUATION_WAKEUP_FAILED" in caplog.text
