import logging
import threading
import time
from types import SimpleNamespace

import pytest

from live.config import Config
from strategy.maker_engine import MakerEngine
from strategy.order_manager import (
    Order,
    OrderManager,
    OrderReconciliationRequired,
    OrderState,
    Side,
)


def _engine() -> MakerEngine:
    cfg = Config()
    cfg.tick_size = 0.1
    cfg.strategy.replace_min_price_change_ticks = 2.0
    cfg.strategy.replace_min_price_change_ticks_reducing = 1.0
    cfg.strategy.replace_min_interval_ms = 500.0
    cfg.strategy.replace_min_interval_ms_reducing = 250.0
    cfg.strategy.replace_pending_coalesce = True
    cfg.strategy.replace_cancel_first_exposure_increasing = False
    cfg.strategy.replace_terminal_continuation = False
    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._replace_throttle_counts = {"BUY": 0, "SELL": 0}
    engine._last_replace_throttle_log = {"BUY": 0.0, "SELL": 0.0}
    engine._replace_pending_coalesce_counts = {"BUY": 0, "SELL": 0}
    engine._last_replace_pending_coalesce_log = {"BUY": 0.0, "SELL": 0.0}
    engine._replace_cancel_first_counts = {"BUY": 0, "SELL": 0}
    engine._last_replace_cancel_first_log = {"BUY": 0.0, "SELL": 0.0}
    engine._replace_terminal_continuation_lock = threading.Lock()
    engine._replace_terminal_continuation_generation = {"BUY": 0, "SELL": 0}
    engine._replace_terminal_continuation_intents = {}
    engine._replace_terminal_continuation_in_flight = {}
    engine._replace_terminal_continuation_event_sequence = 0
    return engine


def _order(side: Side, price: float, age_ms: float, state: OrderState = OrderState.OPEN) -> Order:
    return Order(
        client_order_id=f"test_{side.value}",
        symbol="BTCUSDC",
        side=side,
        price=price,
        quantity=0.001,
        state=state,
        create_time=time.time() - age_ms / 1000.0,
    )


def test_replace_throttle_keeps_small_exposure_increasing_price_move() -> None:
    engine = _engine()
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)

    # Long inventory means BUY increases exposure. A 1-tick move is below the
    # 2-tick add-side threshold, so keep the existing order and avoid REST churn.
    assert not engine._apply_replace_throttle(
        side=Side.BUY,
        now_ts=time.time(),
        q=0.005,
        target_price=100.1,
        order=order,
        needs_update=True,
        force_update=False,
    )
    assert engine._replace_throttle_counts["BUY"] == 1


def test_replace_throttle_allows_reducing_side_after_shorter_threshold() -> None:
    engine = _engine()
    order = _order(Side.SELL, price=100.0, age_ms=2000.0)

    # Long inventory means SELL reduces exposure. The reducing threshold is
    # only 1 tick, so a 1-tick move is allowed to replace.
    assert engine._apply_replace_throttle(
        side=Side.SELL,
        now_ts=time.time(),
        q=0.005,
        target_price=100.1,
        order=order,
        needs_update=True,
        force_update=False,
    )


def test_replace_throttle_keeps_too_young_order_but_not_forced_update() -> None:
    engine = _engine()
    order = _order(Side.BUY, price=100.0, age_ms=100.0)

    assert not engine._apply_replace_throttle(
        side=Side.BUY,
        now_ts=time.time(),
        q=0.005,
        target_price=101.0,
        order=order,
        needs_update=True,
        force_update=False,
    )
    assert engine._apply_replace_throttle(
        side=Side.BUY,
        now_ts=time.time(),
        q=0.005,
        target_price=101.0,
        order=order,
        needs_update=True,
        force_update=True,
    )


def test_pending_replace_coalesce_keeps_order_while_cancel_is_pending() -> None:
    engine = _engine()
    order = _order(Side.SELL, price=100.0, age_ms=2000.0, state=OrderState.PENDING_CANCEL)

    assert engine._order_lifecycle_pending(order)
    assert engine._apply_pending_replace_coalesce(
        side=Side.SELL,
        now_ts=time.time(),
        q=-0.005,
        target_price=101.0,
        order=order,
        needs_update=True,
        can_post=True,
    )
    assert engine._replace_pending_coalesce_counts["SELL"] == 1


def test_pending_replace_coalesce_does_not_block_pause_cancel_path() -> None:
    engine = _engine()
    order = _order(Side.BUY, price=100.0, age_ms=2000.0, state=OrderState.PENDING_NEW)

    assert not engine._apply_pending_replace_coalesce(
        side=Side.BUY,
        now_ts=time.time(),
        q=0.005,
        target_price=101.0,
        order=order,
        needs_update=True,
        can_post=False,
    )


def test_stale_pending_cancel_open_order_absence_keeps_ownership() -> None:
    orders = OrderManager()
    cid = orders.create_order("BTCUSDC", Side.SELL, price=100.0, quantity=0.001)
    orders.confirm_new(cid, 123)
    orders.mark_pending_cancel(cid)
    orders._orders[cid].update_time = time.time() - 31.0

    assert [o.client_order_id for o in orders.get_stale_pending_cancel_orders(30.0)] == [cid]
    assert not orders.reconcile_pending_cancel(cid, exchange_open=False)
    assert orders.active_count() == 1
    assert orders.get_order(cid).state == OrderState.PENDING_CANCEL


def test_stale_pending_cancel_rejects_exchange_order_id_change() -> None:
    orders = OrderManager()
    cid = orders.create_order("BTCUSDC", Side.SELL, price=100.0, quantity=0.001)
    orders.confirm_new(cid, 123)
    orders.mark_pending_cancel(cid)
    orders._orders[cid].update_time = time.time() - 31.0

    with pytest.raises(OrderReconciliationRequired):
        orders.reconcile_pending_cancel(cid, exchange_open=True, exchange_oid=456)
    order = orders.get_order(cid)
    assert order.state == OrderState.PENDING_CANCEL
    assert order.order_id == 123
    assert orders.active_count() == 1
    assert orders.reconciliation_required is True


def test_cancel_first_only_applies_to_exposure_increasing_replaces() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_cancel_first_exposure_increasing = True
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)

    assert engine._should_cancel_first_replace(
        side=Side.BUY,
        q=0.005,
        order=order,
        needs_update=True,
        force_update=False,
        can_post=True,
    )
    assert not engine._should_cancel_first_replace(
        side=Side.SELL,
        q=0.005,
        order=_order(Side.SELL, price=100.0, age_ms=2000.0),
        needs_update=True,
        force_update=False,
        can_post=True,
    )
    assert not engine._should_cancel_first_replace(
        side=Side.BUY,
        q=0.005,
        order=order,
        needs_update=True,
        force_update=True,
        can_post=True,
    )


def test_replace_terminal_continuation_is_disabled_by_default() -> None:
    engine = _engine()

    assert engine.cfg.strategy.replace_terminal_continuation is False
    assert (
        engine._arm_replace_terminal_continuation(
            side=Side.BUY,
            cid="disabled",
        )
        == 0
    )
    assert engine._replace_terminal_continuation_intents == {}


def test_policy_block_cancel_does_not_arm_terminal_continuation() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)

    assert (
        engine._arm_replace_terminal_continuation(
            side=Side.BUY,
            cid=order.client_order_id,
            can_post=False,
        )
        == 0
    )
    assert not engine._publish_replace_terminal_continuation(order)
    assert engine._take_ready_replace_terminal_continuations() == {}


def test_continuation_events_commit_atomically_before_lock_free_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    observed = []

    def observe(payload) -> None:
        assert engine._replace_terminal_continuation_lock.acquire(blocking=False)
        engine._replace_terminal_continuation_lock.release()
        observed.append(
            (
                dict(payload),
                engine.replace_terminal_continuation_telemetry_snapshot(),
            )
        )

    monkeypatch.setattr(
        engine,
        "_log_replace_terminal_continuation_event",
        observe,
    )
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    generation = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )
    assert engine._publish_replace_terminal_continuation(order)
    assert engine._clear_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
        generation=generation,
        reason="test_clear",
    )

    assert [payload["event"] for payload, _ in observed] == [
        "arm",
        "publish",
        "drop",
    ]
    assert [payload["sequence"] for payload, _ in observed] == [1, 2, 3]
    assert observed[0][1]["arm_count"] == 1
    assert observed[0][1]["pending_count"] == 1
    assert observed[1][1]["publish_count"] == 1
    assert observed[1][1]["pending_count"] == 1
    assert observed[2][1]["drop_count"] == 1
    assert observed[2][1]["pending_count"] == 0


def test_replace_terminal_continuation_is_cid_bound_and_one_shot() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    stale = _order(Side.BUY, price=100.0, age_ms=2000.0)
    stale.client_order_id = "stale_buy"
    current = _order(Side.BUY, price=100.0, age_ms=2000.0)
    current.client_order_id = "current_buy"

    generation_1 = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=stale.client_order_id,
    )
    generation_2 = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=current.client_order_id,
    )

    assert generation_2 == generation_1 + 1
    assert not engine._publish_replace_terminal_continuation(stale)
    assert engine._publish_replace_terminal_continuation(current)
    assert not engine._publish_replace_terminal_continuation(current)
    assert set(engine._take_ready_replace_terminal_continuations()) == {
        Side.BUY
    }
    assert engine._take_ready_replace_terminal_continuations() == {}


def test_two_ready_sides_are_consumed_in_one_main_loop_batch() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    buy = _order(Side.BUY, price=100.0, age_ms=2000.0)
    sell = _order(Side.SELL, price=101.0, age_ms=2000.0)

    engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=buy.client_order_id,
    )
    engine._arm_replace_terminal_continuation(
        side=Side.SELL,
        cid=sell.client_order_id,
    )
    assert engine._publish_replace_terminal_continuation(buy)
    assert engine._publish_replace_terminal_continuation(sell)

    ready = engine._take_ready_replace_terminal_continuations()
    assert set(ready) == {
        Side.BUY,
        Side.SELL,
    }
    taken = engine.replace_terminal_continuation_telemetry_snapshot()
    assert taken["pending_count"] == 0
    assert taken["in_flight_count"] == 2
    assert taken["arm_count"] == (
        taken["decision_count"]
        + taken["drop_count"]
        + taken["pending_count"]
        + taken["in_flight_count"]
    )
    decision_start_ts_ns = max(
        intent.terminal_visible_ts_ns for intent in ready.values()
    ) + 1_000
    expected_latency_sum_ns = sum(
        decision_start_ts_ns - intent.terminal_visible_ts_ns
        for intent in ready.values()
    )
    expected_latency_max_ns = max(
        decision_start_ts_ns - intent.terminal_visible_ts_ns
        for intent in ready.values()
    )
    engine._record_replace_terminal_continuation_decisions(
        ready,
        decision_start_ts_ns=decision_start_ts_ns,
    )
    telemetry = engine.replace_terminal_continuation_telemetry_snapshot()

    assert telemetry == {
        "arm_count": 2,
        "publish_count": 2,
        "decision_count": 2,
        "drop_count": 0,
        "buy_decision_count": 1,
        "sell_decision_count": 1,
        "decision_latency_sum_ns": expected_latency_sum_ns,
        "decision_latency_max_ns": expected_latency_max_ns,
        "pending_count": 0,
        "in_flight_count": 0,
    }
    assert engine._take_ready_replace_terminal_continuations() == {}


def test_cancel_reject_clears_only_matching_replace_intent() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    engine._record_exact_order_event = lambda *args, **kwargs: None
    order = _order(Side.SELL, price=100.0, age_ms=2000.0)
    order.client_order_id = "sell_cancel"
    engine._arm_replace_terminal_continuation(
        side=Side.SELL,
        cid=order.client_order_id,
    )

    engine._on_order_lifecycle_event(
        order,
        "cancel_rejected",
        {"_local_receive_ts_ns": time.time_ns()},
    )

    assert engine._replace_terminal_continuation_intents == {}


def test_cancel_callback_keeps_ownership_and_context_until_terminal() -> None:
    engine = _engine()
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    order.client_order_id = "buy_cancel"
    engine._bid_cid = order.client_order_id
    engine._order_context_lock = threading.RLock()
    engine._order_policy_context = {order.client_order_id: {"target_price": 100.0}}
    engine._log_order_outcome = lambda *args, **kwargs: None

    engine._on_cancel(order)

    assert engine._bid_cid == order.client_order_id
    assert order.client_order_id in engine._order_policy_context


def test_terminal_cleanup_publishes_ready_only_after_policy_cleanup() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    order.client_order_id = "buy_terminal"
    engine._bid_cid = order.client_order_id
    engine._order_ref_lock = threading.RLock()
    engine._order_context_lock = threading.RLock()
    engine._order_policy_context = {order.client_order_id: {"target_price": 100.0}}
    engine._exact_opportunity_tape_runtime = None
    observed_cleanup = []

    def observe_policy_cleanup(*args, **kwargs) -> None:
        observed_cleanup.append(
            (
                engine._bid_cid,
                order.client_order_id in engine._order_policy_context,
            )
        )

    engine._on_dynamic_fill_hazard_order_terminal = observe_policy_cleanup
    engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )

    engine._on_order_terminal(order, "cancel_ack")
    engine._on_order_terminal(order, "cancel_ack")

    assert observed_cleanup == [(None, False), (None, False)]
    assert set(engine._take_ready_replace_terminal_continuations()) == {
        Side.BUY
    }
    assert engine._take_ready_replace_terminal_continuations() == {}


def test_tick_routes_ready_side_without_advancing_normal_cadence() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    engine.signal = SimpleNamespace(mid_price=0.0, is_warmed_up=True)
    engine._evaluate_dynamic_fill_hazard_shadow = lambda now_ns: None
    engine._enforce_stale_quote_stop = lambda: False
    engine._cooldown_until = 0.0
    engine._last_cooldown_cancel_time = 0.0
    engine._loss_cooldown_expiry_count = 0
    engine.inventory = SimpleNamespace(reset_consecutive_losses=lambda: None)
    continuation = {Side.SELL: SimpleNamespace(generation=1)}
    engine._take_ready_replace_terminal_continuations = lambda: continuation
    calls = []
    engine._requote = lambda **kwargs: calls.append(kwargs)

    engine.tick()

    assert calls == [
        {
            "route_sides": frozenset({Side.SELL}),
            "advance_requote_clock": False,
            "replace_terminal_continuations": continuation,
        }
    ]


@pytest.mark.parametrize("blocker", ["stale", "cooldown"])
def test_tick_consumes_ready_intent_when_safety_blocked(
    blocker: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    caplog.set_level(logging.INFO, logger="maker_engine")
    engine.signal = SimpleNamespace(mid_price=0.0, is_warmed_up=True)
    engine._evaluate_dynamic_fill_hazard_shadow = lambda now_ns: None
    engine._enforce_stale_quote_stop = lambda: blocker == "stale"
    engine._cooldown_until = time.time() + 60.0 if blocker == "cooldown" else 0.0
    engine._last_cooldown_cancel_time = time.time()
    engine._loss_cooldown_expiry_count = 0
    engine.orders = SimpleNamespace(has_active_orders=lambda: False)
    engine.inventory = SimpleNamespace(reset_consecutive_losses=lambda: None)
    engine._requote = lambda **kwargs: pytest.fail("safety-blocked tick requoted")
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )
    assert engine._publish_replace_terminal_continuation(order)

    engine.tick()

    assert engine._replace_terminal_continuation_intents == {}
    telemetry = engine.replace_terminal_continuation_telemetry_snapshot()
    assert telemetry["arm_count"] == 1
    assert telemetry["publish_count"] == 1
    assert telemetry["decision_count"] == 0
    assert telemetry["drop_count"] == 1
    drop_reason = (
        "stale_quote_stop" if blocker == "stale" else "loss_cooldown"
    )
    assert any(
        "REPLACE_TERMINAL_CONTINUATION event=drop " in record.getMessage()
        and "side=BUY generation=1 " in record.getMessage()
        and f"reason={drop_reason}" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.parametrize(
    ("failure_stage", "expected_decisions", "expected_drops"),
    (
        ("blocker", 0, 1),
        ("before_decision", 0, 1),
        ("after_decision", 1, 0),
    ),
)
def test_tick_exception_finalizes_ready_batch_exactly_once(
    failure_stage: str,
    expected_decisions: int,
    expected_drops: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    caplog.set_level(logging.INFO, logger="maker_engine")
    engine.signal = SimpleNamespace(mid_price=0.0, is_warmed_up=True)
    engine._evaluate_dynamic_fill_hazard_shadow = lambda now_ns: None
    engine._order_manager_callback_dispatch_active = lambda: False
    engine._enforce_stale_quote_stop = (
        lambda: (_ for _ in ()).throw(RuntimeError("blocker failed"))
        if failure_stage == "blocker"
        else False
    )
    engine._cooldown_until = 0.0
    engine._last_cooldown_cancel_time = 0.0
    engine._loss_cooldown_expiry_count = 0
    engine.inventory = SimpleNamespace(reset_consecutive_losses=lambda: None)

    def fail_requote(**kwargs) -> None:
        if failure_stage == "after_decision":
            continuations = kwargs["replace_terminal_continuations"]
            decision_start_ts_ns = max(
                intent.terminal_visible_ts_ns
                for intent in continuations.values()
            ) + 500
            engine._record_replace_terminal_continuation_decisions(
                continuations,
                decision_start_ts_ns=decision_start_ts_ns,
            )
        raise RuntimeError("requote failed")

    engine._requote = fail_requote
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )
    assert engine._publish_replace_terminal_continuation(order)

    with pytest.raises(RuntimeError, match="failed"):
        engine.tick()

    telemetry = engine.replace_terminal_continuation_telemetry_snapshot()
    assert telemetry["decision_count"] == expected_decisions
    assert telemetry["drop_count"] == expected_drops
    assert telemetry["pending_count"] == 0
    assert telemetry["in_flight_count"] == 0
    assert telemetry["arm_count"] == (
        telemetry["decision_count"]
        + telemetry["drop_count"]
        + telemetry["pending_count"]
        + telemetry["in_flight_count"]
    )
    finalized = [
        record.getMessage()
        for record in caplog.records
        if "REPLACE_TERMINAL_CONTINUATION event=decision "
        in record.getMessage()
        or "REPLACE_TERMINAL_CONTINUATION event=drop "
        in record.getMessage()
    ]
    assert len(finalized) == 1


@pytest.mark.parametrize("terminal_reason", ["full_fill", "rejected", "expired"])
def test_non_cancel_terminal_clears_without_continuation(
    terminal_reason: str,
) -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    engine._bid_cid = order.client_order_id
    engine._order_ref_lock = threading.RLock()
    engine._order_context_lock = threading.RLock()
    engine._order_policy_context = {order.client_order_id: {}}
    engine._exact_opportunity_tape_runtime = None
    engine._on_dynamic_fill_hazard_order_terminal = lambda *args, **kwargs: None
    engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )

    engine._on_order_terminal(order, terminal_reason)

    assert engine._replace_terminal_continuation_intents == {}
    assert engine._take_ready_replace_terminal_continuations() == {}


def test_safety_cancel_supersedes_already_pending_replace_intent() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    engine.orders = OrderManager()
    cid = engine.orders.create_order(
        "BTCUSDC",
        Side.BUY,
        price=100.0,
        quantity=0.001,
    )
    engine.orders.confirm_new(cid, 123)
    engine.orders.mark_pending_cancel(cid)
    engine._bid_cid = cid
    engine._ask_cid = None
    engine._order_ref_lock = threading.RLock()
    engine._arm_replace_terminal_continuation(side=Side.BUY, cid=cid)

    assert not engine._cancel_order(cid)
    assert engine._replace_terminal_continuation_intents == {}


def test_cancel_all_supersedes_pending_replace_intents_on_both_sides() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    engine.orders = OrderManager()
    buy_cid = engine.orders.create_order(
        "BTCUSDC",
        Side.BUY,
        price=100.0,
        quantity=0.001,
    )
    sell_cid = engine.orders.create_order(
        "BTCUSDC",
        Side.SELL,
        price=101.0,
        quantity=0.001,
    )
    engine.orders.confirm_new(buy_cid, 123)
    engine.orders.confirm_new(sell_cid, 124)
    engine.orders.mark_pending_cancel(buy_cid)
    engine.orders.mark_pending_cancel(sell_cid)
    engine._bid_cid = buy_cid
    engine._ask_cid = sell_cid
    engine._order_ref_lock = threading.RLock()
    engine._arm_replace_terminal_continuation(side=Side.BUY, cid=buy_cid)
    engine._arm_replace_terminal_continuation(side=Side.SELL, cid=sell_cid)

    assert engine._cancel_all_orders()
    assert engine._replace_terminal_continuation_intents == {}


def test_callback_dispatch_keeps_ready_for_next_main_loop_wake() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    dispatch = {"active": True}
    engine.orders = SimpleNamespace(
        callback_dispatch_active=lambda: dispatch["active"],
    )
    engine.signal = SimpleNamespace(mid_price=0.0, is_warmed_up=True)
    engine._evaluate_dynamic_fill_hazard_shadow = lambda now_ns: None
    engine._enforce_stale_quote_stop = lambda: False
    engine._cooldown_until = 0.0
    engine._last_cooldown_cancel_time = 0.0
    engine._loss_cooldown_expiry_count = 0
    engine._last_requote_time = time.time()
    engine._effective_rq_interval = lambda: 10.0
    engine.inventory = SimpleNamespace(reset_consecutive_losses=lambda: None)
    calls = []
    engine._requote = lambda **kwargs: calls.append(kwargs)
    order = _order(Side.SELL, price=101.0, age_ms=2000.0)
    engine._arm_replace_terminal_continuation(
        side=Side.SELL,
        cid=order.client_order_id,
    )
    assert engine._publish_replace_terminal_continuation(order)

    engine.tick()
    assert calls == []
    assert engine._replace_terminal_continuation_intents["SELL"].ready

    dispatch["active"] = False
    engine.tick()
    assert len(calls) == 1
    assert calls[0]["route_sides"] == frozenset({Side.SELL})
    assert calls[0]["advance_requote_clock"] is False
    continuation = calls[0]["replace_terminal_continuations"]
    assert set(continuation) == {Side.SELL}
    assert continuation[Side.SELL].generation == 1
    assert engine._replace_terminal_continuation_intents == {}


def test_callback_dispatch_check_failure_keeps_ready_fail_closed() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True

    def broken_dispatch_check() -> bool:
        raise RuntimeError("callback queue state unavailable")

    engine.orders = SimpleNamespace(
        callback_dispatch_active=broken_dispatch_check,
    )
    engine.signal = SimpleNamespace(mid_price=0.0, is_warmed_up=True)
    engine._evaluate_dynamic_fill_hazard_shadow = lambda now_ns: None
    engine._enforce_stale_quote_stop = lambda: False
    engine._cooldown_until = 0.0
    engine._last_cooldown_cancel_time = 0.0
    engine._loss_cooldown_expiry_count = 0
    engine._last_requote_time = time.time()
    engine._effective_rq_interval = lambda: 10.0
    engine.inventory = SimpleNamespace(reset_consecutive_losses=lambda: None)
    engine._requote = lambda **kwargs: pytest.fail(
        "failed quiescence check must not requote"
    )
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )
    assert engine._publish_replace_terminal_continuation(order)

    engine.tick()

    assert engine._replace_terminal_continuation_intents["BUY"].ready


def test_side_only_requote_preserves_unrouted_quote_context_and_records_latency(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    engine.cfg.risk.max_exec_book_visible_age_s = 0.0
    caplog.set_level(logging.INFO, logger="maker_engine")
    clock_ns = time.time_ns()
    monkeypatch.setattr(
        "strategy.maker_engine.time.time_ns",
        lambda: clock_ns,
    )
    engine._last_quote_context = {
        "BUY": {"generation": "old_buy"},
        "SELL": {"generation": "old_sell", "nested": {"value": 7}},
    }
    engine._last_requote_time = 123.0
    engine._requote_count = 0
    engine._reset_perf_rest_counters = lambda: None
    engine._check_sync_adjust_degrade = lambda now: None
    snapshot = SimpleNamespace(
        capture_ts_ns=clock_ns + 1_700,
        depth_visible_age_s=0.0,
        mid=100.0,
        bar_pricing_mid=100.0,
    )
    engine.signal = SimpleNamespace(
        compute_signal=lambda: SimpleNamespace(),
        quote_decision_snapshot=lambda: snapshot,
    )
    engine._post_only_guard_for_snapshot = lambda _snapshot: SimpleNamespace()
    engine._quote_snapshot_contract_error = lambda *args, **kwargs: ""
    engine.inventory = SimpleNamespace(
        net_position=0.0,
        snapshot=SimpleNamespace(state="FLAT"),
    )
    engine._log_inventory_campaign_shadow = lambda *args, **kwargs: None
    engine._risk_check = lambda *args, **kwargs: True

    def compute_quotes(*args, **kwargs):
        engine._last_quote_context = {
            "BUY": {"generation": "new_buy"},
            "SELL": {"generation": "new_sell", "nested": {"value": 99}},
        }
        return 99.0, 101.0, 2.0

    engine._compute_quotes = compute_quotes
    routed_context = {}

    def update_orders(*args, **kwargs):
        routed_context.update(engine._last_quote_context)
        assert kwargs["route_sides"] == frozenset({Side.BUY})
        return False, False

    engine._update_orders = update_orders
    engine._log_quote_snapshot_integrity = lambda *args, **kwargs: None
    engine._log_live_perf_telemetry = lambda *args, **kwargs: None
    order = _order(Side.BUY, price=100.0, age_ms=2000.0)
    generation = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=order.client_order_id,
    )
    armed = engine.replace_terminal_continuation_telemetry_snapshot()
    assert armed["arm_count"] == 1
    assert armed["publish_count"] == 0
    assert armed["pending_count"] == 1
    assert armed["in_flight_count"] == 0
    order.lifecycle = SimpleNamespace(terminal_ts_ns=clock_ns + 700)
    assert engine._publish_replace_terminal_continuation(order)
    ready = engine._take_ready_replace_terminal_continuations()

    engine._requote(
        route_sides=frozenset({Side.BUY}),
        advance_requote_clock=False,
        replace_terminal_continuations=ready,
    )

    assert engine._last_requote_time == 123.0
    assert routed_context["BUY"] == {"generation": "new_buy"}
    assert routed_context["SELL"] == {
        "generation": "old_sell",
        "nested": {"value": 7},
    }
    assert engine._last_quote_context["SELL"] == routed_context["SELL"]
    telemetry = engine.replace_terminal_continuation_telemetry_snapshot()
    assert telemetry["arm_count"] == 1
    assert telemetry["publish_count"] == 1
    assert telemetry["decision_count"] == 1
    assert telemetry["drop_count"] == 0
    assert telemetry["buy_decision_count"] == 1
    assert telemetry["sell_decision_count"] == 0
    assert telemetry["decision_latency_sum_ns"] == 1_000
    assert telemetry["decision_latency_max_ns"] == 1_000
    event_messages = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("REPLACE_TERMINAL_CONTINUATION ")
    ]
    assert [message.split()[1] for message in event_messages] == [
        "event=arm",
        "event=publish",
        "event=decision",
    ]
    assert [message.split()[2] for message in event_messages] == [
        "sequence=1",
        "sequence=2",
        "sequence=3",
    ]
    assert all(
        f"side=BUY generation={generation}" in message
        for message in event_messages
    )
    assert (
        f"terminal_visible_ts_ns={clock_ns + 700} "
        f"decision_start_ts_ns={clock_ns + 1_700} "
        "decision_latency_ns=1000"
    ) in event_messages[-1]


def test_authoritative_terminal_before_callback_clears_unready_arm() -> None:
    engine = _engine()
    engine.cfg.strategy.replace_terminal_continuation = True
    engine.orders = OrderManager()
    cid = engine.orders.create_order(
        "BTCUSDC",
        Side.BUY,
        price=100.0,
        quantity=0.001,
    )
    engine.orders.confirm_new(cid, 123)
    engine.orders.on_order_update(
        {
            "c": cid,
            "i": 123,
            "s": "BTCUSDC",
            "S": "BUY",
            "X": "CANCELED",
            "o": "LIMIT",
            "p": "100.0",
            "q": "0.001",
            "z": "0",
            "l": "0",
            "L": "0",
            "T": 1,
        }
    )
    engine._bid_cid = cid
    engine._ask_cid = None
    engine._order_ref_lock = threading.RLock()
    generation = engine._arm_replace_terminal_continuation(
        side=Side.BUY,
        cid=cid,
    )

    assert engine._cancel_order(
        cid,
        replace_continuation_generation=generation,
    )
    assert engine._replace_terminal_continuation_intents == {}


def test_read_only_side_policy_does_not_advance_side_runtime_state() -> None:
    engine = _engine()
    engine._toxicity_probs = lambda pred: (0.5, 0.5)
    engine._current_l2_policy_metrics = lambda *args, **kwargs: {
        "depth_age_s": 0.0,
        "microprice_shift_bps": 0.0,
        "l2_quote_flip_rate": 0.0,
        "l2_book_refresh_ratio": 0.0,
        "l2_book_cancel_ratio": 0.0,
        "l2_near_depth_total": 100.0,
    }
    engine._mo_ema_bid = 0.0
    engine._mo_ema_ask = 0.0
    engine._mo_ref = 50.0
    engine._fill_cooldown_until = {"BUY": 0.0, "SELL": 0.0}
    engine._last_quote_context = {"BUY": {}, "SELL": {}}
    engine._policy_reason_text = lambda reason_mask: "none"
    state_calls = []
    engine._expire_fill_cooldown_state = (
        lambda side, now: state_calls.append(("expire", side))
    )
    engine._apply_buy_fill_selection_live_arm = (
        lambda **kwargs: state_calls.append(("buy_arm", kwargs["side"].value))
    )

    engine._build_side_policy(
        Side.SELL,
        mid=100.0,
        q=0.005,
        pred=SimpleNamespace(),
        mutate_state=False,
    )

    assert state_calls == []


def test_side_only_update_orders_preserves_opposite_runtime_state(
    monkeypatch,
) -> None:
    engine = _engine()
    monkeypatch.setattr(
        "strategy.maker_engine._get_live_routing_cpp",
        lambda: None,
    )
    engine.orders = OrderManager()
    engine._bid_cid = None
    engine._ask_cid = None
    engine._order_submit_fail_closed = False
    engine._min_qty = engine.cfg.lot_size
    engine._last_quote_diagnostics = {"max_spread": 0.0}
    engine._last_quote_context = {
        "BUY": {},
        "SELL": {"marker": "preserve", "nested": {"value": 7}},
    }
    sell_context = engine._last_quote_context["SELL"]
    sell_cooldown_until = time.time() + 120.0
    engine._fill_cooldown_until = {
        "BUY": 0.0,
        "SELL": sell_cooldown_until,
    }
    engine._mo_ema_bid = 0.0
    engine._mo_ema_ask = 0.0
    engine._mo_ref = 50.0
    engine._last_bid_action = "before_buy"
    engine._last_ask_action = "before_sell"
    engine._last_cpp_routing_used = 0
    engine._quote_log_path = "unused.csv"
    state_calls = []
    decision_rows = []
    engine._toxicity_probs = lambda pred: (0.5, 0.5)
    engine._current_l2_policy_metrics = lambda *args, **kwargs: {
        "depth_age_s": 0.0,
        "microprice_shift_bps": 0.0,
        "l2_quote_flip_rate": 0.0,
        "l2_book_refresh_ratio": 0.0,
        "l2_book_cancel_ratio": 0.0,
        "l2_near_depth_total": 100.0,
    }
    engine._expire_fill_cooldown_state = (
        lambda side, now: state_calls.append(("expire", side))
    )
    engine._apply_buy_fill_selection_live_arm = (
        lambda **kwargs: state_calls.append(("buy_arm", kwargs["side"].value))
    )
    engine._apply_sync_adjust_degrade_policy = lambda *args, **kwargs: None
    engine._apply_post_fill_quote_response = (
        lambda **kwargs: (kwargs["bid_price"], kwargs["ask_price"])
    )
    engine._apply_post_policy_spread_cap = (
        lambda mid, bid, ask, **kwargs: (bid, ask, False)
    )
    engine._maybe_apply_state_conditioned_quote_policy = (
        lambda **kwargs: (kwargs["baseline_price"], False)
    )
    engine._evaluate_dynamic_fill_hazard_prospective_recovery = (
        lambda **kwargs: None
    )
    engine._dynamic_fill_hazard_buy_blocked = lambda q: False
    engine._apply_final_p3_side_bbo_floor = lambda **kwargs: (
        kwargs["bid_price"],
        kwargs["ask_price"],
        kwargs["best_bid"],
        kwargs["best_ask"],
        False,
        False,
        False,
        False,
    )
    engine._exact_opportunity_tape_enabled = lambda: False
    engine._record_cross_venue_fair_price_shadow = lambda **kwargs: None
    engine._quote_snapshot_contract_error = lambda *args, **kwargs: ""
    engine._quote_routing_contract_error = lambda **kwargs: ""
    engine._append_row = lambda _path, row: decision_rows.append(row)

    guard = SimpleNamespace(best_bid=99.0, best_ask=101.0, source="test")
    engine._update_orders(
        mid=100.0,
        bid_price=99.0,
        ask_price=101.0,
        q=0.0,
        pred=SimpleNamespace(),
        quote_snapshot=SimpleNamespace(),
        post_only_guard=guard,
        route_sides=frozenset({Side.BUY}),
    )

    assert ("expire", "SELL") not in state_calls
    assert ("buy_arm", "SELL") not in state_calls
    assert engine._fill_cooldown_until["SELL"] == sell_cooldown_until
    assert engine._last_quote_context["SELL"] is sell_context
    assert engine._last_quote_context["SELL"] == {
        "marker": "preserve",
        "nested": {"value": 7},
    }
    assert engine._last_ask_action == "before_sell"
    assert [row.side for row in decision_rows] == ["BUY"]
