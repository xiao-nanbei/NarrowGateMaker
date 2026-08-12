import time

from live.config import Config
from strategy.maker_engine import MakerEngine
from strategy.order_manager import Order, OrderManager, OrderState, Side


def _engine() -> MakerEngine:
    cfg = Config()
    cfg.tick_size = 0.1
    cfg.strategy.replace_min_price_change_ticks = 2.0
    cfg.strategy.replace_min_price_change_ticks_reducing = 1.0
    cfg.strategy.replace_min_interval_ms = 500.0
    cfg.strategy.replace_min_interval_ms_reducing = 250.0
    cfg.strategy.replace_pending_coalesce = True
    cfg.strategy.replace_cancel_first_exposure_increasing = False
    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._replace_throttle_counts = {"BUY": 0, "SELL": 0}
    engine._last_replace_throttle_log = {"BUY": 0.0, "SELL": 0.0}
    engine._replace_pending_coalesce_counts = {"BUY": 0, "SELL": 0}
    engine._last_replace_pending_coalesce_log = {"BUY": 0.0, "SELL": 0.0}
    engine._replace_cancel_first_counts = {"BUY": 0, "SELL": 0}
    engine._last_replace_cancel_first_log = {"BUY": 0.0, "SELL": 0.0}
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


def test_stale_pending_cancel_reconcile_clears_missing_exchange_order() -> None:
    orders = OrderManager()
    cid = orders.create_order("BTCUSDC", Side.SELL, price=100.0, quantity=0.001)
    orders.confirm_new(cid, 123)
    orders.mark_pending_cancel(cid)
    orders._orders[cid].update_time = time.time() - 31.0

    assert [o.client_order_id for o in orders.get_stale_pending_cancel_orders(30.0)] == [cid]
    assert orders.reconcile_pending_cancel(cid, exchange_open=False)
    assert orders.active_count() == 0
    assert orders.get_order(cid).state == OrderState.CANCELED


def test_stale_pending_cancel_reconcile_reopens_exchange_order() -> None:
    orders = OrderManager()
    cid = orders.create_order("BTCUSDC", Side.SELL, price=100.0, quantity=0.001)
    orders.confirm_new(cid, 123)
    orders.mark_pending_cancel(cid)
    orders._orders[cid].update_time = time.time() - 31.0

    assert orders.reconcile_pending_cancel(cid, exchange_open=True, exchange_oid=456)
    order = orders.get_order(cid)
    assert order.state == OrderState.OPEN
    assert order.order_id == 456
    assert orders.active_count() == 1


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
