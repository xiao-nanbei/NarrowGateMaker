from __future__ import annotations

import threading
import time

import pytest

from strategy.order_manager import (
    OrderManager,
    OrderManagerFatalError,
    OrderReconciliationRequired,
    OrderState,
    Side,
)


def _tracked_order(
    *,
    on_fill=None,
    on_cancel=None,
    on_terminal=None,
    on_lifecycle_event=None,
) -> tuple[OrderManager, str]:
    manager = OrderManager(
        on_fill=on_fill,
        on_cancel=on_cancel,
        on_terminal=on_terminal,
        on_lifecycle_event=on_lifecycle_event,
    )
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 42)
    return manager, cid


def _event(cid: str, status: str, cumulative: object, **overrides) -> dict:
    return {
        "s": "BTCUSDC",
        "c": cid,
        "S": "BUY",
        "o": "LIMIT",
        "X": status,
        "i": 42,
        "p": "100.0",
        "q": "0.001",
        "z": cumulative,
        "l": cumulative,
        "L": "100.0",
        "ap": "100.0",
        "n": "0.0",
        **overrides,
    }


def test_orphan_callbacks_can_reenter_order_manager_without_deadlock() -> None:
    callback_names: list[str] = []
    manager: OrderManager

    def reenter(name: str) -> None:
        manager.get_active_orders()
        callback_names.append(name)

    manager = OrderManager(
        on_fill=lambda _order, _event: reenter("fill"),
        on_cancel=lambda _order: reenter("cancel"),
        on_terminal=lambda _order, _reason: reenter("terminal"),
        on_lifecycle_event=lambda _order, _kind, _event: reenter("lifecycle"),
        allowed_symbols={"BTCUSDC"},
    )
    update = _event("mm_B_orphan", "CANCELED", "0.0004", i=99)
    worker = threading.Thread(target=manager.on_order_update, args=(update,))

    worker.start()
    worker.join(timeout=1.0)

    assert not worker.is_alive()
    assert callback_names == ["lifecycle", "fill", "cancel", "terminal"]
    adopted = manager.get_order("mm_B_orphan")
    assert adopted is not None
    assert adopted.state == OrderState.CANCELED
    assert adopted.filled_qty == pytest.approx(0.0004)


def test_callback_dispatch_active_covers_committed_unclaimed_queue() -> None:
    """A committed callback batch is active before a drainer claims it."""

    manager, cid = _tracked_order(on_fill=lambda _order, _event: None)
    committed_before_claim = threading.Event()
    release_dispatch = threading.Event()
    original_dispatch = manager._dispatch_callbacks

    def delayed_dispatch(batch) -> None:
        committed_before_claim.set()
        if not release_dispatch.wait(timeout=2.0):
            raise TimeoutError("test did not release callback dispatch")
        original_dispatch(batch)

    manager._dispatch_callbacks = delayed_dispatch
    worker = threading.Thread(
        target=manager.on_order_update,
        args=(_event(cid, "PARTIALLY_FILLED", "0.0004"),),
    )
    worker.start()

    assert committed_before_claim.wait(timeout=1.0)
    assert manager._callback_dispatch_drainer_thread_id is None
    assert manager._callback_dispatch_queue
    assert manager._callback_commit_sequence > manager._callback_dispatched_sequence
    assert manager.callback_dispatch_active()

    release_dispatch.set()
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert not manager.callback_dispatch_active()


def test_ws_and_rest_fill_callbacks_follow_ledger_commit_sequence() -> None:
    first_callback_entered = threading.Event()
    release_first_callback = threading.Event()
    delivered_cumulative: list[float] = []
    callback_cursor = 0.0
    thread_errors: dict[str, BaseException] = {}

    def on_fill(_order, event) -> None:
        nonlocal callback_cursor
        cumulative = float(event["z"])
        if cumulative == pytest.approx(0.0004):
            first_callback_entered.set()
            if not release_first_callback.wait(timeout=2.0):
                raise TimeoutError("test did not release first fill callback")
        delta = float(event["_fill_qty"])
        assert cumulative == pytest.approx(callback_cursor + delta)
        callback_cursor = cumulative
        delivered_cumulative.append(cumulative)

    manager, cid = _tracked_order(on_fill=on_fill)

    def run(name: str, target) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors[name] = exc

    ws_thread = threading.Thread(
        target=run,
        args=(
            "ws",
            lambda: manager.on_order_update(
                _event(cid, "PARTIALLY_FILLED", "0.0004")
            ),
        ),
    )
    rest_thread = threading.Thread(
        target=run,
        args=(
            "rest",
            lambda: manager.reconcile_exchange_trade(
                exchange_order_id=42,
                trade_id=9002,
                symbol="BTCUSDC",
                side="BUY",
                quantity=0.0006,
                price=100.0,
                commission=0.0,
                commission_asset="USDC",
                cumulative_fill=0.001,
            ),
        ),
    )

    ws_thread.start()
    assert first_callback_entered.wait(timeout=1.0)
    rest_thread.start()
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        order = manager.get_order(cid)
        if order is not None and order.filled_qty == pytest.approx(0.001):
            break
        time.sleep(0.005)

    committed = manager.get_order(cid)
    assert committed is not None
    assert committed.filled_qty == pytest.approx(0.001)
    assert manager.callback_dispatch_active()
    assert rest_thread.is_alive()
    assert delivered_cumulative == []

    release_first_callback.set()
    ws_thread.join(timeout=2.0)
    rest_thread.join(timeout=2.0)

    assert not ws_thread.is_alive()
    assert not rest_thread.is_alive()
    assert thread_errors == {}
    assert delivered_cumulative == pytest.approx([0.0004, 0.001])
    assert callback_cursor == pytest.approx(0.001)
    assert manager.fatal_latched is False


def test_callback_failure_blocks_later_committed_rest_delivery() -> None:
    first_callback_entered = threading.Event()
    release_first_callback = threading.Event()
    attempted_cumulative: list[float] = []
    thread_errors: dict[str, BaseException] = {}

    def on_fill(_order, event) -> None:
        cumulative = float(event["z"])
        attempted_cumulative.append(cumulative)
        if cumulative == pytest.approx(0.0004):
            first_callback_entered.set()
            if not release_first_callback.wait(timeout=2.0):
                raise TimeoutError("test did not release first fill callback")
            raise RuntimeError("inventory sink failed on first committed fill")

    manager, cid = _tracked_order(on_fill=on_fill)

    def run(name: str, target) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors[name] = exc

    ws_thread = threading.Thread(
        target=run,
        args=(
            "ws",
            lambda: manager.on_order_update(
                _event(cid, "PARTIALLY_FILLED", "0.0004")
            ),
        ),
    )
    rest_thread = threading.Thread(
        target=run,
        args=(
            "rest",
            lambda: manager.reconcile_exchange_trade(
                exchange_order_id=42,
                trade_id=9002,
                symbol="BTCUSDC",
                side="BUY",
                quantity=0.0006,
                price=100.0,
                commission=0.0,
                commission_asset="USDC",
                cumulative_fill=0.001,
            ),
        ),
    )

    ws_thread.start()
    assert first_callback_entered.wait(timeout=1.0)
    rest_thread.start()
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        order = manager.get_order(cid)
        if order is not None and order.filled_qty == pytest.approx(0.001):
            break
        time.sleep(0.005)
    release_first_callback.set()
    ws_thread.join(timeout=2.0)
    rest_thread.join(timeout=2.0)

    assert not ws_thread.is_alive()
    assert not rest_thread.is_alive()
    assert isinstance(thread_errors.get("ws"), RuntimeError)
    assert "first committed fill" in str(thread_errors["ws"])
    assert isinstance(thread_errors.get("rest"), OrderManagerFatalError)
    assert "blocked by prior dispatch failure" in str(thread_errors["rest"])
    assert attempted_cumulative == pytest.approx([0.0004])
    committed = manager.get_order(cid)
    assert committed is not None
    assert committed.filled_qty == pytest.approx(0.001)
    assert manager.reconciliation_required is True
    assert manager.callback_dispatch_active()


@pytest.mark.parametrize(
    ("bind_method", "first_event_type"),
    [
        ("confirm_new", "rest_ack"),
        ("bind_exchange_order_identity", "activate_unknown_prefix"),
    ],
)
def test_direct_activation_callback_cannot_be_overtaken_by_ws_fill(
    bind_method: str,
    first_event_type: str,
) -> None:
    first_callback_entered = threading.Event()
    release_first_callback = threading.Event()
    lifecycle_events: list[str] = []
    thread_errors: dict[str, BaseException] = {}

    def on_lifecycle(_order, event_type: str, _event) -> None:
        lifecycle_events.append(event_type)
        if event_type == first_event_type:
            first_callback_entered.set()
            if not release_first_callback.wait(timeout=2.0):
                raise TimeoutError("test did not release activation callback")

    manager = OrderManager(on_lifecycle_event=on_lifecycle)
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)

    def run(name: str, target) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors[name] = exc

    def bind() -> None:
        if bind_method == "confirm_new":
            manager.confirm_new(cid, 42)
        else:
            manager.bind_exchange_order_identity(
                cid,
                42,
                activation_unknown=True,
            )

    bind_thread = threading.Thread(target=run, args=("bind", bind))
    fill_thread = threading.Thread(
        target=run,
        args=(
            "fill",
            lambda: manager.on_order_update(
                _event(cid, "PARTIALLY_FILLED", "0.0004")
            ),
        ),
    )

    bind_thread.start()
    assert first_callback_entered.wait(timeout=1.0)
    fill_thread.start()
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        order = manager.get_order(cid)
        if order is not None and order.filled_qty == pytest.approx(0.0004):
            break
        time.sleep(0.005)

    committed = manager.get_order(cid)
    assert committed is not None
    assert committed.filled_qty == pytest.approx(0.0004)
    assert fill_thread.is_alive()
    assert lifecycle_events == [first_event_type]

    release_first_callback.set()
    bind_thread.join(timeout=2.0)
    fill_thread.join(timeout=2.0)

    assert not bind_thread.is_alive()
    assert not fill_thread.is_alive()
    assert thread_errors == {}
    assert lifecycle_events == [first_event_type, "partial_fill"]


def test_callback_generated_lifecycle_batch_is_deferred_without_nesting() -> None:
    lifecycle_events: list[tuple[str, str]] = []
    callback_depth = 0
    maximum_callback_depth = 0
    manager: OrderManager
    second_cid = ""

    def on_lifecycle(order, event_type: str, _event) -> None:
        nonlocal callback_depth, maximum_callback_depth
        callback_depth += 1
        maximum_callback_depth = max(maximum_callback_depth, callback_depth)
        try:
            lifecycle_events.append((order.client_order_id, event_type))
            if event_type == "rest_ack" and order.client_order_id != second_cid:
                manager.confirm_new(second_cid, 43)
        finally:
            callback_depth -= 1

    manager = OrderManager(on_lifecycle_event=on_lifecycle)
    first_cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    second_cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)

    manager.confirm_new(first_cid, 42)

    assert lifecycle_events == [
        (first_cid, "rest_ack"),
        (second_cid, "rest_ack"),
    ]
    assert maximum_callback_depth == 1
    assert manager.get_order(second_cid).state == OrderState.OPEN


def test_deferred_reentrant_callback_failure_reaches_outer_caller() -> None:
    lifecycle_events: list[str] = []
    manager: OrderManager
    second_cid = ""

    def on_lifecycle(order, event_type: str, _event) -> None:
        lifecycle_events.append(order.client_order_id)
        if event_type != "rest_ack":
            return
        if order.client_order_id != second_cid:
            manager.confirm_new(second_cid, 43)
        else:
            raise RuntimeError("deferred lifecycle sink failed")

    manager = OrderManager(on_lifecycle_event=on_lifecycle)
    first_cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    second_cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)

    with pytest.raises(RuntimeError, match="deferred lifecycle sink failed"):
        manager.confirm_new(first_cid, 42)

    assert lifecycle_events == [first_cid, second_cid]
    assert manager.fatal_latched is True
    assert manager.reconciliation_required is True


def test_identity_only_rest_bind_preserves_unknown_activation_and_unlocks_callback() -> None:
    lifecycle_events: list[str] = []
    manager = OrderManager(
        on_lifecycle_event=lambda _order, event_type, _event: (
            manager.get_active_orders(),
            lifecycle_events.append(event_type),
        )
    )
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)

    assert manager.bind_exchange_order_identity(cid, 42, activation_unknown=True)

    order = manager.get_order(cid)
    assert order is not None
    assert order.order_id == 42
    assert order.state == OrderState.OPEN
    assert lifecycle_events == ["activate_unknown_prefix"]
    snapshot = manager.lifecycle_snapshot(cid)
    assert snapshot is not None
    assert snapshot["activation_ts_ns"] == 0
    assert snapshot["visible_exposure_valid"] is False
    assert snapshot["exchange_exposure_valid"] is False


def test_post_cancel_full_fill_applies_only_missing_delta_once() -> None:
    fill_deltas: list[float] = []
    terminal_reasons: list[str] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: fill_deltas.append(event["_fill_qty"]),
        on_terminal=lambda _order, reason: terminal_reasons.append(reason),
    )

    manager.on_order_update(_event(cid, "PARTIALLY_FILLED", "0.0004"))
    manager.on_order_update(_event(cid, "CANCELED", "0.0004"))
    with pytest.raises(OrderReconciliationRequired):
        manager.on_order_update(_event(cid, "FILLED", "0.001", l="0.0006"))

    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.FILLED
    assert order.filled_qty == pytest.approx(0.001)
    assert fill_deltas == pytest.approx([0.0004, 0.0006])
    # The correction is a new fill fact, not a second exchange-terminal event.
    assert terminal_reasons == ["cancel_ack"]
    assert manager.fatal_status()["reconciliation_required"] is True


@pytest.mark.parametrize("invalid_cumulative", ["0.002", "nan", "inf", "-0.1"])
def test_invalid_cumulative_fill_is_rejected_without_ledger_mutation(
    invalid_cumulative: str,
) -> None:
    fills: list[dict] = []
    manager, cid = _tracked_order(on_fill=lambda _order, event: fills.append(event))

    with pytest.raises(OrderReconciliationRequired):
        manager.on_order_update(_event(cid, "FILLED", invalid_cumulative))

    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.OPEN
    assert order.filled_qty == 0.0
    assert manager.active_count() == 1
    assert fills == []
    assert manager.reconciliation_required is True


def test_incomplete_filled_status_applies_proven_delta_then_latches_fatal() -> None:
    fill_deltas: list[float] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: fill_deltas.append(event["_fill_qty"])
    )

    with pytest.raises(OrderReconciliationRequired):
        manager.on_order_update(_event(cid, "FILLED", "0.0004"))

    partial = manager.get_order(cid)
    assert partial is not None
    assert partial.state == OrderState.PARTIALLY_FILLED
    assert manager.active_count() == 1
    assert fill_deltas == pytest.approx([0.0004])
    assert manager.reconciliation_required is True


def test_regressed_cumulative_event_does_not_regress_state_or_callback() -> None:
    fill_deltas: list[float] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: fill_deltas.append(event["_fill_qty"])
    )
    manager.on_order_update(_event(cid, "PARTIALLY_FILLED", "0.0004"))

    manager.on_order_update(_event(cid, "CANCELED", "0.0003"))

    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.PARTIALLY_FILLED
    assert order.filled_qty == pytest.approx(0.0004)
    assert manager.active_count() == 1
    assert fill_deltas == pytest.approx([0.0004])


def test_terminal_tombstone_survives_rich_history_eviction_and_deduplicates() -> None:
    fill_deltas: list[float] = []
    manager = OrderManager(
        max_history=1,
        on_fill=lambda _order, event: fill_deltas.append(event["_fill_qty"]),
    )
    first = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(first, 42)
    manager.on_order_update(_event(first, "FILLED", "0.001"))
    second = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(second, 43)
    manager.on_order_update(_event(second, "CANCELED", "0", i=43, l="0"))

    assert first not in manager._history
    assert manager.tombstone_count() == 2
    evicted_proof = manager.get_order(first)
    assert evicted_proof is not None
    assert evicted_proof.state == OrderState.FILLED
    assert manager.terminal_identity(first) == {
        "client_order_id": first,
        "exchange_order_id": 42,
        "symbol": "BTCUSDC",
        "side": "BUY",
        "price": 100.0,
        "quantity": 0.001,
        "cumulative_fill": 0.001,
        "average_fill_price": 100.0,
        "terminal_state": "FILLED",
        "terminal_reason": "full_fill",
        "max_trade_id": 0,
    }

    manager.on_order_update(_event(first, "FILLED", "0.001"))

    assert manager.active_count() == 0
    assert fill_deltas == pytest.approx([0.001])


@pytest.mark.parametrize(
    ("event_cid", "event_oid"),
    [("same", 0), ("different", 42), ("same", 43)],
)
def test_active_identity_mismatch_latches_reconciliation(
    event_cid: str,
    event_oid: int,
) -> None:
    manager, cid = _tracked_order()
    cid_value = cid if event_cid == "same" else "mm_B_wrong_cid"

    with pytest.raises(OrderReconciliationRequired):
        manager.on_order_update(_event(cid_value, "NEW", "0", i=event_oid))

    status = manager.fatal_status()
    assert status["latched"] is True
    assert status["reconciliation_required"] is True


def test_history_identity_mismatch_latches_reconciliation() -> None:
    manager, cid = _tracked_order()
    manager.on_order_update(_event(cid, "CANCELED", "0", l="0"))

    with pytest.raises(OrderReconciliationRequired):
        manager.on_order_update(_event(cid, "CANCELED", "0", i=0, l="0"))

    assert manager.reconciliation_required is True


@pytest.mark.parametrize("terminal", [False, True])
def test_duplicate_or_late_rest_ack_cannot_change_bound_order_id(terminal: bool) -> None:
    manager, cid = _tracked_order()
    if terminal:
        manager.on_order_update(_event(cid, "CANCELED", "0", l="0"))

    with pytest.raises(OrderReconciliationRequired, match="REST ACK exchange order ID mismatch"):
        manager.confirm_new(cid, 43)

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.order_id == 42
    assert manager.reconciliation_required is True


def test_callback_exception_latches_fatal_after_ledger_commit_and_reraises() -> None:
    def fail_delivery(_order, _event) -> None:
        raise RuntimeError("inventory sink unavailable")

    manager, cid = _tracked_order(on_fill=fail_delivery)

    with pytest.raises(RuntimeError, match="inventory sink unavailable"):
        manager.on_order_update(_event(cid, "FILLED", "0.001"))

    committed = manager.get_order(cid)
    assert committed is not None
    assert committed.state == OrderState.FILLED
    assert committed.filled_qty == pytest.approx(0.001)
    status = manager.fatal_status()
    assert status["latched"] is True
    assert status["callback"] == "on_fill"
    assert status["reconciliation_required"] is True
    with pytest.raises(OrderManagerFatalError):
        manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)


def test_multi_fill_gap_requires_exact_reconciliation_without_fake_economics() -> None:
    fill_deltas: list[float] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: fill_deltas.append(event["_fill_qty"])
    )
    manager.on_order_update(_event(cid, "PARTIALLY_FILLED", "0.0004"))

    with pytest.raises(OrderReconciliationRequired):
        manager.on_order_update(
            _event(
                cid,
                "PARTIALLY_FILLED",
                "0.0009",
                l="0.0002",
                L="101.0",
                ap="100.2",
                n="0.000001",
            )
        )

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.filled_qty == pytest.approx(0.0004)
    assert retained.avg_fill_price == pytest.approx(100.0)
    assert fill_deltas == pytest.approx([0.0004])
    assert "cumulative fill delta" in str(manager.fatal_status()["reason"])


def test_inconsistent_exchange_average_price_cannot_rewrite_fill_notional() -> None:
    fill_deltas: list[float] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: fill_deltas.append(event["_fill_qty"])
    )
    manager.on_order_update(_event(cid, "PARTIALLY_FILLED", "0.0004"))

    with pytest.raises(OrderReconciliationRequired, match="average fill price"):
        manager.on_order_update(
            _event(
                cid,
                "FILLED",
                "0.001",
                l="0.0006",
                L="101.0",
                ap="100.0",
            )
        )

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.state == OrderState.PARTIALLY_FILLED
    assert retained.filled_qty == pytest.approx(0.0004)
    assert retained.avg_fill_price == pytest.approx(100.0)
    assert fill_deltas == pytest.approx([0.0004])
    assert manager.reconciliation_required is True


def test_consistent_exchange_average_is_cross_checked_but_local_notional_is_stored() -> None:
    manager, cid = _tracked_order()
    manager.on_order_update(_event(cid, "PARTIALLY_FILLED", "0.0004"))

    manager.on_order_update(
        _event(
            cid,
            "FILLED",
            "0.001",
            l="0.0006",
            L="101.0",
            ap="100.60000000001",
        )
    )

    filled = manager.get_order(cid)
    assert filled is not None
    assert filled.state == OrderState.FILLED
    assert filled.avg_fill_price == pytest.approx(100.6)


@pytest.mark.parametrize(
    ("last_price", "average_price"),
    [("0", "100"), ("nan", "100"), ("inf", "100"), ("100", "0")],
)
def test_nonpositive_or_nonfinite_execution_price_is_rejected(
    last_price: str,
    average_price: str,
) -> None:
    fills: list[dict] = []
    manager, cid = _tracked_order(on_fill=lambda _order, event: fills.append(event))

    with pytest.raises(OrderReconciliationRequired):
        manager.on_order_update(_event(cid, "FILLED", "0.001", L=last_price, ap=average_price))

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.state == OrderState.OPEN
    assert retained.filled_qty == 0.0
    assert fills == []
    assert manager.reconciliation_required is True


def test_finite_signed_commission_rebate_is_preserved() -> None:
    commissions: list[float] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: commissions.append(event["_fill_commission"])
    )

    manager.on_order_update(_event(cid, "FILLED", "0.001", n="-0.000001"))

    assert commissions == pytest.approx([-0.000001])


def test_zero_order_quantity_is_rejected_before_registration() -> None:
    manager = OrderManager()

    with pytest.raises(ValueError, match="order quantity must be positive"):
        manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.0)

    assert manager.active_count() == 0


def test_reconcile_exchange_trade_is_oid_trade_id_and_cumulative_idempotent() -> None:
    delivered: list[tuple[float, float, float]] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: delivered.append(
            (
                event["_fill_qty"],
                event["_fill_price"],
                event["_fill_commission"],
            )
        )
    )

    assert manager.reconcile_exchange_trade(
        exchange_order_id=42,
        trade_id=9001,
        symbol="BTCUSDC",
        side="BUY",
        quantity=0.0004,
        price=99.5,
        commission=-0.000001,
        commission_asset="USDC",
        cumulative_fill=0.0004,
    )
    assert not manager.reconcile_exchange_trade(
        exchange_order_id=42,
        trade_id=9001,
        symbol="BTCUSDC",
        side="BUY",
        quantity=0.0004,
        price=99.5,
        commission=-0.000001,
        commission_asset="USDC",
        cumulative_fill=0.0004,
    )

    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.PARTIALLY_FILLED
    assert order.filled_qty == pytest.approx(0.0004)
    assert delivered == pytest.approx([(0.0004, 99.5, -0.000001)])


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("quantity", 0.0009),
        ("price", 99.6),
        ("commission", -0.000002),
        ("commission_asset", "BNB"),
        ("trade_time_ms", 124),
        ("cumulative_fill", 0.0009),
    ],
)
def test_evicted_trade_id_tombstone_rejects_economic_identity_drift(
    field: str,
    changed_value: object,
) -> None:
    manager = OrderManager(max_history=0)
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 42)
    exact_trade = {
        "exchange_order_id": 42,
        "trade_id": 9001,
        "symbol": "BTCUSDC",
        "side": "BUY",
        "quantity": 0.001,
        "price": 99.5,
        "commission": -0.000001,
        "commission_asset": "USDC",
        "cumulative_fill": 0.001,
        "trade_time_ms": 123,
    }
    assert manager.reconcile_exchange_trade(**exact_trade)
    assert manager.get_order(cid) is not None
    assert cid not in manager._history
    assert not manager.reconcile_exchange_trade(**exact_trade)

    drifted_trade = {**exact_trade, field: changed_value}
    with pytest.raises(OrderReconciliationRequired, match="exact economic identity"):
        manager.reconcile_exchange_trade(**drifted_trade)

    assert manager.terminal_identity(cid)["cumulative_fill"] == pytest.approx(0.001)
    assert manager.reconciliation_required is True


def test_reconcile_exchange_trade_unknown_oid_requires_explicit_order_query() -> None:
    manager = OrderManager()

    with pytest.raises(OrderReconciliationRequired, match="individual order query"):
        manager.reconcile_exchange_trade(
            exchange_order_id=999,
            trade_id=1,
            symbol="BTCUSDC",
            side="BUY",
            quantity=0.0001,
            price=100.0,
            commission=0.0,
            commission_asset="USDC",
            cumulative_fill=0.0001,
        )

    assert manager.active_count() == 0
    assert manager.reconciliation_required is True


def test_reconcile_exchange_trade_gap_requires_more_trade_rows() -> None:
    manager, cid = _tracked_order()

    with pytest.raises(OrderReconciliationRequired, match="bridge cumulative cursor"):
        manager.reconcile_exchange_trade(
            exchange_order_id=42,
            trade_id=2,
            symbol="BTCUSDC",
            side="BUY",
            quantity=0.0002,
            price=100.0,
            commission=0.0,
            commission_asset="USDC",
            cumulative_fill=0.0004,
        )

    order = manager.get_order(cid)
    assert order is not None
    assert order.filled_qty == 0.0


def test_reconcile_exchange_trade_delivers_proven_post_terminal_delta_then_stops() -> None:
    fill_deltas: list[float] = []
    manager, cid = _tracked_order(
        on_fill=lambda _order, event: fill_deltas.append(event["_fill_qty"])
    )
    manager.on_order_update(_event(cid, "CANCELED", "0", l="0"))

    with pytest.raises(OrderReconciliationRequired, match="exchange terminal"):
        manager.reconcile_exchange_trade(
            exchange_order_id=42,
            trade_id=77,
            symbol="BTCUSDC",
            side=Side.BUY,
            quantity=0.001,
            price=100.25,
            commission=-0.000001,
            commission_asset="USDC",
            cumulative_fill=0.001,
        )

    corrected = manager.get_order(cid)
    assert corrected is not None
    assert corrected.state == OrderState.FILLED
    assert corrected.filled_qty == pytest.approx(0.001)
    assert fill_deltas == pytest.approx([0.001])
    assert manager.terminal_identity(cid)["max_trade_id"] == 77


@pytest.mark.parametrize(
    "update",
    [
        {"X": "SUSPENDED", "i": 42},
        {"X": "NEW", "i": "not-an-order-id"},
    ],
)
def test_malformed_exchange_order_update_latches_fatal_without_mutation(
    update: dict,
) -> None:
    manager, cid = _tracked_order()
    event = _event(cid, "NEW", "0", l="0")
    event.update(update)

    with pytest.raises(OrderReconciliationRequired, match="invalid_exchange_order_update"):
        manager.on_order_update(event)

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.state == OrderState.OPEN
    assert retained.filled_qty == 0.0
    assert manager.reconciliation_required is True


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("s", "ETHUSDC", "symbol mismatch"),
        ("S", "SELL", "side mismatch"),
        ("q", "0.002", "original quantity mismatch"),
        ("q", "nan", "original quantity must be finite"),
        ("p", "99.0", "limit order price mismatch"),
        ("p", "nan", "order price must be finite"),
        ("o", "MARKET", "order type disagrees"),
    ],
)
def test_active_ws_identity_drift_fails_before_ledger_mutation(
    field: str,
    value: object,
    reason: str,
) -> None:
    manager, cid = _tracked_order()
    before = manager.terminal_identity(cid)

    with pytest.raises(OrderReconciliationRequired, match=reason):
        manager.on_order_update(
            _event(cid, "PARTIALLY_FILLED", "0.0004", **{field: value})
        )

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.state == OrderState.OPEN
    assert retained.filled_qty == 0.0
    assert retained.avg_fill_price == 0.0
    assert manager.terminal_identity(cid) == before
    assert manager.fatal_latched is True
    assert manager.reconciliation_required is True


def test_active_ws_identity_accepts_quantity_epsilon_and_market_missing_price() -> None:
    limit_manager, limit_cid = _tracked_order()
    limit_manager.on_order_update(
        _event(
            limit_cid,
            "NEW",
            "0",
            l="0",
            q=str(0.001 + 0.5e-10),
        )
    )
    assert limit_manager.fatal_latched is False

    market_manager = OrderManager()
    market_cid = market_manager.create_order("BTCUSDC", Side.BUY, 0.0, 0.001)
    market_manager.confirm_new(market_cid, 43)
    market_event = _event(
        market_cid,
        "NEW",
        "0",
        i=43,
        l="0",
        o="MARKET",
    )
    market_event.pop("p")
    market_manager.on_order_update(market_event)
    assert market_manager.fatal_latched is False
    assert market_manager.get_order(market_cid).state == OrderState.OPEN


@pytest.mark.parametrize("max_history", [1, 0])
def test_terminal_ws_identity_drift_fails_for_history_and_tombstone(
    max_history: int,
) -> None:
    manager = OrderManager(max_history=max_history)
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 42)
    manager.on_order_update(_event(cid, "CANCELED", "0", l="0"))
    before = manager.terminal_identity(cid)

    with pytest.raises(OrderReconciliationRequired, match="side mismatch"):
        manager.on_order_update(
            _event(cid, "CANCELED", "0", l="0", S="SELL")
        )

    assert manager.terminal_identity(cid) == before
    assert manager.active_count() == 0
    assert manager.reconciliation_required is True


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("s", "ETHUSDC", "symbol mismatch"),
        ("S", "SELL", "side mismatch"),
        ("q", "0.002", "original quantity mismatch"),
        ("p", "99.0", "limit order price mismatch"),
    ],
)
def test_orphan_followup_preserves_adopted_exact_identity(
    field: str,
    value: object,
    reason: str,
) -> None:
    lifecycle_events: list[str] = []
    manager = OrderManager(
        allowed_symbols={"BTCUSDC"},
        on_lifecycle_event=lambda _order, kind, _event: lifecycle_events.append(kind),
    )
    cid = "mm_B_restart_orphan"
    manager.on_order_update(_event(cid, "NEW", "0", i=99, l="0"))
    before = manager.terminal_identity(cid)

    with pytest.raises(OrderReconciliationRequired, match=reason):
        manager.on_order_update(
            _event(cid, "PARTIALLY_FILLED", "0.0004", i=99, **{field: value})
        )

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.state == OrderState.OPEN
    assert retained.filled_qty == 0.0
    assert manager.terminal_identity(cid) == before
    assert lifecycle_events == ["activate_unknown_prefix"]


@pytest.mark.parametrize(
    ("status", "cumulative"),
    [("PARTIALLY_FILLED", "0.0004"), ("CANCELED", "0")],
)
def test_cross_symbol_strategy_orphan_is_never_adopted_or_dispatched(
    status: str,
    cumulative: str,
) -> None:
    callbacks: list[str] = []
    manager = OrderManager(
        allowed_symbols={"BTCUSDC"},
        on_fill=lambda _order, _event: callbacks.append("fill"),
        on_terminal=lambda _order, _reason: callbacks.append("terminal"),
        on_lifecycle_event=lambda _order, kind, _event: callbacks.append(kind),
    )
    cid = "mm_B_foreign_symbol"
    event = _event(
        cid,
        status,
        cumulative,
        i=99,
        s="ETHUSDC",
        l=cumulative,
    )

    with pytest.raises(OrderReconciliationRequired, match="outside the configured"):
        manager.on_order_update(event)

    assert manager.get_order(cid) is None
    assert manager.active_count() == 0
    assert manager._history == {}
    assert manager._tombstones == {}
    assert callbacks == []
    assert manager.reconciliation_required is True


def test_orphan_followup_with_exact_identity_delivers_fifo_normally() -> None:
    callbacks: list[str] = []
    manager = OrderManager(
        allowed_symbols={"BTCUSDC"},
        on_fill=lambda _order, _event: callbacks.append("fill"),
        on_lifecycle_event=lambda _order, kind, _event: callbacks.append(kind),
    )
    cid = "mm_B_restart_orphan"
    manager.on_order_update(_event(cid, "NEW", "0", i=99, l="0"))
    manager.on_order_update(
        _event(cid, "PARTIALLY_FILLED", "0.0004", i=99)
    )

    retained = manager.get_order(cid)
    assert retained is not None
    assert retained.filled_qty == pytest.approx(0.0004)
    assert callbacks == ["activate_unknown_prefix", "partial_fill", "fill"]
    assert manager.fatal_latched is False


@pytest.mark.parametrize("max_history", [500, 0])
def test_zero_delta_known_ws_trade_id_requires_exact_identity(
    max_history: int,
) -> None:
    manager = OrderManager(max_history=max_history)
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 42)
    exact = _event(
        cid,
        "PARTIALLY_FILLED",
        "0.0004",
        l="0.0004",
        L="99.5",
        ap="99.5",
        n="0.001",
        N="USDC",
        t=9001,
        T=123,
    )
    manager.on_order_update(exact)
    if max_history == 0:
        manager.on_order_update(
            _event(
                cid,
                "CANCELED",
                "0.0004",
                l="0",
                L="0",
                ap="99.5",
                t=0,
            )
        )
    before = manager.terminal_identity(cid)
    callbacks_before = manager._callback_commit_sequence
    drifted = dict(exact)
    drifted.update(L="10", n="999")
    if max_history == 0:
        drifted["X"] = "CANCELED"

    with pytest.raises(OrderReconciliationRequired, match="changed exact WS identity"):
        manager.on_order_update(drifted)

    assert manager.terminal_identity(cid) == before
    assert manager._callback_commit_sequence == callbacks_before
    assert manager.reconciliation_required is True


def test_zero_trade_id_terminal_duplicate_remains_idempotent() -> None:
    manager, cid = _tracked_order()
    terminal = _event(cid, "CANCELED", "0", l="0", t=0)
    manager.on_order_update(terminal)
    before = manager.terminal_identity(cid)

    manager.on_order_update(dict(terminal))

    assert manager.terminal_identity(cid) == before
    assert manager.fatal_latched is False
