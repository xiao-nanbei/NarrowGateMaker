import threading

import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

from strategy.order_manager import (  # noqa: E402, I001
    OrderManager,
    OrderReconciliationRequired,
    OrderState,
    Side,
)


BUY = narrowgate_cpp.CanonicalSide.Buy
SELL = narrowgate_cpp.CanonicalSide.Sell
NATIVE = narrowgate_cpp.NativeLiveOrderState
STATUS = narrowgate_cpp.NativeExchangeOrderStatus


def _python_event(
    cid: str,
    status: str,
    cumulative: str,
    *,
    last_fill: str = "0",
    order_id: int = 42,
) -> dict:
    event = {
        "s": "BTCUSDC",
        "c": cid,
        "S": "BUY",
        "o": "LIMIT",
        "X": status,
        "i": order_id,
        "p": "100.0",
        "q": "0.010",
        "z": cumulative,
        "l": last_fill,
        "T": 1,
    }
    if float(last_fill) > 0.0:
        event.update(
            {
                "L": "100.0",
                "ap": "100.0",
                "n": "0.0",
                "N": "USDC",
                "t": 1,
            }
        )
    return event


def test_native_live_order_state_matches_python_core_lifecycle() -> None:
    manager = OrderManager()
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.010)
    native = narrowgate_cpp.NativeLiveOrderStateCore()

    admitted = native.admit(BUY, cid, "BTCUSDC", 1, 1_000, 10, 1_000)
    assert admitted.state == NATIVE.PendingNew
    assert manager.get_order(cid).state == OrderState.PENDING_NEW

    manager.confirm_new(cid, 42)
    activated = native.confirm_new(BUY, cid, 1, 42, 1_100)
    assert activated.state == NATIVE.Open
    assert manager.get_order(cid).state == OrderState.OPEN

    manager.mark_pending_cancel(cid)
    cancel_pending = native.request_cancel(BUY, cid, 1, 1_200)
    assert cancel_pending.state == NATIVE.PendingCancel
    assert manager.get_order(cid).state == OrderState.PENDING_CANCEL

    manager.on_order_update(
        _python_event(
            cid,
            "PARTIALLY_FILLED",
            "0.004",
            last_fill="0.004",
        )
    )
    partial = native.apply_exchange_update(
        BUY,
        cid,
        1,
        42,
        STATUS.PartiallyFilled,
        4,
        1_300,
    )
    assert partial.state == NATIVE.PendingCancel
    assert partial.order.filled_lots == 4
    assert manager.get_order(cid).state == OrderState.PENDING_CANCEL
    assert manager.get_order(cid).filled_qty == pytest.approx(0.004)

    manager.cancel_rejected(cid, "exchange_busy")
    reopened = native.cancel_rejected(BUY, cid, 1, 42, 1_400)
    assert reopened.state == NATIVE.PartiallyFilled
    assert manager.get_order(cid).state == OrderState.PARTIALLY_FILLED

    manager.mark_pending_cancel(cid)
    native.request_cancel(BUY, cid, 1, 1_500)
    manager.on_order_update(_python_event(cid, "CANCELED", "0.004"))
    canceled = native.apply_exchange_update(
        BUY,
        cid,
        1,
        42,
        STATUS.Canceled,
        4,
        1_600,
    )
    assert canceled.state == NATIVE.Canceled
    assert not canceled.order.ownership_active
    assert canceled.order.terminal
    assert manager.get_order(cid).state == OrderState.CANCELED


def test_native_live_order_state_unknown_submit_keeps_ownership_until_update() -> None:
    manager = OrderManager()
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.010)
    native = narrowgate_cpp.NativeLiveOrderStateCore()
    native.admit(BUY, cid, "BTCUSDC", 9, 1_000, 10, 1_000)

    assert manager.mark_submit_ack_unknown(cid, "response_lost")
    unknown = native.mark_submit_ack_unknown(BUY, cid, 9, 1_100)
    assert unknown.state == NATIVE.PendingNew
    assert unknown.order.ownership_active
    assert unknown.order.ack_unknown_kind == (
        narrowgate_cpp.NativeOrderAckUnknownKind.Submit
    )
    assert unknown.order.transport_unknown_state == (
        narrowgate_cpp.TransportUnknownState.AwaitingReconciliation
    )

    manager.on_order_update(_python_event(cid, "NEW", "0", order_id=77))
    reconciled = native.apply_exchange_update(
        BUY,
        cid,
        9,
        77,
        STATUS.New,
        0,
        1_200,
    )
    assert reconciled.state == NATIVE.Open
    assert reconciled.order.activation_unknown_prefix
    assert reconciled.order.ack_unknown_kind == (
        narrowgate_cpp.NativeOrderAckUnknownKind.None_
    )
    assert manager.get_order(cid).state == OrderState.OPEN
    assert manager.lifecycle_snapshot(cid)["visible_exposure_valid"] is False


def test_native_pending_cancel_absence_never_releases_ownership() -> None:
    manager = OrderManager()
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.010)
    manager.confirm_new(cid, 42)
    manager.mark_pending_cancel(cid)

    native = narrowgate_cpp.NativeLiveOrderStateCore()
    native.admit(BUY, cid, "BTCUSDC", 1, 1_000, 10, 1_000)
    native.confirm_new(BUY, cid, 1, 42, 1_100)
    native.request_cancel(BUY, cid, 1, 1_200)

    assert not manager.reconcile_pending_cancel(cid, exchange_open=False)
    unresolved = native.reconcile_pending_cancel(
        BUY,
        cid,
        1,
        False,
        0,
        1_300,
    )
    assert not unresolved.accepted
    assert unresolved.idempotent
    assert unresolved.reason == "open_order_absence_unresolved"
    assert unresolved.order.ownership_active
    assert unresolved.state == NATIVE.PendingCancel


def test_native_partial_fill_does_not_resolve_unknown_cancel() -> None:
    native = narrowgate_cpp.NativeLiveOrderStateCore()
    native.admit(BUY, "buy-1", "BTCUSDC", 1, 1_000, 10, 1_000)
    native.confirm_new(BUY, "buy-1", 1, 42, 1_100)
    native.request_cancel(BUY, "buy-1", 1, 1_200)
    native.mark_cancel_ack_unknown(BUY, "buy-1", 1, 1_300)

    partial = native.apply_exchange_update(
        BUY,
        "buy-1",
        1,
        42,
        STATUS.PartiallyFilled,
        4,
        1_400,
    )
    assert partial.state == NATIVE.PendingCancel
    assert partial.order.ack_unknown_kind == (
        narrowgate_cpp.NativeOrderAckUnknownKind.Cancel
    )
    assert partial.order.transport_unknown_state == (
        narrowgate_cpp.TransportUnknownState.AwaitingReconciliation
    )

    terminal = native.apply_exchange_update(
        BUY,
        "buy-1",
        1,
        42,
        STATUS.Canceled,
        4,
        1_500,
    )
    assert terminal.state == NATIVE.Canceled
    assert terminal.order.ack_unknown_kind == (
        narrowgate_cpp.NativeOrderAckUnknownKind.None_
    )


def test_native_same_side_overlap_and_wrong_generation_fail_closed() -> None:
    native = narrowgate_cpp.NativeLiveOrderStateCore()
    native.admit(BUY, "buy-1", "BTCUSDC", 1, 1_000, 10, 1_000)

    with pytest.raises(RuntimeError, match="second non-terminal"):
        native.admit(BUY, "buy-2", "BTCUSDC", 2, 1_001, 10, 1_100)

    assert native.reconciliation_required
    assert "second non-terminal" in native.reconciliation_reason
    assert native.snapshot(BUY).client_order_id == "buy-1"
    with pytest.raises(RuntimeError, match="blocked"):
        native.admit(SELL, "sell-1", "BTCUSDC", 1, 1_002, 10, 1_200)


def test_native_identity_and_out_of_order_transitions_fail_closed() -> None:
    wrong_generation = narrowgate_cpp.NativeLiveOrderStateCore()
    wrong_generation.admit(
        BUY,
        "buy-owned",
        "BTCUSDC",
        7,
        1_000,
        10,
        1_000,
    )
    with pytest.raises(RuntimeError, match="generation disagrees"):
        wrong_generation.confirm_new(BUY, "buy-owned", 8, 42, 1_100)

    preserved = wrong_generation.snapshot(BUY)
    assert preserved.client_order_id == "buy-owned"
    assert preserved.ownership_generation == 7
    assert preserved.state == NATIVE.PendingNew
    assert wrong_generation.reconciliation_required

    out_of_order = narrowgate_cpp.NativeLiveOrderStateCore()
    out_of_order.admit(
        SELL,
        "sell-pending-new",
        "BTCUSDC",
        3,
        1_001,
        10,
        2_000,
    )
    with pytest.raises(RuntimeError, match="requires OPEN or PARTIALLY_FILLED"):
        out_of_order.request_cancel(SELL, "sell-pending-new", 3, 2_100)

    preserved = out_of_order.snapshot(SELL)
    assert preserved.client_order_id == "sell-pending-new"
    assert preserved.ownership_generation == 3
    assert preserved.state == NATIVE.PendingNew
    assert out_of_order.reconciliation_required


def test_native_incomplete_filled_matches_python_commit_then_fatal() -> None:
    manager = OrderManager()
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.010)
    manager.confirm_new(cid, 42)
    native = narrowgate_cpp.NativeLiveOrderStateCore()
    native.admit(BUY, cid, "BTCUSDC", 1, 1_000, 10, 1_000)
    native.confirm_new(BUY, cid, 1, 42, 1_100)

    with pytest.raises(
        OrderReconciliationRequired,
        match="incomplete cumulative quantity",
    ):
        manager.on_order_update(
            _python_event(cid, "FILLED", "0.004", last_fill="0.004")
        )
    with pytest.raises(RuntimeError, match="incomplete cumulative quantity"):
        native.apply_exchange_update(
            BUY,
            cid,
            1,
            42,
            STATUS.Filled,
            4,
            1_200,
        )

    assert manager.get_order(cid).state == OrderState.PARTIALLY_FILLED
    assert manager.get_order(cid).filled_qty == pytest.approx(0.004)
    snapshot = native.snapshot(BUY)
    assert snapshot.state == NATIVE.PartiallyFilled
    assert snapshot.filled_lots == 4
    assert snapshot.ownership_active
    assert native.reconciliation_required


def test_native_terminal_releases_side_for_strictly_new_generation() -> None:
    native = narrowgate_cpp.NativeLiveOrderStateCore()
    native.admit(SELL, "sell-1", "BTCUSDC", 4, 1_001, 10, 1_000)
    native.confirm_rejected(SELL, "sell-1", 4, 1_100, True)

    next_order = native.admit(
        SELL,
        "sell-2",
        "BTCUSDC",
        8,
        1_002,
        12,
        1_200,
    )
    assert next_order.state == NATIVE.PendingNew
    assert next_order.order.ownership_generation == 8
    assert next_order.order.client_order_id == "sell-2"
    assert next_order.order.filled_lots == 0

    with pytest.raises(RuntimeError, match="strictly advance"):
        native.confirm_rejected(SELL, "sell-2", 8, 1_300, True)
        native.admit(SELL, "sell-stale", "BTCUSDC", 8, 1_003, 10, 1_400)


def test_native_invalid_fixed_text_input_preserves_prior_side_state() -> None:
    native = narrowgate_cpp.NativeLiveOrderStateCore()
    native.admit(BUY, "buy-terminal", "BTCUSDC", 1, 1_000, 10, 1_000)
    native.confirm_rejected(BUY, "buy-terminal", 1, 1_100, True)
    before = native.snapshot(BUY)

    with pytest.raises(ValueError, match="symbol exceeds fixed native capacity"):
        native.admit(BUY, "buy-next", "X" * 33, 2, 1_001, 10, 1_200)

    after = native.snapshot(BUY)
    assert after.client_order_id == before.client_order_id
    assert after.symbol == before.symbol
    assert after.ownership_generation == before.ownership_generation
    assert after.state == before.state
    assert not native.reconciliation_required


def test_native_order_state_layout_uses_architecture_isolation_boundary() -> None:
    core = narrowgate_cpp.NativeLiveOrderStateCore
    assert narrowgate_cpp.NATIVE_LIVE_ORDER_STATE_CORE_AVAILABLE is True
    assert narrowgate_cpp.NATIVE_LIVE_ORDER_STATE_RESULT_ABI_VERSION == 1
    assert core.result_abi_version == 1
    assert core.snapshot_result_size_bytes == 200
    assert core.transition_result_size_bytes == 216
    assert core.isolation_bytes in {64, 128}
    assert core.side_cell_alignment_bytes == core.isolation_bytes
    assert core.side_cell_size_bytes % core.isolation_bytes == 0
    assert core.core_alignment_bytes == core.isolation_bytes
    assert core.core_size_bytes % core.isolation_bytes == 0
    assert core.max_client_order_id_bytes == 64

    native = core()
    transition = native.admit(
        BUY,
        "b" * core.max_client_order_id_bytes,
        "BTCUSDC",
        1,
        1_000,
        10,
        1_000,
    )
    assert transition.abi_version == core.result_abi_version
    assert transition.order.abi_version == core.result_abi_version
    assert transition.reason_code == (
        narrowgate_cpp.NativeLiveOrderTransitionReason.AdmittedPendingNew
    )
    # String diagnostics remain source-compatible, but are materialized only
    # when accessed after the fixed ABI result has been published.
    assert transition.reason == "admitted_pending_new"
    assert transition.order.client_order_id == "b" * 64
    assert transition.order.symbol == "BTCUSDC"


def test_native_order_state_pybind_cross_side_calls_are_thread_safe() -> None:
    core = narrowgate_cpp.NativeLiveOrderStateCore()
    start = threading.Barrier(3)
    errors: list[BaseException] = []

    def run_side(side, prefix: str, exchange_base: int) -> None:
        try:
            start.wait()
            for generation in range(1, 501):
                client_order_id = f"{prefix}-{generation}"
                timestamp = generation * 10
                core.admit(
                    side,
                    client_order_id,
                    "BTCUSDC",
                    generation,
                    1_000,
                    10,
                    timestamp + 1,
                )
                core.confirm_new(
                    side,
                    client_order_id,
                    generation,
                    exchange_base + generation,
                    timestamp + 2,
                )
                core.request_cancel(
                    side,
                    client_order_id,
                    generation,
                    timestamp + 3,
                )
                core.apply_exchange_update(
                    side,
                    client_order_id,
                    generation,
                    exchange_base + generation,
                    STATUS.Canceled,
                    0,
                    timestamp + 4,
                )
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    buy = threading.Thread(target=run_side, args=(BUY, "buy", 1_000_000))
    sell = threading.Thread(target=run_side, args=(SELL, "sell", 2_000_000))
    buy.start()
    sell.start()
    start.wait()
    buy.join(timeout=10)
    sell.join(timeout=10)

    assert not buy.is_alive()
    assert not sell.is_alive()
    assert errors == []
    assert core.snapshot(BUY).state == NATIVE.Canceled
    assert core.snapshot(SELL).state == NATIVE.Canceled
    assert core.telemetry().transition_count == 4_000
