from __future__ import annotations

import hashlib
import json
import stat
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import strategy.maker_engine as maker_engine_module
from live.main import (
    build_stopped_exchange_reconciliation,
    write_stopped_exchange_reconciliation,
)
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, Side


def _producer_identity(
    *,
    order_id: str,
    side: str,
    quantity: float,
    price: float,
    commission: float,
    commission_asset: str,
    trade_time_ms: int,
    cumulative: float,
) -> dict[str, object]:
    return {
        "order_id": order_id,
        "symbol": "BTCUSDC",
        "side": side,
        "quantity": quantity,
        "price": price,
        "commission": commission,
        "commission_asset": "USDC",
        "raw_commission": commission,
        "raw_commission_asset": commission_asset,
        "trade_time_ms": trade_time_ms,
        "cumulative_filled_qty": cumulative,
    }


def _engine_with_payload(payload: tuple) -> SimpleNamespace:
    return SimpleNamespace(
        cfg=SimpleNamespace(symbol="BTCUSDC"),
        inventory=SimpleNamespace(sync_from_exchange=Mock(return_value={"ok": True})),
        orders=SimpleNamespace(
            reconcile_exchange_trade=Mock(return_value=True),
            fatal_status=Mock(
                return_value={"latched": False, "reconciliation_required": False}
            ),
        ),
        _reconciliation_lock=threading.Lock(),
        _reconciliation_trade_identity_by_id={},
        _stable_exchange_reconciliation_payload=Mock(return_value=payload),
    )


def test_sync_position_installs_initial_identity_barrier_without_replaying_history() -> None:
    engine = _engine_with_payload(
        (
            0.001,
            70_000.0,
            1_900_000_000_000,
            {"41": 0.001},
            ("91",),
            (
                {
                    "exchange_order_id": 41,
                    "trade_id": 91,
                    "symbol": "BTCUSDC",
                    "side": "BUY",
                    "quantity": 0.001,
                    "price": 70_000.0,
                    "commission": 0.01,
                    "commission_asset": "USDC",
                    "cumulative_fill": 0.001,
                    "trade_time_ms": 1_900_000_000_000,
                },
            ),
            {
                "91": _producer_identity(
                    order_id="41",
                    side="BUY",
                    quantity=0.001,
                    price=70_000.0,
                    commission=0.01,
                    commission_asset="USDC",
                    trade_time_ms=1_900_000_000_000,
                    cumulative=0.001,
                )
            },
            True,
        )
    )

    assert MakerEngine.sync_position(engine, required=True) is True
    engine.orders.reconcile_exchange_trade.assert_not_called()
    engine.inventory.sync_from_exchange.assert_called_once_with(
        0.001,
        70_000.0,
        snapshot_update_time_ms=1_900_000_000_000,
        order_cumulative_filled_qty={"41": 0.001},
        included_trade_ids=("91",),
        included_trade_identities={
            "91": _producer_identity(
                order_id="41",
                side="BUY",
                quantity=0.001,
                price=70_000.0,
                commission=0.01,
                commission_asset="USDC",
                trade_time_ms=1_900_000_000_000,
                cumulative=0.001,
            )
        },
    )
    assert set(engine._reconciliation_trade_identity_by_id) == {"91"}


def test_running_sync_delivers_each_identified_trade_before_exact_barrier() -> None:
    trade = {
        "exchange_order_id": 41,
        "trade_id": 92,
        "symbol": "BTCUSDC",
        "side": "SELL",
        "quantity": 0.0004,
        "price": 70_100.0,
        "commission": -0.002,
        "commission_asset": "USDC",
        "cumulative_fill": 0.0004,
        "trade_time_ms": 2_000,
    }
    engine = _engine_with_payload(
        (
            0.0,
            0.0,
            2_000,
            {"41": 0.0004},
            ("92",),
            (trade,),
            {
                "92": _producer_identity(
                    order_id="41",
                    side="SELL",
                    quantity=0.0004,
                    price=70_100.0,
                    commission=-0.002,
                    commission_asset="USDC",
                    trade_time_ms=2_000,
                    cumulative=0.0004,
                )
            },
            False,
        )
    )

    assert MakerEngine.sync_position(engine, required=True) is True
    engine.orders.reconcile_exchange_trade.assert_called_once()
    delivered = engine.orders.reconcile_exchange_trade.call_args.kwargs
    assert {key: delivered[key] for key in trade} == trade
    assert delivered["local_receive_ts_ns"] > 0
    engine.inventory.sync_from_exchange.assert_called_once_with(
        0.0,
        0.0,
        snapshot_update_time_ms=2_000,
        order_cumulative_filled_qty={"41": 0.0004},
        included_trade_ids=("92",),
        included_trade_identities={
            "92": _producer_identity(
                order_id="41",
                side="SELL",
                quantity=0.0004,
                price=70_100.0,
                commission=-0.002,
                commission_asset="USDC",
                trade_time_ms=2_000,
                cumulative=0.0004,
            )
        },
    )


def test_sync_retries_transient_position_before_account_trade_visibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_payload = (0.002, 70_000.0, 2_000, {}, (), (), {}, False)
    second_payload = (
        0.002,
        70_000.0,
        2_000,
        {"41": 0.001},
        ("92",),
        (),
        {
            "92": _producer_identity(
                order_id="41",
                side="SELL",
                quantity=0.001,
                price=70_000.0,
                commission=0.0,
                commission_asset="USDC",
                trade_time_ms=2_000,
                cumulative=0.001,
            )
        },
        False,
    )
    engine = _engine_with_payload(first_payload)
    retry_delays = (
        maker_engine_module._POSITION_RECONCILIATION_IDENTITY_LAG_BACKOFF_S
    )
    identity_lag = RuntimeError(
        "exchange snapshot omitted the identity cursor for a locally "
        "applied fill at or before its update time"
    )
    engine._stable_exchange_reconciliation_payload.side_effect = (
        *(first_payload for _ in retry_delays),
        second_payload,
    )
    engine.inventory.sync_from_exchange.side_effect = (
        *(RuntimeError(str(identity_lag)) for _ in retry_delays),
        {"ok": True},
    )
    engine.latch_runtime_fatal = Mock()
    sleeps: list[float] = []
    monkeypatch.setattr(maker_engine_module.time, "sleep", sleeps.append)

    assert MakerEngine.sync_position(engine, required=True) is True

    expected_attempts = len(retry_delays) + 1
    assert (
        engine._stable_exchange_reconciliation_payload.call_count
        == expected_attempts
    )
    assert engine.inventory.sync_from_exchange.call_count == expected_attempts
    assert sleeps == pytest.approx(retry_delays)
    assert sum(sleeps) > 1.0
    assert engine.latch_runtime_fatal.call_count == 0
    assert set(engine._reconciliation_trade_identity_by_id) == {"92"}


def test_sync_fails_closed_after_bounded_identity_visibility_lag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = (0.002, 70_000.0, 2_000, {}, (), (), {}, False)
    engine = _engine_with_payload(payload)
    retry_delays = (
        maker_engine_module._POSITION_RECONCILIATION_IDENTITY_LAG_BACKOFF_S
    )
    attempts = len(retry_delays) + 1
    engine.inventory.sync_from_exchange.side_effect = tuple(
        RuntimeError(
            "exchange snapshot omitted the identity cursor for a locally "
            "applied fill at or before its update time"
        )
        for _ in range(attempts)
    )
    engine.latch_runtime_fatal = Mock()
    sleeps: list[float] = []
    monkeypatch.setattr(maker_engine_module.time, "sleep", sleeps.append)

    with pytest.raises(RuntimeError, match="required position sync failed"):
        MakerEngine.sync_position(engine, required=True)

    assert engine._stable_exchange_reconciliation_payload.call_count == attempts
    assert engine.inventory.sync_from_exchange.call_count == attempts
    assert sleeps == pytest.approx(retry_delays)
    engine.latch_runtime_fatal.assert_called_once()
    assert engine.latch_runtime_fatal.call_args.kwargs["reason"] == (
        "EXACT_EXECUTION_RECONCILIATION_FAILED"
    )
    assert (
        engine.latch_runtime_fatal.call_args.kwargs["reconciliation_required"]
        is True
    )


def test_sync_does_not_retry_a_non_identity_barrier_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = (0.002, 70_000.0, 2_000, {}, (), (), {}, False)
    engine = _engine_with_payload(payload)
    engine.inventory.sync_from_exchange.side_effect = RuntimeError(
        "exchange order cumulative cursor regressed"
    )
    engine.latch_runtime_fatal = Mock()
    sleeps: list[float] = []
    monkeypatch.setattr(maker_engine_module.time, "sleep", sleeps.append)

    with pytest.raises(RuntimeError, match="required position sync failed"):
        MakerEngine.sync_position(engine, required=True)

    engine._stable_exchange_reconciliation_payload.assert_called_once_with()
    engine.inventory.sync_from_exchange.assert_called_once()
    assert sleeps == []
    engine.latch_runtime_fatal.assert_called_once()


def _stable_fetch_engine(response: object) -> MakerEngine:
    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine._base_asset = "BTC"
    engine._quote_asset = "USDC"
    engine._settlement_asset = "USDC"
    engine._reconciliation_trade_identity_by_id = {}
    engine.inventory = SimpleNamespace(
        reconciliation_snapshot=lambda: {
            "snapshot_update_time_ms": 0,
            "order_cumulative_filled_qty": {},
        }
    )
    engine.rest = SimpleNamespace(
        get_position_risk=Mock(return_value=response),
        get_account_trades=Mock(return_value=[]),
    )
    return engine


@pytest.mark.parametrize(
    "response",
    (
        [],
        [
            {
                "symbol": "ETHUSDC",
                "positionAmt": "0",
                "entryPrice": "0",
                "updateTime": 1,
            }
        ],
        {"symbol": "BTCUSDC", "positionAmt": "0"},
    ),
    ids=("empty", "wrong_symbol", "nonlist"),
)
def test_stable_position_snapshot_rejects_missing_configured_exchange_clock(response) -> None:
    engine = _stable_fetch_engine(response)

    with pytest.raises(RuntimeError):
        engine._stable_exchange_reconciliation_payload(max_attempts=1)
    engine.rest.get_account_trades.assert_not_called()


def test_stable_position_snapshot_accepts_v2_explicit_flat_row() -> None:
    update_time_ms = 1_787_709_422_627
    engine = _stable_fetch_engine(
        [
            {
                "symbol": "BTCUSDC",
                "positionSide": "BOTH",
                "positionAmt": "0.000",
                "entryPrice": "0.0",
                "updateTime": update_time_ms,
            }
        ]
    )

    payload = engine._stable_exchange_reconciliation_payload(max_attempts=1)

    assert payload[0:3] == (0.0, 0.0, update_time_ms)
    engine.rest.get_account_trades.assert_called_once_with(
        symbol="BTCUSDC",
        startTime=update_time_ms,
        endTime=update_time_ms,
        limit=1000,
    )


def test_position_snapshot_and_account_trades_build_monotonic_order_cursors() -> None:
    engine = _stable_fetch_engine([])
    engine._reconciliation_trade_identity_by_id = {
        "80": _producer_identity(
            order_id="40",
            side="BUY",
            quantity=0.0004,
            price=70_000.0,
            commission=0.0,
            commission_asset="",
            trade_time_ms=1_000,
            cumulative=0.0004,
        )
    }
    barrier = {
        "snapshot_update_time_ms": 1_000,
        "order_cumulative_filled_qty": {"40": 0.0004},
    }
    trades = [
        {
            "id": 80,
            "orderId": 40,
            "qty": "0.0004",
            "price": "70000",
            "commission": "0",
            "time": 1_000,
            "side": "BUY",
        },
        {
            "id": 81,
            "orderId": 41,
            "qty": "0.0003",
            "price": "70001",
            "commission": "0.01",
            "commissionAsset": "USDC",
            "time": 1_500,
            "buyer": True,
        },
        {
            "id": 82,
            "orderId": 41,
            "qty": "0.0003",
            "price": "70002",
            "commission": "-0.002",
            "commissionAsset": "USDC",
            "time": 2_000,
            "side": "BUY",
        },
    ]

    payload = engine._exchange_reconciliation_payload(
        (0.001, 70_000.0, 2_000),
        trades,
        barrier=barrier,
    )

    (
        qty,
        entry,
        update_time,
        cursors,
        trade_ids,
        new_trades,
        trade_identities,
        initial_seed,
    ) = payload
    assert qty == pytest.approx(0.001)
    assert entry == pytest.approx(70_000.0)
    assert update_time == 2_000
    assert cursors == {"40": pytest.approx(0.0004), "41": pytest.approx(0.0006)}
    assert trade_ids == ("80", "81", "82")
    assert [trade["cumulative_fill"] for trade in new_trades] == pytest.approx(
        [0.0003, 0.0006]
    )
    assert [trade["commission"] for trade in new_trades] == pytest.approx(
        [0.01, -0.002]
    )
    assert set(trade_identities) == {"80", "81", "82"}
    assert trade_identities["82"]["commission"] == pytest.approx(-0.002)
    assert trade_identities["82"]["cumulative_filled_qty"] == pytest.approx(
        0.0006
    )
    assert initial_seed is False


def test_account_trade_identity_binds_signed_base_rebate_in_quote_asset() -> None:
    engine = _stable_fetch_engine([])
    payload = engine._exchange_reconciliation_payload(
        (0.001, 70_000.0, 2_000),
        [
            {
                "id": 90,
                "orderId": 50,
                "qty": "0.001",
                "price": "70000",
                "commission": "-0.000001",
                "commissionAsset": "BTC",
                "time": 2_000,
                "side": "BUY",
            }
        ],
        barrier={
            "snapshot_update_time_ms": 1_000,
            "order_cumulative_filled_qty": {},
        },
    )

    identity = payload[6]["90"]
    assert identity["commission"] == pytest.approx(-0.07)
    assert identity["commission_asset"] == "USDC"
    assert identity["raw_commission"] == pytest.approx(-0.000001)
    assert identity["raw_commission_asset"] == "BTC"
    assert identity["cumulative_filled_qty"] == pytest.approx(0.001)


def test_account_trade_identity_cannot_drift_across_reconciliation_rounds() -> None:
    engine = _stable_fetch_engine([])
    engine._reconciliation_trade_identity_by_id = {
        "80": _producer_identity(
            order_id="40",
            side="BUY",
            quantity=0.0004,
            price=70_000.0,
            commission=0.0,
            commission_asset="",
            trade_time_ms=1_000,
            cumulative=0.0004,
        )
    }
    barrier = {
        "snapshot_update_time_ms": 1_000,
        "order_cumulative_filled_qty": {"40": 0.0004},
    }

    with pytest.raises(RuntimeError, match="changed identity across"):
        engine._exchange_reconciliation_payload(
            (0.0004, 70_000.0, 1_000),
            [
                {
                    "id": 80,
                    "orderId": 40,
                    "qty": "0.0004",
                    "price": "70001",
                    "commission": "0",
                    "time": 1_000,
                    "side": "BUY",
                }
            ],
            barrier=barrier,
        )


def test_same_millisecond_fill_between_p1_and_p2_rejects_unstable_snapshot() -> None:
    engine = _stable_fetch_engine([])
    engine.inventory = SimpleNamespace(
        reconciliation_snapshot=lambda: {
            "snapshot_update_time_ms": 1_000,
            "order_cumulative_filled_qty": {},
        }
    )
    p1 = [
        {
            "symbol": "BTCUSDC",
            "positionSide": "BOTH",
            "positionAmt": "0",
            "entryPrice": "0",
            "updateTime": 2_000,
        }
    ]
    p2 = [
        {
            "symbol": "BTCUSDC",
            "positionSide": "BOTH",
            "positionAmt": "0.001",
            "entryPrice": "70000",
            "updateTime": 2_000,
        }
    ]
    engine.rest.get_position_risk.side_effect = (p1, p2)
    engine.rest.get_account_trades.return_value = [
        {
            "id": 91,
            "orderId": 41,
            "qty": "0.001",
            "price": "70000",
            "commission": "0",
            "time": 2_000,
            "buyer": True,
        }
    ]

    with pytest.raises(RuntimeError, match="snapshot drifted"):
        engine._stable_exchange_reconciliation_payload(max_attempts=1)

    assert engine.rest.get_position_risk.call_count == 2
    engine.rest.get_account_trades.assert_called_once_with(
        symbol="BTCUSDC",
        startTime=1_000,
        endTime=2_000,
        limit=1000,
    )


def test_fatal_orphan_callback_defers_exact_sync_until_fifo_unwinds() -> None:
    """A fatal callback must not wait on the sync lock held by a FIFO waiter."""

    reconciliation_lock_held = threading.Event()
    fatal_cancel_entered = threading.Event()
    release_fatal_cancel = threading.Event()
    callback_order: list[tuple[str, str]] = []
    sync_calls: list[tuple[bool, bool]] = []
    thread_errors: dict[str, BaseException] = {}

    class _BlockingCancelRest:
        def cancel_open_orders(self, **_kwargs) -> dict:
            fatal_cancel_entered.set()
            if not release_fatal_cancel.wait(timeout=2.0):
                raise TimeoutError("test did not release fatal cancel")
            return {}

    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine.rest = _BlockingCancelRest()
    engine._reconciliation_lock = threading.Lock()
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_fatal_reason = ""
    engine._runtime_fatal_error = None
    engine._runtime_reconciliation_required = False
    engine._runtime_reconciliation_pending = False
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 0
    engine._order_ref_lock = threading.RLock()
    engine._bid_cid = None
    engine._ask_cid = None
    engine._order_submit_fail_closed = False
    engine._running = True
    engine._record_exact_order_event = lambda *_args, **_kwargs: None

    def fake_sync_position(*, required: bool = False) -> bool:
        sync_calls.append((required, engine.orders.in_callback_dispatch()))
        if not engine._reconciliation_lock.acquire(timeout=1.0):
            raise TimeoutError("exact sync re-entered while callback FIFO was blocked")
        engine._reconciliation_lock.release()
        return True

    engine.sync_position = fake_sync_position

    def on_lifecycle(order, event_type, event) -> None:
        if order.orphan_adoption:
            engine._on_order_lifecycle_event(order, event_type, event)
        callback_order.append((order.client_order_id, event_type))

    engine.orders = OrderManager(
        on_lifecycle_event=on_lifecycle,
        allowed_symbols={"BTCUSDC"},
    )
    tracked_cid = engine.orders.create_order(
        "BTCUSDC",
        Side.BUY,
        100.0,
        0.001,
    )
    engine.orders.confirm_new(tracked_cid, 42)
    callback_order.clear()
    engine._bid_cid = tracked_cid

    def run(name: str, target) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors[name] = exc

    def reconcile_while_holding_lock() -> None:
        with engine._reconciliation_lock:
            reconciliation_lock_held.set()
            if not fatal_cancel_entered.wait(timeout=2.0):
                raise TimeoutError("orphan fatal callback did not start")
            engine.orders.reconcile_exchange_trade(
                exchange_order_id=42,
                trade_id=9002,
                symbol="BTCUSDC",
                side="BUY",
                quantity=0.0004,
                price=100.0,
                commission=0.0,
                commission_asset="USDC",
                cumulative_fill=0.0004,
                trade_time_ms=2_000,
            )

    def deliver_orphan() -> None:
        engine.orders.on_order_update(
            {
                "s": "BTCUSDC",
                "c": "mm_B_orphan_conflict",
                "S": "BUY",
                "o": "LIMIT",
                "X": "NEW",
                "i": 99,
                "p": "99.0",
                "q": "0.001",
                "T": 1_000,
            }
        )

    reconcile_thread = threading.Thread(
        target=run,
        args=("reconcile", reconcile_while_holding_lock),
    )
    orphan_thread = threading.Thread(
        target=run,
        args=("orphan", deliver_orphan),
    )
    reconcile_thread.start()
    assert reconciliation_lock_held.wait(timeout=1.0)
    orphan_thread.start()
    assert fatal_cancel_entered.wait(timeout=1.0)

    # The later REST fill is committed but cannot overtake the orphan callback.
    poll = threading.Event()
    committed = None
    for _ in range(200):
        committed = engine.orders.get_order(tracked_cid)
        if committed is not None and committed.filled_qty == pytest.approx(0.0004):
            break
        poll.wait(timeout=0.005)
    assert committed is not None
    assert committed.filled_qty == pytest.approx(0.0004)
    assert reconcile_thread.is_alive()
    assert callback_order == []
    assert sync_calls == []

    release_fatal_cancel.set()
    orphan_thread.join(timeout=2.0)
    reconcile_thread.join(timeout=2.0)

    assert not orphan_thread.is_alive()
    assert not reconcile_thread.is_alive()
    assert thread_errors == {}
    assert callback_order == [
        ("mm_B_orphan_conflict", "activate_unknown_prefix"),
        (tracked_cid, "partial_fill"),
    ]
    assert sync_calls == []
    assert engine._runtime_reconciliation_pending is True

    # The main-loop fatal check runs exact reconciliation outside the callback.
    with pytest.raises(RuntimeError, match="live runtime fatal latch"):
        engine.raise_if_runtime_fatal()
    assert sync_calls == [(True, False)]
    assert engine._runtime_reconciliation_pending is False


def test_shutdown_drain_retries_when_concurrent_fatal_advances_generation() -> None:
    first_sync_entered = threading.Event()
    concurrent_latch_done = threading.Event()
    sync_calls: list[int] = []
    thread_errors: dict[str, BaseException] = {}

    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine.rest = SimpleNamespace(cancel_open_orders=lambda **_kwargs: {})
    engine.signal = SimpleNamespace(stop=lambda: None)
    engine.orders = SimpleNamespace(
        fatal_status=lambda: {
            "latched": False,
            "reason": "",
            "reconciliation_required": False,
        },
        in_callback_dispatch=lambda: False,
    )
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_fatal_reason = "preexisting fatal"
    engine._runtime_fatal_error = RuntimeError("preexisting fatal")
    engine._runtime_reconciliation_required = True
    engine._runtime_reconciliation_pending = True
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 1
    engine._running = False
    engine._persist_fill_cooldown_checkpoint = lambda: None
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = None

    def fake_sync_position(*, required: bool = False) -> bool:
        assert required is True
        sync_calls.append(len(sync_calls) + 1)
        if len(sync_calls) == 1:
            first_sync_entered.set()
            if not concurrent_latch_done.wait(timeout=2.0):
                raise TimeoutError("concurrent fatal latch did not complete")
        return True

    engine.sync_position = fake_sync_position

    def run(name: str, target) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors[name] = exc

    shutdown_thread = threading.Thread(
        target=run,
        args=("shutdown", engine.stop),
    )
    shutdown_thread.start()
    assert first_sync_entered.wait(timeout=1.0)

    def advance_generation() -> None:
        engine.latch_runtime_fatal(
            reason="CONCURRENT_EXECUTION_UNCERTAINTY",
            error=RuntimeError("second fatal"),
            reconciliation_required=True,
        )
        concurrent_latch_done.set()

    latch_thread = threading.Thread(
        target=run,
        args=("latch", advance_generation),
    )
    latch_thread.start()
    latch_thread.join(timeout=2.0)
    shutdown_thread.join(timeout=2.0)

    assert not latch_thread.is_alive()
    assert not shutdown_thread.is_alive()
    assert thread_errors == {}
    assert sync_calls == [1, 2]
    assert engine._runtime_reconciliation_generation == 2
    assert engine._runtime_reconciliation_pending is False
    assert engine._runtime_reconciliation_inflight is False


def test_deferred_reconciliation_generation_drain_is_bounded() -> None:
    engine = object.__new__(MakerEngine)
    engine.orders = SimpleNamespace(in_callback_dispatch=lambda: False)
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_reconciliation_pending = True
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 1
    sync_calls: list[int] = []

    def continually_advance(*, required: bool = False) -> bool:
        assert required is True
        sync_calls.append(1)
        with engine._runtime_fatal_lock:
            engine._runtime_reconciliation_generation += 1
            engine._runtime_reconciliation_pending = True
        return True

    engine.sync_position = continually_advance

    assert engine._drain_deferred_runtime_reconciliation(
        max_stable_generations=3
    ) is False
    assert len(sync_calls) == 3
    assert engine._runtime_reconciliation_pending is True
    assert engine._runtime_reconciliation_inflight is False


def test_deferred_reconciliation_waits_for_cross_thread_callback_quiescence() -> None:
    engine = object.__new__(MakerEngine)
    engine.orders = SimpleNamespace(
        in_callback_dispatch=lambda: False,
        callback_dispatch_active=lambda: True,
    )
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_reconciliation_pending = True
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 1
    engine.sync_position = Mock(return_value=True)

    assert engine._drain_deferred_runtime_reconciliation() is False
    engine.sync_position.assert_not_called()
    assert engine._runtime_reconciliation_pending is True


def test_deferred_reconciliation_does_not_run_after_quiescence_timeout() -> None:
    engine = object.__new__(MakerEngine)
    engine.orders = SimpleNamespace(
        in_callback_dispatch=lambda: False,
        callback_dispatch_active=lambda: False,
    )
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_reconciliation_pending = True
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 1
    engine._runtime_reconciliation_quiescence_blocked = True
    engine.sync_position = Mock(return_value=True)

    assert engine._drain_deferred_runtime_reconciliation() is False
    engine.sync_position.assert_not_called()
    assert engine._runtime_reconciliation_pending is True


def test_stop_finally_drains_reconciliation_latched_during_writer_shutdown() -> None:
    writer_closed = threading.Event()
    sync_calls: list[bool] = []

    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine.rest = SimpleNamespace(cancel_open_orders=lambda **_kwargs: {})
    engine.signal = SimpleNamespace(stop=lambda: None)
    engine.orders = SimpleNamespace(
        fatal_status=lambda: {
            "latched": False,
            "reason": "",
            "reconciliation_required": False,
        },
        in_callback_dispatch=lambda: False,
    )
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_fatal_reason = "preexisting fatal"
    engine._runtime_fatal_error = RuntimeError("preexisting fatal")
    engine._runtime_reconciliation_required = False
    engine._runtime_reconciliation_pending = False
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 0
    engine._running = False
    engine._persist_fill_cooldown_checkpoint = lambda: None
    engine._exact_opportunity_tape_runtime = None
    engine._order_lifecycle_live_writer_v2_shutdown_timeout_s = 1.0

    def sync_position(*, required: bool = False) -> bool:
        assert required is True
        assert writer_closed.is_set()
        sync_calls.append(required)
        return True

    engine.sync_position = sync_position

    class _Writer:
        def close(self, *, drain_timeout_s: float) -> dict[str, object]:
            assert drain_timeout_s == pytest.approx(1.0)
            with engine._runtime_fatal_lock:
                engine._runtime_reconciliation_required = True
                engine._runtime_reconciliation_pending = True
                engine._runtime_reconciliation_generation += 1
            writer_closed.set()
            return {
                "rows_committed": 0,
                "drop_count": 0,
                "error_count": 0,
                "state": "closed",
                "worker_alive": False,
                "queue_depth": 0,
                "callbacks_enqueued": 0,
                "callbacks_processed": 0,
                "formal_collection_valid": True,
            }

    engine._order_lifecycle_live_writer_v2 = _Writer()

    engine.stop()

    assert sync_calls == [True]
    assert engine._runtime_reconciliation_pending is False


class _StoppedReconciliationRest:
    def __init__(
        self,
        *,
        orders: list[object] | None = None,
        positions: list[object] | None = None,
    ) -> None:
        self.orders = list(orders or [[], []])
        row = [
            {
                "symbol": "BTCUSDC",
                "positionSide": "BOTH",
                "positionAmt": "0.001",
                "entryPrice": "70000.0",
                "updateTime": 1_900_000_000_000,
            }
        ]
        self.positions = list(positions or [row, row])

    def get_orders(self, *, symbol: str) -> object:
        assert symbol == "BTCUSDC"
        return self.orders.pop(0)

    def get_position_risk(self, *, symbol: str) -> object:
        assert symbol == "BTCUSDC"
        return self.positions.pop(0)


def test_stopped_reconciliation_double_reads_zero_orders_and_stable_position(
    tmp_path: Path,
) -> None:
    output = tmp_path / "stopped-reconciliation.json"
    result = write_stopped_exchange_reconciliation(
        _StoppedReconciliationRest(),
        symbol="BTCUSDC",
        api_key="fixture-key",
        output_path=output,
        generated_utc="2026-08-29T00:00:00Z",
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["signed_read_sequence"] == [
        "/fapi/v1/openOrders",
        "/fapi/v2/positionRisk",
        "/fapi/v1/openOrders",
        "/fapi/v2/positionRisk",
    ]
    assert payload["position_rows"][0]["position_side"] == "BOTH"
    assert payload["account_key_sha256"] == hashlib.sha256(
        b"fixture-key"
    ).hexdigest()
    assert "position_lineage_sha256" not in payload
    assert set(result) == {"path", "canonical_sha256"}
    assert result["canonical_sha256"] == payload[
        "canonical_exchange_reconciliation_sha256"
    ]
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert output.stat().st_nlink == 1


def test_stopped_reconciliation_rejects_open_order_on_either_read() -> None:
    with pytest.raises(RuntimeError, match="zero exchange open orders"):
        build_stopped_exchange_reconciliation(
            _StoppedReconciliationRest(orders=[[{"orderId": 1}], []]),
            symbol="BTCUSDC",
            api_key="fixture-key",
        )
    with pytest.raises(RuntimeError, match="appeared"):
        build_stopped_exchange_reconciliation(
            _StoppedReconciliationRest(orders=[[], [{"orderId": 1}]]),
            symbol="BTCUSDC",
            api_key="fixture-key",
        )


def test_stopped_reconciliation_rejects_position_drift() -> None:
    first = [
        {
            "symbol": "BTCUSDC",
            "positionSide": "BOTH",
            "positionAmt": "0.001",
            "entryPrice": "70000.0",
            "updateTime": 1_900_000_000_000,
        }
    ]
    second = [dict(first[0], positionAmt="0.002")]
    with pytest.raises(RuntimeError, match="positionRisk drifted"):
        build_stopped_exchange_reconciliation(
            _StoppedReconciliationRest(positions=[first, second]),
            symbol="BTCUSDC",
            api_key="fixture-key",
        )
