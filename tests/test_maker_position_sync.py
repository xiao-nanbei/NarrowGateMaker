from __future__ import annotations

import hashlib
import json
import stat
import threading
import time
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


def _periodic_engine(fetch) -> MakerEngine:
    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine.order_gateway = SimpleNamespace()
    engine._reconciliation_lock = threading.Lock()
    engine._position_reconciliation_generation = 0
    engine._reconciliation_trade_identity_by_id = {}
    engine._periodic_reconciliation_state_lock = threading.Lock()
    engine._periodic_reconciliation_thread = None
    engine._periodic_reconciliation_completion = None
    engine._periodic_reconciliation_shutdown = False
    engine._periodic_reconciliation_required_sync_active = False
    engine._periodic_reconciliation_worker_epoch = 0
    engine._periodic_reconciliation_retry_requested = False
    engine._periodic_reconciliation_retry_not_before_s = 0.0
    engine._periodic_reconciliation_retry_attempt = 0
    engine._periodic_reconciliation_retry_reason = ""
    engine._periodic_reconciliation_scheduled = 0
    engine._periodic_reconciliation_completed = 0
    engine._periodic_reconciliation_committed = 0
    engine._periodic_reconciliation_stale_discarded = 0
    engine._periodic_reconciliation_fetch_errors = 0
    engine._periodic_reconciliation_commit_errors = 0
    engine._periodic_reconciliation_single_flight_coalesced = 0
    engine._periodic_reconciliation_last_error = ""
    engine._periodic_reconciliation_catastrophic_error = None
    engine._periodic_reconciliation_last_completed_monotonic_s = 0.0
    engine.background_reconciliation_client = object()
    barrier = {
        "snapshot_update_time_ms": 0,
        "order_cumulative_filled_qty": {},
        "local_order_cumulative_filled_qty": {},
        "retained_post_snapshot_fill_count": 0,
        "tracked_trade_identity_count": 0,
    }
    engine._test_reconciliation_barrier = barrier
    engine.inventory = SimpleNamespace(
        reconciliation_snapshot=lambda: json.loads(
            json.dumps(engine._test_reconciliation_barrier)
        ),
        sync_from_exchange=Mock(return_value={"ok": True}),
    )
    engine.orders = SimpleNamespace(
        reconcile_exchange_trade=Mock(return_value=True),
        fatal_status=Mock(
            return_value={"latched": False, "reconciliation_required": False}
        ),
    )
    engine._stable_exchange_reconciliation_payload = Mock(side_effect=fetch)
    return engine


def _wait_for_periodic_completion(engine: MakerEngine) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if engine.periodic_position_reconciliation_health()[
            "completion_pending"
        ]:
            return
        threading.Event().wait(0.005)
    raise TimeoutError("periodic reconciliation worker did not complete")


def _flat_seed_payload(update_time_ms: int) -> tuple:
    return (0.0, 0.0, update_time_ms, {}, (), (), {}, True)


def test_periodic_position_sync_is_single_flight_and_commits_only_when_polled() -> None:
    fetch_entered = threading.Event()
    release_fetch = threading.Event()

    def fetch(**kwargs) -> tuple:
        assert kwargs["barrier"]["snapshot_update_time_ms"] == 0
        assert kwargs["committed_trade_identities"] == {}
        assert kwargs["transport"] is engine.background_reconciliation_client
        fetch_entered.set()
        assert release_fetch.wait(timeout=2.0)
        return _flat_seed_payload(2_000)

    engine = _periodic_engine(fetch)

    assert engine.request_periodic_position_sync() is True
    assert fetch_entered.wait(timeout=1.0)
    assert engine.request_periodic_position_sync() is True
    assert engine._stable_exchange_reconciliation_payload.call_count == 1
    assert engine.inventory.sync_from_exchange.call_count == 0

    release_fetch.set()
    _wait_for_periodic_completion(engine)
    assert engine.inventory.sync_from_exchange.call_count == 0
    assert engine.poll_periodic_position_sync() == "committed"
    engine.inventory.sync_from_exchange.assert_called_once_with(
        0.0,
        0.0,
        snapshot_update_time_ms=2_000,
        order_cumulative_filled_qty={},
        included_trade_ids=(),
        included_trade_identities={},
    )
    health = engine.periodic_position_reconciliation_health()
    assert health["scheduled"] == 1
    assert health["completed"] == 1
    assert health["committed"] == 1
    assert health["single_flight_coalesced"] == 1


def test_periodic_position_sync_discards_stale_generation_and_refetches() -> None:
    engine = _periodic_engine(
        lambda **_kwargs: _flat_seed_payload(2_000)
    )

    assert engine.request_periodic_position_sync() is True
    _wait_for_periodic_completion(engine)
    engine._position_reconciliation_generation += 1

    assert engine.poll_periodic_position_sync() == "stale"
    engine.inventory.sync_from_exchange.assert_not_called()
    health = engine.periodic_position_reconciliation_health()
    assert health["stale_discarded"] == 1
    assert health["retry_pending"] is True

    with engine._periodic_reconciliation_state_lock:
        engine._periodic_reconciliation_retry_not_before_s = 0.0
    assert engine.maintain_periodic_position_sync() == "idle"
    _wait_for_periodic_completion(engine)
    assert engine._stable_exchange_reconciliation_payload.call_count == 2


def test_required_position_sync_drains_worker_then_uses_fresh_sync_proof() -> None:
    fetch_entered = threading.Event()
    release_fetch = threading.Event()
    calls = 0
    call_kwargs: list[dict] = []

    def fetch(**kwargs) -> tuple:
        nonlocal calls
        calls += 1
        call_kwargs.append(kwargs)
        if calls == 1:
            fetch_entered.set()
            assert release_fetch.wait(timeout=2.0)
            return _flat_seed_payload(2_000)
        return _flat_seed_payload(3_000)

    engine = _periodic_engine(fetch)
    result: list[bool] = []
    errors: list[BaseException] = []

    assert engine.request_periodic_position_sync() is True
    assert fetch_entered.wait(timeout=1.0)

    def required_sync() -> None:
        try:
            result.append(engine.sync_position(required=True))
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    sync_thread = threading.Thread(target=required_sync)
    sync_thread.start()
    threading.Event().wait(0.02)
    assert sync_thread.is_alive()
    assert engine.inventory.sync_from_exchange.call_count == 0

    release_fetch.set()
    sync_thread.join(timeout=2.0)
    assert not sync_thread.is_alive()
    assert errors == []
    assert result == [True]
    assert calls == 2
    assert call_kwargs[0]["transport"] is engine.background_reconciliation_client
    assert "transport" not in call_kwargs[1]
    engine.inventory.sync_from_exchange.assert_called_once_with(
        0.0,
        0.0,
        snapshot_update_time_ms=3_000,
        order_cumulative_filled_qty={},
        included_trade_ids=(),
        included_trade_identities={},
    )
    health = engine.periodic_position_reconciliation_health()
    assert health["completion_pending"] is False
    assert health["worker_alive"] is False
    assert health["committed"] == 0


def test_periodic_position_sync_shutdown_drains_and_discards_completion() -> None:
    fetch_entered = threading.Event()
    release_fetch = threading.Event()

    def fetch(**_kwargs) -> tuple:
        fetch_entered.set()
        assert release_fetch.wait(timeout=2.0)
        return _flat_seed_payload(2_000)

    engine = _periodic_engine(fetch)
    shutdown_complete = threading.Event()

    assert engine.request_periodic_position_sync() is True
    assert fetch_entered.wait(timeout=1.0)

    def drain() -> None:
        engine._quiesce_periodic_position_reconciliation(permanent=True)
        shutdown_complete.set()

    drain_thread = threading.Thread(target=drain)
    drain_thread.start()
    assert not shutdown_complete.wait(timeout=0.02)
    release_fetch.set()
    assert shutdown_complete.wait(timeout=1.0)
    drain_thread.join(timeout=1.0)

    health = engine.periodic_position_reconciliation_health()
    assert health["shutdown"] is True
    assert health["worker_alive"] is False
    assert health["completion_pending"] is False
    assert engine.inventory.sync_from_exchange.call_count == 0
    assert engine.request_periodic_position_sync() is False


def test_engine_stop_cancels_exchange_before_waiting_for_cold_worker() -> None:
    fetch_entered = threading.Event()
    release_fetch = threading.Event()
    cancel_called = threading.Event()
    events: list[str] = []

    def fetch(**_kwargs) -> tuple:
        fetch_entered.set()
        assert release_fetch.wait(timeout=2.0)
        return _flat_seed_payload(2_000)

    engine = _periodic_engine(fetch)
    engine.orders.get_active_orders = lambda: []
    engine._running = True
    engine._clear_all_replace_terminal_continuations = lambda **_kwargs: None
    engine._persist_fill_cooldown_checkpoint = lambda: None
    engine.signal = SimpleNamespace(stop=lambda: None)
    engine._execution_state_uncertain = lambda: False

    def cancel_all() -> bool:
        events.append("cancel_all")
        cancel_called.set()
        return True

    engine._cancel_all_orders = cancel_all
    engine.sync_position = lambda **_kwargs: events.append("required_sync") or True
    engine._runtime_evidence_writer = None
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = None
    engine._drain_deferred_runtime_reconciliation = lambda: True
    engine.close_fill_cooldown_checkpoint_store = lambda: None
    stop_errors: list[BaseException] = []

    assert engine.request_periodic_position_sync() is True
    assert fetch_entered.wait(timeout=1.0)

    def stop() -> None:
        try:
            engine.stop()
        except BaseException as exc:  # pragma: no cover - asserted below
            stop_errors.append(exc)

    stop_thread = threading.Thread(target=stop)
    stop_thread.start()
    assert cancel_called.wait(timeout=0.2)
    assert stop_thread.is_alive()
    assert events == ["cancel_all", "required_sync"]

    release_fetch.set()
    stop_thread.join(timeout=1.0)
    assert not stop_thread.is_alive()
    assert stop_errors == []


def test_periodic_drain_timeout_poison_prevents_late_completion_resurrection() -> None:
    fetch_entered = threading.Event()
    release_fetch = threading.Event()

    def fetch(**_kwargs) -> tuple:
        fetch_entered.set()
        assert release_fetch.wait(timeout=2.0)
        return _flat_seed_payload(2_000)

    engine = _periodic_engine(fetch)
    assert engine.request_periodic_position_sync() is True
    assert fetch_entered.wait(timeout=1.0)

    with pytest.raises(RuntimeError, match="worker did not drain"):
        engine._quiesce_periodic_position_reconciliation(
            permanent=False,
            timeout_s=0.001,
        )

    release_fetch.set()
    deadline = time.monotonic() + 1.0
    while engine.periodic_position_reconciliation_health()["worker_alive"]:
        assert time.monotonic() < deadline
        threading.Event().wait(0.005)
    health = engine.periodic_position_reconciliation_health()
    assert health["completion_pending"] is False
    assert engine._periodic_reconciliation_required_sync_active is True
    assert engine.request_periodic_position_sync() is False


def test_periodic_identity_lag_retry_is_bounded_and_then_fails_closed() -> None:
    identity_lag = RuntimeError(
        "exchange snapshot omitted the identity cursor for a locally "
        "applied fill at or before its update time"
    )
    engine = _periodic_engine(
        lambda **_kwargs: (0.0, 0.0, 2_000, {}, (), (), {}, False)
    )
    engine._test_reconciliation_barrier["snapshot_update_time_ms"] = 1_000
    engine.inventory.sync_from_exchange.side_effect = identity_lag
    engine.latch_runtime_fatal = Mock()
    outcomes: list[str] = []

    assert engine.request_periodic_position_sync() is True
    for attempt in range(
        len(maker_engine_module._POSITION_RECONCILIATION_IDENTITY_LAG_BACKOFF_S)
        + 1
    ):
        _wait_for_periodic_completion(engine)
        outcomes.append(engine.poll_periodic_position_sync())
        if attempt < len(
            maker_engine_module._POSITION_RECONCILIATION_IDENTITY_LAG_BACKOFF_S
        ):
            with engine._periodic_reconciliation_state_lock:
                engine._periodic_reconciliation_retry_not_before_s = 0.0
            assert engine.maintain_periodic_position_sync() == "idle"

    assert outcomes == ["retry"] * 5 + ["retry_exhausted"]
    assert engine._stable_exchange_reconciliation_payload.call_count == 6
    assert engine.inventory.sync_from_exchange.call_count == 6
    engine.latch_runtime_fatal.assert_called_once_with(
        reason="EXACT_EXECUTION_RECONCILIATION_FAILED",
        error=identity_lag,
        reconciliation_required=True,
    )
    health = engine.periodic_position_reconciliation_health()
    assert health["retry_pending"] is False
    assert health["retry_attempt"] == 5


def test_periodic_position_sync_fetch_error_is_visible_without_ledger_mutation() -> None:
    engine = _periodic_engine(
        lambda **_kwargs: (_ for _ in ()).throw(TimeoutError("cold timeout"))
    )

    assert engine.request_periodic_position_sync() is True
    _wait_for_periodic_completion(engine)
    assert engine.poll_periodic_position_sync() == "fetch_error"
    engine.inventory.sync_from_exchange.assert_not_called()
    health = engine.periodic_position_reconciliation_health()
    assert health["fetch_errors"] == 1
    assert health["last_error"] == "TimeoutError:cold timeout"


def test_periodic_worker_catastrophic_error_stops_runtime() -> None:
    engine = _periodic_engine(
        lambda **_kwargs: (_ for _ in ()).throw(MemoryError("exhausted"))
    )
    engine.latch_runtime_fatal = Mock()

    assert engine.request_periodic_position_sync() is True
    _wait_for_periodic_completion(engine)
    assert engine.poll_periodic_position_sync() == "worker_fatal"
    engine.inventory.sync_from_exchange.assert_not_called()
    engine.latch_runtime_fatal.assert_called_once()
    assert engine.latch_runtime_fatal.call_args.kwargs["reason"] == (
        "PERIODIC_RECONCILIATION_WORKER_FATAL"
    )
    assert engine.latch_runtime_fatal.call_args.kwargs[
        "reconciliation_required"
    ] is False


def test_periodic_stable_fetch_honors_expired_total_deadline() -> None:
    engine = _stable_fetch_engine([])

    with pytest.raises(TimeoutError, match="fetch deadline exceeded"):
        engine._stable_exchange_reconciliation_payload(
            deadline_monotonic_s=time.monotonic() - 1.0,
        )

    engine.rest.get_position_risk.assert_not_called()
    engine.rest.get_account_trades.assert_not_called()


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
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
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


_TRADE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000


def _account_trade(trade_id: int, time_ms: int, **overrides) -> dict:
    return {
        "symbol": "BTCUSDC",
        "id": trade_id,
        "orderId": 40,
        "time": time_ms,
        "qty": "0.001",
        "price": "70000",
        "commission": "0",
        "side": "BUY",
        **overrides,
    }


def _account_trade_pages(trades: list[dict]):
    """Model the two mutually exclusive production endpoint query forms."""

    def fetch(**request):
        assert request["symbol"] == "BTCUSDC"
        assert request["limit"] == 1000
        if "fromId" in request:
            assert "startTime" not in request and "endTime" not in request
            selected = [row for row in trades if row["id"] >= request["fromId"]]
            return sorted(selected, key=lambda row: row["id"])[:1000]
        assert 0 <= request["endTime"] - request["startTime"] < _TRADE_WINDOW_MS
        selected = [
            row
            for row in trades
            if request["startTime"] <= row["time"] <= request["endTime"]
        ]
        return sorted(selected, key=lambda row: (row["time"], row["id"]))[:1000]

    return fetch


@pytest.mark.parametrize("length_ms", (1, _TRADE_WINDOW_MS, _TRADE_WINDOW_MS + 1,
                                      2 * _TRADE_WINDOW_MS + 3))
def test_account_trade_empty_windows_cover_complete_closed_interval(length_ms) -> None:
    engine = _stable_fetch_engine([])
    start = 1_000
    end = start + length_ms - 1

    assert engine._account_trades_through_snapshot(
        previous_snapshot_update_time_ms=start,
        snapshot_update_time_ms=end,
    ) == []

    expected = [
        {
            "symbol": "BTCUSDC",
            "startTime": window_start,
            "endTime": min(window_start + _TRADE_WINDOW_MS - 1, end),
            "limit": 1000,
        }
        for window_start in range(start, end + 1, _TRADE_WINDOW_MS)
    ]
    assert [call.kwargs for call in engine.rest.get_account_trades.call_args_list] == expected


def test_account_trade_windows_include_all_boundary_fills_and_preserve_seed_cursor() -> None:
    start = 1_000
    end = start + 2 * _TRADE_WINDOW_MS
    times = (start, start + _TRADE_WINDOW_MS - 1, start + _TRADE_WINDOW_MS,
             end - 1, end)
    trades = [_account_trade(index + 80, timestamp) for index, timestamp in enumerate(times)]
    # The endpoint may repeat a row; only the existing identity owner dedupes it.
    trades.insert(2, dict(trades[1]))
    engine = _stable_fetch_engine([
        {"symbol": "BTCUSDC", "positionAmt": "0.005", "entryPrice": "70000",
         "updateTime": end}
    ])
    engine.inventory.reconciliation_snapshot = lambda: {
        "snapshot_update_time_ms": start,
        "order_cumulative_filled_qty": {"40": 0.001},
    }
    engine._reconciliation_trade_identity_by_id = {
        "80": _producer_identity(
            order_id="40", side="BUY", quantity=0.001, price=70_000.0,
            commission=0.0, commission_asset="", trade_time_ms=start, cumulative=0.001,
        )
    }
    pages = _account_trade_pages(trades)
    engine.rest.get_account_trades.side_effect = lambda **request: list(reversed(pages(**request)))

    payload = engine._stable_exchange_reconciliation_payload(max_attempts=1)

    assert payload[2] == end
    assert payload[3] == {"40": pytest.approx(0.005)}
    assert payload[4] == ("80", "81", "82", "83", "84")
    assert [row["trade_id"] for row in payload[5]] == [81, 82, 83, 84]
    assert payload[7] is False
    assert engine.rest.get_account_trades.call_count == 3
    assert engine.rest.get_position_risk.call_count == 2
    # Fetching does not commit producer identities before inventory acceptance.
    assert set(engine._reconciliation_trade_identity_by_id) == {"80"}


def test_account_trade_query_handles_old_idle_position_then_new_fills() -> None:
    start = 1_788_948_111_041  # Retained September positionRisk.updateTime.
    end = start + 44 * 24 * 60 * 60 * 1000
    trades = [_account_trade(index + 1, end - 39 + index) for index in range(40)]
    engine = _stable_fetch_engine([])
    engine.rest.get_account_trades.side_effect = _account_trade_pages(trades)

    assert engine._account_trades_through_snapshot(
        previous_snapshot_update_time_ms=start,
        snapshot_update_time_ms=end,
    ) == trades
    assert engine.rest.get_account_trades.call_count == 7
    assert engine.rest.get_account_trades.call_args_list[0].kwargs["startTime"] == start
    assert engine.rest.get_account_trades.call_args_list[-1].kwargs["endTime"] == end


def test_shutdown_exact_sync_covers_long_idle_gap_before_committing_barrier() -> None:
    start = 1_788_948_111_041
    end = start + 44 * 24 * 60 * 60 * 1000
    trades = [_account_trade(index + 1, end - 39 + index) for index in range(40)]
    engine = _stable_fetch_engine([
        {"symbol": "BTCUSDC", "positionAmt": "0.040", "entryPrice": "70000",
         "updateTime": end}
    ])
    events = []
    engine._reconciliation_lock = threading.Lock()
    engine.inventory.reconciliation_snapshot = lambda: {
        "snapshot_update_time_ms": start, "order_cumulative_filled_qty": {},
    }
    engine.inventory.sync_from_exchange = Mock(side_effect=lambda *args, **kwargs: events.append("barrier"))
    engine.orders = SimpleNamespace(
        get_active_orders=lambda: [],
        reconcile_exchange_trade=Mock(side_effect=lambda **kwargs: events.append(kwargs["trade_id"])),
        fatal_status=lambda: {"latched": False, "reconciliation_required": False},
    )
    engine.rest.get_account_trades.side_effect = _account_trade_pages(trades)
    engine.signal = SimpleNamespace(stop=lambda: None)
    engine._running = True
    engine._clear_all_replace_terminal_continuations = lambda **kwargs: None
    engine._persist_fill_cooldown_checkpoint = lambda: None
    engine._execution_state_uncertain = lambda: False
    engine._cancel_all_orders = lambda: events.append("cancel") or True
    engine._drain_deferred_runtime_reconciliation = lambda: True
    engine.close_fill_cooldown_checkpoint_store = lambda: None
    engine.latch_runtime_fatal = Mock()

    engine.stop()

    assert events == ["cancel", *range(1, 41), "barrier"]
    assert engine.rest.get_account_trades.call_count == 7
    engine.inventory.sync_from_exchange.assert_called_once()
    committed = engine.inventory.sync_from_exchange.call_args
    assert committed.args == (0.04, 70_000.0)
    assert committed.kwargs["snapshot_update_time_ms"] == end
    assert committed.kwargs["order_cumulative_filled_qty"] == {"40": pytest.approx(0.04)}
    assert committed.kwargs["included_trade_ids"] == tuple(str(index) for index in range(1, 41))
    assert set(engine._reconciliation_trade_identity_by_id) == {"40"}
    assert engine.is_running is False
    engine.latch_runtime_fatal.assert_not_called()


def test_failed_later_account_trade_window_does_not_install_partial_barrier() -> None:
    start = 1_000
    end = start + _TRADE_WINDOW_MS
    engine = _stable_fetch_engine([
        {"symbol": "BTCUSDC", "positionAmt": "0.001", "entryPrice": "70000",
         "updateTime": end}
    ])
    engine._reconciliation_lock = threading.Lock()
    engine.inventory.reconciliation_snapshot = lambda: {
        "snapshot_update_time_ms": start, "order_cumulative_filled_qty": {},
    }
    engine.inventory.sync_from_exchange = Mock()
    engine.orders = SimpleNamespace(
        reconcile_exchange_trade=Mock(),
        fatal_status=lambda: {"latched": False, "reconciliation_required": False},
    )
    engine.rest.get_account_trades.side_effect = [
        [_account_trade(1, start)], TimeoutError("later-window request failed"),
    ]
    with pytest.raises(RuntimeError, match="required position sync failed"):
        engine.sync_position(required=True)
    engine.orders.reconcile_exchange_trade.assert_not_called()
    engine.inventory.sync_from_exchange.assert_not_called()
    assert engine._reconciliation_trade_identity_by_id == {}


@pytest.mark.parametrize("count", (1000, 1001, 2005))
def test_account_trade_full_pages_preserve_same_millisecond_fills(count) -> None:
    engine = _stable_fetch_engine([])
    trades = [_account_trade(index + 1, 2_000) for index in range(count)]
    engine.rest.get_account_trades.side_effect = _account_trade_pages(trades)

    assert engine._account_trades_through_snapshot(
        previous_snapshot_update_time_ms=1_000,
        snapshot_update_time_ms=2_000,
    ) == trades
    requests = [call.kwargs for call in engine.rest.get_account_trades.call_args_list]
    assert requests[0] == {
        "symbol": "BTCUSDC", "startTime": 1_000, "endTime": 2_000, "limit": 1000,
    }
    assert requests[1:] == [
        {"symbol": "BTCUSDC", "fromId": cursor, "limit": 1000}
        for cursor in range(1001, count + 2, 1000)
    ]


def test_account_trade_id_pagination_crossing_window_requeries_next_window() -> None:
    start = 1_000
    boundary = start + _TRADE_WINDOW_MS - 1
    end = boundary + 100
    first = [_account_trade(index + 1, start + 1) for index in range(1000)]
    trades = first + [
        _account_trade(1001, boundary),
        _account_trade(1002, boundary + 1),
        _account_trade(1003, end),
        _account_trade(1004, end + 1),
    ]
    engine = _stable_fetch_engine([])
    engine.rest.get_account_trades.side_effect = _account_trade_pages(trades)

    assert engine._account_trades_through_snapshot(
        previous_snapshot_update_time_ms=start,
        snapshot_update_time_ms=end,
    ) == trades[:-1]
    assert [call.kwargs for call in engine.rest.get_account_trades.call_args_list] == [
        {"symbol": "BTCUSDC", "startTime": start, "endTime": boundary, "limit": 1000},
        {"symbol": "BTCUSDC", "fromId": 1001, "limit": 1000},
        {"symbol": "BTCUSDC", "startTime": boundary + 1, "endTime": end, "limit": 1000},
    ]


def test_account_trade_id_pagination_filters_fills_after_snapshot() -> None:
    engine = _stable_fetch_engine([])
    trades = [_account_trade(index + 1, 2_000) for index in range(1000)]
    engine.rest.get_account_trades.side_effect = _account_trade_pages(
        trades + [_account_trade(1001, 2_001)]
    )

    assert engine._account_trades_through_snapshot(
        previous_snapshot_update_time_ms=1_000,
        snapshot_update_time_ms=2_000,
    ) == trades


def test_account_trade_query_rejects_snapshot_clock_regression_before_fetch() -> None:
    engine = _stable_fetch_engine([])
    with pytest.raises(RuntimeError, match="snapshot clock regressed"):
        engine._account_trades_through_snapshot(
            previous_snapshot_update_time_ms=2_000,
            snapshot_update_time_ms=1_000,
        )
    engine.rest.get_account_trades.assert_not_called()


def test_account_trade_pagination_rejects_regressed_trade_cursor() -> None:
    engine = _stable_fetch_engine([])
    first = [_account_trade(index + 1, 2_000) for index in range(1000)]
    engine.rest.get_account_trades.side_effect = [first, [_account_trade(1000, 2_000)]]
    with pytest.raises(RuntimeError, match="pagination cursor regressed"):
        engine._account_trades_through_snapshot(
            previous_snapshot_update_time_ms=1_000,
            snapshot_update_time_ms=2_000,
        )
    assert engine.rest.get_account_trades.call_count == 2


def test_account_trade_query_rejects_row_preceding_next_window() -> None:
    engine = _stable_fetch_engine([])
    start = 1_000
    engine.rest.get_account_trades.side_effect = [[], [_account_trade(1, start)]]
    with pytest.raises(RuntimeError, match="preceded requested interval"):
        engine._account_trades_through_snapshot(
            previous_snapshot_update_time_ms=start,
            snapshot_update_time_ms=start + _TRADE_WINDOW_MS,
        )


@pytest.mark.parametrize("full_page", (False, True))
@pytest.mark.parametrize("stop_kind", ("cancel", "deadline"))
def test_account_trade_query_checks_stop_between_windows_and_pages(
    monkeypatch, full_page, stop_kind,
) -> None:
    engine = _stable_fetch_engine([])
    engine.rest.get_account_trades.return_value = (
        [_account_trade(index + 1, 1_001) for index in range(1000)] if full_page else []
    )
    stop_options = {}
    if stop_kind == "cancel":
        stop_options["cancel_requested"] = lambda: engine.rest.get_account_trades.call_count > 0
        expected_exception = RuntimeError
        expected_message = "was canceled"
    else:
        monkeypatch.setattr(
            maker_engine_module.time, "monotonic",
            lambda: 11.0 if engine.rest.get_account_trades.call_count else 9.0,
        )
        stop_options["deadline_monotonic_s"] = 10.0
        expected_exception = TimeoutError
        expected_message = "deadline exceeded"
    with pytest.raises(expected_exception, match=expected_message):
        engine._account_trades_through_snapshot(
            previous_snapshot_update_time_ms=1_000,
            snapshot_update_time_ms=1_000 + _TRADE_WINDOW_MS,
            **stop_options,
        )
    assert engine.rest.get_account_trades.call_count == 1


def test_account_trade_query_retains_global_request_bound() -> None:
    engine = _stable_fetch_engine([])

    def full_page(**request):
        start_id = request.get("fromId", 1)
        return [_account_trade(trade_id, 2_000) for trade_id in range(start_id, start_id + 1000)]

    engine.rest.get_account_trades.side_effect = full_page
    with pytest.raises(RuntimeError, match="exceeded 100 pages"):
        engine._account_trades_through_snapshot(
            previous_snapshot_update_time_ms=1_000,
            snapshot_update_time_ms=2_000,
        )
    assert engine.rest.get_account_trades.call_count == 100


def test_account_trade_windowing_does_not_hide_conflicting_duplicates() -> None:
    engine = _stable_fetch_engine([
        {"symbol": "BTCUSDC", "positionAmt": "0.001", "entryPrice": "70000",
         "updateTime": 1_000 + _TRADE_WINDOW_MS}
    ])
    engine.inventory.reconciliation_snapshot = lambda: {
        "snapshot_update_time_ms": 1_000, "order_cumulative_filled_qty": {},
    }
    engine.rest.get_account_trades.side_effect = [
        [], [_account_trade(1, 1_000 + _TRADE_WINDOW_MS),
             _account_trade(1, 1_000 + _TRADE_WINDOW_MS, price="70001")],
    ]
    with pytest.raises(RuntimeError, match="duplicate account trade ID changed identity"):
        engine._stable_exchange_reconciliation_payload(max_attempts=1)
    assert engine._reconciliation_trade_identity_by_id == {}


@pytest.mark.parametrize("invalid_qty", ("0", "-1", "nan", "inf"))
def test_account_trade_windowing_keeps_exact_quantity_validation(invalid_qty) -> None:
    end = 1_000 + _TRADE_WINDOW_MS
    engine = _stable_fetch_engine([
        {"symbol": "BTCUSDC", "positionAmt": "0.001", "entryPrice": "70000",
         "updateTime": end}
    ])
    engine.inventory.reconciliation_snapshot = lambda: {
        "snapshot_update_time_ms": 1_000, "order_cumulative_filled_qty": {},
    }
    engine.rest.get_account_trades.side_effect = [[], [_account_trade(1, end, qty=invalid_qty)]]
    with pytest.raises(RuntimeError, match="economics/time was invalid"):
        engine._stable_exchange_reconciliation_payload(max_attempts=1)
    assert engine._reconciliation_trade_identity_by_id == {}


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


def test_bnb_account_trade_binds_raw_fee_and_same_recovery_market_valuation() -> None:
    from live.commission import BnbCommissionValuator

    engine = _stable_fetch_engine([])
    client = SimpleNamespace(index_price_klines=Mock(return_value=[
        [60_000, "700", "700", "700", "700", "0", 119_999],
    ]))
    engine._commission_valuator = BnbCommissionValuator(client)
    trade = dict(id=90, orderId=50, qty="0.001", price="70000",
                 commission="0.00004", commissionAsset="BNB", time=120_234, side="SELL")
    barrier = dict(snapshot_update_time_ms=100_000, order_cumulative_filled_qty={})
    payload = engine._exchange_reconciliation_payload((0, 0, 120_234), [trade], barrier=barrier)
    identity = payload[6]["90"]
    assert identity["commission"] == pytest.approx(0.028)
    assert identity["raw_commission"] == 0.00004
    assert identity["raw_commission_asset"] == "BNB"
    assert identity["commission_valuation"]["exact_exchange_conversion"] is False
    assert payload[5][0]["commission_asset"] == "BNB"
    engine._reconciliation_trade_identity_by_id = payload[6]
    next_barrier = dict(snapshot_update_time_ms=120_234, order_cumulative_filled_qty={"50": 0.001})
    repeated = engine._exchange_reconciliation_payload((0, 0, 120_234), [trade], barrier=next_barrier)
    assert repeated[5] == ()
    assert repeated[6]["90"] == identity
    client.index_price_klines.assert_called_once()
    with pytest.raises(RuntimeError, match="changed identity"):
        engine._exchange_reconciliation_payload(
            (0, 0, 120_234), [dict(trade, commission="0.00005")], barrier=next_barrier,
        )


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


def test_reconciliation_retains_only_next_inclusive_time_boundary() -> None:
    engine = object.__new__(MakerEngine)
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
        ),
        "81": _producer_identity(
            order_id="41",
            side="BUY",
            quantity=0.0003,
            price=70_001.0,
            commission=0.0,
            commission_asset="",
            trade_time_ms=2_000,
            cumulative=0.0003,
        ),
    }
    newest = _producer_identity(
        order_id="42",
        side="SELL",
        quantity=0.0002,
        price=70_002.0,
        commission=0.0,
        commission_asset="",
        trade_time_ms=2_000,
        cumulative=0.0002,
    )

    engine._install_reconciliation_trade_identity_boundary(
        snapshot_update_time_ms=2_000,
        trade_identities={"82": newest},
    )

    assert set(engine._reconciliation_trade_identity_by_id) == {"81", "82"}
    assert engine._reconciliation_trade_identity_retained_count == 2
    assert engine._reconciliation_trade_identity_retained_bytes > 0


def test_reconciliation_identity_boundary_rejects_future_identity() -> None:
    engine = object.__new__(MakerEngine)
    engine._reconciliation_trade_identity_by_id = {}
    future = _producer_identity(
        order_id="42",
        side="SELL",
        quantity=0.0002,
        price=70_002.0,
        commission=0.0,
        commission_asset="",
        trade_time_ms=2_001,
        cumulative=0.0002,
    )

    with pytest.raises(RuntimeError, match="exceeds committed barrier"):
        engine._install_reconciliation_trade_identity_boundary(
            snapshot_update_time_ms=2_000,
            trade_identities={"82": future},
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
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
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
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
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


def test_stop_waits_for_order_callbacks_before_sync_and_evidence_close() -> None:
    drain_entered = threading.Event()
    release_callback = threading.Event()
    stop_completed = threading.Event()
    stop_errors: list[BaseException] = []
    sequence: list[str] = []

    class _OrderGateway:
        def drain_async_order_completions(self) -> None:
            sequence.append("callback-drain")
            drain_entered.set()
            assert release_callback.wait(timeout=2.0)

    class _EvidenceRuntime:
        def barrier(self, *, timeout_s: float) -> None:
            assert timeout_s == pytest.approx(10.0)
            sequence.append("evidence-barrier")

    class _LifecycleWriter:
        def close(self, *, drain_timeout_s: float) -> dict[str, object]:
            assert drain_timeout_s == pytest.approx(1.0)
            sequence.append("lifecycle-close")
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

    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine.orders = SimpleNamespace(get_active_orders=lambda: [])
    engine.order_gateway = _OrderGateway()
    engine.signal = SimpleNamespace(stop=lambda: None)
    engine._running = True
    engine._clear_all_replace_terminal_continuations = lambda **_kwargs: None
    engine._persist_fill_cooldown_checkpoint = lambda: None
    engine._execution_state_uncertain = lambda: False
    engine._cancel_all_orders = lambda: sequence.append("cancel-all") or True
    engine.sync_position = (
        lambda *, required=False: sequence.append("sync-position") or True
    )
    engine._drain_deferred_runtime_reconciliation = lambda: True
    engine.close_fill_cooldown_checkpoint_store = lambda: None
    engine._runtime_evidence_writer = _EvidenceRuntime()
    engine._order_lifecycle_live_writer_v2 = _LifecycleWriter()
    engine._order_lifecycle_live_writer_v2_shutdown_timeout_s = 1.0
    engine._exact_opportunity_tape_runtime = None

    def stop_engine() -> None:
        try:
            engine.stop()
        except BaseException as exc:
            stop_errors.append(exc)
        finally:
            stop_completed.set()

    stop_thread = threading.Thread(target=stop_engine)
    stop_thread.start()
    assert drain_entered.wait(timeout=1.0)
    time.sleep(0.01)
    assert not stop_completed.is_set()
    assert sequence == ["cancel-all", "callback-drain"]

    release_callback.set()
    stop_thread.join(timeout=2.0)
    assert not stop_thread.is_alive()
    assert stop_errors == []
    assert sequence == [
        "cancel-all",
        "callback-drain",
        "sync-position",
        "evidence-barrier",
        "lifecycle-close",
    ]


def test_stop_finally_drains_reconciliation_latched_during_writer_shutdown() -> None:
    writer_closed = threading.Event()
    sync_calls: list[bool] = []

    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(symbol="BTCUSDC")
    engine.rest = SimpleNamespace(cancel_open_orders=lambda **_kwargs: {})
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
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
