import threading
import time
from concurrent.futures import Future
from types import SimpleNamespace

import pytest

from live.binance_usdm_transport import BinanceUsdMOrderGateway
from live.config import Config
from live.main import resolve_live_shutdown_exit
from strategy.inventory_manager import PositionState
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, OrderOwnershipStatus, OrderState, Side


def test_live_transport_roles_default_to_legacy_and_split_independently() -> None:
    engine = object.__new__(MakerEngine)
    legacy = object()
    order_gateway = object()
    reconciliation_client = object()
    engine.rest = legacy

    assert engine._order_transport() is legacy
    assert engine._reconciliation_transport() is legacy

    engine.order_gateway = order_gateway
    engine.reconciliation_client = reconciliation_client
    assert engine._order_transport() is order_gateway
    assert engine._reconciliation_transport() is reconciliation_client

    engine.order_gateway = None
    with pytest.raises(RuntimeError, match="order_gateway is unavailable"):
        engine._order_transport()

    engine.order_gateway = order_gateway
    engine.reconciliation_client = None
    with pytest.raises(RuntimeError, match="reconciliation_client is unavailable"):
        engine._reconciliation_transport()

    event_source = object()
    engine.set_event_source(event_source)
    assert engine._active_event_source() is event_source
    assert engine._ws_handler is event_source


class _RestClient:
    def __init__(self, response=None, error=None):
        self.response = response or {"orderId": 7, "status": "NEW"}
        self.error = error
        self.calls = []
        self.cancel_calls = []

    def new_order(self, **params):
        self.calls.append(params)
        if self.error is not None:
            raise self.error
        response = {
            "orderId": 7,
            "status": "NEW",
            "clientOrderId": params["newClientOrderId"],
            "symbol": params["symbol"],
            "side": params["side"],
            "origQty": params["quantity"],
            "executedQty": "0",
        }
        response.update(self.response)
        return response

    def cancel_open_orders(self, **params):
        self.cancel_calls.append(params)
        return []


class _QueryClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def query_order(self, **params):
        self.calls.append(params)
        return self.response


class _AuthoritativeExchangeError(RuntimeError):
    exchange_response_authoritative = True

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


class _PreDispatchUnavailable(ConnectionError):
    may_have_been_dispatched = False
    requires_reconciliation = False


class _UnknownCloseThenNotFoundRest:
    def __init__(self) -> None:
        self.calls = []

    def new_order(self, **params):
        self.calls.append(params)
        raise TimeoutError("submit response lost")

    def query_order(self, **_params):
        raise _AuthoritativeExchangeError(-2013, "Order does not exist")


class _ControllableAsyncGateway:
    supports_narrowgate_request_metadata = True
    async_order_lanes_enabled = True

    def __init__(self) -> None:
        self.new_calls = []
        self.cancel_calls = []
        self.new_future: Future = Future()
        self.cancel_future: Future = Future()

    def new_order_async(self, **params):
        self.new_calls.append(params)
        return self.new_future

    def cancel_order_async(self, **params):
        self.cancel_calls.append(params)
        return self.cancel_future


def _result_for(params, *, status: str = "NEW", order_id: int = 123):
    return {
        "orderId": order_id,
        "status": status,
        "clientOrderId": (
            params.get("newClientOrderId") or params.get("origClientOrderId")
        ),
        "symbol": params["symbol"],
        "side": params.get("side") or params.get("_narrowgate_order_side"),
        "origQty": params.get("quantity", "0.001"),
        "executedQty": "0",
    }


def _private_cancel(cid: str, *, side: Side, order_id: int = 123):
    return {
        "c": cid,
        "i": order_id,
        "s": "BTCUSDC",
        "S": side.value,
        "X": "CANCELED",
        "o": "LIMIT",
        "p": "99.9" if side is Side.BUY else "100.1",
        "q": "0.001",
        "z": "0",
        "l": "0",
        "L": "0",
        "T": int(time.time() * 1000),
        "_local_receive_ts_ns": time.time_ns(),
    }


def _private_fill(cid: str, *, side: Side, order_id: int = 123):
    price = "99.9" if side is Side.BUY else "100.1"
    return {
        "c": cid,
        "i": order_id,
        "s": "BTCUSDC",
        "S": side.value,
        "X": "FILLED",
        "o": "LIMIT",
        "p": price,
        "q": "0.001",
        "l": "0.001",
        "z": "0.001",
        "L": price,
        "n": "0",
        "N": "USDC",
        "t": 991,
        "T": 1_900_000_000_000,
    }


def _engine(rest: _RestClient) -> MakerEngine:
    cfg = Config()
    cfg.strategy.order_size = 0.001
    cfg.strategy.requote_threshold_bps = 0.0
    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine.rest = rest
    engine.orders = OrderManager()
    engine._qty_precision = 3
    engine._price_precision = 1
    engine._close_gtx_rejects = 0
    engine._close_start_time = 0.0
    engine._best_bid = 99.8
    engine._best_ask = 100.2
    engine._bid_cid = None
    engine._ask_cid = None
    engine._order_ref_lock = threading.RLock()
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._shutdown_new_order_admission_revoked = False
    engine._record_exact_order_event = lambda *args, **kwargs: None
    engine._record_perf_rest_latency = lambda *args, **kwargs: None
    engine._log_order_outcome = lambda *args, **kwargs: None
    engine._pop_order_context = lambda *args, **kwargs: None
    return engine


@pytest.mark.parametrize("route", ["limit", "async", "close", "emergency"])
@pytest.mark.parametrize("after_reservation", [False, True])
def test_shutdown_admission_blocks_all_new_order_routes(route, after_reservation):
    rest = _RestClient()
    rest.revoke_new_order_admission = lambda: None
    engine = _engine(rest)
    engine.inventory = SimpleNamespace(net_position=0.001)
    if route == "async":
        gateway = _ControllableAsyncGateway()
        gateway.revoke_new_order_admission = lambda: None
        engine.order_gateway = gateway
        engine.cfg.api.async_order_lanes_enabled = True
    if after_reservation:
        engine._record_exact_order_event = (
            lambda *_args, **_kwargs: engine.revoke_new_order_authority_for_shutdown()
        )
    else:
        engine.revoke_new_order_authority_for_shutdown()

    if route in {"limit", "async"}:
        engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    elif route == "close":
        engine._place_close_order("BTCUSDC", Side.SELL, 100.1, 0.001, use_ioc=True)
    else:
        engine._emergency_close(100.0)

    assert rest.calls == []
    if route == "async":
        assert gateway.new_calls == []
    assert engine._shutdown_new_order_admission_revoked is True
    assert engine._execution_state_uncertain() is False
    assert engine.runtime_safety_snapshot()["ownership_conflict_latched"] is False
    assert engine.orders.get_active_orders() == []


@pytest.mark.parametrize("completion", ["NEW", "EXPIRED", "unknown"])
def test_shutdown_preserves_late_async_response_and_unknown_ownership(completion):
    gateway = _ControllableAsyncGateway()
    gateway.revoke_new_order_admission = lambda: None
    engine = _engine(_RestClient())
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.revoke_new_order_authority_for_shutdown()

    if completion == "unknown":
        gateway.new_future.set_exception(TimeoutError("submit response lost"))
    else:
        gateway.new_future.set_result(_result_for(gateway.new_calls[0], status=completion))
    order = engine.orders.get_order(cid)
    if completion == "unknown":
        assert order.state is OrderState.PENDING_NEW
        assert order.lifecycle.submit_ack_unknown_observed is True
        assert engine._bid_cid == cid
    else:
        assert order.state is (
            OrderState.OPEN if completion == "NEW" else OrderState.EXPIRED
        )
        assert order.lifecycle.submit_ack_unknown_observed is False
    assert engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001) is None
    assert len(gateway.new_calls) == 1


@pytest.mark.parametrize("conflict_before_shutdown", [False, True])
def test_shutdown_does_not_hide_real_ownership_conflict(conflict_before_shutdown):
    rest = _RestClient()
    rest.revoke_new_order_admission = lambda: None
    engine = _engine(rest)
    first = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert engine._reserve_side_order_ownership(side=Side.BUY, cid=first)
    if not conflict_before_shutdown:
        engine.revoke_new_order_authority_for_shutdown()
    second = engine.orders.create_order("BTCUSDC", Side.BUY, 99.8, 0.001)
    assert not engine._reserve_side_order_ownership(side=Side.BUY, cid=second)
    if conflict_before_shutdown:
        engine.revoke_new_order_authority_for_shutdown()
    assert engine._execution_state_uncertain() is True
    assert engine.runtime_safety_snapshot()["ownership_conflict_latched"] is True


@pytest.mark.parametrize("completion", ["unknown", "NEW", "PENDING_CANCEL", "CANCELED"])
def test_shutdown_exit_rejects_only_current_unresolved_submit_ack(completion):
    rest = _RestClient()
    gateway = _ControllableAsyncGateway()
    gateway.revoke_new_order_admission = lambda: None
    gateway.cancel_open_orders = rest.cancel_open_orders
    engine = _engine(rest)
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    engine.signal = SimpleNamespace(stop=lambda: None)
    engine._persist_fill_cooldown_checkpoint = lambda: None
    engine.close_fill_cooldown_checkpoint_store = lambda: None
    sync_calls = []
    engine.sync_position = lambda **kwargs: sync_calls.append(kwargs) or True
    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.revoke_new_order_authority_for_shutdown()

    def drain():
        # Represents callback completion after the user stream has stopped.
        gateway.new_future.set_exception(TimeoutError("submit response lost"))
        assert engine.orders.get_order(cid).lifecycle.submit_ack_unknown_observed
        if completion != "unknown":
            engine.orders.confirm_new(cid, 123)
        if completion == "PENDING_CANCEL":
            engine.orders.mark_pending_cancel(cid)
        if completion == "CANCELED":
            engine.orders.on_order_update(_private_cancel(cid, side=Side.BUY))

    engine._drain_order_completion_callbacks = drain
    cleanup_errors = []
    try:
        engine.stop()
    except RuntimeError as exc:
        cleanup_errors.append(exc)

    assert sync_calls and sync_calls[0] == {"required": True}
    safety = engine.runtime_safety_snapshot()
    assert safety["ownership_conflict_latched"] is False
    assert rest.cancel_calls
    exit_code = resolve_live_shutdown_exit(
        engine=engine, fatal_error=None, fatal_traceback=None,
        cleanup_errors=cleanup_errors,
    )
    if completion == "unknown":
        assert engine.orders.get_order(cid).state is OrderState.PENDING_NEW
        assert engine._bid_cid == cid
        assert safety["reconciliation_required"] is True
        assert exit_code == 78
    else:
        assert safety["reconciliation_required"] is False
        assert cleanup_errors == []
        assert exit_code == 0


def _install_exact_result_trade_sync(
    engine: MakerEngine,
    *,
    cumulative_fill: float,
    price: float,
    commission: float,
) -> None:
    """Provide the independent accountTrades evidence for a RESULT response."""

    def _sync_position(*, required: bool = False) -> bool:
        active = engine.orders.get_active_orders()
        assert len(active) == 1
        order = active[0]
        delta = cumulative_fill - float(order.filled_qty)
        if delta > 1e-12:
            engine.orders.reconcile_exchange_trade(
                exchange_order_id=int(order.order_id),
                trade_id=9_001,
                symbol=order.symbol,
                side=order.side,
                quantity=delta,
                price=price,
                commission=commission,
                commission_asset="USDC",
                cumulative_fill=cumulative_fill,
                trade_time_ms=1_900_000_000_000,
            )
        return True

    engine.sync_position = _sync_position


def test_close_caller_selected_ioc_reaches_exchange_and_stays_latched() -> None:
    rest = _RestClient(response={"orderId": 7, "status": "EXPIRED"})
    engine = _engine(rest)

    engine._place_close_order(
        "BTCUSDC",
        Side.BUY,
        100.4,
        0.001,
        use_ioc=True,
    )

    assert rest.calls[0]["timeInForce"] == "IOC"
    assert rest.calls[0]["reduceOnly"] == "true"
    assert engine._close_gtx_rejects == 3


def test_close_submit_uses_hot_gateway_and_reconcile_uses_cold_client() -> None:
    poison_legacy = _RestClient(error=AssertionError("legacy REST hot path used"))
    hot = _RestClient(response={"orderId": 7, "status": "NEW"})
    cold = _QueryClient(
        {
            "orderId": 7,
            "clientOrderId": "unused",
            "symbol": "BTCUSDC",
            "side": "BUY",
            "status": "NEW",
            "price": "100.4",
            "origQty": "0.001",
            "executedQty": "0",
            "avgPrice": "0",
        }
    )
    engine = _engine(poison_legacy)
    engine.order_gateway = hot
    engine.reconciliation_client = cold

    engine._place_close_order(
        "BTCUSDC",
        Side.BUY,
        100.4,
        0.001,
        use_ioc=False,
    )

    assert len(hot.calls) == 1
    assert poison_legacy.calls == []
    order = engine.orders.get_order(engine._bid_cid)
    assert order is not None
    order.state = OrderState.PENDING_NEW
    cold.response["clientOrderId"] = order.client_order_id
    resolution = engine.reconcile_pending_new_order(order)
    assert resolution == "exchange_status_new_reconciled"
    assert len(cold.calls) == 1
    assert poison_legacy.calls == []


def test_ioc_expired_response_preserves_partial_fill_and_unknown_activation() -> None:
    rest = _RestClient(
        response={
            "orderId": 9,
            "symbol": "BTCUSDC",
            "side": "BUY",
            "status": "EXPIRED",
            "price": "100.4",
            "origQty": "0.001",
            "executedQty": "0.0004",
            "avgPrice": "100.3",
            "updateTime": 1_900_000_000_000,
        }
    )
    engine = _engine(rest)
    _install_exact_result_trade_sync(
        engine,
        cumulative_fill=0.0004,
        price=100.2,
        commission=0.01,
    )
    fills = []
    engine.orders._on_fill = lambda order, event: fills.append(
        (
            order.filled_qty,
            event["_fill_qty"],
            event["_fill_price"],
            event["_fill_commission"],
            event["_fill_commission_asset"],
        )
    )

    engine._place_close_order(
        "BTCUSDC",
        Side.BUY,
        100.4,
        0.001,
        use_ioc=True,
    )

    order = next(iter(engine.orders._history.values()))
    snapshot = order.lifecycle.snapshot()
    assert order.filled_qty == 0.0004
    assert fills == [(0.0004, 0.0004, 100.2, 0.01, "USDC")]
    assert snapshot["activation_ts_ns"] == 0
    assert snapshot["activation_exchange_ts_ns"] == 0
    assert snapshot["terminal_reason"] == "expired"
    assert snapshot["visible_exposure_valid"] is False
    assert snapshot["exchange_exposure_valid"] is False


def test_message_only_5022_keeps_close_submit_unknown_and_owned() -> None:
    rest = _RestClient(error=RuntimeError("APIError(code=-5022): GTX order rejected"))
    engine = _engine(rest)

    engine._place_close_order(
        "BTCUSDC",
        Side.SELL,
        100.1,
        0.001,
        use_ioc=False,
    )

    assert rest.calls[0]["timeInForce"] == "GTX"
    assert engine._close_gtx_rejects == 0
    assert engine._ask_cid is not None
    assert engine.orders.get_order(engine._ask_cid).state.name == "PENDING_NEW"


def test_authoritative_5022_is_exact_gtx_close_rejection() -> None:
    rest = _RestClient(
        error=_AuthoritativeExchangeError(
            -5022,
            "Post Only order will be rejected",
        )
    )
    engine = _engine(rest)

    engine._place_close_order(
        "BTCUSDC",
        Side.SELL,
        100.1,
        0.001,
        use_ioc=False,
    )

    assert engine._close_gtx_rejects == 1
    assert engine._ask_cid is None


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_close_rest_minus_2013_keeps_unknown_submit_owned(side: Side) -> None:
    engine = _engine(_UnknownCloseThenNotFoundRest())

    engine._place_close_order(
        "BTCUSDC",
        side,
        100.1,
        0.001,
        use_ioc=False,
    )

    cid = engine._bid_cid if side == Side.BUY else engine._ask_cid
    assert cid is not None
    resolution = engine.reconcile_pending_new_order(engine.orders.get_order(cid))
    assert resolution == "exchange_not_found_ack_still_unknown"
    assert (engine._bid_cid if side == Side.BUY else engine._ask_cid) == cid
    assert engine.orders.get_order(cid).state.name == "PENDING_NEW"


def test_stale_close_uses_marketable_touch_based_ioc() -> None:
    engine = _engine(_RestClient())
    engine._close_start_time = time.time() - 61.0
    captured = {}

    def _capture(symbol, side, price, quantity, decision_context=None, *, use_ioc=False):
        captured.update(
            symbol=symbol,
            side=side,
            price=price,
            quantity=quantity,
            use_ioc=use_ioc,
        )

    engine._place_close_order = _capture
    engine._handle_closing_requote(-0.001, 100.0, pred=None)

    assert captured["side"] == Side.BUY
    assert captured["use_ioc"] is True
    assert captured["price"] == 100.4


def test_terminal_tombstone_releases_side_after_rich_history_eviction() -> None:
    engine = _engine(_RestClient())
    engine.orders = OrderManager(max_history=1)
    first = engine.orders.create_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    engine.orders.confirm_new(first, 41)
    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": first,
            "S": "SELL",
            "X": "CANCELED",
            "i": 41,
            "p": "100.1",
            "q": "0.001",
        }
    )
    second = engine.orders.create_order("BTCUSDC", Side.SELL, 100.2, 0.001)
    engine.orders.confirm_new(second, 42)
    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": second,
            "S": "SELL",
            "X": "CANCELED",
            "i": 42,
            "p": "100.2",
            "q": "0.001",
        }
    )
    assert first not in engine.orders._history
    engine._ask_cid = first

    assert engine._prune_terminal_side_order_reference(Side.SELL) is True
    assert engine._ask_cid is None


def test_unknown_side_reference_is_not_treated_as_terminal_proof() -> None:
    engine = _engine(_RestClient())
    engine._ask_cid = "mm_S_unknown_without_tombstone"

    assert engine._prune_terminal_side_order_reference(Side.SELL) is False

    assert engine._ask_cid == "mm_S_unknown_without_tombstone"
    assert engine._order_submit_fail_closed is True
    safety = engine.runtime_safety_snapshot(now_monotonic_s=1.0)
    assert safety["fatal_runtime_latched"] is True
    assert safety["reconciliation_required"] is True


def test_prune_consumes_one_atomic_snapshot_across_terminal_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _engine(_RestClient())
    manager = OrderManager()
    engine.orders = manager
    cid = manager.create_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    manager.confirm_new(cid, 41)
    engine._ask_cid = cid

    snapshot_taken = threading.Event()
    transition_done = threading.Event()
    transition_errors: list[BaseException] = []
    original_snapshot = manager.ownership_snapshot

    def reject_split_read(*_args, **_kwargs):
        pytest.fail("side-reference pruning must not perform a split ownership read")

    monkeypatch.setattr(manager, "terminal_identity", reject_split_read)
    monkeypatch.setattr(manager, "get_order", reject_split_read)

    def snapshot_then_wait_for_terminal(client_order_id: str):
        snapshot = original_snapshot(client_order_id)
        assert snapshot.status is OrderOwnershipStatus.ACTIVE_NONTERMINAL
        snapshot_taken.set()
        assert transition_done.wait(timeout=2.0)
        return snapshot

    monkeypatch.setattr(manager, "ownership_snapshot", snapshot_then_wait_for_terminal)

    def terminalize_after_snapshot() -> None:
        assert snapshot_taken.wait(timeout=2.0)
        try:
            manager.on_order_update(
                {
                    "s": "BTCUSDC",
                    "c": cid,
                    "S": "SELL",
                    "X": "CANCELED",
                    "i": 41,
                    "p": "100.1",
                    "q": "0.001",
                }
            )
        except BaseException as exc:  # pragma: no cover - surfaced below
            transition_errors.append(exc)
        finally:
            transition_done.set()

    terminal_thread = threading.Thread(target=terminalize_after_snapshot)
    terminal_thread.start()

    # The snapshot linearizes before the terminal transition.  It may be stale
    # by the time the consumer sees it, but it remains a coherent ACTIVE result
    # and must not be combined with a later terminal lookup into false UNKNOWN.
    assert engine._prune_terminal_side_order_reference(Side.SELL) is False
    terminal_thread.join(timeout=2.0)
    assert not terminal_thread.is_alive()
    assert transition_errors == []
    assert engine._ask_cid == cid
    assert engine._running is True
    assert engine._order_submit_fail_closed is False

    monkeypatch.setattr(manager, "ownership_snapshot", original_snapshot)
    assert engine._prune_terminal_side_order_reference(Side.SELL) is True
    assert engine._ask_cid is None


def test_residual_close_waits_for_old_cancel_then_places_one_candidate() -> None:
    engine = _engine(_RestClient())
    engine.inventory = SimpleNamespace(force_flat=lambda: None)
    old = engine.orders.create_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    engine.orders.confirm_new(old, 41)
    engine.orders.mark_pending_cancel(old)
    engine._ask_cid = old
    candidates = []
    engine._place_close_order = lambda *args, **kwargs: candidates.append(
        (args, kwargs)
    )

    engine._handle_closing_requote(0.001, 100.0, pred=None)
    assert candidates == []
    assert engine._ask_cid == old

    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": old,
            "S": "SELL",
            "X": "CANCELED",
            "i": 41,
            "p": "100.1",
            "q": "0.001",
        }
    )
    engine._handle_closing_requote(0.001, 100.0, pred=None)

    assert len(candidates) == 1
    args, kwargs = candidates[0]
    assert args[1] == Side.SELL
    assert args[3] == pytest.approx(0.001)
    assert kwargs["use_ioc"] is False
    assert engine._ask_cid is None


def test_dust_timeout_position_is_retained_and_latches_reconciliation() -> None:
    rest = _RestClient()
    engine = _engine(rest)
    dust_qty = 0.0004
    force_flat_calls = []
    inventory = SimpleNamespace(
        net_position=dust_qty,
        state=PositionState.TIMEOUT_CLOSING,
        force_flat=lambda: force_flat_calls.append(True),
    )
    engine.inventory = inventory
    required_syncs = []
    engine.sync_position = lambda *, required=False: required_syncs.append(required) or True

    engine._handle_closing_requote(dust_qty, 100.0, pred=None)

    assert inventory.net_position == pytest.approx(dust_qty)
    assert inventory.state == PositionState.TIMEOUT_CLOSING
    assert force_flat_calls == []
    assert required_syncs == [True]
    assert rest.cancel_calls == [{"symbol": "BTCUSDC"}]
    safety = engine.runtime_safety_snapshot(now_monotonic_s=1.0)
    assert safety["fatal_runtime_latched"] is True
    assert safety["fatal_runtime_reason"] == (
        "DUST_POSITION_RECONCILIATION_REQUIRED"
    )
    assert safety["reconciliation_required"] is True

    calls_before = len(rest.calls)
    assert engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001) is None
    assert len(rest.calls) == calls_before


def test_emergency_close_dust_latches_without_rounding_or_submitting() -> None:
    rest = _RestClient()
    engine = _engine(rest)
    dust_qty = -0.0004
    engine.inventory = SimpleNamespace(
        net_position=dust_qty,
        state=PositionState.TIMEOUT_CLOSING,
    )
    engine.sync_position = lambda *, required=False: True

    engine._emergency_close(100.0)

    assert engine.inventory.net_position == pytest.approx(dust_qty)
    assert engine.inventory.state == PositionState.TIMEOUT_CLOSING
    assert rest.calls == []
    safety = engine.runtime_safety_snapshot(now_monotonic_s=1.0)
    assert safety["fatal_runtime_reason"] == (
        "DUST_POSITION_RECONCILIATION_REQUIRED"
    )


@pytest.mark.parametrize("route", ["limit", "close", "emergency"])
def test_explicit_pre_dispatch_submit_failure_releases_local_ownership(route: str) -> None:
    rest = _RestClient(error=_PreDispatchUnavailable("not sent"))
    engine = _engine(rest)

    if route == "limit":
        result = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
        side = Side.BUY
        assert result is None
    elif route == "close":
        engine._place_close_order("BTCUSDC", Side.BUY, 100.4, 0.001)
        side = Side.BUY
    else:
        engine.inventory = SimpleNamespace(net_position=0.001)
        engine._emergency_close(100.0)
        side = Side.SELL

    active = (
        engine.orders.get_bid_orders()
        if side == Side.BUY
        else engine.orders.get_ask_orders()
    )
    assert active == []
    assert engine._side_order_reference(side) is None


@pytest.mark.parametrize("route", ["limit", "close", "emergency"])
def test_result_new_positive_execution_uses_exact_account_trade(route: str) -> None:
    rest = _RestClient(
        response={
            "orderId": 91,
            "status": "NEW",
            "executedQty": "0.0004",
        }
    )
    engine = _engine(rest)
    _install_exact_result_trade_sync(
        engine,
        cumulative_fill=0.0004,
        price=100.2,
        commission=0.01,
    )
    fills = []
    engine.orders._on_fill = lambda order, event: fills.append(
        (
            order.client_order_id,
            event["_fill_qty"],
            event["_fill_price"],
            event["_fill_commission"],
        )
    )

    if route == "limit":
        cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    elif route == "close":
        engine._place_close_order("BTCUSDC", Side.BUY, 100.4, 0.001)
        cid = engine._bid_cid
    else:
        engine.inventory = SimpleNamespace(net_position=0.001)
        engine._emergency_close(100.0)
        cid = engine._ask_cid

    assert cid is not None
    order = engine.orders.get_order(cid)
    assert order is not None
    assert order.filled_qty == pytest.approx(0.0004)
    assert order.avg_fill_price == pytest.approx(100.2)
    assert fills == [(cid, 0.0004, 100.2, 0.01)]


def test_any_positive_result_new_requires_exact_trade_evidence() -> None:
    rest = _RestClient(
        response={
            "orderId": 93,
            "status": "NEW",
            "executedQty": "0.0000000000001",
        }
    )
    engine = _engine(rest)
    engine.sync_position = lambda *, required=False: True

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)

    assert cid is not None
    assert engine.orders.get_order(cid).filled_qty == 0.0
    safety = engine.runtime_safety_snapshot(now_monotonic_s=1.0)
    assert safety["fatal_runtime_latched"] is True
    assert safety["fatal_runtime_reason"] == (
        "REST_POSITIVE_FILL_EVIDENCE_MISSING"
    )


@pytest.mark.parametrize("route", ["limit", "close", "emergency"])
@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("clientOrderId", "wrong-client-id"),
        ("symbol", "ETHUSDC"),
        ("side", "INVALID"),
        ("origQty", "0.002"),
        ("executedQty", "nan"),
        ("executedQty", "0.002"),
    ],
)
def test_submit_result_identity_or_quantity_mismatch_is_fatal(
    route: str,
    field: str,
    bad_value: str,
) -> None:
    rest = _RestClient(
        response={
            "orderId": 92,
            "status": "NEW",
            field: bad_value,
        }
    )
    engine = _engine(rest)
    engine.sync_position = lambda *, required=False: True

    if route == "limit":
        engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    elif route == "close":
        engine._place_close_order("BTCUSDC", Side.BUY, 100.4, 0.001)
    else:
        engine.inventory = SimpleNamespace(net_position=0.001)
        engine._emergency_close(100.0)

    safety = engine.runtime_safety_snapshot(now_monotonic_s=1.0)
    assert safety["fatal_runtime_latched"] is True
    assert safety["fatal_runtime_reason"] == (
        "SUBMIT_RESULT_IDENTITY_OR_QUANTITY_MISMATCH"
    )
    assert safety["reconciliation_required"] is True
    assert len(rest.calls) == 1


def test_normal_submit_keeps_synchronous_b0_when_async_switch_is_off() -> None:
    rest = _RestClient()
    engine = _engine(rest)
    gateway = _ControllableAsyncGateway()
    gateway.new_order = rest.new_order
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = False

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)

    assert cid is not None
    assert gateway.new_calls == []
    assert len(rest.calls) == 1
    assert engine.orders.get_order(cid).state is OrderState.OPEN


def test_async_submit_returns_pending_without_waiting_for_rest() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)

    assert cid is not None
    assert len(gateway.new_calls) == 1
    assert gateway.new_calls[0]["_narrowgate_order_side"] == "BUY"
    assert not gateway.new_future.done()
    assert engine.orders.get_order(cid).state is OrderState.PENDING_NEW
    assert engine._bid_cid == cid


def test_async_submit_result_transitions_pending_new_to_open() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True

    cid = engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    assert cid is not None
    gateway.new_future.set_result(_result_for(gateway.new_calls[0]))

    order = engine.orders.get_order(cid)
    assert order is not None
    assert order.state is OrderState.OPEN
    assert order.order_id == 123
    assert engine._ask_cid == cid


@pytest.mark.parametrize("route", ("limit", "async", "close", "emergency"))
@pytest.mark.parametrize("status", ("NEW", "EXPIRED"))
def test_submit_result_local_evidence_failure_preserves_exchange_state(
    route: str, status: str,
) -> None:
    rest = _RestClient(response={"orderId": 123, "status": status})
    engine = _engine(rest)

    def fail_outcome(*_args, **_kwargs):
        raise RuntimeError("local outcome writer failed")

    engine._log_order_outcome = fail_outcome
    if route == "async":
        gateway = _ControllableAsyncGateway()
        gateway.cancel_open_orders = rest.cancel_open_orders
        engine.order_gateway = gateway
        engine.cfg.api.async_order_lanes_enabled = True
        cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
        gateway.new_future.set_result(_result_for(gateway.new_calls[0], status=status))
    elif route == "limit":
        cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    elif route == "close":
        engine._place_close_order("BTCUSDC", Side.BUY, 99.9, 0.001, use_ioc=True)
        cid = rest.calls[0]["newClientOrderId"]
    else:
        engine.inventory = SimpleNamespace(net_position=0.001)
        engine._emergency_close(100.0)
        cid = rest.calls[0]["newClientOrderId"]

    order = engine.orders.get_order(cid)
    assert order.order_id == 123
    assert order.state is (OrderState.OPEN if status == "NEW" else OrderState.EXPIRED)
    assert order.lifecycle.submit_ack_unknown_observed is False
    # Async terminal results do not need a second outcome row after the
    # authoritative terminal callback and therefore never hit fail_outcome.
    if route == "async" and status == "EXPIRED":
        assert engine._execution_state_uncertain() is False
    else:
        assert engine._running is False
        assert engine._order_submit_fail_closed is True
        assert engine._runtime_fatal_reason == "SUBMIT_RESULT_PROCESSING_FAILED"
        assert rest.cancel_calls == [{"symbol": "BTCUSDC"}]


@pytest.mark.parametrize("terminal", (False, True))
def test_sync_submit_late_timeout_cannot_erase_private_exchange_evidence(
    terminal: bool,
) -> None:
    rest = _RestClient()
    engine = _engine(rest)

    def accepted_then_timeout(**params):
        rest.calls.append(params)
        cid = params["newClientOrderId"]
        engine.orders.confirm_new(cid, 123)
        if terminal:
            engine.orders.on_order_update(_private_cancel(cid, side=Side.BUY))
        raise TimeoutError("late response lost after private acceptance")

    rest.new_order = accepted_then_timeout
    returned_cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    cid = rest.calls[0]["newClientOrderId"]
    order = engine.orders.get_order(cid)
    assert order.state is (OrderState.CANCELED if terminal else OrderState.OPEN)
    assert order.lifecycle.submit_ack_unknown_observed is False
    assert returned_cid == (None if terminal else cid)
    assert engine._execution_state_uncertain() is False


def test_unknown_ack_race_with_private_new_keeps_authoritative_acceptance() -> None:
    engine = _engine(_RestClient())
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine._reserve_side_order_ownership(side=Side.BUY, cid=cid)
    mark_unknown = engine.orders.mark_submit_ack_unknown

    def private_new_before_mark(order_cid, reason):
        engine.orders.confirm_new(order_cid, 123)
        return mark_unknown(order_cid, reason)

    engine.orders.mark_submit_ack_unknown = private_new_before_mark
    assert engine._hold_submit_with_unknown_ack(
        cid=cid, side=Side.BUY, error=TimeoutError("late response"),
    ) is False
    assert engine.orders.get_order(cid).state is OrderState.OPEN
    assert engine.orders.get_order(cid).lifecycle.submit_ack_unknown_observed is False


def test_engine_async_rest_uses_global_fifo_and_consumes_private_terminal_first():
    new_started = threading.Event()
    release_new = threading.Event()
    cancel_started = threading.Event()
    release_cancel = threading.Event()
    network_lock = threading.Lock()
    calls = []
    inflight = 0
    max_inflight = 0

    def request(method, params, *, started=None, release=None):
        nonlocal inflight, max_inflight
        cid = params.get("newClientOrderId") or params["origClientOrderId"]
        with network_lock:
            calls.append((method, cid))
            inflight += 1
            max_inflight = max(max_inflight, inflight)
        try:
            if started is not None:
                started.set()
            if release is not None:
                assert release.wait(2.0), "test HTTP response was not released"
            return _result_for(
                params,
                status="CANCELED" if method == "cancel" else "NEW",
                order_id=124 if params.get("side") == "SELL" else 123,
            )
        finally:
            with network_lock:
                inflight -= 1

    class HeldRest:
        def new_order(self, **params):
            return request(
                "new", params,
                started=new_started if params["side"] == "BUY" else None,
                release=release_new if params["side"] == "BUY" else None,
            )

        def cancel_order(self, **params):
            return request(
                "cancel", {**params, "side": "BUY"},
                started=cancel_started, release=release_cancel,
            )

    rest = HeldRest()
    gateway = BinanceUsdMOrderGateway(
        rest_order_client=rest,
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=False,
    )
    engine = _engine(rest)
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    engine._dynamic_fill_hazard_shadow_runtime = None
    engine._dynamic_fill_hazard_action_lock = threading.RLock()
    engine._dynamic_fill_hazard_action_hold = None
    engine.orders = OrderManager(
        on_cancel=engine._on_cancel,
        on_terminal=engine._on_order_terminal,
    )
    try:
        buy_cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
        assert buy_cid is not None
        assert new_started.wait(1.0)
        assert engine.orders.get_order(buy_cid).state is OrderState.PENDING_NEW
        assert inflight == 1
        private_new = _private_cancel(buy_cid, side=Side.BUY)
        private_new["X"] = "NEW"
        engine.orders.on_order_update(private_new)
        assert engine.orders.get_order(buy_cid).state is OrderState.OPEN

        assert not engine._cancel_order(buy_cid)
        assert engine.orders.get_order(buy_cid).state is OrderState.PENDING_CANCEL
        assert calls == [("new", buy_cid)]
        assert not cancel_started.is_set()
        release_new.set()
        assert cancel_started.wait(1.0)
        assert inflight == 1

        engine.orders.on_order_update(_private_cancel(buy_cid, side=Side.BUY))
        assert engine.orders.get_order(buy_cid).state is OrderState.CANCELED
        assert engine._bid_cid is None
        assert not release_cancel.is_set()
        sell_cid = engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001)
        assert sell_cid is not None
        assert engine.orders.get_order(sell_cid).state is OrderState.PENDING_NEW
        assert calls == [("new", buy_cid), ("cancel", buy_cid)]

        release_cancel.set()
        gateway.drain_async_order_completions(timeout_s=2.0)
        assert calls == [("new", buy_cid), ("cancel", buy_cid), ("new", sell_cid)]
        assert max_inflight == 1
        assert inflight == 0
        assert engine.orders.get_order(buy_cid).state is OrderState.CANCELED
        assert engine.orders.get_order(sell_cid).state is OrderState.OPEN
        assert engine._bid_cid is None
        assert engine._ask_cid == sell_cid
        health = gateway.health_snapshot()
        assert health["active_transport"] == "rest"
        assert list(health["async_order_lanes"]) == ["GLOBAL"]
        assert health["async_order_lanes"]["GLOBAL"]["future_results_delivered"] == 3
        assert engine.is_running is True
    finally:
        release_new.set()
        release_cancel.set()
        gateway.close()


def test_runtime_fatal_cancels_before_failed_continuation_evidence() -> None:
    rest = _RestClient()
    engine = _engine(rest)

    def fail_continuation_cleanup(**_kwargs):
        assert rest.cancel_calls == [{"symbol": "BTCUSDC"}]
        raise RuntimeError("continuation evidence writer failed")

    engine._clear_all_replace_terminal_continuations = fail_continuation_cleanup
    engine.latch_runtime_fatal(
        reason="LOCAL_EVIDENCE_FAILURE",
        error=RuntimeError("writer poisoned"),
        reconciliation_required=True,
        defer_reconciliation=True,
    )
    assert engine._running is False
    assert engine._execution_state_uncertain() is True
    assert rest.cancel_calls == [{"symbol": "BTCUSDC"}]


def test_gateway_evidence_failure_is_checked_outside_submit_classification() -> None:
    rest = _RestClient()
    engine = _engine(rest)

    def fail_evidence_check():
        raise RuntimeError("order gateway receipt collection failed")

    rest.raise_if_evidence_failed = fail_evidence_check
    engine.order_gateway = rest
    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    with pytest.raises(RuntimeError, match="ORDER_GATEWAY_EVIDENCE_FAILED"):
        engine.raise_if_runtime_fatal()
    order = engine.orders.get_order(cid)
    assert order.state is OrderState.OPEN
    assert order.lifecycle.submit_ack_unknown_observed is False
    assert engine._running is False
    assert rest.cancel_calls == [{"symbol": "BTCUSDC"}]


def test_async_submit_late_result_does_not_resurrect_private_terminal() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert cid is not None
    engine.orders.on_order_update(_private_cancel(cid, side=Side.BUY))
    assert engine.orders.ownership_snapshot(cid).status is OrderOwnershipStatus.TERMINAL

    gateway.new_future.set_result(_result_for(gateway.new_calls[0]))

    ownership = engine.orders.ownership_snapshot(cid)
    assert ownership.status is OrderOwnershipStatus.TERMINAL
    assert ownership.terminal_identity["terminal_state"] == "CANCELED"
    assert engine.orders.get_active_orders() == []
    assert engine._bid_cid is None


def test_async_submit_late_terminal_result_does_not_duplicate_outcome() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    outcomes = []
    engine._log_order_outcome = lambda event, _order: outcomes.append(event)

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert cid is not None
    engine.orders.on_order_update(_private_cancel(cid, side=Side.BUY))
    outcomes.clear()

    gateway.new_future.set_result(
        _result_for(gateway.new_calls[0], status="CANCELED")
    )

    assert outcomes == []
    assert engine.orders.ownership_snapshot(cid).status is OrderOwnershipStatus.TERMINAL


def test_async_submit_late_result_does_not_duplicate_private_fill() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    fills = []
    engine.orders._on_fill = lambda _order, event: fills.append(event["_fill_qty"])

    cid = engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    assert cid is not None
    engine.orders.on_order_update(_private_fill(cid, side=Side.SELL))
    assert fills == [pytest.approx(0.001)]

    late_result = _result_for(gateway.new_calls[0], status="FILLED")
    late_result["executedQty"] = "0.001"
    gateway.new_future.set_result(late_result)

    assert fills == [pytest.approx(0.001)]
    ownership = engine.orders.ownership_snapshot(cid)
    assert ownership.status is OrderOwnershipStatus.TERMINAL
    assert ownership.terminal_identity["terminal_state"] == "FILLED"


def test_async_submit_late_transport_error_defers_to_private_terminal() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True

    cid = engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert cid is not None
    engine.orders.on_order_update(_private_cancel(cid, side=Side.BUY))

    gateway.new_future.set_exception(TimeoutError("late response lost"))

    ownership = engine.orders.ownership_snapshot(cid)
    assert ownership.status is OrderOwnershipStatus.TERMINAL
    assert ownership.terminal_identity["terminal_state"] == "CANCELED"
    assert engine._bid_cid is None
    assert engine.runtime_safety_snapshot(
        now_monotonic_s=1.0
    )["fatal_runtime_latched"] is False


def test_async_submit_transport_unknown_retains_pending_new() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True

    cid = engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    assert cid is not None
    gateway.new_future.set_exception(TimeoutError("response lost"))

    order = engine.orders.get_order(cid)
    assert order is not None
    assert order.state is OrderState.PENDING_NEW
    assert engine._ask_cid == cid


def test_async_cancel_consumes_late_result_after_private_terminal_once() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine.orders.create_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    engine.orders.confirm_new(cid, 123)
    engine._ask_cid = cid

    assert not engine._cancel_order(cid)
    assert gateway.cancel_calls[0]["_narrowgate_order_side"] == "SELL"
    assert engine.orders.get_order(cid).state is OrderState.PENDING_CANCEL
    engine.orders.on_order_update(_private_cancel(cid, side=Side.SELL))

    gateway.cancel_future.set_result(
        _result_for(gateway.cancel_calls[0], status="CANCELED")
    )

    ownership = engine.orders.ownership_snapshot(cid)
    assert ownership.status is OrderOwnershipStatus.TERMINAL
    assert ownership.terminal_identity["terminal_state"] == "CANCELED"
    assert engine.orders.get_active_orders() == []
    assert engine._ask_cid is None


def test_async_cancel_result_can_supply_terminal_before_private_callback() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.orders.confirm_new(cid, 123)
    engine._bid_cid = cid

    assert not engine._cancel_order(cid)
    gateway.cancel_future.set_result(
        _result_for(gateway.cancel_calls[0], status="CANCELED")
    )

    ownership = engine.orders.ownership_snapshot(cid)
    assert ownership.status is OrderOwnershipStatus.TERMINAL
    assert ownership.terminal_identity["terminal_state"] == "CANCELED"
    assert engine._bid_cid is None


def test_async_cancel_active_result_clears_stale_replace_intent() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.orders.confirm_new(cid, 123)
    engine._bid_cid = cid
    cleared = []
    engine._clear_replace_terminal_continuation = lambda **values: cleared.append(
        values
    )

    assert not engine._cancel_order(cid, replace_continuation_generation=7)
    gateway.cancel_future.set_result(_result_for(gateway.cancel_calls[0]))

    assert engine.orders.get_order(cid).state is OrderState.OPEN
    assert cleared == [
        {
            "side": Side.BUY,
            "cid": cid,
            "generation": 7,
            "reason": "cancel_result_active",
        }
    ]


def test_async_cancel_transport_unknown_retains_pending_cancel() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.orders.confirm_new(cid, 123)
    engine._bid_cid = cid

    assert not engine._cancel_order(cid)
    gateway.cancel_future.set_exception(TimeoutError("response lost"))

    order = engine.orders.get_order(cid)
    assert order is not None
    assert order.state is OrderState.PENDING_CANCEL
    assert engine._bid_cid == cid


@pytest.mark.parametrize(
    "failure_stage",
    ["before_apply", "after_apply", "after_private_terminal", "after_active_result"],
)
def test_async_cancel_local_processing_failure_preserves_exchange_state(
    failure_stage, caplog,
) -> None:
    rest = _RestClient()
    engine = _engine(rest)
    gateway = _ControllableAsyncGateway()
    gateway.cancel_open_orders = rest.cancel_open_orders
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.orders.confirm_new(cid, 123)
    engine._bid_cid = cid
    assert not engine._cancel_order(cid)
    if failure_stage == "after_private_terminal":
        engine.orders.on_order_update(_private_cancel(cid, side=Side.BUY))

    apply_response = engine._apply_rest_reconciled_order_status
    response_error_calls = []
    engine._complete_cancel_order_error = lambda **kwargs: response_error_calls.append(kwargs)

    def fail_processing(**kwargs):
        if failure_stage != "before_apply":
            apply_response(**kwargs)
        raise RuntimeError("local cancel result processing failed")

    engine._apply_rest_reconciled_order_status = fail_processing
    status = "NEW" if failure_stage == "after_active_result" else "CANCELED"
    gateway.cancel_future.set_result(_result_for(gateway.cancel_calls[0], status=status))

    assert response_error_calls == []
    assert "CANCEL_ACK_UNKNOWN" not in caplog.text
    expected_state = {
        "before_apply": OrderState.PENDING_CANCEL,
        "after_active_result": OrderState.OPEN,
    }.get(failure_stage, OrderState.CANCELED)
    assert engine.orders.get_order(cid).state is expected_state
    assert engine._order_submit_fail_closed is True
    assert engine.is_running is False
    safety = engine.runtime_safety_snapshot()
    assert safety["fatal_runtime_reason"] == "ASYNC_ORDER_CANCEL_COMPLETION_FAILED"
    assert safety["reconciliation_required"] is True
    assert engine._runtime_reconciliation_quiescence_blocked is True
    assert len(rest.cancel_calls) == 1
    assert engine._place_order("BTCUSDC", Side.SELL, 100.1, 0.001) is None
    assert gateway.new_calls == []


@pytest.mark.parametrize("response_kind", ["missing_field", "unrecognized_status"])
def test_async_cancel_unvalidated_response_still_retains_unknown_ownership(response_kind):
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    engine.orders.confirm_new(cid, 123)
    engine._bid_cid = cid
    assert not engine._cancel_order(cid)
    response = _result_for(gateway.cancel_calls[0], status="CANCELED")
    if response_kind == "missing_field":
        del response["executedQty"]
    else:
        response["status"] = "UNRECOGNIZED"
    gateway.cancel_future.set_result(response)

    assert engine.orders.get_order(cid).state is OrderState.PENDING_CANCEL
    assert engine._bid_cid == cid
    assert engine.is_running is True
    assert engine._order_submit_fail_closed is False


def test_async_submit_pre_dispatch_failure_releases_ownership() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True

    def fail_before_dispatch(**_params):
        raise _PreDispatchUnavailable("lane full")

    gateway.new_order_async = fail_before_dispatch

    assert engine._place_order("BTCUSDC", Side.BUY, 99.9, 0.001) is None
    assert engine.orders.get_active_orders() == []
    assert engine._bid_cid is None


def test_async_cancel_pre_dispatch_failure_restores_active_ownership() -> None:
    engine = _engine(_RestClient())
    gateway = _ControllableAsyncGateway()
    engine.order_gateway = gateway
    engine.cfg.api.async_order_lanes_enabled = True
    cid = engine.orders.create_order("BTCUSDC", Side.SELL, 100.1, 0.001)
    engine.orders.confirm_new(cid, 123)
    engine._ask_cid = cid

    def fail_before_dispatch(**_params):
        raise _PreDispatchUnavailable("lane full")

    gateway.cancel_order_async = fail_before_dispatch

    assert not engine._cancel_order(cid)
    assert engine.orders.get_order(cid).state is OrderState.OPEN
    assert engine._ask_cid == cid
