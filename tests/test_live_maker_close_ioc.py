import threading
import time
from types import SimpleNamespace

import pytest

from live.config import Config
from strategy.inventory_manager import PositionState
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, Side


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


class _AuthoritativeExchangeError(RuntimeError):
    exchange_response_authoritative = True

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


class _UnknownCloseThenNotFoundRest:
    def __init__(self) -> None:
        self.calls = []

    def new_order(self, **params):
        self.calls.append(params)
        raise TimeoutError("submit response lost")

    def query_order(self, **_params):
        raise _AuthoritativeExchangeError(-2013, "Order does not exist")


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
    engine._record_exact_order_event = lambda *args, **kwargs: None
    engine._record_perf_rest_latency = lambda *args, **kwargs: None
    engine._log_order_outcome = lambda *args, **kwargs: None
    engine._pop_order_context = lambda *args, **kwargs: None
    return engine


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
