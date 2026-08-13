import threading
import time

import pytest

from live.config import Config
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, Side


class _RestClient:
    def __init__(self, response=None, error=None):
        self.response = response or {"orderId": 7, "status": "NEW"}
        self.error = error
        self.calls = []

    def new_order(self, **params):
        self.calls.append(params)
        if self.error is not None:
            raise self.error
        return dict(self.response)


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
    engine._record_perf_rest_latency = lambda *args, **kwargs: None
    engine._log_order_outcome = lambda *args, **kwargs: None
    engine._pop_order_context = lambda *args, **kwargs: None
    return engine


def test_close_caller_selected_ioc_reaches_exchange_and_stays_latched() -> None:
    rest = _RestClient(response={"orderId": 0, "status": "EXPIRED"})
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
            "clientOrderId": "",
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
    fills = []
    engine.orders._on_fill = lambda order, event: fills.append(
        (order.filled_qty, event["_fill_qty"])
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
    assert fills == [(0.0004, 0.0004)]
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
