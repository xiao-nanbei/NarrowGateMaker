import time

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


def test_binance_5022_counts_as_gtx_close_rejection() -> None:
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
    assert engine._close_gtx_rejects == 1


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
