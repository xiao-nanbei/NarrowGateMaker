import json
import threading
import time
from types import SimpleNamespace

import pytest

from execution.active_order_depth_path import ActiveOrderDepthPathTracker
from live.config import Config, _validate_config
from live.orderbook.binance_usdm import BinanceUsdMDeepBook
from live.ws_handler import WSHandler


def _wait_until(predicate, timeout_s=1.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition did not become true")


class _SnapshotRest:
    def __init__(self, snapshots):
        self._snapshots = list(snapshots)
        self.calls = []

    def depth(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        if len(self._snapshots) > 1:
            return self._snapshots.pop(0)
        return self._snapshots[0]


def _snapshot(update_id=100, bid_qty="2.0", ask_qty="1.0"):
    return {
        "lastUpdateId": update_id,
        "bids": [["100.0", bid_qty], ["99.9", "3.0"]],
        "asks": [["100.1", ask_qty], ["100.2", "4.0"]],
    }


def test_snapshot_diff_book_tracks_depletion_refill_and_coverage():
    rest = _SnapshotRest([_snapshot()])
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(
        lambda: (
            book.snapshot()["valid"] == 1
            and book.snapshot()["syncing"] == 0
        )
    )

    assert rest.calls == [("BTCUSDC", {"limit": 5})]
    initial = book.level_state("BUY", 100.0)
    assert initial.valid
    assert initial.covered
    assert initial.quantity == pytest.approx(2.0)

    receive_ns = time.time_ns()
    book.on_diff_event(
        {
            "e": "depthUpdate",
            "s": "BTCUSDC",
            "U": 101,
            "u": 101,
            "pu": 100,
            "b": [["100.0", "1.25"], ["99.8", "2.0"]],
            "a": [["100.1", "1.5"]],
        },
        receive_ts_ns=receive_ns,
    )

    bid = book.level_state("BUY", 100.0, now_ns=receive_ns)
    ask = book.level_state("SELL", 100.1, now_ns=receive_ns)
    assert bid.quantity == pytest.approx(1.25)
    assert bid.decrease_events == 1
    assert bid.decrease_qty == pytest.approx(0.75)
    assert ask.increase_events == 1
    assert ask.increase_qty == pytest.approx(0.5)

    outside = book.level_state("BUY", 98.0, now_ns=receive_ns)
    assert not outside.covered
    assert not outside.valid

    stale = book.level_state(
        "BUY",
        100.0,
        now_ns=receive_ns + 2_000_000_000,
        max_age_ms=500.0,
    )
    assert not stale.valid
    assert book.snapshot(
        now_ns=receive_ns + 2_000_000_000,
        max_age_ms=500.0,
    )["stale"] == 1
    book.stop()


def test_active_order_path_separates_trade_consumption_cancel_and_refill():
    rest = _SnapshotRest([_snapshot()])
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(lambda: book.snapshot()["valid"] == 1)
    tracker = ActiveOrderDepthPathTracker()
    order = SimpleNamespace(
        client_order_id="mm_B_1",
        state=SimpleNamespace(name="OPEN"),
        side=SimpleNamespace(value="BUY"),
        price=100.0,
    )
    receive_ns = time.time_ns()
    initial = tracker.sync(
        [order],
        level_state=lambda side, price: book.level_state(
            side,
            price,
            now_ns=receive_ns,
        ),
        feature_ready_ts_ns=receive_ns,
    )[0]
    assert initial.valid
    assert initial.initial_visible_qty == pytest.approx(2.0)

    book.on_agg_trade(
        {
            "e": "aggTrade",
            "s": "BTCUSDC",
            "p": "100.0",
            "q": "0.5",
            "m": True,
            "T": 1_000,
        },
        receive_ts_ns=receive_ns + 1,
    )
    book.on_diff_event(
        {
            "e": "depthUpdate",
            "s": "BTCUSDC",
            "U": 101,
            "u": 101,
            "pu": 100,
            "T": 1_000,
            "b": [["100.0", "1.2"]],
            "a": [],
        },
        receive_ts_ns=receive_ns + 2,
    )
    state = tracker.sync(
        [order],
        level_state=lambda side, price: book.level_state(
            side,
            price,
            now_ns=receive_ns + 2,
        ),
        feature_ready_ts_ns=receive_ns + 3,
    )[0]
    assert state.valid
    assert state.raw_decrease_qty == pytest.approx(0.8)
    assert state.exact_price_trade_qty == pytest.approx(0.5)
    assert state.attributed_trade_qty == pytest.approx(0.5)
    assert state.inferred_cancel_qty == pytest.approx(0.3)
    assert state.unresolved_trade_qty == pytest.approx(0.0)
    assert state.queue_ahead_lower == pytest.approx(1.2)
    assert state.queue_ahead_estimate == pytest.approx(1.2)
    assert state.queue_ahead_upper == pytest.approx(1.5)

    book.on_diff_event(
        {
            "e": "depthUpdate",
            "s": "BTCUSDC",
            "U": 102,
            "u": 102,
            "pu": 101,
            "T": 1_100,
            "b": [["100.0", "1.7"]],
            "a": [],
        },
        receive_ts_ns=receive_ns + 4,
    )
    state = tracker.sync(
        [order],
        level_state=lambda side, price: book.level_state(
            side,
            price,
            now_ns=receive_ns + 4,
        ),
        feature_ready_ts_ns=receive_ns + 5,
    )[0]
    assert state.refill_events == 1
    assert state.refill_qty == pytest.approx(0.5)
    book.stop()


def test_active_order_path_fails_closed_when_trade_depth_attribution_is_ambiguous():
    rest = _SnapshotRest([_snapshot()])
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(lambda: book.snapshot()["valid"] == 1)
    tracker = ActiveOrderDepthPathTracker()
    order = SimpleNamespace(
        client_order_id="mm_B_2",
        state=SimpleNamespace(name="OPEN"),
        side=SimpleNamespace(value="BUY"),
        price=100.0,
    )
    tracker.sync(
        [order],
        level_state=lambda side, price: book.level_state(side, price),
    )
    book.on_agg_trade(
        {
            "e": "aggTrade",
            "s": "BTCUSDC",
            "p": "100.0",
            "q": "0.5",
            "m": True,
            "T": 1_000,
        },
        receive_ts_ns=time.time_ns(),
    )
    state = tracker.sync(
        [order],
        level_state=lambda side, price: book.level_state(side, price),
    )[0]
    assert state.ambiguous
    assert not state.valid
    assert state.invalid_reason == "trade_depth_attribution_ambiguous"
    book.stop()


def test_exchange_terminal_order_is_removed_from_active_depth_risk_set():
    rest = _SnapshotRest([_snapshot()])
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(lambda: book.snapshot()["valid"] == 1)
    tracker = ActiveOrderDepthPathTracker()
    order = SimpleNamespace(
        client_order_id="mm_B_hold",
        state=SimpleNamespace(name="OPEN"),
        side=SimpleNamespace(value="BUY"),
        price=100.0,
    )
    tracker.sync(
        [order],
        level_state=lambda side, price: book.level_state(side, price),
    )
    book.on_diff_event(
        {
            "e": "depthUpdate",
            "s": "BTCUSDC",
            "U": 101,
            "u": 101,
            "pu": 100,
            "b": [["100.0", "2.5"]],
            "a": [],
        },
        receive_ts_ns=time.time_ns(),
    )
    pending = tracker.sync(
        [SimpleNamespace(
            client_order_id="mm_B_hold",
            state=SimpleNamespace(name="PENDING_CANCEL"),
            side=SimpleNamespace(value="BUY"),
            price=100.0,
        )],
        level_state=lambda side, price: book.level_state(side, price),
    )
    assert len(pending) == 1
    assert pending[0].client_order_id == "mm_B_hold"
    assert pending[0].refill_qty == pytest.approx(0.5)

    tracker.discard("mm_B_hold")
    terminal = tracker.sync(
        [],
        level_state=lambda side, price: book.level_state(side, price),
    )
    assert terminal == ()
    assert tracker.retained_count() == 0
    with pytest.raises(RuntimeError, match="fill-risk set"):
        tracker.retain("mm_B_hold")
    book.stop()


def test_diff_events_buffer_until_snapshot_and_bridge_sequence():
    release = threading.Event()

    def load_snapshot():
        release.wait(timeout=1.0)
        return _snapshot()

    book = BinanceUsdMDeepBook(
        object(),
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
        snapshot_loader=load_snapshot,
    )
    book.start()
    book.on_diff_event(
        {
            "s": "BTCUSDC",
            "U": 101,
            "u": 101,
            "pu": 100,
            "b": [["100.0", "1.5"]],
            "a": [],
        },
        receive_ts_ns=time.time_ns(),
    )
    assert book.snapshot()["valid"] == 0
    assert book.snapshot()["buffer_events"] == 1

    release.set()
    _wait_until(lambda: book.snapshot()["valid"] == 1)
    state = book.level_state("BUY", 100.0)
    assert state.last_update_id == 101
    assert state.quantity == pytest.approx(1.5)
    book.stop()


def test_sequence_gap_invalidates_then_rebuilds_from_new_snapshot():
    rest = _SnapshotRest(
        [
            _snapshot(update_id=100, bid_qty="2.0"),
            _snapshot(update_id=200, bid_qty="4.0"),
        ]
    )
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(lambda: book.snapshot()["generation"] == 1)

    book.on_diff_event(
        {
            "s": "BTCUSDC",
            "U": 102,
            "u": 102,
            "pu": 999,
            "b": [["100.0", "0.5"]],
            "a": [],
        },
        receive_ts_ns=time.time_ns(),
    )
    _wait_until(lambda: book.snapshot()["generation"] == 2)

    snap = book.snapshot()
    state = book.level_state("BUY", 100.0)
    assert snap["valid"] == 1
    assert snap["gap_count"] == 1
    assert snap["resync_count"] == 2
    assert state.last_update_id == 200
    assert state.quantity == pytest.approx(4.0)
    book.stop()


def test_ws_handler_deep_stream_is_independent_from_partial_depth(monkeypatch):
    clients = []

    class FakeWebSocketClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.diff_requests = []
            self.stopped = False
            clients.append(self)

        def diff_book_depth(self, **kwargs):
            self.diff_requests.append(kwargs)

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(
        "binance.websocket.um_futures.websocket_client.UMFuturesWebsocketClient",
        FakeWebSocketClient,
    )
    cfg = Config()
    cfg.websocket.deep_book_enabled = True
    rest = _SnapshotRest([_snapshot()])
    engine = SimpleNamespace(signal=SimpleNamespace())
    handler = WSHandler(engine, cfg)
    partial_client = object()
    handler._ws_public = partial_client
    handler._running = True
    handler._rest_client = object()
    handler._market_snapshot_client = rest

    assert handler._start_deep_book_stream()
    _wait_until(lambda: handler.deep_book_snapshot()["valid"] == 1)
    assert handler._ws_public is partial_client
    assert clients[0].diff_requests == [
        {"symbol": "btcusdc", "speed": 100, "id": 1}
    ]

    clients[0].kwargs["on_message"](
        None,
        json.dumps(
            {
                "e": "depthUpdate",
                "s": "BTCUSDC",
                "U": 101,
                "u": 101,
                "pu": 100,
                "b": [["100.0", "1.0"]],
                "a": [],
            }
        ),
    )
    assert handler.deep_book_level_state("BUY", 100.0).quantity == pytest.approx(
        1.0
    )
    assert not hasattr(engine.signal, "depth")
    handler._stop_deep_book_stream()
    assert clients[0].stopped


def test_ws_handler_q90_snapshot_uses_one_generation_and_ready_cutoff():
    cfg = Config()
    cfg.websocket.deep_book_enabled = True
    rest = _SnapshotRest([_snapshot()])
    order = SimpleNamespace(
        client_order_id="mm_B_atomic",
        state="OPEN",
        side=SimpleNamespace(value="BUY"),
        price=100.0,
    )
    engine = SimpleNamespace(
        signal=SimpleNamespace(),
        orders=SimpleNamespace(get_active_orders=lambda: [order]),
    )
    handler = WSHandler(engine, cfg)
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(lambda: book.snapshot()["valid"] == 1)
    with handler._deep_book_lock:
        handler._deep_book = book

    ready_ns = time.time_ns()
    paths = handler.maintain_active_order_depth_paths(now_ns=ready_ns)
    visible = handler.dynamic_fill_hazard_visible_snapshot()

    assert len(paths) == 1
    assert visible.feature_ready_ts_ns == ready_ns
    assert visible.deep_book["feature_ready_ts_ns"] == ready_ns
    assert visible.generation == visible.deep_book["generation"]
    assert paths[0].generation == visible.generation
    assert paths[0].feature_ready_ts_ns == ready_ns
    assert paths[0].activation_ts_ns == ready_ns
    assert paths[0].receive_ts_ns <= ready_ns
    book.stop()


def test_ws_handler_stale_deep_book_schedules_independent_reconnect(monkeypatch):
    cfg = Config()
    cfg.websocket.deep_book_enabled = True
    cfg.websocket.deep_book_max_age_s = 0.001
    rest = _SnapshotRest([_snapshot()])
    handler = WSHandler(SimpleNamespace(signal=SimpleNamespace()), cfg)
    handler._running = True
    handler._rest_client = rest
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(
        lambda: (
            book.snapshot()["valid"] == 1
            and book.snapshot()["syncing"] == 0
        )
    )
    with handler._deep_book_lock:
        handler._deep_book = book
        handler._ws_deep = object()

    reconnects = []
    monkeypatch.setattr(
        handler,
        "_schedule_deep_book_reconnect",
        lambda: reconnects.append(True),
    )
    handler.maintain_deep_book(now_ns=time.time_ns() + 2_000_000_000)

    assert reconnects == [True]
    assert handler.deep_book_snapshot()["valid"] == 0
    assert handler.deep_book_snapshot()["stale_restart_count"] == 1
    book.stop()


def test_ws_handler_deep_stream_error_invalidates_and_reconnects(monkeypatch):
    cfg = Config()
    cfg.websocket.deep_book_enabled = True
    rest = _SnapshotRest([_snapshot()])
    handler = WSHandler(SimpleNamespace(signal=SimpleNamespace()), cfg)
    handler._running = True
    handler._rest_client = rest
    book = BinanceUsdMDeepBook(
        rest,
        symbol="BTCUSDC",
        tick_size=0.1,
        snapshot_levels=5,
    )
    book.start()
    _wait_until(lambda: book.snapshot()["valid"] == 1)
    with handler._deep_book_lock:
        handler._deep_book = book

    reconnects = []
    monkeypatch.setattr(
        handler,
        "_schedule_deep_book_reconnect",
        lambda: reconnects.append(True),
    )
    handler._on_deep_book_message(
        None,
        json.dumps({"error": {"code": 1, "msg": "bad subscription"}}),
    )

    assert reconnects == [True]
    assert handler.deep_book_snapshot()["valid"] == 0
    book.stop()


def test_deep_book_config_is_strictly_validated():
    cfg = Config()
    cfg.websocket.deep_book_snapshot_levels = 250
    with pytest.raises(ValueError, match="snapshot_levels"):
        _validate_config(cfg)

    cfg.websocket.deep_book_snapshot_levels = 1000
    cfg.websocket.deep_book_speed = 1_000
    with pytest.raises(ValueError, match="deep_book_speed"):
        _validate_config(cfg)

    cfg.websocket.deep_book_speed = 100
    cfg.strategy.dynamic_fill_hazard_shadow_enabled = True
    cfg.websocket.deep_book_enabled = False
    with pytest.raises(ValueError, match="requires websocket.deep_book_enabled"):
        _validate_config(cfg)

    cfg.websocket.deep_book_enabled = True
    cfg.strategy.dynamic_fill_hazard_shadow_model_path = "model.json"
    cfg.strategy.dynamic_fill_hazard_shadow_model_sha256 = "a" * 64
    cfg.strategy.dynamic_fill_hazard_action_enabled = True
    with pytest.raises(ValueError, match="requires a policy artifact"):
        _validate_config(cfg)
