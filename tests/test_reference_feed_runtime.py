"""Regression tests for synchronous reference-feed state and recording paths."""

import gzip
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from live.config import Config
from live.venues.common import DailyJsonlRecorder
from live.ws_handler import WSHandler
from strategy.signal import SignalEngine


class _FakeFuturesSocket:
    def __init__(self, owner, *, on_abort=None):
        self._owner = owner
        self._on_abort = on_abort
        self.shutdown_called = False

    def abort(self):
        self._owner.stopped = True
        if self._on_abort is not None:
            self._on_abort()

    def shutdown(self):
        self.shutdown_called = True


class _FakeFuturesSocketManager:
    def __init__(self, owner, *, on_abort=None, remains_alive=False):
        self.ws = _FakeFuturesSocket(owner, on_abort=on_abort)
        self._remains_alive = remains_alive
        self.join_timeouts = []

    def join(self, timeout=None):
        self.join_timeouts.append(timeout)

    def is_alive(self):
        return self._remains_alive


class _FakeFuturesClient:
    def __init__(self, *, on_abort=None, remains_alive=False):
        self.stopped = False
        self.socket_manager = _FakeFuturesSocketManager(
            self,
            on_abort=on_abort,
            remains_alive=remains_alive,
        )


def test_cross_trade_batch_matches_single_event_state(monkeypatch):
    monkeypatch.delenv("NARROWGATE_CPP_GLOBAL_FLOW", raising=False)
    single = SignalEngine(enable_ml=False, global_flow_shadow_enabled=True)
    batched = SignalEngine(enable_ml=False, global_flow_shadow_enabled=True)
    receive_ns = 1_800_000_000_500_000_000
    events = [
        {"s": "BTCUSDT", "T": 1_800_000_000_100, "p": "60000.0", "q": "0.1", "m": True},
        {"s": "BTCUSDT", "T": 1_800_000_000_200, "p": "60000.1", "q": "0.2", "m": False},
        {"s": "BTCUSDT", "T": 1_800_000_001_100, "p": "60000.2", "q": "0.3", "m": False},
    ]
    sequences = [10, 11, 12]
    for event, sequence in zip(events, sequences, strict=True):
        single.on_cross_agg_trade(
            event,
            market_type="perp",
            venue="bybit",
            receive_ts_ns=receive_ns,
            sequence_number=sequence,
        )
    batched.on_cross_agg_trade_batch(
        events,
        market_type="perp",
        venue="bybit",
        receive_ts_ns=receive_ns,
        sequence_numbers=sequences,
    )

    key = "bybit:perp:BTCUSDT"
    assert single._cross_current_bars[key] == batched._cross_current_bars[key]
    assert list(single._cross_bar_buffers[key]) == list(batched._cross_bar_buffers[key])
    assert single.market_source_snapshot(now_ns=receive_ns + 1_000_000)[key] == (
        batched.market_source_snapshot(now_ns=receive_ns + 1_000_000)[key]
    )
    single_flow = single._global_flow.market_window(
        key, now_ns=receive_ns + 1_000_000, horizon_ms=1_000
    )
    batched_flow = batched._global_flow.market_window(
        key, now_ns=receive_ns + 1_000_000, horizon_ms=1_000
    )
    assert single_flow == batched_flow


def test_daily_recorder_reports_bounded_queue_metrics(tmp_path):
    recorder = DailyJsonlRecorder(
        tmp_path,
        file_prefix="queue-metrics",
        thread_name="test-queue-metrics-writer",
        queue_size=2,
        compress=False,
    )
    recorder.start()
    recorder.submit(
        {
            "market_id": "okx:perp:BTCUSDT",
            "event_type": "trade",
            "exchange_event_ts_ns": 1_800_000_000_000_000_000,
            "local_receive_ts_ns": 1_800_000_000_010_000_000,
            "price": 60_000.0,
            "size": 0.01,
            "side": "buy",
        }
    )
    recorder.stop()
    snapshot = recorder.snapshot()

    assert snapshot["submitted"] == 1
    assert snapshot["written"] == 1
    assert snapshot["dropped"] == 0
    assert snapshot["queue_capacity"] == 2
    assert snapshot["queue_high_watermark"] >= 1
    assert snapshot["max_queue_age_ms"] >= 0.0


def test_daily_recorder_rotates_per_session_after_a_bad_member(tmp_path):
    def write_once(receive_ns):
        recorder = DailyJsonlRecorder(
            tmp_path,
            file_prefix="session-tape",
            thread_name="test-session-tape",
            compress=True,
        )
        recorder.start()
        recorder.submit(
            {
                "market_id": "okx:perp:BTCUSDT",
                "event_type": "trade",
                "local_receive_ts_ns": receive_ns,
                "price": 60_000.0,
                "size": 0.01,
                "side": "buy",
            }
        )
        recorder.stop()

    receive_ns = 1_800_000_000_000_000_000
    write_once(receive_ns)
    first = next(tmp_path.glob("session-tape_*.jsonl.gz"))
    with first.open("ab") as handle:
        handle.write(b"forced-stop-tail")

    time.sleep(0.001)
    write_once(receive_ns + 1_000_000)
    paths = sorted(tmp_path.glob("session-tape_*.jsonl.gz"))

    assert len(paths) == 2
    with gzip.open(paths[-1], "rt", encoding="utf-8") as handle:
        assert sum(1 for _ in handle) == 1


def test_ws_handler_stops_external_sources_concurrently():
    stopped = []

    class SlowClient:
        def __init__(self, market_id):
            self.market_id = market_id

        def stop(self):
            time.sleep(0.1)
            stopped.append(self.market_id)

    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    handler._external_clients = [
        SlowClient("bitget:perp:BTCUSDT"),
        SlowClient("bybit:perp:BTCUSDT"),
        SlowClient("okx:perp:BTCUSDT"),
    ]

    started = time.monotonic()
    handler._stop_external_venue_streams()
    elapsed = time.monotonic() - started

    assert sorted(stopped) == [
        "bitget:perp:BTCUSDT",
        "bybit:perp:BTCUSDT",
        "okx:perp:BTCUSDT",
    ]
    assert elapsed < 0.22


def test_ws_handler_private_first_start_keeps_all_market_producers_dormant(
    monkeypatch,
):
    events = []
    websocket_clients = []

    class FakeWebSocketClient(_FakeFuturesClient):
        def __init__(self, **kwargs):
            super().__init__()
            self.kwargs = kwargs
            websocket_clients.append(self)

    monkeypatch.setattr(
        "binance.websocket.um_futures.websocket_client.UMFuturesWebsocketClient",
        FakeWebSocketClient,
    )
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    rest = object()
    listen_key_client = object()
    market_snapshot_client = object()
    monkeypatch.setattr(
        handler,
        "_start_user_stream",
        lambda client: events.append(("private", client)) or True,
    )
    monkeypatch.setattr(
        handler,
        "_ensure_listen_key_renewal_thread",
        lambda client: events.append(("renew", client)),
    )
    monkeypatch.setattr(
        handler,
        "_start_market_tape",
        lambda: events.append(("market_tape", None)),
    )
    monkeypatch.setattr(
        handler,
        "_subscribe_market_streams",
        lambda *_args: events.append(("market_subscriptions", None)),
    )
    monkeypatch.setattr(
        handler,
        "_subscribe_public_streams",
        lambda *_args: events.append(("public_subscriptions", None)),
    )
    monkeypatch.setattr(
        handler,
        "_start_deep_book_stream",
        lambda: events.append(("deep_book", None)) or True,
    )
    monkeypatch.setattr(
        handler,
        "_start_spot_stream",
        lambda _symbols: events.append(("spot", None)),
    )
    monkeypatch.setattr(
        handler,
        "_start_external_venue_streams",
        lambda: events.append(("external", None)),
    )
    monkeypatch.setattr(
        handler,
        "_arm_stream_silence_watchdog",
        lambda *_args: events.append(("watchdog", None)),
    )

    with pytest.raises(
        RuntimeError,
        match="private user stream must start before public market streams",
    ):
        handler.start_public_market_streams(
            rest,
            market_snapshot_client=market_snapshot_client,
            expected_user_stream_generation=1,
        )
    assert websocket_clients == []

    handler.start_private_user_stream(
        rest,
        listen_key_client=listen_key_client,
    )

    assert events == [
        ("private", listen_key_client),
        ("renew", listen_key_client),
    ]
    assert websocket_clients == []
    assert handler._private_user_stream_started is True
    assert handler._public_market_streams_started is False
    assert handler._market_snapshot_client is None

    with pytest.raises(
        RuntimeError,
        match="admitted private-stream generation",
    ):
        handler.start_public_market_streams(
            rest,
            market_snapshot_client=market_snapshot_client,
            expected_user_stream_generation=1,
        )
    with handler._user_event_stats_lock:
        handler._user_stream_connected = True
        handler._user_stream_generation = 1

    handler.start_public_market_streams(
        rest,
        market_snapshot_client=market_snapshot_client,
        expected_user_stream_generation=1,
    )
    handler.start_private_user_stream(
        rest,
        listen_key_client=listen_key_client,
    )
    handler.start_public_market_streams(
        rest,
        market_snapshot_client=market_snapshot_client,
        expected_user_stream_generation=1,
    )

    assert len(websocket_clients) == 2
    assert events == [
        ("private", listen_key_client),
        ("renew", listen_key_client),
        ("market_tape", None),
        ("market_subscriptions", None),
        ("public_subscriptions", None),
        ("deep_book", None),
        ("spot", None),
        ("external", None),
        ("watchdog", None),
        ("renew", listen_key_client),
    ]
    assert handler._market_snapshot_client is market_snapshot_client
    assert handler._public_market_streams_started is True

    with pytest.raises(RuntimeError, match="listen-key client is already bound"):
        handler.start_private_user_stream(rest, listen_key_client=object())
    with pytest.raises(RuntimeError, match="market snapshot client is already bound"):
        handler.start_public_market_streams(
            rest,
            market_snapshot_client=object(),
            expected_user_stream_generation=1,
        )


def test_ws_handler_legacy_start_preserves_all_stream_compatibility(monkeypatch):
    handler = WSHandler(SimpleNamespace(), Config())
    calls = []
    rest = object()
    market_snapshot_client = object()
    listen_key_client = object()
    monkeypatch.setattr(
        handler,
        "start_public_market_streams",
        lambda observed_rest, **kwargs: calls.append(
            ("public", observed_rest, kwargs)
        ),
    )
    monkeypatch.setattr(
        handler,
        "start_private_user_stream",
        lambda observed_rest, **kwargs: calls.append(
            ("private", observed_rest, kwargs)
        ),
    )
    monkeypatch.setattr(handler, "wait_for_user_stream_ready", lambda _timeout: True)
    monkeypatch.setattr(
        handler,
        "user_event_safety_snapshot",
        lambda: {
            "user_stream_connected": True,
            "user_stream_generation": 7,
        },
    )

    handler.start(
        rest,
        market_snapshot_client=market_snapshot_client,
        listen_key_client=listen_key_client,
    )

    assert calls == [
        (
            "private",
            rest,
            {"listen_key_client": listen_key_client},
        ),
        (
            "public",
            rest,
            {
                "market_snapshot_client": market_snapshot_client,
                "expected_user_stream_generation": 7,
            },
        ),
    ]


def test_ws_handler_public_start_failure_keeps_admitted_private_stream_running(
    monkeypatch,
):
    clients = []
    close_threads = []

    class FakeWebSocketClient(_FakeFuturesClient):
        def __init__(self, **kwargs):
            self.on_close = kwargs["on_close"]

            def on_abort():
                thread = threading.Thread(target=self.on_close, args=(None,))
                close_threads.append(thread)
                thread.start()
                # A real connector stop joins its socket-manager thread.  This
                # bounded join proves close callbacks do not wait on the broader
                # startup lock held by the cleanup path.
                thread.join(timeout=0.2)

            super().__init__(on_abort=on_abort)
            clients.append(self)

    monkeypatch.setattr(
        "binance.websocket.um_futures.websocket_client.UMFuturesWebsocketClient",
        FakeWebSocketClient,
    )
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    rest = object()
    listen_key_client = object()
    monkeypatch.setattr(handler, "_start_user_stream", Mock(return_value=True))
    monkeypatch.setattr(handler, "_ensure_listen_key_renewal_thread", Mock())
    monkeypatch.setattr(handler, "_start_market_tape", Mock())
    monkeypatch.setattr(handler, "_subscribe_market_streams", Mock())
    monkeypatch.setattr(
        handler,
        "_subscribe_public_streams",
        Mock(side_effect=RuntimeError("public subscription failed")),
    )

    handler.start_private_user_stream(rest, listen_key_client=listen_key_client)
    with handler._user_event_stats_lock:
        handler._user_stream_connected = True
        handler._user_stream_generation = 1
    with pytest.raises(RuntimeError, match="public subscription failed"):
        handler.start_public_market_streams(
            rest,
            market_snapshot_client=object(),
            expected_user_stream_generation=1,
        )

    assert handler._running is True
    assert handler._private_user_stream_started is True
    assert handler._public_market_streams_started is False
    assert handler._ws_market is None
    assert handler._ws_public is None
    assert len(clients) == 2
    assert all(client.stopped for client in clients)
    assert close_threads
    assert all(not thread.is_alive() for thread in close_threads)


def test_ws_handler_public_close_during_startup_fails_without_reconnect(
    monkeypatch,
):
    clients = []

    class ClosingWebSocketClient(_FakeFuturesClient):
        def __init__(self, **kwargs):
            super().__init__()
            clients.append(self)
            kwargs["on_close"](None)

    monkeypatch.setattr(
        "binance.websocket.um_futures.websocket_client.UMFuturesWebsocketClient",
        ClosingWebSocketClient,
    )
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    rest = object()
    monkeypatch.setattr(handler, "_start_user_stream", Mock(return_value=True))
    monkeypatch.setattr(handler, "_ensure_listen_key_renewal_thread", Mock())
    monkeypatch.setattr(handler, "_start_market_tape", Mock())
    monkeypatch.setattr(handler, "_subscribe_market_streams", Mock())
    monkeypatch.setattr(handler, "_subscribe_public_streams", Mock())
    monkeypatch.setattr(handler, "_start_deep_book_stream", Mock(return_value=True))
    monkeypatch.setattr(handler, "_start_spot_stream", Mock())
    monkeypatch.setattr(handler, "_start_external_venue_streams", Mock())
    monkeypatch.setattr(handler, "_arm_stream_silence_watchdog", Mock())

    reconnect = Mock()
    monkeypatch.setattr(handler, "_reconnect_market", reconnect)

    handler.start_private_user_stream(rest, listen_key_client=object())
    with handler._user_event_stats_lock:
        handler._user_stream_connected = True
        handler._user_stream_generation = 1
    with pytest.raises(RuntimeError, match="closed during startup"):
        handler.start_public_market_streams(
            rest,
            market_snapshot_client=object(),
            expected_user_stream_generation=1,
        )

    reconnect.assert_not_called()
    assert handler._public_market_streams_starting is False
    assert handler._public_market_streams_started is False
    assert all(client.stopped for client in clients)


def test_ws_handler_close_at_public_activation_boundary_reconnects(
    monkeypatch,
):
    clients = []

    class ActiveWebSocketClient(_FakeFuturesClient):
        def __init__(self, **_kwargs):
            super().__init__()
            clients.append(self)

    monkeypatch.setattr(
        "binance.websocket.um_futures.websocket_client.UMFuturesWebsocketClient",
        ActiveWebSocketClient,
    )
    monkeypatch.setattr("live.ws_handler.time.sleep", lambda _seconds: None)
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    rest = object()
    monkeypatch.setattr(handler, "_start_user_stream", Mock(return_value=True))
    monkeypatch.setattr(handler, "_ensure_listen_key_renewal_thread", Mock())
    monkeypatch.setattr(handler, "_start_market_tape", Mock())
    monkeypatch.setattr(handler, "_subscribe_market_streams", Mock())
    monkeypatch.setattr(handler, "_subscribe_public_streams", Mock())
    monkeypatch.setattr(handler, "_start_deep_book_stream", Mock(return_value=True))
    monkeypatch.setattr(handler, "_start_spot_stream", Mock())
    monkeypatch.setattr(handler, "_start_external_venue_streams", Mock())
    monkeypatch.setattr(handler, "_arm_stream_silence_watchdog", Mock())

    replacements = (_FakeFuturesClient(), _FakeFuturesClient())
    reconnect = Mock(return_value=replacements)
    monkeypatch.setattr(handler, "_reconnect_market", reconnect)

    handler.start_private_user_stream(rest, listen_key_client=object())
    with handler._user_event_stats_lock:
        handler._user_stream_connected = True
        handler._user_stream_generation = 1
    handler.start_public_market_streams(
        rest,
        market_snapshot_client=object(),
        expected_user_stream_generation=1,
    )
    handler._on_market_close(None)
    deadline = time.monotonic() + 1.0
    while not reconnect.called and time.monotonic() < deadline:
        time.sleep(0.001)

    reconnect.assert_called_once()
    assert reconnect.call_args.kwargs["session_id"] > 0
    assert all(client.stopped for client in clients)
    assert handler._ws_market is replacements[0]
    assert handler._ws_public is replacements[1]
    assert handler._public_market_streams_started is True


def test_market_reconnect_is_single_flight_and_stops_both_old_clients_first(
    monkeypatch,
):
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    handler._running = True
    handler._public_market_streams_started = True
    handler._market_session_id = 7
    stop_threads = []

    old_market = _FakeFuturesClient(
        on_abort=lambda: stop_threads.append(threading.current_thread().name)
    )
    old_public = _FakeFuturesClient(
        on_abort=lambda: stop_threads.append(threading.current_thread().name)
    )
    handler._ws_market = old_market
    handler._ws_public = old_public
    build_entered = threading.Event()
    release_build = threading.Event()
    builds = []

    replacements = (_FakeFuturesClient(), _FakeFuturesClient())

    def rebuild(*, session_id):
        assert old_market.stopped is True
        assert old_public.stopped is True
        builds.append(session_id)
        build_entered.set()
        assert release_build.wait(timeout=1.0)
        return replacements

    monkeypatch.setattr(handler, "_reconnect_market", rebuild)

    handler._on_market_close(None, session_id=7)
    assert build_entered.wait(timeout=1.0)
    handler._on_public_close(None, session_id=7)
    release_build.set()
    deadline = time.monotonic() + 1.0
    while handler._market_reconnect_requested and time.monotonic() < deadline:
        time.sleep(0.001)

    assert builds == [8]
    assert stop_threads == [
        "futures-market-reconnect",
        "futures-market-reconnect",
    ]
    assert handler._ws_market is replacements[0]
    assert handler._ws_public is replacements[1]
    assert handler._market_reconnect_requested is False


def test_old_market_session_message_is_ignored_after_reconnect_generation():
    signal = SimpleNamespace(on_depth=Mock())
    handler = WSHandler(SimpleNamespace(signal=signal), Config())
    handler._running = True
    handler._market_session_id = 4
    handler._market_depth_seen = {"btcusdc": 0.0}
    payload = {
        "e": "depthUpdate",
        "s": "BTCUSDC",
        "E": 1,
        "u": 2,
        "pu": 1,
        "b": [],
        "a": [],
    }

    handler._on_market_message(None, payload, session_id=3)

    signal.on_depth.assert_not_called()
    assert handler._market_depth_seen["btcusdc"] == 0.0


def test_market_reconnect_stop_failure_does_not_build_replacement(monkeypatch):
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    handler._running = True
    handler._public_market_streams_started = True
    handler._market_session_id = 9

    public = _FakeFuturesClient()
    handler._ws_market = _FakeFuturesClient(remains_alive=True)
    handler._ws_public = public
    rebuild = Mock()
    monkeypatch.setattr(handler, "_reconnect_market", rebuild)

    assert handler._request_market_reconnect("test_stop_failure") is True
    deadline = time.monotonic() + 1.0
    while (
        handler._public_market_streams_started
        or handler._market_reconnect_requested
    ) and time.monotonic() < deadline:
        time.sleep(0.001)

    assert public.stopped is True
    rebuild.assert_not_called()
    assert handler._ws_market is not None
    assert handler._ws_public is public
    assert handler._public_market_streams_started is False
    assert handler._market_session_id == 10


def test_replacement_socket_close_during_build_leaves_streams_stopped(monkeypatch):
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    handler._running = True
    handler._public_market_streams_started = True
    handler._market_session_id = 2

    replacements = (_FakeFuturesClient(), _FakeFuturesClient())

    def closes_while_building(*, session_id):
        handler._on_public_close(None, session_id=session_id)
        return replacements

    monkeypatch.setattr(handler, "_reconnect_market", closes_while_building)

    assert handler._request_market_reconnect("test_build_close") is True
    deadline = time.monotonic() + 1.0
    while (
        handler._public_market_streams_started
        or handler._market_reconnect_requested
    ) and time.monotonic() < deadline:
        time.sleep(0.001)

    assert handler._public_market_streams_started is False
    assert handler._market_reconnect_requested is False
    assert handler._ws_market is None
    assert handler._ws_public is None
    assert all(client.stopped for client in replacements)


def test_market_reconnect_second_constructor_failure_stops_first_client(
    monkeypatch,
):
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    handler._running = True
    handler._public_market_streams_started = True
    created = []

    def constructor(**_kwargs):
        if created:
            raise RuntimeError("second constructor failed")
        client = _FakeFuturesClient()
        created.append(client)
        return client

    monkeypatch.setattr(
        "binance.websocket.um_futures.websocket_client.UMFuturesWebsocketClient",
        constructor,
    )

    assert handler._request_market_reconnect("constructor_failure") is True
    deadline = time.monotonic() + 1.0
    while (
        handler._public_market_streams_started
        or handler._market_reconnect_requested
    ) and time.monotonic() < deadline:
        time.sleep(0.001)

    assert len(created) == 1
    assert created[0].stopped is True
    assert handler._ws_market is None
    assert handler._ws_public is None
    assert handler._public_market_streams_started is False


def test_failed_replacement_cleanup_retains_but_fences_live_reader(monkeypatch):
    signal = SimpleNamespace(on_depth=Mock())
    handler = WSHandler(SimpleNamespace(signal=signal), Config())
    handler._running = True
    handler._public_market_streams_started = True
    created = []

    def constructor(**kwargs):
        if created:
            raise RuntimeError("second constructor failed")
        client = _FakeFuturesClient(remains_alive=True)
        client.kwargs = kwargs
        created.append(client)
        return client

    monkeypatch.setattr(
        "binance.websocket.um_futures.websocket_client.UMFuturesWebsocketClient",
        constructor,
    )

    assert handler._request_market_reconnect("cleanup_failure") is True
    deadline = time.monotonic() + 1.0
    while (
        handler._public_market_streams_started
        or handler._market_reconnect_requested
    ) and time.monotonic() < deadline:
        time.sleep(0.001)

    assert handler._ws_market is created[0]
    assert handler._public_market_streams_started is False
    created[0].kwargs["on_message"](
        None,
        {
            "e": "depthUpdate",
            "s": "BTCUSDC",
            "E": 1,
            "u": 2,
            "pu": 1,
            "b": [],
            "a": [],
        },
    )
    signal.on_depth.assert_not_called()


def test_market_client_shutdown_has_bounded_reader_join_and_retains_reference():
    handler = WSHandler(SimpleNamespace(signal=object()), Config())

    class Socket:
        def __init__(self):
            self.aborted = False
            self.shutdown_called = False

        def abort(self):
            self.aborted = True

        def shutdown(self):
            self.shutdown_called = True

    class Manager:
        def __init__(self):
            self.ws = Socket()
            self.join_timeouts = []

        def join(self, timeout=None):
            self.join_timeouts.append(timeout)

        def is_alive(self):
            return True

    client = SimpleNamespace(socket_manager=Manager())
    handler._ws_market = client

    with pytest.raises(RuntimeError, match="callback quiescence"):
        handler.stop()

    assert client.socket_manager.ws.aborted is True
    assert client.socket_manager.ws.shutdown_called is True
    assert client.socket_manager.join_timeouts == [2.0]
    assert handler._ws_market is client


def test_market_client_without_sdk_socket_manager_is_rejected():
    with pytest.raises(TypeError, match="must expose its socket_manager"):
        WSHandler._stop_market_client(SimpleNamespace(stop=Mock()))


def test_stop_while_market_reconnect_builds_does_not_publish_new_clients(
    monkeypatch,
):
    handler = WSHandler(SimpleNamespace(signal=object()), Config())
    handler._running = True
    handler._public_market_streams_started = True

    old_clients = (_FakeFuturesClient(), _FakeFuturesClient())
    replacements = (_FakeFuturesClient(), _FakeFuturesClient())
    handler._ws_market, handler._ws_public = old_clients
    build_entered = threading.Event()
    release_build = threading.Event()

    def rebuild(*, session_id):
        assert session_id > 0
        build_entered.set()
        assert release_build.wait(timeout=1.0)
        return replacements

    monkeypatch.setattr(handler, "_reconnect_market", rebuild)
    assert handler._request_market_reconnect("stop_race") is True
    assert build_entered.wait(timeout=1.0)

    stop_thread = threading.Thread(target=handler.stop)
    stop_thread.start()
    deadline = time.monotonic() + 1.0
    while handler._running and time.monotonic() < deadline:
        time.sleep(0.001)
    release_build.set()
    stop_thread.join(timeout=1.0)

    assert not stop_thread.is_alive()
    assert all(client.stopped for client in old_clients)
    assert all(client.stopped for client in replacements)
    assert handler._ws_market is None
    assert handler._ws_public is None
    assert handler._running is False


def test_spot_subscription_hot_reload_is_rejected_before_config_mutation():
    previous = Config()
    candidate = Config()
    candidate.multi_market.enabled = True
    candidate.multi_market.market_stage = "enhanced"
    handler = WSHandler(SimpleNamespace(signal=object()), previous)
    handler._running = True
    handler._public_market_streams_started = True

    with pytest.raises(ValueError, match="spot market subscriptions are restart-only"):
        handler.on_config_reload(previous, candidate)

    assert handler.cfg is previous


def test_testnet_hot_reload_is_rejected_before_config_mutation():
    previous = Config()
    candidate = Config()
    candidate.api.testnet = not previous.api.testnet
    handler = WSHandler(SimpleNamespace(signal=object()), previous)
    handler._running = True

    with pytest.raises(ValueError, match="api.testnet is restart-only"):
        handler.on_config_reload(previous, candidate)

    assert handler.cfg is previous


def test_ws_handler_private_callback_waits_wholly_behind_startup_barrier():
    orders = SimpleNamespace(on_order_update=Mock())
    handler = WSHandler(SimpleNamespace(orders=orders), Config())
    ws_app = object()
    with handler._user_event_stats_lock:
        handler._running = True
        handler._user_stream_active = True
        handler._ws_user = ws_app
        handler._user_stream_session_token = 11
        handler._user_stream_connected = True
        handler._user_stream_generation = 3

    payload = {
        "e": "ORDER_TRADE_UPDATE",
        "o": {"c": "cid-1", "X": "FILLED"},
    }
    worker = threading.Thread(
        target=handler._on_user_message,
        args=(ws_app, payload, 11),
    )
    with handler.hold_user_event_callbacks():
        worker.start()
        time.sleep(0.02)
        assert worker.is_alive()
        assert handler.user_event_safety_snapshot()["user_event_count"] == 0
        orders.on_order_update.assert_not_called()

    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert handler.user_event_safety_snapshot()["user_event_count"] == 1
    orders.on_order_update.assert_called_once()


def test_signal_metrics_polling_is_quiet_until_explicit_admission(monkeypatch):
    class MetricsClient:
        def __init__(self):
            self.calls = []

        def open_interest(self, **_kwargs):
            self.calls.append("oi")
            return {"openInterest": "12", "time": 1_700_000_000_000}

        def top_long_short_position_ratio(self, **_kwargs):
            self.calls.append("top")
            return [{"longShortRatio": "1.1", "timestamp": 1_700_000_000_000}]

        def long_short_account_ratio(self, **_kwargs):
            self.calls.append("global")
            return [{"longShortRatio": "0.9", "timestamp": 1_700_000_000_000}]

        def taker_long_short_ratio(self, **_kwargs):
            self.calls.append("taker")
            return [{"buySellRatio": "1.2", "timestamp": 1_700_000_000_000}]

    timers = []

    class FakeTimer:
        def __init__(self, interval, callback, args=()):
            self.interval = interval
            self.callback = callback
            self.args = args
            self.daemon = False
            self.started = False
            self.canceled = False
            timers.append(self)

        def start(self):
            self.started = True

        def cancel(self):
            self.canceled = True

    monkeypatch.setattr("strategy.signal.threading.Timer", FakeTimer)
    client = MetricsClient()
    signal = SignalEngine(enable_ml=False, rest_client=client)

    assert client.calls == []
    assert timers == []
    assert signal.poll_metrics_once() is True
    assert client.calls == ["oi", "top", "global", "taker"]
    assert len(signal._metrics_history) == 1
    assert timers == []

    signal.start_metrics_polling()
    signal.start_metrics_polling()
    assert len(timers) == 1
    assert timers[0].started is True
    assert client.calls == ["oi", "top", "global", "taker"]

    signal.stop()
    assert timers[0].canceled is True
