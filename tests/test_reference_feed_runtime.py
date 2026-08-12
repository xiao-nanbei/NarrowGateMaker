"""Regression tests for synchronous reference-feed state and recording paths."""

import gzip
import time
from types import SimpleNamespace

from live.config import Config
from live.venues.common import DailyJsonlRecorder
from live.ws_handler import WSHandler
from strategy.signal import SignalEngine


def test_cross_trade_batch_matches_single_event_state(monkeypatch):
    monkeypatch.delenv("NARROWGATE_CPP_GLOBAL_FLOW", raising=False)
    single = SignalEngine(enable_ml=False)
    batched = SignalEngine(enable_ml=False)
    receive_ns = 1_800_000_000_500_000_000
    events = [
        {"s": "BTCUSDT", "T": 1_800_000_000_100, "p": "60000.0", "q": "0.1", "m": True},
        {"s": "BTCUSDT", "T": 1_800_000_000_200, "p": "60000.1", "q": "0.2", "m": False},
        {"s": "BTCUSDT", "T": 1_800_000_001_100, "p": "60000.2", "q": "0.3", "m": False},
    ]
    sequences = [10, 11, 12]
    for event, sequence in zip(events, sequences):
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
