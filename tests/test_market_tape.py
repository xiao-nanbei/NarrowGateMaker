import gzip
import json
import os
import time
from types import SimpleNamespace

import numpy as np
import pytest

from live.config import Config
from live.venues.common import MARKET_TAPE_SCHEMA_VERSION, normalize_market_tape_row
from live.ws_handler import WSHandler
from research.system_engineering.audit.receive_time_tape import (
    latency_distribution,
    leader_survival,
)


class _FakeFuturesMarketWebSocket:
    def __init__(self):
        self.agg_trade_symbols = []
        self.raw_subscriptions = []
        self.list_requests = []

    def agg_trade(self, *, symbol, id, **_kwargs):
        self.agg_trade_symbols.append((symbol, id))

    def subscribe(self, stream, *, id):
        self.raw_subscriptions.append((stream, id))

    def list_subscribe(self, *, id):
        self.list_requests.append(id)


class _UnexpectedLock:
    def __enter__(self):
        raise AssertionError("disabled deep book must not touch the hot-path lock")

    def __exit__(self, *_args):
        return False


def test_usdm_market_subscription_never_falls_back_to_raw_trade():
    cfg = Config()
    handler = WSHandler(SimpleNamespace(), cfg)
    fake_ws = _FakeFuturesMarketWebSocket()
    handler._ws_market = fake_ws

    handler._subscribe_market_streams("btcusdc", ["btcusdt"])

    assert [symbol for symbol, _ in fake_ws.agg_trade_symbols] == [
        "btcusdc",
        "btcusdt",
    ]
    assert fake_ws.raw_subscriptions == []


def test_stream_watchdog_ignores_trade_and_anchor_event_silence():
    cfg = Config()
    handler = WSHandler(SimpleNamespace(), cfg)
    now = 1_000.0
    handler._market_trade_seen = {"btcusdc": 0.0, "btcusdt": 0.0}
    handler._market_book_seen = {"btcusdc": now, "btcusdt": 0.0}
    handler._market_depth_seen = {"btcusdc": now, "btcusdt": 0.0}
    handler._spot_trade_seen = {"btcusdt": 0.0}
    handler._spot_book_seen = {"btcusdt": 0.0}

    assert handler._execution_stream_silence_reasons(
        market_symbols=["btcusdc", "btcusdt"],
        now_ts=now,
    ) == []


def test_stream_watchdog_restarts_for_periodic_execution_depth_silence():
    cfg = Config()
    handler = WSHandler(SimpleNamespace(), cfg)
    handler._market_depth_seen = {"btcusdc": 900.0}

    assert handler._execution_stream_silence_reasons(
        market_symbols=["btcusdc", "btcusdt"],
        now_ts=1_000.0,
    ) == ["btcusdc@executionDepth 100s"]


def test_execution_depth_message_refreshes_watchdog_transport_clock():
    cfg = Config()
    signal = SimpleNamespace(on_depth=lambda *_args, **_kwargs: None)
    handler = WSHandler(SimpleNamespace(signal=signal), cfg)
    handler._market_depth_seen = {"btcusdc": 0.0}

    handler._on_market_message(
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

    assert handler._market_depth_seen["btcusdc"] > 0.0


def test_execution_trade_skips_deep_book_lock_when_collection_is_disabled():
    cfg = Config()
    cfg.websocket.deep_book_enabled = False
    signal = SimpleNamespace(on_agg_trade=lambda *_args, **_kwargs: None)
    inventory = SimpleNamespace(update_mark_price=lambda *_args, **_kwargs: None)
    engine = SimpleNamespace(signal=signal, inventory=inventory)
    handler = WSHandler(engine, cfg)
    handler._deep_book_lock = _UnexpectedLock()

    handler._on_market_message(
        None,
        {
            "e": "aggTrade",
            "s": "BTCUSDC",
            "E": 1,
            "a": 2,
            "p": "100.0",
            "q": "0.01",
            "m": False,
        },
    )

    assert handler._exec_trade_count == 1


def test_book_ticker_cannot_mask_execution_depth_transport_silence():
    cfg = Config()
    signal = SimpleNamespace(on_book_ticker=lambda *_args, **_kwargs: None)
    inventory = SimpleNamespace(update_mark_price=lambda *_args, **_kwargs: None)
    engine = SimpleNamespace(
        signal=signal,
        inventory=inventory,
        _best_bid=0.0,
        _best_ask=0.0,
    )
    handler = WSHandler(engine, cfg)
    handler._market_depth_seen = {"btcusdc": 900.0}

    handler._on_market_message(
        None,
        {
            "e": "bookTicker",
            "s": "BTCUSDC",
            "E": 999_999,
            "u": 1,
            "b": "100.0",
            "a": "100.1",
        },
    )

    assert handler._market_book_seen["btcusdc"] > 0.0
    assert handler._market_depth_seen["btcusdc"] == 900.0
    assert handler._execution_stream_silence_reasons(
        market_symbols=["btcusdc"],
        now_ts=1_000.0,
    ) == ["btcusdc@executionDepth 100s"]


def test_market_tape_row_separates_transport_and_feature_latency():
    row = normalize_market_tape_row(
        {
            "market_id": "okx:perp:BTCUSDT",
            "transport": "rest",
            "event_type": "trade",
            "exchange_event_ts_ns": 1_000_000_000,
            "local_receive_ts_ns": 1_010_000_000,
            "side": "sell",
            "price": 60_000.0,
            "size": 0.01,
        },
        feature_ready_ts_ns=1_012_500_000,
    )

    assert row["schema_version"] == MARKET_TAPE_SCHEMA_VERSION
    assert row["transport_lag_ms"] == pytest.approx(10.0)
    assert row["feature_latency_us"] == pytest.approx(2_500.0)
    assert row["aggressor_side"] == "sell"
    assert row["gap_flag"] is None
    assert row["event_timestamp_source"] == "exchange"


def test_binance_receive_tape_writes_causal_rows_and_depth_gap(tmp_path):
    cfg = Config()
    cfg.logging.market_tape_enabled = True
    cfg.logging.market_tape_dir = str(tmp_path)
    cfg.logging.market_tape_record_depth = True
    cfg.logging.market_tape_book_interval_ms = 0.0
    handler = WSHandler(SimpleNamespace(), cfg)
    handler._start_market_tape()

    receive_ns = 1_800_000_000_000_000_000
    handler._record_binance_market_event(
        {
            "e": "bookTicker",
            "E": receive_ns // 1_000_000 - 5,
            "s": "BTCUSDC",
            "u": 90,
            "b": "60000.0",
            "B": "1.2",
            "a": "60000.1",
            "A": "0.8",
        },
        market_type="perp",
        event_type="book",
        receive_ts_ns=receive_ns,
        feature_ready_ts_ns=receive_ns + 250_000,
    )
    handler._record_binance_market_event(
        {
            "e": "aggTrade",
            "T": receive_ns // 1_000_000 - 3,
            "s": "BTCUSDT",
            "a": 91,
            "f": 801,
            "l": 803,
            "p": "60002.0",
            "q": "0.04",
            "nq": "0.03",
            "m": True,
        },
        market_type="perp",
        event_type="trade",
        receive_ts_ns=receive_ns + 1_000_000,
        feature_ready_ts_ns=receive_ns + 1_300_000,
    )
    for sequence, previous in ((100, 99), (101, 100), (103, 99)):
        handler._record_binance_market_event(
            {
                "e": "depthUpdate",
                "E": receive_ns // 1_000_000,
                "s": "BTCUSDC",
                "u": sequence,
                "pu": previous,
                "b": [["60000.0", "1.0"]],
                "a": [["60000.1", "2.0"]],
            },
            market_type="perp",
            event_type="depth",
            receive_ts_ns=receive_ns + sequence * 1_000_000,
            feature_ready_ts_ns=receive_ns + sequence * 1_000_000 + 100_000,
        )
    handler._stop_market_tape()

    paths = list(tmp_path.glob("binance_receive_tape_*.jsonl.gz"))
    assert len(paths) == 1
    with gzip.open(paths[0], "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]

    assert len(rows) == 5
    assert {row["schema_version"] for row in rows} == {MARKET_TAPE_SCHEMA_VERSION}
    assert rows[0]["market_id"] == "binance:perp:BTCUSDC"
    assert rows[0]["feature_ready_ts_ns"] > rows[0]["local_receive_ts_ns"]
    assert rows[1]["aggressor_side"] == "sell"
    assert rows[1]["trade_stream_type"] == "aggregate"
    assert rows[1]["trade_payload_schema_version"] == "binance_usdm_aggtrade.v2"
    assert rows[1]["trade_source_contract_id"] == (
        "binance_usdm_public_aggtrade_receive_time.v1"
    )
    assert rows[1]["aggregate_trade_id"] == 91
    assert rows[1]["first_trade_id"] == 801
    assert rows[1]["last_trade_id"] == 803
    assert rows[1]["individual_trade_count_from_id_range"] == 3
    assert rows[1]["normal_quantity"] == pytest.approx(0.03)
    depth = [row for row in rows if row["event_type"] == "depth"]
    assert [row["gap_flag"] for row in depth] == [None, 0, 1]
    assert depth[-1]["previous_sequence_number"] == 101

    latency = latency_distribution(paths)
    book = next(row for row in latency if row["event_type"] == "book")
    assert book["feature_latency_us_p50"] == pytest.approx(250.0)
    depth_latency = next(row for row in latency if row["event_type"] == "depth")
    assert depth_latency["gap_known"] == 2
    assert depth_latency["gap_count"] == 1


def test_leader_survival_separates_early_pending_from_later_follow():
    base = 1_800_000_000_000_000_000
    ms = 1_000_000
    series = {
        "binance:perp:BTCUSDT": (
            np.asarray([base, base + 100 * ms, base + 125 * ms, base + 350 * ms]),
            np.asarray([100.0, 100.0, 100.0, 101.0]),
        ),
        "okx:perp:BTCUSDT": (
            np.asarray([base, base + 100 * ms]),
            np.asarray([100.0, 101.0]),
        ),
    }

    rows = leader_survival(
        series,
        local_market_id="binance:perp:BTCUSDT",
        external_market_ids=["okx:perp:BTCUSDT"],
        horizons_ms=(25, 250),
        lookback_ms=100,
        shock_threshold_bps=1.0,
        max_book_age_ms=500,
    )

    early, late = rows
    assert early["events"] == 1
    assert early["pending_survival_50pct_rate"] == pytest.approx(1.0)
    assert late["events"] == 1
    assert late["absorbed_50pct_rate"] == pytest.approx(1.0)


def test_latency_profile_tolerates_only_an_active_gzip_tail(tmp_path):
    path = tmp_path / "active.jsonl.gz"
    row = {
        "market_id": "okx:perp:BTCUSDT",
        "transport": "websocket",
        "event_type": "trade",
        "local_receive_ts_ns": 1_800_000_000_000_000_000,
        "feature_ready_ts_ns": 1_800_000_000_001_000_000,
    }
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")
    with path.open("ab") as handle:
        handle.write(b"not-a-complete-gzip-member")

    assert latency_distribution([path])[0]["rows"] == 1

    old = time.time() - 60.0
    os.utime(path, (old, old))
    with pytest.raises((gzip.BadGzipFile, OSError, EOFError)):
        latency_distribution([path])
