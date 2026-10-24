from types import SimpleNamespace

import pytest

from live.config import _parse, _validate_config
from live.venues.bybit import (
    BybitPublicRestReferenceClient,
    BybitPublicWebSocketReferenceClient,
)
from market_fusion import BYBIT_VENUE, SPOT_MARKET


class _SignalProbe:
    def __init__(self):
        self.books = []
        self.trades = []

    def on_book_ticker(self, event, **kwargs):
        self.books.append((event, kwargs))

    def on_cross_trade_arrays(
        self, symbol, ts_ms, prices, quantities, is_buyer_maker, **kwargs
    ):
        sequences = kwargs.pop("sequence_numbers")
        for index, ts in enumerate(ts_ms):
            event = {
                "s": symbol,
                "T": int(ts),
                "p": str(prices[index]),
                "q": str(quantities[index]),
                "m": bool(is_buyer_maker[index]),
            }
            event_kwargs = dict(kwargs)
            event_kwargs["sequence_number"] = sequences[index]
            self.trades.append((event, event_kwargs))


def _cfg(tmp_path, *, instrument_type="perp", product_type="linear"):
    return SimpleNamespace(
        symbol="BTCUSDT",
        instrument_type=instrument_type,
        product_type=product_type,
        rest_url="https://api.bybit.com",
        poll_interval_ms=250.0,
        trade_poll_interval_ms=500.0,
        request_timeout_s=2.0,
        max_source_age_s=2.0,
        record_enabled=False,
        record_interval_ms=100.0,
        record_trades=True,
        record_dir=str(tmp_path),
    )


def test_bybit_external_config_accepts_public_market_transports():
    cfg = _parse(
        {
            "external_venues": {
                "enabled": True,
                "shadow_only": True,
                "sources": [
                    {
                        "venue": "bybit",
                        "enabled": True,
                        "transport": "rest",
                        "symbol": "BTCUSDT",
                        "product_type": "linear",
                    }
                ],
            }
        }
    )
    _validate_config(cfg)
    assert cfg.external_venues.sources[0].venue == "bybit"
    assert cfg.external_venues.sources[0].transport == "rest"

    cfg.external_venues.sources[0].transport = "websocket"
    _validate_config(cfg)

    cfg.external_venues.sources[0].transport = "invalid"
    with pytest.raises(ValueError, match="requires transport in"):
        _validate_config(cfg)


def test_bybit_rest_results_are_normalized_and_deduplicated(tmp_path):
    signal = _SignalProbe()
    client = BybitPublicRestReferenceClient(signal, _cfg(tmp_path), project_root=tmp_path)
    receive_ns = 1_700_000_000_250_000_000
    client.handle_book_result(
        {
            "s": "BTCUSDT",
            "a": [["60000.2", "2.5"]],
            "b": [["60000.1", "1.25"]],
            "ts": 1_700_000_000_205,
            "cts": 1_700_000_000_200,
            "u": 10,
            "seq": 123,
        },
        receive_ns=receive_ns,
        rtt_ms=40.0,
    )
    trades = {
        "category": "linear",
        "list": [
            {
                "execId": "trade-2",
                "symbol": "BTCUSDT",
                "price": "60000.2",
                "size": "0.02",
                "side": "Buy",
                "time": "1700000000210",
                "seq": "125",
            },
            {
                "execId": "trade-1",
                "symbol": "BTCUSDT",
                "price": "60000.1",
                "size": "0.01",
                "side": "Sell",
                "time": "1700000000205",
                "seq": "124",
            },
        ],
    }
    client.handle_trade_result(trades, receive_ns=receive_ns, rtt_ms=45.0)
    client.handle_trade_result(trades, receive_ns=receive_ns + 100_000_000, rtt_ms=45.0)

    assert len(signal.books) == 1
    book, book_kwargs = signal.books[0]
    assert book["b"] == "60000.1"
    assert book_kwargs["venue"] == BYBIT_VENUE
    assert book_kwargs["sequence_number"] == 123

    assert len(signal.trades) == 2
    first_trade, first_kwargs = signal.trades[0]
    assert first_trade["m"] is True
    assert first_kwargs["venue"] == BYBIT_VENUE
    assert first_kwargs["sequence_number"] == 124

    snapshot = client.snapshot(now_ns=receive_ns + 500_000_000)
    assert snapshot["market_id"] == "bybit:perp:BTCUSDT"
    assert snapshot["transport"] == "rest"
    assert snapshot["book_count"] == 1
    assert snapshot["trade_count"] == 2
    assert snapshot["book_event_age_ms"] == pytest.approx(550.0, abs=0.001)
    assert snapshot["book_transport_lag_ms"] == pytest.approx(50.0, abs=0.001)
    assert snapshot["stale"] == 0
    assert not hasattr(client, "new_order")


def test_bybit_spot_identity_and_request_category(tmp_path):
    signal = _SignalProbe()
    client = BybitPublicRestReferenceClient(
        signal,
        _cfg(tmp_path, instrument_type="spot", product_type="spot"),
        project_root=tmp_path,
    )
    assert client.market_id == "bybit:spot:BTCUSDT"
    assert client.category == "spot"
    client.handle_book_result(
        {
            "a": [["60000.2", "2"]],
            "b": [["60000.1", "1"]],
            "cts": 1_700_000_000_200,
            "seq": 12,
        },
        receive_ns=1_700_000_000_250_000_000,
    )
    assert signal.books[0][1]["market_type"] == SPOT_MARKET


def test_bybit_spot_websocket_payload_uses_same_normalization(tmp_path):
    signal = _SignalProbe()
    cfg = _cfg(tmp_path, instrument_type="spot", product_type="spot")
    cfg.websocket_url = "wss://stream.bybit.com/v5/public/spot"
    cfg.book_channel = "orderbook.1"
    cfg.trade_channel = "publicTrade"
    client = BybitPublicWebSocketReferenceClient(signal, cfg, project_root=tmp_path)
    receive_ns = 1_700_000_000_250_000_000
    client.handle_ws_payload(
        {
            "topic": "orderbook.1.BTCUSDT",
            "ts": 1_700_000_000_205,
            "cts": 1_700_000_000_200,
            "data": {"s": "BTCUSDT", "b": [["60000.1", "1"]], "a": [["60000.2", "2"]], "u": 12, "seq": 13},
        },
        receive_ns=receive_ns,
    )
    client.handle_ws_payload(
        {
            "topic": "publicTrade.BTCUSDT",
            "data": [{"i": "trade-1", "T": 1_700_000_000_210, "s": "BTCUSDT", "S": "Sell", "v": "0.1", "p": "60000.1", "seq": 14}],
        },
        receive_ns=receive_ns,
    )
    assert client.market_id == "bybit:spot:BTCUSDT"
    assert signal.books[0][1]["market_type"] == SPOT_MARKET
    assert signal.trades[0][0]["m"] is True
    assert client.snapshot(now_ns=receive_ns + 100_000_000)["transport"] == "websocket"
