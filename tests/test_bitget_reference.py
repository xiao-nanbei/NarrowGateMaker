from types import SimpleNamespace

import pytest

from live.config import _parse, _validate_config
from live.venues.bitget import BitgetPublicReferenceClient
from market_fusion import BITGET_VENUE, PERP_MARKET, REFERENCE_ROLE, SPOT_MARKET, MarketSpec


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


def _cfg(tmp_path, *, instrument_type="perp", product_type="USDT-FUTURES"):
    return SimpleNamespace(
        symbol="BTCUSDT",
        instrument_type=instrument_type,
        product_type=product_type,
        book_channel="books1",
        trade_channel="trade",
        websocket_url="wss://ws.bitget.com/v2/ws/public",
        max_source_age_s=2.0,
        record_enabled=False,
        record_interval_ms=100.0,
        record_trades=True,
        record_dir=str(tmp_path),
    )


def test_market_spec_identity_includes_venue():
    binance = MarketSpec("BTCUSDT", PERP_MARKET, REFERENCE_ROLE, "binance")
    bitget = MarketSpec("BTCUSDT", PERP_MARKET, REFERENCE_ROLE, BITGET_VENUE)
    assert binance.market_id == "binance:perp:BTCUSDT"
    assert bitget.market_id == "bitget:perp:BTCUSDT"
    assert binance.market_id != bitget.market_id


def test_external_venue_config_parses_nested_source():
    cfg = _parse(
        {
            "external_venues": {
                "enabled": True,
                "shadow_only": True,
                "sources": [
                    {
                        "venue": "bitget",
                        "enabled": True,
                        "symbol": "BTCUSDT",
                        "record_enabled": False,
                    }
                ],
            }
        }
    )
    _validate_config(cfg)
    assert cfg.external_venues.enabled is True
    assert len(cfg.external_venues.sources) == 1
    assert cfg.external_venues.sources[0].venue == "bitget"
    assert cfg.external_venues.sources[0].symbol == "BTCUSDT"
    assert not hasattr(cfg.external_venues.sources[0], "api_key_env")


def test_config_rejects_unknown_root_and_nested_keys():
    with pytest.raises(ValueError, match="unknown config key.*rl"):
        _parse({"rl": {"enabled": False}})
    with pytest.raises(ValueError, match="unknown config key.*ml.min_fill_prob"):
        _parse({"ml": {"min_fill_prob": 0.05}})


def test_bitget_payload_normalization_is_shadow_only(tmp_path):
    signal = _SignalProbe()
    client = BitgetPublicReferenceClient(signal, _cfg(tmp_path), project_root=tmp_path)
    receive_ns = 1_700_000_000_250_000_000

    client.handle_payload(
        {
            "arg": {"instType": "USDT-FUTURES", "channel": "books1", "instId": "BTCUSDT"},
            "data": [
                {
                    "bids": [["60000.1", "1.25"]],
                    "asks": [["60000.2", "2.5"]],
                    "seq": 123,
                    "ts": "1700000000200",
                }
            ],
        },
        receive_ts_ns=receive_ns,
    )
    client.handle_payload(
        {
            "arg": {"instType": "USDT-FUTURES", "channel": "trade", "instId": "BTCUSDT"},
            "data": [
                {
                    "ts": "1700000000210",
                    "price": "60000.1",
                    "size": "0.02",
                    "side": "sell",
                    "tradeId": "456",
                }
            ],
        },
        receive_ts_ns=receive_ns,
    )

    assert len(signal.books) == 1
    book, book_kwargs = signal.books[0]
    assert book["b"] == "60000.1"
    assert book["a"] == "60000.2"
    assert book_kwargs["venue"] == BITGET_VENUE
    assert book_kwargs["sequence_number"] == 123

    assert len(signal.trades) == 1
    trade, trade_kwargs = signal.trades[0]
    assert trade["m"] is True  # Bitget taker sell == Binance buyer-maker flag.
    assert trade_kwargs["venue"] == BITGET_VENUE
    assert trade_kwargs["sequence_number"] == 456

    snapshot = client.snapshot(now_ns=receive_ns + 500_000_000)
    assert snapshot["market_id"] == "bitget:perp:BTCUSDT"
    assert snapshot["book_count"] == 1
    assert snapshot["trade_count"] == 1
    assert snapshot["book_age_ms"] == 500.0
    assert snapshot["book_event_age_ms"] == pytest.approx(550.0, abs=0.001)
    assert snapshot["trade_event_age_ms"] == pytest.approx(540.0, abs=0.001)
    assert snapshot["book_transport_lag_ms"] == pytest.approx(50.0, abs=0.001)
    assert snapshot["trade_transport_lag_ms"] == pytest.approx(40.0, abs=0.001)
    assert snapshot["book_stale"] == 0
    assert snapshot["trade_stale"] == 0
    assert snapshot["stale"] == 0
    assert not hasattr(client, "new_order")
    assert not hasattr(client, "login")


def test_bitget_v3_public_payload_normalization(tmp_path):
    signal = _SignalProbe()
    cfg = _cfg(tmp_path)
    cfg.websocket_url = "wss://ws.bitget.com/v3/ws/public"
    cfg.trade_channel = "publicTrade"
    client = BitgetPublicReferenceClient(signal, cfg, project_root=tmp_path)
    receive_ns = 1_700_000_000_250_000_000

    args = client.subscription.args()
    assert args[0] == {
        "instType": "usdt-futures",
        "topic": "books1",
        "symbol": "BTCUSDT",
    }
    client.handle_payload(
        {
            "arg": {"topic": "books1", "symbol": "BTCUSDT"},
            "data": [{
                "b": [["60000.1", "1.25"]],
                "a": [["60000.2", "2.5"]],
                "seq": 123,
                "pseq": 122,
                "ts": "1700000000200",
            }],
        },
        receive_ts_ns=receive_ns,
    )
    client.handle_payload(
        {
            "arg": {"topic": "publicTrade", "symbol": "BTCUSDT"},
            "data": [{
                "T": "1700000000210",
                "p": "60000.1",
                "v": "0.02",
                "S": "sell",
                "i": "trade-v3",
            }],
        },
        receive_ts_ns=receive_ns,
    )

    assert signal.books[0][0]["B"] == "1.25"
    assert signal.books[0][1]["sequence_number"] == 123
    assert signal.trades[0][0]["m"] is True
    assert signal.trades[0][1]["sequence_number"] is None


def test_bitget_spot_identity_and_signal_routing(tmp_path):
    signal = _SignalProbe()
    client = BitgetPublicReferenceClient(
        signal,
        _cfg(tmp_path, instrument_type="spot", product_type="SPOT"),
        project_root=tmp_path,
    )
    assert client.market_id == "bitget:spot:BTCUSDT"
    assert client.subscription.args()[0]["instType"] == "SPOT"
    client.handle_payload(
        {
            "arg": {"channel": "books1"},
            "data": [{"bids": [["60000", "1"]], "asks": [["60001", "1"]], "ts": 1700000000000}],
        },
        receive_ts_ns=1_700_000_000_050_000_000,
    )
    assert signal.books[0][1]["market_type"] == SPOT_MARKET


def test_fresh_receive_with_old_exchange_event_is_stale(tmp_path):
    signal = _SignalProbe()
    client = BitgetPublicReferenceClient(signal, _cfg(tmp_path), project_root=tmp_path)
    receive_ns = 1_700_000_010_000_000_000
    old_exchange_ms = 1_700_000_000_000
    client.handle_payload(
        {
            "arg": {"channel": "books1"},
            "data": [{"bids": [["60000", "1"]], "asks": [["60001", "1"]], "ts": old_exchange_ms}],
        },
        receive_ts_ns=receive_ns,
    )
    client.handle_payload(
        {
            "arg": {"channel": "trade"},
            "data": [{"price": "60000", "size": "0.01", "side": "buy", "ts": old_exchange_ms}],
        },
        receive_ts_ns=receive_ns,
    )
    snapshot = client.snapshot(now_ns=receive_ns + 100_000_000)
    assert snapshot["book_age_ms"] == 100.0
    assert snapshot["book_event_age_ms"] == pytest.approx(10_100.0, abs=0.001)
    assert snapshot["book_stale"] == 1
    assert snapshot["trade_stale"] == 1
    assert snapshot["stale"] == 1


def test_trade_silence_does_not_hide_fresh_book_reference(tmp_path):
    signal = _SignalProbe()
    client = BitgetPublicReferenceClient(signal, _cfg(tmp_path), project_root=tmp_path)
    receive_ns = 1_700_000_010_000_000_000
    client.handle_payload(
        {
            "arg": {"channel": "trade"},
            "data": [{"price": "60000", "size": "0.01", "side": "buy", "ts": 1_700_000_010_000}],
        },
        receive_ts_ns=receive_ns,
    )
    fresh_book_receive_ns = receive_ns + 3_000_000_000
    client.handle_payload(
        {
            "arg": {"channel": "books1"},
            "data": [{"bids": [["60000", "1"]], "asks": [["60001", "1"]], "ts": 1_700_000_013_000}],
        },
        receive_ts_ns=fresh_book_receive_ns,
    )
    snapshot = client.snapshot(now_ns=fresh_book_receive_ns + 100_000_000)
    assert snapshot["book_stale"] == 0
    assert snapshot["trade_stale"] == 1
    assert snapshot["stale"] == 0
