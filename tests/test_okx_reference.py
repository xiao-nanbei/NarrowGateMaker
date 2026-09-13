import csv
import gzip
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from data.import_okx_archive import _selected_retained_days, import_day
from data.download_okx_archive import archive_url, required_source_days
from live.config import _parse, _validate_config
from live.venues.okx import (
    OkxPublicRestReferenceClient,
    OkxPublicWebSocketReferenceClient,
)
from market_fusion import OKX_VENUE


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


def _cfg(tmp_path: Path):
    return SimpleNamespace(
        symbol="BTCUSDT",
        instrument_type="perp",
        product_type="SWAP",
        instrument_id="BTC-USDT-SWAP",
        contract_multiplier=0.01,
        rest_url="https://openapi.okx.com",
        poll_interval_ms=250.0,
        trade_poll_interval_ms=500.0,
        request_timeout_s=2.0,
        max_source_age_s=2.0,
        record_enabled=False,
        record_interval_ms=100.0,
        record_trades=True,
        record_dir=str(tmp_path),
    )


def test_okx_external_config_accepts_public_market_transports():
    cfg = _parse({
        "external_venues": {
            "enabled": True,
            "shadow_only": True,
            "sources": [{
                "venue": "okx",
                "enabled": True,
                "transport": "rest",
                "symbol": "BTCUSDT",
                "instrument_type": "perp",
                "product_type": "SWAP",
                "instrument_id": "BTC-USDT-SWAP",
                "contract_multiplier": 0.01,
            }],
        }
    })
    _validate_config(cfg)
    source = cfg.external_venues.sources[0]
    assert source.venue == "okx"
    source.transport = "websocket"
    _validate_config(cfg)
    source.transport = "invalid"
    with pytest.raises(ValueError, match="requires transport"):
        _validate_config(cfg)

    cfg.external_venues.sources[0].transport = "websocket"
    cfg.external_venues.sources[0].instrument_type = "spot"
    cfg.external_venues.sources[0].product_type = "SPOT"
    cfg.external_venues.sources[0].instrument_id = "BTC-USDT"
    cfg.external_venues.sources[0].contract_multiplier = 0.01
    with pytest.raises(ValueError, match="contract_multiplier=1.0"):
        _validate_config(cfg)


def test_okx_rest_payload_normalization_converts_contracts_to_btc(tmp_path):
    signal = _SignalProbe()
    client = OkxPublicRestReferenceClient(signal, _cfg(tmp_path), project_root=tmp_path)
    receive_ns = 1_700_000_000_250_000_000
    client.handle_book_result(
        {
            "asks": [["60000.2", "20", "0", "2"]],
            "bids": [["60000.1", "10", "0", "1"]],
            "ts": "1700000000200",
            "seqId": 123,
        },
        receive_ns=receive_ns,
        rtt_ms=40.0,
    )
    rows = [
        {"tradeId": "2", "ts": "1700000000210", "px": "60000.2", "sz": "3", "side": "buy"},
        {"tradeId": "1", "ts": "1700000000205", "px": "60000.1", "sz": "2", "side": "sell"},
    ]
    client.handle_trade_result(rows, receive_ns=receive_ns, rtt_ms=45.0)
    client.handle_trade_result(rows, receive_ns=receive_ns + 10_000_000, rtt_ms=45.0)

    book, book_kwargs = signal.books[0]
    assert book["B"] == "0.1"
    assert book["A"] == "0.2"
    assert book_kwargs["venue"] == OKX_VENUE
    assert [row[0]["q"] for row in signal.trades] == ["0.02", "0.03"]
    assert client.market_id == "okx:perp:BTCUSDT"
    snapshot = client.snapshot(now_ns=receive_ns + 500_000_000)
    assert snapshot["book_count"] == 1
    assert snapshot["trade_count"] == 2
    assert snapshot["error_count"] == 0
    assert not hasattr(client, "new_order")


def test_okx_websocket_payload_uses_same_normalization(tmp_path):
    signal = _SignalProbe()
    cfg = _cfg(tmp_path)
    cfg.websocket_url = "wss://ws.okx.com:8443/ws/v5/public"
    cfg.book_channel = "bbo-tbt"
    cfg.trade_channel = "trades"
    client = OkxPublicWebSocketReferenceClient(signal, cfg, project_root=tmp_path)
    receive_ns = 1_700_000_000_250_000_000

    client.handle_ws_payload(
        {
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "asks": [["60000.2", "20", "0", "2"]],
                "bids": [["60000.1", "10", "0", "1"]],
                "ts": "1700000000200",
                "seqId": 123,
            }],
        },
        receive_ns=receive_ns,
    )
    client.handle_ws_payload(
        {
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "tradeId": "1",
                "ts": "1700000000210",
                "px": "60000.1",
                "sz": "2",
                "side": "sell",
            }],
        },
        receive_ns=receive_ns,
    )

    assert signal.books[0][0]["B"] == "0.1"
    assert signal.books[0][1]["sequence_number"] == 123
    assert signal.trades[0][0]["q"] == "0.02"
    assert signal.trades[0][0]["m"] is True
    assert client.snapshot(now_ns=receive_ns + 100_000_000)["transport"] == "websocket"


def test_okx_archive_import_is_utc_bound_and_cleans_source(tmp_path):
    day = date(2026, 2, 25)
    archive_dir = tmp_path / "source"
    out_dir = tmp_path / "trades"
    archive_dir.mkdir()
    source = archive_dir / "BTC-USDT-SWAP-trades-2026-02-25.zip"
    next_source = archive_dir / "BTC-USDT-SWAP-trades-2026-02-26.zip"
    csv_text = (
        "instrument_name,trade_id,side,price,size,created_time\n"
        "BTC-USDT-SWAP,9,buy,59999.9,1,1771948800000\n"
        "BTC-USDT-SWAP,10,buy,60000.1,2,1771977600000\n"
        "BTC-USDT-SWAP,11,sell,60000.2,3,1771977600100\n"
    )
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BTC-USDT-SWAP-trades-2026-02-25.csv", csv_text)
    next_csv = (
        "instrument_name,trade_id,side,price,size,created_time\n"
        "BTC-USDT-SWAP,12,buy,60010.1,4,1772063999000\n"
        "BTC-USDT-SWAP,13,sell,60010.2,5,1772064000000\n"
    )
    with zipfile.ZipFile(next_source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BTC-USDT-SWAP-trades-2026-02-26.csv", next_csv)

    result = import_day(
        day=day,
        archive_dir=archive_dir,
        out_dir=out_dir,
        symbol="BTCUSDT",
        instrument_id="BTC-USDT-SWAP",
        instrument_type="perp",
        product_type="SWAP",
        contract_multiplier=Decimal("0.01"),
        source_size_unit="contracts",
        overwrite=False,
        cleanup_source=True,
    )
    assert result.status == "imported"
    assert result.rows == 3
    assert not source.exists()
    assert not next_source.exists()
    with gzip.open(result.output_path, "rt", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert [row["size"] for row in rows] == ["0.02", "0.03", "0.04"]
    assert rows[0]["market_id"] == "okx:perp:BTCUSDT"


def test_okx_spot_import_keeps_base_quantity_and_market_identity(tmp_path):
    day = date(2026, 2, 25)
    archive_dir = tmp_path / "source"
    out_dir = tmp_path / "spot" / "trades"
    archive_dir.mkdir()
    for source_day, rows in (
        ("2026-02-25", [
            "BTC-USDT,10,buy,60000.1,0.25,1771977600000",
        ]),
        ("2026-02-26", [
            "BTC-USDT,11,sell,60010.1,0.50,1772063999000",
        ]),
    ):
        source = archive_dir / f"BTC-USDT-trades-{source_day}.zip"
        content = (
            "instrument_name,trade_id,side,price,size,created_time\n"
            + "\n".join(rows) + "\n"
        )
        with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(f"BTC-USDT-trades-{source_day}.csv", content)

    result = import_day(
        day=day,
        archive_dir=archive_dir,
        out_dir=out_dir,
        symbol="BTCUSDT",
        instrument_id="BTC-USDT",
        instrument_type="spot",
        product_type="SPOT",
        contract_multiplier=Decimal("1.0"),
        source_size_unit="base_asset",
        overwrite=False,
        cleanup_source=True,
    )
    assert result.status == "imported"
    with gzip.open(result.output_path, "rt", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert [row["size"] for row in rows] == ["0.25", "0.5"]
    assert rows[0]["market_id"] == "okx:spot:BTCUSDT"


def test_okx_downloader_derives_utc8_source_union(tmp_path):
    out_dir = tmp_path / "trades"
    out_dir.mkdir()
    (out_dir / "okx_BTCUSDT_trades_2026-01-01.csv.gz").touch()
    days = [date(2026, 1, 1), date(2026, 1, 3), date(2026, 1, 4)]

    required = required_source_days(days, out_dir=out_dir, symbol="BTCUSDT")

    assert required == [date(2026, 1, 3), date(2026, 1, 4), date(2026, 1, 5)]
    assert archive_url("https://static.okx.test/base", "BTC-USDT", required[0]) == (
        "https://static.okx.test/base/20260103/BTC-USDT-trades-2026-01-03.zip?v=999"
    )


def test_okx_incremental_selection_ignores_unrelated_history(tmp_path):
    archive_dir = tmp_path / "source"
    out_dir = tmp_path / "trades"
    archive_dir.mkdir()
    out_dir.mkdir()
    (archive_dir / "BTC-USDT-trades-2026-07-30.zip").touch()
    (archive_dir / "BTC-USDT-trades-2026-07-31.zip").touch()
    (archive_dir / "BTC-USDT-trades-2025-12-31.zip").touch()
    (out_dir / "okx_BTCUSDT_trades_2026-01-01.csv.gz").touch()

    selected = _selected_retained_days(
        {date(2026, 7, 30)},
        archive_dir=archive_dir,
        out_dir=out_dir,
        symbol="BTCUSDT",
        instrument_id="BTC-USDT",
    )

    assert selected == [date(2026, 7, 30)]
