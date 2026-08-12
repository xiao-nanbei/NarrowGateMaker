import csv
import gzip
from datetime import date, datetime, timezone

import pandas as pd

from data.build_external_reference_features import build_day
from data.download_bybit_reference import OUTPUT_COLUMNS as BYBIT_COLUMNS
from data.download_bitget_reference import CSV_COLUMNS


def test_bitget_trade_features_are_causal_and_order_independent(tmp_path):
    trades_dir = tmp_path / "trades"
    out_dir = tmp_path / "features"
    trades_dir.mkdir()
    day = date(2026, 1, 1)
    start_ms = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    source = trades_dir / "bitget_BTCUSDT_trades_2026-01-01.csv.gz"
    # REST pages are newest-first; the feature builder must recover event order.
    records = [
        ("3", start_ms + 1900, "102", "0.3", "buy"),
        ("2", start_ms + 1500, "101", "0.2", "sell"),
        ("1", start_ms + 100, "100", "0.1", "buy"),
    ]
    with gzip.open(source, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for trade_id, ts_ms, price, size, side in records:
            writer.writerow(
                {
                    "venue": "bitget",
                    "market_id": "bitget:perp:BTCUSDT",
                    "symbol": "BTCUSDT",
                    "product_type": "USDT-FUTURES",
                    "trade_id": trade_id,
                    "exchange_event_ts_ms": ts_ms,
                    "price": price,
                    "size": size,
                    "taker_side": side,
                }
            )

    result = build_day(
        day=day,
        symbol="BTCUSDT",
        trades_dir=trades_dir,
        out_dir=out_dir,
        chunksize=10_000,
        overwrite=False,
    )
    assert result.status == "built"
    frame = pd.read_parquet(result.output_path)
    assert frame["timestamp"].tolist() == [start_ms + 1000, start_ms + 2000]
    assert frame["open"].tolist() == [100.0, 101.0]
    assert frame["close"].tolist() == [100.0, 102.0]
    assert frame["trade_count"].tolist() == [1, 2]
    assert frame["buy_volume"].tolist() == [0.1, 0.3]
    assert frame["sell_volume"].tolist() == [0.0, 0.2]
    # Events during second t are visible only at the right edge t+1s.
    assert (frame["timestamp"] > frame["last_event_ts_ms"]).all()


def test_bybit_uses_same_causal_external_feature_builder(tmp_path):
    trades_dir = tmp_path / "trades"
    out_dir = tmp_path / "features"
    trades_dir.mkdir()
    day = date(2026, 1, 1)
    start_ms = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    source = trades_dir / "bybit_BTCUSDT_trades_2026-01-01.csv.gz"
    with gzip.open(source, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=BYBIT_COLUMNS)
        writer.writeheader()
        writer.writerow(
            {
                "venue": "bybit",
                "market_id": "bybit:perp:BTCUSDT",
                "symbol": "BTCUSDT",
                "product_type": "linear",
                "trade_id": "trade-a",
                "exchange_event_ts_ms": start_ms + 250,
                "price": "100",
                "size": "0.1",
                "taker_side": "buy",
                "is_rpi_trade": "0",
            }
        )

    result = build_day(
        day=day,
        symbol="BTCUSDT",
        trades_dir=trades_dir,
        out_dir=out_dir,
        chunksize=10_000,
        overwrite=False,
        venue="bybit",
    )
    assert result.status == "built"
    assert "BTCUSDT-bybit-trades-1s" in result.output_path
    frame = pd.read_parquet(result.output_path)
    assert frame["timestamp"].tolist() == [start_ms + 1000]


def test_external_feature_metadata_preserves_spot_identity(tmp_path):
    trades_dir = tmp_path / "trades"
    out_dir = tmp_path / "features"
    trades_dir.mkdir()
    day = date(2026, 1, 1)
    start_ms = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    source = trades_dir / "bybit_BTCUSDT_trades_2026-01-01.csv.gz"
    with gzip.open(source, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=BYBIT_COLUMNS)
        writer.writeheader()
        writer.writerow(
            {
                "venue": "bybit",
                "market_id": "bybit:spot:BTCUSDT",
                "symbol": "BTCUSDT",
                "product_type": "spot",
                "trade_id": "1",
                "exchange_event_ts_ms": start_ms + 250,
                "price": "100",
                "size": "0.1",
                "taker_side": "buy",
                "is_rpi_trade": "0",
            }
        )
    result = build_day(
        day=day,
        symbol="BTCUSDT",
        trades_dir=trades_dir,
        out_dir=out_dir,
        chunksize=10_000,
        overwrite=False,
        venue="bybit",
        instrument_type="spot",
    )
    metadata = __import__("json").loads(
        open(result.output_path + ".meta.json", encoding="utf-8").read()
    )
    assert metadata["market_id"] == "bybit:spot:BTCUSDT"
