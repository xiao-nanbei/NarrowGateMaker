from __future__ import annotations

import csv
import gzip
import json
from datetime import date
from pathlib import Path

from data.audit_non_cryptohft_range import (
    SourceContract,
    _audit_binance,
    _audit_normalized,
    _timestamp_ms,
    _validated_gzip_row_count,
)


def test_timestamp_ms_accepts_milliseconds_microseconds_and_utc_text() -> None:
    assert _timestamp_ms("1754006400000") == 1_754_006_400_000
    assert _timestamp_ms("1754006400000000") == 1_754_006_400_000
    assert _timestamp_ms("2025-08-01 00:00:00") == 1_754_006_400_000


def test_audit_binance_accepts_metrics_end_boundary(tmp_path: Path) -> None:
    source = tmp_path / "metrics.csv"
    source.write_text(
        "create_time,symbol,value\n2025-08-01 00:05:00,BTCUSDT,1\n2025-08-02 00:00:00,BTCUSDT,2\n",
        encoding="utf-8",
    )
    contract = SourceContract(
        "metrics",
        "",
        "",
        "binance_csv",
        timestamp_index=0,
        expected_columns=3,
        endpoint_tolerance_ms=360_000,
        allow_end_boundary=True,
    )
    result = _audit_binance(source, date(2025, 8, 1), contract)
    assert result.status == "valid"
    assert result.rows == 2


def test_audit_binance_accepts_frozen_headerless_spot_schema(tmp_path: Path) -> None:
    source = tmp_path / "spot.csv"
    source.write_text(
        "1,1.0,1.0,1,1,1754006400000000,True,True\n2,1.0,1.0,2,2,1754092799999000,False,True\n",
        encoding="utf-8",
    )
    contract = SourceContract(
        "spot",
        "",
        "",
        "binance_csv",
        timestamp_index=5,
        expected_columns=8,
        has_header=False,
    )
    result = _audit_binance(source, date(2025, 8, 1), contract)
    assert result.status == "valid"
    assert result.rows == 2


def test_gzip_schema_accepts_only_frozen_bybit_extra_column(tmp_path: Path) -> None:
    source = tmp_path / "bybit.csv.gz"
    with gzip.open(source, "wt", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "venue",
                "market_id",
                "symbol",
                "product_type",
                "trade_id",
                "exchange_event_ts_ms",
                "price",
                "size",
                "taker_side",
                "is_rpi_trade",
            ]
        )
        writer.writerow(
            ["bybit", "bybit:perp:BTCUSDT", "BTCUSDT", "perp", "1", "1", "1", "1", "buy", "false"]
        )
    contract = SourceContract(
        "bybit",
        "",
        "",
        "normalized_gzip",
        normalized_extra_columns=("is_rpi_trade",),
    )
    assert _validated_gzip_row_count(source, contract) == 1


def test_audit_normalized_requires_complete_day_local_metadata(tmp_path: Path) -> None:
    source = tmp_path / "venue.csv.gz"
    with gzip.open(source, "wt", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "venue",
                "market_id",
                "symbol",
                "product_type",
                "trade_id",
                "exchange_event_ts_ms",
                "price",
                "size",
                "taker_side",
            ]
        )
        writer.writerow(
            [
                "venue",
                "venue:perp:BTCUSDT",
                "BTCUSDT",
                "perp",
                "1",
                1_754_006_400_005,
                "1",
                "1",
                "buy",
            ]
        )
    Path(str(source) + ".meta.json").write_text(
        json.dumps(
            {
                "complete": True,
                "utc_day": "2025-08-01",
                "rows": 1,
                "min_ts_ms": 1_754_006_400_005,
                "max_ts_ms": 1_754_092_799_999,
            }
        ),
        encoding="utf-8",
    )
    contract = SourceContract("venue", "", "", "normalized_gzip")
    result = _audit_normalized(source, date(2025, 8, 1), contract)
    assert result.status == "valid"

    metadata = json.loads(Path(str(source) + ".meta.json").read_text(encoding="utf-8"))
    metadata["rows"] = 2
    Path(str(source) + ".meta.json").write_text(json.dumps(metadata), encoding="utf-8")
    mismatch = _audit_normalized(source, date(2025, 8, 1), contract)
    assert mismatch.status == "invalid"
    assert "metadata row mismatch" in mismatch.failure_reason
