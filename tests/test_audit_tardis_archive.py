from data.audit_tardis_archive import _audit_download


def _row(dataset: str, first: str, last: str, header: str) -> dict[str, object]:
    return {
        "dataset": dataset,
        "day": "2026-01-01",
        "venue": "binance-futures",
        "symbol": "BTCUSDC",
        "path": "/tmp/file.csv.zst",
        "sha256": "a" * 64,
        "size_bytes": 123,
        "content_length": 123,
        "csv_rows": 2,
        "zstd_valid": True,
        "header": header,
        "first_data_row": first,
        "last_data_row": last,
    }


def test_book_ticker_boundary_admission() -> None:
    header = (
        "exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,"
        "bid_price,bid_amount"
    )
    row = _row(
        "book_ticker",
        "binance-futures,BTCUSDC,1767225600029000,1767225600031000,1,2,1,1",
        "binance-futures,BTCUSDC,1767311999986000,1767311999988000,1,2,1,1",
        header,
    )
    result = _audit_download(row, boundary_tolerance_us=1_000_000)
    assert result["raw_admissible"] is True


def test_incremental_l2_requires_snapshot_bootstrap() -> None:
    header = (
        "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount"
    )
    row = _row(
        "incremental_book_L2",
        "binance-futures,BTCUSDC,1767225600476000,1767225600801000,false,ask,2,1",
        "binance-futures,BTCUSDC,1767311999983000,1767311999985000,false,bid,1,1",
        header,
    )
    result = _audit_download(row, boundary_tolerance_us=1_000_000)
    assert result["snapshot_bootstrap_valid"] is False
    assert result["raw_admissible"] is False


def test_local_day_partition_allows_small_exchange_clock_crossing() -> None:
    header = (
        "exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,"
        "bid_price,bid_amount"
    )
    row = _row(
        "book_ticker",
        "binance-futures,BTCUSDC,1767225599996000,1767225600001000,1,2,1,1",
        "binance-futures,BTCUSDC,1767311999986000,1767311999988000,1,2,1,1",
        header,
    )
    result = _audit_download(row, boundary_tolerance_us=5_000_000)
    assert result["boundary_valid"] is True
    assert result["raw_admissible"] is True


def test_local_timestamp_outside_partition_is_rejected() -> None:
    header = (
        "exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,"
        "bid_price,bid_amount"
    )
    row = _row(
        "book_ticker",
        "binance-futures,BTCUSDC,1767225600029000,1767225599999000,1,2,1,1",
        "binance-futures,BTCUSDC,1767311999986000,1767311999988000,1,2,1,1",
        header,
    )
    result = _audit_download(row, boundary_tolerance_us=5_000_000)
    assert result["boundary_valid"] is False
    assert result["raw_admissible"] is False
