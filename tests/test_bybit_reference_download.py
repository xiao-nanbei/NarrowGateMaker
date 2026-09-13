import csv
import gzip
import io
from datetime import date

from data.download_bybit_reference import BybitArchiveDownloader, _timestamp_ms


class _ArchiveResponse:
    status_code = 200
    headers = {"Content-Length": "1"}

    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=1024):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]


class _ArchiveSession:
    def __init__(self, content: bytes):
        self.content = content
        self.headers = {}

    def get(self, *args, **kwargs):
        return _ArchiveResponse(self.content)


def _source_archive() -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb") as compressed:
        text = io.TextIOWrapper(compressed, encoding="utf-8", newline="")
        writer = csv.DictWriter(
            text,
            fieldnames=(
                "timestamp",
                "symbol",
                "side",
                "size",
                "price",
                "tickDirection",
                "trdMatchID",
                "grossValue",
                "homeNotional",
                "foreignNotional",
                "RPI",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "timestamp": "1767225600.073",
                "symbol": "BTCUSDT",
                "side": "Buy",
                "size": "0.001",
                "price": "60000.5",
                "trdMatchID": "trade-a",
                "RPI": "0",
            }
        )
        writer.writerow(
            {
                "timestamp": "1767225600.1546",
                "symbol": "BTCUSDT",
                "side": "Sell",
                "size": "0.002",
                "price": "60000.4",
                "trdMatchID": "trade-b",
                "RPI": "1",
            }
        )
        text.flush()
        text.detach()
    return raw.getvalue()


def _spot_source_archive() -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb") as compressed:
        text = io.TextIOWrapper(compressed, encoding="utf-8", newline="")
        writer = csv.DictWriter(
            text, fieldnames=("id", "timestamp", "price", "volume", "side", "rpi")
        )
        writer.writeheader()
        writer.writerow(
            {
                "id": "1",
                "timestamp": "1767225600185",
                "price": "60000.5",
                "volume": "0.001",
                "side": "buy",
                "rpi": "0",
            }
        )
        text.flush()
        text.detach()
    return raw.getvalue()


def test_bybit_timestamp_decimal_conversion_is_exact():
    assert _timestamp_ms("1767225600.1546") == 1_767_225_600_154


def test_bybit_archive_download_normalizes_retained_utc_day(tmp_path):
    downloader = BybitArchiveDownloader(
        symbol="BTCUSDT",
        out_dir=tmp_path,
        session=_ArchiveSession(_source_archive()),
    )
    result = downloader.download_day(date(2026, 1, 1))
    assert result.status == "downloaded"
    assert result.rows == 2
    assert result.min_ts_ms == 1_767_225_600_073
    assert result.max_ts_ms == 1_767_225_600_154
    assert result.source_sha256
    assert not list(tmp_path.glob("*.download"))

    with gzip.open(result.path, "rt", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["venue"] == "bybit"
    assert rows[0]["market_id"] == "bybit:perp:BTCUSDT"
    assert rows[0]["taker_side"] == "buy"
    assert rows[1]["is_rpi_trade"] == "1"

    present = downloader.download_day(date(2026, 1, 1))
    assert present.status == "present"
    assert present.rows == 2


def test_bybit_spot_archive_uses_spot_identity_and_millisecond_timestamp(tmp_path):
    downloader = BybitArchiveDownloader(
        symbol="BTCUSDT",
        out_dir=tmp_path,
        instrument_type="spot",
        session=_ArchiveSession(_spot_source_archive()),
    )
    assert downloader.source_url(date(2026, 1, 1)).endswith(
        "/spot/BTCUSDT/BTCUSDT_2026-01-01.csv.gz"
    )
    result = downloader.download_day(date(2026, 1, 1))
    assert result.status == "downloaded"
    assert result.min_ts_ms == 1_767_225_600_185
    with gzip.open(result.path, "rt", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["market_id"] == "bybit:spot:BTCUSDT"
    assert rows[0]["product_type"] == "spot"
