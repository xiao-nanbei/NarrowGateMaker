import csv
import gzip
from datetime import datetime, timedelta, timezone

from data.download_bitget_reference import (
    BitgetTradeDownloader,
    api_eligible,
    load_manifest,
)


class _Response:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.headers = {}
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append({"url": url, **dict(params)})
        limit = int(params["limit"])
        rows = self.rows[:limit]
        return _Response({"code": "00000", "msg": "success", "data": rows})


def test_manifest_is_unique_sorted_utc_days(tmp_path):
    path = tmp_path / "retained.csv"
    path.write_text("day\n2026-06-02\n2026-06-01\n2026-06-02\n", encoding="utf-8")
    assert [day.isoformat() for day in load_manifest(path)] == ["2026-06-01", "2026-06-02"]


def test_old_day_is_archive_required_without_request(tmp_path):
    session = _Session([])
    downloader = BitgetTradeDownloader(
        symbol="BTCUSDT",
        product_type="USDT-FUTURES",
        out_dir=tmp_path,
        session=session,
    )
    result = downloader.audit_day(datetime(2000, 1, 1).date())
    assert result.status == "archive_required"
    assert not session.calls
    assert not api_eligible(datetime(2000, 1, 1).date())


def test_recent_day_downloads_normalized_daily_gzip(tmp_path):
    day = (datetime.now(timezone.utc) - timedelta(days=2)).date()
    start_ms = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    session = _Session(
        [
            {
                "tradeId": "20",
                "price": "60000.2",
                "size": "0.02",
                "side": "Buy",
                "ts": str(start_ms + 2000),
            },
            {
                "tradeId": "19",
                "price": "60000.1",
                "size": "0.01",
                "side": "Sell",
                "ts": str(start_ms + 1000),
            },
        ]
    )
    downloader = BitgetTradeDownloader(
        symbol="BTCUSDT",
        product_type="USDT-FUTURES",
        out_dir=tmp_path,
        session=session,
        requests_per_second=1000,
    )
    result = downloader.download_day(day)
    assert result.status == "downloaded"
    assert result.rows == 2
    assert (tmp_path / f"bitget_BTCUSDT_trades_{day.isoformat()}.csv.gz.meta.json").exists()
    with gzip.open(result.path, "rt", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["market_id"] == "bitget:perp:BTCUSDT"
    assert rows[0]["taker_side"] == "buy"
    assert rows[1]["taker_side"] == "sell"


def test_recent_spot_day_uses_public_spot_history_contract(tmp_path):
    day = (datetime.now(timezone.utc) - timedelta(days=2)).date()
    start_ms = int(
        datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp()
        * 1000
    )
    session = _Session(
        [
            {
                "tradeId": "20",
                "price": "60000.2",
                "size": "0.02",
                "side": "Buy",
                "ts": str(start_ms + 2000),
            }
        ]
    )
    downloader = BitgetTradeDownloader(
        symbol="BTCUSDT",
        product_type="SPOT",
        instrument_type="spot",
        out_dir=tmp_path,
        session=session,
        requests_per_second=1000,
    )

    result = downloader.download_day(day)

    assert result.status == "downloaded"
    assert all("productType" not in call for call in session.calls)
    assert all("/spot/market/fills-history" in call["url"] for call in session.calls)
    with gzip.open(result.path, "rt", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["market_id"] == "bitget:spot:BTCUSDT"
    assert rows[0]["product_type"] == "SPOT"
