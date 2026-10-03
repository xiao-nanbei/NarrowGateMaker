from datetime import UTC, datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.downloaders import cryptohft_trades as download


def source_file(path, *, price="100.1", quantity="0.002", symbol="BTCUSDC", time_scale=1):
    timestamp = int(datetime(2025, 8, 1, tzinfo=UTC).timestamp()) * 1000
    pq.write_table(pa.table({
        "received_time": [timestamp * 10**6 + 100_000_000],
        "event_time": [timestamp * time_scale], "symbol": [symbol],
        "trade_id": [123], "price": [price], "quantity": [quantity],
        "trade_time": [timestamp * time_scale], "is_buyer_maker": [False],
        "order_type": ["MARKET"],
    }), path)


def test_fixed_calendar_and_boundary():
    plan = download.hourly_plan("2025-08-01", "2026-09-05", next_day_first_hour=True)
    assert len(plan) == len(set(plan)) == 9625
    assert plan[0] == ("2025-08-01", "00")
    assert plan[-1] == ("2026-09-06", "00")


def test_reverse_calendar_rejected():
    with pytest.raises(ValueError):
        download.hourly_plan("2026-09-05", "2025-08-01")


def test_trade_source_identity_and_original_fields(tmp_path):
    path = tmp_path / "source.parquet"
    source_file(path)
    result = download.inspect_trade_file(path, "2025-08-01", "00", "BTCUSDC")
    assert result["status"] == "READABLE"
    assert result["rows"] == 1
    assert result["sha256"] == download.sha256(path)
    assert "order_type" in result["columns"]
    assert result["clock_ranges"]["trade_time"]["min"] == 1754006400000


@pytest.mark.parametrize("field,value", [("price", "0"), ("quantity", "NaN")])
def test_invalid_source_values_retained_as_findings(tmp_path, field, value):
    path = tmp_path / "source.parquet"
    source_file(path, **{field: value})
    before = path.read_bytes()
    result = download.inspect_trade_file(path, "2025-08-01", "00", "BTCUSDC")
    assert result["status"] == "READABLE_WITH_FINDINGS"
    assert result["value_findings"][f"invalid_{field}"] == 1
    assert path.read_bytes() == before


@pytest.mark.parametrize("kwargs", [{"symbol": "BTCUSDT"}, {"time_scale": 1000}])
def test_wrong_symbol_or_clock_rejected(tmp_path, kwargs):
    path = tmp_path / "source.parquet"
    source_file(path, **kwargs)
    with pytest.raises(ValueError):
        download.inspect_trade_file(path, "2025-08-01", "00", "BTCUSDC")


def test_auth_first_failure_allows_existing_refresh_then_stops():
    import threading
    client = download.ObservedClient("not-real", "not-real", threading.Event())
    response = SimpleNamespace(status_code=401, close=lambda: None)
    assert client.observe(response) is response
    assert not client.stop.is_set()
    with pytest.raises(download.AccessBlocked, match="AUTHORIZATION_BLOCKED"):
        client.observe(response)
    assert client.stop.is_set()
    client.session.close()


def test_quota_stops_without_unbounded_retry():
    import threading
    client = download.ObservedClient("not-real", "not-real", threading.Event())
    with pytest.raises(download.AccessBlocked, match="QUOTA_BLOCKED"):
        client.observe(SimpleNamespace(status_code=429, close=lambda: None))
    assert client.stop.is_set()
    client.session.close()


def test_conflicting_reuse_digest_rejected(tmp_path):
    rows = [{"day": "2025-08-01", "hour": "00", "channel": "trades", "status": "READABLE", "sha256": sha}
            for sha in ("abc", "def")]
    path = tmp_path / "objects.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in rows))
    with pytest.raises(ValueError, match="conflicting reusable"):
        download.read_receipts([Path(path)])


def _local_resume_args(tmp_path, monkeypatch):
    args = SimpleNamespace(
        start="2025-08-01", end="2025-08-01", symbol="BTCUSDC", next_day_first_hour=False,
        output_dir=tmp_path / "sources", state_dir=tmp_path / "state", reuse_manifest=[],
        workers=1, attempts=1,
    )
    monkeypatch.setattr(download, "hourly_plan", lambda *a, **k: [("2025-08-01", "00")])
    monkeypatch.setenv(download.transport.API_KEY_ENV, "test-only-not-a-credential")
    monkeypatch.setattr(
        download.transport.CryptoHFTClient, "ensure_jwt", lambda self: "test-only-token",
    )

    def unexpected_download(*args, **kwargs):
        pytest.fail("existing source bytes must never be downloaded again")

    monkeypatch.setattr(download.ObservedClient, "download_file", unexpected_download)
    source = args.output_dir / "binance_futures/2025-08-01/00/BTCUSDC_trades.parquet"
    source.parent.mkdir(parents=True)
    return args, source


@pytest.mark.parametrize("tail", [b'', b'{"day":"2025-08-01","status":', b'{"day":"unfinished"}'])
def test_resume_recovers_unreceipted_file_and_repairs_only_uncommitted_tail(tmp_path, monkeypatch, tail):
    args, source = _local_resume_args(tmp_path, monkeypatch)
    source_file(source)
    before = source.read_bytes()
    args.state_dir.mkdir()
    journal = args.state_dir / "objects.jsonl"
    failed = {"day": "2025-08-01", "hour": "00", "channel": "trades", "status": "FAILED"}
    prefix = json.dumps(failed).encode() + b"\n"
    journal.write_bytes(prefix + tail)

    assert download.acquire(args) == 0
    assert source.read_bytes() == before
    contents = journal.read_bytes()
    assert contents.startswith(prefix) and contents.endswith(b"\n")
    records = [json.loads(line) for line in contents.splitlines()]
    assert len(records) == 2
    assert records[0] == failed
    assert records[1]["status"] == "READABLE"
    assert records[1]["download_status"] == "RECOVERED_EXISTING_SOURCE"
    assert records[1]["sha256"] == download.sha256(source)
    assert download.acquire(args) == 0
    assert journal.read_bytes() == contents


@pytest.mark.parametrize("contents", [b'{"broken":}\n{"unfinished":', b'null\ntrailing'])
def test_resume_rejects_corrupt_complete_record_without_truncating_it(tmp_path, monkeypatch, contents):
    args, source = _local_resume_args(tmp_path, monkeypatch)
    source_file(source)
    before = source.read_bytes()
    args.state_dir.mkdir()
    journal = args.state_dir / "objects.jsonl"
    journal.write_bytes(contents)

    with pytest.raises(ValueError):
        download.acquire(args)
    assert journal.read_bytes() == contents
    assert source.read_bytes() == before


@pytest.mark.parametrize("valid_container", [False, True])
def test_resume_does_not_overwrite_bad_unreceipted_source(tmp_path, monkeypatch, valid_container):
    args, source = _local_resume_args(tmp_path, monkeypatch)
    if valid_container:
        source_file(source, symbol="BTCUSDT")
    else:
        source.write_bytes(b"incomplete or corrupt source")
    before = source.read_bytes()

    assert download.acquire(args) == 1
    assert source.read_bytes() == before
    journal = args.state_dir / "objects.jsonl"
    record = json.loads(journal.read_text())
    assert record["status"] == "FAILED"
    assert record["download_status"] == "RECOVERED_EXISTING_SOURCE"
    assert "sha256" not in record
    assert json.loads((args.state_dir / "summary.json").read_text())["state"] != "COMPLETED"
