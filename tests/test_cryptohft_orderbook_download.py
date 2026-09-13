import json
import sys
from datetime import datetime, timezone

import pandas as pd
import pytest

import data.download_cryptohft_orderbook as cryptohft_orderbook
from data.download_cryptohft_orderbook import (
    DEFAULT_WARMUP_HOURS,
    BadDayRepair,
    DailyOutputWriter,
    OrderBookSequenceState,
    OrderBookState,
    _contiguous_day_ranges,
    _daily_write_start,
    _default_target_roots,
    _load_bad_day_repairs,
    _load_per_day_sequence_audits,
    _load_retained_days,
    _raw_paths_for_repair,
    _retained_process_ranges,
    _select_bad_day_repairs,
    _select_ts_ms,
    _sequence_audit_status,
)


def test_default_warmup_can_reach_a_prior_utc_day_snapshot():
    assert DEFAULT_WARMUP_HOURS >= 24


@pytest.mark.parametrize("suffix", [".parquet", ".parquet.zst"])
def test_retired_native_aggregate_rejected_before_cache_or_network(tmp_path, monkeypatch, suffix):
    from pathlib import Path

    client = cryptohft_orderbook.CryptoHFTClient(None, jwt="opaque", transport="rest")
    monkeypatch.setattr(client.session, "get", lambda *_a, **_k: pytest.fail("must not download"))
    destination = tmp_path / "old-copy"
    destination.write_bytes(b"existing native input must not be reused")
    with pytest.raises(ValueError, match="native aggTrades acquisition is retired"):
        client.download_file(Path(f"binance_futures/2025-08-01/00/BTCUSDC_aggTrades{suffix}"), destination)
    assert destination.read_bytes() == b"existing native input must not be reused"


@pytest.mark.parametrize("relative", [
    "binance_futures/day/00/BTCUSDC_trades.parquet",
    "binance_futures/day/00/BTCUSDC_orderbook.parquet.zst",
    "binance_futures/day/00/BTCUSDT_aggTrades.parquet",
    "binance_spot/day/00/BTCUSDC_aggTrades.parquet",
])
def test_native_retirement_keeps_individual_book_and_reference_paths(tmp_path, relative):
    from pathlib import Path

    client = cryptohft_orderbook.CryptoHFTClient(None, jwt="opaque", transport="rest")
    destination = tmp_path / "existing-source"
    destination.write_bytes(b"existing source")
    assert client.download_file(Path(relative), destination) == "exists"


@pytest.mark.parametrize("root_kind", ["explicit", "same_name", "legacy", "relocated_legacy", "current_ingestion"])
def test_daily_hour_resolution_respects_explicit_roots(tmp_path, monkeypatch, root_kind):
    import data_paths
    import pyarrow as pa
    import pyarrow.parquet as pq

    market_root = tmp_path / "marketdata"
    legacy = market_root / "cryptohftdata"
    relocated = tmp_path / "registered-history"
    ingestion = market_root / "raw/.incoming/orderbook"
    monkeypatch.setattr(cryptohft_orderbook, "_default_raw_root", lambda: ingestion)
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", str(market_root))
    monkeypatch.delenv("NARROWGATE_DAILY_ORDERBOOK_ROOT", raising=False)
    monkeypatch.setattr(data_paths, "_private_storage_roots", lambda: {
        "path_prefix_relocations": {str(legacy): str(relocated)},
    })
    canonical = tmp_path / "global-daily.parquet"
    pq.write_table(pa.table({"source_hour": [3]}), canonical)
    monkeypatch.setattr(cryptohft_orderbook, "daily_market_path", lambda *args, **kwargs: canonical)
    root = {"explicit": tmp_path / "selected", "same_name": tmp_path / "cryptohftdata",
            "legacy": legacy, "relocated_legacy": relocated, "current_ingestion": ingestion}[root_kind]
    hour = root / "binance_futures/2026-01-02/03/BTCUSDC_orderbook.parquet.zst"
    permitted = root_kind in {"legacy", "relocated_legacy", "current_ingestion"}
    assert cryptohft_orderbook.raw_hour_available(hour) is permitted
    assert (cryptohft_orderbook.raw_hour_storage_path(hour) == canonical) is permitted

    # Every explicitly selected root may use its own daily container instead.
    adjacent = root / "binance_futures/BTCUSDC/2026-01-02/incremental_book_L2.parquet"
    adjacent.parent.mkdir(parents=True)
    pq.write_table(pa.table({"source_hour": [3]}), adjacent)
    assert cryptohft_orderbook.raw_hour_storage_path(hour) == adjacent
    assert cryptohft_orderbook.raw_hour_available(hour)


@pytest.mark.parametrize("entries,available", [
    ([{"source_id": "tardis", "sha256": "a", "rows": 2}], False),
    ([{"source_id": "cryptohft", "sha256": "b", "rows": 2, "hours": [2]}], False),
    ([{"source_id": "cryptohft", "sha256": "b", "rows": 2, "hours": [3]}], True),
])
def test_fused_day_only_satisfies_actually_included_crypto_hours(tmp_path, monkeypatch, entries, available):
    import pyarrow as pa
    import pyarrow.parquet as pq

    day = tmp_path / "day.parquet"
    table = pa.table({"source_hour": [3]}).replace_schema_metadata({
        b"narrowgate.included_sources": json.dumps(entries).encode(),
    })
    pq.write_table(table, day)
    monkeypatch.setattr(cryptohft_orderbook, "daily_raw_for_hour", lambda _: day)
    hour = tmp_path / "binance_futures/2026-09-05/03/BTCUSDC_orderbook.parquet.zst"
    assert cryptohft_orderbook.raw_hour_available(hour) is available
    with pytest.raises(ValueError, match="cannot reconstruct retired provider-native"):
        cryptohft_orderbook._decompress_parquet_zst(hour)


def test_download_failure_does_not_log_signed_redirect(tmp_path, monkeypatch, capsys):
    import traceback
    from pathlib import Path

    secret = "example-signature-not-for-logs"
    client = cryptohft_orderbook.CryptoHFTClient(None, jwt="opaque", transport="rest")

    def fail(*args, **kwargs):
        raise cryptohft_orderbook.requests.exceptions.SSLError(
            f"https://storage.invalid/object?X-Amz-Signature={secret}"
        )

    monkeypatch.setattr(client.session, "get", fail)
    monkeypatch.setattr(client, "_reset_session", lambda: None)
    monkeypatch.setattr(cryptohft_orderbook, "DEFAULT_DOWNLOAD_ATTEMPTS", 1)
    monkeypatch.setattr(cryptohft_orderbook.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="SSLError") as error:
        client.download_file(Path("binance_futures/day/00/BTCUSDC_orderbook.parquet.zst"), tmp_path / "raw")
    rendered = capsys.readouterr().out + "".join(traceback.format_exception(error.value))
    assert secret not in rendered and "X-Amz-Signature" not in rendered
    assert "BTCUSDC_orderbook" in rendered
    assert not (tmp_path / "raw").exists()


@pytest.mark.parametrize("failure", ["http", "network", "body"])
def test_jwt_failure_withholds_key_and_response(monkeypatch, failure):
    from types import SimpleNamespace

    secret = "example-private-api-key"
    client = cryptohft_orderbook.CryptoHFTClient(secret, transport="rest")

    def response(*args, **kwargs):
        if failure == "network":
            raise cryptohft_orderbook.requests.ConnectionError(f"https://auth.invalid/?api_key={secret}")
        return SimpleNamespace(status_code=401 if failure == "http" else 200,
                               text=f"unexpected body {secret}", json=lambda: {"detail": secret})

    monkeypatch.setattr(client.session, "post", response)
    monkeypatch.setattr(client.session, "get", response)
    with pytest.raises(RuntimeError) as error:
        client.ensure_jwt()
    assert secret not in str(error.value)


def test_tardis_daily_export_preserves_all_raw_columns_and_reuses(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    day = "2026-09-05"
    for hour in range(24):
        path = tmp_path / "raw" / "binance_futures" / day / f"{hour:02d}" / "BTCUSDC_orderbook.parquet.zst"
        path.parent.mkdir(parents=True)
        pd.DataFrame([dict(received_time=1788566400000111652, event_time=1788566399875,
                           transaction_time=1788566399872, symbol="BTCUSDC", event_type="update",
                           first_update_id=10, final_update_id=11, prev_final_update_id=9,
                           side="ask", price="79648.3000", quantity="0.476000", order_count=None)]).to_parquet(path)
    args = (tmp_path / "raw", tmp_path / "daily", "binance_futures", "BTCUSDC", day)
    result = cryptohft_orderbook.export_tardis_day(*args)
    assert result["rows"] == 24 and result["round_trip_all_original_columns"]
    output = tmp_path / "daily/binance_futures/BTCUSDC/incremental_book_L2/2026-09-05.parquet"
    rows = pq.read_table(output).to_pylist()
    assert rows[0]["timestamp"] == 1788566399872000
    assert rows[0]["local_timestamp"] == 1788566400000111
    assert rows[0]["received_time"] == 1788566400000111652
    assert rows[0]["amount"] == "0.476000"
    assert [r["source_hour"] for r in rows] == list(range(24))
    assert all(r["first_update_id"] == 10 for r in rows)
    assert cryptohft_orderbook.export_tardis_day(*args)["status"] == "reused_verified"
    assert path.exists() and not result["originals_deleted"]
    unrelated = tmp_path / "unrelated.parquet"
    unrelated.write_bytes(b"not the verified daily replacement")
    with pytest.raises(ValueError, match="Daily reader must resolve the verified container"):
        cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True, replacement_path=unrelated)
    assert len(list((tmp_path / "raw").rglob("*_orderbook.parquet.zst"))) == 24
    from data.build_active_order_queue_tape import iter_cryptohft_logical_messages
    before = list(iter_cryptohft_logical_messages(path, 0.1))
    monkeypatch.setenv("NARROWGATE_DAILY_ORDERBOOK_ROOT", str(tmp_path / "daily"))
    original_unlink = type(path).unlink
    def interrupted_unlink(item, *args, **kwargs):
        if item.name == "BTCUSDC_orderbook.parquet.zst" and item.parent.name == "12":
            raise OSError("simulated retirement interruption")
        return original_unlink(item, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(type(path), "unlink", interrupted_unlink)
        with pytest.raises(OSError, match="simulated retirement interruption"):
            cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert output.exists() and path.exists()
    retired = cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert retired["originals_deleted"] and not path.exists()
    assert cryptohft_orderbook.raw_hour_available(path)
    assert list(iter_cryptohft_logical_messages(path, 0.1)) == before
    from models.exchange_book_replay import CryptoHFTExchangeBookTape
    tape = CryptoHFTExchangeBookTape(raw_root=tmp_path / "raw", day=day,
        symbol="BTCUSDC", tick_size=0.1, warmup_hours=0, cache_enabled=False)
    assert not tape.missing_paths and len(list(tape)) == 24
    assert len(tape.identity()["files"]) == 24
    cached = CryptoHFTExchangeBookTape(raw_root=tmp_path / "raw", day=day,
        symbol="BTCUSDC", tick_size=0.1, warmup_hours=0, cache_dir=tmp_path / "cache")
    assert list(cached) == list(tape)
    assert list(cached) == list(tape)
    assert cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)["originals_deleted"]
    # A damaged daily container must not cause retirement of a redownloaded hour.
    path.parent.mkdir(parents=True)
    path.write_bytes(b"unique source")
    with pytest.raises(ValueError, match="Existing conversion differs"):
        cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert path.read_bytes() == b"unique source"
    with output.open("ab") as stream:
        stream.write(b"corrupt")
    with pytest.raises(ValueError, match="Existing conversion differs"):
        cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert path.read_bytes() == b"unique source"


def test_tardis_export_incomplete_day_does_not_publish_or_delete(tmp_path):
    result = cryptohft_orderbook.export_tardis_day(tmp_path, tmp_path / "out", "binance_futures", "BTCUSDC", "2026-09-05")
    assert result["status"] == "missing_hours" and len(result["hours"]) == 24
    assert not list((tmp_path / "out").rglob("*.parquet"))


def test_default_download_is_raw_fusion_only_and_partial_hours_are_not_success(tmp_path, monkeypatch):
    raw = tmp_path / "hours"
    hour = raw / "binance_futures/2026-09-05/00/BTCUSDC_orderbook.parquet.zst"
    hour.parent.mkdir(parents=True)
    hour.write_bytes(b"unique downloaded input; not yet a complete source day")
    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "canonical"))
    monkeypatch.setattr(cryptohft_orderbook, "_prefetch_raw_hours", lambda **kwargs: {"downloaded": 1})
    monkeypatch.setattr(cryptohft_orderbook, "_process_symbol", lambda **kwargs: pytest.fail("must not build single-source derived"))
    monkeypatch.setattr(sys, "argv", ["download_cryptohft_orderbook", "--start", "2026-09-05",
                                    "--end", "2026-09-05", "--raw-root", str(raw),
                                    "--target-root", str(tmp_path / "derived")])
    with pytest.raises(SystemExit, match="preparation is partial"):
        cryptohft_orderbook.main()
    assert hour.exists() and not (tmp_path / "derived").exists()


@pytest.mark.parametrize("existing_tardis", [False, True])
@pytest.mark.parametrize("representation", [b"reconstructed_fusion.v1", b"observed_union.v1", b"narrowgate.daily_book.v1"])
def test_daily_export_cli_reuses_canonical_after_staging_retirement(tmp_path, monkeypatch, existing_tardis, representation):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data.daily_raw import BOOK_SCHEMA, sha256_file
    from data_paths import daily_market_path

    # The publisher's reconstruction semantics are covered in daily_raw tests.
    # Here verify the downloader consumes inclusion, not single-source SHA equality.
    def fused(sources, output, day, *, symbol="BTCUSDC", **_kwargs):
        assert _kwargs.get("unified_output") is True
        old = pq.read_table(output) if output.exists() else None
        prior = json.loads((old.schema.metadata or {}).get(b"narrowgate.included_sources", b"[]")) if old is not None else []
        if list(sources) == ["canonical"]:
            return dict(day=day, status="REUSED_VERIFIED", rows=len(old), sha256=sha256_file(output),
                        included_sources=prior)
        source = sources["cryptohft"]
        table = pq.read_table(source)
        entry = dict(source_id="cryptohft", sha256=sha256_file(source), rows=len(table), hours=list(range(24)))
        core = ["symbol", "timestamp", "is_snapshot", "side", "price", "amount"]
        combined = table.select(core)
        if old is not None:
            combined = pa.concat_tables([old.select(core).replace_schema_metadata(None), combined])
        metadata = {b"narrowgate.included_sources": json.dumps(prior + [entry]).encode(),
                    b"narrowgate.book_fusion": representation}
        output.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(combined.replace_schema_metadata(metadata), output)
        return dict(day=day, symbol=symbol, path=str(output), status="PUBLISHED", rows=len(combined),
                    sha256=sha256_file(output), consumed_inputs=prior + [entry])

    monkeypatch.setattr("data.daily_raw.fuse_orderbook_day", fused)

    day = "2026-09-05"
    raw_root = tmp_path / "hourly"
    staging = tmp_path / "staging"
    monkeypatch.delenv("NARROWGATE_DAILY_ORDERBOOK_ROOT", raising=False)
    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    canonical = daily_market_path(day, "BTCUSDC", "incremental_book_L2")
    next_source = daily_market_path("2026-09-06", "BTCUSDC", "incremental_book_L2")
    next_source.parent.mkdir(parents=True)
    pq.write_table(pa.table({"event_time": [1788652800000] * 24,
                             "received_time": [1788652800000000000] * 24,
                             "source_hour": list(range(24))}), next_source)
    if existing_tardis:
        canonical.parent.mkdir(parents=True)
        prior = pa.table({"symbol": ["BTCUSDC"], "timestamp": [1788566400000000],
                          "is_snapshot": [True], "side": ["bid"], "price": ["90.00"], "amount": ["1.0"]})
        pq.write_table(prior.replace_schema_metadata({b"narrowgate.included_sources": json.dumps([
            {"source_id": "tardis", "sha256": "a" * 64, "rows": 1}]).encode()}), canonical)
    schema = pa.schema(list(BOOK_SCHEMA)[:13])
    hourly = []
    for hour in range(24):
        path = raw_root / "binance_futures" / day / f"{hour:02d}" / "BTCUSDC_orderbook.parquet.zst"
        path.parent.mkdir(parents=True)
        timestamp = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000) + hour * 3_600_000
        pq.write_table(pa.Table.from_pylist([dict(
            received_time=timestamp * 1_000_000 + 1000, event_time=timestamp,
            transaction_time=timestamp, symbol="BTCUSDC", event_type="update",
            first_update_id=hour + 2, final_update_id=hour + 2,
            prev_final_update_id=hour + 1, last_update_id=None,
            side="bid", price="100.00", quantity="2.000", order_count=None,
        )], schema=schema), path)
        hourly.append(path)
    monkeypatch.setattr(sys, "argv", [
        "download_cryptohft_orderbook", "--symbols", "BTCUSDC",
        "--start", day, "--end", day, "--raw-root", str(raw_root),
        "--tardis-output-root", str(staging), "--tardis-export-only",
    ])
    cryptohft_orderbook.main()
    canonical = daily_market_path(day, "BTCUSDC", "incremental_book_L2")
    assert canonical.is_file() and not canonical.is_symlink()
    assert pq.ParquetFile(canonical).metadata.num_rows == 24 + existing_tardis
    if existing_tardis:
        assert "90.00" in pq.read_table(canonical)["price"].to_pylist()
    assert not any(path.exists() for path in hourly)
    assert not list(staging.rglob("*.parquet"))
    assert len(list(staging.rglob("*.fusion.json"))) == 1
    original_hash = sha256_file(canonical)

    # Neither a removed receipt nor removed hourly inputs causes a fresh download.
    cryptohft_orderbook.main()
    assert sha256_file(canonical) == original_hash
    receipt = staging / "binance_futures/BTCUSDC/incremental_book_L2" / f"{day}.json"
    receipt.write_text(json.dumps({"output_sha256": original_hash}))
    cryptohft_orderbook.main()
    assert not receipt.exists()

    # An interrupted cleanup with conflicting evidence is exposed, not erased.
    receipt.write_text(json.dumps({"output_sha256": "f" * 64}))
    with pytest.raises(ValueError, match="retired staging receipt differs"):
        cryptohft_orderbook.main()
    assert receipt.is_file() and sha256_file(canonical) == original_hash


def test_fusion_batch_prepares_next_day_before_retirement_and_resumes_pending_tail(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data.daily_raw import sha256_file
    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    staging = tmp_path / "raw/.incoming/cryptohft_fusion"
    raw = tmp_path / "hours"
    events = []

    def export(raw_root, output_root, exchange, symbol, day, *, delete_hourly=False, **_kwargs):
        staged = output_root / exchange / symbol / "incremental_book_L2" / f"{day}.parquet"
        receipt = staged.with_suffix(".json")
        if not staged.exists():
            staged.parent.mkdir(parents=True, exist_ok=True)
            timestamp = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
            pq.write_table(pa.table({"event_time": [timestamp] * 24,
                                     "received_time": [timestamp * 1000000] * 24,
                                     "source_hour": list(range(24))}), staged)
            receipt.write_text(json.dumps(dict(day=day, symbol=symbol, rows=24,
                output_sha256=sha256_file(staged), sources=[{"hour": h} for h in range(24)],
                round_trip_all_original_columns=True, status="verified")))
        events.append(("retire" if delete_hourly else "prepare", day))
        return {**json.loads(receipt.read_text()), "originals_deleted": delete_hourly}

    def fuse(sources, output, day, *, next_sources, **_kwargs):
        assert _kwargs.get("unified_output") is True
        events.append(("fuse", day))
        if next_sources:
            assert next_sources["cryptohft"].is_file()
        entry = dict(source_id="cryptohft", sha256=sha256_file(sources["cryptohft"]),
                     rows=24, hours=list(range(24)))
        proof = [dict(source_id="cryptohft", sha256=sha256_file(path), rows=24)
                 for path in next_sources.values()]
        metadata = {b"narrowgate.book_fusion": b"reconstructed_fusion.v1",
                    b"narrowgate.included_sources": json.dumps([entry]).encode(),
                    b"narrowgate.fusion_receipt": json.dumps({"boundary_input_files": proof}).encode()}
        output.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"timestamp": [1]}).replace_schema_metadata(metadata), output)
        return dict(sha256=sha256_file(output), consumed_inputs=[entry], rows=1)

    monkeypatch.setattr(cryptohft_orderbook, "export_tardis_day", export)
    monkeypatch.setattr("data.daily_raw.fuse_orderbook_day", fuse)

    def invoke(start, end):
        monkeypatch.setattr(sys, "argv", ["download", "--symbols", "BTCUSDC", "--start", start,
            "--end", end, "--raw-root", str(raw), "--tardis-output-root", str(staging), "--tardis-export-only"])
        with pytest.raises(SystemExit, match="boundary_pending"):
            cryptohft_orderbook.main()

    invoke("2026-09-05", "2026-09-06")
    assert events[:3] == [("prepare", "2026-09-05"), ("prepare", "2026-09-06"), ("fuse", "2026-09-05")]
    directory = staging / "binance_futures/BTCUSDC/incremental_book_L2"
    assert not (directory / "2026-09-05.parquet").exists()
    assert (directory / "2026-09-06.parquet").is_file()
    assert json.loads((directory / "2026-09-06.fusion.json").read_text())["boundary_status"] == "BOUNDARY_PENDING"
    assert daily_market_path("2026-09-06", "BTCUSDC", "incremental_book_L2").is_file()
    events.clear()
    # A new one-day invocation consumes and cleans the prior pending day; the
    # caller need not re-request or re-download the prior date.
    invoke("2026-09-07", "2026-09-07")
    assert ("retire", "2026-09-06") in events
    assert not (directory / "2026-09-06.parquet").exists()
    assert (directory / "2026-09-07.parquet").is_file()
    # The terminal day cannot silently become complete if its retained context
    # is removed outside this transaction.
    (directory / "2026-09-07.parquet").unlink()
    with pytest.raises(ValueError, match="Pending CryptoHFT boundary source is missing"):
        cryptohft_orderbook.main()


@pytest.mark.parametrize("next_kind", ["fused", "union", "tardis", "partial_native", "complete_native"])
def test_crypto_boundary_context_requires_actual_complete_native_capture_day(tmp_path, monkeypatch, next_kind):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path))
    candidate = daily_market_path("2026-09-06", "BTCUSDC", "incremental_book_L2")
    candidate.parent.mkdir(parents=True)
    hours = list(range(24)) if next_kind != "partial_native" else [0]
    n = len(hours)
    table = pa.table({"event_time": [None if next_kind == "tardis" else 1788652800000] * n,
                      "received_time": [None if next_kind == "tardis" else 1788652800000000000] * n,
                      "source_hour": hours})
    if next_kind in {"fused", "union"}:
        marker = b"observed_union.v1" if next_kind == "union" else b"reconstructed_fusion.v1"
        table = table.replace_schema_metadata({b"narrowgate.book_fusion": marker})
    pq.write_table(table, candidate)
    result = cryptohft_orderbook._crypto_boundary_context("2026-09-05", "BTCUSDC")
    assert bool(result["next_sources"]) is (next_kind == "complete_native")
    assert result["status"] == ("NEXT_CAPTURE_DAY_AVAILABLE" if next_kind == "complete_native" else "BOUNDARY_PENDING")


@pytest.mark.parametrize("representation", [b"reconstructed_fusion.v1", b"observed_union.v1"])
def test_crypto_boundary_context_keeps_existing_verified_fusion_proof_without_next_original(tmp_path, monkeypatch, representation):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path))
    canonical = daily_market_path("2026-09-05", "BTCUSDC", "incremental_book_L2")
    canonical.parent.mkdir(parents=True)
    proof = [{"source_id": "cryptohft", "sha256": "a" * 64, "rows": 99}]
    pq.write_table(pa.table({"timestamp": [1]}).replace_schema_metadata({
        b"narrowgate.book_fusion": representation,
        b"narrowgate.fusion_receipt": json.dumps({"day": "2026-09-05", "symbol": "BTCUSDC",
                                                 "boundary_input_files": proof}).encode()}), canonical)
    result = cryptohft_orderbook._crypto_boundary_context("2026-09-05", "BTCUSDC")
    assert result["status"] == "EXISTING_CANONICAL_BOUNDARY_VERIFIED"
    assert result["next_sources"] == {} and result["inherited_verified_boundary"] == proof
    assert result["raw_representation"] == representation.decode()


@pytest.mark.parametrize("representation", [b"reconstructed_fusion.v1", b"observed_union.v1", b"unknown"])
def test_late_union_metadata_never_masquerades_as_lossless_capture_hour(tmp_path, monkeypatch, representation):
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = tmp_path / "raw.parquet"
    table = pa.table({"source_hour": [3], "source_id": ["cryptohft"],
                      "source_native_sequence": [True], "source_row": [0]})
    with pq.ParquetWriter(source, table.schema) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata({b"narrowgate.book_fusion": representation,
            b"narrowgate.included_sources": json.dumps([
                {"source_id": "cryptohft", "sha256": "a" * 64, "rows": 1, "hours": [3]}
            ]).encode()})
    assert b"narrowgate.book_fusion" not in (pq.ParquetFile(source).schema_arrow.metadata or {})
    monkeypatch.setattr(cryptohft_orderbook, "daily_raw_for_hour", lambda _: source)
    hour = tmp_path / "binance_futures/2026-09-05/03/BTCUSDC_orderbook.parquet.zst"
    if representation == b"unknown":
        with pytest.raises(ValueError, match="unrecognized canonical"):
            cryptohft_orderbook.raw_hour_available(hour)
    else:
        assert cryptohft_orderbook.raw_hour_available(hour)
        assert not cryptohft_orderbook._daily_contains_crypto_hour(source, 4)
    match = "source-aware reader" if representation == b"observed_union.v1" else "cannot reconstruct"
    with pytest.raises(ValueError, match=match):
        cryptohft_orderbook._decompress_parquet_zst(hour)


@pytest.mark.parametrize("mode", ["original", "preceding_update_id"])
def test_normalized_buckets_use_sequence_anchored_snapshot_clock(tmp_path, mode):
    hour = int(pd.Timestamp("2026-08-22T14:00:00Z").timestamp() * 1000)
    rows = []
    for kind, ts, identifier, previous in [
        ("snapshot", hour - 2000, 10, None),
        ("update", hour - 200, 11, 10),
        ("snapshot", hour, 11, None),
        ("update", hour - 100, 12, 11),
        ("update", hour + 100, 13, 12),
    ]:
        for side, price in [("bid", 100.), ("ask", 101.)]:
            rows.append(dict(event_type=kind, event_time=ts,
                             transaction_time=0 if kind == "snapshot" else ts,
                             received_time=ts + 5, first_update_id=identifier,
                             final_update_id=identifier, prev_final_update_id=previous,
                             last_update_id=identifier if kind == "snapshot" else None,
                             side=side, price=price, quantity=2.))
    path = tmp_path / "raw.parquet"
    pd.DataFrame(rows).to_parquet(path)
    book = OrderBookState()
    state = OrderBookSequenceState(book, recorder_snapshot_clock=mode)
    writer = DailyOutputWriter([tmp_path / "processed"], "BTCUSDC", 1)
    _, end = cryptohft_orderbook._replay_orderbook_file(
        path, book, writer, 1, 100, hour - 3000, None, None, state, "transaction", 0,
    )
    cryptohft_orderbook._emit_snapshot(book, end, writer, 1, hour - 3000, state, 0)
    writer.close()
    assert state.stats.snapshot_sequence_anchors == (mode != "original")
    assert state.stats.message_time_reversals == (mode == "original")
    if mode != "original":
        bbo = pd.read_parquet(tmp_path / "processed/bbo/BTCUSDC-bbo-2026-08-22.parquet")
        assert bbo.timestamp.is_monotonic_increasing
        assert hour not in bbo.timestamp.tolist()
        assert bbo.timestamp.tolist() == [hour - 2000, hour - 200, hour - 100, hour + 100]


def test_default_normalized_output_is_versioned_staging() -> None:
    roots = _default_target_roots()

    assert len(roots) == 1
    assert roots[0].name == "replay_l2_retained100ms_staging"


def test_bad_day_repair_csv_is_validated_and_boolean_is_parsed(tmp_path):
    manifest = tmp_path / "bad_days.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "yes",
                "missing_raw_hours": "19,20,21",
            },
            {
                "symbol": "BTCUSDT",
                "date": "2025-08-14",
                "cause": "raw_has_no_snapshots",
                "suggested_fix": "not_fixable_by_redownload",
                "redownload_can_fix": "0",
                "missing_raw_hours": "",
            },
        ]
    ).to_csv(manifest, index=False)

    repairs = _load_bad_day_repairs(manifest)

    assert repairs == [
        BadDayRepair(
            symbol="BTCUSDC",
            date="2026-05-26",
            cause="missing_raw_hours",
            suggested_fix="retry_download",
            redownload_can_fix=True,
            missing_raw_hours=("19", "20", "21"),
        ),
        BadDayRepair(
            symbol="BTCUSDT",
            date="2025-08-14",
            cause="raw_has_no_snapshots",
            suggested_fix="not_fixable_by_redownload",
            redownload_can_fix=False,
            missing_raw_hours=(),
        ),
    ]

    missing_column = tmp_path / "bad_days_missing_column.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(missing_column, index=False)
    with pytest.raises(ValueError, match="suggested_fix"):
        _load_bad_day_repairs(missing_column)

    invalid_boolean = tmp_path / "bad_days_invalid_boolean.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "maybe",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(invalid_boolean, index=False)
    with pytest.raises(ValueError, match="redownload_can_fix"):
        _load_bad_day_repairs(invalid_boolean)

    empty_symbol = tmp_path / "bad_days_empty_symbol.csv"
    pd.DataFrame(
        [
            {
                "symbol": "",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(empty_symbol, index=False)
    with pytest.raises(ValueError, match="empty symbol"):
        _load_bad_day_repairs(empty_symbol)

    unknown_fix = tmp_path / "bad_days_unknown_fix.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "unknown_action",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(unknown_fix, index=False)
    with pytest.raises(ValueError, match="invalid suggested_fix"):
        _load_bad_day_repairs(unknown_fix)


def test_bad_day_repair_selection_skips_nonfixable_by_default():
    repairs = [
        BadDayRepair(
            symbol="BTCUSDC",
            date="2026-05-26",
            cause="missing_raw_hours",
            suggested_fix="retry_download",
            redownload_can_fix=True,
            missing_raw_hours=("19",),
        ),
        BadDayRepair(
            symbol="BTCUSDT",
            date="2026-05-07",
            cause="normalized_gap_with_snapshots",
            suggested_fix="force_rebuild_from_raw",
            redownload_can_fix=True,
            missing_raw_hours=(),
        ),
        BadDayRepair(
            symbol="BTCUSDC",
            date="2025-08-14",
            cause="raw_has_no_snapshots",
            suggested_fix="not_fixable_by_redownload",
            redownload_can_fix=False,
            missing_raw_hours=(),
        ),
    ]

    assert _select_bad_day_repairs(repairs) == repairs[:2]
    assert _select_bad_day_repairs(
        repairs,
        symbols={"BTCUSDC"},
        causes={"missing_raw_hours"},
    ) == repairs[:1]
    assert _select_bad_day_repairs(
        repairs,
        symbols={"BTCUSDC"},
        include_nonfixable=True,
    ) == [repairs[0], repairs[2]]
    assert _select_bad_day_repairs(
        repairs,
        include_nonfixable=True,
        limit=1,
    ) == repairs[:1]


def test_bad_day_repair_raw_paths_include_exchange_and_respect_scope(
    tmp_path,
):
    repair = BadDayRepair(
        symbol="BTCUSDC",
        date="2026-05-26",
        cause="missing_raw_hours",
        suggested_fix="retry_download",
        redownload_can_fix=True,
        missing_raw_hours=("03", "19"),
    )

    selected = _raw_paths_for_repair(
        tmp_path,
        "binance_futures",
        repair,
    )
    assert selected == [
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "03"
        / "BTCUSDC_orderbook.parquet.zst",
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "19"
        / "BTCUSDC_orderbook.parquet.zst",
    ]

    entire_day = _raw_paths_for_repair(
        tmp_path,
        "binance_futures",
        repair,
        refresh_entire_day=True,
    )
    assert len(entire_day) == 24
    assert entire_day[0] == (
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "00"
        / "BTCUSDC_orderbook.parquet.zst"
    )
    assert entire_day[-1] == (
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "23"
        / "BTCUSDC_orderbook.parquet.zst"
    )


def test_classified_refresh_action_is_honored_without_cli_override():
    repair = BadDayRepair(
        symbol="BTCUSDC",
        date="2026-05-26",
        cause="raw_decode_errors",
        suggested_fix="refresh_raw_and_rebuild",
        redownload_can_fix=True,
        missing_raw_hours=(),
    )

    scope = cryptohft_orderbook._effective_repair_refresh_scope(
        repair,
        "none",
    )

    assert scope == "day"
    assert cryptohft_orderbook._repair_action(repair, scope) == (
        "refresh_raw_and_rebuild"
    )


def test_repair_raw_refresh_validates_before_atomic_replace(
    tmp_path,
    monkeypatch,
):
    raw_root = tmp_path / "raw"
    raw_path = (
        raw_root
        / "binance_futures"
        / "2026-05-26"
        / "19"
        / "BTCUSDC_orderbook.parquet.zst"
    )
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"old-cache")

    class SuccessfulClient:
        def download_file(self, relative_path, out_path):
            assert relative_path == raw_path.relative_to(raw_root)
            out_path.write_bytes(b"validated-refresh")
            return "downloaded"

    monkeypatch.setattr(
        cryptohft_orderbook,
        "_read_raw_parquet_zst_summary",
        lambda path: ([1], {}),
    )
    assert (
        cryptohft_orderbook._refresh_repair_raw_files(
            SuccessfulClient(),
            raw_root,
            [raw_path],
        )
        == 1
    )
    assert raw_path.read_bytes() == b"validated-refresh"

    raw_path.write_bytes(b"preserve-on-failure")

    class FailingClient:
        def download_file(self, relative_path, out_path):
            del relative_path
            out_path.write_bytes(b"partial-refresh")
            raise RuntimeError("network failed")

    with pytest.raises(RuntimeError, match="network failed"):
        cryptohft_orderbook._refresh_repair_raw_files(
            FailingClient(),
            raw_root,
            [raw_path],
        )
    assert raw_path.read_bytes() == b"preserve-on-failure"
    assert not raw_path.with_suffix(raw_path.suffix + ".refresh").exists()


def test_bad_day_repair_main_dry_run_has_no_side_effects(
    tmp_path,
    monkeypatch,
    capsys,
):
    manifest = tmp_path / "bad_days.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19,20",
            }
        ]
    ).to_csv(manifest, index=False)
    raw_root = tmp_path / "raw"
    target_root = tmp_path / "target"

    def unexpected_call(*args, **kwargs):
        del args, kwargs
        raise AssertionError("dry-run attempted a mutating repair operation")

    monkeypatch.delenv("CRYPTOHFTDATA_API_KEY", raising=False)
    monkeypatch.delenv("CRYPTOHFTDATA_JWT", raising=False)
    monkeypatch.setattr(
        cryptohft_orderbook,
        "CryptoHFTClient",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_prefetch_raw_hours",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_refresh_repair_raw_files",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_process_symbol",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_audit_days",
        unexpected_call,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_cryptohft_orderbook.py",
            "--repair-audit-csv",
            str(manifest),
            "--symbols",
            "BTCUSDC",
            "--start",
            "2026-05-26",
            "--end",
            "2026-05-26",
            "--repair-refresh-raw",
            "listed",
            "--raw-root",
            str(raw_root),
            "--target-root",
            str(target_root),
            "--dry-run",
        ],
    )

    cryptohft_orderbook.main()

    output = capsys.readouterr().out
    assert "dry-run" in output
    assert "no raw or normalized files were changed" in output
    assert not raw_root.exists()
    assert not target_root.exists()


@pytest.mark.parametrize("eligible", [True, False])
def test_retry_download_forwards_credentials_and_enforces_post_audit(
    tmp_path,
    monkeypatch,
    capsys,
    eligible,
):
    manifest = tmp_path / "bad_days.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "true",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(manifest, index=False)
    raw_root = tmp_path / "raw"
    target_root = tmp_path / "target"
    captured = {}
    fake_client = object()

    def build_client(*, api_key, jwt, transport):
        captured["client"] = {
            "api_key": api_key,
            "jwt": jwt,
            "transport": transport,
        }
        return fake_client

    def prefetch_raw_hours(**kwargs):
        captured["prefetch"] = kwargs
        return {"downloaded": 1, "exists": 0, "404": 0}

    def process_symbol(**kwargs):
        captured["process"] = kwargs
        return (
            1,
            0,
            100,
            {
                "day_sequence_audits": {
                    "2026-05-26": {
                        "target_initialized_at_start": True,
                    }
                }
            },
        )

    def audit_days(*args, **kwargs):
        captured["audit"] = {"args": args, "kwargs": kwargs}
        return pd.DataFrame([{"eligible": eligible}])

    def unexpected_refresh(*args, **kwargs):
        del args, kwargs
        raise AssertionError("retry_download unexpectedly forced raw refresh")

    monkeypatch.delenv("CRYPTOHFTDATA_API_KEY", raising=False)
    monkeypatch.delenv("CRYPTOHFTDATA_JWT", raising=False)
    monkeypatch.setattr(
        cryptohft_orderbook,
        "CryptoHFTClient",
        build_client,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_prefetch_raw_hours",
        prefetch_raw_hours,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_refresh_repair_raw_files",
        unexpected_refresh,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_process_symbol",
        process_symbol,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_audit_days",
        audit_days,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_cryptohft_orderbook.py",
            "--repair-audit-csv",
            str(manifest),
            "--symbols",
            "BTCUSDC",
            "--start",
            "2026-05-26",
            "--end",
            "2026-05-26",
            "--raw-root",
            str(raw_root),
            "--target-root",
            str(target_root),
            "--api-key",
            "api-key-sentinel",
            "--jwt",
            "jwt-sentinel",
            "--transport",
            "rest",
            "--legacy-provider-normalize",
        ],
    )

    if eligible:
        cryptohft_orderbook.main()
    else:
        with pytest.raises(SystemExit, match="post-audit failed"):
            cryptohft_orderbook.main()

    assert captured["client"] == {
        "api_key": "api-key-sentinel",
        "jwt": "jwt-sentinel",
        "transport": "rest",
    }
    assert captured["prefetch"]["api_key"] == "api-key-sentinel"
    assert captured["prefetch"]["jwt"] == "jwt-sentinel"
    assert captured["prefetch"]["transport"] == "rest"
    assert captured["prefetch"]["raw_root"] == raw_root
    assert captured["prefetch"]["symbols"] == ["BTCUSDC"]
    assert captured["process"]["client"] is fake_client
    assert captured["process"]["download_missing"] is True
    output = capsys.readouterr().out
    assert "api-key-sentinel" not in output
    assert "jwt-sentinel" not in output


def test_orderbook_top_levels_do_not_repeat_updated_or_readded_prices():
    book = OrderBookState()
    book.apply("bid", 100.0, 1.0)
    book.apply("bid", 100.0, 2.0)
    book.apply("bid", 99.0, 3.0)
    book.apply("bid", 100.0, 0.0)
    book.apply("bid", 100.0, 4.0)

    bids, _ = book.top_levels(5)

    assert bids == [(100.0, 4.0), (99.0, 3.0)]


def test_daily_normalization_start_does_not_truncate_existing_day():
    requested_hour = datetime(2026, 7, 15, 18, 37, tzinfo=timezone.utc)

    assert _daily_write_start(requested_hour) == datetime(
        2026, 7, 15, 0, 0, tzinfo=timezone.utc
    )


def test_sequence_state_requires_snapshot_and_invalidates_gap():
    book = OrderBookState()
    sequence = OrderBookSequenceState(book)

    def begin(
        *,
        event_type="update",
        receive=1_000,
        event=900,
        transaction=890,
        first=None,
        final=None,
        previous=None,
        last=None,
    ):
        return sequence.begin_message(
            event_type=event_type,
            receive_time_ms=receive,
            event_time_ms=event,
            transaction_time_ms=transaction,
            first_update_id=first,
            final_update_id=final,
            previous_final_update_id=previous,
            last_update_id=last,
        )

    assert not begin(first=1, final=2, previous=0)
    assert begin(event_type="snapshot", receive=1_100, event=1_000, last=100)
    book.apply("bid", 99.0, 2.0)
    # Rows from one native snapshot may carry different recorder receive
    # timestamps. They remain one logical snapshot and must all apply.
    assert begin(event_type="snapshot", receive=1_200, event=1_000, last=100)
    assert book.top_levels(1)[0] == [(99.0, 2.0)]

    # The first delta spans the REST snapshot ID; pu may precede it.
    assert begin(receive=1_300, event=1_250, first=100, final=105, previous=98)
    assert begin(receive=1_400, event=1_350, first=106, final=109, previous=105)

    # A subsequent pu mismatch invalidates the full book until a new snapshot.
    assert not begin(receive=1_500, event=1_450, first=110, final=112, previous=107)
    assert book.top_levels(1) == ([], [])
    assert not begin(receive=1_600, event=1_550, first=113, final=114, previous=112)
    assert begin(event_type="snapshot", receive=1_700, event=1_650, last=120)
    assert begin(receive=1_800, event=1_750, first=125, final=130, previous=120)

    assert sequence.stats.duplicate_snapshots == 0
    assert sequence.stats.sequence_gaps == 1
    assert sequence.stats.ignored_before_snapshot == 2
    assert sequence.stats.message_intervals == 7
    assert sequence.stats.message_interval_le_100ms == 7
    assert sequence.stats.message_time_reversals == 0


def test_delta_bootstrap_is_explicit_and_waits_for_convergence():
    book = OrderBookState()
    sequence = OrderBookSequenceState(book, allow_delta_bootstrap=True)

    assert sequence.begin_message(
        event_type="update",
        receive_time_ms=1_010,
        event_time_ms=1_000,
        transaction_time_ms=1_000,
        first_update_id=101,
        final_update_id=105,
        previous_final_update_id=100,
        last_update_id=None,
    )
    book.apply("bid", 100.0, 1.0)

    assert sequence.initialization_source == "delta"
    assert sequence.stats.delta_bootstrap_messages == 1
    assert not sequence.output_ready(60_999, 60_000)
    assert sequence.output_ready(61_000, 60_000)

    assert sequence.begin_message(
        event_type="update",
        receive_time_ms=61_010,
        event_time_ms=61_000,
        transaction_time_ms=61_000,
        first_update_id=106,
        final_update_id=110,
        previous_final_update_id=105,
        last_update_id=None,
    )
    assert not sequence.begin_message(
        event_type="update",
        receive_time_ms=61_110,
        event_time_ms=61_100,
        transaction_time_ms=61_100,
        first_update_id=111,
        final_update_id=115,
        previous_final_update_id=109,
        last_update_id=None,
    )
    assert not sequence.output_ready(120_000, 60_000)


def test_native_snapshot_does_not_require_delta_burn_in():
    book = OrderBookState()
    sequence = OrderBookSequenceState(book, allow_delta_bootstrap=True)

    assert sequence.begin_message(
        event_type="snapshot",
        receive_time_ms=1_010,
        event_time_ms=1_000,
        transaction_time_ms=1_000,
        first_update_id=None,
        final_update_id=None,
        previous_final_update_id=None,
        last_update_id=100,
    )

    assert sequence.initialization_source == "snapshot"
    assert sequence.output_ready(1_000, 60_000)


@pytest.mark.parametrize("clock", ["event", "transaction"])
def test_exchange_clock_never_falls_back_to_receive_time(clock):
    frame = pd.DataFrame({"transaction_time": [0], "event_time": [0],
                          "received_time": [1_700_000_000_000_000_000]})
    with pytest.raises(ValueError, match="cannot substitute"):
        _select_ts_ms(frame, clock)
    assert _select_ts_ms(frame, "received").tolist() == [1_700_000_000_000]


def test_provider_clock_requires_real_receive_time():
    with pytest.raises(ValueError, match="cannot substitute"):
        _select_ts_ms(pd.DataFrame({"event_time": [1700000000000]}), "received")


def test_timestamp_source_can_match_live_transaction_clock():
    frame = pd.DataFrame(
        {
            "event_time": [1_000, 2_000],
            "transaction_time": [990, None],
            "received_time": [1_100_000_000, 2_100_000_000],
        }
    )

    assert _select_ts_ms(frame, "transaction").tolist() == [990, 2_000]
    assert _select_ts_ms(frame, "event").tolist() == [1_000, 2_000]


def test_retained_manifest_is_strict_and_deduplicated(tmp_path):
    manifest = tmp_path / "retained.csv"
    manifest.write_text(
        "day\n2026-01-03\n2026-01-01\n2026-01-03\n",
        encoding="utf-8",
    )

    assert _load_retained_days(manifest) == ["2026-01-01", "2026-01-03"]


def test_retained_days_are_grouped_without_bridging_bad_days():
    ranges = _contiguous_day_ranges(
        ["2026-01-01", "2026-01-03", "2026-01-04"]
    )

    assert [
        (start.isoformat(), end.isoformat())
        for start, end in ranges
    ] == [
        (
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T23:00:00+00:00",
        ),
        (
            "2026-01-03T00:00:00+00:00",
            "2026-01-04T23:00:00+00:00",
        ),
    ]


def test_independent_retained_days_receive_separate_warmup_ranges():
    ranges = _retained_process_ranges(
        ["2026-01-03", "2026-01-04"],
        independent_days=True,
        sequence_bootstrap="snapshot",
    )

    assert [
        (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        for start, end in ranges
    ] == [
        ("2026-01-03", "2026-01-03"),
        ("2026-01-04", "2026-01-04"),
    ]


def test_contiguous_retained_ranges_can_be_bounded_for_parallel_balance():
    ranges = _retained_process_ranges(
        [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
            "2026-01-04",
            "2026-01-05",
        ],
        independent_days=False,
        sequence_bootstrap="snapshot",
        max_days=2,
    )

    assert [
        (start.isoformat(), end.isoformat())
        for start, end in ranges
    ] == [
        (
            "2026-01-01T00:00:00+00:00",
            "2026-01-02T23:00:00+00:00",
        ),
        (
            "2026-01-03T00:00:00+00:00",
            "2026-01-04T23:00:00+00:00",
        ),
        (
            "2026-01-05T00:00:00+00:00",
            "2026-01-05T23:00:00+00:00",
        ),
    ]


def test_target_scoped_sequence_audit_ignores_recovered_warmup_gap():
    passed, gaps, delta_bootstrap = _sequence_audit_status(
        {
            "sequence_gaps": 2,
            "target_initialized_at_start": True,
            "target_initialization_source_at_start": "snapshot",
            "target_accepted_updates": 100,
            "target_sequence_gaps": 0,
            "target_invalid_sequence_messages": 0,
            "target_message_time_reversals": 0,
            "target_delta_bootstrap_messages": 0,
        }
    )

    assert passed
    assert gaps == 0
    assert delta_bootstrap == 0


def test_target_scoped_sequence_audit_requires_snapshot_seed_at_midnight():
    passed, _, _ = _sequence_audit_status(
        {
            "target_initialized_at_start": True,
            "target_initialization_source_at_start": "delta",
            "target_accepted_updates": 100,
            "target_sequence_gaps": 0,
            "target_invalid_sequence_messages": 0,
            "target_message_time_reversals": 0,
        }
    )

    assert not passed


def test_daily_writer_does_not_emit_non_retained_days(tmp_path):
    writer = DailyOutputWriter(
        [tmp_path],
        "BTCUSDC",
        1,
        allowed_days={"2026-01-03"},
    )
    levels = [(100.0, 1.0)]
    writer.append(
        int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000),
        levels,
        [(100.1, 1.0)],
    )
    writer.append(
        int(datetime(2026, 1, 3, tzinfo=timezone.utc).timestamp() * 1000),
        levels,
        [(100.1, 1.0)],
    )
    writer.close()

    assert not (tmp_path / "bbo" / "BTCUSDC-bbo-2026-01-02.parquet").exists()
    assert (tmp_path / "bbo" / "BTCUSDC-bbo-2026-01-03.parquet").exists()


def test_sequence_audit_manifest_requires_one_range_per_day(tmp_path):
    path = tmp_path / "sequence.json"
    path.write_text(
        json.dumps(
            {
                "range_audits": [
                    {
                        "range_start_utc": "2026-01-03T00:00:00+00:00",
                        "range_end_utc": "2026-01-03T23:00:00+00:00",
                        "sequence_audit": {
                            "accepted_updates": 10,
                            "sequence_gaps": 0,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    _, by_day = _load_per_day_sequence_audits(path)
    assert by_day == {
        "2026-01-03": {
            "accepted_updates": 10,
            "sequence_gaps": 0,
        }
    }

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["range_audits"][0]["range_end_utc"] = (
        "2026-01-04T23:00:00+00:00"
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="one sequence audit range"):
        _load_per_day_sequence_audits(path)


def test_sequence_audit_manifest_accepts_target_scoped_days_in_one_range(
    tmp_path,
):
    path = tmp_path / "sequence.json"
    path.write_text(
        json.dumps(
            {
                "range_audits": [
                    {
                        "range_start_utc": "2026-01-03T00:00:00+00:00",
                        "range_end_utc": "2026-01-04T23:00:00+00:00",
                        "sequence_audit": {
                            "day_sequence_audits": {
                                "2026-01-03": {
                                    "target_initialized_at_start": True,
                                    "target_initialization_source_at_start": (
                                        "snapshot"
                                    ),
                                    "target_accepted_updates": 10,
                                    "target_sequence_gaps": 0,
                                },
                                "2026-01-04": {
                                    "target_initialized_at_start": True,
                                    "target_initialization_source_at_start": (
                                        "snapshot"
                                    ),
                                    "target_accepted_updates": 11,
                                    "target_sequence_gaps": 0,
                                },
                            }
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    _, by_day = _load_per_day_sequence_audits(path)

    assert sorted(by_day) == ["2026-01-03", "2026-01-04"]
    assert by_day["2026-01-04"]["target_accepted_updates"] == 11


def test_sequence_audit_manifest_keeps_same_day_for_multiple_symbols(
    tmp_path,
):
    path = tmp_path / "sequence.json"
    path.write_text(
        json.dumps(
            {
                "range_audits": [
                    {
                        "symbol": symbol,
                        "range_start_utc": "2026-01-03T00:00:00+00:00",
                        "range_end_utc": "2026-01-03T23:00:00+00:00",
                        "sequence_audit": {
                            "accepted_updates": accepted_updates,
                            "sequence_gaps": 0,
                        },
                    }
                    for symbol, accepted_updates in (
                        ("BTCUSDC", 10),
                        ("BTCUSDT", 11),
                    )
                ]
            }
        ),
        encoding="utf-8",
    )

    _, by_symbol_day = _load_per_day_sequence_audits(path)

    assert sorted(by_symbol_day) == [
        "BTCUSDC:2026-01-03",
        "BTCUSDT:2026-01-03",
    ]
    assert by_symbol_day["BTCUSDT:2026-01-03"]["accepted_updates"] == 11
