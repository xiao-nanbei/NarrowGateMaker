from __future__ import annotations

import csv
import io
import json
import lzma
import shutil
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard

from data.daily_raw import (
    BOOK_SCHEMA, CHANNEL_SOURCES, FUSED_BOOK_SCHEMA, UNION_BOOK_SCHEMA, _logical_digest, convert_auxiliary_csv,
    book_stream_priority, book_stream_ranks, convert_day_channel, fuse_orderbook_day, migrate_manifest,
)


def test_stream_priority_contract_is_anonymous_serializable_and_legacy_equivalent():
    legacy = book_stream_priority()
    assert book_stream_ranks(legacy) == {"cryptohft": -1, "tardis": 1, "canonical": -3}
    explicit = book_stream_priority(["stream-z", "stream-a"], preferred_index=0)
    assert book_stream_priority(contract=json.loads(json.dumps(explicit))) == explicit
    assert book_stream_ranks(explicit) == {"stream-z": 1, "stream-a": -2}
    with pytest.raises(ValueError, match="identities differ"):
        book_stream_priority(["stream-a", "stream-z"], contract=explicit)
    with pytest.raises(ValueError, match="preference differs"):
        book_stream_priority(preferred_index=1, contract=explicit)
    with pytest.raises(ValueError, match="unique"):
        book_stream_priority(["stream-a", "stream-a"])
    with pytest.raises(ValueError, match="identities are missing"):
        book_stream_priority(contract={"schema": "book_stream_priority.v1"})


def test_anonymous_normalizer_priority_matches_native_and_continuation(tmp_path):
    from data.normalize_tardis_orderbook import iter_fused_book_batches

    pytest.importorskip("narrowgate_cpp")
    start = 1754006400000000
    ids = ["stream-z", "stream-a"]
    sources = []
    for index, identity in enumerate(ids):
        path = tmp_path / f"{identity}.parquet"
        rows = [{"exchange": "binance-futures", "symbol": "BTCUSDC",
                 "timestamp": start + 1000, "local_timestamp": start + 2000,
                 "is_snapshot": True, "event_type": "snapshot", "side": side,
                 "price": str(price), "amount": str(index + 1), "quantity": str(index + 1)}
                for side, price in [("bid", 100), ("bid", 99), ("ask", 101), ("ask", 102)]]
        pq.write_table(pa.Table.from_pylist(rows, schema=BOOK_SCHEMA), path)
        sources.append({"source_id": identity, "path": path, "native_sequence": False})
    priority = book_stream_priority(ids, preferred_index=0)
    batches, stats = iter_fused_book_batches(sources, "2025-08-01", minimum_levels=2,
                                            observed_union=True, stream_priority=priority)
    assert sum(len(batch) for batch in batches) == 8
    state = stats["continuation"]
    assert state["stream_priority"] == priority
    assert state["kernel"]["selected_source"] == 0
    assert state["kernel"]["preferred"] == 0
    with pytest.raises(ValueError, match="stream priority differs"):
        iter_fused_book_batches(sources, "2025-08-02", minimum_levels=2,
                               observed_union=True, previous_state=state,
                               stream_priority=book_stream_priority(ids, preferred_index=1))


def _fusion_input(path, *, native=False, price="100.0"):
    stamp = 1754006400000000
    rows = [{"exchange": "binance-futures", "symbol": "BTCUSDC", "timestamp": stamp,
             "local_timestamp": stamp + 1000, "is_snapshot": True, "event_type": "snapshot",
             "price": price, "amount": "1.000", "quantity": "1.000", "side": "bid",
             "source_row": 0, "source_hour": 0 if native else None,
             "event_time": stamp // 1000 if native else None,
             "received_time": (stamp + 1000) * 1000 if native else None}]
    pq.write_table(pa.Table.from_pylist(rows, schema=BOOK_SCHEMA), path)


def _stub_fusion(monkeypatch, calls, fail=False, options=None):
    import data.normalize_tardis_orderbook as normalizer

    def generate(sources, day, *, symbol, **kwargs):
        calls.append(dict(sources))
        if options is not None:
            options.append(kwargs)
        stats = {"input_sources": len(sources)}

        def tables():
            if fail:
                raise ValueError("injected fusion failure")
            for source_id, path in sources.items():
                row = pq.read_table(path).to_pylist()[0]
                row.update(source_id=source_id, fusion_reason="initial_snapshot",
                           source_observed_timestamp_us=row["timestamp"])
                if kwargs.get("observed_union"):
                    row.update(fusion_reason="source_observation", source_timestamp_us=row["timestamp"],
                               source_native_sequence=source_id == "cryptohft")
                yield pa.Table.from_pylist([row], schema=UNION_BOOK_SCHEMA if kwargs.get("observed_union") else FUSED_BOOK_SCHEMA)
            stats["finished"] = True
            if options is not None:
                stats["continuation"] = {"test_day": day}

        return tables(), stats

    monkeypatch.setattr(normalizer, "iter_fused_book_batches", generate, raising=False)


def test_real_fusion_batches_publish_roundtrip_and_inherit_next_day(tmp_path):
    pytest.importorskip("narrowgate_cpp")
    start = 1754006400000000
    day_us = 86_400_000_000
    source = tmp_path / "source.parquet"
    source2 = tmp_path / "source2.parquet"

    def snapshot(path, stamp, amount):
        rows = [{"exchange": "binance-futures", "symbol": "BTCUSDC",
                 "timestamp": stamp, "local_timestamp": stamp + 1000,
                 "is_snapshot": True, "event_type": "snapshot", "side": side,
                 "price": str(price), "amount": str(amount), "quantity": str(amount),
                 "source_row": index}
                for side, prices in (("bid", range(100, 80, -1)), ("ask", range(101, 121)))
                for index, price in enumerate(prices)]
        pq.write_table(pa.Table.from_pylist(rows, schema=BOOK_SCHEMA), path)

    snapshot(source, start + day_us - 100_000, 1)
    snapshot(source2, start + day_us + 100_000, 2)
    first_path = tmp_path / "raw/2025-08-01/incremental_book_L2.parquet"
    second_path = tmp_path / "raw/2025-08-02/incremental_book_L2.parquet"
    first = fuse_orderbook_day({"tardis": source}, first_path, "2025-08-01")
    second = fuse_orderbook_day({"tardis": source2}, second_path, "2025-08-02")
    assert first["rows"] == 40
    assert second["rows"] == 40
    rows = pq.read_table(second_path).to_pylist()
    opening = [row for row in rows if row["timestamp"] == start + day_us]
    assert opening == []  # Inherited state belongs to the derived view, not new raw observations.
    assert {row["fusion_reason"] for row in rows} == {"source_observation"}
    assert all(row["source_observed_timestamp_us"] <= row["timestamp"] for row in rows)
    receipt = json.loads(pq.ParquetFile(second_path).metadata.metadata[b"narrowgate.fusion_receipt"])
    assert receipt["previous_day_receipt_sha256"]
    assert source.exists() and source2.exists()


def test_book_fusion_adds_existing_other_source_and_embeds_inclusion(tmp_path, monkeypatch):
    output, incoming = tmp_path / "canonical.parquet", tmp_path / "crypto.parquet"
    _fusion_input(output)
    _fusion_input(incoming, native=True)
    calls = []
    _stub_fusion(monkeypatch, calls)
    result = fuse_orderbook_day({"cryptohft": incoming}, output, "2025-08-01")
    assert set(calls[0]) == {"cryptohft", "tardis"}
    assert {r["source_id"] for r in result["included_sources"]} == {"cryptohft", "tardis"}
    metadata = pq.ParquetFile(output).metadata.metadata
    assert metadata[b"narrowgate.book_fusion"] == b"observed_union.v1"
    receipt = json.loads(metadata[b"narrowgate.fusion_receipt"])
    assert receipt["stats"]["finished"] and receipt["output_rows"] == result["rows"] == 2
    assert json.loads(metadata[b"narrowgate.included_sources"]) == result["included_sources"]
    assert next(r for r in result["included_sources"] if r["source_id"] == "cryptohft")["hours"] == [0]
    assert incoming.exists()


@pytest.mark.parametrize("hardlink", [False, True])
def test_fusion_source_context_copy_is_not_added_as_third_clock(tmp_path, monkeypatch, hardlink):
    original, context, tardis = [tmp_path / name for name in ("raw.parquet", "context.parquet", "tardis.parquet")]
    _fusion_input(original, native=True)
    _fusion_input(tardis)
    if hardlink:
        context.hardlink_to(original)
    else:
        shutil.copyfile(original, context)
    calls = []
    _stub_fusion(monkeypatch, calls)
    result = fuse_orderbook_day({"cryptohft": context, "tardis": tardis}, original, "2025-08-01")
    assert set(calls[0]) == {"cryptohft", "tardis"}
    assert {item["source_id"] for item in result["included_sources"]} == {"cryptohft", "tardis"}
    assert context.exists() and tardis.exists()


def test_book_fusion_failure_keeps_existing_and_all_originals(tmp_path, monkeypatch):
    output, incoming = tmp_path / "canonical.parquet", tmp_path / "crypto.parquet"
    _fusion_input(output)
    _fusion_input(incoming, native=True)
    original = output.read_bytes()
    _stub_fusion(monkeypatch, [], fail=True)
    with pytest.raises(ValueError, match="injected"):
        fuse_orderbook_day({"cryptohft": incoming}, output, "2025-08-01", retire_sources=True)
    assert output.read_bytes() == original and incoming.exists()
    assert not list(tmp_path.glob("*.partial"))


def test_book_fusion_reuse_does_not_repeat_events_and_can_retire_stage(tmp_path, monkeypatch):
    output, incoming = tmp_path / "canonical.parquet", tmp_path / "crypto.parquet"
    _fusion_input(incoming, native=True)
    calls = []
    _stub_fusion(monkeypatch, calls)
    first = fuse_orderbook_day({"cryptohft": incoming}, output, "2025-08-01")
    repeated = fuse_orderbook_day({"cryptohft": incoming}, output, "2025-08-01", retire_sources=True)
    assert len(calls) == 1 and repeated["status"] == "REUSED_VERIFIED"
    assert repeated["sha256"] == first["sha256"] and not incoming.exists()
    assert output.exists()


def test_book_fusion_rejects_duplicate_path_before_mutation(tmp_path):
    source = tmp_path / "input.parquet"
    _fusion_input(source)
    with pytest.raises(ValueError, match="more than once"):
        fuse_orderbook_day({"tardis": source, "cryptohft": source}, tmp_path / "out.parquet", "2025-08-01")


def test_convert_preserves_fused_schema_and_metadata(tmp_path, monkeypatch):
    source, fused, copied = (tmp_path / name for name in ("input.parquet", "fused.parquet", "copy.parquet"))
    _fusion_input(source)
    _stub_fusion(monkeypatch, [])
    fuse_orderbook_day({"tardis": source}, fused, "2025-08-01")
    convert_day_channel(fused, copied, "2025-08-01", "incremental_book_L2")
    assert fused.read_bytes() == copied.read_bytes()
    assert convert_day_channel(copied, copied, "2025-08-01", "incremental_book_L2")["status"] == "VERIFIED_EXISTING"


def test_fusion_inherits_adjacent_published_state_and_passes_derived_destination(tmp_path, monkeypatch):
    first = tmp_path / "2025-08-01/book.parquet"
    second = tmp_path / "2025-08-02/book.parquet"
    incoming = tmp_path / "input.parquet"
    _fusion_input(incoming)
    options = []
    _stub_fusion(monkeypatch, [], options=options)
    fuse_orderbook_day({"tardis": incoming}, first, "2025-08-01")
    result = fuse_orderbook_day({"tardis": incoming}, second, "2025-08-02",
                               normalized_root=tmp_path / "derived")
    assert options[1]["previous_state"] == {"test_day": "2025-08-01"}
    assert options[1]["normalized_root"] == tmp_path / "derived"
    assert len(result["previous_day_receipt_sha256"]) == 64


def test_fusion_reuse_rejects_wrong_day_before_retirement(tmp_path, monkeypatch):
    incoming, output = tmp_path / "input.parquet", tmp_path / "out.parquet"
    _fusion_input(incoming)
    _stub_fusion(monkeypatch, [])
    fuse_orderbook_day({"tardis": incoming}, output, "2025-08-01")
    original = output.read_bytes()
    with pytest.raises(ValueError, match="mismatched scope"):
        fuse_orderbook_day({"tardis": incoming}, output, "2025-08-02", retire_sources=True)
    assert output.read_bytes() == original and incoming.exists()


def test_fusion_refresh_keeps_verified_boundary_after_original_retirement(tmp_path, monkeypatch):
    initial, next_day, incoming, output = [tmp_path / name for name in
                                          ("initial.parquet", "next.parquet", "new.parquet", "out.parquet")]
    _fusion_input(initial)
    _fusion_input(next_day)
    _fusion_input(incoming, native=True)
    _stub_fusion(monkeypatch, [])
    first = fuse_orderbook_day({"tardis": initial}, output, "2025-08-01",
                               next_sources={"tardis": next_day})
    next_day.unlink()  # The next-day original was incorporated and retired.
    refreshed = fuse_orderbook_day({"cryptohft": incoming}, output, "2025-08-01")
    assert refreshed["boundary_input_files"] == first["boundary_input_files"]
    assert len(refreshed["included_sources"]) == 2


def _csv(path: Path, header: list[str], rows: list[list[object]], compressed=False):
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    value = buffer.getvalue().encode()
    path.write_bytes(zstandard.ZstdCompressor().compress(value) if compressed else value)


def test_logical_digest_mixed_null_values_survives_parquet_roundtrip(tmp_path):
    # Null slots may contain unused physical payloads before serialization;
    # Parquet is free to replace those payloads without changing any values.
    table = pa.table({
        "local_timestamp": pa.array(
            [1754006400000123, 987, 0, 1754006400000456],
            mask=[False, True, False, False], type=pa.int64(),
        ),
        "quote_qty": pa.array(["1.000", None, "", "2.020"], type=pa.string()),
        "is_buyer_maker": pa.array(
            [True, True, False, False], mask=[False, True, False, False], type=pa.bool_(),
        ),
    })
    output = tmp_path / "mixed-null.parquet"
    pq.write_table(table, output, row_group_size=2)
    restored = pq.read_table(output)

    assert restored.equals(table)
    assert _logical_digest(restored) == _logical_digest(table)
    chunked = pa.concat_tables([table.slice(0, 1), table.slice(1, 2), table.slice(3)])
    assert _logical_digest(chunked) == _logical_digest(table)


@pytest.mark.parametrize("kind, replacement", [(pa.int64(), 0), (pa.string(), ""), (pa.bool_(), False)])
def test_logical_digest_does_not_confuse_null_with_fill_value(kind, replacement):
    with_null = pa.table({"value": pa.array([None, replacement], type=kind)})
    filled = pa.table({"value": pa.array([replacement, replacement], type=kind)})
    assert _logical_digest(with_null) != _logical_digest(filled)


@pytest.mark.parametrize("as_record_batch", [False, True])
def test_verified_writer_splits_large_producer_batches_without_changing_rows(tmp_path, monkeypatch, as_record_batch):
    import data.daily_raw as module

    count = 65_536 + 3
    table = pa.table({
        "id": pa.array(range(count), type=pa.int64()),
        "price": pa.array((["100.00", None, "100.0", "0.000"] * ((count + 3) // 4))[:count]),
        "maker": pa.array(([True, None, False] * ((count + 2) // 3))[:count]),
    })
    batches = [table.slice(0, 0), table, table.slice(0, 2)]
    if as_record_batch:
        batches = [pa.RecordBatch.from_arrays([column.combine_chunks() for column in batch.columns], schema=table.schema)
                   for batch in batches]
    real_digest = module._logical_digest
    digest_rows = []

    def observed_digest(chunk):
        digest_rows.append(chunk.num_rows)
        return real_digest(chunk)

    monkeypatch.setattr(module, "_logical_digest", observed_digest)
    output = tmp_path / "bounded.parquet"
    rows, digest = module._write_verified_tables(iter(batches), output, table.schema, {b"source": b"synthetic"},
        final_metadata=lambda rows: {b"completed_rows": str(rows).encode()})
    parquet = pq.ParquetFile(output)
    assert rows == count + 2 == parquet.metadata.num_rows
    assert [parquet.metadata.row_group(index).num_rows for index in range(parquet.num_row_groups)] == [65_536, 3, 2]
    assert digest_rows == [65_536, 3, 2, 65_536, 3, 2]
    assert parquet.metadata.metadata[b"completed_rows"] == str(rows).encode()
    assert parquet.metadata.metadata[b"source"] == b"synthetic"
    assert pq.read_table(output).replace_schema_metadata(None).equals(pa.concat_tables([table, table.slice(0, 2)]))
    assert digest == module.sha256_file(output)


@pytest.mark.parametrize("fault, message", [
    ("drop_row", "canonical row count mismatch"),
    ("split_group", "canonical row group count mismatch"),
    ("change_value", "canonical logical roundtrip mismatch"),
])
def test_verified_writer_still_rejects_row_group_corruption(tmp_path, monkeypatch, fault, message):
    import data.daily_raw as module

    table = pa.table({"id": pa.array(range(11), type=pa.int64())})
    real_writer = module.pq.ParquetWriter

    class FaultWriter:
        def __init__(self, *args, **kwargs):
            self.writer = real_writer(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.writer.close()

        def write_table(self, chunk, *, row_group_size):
            if fault == "drop_row":
                chunk = chunk.slice(0, chunk.num_rows - 1)
            elif fault == "change_value":
                chunk = pa.table({"id": pa.array([999] + list(range(1, chunk.num_rows)), type=pa.int64())})
            self.writer.write_table(chunk, row_group_size=3 if fault == "split_group" else row_group_size)

    monkeypatch.setattr(module.pq, "ParquetWriter", FaultWriter)
    with pytest.raises(ValueError, match=message):
        module._write_verified_tables([table], tmp_path / "rejected.parquet", table.schema, {})


@pytest.mark.parametrize("suffix", [".zst", ".zstd", ".xz"])
def test_tardis_book_preserves_clock_strings_order_and_missing_native_fields(tmp_path, suffix):
    source = tmp_path / f"book.csv{suffix}"
    timestamp = 1754006400000000
    _csv(source, ["exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"], [
        ["binance-futures", "BTCUSDC", timestamp, timestamp + 987, True, "bid", "100.00", "1.010"],
        ["binance-futures", "BTCUSDC", timestamp, timestamp + 987, True, "ask", "101.00", "2.020"],
        ["binance-futures", "BTCUSDC", timestamp, timestamp + 987, False, "bid", "100.00", "0.000"],
    ])
    raw = source.read_bytes()
    source.write_bytes(lzma.compress(raw) if suffix == ".xz" else zstandard.ZstdCompressor().compress(raw))
    output = tmp_path / "book.parquet"
    result = convert_day_channel(source, output, "2025-08-01", "incremental_book_L2")
    table = pq.read_table(output)
    assert result["rows"] == 3
    assert table.column("timestamp").to_pylist() == [timestamp] * 3
    assert table.column("local_timestamp").to_pylist() == [timestamp + 987] * 3
    assert table.column("price").to_pylist() == ["100.00", "101.00", "100.00"]
    assert table.column("amount").to_pylist() == ["1.010", "2.020", "0.000"]
    assert table.column("source_row").to_pylist() == [0, 1, 2]
    for name in ("received_time", "event_time", "transaction_time", "final_update_id", "source_hour"):
        assert table.column(name).null_count == 3
    assert table.column("event_type").to_pylist() == ["snapshot", "snapshot", "update"]


@pytest.mark.parametrize("suffix", [".zst", ".zstd", ".xz"])
def test_corrupt_compressed_book_does_not_publish_output(tmp_path, suffix):
    source = tmp_path / f"book.csv{suffix}"
    source.write_bytes(b"not a compressed archive")
    output = tmp_path / "book.parquet"
    with pytest.raises((pa.ArrowInvalid, lzma.LZMAError, OSError)):
        convert_day_channel(source, output, "2025-08-01", "incremental_book_L2")
    assert not output.exists()
    assert not list(tmp_path.glob("*.partial"))


def test_trade_conversion_keeps_distinct_ids_at_same_timestamp(tmp_path):
    source = tmp_path / "trades.csv"
    _csv(source, ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"], [
        [21, "100.000", "0.010", "1.00000", 1754006400123, True],
        [22, "100.000", "0.010", "1.00000", 1754006400123, False],
    ])
    output = tmp_path / "trades.parquet"
    result = convert_day_channel(source, output, "2025-08-01", "trades")
    values = pq.read_table(output).to_pydict()
    assert result["rows"] == 2
    assert values["id"] == [21, 22]
    assert values["timestamp"] == [1754006400123000] * 2
    assert values["local_timestamp"] == [None, None]
    assert values["side"] == ["sell", "buy"]
    assert values["amount"] == ["0.010", "0.010"]
    assert values["quote_qty"] == ["1.00000", "1.00000"]


def test_aggregate_conversion_keeps_parent_trade_identity(tmp_path):
    source = tmp_path / "agg.csv"
    _csv(source, ["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker"], [
        [9, "101.20", "4.200", 21, 27, 1754006400123, True],
    ])
    output = tmp_path / "agg.parquet"
    convert_day_channel(source, output, "2025-08-01", "aggTrades")
    row = pq.read_table(output).to_pylist()[0]
    assert (row["agg_trade_id"], row["first_trade_id"], row["last_trade_id"]) == (9, 21, 27)
    assert row["quantity"] == "4.200"
    assert row["timestamp"] == 1754006400123000


def test_funding_split_uses_exchange_day_without_fabricating_mark(tmp_path):
    source = tmp_path / "funding.json"
    source.write_text(json.dumps([
        {"symbol": "BTCUSDC", "fundingTime": 1754006399999, "fundingRate": "0.01"},
        {"symbol": "BTCUSDC", "fundingTime": 1754006400000, "fundingRate": "-0.0000200"},
        {"symbol": "BTCUSDC", "fundingTime": 1754092800000, "fundingRate": "0.02"},
    ]))
    output = tmp_path / "funding.parquet"
    result = convert_day_channel(source, output, "2025-08-01", "funding")
    assert result["rows"] == 1
    row = pq.read_table(output).to_pylist()[0]
    assert row["fundingRate"] == "-0.0000200"
    assert row["markPrice"] is None
    assert row["timestamp"] == 1754006400000000


def test_native_book_reuses_bytes_and_preserves_update_ids(tmp_path):
    source = tmp_path / "native.parquet"
    table = pa.Table.from_pylist([{
        "symbol": "BTCUSDC", "exchange": "binance-futures", "timestamp": 1754006400000000,
        "local_timestamp": 1754006400000123, "price": "100.0", "amount": "2.000",
        "quantity": "2.000", "final_update_id": 42, "source_row": 0,
    }], schema=BOOK_SCHEMA)
    pq.write_table(table, source, compression="zstd")
    output = tmp_path / "canonical.parquet"
    result = convert_day_channel(source, output, "2025-08-01", "incremental_book_L2")
    assert result["mode"] == "verified_hardlink"
    assert result["source_sha256"] == result["sha256"]
    assert source.stat().st_ino == output.stat().st_ino
    assert pq.read_table(output).column("final_update_id").to_pylist() == [42]


def test_empty_canonical_container_is_not_a_verified_existing_day(tmp_path):
    path = tmp_path / "incremental_book_L2.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=BOOK_SCHEMA), path)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="empty raw source"):
        convert_day_channel(path, path, "2025-08-01", "incremental_book_L2")
    assert path.read_bytes() == before


def test_failure_does_not_replace_previous_success_or_leave_partial(tmp_path, monkeypatch):
    source = tmp_path / "trades.csv"
    _csv(source, ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"], [
        [21, "100", "1", "100", 1754006400000, True],
    ])
    output = tmp_path / "canonical.parquet"
    output.write_bytes(b"previous successful output")
    import data.daily_raw as module
    real = module._logical_digest
    calls = []

    def wrong_on_verify(table):
        calls.append(1)
        return real(table) if len(calls) == 1 else "mismatch"

    monkeypatch.setattr(module, "_logical_digest", wrong_on_verify)
    with pytest.raises(ValueError, match="roundtrip mismatch"):
        convert_day_channel(source, output, "2025-08-01", "trades")
    assert output.read_bytes() == b"previous successful output"
    assert list(tmp_path.glob("*.partial")) == []
    assert source.exists()


def test_missing_day_funding_is_not_emitted_as_empty_zero(tmp_path):
    source = tmp_path / "funding.json"
    source.write_text("[]")
    output = tmp_path / "funding.parquet"
    with pytest.raises(ValueError, match="empty raw source"):
        convert_day_channel(source, output, "2025-08-01", "funding")
    assert not output.exists()


def test_wrong_book_market_is_rejected(tmp_path):
    source = tmp_path / "book.csv"
    _csv(source, ["exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"], [
        ["binance", "BTCUSDC", 1754006400000000, 1754006400000001, True, "bid", "100", "1"],
    ])
    with pytest.raises(ValueError, match="exchange mismatch"):
        convert_day_channel(source, tmp_path / "book.parquet", "2025-08-01", "incremental_book_L2")


def _manifest_fixture(tmp_path):
    sources = {}
    for day in ("2025-08-01", "2025-08-02"):
        folder = tmp_path / day
        folder.mkdir()
        book = folder / "book.csv"
        _csv(book, ["exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"], [
            ["binance-futures", "BTCUSDC", 1754006400000000, 1754006400000001, True, "bid", "100", "1"],
        ])
        trades = folder / "trades.csv"
        _csv(trades, ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"], [
            [21, "100", "1", "100", 1754006400000, True],
        ])
        agg = folder / "agg.csv"
        _csv(agg, ["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker"], [
            [9, "100", "1", 21, 21, 1754006400000, True],
        ])
        sources[day] = {"incremental_book_L2": book, "trades": trades, "aggTrades": agg}
    funding = tmp_path / "funding.json"
    funding.write_text(json.dumps([
        {"symbol": "BTCUSDC", "fundingTime": 1754006400000, "fundingRate": "0.01"},
        {"symbol": "BTCUSDC", "fundingTime": 1754092800000, "fundingRate": "0.02"},
    ]))
    for value in sources.values():
        value["funding"] = funding
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"records": [
        {"calendar_date": day, "channels": [
            {"source_id": source_id, "files": [{"path": str(value[channel])}]}
            for channel, source_id in CHANNEL_SOURCES.items()
        ]} for day, value in sources.items()
    ]}))
    return manifest, sources


def test_manifest_separates_accounting_and_shared_funding_not_retired_early(tmp_path):
    manifest, sources = _manifest_fixture(tmp_path)
    root = tmp_path / "canonical"
    result = migrate_manifest(manifest, root, days=["2025-08-01"], workers=1, retire_source=True)
    assert result["calendar_days"] == 1
    assert result["channels_per_day"] == 4
    assert len(result["records"]) == 4
    expected = root / "raw/binance_futures/BTCUSDC/2025-08-01"
    assert {p.name for p in expected.iterdir()} == {
        f"{channel}.parquet" for channel in CHANNEL_SOURCES if channel != "funding"
    }
    funding_path = root / "raw/accounting/funding/BTCUSDC/2025-08-01.parquet"
    assert funding_path.is_file()
    assert pq.read_table(funding_path).num_rows == 1
    assert sources["2025-08-01"]["funding"].exists()
    assert not sources["2025-08-01"]["trades"].exists()
    assert sources["2025-08-02"]["trades"].exists()


@pytest.mark.parametrize("legacy_day", [None, "2025-08-02"])
def test_manifest_imports_three_current_channels_without_native_aggregates(tmp_path, legacy_day):
    manifest, sources = _manifest_fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    for record in payload["records"]:
        if record["calendar_date"] != legacy_day:
            record["channels"] = [c for c in record["channels"]
                                  if c["source_id"] != CHANNEL_SOURCES["aggTrades"]]
            sources[record["calendar_date"]]["aggTrades"].unlink()
    manifest.write_text(json.dumps(payload))
    root = tmp_path / "canonical"
    result = migrate_manifest(manifest, root, workers=1)
    assert result["calendar_days"] == 2
    assert result["channels_per_day"] == (None if legacy_day else 3)
    assert result["channel_counts_by_day"] == {
        "2025-08-01": 3, "2025-08-02": 4 if legacy_day else 3,
    }
    assert len(result["records"]) == (7 if legacy_day else 6)
    assert not (root / "raw/binance_futures/BTCUSDC/2025-08-01/aggTrades.parquet").exists()


@pytest.mark.parametrize("channel", ["incremental_book_L2", "trades", "funding"])
def test_manifest_still_requires_current_raw_channels(tmp_path, channel):
    manifest, _ = _manifest_fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    payload["records"][1]["channels"] = [c for c in payload["records"][1]["channels"]
                                        if c["source_id"] != CHANNEL_SOURCES[channel]]
    manifest.write_text(json.dumps(payload))
    root = tmp_path / "canonical"
    with pytest.raises(ValueError, match="required source is absent"):
        migrate_manifest(manifest, root)
    assert not root.exists()


def test_manifest_registered_native_aggregate_missing_file_still_fails(tmp_path):
    manifest, sources = _manifest_fixture(tmp_path)
    sources["2025-08-02"]["aggTrades"].unlink()
    root = tmp_path / "canonical"
    with pytest.raises(FileNotFoundError, match="aggTrades: selected source is unavailable"):
        migrate_manifest(manifest, root)
    assert not root.exists()


def test_manifest_preflights_all_sources_before_any_output(tmp_path):
    manifest, sources = _manifest_fixture(tmp_path)
    sources["2025-08-02"]["trades"].unlink()
    root = tmp_path / "canonical"
    with pytest.raises(FileNotFoundError, match="selected source is unavailable"):
        migrate_manifest(manifest, root)
    assert not root.exists()


def test_manifest_rejects_duplicate_selected_source(tmp_path):
    manifest, _ = _manifest_fixture(tmp_path)
    value = json.loads(manifest.read_text())
    files = value["records"][0]["channels"][0]["files"]
    files.append(dict(files[0]))
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="exactly one real selected source"):
        migrate_manifest(manifest, tmp_path / "canonical")


def test_auxiliary_bookticker_keeps_header_order_decimal_text_and_clock(tmp_path):
    source = tmp_path / "ticker.csv.zst"
    names = ["exchange", "symbol", "timestamp", "local_timestamp", "ask_amount", "ask_price", "bid_price", "bid_amount"]
    _csv(source, names, [["binance-futures", "BTCUSDC", 1754006400000000, 1754006400000234,
                         "1.00000000000000001", "101.000", "100.000", "2.000"]], compressed=True)
    output = tmp_path / "ticker.parquet"
    result = convert_auxiliary_csv(source, output, "2025-08-01", "book_ticker", "binance_futures", "BTCUSDC")
    table = pq.read_table(output)
    assert result["rows"] == 1
    assert table.column_names == names
    assert table["timestamp"].to_pylist() == [1754006400000000]
    assert table["local_timestamp"].to_pylist() == [1754006400000234]
    assert table["ask_amount"].to_pylist() == ["1.00000000000000001"]
    assert "event_time" not in table.column_names


def test_auxiliary_headerless_spot_keeps_first_row_and_original_microseconds(tmp_path):
    source = tmp_path / "spot.csv"
    source.write_text("171818642,115757.86000000,0.11846000,218206275,218206278,1754006400781689,True,True\n"
                      "171818643,115751.66000000,0.03443000,218206279,218206279,1754006400781689,False,True\n")
    output = tmp_path / "spot.parquet"
    result = convert_auxiliary_csv(source, output, "2025-08-01", "aggTrades", "binance_spot", "BTCUSDC")
    table = pq.read_table(output)
    assert result["rows"] == 2
    assert table["agg_trade_id"].to_pylist() == ["171818642", "171818643"]
    assert table["transact_time"].to_pylist() == [1754006400781689] * 2
    assert table["quantity"].to_pylist() == ["0.11846000", "0.03443000"]
    assert table["is_buyer_maker"].to_pylist() == ["True", "False"]
    assert "local_timestamp" not in table.column_names
    assert "timestamp" not in table.column_names


def test_auxiliary_metrics_keeps_timestamp_string_and_unknown_empty_text(tmp_path):
    source = tmp_path / "metrics.csv"
    source.write_text("create_time,symbol,sum_open_interest,custom_source_note\n"
                      "2025-08-01 00:05:00,BTCUSDC,7275.7130000000000000,\n")
    output = tmp_path / "metrics.parquet"
    convert_auxiliary_csv(source, output, "2025-08-01", "metrics", "binance_futures", "BTCUSDC")
    row = pq.read_table(output).to_pylist()[0]
    assert row == {"create_time": "2025-08-01 00:05:00", "symbol": "BTCUSDC",
                   "sum_open_interest": "7275.7130000000000000", "custom_source_note": ""}


def test_auxiliary_failure_keeps_previous_output(tmp_path):
    source = tmp_path / "metrics.csv"
    source.write_text("create_time,symbol,sum_open_interest\n2025-08-01 00:05:00,BTCUSDT,2.000\n")
    output = tmp_path / "metrics.parquet"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match="symbol mismatch"):
        convert_auxiliary_csv(source, output, "2025-08-01", "metrics", "binance_futures", "BTCUSDC")
    assert output.read_bytes() == b"previous"
    assert not list(tmp_path.glob("*.partial"))


@pytest.mark.parametrize("channel,symbol", [("trades", "BTCUSDC"), ("aggTrades", "BTCUSDT")])
def test_vision_defaults_convert_retire_and_repeat_without_network(tmp_path, monkeypatch, channel, symbol):
    from data.downloaders import binance_vision as vision
    from data_paths import daily_market_path
    from market_fusion import PERP_MARKET

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    jobs = vision.build_jobs(symbols=[symbol], days=["2025-08-01"],
                             dataset=channel, market_type=PERP_MARKET, output_dir=None)
    job = jobs[0]
    assert job.canonical_output == daily_market_path(job.day, job.symbol, channel)
    assert job.target_dir == tmp_path / "raw" / ".incoming" / channel / symbol
    fetched = []

    def fetch(url, target, **_):
        fetched.append(url)
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        if channel == "trades":
            writer.writerow(["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"])
            writer.writerows([[21, "100.00", "1.000", "100.00000", 1754006400123, True],
                              [22, "100.00", "1.000", "100.00000", 1754006400123, False]])
        else:
            writer.writerow(["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker"])
            writer.writerow([9, "100.00", "2.000", 21, 22, 1754006400123, True])
        with zipfile.ZipFile(target, "w") as archive:
            archive.writestr(job.filename.replace(".zip", ".csv"), buffer.getvalue())
        return "OK"

    monkeypatch.setattr(vision, "fetch_zip", fetch)
    monkeypatch.setattr(vision, "maybe_download_checksum", lambda *_: None)
    options = dict(keep_zip=False, overwrite=False, verbose=False)
    assert vision.download_one(job, **options).startswith("[OK]")
    assert len(fetched) == 1
    values = pq.read_table(job.canonical_output).to_pydict()
    assert values["timestamp"] == [1754006400123000] * (2 if channel == "trades" else 1)
    assert values["local_timestamp"] == [None] * len(values["timestamp"])
    if channel == "trades":
        assert values["id"] == [21, 22]  # Equal timestamps do not collapse matching events.
    else:
        assert (values["agg_trade_id"], values["first_trade_id"], values["last_trade_id"]) == ([9], [21], [22])
    assert list(job.target_dir.iterdir()) == []
    assert not job.canonical_output.is_symlink()
    before = job.canonical_output.read_bytes()
    assert vision.download_one(job, **options).startswith("[SKIP]")
    assert len(fetched) == 1 and job.canonical_output.read_bytes() == before


@pytest.mark.parametrize("explicit_output", [False, True])
@pytest.mark.parametrize("symbols", [["BTCUSDC"], ["BTCUSDT", "btcusdc"]])
def test_vision_cannot_restore_retired_execution_aggregates(tmp_path, monkeypatch, explicit_output, symbols):
    from data.downloaders import binance_vision as vision
    from market_fusion import PERP_MARKET

    monkeypatch.setattr(vision, "data_root", lambda *_: pytest.fail("must reject before resolving output"))
    with pytest.raises(ValueError, match="native aggTrades acquisition is retired"):
        vision.build_jobs(symbols=symbols, days=["2025-08-01"], dataset="aggTrades",
                          market_type=PERP_MARKET, output_dir=tmp_path if explicit_output else None)


def test_vision_manual_job_cannot_bypass_native_retirement(tmp_path, monkeypatch):
    from data.downloaders import binance_vision as vision

    output = tmp_path / "downloads"
    job = vision.DownloadJob("BTCUSDC", "2025-08-01", "aggTrades",
                             "https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDC/example.zip",
                             "example.zip", output)
    monkeypatch.setattr(vision, "fetch_zip", lambda *_a, **_k: pytest.fail("must not download"))
    with pytest.raises(ValueError, match="native aggTrades acquisition is retired"):
        vision.download_one(job, keep_zip=False, overwrite=False, verbose=False)
    assert not output.exists()


def test_vision_missing_source_does_not_publish_empty_canonical(tmp_path, monkeypatch):
    from data.downloaders import binance_vision as vision
    from market_fusion import PERP_MARKET

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    job = vision.build_jobs(symbols=["BTCUSDC"], days=["2025-08-01"],
                             dataset="trades", market_type=PERP_MARKET, output_dir=None)[0]
    monkeypatch.setattr(vision, "fetch_zip", lambda *_args, **_kwargs: "404")
    monkeypatch.setattr(vision, "maybe_download_checksum", lambda *_: pytest.fail("404 has no checksum"))
    assert vision.download_one(job, keep_zip=False, overwrite=False, verbose=False).startswith("[404]")
    assert not job.canonical_output.exists()
    assert list(job.target_dir.iterdir()) == []


def test_vision_explicit_legacy_output_is_not_silently_relocated(tmp_path, monkeypatch):
    from data.downloaders import binance_vision as vision
    from market_fusion import PERP_MARKET, SPOT_MARKET

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    explicit = vision.build_jobs(symbols=["BTCUSDC"], days=["2025-08-01"],
        dataset="trades", market_type=PERP_MARKET, output_dir=tmp_path / "explicit")[0]
    assert explicit.canonical_output is None and explicit.target_dir == tmp_path / "explicit" / "BTCUSDC"
    spot = vision.build_jobs(symbols=["BTCUSDC"], days=["2025-08-01"],
        dataset="aggTrades", market_type=SPOT_MARKET, output_dir=tmp_path / "spot")[0]
    assert spot.canonical_output is None


@pytest.mark.parametrize("channel,market,path_suffix,content", [
    ("aggTrades", "spot", "binance_spot/BTCUSDT/2025-08-01/aggTrades.parquet",
     "9,100.000,2.000,21,22,1754006400123456,true,true\n"),
    ("metrics", "perp", "history/binance_futures/BTCUSDT/2025-08-01/metrics.parquet",
     "create_time,symbol,sum_open_interest\n2025-08-01 00:05:00,BTCUSDT,123.000\n"),
])
def test_vision_auxiliary_defaults_preserve_original_clock_and_retire_csv(
    tmp_path, monkeypatch, channel, market, path_suffix, content,
):
    from data.downloaders import binance_vision as vision

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    job = vision.build_jobs(symbols=["BTCUSDT"], days=["2025-08-01"],
        dataset=channel, market_type=market, output_dir=None)[0]
    assert job.canonical_output == tmp_path / "raw" / path_suffix
    count = []

    def fetch(_url, target, **_):
        count.append(1)
        with zipfile.ZipFile(target, "w") as archive:
            archive.writestr(job.filename.replace(".zip", ".csv"), content)
        return "OK"

    monkeypatch.setattr(vision, "fetch_zip", fetch)
    monkeypatch.setattr(vision, "maybe_download_checksum", lambda *_: None)
    options = dict(keep_zip=False, overwrite=False, verbose=False)
    assert vision.download_one(job, **options).startswith("[OK]")
    row = pq.read_table(job.canonical_output).to_pylist()[0]
    if market == "spot":
        assert row["transact_time"] == 1754006400123456
        assert row["quantity"] == "2.000" and row["agg_trade_id"] == "9"
        assert "timestamp" not in row
    else:
        assert row["create_time"] == "2025-08-01 00:05:00"
        assert row["sum_open_interest"] == "123.000"
    assert list(job.target_dir.iterdir()) == []
    assert vision.download_one(job, **options).startswith("[SKIP]")
    assert count == [1]


def _native_book_fixture(path):
    pq.write_table(pa.Table.from_pylist([{
        "symbol": "BTCUSDC", "exchange": "binance-futures", "timestamp": 1754006400000000,
        "price": "100.0", "amount": "2.000", "quantity": "2.000", "final_update_id": 42,
    }], schema=BOOK_SCHEMA), path, compression="zstd")


def test_existing_canonical_output_symlink_cannot_enter_verified_existing_branch(tmp_path):
    source = tmp_path / "source.parquet"
    _native_book_fixture(source)
    output = tmp_path / "canonical.parquet"
    output.symlink_to(source)
    before = source.read_bytes()
    with pytest.raises(ValueError, match="output path must not contain a symlink"):
        convert_day_channel(source, output, "2025-08-01", "incremental_book_L2")
    assert output.is_symlink()
    assert source.read_bytes() == before


@pytest.mark.parametrize("auxiliary", [False, True])
def test_canonical_output_parent_symlink_is_rejected(tmp_path, auxiliary):
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    output = alias / "new-day" / "channel.parquet"
    if auxiliary:
        source = tmp_path / "metrics.csv"
        source.write_text("create_time,symbol,sum_open_interest\n2025-08-01 00:05:00,BTCUSDC,1.0\n")
        invoke = lambda: convert_auxiliary_csv(source, output, "2025-08-01", "metrics", "binance_futures", "BTCUSDC")
    else:
        source = tmp_path / "source.parquet"
        _native_book_fixture(source)
        invoke = lambda: convert_day_channel(source, output, "2025-08-01", "incremental_book_L2")
    with pytest.raises(ValueError, match="output path must not contain a symlink"):
        invoke()
    assert not (real / "new-day").exists()


def test_broken_canonical_output_symlink_is_rejected(tmp_path):
    source = tmp_path / "source.parquet"
    _native_book_fixture(source)
    output = tmp_path / "canonical.parquet"
    output.symlink_to(tmp_path / "missing.parquet")
    with pytest.raises(ValueError, match="output path must not contain a symlink"):
        convert_day_channel(source, output, "2025-08-01", "incremental_book_L2")
    assert output.is_symlink()


def test_legacy_source_alias_and_real_canonical_file_remain_supported(tmp_path):
    original = tmp_path / "original.parquet"
    _native_book_fixture(original)
    source_alias = tmp_path / "legacy-source.parquet"
    source_alias.symlink_to(original)
    existing = convert_day_channel(source_alias, original, "2025-08-01", "incremental_book_L2")
    assert existing["status"] == "VERIFIED_EXISTING"
    destination = tmp_path / "physical" / "canonical.parquet"
    published = convert_day_channel(source_alias, destination, "2025-08-01", "incremental_book_L2")
    assert published["status"] == "PUBLISHED"
    assert destination.is_file() and not destination.is_symlink()
    assert destination.read_bytes() == original.read_bytes()


def test_root_owned_os_tmp_alias_is_not_a_dataset_symlink(tmp_path):
    import os
    import sys
    from data.daily_raw import _reject_output_symlinks

    tmp = Path("/tmp")
    if not (sys.platform == "darwin" and tmp.is_symlink() and tmp.lstat().st_uid == 0):
        pytest.skip("host has no macOS root-owned /tmp alias")
    _reject_output_symlinks(tmp / f"narrowgate-physical-test-{os.getpid()}" / "day.parquet")


def test_vision_default_download_uses_individuals_not_retired_native_aggregates(monkeypatch, capsys):
    from data.downloaders import binance_vision as vision

    monkeypatch.setattr("sys.argv", [
        "download_binance_vision", "--symbols", "BTCUSDC", "--day-start", "2025-08-01",
        "--day-end", "2025-08-01", "--dry-run",
    ])
    assert vision.main() == 0
    output = capsys.readouterr().out
    assert "dataset=trades" in output
    assert "/trades/" in output
    assert "aggTrades" not in output


def _daily_conversion_fixture(path, *, future=False):
    from data.daily_raw import FUSED_BOOK_SCHEMA
    start = 1754006400000000
    pq.write_table(pa.Table.from_pylist([{
        "exchange": "binance-futures", "symbol": "BTCUSDC", "timestamp": start + 1000,
        "local_timestamp": start + 2000, "is_snapshot": True,
        "side": "bid", "price": "100.000", "amount": "2.000",
        "source_id": "tardis", "fusion_reason": "carried_opening_snapshot",
        "source_observed_timestamp_us": start + 2000 if future else start - 5000,
    }], schema=FUSED_BOOK_SCHEMA).replace_schema_metadata({
        b"narrowgate.book_fusion": b"reconstructed_fusion.v1", b"narrowgate.day": b"2025-08-01",
        b"narrowgate.fusion_receipt": b'{"stats":{"normalized":{"bbo":{"rows":1,"sha256":"abc","path":"private-supplier-path"}}}}',
        b"narrowgate.included_sources": b'[{"source_id":"tardis","sha256":"secret-history"}]',
    }), path)


def test_daily_book_atomic_same_path_and_neutral_footer(tmp_path):
    from data.daily_raw import DAILY_BOOK_MARKER, DAILY_BOOK_SCHEMA, daily_book_receipt, sha256_file, unify_orderbook_day
    path = tmp_path / "book.parquet"
    _daily_conversion_fixture(path)
    normalized = {kind: {"rows": 1, "sha256": "a" * 64} for kind in ("bbo", "l2", "clock")}
    result = unify_orderbook_day(path, path, "2025-08-01", normalized=normalized)
    table = pq.read_table(path)
    assert table.schema.remove_metadata() == DAILY_BOOK_SCHEMA
    assert table["price"].to_pylist() == ["100.000"]
    assert table["observed_timestamp_us"].to_pylist() == [1754006400000000 - 5000]
    metadata = pq.ParquetFile(path).metadata.metadata
    assert b"narrowgate.fusion_receipt" not in metadata and b"narrowgate.included_sources" not in metadata
    assert metadata[b"narrowgate.book_fusion"].decode() == DAILY_BOOK_MARKER
    assert daily_book_receipt(path)["normalized"] == normalized
    assert result["sha256"] == sha256_file(path)
    assert convert_day_channel(path, path, "2025-08-01", "incremental_book_L2")["status"] == "VERIFIED_EXISTING"
    assert unify_orderbook_day(path, path, "2025-08-01")["rows"] == 1


@pytest.mark.parametrize("failure", ["future", "writer", "identity"])
def test_daily_book_failure_never_replaces_unique_input(tmp_path, monkeypatch, failure):
    from data import daily_raw
    path = tmp_path / "book.parquet"
    _daily_conversion_fixture(path, future=failure == "future")
    before = path.read_bytes()
    if failure == "writer":
        monkeypatch.setattr(daily_raw, "_write_verified_tables", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("write failed")))
    if failure == "identity":
        original = daily_raw._identity
        calls = []
        def changed(p):
            value = original(p)
            calls.append(1)
            return value if len(calls) == 1 else (*value[:3], value[3] + 1)
        monkeypatch.setattr(daily_raw, "_identity", changed)
    with pytest.raises((ValueError, RuntimeError)):
        daily_raw.unify_orderbook_day(path, path, "2025-08-01")
    assert path.read_bytes() == before
    assert not list(tmp_path.glob("*.partial"))


def test_daily_book_rejects_invalid_derived_identity(tmp_path):
    from data.daily_raw import unify_orderbook_day
    path = tmp_path / "book.parquet"
    _daily_conversion_fixture(path)
    with pytest.raises(ValueError, match="normalized identities"):
        unify_orderbook_day(path, path, "2025-08-01", normalized={"clock": {"rows": 0, "sha256": "0" * 64}})


def test_new_day_fusion_can_publish_only_unified_schema_and_preserve_input_on_failure(tmp_path, monkeypatch):
    from data import daily_raw
    from data.daily_raw import DAILY_BOOK_SCHEMA, daily_book_receipt

    source, output = tmp_path / "input.parquet", tmp_path / "daily.parquet"
    _fusion_input(source)
    _stub_fusion(monkeypatch, [])
    result = fuse_orderbook_day({"tardis": source}, output, "2025-08-01", unified_output=True)
    assert pq.ParquetFile(output).schema_arrow.remove_metadata() == DAILY_BOOK_SCHEMA
    assert daily_book_receipt(output)["top_rows"] == 0
    assert result["schema"] == daily_raw.DAILY_BOOK_MARKER
    before = output.read_bytes()
    with pytest.raises(ValueError, match="explicit anonymous stream"):
        fuse_orderbook_day({"tardis": source}, output, "2025-08-01", unified_output=True, retire_sources=True)
    assert source.exists() and output.read_bytes() == before
    output.unlink()
    monkeypatch.setattr(daily_raw, "unify_orderbook_day", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("projection failed")))
    with pytest.raises(ValueError, match="projection failed"):
        fuse_orderbook_day({"tardis": source}, output, "2025-08-01", unified_output=True, retire_sources=True)
    assert source.exists() and not output.exists()


def test_new_daily_fusion_retains_previous_anonymous_end_state(tmp_path):
    from data.daily_raw import daily_book_receipt
    start = 1754006400000000
    sources = [tmp_path / "a.parquet", tmp_path / "b.parquet"]
    outputs = [tmp_path / "raw" / day / "incremental_book_L2.parquet" for day in ("2025-08-01", "2025-08-02")]
    for index, source in enumerate(sources):
        stamp = start + (index + 1) * 86400000000 - 100000
        rows = [{"exchange": "binance-futures", "symbol": "BTCUSDC", "timestamp": stamp,
                 "local_timestamp": stamp + 1000, "is_snapshot": True, "side": side,
                 "price": str(price), "amount": "1.000", "quantity": "1.000"}
                for side, levels in (("bid", range(100, 80, -1)), ("ask", range(101, 121)))
                for price in levels]
        pq.write_table(pa.Table.from_pylist(rows, schema=BOOK_SCHEMA), source)
        fuse_orderbook_day({"tardis": source}, outputs[index], f"2025-08-0{index + 1}", unified_output=True)
    prior = daily_book_receipt(outputs[0])["final_state"]
    following = daily_book_receipt(outputs[1])["initial_state"]
    assert following == prior
    assert following["state"]["global_observed_us"] == start + 86400000000 - 100000


def test_daily_book_top_insertion_is_sorted_and_keeps_exact_deep_rows(tmp_path):
    from data.daily_raw import daily_book_receipt, unify_orderbook_day
    from data.book_top import TOP_SCHEMA, to_daily_book_rows, from_daily_book_rows

    source, output = tmp_path / "before.parquet", tmp_path / "after.parquet"
    _daily_conversion_fixture(source)
    top = pa.Table.from_pylist([{
        "exchange": "binance-futures", "symbol": "BTCUSDC", "timestamp": 1754006400000000,
        "local_timestamp": 1754006400000001, "bid_price": "99.000", "bid_amount": "2.000",
        "ask_price": "101.000", "ask_amount": "3.000",
    }], schema=TOP_SCHEMA)
    rows = to_daily_book_rows(top)
    result = unify_orderbook_day(source, output, "2025-08-01", top_supplements=rows)
    table = pq.read_table(output)
    assert table["timestamp"].to_pylist() == [1754006400000000, 1754006400000000, 1754006400001000]
    assert from_daily_book_rows(table.filter(table["top_only"])).equals(top)
    assert table["price"][-1].as_py() == "100.000"
    assert result["rows"] == 3 and daily_book_receipt(output)["top_rows"] == 2
    assert source.exists()
