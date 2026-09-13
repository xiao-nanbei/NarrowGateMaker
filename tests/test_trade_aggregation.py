from __future__ import annotations

import csv
from decimal import Decimal
import io
import json
import lzma
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard as zstd

from data import trade_aggregation
from data.daily_raw import TRADE_SCHEMA
from data.trade_aggregation import build_aggregates, prepare_day, sha256_file, union_trades

DAY = "2026-01-01"
START = 1_767_225_600_000
SECONDARY_SCHEMA = pa.schema([
    ("trade_id", pa.int64()), ("price", pa.string()), ("quantity", pa.string()),
    ("trade_time", pa.int64()), ("is_buyer_maker", pa.bool_()), ("symbol", pa.string()),
    ("received_time", pa.int64()), ("event_time", pa.int64()), ("order_type", pa.string()),
])


def primary(*records):
    rows = []
    for record in records:
        row = {"id": 10, "price": "100.00", "qty": "0.001", "time": START + 50,
               "is_buyer_maker": False, "symbol": "BTCUSDC", "exchange": "binance-futures"}
        row.update(record)
        row.setdefault("timestamp", None if row["time"] is None else row["time"] * 1000)
        row.setdefault("local_timestamp", None)
        row.setdefault("amount", row["qty"])
        row.setdefault("side", None if row["is_buyer_maker"] is None else
                       "sell" if row["is_buyer_maker"] else "buy")
        row.setdefault("quote_qty", None)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=TRADE_SCHEMA)


def secondary(*records):
    rows = []
    for record in records:
        row = {"trade_id": 10, "price": "100.0", "quantity": "0.0010",
               "trade_time": START + 50, "is_buyer_maker": False, "symbol": "BTCUSDC",
               "received_time": (START + 53) * 1_000_000, "event_time": START + 52,
               "order_type": "MARKET"}
        row.update(record)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=SECONDARY_SCHEMA)


def quantity(table, column="qty"):
    return sum((Decimal(value) for value in table[column].to_pylist()), Decimal(0))


def test_empty_secondary_preserves_distinct_same_millisecond_native_ids():
    source = primary({"id": 10}, {"id": 11}, {"id": 12})
    before = source.to_pylist()
    union, stats = union_trades(source, secondary(), DAY)
    assert union.schema.names == TRADE_SCHEMA.names
    assert union["id"].to_pylist() == [10, 11, 12]
    assert quantity(union) == Decimal("0.003")
    assert stats["primary_rows"] == 3
    assert stats["secondary_rows"] == stats["added_rows"] == 0
    assert source.to_pylist() == before


def test_explicit_side_repair_preserves_all_other_fields_and_resume_identity(tmp_path):
    old = primary({"is_buyer_maker": True, "local_timestamp": START * 1000 + 123})
    source, repair = tmp_path / "old.parquet", tmp_path / "repair.parquet"
    pq.write_table(old, source)
    pq.write_table(primary({"is_buyer_maker": False}), repair)
    before = sha256_file(source)
    out = tmp_path / "prepared"
    result = prepare_day(source, [], DAY, out, primary_side_repair=repair)
    actual = pq.read_table(out / "trades.parquet")
    assert actual["side"].to_pylist() == ["buy"]
    assert actual["is_buyer_maker"].to_pylist() == [False]
    for name in set(old.column_names) - {"side", "is_buyer_maker"}:
        assert actual[name].equals(old[name])
    assert result["stats"]["primary_side_corrected_rows"] == 1
    assert result["canonical_replacement_required"] is True
    assert sha256_file(source) == before
    assert prepare_day(source, [], DAY, out, primary_side_repair=repair) == result
    with pytest.raises(ValueError, match="side repair identity"):
        prepare_day(source, [], DAY, out)


@pytest.mark.parametrize("change", [{"id": 11}, {"price": "101"},
                                    {"qty": "0.002"}, {"time": START + 51},
                                    {"side": "sell"}])
def test_side_repair_rejects_non_side_mutation_and_alias_conflict(tmp_path, change):
    source, repair = tmp_path / "old.parquet", tmp_path / "repair.parquet"
    pq.write_table(primary({"is_buyer_maker": True}), source)
    pq.write_table(primary(change), repair)
    with pytest.raises(ValueError):
        prepare_day(source, [], DAY, tmp_path / "prepared", primary_side_repair=repair)
    assert not (tmp_path / "prepared/receipt.json").exists()


def test_union_exact_overlap_keeps_vision_clock_and_records_one_ms_conflict():
    source = primary({})
    extra = secondary({"trade_time": START + 51})
    union, stats = union_trades(source, extra, DAY)
    assert union["id"].to_pylist() == [10]
    assert union["time"].to_pylist() == [START + 50]
    assert union["timestamp"].to_pylist() == [(START + 50) * 1000]
    assert stats["overlap_rows"] == 1
    assert stats["added_rows"] == 0
    assert stats["time_conflicts"] == [{"id": 10, "primary_time": START + 50,
                                        "secondary_time": START + 51}]


def test_union_adds_valid_secondary_once_with_exact_small_quantity():
    source = primary({"id": 10})
    extra = secondary({"trade_id": 12, "quantity": "0.00000001", "price": "100.00000001"},
                      {"trade_id": 12, "quantity": "0.00000001", "price": "100.00000001"})
    union, stats = union_trades(source, extra, DAY)
    assert union["id"].to_pylist() == [10, 12]
    assert quantity(union) == Decimal("0.00100001")
    assert Decimal(union["price"][1].as_py()) == Decimal("100.00000001")
    assert stats["added_rows"] == 1
    assert stats["exact_duplicates_removed"] == 1


def test_rejected_secondary_version_cannot_supply_a_valid_ids_output_values():
    extra = secondary({"trade_id": 12, "price": "0", "quantity": "0"},
                      {"trade_id": 12, "price": "100.25", "quantity": "0.003"})
    union, stats = union_trades(primary({}), extra, DAY)
    added = next(row for row in union.to_pylist() if row["id"] == 12)
    assert Decimal(added["price"]) == Decimal("100.25")
    assert Decimal(added["qty"]) == Decimal("0.003")
    assert stats["invalid_secondary_rows"] == 1
    assert stats["added_rows"] == 1


def test_union_filters_secondary_exchange_days_not_receive_days():
    extra = secondary({"trade_id": 8, "trade_time": START - 1},
                      {"trade_id": 12, "trade_time": START + 60,
                       "received_time": (START + 86_400_001) * 1_000_000},
                      {"trade_id": 14, "trade_time": START + 86_400_000})
    union, stats = union_trades(primary({}), extra, DAY)
    assert union["id"].to_pylist() == [10, 12]
    assert stats["outside_day_secondary_rows"] == 2


def test_secondary_only_ambiguous_clock_straddling_day_cannot_be_dropped_silently():
    extra = secondary({"trade_id": 12, "trade_time": START - 1},
                      {"trade_id": 12, "trade_time": START})
    with pytest.raises(ValueError):
        union_trades(primary({}), extra, DAY)


@pytest.mark.parametrize("timestamp", [START * 1000, None])
def test_primary_clock_alias_must_agree_with_native_time(timestamp):
    with pytest.raises(ValueError):
        union_trades(primary({"timestamp": timestamp}), secondary(), DAY)


@pytest.mark.parametrize("event_time", [START - 1, START + 86_400_000])
def test_misfiled_primary_exchange_day_is_not_silently_clipped(event_time):
    with pytest.raises(ValueError):
        union_trades(primary({"time": event_time}), secondary(), DAY)


@pytest.mark.parametrize("field,value", [("price", "100.01"), ("quantity", "0.002"),
                                         ("is_buyer_maker", True)])
def test_union_rejects_positive_core_conflict_with_primary(field, value):
    with pytest.raises(ValueError):
        union_trades(primary({}), secondary({field: value}), DAY)


@pytest.mark.parametrize("field,value", [("price", "100.01"), ("quantity", "0.002"),
                                         ("is_buyer_maker", True)])
def test_union_rejects_conflicting_secondary_versions_of_one_id(field, value):
    with pytest.raises(ValueError):
        union_trades(primary({}), secondary({"trade_id": 12}, {"trade_id": 12, field: value}), DAY)


def test_duplicate_primary_id_is_not_silently_repaired():
    with pytest.raises(ValueError):
        union_trades(primary({"id": 10}, {"id": 10}), secondary(), DAY)


@pytest.mark.parametrize("field,value", [("price", "0"), ("qty", "0"),
                                         ("price", "-1"), ("qty", "-0.1"),
                                         ("price", "NaN"), ("qty", "Infinity")])
def test_primary_nonpositive_or_nonfinite_value_raises(field, value):
    with pytest.raises(ValueError):
        union_trades(primary({field: value}), secondary(), DAY)


@pytest.mark.parametrize("field,value", [("price", "0"), ("quantity", "0"),
                                         ("price", "-1"), ("quantity", "-0.1")])
def test_secondary_nonpositive_record_is_counted_not_unioned(field, value):
    union, stats = union_trades(primary({}), secondary({"trade_id": 12, field: value}), DAY)
    assert union["id"].to_pylist() == [10]
    assert stats["invalid_secondary_rows"] == 1
    assert stats["added_rows"] == 0


@pytest.mark.parametrize("field", ["id", "time", "is_buyer_maker"])
def test_primary_missing_native_identity_clock_or_side_raises(field):
    with pytest.raises(ValueError):
        union_trades(primary({field: None}), secondary(), DAY)


@pytest.mark.parametrize("field", ["trade_id", "trade_time", "is_buyer_maker"])
def test_secondary_missing_native_identity_clock_or_side_raises(field):
    with pytest.raises(ValueError):
        union_trades(primary({}), secondary({field: None}), DAY)


@pytest.mark.parametrize("provider", ["primary", "secondary"])
def test_wrong_symbol_cannot_enter_union(provider):
    p = primary({"symbol": "BTCUSDT"}) if provider == "primary" else primary({})
    s = secondary({"symbol": "BTCUSDT"}) if provider == "secondary" else secondary()
    with pytest.raises(ValueError):
        union_trades(p, s, DAY)


def test_self_aggregate_is_not_a_native_parent_range_and_has_exact_conservation():
    # IDs 10 and 12 share price/side while 11 is another group. [10,12] must
    # never be interpreted as a native parent owning the interleaved ID 11.
    trades = primary({"id": 10, "time": START, "qty": "0.00000001"},
                     {"id": 11, "time": START + 1, "price": "101", "qty": "0.002"},
                     {"id": 12, "time": START + 2, "qty": "0.00000002"},
                     {"id": 13, "time": START + 3, "is_buyer_maker": True, "qty": "0.004"})
    result = build_aggregates(trades, DAY)
    assert len(result) == 3
    assert quantity(result, "quantity") == quantity(trades)
    assert sum(result["trade_count"].to_pylist()) == 4
    assert sum((Decimal(r["price"]) * Decimal(r["quantity"]) for r in result.to_pylist()), Decimal(0)) == sum(
        (Decimal(r["price"]) * Decimal(r["qty"]) for r in trades.to_pylist()), Decimal(0))
    assert not {"agg_trade_id", "first_trade_id", "last_trade_id", "transact_time", "timestamp"}.intersection(result.column_names)
    assert len(set(result["group_id"].to_pylist())) == 3
    buy100 = next(r for r in result.to_pylist()
                  if Decimal(r["price"]) == Decimal(100) and not r["is_buyer_maker"])
    assert buy100["first_event_id"] == 10 and buy100["last_event_id"] == 12
    assert buy100["trade_count"] == 2
    assert Decimal(buy100["quantity"]) == Decimal("0.00000003")
    assert result.schema.metadata[b"narrowgate.schema"] == b"trade_aggregates_100ms_v1"
    assert result.schema.metadata[b"native_aggtrade_identity"] == b"false"
    assert result.schema.metadata[b"execution_event_authority"] == b"false"


def test_fixed_utc_buckets_are_right_open_and_not_visible_before_completion():
    trades = primary({"id": 10, "time": START},
                     {"id": 11, "time": START + 99},
                     {"id": 12, "time": START + 100},
                     {"id": 13, "time": START + 199})
    result = build_aggregates(trades, DAY).to_pylist()
    assert [r["bucket_start_ms"] for r in result] == [START, START + 100]
    assert [r["bucket_end_ms"] for r in result] == [START + 100, START + 200]
    assert [r["trade_count"] for r in result] == [2, 2]
    for row in result:
        assert row["feature_ready_ts_ms"] == row["bucket_end_ms"]
        assert row["bucket_start_ms"] <= row["first_event_ts_ms"] <= row["last_event_ts_ms"]
        assert row["last_event_ts_ms"] < row["feature_ready_ts_ms"]


def test_midnight_bucket_identity_and_visibility_do_not_reset_into_past():
    end = START + 86_400_000
    left = build_aggregates(primary({"id": 10, "time": end - 1}), DAY).to_pylist()[0]
    right = build_aggregates(primary({"id": 11, "time": end}), "2026-01-02").to_pylist()[0]
    assert left["bucket_end_ms"] == end
    assert left["feature_ready_ts_ms"] == end
    assert right["bucket_start_ms"] == end
    assert right["feature_ready_ts_ms"] == end + 100
    assert left["group_id"] != right["group_id"]


def test_aggregate_chunk_layout_cannot_change_groups_or_output_identity():
    trades = primary({"id": 10, "time": START}, {"id": 11, "time": START + 99},
                     {"id": 12, "time": START + 100})
    chunked = pa.concat_tables([trades.slice(0, 1), trades.slice(1, 1), trades.slice(2)])
    assert build_aggregates(trades, DAY).equals(build_aggregates(chunked, DAY), check_metadata=True)


def write_parquet(path: Path, table: pa.Table) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def source_manifest(path: Path):
    return [{"path": str(path), "sha256": sha256_file(path), "rows": pq.ParquetFile(path).metadata.num_rows}]


def test_preparation_retains_midnight_shifted_id_only_on_primary_owner_day(tmp_path):
    midnight = START + 86_400_000
    p0 = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({"id": 10, "time": midnight - 1}))
    p1 = write_parquet(tmp_path / "raw/2026-01-02/trades.parquet", primary({"id": 11, "time": midnight + 1}))
    source = write_parquet(tmp_path / "source.parquet", secondary({"trade_id": 10, "trade_time": midnight}))
    for day, path in [(DAY, p0), ("2026-01-02", p1)]:
        prepare_day(path, source_manifest(source), day, tmp_path / "prepared" / day)
    left = pq.read_table(tmp_path / "prepared" / DAY / "trades.parquet")
    right = pq.read_table(tmp_path / "prepared/2026-01-02/trades.parquet")
    assert left["id"].to_pylist() == [10]
    assert left["time"].to_pylist() == [midnight - 1]
    assert right["id"].to_pylist() == [11]


def test_preparation_receipt_resume_is_idempotent_and_rejects_changed_output(tmp_path):
    source = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({}))
    staged = tmp_path / "prepared"
    first = prepare_day(source, [], DAY, staged)
    identities = {path.name: (sha256_file(path), path.stat().st_mtime_ns) for path in staged.iterdir()}
    assert prepare_day(source, [], DAY, staged) == first
    assert {path.name: (sha256_file(path), path.stat().st_mtime_ns) for path in staged.iterdir()} == identities
    (staged / "trades.parquet").write_bytes(b"corrupt staged bytes")
    with pytest.raises(ValueError):
        prepare_day(source, [], DAY, staged)
    assert source.exists()


def test_preparation_resume_revalidates_actual_secondary_bytes(tmp_path):
    p = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({}))
    source = write_parquet(tmp_path / "source.parquet", secondary({"trade_id": 12}))
    manifest = source_manifest(source)
    staged = tmp_path / "prepared"
    prepare_day(p, manifest, DAY, staged)
    write_parquet(source, secondary({"trade_id": 13}))
    with pytest.raises(ValueError):
        prepare_day(p, manifest, DAY, staged)


def test_preparation_resume_binds_adjacent_primary_ownership_context(tmp_path):
    p = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({"id": 10}))
    previous = write_parquet(tmp_path / "raw/2025-12-31/trades.parquet", primary({"id": 9, "time": START - 1}))
    source = write_parquet(tmp_path / "source.parquet", secondary({"trade_id": 12, "trade_time": START + 51}))
    manifest = source_manifest(source)
    staged = tmp_path / "prepared"
    prepare_day(p, manifest, DAY, staged)
    write_parquet(previous, primary({"id": 12, "time": START - 1}))
    with pytest.raises(ValueError):
        prepare_day(p, manifest, DAY, staged)


def test_partial_preparation_has_no_success_receipt_and_never_deletes_inputs(tmp_path, monkeypatch):
    source = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({}))
    original_sha = sha256_file(source)
    staged = tmp_path / "prepared"
    publish = trade_aggregation._publish_atomic

    def fail_aggregate(temporary, target):
        if target.name == "trade_aggregates_100ms.parquet":
            raise OSError("synthetic storage failure")
        return publish(temporary, target)

    monkeypatch.setattr(trade_aggregation, "_publish_atomic", fail_aggregate)
    with pytest.raises(OSError, match="synthetic storage failure"):
        prepare_day(source, [], DAY, staged)
    assert not (staged / "receipt.json").exists()
    assert sha256_file(source) == original_sha
    monkeypatch.setattr(trade_aggregation, "_publish_atomic", publish)
    assert prepare_day(source, [], DAY, staged)["status"] == "PREPARED_VERIFIED"
    assert sha256_file(source) == original_sha


def test_primary_replacement_between_hash_and_read_cannot_get_success_receipt(tmp_path, monkeypatch):
    source = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({}))
    original_hash = trade_aggregation.sha256_file
    changed = False

    def replace_after_hash(path):
        nonlocal changed
        result = original_hash(path)
        if Path(path) == source and not changed:
            changed = True
            write_parquet(source, primary({"id": 11}))
        return result

    monkeypatch.setattr(trade_aggregation, "sha256_file", replace_after_hash)
    staged = tmp_path / "prepared"
    with pytest.raises(ValueError):
        prepare_day(source, [], DAY, staged)
    assert not (staged / "receipt.json").exists()


def write_tardis_csv(path: Path, *records) -> Path:
    fields = ["exchange", "symbol", "timestamp", "local_timestamp", "id", "side", "price", "amount"]
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for override in records:
        row = dict(exchange="binance-futures", symbol="BTCUSDC", timestamp=(START + 50) * 1000,
                   local_timestamp=(START + 53) * 1000 + 17, id=10, side="buy", price="100.0", amount="0.0010")
        row.update(override)
        writer.writerow(row)
    content = stream.getvalue().encode()
    if path.suffix == ".xz":
        content = lzma.compress(content)
    elif path.suffix in {".zst", ".zstd"}:
        content = zstd.ZstdCompressor().compress(content)
    path.write_bytes(content)
    return path


@pytest.mark.parametrize("suffix", [".csv", ".csv.xz", ".csv.zst", ".csv.zstd"])
def test_tardis_csv_secondary_preserves_distinct_ids_side_decimal_and_receive_clock(tmp_path, suffix):
    path = write_tardis_csv(tmp_path / ("trades" + suffix), {},
                            {"id": 11, "side": "sell", "amount": "0.00000001", "price": "100.00000001"},
                            {"id": 12})
    source = trade_aggregation._read_secondary(path)
    result, stats = union_trades(primary({}), source, DAY)
    assert result["id"].to_pylist() == [10, 11, 12]
    assert result["time"].to_pylist() == [START + 50] * 3
    assert result["is_buyer_maker"].to_pylist() == [False, True, False]
    assert result["side"].to_pylist() == ["buy", "sell", "buy"]
    assert result["local_timestamp"].to_pylist() == [None, (START + 53) * 1000 + 17, (START + 53) * 1000 + 17]
    assert Decimal(result["qty"][1].as_py()) == Decimal("0.00000001")
    assert Decimal(result["price"][1].as_py()) == Decimal("100.00000001")
    assert stats["added_rows"] == 2 and stats["content_changed"] is True


@pytest.mark.parametrize("suffix", [".csv.xz", ".csv.zst", ".csv.zstd", ".parquet"])
def test_reference_secondary_requires_explicit_matching_symbol(tmp_path, suffix):
    path = tmp_path / ("reference" + suffix)
    if suffix == ".parquet":
        write_parquet(path, primary({"symbol": "BTCUSDT"}))
    else:
        write_tardis_csv(path, {"symbol": "BTCUSDT"})
    with pytest.raises(ValueError, match="symbol mismatch"):
        trade_aggregation._read_secondary(path)
    result = trade_aggregation._read_secondary(path, symbol="BTCUSDT")
    assert result["symbol"].to_pylist() == ["BTCUSDT"]
    assert result["trade_time"].to_pylist() == [START + 50]


def test_reference_preparation_preserves_market_boundary_and_rebuilds_individual_bars(tmp_path):
    from features.preprocess import process_file

    p = write_parquet(tmp_path / "raw/BTCUSDT" / DAY / "trades.parquet",
                      primary({"symbol": "BTCUSDT"}))
    write_parquet(tmp_path / "raw/BTCUSDT/2025-12-31/trades.parquet",
                  primary({"symbol": "BTCUSDT", "id": 9, "time": START - 1}))
    s = write_tardis_csv(tmp_path / "reference.csv.zst",
                         {"symbol": "BTCUSDT", "id": 9, "timestamp": START * 1000},
                         {"symbol": "BTCUSDT", "id": 11, "side": "sell", "amount": "0.002"})
    sources = [{"path": str(s), "sha256": sha256_file(s), "symbol": "BTCUSDT", "rows": 2}]
    out = tmp_path / "staged" / DAY
    receipt = prepare_day(p, sources, DAY, out, symbol="BTCUSDT")
    assert receipt["symbol"] == "BTCUSDT"
    assert receipt["stats"]["adjacent_primary_ids_retained_on_owner_day"] == 1
    result = pq.read_table(out / "trades.parquet")
    assert result["id"].to_pylist() == [10, 11]
    assert result["symbol"].unique().to_pylist() == ["BTCUSDT"]
    assert result.schema.metadata[b"narrowgate.symbol"] == b"BTCUSDT"
    assert pq.read_table(out / "trade_aggregates_100ms.parquet")["symbol"].unique().to_pylist() == ["BTCUSDT"]
    bars = tmp_path / "bars"
    bars.mkdir()
    path, status, rows, trades = process_file(out / "trades.parquet", "BTCUSDT", bars, data_type="trades")
    assert status == "ok" and rows == 1 and trades == 2
    bar = pq.read_table(path).to_pylist()[0]
    assert bar["trade_count"] == 2 and bar["buy_count"] == bar["sell_count"] == 1
    assert bar["volume"] == pytest.approx(0.003)
    assert prepare_day(p, sources, DAY, out, symbol="BTCUSDT") == receipt
    with pytest.raises(ValueError, match="symbol changed"):
        prepare_day(p, sources, DAY, out)


def test_reference_preparation_rejects_wrong_market_in_primary_or_neighbor(tmp_path):
    p = write_parquet(tmp_path / "raw/BTCUSDT" / DAY / "trades.parquet", primary({}))
    with pytest.raises(ValueError, match="symbol mismatch"):
        prepare_day(p, [], DAY, tmp_path / "bad-primary", symbol="BTCUSDT")
    write_parquet(p, primary({"symbol": "BTCUSDT"}))
    write_parquet(tmp_path / "raw/BTCUSDT/2025-12-31/trades.parquet",
                  primary({"id": 9, "time": START - 1}))
    s = write_tardis_csv(tmp_path / "reference.csv.zst", {"symbol": "BTCUSDT", "id": 11})
    with pytest.raises(ValueError, match="adjacent primary market identity"):
        prepare_day(p, [{"path": str(s), "sha256": sha256_file(s)}], DAY,
                    tmp_path / "bad-neighbor", symbol="BTCUSDT")


def test_reference_cli_selects_symbol_without_reading_other_market_manifest(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    write_parquet(raw / "binance_futures/BTCUSDT" / DAY / "trades.parquet",
                  primary({"symbol": "BTCUSDT"}))
    s = write_tardis_csv(tmp_path / "reference.csv.zst", {"symbol": "BTCUSDT", "id": 11})
    manifest = tmp_path / "sources.jsonl"
    rows = [{"day": DAY, "channel": "trades", "symbol": "BTCUSDC", "path": "not-read", "sha256": "0"},
            {"day": DAY, "channel": "trades", "symbol": "BTCUSDT", "path": str(s), "sha256": sha256_file(s)}]
    manifest.write_text("\n".join(json.dumps(row) for row in rows))
    output = tmp_path / "prepared"
    monkeypatch.setattr("sys.argv", ["trade_aggregation", "--raw-root", str(raw), "--symbol", "BTCUSDT",
                                    "--source-manifest", str(manifest), "--start-day", DAY,
                                    "--end-day", DAY, "--output-root", str(output)])
    trade_aggregation.main()
    assert json.loads((output / DAY / "receipt.json").read_text())["symbol"] == "BTCUSDT"
    assert pq.read_table(output / DAY / "trades.parquet")["symbol"].unique().to_pylist() == ["BTCUSDT"]


def test_canonical_secondary_parquet_and_direct_union_are_equivalent(tmp_path):
    table = primary({"id": 12, "is_buyer_maker": True, "local_timestamp": (START + 55) * 1000 + 321})
    path = write_parquet(tmp_path / "canonical.parquet", table)
    adapted = trade_aggregation._read_secondary(path)
    direct, stats = union_trades(primary({}), table, DAY)
    assert direct.equals(union_trades(primary({}), adapted, DAY)[0])
    assert direct["id"].to_pylist() == [10, 12]
    assert direct["local_timestamp"][1].as_py() == (START + 55) * 1000 + 321
    assert stats["content_changed"] is True


def test_secondary_accepts_verified_daily_raw_converter_output(tmp_path):
    from data.daily_raw import convert_day_channel

    source = tmp_path / "individual.csv"
    source.write_text("id,price,qty,quote_qty,time,is_buyer_maker\n"
                      f"12,100.00,0.001,0.10000,{START + 50},true\n")
    canonical = tmp_path / "trades.parquet"
    receipt = convert_day_channel(source, canonical, DAY, "trades")
    assert receipt["rows"] == 1
    adapted = trade_aggregation._read_secondary(canonical)
    result, stats = union_trades(primary({}), adapted, DAY)
    assert result["id"].to_pylist() == [10, 12]
    assert result["is_buyer_maker"].to_pylist() == [False, True]
    assert stats["content_changed"] is True


def test_matching_new_daily_source_marks_canonical_content_unchanged(tmp_path):
    original = primary({})
    p = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", original)
    s = write_parquet(tmp_path / "incoming.parquet", primary({"price": "100.0", "qty": "0.0010"}))
    receipt = prepare_day(p, source_manifest(s), DAY, tmp_path / "prepared")
    assert receipt["status"] == "PREPARED_VERIFIED"
    assert receipt["content_changed"] is receipt["canonical_replacement_required"] is False
    assert receipt["stats"]["added_rows"] == 0
    assert pq.read_table(tmp_path / "prepared/trades.parquet").equals(original)
    assert pq.read_table(p).equals(original)


@pytest.mark.parametrize("override,match", [
    ({"timestamp": (START + 50) * 1000 + 1}, "native milliseconds"),
    ({"timestamp": None}, "timestamp must contain integers"),
    ({"id": None}, "id must contain integers"),
    ({"side": "unknown"}, "aggressor side"),
    ({"side": None}, "aggressor side"),
    ({"symbol": "BTCUSDT"}, "symbol mismatch"),
    ({"exchange": "binance"}, "venue mismatch"),
])
def test_tardis_secondary_rejects_unknown_identity_side_clock_or_market(tmp_path, override, match):
    path = write_tardis_csv(tmp_path / "source.csv.xz", override)
    with pytest.raises(ValueError, match=match):
        trade_aggregation._read_secondary(path)


@pytest.mark.parametrize("field,value,match", [
    ("time", START + 51, "clock aliases"),
    ("timestamp", (START + 50) * 1000 + 1, "native milliseconds"),
    ("side", "sell", "maker aliases"),
    ("qty", "0.002", "quantity aliases"),
])
def test_canonical_secondary_rejects_alias_inconsistency(tmp_path, field, value, match):
    row = primary({}).to_pylist()[0]
    row[field] = value
    path = write_parquet(tmp_path / "source.parquet", pa.Table.from_pylist([row], schema=TRADE_SCHEMA))
    with pytest.raises(ValueError, match=match):
        trade_aggregation._read_secondary(path)


@pytest.mark.parametrize("field,value", [("price", "101"), ("amount", "0.002"), ("side", "sell")])
def test_tardis_secondary_shared_id_conflicts_are_not_averaged(tmp_path, field, value):
    path = write_tardis_csv(tmp_path / "source.csv.zst", {field: value})
    with pytest.raises(ValueError, match="price/quantity/side conflict"):
        union_trades(primary({}), trade_aggregation._read_secondary(path), DAY)


def test_tardis_csv_receive_day_does_not_change_midnight_primary_id_ownership(tmp_path):
    midnight = START + 86_400_000
    p0 = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({"time": midnight - 1}))
    p1 = write_parquet(tmp_path / "raw/2026-01-02/trades.parquet", primary({"id": 11, "time": midnight + 1}))
    source = write_tardis_csv(tmp_path / "receive-day.csv.xz",
                              {"id": 10, "timestamp": midnight * 1000, "local_timestamp": (midnight + 5) * 1000},
                              {"id": 12, "timestamp": (midnight + 2) * 1000, "local_timestamp": (midnight + 6) * 1000})
    manifest = [{"path": str(source), "sha256": sha256_file(source), "rows": 2}]
    left = prepare_day(p0, manifest, DAY, tmp_path / "left")
    right = prepare_day(p1, manifest, "2026-01-02", tmp_path / "right")
    assert pq.read_table(tmp_path / "left/trades.parquet")["id"].to_pylist() == [10]
    assert pq.read_table(tmp_path / "right/trades.parquet")["id"].to_pylist() == [11, 12]
    assert left["stats"]["time_conflicts"][0]["primary_time"] == midnight - 1
    assert right["stats"]["adjacent_primary_ids_retained_on_owner_day"] == 1


def test_secondary_reader_keeps_legacy_wrapped_parquet_receive_clock(tmp_path):
    stream = io.BytesIO()
    pq.write_table(secondary({"trade_id": 12}), stream)
    path = tmp_path / "legacy.parquet.zstd"
    path.write_bytes(zstd.ZstdCompressor().compress(stream.getvalue()))
    result, _ = union_trades(primary({}), trade_aggregation._read_secondary(path), DAY)
    assert result["local_timestamp"][1].as_py() == (START + 53) * 1000


def test_mixed_canonical_and_hourly_sources_share_one_secondary_schema(tmp_path):
    p = write_parquet(tmp_path / "raw" / DAY / "trades.parquet", primary({}))
    a = write_parquet(tmp_path / "canonical.parquet", primary({"id": 11}))
    b = write_parquet(tmp_path / "hourly.parquet", secondary({"trade_id": 12}))
    prepare_day(p, source_manifest(a) + source_manifest(b), DAY, tmp_path / "prepared")
    result = pq.read_table(tmp_path / "prepared/trades.parquet")
    assert result["id"].to_pylist() == [10, 11, 12]
    assert result["local_timestamp"].to_pylist() == [None, None, (START + 53) * 1000]


@pytest.mark.parametrize("suffix", [".xz", ".zst", ".zstd"])
@pytest.mark.parametrize("damage", ["truncated", "trailing_garbage"])
def test_tardis_csv_reader_rejects_invalid_complete_frames(tmp_path, suffix, damage):
    path = write_tardis_csv(tmp_path / ("source.csv" + suffix), {"id": 12})
    original = path.read_bytes()
    path.write_bytes(original[:-2] if damage == "truncated" else original + b"not-another-frame")
    with pytest.raises((RuntimeError, EOFError, lzma.LZMAError, zstd.ZstdError)):
        trade_aggregation._read_secondary(path)


@pytest.mark.parametrize("suffix", [".xz", ".zst"])
def test_tardis_csv_reader_accepts_concatenated_complete_frames(tmp_path, suffix):
    path = write_tardis_csv(tmp_path / "source.csv", {"id": 12})
    content = path.read_bytes()
    split = len(content) // 2
    compressor = lzma.compress if suffix == ".xz" else zstd.ZstdCompressor().compress
    compressed = tmp_path / ("source.csv" + suffix)
    compressed.write_bytes(compressor(content[:split]) + compressor(content[split:]))
    result = trade_aggregation._read_secondary(compressed)
    assert result["trade_id"].to_pylist() == [12]


@pytest.mark.parametrize("field,value", [("id", "10.5"), ("id", "18446744073709551616"),
                                        ("timestamp", "1767225600050000.5")])
def test_tardis_csv_native_identity_and_clock_are_not_lossily_cast(tmp_path, field, value):
    path = write_tardis_csv(tmp_path / "source.csv", {field: value})
    with pytest.raises(pa.ArrowInvalid):
        trade_aggregation._read_secondary(path)
