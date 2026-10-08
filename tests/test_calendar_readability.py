import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.quality import calendar_content
from data.quality.calendar_content import (
    audit_book_clocks,
    audit_book_calendar,
    audit_relationships,
    audit_trade_relationships,
    observation_grid,
    scan_file,
    scan_funding,
    validate_book_consumers,
)


def _clock_only_day(root, day, *, changed=None):
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp())*1_000_000
    axis = [start, start + 86_400_000_000 - 100_000]
    data = {"timestamp": [value//1000 for value in axis],
            "last_observation_timestamp_us": [start-200_000, axis[-1]-200_000],
            "observation_age_us": [200_000, 200_000],
            "observation_kind": ["source_observed", "source_observed"],
            "bbo_last_observation_timestamp_us": [start-50_000, axis[-1]-50_000],
            "bbo_observation_age_us": [50_000, 50_000],
            "bbo_observation_kind": ["source_observed", "source_observed"],
            "bbo_usable": [True, True]}
    data.update(changed or {})
    quality = {"day": day, "symbol": "BTCUSDC", "invalid_spread_buckets": 0,
               "observation_grid": {"cross_day_inherited": False},
               "final_state": {"last_message_ts_us": start+86_400_000_000-1}}
    for kind in ("clock", "bbo", "l2"):
        (root/kind).mkdir(parents=True, exist_ok=True)
        path = root/kind/f"BTCUSDC-{kind}-{day}.parquet"
        pq.write_table(pa.table(data if kind == "clock" else {"timestamp": data["timestamp"]}), path)
        quality[f"{kind}_output"] = {"path": str(path), "sha256": sha256_file(path), "rows": 2}
    (root/"quality").mkdir(exist_ok=True)
    path = root/"quality"/f"BTCUSDC-{day}.json"
    path.write_text(json.dumps(quality))
    return path


def test_clock_only_uses_real_grid_tail_not_book_final_and_never_rewrites_quality(tmp_path):
    days = ["2026-01-01", "2026-01-02"]
    paths = [_clock_only_day(tmp_path, day) for day in days]
    before = [path.read_bytes() for path in paths]
    records = [{"calendar_date": day, "research_use": {"locked": True}} for day in days]
    result = audit_book_clocks(records, tmp_path, manifest_sha="fixture")
    assert result["status"] == "COMPLETED"
    assert result["date_count"] == 2
    assert result["calendar_grid_states"] == 1_728_000
    assert [path.read_bytes() for path in paths] == before
    for kind in ("l2", "bbo"):
        first, second = [row["channels"][kind] for row in result["records"]]
        assert first["grid_predecessor"] is None
        assert second["grid_predecessor"] == [first["grid"]["last_output_us"], first["grid"]["last_observed_us"]]
        assert second["grid"]["cross_day_inherited"] is True
        assert result["channels"][kind]["verified_grid_predecessor_days"] == 1
    assert result["records"][0]["channels"]["l2"]["invalid_quote_rows"] == "UNKNOWN"
    assert all(row["research_use"] == {"locked": True} for row in result["records"])
    assert not result["raw_payload_read"] and not result["price_payload_read"]
    assert not result["current_quality_or_index_written"]


def test_clock_only_missing_day_stays_in_denominator_and_does_not_seed_next_day(tmp_path):
    days = ["2026-01-01", "2026-01-02", "2026-01-03"]
    for day in (days[0], days[2]):
        _clock_only_day(tmp_path, day)
    result = audit_book_clocks([{"calendar_date": day} for day in days], tmp_path, manifest_sha="fixture")
    assert result["status_counts"] == {"CHECKED": 2, "BLOCKED": 1}
    assert [r["calendar_date"] for r in result["records"]] == days
    assert result["records"][2]["channels"]["l2"]["grid_predecessor"] is None
    assert result["channels"]["l2"]["unknown_state_rows"] == "UNKNOWN"


def test_clock_only_midnight_prefix_uses_previous_sample_without_refreshing_clock(tmp_path):
    first, second = "2026-01-01", "2026-01-02"
    first_quality = json.loads(_clock_only_day(tmp_path, first).read_text())
    previous_clock = pq.read_table(first_quality["clock_output"]["path"])
    old_l2 = previous_clock["last_observation_timestamp_us"][-1].as_py()
    old_bbo = previous_clock["bbo_last_observation_timestamp_us"][-1].as_py()
    start = int(datetime.fromisoformat(second).replace(tzinfo=UTC).timestamp())*1_000_000
    _clock_only_day(tmp_path, second, changed={
        "timestamp": [(start+100_000)//1000, (start+86_400_000_000-100_000)//1000],
        "last_observation_timestamp_us": [old_l2, start+86_400_000_000-300_000],
        "observation_age_us": [start+100_000-old_l2, 200_000],
        "observation_kind": ["carried_forward", "source_observed"],
        "bbo_last_observation_timestamp_us": [old_bbo, start+86_400_000_000-150_000],
        "bbo_observation_age_us": [start+100_000-old_bbo, 50_000],
        "bbo_observation_kind": ["carried_forward", "source_observed"]})
    result = audit_book_clocks([{"calendar_date": day} for day in (first, second)], tmp_path, manifest_sha="fixture")
    assert result["status"] == "COMPLETED"
    row = result["records"][1]
    for kind, observed in (("l2", old_l2), ("bbo", old_bbo)):
        assert row["channels"][kind]["grid"]["first_grid_observed_us"] == observed
        assert row["channels"][kind]["grid"]["first_grid_age_us"] == start-observed
        assert row["channels"][kind]["grid"]["unknown_state_rows"] == 0
    assert row["missing_calendar_grid_states"] > 0  # Missing emissions are not capture-complete.


@pytest.mark.parametrize("days", [["2026-01-01", "2026-01-03"], ["2026-01-01", "2026-01-01"]])
def test_clock_only_rejects_noncontinuous_or_duplicate_calendar(tmp_path, days):
    with pytest.raises(ValueError, match="unique continuous"):
        audit_book_clocks([{"calendar_date": day} for day in days], tmp_path, manifest_sha="fixture")


def test_clock_only_future_fill_not_hidden_by_quality(tmp_path):
    day = "2026-01-01"
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp())*1_000_000
    _clock_only_day(tmp_path, day, changed={"last_observation_timestamp_us": [start+1, start+86_400_000_000-200_000],
                                           "observation_age_us": [-1, 100_000]})
    result = audit_book_clocks([{"calendar_date": day}], tmp_path, manifest_sha="fixture")
    assert result["status_counts"] == {"BLOCKED": 1}
    assert result["records"][0]["channels"]["l2"]["future_fill_violations"] == 1


def test_clock_only_explicit_unknown_is_not_forward_filled(tmp_path):
    day = "2026-01-01"
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp())*1_000_000
    _clock_only_day(tmp_path, day, changed={"bbo_last_observation_timestamp_us": [None, start+86_400_000_000-150_000],
        "bbo_observation_age_us": [None, 50_000], "bbo_observation_kind": ["unknown", "source_observed"],
        "bbo_usable": [False, True]})
    result = audit_book_clocks([{"calendar_date": day}], tmp_path, manifest_sha="fixture")
    assert result["status_counts"] == {"FINDINGS": 1}
    bbo = result["records"][0]["channels"]["bbo"]
    assert bbo["unknown_state_rows"] == 1
    assert bbo["grid"]["status"] == "NOT_VERIFIED"


def test_clock_only_actual_output_axis_is_checked_by_shared_reader(tmp_path):
    day = "2026-01-01"
    quality_path = _clock_only_day(tmp_path, day)
    quality = json.loads(quality_path.read_text())
    path = Path(quality["bbo_output"]["path"])
    axis = pq.read_table(path)["timestamp"].to_pylist()
    pq.write_table(pa.table({"timestamp": [axis[0]+1, axis[1]]}), path)
    quality["bbo_output"]["sha256"] = sha256_file(path)
    quality_path.write_text(json.dumps(quality))
    result = audit_book_clocks([{"calendar_date": day}], tmp_path, manifest_sha="fixture")
    assert result["status_counts"] == {"BLOCKED": 1}
    assert "axis differs" in result["records"][0]["error"]


def test_clock_manifest_accepts_only_explicit_denial_without_rewriting_input():
    manifest = {"calendar_start": "2026-01-01", "calendar_end": "2026-01-01", "calendar_day_count": 1,
                "partial_day": {"calendar_date": "2026-01-02"}, "records": [{"calendar_date": "2026-01-01",
                "channels": [{"economic_admission": False}, {"economic_admission": "NOT_GRANTED"}]}]}
    before = copy.deepcopy(manifest)
    calendar_content.validate_clock_manifest(manifest)
    assert manifest == before
    for value in (True, None, "GRANTED", 0):
        manifest["records"][0]["channels"][0]["economic_admission"] = value
        with pytest.raises(ValueError, match="cannot grant"):
            calendar_content.validate_clock_manifest(manifest)
from data.quality.calendar_gap_manifest import sha256_file
from data.quality.calendar_readability import (
    UNKNOWN,
    _quality,
    build_readability_manifest,
    main,
    validate_readability_manifest,
)


@pytest.fixture
def inventory(tmp_path):
    path = tmp_path / "book-2026-01-01.parquet"
    pq.write_table(pa.table({"timestamp": [1, 2], "bid": [10.0, 11.0]}), path)
    spec = {"id": "book", "source": "provider", "exchange": "binance",
            "market": "usd_m_perpetual", "symbol": "BTCUSDC", "data_type": "l2",
            "stage": "processed", "version": "v1", "inventories": [
                {"node": "local", "directory": str(tmp_path), "pattern": "book-{day}.parquet"}]}
    manifest = tmp_path / "inventory.json"
    manifest.write_text(json.dumps({"datasets": [spec]}))
    return manifest


def build(path, **kwargs):
    return build_readability_manifest(path, start_day="2026-01-01",
                                      as_of=datetime(2026, 1, 4, tzinfo=UTC),
                                      dataset_ids=["book"], **kwargs)


def test_all_dates_retained_and_partial_separate(inventory):
    out = build(inventory)
    assert out["calendar_day_count"] == 3
    assert out["partial_day"]["calendar_date"] == "2026-01-04"
    assert out["records"][0]["channels"][0]["reader"] == "METADATA_READABLE"
    assert out["records"][1]["channels"][0]["reader"] == "BLOCKED_OR_UNREGISTERED"
    for row in out["records"]:
        channel = row["channels"][0]
        assert channel["raw_observed_coverage"] == UNKNOWN
        assert channel["future_fill_violations"] == UNKNOWN
        assert channel["unknown_missing_coverage"] == UNKNOWN
        assert channel["economic_admission"] == "NOT_GRANTED"


def test_rights_not_erased_by_missing_data(inventory, tmp_path):
    usage = tmp_path / "usage.json"
    usage.write_text(json.dumps({"days": ["2026-01-02"]}))
    out = build(inventory, usage_sources=[{"path": str(usage), "day_fields": ["days"],
                                          "role": "Development", "identity": "used"}])
    assert out["records"][1]["research_use"]["development"] == "PREVIOUSLY_USED"
    assert out["records"][0]["research_use"]["training"] == UNKNOWN
    assert out["records"][1]["research_use"]["new_use_authorized"] is False


def test_owner_fixed_calendar_does_not_expand_with_current_time(inventory):
    owner = json.loads(inventory.read_text())
    owner.update(calendar_window_policy="owner_fixed", start_day="2026-01-01",
                 end_day="2026-01-02")
    inventory.write_text(json.dumps(owner))
    out = build(inventory)
    assert out["calendar_day_count"] == 2
    assert out["calendar_end"] == "2026-01-02"
    assert out["calendar_window_policy"] == "owner_fixed"
    assert out["partial_day"]["calendar_date"] == "2026-01-04"
    assert out["records"][1]["channels"][0]["reader"] == "BLOCKED_OR_UNREGISTERED"


def test_readability_preserves_explicit_synthetic_contract(inventory):
    owner = json.loads(inventory.read_text())
    owner["datasets"][0].update(aggregation_kind="synthetic_100ms",
                                native_aggtrade_identity=False, lifecycle="current")
    inventory.write_text(json.dumps(owner))
    channel = build(inventory)["records"][0]["channels"][0]
    assert channel["aggregation_kind"] == "synthetic_100ms"
    assert channel["native_aggtrade_identity"] is False
    assert channel["economic_admission"] == "NOT_GRANTED"


@pytest.mark.parametrize("changes,match", [
    ({"start_day": "2026-01-02"}, "start day differs"),
    ({"end_day": "2026-01-04"}, "complete UTC day"),
    ({"calendar_window_policy": "unknown"}, "unknown calendar"),
])
def test_owner_fixed_calendar_rejects_implicit_scope_changes(inventory, changes, match):
    owner = json.loads(inventory.read_text())
    owner.update(calendar_window_policy="owner_fixed", start_day="2026-01-01",
                 end_day="2026-01-02")
    owner.update(changes)
    inventory.write_text(json.dumps(owner))
    with pytest.raises(ValueError, match=match):
        build(inventory)


@pytest.mark.parametrize("mutation", ["drop", "duplicate", "partial", "admit"])
def test_self_check_rejects_false_manifest(inventory, mutation):
    out = copy.deepcopy(build(inventory))
    if mutation == "drop":
        out["records"].pop()
    elif mutation == "duplicate":
        out["records"][1] = out["records"][0]
    elif mutation == "partial":
        out["partial_day"]["calendar_date"] = "2026-01-01"
    else:
        out["records"][0]["channels"][0]["economic_admission"] = "PASS"
    with pytest.raises(ValueError):
        validate_readability_manifest(out)


def test_duplicate_selection_rejected(inventory):
    with pytest.raises(ValueError, match="duplicate"):
        build_readability_manifest(inventory, start_day="2026-01-01",
                                   as_of=datetime(2026, 1, 3, tzinfo=UTC),
                                   dataset_ids=["book", "book"])


def test_cli_exclusive_and_private(inventory, tmp_path):
    output = tmp_path / "result.json"
    args = ["--owner-manifest", str(inventory), "--dataset", "book", "--start-day", "2026-01-01",
            "--as-of", "2026-01-03T00:00:00+00:00", "--output", str(output)]
    assert main(args) == 0
    original = output.read_bytes()
    assert output.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        main(args)
    assert output.read_bytes() == original


def test_bad_parquet_is_blocked_not_a_missing_date(inventory, tmp_path):
    (tmp_path / "book-2026-01-01.parquet").write_bytes(b"not parquet")
    out = build(inventory)
    assert out["calendar_day_count"] == 3
    assert out["records"][0]["channels"][0]["reader"] == "BLOCKED"


def test_content_scan_reads_values_not_only_footer(tmp_path):
    path = tmp_path / "book.parquet"
    pq.write_table(pa.table({"timestamp": [1767225600000, 1767225600100],
                            "best_bid": [10., 12.], "best_ask": [11., 11.]}), path)
    result = scan_file(path, "2026-01-01", batch_size=1)
    assert result["rows"] == 2
    assert result["counts"]["invalid_spread"] == 1
    assert result["status"] == "CONTENT_FINDINGS"
    assert result["max_stale_age_us"] == "UNKNOWN"


def test_content_raw_level_rows_are_not_duplicate_events(tmp_path):
    path = tmp_path / "raw.parquet"
    pq.write_table(pa.table({"timestamp": [1767225600000000]*2,
                            "local_timestamp": [1767225600001000]*2,
                            "symbol": ["BTCUSDC"]*2, "side": ["bid", "ask"],
                            "price": ["10", "11"], "amount": ["0", "1"]}), path)
    result = scan_file(path, "2026-01-01", batch_size=1)
    assert result["status"] == "CONTENT_READABLE"
    assert result["counts"]["timestamp_us_adjacent_duplicates"] == 1
    assert result["capture_completeness"] == "UNKNOWN"
    assert result["volume_sum"] == "NOT_APPLICABLE"


def test_content_checks_trade_identity_across_chunks_not_timestamp(tmp_path):
    path = tmp_path / "trades.csv"
    path.write_text("id,time,price,qty\n1,1767225600000,10,1\n2,1767225600000,10,2\n2,1767225600001,10,1\n")
    result = scan_file(path, "2026-01-01")
    assert result["counts"]["id_adjacent_duplicates"] == 1
    assert "id_nonincreasing" in result["errors"]
    assert result["volume_sum"] == 4


def test_content_gap_includes_batch_boundary_and_no_fake_freshness(tmp_path):
    path = tmp_path / "book.parquet"
    pq.write_table(pa.table({"timestamp": [1767225600000, 1767225610000],
                            "best_bid": [10., 10.], "best_ask": [11., 11.]}), path)
    result = scan_file(path, "2026-01-01", batch_size=1)
    assert result["max_internal_gap_us"] == 10_000_000
    assert result["counts"]["gaps_gt_5000000us"] == 1
    assert result["future_fill_violations"] == "UNKNOWN"
    assert result["occupied_100ms_buckets"] == 2
    assert result["volume_sum"] == "NOT_APPLICABLE"


def test_content_checks_clock_when_observation_is_available(tmp_path):
    path = tmp_path / "book.parquet"
    pq.write_table(pa.table({"timestamp": [1767225600000, 1767225601000],
                            "last_observation_timestamp_us": [1767225600100000, 1767225600000000]}), path)
    result = scan_file(path, "2026-01-01", batch_size=1)
    assert result["future_fill_violations"] == 1
    assert result["max_stale_age_us"] == 1_000_000


def test_content_bar_start_is_not_availability_time(tmp_path):
    path = tmp_path / "bars.parquet"
    pq.write_table(pa.table({"timestamp": [1767225600000], "last_event_ts_ms": [1767225600999],
                            "open": [10.], "high": [11.], "low": [9.], "close": [10.]}), path)
    result = scan_file(path, "2026-01-01")
    assert result["counts"]["bar_event_outside_interval"] == 0


def test_content_funding_daily_identity_not_pnl(tmp_path):
    path = tmp_path / "funding.json"
    row = {"symbol": "BTCUSDC", "fundingTime": 1767225600000,
           "fundingRate": "-0.001", "markPrice": "10000"}
    path.write_text(json.dumps([row, row]))
    result = scan_funding(path, "2026-01-01")
    assert result["duplicate_ids"] == 1
    assert result["status"] == "CONTENT_FINDINGS"


@pytest.mark.parametrize("channel", ["trades", "aggTrades"])
def test_canonical_trade_parquet_content_matches_csv_without_invented_receive(tmp_path, channel):
    from data.daily_raw import convert_day_channel

    source = tmp_path / f"{channel}.csv"
    if channel == "trades":
        source.write_text("id,time,price,qty,quote_qty,is_buyer_maker\n"
                          "1,1767225600100,10.000,1.00,10.00000,true\n"
                          "2,1767225600100,11.000,2.00,22.00000,false\n")
    else:
        source.write_text("agg_trade_id,transact_time,price,quantity,first_trade_id,last_trade_id,is_buyer_maker\n"
                          "3,1767225600100,10.000,1.00,1,1,true\n"
                          "4,1767225600100,11.000,2.00,2,2,false\n")
    output = tmp_path / f"{channel}.parquet"
    convert_day_channel(source, output, "2026-01-01", channel)
    original = scan_file(source, "2026-01-01", batch_size=1)
    converted = scan_file(output, "2026-01-01", batch_size=1)
    assert converted["status"] == original["status"] == "CONTENT_READABLE"
    assert converted["volume_sum"] == original["volume_sum"] == 3
    assert converted["rows"] == original["rows"] == 2
    assert converted["time_and_id_ranges"] == original["time_and_id_ranges"]
    assert converted["top_book_digests"]["time"] == original["top_book_digests"]["time"]
    assert converted["receive_clock_status"] == UNKNOWN
    assert converted["counts"]["local_timestamp_unobserved_rows"] == 2
    assert "local_timestamp_us" not in converted["time_and_id_ranges"]
    assert "exchange_after_receive" not in converted["counts"]
    if channel == "trades":
        assert converted["counts"]["invalid_side"] == 0


def test_canonical_trade_partial_receive_clock_only_measures_known_rows(tmp_path):
    path = tmp_path / "trades.parquet"
    pq.write_table(pa.table({
        "timestamp": [1767225600000000, 1767225600100000, 1767225600200000],
        "local_timestamp": pa.array([None, 1767225600101234, None], type=pa.int64()),
        "symbol": ["BTCUSDT"] * 3, "id": [1, 2, 3],
        "side": ["buy", "sell", "buy"], "price": ["10"] * 3, "qty": ["1"] * 3,
    }), path)
    result = scan_file(path, "2026-01-01", symbol="BTCUSDT", batch_size=1)
    assert result["status"] == "CONTENT_READABLE"
    assert result["receive_clock_status"] == "PARTIALLY_OBSERVED"
    assert result["counts"]["local_timestamp_observed_rows"] == 1
    assert result["counts"]["local_timestamp_unobserved_rows"] == 2
    assert result["time_and_id_ranges"]["local_timestamp_us"] == [1767225600101234] * 2
    assert result["volume_sum"] == 3


def test_canonical_trade_duplicate_identity_is_still_a_finding(tmp_path):
    from data.daily_raw import convert_day_channel

    source = tmp_path / "trades.csv"
    source.write_text("id,time,price,qty,quote_qty,is_buyer_maker\n"
                      "7,1767225600100,10,1,10,true\n"
                      "7,1767225600200,10,1,10,true\n")
    output = tmp_path / "trades.parquet"
    convert_day_channel(source, output, "2026-01-01", "trades")
    result = scan_file(output, "2026-01-01", batch_size=1)
    assert result["rows"] == 2
    assert result["counts"]["id_adjacent_duplicates"] == 1
    assert "id_nonincreasing" in result["errors"]


def test_canonical_funding_parquet_matches_source_identity(tmp_path):
    from data.daily_raw import convert_day_channel

    source = tmp_path / "funding.json"
    source.write_text(json.dumps([
        {"symbol": "BTCUSDC", "fundingTime": 1767225600000, "fundingRate": "-0.001", "markPrice": "10000"},
        {"symbol": "BTCUSDC", "fundingTime": 1767312000000, "fundingRate": "0.002", "markPrice": "11000"},
    ]))
    output = tmp_path / "funding.parquet"
    convert_day_channel(source, output, "2026-01-01", "funding")
    original, converted = scan_funding(source, "2026-01-01"), scan_funding(output, "2026-01-01")
    for key in ("rows", "settlement_times_ms", "duplicate_ids", "status", "errors", "expected_schedule_completeness"):
        assert converted[key] == original[key]


def test_canonical_funding_missing_mark_is_unknown_not_fabricated(tmp_path):
    path = tmp_path / "funding.parquet"
    pq.write_table(pa.table({"symbol": ["BTCUSDC"], "fundingTime": [1767225600000],
                            "fundingRate": ["0.001"], "markPrice": pa.array([None], type=pa.string())}), path)
    result = scan_funding(path, "2026-01-01")
    assert result["status"] == "CONTENT_FINDINGS"
    assert result["errors"] == ["funding_identity_or_values"]


@pytest.mark.parametrize("bad_volume", [False, True])
@pytest.mark.parametrize("stored_index", [False, True])
@pytest.mark.parametrize("canonical", [False, True])
def test_content_relationships_use_ids_and_actual_bar_content(tmp_path, bad_volume, stored_index, canonical):
    child = tmp_path / "trades.csv"
    parent = tmp_path / "agg.csv"
    bar = tmp_path / "bar.parquet"
    child.write_text("id,time,price,qty,is_buyer_maker\n1,1767225600100,10,1,true\n2,1767225600200,10,2,true\n")
    parent.write_text("agg_trade_id,transact_time,price,quantity,first_trade_id,last_trade_id,is_buyer_maker\n1,1767225600100,10,3,1,2,true\n")
    if canonical:
        from data.daily_raw import convert_day_channel

        # quote_qty is source metadata in the canonical individual schema.
        child.write_text("id,time,price,qty,quote_qty,is_buyer_maker\n1,1767225600100,10,1,10,true\n2,1767225600200,10,2,20,true\n")
        convert_day_channel(child, tmp_path / "trades.parquet", "2026-01-01", "trades")
        convert_day_channel(parent, tmp_path / "agg.parquet", "2026-01-01", "aggTrades")
        child, parent = tmp_path / "trades.parquet", tmp_path / "agg.parquet"
    pq.write_table(pa.table({"timestamp": [1767225600000], "open": [10.],
                            "high": [10.], "low": [10.], "close": [10.],
                            "volume": [4. if bad_volume else 3.], "trade_count": [1]}), bar)
    if stored_index:
        frame = pq.read_table(bar).to_pandas().set_index("timestamp")
        frame.to_parquet(bar)
    record = {"calendar_date": "2026-01-01", "channels": [
        {"source_id": source, "files": [{"path": str(path)}]} for source, path in
        zip(("btcusdc-raw-trades", "btcusdc-raw-aggtrades", "btcusdc-selected-bars401"),
            (child, parent, bar), strict=True)]}
    out = audit_relationships(record, tmp_path, "test")
    assert out["trade_mapping"]["mapped_individual_rows"] == 2
    assert out["bar_trade_identity"]["volume_mismatch"] == int(bad_volume)
    assert out["status"] == ("CONTENT_FINDINGS" if bad_volume else "CONTENT_READABLE")
    with pytest.raises(FileExistsError):
        audit_relationships(record, tmp_path, "test")


@pytest.fixture
def synthetic_trade_record(tmp_path):
    from data.daily_raw import TRADE_SCHEMA
    from data.trade_aggregation import build_aggregates

    day = "2026-01-01"
    ts = 1767225600000
    trades = pa.Table.from_pylist([
        dict(exchange="binance-futures", symbol="BTCUSDC", timestamp=(ts + offset) * 1000,
             local_timestamp=None, id=identity, side="sell" if maker else "buy",
             price=price, qty=qty, amount=qty, quote_qty=None,
             time=ts + offset, is_buyer_maker=maker)
        for identity, offset, price, qty, maker in (
            (1, 10, "10", "1", True), (2, 20, "10", "2", True),
            (3, 30, "11", "3", False), (4, 120, "12", "4", True))
    ], schema=TRADE_SCHEMA)
    child, aggregate, bars = (tmp_path / f"{name}.parquet" for name in ("trades", "derived", "bars"))
    pq.write_table(trades, child)
    pq.write_table(build_aggregates(trades, day), aggregate)
    pq.write_table(pa.table({"timestamp": [ts], "open": [10.], "high": [12.],
                            "low": [10.], "close": [12.], "volume": [10.], "trade_count": [4]}), bars)
    return {"calendar_date": day, "research_use": {"locked": True}, "channels": [
        {"source_id": identity, "files": [{"path": str(path)}]}
        for identity, path in (("btcusdc-raw-trades", child),
                               ("btcusdc-derived-trade-aggregates-100ms", aggregate),
                               ("btcusdc-selected-bars401", bars))]}


def test_synthetic_content_has_explicit_clock_and_no_native_identity(synthetic_trade_record):
    path = synthetic_trade_record["channels"][1]["files"][0]["path"]
    result = scan_file(Path(path), "2026-01-01", batch_size=1)
    assert result["status"] == "CONTENT_READABLE"
    assert result["native_aggtrade_identity"] is False
    assert result["execution_event_authority"] is False
    assert result["counts"]["timestamp_us_adjacent_duplicates"] == 1
    assert result["volume_sum"] == 10
    assert result["max_stale_age_us"] == UNKNOWN


def test_synthetic_conservation_and_individual_bar_counts(tmp_path, synthetic_trade_record):
    result = audit_relationships(synthetic_trade_record, tmp_path, "test")
    assert result["status"] == "CONTENT_READABLE"
    assert result["trade_mapping"]["status"] == "UNAVAILABLE"
    assert result["trade_mapping"]["synthetic_substitution"] is False
    assert result["bar_source_semantics"] == "individual_trades"
    assert result["bar_trade_identity"]["trade_count_mismatch"] == 0
    assert result["synthetic_conservation"]["individual_rows"] == 4
    assert result["synthetic_conservation"]["synthetic_rows"] == 3
    assert result["synthetic_conservation"]["synthetic_child_count"] == 4


@pytest.mark.parametrize("field,value", [("quantity", "99"), ("trade_count", 99),
                                        ("feature_ready_ts_ms", 1767225600000)])
def test_synthetic_conservation_detects_changed_values(tmp_path, synthetic_trade_record, field, value):
    path = synthetic_trade_record["channels"][1]["files"][0]["path"]
    table = pq.read_table(path)
    values = table[field].to_pylist()
    values[0] = value
    table = table.set_column(table.schema.get_field_index(field), field,
                             pa.array(values, type=table[field].type))
    pq.write_table(table, path)
    result = audit_relationships(synthetic_trade_record, tmp_path, "test")
    assert result["status"] == "CONTENT_FINDINGS"
    assert result["synthetic_conservation"]["counts"][field + "_mismatch"] == 1
    if field == "feature_ready_ts_ms":
        scanned = scan_file(Path(path), "2026-01-01")
        assert scanned["counts"]["invalid_synthetic_interval"] == 1


@pytest.mark.parametrize("registration", ["absent", "retired", "mislabelled"])
def test_native_mapping_unavailable_never_uses_synthetic(tmp_path, synthetic_trade_record, registration):
    record = copy.deepcopy(synthetic_trade_record)
    if registration != "absent":
        spec = copy.deepcopy(record["channels"][1])
        spec["source_id"] = "btcusdc-raw-aggtrades"
        if registration == "retired":
            spec.update(lifecycle="historical", files=[])
        record["channels"].append(spec)
    result = audit_trade_relationships(record, tmp_path, "test", {record["calendar_date"]: record})
    assert result["status"] == "UNAVAILABLE"
    assert result["trade_mapping"]["synthetic_substitution"] is False
    assert result["research_use"]["locked"] is True
    assert result["inputs"] == []


@pytest.fixture
def trade_relationship_calendar(tmp_path):
    from data.daily_raw import AGG_SCHEMA, TRADE_SCHEMA

    midnight = int(datetime(2026, 1, 2, tzinfo=UTC).timestamp() * 1000)
    records = {}
    for day, children, parents in (
        ("2026-01-01", [(101, midnight - 100, "1", True)],
         [(501, midnight - 100, 101, 102, "3", True)]),
        ("2026-01-02", [(102, midnight + 50, "2", True),
                        (103, midnight + 200, "4", False)],
         [(502, midnight + 200, 103, 103, "4", False)]),
    ):
        directory = tmp_path / day
        directory.mkdir()
        child_rows = [
            {"exchange": "binance-futures", "symbol": "BTCUSDC",
             "timestamp": ts * 1000, "local_timestamp": None, "id": identity,
             "side": "sell" if maker else "buy", "price": "10",
             "amount": qty, "quote_qty": str(int(qty) * 10),
             "time": ts, "qty": qty, "is_buyer_maker": maker}
            for identity, ts, qty, maker in children
        ]
        parent_rows = [
            {"exchange": "binance-futures", "symbol": "BTCUSDC",
             "timestamp": ts * 1000, "local_timestamp": None,
             "agg_trade_id": identity, "first_trade_id": first,
             "last_trade_id": last, "price": "10", "quantity": qty,
             "transact_time": ts, "is_buyer_maker": maker}
            for identity, ts, first, last, qty, maker in parents
        ]
        channels = []
        for name, source, rows, schema in (
            ("trades", "btcusdc-raw-trades", child_rows, TRADE_SCHEMA),
            ("aggTrades", "btcusdc-raw-aggtrades", parent_rows, AGG_SCHEMA),
        ):
            path = directory / f"{name}.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
            channels.append({"source_id": source, "symbol": "BTCUSDC",
                             "market": "usd_m_perpetual", "files": [{"path": str(path)}]})
        records[day] = {"calendar_date": day, "channels": channels}
    return records


def _replace_trade_column(record, source_id, column, values):
    channel = next(c for c in record["channels"] if c["source_id"] == source_id)
    path = channel["files"][0]["path"]
    table = pq.read_table(path)
    index = table.schema.get_field_index(column)
    replacement = pa.array(values, type=table.field(column).type)
    pq.write_table(table.set_column(index, column, replacement), path)


def test_trade_relationships_cross_midnight_without_bars_keep_daily_denominators(
    tmp_path, trade_relationship_calendar,
):
    records = trade_relationship_calendar
    results = {day: audit_trade_relationships(record, tmp_path, "synthetic", records)
               for day, record in records.items()}
    before, after = results.values()
    assert all(result["status"] == "CONTENT_READABLE" for result in results.values())
    assert [result["target_individual_rows"] for result in results.values()] == [1, 2]
    assert [result["target_aggregate_rows"] for result in results.values()] == [1, 1]
    assert sum(result["target_individual_rows"] for result in results.values()) == 3
    assert [result["trade_mapping"]["mapped_individual_rows"]
            for result in results.values()] == [2, 1]
    assert [result["borrowed_individual_rows"] for result in results.values()] == [1, 0]
    assert before["adjacent_context"]["2026-01-02"]["borrowed_children"] == 1
    assert after["target_child_coverage"] == {
        "mapped": 2, "unmapped": 0, "unmapped_id_examples": [],
        "unmapped_quantity_btc": "0",
        "mapped_to_adjacent_day_parent": 1,
    }
    assert not list(tmp_path.rglob("*bar*"))
    assert all("bar_trade_identity" not in result for result in results.values())
    assert json.loads((tmp_path / "2026-01-02.json").read_text()) == after


@pytest.mark.parametrize("day", ["2026-01-01", "2026-01-02"])
def test_trade_relationships_missing_adjacent_boundary_is_a_finding(
    tmp_path, trade_relationship_calendar, day,
):
    record = trade_relationship_calendar[day]
    result = audit_trade_relationships(record, tmp_path, "synthetic", {day: record})
    assert result["status"] == "CONTENT_FINDINGS"
    assert "individual_aggregate_relation" in result["errors"]
    other_day = next(other for other in trade_relationship_calendar if other != day)
    assert result["adjacent_context"][other_day] == "OUTSIDE_REGISTERED_CALENDAR"
    if day == "2026-01-01":
        assert result["trade_mapping"]["partial_child_aggregate_rows"] == 1
        assert result["trade_mapping"]["status"] == "findings"
    else:
        assert result["target_child_coverage"]["unmapped_id_examples"] == [102]
        assert result["target_child_coverage"]["unmapped_quantity_btc"] == "2"
        assert result["target_individual_rows"] == 2


@pytest.mark.parametrize("column,values,reason", [
    ("quantity", ["4"], "quantity_match"),
    ("is_buyer_maker", [False], "aggressor_side_match"),
])
def test_trade_relationships_value_findings_remain_structured(
    tmp_path, trade_relationship_calendar, column, values, reason,
):
    record = trade_relationship_calendar["2026-01-01"]
    _replace_trade_column(record, "btcusdc-raw-aggtrades", column, values)
    result = audit_trade_relationships(record, tmp_path, "synthetic", trade_relationship_calendar)
    assert result["status"] == "CONTENT_FINDINGS"
    assert result["trade_mapping"]["status"] == "findings"
    assert result["trade_mapping"]["reason_counts"][reason] == 1
    assert result["trade_mapping"]["reason_denominator_aggregate_rows"] == 1
    assert result["trade_mapping"]["aggregate_finding_examples"][0]["agg_trade_id"] == 501
    assert result["target_individual_rows"] == 1
    assert result["trade_mapping"]["mapped_individual_rows"] == 2


@pytest.mark.parametrize("source_id", ["btcusdc-raw-trades", "btcusdc-raw-aggtrades"])
def test_trade_relationships_reject_inconsistent_canonical_clock(
    tmp_path, trade_relationship_calendar, source_id,
):
    record = trade_relationship_calendar["2026-01-01"]
    _replace_trade_column(record, source_id, "timestamp", [1767311999900001])
    result = audit_trade_relationships(record, tmp_path, "synthetic", trade_relationship_calendar)
    assert result["status"] == "CONTENT_FINDINGS"
    assert any("canonical exchange clock differs" in error for error in result["errors"])
    assert "trade_mapping" not in result


@pytest.mark.parametrize("location", ["channel", "rows"])
def test_trade_relationships_reject_other_source_symbol(
    tmp_path, trade_relationship_calendar, location,
):
    record = trade_relationship_calendar["2026-01-01"]
    if location == "channel":
        record["channels"][0]["symbol"] = "BTCUSDT"
    else:
        _replace_trade_column(record, "btcusdc-raw-trades", "symbol", ["BTCUSDT"])
    result = audit_trade_relationships(record, tmp_path, "synthetic", trade_relationship_calendar)
    assert result["status"] == "CONTENT_FINDINGS"
    assert any("symbol" in error for error in result["errors"])
    assert "trade_mapping" not in result


def test_trade_relationships_adjacent_duplicate_parent_is_not_deduplicated(
    tmp_path, trade_relationship_calendar,
):
    record = trade_relationship_calendar["2026-01-02"]
    _replace_trade_column(record, "btcusdc-raw-aggtrades", "agg_trade_id", [501])
    result = audit_trade_relationships(record, tmp_path, "synthetic", trade_relationship_calendar)
    assert result["status"] == "CONTENT_FINDINGS"
    assert any("aggregate IDs duplicate" in error for error in result["errors"])
    assert result["target_individual_rows"] == 2
    assert result["target_aggregate_rows"] == 1
    assert "trade_mapping" not in result


def test_csv_quality_and_hash_mismatch_stay_distinct(tmp_path):
    path = tmp_path / "quality.csv"
    path.write_text("day,formal_eligible\n2026-01-01,False\n")
    row = {"day": "2026-01-01", "source_quality_path": str(path),
           "source_quality_sha256": sha256_file(path)}
    result = _quality(row)
    assert result["status"] == "RECORDED_CSV_HASH_VERIFIED"
    assert result["fields"]["formal_eligible"] == "False"
    path.write_text("changed")
    assert _quality(row)["status"] == "IDENTITY_MISMATCH"


def test_locked_rights_never_become_development(inventory, tmp_path):
    usage = tmp_path / "usage.json"
    usage.write_text(json.dumps({"days": ["2026-01-02"]}))
    result = build(inventory, usage_sources=[{"path": str(usage), "day_fields": ["days"],
                                             "role": "sealed_holdout", "identity": "sealed"}])
    rights = result["records"][1]["research_use"]
    assert rights["locked"] is True
    assert rights["holdout"] == "SEALED"
    assert rights["development"] == UNKNOWN


def test_observation_grid_preserves_unknown_edges_and_real_clock_age():
    start = 1767225600000000
    result = observation_grid(
        "2026-01-01", np.array([start + 100000, start + 500000]),
        np.array([start + 100000, start + 100000]),
    )
    assert result["grid_rows"] == 864000
    assert result["unknown_state_rows"] == 1
    assert result["first_grid_observed_us"] == UNKNOWN
    assert result["last_observed_us"] == start + 100000
    assert result["max_stale_age_us"] == 86399800000
    assert result["future_fill_violations"] == result["carry_age_violations"] == 0
    assert result["missing_update_semantics"] == "UNKNOWN_NOT_CONFIRMED_NO_UPDATE"
    assert result["account_order_fifo_continuity"] == "NOT_TESTED"


def test_observation_grid_carries_midnight_without_resetting_source_time():
    start = 1767312000000000
    previous = (start - 100000, start - 300000)
    result = observation_grid("2026-01-02", np.array([start + 200000]),
                              np.array([start + 200000]), previous)
    assert result["cross_day_inherited"] is True
    assert result["first_grid_observed_us"] == start - 300000
    assert result["first_grid_age_us"] == 300000
    assert result["unknown_state_rows"] == 0
    assert result["last_observed_us"] == start + 200000
    assert result["future_fill_violations"] == result["carry_age_violations"] == 0


@pytest.mark.parametrize("outputs,observations,previous", [
    ([100000], [100001], None),
    ([100000, 200000], [100000, 99999], None),
    ([100000, 100000], [100000, 100000], None),
    ([100000], [0], (-100000, 1)),
])
def test_observation_grid_rejects_future_reversed_and_invalid_previous(
    outputs, observations, previous,
):
    start = 1767225600000000
    prior = tuple(start + value for value in previous) if previous else None
    with pytest.raises(ValueError):
        observation_grid("2026-01-01", start + np.array(outputs),
                         start + np.array(observations), prior)


@pytest.fixture
def book_calendar(tmp_path):
    """Tiny Parquet source clocks and retained BBO/L2; no economics."""
    root = tmp_path / "retained"
    origin = tmp_path / "producer"
    for base in (root, origin):
        for channel in ("bbo", "l2", "clock", "quality"):
            (base / channel).mkdir(parents=True)
    records, sources = [], []
    for offset in range(2):
        day = f"2026-01-0{offset + 1}"
        start = 1767225600000000 + offset * 86400000000
        times = [start + 100000, start + 86399900000]
        raw = tmp_path / f"raw-{day}.parquet"
        pq.write_table(pa.table({"timestamp": times}), raw)
        bbo = pa.table({"timestamp": [v // 1000 for v in times],
                        "best_bid": [10.0, 10.0], "best_ask": [11.0, 11.0]})
        l2 = pa.table({"timestamp": [v // 1000 for v in times],
                       "bid_px_1": [10.0, 10.0], "ask_px_1": [11.0, 11.0],
                       "bid_qty_1": [1.0, 1.0], "ask_qty_1": [1.0, 1.0]})
        hashes = {}
        for kind, table in (("bbo", bbo), ("l2", l2)):
            p = origin / kind / f"BTCUSDC-{kind}-{day}.parquet"
            pq.write_table(table, p)
            (root / kind / p.name).write_bytes(p.read_bytes())
            hashes[f"{kind}_sha256"] = sha256_file(p)
        sources.append({"day": day, "source_clock": "transaction",
                        "root": str(origin), "source_quality": hashes})
        records.append({"calendar_date": day,
                        "research_use": {"locked": True, "new_use_authorized": False},
                        "channels": [{"source_id": "btcusdc-daily-raw-l2",
                                      "source_content_validation": {"sha256": sha256_file(raw),
                                                                    "source_sha256": sha256_file(raw)},
                                      "files": [{"path": str(raw), "sha256": sha256_file(raw)}]}]})
    (root / "manifest.json").write_text(json.dumps({"sources": sources}))
    return records, root, origin


def test_book_calendar_causal_read_does_not_claim_reconstruction(book_calendar, tmp_path):
    records, root, _ = book_calendar
    summary = audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")
    assert summary["status"] == "COMPLETED"
    assert summary["days"] == 2
    assert summary["future_fill_violations"] == summary["carry_age_violations"] == 0
    assert summary["raw_reconstruction"] == "RETAINED_NOT_REBUILT"
    assert summary["economic_replay"] is False
    second = json.loads((root / "quality/BTCUSDC-2026-01-02.json").read_text())
    assert second["grid"]["first_grid_age_us"] == 100000
    assert second["grid"]["cross_day_inherited"] is True
    assert second["research_use"] == records[1]["research_use"]
    assert second["provider_normalized_replay_candidate"] is False
    assert second["exact_queue"] is False
    binding = second["source_clock_binding"]
    assert binding["producer"] == "data.downloaders.cryptohft_orderbook._emit_snapshot"
    producer = Path(__file__).resolve().parents[1] / "data/downloaders/cryptohft_orderbook.py"
    assert binding["producer_source_sha256"] == sha256_file(producer)


@pytest.fixture(params=["reconstructed_fusion.v1", "observed_union.v1"])
def fused_book_calendar(book_calendar, request):
    """Same-pass fusion whose real clock differs from presentation/grid time."""
    records, root, _ = book_calendar
    representation = request.param
    sources = []
    for record in records:
        day = record["calendar_date"]
        channel = record["channels"][0]
        raw = Path(channel["files"][0]["path"])
        raw_original = sha256_file(raw)
        axis = np.asarray(pq.read_table(root / "bbo" / f"BTCUSDC-bbo-{day}.parquet")["timestamp"])
        observed = axis * 1000 - 30_000
        clock_path = root / "clock" / f"BTCUSDC-clock-{day}.parquet"
        clock = pa.table({"timestamp": axis, "last_observation_timestamp_us": observed,
                          "observation_age_us": np.full(2, 30_000, dtype=np.int64),
                          "observation_kind": ["source_observed", "carried_forward"],
                          "source_id": ["tardis", "cryptohft"],
                          "update_coverage": ["source_message_present", "unknown"]})
        pq.write_table(clock, clock_path)
        outputs = {kind: {"path": str(root / kind / f"BTCUSDC-{kind}-{day}.parquet"),
                           "sha256": sha256_file(root / kind / f"BTCUSDC-{kind}-{day}.parquet"), "rows": 2}
                   for kind in ("bbo", "l2", "clock")}
        included = [{"source_id": "tardis", "sha256": raw_original, "rows": 2}]
        receipt = {"schema": representation, "day": day, "symbol": "BTCUSDC", "output_rows": 2,
                   "included_sources": included, "stats": {"normalized": outputs}}
        table = pa.table({"timestamp": observed + 10_000,
                          "source_observed_timestamp_us": observed})
        pq.write_table(table.replace_schema_metadata({
            b"narrowgate.book_fusion": representation.encode(),
            b"narrowgate.fusion_receipt": json.dumps(receipt).encode()}), raw)
        raw_sha = sha256_file(raw)
        channel["source_content_validation"].update(sha256=raw_sha, source_sha256=raw_sha)
        channel["files"][0]["sha256"] = raw_sha
        quality = {"schema": "book_fusion_same_pass.v1", "day": day, "symbol": "BTCUSDC",
                   "timestamp_source": "exchange", "raw_reconstruction": representation,
                   "included_sources": included, "raw_source": {"sha256": raw_sha},
                   "economic_admission": False,
                   **{kind + "_output": claim for kind, claim in outputs.items()}}
        qpath = root / "quality" / f"BTCUSDC-{day}.json"
        qpath.write_text(json.dumps(quality))
        sources.append({"day": day, "source_clock": "fused_exchange", "root": str(root),
                        "raw_sha256": raw_sha, "quality_sha256": sha256_file(qpath), "included_sources": included})
    (root / "manifest.json").write_text(json.dumps({"sources": sources}))
    return records, root


def test_fused_calendar_binds_real_clock_and_preserves_producer_bytes(fused_book_calendar, tmp_path):
    records, root = fused_book_calendar
    before = {p: sha256_file(p) for kind in ("bbo", "l2", "clock", "quality") for p in (root / kind).iterdir()}
    output = tmp_path / "fusion-acceptance"
    result = audit_book_calendar(records, root, output, "fused-fixture")
    assert result["status"] == "COMPLETED"
    marker = pq.ParquetFile(records[0]["channels"][0]["files"][0]["path"]).metadata.metadata[b"narrowgate.book_fusion"].decode()
    assert result["raw_reconstruction"] == marker
    assert result["economic_replay"] is False
    assert {p: sha256_file(p) for p in before} == before
    rows = [json.loads(line) for line in (output / "calendar.jsonl").read_text().splitlines()]
    assert all(row["source_clock_membership_missing"] == 0 for row in rows)
    first = rows[0]
    assert first["raw_source"]["exchange_clock_column"] == "source_observed_timestamp_us"
    assert first["raw_source"]["presentation_clock_column"] == "timestamp"
    assert first["source_clock_binding"]["canonical_source_membership"] == "FUSED_REAL_OBSERVATION_MEMBERSHIP_VERIFIED"
    accepted = json.loads(Path(first["quality_path"]).read_text())
    assert Path(first["quality_path"]).parent == output / "quality"
    assert accepted["economic_admission"] is False and accepted["research_use"] == records[0]["research_use"]
    clock = pq.read_table(root / "clock" / "BTCUSDC-clock-2026-01-01.parquet")
    assert clock["source_id"].to_pylist() == ["tardis", "cryptohft"]
    assert clock["observation_kind"].to_pylist() == ["source_observed", "carried_forward"]
    assert audit_book_calendar(records, root, output, "fused-fixture", resume=True)["status"] == "COMPLETED"


@pytest.mark.parametrize("broken_clock", [False, True])
def test_unified_daily_book_binds_observation_clock_without_supplier_metadata(
        fused_book_calendar, tmp_path, broken_clock):
    from data.daily_raw import DAILY_BOOK_MARKER

    records, root = fused_book_calendar
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for record, source in zip(records, manifest["sources"], strict=True):
        day = record["calendar_date"]
        raw = Path(record["channels"][0]["files"][0]["path"])
        table = pq.read_table(raw)
        old_receipt = json.loads(table.schema.metadata[b"narrowgate.fusion_receipt"])
        receipt = {"schema": DAILY_BOOK_MARKER, "day": day, "symbol": "BTCUSDC",
                   "output_rows": table.num_rows, "normalized": old_receipt["stats"]["normalized"]}
        table = table.rename_columns(["observed_timestamp_us" if name == "source_observed_timestamp_us"
                                      else name for name in table.column_names])
        table = table.append_column("top_only", pa.array([False] * len(table)))
        if broken_clock:
            table = table.set_column(table.schema.get_field_index("observed_timestamp_us"),
                                     "observed_timestamp_us", table["timestamp"].combine_chunks())
            # A new output timestamp is not permission to refresh observation.
        pq.write_table(table.replace_schema_metadata({
            b"narrowgate.book_fusion": DAILY_BOOK_MARKER.encode(),
            b"narrowgate.book_receipt": json.dumps(receipt).encode()}), raw)
        raw_sha = sha256_file(raw)
        record["channels"][0]["source_content_validation"]["sha256"] = raw_sha
        qpath = root / "quality" / f"BTCUSDC-{day}.json"
        quality = json.loads(qpath.read_text())
        quality.pop("included_sources")
        quality.update(raw_reconstruction=DAILY_BOOK_MARKER, raw_source={"sha256": raw_sha})
        qpath.write_text(json.dumps(quality))
        source.pop("included_sources")
        source.update(raw_sha256=raw_sha, quality_sha256=sha256_file(qpath))
    manifest_path.write_text(json.dumps(manifest))
    output = tmp_path / "unified-book"
    if broken_clock:
        with pytest.raises(ValueError, match="output source clocks absent"):
            audit_book_calendar(records, root, output, "unified-clock-defect")
    else:
        result = audit_book_calendar(records, root, output, "unified-clock")
        assert result["status"] == "COMPLETED"
        assert result["raw_reconstruction"] == DAILY_BOOK_MARKER
        rows = [json.loads(line) for line in (output / "calendar.jsonl").read_text().splitlines()]
        assert all(row["raw_source"]["exchange_clock_column"] == "observed_timestamp_us" for row in rows)
        assert all("included_sources" not in row["source_clock_binding"] for row in rows)
        assert result["economic_replay"] is False


@pytest.mark.parametrize("mutation", ["marker", "clock_column", "quality_binding", "quality_raw", "inclusion", "clock_hash", "bbo_hash"])
def test_fused_calendar_rejects_unbound_same_pass_inputs(fused_book_calendar, tmp_path, mutation):
    records, root = fused_book_calendar
    raw = Path(records[0]["channels"][0]["files"][0]["path"])
    qpath = root / "quality" / "BTCUSDC-2026-01-01.json"
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if mutation in {"marker", "clock_column"}:
        table = pq.read_table(raw)
        if mutation == "marker":
            table = table.replace_schema_metadata(None)
        else:
            table = table.drop(["source_observed_timestamp_us"])
        pq.write_table(table, raw)
        records[0]["channels"][0]["source_content_validation"]["sha256"] = sha256_file(raw)
    elif mutation == "quality_binding":
        manifest["sources"][0].pop("quality_sha256")
    elif mutation in {"quality_raw", "inclusion"}:
        value = json.loads(qpath.read_text())
        if mutation == "quality_raw":
            value["raw_source"]["sha256"] = "f" * 64
        else:
            value["included_sources"][0]["sha256"] = "f" * 64
        qpath.write_text(json.dumps(value))
        manifest["sources"][0]["quality_sha256"] = sha256_file(qpath)
    else:
        kind = mutation.split("_")[0]
        path = root / kind / f"BTCUSDC-{kind}-2026-01-01.parquet"
        pq.write_table(pq.read_table(path).replace_schema_metadata({b"changed": b"yes"}), path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        audit_book_calendar(records, root, tmp_path / "fusion-bad", "fused-fixture")


def test_fused_calendar_resume_rechecks_original_same_pass_quality(fused_book_calendar, tmp_path):
    records, root = fused_book_calendar
    output = tmp_path / "fusion-acceptance"
    audit_book_calendar(records, root, output, "fused-fixture")
    qpath = root / "quality" / "BTCUSDC-2026-01-01.json"
    value = json.loads(qpath.read_text())
    value["unexpected_mutation"] = True
    qpath.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="same-pass producer quality changed"):
        audit_book_calendar(records, root, output, "fused-fixture", resume=True)


def test_union_raw_activity_does_not_refresh_selected_book_clock(fused_book_calendar, tmp_path):
    records, root = fused_book_calendar
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for record, source in zip(records, manifest["sources"], strict=True):
        channel = record["channels"][0]
        raw = Path(channel["files"][0]["path"])
        old = pq.read_table(raw)
        observed = old["source_observed_timestamp_us"].to_numpy()
        # Real updates on another source need not change the selected book.
        all_observed = np.sort(np.r_[observed, observed + 20_000])
        metadata = dict(old.schema.metadata)
        receipt = json.loads(metadata[b"narrowgate.fusion_receipt"])
        receipt.update(schema="observed_union.v1", output_rows=len(all_observed))
        metadata.update({b"narrowgate.book_fusion": b"observed_union.v1",
                         b"narrowgate.fusion_receipt": json.dumps(receipt).encode()})
        table = pa.table({"timestamp": all_observed + 10_000,
                          "source_observed_timestamp_us": all_observed})
        pq.write_table(table.replace_schema_metadata(metadata), raw)
        digest = sha256_file(raw)
        channel["source_content_validation"]["sha256"] = digest
        qpath = root / "quality" / f"BTCUSDC-{record['calendar_date']}.json"
        quality = json.loads(qpath.read_text())
        quality.update(raw_reconstruction="observed_union.v1", raw_source={"sha256": digest})
        qpath.write_text(json.dumps(quality))
        source.update(raw_sha256=digest, quality_sha256=sha256_file(qpath))
    manifest_path.write_text(json.dumps(manifest))
    clocks = {p: sha256_file(p) for p in (root / "clock").iterdir()}
    output = tmp_path / "union-activity"
    result = audit_book_calendar(records, root, output, "union-fixture")
    assert result["status"] == "COMPLETED"
    assert result["raw_reconstruction"] == "observed_union.v1"
    assert {p: sha256_file(p) for p in clocks} == clocks
    rows = [json.loads(line) for line in (output / "calendar.jsonl").read_text().splitlines()]
    for row in rows:
        binding = row["source_clock_binding"]
        assert binding["native_sequence_authority"] is False
        assert binding["freshness_authority"] == "HASH_BOUND_SELECTED_NORMALIZED_CLOCK_NOT_RAW_UNION_ACTIVITY"
        assert row["grid"]["last_observed_us"] == row["raw_source"]["last_exchange_us"] - 20_000


def test_calendar_rejects_unknown_raw_representation(fused_book_calendar, tmp_path):
    records, root = fused_book_calendar
    raw = Path(records[0]["channels"][0]["files"][0]["path"])
    table = pq.read_table(raw)
    metadata = dict(table.schema.metadata)
    metadata[b"narrowgate.book_fusion"] = b"unknown_representation"
    pq.write_table(table.replace_schema_metadata(metadata), raw)
    with pytest.raises(ValueError, match="unrecognized canonical book fusion schema"):
        audit_book_calendar(records, root, tmp_path / "unknown-schema", "unknown-fixture")


def test_fused_calendar_reads_late_parquet_receipt_not_only_arrow_schema(fused_book_calendar, tmp_path):
    records, root = fused_book_calendar
    channel = records[0]["channels"][0]
    raw = Path(channel["files"][0]["path"])
    table = pq.read_table(raw)
    metadata = dict(table.schema.metadata)
    late_receipt = metadata.pop(b"narrowgate.fusion_receipt")
    table = table.replace_schema_metadata(metadata)
    with pq.ParquetWriter(raw, table.schema) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata({b"narrowgate.fusion_receipt": late_receipt})
    parquet = pq.ParquetFile(raw)
    assert b"narrowgate.fusion_receipt" not in parquet.schema_arrow.metadata
    assert parquet.metadata.metadata[b"narrowgate.fusion_receipt"] == late_receipt
    raw_sha = sha256_file(raw)
    channel["source_content_validation"]["sha256"] = raw_sha
    qpath = root / "quality" / "BTCUSDC-2026-01-01.json"
    quality = json.loads(qpath.read_text())
    quality["raw_source"]["sha256"] = raw_sha
    qpath.write_text(json.dumps(quality))
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["sources"][0].update(raw_sha256=raw_sha, quality_sha256=sha256_file(qpath))
    manifest_path.write_text(json.dumps(manifest))
    result = audit_book_calendar(records, root, tmp_path / "late-footer-acceptance", "late-receipt")
    marker = parquet.metadata.metadata[b"narrowgate.book_fusion"].decode()
    assert result["status"] == "COMPLETED" and result["raw_reconstruction"] == marker


@pytest.mark.parametrize("defect", ["age", "receipt_scope"])
def test_fused_calendar_valid_hashes_do_not_override_clock_or_scope_errors(fused_book_calendar, tmp_path, defect):
    records, root = fused_book_calendar
    channel = records[0]["channels"][0]
    raw = Path(channel["files"][0]["path"])
    table = pq.read_table(raw)
    metadata = dict(table.schema.metadata)
    receipt = json.loads(metadata[b"narrowgate.fusion_receipt"])
    qpath = root / "quality" / "BTCUSDC-2026-01-01.json"
    quality = json.loads(qpath.read_text())
    if defect == "age":
        clock_path = root / "clock" / "BTCUSDC-clock-2026-01-01.parquet"
        clock = pq.read_table(clock_path)
        clock = clock.set_column(clock.schema.get_field_index("observation_age_us"),
                                 "observation_age_us", pa.array([29_999, 30_000], type=pa.int64()))
        pq.write_table(clock, clock_path)
        digest = sha256_file(clock_path)
        quality["clock_output"]["sha256"] = digest
        receipt["stats"]["normalized"]["clock"]["sha256"] = digest
    else:
        receipt["day"] = "2026-01-02"
    metadata[b"narrowgate.fusion_receipt"] = json.dumps(receipt).encode()
    pq.write_table(table.replace_schema_metadata(metadata), raw)
    raw_sha = sha256_file(raw)
    channel["source_content_validation"]["sha256"] = raw_sha
    quality["raw_source"]["sha256"] = raw_sha
    qpath.write_text(json.dumps(quality))
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["sources"][0].update(raw_sha256=raw_sha, quality_sha256=sha256_file(qpath))
    manifest_path.write_text(json.dumps(manifest))
    match = "clock age" if defect == "age" else "receipt scope"
    with pytest.raises(ValueError, match=match):
        audit_book_calendar(records, root, tmp_path / "bad-semantic-binding", "fused-fixture")


def test_book_calendar_rejects_output_clock_absent_from_raw(book_calendar, tmp_path):
    records, root, _ = book_calendar
    raw = Path(records[0]["channels"][0]["files"][0]["path"])
    pq.write_table(pa.table({"timestamp": [1767225600000001]}), raw)
    records[0]["channels"][0]["source_content_validation"]["sha256"] = sha256_file(raw)
    with pytest.raises(ValueError, match="source clocks absent"):
        audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")
    state = json.loads((tmp_path / "acceptance/state.json").read_text())
    assert state["status"] == "FAILED"
    assert state["completed"] == 0
    assert not (tmp_path / "acceptance/calendar.jsonl").exists()


@pytest.mark.parametrize("future_only", [False, True])
def test_book_calendar_next_capture_partition_only_proves_past_exchange_event(
    book_calendar, tmp_path, future_only,
):
    records, root, _ = book_calendar
    missing = 1767311999900000
    first = records[0]["channels"][0]
    raw = Path(first["files"][0]["path"])
    pq.write_table(pa.table({"timestamp": [1767225600100000]}), raw)
    first["source_content_validation"]["sha256"] = sha256_file(raw)
    second = records[1]["channels"][0]
    raw = Path(second["files"][0]["path"])
    times = pq.read_table(raw)["timestamp"].to_pylist()
    pq.write_table(pa.table({"timestamp": sorted([*times, missing + (200000 if future_only else 0)])}), raw)
    second["source_content_validation"]["sha256"] = sha256_file(raw)
    output = tmp_path / "acceptance"
    if future_only:
        with pytest.raises(ValueError, match="source clocks absent"):
            audit_book_calendar(records, root, output, "fixture")
        return
    summary = audit_book_calendar(records, root, output, "fixture")
    assert summary["future_fill_violations"] == 0
    first_result = json.loads((output / "calendar.jsonl").read_text().splitlines()[0])
    binding = first_result["source_clock_binding"]
    assert first_result["source_clock_membership_missing"] == 0
    assert binding["following_capture_partition"]["sha256"] == sha256_file(raw)
    assert binding["following_capture_partition"]["pre_midnight_distinct_clocks"] == 1
    assert first_result["grid"]["last_observed_us"] == missing
    table = pq.read_table(raw)
    pq.write_table(table.replace_schema_metadata({b"changed": b"true"}), raw)
    with pytest.raises(ValueError, match="completed adjacent source identity changed"):
        audit_book_calendar(records, root, output, "fixture", resume=True)


def test_book_calendar_rejects_modified_retained_producer_output(book_calendar, tmp_path):
    records, root, _ = book_calendar
    path = root / "bbo/BTCUSDC-bbo-2026-01-01.parquet"
    table = pq.read_table(path)
    pq.write_table(table.replace_schema_metadata({b"changed": b"true"}), path)
    with pytest.raises(ValueError, match="differs from transaction-clock producer"):
        audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")


@pytest.mark.parametrize("target", ["raw", "bbo", "clock", "quality"])
def test_book_calendar_resume_revalidates_successful_prefix(book_calendar, tmp_path, target):
    records, root, _ = book_calendar
    output = tmp_path / "acceptance"
    audit_book_calendar(records, root, output, "fixture")
    if target == "raw":
        path = Path(records[0]["channels"][0]["files"][0]["path"])
    elif target == "quality":
        path = root / "quality/BTCUSDC-2026-01-01.json"
    else:
        path = root / target / f"BTCUSDC-{target}-2026-01-01.parquet"
    if target == "quality":
        payload = json.loads(path.read_text())
        payload["research_use"]["locked"] = False
        path.write_text(json.dumps(payload))
    else:
        table = pq.read_table(path)
        pq.write_table(table.replace_schema_metadata({b"changed": b"true"}), path)
    with pytest.raises(ValueError):
        audit_book_calendar(records, root, output, "fixture", resume=True)


@pytest.mark.parametrize("change", ["duplicate_source", "nonadjacent_days"])
def test_book_calendar_rejects_ambiguous_or_discontinuous_calendar(
    book_calendar, tmp_path, change,
):
    records, root, _ = book_calendar
    if change == "duplicate_source":
        path = root / "manifest.json"
        payload = json.loads(path.read_text())
        payload["sources"].append(copy.deepcopy(payload["sources"][0]))
        path.write_text(json.dumps(payload))
    else:
        records[1]["calendar_date"] = "2026-01-03"
    with pytest.raises(ValueError):
        audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")
    assert not (tmp_path / "acceptance").exists()


@pytest.mark.parametrize("mutation", [None, "clock_hash", "clock_axis", "future_observation"])
def test_book_calendar_tardis_requires_original_clock_binding(
    book_calendar, tmp_path, mutation,
):
    records, root, origin = book_calendar
    day = records[0]["calendar_date"]
    path = root / "manifest.json"
    manifest = json.loads(path.read_text())
    source = manifest["sources"][0]
    source["source_clock"] = "tardis_exchange"
    path.write_text(json.dumps(manifest))
    ts = np.array([1767225600100000, 1767311999900000], dtype=np.int64)
    observed = ts.copy()
    axis = ts // 1000
    if mutation == "clock_axis":
        axis[0] += 100
    elif mutation == "future_observation":
        observed[0] += 1
        raw = Path(records[0]["channels"][0]["files"][0]["path"])
        pq.write_table(pa.table({"timestamp": observed}), raw)
        records[0]["channels"][0]["source_content_validation"]["sha256"] = sha256_file(raw)
    clock_path = origin / "clock" / f"BTCUSDC-clock-{day}.parquet"
    clock = pa.table({"timestamp": axis, "last_observation_timestamp_us": observed})
    pq.write_table(clock, clock_path)
    quality = {kind + "_output": {"sha256": source["source_quality"][kind + "_sha256"]}
               for kind in ("bbo", "l2")}
    quality["clock_output"] = {"sha256": sha256_file(clock_path)}
    quality["raw_inputs"] = {"incremental_book_L2": {
        "sha256": records[0]["channels"][0]["source_content_validation"]["source_sha256"],
    }}
    (origin / "quality" / f"BTCUSDC-{day}.json").write_text(json.dumps(quality))
    if mutation == "clock_hash":
        pq.write_table(clock.replace_schema_metadata({b"changed": b"true"}), clock_path)
    if mutation:
        with pytest.raises(ValueError):
            audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")
    else:
        summary = audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")
        assert summary["status"] == "COMPLETED"
        accepted = json.loads((root / "quality" / f"BTCUSDC-{day}.json").read_text())
        assert accepted["source_clock_binding"]["kind"] == "HASH_BOUND_RESAMPLE_CLOCK"


def test_book_calendar_resumes_after_result_flush_before_state(book_calendar, tmp_path, monkeypatch):
    records, root, _ = book_calendar
    output = tmp_path / "acceptance"
    original = calendar_content._atomic_json
    injected = False

    def crash_once(path, payload):
        nonlocal injected
        if path.name == "state.json" and payload.get("status") == "RUNNING" and not injected:
            injected = True
            raise OSError("injected after durable result")
        return original(path, payload)

    monkeypatch.setattr(calendar_content, "_atomic_json", crash_once)
    with pytest.raises(OSError, match="injected"):
        audit_book_calendar(records, root, output, "fixture")
    assert len((output / "calendar.jsonl").read_text().splitlines()) == 1
    monkeypatch.setattr(calendar_content, "_atomic_json", original)
    assert audit_book_calendar(records, root, output, "fixture", resume=True)["days"] == 2
    rows = [json.loads(line) for line in (output / "calendar.jsonl").read_text().splitlines()]
    assert [row["calendar_date"] for row in rows] == [row["calendar_date"] for row in records]


def test_book_calendar_binds_raw_values_not_only_clock_membership(book_calendar, tmp_path):
    records, root, _ = book_calendar
    raw = Path(records[0]["channels"][0]["files"][0]["path"])
    table = pq.read_table(raw)
    pq.write_table(table.append_column("unexpected_values", pa.array([10.0, 20.0])), raw)
    with pytest.raises(ValueError):
        audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")


@pytest.fixture
def consumer_calendar(monkeypatch, tmp_path):
    from models import backtest_tick as replay
    from models.tick_data_types import HistoricalBBOData, HistoricalL2Data

    books, records, results = {}, [], []
    previous = None
    for offset in range(2):
        day = f"2026-01-0{offset + 1}"
        start = 1767225600000 + offset * 86400000
        times = np.array([start + 100, start + 86399900])
        observed = times * 1000
        bbo = HistoricalBBOData(times, np.full(2, 100.0), np.full(2, 101.0),
                                np.ones(2), np.ones(2), observation_ts_us=observed)
        l2 = HistoricalL2Data(times, np.tile(100.0 - np.arange(20), (2, 1)),
                              np.ones((2, 20)), np.tile(101.0 + np.arange(20), (2, 1)),
                              np.ones((2, 20)), observation_ts_us=observed)
        books[day] = {"bbo": bbo, "l2": l2}
        clock_path = tmp_path / "selected" / "clock" / f"BTCUSDC-clock-{day}.parquet"
        clock_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"timestamp": times,
                                 "last_observation_timestamp_us": observed}), clock_path)
        records.append({"calendar_date": day})
        grid = observation_grid(day, times * 1000, observed, previous)
        results.append({"calendar_date": day, "grid": grid})
        previous = grid["last_output_us"], grid["last_observed_us"]
    original_dirs = tmp_path / "original-bbo", tmp_path / "original-l2"
    monkeypatch.setattr(replay, "BBO_DIR", original_dirs[0])
    monkeypatch.setattr(replay, "L2_DIR", original_dirs[1])
    seen = []

    def load(day, kind, allowed):
        assert allowed == [r["calendar_date"] for r in records]
        seen.append((day, kind))
        return books[day][kind]

    monkeypatch.setattr(replay, "load_bbo_data",
                        lambda days, quality_allowed_days: load(days[0], "bbo", quality_allowed_days))
    monkeypatch.setattr(replay, "load_l2_data",
                        lambda days, quality_allowed_days: load(days[0], "l2", quality_allowed_days))
    return records, results, books, seen, original_dirs


def test_shared_book_consumers_keep_calendar_and_separate_observed_from_carry(
    consumer_calendar, tmp_path, monkeypatch,
):
    import os
    from models import backtest_tick as replay

    records, results, _, seen, original_dirs = consumer_calendar
    monkeypatch.setenv("MM_L2_MAX_LEVELS", "3")
    result = validate_book_consumers(records, results, tmp_path / "selected")
    assert result["days"] == 2
    assert result["grid_rows"] == 1728000
    assert result["observed_at_grid_rows"] == 4
    assert result["unknown_grid_rows"] == 1
    assert result["carried_grid_rows"] == 1727995
    assert result["midnight_inherited_days"] == 1
    assert result["retained_book_rows"] == 4
    assert result["future_fill_violations"] == result["stale_age_reset_violations"] == 0
    assert result["raw_reconstruction"] == "RETAINED_NOT_REBUILT"
    assert result["economic_replay"] is False
    assert len(seen) == 4
    assert (replay.BBO_DIR, replay.L2_DIR) == original_dirs
    assert os.environ["MM_L2_MAX_LEVELS"] == "3"


@pytest.mark.parametrize("defect", ["excluded_day", "unknown_clock", "price_mismatch", "clock_changed"])
def test_shared_book_consumers_reject_loader_loss_and_restore_context(
    consumer_calendar, tmp_path, monkeypatch, defect,
):
    import os
    from dataclasses import replace
    from models import backtest_tick as replay

    records, results, books, _, original_dirs = consumer_calendar
    monkeypatch.setenv("MM_L2_MAX_LEVELS", "3")
    day = records[1]["calendar_date"]
    if defect == "excluded_day":
        books[day]["l2"] = None
    elif defect == "unknown_clock":
        books[day]["l2"] = replace(books[day]["l2"], observation_ts_us=None)
    elif defect == "price_mismatch":
        books[day]["bbo"] = replace(books[day]["bbo"], best_bid=np.full(2, 99.0))
    else:
        for kind in ("bbo", "l2"):
            book = books[day][kind]
            books[day][kind] = replace(book, observation_ts_us=book.observation_ts_us - 1000)
    with pytest.raises(ValueError):
        validate_book_consumers(records, results, tmp_path / "selected")
    assert (replay.BBO_DIR, replay.L2_DIR) == original_dirs
    assert os.environ["MM_L2_MAX_LEVELS"] == "3"


def test_shared_book_consumer_preflight_failure_does_not_mutate_environment(
    consumer_calendar, tmp_path, monkeypatch,
):
    import os

    records, results, _, seen, _ = consumer_calendar
    monkeypatch.setenv("MM_L2_MAX_LEVELS", "3")
    with pytest.raises(ValueError, match="full completed calendar"):
        validate_book_consumers(records, results[:-1], tmp_path / "selected")
    assert seen == []
    assert os.environ["MM_L2_MAX_LEVELS"] == "3"


@pytest.fixture
def alternate_retained_book_calendar(book_calendar):
    records, root, origin = book_calendar
    day = records[0]["calendar_date"]
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    source = manifest["sources"][0]
    source["source_clock"] = "tardis_exchange"
    manifest_path.write_text(json.dumps(manifest))
    original_raw_sha = records[0]["channels"][0]["source_content_validation"]["source_sha256"]
    output_times = pq.read_table(root / "bbo" / f"BTCUSDC-bbo-{day}.parquet")["timestamp"]
    clock = pa.table({"timestamp": output_times,
                      "last_observation_timestamp_us": np.asarray(output_times) * 1000})
    clock_path = origin / "clock" / f"BTCUSDC-clock-{day}.parquet"
    pq.write_table(clock, clock_path)
    quality = {kind + "_output": {"sha256": source["source_quality"][kind + "_sha256"]}
               for kind in ("bbo", "l2")}
    quality["clock_output"] = {"sha256": sha256_file(clock_path)}
    quality["raw_inputs"] = {"incremental_book_L2": {"sha256": original_raw_sha}}
    quality_path = origin / "quality" / f"BTCUSDC-{day}.json"
    quality_path.write_text(json.dumps(quality))
    # Same market, different retained supplier source: valid clocks not in the
    # selected canonical file. Original clock/value identities remain unchanged.
    canonical = Path(records[0]["channels"][0]["files"][0]["path"])
    pq.write_table(pa.table({"timestamp": [1767225600000001]}), canonical)
    records[0]["channels"][0]["source_content_validation"].update(
        sha256=sha256_file(canonical), source_sha256=sha256_file(canonical),
    )
    return records, root, origin, quality_path, original_raw_sha


def test_book_calendar_keeps_bound_alternate_source_membership_explicit(
    alternate_retained_book_calendar, tmp_path,
):
    records, root, _, _, _ = alternate_retained_book_calendar
    output = tmp_path / "acceptance"
    result = audit_book_calendar(records, root, output, "fixture")
    assert result["status"] == "COMPLETED"
    first = json.loads((output / "calendar.jsonl").read_text().splitlines()[0])
    assert first["source_clock_membership_missing"] == 2
    binding = first["source_clock_binding"]
    assert binding["canonical_source_membership"] == "DIFFERENT_HASH_BOUND_RETAINED_SOURCE"
    assert binding["canonical_clock_membership_missing"] == 2
    assert binding["retained_original_source_sha256"] != binding["canonical_original_source_sha256"]
    assert first["grid"]["future_fill_violations"] == 0
    assert first["research_use"]["locked"] is True
    quality = json.loads(Path(first["quality_path"]).read_text())
    assert quality["raw_reconstruction"] == "RETAINED_NOT_REBUILT"
    assert quality["provider_normalized_replay_candidate"] is False
    assert quality["exact_queue"] is False


@pytest.mark.parametrize("defect", ["same_source", "missing_origin_sha", "missing_canonical_origin_sha",
                                     "bad_clock_hash", "bad_bbo_hash", "bad_l2_hash"])
def test_alternate_source_exception_requires_identified_bound_triplet(
    alternate_retained_book_calendar, tmp_path, defect,
):
    records, root, _, quality_path, original_sha = alternate_retained_book_calendar
    if defect == "same_source":
        records[0]["channels"][0]["source_content_validation"]["source_sha256"] = original_sha
    elif defect == "missing_canonical_origin_sha":
        del records[0]["channels"][0]["source_content_validation"]["source_sha256"]
    else:
        quality = json.loads(quality_path.read_text())
        if defect == "missing_origin_sha":
            del quality["raw_inputs"]["incremental_book_L2"]["sha256"]
        else:
            kind = defect.removeprefix("bad_").removesuffix("_hash")
            quality[kind + "_output"]["sha256"] = "0" * 64
        quality_path.write_text(json.dumps(quality))
    with pytest.raises(ValueError):
        audit_book_calendar(records, root, tmp_path / "acceptance", "fixture")


@pytest.mark.parametrize("changed", ["input_manifest", "selection_manifest", "calendar"])
def test_book_calendar_validator_upgrade_cannot_change_frozen_inputs(
    book_calendar, tmp_path, changed,
):
    records, root, _ = book_calendar
    output = tmp_path / "acceptance"
    audit_book_calendar(records, root, output, "fixture")
    state_path = output / "state.json"
    state = json.loads(state_path.read_text())
    state["identity"]["reader_source_sha256"] = "0" * 64
    state_path.write_text(json.dumps(state))
    digest = "fixture"
    if changed == "input_manifest":
        digest = "changed"
    elif changed == "selection_manifest":
        path = root / "manifest.json"
        value = json.loads(path.read_text())
        value["selection_changed"] = True
        path.write_text(json.dumps(value))
    else:
        records = records[:-1]
    saved_rows = (output / "calendar.jsonl").read_bytes()
    with pytest.raises(ValueError, match="resume identity changed"):
        audit_book_calendar(records, root, output, digest, resume=True)
    assert (output / "calendar.jsonl").read_bytes() == saved_rows


def test_book_calendar_validator_upgrade_records_prior_execution_without_rewriting_prefix(
    book_calendar, tmp_path,
):
    records, root, _ = book_calendar
    output = tmp_path / "acceptance"
    audit_book_calendar(records, root, output, "fixture")
    state_path = output / "state.json"
    state = json.loads(state_path.read_text())
    state["identity"]["reader_source_sha256"] = "0" * 64
    state["status"] = "FAILED"
    state["error"] = "synthetic older validator limitation"
    state_path.write_text(json.dumps(state))
    saved_rows = (output / "calendar.jsonl").read_bytes()
    result = audit_book_calendar(records, root, output, "fixture", resume=True)
    assert result["status"] == "COMPLETED"
    assert (output / "calendar.jsonl").read_bytes() == saved_rows
    assert result["execution_history"][-1] == {
        "reader_source_sha256": "0" * 64, "completed_prefix_days": 2,
        "prior_status": "FAILED", "prior_error": "synthetic older validator limitation",
        "prefix_revalidated_by_current_code": True,
    }
