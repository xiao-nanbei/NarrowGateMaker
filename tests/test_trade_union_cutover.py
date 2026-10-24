from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import copy
import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.daily_raw import TRADE_SCHEMA, sha256_file
from data.trade_aggregation import prepare_day
from data import trade_union_cutover as cutover


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


@pytest.fixture
def setup(tmp_path):
    root, staged, state = tmp_path / "market", tmp_path / "staged", tmp_path / "state"
    days = ["2026-01-01", "2026-01-02"]
    records, acquisitions, hours = [], [], []
    for index, day in enumerate(days):
        start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        folder = root / "raw/binance_futures/BTCUSDC" / day
        folder.mkdir(parents=True)
        table = pa.Table.from_pylist([{
            "exchange": "binance-futures", "symbol": "BTCUSDC", "timestamp": (start + 50) * 1000,
            "local_timestamp": None, "id": index + 10, "side": "buy", "price": "100.0",
            "amount": "0.001", "quote_qty": "0.1", "time": start + 50, "qty": "0.001",
            "is_buyer_maker": False,
        }], schema=TRADE_SCHEMA)
        pq.write_table(table, folder / "trades.parquet")
        for channel in ("incremental_book_L2", "trades", "aggTrades", "funding"):
            path = folder / f"{channel}.parquet"
            if channel != "trades":
                path.write_bytes(f"retained {day} {channel}".encode())
            records.append({"day": day, "symbol": "BTCUSDC", "channel": channel,
                            "path": str(path), "final_path": str(path), "sha256": sha256_file(path),
                            "rows": 1, "source": "historical identity"})
    unrelated = root / "raw/binance_futures/BTCUSDT/2026-01-01/aggTrades.parquet"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"reference native input must survive")
    records.append({"day": days[0], "symbol": "BTCUSDT", "channel": "aggTrades",
                    "path": str(unrelated), "sha256": sha256_file(unrelated)})
    raw_index = root / "raw/daily-index.json"
    write_json(raw_index, {"schema": "narrowgate_daily_raw_v1", "records": records,
                          "channels_per_symbol": {"BTCUSDC": 4, "BTCUSDT": 1},
                          "calendar_days": 2, "private_custom_field": "preserved"})
    all_days = days + [(date.fromisoformat(days[-1]) + timedelta(days=1)).isoformat()]
    for index, day in enumerate(all_days):
        start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        for hour in range(1 if index == 2 else 24):
            path = tmp_path / "source" / day / f"{hour:02}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({"trade_id": [index + 10], "price": ["100.0"],
                "quantity": ["0.001"], "trade_time": [start + 50], "is_buyer_maker": [False],
                "symbol": ["BTCUSDC"], "received_time": [(start + 51) * 1_000_000]}), path)
            acquisitions.append({"day": day, "hour": f"{hour:02}", "channel": "trades",
                                 "status": "READABLE", "path": str(path), "sha256": sha256_file(path), "rows": 1})
            hours.append([day, f"{hour:02}"])
    acquisition_plan = tmp_path / "acquisition-plan.json"
    write_json(acquisition_plan, {"configuration": {"start": days[0], "end": days[-1],
        "symbol": "BTCUSDC", "channel": "trades", "next_day_first_hour": True}, "hours": hours})
    objects = tmp_path / "objects.jsonl"
    objects.write_text("".join(json.dumps(row) + "\n" for row in acquisitions))
    preparation = []
    for day in days:
        neighbors = {(date.fromisoformat(day) + timedelta(days=n)).isoformat() for n in (-1, 0, 1)}
        sources = [row for row in acquisitions if row["day"] in neighbors]
        prepare_day(root / "raw/binance_futures/BTCUSDC" / day / "trades.parquet", sources, day, staged / day)
        code = {name: sha256_file(Path(cutover.__file__).resolve().parents[1] / name)
                for name in ("data/trade_aggregation.py", "data/daily_raw.py")}
        preparation.append({"day": day, "status": "PREPARED_VERIFIED", "source_availability": "ALL_READABLE",
                            "receipt_sha256": sha256_file(staged / day / "receipt.json"), "code_identity": code})
    prepared = tmp_path / "prepared.jsonl"
    prepared.write_text("".join(json.dumps(row) + "\n" for row in preparation))
    history = root / "derived/baseline401_input_view/bars_1s/untouched.parquet"
    history.parent.mkdir(parents=True)
    history.write_bytes(b"frozen historical bars")
    return {"root": root, "staged": staged, "state": state, "days": days,
            "plan": acquisition_plan, "objects": objects, "prepared": prepared,
            "native": [root / "raw/binance_futures/BTCUSDC" / day / "aggTrades.parquet" for day in days],
            "reference": unrelated, "history": history}


def prepare(value):
    return cutover.prepare_cutover(value["root"], value["staged"], value["plan"], value["objects"],
        value["prepared"], value["state"], start_day=value["days"][0], end_day=value["days"][-1])


def test_prepare_publish_and_explicit_retirement_are_separate_and_scoped(setup):
    before = sha256_file(setup["root"] / "raw/daily-index.json")
    plan = prepare(setup)
    assert plan["source_objects"] == 49
    assert plan["cross_day_native_id_check"]["status"] == "UNIQUE_FULL_CALENDAR"
    assert sha256_file(setup["root"] / "raw/daily-index.json") == before
    assert all(path.exists() for path in setup["native"])
    with pytest.raises(ValueError, match="all calendar publication"):
        cutover.retire_native(setup["state"], explicit=True)
    result = cutover.publish_cutover(setup["state"])
    assert result["phase"] == "PUBLISHED"
    assert all(path.exists() for path in setup["native"])
    index = cutover._read(setup["root"] / "raw/daily-index.json")
    assert index["channels_per_symbol"] == {"BTCUSDC": 3, "BTCUSDT": 1}
    assert index["private_custom_field"] == "preserved"
    assert len(index["records"]) == 7
    derived = cutover._read(setup["root"] / "derived/trade_aggregate_index.json")
    assert derived["native_aggtrade_identity"] is False
    assert derived["execution_event_authority"] is False
    assert all(row["individual_count_conserved"] for row in derived["records"])
    assert cutover.publish_cutover(setup["state"])["phase"] == "PUBLISHED"
    with pytest.raises(ValueError, match="explicit authorization"):
        cutover.retire_native(setup["state"])
    assert cutover.retire_native(setup["state"], explicit=True)["phase"] == "RETIRED"
    assert not any(path.exists() for path in setup["native"])
    retired = cutover._read(setup["state"] / "retired-native-index.json")
    assert retired["status"] == "RETIRED" and retired["retired_files"] == 2
    assert len(retired["records"]) == 2
    assert setup["reference"].read_bytes() == b"reference native input must survive"
    assert setup["history"].read_bytes() == b"frozen historical bars"
    assert cutover.retire_native(setup["state"], explicit=True)["phase"] == "RETIRED"


@pytest.mark.parametrize("problem", ["missing", "failed", "partial_preparation", "bad_source_hash", "staged_changed"])
def test_incomplete_or_changed_inputs_never_prepare_publication(setup, problem):
    rows = [json.loads(line) for line in setup["objects"].read_text().splitlines()]
    if problem == "missing":
        rows.pop()
    elif problem == "failed":
        rows[-1]["status"] = "NOT_FOUND"
    elif problem == "bad_source_hash":
        Path(rows[-1]["path"]).write_bytes(b"source changed")
    elif problem == "partial_preparation":
        prepared = [json.loads(line) for line in setup["prepared"].read_text().splitlines()]
        prepared[-1]["source_availability"] = "PARTIAL_OR_UNKNOWN"
        setup["prepared"].write_text("".join(json.dumps(row) + "\n" for row in prepared))
    else:
        (setup["staged"] / setup["days"][-1] / "trades.parquet").write_bytes(b"staged changed")
    setup["objects"].write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError):
        prepare(setup)
    assert not (setup["state"] / "plan.json").exists()
    assert all(path.exists() for path in setup["native"])


def test_mid_publish_failure_resumes_without_retiring_any_native(setup, monkeypatch):
    prepare(setup)
    original = cutover._copy_publish
    failed = False

    def fail_once(source, destination, expected, old):
        nonlocal failed
        if destination.name == cutover.AGGREGATE and not failed:
            failed = True
            raise OSError("simulated interrupted copy")
        return original(source, destination, expected, old)

    monkeypatch.setattr(cutover, "_copy_publish", fail_once)
    with pytest.raises(OSError, match="interrupted"):
        cutover.publish_cutover(setup["state"])
    assert all(path.exists() for path in setup["native"])
    assert cutover._read(setup["state"] / "journal.json")["phase"] == "PUBLISHING"
    assert cutover.publish_cutover(setup["state"])["phase"] == "PUBLISHED"


def old_bar_pair(setup):
    bar = setup["root"] / "derived/bars_1s" / f"BTCUSDC-1s-{setup['days'][0]}.parquet"
    bar.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"trade_count": [7]}), bar)
    meta = bar.with_suffix(".parquet.meta.json")
    write_json(meta, {"prior_source": "historical native bars"})
    return bar, meta


def test_bar_copy_then_metadata_crash_resumes_from_frozen_pair(setup, monkeypatch):
    bar, meta = old_bar_pair(setup)
    old_meta = meta.read_bytes()
    prepare(setup)
    original, failed = cutover._copy_publish, False

    def crash(source, destination, expected, old):
        nonlocal failed
        if destination == meta and not failed:
            failed = True
            assert pq.read_table(bar)["trade_count"].to_pylist() == [1]
            assert meta.read_bytes() == old_meta
            raise OSError("simulated crash between bar and metadata publication")
        return original(source, destination, expected, old)

    monkeypatch.setattr(cutover, "_copy_publish", crash)
    with pytest.raises(OSError, match="between bar and metadata"):
        cutover.publish_cutover(setup["state"])
    journal = cutover._read(setup["state"] / "journal.json")
    assert journal["days"][setup["days"][0]]["state"] == "BAR_PUBLISH_INTENT"
    frozen_meta = journal["days"][setup["days"][0]]["bar_meta_sha256"]
    assert all(path.exists() for path in setup["native"])
    assert cutover.publish_cutover(setup["state"])["phase"] == "PUBLISHED"
    assert sha256_file(meta) == frozen_meta
    primary = setup["root"] / "raw/binance_futures/BTCUSDC" / setup["days"][0] / "trades.parquet"
    assert cutover._read(meta)["source_path"] == str(primary.resolve())


def test_scratch_bar_without_metadata_is_rebuilt_before_any_canonical_bar_publish(setup, monkeypatch):
    from features import preprocess

    bar, meta = old_bar_pair(setup)
    old = bar.read_bytes(), meta.read_bytes()
    prepare(setup)
    original, failed = preprocess._write_bar_metadata, False

    def crash(*args, **kwargs):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("simulated incomplete scratch pair")
        return original(*args, **kwargs)

    monkeypatch.setattr(preprocess, "_write_bar_metadata", crash)
    with pytest.raises(OSError, match="incomplete scratch"):
        cutover.publish_cutover(setup["state"])
    assert (bar.read_bytes(), meta.read_bytes()) == old
    assert cutover.publish_cutover(setup["state"])["phase"] == "PUBLISHED"


@pytest.mark.parametrize("changed", ["bar", "metadata"])
def test_unknown_current_bar_or_metadata_change_is_not_overwritten(setup, changed):
    bar, meta = old_bar_pair(setup)
    prepare(setup)
    target = bar if changed == "bar" else meta
    target.write_bytes(b"independent user bytes after preflight")
    primary = setup["root"] / "raw/binance_futures/BTCUSDC" / setup["days"][0] / "trades.parquet"
    raw_before = sha256_file(primary)
    with pytest.raises(ValueError, match="unplanned destination bytes"):
        cutover.publish_cutover(setup["state"])
    assert target.read_bytes() == b"independent user bytes after preflight"
    assert sha256_file(primary) == raw_before
    assert all(path.exists() for path in setup["native"])


def test_index_publication_crash_recovers_after_raw_index_replace(setup, monkeypatch):
    prepare(setup)
    original = cutover._write
    failed = False

    def crash(path, payload, **kwargs):
        nonlocal failed
        if path.name == "journal.json" and payload.get("phase") == "PUBLISHED" and not failed:
            failed = True
            raise OSError("simulated crash after index replace")
        return original(path, payload, **kwargs)

    monkeypatch.setattr(cutover, "_write", crash)
    with pytest.raises(OSError, match="index replace"):
        cutover.publish_cutover(setup["state"])
    assert cutover._read(setup["state"] / "journal.json")["phase"] == "INDEX_PUBLISH_INTENT"
    assert cutover.publish_cutover(setup["state"])["phase"] == "PUBLISHED"


def test_retirement_preflight_checks_last_day_before_deleting_first(setup):
    prepare(setup)
    cutover.publish_cutover(setup["state"])
    setup["native"][-1].write_bytes(b"unplanned native version")
    with pytest.raises(ValueError, match="identity differs"):
        cutover.retire_native(setup["state"], explicit=True)
    assert all(path.exists() for path in setup["native"])


def test_retirement_rejects_changed_bar_before_deleting_native(setup):
    prepare(setup)
    result = cutover.publish_cutover(setup["state"])
    Path(result["days"][setup["days"][-1]]["bar_path"]).write_bytes(b"broken bar")
    with pytest.raises(ValueError, match="identity differs"):
        cutover.retire_native(setup["state"], explicit=True)
    assert all(path.exists() for path in setup["native"])


def test_retired_native_reappearing_with_same_hash_is_not_deleted_again(setup):
    original = setup["native"][0].read_bytes()
    prepare(setup)
    cutover.publish_cutover(setup["state"])
    cutover.retire_native(setup["state"], explicit=True)
    setup["native"][0].write_bytes(original)
    journal_before = (setup["state"] / "journal.json").read_bytes()
    with pytest.raises(ValueError, match="reappeared"):
        cutover.retire_native(setup["state"], explicit=True)
    assert setup["native"][0].read_bytes() == original
    assert (setup["state"] / "journal.json").read_bytes() == journal_before
    assert not setup["native"][1].exists()


def test_changed_original_index_snapshot_is_not_silently_published(setup):
    prepare(setup)
    backup = setup["state"] / "raw-index-before.json"
    data = cutover._read(backup)
    data["records"] = []
    write_json(backup, data)
    with pytest.raises(ValueError, match="identity differs"):
        cutover.publish_cutover(setup["state"])
    assert all(path.exists() for path in setup["native"])


def test_retirement_resumes_recorded_unlink_before_completion_write(setup, monkeypatch):
    prepare(setup)
    cutover.publish_cutover(setup["state"])
    original = cutover._sync_directory
    failed = False

    def fail_after_unlink(path):
        nonlocal failed
        if path == setup["native"][0].parent and not failed:
            failed = True
            raise OSError("simulated crash after unlink")
        return original(path)

    monkeypatch.setattr(cutover, "_sync_directory", fail_after_unlink)
    with pytest.raises(OSError, match="after unlink"):
        cutover.retire_native(setup["state"], explicit=True)
    assert not setup["native"][0].exists() and setup["native"][1].exists()
    journal = cutover._read(setup["state"] / "journal.json")
    assert journal["native_retirement"][setup["days"][0]] == "DELETE_INTENT"
    assert cutover.retire_native(setup["state"], explicit=True)["phase"] == "RETIRED"


def test_cross_day_range_overlap_uses_exact_membership_not_range_only(tmp_path):
    days = ["2026-01-01", "2026-01-02", "2026-01-03"]
    for day, values in zip(days, ([1, 5], [2, 3], [10]), strict=True):
        path = tmp_path / day / "trades.parquet"
        path.parent.mkdir()
        start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        pq.write_table(pa.table({"id": values, "time": [start] * len(values)}), path)
    result = cutover._verify_calendar_ids(tmp_path, days)
    assert result["exact_overlapping_range_checks"] == 1
    pq.write_table(pa.table({"id": [1, 10], "time": [start, start]}), tmp_path / days[2] / "trades.parquet")
    with pytest.raises(ValueError, match="multiple UTC days"):
        cutover._verify_calendar_ids(tmp_path, days)


@pytest.mark.parametrize("offsets, match", [
    ([1500, 1499], "not nondecreasing"),
    ([500, 86400000], "outside its UTC day"),
    ([-1, 500], "outside its UTC day"),
])
def test_individual_time_order_and_utc_bounds_are_actual_bar_preconditions(tmp_path, offsets, match):
    day = "2026-01-01"
    folder = tmp_path / day
    folder.mkdir()
    start = 1767225600000
    pq.write_table(pa.table({"id": [1, 2], "time": [start + value for value in offsets]}),
                   folder / "trades.parquet")
    with pytest.raises(ValueError, match=match):
        cutover._verify_calendar_ids(tmp_path, [day])


@pytest.mark.parametrize("field, value, match", [
    ("feature_ready_ts_ms", 1767225600049, "future-fill"),
    ("trade_count", 0, "every individual execution"),
])
def test_actual_aggregate_content_is_checked_not_just_receipt_claim(setup, field, value, match):
    folder = setup["staged"] / setup["days"][0]
    path = folder / cutover.AGGREGATE
    table = pq.read_table(path)
    index = table.column_names.index(field)
    table = table.set_column(index, field, pa.array([value], type=pa.int64()))
    pq.write_table(table, path)
    receipt = cutover._read(folder / "receipt.json")
    receipt["output_sha256"][cutover.AGGREGATE] = sha256_file(path)
    with pytest.raises(ValueError, match=match):
        cutover._verify_staged(folder, receipt)


@pytest.fixture
def incremental(setup):
    prepare(setup)
    cutover.publish_cutover(setup["state"])
    cutover.retire_native(setup["state"], explicit=True)
    root, day = setup["root"], setup["days"][0]
    raw_path, aggregate_path = root / "raw/daily-index.json", root / "derived/trade_aggregate_index.json"
    raw, aggregate = cutover._read(raw_path), cutover._read(aggregate_path)
    for row in aggregate["records"]:
        target = Path(row["path"]).with_suffix(".receipt.json")
        target.write_bytes(Path(row["source_receipt"]).read_bytes())
        row["source_receipt"], row["source_receipt_sha256"] = str(target), sha256_file(target)
        trade = next(x for x in raw["records"] if x.get("day") == row["day"] and x.get("symbol") == "BTCUSDC"
                     and x.get("channel") == "trades")
        trade.update(source_receipt=str(target), source_receipt_sha256=sha256_file(target))
    write_json(raw_path, raw)
    write_json(aggregate_path, aggregate)
    catalog = setup["root"] / "catalog"
    catalog.mkdir()
    primary = root / "raw/binance_futures/BTCUSDC" / day / "trades.parquet"
    group = root / "derived/binance_futures/BTCUSDC" / day / cutover.AGGREGATE
    bar = root / "derived/bars_1s" / f"BTCUSDC-1s-{day}.parquet"
    feature = root / "derived/features" / f"features_{day}.parquet"
    feature.parent.mkdir()
    pq.write_table(pa.table({"feature": [1.]}), feature)
    datasets, channels = [], []
    descriptions = [(cutover.RAW_DATASET, "individual_trades", primary),
                    (cutover.AGGREGATE_DATASET, "synthetic_trade_aggregates_100ms", group),
                    ("btcusdc-selected-bars401", "bars_1s", bar),
                    ("btcusdc-baseline401-selected-features", "model_features", feature)]
    for key, kind, path in descriptions:
        audit = catalog / f"{key}.csv"
        with audit.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["day", "rows", "sha256", "source_sha256", "etl_identity_verified"])
            writer.writeheader()
            writer.writerow(dict(day=day, rows=pq.ParquetFile(path).metadata.num_rows,
                                 sha256=sha256_file(path), source_sha256=sha256_file(primary), etl_identity_verified=True))
        file = {"path": str(path), "sha256": sha256_file(path), "size_bytes": path.stat().st_size,
                "rows": pq.ParquetFile(path).metadata.num_rows}
        datasets.append({"id": key, "symbol": "BTCUSDC", "data_type": kind,
            "audit": {"path": str(audit), "check_column": "etl_identity_verified", "sha256": sha256_file(audit)},
            "inventories": [{"node": "local", "files_by_day": {day: [copy.deepcopy(file)]}}],
            "historical_audit": {"old_trade_sha256": sha256_file(primary)}})
        channels.append({"source_id": key, "channel": kind, "symbol": "BTCUSDC",
            "files": [copy.deepcopy(file)], "quality": {"path": str(audit), "sha256": sha256_file(audit)},
            "historical_audit": {"old_trade_sha256": sha256_file(primary)},
            "model_input_verification": {"old_trade_sha256": sha256_file(primary)}})
    write_json(catalog / "owner-manifest.json", {"datasets": datasets, "start_day": setup["days"][0], "end_day": setup["days"][-1]})
    write_json(catalog / "readability.json", {"records": [{"calendar_date": day, "channels": channels,
        "research_use": {"status": "Development", "previously_consumed": True, "holdout": "NOT_GRANTED"}}]})
    source = setup["root"] / "secondary.csv"
    source.write_text("exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
                      "binance-futures,BTCUSDC,1767225600050000,1767225600050100,10,buy,100.0,0.001\n")
    manifest = setup["root"] / "secondary.jsonl"
    manifest.write_text(json.dumps({"day": day, "channel": "trades", "symbol": "BTCUSDC",
        "exchange": "binance-futures", "path": str(source), "sha256": sha256_file(source)}) + "\n")
    return {**setup, "catalog": catalog, "source": source, "manifest": manifest,
            "incremental_state": setup["root"] / "incremental", "primary": primary, "aggregate": group,
            "bar": bar, "feature": feature}


def prepare_incremental(value):
    return cutover.prepare_incremental_day(value["root"], value["manifest"], value["incremental_state"],
        day=value["days"][0], catalog_root=value["catalog"])


def add_secondary_trade(value, *, identifier=100, timestamp=1767225600060000):
    with value["source"].open("a") as handle:
        handle.write(f"binance-futures,BTCUSDC,{timestamp},{timestamp+100},{identifier},buy,100.0,0.001\n")
    row = json.loads(value["manifest"].read_text())
    row["sha256"] = sha256_file(value["source"])
    value["manifest"].write_text(json.dumps(row)+"\n")


def test_incremental_no_change_preserves_all_current_artifact_and_catalog_bytes(incremental):
    value = incremental
    paths = [value[k] for k in ("primary", "aggregate", "bar", "feature")]
    paths += list(value["catalog"].glob("*"))
    paths += [value["root"] / "raw/daily-index.json", value["root"] / "derived/trade_aggregate_index.json"]
    before = {p: p.read_bytes() for p in paths}
    result = prepare_incremental(value)
    assert result["content_changed"] is False and result["status"] == "NO_CHANGE"
    assert not (value["incremental_state"] / "publication.json").exists()
    assert cutover.publish_incremental_day(value["incremental_state"])["current_files_replaced"] == 0
    assert {p: p.read_bytes() for p in paths} == before
    assert prepare_incremental(value) == result


def test_incremental_changes_only_trade_pair_and_current_metadata_marks_dependents(incremental):
    from data.daily_schema_cutover import usage_digest
    value = incremental
    add_secondary_trade(value)
    before_raw = value["primary"].read_bytes()
    unchanged = {value[k]: value[k].read_bytes() for k in ("bar", "feature", "reference")}
    old_sha = sha256_file(value["primary"])
    rights = usage_digest(cutover._read(value["catalog"] / "readability.json"))
    prepared = prepare_incremental(value)
    assert prepared["content_changed"] is True
    assert value["primary"].read_bytes() == before_raw  # Preparation never publishes.
    result = cutover.publish_incremental_day(value["incremental_state"])
    assert result["status"] == "PUBLISHED_VERIFIED"
    assert pq.read_table(value["primary"])["id"].to_pylist() == [10, 100]
    assert sum(pq.read_table(value["aggregate"])["trade_count"].to_pylist()) == 2
    assert {p: p.read_bytes() for p in unchanged} == unchanged
    assert all(not p.exists() for p in value["native"])
    readability = cutover._read(value["catalog"] / "readability.json")
    assert usage_digest(readability) == rights
    feature = next(c for c in readability["records"][0]["channels"] if c["channel"] == "model_features")
    assert feature["trade_dependency_status"] == "REFRESH_REQUIRED"
    assert feature["files"][0]["sha256"] == sha256_file(value["feature"])
    assert feature["historical_audit"]["old_trade_sha256"] == old_sha
    assert feature["model_input_verification"]["old_trade_sha256"] == old_sha
    index = cutover._read(value["root"] / "derived/trade_aggregate_index.json")
    row = next(r for r in index["records"] if r["day"] == value["days"][0])
    assert row["dependency_refresh"]["taker_tempo"] == "REFRESH_REQUIRED"
    assert row["dependency_refresh"]["labels"] == "REBUILD_REQUIRED"
    assert row["dependency_refresh"]["trained_models"] == "RETRAIN_REQUIRED"
    assert row["dependency_refresh"]["reuse_old_labels_or_models"] is False
    assert row["individual_source_sha256"] == sha256_file(value["primary"])
    assert cutover.publish_incremental_day(value["incremental_state"])["status"] == "PUBLISHED_VERIFIED"


def test_incremental_mid_transaction_resume_uses_existing_publisher(incremental, monkeypatch):
    import data.daily_schema_cutover as transactions
    value = incremental
    add_secondary_trade(value)
    prepare_incremental(value)
    original = transactions.os.replace
    failed = False
    def interrupt(source, target):
        nonlocal failed
        if Path(target) == value["aggregate"] and not failed:
            failed = True
            raise OSError("interrupted between raw and aggregate")
        return original(source, target)
    monkeypatch.setattr(transactions.os, "replace", interrupt)
    with pytest.raises(OSError, match="interrupted"):
        cutover.publish_incremental_day(value["incremental_state"])
    assert cutover.publish_incremental_day(value["incremental_state"])["status"] == "PUBLISHED_VERIFIED"


@pytest.mark.parametrize("problem", ["source", "aggregate", "rights", "adjacent"])
def test_incremental_publish_refuses_drift_before_any_replace(incremental, problem):
    value = incremental
    add_secondary_trade(value)
    prepare_incremental(value)
    before = value["primary"].read_bytes()
    if problem == "source":
        value["source"].write_text("changed capture")
    elif problem == "aggregate":
        value["aggregate"].write_bytes(b"unplanned replacement")
    elif problem == "adjacent":
        (value["primary"].parent.parent / value["days"][1] / "trades.parquet").write_bytes(b"changed adjacent owner")
    else:
        path = value["catalog"] / "readability.json"
        data = cutover._read(path)
        data["records"][0]["research_use"]["holdout"] = "CHANGED"
        write_json(path, data)
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        cutover.publish_incremental_day(value["incremental_state"])
    assert value["primary"].read_bytes() == before


def test_incremental_neighbor_id_owner_prevents_cross_midnight_duplicate(incremental):
    value = incremental
    add_secondary_trade(value, identifier=11)
    result = prepare_incremental(value)
    assert result["stats"]["adjacent_primary_ids_retained_on_owner_day"] == 1
    assert result["content_changed"] is False


@pytest.mark.parametrize("field,value", [("channel", "aggTrades"), ("symbol", "BTCUSDT"),
                                        ("exchange", "binance"), ("day", "2026-02-01")])
def test_incremental_manifest_cannot_restore_native_or_change_market_window(incremental, field, value):
    row = json.loads(incremental["manifest"].read_text())
    row[field] = value
    incremental["manifest"].write_text(json.dumps(row)+"\n")
    before = incremental["primary"].read_bytes()
    with pytest.raises(ValueError, match="same-market"):
        prepare_incremental(incremental)
    assert incremental["primary"].read_bytes() == before


def test_incremental_second_independent_import_of_same_capture_is_a_noop(incremental):
    value = incremental
    add_secondary_trade(value)
    prepare_incremental(value)
    cutover.publish_incremental_day(value["incremental_state"])
    before = {p: p.read_bytes() for p in [value["primary"], value["aggregate"],
        value["catalog"] / "owner-manifest.json", value["catalog"] / "readability.json"]}
    second = {**value, "incremental_state": value["root"] / "another-incremental"}
    result = prepare_incremental(second)
    assert result["status"] == "NO_CHANGE"
    assert cutover.publish_incremental_day(second["incremental_state"])["current_files_replaced"] == 0
    assert {p: p.read_bytes() for p in before} == before


def test_incremental_data_staging_and_catalog_staging_follow_target_filesystems(incremental):
    value = incremental
    add_secondary_trade(value)
    plan = prepare_incremental(value)
    assert Path(plan["prepared_receipt"]).is_relative_to(value["root"] / "derived/.trade-incremental")
    journal = cutover._read(Path(plan["publication_journal"]))
    for record in journal["files"]:
        staged, target = Path(record["staged"]), Path(record["after"]["path"])
        assert staged.stat().st_dev == target.parent.stat().st_dev
        if target.suffix == ".csv" or target.name in {"owner-manifest.json", "readability.json", "daily-index.json", "trade_aggregate_index.json"}:
            assert staged.parent == target.parent
