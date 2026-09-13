"""Explicit, recoverable publication of staged individual-trade unions.

Preparation, publication and native-aggregate retirement are separate operations.
This module never promotes synthetic groups into exchange-native parent packets,
never grants economic admission, and never touches reference-market channels.
"""
from __future__ import annotations

import argparse
from collections import Counter
import copy
from datetime import date, datetime, timedelta, timezone
import fcntl
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import uuid

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

from data.daily_raw import _reject_output_symlinks, sha256_file
from features.preprocess import process_file

SYMBOL = "BTCUSDC"
START_DAY, END_DAY = "2025-08-01", "2026-09-05"
AGGREGATE = "trade_aggregates_100ms.parquet"
RAW_DATASET = "btcusdc-raw-trades"
AGGREGATE_DATASET = "btcusdc-derived-trade-aggregates-100ms"
TRADE_DEPENDENCIES = {"bars_1s", "model_features", "taker_tempo", "trade_tempo"}


def _read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write(path: Path, payload: dict, *, exclusive: bool = False) -> None:
    _reject_output_symlinks(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if exclusive:
        with path.open("x", encoding="utf-8") as handle:
            os.chmod(path, 0o600)
            json.dump(payload, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _sync_directory(path.parent)
        return
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _days(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("calendar start exceeds end")
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def _digest(path: Path, expected: str) -> None:
    _reject_output_symlinks(path)
    if not path.is_file() or sha256_file(path) != expected:
        raise ValueError(f"file identity differs: {path}")


def _record_path(row: dict) -> Path:
    first = row.get("final_path") or row.get("path")
    if not first:
        raise ValueError("raw index record lacks a path")
    path = Path(first)
    if row.get("path") and row.get("final_path") and Path(row["path"]) != Path(row["final_path"]):
        raise ValueError("raw index path aliases differ")
    return path


def _locked(operation):
    @wraps(operation)
    def run(state_dir, *args, **kwargs):
        path = Path(state_dir) / "operation.lock"
        _reject_output_symlinks(path)
        with path.open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return operation(state_dir, *args, **kwargs)
    return run


def _source_rows(plan_path: Path, objects_path: Path, days: list[str]) -> dict:
    acquisition = _read(plan_path)
    config = acquisition["configuration"]
    if (config["start"], config["end"], config["symbol"], config["channel"]) != (
        days[0], days[-1], SYMBOL, "trades"
    ) or config.get("next_day_first_hour") is not True:
        raise ValueError("acquisition plan does not match the continuous calendar")
    expected = {(day, f"{hour:02}") for day in days for hour in range(24)}
    expected.add(((date.fromisoformat(days[-1]) + timedelta(days=1)).isoformat(), "00"))
    hours = [tuple(pair) for pair in acquisition["hours"]]
    if len(hours) != len(expected) or set(hours) != expected:
        raise ValueError("acquisition plan has missing or duplicate hours")
    rows = {}
    for line in objects_path.read_text().splitlines():
        row = json.loads(line)
        key = row["day"], f"{int(row['hour']):02}"
        if row.get("channel") != "trades" or key not in expected:
            raise ValueError("unexpected acquisition object")
        if (key in rows and rows[key].get("sha256") and row.get("sha256")
                and rows[key]["sha256"] != row["sha256"]):
            raise ValueError("conflicting acquisition receipt versions")
        rows[key] = row
    if set(rows) != expected:
        raise ValueError("not every acquisition hour is terminal")
    for row in rows.values():
        if not row.get("status", "").startswith("READABLE"):
            raise ValueError("incomplete source availability: every planned hour must be READABLE")
        _digest(Path(row["path"]), row["sha256"])
    return rows


def _verify_staged(folder: Path, receipt: dict) -> tuple[int, int]:
    if receipt.get("status") != "PREPARED_VERIFIED" or receipt.get("future_fill_violations") != 0:
        raise ValueError("unverified staged day")
    if receipt.get("native_aggtrade_identity") is not False or receipt.get("economic_admission") is not False:
        raise ValueError("staged evidence authority differs")
    if set(receipt["output_sha256"]) != {"trades.parquet", AGGREGATE}:
        raise ValueError("unexpected staged output set")
    for name, digest in receipt["output_sha256"].items():
        _digest(folder / name, digest)
    trades = pq.ParquetFile(folder / "trades.parquet").metadata.num_rows
    aggregate = pq.ParquetFile(folder / AGGREGATE)
    if trades != receipt["stats"]["primary_rows"] + receipt["stats"]["added_rows"]:
        raise ValueError("staged union row count differs")
    if aggregate.metadata.num_rows != receipt["aggregate_rows"]:
        raise ValueError("staged aggregate row count differs")
    metadata = aggregate.schema_arrow.metadata or {}
    if metadata.get(b"native_aggtrade_identity") != b"false" or metadata.get(b"execution_event_authority") != b"false":
        raise ValueError("synthetic aggregate metadata missing")
    count = 0
    columns = ["trade_count", "bucket_start_ms", "bucket_end_ms", "feature_ready_ts_ms",
               "first_event_ts_ms", "last_event_ts_ms"]
    for batch in aggregate.iter_batches(columns=columns):
        if any(batch.column(name).null_count for name in columns):
            raise ValueError("unknown aggregate clock or count")
        count += pc.sum(batch.column("trade_count")).as_py()
        start, end, ready = (batch.column(name) for name in columns[1:4])
        invalid = pc.or_(pc.not_equal(pc.subtract(end, start), 100), pc.not_equal(end, ready))
        invalid = pc.or_(invalid, pc.less(batch.column("first_event_ts_ms"), start))
        invalid = pc.or_(invalid, pc.greater_equal(batch.column("last_event_ts_ms"), ready))
        if pc.any(invalid).as_py():
            raise ValueError("aggregate future-fill or bucket boundary violation")
    if count != trades:
        raise ValueError("aggregate does not account for every individual execution")
    return trades, aggregate.metadata.num_rows


def _verify_calendar_ids(staged_root: Path, days: list[str]) -> dict:
    """Fast disjoint intervals; exact intersections for any overlapping ranges."""
    intervals = []
    intersections = 0
    for day in days:
        path = staged_root / day / "trades.parquet"
        table = pq.read_table(path, columns=["id", "time"])
        if table["id"].null_count or table["time"].null_count:
            raise ValueError(f"unknown native ID or exchange time in {day}")
        ids, times = table["id"].to_numpy(), table["time"].to_numpy()
        if not np.issubdtype(ids.dtype, np.integer) or not np.issubdtype(times.dtype, np.integer):
            raise ValueError(f"native ID and exchange time must be integers in {day}")
        start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 1000
        if np.any(times < start) or np.any(times >= start + 86_400_000):
            raise ValueError(f"individual exchange time is outside its UTC day: {day}")
        if np.any(times[1:] < times[:-1]):
            raise ValueError(f"individual exchange time is not nondecreasing: {day}")
        unique = np.unique(ids)
        if not len(ids) or len(unique) != len(ids):
            raise ValueError(f"duplicate or empty native trade IDs in {day}")
        low, high = int(unique[0]), int(unique[-1])
        for previous in intervals:
            if previous["maximum"] < low or previous["minimum"] > high:
                continue
            old = pq.read_table(staged_root / previous["day"] / "trades.parquet", columns=["id"])["id"].to_numpy()
            intersections += 1
            if np.intersect1d(unique, old).size:
                raise ValueError(f"native trade ID occurs on multiple UTC days: {previous['day']} / {day}")
        intervals.append({"day": day, "minimum": low, "maximum": high, "rows": len(ids)})
    return {"status": "UNIQUE_FULL_CALENDAR", "days": len(days),
            "exchange_time_order": "NONDECREASING", "outside_utc_day_rows": 0,
            "exact_overlapping_range_checks": intersections, "intervals": intervals}


def prepare_cutover(project_root: Path, staged_root: Path, acquisition_plan: Path,
                    acquisition_objects: Path, preparation_manifest: Path, state_dir: Path,
                    *, start_day: str = START_DAY, end_day: str = END_DAY) -> dict:
    """Freeze verified inputs and exact retirement targets; do not publish/delete."""
    for path in (project_root, staged_root, state_dir):
        _reject_output_symlinks(Path(path))
    project_root, staged_root, state_dir = map(lambda p: Path(p).absolute(), (project_root, staged_root, state_dir))
    if (state_dir / "plan.json").exists():
        raise FileExistsError("a cutover plan exists; use publish/retire to resume it")
    days = _days(start_day, end_day)
    source_rows = _source_rows(Path(acquisition_plan), Path(acquisition_objects), days)
    preparation = [json.loads(line) for line in Path(preparation_manifest).read_text().splitlines()]
    prepared = {row["day"]: row for row in preparation}
    if len(prepared) != len(preparation) or set(prepared) != set(days):
        raise ValueError("preparation manifest is not one complete record per calendar day")
    expected_code = {name: sha256_file(Path(__file__).resolve().parents[1] / name)
                     for name in ("data/trade_aggregation.py", "data/daily_raw.py")}
    if any(row.get("code_identity") != expected_code for row in prepared.values()):
        raise ValueError("prepared implementation identity differs from current verified implementation")
    index_path = project_root / "raw/daily-index.json"
    index_before = sha256_file(index_path)
    index = _read(index_path)
    wanted = [row for row in index["records"] if row.get("symbol") == SYMBOL and row.get("day") in days]
    keyed = {(row["day"], row["channel"]): row for row in wanted}
    if len(keyed) != len(wanted) or set(keyed) != {(d, c) for d in days for c in ("trades", "aggTrades", "funding", "incremental_book_L2")}:
        raise ValueError("current BTCUSDC index is not four channels per declared date")
    entries = []
    clock_conflicts, secondary_clock_conflicts = set(), set()
    for day in days:
        progress = prepared[day]
        if progress.get("status") != "PREPARED_VERIFIED" or progress.get("source_availability") != "ALL_READABLE":
            raise ValueError(f"day {day} has partial or failed source preparation")
        folder = staged_root / day
        _digest(folder / "receipt.json", progress["receipt_sha256"])
        receipt = _read(folder / "receipt.json")
        if receipt.get("day") != day:
            raise ValueError("receipt UTC date differs")
        neighbors = {(date.fromisoformat(day) + timedelta(days=n)).isoformat() for n in (-1, 0, 1)}
        expected_sources = [[source_rows[key]["path"], source_rows[key]["sha256"]]
                            for key in sorted(source_rows) if key[0] in neighbors]
        if receipt.get("sources") != expected_sources:
            raise ValueError("prepared sources differ from completed acquisition objects")
        primary, native = keyed[day, "trades"], keyed[day, "aggTrades"]
        for row in (primary, native):
            expected = project_root / "raw/binance_futures" / SYMBOL / day / f"{row['channel']}.parquet"
            if _record_path(row) != expected:
                raise ValueError("current raw path is not the declared canonical target")
            _digest(expected, row["sha256"])
        if receipt["primary_sha256"] != primary["sha256"]:
            raise ValueError("canonical primary changed after preparation")
        for neighbor, digest in receipt.get("adjacent_primary_sha256", {}).items():
            _digest(Path(neighbor), digest)
        rows, groups = _verify_staged(folder, receipt)
        clock_conflicts.update(row["id"] for row in receipt["stats"].get("time_conflicts", []))
        secondary_clock_conflicts.update(row["id"] for row in receipt["stats"].get("secondary_time_conflicts", []))
        bar = project_root / "derived/bars_1s" / f"{SYMBOL}-1s-{day}.parquet"
        bar_meta = bar.with_suffix(".parquet.meta.json")
        _reject_output_symlinks(bar)
        _reject_output_symlinks(bar_meta)
        target_aggregate = project_root / "derived/binance_futures" / SYMBOL / day / AGGREGATE
        if target_aggregate.exists():
            raise FileExistsError("synthetic destination already exists without this cutover plan")
        entries.append({"day": day, "receipt": str(folder / "receipt.json"),
                        "receipt_sha256": progress["receipt_sha256"], "rows": rows, "aggregate_rows": groups,
                        "old_trade_record": primary, "native_record": native,
                        "output_sha256": receipt["output_sha256"], "code_identity": progress["code_identity"],
                        "bar_before_sha256": sha256_file(bar) if bar.exists() else None,
                        "bar_meta_before_sha256": sha256_file(bar_meta) if bar_meta.exists() else None})
    id_check = _verify_calendar_ids(staged_root, days)
    _digest(index_path, index_before)
    derived_index = project_root / "derived/trade_aggregate_index.json"
    if derived_index.exists():
        raise FileExistsError("derived aggregate index already exists; use its original cutover plan")
    plan = {"schema": "narrowgate.trade_union_cutover.v1", "transaction": uuid.uuid4().hex,
            "created_at_utc": datetime.now(timezone.utc).isoformat(), "symbol": SYMBOL,
            "project_root": str(project_root), "staged_root": str(staged_root), "days": days,
            "raw_index_path": str(index_path), "raw_index_before_sha256": index_before,
            "derived_index_path": str(derived_index), "entries": entries,
            "source_objects": len(source_rows), "source_availability": "ALL_READABLE",
            "acquisition_objects_sha256": sha256_file(Path(acquisition_objects)),
            "preparation_manifest_sha256": sha256_file(Path(preparation_manifest)),
            "cross_day_native_id_check": id_check,
            "distinct_shared_clock_discrepancy_ids": len(clock_conflicts),
            "distinct_secondary_clock_discrepancy_ids": len(secondary_clock_conflicts),
            "native_parent_retirement_is_not_equivalent_conversion": True, "economic_admission": False}
    _write(state_dir / "raw-index-before.json", index, exclusive=True)
    plan["raw_index_snapshot_sha256"] = sha256_file(state_dir / "raw-index-before.json")
    _write(state_dir / "retired-native-index.json", {
        "status": "PLANNED_NOT_DELETED", "records": [entry["native_record"] for entry in entries],
        "historical_native_parent_availability_after_retirement": "UNAVAILABLE_REQUIRES_REACQUISITION",
    }, exclusive=True)
    _write(state_dir / "plan.json", plan, exclusive=True)
    _write(state_dir / "journal.json", {"plan_sha256": sha256_file(state_dir / "plan.json"),
        "phase": "PREPARED", "days": {}, "native_retirement": {}}, exclusive=True)
    return plan


def _load(state_dir: Path) -> tuple[dict, dict]:
    plan, journal = _read(state_dir / "plan.json"), _read(state_dir / "journal.json")
    _digest(state_dir / "plan.json", journal["plan_sha256"])
    _digest(state_dir / "raw-index-before.json", plan["raw_index_snapshot_sha256"])
    if plan["symbol"] != SYMBOL or plan["source_availability"] != "ALL_READABLE":
        raise ValueError("cutover scope differs")
    return plan, journal


def _destination_is_expected(destination: Path, expected: str | None, old: str | None) -> bool:
    _reject_output_symlinks(destination)
    if destination.exists():
        observed = sha256_file(destination)
        if observed == expected:
            return True
        if old is None or observed != old:
            raise ValueError(f"unplanned destination bytes: {destination}")
    elif old is not None:
        raise ValueError(f"original destination disappeared: {destination}")
    return False


def _copy_publish(source: Path, destination: Path, expected: str, old: str | None) -> None:
    _digest(source, expected)
    if _destination_is_expected(destination, expected, old):
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.cutover.", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as input_file:
            shutil.copyfileobj(input_file, output, length=8 * 1024 * 1024)
            output.flush()
            os.fsync(output.fileno())
        _digest(Path(temporary), expected)
        os.replace(temporary, destination)
        _sync_directory(destination.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _verify_bar(path: Path, trade_sha: str, count: int, *, source_path: Path | None = None) -> dict:
    metadata = _read(path.with_suffix(".parquet.meta.json"))
    if (metadata.get("source_data_type"), metadata.get("source_sha256"), metadata.get("source_rows"),
        metadata.get("trade_count_unit"), metadata.get("causal_visible_at")) != (
        "trades", trade_sha, count, "individual_execution", "t+1s"
    ):
        raise ValueError("bar provenance is not bound to individual executions")
    if source_path is not None and metadata.get("source_path") != str(source_path.resolve()):
        raise ValueError("bar source path does not identify the published individual tape")
    _digest(path, metadata["output_sha256"])
    observed = sum(pc.sum(batch.column("trade_count")).as_py() for batch in
                   pq.ParquetFile(path).iter_batches(columns=["trade_count"]))
    if observed != count:
        raise ValueError("bar counts do not account for every individual execution")
    return metadata


@_locked
def publish_cutover(state_dir: Path) -> dict:
    """Publish staged data and rebuild mutable bars; native originals stay intact."""
    state_dir = Path(state_dir)
    plan, journal = _load(state_dir)
    root = Path(plan["project_root"])
    if journal["phase"] in {"PUBLISHED", "RETIRING", "RETIRED"}:
        _verify_published(plan, journal)
        return journal
    if journal["phase"] == "INDEX_PUBLISH_INTENT":
        return _publish_indices(state_dir, plan, journal)
    _digest(Path(plan["raw_index_path"]), plan["raw_index_before_sha256"])
    journal["phase"] = "PUBLISHING"
    _write(state_dir / "journal.json", journal)
    for entry in plan["entries"]:
        day = entry["day"]
        folder = Path(plan["staged_root"]) / day
        _digest(Path(entry["receipt"]), entry["receipt_sha256"])
        primary = _record_path(entry["old_trade_record"])
        aggregate = root / "derived/binance_futures" / SYMBOL / day / AGGREGATE
        bars = root / "derived/bars_1s"
        bar = bars / f"{SYMBOL}-1s-{day}.parquet"
        bar_meta = bar.with_suffix(".parquet.meta.json")
        previous = journal["days"].get(day, {})
        # Check both files before changing this day's canonical inputs. Unknown
        # user bytes are not the same thing as an interrupted owned publication.
        _destination_is_expected(bar, previous.get("bar_sha256"), entry["bar_before_sha256"])
        _destination_is_expected(bar_meta, previous.get("bar_meta_sha256"), entry["bar_meta_before_sha256"])
        journal["days"][day] = {**previous, "state": "PUBLISH_INTENT"}
        _write(state_dir / "journal.json", journal)
        _copy_publish(folder / "trades.parquet", primary, entry["output_sha256"]["trades.parquet"], entry["old_trade_record"]["sha256"])
        _copy_publish(folder / AGGREGATE, aggregate, entry["output_sha256"][AGGREGATE], None)
        scratch = state_dir / "staged-bars" / day
        staged_bar = scratch / bar.name
        staged_meta = staged_bar.with_suffix(".parquet.meta.json")
        if previous.get("bar_sha256") is None:
            scratch.mkdir(parents=True, exist_ok=True)
            # A crash between scratch bar and metadata writes is harmless:
            # process_file rebuilds an incomplete/mismatched owned scratch pair.
            process_file(primary, SYMBOL, scratch, data_type="trades")
        else:
            _digest(staged_bar, previous["bar_sha256"])
            _digest(staged_meta, previous["bar_meta_sha256"])
        metadata = _verify_bar(staged_bar, entry["output_sha256"]["trades.parquet"], entry["rows"], source_path=primary)
        bar_state = {"state": "BAR_PUBLISH_INTENT", "bar_path": str(bar),
                     "bar_sha256": metadata["output_sha256"], "bar_rows": metadata["rows"],
                     "bar_meta_path": str(bar_meta), "bar_meta_sha256": sha256_file(staged_meta)}
        journal["days"][day] = bar_state
        _write(state_dir / "journal.json", journal)
        _destination_is_expected(bar, bar_state["bar_sha256"], entry["bar_before_sha256"])
        _destination_is_expected(bar_meta, bar_state["bar_meta_sha256"], entry["bar_meta_before_sha256"])
        _copy_publish(staged_bar, bar, bar_state["bar_sha256"], entry["bar_before_sha256"])
        _copy_publish(staged_meta, bar_meta, bar_state["bar_meta_sha256"], entry["bar_meta_before_sha256"])
        _verify_bar(bar, entry["output_sha256"]["trades.parquet"], entry["rows"], source_path=primary)
        journal["days"][day]["state"] = "PUBLISHED_VERIFIED"
        _write(state_dir / "journal.json", journal)
    index = _read(state_dir / "raw-index-before.json")
    changed = {entry["day"]: entry for entry in plan["entries"]}
    records = []
    for row in index["records"]:
        entry = changed.get(row.get("day")) if row.get("symbol") == SYMBOL else None
        if entry and row["channel"] == "aggTrades":
            continue
        if entry and row["channel"] == "trades":
            row = dict(row)
            source = Path(plan["staged_root"]) / entry["day"] / "trades.parquet"
            target = _record_path(row)
            stat = target.stat()
            source_stat = source.stat()
            row.update(sha256=entry["output_sha256"]["trades.parquet"], rows=entry["rows"],
                       source=str(source), source_sha256=entry["output_sha256"]["trades.parquet"],
                       source_kind="individual_trade_union", source_receipt=entry["receipt"],
                       source_receipt_sha256=entry["receipt_sha256"], mode="union_published",
                       output_identity=[stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns],
                       source_identity=[source_stat.st_dev, source_stat.st_ino, source_stat.st_size, source_stat.st_mtime_ns],
                       output_ctime_ns=stat.st_ctime_ns, same_inode=False)
        records.append(row)
    index["records"] = records
    index["channels_per_symbol"][SYMBOL] = 3
    index["trade_union_transaction"] = plan["transaction"]
    derived = {"schema": "narrowgate.trade_aggregate_index.v1", "transaction": plan["transaction"],
               "native_aggtrade_identity": False, "execution_event_authority": False,
               "economic_admission": False, "calendar_start": plan["days"][0], "calendar_end": plan["days"][-1],
               "records": [{"day": entry["day"], "symbol": SYMBOL, "channel": "trade_aggregates_100ms",
                            "path": str(root / "derived/binance_futures" / SYMBOL / entry["day"] / AGGREGATE),
                            "sha256": entry["output_sha256"][AGGREGATE], "rows": entry["aggregate_rows"],
                            "individual_source_sha256": entry["output_sha256"]["trades.parquet"],
                            "individual_source_path": str(_record_path(entry["old_trade_record"])),
                            "individual_rows": entry["rows"],
                            "bar_path": journal["days"][entry["day"]]["bar_path"],
                            "bar_sha256": journal["days"][entry["day"]]["bar_sha256"],
                            "bar_rows": journal["days"][entry["day"]]["bar_rows"],
                            "bar_meta_path": journal["days"][entry["day"]]["bar_meta_path"],
                            "bar_meta_sha256": journal["days"][entry["day"]]["bar_meta_sha256"],
                            "source_receipt": entry["receipt"], "source_receipt_sha256": entry["receipt_sha256"],
                            "individual_count_conserved": True, "future_fill_violations": 0,
                            "source_availability": "ALL_READABLE",
                            "feature_ready_clock": "bucket_end_ms_not_receive_time"} for entry in plan["entries"]]}
    _write(state_dir / "raw-index-after.json", index)
    _write(state_dir / "derived-index-after.json", derived)
    journal.update(phase="INDEX_PUBLISH_INTENT", raw_index_sha256=sha256_file(state_dir / "raw-index-after.json"),
                   derived_index_sha256=sha256_file(state_dir / "derived-index-after.json"))
    _write(state_dir / "journal.json", journal)
    return _publish_indices(state_dir, plan, journal)


def _publish_indices(state_dir: Path, plan: dict, journal: dict) -> dict:
    _copy_publish(state_dir / "derived-index-after.json", Path(plan["derived_index_path"]),
                  journal["derived_index_sha256"], None)
    _copy_publish(state_dir / "raw-index-after.json", Path(plan["raw_index_path"]),
                  journal["raw_index_sha256"], plan["raw_index_before_sha256"])
    journal["phase"] = "PUBLISHED"
    _write(state_dir / "journal.json", journal)
    return journal


def _verify_published(plan: dict, journal: dict) -> None:
    if len(journal["days"]) != len(plan["days"]) or any(row["state"] != "PUBLISHED_VERIFIED" for row in journal["days"].values()):
        raise ValueError("calendar publication is incomplete")
    _digest(Path(plan["raw_index_path"]), journal["raw_index_sha256"])
    _digest(Path(plan["derived_index_path"]), journal["derived_index_sha256"])
    root = Path(plan["project_root"])
    native_paths = {str(_record_path(entry["native_record"])) for entry in plan["entries"]}
    current = _read(Path(plan["raw_index_path"]))
    if any(str(_record_path(row)) in native_paths for row in current["records"]):
        raise ValueError("raw index still references a native retirement target")
    for entry in plan["entries"]:
        _digest(_record_path(entry["old_trade_record"]), entry["output_sha256"]["trades.parquet"])
        _digest(root / "derived/binance_futures" / SYMBOL / entry["day"] / AGGREGATE, entry["output_sha256"][AGGREGATE])
        row = journal["days"][entry["day"]]
        _digest(Path(row["bar_path"]), row["bar_sha256"])
        _digest(Path(row["bar_meta_path"]), row["bar_meta_sha256"])
        _verify_bar(Path(row["bar_path"]), entry["output_sha256"]["trades.parquet"], entry["rows"],
                    source_path=_record_path(entry["old_trade_record"]))


@_locked
def retire_native(state_dir: Path, *, explicit: bool = False) -> dict:
    """Delete only predeclared BTCUSDC native files after whole-calendar proof."""
    if not explicit:
        raise ValueError("native retirement requires explicit authorization")
    state_dir = Path(state_dir)
    plan, journal = _load(state_dir)
    if journal["phase"] not in {"PUBLISHED", "RETIRING", "RETIRED"}:
        raise ValueError("all calendar publication must finish before native retirement")
    _verify_published(plan, journal)
    # Preflight every old file before deleting even the first; a later-date
    # mismatch must not leave an earlier date destructively half-retired.
    for entry in plan["entries"]:
        path = _record_path(entry["native_record"])
        prior = journal["native_retirement"].get(entry["day"])
        if path.exists():
            if prior == "RETIRED":
                raise ValueError("retired native file reappeared; refusing repeated deletion")
            _digest(path, entry["native_record"]["sha256"])
        elif prior not in {"DELETE_INTENT", "RETIRED"}:
            raise ValueError("native file disappeared without a recorded retirement intent")
    journal["phase"] = "RETIRING"
    _write(state_dir / "journal.json", journal)
    for entry in plan["entries"]:
        day, row = entry["day"], entry["native_record"]
        path = _record_path(row)
        if journal["native_retirement"].get(day) == "RETIRED":
            continue
        journal["native_retirement"][day] = "DELETE_INTENT"
        _write(state_dir / "journal.json", journal)
        if path.exists():
            _digest(path, row["sha256"])
            path.unlink()
            _sync_directory(path.parent)
        journal["native_retirement"][day] = "RETIRED"
        _write(state_dir / "journal.json", journal)
    journal["phase"] = "RETIRED"
    _write(state_dir / "journal.json", journal)
    _write(state_dir / "retired-native-index.json", {
        "status": "RETIRED", "transaction": plan["transaction"],
        "records": [entry["native_record"] for entry in plan["entries"]],
        "retired_files": len(plan["entries"]),
        "historical_native_parent_availability_after_retirement": "UNAVAILABLE_REQUIRES_REACQUISITION",
    })
    return journal


def _incremental_sources(path: Path, day: str) -> list[dict]:
    """An explicit capture manifest, not the retired hourly acquisition plan."""
    adjacent = {(date.fromisoformat(day) + timedelta(days=n)).isoformat() for n in (-1, 0, 1)}
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    seen = set()
    for row in rows:
        if (row.get("day") not in adjacent or row.get("channel") != "trades"
                or row.get("symbol") != SYMBOL or row.get("exchange") != "binance-futures"):
            raise ValueError("incremental sources require explicit same-market trades in D-1/D/D+1")
        source = Path(row["path"])
        if str(source) in seen:
            raise ValueError("duplicate incremental source path")
        seen.add(str(source))
        _digest(source, row["sha256"])
    return rows


def _one(items, predicate, description):
    matches = [row for row in items if predicate(row)]
    if len(matches) != 1:
        raise ValueError(f"expected one current {description}")
    return matches[0]


def prepare_incremental_day(project_root: Path, source_manifest: Path, state_dir: Path, *,
                            day: str, catalog_root: Path) -> dict:
    """Prepare one current-canonical supplement, never publish or retire files.

    The JSONL source manifest has explicit path/sha256/day/channel/symbol/exchange
    fields. Its D-1/D/D+1 captures are reconciled by the existing native-ID owner
    rule. This mode requires the current three-raw-channel layout and an existing
    synthetic aggregate index; it neither restores native aggTrades nor rebuilds
    bars/features while pretending those unchanged artifacts are current.
    """
    from data.daily_schema_cutover import identity, planned_files, prepare_transaction, rebind_current, rebind_csv, rebind_json, usage_digest
    from data.trade_aggregation import prepare_day

    project_root, source_manifest, state_dir, catalog_root = [Path(p).absolute() for p in
        (project_root, source_manifest, state_dir, catalog_root)]
    for path in (project_root, state_dir, catalog_root):
        _reject_output_symlinks(path)
    date.fromisoformat(day)
    plan_path = state_dir / "incremental.json"
    if plan_path.exists():
        previous = _read(plan_path)
        if (previous["day"], previous["project_root"], previous["catalog_root"], previous["source_manifest"]) != (
                day, str(project_root), str(catalog_root), str(source_manifest)):
            raise ValueError("incremental preparation scope changed")
        _digest(source_manifest, previous["source_manifest_sha256"])
        return previous
    sources = _incremental_sources(source_manifest, day)
    source_manifest_sha = sha256_file(source_manifest)
    raw_index_path = project_root / "raw/daily-index.json"
    aggregate_index_path = project_root / "derived/trade_aggregate_index.json"
    owner_path, readability_path = catalog_root / "owner-manifest.json", catalog_root / "readability.json"
    protected = [identity(p) for p in (raw_index_path, aggregate_index_path, owner_path, readability_path)]
    raw_index, aggregate_index, owner, readability = map(_read,
        (raw_index_path, aggregate_index_path, owner_path, readability_path))
    current = [r for r in raw_index["records"] if r.get("day") == day and r.get("symbol") == SYMBOL]
    if len(current) != 3 or {r["channel"] for r in current} != {"trades", "incremental_book_L2", "funding"}:
        raise ValueError("incremental publication requires the current three-channel raw layout")
    raw_row = _one(current, lambda r: r["channel"] == "trades", "individual trade record")
    agg_row = _one(aggregate_index["records"], lambda r: r.get("day") == day and r.get("symbol") == SYMBOL,
                   "synthetic aggregate record")
    if (agg_row.get("channel") != "trade_aggregates_100ms"
            or aggregate_index.get("native_aggtrade_identity") is not False
            or aggregate_index.get("execution_event_authority") is not False):
        raise ValueError("current aggregate index is not the synthetic feature-only contract")
    primary = project_root / "raw/binance_futures" / SYMBOL / day / "trades.parquet"
    aggregate = project_root / "derived/binance_futures" / SYMBOL / day / AGGREGATE
    receipt_target = aggregate.with_suffix(".receipt.json")
    if (_record_path(raw_row) != primary or Path(agg_row["path"]) != aggregate
            or Path(agg_row["source_receipt"]) != receipt_target
            or raw_row.get("source_receipt") != str(receipt_target)):
        raise ValueError("current trade/aggregate/receipt targets disagree")
    for path, digest in ((primary, raw_row["sha256"]), (aggregate, agg_row["sha256"]),
                         (receipt_target, agg_row["source_receipt_sha256"])):
        _digest(path, digest)
        protected.append(identity(path))
    if (raw_row.get("source_receipt_sha256") != agg_row["source_receipt_sha256"]
            or agg_row.get("individual_source_sha256") != raw_row["sha256"]):
        raise ValueError("current aggregate is not bound to the canonical individual tape")
    specs = {name: _one(owner["datasets"], lambda r, name=name: r.get("id") == name, name)
             for name in (RAW_DATASET, AGGREGATE_DATASET)}
    for name, target in ((RAW_DATASET, primary), (AGGREGATE_DATASET, aggregate)):
        local = _one(specs[name]["inventories"], lambda r: r.get("node") == "local", "local inventory")
        registered = _one(local["files_by_day"][day], lambda r, target=target: r.get("path") == str(target), "inventory file")
        if registered.get("sha256") != sha256_file(target):
            raise ValueError("owner inventory identity differs from current indexed input")
    rights = usage_digest(readability)
    # Raw and generated data live on the data volume (often an external disk),
    # while private state/catalogs can live on another filesystem. Every final
    # os.replace still uses a staged sibling on its own target filesystem.
    namespace = hashlib.sha256(f"{state_dir}\n{day}\n{source_manifest_sha}".encode()).hexdigest()[:24]
    folder = project_root / "derived/.trade-incremental" / namespace / day
    receipt = prepare_day(primary, sources, day, folder)
    trades, groups = _verify_staged(folder, receipt)
    _verify_calendar_ids(folder.parent, [day])
    changed = receipt.get("content_changed")
    if (type(changed) is not bool or receipt.get("canonical_replacement_required") is not changed
            or changed != bool(receipt["stats"]["added_rows"])):
        raise ValueError("incremental content-change declarations disagree")
    for record in protected:
        _digest(Path(record["path"]), record["sha256"])
    _digest(source_manifest, source_manifest_sha)
    plan = {"schema": "narrowgate.incremental_trade_union.v1", "day": day,
        "project_root": str(project_root), "catalog_root": str(catalog_root),
        "source_manifest": str(source_manifest), "source_manifest_sha256": source_manifest_sha,
        "sources": sources, "content_changed": changed,
        "canonical_replacement_required": changed, "prepared_receipt": str(folder / "receipt.json"),
        "published_receipt": str(receipt_target),
        "prepared_receipt_sha256": sha256_file(folder / "receipt.json"), "protected_inputs": protected,
        "adjacent_primary_sha256": receipt["adjacent_primary_sha256"],
        "research_use_sha256": rights, "economic_admission": False,
        "source_capture_completeness": "UNKNOWN", "native_aggregate_created_or_retired": False,
        "stats": receipt["stats"], "status": "PREPARED" if changed else "NO_CHANGE",
        "created_at_utc": datetime.now(timezone.utc).isoformat()}
    if not changed:
        _write(plan_path, plan, exclusive=True)
        return plan  # Critically: no current metadata or artifact gets new bytes/SHA.
    pairs = [(folder / "trades.parquet", primary), (folder / AGGREGATE, aggregate),
             (folder / "receipt.json", receipt_target)]
    records = planned_files(pairs)
    raw_sha, aggregate_sha = receipt["output_sha256"]["trades.parquet"], receipt["output_sha256"][AGGREGATE]
    dependency = {"status": "REFRESH_REQUIRED", "changed_trade_day": day, "required_trade_sha256": raw_sha,
        "bars_1s": "REFRESH_REQUIRED", "taker_tempo": "REFRESH_REQUIRED", "model_features": "REFRESH_REQUIRED",
        "labels": "REBUILD_REQUIRED", "trained_models": "RETRAIN_REQUIRED", "reuse_old_labels_or_models": False,
        "later_feature_context": "REQUIRES_DEPENDENCY_WINDOW_CHECK"}
    extras = []
    def stage_path(target):
        target = Path(target)
        path = target.with_name(f".{target.name}.trade-incremental-{namespace}.staged")
        if path.exists():
            raise FileExistsError("unpublished metadata staging exists; preserve it for explicit recovery")
        pairs.append((path, target))
        return path
    def mutate_raw(value):
        row = _one(value["records"], lambda r: r.get("day") == day and r.get("symbol") == SYMBOL
                   and r.get("channel") == "trades", "raw trade record")
        row.update(rebind_current(row, records))
        for key in ("output_identity", "output_ctime_ns", "source_identity", "same_inode"):
            row.pop(key, None)
        row.update(rows=trades, source=str(primary), source_sha256=raw_sha,
                   source_kind="individual_trade_union", mode="incremental_union_published")
    def mutate_aggregate(value):
        row = _one(value["records"], lambda r: r.get("day") == day and r.get("symbol") == SYMBOL,
                   "aggregate record")
        row.update(rebind_current(row, records))
        row.update(rows=groups, individual_rows=trades, individual_source_sha256=raw_sha,
            future_fill_violations=0, individual_count_conserved=True, dependency_refresh=copy.deepcopy(dependency),
            source_availability="DECLARED_CAPTURE_INPUTS_VERIFIED_CAPTURE_COMPLETENESS_UNKNOWN")
    for path, mutate in ((raw_index_path, mutate_raw), (aggregate_index_path, mutate_aggregate)):
        extras.append(rebind_json(path, [], mutate=mutate, output=stage_path(path)))
    dependencies = [x for x in owner["datasets"] if x.get("symbol") == SYMBOL and x.get("data_type") in TRADE_DEPENDENCIES]
    common_audit = {"etl_identity_verified": True, "encoding_verified": True, "individual_rows": trades,
        "source_sha256": raw_sha, "etl_receipt_sha256": sha256_file(folder / "receipt.json"),
        "added_individual_rows": receipt["stats"]["added_rows"],
        "source_invalid_rows": receipt["stats"]["invalid_secondary_rows"],
        "source_overlap_rows": receipt["stats"]["overlap_rows"],
        "source_time_conflicts": len(receipt["stats"]["time_conflicts"]), "future_fill_violations": 0,
        "capture_completeness": "UNKNOWN", "economic_admission": False,
        "notes": "Current individual-ID union and self-generated 100ms groups verified; dependent bars/tempo/features require refresh."}
    for name, count, digest in ((RAW_DATASET, trades, raw_sha), (AGGREGATE_DATASET, groups, aggregate_sha)):
        path = Path(specs[name]["audit"]["path"])
        extras.append(rebind_csv(path, records, day=day,
            updates={**common_audit, "rows": count, "sha256": digest}, output=stage_path(path)))
    for spec in dependencies:
        audit = spec.get("audit", {})
        if audit.get("path"):
            path = Path(audit["path"])
            updates = {"trade_dependency_status": "REFRESH_REQUIRED", "required_trade_sha256": raw_sha,
                       "notes": "Retained artifact bytes have not been rebuilt for the changed individual tape."}
            if audit.get("check_column"):
                updates[audit["check_column"]] = False
            extras.append(rebind_csv(path, [], day=day, updates=updates, output=stage_path(path)))
    def mutate_owner(value):
        for spec in value["datasets"]:
            if spec.get("id") in specs:
                spec.update(rebind_current(spec, records + extras))
            elif spec.get("symbol") == SYMBOL and spec.get("data_type") in TRADE_DEPENDENCIES:
                # Rebind audit-file identity only, never stale input/output SHAs.
                spec.update(rebind_current(spec, extras))
                spec.setdefault("trade_dependency_refresh", {})[day] = copy.deepcopy(dependency)
    extras.append(rebind_json(owner_path, [], mutate=mutate_owner, output=stage_path(owner_path)))
    def mutate_readability(value):
        row = _one(value["records"], lambda r: r.get("calendar_date") == day, "calendar day")
        row["trade_dependency_refresh"] = copy.deepcopy(dependency)
        for channel in row["channels"]:
            if channel.get("source_id") in specs:
                channel.update(rebind_current(channel, records + extras))
                channel.update(economic_admission="NOT_GRANTED", future_fill_violations=0)
                channel["current_trade_union_validation"] = {
                    "status": "ETL_VERIFIED", "individual_rows": trades, "added_rows": receipt["stats"]["added_rows"],
                    "raw_sha256": raw_sha, "aggregate_sha256": aggregate_sha,
                    "native_aggtrade_identity": False, "capture_completeness": "UNKNOWN"}
            elif channel.get("symbol") == SYMBOL and channel.get("channel") in TRADE_DEPENDENCIES:
                channel.update(rebind_current(channel, extras))
                channel["trade_dependency_status"] = "REFRESH_REQUIRED"
                channel["required_trade_sha256"] = raw_sha
        if usage_digest(value) != rights:
            raise ValueError("incremental data preparation changed previous-use")
    extras.append(rebind_json(readability_path, [], mutate=mutate_readability, output=stage_path(readability_path)))
    for record in protected:
        _digest(Path(record["path"]), record["sha256"])
    journal = prepare_transaction(state_dir / "publication.json", pairs, day=day,
        metadata={"mode": "incremental_trade_union.v1", "catalog_prepared": True,
                  "research_use_sha256": rights, "dependency_refresh": dependency})
    plan.update(publication_journal=str(state_dir / "publication.json"), dependency_refresh=dependency,
                publication_files=len(journal["files"]))
    _write(plan_path, plan, exclusive=True)
    return plan


@_locked
def publish_incremental_day(state_dir: Path) -> dict:
    """Explicitly publish a prepared data-only transaction, or verify its no-op."""
    from data.daily_schema_cutover import identity, publish_transaction, usage_digest

    state_dir = Path(state_dir)
    plan = _read(state_dir / "incremental.json")
    if plan.get("schema") != "narrowgate.incremental_trade_union.v1":
        raise ValueError("unexpected incremental publication plan")
    _digest(Path(plan["source_manifest"]), plan["source_manifest_sha256"])
    for source in plan["sources"]:
        _digest(Path(source["path"]), source["sha256"])
    for path, digest in plan["adjacent_primary_sha256"].items():
        _digest(Path(path), digest)
    readability = Path(plan["catalog_root"]) / "readability.json"
    if usage_digest(_read(readability)) != plan["research_use_sha256"]:
        raise ValueError("previous-use changed after incremental preparation")
    if not plan["content_changed"]:
        for record in plan["protected_inputs"]:
            _digest(Path(record["path"]), record["sha256"])
        return {**plan, "status": "NO_CHANGE", "current_files_replaced": 0}
    state = publish_transaction(Path(plan["publication_journal"]))
    for record in state["files"]:
        if identity(Path(record["after"]["path"]))["sha256"] != record["after"]["sha256"]:
            raise ValueError("incremental publication differs from verified staging")
    if usage_digest(_read(readability)) != plan["research_use_sha256"]:
        raise ValueError("incremental publication changed previous-use")
    state["status"] = "PUBLISHED_VERIFIED"
    _write(Path(plan["publication_journal"]), state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("prepare", "publish", "retire", "incremental-prepare", "incremental-publish"))
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--project-root", type=Path)
    parser.add_argument("--staged-root", type=Path)
    parser.add_argument("--acquisition-plan", type=Path)
    parser.add_argument("--acquisition-objects", type=Path)
    parser.add_argument("--preparation-manifest", type=Path)
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--catalog-root", type=Path)
    parser.add_argument("--day")
    parser.add_argument("--start-day", default=START_DAY)
    parser.add_argument("--end-day", default=END_DAY)
    parser.add_argument("--retire-native", action="store_true")
    args = parser.parse_args()
    if args.operation == "incremental-prepare":
        if any(value is None for value in (args.project_root, args.source_manifest, args.catalog_root, args.day)):
            parser.error("incremental-prepare requires project/catalog roots, source manifest and day")
        result = prepare_incremental_day(args.project_root, args.source_manifest, args.state_dir,
                                        day=args.day, catalog_root=args.catalog_root)
        print(json.dumps({key: result[key] for key in ("day", "status", "content_changed")}))
    elif args.operation == "incremental-publish":
        result = publish_incremental_day(args.state_dir)
        print(json.dumps({key: result[key] for key in ("day", "status")}))
    elif args.operation == "prepare":
        required = (args.project_root, args.staged_root, args.acquisition_plan,
                    args.acquisition_objects, args.preparation_manifest)
        if any(path is None for path in required):
            parser.error("prepare requires project/staged roots and all three input manifests")
        result = prepare_cutover(*required, args.state_dir, start_day=args.start_day, end_day=args.end_day)
        print(json.dumps({"transaction": result["transaction"], "days": len(result["days"]), "phase": "PREPARED"}))
    else:
        result = publish_cutover(args.state_dir) if args.operation == "publish" else retire_native(args.state_dir, explicit=args.retire_native)
        print(json.dumps({"phase": result["phase"], "days": dict(Counter(row["state"] for row in result["days"].values()))}))


if __name__ == "__main__":
    main()
