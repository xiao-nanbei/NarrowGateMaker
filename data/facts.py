"""Bound fact storage and fixed-calendar inventory; no strategy or source fallback.

Acquisition-specific decoding stays behind the shared parser. Private manifests
retain real source identities; user-facing commands never require a supplier.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import fcntl
from collections import Counter
from collections.abc import MutableMapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, fields, replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from data.tardis_input import (
    CONTRACT, BookMessage, ObservableBook, SourceFragment, SourceQuality, TradeExecution,
    iter_book_messages, iter_trade_executions,
)


def _real(path: Path) -> Path:
    path = path.expanduser().absolute()
    if path != path.resolve():
        raise ValueError("data paths must be real directories/files, not symlinks")
    return path


def save_private_json(path, payload):
    path = _real(Path(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".part")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def inventory_calendar(root: Path, *, scope=None):
    """Metadata inventory, NOT full decompression or research admission.

    A recorded checksum is labelled recorded, never current-byte verified. The
    full materializer computes the input identity once while freezing its plan.
    """
    root = _real(root)
    scope = scope or json.loads(Path(__file__).with_name("dataset_scope.json").read_text())
    start, end = date.fromisoformat(scope["start_day"]), date.fromisoformat(scope["end_day_inclusive"])
    rows = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        channels = []
        for symbol in scope["symbols"]:
            for channel in scope["symbols"][symbol]["raw_channels"]:
                key = f"binance-futures/{channel}/{day:%Y/%m/%d}/{symbol}"
                matches = [root / (key + ".csv." + ext) for ext in ("xz", "zst", "zstd")]
                matches = [p for p in matches if p.is_file()]
                record = {"symbol": symbol, "channel": channel, "status": "missing",
                          "source_files": [], "observed_coverage": None, "carried_coverage": None,
                          "unknown_coverage": None, "max_source_gap_us": None,
                          "future_fill_violations": None, "sequence_continuity": "unknown",
                          "deduplication": "not_checked", "content_status": "not_scanned"}
                if len(matches) > 1:
                    record["status"] = "duplicate_source_selection"
                elif matches:
                    path = matches[0]
                    if path != path.resolve():
                        record["status"] = "symlink_rejected"
                    else:
                        stat = path.stat()
                        record["status"] = "present_not_content_accepted"
                        record["source_files"] = [{"path": str(path.relative_to(root)),
                            "size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}]
                        receipt = root / (".delivery-state/" + key + ".json")
                        if receipt.is_file() and not receipt.is_symlink():
                            state = json.loads(receipt.read_text())
                            if state.get("status") == "completed" and state.get("size_bytes") == stat.st_size:
                                record["recorded_sha256"] = state.get("sha256")
                                record["checksum_status"] = "recorded_not_rehashed"
                                record["download_compression_verified"] = state.get("compression_verified")
                channels.append(record)
        rows.append({"calendar_date": day.isoformat(), "channels": channels,
                     "research_use": "unknown_preserve_existing_previous_use",
                     "economic_admission": False, "cross_day_continuity": "not_scanned"})
    counts = Counter(c["status"] for row in rows for c in row["channels"])
    return {"schema": "data.calendar.v1", "input_contract": CONTRACT,
            "visibility": "local_only_do_not_publish", "source_root": str(root),
            "summary": {"dates": len(rows), "start": start.isoformat(), "end": end.isoformat(),
                        "channels": dict(counts), "full_content_acceptance": False}, "days": rows}


class DiskTradeIdentities(MutableMapping):
    """Bounded SQLite dedup index shared across every file in one fact bundle."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA cache_size=-16384")
        self.connection.execute("CREATE TABLE identities (market TEXT, id INTEGER, content TEXT, PRIMARY KEY(market,id)) WITHOUT ROWID")

    def __getitem__(self, key):
        row = self.connection.execute("SELECT content FROM identities WHERE market=? AND id=?", key).fetchone()
        if row is None:
            raise KeyError(key)
        ts, side, price, quantity = json.loads(row[0])
        return ts, side, Decimal(price), Decimal(quantity)

    def __setitem__(self, key, value):
        ts, side, price, quantity = value
        self.connection.execute("INSERT INTO identities VALUES (?,?,?)", (*key, json.dumps([ts, side, str(price), str(quantity)])))

    def __delitem__(self, key):
        raise TypeError("fact identity index is append-only")

    def __iter__(self):
        return iter(self.connection.execute("SELECT market,id FROM identities"))

    def __len__(self):
        return self.connection.execute("SELECT count(*) FROM identities").fetchone()[0]

    def close(self):
        self.connection.close()


def calendar_plan(root, *, start, end, symbols, channels):
    """Select a whole declared interval, never good-day filtering or fallback."""
    inventory = inventory_calendar(Path(root))
    if (not inventory["summary"]["start"] <= start <= end <= inventory["summary"]["end"]
            or len(set(symbols)) != len(symbols) or len(set(channels)) != len(channels)
            or not symbols or not channels or not set(symbols) <= {"BTCUSDC", "BTCUSDT"}
            or not set(channels) <= {"incremental_book_L2", "trades"}):
        raise ValueError("invalid, duplicate or out-of-scope calendar selection")
    date.fromisoformat(start)
    date.fromisoformat(end)
    files = []
    for row in inventory["days"]:
        if not start <= row["calendar_date"] <= end:
            continue
        for item in row["channels"]:
            if item["symbol"] in symbols and item["channel"] in channels:
                if item["status"] != "present_not_content_accepted":
                    raise ValueError(f"required input {row['calendar_date']} {item['symbol']} {item['channel']}: {item['status']}")
                files.append({"path": str(Path(root) / item["source_files"][0]["path"]),
                              "symbol": item["symbol"], "channel": item["channel"],
                              "file_date": row["calendar_date"]})
    return {"source_profile": "tardis_only", "files": files,
            "calendar_start": start, "calendar_end": end, "mapper_evidence": "unknown",
            "purpose": "source_facts_not_economic_admission"}


_COMMON = [("market_id", pa.string()), ("source_file_id", pa.string()),
           ("source_ordinal", pa.int64()), ("source_timestamp_us", pa.int64()),
           ("source_clock_kind", pa.string()), ("exchange_ts_ns", pa.int64()),
           ("provider_receive_ts_us", pa.int64())]
BOOK_SCHEMA = pa.schema(_COMMON + [("first_row", pa.int64()), ("last_row", pa.int64()),
    ("provider_group_id", pa.string()), ("kind", pa.string()), ("boundary_status", pa.string()),
    ("state_time_certainty", pa.string()), ("levels", pa.list_(pa.struct([
        ("side", pa.string()), ("price", pa.string()), ("quantity_after", pa.string())])))])
TRADE_SCHEMA = pa.schema(_COMMON + [("source_row", pa.int64()), ("trade_id", pa.int64()),
    ("aggressor_side", pa.string()), ("price", pa.string()), ("quantity", pa.string()),
    ("normal_quantity", pa.string()), ("individual_execution_count", pa.int64()),
    ("native_aggregate_packet_count", pa.int64())])


def _record(event):
    # Facts are immutable; recursively deep-copying every Decimal level here
    # needlessly dominates full-calendar conversion.
    record = {field.name: getattr(event, field.name) for field in fields(event)}
    if isinstance(event, BookMessage):
        record["levels"] = [{"side": s, "price": str(p), "quantity_after": str(q)} for s, p, q in event.levels]
    else:
        record["price"], record["quantity"] = str(event.price), str(event.quantity)
    return record


def materialize(plan_path: Path, output: Path, *, row_group_size=8192):
    """Create-only atomic fact bundle. No partial bundle is admitted on failure.

    The ordered plan includes complete originals (including required adjacent
    context). All selected trade files share one disk identity index; no resets
    at UTC midnight. Output row groups hold whole BookMessages, not split levels.
    """
    if row_group_size < 1:
        raise ValueError("positive row group size required")
    plan = plan_path if isinstance(plan_path, dict) else json.loads(_real(plan_path).read_text())
    if plan.get("source_profile") != "tardis_only" or not plan.get("files"):
        raise ValueError("an explicit single-source input plan is required")
    output = _real(output)
    if output.exists():
        raise FileExistsError("fact bundle exists; do not overwrite a consumed input")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.with_name(output.name + "." + uuid4().hex + ".part")
    stage.mkdir(mode=0o700)
    identity_path = stage / "identities.sqlite"
    identities = DiskTradeIdentities(identity_path)
    seen, records, clocks, ordinals = set(), [], {}, {}
    try:
        for index, spec in enumerate(plan["files"]):
            path = _real(Path(spec["path"]))
            before = path.stat()
            digest = _digest(path)
            if spec.get("sha256") and spec["sha256"] != digest:
                raise ValueError("input checksum mismatch")
            if digest in seen:
                raise ValueError("duplicate source file selection")
            seen.add(digest)
            quality = SourceQuality()
            fragment = SourceFragment(path, digest)
            symbol, channel = spec["symbol"], spec["channel"]
            if channel == "incremental_book_L2":
                events = iter_book_messages([fragment], symbol=symbol, quality=quality,
                    delta_clock_kind=spec.get("delta_clock_kind", "unknown"),
                    snapshot_clock_kind=spec.get("snapshot_clock_kind", "unknown"))
                schema = BOOK_SCHEMA
            elif channel == "trades":
                events = iter_trade_executions([fragment], symbol=symbol, quality=quality,
                    identities=identities, clock_kind=spec.get("clock_kind", "unknown"))
                schema = TRADE_SCHEMA
            else:
                raise ValueError("unsupported fact channel")
            filename = f"{index:05d}-{symbol}-{channel}.parquet"
            metadata = {b"input_contract": CONTRACT.encode(), b"source_sha256": digest.encode(),
                        b"channel": channel.encode(), b"source_profile": b"tardis_only"}
            first = last = None
            previous = clocks.get((symbol, channel))
            predecessor = previous
            largest_gap, regressions, batch = 0, 0, []
            book = ObservableBook() if channel == "incremental_book_L2" else None
            acceptance = {"first_event_kind": None, "invalid_book_states": 0,
                "preinitialization_deltas": 0, "snapshots": 0,
                "first_source_observation_us": None, "trade_id_min": None,
                "trade_id_max": None, "source_row_count_verified": True,
                "normal_quantity_observability": "unavailable"}
            with pq.ParquetWriter(stage / filename, schema.with_metadata(metadata), compression="zstd") as writer:
                for event in events:
                    # Parser ordinals are file-local. Public stream ordinals
                    # must not restart at midnight or reorder equal-time trades.
                    stream_key = (symbol, channel)
                    ordinal = ordinals.get(stream_key, 0)
                    event = replace(event, source_ordinal=ordinal)
                    ordinals[stream_key] = ordinal + 1
                    timestamp = event.source_timestamp_us
                    if acceptance["first_source_observation_us"] is None:
                        acceptance["first_source_observation_us"] = timestamp
                        acceptance["first_event_kind"] = event.kind if book is not None else "trade"
                    if book is not None:
                        acceptance["snapshots"] += event.kind == "snapshot"
                        acceptance["preinitialization_deltas"] += not book.apply(event)
                        acceptance["invalid_book_states"] += not book.view(1).valid
                    else:
                        lo, hi = acceptance["trade_id_min"], acceptance["trade_id_max"]
                        acceptance["trade_id_min"] = event.trade_id if lo is None else min(lo, event.trade_id)
                        acceptance["trade_id_max"] = event.trade_id if hi is None else max(hi, event.trade_id)
                    first = timestamp if first is None else min(first, timestamp)
                    last = timestamp if last is None else max(last, timestamp)
                    if previous is not None:
                        largest_gap = max(largest_gap, timestamp - previous)
                        if first == timestamp and quality.emitted_events == 1 and timestamp < previous:
                            regressions += 1
                    previous = timestamp
                    regressions += len(quality.time_regressions)
                    # Persist regression evidence in rows, count it without an
                    # unbounded Python list; original timestamps remain intact.
                    quality.time_regressions.clear()
                    batch.append(_record(event))
                    if len(batch) >= row_group_size:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                        batch.clear()
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            clocks[symbol, channel] = previous
            after = path.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                raise ValueError("source changed while normalizing")
            records.append({"file": filename, "channel": channel, "symbol": symbol,
                "source_path": str(path), "source_sha256": digest, "source_size_bytes": before.st_size,
                "source_mtime_ns": before.st_mtime_ns, "sha256": _digest(stage / filename),
                "quality": {**asdict(quality), **acceptance, "time_regression_count": regressions,
                    "source_time_min_us": first, "source_time_max_us": last,
                    "predecessor_observation_us": predecessor, "last_source_observation_us": previous,
                    "max_observation_gap_us": largest_gap, "capture_completeness": "unknown",
                    "future_fill_violations": 0, "future_fill_check": "facts_do_not_fill"}})
        identities.close()
        identity_path.unlink()
        identity_path.with_name(identity_path.name + "-journal").unlink(missing_ok=True)
        result = {"schema": "data.facts.v1", "input_contract": CONTRACT, "source_profile": "tardis_only",
                  "visibility": "local_only_do_not_publish", "status": "facts_materialized_not_economic_admission",
                  "plan": plan, "files": records, "observation_contract_status": "not_run",
                  "research_use": "unchanged_requires_explicit_split_manifest"}
        save_private_json(stage / "manifest.json", result)
        os.rename(stage, output)
        return result
    except BaseException:
        identities.close()
        shutil.rmtree(stage)
        raise


def read_facts(bundle: Path, *, verify=True, channels=None):
    """Stream only source-bound fact schemas; never reinterpret legacy Parquet."""
    bundle = _real(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text())
    if (manifest.get("schema") != "data.facts.v1" or manifest.get("input_contract") != CONTRACT
            or manifest.get("source_profile") != "tardis_only"):
        raise ValueError("incompatible fact bundle")
    for spec in manifest["files"]:
        if spec["channel"] not in {"incremental_book_L2", "trades"}:
            raise ValueError("incompatible fact channel")
        if channels is not None and spec["channel"] not in channels:
            continue
        path = _real(bundle / spec["file"])
        if path.parent != bundle or (verify and _digest(path) != spec["sha256"]):
            raise ValueError("fact shard identity mismatch")
        file = pq.ParquetFile(path)
        schema = BOOK_SCHEMA if spec["channel"] == "incremental_book_L2" else TRADE_SCHEMA
        if not file.schema_arrow.remove_metadata().equals(schema):
            raise ValueError("fact shard schema mismatch")
        metadata = file.schema_arrow.metadata or {}
        if (metadata.get(b"input_contract") != CONTRACT.encode()
                or metadata.get(b"source_sha256") != spec["source_sha256"].encode()
                or metadata.get(b"source_profile") != b"tardis_only"
                or metadata.get(b"channel") != spec["channel"].encode()):
            raise ValueError("fact shard source binding mismatch")
        for batch in file.iter_batches(batch_size=8192):
            for row in batch.to_pylist():
                if spec["channel"] == "incremental_book_L2":
                    row["levels"] = tuple((x["side"], Decimal(x["price"]), Decimal(x["quantity_after"])) for x in row["levels"])
                    yield BookMessage(**row)
                else:
                    row["price"], row["quantity"] = Decimal(row["price"]), Decimal(row["quantity"])
                    yield TradeExecution(**row)


def validate_bundle(bundle: Path):
    """Full bound-fact read and continuous book acceptance, zero economics."""
    books, rows, previous_file = {}, {}, {}
    for event in read_facts(bundle):
        key = event.source_file_id
        row = rows.setdefault(key, {"source_file_id": key, "market_id": event.market_id,
            "events": 0, "invalid_book_states": 0, "preinitialization_deltas": 0,
            "snapshots": 0, "max_observation_gap_us": 0, "time_regressions": 0,
            "future_fill_violations": 0, "native_sequence_continuity": "unknown",
            "economic_admission": False})
        row["events"] += 1
        if isinstance(event, BookMessage):
            book = books.setdefault(event.market_id, ObservableBook())
            prior = book.source_asof_us
            if previous_file.get(event.market_id) != key:
                row["predecessor_source_observation_us"] = prior
                previous_file[event.market_id] = key
            if prior is not None:
                row["max_observation_gap_us"] = max(row["max_observation_gap_us"], event.source_timestamp_us - prior)
                row["time_regressions"] += event.source_timestamp_us < prior
            row["snapshots"] += event.kind == "snapshot"
            if not book.apply(event):
                row["preinitialization_deltas"] += 1
            view = book.view(1)
            row["future_fill_violations"] += view.source_asof_us is not None and view.source_asof_us > event.source_timestamp_us
            row["invalid_book_states"] += not view.valid
            row["last_source_observation_us"] = view.source_asof_us
        else:
            row["channel"] = "trades"
    return {"schema": "data.fact_acceptance.v1", "visibility": "local_only_do_not_publish",
            "status": "full_selected_bundle_read_not_full_calendar_admission",
            "bundle_manifest_sha256": _digest(bundle / "manifest.json"), "files": list(rows.values()),
            "strategy_observation_acceptance": "not_run", "economic_replay": "not_run"}


def _build_identity():
    """Cache identity, not an economic or source-clock evidence claim."""
    names = ("facts.py", "tardis_input.py", "normalize_tardis_orderbook.py")
    return hashlib.sha256("".join(_digest(Path(__file__).with_name(n)) for n in names).encode()).hexdigest()


def _check_day_sources(plan, result):
    """Bind explicit current locations to an immutable ordered source plan.

    No historical path is resolved or searched. Changed locations require the
    recorded content digest; unchanged locations retain the existing stat check.
    Parser identity and every non-location plan field must still match.
    """
    original = result["plan"]
    if ({k: v for k, v in original.items() if k != "files"}
            != {k: v for k, v in plan.items() if k != "files"}
            or len(original["files"]) != len(plan["files"])
            or len(plan["files"]) != len(result["files"])):
        raise ValueError("existing day belongs to another source plan/parser; no overwrite")
    for old, current, record in zip(original["files"], plan["files"], result["files"], strict=True):
        if ({k: v for k, v in old.items() if k != "path"}
                != {k: v for k, v in current.items() if k != "path"}
                or old["path"] != record["source_path"]
                or any(current[k] != record[k] for k in ("symbol", "channel"))):
            raise ValueError("ordered source identity changed")
        source = _real(Path(current["path"]))
        stat = source.stat()
        if (stat.st_size, stat.st_mtime_ns) != (record["source_size_bytes"], record["source_mtime_ns"]):
            raise ValueError("completed day source changed; no silent cache reuse")
        if current["path"] != old["path"] and _digest(source) != record["source_sha256"]:
            raise ValueError("relocated source content changed")


def _calendar_day_task(plan, output, build_identity, reuse_build_identity=None):
    output = Path(output)
    if plan["build_identity"] != build_identity:
        raise ValueError("worker implementation drift")
    if output.exists():
        result = json.loads((output / "manifest.json").read_text())
        source_build = result["plan"]["build_identity"]
        if source_build != build_identity and (reuse_build_identity is None or source_build != reuse_build_identity):
            raise ValueError("existing day belongs to another source plan/parser; no overwrite")
        reuse_plan = dict(plan, build_identity=source_build)
        _check_day_sources(reuse_plan, result)
        for record in result["files"]:
            if _digest(output / record["file"]) != record["sha256"]:
                raise ValueError("completed day changed; no silent cache reuse")
    else:
        result = materialize(plan, output)
    return {"calendar_date": plan["calendar_start"], "status": "content_scanned",
            "source_build_identity": result["plan"]["build_identity"],
            "bundle": str(output), "manifest_sha256": _digest(output / "manifest.json"),
            "files": result["files"], "research_use": "unchanged_requires_explicit_split_manifest",
            "economic_admission": False}


def _calendar_boundaries(days):
    """Reconcile independent parser tasks without pretending UTC resets state.

    A complete first snapshot replaces the predecessor book. A prefix of deltas
    requires predecessor reconstruction and stays blocked here. Disjoint trade
    ID ranges prove cross-file uniqueness; overlaps are checked by exact content.
    """
    previous, trade_ranges, overlaps = {}, {}, []
    for row in days:
        if row["status"] != "content_scanned":
            previous.clear()
            continue
        for record in row["files"]:
            key = record["symbol"], record["channel"]
            q = record["quality"]
            prior = previous.get(key)
            boundary = {"predecessor_source_observation_us": None,
                        "clock_regressed": None, "interval_us": None,
                        "status": "no_predecessor_context"}
            if prior:
                last = prior["quality"]["last_source_observation_us"]
                first = q["first_source_observation_us"]
                boundary.update(predecessor_source_observation_us=last,
                    clock_regressed=(first < last) if first is not None and last is not None else None,
                    interval_us=(first - last) if first is not None and last is not None else None,
                    status="source_boundary_checked")
            if record["channel"] == "incremental_book_L2":
                boundary["book_initialization"] = ("atomic_snapshot_replaces_predecessor"
                    if q["first_event_kind"] == "snapshot" else "requires_predecessor_book_reconstruction")
                boundary["carried_clock_refreshed"] = False
                boundary["account_continuity"] = "not_tested_data_only"
            else:
                lo, hi = q["trade_id_min"], q["trade_id_max"]
                ranges = trade_ranges.setdefault(record["symbol"], [])
                if lo is not None:
                    for old_lo, old_hi, old_row in ranges:
                        if max(lo, old_lo) <= min(hi, old_hi):
                            overlaps.append((old_row, row, record["symbol"], max(lo, old_lo), min(hi, old_hi)))
                    ranges.append((lo, hi, row))
                boundary["deduplication"] = "disjoint_identity_ranges" if lo is not None else "empty_input"
            record["boundary"] = boundary
            previous[key] = record
    duplicate_count = 0
    for left, right, symbol, lo, hi in overlaps:
        identity = {}
        for row in (left, right):
            for event in read_facts(Path(row["bundle"]), channels={"trades"}):
                if event.market_id.endswith(":" + symbol) and lo <= event.trade_id <= hi:
                    content = (event.source_timestamp_us, event.aggressor_side, event.price, event.quantity)
                    if event.trade_id in identity:
                        if identity[event.trade_id] != content:
                            raise ValueError("conflicting cross-file trade identity")
                        duplicate_count += 1
                    else:
                        identity[event.trade_id] = content
            for record in row["files"]:
                if record["symbol"] == symbol and record["channel"] == "trades":
                    record["boundary"]["deduplication"] = "overlap_content_checked_consumer_dedup_required"
    return {"cross_file_equal_duplicates": duplicate_count, "overlap_pairs": len(overlaps),
            "identity_conflicts": 0, "native_sequence_continuity": "unknown"}


def materialize_calendar(root, output, *, start, end, symbols, workers=4, reuse_build_identity=None):
    """Resumable fixed-calendar full-content build, one atomic bundle per day.

    Workers parse complete originals, never independent partial snapshots.
    Failures stay in the denominator and successful dates are not discarded.
    The coordinator lock prevents two writers racing the same calendar.
    """
    if type(workers) is not int or not 1 <= workers <= 32:
        raise ValueError("workers must be between 1 and 32")
    if reuse_build_identity is not None and (not isinstance(reuse_build_identity, str)
            or len(reuse_build_identity) != 64 or any(c not in "0123456789abcdef" for c in reuse_build_identity)):
        raise ValueError("reuse_build_identity must be the explicit frozen parser digest")
    output = _real(Path(output))
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_fd = os.open(output / ".lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity = _build_identity()
        inventory = inventory_calendar(Path(root))
        selected = [r for r in inventory["days"] if start <= r["calendar_date"] <= end]
        if (not selected or selected[0]["calendar_date"] != start or selected[-1]["calendar_date"] != end
                or not symbols or len(set(symbols)) != len(symbols) or not set(symbols) <= {"BTCUSDC", "BTCUSDT"}):
            raise ValueError("invalid fixed calendar selection")
        result = {"schema": "data.full_calendar.v1", "input_contract": CONTRACT,
                  "source_profile": "tardis_only", "build_identity": identity,
                  "visibility": "local_only_do_not_publish", "start": start, "end": end,
                  "symbols": symbols, "status": "running", "days": [],
                  "training": "not_run", "economic_replay": "not_run"}
        if reuse_build_identity is not None:
            result["reuse_build_identity"] = reuse_build_identity
        rows = {r["calendar_date"]: {"calendar_date": r["calendar_date"], "status": "pending",
                "research_use": r["research_use"], "economic_admission": False} for r in selected}

        def publish():
            result["days"] = [rows[k] for k in sorted(rows)]
            result["summary"] = dict(Counter(r["status"] for r in result["days"]))
            result["calendar_dates"] = len(rows)
            save_private_json(output / "manifest.json", result)

        publish()
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for day in rows:
                try:
                    plan = calendar_plan(root, start=day, end=day, symbols=symbols,
                                         channels=["incremental_book_L2", "trades"])
                    plan["build_identity"] = identity
                    futures[executor.submit(_calendar_day_task, plan, output / day, identity,
                                            reuse_build_identity)] = day
                except Exception as exc:
                    rows[day].update(status="blocked", reason=str(exc))
            for future in as_completed(futures):
                day = futures[future]
                try:
                    rows[day] = future.result()
                except Exception as exc:
                    rows[day].update(status="failed", reason=f"{type(exc).__name__}: {exc}")
                publish()
                print(json.dumps({"date": day, "status": rows[day]["status"], "summary": result["summary"]}), flush=True)
        try:
            result["boundary_checks"] = _calendar_boundaries(result["days"])
            result["status"] = "full_content_scanned" if all(r["status"] == "content_scanned" for r in rows.values()) else "incomplete"
        except Exception as exc:
            result["status"] = "boundary_failed"
            result["boundary_error"] = str(exc)
        publish()
        return result
    finally:
        os.close(lock_fd)


def validate_calendar(root, *, raw_root=None):
    """Verify every published day's bytes and summarize the complete scan.

    The original build already decompressed every source row. This checks its
    source binding and complete derived checksums, not a second raw parse.
    An explicit new raw root additionally verifies moved source digests.
    Findings remain on their original dates; no economic admission.
    """
    root = _real(Path(root))
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("schema") != "data.full_calendar.v1" or manifest.get("source_profile") != "tardis_only":
        raise ValueError("bound full calendar required")
    first, last = date.fromisoformat(manifest["start"]), date.fromisoformat(manifest["end"])
    expected = [(first + timedelta(days=i)).isoformat() for i in range((last-first).days+1)]
    if [r["calendar_date"] for r in manifest["days"]] != expected:
        raise ValueError("calendar dates missing, duplicated or reordered")
    days, totals = [], {}
    for row in manifest["days"]:
        if row["status"] != "content_scanned":
            raise ValueError(f"unscanned date: {row['calendar_date']}")
        bundle = _real(Path(row["bundle"]))
        if _digest(bundle / "manifest.json") != row["manifest_sha256"]:
            raise ValueError("daily manifest changed")
        daily = json.loads((bundle / "manifest.json").read_text())
        if raw_root is not None:
            current_plan = calendar_plan(raw_root, start=row["calendar_date"], end=row["calendar_date"],
                                         symbols=manifest["symbols"], channels=["incremental_book_L2", "trades"])
            current_plan["build_identity"] = daily["plan"]["build_identity"]
            _check_day_sources(current_plan, daily)
        if [(f["symbol"], f["channel"]) for f in daily["files"]] != [
                (s, c) for s in manifest["symbols"] for c in ("incremental_book_L2", "trades")]:
            raise ValueError("required channel identity mismatch")
        channels = []
        for item, linked in zip(daily["files"], row["files"], strict=True):
            if any(item.get(k) != linked.get(k) for k in ("file", "sha256", "quality", "source_sha256")):
                raise ValueError("calendar/daily source binding mismatch")
            if raw_root is None:
                source = _real(Path(item["source_path"]))
                stat = source.stat()
                if (stat.st_size, stat.st_mtime_ns) != (item["source_size_bytes"], item["source_mtime_ns"]):
                    raise ValueError("source changed since full content scan")
            shard = _real(bundle / item["file"])
            if shard.parent != bundle or _digest(shard) != item["sha256"]:
                raise ValueError("normalized fact bytes changed")
            q = item["quality"]
            if (not q.get("source_row_count_verified") or q["rows"] < 1
                    or pq.read_metadata(shard).num_rows != q["emitted_events"]):
                raise ValueError("empty/unverified source or fact row mismatch")
            key = item["symbol"] + "/" + item["channel"]
            total = totals.setdefault(key, Counter())
            for field in ("rows", "emitted_events", "duplicate_trades", "trade_conflicts",
                          "invalid_book_states", "preinitialization_deltas", "time_regression_count",
                          "future_fill_violations"):
                total[field] += q[field]
            total["max_observation_gap_us"] = max(total["max_observation_gap_us"], q["max_observation_gap_us"])
            findings = [f for f in ("invalid_book_states", "preinitialization_deltas", "time_regression_count") if q[f]]
            channels.append({"channel": item["channel"], "symbol": item["symbol"],
                "source_sha256": item["source_sha256"], "fact_sha256": item["sha256"],
                "quality": q, "boundary": linked.get("boundary"), "findings": findings,
                "observed_time_coverage": None, "capture_completeness": "unknown",
                "status": "readable_with_findings" if findings else "content_verified",
                "sequence_continuity": "unknown"})
        days.append({"calendar_date": row["calendar_date"], "channels": channels,
                     "source_build_identity": daily["plan"]["build_identity"],
                     "research_use": row["research_use"], "economic_admission": False})
    return {"schema": "data.calendar_acceptance.v1", "visibility": "local_only_do_not_publish",
        "calendar_manifest_sha256": _digest(root / "manifest.json"), "start": expected[0], "end": expected[-1],
        "dates": len(days), "status": "content_verified_with_disclosed_quality",
        "verification": ("full_source_parse_at_build_plus_current_fact_hash_and_explicit_source_binding"
                         if raw_root is not None else "full_source_parse_at_build_plus_current_fact_hash_and_source_stat"),
        "source_build_identity": manifest["build_identity"], "totals": totals,
        "boundary_checks": manifest.get("boundary_checks"), "days": days,
        "observation_and_replay_acceptance": "separate_required", "economic_admission": False,
        "training": "not_run", "economic_replay": "not_run", "live": False}
