"""Supplier-neutral daily market containers and causal orderbook fusion.

CSV adapters are ingestion boundaries. Research readers consume daily Parquet
channels; native aggregates remain an optional historical import, not a required
input to the current individual-trade pipeline. The supplier is provenance.
Direct conversions are lossless. Book fusion publishes changes of the selected
reconstructed source state, with explicit provenance; it is not a byte-lossless
archive of duplicate provider messages or a reconstruction of unknown events.
"""

from __future__ import annotations

import argparse
import csv
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import io
import json
import lzma
import os
from pathlib import Path
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


CHANNEL_SOURCES = {
    "incremental_book_L2": "btcusdc-daily-raw-l2",
    "trades": "btcusdc-raw-trades",
    "aggTrades": "btcusdc-raw-aggtrades",
    "funding": "btcusdc-funding-full401",
}
SCHEMA_VERSION = "narrowgate_daily_raw_v1"


def market_day_from_path(path: Path) -> str:
    """UTC date in a purchased YYYY/MM/DD archive or historical daily path."""
    path = Path(path)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.parent.name):
        return date.fromisoformat(path.parent.name).isoformat()
    if (re.fullmatch(r"\d{2}", path.parent.name)
            and re.fullmatch(r"\d{2}", path.parent.parent.name)
            and re.fullmatch(r"\d{4}", path.parent.parent.parent.name)):
        return date(int(path.parents[2].name), int(path.parents[1].name), int(path.parent.name)).isoformat()
    match = re.search(r"\d{4}-\d{2}-\d{2}", path.name)
    if match:
        return date.fromisoformat(match[0]).isoformat()
    raise ValueError(f"No UTC date in daily market path: {path.name}")


def is_tardis_trade_archive(path: Path) -> bool:
    """Purchased CSV naming, not a claim of verified content."""
    return bool(re.fullmatch(r"[A-Z0-9]+\.csv(?:\.(?:xz|zst|zstd))?", Path(path).name))


def iter_tardis_trade_frames(path: Path, *, symbol: str, chunk_size: int = 65536,
                             identities: dict | None = None):
    """Legacy millisecond ABI projection of the shared exact Tardis facts.

    Not a strategy-visible Bar or proof of historical mapper version. Callers
    spanning source files can share the identity store. Fine source precision
    and regressing timestamps require the new observation scheduler, not silent
    rounding/reordering here. File-date clipping belongs after source parsing.
    """
    import pandas as pd
    from data.tardis_input import SourceFragment, iter_trade_executions

    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    previous = None
    rows = []
    for trade in iter_trade_executions([SourceFragment(Path(path), str(Path(path).resolve()))],
                                      symbol=symbol, identities=identities):
        timestamp = trade.source_timestamp_us
        if timestamp % 1000 or (previous is not None and timestamp < previous):
            raise ValueError("legacy trade projection cannot round/reorder source time")
        previous = timestamp
        rows.append({"id": trade.trade_id, "price": float(trade.price),
            "quote_qty": float(trade.price * trade.quantity), "qty": float(trade.quantity),
            "time": timestamp // 1000, "is_buyer_maker": trade.aggressor_side == "sell"})
        if len(rows) == chunk_size:
            yield pd.DataFrame(rows)
            rows = []
    if rows:
        yield pd.DataFrame(rows)

BOOK_SCHEMA = pa.schema([
    ("received_time", pa.int64()), ("event_time", pa.int64()),
    ("transaction_time", pa.int64()), ("symbol", pa.string()),
    ("event_type", pa.string()), ("first_update_id", pa.int64()),
    ("final_update_id", pa.int64()), ("prev_final_update_id", pa.int64()),
    ("last_update_id", pa.int64()), ("side", pa.string()),
    ("price", pa.string()), ("quantity", pa.string()),
    ("order_count", pa.int64()), ("exchange", pa.string()),
    ("timestamp", pa.int64()), ("local_timestamp", pa.int64()),
    ("is_snapshot", pa.bool_()), ("amount", pa.string()),
    ("source_hour", pa.int8()), ("source_row", pa.int64()),
])
FUSED_BOOK_SCHEMA = pa.schema([
    *BOOK_SCHEMA,
    ("source_id", pa.string()),
    ("fusion_reason", pa.string()),
    ("source_observed_timestamp_us", pa.int64()),
])
BOOK_FUSION_SCHEMA = "reconstructed_fusion.v1"
BOOK_UNION_SCHEMA = "observed_union.v1"
UNION_BOOK_SCHEMA = pa.schema([
    *FUSED_BOOK_SCHEMA,
    ("source_timestamp_us", pa.int64()),
    ("source_native_sequence", pa.bool_()),
])
DAILY_BOOK_MARKER = "narrowgate.daily_book.v1"
DAILY_BOOK_SCHEMA = pa.schema([
    ("exchange", pa.string()), ("symbol", pa.string()),
    ("timestamp", pa.int64()), ("local_timestamp", pa.int64()),
    ("is_snapshot", pa.bool_()), ("side", pa.string()),
    ("price", pa.string()), ("amount", pa.string()),
    ("stream_id", pa.string()), ("stream_priority", pa.int16()),
    ("queue_rebase", pa.bool_()), ("observation_only", pa.bool_()),
    ("top_only", pa.bool_()),
    ("native_sequence", pa.bool_()), ("observed_timestamp_us", pa.int64()),
    ("original_timestamp_us", pa.int64()),
    ("received_time", pa.int64()), ("event_time", pa.int64()),
    ("transaction_time", pa.int64()), ("first_update_id", pa.int64()),
    ("final_update_id", pa.int64()), ("prev_final_update_id", pa.int64()),
    ("last_update_id", pa.int64()),
    ("order_count", pa.int64()),
])


def daily_book_receipt(path: Path) -> dict:
    metadata = pq.ParquetFile(path).metadata.metadata or {}
    if metadata.get(b"narrowgate.book_fusion") != DAILY_BOOK_MARKER.encode():
        raise ValueError("not a unified daily book")
    receipt = json.loads(metadata[b"narrowgate.book_receipt"])
    if receipt.get("schema") != DAILY_BOOK_MARKER:
        raise ValueError("daily book receipt schema mismatch")
    book_stream_priority(contract=receipt.get("stream_priority"))
    return receipt


def _legacy_book_priority(receipt: Mapping) -> dict:
    priority = receipt.get("stream_priority", receipt.get("stats", {}).get("stream_priority"))
    seed = receipt.get("initial_continuation") or receipt.get("stats", {}).get("continuation")
    if priority is None and seed is not None:
        return book_stream_priority(seed["source_ids"], preferred_index=seed["kernel"]["preferred"])
    return book_stream_priority(contract=priority)


def project_daily_book_batch(batch, marker: str, priority: Mapping) -> pa.Table:
    """Lossless values/order projection; old names are an import boundary only."""
    n = len(batch)
    values = {name: batch[name] for name in batch.schema.names}
    if marker == DAILY_BOOK_MARKER:
        return pa.Table.from_arrays([values[f.name] for f in DAILY_BOOK_SCHEMA], schema=DAILY_BOOK_SCHEMA)
    union = marker == BOOK_UNION_SCHEMA
    ids = priority["stream_ids"]
    if union:
        indices = pc.index_in(values["source_id"], value_set=pa.array(ids))
        if indices.null_count:
            raise ValueError("undeclared book stream")
        values["stream_id"] = pc.take(pa.array([f"s{i}" for i in range(len(ids))]), indices)
        values["stream_priority"] = pc.take(pa.array(list(book_stream_ranks(priority).values()), type=pa.int16()), indices)
        values["queue_rebase"] = values["is_snapshot"]
        values["observation_only"] = pa.repeat(False, n)
        values["native_sequence"] = values["source_native_sequence"]
        values["original_timestamp_us"] = values["source_timestamp_us"]
    else:
        # These rows are patches of ONE selected book, not independent books
        # named by the origin of each patch. Switching origin rebases queues.
        values["stream_id"], values["stream_priority"] = pa.repeat("s0", n), pa.repeat(pa.scalar(1, pa.int16()), n)
        reason = values.get("fusion_reason", pa.repeat("source_update", n))
        values["queue_rebase"] = pc.or_(values["is_snapshot"], pc.is_in(reason, value_set=pa.array([
            "source_switch", "source_snapshot_reset", "carried_opening_snapshot", "unknown_reconstruction"])))
        values["observation_only"] = pc.equal(reason, "observation_refresh")
        values["native_sequence"] = pa.repeat(False, n)
        values["original_timestamp_us"] = pa.nulls(n, pa.int64())
    values["observed_timestamp_us"] = values.get("source_observed_timestamp_us", values["timestamp"])
    values["top_only"] = pa.repeat(False, n)
    return pa.Table.from_arrays([
        pc.cast(values[f.name], f.type) if f.name in values else pa.nulls(n, f.type)
        for f in DAILY_BOOK_SCHEMA], schema=DAILY_BOOK_SCHEMA)


def daily_book_legacy_batch(batch) -> pa.Table:
    """Internal ABI adapter; never serialize these compatibility names."""
    values = {name: batch[name] for name in batch.schema.names}
    n = len(batch)
    values.update(source_id=values["stream_id"], source_native_sequence=values["native_sequence"],
                  source_observed_timestamp_us=values["observed_timestamp_us"],
                  source_timestamp_us=values["original_timestamp_us"], quantity=values["amount"],
                  event_type=pc.if_else(values["is_snapshot"], "snapshot", "update"),
                  fusion_reason=pc.if_else(values["observation_only"], "observation_refresh",
                      pc.if_else(values["queue_rebase"], "source_switch", "source_observation")))
    return pa.Table.from_arrays([values.get(f.name, pa.nulls(n, f.type)) for f in UNION_BOOK_SCHEMA], schema=UNION_BOOK_SCHEMA)


def _pack_book_state(state: Mapping | None, priority: Mapping, *, selected_view: bool) -> dict | None:
    if state is None:
        return None
    kernel = copy.deepcopy(state["kernel"])
    ids = priority["stream_ids"]
    if selected_view:
        # Keep the final selected view, never re-invent the discarded source
        # histories as if the differential rows could reconstruct them.
        selected = int(kernel.get("selected_source", -1))
        streams = kernel["sources"]
        source = copy.deepcopy(streams[selected]) if selected >= 0 else copy.deepcopy(streams[0])
        source.update(levels=copy.deepcopy(kernel["global_levels"]),
                      observed_us=kernel["global_observed_us"], local_us=kernel["global_local_us"],
                      presentation_us=kernel["presentation_us"],
                      initialized=bool(kernel["global_levels"]), bridge=False, last_id=-1)
        if source.get("last_message"):
            source["last_message"][4] = 0
        kernel.update(sources=[source], source_count=1, preferred=0,
                      selected_source=0 if kernel["global_levels"] else -1,
                      view_source=0 if kernel["global_levels"] else -1)
        ids = ["s0"]
    else:
        ids = [f"s{i}" for i in range(len(ids))]
    for key in ("schema", "raw_diff_output", "legacy_continuation_seed"):
        kernel.pop(key, None)
    kernel["streams"] = kernel.pop("sources")
    kernel["stream_count"] = kernel.pop("source_count")
    kernel["selected_stream"] = kernel.pop("selected_source")
    kernel["view_stream"] = kernel.pop("view_source", -1)
    packed = {"symbol": state["symbol"], "next_day_start_us": state["next_day_start_us"],
              "stream_priority": book_stream_priority(ids, preferred_index=kernel["preferred"]),
              "state": kernel, "pending_rows": {}, "pending_observations": {}}
    if not selected_view:
        for i, old in enumerate(priority["stream_ids"]):
            key = f"s{i}"
            packed["pending_rows"][key] = copy.deepcopy(state.get("deferred_rows_by_source", {}).get(old, []))
            rows = state.get("deferred_observations_by_source", {}).get(old, [])
            packed["pending_observations"][key] = (project_daily_book_batch(
                pa.Table.from_pylist(rows, schema=UNION_BOOK_SCHEMA), BOOK_UNION_SCHEMA, priority).to_pylist()
                if rows else [])
    return packed


def daily_book_continuation(state: Mapping | None) -> dict | None:
    """Restore the numeric kernel ABI from a supplier-neutral state payload."""
    if state is None:
        return None
    priority = book_stream_priority(contract=state["stream_priority"])
    kernel = copy.deepcopy(state["state"])
    kernel.update(schema="book_fusion.continuation.v2", raw_diff_output=False,
                  legacy_continuation_seed=False, sources=kernel.pop("streams"),
                  source_count=kernel.pop("stream_count"), selected_source=kernel.pop("selected_stream"),
                  view_source=kernel.pop("view_stream"))
    rows = {}
    for identity, observations in state.get("pending_observations", {}).items():
        rows[identity] = (daily_book_legacy_batch(pa.Table.from_pylist(observations, schema=DAILY_BOOK_SCHEMA)).to_pylist()
                          if observations else [])
    return {"symbol": state["symbol"], "next_day_start_us": state["next_day_start_us"],
            "source_ids": priority["stream_ids"], "stream_priority": priority, "kernel": kernel,
            "deferred_rows_by_source": copy.deepcopy(state.get("pending_rows", {})),
            "deferred_observations_by_source": rows}


def unify_orderbook_day(source: Path, output: Path, day: str, *, symbol: str = "BTCUSDC",
                       normalized: Mapping | None = None, top_supplements: pa.Table | None = None,
                       top_initial_state: Mapping | None = None, top_final_state: Mapping | None = None,
                       initial_state: Mapping | None = None, final_state: Mapping | None = None,
                       consumed_inputs: Sequence[Mapping] | None = None) -> dict:
    """Bounded physical rewrite; verify every projected row before atomic cutover."""
    source, output = Path(source), Path(output)
    _reject_output_symlinks(output)
    start, end = _day_bounds(day)
    before = _identity(source)
    parquet = pq.ParquetFile(source)
    metadata = parquet.metadata.metadata or {}
    marker = metadata.get(b"narrowgate.book_fusion", b"").decode()
    if marker not in (BOOK_FUSION_SCHEMA, BOOK_UNION_SCHEMA, DAILY_BOOK_MARKER):
        raise ValueError("daily unification requires an identified existing book")
    if metadata.get(b"narrowgate.day", day.encode()) != day.encode():
        raise ValueError("daily book UTC date mismatch")
    if marker == DAILY_BOOK_MARKER:
        receipt = daily_book_receipt(source)
        priority = receipt["stream_priority"]
    else:
        old = json.loads(metadata.get(b"narrowgate.fusion_receipt", b"{}"))
        priority = _legacy_book_priority(old)
        stats = old.get("stats", {})
        selected = marker == BOOK_FUSION_SCHEMA
        receipt = {"schema": DAILY_BOOK_MARKER, "day": day, "symbol": symbol,
            "stream_priority": book_stream_priority(["s0"] if selected else [f"s{i}" for i in range(len(priority["stream_ids"]))],
                                                    preferred_index=0 if selected else priority["preferred_index"]),
            "normalized": {kind: {key: value[key] for key in ("rows", "sha256") if key in value}
                           for kind, value in stats.get("normalized", {}).items() if kind in ("bbo", "l2", "clock")},
            "initial_state": _pack_book_state(old.get("initial_continuation"), priority, selected_view=selected),
            "final_state": _pack_book_state(stats.get("continuation"), priority, selected_view=selected)}
    for name, value in (("initial_state", initial_state), ("final_state", final_state)):
        if value is not None:
            receipt[name] = copy.deepcopy(dict(value))
    if consumed_inputs is not None:
        receipt["consumed_inputs"] = copy.deepcopy(list(consumed_inputs))
    if normalized is not None:
        if set(normalized) != {"bbo", "l2", "clock"}:
            raise ValueError("normalized identities require bbo/l2/clock")
        for value in normalized.values():
            if (set(value) != {"rows", "sha256"} or type(value["rows"]) is not int or value["rows"] < 0
                    or not isinstance(value["sha256"], str) or len(value["sha256"]) != 64
                    or any(c not in "0123456789abcdef" for c in value["sha256"])):
                raise ValueError("invalid normalized identity")
        receipt["normalized"] = copy.deepcopy(dict(normalized))
    if (receipt.get("day"), receipt.get("symbol")) != (day, symbol):
        raise ValueError("daily book receipt scope mismatch")
    for name, value, boundary in (("top_initial_state", top_initial_state, start),
                                  ("top_final_state", top_final_state, end)):
        value = value if value is not None else receipt.get(name)
        if value is None:
            receipt[name] = None
            continue
        from data.book_top import validate_top_state
        receipt[name] = validate_top_state(value, symbol=symbol, boundary_us=boundary)
    for name, boundary in (("initial_state", start), ("final_state", end)):
        state = receipt.get(name)
        if state is None:
            continue
        if (state.get("symbol") != symbol or state.get("next_day_start_us") != boundary
                or state.get("stream_priority") != receipt["stream_priority"]):
            raise ValueError("daily book state boundary/stream mismatch")
        kernel = state["state"]
        if (kernel["global_observed_us"] > boundary or kernel["presentation_us"] > boundary
                or any(value["observed_us"] > boundary or value["presentation_us"] > boundary
                       for value in kernel["streams"])):
            raise ValueError("daily book state contains future observations")
    ranks = book_stream_ranks(receipt["stream_priority"])
    previous = start
    top_rows = 0
    def batches():
        nonlocal previous, top_rows
        for batch in parquet.iter_batches(batch_size=65_536):
            table = project_daily_book_batch(batch, marker, priority)
            for key, expected in (("exchange", "binance-futures"), ("symbol", symbol)):
                if table[key].null_count or pc.any(pc.not_equal(table[key], expected)).as_py():
                    raise ValueError(f"daily book {key} mismatch")
            timestamps = table["timestamp"].combine_chunks().to_numpy()
            if (len(timestamps) and (timestamps[0] < previous or timestamps[-1] >= end
                                    or (timestamps[1:] < timestamps[:-1]).any())):
                raise ValueError("daily book timestamp order/scope mismatch")
            if len(timestamps):
                previous = int(timestamps[-1])
            observed = table["observed_timestamp_us"]
            if (observed.null_count or pc.any(pc.less_equal(observed, 0)).as_py()
                    or pc.any(pc.greater(observed, table["timestamp"])).as_py()):
                raise ValueError("daily book missing/future observation")
            if table["top_only"].null_count:
                raise ValueError("daily book top-only declaration is missing")
            top = table.filter(table["top_only"])
            top_rows += len(top)
            if len(top) and (pc.any(top["native_sequence"]).as_py() or pc.any(top["is_snapshot"]).as_py()
                            or pc.any(top["queue_rebase"]).as_py()
                            or pc.any(pc.invert(top["observation_only"])).as_py()):
                raise ValueError("top-only observations cannot reset deep books or claim queue/sequence progress")
            deep = table.filter(pc.invert(table["top_only"]))
            for stream in pc.unique(deep["stream_id"]).to_pylist():
                if stream not in ranks:
                    raise ValueError("daily book undeclared stream")
                part = deep.filter(pc.equal(deep["stream_id"], stream))
                if pc.any(pc.not_equal(part["stream_priority"], ranks[stream])).as_py():
                    raise ValueError("daily book stream rank drift")
            yield table
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".partial", dir=output.parent)
    os.close(fd)
    temporary = Path(name)
    def complete(rows):
        receipt["output_rows"] = rows
        receipt["top_rows"] = top_rows + (len(top_supplements) if top_supplements is not None else 0)
        return {b"narrowgate.book_receipt": json.dumps(receipt, sort_keys=True).encode()}
    try:
        projected = batches()
        if top_supplements is not None:
            if not _schema_matches(top_supplements.schema, DAILY_BOOK_SCHEMA):
                raise ValueError("top supplements require the unified daily book schema")
            for field, expected in (("top_only", True), ("observation_only", True),
                                    ("native_sequence", False), ("is_snapshot", False),
                                    ("queue_rebase", False), ("exchange", "binance-futures"), ("symbol", symbol)):
                if (top_supplements[field].null_count or
                        pc.any(pc.not_equal(top_supplements[field], expected)).as_py()):
                    raise ValueError("invalid top-only supplement semantics")
            if len(top_supplements):
                clock, observed = top_supplements["timestamp"], top_supplements["observed_timestamp_us"]
                if (clock.null_count or observed.null_count or pc.any(pc.less(clock, start)).as_py()
                        or pc.any(pc.greater_equal(clock, end)).as_py()
                        or pc.any(pc.less_equal(observed, 0)).as_py()
                        or pc.any(pc.greater(observed, clock)).as_py()):
                    raise ValueError("top supplement contains invalid/future observation")
            from data.book_top import merge_book_rows
            projected = merge_book_rows(projected, top_supplements)
        rows, digest = _write_verified_tables(projected, temporary, DAILY_BOOK_SCHEMA,
            {b"narrowgate.schema": DAILY_BOOK_MARKER.encode(), b"narrowgate.book_fusion": DAILY_BOOK_MARKER.encode(),
             b"narrowgate.day": day.encode(), b"narrowgate.channel": b"incremental_book_L2",
             b"narrowgate.timestamp_unit": b"exchange_microseconds"}, final_metadata=complete)
        if _identity(source) != before:
            raise ValueError("daily book changed during unification")
        _publish_atomic(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return {"schema": DAILY_BOOK_MARKER, "day": day, "path": str(output), "rows": rows,
            "sha256": digest, "status": "PUBLISHED", "receipt": receipt}


def _daily_supplement_seed(receipt: Mapping, day: str, stream_id: str,
                           previous_state: Mapping | None, minimum_levels: int) -> tuple[dict, dict]:
    """Add one empty anonymous slot; never reinterpret an existing slot."""
    import narrowgate_cpp

    priority = book_stream_priority(contract=receipt["stream_priority"])
    old_ids = priority["stream_ids"]
    if old_ids != [f"s{i}" for i in range(len(old_ids))]:
        raise ValueError("daily supplementation requires explicit consecutive anonymous slots")
    if stream_id not in old_ids and stream_id != f"s{len(old_ids)}":
        raise ValueError("supplement stream must be the next anonymous slot")
    ids = old_ids if stream_id in old_ids else [*old_ids, stream_id]
    expanded = book_stream_priority(ids, preferred_index=priority["preferred_index"])
    start, _ = _day_bounds(day)
    kernel = narrowgate_cpp.BookFusion(len(ids), expanded["preferred_index"], minimum_levels)
    kernel.configure_raw_diff_output(False)
    empty = kernel.continuation()
    if previous_state is not None:
        seed = daily_book_continuation(previous_state)
        if (seed["source_ids"] != ids or seed["stream_priority"] != expanded
                or seed["next_day_start_us"] != start or seed["symbol"] != receipt["symbol"]):
            raise ValueError("supplement continuation requires adjacent day and identical stream mapping/symbol")
    elif receipt.get("initial_state") is not None:
        seed = daily_book_continuation(receipt["initial_state"])
        if (seed["source_ids"] != old_ids or seed["stream_priority"] != priority
                or seed["next_day_start_us"] != start or seed["symbol"] != receipt["symbol"]):
            raise ValueError("current daily initial state disagrees with its stream mapping/boundary")
        if ids != old_ids:
            seed["kernel"]["sources"].append(copy.deepcopy(empty["sources"][-1]))
            seed["kernel"]["source_count"] = len(ids)
            seed.update(source_ids=ids, stream_priority=expanded)
            seed["deferred_rows_by_source"][stream_id] = []
            seed["deferred_observations_by_source"][stream_id] = []
    else:
        seed = {"symbol": receipt["symbol"], "next_day_start_us": start,
                "source_ids": ids, "stream_priority": expanded, "kernel": empty,
                "deferred_rows_by_source": {}, "deferred_observations_by_source": {}}
    kernel.restore(seed["kernel"])  # Existing kernel validates full state before any file is written.
    return expanded, _pack_book_state(seed, expanded, selected_view=False)


def _daily_supplement_seam_seed(receipt: Mapping, day: str, stream_id: str,
                                previous_state: Mapping, previous_stream_id: str,
                                logical_stream_id: str, minimum_levels: int) -> tuple[dict, dict, dict]:
    """Opt-in layout seam: today's base is authoritative; only the named extra stream migrates.

    A selected-view s0 is not one of the old independent base sources. No old
    base source is renamed into today's layout and no synthetic observation is
    pushed into BookFusion to make it choose a view.
    """
    import narrowgate_cpp

    if not isinstance(logical_stream_id, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", logical_stream_id):
        raise ValueError("supplement seam requires a stable anonymous logical stream identity")
    previous = daily_book_continuation(previous_state)
    start, _ = _day_bounds(day)
    prior_ids = previous["source_ids"]
    current_ids = receipt["stream_priority"]["stream_ids"]
    current_supplemented = stream_id in current_ids
    if current_supplemented:
        contract = receipt.get("supplementation_contract", {})
        expected_binding = {"logical_stream_id": logical_stream_id, "stream_id": stream_id}
        if (current_ids != [f"s{i}" for i in range(len(current_ids))]
                or stream_id != current_ids[-1]
                or contract.get("schema") != "daily_book_supplement.v1"
                or any(contract.get(key) != value for key, value in expected_binding.items())
                or (receipt.get("initial_state") or {}).get("supplementation") != expected_binding):
            raise ValueError("supplemented seam requires the same explicitly bound logical capture")
    base_count = len(current_ids) - int(current_supplemented)
    if {len(prior_ids) - 1, base_count} != {1, 3}:
        raise ValueError("supplement seam only supports the explicit one/three base-slot layouts")
    if (len(prior_ids) < 2 or previous_stream_id != prior_ids[-1]
            or prior_ids != [f"s{i}" for i in range(len(prior_ids))]
            or previous["next_day_start_us"] != start or previous["symbol"] != receipt["symbol"]):
        raise ValueError("supplement seam requires the adjacent explicitly mapped last slot")
    binding = previous_state.get("supplementation")
    if current_supplemented and binding is None:
        raise ValueError("supplemented seam requires an explicitly bound previous capture")
    if binding is not None and binding != {"logical_stream_id": logical_stream_id, "stream_id": previous_stream_id}:
        raise ValueError("supplement seam logical identity/previous slot mismatch")
    priority, initial = _daily_supplement_seed(receipt, day, stream_id, None, minimum_levels)
    target = daily_book_continuation(initial)
    if previous["stream_priority"] == priority:
        raise ValueError("same-layout continuation must use the normal full-state path")
    old_index, new_index = prior_ids.index(previous_stream_id), priority["stream_ids"].index(stream_id)

    def validate(kernel, count, preferred):
        if (kernel["presentation_us"] > start or kernel["global_observed_us"] > start
                or kernel.get("last_sample_observed_us", 0) > start
                or any(item["observed_us"] > start or item["presentation_us"] > start
                       for item in kernel["sources"])):
            raise ValueError("supplement seam state contains a future observation/presentation")
        verifier = narrowgate_cpp.BookFusion(count, preferred, minimum_levels)
        verifier.configure_raw_diff_output(False)
        verifier.restore(kernel)

    validate(previous["kernel"], len(prior_ids), previous["kernel"]["preferred"])
    validate(target["kernel"], len(priority["stream_ids"]), priority["preferred_index"])
    if current_supplemented and target["kernel"]["selected_source"] == new_index:
        # The old publication's capture state is about to be replaced. Keep
        # its true clock as a no-older-fallback floor, not as a selected source
        # whose levels/clock would disagree with the newly inherited state.
        target["kernel"].update(selected_source=-1, view_source=-1)
    target["kernel"]["sources"][new_index] = copy.deepcopy(previous["kernel"]["sources"][old_index])
    target["deferred_rows_by_source"][stream_id] = copy.deepcopy(
        previous["deferred_rows_by_source"].get(previous_stream_id, []))
    deferred = copy.deepcopy(previous["deferred_observations_by_source"].get(previous_stream_id, []))
    for row in deferred:
        if row.get("source_id") != previous_stream_id:
            raise ValueError("supplement seam deferred observation has a different source identity")
        row["source_id"] = stream_id
    if len(deferred) != len(target["deferred_rows_by_source"][stream_id]):
        raise ValueError("supplement seam lacks original deferred observations")
    target["deferred_observations_by_source"][stream_id] = deferred
    kernel, prior_kernel = target["kernel"], previous["kernel"]
    supplemental = kernel["sources"][new_index]
    kernel["presentation_us"] = max(kernel["presentation_us"], supplemental["presentation_us"],
                                    prior_kernel["presentation_us"])
    # A layout seam must not undo BookFusion's no-older-fallback rule. When
    # today's base cannot identify the previous selected base view, keep its
    # real clock/levels as an unsampled floor, not as a fabricated new source.
    # Normalized output remains absent until a mapped state reaches that clock.
    if prior_kernel["global_observed_us"] > kernel["global_observed_us"]:
        for key in ("global_levels", "global_observed_us", "global_local_us", "last_sample_observed_us"):
            kernel[key] = copy.deepcopy(prior_kernel[key])
        kernel.update(selected_source=-1, view_source=-1)
    candidates = []

    # A previous selected supplement can itself have become invalid while its
    # last valid view was retained as aged. Preserve that view and its clocks.
    if prior_kernel["selected_source"] == old_index and prior_kernel["global_levels"]:
        candidate = copy.deepcopy(kernel)
        for key in ("global_levels", "global_observed_us", "global_local_us", "last_sample_observed_us"):
            candidate[key] = copy.deepcopy(prior_kernel[key])
        candidate.update(selected_source=new_index,
                         view_source=new_index if prior_kernel["view_source"] == old_index else -1)
        validate(candidate, len(priority["stream_ids"]), priority["preferred_index"])
        candidates.append(candidate)

    # BookFusion.restore provides the existing full-depth/spread validity check.
    # An invalid supplemental source is not promoted into a valid observation.
    candidate = copy.deepcopy(kernel)
    candidate.update(global_levels=copy.deepcopy(supplemental["levels"]),
                     global_observed_us=supplemental["observed_us"],
                     global_local_us=supplemental["local_us"], selected_source=new_index,
                     view_source=new_index,
                     last_sample_observed_us=prior_kernel.get("last_sample_observed_us", 0))
    try:
        validate(candidate, len(priority["stream_ids"]), priority["preferred_index"])
    except ValueError as exc:
        if "invalid fusion checkpoint selected view" not in str(exc):
            raise
    else:
        candidates.append(candidate)
    selected = "current_canonical_initial" if kernel["selected_source"] >= 0 else "unknown_until_observation"
    for candidate in candidates:
        if (candidate["global_observed_us"] > kernel["global_observed_us"]
                or (candidate["global_observed_us"] == kernel["global_observed_us"]
                    and kernel["selected_source"] < 0 and candidate["global_levels"])):
            kernel = candidate
            selected = "carried_supplement"
    target["kernel"] = kernel
    validate(kernel, len(priority["stream_ids"]), priority["preferred_index"])
    transition = {
        "schema": "daily_book_supplement.seam.v1", "logical_stream_id": logical_stream_id,
        "previous_stream_id": previous_stream_id, "stream_id": stream_id,
        "previous_priority": previous["stream_priority"], "current_priority": priority,
        "base_seed": "current_canonical_initial" if receipt.get("initial_state") is not None else "unknown_no_initial_state",
        "opening_view": selected, "source_observation_created": False,
        "queue_progress_created": False, "base_source_identity_inherited": False,
        "current_supplement_reused": current_supplemented,
        "last_global_observation_floor_us": prior_kernel["global_observed_us"],
    }
    return priority, _pack_book_state(target, priority, selected_view=False), transition


def _merge_daily_streams(left, right):
    """Bounded, stable merge: old rows win equal timestamps, neither input is deduplicated."""
    import numpy as np

    iterators = [iter(left), iter(right)]
    current = [next(iterator, None) for iterator in iterators]
    while any(table is not None for table in current):
        limit = min(table["timestamp"][-1].as_py() for table in current if table is not None)
        # A source batch may split an arbitrarily long same-clock message.
        # Hold the new stream's boundary rows until every old boundary row
        # has passed, preserving old-before-new ties across batch boundaries.
        hold_right_boundary = current[0] is not None and current[0]["timestamp"][-1].as_py() == limit
        parts = []
        for index, table in enumerate(current):
            if table is None:
                continue
            side = "left" if index == 1 and hold_right_boundary else "right"
            count = int(np.searchsorted(table["timestamp"].to_numpy(), limit, side=side))
            if count:
                parts.append(table.slice(0, count))
            current[index] = (table.slice(count) if count < len(table)
                              else next(iterators[index], None))
        merged = pa.concat_tables(parts)
        order = np.argsort(merged["timestamp"].to_numpy(), kind="stable")
        yield merged.take(pa.array(order))


def supplement_capture_contract(capture_mode: str, stream_id: str,
                                logical_stream_id: str | None = None) -> dict | None:
    """Freeze the existing fusion clock, not a receiver-clock approximation."""
    if capture_mode == "tardis":
        return None  # Keep historical Tardis consumption/continuation bytes unchanged.
    if capture_mode != "binance_futures_native":
        raise ValueError("unsupported supplement capture mode")
    return {"schema": "daily_book_native_capture.v1", "capture_mode": capture_mode,
            "symbol": "BTCUSDC", "stream_id": stream_id,
            "logical_stream_id": logical_stream_id,
            "clock": "exchange_event_ms_then_transaction_ms",
            "recorder_snapshot_clock": "reject_unanchored_hour_rounded"}


def _native_supplement_clock(batch, selection: dict):
    """Validate actual native messages; never promote deltas into snapshots.

    Match iter_fused_book_batches' event-first contract. Original E/T remain
    present, and exact fallback row ranges make mixed clocks inspectable.
    Rounded recorder snapshots need a separately proven sequence-clock anchor;
    the event-first fusion path does not implement the transaction-only anchor
    used by the legacy Crypto normalizer, so reject rather than bypass it.
    """
    import numpy as np

    def integers(name):
        return pc.fill_null(batch[name], -1).to_numpy(zero_copy_only=False)

    snapshot = batch["is_snapshot"].to_numpy(zero_copy_only=False)
    kinds = pc.utf8_lower(batch["event_type"])
    if (kinds.null_count or not pc.all(pc.is_in(kinds, value_set=pa.array(["snapshot", "update"]))).as_py()
            or not pc.all(pc.equal(pc.equal(kinds, "snapshot"), batch["is_snapshot"])).as_py()):
        raise ValueError("native event_type and snapshot declaration disagree")
    event, transaction = integers("event_time"), integers("transaction_time")
    if np.any(event >= 10**14) or np.any(transaction >= 10**14):
        raise ValueError("native E/T must be milliseconds")
    real = np.where(event > 0, event, transaction) * 1000
    if np.any(real <= 0):
        raise ValueError("native capture lacks a real exchange E/T clock; receive fallback forbidden")
    if np.any(snapshot & (transaction <= 0) & (event > 0) & (event % 3_600_000 == 0)):
        raise ValueError("hour-rounded recorder snapshot lacks a proven sequence-clock anchor")
    first, final, previous, last = (integers(name) for name in (
        "first_update_id", "final_update_id", "prev_final_update_id", "last_update_id"))
    if np.any(~snapshot & ((first < 0) | (final < first) | (previous < 0))):
        raise ValueError("native delta requires real U/u/pu sequence IDs")
    if np.any(snapshot & (np.where(last >= 0, last, final) < 0)):
        raise ValueError("native snapshot requires a real lastUpdateId")
    fallback = event <= 0
    offset, n = selection["input_rows"], len(batch)
    changes = np.flatnonzero(np.diff(np.r_[False, fallback, False].astype(np.int8)))
    for left, right in changes.reshape(-1, 2):
        ranges = selection["transaction_fallback_row_ranges"]
        if ranges and ranges[-1][1] == offset + int(left):
            ranges[-1][1] = offset + int(right)
        else:
            ranges.append([offset + int(left), offset + int(right)])
    selection["input_rows"] += n
    selection["event_rows"] += int((~fallback).sum())
    selection["transaction_fallback_rows"] += int(fallback.sum())
    return real


def supplement_daily_book_day(current: Path, supplement: Path | None, output: Path, day: str, *,
                              stream_id: str, previous_state: Mapping | None = None,
                              normalized_root: Path, minimum_levels: int = 20,
                              previous_top_state: Mapping | None = None,
                              logical_stream_id: str | None = None,
                              previous_stream_id: str | None = None,
                              capture_mode: str = "tardis", write_workers: int = 1,
                              write_max_pending_bytes: int = 32 * 1024**2) -> dict:
    """Stage a unified daily book plus one explicitly identified real L2 stream.

    The current 24-field rows (including selected-view patches and top-only
    observations) pass through unchanged. The additional input is a same-market
    BOOK_SCHEMA capture, not a previously reconstructed view. ``previous_state``
    is the prior successful DAILY receipt's packed final_state. A missing input
    day can carry that state without inventing observations. Publication into
    current indexes and retirement are deliberately owned by the caller's daily
    transaction; current raw/derived files are never replaced here.

    ``logical_stream_id`` plus ``previous_stream_id`` explicitly opts into a
    one/three-base-slot seam. They identify only the added capture stream;
    baseline slots are restored from today's own canonical initial state.
    ``binance_futures_native`` explicitly retains native snapshot/delta sequence
    validation and the existing fusion E-then-T clock. It never borrows a base
    book to initialize the new stream, including when its first file is deltas.
    """
    import numpy as np
    from data.normalize_tardis_orderbook import iter_fused_book_batches

    current, output, normalized_root = Path(current), Path(output), Path(normalized_root)
    supplement = Path(supplement) if supplement is not None else None
    capture_contract = supplement_capture_contract(capture_mode, stream_id, logical_stream_id)
    native = capture_contract is not None
    _reject_output_symlinks(output)
    _reject_output_symlinks(normalized_root)
    if output.exists() or output.resolve() == current.resolve():
        raise ValueError("supplementation output must be a new staging file")
    if normalized_root.exists() and any(normalized_root.iterdir()):
        raise ValueError("supplementation normalized root must be empty staging")
    start, end = _day_bounds(day)
    receipt = daily_book_receipt(current)
    current_metadata = pq.ParquetFile(current).metadata.metadata or {}
    if current_metadata.get(b"narrowgate.timestamp_unit", b"exchange_microseconds") != b"exchange_microseconds":
        raise ValueError("current daily book uses an incompatible clock")
    if (receipt.get("day"), receipt.get("symbol")) != (day, "BTCUSDC"):
        raise ValueError("daily supplement requires the matching BTCUSDC perpetual day")
    current_before, current_sha = _identity(current), sha256_file(current)
    consumed = copy.deepcopy(receipt.get("consumed_inputs", []))
    if not isinstance(consumed, list) or any(not isinstance(item, dict) for item in consumed):
        raise ValueError("invalid consumed input identities")
    existing_contract = receipt.get("supplementation_contract", {})
    if (existing_contract.get("stream_id") == stream_id
            and existing_contract.get("logical_stream_id") is not None
            and existing_contract["logical_stream_id"] != logical_stream_id):
        raise ValueError("existing supplement logical identity cannot change or be removed")
    if (existing_contract.get("stream_id") == stream_id
            and existing_contract.get("capture_mode", "tardis") != capture_mode):
        raise ValueError("existing supplement capture mode cannot change")
    if (native and stream_id in receipt["stream_priority"]["stream_ids"]
            and existing_contract.get("capture_contract") != capture_contract):
        raise ValueError("native supplement cannot borrow an existing unbound source slot")
    if previous_state is not None:
        previous_capture = previous_state.get("capture_contract")
        expected_capture = supplement_capture_contract(
            capture_mode, previous_stream_id or stream_id, logical_stream_id)
        if previous_capture != expected_capture:
            raise ValueError("supplement continuation capture mode/symbol/clock identity differs")
    incoming = None
    reused = False
    if supplement is not None:
        if supplement.resolve() == current.resolve():
            raise ValueError("the unified book cannot be its own independent supplement")
        parquet = pq.ParquetFile(supplement)
        if (not _schema_matches(parquet.schema_arrow, BOOK_SCHEMA)
                or (parquet.metadata.metadata or {}).get(b"narrowgate.book_fusion")):
            raise ValueError("supplement requires a real BOOK_SCHEMA capture, not a fused book")
        incoming = {"sha256": sha256_file(supplement), "rows": parquet.metadata.num_rows,
                    "schema": SCHEMA_VERSION, "day": day, "symbol": "BTCUSDC", "stream_id": stream_id}
        if native:
            incoming["capture_contract"] = capture_contract
        if not incoming["rows"]:
            raise ValueError("empty supplement is missing data, not an observation")
        same = [item for item in consumed if item.get("sha256") == incoming["sha256"]]
        if same and same != [incoming]:
            raise ValueError("consumed supplement identity/mapping differs")
        reused = bool(same)
        if not reused and stream_id in receipt["stream_priority"]["stream_ids"]:
            contract = receipt.get("supplementation_contract", {})
            if (contract.get("schema") != "daily_book_supplement.v1"
                    or contract.get("stream_id") != stream_id
                    or any(item.get("stream_id") == stream_id for item in consumed)):
                raise ValueError("a new capture cannot reuse an existing stream without an identity merge")
            for batch in pq.ParquetFile(current).iter_batches(columns=["stream_id"], batch_size=65_536):
                if pc.any(pc.equal(batch["stream_id"], stream_id)).as_py():
                    raise ValueError("a new capture cannot reuse an existing stream containing observations")
        supplement_before = _identity(supplement)
    seam = None
    if logical_stream_id is not None and (
        not isinstance(logical_stream_id, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", logical_stream_id)
    ):
        raise ValueError("supplement requires a stable anonymous logical stream identity")
    if (logical_stream_id is None) != (previous_stream_id is None) and previous_state is not None:
        raise ValueError("logical and previous supplement slot must be explicitly paired")
    binding = previous_state.get("supplementation") if previous_state is not None else None
    if binding is not None and binding != {"logical_stream_id": logical_stream_id, "stream_id": previous_stream_id}:
        raise ValueError("supplement continuation logical identity/previous slot mismatch")
    target_ids = receipt["stream_priority"]["stream_ids"]
    target_ids = target_ids if stream_id in target_ids else [*target_ids, stream_id]
    expected = book_stream_priority(target_ids, preferred_index=receipt["stream_priority"]["preferred_index"])
    changed_layout = previous_state is not None and previous_state["stream_priority"] != expected
    if changed_layout and logical_stream_id is not None and previous_stream_id is not None:
        priority, initial, seam = _daily_supplement_seam_seed(
            receipt, day, stream_id, previous_state, previous_stream_id, logical_stream_id, minimum_levels)
    else:
        if previous_stream_id is not None and previous_stream_id != stream_id:
            raise ValueError("same-layout supplement slot cannot be renamed")
        priority, initial = _daily_supplement_seed(receipt, day, stream_id, previous_state, minimum_levels)
    if native:
        source = initial["state"]["streams"][priority["stream_ids"].index(stream_id)]
        if source["initialized"] and (not source.get("last_message") or source["last_message"][4] != 1):
            raise ValueError("native supplement cannot initialize from a non-native book")
        pending = initial.get("pending_rows", {}).get(stream_id, [])
        if any(row[4] != 1 for row in pending):
            raise ValueError("native supplement continuation contains non-native pending rows")
        initial["capture_contract"] = capture_contract
    if incoming is not None and not reused:
        consumed.append(incoming)
    provisional = copy.deepcopy(receipt)
    provisional.update(stream_priority=priority, initial_state=initial, final_state=None,
                       normalized={}, consumed_inputs=consumed,
                       supplementation_contract={"schema": "daily_book_supplement.v1", "stream_id": stream_id})
    selection = {"input_rows": 0, "event_rows": 0, "transaction_fallback_rows": 0,
                 "transaction_fallback_row_ranges": [], "row_ranges": "zero_based_half_open_input_order"}
    if native:
        provisional["supplementation_contract"].update(capture_mode=capture_mode,
                                                       capture_contract=capture_contract)
        if reused:
            selection = copy.deepcopy(receipt["capture_clock_selection"])
        provisional["capture_clock_selection"] = selection
    if logical_stream_id is not None:
        supplement_binding = {"logical_stream_id": logical_stream_id, "stream_id": stream_id}
        initial["supplementation"] = supplement_binding
        provisional["supplementation_contract"].update(supplement_binding)
    if seam is not None:
        provisional["seam_transition"] = seam
    if previous_top_state is not None:
        from data.book_top import validate_top_state
        provisional["top_initial_state"] = validate_top_state(previous_top_state, boundary_us=start)

    def old_rows():
        for batch in pq.ParquetFile(current).iter_batches(batch_size=65_536):
            yield project_daily_book_batch(batch, DAILY_BOOK_MARKER, priority)

    def new_rows():
        if supplement is None or reused:
            return
        previous = start
        rank = book_stream_ranks(priority)[stream_id]
        for batch in pq.ParquetFile(supplement).iter_batches(batch_size=65_536):
            for field, expected in (("exchange", "binance-futures"), ("symbol", "BTCUSDC")):
                if batch[field].null_count or pc.any(pc.not_equal(batch[field], expected)).as_py():
                    raise ValueError("supplement market/symbol mismatch")
            if batch["is_snapshot"].null_count or (not native and batch["timestamp"].null_count):
                raise ValueError("supplement is missing an exchange clock or snapshot declaration")
            observed = _native_supplement_clock(batch, selection) if native else batch["timestamp"].to_numpy()
            # Capture partitions can arrive out of exchange order. Presentation
            # is causal prefix-max; the genuine exchange timestamp stays intact.
            if np.any(observed <= 0) or np.any(observed >= end):
                raise ValueError("supplement exchange clock is outside the declared UTC day")
            presentation = np.maximum.accumulate(np.maximum(observed, previous))
            previous = int(presentation[-1])
            values = {name: batch[name] for name in DAILY_BOOK_SCHEMA.names if name in batch.schema.names}
            n = len(batch)
            values.update(timestamp=pa.array(presentation), stream_id=pa.repeat(stream_id, n),
                stream_priority=pa.repeat(pa.scalar(rank, pa.int16()), n),
                queue_rebase=batch["is_snapshot"], observation_only=pa.repeat(False, n),
                top_only=pa.repeat(False, n), native_sequence=pa.repeat(native, n),
                observed_timestamp_us=pa.array(observed), original_timestamp_us=pa.array(observed))
            yield pa.Table.from_arrays([values.get(f.name, pa.nulls(n, f.type)) for f in DAILY_BOOK_SCHEMA],
                                       schema=DAILY_BOOK_SCHEMA)

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".daily-supplement-", dir=output.parent) as name:
        stage = Path(name)
        merged = stage / "merged.parquet"
        metadata = {b"narrowgate.schema": DAILY_BOOK_MARKER.encode(),
                    b"narrowgate.book_fusion": DAILY_BOOK_MARKER.encode(), b"narrowgate.day": day.encode(),
                    b"narrowgate.channel": b"incremental_book_L2", b"narrowgate.timestamp_unit": b"exchange_microseconds"}
        def footer(count):
            provisional["output_rows"] = count
            return {b"narrowgate.book_receipt": json.dumps(provisional, sort_keys=True).encode()}
        _write_verified_tables(_merge_daily_streams(old_rows(), new_rows()), merged,
                               DAILY_BOOK_SCHEMA, metadata, final_metadata=footer)
        validated = stage / "validated.parquet"
        unify_orderbook_day(merged, validated, day)
        batches, stats = iter_fused_book_batches({"canonical": validated}, day,
            observed_union=True, normalized_root=stage / "derived", minimum_levels=minimum_levels,
            write_workers=write_workers, write_max_pending_bytes=write_max_pending_bytes)
        for _ in batches:
            pass  # Raw ABI projection is NOT the publication: retain the original DAILY rows.
        final = _pack_book_state(stats["continuation"], priority, selected_view=False)
        if native:
            final["capture_contract"] = capture_contract
        if logical_stream_id is not None:
            final["supplementation"] = supplement_binding
        normalized = {kind: {"rows": value["rows"], "sha256": value["sha256"]}
                      for kind, value in stats["normalized"].items()}
        if _identity(current) != current_before or sha256_file(current) != current_sha:
            raise ValueError("current daily book changed during supplementation")
        if supplement is not None and (_identity(supplement) != supplement_before
                                        or sha256_file(supplement) != incoming["sha256"]):
            raise ValueError("supplement changed during reconstruction")
        # These are new staging files only. The caller's existing transaction
        # owns their atomic cutover with raw, quality, indexes and previous-use.
        normalized_root.mkdir(parents=True, exist_ok=True)
        for kind, value in stats["normalized"].items():
            target = normalized_root / kind / Path(value["path"]).name
            target.parent.mkdir(parents=True, exist_ok=True)
            _publish_atomic(Path(value["path"]), target)
            value["path"] = str(target)
        result = unify_orderbook_day(validated, output, day, normalized=normalized,
            initial_state=initial, final_state=final, consumed_inputs=consumed,
            top_initial_state=stats.get("top_initial_state"), top_final_state=stats.get("top_final_state"))
    result.update(status="STAGED", stats=stats, initial_state=initial, final_state=final,
        top_initial_state=stats.get("top_initial_state"), top_final_state=stats.get("top_final_state"),
        top_metrics=stats.get("top_metrics"), consumed_inputs=consumed, input_reused=reused)
    if native:
        result.update(capture_contract=capture_contract, capture_clock_selection=selection)
    if seam is not None:
        result["seam_transition"] = seam
    return result
TRADE_SCHEMA = pa.schema([
    ("exchange", pa.string()), ("symbol", pa.string()),
    ("timestamp", pa.int64()), ("local_timestamp", pa.int64()),
    ("id", pa.int64()), ("side", pa.string()), ("price", pa.string()),
    ("amount", pa.string()), ("quote_qty", pa.string()),
    ("time", pa.int64()), ("qty", pa.string()), ("is_buyer_maker", pa.bool_()),
])
AGG_SCHEMA = pa.schema([
    ("exchange", pa.string()), ("symbol", pa.string()),
    ("timestamp", pa.int64()), ("local_timestamp", pa.int64()),
    ("agg_trade_id", pa.int64()), ("first_trade_id", pa.int64()),
    ("last_trade_id", pa.int64()), ("price", pa.string()),
    ("quantity", pa.string()), ("transact_time", pa.int64()),
    ("is_buyer_maker", pa.bool_()),
])
FUNDING_SCHEMA = pa.schema([
    ("symbol", pa.string()), ("fundingTime", pa.int64()),
    ("fundingRate", pa.string()), ("markPrice", pa.string()),
    ("timestamp", pa.int64()), ("exchange", pa.string()),
])
SCHEMAS = dict(zip(CHANNEL_SOURCES, (BOOK_SCHEMA, TRADE_SCHEMA, AGG_SCHEMA, FUNDING_SCHEMA), strict=True))


def book_stream_priority(
    stream_ids: Sequence[str] | None = None, *, preferred_index: int | None = None,
    contract: Mapping | None = None,
) -> dict:
    """Freeze anonymous stream order and a preferred equal-clock stream.

    The numeric contract matches BookFusion: preferred wins an equal clock;
    otherwise the lowest stream slot wins. Supplier spellings occur only in
    this old-file compatibility default, never in the selection loop. Explicit
    contracts are independent of names and survive serialization/continuation.
    """
    if contract is not None:
        if contract.get("schema") != "book_stream_priority.v1":
            raise ValueError("unsupported book stream priority contract")
        if stream_ids is not None and list(stream_ids) != contract.get("stream_ids"):
            raise ValueError("book stream priority identities differ")
        if preferred_index is not None and preferred_index != contract.get("preferred_index"):
            raise ValueError("book stream priority preference differs")
        stream_ids, preferred_index = contract.get("stream_ids"), contract.get("preferred_index")
        if not isinstance(stream_ids, (list, tuple)):
            raise ValueError("book stream priority identities are missing")
    if isinstance(stream_ids, (str, bytes)):
        raise ValueError("book stream identities must be a sequence of identifiers")
    ids = list(stream_ids) if stream_ids is not None else ["cryptohft", "tardis", "canonical"]
    if (not ids or any(not isinstance(item, str) or not item for item in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("book stream identities must be nonempty and unique")
    if preferred_index is None:
        if contract is not None:
            raise ValueError("book stream priority preference is missing")
        preferred_index = ids.index("tardis") if "tardis" in ids else 0
    if type(preferred_index) is not int or not 0 <= preferred_index < len(ids):
        raise ValueError("book stream priority index is invalid")
    return {"schema": "book_stream_priority.v1", "stream_ids": ids,
            "preferred_index": preferred_index}


def book_stream_ranks(contract: Mapping) -> dict[str, int]:
    """Return unique numeric ranks; larger ranks win equal observation clocks."""
    value = book_stream_priority(contract=contract)
    ids, preferred = value["stream_ids"], value["preferred_index"]
    return {identity: 1 if index == preferred else -index - 1
            for index, identity in enumerate(ids)}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _identity(path: Path) -> tuple[int, int, int, int]:
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _reject_output_symlinks(output: Path) -> None:
    """Canonical destinations are real files/directories, not provider aliases.

    macOS exposes /var, /tmp and /etc through root-owned OS aliases. Those
    filesystem entry points do not make an otherwise physical dataset an alias;
    all dataset-level links, including broken output links, remain prohibited.
    Source paths intentionally have no corresponding restriction during import.
    """
    os_aliases = {Path(f"/{name}"): Path(f"/private/{name}") for name in ("var", "tmp", "etc")}
    absolute = output.absolute()
    for path in (absolute, *absolute.parents):
        if not path.is_symlink():
            continue
        expected = os_aliases.get(path)
        if expected is not None and path.lstat().st_uid == 0 and path.resolve() == expected:
            continue
        raise ValueError(f"canonical output path must not contain a symlink: {path}")


def _logical_digest(table: pa.Table) -> str:
    # Each bounded output group is verified. Equality includes string decimal
    # spelling and event order, not merely numeric totals or timestamp sets.
    table = table.replace_schema_metadata(None).combine_chunks()
    digest = hashlib.sha256(table.schema.serialize().to_pybytes())
    for field in table.schema:
        column = table[field.name].combine_chunks()
        # Arrow's unused values underneath null slots are not logical data and
        # can change after Parquet decoding. Hash validity independently and
        # replace those slots before serializing the actual values.
        digest.update(pc.is_null(column).to_numpy(zero_copy_only=False).tobytes())
        if column.null_count:
            fill = "" if pa.types.is_string(field.type) else False if pa.types.is_boolean(field.type) else 0
            column = pc.fill_null(column, pa.scalar(fill, type=field.type))
        digest.update(pa.record_batch([column], names=[field.name]).serialize())
    return digest.hexdigest()


def _schema_matches(actual: pa.Schema, expected: pa.Schema) -> bool:
    return actual.names == expected.names and all(
        actual.field(name).type == expected.field(name).type for name in expected.names
    )


def _write_verified_tables(batches, temporary: Path, schema: pa.Schema, metadata: dict,
                           final_metadata=None) -> tuple[int, str]:
    expected = []
    rows = 0
    with pq.ParquetWriter(temporary, schema.with_metadata(metadata), compression="zstd") as writer:
        for table in batches:
            if not table.num_rows:
                continue
            if isinstance(table, pa.RecordBatch):
                table = pa.Table.from_batches([table])
            # Arrow silently caps row_group_size at 64 Mi rows. Producer
            # batches can exceed that cap, so they cannot define verification
            # boundaries. Explicit small groups also bound readback/digest
            # memory while preserving every row and its original order.
            for offset in range(0, table.num_rows, 65_536):
                group = table.slice(offset, 65_536)
                expected.append((group.num_rows, _logical_digest(group)))
                writer.write_table(group, row_group_size=group.num_rows)
                rows += group.num_rows
        if final_metadata is not None:
            writer.add_key_value_metadata(final_metadata(rows))
    if not rows:
        raise ValueError("empty raw source cannot stand in for missing data")
    parquet = pq.ParquetFile(temporary)
    if parquet.metadata.num_rows != rows:
        raise ValueError(f"canonical row count mismatch: {parquet.metadata.num_rows} != {rows}")
    if parquet.num_row_groups != len(expected):
        raise ValueError(f"canonical row group count mismatch: {parquet.num_row_groups} != {len(expected)}")
    for index, (count, digest) in enumerate(expected):
        restored = parquet.read_row_group(index).cast(schema)
        if restored.num_rows != count or _logical_digest(restored) != digest:
            raise ValueError("canonical logical roundtrip mismatch")
    return rows, sha256_file(temporary)


def _publish_atomic(temporary: Path, output: Path) -> None:
    _reject_output_symlinks(output)
    with temporary.open("rb") as stream:
        os.fsync(stream.fileno())
    os.replace(temporary, output)
    directory = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _day_bounds(day: str) -> tuple[int, int]:
    parsed = date.fromisoformat(day)
    if parsed.isoformat() != day:
        raise ValueError("day must be YYYY-MM-DD")
    start = int(datetime.combine(parsed, time(), timezone.utc).timestamp()) * 1_000_000
    return start, start + 86_400_000_000


def _constant(value: str, rows: int) -> pa.Array:
    return pa.repeat(pa.scalar(value), rows)


def _adapt_batch(batch: pa.RecordBatch, channel: str, symbol: str, ordinal: int) -> pa.Table:
    rows = batch.num_rows
    values = {name: batch.column(index) for index, name in enumerate(batch.schema.names)}
    exchange = _constant("binance-futures", rows)
    expected_symbol = _constant(symbol, rows)
    if channel == "incremental_book_L2":
        if pc.any(pc.not_equal(values["symbol"], expected_symbol)).as_py():
            raise ValueError("raw book symbol mismatch")
        if pc.any(pc.not_equal(values["exchange"], exchange)).as_py():
            raise ValueError("raw book exchange mismatch")
        values.update({
            "quantity": values["amount"],
            "event_type": pc.if_else(values["is_snapshot"], "snapshot", "update"),
            "source_row": pa.array(range(ordinal, ordinal + rows), type=pa.int64()),
        })
        # Absent exchange-native T/E/update IDs remain NULL. timestamp and
        # local_timestamp retain the exact original microsecond values.
    else:
        values.update(exchange=exchange, symbol=expected_symbol)
        clock = "time" if channel == "trades" else "transact_time"
        values["timestamp"] = pc.multiply_checked(values[clock], 1000)
        if channel == "trades":
            values["amount"] = values["qty"]
            values["side"] = pc.if_else(values["is_buyer_maker"], "sell", "buy")
    schema = SCHEMAS[channel]
    arrays = [values.get(field.name, pa.nulls(rows, type=field.type)) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _csv_batches(source: Path, channel: str, symbol: str):
    if channel == "incremental_book_L2":
        types = {name: BOOK_SCHEMA.field(name).type for name in (
            "exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"
        )}
    elif channel == "trades":
        types = {name: TRADE_SCHEMA.field(name).type for name in (
            "id", "price", "qty", "quote_qty", "time", "is_buyer_maker"
        )}
    else:
        types = {name: AGG_SCHEMA.field(name).type for name in (
            "agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
            "transact_time", "is_buyer_maker"
        )}
    with pa.input_stream(source, compression=None) as raw:
        if source.suffix in {".zst", ".zstd"}:
            stream = pa.CompressedInputStream(raw, "zstd")
        elif source.suffix == ".xz":
            stream = lzma.LZMAFile(raw, "rb")
        else:
            stream = raw
        reader = pacsv.open_csv(
            stream,
            read_options=pacsv.ReadOptions(block_size=4 * 1024 * 1024, use_threads=False),
            convert_options=pacsv.ConvertOptions(column_types=types, strings_can_be_null=False),
        )
        ordinal = 0
        for batch in reader:
            table = _adapt_batch(batch, channel, symbol, ordinal)
            ordinal += table.num_rows
            yield table


def _funding_table(source: Path, day: str, symbol: str) -> pa.Table:
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("funding source must be the existing exchange response list")
    start, end = _day_bounds(day)
    values = []
    for row in payload:
        timestamp = int(row["fundingTime"]) * 1000
        if not start <= timestamp < end:
            continue
        if row["symbol"] != symbol:
            raise ValueError("funding symbol mismatch")
        values.append({
            "symbol": symbol, "fundingTime": int(row["fundingTime"]),
            "fundingRate": str(row["fundingRate"]),
            "markPrice": None if row.get("markPrice") is None else str(row["markPrice"]),
            "timestamp": timestamp, "exchange": "binance-futures",
        })
    return pa.Table.from_pylist(values, schema=FUNDING_SCHEMA)


def convert_day_channel(
    source: Path, output: Path, day: str, channel: str, symbol: str = "BTCUSDC"
) -> dict:
    """Validate and atomically publish one canonical channel, without deleting input."""
    source, output = Path(source), Path(output)
    _reject_output_symlinks(output)
    _day_bounds(day)
    if channel not in SCHEMAS:
        raise ValueError(f"unsupported raw channel: {channel}")
    before = _identity(source)
    source_sha = sha256_file(source)
    schema = SCHEMAS[channel]
    if channel == "incremental_book_L2" and source.suffix == ".parquet":
        stored = pq.ParquetFile(source).schema_arrow
        if stored.metadata and stored.metadata.get(b"narrowgate.book_fusion"):
            marker = stored.metadata[b"narrowgate.book_fusion"]
            if marker not in (BOOK_FUSION_SCHEMA.encode(), BOOK_UNION_SCHEMA.encode(), DAILY_BOOK_MARKER.encode()):
                raise ValueError("unsupported fused book schema")
            schema = DAILY_BOOK_SCHEMA if marker == DAILY_BOOK_MARKER.encode() else UNION_BOOK_SCHEMA if marker == BOOK_UNION_SCHEMA.encode() else FUSED_BOOK_SCHEMA
    if source.resolve() == output.resolve():
        parquet = pq.ParquetFile(source)
        if not _schema_matches(parquet.schema_arrow, schema):
            raise ValueError("existing canonical schema mismatch")
        if not parquet.metadata.num_rows:
            raise ValueError("empty raw source cannot stand in for missing data")
        return dict(day=day, channel=channel, status="VERIFIED_EXISTING", rows=parquet.metadata.num_rows,
                    source=str(source), path=str(output), source_sha256=source_sha, sha256=source_sha)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".partial", dir=output.parent)
    os.close(fd)
    temporary = Path(name)
    mode = "converted"
    try:
        if channel == "incremental_book_L2" and source.suffix == ".parquet":
            parquet = pq.ParquetFile(source)
            if not _schema_matches(parquet.schema_arrow, schema):
                raise ValueError("existing raw-book Parquet schema mismatch")
            rows = parquet.metadata.num_rows
            if rows == 0:
                raise ValueError("empty raw source cannot stand in for missing data")
            temporary.unlink()
            try:
                # Source aliases are allowed for ingestion, but the published
                # object must be a hardlink to the regular source, not its alias.
                os.link(source.resolve(strict=True), temporary)
                mode = "verified_hardlink"
            except OSError:
                shutil.copyfile(source, temporary)
                mode = "verified_copy"
            output_sha = source_sha if mode == "verified_hardlink" else sha256_file(temporary)
            if output_sha != source_sha:
                raise ValueError("book copy digest mismatch")
        else:
            batches = [_funding_table(source, day, symbol)] if channel == "funding" else _csv_batches(source, channel, symbol)
            metadata = {
                b"narrowgate.schema": SCHEMA_VERSION.encode(), b"narrowgate.day": day.encode(),
                b"narrowgate.channel": channel.encode(), b"narrowgate.source_sha256": source_sha.encode(),
                b"narrowgate.timestamp_unit": b"exchange_microseconds",
            }
            rows, output_sha = _write_verified_tables(batches, temporary, schema, metadata)
        if before != _identity(source):
            raise ValueError("source changed during conversion")
        _publish_atomic(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return dict(day=day, channel=channel, status="PUBLISHED", mode=mode, rows=rows,
                source=str(source), path=str(output), source_sha256=source_sha, sha256=output_sha)


def _book_inclusion(path: Path, source_id: str) -> tuple[dict, list[dict]]:
    """Identify an input; inherited identities describe inputs, not raw equality."""
    parquet = pq.ParquetFile(path)
    metadata = parquet.metadata.metadata or {}
    fused = metadata.get(b"narrowgate.book_fusion")
    expected_schema = (DAILY_BOOK_SCHEMA if fused == DAILY_BOOK_MARKER.encode()
                       else UNION_BOOK_SCHEMA if fused == BOOK_UNION_SCHEMA.encode()
                       else FUSED_BOOK_SCHEMA if fused else BOOK_SCHEMA)
    if (fused and fused not in (BOOK_FUSION_SCHEMA.encode(), BOOK_UNION_SCHEMA.encode(), DAILY_BOOK_MARKER.encode())) or not _schema_matches(parquet.schema_arrow, expected_schema):
        raise ValueError("unsupported orderbook input schema")
    if not parquet.metadata.num_rows:
        raise ValueError("empty orderbook source")
    entry = {"source_id": source_id, "sha256": sha256_file(path),
             "rows": parquet.metadata.num_rows}
    if fused == DAILY_BOOK_MARKER.encode():
        daily_book_receipt(path)
        return entry, []  # No retired supplier history in a current container.
    if fused:
        inherited = json.loads(metadata[b"narrowgate.included_sources"])
        if not isinstance(inherited, list) or not inherited:
            raise ValueError("fused input is missing source inclusion identities")
        return entry, inherited
    if source_id == "cryptohft":
        hours = set()
        for batch in parquet.iter_batches(columns=["source_hour"], batch_size=1_000_000):
            hours.update(value for value in pc.unique(batch.column(0)).to_pylist() if value is not None)
        if any(hour < 0 or hour > 23 for hour in hours):
            raise ValueError("invalid CryptoHFT source-hour provenance")
        entry["hours"] = sorted(hours)
    return entry, []


def _existing_book_source(path: Path) -> str:
    parquet = pq.ParquetFile(path)
    if (parquet.metadata.metadata or {}).get(b"narrowgate.book_fusion"):
        return "canonical"
    # The lossless adapter leaves every native field null for Tardis. Native
    # Crypto snapshots need not have U/u, but retain their real event/receive
    # fields. This identifies the adapter, not economic or sequence quality.
    for batch in parquet.iter_batches(columns=["received_time", "event_time"], batch_size=1024):
        return "cryptohft" if any(column.null_count < batch.num_rows for column in batch.columns) else "tardis"
    raise ValueError("empty existing canonical book")


def fuse_orderbook_day(
    sources: dict[str, Path], output: Path, day: str, *, symbol: str = "BTCUSDC",
    retire_sources: bool = False, previous_state: dict | None = None,
    normalized_root: Path | None = None, next_sources: dict[str, Path] | None = None,
    observed_union: bool = True, allow_legacy_continuation: bool = False,
    stream_priority: Mapping | None = None,
    unified_output: bool = False,
    previous_top_state: Mapping | None = None,
    supplement_stream_id: str | None = None,
) -> dict:
    """Atomically publish real source observations in one Tardis-format file.

    The default raw archive retains selected and nonselected source observations;
    source switches exist only in the derived state, never as invented raw zeros.
    Original timestamp/IDs/decimal values remain provenance. The explicit legacy
    mode is retained for reading/testing historical reconstructed-view contracts.
    Existing canonical observations are included when a new provider arrives.
    ``normalized_root`` is a staging destination; the caller publishes the
    derived triplet and its quality/index binding as one verified generation.
    The caller may retire validated superseded originals, never prior to output
    roundtrip verification and durable publication. No strategy replay is run.
    """
    from data.normalize_tardis_orderbook import iter_fused_book_batches

    output = Path(output)
    _reject_output_symlinks(output)
    _day_bounds(day)
    if symbol != "BTCUSDC":
        raise ValueError("orderbook fusion currently supports BTCUSDC perpetual only")
    if supplement_stream_id is not None:
        if (set(sources) not in ({"canonical"}, {"canonical", "tardis"})
                or normalized_root is None or not observed_union or not unified_output
                or retire_sources or next_sources or allow_legacy_continuation
                or stream_priority is not None):
            raise ValueError("explicit daily supplementation requires canonical/[tardis], fresh staging and caller-owned retirement")
        return supplement_daily_book_day(sources["canonical"], sources.get("tardis"), output, day,
            stream_id=supplement_stream_id, previous_state=previous_state, normalized_root=normalized_root,
            previous_top_state=previous_top_state)
    for path in [*map(Path, sources.values()), *([output] if output.is_file() else [])]:
        if (pq.ParquetFile(path).metadata.metadata or {}).get(b"narrowgate.book_fusion") == DAILY_BOOK_MARKER.encode():
            raise ValueError("updating a unified daily book requires an explicit anonymous stream/duplicate-message contract; no input was replaced")
    previous_identity = None
    schema_marker = BOOK_UNION_SCHEMA if observed_union else BOOK_FUSION_SCHEMA
    output_schema = UNION_BOOK_SCHEMA if observed_union else FUSED_BOOK_SCHEMA
    priority = book_stream_priority(["cryptohft", "tardis", "canonical"], contract=stream_priority)
    if previous_state is None:
        previous_day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        previous_path = output.parent.parent / previous_day / output.name
        if previous_path.is_file():
            previous_metadata = pq.ParquetFile(previous_path).metadata.metadata or {}
            previous_marker = previous_metadata.get(b"narrowgate.book_fusion")
            if previous_marker == DAILY_BOOK_MARKER.encode():
                previous_receipt = daily_book_receipt(previous_path)
                if previous_top_state is None:
                    previous_top_state = previous_receipt.get("top_final_state")
                previous_state = daily_book_continuation(previous_receipt.get("final_state"))
                # Compatibility with the fixed ingestion slots used by this
                # publisher. Their meaning is numeric; do not infer a match
                # from an arbitrary new stream's name or native flag.
                if (previous_state is None or previous_state["source_ids"] != ["s0", "s1", "s2"]
                        or previous_state["kernel"]["preferred"] != priority["preferred_index"]):
                    raise ValueError("new-day ingestion continuation requires its explicit compatible stream slots")
                aliases = dict(zip(previous_state["source_ids"], priority["stream_ids"], strict=True))
                previous_state["source_ids"] = priority["stream_ids"]
                previous_state["stream_priority"] = priority
                for field in ("deferred_rows_by_source", "deferred_observations_by_source"):
                    previous_state[field] = {aliases[key]: value for key, value in previous_state[field].items()}
                for rows in previous_state["deferred_observations_by_source"].values():
                    for row in rows:
                        row["source_id"] = aliases[row["source_id"]]
                previous_identity = hashlib.sha256(previous_metadata[b"narrowgate.book_receipt"]).hexdigest()
            if previous_marker in (BOOK_FUSION_SCHEMA.encode(), BOOK_UNION_SCHEMA.encode()):
                if observed_union and previous_marker == BOOK_FUSION_SCHEMA.encode() and not allow_legacy_continuation:
                    raise ValueError("legacy reconstructed continuation requires an explicit, limited seed")
                previous_receipt = json.loads(previous_metadata[b"narrowgate.fusion_receipt"])
                if (previous_receipt.get("day"), previous_receipt.get("symbol")) != (previous_day, symbol):
                    raise ValueError("previous canonical book has mismatched scope")
                previous_state = previous_receipt["stats"].get("continuation")
                if previous_state is None:
                    raise ValueError("previous fused day is missing its continuation")
                previous_identity = hashlib.sha256(previous_metadata[b"narrowgate.fusion_receipt"]).hexdigest()
    inputs = {str(key): Path(value) for key, value in sources.items()}
    if not inputs or any(key not in {"cryptohft", "tardis", "canonical"} for key in inputs):
        raise ValueError("use explicit supported orderbook source identities")
    if len({path.resolve(strict=True) for path in inputs.values()}) != len(inputs):
        raise ValueError("same orderbook source path supplied more than once")
    if "canonical" in inputs:
        detected = _existing_book_source(inputs["canonical"])
        if detected != "canonical" and detected not in inputs:
            inputs[detected] = inputs.pop("canonical")
    if output.exists() and output.resolve() not in {path.resolve() for path in inputs.values()}:
        kind = _existing_book_source(output)
        # A refreshed provider cannot overwrite its predecessor or a previously
        # fused complementary source. Treat the existing coherent stream as its
        # own input when both use the same provider label.
        same_source_copy = kind in inputs and (
            _identity(inputs[kind])[:2] == _identity(output)[:2]
            or sha256_file(inputs[kind]) == sha256_file(output)
        )
        if not same_source_copy:
            key = kind if kind not in inputs else "canonical"
            if key in inputs:
                raise ValueError("explicit canonical input conflicts with existing output")
            inputs[key] = output
    before = {key: _identity(path) for key, path in inputs.items()}
    if observed_union:
        for path in inputs.values():
            marker = (pq.ParquetFile(path).metadata.metadata or {}).get(b"narrowgate.book_fusion")
            if marker not in (None, BOOK_UNION_SCHEMA.encode()):
                raise ValueError("original source observations are required; a reconstructed view is lossy")
    adjacent = {str(key): Path(path) for key, path in (next_sources or {}).items()}
    if any(key not in {"cryptohft", "tardis", "canonical"} for key in adjacent):
        raise ValueError("unsupported adjacent orderbook source identity")
    adjacent_before = {key: _identity(path) for key, path in adjacent.items()}
    adjacent_entries = [{"source_id": key, "sha256": sha256_file(path),
                         "rows": pq.ParquetFile(path).metadata.num_rows,
                         "use": "exchange_clock_before_current_day_end_only"}
                        for key, path in adjacent.items()]
    state_sha = (hashlib.sha256(json.dumps(previous_state, sort_keys=True).encode()).hexdigest()
                 if previous_state is not None else None)
    entries = {}
    included = {}
    boundary_inclusions = {(item["source_id"], item["sha256"]): item for item in adjacent_entries}
    existing_included = set()
    for key, path in inputs.items():
        entry, inherited = _book_inclusion(path, key)
        entries[key] = entry
        if inherited:
            metadata = pq.ParquetFile(path).metadata.metadata or {}
            prior = json.loads(metadata[b"narrowgate.fusion_receipt"])
            if (prior.get("day"), prior.get("symbol")) != (day, symbol):
                raise ValueError("fused source has mismatched scope")
            for item in prior.get("boundary_input_files", []):
                # The current coherent tape already contains the verified
                # boundary observations. Their original next-day input may
                # have been retired; do not erase that provenance on refresh.
                boundary_inclusions.setdefault((item["source_id"], item["sha256"]), item)
        for item in inherited or [entry]:
            included[(item["source_id"], item["sha256"])] = item
        if path.resolve() == output.resolve() and inherited:
            existing_included = {item["sha256"] for item in inherited}
    bound_adjacent_entries = list(boundary_inclusions.values())
    incoming = [entry for key, entry in entries.items() if inputs[key].resolve() != output.resolve()]
    reused = output.exists() and existing_included and all(entry["sha256"] in existing_included for entry in incoming)
    if reused:
        stored = json.loads((pq.ParquetFile(output).metadata.metadata or {})[b"narrowgate.fusion_receipt"])
        reused = (stored.get("boundary_input_files", []) == bound_adjacent_entries
                  and stored.get("previous_state_sha256") == state_sha
                  and stored.get("schema") == schema_marker
                  and book_stream_priority(contract=stored.get("stream_priority")) == priority)
    if reused:
        metadata = pq.ParquetFile(output).metadata.metadata or {}
        receipt = json.loads(metadata[b"narrowgate.fusion_receipt"])
        if (receipt.get("day"), receipt.get("symbol")) != (day, symbol):
            raise ValueError("existing fused day has mismatched scope")
        result = {**receipt, "status": "REUSED_VERIFIED", "path": str(output),
                  "sha256": sha256_file(output), "rows": receipt["output_rows"]}
    else:
        included_sources = list(included.values())
        output.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".partial", dir=output.parent)
        os.close(fd)
        temporary = Path(name)
        try:
            batches, stats = iter_fused_book_batches(
                inputs, day, symbol=symbol, previous_state=previous_state,
                normalized_root=normalized_root, next_sources=adjacent,
                observed_union=observed_union, allow_legacy_continuation=allow_legacy_continuation,
                stream_priority=priority,
                uniform_clock=unified_output,
                top_initial_state=previous_top_state,
            )
            receipt = {"schema": schema_marker, "day": day, "symbol": symbol,
                       "included_sources": included_sources, "input_files": list(entries.values()),
                       "boundary_input_files": bound_adjacent_entries,
                       "previous_day_receipt_sha256": previous_identity,
                       "previous_state_sha256": state_sha, "stream_priority": priority, "stats": stats}
            if observed_union:
                # A standalone reader may seed only this declared start state,
                # never the end-of-day continuation in stats.
                receipt["initial_continuation"] = previous_state

            def complete_metadata(rows):
                receipt["output_rows"] = rows
                return {b"narrowgate.fusion_receipt": json.dumps(receipt, sort_keys=True).encode()}

            rows, output_sha = _write_verified_tables(
                batches, temporary, output_schema,
                {b"narrowgate.schema": schema_marker.encode(),
                 b"narrowgate.book_fusion": schema_marker.encode(),
                 b"narrowgate.day": day.encode(), b"narrowgate.channel": b"incremental_book_L2",
                 b"narrowgate.timestamp_unit": b"exchange_microseconds",
                 b"narrowgate.included_sources": json.dumps(included_sources, sort_keys=True).encode()},
                final_metadata=complete_metadata,
            )
            unified = unify_orderbook_day(temporary, temporary, day, symbol=symbol,
                top_initial_state=stats.get("top_initial_state"),
                top_final_state=stats.get("top_final_state")) if unified_output else None
            if unified is not None:
                output_sha = unified["sha256"]
            for key, path in inputs.items():
                if _identity(path) != before[key]:
                    raise ValueError("orderbook input changed during fusion")
            for key, path in adjacent.items():
                if _identity(path) != adjacent_before[key]:
                    raise ValueError("adjacent orderbook input changed during fusion")
            _publish_atomic(temporary, output)
            result = {**receipt, "status": "PUBLISHED", "path": str(output),
                      "rows": rows, "sha256": output_sha}
            if unified is not None:
                result = {"schema": DAILY_BOOK_MARKER, "day": day, "symbol": symbol,
                          "status": "PUBLISHED", "path": str(output), "rows": rows,
                          "output_rows": rows, "sha256": output_sha,
                          "consumed_inputs": [{"sha256": value["sha256"], "rows": value["rows"]}
                                              for value in entries.values()],
                          "stream_priority": unified["receipt"]["stream_priority"],
                          "stats": {"normalized": stats.get("normalized", {}),
                                    "continuation_location": "canonical_parquet_footer"}}
        finally:
            temporary.unlink(missing_ok=True)
    if retire_sources:
        retiring = [(key, path) for key, path in inputs.items() if path.resolve() != output.resolve()]
        for key, path in retiring:
            if _identity(path) != before[key] or sha256_file(path) != entries[key]["sha256"]:
                raise ValueError("orderbook original changed before retirement")
        for _, path in retiring:
            path.unlink()
        result["retired_sources"] = [entry["sha256"] for key, entry in entries.items()
                                     if inputs[key].resolve() != output.resolve()]
    # Keep the full continuation once, in the verified Parquet footer. Download
    # manifests and current catalogs only need a compact identity, not hundreds
    # of repeated source books in an ever-growing JSON document.
    continuation = result.get("stats", {}).get("continuation")
    if continuation is not None:
        result["stats"] = {key: value for key, value in result["stats"].items()
                           if key != "continuation"}
        result["stats"]["continuation_sha256"] = hashlib.sha256(
            json.dumps(continuation, sort_keys=True).encode()).hexdigest()
        result["stats"]["continuation_location"] = "canonical_parquet_footer"
    initial = result.pop("initial_continuation", None)
    if initial is not None:
        result["initial_continuation_sha256"] = hashlib.sha256(
            json.dumps(initial, sort_keys=True).encode()).hexdigest()
        result["initial_continuation_location"] = "canonical_parquet_footer"
    return result


def convert_auxiliary_csv(
    source: Path, output: Path, day: str, channel: str, exchange: str, symbol: str,
) -> dict:
    """Losslessly containerize existing auxiliary CSV without altering source clocks.

    This does not promote an auxiliary source into an execution channel. IDs,
    decimal values and remaining source fields stay exact text. Only supplied
    timestamp columns become integers, retaining their original units.
    """
    source, output = Path(source), Path(output)
    _reject_output_symlinks(output)
    _day_bounds(day)
    if channel not in {"book_ticker", "aggTrades", "metrics"}:
        raise ValueError("unsupported auxiliary channel")
    market_names = {
        "binance_spot": "binance", "binance-spot": "binance", "binance": "binance",
        "binance_futures": "binance-futures", "binance-futures": "binance-futures",
    }
    if exchange not in market_names:
        raise ValueError("unsupported auxiliary exchange")
    if source.resolve() == output.resolve():
        raise ValueError("CSV input and Parquet output cannot be the same file")
    before = _identity(source)
    source_sha = sha256_file(source)
    with pa.input_stream(str(source), compression="detect") as raw, io.TextIOWrapper(raw) as text:
        first = next(csv.reader(text), None)
    if not first:
        raise ValueError("empty auxiliary CSV")
    headerless_spot = market_names[exchange] == "binance" and channel == "aggTrades" and first[0].isdigit()
    if headerless_spot:
        names = ["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
                 "transact_time", "is_buyer_maker", "is_best_match"]
        if len(first) != len(names):
            raise ValueError("headerless Binance spot aggTrades must have eight columns")
    else:
        names = first
    if len(set(names)) != len(names):
        raise ValueError("duplicate auxiliary CSV column name")
    clocks = {"timestamp", "local_timestamp", "transact_time"}
    schema = pa.schema([(name, pa.int64() if name in clocks else pa.string()) for name in names])

    def batches():
        with pa.input_stream(str(source), compression="detect") as stream:
            reader = pacsv.open_csv(
                stream,
                read_options=pacsv.ReadOptions(
                    block_size=4 * 1024 * 1024, use_threads=False,
                    column_names=names if headerless_spot else None,
                ),
                convert_options=pacsv.ConvertOptions(
                    column_types={field.name: field.type for field in schema}, strings_can_be_null=False,
                ),
            )
            for batch in reader:
                table = pa.Table.from_batches([batch], schema=schema)
                for name, expected in (("symbol", symbol), ("exchange", market_names[exchange])):
                    if name in names and pc.any(pc.not_equal(table[name], expected)).as_py():
                        raise ValueError(f"auxiliary {name} mismatch")
                yield table

    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".partial", dir=output.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        rows, output_sha = _write_verified_tables(batches(), temporary, schema, {
            b"narrowgate.schema": SCHEMA_VERSION.encode(), b"narrowgate.day": day.encode(),
            b"narrowgate.channel": channel.encode(), b"narrowgate.exchange": exchange.encode(),
            b"narrowgate.source_sha256": source_sha.encode(), b"narrowgate.timestamp_unit": b"source_preserved",
        })
        if before != _identity(source):
            raise ValueError("source changed during conversion")
        _publish_atomic(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return dict(day=day, channel=channel, exchange=exchange, symbol=symbol, status="PUBLISHED",
                mode="auxiliary_exact_csv", rows=rows, source=str(source), path=str(output),
                source_sha256=source_sha, sha256=output_sha, timestamp_units="source_preserved")


def migrate_manifest(
    manifest: Path, output_root: Path, *, days: list[str] | None = None,
    workers: int = 2, retire_source: bool = False,
) -> dict:
    """Import all required raw channels selected by the current readability manifest."""
    from data_paths import daily_market_path

    if workers not in (1, 2):
        raise ValueError("workers must be 1 or 2 for bounded-memory raw conversion")
    payload = json.loads(Path(manifest).read_text(encoding="utf-8"))
    requested = set(days) if days else None
    records = [r for r in payload["records"] if requested is None or r["calendar_date"] in requested]
    if requested is not None and {r["calendar_date"] for r in records} != requested:
        raise ValueError("requested date missing from input manifest")
    sources = {}
    tasks = []
    channel_counts = {}
    for record in records:
        day = record["calendar_date"]
        _day_bounds(day)
        channels = {c["source_id"]: c for c in record["channels"]}
        channel_counts[day] = 0
        for channel, source_id in CHANNEL_SOURCES.items():
            if source_id not in channels:
                if channel == "aggTrades":
                    continue
                raise ValueError(f"{day} {channel}: required source is absent from manifest")
            files = channels[source_id]["files"]
            if len(files) != 1:
                raise ValueError(f"{day} {channel}: exactly one real selected source is required")
            source = Path(files[0]["path"])
            if not source.is_file():
                raise FileNotFoundError(f"{day} {channel}: selected source is unavailable: {source}")
            output = daily_market_path(day, "BTCUSDC", channel, root=Path(output_root))
            tasks.append((source, output, day, channel))
            channel_counts[day] += 1
            sources.setdefault(str(source.resolve()), set()).add(day)
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(lambda args: convert_day_channel(*args), tasks):
            results.append(result)
            print(json.dumps({k: result[k] for k in ("day", "channel", "status", "rows")}), flush=True)
    retired = []
    if retire_source:
        # An interval funding JSON can serve days outside this invocation. Only
        # retire when every manifest reference to the same physical file migrated.
        all_references = {}
        for record in payload["records"]:
            for item in record["channels"]:
                if item["source_id"] not in CHANNEL_SOURCES.values():
                    continue
                for file in item["files"]:
                    all_references.setdefault(str(Path(file["path"]).resolve()), set()).add(record["calendar_date"])
        for original, covered in sources.items():
            if all_references.get(original, set()) - covered:
                continue
            matching = [r for r in results if str(Path(r["source"]).resolve()) == original]
            if any(Path(r["path"]).resolve() == Path(original) for r in matching):
                continue
            if sha256_file(Path(original)) != matching[0]["source_sha256"]:
                raise ValueError("source changed before retirement")
            Path(original).unlink()
            retired.append(original)
    counts = set(channel_counts.values())
    return {"schema": SCHEMA_VERSION, "calendar_days": len(records),
            "channels_per_day": next(iter(counts)) if len(counts) == 1 else None,
            "channel_counts_by_day": channel_counts,
            "records": results, "retired_sources": retired, "economic_replay": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--days", nargs="*")
    parser.add_argument("--workers", type=int, choices=(1, 2), default=2)
    parser.add_argument("--retire-source", action="store_true")
    args = parser.parse_args()
    result = migrate_manifest(args.manifest, args.output_root, days=args.days,
                              workers=args.workers, retire_source=args.retire_source)
    target = args.output_root / "raw" / "daily-index.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".daily-index.", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
