#!/usr/bin/env python3
"""Reconstruct source-separated Tardis BTCUSDC top-20 books at 100 ms.

The default clock is exchange ``timestamp``, before adding modeled host latency.
Provider ``local_timestamp`` is an explicitly selected diagnostic clock only.
A row at boundary ``b`` contains only messages strictly before ``b`` on the
selected clock. Provider receive timestamps remain provenance in both products.
Tardis incremental L2 does not expose Binance
``U/u/pu`` sequence IDs; the resulting book is therefore a provider-normalized
replay candidate, never native-sequence or exact-queue evidence.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import csv
import hashlib
import json
import math
import os
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq

from data.downloaders.cryptohft_orderbook import (
    OrderBookState,
    _bbo_schema,
    _l2_schema,
)
from data.downloaders.tardis_archive import resolve_tardis_artifact_path
from data_paths import data_root, marketdata_root

SOURCE_ID = "tardis.0730-beinan.binance-futures.BTCUSDC.v1"
DATASET_ID = "normalized_tardis_l2_100ms_v1"
EXCHANGE_DATASET_ID = "normalized_tardis_l2_exchange_100ms_v1"
BOOK_TICKER = "book_ticker"
INCREMENTAL_L2 = "incremental_book_L2"
DAY_US = 86_400 * 1_000_000
DEFAULT_CADENCE_MS = 100
OBSERVATION_SCHEMA = "narrowgate.book_observation.v3"
DEFAULT_FRESHNESS_MS = 500
CROSS_CHANNEL_MAX_AGE_MS = 5_000
CROSS_CHANNEL_MIN_COMPARABLE_RATIO = 0.99
CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO = 0.95
CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO = 0.95
# One merged input can expand into many full-depth source-switch deltas. Drain
# the native output frequently; input Parquet batch size is not an output bound.
FUSION_PUSH_ROWS = 8_192
DEFAULT_WRITE_MAX_PENDING_BYTES = 32 * 1024**2
ESTIMATED_NORMALIZED_BYTES_PER_FULL_DAY = 200 * 1024**2
GAP_EDGES_US = (
    1_000,
    2_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _day_start_us(day: str) -> int:
    value = date.fromisoformat(day)
    return int(
        datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp()
        * 1_000_000
    )


def _publish_files(pairs: Sequence[tuple[Path, Path]]) -> None:
    """Rollback ordinary failures. Marker hashes detect a crash between renames."""
    pairs[0][1].parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=pairs[0][1].parent) as backup_dir:
        backups: list[tuple[Path, Path]] = []
        installed: list[Path] = []
        try:
            for index, (source, target) in enumerate(pairs):
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    backup = Path(backup_dir) / str(index)
                    os.replace(target, backup)
                    backups.append((backup, target))
                os.replace(source, target)
                installed.append(target)
        except BaseException:
            for target in reversed(installed):
                target.unlink(missing_ok=True)
            for backup, target in reversed(backups):
                os.replace(backup, target)
            raise


def _freshness_union_coverage(
    timestamps_ms: Sequence[int],
    *,
    start_ms: int,
    end_ms: int,
    freshness_ms: int,
) -> tuple[float, float]:
    """Return forward-visible interval coverage and timestamp p99 gap."""

    if not timestamps_ms or end_ms <= start_ms or freshness_ms <= 0:
        return 0.0, float("inf")
    ordered = np.asarray(timestamps_ms, dtype=np.int64)
    ordered.sort()
    covered = 0
    current_start = max(start_ms, int(ordered[0]))
    current_end = min(end_ms, int(ordered[0]) + freshness_ms)
    for timestamp in ordered[1:]:
        interval_start = max(start_ms, int(timestamp))
        interval_end = min(end_ms, int(timestamp) + freshness_ms)
        if interval_end <= interval_start:
            continue
        if interval_start <= current_end:
            current_end = max(current_end, interval_end)
        else:
            covered += max(0, current_end - current_start)
            current_start = interval_start
            current_end = interval_end
    covered += max(0, current_end - current_start)
    gaps = np.diff(ordered).astype(np.float64)
    p99_gap_ms = float(np.quantile(gaps, 0.99)) if len(gaps) else 0.0
    return min(1.0, covered / max(1, end_ms - start_ms)), p99_gap_ms


@dataclass
class GapHistogram:
    counts: list[int] = field(
        default_factory=lambda: [0] * (len(GAP_EDGES_US) + 1)
    )
    observations: int = 0
    maximum_us: int = 0

    def add(self, value_us: int) -> None:
        value = max(0, int(value_us))
        self.observations += 1
        self.maximum_us = max(self.maximum_us, value)
        index = int(np.searchsorted(GAP_EDGES_US, value, side="left"))
        self.counts[index] += 1

    def quantile_upper_us(self, probability: float) -> int | None:
        if not self.observations:
            return None
        target = int(math.ceil(float(probability) * self.observations))
        cumulative = 0
        for index, count in enumerate(self.counts):
            cumulative += count
            if cumulative >= target:
                if index < len(GAP_EDGES_US):
                    return int(GAP_EDGES_US[index])
                return int(self.maximum_us)
        return int(self.maximum_us)

    def as_dict(self) -> dict[str, Any]:
        return {
            "observations": self.observations,
            "maximum_us": self.maximum_us,
            "p99_upper_us": self.quantile_upper_us(0.99),
            "count_gt_500ms": sum(
                count
                for index, count in enumerate(self.counts)
                if index >= int(np.searchsorted(GAP_EDGES_US, 500_000, side="right"))
            ),
            "count_gt_5s": sum(
                count
                for index, count in enumerate(self.counts)
                if index >= int(np.searchsorted(GAP_EDGES_US, 5_000_000, side="right"))
            ),
        }


class _ParquetPairWriter:
    """Write BBO/L2/provider-clock files under one source identity."""

    def __init__(self, root: Path, symbol: str, day: str, levels: int,
                 *, timestamp_source: str = "exchange", write_workers: int = 1,
                 write_max_pending_bytes: int = DEFAULT_WRITE_MAX_PENDING_BYTES) -> None:
        if type(write_workers) is not int or write_workers not in (1, 2):
            raise ValueError("write workers must be 1 or 2")
        if type(write_max_pending_bytes) is not int or write_max_pending_bytes <= 0:
            raise ValueError("write_max_pending_bytes must be a positive integer")
        self.write_workers, self.max_pending_bytes = write_workers, write_max_pending_bytes
        self._executor = None
        self._pending = None
        self._write_error = None
        self._write_runtime = {
            "workers": write_workers,
            "mode": "sequential_producer_single_background_writer" if write_workers == 2 else "serial",
            "max_pending_bytes": write_max_pending_bytes, "max_in_flight_batches": 0,
            "peak_pending_bytes": 0, "async_batches": 0, "synchronous_batches": 0,
            "oversize_synchronous_batches": 0,
        }
        self.levels = int(levels)
        self.timestamp_source = timestamp_source
        self.age_column = ("exchange_resample_age_us" if timestamp_source == "exchange"
                           else "provider_visibility_delay_us")
        self.bbo_final = root / "bbo" / f"{symbol}-bbo-{day}.parquet"
        self.l2_final = root / "l2" / f"{symbol}-l2-{day}.parquet"
        self.clock_final = root / "clock" / f"{symbol}-clock-{day}.parquet"
        self.bbo_tmp = self.bbo_final.with_suffix(".parquet.tmp")
        self.l2_tmp = self.l2_final.with_suffix(".parquet.tmp")
        self.clock_tmp = self.clock_final.with_suffix(".parquet.tmp")
        self.bbo_final.parent.mkdir(parents=True, exist_ok=True)
        self.l2_final.parent.mkdir(parents=True, exist_ok=True)
        self.clock_final.parent.mkdir(parents=True, exist_ok=True)
        self.bbo_tmp.unlink(missing_ok=True)
        self.l2_tmp.unlink(missing_ok=True)
        self.clock_tmp.unlink(missing_ok=True)
        self.bbo_writer = pq.ParquetWriter(
            self.bbo_tmp, _bbo_schema(), compression="zstd"
        )
        self.l2_writer = pq.ParquetWriter(
            self.l2_tmp, _l2_schema(self.levels), compression="zstd"
        )
        self.clock_schema = pa.schema(
            [
                ("timestamp", pa.int64()),
                ("exchange_cut_timestamp_us", pa.int64()),
                ("last_provider_local_timestamp_us", pa.int64()),
                (self.age_column, pa.int64()),
                ("last_observation_timestamp_us", pa.int64()),
                ("observation_age_us", pa.int64()),
                ("observation_kind", pa.string()),
                ("update_coverage", pa.string()),
            ]
        )
        self.clock_writer = pq.ParquetWriter(
            self.clock_tmp, self.clock_schema, compression="zstd"
        )
        self.bbo: dict[str, list[float | int]] = {
            "timestamp": [],
            "best_bid": [],
            "best_bid_qty": [],
            "best_ask": [],
            "best_ask_qty": [],
        }
        self.l2: dict[str, list[float | int]] = {"timestamp": []}
        for level in range(1, self.levels + 1):
            for column_prefix in ("bid_px", "bid_qty", "ask_px", "ask_qty"):
                self.l2[f"{column_prefix}_{level}"] = []
        self.clock: dict[str, list[Any]] = {
            "timestamp": [],
            "exchange_cut_timestamp_us": [],
            "last_provider_local_timestamp_us": [],
            self.age_column: [],
            "last_observation_timestamp_us": [],
            "observation_age_us": [],
            "observation_kind": [],
            "update_coverage": [],
        }
        self.rows = 0
        self.closed = False
        self.finalized = False

    def _write_tables(self, tables: tuple[pa.Table, pa.Table, pa.Table]) -> None:
        # One caller owns these handles at a time. Arrow buffers own their
        # native copies; a later BookFusion push cannot mutate this payload.
        for writer, table in zip((self.bbo_writer, self.l2_writer, self.clock_writer), tables, strict=True):
            writer.write_table(table)

    def _drain(self) -> None:
        pending = self._pending
        if pending is not None:
            try:
                pending.result()
            except BaseException as exc:
                self._write_error = exc
            finally:
                self._pending = None
        if self._write_error is not None:
            raise self._write_error

    def check(self) -> None:
        """Surface a completed background failure before the next native push."""
        if self._write_error is not None or (self._pending is not None and self._pending.done()):
            self._drain()

    def _write_or_submit(self, tables: tuple[pa.Table, pa.Table, pa.Table]) -> None:
        if self.closed:
            raise ValueError("cannot append to a closed book writer")
        self._drain()  # ONE queued/running batch, never a calendar backlog.
        size = sum(table.get_total_buffer_size() for table in tables)
        if self.write_workers == 2 and size <= self.max_pending_bytes:
            if self._executor is None:
                self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="ng-book-write")
            self._pending = self._executor.submit(self._write_tables, tables)
            self._write_runtime["async_batches"] += 1
            self._write_runtime["max_in_flight_batches"] = 1
            self._write_runtime["peak_pending_bytes"] = max(self._write_runtime["peak_pending_bytes"], size)
        else:
            # Huge gap outputs are synchronous after draining. Do not change
            # their row-group boundaries just to force them into the queue.
            try:
                self._write_tables(tables)
            except BaseException as exc:
                self._write_error = exc
                raise
            self._write_runtime["synchronous_batches"] += 1
            self._write_runtime["oversize_synchronous_batches"] += int(self.write_workers == 2)

    def runtime(self) -> dict:
        return {**self._write_runtime, "closed": self.closed, "pending_batches": int(self._pending is not None),
                "failure_observed": self._write_error is not None}

    def append(
        self,
        timestamp_ms: int,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        *,
        exchange_cut_us: int,
        last_provider_local_us: int,
        carried: bool = False,
    ) -> None:
        self.bbo["timestamp"].append(timestamp_ms)
        self.bbo["best_bid"].append(bids[0][0])
        self.bbo["best_bid_qty"].append(bids[0][1])
        self.bbo["best_ask"].append(asks[0][0])
        self.bbo["best_ask_qty"].append(asks[0][1])
        self.l2["timestamp"].append(timestamp_ms)
        for offset in range(self.levels):
            bid_price, bid_quantity = bids[offset]
            ask_price, ask_quantity = asks[offset]
            level = offset + 1
            self.l2[f"bid_px_{level}"].append(bid_price)
            self.l2[f"bid_qty_{level}"].append(bid_quantity)
            self.l2[f"ask_px_{level}"].append(ask_price)
            self.l2[f"ask_qty_{level}"].append(ask_quantity)
        boundary_us = int(timestamp_ms) * 1_000
        self.clock["timestamp"].append(timestamp_ms)
        self.clock["exchange_cut_timestamp_us"].append(exchange_cut_us)
        self.clock["last_provider_local_timestamp_us"].append(
            last_provider_local_us
        )
        self.clock[self.age_column].append(
            boundary_us - (exchange_cut_us if self.timestamp_source == "exchange"
                           else last_provider_local_us)
        )
        observed_us = exchange_cut_us if self.timestamp_source == "exchange" else last_provider_local_us
        self.clock["last_observation_timestamp_us"].append(observed_us)
        self.clock["observation_age_us"].append(boundary_us - observed_us)
        self.clock["observation_kind"].append("carried_forward" if carried else "source_observed")
        self.clock["update_coverage"].append("unknown" if carried else "source_message_present")
        if len(self.bbo["timestamp"]) >= 10_000:
            self.flush()

    def flush(self) -> None:
        count = len(self.bbo["timestamp"])
        if not count:
            return
        self._write_or_submit((pa.Table.from_pydict(self.bbo, schema=_bbo_schema()),
                               pa.Table.from_pydict(self.l2, schema=_l2_schema(self.levels)),
                               pa.Table.from_pydict(self.clock, schema=self.clock_schema)))
        self.rows += count
        for values in self.bbo.values():
            values.clear()
        for values in self.l2.values():
            values.clear()
        for values in self.clock.values():
            values.clear()

    def append_fusion_batch(self, payload: Mapping[str, Any]) -> None:
        """Write native sampled matrices without a Python loop per level/row."""
        self.check()
        timestamps = np.asarray(payload["normalized_timestamp"], dtype=np.int64)
        if not len(timestamps):
            return
        self.flush()
        matrix = np.asarray(payload["normalized_levels"], dtype=np.int64).reshape(-1, self.levels * 4) / 1e8
        observed = np.asarray(payload["normalized_observed"], dtype=np.int64)
        local = np.asarray(payload["normalized_local"], dtype=np.int64)
        carried = np.asarray(payload["normalized_carried"], dtype=bool)
        bbo = {"timestamp": timestamps, "best_bid": matrix[:, 0], "best_bid_qty": matrix[:, 1],
               "best_ask": matrix[:, 2], "best_ask_qty": matrix[:, 3]}
        l2 = {"timestamp": timestamps}
        for offset in range(self.levels):
            for field_offset, prefix in enumerate(("bid_px", "bid_qty", "ask_px", "ask_qty")):
                l2[f"{prefix}_{offset + 1}"] = matrix[:, offset * 4 + field_offset]
        age = timestamps * 1000 - observed
        if np.any(age < 0):
            raise ValueError("fusion sampled book contains a future observation")
        clock = {"timestamp": timestamps, "exchange_cut_timestamp_us": observed,
                 "last_provider_local_timestamp_us": local, self.age_column: age,
                 "last_observation_timestamp_us": observed, "observation_age_us": age,
                 "observation_kind": np.where(carried, "carried_forward", "source_observed"),
                 "update_coverage": np.where(carried, "unknown", "source_message_present")}
        self._write_or_submit((pa.Table.from_pydict(bbo, schema=_bbo_schema()),
                               pa.Table.from_pydict(l2, schema=_l2_schema(self.levels)),
                               pa.Table.from_pydict(clock, schema=self.clock_schema)))
        self.rows += len(timestamps)

    def _finish(self, *, flush_pending: bool) -> BaseException | None:
        if self.closed:
            return self._write_error
        failure = None
        try:
            if flush_pending:
                self.flush()
            self._drain()
        except BaseException as exc:
            failure = exc
        finally:
            # A main-thread interrupt of future.result() does not stop its
            # writer. Always join before touching temporary files or handles.
            if self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None
            for writer in (self.bbo_writer, self.l2_writer, self.clock_writer):
                try:
                    writer.close()
                except BaseException as exc:
                    failure = failure or exc
        self.closed = True
        self._write_error = failure or self._write_error
        return self._write_error

    def finish(self) -> None:
        """Drain the temporary triplet before top processing or publication."""
        failure = self._finish(flush_pending=True)
        if failure is not None:
            raise failure

    def close(self, *, publish: bool) -> None:
        if self.finalized:
            return
        try:
            if publish:
                self.finish()
                if self.rows:
                    _publish_files([(self.bbo_tmp, self.bbo_final),
                                    (self.l2_tmp, self.l2_final),
                                    (self.clock_tmp, self.clock_final)])
                    self.finalized = True
                    return
            else:
                self._finish(flush_pending=False)
        except BaseException:
            self.close(publish=False)
            raise
        finally:
            if not self.finalized:
                self.bbo_tmp.unlink(missing_ok=True)
                self.l2_tmp.unlink(missing_ok=True)
                self.clock_tmp.unlink(missing_ok=True)
        self.finalized = True


@dataclass
class ReconstructionStats:
    raw_rows: int = 0
    logical_messages: int = 0
    snapshot_messages: int = 0
    update_messages: int = 0
    invalid_rows: int = 0
    pre_snapshot_rows: int = 0
    causal_violations: int = 0
    local_clock_reversals: int = 0
    exchange_clock_reversals: int = 0
    emitted_rows: int = 0
    insufficient_depth_buckets: int = 0
    invalid_spread_buckets: int = 0


@dataclass
class L2Continuation:
    """Caller-owned reconstruction state, not an exchange queue or trade tape.

    Preserve observed clocks across adjacent UTC files. A missing update never
    advances these clocks, and this state grants no source/sequence admission.
    """

    book: OrderBookState = field(default_factory=OrderBookState)
    initialized: bool = False
    next_day_start_us: int | None = None
    symbol: str = "BTCUSDC"
    timestamp_source: str = "exchange"
    last_exchange_us: int = 0
    last_local_us: int = 0


def _open_csv(path: Path, *, exact_values: bool = False):
    if path.suffix == ".parquet":
        # The daily raw boundary has one schema regardless of its supplier.
        # Cast numeric text without discarding source-native extension fields.
        source = pq.ParquetFile(path)
        casts = {
            "timestamp": pa.int64(), "local_timestamp": pa.int64(),
            "is_snapshot": pa.bool_(),
            "price": pa.string() if exact_values else pa.float64(),
            "amount": pa.string() if exact_values else pa.float64(),
        }
        schema = pa.schema([
            pa.field(field.name, casts.get(field.name, field.type), nullable=field.nullable)
            for field in source.schema_arrow
        ])
        return pa.RecordBatchReader.from_batches(
            schema, (batch.cast(schema) for batch in source.iter_batches(batch_size=65536)),
        )
    # Arrow does not infer XZ / .zstd consistently. Use actual magic, not
    # the stable download URL's suffix, and close decoder/file with the tape.
    import lzma
    import zstandard

    raw = path.open("rb")
    magic = raw.read(6)
    raw.seek(0)
    decoded = (lzma.LZMAFile(raw) if magic.startswith(b"\xfd7zXZ\x00") else
               zstandard.ZstdDecompressor().stream_reader(raw) if magic.startswith(b"\x28\xb5\x2f\xfd") else raw)
    try:
        reader = pacsv.open_csv(decoded, read_options=pacsv.ReadOptions(block_size=8 * 1024 * 1024),
            convert_options=pacsv.ConvertOptions(column_types={
                "price": pa.string(), "amount": pa.string(), "id": pa.string(),
                "timestamp": pa.int64(), "local_timestamp": pa.int64(),
            } if exact_values else {}))
    except BaseException:
        decoded.close()
        raw.close()
        raise

    def batches():
        try:
            yield from reader
        finally:
            reader.close()
            decoded.close()
            raw.close()
    return pa.RecordBatchReader.from_batches(reader.schema, batches())


def iter_fused_book_batches(
    sources: Sequence[Mapping[str, Any]] | Mapping[str, Path], day: str, *, symbol: str = "BTCUSDC",
    minimum_levels: int = 20, batch_rows: int = 262_144,
    previous_state: Mapping[str, Any] | None = None,
    normalized_root: Path | None = None,
    output_start_us: int | None = None,
    output_end_us: int | None = None,
    next_sources: Mapping[str, Path] | None = None,
    observed_union: bool = False,
    allow_legacy_continuation: bool = False,
    stream_priority: Mapping[str, Any] | None = None,
    uniform_clock: bool = False,
    top_initial_state: Mapping[str, Any] | None = None,
    write_workers: int = 1,
    write_max_pending_bytes: int = DEFAULT_WRITE_MAX_PENDING_BYTES,
):
    """Return a bounded C++-backed stream and its mutable completion statistics.

    Inputs are source descriptors with ``source_id``, ``paths`` (or ``path``),
    and ``native_sequence``. Each source stays in its original row order. Its
    presentation clock is the causal prefix maximum of actual exchange E;
    same-presentation native message fragments are regrouped before applying
    sequence checks. Legacy mode emits a reconstructed view; observed-union
    mode preserves actual rows and samples separate source books without raw
    state differences. Multiple current files for one source require a future
    identity-aware merge and are rejected, never concatenated or deduplicated
    by timestamp. The daily publisher can still reuse already-included exact
    input hashes without invoking this reader.
    """
    import narrowgate_cpp
    from data.daily_raw import (BOOK_UNION_SCHEMA, DAILY_BOOK_MARKER, DAILY_BOOK_SCHEMA, FUSED_BOOK_SCHEMA, UNION_BOOK_SCHEMA,
                                _existing_book_source, book_stream_priority, daily_book_receipt,
                                daily_book_legacy_batch, daily_book_continuation)

    def is_observed_union(path):
        return (path is not None and Path(path).suffix == ".parquet" and
                (pq.ParquetFile(path).metadata.metadata or {}).get(b"narrowgate.book_fusion")
                in (BOOK_UNION_SCHEMA.encode(), DAILY_BOOK_MARKER.encode()))

    if isinstance(sources, Mapping):
        union_path = sources.get("canonical") if observed_union else None
        is_union = is_observed_union(union_path)
        daily = (daily_book_receipt(union_path) if is_union and
                 (pq.ParquetFile(union_path).metadata.metadata or {}).get(b"narrowgate.book_fusion")
                 == DAILY_BOOK_MARKER.encode() else None)
        if daily is not None:
            if set(sources) != {"canonical"}:
                raise ValueError("updating a daily multi-stream book requires explicit stream descriptors")
            stream_priority = daily["stream_priority"] if stream_priority is None else stream_priority
            if previous_state is None:
                previous_state = daily_book_continuation(daily.get("initial_state"))
            sources = [{"source_id": identity, "paths": [union_path], "native_sequence": None}
                       for identity in daily["stream_priority"]["stream_ids"]]
        else:
            sources = [{"source_id": key, "paths": ([union_path] if is_union else []) +
                    ([sources[key]] if key in sources and not (key == "canonical" and is_union) else []),
                    "native_sequence": None if key == "canonical" and (is_union or key not in sources) else key == "cryptohft" or (
                        key == "canonical" and key in sources
                        and _existing_book_source(Path(sources[key])) == "cryptohft")}
                       for key in ("cryptohft", "tardis", "canonical")]
    if not sources or batch_rows <= 0:
        raise ValueError("fusion requires sources and a positive batch size")
    daily_paths = {}
    for spec in sources:
        for value in spec["paths"] if "paths" in spec else [spec.get("path")]:
            if value is not None and Path(value).suffix == ".parquet":
                path = Path(value)
                if (pq.ParquetFile(path).metadata.metadata or {}).get(b"narrowgate.book_fusion") == DAILY_BOOK_MARKER.encode():
                    daily_paths[path] = daily_book_receipt(path)
    uniform_clock = uniform_clock or bool(daily_paths)
    source_ids = [str(source["source_id"]) for source in sources]
    if len(set(source_ids)) != len(source_ids):
        raise ValueError("fusion source_id must be unique")
    boundary_paths = {source: [] for source in source_ids}
    for source, path in (next_sources or {}).items():
        if source not in boundary_paths:
            raise ValueError("adjacent source_id is not declared")
        targets = source_ids if observed_union and is_observed_union(path) else [source]
        for target in targets:
            boundary_paths[target].append(path)
    if observed_union:
        # Same-provider archive revisions need an explicit identity merge, not
        # concatenation: prefix-max after the first file would move early new
        # observations to its final clock. Do not silently duplicate or reorder.
        for spec in sources:
            paths = spec["paths"] if "paths" in spec else [spec.get("path")]
            if len(paths) > 1 or len(boundary_paths[spec["source_id"]]) > 1:
                raise ValueError("multiple files for one source require an explicit identity merge")
    day_start, day_end = _day_start_us(day), _day_start_us(day) + DAY_US
    output_start = day_start if output_start_us is None else int(output_start_us)
    output_end = day_end if output_end_us is None else int(output_end_us)
    if not day_start <= output_start < output_end <= day_end or output_start % 100_000 or output_end % 100_000:
        raise ValueError("fusion output window must be cadence-aligned within its UTC day")
    priority = book_stream_priority(source_ids, contract=stream_priority)
    preferred = priority["preferred_index"]
    kernel = narrowgate_cpp.BookFusion(len(sources), preferred, minimum_levels)
    if observed_union:
        kernel.configure_raw_diff_output(False, allow_legacy_continuation)
    if previous_state is not None:
        if (previous_state.get("symbol") != symbol or previous_state.get("source_ids") != source_ids
                or previous_state.get("next_day_start_us") != day_start):
            raise ValueError("fusion continuation requires adjacent UTC days and identical sources/symbol")
        prior_priority = book_stream_priority(
            source_ids, preferred_index=previous_state["kernel"]["preferred"],
            contract=previous_state.get("stream_priority"),
        )
        if prior_priority != priority:
            raise ValueError("fusion continuation stream priority differs")
        kernel.restore(previous_state["kernel"])
    kernel.configure_output(output_start, output_end)
    writer = None
    if normalized_root is not None:
        kernel.configure_sampling(output_start, output_end, 100_000)
        writer = _ParquetPairWriter(Path(normalized_root), symbol, day, minimum_levels,
                                   write_workers=write_workers, write_max_pending_bytes=write_max_pending_bytes)
    stats: dict[str, Any] = {
        "status": "RUNNING", "day": day, "symbol": symbol,
        "stream_priority": priority,
        "included_sources": [spec["source_id"] for spec in sources if spec.get("paths") or spec.get("path")],
        "source_stats": {},
        "native_sequence_authority": False, "byte_lossless_archive": False,
        "presentation_clock": "causal_prefix_max_exchange_E",
        "observation_clock": "maximum_real_exchange_E_in_selected_state",
        "raw_representation": "observed_union.v1" if observed_union else "reconstructed_fusion.v1",
    }
    deferred: dict[str, list[list[int]]] = {key: [] for key in source_ids}
    deferred_observations: dict[str, list[dict]] = {key: [] for key in source_ids}
    observed_output_rows = 0

    def validate_union_identity(batch):
        for name in ("source_id", "source_native_sequence"):
            if name not in batch.schema.names or batch[name].null_count:
                raise ValueError(f"union {name} must be present and non-null")
        if pc.any(pc.invert(pc.is_in(batch["source_id"], value_set=pa.array(source_ids)))).as_py():
            raise ValueError("union contains an undeclared source_id")

    def original_observations(batch, presentation, real, index, native):
        """Preserve source fields; a view switch is never an archive event."""
        n = len(batch)
        columns = {name: batch[name] for name in batch.schema.names}
        original_timestamp = columns.get("source_timestamp_us", columns.get("timestamp"))
        columns.update(timestamp=pa.array(presentation), source_timestamp_us=original_timestamp,
                       source_observed_timestamp_us=pa.array(real),
                       source_id=pa.repeat(source_ids[index], n),
                       source_native_sequence=pa.repeat(native, n),
                       fusion_reason=pa.repeat("source_observation", n))
        return pa.Table.from_arrays([
            pc.cast(columns[field.name], field.type, safe=True) if field.name in columns
            else pa.nulls(n, type=field.type) for field in UNION_BOOK_SCHEMA
        ], schema=UNION_BOOK_SCHEMA)

    def integer(table, name, default=-1):
        if name not in table.schema.names:
            return np.full(len(table), default, dtype=np.int64)
        values = pc.cast(table[name], pa.int64())
        return np.asarray(pc.fill_null(values, default).to_numpy(zero_copy_only=False), dtype=np.int64)

    def units(values):
        # Decimal128's low 64 bits are the exact scaled integer. The selected
        # precision fits signed int64; Arrow rejects excess precision/overflow.
        decimal = pc.cast(values, pa.decimal128(18, 8), safe=True)
        if decimal.null_count:
            raise ValueError("fusion price/amount contains null")
        if isinstance(decimal, pa.ChunkedArray):
            decimal = decimal.combine_chunks()
        words = np.frombuffer(decimal.buffers()[1], dtype="<i8")
        return words[decimal.offset * 2:(decimal.offset + len(decimal)) * 2:2].copy()

    def source_batches(index, spec):
        paths = spec["paths"] if "paths" in spec else [spec.get("path")]
        if any(path is None for path in paths):
            raise ValueError("fusion source lacks paths")
        declared_native = spec.get("native_sequence", False)
        native = bool(declared_native)
        previous = (int(previous_state["kernel"]["sources"][index]["presentation_us"])
                    if previous_state is not None else 0)
        # An adjacent capture partition can repeat a prefix already consumed
        # during lookahead. A source may lag the merged checkpoint, especially
        # when its prefix had no snapshot and was never accepted. Resume the
        # presentation axis at the global consumed frontier, without changing
        # real E, sequence IDs, or the selected state's observation clock.
        previous = max(previous, int(previous_state["kernel"]["presentation_us"])
                       if previous_state is not None else 0, 0)
        ordinal = 0
        observed_rows = delayed = fallback = window_rows = 0
        max_delay = 0
        prior_rows = ((previous_state or {}).get("deferred_rows_by_source", {}).get(source_ids[index], []))
        if prior_rows:
            prior = np.array(prior_rows, dtype=np.int64, copy=True).reshape(-1, 14)
            prior[:, 0] = np.maximum.accumulate(np.maximum(prior[:, 0], previous))
            within = prior[:, 0] < output_end
            deferred[source_ids[index]].extend(prior[~within].tolist())
            raw_prior = None
            if observed_union:
                stored = (previous_state or {}).get("deferred_observations_by_source", {}).get(source_ids[index], [])
                if len(stored) != len(prior):
                    raise ValueError("union continuation lacks original deferred source observations")
                raw_prior = pa.Table.from_pylist(stored, schema=UNION_BOOK_SCHEMA)
                validate_union_identity(raw_prior)
                if pc.any(pc.not_equal(raw_prior["source_id"], source_ids[index])).as_py():
                    raise ValueError("deferred union source_id differs from its source slot")
                prior_native = pc.unique(raw_prior["source_native_sequence"]).to_pylist()
                if len(prior_native) != 1 or (declared_native is not None and prior_native[0] != native):
                    raise ValueError("union source sequence identity differs")
                native = prior_native[0]
                declared_native = native
                raw_prior = raw_prior.set_column(raw_prior.schema.get_field_index("timestamp"),
                                                "timestamp", pa.array(prior[:, 0]))
                deferred_observations[source_ids[index]].extend(raw_prior.filter(pa.array(~within)).to_pylist())
            if within.any():
                previous = max(previous, int(prior[within][-1, 0]))
                yield prior[within], raw_prior.filter(pa.array(within)) if raw_prior is not None else None
        selected_paths = [(raw, False) for raw in paths]
        selected_paths.extend((raw, True) for raw in boundary_paths[source_ids[index]])
        for raw, lookahead in selected_paths:
            path = Path(raw)
            union_file = False
            daily_file = False
            if path.suffix == ".parquet":
                parquet = pq.ParquetFile(path)
                marker = (parquet.metadata.metadata or {}).get(b"narrowgate.book_fusion")
                daily_file = marker == DAILY_BOOK_MARKER.encode()
                union_file = marker in (BOOK_UNION_SCHEMA.encode(), DAILY_BOOK_MARKER.encode())
                if observed_union and marker not in (None, BOOK_UNION_SCHEMA.encode(), DAILY_BOOK_MARKER.encode()):
                    raise ValueError("a reconstructed view cannot replace original observations in a raw union")
                if declared_native is None and not union_file:
                    native = _existing_book_source(path) == "cryptohft"
                    declared_native = native
                groups = None
                if lookahead:
                    clock_name = "observed_timestamp_us" if daily_file else "source_observed_timestamp_us" if union_file else "event_time" if native else "timestamp"
                    column = parquet.schema_arrow.get_field_index(clock_name)
                    if column < 0:
                        raise ValueError("lookahead source lacks its declared exchange clock")
                    bound = output_end // 1000 if native and not union_file else output_end
                    groups = []
                    for group in range(parquet.num_row_groups):
                        summary = parquet.metadata.row_group(group).column(column).statistics
                        # Missing statistics require reading, not assuming the
                        # remainder of a receive-day contains no earlier E.
                        if summary is None or summary.min is None or summary.null_count or summary.min < bound:
                            groups.append(group)
                batches = parquet.iter_batches(batch_size=batch_rows, row_groups=groups)
            else:
                batches = pacsv.open_csv(path, read_options=pacsv.ReadOptions(block_size=16 * 1024**2),
                    convert_options=pacsv.ConvertOptions(column_types={
                        "price": pa.string(), "amount": pa.string(),
                        "timestamp": pa.int64(), "local_timestamp": pa.int64(),
                        "is_snapshot": pa.bool_(),
                    }))
            for batch in batches:
                if daily_file:
                    if "top_only" not in batch.schema.names or batch["top_only"].null_count:
                        raise ValueError("daily book top-only declaration is missing")
                    batch = batch.filter(pc.invert(batch["top_only"]))
                    if not len(batch):
                        continue
                    # The kernel consumes book values, not queue effects. Raw
                    # re-publication of view-patch semantics needs the daily
                    # writer; never turn those patches into original messages.
                    if observed_union and normalized_root is None and (
                            pc.any(batch["observation_only"]).as_py()
                            or pc.any(pc.and_(batch["queue_rebase"], pc.invert(batch["is_snapshot"]))).as_py()
                            or batch["original_timestamp_us"].null_count):
                        raise ValueError("daily state patches require semantic-preserving publication; original-message union is unavailable")
                    batch = daily_book_legacy_batch(batch)
                if observed_union and (union_file or "source_native_sequence" in batch.schema.names
                                       or "source_id" in batch.schema.names):
                    validate_union_identity(batch)
                    batch = batch.filter(pc.equal(batch["source_id"], source_ids[index]))
                    if len(batch):
                        batch_native = pc.unique(batch["source_native_sequence"]).to_pylist()
                        if len(batch_native) != 1 or (declared_native is not None and batch_native[0] != native):
                            raise ValueError("union source sequence identity differs")
                        native = batch_native[0]
                        declared_native = native
                n = len(batch)
                if not n:
                    continue
                for column_name, expected in (("exchange", "binance-futures"), ("symbol", symbol)):
                    if (column_name not in batch.schema.names or batch[column_name].null_count
                            or pc.any(pc.not_equal(batch[column_name], expected)).as_py()):
                        raise ValueError(f"fusion source {column_name} mismatch")
                receive = integer(batch, "local_timestamp", 0)
                if native:
                    event = integer(batch, "event_time", 0)
                    transaction = integer(batch, "transaction_time", 0)
                    # Canonical native fields are venue millisecond clocks.
                    if np.any(event >= 10**14) or np.any(transaction >= 10**14):
                        raise ValueError("native E/T must be milliseconds")
                    real = np.where(event > 0, event, transaction) * 1000
                    fallback += int(np.count_nonzero((event <= 0) & (transaction > 0)))
                else:
                    real = integer(batch, "source_observed_timestamp_us" if "source_observed_timestamp_us" in batch.schema.names
                                   else "timestamp", 0)
                    transaction = np.full(n, -1, dtype=np.int64)
                if np.any(real <= 0):
                    raise ValueError("fusion source has no real exchange clock; receive fallback forbidden")
                if lookahead:
                    eligible = real < output_end
                    if not eligible.any():
                        continue
                    batch = batch.filter(pa.array(eligible))
                    real, receive, transaction = real[eligible], receive[eligible], transaction[eligible]
                    n = len(batch)
                # A next-day union's presentation axis can include that day's
                # checkpoint floor. Recover the original source axis for this
                # day's lookahead, never a future exchange observation.
                axis_field = "source_timestamp_us" if lookahead and union_file else "timestamp"
                source_axis = integer(batch, axis_field, 0) if not native else real
                presentation = np.maximum.accumulate(np.maximum(np.maximum(real, source_axis), previous))
                within = presentation < output_end
                window_rows += int(np.count_nonzero(within & (presentation >= output_start)))
                if within.any():
                    previous = int(presentation[within][-1])
                delta = presentation - real
                delayed += int(np.count_nonzero(delta))
                max_delay = max(max_delay, int(delta.max()))
                sides = pc.cast(batch["side"], pa.string())
                is_bid, is_ask = pc.equal(sides, "bid"), pc.equal(sides, "ask")
                if pc.any(pc.invert(pc.or_(is_bid, is_ask))).as_py():
                    raise ValueError("fusion source contains invalid side")
                matrix = np.column_stack((
                    presentation, real, receive,
                    np.asarray(batch["is_snapshot"].to_numpy(zero_copy_only=False), dtype=np.int64),
                    np.full(n, int(native), dtype=np.int64),
                    integer(batch, "first_update_id"), integer(batch, "final_update_id"),
                    integer(batch, "prev_final_update_id"), integer(batch, "last_update_id"),
                    transaction, np.asarray(is_ask.to_numpy(zero_copy_only=False), dtype=np.int64),
                    units(batch["price"]), units(batch["amount"]),
                    np.arange(ordinal, ordinal + n, dtype=np.int64),
                ))
                ordinal += n
                observed_rows += n
                if not lookahead and output_end == day_end:
                    deferred[source_ids[index]].extend(matrix[~within].tolist())
                original = original_observations(batch, presentation, real, index, native) if observed_union else None
                if original is not None and not lookahead and output_end == day_end:
                    deferred_observations[source_ids[index]].extend(original.filter(pa.array(~within)).to_pylist())
                # Later source records are not used to create this day's state.
                matrix = matrix[within]
                stats["source_stats"][source_ids[index]] = {
                    "input_rows_read": observed_rows, "presentation_delayed_rows": delayed,
                    "max_presentation_delay_us": max_delay, "exchange_T_fallback_rows": fallback,
                    "input_window_rows": window_rows,
                }
                if len(matrix):
                    yield matrix, original.filter(pa.array(within)) if original is not None else None
                if not within.all() and output_end < day_end:
                    break

    def decimal_strings(values):
        words = np.empty((len(values), 2), dtype="<i8")
        words[:, 0], words[:, 1] = values, np.where(values < 0, -1, 0)
        array = pa.Array.from_buffers(pa.decimal128(18, 8), len(values), [None, pa.py_buffer(words)])
        return pc.cast(array, pa.string())

    def output_batch(payload):
        if writer is not None:
            writer.append_fusion_batch(payload)
        keep = np.asarray(payload["timestamp"]) >= output_start
        if not keep.any():
            return None
        values = {key: np.asarray(value)[keep] for key, value in payload.items()
                  if not key.startswith("normalized_")}
        n = len(values["timestamp"])
        null = lambda typ: pa.nulls(n, type=typ)
        nullable_id = lambda name: pa.array(values[name], mask=values[name] < 0, type=pa.int64())
        prices, amounts = decimal_strings(values["price"]), decimal_strings(values["amount"])
        fields = {
            "exchange": pa.repeat("binance-futures", n), "symbol": pa.repeat(symbol, n),
            "timestamp": values["timestamp"], "local_timestamp": pa.array(values["local"], mask=values["local"] <= 0),
            "is_snapshot": values["snapshot"].astype(bool),
            "side": pc.take(pa.array(["bid", "ask"]), pa.array(values["side"])),
            "price": prices, "amount": amounts, "quantity": amounts,
            "event_time": values["event"] // 1000,
            "transaction_time": nullable_id("transaction"),
            "first_update_id": nullable_id("first"), "final_update_id": nullable_id("final"),
            "prev_final_update_id": nullable_id("previous"), "last_update_id": nullable_id("last"),
            "event_type": pc.take(pa.array(["update", "snapshot"]), pa.array(values["snapshot"])),
            "source_row": nullable_id("source_row"),
            "source_id": pc.take(pa.array(source_ids), pa.array(values["source"])),
            "fusion_reason": pc.take(pa.array(["source_update", "source_switch", "observation_refresh", "carried_opening_snapshot"]), pa.array(values["reason"])),
            "source_observed_timestamp_us": values["observed"],
        }
        return pa.RecordBatch.from_arrays([
            fields.get(field.name, null(field.type)) for field in FUSED_BOOK_SCHEMA
        ], schema=FUSED_BOOK_SCHEMA)

    def generate():
        nonlocal observed_output_rows
        iterators = [iter(source_batches(n, spec)) for n, spec in enumerate(sources)]
        try:
            current = [next(iterator, None) for iterator in iterators]
            while any(batch is not None for batch in current):
                limit = min(int(item[0][-1, 0]) for item in current if item is not None)
                chunks, ids, originals = [], [], []
                for index, item in enumerate(current):
                    if item is None:
                        continue
                    batch, original = item
                    take = int(np.searchsorted(batch[:, 0], limit, side="right"))
                    if take:
                        chunks.append(batch[:take])
                        ids.append(np.full(take, index, dtype=np.int64))
                        if original is not None:
                            originals.append(original.slice(0, take))
                    current[index] = ((batch[take:], original.slice(take) if original is not None else None)
                                      if take < len(batch) else next(iterators[index], None))
                merged, merged_ids = np.concatenate(chunks), np.concatenate(ids)
                original = pa.concat_tables(originals) if observed_union else None
                order = np.argsort(merged[:, 0], kind="stable")
                for offset in range(0, len(order), FUSION_PUSH_ROWS):
                    selection = order[offset:offset + FUSION_PUSH_ROWS]
                    if writer is not None:
                        writer.check()
                    # The kernel retains unfinished same-clock messages across
                    # pushes, so this changes allocation size, not book events.
                    payload = kernel.push_rows(merged_ids[selection], merged[selection])
                    if observed_union:
                        if writer is not None:
                            writer.append_fusion_batch(payload)
                        selected = original.take(pa.array(selection))
                        selected = selected.filter(pc.greater_equal(selected["timestamp"], output_start))
                        observed_output_rows += len(selected)
                        result = selected.combine_chunks().to_batches()[0] if len(selected) else None
                    else:
                        result = output_batch(payload)
                    if result is not None:
                        yield result
            payload = kernel.finish()
            if observed_union:
                if writer is not None:
                    writer.append_fusion_batch(payload)
                result = None
            else:
                result = output_batch(payload)
            if result is not None:
                yield result
            stats.update(kernel.stats())
            if observed_union:
                stats["output_rows"] = observed_output_rows
                stats["raw_state_differences_materialized"] = False
            stats["status"] = "COMPLETED"
            stats["continuation"] = {
                "symbol": symbol, "source_ids": source_ids, "next_day_start_us": day_end,
                "stream_priority": priority,
                "kernel": kernel.continuation(),
                "deferred_rows_by_source": deferred,
                "deferred_observations_by_source": deferred_observations,
            } if output_end == day_end else None
            stats["deferred_rows_by_source"] = {key: len(rows) for key, rows in deferred.items()}
            if writer is not None:
                writer.finish()
                if uniform_clock:
                    from data.book_top import apply_top_observations, from_daily_book_rows
                    tables = []
                    initial_top = top_initial_state
                    for path, receipt in daily_paths.items():
                        if receipt.get("top_initial_state") is not None:
                            if initial_top is not None and initial_top != receipt["top_initial_state"]:
                                raise ValueError("daily inputs disagree on initial top-only state")
                            initial_top = receipt["top_initial_state"]
                        parquet = pq.ParquetFile(path)
                        column = parquet.schema_arrow.get_field_index("top_only")
                        groups = []
                        for index in range(parquet.num_row_groups):
                            summary = parquet.metadata.row_group(index).column(column).statistics
                            if summary is None or summary.max is not False:
                                groups.append(index)
                        for batch in parquet.iter_batches(batch_size=65_536, row_groups=groups):
                            top = batch.filter(batch["top_only"])
                            if len(top):
                                tables.append(pa.Table.from_batches([top]))
                    top_rows = pa.concat_tables(tables) if tables else pa.Table.from_pylist([], schema=DAILY_BOOK_SCHEMA)
                    top_events = from_daily_book_rows(top_rows)
                    bbo, clock, top_state, top_metrics = apply_top_observations(
                        pq.read_table(writer.bbo_tmp), pq.read_table(writer.clock_tmp),
                        top_events, previous_top_state=initial_top)
                    # No final output is replaced before every top clock/value
                    # check succeeds.  Ordinary publication failures roll back
                    # the complete BBO/L2/clock triplet, not just two top files.
                    pq.write_table(bbo, writer.bbo_tmp, compression="zstd")
                    pq.write_table(clock, writer.clock_tmp, compression="zstd")
                    stats["top_initial_state"], stats["top_final_state"] = initial_top, top_state
                    stats["top_metrics"] = top_metrics
                writer.close(publish=True)
                stats["normalized"] = {kind: {"path": str(path), "sha256": _sha256(path),
                                              "rows": writer.rows}
                    for kind, path in (("bbo", writer.bbo_final), ("l2", writer.l2_final),
                                       ("clock", writer.clock_final))}
                stats["write_pipeline"] = writer.runtime()
        except BaseException:
            stats.update(kernel.stats())
            stats["status"] = "FAILED"
            if writer is not None:
                writer.close(publish=False)
                stats["write_pipeline"] = writer.runtime()
            raise
        finally:
            for iterator in iterators:
                iterator.close()

    return generate(), stats


def reconstruct_l2(
    raw_path: Path,
    *,
    output_root: Path,
    day: str,
    symbol: str = "BTCUSDC",
    levels: int = 20,
    cadence_ms: int = DEFAULT_CADENCE_MS,
    pilot_duration_s: int | None = None,
    timestamp_source: str = "exchange",
    continuation: L2Continuation | None = None,
    gap_policy: str = "missing",
    output_start_us: int | None = None,
    output_end_us: int | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    if Path(raw_path).suffix == ".parquet" and (
            pq.ParquetFile(raw_path).metadata.metadata or {}).get(b"narrowgate.book_fusion") in (
                b"observed_union.v1", b"narrowgate.daily_book.v1"):
        raise ValueError("multi-stream books require iter_fused_book_batches; single-book reconstruction is forbidden")
    if timestamp_source not in {"provider", "exchange"}:
        raise ValueError("timestamp_source must be provider or exchange")
    if gap_policy not in {"missing", "carry_forward"}:
        raise ValueError("gap_policy must be missing or carry_forward")
    started = time.perf_counter()
    day_start = _day_start_us(day)
    day_end = day_start + DAY_US
    requested_end = day_end
    if pilot_duration_s is not None:
        requested_end = min(day_end, day_start + int(pilot_duration_s) * 1_000_000)
    window_start = day_start if output_start_us is None else int(output_start_us)
    if output_end_us is not None:
        requested_end = min(requested_end, int(output_end_us))
    cadence_us = int(cadence_ms) * 1_000
    if cadence_ms <= 0 or levels <= 0 or (pilot_duration_s is not None and pilot_duration_s <= 0):
        raise ValueError("cadence, levels and pilot duration must be positive")
    if not day_start <= window_start < requested_end <= day_end:
        raise ValueError("output window must be within its UTC day")
    if window_start % cadence_us or requested_end % cadence_us:
        raise ValueError("output window must align with cadence")
    inherited = continuation is not None and continuation.next_day_start_us is not None
    if inherited and (continuation.next_day_start_us != day_start
                      or continuation.symbol != symbol
                      or continuation.timestamp_source != timestamp_source):
        raise ValueError("continuation requires adjacent days and the same market/clock")
    if inherited and ((continuation.last_exchange_us if timestamp_source == "exchange"
                       else continuation.last_local_us) >= day_start):
        raise ValueError("continuation observation must precede the next UTC day")
    # Do not mutate the caller's last complete state if reading/writing fails.
    book = copy.deepcopy(continuation.book) if inherited else OrderBookState()
    stats = ReconstructionStats()
    gaps = GapHistogram()
    writer = _ParquetPairWriter(output_root, symbol, day, levels,
                               timestamp_source=timestamp_source)
    initialized = bool(inherited and continuation.initialized)
    current_message: tuple[int, int, bool] | None = None
    current_bucket: int | None = None
    previous_local_us = continuation.last_local_us if initialized else None
    previous_exchange_us = continuation.last_exchange_us if initialized else None
    maximum_applied_exchange_us = continuation.last_exchange_us if initialized else 0
    last_applied_local_us = continuation.last_local_us if initialized else 0
    inherited_exchange_us = maximum_applied_exchange_us or None
    inherited_local_us = last_applied_local_us or None
    output_first_ms: int | None = None
    output_last_ms: int | None = None
    first_exchange_cut_us: int | None = None
    last_exchange_cut_us: int | None = None
    snapshot_seen_at_start = False
    stopped_at_cut = False
    unobserved_intervals: list[dict[str, Any]] = []
    last_emitted_observation_us: int | None = None
    observed_output_timestamps_ms: list[int] = []
    observed_output_clocks_us: list[int] = []
    carried_rows = 0
    initial_observation_us = inherited_exchange_us if timestamp_source == "exchange" else inherited_local_us

    def record_unobserved(start_us: int, end_us: int, last_observation_us: int | None) -> None:
        if end_us > start_us:
            unobserved_intervals.append({
                "start_us": start_us, "end_us": end_us,
                "last_observation_us": last_observation_us,
                "reason": "no_valid_observation_in_bucket",
                "update_coverage": "unknown_not_confirmed_no_updates",
            })

    def emit(bucket: int | None, *, carried: bool = False) -> None:
        nonlocal output_first_ms, output_last_ms
        nonlocal first_exchange_cut_us, last_exchange_cut_us
        nonlocal last_emitted_observation_us
        nonlocal carried_rows
        nonlocal initial_observation_us
        if bucket is None or not initialized:
            return
        boundary_us = (int(bucket) + 1) * cadence_us
        if (boundary_us < window_start or boundary_us > requested_end or boundary_us >= day_end
                or (output_end_us is not None and boundary_us == requested_end)):
            return
        bids, asks = book.top_levels(levels)
        if len(bids) < levels or len(asks) < levels:
            stats.insufficient_depth_buckets += 1
            return
        if bids[0][0] <= 0.0 or asks[0][0] <= bids[0][0]:
            stats.invalid_spread_buckets += 1
            return
        timestamp_ms = boundary_us // 1_000
        last_emitted_observation_us = (maximum_applied_exchange_us if timestamp_source == "exchange"
                                      else last_applied_local_us)
        if last_emitted_observation_us >= boundary_us:
            raise ValueError("source observation must precede output boundary")
        if output_first_ms is None and last_emitted_observation_us < window_start:
            initial_observation_us = last_emitted_observation_us
        writer.append(
            timestamp_ms,
            bids,
            asks,
            exchange_cut_us=maximum_applied_exchange_us,
            last_provider_local_us=last_applied_local_us,
            carried=carried,
        )
        stats.emitted_rows += 1
        if carried:
            carried_rows += 1
        else:
            observed_output_timestamps_ms.append(timestamp_ms)
            observed_output_clocks_us.append(last_emitted_observation_us)
        output_first_ms = timestamp_ms if output_first_ms is None else output_first_ms
        output_last_ms = timestamp_ms
        first_exchange_cut_us = (
            maximum_applied_exchange_us
            if first_exchange_cut_us is None
            else first_exchange_cut_us
        )
        last_exchange_cut_us = maximum_applied_exchange_us

    def carry_until(first_boundary: int, end_boundary: int) -> None:
        if gap_policy == "carry_forward":
            for boundary in range(max(window_start, first_boundary), end_boundary, cadence_us):
                emit(boundary // cadence_us - 1, carried=True)

    try:
        for batch in _open_csv(raw_path):
            fused_observations = "source_observed_timestamp_us" in batch.schema.names
            if fused_observations and timestamp_source != "exchange":
                raise ValueError("fused source receive clocks are provenance, not one provider timeline")
            columns = {
                name: batch.column(name).to_numpy(zero_copy_only=False)
                for name in (
                    "exchange",
                    "symbol",
                    "timestamp",
                    "local_timestamp",
                    "is_snapshot",
                    "side",
                    "price",
                    "amount",
                )
            }
            observed_clocks = (batch.column("source_observed_timestamp_us").to_numpy(zero_copy_only=False)
                               if fused_observations else columns["timestamp"])
            for exchange, row_symbol, exchange_us, local_us, is_snapshot, side, price, amount, observed_us in zip(
                columns["exchange"],
                columns["symbol"],
                columns["timestamp"],
                columns["local_timestamp"],
                columns["is_snapshot"],
                columns["side"],
                columns["price"],
                columns["amount"],
                observed_clocks,
                strict=True,
            ):
                stats.raw_rows += 1
                exchange_us = int(exchange_us)
                local_us = int(local_us) if local_us is not None and math.isfinite(float(local_us)) else 0
                observed_us = int(observed_us)
                if observed_us <= 0 or observed_us > exchange_us:
                    raise ValueError("raw source observation timestamp is invalid or in the future")
                snapshot = bool(is_snapshot)
                clock_us = exchange_us if timestamp_source == "exchange" else local_us
                if clock_us >= requested_end:
                    stopped_at_cut = True
                    break
                if (
                    str(exchange) != "binance-futures"
                    or str(row_symbol) != symbol
                    or str(side) not in {"bid", "ask"}
                    or not math.isfinite(float(price))
                    or float(price) <= 0.0
                    or not math.isfinite(float(amount))
                    or float(amount) < 0.0
                    or not (day_start <= clock_us < day_end)
                ):
                    stats.invalid_rows += 1
                    continue
                if local_us < exchange_us:
                    stats.causal_violations += 1
                # Technical provider grouping is independent of output clock.
                # Equal exchange timestamps do not identify the same message.
                message = (exchange_us, local_us, snapshot)
                if message != current_message:
                    stats.logical_messages += 1
                    if previous_local_us is not None:
                        if local_us < previous_local_us:
                            stats.local_clock_reversals += 1
                            if timestamp_source == "provider":
                                raise ValueError("provider-clock normalization cannot reorder a regressing source")
                        else:
                            gaps.add(local_us - previous_local_us)
                    if (
                        previous_exchange_us is not None
                        and exchange_us < previous_exchange_us
                    ):
                        stats.exchange_clock_reversals += 1
                        if timestamp_source == "exchange":
                            raise ValueError("exchange-clock normalization cannot reorder a regressing source")
                    previous_local_us = local_us
                    previous_exchange_us = exchange_us
                    next_bucket = clock_us // cadence_us
                    if current_bucket is not None and next_bucket != current_bucket:
                        emit(current_bucket)
                        carry_until((current_bucket + 2) * cadence_us,
                                    min((next_bucket + 1) * cadence_us, requested_end))
                    elif current_bucket is None and inherited:
                        carry_until(window_start, min((next_bucket + 1) * cadence_us, requested_end))
                    current_bucket = next_bucket
                    current_message = message
                    if snapshot:
                        book.reset()
                        initialized = True
                        stats.snapshot_messages += 1
                        if stats.logical_messages == 1:
                            snapshot_seen_at_start = True
                    else:
                        stats.update_messages += 1
                if not initialized:
                    stats.pre_snapshot_rows += 1
                    continue
                book.apply(str(side), float(price), float(amount))
                maximum_applied_exchange_us = max(
                    maximum_applied_exchange_us, observed_us
                )
                last_applied_local_us = local_us
            if stopped_at_cut:
                break
        emit(current_bucket)
        carry_until(window_start if current_bucket is None else (current_bucket + 2) * cadence_us,
                    requested_end)
        if not initialized:
            raise ValueError("primary L2 has no snapshot or inherited book")
        writer.close(publish=True)
    except BaseException:
        writer.close(publish=False)
        raise

    # Held rows do not erase missing-observation intervals or prove silence.
    expected = window_start
    observed_before_gap = initial_observation_us
    for timestamp_ms, observed_us in zip(observed_output_timestamps_ms, observed_output_clocks_us, strict=True):
        record_unobserved(expected, timestamp_ms * 1000, observed_before_gap)
        expected = timestamp_ms * 1000 + cadence_us
        observed_before_gap = observed_us
    record_unobserved(expected, requested_end, observed_before_gap)

    # Preserve the legacy midnight exclusion for standalone days; an inherited
    # book can also produce the next day's midnight row with its old clock.
    first_boundary = window_start
    if window_start == day_start and not (inherited and gap_policy == "carry_forward"):
        first_boundary += cadence_us
    exclusive_end = requested_end
    if output_end_us is None and requested_end < day_end:
        exclusive_end += cadence_us  # legacy pilot includes its last right edge
    possible_rows = max(1, (exclusive_end - first_boundary) // cadence_us)
    bucket_density = float(stats.emitted_rows / possible_rows)
    freshness_coverage, output_p99_gap_ms = _freshness_union_coverage(
        observed_output_timestamps_ms,
        start_ms=window_start // 1_000,
        end_ms=requested_end // 1_000,
        freshness_ms=DEFAULT_FRESHNESS_MS,
    )
    gap_summary = gaps.as_dict()
    quality: dict[str, Any] = {
        "schema_version": "narrowgate.normalized_tardis_l2_day.v1",
        "observation_schema": OBSERVATION_SCHEMA,
        "gap_policy": gap_policy,
        "source_id": SOURCE_ID,
        "dataset_id": EXCHANGE_DATASET_ID if timestamp_source == "exchange" else DATASET_ID,
        "symbol": symbol,
        "day": day,
        "clock_source": "tardis_exchange" if timestamp_source == "exchange" else "tardis_provider_local",
        "clock_unit": "microseconds_since_unix_epoch_utc",
        "causal_cut": ("raw timestamp < normalized right boundary" if timestamp_source == "exchange"
                       else "raw local_timestamp < normalized right boundary"),
        "cadence_ms": cadence_ms,
        "levels": levels,
        "complete_day": window_start == day_start and requested_end == day_end,
        "output_start_us": window_start,
        "output_end_us": requested_end,
        "pilot_duration_s": pilot_duration_s,
        "snapshot_seen_at_start": snapshot_seen_at_start,
        "raw_rows": stats.raw_rows,
        "logical_messages": stats.logical_messages,
        "snapshot_messages": stats.snapshot_messages,
        "update_messages": stats.update_messages,
        "invalid_rows": stats.invalid_rows,
        "pre_snapshot_rows": stats.pre_snapshot_rows,
        "causal_violations": stats.causal_violations,
        "local_clock_reversals": stats.local_clock_reversals,
        "exchange_clock_reversals": stats.exchange_clock_reversals,
        "emitted_rows": stats.emitted_rows,
        "source_observed_rows": len(observed_output_timestamps_ms),
        "carried_forward_rows": carried_rows,
        "possible_rows": possible_rows,
        "bucket_density": bucket_density,
        "freshness_ms": DEFAULT_FRESHNESS_MS,
        "freshness_union_coverage": freshness_coverage,
        "output_p99_gap_ms": output_p99_gap_ms,
        "first_timestamp_ms": output_first_ms,
        "last_timestamp_ms": output_last_ms,
        "insufficient_depth_buckets": stats.insufficient_depth_buckets,
        "invalid_spread_buckets": stats.invalid_spread_buckets,
        "logical_message_gap": gap_summary,
        "unobserved_intervals": unobserved_intervals,
        "gap_semantics": "missing_output_not_confirmed_no_updates_or_no_trades",
        "continuation_inherited": inherited,
        "continuation_requested": continuation is not None,
        "inherited_last_exchange_timestamp_us": inherited_exchange_us,
        "inherited_last_provider_timestamp_us": inherited_local_us,
        "last_observed_exchange_timestamp_us": maximum_applied_exchange_us or None,
        "last_observed_provider_timestamp_us": last_applied_local_us or None,
        "exchange_timestamp_summary": {
            "first_applied_cut_us": first_exchange_cut_us,
            "last_applied_cut_us": last_exchange_cut_us,
            "cut_is_maximum_exchange_timestamp_applied_before_provider_boundary": timestamp_source == "provider",
            "cut_is_maximum_exchange_timestamp_applied_before_exchange_boundary": timestamp_source == "exchange",
        },
        "observed_internal_gap_valid": bool(
            gap_summary["p99_upper_us"] is not None
            and int(gap_summary["p99_upper_us"]) <= 500_000
            and int(gap_summary["maximum_us"]) <= 5_000_000
        ),
        "native_binance_sequence_ids_present": False,
        "native_sequence_continuity_proven": False,
        "exact_queue_policy_eligible": False,
        "aws_tokyo_receive_time": False,
        "policy_visible": False,
        "live_transport_eligible": False,
        "normalized_replay_candidate_before_cross_channel": bool(
            gap_policy == "missing"
            and window_start == day_start and requested_end == day_end
            and snapshot_seen_at_start
            and stats.causal_violations == 0
            and stats.local_clock_reversals == 0
            and stats.invalid_spread_buckets == 0
            and freshness_coverage >= 0.99
            and output_p99_gap_ms <= DEFAULT_FRESHNESS_MS
            and gap_summary["p99_upper_us"] is not None
            and int(gap_summary["p99_upper_us"]) <= 500_000
            and int(gap_summary["maximum_us"]) <= 5_000_000
        ),
    }
    elapsed = time.perf_counter() - started
    quality["reconstruction_elapsed_s"] = elapsed
    quality["reconstruction_raw_rows_per_s"] = float(
        stats.raw_rows / max(elapsed, 1e-9)
    )
    if gap_policy == "carry_forward":
        quality["dataset_id"] += "_carried_view_v2"
    if continuation is not None:
        continuation.book = book
        continuation.initialized = initialized
        continuation.next_day_start_us = requested_end
        continuation.symbol = symbol
        continuation.timestamp_source = timestamp_source
        continuation.last_exchange_us = maximum_applied_exchange_us
        continuation.last_local_us = last_applied_local_us
    return writer.bbo_final, writer.l2_final, quality


def audit_book_ticker(
    raw_path: Path,
    normalized_bbo: Path,
    *,
    day: str,
    pilot_duration_s: int | None = None,
    max_age_ms: int = CROSS_CHANNEL_MAX_AGE_MS,
    tick_size: float = 0.1,
    timestamp_source: str = "exchange",
) -> dict[str, Any]:
    started = time.perf_counter()
    table = pq.read_table(normalized_bbo)
    boundaries_us = table.column("timestamp").to_numpy() * 1_000
    bid = table.column("best_bid").to_numpy()
    bid_qty = table.column("best_bid_qty").to_numpy()
    ask = table.column("best_ask").to_numpy()
    ask_qty = table.column("best_ask_qty").to_numpy()
    index = 0
    latest: tuple[int, float, float, float, float] | None = None
    comparable = 0
    exact_price = 0
    within_tick = 0
    exact_quantity = 0
    quantity_close = 0
    age = GapHistogram()
    causal_violations = 0
    local_reversals = 0
    previous_local: int | None = None
    previous_clock: int | None = None
    stop = False
    mismatch_examples: list[dict[str, Any]] = []
    raw_rows = 0
    day_start = _day_start_us(day)
    requested_end = day_start + DAY_US
    if pilot_duration_s is not None:
        requested_end = min(
            requested_end, day_start + int(pilot_duration_s) * 1_000_000
        )

    def compare_until(boundary_limit: int) -> None:
        nonlocal index, comparable, exact_price, within_tick
        nonlocal exact_quantity, quantity_close
        while index < len(boundaries_us) and boundaries_us[index] <= boundary_limit:
            if latest is not None:
                local_us, ticker_bid, ticker_bid_qty, ticker_ask, ticker_ask_qty = latest
                lag = int(boundaries_us[index] - local_us)
                if 0 <= lag <= max_age_ms * 1_000:
                    comparable += 1
                    age.add(lag)
                    price_error = max(
                        abs(float(bid[index]) - ticker_bid),
                        abs(float(ask[index]) - ticker_ask),
                    )
                    quantity_error = max(
                        abs(float(bid_qty[index]) - ticker_bid_qty),
                        abs(float(ask_qty[index]) - ticker_ask_qty),
                    )
                    if price_error <= 1e-9:
                        exact_price += 1
                    if price_error <= tick_size + 1e-9:
                        within_tick += 1
                    if quantity_error <= 1e-9:
                        exact_quantity += 1
                    quantity_scale = max(
                        abs(ticker_bid_qty), abs(ticker_ask_qty), 1e-9
                    )
                    if quantity_error <= max(0.001, 0.05 * quantity_scale):
                        quantity_close += 1
                    if price_error > tick_size + 1e-9 and len(mismatch_examples) < 20:
                        mismatch_examples.append(
                            {
                                "timestamp_ms": int(boundaries_us[index] // 1_000),
                                "book_ticker_age_us": lag,
                                "l2_bid": float(bid[index]),
                                "ticker_bid": ticker_bid,
                                "l2_ask": float(ask[index]),
                                "ticker_ask": ticker_ask,
                            }
                        )
            index += 1

    for batch in _open_csv(raw_path):
        timestamp = batch.column("timestamp").to_numpy(zero_copy_only=False)
        local = batch.column("local_timestamp").to_numpy(zero_copy_only=False)
        ask_amount = batch.column("ask_amount").to_numpy(zero_copy_only=False)
        ask_price = batch.column("ask_price").to_numpy(zero_copy_only=False)
        bid_price = batch.column("bid_price").to_numpy(zero_copy_only=False)
        bid_amount = batch.column("bid_amount").to_numpy(zero_copy_only=False)
        for exchange_us, local_us, aq, ap, bp, bq in zip(
            timestamp,
            local,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
            strict=True,
        ):
            raw_rows += 1
            exchange_us = int(exchange_us)
            local_us = int(local_us)
            clock_us = exchange_us if timestamp_source == "exchange" else local_us
            if timestamp_source == "exchange" and previous_clock is not None and clock_us < previous_clock:
                raise ValueError("exchange-clock ticker audit cannot reorder a regressing source")
            previous_clock = clock_us
            if clock_us >= requested_end:
                stop = True
                break
            compare_until(clock_us)
            if local_us < exchange_us:
                causal_violations += 1
            if previous_local is not None and local_us < previous_local:
                local_reversals += 1
            previous_local = local_us
            latest = (
                clock_us,
                float(bp),
                float(bq),
                float(ap),
                float(aq),
            )
        if stop:
            break
    compare_until(2**63 - 1)
    denominator = max(1, comparable)
    elapsed = time.perf_counter() - started
    return {
        "comparison_clock": timestamp_source,
        "book_ticker_raw_rows": raw_rows,
        "book_ticker_audit_elapsed_s": elapsed,
        "book_ticker_raw_rows_per_s": float(raw_rows / max(elapsed, 1e-9)),
        "book_ticker_rows_compared": comparable,
        "book_ticker_comparable_ratio": float(comparable / max(1, len(boundaries_us))),
        "book_ticker_price_exact_ratio": float(exact_price / denominator),
        "book_ticker_price_within_one_tick_ratio": float(within_tick / denominator),
        "book_ticker_quantity_exact_ratio": float(exact_quantity / denominator),
        "book_ticker_quantity_close_ratio": float(quantity_close / denominator),
        "book_ticker_age": age.as_dict(),
        "book_ticker_causal_violations": causal_violations,
        "book_ticker_local_clock_reversals": local_reversals,
        "price_exact_tolerance": 1e-9,
        "price_one_tick_tolerance": tick_size,
        "quantity_close_tolerance": "max(0.001 BTC, 5% of ticker top-size)",
        "price_gate": (
            f"exact>={CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO} and "
            f"within_one_tick>={CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO}"
        ),
        "threshold_provenance": (
            "v1 engineering QA envelope frozen before the 2025 batch; it "
            "allows independent-channel publication races and is not a "
            "statistical, policy, or exact-queue threshold"
        ),
        "quantity_is_diagnostic_not_gate": True,
        "mismatch_examples": mismatch_examples,
    }


def _sample_l2(path: Path, *, levels: int, stride: int) -> dict[str, np.ndarray]:
    columns = ["timestamp"]
    for level in range(1, levels + 1):
        columns.extend(
            (
                f"bid_px_{level}",
                f"bid_qty_{level}",
                f"ask_px_{level}",
                f"ask_qty_{level}",
            )
        )
    chunks: dict[str, list[np.ndarray]] = {column: [] for column in columns}
    offset = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=50_000, columns=columns):
        take = np.arange(batch.num_rows)
        keep = ((take + offset) % stride) == 0
        for name in columns:
            chunks[name].append(
                batch.column(name).to_numpy(zero_copy_only=False)[keep]
            )
        offset += batch.num_rows
    return {
        name: np.concatenate(values) if values else np.asarray([])
        for name, values in chunks.items()
    }


def _l2_columns(levels: int) -> list[str]:
    columns = ["timestamp"]
    for level in range(1, levels + 1):
        columns.extend(
            (
                f"bid_px_{level}",
                f"bid_qty_{level}",
                f"ask_px_{level}",
                f"ask_qty_{level}",
            )
        )
    return columns


def _take_l2_rows(
    path: Path, *, levels: int, row_indices: np.ndarray
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    requested = np.unique(np.asarray(row_indices, dtype=np.int64))
    requested = requested[requested >= 0]
    columns = _l2_columns(levels)
    chunks: dict[str, list[np.ndarray]] = {name: [] for name in columns}
    selected_indices: list[np.ndarray] = []
    offset = 0
    cursor = 0
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=50_000, columns=columns
    ):
        end = offset + batch.num_rows
        begin_cursor = cursor
        while cursor < len(requested) and requested[cursor] < end:
            cursor += 1
        chosen = requested[begin_cursor:cursor]
        if len(chosen):
            local = pa.array(chosen - offset)
            selected_indices.append(chosen)
            for name in columns:
                chunks[name].append(
                    batch.column(name).take(local).to_numpy(zero_copy_only=False)
                )
        offset = end
        if cursor == len(requested):
            break
    indices = (
        np.concatenate(selected_indices)
        if selected_indices
        else np.asarray([], dtype=np.int64)
    )
    values = {
        name: np.concatenate(parts) if parts else np.asarray([])
        for name, parts in chunks.items()
    }
    return indices, values


def _top20_metrics(
    left: Mapping[str, np.ndarray],
    right: Mapping[str, np.ndarray],
    *,
    valid: np.ndarray,
    levels: int,
    tick_size: float,
) -> dict[str, Any]:
    price_total = 0
    price_exact = 0
    price_one_tick = 0
    quantity_total = 0
    quantity_exact = 0
    quantity_close = 0
    for level in range(1, levels + 1):
        for side in ("bid", "ask"):
            price_name = f"{side}_px_{level}"
            quantity_name = f"{side}_qty_{level}"
            price_error = np.abs(left[price_name][valid] - right[price_name][valid])
            quantity_left = left[quantity_name][valid]
            quantity_right = right[quantity_name][valid]
            quantity_error = np.abs(quantity_left - quantity_right)
            price_total += len(price_error)
            price_exact += int((price_error <= 1e-9).sum())
            price_one_tick += int((price_error <= tick_size + 1e-9).sum())
            quantity_total += len(quantity_error)
            quantity_exact += int((quantity_error <= 1e-9).sum())
            quantity_close += int(
                (
                    quantity_error
                    <= np.maximum(
                        0.001,
                        0.05 * np.maximum(np.abs(quantity_right), 1e-9),
                    )
                ).sum()
            )
    return {
        "matched_rows": int(valid.sum()),
        "matched_ratio": float(valid.mean()) if len(valid) else 0.0,
        "top20_price_exact_ratio": float(price_exact / max(1, price_total)),
        "top20_price_within_one_tick_ratio": float(
            price_one_tick / max(1, price_total)
        ),
        "top20_quantity_exact_ratio": float(
            quantity_exact / max(1, quantity_total)
        ),
        "top20_quantity_close_ratio": float(
            quantity_close / max(1, quantity_total)
        ),
    }


def compare_normalized_sources(
    tardis_l2: Path,
    cryptohft_l2: Path,
    *,
    tardis_clock: Path | None = None,
    levels: int = 20,
    stride: int = 100,
    max_nearest_lag_ms: int = 100,
    tick_size: float = 0.1,
) -> dict[str, Any]:
    """Compare overlap under causal exchange-time and nearest-clock views."""

    started = time.perf_counter()
    tardis = _sample_l2(tardis_l2, levels=levels, stride=stride)
    left_ts = tardis["timestamp"].astype(np.int64)
    right_ts = (
        pq.read_table(cryptohft_l2, columns=["timestamp"])
        .column("timestamp")
        .to_numpy()
        .astype(np.int64)
    )
    if not len(left_ts) or not len(right_ts):
        return {"dual_source_available": False, "matched_rows": 0}
    if bool(np.any(np.diff(right_ts) < 0)):
        return {
            "dual_source_available": True,
            "cryptohft_timestamp_monotonic": False,
            "comparison_valid": False,
            "cannot_upgrade_native_sequence_or_exact_queue": True,
        }
    insertion = np.searchsorted(right_ts, left_ts)
    hi = np.clip(insertion, 0, len(right_ts) - 1)
    lo = np.clip(insertion - 1, 0, len(right_ts) - 1)
    choose_hi = np.abs(right_ts[hi] - left_ts) < np.abs(right_ts[lo] - left_ts)
    nearest_index = np.where(choose_hi, hi, lo)
    nearest_lag = right_ts[nearest_index] - left_ts
    nearest_valid = np.abs(nearest_lag) <= int(max_nearest_lag_ms)

    causal_index = np.full(len(left_ts), -1, dtype=np.int64)
    causal_lag = np.full(len(left_ts), np.iinfo(np.int64).max, dtype=np.int64)
    causal_valid = np.zeros(len(left_ts), dtype=bool)
    exchange_cut_ms = np.asarray([], dtype=np.int64)
    if tardis_clock is not None and tardis_clock.is_file():
        clock = pq.read_table(
            tardis_clock, columns=["exchange_cut_timestamp_us"]
        ).column("exchange_cut_timestamp_us").to_numpy()
        exchange_cut_ms = clock[::stride].astype(np.int64) // 1_000
        if len(exchange_cut_ms) != len(left_ts):
            raise ValueError("Tardis L2 and clock sidecar row counts differ")
        causal_index = np.searchsorted(
            right_ts, exchange_cut_ms, side="right"
        ).astype(np.int64) - 1
        safe = np.clip(causal_index, 0, len(right_ts) - 1)
        causal_lag = exchange_cut_ms - right_ts[safe]
        causal_valid = (
            (causal_index >= 0)
            & (causal_lag >= 0)
            & (causal_lag <= int(max_nearest_lag_ms))
        )

    requested_indices = np.concatenate(
        (
            nearest_index[nearest_valid],
            causal_index[causal_valid],
        )
    )
    selected_indices, selected = _take_l2_rows(
        cryptohft_l2, levels=levels, row_indices=requested_indices
    )
    positions = {int(index): offset for offset, index in enumerate(selected_indices)}

    def align(indices: np.ndarray, valid: np.ndarray) -> dict[str, np.ndarray]:
        aligned: dict[str, np.ndarray] = {}
        safe_positions = np.zeros(len(indices), dtype=np.int64)
        for offset in np.flatnonzero(valid):
            safe_positions[offset] = positions[int(indices[offset])]
        for name in _l2_columns(levels):
            values = np.zeros(len(indices), dtype=selected[name].dtype)
            if valid.any():
                values[valid] = selected[name][safe_positions[valid]]
            aligned[name] = values
        return aligned

    nearest_values = align(nearest_index, nearest_valid)
    nearest_metrics = _top20_metrics(
        tardis,
        nearest_values,
        valid=nearest_valid,
        levels=levels,
        tick_size=tick_size,
    )
    nearest_metrics.update(
        {
            "clock": "clock-agnostic nearest normalized timestamp",
            "is_causality_proof": False,
            "max_abs_lag_ms": max_nearest_lag_ms,
            "lag_abs_p50_ms": float(
                np.quantile(np.abs(nearest_lag[nearest_valid]), 0.50)
            )
            if nearest_valid.any()
            else None,
            "lag_abs_p99_ms": float(
                np.quantile(np.abs(nearest_lag[nearest_valid]), 0.99)
            )
            if nearest_valid.any()
            else None,
        }
    )
    causal_metrics: dict[str, Any]
    if len(exchange_cut_ms):
        causal_values = align(causal_index, causal_valid)
        causal_metrics = _top20_metrics(
            tardis,
            causal_values,
            valid=causal_valid,
            levels=levels,
            tick_size=tick_size,
        )
        causal_metrics.update(
            {
                "clock": "Tardis applied exchange cut as-of CryptoHFT timestamp",
                "future_crypto_rows_forbidden": True,
                "max_backward_age_ms": max_nearest_lag_ms,
                "backward_age_p50_ms": float(
                    np.quantile(causal_lag[causal_valid], 0.50)
                )
                if causal_valid.any()
                else None,
                "backward_age_p99_ms": float(
                    np.quantile(causal_lag[causal_valid], 0.99)
                )
                if causal_valid.any()
                else None,
            }
        )
    else:
        causal_metrics = {
            "available": False,
            "reason": "missing Tardis exchange-cut clock sidecar",
        }
    output = {
        "dual_source_available": True,
        "cryptohft_timestamp_monotonic": True,
        "sample_stride_rows": stride,
        "tardis_sample_rows": len(left_ts),
        "cryptohft_rows": len(right_ts),
        "exchange_time_causal_asof": causal_metrics,
        "clock_agnostic_nearest": nearest_metrics,
        "price_exact_tolerance": 1e-9,
        "price_one_tick_tolerance": tick_size,
        "quantity_close_tolerance": "max(0.001 BTC, 5% of CryptoHFTData level size)",
        "provider_local_is_aws_receive_time": False,
        "policy_visible": False,
        "cannot_upgrade_native_sequence_or_exact_queue": True,
    }
    output["comparison_elapsed_s"] = time.perf_counter() - started
    return output


def _download_rows(manifest: Path, day: str) -> dict[str, Mapping[str, Any]]:
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    rows = {}
    for row in payload.get("downloads", []):
        if str(row.get("day")) != day:
            continue
        dataset = str(row["dataset"])
        if dataset in rows:
            raise ValueError(f"duplicate source selection for {day}: {dataset}")
        rows[dataset] = row
    # A batch-wide download flag or absent auxiliary ticker must not hide an
    # available L2 source. Missing primary L2 still has no reconstructable book.
    missing = {INCREMENTAL_L2}.difference(rows)
    if missing:
        raise RuntimeError(f"{day} is missing Tardis datasets: {sorted(missing)}")
    return rows


def normalize_day(
    manifest: Path,
    *,
    day: str,
    output_root: Path,
    cryptohft_root: Path | None = None,
    pilot_duration_s: int | None = None,
    force: bool = False,
    timestamp_source: str = "exchange",
    continuation: L2Continuation | None = None,
    gap_policy: str = "missing",
    output_start_us: int | None = None,
    output_end_us: int | None = None,
) -> dict[str, Any]:
    rows = _download_rows(manifest, day)
    manifest_sha256 = _sha256(manifest)
    l2_row = rows[INCREMENTAL_L2]
    ticker_row = rows.get(BOOK_TICKER)
    l2_raw = resolve_tardis_artifact_path(str(l2_row["path"]))
    ticker_raw = resolve_tardis_artifact_path(str(ticker_row["path"])) if ticker_row else None
    quality_path = output_root / "quality" / f"BTCUSDC-{day}.json"
    cached = None
    if quality_path.is_file():
        cached = json.loads(quality_path.read_text(encoding="utf-8"))
        expected_clock = "tardis_exchange" if timestamp_source == "exchange" else "tardis_provider_local"
        if cached.get("clock_source") != expected_clock:
            raise ValueError("normalization clock changed: select a separate output root")
        if bool(cached.get("continuation_requested")) != (continuation is not None):
            raise ValueError("normalization continuation changed: select a separate output root")
        if cached.get("gap_policy", "missing") != gap_policy:
            raise ValueError("normalization gap policy changed: select a separate output root")
    if not force and cached is not None and continuation is None:
        expected_complete = pilot_duration_s is None
        raw_inputs = cached.get("raw_inputs", {})
        output_identities = [
            cached.get(name, {})
            for name in ("bbo_output", "l2_output", "clock_output")
        ]
        cache_valid = bool(
            bool(cached.get("complete_day")) == expected_complete
            and int(cached.get("pilot_duration_s") or 0)
            == int(pilot_duration_s or 0)
            and cached.get("download_manifest", {}).get("sha256")
            == manifest_sha256
            and raw_inputs.get(INCREMENTAL_L2, {}).get("sha256")
            == str(l2_row["sha256"])
            and raw_inputs.get(BOOK_TICKER, {}).get("sha256")
            == (str(ticker_row["sha256"]) if ticker_row else None)
            and cached.get("observation_schema") == OBSERVATION_SCHEMA
            and output_start_us is None and output_end_us is None
            and cached.get("output_start_us") == _day_start_us(day)
            and cached.get("output_end_us") == _day_start_us(day) + (
                DAY_US if pilot_duration_s is None else min(DAY_US, pilot_duration_s * 1_000_000))
            and all(
                identity.get("path")
                and Path(str(identity["path"])).is_file()
                and _sha256(Path(str(identity["path"])))
                == identity.get("sha256")
                for identity in output_identities
            )
        )
        if cache_valid:
            cached["resume_status"] = "validated_existing"
            return cached
    # Build all files and audits privately. Only a fully published day can
    # advance caller-owned state; failed audits cannot poison the next day.
    if not l2_raw.is_file():
        raise FileNotFoundError("primary L2 file is unavailable")
    if _sha256(l2_raw) != str(l2_row["sha256"]):
        raise ValueError("primary L2 identity differs from download manifest")
    output_root.mkdir(parents=True, exist_ok=True)
    staged_state = copy.deepcopy(continuation)
    with tempfile.TemporaryDirectory(prefix=".normalize-", dir=output_root) as directory:
        stage = Path(directory)
        quality = _build_day(
            manifest, day=day, output_root=stage, cryptohft_root=cryptohft_root,
            pilot_duration_s=pilot_duration_s, timestamp_source=timestamp_source,
            continuation=staged_state, gap_policy=gap_policy,
            output_start_us=output_start_us, output_end_us=output_end_us,
            rows=rows, manifest_sha256=manifest_sha256, l2_raw=l2_raw, ticker_raw=ticker_raw,
        )
        pairs = []
        for name in ("bbo_output", "l2_output", "clock_output"):
            source = Path(quality[name]["path"])
            target = output_root.resolve() / source.relative_to(stage.resolve())
            quality[name]["path"] = str(target)
            pairs.append((source, target))
        staged_quality = stage / "quality" / quality_path.name
        _atomic_json(quality, staged_quality)
        pairs.append((staged_quality, quality_path))
        _publish_files(pairs)
    if continuation is not None:
        continuation.__dict__.update(staged_state.__dict__)
    return quality


def _build_day(
    manifest: Path, *, day: str, output_root: Path, cryptohft_root: Path | None,
    pilot_duration_s: int | None, timestamp_source: str,
    continuation: L2Continuation | None, gap_policy: str,
    output_start_us: int | None, output_end_us: int | None,
    rows: Mapping[str, Mapping[str, Any]], manifest_sha256: str,
    l2_raw: Path, ticker_raw: Path | None,
) -> dict[str, Any]:
    l2_row, ticker_row = rows[INCREMENTAL_L2], rows.get(BOOK_TICKER)
    started = time.perf_counter()
    bbo_path, l2_path, quality = reconstruct_l2(
        l2_raw,
        output_root=output_root,
        day=day,
        pilot_duration_s=pilot_duration_s,
        timestamp_source=timestamp_source,
        continuation=continuation,
        gap_policy=gap_policy,
        output_start_us=output_start_us,
        output_end_us=output_end_us,
    )
    quality["raw_inputs"] = {
        INCREMENTAL_L2: {
            "path": str(l2_raw.resolve()),
            "sha256": str(l2_row["sha256"]),
            "size_bytes": int(l2_row["size_bytes"]),
        },
    }
    if ticker_row:
        quality["raw_inputs"][BOOK_TICKER] = {
            "path": str(ticker_raw.resolve()),
            "sha256": str(ticker_row["sha256"]),
            "size_bytes": int(ticker_row["size_bytes"]),
        }
    quality["download_manifest"] = {
        "path": str(manifest.resolve()),
        "sha256": manifest_sha256,
    }
    quality["book_ticker_audit"] = {"status": "unavailable", "reason": "auxiliary_book_ticker_missing"}
    if ticker_raw is not None and ticker_raw.is_file():
        try:
            if _sha256(ticker_raw) != str(ticker_row["sha256"]):
                raise ValueError("auxiliary book ticker identity differs from manifest")
            quality["book_ticker_audit"] = audit_book_ticker(
                ticker_raw, bbo_path, day=day, pilot_duration_s=pilot_duration_s,
                timestamp_source=timestamp_source)
        except (OSError, pa.ArrowInvalid, pa.ArrowKeyError, ValueError) as exc:
            quality["book_ticker_audit"] = {
                "status": "unavailable", "reason": "auxiliary_book_ticker_unreadable",
                "error_type": type(exc).__name__,
            }
    ticker_audit = quality["book_ticker_audit"]
    quality["cross_channel_contract_valid"] = bool(
        ticker_audit.get("book_ticker_comparable_ratio", 0)
        >= CROSS_CHANNEL_MIN_COMPARABLE_RATIO
        and ticker_audit.get("book_ticker_price_exact_ratio", 0)
        >= CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO
        and ticker_audit.get("book_ticker_price_within_one_tick_ratio", 0)
        >= CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO
        and ticker_audit["book_ticker_causal_violations"] == 0
        and ticker_audit["book_ticker_local_clock_reversals"] == 0
    )
    quality["provider_normalized_replay_candidate"] = bool(
        quality["normalized_replay_candidate_before_cross_channel"]
        and quality["cross_channel_contract_valid"]
    )
    quality["bbo_output"] = {
        "path": str(bbo_path.resolve()),
        "sha256": _sha256(bbo_path),
        "size_bytes": bbo_path.stat().st_size,
    }
    quality["l2_output"] = {
        "path": str(l2_path.resolve()),
        "sha256": _sha256(l2_path),
        "size_bytes": l2_path.stat().st_size,
    }
    writer_clock = (
        output_root / "clock" / f"BTCUSDC-clock-{day}.parquet"
    ).resolve()
    quality["clock_output"] = {
        "path": str(writer_clock),
        "sha256": _sha256(writer_clock),
        "size_bytes": writer_clock.stat().st_size,
    }
    crypto_path = None
    if cryptohft_root is not None:
        candidate = (
            cryptohft_root / "l2" / f"BTCUSDC-l2-{day}.parquet"
        )
        if candidate.is_file():
            crypto_path = candidate
    if crypto_path is not None:
        quality["cryptohft_dual_source"] = compare_normalized_sources(
            l2_path, crypto_path, tardis_clock=writer_clock
        )
        quality["cryptohft_dual_source"]["path"] = str(crypto_path.resolve())
        quality["cryptohft_dual_source"]["sha256"] = _sha256(crypto_path)
    else:
        quality["cryptohft_dual_source"] = {
            "dual_source_available": False,
            "cannot_upgrade_native_sequence_or_exact_queue": True,
        }
    quality["total_elapsed_s"] = time.perf_counter() - started
    quality["resume_status"] = "rebuilt"
    return quality


def _normalize_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    cryptohft = payload.get("cryptohft_root")
    return normalize_day(
        Path(str(payload["manifest"])),
        day=str(payload["day"]),
        output_root=Path(str(payload["output_root"])),
        cryptohft_root=Path(str(cryptohft)) if cryptohft else None,
        pilot_duration_s=(
            int(payload["pilot_duration_s"])
            if payload.get("pilot_duration_s") is not None
            else None
        ),
        force=bool(payload.get("force")),
        timestamp_source=str(payload.get("timestamp_source", "exchange")),
        continuation=payload.get("continuation"),
        gap_policy=str(payload.get("gap_policy", "missing")),
    )


def _normalize_day_safe(payload: Mapping[str, Any]) -> dict[str, Any]:
    day = str(payload["day"])
    try:
        return {"day": day, "ok": True, "quality": _normalize_day_task(payload)}
    except Exception as exc:  # noqa: BLE001 - isolate one daily admission
        return {
            "day": day,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--day", action="append", default=[])
    parser.add_argument(
        "--days-file",
        type=Path,
        help="Optional CSV with a required day column; combined with repeated --day",
    )
    parser.add_argument(
        "--output-root", type=Path
    )
    parser.add_argument("--timestamp-source", choices=("provider", "exchange"), default="exchange",
                        help="Exchange bins exclude historical provider transport; current-host delay is simulated later")
    parser.add_argument(
        "--cryptohft-root", type=Path, default=data_root() / "normalized_l2_100ms_v2"
    )
    parser.add_argument("--pilot-duration-s", type=int)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--continuous", action="store_true",
                        help="Carry reconstruction and real observed clocks through adjacent days; workers=1")
    parser.add_argument("--gap-policy", choices=("missing", "carry_forward"), default="missing",
                        help="Optional carried view preserves age and unknown coverage; not replay admission")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _requested_days(explicit: Sequence[str], days_file: Path | None) -> list[str]:
    values = list(explicit)
    if days_file is not None:
        with days_file.expanduser().resolve().open(
            newline="", encoding="utf-8-sig"
        ) as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None or "day" not in reader.fieldnames:
                raise ValueError("--days-file must be a CSV with a day column")
            values.extend(str(row["day"]).strip() for row in reader)
    return sorted({date.fromisoformat(day).isoformat() for day in values if day})


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    dataset_id = EXCHANGE_DATASET_ID if args.timestamp_source == "exchange" else DATASET_ID
    if args.output_root is None:
        args.output_root = data_root() / dataset_id
    if args.pilot_duration_s is not None and args.pilot_duration_s <= 0:
        raise SystemExit("--pilot-duration-s must be positive")
    if args.workers < 1 or args.workers > 4:
        raise SystemExit("--workers must be in [1, 4]")
    try:
        days = _requested_days(args.day, args.days_file)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not days:
        raise SystemExit("at least one --day or --days-file row is required")
    if args.continuous and (args.workers != 1 or args.pilot_duration_s is not None
                           or any(_day_start_us(b) - _day_start_us(a) != DAY_US
                                  for a, b in zip(days, days[1:], strict=False))):
        raise SystemExit("--continuous requires adjacent complete days and --workers 1")
    free = os.statvfs(args.output_root.parent if args.output_root.parent.exists() else marketdata_root())
    free_bytes = int(free.f_bavail * free.f_frsize)
    estimated_new = (
        len(days)
        * ESTIMATED_NORMALIZED_BYTES_PER_FULL_DAY
        * (1.0 if args.pilot_duration_s is None else args.pilot_duration_s / 86_400)
    )
    required_free = int(60 * 1024**3 + 2.5 * estimated_new)
    if free_bytes < max(50 * 1024**3, required_free):
        raise SystemExit(
            "storage safety gate failed: "
            f"free={free_bytes} required={required_free}"
        )
    payloads = [
        {
            "manifest": str(args.manifest.expanduser().resolve()),
            "day": day,
            "output_root": str(args.output_root.expanduser().resolve()),
            "cryptohft_root": str(args.cryptohft_root.expanduser().resolve()),
            "pilot_duration_s": args.pilot_duration_s,
            "force": args.force,
            "timestamp_source": args.timestamp_source,
            "gap_policy": args.gap_policy,
        }
        for day in days
    ]
    if args.workers == 1:
        state = L2Continuation() if args.continuous else None
        task_results = []
        for payload in payloads:
            result = _normalize_day_safe({**payload, "continuation": state})
            task_results.append(result)
            if not result["ok"] and state is not None:
                # Never skip a failed day then silently inherit a stale state.
                break
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers
        ) as executor:
            task_results = list(executor.map(_normalize_day_safe, payloads))
    task_results.sort(key=lambda row: str(row["day"]))
    results = [row["quality"] for row in task_results if row["ok"]]
    failures = [
        {"day": row["day"], "error": row["error"]}
        for row in task_results
        if not row["ok"]
    ]
    compact_days = [
        {
            "day": row["day"],
            "resume_status": row.get("resume_status"),
            "provider_normalized_replay_candidate": bool(
                row.get("provider_normalized_replay_candidate")
            ),
            "quality_path": str(
                args.output_root.expanduser().resolve()
                / "quality"
                / f"BTCUSDC-{row['day']}.json"
            ),
            "total_elapsed_s": row.get("total_elapsed_s"),
        }
        for row in results
    ]
    manifest_path = args.manifest.expanduser().resolve()
    summary = {
        "schema_version": "narrowgate.normalized_tardis_l2_batch.v1",
        "source_id": SOURCE_ID,
        "dataset_id": dataset_id,
        "download_manifest": {
            "path": str(manifest_path),
            "sha256": _sha256(manifest_path),
        },
        "requested_days": days,
        "completed_days": [row["day"] for row in results],
        "failed_days": failures,
        "not_run_days": [day for day in days if day not in {row["day"] for row in task_results}],
        "continuous": args.continuous,
        "gap_policy": args.gap_policy,
        "workers": args.workers,
        "storage_preflight": {
            "free_bytes": free_bytes,
            "estimated_new_bytes": int(estimated_new),
            "required_free_bytes": required_free,
        },
        "provider_normalized_replay_candidates": sum(
            bool(row["provider_normalized_replay_candidate"]) for row in results
        ),
        "native_binance_sequence_ids_present": False,
        "exact_queue_policy_eligible": False,
        "daily_admissions": compact_days,
    }
    if args.summary_json:
        _atomic_json(summary, args.summary_json.expanduser().resolve())
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
