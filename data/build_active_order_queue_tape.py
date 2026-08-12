#!/usr/bin/env python3
"""Build a strict sparse queue tape for active order price levels.

The builder consumes one UTC day of pre-existing CryptoHFTData Binance Futures
hourly order-book archives. It never downloads data and it does not connect the
result to replay. A watch is seeded from the last complete exchange-time state
strictly before activation; an exact update in the activation millisecond is
emitted separately and marked ambiguous.
"""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import math
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from data.download_cryptohft_orderbook import (
    DEFAULT_EXCHANGE,
    OrderBookSequenceState,
    OrderBookState,
    _decompress_parquet_zst,
    _extract_ts_ms,
    _select_ts_ms,
)
from data_paths import marketdata_root

DEFAULT_RAW_ROOT = marketdata_root() / "cryptohftdata"
DEFAULT_SYMBOL = "BTCUSDC"
DEFAULT_TICK_SIZE = 0.1
DEFAULT_WARMUP_HOURS = 24
REQUIRED_WATCH_COLUMNS = {
    "day",
    "watch_id",
    "order_id",
    "side",
    "price",
    "activate_ts_ms",
    "stop_ts_ms",
}
RAW_COLUMNS = (
    "event_time",
    "transaction_time",
    "received_time",
    "event_type",
    "first_update_id",
    "final_update_id",
    "prev_final_update_id",
    "last_update_id",
    "side",
    "price",
    "quantity",
)

SEED_SCHEMA = pa.schema(
    [
        ("day", pa.string()),
        ("watch_id", pa.string()),
        ("order_id", pa.string()),
        ("side", pa.string()),
        ("price_tick", pa.int64()),
        ("activate_ts_ms", pa.int64()),
        ("stop_ts_ms", pa.int64()),
        ("seed_status", pa.string()),
        ("seed_reason", pa.string()),
        ("seed_qty", pa.float64()),
        ("seed_asof_ts_ms", pa.int64()),
        ("seed_update_id", pa.int64()),
        ("segment_id", pa.int32()),
        ("seed_best_bid_tick", pa.int64()),
        ("seed_best_ask_tick", pa.int64()),
        ("snapshot_min_tick", pa.int64()),
        ("snapshot_max_tick", pa.int64()),
        ("ambiguous", pa.bool_()),
    ]
)

LEVEL_EVENT_SCHEMA = pa.schema(
    [
        ("day", pa.string()),
        ("watch_id", pa.string()),
        ("order_id", pa.string()),
        ("side", pa.string()),
        ("price_tick", pa.int64()),
        ("exchange_ts_ms", pa.int64()),
        ("source_receive_ts_ns", pa.int64()),
        ("message_ordinal", pa.int64()),
        ("segment_id", pa.int32()),
        ("update_id", pa.int64()),
        ("qty_after", pa.float64()),
        ("event_code", pa.string()),
        ("state_status", pa.string()),
        ("ambiguous", pa.bool_()),
    ]
)


@dataclass(frozen=True)
class Watch:
    day: str
    watch_id: str
    order_id: str
    side: str
    price_tick: int
    activate_ts_ms: int
    stop_ts_ms: int


@dataclass(frozen=True)
class RawLevelRow:
    event_type: str
    exchange_ts_ms: int
    receive_time_ms: int
    receive_time_ns: int
    event_time_ms: int
    transaction_time_ms: int
    first_update_id: int | None
    final_update_id: int | None
    previous_final_update_id: int | None
    last_update_id: int | None
    side: str | None
    price_tick: int | None
    quantity: float | None

    @property
    def message_key(self) -> tuple[object, ...]:
        if self.event_type == "snapshot":
            return (
                self.event_type,
                self.event_time_ms,
                self.last_update_id,
                self.final_update_id,
            )
        return (
            self.event_type,
            self.receive_time_ms,
            self.event_time_ms,
            self.transaction_time_ms,
            self.first_update_id,
            self.final_update_id,
            self.previous_final_update_id,
            self.last_update_id,
        )


@dataclass
class LogicalMessage:
    event_type: str
    exchange_ts_ms: int
    receive_time_ms: int
    receive_time_ns: int
    event_time_ms: int
    transaction_time_ms: int
    first_update_id: int | None
    final_update_id: int | None
    previous_final_update_id: int | None
    last_update_id: int | None
    levels: list[tuple[str, int, float]]

    @classmethod
    def from_row(cls, row: RawLevelRow) -> LogicalMessage:
        levels = []
        if row.side is not None and row.price_tick is not None and row.quantity is not None:
            levels.append((row.side, row.price_tick, row.quantity))
        return cls(
            event_type=row.event_type,
            exchange_ts_ms=row.exchange_ts_ms,
            receive_time_ms=row.receive_time_ms,
            receive_time_ns=row.receive_time_ns,
            event_time_ms=row.event_time_ms,
            transaction_time_ms=row.transaction_time_ms,
            first_update_id=row.first_update_id,
            final_update_id=row.final_update_id,
            previous_final_update_id=row.previous_final_update_id,
            last_update_id=row.last_update_id,
            levels=levels,
        )

    def append(self, row: RawLevelRow) -> None:
        self.receive_time_ms = max(self.receive_time_ms, row.receive_time_ms)
        self.receive_time_ns = max(self.receive_time_ns, row.receive_time_ns)
        if row.side is not None and row.price_tick is not None and row.quantity is not None:
            self.levels.append((row.side, row.price_tick, row.quantity))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_side(value: object) -> str:
    side = str(value).strip().lower()
    if side in {"buy", "bid"}:
        return "bid"
    if side in {"sell", "ask"}:
        return "ask"
    raise ValueError(f"unsupported watch side: {value!r}")


def _price_tick(price: float, tick_size: float, *, strict: bool) -> int:
    if not math.isfinite(price) or price <= 0.0:
        raise ValueError(f"invalid price: {price!r}")
    scaled = price / tick_size
    tick = int(round(scaled))
    if strict and not math.isclose(
        price,
        tick * tick_size,
        rel_tol=0.0,
        abs_tol=max(1e-9, tick_size * 1e-8),
    ):
        raise ValueError(f"price {price!r} is not aligned to tick size {tick_size!r}")
    return tick


def _coerce_epoch_ms(series: pd.Series, name: str) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=np.float64)
    if not np.isfinite(values).all():
        raise ValueError(f"{name} must contain finite Unix epoch milliseconds")
    rounded = np.rint(values)
    if not np.allclose(values, rounded, rtol=0.0, atol=0.0):
        raise ValueError(f"{name} must contain integer Unix epoch milliseconds")
    return rounded.astype(np.int64)


def _normalize_day(series: pd.Series) -> list[str]:
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("watch manifest contains an invalid UTC day")
    return parsed.dt.strftime("%Y-%m-%d").tolist()


def load_watch_manifest(path: Path, tick_size: float) -> tuple[str, list[Watch]]:
    frame = pd.read_parquet(path)
    missing = sorted(REQUIRED_WATCH_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"watch manifest is missing columns: {missing}")
    if frame.empty:
        raise ValueError("watch manifest is empty")
    if not math.isfinite(tick_size) or tick_size <= 0.0:
        raise ValueError("tick_size must be positive")

    days = _normalize_day(frame["day"])
    unique_days = sorted(set(days))
    if len(unique_days) != 1:
        raise ValueError(f"single-day builder requires exactly one UTC day: {unique_days}")
    day = unique_days[0]
    day_start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_start_ms = int(day_start.timestamp() * 1000)
    day_end_ms = int((day_start + timedelta(days=1)).timestamp() * 1000)

    activate = _coerce_epoch_ms(frame["activate_ts_ms"], "activate_ts_ms")
    stop = _coerce_epoch_ms(frame["stop_ts_ms"], "stop_ts_ms")
    if np.any(stop <= activate):
        raise ValueError("every stop_ts_ms must be greater than activate_ts_ms")
    if np.any(activate < day_start_ms) or np.any(activate >= day_end_ms):
        raise ValueError("activate_ts_ms must fall inside the manifest UTC day")
    if np.any(stop > day_end_ms):
        raise ValueError("stop_ts_ms must not extend past the manifest UTC day")

    watch_ids = frame["watch_id"].astype(str)
    if watch_ids.duplicated().any():
        duplicate = watch_ids[watch_ids.duplicated()].iloc[0]
        raise ValueError(f"watch_id must be unique within the day: {duplicate}")

    prices = pd.to_numeric(frame["price"], errors="coerce").to_numpy(dtype=np.float64)
    order_ids = frame["order_id"].astype(str)
    watches = []
    for index in range(len(frame)):
        watches.append(
            Watch(
                day=day,
                watch_id=str(watch_ids.iloc[index]),
                order_id=str(order_ids.iloc[index]),
                side=_normalize_side(frame["side"].iloc[index]),
                price_tick=_price_tick(float(prices[index]), tick_size, strict=True),
                activate_ts_ms=int(activate[index]),
                stop_ts_ms=int(stop[index]),
            )
        )
    watches.sort(key=lambda item: (item.activate_ts_ms, item.watch_id))
    return day, watches


def _optional_int_array(frame: pd.DataFrame, column: str) -> np.ndarray:
    if column not in frame.columns:
        return np.full(len(frame), np.nan, dtype=np.float64)
    return pd.to_numeric(frame[column], errors="coerce").to_numpy(
        dtype=np.float64,
        copy=False,
    )


def _timestamp_ms_array(frame: pd.DataFrame, column: str) -> np.ndarray:
    if column not in frame.columns:
        return np.zeros(len(frame), dtype=np.int64)
    return np.asarray(_extract_ts_ms(frame, column), dtype=np.int64)


def _optional_id(value: float) -> int | None:
    return int(value) if pd.notna(value) else None


def _receive_time_ns(frame: pd.DataFrame) -> np.ndarray:
    if "received_time" not in frame.columns:
        return np.zeros(len(frame), dtype=np.int64)
    values = pd.to_numeric(frame["received_time"], errors="coerce").fillna(0).to_numpy(
        dtype=np.int64,
        copy=False,
    )
    positive = values > 0
    if not positive.any():
        return values
    largest = int(values[positive].max())
    if largest < 10**14:
        values = values * 1_000_000
    elif largest < 10**17:
        values = values * 1_000
    return values


def _iter_raw_rows(
    path: Path,
    tick_size: float,
    *,
    batch_size: int = 100_000,
) -> Iterator[RawLevelRow]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    temporary: Path | None = None
    try:
        temporary = _decompress_parquet_zst(path)
        parquet = pq.ParquetFile(temporary)
        columns = [column for column in RAW_COLUMNS if column in parquet.schema.names]
        # Raw hourly books can contain several million price-level rows.
        # Keeping this batch bounded avoids multi-gigabyte pandas/object peaks
        # when a native tape is streamed directly through tick replay.
        for batch in parquet.iter_batches(
            batch_size=int(batch_size),
            columns=columns,
        ):
            frame = batch.to_pandas()
            if frame.empty:
                continue

            exchange_ts = _select_ts_ms(frame, "transaction")

            event_ts = _timestamp_ms_array(frame, "event_time")
            transaction_ts = _timestamp_ms_array(frame, "transaction_time")
            receive_ms = _timestamp_ms_array(frame, "received_time")
            receive_ns = _receive_time_ns(frame)
            event_types = (
                frame["event_type"].astype(str).str.lower().to_numpy(copy=False)
                if "event_type" in frame.columns
                else np.full(len(frame), "update", dtype=object)
            )
            first_ids = _optional_int_array(frame, "first_update_id")
            final_ids = _optional_int_array(frame, "final_update_id")
            previous_ids = _optional_int_array(frame, "prev_final_update_id")
            last_ids = _optional_int_array(frame, "last_update_id")
            sides = (
                frame["side"].astype(str).str.lower().to_numpy(copy=False)
                if "side" in frame.columns
                else np.full(len(frame), "", dtype=object)
            )
            prices = _optional_int_array(frame, "price")
            quantities = _optional_int_array(frame, "quantity")

            for index in range(len(frame)):
                event_type = event_types[index]
                selected_ts = exchange_ts[index]
                source_receive_ms = receive_ms[index]
                source_receive_ns = receive_ns[index]
                event_time_ms = event_ts[index]
                transaction_time_ms = transaction_ts[index]
                first_id = first_ids[index]
                final_id = final_ids[index]
                previous_id = previous_ids[index]
                last_id = last_ids[index]
                side = sides[index]
                price = prices[index]
                quantity = quantities[index]
                normalized_side = str(side).lower()
                valid_level = (
                    normalized_side in {"bid", "ask"}
                    and pd.notna(price)
                    and float(price) > 0.0
                    and pd.notna(quantity)
                )
                yield RawLevelRow(
                    event_type=str(event_type).lower(),
                    exchange_ts_ms=int(selected_ts),
                    receive_time_ms=int(source_receive_ms),
                    receive_time_ns=int(source_receive_ns),
                    event_time_ms=int(event_time_ms),
                    transaction_time_ms=int(transaction_time_ms),
                    first_update_id=_optional_id(first_id),
                    final_update_id=_optional_id(final_id),
                    previous_final_update_id=_optional_id(previous_id),
                    last_update_id=_optional_id(last_id),
                    side=normalized_side if valid_level else None,
                    price_tick=(
                        _price_tick(float(price), tick_size, strict=False)
                        if valid_level
                        else None
                    ),
                    quantity=max(float(quantity), 0.0) if valid_level else None,
                )
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def iter_cryptohft_logical_messages(
    path: Path,
    tick_size: float,
    *,
    batch_size: int = 100_000,
    include_levels: bool = True,
) -> Iterator[LogicalMessage]:
    """Yield complete native snapshot/delta messages from one raw hour.

    CryptoHFT stores one row per changed price level, so an active hour can
    contain several million rows but only roughly one hundred thousand logical
    messages.  Converting every batch to pandas and allocating one Python
    ``RawLevelRow`` per level dominated native replay runtime.  This parser
    keeps conversion and message-boundary detection inside Arrow/NumPy and
    allocates Python objects only once per logical message.
    """

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if not math.isfinite(tick_size) or tick_size <= 0.0:
        raise ValueError("tick_size must be positive")

    def integer_values(batch: pa.RecordBatch, name: str) -> np.ndarray:
        index = batch.schema.get_field_index(name)
        if index < 0:
            return np.full(batch.num_rows, -1, dtype=np.int64)
        array = pc.cast(batch.column(index), pa.int64(), safe=False)
        return np.asarray(
            pc.fill_null(array, -1).to_numpy(zero_copy_only=False),
            dtype=np.int64,
        )

    def timestamp_ms(values: np.ndarray) -> np.ndarray:
        out = np.asarray(values, dtype=np.int64).copy()
        positive = out > 0
        if positive.any() and int(out[positive].max()) >= 10**15:
            out[positive] //= 1_000_000
        out[~positive] = 0
        return out

    def receive_time_ns(values: np.ndarray) -> np.ndarray:
        out = np.asarray(values, dtype=np.int64).copy()
        positive = out > 0
        if not positive.any():
            return out
        largest = int(out[positive].max())
        if largest < 10**14:
            out[positive] *= 1_000_000
        elif largest < 10**17:
            out[positive] *= 1_000
        out[~positive] = 0
        return out

    def floating_values(batch: pa.RecordBatch, name: str) -> np.ndarray:
        index = batch.schema.get_field_index(name)
        if index < 0:
            return np.full(batch.num_rows, np.nan, dtype=np.float64)
        return np.asarray(
            pc.cast(batch.column(index), pa.float64(), safe=False).to_numpy(
                zero_copy_only=False,
            ),
            dtype=np.float64,
        )

    def equals_text(
        batch: pa.RecordBatch,
        name: str,
        value: str,
    ) -> np.ndarray:
        index = batch.schema.get_field_index(name)
        if index < 0:
            return np.zeros(batch.num_rows, dtype=bool)
        lowered = pc.utf8_lower(pc.cast(batch.column(index), pa.string()))
        return np.asarray(
            pc.fill_null(pc.equal(lowered, value), False).to_numpy(
                zero_copy_only=False,
            ),
            dtype=bool,
        )

    current_key: tuple[object, ...] | None = None
    current: LogicalMessage | None = None
    temporary: Path | None = None
    try:
        temporary = _decompress_parquet_zst(path)
        parquet = pq.ParquetFile(temporary)
        required_columns = (
            RAW_COLUMNS
            if include_levels
            else (
                "received_time",
                "event_time",
                "transaction_time",
                "event_type",
                "first_update_id",
                "final_update_id",
                "prev_final_update_id",
                "last_update_id",
            )
        )
        columns = [
            column
            for column in required_columns
            if column in parquet.schema.names
        ]
        for batch in parquet.iter_batches(
            batch_size=int(batch_size),
            columns=columns,
        ):
            if batch.num_rows <= 0:
                continue

            received_raw = integer_values(batch, "received_time")
            event_raw = integer_values(batch, "event_time")
            transaction_raw = integer_values(batch, "transaction_time")
            receive_ms = timestamp_ms(received_raw)
            event_ms = timestamp_ms(event_raw)
            transaction_ms = timestamp_ms(transaction_raw)
            exchange_ms = transaction_ms.copy()
            missing = exchange_ms <= 0
            exchange_ms[missing] = event_ms[missing]
            missing = exchange_ms <= 0
            exchange_ms[missing] = receive_ms[missing]
            receive_ns = receive_time_ns(received_raw)

            first_ids = integer_values(batch, "first_update_id")
            final_ids = integer_values(batch, "final_update_id")
            previous_ids = integer_values(batch, "prev_final_update_id")
            last_ids = integer_values(batch, "last_update_id")
            snapshots = equals_text(batch, "event_type", "snapshot")
            if include_levels:
                asks = equals_text(batch, "side", "ask")
                bids = equals_text(batch, "side", "bid")
                prices = floating_values(batch, "price")
                quantities = floating_values(batch, "quantity")
                valid_levels = (
                    (asks | bids)
                    & np.isfinite(prices)
                    & (prices > 0.0)
                    & np.isfinite(quantities)
                )
                safe_prices = np.where(np.isfinite(prices), prices, 0.0)
                price_ticks = np.rint(
                    safe_prices / float(tick_size)
                ).astype(
                    np.int64,
                    casting="unsafe",
                    copy=False,
                )
            else:
                asks = bids = valid_levels = price_ticks = quantities = None

            starts = np.ones(batch.num_rows, dtype=bool)
            if batch.num_rows > 1:
                same_type = snapshots[1:] == snapshots[:-1]
                same_snapshot = (
                    snapshots[1:]
                    & snapshots[:-1]
                    & (event_ms[1:] == event_ms[:-1])
                    & (last_ids[1:] == last_ids[:-1])
                    & (final_ids[1:] == final_ids[:-1])
                )
                same_update = (
                    ~snapshots[1:]
                    & ~snapshots[:-1]
                    & (receive_ms[1:] == receive_ms[:-1])
                    & (event_ms[1:] == event_ms[:-1])
                    & (transaction_ms[1:] == transaction_ms[:-1])
                    & (first_ids[1:] == first_ids[:-1])
                    & (final_ids[1:] == final_ids[:-1])
                    & (previous_ids[1:] == previous_ids[:-1])
                    & (last_ids[1:] == last_ids[:-1])
                )
                starts[1:] = ~(same_type & (same_snapshot | same_update))
            group_starts = np.flatnonzero(starts)
            group_ends = np.r_[group_starts[1:], batch.num_rows]

            for start, end in zip(group_starts, group_ends, strict=True):
                snapshot = bool(snapshots[start])
                event_type = "snapshot" if snapshot else "update"
                first_id = int(first_ids[start])
                final_id = int(final_ids[start])
                previous_id = int(previous_ids[start])
                last_id = int(last_ids[start])
                key: tuple[object, ...]
                if snapshot:
                    key = (
                        event_type,
                        int(event_ms[start]),
                        None if last_id < 0 else last_id,
                        None if final_id < 0 else final_id,
                    )
                else:
                    key = (
                        event_type,
                        int(receive_ms[start]),
                        int(event_ms[start]),
                        int(transaction_ms[start]),
                        None if first_id < 0 else first_id,
                        None if final_id < 0 else final_id,
                        None if previous_id < 0 else previous_id,
                        None if last_id < 0 else last_id,
                    )

                if include_levels:
                    assert valid_levels is not None
                    assert asks is not None
                    assert price_ticks is not None
                    assert quantities is not None
                    level_indices = np.flatnonzero(
                        valid_levels[start:end]
                    ) + int(start)
                    levels = [
                        (
                            "ask" if bool(asks[index]) else "bid",
                            int(price_ticks[index]),
                            max(float(quantities[index]), 0.0),
                        )
                        for index in level_indices
                    ]
                else:
                    levels = []
                if current is not None and key == current_key:
                    current.receive_time_ms = max(
                        current.receive_time_ms,
                        int(receive_ms[start]),
                    )
                    current.receive_time_ns = max(
                        current.receive_time_ns,
                        int(receive_ns[start]),
                    )
                    current.levels.extend(levels)
                    continue
                if current is not None:
                    yield current
                current_key = key
                current = LogicalMessage(
                    event_type=event_type,
                    exchange_ts_ms=int(exchange_ms[start]),
                    receive_time_ms=int(receive_ms[start]),
                    receive_time_ns=int(receive_ns[start]),
                    event_time_ms=int(event_ms[start]),
                    transaction_time_ms=int(transaction_ms[start]),
                    first_update_id=None if first_id < 0 else first_id,
                    final_update_id=None if final_id < 0 else final_id,
                    previous_final_update_id=(
                        None if previous_id < 0 else previous_id
                    ),
                    last_update_id=None if last_id < 0 else last_id,
                    levels=levels,
                )
        if current is not None:
            yield current
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


# Historical callers used the private name. Keep the alias while the sparse
# trajectory-dependent tape remains available as an archived diagnostic.
_iter_logical_messages = iter_cryptohft_logical_messages


class SparseQueueTapeBuilder:
    def __init__(self, watches: list[Watch]):
        self.watches = watches
        self.book = OrderBookState()
        self.sequence = OrderBookSequenceState(self.book, allow_delta_bootstrap=False)
        self.snapshot_ranges: dict[str, tuple[int, int] | None] = {
            "bid": None,
            "ask": None,
        }
        self.known_ticks: dict[str, set[int]] = {"bid": set(), "ask": set()}
        self.current_segment_id = 0
        self.segment_count = 0
        self.last_applied_ts_ms: int | None = None
        self.last_message_ts_ms: int | None = None
        self.message_ordinal = 0
        self.next_activation = 0
        self.active: set[int] = set()
        self.active_by_level: dict[tuple[str, int], set[int]] = {}
        self.stop_heap: list[tuple[int, int]] = []
        self.seeds: list[dict[str, object]] = []
        self.seed_row_by_watch: dict[int, dict[str, object]] = {}
        self.events: list[dict[str, object]] = []
        self.segments: list[dict[str, object]] = []
        self.source_gap_count = 0
        self.time_reversal_count = 0

    def _state_valid(self) -> bool:
        return self.sequence.initialized and self.current_segment_id > 0

    def _classify(
        self,
        side: str,
        tick: int,
    ) -> tuple[
        str,
        float | None,
        str,
        int | None,
        int | None,
    ]:
        if not self._state_valid():
            return "unknown", None, "sequence_unavailable", None, None
        levels = self.book.bid_levels if side == "bid" else self.book.ask_levels
        bounds = self.snapshot_ranges[side]
        minimum = bounds[0] if bounds is not None else None
        maximum = bounds[1] if bounds is not None else None
        quantity = levels.get(float(tick))
        if quantity is not None and quantity > 0.0:
            return (
                "exact",
                float(quantity),
                "visible_quantity",
                minimum,
                maximum,
            )
        if tick in self.known_ticks[side]:
            return (
                "known_zero",
                0.0,
                "explicit_zero_or_removed_level",
                minimum,
                maximum,
            )
        if bounds is not None and bounds[0] <= tick <= bounds[1]:
            return (
                "known_zero",
                0.0,
                "inside_snapshot_range_absent",
                minimum,
                maximum,
            )
        return (
            "unknown",
            None,
            "outside_snapshot_range",
            minimum,
            maximum,
        )

    def _stop_watches(self, ts_ms: int, *, include_equal: bool) -> None:
        while self.stop_heap:
            stop_ts, index = self.stop_heap[0]
            if stop_ts > ts_ms or (stop_ts == ts_ms and not include_equal):
                break
            heapq.heappop(self.stop_heap)
            if index not in self.active:
                continue
            self.active.remove(index)
            watch = self.watches[index]
            key = (watch.side, watch.price_tick)
            members = self.active_by_level.get(key)
            if members is not None:
                members.discard(index)
                if not members:
                    self.active_by_level.pop(key, None)

    def _activate_watch(self, index: int) -> None:
        watch = self.watches[index]
        (
            status,
            quantity,
            reason,
            snapshot_min_tick,
            snapshot_max_tick,
        ) = self._classify(watch.side, watch.price_tick)
        best_bid_tick = (
            int(max(self.book.bid_levels)) if self.book.bid_levels else None
        )
        best_ask_tick = (
            int(min(self.book.ask_levels)) if self.book.ask_levels else None
        )
        row: dict[str, object] = {
            "day": watch.day,
            "watch_id": watch.watch_id,
            "order_id": watch.order_id,
            "side": watch.side,
            "price_tick": watch.price_tick,
            "activate_ts_ms": watch.activate_ts_ms,
            "stop_ts_ms": watch.stop_ts_ms,
            "seed_status": status,
            "seed_reason": reason,
            "seed_qty": quantity,
            "seed_asof_ts_ms": (
                self.last_applied_ts_ms if self._state_valid() else None
            ),
            "seed_update_id": (
                self.sequence.last_update_id if self._state_valid() else None
            ),
            "segment_id": (
                self.current_segment_id if self._state_valid() else None
            ),
            "seed_best_bid_tick": best_bid_tick,
            "seed_best_ask_tick": best_ask_tick,
            "snapshot_min_tick": snapshot_min_tick,
            "snapshot_max_tick": snapshot_max_tick,
            "ambiguous": False,
        }
        self.seeds.append(row)
        self.seed_row_by_watch[index] = row
        self.active.add(index)
        self.active_by_level.setdefault((watch.side, watch.price_tick), set()).add(index)
        heapq.heappush(self.stop_heap, (watch.stop_ts_ms, index))

    def _activate_watches(self, ts_ms: int, *, include_equal: bool) -> None:
        while self.next_activation < len(self.watches):
            watch = self.watches[self.next_activation]
            if watch.activate_ts_ms > ts_ms or (
                watch.activate_ts_ms == ts_ms and not include_equal
            ):
                break
            self._activate_watch(self.next_activation)
            self.next_activation += 1

    def advance_time(self, ts_ms: int, *, include_equal: bool) -> None:
        self._stop_watches(ts_ms, include_equal=include_equal)
        self._activate_watches(ts_ms, include_equal=include_equal)

    def _append_event(
        self,
        index: int,
        *,
        exchange_ts_ms: int,
        receive_time_ns: int | None,
        update_id: int | None,
        quantity: float | None,
        event_code: str,
        state_status: str,
        ambiguous: bool,
        segment_id: int | None = None,
    ) -> None:
        watch = self.watches[index]
        self.events.append(
            {
                "day": watch.day,
                "watch_id": watch.watch_id,
                "order_id": watch.order_id,
                "side": watch.side,
                "price_tick": watch.price_tick,
                "exchange_ts_ms": exchange_ts_ms,
                "source_receive_ts_ns": receive_time_ns,
                "message_ordinal": self.message_ordinal,
                "segment_id": (
                    self.current_segment_id if segment_id is None else segment_id
                ),
                "update_id": update_id,
                "qty_after": quantity,
                "event_code": event_code,
                "state_status": state_status,
                "ambiguous": ambiguous,
            }
        )
        if ambiguous:
            self.seed_row_by_watch[index]["ambiguous"] = True

    def _invalidate_active(
        self,
        ts_ms: int,
        *,
        receive_time_ns: int | None,
        update_id: int | None,
    ) -> None:
        old_segment = self.current_segment_id or None
        for index in sorted(self.active):
            self._append_event(
                index,
                exchange_ts_ms=ts_ms,
                receive_time_ns=receive_time_ns,
                update_id=update_id,
                quantity=None,
                event_code="invalidate",
                state_status="unknown",
                ambiguous=False,
                segment_id=old_segment,
            )
        self.snapshot_ranges = {"bid": None, "ask": None}
        self.known_ticks = {"bid": set(), "ask": set()}
        self.current_segment_id = 0

    def source_gap(self, ts_ms: int) -> None:
        self.advance_time(ts_ms, include_equal=False)
        was_valid = self._state_valid()
        self.sequence.invalidate_source_gap()
        self.source_gap_count += 1
        self.message_ordinal += 1
        if was_valid:
            self._invalidate_active(
                ts_ms,
                receive_time_ns=None,
                update_id=None,
            )
        else:
            self.snapshot_ranges = {"bid": None, "ask": None}
            self.known_ticks = {"bid": set(), "ask": set()}
            self.current_segment_id = 0
        self.advance_time(ts_ms, include_equal=True)

    def _set_snapshot_state(self, message: LogicalMessage) -> None:
        self.segment_count += 1
        self.current_segment_id = self.segment_count
        self.known_ticks = {"bid": set(), "ask": set()}
        for side, levels in (
            ("bid", self.book.bid_levels),
            ("ask", self.book.ask_levels),
        ):
            ticks = [int(price) for price in levels]
            self.snapshot_ranges[side] = (
                (min(ticks), max(ticks)) if ticks else None
            )
        bid_range = self.snapshot_ranges["bid"]
        ask_range = self.snapshot_ranges["ask"]
        self.segments.append(
            {
                "segment_id": self.current_segment_id,
                "snapshot_ts_ms": message.exchange_ts_ms,
                "snapshot_update_id": (
                    message.last_update_id
                    if message.last_update_id is not None
                    else message.final_update_id
                ),
                "bid_min_tick": bid_range[0] if bid_range is not None else None,
                "bid_max_tick": bid_range[1] if bid_range is not None else None,
                "ask_min_tick": ask_range[0] if ask_range is not None else None,
                "ask_max_tick": ask_range[1] if ask_range is not None else None,
                "bid_level_count": len(self.book.bid_levels),
                "ask_level_count": len(self.book.ask_levels),
            }
        )

    def _emit_snapshot_state(self, message: LogicalMessage) -> None:
        update_id = (
            message.last_update_id
            if message.last_update_id is not None
            else message.final_update_id
        )
        for index in sorted(self.active):
            watch = self.watches[index]
            status, quantity, _, _, _ = self._classify(
                watch.side,
                watch.price_tick,
            )
            ambiguous = watch.activate_ts_ms == message.exchange_ts_ms
            self._append_event(
                index,
                exchange_ts_ms=message.exchange_ts_ms,
                receive_time_ns=message.receive_time_ns or None,
                update_id=update_id,
                quantity=quantity,
                event_code="snapshot",
                state_status=status,
                ambiguous=ambiguous,
            )

    def _emit_level_updates(
        self,
        message: LogicalMessage,
        before_quantities: dict[tuple[str, int], float],
    ) -> None:
        final_quantities: dict[tuple[str, int], float] = {}
        for side, tick, quantity in message.levels:
            final_quantities[(side, tick)] = quantity
        for key, quantity in final_quantities.items():
            quantity_before = float(before_quantities.get(key, 0.0))
            if math.isclose(
                quantity_before,
                float(quantity),
                rel_tol=0.0,
                abs_tol=1e-15,
            ):
                continue
            for index in sorted(self.active_by_level.get(key, ())):
                watch = self.watches[index]
                status, classified_quantity, _, _, _ = self._classify(
                    watch.side,
                    watch.price_tick,
                )
                self._append_event(
                    index,
                    exchange_ts_ms=message.exchange_ts_ms,
                    receive_time_ns=message.receive_time_ns or None,
                    update_id=message.final_update_id,
                    quantity=classified_quantity,
                    event_code="delete" if quantity <= 0.0 else "update",
                    state_status=status,
                    ambiguous=watch.activate_ts_ms == message.exchange_ts_ms,
                )

    def process_message(self, message: LogicalMessage) -> None:
        if message.exchange_ts_ms <= 0:
            return
        if (
            self.last_message_ts_ms is not None
            and message.exchange_ts_ms < self.last_message_ts_ms
        ):
            self.time_reversal_count += 1
            self.source_gap(message.exchange_ts_ms)
            return

        self.last_message_ts_ms = message.exchange_ts_ms
        self.advance_time(message.exchange_ts_ms, include_equal=True)
        self.message_ordinal += 1
        was_valid = self._state_valid()
        previous_gap_count = self.sequence.stats.sequence_gaps
        apply_message = self.sequence.begin_message(
            event_type=message.event_type,
            receive_time_ms=message.receive_time_ms,
            event_time_ms=message.event_time_ms,
            transaction_time_ms=message.transaction_time_ms,
            first_update_id=message.first_update_id,
            final_update_id=message.final_update_id,
            previous_final_update_id=message.previous_final_update_id,
            last_update_id=message.last_update_id,
        )
        if not apply_message:
            if self.sequence.stats.sequence_gaps > previous_gap_count:
                if was_valid:
                    self._invalidate_active(
                        message.exchange_ts_ms,
                        receive_time_ns=message.receive_time_ns or None,
                        update_id=message.final_update_id,
                    )
                else:
                    self.snapshot_ranges = {"bid": None, "ask": None}
                    self.known_ticks = {"bid": set(), "ask": set()}
                    self.current_segment_id = 0
            return

        before_quantities: dict[tuple[str, int], float] = {}
        if message.event_type != "snapshot":
            for side, tick, _ in message.levels:
                key = (side, tick)
                if key not in self.active_by_level or key in before_quantities:
                    continue
                levels = (
                    self.book.bid_levels
                    if side == "bid"
                    else self.book.ask_levels
                )
                before_quantities[key] = float(levels.get(float(tick), 0.0))

        for side, tick, quantity in message.levels:
            self.book.apply(side, float(tick), quantity)
            self.known_ticks[side].add(tick)
        self.last_applied_ts_ms = message.exchange_ts_ms

        if message.event_type == "snapshot":
            self._set_snapshot_state(message)
            self._emit_snapshot_state(message)
        else:
            self._emit_level_updates(message, before_quantities)


def _write_parquet_atomic(
    rows: list[dict[str, object]],
    schema: pa.Schema,
    path: Path,
) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, temporary, compression="zstd")
    temporary.replace(path)


def _write_json_atomic(payload: dict[str, object], path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def build_active_order_queue_tape(
    *,
    watch_manifest: Path,
    raw_root: Path,
    output_dir: Path,
    symbol: str = DEFAULT_SYMBOL,
    exchange: str = DEFAULT_EXCHANGE,
    tick_size: float = DEFAULT_TICK_SIZE,
    warmup_hours: int = DEFAULT_WARMUP_HOURS,
    reuse_raw_only: bool = True,
) -> dict[str, object]:
    if not reuse_raw_only:
        raise NotImplementedError("only reuse-raw-only mode is supported")
    if warmup_hours < 0:
        raise ValueError("warmup_hours must be non-negative")

    day, watches = load_watch_manifest(watch_manifest, tick_size)
    day_start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    process_start = day_start - timedelta(hours=warmup_hours)
    max_stop_ms = max(watch.stop_ts_ms for watch in watches)
    final_event_ms = max_stop_ms - 1
    final_hour = datetime.fromtimestamp(final_event_ms / 1000, tz=timezone.utc).replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    builder = SparseQueueTapeBuilder(watches)
    raw_paths: list[Path] = []
    missing_hours: list[str] = []
    missing_warmup_hours: list[str] = []
    current = process_start
    while current <= final_hour:
        hour_start_ms = int(current.timestamp() * 1000)
        hour_end_ms = int((current + timedelta(hours=1)).timestamp() * 1000)
        raw_path = (
            raw_root
            / exchange
            / current.strftime("%Y-%m-%d")
            / current.strftime("%H")
            / f"{symbol}_orderbook.parquet.zst"
        )
        if not raw_path.exists():
            missing_target = (
                missing_warmup_hours if current < day_start else missing_hours
            )
            missing_target.append(current.strftime("%Y-%m-%dT%H:00:00Z"))
            builder.source_gap(hour_start_ms)
            builder.advance_time(
                min(hour_end_ms, max_stop_ms),
                include_equal=False,
            )
            current += timedelta(hours=1)
            continue

        raw_paths.append(raw_path)
        message_count = 0
        for message in iter_cryptohft_logical_messages(raw_path, tick_size):
            if message.exchange_ts_ms >= max_stop_ms:
                break
            builder.process_message(message)
            message_count += 1
        if message_count == 0:
            builder.source_gap(hour_start_ms)
        builder.advance_time(
            min(hour_end_ms, max_stop_ms),
            include_equal=False,
        )
        current += timedelta(hours=1)

    builder.advance_time(max_stop_ms, include_equal=False)
    if not raw_paths:
        raise FileNotFoundError(
            f"no raw hourly order-book files found for {symbol} on {day}"
        )
    if len(builder.seeds) != len(watches):
        raise RuntimeError(
            f"seed row count mismatch: {len(builder.seeds)} != {len(watches)}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    seeds_path = output_dir / "seeds.parquet"
    events_path = output_dir / "level_events.parquet"
    summary_path = output_dir / "summary.json"
    sequence_path = output_dir / "sequence_audit.json"
    _write_parquet_atomic(builder.seeds, SEED_SCHEMA, seeds_path)
    _write_parquet_atomic(builder.events, LEVEL_EVENT_SCHEMA, events_path)

    seed_counts: dict[str, int] = {}
    seed_reason_counts: dict[str, int] = {}
    for row in builder.seeds:
        status = str(row["seed_status"])
        seed_counts[status] = seed_counts.get(status, 0) + 1
        reason = str(row["seed_reason"])
        seed_reason_counts[reason] = seed_reason_counts.get(reason, 0) + 1
    ambiguous_events = sum(bool(row["ambiguous"]) for row in builder.events)
    strict_seed_count = sum(
        count
        for status, count in seed_counts.items()
        if status in {"exact", "known_zero"}
    )
    summary: dict[str, object] = {
        "schema_version": "active_order_queue_tape_v3",
        "day": day,
        "symbol": symbol,
        "exchange": exchange,
        "tick_size": tick_size,
        "timestamp_semantics": {
            "book_ordering": "transaction_time_ms_with_event_time_fallback",
            "seed_cutoff": "exchange_ts_ms_strictly_before_activate_ts_ms",
            "watch_interval": "[activate_ts_ms, stop_ts_ms)",
            "same_ms_exact_update": "event_is_ambiguous_and_not_in_seed",
        },
        "reuse_raw_only": True,
        "warmup_hours": int(warmup_hours),
        "process_start_utc": process_start.isoformat(),
        "watch_manifest": watch_manifest.name,
        "watch_manifest_sha256": _sha256(watch_manifest),
        "raw_files_used": [str(path.resolve()) for path in raw_paths],
        "missing_raw_hours": missing_hours,
        "missing_warmup_hours": missing_warmup_hours,
        "watch_count": len(watches),
        "seed_status_counts": seed_counts,
        "seed_reason_counts": seed_reason_counts,
        "strict_seed_coverage": (
            strict_seed_count / len(watches) if watches else 0.0
        ),
        "level_event_count": len(builder.events),
        "ambiguous_seed_count": sum(
            bool(row["ambiguous"]) for row in builder.seeds
        ),
        "ambiguous_event_count": ambiguous_events,
        "segment_count": builder.segment_count,
        "output_files": {
            "seeds": seeds_path.name,
            "level_events": events_path.name,
            "summary": summary_path.name,
            "sequence_audit": sequence_path.name,
        },
    }
    sequence_audit: dict[str, object] = {
        "schema_version": "active_order_queue_tape_sequence_v3",
        "day": day,
        "symbol": symbol,
        "exchange": exchange,
        "strict_native_snapshot": True,
        "delta_bootstrap_allowed": False,
        "sequence_stats": asdict(builder.sequence.stats),
        "source_gap_count": builder.source_gap_count,
        "time_reversal_count": builder.time_reversal_count,
        "missing_raw_hours": missing_hours,
        "missing_warmup_hours": missing_warmup_hours,
        "segment_count": builder.segment_count,
        "segments": builder.segments,
        "final_initialized": builder.sequence.initialized,
        "final_initialization_source": builder.sequence.initialization_source,
        "final_update_id": builder.sequence.last_update_id,
    }
    _write_json_atomic(summary, summary_path)
    _write_json_atomic(sequence_audit, sequence_path)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a strict single-day sparse active-order queue tape"
    )
    parser.add_argument("--watch-manifest", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    parser.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE)
    parser.add_argument(
        "--warmup-hours",
        type=int,
        default=DEFAULT_WARMUP_HOURS,
        help=(
            "Hours before the watch day used to find a native snapshot and "
            "replay a continuous delta chain"
        ),
    )
    parser.add_argument(
        "--reuse-raw-only",
        action="store_true",
        help="Require pre-existing raw hourly archives; downloads are unsupported",
    )
    args = parser.parse_args()
    if not args.reuse_raw_only:
        parser.error("this first version requires --reuse-raw-only")
    return args


def main() -> None:
    args = _parse_args()
    summary = build_active_order_queue_tape(
        watch_manifest=args.watch_manifest,
        raw_root=args.raw_root,
        output_dir=args.output_dir,
        symbol=args.symbol,
        exchange=args.exchange,
        tick_size=args.tick_size,
        warmup_hours=args.warmup_hours,
        reuse_raw_only=args.reuse_raw_only,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
