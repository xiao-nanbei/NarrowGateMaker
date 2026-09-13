"""Shared Tardis-only facts, not native packets or strategy-visible observations.

No sorting, latency model, external-source fallback or economic state lives here.
Source fragments refer to original CSV data-row offsets (first data row is 1).
Callers bind file_id and timestamp evidence in their input manifest. Unknown
mapper provenance is the default, even when a numeric source timestamp exists.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import heapq
from pathlib import Path

CONTRACT = "tardis_source_facts.v1"
CLOCK_KINDS = frozenset({"exchange_event_E", "exchange_trade_T",
    "exchange_snapshot_anchor", "provider_receive_fallback", "unknown"})


@dataclass(frozen=True)
class SourceFragment:
    path: Path
    file_id: str
    first_row: int = 1
    complete_file: bool = True


@dataclass
class SourceQuality:
    rows: int = 0
    emitted_events: int = 0
    duplicate_trades: int = 0
    trade_conflicts: int = 0
    # Preserve actual regression records, never overwrite or clamp source time.
    time_regressions: list[dict] = field(default_factory=list)
    sequence_continuity: str = "unknown"


@dataclass(frozen=True)
class BookMessage:
    market_id: str
    source_file_id: str
    first_row: int
    last_row: int
    source_ordinal: int
    provider_group_id: str
    kind: str
    source_timestamp_us: int
    source_clock_kind: str
    exchange_ts_ns: int | None
    provider_receive_ts_us: int
    boundary_status: str
    state_time_certainty: str
    levels: tuple[tuple[str, Decimal, Decimal], ...]


@dataclass(frozen=True)
class TradeExecution:
    market_id: str
    source_file_id: str
    source_row: int
    source_ordinal: int
    source_timestamp_us: int
    source_clock_kind: str
    exchange_ts_ns: int | None
    provider_receive_ts_us: int
    trade_id: int
    aggressor_side: str
    price: Decimal
    quantity: Decimal
    normal_quantity: None = None
    individual_execution_count: int = 1
    native_aggregate_packet_count: None = None


def _decimal(value, *, positive: bool) -> Decimal:
    # The shared CSV reader provides text, so no intermediate binary float.
    if isinstance(value, float):
        raise ValueError("fact parser requires exact decimal text, not float input")
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("invalid decimal value") from exc
    if not number.is_finite() or number < 0 or (positive and number == 0):
        raise ValueError("price/quantity outside permitted range")
    return number


def _integer(value, *, positive=False) -> int:
    if isinstance(value, bool) or not str(value).isdigit():
        raise ValueError("timestamp/identity must be a nonnegative integer")
    number = int(value)
    if positive and not number:
        raise ValueError("source timestamp must be positive")
    return number


def _clock(kind: str, timestamp: int) -> int | None:
    if kind not in CLOCK_KINDS:
        raise ValueError("unsupported source clock kind")
    return timestamp * 1000 if kind.startswith("exchange_") else None


def _source_rows(fragments: Iterable[SourceFragment], quality: SourceQuality):
    from data.normalize_tardis_orderbook import _open_csv

    pending_id, next_row = None, 1
    closed_ids: set[str] = set()
    for fragment in fragments:
        path = Path(fragment.path)
        if path.is_symlink() or path.suffix == ".parquet":
            raise ValueError("source facts require original CSV fragments, not mixed Parquet")
        if not fragment.file_id or fragment.first_row < 1:
            raise ValueError("source fragment identity/row offset required")
        if pending_id is not None:
            if fragment.file_id != pending_id or fragment.first_row != next_row:
                raise ValueError("non-contiguous physical source fragments")
        elif fragment.file_id in closed_ids or fragment.first_row != 1:
            raise ValueError("duplicate source file or missing source prefix")
        row_number = fragment.first_row
        with _open_csv(path, exact_values=True) as reader:
            for batch in reader:
                columns = batch.to_pydict()
                names = tuple(columns)
                for values in zip(*(columns[name] for name in names), strict=True):
                    quality.rows += 1
                    yield fragment, row_number, dict(zip(names, values, strict=True))
                    row_number += 1
        yield fragment, row_number, None
        if fragment.complete_file:
            closed_ids.add(fragment.file_id)
            pending_id, next_row = None, 1
        else:
            pending_id, next_row = fragment.file_id, row_number
    if pending_id is not None:
        raise ValueError("incomplete original source EOF; pending message not published")


def _identity(row: dict, symbol: str) -> tuple[str, int, int]:
    if symbol not in {"BTCUSDC", "BTCUSDT"}:
        raise ValueError("unsupported execution/reference symbol")
    if row.get("exchange") != "binance-futures" or row.get("symbol") != symbol:
        raise ValueError("Tardis market/symbol mismatch")
    return (f"binance_futures:perpetual:{symbol}",
            _integer(row["timestamp"], positive=True),
            _integer(row["local_timestamp"]))


def _regression(quality, previous, timestamp, *, market, channel, ordinal, kind):
    if previous is not None and timestamp < previous:
        quality.time_regressions.append({"market_id": market, "channel": channel,
            "source_ordinal": ordinal, "previous_source_timestamp": previous,
            "current_source_timestamp": timestamp, "regression_size": previous - timestamp,
            "kind": kind})


def iter_book_messages(fragments: Iterable[SourceFragment], *, symbol: str,
                       delta_clock_kind: str = "unknown", snapshot_clock_kind: str = "unknown",
                       quality: SourceQuality | None = None) -> Iterator[BookMessage]:
    """Contiguous provider groups survive physical chunks; complete EOF flushes."""
    quality = quality if quality is not None else SourceQuality()
    _clock(delta_clock_kind, 1)
    _clock(snapshot_clock_kind, 1)
    current, levels = None, []
    first = last = ordinal = 0
    previous = None

    def finish():
        market, file_id, timestamp, local, snapshot = current
        kind = snapshot_clock_kind if snapshot else delta_clock_kind
        return BookMessage(market, file_id, first, last, ordinal,
            f"{file_id}:{first}-{last}", "snapshot" if snapshot else "delta",
            timestamp, kind, _clock(kind, timestamp), local,
            "provider_contiguous_group_not_native_packet",
            "snapshot_anchor_not_exact_state" if snapshot else
            ("exchange_field_not_matching_time" if kind.startswith("exchange_") else "unknown"),
            tuple(levels))

    for fragment, row_number, row in _source_rows(fragments, quality):
        if row is None:
            if fragment.complete_file and current is not None:
                quality.emitted_events += 1
                yield finish()
                ordinal += 1
                current, levels = None, []
            continue
        market, timestamp, local = _identity(row, symbol)
        snapshot = row["is_snapshot"]
        if not isinstance(snapshot, bool) or row["side"] not in {"bid", "ask"}:
            raise ValueError("invalid snapshot flag/book side")
        level = (row["side"], _decimal(row["price"], positive=True),
                 _decimal(row["amount"], positive=False))
        key = (market, fragment.file_id, timestamp, local, snapshot)
        if key != current:
            if current is not None:
                quality.emitted_events += 1
                yield finish()
                ordinal += 1
            _regression(quality, previous, timestamp, market=market,
                        channel="incremental_book_L2", ordinal=ordinal,
                        kind="snapshot" if snapshot else "delta")
            previous = timestamp
            current, levels, first = key, [], row_number
        last = row_number
        levels.append(level)


def iter_trade_executions(fragments: Iterable[SourceFragment], *, symbol: str,
                          clock_kind: str = "unknown", quality: SourceQuality | None = None,
                          identities: dict | None = None) -> Iterator[TradeExecution]:
    """Exact identity dedup over this stream (or a caller-owned overlap store).

    No date clipping or ID-range-derived missing volume. The explicit identity
    store must be sized/persisted by a full-calendar runner; this parser does not
    claim constant-memory global dedup. File order is retained, including time
    regressions recorded in quality. Strict scheduling is a consumer decision.
    """
    quality = quality if quality is not None else SourceQuality()
    identities = identities if identities is not None else {}
    _clock(clock_kind, 1)
    previous = None
    ordinal = 0
    for fragment, row_number, row in _source_rows(fragments, quality):
        if row is None:
            continue
        market, timestamp, local = _identity(row, symbol)
        trade_id = _integer(row["id"])
        if row["side"] not in {"buy", "sell"}:
            raise ValueError("invalid aggressor side")
        price, quantity = _decimal(row["price"], positive=True), _decimal(row["amount"], positive=True)
        content = (timestamp, row["side"], price, quantity)
        key = (market, trade_id)
        if key in identities:
            if identities[key] != content:
                quality.trade_conflicts += 1
                raise ValueError(f"conflicting trade identity {trade_id}")
            quality.duplicate_trades += 1
            continue
        identities[key] = content
        _regression(quality, previous, timestamp, market=market, channel="trades",
                    ordinal=ordinal, kind="trade")
        previous = timestamp
        quality.emitted_events += 1
        yield TradeExecution(market, fragment.file_id, row_number, ordinal, timestamp,
            clock_kind, _clock(clock_kind, timestamp), local, trade_id, row["side"], price, quantity)
        ordinal += 1


@dataclass(frozen=True)
class BookView:
    version: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    source_asof_us: int | None
    state_change_us: int | None
    state_time_certainty: str
    valid: bool

    @property
    def bbo(self):
        return (self.bids[0], self.asks[0]) if self.valid else None


class ObservableBook:
    """Atomic all-known-level book; deliberately has no own orders or queues."""

    def __init__(self):
        self._levels: dict = {}
        self._prices = {"bid": [], "ask": []}
        self.market_id = None
        self.version = 0
        self.initialized = False
        self.source_asof_us = self.state_change_us = None
        self.state_time_certainty = "unknown"

    def apply(self, message: BookMessage) -> bool:
        if self.market_id is not None and message.market_id != self.market_id:
            raise ValueError("book market mismatch")
        if message.kind not in {"snapshot", "delta"}:
            raise ValueError("unsupported book message kind")
        if not self.initialized and message.kind != "snapshot":
            return False
        # Validate the entire message before mutation. A delta stages only its
        # touched levels, avoiding a full-book copy per incremental message.
        updates = {}
        for side, price, quantity in message.levels:
            if side not in {"bid", "ask"} or not price.is_finite() or price <= 0 or not quantity.is_finite() or quantity < 0:
                raise ValueError("invalid level; previous state unchanged")
            updates[(side, price)] = quantity
        if message.kind == "snapshot":
            staged = {key: quantity for key, quantity in updates.items() if quantity}
            changed = staged != self._levels
            self._levels = staged
            self._reindex()
        else:
            changed = any(self._levels.get(key) != (quantity or None) for key, quantity in updates.items())
            for key, quantity in updates.items():
                if quantity:
                    if key not in self._levels:
                        heapq.heappush(self._prices[key[0]], key[1].copy_negate() if key[0] == "bid" else key[1])
                    self._levels[key] = quantity
                else:
                    self._levels.pop(key, None)
            if sum(map(len, self._prices.values())) > 2 * len(self._levels) + 2048:
                self._reindex()
        self.market_id = message.market_id
        self.initialized = True
        self.version += 1
        self.source_asof_us = message.source_timestamp_us
        if changed:
            self.state_change_us = message.source_timestamp_us
        # A later local delta does not establish exact time for untouched levels.
        if message.kind == "snapshot":
            self.state_time_certainty = message.state_time_certainty
        return True

    def quantity(self, side: str, price: Decimal) -> Decimal | None:
        # Without declared coverage bounds, absence does not prove zero.
        return self._levels.get((side, price))

    def _reindex(self):
        self._prices = {"bid": [], "ask": []}
        for side, price in self._levels:
            self._prices[side].append(price.copy_negate() if side == "bid" else price)
        for prices in self._prices.values():
            heapq.heapify(prices)

    def _top(self, side, depth):
        heap = self._prices[side]
        while heap and (side, heap[0].copy_negate() if side == "bid" else heap[0]) not in self._levels:
            heapq.heappop(heap)
        if not heap:
            return ()
        if depth == 1:
            price = heap[0].copy_negate() if side == "bid" else heap[0]
            return ((price, self._levels[side, price]),)
        # Extract only the requested top levels from the maintained price heap.
        # Sorting the full known book at every 100ms publication makes multi-day
        # consumers unnecessarily quadratic in retained depth. Deleted/re-added
        # prices can leave duplicate heap entries: retain one live entry only.
        retained, seen, result = [], set(), []
        while heap and len(result) < depth:
            entry = heapq.heappop(heap)
            price = entry.copy_negate() if side == "bid" else entry
            quantity = self._levels.get((side, price))
            if quantity is None or price in seen:
                continue
            seen.add(price)
            retained.append(entry)
            result.append((price, quantity))
        for entry in retained:
            heapq.heappush(heap, entry)
        return tuple(result)

    def view(self, depth: int = 20) -> BookView:
        if depth < 1:
            raise ValueError("depth must be positive")
        bids = self._top("bid", depth)
        asks = self._top("ask", depth)
        valid = bool(self.initialized and bids and asks and bids[0][0] < asks[0][0])
        return BookView(self.version, bids, asks, self.source_asof_us,
                        self.state_change_us, self.state_time_certainty, valid)
