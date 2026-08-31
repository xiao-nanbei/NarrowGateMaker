"""Strategy-independent native exchange-time order-book replay.

The scheduler reconstructs one public exchange book from native snapshot and
delta messages.  It deliberately has no order IDs, quote policy, inventory, or
campaign state.  Replay orders may query the reconstructed state at activation
and consume emitted level changes, but they cannot influence which market-data
events exist.
"""

from __future__ import annotations

import hashlib
import json
import warnings
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType

import numpy as np

from data.download_cryptohft_orderbook import (
    DEFAULT_EXCHANGE,
    OrderBookSequenceState,
    OrderBookState,
)
from data_paths import native_exchange_book_cache_root
from models.native_exchange_book_cache import (
    ensure_native_book_hour_cache,
    iter_native_book_hour_cache,
    native_book_hour_identity,
    native_book_parser_identity,
    require_native_book_hour_cache,
)
from models.tick_data_types import HistoricalExchangeBookEvent


def _normalize_side(value: str) -> str:
    side = str(value).strip().lower()
    if side in {"buy", "bid"}:
        return "bid"
    if side in {"sell", "ask"}:
        return "ask"
    raise ValueError(f"unsupported exchange-book side={value!r}")


def _optional_ms(value_ns: int) -> int:
    return max(0, int(value_ns) // 1_000_000)


@dataclass(frozen=True)
class ExchangeBookLevelChange:
    exchange_ts_ns: int
    receive_ts_ns: int
    side: str
    price_tick: int
    quantity_before: float
    quantity_after: float
    event_type: str
    segment_id: int
    update_id: int | None
    feature_ready_ts_ns: int = 0

    @property
    def delta_quantity(self) -> float:
        return float(self.quantity_after - self.quantity_before)


@dataclass(frozen=True)
class ExchangeBookAdvance:
    exchange_ts_ns: int
    source_events: tuple[HistoricalExchangeBookEvent, ...]
    level_changes: tuple[ExchangeBookLevelChange, ...]
    accepted_events: int
    rejected_events: int
    snapshot_reset: bool
    invalidated: bool
    feature_ready_ts_ns: int = 0


@dataclass(frozen=True)
class ExchangeBookBoundaryPreview:
    """Read-only preview of native messages at one exchange-time boundary."""

    exchange_ts_ns: int
    event_count: int
    touched_levels: frozenset[tuple[str, int]]
    snapshot_or_gap: bool


@dataclass(frozen=True)
class ExchangeBookLookup:
    side: str
    price_tick: int
    status: str
    reason: str
    quantity: float | None
    asof_exchange_ts_ns: int
    segment_id: int
    snapshot_min_tick: int | None
    snapshot_max_tick: int | None

    @property
    def strict_usable(self) -> bool:
        return (
            self.status in {"exact", "known_zero"}
            and self.quantity is not None
            and np.isfinite(self.quantity)
            and self.quantity >= 0.0
        )


@dataclass(frozen=True)
class ExchangeBookSchedulerStats:
    consumed_events: int
    accepted_events: int
    rejected_events: int
    snapshot_events: int
    delta_events: int
    delta_bootstrap_events: int
    source_gap_events: int
    sequence_gaps: int
    invalid_sequence_messages: int
    message_time_reversals: int
    segment_count: int
    last_exchange_ts_ns: int
    initialized: bool
    transaction_timestamp_events: int
    event_timestamp_fallback_events: int
    receive_timestamp_fallback_events: int
    unknown_timestamp_source_events: int


@dataclass(frozen=True)
class ExchangeBookVisibilityStats:
    enqueued_events: int
    delivered_events: int
    pre_exchange_clamped_events: int
    head_of_line_clamped_events: int
    max_head_of_line_delay_ns: int
    last_truth_exchange_ts_ns: int
    last_proposed_ready_ts_ns: int
    last_assigned_ready_ts_ns: int
    next_ready_ts_ns: int


@dataclass(frozen=True)
class ScheduledExchangeBookVisibilityEvent:
    """One admitted native event plus its immutable visibility identity."""

    event: HistoricalExchangeBookEvent
    provider_receive_ts_ns: int
    proposed_feature_ready_ts_ns: int
    assigned_feature_ready_ts_ns: int


class CryptoHFTExchangeBookTape:
    """Re-iterable native CryptoHFT source with optional warmup/continuation."""

    def __init__(
        self,
        *,
        raw_root: Path,
        day: str,
        symbol: str,
        tick_size: float,
        exchange: str = DEFAULT_EXCHANGE,
        warmup_hours: int = 24,
        continuation_hours: int = 0,
        strict_complete: bool = True,
        cache_dir: Path | None = None,
        cache_enabled: bool = True,
        refresh_cache: bool = False,
        cache_read_only: bool = False,
    ) -> None:
        if tick_size <= 0.0:
            raise ValueError("tick_size must be positive")
        if warmup_hours < 0:
            raise ValueError("warmup_hours must be non-negative")
        if continuation_hours < 0:
            raise ValueError("continuation_hours must be non-negative")
        day_start = datetime.strptime(str(day), "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        process_start = day_start - timedelta(hours=int(warmup_hours))
        process_end = day_start + timedelta(
            days=1,
            hours=int(continuation_hours),
        )
        expected: list[tuple[datetime, Path]] = []
        current = process_start
        while current < process_end:
            path = (
                Path(raw_root).expanduser().resolve()
                / str(exchange)
                / current.strftime("%Y-%m-%d")
                / current.strftime("%H")
                / f"{str(symbol).upper()}_orderbook.parquet.zst"
            )
            expected.append((current, path))
            current += timedelta(hours=1)
        missing = tuple(path for _, path in expected if not path.is_file())
        if strict_complete and missing:
            preview = ", ".join(str(path) for path in missing[:3])
            raise FileNotFoundError(
                f"native exchange-book tape is missing {len(missing)} hours: "
                f"{preview}"
            )

        self.raw_root = Path(raw_root).expanduser().resolve()
        self.day = day_start.strftime("%Y-%m-%d")
        self.symbol = str(symbol).upper()
        self.market_id = f"{exchange}:perpetual:{self.symbol}"
        self.tick_size = float(tick_size)
        self.exchange = str(exchange)
        self.warmup_hours = int(warmup_hours)
        self.continuation_hours = int(continuation_hours)
        self.strict_complete = bool(strict_complete)
        self.cache_enabled = bool(cache_enabled)
        self.cache_read_only = bool(cache_read_only)
        if self.cache_read_only and not self.cache_enabled:
            raise ValueError("cache_read_only requires cache_enabled")
        self.cache_dir = (
            Path(cache_dir).expanduser().resolve()
            if cache_dir is not None
            else native_exchange_book_cache_root()
        )
        self.refresh_cache = bool(refresh_cache)
        self.day_start_ns = int(day_start.timestamp() * 1_000_000_000)
        self.day_end_ns = int(
            (day_start + timedelta(days=1)).timestamp() * 1_000_000_000
        )
        self.process_start_ns = int(process_start.timestamp() * 1_000_000_000)
        self.process_end_ns = int(process_end.timestamp() * 1_000_000_000)
        self._expected = tuple(expected)
        self.missing_paths = missing
        self._parser_identity_sha256 = native_book_parser_identity()
        self._refreshed_paths: set[Path] = set()
        self._cache_hits = 0
        self._cache_misses = 0
        self._cache_failures = 0

    @property
    def source_paths(self) -> tuple[Path, ...]:
        return tuple(path for _, path in self._expected if path.is_file())

    def _iter_source_hour(
        self,
        path: Path,
    ) -> Iterator[HistoricalExchangeBookEvent]:
        # Keep the heavy parser outside module import so ordinary replay does
        # not pay its PyArrow setup cost when native exchange-book mode is off.
        from data.build_active_order_queue_tape import (
            iter_cryptohft_logical_messages,
        )

        ordinal = 0
        for message in iter_cryptohft_logical_messages(
            path,
            self.tick_size,
        ):
            exchange_ns = int(message.exchange_ts_ms) * 1_000_000
            if exchange_ns <= 0:
                continue
            if (
                int(message.transaction_time_ms) > 0
                and int(message.exchange_ts_ms)
                == int(message.transaction_time_ms)
            ):
                exchange_ts_source = "transaction"
            elif (
                int(message.event_time_ms) > 0
                and int(message.exchange_ts_ms)
                == int(message.event_time_ms)
            ):
                exchange_ts_source = "event"
            else:
                exchange_ts_source = "receive"
            ordinal += 1
            yield HistoricalExchangeBookEvent(
                market_id=self.market_id,
                event_type=message.event_type,
                exchange_ts_ns=exchange_ns,
                exchange_ts_source=exchange_ts_source,
                local_receive_ts_ns=int(message.receive_time_ns),
                event_time_ns=int(message.event_time_ms) * 1_000_000,
                transaction_time_ns=(
                    int(message.transaction_time_ms) * 1_000_000
                ),
                first_update_id=message.first_update_id,
                final_update_id=message.final_update_id,
                previous_final_update_id=message.previous_final_update_id,
                last_update_id=message.last_update_id,
                levels=tuple(message.levels),
                source=str(path),
                source_ordinal=ordinal,
            )

    def _iter_hour(
        self,
        path: Path,
    ) -> Iterator[HistoricalExchangeBookEvent]:
        if not self.cache_enabled:
            yield from self._iter_source_hour(path)
            return
        identity = native_book_hour_identity(
            source_path=path,
            symbol=self.symbol,
            exchange=self.exchange,
            market_id=self.market_id,
            tick_size=self.tick_size,
            parser_identity_sha256=self._parser_identity_sha256,
        )
        if self.cache_read_only:
            artifact = require_native_book_hour_cache(
                cache_root=self.cache_dir,
                identity=identity,
                verify_sha256=False,
            )
            self._cache_hits += 1
            yield from iter_native_book_hour_cache(artifact)
            return
        refresh = self.refresh_cache and path not in self._refreshed_paths
        try:
            artifact = ensure_native_book_hour_cache(
                cache_root=self.cache_dir,
                identity=identity,
                events_factory=lambda: self._iter_source_hour(path),
                refresh=refresh,
            )
            self._refreshed_paths.add(path)
            if artifact.cache_hit:
                self._cache_hits += 1
            else:
                self._cache_misses += 1
            yield from iter_native_book_hour_cache(artifact)
        except Exception as exc:
            self._cache_failures += 1
            warnings.warn(
                f"native book cache unavailable for {path}; reparsing source: "
                f"{exc}",
                RuntimeWarning,
                stacklevel=2,
            )
            yield from self._iter_source_hour(path)

    def materialize_cache(
        self,
        *,
        verify_sha256: bool = True,
        progress: Callable[[int, int, Path, bool], None] | None = None,
    ) -> dict[str, object]:
        """Single-owner materialization for every expected raw source hour."""

        if not self.cache_enabled:
            raise RuntimeError("native tape cache materialization is disabled")
        if self.cache_read_only:
            raise RuntimeError("read-only native tape cannot materialize cache")
        if self.missing_paths:
            raise FileNotFoundError(
                "native tape cache materialization requires all source hours"
            )
        total_hours = len(self._expected)
        for index, (_, path) in enumerate(self._expected, start=1):
            identity = native_book_hour_identity(
                source_path=path,
                symbol=self.symbol,
                exchange=self.exchange,
                market_id=self.market_id,
                tick_size=self.tick_size,
                parser_identity_sha256=self._parser_identity_sha256,
            )
            refresh = self.refresh_cache and path not in self._refreshed_paths
            artifact = ensure_native_book_hour_cache(
                cache_root=self.cache_dir,
                identity=identity,
                events_factory=lambda source=path: self._iter_source_hour(source),
                refresh=refresh,
            )
            self._refreshed_paths.add(path)
            if artifact.cache_hit:
                self._cache_hits += 1
            else:
                self._cache_misses += 1
            if progress is not None:
                progress(index, total_hours, path, bool(artifact.cache_hit))
        return self.cache_completeness(verify_sha256=verify_sha256)

    def cache_completeness(
        self,
        *,
        verify_sha256: bool = True,
    ) -> dict[str, object]:
        """Validate and identify the immutable cache backing this tape."""

        if not self.cache_enabled:
            raise RuntimeError("native tape cache validation is disabled")
        if self.missing_paths:
            raise FileNotFoundError(
                "native tape cache validation requires all source hours"
            )
        hours: list[dict[str, object]] = []
        for hour, path in self._expected:
            identity = native_book_hour_identity(
                source_path=path,
                symbol=self.symbol,
                exchange=self.exchange,
                market_id=self.market_id,
                tick_size=self.tick_size,
                parser_identity_sha256=self._parser_identity_sha256,
            )
            artifact = require_native_book_hour_cache(
                cache_root=self.cache_dir,
                identity=identity,
                verify_sha256=verify_sha256,
            )
            with artifact.manifest_path.open("r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            hours.append(
                {
                    "utc_hour": hour.strftime("%Y-%m-%dT%H:00:00Z"),
                    "source_path": str(path),
                    "cache_identity_sha256": artifact.identity_sha256,
                    "data_path": str(artifact.data_path),
                    "data_size_bytes": int(artifact.data_path.stat().st_size),
                    "data_sha256": str(manifest["data_sha256"]),
                    "manifest_path": str(artifact.manifest_path),
                    "manifest_sha256": hashlib.sha256(
                        artifact.manifest_path.read_bytes()
                    ).hexdigest(),
                    "event_count": int(artifact.event_count),
                    "level_count": int(artifact.level_count),
                }
            )
        body: dict[str, object] = {
            "schema_version": "native_exchange_book_tape_cache.v1",
            "day": self.day,
            "warmup_hours": self.warmup_hours,
            "continuation_hours": self.continuation_hours,
            "expected_hour_count": len(self._expected),
            "complete_hour_count": len(hours),
            "verify_sha256": bool(verify_sha256),
            "hours": hours,
        }
        body["canonical_identity_sha256"] = hashlib.sha256(
            json.dumps(
                body,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
        ).hexdigest()
        return body

    def __iter__(self) -> Iterator[HistoricalExchangeBookEvent]:
        ordinal = 0
        last_exchange_ns = 0
        for hour, path in self._expected:
            if not path.is_file():
                ordinal += 1
                yield HistoricalExchangeBookEvent(
                    market_id=self.market_id,
                    event_type="source_gap",
                    exchange_ts_ns=int(hour.timestamp() * 1_000_000_000),
                    exchange_ts_source="source_gap",
                    source=str(path),
                    source_ordinal=ordinal,
                )
                continue
            for event in self._iter_hour(path):
                exchange_ns = int(event.exchange_ts_ns)
                if exchange_ns < last_exchange_ns:
                    raise ValueError(
                        "native CryptoHFT exchange time regressed across raw "
                        f"messages: {exchange_ns} < {last_exchange_ns} ({path})"
                    )
                last_exchange_ns = exchange_ns
                ordinal += 1
                yield replace(
                    event,
                    source=str(path),
                    source_ordinal=ordinal,
                )

    def cache_stats(self) -> dict[str, object]:
        return {
            "enabled": self.cache_enabled,
            "read_only": self.cache_read_only,
            "cache_dir": str(self.cache_dir),
            "parser_identity_sha256": self._parser_identity_sha256,
            "hour_hits": self._cache_hits,
            "hour_misses_or_writes": self._cache_misses,
            "hour_failures_fallback_to_source": self._cache_failures,
        }

    def identity(self, *, include_sha256: bool = True) -> dict[str, object]:
        files = []
        for path in self.source_paths:
            row: dict[str, object] = {
                "path": str(path),
                "size_bytes": int(path.stat().st_size),
                "mtime_ns": int(path.stat().st_mtime_ns),
            }
            if include_sha256:
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                row["sha256"] = digest.hexdigest()
            files.append(row)
        return {
            "schema_version": "native_exchange_book_tape.v1",
            "day": self.day,
            "symbol": self.symbol,
            "market_id": self.market_id,
            "tick_size": self.tick_size,
            "exchange_clock": (
                "transaction_time_with_event_then_receive_fallback"
            ),
            "warmup_hours": self.warmup_hours,
            "continuation_hours": self.continuation_hours,
            "strict_complete": self.strict_complete,
            "missing_paths": [str(path) for path in self.missing_paths],
            "files": files,
        }


class HistoricalExchangeBookScheduler:
    """Reconstruct a native book on the exchange clock.

    The scheduler is intentionally policy-blind.  It emits public level
    changes and exact state lookups; queue position, cancel-ahead assumptions,
    and action decisions remain replay responsibilities.
    """

    def __init__(
        self,
        events: Iterable[HistoricalExchangeBookEvent],
        *,
        strict_sequence: bool = True,
        strict_after_ns: int = 0,
        allow_delta_bootstrap: bool = False,
        allow_one_shot: bool = False,
        track_mid_changes: bool = False,
        mid_change_start_ns: int = 0,
    ) -> None:
        iterator = iter(events)
        if iterator is events and not allow_one_shot:
            raise TypeError(
                "native exchange-book scheduler requires a re-iterable source"
            )
        self._iterator = iterator
        self._lookahead: deque[HistoricalExchangeBookEvent] = deque()
        self._next_event: HistoricalExchangeBookEvent | None = None
        self._strict_sequence = bool(strict_sequence)
        self._strict_after_ns = max(0, int(strict_after_ns))
        self._track_mid_changes = bool(track_mid_changes)
        self._mid_change_start_ns = int(mid_change_start_ns)
        self._last_mid_tick: float | None = None
        self._mid_changes: list[tuple[int, float]] = []
        self.book = OrderBookState()
        self.sequence = OrderBookSequenceState(
            self.book,
            allow_delta_bootstrap=bool(allow_delta_bootstrap),
        )
        self.snapshot_ranges: dict[str, tuple[int, int] | None] = {
            "bid": None,
            "ask": None,
        }
        self.known_ticks: dict[str, set[int]] = {
            "bid": set(),
            "ask": set(),
        }
        self.segment_id = 0
        self._segment_count = 0
        self._last_source_ts_ns = 0
        self._last_exchange_ts_ns = 0
        self._last_local_receive_ts_ns = 0
        self._last_boundary_ns = 0
        self._last_boundary_inclusive = False
        self._latest_batch_ts_ns: int | None = None
        self._latest_batch_prior_asof_ns = 0
        self._latest_batch_prior_segment_id = 0
        self._latest_batch_prior_initialized = False
        self._latest_batch_touched_levels: set[tuple[str, int]] = set()
        self._latest_batch_discontinuous = False
        self._consumed = 0
        self._accepted = 0
        self._rejected = 0
        self._snapshot_events = 0
        self._delta_events = 0
        self._source_gap_events = 0
        self._timestamp_source_counts = {
            "transaction": 0,
            "event": 0,
            "receive": 0,
            "unknown": 0,
        }
        self._push_next()

    def _strict_at(self, exchange_ts_ns: int) -> bool:
        return bool(
            self._strict_sequence
            and int(exchange_ts_ns) >= self._strict_after_ns
        )

    def _read_source_event(
        self,
    ) -> HistoricalExchangeBookEvent | None:
        try:
            event = next(self._iterator)
        except StopIteration:
            return None
        if not isinstance(event, HistoricalExchangeBookEvent):
            raise TypeError(
                "native exchange-book tape yielded "
                f"{type(event).__name__}, expected HistoricalExchangeBookEvent"
            )
        if int(event.exchange_ts_ns) < self._last_source_ts_ns:
            raise ValueError(
                "native exchange-book tape is not exchange-time sorted: "
                f"{event.exchange_ts_ns} < {self._last_source_ts_ns}"
            )
        self._last_source_ts_ns = int(event.exchange_ts_ns)
        return event

    def _push_next(self) -> None:
        event = (
            self._lookahead.popleft()
            if self._lookahead
            else self._read_source_event()
        )
        self._next_event = event

    def _ensure_lookahead_past(self, exchange_ts_ns: int) -> None:
        target = int(exchange_ts_ns)
        while (
            not self._lookahead
            or int(self._lookahead[-1].exchange_ts_ns) <= target
        ):
            event = self._read_source_event()
            if event is None:
                return
            self._lookahead.append(event)
            if int(event.exchange_ts_ns) > target:
                return

    def preview_at(
        self,
        exchange_ts_ns: int,
    ) -> ExchangeBookBoundaryPreview:
        """Inspect native messages at ``t`` without advancing book state."""

        target = int(exchange_ts_ns)
        if (
            self._next_event is None
            or int(self._next_event.exchange_ts_ns) != target
        ):
            return ExchangeBookBoundaryPreview(
                exchange_ts_ns=target,
                event_count=0,
                touched_levels=frozenset(),
                snapshot_or_gap=False,
            )
        self._ensure_lookahead_past(target)
        events = [self._next_event]
        events.extend(
            event
            for event in self._lookahead
            if int(event.exchange_ts_ns) == target
        )
        return ExchangeBookBoundaryPreview(
            exchange_ts_ns=target,
            event_count=len(events),
            touched_levels=frozenset(
                (str(side), int(tick))
                for event in events
                for side, tick, _ in event.levels
            ),
            snapshot_or_gap=any(
                event.event_type in {"snapshot", "source_gap"}
                for event in events
            ),
        )

    def _invalidate_local_state(self) -> None:
        self.book.reset()
        self.snapshot_ranges = {"bid": None, "ask": None}
        self.known_ticks = {"bid": set(), "ask": set()}
        self.segment_id = 0
        self._last_mid_tick = None

    def _record_mid_change(self, exchange_ts_ns: int) -> None:
        if not self._track_mid_changes or not self.sequence.initialized:
            return
        bids, asks = self.top_levels(1)
        if not bids or not asks:
            return
        best_bid_tick = float(bids[0][0])
        best_ask_tick = float(asks[0][0])
        if best_ask_tick <= best_bid_tick:
            raise ValueError(
                "native exchange-book mid tracker observed a crossed book at "
                f"{exchange_ts_ns}"
            )
        mid_tick = 0.5 * (best_bid_tick + best_ask_tick)
        previous = self._last_mid_tick
        self._last_mid_tick = mid_tick
        if (
            previous is not None
            and not np.isclose(
                mid_tick,
                previous,
                rtol=0.0,
                atol=1e-12,
            )
            and int(exchange_ts_ns) >= self._mid_change_start_ns
        ):
            self._mid_changes.append((int(exchange_ts_ns), float(mid_tick)))

    def _process_event(
        self,
        event: HistoricalExchangeBookEvent,
        *,
        emitted_levels: set[tuple[str, int]] | None = None,
    ) -> tuple[tuple[ExchangeBookLevelChange, ...], bool, bool, bool]:
        event_ts_ns = int(event.exchange_ts_ns)
        if self._latest_batch_ts_ns != event_ts_ns:
            # Retain only the latest timestamp's causal boundary, not a copy
            # of the book. Repeated advances/messages at that timestamp must
            # not replace the strictly earlier watermark with the new one.
            self._latest_batch_ts_ns = event_ts_ns
            self._latest_batch_prior_asof_ns = int(self._last_exchange_ts_ns)
            self._latest_batch_prior_segment_id = int(self.segment_id)
            self._latest_batch_prior_initialized = bool(self.sequence.initialized)
            self._latest_batch_touched_levels.clear()
            self._latest_batch_discontinuous = False
        self._latest_batch_touched_levels.update(
            (side, int(tick)) for side, tick, _ in event.levels
        )
        self._latest_batch_discontinuous |= event.event_type in {
            "snapshot", "source_gap"
        }
        self._last_local_receive_ts_ns = max(
            self._last_local_receive_ts_ns,
            int(event.local_receive_ts_ns or 0),
        )
        if event.event_type == "source_gap":
            was_initialized = bool(self.sequence.initialized)
            self.sequence.invalidate_source_gap()
            self._invalidate_local_state()
            self._source_gap_events += 1
            if self._strict_at(event.exchange_ts_ns):
                raise ValueError(
                    f"native exchange-book source gap at {event.exchange_ts_ns}"
                )
            return (), False, was_initialized, False

        was_initialized = bool(self.sequence.initialized)
        previous_gap_count = int(self.sequence.stats.sequence_gaps)
        apply_message = self.sequence.begin_message(
            event_type=event.event_type,
            receive_time_ms=_optional_ms(event.local_receive_ts_ns),
            event_time_ms=_optional_ms(event.event_time_ns),
            transaction_time_ms=_optional_ms(event.transaction_time_ns),
            first_update_id=event.first_update_id,
            final_update_id=event.final_update_id,
            previous_final_update_id=event.previous_final_update_id,
            last_update_id=event.last_update_id,
        )
        if not apply_message:
            invalidated = (
                int(self.sequence.stats.sequence_gaps) > previous_gap_count
            )
            if invalidated:
                self._invalidate_local_state()
                if self._strict_at(event.exchange_ts_ns):
                    raise ValueError(
                        "native exchange-book sequence gap at "
                        f"{event.exchange_ts_ns}"
                    )
            return (), False, bool(invalidated and was_initialized), False

        if (
            event.event_type != "snapshot"
            and not was_initialized
            and self.sequence.initialization_source == "delta"
        ):
            # Only levels explicitly touched by a native delta are known.
            # Untouched prices remain unknown because there is no snapshot
            # range from which absence could be interpreted as zero.
            self._segment_count += 1
            self.segment_id = self._segment_count
            self.known_ticks = {"bid": set(), "ask": set()}
            self.snapshot_ranges = {"bid": None, "ask": None}

        before: dict[tuple[str, int], float] = {}
        if event.event_type == "delta":
            for side, tick, _ in event.levels:
                key = (side, int(tick))
                if (
                    emitted_levels is not None
                    and key not in emitted_levels
                ):
                    continue
                levels = (
                    self.book.bid_levels
                    if side == "bid"
                    else self.book.ask_levels
                )
                before[key] = float(
                    levels.get(float(tick), 0.0)
                )

        if event.event_type == "snapshot":
            # A native snapshot replaces the previous segment's book. Keeping
            # old levels would manufacture depth that the new snapshot did not
            # attest and would widen the apparent exact-queue support range.
            self.book.reset()

        for side, tick, quantity in event.levels:
            self.book.apply(side, float(tick), float(quantity))

        if event.event_type == "snapshot":
            self._segment_count += 1
            self.segment_id = self._segment_count
            self.known_ticks = {"bid": set(), "ask": set()}
            for side, tick, _ in event.levels:
                self.known_ticks[side].add(int(tick))
            for side, levels in (
                ("bid", self.book.bid_levels),
                ("ask", self.book.ask_levels),
            ):
                ticks = [int(price) for price in levels]
                self.snapshot_ranges[side] = (
                    (min(ticks), max(ticks)) if ticks else None
                )
            self._snapshot_events += 1
            return (), True, False, True

        changes: list[ExchangeBookLevelChange] = []
        final_quantities: dict[tuple[str, int], float] = {}
        for side, tick, quantity in event.levels:
            self.known_ticks[side].add(int(tick))
            if (
                emitted_levels is not None
                and (side, int(tick)) not in emitted_levels
            ):
                continue
            final_quantities[(side, int(tick))] = float(quantity)
        for (side, tick), quantity_after in final_quantities.items():
            quantity_before = float(before.get((side, tick), 0.0))
            if np.isclose(
                quantity_before,
                quantity_after,
                rtol=0.0,
                atol=1e-15,
            ):
                continue
            changes.append(
                ExchangeBookLevelChange(
                    exchange_ts_ns=int(event.exchange_ts_ns),
                    receive_ts_ns=int(event.local_receive_ts_ns or 0),
                    side=side,
                    price_tick=tick,
                    quantity_before=quantity_before,
                    quantity_after=quantity_after,
                    event_type="delta",
                    segment_id=int(self.segment_id),
                    update_id=event.final_update_id,
                )
            )
        self._delta_events += 1
        return tuple(changes), False, False, True

    def advance_to(
        self,
        exchange_ts_ns: int,
        *,
        inclusive: bool = True,
        emitted_levels: set[tuple[str, int]] | None = None,
    ) -> ExchangeBookAdvance:
        """Advance the complete book and optionally filter emitted changes.

        The filter affects only the notification payload. Every native
        snapshot/delta level is still sequence-validated and applied to the
        full exchange book, so state and later exact lookups remain independent
        of the strategy trajectory.
        """

        target = int(exchange_ts_ns)
        if target < self._last_boundary_ns:
            raise ValueError(
                "native exchange-book replay time regressed: "
                f"{target} < {self._last_boundary_ns}"
            )
        if (
            target == self._last_boundary_ns
            and self._last_boundary_inclusive
            and not inclusive
        ):
            # Multiple execution trades can share one millisecond. The native
            # event at that timestamp was already consumed by the first loop;
            # callers cannot rewind, so return an explicit no-op boundary.
            return ExchangeBookAdvance(
                exchange_ts_ns=target,
                source_events=(),
                level_changes=(),
                accepted_events=0,
                rejected_events=0,
                snapshot_reset=False,
                invalidated=False,
            )

        source_events: list[HistoricalExchangeBookEvent] = []
        changes: list[ExchangeBookLevelChange] = []
        accepted_now = 0
        rejected_now = 0
        snapshot_reset = False
        invalidated = False
        while self._next_event is not None:
            event_ts = int(self._next_event.exchange_ts_ns)
            if event_ts > target or (event_ts == target and not inclusive):
                break
            event = self._next_event
            source_events.append(event)
            timestamp_source = str(event.exchange_ts_source)
            if timestamp_source == "source_gap":
                pass
            elif timestamp_source in self._timestamp_source_counts:
                self._timestamp_source_counts[timestamp_source] += 1
            else:
                self._timestamp_source_counts["unknown"] += 1
            event_changes, reset, invalid, accepted = self._process_event(
                event,
                emitted_levels=emitted_levels,
            )
            if accepted:
                self._record_mid_change(int(event.exchange_ts_ns))
            self._consumed += 1
            self._accepted += int(accepted)
            self._rejected += int(not accepted)
            accepted_now += int(accepted)
            rejected_now += int(not accepted)
            snapshot_reset = snapshot_reset or reset
            invalidated = invalidated or invalid
            changes.extend(event_changes)
            self._last_exchange_ts_ns = max(
                self._last_exchange_ts_ns,
                int(event.exchange_ts_ns),
            )
            self._push_next()
        self._last_boundary_ns = target
        self._last_boundary_inclusive = bool(inclusive)
        return ExchangeBookAdvance(
            exchange_ts_ns=target,
            source_events=tuple(source_events),
            level_changes=tuple(changes),
            accepted_events=accepted_now,
            rejected_events=rejected_now,
            snapshot_reset=snapshot_reset,
            invalidated=invalidated,
            feature_ready_ts_ns=0,
        )

    def apply_scheduled_events(
        self,
        events: Iterable[HistoricalExchangeBookEvent],
        *,
        boundary_ts_ns: int,
        inclusive: bool = True,
        emitted_levels: set[tuple[str, int]] | None = None,
    ) -> ExchangeBookAdvance:
        """Apply externally scheduled events to an otherwise empty source.

        This is used by the strategy-visibility scheduler below. The ordinary
        exchange-time scheduler must continue to consume its own immutable
        source via :meth:`advance_to`; mixing the two ingestion modes would
        make event lineage ambiguous and therefore fails closed.
        """

        if self._next_event is not None or self._lookahead:
            raise RuntimeError(
                "scheduled exchange-book events cannot be mixed with an "
                "iterator-backed scheduler"
            )
        target = int(boundary_ts_ns)
        if target < self._last_boundary_ns:
            raise ValueError(
                "scheduled exchange-book boundary regressed: "
                f"{target} < {self._last_boundary_ns}"
            )
        if (
            target == self._last_boundary_ns
            and self._last_boundary_inclusive
            and not inclusive
        ):
            raise ValueError(
                "scheduled exchange-book boundary cannot move from inclusive "
                "back to exclusive"
            )

        source_events = tuple(events)
        changes: list[ExchangeBookLevelChange] = []
        accepted_now = 0
        rejected_now = 0
        snapshot_reset = False
        invalidated = False
        previous_ts = int(self._last_exchange_ts_ns)
        for event in source_events:
            event_ts = int(event.exchange_ts_ns)
            if event_ts > target or (event_ts == target and not inclusive):
                raise ValueError(
                    "scheduled exchange-book event exceeds its visibility "
                    f"boundary: event={event_ts} boundary={target}"
                )
            if event_ts < previous_ts:
                raise ValueError(
                    "scheduled exchange-book events are not visibility-time "
                    f"sorted: {event_ts} < {previous_ts}"
                )
            previous_ts = event_ts
            timestamp_source = str(event.exchange_ts_source)
            if timestamp_source == "source_gap":
                pass
            elif timestamp_source in self._timestamp_source_counts:
                self._timestamp_source_counts[timestamp_source] += 1
            else:
                self._timestamp_source_counts["unknown"] += 1
            event_changes, reset, invalid, accepted = self._process_event(
                event,
                emitted_levels=emitted_levels,
            )
            if accepted:
                self._record_mid_change(event_ts)
            self._consumed += 1
            self._accepted += int(accepted)
            self._rejected += int(not accepted)
            accepted_now += int(accepted)
            rejected_now += int(not accepted)
            snapshot_reset = snapshot_reset or reset
            invalidated = invalidated or invalid
            changes.extend(event_changes)
            self._last_exchange_ts_ns = max(
                self._last_exchange_ts_ns,
                event_ts,
            )
        self._last_boundary_ns = target
        self._last_boundary_inclusive = bool(inclusive)
        return ExchangeBookAdvance(
            exchange_ts_ns=target,
            source_events=source_events,
            level_changes=tuple(changes),
            accepted_events=accepted_now,
            rejected_events=rejected_now,
            snapshot_reset=snapshot_reset,
            invalidated=invalidated,
            feature_ready_ts_ns=0,
        )

    def lookup(self, side: str, price_tick: int) -> ExchangeBookLookup:
        normalized_side = _normalize_side(side)
        tick = int(price_tick)
        bounds = self.snapshot_ranges[normalized_side]
        opposite_side = "ask" if normalized_side == "bid" else "bid"
        opposite_bounds = self.snapshot_ranges[opposite_side]
        bid_bounds = self.snapshot_ranges["bid"]
        ask_bounds = self.snapshot_ranges["ask"]
        snapshot_uncrossed = bool(
            bid_bounds is not None
            and ask_bounds is not None
            and bid_bounds[1] < ask_bounds[0]
        )
        minimum = bounds[0] if bounds is not None else None
        maximum = bounds[1] if bounds is not None else None
        if not self.sequence.initialized or self.segment_id <= 0:
            return ExchangeBookLookup(
                side=normalized_side,
                price_tick=tick,
                status="unknown",
                reason="sequence_unavailable",
                quantity=None,
                asof_exchange_ts_ns=int(self._last_exchange_ts_ns),
                segment_id=0,
                snapshot_min_tick=minimum,
                snapshot_max_tick=maximum,
            )
        levels = (
            self.book.bid_levels
            if normalized_side == "bid"
            else self.book.ask_levels
        )
        quantity = levels.get(float(tick))
        if quantity is not None and quantity > 0.0:
            status = "exact"
            reason = "visible_quantity"
            value: float | None = float(quantity)
        elif tick in self.known_ticks[normalized_side]:
            status = "known_zero"
            reason = "explicit_zero_or_removed_level"
            value = 0.0
        elif bounds is not None and bounds[0] <= tick <= bounds[1]:
            status = "known_zero"
            reason = "inside_snapshot_range_absent"
            value = 0.0
        elif (
            normalized_side == "bid"
            and snapshot_uncrossed
            and opposite_bounds is not None
            and tick >= opposite_bounds[0]
        ) or (
            normalized_side == "ask"
            and snapshot_uncrossed
            and opposite_bounds is not None
            and tick <= opposite_bounds[1]
        ):
            # At the snapshot boundary a bid at/above best ask, or an ask
            # at/below best bid, is structurally impossible in an uncrossed
            # book. Any later creation at that price must arrive as a delta,
            # so untouched prices in this half-line are exact known zeros.
            status = "known_zero"
            reason = "opposite_top_structural_zero"
            value = 0.0
        else:
            status = "unknown"
            reason = "outside_snapshot_range"
            value = None
        return ExchangeBookLookup(
            side=normalized_side,
            price_tick=tick,
            status=status,
            reason=reason,
            quantity=value,
            asof_exchange_ts_ns=int(self._last_exchange_ts_ns),
            segment_id=int(self.segment_id),
            snapshot_min_tick=minimum,
            snapshot_max_tick=maximum,
        )

    def lookup_strictly_before(
        self,
        side: str,
        price_tick: int,
        exchange_ts_ns: int,
    ) -> ExchangeBookLookup:
        """Read an activation seed without consuming or rewinding events.

        Callers advance to the activation boundary first. If its native batch
        was already consumed, an untouched level is still identical to its
        strictly prior state, provided the initialized segment did not change.
        Touched levels and discontinuities remain unavailable: this method
        does not choose an ordering for same-timestamp events. The touched set
        comes from all source levels, independent of ``emitted_levels``.
        """

        target = int(exchange_ts_ns)
        lookup = self.lookup(side, price_tick)
        if lookup.asof_exchange_ts_ns < target:
            return lookup
        if (
            lookup.asof_exchange_ts_ns > target
            or self._latest_batch_ts_ns != target
        ):
            return replace(
                lookup,
                status="unknown",
                reason="strict_before_state_not_retained",
                quantity=None,
            )
        if (
            self._latest_batch_discontinuous
            or int(self.segment_id) != self._latest_batch_prior_segment_id
            or bool(self.sequence.initialized)
            != self._latest_batch_prior_initialized
        ):
            reason = "same_timestamp_book_discontinuity"
        elif not self._latest_batch_prior_initialized:
            reason = "strict_before_sequence_unavailable"
        elif (lookup.side, lookup.price_tick) in self._latest_batch_touched_levels:
            reason = "same_timestamp_level_touched"
        else:
            return replace(
                lookup,
                asof_exchange_ts_ns=self._latest_batch_prior_asof_ns,
            )
        return replace(lookup, status="ambiguous", reason=reason, quantity=None)

    def top_levels(
        self,
        count: int,
    ) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        return self.book.top_levels(int(count))

    @property
    def boundary_exchange_ts_ns(self) -> int:
        """Latest exchange-time boundary consumed by the scheduler."""

        return int(self._last_boundary_ns)

    @property
    def last_local_receive_ts_ns(self) -> int:
        """Latest source receive timestamp consumed by the causal scheduler."""

        return int(self._last_local_receive_ts_ns)

    @property
    def next_exchange_ts_ns(self) -> int | None:
        """Next native message boundary without consuming it."""

        if self._next_event is None:
            return None
        return int(self._next_event.exchange_ts_ns)

    @property
    def mid_changes(self) -> tuple[tuple[int, float], ...]:
        """Recorded native mid changes as ``(exchange_ts_ns, mid_tick)``."""

        return tuple(self._mid_changes)

    def mid_changes_since(
        self,
        cursor: int,
    ) -> tuple[tuple[tuple[int, float], ...], int]:
        """Return only unseen mid changes and the next monotonic cursor."""

        start = max(0, min(int(cursor), len(self._mid_changes)))
        return tuple(self._mid_changes[start:]), len(self._mid_changes)

    @property
    def boundary_inclusive(self) -> bool:
        return bool(self._last_boundary_inclusive)

    def state_fingerprint(self) -> str:
        digest = hashlib.sha256()
        digest.update(
            (
                f"{self.segment_id}|{self.sequence.last_update_id}|"
                f"{self._last_exchange_ts_ns}\n"
            ).encode("ascii")
        )
        for side, levels in (
            ("bid", self.book.bid_levels),
            ("ask", self.book.ask_levels),
        ):
            for price_tick, quantity in sorted(levels.items()):
                digest.update(
                    f"{side}|{int(price_tick)}|{quantity:.12g}\n".encode(
                        "ascii"
                    )
                )
        return digest.hexdigest()

    def stats(self) -> ExchangeBookSchedulerStats:
        sequence_stats = self.sequence.stats
        return ExchangeBookSchedulerStats(
            consumed_events=int(self._consumed),
            accepted_events=int(self._accepted),
            rejected_events=int(self._rejected),
            snapshot_events=int(self._snapshot_events),
            delta_events=int(self._delta_events),
            delta_bootstrap_events=int(
                sequence_stats.delta_bootstrap_messages
            ),
            source_gap_events=int(self._source_gap_events),
            sequence_gaps=int(sequence_stats.sequence_gaps),
            invalid_sequence_messages=int(
                sequence_stats.invalid_sequence_messages
            ),
            message_time_reversals=int(
                sequence_stats.message_time_reversals
            ),
            segment_count=int(self._segment_count),
            last_exchange_ts_ns=int(self._last_exchange_ts_ns),
            initialized=bool(self.sequence.initialized),
            transaction_timestamp_events=int(
                self._timestamp_source_counts["transaction"]
            ),
            event_timestamp_fallback_events=int(
                self._timestamp_source_counts["event"]
            ),
            receive_timestamp_fallback_events=int(
                self._timestamp_source_counts["receive"]
            ),
            unknown_timestamp_source_events=int(
                self._timestamp_source_counts["unknown"]
            ),
        )

    def stats_dict(self) -> dict[str, object]:
        return asdict(self.stats())


class HistoricalMessageDeliverySchedule:
    """Immutable feature-ready delivery times for retained source messages.

    Input rows are in message order, including interleaved channels sharing a
    connection. All three clocks must already be aligned physical timestamps;
    profile sampling and clock-offset treatment belong to the caller. By
    default only ready-time head-of-line ordering is imposed. Opt-in callback
    serialization also queues callback entry behind the preceding completion,
    preserving each measured ready-minus-receive service duration exactly.
    Neither mode samples additional CPU or network latency.

    A channel identifies one ordered source array. Connection IDs determine
    which channels share head-of-line blocking; absent IDs use one connection
    per channel. Queries return the channel-local index, never the interleaved
    input ordinal, and do not mutate or resample the schedule.
    """

    def __init__(
        self,
        exchange_ts_ns,
        receive_ts_ns,
        feature_ready_ts_ns,
        *,
        channel_ids=None,
        connection_ids=None,
        serialize_callback_service: bool = False,
    ) -> None:
        def timestamps(values, name: str) -> np.ndarray:
            array = np.asarray(values)
            if array.ndim != 1 or (array.size and array.dtype.kind not in "iu"):
                raise ValueError(f"{name} must be a one-dimensional integer ns array")
            if array.size and (np.any(array < 0) or np.any(array > np.iinfo(np.int64).max)):
                raise ValueError(f"{name} must contain nonnegative int64 ns timestamps")
            return array.astype(np.int64, copy=False)

        exchange = timestamps(exchange_ts_ns, "exchange_ts_ns")
        receive = timestamps(receive_ts_ns, "receive_ts_ns")
        proposed = timestamps(feature_ready_ts_ns, "feature_ready_ts_ns")
        if receive.shape != exchange.shape or proposed.shape != exchange.shape:
            raise ValueError("message exchange/receive/ready arrays must be aligned")
        if np.any(exchange > receive) or np.any(receive > proposed):
            raise ValueError("aligned message clocks require exchange <= receive <= ready")

        def labels(values, default: np.ndarray, name: str) -> np.ndarray:
            array = default if values is None else np.asarray(values)
            if array.shape != exchange.shape or (array.size and array.dtype.kind not in "iuUS"):
                raise ValueError(f"{name} must be aligned integer or string labels")
            return array

        channels = labels(channel_ids, np.zeros(exchange.size, dtype=np.uint8), "channel_ids")
        connections = labels(connection_ids, channels, "connection_ids")
        assigned = proposed.copy()
        assigned_receive = receive.copy()
        connection_keys = np.unique(connections)
        for connection in connection_keys:
            indices = np.flatnonzero(connections == connection)
            if not serialize_callback_service:
                assigned[indices] = np.maximum.accumulate(proposed[indices])
                continue
            service = proposed[indices] - receive[indices]
            cumulative = np.cumsum(service, dtype=np.int64)
            if np.any(cumulative < 0) or np.any(cumulative[1:] < cumulative[:-1]):
                raise ValueError("callback service cumulative duration exceeds int64")
            prior = cumulative - service
            # Max-plus FIFO recurrence, vectorized without rounding nanoseconds:
            # finish_i = cumulative_i + max_j<=i(receive_j - cumulative_(j-1)).
            origin = np.maximum.accumulate(receive[indices] - prior)
            if np.any(origin > np.iinfo(np.int64).max - cumulative):
                raise ValueError("serialized callback completion exceeds int64")
            finish = cumulative + origin
            assigned[indices] = finish
            assigned_receive[indices] = finish - service

        self._ready_by_channel: dict[object, np.ndarray] = {}
        self._exchange_by_channel: dict[object, np.ndarray] = {}
        self._receive_by_channel: dict[object, np.ndarray] = {}
        for channel in np.unique(channels):
            indices = np.flatnonzero(channels == channel)
            ready = assigned[indices]
            if np.any(ready[1:] < ready[:-1]):
                raise ValueError(
                    "channel delivery order regressed across connections; "
                    "split sessions or supply an ordered connection group"
                )
            source = exchange[indices]
            received = assigned_receive[indices]
            ready.setflags(write=False)
            source.setflags(write=False)
            received.setflags(write=False)
            self._ready_by_channel[channel] = ready
            self._exchange_by_channel[channel] = source
            self._receive_by_channel[channel] = received
        if not exchange.size and channel_ids is None:
            empty = np.asarray([], dtype=np.int64)
            empty.setflags(write=False)
            self._ready_by_channel[0] = empty
            self._exchange_by_channel[0] = empty
            self._receive_by_channel[0] = empty
        adjustment = assigned - proposed
        callback_queue = assigned_receive - receive
        self._stats = {
            "message_count": int(exchange.size),
            "channel_count": len(self._ready_by_channel),
            "connection_count": int(connection_keys.size),
            "head_of_line_clamped_events": int(np.count_nonzero(adjustment)),
            "max_head_of_line_delay_ns": int(adjustment.max(initial=0)),
            "serialize_callback_service": bool(serialize_callback_service),
            "callback_queued_events": int(np.count_nonzero(callback_queue)),
            "max_callback_queue_delay_ns": int(callback_queue.max(initial=0)),
        }

    def _channel_key(self, channel):
        if channel is None:
            if len(self._ready_by_channel) != 1:
                raise ValueError("channel is required for a multi-channel schedule")
            return next(iter(self._ready_by_channel))
        if channel not in self._ready_by_channel:
            raise ValueError(f"unknown message channel: {channel!r}")
        return channel

    def ready_ns_for_channel(self, channel=None) -> np.ndarray:
        """Return read-only assigned ready times in channel-local row order."""
        return self._ready_by_channel[self._channel_key(channel)].view()

    def exchange_ns_for_channel(self, channel=None) -> np.ndarray:
        """Return unchanged read-only source times in channel-local row order."""
        return self._exchange_by_channel[self._channel_key(channel)].view()

    def receive_ns_for_channel(self, channel=None) -> np.ndarray:
        """Return receive/serialized callback-entry times in channel-local order."""
        return self._receive_by_channel[self._channel_key(channel)].view()

    def latest_visible_index(
        self, now_ts_ns: int, *, channel=None, inclusive: bool = False
    ) -> int:
        """Return the last delivered channel-local row, or -1 before delivery."""
        if isinstance(now_ts_ns, (bool, np.bool_)) or not isinstance(now_ts_ns, (int, np.integer)):
            raise ValueError("now_ts_ns must be an integer nanosecond boundary")
        ready = self._ready_by_channel[self._channel_key(channel)]
        return int(np.searchsorted(ready, now_ts_ns, side="right" if inclusive else "left") - 1)

    def stats_dict(self) -> dict[str, int]:
        return dict(self._stats)


@dataclass(frozen=True)
class _ReceiveTimeCooldownDecision:
    action_id: str
    duration_ms: float
    fallback_reason: str | None
    matched_rule_index: int | None
    policy_sha256: str
    predicate_bundle_sha256: str
    snapshot_id: str
    support_valid: bool


@dataclass(frozen=True)
class _ReceiveTimeCooldownSnapshot:
    snapshot_id: str
    assignment_id: str
    m0_context: object
    decision: _ReceiveTimeCooldownDecision
    policy_input_valid: bool
    fallback_policy_id: str | None
    fallback_reason: str | None
    source_bundle_sha256: str = ""


class ReceiveTimeCooldownReplayAdapter:
    """Replay supplied live policies using delivered depth callbacks.

    This diagnostic adapter reuses each policy's own receive-time aggregation,
    warmup and control fallback. It does not turn source-time research windows
    into receive-time windows by changing their timestamps, and grants no
    research-snapshot authority. The caller supplies the retained depth stream
    (including warmup), its simulated delivery schedule and loaded policies.
    """

    def __init__(self, depth_data, delivery_schedule, *, policies, channel=None):
        self._depth = depth_data
        self._ready = delivery_schedule.ready_ns_for_channel(channel)
        raw_receive = delivery_schedule.receive_ns_for_channel(channel)
        # A depth connection cannot deliver later sequence messages before an
        # earlier one. Sampled marginal receive delays may otherwise regress.
        self._receive = np.maximum.accumulate(raw_receive)
        self._receive_clamps = int(np.count_nonzero(self._receive != raw_receive))
        exchange = delivery_schedule.exchange_ns_for_channel(channel)
        if not np.array_equal(np.asarray(depth_data.ts_ms) * 1_000_000, exchange):
            raise ValueError("receive-time policy depth rows and delivery clocks differ")
        if any(
            len(getattr(depth_data, name)) != len(exchange)
            for name in ("bid_px", "ask_px", "bid_qty", "ask_qty")
        ):
            raise ValueError("receive-time policy depth arrays must be aligned")
        self._policies = {str(side).upper(): policy for side, policy in policies.items()}
        if not self._policies or set(self._policies) - {"BUY", "SELL"}:
            raise ValueError("receive-time policy sides must be BUY or SELL")
        self._cursor = 0
        self._last_cutoff = -1
        self._captures = 0
        self._fallbacks = 0
        self._evaluations = 0

    def capture_exposure_fill(
        self, *, assignment_id, fill_exchange_ts_ns, fill_visible_ts_ns, m0_context, **_lineage
    ):
        exchange_ns, cutoff = int(fill_exchange_ts_ns), int(fill_visible_ts_ns)
        if exchange_ns < 0 or exchange_ns > cutoff or cutoff < self._last_cutoff:
            raise ValueError("receive-time policy fill clocks are not causal/monotonic")
        context = dict(m0_context)
        if int(context["fill_visible_ts_ns"]) != cutoff:
            raise ValueError("receive-time policy context fill clock differs")
        side = str(context["side"]).upper()
        policy = self._policies[side]
        last = int(np.searchsorted(self._ready, cutoff, side="left"))
        # All callbacks are delivered, not just the latest book. A same-time
        # callback is withheld because its order relative to the fill is unknown.
        for index in range(self._cursor, last):
            bids = list(zip(self._depth.bid_px[index], self._depth.bid_qty[index], strict=True))
            asks = list(zip(self._depth.ask_px[index], self._depth.ask_qty[index], strict=True))
            for observer in self._policies.values():
                observer.observe_depth(
                    receive_ts_ns=int(self._receive[index]), bids=bids, asks=asks,
                    market_generation=index + 1, depth_generation=index + 1,
                )
        self._cursor = last
        self._last_cutoff = cutoff
        snapshot_id = f"{assignment_id}:receive-time-policy"
        raw = policy.evaluate(
            side=side, baseline_duration_ms=int(round(context["baseline_duration_ms"])),
            campaign_age_s=float(context["campaign_age_s"]), decision_ts_ns=cutoff,
            snapshot_id=snapshot_id,
        )
        decision = _ReceiveTimeCooldownDecision(
            action_id=str(raw.action_id), duration_ms=float(raw.duration_ms),
            fallback_reason=raw.fallback_reason, matched_rule_index=raw.matched_rule_index,
            policy_sha256=str(raw.policy_sha256),
            predicate_bundle_sha256=str(raw.predicate_bundle_sha256),
            snapshot_id=snapshot_id, support_valid=bool(raw.support_valid),
        )
        self._captures += 1
        self._fallbacks += int(raw.fallback_reason is not None)
        return _ReceiveTimeCooldownSnapshot(
            snapshot_id=snapshot_id, assignment_id=str(assignment_id),
            m0_context=MappingProxyType(context), decision=decision,
            policy_input_valid=decision.support_valid,
            fallback_policy_id=decision.action_id if decision.fallback_reason else None,
            fallback_reason=decision.fallback_reason,
        )

    def evaluate(self, snapshot, baseline_duration_ms):
        if float(baseline_duration_ms) != float(snapshot.m0_context["baseline_duration_ms"]):
            raise ValueError("receive-time policy baseline changed after capture")
        self._evaluations += 1
        return snapshot.decision

    def audit(self):
        return {
            "transport": "receive_time_policy",
            "feature_clock": "live_policy_receive_time_windows",
            "visibility": "depth_feature_ready_strictly_before_fill_visible",
            "research_snapshot_authority": False,
            "depth_rows_available": len(self._ready),
            "depth_callbacks_consumed": self._cursor,
            "receive_head_of_line_clamped_events": self._receive_clamps,
            "snapshots_emitted": self._captures,
            "fallback_snapshots": self._fallbacks,
            "evaluations": self._evaluations,
            "policies": {side: policy.audit() for side, policy in self._policies.items()},
        }


class HistoricalExchangeBookVisibilityScheduler:
    """Reconstruct a strategy-visible book on an explicit feature-ready clock.

    Native events are first admitted by the exchange-time truth scheduler, then
    enqueued here with a separately computed feature-ready timestamp. Provider
    receive timestamps can regress because they are measured on another clock;
    TCP/sequence visibility cannot. We therefore apply a head-of-line clamp in
    native source order and record every clamp for transport diagnostics.
    """

    def __init__(
        self,
        *,
        strict_sequence: bool = True,
        strict_after_ns: int = 0,
        allow_delta_bootstrap: bool = False,
    ) -> None:
        self._book = HistoricalExchangeBookScheduler(
            (),
            strict_sequence=bool(strict_sequence),
            strict_after_ns=int(strict_after_ns),
            allow_delta_bootstrap=bool(allow_delta_bootstrap),
        )
        self._pending: deque[ScheduledExchangeBookVisibilityEvent] = deque()
        self._enqueued = 0
        self._delivered = 0
        self._pre_exchange_clamped = 0
        self._head_of_line_clamped = 0
        self._max_head_of_line_delay_ns = 0
        self._last_truth_exchange_ts_ns = 0
        self._last_proposed_ready_ts_ns = 0
        self._last_assigned_ready_ts_ns = 0
        self._last_delivered_ready_ts_ns = 0
        self._last_delivered_provider_receive_ts_ns = 0
        self._last_boundary_ns = 0
        self._last_boundary_inclusive = False

    def enqueue(
        self,
        event: HistoricalExchangeBookEvent,
        *,
        feature_ready_ts_ns: int,
    ) -> int:
        """Schedule one native event and return its assigned ready timestamp."""

        truth_ts = int(event.exchange_ts_ns)
        if truth_ts < self._last_truth_exchange_ts_ns:
            raise ValueError(
                "visibility scheduler source exchange time regressed: "
                f"{truth_ts} < {self._last_truth_exchange_ts_ns}"
            )
        proposed = int(feature_ready_ts_ns)
        causal_ready = proposed
        if causal_ready < truth_ts:
            causal_ready = truth_ts
            self._pre_exchange_clamped += 1
        assigned = max(causal_ready, self._last_assigned_ready_ts_ns)
        if assigned > causal_ready:
            self._head_of_line_clamped += 1
            self._max_head_of_line_delay_ns = max(
                self._max_head_of_line_delay_ns,
                assigned - causal_ready,
            )
        if assigned < self._last_boundary_ns or (
            assigned == self._last_boundary_ns
            and self._last_boundary_inclusive
        ):
            raise ValueError(
                "feature-ready exchange-book event arrived behind the visible "
                f"scheduler boundary: ready={assigned} "
                f"boundary={self._last_boundary_ns}"
            )

        scheduled = ScheduledExchangeBookVisibilityEvent(
            event=event,
            provider_receive_ts_ns=int(event.local_receive_ts_ns or 0),
            proposed_feature_ready_ts_ns=proposed,
            assigned_feature_ready_ts_ns=assigned,
        )
        self._pending.append(scheduled)
        self._enqueued += 1
        self._last_truth_exchange_ts_ns = truth_ts
        self._last_proposed_ready_ts_ns = proposed
        self._last_assigned_ready_ts_ns = assigned
        return assigned

    def enqueue_many(
        self,
        events: Iterable[HistoricalExchangeBookEvent],
        *,
        ready_timestamp: Callable[[HistoricalExchangeBookEvent], int],
    ) -> tuple[int, ...]:
        """Schedule events using a deterministic caller-owned clock resolver."""

        return tuple(
            self.enqueue(
                event,
                feature_ready_ts_ns=int(ready_timestamp(event)),
            )
            for event in events
        )

    def advance_to(
        self,
        feature_ready_ts_ns: int,
        *,
        inclusive: bool = True,
        emitted_levels: set[tuple[str, int]] | None = None,
    ) -> ExchangeBookAdvance:
        target = int(feature_ready_ts_ns)
        if target < self._last_boundary_ns:
            raise ValueError(
                "exchange-book visibility time regressed: "
                f"{target} < {self._last_boundary_ns}"
            )
        if (
            target == self._last_boundary_ns
            and self._last_boundary_inclusive
            and not inclusive
        ):
            raise ValueError(
                "exchange-book visibility boundary cannot move from inclusive "
                "back to exclusive"
            )
        due: list[ScheduledExchangeBookVisibilityEvent] = []
        while self._pending:
            ready = int(self._pending[0].assigned_feature_ready_ts_ns)
            if ready > target or (ready == target and not inclusive):
                break
            due.append(self._pending.popleft())
        source_events: list[HistoricalExchangeBookEvent] = []
        level_changes: list[ExchangeBookLevelChange] = []
        accepted_events = 0
        rejected_events = 0
        snapshot_reset = False
        invalidated = False
        for scheduled in due:
            ready = int(scheduled.assigned_feature_ready_ts_ns)
            step = self._book.apply_scheduled_events(
                (scheduled.event,),
                boundary_ts_ns=ready,
                inclusive=True,
                emitted_levels=emitted_levels,
            )
            source_events.extend(step.source_events)
            level_changes.extend(
                replace(change, feature_ready_ts_ns=ready)
                for change in step.level_changes
            )
            accepted_events += int(step.accepted_events)
            rejected_events += int(step.rejected_events)
            snapshot_reset = snapshot_reset or bool(step.snapshot_reset)
            invalidated = invalidated or bool(step.invalidated)
            self._last_delivered_ready_ts_ns = ready
            self._last_delivered_provider_receive_ts_ns = max(
                self._last_delivered_provider_receive_ts_ns,
                int(scheduled.provider_receive_ts_ns),
            )
        if target > self._book.boundary_exchange_ts_ns or (
            target == self._book.boundary_exchange_ts_ns
            and inclusive
            and not self._book.boundary_inclusive
        ):
            self._book.apply_scheduled_events(
                (),
                boundary_ts_ns=target,
                inclusive=bool(inclusive),
                emitted_levels=emitted_levels,
            )
        advance = ExchangeBookAdvance(
            exchange_ts_ns=target,
            source_events=tuple(source_events),
            level_changes=tuple(level_changes),
            accepted_events=accepted_events,
            rejected_events=rejected_events,
            snapshot_reset=snapshot_reset,
            invalidated=invalidated,
            feature_ready_ts_ns=target,
        )
        self._delivered += len(due)
        self._last_boundary_ns = target
        self._last_boundary_inclusive = bool(inclusive)
        return advance

    def lookup(self, side: str, price_tick: int) -> ExchangeBookLookup:
        return self._book.lookup(side, price_tick)

    def top_levels(
        self,
        count: int,
    ) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        return self._book.top_levels(count)

    @property
    def sequence(self) -> OrderBookSequenceState:
        return self._book.sequence

    @property
    def segment_id(self) -> int:
        return int(self._book.segment_id)

    @property
    def last_feature_ready_ts_ns(self) -> int:
        return int(self._last_delivered_ready_ts_ns)

    @property
    def last_provider_receive_ts_ns(self) -> int:
        return int(self._last_delivered_provider_receive_ts_ns)

    @property
    def last_truth_exchange_ts_ns(self) -> int:
        return int(self._book.stats().last_exchange_ts_ns)

    @property
    def next_feature_ready_ts_ns(self) -> int | None:
        return (
            int(self._pending[0].assigned_feature_ready_ts_ns)
            if self._pending
            else None
        )

    @property
    def boundary_feature_ready_ts_ns(self) -> int:
        return int(self._last_boundary_ns)

    @property
    def boundary_inclusive(self) -> bool:
        return bool(self._last_boundary_inclusive)

    def state_fingerprint(self) -> str:
        return self._book.state_fingerprint()

    def book_stats(self) -> ExchangeBookSchedulerStats:
        return self._book.stats()

    def stats(self) -> ExchangeBookVisibilityStats:
        return ExchangeBookVisibilityStats(
            enqueued_events=int(self._enqueued),
            delivered_events=int(self._delivered),
            pre_exchange_clamped_events=int(self._pre_exchange_clamped),
            head_of_line_clamped_events=int(self._head_of_line_clamped),
            max_head_of_line_delay_ns=int(self._max_head_of_line_delay_ns),
            last_truth_exchange_ts_ns=int(self._last_truth_exchange_ts_ns),
            last_proposed_ready_ts_ns=int(self._last_proposed_ready_ts_ns),
            last_assigned_ready_ts_ns=int(self._last_assigned_ready_ts_ns),
            next_ready_ts_ns=int(self.next_feature_ready_ts_ns or 0),
        )

    def stats_dict(self) -> dict[str, object]:
        return asdict(self.stats())
