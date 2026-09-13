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
import math
import re
import warnings
from copy import deepcopy
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType

import numpy as np

from data.daily_raw import book_stream_priority, book_stream_ranks
from data.downloaders.cryptohft_orderbook import (
    DEFAULT_EXCHANGE,
    OrderBookSequenceState,
    OrderBookState,
    recorder_snapshot_anchor_ms,
    raw_hour_available,
    raw_hour_storage_path,
)
from data_paths import daily_market_path, native_exchange_book_cache_root, resolve_portable_path
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
    provider_ordered_events: int = 0
    sequence_anchored_snapshot_events: int = 0


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


@dataclass(frozen=True)
class ReconstructedExchangeBookEvent(HistoricalExchangeBookEvent):
    """A selected-state observation, not an original exchange delta."""

    fusion_reason: str = "unknown_reconstruction"
    source_id: str = "unknown"
    source_observed_ts_ns: int = 0

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.sequence_scope != "provider_ordered":
            raise ValueError("reconstructed books cannot claim exchange sequence authority")
        if self.fusion_reason not in {
            "source_update", "source_switch", "source_snapshot_reset",
            "observation_refresh", "carried_opening_snapshot", "unknown_reconstruction",
        }:
            raise ValueError("unsupported reconstructed book reason")
        if not 0 <= self.source_observed_ts_ns <= self.exchange_ts_ns:
            raise ValueError("reconstructed book has a future or invalid source observation")
        if self.fusion_reason != "unknown_reconstruction" and self.source_observed_ts_ns == 0:
            raise ValueError("reconstructed source observation is missing")

    @property
    def state_rebase(self) -> bool:
        return self.event_type == "snapshot" or self.fusion_reason in {
            "source_switch", "source_snapshot_reset", "carried_opening_snapshot",
            "unknown_reconstruction",
        }


@dataclass(frozen=True)
class ObservedUnionExchangeBookEvent(HistoricalExchangeBookEvent):
    """One original source message inside a multi-source observation union."""

    source_id: str = ""
    source_native_sequence: bool = False
    source_observed_ts_ns: int = 0
    source_timestamp_ns: int = 0
    stream_priority: int | None = None
    queue_rebase: bool = False
    observation_only: bool = False
    stream_contract: Mapping[str, object] | None = None
    day_initial_state: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.source_id or type(self.source_native_sequence) is not bool:
            raise ValueError("union source identity/sequence declaration is missing")
        expected = "exchange_sequence" if self.source_native_sequence else "provider_ordered"
        if self.sequence_scope != expected:
            raise ValueError("union source sequence scope disagrees with its native declaration")
        if (not 0 < self.source_observed_ts_ns <= self.exchange_ts_ns or self.source_timestamp_ns < 0
                or (self.source_native_sequence and self.source_timestamp_ns == 0)):
            raise ValueError("union source observation is missing or in the future")
        if self.stream_priority is not None and type(self.stream_priority) is not int:
            raise ValueError("union stream priority must be an integer")


class _UnionSourceBook(OrderBookState):
    def reset(self) -> None:
        # A selected outer view may still reference the old valid containers.
        # Replacing them retains that aged view without copying on every delta.
        self.bid_levels, self.ask_levels = {}, {}
        self.bid_heap, self.ask_heap = [], []


@dataclass
class _UnionSourceState:
    scheduler: HistoricalExchangeBookScheduler
    native: bool | None = None
    observed_ns: int = 0
    last_presentation_ns: int = 0
    local_ns: int = 0


def _observed_union_metadata(metadata: Mapping[bytes, bytes]) -> bool:
    return metadata.get(b"narrowgate.book_fusion") in {b"observed_union.v1", b"narrowgate.daily_book.v1"}


def _reconstructed_book_metadata(metadata: Mapping[bytes, bytes]) -> bool:
    """Never decode a source union as one ordinary provider state."""
    fusion = metadata.get(b"narrowgate.book_fusion")
    if fusion not in {None, b"reconstructed_fusion.v1"} or any(
        b"observed_union" in metadata.get(key, b"")
        for key in (b"narrowgate.schema", b"narrowgate.book_union")
    ):
        raise ValueError("observed/unknown orderbook union requires its source-aware adapter")
    return fusion == b"reconstructed_fusion.v1"


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
        recorder_snapshot_clock: str = "original",
    ) -> None:
        if tick_size <= 0.0:
            raise ValueError("tick_size must be positive")
        if warmup_hours < 0:
            raise ValueError("warmup_hours must be non-negative")
        if continuation_hours < 0:
            raise ValueError("continuation_hours must be non-negative")
        if recorder_snapshot_clock not in {"original", "preceding_update_id"}:
            raise ValueError("unsupported recorder snapshot clock")
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
        missing = tuple(path for _, path in expected if not raw_hour_available(path))
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
        self.recorder_snapshot_clock = recorder_snapshot_clock
        self._snapshot_sequence_anchors = 0
        self._snapshot_anchor_context = (
            self.raw_root / self.exchange
            / (process_start - timedelta(hours=1)).strftime("%Y-%m-%d/%H")
            / f"{self.symbol}_orderbook.parquet.zst"
        )

    @property
    def source_paths(self) -> tuple[Path, ...]:
        return tuple(path for _, path in self._expected if raw_hour_available(path))

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
        preceding_update = None
        self._snapshot_sequence_anchors = 0
        for hour_index, (hour, path) in enumerate(self._expected):
            if not raw_hour_available(path):
                preceding_update = None
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
                if (self.recorder_snapshot_clock == "preceding_update_id"
                        and event.event_type == "snapshot"):
                    if ordinal == 0 and hour_index == 0 and raw_hour_available(self._snapshot_anchor_context):
                        # Input rotation may begin at an archive snapshot. Read
                        # only the prior hour's terminal message, never fabricate
                        # an anchor from the next (future) update.
                        from data.build_active_order_queue_tape import iter_cryptohft_logical_messages
                        terminal = None
                        for message in iter_cryptohft_logical_messages(
                            self._snapshot_anchor_context, self.tick_size, include_levels=False,
                        ):
                            terminal = message
                        if terminal is not None and terminal.event_type != "snapshot":
                            preceding_update = (terminal.final_update_id, terminal.exchange_ts_ms)
                    anchor = recorder_snapshot_anchor_ms(
                        event_type=event.event_type,
                        event_time_ms=_optional_ms(event.event_time_ns),
                        transaction_time_ms=_optional_ms(event.transaction_time_ns),
                        snapshot_update_id=(event.last_update_id if event.last_update_id is not None
                                            else event.final_update_id),
                        preceding_update_id=preceding_update[0] if preceding_update else None,
                        preceding_update_time_ms=preceding_update[1] if preceding_update else None,
                    )
                    if anchor is not None:
                        event = replace(event, exchange_ts_ns=anchor * 1_000_000,
                                        exchange_ts_source="preceding_update_sequence_anchor")
                        self._snapshot_sequence_anchors += 1
                exchange_ns = int(event.exchange_ts_ns)
                if exchange_ns < last_exchange_ns:
                    raise ValueError(
                        "native CryptoHFT exchange time regressed across raw "
                        f"messages: {exchange_ns} < {last_exchange_ns} ({path})"
                    )
                last_exchange_ns = exchange_ns
                preceding_update = ((event.final_update_id, event.exchange_ts_ms)
                                    if event.event_type != "snapshot" else None)
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
            "snapshot_sequence_anchors": self._snapshot_sequence_anchors,
        }

    def identity(self, *, include_sha256: bool = True) -> dict[str, object]:
        files = []
        digests = {}
        for path in self.source_paths:
            storage = raw_hour_storage_path(path)
            row: dict[str, object] = {
                "path": str(path),
                "storage_path": str(storage),
                "size_bytes": int(storage.stat().st_size),
                "mtime_ns": int(storage.stat().st_mtime_ns),
            }
            if include_sha256:
                if storage not in digests:
                    digest = hashlib.sha256()
                    with storage.open("rb") as handle:
                        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                            digest.update(chunk)
                    digests[storage] = digest.hexdigest()
                row["sha256"] = digests[storage]
            files.append(row)
        return {
            "schema_version": "native_exchange_book_tape.v1",
            "recorder_snapshot_clock": self.recorder_snapshot_clock,
            "snapshot_anchor_context": (
                {"path": str(self._snapshot_anchor_context),
                 "storage_path": str(raw_hour_storage_path(self._snapshot_anchor_context)),
                 "size_bytes": raw_hour_storage_path(self._snapshot_anchor_context).stat().st_size,
                 "sha256": hashlib.sha256(raw_hour_storage_path(self._snapshot_anchor_context).read_bytes()).hexdigest()
                 if include_sha256 else None}
                if self.recorder_snapshot_clock == "preceding_update_id"
                and raw_hour_available(self._snapshot_anchor_context) else None
            ),
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


class TardisExchangeBookTape:
    """Stream raw provider L2 messages without inventing exchange sequence IDs.

    Contiguous provider groups are applied atomically across Arrow batches.
    Provider time identifies groups, never our receipt or latency. The
    explicit provider mode exists only for historical diagnostic reproduction.
    Files must be supplied in chronological order, with real snapshot context.
    """

    def __init__(self, paths: Iterable[Path], *, symbol: str, tick_size: float,
                 clock_mode: str = "exchange"):
        self.paths = tuple(Path(path).expanduser().resolve() for path in paths)
        self.symbol = str(symbol).upper()
        self.tick_size = float(tick_size)
        if clock_mode not in {"exchange", "provider"}:
            raise ValueError("unsupported Tardis clock mode")
        self.clock_mode = clock_mode
        if self.clock_mode == "exchange" and any(path.suffix != ".parquet" for path in self.paths) and any(path.suffix == ".parquet" for path in self.paths):
            raise ValueError("do not mix purchased Tardis source facts with legacy Parquet tapes")
        if not self.paths or not math.isfinite(self.tick_size) or self.tick_size <= 0:
            raise ValueError("Tardis tape requires source files and a positive finite tick size")
        for path in self.paths:
            if not path.is_file():
                raise FileNotFoundError(path)

    def __iter__(self) -> Iterator[HistoricalExchangeBookEvent]:
        from data.normalize_tardis_orderbook import _open_csv

        if self.clock_mode == "exchange" and all(path.suffix != ".parquet" for path in self.paths):
            yield from self._source_fact_events()
            return

        ordinal = 0
        previous_exchange = previous_receive = 0
        for path in self.paths:
            import pyarrow.parquet as pq
            metadata = (pq.ParquetFile(path).metadata.metadata or {}) if path.suffix == ".parquet" else {}
            if _observed_union_metadata(metadata):
                for item in self._observed_union_events(path):
                    if item.exchange_ts_ns // 1000 < previous_exchange:
                        raise ValueError("union source presentation clock regressed across files")
                    previous_exchange = item.exchange_ts_ns // 1000
                    yield replace(item, source_ordinal=ordinal)
                    ordinal += 1
                continue
            fused = (path.suffix == ".parquet" and
                     _reconstructed_book_metadata(metadata))
            message = None
            levels: list[tuple[str, int, float]] = []

            def event(message, levels, path, ordinal, *, fused=fused):
                exchange_us, receive_us, snapshot, source_id, reason, observed_us = message
                event_class = ReconstructedExchangeBookEvent if fused else HistoricalExchangeBookEvent
                extra = ({"fusion_reason": reason, "source_id": source_id,
                          "source_observed_ts_ns": observed_us * 1000} if fused else {})
                return event_class(
                    market_id=f"binance_futures:perpetual:{self.symbol}",
                    event_type="snapshot" if snapshot else "delta",
                    exchange_ts_ns=exchange_us * 1000,
                    exchange_ts_source="unknown" if fused else "event",
                    event_time_ns=(observed_us if fused else exchange_us) * 1000,
                    local_receive_ts_ns=receive_us * 1000 if self.clock_mode == "provider" or fused else 0,
                    levels=tuple(levels), source=str(path), source_ordinal=ordinal,
                    sequence_scope="provider_ordered", **extra,
                )

            with _open_csv(path) as reader:
                for batch in reader:
                    columns = [batch.column(name).to_numpy(zero_copy_only=False) for name in (
                        "exchange", "symbol", "timestamp", "local_timestamp",
                        "is_snapshot", "side", "price", "amount",
                    )]
                    annotations = []
                    annotations_bound = fused and {
                        "source_id", "fusion_reason", "source_observed_timestamp_us"
                    } <= set(batch.schema.names)
                    for name, default in (("source_id", "unknown"),
                                          ("fusion_reason", "unknown_reconstruction"),
                                          ("source_observed_timestamp_us", 0)):
                        annotations.append(batch.column(name).to_pylist()
                                           if annotations_bound else [default] * len(batch))
                    for row, annotation in zip(zip(*columns, strict=True), zip(*annotations, strict=True), strict=True):
                        exchange, symbol, exchange_us, receive_us, snapshot, side, price, amount = row
                        source_id, reason, observed_us = annotation
                        exchange_us = int(exchange_us)
                        receive_us = (int(receive_us) if receive_us is not None
                                      and math.isfinite(float(receive_us)) else 0)
                        price, amount = float(price), float(amount)
                        if (str(exchange) != "binance-futures" or str(symbol) != self.symbol
                                or str(side) not in {"bid", "ask"}
                                or not math.isfinite(price) or price <= 0
                                or not math.isfinite(amount) or amount < 0):
                            raise ValueError(f"invalid Tardis L2 row in {path}")
                        if fused and (not isinstance(source_id, str) or not source_id
                                      or not isinstance(reason, str) or observed_us is None):
                            raise ValueError(f"invalid reconstructed source annotation in {path}")
                        observed_us = int(observed_us)
                        if fused and not 0 <= observed_us <= exchange_us:
                            raise ValueError(f"reconstructed source observation is invalid/future in {path}")
                        key = (exchange_us, receive_us, bool(snapshot), source_id, reason, observed_us)
                        if key != message:
                            if exchange_us < previous_exchange or (self.clock_mode == "provider" and not fused and receive_us < previous_receive):
                                raise ValueError(f"Tardis source clock regressed in {path}")
                            if exchange_us <= 0 or (self.clock_mode == "provider" and not fused and receive_us < exchange_us):
                                raise ValueError(f"Tardis provider receive precedes exchange time in {path}")
                            if message is not None:
                                yield event(message, levels, path, ordinal)
                                ordinal += 1
                            message, levels = key, []
                            previous_exchange, previous_receive = exchange_us, receive_us
                        price_tick = round(price / self.tick_size)
                        if not math.isclose(price_tick * self.tick_size, price,
                                            rel_tol=0., abs_tol=max(1e-9, self.tick_size * 1e-7)):
                            raise ValueError(f"Tardis price is not tick aligned in {path}")
                        levels.append((str(side), price_tick, amount))
            if message is not None:
                yield event(message, levels, path, ordinal)
                ordinal += 1

    def _source_fact_events(self):
        """Compatibility schedule from source time, explicitly not exact E/T.

        Facts preserve regressions; this legacy monotone scheduler refuses them.
        A new modeled regression schedule must be a separately bound scenario.
        """
        from decimal import Decimal
        from data.tardis_input import SourceFragment, SourceQuality, iter_book_messages

        self.source_quality = SourceQuality()
        fragments = [SourceFragment(path, str(path)) for path in self.paths]
        tick = Decimal(str(self.tick_size))
        previous = None
        for message in iter_book_messages(fragments, symbol=self.symbol, quality=self.source_quality):
            if previous is not None and message.source_timestamp_us < previous:
                raise ValueError("Tardis source clock regressed; see source_quality; no reordering")
            previous = message.source_timestamp_us
            levels = []
            for side, price, quantity in message.levels:
                price_tick = price / tick
                if price_tick != price_tick.to_integral_value():
                    raise ValueError("Tardis price is not tick aligned")
                levels.append((side, int(price_tick), float(quantity)))
            yield HistoricalExchangeBookEvent(
                market_id=message.market_id, event_type=message.kind,
                exchange_ts_ns=message.source_timestamp_us * 1000,
                exchange_ts_source="unknown", local_receive_ts_ns=0,
                levels=tuple(levels), source=str(message.source_file_id),
                source_ordinal=message.source_ordinal, sequence_scope="provider_ordered")

    def _observed_union_events(self, path: Path) -> Iterator[ObservedUnionExchangeBookEvent]:
        from data.normalize_tardis_orderbook import _open_csv
        import pyarrow.parquet as pq

        metadata = pq.ParquetFile(path).metadata.metadata or {}
        unified = metadata.get(b"narrowgate.book_fusion") == b"narrowgate.daily_book.v1"
        receipt = json.loads(metadata.get(b"narrowgate.book_receipt" if unified else b"narrowgate.fusion_receipt", b"{}"))
        priority = receipt.get("stream_priority", receipt.get("stats", {}).get("stream_priority"))
        if priority is None:
            seed = receipt.get("initial_continuation")
            if seed is not None:
                priority = book_stream_priority(seed["source_ids"], preferred_index=seed["kernel"]["preferred"],
                                                contract=seed.get("stream_priority"))
        priority = book_stream_priority(contract=priority)
        ranks = book_stream_ranks(priority)
        slots = {identity: slot for slot, identity in enumerate(priority["stream_ids"])}

        required = ("exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot",
                    "side", "price", "amount", "source_id", "fusion_reason",
                    "source_native_sequence", "source_observed_timestamp_us", "source_timestamp_us",
                    "event_time", "transaction_time", "first_update_id", "final_update_id",
                    "prev_final_update_id", "last_update_id")
        if unified:
            required = tuple({"source_id": "stream_id", "source_native_sequence": "native_sequence",
                              "source_observed_timestamp_us": "observed_timestamp_us",
                              "source_timestamp_us": "original_timestamp_us"}.get(name, name)
                             for name in required if name != "fusion_reason") + ("stream_priority", "queue_rebase", "observation_only")
        pending_ts = None
        pending: dict[tuple, tuple[dict, list]] = {}
        first_message = True

        def messages():
            nonlocal first_message
            # Native fragments can cross Arrow batches or interleave with the
            # other source at the same presentation clock. Sort only complete
            # native messages by their original IDs, as the source normalizer.
            ordered = list(pending.values())
            ordered.sort(key=lambda item: (
                slots[item[0]["source_id"]],
                (item[0]["last_update_id"] if item[0]["event_type"] == "snapshot"
                 and item[0]["last_update_id"] is not None else item[0]["final_update_id"])
                if item[0]["source_native_sequence"] else 0,
                -int(item[0]["event_type"] == "snapshot") if item[0]["source_native_sequence"] else 0,
            ))
            for payload, levels in ordered:
                if unified and first_message:
                    from data.daily_raw import daily_book_continuation
                    payload.update(stream_contract=priority,
                                   day_initial_state=daily_book_continuation(receipt.get("initial_state")))
                    first_message = False
                yield ObservedUnionExchangeBookEvent(**payload, levels=tuple(levels))

        with _open_csv(path) as reader:
            for batch in reader:
                if unified:
                    import pyarrow.compute as pc
                    if "top_only" not in batch.schema.names or batch["top_only"].null_count:
                        raise ValueError("daily book top-only declaration is missing")
                    batch = batch.filter(pc.invert(batch["top_only"]))
                    if not len(batch):
                        continue
                if set(required) - set(batch.schema.names):
                    raise ValueError("observed union source schema requires its original source fields")
                columns = [batch.column(name).to_pylist() for name in required]
                for values in zip(*columns, strict=True):
                    r = dict(zip(required, values, strict=True))
                    if unified:
                        r.update(source_id=r.pop("stream_id"), source_native_sequence=r.pop("native_sequence"),
                                 source_observed_timestamp_us=r.pop("observed_timestamp_us"),
                                 source_timestamp_us=r.pop("original_timestamp_us"), fusion_reason="source_observation")
                        if (type(r["queue_rebase"]) is not bool or type(r["observation_only"]) is not bool
                                or r["stream_priority"] != ranks.get(r["source_id"])):
                            raise ValueError("invalid daily book event semantics/priority")
                    if (r["exchange"] != "binance-futures" or r["symbol"] != self.symbol
                            or r["fusion_reason"] != "source_observation"
                            or type(r["source_native_sequence"]) is not bool
                            or type(r["is_snapshot"]) is not bool
                            or not isinstance(r["source_id"], str) or not r["source_id"]):
                        raise ValueError("invalid observed union source identity")
                    if r["source_id"] not in ranks:
                        raise ValueError("union stream is not declared in its priority contract")
                    presentation = int(r["timestamp"])
                    observed, original = int(r["source_observed_timestamp_us"]), int(r["source_timestamp_us"] or 0)
                    if not 0 < observed <= presentation or (original <= 0 and (not unified or r["source_native_sequence"])):
                        raise ValueError("union source observation is invalid or in the future")
                    if pending_ts is not None and presentation != pending_ts:
                        if presentation < pending_ts:
                            raise ValueError("union source presentation clock regressed")
                        yield from messages()
                        pending.clear()
                    pending_ts = presentation
                    native, snapshot = r["source_native_sequence"], r["is_snapshot"]
                    ids = tuple(None if r[name] is None else int(r[name]) for name in (
                        "first_update_id", "final_update_id", "prev_final_update_id", "last_update_id"))
                    if native and (ids[3] if snapshot and ids[3] is not None else ids[1]) is None:
                        raise ValueError("native union message lacks update identity")
                    local = int(r["local_timestamp"] or 0)
                    event_ns, transaction_ns = int(r["event_time"] or 0) * 1_000_000, int(r["transaction_time"] or 0) * 1_000_000
                    if native and (event_ns or transaction_ns) != observed * 1000:
                        raise ValueError("native union E/T and declared observation clock disagree")
                    key = ((r["source_id"], observed, True, ids[3] if ids[3] is not None else ids[1])
                           if native and snapshot else
                           (r["source_id"], observed, False, *ids, transaction_ns) if native else
                           (r["source_id"], observed, local, snapshot, r.get("queue_rebase", False), r.get("observation_only", False)))
                    if key not in pending:
                        payload = dict(market_id=f"binance_futures:perpetual:{self.symbol}",
                            event_type="snapshot" if snapshot else "delta", exchange_ts_ns=presentation * 1000,
                            exchange_ts_source="unknown", event_time_ns=event_ns, transaction_time_ns=transaction_ns,
                            local_receive_ts_ns=max(0, local) * 1000, source=str(path),
                            sequence_scope="exchange_sequence" if native else "provider_ordered",
                            first_update_id=ids[0] if native else None, final_update_id=ids[1] if native else None,
                            previous_final_update_id=ids[2] if native else None, last_update_id=ids[3] if native else None,
                            source_id=r["source_id"], source_native_sequence=native,
                            stream_priority=ranks[r["source_id"]],
                            queue_rebase=r.get("queue_rebase", False), observation_only=r.get("observation_only", False),
                            source_observed_ts_ns=observed * 1000, source_timestamp_ns=original * 1000)
                        pending[key] = (payload, [])
                    else:
                        pending[key][0]["local_receive_ts_ns"] = max(pending[key][0]["local_receive_ts_ns"], max(0, local) * 1000)
                    price, quantity = float(r["price"]), float(r["amount"])
                    if not math.isfinite(price) or price <= 0 or not math.isfinite(quantity) or quantity < 0:
                        raise ValueError("invalid observed union price/quantity")
                    tick = round(price / self.tick_size)
                    if not math.isclose(tick * self.tick_size, price, rel_tol=0., abs_tol=max(1e-9, self.tick_size * 1e-7)):
                        raise ValueError("observed union price is not tick aligned")
                    pending[key][1].append((str(r["side"]), tick, quantity))
        yield from messages()

    @property
    def initial_continuation(self) -> Mapping[str, object] | None:
        import pyarrow.parquet as pq

        path = self.paths[0]
        if path.suffix != ".parquet":
            return None
        metadata = pq.ParquetFile(path).metadata.metadata or {}
        if not _observed_union_metadata(metadata):
            return None
        if metadata.get(b"narrowgate.book_fusion") == b"narrowgate.daily_book.v1":
            from data.daily_raw import daily_book_continuation, daily_book_receipt
            seed = daily_book_continuation(daily_book_receipt(path).get("initial_state"))
        else:
            receipt = json.loads(metadata.get(b"narrowgate.fusion_receipt", b"{}"))
            seed = receipt.get("initial_continuation")
        if seed is None:
            return None  # Never substitute stats.continuation (the day's end).
        day = metadata.get(b"narrowgate.day", b"").decode()
        expected = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) * 1_000_000
        if seed.get("next_day_start_us") != expected or seed.get("symbol") != self.symbol:
            raise ValueError("union initial continuation is not this UTC day's starting state")
        return seed

    def identity(self) -> dict[str, object]:
        import pyarrow.parquet as pq

        metadata = [(pq.ParquetFile(path).metadata.metadata or {}) if path.suffix == ".parquet" else {}
                    for path in self.paths]
        union = any(_observed_union_metadata(item) for item in metadata)
        reconstructed = any(_reconstructed_book_metadata(item) for item in metadata
                            if not _observed_union_metadata(item))
        return {
            "source": "observed_source_union" if union else
                      "reconstructed_book_state" if reconstructed else "tardis_incremental_book_L2",
            "sequence_scope": "provider_ordered",
            "exchange_sequence_available": False, "provider_receive_is_live_receive": False,
            "clock_mode": self.clock_mode,
            "reader_contract": "tardis_source_facts.v1" if self.clock_mode == "exchange" else "legacy_provider_grouping.v1",
            "timestamp_mapping_evidence": "unknown",
            "schedule_time_origin": "source_timestamp_compatibility_not_verified_exchange_state",
            "native_observation_parity": False,
            **({"native_delta_authority": False,
                "reconstruction_changes_are_exchange_cancellations": False} if reconstructed else {}),
            **({"global_exchange_sequence_available": False,
                "source_aware_state_selection": True, "synthetic_switch_deltas": False} if union else {}),
            "symbol": self.symbol, "tick_size": self.tick_size,
            "files": [{"path": str(path), "size_bytes": path.stat().st_size} for path in self.paths],
        }


class PlannedExchangeBookTape:
    """One explicitly selected raw source per UTC day, not automatic fallback."""

    def __init__(self, plan: Mapping[str, object], *, days: list[str], symbol: str,
                 tick_size: float):
        if plan.get("symbol") != symbol:
            raise ValueError("exchange-book source plan symbol mismatch")
        rows = plan.get("days", [])
        if not rows:
            from data_paths import tardis_raw_root, tardis_market_path
            if tardis_raw_root() is not None:
                rows = [{"day": day, "provider": "tardis", "clock_mode": "exchange",
                         "raw_file": str(tardis_market_path(day, symbol, "incremental_book_L2"))}
                        for day in days]
            else:
                rows = [{"day": day, "provider": "daily",
                         "raw_file": str(daily_market_path(day, symbol, "incremental_book_L2"))}
                        for day in days]
        entries = {row["day"]: row for row in rows}
        if len(entries) != len(rows):
            raise ValueError("exchange-book source plan has duplicate dates")
        self.tapes = []
        self.tick_size = float(tick_size)
        self.symbol = str(symbol).upper()
        self.handovers = []
        self.days = list(days)
        for day in days:
            if day not in entries:
                raise ValueError(f"exchange-book source plan lacks requested context day {day}")
            row = entries[day]
            provider = row.get("provider", "daily")
            if provider == "daily":
                import pyarrow.parquet as pq
                raw_file = Path(row.get("raw_file") or daily_market_path(day, symbol, "incremental_book_L2"))
                footer = pq.ParquetFile(raw_file)
                native = False
                metadata = footer.metadata.metadata or {}
                union = _observed_union_metadata(metadata)
                fused = False if union else _reconstructed_book_metadata(metadata)
                # Retained native IDs in a reconstructed fusion are provenance,
                # not the identity of its synthetic global-state differences.
                for name in (() if fused or union else ("last_update_id", "final_update_id")):
                    column = footer.schema_arrow.get_field_index(name)
                    if column < 0:
                        continue
                    for batch in footer.iter_batches(columns=[name], batch_size=1):
                        native = batch.column(0).null_count < batch.num_rows
                        break
                    if native:
                        break
                if native:
                    provider = "cryptohft"
                    row = {**row, "raw_root": str(raw_file.parents[3]),
                           "recorder_snapshot_clock": row.get("recorder_snapshot_clock", "preceding_update_id")}
                else:
                    provider = "tardis"
                    row = {**row, "raw_file": str(raw_file)}
            handover = row.get("handover", "snapshot_required")
            if handover not in {"snapshot_required", "invalidate_then_delta_bootstrap"}:
                raise ValueError("unsupported exchange-book handover")
            if handover == "invalidate_then_delta_bootstrap" and provider != "cryptohft":
                raise ValueError("delta bootstrap handover requires an exchange-sequenced source")
            self.handovers.append(handover)
            if provider == "cryptohft":
                tape = CryptoHFTExchangeBookTape(raw_root=Path(row["raw_root"]),
                    day=day, symbol=symbol, tick_size=tick_size, warmup_hours=0,
                    strict_complete=True,
                    cache_enabled=bool(row.get("cache_enabled", True)),
                    recorder_snapshot_clock=row.get("recorder_snapshot_clock", "original"))
            elif provider == "tardis":
                tape = TardisExchangeBookTape([Path(row["raw_file"])],
                    symbol=symbol, tick_size=tick_size,
                    clock_mode=row.get("clock_mode", plan.get("clock_mode", "exchange")))
            else:
                raise ValueError(f"unsupported exchange-book provider {row['provider']!r}")
            self.tapes.append(tape)

    def __iter__(self) -> Iterator[HistoricalExchangeBookEvent]:
        if not self.tapes:
            return
        ordinal = 0
        current = iter(self.tapes[0])
        following = None
        event = next(current, None)
        try:
            for index in range(len(self.tapes)):
                if event is None:
                    raise ValueError("selected daily exchange-book source is empty")
                if self.handovers[index] == "invalidate_then_delta_bootstrap":
                    # Explicit uncertainty boundary, not an invented snapshot.
                    # Keep it in this file's cursor even when an input batch
                    # starts here without the preceding provider's file.
                    yield replace(event, event_type="source_gap", levels=(),
                                  first_update_id=None, final_update_id=None,
                                  previous_final_update_id=None, last_update_id=None,
                                  source_ordinal=ordinal)
                    ordinal += 1
                next_first = None
                if index + 1 < len(self.tapes):
                    following = iter(self.tapes[index + 1])
                    next_first = next(following, None)
                    if next_first is None:
                        raise ValueError("selected daily exchange-book source is empty")
                    if (type(self.tapes[index]) is not type(self.tapes[index + 1])
                            and next_first.event_type != "snapshot"
                            and self.handovers[index + 1] == "snapshot_required"):
                        raise ValueError("daily source handover requires an actual opening snapshot")
                    if next_first.exchange_ts_ns <= event.exchange_ts_ns:
                        raise ValueError("daily source handover must advance exchange time")
                # A file's opening snapshot can precede midnight. Hand over
                # at that actual source time, not after the old file's later,
                # overlapping tail. Never rewrite clocks or replay both tails.
                while event is not None:
                    if (next_first is not None
                            and (next_first.event_type == "snapshot"
                                 or self.handovers[index + 1] == "invalidate_then_delta_bootstrap")
                            and event.exchange_ts_ns >= next_first.exchange_ts_ns):
                        break
                    yield replace(event, source_ordinal=ordinal)
                    ordinal += 1
                    event = next(current, None)
                current.close()
                current, following, event = following, None, next_first
        finally:
            if current is not None:
                current.close()
            if following is not None:
                following.close()

    def identity(self) -> dict[str, object]:
        return {"source": "explicit_daily_source_plan", "days": self.days,
                "sources": [tape.identity(include_sha256=False) if isinstance(tape, CryptoHFTExchangeBookTape)
                            else tape.identity() for tape in self.tapes],
                "automatic_source_fallback": False,
                "handovers": dict(zip(self.days, self.handovers, strict=True)),
                "daily_handover": "opening_snapshot_excludes_old_overlap_otherwise_preserve_deltas"}

    @property
    def initial_continuation(self) -> Mapping[str, object] | None:
        return self.tapes[0].initial_continuation if self.tapes and isinstance(self.tapes[0], TardisExchangeBookTape) else None


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
        union_minimum_levels: int = 20,
        initial_continuation: Mapping[str, object] | None = None,
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
        self._source_read_count = 0
        self._last_read_event: HistoricalExchangeBookEvent | None = None
        self._last_source_file = ""
        self._source_file_read_count = 0
        self._accepted = 0
        self._rejected = 0
        self._snapshot_events = 0
        self._delta_events = 0
        self._source_gap_events = 0
        self._provider_ordered_events = 0
        self._reconstructed_events = 0
        self._last_source_observed_ts_ns = 0
        if union_minimum_levels <= 0:
            raise ValueError("union minimum valid depth must be positive")
        self._union_minimum_levels = int(union_minimum_levels)
        self._union_sources: dict[str, _UnionSourceState] = {}
        self._union_stream_ranks: dict[str, int] = {}
        self._union_selected: str | None = None
        self._union_view_attached = False
        self._union_seed_start_ns = 0
        self._union_tick_size = events.tick_size if isinstance(events, (TardisExchangeBookTape, PlannedExchangeBookTape)) else None
        self._union_symbol = events.symbol if isinstance(events, (TardisExchangeBookTape, PlannedExchangeBookTape)) else None
        self._sequence_scope = "exchange_sequence"
        self._timestamp_source_counts = {
            "preceding_update_sequence_anchor": 0,
            "transaction": 0,
            "event": 0,
            "receive": 0,
            "unknown": 0,
        }
        if initial_continuation is None and isinstance(events, (TardisExchangeBookTape, PlannedExchangeBookTape)):
            initial_continuation = events.initial_continuation
        if initial_continuation is not None:
            self._restore_union_initial(initial_continuation)
        self._push_next()

    def checkpoint(self) -> dict[str, object]:
        """Detach book/sequence/lookahead state without retaining the input tape.

        This is one component of a replay checkpoint, not strategy or account
        state. Persist the returned object graph together (for example in a
        trusted local pickle) so ``sequence.book`` keeps its shared identity.
        The caller saves its source cursor after ``source_read_count`` reads;
        prefetched events are already included here and must not be replayed
        from that cursor. No future input is consumed while saving.
        """
        return {
            "schema": "exchange_book_scheduler.v1",
            "state": deepcopy({key: value for key, value in vars(self).items()
                               if key != "_iterator"}),
        }

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Mapping[str, object],
        remaining_events: Iterable[HistoricalExchangeBookEvent],
    ) -> HistoricalExchangeBookScheduler:
        """Resume from the unread source tail, retaining buffered same-time events.

        At an exhausted input-batch boundary, ``remaining_events`` is simply
        the next batch. Otherwise it starts after the saved source read count,
        not after the consumed-event count. The iterator is supplied by the
        caller rather than serialized with open file handles or the old tape.
        """
        if checkpoint.get("schema") != "exchange_book_scheduler.v1":
            raise ValueError("unsupported exchange-book scheduler checkpoint")
        state = deepcopy(checkpoint["state"])
        if not isinstance(state, dict) or "_iterator" in state:
            raise ValueError("invalid exchange-book scheduler checkpoint state")
        restored = cls.__new__(cls)
        # Existing v1 checkpoints predate reconstruction-aware event input.
        state.setdefault("_reconstructed_events", 0)
        state.setdefault("_last_source_observed_ts_ns", 0)
        for key, value in {
            "_union_minimum_levels": 20, "_union_sources": {},
            "_union_selected": None, "_union_view_attached": False,
            "_union_seed_start_ns": 0, "_union_tick_size": None,
            "_union_symbol": None,
        }.items():
            state.setdefault(key, value)
        if "_union_stream_ranks" not in state:
            legacy_ranks = book_stream_ranks(book_stream_priority())
            state["_union_stream_ranks"] = {identity: legacy_ranks[identity]
                                             for identity in state["_union_sources"]}
        restored.__dict__.update(state)
        if restored.sequence.book is not restored.book:
            raise ValueError("checkpoint lost shared sequence/book identity")
        restored._iterator = iter(remaining_events)
        if restored._next_event is None:
            restored._push_next()
        return restored

    @property
    def source_read_count(self) -> int:
        """Source cursor including lookahead, distinct from applied events."""
        return self._source_read_count

    def _strict_at(self, exchange_ts_ns: int) -> bool:
        return bool(
            self._strict_sequence
            and int(exchange_ts_ns) >= self._strict_after_ns
        )

    def resume_input_source(self, events: Iterable[HistoricalExchangeBookEvent]) -> None:
        """Rebind a new overlapping raw-file window after its saved read cursor.

        The saved book, pending same-time group and lookahead are untouched.
        Source files are read from their beginning, so a within-file ordinal
        distinguishes even byte-identical duplicate messages. Full source-file
        prefixes must be supplied; a sliced arbitrary iterator is insufficient.
        """
        marker = self._last_read_event
        iterator = iter(events)
        if marker is None:
            self._iterator = iterator
            return
        if not marker.source:
            raise ValueError("native input rotation requires a source-file cursor")
        wanted_file = Path(marker.source).name
        within_file = 0
        current_file = None
        for event in iterator:
            source_file = event.source
            if source_file != current_file:
                current_file, within_file = source_file, 0
            within_file += 1
            if Path(source_file).name != wanted_file or within_file != self._source_file_read_count:
                continue
            # Providers reuse the same basename in every date/hour directory.
            # An earlier file's matching ordinal is not the saved source cursor.
            if int(event.exchange_ts_ns) < int(marker.exchange_ts_ns):
                continue
            if replace(event, source=marker.source, source_ordinal=marker.source_ordinal) != marker:
                raise ValueError("native input rotation changed the saved source message")
            ordinal_offset = int(marker.source_ordinal) - int(event.source_ordinal)
            self._last_source_file = event.source
            self._iterator = (
                replace(item, source_ordinal=int(item.source_ordinal) + ordinal_offset)
                for item in iterator
            )
            return
        raise ValueError("native input window does not contain the saved source-file cursor")

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
        self._source_read_count += 1
        if event.source != self._last_source_file:
            self._last_source_file = event.source
            self._source_file_read_count = 0
        self._source_file_read_count += 1
        self._last_read_event = event
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
                or (isinstance(event, ReconstructedExchangeBookEvent) and event.state_rebase)
                or (isinstance(event, ObservedUnionExchangeBookEvent) and event.queue_rebase)
                or (isinstance(event, ObservedUnionExchangeBookEvent) and event.stream_contract is not None
                    and book_stream_ranks(event.stream_contract) != self._union_stream_ranks)
                for event in events
            ),
        )

    def _invalidate_local_state(self) -> None:
        self.book.reset()
        self.snapshot_ranges = {"bid": None, "ask": None}
        self.known_ticks = {"bid": set(), "ask": set()}
        self.segment_id = 0
        self._last_mid_tick = None
        self._last_source_observed_ts_ns = 0

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

    def _new_union_source(self) -> _UnionSourceState:
        child = HistoricalExchangeBookScheduler((), strict_sequence=False,
            union_minimum_levels=self._union_minimum_levels)
        child.book = _UnionSourceBook()
        child.sequence.book = child.book
        return _UnionSourceState(child)

    @staticmethod
    def _book_view(book: OrderBookState) -> OrderBookState:
        view = OrderBookState()
        view.bid_levels, view.ask_levels = book.bid_levels, book.ask_levels
        view.bid_heap, view.ask_heap = book.bid_heap, book.ask_heap
        return view

    def _valid_union_source(self, source: _UnionSourceState) -> bool:
        book = source.scheduler.book
        if (not source.scheduler.sequence.initialized or source.observed_ns <= 0
                or len(book.bid_levels) < self._union_minimum_levels
                or len(book.ask_levels) < self._union_minimum_levels):
            return False
        bids, asks = book.top_levels(1)
        return bool(bids and asks and bids[0][0] < asks[0][0])

    def _restore_union_initial(self, seed: Mapping[str, object]) -> None:
        """Consume only the explicitly supplied day-start state, never a terminal footer."""
        if self._strict_sequence:
            raise ValueError("observed union cannot prove strict global exchange sequence continuity")
        start = int(seed["next_day_start_us"]) * 1000
        kernel = seed["kernel"]
        identities = seed["source_ids"]
        priority = book_stream_priority(identities, preferred_index=kernel["preferred"],
                                        contract=seed.get("stream_priority"))
        if (kernel.get("schema") not in {"book_fusion.continuation.v1", "book_fusion.continuation.v2"}
                or start <= 0 or start % (86_400 * 1_000_000_000)
                or kernel["minimum_levels"] != self._union_minimum_levels
                or len(identities) != len(set(identities)) or len(identities) != len(kernel["sources"])
                or self._union_tick_size is None or not math.isfinite(self._union_tick_size)
                or self._union_tick_size <= 0 or seed.get("symbol") != self._union_symbol):
            raise ValueError("invalid union initial continuation configuration")

        def load_book(rows):
            book = _UnionSourceBook()
            for row in rows:
                if (len(row) != 17 or row[0] not in {0, 1} or row[1] <= 0 or row[2] <= 0
                        or row[3] * 1000 > start or row[4] * 1000 > start):
                    raise ValueError("invalid initial union source level")
                price = row[1] / 1e8
                tick = round(price / self._union_tick_size)
                if not math.isclose(tick * self._union_tick_size, price, rel_tol=0., abs_tol=1e-8):
                    raise ValueError("initial union source level is not tick aligned")
                side = "bid" if row[0] == 0 else "ask"
                if float(tick) in (book.bid_levels if side == "bid" else book.ask_levels):
                    raise ValueError("duplicate initial union source level")
                book.apply(side, float(tick), row[2] / 1e8)
            return book

        sources = {}
        for identity, value in zip(identities, kernel["sources"], strict=True):
            source = self._new_union_source()
            source.observed_ns = int(value["observed_us"]) * 1000
            source.last_presentation_ns = max(0, int(value["presentation_us"])) * 1000
            if not 0 <= source.observed_ns <= start or source.last_presentation_ns > start:
                raise ValueError("initial union continuation contains future source state")
            last_message = value["last_message"]
            if len(last_message) != 14:
                raise ValueError("initial union source message shape differs")
            source.native = bool(last_message[4]) if source.observed_ns else None
            source.local_ns = max(0, int(value["local_us"])) * 1000
            child = source.scheduler
            child.book = load_book(value["levels"])
            child.sequence.book = child.book
            child.sequence.initialized = bool(value["initialized"])
            child.sequence.initialization_source = "snapshot" if value["initialized"] else None
            child.sequence.initialization_ts_ms = source.observed_ns // 1_000_000 if source.observed_ns else None
            child.sequence.bridge_pending = bool(value["bridge"])
            child.sequence.last_update_id = int(value["last_id"]) if int(value["last_id"]) >= 0 else None
            if int(value["last_snapshot_id"]) >= 0:
                child.sequence.last_snapshot_key = (int(value["last_snapshot_e"]) // 1000, int(value["last_snapshot_id"]))
            child._sequence_scope = "exchange_sequence" if source.native else "provider_ordered"
            child._last_exchange_ts_ns = source.last_presentation_ns
            sources[str(identity)] = source
        global_observed = int(kernel["global_observed_us"]) * 1000
        if not 0 <= global_observed <= start or int(kernel["presentation_us"]) * 1000 > start:
            raise ValueError("initial union continuation contains future selected state")
        selected = int(kernel["selected_source"])
        if selected < -1 or selected >= len(identities):
            raise ValueError("initial union selected source is invalid")
        book = load_book(kernel["global_levels"])
        self._union_sources = sources
        self._union_stream_ranks = book_stream_ranks(priority)
        self._union_seed_start_ns = start
        self._union_selected = str(identities[selected]) if selected >= 0 else None
        self.book = book
        view_source = int(kernel.get("view_source", -1))
        if view_source < -1 or view_source >= len(identities):
            raise ValueError("initial union source view is invalid")
        if view_source >= 0:
            if view_source != selected:
                raise ValueError("initial union selected/source view differs")
            selected_book = sources[str(identities[view_source])].scheduler.book
            if book.bid_levels != selected_book.bid_levels or book.ask_levels != selected_book.ask_levels:
                raise ValueError("initial union source and selected view disagree")
            self.book = self._book_view(selected_book)
            self._union_view_attached = True
        self.sequence.book = self.book
        self.sequence.initialized = bool(self._union_selected is not None and global_observed > 0
                                         and self._union_view_attached
                                         and self._valid_union_source(sources[self._union_selected]))
        self.sequence.initialization_source = "snapshot" if self.sequence.initialized else None
        self._sequence_scope = "provider_ordered"
        self._last_source_observed_ts_ns = global_observed
        # The footer is an explicitly available day-start checkpoint, not
        # evidence that this selected state existed at every earlier instant.
        self._last_exchange_ts_ns = start
        self._last_boundary_ns = start
        if self.sequence.initialized:
            self._segment_count += 1
            self.segment_id = self._segment_count

    def _process_union_group(self, events, *, emitted_levels=None):
        """Apply independent original messages, selecting one coherent state once."""
        if self._strict_sequence:
            raise ValueError("observed union cannot prove strict global exchange sequence continuity")
        timestamp = events[0].exchange_ts_ns
        if any(e.exchange_ts_ns != timestamp for e in events):
            raise ValueError("union atomic source group has mixed presentation clocks")
        boundary_changed = False
        for event in events:
            if event.stream_contract is None:
                continue
            ranks = book_stream_ranks(event.stream_contract)
            if ranks == self._union_stream_ranks:
                continue
            boundary_changed = True
            # A daily stream-set change is explicit metadata, not an exchange
            # cancellation. Keep the prior aged global view until the new
            # day-start seed or coherent snapshot can take over.
            self._union_sources = {}
            self._union_selected, self._union_view_attached = None, False
            self._union_stream_ranks = ranks
            self.sequence.initialized = False
            self.segment_id = 0
            self.snapshot_ranges = {"bid": None, "ask": None}
            self.known_ticks = {"bid": set(), "ask": set()}
            if event.day_initial_state is not None:
                self._restore_union_initial(event.day_initial_state)
        if self._union_seed_start_ns and timestamp // 86_400_000_000_000 * 86_400_000_000_000 < self._union_seed_start_ns:
            raise ValueError("union initial continuation starts after source input")
        if self._latest_batch_ts_ns != timestamp:
            self._latest_batch_ts_ns = timestamp
            self._latest_batch_prior_asof_ns = self._last_exchange_ts_ns
            self._latest_batch_prior_segment_id = self.segment_id
            self._latest_batch_prior_initialized = self.sequence.initialized
            self._latest_batch_touched_levels.clear()
            self._latest_batch_discontinuous = False
        if boundary_changed:
            self._latest_batch_discontinuous = True
        old_selected, old_attached = self._union_selected, self._union_view_attached
        undo, updates, resets, accepted_by_source = {}, {}, set(), {}
        accepted = 0
        for event in events:
            rank = event.stream_priority
            if rank is None:
                legacy_ranks = book_stream_ranks(book_stream_priority())
                if event.source_id not in legacy_ranks:
                    raise ValueError("anonymous union stream requires explicit priority")
                rank = legacy_ranks[event.source_id]
            if event.source_id in self._union_stream_ranks:
                if self._union_stream_ranks[event.source_id] != rank:
                    raise ValueError("union stream priority changed")
            elif rank in self._union_stream_ranks.values():
                raise ValueError("union stream priority must be unique")
            self._union_stream_ranks[event.source_id] = rank
            self._latest_batch_touched_levels.update((s, int(p)) for s, p, _ in event.levels)
            if event.source_id not in self._union_sources:
                self._union_sources[event.source_id] = self._new_union_source()
            source = self._union_sources[event.source_id]
            if source.native is not None and source.native != event.source_native_sequence:
                raise ValueError("union source changed sequence authority")
            source.native = event.source_native_sequence
            child = source.scheduler
            if not source.native and event.source_observed_ts_ns < source.observed_ns:
                continue
            if source.native and event.event_type == "snapshot":
                identifier = event.last_update_id if event.last_update_id is not None else event.final_update_id
                if child.sequence.initialized and child.sequence.last_update_id is not None and identifier < child.sequence.last_update_id:
                    continue
            # Only touched old quantities are saved. Snapshot/gap reset swaps
            # child containers; the outer view then keeps the old containers.
            if old_attached and event.source_id == old_selected:
                for side, tick, _ in event.levels:
                    child_levels = child.book.bid_levels if side == "bid" else child.book.ask_levels
                    view_levels = self.book.bid_levels if side == "bid" else self.book.ask_levels
                    if child_levels is view_levels:
                        undo.setdefault((side, tick), view_levels.get(float(tick)))
            plain = HistoricalExchangeBookEvent(**{f.name: getattr(event, f.name) for f in fields(HistoricalExchangeBookEvent)})
            step = child.apply_scheduled_events([plain], boundary_ts_ns=timestamp,
                                               emitted_levels=emitted_levels)
            accepted += step.accepted_events
            if step.accepted_events:
                source.observed_ns = max(source.observed_ns, event.source_observed_ts_ns)
                source.last_presentation_ns = timestamp
                source.local_ns = event.local_receive_ts_ns
                accepted_by_source.setdefault(event.source_id, []).append(event)
                if not event.observation_only:
                    updates.setdefault(event.source_id, []).extend(step.level_changes)
                if step.snapshot_reset or event.queue_rebase:
                    resets.add(event.source_id)
        candidates = [(value.observed_ns, self._union_stream_ranks[identity], identity, value)
                      for identity, value in self._union_sources.items() if self._valid_union_source(value)]
        selected = max(candidates, default=None, key=lambda item: item[:3])
        if (selected is None or selected[0] < self._last_source_observed_ts_ns
                or (selected[2] == old_selected and selected[3].last_presentation_ns < timestamp)):
            # Invalid selected updates may have touched shared containers.
            # Materialize an aged copy only on this exceptional path.
            if undo:
                aged = OrderBookState()
                aged.bid_levels, aged.ask_levels = self.book.bid_levels.copy(), self.book.ask_levels.copy()
                for (side, tick), quantity in undo.items():
                    levels = aged.bid_levels if side == "bid" else aged.ask_levels
                    if quantity is None:
                        levels.pop(float(tick), None)
                    else:
                        levels[float(tick)] = quantity
                aged._rebuild("bid")
                aged._rebuild("ask")
                self.book = aged
                self.sequence.book = aged
            if old_selected is not None:
                child_book = self._union_sources[old_selected].scheduler.book
                self._union_view_attached = self.book.bid_levels is child_book.bid_levels and self.book.ask_levels is child_book.ask_levels
            invalidated = bool(self.sequence.initialized and old_selected is not None
                               and not self._valid_union_source(self._union_sources[old_selected]))
            if invalidated:
                # The last coherent prices remain useful with their original
                # age, but the broken source cannot support queue continuity.
                # Do not clear the carried book or manufacture depletion.
                self.sequence.initialized = False
                self.segment_id = 0
                self.snapshot_ranges = {"bid": None, "ask": None}
                self.known_ticks = {"bid": set(), "ask": set()}
                self._latest_batch_discontinuous = True
                self._last_mid_tick = None
            return (), False, invalidated, accepted
        _, _, identity, source = selected
        rebase = (identity != old_selected or not old_attached or identity in resets)
        self.book = self._book_view(source.scheduler.book)
        self.sequence.book = self.book
        self.sequence.initialized = True
        self.sequence.initialization_source = "snapshot"
        self.sequence.last_update_id = None
        self._sequence_scope = "provider_ordered"
        self._union_selected, self._union_view_attached = identity, True
        self._last_source_observed_ts_ns = source.observed_ns
        self._last_local_receive_ts_ns = max(self._last_local_receive_ts_ns, source.local_ns)
        self.snapshot_ranges = {"bid": None, "ask": None}
        if rebase:
            self._segment_count += 1
            self.segment_id = self._segment_count
            self.known_ticks = {"bid": set(), "ask": set()}
            self._latest_batch_discontinuous = True
            self._snapshot_events += 1
            return (), True, False, accepted
        changes = tuple(replace(change, segment_id=self.segment_id) for change in updates.get(identity, ()))
        for event in accepted_by_source.get(identity, ()):
            self.known_ticks["bid"].update(int(p) for s, p, _ in event.levels if s == "bid")
            self.known_ticks["ask"].update(int(p) for s, p, _ in event.levels if s == "ask")
        self._delta_events += len(accepted_by_source.get(identity, ()))
        return changes, False, False, accepted

    def _consume_union_events(self, events, *, emitted_levels=None):
        changes, reset, invalid, accepted = self._process_union_group(events, emitted_levels=emitted_levels)
        count = len(events)
        self._consumed += count
        self._accepted += accepted
        self._rejected += count - accepted
        self._provider_ordered_events += count
        self._timestamp_source_counts["unknown"] += count
        self._last_exchange_ts_ns = max(self._last_exchange_ts_ns, events[-1].exchange_ts_ns)
        if accepted:
            self._record_mid_change(events[-1].exchange_ts_ns)
        return changes, reset, invalid, accepted, count - accepted

    def _process_event(
        self,
        event: HistoricalExchangeBookEvent,
        *,
        emitted_levels: set[tuple[str, int]] | None = None,
    ) -> tuple[tuple[ExchangeBookLevelChange, ...], bool, bool, bool]:
        event_ts_ns = int(event.exchange_ts_ns)
        reconstructed = isinstance(event, ReconstructedExchangeBookEvent)
        state_rebase = reconstructed and event.state_rebase
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
        } or state_rebase
        self._last_local_receive_ts_ns = max(
            self._last_local_receive_ts_ns,
            int(event.local_receive_ts_ns or 0),
        )
        if event.event_type == "source_gap":
            was_initialized = bool(self.sequence.initialized)
            self.sequence.invalidate_source_gap()
            self._invalidate_local_state()
            self._sequence_scope = event.sequence_scope
            self._source_gap_events += 1
            if self._strict_at(event.exchange_ts_ns):
                raise ValueError(
                    f"native exchange-book source gap at {event.exchange_ts_ns}"
                )
            return (), False, was_initialized, False

        was_initialized = bool(self.sequence.initialized)
        previous_gap_count = int(self.sequence.stats.sequence_gaps)
        if event.sequence_scope == "provider_ordered":
            if self._strict_sequence:
                raise ValueError("provider-ordered L2 cannot prove strict exchange sequence continuity")
            self._provider_ordered_events += 1
            if event.event_type == "snapshot":
                self.sequence.initialized = True
                self.sequence.initialization_source = "snapshot"
                self.sequence.initialization_ts_ms = event.exchange_ts_ms
                self.sequence.bridge_pending = False
                self.sequence.last_update_id = None
                self.sequence.current_message_key = None
                self._sequence_scope = "provider_ordered"
                apply_message = True
            else:
                if was_initialized and self._sequence_scope != "provider_ordered":
                    raise ValueError("changing book providers requires an actual source snapshot")
                apply_message = was_initialized and self._sequence_scope == "provider_ordered"
        elif self._sequence_scope == "provider_ordered" and event.event_type != "snapshot":
            raise ValueError("changing book providers requires an actual source snapshot")
        else:
            if event.event_type == "snapshot":
                self._sequence_scope = "exchange_sequence"
            apply_message = self.sequence.begin_message(
                event_type=event.event_type,
                receive_time_ms=_optional_ms(event.local_receive_ts_ns),
                event_time_ms=(event.exchange_ts_ms
                               if event.exchange_ts_source == "preceding_update_sequence_anchor"
                               else _optional_ms(event.event_time_ns)),
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

        if reconstructed:
            self._reconstructed_events += 1
            self._last_source_observed_ts_ns = event.source_observed_ts_ns

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

        if event.event_type == "snapshot" or state_rebase:
            self._segment_count += 1
            self.segment_id = self._segment_count
            self.known_ticks = {"bid": set(), "ask": set()}
            self.snapshot_ranges = {"bid": None, "ask": None}
            if not reconstructed:
                for side, tick, _ in event.levels:
                    self.known_ticks[side].add(int(tick))
            for side, levels in (
                ("bid", self.book.bid_levels),
                ("ask", self.book.ask_levels),
            ):
                ticks = [int(price) for price in levels]
                if reconstructed:
                    # A rebase proves selected positive state, not exchange
                    # cancellation or the absence of depth outside its source.
                    self.known_ticks[side].update(ticks)
                else:
                    self.snapshot_ranges[side] = (
                        (min(ticks), max(ticks)) if ticks else None
                    )
            self._snapshot_events += 1
            return (), True, False, True

        if reconstructed and event.fusion_reason == "observation_refresh":
            # This synthetic level repeats state only to carry a real source
            # observation clock. It must never advance a queue or flow count.
            return (), False, False, True

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
            if isinstance(self._next_event, ObservedUnionExchangeBookEvent):
                group = []
                while (isinstance(self._next_event, ObservedUnionExchangeBookEvent)
                       and self._next_event.exchange_ts_ns == event_ts):
                    group.append(self._next_event)
                    self._push_next()
                source_events.extend(group)
                group_changes, reset, invalid, accepted, rejected = self._consume_union_events(
                    group, emitted_levels=emitted_levels)
                changes.extend(group_changes)
                accepted_now += accepted
                rejected_now += rejected
                snapshot_reset |= reset
                invalidated |= invalid
                continue
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
        index = 0
        while index < len(source_events):
            event = source_events[index]
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
            if isinstance(event, ObservedUnionExchangeBookEvent):
                end = index + 1
                while (end < len(source_events) and isinstance(source_events[end], ObservedUnionExchangeBookEvent)
                       and source_events[end].exchange_ts_ns == event_ts):
                    end += 1
                group_changes, reset, invalid, accepted, rejected = self._consume_union_events(
                    source_events[index:end], emitted_levels=emitted_levels)
                changes.extend(group_changes)
                accepted_now += accepted
                rejected_now += rejected
                snapshot_reset |= reset
                invalidated |= invalid
                index = end
                continue
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
            index += 1
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
        if self._union_seed_start_ns and target <= self._union_seed_start_ns:
            return replace(lookup, status="unknown", reason="strict_before_initial_continuation",
                           quantity=None)
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
        source_sequence_stats = [source.scheduler.sequence.stats for source in self._union_sources.values()]
        def sequence_count(field):
            return int(getattr(sequence_stats, field)) + sum(int(getattr(value, field)) for value in source_sequence_stats)
        return ExchangeBookSchedulerStats(
            consumed_events=int(self._consumed),
            accepted_events=int(self._accepted),
            rejected_events=int(self._rejected),
            snapshot_events=int(self._snapshot_events),
            delta_events=int(self._delta_events),
            delta_bootstrap_events=sequence_count("delta_bootstrap_messages"),
            source_gap_events=int(self._source_gap_events),
            sequence_gaps=sequence_count("sequence_gaps"),
            invalid_sequence_messages=sequence_count("invalid_sequence_messages"),
            message_time_reversals=sequence_count("message_time_reversals"),
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
            provider_ordered_events=int(self._provider_ordered_events),
            sequence_anchored_snapshot_events=int(
                self._timestamp_source_counts.get("preceding_update_sequence_anchor", 0)
            ),
        )

    @property
    def evidence_scope(self) -> str:
        if self._union_sources:
            return "source_observation_union_selected_state_not_global_native_sequence_v1"
        if self._reconstructed_events:
            return "strategy_independent_reconstructed_state_not_native_delta_evidence_v1"
        return ("strategy_independent_provider_ordered_l2_exchange_time_v1"
                if self._provider_ordered_events else
                "strategy_independent_native_snapshot_delta_exchange_time_v1")

    @property
    def last_source_observed_ts_ns(self) -> int:
        """Retained real observation, distinct from a carried presentation."""
        return self._last_source_observed_ts_ns

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
        initial_ready_by_connection=None,
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
        initial = dict(initial_ready_by_connection or {})
        if (set(initial) - set(connection_keys) or any(
            isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, np.integer))
            or not 0 <= int(value) <= np.iinfo(np.int64).max for value in initial.values()
        )):
            raise ValueError("initial connection completion must be an aligned nonnegative ns clock")
        for connection in connection_keys:
            indices = np.flatnonzero(connections == connection)
            previous_ready = int(initial.get(connection, 0))
            if not serialize_callback_service:
                assigned[indices] = np.maximum(np.maximum.accumulate(proposed[indices]), previous_ready)
                continue
            service = proposed[indices] - receive[indices]
            cumulative = np.cumsum(service, dtype=np.int64)
            if np.any(cumulative < 0) or np.any(cumulative[1:] < cumulative[:-1]):
                raise ValueError("callback service cumulative duration exceeds int64")
            prior = cumulative - service
            # Max-plus FIFO recurrence, vectorized without rounding nanoseconds:
            # finish_i = cumulative_i + max_j<=i(receive_j - cumulative_(j-1)).
            origin = np.maximum(np.maximum.accumulate(receive[indices] - prior), previous_ready)
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


_CPP_FIXED_DURATION_RE = re.compile(r"FIXED_(\d+)S")
_CPP_BUY_E3_SOURCE_RE = re.compile(
    r"^(?:tri|value)::mid_usdc_per_btc__h"
    r"(?P<fast>\d+(?:p\d+)?)s__h(?P<slow>\d+(?:p\d+)?)s::"
    r"(?P<metric>[a-z_]+)$"
)
_CPP_BUY_E3_DIRECT_CAMPAIGN_AGE = (
    "predicate::m0::campaign_age_gt_control_duration"
)


def _compile_cpp_boolean_cooldown_policy(cpp, policy, *, declarative: bool):
    """Compile one loaded policy object into the generic native rule ABI."""

    evaluator = policy.evaluator
    columns = tuple(str(value) for value in evaluator.predicate_columns)
    if not columns or tuple(sorted(columns)) != columns or len(set(columns)) != len(columns):
        raise ValueError("cooldown_cpp_predicate_columns_invalid")
    policy_sha256 = str(evaluator.policy_sha256).lower()
    predicate_sha256 = str(evaluator.predicate_bundle_sha256).lower()
    if (
        re.fullmatch(r"[0-9a-f]{64}", policy_sha256) is None
        or re.fullmatch(r"[0-9a-f]{64}", predicate_sha256) is None
    ):
        raise ValueError("cooldown_cpp_policy_identity_invalid")

    compiled = cpp.F05BooleanPolicy()
    compiled.policy_sha256 = policy_sha256
    compiled.predicate_bundle_sha256 = predicate_sha256
    compiled.predicate_columns = list(columns)
    compiled.default_action = "CONTROL_85N"
    column_index = {name: index for index, name in enumerate(columns)}
    compiled_rules = []
    for action_id, raw_clauses in evaluator.rules:
        match = _CPP_FIXED_DURATION_RE.fullmatch(str(action_id))
        if match is None:
            raise ValueError("cooldown_cpp_rule_action_invalid")
        rule = cpp.F05BooleanRule()
        rule.action_id = str(action_id)
        rule.duration_ms = int(match.group(1)) * 1_000
        clauses = []
        for raw_clause in raw_clauses:
            clause = cpp.F05BooleanClause()
            literals = []
            for name, negated in raw_clause:
                if str(name) not in column_index:
                    raise ValueError("cooldown_cpp_rule_predicate_unbound")
                literal = cpp.F05BooleanLiteral()
                literal.predicate_index = column_index[str(name)]
                literal.negated = bool(negated)
                literals.append(literal)
            if not literals:
                raise ValueError("cooldown_cpp_rule_clause_empty")
            clause.literals = literals
            clauses.append(clause)
        if not clauses:
            raise ValueError("cooldown_cpp_rule_clauses_empty")
        rule.clauses = clauses
        compiled_rules.append(rule)
    compiled.rules = compiled_rules
    if not declarative:
        return compiled

    half_lives = tuple(float(value) for value in policy.ema_half_lives_s)
    pairs = tuple(
        (float(fast), float(slow)) for fast, slow in policy.ema_pairs_s
    )
    if (
        not half_lives
        or tuple(sorted(half_lives)) != half_lives
        or len(set(half_lives)) != len(half_lives)
        or any(not math.isfinite(value) or value <= 0.0 for value in half_lives)
    ):
        raise ValueError("cooldown_cpp_ema_half_lives_invalid")
    half_life_index = {value: index for index, value in enumerate(half_lives)}
    cpp_pairs = []
    pair_index: dict[tuple[float, float], int] = {}
    for fast, slow in pairs:
        if fast not in half_life_index or slow not in half_life_index or fast >= slow:
            raise ValueError("cooldown_cpp_ema_pair_invalid")
        if (fast, slow) in pair_index:
            raise ValueError("cooldown_cpp_ema_pair_duplicate")
        pair_index[(fast, slow)] = len(cpp_pairs)
        pair = cpp.F05PredicatePair()
        pair.fast_ema_index = half_life_index[fast]
        pair.slow_ema_index = half_life_index[slow]
        cpp_pairs.append(pair)
    compiled.ema_half_lives_s = list(half_lives)
    compiled.predicate_pairs = cpp_pairs

    metric_by_name = {
        "positive_ordering": cpp.F05PredicateMetric.POSITIVE_ORDERING,
        "last_cross_positive": cpp.F05PredicateMetric.LAST_CROSS_POSITIVE,
        "expanding": cpp.F05PredicateMetric.EXPANDING,
        "converging": cpp.F05PredicateMetric.CONVERGING,
        "abs_distance": cpp.F05PredicateMetric.ABS_DISTANCE,
        "cross_age_s": cpp.F05PredicateMetric.CROSS_AGE_S,
        "arrangement_persistence_s": (
            cpp.F05PredicateMetric.ARRANGEMENT_PERSISTENCE_S
        ),
        "signed_distance": cpp.F05PredicateMetric.SIGNED_DISTANCE,
        "signed_distance_velocity": (
            cpp.F05PredicateMetric.SIGNED_DISTANCE_VELOCITY
        ),
        "signed_distance_acceleration": (
            cpp.F05PredicateMetric.SIGNED_DISTANCE_ACCELERATION
        ),
    }
    raw_definitions = {
        str(name): value for name, value in policy.definitions.items()
    }
    direct = frozenset(str(name) for name in policy.direct_predicates)
    if direct - {_CPP_BUY_E3_DIRECT_CAMPAIGN_AGE}:
        raise ValueError("cooldown_cpp_direct_predicate_unsupported")
    definitions = []
    for index, name in enumerate(columns):
        definition = cpp.F05PredicateDefinition()
        definition.predicate_index = index
        if name in direct:
            definition.metric = cpp.F05PredicateMetric.CAMPAIGN_AGE_GT_CONTROL
            definitions.append(definition)
            continue
        raw = raw_definitions.get(name)
        if not isinstance(raw, Mapping):
            raise ValueError("cooldown_cpp_predicate_definition_missing")
        source = str(raw.get("source_field", ""))
        match = _CPP_BUY_E3_SOURCE_RE.fullmatch(source)
        if match is None:
            raise ValueError("cooldown_cpp_predicate_source_unsupported")
        fast = float(match.group("fast").replace("p", "."))
        slow = float(match.group("slow").replace("p", "."))
        if (fast, slow) not in pair_index:
            raise ValueError("cooldown_cpp_predicate_pair_unbound")
        metric = metric_by_name.get(match.group("metric"))
        if metric is None:
            raise ValueError("cooldown_cpp_predicate_metric_unsupported")
        definition.metric = metric
        definition.pair_index = pair_index[(fast, slow)]
        kind = str(raw.get("kind", ""))
        if kind == "preserved_tri":
            definition.threshold_enabled = False
        elif kind == "quantile_ge":
            threshold = float(raw.get("threshold"))
            if not math.isfinite(threshold):
                raise ValueError("cooldown_cpp_predicate_threshold_invalid")
            definition.threshold_enabled = True
            definition.threshold = threshold
        else:
            raise ValueError("cooldown_cpp_predicate_kind_unsupported")
        definitions.append(definition)
    if set(raw_definitions) | set(direct) != set(columns):
        raise ValueError("cooldown_cpp_predicate_definition_set_drifted")
    compiled.predicate_definitions = definitions
    return compiled


def build_configured_cooldown_policy_adapter(*, window, params):
    """Load fresh per-arm live policy state from the selected replay config.

    Artifact hashes come from that config, not a historical research freeze.
    The caller must supply the same already-scheduled depth callbacks used by
    the execution replay; this function never resamples transport or reads a
    mutable live selector. Missing source channels cannot become zero features.
    """
    enabled = {
        "SELL": params.get("boolean_cooldown_policy_enabled", False),
        "BUY": params.get("buy_e3_cooldown_policy_enabled", False),
    }
    if any(type(value) is not bool for value in enabled.values()):
        raise ValueError("configured cooldown enabled flags must be boolean")
    if not any(enabled.values()):
        return None
    depth = (
        window.get("l2_data") if isinstance(window, Mapping) else getattr(window, "l2_data", None)
    )
    if depth is None or not len(getattr(depth, "ts_ms", ())):
        raise ValueError("configured cooldown policy requires retained warmup/target depth")
    for field in ("bid_px", "ask_px", "bid_qty", "ask_qty"):
        values = np.asarray(getattr(depth, field, ()))
        if values.ndim != 2 or values.shape[0] != len(depth.ts_ms) or values.shape[1] < 1:
            raise ValueError(f"configured cooldown policy missing aligned depth channel: {field}")
    deliveries = params.get("_exec_message_delivery")
    delivery = deliveries.get("depth") if isinstance(deliveries, Mapping) else None
    clocks = ("exchange_ts_ns", "receive_ts_ns", "feature_ready_ts_ns")
    if not isinstance(delivery, Mapping) or any(key not in delivery for key in clocks):
        raise ValueError("configured cooldown policy requires explicit depth message delivery")
    schedule = HistoricalMessageDeliverySchedule(*(delivery[key] for key in clocks))
    max_age = float(params.get("max_exec_book_visible_age_s", 0.0))
    if not math.isfinite(max_age) or max_age <= 0.0:
        raise ValueError("configured cooldown policy feature-age limit must be positive")

    from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy
    from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy

    root = Path(__file__).resolve().parents[1]

    def artifact_path(key):
        value = params.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"configured cooldown policy missing artifact path: {key}")
        path = resolve_portable_path(value, root=root)
        return path if path.is_absolute() else root / path

    policies = {}
    for side, prefix, loader in (
        ("SELL", "boolean_cooldown", LiveBooleanCooldownPolicy),
        ("BUY", "buy_e3_cooldown", LiveBuyE3CooldownPolicy),
    ):
        if not enabled[side]:
            continue
        kwargs = {
            "policy_path": artifact_path(f"{prefix}_policy_path"),
            "policy_sha256": params.get(f"{prefix}_policy_sha256", ""),
            "predicate_bundle_path": artifact_path(f"{prefix}_predicate_bundle_path"),
            "predicate_bundle_sha256": params.get(f"{prefix}_predicate_bundle_sha256", ""),
            "warmup_s": params.get(f"{prefix}_ema_warmup_s", 0.0),
            "max_feature_age_s": max_age,
        }
        if side == "BUY":
            kwargs.update(
                artifact_manifest_path=artifact_path("buy_e3_cooldown_artifact_manifest_path"),
                artifact_manifest_sha256=params.get("buy_e3_cooldown_artifact_manifest_sha256", ""),
                expected_artifact_sha256=params.get("buy_e3_cooldown_artifact_sha256", ""),
            )
        policies[side] = loader.from_files(**kwargs)
    return ReceiveTimeCooldownReplayAdapter(depth, schedule, policies=policies)


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
        raw_ready = delivery_schedule.ready_ns_for_channel(channel)
        raw_receive = delivery_schedule.receive_ns_for_channel(channel)
        # A depth connection cannot deliver later sequence messages before an
        # earlier one. Sampled marginal receive delays may otherwise regress.
        self._receive = np.maximum.accumulate(raw_receive)
        self._receive_clamps = int(np.count_nonzero(self._receive != raw_receive))
        # This adapter's current contract has one callback-entry clock. Keep
        # capture visibility on the same source-order-clamped boundary rather
        # than searching the schedule's unclamped/raw receive sequence.
        self._ready = np.maximum.accumulate(
            np.maximum(np.asarray(raw_ready, dtype=np.int64), self._receive)
        )
        self._ready_post_clamps = int(
            np.count_nonzero(self._ready != np.asarray(raw_ready))
        )
        self._ready_schedule_clamps = int(
            delivery_schedule.stats_dict().get("head_of_line_clamped_events", 0)
        )
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
        self._source_row_offset = 0
        self._last_cutoff = -1
        self._captures = 0
        self._fallbacks = 0
        self._evaluations = 0
        self._cpp_window_arrays_cache = None

    def resume_input_window(self, fresh, *, before_ts_ns=None):
        """Rebind depth inputs without replacing policy, EMA or pending windows.

        Keep every not-yet-delivered callback. Lazy callbacks strictly before
        the next replay event can be folded into the saved EMA before dropping
        their input rows; no policy decision or fill is manufactured. The caller
        supplies already-continued receive/ready clocks.
        """
        if type(fresh) is not type(self) or self.cpp_policy_bindings != fresh.cpp_policy_bindings:
            raise ValueError("cooldown input rotation changed the configured policy")
        old, new = self._depth.ts_ms, fresh._depth.ts_ms
        offset = int(np.searchsorted(old, new[0], side="left"))
        count = min(len(old) - offset, len(new))
        if count <= 0 or not np.array_equal(old[offset:offset + count], new[:count]):
            raise ValueError("cooldown depth windows need unchanged overlapping timestamps")
        if offset > self._cursor and (
            before_ts_ns is None or self._ready[offset - 1] >= int(before_ts_ns)
        ):
            raise ValueError("cooldown input window discarded undelivered depth callbacks")
        for name in ("bid_px", "ask_px", "bid_qty", "ask_qty"):
            if not np.array_equal(getattr(self._depth, name)[offset:offset + count],
                                  getattr(fresh._depth, name)[:count], equal_nan=True):
                raise ValueError("cooldown input rotation changed overlapping depth values")
        for name in ("_receive", "_ready"):
            if not np.array_equal(getattr(self, name)[offset:offset + count],
                                  getattr(fresh, name)[:count]):
                raise ValueError("cooldown input rotation changed message delivery clocks")
        if offset > self._cursor:
            self._consume_depth_callbacks(offset)
        self._cursor -= offset
        self._source_row_offset += offset
        self._depth = fresh._depth
        self._receive, self._ready = fresh._receive, fresh._ready
        self._cpp_window_arrays_cache = None

    @property
    def cpp_policy_bindings(self) -> Mapping[str, Mapping[str, str]]:
        """Return the two side-specific identities consumed by native replay."""

        bindings = {}
        for side, policy in self._policies.items():
            evaluator = policy.evaluator
            binding = {
                "policy_sha256": str(evaluator.policy_sha256),
                "predicate_bundle_sha256": str(
                    evaluator.predicate_bundle_sha256
                ),
            }
            artifact_sha256 = str(getattr(evaluator, "artifact_sha256", "") or "")
            if artifact_sha256:
                binding["artifact_sha256"] = artifact_sha256
            bindings[side] = MappingProxyType(binding)
        return MappingProxyType(bindings)

    @property
    def cpp_runtime_binding_sha256(self) -> str:
        """Return one root identity for the compiled side-policy binding."""

        payload = {
            "schema": "current_receive_time_cooldown_cpp_binding",
            "feature_clock": "receive_time_full_mid_ema_bank_v1",
            "policies": {
                side: dict(binding)
                for side, binding in sorted(self.cpp_policy_bindings.items())
            },
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def cpp_window_arrays(self) -> Mapping[str, np.ndarray]:
        """Materialize the exact completed receive-time mid-window stream."""

        if self._cpp_window_arrays_cache is not None:
            return self._cpp_window_arrays_cache
        width = 100_000_000
        receive = np.asarray(self._receive, dtype=np.int64)
        if receive.ndim != 1 or receive.size < 2 or np.any(receive <= 0):
            raise ValueError("cooldown_cpp_receive_clock_incomplete")
        if not np.array_equal(np.asarray(self._ready, dtype=np.int64), receive):
            raise ValueError("cooldown_cpp_delivery_and_callback_clocks_differ")
        if np.any(receive[1:] < receive[:-1]):
            raise ValueError("cooldown_cpp_receive_clock_regressed")
        buckets = (receive // width) * width
        group_starts = np.concatenate(
            (
                np.asarray([0], dtype=np.int64),
                np.flatnonzero(buckets[1:] != buckets[:-1]).astype(np.int64)
                + 1,
            )
        )
        if group_starts.size < 2:
            raise ValueError("cooldown_cpp_no_completed_receive_time_window")
        pending_left = buckets[group_starts[:-1]]
        next_left = buckets[group_starts[1:]]
        gap_counts = (next_left - pending_left) // width - 1
        if np.any(gap_counts < 0):
            raise ValueError("cooldown_cpp_window_clock_regressed")
        max_feature_age_s = min(
            float(policy.windows.max_feature_age_s)
            for policy in self._policies.values()
        )
        reset_transition = (
            gap_counts * width > max_feature_age_s * 1_000_000_000.0
        )
        represented_gap_counts = np.where(reset_transition, 0, gap_counts)
        total = int(
            len(represented_gap_counts) + int(np.sum(represented_gap_counts))
        )
        left = np.empty(total, dtype=np.int64)
        right = np.empty(total, dtype=np.int64)
        ready = np.empty(total, dtype=np.int64)
        mid = np.full(total, np.nan, dtype=np.float64)
        source_gap = np.ones(total, dtype=np.uint8)
        reset_feature_state = np.zeros(total, dtype=np.uint8)
        prior_last = group_starts[1:] - 1
        bid = np.asarray(self._depth.bid_px)
        ask = np.asarray(self._depth.ask_px)
        if bid.ndim != 2 or ask.ndim != 2 or bid.shape[1] == 0 or ask.shape[1] == 0:
            raise ValueError("cooldown_cpp_depth_bbo_missing")
        prior_mid = (
            np.asarray(bid[prior_last, 0], dtype=np.float64)
            + np.asarray(ask[prior_last, 0], dtype=np.float64)
        ) / 2.0
        if np.any(~np.isfinite(prior_mid)) or np.any(prior_mid <= 0.0):
            raise ValueError("cooldown_cpp_depth_bbo_invalid")
        feature_ready = receive[group_starts[1:]]
        cursor = 0
        for index, gap_count in enumerate(represented_gap_counts):
            if reset_transition[index]:
                # The callback at next_left is the exact live reset boundary:
                # the old pending bucket is discarded and the new bucket stays
                # pending until a later callback completes it.
                left[cursor] = next_left[index]
                right[cursor] = next_left[index]
                ready[cursor] = feature_ready[index]
                source_gap[cursor] = 0
                reset_feature_state[cursor] = 1
                cursor += 1
                continue
            count = int(gap_count)
            left[cursor] = pending_left[index]
            right[cursor] = pending_left[index] + width
            ready[cursor] = feature_ready[index]
            mid[cursor] = prior_mid[index]
            source_gap[cursor] = 0
            cursor += 1
            if count:
                positions = slice(cursor, cursor + count)
                left[positions] = pending_left[index] + width * np.arange(
                    1, count + 1, dtype=np.int64
                )
                right[positions] = left[positions] + width
                ready[positions] = feature_ready[index]
                cursor += count
        assert cursor == total
        generation = np.arange(1, total + 1, dtype=np.int64)
        zeros = np.zeros(total, dtype=np.uint8)
        arrays = {
            "left_ts_ns": left,
            "right_ts_ns": right,
            "feature_ready_ts_ns": ready,
            "market_generation": generation,
            "depth_generation": generation,
            "mid_usdc_per_btc": mid,
            "reset_feature_state": reset_feature_state,
            "source_gap": source_gap,
            "source_stale": zeros.copy(),
            "warmup_admitted": zeros.copy(),
            "channel_support_valid": (
                (1 - source_gap) * (1 - reset_feature_state)
            ).astype(np.uint8),
        }
        for value in arrays.values():
            value.setflags(write=False)
        self._cpp_window_arrays_cache = MappingProxyType(arrays)
        return self._cpp_window_arrays_cache

    def compile_cpp_runtime(
        self,
        cpp,
        *,
        parity_qualified: bool = False,
        parity_qualification_sha256: str = "",
        qualification_under_test: bool = False,
    ):
        """Compile the loaded BUY/SELL policies into one native runtime.

        Compilation does not grant parity authority. A caller may set the
        qualification fields only when it also binds the matching external
        parity receipt through the replay parameters. ``qualification_under_test``
        admits only an explicitly paired, non-promotable qualification run and
        cannot coexist with a receipt.
        """

        if set(self._policies) != {"BUY", "SELL"}:
            raise ValueError("current_cpp_cooldown_requires_buy_and_sell_policies")
        qualification = str(parity_qualification_sha256).lower()
        if qualification_under_test and (parity_qualified or qualification):
            raise ValueError(
                "cooldown_cpp_qualification_under_test_conflicts_with_parity"
            )
        if bool(parity_qualified) != bool(qualification):
            raise ValueError("cooldown_cpp_parity_qualification_incomplete")
        if qualification and re.fullmatch(r"[0-9a-f]{64}", qualification) is None:
            raise ValueError("cooldown_cpp_parity_qualification_invalid")
        windows = [policy.windows for policy in self._policies.values()]
        warmup = {float(window.warmup_s) for window in windows}
        max_age = {float(window.max_feature_age_s) for window in windows}
        if len(warmup) != 1 or len(max_age) != 1:
            raise ValueError("cooldown_cpp_side_clock_contracts_differ")

        config = cpp.F05RepeatedBooleanCooldownConfig()
        config.parity_qualified = bool(parity_qualified)
        config.qualification_under_test = bool(qualification_under_test)
        config.parity_qualification_sha256 = qualification
        config.qualification_scope = "current_receive_time_full_replay_v1"
        config.feature_clock_semantics = "receive_time_full_mid_ema_bank_v1"
        config.warmup_s = warmup.pop()
        config.max_feature_age_s = max_age.pop()
        config.policy = _compile_cpp_boolean_cooldown_policy(
            cpp, self._policies["SELL"], declarative=False
        )
        config.buy_policy = _compile_cpp_boolean_cooldown_policy(
            cpp, self._policies["BUY"], declarative=True
        )
        runtime = cpp.F05RepeatedBooleanCooldownRuntime(config)
        if (
            bool(runtime.parity_qualified) != bool(parity_qualified)
            or bool(runtime.qualification_under_test)
            != bool(qualification_under_test)
        ):
            raise ValueError(
                "compiled current cooldown runtime rejected its policy binding: "
                + str(runtime.binding_error)
            )
        return runtime

    def _consume_depth_callbacks(self, last):
        for index in range(self._cursor, last):
            bids = list(zip(self._depth.bid_px[index], self._depth.bid_qty[index], strict=True))
            asks = list(zip(self._depth.ask_px[index], self._depth.ask_qty[index], strict=True))
            for observer in self._policies.values():
                observer.observe_depth(
                    receive_ts_ns=int(self._receive[index]), bids=bids, asks=asks,
                    market_generation=self._source_row_offset + index + 1,
                    depth_generation=self._source_row_offset + index + 1,
                )
        self._cursor = last

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
        if side not in {"BUY", "SELL"}:
            raise ValueError("receive-time policy fill side must be BUY or SELL")
        policy = self._policies.get(side)
        last = int(np.searchsorted(self._ready, cutoff, side="left"))
        # All callbacks are delivered, not just the latest book. A same-time
        # callback is withheld because its order relative to the fill is unknown.
        self._consume_depth_callbacks(last)
        self._last_cutoff = cutoff
        snapshot_id = f"{assignment_id}:receive-time-policy"
        raw = (
            policy.evaluate(
                side=side, baseline_duration_ms=int(round(context["baseline_duration_ms"])),
                campaign_age_s=float(context["campaign_age_s"]), decision_ts_ns=cutoff,
                snapshot_id=snapshot_id,
            )
            if policy is not None else _ReceiveTimeCooldownDecision(
                action_id="CONTROL_85N", duration_ms=float(context["baseline_duration_ms"]),
                fallback_reason="configured_policy_disabled_for_side", matched_rule_index=None,
                policy_sha256="", predicate_bundle_sha256="", snapshot_id=snapshot_id,
                support_valid=False,
            )
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
            "depth_rows_available": self._source_row_offset + len(self._ready),
            "depth_callbacks_consumed": self._source_row_offset + self._cursor,
            "receive_head_of_line_clamped_events": self._receive_clamps,
            "delivery_ready_head_of_line_clamped_events": (
                self._ready_schedule_clamps
            ),
            "adapter_ready_post_clamped_events": self._ready_post_clamps,
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
