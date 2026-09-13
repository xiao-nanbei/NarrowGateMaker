"""Synchronized Binance USD-M deep local order book.

The live strategy keeps its existing partial-depth stream for quote features.
This module owns a separate diff-depth stream and combines it with a public
REST snapshot so active-order queue features can query deeper price levels.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

logger = logging.getLogger("live.orderbook.binance_usdm")


@dataclass(frozen=True)
class DeepLevelState:
    valid: bool
    covered: bool
    generation: int
    side: str
    price: float
    quantity: float
    last_update_id: int
    receive_ts_ns: int
    age_ms: float
    decrease_events: int
    decrease_qty: float
    increase_events: int
    increase_qty: float
    trade_events: int
    trade_qty: float
    last_exchange_ts_ns: int


class BinanceUsdMDeepBook:
    """Maintain a bounded local book from snapshot plus diff-depth updates."""

    def __init__(
        self,
        rest_client: Any,
        *,
        symbol: str,
        tick_size: float,
        snapshot_levels: int = 1_000,
        max_buffer_events: int = 20_000,
        resync_backoff_s: float = 1.0,
        snapshot_loader: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        if not symbol:
            raise ValueError("deep-book symbol cannot be empty")
        if not math.isfinite(tick_size) or tick_size <= 0.0:
            raise ValueError("deep-book tick_size must be positive")
        if snapshot_levels not in {5, 10, 20, 50, 100, 500, 1_000}:
            raise ValueError("unsupported Binance USD-M depth snapshot limit")
        if max_buffer_events <= 0:
            raise ValueError("deep-book buffer must be positive")

        self._rest_client = rest_client
        self.symbol = str(symbol).upper()
        self.tick_size = float(tick_size)
        self.snapshot_levels = int(snapshot_levels)
        self.max_buffer_events = int(max_buffer_events)
        self.resync_backoff_s = max(0.05, float(resync_backoff_s))
        self._snapshot_loader = snapshot_loader

        self._lock = threading.RLock()
        self._running = False
        self._syncing = False
        self._valid = False
        self._generation = 0
        self._last_update_id = 0
        self._last_receive_ts_ns = 0
        self._last_exchange_ts_ns = 0
        self._last_trade_receive_ts_ns = 0
        self._last_trade_feature_ready_ts_ns = 0
        self._last_trade_exchange_ts_ns = 0
        self._trade_count = 0
        self._last_error = ""
        self._gap_count = 0
        self._resync_count = 0
        self._ignored_events = 0
        self._buffer_overflow_count = 0
        self._buffer: deque[tuple[dict[str, Any], int]] = deque()
        self._bids: dict[int, float] = {}
        self._asks: dict[int, float] = {}
        self._flow: dict[str, dict[int, list[float]]] = {
            "BUY": {},
            "SELL": {},
        }

    def start(self) -> None:
        with self._lock:
            self._running = True
            self._valid = False
            self._last_receive_ts_ns = 0
            self._last_trade_feature_ready_ts_ns = 0
            self._launch_sync_locked("startup")

    @contextmanager
    def atomic_read(self) -> Iterator["BinanceUsdMDeepBook"]:
        """Hold one immutable deep-book generation while callers snapshot it."""

        with self._lock:
            yield self

    def stop(self) -> None:
        with self._lock:
            self._running = False
            self._valid = False
            self._buffer.clear()

    def invalidate(self, reason: str, *, clear_buffer: bool = True) -> None:
        """Make deep state unusable until a fresh snapshot is installed."""
        with self._lock:
            self._valid = False
            self._last_error = str(reason)
            if clear_buffer:
                self._buffer.clear()

    def _price_tick(self, price: Any) -> int:
        value = float(price)
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError("invalid deep-book price")
        return int(round(value / self.tick_size))

    @staticmethod
    def _quantity(value: Any) -> float:
        quantity = float(value)
        if not math.isfinite(quantity) or quantity < 0.0:
            raise ValueError("invalid deep-book quantity")
        return quantity

    def _load_snapshot(self) -> Mapping[str, Any]:
        if self._snapshot_loader is not None:
            return self._snapshot_loader()
        return self._rest_client.depth(
            self.symbol,
            limit=self.snapshot_levels,
        )

    def _launch_sync_locked(self, reason: str) -> None:
        if self._syncing or not self._running:
            return
        self._syncing = True
        thread = threading.Thread(
            target=self._sync_loop,
            args=(str(reason),),
            daemon=True,
            name=f"deep-book-sync-{self.symbol.lower()}",
        )
        thread.start()

    def _sync_loop(self, reason: str) -> None:
        while True:
            with self._lock:
                if not self._running:
                    self._syncing = False
                    return
            try:
                payload = self._load_snapshot()
                snapshot_receive_ts_ns = time.time_ns()
                installed = self._install_snapshot(
                    payload,
                    snapshot_receive_ts_ns=snapshot_receive_ts_ns,
                )
                if installed:
                    logger.info(
                        "Deep book synchronized: symbol=%s generation=%d "
                        "last_update_id=%d levels=%d reason=%s",
                        self.symbol,
                        self._generation,
                        self._last_update_id,
                        self.snapshot_levels,
                        reason,
                    )
                    with self._lock:
                        self._syncing = False
                    return
            except Exception as exc:
                with self._lock:
                    self._last_error = str(exc)
                logger.warning(
                    "Deep book snapshot sync failed: symbol=%s reason=%s error=%s",
                    self.symbol,
                    reason,
                    exc,
                )
            time.sleep(self.resync_backoff_s)

    @staticmethod
    def _event_ids(event: Mapping[str, Any]) -> tuple[int, int, int]:
        return (
            int(event.get("U", 0) or 0),
            int(event.get("u", 0) or 0),
            int(event.get("pu", 0) or 0),
        )

    def _parse_snapshot_side(self, rows: Any) -> dict[int, float]:
        parsed: dict[int, float] = {}
        for raw in rows or ():
            if not isinstance(raw, (list, tuple)) or len(raw) < 2:
                continue
            price_tick = self._price_tick(raw[0])
            quantity = self._quantity(raw[1])
            if quantity > 0.0:
                parsed[price_tick] = quantity
        return parsed

    def _install_snapshot(
        self,
        payload: Mapping[str, Any],
        *,
        snapshot_receive_ts_ns: int,
    ) -> bool:
        last_update_id = int(payload.get("lastUpdateId", 0) or 0)
        if last_update_id <= 0:
            raise ValueError("deep-book snapshot lacks lastUpdateId")
        bids = self._parse_snapshot_side(payload.get("bids"))
        asks = self._parse_snapshot_side(payload.get("asks"))
        if not bids or not asks:
            raise ValueError("deep-book snapshot is empty")

        with self._lock:
            if not self._running:
                return False
            buffered = list(self._buffer)
            if len(buffered) > self.max_buffer_events:
                self._buffer_overflow_count += 1
                self._buffer.clear()
                self._last_error = "diff buffer overflow during snapshot sync"
                return False

            current_id = last_update_id
            last_receive_ts_ns = int(snapshot_receive_ts_ns)
            last_exchange_ts_ns = 0
            applied_any = False
            for event, receive_ts_ns in buffered:
                first_id, final_id, previous_id = self._event_ids(event)
                if final_id <= current_id:
                    continue
                if not applied_any:
                    bridges_snapshot = (
                        first_id <= current_id <= final_id
                        or first_id <= current_id + 1 <= final_id
                        or previous_id == current_id
                    )
                    if not bridges_snapshot:
                        if first_id > current_id + 1:
                            self._last_error = (
                                "snapshot cannot bridge buffered diff event"
                            )
                            return False
                        continue
                elif previous_id != current_id:
                    self._last_error = "buffered diff sequence gap"
                    return False

                self._apply_updates_to(
                    bids,
                    asks,
                    event,
                    count_flow=False,
                )
                current_id = final_id
                last_receive_ts_ns = max(
                    last_receive_ts_ns,
                    int(receive_ts_ns),
                )
                last_exchange_ts_ns = max(
                    last_exchange_ts_ns,
                    self._event_exchange_ts_ns(event),
                )
                applied_any = True

            self._bids = bids
            self._asks = asks
            self._flow = {"BUY": {}, "SELL": {}}
            self._last_update_id = current_id
            self._last_receive_ts_ns = last_receive_ts_ns
            self._last_exchange_ts_ns = last_exchange_ts_ns
            self._generation += 1
            self._resync_count += 1
            self._valid = True
            self._last_error = ""
            self._buffer.clear()
            return True

    def _flow_state(self, side: str, price_tick: int) -> list[float]:
        return self._flow[side].setdefault(
            price_tick,
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        )

    @staticmethod
    def _event_exchange_ts_ns(event: Mapping[str, Any]) -> int:
        value = int(event.get("T", event.get("E", 0)) or 0)
        return value * 1_000_000 if value > 0 else 0

    def _apply_side_updates(
        self,
        book: dict[int, float],
        rows: Any,
        *,
        side: str,
        count_flow: bool,
    ) -> None:
        for raw in rows or ():
            if not isinstance(raw, (list, tuple)) or len(raw) < 2:
                continue
            price_tick = self._price_tick(raw[0])
            new_quantity = self._quantity(raw[1])
            old_quantity = float(book.get(price_tick, 0.0))
            if count_flow and not math.isclose(
                new_quantity,
                old_quantity,
                rel_tol=0.0,
                abs_tol=1e-15,
            ):
                counters = self._flow_state(side, price_tick)
                if new_quantity < old_quantity:
                    counters[0] += 1.0
                    counters[1] += old_quantity - new_quantity
                else:
                    counters[2] += 1.0
                    counters[3] += new_quantity - old_quantity
            if new_quantity <= 0.0:
                book.pop(price_tick, None)
            else:
                book[price_tick] = new_quantity

    def _trim(
        self,
        book: dict[int, float],
        *,
        side: str,
        reverse: bool,
    ) -> None:
        threshold = max(self.snapshot_levels + 100, int(self.snapshot_levels * 1.25))
        if len(book) <= threshold:
            return
        keep = set(sorted(book, reverse=reverse)[: self.snapshot_levels])
        for price_tick in tuple(book):
            if price_tick not in keep:
                book.pop(price_tick, None)
                self._flow[side].pop(price_tick, None)

    def _apply_updates_to(
        self,
        bids: dict[int, float],
        asks: dict[int, float],
        event: Mapping[str, Any],
        *,
        count_flow: bool,
    ) -> None:
        self._apply_side_updates(
            bids,
            event.get("b"),
            side="BUY",
            count_flow=count_flow,
        )
        self._apply_side_updates(
            asks,
            event.get("a"),
            side="SELL",
            count_flow=count_flow,
        )
        self._trim(bids, side="BUY", reverse=True)
        self._trim(asks, side="SELL", reverse=False)

    def on_diff_event(
        self,
        event: Mapping[str, Any],
        *,
        receive_ts_ns: int,
    ) -> None:
        event_symbol = str(event.get("s", self.symbol)).upper()
        if event_symbol != self.symbol:
            return
        first_id, final_id, previous_id = self._event_ids(event)
        if first_id <= 0 or final_id <= 0 or final_id < first_id:
            with self._lock:
                self._ignored_events += 1
            return

        copied = {
            "s": event_symbol,
            "U": first_id,
            "u": final_id,
            "pu": previous_id,
            "E": int(event.get("E", 0) or 0),
            "T": int(event.get("T", event.get("E", 0)) or 0),
            "b": list(event.get("b") or ()),
            "a": list(event.get("a") or ()),
        }
        with self._lock:
            if not self._running:
                return
            if not self._valid:
                self._buffer.append((copied, int(receive_ts_ns)))
                if len(self._buffer) > self.max_buffer_events:
                    self._buffer_overflow_count += 1
                    self._buffer.popleft()
                    self._last_error = "diff buffer overflow"
                self._launch_sync_locked("unsynchronized_diff")
                return

            if final_id <= self._last_update_id:
                self._ignored_events += 1
                return
            if previous_id != self._last_update_id:
                self._gap_count += 1
                self._valid = False
                self._last_error = (
                    f"diff sequence gap pu={previous_id} "
                    f"expected={self._last_update_id}"
                )
                self._buffer.clear()
                self._buffer.append((copied, int(receive_ts_ns)))
                self._launch_sync_locked("sequence_gap")
                return

            self._apply_updates_to(
                self._bids,
                self._asks,
                copied,
                count_flow=True,
            )
            self._last_update_id = final_id
            self._last_receive_ts_ns = int(receive_ts_ns)
            self._last_exchange_ts_ns = self._event_exchange_ts_ns(copied)

    def on_agg_trade(
        self,
        event: Mapping[str, Any],
        *,
        receive_ts_ns: int,
        feature_ready_ts_ns: int | None = None,
    ) -> None:
        """Record exact-price taker quantity for depth-decrease attribution."""

        event_symbol = str(event.get("s", self.symbol)).upper()
        if event_symbol != self.symbol:
            return
        try:
            price_tick = self._price_tick(event.get("p"))
            quantity = self._quantity(event.get("q"))
        except (TypeError, ValueError):
            return
        if quantity <= 0.0:
            return
        resting_side = "BUY" if bool(event.get("m", False)) else "SELL"
        exchange_ts_ns = self._event_exchange_ts_ns(event)
        with self._lock:
            if not self._running or not self._valid:
                return
            book = self._bids if resting_side == "BUY" else self._asks
            if book and not (min(book) <= price_tick <= max(book)):
                return
            counters = self._flow_state(resting_side, price_tick)
            counters[4] += 1.0
            counters[5] += quantity
            self._trade_count += 1
            self._last_trade_receive_ts_ns = int(receive_ts_ns)
            self._last_trade_feature_ready_ts_ns = int(
                feature_ready_ts_ns
                if feature_ready_ts_ns is not None
                else time.time_ns()
            )
            self._last_trade_exchange_ts_ns = max(
                self._last_trade_exchange_ts_ns,
                exchange_ts_ns,
            )

    def level_state(
        self,
        side: str,
        price: float,
        *,
        now_ns: int | None = None,
        max_age_ms: float | None = None,
    ) -> DeepLevelState:
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("deep-book side must be BUY or SELL")
        price_tick = self._price_tick(price)
        with self._lock:
            book = self._bids if normalized_side == "BUY" else self._asks
            counters = self._flow[normalized_side].get(
                price_tick,
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            )
            receive_ts_ns = int(self._last_receive_ts_ns)
            clock_ns = int(now_ns if now_ns is not None else time.time_ns())
            age_ms = (
                max(0.0, (clock_ns - receive_ts_ns) / 1_000_000.0)
                if receive_ts_ns > 0
                else math.inf
            )
            covered = bool(book) and min(book) <= price_tick <= max(book)
            fresh = max_age_ms is None or age_ms <= float(max_age_ms)
            return DeepLevelState(
                valid=bool(self._valid and covered and fresh),
                covered=bool(covered),
                generation=int(self._generation),
                side=normalized_side,
                price=float(price_tick * self.tick_size),
                quantity=float(book.get(price_tick, 0.0)),
                last_update_id=int(self._last_update_id),
                receive_ts_ns=receive_ts_ns,
                age_ms=float(age_ms),
                decrease_events=int(counters[0]),
                decrease_qty=float(counters[1]),
                increase_events=int(counters[2]),
                increase_qty=float(counters[3]),
                trade_events=int(counters[4]),
                trade_qty=float(counters[5]),
                last_exchange_ts_ns=int(self._last_exchange_ts_ns),
            )

    def snapshot(
        self,
        *,
        now_ns: int | None = None,
        max_age_ms: float | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            clock_ns = int(now_ns if now_ns is not None else time.time_ns())
            best_bid_tick = max(self._bids) if self._bids else 0
            best_ask_tick = min(self._asks) if self._asks else 0
            age_ms = (
                max(
                    0.0,
                    (clock_ns - int(self._last_receive_ts_ns)) / 1_000_000.0,
                )
                if self._last_receive_ts_ns > 0
                else math.inf
            )
            stale = max_age_ms is not None and age_ms > float(max_age_ms)
            return {
                "enabled": int(self._running),
                "valid": int(self._valid and not stale),
                "stale": int(stale),
                "syncing": int(self._syncing),
                "symbol": self.symbol,
                "generation": int(self._generation),
                "last_update_id": int(self._last_update_id),
                "last_exchange_ts_ns": int(self._last_exchange_ts_ns),
                "last_receive_ts_ns": int(self._last_receive_ts_ns),
                "feature_ready_ts_ns": int(clock_ns),
                "age_ms": float(age_ms),
                "bid_levels": len(self._bids),
                "ask_levels": len(self._asks),
                "best_bid": float(best_bid_tick * self.tick_size),
                "best_bid_qty": float(self._bids.get(best_bid_tick, 0.0)),
                "best_ask": float(best_ask_tick * self.tick_size),
                "best_ask_qty": float(self._asks.get(best_ask_tick, 0.0)),
                "buffer_events": len(self._buffer),
                "gap_count": int(self._gap_count),
                "resync_count": int(self._resync_count),
                "ignored_events": int(self._ignored_events),
                "buffer_overflow_count": int(self._buffer_overflow_count),
                "trade_count": int(self._trade_count),
                "last_trade_receive_ts_ns": int(
                    self._last_trade_receive_ts_ns
                ),
                "last_trade_feature_ready_ts_ns": int(
                    self._last_trade_feature_ready_ts_ns
                ),
                "last_trade_exchange_ts_ns": int(
                    self._last_trade_exchange_ts_ns
                ),
                "last_error": str(self._last_error),
            }
