"""Shared primitives for read-only external venue market-data adapters."""

from __future__ import annotations

import gzip
import json
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

MARKET_TAPE_SCHEMA_VERSION = "market_tape.v1"
logger = logging.getLogger("market_data_dispatch")


def _should_log_counter(value: int) -> bool:
    """Log the first loss and then powers of two without flooding the feed."""
    return value == 1 or (value > 0 and value & (value - 1) == 0)


def normalize_market_tape_row(
    row: dict,
    *,
    feature_ready_ts_ns: int | None = None,
) -> dict:
    """Return one causal, venue-neutral market-tape event.

    ``local_receive_ts_ns`` is when the callback first observed the payload;
    ``feature_ready_ts_ns`` is captured only after the signal state consumed
    it.  Keeping both timestamps prevents decode/feature time from being
    mistaken for exchange transport latency.
    """
    out = dict(row)
    receive_ns = int(out.get("local_receive_ts_ns", 0) or time.time_ns())
    ready_ns = int(
        feature_ready_ts_ns
        if feature_ready_ts_ns is not None
        else out.get("feature_ready_ts_ns", 0) or time.time_ns()
    )
    ready_ns = max(receive_ns, ready_ns)
    exchange_ns = int(out.get("exchange_event_ts_ns", 0) or 0)
    event_type = str(out.get("event_type", "")).strip().lower()
    market_id = str(out.get("market_id", "")).strip()
    if not market_id or not event_type:
        raise ValueError("market tape row requires market_id and event_type")

    out["schema_version"] = MARKET_TAPE_SCHEMA_VERSION
    out["market_id"] = market_id
    out["event_type"] = event_type
    out["exchange_event_ts_ns"] = exchange_ns
    out["local_receive_ts_ns"] = receive_ns
    out["feature_ready_ts_ns"] = ready_ns
    out["feature_latency_us"] = (ready_ns - receive_ns) / 1_000.0
    out.setdefault(
        "event_timestamp_source",
        "exchange" if exchange_ns > 0 else "missing",
    )
    if exchange_ns > 0 and out.get("transport_lag_ms") is None:
        out["transport_lag_ms"] = (receive_ns - exchange_ns) / 1_000_000.0
    out.setdefault("transport", "unknown")
    out.setdefault("sequence_number", None)
    out.setdefault("previous_sequence_number", None)
    # ``None`` means the transport does not guarantee contiguous sequence
    # numbers.  Do not turn a polling snapshot id jump into a fake feed gap.
    out.setdefault("gap_flag", None)
    if event_type == "trade":
        side = str(out.get("aggressor_side", out.get("side", ""))).strip().lower()
        out["aggressor_side"] = side if side in {"buy", "sell"} else None
    return out


class DailyJsonlRecorder:
    """Write normalized external events away from feed/poll callback threads."""

    def __init__(
        self,
        root: Path,
        *,
        file_prefix: str,
        thread_name: str,
        queue_size: int = 20_000,
        compress: bool = True,
    ):
        self.root = Path(root).expanduser()
        self.file_prefix = str(file_prefix).strip().lower().replace("/", "-")
        self.thread_name = str(thread_name)
        self.compress = bool(compress)
        started_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        self.session_id = f"{started_at}_p{os.getpid()}"
        self._queue: queue.Queue[tuple[int, dict] | None] = queue.Queue(
            maxsize=max(1, queue_size)
        )
        self._running = False
        self._thread: threading.Thread | None = None
        self._metrics_lock = threading.Lock()
        self.submitted = 0
        self.dropped = 0
        self.invalid = 0
        self.written = 0
        self.queue_high_watermark = 0
        self.last_queue_age_ms = 0.0
        self.max_queue_age_ms = 0.0

    def start(self) -> None:
        if self._running:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name=self.thread_name)
        self._thread.start()

    def submit(self, row: dict) -> None:
        if not self._running:
            return
        try:
            normalized = normalize_market_tape_row(row)
        except (TypeError, ValueError):
            with self._metrics_lock:
                self.invalid += 1
            return
        enqueued_ns = time.monotonic_ns()
        try:
            self._queue.put_nowait((enqueued_ns, normalized))
            depth = self._queue.qsize()
            with self._metrics_lock:
                self.submitted += 1
                self.queue_high_watermark = max(self.queue_high_watermark, depth)
        except queue.Full:
            with self._metrics_lock:
                self.dropped += 1
                dropped = self.dropped
            if _should_log_counter(dropped):
                logger.warning(
                    "Market-tape recorder queue full prefix=%s dropped=%d size=%d",
                    self.file_prefix,
                    dropped,
                    self._queue.maxsize,
                )

    def snapshot(self) -> dict[str, int | float]:
        now_ns = time.monotonic_ns()
        current_age_ms = 0.0
        with self._queue.mutex:
            for item in self._queue.queue:
                if item is not None:
                    current_age_ms = max(0.0, (now_ns - item[0]) / 1_000_000.0)
                    break
            depth = len(self._queue.queue)
        with self._metrics_lock:
            return {
                "submitted": int(self.submitted),
                "written": int(self.written),
                "dropped": int(self.dropped),
                "invalid": int(self.invalid),
                "queue_depth": int(depth),
                "queue_capacity": int(self._queue.maxsize),
                "queue_high_watermark": int(self.queue_high_watermark),
                "queue_age_ms": float(current_age_ms),
                "last_queue_age_ms": float(self.last_queue_age_ms),
                "max_queue_age_ms": float(self.max_queue_age_ms),
            }

    def stop(self, timeout_s: float = 5.0) -> None:
        if not self._running:
            return
        self._running = False
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        if self._thread is not None:
            self._thread.join(timeout=max(0.0, timeout_s))
        self._thread = None

    def _run(self) -> None:
        handle = None
        active_day = ""
        last_flush = time.monotonic()
        try:
            while self._running or not self._queue.empty():
                try:
                    item = self._queue.get(timeout=0.5)
                except queue.Empty:
                    item = None
                if item is None:
                    if handle is not None and time.monotonic() - last_flush >= 1.0:
                        handle.flush()
                        last_flush = time.monotonic()
                    if not self._running:
                        break
                    continue

                enqueued_ns, row = item
                queue_age_ms = max(
                    0.0, (time.monotonic_ns() - enqueued_ns) / 1_000_000.0
                )
                with self._metrics_lock:
                    self.last_queue_age_ms = queue_age_ms
                    self.max_queue_age_ms = max(self.max_queue_age_ms, queue_age_ms)

                receive_ns = int(row.get("local_receive_ts_ns", time.time_ns()))
                day = (
                    datetime.fromtimestamp(receive_ns / 1_000_000_000, tz=timezone.utc)
                    .date()
                    .isoformat()
                )
                if day != active_day:
                    if handle is not None:
                        handle.flush()
                        handle.close()
                    suffix = ".jsonl.gz" if self.compress else ".jsonl"
                    path = self.root / (
                        f"{self.file_prefix}_{day}_{self.session_id}{suffix}"
                    )
                    handle = (
                        gzip.open(path, "at", encoding="utf-8", compresslevel=1)
                        if self.compress
                        else path.open("a", encoding="utf-8", buffering=1 << 20)
                    )
                    active_day = day
                handle.write(json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n")
                with self._metrics_lock:
                    self.written += 1
                if time.monotonic() - last_flush >= 1.0:
                    handle.flush()
                    last_flush = time.monotonic()
        finally:
            if handle is not None:
                handle.flush()
                handle.close()

def publish_cross_trade_arrays(
    signal: Any,
    *,
    symbol: str,
    ts_ms: Sequence[int],
    prices: Sequence[float],
    quantities: Sequence[float],
    is_buyer_maker: Sequence[bool],
    market_type: str,
    venue: str,
    receive_ts_ns: int,
    sequence_numbers: Sequence[Optional[int]],
) -> None:
    """Publish a compact cross-venue trade frame."""
    count = len(ts_ms)
    if count == 0:
        return
    if not (
        len(prices) == count
        and len(quantities) == count
        and len(is_buyer_maker) == count
    ):
        raise ValueError("cross-trade arrays must have equal length")
    signal.on_cross_trade_arrays(
        symbol,
        ts_ms,
        prices,
        quantities,
        is_buyer_maker,
        market_type=market_type,
        venue=venue,
        receive_ts_ns=receive_ts_ns,
        sequence_numbers=sequence_numbers,
    )
