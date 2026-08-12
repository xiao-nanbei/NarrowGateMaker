"""Non-blocking live adapter for the synchronous lifecycle journal-v2 writer.

The maker callback performs no filesystem work.  It takes an immutable copy of
the small lifecycle state and uses ``put_nowait`` on a bounded queue.  All
validation, Parquet/JSONL persistence, fsync, health publication, and cursor
advancement happen on the dedicated worker.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import queue
import resource
import threading
import time
import uuid
from bisect import bisect_left, insort_right
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from execution.order_lifecycle import OrderLifecyclePhase, QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_storage_v2 import (
    BOUNDED_REMOTE_SPOOL,
    LIFECYCLE_JOURNAL_V2_STORAGE_PROFILES,
    LOCAL_ORICO_REPLAY_ADMISSION,
)
from execution.order_lifecycle_journal_v2 import OrderLifecycleJournalV2SourceCallback
from execution.order_lifecycle_journal_writer_v2 import (
    OrderLifecycleJournalRuntimeBridgeV2,
    OrderLifecycleJournalWriterV2,
)

ORDER_LIFECYCLE_LIVE_WRITER_V2_SCHEMA_VERSION = "order_lifecycle_live_writer.v2"
ORDER_LIFECYCLE_LIVE_WRITER_V2_HEALTH_VERSION = "order_lifecycle_live_writer_health.v2"
_SNAPSHOT_CAPTURE_MAX_ATTEMPTS = 2


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _extend_rolling_ordered(
    history: deque[float | int],
    ordered: list[float | int],
    values: Iterable[float | int],
) -> None:
    """Update one exact rolling order statistic without re-sorting history."""

    for value in values:
        if history.maxlen is not None and len(history) == history.maxlen:
            expired = history.popleft()
            index = bisect_left(ordered, expired)
            if index >= len(ordered) or ordered[index] != expired:
                raise RuntimeError("latency order-statistic history is inconsistent")
            ordered.pop(index)
        history.append(value)
        insort_right(ordered, value)


def _latency_summary(
    ordered: Sequence[float | int],
    *,
    scale: float,
) -> tuple[float, float, float]:
    if not ordered:
        return 0.0, 0.0, 0.0

    def percentile(percentile_value: float) -> float:
        index = max(
            0,
            min(
                len(ordered) - 1,
                math.ceil(percentile_value * len(ordered)) - 1,
            ),
        )
        return float(ordered[index]) / scale

    return percentile(0.50), percentile(0.99), float(ordered[-1]) / scale


def _max_rss_mb() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # macOS reports bytes; Linux reports KiB.
    if value > 10_000_000:
        return value / (1024.0 * 1024.0)
    return value / 1024.0


def _tree_size_bytes(root: Path) -> int:
    total = 0
    if not root.exists():
        return total
    for path in root.rglob("*"):
        try:
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total


@dataclass(frozen=True, slots=True)
class _LifecycleWorkItem:
    lifecycle: QuantityWeightedOrderLifecycle
    client_order_id: str
    exchange_order_id: Any
    symbol: Any
    side: Any
    source_event_type: str
    received_ts_ns: int
    exchange_ts_ns: int | None


@dataclass(frozen=True, slots=True)
class _PreparedLifecycleWorkItem:
    lifecycle_id: str
    lifecycle: QuantityWeightedOrderLifecycle
    client_order_id: str
    exchange_order_id: str | None
    symbol: str
    side: str
    callback: OrderLifecycleJournalV2SourceCallback


def _snapshot_inconsistency_reason(
    lifecycle: QuantityWeightedOrderLifecycle,
) -> str | None:
    """Compare copied scalar state with the copied latest event in constant time."""

    if not lifecycle._events:
        return "latest_event_missing"
    latest = lifecycle._events[-1]
    if int(latest.sequence) != len(lifecycle._events):
        return "latest_event_sequence_mismatch"
    if lifecycle.phase.value != latest.phase_after:
        return "phase_mismatch"
    if lifecycle.remaining_quantity != latest.remaining_qty_after:
        return "remaining_quantity_mismatch"
    visible_exposure = lifecycle.quantity_time_exposure_btc_s
    if (
        visible_exposure != latest.quantity_time_exposure_btc_s
        or visible_exposure != latest.quantity_time_exposure_visible_btc_s
    ):
        return "visible_exposure_mismatch"
    if bool(lifecycle.exchange_exposure_valid) != bool(latest.exchange_exposure_valid):
        return "exchange_valid_mismatch"
    if lifecycle.exchange_exposure_valid and lifecycle.activation_exchange_ts_ns > 0:
        if (
            latest.quantity_time_exposure_exchange_btc_s is None
            or lifecycle.quantity_time_exposure_exchange_accumulated_btc_s
            != latest.quantity_time_exposure_exchange_btc_s
        ):
            return "exchange_exposure_mismatch"
    elif latest.quantity_time_exposure_exchange_btc_s is not None:
        return "exchange_exposure_availability_mismatch"
    return None


def _diagnostic_text(value: Any) -> str:
    try:
        normalized = str(value).strip()
    except Exception:
        return f"<{type(value).__name__}>"
    return normalized or "<missing>"


def _normalize_local_shutdown_censor(
    lifecycle: QuantityWeightedOrderLifecycle,
    source_event_type: str,
) -> None:
    """Translate the legacy local terminal mutation on the private snapshot.

    ``OrderManager.cancel_all_local`` removes local ownership but does not
    prove exchange terminality.  The strict v2 journal therefore records a
    right-censor while leaving the trading object's legacy state untouched.
    """

    if str(source_event_type) not in {
        "administrative_cancel",
        "local_shutdown_cancel",
        "shutdown",
    }:
        return
    if not lifecycle._events:
        raise ValueError("local shutdown lifecycle has no event")
    latest = lifecycle._events[-1]
    if latest.event != "exchange_terminal" or latest.reason not in {
        "administrative_cancel",
        "local_shutdown_cancel",
        "shutdown",
    }:
        raise ValueError("local shutdown lifecycle lacks the legacy terminal event")
    prior_phase = OrderLifecyclePhase(latest.phase_before)
    prior_event = lifecycle._events[-2] if len(lifecycle._events) > 1 else None
    prior_exchange_valid = (
        bool(prior_event.exchange_exposure_valid) if prior_event is not None else True
    )
    prior_exchange_exposure = (
        prior_event.quantity_time_exposure_exchange_btc_s if prior_event is not None else None
    )
    lifecycle._events[-1] = replace(
        latest,
        event="local_shutdown_censor",
        exchange_ts_ns=0,
        phase_after=prior_phase.value,
        quantity_time_exposure_exchange_btc_s=prior_exchange_exposure,
        exchange_exposure_valid=prior_exchange_valid,
    )
    lifecycle.phase = prior_phase
    lifecycle.terminal_ts_ns = 0
    lifecycle.terminal_exchange_ts_ns = 0
    lifecycle.terminal_reason = ""
    lifecycle.terminal_policy_route_name = ""
    lifecycle.exchange_exposure_valid = prior_exchange_valid
    if prior_exchange_valid:
        lifecycle.exchange_exposure_invalid_reason = ""
    lifecycle.exchange_exposure_complete = False


class OrderLifecycleLiveWriterV2:
    """Bounded asynchronous adapter; collection failure never stops trading."""

    def __init__(
        self,
        root: str | Path,
        *,
        session_id: str,
        baseline_epoch_id: str,
        runtime_identity: Mapping[str, Any],
        queue_size: int = 8192,
        storage_format: str = "parquet",
        heartbeat_interval_s: float = 5.0,
        initial_active_order_ids=(),
        latency_sample_size: int = 8192,
        writer_factory=OrderLifecycleJournalWriterV2,
        storage_profile: str = LOCAL_ORICO_REPLAY_ADMISSION,
        epoch_root: str | Path | None = None,
        session_max_duration_s: float | None = None,
        session_max_bytes: int | None = None,
    ) -> None:
        if int(queue_size) <= 0:
            raise ValueError("live lifecycle journal queue_size must be positive")
        if float(heartbeat_interval_s) <= 0.0:
            raise ValueError("live lifecycle journal heartbeat interval must be positive")
        if int(latency_sample_size) <= 0:
            raise ValueError("live lifecycle latency sample size must be positive")
        normalized_profile = str(storage_profile).strip()
        if normalized_profile not in LIFECYCLE_JOURNAL_V2_STORAGE_PROFILES:
            raise ValueError("unsupported lifecycle journal storage profile")
        if normalized_profile == BOUNDED_REMOTE_SPOOL:
            if epoch_root is None:
                raise ValueError("bounded remote spool requires epoch_root")
            if session_max_duration_s is None or float(session_max_duration_s) <= 0.0:
                raise ValueError("bounded remote spool requires positive session duration")
            if session_max_bytes is None or int(session_max_bytes) <= 0:
                raise ValueError("bounded remote spool requires positive session byte limit")
        self.root = Path(root).expanduser().resolve()
        self.session_id = str(session_id)
        self.baseline_epoch_id = str(baseline_epoch_id)
        self.storage_profile = normalized_profile
        self.epoch_root = (
            Path(epoch_root).expanduser().resolve() if epoch_root is not None else None
        )
        self._session_max_duration_s = (
            float(session_max_duration_s) if normalized_profile == BOUNDED_REMOTE_SPOOL else None
        )
        self._session_max_bytes = (
            int(session_max_bytes) if normalized_profile == BOUNDED_REMOTE_SPOOL else None
        )
        self._collection_started_ts_ns = time.time_ns()
        self._collection_started_monotonic_ns = time.monotonic_ns()
        self._collection_started_monotonic = self._collection_started_monotonic_ns / 1_000_000_000.0
        self._collection_deadline_monotonic_ns = (
            self._collection_started_monotonic_ns
            + int(float(session_max_duration_s) * 1_000_000_000)
            if normalized_profile == BOUNDED_REMOTE_SPOOL
            else 0
        )
        self._collection_bound_reached = False
        self._collection_bound_reason = ""
        self._collection_stopped_ts_ns = 0
        self._collection_duration_s = 0.0
        self._callbacks_ignored_after_bound = 0
        self._observed_spool_bytes = 0
        self._session_byte_limit_exceeded = False
        self.health_path = self.root / f"session-{self.session_id}" / "live_health.json"
        self._queue: queue.Queue[_LifecycleWorkItem | object] = queue.Queue(maxsize=int(queue_size))
        self._sentinel = object()
        self._enqueue_metrics_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._health_lock = threading.Lock()
        self._latency_sample_size = int(latency_sample_size)
        self._enqueue_latency_pending_ns: deque[int] = deque(maxlen=self._latency_sample_size)
        self._enqueue_latency_history_ns: deque[int] = deque(maxlen=self._latency_sample_size)
        self._enqueue_latency_ordered_ns: list[int] = []
        self._write_latency_pending_ms: deque[float] = deque(maxlen=self._latency_sample_size)
        self._write_latency_history_ms: deque[float] = deque(maxlen=self._latency_sample_size)
        self._write_latency_ordered_ms: list[float] = []
        self._queue_hwm = 0
        self._enqueued = 0
        self._processed = 0
        self._rows_committed = 0
        self._drops = 0
        self._producer_errors = 0
        self._producer_last_error = ""
        self._producer_last_error_ts_ns = 0
        self._errors = 0
        self._last_error = ""
        self._last_error_ts_ns = 0
        self._last_enqueue_ts_ns = 0
        self._last_worker_flush_ts_ns = 0
        self._last_health_write_ts_ns = 0
        self._closed = False
        self._worker_alive = False
        self._worker_thread_cpu_s = 0.0
        self._heartbeat_interval_s = float(heartbeat_interval_s)
        self._writer = writer_factory(
            self.root,
            session_id=self.session_id,
            runtime_identity=dict(runtime_identity),
            storage_format=str(storage_format),
            initial_active_order_ids=tuple(initial_active_order_ids),
            heartbeat_interval_s=float(heartbeat_interval_s),
            start_heartbeat=True,
        )
        self._bridge = OrderLifecycleJournalRuntimeBridgeV2(self._writer)
        self._core_health = self._writer.health_snapshot()
        self._registered: set[str] = set()
        self._thread = threading.Thread(
            target=self._worker_loop,
            name=f"lifecycle-live-v2-{self.session_id}",
            daemon=True,
        )
        self._thread.start()

    def _update_duration_bound(self, *, now_monotonic_ns: int | None = None) -> bool:
        if self.storage_profile != BOUNDED_REMOTE_SPOOL:
            return False
        if self._collection_bound_reached:
            return True
        observed_monotonic_ns = (
            int(now_monotonic_ns) if now_monotonic_ns is not None else time.monotonic_ns()
        )
        if observed_monotonic_ns < self._collection_deadline_monotonic_ns:
            return False
        with self._metrics_lock:
            if self._collection_bound_reached:
                return True
            self._collection_bound_reached = True
            self._collection_bound_reason = "max_duration_reached"
            self._collection_stopped_ts_ns = self._collection_started_ts_ns + int(
                float(self._session_max_duration_s) * 1_000_000_000
            )
            self._collection_duration_s = float(self._session_max_duration_s)
            return True

    def _update_remote_spool_size(self) -> None:
        if self.storage_profile != BOUNDED_REMOTE_SPOOL:
            return
        observed = _tree_size_bytes(self._writer.session_root)
        if self.epoch_root is not None:
            observed += _tree_size_bytes(self.epoch_root)
        with self._metrics_lock:
            self._observed_spool_bytes = observed
            if observed > int(self._session_max_bytes):
                first_violation = not self._session_byte_limit_exceeded
                self._session_byte_limit_exceeded = True
                self._collection_bound_reached = True
                self._collection_bound_reason = "max_bytes_exceeded"
                self._collection_stopped_ts_ns = time.time_ns()
                self._collection_duration_s = max(
                    0.0,
                    time.monotonic() - self._collection_started_monotonic,
                )
                if first_violation:
                    self._errors += 1
                    self._last_error = "remote_spool_byte_limit_exceeded"
                    self._last_error_ts_ns = time.time_ns()
            elif observed >= int(self._session_max_bytes):
                self._collection_bound_reached = True
                self._collection_bound_reason = "max_bytes_reached"
                self._collection_stopped_ts_ns = time.time_ns()
                self._collection_duration_s = max(
                    0.0,
                    time.monotonic() - self._collection_started_monotonic,
                )

    @staticmethod
    def _callback(
        *,
        baseline_epoch_id: str,
        client_order_id: str,
        source_event_type: str,
        lifecycle: QuantityWeightedOrderLifecycle,
        received_ts_ns: int,
        exchange_ts_ns: int | None,
    ) -> OrderLifecycleJournalV2SourceCallback:
        latest = lifecycle.latest_event()
        normalized_exchange_ts_ns = int(exchange_ts_ns or 0)
        callback_payload = (
            f"{baseline_epoch_id}|{client_order_id}|{latest.sequence}|"
            f"{source_event_type}|{received_ts_ns}|{normalized_exchange_ts_ns}"
        ).encode()
        return OrderLifecycleJournalV2SourceCallback(
            callback_id=hashlib.sha256(callback_payload).hexdigest(),
            callback_type=str(source_event_type),
            received_ts_ns=received_ts_ns,
            exchange_ts_ns=(normalized_exchange_ts_ns if normalized_exchange_ts_ns > 0 else None),
        )

    @staticmethod
    def _capture_callback_fields(
        *,
        source_event_type: Any,
        lifecycle: QuantityWeightedOrderLifecycle,
        raw_event: Mapping[str, Any] | None,
    ) -> tuple[str, int, int | None]:
        callback_type = str(source_event_type)
        callback_type_identity = callback_type.strip()
        if not callback_type_identity or callback_type_identity.lower() in {"nan", "none", "null"}:
            raise ValueError("source callback type is required")
        latest = lifecycle.latest_event()
        if raw_event is None:
            local_receive_ts_ns = 0
            exchange_ts_ns = 0
            exchange_event_ts_ms = 0
        else:
            local_receive_ts_ns = raw_event.get("_local_receive_ts_ns", 0)
            exchange_ts_ns = raw_event.get("_exchange_ts_ns", 0)
            exchange_event_ts_ms = raw_event.get("T", 0)
        received_ts_ns = int(local_receive_ts_ns or latest.visibility_ts_ns)
        if received_ts_ns <= 0:
            raise ValueError("source callback received timestamp must be positive")
        normalized_exchange_ts_ns = int(exchange_ts_ns or 0)
        if normalized_exchange_ts_ns <= 0:
            normalized_exchange_ts_ns = int(exchange_event_ts_ms or 0) * 1_000_000
        if normalized_exchange_ts_ns <= 0:
            normalized_exchange_ts_ns = int(latest.exchange_ts_ns or 0)
        normalized_exchange_ts = (
            normalized_exchange_ts_ns if normalized_exchange_ts_ns > 0 else None
        )
        if normalized_exchange_ts is not None and normalized_exchange_ts > received_ts_ns:
            raise ValueError("source callback exchange timestamp is after received time")
        return callback_type, received_ts_ns, normalized_exchange_ts

    def _capture_consistent_snapshot(
        self,
        lifecycle: QuantityWeightedOrderLifecycle,
        raw_client_order_id: Any,
    ) -> QuantityWeightedOrderLifecycle:
        last_reason = "snapshot_not_attempted"
        for _attempt in range(_SNAPSHOT_CAPTURE_MAX_ATTEMPTS):
            snapshot = lifecycle.journal_snapshot()
            reason = _snapshot_inconsistency_reason(snapshot)
            if reason is None:
                return snapshot
            last_reason = reason
        client_order_id = _diagnostic_text(raw_client_order_id)
        lifecycle_id = f"{self.baseline_epoch_id}:{client_order_id}"
        raise ValueError(
            "lifecycle_snapshot_inconsistent:"
            f"lifecycle_id={lifecycle_id}:"
            f"client_order_id={client_order_id}:"
            f"attempts={_SNAPSHOT_CAPTURE_MAX_ATTEMPTS}:"
            f"last_reason={last_reason}"
        )

    def _prepare_work_item(
        self,
        item: _LifecycleWorkItem,
    ) -> _PreparedLifecycleWorkItem:
        exchange_order_id = str(item.exchange_order_id or "").strip() or None
        callback = self._callback(
            baseline_epoch_id=self.baseline_epoch_id,
            client_order_id=item.client_order_id,
            source_event_type=item.source_event_type,
            lifecycle=item.lifecycle,
            received_ts_ns=item.received_ts_ns,
            exchange_ts_ns=item.exchange_ts_ns,
        )
        return _PreparedLifecycleWorkItem(
            lifecycle_id=f"{self.baseline_epoch_id}:{item.client_order_id}",
            lifecycle=item.lifecycle,
            client_order_id=item.client_order_id,
            exchange_order_id=exchange_order_id,
            symbol=str(item.symbol).strip(),
            side=str(item.side).strip(),
            callback=callback,
        )

    def enqueue_order_event(
        self,
        order: Any,
        source_event_type: str,
        raw_event: Mapping[str, Any] | None = None,
    ) -> bool:
        """Copy and enqueue one callback without filesystem I/O or waiting."""

        started = time.monotonic_ns()
        raw_client_order_id: Any = "<unread>"
        try:
            if self._closed:
                raise RuntimeError("live lifecycle writer is closed")
            if self._update_duration_bound(now_monotonic_ns=started):
                with self._enqueue_metrics_lock:
                    self._callbacks_ignored_after_bound += 1
                return False
            lifecycle = getattr(order, "lifecycle", None)
            if lifecycle is None:
                raise ValueError("order lifecycle is missing")
            raw_client_order_id = getattr(order, "client_order_id", "")
            lifecycle_snapshot = self._capture_consistent_snapshot(
                lifecycle,
                raw_client_order_id,
            )
            source_event_type_text = str(source_event_type)
            _normalize_local_shutdown_censor(
                lifecycle_snapshot,
                source_event_type_text,
            )
            client_order_id = str(raw_client_order_id).strip()
            if not client_order_id:
                raise ValueError("client order id is missing")
            (
                callback_type,
                received_ts_ns,
                exchange_ts_ns,
            ) = self._capture_callback_fields(
                source_event_type=source_event_type_text,
                lifecycle=lifecycle_snapshot,
                raw_event=raw_event,
            )
            item = _LifecycleWorkItem(
                lifecycle=lifecycle_snapshot,
                client_order_id=client_order_id,
                exchange_order_id=getattr(order, "order_id", ""),
                symbol=getattr(order, "symbol", ""),
                side=getattr(getattr(order, "side", None), "value", ""),
                source_event_type=callback_type,
                received_ts_ns=received_ts_ns,
                exchange_ts_ns=exchange_ts_ns,
            )
            self._queue.put_nowait(item)
            elapsed_ns = time.monotonic_ns() - started
            queue_depth = self._queue.qsize()
            enqueue_ts_ns = time.time_ns()
            with self._enqueue_metrics_lock:
                self._enqueue_latency_pending_ns.append(elapsed_ns)
                self._enqueued += 1
                self._last_enqueue_ts_ns = enqueue_ts_ns
                self._queue_hwm = max(self._queue_hwm, queue_depth)
            return True
        except queue.Full:
            elapsed_ns = time.monotonic_ns() - started
            error_ts_ns = time.time_ns()
            with self._enqueue_metrics_lock:
                self._enqueue_latency_pending_ns.append(elapsed_ns)
                self._drops += 1
                self._producer_last_error = "bounded_queue_full"
                self._producer_last_error_ts_ns = error_ts_ns
            return False
        except Exception as exc:
            elapsed_ns = time.monotonic_ns() - started
            client_order_id = _diagnostic_text(raw_client_order_id)
            lifecycle_id = f"{self.baseline_epoch_id}:{client_order_id}"
            error_ts_ns = time.time_ns()
            with self._enqueue_metrics_lock:
                self._enqueue_latency_pending_ns.append(elapsed_ns)
                self._drops += 1
                self._producer_errors += 1
                self._producer_last_error = (
                    "producer:"
                    f"lifecycle_id={lifecycle_id}:"
                    f"client_order_id={client_order_id}:"
                    f"{type(exc).__name__}:{exc}"
                )
                self._producer_last_error_ts_ns = error_ts_ns
            return False

    def _register_or_update(self, item: _PreparedLifecycleWorkItem) -> None:
        if item.lifecycle_id not in self._registered:
            self._bridge.register_lifecycle(
                lifecycle_id=item.lifecycle_id,
                runtime_source="live",
                client_order_id=item.client_order_id,
                symbol=item.symbol,
                side=item.side,
                exchange_order_id=item.exchange_order_id,
            )
            self._registered.add(item.lifecycle_id)
        elif item.exchange_order_id is not None:
            self._bridge.bind_exchange_order_id(
                item.lifecycle_id,
                item.exchange_order_id,
            )

    def _worker_loop(self) -> None:
        self._worker_alive = True
        last_health = time.monotonic()
        thread_clock = getattr(time, "thread_time", time.process_time)
        cpu_started = thread_clock()
        try:
            while True:
                timeout = max(0.05, self._heartbeat_interval_s / 2.0)
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    item = None
                if item is self._sentinel:
                    self._queue.task_done()
                    break
                if isinstance(item, _LifecycleWorkItem):
                    write_started = time.perf_counter_ns()
                    prepared: _PreparedLifecycleWorkItem | None = None
                    try:
                        prepared = self._prepare_work_item(item)
                        self._register_or_update(prepared)
                        result = self._bridge.submit_callback(
                            lifecycle_id=prepared.lifecycle_id,
                            lifecycle=prepared.lifecycle,
                            callback=prepared.callback,
                        )
                        with self._metrics_lock:
                            self._processed += 1
                            self._rows_committed += int(result.row_count)
                            self._last_worker_flush_ts_ns = time.time_ns()
                    except Exception as exc:
                        client_order_id = (
                            prepared.client_order_id
                            if prepared is not None
                            else _diagnostic_text(item.client_order_id)
                        )
                        lifecycle_id = (
                            prepared.lifecycle_id
                            if prepared is not None
                            else f"{self.baseline_epoch_id}:{client_order_id}"
                        )
                        with self._metrics_lock:
                            self._errors += 1
                            self._last_error = (
                                "worker:"
                                f"lifecycle_id={lifecycle_id}:"
                                f"client_order_id={client_order_id}:"
                                f"{type(exc).__name__}:{exc}"
                            )
                            self._last_error_ts_ns = time.time_ns()
                    finally:
                        elapsed_ms = (time.perf_counter_ns() - write_started) / 1_000_000.0
                        with self._metrics_lock:
                            self._write_latency_pending_ms.append(elapsed_ms)
                        self._queue.task_done()
                now = time.monotonic()
                self._update_duration_bound()
                if now - last_health >= self._heartbeat_interval_s:
                    self._worker_thread_cpu_s = max(0.0, thread_clock() - cpu_started)
                    try:
                        self._update_remote_spool_size()
                        core_health = self._writer.health_snapshot()
                        with self._metrics_lock:
                            self._core_health = core_health
                        self._persist_live_health()
                    except Exception as exc:
                        with self._metrics_lock:
                            self._errors += 1
                            self._last_error = f"health_write:{type(exc).__name__}:{exc}"
                            self._last_error_ts_ns = time.time_ns()
                    last_health = now
        finally:
            self._worker_thread_cpu_s = max(0.0, thread_clock() - cpu_started)
            self._worker_alive = False
            try:
                self._persist_live_health()
            except Exception:
                pass

    def _producer_metrics_snapshot(self) -> tuple[deque[int], dict[str, Any]]:
        """Drain producer samples with an O(1) swap, never a history copy.

        The history belongs to the serialized health-reader path. Producer
        callbacks only append to the pending deque and update scalar counters.
        """

        with self._enqueue_metrics_lock:
            pending = self._enqueue_latency_pending_ns
            self._enqueue_latency_pending_ns = deque(maxlen=self._latency_sample_size)
            metrics = {
                "queue_hwm": self._queue_hwm,
                "callbacks_enqueued": self._enqueued,
                "drop_count": self._drops,
                "producer_error_count": self._producer_errors,
                "producer_last_error": self._producer_last_error,
                "producer_last_error_ts_ns": self._producer_last_error_ts_ns,
                "last_enqueue_ts_ns": self._last_enqueue_ts_ns,
                "callbacks_ignored_after_bound": self._callbacks_ignored_after_bound,
            }
        return pending, metrics

    def _health_payload_unlocked(self) -> dict[str, Any]:
        enqueue_pending_ns, producer_metrics = self._producer_metrics_snapshot()
        with self._metrics_lock:
            write_pending_ms = self._write_latency_pending_ms
            self._write_latency_pending_ms = deque(maxlen=self._latency_sample_size)
            worker_error_count = self._errors
            if self._last_error_ts_ns >= producer_metrics["producer_last_error_ts_ns"]:
                last_error = self._last_error
            else:
                last_error = producer_metrics["producer_last_error"]
            metrics = {
                "queue_hwm": producer_metrics["queue_hwm"],
                "callbacks_enqueued": producer_metrics["callbacks_enqueued"],
                "callbacks_processed": self._processed,
                "rows_committed": self._rows_committed,
                "drop_count": producer_metrics["drop_count"],
                "error_count": producer_metrics["producer_error_count"] + worker_error_count,
                "last_error": last_error,
                "last_enqueue_ts_ns": producer_metrics["last_enqueue_ts_ns"],
                "last_worker_flush_ts_ns": self._last_worker_flush_ts_ns,
                "last_health_write_ts_ns": self._last_health_write_ts_ns,
                "worker_cpu_time_s": self._worker_thread_cpu_s,
                "core_health": dict(self._core_health),
                "collection_bound_reached": self._collection_bound_reached,
                "collection_bound_reason": self._collection_bound_reason,
                "collection_stopped_ts_ns": self._collection_stopped_ts_ns,
                "collection_duration_s": self._collection_duration_s,
                "callbacks_ignored_after_bound": producer_metrics["callbacks_ignored_after_bound"],
                "observed_spool_bytes": self._observed_spool_bytes,
                "session_byte_limit_exceeded": self._session_byte_limit_exceeded,
            }
        _extend_rolling_ordered(
            self._enqueue_latency_history_ns,
            self._enqueue_latency_ordered_ns,
            enqueue_pending_ns,
        )
        _extend_rolling_ordered(
            self._write_latency_history_ms,
            self._write_latency_ordered_ms,
            write_pending_ms,
        )
        enqueue_p50_us, enqueue_p99_us, enqueue_max_us = _latency_summary(
            self._enqueue_latency_ordered_ns,
            scale=1_000.0,
        )
        write_p50_ms, write_p99_ms, write_max_ms = _latency_summary(
            self._write_latency_ordered_ms,
            scale=1.0,
        )
        elapsed_s = max(0.0, time.monotonic() - self._collection_started_monotonic)
        payload = {
            "schema_version": ORDER_LIFECYCLE_LIVE_WRITER_V2_HEALTH_VERSION,
            "session_id": self.session_id,
            "baseline_epoch_id": self.baseline_epoch_id,
            "state": (
                "closed"
                if self._closed
                else "bounded_complete"
                if metrics["collection_bound_reached"]
                else "collecting"
            ),
            "storage_profile": self.storage_profile,
            "remote_spool_only": self.storage_profile == BOUNDED_REMOTE_SPOOL,
            "local_admission_complete": False,
            "collection_started_ts_ns": self._collection_started_ts_ns,
            "collection_elapsed_s": elapsed_s,
            "session_max_duration_s": self._session_max_duration_s,
            "session_max_bytes": self._session_max_bytes,
            "epoch_root": str(self.epoch_root) if self.epoch_root is not None else None,
            "queue_capacity": self._queue.maxsize,
            "queue_depth": self._queue.qsize(),
            **metrics,
            "enqueue_latency_p50_us": enqueue_p50_us,
            "enqueue_latency_p99_us": enqueue_p99_us,
            "enqueue_latency_max_us": enqueue_max_us,
            "write_latency_p50_ms": write_p50_ms,
            "write_latency_p99_ms": write_p99_ms,
            "write_latency_max_ms": write_max_ms,
            "process_cpu_time_s": time.process_time(),
            "process_max_rss_mb": _max_rss_mb(),
            "worker_alive": self._worker_alive,
        }
        core = payload["core_health"]
        mechanics_valid = bool(
            payload["drop_count"] == 0
            and payload["error_count"] == 0
            and (payload["worker_alive"] or self._closed)
            and bool(core.get("formal_collection_valid", False))
        )
        payload["remote_spool_valid"] = bool(
            self.storage_profile == BOUNDED_REMOTE_SPOOL
            and mechanics_valid
            and not payload["session_byte_limit_exceeded"]
            and (self._closed or payload["collection_bound_reached"])
        )
        payload["formal_collection_valid"] = bool(
            self.storage_profile == LOCAL_ORICO_REPLAY_ADMISSION and mechanics_valid
        )
        payload["formal_collection_valid_reason"] = (
            "local_profile_writer_valid"
            if payload["formal_collection_valid"]
            else "remote_spool_requires_rsync_and_local_orico_admission"
            if self.storage_profile == BOUNDED_REMOTE_SPOOL
            else "writer_health_invalid"
        )
        return payload

    def _health_payload(self) -> dict[str, Any]:
        with self._health_lock:
            return self._health_payload_unlocked()

    def _persist_live_health(self) -> None:
        payload = self._health_payload()
        payload["last_health_write_ts_ns"] = time.time_ns()
        _atomic_write_json(self.health_path, payload)
        with self._metrics_lock:
            self._last_health_write_ts_ns = int(payload["last_health_write_ts_ns"])

    def health_snapshot(self) -> dict[str, Any]:
        return self._health_payload()

    def close(self, *, drain_timeout_s: float = 5.0) -> dict[str, Any]:
        if self._closed:
            return self.health_snapshot()
        if self.storage_profile == BOUNDED_REMOTE_SPOOL:
            with self._metrics_lock:
                if self._collection_stopped_ts_ns == 0:
                    self._collection_stopped_ts_ns = time.time_ns()
                    self._collection_duration_s = max(
                        0.0,
                        time.monotonic() - self._collection_started_monotonic,
                    )
                    self._collection_bound_reason = "producer_shutdown"
        self._closed = True
        deadline = time.monotonic() + max(0.0, float(drain_timeout_s))
        while self._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.01)
        if self._queue.unfinished_tasks:
            with self._metrics_lock:
                self._errors += 1
                self._last_error = "shutdown_drain_timeout"
                self._last_error_ts_ns = time.time_ns()
        try:
            self._queue.put_nowait(self._sentinel)
        except queue.Full:
            with self._metrics_lock:
                self._errors += 1
                self._last_error = "shutdown_sentinel_queue_full"
                self._last_error_ts_ns = time.time_ns()
        self._thread.join(timeout=max(0.1, float(drain_timeout_s)))
        if self._thread.is_alive():
            with self._metrics_lock:
                self._errors += 1
                self._last_error = "worker_join_timeout"
                self._last_error_ts_ns = time.time_ns()
        core = self._writer.close()
        self._update_remote_spool_size()
        with self._metrics_lock:
            self._core_health = dict(core)
        try:
            self._persist_live_health()
        except Exception:
            pass
        return {**self.health_snapshot(), "core_health": core}
