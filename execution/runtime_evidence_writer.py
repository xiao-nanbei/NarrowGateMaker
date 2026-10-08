"""Single-owner asynchronous writer for live evidence and health artifacts.

The live decision and private-stream callback threads only freeze small payloads
and enqueue them.  A single worker owns every CSV descriptor and every atomic
health publication, so accepted work is written in one global FIFO order.

Queue exhaustion and worker failure are never treated as successful evidence
collection: producers receive an exception and the health snapshot records the
accepted, committed, and uncommitted counts.  In the absence of a worker/I/O
failure, normal shutdown stops admission, drains every accepted item, flushes
and closes all descriptors, and only then returns.

This is an in-process asynchronous writer, not a write-ahead log.  It does not
promise zero loss across process termination, kernel failure, power loss, or
storage failure.  Those failures invalidate the collection and require an
external durable journal if recovery of every admitted item is required.
"""

from __future__ import annotations

import copy
import csv
import json
import os
import queue
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

RUNTIME_EVIDENCE_WRITER_SCHEMA_VERSION = "runtime_evidence_writer.v1"

_IMMUTABLE_CSV_SCALAR_TYPES = (str, bytes, int, float, bool, type(None))


class RuntimeEvidenceWriterError(RuntimeError):
    """Base error for evidence admission, worker, and shutdown failures."""


class RuntimeEvidenceQueueFull(RuntimeEvidenceWriterError):
    """Raised when the bounded queue cannot admit one canonical item."""


class RuntimeEvidenceWorkerFailed(RuntimeEvidenceWriterError):
    """Raised after the single writer worker has failed."""


@dataclass(frozen=True, slots=True)
class _CsvItem:
    sequence: int
    path: Path
    fieldnames: tuple[str, ...]
    values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _JsonItem:
    sequence: int
    path: Path
    payload: dict[str, Any]
    mode: int


@dataclass(frozen=True, slots=True)
class _JsonFactoryItem:
    sequence: int
    path: Path
    payload_factory: Callable[[], Mapping[str, Any]]
    mode: int


@dataclass(frozen=True, slots=True)
class _TaskItem:
    sequence: int
    name: str
    task: Callable[[], None]


@dataclass(frozen=True, slots=True)
class _BarrierItem:
    sequence: int
    committed: threading.Event


_WorkItem = _CsvItem | _JsonItem | _JsonFactoryItem | _TaskItem | _BarrierItem


class RuntimeEvidenceWriter:
    """Bounded FIFO writer shared by ordinary live evidence and health.

    ``enqueue_timeout_s`` bounds the exceptional overload wait.  A full queue
    raises rather than silently dropping or reordering a canonical row.
    """

    def __init__(
        self,
        *,
        queue_capacity: int = 16_384,
        enqueue_timeout_s: float = 0.0,
        thread_name: str = "runtime-evidence-writer",
    ) -> None:
        if int(queue_capacity) <= 0:
            raise ValueError("queue_capacity must be positive")
        if float(enqueue_timeout_s) < 0.0:
            raise ValueError("enqueue_timeout_s must be non-negative")
        self._queue: queue.Queue[_WorkItem] = queue.Queue(maxsize=int(queue_capacity))
        self._enqueue_timeout_s = float(enqueue_timeout_s)
        self._state_lock = threading.Lock()
        self._admission_lock = threading.Lock()
        self._accepting = True
        self._closed = False
        self._next_sequence = 0
        self._last_committed_sequence = 0
        self._queue_high_watermark = 0
        self._accepted_count = 0
        self._committed_count = 0
        self._csv_rows_committed = 0
        self._json_snapshots_committed = 0
        self._tasks_committed = 0
        self._queue_full_count = 0
        self._error_count = 0
        self._fatal_error = ""
        self._last_commit_ts_ns = 0
        self._stop_requested = threading.Event()
        self._handles: dict[Path, tuple[TextIO, Any, tuple[str, ...]]] = {}
        self._thread = threading.Thread(
            target=self._run,
            name=str(thread_name),
            daemon=True,
        )
        self._thread.start()

    def enqueue_csv(self, path: str | Path, payload: Mapping[str, Any]) -> int:
        """Recursively freeze and enqueue one header-compatible CSV payload."""

        normalized_path = Path(path)
        frozen_payload = copy.deepcopy(dict(payload))
        items = tuple(frozen_payload.items())
        fieldnames = tuple(str(key) for key, _value in items)
        if not fieldnames:
            raise ValueError("CSV payload must contain at least one field")
        values = tuple(value for _key, value in items)
        return self._admit(
            lambda sequence: _CsvItem(
                sequence=sequence,
                path=normalized_path,
                fieldnames=fieldnames,
                values=values,
            )
        )

    def enqueue_csv_values(
        self,
        path: str | Path,
        *,
        fieldnames: tuple[str, ...],
        values: tuple[Any, ...],
    ) -> int:
        """Enqueue an already flattened immutable row without dict/deepcopy.

        This is the hot-path API for frozen ``slots`` log rows.  Callers must
        pass only scalar immutable values; mutable containers are rejected so
        a producer cannot alter an admitted row behind the writer thread.
        """

        normalized_path = Path(path)
        normalized_fields = tuple(str(name) for name in fieldnames)
        normalized_values = tuple(values)
        if not normalized_fields:
            raise ValueError("CSV fieldnames must contain at least one field")
        if len(normalized_fields) != len(normalized_values):
            raise ValueError("CSV fieldnames and values must have equal length")
        if any(
            not isinstance(value, _IMMUTABLE_CSV_SCALAR_TYPES)
            for value in normalized_values
        ):
            raise TypeError("flattened CSV values must be immutable scalars")
        return self._admit(
            lambda sequence: _CsvItem(
                sequence=sequence,
                path=normalized_path,
                fieldnames=normalized_fields,
                values=normalized_values,
            )
        )

    def enqueue_json_snapshot(
        self,
        path: str | Path,
        payload: Mapping[str, Any],
        *,
        mode: int = 0o600,
    ) -> int:
        """Enqueue one immutable atomic JSON publication."""

        return self._admit(
            lambda sequence: _JsonItem(
                sequence=sequence,
                path=Path(path),
                payload=copy.deepcopy(
                    {str(key): value for key, value in payload.items()}
                ),
                mode=int(mode),
            )
        )

    def enqueue_json_snapshot_factory(
        self,
        path: str | Path,
        payload_factory: Callable[[], Mapping[str, Any]],
        *,
        mode: int = 0o600,
    ) -> int:
        """Collect and atomically publish one JSON snapshot on the worker.

        Unlike :meth:`enqueue_json_snapshot`, this deliberately defers
        collection until its FIFO position reaches the worker.  The returned
        mapping is recursively frozen before serialization.
        """

        if not callable(payload_factory):
            raise TypeError("payload_factory must be callable")
        return self._admit(
            lambda sequence: _JsonFactoryItem(
                sequence=sequence,
                path=Path(path),
                payload_factory=payload_factory,
                mode=int(mode),
            )
        )

    def enqueue_task(self, name: str, task: Callable[[], None]) -> int:
        """Run an observational callable at its position on the FIFO worker.

        The callable is not a frozen payload.  It must either capture immutable
        values or intentionally observe worker-time state.
        """

        if not callable(task):
            raise TypeError("task must be callable")
        normalized_name = str(name).strip()
        if not normalized_name:
            raise ValueError("task name must be non-empty")
        return self._admit(
            lambda sequence: _TaskItem(
                sequence=sequence,
                name=normalized_name,
                task=task,
            )
        )

    def barrier(self, *, timeout_s: float = 10.0) -> dict[str, Any]:
        """Wait until every item admitted before this call is committed.

        The barrier itself occupies one FIFO sequence.  A timeout invalidates
        admission because the caller can no longer prove the requested commit
        boundary; it does not claim storage durability beyond the writer's
        documented in-process contract.
        """

        timeout = max(0.0, float(timeout_s))
        committed = threading.Event()
        sequence = self._admit(
            lambda admitted_sequence: _BarrierItem(
                sequence=admitted_sequence,
                committed=committed,
            )
        )
        deadline = time.monotonic() + timeout
        while not committed.wait(timeout=min(0.050, max(0.0, deadline - time.monotonic()))):
            self.raise_if_failed()
            if time.monotonic() >= deadline:
                with self._state_lock:
                    self._error_count += 1
                    if not self._fatal_error:
                        self._fatal_error = (
                            f"barrier_timeout:sequence={sequence}:timeout_s={timeout:.6f}"
                        )
                    self._accepting = False
                    summary = self._count_summary_locked()
                raise RuntimeEvidenceWorkerFailed(
                    f"runtime evidence barrier timed out; {summary}"
                )
        self.raise_if_failed()
        health = self.health_snapshot()
        if int(health["last_committed_sequence"]) < sequence:
            raise RuntimeEvidenceWorkerFailed(
                "runtime evidence barrier signaled before commit: "
                f"sequence={sequence} last={health['last_committed_sequence']}"
            )
        return health

    def _admit(self, factory: Callable[[int], _WorkItem]) -> int:
        with self._admission_lock:
            with self._state_lock:
                self._raise_if_unavailable_locked()
                sequence = self._next_sequence + 1
            item = factory(sequence)
            try:
                if self._enqueue_timeout_s == 0.0:
                    self._queue.put_nowait(item)
                else:
                    self._queue.put(item, timeout=self._enqueue_timeout_s)
            except queue.Full as exc:
                with self._state_lock:
                    self._queue_full_count += 1
                    self._error_count += 1
                    self._accepting = False
                raise RuntimeEvidenceQueueFull(
                    "runtime evidence queue is full; canonical collection is invalid"
                ) from exc
            with self._state_lock:
                # Admission is serialized, so this is also the global FIFO
                # identity across decision and callback producer threads.
                self._next_sequence = sequence
                self._accepted_count += 1
                self._queue_high_watermark = max(
                    self._queue_high_watermark,
                    self._queue.qsize(),
                )
            return sequence

    def _raise_if_unavailable_locked(self) -> None:
        if self._fatal_error:
            raise RuntimeEvidenceWorkerFailed(
                f"{self._fatal_error}; {self._count_summary_locked()}"
            )
        if self._queue_full_count:
            raise RuntimeEvidenceQueueFull(
                "runtime evidence queue was exhausted; canonical collection "
                f"is invalid; {self._count_summary_locked()}"
            )
        if not self._accepting or self._closed:
            raise RuntimeEvidenceWriterError("runtime evidence writer is closed")

    def _count_summary_locked(self) -> str:
        return (
            f"accepted={self._accepted_count} "
            f"committed={self._committed_count} "
            f"uncommitted={self._accepted_count - self._committed_count}"
        )

    def _run(self) -> None:
        expected_sequence = 1
        try:
            while True:
                try:
                    item = self._queue.get(timeout=0.050)
                except queue.Empty:
                    if self._stop_requested.is_set():
                        return
                    continue
                try:
                    # The producer holds _admission_lock until it has recorded
                    # accepted_count/next_sequence. Acquiring and immediately
                    # releasing it prevents a fast worker from committing an
                    # item before its admission accounting becomes visible.
                    with self._admission_lock:
                        pass
                    if item.sequence != expected_sequence:
                        raise RuntimeError(
                            "runtime evidence FIFO sequence mismatch: "
                            f"expected={expected_sequence} actual={item.sequence}"
                        )
                    if isinstance(item, _CsvItem):
                        self._write_csv(item)
                        kind = "csv"
                    elif isinstance(item, (_JsonItem, _JsonFactoryItem)):
                        self._write_json(item)
                        kind = "json"
                    elif isinstance(item, _TaskItem):
                        item.task()
                        kind = "task"
                    else:
                        kind = "task"
                    with self._state_lock:
                        self._last_committed_sequence = item.sequence
                        self._committed_count += 1
                        self._last_commit_ts_ns = time.time_ns()
                        if kind == "csv":
                            self._csv_rows_committed += 1
                        elif kind == "json":
                            self._json_snapshots_committed += 1
                        else:
                            self._tasks_committed += 1
                    if isinstance(item, _BarrierItem):
                        item.committed.set()
                    expected_sequence += 1
                finally:
                    self._queue.task_done()
        except BaseException as exc:
            with self._state_lock:
                self._error_count += 1
                self._fatal_error = f"{type(exc).__name__}:{exc}"
                self._accepting = False
            # Do not leave close() blocked behind work that can no longer be
            # written.  These rows are explicitly invalidated by fatal_error.
            while True:
                try:
                    pending = self._queue.get_nowait()
                except queue.Empty:
                    break
                else:
                    if isinstance(pending, _BarrierItem):
                        pending.committed.set()
                    self._queue.task_done()
        finally:
            try:
                self._close_handles()
            except BaseException as exc:
                with self._state_lock:
                    self._error_count += 1
                    if not self._fatal_error:
                        self._fatal_error = f"{type(exc).__name__}:{exc}"
                    self._accepting = False

    def _write_csv(self, item: _CsvItem) -> None:
        path = item.path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise ValueError(f"evidence CSV path must not be a symlink: {path}")
        existing = self._handles.get(path)
        if existing is None:
            handle = path.open("a", newline="", encoding="utf-8")
            writer = csv.writer(handle)
            existing = (handle, writer, item.fieldnames)
            self._handles[path] = existing
        handle, writer, fieldnames = existing
        if fieldnames != item.fieldnames:
            raise ValueError(f"evidence CSV schema changed in-process: {path}")
        writer.writerow(item.values)
        # Preserve the old per-row userspace visibility without imposing the
        # old open/write/close cost on the decision thread. Durability is still
        # governed by the same process-shutdown contract, not per-row fsync.
        handle.flush()

    @staticmethod
    def _write_json(item: _JsonItem | _JsonFactoryItem) -> None:
        path = item.path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise ValueError(f"health path must not be a symlink: {path}")
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{item.sequence}.tmp")
        try:
            if isinstance(item, _JsonFactoryItem):
                generated = item.payload_factory()
                if not isinstance(generated, Mapping):
                    raise TypeError("JSON payload factory must return a mapping")
                payload = copy.deepcopy(dict(generated))
            else:
                payload = item.payload
            temporary.write_text(
                json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            os.chmod(temporary, item.mode)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def _close_handles(self) -> None:
        first_error: BaseException | None = None
        for handle, _writer, _fieldnames in self._handles.values():
            try:
                handle.flush()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
            try:
                handle.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        self._handles.clear()
        if first_error is not None:
            raise first_error

    def health_snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            return {
                "schema_version": RUNTIME_EVIDENCE_WRITER_SCHEMA_VERSION,
                "accepting": bool(self._accepting),
                "closed": bool(self._closed),
                "worker_alive": bool(self._thread.is_alive()),
                "queue_capacity": int(self._queue.maxsize),
                "queue_depth": int(self._queue.qsize()),
                "queue_high_watermark": int(self._queue_high_watermark),
                "accepted_count": int(self._accepted_count),
                "committed_count": int(self._committed_count),
                "uncommitted_count": int(
                    self._accepted_count - self._committed_count
                ),
                "csv_rows_committed": int(self._csv_rows_committed),
                "json_snapshots_committed": int(self._json_snapshots_committed),
                "tasks_committed": int(self._tasks_committed),
                "queue_full_count": int(self._queue_full_count),
                "error_count": int(self._error_count),
                "fatal_error": str(self._fatal_error),
                "last_committed_sequence": int(self._last_committed_sequence),
                "last_commit_ts_ns": int(self._last_commit_ts_ns),
                "valid": not self._fatal_error and self._queue_full_count == 0,
            }

    def raise_if_failed(self) -> None:
        """Make an asynchronous worker failure visible to the live loop."""

        with self._state_lock:
            if self._fatal_error:
                raise RuntimeEvidenceWorkerFailed(
                    f"{self._fatal_error}; {self._count_summary_locked()}"
                )
            if self._queue_full_count:
                raise RuntimeEvidenceQueueFull(
                    "runtime evidence queue was exhausted; canonical collection "
                    f"is invalid; {self._count_summary_locked()}"
                )

    def close(self, *, drain_timeout_s: float = 10.0) -> dict[str, Any]:
        """Drain accepted work and return only after a clean normal shutdown.

        A worker/I/O fatal or queue exhaustion makes the collection invalid;
        in that case the exception and :meth:`health_snapshot` expose how much
        accepted work was committed and how much remains uncommitted.
        """

        timeout_s = max(0.0, float(drain_timeout_s))
        deadline = time.monotonic() + timeout_s
        already_closed = False
        with self._admission_lock:
            with self._state_lock:
                if self._closed:
                    already_closed = True
                else:
                    self._accepting = False
            if not already_closed:
                self._stop_requested.set()
                with self._state_lock:
                    self._closed = True

        def shutdown_invalid(health: Mapping[str, Any]) -> bool:
            return bool(
                health["worker_alive"]
                or not health["valid"]
                or int(health["queue_depth"]) != 0
                or int(health["uncommitted_count"]) != 0
                or int(health["last_committed_sequence"])
                != int(health["accepted_count"])
            )

        if already_closed:
            health = self.health_snapshot()
            if shutdown_invalid(health):
                raise RuntimeEvidenceWriterError(
                    "runtime evidence writer did not close cleanly: "
                    f"{health['fatal_error'] or 'queue_full'}; "
                    f"accepted={health['accepted_count']} "
                    f"committed={health['committed_count']} "
                    f"uncommitted={health['uncommitted_count']}"
                )
            return health

        self._thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if self._thread.is_alive():
            with self._state_lock:
                self._error_count += 1
                if not self._fatal_error:
                    self._fatal_error = "shutdown_worker_timeout"
        health = self.health_snapshot()
        if shutdown_invalid(health):
            raise RuntimeEvidenceWriterError(
                "runtime evidence writer did not close cleanly: "
                f"{health['fatal_error'] or 'queue_full'}; "
                f"accepted={health['accepted_count']} "
                f"committed={health['committed_count']} "
                f"uncommitted={health['uncommitted_count']}"
            )
        return health
