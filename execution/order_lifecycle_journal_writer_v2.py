"""Atomic mechanics-only persistence for order lifecycle journal v2.

The journal emitter deliberately owns only validation and batch construction.
This module supplies the durable boundary: an immutable callback part is
published first, writer health is updated second, and the lifecycle cursor is
advanced last.  Recovery derives cursors from validated parts, so a process
failure at any boundary can be replayed without dropping or duplicating an
event.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    OrderLifecycleJournalV2Batch,
    OrderLifecycleJournalV2BatchEmitter,
    OrderLifecycleJournalV2Cursor,
    OrderLifecycleJournalV2SourceCallback,
    validate_order_lifecycle_journal_v2_payload,
)

ORDER_LIFECYCLE_JOURNAL_WRITER_V2_SCHEMA_VERSION = "order_lifecycle_journal_writer.v2"
ORDER_LIFECYCLE_JOURNAL_WRITER_V2_HEALTH_VERSION = "order_lifecycle_journal_writer_health.v2"
ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION = "order_lifecycle_journal_part.v2"
ORDER_LIFECYCLE_JOURNAL_WRITER_V2_IDENTITY_VERSION = "order_lifecycle_journal_writer_identity.v2"

_SAFE_SESSION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_FORBIDDEN_ECONOMIC_FRAGMENTS = ("pnl", "reward", "markout")
_EXCHANGE_TERMINAL_REASONS = frozenset(
    {
        "cancel_ack",
        "cancel_ack_reconciled",
        "expired",
        "filled_before_cancel_ack",
        "full_fill",
        "rejected",
    }
)
_LOCAL_SHUTDOWN_REASONS = frozenset(
    {
        "administrative_cancel",
        "local_shutdown_cancel",
        "local_shutdown_unknown_ack",
        "shutdown",
    }
)

JournalStorageFormat = Literal["parquet", "jsonl"]
FaultInjector = Callable[[str, Mapping[str, Any]], None]


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    before_replace: Callable[[], None] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
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
        if before_replace is not None:
            before_replace()
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _assert_mechanics_only(value: Any, *, path: str = "root") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower()
            if any(fragment in normalized for fragment in _FORBIDDEN_ECONOMIC_FRAGMENTS):
                raise ValueError(f"economic field is forbidden in lifecycle writer: {path}.{key}")
            _assert_mechanics_only(nested, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, nested in enumerate(value):
            _assert_mechanics_only(nested, path=f"{path}[{index}]")


def _journal_schema_sha256() -> str:
    return _canonical_sha256(
        {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
            "columns": list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS),
        }
    )


def _cursor_from_mapping(value: Mapping[str, object]) -> OrderLifecycleJournalV2Cursor:
    expected = (
        "schema_version",
        "lifecycle_id",
        "client_order_id",
        "last_emitted_sequence",
        "last_event_id",
    )
    if set(value) != set(expected):
        raise ValueError("lifecycle cursor checkpoint schema mismatch")
    return OrderLifecycleJournalV2Cursor.from_checkpoint({key: value[key] for key in expected})


def _normalize_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if set(payload) != set(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS):
        raise ValueError("persisted lifecycle journal payload schema mismatch")
    normalized = {column: payload[column] for column in ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS}
    validate_order_lifecycle_journal_v2_payload(normalized)
    return normalized


def _validate_terminal_semantics(payloads: Sequence[Mapping[str, Any]]) -> None:
    local_censor_positions: list[int] = []
    for index, payload in enumerate(payloads):
        event = str(payload["lifecycle_event"])
        observation = str(payload["terminal_observation"])
        exchange_reason = str(payload["exchange_terminal_reason"])
        local_reason = str(payload["local_censor_reason"])
        if observation == "EXCHANGE_TERMINAL":
            if exchange_reason not in _EXCHANGE_TERMINAL_REASONS or local_reason:
                raise ValueError("unsupported exchange terminal reason")
        elif observation == "LOCAL_SHUTDOWN_CENSOR":
            if local_reason not in _LOCAL_SHUTDOWN_REASONS or exchange_reason:
                raise ValueError("unsupported local shutdown censor reason")
            if event not in {
                "local_shutdown_censor",
                "submit_ack_unknown_censored",
            }:
                raise ValueError("local shutdown censor requires an explicit lifecycle event")
            local_censor_positions.append(index)
        elif observation != "NONE":
            raise ValueError("unsupported terminal observation")
    if local_censor_positions and local_censor_positions != [len(payloads) - 1]:
        raise ValueError("local_shutdown_censor must be the final event in its callback batch")


def _payloads_from_batch(batch: OrderLifecycleJournalV2Batch) -> tuple[dict[str, Any], ...]:
    payloads = tuple(_normalize_payload(payload) for payload in batch.payloads())
    _validate_terminal_semantics(payloads)
    return payloads


def _pyarrow_schema():
    import pyarrow as pa

    string_columns = {
        "schema_version",
        "event_id",
        "lifecycle_id",
        "runtime_source",
        "source_callback_id",
        "source_callback_type",
        "client_order_id",
        "exchange_order_id",
        "symbol",
        "side",
        "lifecycle_event",
        "phase_before",
        "phase_after",
        "event_reason",
        "observation_origin",
        "left_truncation_reason",
        "terminal_observation",
        "exchange_terminal_reason",
        "local_censor_reason",
        "visible_exposure_invalid_reason",
        "exchange_exposure_invalid_reason",
    }
    integer_columns = {
        "source_callback_event_ordinal",
        "source_callback_event_count",
        "source_callback_received_ts_ns",
        "source_callback_exchange_ts_ns",
        "lifecycle_sequence",
        "event_visibility_ts_ns",
        "event_exchange_ts_ns",
    }
    boolean_columns = {
        "source_callback_exchange_clock_valid",
        "event_exchange_clock_valid",
        "left_truncated",
        "fill_risk_active_after",
        "visible_exposure_valid",
        "visible_exposure_complete",
        "exchange_exposure_valid",
        "exchange_exposure_complete",
    }
    fields = []
    for column in ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS:
        if column in string_columns:
            fields.append(pa.field(column, pa.string(), nullable=column == "exchange_order_id"))
        elif column in integer_columns:
            fields.append(
                pa.field(
                    column,
                    pa.int64(),
                    nullable=column in {"source_callback_exchange_ts_ns", "event_exchange_ts_ns"},
                )
            )
        elif column in boolean_columns:
            fields.append(pa.field(column, pa.bool_(), nullable=column == "fill_risk_active_after"))
        else:
            fields.append(
                pa.field(
                    column,
                    pa.float64(),
                    nullable=column == "quantity_time_exposure_exchange_btc_s",
                )
            )
    return pa.schema(fields)


@dataclass(frozen=True, slots=True)
class LifecycleJournalCommitResult:
    status: Literal["committed", "duplicate", "noop", "quarantined"]
    batch_id: str
    row_count: int
    checkpoint: Mapping[str, object]
    reason: str = ""


@dataclass(slots=True)
class _LifecycleBinding:
    lifecycle_id: str
    runtime_source: str
    client_order_id: str
    symbol: str
    side: str
    exchange_order_id: str | int | None
    orphan_adoption: bool
    left_truncation_reason: str


class OrderLifecycleJournalWriterV2:
    """Synchronous callback-atomic writer with restart reconciliation."""

    def __init__(
        self,
        root: str | Path,
        *,
        session_id: str,
        runtime_identity: Mapping[str, Any],
        storage_format: JournalStorageFormat = "parquet",
        initial_active_order_ids: Sequence[str] = (),
        heartbeat_interval_s: float = 5.0,
        start_heartbeat: bool = True,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        if not _SAFE_SESSION_ID.fullmatch(str(session_id)):
            raise ValueError("unsafe lifecycle journal session id")
        if storage_format not in {"parquet", "jsonl"}:
            raise ValueError("lifecycle journal storage format must be parquet or jsonl")
        if not math.isfinite(float(heartbeat_interval_s)) or heartbeat_interval_s <= 0:
            raise ValueError("lifecycle journal heartbeat interval must be positive")
        _assert_mechanics_only(runtime_identity, path="runtime_identity")

        self.root = Path(root).expanduser().resolve()
        self.session_id = str(session_id)
        self.session_root = self.root / f"session-{self.session_id}"
        self.parts_root = self.session_root / "parts"
        self.cursors_root = self.session_root / "cursors"
        self.identity_path = self.session_root / "runtime_identity.json"
        self.health_path = self.session_root / "health.json"
        self.storage_format: JournalStorageFormat = storage_format
        self.heartbeat_interval_s = float(heartbeat_interval_s)
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._closed = False
        self._lock_handle = None
        self._heartbeat_thread: threading.Thread | None = None

        self.session_root.mkdir(parents=True, exist_ok=True)
        self.parts_root.mkdir(parents=True, exist_ok=True)
        self.cursors_root.mkdir(parents=True, exist_ok=True)
        self._acquire_process_lock()
        try:
            self._runtime_identity = dict(runtime_identity)
            self._runtime_identity_sha256 = _canonical_sha256(self._runtime_identity)
            self._persist_or_validate_identity()
            prior_health = self._load_prior_health()
            initial_ids = {
                str(value).strip() for value in initial_active_order_ids if str(value).strip()
            }
            prior_initial = set(prior_health.get("quarantine_order_ids", []))
            self._quarantine_order_ids = prior_initial | initial_ids
            self._excluded_lifecycle_ids = set(prior_health.get("excluded_lifecycle_ids", []))
            self._excluded_client_order_ids = set(prior_health.get("excluded_client_order_ids", []))
            self._state = "quarantine" if self._quarantine_order_ids else "collecting"
            self._restart_count = int(prior_health.get("restart_count", -1)) + 1
            self._callbacks_quarantined = int(prior_health.get("callbacks_quarantined", 0))
            self._rows_dropped = int(prior_health.get("rows_dropped", 0))
            self._error_count = int(prior_health.get("error_count", 0))
            self._last_error = str(prior_health.get("last_error", ""))
            self._last_heartbeat_ts_ns = 0
            self._last_flush_ts_ns = int(prior_health.get("last_flush_ts_ns", 0))
            self._last_flush_batch_id = str(prior_health.get("last_flush_batch_id", ""))
            self._cursors: dict[str, OrderLifecycleJournalV2Cursor] = {}
            self._records: dict[str, dict[str, Any]] = {}
            self._committed_event_ids: set[str] = set()
            self._censored_lifecycle_ids: set[str] = set()
            self._recovery_required = False
            self._cleanup_partial_files()
            with self._lock:
                self._recover_locked()
            if start_heartbeat:
                self._heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop,
                    name=f"lifecycle-journal-v2-{self.session_id}",
                    daemon=True,
                )
                self._heartbeat_thread.start()
        except Exception:
            self._release_process_lock()
            raise

    def __enter__(self) -> OrderLifecycleJournalWriterV2:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _acquire_process_lock(self) -> None:
        lock_path = self.session_root / "writer.lock"
        self._lock_handle = lock_path.open("a+")
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._lock_handle.close()
            self._lock_handle = None
            raise RuntimeError("lifecycle journal session already has an active writer") from exc

    def _release_process_lock(self) -> None:
        if self._lock_handle is None:
            return
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._lock_handle.close()
            self._lock_handle = None

    def _persist_or_validate_identity(self) -> None:
        identity = {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_WRITER_V2_IDENTITY_VERSION,
            "journal_schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
            "journal_schema_sha256": _journal_schema_sha256(),
            "storage_format": self.storage_format,
            "runtime_identity": self._runtime_identity,
            "runtime_identity_sha256": self._runtime_identity_sha256,
            "economic_outcomes_read": False,
            "q90_action_authorized": False,
        }
        if self.identity_path.exists():
            if _read_json(self.identity_path) != identity:
                raise ValueError("lifecycle journal runtime identity mismatch on restart")
            return
        _atomic_write_json(self.identity_path, identity)

    def _load_prior_health(self) -> dict[str, Any]:
        if not self.health_path.exists():
            return {}
        health = _read_json(self.health_path)
        if health.get("schema_version") != ORDER_LIFECYCLE_JOURNAL_WRITER_V2_HEALTH_VERSION:
            raise ValueError("unsupported lifecycle journal health schema")
        if health.get("runtime_identity_sha256") != self._runtime_identity_sha256:
            raise ValueError("lifecycle journal health identity mismatch")
        if health.get("storage_format") != self.storage_format:
            raise ValueError("lifecycle journal health storage format mismatch")
        return health

    def _cleanup_partial_files(self) -> None:
        for root in (self.session_root, self.parts_root, self.cursors_root):
            for path in root.glob(".*.partial-*"):
                path.unlink(missing_ok=True)

    def set_fault_injector(self, fault_injector: FaultInjector | None) -> None:
        with self._lock:
            self._fault_injector = fault_injector

    def _inject(self, point: str, context: Mapping[str, Any]) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point, context)

    @property
    def collecting(self) -> bool:
        with self._lock:
            return self._state == "collecting" and not self._closed

    def health_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._health_payload_locked())

    def cursor_for(
        self,
        *,
        lifecycle_id: str,
        client_order_id: str,
    ) -> OrderLifecycleJournalV2Cursor:
        with self._lock:
            cursor = self._cursors.get(str(lifecycle_id))
            if cursor is None:
                return OrderLifecycleJournalV2Cursor(
                    lifecycle_id=str(lifecycle_id),
                    client_order_id=str(client_order_id),
                )
            if cursor.client_order_id != str(client_order_id):
                raise ValueError("durable lifecycle cursor client order id mismatch")
            return cursor

    def collection_status(
        self,
        *,
        lifecycle_id: str,
        client_order_id: str,
    ) -> tuple[bool, str]:
        with self._lock:
            if self._closed:
                raise RuntimeError("lifecycle journal writer is closed")
            lifecycle = str(lifecycle_id)
            client = str(client_order_id)
            if self._state == "quarantine":
                self._excluded_lifecycle_ids.add(lifecycle)
                self._excluded_client_order_ids.add(client)
                self._callbacks_quarantined += 1
                self._persist_health_locked()
                return False, "hot_start_quarantine"
            if (
                lifecycle in self._excluded_lifecycle_ids
                or client in self._excluded_client_order_ids
            ):
                self._callbacks_quarantined += 1
                self._persist_health_locked()
                return False, "pre_cutover_lifecycle"
            if lifecycle in self._censored_lifecycle_ids:
                raise ValueError("lifecycle received an event after local_shutdown_censor")
            return True, ""

    def observe_quarantined_exchange_terminal(
        self,
        client_order_id: str,
        *,
        terminal_reason: str,
        lifecycle_id: str | None = None,
    ) -> None:
        reason = str(terminal_reason)
        if reason not in _EXCHANGE_TERMINAL_REASONS:
            if reason in _LOCAL_SHUTDOWN_REASONS:
                raise ValueError("local_shutdown_censor cannot release a hot-start exchange order")
            raise ValueError("unsupported hot-start exchange terminal reason")
        client = str(client_order_id)
        with self._lock:
            self._quarantine_order_ids.discard(client)
            self._excluded_client_order_ids.discard(client)
            if lifecycle_id is not None:
                self._excluded_lifecycle_ids.discard(str(lifecycle_id))
            if not self._quarantine_order_ids:
                self._state = "collecting"
            self._persist_health_locked()

    def report_drop(self, reason: str) -> None:
        with self._lock:
            self._rows_dropped += 1
            self._last_error = str(reason)
            self._persist_health_locked()

    def report_error(self, reason: str) -> None:
        with self._lock:
            self._error_count += 1
            self._last_error = str(reason)
            self._persist_health_locked()

    def heartbeat(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._persist_health_locked()

    def reconcile(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("lifecycle journal writer is closed")
            self._recover_locked()
            self._recovery_required = False

    def commit_batch(self, batch: OrderLifecycleJournalV2Batch) -> LifecycleJournalCommitResult:
        payloads = _payloads_from_batch(batch)
        if not payloads:
            return LifecycleJournalCommitResult(
                status="noop",
                batch_id="",
                row_count=0,
                checkpoint=dict(batch.checkpoint),
                reason="no_unseen_events",
            )
        with self._lock:
            if self._closed:
                raise RuntimeError("lifecycle journal writer is closed")
            try:
                if self._recovery_required:
                    self._recover_locked()
                    self._recovery_required = False
                result = self._commit_batch_locked(batch=batch, payloads=payloads)
                self._recovery_required = False
                return result
            except Exception as exc:
                self._recovery_required = True
                self._error_count += 1
                self._last_error = f"{type(exc).__name__}:{exc}"
                try:
                    self._persist_health_locked()
                except Exception:
                    pass
                raise

    def _commit_batch_locked(
        self,
        *,
        batch: OrderLifecycleJournalV2Batch,
        payloads: tuple[dict[str, Any], ...],
    ) -> LifecycleJournalCommitResult:
        first = payloads[0]
        lifecycle_id = str(first["lifecycle_id"])
        client_order_id = str(first["client_order_id"])
        if any(str(row["lifecycle_id"]) != lifecycle_id for row in payloads):
            raise ValueError("callback batch spans multiple lifecycle ids")
        if any(str(row["client_order_id"]) != client_order_id for row in payloads):
            raise ValueError("callback batch spans multiple client order ids")

        batch_id = _canonical_sha256(
            {
                "schema_version": ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION,
                "lifecycle_id": lifecycle_id,
                "event_ids": [row["event_id"] for row in payloads],
                "source_callback_id": first["source_callback_id"],
                "checkpoint_after": dict(batch.checkpoint),
            }
        )
        if batch_id in self._records:
            return LifecycleJournalCommitResult(
                status="duplicate",
                batch_id=batch_id,
                row_count=len(payloads),
                checkpoint=dict(batch.checkpoint),
                reason="batch_already_committed",
            )
        if all(str(row["event_id"]) in self._committed_event_ids for row in payloads):
            return LifecycleJournalCommitResult(
                status="duplicate",
                batch_id=batch_id,
                row_count=len(payloads),
                checkpoint=dict(batch.checkpoint),
                reason="events_already_committed",
            )
        if any(str(row["event_id"]) in self._committed_event_ids for row in payloads):
            raise ValueError("callback batch partially overlaps committed lifecycle events")

        cursor_before = self.cursor_for(
            lifecycle_id=lifecycle_id,
            client_order_id=client_order_id,
        )
        expected_first = cursor_before.last_emitted_sequence + 1
        if int(first["lifecycle_sequence"]) != expected_first:
            raise ValueError("callback batch does not continue the durable lifecycle cursor")
        if lifecycle_id in self._censored_lifecycle_ids:
            raise ValueError("event follows a durable local_shutdown_censor")

        record = self._publish_part_locked(
            batch_id=batch_id,
            payloads=payloads,
            checkpoint_before=cursor_before.checkpoint(),
            checkpoint_after=dict(batch.checkpoint),
        )

        prospective_records = dict(self._records)
        prospective_records[batch_id] = record
        prospective = self._derived_storage_state(prospective_records)
        health_payload = self._health_payload_locked(
            records=prospective_records,
            derived=prospective,
            last_flush_ts_ns=int(record["committed_ts_ns"]),
            last_flush_batch_id=batch_id,
        )
        self._write_health_payload_locked(
            health_payload,
            fault_context={"operation": "batch_commit", "batch_id": batch_id},
        )
        self._records = prospective_records
        self._committed_event_ids = set(prospective["event_ids"])
        self._censored_lifecycle_ids = set(prospective["censored_lifecycle_ids"])
        self._last_flush_ts_ns = int(record["committed_ts_ns"])
        self._last_flush_batch_id = batch_id
        self._inject("after_health_replace", {"batch_id": batch_id})

        next_cursor = _cursor_from_mapping(batch.checkpoint)
        self._persist_cursor_locked(next_cursor, batch_id=batch_id, inject=True)
        self._cursors[lifecycle_id] = next_cursor
        self._inject("after_cursor_replace", {"batch_id": batch_id})
        return LifecycleJournalCommitResult(
            status="committed",
            batch_id=batch_id,
            row_count=len(payloads),
            checkpoint=dict(batch.checkpoint),
        )

    def _publish_part_locked(
        self,
        *,
        batch_id: str,
        payloads: tuple[dict[str, Any], ...],
        checkpoint_before: Mapping[str, object],
        checkpoint_after: Mapping[str, object],
    ) -> dict[str, Any]:
        suffix = ".parquet" if self.storage_format == "parquet" else ".jsonl"
        data_path = self.parts_root / f"part-{batch_id}{suffix}"
        manifest_path = self.parts_root / f"part-{batch_id}.manifest.json"
        context = {"batch_id": batch_id, "operation": "batch_commit"}

        if not data_path.exists():
            temporary = data_path.with_name(
                f".{data_path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
            )
            try:
                if self.storage_format == "parquet":
                    import pyarrow as pa
                    import pyarrow.parquet as pq

                    table = pa.Table.from_pylist(list(payloads), schema=_pyarrow_schema())
                    pq.write_table(table, temporary, compression="zstd")
                    with temporary.open("rb") as handle:
                        os.fsync(handle.fileno())
                else:
                    with temporary.open("w", encoding="utf-8") as handle:
                        for payload in payloads:
                            handle.write(
                                json.dumps(
                                    payload,
                                    sort_keys=False,
                                    separators=(",", ":"),
                                    ensure_ascii=True,
                                    allow_nan=False,
                                )
                            )
                            handle.write("\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                self._inject("before_payload_replace", context)
                os.replace(temporary, data_path)
                _fsync_directory(self.parts_root)
                self._inject("after_payload_replace", context)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
        persisted_payloads = self._read_payloads(data_path, self.storage_format)
        if persisted_payloads != payloads:
            raise ValueError("existing lifecycle journal part payload disagrees with callback")

        if manifest_path.exists():
            record = _read_json(manifest_path)
            self._validate_part_record(record, expected_batch_id=batch_id)
            return record

        committed_ts_ns = time.time_ns()
        record = {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION,
            "batch_id": batch_id,
            "runtime_identity_sha256": self._runtime_identity_sha256,
            "journal_schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
            "journal_schema_sha256": _journal_schema_sha256(),
            "storage_format": self.storage_format,
            "data_file": data_path.name,
            "data_sha256": _sha256_file(data_path),
            "row_count": len(payloads),
            "lifecycle_id": payloads[0]["lifecycle_id"],
            "client_order_id": payloads[0]["client_order_id"],
            "source_callback_id": payloads[0]["source_callback_id"],
            "source_callback_type": payloads[0]["source_callback_type"],
            "first_lifecycle_sequence": payloads[0]["lifecycle_sequence"],
            "last_lifecycle_sequence": payloads[-1]["lifecycle_sequence"],
            "first_event_id": payloads[0]["event_id"],
            "last_event_id": payloads[-1]["event_id"],
            "event_ids": [payload["event_id"] for payload in payloads],
            "checkpoint_before": dict(checkpoint_before),
            "checkpoint_after": dict(checkpoint_after),
            "contains_local_shutdown_censor": any(
                payload["terminal_observation"] == "LOCAL_SHUTDOWN_CENSOR" for payload in payloads
            ),
            "committed_ts_ns": committed_ts_ns,
            "economic_outcomes_read": False,
        }
        _atomic_write_json(
            manifest_path,
            record,
            before_replace=lambda: self._inject("before_manifest_replace", context),
        )
        self._inject("after_manifest_replace", context)
        return record

    def _read_payloads(
        self,
        data_path: Path,
        storage_format: JournalStorageFormat,
    ) -> tuple[dict[str, Any], ...]:
        if storage_format == "parquet":
            import pyarrow.parquet as pq

            raw_rows = pq.read_table(data_path).to_pylist()
        else:
            raw_rows = []
            with data_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        raw_rows.append(json.loads(line))
        payloads = tuple(_normalize_payload(row) for row in raw_rows)
        _validate_terminal_semantics(payloads)
        return payloads

    def _validate_part_record(
        self,
        record: Mapping[str, Any],
        *,
        expected_batch_id: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        required = {
            "schema_version",
            "batch_id",
            "runtime_identity_sha256",
            "journal_schema_version",
            "journal_schema_sha256",
            "storage_format",
            "data_file",
            "data_sha256",
            "row_count",
            "lifecycle_id",
            "client_order_id",
            "source_callback_id",
            "source_callback_type",
            "first_lifecycle_sequence",
            "last_lifecycle_sequence",
            "first_event_id",
            "last_event_id",
            "event_ids",
            "checkpoint_before",
            "checkpoint_after",
            "contains_local_shutdown_censor",
            "committed_ts_ns",
            "economic_outcomes_read",
        }
        if set(record) != required:
            raise ValueError("lifecycle journal part manifest schema mismatch")
        if record["schema_version"] != ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION:
            raise ValueError("unsupported lifecycle journal part manifest")
        batch_id = str(record["batch_id"])
        if expected_batch_id is not None and batch_id != expected_batch_id:
            raise ValueError("lifecycle journal part batch id mismatch")
        if record["runtime_identity_sha256"] != self._runtime_identity_sha256:
            raise ValueError("lifecycle journal part runtime identity mismatch")
        if record["journal_schema_version"] != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
            raise ValueError("lifecycle journal part schema version mismatch")
        if record["journal_schema_sha256"] != _journal_schema_sha256():
            raise ValueError("lifecycle journal part schema identity mismatch")
        if record["storage_format"] != self.storage_format:
            raise ValueError("lifecycle journal part storage format mismatch")
        if bool(record["economic_outcomes_read"]):
            raise ValueError("lifecycle journal part cannot read economic outcomes")
        data_path = self.parts_root / str(record["data_file"])
        suffix = ".parquet" if self.storage_format == "parquet" else ".jsonl"
        if data_path.name != f"part-{batch_id}{suffix}":
            raise ValueError("lifecycle journal part data filename mismatch")
        if data_path.parent != self.parts_root or not data_path.is_file():
            raise ValueError("lifecycle journal part data file is missing")
        if _sha256_file(data_path) != str(record["data_sha256"]):
            raise ValueError("lifecycle journal part SHA256 mismatch")
        payloads = self._read_payloads(data_path, self.storage_format)
        if len(payloads) != int(record["row_count"]):
            raise ValueError("lifecycle journal part row count mismatch")
        if not payloads:
            raise ValueError("lifecycle journal part cannot be empty")
        if [row["event_id"] for row in payloads] != list(record["event_ids"]):
            raise ValueError("lifecycle journal part event identity mismatch")
        if (
            payloads[0]["lifecycle_id"] != record["lifecycle_id"]
            or payloads[0]["client_order_id"] != record["client_order_id"]
            or payloads[0]["source_callback_id"] != record["source_callback_id"]
            or payloads[0]["source_callback_type"] != record["source_callback_type"]
            or payloads[0]["lifecycle_sequence"] != int(record["first_lifecycle_sequence"])
            or payloads[-1]["lifecycle_sequence"] != int(record["last_lifecycle_sequence"])
            or payloads[0]["event_id"] != record["first_event_id"]
            or payloads[-1]["event_id"] != record["last_event_id"]
        ):
            raise ValueError("lifecycle journal part manifest disagrees with payload")
        checkpoint_after = _cursor_from_mapping(record["checkpoint_after"])
        if (
            checkpoint_after.lifecycle_id != record["lifecycle_id"]
            or checkpoint_after.client_order_id != record["client_order_id"]
            or checkpoint_after.last_emitted_sequence != int(record["last_lifecycle_sequence"])
            or checkpoint_after.last_event_id != record["last_event_id"]
        ):
            raise ValueError("lifecycle journal part checkpoint-after mismatch")
        _cursor_from_mapping(record["checkpoint_before"])
        expected_batch_id = _canonical_sha256(
            {
                "schema_version": ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION,
                "lifecycle_id": payloads[0]["lifecycle_id"],
                "event_ids": [payload["event_id"] for payload in payloads],
                "source_callback_id": payloads[0]["source_callback_id"],
                "checkpoint_after": dict(record["checkpoint_after"]),
            }
        )
        if batch_id != expected_batch_id:
            raise ValueError("lifecycle journal part content-addressed batch id mismatch")
        if int(record["committed_ts_ns"]) <= 0:
            raise ValueError("lifecycle journal part committed timestamp must be positive")
        has_censor = any(
            payload["terminal_observation"] == "LOCAL_SHUTDOWN_CENSOR" for payload in payloads
        )
        if has_censor != bool(record["contains_local_shutdown_censor"]):
            raise ValueError("lifecycle journal part censor flag mismatch")
        return payloads

    def _load_records_locked(self) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        for path in sorted(self.parts_root.glob("part-*.manifest.json")):
            record = _read_json(path)
            batch_id = str(record.get("batch_id", ""))
            self._validate_part_record(record, expected_batch_id=batch_id)
            if path.name != f"part-{batch_id}.manifest.json":
                raise ValueError("lifecycle journal part manifest filename mismatch")
            if batch_id in records:
                raise ValueError("duplicate lifecycle journal batch manifest")
            records[batch_id] = record
        return records

    def _derived_storage_state(
        self,
        records: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        by_lifecycle: dict[str, list[Mapping[str, Any]]] = {}
        for record in records.values():
            by_lifecycle.setdefault(str(record["lifecycle_id"]), []).append(record)
        cursors: dict[str, OrderLifecycleJournalV2Cursor] = {}
        event_ids: set[str] = set()
        censored: set[str] = set()
        for lifecycle_id, lifecycle_records in by_lifecycle.items():
            lifecycle_records.sort(key=lambda item: int(item["first_lifecycle_sequence"]))
            expected_sequence = 1
            prior_event_id = ""
            client_order_id = str(lifecycle_records[0]["client_order_id"])
            for index, record in enumerate(lifecycle_records):
                if str(record["client_order_id"]) != client_order_id:
                    raise ValueError("lifecycle journal client order identity changed")
                before = _cursor_from_mapping(record["checkpoint_before"])
                if (
                    before.lifecycle_id != lifecycle_id
                    or before.client_order_id != client_order_id
                    or before.last_emitted_sequence != expected_sequence - 1
                    or before.last_event_id != prior_event_id
                ):
                    raise ValueError("lifecycle journal part cursor chain is broken")
                if int(record["first_lifecycle_sequence"]) != expected_sequence:
                    raise ValueError("lifecycle journal part sequence has a gap or overlap")
                ids = [str(value) for value in record["event_ids"]]
                if event_ids.intersection(ids):
                    raise ValueError("lifecycle journal event id is duplicated across parts")
                event_ids.update(ids)
                if lifecycle_id in censored:
                    raise ValueError("lifecycle journal event follows local_shutdown_censor")
                if bool(record["contains_local_shutdown_censor"]):
                    if index != len(lifecycle_records) - 1:
                        raise ValueError("local_shutdown_censor is not the final durable event")
                    censored.add(lifecycle_id)
                after = _cursor_from_mapping(record["checkpoint_after"])
                expected_sequence = after.last_emitted_sequence + 1
                prior_event_id = after.last_event_id
            cursors[lifecycle_id] = _cursor_from_mapping(lifecycle_records[-1]["checkpoint_after"])
        return {
            "cursors": cursors,
            "event_ids": event_ids,
            "censored_lifecycle_ids": censored,
            "row_count": sum(int(record["row_count"]) for record in records.values()),
        }

    def _recover_locked(self) -> None:
        records = self._load_records_locked()
        derived = self._derived_storage_state(records)
        for lifecycle_id, persisted_path in self._cursor_files().items():
            persisted = _cursor_from_mapping(_read_json(persisted_path))
            recovered = derived["cursors"].get(lifecycle_id)
            if recovered is None:
                raise ValueError("durable cursor exists without an immutable journal part")
            if persisted.last_emitted_sequence > recovered.last_emitted_sequence:
                raise ValueError("durable cursor is ahead of immutable journal parts")
            if persisted.last_emitted_sequence:
                expected_event_id = self._event_id_at_sequence(
                    records,
                    lifecycle_id=lifecycle_id,
                    sequence=persisted.last_emitted_sequence,
                )
                if persisted.last_event_id != expected_event_id:
                    raise ValueError("durable cursor disagrees with immutable journal parts")
                checkpoint_boundaries = {
                    (
                        _cursor_from_mapping(record["checkpoint_after"]).last_emitted_sequence,
                        _cursor_from_mapping(record["checkpoint_after"]).last_event_id,
                    )
                    for record in records.values()
                    if str(record["lifecycle_id"]) == lifecycle_id
                }
                if (
                    persisted.last_emitted_sequence,
                    persisted.last_event_id,
                ) not in checkpoint_boundaries:
                    raise ValueError("durable cursor splits an atomic callback batch")

        self._records = records
        self._committed_event_ids = set(derived["event_ids"])
        self._censored_lifecycle_ids = set(derived["censored_lifecycle_ids"])
        if records:
            latest = max(records.values(), key=lambda item: int(item["committed_ts_ns"]))
            self._last_flush_ts_ns = max(
                self._last_flush_ts_ns,
                int(latest["committed_ts_ns"]),
            )
            self._last_flush_batch_id = str(latest["batch_id"])
        self._write_health_payload_locked(
            self._health_payload_locked(records=records, derived=derived),
            fault_context=None,
        )
        for lifecycle_id, cursor in derived["cursors"].items():
            persisted = self._read_cursor_if_present(lifecycle_id)
            if persisted != cursor:
                self._persist_cursor_locked(cursor, batch_id="recovery", inject=False)
        self._cursors = dict(derived["cursors"])

    def _event_id_at_sequence(
        self,
        records: Mapping[str, Mapping[str, Any]],
        *,
        lifecycle_id: str,
        sequence: int,
    ) -> str:
        for record in records.values():
            if str(record["lifecycle_id"]) != lifecycle_id:
                continue
            first = int(record["first_lifecycle_sequence"])
            last = int(record["last_lifecycle_sequence"])
            if first <= sequence <= last:
                return str(record["event_ids"][sequence - first])
        raise ValueError("durable cursor sequence is absent from immutable journal parts")

    def _cursor_path(self, lifecycle_id: str) -> Path:
        digest = hashlib.sha256(str(lifecycle_id).encode("utf-8")).hexdigest()
        return self.cursors_root / f"cursor-{digest}.json"

    def _cursor_files(self) -> dict[str, Path]:
        results: dict[str, Path] = {}
        for path in self.cursors_root.glob("cursor-*.json"):
            payload = _read_json(path)
            cursor = _cursor_from_mapping(payload)
            if path != self._cursor_path(cursor.lifecycle_id):
                raise ValueError("lifecycle cursor filename does not match lifecycle identity")
            if cursor.lifecycle_id in results:
                raise ValueError("duplicate durable lifecycle cursor")
            results[cursor.lifecycle_id] = path
        return results

    def _read_cursor_if_present(
        self,
        lifecycle_id: str,
    ) -> OrderLifecycleJournalV2Cursor | None:
        path = self._cursor_path(lifecycle_id)
        if not path.exists():
            return None
        return _cursor_from_mapping(_read_json(path))

    def _persist_cursor_locked(
        self,
        cursor: OrderLifecycleJournalV2Cursor,
        *,
        batch_id: str,
        inject: bool,
    ) -> None:
        context = {"operation": "batch_commit", "batch_id": batch_id}
        _atomic_write_json(
            self._cursor_path(cursor.lifecycle_id),
            cursor.checkpoint(),
            before_replace=(
                (lambda: self._inject("before_cursor_replace", context)) if inject else None
            ),
        )

    def _health_payload_locked(
        self,
        *,
        records: Mapping[str, Mapping[str, Any]] | None = None,
        derived: Mapping[str, Any] | None = None,
        last_flush_ts_ns: int | None = None,
        last_flush_batch_id: str | None = None,
    ) -> dict[str, Any]:
        active_records = self._records if records is None else records
        active_derived = self._derived_storage_state(active_records) if derived is None else derived
        orphan_payloads = self._orphan_payload_files(active_records)
        return {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_WRITER_V2_HEALTH_VERSION,
            "session_id": self.session_id,
            "runtime_identity_sha256": self._runtime_identity_sha256,
            "storage_format": self.storage_format,
            "state": self._state,
            "closed": self._closed,
            "restart_count": self._restart_count,
            "heartbeat_interval_s": self.heartbeat_interval_s,
            "last_heartbeat_ts_ns": self._last_heartbeat_ts_ns,
            "last_flush_ts_ns": (
                self._last_flush_ts_ns if last_flush_ts_ns is None else int(last_flush_ts_ns)
            ),
            "last_flush_batch_id": (
                self._last_flush_batch_id
                if last_flush_batch_id is None
                else str(last_flush_batch_id)
            ),
            "callbacks_committed": len(active_records),
            "rows_committed": int(active_derived["row_count"]),
            "rows_dropped": self._rows_dropped,
            "callbacks_quarantined": self._callbacks_quarantined,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "quarantine_order_ids": sorted(self._quarantine_order_ids),
            "excluded_lifecycle_ids": sorted(self._excluded_lifecycle_ids),
            "excluded_client_order_ids": sorted(self._excluded_client_order_ids),
            "durable_cursor_count": len(active_derived["cursors"]),
            "local_shutdown_censor_count": len(active_derived["censored_lifecycle_ids"]),
            "orphan_payload_count": len(orphan_payloads),
            "orphan_payload_files": orphan_payloads,
            "formal_collection_valid": bool(
                self._rows_dropped == 0
                and self._error_count == 0
                and self._state in {"collecting", "closed"}
                and not self._quarantine_order_ids
                and not orphan_payloads
            ),
            "economic_outcomes_read": False,
            "q90_action_authorized": False,
        }

    def _orphan_payload_files(
        self,
        records: Mapping[str, Mapping[str, Any]],
    ) -> list[str]:
        admitted = {str(record["data_file"]) for record in records.values()}
        candidates = list(self.parts_root.glob("part-*.parquet")) + list(
            self.parts_root.glob("part-*.jsonl")
        )
        return sorted(path.name for path in candidates if path.name not in admitted)

    def _persist_health_locked(self) -> None:
        self._write_health_payload_locked(self._health_payload_locked(), fault_context=None)

    def _write_health_payload_locked(
        self,
        payload: Mapping[str, Any],
        *,
        fault_context: Mapping[str, Any] | None,
    ) -> None:
        now_ns = time.time_ns()
        normalized = dict(payload)
        normalized["last_heartbeat_ts_ns"] = now_ns
        before_replace: Callable[[], None] | None = None
        if fault_context is not None:

            def inject_before_health_replace() -> None:
                self._inject("before_health_replace", fault_context)

            before_replace = inject_before_health_replace
        _atomic_write_json(self.health_path, normalized, before_replace=before_replace)
        self._last_heartbeat_ts_ns = now_ns

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval_s):
            try:
                self.heartbeat()
            except Exception:
                with self._lock:
                    self._error_count += 1
                    self._last_error = "heartbeat_write_failed"

    def close(self) -> dict[str, Any]:
        with self._lock:
            if self._closed:
                return self._health_payload_locked()
            self._closed = True
            self._state = "closed"
            self._stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=max(1.0, self.heartbeat_interval_s * 2.0))
        with self._lock:
            self._persist_health_locked()
            result = self._health_payload_locked()
        self._release_process_lock()
        return result


class OrderLifecycleJournalRuntimeBridgeV2:
    """Build callback batches from durable cursors and commit them atomically."""

    def __init__(self, writer: OrderLifecycleJournalWriterV2) -> None:
        self.writer = writer
        self._bindings: dict[str, _LifecycleBinding] = {}

    def register_lifecycle(
        self,
        *,
        lifecycle_id: str,
        runtime_source: str,
        client_order_id: str,
        symbol: str,
        side: str,
        exchange_order_id: str | int | None = None,
        orphan_adoption: bool = False,
        left_truncation_reason: str = "",
    ) -> None:
        binding = _LifecycleBinding(
            lifecycle_id=str(lifecycle_id),
            runtime_source=str(runtime_source),
            client_order_id=str(client_order_id),
            symbol=str(symbol),
            side=str(side),
            exchange_order_id=exchange_order_id,
            orphan_adoption=bool(orphan_adoption),
            left_truncation_reason=str(left_truncation_reason),
        )
        existing = self._bindings.get(binding.lifecycle_id)
        if existing is not None and existing != binding:
            raise ValueError("lifecycle runtime binding changed")
        self._bindings[binding.lifecycle_id] = binding

    def bind_exchange_order_id(
        self,
        lifecycle_id: str,
        exchange_order_id: str | int,
    ) -> None:
        binding = self._bindings.get(str(lifecycle_id))
        if binding is None:
            raise KeyError("lifecycle must be registered before binding exchange order id")
        normalized = str(exchange_order_id).strip()
        if not normalized or normalized.lower() in {"nan", "none", "null"}:
            raise ValueError("exchange order id is required")
        if binding.exchange_order_id not in {None, exchange_order_id, normalized}:
            raise ValueError("exchange order id changed within lifecycle")
        binding.exchange_order_id = normalized

    def submit_callback(
        self,
        *,
        lifecycle_id: str,
        lifecycle: QuantityWeightedOrderLifecycle,
        callback: OrderLifecycleJournalV2SourceCallback,
    ) -> LifecycleJournalCommitResult:
        binding = self._bindings.get(str(lifecycle_id))
        if binding is None:
            raise KeyError("lifecycle must be registered before callback submission")
        collect, reason = self.writer.collection_status(
            lifecycle_id=binding.lifecycle_id,
            client_order_id=binding.client_order_id,
        )
        if not collect:
            cursor = self.writer.cursor_for(
                lifecycle_id=binding.lifecycle_id,
                client_order_id=binding.client_order_id,
            )
            return LifecycleJournalCommitResult(
                status="quarantined",
                batch_id="",
                row_count=0,
                checkpoint=cursor.checkpoint(),
                reason=reason,
            )
        cursor = self.writer.cursor_for(
            lifecycle_id=binding.lifecycle_id,
            client_order_id=binding.client_order_id,
        )
        emitter = OrderLifecycleJournalV2BatchEmitter(
            lifecycle_id=binding.lifecycle_id,
            runtime_source=binding.runtime_source,
            client_order_id=binding.client_order_id,
            symbol=binding.symbol,
            side=binding.side,
            exchange_order_id=binding.exchange_order_id,
            orphan_adoption=binding.orphan_adoption,
            left_truncation_reason=binding.left_truncation_reason,
            cursor=cursor,
        )
        try:
            batch = emitter.emit_unseen(lifecycle=lifecycle, callback=callback)
        except Exception as exc:
            self.writer.report_error(f"callback_validation_failed:{type(exc).__name__}:{exc}")
            raise
        return self.writer.commit_batch(batch)
