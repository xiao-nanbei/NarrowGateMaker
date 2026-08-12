"""Day-staging atomic writer for formal lifecycle replay.

Live callbacks need callback-durable recovery.  A formal replay day has a
different outer transaction: one process owns a disposable staging directory,
and the parent admits the day only after replay, journal validation, and an
atomic directory rename.  Persisting one tiny Parquet file per callback makes
that replay path dominated by filesystem commits.

This successor validates every callback and advances strict in-memory cursors,
then publishes one content-addressed Parquet part when the day closes.  A crash
never resumes this buffer; the unadmitted day staging directory is discarded
and replayed from frozen inputs.  The live and historical journal writers are
unchanged.
"""

from __future__ import annotations

import hashlib
import os
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from execution.order_lifecycle_journal_v2_strict_native import (
    OrderLifecycleJournalV2Batch,
)
from execution.order_lifecycle_journal_writer_v2_strict_native import (
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    LifecycleJournalCommitResult,
    OrderLifecycleJournalWriterV2,
    _atomic_write_json,
    _canonical_sha256,
    _fsync_directory,
    _journal_schema_sha256,
    _payloads_from_batch,
    _pyarrow_schema,
    _sha256_file,
    _cursor_from_mapping,
)

REPLAY_WRITER_ID = "order_lifecycle_journal_writer_v2.replay_day_buffered.v1"
REPLAY_DAY_PART_VERSION = "order_lifecycle_journal_replay_day_part.v1"
_HEALTH_CHECKPOINT_CALLBACKS = 2048


class DayBufferedReplayJournalWriterV2(OrderLifecycleJournalWriterV2):
    """Strict callback validator with one atomic day-staging payload."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._pending_payloads: list[dict[str, Any]] = []
        self._buffered_batch_ids: set[str] = set()
        self._buffered_callback_count = 0
        self._buffered_event_id_hasher = hashlib.sha256()
        self._day_part_count = 0
        self._day_part_id = ""
        self._day_payload_flushed = False
        super().__init__(*args, **kwargs)
        try:
            if self.storage_format != "parquet":
                raise ValueError("day-buffered replay writer requires Parquet")
            if self._restart_count != 0 or self._records or any(self.cursors_root.iterdir()):
                raise RuntimeError(
                    "day-buffered replay cannot resume an unadmitted staging session"
                )
        except Exception:
            self._release_process_lock()
            raise

    def _assert_incremental_owner_locked(self) -> None:
        if self._closed:
            raise RuntimeError("lifecycle journal writer is closed")
        if self._lock_handle is None or self._lock_handle.closed:
            raise RuntimeError("day-buffered replay writer lost its process lock")

    def reconcile(self) -> None:
        with self._lock:
            self._assert_incremental_owner_locked()

    def _health_payload_locked(
        self,
        *,
        records: Mapping[str, Mapping[str, Any]] | None = None,
        derived: Mapping[str, Any] | None = None,
        last_flush_ts_ns: int | None = None,
        last_flush_batch_id: str | None = None,
    ) -> dict[str, Any]:
        payload = super()._health_payload_locked(
            records=records,
            derived=derived,
            last_flush_ts_ns=last_flush_ts_ns,
            last_flush_batch_id=last_flush_batch_id,
        )
        committed = bool(self._day_payload_flushed)
        payload.update(
            {
                "callbacks_committed": (
                    self._buffered_callback_count if committed else 0
                ),
                "rows_committed": len(self._pending_payloads) if committed else 0,
                "durable_cursor_count": len(self._cursors) if committed else 0,
                "local_shutdown_censor_count": (
                    len(self._censored_lifecycle_ids) if committed else 0
                ),
                "orphan_payload_count": 0,
                "orphan_payload_files": [],
                "formal_collection_valid": bool(
                    self._rows_dropped == 0
                    and self._error_count == 0
                    and self._state in {"collecting", "closed"}
                    and not self._quarantine_order_ids
                    and (not self._closed or committed)
                ),
                "replay_writer_id": REPLAY_WRITER_ID,
                "atomic_commit_scope": "unadmitted_day_staging",
                "crash_recovery": "discard_staging_and_replay_frozen_day",
                "callbacks_buffered": self._buffered_callback_count,
                "rows_buffered": len(self._pending_payloads),
                "buffered_cursor_count": len(self._cursors),
                "part_count": self._day_part_count,
                "day_part_id": self._day_part_id,
            }
        )
        return payload

    def commit_batch(
        self,
        batch: OrderLifecycleJournalV2Batch,
    ) -> LifecycleJournalCommitResult:
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
            self._assert_incremental_owner_locked()
            try:
                first = payloads[0]
                lifecycle_id = str(first["lifecycle_id"])
                client_order_id = str(first["client_order_id"])
                if any(str(row["lifecycle_id"]) != lifecycle_id for row in payloads):
                    raise ValueError("callback batch spans multiple lifecycle ids")
                if any(str(row["client_order_id"]) != client_order_id for row in payloads):
                    raise ValueError("callback batch spans multiple client order ids")
                batch_id = _canonical_sha256(
                    {
                        "writer": REPLAY_WRITER_ID,
                        "lifecycle_id": lifecycle_id,
                        "event_ids": [row["event_id"] for row in payloads],
                        "source_callback_id": first["source_callback_id"],
                        "checkpoint_after": dict(batch.checkpoint),
                    }
                )
                if batch_id in self._buffered_batch_ids:
                    return LifecycleJournalCommitResult(
                        status="duplicate",
                        batch_id=batch_id,
                        row_count=len(payloads),
                        checkpoint=dict(batch.checkpoint),
                        reason="batch_already_buffered",
                    )
                event_ids = [str(row["event_id"]) for row in payloads]
                if all(event_id in self._committed_event_ids for event_id in event_ids):
                    return LifecycleJournalCommitResult(
                        status="duplicate",
                        batch_id=batch_id,
                        row_count=len(payloads),
                        checkpoint=dict(batch.checkpoint),
                        reason="events_already_buffered",
                    )
                if any(event_id in self._committed_event_ids for event_id in event_ids):
                    raise ValueError("callback batch partially overlaps buffered events")
                cursor_before = self.cursor_for(
                    lifecycle_id=lifecycle_id,
                    client_order_id=client_order_id,
                )
                if int(first["lifecycle_sequence"]) != cursor_before.last_emitted_sequence + 1:
                    raise ValueError("callback batch does not continue lifecycle cursor")
                if lifecycle_id in self._censored_lifecycle_ids:
                    raise ValueError("event follows a local shutdown censor")

                next_cursor = _cursor_from_mapping(batch.checkpoint)
                self._pending_payloads.extend(payloads)
                self._buffered_batch_ids.add(batch_id)
                self._committed_event_ids.update(event_ids)
                self._cursors[lifecycle_id] = next_cursor
                if any(
                    row["terminal_observation"] == "LOCAL_SHUTDOWN_CENSOR"
                    for row in payloads
                ):
                    self._censored_lifecycle_ids.add(lifecycle_id)
                for event_id in event_ids:
                    self._buffered_event_id_hasher.update(event_id.encode("utf-8"))
                    self._buffered_event_id_hasher.update(b"\0")
                self._buffered_callback_count += 1
                if self._buffered_callback_count % _HEALTH_CHECKPOINT_CALLBACKS == 0:
                    self._persist_health_locked()
                return LifecycleJournalCommitResult(
                    status="committed",
                    batch_id=batch_id,
                    row_count=len(payloads),
                    checkpoint=dict(batch.checkpoint),
                )
            except Exception as exc:
                self._error_count += 1
                self._last_error = f"{type(exc).__name__}:{exc}"
                try:
                    self._persist_health_locked()
                except Exception:
                    pass
                raise

    def _publish_day_part_locked(self) -> None:
        if not self._pending_payloads:
            raise RuntimeError("day-buffered replay emitted zero rows")
        cursor_sha256 = _canonical_sha256(
            [self._cursors[key].checkpoint() for key in sorted(self._cursors)]
        )
        part_id = _canonical_sha256(
            {
                "schema_version": REPLAY_DAY_PART_VERSION,
                "runtime_identity_sha256": self._runtime_identity_sha256,
                "row_count": len(self._pending_payloads),
                "callback_count": self._buffered_callback_count,
                "event_ids_sha256": self._buffered_event_id_hasher.hexdigest(),
                "lifecycle_cursors_sha256": cursor_sha256,
            }
        )
        data_path = self.parts_root / f"part-{part_id}.parquet"
        manifest_path = self.parts_root / f"part-{part_id}.manifest.json"
        temporary = data_path.with_name(
            f".{data_path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
        )
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.Table.from_pylist(self._pending_payloads, schema=_pyarrow_schema())
            pq.write_table(table, temporary, compression="zstd")
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary, data_path)
            _fsync_directory(self.parts_root)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        committed_ts_ns = time.time_ns()
        manifest = {
            "schema_version": REPLAY_DAY_PART_VERSION,
            "identity": REPLAY_WRITER_ID,
            "part_id": part_id,
            "runtime_identity_sha256": self._runtime_identity_sha256,
            "journal_schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
            "journal_schema_sha256": _journal_schema_sha256(),
            "storage_format": "parquet",
            "data_file": data_path.name,
            "data_sha256": _sha256_file(data_path),
            "row_count": len(self._pending_payloads),
            "callback_count": self._buffered_callback_count,
            "lifecycle_count": len(self._cursors),
            "event_ids_sha256": self._buffered_event_id_hasher.hexdigest(),
            "lifecycle_cursors_sha256": cursor_sha256,
            "atomic_commit_scope": "unadmitted_day_staging",
            "committed_ts_ns": committed_ts_ns,
            "economic_outcomes_read": False,
            "q90_action_authorized": False,
        }
        _atomic_write_json(manifest_path, manifest)
        self._day_part_count = 1
        self._day_part_id = part_id
        self._day_payload_flushed = True
        self._last_flush_ts_ns = committed_ts_ns
        self._last_flush_batch_id = part_id

    def close(self) -> dict[str, Any]:
        with self._lock:
            if self._closed:
                return self._health_payload_locked()
            try:
                self._assert_incremental_owner_locked()
                self._publish_day_part_locked()
                self._closed = True
                self._state = "closed"
                self._stop.set()
                self._persist_health_locked()
                result = self._health_payload_locked()
            except Exception as exc:
                self._error_count += 1
                self._last_error = f"{type(exc).__name__}:{exc}"
                try:
                    self._persist_health_locked()
                except Exception:
                    pass
                raise
            finally:
                self._release_process_lock()
        return result
