"""Single-owner replay persistence for strict-native lifecycle journal v2.

The general journal writer rescans every immutable part before every callback so
an independently invoked live callback can recover from an arbitrary process
boundary.  A formal replay day has a stronger outer contract: one process owns
the session lock, writes only below a disposable day staging directory, and the
parent admits that directory only after the worker closes and validates it.

This successor keeps the same immutable per-callback parts, health record, and
durable cursors.  It performs the full disk recovery once in the base
constructor, then advances the already validated in-memory state while the
exclusive process lock remains held.  Explicit ``reconcile()`` remains a cheap
ownership assertion; crash recovery is performed by discarding the unadmitted
day staging directory and replaying the day from its frozen inputs.
"""

from __future__ import annotations

from execution.order_lifecycle_journal_v2_strict_native import (
    OrderLifecycleJournalV2Batch,
)
from execution.order_lifecycle_journal_writer_v2_strict_native import (
    LifecycleJournalCommitResult,
    OrderLifecycleJournalWriterV2,
    _payloads_from_batch,
)

REPLAY_WRITER_ID = "order_lifecycle_journal_writer_v2.replay_single_owner.v1"


class SingleOwnerReplayJournalWriterV2(OrderLifecycleJournalWriterV2):
    """Strict writer whose incremental commits trust its exclusive owner state."""

    def _assert_incremental_owner_locked(self) -> None:
        if self._closed:
            raise RuntimeError("lifecycle journal writer is closed")
        if self._lock_handle is None or self._lock_handle.closed:
            raise RuntimeError("single-owner replay writer lost its process lock")

    def reconcile(self) -> None:
        """Assert ownership without re-reading all immutable parts.

        The base constructor already completed full recovery before this method
        can be called.  No second writer can enter the session while the process
        lock is held.
        """

        with self._lock:
            self._assert_incremental_owner_locked()

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
            self._assert_incremental_owner_locked()
            try:
                return self._commit_batch_locked(batch=batch, payloads=payloads)
            except Exception as exc:
                self._error_count += 1
                self._last_error = f"{type(exc).__name__}:{exc}"
                try:
                    self._persist_health_locked()
                except Exception:
                    pass
                raise
