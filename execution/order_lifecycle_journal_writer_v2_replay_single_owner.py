"""Single-owner strict-native replay writer.

The canonical writer performs full recovery at startup and after a failed
commit, then advances validated state incrementally. This adapter adds the
formal replay owner's process-lock assertion while preserving the historical
``reconcile()`` interface as a cheap ownership check.
"""

from __future__ import annotations

from execution.order_lifecycle_journal_writer_v2_strict_native import (
    OrderLifecycleJournalWriterV2,
)

REPLAY_WRITER_ID = "order_lifecycle_journal_writer_v2.replay_single_owner.v1"


class SingleOwnerReplayJournalWriterV2(OrderLifecycleJournalWriterV2):
    """Strict writer whose incremental commits trust its exclusive owner state."""

    def _assert_operation_allowed_locked(self) -> None:
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
            self._assert_operation_allowed_locked()
