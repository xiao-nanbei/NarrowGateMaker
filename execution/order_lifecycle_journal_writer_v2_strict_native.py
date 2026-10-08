"""Strict-native schema adapter for the canonical lifecycle journal writer.

Persistence, recovery, locking, fsync, cursor, and bridge mechanics live in
``order_lifecycle_journal_writer_v2``. This module preserves the historical
import surface while selecting the strict-native payload schema and emitter.
"""

from __future__ import annotations

from execution.order_lifecycle_journal_v2_strict_native import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    OrderLifecycleJournalV2BatchEmitter,
    OrderLifecycleJournalV2Cursor,
    validate_order_lifecycle_journal_v2_payload,
)
from execution.order_lifecycle_journal_writer_v2 import (
    LifecycleJournalCommitResult,
    LifecycleJournalSchemaContract,
    OrderLifecycleJournalRuntimeBridgeV2,
)
from execution.order_lifecycle_journal_writer_v2 import (
    OrderLifecycleJournalWriterV2 as _CanonicalOrderLifecycleJournalWriterV2,
)

STRICT_NATIVE_SCHEMA_CONTRACT = LifecycleJournalSchemaContract(
    schema_version=ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    columns=tuple(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS),
    cursor_type=OrderLifecycleJournalV2Cursor,
    batch_emitter_type=OrderLifecycleJournalV2BatchEmitter,
    payload_validator=validate_order_lifecycle_journal_v2_payload,
    extra_string_columns=frozenset({"simulator_queue_source"}),
    extra_boolean_columns=frozenset({"exact_queue_path_valid"}),
)


class OrderLifecycleJournalWriterV2(_CanonicalOrderLifecycleJournalWriterV2):
    """Canonical durable writer configured for the strict-native schema."""

    SCHEMA_CONTRACT = STRICT_NATIVE_SCHEMA_CONTRACT


__all__ = [
    "LifecycleJournalCommitResult",
    "OrderLifecycleJournalRuntimeBridgeV2",
    "OrderLifecycleJournalWriterV2",
]
