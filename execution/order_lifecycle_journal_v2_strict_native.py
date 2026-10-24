"""Native-queue provenance schema over the shared lifecycle journal mechanics.

The historical column order and import surface are preserved. Event derivation,
clock checks, batching and recovery cursors have one implementation, in v2.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, make_dataclass

from execution import order_lifecycle_journal_v2 as core

ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION = core.ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION
ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION = ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION
ORDER_LIFECYCLE_JOURNAL_V2_CURSOR_VERSION = core.ORDER_LIFECYCLE_JOURNAL_V2_CURSOR_VERSION
OrderLifecycleJournalV2Cursor = core.OrderLifecycleJournalV2Cursor

_fields = []
for field in fields(core.OrderLifecycleJournalV2Row):
    _fields.append((field.name, field.type))
    if field.name == 'fill_risk_active_after':
        _fields.extend([('simulator_queue_source', str), ('exact_queue_path_valid', bool)])
OrderLifecycleJournalV2Row = make_dataclass(
    'OrderLifecycleJournalV2Row', _fields, frozen=True, slots=True,
    namespace={'__module__': __name__},
)
ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS = tuple(OrderLifecycleJournalV2Row.__dataclass_fields__)


def _validate_queue(source: object, exact: object) -> None:
    source = core._require_id('simulator queue source', source)
    if bool(exact) and source != 'native_exchange_book':
        raise ValueError('exact queue path validity requires native_exchange_book source')


@dataclass(frozen=True, slots=True)
class OrderLifecycleJournalV2SourceCallback(core.OrderLifecycleJournalV2SourceCallback):
    simulator_queue_source: str = 'not_recorded'
    exact_queue_path_valid: bool = False

    def __post_init__(self) -> None:
        core.OrderLifecycleJournalV2SourceCallback.__post_init__(self)
        _validate_queue(self.simulator_queue_source, self.exact_queue_path_valid)


def validate_order_lifecycle_journal_v2_payload(payload: Mapping[str, object]) -> None:
    if tuple(payload) != ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS:
        raise ValueError('order lifecycle journal v2 payload schema mismatch')
    core.validate_order_lifecycle_journal_v2_payload(
        {name: payload[name] for name in core.ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS}
    )
    _validate_queue(payload['simulator_queue_source'], payload['exact_queue_path_valid'])


class OrderLifecycleJournalV2Batch(core.OrderLifecycleJournalV2Batch):
    __slots__ = ()
    validate_payload = staticmethod(validate_order_lifecycle_journal_v2_payload)


class OrderLifecycleJournalV2BatchEmitter(core.OrderLifecycleJournalV2BatchEmitter):
    row_type = OrderLifecycleJournalV2Row
    batch_type = OrderLifecycleJournalV2Batch

    @staticmethod
    def row_extensions(callback: OrderLifecycleJournalV2SourceCallback) -> dict[str, object]:
        return dict(simulator_queue_source=str(callback.simulator_queue_source),
                    exact_queue_path_valid=bool(callback.exact_queue_path_valid))


OrderLifecycleJournalV2Emitter = OrderLifecycleJournalV2BatchEmitter
