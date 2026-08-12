"""Authoritative live/replay journal schema for order lifecycle mechanics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from execution.order_lifecycle import QuantityWeightedOrderLifecycle

ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION = "order_lifecycle_journal.v1"


@dataclass(frozen=True)
class OrderLifecycleJournalRow:
    schema_version: str
    runtime_source: str
    source_event_type: str
    lifecycle_event: str
    lifecycle_sequence: int
    client_order_id: str
    exchange_order_id: int
    symbol: str
    side: str
    order_state: str
    visibility_ts_ns: int
    exchange_ts_ns: int
    phase_before: str
    phase_after: str
    fill_risk_active: int
    initial_quantity: float
    remaining_quantity_before: float
    remaining_quantity_after: float
    quantity_time_exposure_visible_btc_s: float
    quantity_time_exposure_exchange_btc_s: float | None
    quantity_time_exposure_visibility_minus_exchange_btc_s: float | None
    exchange_exposure_valid: int
    exchange_exposure_complete: int
    exchange_exposure_invalid_reason: str
    terminal_reason: str
    terminal_policy_route: str
    event_reason: str


def build_order_lifecycle_journal_row(
    *,
    lifecycle: QuantityWeightedOrderLifecycle,
    runtime_source: str,
    source_event_type: str,
    client_order_id: str,
    exchange_order_id: int,
    symbol: str,
    side: str,
    order_state: str,
) -> OrderLifecycleJournalRow:
    """Build one row from the latest lifecycle transition and its clock state."""

    events = lifecycle.events()
    if not events:
        raise ValueError("order lifecycle has no event to journal")
    event = events[-1]
    snapshot = lifecycle.snapshot()
    visible = float(snapshot["quantity_time_exposure_visible_btc_s"])
    exchange_raw = snapshot["quantity_time_exposure_exchange_btc_s"]
    exchange = float(exchange_raw) if exchange_raw is not None else None
    difference = visible - exchange if exchange is not None else None
    return OrderLifecycleJournalRow(
        schema_version=ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION,
        runtime_source=str(runtime_source),
        source_event_type=str(source_event_type),
        lifecycle_event=str(event["event"]),
        lifecycle_sequence=int(event["sequence"]),
        client_order_id=str(client_order_id),
        exchange_order_id=int(exchange_order_id),
        symbol=str(symbol),
        side=str(side).upper(),
        order_state=str(order_state),
        visibility_ts_ns=int(event["visibility_ts_ns"]),
        exchange_ts_ns=int(event["exchange_ts_ns"]),
        phase_before=str(event["phase_before"]),
        phase_after=str(event["phase_after"]),
        fill_risk_active=int(bool(snapshot["fill_risk_active"])),
        initial_quantity=float(snapshot["initial_quantity"]),
        remaining_quantity_before=float(event["remaining_qty_before"]),
        remaining_quantity_after=float(event["remaining_qty_after"]),
        quantity_time_exposure_visible_btc_s=visible,
        quantity_time_exposure_exchange_btc_s=exchange,
        quantity_time_exposure_visibility_minus_exchange_btc_s=difference,
        exchange_exposure_valid=int(bool(snapshot["exchange_exposure_valid"])),
        exchange_exposure_complete=int(bool(snapshot["exchange_exposure_complete"])),
        exchange_exposure_invalid_reason=str(snapshot["exchange_exposure_invalid_reason"]),
        terminal_reason=str(snapshot["terminal_reason"]),
        terminal_policy_route=str(snapshot["terminal_policy_route"]),
        event_reason=str(event["reason"]),
    )


def order_lifecycle_journal_payload(**kwargs: Any) -> dict[str, object]:
    """Return a CSV/parquet-friendly payload with a stable column order."""

    return asdict(build_order_lifecycle_journal_row(**kwargs))


def audit_order_lifecycle_journal(
    rows: list[dict[str, object]] | tuple[dict[str, object], ...],
) -> dict[str, object]:
    """Summarize clock coverage without treating C++ as an exposure source."""

    terminal_rows = [row for row in rows if str(row.get("phase_after", "")) == "EXCHANGE_TERMINAL"]
    invalid_reasons: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("exchange_exposure_invalid_reason", "") or "")
        if reason:
            invalid_reasons[reason] = invalid_reasons.get(reason, 0) + 1
    return {
        "schema_version": ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION,
        "row_count": len(rows),
        "order_count": len({str(row.get("client_order_id", "")) for row in rows}),
        "terminal_row_count": len(terminal_rows),
        "terminal_exchange_exposure_complete_count": sum(
            int(bool(row.get("exchange_exposure_complete", 0))) for row in terminal_rows
        ),
        "exchange_exposure_null_row_count": sum(
            int(row.get("quantity_time_exposure_exchange_btc_s") is None) for row in rows
        ),
        "exchange_exposure_invalid_row_count": sum(
            int(not bool(row.get("exchange_exposure_valid", 0))) for row in rows
        ),
        "exchange_exposure_invalid_reason_counts": invalid_reasons,
        "unsupported_terminal_route_count": sum(
            int(str(row.get("terminal_policy_route", "")) == "UNSUPPORTED") for row in terminal_rows
        ),
        "cpp_exposure_authority": False,
    }
