"""Exact decision and order-lifecycle journal for quote opportunities."""

from __future__ import annotations

import math
from dataclasses import dataclass

EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION = "exact_quote_opportunity_tape.v1"

DECISION_EVENT = "decision"
ORDER_EVENTS = frozenset(
    {
        "submit",
        "rest_ack",
        "activate",
        "cancel_rejected",
        "cancel_request",
        "partial_fill",
        "full_fill",
        "cancel_ack",
        "cancel_ack_reconciled",
        "expired",
        "rejected",
        "local_shutdown_cancel",
    }
)


def exact_quote_role(
    side: str,
    signed_inventory_before: float,
    *,
    zero_tolerance: float = 1e-12,
) -> str:
    """Classify role from decision-visible signed inventory."""

    normalized = str(side).strip().upper()
    inventory = float(signed_inventory_before)
    if normalized not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported quote side {side!r}")
    if not math.isfinite(inventory):
        raise ValueError("signed inventory must be finite")
    if abs(inventory) <= float(zero_tolerance):
        return "opener"
    if normalized == "BUY":
        return "add" if inventory > 0.0 else "reducing"
    return "add" if inventory < 0.0 else "reducing"


@dataclass(frozen=True)
class ExactQuoteOpportunityTapeRow:
    schema_version: str
    event_type: str
    event_ts_ns: int
    exchange_ts_ns: int
    visibility_ts_ns: int
    decision_group_id: str
    decision_id: str
    origin_decision_id: str
    trigger_decision_id: str
    decision_start_ts_ns: int
    feature_ready_ts_ns: int
    symbol: str
    side: str
    role: str
    signed_inventory_before: float
    exposure_increasing: int
    baseline_eligible: int
    baseline_quote_price: float
    candidate_quote_price: float
    guard_valid: int
    guard_reason: str
    guard_adverse_side: str
    requested_outward_ticks: int
    effective_outward_ticks: int
    client_order_id: str
    replaced_client_order_id: str
    final_executed_action: str
    queue_reset: int
    lifecycle_sequence: int
    order_state: str
    terminal_reason: str
    order_quantity: float
    remaining_quantity: float
    fill_quantity: float
    fill_price: float


def empty_exact_opportunity_row(
    *,
    event_type: str,
    event_ts_ns: int,
    symbol: str,
    side: str,
) -> dict[str, object]:
    """Return a complete row payload so every event shares one CSV schema."""

    return {
        "schema_version": EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION,
        "event_type": str(event_type),
        "event_ts_ns": int(event_ts_ns),
        "exchange_ts_ns": 0,
        "visibility_ts_ns": int(event_ts_ns),
        "decision_group_id": "",
        "decision_id": "",
        "origin_decision_id": "",
        "trigger_decision_id": "",
        "decision_start_ts_ns": 0,
        "feature_ready_ts_ns": 0,
        "symbol": str(symbol),
        "side": str(side).upper(),
        "role": "unknown",
        "signed_inventory_before": math.nan,
        "exposure_increasing": 0,
        "baseline_eligible": 0,
        "baseline_quote_price": 0.0,
        "candidate_quote_price": 0.0,
        "guard_valid": 0,
        "guard_reason": "not_evaluated",
        "guard_adverse_side": "",
        "requested_outward_ticks": 0,
        "effective_outward_ticks": 0,
        "client_order_id": "",
        "replaced_client_order_id": "",
        "final_executed_action": "none",
        "queue_reset": 0,
        "lifecycle_sequence": 0,
        "order_state": "",
        "terminal_reason": "",
        "order_quantity": 0.0,
        "remaining_quantity": 0.0,
        "fill_quantity": 0.0,
        "fill_price": 0.0,
    }
