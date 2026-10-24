"""Replay lifecycle owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Optional


class LifecycleAccounting:
    def _record_order_lifecycle_submit(self, order: dict[str, Any], ts_ms: int, *, request_sequence=None) -> None:
        if self.state.response_update_observer is not None:
            self.state.response_update_observer.submitted('new', order['trace_id'], ts_ms,
                                                         request_sequence=request_sequence)
        event = self.state.l2_production.observe("order_submit", ts_ms, order["side"])
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("order_submit", ts_ms, side=order["side"], payload=self._order_trace_fields(order), production_event=event)
        self.state.local_order_lifecycle.submit(order, int(ts_ms))
        if self.state.risk_selection is not None:
            self.state.risk_selection.record_execution(str(order["side"]), "submit")
        if self.state.order_lifecycle_journal_v2_adapter is not None:
            self.state.order_lifecycle_journal_v2_adapter.submit(order, int(ts_ms))

    def _record_order_lifecycle_activate(self,
        order: dict[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
        mid: float,
    ) -> None:
        event = self.state.l2_production.observe("order_active_notification", visibility_ts_ms, order["side"])
        if self.state.quote_schedule is not None:
            self.state.quote_schedule.change("VISIBLE_ORDER")
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("order_active_notification", visibility_ts_ms, side=order["side"],
                payload={**self._order_trace_fields(order), "exchange_effective_ts_ms": exchange_ts_ms}, production_event=event)
        self.state.local_order_lifecycle.activate(order, int(exchange_ts_ms), mid=float(mid))
        if self.state.order_lifecycle_journal_v2_adapter is not None:
            self.state.order_lifecycle_journal_v2_adapter.activate(
                order,
                visibility_ts_ms=int(visibility_ts_ms),
                exchange_ts_ms=int(exchange_ts_ms),
            )

    def _record_order_lifecycle_reject(self,
        order: dict[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
        reason: str,
    ) -> None:
        event = self.state.l2_production.observe("order_reject", visibility_ts_ms, order["side"])
        if self.state.quote_schedule is not None:
            self.state.quote_schedule.change("VISIBLE_ORDER")
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("order_reject", visibility_ts_ms, side=order["side"],
                payload={**self._order_trace_fields(order), "exchange_effective_ts_ms": exchange_ts_ms, "reason": reason}, production_event=event)
        self.state.local_order_lifecycle.reject(order, int(exchange_ts_ms), reason=str(reason))
        if self.state.order_lifecycle_journal_v2_adapter is not None:
            self.state.order_lifecycle_journal_v2_adapter.reject(
                order,
                visibility_ts_ms=int(visibility_ts_ms),
                exchange_ts_ms=int(exchange_ts_ms),
            )

    def _record_order_lifecycle_cancel_request(self,
        order: dict[str, Any],
        ts_ms: int,
        *,
        reason: str,
        request_sequence=None,
    ) -> None:
        if self.state.response_update_observer is not None:
            self.state.response_update_observer.submitted('cancel', order['trace_id'], ts_ms,
                                                         request_sequence=request_sequence)
        event = self.state.l2_production.observe("cancel_request", ts_ms, order["side"])
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("cancel_request", ts_ms, side=order["side"],
                payload={**self._order_trace_fields(order), "reason": reason}, production_event=event)
        self.state.local_order_lifecycle.request_cancel(order, int(ts_ms), reason=str(reason))
        if self.state.risk_selection is not None:
            self.state.risk_selection.record_execution(str(order["side"]), "cancel_request", str(reason))
        if self.state.order_lifecycle_journal_v2_adapter is not None:
            self.state.order_lifecycle_journal_v2_adapter.request_cancel(order, int(ts_ms))

    def _record_order_lifecycle_cancel_ack(self,
        order: dict[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
        reason: str,
    ) -> None:
        event = self.state.l2_production.observe("cancel_terminal_notification", visibility_ts_ms, order["side"])
        if (self.state.quote_schedule is not None and not (
                self.state.replace_terminal_continuation
                and order.get("replace_terminal_continuation_armed", False)
                and str(order.get("cancel_reason") or "") in {"requote_replace", "cancel_first_replace"})):
            self.state.quote_schedule.change("VISIBLE_ORDER")
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("cancel_terminal_notification", visibility_ts_ms, side=order["side"],
                payload={**self._order_trace_fields(order), "exchange_effective_ts_ms": exchange_ts_ms, "reason": reason}, production_event=event)
        self.state.local_order_lifecycle.cancel_ack(
            order,
            int(visibility_ts_ms),
            reason=str(reason),
        )
        if self.state.order_lifecycle_journal_v2_adapter is None:
            return
        if str(reason) in {"expired", "ioc_expired", "ioc_no_top_liquidity"}:
            self.state.order_lifecycle_journal_v2_adapter.expire(
                order,
                visibility_ts_ms=int(visibility_ts_ms),
                exchange_ts_ms=int(exchange_ts_ms),
            )
        else:
            self.state.order_lifecycle_journal_v2_adapter.cancel_ack(
                order,
                visibility_ts_ms=int(visibility_ts_ms),
                exchange_ts_ms=int(exchange_ts_ms),
            )

    def _record_order_lifecycle_fill(self,
        order: dict[str, Any],
        visibility_ts_ms: int,
        *,
        exchange_ts_ms: Optional[int] = None,
        fill_qty: float,
        remaining_before: float,
        remaining_after: float,
        fill_price: float,
        inventory_before: float,
        inventory_after: float,
        inventory_lifecycle_id: int,
        physical_fill_identity: str,
        economic_legs: tuple[dict[str, Any], ...],
    ) -> None:
        event = self.state.l2_production.observe("fill_notification", visibility_ts_ms, order["side"])
        if self.state.quote_schedule is not None:
            self.state.quote_schedule.change("VISIBLE_ACCOUNT")
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("fill_notification", visibility_ts_ms, side=order["side"],
                payload={**self._order_trace_fields(order), "exchange_fill_ts_ms": exchange_ts_ms,
                         "fill_qty": fill_qty, "fill_price": fill_price,
                         "remaining_before": remaining_before, "remaining_after": remaining_after,
                         "inventory_before": inventory_before, "inventory_after": inventory_after,
                         "physical_fill_identity": physical_fill_identity}, production_event=event)
        self.state.local_order_lifecycle.fill(
            order,
            int(visibility_ts_ms),
            fill_qty=float(fill_qty),
            remaining_before=float(remaining_before),
            remaining_after=float(remaining_after),
            fill_price=float(fill_price),
            inventory_before=float(inventory_before),
            inventory_after=float(inventory_after),
            inventory_lifecycle_id=int(inventory_lifecycle_id),
            physical_fill_identity=str(physical_fill_identity),
            economic_legs=economic_legs,
        )
        if self.state.order_lifecycle_journal_v2_adapter is not None:
            journal_full_fill = float(remaining_after) <= 1e-12
            self.state.order_lifecycle_journal_v2_adapter.fill(
                order,
                remaining_after=(
                    0.0 if journal_full_fill else float(remaining_after)
                ),
                visibility_ts_ms=int(visibility_ts_ms),
                exchange_ts_ms=int(
                    visibility_ts_ms
                    if exchange_ts_ms is None
                    else exchange_ts_ms
                ),
                full_fill=journal_full_fill,
            )

    def _inventory_lifecycle_legs_for_order_fill(self,
        order: Mapping[str, Any],
        *,
        ts_ms: int,
        side: str,
        inventory_before: float,
        inventory_after: float,
        fill_qty: float,
        fill_price: float,
        fee_rate: float,
        remaining_before: float,
        remaining_after: float,
    ) -> tuple[str, tuple[dict[str, Any], ...]]:
        trace_id = int(order.get("trace_id", -1) or -1)
        physical_identity = (
            f"replay_order_fill:{trace_id}:{int(ts_ms)}:"
            f"{float(remaining_before):.12g}->{float(remaining_after):.12g}"
        )
        opening_inventory_lifecycle_id = int(self.state.inventory_lifecycle_count) + int(
            (
                abs(float(inventory_before)) <= 1e-10
                and abs(float(inventory_after)) > 1e-10
            )
            or float(inventory_before) * float(inventory_after) < -1e-20
        )
        legs = self.services.inventory_lifecycle_fill_economic_legs(
            physical_fill_identity=physical_identity,
            side=side,
            inventory_before=inventory_before,
            inventory_after=inventory_after,
            fill_quantity=fill_qty,
            fill_price=fill_price,
            fee_rate=fee_rate,
            closing_inventory_lifecycle_id=int(self.state.inventory_lifecycle_count),
            opening_inventory_lifecycle_id=opening_inventory_lifecycle_id,
        )
        return physical_identity, legs
