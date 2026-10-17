"""Exact-level public-book paths attached to NarrowGate live orders."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


class DepthLevelState(Protocol):
    """Structural book-level view required by the execution tracker."""

    valid: bool
    covered: bool
    generation: int
    price: float
    quantity: float
    receive_ts_ns: int
    age_ms: float
    decrease_events: int
    decrease_qty: float
    increase_events: int
    increase_qty: float
    trade_events: int
    trade_qty: float


@dataclass(frozen=True)
class ActiveOrderDepthPathState:
    """Deep-book path observed after one live order becomes active.

    Public depth cannot reveal whether a cancel occurred ahead of or behind
    our order, so queue state is reported as bounds plus a proportional
    estimate rather than as exact exchange priority.
    """

    client_order_id: str
    valid: bool
    covered: bool
    ambiguous: bool
    invalid_reason: str
    generation: int
    side: str
    price: float
    age_ms: float
    initial_visible_qty: float
    current_visible_qty: float
    raw_decrease_events: int
    raw_decrease_qty: float
    exact_price_trade_events: int
    exact_price_trade_qty: float
    attributed_trade_qty: float
    inferred_cancel_events: int
    inferred_cancel_qty: float
    unresolved_trade_qty: float
    refill_events: int
    refill_qty: float
    queue_ahead_lower: float
    queue_ahead_estimate: float
    queue_ahead_upper: float
    receive_ts_ns: int
    feature_ready_ts_ns: int
    activation_ts_ns: int


@dataclass
class _ActiveOrderDepthCursor:
    client_order_id: str
    generation: int
    side: str
    price: float
    initial_visible_qty: float
    decrease_events: int
    decrease_qty: float
    increase_events: int
    increase_qty: float
    trade_events: int
    trade_qty: float
    activation_ts_ns: int
    valid: bool = True
    invalid_reason: str = ""


class ActiveOrderDepthPathTracker:
    """Bind cumulative exact-level public-book flow to active live orders."""

    _ACTIVE_STATES = {"OPEN", "PARTIALLY_FILLED", "PENDING_CANCEL"}

    def __init__(self) -> None:
        self._cursors: dict[str, _ActiveOrderDepthCursor] = {}
        self._states: dict[str, ActiveOrderDepthPathState] = {}

    @staticmethod
    def _state_name(order: Any) -> str:
        state = getattr(order, "state", "")
        return str(getattr(state, "name", state)).upper()

    def reset(self) -> None:
        self._cursors.clear()
        self._states.clear()

    def retain(self, client_order_id: str) -> bool:
        """Reject the retired post-terminal active-order retention contract."""

        raise RuntimeError(
            "terminal orders cannot remain in the active-order fill-risk set"
        )

    def release(self, client_order_id: str) -> None:
        self.discard(client_order_id)

    def discard(self, client_order_id: str) -> None:
        normalized = str(client_order_id)
        self._cursors.pop(normalized, None)
        self._states.pop(normalized, None)

    def retained_count(self) -> int:
        return 0

    def sync(
        self,
        orders: Any,
        *,
        level_state: Callable[[str, float], DepthLevelState | None],
        feature_ready_ts_ns: int | None = None,
    ) -> tuple[ActiveOrderDepthPathState, ...]:
        ready_ns = int(
            feature_ready_ts_ns
            if feature_ready_ts_ns is not None
            else time.time_ns()
        )
        order_views = list(orders)

        active_ids: set[str] = set()
        for order in order_views:
            if self._state_name(order) not in self._ACTIVE_STATES:
                continue
            client_order_id = str(getattr(order, "client_order_id", "") or "")
            side_value = getattr(getattr(order, "side", ""), "value", None)
            side = str(side_value or getattr(order, "side", "")).upper()
            price = float(getattr(order, "price", 0.0) or 0.0)
            if not client_order_id or side not in {"BUY", "SELL"} or price <= 0.0:
                continue
            active_ids.add(client_order_id)
            observed = level_state(side, price)
            if observed is None:
                self._states.pop(client_order_id, None)
                continue

            cursor = self._cursors.get(client_order_id)
            if cursor is None:
                cursor = _ActiveOrderDepthCursor(
                    client_order_id=client_order_id,
                    generation=int(observed.generation),
                    side=side,
                    price=float(observed.price),
                    initial_visible_qty=float(observed.quantity),
                    decrease_events=int(observed.decrease_events),
                    decrease_qty=float(observed.decrease_qty),
                    increase_events=int(observed.increase_events),
                    increase_qty=float(observed.increase_qty),
                    trade_events=int(observed.trade_events),
                    trade_qty=float(observed.trade_qty),
                    activation_ts_ns=ready_ns,
                    valid=bool(observed.valid),
                    invalid_reason=(
                        ""
                        if observed.valid
                        else "deep_level_invalid_at_activation"
                    ),
                )
                self._cursors[client_order_id] = cursor
            elif int(observed.generation) != cursor.generation:
                cursor.valid = False
                cursor.invalid_reason = "deep_book_generation_changed"

            decrease_events = max(
                0,
                int(observed.decrease_events) - cursor.decrease_events,
            )
            decrease_qty = max(
                0.0,
                float(observed.decrease_qty) - cursor.decrease_qty,
            )
            increase_events = max(
                0,
                int(observed.increase_events) - cursor.increase_events,
            )
            increase_qty = max(
                0.0,
                float(observed.increase_qty) - cursor.increase_qty,
            )
            trade_events = max(
                0,
                int(observed.trade_events) - cursor.trade_events,
            )
            trade_qty = max(
                0.0,
                float(observed.trade_qty) - cursor.trade_qty,
            )
            attributed_trade_qty = min(decrease_qty, trade_qty)
            inferred_cancel_qty = max(0.0, decrease_qty - trade_qty)
            unresolved_trade_qty = max(0.0, trade_qty - decrease_qty)
            inferred_cancel_events = (
                decrease_events if inferred_cancel_qty > 1e-12 else 0
            )

            initial = max(0.0, cursor.initial_visible_qty)
            after_trade = max(0.0, initial - attributed_trade_qty)
            queue_lower = max(0.0, after_trade - inferred_cancel_qty)
            queue_upper = after_trade
            public_before_cancel = max(
                0.0,
                initial - attributed_trade_qty + increase_qty,
            )
            ahead_share = (
                after_trade / public_before_cancel
                if public_before_cancel > 1e-12
                else 0.0
            )
            queue_estimate = max(
                queue_lower,
                min(
                    queue_upper,
                    after_trade - inferred_cancel_qty * ahead_share,
                ),
            )
            ambiguous = bool(unresolved_trade_qty > 1e-12)
            valid = bool(
                cursor.valid
                and observed.valid
                and observed.covered
                and not ambiguous
            )
            invalid_reason = cursor.invalid_reason
            if not observed.covered:
                invalid_reason = "order_price_outside_deep_book"
            elif not observed.valid:
                invalid_reason = "deep_level_stale_or_invalid"
            elif ambiguous:
                invalid_reason = "trade_depth_attribution_ambiguous"

            self._states[client_order_id] = ActiveOrderDepthPathState(
                client_order_id=client_order_id,
                valid=valid,
                covered=bool(observed.covered),
                ambiguous=ambiguous,
                invalid_reason=invalid_reason,
                generation=int(observed.generation),
                side=side,
                price=float(observed.price),
                age_ms=float(observed.age_ms),
                initial_visible_qty=initial,
                current_visible_qty=float(observed.quantity),
                raw_decrease_events=decrease_events,
                raw_decrease_qty=decrease_qty,
                exact_price_trade_events=trade_events,
                exact_price_trade_qty=trade_qty,
                attributed_trade_qty=attributed_trade_qty,
                inferred_cancel_events=inferred_cancel_events,
                inferred_cancel_qty=inferred_cancel_qty,
                unresolved_trade_qty=unresolved_trade_qty,
                refill_events=increase_events,
                refill_qty=increase_qty,
                queue_ahead_lower=queue_lower,
                queue_ahead_estimate=queue_estimate,
                queue_ahead_upper=queue_upper,
                receive_ts_ns=int(observed.receive_ts_ns),
                feature_ready_ts_ns=ready_ns,
                activation_ts_ns=int(cursor.activation_ts_ns),
            )

        for client_order_id in tuple(self._cursors):
            if client_order_id not in active_ids:
                self._cursors.pop(client_order_id, None)
                self._states.pop(client_order_id, None)
        return tuple(self._states[key] for key in sorted(self._states))

    def snapshot(self) -> tuple[ActiveOrderDepthPathState, ...]:
        return tuple(self._states[key] for key in sorted(self._states))
