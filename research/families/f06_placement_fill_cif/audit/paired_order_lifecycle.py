"""Paired placement lifecycle mechanics for a narrow native-deep smoke.

The module is deliberately policy blind.  One baseline side-decision creates
three shadow children that share submit/activation/cancel timing and public
events.  Each child still resolves its own GTX activation, native visible
queue and exact/through fill path.  Shadow fills never feed inventory or the
baseline strategy state.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from models.exchange_book_replay import (
    ExchangeBookLevelChange,
    ExchangeBookLookup,
)

SCHEMA_VERSION = "paired_order_lifecycle_smoke.v2"
ACTION_ORDER = ("closer_1tick", "current", "farther_1tick")
ACTION_DELTAS = {
    "closer_1tick": -1,
    "current": 0,
    "farther_1tick": 1,
}
HORIZONS_MS = (1_000, 5_000, 10_000)


def action_price_tick(side: str, current_tick: int, action: str) -> int:
    """Return a price tick from a positive distance-to-BBO delta."""

    if action not in ACTION_DELTAS:
        raise ValueError(f"unsupported paired placement action={action!r}")
    direction = 1 if str(side).upper() == "BUY" else -1
    return int(current_tick) - direction * int(ACTION_DELTAS[action])


def _floor_lot(quantity: float, lot_size: float) -> float:
    if lot_size <= 0.0:
        raise ValueError("lot_size must be positive")
    units = math.floor(max(0.0, float(quantity)) / lot_size + 1e-12)
    return float(units) * float(lot_size)


@dataclass
class PlacementChild:
    action: str
    distance_delta_ticks: int
    side: str
    price_tick: int
    quantity: float
    submit_ts_ns: int
    activate_ts_ns: int
    cancel_request_ts_ns: int
    cancel_ack_ts_ns: int
    observation_end_ts_ns: int
    queue_deplete_mult: float = 1.0
    lot_size: float = 0.001
    state: str = "pending_new"
    activation_status: str = "pending"
    activation_queue_status: str = "unresolved"
    activation_queue_reason: str = ""
    activation_queue_asof_ts_ns: int = 0
    activation_queue_segment_id: int = 0
    activation_queue_qty: float = math.nan
    queue_left: float = math.nan
    queue_path_valid: bool = False
    queue_invalid_reason: str = ""
    queue_trade_since_update: float = 0.0
    first_touch_ts_ns: int = 0
    first_touch_type: str = ""
    exact_touch_ts_ns: int = 0
    through_touch_ts_ns: int = 0
    first_fill_ts_ns: int = 0
    first_fill_mechanism: str = ""
    fill_qty: float = 0.0
    remaining_qty: float = field(init=False)
    partial_fill_count: int = 0
    full_fill_ts_ns: int = 0
    fill_while_cancel_pending_qty: float = 0.0
    first_pending_cancel_fill_ts_ns: int = 0
    cancel_requested: bool = False
    cancel_acked: bool = False
    request_state_observed: bool = False
    request_order_state_before: str = ""
    request_order_age_ms: float = math.nan
    request_remaining_qty: float = math.nan
    request_queue_left: float = math.nan
    request_queue_path_valid: bool = False
    request_native_cancel_count: int = 0
    request_native_cancel_qty: float = 0.0
    request_native_refill_count: int = 0
    request_native_refill_qty: float = 0.0
    request_native_level_event_count: int = 0
    terminal_ts_ns: int = 0
    terminal_reason: str = ""
    native_cancel_count: int = 0
    native_cancel_qty: float = 0.0
    native_refill_count: int = 0
    native_refill_qty: float = 0.0
    native_level_event_count: int = 0
    same_ms_ambiguity_count: int = 0

    def __post_init__(self) -> None:
        self.side = str(self.side).upper()
        if self.side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported side={self.side!r}")
        if self.action not in ACTION_DELTAS:
            raise ValueError(f"unsupported action={self.action!r}")
        if self.quantity <= 0.0:
            raise ValueError("quantity must be positive")
        self.remaining_qty = float(self.quantity)

    @property
    def active(self) -> bool:
        return self.state in {"open", "pending_cancel"}

    @property
    def filled(self) -> bool:
        return self.first_fill_ts_ns > 0

    @property
    def fully_filled(self) -> bool:
        return self.full_fill_ts_ns > 0

    def invalidate_queue(self, reason: str) -> None:
        if self.queue_path_valid:
            self.queue_path_valid = False
        if not self.queue_invalid_reason:
            self.queue_invalid_reason = str(reason)

    def activate(
        self,
        *,
        lookup: ExchangeBookLookup,
        best_bid_tick: int,
        best_ask_tick: int,
        same_boundary_native_event: bool,
    ) -> None:
        if self.state != "pending_new":
            return
        if self.cancel_ack_ts_ns and self.cancel_ack_ts_ns <= self.activate_ts_ns:
            self.state = "cancelled"
            self.activation_status = "cancelled_before_activation"
            self.terminal_ts_ns = int(self.cancel_ack_ts_ns)
            self.terminal_reason = "cancel_ack_before_activation"
            return
        invalid_book = best_bid_tick <= 0 or best_ask_tick <= best_bid_tick
        would_cross = (
            self.price_tick >= best_ask_tick
            if self.side == "BUY"
            else self.price_tick <= best_bid_tick
        )
        if invalid_book or would_cross:
            self.state = "rejected"
            self.activation_status = (
                "invalid_book" if invalid_book else "gtx_reject"
            )
            self.terminal_ts_ns = int(self.activate_ts_ns)
            self.terminal_reason = self.activation_status
            return

        strictly_before = int(lookup.asof_exchange_ts_ns) < int(
            self.activate_ts_ns
        )
        self.activation_queue_status = (
            str(lookup.status) if strictly_before else "ambiguous"
        )
        self.activation_queue_reason = (
            str(lookup.reason)
            if strictly_before
            else "same_boundary_state_not_strictly_before_activation"
        )
        self.activation_queue_asof_ts_ns = int(lookup.asof_exchange_ts_ns)
        self.activation_queue_segment_id = int(lookup.segment_id)
        self.queue_path_valid = bool(
            lookup.strict_usable
            and strictly_before
            and not same_boundary_native_event
        )
        if lookup.quantity is not None and strictly_before:
            self.activation_queue_qty = max(0.0, float(lookup.quantity))
            self.queue_left = float(self.activation_queue_qty)
        else:
            self.queue_left = math.nan
        if same_boundary_native_event:
            self.same_ms_ambiguity_count += 1
            self.invalidate_queue("same_boundary_activation_book_event")
        elif not self.queue_path_valid:
            self.invalidate_queue(self.activation_queue_reason)
        self.state = "pending_cancel" if self.cancel_requested else "open"
        self.activation_status = "active"

    def request_cancel(self, ts_ns: int) -> None:
        if self.state in {"filled", "cancelled", "rejected", "censored"}:
            return
        if not self.request_state_observed:
            self.request_state_observed = True
            self.request_order_state_before = str(self.state)
            self.request_order_age_ms = (
                max(0.0, (int(ts_ns) - self.activate_ts_ns) / 1_000_000.0)
                if self.activation_status == "active"
                else math.nan
            )
            self.request_remaining_qty = float(self.remaining_qty)
            self.request_queue_left = float(self.queue_left)
            self.request_queue_path_valid = bool(self.queue_path_valid)
            self.request_native_cancel_count = int(self.native_cancel_count)
            self.request_native_cancel_qty = float(self.native_cancel_qty)
            self.request_native_refill_count = int(self.native_refill_count)
            self.request_native_refill_qty = float(self.native_refill_qty)
            self.request_native_level_event_count = int(
                self.native_level_event_count
            )
        self.cancel_requested = True
        if self.state == "open":
            self.state = "pending_cancel"

    def acknowledge_cancel(self, ts_ns: int) -> None:
        if self.state in {"filled", "cancelled", "rejected", "censored"}:
            return
        self.cancel_acked = True
        self.state = "cancelled"
        self.terminal_ts_ns = int(ts_ns)
        self.terminal_reason = "cancel_ack"

    def invalidate_native_path(self, reason: str) -> None:
        if self.active:
            self.invalidate_queue(reason)

    def apply_level_change(
        self,
        change: ExchangeBookLevelChange,
        *,
        ambiguous_with_trade_or_activation: bool,
    ) -> None:
        native_side = "bid" if self.side == "BUY" else "ask"
        if (
            not self.active
            or change.side != native_side
            or int(change.price_tick) != int(self.price_tick)
        ):
            return
        self.native_level_event_count += 1
        if ambiguous_with_trade_or_activation:
            self.same_ms_ambiguity_count += 1
            self.queue_trade_since_update = 0.0
            self.invalidate_queue("same_ms_native_trade_or_activation")
            return
        if not self.queue_path_valid:
            self.queue_trade_since_update = 0.0
            return

        before = max(0.0, float(change.quantity_before))
        after = max(0.0, float(change.quantity_after))
        decrease = max(0.0, before - after)
        increase = max(0.0, after - before)
        explained_trade = min(before, max(0.0, self.queue_trade_since_update))
        cancellation = max(0.0, decrease - explained_trade)
        if cancellation > 0.0:
            ahead = max(0.0, float(self.queue_left))
            public_after_trade = max(0.0, before - explained_trade)
            behind = max(0.0, public_after_trade - ahead)
            denominator = ahead + behind
            ahead_probability = ahead / denominator if denominator > 0.0 else 0.0
            removed = min(ahead, cancellation * ahead_probability)
            self.queue_left = ahead - removed
            self.native_cancel_count += 1
            self.native_cancel_qty += float(cancellation)
        if increase > 0.0:
            self.native_refill_count += 1
            self.native_refill_qty += float(increase)
        self.queue_trade_since_update = 0.0

    def apply_trade(
        self,
        *,
        ts_ns: int,
        trade_price_tick: int,
        trade_qty: float,
        is_buyer_maker: bool,
    ) -> None:
        if not self.active or trade_qty <= 0.0:
            return
        passive_side = "BUY" if bool(is_buyer_maker) else "SELL"
        if passive_side != self.side:
            return
        exact = int(trade_price_tick) == int(self.price_tick)
        through = (
            int(trade_price_tick) < int(self.price_tick)
            if self.side == "BUY"
            else int(trade_price_tick) > int(self.price_tick)
        )
        if not exact and not through:
            return
        if self.first_touch_ts_ns <= 0:
            self.first_touch_ts_ns = int(ts_ns)
            self.first_touch_type = "exact" if exact else "through"
        if exact and self.exact_touch_ts_ns <= 0:
            self.exact_touch_ts_ns = int(ts_ns)
        if through and self.through_touch_ts_ns <= 0:
            self.through_touch_ts_ns = int(ts_ns)

        if through:
            available = float(self.remaining_qty)
            mechanism = "strict_through"
            self.queue_left = 0.0
        else:
            self.queue_trade_since_update += float(trade_qty)
            if not self.queue_path_valid or not math.isfinite(self.queue_left):
                return
            available = float(trade_qty) * max(
                0.0, float(self.queue_deplete_mult)
            )
            eaten = min(max(0.0, self.queue_left), available)
            self.queue_left = max(0.0, self.queue_left - eaten)
            available -= eaten
            mechanism = "exact_queue"

        fill_qty = _floor_lot(
            min(max(0.0, available), self.remaining_qty),
            self.lot_size,
        )
        if fill_qty < self.lot_size:
            return
        if self.first_fill_ts_ns <= 0:
            self.first_fill_ts_ns = int(ts_ns)
            self.first_fill_mechanism = mechanism
        self.fill_qty += float(fill_qty)
        self.remaining_qty = max(0.0, self.remaining_qty - fill_qty)
        if self.cancel_requested and not self.cancel_acked:
            if self.first_pending_cancel_fill_ts_ns <= 0:
                self.first_pending_cancel_fill_ts_ns = int(ts_ns)
            self.fill_while_cancel_pending_qty += float(fill_qty)
        if self.remaining_qty < self.lot_size:
            self.remaining_qty = 0.0
            self.full_fill_ts_ns = int(ts_ns)
            self.state = "filled"
            self.terminal_ts_ns = int(ts_ns)
            self.terminal_reason = mechanism
        else:
            self.partial_fill_count += 1

    def censor(self, ts_ns: int) -> None:
        if self.state in {"filled", "cancelled", "rejected"}:
            return
        self.state = "censored"
        self.terminal_ts_ns = int(ts_ns)
        self.terminal_reason = "administrative_censor"

    def as_record(self) -> dict[str, Any]:
        observed_end = int(
            self.terminal_ts_ns or self.observation_end_ts_ns
        )
        active_duration_ms = (
            max(0.0, (observed_end - self.activate_ts_ns) / 1_000_000.0)
            if self.activation_status == "active"
            else 0.0
        )
        terminal_observed = self.state in {"filled", "cancelled", "rejected"}
        record: dict[str, Any] = {
            "action": self.action,
            "distance_delta_ticks": int(self.distance_delta_ticks),
            "price_tick": int(self.price_tick),
            "requested_price_tick": int(self.price_tick),
            "effective_price_tick": (
                int(self.price_tick)
                if self.activation_status == "active"
                else None
            ),
            "activation_ts_ns": (
                int(self.activate_ts_ns)
                if self.activation_status != "pending"
                else 0
            ),
            "activation_status": self.activation_status,
            "activation_queue_status": self.activation_queue_status,
            "activation_queue_reason": self.activation_queue_reason,
            "activation_queue_asof_ts_ns": int(
                self.activation_queue_asof_ts_ns
            ),
            "activation_queue_segment_id": int(
                self.activation_queue_segment_id
            ),
            "activation_queue_qty": float(self.activation_queue_qty),
            "queue_path_valid": int(self.queue_path_valid),
            "queue_invalid_reason": self.queue_invalid_reason,
            "first_touch_ts_ns": int(self.first_touch_ts_ns),
            "first_touch_type": self.first_touch_type,
            "exact_touch_ts_ns": int(self.exact_touch_ts_ns),
            "through_touch_ts_ns": int(self.through_touch_ts_ns),
            "first_fill_ts_ns": int(self.first_fill_ts_ns),
            "first_fill_mechanism": self.first_fill_mechanism,
            "fill_qty": float(self.fill_qty),
            "remaining_qty": float(self.remaining_qty),
            "full_fill_ts_ns": int(self.full_fill_ts_ns),
            "partial_fill_count": int(self.partial_fill_count),
            "cancel_request_ts_ns": int(self.cancel_request_ts_ns),
            "request_state_observed": int(self.request_state_observed),
            "request_order_state_before": self.request_order_state_before,
            "request_order_age_ms": float(self.request_order_age_ms),
            "request_remaining_qty": float(self.request_remaining_qty),
            "request_queue_left": float(self.request_queue_left),
            "request_queue_path_valid": int(self.request_queue_path_valid),
            "request_native_cancel_count": int(
                self.request_native_cancel_count
            ),
            "request_native_cancel_qty": float(self.request_native_cancel_qty),
            "request_native_refill_count": int(
                self.request_native_refill_count
            ),
            "request_native_refill_qty": float(self.request_native_refill_qty),
            "request_native_level_event_count": int(
                self.request_native_level_event_count
            ),
            "cancel_ack_ts_ns": int(self.cancel_ack_ts_ns),
            "cancel_acked": int(self.cancel_acked),
            "fill_while_cancel_pending_qty": float(
                self.fill_while_cancel_pending_qty
            ),
            "first_pending_cancel_fill_ts_ns": int(
                self.first_pending_cancel_fill_ts_ns
            ),
            "terminal_state": self.state,
            "terminal_ts_ns": int(self.terminal_ts_ns),
            "terminal_reason": self.terminal_reason,
            "terminal_observed": int(terminal_observed),
            "active_duration_ms": float(active_duration_ms),
            "native_cancel_count": int(self.native_cancel_count),
            "native_cancel_qty": float(self.native_cancel_qty),
            "native_refill_count": int(self.native_refill_count),
            "native_refill_qty": float(self.native_refill_qty),
            "native_level_event_count": int(self.native_level_event_count),
            "same_ms_ambiguity_count": int(self.same_ms_ambiguity_count),
        }
        for horizon_ms in HORIZONS_MS:
            active_horizon_ns = (
                self.activate_ts_ns + horizon_ms * 1_000_000
            )
            placement_horizon_ns = (
                self.submit_ts_ns + horizon_ms * 1_000_000
            )
            terminal_before_active_horizon = bool(
                terminal_observed
                and self.terminal_ts_ns > 0
                and self.terminal_ts_ns <= active_horizon_ns
            )
            terminal_before_placement_horizon = bool(
                terminal_observed
                and self.terminal_ts_ns > 0
                and self.terminal_ts_ns <= placement_horizon_ns
            )
            active_observed = bool(
                self.activation_status == "active"
                and (
                    active_duration_ms >= float(horizon_ms)
                    or terminal_before_active_horizon
                )
            )
            placement_observed = bool(
                self.observation_end_ts_ns >= placement_horizon_ns
                or terminal_before_placement_horizon
            )
            active_filled = bool(
                self.first_fill_ts_ns > 0
                and self.first_fill_ts_ns <= active_horizon_ns
            )
            placement_filled = bool(
                self.first_fill_ts_ns > 0
                and self.first_fill_ts_ns <= placement_horizon_ns
            )
            record[f"active_observed_{horizon_ms}ms"] = int(
                active_observed
            )
            record[f"active_filled_{horizon_ms}ms"] = int(
                active_observed and active_filled
            )
            record[f"placement_observed_{horizon_ms}ms"] = int(
                placement_observed
            )
            record[f"placement_filled_{horizon_ms}ms"] = int(
                placement_observed and placement_filled
            )
        return record


@dataclass
class PlacementCohort:
    cohort_id: str
    decision_id: str
    day: str
    side: str
    inventory_role: str
    campaign_id: int
    submit_ts_ns: int
    activate_ts_ns: int
    cancel_request_ts_ns: int
    cancel_ack_ts_ns: int
    observation_end_ts_ns: int
    baseline_price_tick: int
    quantity: float
    children: dict[str, PlacementChild]
    cancel_request_reason: str = ""
    decision_features: dict[str, Any] = field(default_factory=dict)
    monotonicity_violations: list[str] = field(default_factory=list)

    @classmethod
    def create(
        cls,
        *,
        cohort_id: str,
        decision_id: str,
        day: str,
        side: str,
        inventory_role: str,
        campaign_id: int,
        submit_ts_ns: int,
        activate_ts_ns: int,
        cancel_request_ts_ns: int,
        cancel_ack_ts_ns: int,
        observation_end_ts_ns: int,
        baseline_price_tick: int,
        quantity: float,
        queue_deplete_mult: float,
        lot_size: float,
        decision_features: dict[str, Any],
        cancel_request_reason: str = "",
    ) -> PlacementCohort:
        children = {
            action: PlacementChild(
                action=action,
                distance_delta_ticks=ACTION_DELTAS[action],
                side=side,
                price_tick=action_price_tick(
                    side, baseline_price_tick, action
                ),
                quantity=quantity,
                submit_ts_ns=submit_ts_ns,
                activate_ts_ns=activate_ts_ns,
                cancel_request_ts_ns=cancel_request_ts_ns,
                cancel_ack_ts_ns=cancel_ack_ts_ns,
                observation_end_ts_ns=observation_end_ts_ns,
                queue_deplete_mult=queue_deplete_mult,
                lot_size=lot_size,
            )
            for action in ACTION_ORDER
        }
        return cls(
            cohort_id=cohort_id,
            decision_id=decision_id,
            day=day,
            side=str(side).upper(),
            inventory_role=str(inventory_role),
            campaign_id=int(campaign_id),
            submit_ts_ns=int(submit_ts_ns),
            activate_ts_ns=int(activate_ts_ns),
            cancel_request_ts_ns=int(cancel_request_ts_ns),
            cancel_ack_ts_ns=int(cancel_ack_ts_ns),
            observation_end_ts_ns=int(observation_end_ts_ns),
            baseline_price_tick=int(baseline_price_tick),
            quantity=float(quantity),
            children=children,
            cancel_request_reason=str(cancel_request_reason),
            decision_features=dict(decision_features),
        )

    def check_monotonicity(self) -> None:
        ordered = [self.children[action] for action in ACTION_ORDER]
        for index in range(len(ordered) - 1):
            shallow = ordered[index]
            deep = ordered[index + 1]
            comparable = bool(
                shallow.activation_status == "active"
                and deep.activation_status == "active"
                and shallow.queue_path_valid
                and deep.queue_path_valid
            )
            if not comparable:
                continue
            if deep.filled and not shallow.filled:
                self.monotonicity_violations.append(
                    f"{deep.action}_filled_without_{shallow.action}"
                )
            if deep.fill_qty > shallow.fill_qty + 1e-12:
                self.monotonicity_violations.append(
                    f"{deep.action}_qty_exceeds_{shallow.action}"
                )

    def as_wide_record(self) -> dict[str, Any]:
        self.check_monotonicity()
        row: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "cohort_id": self.cohort_id,
            "decision_id": self.decision_id,
            "day": self.day,
            "side": self.side,
            "inventory_role": self.inventory_role,
            "campaign_id": int(self.campaign_id),
            "submit_ts_ns": int(self.submit_ts_ns),
            "activate_ts_ns": int(self.activate_ts_ns),
            "cancel_request_ts_ns": int(self.cancel_request_ts_ns),
            "cancel_request_reason": str(self.cancel_request_reason),
            "cancel_ack_ts_ns": int(self.cancel_ack_ts_ns),
            "observation_end_ts_ns": int(self.observation_end_ts_ns),
            "baseline_price_tick": int(self.baseline_price_tick),
            "quantity": float(self.quantity),
            "monotonicity_violation_count": int(
                len(set(self.monotonicity_violations))
            ),
            "monotonicity_violations": "|".join(
                sorted(set(self.monotonicity_violations))
            ),
        }
        row.update(self.decision_features)
        for action in ACTION_ORDER:
            for name, value in self.children[action].as_record().items():
                row[f"{action}__{name}"] = value
        return row
