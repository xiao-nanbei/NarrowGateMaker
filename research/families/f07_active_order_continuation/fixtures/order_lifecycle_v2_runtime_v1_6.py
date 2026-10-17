"""Shared order-lifecycle clock and quantity-weighted exposure contract."""

from __future__ import annotations

from copy import copy
from dataclasses import asdict, dataclass, field
from enum import Enum

from execution.order_lifecycle_quantity_contract import (
    PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC,
    QUANTITY_INCREASE_ABS_TOLERANCE_BTC,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
    is_terminal_zero,
    validate_fill_terminal_claim,
)


class OrderLifecyclePhase(str, Enum):
    """Exchange order state followed by the separate policy recovery state."""

    SUBMITTED = "SUBMITTED"
    ACTIVE = "ACTIVE"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    EXCHANGE_TERMINAL = "EXCHANGE_TERMINAL"
    POST_CANCEL_RECOVERY = "POST_CANCEL_RECOVERY"
    REENTRY_ELIGIBLE = "REENTRY_ELIGIBLE"


class TerminalPolicyRoute(str, Enum):
    """Policy route after the exchange order has left its fill-risk set."""

    PROSPECTIVE_CANCEL_REENTRY = "PROSPECTIVE_CANCEL_REENTRY"
    TERMINAL_COMPLETE = "TERMINAL_COMPLETE"
    BASELINE_RESUBMIT = "BASELINE_RESUBMIT"
    SHUTDOWN_NO_REENTRY = "SHUTDOWN_NO_REENTRY"
    UNSUPPORTED = "UNSUPPORTED"


_CANCEL_RECOVERY_REASONS = frozenset({"cancel_ack", "cancel_ack_reconciled"})
_BASELINE_RESUBMIT_REASONS = frozenset({"expired", "rejected"})
_SHUTDOWN_REASONS = frozenset(
    {"administrative_cancel", "local_shutdown_cancel", "shutdown"}
)


def terminal_policy_route(
    reason: str,
    remaining_quantity: float,
) -> TerminalPolicyRoute:
    """Classify terminal policy semantics without authorizing a new order."""

    normalized = str(reason).strip().lower()
    if normalized in {"full_fill", "filled_before_cancel_ack"}:
        return (
            TerminalPolicyRoute.TERMINAL_COMPLETE
            if is_terminal_zero(remaining_quantity)
            else TerminalPolicyRoute.UNSUPPORTED
        )
    if is_terminal_zero(remaining_quantity):
        return TerminalPolicyRoute.TERMINAL_COMPLETE
    if normalized in _CANCEL_RECOVERY_REASONS:
        return TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY
    if normalized in _BASELINE_RESUBMIT_REASONS:
        return TerminalPolicyRoute.BASELINE_RESUBMIT
    if normalized in _SHUTDOWN_REASONS:
        return TerminalPolicyRoute.SHUTDOWN_NO_REENTRY
    return TerminalPolicyRoute.UNSUPPORTED


FILL_RISK_PHASES = frozenset(
    {
        OrderLifecyclePhase.ACTIVE,
        OrderLifecyclePhase.PARTIALLY_FILLED,
        OrderLifecyclePhase.CANCEL_PENDING,
    }
)


_ALLOWED_TRANSITIONS = {
    OrderLifecyclePhase.SUBMITTED: {
        OrderLifecyclePhase.ACTIVE,
        OrderLifecyclePhase.EXCHANGE_TERMINAL,
    },
    OrderLifecyclePhase.ACTIVE: {
        OrderLifecyclePhase.PARTIALLY_FILLED,
        OrderLifecyclePhase.CANCEL_PENDING,
        OrderLifecyclePhase.EXCHANGE_TERMINAL,
    },
    OrderLifecyclePhase.PARTIALLY_FILLED: {
        OrderLifecyclePhase.CANCEL_PENDING,
        OrderLifecyclePhase.EXCHANGE_TERMINAL,
    },
    OrderLifecyclePhase.CANCEL_PENDING: {
        OrderLifecyclePhase.ACTIVE,
        OrderLifecyclePhase.PARTIALLY_FILLED,
        OrderLifecyclePhase.EXCHANGE_TERMINAL,
    },
    OrderLifecyclePhase.EXCHANGE_TERMINAL: {
        OrderLifecyclePhase.POST_CANCEL_RECOVERY,
    },
    OrderLifecyclePhase.POST_CANCEL_RECOVERY: {
        OrderLifecyclePhase.REENTRY_ELIGIBLE,
    },
    OrderLifecyclePhase.REENTRY_ELIGIBLE: set(),
}


@dataclass(frozen=True)
class OrderLifecycleEvent:
    sequence: int
    event: str
    visibility_ts_ns: int
    exchange_ts_ns: int
    phase_before: str
    phase_after: str
    remaining_qty_before: float
    remaining_qty_after: float
    quantity_time_exposure_btc_s: float
    quantity_time_exposure_visible_btc_s: float
    quantity_time_exposure_exchange_btc_s: float | None
    exchange_exposure_valid: bool
    reason: str = ""


@dataclass
class QuantityWeightedOrderLifecycle:
    """Track one order's causal lifecycle and remaining-quantity exposure.

    Exposure accrues only while the exchange order can still fill. Policy
    recovery after exchange terminal is represented explicitly but has zero
    fill-risk exposure and cannot inherit the old order's queue or age.
    """

    initial_quantity: float
    submitted_ts_ns: int
    phase: OrderLifecyclePhase = OrderLifecyclePhase.SUBMITTED
    remaining_quantity: float = field(init=False)
    activation_ts_ns: int = 0
    activation_exchange_ts_ns: int = 0
    first_fill_ts_ns: int = 0
    first_fill_exchange_ts_ns: int = 0
    cancel_request_ts_ns: int = 0
    terminal_ts_ns: int = 0
    terminal_exchange_ts_ns: int = 0
    post_cancel_recovery_ts_ns: int = 0
    reentry_eligible_ts_ns: int = 0
    terminal_reason: str = ""
    terminal_policy_route_name: str = ""
    quantity_time_exposure_btc_s: float = 0.0
    quantity_time_exposure_exchange_accumulated_btc_s: float = 0.0
    exchange_exposure_valid: bool = True
    exchange_exposure_complete: bool = False
    exchange_exposure_invalid_reason: str = ""
    _last_event_ts_ns: int = field(init=False, repr=False)
    _last_exchange_event_ts_ns: int = field(default=0, init=False, repr=False)
    _events: list[OrderLifecycleEvent] = field(
        default_factory=list,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.initial_quantity = float(self.initial_quantity)
        self.submitted_ts_ns = int(self.submitted_ts_ns)
        if self.initial_quantity < 0.0:
            raise ValueError("order quantity must be non-negative")
        if self.submitted_ts_ns <= 0:
            raise ValueError("submitted timestamp must be positive")
        self.remaining_quantity = self.initial_quantity
        self._last_event_ts_ns = self.submitted_ts_ns
        self._record(
            event="submit",
            visibility_ts_ns=self.submitted_ts_ns,
            exchange_ts_ns=0,
            phase_before=self.phase,
            remaining_before=self.remaining_quantity,
            reason="",
        )

    @property
    def fill_risk_active(self) -> bool:
        return bool(
            self.phase in FILL_RISK_PHASES
            and self.remaining_quantity > TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
        )

    @property
    def first_fill_latency_s(self) -> float | None:
        if self.activation_ts_ns <= 0 or self.first_fill_ts_ns <= 0:
            return None
        return max(
            0.0,
            (self.first_fill_ts_ns - self.activation_ts_ns)
            / 1_000_000_000.0,
        )

    @property
    def first_fill_latency_exchange_s(self) -> float | None:
        if (
            self.activation_exchange_ts_ns <= 0
            or self.first_fill_exchange_ts_ns <= 0
            or self.first_fill_exchange_ts_ns < self.activation_exchange_ts_ns
        ):
            return None
        return max(
            0.0,
            (
                self.first_fill_exchange_ts_ns
                - self.activation_exchange_ts_ns
            )
            / 1_000_000_000.0,
        )

    def _invalidate_exchange_exposure(self, reason: str) -> None:
        if self.exchange_exposure_valid:
            self.exchange_exposure_invalid_reason = str(reason)
        self.exchange_exposure_valid = False

    def _validate_exchange_timestamp(
        self,
        exchange_ts_ns: int,
        visibility_ts_ns: int,
        *,
        event: str,
    ) -> int:
        timestamp = int(exchange_ts_ns)
        if timestamp <= 0:
            self._invalidate_exchange_exposure(
                f"missing_exchange_timestamp:{event}"
            )
            return 0
        if timestamp > int(visibility_ts_ns):
            self._invalidate_exchange_exposure(
                f"exchange_timestamp_after_visibility:{event}"
            )
        if (
            self._last_exchange_event_ts_ns > 0
            and timestamp < self._last_exchange_event_ts_ns
        ):
            self._invalidate_exchange_exposure(
                f"exchange_timestamp_regressed:{event}"
            )
        return timestamp

    def _observe_exchange_activation(
        self,
        exchange_ts_ns: int,
        visibility_ts_ns: int,
    ) -> None:
        timestamp = self._validate_exchange_timestamp(
            exchange_ts_ns,
            visibility_ts_ns,
            event="activate",
        )
        if timestamp <= 0:
            return
        if self.activation_exchange_ts_ns <= 0:
            self.activation_exchange_ts_ns = timestamp
            self._last_exchange_event_ts_ns = timestamp
            return
        self._accrue_exchange_to(
            timestamp,
            visibility_ts_ns,
            event="activate",
        )

    def _accrue_exchange_to(
        self,
        exchange_ts_ns: int,
        visibility_ts_ns: int,
        *,
        event: str,
    ) -> None:
        timestamp = self._validate_exchange_timestamp(
            exchange_ts_ns,
            visibility_ts_ns,
            event=event,
        )
        if timestamp <= 0:
            return
        if self.activation_exchange_ts_ns <= 0:
            self._invalidate_exchange_exposure(
                f"activation_exchange_timestamp_missing_before:{event}"
            )
            self._last_exchange_event_ts_ns = max(
                self._last_exchange_event_ts_ns,
                timestamp,
            )
            return
        if (
            self.exchange_exposure_valid
            and self.fill_risk_active
            and timestamp >= self._last_exchange_event_ts_ns
        ):
            self.quantity_time_exposure_exchange_accumulated_btc_s += (
                self.remaining_quantity
                * (timestamp - self._last_exchange_event_ts_ns)
                / 1_000_000_000.0
            )
        self._last_exchange_event_ts_ns = max(
            self._last_exchange_event_ts_ns,
            timestamp,
        )

    def _accrue(self, visibility_ts_ns: int) -> tuple[float, float]:
        now_ns = int(visibility_ts_ns)
        if now_ns < self._last_event_ts_ns:
            raise ValueError("order lifecycle visibility time regressed")
        remaining_before = self.remaining_quantity
        if self.fill_risk_active:
            self.quantity_time_exposure_btc_s += (
                remaining_before
                * (now_ns - self._last_event_ts_ns)
                / 1_000_000_000.0
            )
        self._last_event_ts_ns = now_ns
        return remaining_before, self.quantity_time_exposure_btc_s

    def _record(
        self,
        *,
        event: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int,
        phase_before: OrderLifecyclePhase,
        remaining_before: float,
        reason: str,
    ) -> None:
        self._events.append(
            OrderLifecycleEvent(
                sequence=len(self._events) + 1,
                event=str(event),
                visibility_ts_ns=int(visibility_ts_ns),
                exchange_ts_ns=max(0, int(exchange_ts_ns)),
                phase_before=phase_before.value,
                phase_after=self.phase.value,
                remaining_qty_before=float(remaining_before),
                remaining_qty_after=float(self.remaining_quantity),
                quantity_time_exposure_btc_s=float(
                    self.quantity_time_exposure_btc_s
                ),
                quantity_time_exposure_visible_btc_s=float(
                    self.quantity_time_exposure_btc_s
                ),
                quantity_time_exposure_exchange_btc_s=(
                    float(
                        self.quantity_time_exposure_exchange_accumulated_btc_s
                    )
                    if self.exchange_exposure_valid
                    and self.activation_exchange_ts_ns > 0
                    else None
                ),
                exchange_exposure_valid=bool(self.exchange_exposure_valid),
                reason=str(reason),
            )
        )

    def _transition(
        self,
        target: OrderLifecyclePhase,
        *,
        event: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int = 0,
        reason: str = "",
    ) -> None:
        before = self.phase
        remaining_before, _ = self._accrue(visibility_ts_ns)
        if target == before:
            self._record(
                event=event,
                visibility_ts_ns=visibility_ts_ns,
                exchange_ts_ns=exchange_ts_ns,
                phase_before=before,
                remaining_before=remaining_before,
                reason=reason,
            )
            return
        if target not in _ALLOWED_TRANSITIONS[before]:
            raise ValueError(
                f"invalid order lifecycle transition {before.value} -> "
                f"{target.value}"
            )
        self.phase = target
        self._record(
            event=event,
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
            phase_before=before,
            remaining_before=remaining_before,
            reason=reason,
        )

    def activate(
        self,
        visibility_ts_ns: int,
        *,
        exchange_ts_ns: int = 0,
    ) -> None:
        self._observe_exchange_activation(
            exchange_ts_ns,
            visibility_ts_ns,
        )
        self._transition(
            OrderLifecyclePhase.ACTIVE,
            event="activate",
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
        )
        if self.activation_ts_ns <= 0:
            self.activation_ts_ns = int(visibility_ts_ns)

    def request_cancel(self, visibility_ts_ns: int) -> None:
        if self.phase == OrderLifecyclePhase.CANCEL_PENDING:
            return
        self._transition(
            OrderLifecyclePhase.CANCEL_PENDING,
            event="cancel_request",
            visibility_ts_ns=visibility_ts_ns,
        )
        if self.cancel_request_ts_ns <= 0:
            self.cancel_request_ts_ns = int(visibility_ts_ns)

    def cancel_rejected(
        self,
        visibility_ts_ns: int,
        *,
        exchange_ts_ns: int = 0,
    ) -> None:
        """Resume the exchange risk set without losing partial-fill identity."""

        target = (
            OrderLifecyclePhase.PARTIALLY_FILLED
            if self.remaining_quantity
            < self.initial_quantity - PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
            else OrderLifecyclePhase.ACTIVE
        )
        if int(exchange_ts_ns) > 0:
            self._accrue_exchange_to(
                exchange_ts_ns,
                visibility_ts_ns,
                event="cancel_rejected",
            )
        self._transition(
            target,
            event="cancel_rejected",
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
        )

    def observe_fill(
        self,
        *,
        remaining_after: float,
        visibility_ts_ns: int,
        exchange_ts_ns: int = 0,
        full_fill: bool = False,
    ) -> None:
        if self.phase not in FILL_RISK_PHASES:
            raise ValueError("fill observed outside the exchange fill-risk set")
        remaining, terminal = validate_fill_terminal_claim(
            remaining_after=remaining_after,
            full_fill_claimed=full_fill,
        )
        before = self.phase
        self._accrue_exchange_to(
            exchange_ts_ns,
            visibility_ts_ns,
            event="full_fill" if terminal else "partial_fill",
        )
        remaining_before, _ = self._accrue(visibility_ts_ns)
        if remaining > remaining_before + QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
            raise ValueError("remaining order quantity increased after fill")
        self.remaining_quantity = remaining
        if (
            self.first_fill_ts_ns <= 0
            and remaining
            < remaining_before - PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
        ):
            self.first_fill_ts_ns = int(visibility_ts_ns)
            if int(exchange_ts_ns) > 0:
                self.first_fill_exchange_ts_ns = int(exchange_ts_ns)
        if terminal:
            self.phase = OrderLifecyclePhase.EXCHANGE_TERMINAL
            self.terminal_ts_ns = int(visibility_ts_ns)
            self.terminal_exchange_ts_ns = max(0, int(exchange_ts_ns))
            self.terminal_reason = "full_fill"
            self.terminal_policy_route_name = (
                TerminalPolicyRoute.TERMINAL_COMPLETE.value
            )
            self.exchange_exposure_complete = bool(
                self.exchange_exposure_valid
                and self.activation_exchange_ts_ns > 0
                and self.terminal_exchange_ts_ns > 0
            )
            event = "full_fill"
        else:
            if before != OrderLifecyclePhase.CANCEL_PENDING:
                self.phase = OrderLifecyclePhase.PARTIALLY_FILLED
            event = "partial_fill"
        self._record(
            event=event,
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
            phase_before=before,
            remaining_before=remaining_before,
            reason="",
        )

    def exchange_terminal(
        self,
        visibility_ts_ns: int,
        *,
        reason: str,
        exchange_ts_ns: int = 0,
    ) -> None:
        if self.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL:
            return
        route = terminal_policy_route(reason, self.remaining_quantity)
        if route == TerminalPolicyRoute.UNSUPPORTED:
            raise ValueError(f"unsupported order terminal reason: {reason}")
        was_fill_risk_active = self.fill_risk_active
        if was_fill_risk_active:
            self._accrue_exchange_to(
                exchange_ts_ns,
                visibility_ts_ns,
                event=f"exchange_terminal:{reason}",
            )
        self._transition(
            OrderLifecyclePhase.EXCHANGE_TERMINAL,
            event="exchange_terminal",
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
            reason=reason,
        )
        self.terminal_ts_ns = int(visibility_ts_ns)
        self.terminal_exchange_ts_ns = max(0, int(exchange_ts_ns))
        self.terminal_reason = str(reason)
        self.terminal_policy_route_name = route.value
        if not was_fill_risk_active:
            self.exchange_exposure_complete = True
        else:
            self.exchange_exposure_complete = bool(
                self.exchange_exposure_valid
                and self.activation_exchange_ts_ns > 0
                and self.terminal_exchange_ts_ns > 0
            )

    def enter_post_cancel_recovery(self, visibility_ts_ns: int) -> None:
        route = terminal_policy_route(
            self.terminal_reason,
            self.remaining_quantity,
        )
        if route != TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY:
            raise ValueError(
                "post-cancel recovery requires cancel ACK with remaining quantity; "
                f"got {route.value}"
            )
        self._transition(
            OrderLifecyclePhase.POST_CANCEL_RECOVERY,
            event="post_cancel_recovery",
            visibility_ts_ns=visibility_ts_ns,
            reason="old_order_risk_set_ended",
        )
        self.post_cancel_recovery_ts_ns = int(visibility_ts_ns)

    def mark_reentry_eligible(self, visibility_ts_ns: int) -> None:
        self._transition(
            OrderLifecyclePhase.REENTRY_ELIGIBLE,
            event="reentry_eligible",
            visibility_ts_ns=visibility_ts_ns,
            reason="prospective_placement_state_supported",
        )
        self.reentry_eligible_ts_ns = int(visibility_ts_ns)

    def exposure_btc_s(self, *, now_ns: int | None = None) -> float:
        """Return the legacy strategy-visible BTC*s exposure estimand."""

        value = float(self.quantity_time_exposure_btc_s)
        if now_ns is not None and self.fill_risk_active:
            timestamp = int(now_ns)
            if timestamp < self._last_event_ts_ns:
                raise ValueError("order lifecycle snapshot time regressed")
            value += (
                self.remaining_quantity
                * (timestamp - self._last_event_ts_ns)
                / 1_000_000_000.0
            )
        return value

    def exchange_exposure_btc_s(
        self,
        *,
        now_exchange_ns: int | None = None,
    ) -> float | None:
        if not self.exchange_exposure_valid:
            return None
        if self.activation_exchange_ts_ns <= 0:
            return 0.0 if self.exchange_exposure_complete else None
        value = float(
            self.quantity_time_exposure_exchange_accumulated_btc_s
        )
        if now_exchange_ns is not None and self.fill_risk_active:
            timestamp = int(now_exchange_ns)
            if timestamp < self._last_exchange_event_ts_ns:
                raise ValueError("order lifecycle exchange snapshot time regressed")
            value += (
                self.remaining_quantity
                * (timestamp - self._last_exchange_event_ts_ns)
                / 1_000_000_000.0
            )
        return value

    def snapshot(self, *, now_ns: int | None = None) -> dict[str, object]:
        visible_exposure = self.exposure_btc_s(now_ns=now_ns)
        exchange_exposure = self.exchange_exposure_btc_s()
        return {
            "phase": self.phase.value,
            "fill_risk_active": self.fill_risk_active,
            "initial_quantity": float(self.initial_quantity),
            "remaining_quantity": float(self.remaining_quantity),
            "submitted_ts_ns": int(self.submitted_ts_ns),
            "activation_ts_ns": int(self.activation_ts_ns),
            "activation_visibility_ts_ns": int(self.activation_ts_ns),
            "activation_exchange_ts_ns": int(self.activation_exchange_ts_ns),
            "first_fill_ts_ns": int(self.first_fill_ts_ns),
            "first_fill_visibility_ts_ns": int(self.first_fill_ts_ns),
            "first_fill_exchange_ts_ns": int(self.first_fill_exchange_ts_ns),
            "first_fill_latency_s": self.first_fill_latency_s,
            "first_fill_latency_visible_s": self.first_fill_latency_s,
            "first_fill_latency_exchange_s": self.first_fill_latency_exchange_s,
            "cancel_request_ts_ns": int(self.cancel_request_ts_ns),
            "terminal_ts_ns": int(self.terminal_ts_ns),
            "terminal_visibility_ts_ns": int(self.terminal_ts_ns),
            "terminal_exchange_ts_ns": int(self.terminal_exchange_ts_ns),
            "terminal_reason": self.terminal_reason,
            "terminal_policy_route": self.terminal_policy_route_name,
            "post_cancel_recovery_ts_ns": int(
                self.post_cancel_recovery_ts_ns
            ),
            "reentry_eligible_ts_ns": int(self.reentry_eligible_ts_ns),
            "quantity_time_exposure_btc_s": visible_exposure,
            "quantity_time_exposure_visible_btc_s": visible_exposure,
            "quantity_time_exposure_exchange_btc_s": exchange_exposure,
            "quantity_time_exposure_visibility_minus_exchange_btc_s": (
                visible_exposure - exchange_exposure
                if exchange_exposure is not None
                else None
            ),
            "exchange_exposure_available": bool(
                self.exchange_exposure_valid
                and self.activation_exchange_ts_ns > 0
            ),
            "exchange_exposure_valid": bool(self.exchange_exposure_valid),
            "exchange_exposure_complete": bool(self.exchange_exposure_complete),
            "exchange_exposure_invalid_reason": self.exchange_exposure_invalid_reason,
        }

    def events(self) -> tuple[dict[str, object], ...]:
        return tuple(asdict(event) for event in self._events)

    def journal_snapshot(self) -> QuantityWeightedOrderLifecycle:
        """Return an immutable-in-practice copy for the async journal worker."""

        snapshot = copy(self)
        snapshot._events = list(self._events)
        return snapshot

    def latest_event(self) -> OrderLifecycleEvent:
        """Return the latest frozen lifecycle event without serializing history."""

        if not self._events:
            raise ValueError("order lifecycle has no events")
        return self._events[-1]
