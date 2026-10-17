"""Fail-closed planned-maintenance and restart boundary state machine."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .replay_state_checkpoint import ContinuousReplayState

SCHEMA_VERSION = "restart_boundary_contract.v1"


class RestartPhase(str, Enum):
    ACTIVE = "ACTIVE"
    CANCEL_DRAIN = "CANCEL_DRAIN"
    OFFLINE = "OFFLINE"
    WARMING = "WARMING"
    READY = "READY"


class OrderPhase(str, Enum):
    ACTIVE = "ACTIVE"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"


TERMINAL_REASONS = frozenset({"CANCEL_ACK", "FULL_FILL", "EXPIRED", "REJECTED"})


@dataclass(frozen=True)
class PlannedRestartInterval:
    gap_id: str
    quote_stop_ts_ms: int
    cancel_deadline_ts_ms: int
    offline_start_ts_ms: int
    resume_snapshot_ts_ms: int

    @classmethod
    def from_manifest_row(cls, row: Mapping[str, Any]) -> PlannedRestartInterval:
        interval = cls(
            gap_id=str(row["gap_id"]),
            quote_stop_ts_ms=int(row["quote_stop_ts_ms"]),
            cancel_deadline_ts_ms=int(row["cancel_deadline_ts_ms"]),
            offline_start_ts_ms=int(row["offline_start_ts_ms"]),
            resume_snapshot_ts_ms=int(row["resume_snapshot_ts_ms"]),
        )
        interval.validate()
        return interval

    def validate(self) -> None:
        if not self.gap_id.strip():
            raise ValueError("planned restart gap_id must be non-empty")
        if not (
            self.quote_stop_ts_ms
            <= self.cancel_deadline_ts_ms
            < self.offline_start_ts_ms
            < self.resume_snapshot_ts_ms
        ):
            raise ValueError("planned restart timestamps are not ordered")


@dataclass
class TrackedOrder:
    client_order_id: str
    remaining_quantity_btc: float
    phase: OrderPhase = OrderPhase.ACTIVE
    cancel_request_count: int = 0
    partially_filled: bool = False


@dataclass(frozen=True)
class BoundaryJournalRow:
    ts_ms: int
    event: str
    gap_id: str
    client_order_id: str = ""
    detail: str = ""


class RestartBoundaryMachine:
    """Require observable order terminality before entering a data gap."""

    def __init__(self) -> None:
        self.phase = RestartPhase.ACTIVE
        self.interval: PlannedRestartInterval | None = None
        self.orders: dict[str, TrackedOrder] = {}
        self.journal: list[BoundaryJournalRow] = []
        self.snapshot_identity = ""
        self.feature_ready_ts_ms: int | None = None

    @property
    def new_quotes_allowed(self) -> bool:
        return self.phase in {RestartPhase.ACTIVE, RestartPhase.READY}

    def register_active_order(
        self,
        *,
        client_order_id: str,
        remaining_quantity_btc: float,
    ) -> None:
        if not self.new_quotes_allowed:
            raise RuntimeError("new order registered while restart boundary blocks quoting")
        order_id = str(client_order_id).strip()
        if not order_id or order_id in self.orders:
            raise ValueError("active order id must be unique and non-empty")
        if float(remaining_quantity_btc) <= 0:
            raise ValueError("active order remaining quantity must be positive")
        self.orders[order_id] = TrackedOrder(
            client_order_id=order_id,
            remaining_quantity_btc=float(remaining_quantity_btc),
        )

    def begin_maintenance(
        self,
        interval: PlannedRestartInterval,
        *,
        now_ts_ms: int,
    ) -> None:
        interval.validate()
        if self.phase not in {RestartPhase.ACTIVE, RestartPhase.READY}:
            raise RuntimeError("restart maintenance began from an invalid phase")
        if int(now_ts_ms) != interval.quote_stop_ts_ms:
            raise ValueError("maintenance must start at the frozen quote-stop timestamp")
        self.phase = RestartPhase.CANCEL_DRAIN
        self.interval = interval
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(now_ts_ms),
                event="QUOTE_STOP",
                gap_id=interval.gap_id,
                detail=f"active_orders={len(self.orders)}",
            )
        )

    def request_cancel(self, client_order_id: str, *, ts_ms: int) -> None:
        if self.phase != RestartPhase.CANCEL_DRAIN or self.interval is None:
            raise RuntimeError("cancel request outside the maintenance drain phase")
        order = self.orders.get(str(client_order_id))
        if order is None:
            raise KeyError(client_order_id)
        if int(ts_ms) > self.interval.cancel_deadline_ts_ms:
            raise RuntimeError("cancel request arrived after the frozen deadline")
        order.phase = OrderPhase.CANCEL_PENDING
        order.cancel_request_count += 1
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(ts_ms),
                event="CANCEL_REQUEST",
                gap_id=self.interval.gap_id,
                client_order_id=order.client_order_id,
            )
        )

    def partial_fill(
        self,
        client_order_id: str,
        *,
        ts_ms: int,
        filled_quantity_btc: float,
    ) -> None:
        order = self.orders.get(str(client_order_id))
        if order is None:
            raise KeyError(client_order_id)
        quantity = float(filled_quantity_btc)
        if quantity <= 0 or quantity >= order.remaining_quantity_btc:
            raise ValueError("partial fill must leave positive remaining quantity")
        order.remaining_quantity_btc -= quantity
        order.partially_filled = True
        if order.phase != OrderPhase.CANCEL_PENDING:
            order.phase = OrderPhase.PARTIALLY_FILLED
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(ts_ms),
                event="PARTIAL_FILL",
                gap_id=self.interval.gap_id if self.interval else "",
                client_order_id=order.client_order_id,
                detail=f"remaining={order.remaining_quantity_btc:.12g}",
            )
        )

    def cancel_reject(self, client_order_id: str, *, ts_ms: int) -> None:
        if self.phase != RestartPhase.CANCEL_DRAIN or self.interval is None:
            raise RuntimeError("cancel reject outside the maintenance drain phase")
        order = self.orders.get(str(client_order_id))
        if order is None or order.phase != OrderPhase.CANCEL_PENDING:
            raise RuntimeError("cancel reject does not match a pending cancel")
        order.phase = (
            OrderPhase.PARTIALLY_FILLED
            if order.partially_filled
            else OrderPhase.ACTIVE
        )
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(ts_ms),
                event="CANCEL_REJECT",
                gap_id=self.interval.gap_id,
                client_order_id=order.client_order_id,
            )
        )

    def terminal(
        self,
        client_order_id: str,
        *,
        ts_ms: int,
        reason: str,
    ) -> None:
        order_id = str(client_order_id)
        if order_id not in self.orders:
            raise KeyError(order_id)
        reason = str(reason).upper()
        if reason not in TERMINAL_REASONS:
            raise ValueError(f"unsupported terminal reason: {reason}")
        if reason == "CANCEL_ACK" and self.orders[order_id].phase != OrderPhase.CANCEL_PENDING:
            raise RuntimeError("cancel ACK does not match a pending cancel")
        del self.orders[order_id]
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(ts_ms),
                event=reason,
                gap_id=self.interval.gap_id if self.interval else "",
                client_order_id=order_id,
            )
        )

    def enter_offline(
        self,
        *,
        ts_ms: int,
        state: ContinuousReplayState,
    ) -> ContinuousReplayState:
        if self.phase != RestartPhase.CANCEL_DRAIN or self.interval is None:
            raise RuntimeError("offline transition outside the cancel drain phase")
        if int(ts_ms) != self.interval.offline_start_ts_ms:
            raise ValueError("offline transition must use the frozen timestamp")
        if self.orders:
            pending = ",".join(sorted(self.orders))
            raise RuntimeError(f"orders failed to terminate before data gap: {pending}")
        clean = state.for_planned_restart(int(ts_ms))
        self.phase = RestartPhase.OFFLINE
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(ts_ms),
                event="OFFLINE",
                gap_id=self.interval.gap_id,
            )
        )
        return clean

    def begin_restart(
        self,
        *,
        ts_ms: int,
        snapshot_identity: str,
        state: ContinuousReplayState,
    ) -> ContinuousReplayState:
        if self.phase != RestartPhase.OFFLINE or self.interval is None:
            raise RuntimeError("restart began outside the offline phase")
        if int(ts_ms) != self.interval.resume_snapshot_ts_ms:
            raise ValueError("restart must begin at the frozen snapshot timestamp")
        if not str(snapshot_identity).strip():
            raise ValueError("restart requires a non-empty snapshot identity")
        state.validate(require_restart_safe=True)
        self.snapshot_identity = str(snapshot_identity)
        self.feature_ready_ts_ms = None
        self.phase = RestartPhase.WARMING
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(ts_ms),
                event="RESTART_SNAPSHOT",
                gap_id=self.interval.gap_id,
                detail=self.snapshot_identity,
            )
        )
        return state.with_mark(int(ts_ms), state.last_mark_price)

    def complete_warmup(
        self,
        *,
        feature_ready_ts_ms: int,
        decision_ts_ms: int,
        state: ContinuousReplayState,
    ) -> ContinuousReplayState:
        if self.phase != RestartPhase.WARMING or self.interval is None:
            raise RuntimeError("warmup completed outside the warming phase")
        if int(feature_ready_ts_ms) > int(decision_ts_ms):
            raise RuntimeError("feature-ready time is later than the quote decision")
        if int(feature_ready_ts_ms) < self.interval.resume_snapshot_ts_ms:
            raise RuntimeError("warmup cannot be ready before the restart snapshot")
        state.validate(require_restart_safe=True)
        self.feature_ready_ts_ms = int(feature_ready_ts_ms)
        self.phase = RestartPhase.READY
        ready = ContinuousReplayState.from_dict(
            {
                **state.to_dict(),
                "checkpoint_ts_ms": int(decision_ts_ms),
                "feature_warmup_ready": True,
                "quoting_enabled": True,
            }
        )
        self.journal.append(
            BoundaryJournalRow(
                ts_ms=int(decision_ts_ms),
                event="REENTRY_ELIGIBLE",
                gap_id=self.interval.gap_id,
                detail=f"feature_ready_ts_ms={int(feature_ready_ts_ms)}",
            )
        )
        return ready
