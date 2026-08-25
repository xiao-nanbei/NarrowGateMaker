"""Complete replay order lifecycle and delayed-entry campaign risk sets."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

import pandas as pd

from execution.order_lifecycle_quantity_contract import (
    canonicalize_remaining_quantity,
)

SCHEMA_VERSION = "local_order_lifecycle.v2"
INTERVAL_SCHEMA_VERSION = "local_order_risk_interval.v2"

DEFAULT_RISK_SNAPSHOT_EDGES_MS = (
    0,
    100,
    200,
    300,
    500,
    750,
    1_000,
    1_500,
    2_500,
    4_000,
    6_000,
    10_000,
    20_000,
    40_000,
    85_000,
)

ACTIVE_STATES = {"open", "pending_cancel"}
TERMINAL_STATES = {"filled", "cancelled", "rejected", "censored"}
EVENT_PROFILES = {"full", "placement_start_stop"}


def _ns(ts_ms: int) -> int:
    return int(ts_ms) * 1_000_000


class OrderLifecycleRecorder:
    """Record exchange/order transitions without making jump or repair absorbing."""

    def __init__(
        self,
        *,
        symbol: str,
        lot_size: float,
        tick_size: float,
        price_jump_ticks: float,
        max_orders: int,
        risk_snapshot_edges_ms: Iterable[int] = DEFAULT_RISK_SNAPSHOT_EDGES_MS,
        event_profile: str = "full",
    ) -> None:
        self.symbol = str(symbol)
        self.lot_size = float(lot_size)
        self.tick_size = float(tick_size)
        self.price_jump_ticks = max(0.0, float(price_jump_ticks))
        self.max_orders = max(0, int(max_orders))
        self.event_profile = str(event_profile)
        if self.event_profile not in EVENT_PROFILES:
            raise ValueError(
                "event_profile must be one of "
                f"{sorted(EVENT_PROFILES)}, got {self.event_profile!r}"
            )
        edges = tuple(sorted({int(edge) for edge in risk_snapshot_edges_ms}))
        if not edges or edges[0] != 0 or any(edge < 0 for edge in edges):
            raise ValueError("risk snapshot edges must be unique, non-negative, and start at 0")
        self.risk_snapshot_edges_ms = edges
        self._states: dict[int, dict[str, Any]] = {}
        self._active_order_ids: set[int] = set()
        self._campaign_order_ids: dict[int, set[int]] = {}
        self._campaign_membership_versions: dict[int, int] = {}
        self._campaign_repair_snapshots: dict[
            int, tuple[int, bool, float, bool]
        ] = {}
        self._latest_same_time_events: dict[
            int, tuple[int, set[str], list[dict[str, Any]]]
        ] = {}
        self._events: list[dict[str, Any]] = []
        self._next_event_seq = 1

    @property
    def enabled(self) -> bool:
        return self.max_orders > 0

    @property
    def records_dynamic_state(self) -> bool:
        """Whether the trace includes jump, risk, and campaign transitions."""

        return self.enabled and self.event_profile == "full"

    def _state(self, order: dict[str, Any] | int) -> dict[str, Any] | None:
        order_id = (
            int(order)
            if isinstance(order, int)
            else int(order.get("trace_id", -1))
        )
        return self._states.get(order_id)

    def _set_campaign_id(self, state: dict[str, Any], campaign_id: int) -> None:
        campaign_id = int(campaign_id)
        old_id = int(state.get("campaign_id", 0) or 0)
        order_id = int(state["order_id"])
        if old_id == campaign_id:
            return
        if old_id > 0:
            members = self._campaign_order_ids.get(old_id)
            if members is not None:
                members.discard(order_id)
                self._campaign_membership_versions[old_id] = (
                    self._campaign_membership_versions.get(old_id, 0) + 1
                )
                if not members:
                    self._campaign_order_ids.pop(old_id, None)
        state["campaign_id"] = campaign_id
        if campaign_id > 0:
            self._campaign_order_ids.setdefault(campaign_id, set()).add(
                order_id
            )
            self._campaign_membership_versions[campaign_id] = (
                self._campaign_membership_versions.get(campaign_id, 0) + 1
            )

    def _emit(
        self,
        state: dict[str, Any],
        event_type: str,
        ts_ns: int,
        *,
        state_before: str | None = None,
        reason: str = "",
        source: str = "replay_state_machine",
        same_ms_ordering_resolved: int = 1,
        **updates: Any,
    ) -> None:
        sequence = self._next_event_seq
        self._next_event_seq += 1
        before = str(state_before if state_before is not None else state["state"])
        state.update(updates)
        row = {
            "schema_version": SCHEMA_VERSION,
            "event_profile": self.event_profile,
            "decision_id": state["decision_id"],
            "day": pd.to_datetime(int(ts_ns), unit="ns", utc=True).strftime(
                "%Y-%m-%d"
            ),
            "symbol": self.symbol,
            "order_id": int(state["order_id"]),
            "campaign_id": int(state.get("campaign_id", 0) or 0),
            "side": str(state["side"]),
            "inventory_role": str(state.get("inventory_role", "unknown")),
            "event_type": str(event_type),
            "event_ts_ns": int(ts_ns),
            "event_seq": int(sequence),
            "event_source": str(source),
            "event_reason": str(reason),
            "state_before": before,
            "state_after": str(state["state"]),
            "decision_ts_ns": int(state["submit_ts_ns"]),
            "feature_ready_ts_ns": int(
                updates.get("feature_ready_ts_ns", state["submit_ts_ns"])
            ),
            "feature_source_ts_ns": int(
                updates.get("feature_source_ts_ns", state["submit_ts_ns"])
            ),
            "order_submit_ts_ns": int(state["submit_ts_ns"]),
            "order_activation_ts_ns": int(state.get("activation_ts_ns", 0) or 0),
            "order_terminal_ts_ns": int(state.get("terminal_ts_ns", 0) or 0),
            "order_price": float(state["order_price"]),
            "order_qty": float(state["order_qty"]),
            "reduce_only": int(bool(state.get("reduce_only", False))),
            "circuit_breaker_close": int(
                bool(state.get("circuit_breaker_close", False))
            ),
            "remaining_qty": float(state["remaining_qty"]),
            "cancel_request_ts_ns": int(
                state.get("cancel_request_ts_ns", 0) or 0
            ),
            "cancel_request_event_seq": int(
                state.get("cancel_request_event_seq", 0) or 0
            ),
            "cancel_ack_ts_ns": int(state.get("cancel_ack_ts_ns", 0) or 0),
            "cancel_ack_event_seq": int(
                state.get("cancel_ack_event_seq", 0) or 0
            ),
            "remaining_qty_at_cancel_request": float(
                state.get("remaining_qty_at_cancel_request", math.nan)
            ),
            "remaining_qty_at_cancel_ack": float(
                state.get("remaining_qty_at_cancel_ack", math.nan)
            ),
            "fill_while_cancel_pending_qty": float(
                state.get("fill_while_cancel_pending_qty", 0.0)
            ),
            "future_mid_first_hit_ts_ns": int(
                state.get("future_mid_first_hit_ts_ns", 0) or 0
            ),
            "future_mid_first_hit_direction": str(
                state.get("future_mid_first_hit_direction", "")
            ),
            "future_mid_first_hit_source": str(
                state.get("future_mid_first_hit_source", "")
            ),
            "future_mid_first_hit_event_seq": int(
                state.get("future_mid_first_hit_event_seq", 0) or 0
            ),
            "same_ms_ordering_resolved": int(bool(same_ms_ordering_resolved)),
            "repair_risk_entry_ts_ns": int(
                state.get("repair_risk_entry_ts_ns", 0) or 0
            ),
            "repair_risk_exit_ts_ns": int(
                state.get("repair_risk_exit_ts_ns", 0) or 0
            ),
            "repair_at_risk": int(bool(state.get("repair_at_risk", False))),
            "campaign_active": int(bool(state.get("campaign_active", False))),
            "reducing_quote_active": int(
                bool(state.get("reducing_quote_active", False))
            ),
            "reducing_quote_eligible": int(
                bool(state.get("reducing_quote_eligible", False))
            ),
            "inventory": float(state.get("inventory", 0.0)),
            "repair_ts_ns": int(state.get("repair_ts_ns", 0) or 0),
            "campaign_repair_event_seq": int(
                state.get("campaign_repair_event_seq", 0) or 0
            ),
            "risk_snapshot_edge_ms": int(
                updates.get("risk_snapshot_edge_ms", -1) or 0
            ),
            "risk_snapshot_elapsed_ms": float(
                updates.get("risk_snapshot_elapsed_ms", math.nan)
            ),
            "risk_snapshot_missed_edges": int(
                updates.get("risk_snapshot_missed_edges", 0) or 0
            ),
        }
        for name, value in updates.items():
            if name not in row:
                row[name] = value
        row["same_ms_cross_stream_ambiguity"] = 0
        order_id = int(state["order_id"])
        same_time = self._latest_same_time_events.get(order_id)
        if same_time is None or int(same_time[0]) != int(ts_ns):
            same_time = (int(ts_ns), set(), [])
            self._latest_same_time_events[order_id] = same_time
        event_types = same_time[1]
        event_rows = same_time[2]
        event_types.add(str(event_type))
        event_rows.append(row)
        cross_stream = (
            "native_price_jump" in event_types
            and bool(
                event_types
                & {"activate", "partial_fill", "full_fill", "cancel_ack"}
            )
        ) or (
            "risk_snapshot" in event_types
            and bool(
                event_types
                & {
                    "partial_fill",
                    "full_fill",
                    "cancel_request",
                    "cancel_ack",
                }
            )
        )
        if cross_stream:
            for event_row in event_rows:
                event_row["same_ms_ordering_resolved"] = 0
                event_row["same_ms_cross_stream_ambiguity"] = 1
        self._events.append(row)

    def submit(self, order: dict[str, Any], ts_ms: int) -> None:
        if not self.enabled or len(self._states) >= self.max_orders:
            return
        order_id = int(order.get("trace_id", -1))
        if order_id < 0 or order_id in self._states:
            return
        quantity = max(0.0, float(order.get("quantity", 0.0) or 0.0))
        remaining = max(0.0, float(order.get("remaining", quantity) or 0.0))
        state = {
            "decision_id": f"order_lifecycle:{self.symbol}:{order_id}:{int(ts_ms)}",
            "order_id": order_id,
            "side": str(order.get("side", "")),
            "inventory_role": str(
                order.get("inventory_role_at_submit", "unknown")
            ),
            "campaign_id": int(order.get("campaign_id_at_submit", 0) or 0),
            "submit_ts_ns": _ns(ts_ms),
            "activation_ts_ns": 0,
            "terminal_ts_ns": 0,
            "state": "pending_new",
            "order_price": float(order.get("price", 0.0) or 0.0),
            "order_qty": quantity,
            "reduce_only": bool(order.get("reduce_only", False)),
            "circuit_breaker_close": bool(
                order.get("circuit_breaker_close", False)
            ),
            "remaining_qty": remaining,
            "activation_mid": math.nan,
            "cancel_request_ts_ns": 0,
            "cancel_request_event_seq": 0,
            "cancel_ack_ts_ns": 0,
            "cancel_ack_event_seq": 0,
            "remaining_qty_at_cancel_request": math.nan,
            "remaining_qty_at_cancel_ack": math.nan,
            "fill_while_cancel_pending_qty": 0.0,
            "future_mid_first_hit_ts_ns": 0,
            "future_mid_first_hit_direction": "",
            "future_mid_first_hit_source": "",
            "future_mid_first_hit_event_seq": 0,
            "repair_risk_entry_ts_ns": 0,
            "repair_risk_exit_ts_ns": 0,
            "repair_at_risk": False,
            "campaign_active": False,
            "reducing_quote_active": False,
            "reducing_quote_eligible": False,
            "inventory": float(order.get("inventory_at_submit", 0.0) or 0.0),
            "repair_ts_ns": 0,
            "campaign_repair_event_seq": 0,
            "last_risk_snapshot_edge_index": -1,
            "risk_anchor_mid": math.nan,
            "risk_worst_adverse_ticks": 0.0,
            "risk_anchor_microprice": math.nan,
            "risk_worst_microprice_adverse_ticks": 0.0,
            "risk_anchor_top_size": math.nan,
            "last_fill_event_index": -1,
        }
        self._states[order_id] = state
        if int(state["campaign_id"]) > 0:
            campaign_id = int(state["campaign_id"])
            self._campaign_order_ids.setdefault(campaign_id, set()).add(
                order_id
            )
            self._campaign_membership_versions[campaign_id] = (
                self._campaign_membership_versions.get(campaign_id, 0) + 1
            )
        self._emit(state, "submit", _ns(ts_ms), state_before="not_submitted")

    def activate(self, order: dict[str, Any], ts_ms: int, *, mid: float) -> None:
        state = self._state(order)
        if state is None or state["state"] in TERMINAL_STATES:
            return
        before = str(state["state"])
        state["state"] = (
            "pending_cancel"
            if int(order.get("cancel_ts", -1) or -1) > int(ts_ms)
            else "open"
        )
        self._active_order_ids.add(int(state["order_id"]))
        self._emit(
            state,
            "activate",
            _ns(ts_ms),
            state_before=before,
            activation_ts_ns=_ns(ts_ms),
            activation_mid=float(mid),
            remaining_qty=float(order.get("remaining", state["remaining_qty"])),
        )

    def reject(self, order: dict[str, Any], ts_ms: int, *, reason: str) -> None:
        state = self._state(order)
        if state is None or state["state"] in TERMINAL_STATES:
            return
        before = str(state["state"])
        state["state"] = "rejected"
        self._active_order_ids.discard(int(state["order_id"]))
        self._emit(
            state,
            "reject",
            _ns(ts_ms),
            state_before=before,
            reason=reason,
            terminal_ts_ns=_ns(ts_ms),
        )

    def request_cancel(
        self,
        order: dict[str, Any],
        ts_ms: int,
        *,
        reason: str,
    ) -> None:
        state = self._state(order)
        if state is None or state["state"] in TERMINAL_STATES:
            return
        if int(state.get("cancel_request_ts_ns", 0) or 0) > 0:
            return
        before = str(state["state"])
        if state["state"] == "open":
            state["state"] = "pending_cancel"
        sequence = self._next_event_seq
        self._emit(
            state,
            "cancel_request",
            _ns(ts_ms),
            state_before=before,
            reason=reason,
            cancel_request_ts_ns=_ns(ts_ms),
            cancel_request_event_seq=sequence,
            remaining_qty_at_cancel_request=float(state["remaining_qty"]),
        )

    def fill(
        self,
        order: dict[str, Any],
        ts_ms: int,
        *,
        fill_qty: float,
        remaining_before: float,
        remaining_after: float,
        fill_price: float,
        inventory_before: float,
        inventory_after: float,
        campaign_id: int,
        physical_fill_identity: str = "",
        economic_legs: Iterable[dict[str, Any]] = (),
    ) -> None:
        state = self._state(order)
        if state is None or state["state"] in TERMINAL_STATES:
            return
        fill_qty = max(0.0, float(fill_qty))
        if fill_qty <= 0.0:
            return
        before = str(state["state"])
        canonical_remaining = canonicalize_remaining_quantity(remaining_after)
        partial = canonical_remaining > 0.0
        pending = before == "pending_cancel"
        normalized_legs = [dict(leg) for leg in economic_legs]
        if normalized_legs:
            identities = {
                str(leg.get("physical_fill_identity", ""))
                for leg in normalized_legs
            }
            leg_qty = sum(
                float(leg.get("quantity_btc", 0.0))
                for leg in normalized_legs
            )
            if (
                len(identities) != 1
                or identities != {str(physical_fill_identity)}
                or not math.isclose(
                    leg_qty,
                    fill_qty,
                    rel_tol=0.0,
                    abs_tol=1e-10,
                )
            ):
                raise ValueError(
                    "order lifecycle economic legs do not conserve the physical fill"
                )
        if not partial:
            state["state"] = "filled"
            self._active_order_ids.discard(int(state["order_id"]))
        self._set_campaign_id(state, int(campaign_id))
        sequence = self._next_event_seq
        self._emit(
            state,
            "partial_fill" if partial else "full_fill",
            _ns(ts_ms),
            state_before=before,
            fill_ts_ns=_ns(ts_ms),
            fill_event_seq=sequence,
            fill_qty=fill_qty,
            fill_price=float(fill_price),
            fill_is_partial=int(partial),
            remaining_qty_start=float(remaining_before),
            remaining_qty_end=canonical_remaining,
            remaining_qty_after_fill=canonical_remaining,
            remaining_qty=canonical_remaining,
            fill_while_cancel_pending_qty=float(
                state.get("fill_while_cancel_pending_qty", 0.0)
                + (fill_qty if pending else 0.0)
            ),
            terminal_ts_ns=(0 if partial else _ns(ts_ms)),
            inventory=float(inventory_after),
            inventory_before_fill=float(inventory_before),
            campaign_id=int(campaign_id),
            physical_fill_identity=str(physical_fill_identity),
            economic_leg_count=len(normalized_legs),
            economic_legs=normalized_legs,
        )
        state["last_fill_event_index"] = len(self._events) - 1

    def annotate_fill_value(
        self,
        order: dict[str, Any] | int,
        *,
        markout_bps: float,
        horizon_ms: int,
        horizon_censored: bool,
        observation_ts_ns: int,
        observation_mid: float,
        target_ts_ns: int = 0,
        observation_source: str = "",
        observation_age_ms: float = 0.0,
    ) -> None:
        """Attach the post-fill value label to the exact lifecycle fill event."""

        state = self._state(order)
        if state is None:
            return
        index = int(state.get("last_fill_event_index", -1) or -1)
        if index < 0 or index >= len(self._events):
            return
        row = self._events[index]
        if str(row.get("event_type", "")) not in {"partial_fill", "full_fill"}:
            return
        row.update(
            {
                "fill_value_markout_bps": float(markout_bps),
                "fill_value_horizon_ms": int(horizon_ms),
                "fill_value_horizon_censored": int(bool(horizon_censored)),
                "fill_value_observation_ts_ns": int(observation_ts_ns),
                "fill_value_observation_mid": float(observation_mid),
                "fill_value_target_ts_ns": int(target_ts_ns),
                "fill_value_observation_source": str(observation_source),
                "fill_value_observation_age_ms": float(observation_age_ms),
            }
        )

    def risk_snapshot(
        self,
        order: dict[str, Any],
        ts_ms: int,
        *,
        feature_source_ts_ns: int,
        feature_ready_ts_ns: int,
        inventory_role: str,
        inventory: float,
        campaign_id: int,
        mid: float,
        microprice: float,
        top_size: float,
        features: dict[str, Any],
    ) -> None:
        """Emit one causal state row at each pre-registered elapsed boundary.

        A skipped clock boundary is never backfilled with a later state.  The
        newest crossed edge is emitted once and the number of skipped edges is
        retained as an observability diagnostic.
        """

        if not self.records_dynamic_state:
            return
        state = self._state(order)
        if state is None or state["state"] != "open":
            return
        if int(state.get("cancel_request_ts_ns", 0) or 0) > 0:
            return
        activation_ts_ns = int(state.get("activation_ts_ns", 0) or 0)
        if activation_ts_ns <= 0:
            return
        event_ts_ns = _ns(ts_ms)
        if event_ts_ns < activation_ts_ns:
            return
        if int(feature_source_ts_ns) > int(feature_ready_ts_ns):
            raise ValueError("risk snapshot feature source is later than feature readiness")
        if int(feature_ready_ts_ns) > event_ts_ns:
            raise ValueError("risk snapshot contains future-ready features")

        elapsed_ms = max(0.0, (event_ts_ns - activation_ts_ns) / 1_000_000.0)
        crossed = [
            index
            for index, edge in enumerate(self.risk_snapshot_edges_ms)
            if float(edge) <= elapsed_ms
        ]
        if not crossed:
            return
        edge_index = crossed[-1]
        previous_index = int(state.get("last_risk_snapshot_edge_index", -1))
        if edge_index <= previous_index:
            return

        side = str(state["side"])
        anchor_mid = float(state.get("risk_anchor_mid", math.nan))
        if not math.isfinite(anchor_mid) or anchor_mid <= 0.0:
            anchor_mid = float(mid)
            state["risk_anchor_mid"] = anchor_mid
        signed_mid_move_ticks = (
            (anchor_mid - float(mid)) / self.tick_size
            if side == "BUY"
            else (float(mid) - anchor_mid) / self.tick_size
        )
        adverse_ticks = max(0.0, signed_mid_move_ticks)
        worst_adverse = max(
            float(state.get("risk_worst_adverse_ticks", 0.0) or 0.0),
            adverse_ticks,
        )
        state["risk_worst_adverse_ticks"] = worst_adverse
        price_recovery = (
            min(1.0, max(0.0, 1.0 - adverse_ticks / worst_adverse))
            if worst_adverse > 1e-12
            else 1.0
        )

        anchor_microprice = float(
            state.get("risk_anchor_microprice", math.nan)
        )
        if not math.isfinite(anchor_microprice) or anchor_microprice <= 0.0:
            anchor_microprice = float(microprice)
            state["risk_anchor_microprice"] = anchor_microprice
        signed_microprice_move_ticks = (
            (anchor_microprice - float(microprice)) / self.tick_size
            if side == "BUY"
            else (float(microprice) - anchor_microprice) / self.tick_size
        )
        microprice_adverse_ticks = max(0.0, signed_microprice_move_ticks)
        worst_microprice_adverse = max(
            float(
                state.get(
                    "risk_worst_microprice_adverse_ticks",
                    0.0,
                )
                or 0.0
            ),
            microprice_adverse_ticks,
        )
        state["risk_worst_microprice_adverse_ticks"] = worst_microprice_adverse
        microprice_recovery = (
            min(
                1.0,
                max(
                    0.0,
                    1.0
                    - microprice_adverse_ticks / worst_microprice_adverse,
                ),
            )
            if worst_microprice_adverse > 1e-12
            else 1.0
        )

        anchor_top_size = float(state.get("risk_anchor_top_size", math.nan))
        if not math.isfinite(anchor_top_size) or anchor_top_size <= 0.0:
            anchor_top_size = max(0.0, float(top_size))
            state["risk_anchor_top_size"] = anchor_top_size
        depth_recovery = (
            min(2.0, max(0.0, float(top_size)) / anchor_top_size)
            if anchor_top_size > 1e-12
            else 0.0
        )

        previous_snapshot_index = int(
            state.get("last_risk_snapshot_edge_index", -1)
        )
        state["last_risk_snapshot_edge_index"] = edge_index
        state["inventory_role"] = str(inventory_role)
        state["inventory"] = float(inventory)
        if int(campaign_id) > 0:
            state["campaign_id"] = int(campaign_id)
        updates = dict(features)
        jump_ts_ns = int(state.get("future_mid_first_hit_ts_ns", 0) or 0)
        updates.update(
            {
                "feature_source_ts_ns": int(feature_source_ts_ns),
                "feature_ready_ts_ns": int(feature_ready_ts_ns),
                "risk_snapshot_edge_ms": int(
                    self.risk_snapshot_edges_ms[edge_index]
                ),
                "risk_snapshot_elapsed_ms": float(elapsed_ms),
                "risk_snapshot_missed_edges": max(
                    0,
                    edge_index - previous_snapshot_index - 1,
                ),
                "current_inventory_role": str(inventory_role),
                "inventory": float(inventory),
                "campaign_id": int(campaign_id),
                "visible_mid": float(mid),
                "visible_microprice": float(microprice),
                "visible_same_side_top_size": max(0.0, float(top_size)),
                "price_adverse_ticks": float(adverse_ticks),
                "price_worst_adverse_ticks": float(worst_adverse),
                "price_recovery_ratio": float(price_recovery),
                "microprice_adverse_ticks": float(microprice_adverse_ticks),
                "microprice_worst_adverse_ticks": float(
                    worst_microprice_adverse
                ),
                "microprice_recovery_ratio": float(microprice_recovery),
                "visible_depth_recovery_ratio": float(depth_recovery),
                "native_adverse_jump_seen": int(jump_ts_ns > 0),
                "time_since_native_adverse_jump_ms": (
                    max(0.0, (event_ts_ns - jump_ts_ns) / 1_000_000.0)
                    if jump_ts_ns > 0
                    else -1.0
                ),
            }
        )
        self._emit(
            state,
            "risk_snapshot",
            event_ts_ns,
            source="causal_receive_time_state",
            same_ms_ordering_resolved=int(
                not bool(order.get("exchange_book_queue_ambiguous", False))
            ),
            **updates,
        )

    def cancel_ack(
        self,
        order: dict[str, Any],
        ts_ms: int,
        *,
        reason: str,
    ) -> None:
        state = self._state(order)
        if state is None or state["state"] in TERMINAL_STATES:
            return
        before = str(state["state"])
        state["state"] = "cancelled"
        self._active_order_ids.discard(int(state["order_id"]))
        sequence = self._next_event_seq
        self._emit(
            state,
            "cancel_ack",
            _ns(ts_ms),
            state_before=before,
            reason=reason,
            cancel_ack_ts_ns=_ns(ts_ms),
            cancel_ack_event_seq=sequence,
            remaining_qty_at_cancel_ack=float(state["remaining_qty"]),
            terminal_ts_ns=_ns(ts_ms),
        )

    def bind_campaign(self, order: dict[str, Any], campaign_id: int) -> None:
        state = self._state(order)
        if state is not None and int(campaign_id) > 0:
            self._set_campaign_id(state, int(campaign_id))

    def native_mid(
        self,
        ts_ns: int,
        mid: float,
        *,
        segment_id: int,
        same_ms_ordering_resolved: bool,
    ) -> None:
        if (
            not self.records_dynamic_state
            or self.price_jump_ticks <= 0.0
            or mid <= 0.0
        ):
            return
        threshold = self.price_jump_ticks * self.tick_size
        for order_id in tuple(self._active_order_ids):
            state = self._states.get(order_id)
            if state is None:
                continue
            if state["state"] not in ACTIVE_STATES:
                self._active_order_ids.discard(order_id)
                continue
            if int(state.get("future_mid_first_hit_ts_ns", 0) or 0) > 0:
                continue
            anchor = float(state.get("activation_mid", math.nan))
            if not math.isfinite(anchor) or anchor <= 0.0:
                continue
            side = str(state["side"])
            adverse = mid <= anchor - threshold if side == "BUY" else mid >= anchor + threshold
            if not adverse:
                continue
            sequence = self._next_event_seq
            self._emit(
                state,
                "native_price_jump",
                int(ts_ns),
                source="native_exchange_book",
                same_ms_ordering_resolved=int(bool(same_ms_ordering_resolved)),
                future_mid_first_hit_ts_ns=int(ts_ns),
                future_mid_first_hit_direction=("down" if side == "BUY" else "up"),
                future_mid_first_hit_source="native_exchange_book_mid",
                future_mid_first_hit_event_seq=sequence,
                native_mid=float(mid),
                native_segment_id=int(segment_id),
            )

    def sync_repair_state(
        self,
        ts_ms: int,
        *,
        campaign_id: int,
        campaign_active: bool,
        inventory: float,
        active_orders: Iterable[dict[str, Any]],
    ) -> None:
        if not self.records_dynamic_state:
            return
        reducing_side = "SELL" if inventory > 1e-12 else "BUY" if inventory < -1e-12 else ""
        reducing: list[dict[str, Any]] = []
        for order in active_orders:
            lifecycle_state = self._state(order)
            if lifecycle_state is None:
                continue
            if str(lifecycle_state.get("state", "")) not in ACTIVE_STATES:
                continue
            if str(order.get("side", "")) != reducing_side:
                continue
            if (
                float(order.get("remaining", 0.0) or 0.0)
                < self.lot_size - 1e-12
            ):
                continue
            if not bool(order.get("fill_eligible", True)):
                continue
            reducing.append(order)
        reducing_active = bool(reducing)
        desired = bool(campaign_active and abs(inventory) > 1e-12 and reducing_active)
        campaign_id = int(campaign_id)
        snapshot = (
            int(self._campaign_membership_versions.get(campaign_id, 0)),
            bool(campaign_active),
            float(inventory),
            bool(reducing_active),
        )
        if self._campaign_repair_snapshots.get(campaign_id) == snapshot:
            return
        self._campaign_repair_snapshots[campaign_id] = snapshot
        for order_id in tuple(
            self._campaign_order_ids.get(campaign_id, set())
        ):
            state = self._states.get(order_id)
            if state is None:
                continue
            if int(state.get("repair_ts_ns", 0) or 0) > 0:
                continue
            previous = bool(state.get("repair_at_risk", False))
            state["campaign_active"] = bool(campaign_active)
            state["inventory"] = float(inventory)
            state["reducing_quote_active"] = reducing_active
            state["reducing_quote_eligible"] = reducing_active
            if desired == previous:
                continue
            if desired:
                self._emit(
                    state,
                    "repair_risk_enter",
                    _ns(ts_ms),
                    repair_at_risk=True,
                    repair_risk_entry_ts_ns=_ns(ts_ms),
                )
            else:
                self._emit(
                    state,
                    "repair_risk_exit",
                    _ns(ts_ms),
                    repair_at_risk=False,
                    repair_risk_exit_ts_ns=_ns(ts_ms),
                )

    def campaign_repair(self, campaign_id: int, ts_ms: int) -> None:
        if not self.records_dynamic_state:
            return
        campaign_id = int(campaign_id)
        self._campaign_repair_snapshots.pop(campaign_id, None)
        for order_id in tuple(
            self._campaign_order_ids.get(campaign_id, set())
        ):
            state = self._states.get(order_id)
            if state is None:
                continue
            if int(state.get("repair_ts_ns", 0) or 0) > 0:
                continue
            if bool(state.get("repair_at_risk", False)):
                self._emit(
                    state,
                    "repair_risk_exit",
                    _ns(ts_ms),
                    repair_at_risk=False,
                    repair_risk_exit_ts_ns=_ns(ts_ms),
                    campaign_active=False,
                    inventory=0.0,
                )
            sequence = self._next_event_seq
            self._emit(
                state,
                "campaign_repair",
                _ns(ts_ms),
                source="campaign_state_machine",
                repair_ts_ns=_ns(ts_ms),
                campaign_repair_event_seq=sequence,
                campaign_active=False,
                repair_at_risk=False,
                inventory=0.0,
                reducing_quote_active=False,
                reducing_quote_eligible=False,
            )

    def censor_all(self, ts_ms: int) -> None:
        for state in self._states.values():
            if state["state"] not in TERMINAL_STATES:
                before = str(state["state"])
                state["state"] = "censored"
                self._active_order_ids.discard(int(state["order_id"]))
                self._emit(
                    state,
                    "day_end_censor",
                    _ns(ts_ms),
                    state_before=before,
                    reason="end_of_window",
                    terminal_ts_ns=_ns(ts_ms),
                    repair_at_risk=False,
                    repair_risk_exit_ts_ns=(
                        _ns(ts_ms)
                        if bool(state.get("repair_at_risk", False))
                        else int(state.get("repair_risk_exit_ts_ns", 0) or 0)
                    ),
                )
            elif self.records_dynamic_state and (
                int(state.get("campaign_id", 0) or 0) > 0
                and int(state.get("repair_ts_ns", 0) or 0) <= 0
            ):
                self._emit(
                    state,
                    "campaign_end_censor",
                    _ns(ts_ms),
                    reason="end_of_window",
                    source="campaign_state_machine",
                    repair_at_risk=False,
                    repair_risk_exit_ts_ns=(
                        _ns(ts_ms)
                        if bool(state.get("repair_at_risk", False))
                        else int(state.get("repair_risk_exit_ts_ns", 0) or 0)
                    ),
                )

    def events(self, *, copy_rows: bool = True) -> list[dict[str, Any]]:
        if copy_rows:
            return [dict(row) for row in self._events]
        return self._events


def build_order_risk_intervals(events: pd.DataFrame) -> pd.DataFrame:
    """Convert transition rows into explicit start/stop risk intervals."""

    if events.empty:
        return pd.DataFrame()
    required = {"order_id", "event_ts_ns", "event_seq", "state_after", "remaining_qty"}
    missing = sorted(required - set(events.columns))
    if missing:
        raise ValueError(f"lifecycle events missing columns: {missing}")
    identity_columns = ["day", "order_id"] if "day" in events.columns else ["order_id"]
    ordered = events.sort_values(
        [*identity_columns, "event_ts_ns", "event_seq"], kind="mergesort"
    )
    grouped = ordered.groupby(identity_columns, sort=False)
    next_ts = grouped["event_ts_ns"].shift(-1, fill_value=-1)
    next_seq = grouped["event_seq"].shift(-1, fill_value=-1)
    next_type = grouped["event_type"].shift(-1, fill_value="")
    has_next = next_ts.ge(0)
    if not bool(has_next.any()):
        return pd.DataFrame()

    intervals = ordered.loc[has_next].copy()
    start_ns = pd.to_numeric(
        intervals["event_ts_ns"],
        errors="raise",
    ).astype("int64")
    end_ns = pd.to_numeric(
        next_ts.loc[has_next],
        errors="raise",
    ).astype("int64")
    if bool((end_ns < start_ns).any()):
        raise ValueError("lifecycle interval time regressed")

    state = intervals["state_after"].astype(str)
    remaining = pd.to_numeric(
        intervals["remaining_qty"],
        errors="coerce",
    ).fillna(0.0)
    intervals["schema_version"] = INTERVAL_SCHEMA_VERSION
    intervals["risk_interval_start_ts_ns"] = start_ns
    intervals["risk_interval_start_event_seq"] = pd.to_numeric(
        intervals["event_seq"],
        errors="raise",
    ).astype("int64")
    intervals["risk_interval_end_ts_ns"] = end_ns
    intervals["risk_interval_end_event_seq"] = pd.to_numeric(
        next_seq.loc[has_next],
        errors="raise",
    ).astype("int64")
    intervals["interval_ms"] = (
        (end_ns - start_ns).clip(lower=0).astype(float) / 1_000_000.0
    )
    intervals["next_event_type"] = next_type.loc[has_next].astype(str)
    intervals["next_event_ts_ns"] = end_ns
    intervals["next_event_seq"] = intervals[
        "risk_interval_end_event_seq"
    ]
    active = state.isin(ACTIVE_STATES) & remaining.gt(0.0)
    intervals["fill_at_risk"] = active.astype(int)
    intervals["cancel_at_risk"] = (
        state.isin({"pending_new", "open", "pending_cancel"})
        & remaining.gt(0.0)
    ).astype(int)
    intervals["jump_at_risk"] = active.astype(int)
    if "repair_at_risk" in intervals:
        repair_at_risk = intervals["repair_at_risk"].fillna(0).astype(bool)
    else:
        repair_at_risk = pd.Series(False, index=intervals.index)
    intervals["repair_at_risk"] = repair_at_risk.astype(int)
    intervals["censor_ts_ns"] = end_ns
    return intervals.reset_index(drop=True)
