"""Live inference and isolated action mapping for dynamic fill hazards.

The model bundle remains prediction-only. A separately hashed policy artifact
may map its two BUY probabilities into one narrowly scoped live action.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BUNDLE_SCHEMA_VERSION = "dynamic_fill_hazard_bundle.v2"
ACTION_POLICY_SCHEMA_VERSION = "dynamic_fill_hazard_live_policy.v1"
CPP_DYNAMIC_FILL_HAZARD_ABI_VERSION = (
    "dynamic_fill_hazard_native_book_q90.v4"
)
MODEL_FEATURES = (
    "elapsed_log1p",
    "visible_state_age_log1p",
    "spread_ticks",
    "quote_distance_ticks",
    "top_bid_size_log1p",
    "top_ask_size_log1p",
    "book_imbalance",
    "side_microprice_adverse_ticks",
    "queue_initial_log1p",
    "queue_remaining_log1p",
    "policy_queue_fraction_left",
    "policy_queue_progress",
    "visible_cancel_events_log1p",
    "visible_cancel_size_log1p",
    "visible_refill_events_log1p",
    "visible_refill_size_log1p",
    "visible_refill_event_share",
    "price_adverse_ticks",
    "price_worst_adverse_ticks",
    "price_recovery_ratio",
    "microprice_adverse_ticks",
    "microprice_worst_adverse_ticks",
    "microprice_recovery_ratio",
    "visible_depth_recovery_ratio",
    "native_adverse_jump_seen",
    "time_since_native_adverse_jump_log1p",
    "clock_hour_sin",
    "clock_hour_cos",
    "role_opener",
    "role_add",
    "role_reducing",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _finite(raw: Mapping[str, Any], name: str, default: float = 0.0) -> float:
    value = float(raw.get(name, default))
    if not math.isfinite(value):
        raise ValueError(f"dynamic hazard feature {name} is non-finite")
    return value


def build_dynamic_fill_hazard_features(
    raw: Mapping[str, Any],
) -> dict[str, float]:
    """Apply the frozen training-time feature transform to one live row."""

    elapsed_ms = max(0.0, _finite(raw, "risk_snapshot_elapsed_ms"))
    visible_age_ms = max(0.0, _finite(raw, "visible_state_age_ms"))
    queue_initial = max(0.0, _finite(raw, "policy_queue_initial"))
    queue_remaining = max(0.0, _finite(raw, "policy_queue_remaining"))
    cancel_events = max(0.0, _finite(raw, "visible_cancel_events"))
    cancel_size = max(0.0, _finite(raw, "visible_cancel_size"))
    refill_events = max(0.0, _finite(raw, "visible_refill_events"))
    refill_size = max(0.0, _finite(raw, "visible_refill_size"))
    jump_age_ms = _finite(raw, "time_since_native_adverse_jump_ms", -1.0)
    role = str(raw.get("current_inventory_role", "")).strip().lower()
    features = {
        "elapsed_log1p": math.log1p(elapsed_ms),
        "visible_state_age_log1p": math.log1p(visible_age_ms),
        "spread_ticks": _finite(raw, "spread_ticks"),
        "quote_distance_ticks": _finite(raw, "quote_distance_ticks"),
        "top_bid_size_log1p": math.log1p(
            max(0.0, _finite(raw, "top_bid_size"))
        ),
        "top_ask_size_log1p": math.log1p(
            max(0.0, _finite(raw, "top_ask_size"))
        ),
        "book_imbalance": _finite(raw, "book_imbalance"),
        "side_microprice_adverse_ticks": _finite(
            raw,
            "side_microprice_adverse_ticks",
        ),
        "queue_initial_log1p": math.log1p(queue_initial),
        "queue_remaining_log1p": math.log1p(queue_remaining),
        "policy_queue_fraction_left": _finite(
            raw,
            "policy_queue_fraction_left",
        ),
        "policy_queue_progress": _finite(raw, "policy_queue_progress"),
        "visible_cancel_events_log1p": math.log1p(cancel_events),
        "visible_cancel_size_log1p": math.log1p(cancel_size),
        "visible_refill_events_log1p": math.log1p(refill_events),
        "visible_refill_size_log1p": math.log1p(refill_size),
        "visible_refill_event_share": _finite(
            raw,
            "visible_refill_event_share",
        ),
        "price_adverse_ticks": _finite(raw, "price_adverse_ticks"),
        "price_worst_adverse_ticks": _finite(
            raw,
            "price_worst_adverse_ticks",
        ),
        "price_recovery_ratio": _finite(raw, "price_recovery_ratio"),
        "microprice_adverse_ticks": _finite(
            raw,
            "microprice_adverse_ticks",
        ),
        "microprice_worst_adverse_ticks": _finite(
            raw,
            "microprice_worst_adverse_ticks",
        ),
        "microprice_recovery_ratio": _finite(
            raw,
            "microprice_recovery_ratio",
        ),
        "visible_depth_recovery_ratio": _finite(
            raw,
            "visible_depth_recovery_ratio",
        ),
        "native_adverse_jump_seen": _finite(
            raw,
            "native_adverse_jump_seen",
        ),
        "time_since_native_adverse_jump_log1p": (
            math.log1p(max(0.0, jump_age_ms))
            if jump_age_ms >= 0.0
            else 0.0
        ),
        "clock_hour_sin": _finite(raw, "clock_hour_sin"),
        "clock_hour_cos": _finite(raw, "clock_hour_cos"),
        "role_opener": float(role == "opener"),
        "role_add": float(role == "add"),
        "role_reducing": float(role == "reducing"),
    }
    if tuple(features) != MODEL_FEATURES:
        raise RuntimeError("dynamic hazard live feature order changed")
    return features


@dataclass(frozen=True)
class DynamicFillHazardPrediction:
    side: str
    exposure_ms: float
    favorable_probability: float
    adverse_probability: float
    favorable_raw_probability: float
    adverse_raw_probability: float
    model_family_id: str


@dataclass(frozen=True)
class DynamicFillHazardShadowObservation:
    client_order_id: str
    side: str
    inventory_role: str
    valid: bool
    reason: str
    edge_ms: int
    elapsed_ms: float
    missed_edges: int
    feature_source_ts_ns: int
    feature_ready_ts_ns: int
    deep_generation: int
    deep_age_ms: float
    order_price: float
    mid: float
    microprice: float
    queue_initial: float
    queue_remaining: float
    cancel_events: int
    cancel_qty: float
    refill_events: int
    refill_qty: float
    favorable_probability: float
    adverse_probability: float
    favorable_raw_probability: float
    adverse_raw_probability: float
    model_family_id: str


@dataclass(frozen=True)
class ProspectivePlacementRecoveryEvaluation:
    """Fresh post-cancel placement state, independent of the retired order."""

    terminal_policy_route: str
    terminal_reason: str
    remaining_quantity: float
    candidate_price: float
    age_ms: float
    fresh_queue_at_tail: float
    gtx_eligible: bool
    activation_supported: bool
    old_path_reused: bool
    observation: DynamicFillHazardShadowObservation


@dataclass
class _LiveOrderHazardState:
    activation_ts_ns: int
    last_edge_index: int
    anchor_mid: float
    anchor_microprice: float
    anchor_top_size: float
    worst_adverse_ticks: float = 0.0
    worst_microprice_adverse_ticks: float = 0.0
    adverse_jump_ts_ns: int = 0


class DynamicFillHazardShadowRuntime:
    """Build causal live rows at the same pre-registered elapsed edges."""

    DEFAULT_EDGES_MS = (
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

    def __init__(
        self,
        bundle: "DynamicFillHazardBundle",
        *,
        tick_size: float,
        lot_size: float,
        exposure_ms: float,
        price_jump_ticks: float,
        edges_ms: Sequence[int] = DEFAULT_EDGES_MS,
        evaluation_interval_ms: float = 0.0,
    ) -> None:
        if tick_size <= 0.0 or lot_size <= 0.0:
            raise ValueError("dynamic hazard live units must be positive")
        if exposure_ms <= 0.0 or price_jump_ticks <= 0.0:
            raise ValueError("dynamic hazard live horizons must be positive")
        edges = tuple(sorted({int(edge) for edge in edges_ms}))
        if not edges or edges[0] != 0 or any(edge < 0 for edge in edges):
            raise ValueError("dynamic hazard elapsed edges are invalid")
        self.bundle = bundle
        self.tick_size = float(tick_size)
        self.lot_size = float(lot_size)
        self.exposure_ms = float(exposure_ms)
        self.price_jump_ticks = float(price_jump_ticks)
        self.edges_ms = edges
        self.evaluation_interval_ms = max(
            0.0,
            float(evaluation_interval_ms),
        )
        self._orders: dict[str, _LiveOrderHazardState] = {}

    @staticmethod
    def _inventory_role(side: str, inventory: float, lot_size: float) -> str:
        if abs(inventory) < max(abs(lot_size) * 0.5, 1e-12):
            return "opener"
        if (side == "BUY" and inventory > 0.0) or (
            side == "SELL" and inventory < 0.0
        ):
            return "add"
        return "reducing"

    def drop_inactive(self, active_client_order_ids: Sequence[str]) -> None:
        active = {str(value) for value in active_client_order_ids}
        for client_order_id in tuple(self._orders):
            if client_order_id not in active:
                self._orders.pop(client_order_id, None)

    def drop_order(self, client_order_id: str) -> None:
        self._orders.pop(str(client_order_id), None)

    def evaluate(
        self,
        *,
        client_order_id: str,
        side: str,
        order_price: float,
        inventory: float,
        path: Any,
        deep_book: Mapping[str, Any],
        now_ns: int,
    ) -> DynamicFillHazardShadowObservation | None:
        normalized_side = str(side).upper()
        if normalized_side not in self.bundle.shadow_sides:
            return None
        best_bid = float(deep_book.get("best_bid", 0.0) or 0.0)
        best_ask = float(deep_book.get("best_ask", 0.0) or 0.0)
        bid_size = max(
            0.0,
            float(deep_book.get("best_bid_qty", 0.0) or 0.0),
        )
        ask_size = max(
            0.0,
            float(deep_book.get("best_ask_qty", 0.0) or 0.0),
        )
        valid_book = bool(
            int(deep_book.get("valid", 0) or 0)
            and best_bid > 0.0
            and best_ask > best_bid
        )
        mid = 0.5 * (best_bid + best_ask) if valid_book else 0.0
        size_sum = bid_size + ask_size
        microprice = (
            (best_ask * bid_size + best_bid * ask_size) / size_sum
            if valid_book and size_sum > 1e-12
            else mid
        )
        state = self._orders.get(client_order_id)
        if state is None:
            activation_ts_ns = int(
                getattr(path, "activation_ts_ns", 0) or int(now_ns)
            )
            state = _LiveOrderHazardState(
                activation_ts_ns=activation_ts_ns,
                last_edge_index=-1,
                anchor_mid=float(mid),
                anchor_microprice=float(microprice),
                anchor_top_size=(
                    bid_size if normalized_side == "BUY" else ask_size
                ),
            )
            self._orders[client_order_id] = state

        elapsed_ms = max(
            0.0,
            (int(now_ns) - state.activation_ts_ns) / 1_000_000.0,
        )
        if self.evaluation_interval_ms > 0.0:
            edge_index = int(elapsed_ms // self.evaluation_interval_ms)
            edge_ms = int(round(edge_index * self.evaluation_interval_ms))
        else:
            crossed = [
                index
                for index, edge in enumerate(self.edges_ms)
                if float(edge) <= elapsed_ms
            ]
            if not crossed:
                return None
            edge_index = crossed[-1]
            edge_ms = int(self.edges_ms[edge_index])
        if edge_index <= state.last_edge_index:
            return None

        if valid_book and state.anchor_mid > 0.0:
            price_adverse = max(
                0.0,
                (
                    (state.anchor_mid - mid) / self.tick_size
                    if normalized_side == "BUY"
                    else (mid - state.anchor_mid) / self.tick_size
                ),
            )
            microprice_adverse = max(
                0.0,
                (
                    (state.anchor_microprice - microprice) / self.tick_size
                    if normalized_side == "BUY"
                    else (microprice - state.anchor_microprice) / self.tick_size
                ),
            )
        else:
            price_adverse = 0.0
            microprice_adverse = 0.0
        state.worst_adverse_ticks = max(
            state.worst_adverse_ticks,
            price_adverse,
        )
        state.worst_microprice_adverse_ticks = max(
            state.worst_microprice_adverse_ticks,
            microprice_adverse,
        )
        if (
            state.adverse_jump_ts_ns <= 0
            and price_adverse >= self.price_jump_ticks
        ):
            state.adverse_jump_ts_ns = int(now_ns)

        previous_edge_index = state.last_edge_index
        state.last_edge_index = edge_index
        missed_edges = max(0, edge_index - previous_edge_index - 1)
        feature_source_ts_ns = max(
            int(getattr(path, "receive_ts_ns", 0) or 0),
            int(deep_book.get("last_trade_receive_ts_ns", 0) or 0),
        )
        path_feature_ready_ts_ns = int(
            getattr(path, "feature_ready_ts_ns", 0)
            or getattr(path, "receive_ts_ns", 0)
            or 0
        )
        feature_ready_ts_ns = max(
            path_feature_ready_ts_ns,
            int(deep_book.get("feature_ready_ts_ns", 0) or 0),
            int(
                deep_book.get("last_trade_feature_ready_ts_ns", 0)
                or deep_book.get("last_trade_receive_ts_ns", 0)
                or 0
            ),
        )
        role = self._inventory_role(
            normalized_side,
            float(inventory),
            self.lot_size,
        )
        queue_initial = max(
            0.0,
            float(getattr(path, "initial_visible_qty", 0.0) or 0.0),
        )
        queue_remaining = max(
            0.0,
            float(getattr(path, "queue_ahead_estimate", 0.0) or 0.0),
        )
        queue_fraction = (
            min(1.0, queue_remaining / queue_initial)
            if queue_initial > 1e-12
            else 0.0
        )
        cancel_events = max(
            0,
            int(getattr(path, "inferred_cancel_events", 0) or 0),
        )
        cancel_qty = max(
            0.0,
            float(getattr(path, "inferred_cancel_qty", 0.0) or 0.0),
        )
        refill_events = max(
            0,
            int(getattr(path, "refill_events", 0) or 0),
        )
        refill_qty = max(
            0.0,
            float(getattr(path, "refill_qty", 0.0) or 0.0),
        )
        path_events = cancel_events + refill_events
        same_side_top_size = (
            bid_size if normalized_side == "BUY" else ask_size
        )
        depth_recovery = (
            min(2.0, same_side_top_size / state.anchor_top_size)
            if state.anchor_top_size > 1e-12
            else 0.0
        )
        seconds = (int(now_ns) // 1_000_000_000) % 86_400
        angle = 2.0 * math.pi * float(seconds) / 86_400.0
        quote_distance = (
            max(0.0, mid - float(order_price)) / self.tick_size
            if normalized_side == "BUY"
            else max(0.0, float(order_price) - mid) / self.tick_size
        )
        raw_features = {
            "risk_snapshot_elapsed_ms": elapsed_ms,
            "visible_state_age_ms": max(
                float(deep_book.get("age_ms", math.inf)),
                float(getattr(path, "age_ms", math.inf)),
            ),
            "spread_ticks": (
                (best_ask - best_bid) / self.tick_size
                if valid_book
                else 0.0
            ),
            "quote_distance_ticks": quote_distance,
            "top_bid_size": bid_size,
            "top_ask_size": ask_size,
            "book_imbalance": (
                (bid_size - ask_size) / size_sum
                if size_sum > 1e-12
                else 0.0
            ),
            "side_microprice_adverse_ticks": (
                (mid - microprice) / self.tick_size
                if normalized_side == "BUY"
                else (microprice - mid) / self.tick_size
            ),
            "policy_queue_initial": queue_initial,
            "policy_queue_remaining": queue_remaining,
            "policy_queue_fraction_left": queue_fraction,
            "policy_queue_progress": max(0.0, 1.0 - queue_fraction),
            "visible_cancel_events": cancel_events,
            "visible_cancel_size": cancel_qty,
            "visible_refill_events": refill_events,
            "visible_refill_size": refill_qty,
            "visible_refill_event_share": (
                refill_events / path_events if path_events > 0 else 0.5
            ),
            "price_adverse_ticks": price_adverse,
            "price_worst_adverse_ticks": state.worst_adverse_ticks,
            "price_recovery_ratio": (
                min(
                    1.0,
                    max(
                        0.0,
                        1.0
                        - price_adverse
                        / max(state.worst_adverse_ticks, 1e-12),
                    ),
                )
                if state.worst_adverse_ticks > 1e-12
                else 1.0
            ),
            "microprice_adverse_ticks": microprice_adverse,
            "microprice_worst_adverse_ticks": (
                state.worst_microprice_adverse_ticks
            ),
            "microprice_recovery_ratio": (
                min(
                    1.0,
                    max(
                        0.0,
                        1.0
                        - microprice_adverse
                        / max(
                            state.worst_microprice_adverse_ticks,
                            1e-12,
                        ),
                    ),
                )
                if state.worst_microprice_adverse_ticks > 1e-12
                else 1.0
            ),
            "visible_depth_recovery_ratio": depth_recovery,
            "native_adverse_jump_seen": int(
                state.adverse_jump_ts_ns > 0
            ),
            "time_since_native_adverse_jump_ms": (
                max(
                    0.0,
                    (int(now_ns) - state.adverse_jump_ts_ns) / 1_000_000.0,
                )
                if state.adverse_jump_ts_ns > 0
                else -1.0
            ),
            "clock_hour_sin": math.sin(angle),
            "clock_hour_cos": math.cos(angle),
            "current_inventory_role": role,
        }
        valid = bool(
            valid_book
            and bool(getattr(path, "valid", False))
            and feature_source_ts_ns > 0
            and feature_ready_ts_ns > 0
            and feature_ready_ts_ns <= int(now_ns)
            and state.activation_ts_ns <= int(now_ns)
        )
        reason = "ok"
        if not valid_book:
            reason = "deep_book_invalid"
        elif not bool(getattr(path, "valid", False)):
            reason = str(
                getattr(path, "invalid_reason", "deep_path_invalid")
            )
        elif feature_source_ts_ns <= 0:
            reason = "missing_feature_source_time"
        elif feature_ready_ts_ns <= 0:
            reason = "missing_feature_ready_time"
        elif feature_ready_ts_ns > int(now_ns):
            reason = "future_feature_time"
        elif state.activation_ts_ns > int(now_ns):
            reason = "future_activation_time"

        prediction = None
        if valid:
            prediction = self.bundle.predict(
                side=normalized_side,
                raw_features=raw_features,
                exposure_ms=self.exposure_ms,
            )
        nan = float("nan")
        return DynamicFillHazardShadowObservation(
            client_order_id=str(client_order_id),
            side=normalized_side,
            inventory_role=role,
            valid=valid,
            reason=reason,
            edge_ms=edge_ms,
            elapsed_ms=float(elapsed_ms),
            missed_edges=missed_edges,
            feature_source_ts_ns=feature_source_ts_ns,
            feature_ready_ts_ns=feature_ready_ts_ns,
            deep_generation=int(getattr(path, "generation", 0) or 0),
            deep_age_ms=float(deep_book.get("age_ms", math.inf)),
            order_price=float(order_price),
            mid=float(mid),
            microprice=float(microprice),
            queue_initial=queue_initial,
            queue_remaining=queue_remaining,
            cancel_events=cancel_events,
            cancel_qty=cancel_qty,
            refill_events=refill_events,
            refill_qty=refill_qty,
            favorable_probability=(
                prediction.favorable_probability if prediction else nan
            ),
            adverse_probability=(
                prediction.adverse_probability if prediction else nan
            ),
            favorable_raw_probability=(
                prediction.favorable_raw_probability if prediction else nan
            ),
            adverse_raw_probability=(
                prediction.adverse_raw_probability if prediction else nan
            ),
            model_family_id=self.bundle.family_id,
        )

    @staticmethod
    def _state_value(state: Any, name: str, default: Any = 0) -> Any:
        if isinstance(state, Mapping):
            return state.get(name, default)
        return getattr(state, name, default)

    def evaluate_prospective_cancel_reentry(
        self,
        *,
        terminal_policy_route: str,
        terminal_reason: str,
        remaining_quantity: float,
        candidate_price: float,
        inventory: float,
        deep_book: Mapping[str, Any],
        candidate_level: Any,
        now_ns: int,
        prospective_id: str = "prospective_cancel_reentry",
    ) -> ProspectivePlacementRecoveryEvaluation:
        """Score a fresh BUY placement after cancel ACK.

        The retired order path is deliberately absent from this interface.
        Queue-at-tail is rebuilt from the current candidate price level; an
        inside-spread candidate starts behind zero displayed quantity.
        """

        route = str(terminal_policy_route).strip().upper()
        reason = str(terminal_reason).strip().lower()
        remaining = float(remaining_quantity)
        if route != "PROSPECTIVE_CANCEL_REENTRY":
            raise ValueError(
                "fresh q90 recovery requires PROSPECTIVE_CANCEL_REENTRY"
            )
        if reason not in {"cancel_ack", "cancel_ack_reconciled"}:
            raise ValueError("fresh q90 recovery requires cancel ACK")
        if not math.isfinite(remaining) or remaining <= 1e-12:
            raise ValueError(
                "fresh q90 recovery requires positive remaining quantity"
            )

        now = int(now_ns)
        price = float(candidate_price)
        if now <= 0 or not math.isfinite(price) or price <= 0.0:
            raise ValueError("fresh q90 recovery candidate identity is invalid")
        price_tick = round(price / self.tick_size)
        on_tick = abs(price - price_tick * self.tick_size) <= max(
            1e-9,
            self.tick_size * 1e-8,
        )

        best_bid = float(deep_book.get("best_bid", 0.0) or 0.0)
        best_ask = float(deep_book.get("best_ask", 0.0) or 0.0)
        bid_size = max(
            0.0,
            float(deep_book.get("best_bid_qty", 0.0) or 0.0),
        )
        ask_size = max(
            0.0,
            float(deep_book.get("best_ask_qty", 0.0) or 0.0),
        )
        valid_book = bool(
            int(deep_book.get("valid", 0) or 0)
            and best_bid > 0.0
            and best_ask > best_bid
        )
        mid = 0.5 * (best_bid + best_ask) if valid_book else 0.0
        size_sum = bid_size + ask_size
        microprice = (
            (best_ask * bid_size + best_bid * ask_size) / size_sum
            if valid_book and size_sum > 1e-12
            else mid
        )
        inside_spread = bool(
            valid_book
            and price > best_bid + self.tick_size * 0.5
            and price < best_ask - self.tick_size * 0.5
        )
        level_valid = bool(
            self._state_value(candidate_level, "valid", False)
        )
        level_covered = bool(
            self._state_value(candidate_level, "covered", False)
        )
        level_price = float(
            self._state_value(candidate_level, "price", price) or price
        )
        level_price_matches = abs(level_price - price) <= self.tick_size * 0.5
        level_quantity = max(
            0.0,
            float(self._state_value(candidate_level, "quantity", 0.0) or 0.0),
        )
        fresh_queue = 0.0 if inside_spread else level_quantity
        queue_known = bool(
            inside_spread
            or (level_valid and level_covered and level_price_matches)
        )
        gtx_eligible = bool(valid_book and on_tick and price < best_ask - 1e-12)

        feature_source_ts_ns = max(
            int(deep_book.get("last_receive_ts_ns", 0) or 0),
            int(deep_book.get("last_trade_receive_ts_ns", 0) or 0),
            int(self._state_value(candidate_level, "receive_ts_ns", 0) or 0),
        )
        feature_ready_ts_ns = max(
            int(deep_book.get("feature_ready_ts_ns", 0) or 0),
            int(
                deep_book.get("last_trade_feature_ready_ts_ns", 0)
                or deep_book.get("last_trade_receive_ts_ns", 0)
                or 0
            ),
            int(
                self._state_value(
                    candidate_level,
                    "feature_ready_ts_ns",
                    0,
                )
                or 0
            ),
        )
        deep_age_ms = float(deep_book.get("age_ms", math.inf))
        level_age_ms = float(
            self._state_value(candidate_level, "age_ms", math.inf)
        )
        visible_age_ms = max(deep_age_ms, level_age_ms)
        activation_supported = bool(
            valid_book
            and queue_known
            and gtx_eligible
            and feature_source_ts_ns > 0
            and feature_ready_ts_ns > 0
            and feature_ready_ts_ns <= now
            and math.isfinite(visible_age_ms)
        )
        invalid_reason = "ok"
        if not valid_book:
            invalid_reason = "deep_book_invalid"
        elif not on_tick:
            invalid_reason = "candidate_price_off_tick"
        elif not gtx_eligible:
            invalid_reason = "candidate_gtx_reject"
        elif not queue_known:
            invalid_reason = "candidate_level_not_causally_covered"
        elif feature_source_ts_ns <= 0:
            invalid_reason = "missing_feature_source_time"
        elif feature_ready_ts_ns <= 0:
            invalid_reason = "missing_feature_ready_time"
        elif feature_ready_ts_ns > now:
            invalid_reason = "future_feature_time"
        elif not math.isfinite(visible_age_ms):
            invalid_reason = "candidate_level_age_invalid"

        role = self._inventory_role("BUY", float(inventory), self.lot_size)
        seconds = (now // 1_000_000_000) % 86_400
        angle = 2.0 * math.pi * float(seconds) / 86_400.0
        raw_features = {
            "risk_snapshot_elapsed_ms": 0.0,
            "visible_state_age_ms": (
                visible_age_ms if math.isfinite(visible_age_ms) else 0.0
            ),
            "spread_ticks": (
                (best_ask - best_bid) / self.tick_size if valid_book else 0.0
            ),
            "quote_distance_ticks": (
                max(0.0, mid - price) / self.tick_size if valid_book else 0.0
            ),
            "top_bid_size": bid_size,
            "top_ask_size": ask_size,
            "book_imbalance": (
                (bid_size - ask_size) / size_sum
                if size_sum > 1e-12
                else 0.0
            ),
            "side_microprice_adverse_ticks": (
                (mid - microprice) / self.tick_size if valid_book else 0.0
            ),
            "policy_queue_initial": fresh_queue,
            "policy_queue_remaining": fresh_queue,
            "policy_queue_fraction_left": 1.0 if fresh_queue > 1e-12 else 0.0,
            "policy_queue_progress": 0.0,
            "visible_cancel_events": 0,
            "visible_cancel_size": 0.0,
            "visible_refill_events": 0,
            "visible_refill_size": 0.0,
            "visible_refill_event_share": 0.5,
            "price_adverse_ticks": 0.0,
            "price_worst_adverse_ticks": 0.0,
            "price_recovery_ratio": 1.0,
            "microprice_adverse_ticks": 0.0,
            "microprice_worst_adverse_ticks": 0.0,
            "microprice_recovery_ratio": 1.0,
            "visible_depth_recovery_ratio": 1.0,
            "native_adverse_jump_seen": 0,
            "time_since_native_adverse_jump_ms": -1.0,
            "clock_hour_sin": math.sin(angle),
            "clock_hour_cos": math.cos(angle),
            "current_inventory_role": role,
        }
        prediction = None
        if activation_supported:
            prediction = self.bundle.predict(
                side="BUY",
                raw_features=raw_features,
                exposure_ms=self.exposure_ms,
            )
        nan = float("nan")
        observation = DynamicFillHazardShadowObservation(
            client_order_id=str(prospective_id),
            side="BUY",
            inventory_role=role,
            valid=activation_supported,
            reason=invalid_reason,
            edge_ms=0,
            elapsed_ms=0.0,
            missed_edges=0,
            feature_source_ts_ns=feature_source_ts_ns,
            feature_ready_ts_ns=feature_ready_ts_ns,
            deep_generation=int(deep_book.get("generation", 0) or 0),
            deep_age_ms=deep_age_ms,
            order_price=price,
            mid=mid,
            microprice=microprice,
            queue_initial=fresh_queue,
            queue_remaining=fresh_queue,
            cancel_events=0,
            cancel_qty=0.0,
            refill_events=0,
            refill_qty=0.0,
            favorable_probability=(
                prediction.favorable_probability if prediction else nan
            ),
            adverse_probability=(
                prediction.adverse_probability if prediction else nan
            ),
            favorable_raw_probability=(
                prediction.favorable_raw_probability if prediction else nan
            ),
            adverse_raw_probability=(
                prediction.adverse_raw_probability if prediction else nan
            ),
            model_family_id=self.bundle.family_id,
        )
        return ProspectivePlacementRecoveryEvaluation(
            terminal_policy_route=route,
            terminal_reason=reason,
            remaining_quantity=remaining,
            candidate_price=price,
            age_ms=0.0,
            fresh_queue_at_tail=fresh_queue,
            gtx_eligible=gtx_eligible,
            activation_supported=activation_supported,
            old_path_reused=False,
            observation=observation,
        )

    def has_active_order_state(self, client_order_id: str) -> bool:
        return str(client_order_id) in self._orders

    @property
    def active_order_state_count(self) -> int:
        return len(self._orders)


class DynamicFillHazardBundle:
    """Strict loader and scalar predictor for the frozen hazard bundle."""

    def __init__(
        self,
        payload: Mapping[str, Any],
        *,
        path: Path,
        file_sha256: str,
        shadow_sides: Sequence[str],
    ) -> None:
        self.path = path
        self.file_sha256 = str(file_sha256)
        self.family_id = str(payload["family_id"])
        self.action_family_allowed = bool(
            payload.get("action_family_allowed", False)
        )
        if self.action_family_allowed:
            raise ValueError(
                "dynamic fill-hazard live shadow requires a prediction-only artifact"
            )
        feature_names = tuple(str(name) for name in payload["feature_names"])
        if feature_names != MODEL_FEATURES:
            raise ValueError("dynamic fill-hazard artifact feature schema changed")
        self._models = payload["models"]
        self.shadow_sides = tuple(
            dict.fromkeys(str(side).strip().upper() for side in shadow_sides)
        )
        if not self.shadow_sides or any(
            side not in {"BUY", "SELL"} for side in self.shadow_sides
        ):
            raise ValueError("dynamic fill-hazard shadow sides are invalid")
        missing = [side for side in self.shadow_sides if side not in self._models]
        if missing:
            raise ValueError(
                f"dynamic fill-hazard artifact lacks shadow side(s): {missing}"
            )

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        expected_file_sha256: str = "",
        shadow_sides: Sequence[str] = ("BUY",),
    ) -> "DynamicFillHazardBundle":
        resolved = Path(path).expanduser().resolve()
        file_sha256 = _sha256(resolved)
        if expected_file_sha256 and file_sha256 != expected_file_sha256:
            raise ValueError(
                "dynamic fill-hazard artifact SHA256 does not match config"
            )
        payload = json.loads(resolved.read_text(encoding="utf-8"))
        if payload.get("schema_version") != BUNDLE_SCHEMA_VERSION:
            raise ValueError("unsupported dynamic fill-hazard bundle schema")
        expected_bundle_sha = str(payload.get("bundle_sha256", ""))
        without_hash = dict(payload)
        without_hash.pop("bundle_sha256", None)
        if not expected_bundle_sha or _canonical_sha256(without_hash) != expected_bundle_sha:
            raise ValueError("dynamic fill-hazard internal bundle hash is invalid")
        return cls(
            payload,
            path=resolved,
            file_sha256=file_sha256,
            shadow_sides=shadow_sides,
        )

    @staticmethod
    def _predict_model(
        model: Mapping[str, Any],
        features: Mapping[str, float],
        *,
        exposure_ms: float,
    ) -> tuple[float, float]:
        names = tuple(str(name) for name in model["feature_names"])
        if names != MODEL_FEATURES:
            raise ValueError("dynamic fill-hazard head feature schema changed")
        mean = tuple(float(value) for value in model["feature_mean"])
        scale = tuple(float(value) for value in model["feature_scale"])
        coefficients = tuple(float(value) for value in model["coefficients"])
        if not (len(mean) == len(scale) == len(coefficients) == len(names)):
            raise ValueError("dynamic fill-hazard head dimensions are invalid")
        eta = float(model["intercept"])
        for index, name in enumerate(names):
            standardized = (
                (float(features[name]) - mean[index])
                / max(scale[index], 1e-12)
            )
            standardized = max(-12.0, min(12.0, standardized))
            eta += standardized * coefficients[index]
        eta = max(-25.0, min(20.0, eta))
        exposure_s = max(0.001, float(exposure_ms) / 1_000.0)
        raw = -math.expm1(-min(50.0, math.exp(eta) * exposure_s))
        calibrator = model.get("nested_calibrator")
        if not isinstance(calibrator, Mapping):
            return raw, raw
        contract = calibrator["contract"]
        lower, upper = (
            float(value) for value in contract["probability_clip"]
        )
        clipped = max(lower, min(upper, raw))
        score = math.log(-math.log1p(-clipped))
        calibrated_eta = (
            float(calibrator["intercept"])
            + float(calibrator["slope"]) * score
        )
        cumulative_hazard = math.exp(
            max(-25.0, min(20.0, calibrated_eta))
        )
        calibrated = -math.expm1(-min(50.0, cumulative_hazard))
        return raw, calibrated

    def predict(
        self,
        *,
        side: str,
        raw_features: Mapping[str, Any],
        exposure_ms: float,
    ) -> DynamicFillHazardPrediction:
        normalized_side = str(side).strip().upper()
        if normalized_side not in self.shadow_sides:
            raise ValueError(
                f"dynamic fill-hazard side {normalized_side} is not shadow-enabled"
            )
        features = build_dynamic_fill_hazard_features(raw_features)
        favorable_raw, favorable = self._predict_model(
            self._models[normalized_side]["favorable_fill"],
            features,
            exposure_ms=exposure_ms,
        )
        adverse_raw, adverse = self._predict_model(
            self._models[normalized_side]["adverse_fill"],
            features,
            exposure_ms=exposure_ms,
        )
        return DynamicFillHazardPrediction(
            side=normalized_side,
            exposure_ms=float(exposure_ms),
            favorable_probability=float(favorable),
            adverse_probability=float(adverse),
            favorable_raw_probability=float(favorable_raw),
            adverse_raw_probability=float(adverse_raw),
            model_family_id=self.family_id,
        )

    def native_model_payload(self, side: str) -> dict[str, Any]:
        """Return one verified side payload for the native inference ABI."""

        normalized_side = str(side).strip().upper()
        if normalized_side not in self.shadow_sides:
            raise ValueError(
                f"dynamic fill-hazard side {normalized_side} is not shadow-enabled"
            )
        payload = self._models[normalized_side]
        # Detach the native handoff from the bundle's in-memory identity. The
        # loader above remains the only authority that admits artifact bytes.
        return json.loads(json.dumps(payload, allow_nan=False))


@dataclass(frozen=True)
class DynamicFillHazardActionPolicy:
    """Frozen BUY-only cancel/re-enter mapping over model probabilities."""

    path: Path
    file_sha256: str
    policy_id: str
    model_family_id: str
    model_file_sha256: str
    entry_threshold: float
    evaluation_interval_ms: float
    eligible_roles: tuple[str, ...]
    validation_activation_rate: float

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        expected_file_sha256: str,
        model_bundle: DynamicFillHazardBundle,
    ) -> "DynamicFillHazardActionPolicy":
        resolved = Path(path).expanduser().resolve()
        file_sha256 = _sha256(resolved)
        if not expected_file_sha256 or file_sha256 != expected_file_sha256:
            raise ValueError(
                "dynamic fill-hazard action policy SHA256 does not match config"
            )
        payload = json.loads(resolved.read_text(encoding="utf-8"))
        if payload.get("schema_version") != ACTION_POLICY_SCHEMA_VERSION:
            raise ValueError(
                "unsupported dynamic fill-hazard action policy schema"
            )
        if str(payload.get("model_family_id", "")) != model_bundle.family_id:
            raise ValueError(
                "dynamic fill-hazard action policy model family changed"
            )
        if str(payload.get("model_file_sha256", "")) != model_bundle.file_sha256:
            raise ValueError(
                "dynamic fill-hazard action policy model hash changed"
            )
        if str(payload.get("side", "")).upper() != "BUY":
            raise ValueError("dynamic fill-hazard action policy must be BUY-only")
        if str(payload.get("score_formula", "")) != (
            "probability_adverse_fill-probability_favorable_fill"
        ):
            raise ValueError("dynamic fill-hazard action score formula changed")
        if str(payload.get("entry_action", "")) != "cancel":
            raise ValueError("dynamic fill-hazard entry action must be cancel")
        if str(payload.get("recovery_rule", "")) != "score_below_entry_threshold":
            raise ValueError("dynamic fill-hazard recovery rule changed")
        if str(payload.get("reentry_action", "")) != "baseline_reenter":
            raise ValueError("dynamic fill-hazard re-entry action changed")
        if not bool(payload.get("reducing_side_unchanged", False)):
            raise ValueError("dynamic fill-hazard policy must preserve reducing")
        roles = tuple(str(value).lower() for value in payload["eligible_roles"])
        if roles != ("opener", "add"):
            raise ValueError(
                "dynamic fill-hazard policy eligibility must be opener/add"
            )
        threshold = float(payload["entry_threshold"])
        interval_ms = float(payload["evaluation_interval_ms"])
        if not math.isfinite(threshold) or threshold <= 0.0:
            raise ValueError("dynamic fill-hazard action threshold is invalid")
        if not math.isfinite(interval_ms) or interval_ms <= 0.0:
            raise ValueError(
                "dynamic fill-hazard action evaluation interval is invalid"
            )
        return cls(
            path=resolved,
            file_sha256=file_sha256,
            policy_id=str(payload["policy_id"]),
            model_family_id=model_bundle.family_id,
            model_file_sha256=model_bundle.file_sha256,
            entry_threshold=threshold,
            evaluation_interval_ms=interval_ms,
            eligible_roles=roles,
            validation_activation_rate=float(
                payload["validation_activation_rate"]
            ),
        )

    @staticmethod
    def adverse_value(
        favorable_probability: float,
        adverse_probability: float,
    ) -> float:
        return float(adverse_probability) - float(favorable_probability)

    def eligible(self, observation: DynamicFillHazardShadowObservation) -> bool:
        return bool(
            observation.valid
            and observation.side == "BUY"
            and observation.inventory_role in self.eligible_roles
        )

    def score(self, observation: DynamicFillHazardShadowObservation) -> float:
        return self.adverse_value(
            observation.favorable_probability,
            observation.adverse_probability,
        )

    def cancel_required(
        self,
        observation: DynamicFillHazardShadowObservation,
    ) -> bool:
        return bool(
            self.eligible(observation)
            and self.score(observation) >= self.entry_threshold
        )

    def recovered(
        self,
        observation: DynamicFillHazardShadowObservation,
    ) -> bool:
        return bool(
            self.eligible(observation)
            and self.score(observation) < self.entry_threshold
        )


@dataclass(frozen=True)
class CppDynamicFillHazardRuntime:
    """Hash-bound adapter around the native BUY q90 parity kernel.

    This adapter is evidence infrastructure. Its presence does not authorize
    C++ tick replay, a randomized action, shadow trading, or live deployment.
    """

    runtime: Any
    bundle: DynamicFillHazardBundle
    policy: DynamicFillHazardActionPolicy
    native_module_path: Path
    native_module_sha256: str
    abi_version: str

    def identity(self) -> dict[str, Any]:
        return {
            "schema_version": "dynamic_fill_hazard_cpp_identity.v1",
            "abi_version": self.abi_version,
            "native_module_path": str(self.native_module_path),
            "native_module_sha256": self.native_module_sha256,
            "model_family_id": self.bundle.family_id,
            "model_path": str(self.bundle.path),
            "model_file_sha256": self.bundle.file_sha256,
            "policy_id": self.policy.policy_id,
            "policy_path": str(self.policy.path),
            "policy_file_sha256": self.policy.file_sha256,
            "feature_schema_sha256": _canonical_sha256(list(MODEL_FEATURES)),
            "scope": "native_book_and_buy_q90_parity_kernel_only",
            "full_cpp_tick_replay_authority": False,
            "action_or_live_authorization": False,
        }

    def apply_exchange_book_event(
        self,
        event: Any,
        *,
        feature_ready_ts_ns: int | None = None,
        feature_source_ts_ns: int | None = None,
        execution_trade_same_ms: bool = False,
    ) -> dict[str, Any]:
        visible_ts_ns = int(
            event.local_receive_ts_ns
            if feature_ready_ts_ns is None
            else feature_ready_ts_ns
        )
        return dict(
            self.runtime.apply_book_message(
                event_type=str(event.event_type),
                exchange_ts_ns=int(event.exchange_ts_ns),
                receive_ts_ns=int(
                    event.local_receive_ts_ns
                    if feature_source_ts_ns is None
                    else feature_source_ts_ns
                ),
                feature_ready_ts_ns=visible_ts_ns,
                event_time_ms=int(event.event_time_ns or 0) // 1_000_000,
                transaction_time_ms=(
                    int(event.transaction_time_ns or 0) // 1_000_000
                ),
                first_update_id=event.first_update_id,
                final_update_id=event.final_update_id,
                previous_final_update_id=event.previous_final_update_id,
                last_update_id=event.last_update_id,
                levels=tuple(event.levels),
                execution_trade_same_ms=bool(execution_trade_same_ms),
            )
        )

    def activate_order(
        self,
        client_order_id: str,
        order_price: float,
        activation_ts_ns: int,
    ) -> dict[str, Any]:
        return dict(
            self.runtime.activate_order(
                str(client_order_id),
                float(order_price),
                int(activation_ts_ns),
            )
        )

    def invalidate_order(self, client_order_id: str, reason: str) -> None:
        self.runtime.invalidate_order(str(client_order_id), str(reason))

    def drop_inactive(self, active_client_order_ids: Sequence[str]) -> None:
        self.runtime.drop_inactive([str(value) for value in active_client_order_ids])

    def observe_trade(
        self,
        *,
        is_sell_trade: bool,
        trade_price: float,
        quantity: float,
        receive_ts_ns: int,
        feature_ready_ts_ns: int | None = None,
    ) -> None:
        ready_ts_ns = int(
            receive_ts_ns
            if feature_ready_ts_ns is None
            else feature_ready_ts_ns
        )
        self.runtime.observe_trade(
            bool(is_sell_trade),
            float(trade_price),
            float(quantity),
            int(receive_ts_ns),
            ready_ts_ns,
        )

    def evaluate(
        self,
        client_order_id: str,
        inventory: float,
        now_ns: int,
    ) -> dict[str, Any]:
        return dict(
            self.runtime.evaluate(
                str(client_order_id),
                float(inventory),
                int(now_ns),
            )
        )

    def evaluate_prospective_cancel_reentry(
        self,
        *,
        candidate_price: float,
        inventory: float,
        now_ns: int,
    ) -> dict[str, Any]:
        return dict(
            self.runtime.evaluate_prospective_cancel_reentry(
                float(candidate_price),
                float(inventory),
                int(now_ns),
            )
        )

    def on_fill(
        self,
        client_order_id: str,
        remaining_after: float,
        now_ns: int,
    ) -> str:
        return str(
            self.runtime.on_fill(
                str(client_order_id),
                float(remaining_after),
                int(now_ns),
            )
        )

    def on_cancel_ack(
        self,
        client_order_id: str,
        now_ns: int,
        remaining_after: float = math.nan,
    ) -> str:
        return str(
            self.runtime.on_cancel_ack(
                str(client_order_id),
                int(now_ns),
                float(remaining_after),
            )
        )

    def on_order_terminal(
        self,
        client_order_id: str,
        now_ns: int,
        reason: str = "unsupported",
        remaining_after: float = math.nan,
    ) -> str:
        return str(
            self.runtime.on_order_terminal(
                str(client_order_id),
                int(now_ns),
                str(reason),
                float(remaining_after),
            )
        )

    def counters(self) -> dict[str, int]:
        native = dict(self.runtime.counters())
        legacy_keys = (
            "eval_count",
            "valid_eval_count",
            "invalid_eval_count",
            "keep_count",
            "cancel_request_count",
            "cancel_ack_count",
            "pre_ack_fill_count",
            "recovery_count",
            "reentry_count",
            "retain_invalid_count",
        )
        return {key: int(native[key]) for key in legacy_keys}

    def prospective_counters(self) -> dict[str, int]:
        native = dict(self.runtime.counters())
        prospective_keys = (
            "prospective_eval_count",
            "prospective_valid_count",
            "prospective_invalid_count",
        )
        return {key: int(native[key]) for key in prospective_keys}

    def sequence_stats(self) -> dict[str, int]:
        return {
            str(key): int(value)
            for key, value in dict(self.runtime.sequence_stats()).items()
        }

    @property
    def hold_active(self) -> bool:
        return bool(self.runtime.hold_active)

    @property
    def hold_order_id(self) -> str:
        return str(self.runtime.hold_order_id)

    @property
    def hold_phase(self) -> str:
        return str(self.runtime.hold_phase)

    @property
    def tracked_path_count(self) -> int:
        return int(self.runtime.tracked_path_count)

    @property
    def evaluation_state_count(self) -> int:
        return int(self.runtime.evaluation_state_count)

    def has_tracked_path(self, client_order_id: str) -> bool:
        return bool(self.runtime.has_tracked_path(str(client_order_id)))


def load_cpp_dynamic_fill_hazard_runtime(
    *,
    model_path: str | Path,
    expected_model_sha256: str,
    policy_path: str | Path,
    expected_policy_sha256: str,
    tick_size: float,
    lot_size: float,
    exposure_ms: float,
    price_jump_ticks: float,
    strict_sequence: bool = True,
    strict_after_ns: int = 0,
) -> CppDynamicFillHazardRuntime:
    """Load the native parity kernel only after strict artifact validation."""

    bundle = DynamicFillHazardBundle.load(
        model_path,
        expected_file_sha256=str(expected_model_sha256),
        shadow_sides=("BUY",),
    )
    policy = DynamicFillHazardActionPolicy.load(
        policy_path,
        expected_file_sha256=str(expected_policy_sha256),
        model_bundle=bundle,
    )
    try:
        import narrowgate_cpp  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "native dynamic fill-hazard parity kernel is not importable"
        ) from exc
    abi_version = str(
        getattr(narrowgate_cpp, "DYNAMIC_FILL_HAZARD_ABI_VERSION", "")
    )
    if abi_version != CPP_DYNAMIC_FILL_HAZARD_ABI_VERSION:
        raise RuntimeError(
            "native dynamic fill-hazard ABI identity is missing or stale"
        )
    runtime_type = getattr(narrowgate_cpp, "DynamicFillHazardRuntime", None)
    if runtime_type is None:
        raise RuntimeError("native dynamic fill-hazard runtime is unavailable")
    module_path = Path(str(narrowgate_cpp.__file__)).resolve()
    runtime = runtime_type(
        bundle.native_model_payload("BUY"),
        {
            "tick_size": float(tick_size),
            "lot_size": float(lot_size),
            "exposure_ms": float(exposure_ms),
            "price_jump_ticks": float(price_jump_ticks),
            "evaluation_interval_ms": policy.evaluation_interval_ms,
            "entry_threshold": policy.entry_threshold,
            "strict_sequence": bool(strict_sequence),
            "strict_after_ns": int(strict_after_ns),
        },
    )
    return CppDynamicFillHazardRuntime(
        runtime=runtime,
        bundle=bundle,
        policy=policy,
        native_module_path=module_path,
        native_module_sha256=_sha256(module_path),
        abi_version=abi_version,
    )
