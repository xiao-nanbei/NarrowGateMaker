"""Path-dependent controls shared by Python and C++ replay contracts.

The Python implementations here are intentionally small reference state
machines. C++ replay mirrors their transition semantics and parity tests bind
the two implementations to the same event sequence.
"""

from __future__ import annotations

import hashlib
import json
import math
import operator
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

SYNC_DEGRADE_TAPE_SCHEMA = "narrowgate_sync_degrade_event_tape.v1"
LOSS_COOLDOWN_SEMANTICS = (
    "round_trip_realized_pnl_policy_clock_flip_fee_split_v2"
)
LOSS_COOLDOWN_SNAPSHOT_SCHEMA = "narrowgate_loss_cooldown_snapshot.v2"
LOSS_COOLDOWN_SNAPSHOT_STATE_FIELDS = (
    "max_consecutive_losses",
    "cooldown_ms",
    "inventory",
    "avg_entry",
    "open_commission",
    "round_trip_pnl",
    "consecutive_losses",
    "cooldown_until_ms",
    "last_cancel_ts_ms",
    "threshold_pending",
    "trigger_count",
    "expiry_count",
    "losing_round_trips",
    "winning_or_flat_round_trips",
    "max_observed_consecutive_losses",
)
SYNC_DEGRADE_SEMANTICS = "system_event_after_market_same_ms_v1"
SYNC_EVENT_CODE = 4
SYNC_CENSOR_CODE = 5
SYNC_REPLAY_MODES = frozenset({"disabled", "frozen_tape", "censor", "stress"})
VISIBILITY_BATCH_AMBIGUITY_REASON = "same_ms_exchange_book_ambiguity"


def effective_latency_seed(params: Mapping[str, Any]) -> int:
    """Resolve the latency seed exactly once across replay identities/backends."""
    raw = params.get("latency_seed")
    return int(params.get("rng_seed", 42)) + 17 if raw is None else int(raw)


def effective_decision_to_gateway_latency_seed(params: Mapping[str, Any]) -> int:
    """Resolve the compute/local-work seed from the effective latency seed."""
    raw = params.get("decision_to_gateway_latency_seed")
    return effective_latency_seed(params) if raw is None else int(raw)


def subtract_lot_quantity(quantity: float, consumed: float, lot: float) -> float:
    """Subtract quantities without losing a lot to binary64 cancellation.

    Snap only a lot boundary inside the representation/subtraction/reconstruction
    ULP budget. Genuine fractional queue residuals outside that budget stay
    fractional; this is not an epsilon added to every fill or a liquidity boost.
    Source quantity precision must justify the exchange-lot lattice: a true
    fraction only a few ULPs from it is indistinguishable from storage roundoff.
    """
    if (
        not math.isfinite(quantity) or not math.isfinite(consumed)
        or not math.isfinite(lot) or quantity < 0.0 or consumed < 0.0 or lot <= 0.0
    ):
        raise ValueError(
            "quantity subtraction requires finite nonnegative quantities and positive lot"
        )
    remaining = max(0.0, quantity - consumed)
    error = math.ulp(quantity) + math.ulp(consumed) + math.ulp(remaining)
    if error >= lot * 0.5:
        return remaining
    units = remaining / lot
    if not math.isfinite(units):
        return remaining
    count = round(units)
    nearest = count * lot
    error += math.ulp(nearest) + abs(count) * math.ulp(lot)
    if error < lot * 0.5 and abs(remaining - nearest) <= error:
        return nearest
    return remaining


def cap_exposure_qty_by_position_value(
    *, side: str, current_qty: float, mid: float, requested_qty: float,
    max_position_value: float, lot: float,
) -> float:
    """The live quote-currency fuse, before submitting an increasing order."""
    if requested_qty <= 0.0 or mid <= 0.0 or max_position_value <= 0.0 or lot <= 0.0:
        return 0.0
    side = str(side).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("position-value cap requires BUY or SELL")
    if math.isinf(max_position_value):
        return requested_qty
    room = max_position_value / mid + (-current_qty if side == "BUY" else current_qty)
    room = max(0.0, math.floor(max(0.0, room) / lot + 1e-12) * lot)
    return min(requested_qty, room)


def hard_risk_reason(
    *, daily_pnl: float, position_value: float, drawdown: float,
    max_daily_loss: float, max_position_value: float, emergency_close_dd: float,
) -> str:
    """Live's ordered hard fuses; equality does not trip a limit."""
    if daily_pnl < -max_daily_loss:
        return "daily_loss"
    if position_value > max_position_value:
        return "position_value"
    if drawdown > emergency_close_dd:
        return "emergency_drawdown"
    return ""


def replay_hard_risk_limits(params: Mapping[str, Any]) -> tuple[float, float, float]:
    """Legacy synthetic calls omit fuses; config-derived replay must carry them."""
    values = []
    for name in ("max_daily_loss", "max_position_value", "emergency_close_dd"):
        if name not in params or params[name] is None:
            values.append(math.inf)
            continue
        value = float(params[name])
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError(f"{name} must be positive and finite when supplied")
        values.append(value)
    return tuple(values)


@dataclass
class ReplayHardRiskState:
    """Marked PnL risk clocks; UTC rollover never resets the session peak."""

    utc_day: int
    day_start_total_pnl: float = 0.0
    session_peak_pnl: float = 0.0
    last_total_pnl: float = 0.0
    total_pnl_offset: float = 0.0

    @classmethod
    def restore(cls, payload: Mapping[str, Any], *, start_ts_ms: int) -> ReplayHardRiskState:
        """Restore an explicit complete risk state, never invent missing baselines."""
        if not isinstance(payload, Mapping):
            raise ValueError("initial risk_state must be a mapping")
        required = {
            "utc_day", "day_start_total_pnl", "session_peak_pnl",
            "last_total_pnl", "total_pnl_offset",
        }
        missing = required - payload.keys()
        if missing:
            raise ValueError("initial risk_state missing fields: " + ", ".join(sorted(missing)))
        if set(payload) - required:
            raise ValueError("initial risk_state contains unknown fields")
        try:
            values = {key: float(payload[key]) for key in required}
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("initial risk_state values must be finite numbers") from exc
        if any(not math.isfinite(value) for value in values.values()):
            raise ValueError("initial risk_state values must be finite")
        day = values["utc_day"]
        if isinstance(payload["utc_day"], bool) or day != int(day):
            raise ValueError("initial risk_state utc_day must be an integer")
        if day > int(start_ts_ms) // 86_400_000:
            raise ValueError("initial risk_state cannot come from a future UTC day")
        values["utc_day"] = int(day)
        return cls(**values)

    def observe(self, ts_ms: int, cash: float, quantity: float, mark: float) -> None:
        day = int(ts_ms) // 86_400_000
        if day > self.utc_day:
            self.utc_day = day
            self.day_start_total_pnl = self.last_total_pnl
        self.last_total_pnl = float(cash) + float(quantity) * float(mark) + self.total_pnl_offset
        self.session_peak_pnl = max(self.session_peak_pnl, self.last_total_pnl)

    @property
    def daily_pnl(self) -> float:
        return self.last_total_pnl - self.day_start_total_pnl

    @property
    def drawdown(self) -> float:
        return max(0.0, self.session_peak_pnl - self.last_total_pnl)


def sha256_file(path: str | Path) -> str:
    resolved = Path(path).expanduser().resolve()
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class RoundTripFillUpdate:
    closed_round_trip: bool
    round_trip_pnl: float
    consecutive_losses: int
    threshold_pending: bool


@dataclass
class ConsecutiveLossCooldown:
    """Mirror InventoryManager's full-round-trip loss counter.

    A threshold crossing becomes active only at the next policy clock, just as
    live detects it inside ``_risk_check`` during a requote. Pending-cancel
    fills continue to update this state while the global quote pause is active.
    """

    max_consecutive_losses: int
    cooldown_ms: int
    inventory: float = 0.0
    avg_entry: float = 0.0
    open_commission: float = 0.0
    round_trip_pnl: float = 0.0
    consecutive_losses: int = 0
    cooldown_until_ms: int = 0
    last_cancel_ts_ms: int = -1
    threshold_pending: bool = False
    trigger_count: int = 0
    expiry_count: int = 0
    losing_round_trips: int = 0
    winning_or_flat_round_trips: int = 0
    max_observed_consecutive_losses: int = 0

    def __post_init__(self) -> None:
        for field_name in ("max_consecutive_losses", "cooldown_ms"):
            raw_value = getattr(self, field_name)
            if isinstance(raw_value, bool):
                raise ValueError(f"loss-cooldown {field_name} must be an integer")
            try:
                value = operator.index(raw_value)
            except TypeError as exc:
                raise ValueError(
                    f"loss-cooldown {field_name} must be an integer"
                ) from exc
            if value < 0:
                raise ValueError(
                    f"loss-cooldown {field_name} must be non-negative"
                )
            setattr(self, field_name, value)
        self.inventory = float(self.inventory)
        self.avg_entry = float(self.avg_entry)
        if not math.isfinite(self.inventory) or not math.isfinite(self.avg_entry):
            raise ValueError("loss-cooldown inventory and avg_entry must be finite")
        if abs(self.inventory) <= 1e-10:
            if abs(self.avg_entry) > 1e-10:
                raise ValueError("flat loss-cooldown state requires avg_entry == 0")
            self.inventory = 0.0
            self.avg_entry = 0.0
        elif self.avg_entry <= 0.0:
            raise ValueError("non-flat loss-cooldown state requires avg_entry > 0")
        for field_name in ("open_commission", "round_trip_pnl"):
            value = float(getattr(self, field_name))
            if not math.isfinite(value):
                raise ValueError(f"loss-cooldown {field_name} must be finite")
            setattr(self, field_name, value)
        for field_name in (
            "consecutive_losses",
            "cooldown_until_ms",
            "trigger_count",
            "expiry_count",
            "losing_round_trips",
            "winning_or_flat_round_trips",
            "max_observed_consecutive_losses",
        ):
            raw_value = getattr(self, field_name)
            if isinstance(raw_value, bool):
                raise ValueError(f"loss-cooldown {field_name} must be an integer")
            try:
                value = operator.index(raw_value)
            except TypeError as exc:
                raise ValueError(
                    f"loss-cooldown {field_name} must be an integer"
                ) from exc
            if value < 0:
                raise ValueError(f"loss-cooldown {field_name} must be non-negative")
            setattr(self, field_name, value)
        if isinstance(self.last_cancel_ts_ms, bool):
            raise ValueError("loss-cooldown last_cancel_ts_ms must be an integer")
        try:
            self.last_cancel_ts_ms = operator.index(self.last_cancel_ts_ms)
        except TypeError as exc:
            raise ValueError(
                "loss-cooldown last_cancel_ts_ms must be an integer"
            ) from exc
        if self.last_cancel_ts_ms < -1:
            raise ValueError(
                "loss-cooldown last_cancel_ts_ms must be -1 or non-negative"
            )
        if type(self.threshold_pending) is not bool:
            raise ValueError("loss-cooldown threshold_pending must be a boolean")

    def _validate_snapshot_policy_state(self) -> None:
        reached = bool(
            self.enabled
            and self.consecutive_losses >= self.max_consecutive_losses
        )
        active_clock = self.cooldown_until_ms > 0
        if self.consecutive_losses > self.max_observed_consecutive_losses:
            raise ValueError("loss-cooldown snapshot maximum streak regressed")
        if self.max_observed_consecutive_losses > self.losing_round_trips:
            raise ValueError(
                "loss-cooldown snapshot maximum streak exceeds losing rounds"
            )
        if self.trigger_count > self.losing_round_trips:
            raise ValueError(
                "loss-cooldown snapshot triggers exceed losing rounds"
            )
        expected_trigger_count = self.expiry_count + int(active_clock)
        if self.trigger_count != expected_trigger_count:
            raise ValueError(
                "loss-cooldown snapshot trigger/expiry clock is inconsistent"
            )
        if not self.enabled:
            if (
                self.consecutive_losses != 0
                or active_clock
                or self.last_cancel_ts_ms != -1
                or self.threshold_pending
                or self.max_observed_consecutive_losses != 0
                or self.trigger_count != 0
                or self.expiry_count != 0
            ):
                raise ValueError(
                    "disabled loss-cooldown snapshot retained policy state"
                )
            return
        if active_clock:
            return
        if self.threshold_pending != reached:
            raise ValueError(
                "loss-cooldown snapshot pending threshold is inconsistent"
            )

    def snapshot(self) -> dict[str, Any]:
        """Serialize every state field that affects the next policy decision."""
        self._validate_snapshot_policy_state()
        return {
            "schema_version": LOSS_COOLDOWN_SNAPSHOT_SCHEMA,
            "semantics": LOSS_COOLDOWN_SEMANTICS,
            "max_consecutive_losses": self.max_consecutive_losses,
            "cooldown_ms": self.cooldown_ms,
            "inventory": self.inventory,
            "avg_entry": self.avg_entry,
            "open_commission": self.open_commission,
            "round_trip_pnl": self.round_trip_pnl,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_until_ms": self.cooldown_until_ms,
            "last_cancel_ts_ms": self.last_cancel_ts_ms,
            "threshold_pending": self.threshold_pending,
            "trigger_count": self.trigger_count,
            "expiry_count": self.expiry_count,
            "losing_round_trips": self.losing_round_trips,
            "winning_or_flat_round_trips": self.winning_or_flat_round_trips,
            "max_observed_consecutive_losses": (
                self.max_observed_consecutive_losses
            ),
        }

    @classmethod
    def restore(cls, payload: Mapping[str, Any]) -> ConsecutiveLossCooldown:
        """Restore only a fully semantics-bound snapshot; legacy state is stale."""
        if payload.get("schema_version") != LOSS_COOLDOWN_SNAPSHOT_SCHEMA:
            raise ValueError("loss-cooldown snapshot schema is stale")
        if payload.get("semantics") != LOSS_COOLDOWN_SEMANTICS:
            raise ValueError("loss-cooldown snapshot semantics are stale")
        required = set(LOSS_COOLDOWN_SNAPSHOT_STATE_FIELDS)
        missing = sorted(required.difference(payload))
        if missing:
            raise ValueError(
                "loss-cooldown snapshot fields are missing: " + ", ".join(missing)
            )
        restored = cls(**{field: payload[field] for field in required})
        if abs(restored.inventory) <= 1e-10 and (
            abs(restored.open_commission) > 1e-10
            or abs(restored.round_trip_pnl) > 1e-10
        ):
            raise ValueError("flat loss-cooldown snapshot retained open economics")
        if (
            restored.max_observed_consecutive_losses
            < restored.consecutive_losses
        ):
            raise ValueError("loss-cooldown snapshot maximum streak regressed")
        restored._validate_snapshot_policy_state()
        return restored

    @property
    def enabled(self) -> bool:
        return self.max_consecutive_losses > 0 and self.cooldown_ms > 0

    def active(self, now_ms: int) -> bool:
        return bool(self.enabled and int(now_ms) < self.cooldown_until_ms)

    def on_policy_clock(self, now_ms: int) -> str:
        """Advance expiry/trigger transitions at a strategy decision clock."""

        now = int(now_ms)
        if self.cooldown_until_ms > 0:
            if now < self.cooldown_until_ms:
                return "active"
            self.cooldown_until_ms = 0
            self.consecutive_losses = 0
            self.threshold_pending = False
            self.expiry_count += 1
            return "expired"
        if self.enabled and self.threshold_pending:
            self.threshold_pending = False
            self.cooldown_until_ms = now + self.cooldown_ms
            self.trigger_count += 1
            return "triggered"
        return "inactive"

    def on_fill(
        self,
        *,
        side: str,
        quantity: float,
        price: float,
        commission: float,
    ) -> RoundTripFillUpdate:
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported fill side {side!r}")
        qty = float(quantity)
        fill_price = float(price)
        fee = float(commission)
        if (
            not math.isfinite(qty)
            or not math.isfinite(fill_price)
            or not math.isfinite(fee)
            or qty <= 0.0
            or fill_price <= 0.0
        ):
            raise ValueError("loss-cooldown fill values are invalid")

        signed_qty = qty if normalized_side == "BUY" else -qty
        closed = False
        closed_pnl = 0.0
        if abs(self.inventory) <= 1e-10:
            self.inventory = signed_qty
            self.avg_entry = fill_price
            self.open_commission = fee
            self.round_trip_pnl = 0.0
        elif (self.inventory > 0.0 and signed_qty > 0.0) or (
            self.inventory < 0.0 and signed_qty < 0.0
        ):
            old_abs = abs(self.inventory)
            new_abs = old_abs + qty
            self.avg_entry = (
                self.avg_entry * old_abs + fill_price * qty
            ) / new_abs
            self.inventory += signed_qty
            self.open_commission += fee
        else:
            old_inventory = self.inventory
            old_abs = abs(old_inventory)
            close_qty = min(qty, old_abs)
            closing_fee = fee * close_qty / qty
            opening_fee = fee - closing_fee
            open_fee_share = (
                self.open_commission * close_qty / old_abs
                if old_abs > 1e-10
                else self.open_commission
            )
            if old_inventory > 0.0:
                realized = (
                    (fill_price - self.avg_entry) * close_qty
                    - closing_fee
                    - open_fee_share
                )
            else:
                realized = (
                    (self.avg_entry - fill_price) * close_qty
                    - closing_fee
                    - open_fee_share
                )
            self.open_commission -= open_fee_share
            self.round_trip_pnl += realized
            remaining = old_abs - close_qty
            if remaining < 1e-10:
                closed = True
                closed_pnl = float(self.round_trip_pnl)
                if closed_pnl < 0.0:
                    self.losing_round_trips += 1
                    if self.enabled:
                        self.consecutive_losses += 1
                    else:
                        self.consecutive_losses = 0
                else:
                    self.consecutive_losses = 0
                    self.winning_or_flat_round_trips += 1
                self.max_observed_consecutive_losses = max(
                    self.max_observed_consecutive_losses,
                    self.consecutive_losses,
                )
                self.threshold_pending = bool(
                    self.enabled
                    and self.consecutive_losses >= self.max_consecutive_losses
                )

                flip_qty = qty - close_qty
                if flip_qty > 1e-10:
                    self.inventory = flip_qty if signed_qty > 0.0 else -flip_qty
                    self.avg_entry = fill_price
                    self.open_commission = opening_fee
                else:
                    self.inventory = 0.0
                    self.avg_entry = 0.0
                    self.open_commission = 0.0
                self.round_trip_pnl = 0.0
            else:
                self.inventory += signed_qty

        return RoundTripFillUpdate(
            closed_round_trip=closed,
            round_trip_pnl=closed_pnl,
            consecutive_losses=int(self.consecutive_losses),
            threshold_pending=bool(self.threshold_pending),
        )


@dataclass
class ReplayOrderDepthPath:
    """Native exact-level path with the live tracker feature semantics."""

    client_order_id: str
    side: str
    price: float
    generation: int
    initial_visible_qty: float
    current_visible_qty: float
    receive_ts_ns: int
    activation_ts_ns: int = 0
    feature_ready_ts_ns: int = 0
    valid: bool = True
    invalid_reason: str = ""
    decrease_events: int = 0
    decrease_qty: float = 0.0
    exact_price_trade_events: int = 0
    exact_price_trade_qty: float = 0.0
    refill_events: int = 0
    refill_qty: float = 0.0
    age_ms: float = 0.0

    @property
    def inferred_cancel_qty(self) -> float:
        return max(0.0, self.decrease_qty - self.exact_price_trade_qty)

    @property
    def inferred_cancel_events(self) -> int:
        return self.decrease_events if self.inferred_cancel_qty > 1e-12 else 0

    @property
    def queue_ahead_estimate(self) -> float:
        initial = max(0.0, self.initial_visible_qty)
        attributed_trade = min(self.decrease_qty, self.exact_price_trade_qty)
        after_trade = max(0.0, initial - attributed_trade)
        cancellation = self.inferred_cancel_qty
        lower = max(0.0, after_trade - cancellation)
        public_before_cancel = max(
            0.0,
            initial - attributed_trade + self.refill_qty,
        )
        ahead_share = (
            after_trade / public_before_cancel
            if public_before_cancel > 1e-12
            else 0.0
        )
        return max(
            lower,
            min(after_trade, after_trade - cancellation * ahead_share),
        )

    def invalidate(self, reason: str) -> None:
        self.valid = False
        if not self.invalid_reason:
            self.invalid_reason = str(reason)

    def observe_trade(
        self,
        quantity: float,
        *,
        receive_ts_ns: int,
        feature_ready_ts_ns: int | None = None,
    ) -> None:
        qty = max(0.0, float(quantity))
        if qty <= 0.0:
            return
        self.exact_price_trade_events += 1
        self.exact_price_trade_qty += qty
        self.receive_ts_ns = max(self.receive_ts_ns, int(receive_ts_ns))
        self.feature_ready_ts_ns = max(
            self.feature_ready_ts_ns,
            int(
                receive_ts_ns
                if feature_ready_ts_ns is None
                else feature_ready_ts_ns
            ),
        )

    def observe_level_change(
        self,
        *,
        quantity_before: float,
        quantity_after: float,
        generation: int,
        receive_ts_ns: int,
        feature_ready_ts_ns: int | None = None,
        ambiguous: bool = False,
    ) -> None:
        if int(generation) != int(self.generation):
            self.invalidate("deep_book_generation_changed")
        if ambiguous:
            self.invalidate("same_ms_exchange_book_ambiguity")
        before = max(0.0, float(quantity_before))
        after = max(0.0, float(quantity_after))
        if after < before - 1e-12:
            self.decrease_events += 1
            self.decrease_qty += before - after
        elif after > before + 1e-12:
            self.refill_events += 1
            self.refill_qty += after - before
        self.current_visible_qty = after
        self.receive_ts_ns = max(self.receive_ts_ns, int(receive_ts_ns))
        self.feature_ready_ts_ns = max(
            self.feature_ready_ts_ns,
            int(
                receive_ts_ns
                if feature_ready_ts_ns is None
                else feature_ready_ts_ns
            ),
        )

    def update_age(self, now_ns: int) -> None:
        source_ts_ns = int(self.feature_ready_ts_ns or self.receive_ts_ns)
        if source_ts_ns <= 0:
            self.age_ms = math.inf
        else:
            self.age_ms = max(
                0.0,
                (int(now_ns) - source_ts_ns) / 1_000_000.0,
            )


def synchronize_visibility_batch_ambiguity_to_cpp(
    paths: Mapping[str, ReplayOrderDepthPath],
    cpp_runtime: Any,
    synchronized_ids: set[str],
) -> int:
    """Mirror authoritative visibility-batch ambiguity into native paths.

    Exchange truth remains ordered by native sequence. When a book update and
    execution trade share one exact feature-ready boundary, their strategy-
    visible order is unknown. The Python visibility scheduler owns that batch
    boundary, so its fail-closed path invalidation must be copied explicitly
    into the message-oriented C++ kernel before the next score evaluation.
    """

    synchronized = 0
    for raw_id, path in paths.items():
        client_order_id = str(raw_id)
        if (
            client_order_id in synchronized_ids
            or str(path.side).upper() != "BUY"
            or bool(path.valid)
            or str(path.invalid_reason) != VISIBILITY_BATCH_AMBIGUITY_REASON
            or not bool(cpp_runtime.has_tracked_path(client_order_id))
        ):
            continue
        cpp_runtime.invalidate_order(
            client_order_id,
            VISIBILITY_BATCH_AMBIGUITY_REASON,
        )
        synchronized_ids.add(client_order_id)
        synchronized += 1
    return synchronized


@dataclass(frozen=True)
class SyncDegradeEvents:
    mode: str
    timestamps_ms: np.ndarray
    event_code: int
    artifact_path: str
    artifact_sha256: str
    environment: str
    coverage_start_ts_ms: int = 0
    coverage_end_ts_ms: int = 0
    semantics_version: str = SYNC_DEGRADE_SEMANTICS
    stress_seed: int | None = None

    @property
    def promotion_eligible(self) -> bool:
        return self.mode in {"disabled", "frozen_tape"}


def _validate_sync_payload(
    payload: Mapping[str, Any],
) -> tuple[str, int, int, np.ndarray]:
    if payload.get("schema_version") != SYNC_DEGRADE_TAPE_SCHEMA:
        raise ValueError("unsupported sync-degrade event tape schema")
    environment = str(payload.get("environment", "") or "").strip()
    if not environment:
        raise ValueError("sync-degrade event tape requires environment")
    coverage_start = int(payload.get("start_ts_ms", 0) or 0)
    coverage_end = int(payload.get("end_ts_ms", 0) or 0)
    if coverage_start <= 0 or coverage_end < coverage_start:
        raise ValueError(
            "sync-degrade event tape requires a valid start_ts_ms/end_ts_ms"
        )
    raw_events = payload.get("events")
    if not isinstance(raw_events, list):
        raise ValueError("sync-degrade event tape events must be a list")
    timestamps: list[int] = []
    forbidden = {"pnl", "reward", "terminal_pnl", "action", "uplift"}
    for row in raw_events:
        if not isinstance(row, Mapping):
            raise ValueError("sync-degrade event rows must be objects")
        if forbidden.intersection(str(key).lower() for key in row):
            raise ValueError("sync-degrade tape contains outcome/action fields")
        ts_ms = int(row.get("ts_ms", 0) or 0)
        if ts_ms <= 0:
            raise ValueError("sync-degrade event ts_ms must be positive")
        timestamps.append(ts_ms)
    values = np.asarray(timestamps, dtype=np.int64)
    if values.size and (
        np.any(values[1:] <= values[:-1])
        or np.unique(values).size != values.size
    ):
        raise ValueError("sync-degrade event timestamps must be sorted and unique")
    if values.size and (
        int(values[0]) < coverage_start or int(values[-1]) > coverage_end
    ):
        raise ValueError("sync-degrade events fall outside tape coverage")
    return (
        environment,
        coverage_start,
        coverage_end,
        np.ascontiguousarray(values),
    )


def load_sync_degrade_events(
    *,
    mode: str,
    tape_path: str | Path | None,
    expected_sha256: str = "",
    expected_environment: str = "",
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
    stress_seed: int = 0,
    stress_interval_s: float = 21_600.0,
) -> SyncDegradeEvents:
    normalized = str(mode or "disabled").strip().lower()
    if normalized not in SYNC_REPLAY_MODES:
        raise ValueError(
            "sync_adjust_replay_mode must be disabled, frozen_tape, censor, or stress"
        )
    if normalized == "disabled":
        return SyncDegradeEvents(
            mode=normalized,
            timestamps_ms=np.empty(0, dtype=np.int64),
            event_code=SYNC_EVENT_CODE,
            artifact_path="",
            artifact_sha256="",
            environment="",
        )
    if normalized in {"frozen_tape", "censor"}:
        if not tape_path:
            raise ValueError(f"sync-degrade {normalized} mode requires a tape")
        resolved = Path(tape_path).expanduser().resolve()
        actual_sha = sha256_file(resolved)
        expected = str(expected_sha256 or "").strip().lower()
        if not expected or actual_sha != expected:
            raise ValueError("sync-degrade event tape SHA256 does not match")
        payload = json.loads(resolved.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("sync-degrade event tape must contain an object")
        (
            environment,
            coverage_start,
            coverage_end,
            timestamps,
        ) = _validate_sync_payload(payload)
        declared_environment = str(expected_environment or "").strip()
        if declared_environment and environment != declared_environment:
            raise ValueError(
                "sync-degrade event tape environment does not match"
            )
        if start_ts_ms is not None and coverage_start > int(start_ts_ms):
            raise ValueError(
                "sync-degrade event tape starts after the replay window"
            )
        if end_ts_ms is not None and coverage_end < int(end_ts_ms):
            raise ValueError(
                "sync-degrade event tape ends before the replay window"
            )
        return SyncDegradeEvents(
            mode=normalized,
            timestamps_ms=timestamps,
            event_code=(
                SYNC_CENSOR_CODE if normalized == "censor" else SYNC_EVENT_CODE
            ),
            artifact_path=str(resolved),
            artifact_sha256=actual_sha,
            environment=environment,
            coverage_start_ts_ms=coverage_start,
            coverage_end_ts_ms=coverage_end,
        )

    if start_ts_ms is None or end_ts_ms is None or end_ts_ms < start_ts_ms:
        raise ValueError("sync-degrade stress mode requires a valid replay interval")
    interval_ms = max(1, int(round(float(stress_interval_s) * 1000.0)))
    rng = np.random.default_rng(int(stress_seed))
    offset = int(rng.integers(0, interval_ms)) if interval_ms > 1 else 0
    first = int(start_ts_ms) + offset
    timestamps = (
        np.arange(first, int(end_ts_ms) + 1, interval_ms, dtype=np.int64)
        if first <= int(end_ts_ms)
        else np.empty(0, dtype=np.int64)
    )
    return SyncDegradeEvents(
        mode=normalized,
        timestamps_ms=np.ascontiguousarray(timestamps),
        event_code=SYNC_EVENT_CODE,
        artifact_path="",
        artifact_sha256="",
        environment="deterministic_stress",
        coverage_start_ts_ms=int(start_ts_ms),
        coverage_end_ts_ms=int(end_ts_ms),
        stress_seed=int(stress_seed),
    )
