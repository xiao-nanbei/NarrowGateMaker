"""Checked Python boundary for the native live order-action planner.

The native planner consumes only exchange-lattice integers. This adapter is
the sole float boundary used by MakerEngine: immutable configuration is
validated and converted once after exchange filters are synchronized, while
each decision converts only dynamic prices, quantities, inventory and order
age. Full Python/B0 comparison belongs to release qualification tests, not the
live hot path. This module never submits, cancels, acknowledges or fills an
order.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

QUOTE_ATOMS_PER_UNIT = 100_000_000
_INT64_MAX = (1 << 63) - 1


class NativeOrderActionBoundaryError(RuntimeError):
    """The native planner boundary cannot preserve current B0 semantics."""


@dataclass(frozen=True, slots=True)
class NativeOrderSideBoundary:
    target_price: float
    desired_quantity: float
    exposure_probe_quantity: float
    order: Any
    needs_update: bool
    force_update: bool
    route_allowed: bool
    allow_post: bool
    allow_exposure_increase: bool


def _finite(value: Any, *, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise NativeOrderActionBoundaryError(f"{name} is not numeric") from exc
    if not math.isfinite(result):
        raise NativeOrderActionBoundaryError(f"{name} must be finite")
    return result


def checked_lattice_units(
    value: Any,
    quantum: Any,
    *,
    name: str,
    allow_zero: bool = True,
) -> int:
    """Return an exact int64 lattice coordinate or fail closed.

    The tolerance is the same physical scale used by live exposure checks,
    augmented only by the binary64 reconstruction ULP budget.  It cannot turn
    a genuine fractional tick/lot into a valid exchange value.
    """

    numeric = _finite(value, name=name)
    step = _finite(quantum, name=f"{name}.quantum")
    if step <= 0.0:
        raise NativeOrderActionBoundaryError(f"{name}.quantum must be positive")
    if numeric < 0.0 or (not allow_zero and numeric <= 0.0):
        comparator = "nonnegative" if allow_zero else "positive"
        raise NativeOrderActionBoundaryError(f"{name} must be {comparator}")
    scaled = numeric / step
    if not math.isfinite(scaled):
        raise NativeOrderActionBoundaryError(f"{name} lattice coordinate overflowed")
    units = round(scaled)
    if units < 0 or units > _INT64_MAX:
        raise NativeOrderActionBoundaryError(f"{name} is outside int64 lattice range")
    reconstructed = units * step
    tolerance = max(
        abs(step) * 1e-9,
        1e-12,
        8.0 * math.ulp(numeric),
        8.0 * abs(units) * math.ulp(step),
    )
    if abs(numeric - reconstructed) > tolerance:
        raise NativeOrderActionBoundaryError(
            f"{name}={numeric!r} is off the {step!r} lattice"
        )
    return int(units)


def checked_signed_lattice_units(value: Any, quantum: Any, *, name: str) -> int:
    numeric = _finite(value, name=name)
    if numeric >= 0.0:
        return checked_lattice_units(numeric, quantum, name=name)
    units = checked_lattice_units(-numeric, quantum, name=name)
    return -units


def checked_quote_atoms(
    value: Any,
    *,
    name: str,
    allow_zero: bool = True,
) -> int:
    numeric = _finite(value, name=name)
    if numeric < 0.0 or (not allow_zero and numeric <= 0.0):
        comparator = "nonnegative" if allow_zero else "positive"
        raise NativeOrderActionBoundaryError(f"{name} must be {comparator}")
    scaled = numeric * QUOTE_ATOMS_PER_UNIT
    atoms = round(scaled)
    tolerance_atoms = max(1e-6, 8.0 * math.ulp(scaled))
    if (
        atoms < 0
        or atoms > _INT64_MAX
        or abs(scaled - atoms) > tolerance_atoms
    ):
        raise NativeOrderActionBoundaryError(
            f"{name}={numeric!r} is not representable in quote atoms"
        )
    return int(atoms)


def _order_age_ms(order: Any, now_ts: float) -> float:
    if order is None:
        return 0.0
    create_time = _finite(
        getattr(order, "create_time", now_ts),
        name="order.create_time",
    )
    return max(0.0, (now_ts - create_time) * 1000.0)


def _order_state(native: Any, order: Any) -> Any:
    if order is None:
        return native.LivePlannerOrderState.Empty
    state_name = str(getattr(getattr(order, "state", None), "name", ""))
    mapping = {
        "PENDING_NEW": native.LivePlannerOrderState.PendingNew,
        "OPEN": native.LivePlannerOrderState.Active,
        "PARTIALLY_FILLED": native.LivePlannerOrderState.Active,
        "PENDING_CANCEL": native.LivePlannerOrderState.PendingCancel,
        "FILLED": native.LivePlannerOrderState.Terminal,
        "CANCELED": native.LivePlannerOrderState.Terminal,
        "EXPIRED": native.LivePlannerOrderState.Terminal,
        "REJECTED": native.LivePlannerOrderState.Terminal,
    }
    try:
        return mapping[state_name]
    except KeyError as exc:
        raise NativeOrderActionBoundaryError(
            f"unsupported live order state: {state_name!r}"
        ) from exc


def _side_tuple(
    native: Any,
    *,
    boundary: NativeOrderSideBoundary,
    tick_size: float,
    lot_size: float,
    now_ts: float,
) -> tuple[object, ...]:
    order = boundary.order
    target_price = _finite(boundary.target_price, name="target_price")
    flags = int(native.LIVE_ORDER_SIDE_FLAG_USE_PROVIDED_NEEDS_UPDATE)
    if boundary.route_allowed:
        flags |= int(native.LIVE_ORDER_SIDE_FLAG_ROUTE_ALLOWED)
    if boundary.allow_post:
        flags |= int(native.LIVE_ORDER_SIDE_FLAG_ALLOW_POST)
    if boundary.allow_exposure_increase:
        flags |= int(native.LIVE_ORDER_SIDE_FLAG_ALLOW_EXPOSURE)
    if boundary.force_update:
        flags |= int(native.LIVE_ORDER_SIDE_FLAG_FORCE_UPDATE)
    if boundary.needs_update:
        flags |= int(native.LIVE_ORDER_SIDE_FLAG_PROVIDED_NEEDS_UPDATE)

    age_ms = _order_age_ms(order, now_ts)
    existing_price = (
        _finite(getattr(order, "price", 0.0), name="existing_price")
        if order is not None
        else 0.0
    )
    checked_lattice_units(
        existing_price,
        tick_size,
        name="existing_price",
    )
    price_delta_ticks = (
        abs(target_price - existing_price) / tick_size
        if existing_price > 0.0
        else 0.0
    )
    return (
        checked_lattice_units(
            target_price,
            tick_size,
            name="target_price",
            allow_zero=False,
        ),
        checked_lattice_units(
            boundary.desired_quantity,
            lot_size,
            name="desired_quantity",
        ),
        checked_lattice_units(
            boundary.exposure_probe_quantity,
            lot_size,
            name="exposure_probe_quantity",
            allow_zero=False,
        ),
        target_price,
        checked_lattice_units(
            getattr(order, "remaining_qty", 0.0) if order is not None else 0.0,
            lot_size,
            name="existing_remaining_quantity",
        ),
        age_ms,
        price_delta_ticks,
        _order_state(native, order),
        flags,
    )


def _nonnegative_or_infinite(value: Any, *, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise NativeOrderActionBoundaryError(f"{name} is not numeric") from exc
    if math.isnan(result) or result < 0.0:
        raise NativeOrderActionBoundaryError(
            f"{name} must be nonnegative or positive infinity"
        )
    return result


def _assert_side_result_safety(
    native: Any,
    *,
    plan: Any,
    values: tuple[object, ...],
) -> None:
    """Reject only impossible wire output; semantic parity is qualified offline."""

    target_price = int(values[0])
    desired_quantity = int(values[1])
    target_quantity = int(plan.target_quantity_lots)
    if plan.action == native.LiveOrderAction.Invalid:
        raise NativeOrderActionBoundaryError("native planner rejected checked input")
    if int(plan.target_price_ticks) != target_price:
        raise NativeOrderActionBoundaryError("native planner changed target price")
    if not 0 <= target_quantity <= desired_quantity:
        raise NativeOrderActionBoundaryError(
            "native planner returned quantity outside requested bounds"
        )


class CheckedNativeOrderActionPlanner:
    """Process-frozen checked adapter for the native dual-side planner."""

    __slots__ = (
        "_context_abi",
        "_context_tail",
        "_lot_size",
        "_max_inventory",
        "_max_position_value",
        "_native",
        "_replace",
        "_tick_size",
    )

    def __init__(
        self,
        native: Any,
        *,
        max_inventory: float,
        max_position_value: float,
        tick_size: float,
        lot_size: float,
        min_quantity: float,
        min_notional: float,
        requote_threshold_bps: float,
        add_min_price_change_ticks: float,
        reducing_min_price_change_ticks: float,
        add_min_interval_ms: float,
        reducing_min_interval_ms: float,
        replace_pending_coalesce: bool,
        replace_cancel_first_exposure_increasing: bool,
    ) -> None:
        tick = _finite(tick_size, name="tick_size")
        lot = _finite(lot_size, name="lot_size")
        inventory_limit = _finite(max_inventory, name="max_inventory")
        minimum_quantity = _finite(min_quantity, name="min_quantity")
        minimum_notional = _finite(min_notional, name="min_notional")
        threshold_bps = _finite(
            requote_threshold_bps,
            name="requote_threshold_bps",
        )
        value_limit = _nonnegative_or_infinite(
            max_position_value,
            name="max_position_value",
        )
        add_ticks = _finite(
            add_min_price_change_ticks,
            name="add_min_price_change_ticks",
        )
        reducing_ticks = _finite(
            reducing_min_price_change_ticks,
            name="reducing_min_price_change_ticks",
        )
        add_interval = _finite(add_min_interval_ms, name="add_min_interval_ms")
        reducing_interval = _finite(
            reducing_min_interval_ms,
            name="reducing_min_interval_ms",
        )
        if min(tick, lot, inventory_limit, minimum_quantity) <= 0.0:
            raise NativeOrderActionBoundaryError(
                "tick, lot, max_inventory and min_quantity must be positive"
            )
        if min(
            minimum_notional,
            threshold_bps,
            add_ticks,
            reducing_ticks,
            add_interval,
            reducing_interval,
        ) < 0.0:
            raise NativeOrderActionBoundaryError(
                "native order thresholds must be nonnegative"
            )

        replace_flags = 0
        if replace_pending_coalesce:
            replace_flags |= int(
                native.LIVE_ORDER_REPLACE_FLAG_PENDING_COALESCE
            )
        if replace_cancel_first_exposure_increasing:
            replace_flags |= int(
                native.LIVE_ORDER_REPLACE_FLAG_CANCEL_FIRST_EXPOSURE
            )
        self._native = native
        self._tick_size = tick
        self._lot_size = lot
        self._context_abi = int(native.LIVE_ORDER_ACTION_PLAN_CONTEXT_ABI)
        if self._context_abi != 2:
            raise NativeOrderActionBoundaryError(
                "native order-action planner context ABI mismatch"
            )
        self._max_inventory = inventory_limit
        self._max_position_value = value_limit
        checked_lattice_units(
            inventory_limit,
            lot,
            name="max_inventory",
            allow_zero=False,
        )
        if not math.isinf(value_limit):
            checked_quote_atoms(value_limit, name="max_position_value")
        checked_quote_atoms(
            tick * lot,
            name="quote_value_per_price_tick_lot",
            allow_zero=False,
        )
        checked_quote_atoms(minimum_notional, name="min_notional")
        self._context_tail = (
            checked_lattice_units(
                minimum_quantity,
                lot,
                name="min_quantity",
                allow_zero=False,
            ),
            threshold_bps,
        )
        self._replace = (
            tick,
            lot,
            minimum_notional,
            add_ticks,
            reducing_ticks,
            add_interval,
            reducing_interval,
            replace_flags,
        )

    @property
    def native_module(self) -> Any:
        return self._native

    def compute(
        self,
        *,
        inventory: float,
        mid: float,
        now_ts: float,
        buy: NativeOrderSideBoundary,
        sell: NativeOrderSideBoundary,
    ) -> Any:
        current_inventory = _finite(inventory, name="inventory")
        mark = _finite(mid, name="mid")
        timestamp = _finite(now_ts, name="now_ts")
        if mark <= 0.0:
            raise NativeOrderActionBoundaryError("mid must be positive")
        inventory_lots = checked_signed_lattice_units(
            current_inventory,
            self._lot_size,
            name="inventory",
        )
        checked_quote_atoms(
            mark * self._lot_size,
            name="mid_notional_per_lot",
            allow_zero=False,
        )
        context = (
            self._context_abi,
            current_inventory,
            self._max_inventory,
            self._max_position_value,
            mark,
            self._lot_size,
            inventory_lots,
            *self._context_tail,
        )
        buy_values = _side_tuple(
            self._native,
            boundary=buy,
            tick_size=self._tick_size,
            lot_size=self._lot_size,
            now_ts=timestamp,
        )
        sell_values = _side_tuple(
            self._native,
            boundary=sell,
            tick_size=self._tick_size,
            lot_size=self._lot_size,
            now_ts=timestamp,
        )
        plan = self._native.compute_live_order_action_plan(
            context,
            self._replace,
            buy_values,
            sell_values,
        )
        _assert_side_result_safety(
            self._native,
            plan=plan.buy,
            values=buy_values,
        )
        _assert_side_result_safety(
            self._native,
            plan=plan.sell,
            values=sell_values,
        )
        return plan


def quantity_from_lots(lots: Any, lot_size: float, *, name: str) -> float:
    count = int(lots)
    if count < 0 or count > _INT64_MAX:
        raise NativeOrderActionBoundaryError(f"{name} lots are invalid")
    quantity = count * float(lot_size)
    if checked_lattice_units(quantity, lot_size, name=name) != count:
        raise NativeOrderActionBoundaryError(f"{name} lot reconstruction drifted")
    return quantity
