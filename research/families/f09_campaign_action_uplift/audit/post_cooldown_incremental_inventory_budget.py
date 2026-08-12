"""Mechanics-only incremental inventory budget after cooldown release."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Final

_EPS: Final = 1e-10


@dataclass(frozen=True)
class BudgetAdmission:
    """Ex-ante order admission result in normalized fill units."""

    allowed: bool
    planned_units: float
    available_units_before: float
    reason: str


@dataclass
class PostCooldownIncrementalInventoryBudget:
    """Track consumed and committed exposure-increasing fill units.

    Reducing orders are deliberately outside this state. An admitted active or
    pending-cancel exposure-increasing order keeps its remaining quantity
    reserved until fill or terminal ACK, preventing overlapping replacements
    from spending the same risk budget twice.
    """

    side: str
    budget_units: float
    unit_size_btc: float
    consumed_units: float = 0.0
    _reserved_by_order: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized_side = str(self.side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("incremental inventory budget side must be BUY or SELL")
        self.side = normalized_side
        self.budget_units = float(self.budget_units)
        self.unit_size_btc = float(self.unit_size_btc)
        if self.unit_size_btc <= 0.0 or not math.isfinite(self.unit_size_btc):
            raise ValueError("incremental inventory unit size must be finite and positive")
        if self.budget_units < 0.0 or math.isnan(self.budget_units):
            raise ValueError("incremental inventory budget cannot be negative or NaN")

    @property
    def reserved_units(self) -> float:
        return float(sum(self._reserved_by_order.values()))

    def reservation_units(self, order_id: str | int) -> float:
        """Return the outstanding reservation for one order."""

        return float(self._reserved_by_order.get(str(order_id), 0.0))

    @property
    def available_units(self) -> float:
        if math.isinf(self.budget_units):
            return math.inf
        return max(
            0.0,
            float(self.budget_units - self.consumed_units - self.reserved_units),
        )

    def reserve(
        self,
        order_id: str | int,
        remaining_qty_btc: float,
        *,
        exposure_increasing: bool,
    ) -> BudgetAdmission:
        """Reserve planned quantity before an order is submitted."""

        if not exposure_increasing:
            return BudgetAdmission(True, 0.0, self.available_units, "reducing_bypass")
        key = str(order_id)
        if key in self._reserved_by_order:
            raise ValueError("incremental inventory order already has a reservation")
        quantity = float(remaining_qty_btc)
        if quantity <= 0.0 or not math.isfinite(quantity):
            raise ValueError("reserved order quantity must be finite and positive")
        planned_units = quantity / self.unit_size_btc
        available_before = self.available_units
        if planned_units > available_before + _EPS:
            return BudgetAdmission(
                False,
                planned_units,
                available_before,
                "incremental_inventory_budget_exhausted",
            )
        self._reserved_by_order[key] = planned_units
        self.assert_conservation()
        return BudgetAdmission(True, planned_units, available_before, "admitted")

    def fill(
        self,
        order_id: str | int,
        fill_qty_btc: float,
        *,
        exposure_increasing: bool,
    ) -> None:
        """Transfer an admitted partial/full fill from reserved to consumed."""

        if not exposure_increasing:
            return
        key = str(order_id)
        if key not in self._reserved_by_order:
            raise ValueError("exposure-increasing fill has no budget reservation")
        quantity = float(fill_qty_btc)
        if quantity <= 0.0 or not math.isfinite(quantity):
            raise ValueError("budgeted fill quantity must be finite and positive")
        fill_units = quantity / self.unit_size_btc
        reserved = float(self._reserved_by_order[key])
        if fill_units > reserved + _EPS:
            raise ValueError("budgeted fill exceeds the order reservation")
        remaining = max(0.0, reserved - fill_units)
        self.consumed_units += fill_units
        if remaining <= _EPS:
            self._reserved_by_order.pop(key)
        else:
            self._reserved_by_order[key] = remaining
        self.assert_conservation()

    def release(self, order_id: str | int) -> float:
        """Release unfilled units after cancel ACK or another terminal state."""

        released = float(self._reserved_by_order.pop(str(order_id), 0.0))
        self.assert_conservation()
        return released

    def rename_reservation(
        self,
        prepared_id: str | int,
        order_id: str | int,
    ) -> float:
        """Atomically bind a prepared admission to its exchange order id."""

        prepared_key = str(prepared_id)
        order_key = str(order_id)
        if prepared_key not in self._reserved_by_order:
            raise ValueError("prepared inventory reservation does not exist")
        if order_key in self._reserved_by_order:
            raise ValueError("inventory reservation target order already exists")
        reserved = float(self._reserved_by_order.pop(prepared_key))
        self._reserved_by_order[order_key] = reserved
        self.assert_conservation()
        return reserved

    def snapshot(self) -> dict[str, float | int]:
        """Return mechanics-only accounting state for replay traces."""

        return {
            "budget_units": float(self.budget_units),
            "unit_size_btc": float(self.unit_size_btc),
            "consumed_units": float(self.consumed_units),
            "reserved_units": float(self.reserved_units),
            "available_units": float(self.available_units),
            "reserved_order_count": int(len(self._reserved_by_order)),
        }

    def assert_conservation(self) -> None:
        if self.consumed_units < -_EPS or self.reserved_units < -_EPS:
            raise RuntimeError("incremental inventory budget became negative")
        if (
            math.isfinite(self.budget_units)
            and self.consumed_units + self.reserved_units
            > self.budget_units + _EPS
        ):
            raise RuntimeError("incremental inventory budget was overspent")


def outcome_blind_budget_grid(
    realized_post_release_units: list[float],
    *,
    maximum_units: int = 3,
) -> tuple[int, ...]:
    """Return distinct p25/p50/p75 whole-unit mechanics candidates."""

    if maximum_units < 1:
        raise ValueError("maximum budget units must be positive")
    values = sorted(
        float(value)
        for value in realized_post_release_units
        if math.isfinite(float(value)) and float(value) > 0.0
    )
    if not values:
        return ()

    def nearest_quantile(probability: float) -> float:
        position = probability * (len(values) - 1)
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        if lower == upper:
            return values[lower]
        weight = position - lower
        return values[lower] * (1.0 - weight) + values[upper] * weight

    candidates = {
        min(maximum_units, max(1, int(math.floor(nearest_quantile(q) + 0.5))))
        for q in (0.25, 0.5, 0.75)
    }
    return tuple(sorted(candidates))
