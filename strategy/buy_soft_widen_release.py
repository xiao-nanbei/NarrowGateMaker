"""Research-only single-decision BUY soft-widen release kernel."""

from __future__ import annotations

import math
from dataclasses import dataclass


VALID_ROLES = ("opener", "add")


@dataclass(frozen=True)
class BuySoftWidenReleaseDecision:
    target_reached: bool
    role: str
    eligible: bool
    requested: bool
    effective: bool
    baseline_spread_mult: float
    selected_spread_mult: float
    reason: str


def inventory_role(inventory_btc: float, lot_size_btc: float) -> str:
    """Return the pre-decision BUY exposure role."""

    tolerance = max(abs(float(lot_size_btc)) * 0.5, 1e-12)
    if abs(float(inventory_btc)) < tolerance:
        return "opener"
    if float(inventory_btc) > 0.0:
        return "add"
    return "reducing"


def evaluate_buy_soft_widen_release(
    *,
    enabled: bool,
    decision_ts_ms: int,
    target_decision_ts_ms: int,
    target_role: str,
    inventory_btc: float,
    lot_size_btc: float,
    allow_post: bool,
    allow_exposure_increase: bool,
    hard_reason_active: bool,
    baseline_spread_mult: float,
    spread_mult_cap: float = 1.0,
) -> BuySoftWidenReleaseDecision:
    """Evaluate one pre-frozen action without consulting an outcome model."""

    role = inventory_role(inventory_btc, lot_size_btc)
    normalized_role = str(target_role).strip().lower()
    if normalized_role not in VALID_ROLES:
        raise ValueError(f"target_role must be one of {VALID_ROLES}")
    if not math.isfinite(float(baseline_spread_mult)) or baseline_spread_mult <= 0.0:
        raise ValueError("baseline_spread_mult must be finite and positive")
    if not math.isclose(float(spread_mult_cap), 1.0, abs_tol=1e-12):
        raise ValueError("v1 freezes spread_mult_cap=1.0")

    reached = bool(enabled and int(decision_ts_ms) == int(target_decision_ts_ms))
    if not reached:
        return BuySoftWidenReleaseDecision(
            target_reached=False,
            role=role,
            eligible=False,
            requested=False,
            effective=False,
            baseline_spread_mult=float(baseline_spread_mult),
            selected_spread_mult=float(baseline_spread_mult),
            reason="not_target",
        )

    eligible = bool(
        role == normalized_role
        and role in VALID_ROLES
        and allow_post
        and allow_exposure_increase
        and not hard_reason_active
    )
    if not eligible:
        return BuySoftWidenReleaseDecision(
            target_reached=True,
            role=role,
            eligible=False,
            requested=False,
            effective=False,
            baseline_spread_mult=float(baseline_spread_mult),
            selected_spread_mult=float(baseline_spread_mult),
            reason="role_or_permission_ineligible",
        )

    selected = min(float(baseline_spread_mult), float(spread_mult_cap))
    effective = selected < float(baseline_spread_mult) - 1e-12
    return BuySoftWidenReleaseDecision(
        target_reached=True,
        role=role,
        eligible=True,
        requested=True,
        effective=effective,
        baseline_spread_mult=float(baseline_spread_mult),
        selected_spread_mult=selected,
        reason="applied" if effective else "spread_mult_already_at_or_below_cap",
    )


__all__ = [
    "BuySoftWidenReleaseDecision",
    "VALID_ROLES",
    "evaluate_buy_soft_widen_release",
    "inventory_role",
]
