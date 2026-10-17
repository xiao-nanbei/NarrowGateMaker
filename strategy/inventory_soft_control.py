"""Pure offline contribution boundaries, without live activation or new risk gates."""

import math

from strategy.risk_selection import (
    RiskSelectionCandidate, RiskSelectionObservation, candidate_role,
)

COOLDOWN_MODES = frozenset({"original", "no_consecutive_multiplier", "admission_off"})
COOLDOWN_KEY = "_research_inventory_cooldown_mode"
MARKOUT_KEY = "_research_inventory_generic_markout_scope"


def resolve_modes(params):
    pair = (params.get(COOLDOWN_KEY, "original"),
            params.get(MARKOUT_KEY, "original_all_roles"))
    if (pair[0] not in COOLDOWN_MODES
            or pair[1] not in {"original_all_roles", "exclude_pure_reducing"}
            or pair == ("admission_off", "exclude_pure_reducing")):
        raise ValueError("undeclared inventory soft-control mode combination")
    if (pair != ("original", "original_all_roles")
            and params.get("_research_generic_markout_policy_enabled", True) is not True):
        raise ValueError("role-scoped study cannot inherit global markout deletion")
    return pair


def consecutive_multiplier(units, cooldown_mode):
    if cooldown_mode not in COOLDOWN_MODES:
        raise ValueError("invalid cooldown mode")
    if not math.isfinite(units) or units < 0:
        raise ValueError("invalid same-side quantity units")
    return 1.0 if cooldown_mode == "no_consecutive_multiplier" else max(1.0, units)


def cooldown_blocks_action(latent_active, cooldown_mode):
    if type(latent_active) is not bool or cooldown_mode not in COOLDOWN_MODES:
        raise ValueError("invalid cooldown admission input")
    return latent_active and cooldown_mode != "admission_off"


def pure_reducing_role(*, side, inventory, quantity, pending_orders, snapshot_known):
    """Reuse the existing whole-order reachable-inventory classifier.

    Callers provide the original, pre-generic candidate quantity and coherent
    visible pending snapshot. Unknown only denies this optional exemption; it
    is not an instruction to block the baseline order. No future fills enter.
    """
    if snapshot_known is not True:
        return "unknown"
    try:
        observation = RiskSelectionObservation(0, 0, inventory, tuple(pending_orders))
        candidate = RiskSelectionCandidate("inventory-soft-control", "E", side,
                                           quantity, "POST")
        return candidate_role(observation, candidate)
    except (TypeError, ValueError, OverflowError):
        return "unknown"


def generic_markout_enabled_for_role(scope, role):
    if scope not in {"original_all_roles", "exclude_pure_reducing"}:
        raise ValueError("invalid generic markout scope")
    return not (scope == "exclude_pure_reducing" and role == "reducing")


def rounded_release_quantity(*, quantity, size_mult, lot, side, inventory, pending_orders):
    """Confirm minimum-lot restoration cannot turn the exempt order into an add.

    This is checked before prices are composed. It never grants eligibility to
    a pre-generic crossing order. Later original room/anti-flip/notional caps
    only decrease this quantity; a zero quantity is not an executable release.
    """
    if (not all(math.isfinite(x) for x in (quantity, size_mult, lot))
            or quantity <= 0 or lot <= 0 or not 0 < size_mult <= 1):
        return None
    rounded = math.floor(quantity * size_mult / lot) * lot
    if quantity >= lot and rounded < lot:
        rounded = lot
    if rounded <= 0 or rounded > quantity:
        return None
    if pure_reducing_role(side=side, inventory=inventory, quantity=rounded,
                          pending_orders=pending_orders, snapshot_known=True) != "reducing":
        return None
    return rounded


def pre_generic_sizes(*, order_size, lot, inventory, max_inventory, eta, symmetric):
    """Same original pre-policy sizing arithmetic, before generic shrinkage.

    This is a read-only preview for role eligibility, not an order-size override.
    Actual submission continues through the original sizing and risk consumers.
    """
    bid = ask = order_size
    if eta > 0 and max_inventory > 1e-10:
        normalized = inventory / max_inventory
        if inventory > 0:
            bid = max(lot, math.floor(order_size * math.exp(-eta * normalized) / lot) * lot)
        if inventory < 0:
            ask = max(lot, math.floor(order_size * math.exp(eta * normalized) / lot) * lot)
    if symmetric:
        bid = ask = min(bid, ask)
    return bid, ask
