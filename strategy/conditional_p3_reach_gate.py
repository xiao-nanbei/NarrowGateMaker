"""Action-bound conditional-P3 reach gate for executable quote prices.

The independent toxicity policy proposes the outward move. Conditional P3
only supplies the already-frozen mechanics status for the current side, role,
distance, and prediction epoch. This module changes quote price only; it does
not own order lifetime, cancellation, cooldown, size, or inventory state.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ConditionalP3QuoteDecision:
    bid_price: float
    ask_price: float
    bid_distance_ticks: int
    ask_distance_ticks: int
    bid_requested: bool
    ask_requested: bool
    bid_price_changed: bool
    ask_price_changed: bool
    bid_spread_cap_noop: bool
    ask_spread_cap_noop: bool


def executable_price_tick(price: float, tick_size: float, *, name: str) -> int:
    value = float(price)
    tick = float(tick_size)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError(f"{name} must be positive and finite")
    if not math.isfinite(tick) or tick <= 0.0:
        raise ValueError("tick_size must be positive and finite")
    ratio = value / tick
    nearest = round(ratio)
    if abs(ratio - nearest) > 1e-9:
        raise ValueError(f"{name} must lie on the executable tick grid")
    return int(nearest)


def apply_conditional_p3_outward_quote_action(
    *,
    bid_price: float,
    ask_price: float,
    best_bid: float,
    best_ask: float,
    inventory_btc: float,
    lot_size: float,
    tick_size: float,
    max_pair_spread: float,
    bid_allowed: bool,
    ask_allowed: bool,
    tox_bid: float,
    tox_ask: float,
    bid_toxicity_threshold: float,
    ask_toxicity_threshold: float,
    bid_gate_status: int,
    ask_gate_status: int,
    outward_ticks: int,
) -> ConditionalP3QuoteDecision:
    """Apply one frozen finite-tick outward proposal to exposure quotes."""

    if int(bid_gate_status) not in {0, 1, 2} or int(ask_gate_status) not in {
        0,
        1,
        2,
    }:
        raise ValueError("conditional P3 gate status must be 0, 1, or 2")
    if int(outward_ticks) <= 0:
        raise ValueError("outward_ticks must be positive")
    for value, name in (
        (tox_bid, "tox_bid"),
        (tox_ask, "tox_ask"),
        (bid_toxicity_threshold, "bid_toxicity_threshold"),
        (ask_toxicity_threshold, "ask_toxicity_threshold"),
    ):
        numeric = float(value)
        if not math.isfinite(numeric) or not 0.0 <= numeric <= 1.0:
            raise ValueError(f"{name} must lie in [0, 1]")

    bid_tick = executable_price_tick(bid_price, tick_size, name="bid_price")
    ask_tick = executable_price_tick(ask_price, tick_size, name="ask_price")
    best_bid_tick = executable_price_tick(best_bid, tick_size, name="best_bid")
    best_ask_tick = executable_price_tick(best_ask, tick_size, name="best_ask")
    if best_bid_tick >= best_ask_tick or bid_tick >= ask_tick:
        raise ValueError("conditional P3 action requires a valid executable book/quote")

    inventory = float(inventory_btc)
    lot = float(lot_size)
    if not math.isfinite(inventory) or not math.isfinite(lot) or lot <= 0.0:
        raise ValueError("inventory and lot size must be finite, with lot_size > 0")
    bid_exposure = inventory >= 0.0
    ask_exposure = inventory <= 0.0
    bid_requested = bool(
        bid_allowed
        and bid_exposure
        and float(tox_bid) >= float(bid_toxicity_threshold)
        and int(bid_gate_status) == 2
    )
    ask_requested = bool(
        ask_allowed
        and ask_exposure
        and float(tox_ask) >= float(ask_toxicity_threshold)
        and int(ask_gate_status) == 2
    )

    proposed_bid_tick = bid_tick - int(outward_ticks) if bid_requested else bid_tick
    proposed_ask_tick = ask_tick + int(outward_ticks) if ask_requested else ask_tick
    proposed_bid = proposed_bid_tick * float(tick_size)
    proposed_ask = proposed_ask_tick * float(tick_size)
    spread_limit = float(max_pair_spread)
    if not math.isfinite(spread_limit) or spread_limit < 0.0:
        raise ValueError("max_pair_spread must be finite and nonnegative")
    spread_supported = bool(
        spread_limit <= 0.0
        or proposed_ask - proposed_bid <= spread_limit + 1e-12
    )
    bid_changed = bool(spread_supported and bid_requested and proposed_bid_tick < bid_tick)
    ask_changed = bool(spread_supported and ask_requested and proposed_ask_tick > ask_tick)
    return ConditionalP3QuoteDecision(
        bid_price=proposed_bid if bid_changed else float(bid_price),
        ask_price=proposed_ask if ask_changed else float(ask_price),
        bid_distance_ticks=best_bid_tick - bid_tick,
        ask_distance_ticks=ask_tick - best_ask_tick,
        bid_requested=bid_requested,
        ask_requested=ask_requested,
        bid_price_changed=bid_changed,
        ask_price_changed=ask_changed,
        bid_spread_cap_noop=bool(bid_requested and not spread_supported),
        ask_spread_cap_noop=bool(ask_requested and not spread_supported),
    )


__all__ = [
    "ConditionalP3QuoteDecision",
    "apply_conditional_p3_outward_quote_action",
    "executable_price_tick",
]
