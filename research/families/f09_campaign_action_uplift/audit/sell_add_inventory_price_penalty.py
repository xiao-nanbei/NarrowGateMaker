"""Frozen SELL-add inventory price-penalty mechanics.

The action changes only exposure-increasing SELL quotes after a campaign is
already short.  It is intentionally a single fixed curve rather than a tuning
surface.  Quantity, reducing BUY quotes, cooldowns, and every other blocker
remain outside this module.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
    StratifiedBernoulliLineageRandomizer,
)

CONTROL_ACTION = "baseline_current_quote"
CANDIDATE_ACTION = "sell_add_inventory_price_penalty"
ACTIONS = (CONTROL_ACTION, CANDIDATE_ACTION)

DEFAULT_INVENTORY_UNIT_BTC = 0.001
DEFAULT_STEP_BPS_PER_SHORT_UNIT = 0.5
DEFAULT_MAX_PENALTY_BPS = 1.5


def normalize_probabilities(value: Mapping[str, object] | None) -> dict[str, float]:
    raw = value or {CONTROL_ACTION: 0.5, CANDIDATE_ACTION: 0.5}
    if set(raw) != set(ACTIONS):
        raise ValueError(
            "SELL-add price-penalty probabilities must contain exactly "
            f"{list(ACTIONS)}"
        )
    probabilities = {action: float(raw[action]) for action in ACTIONS}
    if any(
        not math.isfinite(probability) or probability <= 0.0
        for probability in probabilities.values()
    ):
        raise ValueError("SELL-add price-penalty probabilities must be positive")
    if not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-12):
        raise ValueError("SELL-add price-penalty probabilities must sum to one")
    return probabilities


@dataclass(frozen=True)
class SellAddPenaltyAssignment:
    action: str
    uniform_draw: float
    randomization_stratum: str


@dataclass(frozen=True)
class SellAddPenaltyQuote:
    baseline_ask: float
    requested_ask: float
    selected_ask: float
    short_units: float
    requested_penalty_bps: float
    requested_penalty_ticks: float
    realized_penalty_bps: float
    realized_penalty_ticks: float
    cap_truncated: bool
    fully_truncated: bool


@dataclass(frozen=True)
class SellAddPenaltyRandomizer:
    seed: int
    family_id: str

    def assign(
        self,
        *,
        utc_day: str,
        pre_assignment_campaign_uid: str,
    ) -> SellAddPenaltyAssignment:
        randomizer = StratifiedBernoulliLineageRandomizer(
            seed=int(self.seed),
            family_id=str(self.family_id),
        )
        legacy_action, uniform_draw, stratum = randomizer.assign(
            utc_day=str(utc_day),
            side="SELL",
            pre_assignment_lineage_uid=str(pre_assignment_campaign_uid),
        )
        action = (
            CONTROL_ACTION
            if legacy_action == LINEAGE_CONTROL_ACTION
            else CANDIDATE_ACTION
        )
        return SellAddPenaltyAssignment(
            action=action,
            uniform_draw=float(uniform_draw),
            randomization_stratum=str(stratum),
        )


def short_inventory_units(
    inventory_btc: float,
    *,
    inventory_unit_btc: float = DEFAULT_INVENTORY_UNIT_BTC,
) -> float:
    unit = float(inventory_unit_btc)
    if not math.isfinite(unit) or unit <= 0.0:
        raise ValueError("inventory_unit_btc must be positive and finite")
    inventory = float(inventory_btc)
    if not math.isfinite(inventory):
        raise ValueError("inventory_btc must be finite")
    return max(0.0, -inventory / unit)


def sell_add_penalty_bps(
    inventory_btc: float,
    *,
    inventory_unit_btc: float = DEFAULT_INVENTORY_UNIT_BTC,
    step_bps_per_short_unit: float = DEFAULT_STEP_BPS_PER_SHORT_UNIT,
    max_penalty_bps: float = DEFAULT_MAX_PENALTY_BPS,
) -> float:
    step = float(step_bps_per_short_unit)
    cap = float(max_penalty_bps)
    if not math.isfinite(step) or step <= 0.0:
        raise ValueError("step_bps_per_short_unit must be positive and finite")
    if not math.isfinite(cap) or cap <= 0.0:
        raise ValueError("max_penalty_bps must be positive and finite")
    return min(
        cap,
        step
        * short_inventory_units(
            inventory_btc,
            inventory_unit_btc=inventory_unit_btc,
        ),
    )


def apply_sell_add_price_penalty(
    *,
    baseline_bid: float,
    baseline_ask: float,
    mid: float,
    inventory_btc: float,
    tick_size: float,
    max_pair_spread: float,
    inventory_unit_btc: float = DEFAULT_INVENTORY_UNIT_BTC,
    step_bps_per_short_unit: float = DEFAULT_STEP_BPS_PER_SHORT_UNIT,
    max_penalty_bps: float = DEFAULT_MAX_PENALTY_BPS,
) -> SellAddPenaltyQuote:
    """Move only the SELL-add ask outward, preserving the reducing bid.

    The requested penalty is converted from bps using the decision mid and
    rounded away from the market.  A positive pair-spread cap is then applied
    by clipping only the ask; this makes cap truncation directly auditable.
    """

    bid = float(baseline_bid)
    ask = float(baseline_ask)
    midpoint = float(mid)
    tick = float(tick_size)
    pair_cap = float(max_pair_spread)
    if not all(math.isfinite(value) for value in (bid, ask, midpoint, tick)):
        raise ValueError("quote prices, mid, and tick must be finite")
    if bid <= 0.0 or ask <= 0.0 or midpoint <= 0.0 or tick <= 0.0:
        raise ValueError("quote prices, mid, and tick must be positive")
    if ask + 1e-12 < bid:
        raise ValueError("baseline ask cannot be below baseline bid")

    short_units = short_inventory_units(
        inventory_btc,
        inventory_unit_btc=inventory_unit_btc,
    )
    requested_bps = sell_add_penalty_bps(
        inventory_btc,
        inventory_unit_btc=inventory_unit_btc,
        step_bps_per_short_unit=step_bps_per_short_unit,
        max_penalty_bps=max_penalty_bps,
    )
    raw_requested = ask + midpoint * requested_bps / 10_000.0
    requested_ask = math.ceil(raw_requested / tick - 1e-9) * tick
    requested_ask = max(ask, requested_ask)

    selected_ask = requested_ask
    if math.isfinite(pair_cap) and pair_cap > 0.0:
        max_ask = math.floor((bid + pair_cap) / tick + 1e-9) * tick
        selected_ask = min(selected_ask, max(ask, max_ask))
    selected_ask = max(ask, selected_ask)

    requested_ticks = max(0.0, (requested_ask - ask) / tick)
    realized_ticks = max(0.0, (selected_ask - ask) / tick)
    realized_bps = max(0.0, (selected_ask - ask) / midpoint * 10_000.0)
    truncated = selected_ask + 1e-12 < requested_ask
    return SellAddPenaltyQuote(
        baseline_ask=ask,
        requested_ask=float(requested_ask),
        selected_ask=float(selected_ask),
        short_units=float(short_units),
        requested_penalty_bps=float(requested_bps),
        requested_penalty_ticks=float(requested_ticks),
        realized_penalty_bps=float(realized_bps),
        realized_penalty_ticks=float(realized_ticks),
        cap_truncated=bool(truncated),
        fully_truncated=bool(truncated and realized_ticks <= 1e-12),
    )
