"""Frozen maker-only reducing-BUY aggression for multi-level SHORT campaigns."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from strategy.fill_cooldown import (
    LINEAGE_CONTROL_ACTION,
    StratifiedBernoulliLineageRandomizer,
)

CONTROL_ACTION = "baseline_reducing_buy"
CANDIDATE_ACTION = "aggressive_maker_reducing_buy"
ACTIONS = (CONTROL_ACTION, CANDIDATE_ACTION)

DEFAULT_TRIGGER_INVENTORY_BTC = -0.002
DEFAULT_RELEASE_INVENTORY_BTC = -0.001


def normalize_probabilities(value: Mapping[str, object] | None) -> dict[str, float]:
    raw = value or {CONTROL_ACTION: 0.5, CANDIDATE_ACTION: 0.5}
    if set(raw) != set(ACTIONS):
        raise ValueError(
            "reducing-BUY probabilities must contain exactly " f"{list(ACTIONS)}"
        )
    probabilities = {action: float(raw[action]) for action in ACTIONS}
    if any(
        not math.isfinite(probability) or probability <= 0.0
        for probability in probabilities.values()
    ):
        raise ValueError("reducing-BUY probabilities must be positive and finite")
    if not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-12):
        raise ValueError("reducing-BUY probabilities must sum to one")
    return probabilities


@dataclass(frozen=True)
class MultiShortRepairAssignment:
    action: str
    uniform_draw: float
    randomization_stratum: str


@dataclass(frozen=True)
class MultiShortRepairRandomizer:
    seed: int
    family_id: str

    def assign(
        self,
        *,
        utc_day: str,
        pre_assignment_campaign_uid: str,
    ) -> MultiShortRepairAssignment:
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
        return MultiShortRepairAssignment(
            action=action,
            uniform_draw=float(uniform_draw),
            randomization_stratum=str(stratum),
        )


@dataclass(frozen=True)
class AggressiveMakerBuyQuote:
    baseline_price: float
    best_bid: float
    best_ask: float
    maker_ceiling: float
    selected_price: float
    improvement_ticks: float
    improvement_bps: float
    changed: bool
    maker_valid: bool


def aggressive_maker_buy_price(
    *,
    baseline_price: float,
    best_bid: float,
    best_ask: float,
    tick_size: float,
    mark_price: float,
) -> AggressiveMakerBuyQuote:
    """Return ``min(ask1-tick, max(baseline, bid1))`` on the tick grid."""

    baseline = float(baseline_price)
    bid = float(best_bid)
    ask = float(best_ask)
    tick = float(tick_size)
    mark = float(mark_price)
    values = (baseline, bid, ask, tick, mark)
    if not all(math.isfinite(value) for value in values):
        raise ValueError("reducing-BUY quote inputs must be finite")
    if min(values) <= 0.0:
        raise ValueError("reducing-BUY quote inputs must be positive")
    if ask <= bid:
        raise ValueError("reducing-BUY action requires a non-crossed BBO")

    grid_tolerance = max(tick * 1e-6, abs(ask) * 1e-12)

    def tick_index(price: float, label: str) -> int:
        index = int(round(price / tick))
        if abs(float(index) * tick - price) > grid_tolerance:
            raise ValueError(f"{label} is not aligned to the exchange tick grid")
        return index

    baseline_tick = tick_index(baseline, "baseline reducing BUY")
    bid_tick = tick_index(bid, "best bid")
    ask_tick = tick_index(ask, "best ask")
    maker_ceiling_tick = ask_tick - 1
    if maker_ceiling_tick < bid_tick:
        raise ValueError("BBO has no valid maker BUY price")
    selected_tick = min(maker_ceiling_tick, max(baseline_tick, bid_tick))
    maker_ceiling = float(maker_ceiling_tick) * tick
    selected = float(selected_tick) * tick
    maker_valid = bool(bid_tick <= selected_tick <= maker_ceiling_tick)
    if not maker_valid:
        raise ValueError("candidate reducing BUY is not a valid maker quote")

    improvement_ticks = float(selected_tick - baseline_tick)
    improvement_bps = (selected - baseline) / mark * 10_000.0
    return AggressiveMakerBuyQuote(
        baseline_price=baseline,
        best_bid=bid,
        best_ask=ask,
        maker_ceiling=float(maker_ceiling),
        selected_price=float(selected),
        improvement_ticks=float(improvement_ticks),
        improvement_bps=float(improvement_bps),
        changed=bool(abs(selected - baseline) > 1e-12),
        maker_valid=maker_valid,
    )


def treatment_should_start(
    inventory_btc: float,
    *,
    trigger_inventory_btc: float = DEFAULT_TRIGGER_INVENTORY_BTC,
) -> bool:
    trigger = float(trigger_inventory_btc)
    if not math.isfinite(trigger) or trigger >= 0.0:
        raise ValueError("trigger inventory must be finite and negative")
    return float(inventory_btc) <= trigger + 1e-12


def treatment_should_end(
    inventory_btc: float,
    *,
    release_inventory_btc: float = DEFAULT_RELEASE_INVENTORY_BTC,
) -> bool:
    release = float(release_inventory_btc)
    if not math.isfinite(release) or release >= 0.0:
        raise ValueError("release inventory must be finite and negative")
    return float(inventory_btc) >= release - 1e-12
