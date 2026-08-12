"""One frozen two-sided quote-center action for F09 randomized replay."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from strategy.cross_venue_fair_price import (
    CrossVenueFairPriceState,
    FairPriceQuoteShadow,
    project_fair_center_shadow,
)
from strategy.fill_cooldown import (
    LINEAGE_CONTROL_ACTION,
    StratifiedBernoulliLineageRandomizer,
)

CONTROL_ACTION = "local_quote_center"
CANDIDATE_ACTION = "cross_venue_fair_quote_center"
ACTIONS = (CONTROL_ACTION, CANDIDATE_ACTION)


def normalize_probabilities(value: Mapping[str, object] | None) -> dict[str, float]:
    raw = value or {CONTROL_ACTION: 0.5, CANDIDATE_ACTION: 0.5}
    if set(raw) != set(ACTIONS):
        raise ValueError(f"fair-center probabilities require exactly {list(ACTIONS)}")
    probabilities = {action: float(raw[action]) for action in ACTIONS}
    if any(
        not math.isfinite(probability) or probability <= 0.0
        for probability in probabilities.values()
    ):
        raise ValueError("fair-center probabilities must be positive and finite")
    if not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-12):
        raise ValueError("fair-center probabilities must sum to one")
    return probabilities


@dataclass(frozen=True)
class FairCenterAssignment:
    action: str
    uniform_draw: float
    randomization_stratum: str
    assignment_lead_direction: str


@dataclass(frozen=True)
class FairCenterRandomizer:
    seed: int
    family_id: str

    def assign(
        self,
        *,
        utc_day: str,
        assignment_lead_direction: str,
        pre_assignment_campaign_uid: str,
    ) -> FairCenterAssignment:
        direction = str(assignment_lead_direction).upper()
        if direction not in {"BUY", "SELL"}:
            raise ValueError("assignment lead direction must be BUY or SELL")
        legacy, draw, stratum = StratifiedBernoulliLineageRandomizer(
            seed=int(self.seed),
            family_id=str(self.family_id),
        ).assign(
            utc_day=str(utc_day),
            side=direction,
            pre_assignment_lineage_uid=str(pre_assignment_campaign_uid),
        )
        return FairCenterAssignment(
            action=(
                CONTROL_ACTION
                if legacy == LINEAGE_CONTROL_ACTION
                else CANDIDATE_ACTION
            ),
            uniform_draw=float(draw),
            randomization_stratum=str(stratum),
            assignment_lead_direction=direction,
        )


def assignment_lead_direction(state: CrossVenueFairPriceState) -> str:
    if not state.valid or not math.isfinite(float(state.center_shift_price)):
        raise ValueError("lead direction requires a valid fair-price state")
    if float(state.center_shift_price) > 0.0:
        return "BUY"
    if float(state.center_shift_price) < 0.0:
        return "SELL"
    raise ValueError("zero fair-center shift has no assignment direction")


def project_action_pair(
    action: str,
    state: CrossVenueFairPriceState,
    *,
    baseline_bid: float,
    baseline_ask: float,
    best_bid: float,
    best_ask: float,
    tick_size: float,
) -> FairPriceQuoteShadow:
    normalized = str(action)
    if normalized not in ACTIONS:
        raise ValueError(f"unsupported fair-center action: {normalized}")
    shadow = project_fair_center_shadow(
        state,
        baseline_bid=baseline_bid,
        baseline_ask=baseline_ask,
        best_bid=best_bid,
        best_ask=best_ask,
        tick_size=tick_size,
    )
    if normalized == CANDIDATE_ACTION:
        return shadow
    return FairPriceQuoteShadow(
        valid=shadow.valid,
        reason=shadow.reason,
        baseline_bid=float(baseline_bid),
        baseline_ask=float(baseline_ask),
        candidate_bid=float(baseline_bid),
        candidate_ask=float(baseline_ask),
        requested_shift_ticks=int(shadow.requested_shift_ticks),
        effective_shift_ticks=0,
        gtx_clamped=bool(shadow.gtx_clamped),
        pair_spread_preserved=True,
    )
