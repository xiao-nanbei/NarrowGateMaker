"""Outcome-blind projection for a robust outward-only external edge guard."""

from __future__ import annotations

import math
from dataclasses import dataclass

from strategy.cross_venue_fair_price import (
    CrossVenueFairPriceState,
    weighted_median,
)

PROFILE_OMISSIONS: tuple[tuple[str, str | None], ...] = (
    ("all_venues", None),
    ("leave_bitget_out", "bitget"),
    ("leave_bybit_out", "bybit"),
    ("leave_okx_out", "okx"),
)


@dataclass(frozen=True)
class ExternalEdgeProfile:
    profile: str
    adverse_side: str
    buy_requested_ticks: int
    sell_requested_ticks: int


@dataclass(frozen=True)
class ExternalAdverseQuoteEdgeProjection:
    valid: bool
    reason: str
    adverse_side: str
    requested_ticks: int
    effective_ticks: int
    candidate_bid: float
    candidate_ask: float
    cap_clipped: bool
    loo_consistent: bool
    profiles: tuple[ExternalEdgeProfile, ...]


def _invalid_projection(
    reason: str,
    *,
    baseline_bid: float,
    baseline_ask: float,
    profiles: tuple[ExternalEdgeProfile, ...] = (),
) -> ExternalAdverseQuoteEdgeProjection:
    return ExternalAdverseQuoteEdgeProjection(
        valid=False,
        reason=str(reason),
        adverse_side="",
        requested_ticks=0,
        effective_ticks=0,
        candidate_bid=float(baseline_bid),
        candidate_ask=float(baseline_ask),
        cap_clipped=False,
        loo_consistent=False,
        profiles=profiles,
    )


def project_external_adverse_quote_edge(
    state: CrossVenueFairPriceState,
    *,
    baseline_bid: float,
    baseline_ask: float,
    tick_size: float,
    max_pair_spread_bps: float,
) -> ExternalAdverseQuoteEdgeProjection:
    """Require all and each leave-one-venue-out profile to agree."""

    bid = float(baseline_bid)
    ask = float(baseline_ask)
    tick = float(tick_size)
    if not state.valid:
        return _invalid_projection(
            state.reason,
            baseline_bid=bid,
            baseline_ask=ask,
        )
    if len(state.venues) != 3:
        return _invalid_projection(
            "three_venue_common_support_required",
            baseline_bid=bid,
            baseline_ask=ask,
        )
    if not (
        tick > 0.0
        and float(max_pair_spread_bps) > 0.0
        and bid > 0.0
        and ask > bid
        and state.local_mid > 0.0
    ):
        return _invalid_projection(
            "invalid_quote_geometry",
            baseline_bid=bid,
            baseline_ask=ask,
        )

    profiles: list[ExternalEdgeProfile] = []
    for profile, omitted in PROFILE_OMISSIONS:
        rows = [
            row
            for venue, row in sorted(state.venues.items())
            if venue != omitted
        ]
        if len(rows) < 2:
            return _invalid_projection(
                f"unsupported_profile:{profile}",
                baseline_bid=bid,
                baseline_ask=ask,
                profiles=tuple(profiles),
            )
        fair = weighted_median((row.fair_price, row.weight) for row in rows)
        if not math.isfinite(fair) or fair <= 0.0:
            return _invalid_projection(
                f"unsupported_profile:{profile}",
                baseline_bid=bid,
                baseline_ask=ask,
                profiles=tuple(profiles),
            )
        lead_bps = math.log(fair / state.local_mid) * 10_000.0
        tick_bps = tick / state.local_mid * 10_000.0
        lower = min(
            row.fair_price
            * math.exp(
                -(
                    math.sqrt(max(0.0, row.tracking_variance_bps2))
                    + tick_bps
                )
                / 10_000.0
            )
            for row in rows
        )
        upper = max(
            row.fair_price
            * math.exp(
                (
                    math.sqrt(max(0.0, row.tracking_variance_bps2))
                    + tick_bps
                )
                / 10_000.0
            )
            for row in rows
        )
        adverse_side = "SELL" if lead_bps > 0.0 else "BUY" if lead_bps < 0.0 else ""
        profiles.append(
            ExternalEdgeProfile(
                profile=profile,
                adverse_side=adverse_side,
                buy_requested_ticks=max(
                    0,
                    int(math.ceil((bid - lower) / tick - 1e-12)),
                ),
                sell_requested_ticks=max(
                    0,
                    int(math.ceil((upper - ask) / tick - 1e-12)),
                ),
            )
        )

    directions = {row.adverse_side for row in profiles}
    if len(directions) != 1 or "" in directions:
        return _invalid_projection(
            "loo_direction_disagreement",
            baseline_bid=bid,
            baseline_ask=ask,
            profiles=tuple(profiles),
        )
    adverse_side = next(iter(directions))
    requested = max(
        row.buy_requested_ticks
        if adverse_side == "BUY"
        else row.sell_requested_ticks
        for row in profiles
    )
    if requested <= 0:
        return _invalid_projection(
            "conservative_edge_nonnegative",
            baseline_bid=bid,
            baseline_ask=ask,
            profiles=tuple(profiles),
        )

    max_pair_spread = state.local_mid * float(max_pair_spread_bps) / 10_000.0
    if adverse_side == "BUY":
        room = max(
            0,
            int(math.floor((bid - (ask - max_pair_spread)) / tick + 1e-12)),
        )
        effective = min(requested, room)
        candidate_bid = bid - effective * tick
        candidate_ask = ask
    else:
        room = max(
            0,
            int(math.floor(((bid + max_pair_spread) - ask) / tick + 1e-12)),
        )
        effective = min(requested, room)
        candidate_bid = bid
        candidate_ask = ask + effective * tick
    return ExternalAdverseQuoteEdgeProjection(
        valid=True,
        reason="valid" if effective > 0 else "spread_cap_no_room",
        adverse_side=adverse_side,
        requested_ticks=int(requested),
        effective_ticks=int(effective),
        candidate_bid=float(candidate_bid),
        candidate_ask=float(candidate_ask),
        cap_clipped=bool(effective < requested),
        loo_consistent=True,
        profiles=tuple(profiles),
    )
