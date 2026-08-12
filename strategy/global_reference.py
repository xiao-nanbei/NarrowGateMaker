"""Causal hierarchical global-reference state for shadow research.

The execution market is Binance BTCUSDC perpetual.  Independent external spot
and perpetual markets estimate two common *innovations*.  Binance BTCUSDT is a
local level bridge, while USDCUSDT or BTCUSDC spot supplies target-currency
conversion.  This module deliberately produces state only; it does not alter
quotes, size, inventory limits, or order lifecycle.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median
from typing import Mapping, Optional


@dataclass(frozen=True)
class ReferenceObservation:
    mid: float
    prior_mid: float
    source_age_ms: float
    valid: bool = True

    @property
    def move_bps(self) -> float:
        if not self.valid or self.mid <= 0.0 or self.prior_mid <= 0.0:
            return math.nan
        return math.log(self.mid / self.prior_mid) * 10_000.0


@dataclass(frozen=True)
class GlobalReferenceState:
    ref_px_usdc: float
    local_bridge_px_usdc: float
    residual_bps: float
    global_spot_move_bps: float
    global_perp_move_bps: float
    perp_spot_divergence_bps: float
    cross_venue_dispersion_bps: float
    fresh_spot_venues: int
    fresh_perp_venues: int
    consensus_direction: int
    confidence: float
    leader_venue: Optional[str]
    max_source_age_ms: float
    valid: bool
    external_correction_bps: float = 0.0
    bridge_basis_bps: float = 0.0
    bridge_source: str = ""
    bridge_basis_sample_count: int = 0
    validity_reason: str = ""


@dataclass(frozen=True)
class _FactorState:
    move_bps: float
    dispersion_bps: float
    fresh_count: int
    agreement: float
    max_age_ms: float
    venue_moves: Mapping[str, float]


def _finite(value: float) -> bool:
    return math.isfinite(float(value))


def _factor(
    observations: Mapping[str, ReferenceObservation],
    *,
    max_source_age_ms: float,
) -> _FactorState:
    venue_moves: dict[str, float] = {}
    ages: list[float] = []
    for venue, observation in observations.items():
        move = observation.move_bps
        if (
            observation.valid
            and _finite(move)
            and 0.0 <= observation.source_age_ms <= max_source_age_ms
        ):
            venue_moves[str(venue).lower()] = move
            ages.append(float(observation.source_age_ms))
    values = list(venue_moves.values())
    if len(values) < 2:
        return _FactorState(math.nan, math.nan, len(values), 0.0, max(ages, default=math.inf), venue_moves)
    # With two sources no outlier can be identified, so use the mean and let
    # confidence carry the lower information quality.  Three sources use the
    # median, making one venue unable to dominate the common innovation.
    common = median(values) if len(values) >= 3 else sum(values) / len(values)
    dispersion = max(values) - min(values)
    direction = 1 if common > 0.0 else -1 if common < 0.0 else 0
    if direction == 0:
        agreement = 1.0 if all(abs(value) < 1e-12 for value in values) else 0.0
    else:
        agreement = sum(
            1 for value in values if (1 if value > 0.0 else -1 if value < 0.0 else 0) == direction
        ) / len(values)
    return _FactorState(common, dispersion, len(values), agreement, max(ages), venue_moves)


def _leader(
    spot: _FactorState,
    perp: _FactorState,
    consensus_direction: int,
) -> Optional[str]:
    if consensus_direction == 0:
        return None
    combined: dict[str, list[float]] = {}
    for venue, value in spot.venue_moves.items():
        combined.setdefault(venue, []).append(value)
    for venue, value in perp.venue_moves.items():
        combined.setdefault(venue, []).append(value)
    candidates = []
    for venue, values in combined.items():
        value = sum(values) / len(values)
        direction = 1 if value > 0.0 else -1 if value < 0.0 else 0
        if direction == consensus_direction:
            candidates.append((abs(value), venue))
    return max(candidates)[1] if candidates else None


def build_global_reference_state(
    *,
    external_spot: Mapping[str, ReferenceObservation],
    external_perp: Mapping[str, ReferenceObservation],
    binance_btcusdt_perp: ReferenceObservation,
    execution_btcusdc_perp_mid: float,
    slow_bridge_basis_bps: float,
    usdcusdt_mid: float = math.nan,
    binance_btcusdc_spot_mid: float = math.nan,
    max_source_age_ms: float = 2_000.0,
    max_dispersion_bps: float = 2.0,
    min_consensus_move_bps: float = 0.05,
    correction_beta: float = 1.0,
    correction_cap_bps: float = 0.02,
    bridge_basis_sample_count: int = 0,
) -> GlobalReferenceState:
    """Build a strict 2-of-3 spot/perp reference without policy side effects."""
    spot = _factor(external_spot, max_source_age_ms=max_source_age_ms)
    perp = _factor(external_perp, max_source_age_ms=max_source_age_ms)
    binance_move = binance_btcusdt_perp.move_bps

    bridge_source = ""
    local_bridge = math.nan
    if _finite(usdcusdt_mid) and usdcusdt_mid > 0.0 and binance_btcusdt_perp.mid > 0.0:
        # USDCUSDT is USDT per USDC, so BTCUSDT / USDCUSDT is BTCUSDC.
        local_bridge = binance_btcusdt_perp.mid / usdcusdt_mid
        bridge_source = "binance_btcusdt_perp/usdcusdt"
    elif _finite(binance_btcusdc_spot_mid) and binance_btcusdc_spot_mid > 0.0:
        local_bridge = binance_btcusdc_spot_mid
        bridge_source = "binance_btcusdc_spot"

    moves_valid = (
        spot.fresh_count >= 2
        and perp.fresh_count >= 2
        and _finite(spot.move_bps)
        and _finite(perp.move_bps)
        and _finite(binance_move)
    )
    spot_direction = 1 if spot.move_bps > 0.0 else -1 if spot.move_bps < 0.0 else 0
    perp_direction = 1 if perp.move_bps > 0.0 else -1 if perp.move_bps < 0.0 else 0
    directions_confirm = (
        spot_direction == perp_direction
        or (
            abs(spot.move_bps) < min_consensus_move_bps
            and abs(perp.move_bps) < min_consensus_move_bps
        )
    ) if moves_valid else False
    dispersion = max(spot.dispersion_bps, perp.dispersion_bps) if moves_valid else math.nan
    dispersion_ok = _finite(dispersion) and dispersion <= max_dispersion_bps
    bridge_valid = _finite(local_bridge) and local_bridge > 0.0
    execution_valid = _finite(execution_btcusdc_perp_mid) and execution_btcusdc_perp_mid > 0.0
    basis_valid = _finite(slow_bridge_basis_bps)
    valid = bool(
        moves_valid
        and directions_confirm
        and dispersion_ok
        and bridge_valid
        and execution_valid
        and basis_valid
    )
    if valid:
        validity_reason = "valid"
    elif not moves_valid:
        validity_reason = "source"
    elif not directions_confirm:
        validity_reason = "direction"
    elif not dispersion_ok:
        validity_reason = "dispersion"
    elif not bridge_valid:
        validity_reason = "bridge"
    elif not execution_valid:
        validity_reason = "execution"
    else:
        validity_reason = "basis_warmup"

    global_move = (
        0.5 * (spot.move_bps + perp.move_bps) if valid else 0.0
    )
    consensus_direction = (
        1 if global_move >= min_consensus_move_bps
        else -1 if global_move <= -min_consensus_move_bps
        else 0
    )
    max_age = max(spot.max_age_ms, perp.max_age_ms, binance_btcusdt_perp.source_age_ms)
    if valid:
        source_count_confidence = min(spot.fresh_count, perp.fresh_count) / 3.0
        agreement = min(spot.agreement, perp.agreement)
        freshness = math.exp(-max(0.0, max_age) / max(1.0, max_source_age_ms))
        dispersion_confidence = math.exp(-dispersion / max(1e-9, max_dispersion_bps))
        confidence = max(0.0, min(1.0, source_count_confidence * agreement * freshness * dispersion_confidence))
        unabsorbed = global_move - binance_move
        raw_correction = correction_beta * unabsorbed * confidence
        correction = max(-abs(correction_cap_bps), min(abs(correction_cap_bps), raw_correction))
    else:
        confidence = 0.0
        correction = 0.0

    basis = float(slow_bridge_basis_bps) if basis_valid else 0.0
    ref_px = (
        local_bridge * math.exp((basis + correction) / 10_000.0)
        if bridge_valid else math.nan
    )
    residual = (
        math.log(ref_px / execution_btcusdc_perp_mid) * 10_000.0
        if _finite(ref_px) and execution_valid else math.nan
    )
    return GlobalReferenceState(
        ref_px_usdc=ref_px,
        local_bridge_px_usdc=local_bridge,
        residual_bps=residual,
        global_spot_move_bps=spot.move_bps,
        global_perp_move_bps=perp.move_bps,
        perp_spot_divergence_bps=(perp.move_bps - spot.move_bps if moves_valid else math.nan),
        cross_venue_dispersion_bps=dispersion,
        fresh_spot_venues=spot.fresh_count,
        fresh_perp_venues=perp.fresh_count,
        consensus_direction=consensus_direction,
        confidence=confidence,
        leader_venue=_leader(spot, perp, consensus_direction) if valid else None,
        max_source_age_ms=max_age,
        valid=valid,
        external_correction_bps=correction,
        bridge_basis_bps=basis,
        bridge_source=bridge_source,
        bridge_basis_sample_count=max(0, int(bridge_basis_sample_count)),
        validity_reason=validity_reason,
    )
