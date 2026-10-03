"""Causal cross-venue fair-price state and quote-pair shadow projection.

This module is evidence-only.  It never submits, cancels, or replaces an order.
The estimator consumes only observations whose feature-ready timestamp is at or
before the decision clock.  Basis, lead variance, and measurement-noise state
are read before the current observation is committed, preserving a strict
past-only online contract.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

FAIR_PRICE_SCHEMA_VERSION = "cross_venue_causal_fair_price.v1"
FAIR_PRICE_VENUES = ("bitget", "bybit", "okx")
FAIR_PRICE_MARKETS = ("spot", "perp")


def _finite(value: float) -> bool:
    return math.isfinite(float(value))


@dataclass(frozen=True)
class FairPriceSource:
    venue: str
    market_type: str
    bid: float
    ask: float
    exchange_ts_ns: int
    local_receive_ts_ns: int
    feature_ready_ts_ns: int
    valid: bool = True
    mid_override: float = math.nan
    source_kind: str = "receive_time_bbo"
    transport_supported: bool = True

    @property
    def mid(self) -> float:
        override = float(self.mid_override)
        if _finite(override) and override > 0.0:
            return override
        bid = float(self.bid)
        ask = float(self.ask)
        return 0.5 * (bid + ask) if bid > 0.0 and ask > bid else math.nan


@dataclass(frozen=True)
class CrossVenueFairPriceConfig:
    venues: tuple[str, ...] = FAIR_PRICE_VENUES
    market_types: tuple[str, ...] = FAIR_PRICE_MARKETS
    minimum_valid_venues: int = 2
    max_source_age_ms: float = 2_000.0
    max_anchor_age_ms: float = 30_000.0
    max_dispersion_bps: float = 2.0
    maximum_abs_basis_bps: float = 100.0
    basis_half_life_s: float = 360.0
    variance_half_life_s: float = 360.0
    minimum_basis_samples: int = 30
    minimum_gain_samples: int = 30
    variance_floor_bps2: float = 1e-6

    def __post_init__(self) -> None:
        if len(set(self.venues)) != len(self.venues) or len(self.venues) < 2:
            raise ValueError("fair-price venues must contain at least two unique values")
        if len(set(self.market_types)) != len(self.market_types):
            raise ValueError("fair-price market types must be unique")
        if not 2 <= int(self.minimum_valid_venues) <= len(self.venues):
            raise ValueError("minimum_valid_venues is outside the venue set")
        for name in (
            "max_source_age_ms",
            "max_anchor_age_ms",
            "max_dispersion_bps",
            "maximum_abs_basis_bps",
            "basis_half_life_s",
            "variance_half_life_s",
            "variance_floor_bps2",
        ):
            if float(getattr(self, name)) <= 0.0:
                raise ValueError(f"{name} must be positive")
        if int(self.minimum_basis_samples) < 2 or int(self.minimum_gain_samples) < 2:
            raise ValueError("fair-price warmup support must be at least two samples")


@dataclass(frozen=True)
class VenueFairPriceState:
    venue: str
    fair_price: float
    weight: float
    source_count: int
    max_source_age_ms: float
    max_feed_latency_ms: float
    max_feature_latency_ms: float
    tracking_variance_bps2: float
    minimum_basis_samples: int
    source_kinds: tuple[str, ...]
    transport_supported: bool


@dataclass(frozen=True)
class CrossVenueFairPriceState:
    schema_version: str
    decision_ts_ns: int
    valid: bool
    reason: str
    local_mid: float
    fair_price: float
    raw_lead_bps: float
    gain: float
    center_shift_price: float
    center_shift_bps: float
    confidence: float
    dispersion_bps: float
    valid_venues: int
    venue_ids: tuple[str, ...]
    minimum_basis_samples: int
    lead_variance_bps2: float
    noise_variance_bps2: float
    max_source_age_ms: float
    max_feed_latency_ms: float
    max_feature_latency_ms: float
    source_kinds: tuple[str, ...]
    transport_supported: bool
    venues: Mapping[str, VenueFairPriceState] = field(default_factory=dict)


@dataclass(frozen=True)
class FairPriceQuoteShadow:
    valid: bool
    reason: str
    baseline_bid: float
    baseline_ask: float
    candidate_bid: float
    candidate_ask: float
    requested_shift_ticks: int
    effective_shift_ticks: int
    gtx_clamped: bool
    pair_spread_preserved: bool


@dataclass
class _EwMoments:
    half_life_s: float
    count: int = 0
    mean: float = 0.0
    variance: float = 0.0
    last_ts_ns: int = 0

    def update(self, value: float, ts_ns: int) -> None:
        sample = float(value)
        clock = int(ts_ns)
        if not _finite(sample) or clock <= 0:
            raise ValueError("EW moments require a finite value and positive timestamp")
        if self.count == 0:
            self.count = 1
            self.mean = sample
            self.variance = 0.0
            self.last_ts_ns = clock
            return
        if clock < self.last_ts_ns:
            raise ValueError("EW moment timestamp regressed")
        elapsed_s = max(1e-6, (clock - self.last_ts_ns) / 1_000_000_000.0)
        alpha = 1.0 - math.exp(-math.log(2.0) * elapsed_s / self.half_life_s)
        delta = sample - self.mean
        new_mean = self.mean + alpha * delta
        self.variance = max(
            0.0,
            (1.0 - alpha) * (self.variance + alpha * delta * delta),
        )
        self.mean = new_mean
        self.count += 1
        self.last_ts_ns = clock


def _weighted_mean(rows: Iterable[tuple[float, float]]) -> float:
    clean = [
        (float(value), float(weight))
        for value, weight in rows
        if _finite(value) and _finite(weight) and weight > 0.0
    ]
    total = sum(weight for _, weight in clean)
    return sum(value * weight for value, weight in clean) / total if total > 0.0 else math.nan


def weighted_median(rows: Iterable[tuple[float, float]]) -> float:
    clean = sorted(
        (
            (float(value), float(weight))
            for value, weight in rows
            if _finite(value) and _finite(weight) and weight > 0.0
        ),
        key=lambda row: row[0],
    )
    total = sum(weight for _, weight in clean)
    if total <= 0.0:
        return math.nan
    threshold = 0.5 * total
    cumulative = 0.0
    for index, (value, weight) in enumerate(clean):
        cumulative += weight
        if math.isclose(cumulative, threshold, rel_tol=0.0, abs_tol=1e-15):
            if index + 1 < len(clean):
                return 0.5 * (value + clean[index + 1][0])
            return value
        if cumulative > threshold:
            return value
    return clean[-1][0]


class CrossVenueFairPriceEstimator:
    """Past-only fair-price estimator shared by live shadow and replay."""

    def __init__(self, config: CrossVenueFairPriceConfig | None = None) -> None:
        self.config = config or CrossVenueFairPriceConfig()
        self._basis: dict[tuple[str, str], _EwMoments] = {}
        self._last_source_identity: dict[tuple[str, str], int] = {}
        self._lead = _EwMoments(self.config.variance_half_life_s)
        self._noise = _EwMoments(self.config.variance_half_life_s)
        self._last_consensus_identity: tuple[int, ...] | None = None
        self._last_decision_ts_ns = 0

    def _invalid(
        self,
        *,
        decision_ts_ns: int,
        local_mid: float,
        reason: str,
        venue_states: Mapping[str, VenueFairPriceState] | None = None,
    ) -> CrossVenueFairPriceState:
        rows = dict(venue_states or {})
        return CrossVenueFairPriceState(
            schema_version=FAIR_PRICE_SCHEMA_VERSION,
            decision_ts_ns=int(decision_ts_ns),
            valid=False,
            reason=str(reason),
            local_mid=float(local_mid),
            fair_price=math.nan,
            raw_lead_bps=math.nan,
            gain=0.0,
            center_shift_price=0.0,
            center_shift_bps=0.0,
            confidence=0.0,
            dispersion_bps=math.nan,
            valid_venues=len(rows),
            venue_ids=tuple(sorted(rows)),
            minimum_basis_samples=min(
                (row.minimum_basis_samples for row in rows.values()),
                default=0,
            ),
            lead_variance_bps2=float(self._lead.variance),
            noise_variance_bps2=float(self._noise.mean if self._noise.count else 0.0),
            max_source_age_ms=max(
                (row.max_source_age_ms for row in rows.values()),
                default=math.inf,
            ),
            max_feed_latency_ms=max(
                (row.max_feed_latency_ms for row in rows.values()),
                default=math.inf,
            ),
            max_feature_latency_ms=max(
                (row.max_feature_latency_ms for row in rows.values()),
                default=math.inf,
            ),
            source_kinds=tuple(
                sorted({kind for row in rows.values() for kind in row.source_kinds})
            ),
            transport_supported=bool(rows)
            and all(row.transport_supported for row in rows.values()),
            venues=rows,
        )

    def observe(
        self,
        *,
        decision_ts_ns: int,
        local_mid: float,
        stablecoin_mid: float,
        stablecoin_feature_ready_ts_ns: int,
        sources: Iterable[FairPriceSource],
    ) -> CrossVenueFairPriceState:
        cfg = self.config
        decision_ns = int(decision_ts_ns)
        if decision_ns <= 0 or decision_ns < self._last_decision_ts_ns:
            raise ValueError("fair-price decision clock must be positive and monotonic")
        self._last_decision_ts_ns = decision_ns
        local = float(local_mid)
        anchor = float(stablecoin_mid)
        anchor_ready = int(stablecoin_feature_ready_ts_ns)
        if not (_finite(local) and local > 0.0):
            return self._invalid(
                decision_ts_ns=decision_ns,
                local_mid=local,
                reason="invalid_local_mid",
            )
        if not (_finite(anchor) and anchor > 0.0 and anchor_ready > 0):
            return self._invalid(
                decision_ts_ns=decision_ns,
                local_mid=local,
                reason="invalid_stablecoin_anchor",
            )
        anchor_age_ms = (decision_ns - anchor_ready) / 1_000_000.0
        if anchor_age_ms < 0.0 or anchor_age_ms > cfg.max_anchor_age_ms:
            return self._invalid(
                decision_ts_ns=decision_ns,
                local_mid=local,
                reason="stale_or_future_stablecoin_anchor",
            )

        grouped: dict[str, list[FairPriceSource]] = {
            venue: [] for venue in cfg.venues
        }
        pending_updates: list[tuple[tuple[str, str], float, int]] = []
        source_identities: dict[tuple[str, str], int] = {}
        for source in sources:
            venue = str(source.venue).lower()
            market = str(source.market_type).lower()
            if venue not in grouped or market not in cfg.market_types:
                continue
            grouped[venue].append(source)

        venue_states: dict[str, VenueFairPriceState] = {}
        basis_warmup_pending = False
        for venue in cfg.venues:
            adjusted_rows: list[tuple[float, float]] = []
            ages: list[float] = []
            feed_latencies: list[float] = []
            feature_latencies: list[float] = []
            variances: list[float] = []
            basis_counts: list[int] = []
            source_kinds: list[str] = []
            transport_support: list[bool] = []
            for source in grouped[venue]:
                ready_ns = int(source.feature_ready_ts_ns)
                receive_ns = int(source.local_receive_ts_ns)
                exchange_ns = int(source.exchange_ts_ns)
                source_age_ms = (decision_ns - ready_ns) / 1_000_000.0
                feed_latency_ms = max(
                    0.0, (receive_ns - exchange_ns) / 1_000_000.0
                )
                feature_latency_ms = (ready_ns - receive_ns) / 1_000_000.0
                if (
                    not source.valid
                    or ready_ns <= 0
                    or receive_ns <= 0
                    or ready_ns > decision_ns
                    or exchange_ns <= 0
                    or exchange_ns > receive_ns
                    or receive_ns > ready_ns
                    or source_age_ms < 0.0
                    or source_age_ms > cfg.max_source_age_ms
                    or feature_latency_ms < 0.0
                    or not (_finite(source.mid) and source.mid > 0.0)
                ):
                    continue
                converted_mid = source.mid / anchor
                raw_basis_bps = math.log(converted_mid / local) * 10_000.0
                if (
                    not _finite(raw_basis_bps)
                    or abs(raw_basis_bps) > cfg.maximum_abs_basis_bps
                ):
                    continue
                key = (venue, str(source.market_type).lower())
                tracker = self._basis.setdefault(
                    key, _EwMoments(cfg.basis_half_life_s)
                )
                source_identity = ready_ns
                source_identities[key] = source_identity
                if self._last_source_identity.get(key) != source_identity:
                    pending_updates.append((key, raw_basis_bps, decision_ns))
                if tracker.count < cfg.minimum_basis_samples:
                    basis_warmup_pending = True
                    continue
                basis_counts.append(int(tracker.count))
                adjusted_mid = converted_mid * math.exp(-tracker.mean / 10_000.0)
                tracking_variance = max(
                    cfg.variance_floor_bps2, float(tracker.variance)
                )
                freshness_weight = math.exp(
                    -source_age_ms / max(1.0, cfg.max_source_age_ms)
                )
                feature_weight = math.exp(
                    -feature_latency_ms / max(1.0, cfg.max_source_age_ms)
                )
                feed_weight = math.exp(
                    -feed_latency_ms / max(1.0, cfg.max_source_age_ms)
                )
                weight = (
                    freshness_weight
                    * feed_weight
                    * feature_weight
                    / math.sqrt(tracking_variance)
                )
                adjusted_rows.append((adjusted_mid, weight))
                ages.append(source_age_ms)
                feed_latencies.append(feed_latency_ms)
                feature_latencies.append(feature_latency_ms)
                variances.append(tracking_variance)
                source_kinds.append(str(source.source_kind or "unknown"))
                transport_support.append(bool(source.transport_supported))

            if adjusted_rows:
                venue_price = _weighted_mean(adjusted_rows)
                venue_weight = sum(weight for _, weight in adjusted_rows)
                venue_states[venue] = VenueFairPriceState(
                    venue=venue,
                    fair_price=venue_price,
                    weight=venue_weight,
                    source_count=len(adjusted_rows),
                    max_source_age_ms=max(ages),
                    max_feed_latency_ms=max(feed_latencies),
                    max_feature_latency_ms=max(feature_latencies),
                    tracking_variance_bps2=_weighted_mean(
                        zip(
                            variances,
                            (weight for _, weight in adjusted_rows),
                            strict=True,
                        )
                    ),
                    minimum_basis_samples=min(basis_counts, default=0),
                    source_kinds=tuple(sorted(set(source_kinds))),
                    transport_supported=bool(transport_support)
                    and all(transport_support),
                )

        # Current observations become basis history only after current output
        # features have been formed from the prior tracker state.
        for key, value, clock in pending_updates:
            self._basis[key].update(value, clock)
            self._last_source_identity[key] = source_identities[key]

        if len(venue_states) < cfg.minimum_valid_venues:
            return self._invalid(
                decision_ts_ns=decision_ns,
                local_mid=local,
                reason=(
                    "basis_warmup"
                    if basis_warmup_pending
                    else "insufficient_valid_venues"
                ),
                venue_states=venue_states,
            )

        fair = weighted_median(
            (row.fair_price, row.weight) for row in venue_states.values()
        )
        venue_leads = [
            math.log(row.fair_price / local) * 10_000.0
            for row in venue_states.values()
        ]
        dispersion = max(venue_leads) - min(venue_leads)
        raw_lead_bps = math.log(fair / local) * 10_000.0
        total_weight = sum(row.weight for row in venue_states.values())
        current_noise = (
            sum(
                row.weight
                * (math.log(row.fair_price / fair) * 10_000.0) ** 2
                for row in venue_states.values()
            )
            / max(total_weight, 1e-12)
        )
        lead_variance = float(self._lead.variance)
        noise_variance = float(
            self._noise.mean if self._noise.count else current_noise
        )
        support_ready = (
            self._lead.count >= cfg.minimum_gain_samples
            and self._noise.count >= cfg.minimum_gain_samples
        )
        denominator = lead_variance + noise_variance
        gain = (
            max(0.0, min(1.0, lead_variance / denominator))
            if support_ready and denominator > cfg.variance_floor_bps2
            else 0.0
        )
        center_shift_price = gain * (fair - local)
        center_shift_bps = center_shift_price / local * 10_000.0
        max_age = max(row.max_source_age_ms for row in venue_states.values())
        max_feed_latency = max(
            row.max_feed_latency_ms for row in venue_states.values()
        )
        max_feature_latency = max(
            row.max_feature_latency_ms for row in venue_states.values()
        )
        coverage = len(venue_states) / len(cfg.venues)
        freshness_confidence = math.exp(
            -max_age / max(1.0, cfg.max_source_age_ms)
        )
        dispersion_confidence = math.exp(
            -dispersion / max(cfg.max_dispersion_bps, 1e-12)
        )
        confidence = max(
            0.0,
            min(
                1.0,
                coverage
                * freshness_confidence
                * dispersion_confidence
                * math.sqrt(max(gain, 0.0)),
            ),
        )

        consensus_identity = tuple(
            source_identities[key]
            for key in sorted(source_identities)
        )
        if consensus_identity and consensus_identity != self._last_consensus_identity:
            self._lead.update(raw_lead_bps, decision_ns)
            self._noise.update(current_noise, decision_ns)
            self._last_consensus_identity = consensus_identity

        if dispersion > cfg.max_dispersion_bps:
            reason = "venue_dispersion"
            valid = False
        elif not support_ready:
            reason = "gain_warmup"
            valid = False
        else:
            reason = "valid"
            valid = True
        return CrossVenueFairPriceState(
            schema_version=FAIR_PRICE_SCHEMA_VERSION,
            decision_ts_ns=decision_ns,
            valid=valid,
            reason=reason,
            local_mid=local,
            fair_price=fair,
            raw_lead_bps=raw_lead_bps,
            gain=gain if valid else 0.0,
            center_shift_price=center_shift_price if valid else 0.0,
            center_shift_bps=center_shift_bps if valid else 0.0,
            confidence=confidence if valid else 0.0,
            dispersion_bps=dispersion,
            valid_venues=len(venue_states),
            venue_ids=tuple(sorted(venue_states)),
            minimum_basis_samples=min(
                row.minimum_basis_samples for row in venue_states.values()
            ),
            lead_variance_bps2=lead_variance,
            noise_variance_bps2=noise_variance,
            max_source_age_ms=max_age,
            max_feed_latency_ms=max_feed_latency,
            max_feature_latency_ms=max_feature_latency,
            source_kinds=tuple(
                sorted(
                    {
                        kind
                        for row in venue_states.values()
                        for kind in row.source_kinds
                    }
                )
            ),
            transport_supported=all(
                row.transport_supported for row in venue_states.values()
            ),
            venues=venue_states,
        )


def project_fair_center_shadow(
    state: CrossVenueFairPriceState,
    *,
    baseline_bid: float,
    baseline_ask: float,
    best_bid: float,
    best_ask: float,
    tick_size: float,
) -> FairPriceQuoteShadow:
    """Move a whole quote pair while preserving spread and GTX passivity."""

    bid = float(baseline_bid)
    ask = float(baseline_ask)
    bbo_bid = float(best_bid)
    bbo_ask = float(best_ask)
    tick = float(tick_size)

    def fallback(reason: str) -> FairPriceQuoteShadow:
        return FairPriceQuoteShadow(
            valid=False,
            reason=reason,
            baseline_bid=bid,
            baseline_ask=ask,
            candidate_bid=bid,
            candidate_ask=ask,
            requested_shift_ticks=0,
            effective_shift_ticks=0,
            gtx_clamped=False,
            pair_spread_preserved=True,
        )

    if not state.valid:
        return fallback(state.reason)
    if not (
        tick > 0.0
        and bid > 0.0
        and ask > bid
        and bbo_bid > 0.0
        and bbo_ask > bbo_bid
    ):
        return fallback("invalid_quote_geometry")

    requested_ticks = int(round(float(state.center_shift_price) / tick))
    minimum_shift = bbo_bid + tick - ask
    maximum_shift = bbo_ask - tick - bid
    minimum_ticks = math.ceil(minimum_shift / tick - 1e-9)
    maximum_ticks = math.floor(maximum_shift / tick + 1e-9)
    if minimum_ticks > maximum_ticks:
        return fallback("no_pair_preserving_gtx_support")
    effective_ticks = max(minimum_ticks, min(maximum_ticks, requested_ticks))
    candidate_bid = round((bid + effective_ticks * tick) / tick) * tick
    candidate_ask = round((ask + effective_ticks * tick) / tick) * tick
    spread_preserved = math.isclose(
        candidate_ask - candidate_bid,
        ask - bid,
        rel_tol=0.0,
        abs_tol=tick * 1e-6,
    )
    if not spread_preserved:
        return fallback("pair_spread_rounding_failure")
    if candidate_bid >= bbo_ask or candidate_ask <= bbo_bid:
        return fallback("gtx_projection_failure")
    return FairPriceQuoteShadow(
        valid=True,
        reason="valid",
        baseline_bid=bid,
        baseline_ask=ask,
        candidate_bid=candidate_bid,
        candidate_ask=candidate_ask,
        requested_shift_ticks=requested_ticks,
        effective_shift_ticks=effective_ticks,
        gtx_clamped=effective_ticks != requested_ticks,
        pair_spread_preserved=True,
    )
