"""Shared quote-policy guards for live and tick replay."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

POLICY_REASON_FILL_COOLDOWN = 1 << 0
POLICY_REASON_MARKOUT = 1 << 2
POLICY_REASON_STALE_WARN = 1 << 3
POLICY_REASON_STALE_HARD = 1 << 4
POLICY_REASON_BURST = 1 << 5
POLICY_REASON_THIN_DEPTH = 1 << 6
POLICY_REASON_INV_LIMIT = 1 << 7
POLICY_REASON_EXPOSURE_ONLY = 1 << 8
POLICY_REASON_ADVERSE = 1 << 9
POLICY_REASON_FLAT_TTL = 1 << 11
POLICY_REASON_DEFENSE = 1 << 12
POLICY_REASON_SYNC_DEGRADED = 1 << 13
POLICY_REASON_BUY_FILL_SELECTION = 1 << 15
POLICY_REASON_SPREAD_CAP = 1 << 16
POLICY_REASON_BUY_HAZARD_CANCEL = 1 << 17


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


@dataclass(frozen=True)
class AdaptiveAddCooldownConfig:
    """State-dependent multiplier for exposure-increasing fill cooldown."""

    enabled: bool = False
    min_mult: float = 0.5
    max_mult: float = 2.5
    w_markout: float = 0.0
    w_flow: float = 0.0
    w_campaign: float = 0.0
    w_trend: float = 0.0
    w_refill_weak: float = 0.0
    w_refill_good: float = 0.0
    w_reversion: float = 0.0
    mo_ref: float = 50.0
    flow_ref: float = 2.0
    campaign_inv_ref: float = 0.006
    campaign_age_ref_s: float = 3600.0
    trend_ret_ref: float = 2e-5
    refill_ref: float = 0.10
    reversion_ref: float = 1.0
    gate_enabled: bool = False
    gate_mult: float = 1.75
    gate_campaign_score: float = 1.0
    gate_trend_score: float = 1.0
    gate_refill_edge_max: float = 0.0
    gate_reversion_max: float = 0.5
    gate_side: str = "BOTH"


@dataclass(frozen=True)
class CampaignSoftGateConfig:
    """Quote-time gate for campaign soft-widen research arms."""

    enabled: bool = False
    campaign_inv_ref: float = 0.006
    campaign_age_ref_s: float = 3600.0
    trend_ret_ref: float = 2e-5
    refill_ref: float = 0.10
    gate_campaign_score: float = 1.0
    gate_trend_score: float = 1.0
    gate_refill_edge_max: float = 0.0
    gate_reversion_max: float = 0.5
    gate_side: str = "BOTH"


@dataclass(frozen=True)
class CampaignSoftGateResult:
    active: bool
    campaign_score: float
    trend_score: float
    repair_weak: bool
    side_adverse_ret: float
    refill_edge: float
    micro_reversion_score: float


def adaptive_add_cooldown_multiplier(
    *,
    side: str = "",
    side_markout_ema: float,
    consec_units: float,
    prev_inventory: float,
    max_inventory: float,
    campaign_age_s: float,
    side_adverse_ret: float,
    refill_edge: float,
    micro_reversion_score: float,
    cfg: AdaptiveAddCooldownConfig,
) -> float:
    """Return the bounded add-side cooldown multiplier shared by live/replay."""
    if not cfg.enabled:
        return 1.0

    neg_markout = _clamp(
        max(0.0, -float(side_markout_ema)) / max(cfg.mo_ref, 1e-6), 0.0, 1.0
    )
    flow = _clamp(
        max(0.0, float(consec_units) - 1.0) / max(cfg.flow_ref, 1e-6), 0.0, 1.0
    )
    inv_ref = cfg.campaign_inv_ref if cfg.campaign_inv_ref > 0.0 else max_inventory
    inv_risk = _clamp(
        abs(float(prev_inventory)) / max(inv_ref, 1e-12), 0.0, 1.0
    )
    age_risk = _clamp(
        float(campaign_age_s) / max(cfg.campaign_age_ref_s, 1e-6), 0.0, 1.0
    )
    campaign = max(inv_risk, age_risk)
    trend = _clamp(
        max(0.0, float(side_adverse_ret)) / max(cfg.trend_ret_ref, 1e-12),
        0.0,
        1.0,
    )
    weak_refill = _clamp(
        max(0.0, -float(refill_edge)) / max(cfg.refill_ref, 1e-12), 0.0, 1.0
    )
    good_refill = _clamp(
        max(0.0, float(refill_edge)) / max(cfg.refill_ref, 1e-12), 0.0, 1.0
    )
    reversion = _clamp(
        max(0.0, float(micro_reversion_score)) / max(cfg.reversion_ref, 1e-12),
        0.0,
        1.0,
    )

    if cfg.gate_enabled:
        gate_side = str(cfg.gate_side or "BOTH").upper()
        if gate_side in {"BUY", "SELL"} and str(side or "").upper() != gate_side:
            return 1.0
        campaign_hit = campaign >= max(0.0, float(cfg.gate_campaign_score))
        trend_hit = trend >= max(0.0, float(cfg.gate_trend_score))
        repair_weak = (
            float(refill_edge) <= float(cfg.gate_refill_edge_max)
            and reversion <= max(0.0, float(cfg.gate_reversion_max))
        )
        if campaign_hit and trend_hit and repair_weak:
            return _clamp(float(cfg.gate_mult), cfg.min_mult, cfg.max_mult)
        return 1.0

    mult = (
        1.0
        + cfg.w_markout * neg_markout
        + cfg.w_flow * flow
        + cfg.w_campaign * campaign
        + cfg.w_trend * trend
        + cfg.w_refill_weak * weak_refill
        - cfg.w_refill_good * good_refill
        - cfg.w_reversion * reversion
    )
    return _clamp(mult, cfg.min_mult, cfg.max_mult)


def campaign_soft_gate_result(
    *,
    side: str = "",
    prev_inventory: float,
    max_inventory: float,
    campaign_age_s: float,
    side_adverse_ret: float,
    refill_edge: float,
    micro_reversion_score: float,
    cfg: CampaignSoftGateConfig,
) -> CampaignSoftGateResult:
    """Evaluate the campaign soft-widen gate from quote-time-visible state."""
    inv_ref = cfg.campaign_inv_ref if cfg.campaign_inv_ref > 0.0 else max_inventory
    campaign_score = max(
        _clamp(abs(float(prev_inventory)) / max(inv_ref, 1e-12), 0.0, 1.0),
        _clamp(
            float(campaign_age_s) / max(cfg.campaign_age_ref_s, 1e-6),
            0.0,
            1.0,
        ),
    )
    trend_score = _clamp(
        max(0.0, float(side_adverse_ret)) / max(cfg.trend_ret_ref, 1e-12),
        0.0,
        1.0,
    )
    reversion_score = _clamp(
        max(0.0, float(micro_reversion_score)), 0.0, 1.0
    )
    if not cfg.enabled:
        return CampaignSoftGateResult(
            active=True,
            campaign_score=campaign_score,
            trend_score=trend_score,
            repair_weak=True,
            side_adverse_ret=float(side_adverse_ret),
            refill_edge=float(refill_edge),
            micro_reversion_score=float(micro_reversion_score),
        )

    gate_side = str(cfg.gate_side or "BOTH").upper()
    side_name = str(side or "").upper()
    side_allowed = gate_side not in {"BUY", "SELL"} or side_name == gate_side
    repair_weak = (
        float(refill_edge) <= float(cfg.gate_refill_edge_max)
        and reversion_score <= max(0.0, float(cfg.gate_reversion_max))
    )
    return CampaignSoftGateResult(
        active=bool(
            side_allowed
            and campaign_score >= max(0.0, float(cfg.gate_campaign_score))
            and trend_score >= max(0.0, float(cfg.gate_trend_score))
            and repair_weak
        ),
        campaign_score=campaign_score,
        trend_score=trend_score,
        repair_weak=bool(repair_weak),
        side_adverse_ret=float(side_adverse_ret),
        refill_edge=float(refill_edge),
        micro_reversion_score=float(micro_reversion_score),
    )


def adaptive_add_cooldown_config_from_params(params) -> AdaptiveAddCooldownConfig:
    return AdaptiveAddCooldownConfig(
        enabled=bool(params.get("adaptive_add_cooldown_enabled", False)),
        min_mult=float(params.get("adaptive_add_cooldown_min_mult", 0.5)),
        max_mult=float(params.get("adaptive_add_cooldown_max_mult", 2.5)),
        w_markout=float(params.get("adaptive_add_cooldown_w_markout", 0.0)),
        w_flow=float(params.get("adaptive_add_cooldown_w_flow", 0.0)),
        w_campaign=float(params.get("adaptive_add_cooldown_w_campaign", 0.0)),
        w_trend=float(params.get("adaptive_add_cooldown_w_trend", 0.0)),
        w_refill_weak=float(
            params.get("adaptive_add_cooldown_w_refill_weak", 0.0)
        ),
        w_refill_good=float(
            params.get("adaptive_add_cooldown_w_refill_good", 0.0)
        ),
        w_reversion=float(params.get("adaptive_add_cooldown_w_reversion", 0.0)),
        mo_ref=float(params.get("adaptive_add_cooldown_mo_ref", 50.0)),
        flow_ref=float(params.get("adaptive_add_cooldown_flow_ref", 2.0)),
        campaign_inv_ref=float(
            params.get("adaptive_add_cooldown_campaign_inv_ref", 0.006)
        ),
        campaign_age_ref_s=float(
            params.get("adaptive_add_cooldown_campaign_age_ref_s", 3600.0)
        ),
        trend_ret_ref=float(
            params.get("adaptive_add_cooldown_trend_ret_ref", 2e-5)
        ),
        refill_ref=float(params.get("adaptive_add_cooldown_refill_ref", 0.10)),
        reversion_ref=float(
            params.get("adaptive_add_cooldown_reversion_ref", 1.0)
        ),
        gate_enabled=bool(
            params.get("adaptive_add_cooldown_gate_enabled", False)
        ),
        gate_mult=float(params.get("adaptive_add_cooldown_gate_mult", 1.75)),
        gate_campaign_score=float(
            params.get("adaptive_add_cooldown_gate_campaign_score", 1.0)
        ),
        gate_trend_score=float(
            params.get("adaptive_add_cooldown_gate_trend_score", 1.0)
        ),
        gate_refill_edge_max=float(
            params.get("adaptive_add_cooldown_gate_refill_edge_max", 0.0)
        ),
        gate_reversion_max=float(
            params.get("adaptive_add_cooldown_gate_reversion_max", 0.5)
        ),
        gate_side=str(
            params.get("adaptive_add_cooldown_gate_side", "BOTH") or "BOTH"
        ).upper(),
    )


def campaign_soft_gate_config_from_params(params) -> CampaignSoftGateConfig:
    return CampaignSoftGateConfig(
        enabled=bool(params.get("campaign_soft_gate_enabled", False)),
        campaign_inv_ref=float(
            params.get("campaign_soft_gate_campaign_inv_ref", 0.006)
        ),
        campaign_age_ref_s=float(
            params.get("campaign_soft_gate_campaign_age_ref_s", 3600.0)
        ),
        trend_ret_ref=float(
            params.get("campaign_soft_gate_trend_ret_ref", 2e-5)
        ),
        refill_ref=float(params.get("campaign_soft_gate_refill_ref", 0.10)),
        gate_campaign_score=float(
            params.get("campaign_soft_gate_campaign_score", 1.0)
        ),
        gate_trend_score=float(
            params.get("campaign_soft_gate_trend_score", 1.0)
        ),
        gate_refill_edge_max=float(
            params.get("campaign_soft_gate_refill_edge_max", 0.0)
        ),
        gate_reversion_max=float(
            params.get("campaign_soft_gate_reversion_max", 0.5)
        ),
        gate_side=str(
            params.get("campaign_soft_gate_side", "BOTH") or "BOTH"
        ).upper(),
    )


@dataclass(frozen=True)
class CommonSidePolicyInput:
    exposure_increasing: bool
    fill_cooldown_active: bool = False
    inventory_ratio: float = 0.0
    depth_age_s: float = 0.0
    max_book_age_s: float = 0.0
    toxicity: float = 0.5
    markout_ema: float = 0.0
    markout_spread_scale: float = 0.0
    markout_reference: float = 1.0
    microprice_shift_bps: float = 0.0
    l2_quote_flip_rate: float = 0.0
    l2_book_cancel_ratio: float = 0.0
    l2_near_depth_total: float = 0.0
    thin_depth_threshold: float = 0.0
    kappa_depth_baseline: float = 50.0
    side_adverse: bool = False
    side_adverse_pause: bool = False
    local_extreme_guard: bool = False
    local_extreme_spread_mult: float = 1.0
    local_extreme_pause: bool = False
    defense_guard: bool = False
    defense_spread_mult: float = 1.0
    defense_pause: bool = False


@dataclass(frozen=True)
class CommonSidePolicyResult:
    allow_post: bool = True
    allow_exposure_increase: bool = True
    spread_mult: float = 1.0
    size_mult: float = 1.0
    reason_mask: int = 0


def evaluate_common_side_policy(inputs: CommonSidePolicyInput) -> CommonSidePolicyResult:
    """Evaluate guards shared by live quoting and authoritative Python replay."""
    allow_post = True
    allow_exposure = True
    spread_mult = 1.0
    size_mult = 1.0
    reason_mask = 0

    if inputs.fill_cooldown_active:
        allow_post = False
        reason_mask |= POLICY_REASON_FILL_COOLDOWN

    if inputs.max_book_age_s > 0.0:
        if inputs.depth_age_s < 0.0 or inputs.depth_age_s == float("inf") or inputs.depth_age_s != inputs.depth_age_s:
            allow_post = False
            reason_mask |= POLICY_REASON_STALE_HARD
        elif inputs.depth_age_s >= inputs.max_book_age_s:
            allow_post = False
            reason_mask |= POLICY_REASON_STALE_HARD
        elif inputs.depth_age_s >= 0.5 * inputs.max_book_age_s:
            spread_mult = max(spread_mult, 1.25)
            size_mult = min(size_mult, 0.65)
            reason_mask |= POLICY_REASON_STALE_WARN

    if inputs.markout_ema < 0.0 and inputs.markout_spread_scale > 0.0:
        severity = min(abs(inputs.markout_ema) / max(inputs.markout_reference, 1e-6), 1.0)
        spread_mult = max(spread_mult, 1.05 + 0.25 * severity)
        size_mult = min(size_mult, 0.85 - 0.35 * severity)
        reason_mask |= POLICY_REASON_MARKOUT

    if inputs.side_adverse:
        size_mult = min(size_mult, 0.70)
        reason_mask |= POLICY_REASON_ADVERSE
        if inputs.side_adverse_pause:
            allow_exposure = False

    if inputs.local_extreme_guard:
        spread_mult = max(spread_mult, max(1.0, inputs.local_extreme_spread_mult))
        reason_mask |= POLICY_REASON_ADVERSE
        if inputs.local_extreme_pause:
            allow_exposure = False

    if inputs.defense_guard:
        spread_mult = max(spread_mult, max(1.0, inputs.defense_spread_mult))
        size_mult = min(size_mult, 0.70)
        reason_mask |= POLICY_REASON_DEFENSE
        if inputs.defense_pause:
            allow_post = False

    if (
        inputs.l2_quote_flip_rate >= 0.35
        and inputs.l2_book_cancel_ratio >= 0.04
        and abs(inputs.microprice_shift_bps) >= 0.5
    ):
        allow_exposure = False
        spread_mult = max(spread_mult, 1.35)
        size_mult = min(size_mult, 0.45)
        reason_mask |= POLICY_REASON_BURST

    thin_depth_threshold = inputs.thin_depth_threshold
    if thin_depth_threshold <= 0.0:
        thin_depth_threshold = max(1.0, inputs.kappa_depth_baseline * 0.5)
    if 0.0 < inputs.l2_near_depth_total < thin_depth_threshold:
        spread_mult = max(spread_mult, 1.10)
        size_mult = min(size_mult, 0.75)
        reason_mask |= POLICY_REASON_THIN_DEPTH

    if inputs.inventory_ratio >= 0.98 and inputs.exposure_increasing:
        allow_exposure = False
        reason_mask |= POLICY_REASON_INV_LIMIT

    return CommonSidePolicyResult(
        allow_post=allow_post,
        allow_exposure_increase=allow_exposure,
        spread_mult=max(1.0, spread_mult),
        size_mult=max(0.0, min(1.0, size_mult)),
        reason_mask=reason_mask,
    )


@dataclass(frozen=True)
class LocalExtremeGuardConfig:
    enabled: bool = False
    window_s: float = 120.0
    rank_threshold: float = 0.80
    require_thin_depth: bool = True
    thin_depth_threshold: float = 0.0
    spread_mult: float = 1.0
    pause: bool = False
    fragile_order_ttl_s: float = 0.0
    kappa_depth_baseline: float = 50.0
    tick_size: float = 0.1


@dataclass(frozen=True)
class LocalExtremeGuardResult:
    bid_active: bool = False
    ask_active: bool = False
    thin_active: bool = False
    fragile_ttl_active: bool = False
    rank: float = 0.5
    local_low: float = 0.0
    local_high: float = 0.0


def local_extreme_rank(prices: Iterable[float], mid_px: float, tick_size: float = 0.0) -> tuple[float, float, float]:
    """Return mid's [0,1] rank inside recent prices plus local low/high."""
    values: list[float] = []
    for px in prices:
        try:
            value = float(px)
        except (TypeError, ValueError):
            continue
        if value > 0.0:
            values.append(value)
    if not values or mid_px <= 0.0:
        return 0.5, mid_px, mid_px
    local_low = min(values)
    local_high = max(values)
    span = local_high - local_low
    if span <= max(float(tick_size or 0.0), 0.0):
        return 0.5, local_low, local_high
    rank = (mid_px - local_low) / span
    return max(0.0, min(1.0, rank)), local_low, local_high


def apply_local_extreme_guard_context(
    quote_context: dict[str, dict],
    *,
    mid_px: float,
    rank: float,
    local_low: float,
    local_high: float,
    cfg: LocalExtremeGuardConfig,
) -> LocalExtremeGuardResult:
    """Mutate BUY/SELL quote context with shared local-extreme diagnostics."""
    if not cfg.enabled and cfg.fragile_order_ttl_s <= 0.0:
        return LocalExtremeGuardResult(rank=rank, local_low=local_low, local_high=local_high)

    thin_threshold = cfg.thin_depth_threshold
    if thin_threshold <= 0.0:
        # 这是兼容旧配置的保守 fallback；BTCUSDC live 应优先显式配置 thin_depth_threshold，
        # 否则 kappa_depth_baseline * 0.5 可能让 local_extreme/fragile TTL 长期常驻。
        thin_threshold = max(1.0, float(cfg.kappa_depth_baseline) * 0.5)

    bid_ctx = quote_context["BUY"]
    ask_ctx = quote_context["SELL"]
    near_depth = float(bid_ctx.get("near_depth_total", bid_ctx.get("l2_near_depth_total", 0.0)) or 0.0)
    thin_active = 0.0 < near_depth < thin_threshold
    fragile_ttl_active = bool(cfg.fragile_order_ttl_s > 0.0 and thin_active)
    ttl_ms = int(max(0.0, cfg.fragile_order_ttl_s) * 1000.0)
    if fragile_ttl_active:
        bid_ctx["order_ttl_ms"] = ttl_ms
        ask_ctx["order_ttl_ms"] = ttl_ms

    if not cfg.enabled:
        return LocalExtremeGuardResult(
            thin_active=thin_active,
            fragile_ttl_active=fragile_ttl_active,
            rank=rank,
            local_low=local_low,
            local_high=local_high,
        )

    if cfg.require_thin_depth and not thin_active:
        for side_ctx in (bid_ctx, ask_ctx):
            side_ctx["local_extreme_rank"] = 0.5
            side_ctx["local_extreme_low"] = mid_px
            side_ctx["local_extreme_high"] = mid_px
            side_ctx["local_extreme_window_s"] = cfg.window_s
            side_ctx["local_extreme_thin_depth"] = False
            side_ctx["local_extreme_guard"] = False
            side_ctx["local_extreme_pause"] = False
            side_ctx["local_extreme_spread_mult"] = 1.0
            side_ctx.setdefault("order_ttl_ms", 0)
        return LocalExtremeGuardResult(
            thin_active=thin_active,
            fragile_ttl_active=fragile_ttl_active,
            rank=0.5,
            local_low=mid_px,
            local_high=mid_px,
        )

    allowed_by_depth = thin_active or not cfg.require_thin_depth
    # rank 越接近 1 越靠近局部高点，挡/拉宽 BUY；越接近 0 越靠近局部低点，挡/拉宽 SELL。
    bid_active = bool(allowed_by_depth and rank >= cfg.rank_threshold)
    ask_active = bool(allowed_by_depth and rank <= (1.0 - cfg.rank_threshold))

    for side_name, active in (("BUY", bid_active), ("SELL", ask_active)):
        side_ctx = quote_context[side_name]
        side_ctx["local_extreme_rank"] = rank
        side_ctx["local_extreme_low"] = local_low
        side_ctx["local_extreme_high"] = local_high
        side_ctx["local_extreme_window_s"] = cfg.window_s
        side_ctx["local_extreme_thin_depth"] = thin_active
        side_ctx["local_extreme_guard"] = active
        side_ctx["local_extreme_pause"] = bool(active and cfg.pause)
        side_ctx["local_extreme_spread_mult"] = cfg.spread_mult if active else 1.0
        if active and ttl_ms > 0:
            side_ctx["order_ttl_ms"] = ttl_ms
        else:
            side_ctx.setdefault("order_ttl_ms", 0)
        if active:
            side_ctx["any_constraint_changed"] = True

    return LocalExtremeGuardResult(
        bid_active=bid_active,
        ask_active=ask_active,
        thin_active=thin_active,
        fragile_ttl_active=fragile_ttl_active,
        rank=rank,
        local_low=local_low,
        local_high=local_high,
    )
