from strategy.policy_guards import (
    AdaptiveAddCooldownConfig,
    CampaignSoftGateConfig,
    adaptive_add_cooldown_config_from_params,
    adaptive_add_cooldown_multiplier,
    campaign_soft_gate_config_from_params,
    campaign_soft_gate_result,
)


def test_adaptive_add_cooldown_disabled_is_noop() -> None:
    cfg = AdaptiveAddCooldownConfig(enabled=False, max_mult=3.0)
    assert adaptive_add_cooldown_multiplier(
        side_markout_ema=-100.0,
        consec_units=5.0,
        prev_inventory=0.02,
        max_inventory=0.02,
        campaign_age_s=7200.0,
        side_adverse_ret=0.001,
        refill_edge=-0.5,
        micro_reversion_score=0.0,
        cfg=cfg,
    ) == 1.0


def test_adaptive_add_cooldown_risk_inputs_lengthen() -> None:
    cfg = AdaptiveAddCooldownConfig(
        enabled=True,
        min_mult=0.5,
        max_mult=2.5,
        w_markout=0.25,
        w_flow=0.25,
        w_campaign=0.25,
        w_trend=0.25,
        w_refill_weak=0.25,
        mo_ref=50.0,
        flow_ref=2.0,
        campaign_inv_ref=0.006,
        campaign_age_ref_s=3600.0,
        trend_ret_ref=2e-5,
        refill_ref=0.10,
    )
    mult = adaptive_add_cooldown_multiplier(
        side_markout_ema=-50.0,
        consec_units=3.0,
        prev_inventory=0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=2e-5,
        refill_edge=-0.10,
        micro_reversion_score=0.0,
        cfg=cfg,
    )
    assert mult > 1.0
    assert mult <= cfg.max_mult


def test_adaptive_add_cooldown_refill_reversion_can_shorten() -> None:
    cfg = AdaptiveAddCooldownConfig(
        enabled=True,
        min_mult=0.6,
        max_mult=2.5,
        w_refill_good=0.25,
        w_reversion=0.25,
        refill_ref=0.10,
        reversion_ref=1.0,
    )
    mult = adaptive_add_cooldown_multiplier(
        side_markout_ema=10.0,
        consec_units=1.0,
        prev_inventory=0.001,
        max_inventory=0.02,
        campaign_age_s=0.0,
        side_adverse_ret=0.0,
        refill_edge=0.10,
        micro_reversion_score=1.0,
        cfg=cfg,
    )
    assert cfg.min_mult <= mult < 1.0


def test_adaptive_add_cooldown_config_from_params() -> None:
    cfg = adaptive_add_cooldown_config_from_params(
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_w_flow": 0.2,
            "adaptive_add_cooldown_w_reversion": 0.3,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_gate_mult": 1.7,
            "adaptive_add_cooldown_gate_side": "SELL",
        }
    )
    assert cfg.enabled
    assert cfg.w_flow == 0.2
    assert cfg.w_reversion == 0.3
    assert cfg.gate_enabled
    assert cfg.gate_mult == 1.7
    assert cfg.gate_side == "SELL"


def test_adaptive_add_cooldown_gate_is_noop_until_all_conditions_hit() -> None:
    cfg = AdaptiveAddCooldownConfig(
        enabled=True,
        gate_enabled=True,
        min_mult=1.0,
        max_mult=2.0,
        gate_mult=1.75,
        gate_campaign_score=1.0,
        gate_trend_score=1.0,
        gate_refill_edge_max=0.0,
        gate_reversion_max=0.5,
        campaign_inv_ref=0.006,
        campaign_age_ref_s=3600.0,
        trend_ret_ref=2e-5,
        refill_ref=0.10,
    )

    # Campaign risk and adverse trend are present, but positive refill means
    # local repair evidence is not weak.  The gated arm must leave baseline
    # 41s behavior unchanged.
    assert adaptive_add_cooldown_multiplier(
        side_markout_ema=-100.0,
        consec_units=4.0,
        prev_inventory=0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=2e-5,
        refill_edge=0.05,
        micro_reversion_score=0.6,
        cfg=cfg,
    ) == 1.0

    assert adaptive_add_cooldown_multiplier(
        side_markout_ema=-100.0,
        consec_units=4.0,
        prev_inventory=0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=2e-5,
        refill_edge=-0.01,
        micro_reversion_score=0.0,
        cfg=cfg,
    ) == 1.75


def test_adaptive_add_cooldown_gate_side_filter() -> None:
    cfg = AdaptiveAddCooldownConfig(
        enabled=True,
        gate_enabled=True,
        gate_side="SELL",
        min_mult=1.0,
        max_mult=2.0,
        gate_mult=1.75,
        gate_campaign_score=1.0,
        gate_trend_score=1.0,
        gate_refill_edge_max=0.0,
        gate_reversion_max=0.5,
        campaign_inv_ref=0.006,
        campaign_age_ref_s=3600.0,
        trend_ret_ref=2e-5,
        refill_ref=0.10,
    )
    common = dict(
        side_markout_ema=-100.0,
        consec_units=4.0,
        prev_inventory=0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=2e-5,
        refill_edge=-0.01,
        micro_reversion_score=0.0,
        cfg=cfg,
    )
    assert adaptive_add_cooldown_multiplier(side="BUY", **common) == 1.0
    assert adaptive_add_cooldown_multiplier(side="SELL", **common) == 1.75


def test_campaign_soft_gate_disabled_preserves_legacy_soft_control() -> None:
    cfg = CampaignSoftGateConfig(enabled=False)
    result = campaign_soft_gate_result(
        side="SELL",
        prev_inventory=-0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=0.0,
        refill_edge=0.20,
        micro_reversion_score=1.0,
        cfg=cfg,
    )
    assert result.active


def test_campaign_soft_gate_requires_campaign_trend_and_weak_repair() -> None:
    cfg = CampaignSoftGateConfig(
        enabled=True,
        campaign_inv_ref=0.006,
        campaign_age_ref_s=3600.0,
        trend_ret_ref=2e-5,
        gate_campaign_score=1.0,
        gate_trend_score=1.0,
        gate_refill_edge_max=0.0,
        gate_reversion_max=0.5,
    )

    # Campaign risk and adverse trend are present, but positive refill and
    # micro-reversion imply natural repair is still plausible, so no widen.
    assert not campaign_soft_gate_result(
        side="SELL",
        prev_inventory=-0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=2e-5,
        refill_edge=0.05,
        micro_reversion_score=0.8,
        cfg=cfg,
    ).active

    assert campaign_soft_gate_result(
        side="SELL",
        prev_inventory=-0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=2e-5,
        refill_edge=-0.01,
        micro_reversion_score=0.0,
        cfg=cfg,
    ).active


def test_campaign_soft_gate_side_filter_and_config() -> None:
    cfg = campaign_soft_gate_config_from_params(
        {
            "campaign_soft_gate_enabled": True,
            "campaign_soft_gate_side": "SELL",
            "campaign_soft_gate_trend_score": 0.8,
        }
    )
    assert cfg.enabled
    assert cfg.gate_side == "SELL"
    assert cfg.gate_trend_score == 0.8

    common = dict(
        prev_inventory=-0.006,
        max_inventory=0.02,
        campaign_age_s=3600.0,
        side_adverse_ret=2e-5,
        refill_edge=-0.01,
        micro_reversion_score=0.0,
        cfg=cfg,
    )
    assert not campaign_soft_gate_result(side="BUY", **common).active
    assert campaign_soft_gate_result(side="SELL", **common).active
