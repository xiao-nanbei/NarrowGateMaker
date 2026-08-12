from __future__ import annotations

import math

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    ADD_NOW,
    WAIT_ONE_EPOCH,
    ArmWashoutState,
    ContinuousTimeEmaSurface,
    ExternalDecision,
    MarketGeneration,
    baseline_add_action,
    campaign_unit_weights,
    choose_against_baseline,
    joint_washout_complete,
    model_feature_names,
    next_external_decision,
    validate_feature_row,
)


def _generation(index: int) -> MarketGeneration:
    return MarketGeneration(
        bbo_index=index,
        l2_index=index,
        trade_index=index,
        feature_ready_index=index,
        prediction_index=index // 2,
        snapshot_mid_tick_x2=1_000 + index,
    )


def _washout(**overrides: object) -> ArmWashoutState:
    values = {
        "inventory_btc": 0.0,
        "campaign_active": False,
        "active_order_count": 0,
        "pending_submit_count": 0,
        "pending_cancel_count": 0,
        "pending_ack_count": 0,
        "descendant_unterminal_count": 0,
        "cursor_owner_count": 0,
        "hazard_owner_count": 0,
        "second_assignment_count": 0,
    }
    values.update(overrides)
    return ArmWashoutState(**values)


def test_continuous_time_ema_uses_elapsed_time_and_side_mapping() -> None:
    surface = ContinuousTimeEmaSurface((1.0, 2.0, 4.0))
    surface.update(ts_ns=0, price=100.0)
    surface.update(ts_ns=1_000_000_000, price=102.0)

    expected_fast = 0.5 * 100.0 + 0.5 * 102.0
    assert surface.pair_z_bps(1.0, 2.0) == pytest.approx(
        10_000.0 * (expected_fast - (math.sqrt(0.5) * 100.0 + (1.0 - math.sqrt(0.5)) * 102.0)) / 102.0
    )
    buy = surface.feature_row(
        side="BUY", causal_volatility_bps=2.0, tick_bps=0.5
    )
    sell = surface.feature_row(
        side="SELL", causal_volatility_bps=2.0, tick_bps=0.5
    )
    for name in model_feature_names((1.0, 2.0, 4.0)):
        if "positive_fraction" in name:
            continue
        if "age_" in name or "missing" in name or "ordering_persistence" in name:
            assert float(buy[name]) == pytest.approx(float(sell[name]))
        else:
            assert float(buy[name]) == pytest.approx(-float(sell[name]))


def test_duplicate_timestamp_cannot_change_canonical_price() -> None:
    surface = ContinuousTimeEmaSurface((1.0, 2.0))
    surface.update(ts_ns=1, price=100.0)
    with pytest.raises(ValueError, match="duplicate EMA timestamp"):
        surface.update(ts_ns=1, price=100.1)


def test_external_epoch_ignores_forced_or_unready_decisions() -> None:
    previous = _generation(1)
    decisions = [
        ExternalDecision(2_000, _generation(2), False, True),
        ExternalDecision(3_000, _generation(3), True, False),
        ExternalDecision(4_000, _generation(4), True, True),
    ]
    selected = next_external_decision(
        decisions,
        after_ts_ms=1_000,
        previous_generation=previous,
    )
    assert selected is not None
    assert selected.decision_ts_ms == 4_000


def test_market_generation_rejects_regression() -> None:
    with pytest.raises(ValueError, match="regressed"):
        _generation(1).is_strictly_after(_generation(2))


def test_market_generation_allows_mid_to_move_down() -> None:
    previous = _generation(1)
    current = MarketGeneration(
        bbo_index=2,
        l2_index=2,
        trade_index=2,
        feature_ready_index=2,
        prediction_index=1,
        snapshot_mid_tick_x2=999,
    )
    assert current.is_strictly_after(previous)


def test_policy_falls_back_to_current_cooldown_behavior() -> None:
    assert baseline_add_action(cooldown_active=True, baseline_can_add=True) == WAIT_ONE_EPOCH
    assert baseline_add_action(cooldown_active=False, baseline_can_add=True) == ADD_NOW
    assert baseline_add_action(cooldown_active=False, baseline_can_add=False) == WAIT_ONE_EPOCH

    assert choose_against_baseline(
        baseline_action=WAIT_ONE_EPOCH,
        add_minus_wait_lcb_usdc=0.02,
        add_minus_wait_ucb_usdc=0.03,
        economic_threshold_usdc=0.01,
    ) == ADD_NOW
    assert choose_against_baseline(
        baseline_action=WAIT_ONE_EPOCH,
        add_minus_wait_lcb_usdc=-0.01,
        add_minus_wait_ucb_usdc=0.03,
        economic_threshold_usdc=0.01,
    ) == WAIT_ONE_EPOCH
    assert choose_against_baseline(
        baseline_action=ADD_NOW,
        add_minus_wait_lcb_usdc=-0.04,
        add_minus_wait_ucb_usdc=-0.02,
        economic_threshold_usdc=0.01,
    ) == WAIT_ONE_EPOCH
    assert choose_against_baseline(
        baseline_action=ADD_NOW,
        add_minus_wait_lcb_usdc=-0.02,
        add_minus_wait_ucb_usdc=0.02,
        economic_threshold_usdc=0.01,
    ) == ADD_NOW


def test_joint_washout_is_zero_tolerance_on_owned_state() -> None:
    complete = _washout()
    assert joint_washout_complete(complete, complete)
    assert not joint_washout_complete(
        complete,
        _washout(pending_ack_count=1),
    )
    assert not joint_washout_complete(
        complete,
        _washout(inventory_btc=0.001),
    )
    assert not joint_washout_complete(
        complete,
        _washout(campaign_active=True),
    )


def test_campaign_weights_sum_to_one_per_campaign() -> None:
    frame = pd.DataFrame(
        {
            "day": ["2026-01-01"] * 4,
            "side": ["SELL"] * 4,
            "campaign_id": [1, 1, 1, 2],
        }
    )
    weights = campaign_unit_weights(frame)
    assert weights.tolist() == pytest.approx([1 / 3, 1 / 3, 1 / 3, 1.0])


def test_cross_diagnostics_are_not_model_features() -> None:
    surface = ContinuousTimeEmaSurface((1.0, 2.0, 4.0))
    surface.update(ts_ns=0, price=100.0)
    surface.update(ts_ns=1_000_000_000, price=101.0)
    row = surface.feature_row(
        side="SELL", causal_volatility_bps=2.0, tick_bps=0.5
    )
    validate_feature_row(row, (1.0, 2.0, 4.0))
    assert not any("cross_sign" in name for name in model_feature_names())
