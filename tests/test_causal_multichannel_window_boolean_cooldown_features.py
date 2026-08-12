from __future__ import annotations

import numpy as np
import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    BUY_DURATION_POLICY_IDS,
    COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE,
    COOLDOWN_DEADLINE_OWNER_NONE,
    SELL_DURATION_POLICY_IDS,
    CausalMultichannelEmaState,
    CausalWindowObservation,
    FeatureContractError,
    TriState,
    feature_schema,
    fit_cumulative_depth_shape,
    pair_key,
    tri_and,
    tri_not,
    tri_or,
    validate_m0_context,
)


def test_closed_form_depth_shape_matches_lstsq() -> None:
    rng = np.random.default_rng(20260810)
    for side in ("bid", "ask"):
        for _ in range(50):
            distances = np.cumsum(rng.integers(1, 25, size=20)).astype(float)
            distances -= distances[0]
            quantities = rng.uniform(0.0, 3.0, size=20)
            prices = (
                650_000.0 - distances
                if side == "bid"
                else 650_000.0 + distances
            )
            design = np.column_stack(
                (np.ones(20), distances, 0.5 * np.square(distances))
            )
            expected = np.linalg.lstsq(
                design, np.cumsum(quantities), rcond=None
            )[0]
            observed = fit_cumulative_depth_shape(
                prices, quantities, side=side
            )
            assert observed is not None
            assert observed[0] == pytest.approx(expected[1], abs=1e-11)
            assert observed[1] == pytest.approx(expected[2], abs=1e-11)


def _m0(*, side: str = "BUY", before: float = 0.0, after: float = 0.001) -> dict:
    return {
        "assignment_ts_ns": 1_700_000_000_200_000_000,
        "fill_visible_ts_ns": 1_700_000_000_200_000_000,
        "side": side,
        "role_at_fill": "opener" if before == 0.0 else "add",
        "inventory_before_fill_btc": before,
        "inventory_after_fill_btc": after,
        "fill_qty_btc": abs(after - before),
        "order_qty_btc": abs(after - before),
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": abs(after - before),
        "remaining_order_qty_after_btc": 0.0,
        "partial_fill_ordinal": 1,
        "fill_is_partial": False,
        "order_age_s": 1.0,
        "queue_ahead_before_fill_btc": 0.0,
        "queue_state_before_fill": "known_zero",
        "target_price_tick": 640_000,
        "target_price_displayed_qty_btc": 0.0,
        "target_price_displayed_qty_status": "known_zero",
        "target_price_displayed_qty_known": True,
        "target_price_displayed_qty_is_queue_ahead": False,
        "consecutive_units_after": abs(after) / 0.001,
        "baseline_duration_ms": 85_000.0 * (abs(after) / 0.001),
        "campaign_age_s": 0.0,
        "campaign_add_count": 0,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": 0.0,
        "last_opposite_side_fill_age_s": 0.0,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _window(
    index: int,
    value: float | None,
    *,
    ready_delay_ns: int = 0,
    gap: bool = False,
) -> CausalWindowObservation:
    right = 1_700_000_000_000_000_000 + index * BASE_WINDOW_WIDTH_NS
    return CausalWindowObservation(
        left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
        right_ts_ns=right,
        feature_ready_ts_ns=right + ready_delay_ns,
        market_generation=index,
        depth_generation=index,
        values={"mid_usdc_per_btc": value},
        source_gap=gap,
    )


def _m2_window(
    index: int,
    *,
    displayed_change: float | None,
) -> CausalWindowObservation:
    right = 1_700_000_000_000_000_000 + index * BASE_WINDOW_WIDTH_NS
    values = {
        channel["name"]: 1.0 for channel in feature_schema()["blocks"]["M2"]
    }
    for name in (
        "topk_bid_displayed_depth_increase_btc_per_s",
        "topk_bid_displayed_depth_decrease_btc_per_s",
        "topk_ask_displayed_depth_increase_btc_per_s",
        "topk_ask_displayed_depth_decrease_btc_per_s",
    ):
        values[name] = displayed_change
    return CausalWindowObservation(
        left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
        right_ts_ns=right,
        feature_ready_ts_ns=right,
        market_generation=index,
        depth_generation=index,
        values=values,
    )


def test_three_valued_boolean_never_turns_missing_into_true() -> None:
    assert tri_not(TriState.UNOBSERVED) is TriState.UNOBSERVED
    assert tri_and((TriState.TRUE, TriState.UNOBSERVED)) is TriState.UNOBSERVED
    assert tri_and((TriState.FALSE, TriState.UNOBSERVED)) is TriState.FALSE
    assert tri_or((TriState.FALSE, TriState.UNOBSERVED)) is TriState.UNOBSERVED
    assert tri_or((TriState.TRUE, TriState.UNOBSERVED)) is TriState.TRUE


def test_duration_vocabulary_is_inherited_without_reselection() -> None:
    assert BUY_DURATION_POLICY_IDS == (
        "CONTROL_85N",
        "FIXED_79S",
        "FIXED_173S",
        "FIXED_223S",
        "FIXED_356S",
        "FIXED_640S",
        "FIXED_709S",
        "FIXED_2048S",
    )
    assert SELL_DURATION_POLICY_IDS[-1] == "FIXED_1748S"
    assert len(BUY_DURATION_POLICY_IDS) == len(SELL_DURATION_POLICY_IDS) == 8


def test_m0_binds_action_magnitude_and_rejects_reducing_or_wrong_85n() -> None:
    row = _m0(side="BUY", before=0.001, after=0.002)
    assert validate_m0_context(row)["role_at_fill"] == "add"

    wrong = dict(row, baseline_duration_ms=85_000.0)
    with pytest.raises(FeatureContractError, match="CONTROL_85N"):
        validate_m0_context(wrong)

    reducing = _m0(side="SELL", before=0.002, after=0.001)
    with pytest.raises(FeatureContractError, match="not exposure increasing"):
        validate_m0_context(reducing)

    partial = _m0(side="BUY", before=0.0, after=0.0004)
    partial["baseline_duration_ms"] = 85_000.0
    assert validate_m0_context(partial)["consecutive_units_after"] == pytest.approx(
        0.4
    )

    no_history = dict(partial)
    no_history["last_same_side_fill_age_s"] = None
    no_history["last_opposite_side_fill_age_s"] = None
    validated = validate_m0_context(no_history)
    assert validated["last_same_side_fill_age_s"] is None
    assert validated["last_opposite_side_fill_age_s"] is None

    with pytest.raises(FeatureContractError, match="unknown fields"):
        validate_m0_context(dict(row, hidden_label=1.0))


def test_m0_normalizes_bounded_cooldown_owner_and_rejects_drift() -> None:
    inactive = validate_m0_context(_m0())
    assert inactive["cooldown_deadline_owner"] == COOLDOWN_DEADLINE_OWNER_NONE

    active = _m0(side="BUY", before=0.001, after=0.002)
    active.update(
        cooldown_remaining_ms=12_500.0,
        cooldown_blocker_active=True,
        cooldown_lineage_revision_before=3,
        cooldown_deadline_owner="buy-lineage-3",
    )
    normalized = validate_m0_context(active)
    assert normalized["cooldown_deadline_owner"] == (
        COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE
    )
    assert validate_m0_context(normalized) == normalized

    wrong_side = dict(active, cooldown_deadline_owner="sell-lineage-3")
    with pytest.raises(FeatureContractError, match="side or lineage revision"):
        validate_m0_context(wrong_side)

    wrong_revision = dict(active, cooldown_deadline_owner="buy-lineage-2")
    with pytest.raises(FeatureContractError, match="side or lineage revision"):
        validate_m0_context(wrong_revision)

    arbitrary = dict(active, cooldown_deadline_owner="legacy-owner")
    with pytest.raises(FeatureContractError, match="supported lineage identity"):
        validate_m0_context(arbitrary)

    inconsistent_blocker = dict(active, cooldown_blocker_active=False)
    with pytest.raises(FeatureContractError, match="blocker disagrees"):
        validate_m0_context(inconsistent_blocker)


def test_completed_window_and_feature_ready_cutoff_are_fail_closed() -> None:
    state = CausalMultichannelEmaState(
        block="R0",
        half_lives_s=(0.5, 1.0),
        warmup_admitted=True,
        warmup_identity="d-minus-1:sha256",
    )
    state.update(_window(1, 100.0, ready_delay_ns=50_000_000))
    with pytest.raises(FeatureContractError, match="crossed the decision cutoff"):
        state.feature_row(
            side="BUY",
            decision_ts_ns=_window(1, 100.0).right_ts_ns,
            m0_context=_m0(),
        )

    malformed = _window(2, 101.0)
    malformed = CausalWindowObservation(
        left_ts_ns=malformed.left_ts_ns + 1,
        right_ts_ns=malformed.right_ts_ns,
        feature_ready_ts_ns=malformed.feature_ready_ts_ns,
        market_generation=malformed.market_generation,
        depth_generation=malformed.depth_generation,
        values=malformed.values,
    )
    with pytest.raises(FeatureContractError, match="width"):
        state.update(malformed)


def test_window_schema_and_missing_grid_rows_are_explicit() -> None:
    state = CausalMultichannelEmaState(
        block="R0",
        half_lives_s=(0.5, 1.0),
        warmup_admitted=True,
        warmup_identity="d-minus-1:sha256",
    )
    state.update(_window(1, 100.0))

    missing_grid_row = _window(3, 101.0)
    with pytest.raises(FeatureContractError, match="emitted explicitly"):
        state.update(missing_grid_row)

    extra_channel = _window(2, 101.0)
    extra_channel = CausalWindowObservation(
        left_ts_ns=extra_channel.left_ts_ns,
        right_ts_ns=extra_channel.right_ts_ns,
        feature_ready_ts_ns=extra_channel.feature_ready_ts_ns,
        market_generation=extra_channel.market_generation,
        depth_generation=extra_channel.depth_generation,
        values={"mid_usdc_per_btc": 101.0, "hidden_label": 1.0},
    )
    with pytest.raises(FeatureContractError, match="channel schema drifted"):
        state.update(extra_channel)


def test_gap_is_unobserved_without_forward_fill_and_clean_window_recovers() -> None:
    state = CausalMultichannelEmaState(
        block="R0",
        half_lives_s=(0.5, 1.0),
        warmup_admitted=True,
        warmup_identity="d-minus-1:sha256",
    )
    state.update(_window(1, 100.0))
    state.update(_window(2, 101.0))
    state.update(_window(3, None, gap=True))
    missing = state.feature_row(
        side="BUY",
        decision_ts_ns=_window(3, None).right_ts_ns,
        m0_context=_m0(),
    )
    key = pair_key("mid_usdc_per_btc", 0.5, 1.0)
    assert missing["channel::mid_usdc_per_btc::observed"] == 0
    assert missing[f"tri::{key}::positive_ordering"] == TriState.UNOBSERVED
    assert missing["support_valid"] is False

    state.update(_window(4, 102.0))
    recovered = state.feature_row(
        side="BUY",
        decision_ts_ns=_window(4, 102.0).right_ts_ns,
        m0_context=_m0(),
    )
    assert recovered["channel::mid_usdc_per_btc::observed"] == 1
    assert recovered["support_valid"] is True


def test_buy_sell_price_ordering_is_side_signed_and_checkpoint_stable() -> None:
    state = CausalMultichannelEmaState(
        block="R0",
        half_lives_s=(0.5, 1.0),
        warmup_admitted=True,
        warmup_identity="d-minus-1:sha256",
    )
    for index, value in enumerate((100.0, 101.0, 102.0), start=1):
        state.update(_window(index, value))
    decision = _window(3, 102.0).right_ts_ns
    key = pair_key("mid_usdc_per_btc", 0.5, 1.0)
    buy = state.feature_row(side="BUY", decision_ts_ns=decision, m0_context=_m0())
    sell = state.feature_row(
        side="SELL",
        decision_ts_ns=decision,
        m0_context=_m0(side="SELL", before=0.0, after=-0.001),
    )
    assert buy[f"tri::{key}::positive_ordering"] == TriState.TRUE
    assert sell[f"tri::{key}::positive_ordering"] == TriState.FALSE

    restored = CausalMultichannelEmaState.restore(state.checkpoint())
    assert restored.feature_row(
        side="BUY", decision_ts_ns=decision, m0_context=_m0()
    ) == buy


def test_feature_schema_separates_blocks_and_forbids_cross_unit_pairs() -> None:
    schema = feature_schema()
    assert schema["window_contract"]["base_window_width_ns"] == 100_000_000
    assert schema["cross_channel_ema_pairs_forbidden"] is True
    assert schema["top_k_depth_levels"] == 20
    assert schema["price_tick_size_usdc_per_btc"] == pytest.approx(0.1)
    assert "OLS fit" in schema["depth_shape_formula"]
    assert schema["displayed_depth_change_is_exact_depletion_refill"] is False
    assert "trade_absorption_ratio" in schema["deferred_m2_channels"]
    assert "within_window_mid_change_bps" in schema["deferred_m2_channels"]
    assert "bid_depth_slope" not in schema["deferred_m2_channels"]
    assert all(
        channel["name"] != "trade_absorption_ratio"
        for channel in schema["blocks"]["M2"]
    )
    m2_names = {channel["name"] for channel in schema["blocks"]["M2"]}
    assert "bid_depth_slope_btc_per_tick" in m2_names
    assert "ask_depth_convexity_btc_per_tick2" in m2_names
    assert "topk_bid_displayed_depth_increase_btc_per_s" in m2_names
    assert len(schema["blocks"]["R0"]) == 1
    assert len(schema["blocks"]["M2"]) > len(schema["blocks"]["M1"])
    assert schema["economic_outcomes_read"] is False


def test_displayed_change_channel_uses_three_state_missing_and_checkpoint() -> None:
    state = CausalMultichannelEmaState(
        block="M2",
        half_lives_s=(0.5, 1.0),
        warmup_admitted=True,
        warmup_identity="d-minus-1:sha256",
    )
    channel = "topk_bid_displayed_depth_increase_btc_per_s"
    key = pair_key(channel, 0.5, 1.0)

    state.update(_m2_window(1, displayed_change=None))
    first = state.feature_row(
        side="BUY",
        decision_ts_ns=_m2_window(1, displayed_change=None).right_ts_ns,
        m0_context=_m0(),
    )
    assert first[f"channel::{channel}::observed"] == 0
    assert first[f"tri::{key}::positive_ordering"] == TriState.UNOBSERVED
    assert first["support_valid"] is False

    state.update(_m2_window(2, displayed_change=1.0))
    state.update(_m2_window(3, displayed_change=2.0))
    decision_ts_ns = _m2_window(3, displayed_change=2.0).right_ts_ns
    observed = state.feature_row(
        side="BUY",
        decision_ts_ns=decision_ts_ns,
        m0_context=_m0(),
    )
    assert observed[f"channel::{channel}::observed"] == 1
    assert observed[f"tri::{key}::positive_ordering"] == TriState.TRUE
    assert observed["support_valid"] is True

    restored = CausalMultichannelEmaState.restore(state.checkpoint())
    assert restored.feature_row(
        side="BUY",
        decision_ts_ns=decision_ts_ns,
        m0_context=_m0(),
    ) == observed
