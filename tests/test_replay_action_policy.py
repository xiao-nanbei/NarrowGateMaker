from __future__ import annotations

import numpy as np
import pytest

from models.replay_policies import (
    CAMPAIGN_STOP_ADD_ACTIONS,
    LOCAL_ACTIONS,
    QUEUE_VALUE_CANCEL_REENTER_ACTIONS,
    QUEUE_VALUE_KEEP_CANCEL_ACTIONS,
    SELL_ADD_SKIP_ACTIONS,
    apply_local_add_action,
    choose_action,
    choose_campaign_stop_add_action,
    choose_queue_value_action,
    choose_queue_value_cancel_reenter_action,
    choose_sell_add_skip_action,
    normalize_action_probabilities,
    normalize_campaign_stop_add_probabilities,
    normalize_queue_value_cancel_reenter_probabilities,
    normalize_queue_value_probabilities,
    normalize_sell_add_skip_probabilities,
)


def test_default_probabilities_are_complete_and_pre_registered() -> None:
    probabilities = normalize_action_probabilities()

    assert tuple(probabilities) == LOCAL_ACTIONS
    assert probabilities["baseline"] == pytest.approx(0.90)
    assert sum(probabilities.values()) == pytest.approx(1.0)
    assert all(value > 0.0 for value in probabilities.values())


def test_action_assignment_uses_independent_probability_vector() -> None:
    probabilities = normalize_action_probabilities(
        {
            "baseline": 0.25,
            "prevent_over_widen": 0.25,
            "widen_1tick": 0.25,
            "recenter_1tick": 0.25,
        }
    )
    rng = np.random.default_rng(7)
    actions = [choose_action(rng, probabilities)[0] for _ in range(2000)]

    assert set(actions) == set(LOCAL_ACTIONS)


def test_buy_actions_are_bounded_and_leave_other_side_implicit() -> None:
    common = {
        "side": "BUY",
        "baseline_price": 99.0,
        "pre_guard_price": 99.5,
        "other_side_price": 101.0,
        "mid": 100.0,
        "best_bid": 99.8,
        "best_ask": 100.2,
        "tick": 0.1,
        "max_pair_spread": 2.0,
    }

    keep = apply_local_add_action(
        action="prevent_over_widen", microprice_shift_bps=0.0, **common
    )
    widen = apply_local_add_action(
        action="widen_1tick", microprice_shift_bps=0.0, **common
    )
    recenter = apply_local_add_action(
        action="recenter_1tick", microprice_shift_bps=1.0, **common
    )

    assert keep.selected_price == pytest.approx(99.1)
    assert widen.selected_price == pytest.approx(99.0)
    assert widen.clamp_reason == "pair_spread_cap"
    assert recenter.selected_price == pytest.approx(99.1)


def test_baseline_never_moves_an_already_valid_replay_price() -> None:
    baseline = 63_435.700000000004
    result = apply_local_add_action(
        side="BUY",
        action="baseline",
        baseline_price=baseline,
        pre_guard_price=baseline + 1.0,
        other_side_price=63_437.0,
        mid=63_436.0,
        best_bid=63_435.8,
        best_ask=63_436.2,
        microprice_shift_bps=1.0,
        tick=0.1,
        max_pair_spread=2.0,
    )

    assert result.selected_price == baseline
    assert result.effective is False


def test_sell_recenter_follows_local_price_direction_and_is_post_only() -> None:
    result = apply_local_add_action(
        side="SELL",
        action="recenter_1tick",
        baseline_price=101.0,
        pre_guard_price=100.5,
        other_side_price=99.0,
        mid=100.0,
        best_bid=99.9,
        best_ask=100.2,
        microprice_shift_bps=-1.0,
        tick=0.1,
        max_pair_spread=2.0,
    )

    assert result.selected_price == pytest.approx(100.9)
    assert result.delta_ticks == pytest.approx(-1.0)


def test_probability_vector_requires_support_for_every_action() -> None:
    with pytest.raises(ValueError, match="missing"):
        normalize_action_probabilities({"baseline": 1.0})


def test_frozen_binary_family_keeps_inactive_actions_as_explicit_zeros() -> None:
    probabilities = normalize_action_probabilities(
        {
            "baseline": 0.5,
            "prevent_over_widen": 0.0,
            "widen_1tick": 0.5,
            "recenter_1tick": 0.0,
        },
        allow_zero_support=True,
    )
    rng = np.random.default_rng(17)
    actions = [choose_action(rng, probabilities)[0] for _ in range(1_000)]

    assert set(actions) == {"baseline", "widen_1tick"}


def test_sell_add_skip_family_has_exact_overlap_and_only_two_actions() -> None:
    probabilities = normalize_sell_add_skip_probabilities()
    rng = np.random.default_rng(23)
    actions = [
        choose_sell_add_skip_action(rng, probabilities)[0]
        for _ in range(1_000)
    ]

    assert tuple(probabilities) == SELL_ADD_SKIP_ACTIONS
    assert probabilities == {
        "baseline": 0.5,
        "skip_one_add_cycle": 0.5,
    }
    assert set(actions) == set(SELL_ADD_SKIP_ACTIONS)


def test_campaign_stop_add_family_has_exact_overlap() -> None:
    probabilities = normalize_campaign_stop_add_probabilities()
    rng = np.random.default_rng(31)
    actions = [
        choose_campaign_stop_add_action(rng, probabilities)[0]
        for _ in range(1_000)
    ]

    assert tuple(probabilities) == CAMPAIGN_STOP_ADD_ACTIONS
    assert probabilities == {
        "baseline": 0.5,
        "stop_add_until_flat": 0.5,
    }
    assert set(actions) == set(CAMPAIGN_STOP_ADD_ACTIONS)


def test_queue_value_family_has_exact_overlap_and_only_two_actions() -> None:
    probabilities = normalize_queue_value_probabilities()
    rng = np.random.default_rng(29)
    actions = [
        choose_queue_value_action(rng, probabilities)[0]
        for _ in range(1_000)
    ]

    assert tuple(probabilities) == QUEUE_VALUE_KEEP_CANCEL_ACTIONS
    assert probabilities == {
        "keep": 0.5,
        "cancel_until_state_exit": 0.5,
    }
    assert set(actions) == set(QUEUE_VALUE_KEEP_CANCEL_ACTIONS)


def test_queue_value_cancel_reenter_has_exact_overlap() -> None:
    probabilities = normalize_queue_value_cancel_reenter_probabilities()
    rng = np.random.default_rng(31)
    actions = [
        choose_queue_value_cancel_reenter_action(rng, probabilities)[0]
        for _ in range(1_000)
    ]

    assert tuple(probabilities) == QUEUE_VALUE_CANCEL_REENTER_ACTIONS
    assert probabilities == {
        "keep": 0.5,
        "cancel_then_baseline_reenter": 0.5,
    }
    assert set(actions) == set(QUEUE_VALUE_CANCEL_REENTER_ACTIONS)
