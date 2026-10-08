from __future__ import annotations

import pytest

from strategy.conditional_p3_reach_gate import (
    apply_conditional_p3_outward_quote_action,
    executable_price_tick,
)


def _apply(**overrides):
    payload = {
        "bid_price": 99.0,
        "ask_price": 101.0,
        "best_bid": 99.5,
        "best_ask": 100.5,
        "inventory_btc": 0.0,
        "lot_size": 0.001,
        "tick_size": 0.1,
        "max_pair_spread": 10.0,
        "bid_allowed": True,
        "ask_allowed": True,
        "tox_bid": 0.9,
        "tox_ask": 0.9,
        "bid_toxicity_threshold": 0.8,
        "ask_toxicity_threshold": 0.8,
        "bid_gate_status": 2,
        "ask_gate_status": 2,
        "outward_ticks": 16,
    }
    payload.update(overrides)
    return apply_conditional_p3_outward_quote_action(**payload)


def test_flat_exposure_quotes_move_outward_on_price_axis_only():
    decision = _apply()

    assert decision.bid_price == pytest.approx(97.4)
    assert decision.ask_price == pytest.approx(102.6)
    assert decision.bid_price_changed is True
    assert decision.ask_price_changed is True
    assert decision.bid_requested is True
    assert decision.ask_requested is True
    assert decision.bid_distance_ticks == 5
    assert decision.ask_distance_ticks == 5


def test_reducing_side_is_never_changed():
    long_inventory = _apply(inventory_btc=0.001)
    short_inventory = _apply(inventory_btc=-0.001)

    assert long_inventory.bid_price == pytest.approx(97.4)
    assert long_inventory.ask_price == pytest.approx(101.0)
    assert long_inventory.bid_price_changed is True
    assert long_inventory.ask_price_changed is False
    assert short_inventory.bid_price == pytest.approx(99.0)
    assert short_inventory.ask_price == pytest.approx(102.6)
    assert short_inventory.bid_price_changed is False
    assert short_inventory.ask_price_changed is True


def test_status_and_toxicity_must_both_pass():
    unsupported = _apply(bid_gate_status=1, ask_gate_status=0)
    low_score = _apply(tox_bid=0.79, tox_ask=0.79)

    assert unsupported.bid_price == pytest.approx(99.0)
    assert unsupported.ask_price == pytest.approx(101.0)
    assert low_score.bid_price == pytest.approx(99.0)
    assert low_score.ask_price == pytest.approx(101.0)


def test_joint_spread_cap_fails_closed_without_partial_side_application():
    decision = _apply(max_pair_spread=5.0)

    assert decision.bid_price == pytest.approx(99.0)
    assert decision.ask_price == pytest.approx(101.0)
    assert decision.bid_spread_cap_noop is True
    assert decision.ask_spread_cap_noop is True


def test_executable_tick_identity_rejects_off_grid_prices():
    assert executable_price_tick(64_714.6, 0.1, name="price") == 647_146
    with pytest.raises(ValueError, match="tick grid"):
        executable_price_tick(64_714.65, 0.1, name="price")
