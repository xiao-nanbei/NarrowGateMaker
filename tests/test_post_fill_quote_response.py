import pytest

from strategy.post_fill_quote_response import (
    FLOW_ADD_WIDEN_MODE,
    HYBRID_MODE,
    INVENTORY_SHIFT_MODE,
    PostFillQuoteResponse,
    PostFillQuoteResponseConfig,
)


def _config(mode: str) -> PostFillQuoteResponseConfig:
    return PostFillQuoteResponseConfig(
        enabled=True,
        mode=mode,
        inventory_ticks_per_order_unit=0.5,
        inventory_max_ticks=4.0,
        flow_ticks_per_excitation=2.0,
        flow_max_ticks=8.0,
        response_half_life_s=20.0,
        response_half_life_min_s=4.0,
        response_half_life_max_s=120.0,
        volatility_weight=0.0,
        refill_weight=0.0,
        repair_probability_weight=0.0,
    )


def _quote(response: PostFillQuoteResponse, *, now_ms: int, inventory: float):
    return response.quote(
        now_ms=now_ms,
        inventory=inventory,
        order_size=0.001,
        baseline_bid=99.0,
        baseline_ask=101.0,
        tick_size=0.1,
        max_pair_spread=10.0,
        best_bid=98.0,
        best_ask=102.0,
        volatility_bps=3.0,
        refill_edge=0.0,
        repair_probability=0.5,
    )


@pytest.mark.parametrize(
    ("inventory", "side", "expected_bid", "expected_ask"),
    [
        (0.002, "BUY", 98.7, 100.9),
        (-0.002, "SELL", 99.1, 101.3),
    ],
)
def test_hybrid_formula_moves_add_farther_and_reduce_only_by_inventory(
    inventory, side, expected_bid, expected_ask
):
    response = PostFillQuoteResponse(_config(HYBRID_MODE))
    assert response.record_fill(
        side=side,
        inventory_before=inventory / 2.0,
        inventory_after=inventory,
        fill_qty=0.001,
        order_size=0.001,
        ts_ms=0,
    )

    decision = _quote(response, now_ms=0, inventory=inventory)

    assert decision.inventory_shift_ticks == 1
    assert decision.add_widen_ticks == 2
    assert decision.bid_price == pytest.approx(expected_bid)
    assert decision.ask_price == pytest.approx(expected_ask)
    assert decision.pair_spread_delta == pytest.approx(0.2)


@pytest.mark.parametrize("inventory", [0.002, -0.002])
def test_a_term_leaves_reducing_quote_exactly_equal_to_inventory_only(inventory):
    inventory_only = PostFillQuoteResponse(_config(INVENTORY_SHIFT_MODE))
    hybrid = PostFillQuoteResponse(_config(HYBRID_MODE))
    side = "BUY" if inventory > 0 else "SELL"
    hybrid.record_fill(
        side=side,
        inventory_before=inventory / 2.0,
        inventory_after=inventory,
        fill_qty=0.001,
        order_size=0.001,
        ts_ms=0,
    )

    i_decision = _quote(inventory_only, now_ms=0, inventory=inventory)
    hybrid_decision = _quote(hybrid, now_ms=0, inventory=inventory)

    if inventory > 0.0:
        assert hybrid_decision.ask_price == pytest.approx(i_decision.ask_price)
        assert hybrid_decision.bid_price < i_decision.bid_price
    else:
        assert hybrid_decision.bid_price == pytest.approx(i_decision.bid_price)
        assert hybrid_decision.ask_price > i_decision.ask_price


def test_response_excitation_halves_at_configured_half_life():
    response = PostFillQuoteResponse(_config(FLOW_ADD_WIDEN_MODE))
    response.record_fill(
        side="BUY",
        inventory_before=0.0,
        inventory_after=0.001,
        fill_qty=0.001,
        order_size=0.001,
        ts_ms=0,
    )

    first = _quote(response, now_ms=0, inventory=0.001)
    half = _quote(response, now_ms=20_000, inventory=0.001)

    assert first.excitation == pytest.approx(1.0)
    assert half.excitation == pytest.approx(0.5)
    assert first.add_widen_ticks == 2
    assert half.add_widen_ticks == 1


def test_good_refill_and_high_repair_shorten_half_life():
    cfg = PostFillQuoteResponseConfig(
        enabled=True,
        mode=FLOW_ADD_WIDEN_MODE,
        response_half_life_s=20.0,
        response_half_life_min_s=4.0,
        response_half_life_max_s=120.0,
        volatility_weight=0.0,
        refill_weight=1.0,
        repair_probability_weight=1.0,
    )
    response = PostFillQuoteResponse(cfg)
    response.record_fill(
        side="BUY",
        inventory_before=0.0,
        inventory_after=0.001,
        fill_qty=0.001,
        order_size=0.001,
        ts_ms=0,
    )
    decision = response.quote(
        now_ms=0,
        inventory=0.001,
        order_size=0.001,
        baseline_bid=99.0,
        baseline_ask=101.0,
        tick_size=0.1,
        max_pair_spread=10.0,
        best_bid=98.0,
        best_ask=102.0,
        volatility_bps=3.0,
        refill_edge=0.1,
        repair_probability=0.9,
    )

    assert decision.effective_half_life_s < cfg.response_half_life_s


def test_pair_spread_cap_clamps_a_without_changing_i_shift():
    response = PostFillQuoteResponse(_config(HYBRID_MODE))
    response.record_fill(
        side="BUY",
        inventory_before=0.001,
        inventory_after=0.002,
        fill_qty=0.001,
        order_size=0.001,
        ts_ms=0,
    )
    decision = response.quote(
        now_ms=0,
        inventory=0.002,
        order_size=0.001,
        baseline_bid=99.0,
        baseline_ask=101.0,
        tick_size=0.1,
        max_pair_spread=2.1,
        best_bid=98.0,
        best_ask=102.0,
        volatility_bps=3.0,
        refill_edge=0.0,
        repair_probability=0.5,
    )

    assert decision.inventory_shift_ticks == 1
    assert decision.raw_add_widen_ticks == 2
    assert decision.add_widen_ticks == 1
    assert decision.cap_limited
    assert decision.ask_price == pytest.approx(100.9)
    assert decision.bid_price == pytest.approx(98.8)


def test_disabled_response_is_exact_noop():
    response = PostFillQuoteResponse()
    decision = _quote(response, now_ms=1_000, inventory=0.004)

    assert not decision.active
    assert decision.bid_price == 99.0
    assert decision.ask_price == 101.0


def test_expected_adverse_amplitude_is_distance_capped():
    response = PostFillQuoteResponse(
        PostFillQuoteResponseConfig(
            enabled=True,
            mode=FLOW_ADD_WIDEN_MODE,
            flow_amplitude_mode="expected_adverse",
            flow_expected_adverse_buy_ticks=48.0,
            flow_expected_adverse_sell_ticks=47.0,
            flow_add_distance_fraction_buy=0.10,
            flow_add_distance_fraction_sell=0.20,
            flow_max_ticks=100.0,
            response_half_life_s=9.0,
            volatility_weight=0.0,
            refill_weight=0.0,
            repair_probability_weight=0.0,
        )
    )
    assert response.record_fill(
        side="BUY",
        inventory_before=0.0,
        inventory_after=0.001,
        fill_qty=0.001,
        order_size=0.001,
        ts_ms=1_000,
    )

    decision = _quote(response, now_ms=1_000, inventory=0.001)

    assert decision.flow_amplitude_mode == "expected_adverse"
    assert decision.baseline_add_distance_ticks == 10
    assert decision.raw_add_widen_ticks == 1
    assert decision.add_widen_ticks == 1
    assert decision.bid_price == pytest.approx(98.9)
    assert decision.ask_price == pytest.approx(101.0)
