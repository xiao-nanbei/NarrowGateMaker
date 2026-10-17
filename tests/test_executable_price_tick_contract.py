import math

import numpy as np
import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

from models import backtest_tick as bt  # noqa: E402


@pytest.mark.parametrize("tick_size", [0.001, 0.01, 0.05, 0.1, 0.5])
def test_python_cpp_price_tick_identity_matches_across_tick_sizes(tick_size):
    price_tick = 647_146
    reconstructed = float(price_tick) * float(tick_size)
    parsed_lower = float(np.nextafter(reconstructed, -math.inf))
    parsed_upper = float(np.nextafter(reconstructed, math.inf))

    for value in (reconstructed, parsed_lower, parsed_upper):
        assert bt._price_to_tick(value, tick_size) == price_tick
        assert narrowgate_cpp.price_to_tick(value, tick_size) == price_tick
        assert narrowgate_cpp.same_price_tick(value, reconstructed, tick_size)


def test_sell_exact_price_trade_crosses_despite_one_ulp_representation_gap():
    tick_size = 0.1
    order_price = 647_146 * tick_size
    parsed_trade_price = float("64714.6")

    assert parsed_trade_price < order_price
    assert not (parsed_trade_price >= order_price)
    assert bt._trade_crosses_order_tick(
        "SELL", parsed_trade_price, order_price, tick_size
    )
    assert narrowgate_cpp.trade_crosses_order_ticks(
        narrowgate_cpp.Side.Sell,
        parsed_trade_price,
        order_price,
        tick_size,
    )


def test_adjacent_tick_is_not_collapsed_into_exact_price_boundary():
    tick_size = 0.1
    order_tick = 647_146
    order_price = order_tick * tick_size
    lower_trade = (order_tick - 1) * tick_size
    upper_trade = (order_tick + 1) * tick_size

    assert not bt._trade_crosses_order_tick(
        "SELL", lower_trade, order_price, tick_size
    )
    assert not narrowgate_cpp.trade_crosses_order_ticks(
        narrowgate_cpp.Side.Sell,
        lower_trade,
        order_price,
        tick_size,
    )
    assert not bt._trade_crosses_order_tick(
        "BUY", upper_trade, order_price, tick_size
    )
    assert not narrowgate_cpp.trade_crosses_order_ticks(
        narrowgate_cpp.Side.Buy,
        upper_trade,
        order_price,
        tick_size,
    )


@pytest.mark.parametrize("price", [64714.65, math.nan, math.inf, -1.0, 0.0])
def test_off_grid_or_invalid_executable_price_fails_closed(price):
    with pytest.raises(ValueError):
        bt._price_to_tick(price, 0.1)
    with pytest.raises((ValueError, OverflowError)):
        narrowgate_cpp.price_to_tick(price, 0.1)
