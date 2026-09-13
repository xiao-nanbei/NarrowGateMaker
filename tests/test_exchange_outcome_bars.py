from decimal import Decimal as D

from data.runtime import ExchangeOutcomeBars
from data.tardis_input import TradeExecution


def trade(price, quantity, timestamp):
    return TradeExecution("market", "synthetic", 1, 1, timestamp//1000, "unknown", None,
                          timestamp//1000, 1, "sell", D(price), D(quantity))


def test_outcomes_preserve_half_open_boundaries_and_empty_prices():
    bars = ExchangeOutcomeBars(0, "observed")
    assert not bars.advance(100, (trade("100", "2", 100),))
    first, = bars.advance(1_000_000_000, (trade("101", "3", 1_000_000_000),))
    assert first["close"] == D(100) and first["volume"] == D(2)
    second, empty = bars.advance(3_000_000_000)
    assert second["close"] == D(101) and second["turnover"] == D(303)
    assert empty["close"] is None and empty["volume"] == 0
    assert first["volume"]+second["volume"] == 5


def test_unknown_coverage_not_zero_or_claimed_complete():
    bars = ExchangeOutcomeBars(0, "unknown")
    bars.advance(100, (trade("100", "2", 100),))
    first, empty = bars.advance(2_000_000_000)
    assert first["close"] == 100 and first["volume"] is None
    assert empty["individual_count"] is None and empty["close"] is None
