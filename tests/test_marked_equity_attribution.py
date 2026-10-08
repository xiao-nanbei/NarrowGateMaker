import pytest

from models.replay.continuous_accounting import marked_equity_change
from research.families.f03_causal_13_head.time_weighted_evaluation import _row_pnl


def state(t, cash, inventory, price, clock=None):
    return dict(boundary_ts_ms=t, cash_usdc=cash, inventory_btc=inventory,
                mark_price=price, mark_clock_ts_ms=t-1 if clock is None else clock)


def change(a, b, fee=0, funding=0, age=1000):
    return marked_equity_change(a, b, fees_usdc=fee,
                                funding_cashflow_usdc=funding, max_mark_age_ms=age)


def test_carried_risk_is_marked_without_next_day_closing_profit():
    # Old useful test: half the inventory sold at102; remainder marked at103.
    # A next-day close at104 must not be credited to this interval.
    a, b = state(1000, -100, 1, 101), state(2000, -49, .5, 103)
    assert change(a, b)["net_equity_change_usdc"] == pytest.approx(1.5)
    c = state(3000, 3, 0, 104)
    assert change(b, c)["net_equity_change_usdc"] == pytest.approx(.5)


def test_two_days_telescope_with_fees_and_funding_once():
    a = state(1000, 0, 0, None)
    b = state(2000, -102, 1, 105)  # buy100, fee1, funding-1 already in cash
    c = state(3000, -48.5, .5, 108)  # sell .5@110, fee.5, funding-1
    first, second = change(a, b, 1, -1), change(b, c, .5, -1)
    whole = change(a, c, 1.5, -2)
    assert first["net_equity_change_usdc"] + second["net_equity_change_usdc"] == whole["net_equity_change_usdc"]
    assert whole["net_equity_change_usdc"] == 5.5
    assert not whole["terminal_liquidation_applied"]


@pytest.mark.parametrize("clock", [0, 2000, 2001])
def test_stale_equal_time_or_future_mark_is_not_usable(clock):
    result = change(state(1000, 0, 0, None), state(2000, -100, 1, 105, clock))
    assert result["net_equity_change_usdc"] is None
    assert not result["economic_complete"]


def test_unknown_funding_is_not_zero_and_flat_needs_no_mark():
    a, b = state(1000, 0, 0, None), state(2000, 2, 0, None)
    assert change(a, b, funding=None)["net_equity_change_usdc"] is None
    assert change(a, b)["net_equity_change_usdc"] == 2


def test_f03_consumer_enforces_declared_mark_age():
    row = dict(start_ts_ms=1000, end_ts_ms_exclusive=2000,
               start_cash_usdc=0, start_inventory_btc=0, start_equity_usdc=0,
               start_mark_price=None, start_mark_clock_ts_ms=None,
               end_cash_usdc=-100, end_inventory_btc=1, end_equity_usdc=5,
               end_mark_price=105, end_mark_clock_ts_ms=1999,
               net_equity_change_usdc=5, fees_usdc=0, funding_cashflow_usdc=0,
               max_mark_age_ms=10)
    assert _row_pnl(row) == 5
    row["end_mark_clock_ts_ms"] = 1900
    with pytest.raises(ValueError, match="incomplete"):
        _row_pnl(row)
