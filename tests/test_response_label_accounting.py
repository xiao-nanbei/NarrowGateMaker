from types import SimpleNamespace

import pytest

from research.families.f08_side_taker_lifecycle.response_label_accounting import wealth_at, paired_labels
from research.families.f08_side_taker_lifecycle.response_label_accounting import wealth_at_many


def fill(ms, side, price, fee=0., quantity=1.):
    return dict(fill_ts=ms, side=side, quote_px=price, fill_fee_usdc=fee, fill_qty=quantity)


@pytest.mark.parametrize('seed', range(12))
def test_many_boundaries_exactly_match_scalar_reference(seed):
    import random
    rng = random.Random(seed)
    rows = [fill(rng.randrange(20), rng.choice(('BUY', 'SELL')),
                 rng.uniform(90, 110), rng.choice((-.01, 0., .02)),
                 rng.choice((.001, .002, .003))) for _ in range(100)]
    rows.sort(key=lambda row: row['fill_ts'])
    result = {'_fill_trace': rows}
    funding = {'events': [dict(settlement_ns=n*1_000_000, mark_price=100., rate=.001)
                          for n in (10, 0, 15, 10)]}
    marks = {n*1_000_000: (None if n % 3 == 0 else 100.) for n in range(22)}
    assert wealth_at_many(result, funding, marks) == {
        n: wealth_at(result, funding, n, mark) for n, mark in marks.items()}


def test_boundary_funding_precedes_equal_fill_and_notification_is_not_a_second_fill():
    result = dict(private_fill_visibility_enabled=True,
        _economic_fill_trace=[fill(1000, 'BUY', 100., -.1), fill(2000, 'SELL', 110.)],
        _fill_trace=[fill(3000, 'BUY', 100., -.1)])
    funding = {'events': [dict(settlement_ns=2_000_000_000, mark_price=100., rate=.01)]}
    before = wealth_at(result, funding, 2_000_000_000, 105.)
    assert before['wealth_usdc'] == pytest.approx(4.1)
    assert before['funding_usdc'] == -1.
    assert wealth_at(result, funding, 3_000_000_000, None)['wealth_usdc'] == pytest.approx(9.1)
    assert wealth_at(result, funding, 2_000_000_000, None)['wealth_usdc'] is None


def test_pair_removes_common_prefix_and_preserves_zero_labels():
    request = SimpleNamespace(decision_ns=2_000_000_000, execution_end_ns=122_000_000_000)
    keep = dict(_fill_trace=[fill(1000, 'BUY', 100.)],
        response_update_fork=dict(action='KEEP_EXISTING', decision_ns=request.decision_ns))
    update = dict(_fill_trace=[fill(1000, 'BUY', 100.), fill(8000, 'SELL', 106., .1)],
        response_update_fork=dict(action='UPDATE_TO_B0_TARGET', decision_ns=request.decision_ns))
    marks = {2_000_000_000: 104., 7_000_000_000: 105., 32_000_000_000: 103., 122_000_000_000: 102.}
    accounts = [dict(economic_complete=True, account_end_ns=request.execution_end_ns, all_in_net_pnl=p)
                for p in (2., 5.9)]
    value = paired_labels(keep, update, accounts, {'events': []}, request, marks,
                          day_end_ns=200_000_000_000, account_end_ns=200_000_000_000)
    assert value['labels']['5']['delta_usdc'] == 0.
    assert value['labels']['30']['keep_usdc'] == -1.
    assert value['labels']['30']['delta_usdc'] == pytest.approx(2.9)
    assert value['labels']['120']['delta_usdc'] == pytest.approx(3.9)
    update['_fill_trace'][0] = fill(1000, 'BUY', 101.)
    with pytest.raises(ValueError):
        paired_labels(keep, update, accounts, {'events': []}, request, marks,
                      day_end_ns=200_000_000_000, account_end_ns=200_000_000_000)
