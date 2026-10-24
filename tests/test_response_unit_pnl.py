import pytest

from research.families.f08_side_taker_lifecycle.unit_pnl import paired_comparison, unit_returns


def row(p, q, n, name='synthetic'):
    return dict(parent_account_id=name, start_ns=0, end_ns=1, scenario_id='synthetic',
                economic_complete=True, net_pnl_usdc=p, volume_btc=q, turnover_usdc=n)


@pytest.mark.parametrize('candidate', [row(-70, 5, 500000), row(-105, 11, 1100000)])
def test_less_loss_or_better_ratio_alone_does_not_pass(candidate):
    result = paired_comparison([row(-100, 10, 1000000)], [candidate])
    assert not result['all_three_point_estimates_improved']


def test_ratio_of_sums_keeps_zero_volume_pnl():
    rows = [row(-10, 1, 100000, 'one'), row(2, 0, 0, 'inventory_mtm')]
    result = paired_comparison(rows, rows)
    assert result['baseline']['net_pnl_per_btc'] == -8
    assert result['baseline']['net_pnl_per_10000_turnover'] == -.8
    assert unit_returns(2, 0, 0)['net_pnl_per_btc'] is None


def test_partial_fill_segmentation_does_not_change_denominators():
    whole = [(2, 100)]
    split = [(.5, 100), (1.5, 100)]
    def metrics(fills):
        return unit_returns(-1, sum(q for q, _ in fills), sum(q*p for q, p in fills))
    assert metrics(whole) == metrics(split)


def test_positive_point_estimate_is_not_profit_or_information_proof():
    result = paired_comparison([row(-100, 10, 1000000)], [row(-90, 10, 1000000)])
    assert result['all_three_point_estimates_improved']
    assert result['candidate']['net_pnl_usdc'] < 0
    assert result['information_increment'] == result['risk_acceptance'] == 'not_assessed'


def test_no_silent_intersection_or_incomplete_accounts():
    with pytest.raises(ValueError, match='identical'):
        paired_comparison([row(1, 1, 100, 'one')], [row(1, 1, 100, 'two')])
    bad = dict(row(1, 1, 100), economic_complete=False)
    with pytest.raises(ValueError, match='incomplete'):
        paired_comparison([bad], [bad])
    with pytest.raises(ValueError, match='duplicate'):
        paired_comparison([row(1, 1, 100)]*2, [row(1, 1, 100)])


@pytest.mark.parametrize('values', [(float('nan'), 1, 100), (1, 0, 100), (1, -1, 100)])
def test_missing_or_invalid_is_not_zero(values):
    with pytest.raises(ValueError):
        unit_returns(*values)
