"""Current bounded labels, not historical local-cash inventory_lifecycle labels."""
from copy import deepcopy

import pytest

from models.replay.risk_selection import assemble_prefill_label, PREFILL_LABEL_CONTRACT
from research.families.f05_fill_quality_quote_ev.risk_selection_training import train_chronological_ridge


def pair():
    row = dict(opportunity_id='op', kind='E', side='BUY', order_id='', role='opener',
        decision_ts_ns=1_000_000_000, decision_sequence=1, feature_ready_ts_ns=900_000_000,
        selection_scope='visible_inventory', baseline_action='POST', action='POST',
        features={'x': 1.}, quantity_btc=.001, price=100.)
    end = 31_000_000_000
    base = dict(risk_selection_mode='B', risk_selection_control='learned',
        risk_selection_intervention_count=0, risk_selection_opportunity_counts={'E': 1, 'C': 0},
        _risk_selection_opportunities=[row], _economic_fill_trace=[], _fill_trace=[],
        private_fill_visibility_enabled=True, private_fill_pending_visibility_count=2,
        execution_window=dict(schema='public_bounded_execution.v1', completed=True,
            exclusive_end_ns=end, last_market_event_ns=end-1_000_000, first_unprocessed_event_ns=end),
        public_input_contract=dict(input_manifest_id='fixture', account_start_ns=0, account_end_ns=end))
    alt = deepcopy(base)
    alt['risk_selection_intervention_count'] = 1
    alt['_risk_selection_opportunities'][0]['action'] = 'WAIT'
    account = dict(account_end_ns=end, account_start_ns=0, economic_complete=True,
        accounting_contract='public_independent_mtm.v1', terminal_liquidation_applied=False,
        valuation_origin='delivered_BBO_mid_not_official_mark', valuation_price=100.,
        valuation_age_ns=100, initial_capital=10000., all_in_net_pnl=2., terminal_equity=10002.,
        fees=.1, funding_cashflow=-.2, fill_clock_basis='producer_matching_facts')
    other = {**account, 'all_in_net_pnl': 1., 'terminal_equity': 10001., 'funding_cashflow': -.5}
    fork = dict(parent_checkpoint_id='controlled-fixture', phase='baseline_intent_before_order_budget',
        opportunity_id='op', decision_ts_ns=1_000_000_000, execution_end_ns=end,
        decision_sequence=1, order_id='', economic_prefix_count=0, notification_prefix_count=0)
    base['prefill_fork'] = deepcopy(fork)
    alt['prefill_fork'] = deepcopy(fork)
    return base, alt, dict(intervention={'opportunity_id': 'op', 'action': 'WAIT'},
        baseline_accounting=account, alternative_accounting=other, fork=fork)


def test_prefill_uses_authoritative_pnl_without_double_funding_or_notification_veto():
    base, alt, kw = pair()
    label = assemble_prefill_label(base, alt, **kw)
    assert label['value_difference_usdc'] == 1.
    assert label['label_contract'] == PREFILL_LABEL_CONTRACT
    assert label['horizon_ns'] == 30_000_000_000
    assert label['additive_portfolio_return'] is False
    with pytest.raises(ValueError, match='explicitly selected training contract'):
        train_chronological_ridge([label], feature_units={'x': '1'}, validation_start_ns=10**15)
    _, report = train_chronological_ridge([label], feature_units={'x': '1'},
        validation_start_ns=10**15, min_train_rows=128, required_label_contract=PREFILL_LABEL_CONTRACT)
    assert report['surfaces']['E:BUY']['status'] == 'insufficient_training_rows'


@pytest.mark.parametrize('fault', ['cut', 'horizon', 'input', 'mark', 'incomplete', 'feature', 'action', 'prefix'])
def test_prefill_rejects_unproved_or_incomparable_branches(fault):
    base, alt, kw = pair()
    if fault == 'cut':
        alt['execution_window']['exclusive_end_ns'] += 1
    elif fault == 'horizon':
        kw['fork']['execution_end_ns'] += 1
    elif fault == 'input':
        alt['public_input_contract']['input_manifest_id'] = 'different'
    elif fault == 'mark':
        kw['alternative_accounting']['valuation_price'] += 1
    elif fault == 'incomplete':
        kw['alternative_accounting']['economic_complete'] = False
    elif fault == 'feature':
        alt['_risk_selection_opportunities'][0]['features']['x'] += 1
    elif fault == 'action':
        alt['_risk_selection_opportunities'][0]['action'] = 'POST'
    else:
        kw['fork']['economic_prefix_count'] = 1
    with pytest.raises(ValueError):
        assemble_prefill_label(base, alt, **kw)


def test_prefill_feature_units_signs_and_unknown_are_shared_pure_transforms():
    from strategy.risk_selection import prefill_features, PREFILL_FEATURE_UNITS

    state = dict(kind='C', decision_ns=10_000_000_000, market_ready_ns=9_999_000_000,
        tick_size=.1, lot_size=.001, best_bid=99., best_ask=101., bid_quantity=3., ask_quantity=1.,
        order_price=98.9, visible_inventory=-.002, other_same_side_pending=.001,
        opposite_side_pending=.003, absolute_price_variance_rate=4.,
        trade_imbalance_5000ms=.2, visible_mid_5000ms_ago=99., local_submit_ns=9_500_000_000)
    buy = prefill_features(side='BUY', **state)
    sell = prefill_features(side='SELL', **{**state, 'order_price': 101.1})
    assert set(buy) == set(PREFILL_FEATURE_UNITS)
    assert buy['side_l1_imbalance'] == .5 == -sell['side_l1_imbalance']
    assert buy['side_trade_imbalance_5000ms'] == .2 == -sell['side_trade_imbalance_5000ms']
    assert buy['adverse_mid_change_5000ms_bps'] == pytest.approx(-10000/99)
    assert sell['adverse_mid_change_5000ms_bps'] == -buy['adverse_mid_change_5000ms_bps']
    assert buy['volatility_rate_bps_per_sqrt_s'] == 200.
    assert buy['same_side_distance_ticks'] == pytest.approx(1.)
    assert buy['visible_inventory_lots'] == sell['visible_inventory_lots'] == -2.
    assert buy['local_order_age_ms'] == 500.
    missing = prefill_features(side='BUY', **{**state, 'kind': 'E',
        'trade_imbalance_5000ms': None, 'visible_mid_5000ms_ago': None})
    assert missing['side_trade_imbalance_5000ms'] is None
    assert missing['adverse_mid_change_5000ms_bps'] is None
    assert missing['local_order_age_ms'] == 0.
    assert all(x is None for x in prefill_features(side='BUY', **{**state,
        'market_ready_ns': state['decision_ns']+1}).values())


def test_suffix_sampling_uses_visible_identity_not_outcome():
    from research.families.f05_fill_quality_quote_ev.prefill_labels import OutcomeBlindSelection

    select = OutcomeBlindSelection(per_surface=2, spacing_ns=30_000_000_000,
        end_ns=200_000_000_000, feature_columns=['x'])
    def row(t, order, **extra):
        return dict(kind='C', side='BUY', decision_ts_ns=t*1_000_000_000,
                    order_id=order, features={'x': 1.}, **extra)
    rows = [row(1, 'a', future_pnl=999), row(2, 'b'), row(40, 'a'), row(41, 'b', future_pnl=-999)]
    assert select.select(rows) == [rows[0], rows[3]]
    assert select.select([row(100, 'c')]) == []


def test_prefix_unknown_diagnostic_is_equal_but_never_masks_economic_difference():
    from models.replay.risk_selection import _same_trace_value

    left = [{'fill_sequence': 0, 'fill_qty': .001, 'diagnostic': float('nan')}]
    right = [{'fill_sequence': 0, 'fill_qty': .001, 'diagnostic': float('nan')}]
    assert _same_trace_value(left, right)
    assert not _same_trace_value(left, [{**right[0], 'fill_qty': .002}])
    assert not _same_trace_value(left, [{**right[0], 'diagnostic': 0.}])
    assert not _same_trace_value(left, [{**right[0], 'diagnostic': None}])


def test_retained_selection_counts_against_time_stratified_budget():
    from research.families.f05_fill_quality_quote_ev.prefill_labels import OutcomeBlindSelection

    sampler = OutcomeBlindSelection(per_surface=2, spacing_ns=30_000_000_000,
        end_ns=200_000_000_000, feature_columns=['x'], sample_start_ns=0)
    old = dict(kind='C', side='BUY', order_id='old', decision_ts_ns=1_000_000_000,
               features={'x': 1.})
    sampler.restore_selected([old])
    assert not sampler.select([{**old, 'order_id': 'early', 'decision_ts_ns': 90_000_000_000}])
    assert not sampler.select([{**old, 'decision_ts_ns': 110_000_000_000}])
    assert sampler.select([{**old, 'order_id': 'new', 'decision_ts_ns': 110_000_000_000}])
