import pytest

from research.families.f01_fixed_parameter_racing.mechanism_report import compare_opportunities,complete_panel,RISK_CONTRACT


def row(key,action='place',side='BUY',negative=False,asym=0):
    return {'event_type':'decision','side':side,'record':{
        'quote_snapshot_market_generation':key,'action':action,
        'generic_markout_negative_condition':negative,'raw_asym_shift':asym}}


def test_repeated_own_callbacks_and_noncommon_support_remain_explicit():
    result=compare_opportunities([row('a'),row('a','keep',negative=True),row('b')],
        [row('a','wait',asym=.1),row('c'),row('',side='SELL')])
    assert result['common_keys']==1 and result['union_keys']==3
    assert result['left_only_keys']==result['right_only_keys']==1
    assert result['common_action_set_disagreements']==1
    assert result['common_callback_count_disagreements']==1
    assert result['left_negative_markout_callbacks']==1
    assert result['right_nonzero_asym_callbacks']==1
    assert result['right_production']['decision_key_unknown']==1


def test_side_is_part_of_common_key():
    result=compare_opportunities([row('a',side='BUY')],[row('a',side='SELL')])
    assert result['common_keys']==0 and result['union_keys']==2


def test_partial_panel_cannot_be_complete():
    with pytest.raises(ValueError):complete_panel([])


def test_risk_units_do_not_relabel_engine_drawdown_as_funding_equity():
    assert RISK_CONTRACT['abs_inventory_time_s'].startswith('BTC seconds')
    assert RISK_CONTRACT['sq_inventory_time_s'].startswith('BTC squared seconds')
    assert 'not relabelled' in RISK_CONTRACT['max_drawdown']
    assert 'censored' in RISK_CONTRACT['inventory_lifecycle_max_duration_s']
