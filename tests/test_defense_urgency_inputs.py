from copy import deepcopy
from datetime import datetime, timezone

import pytest

from research.families.f01_fixed_parameter_racing.defense_urgency_inputs import continuous_plan, DAY_NS
from tests.test_defense_urgency_continuous import contract


def fixture_registry():
    start = int(datetime(2026, 5, 28, tzinfo=timezone.utc).timestamp())*10**9
    rows = {}
    for i in range(54):
        begin = start+i*2*DAY_NS
        end = min(begin+2*DAY_NS, start+107*DAY_NS)
        days = [datetime.fromtimestamp((begin//10**9)-86400+j*86400, timezone.utc).date().isoformat()
                for j in range(1+(end-begin)//DAY_NS)]
        parent = f'final-{i:03d}'
        events = [{'settlement_ns': t, 'rate': .0001, 'mark_price': 100.}
                  for t in range(begin, end+1, DAY_NS)]
        rows[parent] = dict(identity=dict(parent_id=parent, start_ns=begin, end_ns=end),
            params={'cross_market_enabled': False}, input_config=dict(
            observation=dict(start_ns=begin-120*10**9, end_ns=end, market_id='m',
                observation_profile={'depth_connection_id':'depth','trade_connection_id':'trade'},
                include_response_observations=True,
                source_bundles=[{'path':'old/'+d,'sha256':d} for d in days]),
            facts={d:{'manifest.json':12} for d in days},
            probe={'initial_capital':10000,'max_mark_age_ns':10**9},
            funding=dict(market_id='m', coverage_start_ns=begin, coverage_end_ns=end,
                source_identity=['funding'], events=events,
                expected_settlements_ns=[e['settlement_ns'] for e in events])))
    return {'parents':rows}


def test_calendar_funding_and_warmup_are_one_global_contract():
    r=fixture_registry(); original=deepcopy(r)
    p=continuous_plan(contract(),r,'/facts','/latency.json')
    assert len(p['facts'])==108
    assert p['end_ns']-p['start_ns']==107*DAY_NS
    assert p['warmup_ns']==120*10**9
    assert len(p['funding']['events'])==107
    assert p['observation']['source_bundles'][0]['path']=='/facts/2026-05-27'
    assert r==original


@pytest.mark.parametrize('fault', ['missing','overlap','profile','funding','source'])
def test_incomplete_or_conflicting_contract_fails(fault):
    r=fixture_registry(); row=r['parents']['final-001']
    if fault=='missing': del r['parents']['final-001']
    elif fault=='overlap': row['identity']['start_ns']-=1
    elif fault=='profile': row['input_config']['observation']['observation_profile']['trade_connection_id']='new'
    elif fault=='funding': row['input_config']['funding']['events'][0]['rate']=.2
    else: row['input_config']['observation']['source_bundles'][0]['sha256']='changed'
    with pytest.raises(ValueError):continuous_plan(contract(),r,'/facts','/latency.json')
