"""Full-calendar scope must coexist with, not relabel, the original fixed panel."""
from copy import deepcopy
from datetime import date, timedelta

import pytest

from research.families.f01_fixed_parameter_racing.mechanism_ablation import FULL_EXPERIMENT, select_full_parents
from research.families.f01_fixed_parameter_racing.mechanism_execution import FULL_PARENTS, identity
from tests.test_mechanism_execution import spec
from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_defense_urgency_execution import options


def calendar():
    result = {'groups': {'Development': [], 'Final': []}}
    for i, (name, _) in enumerate(FULL_PARENTS.items()):
        start = date(2025, 8, 1)+timedelta(days=2*i)
        days = 1 if i == 203 else 2
        result['groups']['Development' if i < 150 else 'Final'].append(dict(
            shard_id=name, original_parent_index=i if i < 150 else i-150,
            account_start_utc=f'{start}T00:00:00Z',
            account_end_utc_exclusive=str(start+timedelta(days=days)), duration_hours=24*days,
            calendar_days=[str(start+timedelta(days=j)) for j in range(days)]))
    return result


def test_all_original_dates_and_tail_no_extra_seeds_or_combination():
    parents = select_full_parents(calendar())
    assert len(parents) == 204 and sum(p['duration_hours'] for p in parents) == 407*24
    assert len(FULL_PARENTS) == 204
    for parent, (start, end) in FULL_PARENTS.items():
        cell = dict(spec(), experiment_id=FULL_EXPERIMENT, account_contract='MECHANISM407_INDEPENDENT',
                    parent_id=parent, start_ns=start, end_ns=end)
        assert identity(cell) == cell
        for patch in [{'arm_id': 'A_PLUS_B'}, {'end_ns': end+1}, {'seed_pair': {'rng_seed': 1, 'latency_seed': 59}}]:
            with pytest.raises(ValueError):
                identity({**cell, **patch})
    assert identity(spec()) == spec()


@pytest.mark.parametrize('change', ['missing', 'wrong_id', 'wrong_time', 'tail_48h', 'duplicate'])
def test_reject_calendar_substitution(change):
    c = deepcopy(calendar())
    if change == 'missing':
        c['groups']['Final'].pop()
    elif change == 'wrong_id':
        c['groups']['Final'][0]['shard_id'] = 'new-name'
    elif change == 'wrong_time':
        c['groups']['Development'][0]['account_start_utc'] = '2025-08-01T01:00:00Z'
    elif change == 'tail_48h':
        c['groups']['Final'][-1]['duration_hours'] = 48
    else:
        c['groups']['Final'][-1] = deepcopy(c['groups']['Final'][0])
    with pytest.raises(ValueError):
        select_full_parents(c)


@pytest.mark.parametrize('arm', ['B0', 'GENERIC_MARKOUT_POLICY_OFF', 'DIRECT_DIRECTION_ASYM_OFF'])
def test_complete_24h_tail_contract(synthetic_parent, tmp_path, monkeypatch, arm):  # noqa: F811
    import research.families.f01_fixed_parameter_racing.mechanism_execution as execution
    prepared, base = synthetic_parent
    kw = options(prepared, base, tmp_path/'tail', arm)
    start, end = kw['spec']['start_ns'], kw['spec']['end_ns']
    parent = 'final-053-2026-09-11'
    monkeypatch.setattr(execution, 'FULL_PARENTS', {parent: (start, end)})
    monkeypatch.setattr(execution, 'DAY_NS', end-start)
    kw['spec'] = dict(spec(), experiment_id=FULL_EXPERIMENT, account_contract='MECHANISM407_INDEPENDENT',
                     parent_id=parent, start_ns=start, end_ns=end, arm_id=arm,
                     input_manifest_id=prepared.manifest_id)
    kw.pop('checkpoint_cuts')
    kw['common_params'].update(asym_strength=.1, markout_spread_scale=.2,
                              response_observe_only=True, response_update_branch_enabled=True)
    assert execution.execute_cell(prepared, **kw)['status'] == 'COMPLETE'
    assert execution.validate_cell(kw['output'], kw['spec'])['economic_complete']
    assert len(execution._load(kw['output']/'daily-ledger.json')['rows']) == 1
    assert execution._load(kw['output']/'diagnostics.json')['schema'] == 'mechanism407_diagnostics.v1'
