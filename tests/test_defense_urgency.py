"""Offline admission tests; these do not dispatch replay or mutate live."""
from copy import deepcopy

import pytest

from research.families.f01_fixed_parameter_racing.defense_urgency import (
    BASELINE, CONDITIONAL, PATCHES, RESPONSE_PATH, aggregate, arm_parameters,
    baseline_completion_gate, choose_from_b, conditional_removal,
)
from strategy.response_action_features import FEATURES


def params():
    size = len(FEATURES)
    return dict(
        account_start='parent-specific', defense_guard_enabled=True,
        defense_pause=True, exit_urgency_strength=0.5,
        response_action_policy=dict(
            path_id=RESPONSE_PATH,
            compute=dict(schema='response_compute.v1', event_ns=109169,
                         call_ns=195875, common_to_both_arms=True),
            artifact=dict(scope='modeled_action_labels_not_account_pnl', final_used=False,
                          models={RESPONSE_PATH: dict(features=list(FEATURES),
                                  means=[0.] * size, scales=[1.] * size,
                                  coefficients=[0.] * size, intercept=0.,
                                  roles=['opener', 'add', 'reducing'], alpha=10)})))


@pytest.mark.parametrize('arm', PATCHES)
def test_nine_fixed_arms_preserve_parent_and_model(arm):
    original = params()
    before = deepcopy(original)
    result, mode = arm_parameters(original, arm)
    assert original == before
    assert result == dict(before, **PATCHES[arm])
    assert mode == (arm if arm in CONDITIONAL else 'B0')
    result['response_action_policy']['artifact']['models'][RESPONSE_PATH]['means'][0] = 99
    assert original == before


@pytest.mark.parametrize('change', ['old_path', 'cost', 'alpha', 'features', 'guard'])
def test_reject_wrong_baseline(change):
    p = params()
    policy = p['response_action_policy']
    if change == 'old_path':
        policy['path_id'] = 'B0'
    elif change == 'cost':
        policy['compute']['event_ns'] += 1
    elif change == 'alpha':
        policy['artifact']['models'][RESPONSE_PATH]['alpha'] = 1
    elif change == 'features':
        policy['artifact']['models'][RESPONSE_PATH]['features'].reverse()
    else:
        p['defense_guard_enabled'] = False
    with pytest.raises(ValueError):
        arm_parameters(p, 'B0')


def removal(mode, **overrides):
    state = dict(potential_active=True, potential_pause=True, pure_reducing=True,
                 visible_state_consistent=True, visible_pnl=-1.)
    return conditional_removal(mode, **dict(state, **overrides))


@pytest.mark.parametrize('mode', CONDITIONAL)
@pytest.mark.parametrize('bad', [dict(pure_reducing=False), dict(pure_reducing=None),
                               dict(visible_state_consistent=False),
                               dict(visible_pnl=None), dict(visible_pnl=float('nan'))])
def test_unknown_or_cross_zero_never_releases(mode, bad):
    result = removal(mode, **bad)
    assert not any((result.defense_all, result.defense_pause, result.urgency))


@pytest.mark.parametrize('pnl', [0., 1.])
@pytest.mark.parametrize('mode', CONDITIONAL[:2])
def test_loss_is_strict(mode, pnl):
    assert not any(vars(removal(mode, visible_pnl=pnl)).values())


def test_contributions_are_separate_and_upstream():
    assert vars(removal(CONDITIONAL[0])) == dict(defense_all=False, defense_pause=True, urgency=False)
    assert vars(removal(CONDITIONAL[1])) == dict(defense_all=True, defense_pause=False, urgency=False)
    assert vars(removal(CONDITIONAL[2], visible_pnl=1.)) == dict(defense_all=False, defense_pause=False, urgency=True)
    assert not removal(CONDITIONAL[2], potential_pause=False).urgency
    assert not removal(CONDITIONAL[2], potential_active=False).urgency


def calendar():
    ids = [f'parent{i:03}' for i in range(204)]
    return dict(groups=dict(Development=[dict(shard_id=p) for p in ids[:150]],
                            Final=[dict(shard_id=p) for p in ids[150:]]),
                analysis_views=dict(B_SELECTION_PARENTS=dict(parent_ids=ids[:25])))


def admitted():
    return [dict(parent_id=f'parent{i:03}', baseline_id=BASELINE,
                 source_path_id=RESPONSE_PATH, economics_status='COMPLETE',
                 binding_verified=True, terminal_cut_verified=True,
                 accounting_verified=True, source_result_ref=f'result/{i}')
            for i in range(204)]


def test_gate_requires_all204_verified_new_baseline():
    rows = admitted()
    assert baseline_completion_gate(calendar(), rows)['ready']
    assert not baseline_completion_gate(calendar(), rows[:169])['ready']
    for field, value in [('baseline_id', 'old_B0'), ('source_path_id', 'B0'),
                         ('economics_status', 'RUNNING'), ('binding_verified', False),
                         ('terminal_cut_verified', False), ('accounting_verified', False),
                         ('source_result_ref', '')]:
        altered = deepcopy(rows)
        altered[0][field] = value
        assert baseline_completion_gate(calendar(), altered)['verified'] == 203
    with pytest.raises(ValueError):
        baseline_completion_gate(calendar(), rows + [rows[0]])


def selection():
    return [dict(parent_id=f'parent{i:03}', arm_id=a, economics_status='COMPLETE',
                 net_pnl=-1. if a == 'B0' else -2., volume_btc=1., notional_usdc=100.,
                 hard_risk_contract_unchanged=True)
            for i in range(25) for a in PATCHES]


def test_negative_challenger_is_frozen_even_without_quality_winner():
    result = choose_from_b(calendar(), selection())
    assert result['B_raw_winner'] == 'B0'
    assert result['frozen_challenger'] == 'DEFENSE_OFF__URGENCY_OFF'
    assert result['B_quality_winner'] is None


@pytest.mark.parametrize('change', ['C', 'duplicate', 'missing', 'running'])
def test_selection_cannot_read_other_rewards_or_partial_grid(change):
    rows = selection()
    if change == 'C':
        rows[0]['parent_id'] = 'parent113'
    elif change == 'duplicate':
        rows.append(rows[0])
    elif change == 'missing':
        rows.pop()
    else:
        rows[0]['economics_status'] = 'RUNNING'
    with pytest.raises(ValueError):
        choose_from_b(calendar(), rows)


def test_ratio_of_sums_and_zero_turnover():
    rows = [dict(net_pnl=1., volume_btc=1., notional_usdc=100.),
            dict(net_pnl=1., volume_btc=9., notional_usdc=900.)]
    assert aggregate(iter(rows))['eta_Q'] == .2
    assert aggregate(rows)['eta_N'] == 20.
    assert aggregate([])['eta_Q'] is None
    rows[0]['volume_btc'] = -1.
    with pytest.raises(ValueError):
        aggregate(rows)
