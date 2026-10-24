"""Small contract tests only; no market replay, training or task dispatch."""
from copy import deepcopy
from dataclasses import replace
from datetime import date, timedelta

import pytest

from research.families.f01_fixed_parameter_racing.mechanism_ablation import (
    ARMS, MARKOUT_SWITCH, PARENT_INDICES, arm_parameters, select_parents,
)
from strategy.policy_guards import (
    CommonSidePolicyInput, POLICY_REASON_MARKOUT, evaluate_common_side_policy,
)
from test_defense_urgency import params


def parent():
    return dict(params(), asym_strength=.1, markout_spread_scale=.2,
                rng_seed=42, latency_seed=59, response_observe_only=True,
                response_update_branch_enabled=True, vol_blend=.5)


@pytest.mark.parametrize('arm', ARMS)
def test_exact_inheritance_and_independent_models(arm):
    p = parent()
    original = deepcopy(p)
    actual = arm_parameters(p, arm)
    expected = deepcopy(p)
    expected['exit_urgency_strength'] = 0.
    if arm == ARMS[1]:
        expected[MARKOUT_SWITCH] = False
    elif arm == ARMS[2]:
        expected['asym_strength'] = 0.
    assert actual == expected
    actual['response_action_policy']['compute']['event_ns'] = 1
    assert p == original


@pytest.mark.parametrize('patch', [
    {'exit_urgency_strength': 0.}, {'asym_strength': 0.},
    {'markout_spread_scale': 0.}, {'rng_seed': 43},
    {'response_update_branch_enabled': False}, {MARKOUT_SWITCH: True},
])
def test_reject_changed_parent(patch):
    with pytest.raises(ValueError):
        arm_parameters(dict(parent(), **patch), 'B0')


def test_no_extra_arm():
    with pytest.raises(ValueError):
        arm_parameters(parent(), 'A_PLUS_B')


def test_direction_parameter_maps_without_removing_other_predictions():
    from dataclasses import asdict
    from strategy.quote_core import quote_core_config_from_params
    from test_defense_urgency_quote_wiring import fixture
    _, cfg, _, _ = fixture()
    p = dict(asdict(cfg), asym_strength=.1, exit_urgency_strength=0., vol_blend=.5)
    kwargs = dict(tick_size=.1, lot_size=.001, use_ml=True,
                  use_depth_weighted_mid_proxy=True, use_depth_liquidity_scaling=True)
    original = quote_core_config_from_params(p, **kwargs)
    candidate = quote_core_config_from_params(dict(p, asym_strength=0.), **kwargs)
    assert candidate == replace(original, asym_strength=0.)


@pytest.mark.parametrize('guards', [
    {}, {'fill_cooldown_active': True}, {'inventory_ratio': 1.},
    {'depth_age_s': 5., 'max_book_age_s': 2.},
    {'side_adverse': True, 'side_adverse_pause': True},
    {'defense_guard': True, 'defense_pause': True, 'defense_spread_mult': 1.5},
    {'local_extreme_guard': True, 'local_extreme_pause': True},
    {'l2_quote_flip_rate': .5, 'l2_book_cancel_ratio': .1,
     'weighted_mid_proxy_shift_bps': 1.},
])
def test_only_generic_contribution_removed(guards):
    inputs = CommonSidePolicyInput(exposure_increasing=True, markout_ema=-2.,
                                  markout_spread_scale=.2, **guards)
    before = deepcopy(inputs)
    control = evaluate_common_side_policy(inputs)
    assert control == evaluate_common_side_policy(inputs, generic_markout_enabled=True)
    removed = evaluate_common_side_policy(inputs, generic_markout_enabled=False)
    assert removed == evaluate_common_side_policy(replace(inputs, markout_ema=0.))
    assert not removed.reason_mask & POLICY_REASON_MARKOUT
    assert control.allow_post == removed.allow_post
    assert control.allow_exposure_increase == removed.allow_exposure_increase
    assert inputs == before


@pytest.mark.parametrize('bad', [None, 0, 1, 'false'])
def test_strict_switch(bad):
    with pytest.raises(ValueError):
        evaluate_common_side_policy(CommonSidePolicyInput(True), generic_markout_enabled=bad)
    from models.backtest_tick import simulate_tick
    with pytest.raises(ValueError, match=MARKOUT_SWITCH):
        simulate_tick(None, None, None, {MARKOUT_SWITCH: bad})


def test_native_full_replay_fails_closed():
    from models.backtest_tick import _simulate_tick_cpp
    with pytest.raises(NotImplementedError, match='generic markout'):
        _simulate_tick_cpp(None, None, None, {MARKOUT_SWITCH: False})


def test_switch_is_bound_into_recovery_identity():
    from models.replay.runtime_checkpoint_io import public_checkpoint_binding
    control = arm_parameters(parent(), 'B0')
    ablation = arm_parameters(parent(), ARMS[1])
    assert public_checkpoint_binding('test-input', control, None) != public_checkpoint_binding(
        'test-input', ablation, None)


@pytest.mark.parametrize('enabled', [True, False])
def test_actual_nested_replay_consumer_wiring(enabled):
    # Exercise the actual nested consumer without starting a market replay.
    import ast
    from pathlib import Path
    from types import SimpleNamespace
    import models.backtest_tick as replay
    tree = ast.parse(Path(replay.__file__).read_text())
    nodes = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
             and n.name == '_replay_common_side_policy']
    assert len(nodes) == 1
    from strategy.inventory_soft_control import cooldown_blocks_action
    namespace = dict(generic_markout_enabled=enabled,
                     inventory_cooldown_mode="original", inventory_markout_scope="original_all_roles",
                     inventory_cooldown_blocks_action=cooldown_blocks_action,
                     _tick_state=SimpleNamespace(
                         mo_ema_bid=-2., mo_ema_ask=-2., cur_tox_bid=.5, cur_tox_ask=.5,
                         q=0., t=0, max_inv=.026, max_exec_book_age_ms=2000,
                         markout_spread_scale=.2, mo_ref=1., thin_depth_threshold=0.,
                         depth_liquidity_baseline=50.),
                     _fill_cooldown_active=lambda *args, **kwargs: False,
                     CommonSidePolicyInput=CommonSidePolicyInput,
                     evaluate_common_side_policy=evaluate_common_side_policy)
    import math
    namespace['math'] = math
    exec(compile(ast.Module(body=nodes, type_ignores=[]), replay.__file__, 'exec'), namespace)
    ctx = {'depth_age_s': 0.}
    result, spread = namespace['_replay_common_side_policy']('BUY', ctx)
    assert ctx['policy_markout'] is enabled
    assert spread == (1.30 if enabled else 1.)
    assert result.size_mult == (.50 if enabled else 1.)


def calendar():
    rows = []
    for i in range(150):
        start = date(2025, 8, 1) + timedelta(days=2*i)
        rows.append(dict(shard_id=f'development-{i:03d}-{start}', original_parent_index=i,
                         account_start_utc=f'{start}T00:00:00Z',
                         account_end_utc_exclusive=str(start + timedelta(days=2)),
                         duration_hours=48,
                         calendar_days=[str(start), str(start + timedelta(days=1))]))
    return {'groups': {'Development': rows}}


def test_panel_is_date_only_and_defensive_copy():
    c = calendar()
    c['groups']['Development'].reverse()
    selected = select_parents(c)
    assert tuple(p['original_parent_index'] for p in selected) == PARENT_INDICES
    assert PARENT_INDICES == (6, 18, 31, 43, 56, 68, 81, 93, 106, 118, 131, 143)
    selected[0]['calendar_days'].clear()
    assert all(len(p['calendar_days']) == 2 for p in c['groups']['Development'])


@pytest.mark.parametrize('kind', ['missing', 'gap', 'duplicate', 'index'])
def test_no_replacement_for_unavailable_parent(kind):
    c = calendar()
    rows = c['groups']['Development']
    if kind == 'missing':
        rows.pop()
    elif kind == 'gap':
        rows[10]['account_end_utc_exclusive'] = '2025-09-01'
    elif kind == 'duplicate':
        rows[10]['shard_id'] = rows[9]['shard_id']
    else:
        rows[10]['original_parent_index'] = 0
    with pytest.raises(ValueError):
        select_parents(c)
