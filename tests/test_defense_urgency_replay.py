from copy import deepcopy

import pytest

from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_python_planned_maintenance_replay import _async_fifo_params
from tests.test_defense_urgency import params as policy_params
from tests.test_target_observation import exact
from research.families.f01_fixed_parameter_racing.defense_urgency import PATCHES
from research.families.f01_fixed_parameter_racing.public_input import iter_defense_urgency_candidates


@pytest.mark.parametrize('arm', PATCHES)
def test_real_panel_entry_keeps_model_cost_and_recovery(synthetic_parent, arm):  # noqa: F811
    prepared, base = synthetic_parent
    params = dict(base, **_async_fifo_params(),
        replace_terminal_continuation=True,
        _pre_snapshot_compute_latency_samples_ms=[1.],
        _decision_to_gateway_latency_samples_ms=[2.])
    model_params = policy_params()
    model_params.pop('account_start')
    model_params['response_action_policy']['compute']['measurement_receipt'] = 'synthetic-test-only'
    params.update(model_params)
    plan = prepared.inputs['bundle'].manifest['plan']
    params.setdefault('account_start_ns',plan['start_ns'])
    before = deepcopy(params)
    funding = dict(market_id=plan['market_id'],source_identity='synthetic-test',
        coverage_start_ns=plan['start_ns'],coverage_end_ns=plan['end_ns'],
        events=[],expected_settlements_ns=[])
    kwargs = dict(common_params=params,prepared_predictions=None,
        initial_capital=10000.,max_mark_age_ns=100*10**9,funding=funding)
    _, full = next(iter_defense_urgency_candidates(prepared,[arm],**kwargs))
    _, resumed = next(iter_defense_urgency_candidates(
        prepared,[arm],checkpoint_cuts=[10000],**kwargs))
    exact(full,resumed)
    exact(params,before)
    assert full['accounting']['economic_complete'] is True
    counts=full['replay']['response_action_policy']['counts']
    assert counts['compute_calls']>0 and counts['charged_ms']>0
    assert full['effective_params']['response_action_policy']==params['response_action_policy']
