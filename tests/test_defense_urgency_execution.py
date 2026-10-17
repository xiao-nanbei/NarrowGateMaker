from copy import deepcopy

import pytest

from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_defense_urgency import params as policy_params
from tests.test_python_planned_maintenance_replay import _async_fifo_params
from research.families.f01_fixed_parameter_racing.defense_urgency_execution import (
    execute_cell, validate_cell, cell_lock,
)
from research.families.f01_fixed_parameter_racing.defense_urgency import BASELINE, PATCHES


def options(prepared, base, output, arm):
    policy = policy_params()
    policy.pop('account_start')
    policy['response_action_policy']['compute']['measurement_receipt'] = 'synthetic-only'
    params = {**base, **_async_fifo_params(), **policy,
              'replace_terminal_continuation': True,
              '_pre_snapshot_compute_latency_samples_ms': [1.],
              '_decision_to_gateway_latency_samples_ms': [2.]}
    plan = prepared.inputs['bundle'].manifest['plan']
    params.update(account_start_ns=plan['start_ns'], rng_seed=42, latency_seed=59)
    spec = dict(experiment_id='B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407',
        baseline_id=BASELINE, account_contract='DEVELOPMENT300_ACCOUNT_PANEL',
        parent_id='synthetic-only', arm_id=arm, seed_pair=dict(rng_seed=42, latency_seed=59),
        start_ns=plan['start_ns'], end_ns=plan['end_ns'], input_manifest_id=prepared.manifest_id,
        release_identity='synthetic-not-production')
    funding = dict(market_id=plan['market_id'], source_identity='synthetic',
        coverage_start_ns=plan['start_ns'], coverage_end_ns=plan['end_ns'],
        events=[], expected_settlements_ns=[])
    return dict(spec=spec, common_params=params, prepared_predictions=None,
        initial_capital=10000., max_mark_age_ns=100*10**9, funding=funding,
        output=output, checkpoint_cuts=[10000])


@pytest.mark.parametrize('arm', PATCHES)
def test_real_cell_is_atomic_idempotent_and_drops_completed_checkpoint(synthetic_parent, tmp_path, arm):  # noqa: F811
    prepared, base = synthetic_parent
    kwargs = options(prepared, base, tmp_path/'cell', arm)
    assert execute_cell(prepared, **kwargs)['status'] == 'COMPLETE'
    receipt = validate_cell(kwargs['output'], kwargs['spec'])
    assert receipt['economic_complete'] is True
    assert not (kwargs['output']/'latest.checkpoint').exists()
    assert execute_cell(prepared, **kwargs)['status'] == 'ALREADY_COMPLETE'
    changed = deepcopy(kwargs)
    changed['common_params']['maker_fee'] = .0001
    with pytest.raises(ValueError, match='parameters changed'):
        execute_cell(prepared, **changed)
    (kwargs['output']/'metrics.json').write_text('{}')
    with pytest.raises(ValueError, match='payload changed'):
        validate_cell(kwargs['output'], kwargs['spec'])


def test_crash_after_durable_cut_resumes_same_cell(synthetic_parent, tmp_path):  # noqa: F811
    prepared, base = synthetic_parent
    kwargs = options(prepared, base, tmp_path/'resume', 'DEFENSE_ALL_RELEASE_ON_LOSS')

    def crash(status):
        raise RuntimeError('simulated disconnect')

    with pytest.raises(RuntimeError, match='disconnect'):
        execute_cell(prepared, **kwargs, progress=crash)
    assert (kwargs['output']/'latest.checkpoint').exists()
    assert not (kwargs['output']/'completion.json').exists()
    assert execute_cell(prepared, **kwargs)['status'] == 'COMPLETE'
    other = {**kwargs, 'output': tmp_path/'full'}
    execute_cell(prepared, **other)
    assert (kwargs['output']/'accounting.json').read_bytes() == (other['output']/'accounting.json').read_bytes()
    assert (kwargs['output']/'result.json').read_bytes() == (other['output']/'result.json').read_bytes()


def test_exclusive_ownership_and_reject_foreign_inputs(synthetic_parent, tmp_path):  # noqa: F811
    prepared, base = synthetic_parent
    kwargs = options(prepared, base, tmp_path/'cell', 'B0')
    with cell_lock(kwargs['output']):
        with pytest.raises(BlockingIOError):
            execute_cell(prepared, **kwargs)
    kwargs['spec']['input_manifest_id'] = 'wrong'
    with pytest.raises(ValueError, match='binding differs'):
        execute_cell(prepared, **kwargs)
