"""The new owner exercises the real shared executor, not a second replay loop."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_defense_urgency_execution import options
from tests.test_inventory_soft_control import baseline, calendar, spec
from research.families.f01_fixed_parameter_racing import inventory_soft_control_execution as runner
from research.families.f01_fixed_parameter_racing.inventory_soft_control import ARMS


@pytest.mark.parametrize('arm', ARMS[1:])
def test_new_owner_real_accounting_and_no_duplicate_execution(synthetic_parent, tmp_path, monkeypatch, arm):  # noqa: F811
    from research.families.f01_fixed_parameter_racing import mechanism_execution as shared
    prepared, base = synthetic_parent
    old = options(prepared, base, tmp_path/'cell', arm)
    params = dict(old['common_params'])
    overrides = baseline()
    overrides.pop('account_start', None)
    overrides.pop('response_action_policy')
    params.update(overrides)
    params = json.loads(json.dumps(params))  # Production originals are JSON artifacts.
    start, end = old['spec']['start_ns'], old['spec']['end_ns']
    ident = dict(spec(), arm_id=arm, start_ns=start, end_ns=end,
                 input_manifest_id=prepared.manifest_id)
    # Synthetic compressed clock only; production identity tests independently
    # enforce all 204 actual original intervals and the continuous boundary.
    def test_identity(value, original_calendar):
        assert original_calendar == calendar()
        if value != ident:
            raise ValueError('foreign synthetic identity')
        return deepcopy(value)
    monkeypatch.setattr(runner, 'identity', test_identity)
    monkeypatch.setattr(shared, 'DAY_NS', (end-start)//2)
    kw = dict(spec=ident, original_calendar=calendar(), original_b0_params=params,
        prepared_predictions=None, initial_capital=10000,
        max_mark_age_ns=old['max_mark_age_ns'], funding=old['funding'], output=old['output'])
    assert runner.execute_cell(prepared, **kw)['status'] == 'COMPLETE'
    assert runner.validate_cell(old['output'], ident,
        original_calendar=calendar(), original_b0_params=params)['economic_complete']
    assert runner.execute_cell(prepared, **kw)['status'] == 'ALREADY_COMPLETE'
    changed = deepcopy(params)
    changed['maker_fee'] = .0001
    with pytest.raises(ValueError, match='effective parameters differ'):
        runner.execute_cell(prepared, **dict(kw, original_b0_params=changed))


def test_r0_cannot_be_redispatched_as_a_new_formal_account():
    with pytest.raises(ValueError, match='only the four new candidates'):
        runner._admitted(dict(spec(), arm_id='B0'), calendar(), baseline(),
                         runner.INDEPENDENT_CONTRACT)


def test_new_owner_defers_completed_checkpoint_retirement_until_worker_exit(monkeypatch):
    captured = {}
    def bound(*args, **kwargs):
        captured.update(kwargs)
        return {'status':'BOUND_ONLY_NO_REPLAY'}
    monkeypatch.setattr(runner, 'execute_bound_cell', bound)
    result = runner.execute_cell(None, spec=dict(spec(),arm_id=ARMS[1]),
        original_calendar=calendar(), original_b0_params=baseline(),
        prepared_predictions=None, initial_capital=10000, max_mark_age_ns=10**9,
        funding={}, output='not-created')
    assert result['status'] == 'BOUND_ONLY_NO_REPLAY'
    assert captured['retain_completed_checkpoint'] is True
    assert captured['incremental_journal'] is True


def test_old_identity_rejected_before_input_or_output_access(tmp_path):
    with pytest.raises(ValueError, match='identity differs'):
        runner.execute_cell(None, spec=dict(spec(), experiment_id='B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407'),
            original_calendar=calendar(), original_b0_params=baseline(),
            prepared_predictions=None, initial_capital=10000, max_mark_age_ns=10**9,
            funding={}, output=tmp_path/'must-not-exist')
    assert not (tmp_path/'must-not-exist').exists()


@pytest.mark.parametrize('mutation, message', [
    ({'cooldown_blocks_action': True}, 'latent cooldown'),
    ({'role_scoped_generic_markout_enabled': False,
      'final_quantity_role': 'increasing'}, 'non-reducing'),
    ({'action': None}, 'missing fields'),
])
def test_new_diagnostics_reject_missing_or_invalid_delivery(monkeypatch, mutation, message):
    import execution.chunked_parquet_journal as journal_module
    p = dict(latent_cooldown_active=True, cooldown_blocks_action=False,
             inventory_cooldown_mode='admission_off', pre_generic_role='reducing',
             final_quantity_role='reducing', generic_markout_negative_condition=True,
             role_scoped_generic_markout_enabled=True, same_side_pending_before=False,
             allow_post=True, action='post', final_size=.001)
    p.update(mutation)
    if p['action'] is None:
        del p['action']
    monkeypatch.setattr(runner, 'shared_diagnostics', lambda *a, **k: {'risk':{},'risk_missing':[]})
    monkeypatch.setattr(journal_module, 'iter_chunked_parquet_journal',
                        lambda *a, **k: iter([{'event_type':'decision','record':p}]))
    with pytest.raises(ValueError, match=message):
        runner.diagnostics({}, {'manifest':'unused'})


@pytest.mark.parametrize('arm', ARMS[1:])
def test_new_continuous_owner_accounting_and_idempotency(synthetic_parent, tmp_path, monkeypatch, arm):  # noqa: F811
    import models.replay.public_input_window as inputs
    from models.backtest_tick import simulate_prepared_inputs
    from research.families.f01_fixed_parameter_racing import mechanism_continuous as shared
    from research.families.f01_fixed_parameter_racing import mechanism_execution as execution
    prepared, base = synthetic_parent
    cfg = options(prepared, base, tmp_path/'continuous', arm)
    params = dict(cfg['common_params'])
    overrides = baseline(); overrides.pop('account_start', None); overrides.pop('response_action_policy')
    params.update(overrides)
    params = json.loads(json.dumps(params))
    start, end = cfg['spec']['start_ns'], cfg['spec']['end_ns']
    ident = dict(spec(), arm_id=arm, account_contract=runner.CONTINUOUS_CONTRACT,
                 parent_id='Final107-continuous', start_ns=start, end_ns=end,
                 input_manifest_id=prepared.manifest_id)
    def test_identity(value, original_calendar):
        assert original_calendar == calendar()
        if value != ident:
            raise ValueError('foreign synthetic identity')
        return deepcopy(value)
    monkeypatch.setattr(runner, 'identity', test_identity)
    monkeypatch.setattr(shared, 'DAY_NS', (end-start)//2)
    monkeypatch.setattr(execution, 'DAY_NS', (end-start)//2)
    bundle = prepared.inputs['bundle']
    monkeypatch.setattr(inputs, 'GlobalPublicTimelines', lambda *a: SimpleNamespace(
        bundle=bundle, identity='fixture-timeline', manifest={'model_identity':'fixture-model'}))
    def replay(timelines, actual, *, journal, **kwargs):
        return simulate_prepared_inputs(prepared, dict(actual, _l2_journal=journal),
                                        prepared_predictions=None)
    monkeypatch.setattr(shared, 'replay_windows', replay)
    cfg.update(spec=ident, common_params=params, bundle=str(bundle.root), timelines='unused',
               timeline_identity='fixture-timeline', timeline_model_identity='fixture-model')
    kw = dict(original_calendar=calendar(), original_b0_params=params)
    assert runner.execute_continuous(cfg, **kw)['status'] == 'COMPLETE'
    assert runner.validate_cell(cfg['output'], ident, **kw)['economic_complete']
    assert runner.execute_continuous(cfg, **kw)['status'] == 'ALREADY_COMPLETE'
