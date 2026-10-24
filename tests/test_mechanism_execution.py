"""Synthetic engine/output parity and strict prospective identity checks."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_defense_urgency_execution import options
from tests.test_tick_runtime_checkpoint import assert_same
from research.families.f01_fixed_parameter_racing.mechanism_ablation import ARMS, BASELINE, EXPERIMENT, arm_parameters
from research.families.f01_fixed_parameter_racing.mechanism_execution import PARENTS, identity, simulate_slice, diagnostics, validate_effective_params
from models.replay.l2_journal import ReplayL2Journal
from execution.chunked_parquet_journal import iter_chunked_parquet_journal

def spec():
    parent=next(iter(PARENTS));start,end=PARENTS[parent]
    return dict(experiment_id=EXPERIMENT,baseline_id=BASELINE,
        account_contract='MECHANISM12_INDEPENDENT48H',parent_id=parent,
        start_ns=start,end_ns=end,arm_id='B0',seed_pair={'rng_seed':42,'latency_seed':59},
        input_manifest_id='a'*64,release_identity='b'*64)

def test_fixed_universe():
    assert len(PARENTS)==12
    assert identity(spec())==spec()

@pytest.mark.parametrize('patch',[
    {'experiment_id':'B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407'},
    {'baseline_id':'B0_RESPONSE_20261007'},{'arm_id':'A_PLUS_B'},
    {'parent_id':'development-007-2025-08-15'}, {'end_ns':0},
    {'seed_pair':{'rng_seed':43,'latency_seed':60}},{'release_identity':''},
    {'release_identity':'z'*64},
])
def test_foreign_identity_rejected(patch):
    with pytest.raises(ValueError):identity({**spec(),**patch})

@pytest.mark.parametrize('arm',ARMS)
def test_real_synthetic_engine_journal_and_saved_recovery(synthetic_parent,tmp_path,arm):  # noqa: F811
    from models.backtest_tick import simulate_prepared_inputs
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint,load_trusted_runtime_checkpoint
    prepared,base=synthetic_parent
    kw=options(prepared,base,tmp_path/'unused','B0')
    parent={**kw['common_params'],'asym_strength':.1,'markout_spread_scale':.2,
            'response_observe_only':True,'response_update_branch_enabled':True}
    params=arm_parameters(parent,arm);before=deepcopy(params)
    validate_effective_params(params, arm)
    with pytest.raises(ValueError):validate_effective_params({**params,'markout_spread_scale':0},arm)
    expected=simulate_prepared_inputs(prepared,params,prepared_predictions=None)
    whole=simulate_slice(prepared,params,None,ReplayL2Journal(tmp_path/'whole',identity={'test':arm}),initialization_lock=tmp_path/'initialization.lock')
    whole_receipt=whole.pop('_l2_journal')
    assert_same(whole,expected)
    partial=simulate_slice(prepared,params,None,ReplayL2Journal(tmp_path/'prefix',identity={'test':arm}),cut=10000,initialization_lock=tmp_path/'initialization.lock')
    path=tmp_path/'recovery';save_runtime_checkpoint(path,partial['_replay_checkpoint'])
    restored=simulate_slice(prepared,params,None,ReplayL2Journal(tmp_path/'restored',identity={'test':arm}),load_trusted_runtime_checkpoint(path),initialization_lock=tmp_path/'initialization.lock')
    restored_receipt=restored.pop('_l2_journal')
    assert_same(restored,whole)
    assert list(iter_chunked_parquet_journal(whole_receipt['manifest']))==list(iter_chunked_parquet_journal(restored_receipt['manifest']))
    diag=diagnostics(restored,restored_receipt)
    assert not diag['missing_fields']
    assert diag['production']==whole_receipt['production']
    assert params==before

@pytest.mark.parametrize('arm', ARMS)
def test_complete_executor_and_validator(synthetic_parent,tmp_path,monkeypatch,arm):  # noqa: F811
    import research.families.f01_fixed_parameter_racing.mechanism_execution as execution
    prepared,base=synthetic_parent
    kw=options(prepared,base,tmp_path/'cell',arm)
    start=kw['spec']['start_ns'];end=kw['spec']['end_ns']
    parent=next(iter(PARENTS))
    # Test-only compressed clock, never exported into a production binding.
    monkeypatch.setattr(execution,'PARENTS',{parent:(start,end)})
    from narrowgate.replay import account
    monkeypatch.setattr(account,'DAY_NS',(end-start)//2)
    kw['spec']={**spec(),'parent_id':parent,'start_ns':start,'end_ns':end,
        'input_manifest_id':prepared.manifest_id,'arm_id':arm}
    kw.pop('checkpoint_cuts')
    kw['common_params'].update(asym_strength=.1,markout_spread_scale=.2,
        response_observe_only=True,response_update_branch_enabled=True)
    assert execution.execute_cell(prepared,**kw)['status']=='COMPLETE'
    assert execution.validate_cell(kw['output'],kw['spec'])['economic_complete']
    assert execution.execute_cell(prepared,**kw)['status']=='ALREADY_COMPLETE'
    assert not (kw['output']/'latest.checkpoint').exists()


@pytest.mark.parametrize('incremental', [False, True])
@pytest.mark.parametrize('retain_checkpoint', [False, True])
def test_daily_executor_preserves_disk_prefix_and_resumes_between_saves(
        synthetic_parent, tmp_path, monkeypatch, incremental, retain_checkpoint):  # noqa: F811
    """Exercise real public replay, both sinks, persisted restore and accounting."""
    from narrowgate.replay import account as execution
    from models.replay import runtime_checkpoint_io as checkpoint_io

    prepared, base = synthetic_parent
    cfg = options(prepared, base, tmp_path/'resumed', 'B0')
    start, end = cfg['spec']['start_ns'], cfg['spec']['end_ns']
    # Compressed operational hours/days are fixture-only. The production
    # executor still sees its normal interface and real serialized runtime.
    monkeypatch.setattr(execution, 'HOUR_MS', 1000)
    monkeypatch.setattr(execution, 'DAY_MS', 4000)
    monkeypatch.setattr(execution, 'DAY_NS', (end-start)//2)
    ident = cfg['spec']
    params = cfg['common_params']
    parameter_record = execution.effective_parameter_record(params)

    def validate(output):
        def parameters_equal(actual, arm):
            assert arm == ident['arm_id']
            assert actual == parameter_record
        return execution.validate_bound_cell(output, ident, parameters_equal)

    def run(output, *, interval=None, progress=None):
        return execution.execute_bound_cell(prepared, ident=ident, params=params,
            prepared_predictions=None, initial_capital=cfg['initial_capital'],
            max_mark_age_ns=cfg['max_mark_age_ns'], funding=cfg['funding'],
            output=output, validate_output=validate, full_calendar=False,
            progress=progress, incremental_journal=incremental,
            retain_completed_checkpoint=retain_checkpoint, durable_interval_ms=interval)

    saved = []
    original_save = checkpoint_io.save_runtime_checkpoint

    def save(path, checkpoint):
        original_save(path, checkpoint)
        saved.append(checkpoint['cut_ts_ms'])

    monkeypatch.setattr(checkpoint_io, 'save_runtime_checkpoint', save)
    output = tmp_path/'resumed'
    seen = []
    start_ms = start//10**6

    def interrupted(status):
        checkpoint = checkpoint_io.load_trusted_runtime_checkpoint(output/'latest.checkpoint')
        prefix = Path(checkpoint['l2_prefix']['manifest'])
        assert prefix.exists()
        # This verifies the actual retained parts, not only a remembered path.
        list(iter_chunked_parquet_journal(prefix, verify_checksum=True))
        assert checkpoint['cut_ts_ms'] == status['durable_cut_ms']
        seen.append(status)
        if status['memory_cut_ms'] == start_ms+6000:
            assert status['durable_cut_ms'] == start_ms+4000
            assert status['checkpoint_save_seconds'] == 0
            raise RuntimeError('disconnect between daily saves')

    with pytest.raises(RuntimeError, match='between daily saves'):
        run(output, interval=execution.DAY_MS, progress=interrupted)
    assert saved == [start_ms+1000, start_ms+4000]
    resumed_status = []
    assert run(output, interval=execution.DAY_MS, progress=resumed_status.append)['status'] == 'COMPLETE'
    assert resumed_status[0]['memory_cut_ms'] == start_ms+5000
    assert resumed_status[0]['durable_cut_ms'] == start_ms+4000
    assert resumed_status[0]['checkpoint_save_seconds'] == 0
    expected_saves = [start_ms+1000, *range(start_ms+4000, end//10**6, 4000)]
    assert saved == expected_saves
    validate(output)
    assert (output/'latest.checkpoint').exists() is retain_checkpoint
    assert (output/'latest.checkpoint.metadata.json').exists() is retain_checkpoint
    if retain_checkpoint:
        checkpoint = checkpoint_io.load_trusted_runtime_checkpoint(output/'latest.checkpoint')
        list(iter_chunked_parquet_journal(checkpoint['l2_prefix']['manifest'], verify_checksum=True))

    hourly = tmp_path/'hourly'
    saved.clear()
    assert run(hourly)['status'] == 'COMPLETE'
    assert saved == list(range(start_ms+1000, end//10**6, 1000))
    validate(hourly)
    for name in ('accounting', 'daily-ledger', 'metrics', 'effective_params', 'diagnostics'):
        assert (output/(name+'.json')).read_bytes() == (hourly/(name+'.json')).read_bytes()
    actual = json.loads((output/'result.json').read_text())
    expected = json.loads((hourly/'result.json').read_text())
    actual_journal = actual.pop('_l2_journal')
    expected_journal = expected.pop('_l2_journal')
    assert_same(actual, expected)
    assert list(iter_chunked_parquet_journal(actual_journal['manifest'])) == list(
        iter_chunked_parquet_journal(expected_journal['manifest']))
    # Completion is idempotent, including the formal journal file manifest.
    assert run(output, interval=execution.DAY_MS)['status'] == 'ALREADY_COMPLETE'


@pytest.mark.parametrize('interval', [True, 0, -1, 1.5, 60_000])
def test_invalid_durable_cadence_rejected_before_input_or_output(interval, tmp_path):
    from research.families.f01_fixed_parameter_racing.mechanism_execution import execute_bound_cell
    with pytest.raises(ValueError, match='one replay hour or day'):
        execute_bound_cell(None, ident={}, params={}, prepared_predictions=None,
            initial_capital=10000, max_mark_age_ns=1, funding={}, output=tmp_path/'unused',
            validate_output=lambda path: None, full_calendar=False,
            durable_interval_ms=interval)
    assert not (tmp_path/'unused').exists()
