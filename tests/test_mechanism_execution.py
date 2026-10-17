"""Synthetic engine/output parity and strict prospective identity checks."""
from copy import deepcopy

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
    monkeypatch.setattr(execution,'DAY_NS',(end-start)//2)
    kw['spec']={**spec(),'parent_id':parent,'start_ns':start,'end_ns':end,
        'input_manifest_id':prepared.manifest_id,'arm_id':arm}
    kw.pop('checkpoint_cuts')
    kw['common_params'].update(asym_strength=.1,markout_spread_scale=.2,
        response_observe_only=True,response_update_branch_enabled=True)
    assert execution.execute_cell(prepared,**kw)['status']=='COMPLETE'
    assert execution.validate_cell(kw['output'],kw['spec'])['economic_complete']
    assert execution.execute_cell(prepared,**kw)['status']=='ALREADY_COMPLETE'
    assert not (kw['output']/'latest.checkpoint').exists()
