"""New continuous identity and persistence never imply economic resets."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from research.families.f01_fixed_parameter_racing import mechanism_continuous as runner
from research.families.f01_fixed_parameter_racing.mechanism_execution import identity
from research.families.f01_fixed_parameter_racing.mechanism_ablation import FULL_EXPERIMENT, BASELINE
from tests.test_optional_update_branch import synthetic_parent  # noqa: F401


def spec():
    return dict(experiment_id=FULL_EXPERIMENT, baseline_id=BASELINE,
        account_contract='MECHANISM407_FINAL107_CONTINUOUS', parent_id='Final107-continuous',
        arm_id='DIRECT_DIRECTION_ASYM_OFF', seed_pair={'rng_seed': 42, 'latency_seed': 59},
        start_ns=1779926400000000000, end_ns=1789171200000000000,
        input_manifest_id='a'*64, release_identity='b'*64)


def test_continuous_identity_preserves_exact_global_interval():
    assert identity(spec()) == spec()


@pytest.mark.parametrize('field,value', [('baseline_id','B0_RESPONSE_20261007'),
    ('end_ns',1789171200000000000-3600000000000), ('parent_id','final-000-2026-05-28'),
    ('arm_id','COMBINED'), ('seed_pair',{'rng_seed':43,'latency_seed':59})])
def test_wrong_continuous_contract_refused(field,value):
    bad=spec();bad[field]=value
    with pytest.raises(ValueError): identity(bad)


def run(monkeypatch,tmp_path,interval,resume=None):
    import models.backtest_tick as tick
    import models.replay.runtime_input_window as window
    end=107*runner.DAY_MS
    calls=[];saved=[];progress=[];prepared=[]
    monkeypatch.setattr(window,'runtime_input_context_start',lambda r,b,c,**k:b)
    def simulate(p,params,*,prepared_predictions,checkpoint_at_ts_ms,resume_checkpoint,consume_resume_checkpoint):
        old=0 if resume_checkpoint is None else resume_checkpoint['runtime']['value']
        cut=end if checkpoint_at_ts_ms is None else checkpoint_at_ts_ms
        calls.append((cut,old,consume_resume_checkpoint))
        return {'value':old+1,'_replay_checkpoint':{'runtime':{'value':old+1},'cut_ts_ms':cut,
            'l2_prefix':{'manifest':str(tmp_path/f'prefix-{cut}.json')}}}
    def prepare(a,b,**kwargs):
        prepared.append((a,b,kwargs));return object(),object()
    monkeypatch.setattr(tick,'simulate_prepared_inputs',simulate)
    timelines=SimpleNamespace(bundle=SimpleNamespace(manifest={'plan':{'start_ns':0,'end_ns':end*10**6}}),prepare=prepare)
    result=runner.replay_windows(timelines,{'account_start_ns':0,'tick_size':.1},start_ns=0,end_ns=end*10**6,
        journal=SimpleNamespace(writer=SimpleNamespace(output_dir=tmp_path)),checkpoint=resume,
        persist=lambda c:saved.append(deepcopy(c)),progress=progress.append,durable_interval_ms=interval)
    return result,calls,saved,progress,prepared


def test_daily_hourly_identical_window_flow_and_account(monkeypatch,tmp_path):
    hourly=run(monkeypatch,tmp_path,runner.HOUR_MS);daily=run(monkeypatch,tmp_path,runner.DAY_MS)
    assert hourly[0:2]==daily[0:2] and hourly[4]==daily[4]
    assert len(hourly[2])==2567 and len(daily[2])==107
    assert daily[3][1]['memory_cut_ms']==2*runner.HOUR_MS
    assert daily[3][1]['durable_cut_ms']==runner.HOUR_MS
    assert daily[3][-1]['status']=='SETTLING'


def test_recovery_preserves_account_then_first_and_aligned_saves(monkeypatch,tmp_path):
    original=run(monkeypatch,tmp_path,runner.DAY_MS)
    checkpoint=original[2][2]
    restored=run(monkeypatch,tmp_path,runner.DAY_MS,checkpoint)
    assert restored[0]==original[0]
    assert restored[2][0]['cut_ts_ms']==checkpoint['cut_ts_ms']+runner.HOUR_MS
    assert all(c['cut_ts_ms']%runner.DAY_MS==0 for c in restored[2][1:])


@pytest.mark.parametrize('interval',[True,0,25*runner.HOUR_MS])
def test_bad_interval_refused(monkeypatch,tmp_path,interval):
    with pytest.raises(ValueError):run(monkeypatch,tmp_path,interval)


def test_only_owned_obsolete_prefix_is_retired(tmp_path):
    journal=SimpleNamespace(writer=SimpleNamespace(output_dir=tmp_path/'current'))
    journal.writer.output_dir.mkdir()
    old=journal.writer.output_dir/'prefix-1.json';old.write_text('{}')
    other=tmp_path/'prefix-2.json';other.write_text('{}')
    payload=journal.writer.output_dir/'part-1.parquet';payload.write_bytes(b'keep')
    for path in [old,other,payload]:runner._retire_prefix(str(path),journal)
    assert not old.exists() and other.exists() and payload.exists()


@pytest.mark.parametrize('arm',['GENERIC_MARKOUT_POLICY_OFF','DIRECT_DIRECTION_ASYM_OFF'])
def test_continuous_final_publication_accounting_and_idempotency(synthetic_parent,tmp_path,monkeypatch,arm):  # noqa: F811
    import models.replay.public_input_window as inputs
    import research.families.f01_fixed_parameter_racing.mechanism_execution as execution
    from models.backtest_tick import simulate_prepared_inputs
    from tests.test_defense_urgency_execution import options
    prepared,base=synthetic_parent
    cfg=options(prepared,base,tmp_path/'continuous',arm)
    start,end=cfg['spec']['start_ns'],cfg['spec']['end_ns']
    ident={**spec(),'arm_id':arm,'start_ns':start,'end_ns':end,'input_manifest_id':prepared.manifest_id}
    # Only the test fixture clock is compressed. Production identity remains
    # the exact Final107 interval, tested separately above.
    def test_identity(value):
        if value!=ident:raise ValueError('foreign synthetic identity')
        return deepcopy(value)
    monkeypatch.setattr(execution,'identity',test_identity)
    monkeypatch.setattr(runner,'identity',test_identity)
    monkeypatch.setattr(execution,'DAY_NS',(end-start)//2)
    monkeypatch.setattr(runner,'DAY_NS',(end-start)//2)
    bundle=prepared.inputs['bundle']
    monkeypatch.setattr(inputs,'GlobalPublicTimelines',lambda *a:SimpleNamespace(
        bundle=bundle,identity='timeline-fixture',manifest={'model_identity':'model-fixture'}))
    def replay(timelines,params,*,journal,**kwargs):
        return simulate_prepared_inputs(prepared,dict(params,_l2_journal=journal),prepared_predictions=None)
    monkeypatch.setattr(runner,'replay_windows',replay)
    cfg['spec']=ident
    cfg['common_params'].update(asym_strength=.1,markout_spread_scale=.2,
        response_observe_only=True,response_update_branch_enabled=True)
    cfg.update(bundle=str(bundle.root),timelines='unused-test-fixture',
        timeline_identity='timeline-fixture',timeline_model_identity='model-fixture')
    assert runner.execute_continuous(cfg)['status']=='COMPLETE'
    assert execution.validate_cell(cfg['output'],ident)['economic_complete'] is True
    assert runner.execute_continuous(cfg)['status']=='ALREADY_COMPLETE'
    changed=deepcopy(cfg);changed['common_params']['maker_fee']=.0001
    with pytest.raises(ValueError,match='parameters changed'):runner.execute_continuous(changed)
