"""Contract and actual signal-path tests for new-B0 M0/M1 Final evaluation."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from research.families.f04_external_market_alpha import final_b0 as f
from research.families.f04_external_market_alpha import tardis_direction as d


def cell():
    return dict(experiment_id=f.EXPERIMENT, baseline_id=f.BASELINE,
                parent_id='final-000-2026-05-28', arm_id='M0', start_ns=f.START_NS,
                end_ns=f.START_NS+2*f.DAY_NS, input_manifest_id='a'*64,
                model_identity='b'*64, release_identity='c'*64,
                seed_pair={'rng_seed': 42, 'latency_seed': 59})


def test_calendar_identity_and_tail():
    c=cell()
    assert f.identity(c)==c
    c.update(parent_id='final-053-2026-09-11', start_ns=f.END_NS-f.DAY_NS, end_ns=f.END_NS)
    assert f.identity(c)==c


@pytest.mark.parametrize('key,value', [('arm_id','B0'),('arm_id','M2'),
    ('baseline_id','B0_RESPONSE_20261007'),('parent_id','development-014-2025-08-29'),
    ('end_ns',f.END_NS),('model_identity','unknown'),('seed_pair',{'rng_seed':42,'latency_seed':42})])
def test_wrong_identity_refused(key,value):
    c=cell();c[key]=value
    with pytest.raises(ValueError):f.identity(c)


def test_exact_new_b0_inheritance(monkeypatch):
    parent={'exit_urgency_strength':.5,'nested':{'a':1}}
    calls=[]
    def inherited(p,arm):
        calls.append(arm);x=deepcopy(p);x['exit_urgency_strength']=0.;return x
    monkeypatch.setattr(f,'arm_parameters',inherited)
    a,b=f.parameters(parent,'M0'),f.parameters(parent,'M1')
    assert a==b and calls==['B0','B0'] and parent['exit_urgency_strength']==.5
    a['nested']['a']=2
    assert b['nested']['a']==1
    with pytest.raises(ValueError):f.parameters(parent,'M2')


@pytest.mark.parametrize('arm', ['M0','M1'])
def test_only_direction_changes_through_real_consumer(monkeypatch,arm):
    ep={'market_id':d.EXECUTION_MARKET,'start_ns':1,'end_ns':100}
    rp={'market_id':d.REFERENCE_MARKET,'start_ns':1,'end_ns':100,
        'execution_parent_input_manifest_id':'a'*64}
    bundle=SimpleNamespace(input_manifest_id='a'*64,manifest={'plan':ep})
    plan={'prediction':{'frozen_F03_inf_model_manifest_sha256':'b'*64,'reference_columns':['ref']},
          'observation':{'reference_asof_max_age_ns':10}}
    cols=('local',) if arm=='M0' else ('local',d.REFERENCE_FEATURE_PREFIX+'ref')
    seen=[]
    model=SimpleNamespace(arm=arm,plan=plan,feature_cols=cols,
        predict=lambda x:(seen.append(x.copy()) or np.array([.75])))
    base_prediction=SimpleNamespace(touch_conditioned_up_probability_10000ms=.2,
        absolute_price_variance_rate_10000ms=123.,other=object())
    base=SimpleNamespace(compute_feature_frames=lambda frames:[deepcopy(base_prediction)])
    monkeypatch.setattr(f,'ConsumerBundle',lambda _:bundle)
    monkeypatch.setattr(f,'sha',lambda _:'b'*64)
    from strategy.signal import SignalEngine
    monkeypatch.setattr(SignalEngine,'from_public_models',lambda *a,**kw:base)
    monkeypatch.setattr(f,'FeatureCursor',lambda _:SimpleNamespace(bundle=SimpleNamespace(manifest={'plan':rp})))
    monkeypatch.setattr(d,'FEATURE_COLUMNS',('local',))
    monkeypatch.setattr(d,'_feature_metadata',lambda *a:{})
    monkeypatch.setattr(d,'model_row',lambda *a,**kw:[2.])
    monkeypatch.setattr(d,'_reference_row',lambda *a,**kw:({'ref':float('nan')},None,True))
    engine=f.FinalDirectionSignalEngine('execution','reference' if arm=='M1' else None,'base',model,
        expected_input_id='a'*64,expected_reference_plan=rp if arm=='M1' else None)
    result=engine.compute_feature_frames([SimpleNamespace(cutoff_ns=50)])[0]
    assert result.touch_conditioned_up_probability_10000ms==.75
    assert result.absolute_price_variance_rate_10000ms==123.
    assert engine.decisions==1 and seen[0][0,0]==2.
    if arm=='M1':
        assert np.isnan(seen[0][0,1]) and engine.reference_context_missing==1
    else:
        with pytest.raises(ValueError,match='M0'):
            f.FinalDirectionSignalEngine('execution','reference','base',model,expected_input_id='a'*64)


def test_reference_plan_preserves_clock_and_binds_new_contract(tmp_path,monkeypatch):
    ep={'market_id':d.EXECUTION_MARKET,'start_ns':1,'end_ns':100,
        'observation_profile':{'clock_policy':'execution','extra':'retained'}}
    day=tmp_path/'2026-05-27';day.mkdir();(day/'manifest.json').write_text(json.dumps({'files':[{'symbol':'BTCUSDT'}]}))
    bundle=SimpleNamespace(input_manifest_id='a'*64,manifest={'plan':ep,'source_bundles':[{'path':str(day)}]})
    declared={k:None for k in ('profile_id','clock_policy','market_delay_ns','processing_ns','measured_latency_path','measured_latency_sha256','measured_latency_market_id')}
    declared.update(profile_id='frozen',clock_policy='source_timestamp_proxy',market_delay_ns=250000000,processing_ns=1000000,origin='simulated')
    monkeypatch.setattr(f,'ConsumerBundle',lambda _:bundle)
    monkeypatch.setattr(f,'bundle_paths',lambda *a,**kw:[day])
    monkeypatch.setattr(f,'ObservationProfile',lambda **kw:SimpleNamespace(require_market=lambda m:None))
    plan=f.reference_plan('execution',tmp_path,{'observation':{'reference':declared}},contract_sha='c'*64,parent_id='final-000-2026-05-28')
    assert plan['observation_profile']['market_delay_ns']==250000000
    assert plan['observation_profile']['extra']=='retained'
    assert plan['execution_parent_input_manifest_id']=='a'*64
    assert plan['frozen_contract_sha256']=='c'*64
    assert plan['market_id']==d.REFERENCE_MARKET and not plan['include_outcome_bars']
    assert ep['observation_profile']['clock_policy']=='execution'


def test_no_original_first_batch_source_change():
    source=Path(f.__file__).read_text()
    assert '.fit(' not in source
    assert 'live/config' not in source
