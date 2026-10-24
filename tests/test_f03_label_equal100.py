"""Original100, all-head unit-label and single-market reuse are exact contracts."""
from copy import deepcopy
from dataclasses import replace

import pandas as pd
import pytest

from research.families.f03_causal_13_head import label_equal100_training as m


def fixtures():
    days = tuple(str(x.date()) for x in pd.date_range('2025-08-01', periods=100, freq='3D'))
    policy = dict(schema_version='narrowgate.f03.time_half_life_daily.v2',
        decision_clock='feature_ready_index', half_life_days='inf', reference_date='2026-05-27',
        date_normalization='effective_head_rows',
        previous_time_weight={'reference_date':'2026-05-27', 'lambda':0,
                              'formula':'exp(-lambda * days_ago / 30.44)'},
        outcome_end_columns={h:'label_outcome_end_'+h for h in m.ml.MODEL_SPECS})
    c = m.ml.TrainOnlySelectionContract('test', '', '', 'tardis_public_input',
        days[:79], days[79:80], days[80:], days, '', '', '', '', policy,
        tuple('feature'+str(i) for i in range(29)))
    metadata = {h:dict(train_only_selection=c.to_metadata(), feature_cols=list(c.feature_cols),
        selection_fit_params=dict(n_jobs=4, seed=42, n_estimators=2000, **({'is_unbalance':True} if s[3] else {})))
        for h,s in m.ml.MODEL_SPECS.items()}
    plan = dict(candidate=m.CANDIDATE, economic_accounts=54, new_control_accounts=0,
        continuous_accounts=0, training_threads=2, per_valid_label_weight=1.,
        date_normalization='none', class_adjustment='none', age_decay='none', reference_market=None,
        training_days=list(days), newly_fitted_heads=[m.HEAD], reused_heads=list(m.REUSED))
    return plan, dict(baseline_head_metadata=metadata)


def test_inherits_exact_sparse_B0_calendar_not_recent100():
    p,b = fixtures(); prior,equal=m.validate_plan(p,b)
    assert equal.refit_days==prior.refit_days
    assert len(equal.refit_days)==100 and equal.refit_days[-1]=='2026-05-25'
    assert equal.sample_weight_policy['date_normalization']=='none'
    assert equal.sample_weight_policy['class_adjustment']=='none'
    assert len(m.REUSED)==12 and m.HEAD not in m.REUSED


@pytest.mark.parametrize('change', [dict(reference_market='BTCUSDT'), dict(per_valid_label_weight=2),
    dict(class_adjustment='balanced'), dict(date_normalization='effective_head_rows'),
    dict(age_decay='time'), dict(economic_accounts=55), dict(continuous_accounts=1),
    dict(new_control_accounts=1), dict(newly_fitted_heads=[]), dict(reused_heads=[]), dict(training_threads=4)])
def test_scope_changes_fail_before_data_access(change):
    p,b=fixtures()
    with pytest.raises(ValueError,match='scope'):m.validate_plan(dict(p,**change),b)


def test_wrong_days_or_head_calendar_rejected():
    p,b=fixtures();p['training_days']=p['training_days'][::-1]
    with pytest.raises(ValueError,match='calendar'):m.validate_plan(p,b)
    p,b=fixtures();head=m.REUSED[0]
    b['baseline_head_metadata'][head]['train_only_selection']['fit_days']=p['training_days'][:78]
    with pytest.raises(ValueError,match='differ'):m.validate_plan(p,b)


def reused_fixture():
    p,b=fixtures();_,eq=m.validate_plan(p,b);head=m.REUSED[0];base=b['baseline_head_metadata'][head]
    item=dict(feature_cols=list(eq.feature_cols),training_days=list(eq.refit_days),
        per_valid_label_weight=1.,date_normalization='none',class_adjustment='none',age_decay='none',is_unbalance=False)
    params=m.shared.parameters(base,head,2)
    support={phase:dict(rows=10,report=dict(example=phase)) for phase in ('fit','selection','refit')}
    meta=dict(selection_fit_params=params,refit_fit_params=dict(params,n_estimators=4),best_iteration=4,
        train_only_selection=eq.to_metadata(),feature_cols=list(eq.feature_cols),
        per_head_time_weighting={k:v['report'] for k,v in support.items()},
        saved_model_prediction_checks={k:dict(rows=10,exact_saved_reload=True) for k in support})
    return head,item,meta,base,eq,support


def test_only_exact_accepted_twelve_are_reusable():
    args=reused_fixture();m.validate_reused(*args)
    with pytest.raises(ValueError,match='joint direction'):m.validate_reused(m.HEAD,*args[1:])
    bad=deepcopy(args);bad[1]['feature_cols'].append('reference_market_feature')
    with pytest.raises(ValueError,match='input/weight'):m.validate_reused(*bad)
    bad=deepcopy(args);bad[2]['selection_fit_params']['seed']=999
    with pytest.raises(ValueError,match='method'):m.validate_reused(*bad)
    bad=deepcopy(args);bad[2]['saved_model_prediction_checks']['refit']['rows']=11
    with pytest.raises(ValueError,match='support'):m.validate_reused(*bad)
    bad=list(deepcopy(args));bad[4]=replace(bad[4],feature_cols=('only_one',))
    with pytest.raises(ValueError,match='input/weight'):m.validate_reused(*bad)


def test_final_identity_keeps_new_model_separate_and_rejects_joint_arm():
    from research.families.f03_causal_13_head import label_equal100_final as f
    spec=dict(experiment_id=f.EXPERIMENT,baseline_id=f.final.BASELINE,
        parent_id='final-000-2026-05-28',arm_id=f.CANDIDATE,
        start_ns=f.final.START_NS,end_ns=f.final.START_NS+2*f.final.DAY_NS,
        input_manifest_id='1'*64,model_identity='2'*64,release_identity='3'*64,
        seed_pair={'rng_seed':42,'latency_seed':59})
    assert f.identity(spec)==spec
    with pytest.raises(ValueError):f.identity(dict(spec,arm_id=m.shared.CANDIDATE))
    with pytest.raises(ValueError):f.identity(dict(spec,end_ns=f.final.END_NS+1))
