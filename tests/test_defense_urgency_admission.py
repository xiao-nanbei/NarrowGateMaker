import hashlib
import json
from copy import deepcopy

import pytest

from research.families.f01_fixed_parameter_racing.defense_urgency_admission import admit_baseline
from tests.test_defense_urgency import calendar, params, RESPONSE_PATH


@pytest.fixture
def receipts(tmp_path):
    def save(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
    cal = calendar()
    for parents in cal['groups'].values():
        for spec in parents:
            spec.update(account_start_utc='2025-08-01', account_end_utc_exclusive='2025-08-03')
    cfg=params()
    cfg.pop('account_start')
    cfg['account_start_ns']=1754006400000000000
    start=cfg['account_start_ns']
    end=start+2*86400*10**9
    policy=cfg['response_action_policy']
    save(tmp_path/'fitted-response-models.json',policy['artifact'])
    save(tmp_path/'response-compute-calibration.json',policy['compute'])
    frozen=tmp_path/'source'/'overlay.py'
    frozen.parent.mkdir()
    frozen.write_text('# frozen\n')
    binding={'overlay':{'overlay.py':hashlib.sha256(frozen.read_bytes()).hexdigest()}}
    save(tmp_path/'source-binding.json',binding)
    plan=dict(baseline_binding=dict(source_binding=binding),response_layer_invariants=dict(
        model_sha256=hashlib.sha256((tmp_path/'fitted-response-models.json').read_bytes()).hexdigest()))
    rows=[]
    for i in range(204):
        parent=f'parent{i:03}'
        out=tmp_path/'accounts'/parent/RESPONSE_PATH
        rows.append(dict(account=parent,local_result=str(out),input_manifest_id=parent,
                         phase='development' if i<150 else 'final'))
        save(out/'effective_params.json',cfg)
        save(out/'metrics.json',dict(net_pnl=-1.,volume_btc=1.,notional_usdc=100.))
        save(out/'accounting.json',dict(economic_complete=True,account_start_ns=start,
             account_end_ns=end,all_in_net_pnl=-1.))
        save(out/'receipt.json',dict(economic_complete=True,account=parent,arm=RESPONSE_PATH,
             durable_cut_ms=end//10**6,input_manifest_id=parent))
        save(out/'result.json',dict(public_input_contract=dict(input_manifest_id=parent,
             account_start_ns=start),response_action_policy=dict(path_id=RESPONSE_PATH,
             compute=policy['compute'])))
        save(out.parent/'stage-receipt.json',dict(status='local_input_verified',account=parent,
             input_manifest_id=parent,account_start_ns=start,account_end_ns=end))
    save(tmp_path/'closure-receipt.json',dict(status='baseline_economics_archived_locally',
        local_account_index=rows))
    return cal,plan,tmp_path,save


def test_actual_receipts_admit_once_without_mutation(receipts):
    cal,plan,root,_=receipts
    before=deepcopy(plan)
    rows=admit_baseline(cal,plan,root)
    assert len(rows)==204 and len({r['parent_id'] for r in rows})==204
    assert all(r['result_reused'] and r['diagnostics_status']=='PARTIAL' for r in rows)
    assert plan==before


@pytest.mark.parametrize('name,field,value',[
    ('receipt','durable_cut_ms',0), ('receipt','arm','B0'),
    ('accounting','economic_complete',False), ('accounting','account_end_ns',0),
    ('metrics','net_pnl',10.), ('metrics','volume_btc',float('nan')),
    ('metrics','notional_usdc',-1.),
])
def test_reject_corrupt_terminal_identity_or_economics(receipts,name,field,value):
    cal,plan,root,save=receipts
    path=root/'accounts'/'parent000'/RESPONSE_PATH/(name+'.json')
    obj=json.loads(path.read_text())
    obj[field]=value
    save(path,obj)
    with pytest.raises(ValueError):
        admit_baseline(cal,plan,root)


@pytest.mark.parametrize('target',['result','source','model','duplicate','missing'])
def test_reject_changed_actual_result_source_and_universe(receipts,target):
    cal,plan,root,save=receipts
    if target=='result':
        path=root/'accounts'/'parent000'/RESPONSE_PATH/'result.json'
        obj=json.loads(path.read_text())
        obj['public_input_contract']['input_manifest_id']='other'
        save(path,obj)
    elif target in ('source','model'):
        (root/'source'/'overlay.py' if target=='source' else root/'fitted-response-models.json').write_text('{}')
    else:
        path=root/'closure-receipt.json'
        obj=json.loads(path.read_text())
        rows=obj['local_account_index']
        rows.append(rows[0]) if target=='duplicate' else rows.pop()
        save(path,obj)
    with pytest.raises(ValueError):
        admit_baseline(cal,plan,root)
