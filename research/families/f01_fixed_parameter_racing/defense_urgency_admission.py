"""Read actual prior results into the fixed D/U baseline reuse index.

Historical files are never rewritten. Hashing here is model/source identity
admission, not rehashing market or prepare payloads for archive acceptance.
"""
import hashlib
import json
import math
from pathlib import Path
from datetime import datetime, timezone

from .defense_urgency import BASELINE, RESPONSE_PATH, arm_parameters, baseline_completion_gate


def load(path):
    return json.loads(Path(path).read_text())


def utc_ns(value):
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:
        parsed=parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())*10**9


def admit_baseline(calendar, execution_plan, baseline_root):
    root = Path(baseline_root)
    binding = execution_plan['baseline_binding']['source_binding']
    if load(root/'source-binding.json') != binding:
        raise ValueError('baseline source binding changed')
    for name, digest in binding['overlay'].items():
        if hashlib.sha256((root/'source'/name).read_bytes()).hexdigest() != digest:
            raise ValueError('baseline frozen overlay changed')
    model_path = root/'fitted-response-models.json'
    expected_model = execution_plan['response_layer_invariants']['model_sha256']
    if hashlib.sha256(model_path.read_bytes()).hexdigest() != expected_model:
        raise ValueError('baseline model changed')
    model = load(model_path)
    cost = load(root/'response-compute-calibration.json')
    cost.pop('samples',None)
    closure = load(root/'closure-receipt.json')
    if closure['status'] != 'baseline_economics_archived_locally':
        raise ValueError('baseline closure not complete')
    expected = {p['shard_id']:p for g in calendar['groups'].values() for p in g}
    rows=[]
    for source in closure['local_account_index']:
        parent=source['account']
        spec=expected[parent]
        out=Path(source['local_result'])
        receipt=load(out/'receipt.json')
        accounting=load(out/'accounting.json')
        params=load(out/'effective_params.json')
        metrics=load(out/'metrics.json')
        result=load(out/'result.json')
        stage=load(out.parent/'stage-receipt.json')
        start=utc_ns(spec['account_start_utc'])
        end=utc_ns(spec['account_end_utc_exclusive'])
        policy=params.get('response_action_policy',{})
        if (policy.get('artifact')!=model or policy.get('compute')!=cost
                or policy.get('path_id')!=RESPONSE_PATH):
            raise ValueError(f'{parent}: response identity changed')
        arm_parameters(params,'B0')
        if (receipt.get('economic_complete') is not True
                or accounting.get('economic_complete') is not True
                or receipt['account']!=parent or receipt['arm']!=RESPONSE_PATH
                or receipt['durable_cut_ms']*10**6!=end
                or accounting['account_end_ns']!=end
                or accounting['account_start_ns']!=start
                or params['account_start_ns']!=start
                or receipt['input_manifest_id']!=source['input_manifest_id']
                or receipt['input_manifest_id']!=stage['input_manifest_id']
                or result['public_input_contract']['input_manifest_id']!=receipt['input_manifest_id']
                or result['public_input_contract']['account_start_ns']!=start
                or result['response_action_policy']['path_id']!=RESPONSE_PATH
                or result['response_action_policy']['compute']!=cost
                or stage['status']!='local_input_verified'
                or stage['account']!=parent
                or stage['account_start_ns']!=start
                or stage['account_end_ns']!=end
                or metrics['net_pnl']!=accounting['all_in_net_pnl']
                or not all(math.isfinite(metrics[k]) for k in
                    ('net_pnl','volume_btc','notional_usdc'))
                or metrics['volume_btc']<0 or metrics['notional_usdc']<0):
            raise ValueError(f'{parent}: incomplete/mismatched baseline economics')
        rows.append(dict(parent_id=parent,arm_id='B0',baseline_id=BASELINE,
            source_path_id=RESPONSE_PATH,source_result_ref=str(out),
            source_run_id='response-final107' if source['phase']=='final' else 'response-b0-407',
            result_reused=True,economics_status='COMPLETE',diagnostics_status='PARTIAL',
            binding_verified=True,terminal_cut_verified=True,accounting_verified=True,
            input_manifest_id=receipt['input_manifest_id'],start_ns=start,end_ns=end,
            net_pnl=metrics['net_pnl'],volume_btc=metrics['volume_btc'],
            notional_usdc=metrics['notional_usdc'],hard_risk_contract_unchanged=True))
    gate=baseline_completion_gate(calendar,rows)
    if not gate['ready']:
        raise ValueError(f'incomplete baseline: {gate}')
    return sorted(rows,key=lambda x:x['parent_id'])
