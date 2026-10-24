"""Finite mechanism12 accounts with independent recovery and audited diagnostics.

This is not an alias for the historical D/U executor. Source/input admission and
placement remain external; this module never schedules work or selects dates.
"""
from copy import deepcopy
from datetime import date, timedelta

from .mechanism_ablation import ARMS, BASELINE, EXPERIMENT, FULL_EXPERIMENT, PARENT_INDICES, arm_parameters

from narrowgate.replay.account import (  # Temporary public import compatibility; no copied engine.
    DAY_NS as DAY_NS, HOUR_MS as HOUR_MS, DAY_MS as DAY_MS,
    sha as sha, simulate_slice as simulate_slice, diagnostics as diagnostics,
    validate_bound_cell as validate_bound_cell,
    effective_parameter_record as effective_parameter_record,
    execute_bound_cell as execute_bound_cell,
)
from narrowgate.runtime.artifact_io import (  # noqa: F401 -- retained import API, one implementation
    atomic_json as _atomic, load_json as _load, numeric_json as _numeric,
    cell_lock as cell_lock,
)
PARENTS = {
    f'development-{i:03d}-{date(2025, 8, 1)+timedelta(days=2*i)}':
        (1754006400000000000+2*i*DAY_NS, 1754006400000000000+(2*i+2)*DAY_NS)
    for i in PARENT_INDICES
}
FULL_PARENTS = {
    f"{'development' if i < 150 else 'final'}-{i if i < 150 else i-150:03d}-{date(2025, 8, 1)+timedelta(days=2*i)}":
        (1754006400000000000+2*i*DAY_NS,
         1754006400000000000+min(2*i+2, 407)*DAY_NS)
    for i in range(204)
}

def identity(spec):
    keys = ('experiment_id', 'baseline_id', 'account_contract', 'parent_id',
            'arm_id', 'seed_pair', 'start_ns', 'end_ns', 'input_manifest_id', 'release_identity')
    v = {key: deepcopy(spec[key]) for key in keys}
    contracts = {EXPERIMENT: ('MECHANISM12_INDEPENDENT48H', PARENTS),
                 FULL_EXPERIMENT: ('MECHANISM407_INDEPENDENT', FULL_PARENTS)}
    contract, parents = contracts.get(v['experiment_id'], (None, {}))
    if (v['experiment_id'] == FULL_EXPERIMENT
            and v['account_contract'] == 'MECHANISM407_FINAL107_CONTINUOUS'):
        contract = 'MECHANISM407_FINAL107_CONTINUOUS'
        parents = {'Final107-continuous': (1779926400000000000, 1789171200000000000)}
    if (contract is None or v['baseline_id'] != BASELINE
            or v['account_contract'] != contract
            or v['arm_id'] not in ARMS or v['parent_id'] not in parents
            or (v['start_ns'], v['end_ns']) != parents.get(v['parent_id'])
            or v['seed_pair'] != {'rng_seed': 42, 'latency_seed': 59}
            or any(not isinstance(v[k], str) or len(v[k]) != 64
                   or any(c not in '0123456789abcdef' for c in v[k])
                   for k in ('input_manifest_id', 'release_identity'))):
        raise ValueError('mechanism12 identity differs from fixed contract')
    return v

def validate_cell(output, spec):
    return validate_bound_cell(output, identity(spec), validate_effective_params)

def validate_effective_params(params, arm):
    if (arm not in ARMS or params.get('exit_urgency_strength') != 0.0
            or params.get('asym_strength') != (0.0 if arm == 'DIRECT_DIRECTION_ASYM_OFF' else 0.1)
            or params.get('markout_spread_scale') != 0.2
            or params.get('response_observe_only') is not True
            or params.get('response_update_branch_enabled') is not True
            or params.get('_research_generic_markout_policy_enabled', True) is not (arm != 'GENERIC_MARKOUT_POLICY_OFF')):
        raise ValueError('mechanism effective arm parameters differ')

def execute_cell(prepared, *, spec, common_params, prepared_predictions,
                 initial_capital, max_mark_age_ns, funding, output, progress=None,
                 initialization_lock=None):
    ident = identity(spec)
    return execute_bound_cell(prepared, ident=ident,
        params=arm_parameters(common_params, ident['arm_id']),
        prepared_predictions=prepared_predictions, initial_capital=initial_capital,
        max_mark_age_ns=max_mark_age_ns, funding=funding, output=output,
        progress=progress, initialization_lock=initialization_lock,
        validate_output=lambda path: validate_cell(path, spec),
        full_calendar=ident['experiment_id'] == FULL_EXPERIMENT)
