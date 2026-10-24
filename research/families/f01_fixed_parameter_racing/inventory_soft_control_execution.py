"""Thin five-cell ownership boundary over the maintained replay primitives.

No preparation, dispatch, model fitting or historical contract mutation occurs
here. R0 is reused; the separate engineering parity harness owns short probes.
"""
from functools import partial
from collections import Counter
import json

from .inventory_soft_control import (
    CONTINUOUS_CONTRACT, INDEPENDENT_CONTRACT, arm_parameters, identity,
    validate_effective_params,
)
from narrowgate.replay.account import (
    diagnostics as shared_diagnostics, execute_bound_cell, validate_bound_cell,
)
from .mechanism_continuous import execute_bound_continuous

CHECKPOINT_KEY = 'inventory_soft_control_cell_v1'


def diagnostics(result, journal, *, full_calendar=True):
    joint = Counter()
    missing = Counter()
    fields = ('latent_cooldown_active', 'cooldown_blocks_action', 'pre_generic_role',
              'final_quantity_role', 'generic_markout_negative_condition',
              'role_scoped_generic_markout_enabled', 'same_side_pending_before',
              'allow_post', 'action')
    def observe_decision(p):
        for key in fields:
            if key not in p:
                missing[key] += 1
        joint[json.dumps({key:p.get(key, 'UNKNOWN') for key in fields}, sort_keys=True)] += 1
        if (p.get('inventory_cooldown_mode') == 'admission_off'
                and p.get('cooldown_blocks_action') is not False):
            raise ValueError('R2 latent cooldown leaked into economic admission')
        if (p.get('role_scoped_generic_markout_enabled') is False
                and p.get('allow_post') and p.get('final_size', 0) > 0
                and p.get('final_quantity_role') != 'reducing'):
            raise ValueError('released generic contribution has non-reducing final quantity')
    # The shared verified traversal owns all journal reads; observe every
    # decision there instead of independently decoding the same journal again.
    value = shared_diagnostics(result, journal, full_calendar=full_calendar,
                               decision_observer=observe_decision)
    value['schema'] = 'inventory_soft_control_diagnostics.v1'
    if missing:
        raise ValueError('new candidate joint diagnostics missing fields: '+str(dict(missing)))
    value['joint_decision_counts'] = dict(joint)
    value['joint_missing_fields'] = dict(missing)
    value['common_opportunity_key'] = (
        'No cross-arm state equivalence from side/market generation alone. '
        'Joint counts are within each arm; account/time blocks are paired for economics.')
    for key in ('signed_inventory_time_s', 'notional_inventory_time_s',
                'signed_notional_inventory_time_s', 'time_avg_abs_inventory',
                'time_avg_sq_inventory', 'time_avg_notional_inventory'):
        value['risk'][key] = result.get(key)
        if key not in result:
            value['risk_missing'].append(key)
    value['tail_contract'] = (
        'all original parents; four paired contrasts plus R4-R1-R3+R0; '
        '2-parent primary blocks, 1/4 sensitivity; no fresh holdout or promotion')
    return value


def validate_cell(output, spec, *, original_calendar, original_b0_params):
    ident = identity(spec, original_calendar)
    def validate_params(actual, arm):
        validate_effective_params(actual, original_b0_params, arm)
    return validate_bound_cell(output, ident, validate_params)


def _admitted(spec, original_calendar, original_b0_params, contract):
    ident = identity(spec, original_calendar)
    if ident['account_contract'] != contract or ident['arm_id'] == 'B0':
        raise ValueError('only the four new candidates may enter economic execution')
    params = arm_parameters(original_b0_params, ident['arm_id'])
    return ident, params


def execute_cell(prepared, *, spec, original_calendar, original_b0_params,
                 prepared_predictions, initial_capital, max_mark_age_ns,
                 funding, output, progress=None, initialization_lock=None):
    ident, params = _admitted(spec, original_calendar, original_b0_params,
                              INDEPENDENT_CONTRACT)
    if initial_capital != 10000:
        raise ValueError('original account capital differs')
    return execute_bound_cell(prepared, ident=ident, params=params,
        prepared_predictions=prepared_predictions, initial_capital=initial_capital,
        max_mark_age_ns=max_mark_age_ns, funding=funding, output=output,
        progress=progress, initialization_lock=initialization_lock,
        validate_output=partial(validate_cell, spec=spec,
            original_calendar=original_calendar, original_b0_params=original_b0_params),
        full_calendar=True, checkpoint_key=CHECKPOINT_KEY,
        diagnostic_builder=diagnostics, retain_completed_checkpoint=True,
        incremental_journal=True)


def execute_continuous(config, *, original_calendar, original_b0_params, progress=None):
    ident, params = _admitted(config['spec'], original_calendar, original_b0_params,
                              CONTINUOUS_CONTRACT)
    return execute_bound_continuous(config, ident=ident, params=params,
        progress=progress, checkpoint_key=CHECKPOINT_KEY,
        diagnostic_builder=diagnostics,
        validate_output=partial(validate_cell, spec=config['spec'],
            original_calendar=original_calendar, original_b0_params=original_b0_params))
