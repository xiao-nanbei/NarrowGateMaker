"""Adaptive depth-price account adapter for the maintained replay owner.

No execution loop, fitting, placement or input generation lives here. A task
binds the original B0 config, accepted local input locations and measured costs.
"""

import argparse
from copy import deepcopy
from datetime import date, timedelta
import json
from pathlib import Path
import shutil
import sys
import time

from narrowgate.replay.account import (
    DAY_MS, DAY_NS, effective_parameter_record, execute_bound_cell, sha, validate_bound_cell,
)
from narrowgate.runtime.artifact_io import atomic_json, load_json
from research.families.f01_fixed_parameter_racing.defense_urgency import arm_parameters
from research.families.f08_side_taker_lifecycle.depth_price_replay import DepthPriceReplayRuntime
from research.families.f08_side_taker_lifecycle.depth_quote_runtime import DepthQuotingPolicy
from research.families.f08_side_taker_lifecycle.depth_information import admitted_observation

EXPERIMENT = 'F08_DEPTH_PRICING_ADAPTIVE_20261024'
QUOTING_EXPERIMENT = 'F08_DEPTH_QUOTING_20261024'
BASELINE = 'B0_20261007_02'
ARMS = {'B0', 'PRICE_ONLY', 'NEAR_STATIC', 'NEAR_DYNAMIC', 'DEEP_STATIC', 'DEEP_DYNAMIC'}


def bind_depth_source(binding, baseline, *, measured_latency_location=None):
    """Bind the original complete account, not the one-hour engineering cut."""
    source = dict(schema='f08.depth-price-source.v1', bundle=binding['bundle'],
        input_manifest_id=baseline['spec']['input_manifest_id'],
        start_ns=baseline['spec']['start_ns'], end_ns=baseline['spec']['end_ns'],
        frozen_information=dict(bundle=binding['bundle'], root=binding['timelines'],
            identity=baseline['timeline_identity'], model_identity=baseline['timeline_model_identity']))
    if measured_latency_location is not None:
        source['measured_latency_location'] = str(measured_latency_location)
    admitted_observation(source)
    return source


def validate_depth_source(source, binding, baseline, ident):
    if (source.get('schema') != 'f08.depth-price-source.v1'
            or source.get('bundle') != binding['bundle']
            or source.get('input_manifest_id') != ident['input_manifest_id']
            or (source.get('start_ns'), source.get('end_ns')) != (ident['start_ns'], ident['end_ns'])
            or source.get('frozen_information') != dict(bundle=binding['bundle'], root=binding['timelines'],
                identity=baseline['timeline_identity'], model_identity=baseline['timeline_model_identity'])):
        raise ValueError('full parent delivered-depth source differs from account binding')
    admitted_observation(source)


def verify_final_selection(policy):
    """Bind the selected control to the completed Development record."""
    binding = policy.get('development_selection')
    if not isinstance(binding, dict) or set(binding) != {'path', 'sha256'}:
        raise ValueError('Final fixed control requires the frozen Development record')
    source = Path(binding['path'])
    if sha(source) != binding['sha256']:
        raise ValueError('frozen Development selection changed')
    record = load_json(source)
    selected = record.get('selection', {})
    if (record.get('experiment') != EXPERIMENT or record.get('baseline') != BASELINE
            or selected.get('parents') != 150 or selected.get('days') != 300
            or selected.get('final_outcomes_used') is not False
            or selected.get('constant') != policy['constant']):
        raise ValueError('Final control differs from full Development selection')
    if selected.get('reuse_baseline'):
        raise ValueError('selected zero control reuses B0; do not rerun it with extra cost')
    return record


def validate_identity(ident, baseline, policy):
    quoting = ident['experiment_id'] == QUOTING_EXPERIMENT
    if (ident['experiment_id'] not in {EXPERIMENT, QUOTING_EXPERIMENT} or ident['baseline_id'] != BASELINE
            or policy['arm'] not in (DepthQuotingPolicy.ARMS if quoting else ARMS)
            or ident['seed_pair'] != {'rng_seed': 42, 'latency_seed': 59}
            or ident['account_contract'] != ('DEPTH_QUOTING_FINAL107' if quoting else 'DEPTH_PRICE_INDEPENDENT407')):
        raise ValueError('depth-price experiment/account identity differs')
    if quoting and (not ident['parent_id'].startswith('final-')
                    or policy.get('schema') != DepthQuotingPolicy.SCHEMA
                    or ident.get('cost_profile', {}).get('status') != 'FROZEN_REAL_PREFLIGHT'
                    or not any(ident.get('cost_profile', {}).get('ns_per_unit', {}).values())):
        raise ValueError('new Final quoting policy needs its own schema and real frozen cost profile')
    for key in ('input_manifest_id','release_identity'):
        value = ident[key]
        if not isinstance(value,str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('invalid source identity')
    start_date = date(2025, 8, 1)
    expected = {}
    for i in range(204):
        parent = f"{'development' if i < 150 else 'final'}-{i if i < 150 else i-150:03d}-{start_date+timedelta(days=2*i)}"
        expected[parent] = (1754006400000000000+2*i*DAY_NS,
                            1754006400000000000+min(2*i+2,407)*DAY_NS)
    if expected.get(ident['parent_id']) != (ident['start_ns'], ident['end_ns']):
        raise ValueError('foreign parent/calendar')
    original = baseline['spec']
    if any(ident[k] != original[k] for k in ('parent_id','start_ns','end_ns','input_manifest_id','seed_pair')):
        raise ValueError('original B0 input/account identity differs')
    if original['arm_id'] != 'DEFENSE_ON__URGENCY_OFF':
        raise ValueError('wrong versioned B0 comparator')
    if policy['arm'] == 'PRICE_ONLY':
        constant = policy.get('constant')
        if not isinstance(constant, list) or len(constant) != 2 or any(v not in (-.1,0.,.1) for v in constant):
            raise ValueError('constant control outside declared nine-point grid')
        if ident['parent_id'].startswith('final') and not policy.get('development_selection'):
            raise ValueError('Final PRICE_ONLY requires frozen Development selection')
        expected_arm = 'PRICE_ONLY_u%+03d_v%+03d' % tuple(round(v*100) for v in constant)
    else:
        expected_arm = policy['arm']
        if policy.get('constant') is not None:
            raise ValueError('constant supplied to nonconstant arm')
    if ident['arm_id'] != expected_arm or ident['policy'] != policy:
        raise ValueError('policy identity differs')
    return deepcopy(ident)


def bound_configuration(parameters):
    binding = parameters['binding']
    source = Path(binding['baseline_config'])
    if sha(source) != binding['baseline_config_sha256']:
        raise ValueError('original B0 configuration changed')
    baseline = load_json(source)
    policy = parameters['policy']
    ident = validate_identity(parameters['cell'], baseline, policy)
    if policy['arm'] == 'PRICE_ONLY' and ident['parent_id'].startswith('final-'):
        verify_final_selection(policy)
    params, _ = arm_parameters(baseline['common_params'], 'DEFENSE_ON__URGENCY_OFF')
    # Only explicit, content-verified model location adaptation is allowed.
    for key, location in binding.get('parameter_locations', {}).items():
        if key not in {'model_dir','resolved_model_dir','touch_probability_model_path'}:
            raise ValueError('unsupported parameter location adaptation')
        path = Path(location['path'])
        if not location['files'] or path.is_symlink():
            raise ValueError('explicit model content binding required')
        for name, digest in location['files'].items():
            file = path/name if path.is_dir() else path
            if sha(file) != digest:
                raise ValueError('model location content differs')
        params[key] = str(path)
    if policy['arm'] != 'B0':
        costs = parameters['cost_profile']
        if ident['cost_profile'] != costs:
            raise ValueError('cost profile differs from account identity')
        source_config = parameters.get('depth_source')
        if policy['arm'] != 'PRICE_ONLY':
            if source_config is None:
                raise ValueError('full parent delivered-depth source required')
            validate_depth_source(source_config, binding, baseline, ident)
        params['_quote_price_runtime'] = DepthPriceReplayRuntime(
            arm=policy['arm'], tick_size=params['tick_size'], cost_profile=costs,
            constant=policy.get('constant'), source_config=source_config)
    elif parameters.get('depth_source') is not None or parameters.get('cost_profile') is not None:
        raise ValueError('B0 must not acquire extra policy state/cost')
    return baseline, ident, params


def validate_output(output, parameters):
    _, ident, params = bound_configuration(parameters)
    expected = effective_parameter_record(params)

    def validate_parameters(actual, arm):
        if arm != ident['arm_id'] or actual != expected:
            raise ValueError('effective depth-price parameters differ')

    receipt = validate_bound_cell(output, ident, validate_parameters)
    if ident['experiment_id'] == QUOTING_EXPERIMENT:
        result = load_json(Path(output)/'result.json')
        diagnostic = load_json(Path(output)/'diagnostics.json')
        audit = result.get('quote_price_policy', {})
        captures = audit.get('counts', {}).get('captures', 0)
        if (audit.get('contract', {}).get('schema') != 'quote-price-replay-v2'
                or diagnostic.get('events', {}).get('depth_quoting_capture', 0) != captures
                or audit.get('contract', {}).get('arm') != ident['arm_id']):
            raise ValueError('complete depth-quoting capture panel missing or mismatched')
    return receipt


def execute(parameters, output, *, progress=None):
    from models.backtest_tick import prepare_public_inputs
    from models.replay.public_accounting import funding_window
    from models.replay.public_input_window import GlobalPublicTimelines

    baseline, ident, params = bound_configuration(parameters)
    b = parameters['binding']
    cache = Path(b['cache']).resolve()
    if sys.platform == 'darwin' and str(cache).startswith('/Volumes/'):
        raise ValueError('external disk replay is not admitted')
    prepared = prepare_public_inputs(b['bundle'], tick_size=params['tick_size'],
                                    cache_dir=cache, require_existing_cache=True)
    if prepared.manifest_id != ident['input_manifest_id']:
        raise ValueError('accepted prepare version differs')
    timelines = GlobalPublicTimelines(b['bundle'], b['timelines'])
    if (timelines.identity != baseline['timeline_identity']
            or timelines.manifest['model_identity'] != baseline['timeline_model_identity']):
        raise ValueError('original frozen predictions changed')
    if progress is not None:
        params['_replay_progress_callback'] = progress
    return execute_bound_cell(prepared, ident=ident, params=params,
        prepared_predictions=timelines.predictions_for(prepared),
        initial_capital=baseline['initial_capital'], max_mark_age_ns=baseline['max_mark_age_ns'],
        funding=funding_window(baseline['funding'], start_ns=ident['start_ns'], end_ns=ident['end_ns']),
        output=output, validate_output=lambda path: validate_output(path,parameters),
        full_calendar=True, progress=progress, checkpoint_key='depth_price_cell',
        incremental_journal=True, durable_interval_ms=DAY_MS)


def worker(attempt_path):
    root = Path(attempt_path)
    attempt = load_json(root/'attempt.json')
    parameters = attempt['spec']['parameters']
    if attempt['host'] not in ('local','oci-tokyo','oci-tokyo-4','shanghai'):
        raise ValueError('host not authorized for this experiment')
    if shutil.disk_usage(root).free < 32*1024**3 + attempt['resources']['disk_bytes']:
        raise ValueError('execution disk reserve would be crossed')
    output = root/'work'/'account'
    last = [0.]

    def progress(row):
        if time.monotonic()-last[0] >= 15 or row.get('memory_cut_ms'):
            atomic_json(root/'work'/'economic-progress.json',dict(row,wall_time=time.time()))
            last[0] = time.monotonic()

    result = execute(parameters, output, progress=progress)
    receipt = validate_output(output,parameters)
    destination = root/'outputs'
    for name in [*receipt['payload_sha256'],'completion.json']:
        target = destination/name
        target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists():
            if sha(target) != sha(output/name):
                raise ValueError('conflicting output publication')
        else:
            shutil.copy2(output/name,target)
    validate_output(destination,parameters)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--attempt')
    group.add_argument('--validate',action='store_true')
    args = parser.parse_args()
    if args.validate:
        request = json.load(sys.stdin)
        receipt = validate_output(request['outputs'],request['attempt']['spec']['parameters'])
        print(json.dumps(dict(valid=True, files=[*receipt['payload_sha256'],'completion.json'],
                              summary=dict(identity=receipt['identity'],economic_complete=True))))
    else:
        print(json.dumps(worker(args.attempt)))


if __name__ == '__main__':
    main()
