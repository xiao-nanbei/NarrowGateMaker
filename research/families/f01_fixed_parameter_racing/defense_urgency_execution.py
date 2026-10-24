"""One finite D/U cell through the existing public replay and PoolRun protocol.

No scheduler, retry loop, source rebinding or infrastructure mutation lives here.
Prepared inputs must already be admitted on the executing host's local disk.
"""
import hashlib
import json
import math
from pathlib import Path
import time

from narrowgate.runtime.artifact_io import (
    numeric_json as _numeric, atomic_json as _atomic, load_json as _load, cell_lock as cell_lock,
)

from .defense_urgency import BASELINE, PATCHES, arm_parameters
from .public_input import iter_defense_urgency_candidates

def _identity(spec):
    names = ('experiment_id', 'baseline_id', 'account_contract', 'parent_id',
             'arm_id', 'seed_pair', 'start_ns', 'end_ns', 'input_manifest_id',
             'release_identity')
    result = {k: spec[k] for k in names}
    if (result['experiment_id'] != 'B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407'
            or result['baseline_id'] != BASELINE or result['arm_id'] not in PATCHES
            or result['account_contract'] not in ('DEVELOPMENT300_ACCOUNT_PANEL', 'FINAL107_ACCOUNT_PANEL', 'FINAL107_CONTINUOUS')
            or not result['parent_id'] or not result['release_identity']
            or type(result['start_ns']) is not int or type(result['end_ns']) is not int
            or result['start_ns'] >= result['end_ns']
            or set(result['seed_pair']) != {'rng_seed', 'latency_seed'}):
        raise ValueError('invalid canonical D/U cell identity')
    if result['account_contract'] == 'FINAL107_CONTINUOUS' and (
            result['parent_id'] != 'Final107-continuous'
            or result['end_ns']-result['start_ns'] != 107*86400*10**9):
        raise ValueError('continuous identity must be one global 107-day account')
    return result


def validate_cell(directory, spec):
    """A completion marker alone is insufficient; verify its retained payloads."""
    directory = Path(directory)
    identity = _identity(spec)
    receipt = _load(directory/'completion.json')
    if receipt.get('identity') != identity or receipt.get('economic_complete') is not True:
        raise ValueError('completion identity differs')
    expected = {'result.json', 'accounting.json', 'effective_params.json', 'metrics.json',
                'daily-ledger.json'}
    if set(receipt['payload_sha256']) != expected:
        raise ValueError('incomplete completion payload list')
    for name, digest in receipt['payload_sha256'].items():
        if hashlib.sha256((directory/name).read_bytes()).hexdigest() != digest:
            raise ValueError('completed payload changed: ' + name)
    accounting = _load(directory/'accounting.json')
    result = _load(directory/'result.json')
    metrics = _load(directory/'metrics.json')
    params = _load(directory/'effective_params.json')
    ledger = _load(directory/'daily-ledger.json')
    if (accounting.get('economic_complete') is not True
            or accounting['account_start_ns'] != identity['start_ns']
            or accounting['account_end_ns'] != identity['end_ns']
            or result['public_input_contract']['input_manifest_id'] != identity['input_manifest_id']
            or result['public_input_contract']['account_start_ns'] != identity['start_ns']
            or receipt['durable_cut_ms'] * 10**6 != identity['end_ns']
            or params['account_start_ns'] != identity['start_ns']
            or any(params.get(k) != v for k, v in identity['seed_pair'].items())
            or metrics['net_pnl'] != accounting['all_in_net_pnl']
            or not all(math.isfinite(metrics[k]) for k in ('net_pnl', 'volume_btc', 'notional_usdc'))
            or not math.isclose(accounting['terminal_equity']-accounting['initial_capital'],
                                metrics['net_pnl'], rel_tol=0, abs_tol=1e-8)
            or ledger['accounting'] != accounting
            or ledger['account_reset'] is not False
            or ledger['input_manifest_id'] != identity['input_manifest_id']
            or ledger['account_start_ns'] != identity['start_ns']
            or ledger['account_end_ns'] != identity['end_ns']
            or not math.isclose(math.fsum(r['net_pnl'] for r in ledger['rows']),
                                metrics['net_pnl'], abs_tol=1e-8, rel_tol=0)):
        raise ValueError('incomplete or mismatched cell accounting')
    return receipt


def execute_cell(prepared, *, spec, common_params, prepared_predictions,
                 initial_capital, max_mark_age_ns, funding, output,
                 checkpoint_cuts=(), progress=None):
    """Idempotent cell ownership; resume only the same admitted cell/inputs."""
    from models.replay.runtime_checkpoint_io import (
        save_runtime_checkpoint, load_trusted_runtime_checkpoint,
    )
    identity = _identity(spec)
    if identity['account_contract'] == 'FINAL107_CONTINUOUS':
        raise ValueError('continuous production account requires bounded input execution')
    plan = prepared.inputs['bundle'].manifest['plan']
    if (prepared.manifest_id != identity['input_manifest_id']
            or plan['end_ns'] != identity['end_ns']
            or common_params['account_start_ns'] != identity['start_ns']
            or any(common_params.get(k) != v for k, v in identity['seed_pair'].items())):
        raise ValueError('cell input/date/seed binding differs')
    params, _ = arm_parameters(common_params, identity['arm_id'])
    output = Path(output)
    with cell_lock(output):
        owner = output/'identity.json'
        if owner.exists() and _load(owner) != identity:
            raise ValueError('cell directory already has a different owner')
        _atomic(owner, identity)
        if (output/'completion.json').exists():
            validate_cell(output, spec)
            if _load(output/'effective_params.json') != json.loads(json.dumps(params, default=_numeric)):
                raise ValueError('completed cell parameters changed')
            # Recover a crash after atomic completion but before disposable
            # checkpoint cleanup. The cell lock excludes another executor.
            (output/'latest.checkpoint').unlink(missing_ok=True)
            return {'status': 'ALREADY_COMPLETE', 'identity': identity}
        checkpoint_path = output/'latest.checkpoint'
        checkpoint = (load_trusted_runtime_checkpoint(checkpoint_path)
                      if checkpoint_path.exists() else None)
        if checkpoint is not None and checkpoint.get('defense_urgency_cell') != identity:
            raise ValueError('checkpoint belongs to a different cell')
        start = time.monotonic()

        def saved(arm, state):
            state['defense_urgency_cell'] = identity
            save_runtime_checkpoint(checkpoint_path, state)
            status = dict(status='PARTIAL', identity=identity, durable_cut_ms=state['cut_ts_ms'])
            _atomic(output/'progress.json', status)
            if progress is not None:
                progress(status)

        _, cell = next(iter_defense_urgency_candidates(
            prepared, [identity['arm_id']], common_params=common_params,
            prepared_predictions=prepared_predictions, initial_capital=initial_capital,
            max_mark_age_ns=max_mark_age_ns, funding=funding,
            checkpoint_cuts=checkpoint_cuts, on_checkpoint=saved,
            resume_checkpoint=checkpoint))
        result, accounting = cell['replay'], cell['accounting']
        from models.replay.public_accounting import public_equity_ledger
        day_ns = 86_400*10**9
        ends = list(range((identity['start_ns']//day_ns+1)*day_ns,
                          identity['end_ns'], day_ns)) + [identity['end_ns']]
        ledger = public_equity_ledger(prepared.inputs['bundle'].root, result, ends,
            initial_capital=initial_capital, max_mark_age_ns=max_mark_age_ns, funding=funding)
        fills = result['_economic_fill_trace'] if result.get('private_fill_visibility_enabled') else result['_fill_trace']
        metrics = dict(net_pnl=accounting['all_in_net_pnl'],
                       volume_btc=math.fsum(r['fill_qty'] for r in fills),
                       notional_usdc=math.fsum(r['fill_qty']*r['quote_px'] for r in fills))
        values = dict(result=result, accounting=accounting, metrics=metrics,
                      effective_params=cell['effective_params'])
        values['daily-ledger'] = ledger
        for name, value in values.items():
            _atomic(output/(name+'.json'), value)
        receipt = dict(identity=identity, economic_complete=True,
            durable_cut_ms=identity['end_ns']//10**6, wall_seconds=time.monotonic()-start,
            payload_sha256={name+'.json': hashlib.sha256((output/(name+'.json')).read_bytes()).hexdigest()
                            for name in values})
        _atomic(output/'completion.json', receipt)
        validate_cell(output, spec)
        # The just-finished executor no longer has a recovery consumer. Formal
        # results and identity are retained; only this cell's checkpoint goes.
        checkpoint_path.unlink(missing_ok=True)
        return {'status': 'COMPLETE', 'identity': identity}


def execute_continuous(config, *, progress=None):
    """Same finite ownership/receipt contract, with bounded global input loads."""
    from models.replay.public_input_window import GlobalPublicTimelines
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from .defense_urgency_continuous import continuous_bounds, replay_continuous_final_windows
    from .defense_urgency_results import load_frozen_selection
    spec = config['spec']
    identity = _identity(spec)
    calendar = _load(config['calendar'])
    if (identity['account_contract'] != 'FINAL107_CONTINUOUS'
            or continuous_bounds(calendar) != (identity['start_ns'], identity['end_ns'])
            or config['common_params']['account_start_ns'] != identity['start_ns']
            or any(config['common_params'].get(k) != v for k,v in identity['seed_pair'].items())):
        raise ValueError('continuous configuration dates/seeds differ')
    selection = load_frozen_selection(config['selection'],calendar=calendar,
                                     run_identity=identity['release_identity'])
    if identity['arm_id'] not in ('B0',selection['frozen_challenger']):
        raise ValueError('continuous pair must use the B-frozen challenger')
    output = Path(config['output'])
    with cell_lock(output):
        if (output/'identity.json').exists() and _load(output/'identity.json') != identity:
            raise ValueError('continuous directory already has a different owner')
        _atomic(output/'identity.json',identity)
        params, _ = arm_parameters(config['common_params'],identity['arm_id'])
        if (output/'completion.json').exists():
            validate_cell(output,spec)
            if _load(output/'effective_params.json') != json.loads(json.dumps(params,default=_numeric)):
                raise ValueError('completed continuous parameters changed')
            (output/'latest.checkpoint').unlink(missing_ok=True)
            return {'status':'ALREADY_COMPLETE','identity':identity}
        timelines = GlobalPublicTimelines(config['bundle'],config['timelines'])
        if (timelines.bundle.input_manifest_id != identity['input_manifest_id']
                or timelines.identity != config['timeline_identity']
                or timelines.manifest['model_identity'] != config['timeline_model_identity']):
            raise ValueError('continuous input/model timeline binding differs')
        checkpoint_path = output/'latest.checkpoint'
        checkpoint = load_trusted_runtime_checkpoint(checkpoint_path) if checkpoint_path.exists() else None
        if checkpoint is not None and checkpoint.get('defense_urgency_cell') != identity:
            raise ValueError('continuous checkpoint belongs to a different cell')
        def saved(arm,state):
            state['defense_urgency_cell'] = identity
            save_runtime_checkpoint(checkpoint_path,state)
            status = dict(status='PARTIAL',identity=identity,durable_cut_ms=state['cut_ts_ms'])
            _atomic(output/'progress.json',status)
            if progress is not None:
                progress(status)
        started = time.monotonic()
        cell = replay_continuous_final_windows(timelines,identity['arm_id'],calendar=calendar,
            selection_path=config['selection'],run_identity=identity['release_identity'],
            common_params=config['common_params'],initial_capital=config['initial_capital'],
            max_mark_age_ns=config['max_mark_age_ns'],funding=config['funding'],
            on_checkpoint=saved,resume_checkpoint=checkpoint,**config['window_resources'])
        result, accounting = cell['replay'],cell['accounting']
        fills = result['_economic_fill_trace'] if result.get('private_fill_visibility_enabled') else result['_fill_trace']
        values = {'result':result,'accounting':accounting,'effective_params':cell['effective_params'],
            'daily-ledger':cell['daily_ledger'],'metrics':dict(net_pnl=accounting['all_in_net_pnl'],
                volume_btc=math.fsum(r['fill_qty'] for r in fills),
                notional_usdc=math.fsum(r['fill_qty']*r['quote_px'] for r in fills))}
        for name,value in values.items():
            _atomic(output/(name+'.json'),value)
        _atomic(output/'completion.json',dict(identity=identity,economic_complete=True,
            durable_cut_ms=identity['end_ns']//10**6,wall_seconds=time.monotonic()-started,
            payload_sha256={name+'.json':hashlib.sha256((output/(name+'.json')).read_bytes()).hexdigest()
                            for name in values}))
        validate_cell(output,spec)
        checkpoint_path.unlink(missing_ok=True)
        return {'status':'COMPLETE','identity':identity}


def main():
    """PoolRun business.json adapter; staging is an explicit prior operation."""
    import argparse
    import shutil
    import sys
    from models.backtest_tick import prepare_public_inputs, XMARKET_REPLAY_FEATURE_COLUMNS
    from models.replay.public_input import PreparedPublicPredictions
    from strategy.signal import SignalEngine

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--attempt', type=Path)
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()
    if args.validate:
        request = json.load(sys.stdin)
        try:
            receipt = validate_cell(request['outputs'], request['attempt']['spec']['parameters']['cell'])
            print(json.dumps(dict(valid=True, files=[*receipt['payload_sha256'], 'completion.json'],
                                  summary={'identity': receipt['identity'], 'economic_complete': True})))
        except (ValueError, KeyError, OSError) as error:
            print(json.dumps(dict(valid=False, files=[], summary={'error': str(error)})))
        return
    if args.attempt is None:
        parser.error('--attempt is required for execution')
    parameters = _load(args.attempt/'business.json')['parameters']
    config_path = Path(parameters['config'])
    config = _load(config_path)
    if hashlib.sha256(config_path.read_bytes()).hexdigest() != parameters['config_sha256']:
        raise ValueError('PoolRun config binding changed')
    if config['spec'] != parameters['cell']:
        raise ValueError('PoolRun task and cell differ')
    if config['spec']['account_contract'] == 'FINAL107_CONTINUOUS':
        execute_continuous(config,progress=lambda value:_atomic(args.attempt/'progress.json',value))
    else:
        prepared = prepare_public_inputs(config['bundle'],
            tick_size=config['common_params']['tick_size'], cache_dir=config['cache'],
            require_existing_cache=True)
        if 'timelines' in config:
            from models.replay.public_input_window import GlobalPublicTimelines
            timelines = GlobalPublicTimelines(config['bundle'],config['timelines'])
            if (timelines.identity != config['timeline_identity']
                    or timelines.manifest['model_identity'] != config['timeline_model_identity']):
                raise ValueError('panel frozen prediction binding differs')
            predictions = timelines.predictions_for(prepared)
        else:
            signal = SignalEngine.from_public_models(config['model'],
                symbol=config['common_params']['symbol'],
                ret_demean_halflife=config['common_params']['ret_demean_halflife'])
            predictions = PreparedPublicPredictions.create(prepared, signal, XMARKET_REPLAY_FEATURE_COLUMNS)
        execute_cell(prepared, spec=config['spec'], common_params=config['common_params'],
            prepared_predictions=predictions, initial_capital=config['initial_capital'],
            max_mark_age_ns=config['max_mark_age_ns'], funding=config['funding'],
            output=config['output'], checkpoint_cuts=config['checkpoint_cuts'],
            progress=lambda value: _atomic(args.attempt/'progress.json', value))
    # A canonical cell survives Attempt replacement. PoolRun receives a verified
    # result copy, not a second executor or a renamed historical B0 result.
    with cell_lock(config['output']):
        receipt = validate_cell(config['output'], config['spec'])
        outputs = args.attempt/'outputs'
        outputs.mkdir(exist_ok=True)
        for name in receipt['payload_sha256']:
            shutil.copy2(Path(config['output'])/name, outputs/name)
        _atomic(outputs/'completion.json', receipt)
        validate_cell(outputs, config['spec'])


if __name__ == '__main__':
    main()
