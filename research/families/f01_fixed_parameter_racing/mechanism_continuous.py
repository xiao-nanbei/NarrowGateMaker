"""Bounded continuous execution after an owner validates its account contract.

Explicit hour/day execution windows preserve carried account/cursor semantics.
Durable cadence is independent of lightweight progress observations. This
module neither schedules tasks nor admits inputs.
"""
from copy import deepcopy
import json
import math
from pathlib import Path
import resource
import time
import uuid

from narrowgate.runtime.artifact_io import (
    atomic_json as _atomic, load_json as _load, numeric_json as _numeric, cell_lock,
)
from .mechanism_ablation import arm_parameters
from narrowgate.replay.account import DAY_NS, diagnostics, sha
from .mechanism_execution import identity, validate_cell

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS


def replay_windows(timelines, params, *, start_ns, end_ns, journal,
                   checkpoint=None, persist=None, progress=None,
                   durable_interval_ms=DAY_MS, window_ms=HOUR_MS,
                   context_ms=60_000, lookahead_ms=60_000, before_window=None):
    """Window-only primitive; caller owns the immutable account contract."""
    from models.backtest_tick import simulate_prepared_inputs
    from models.replay.runtime_input_window import runtime_input_context_start

    if (type(durable_interval_ms) is not int or type(window_ms) is not int
            or window_ms not in (HOUR_MS, DAY_MS)
            or durable_interval_ms not in (HOUR_MS, DAY_MS)
            or durable_interval_ms < window_ms
            or context_ms != 60_000 or lookahead_ms != 60_000
            or params['account_start_ns'] != start_ns
            or timelines.bundle.manifest['plan']['end_ns'] != end_ns
            or timelines.bundle.manifest['plan']['start_ns'] > start_ns):
        raise ValueError('continuous window/input/initialization contract differs')
    actual = dict(params, _l2_journal=journal)
    start, end = start_ns//10**6, end_ns//10**6
    cursor = start if checkpoint is None else checkpoint['cut_ts_ms']
    if not start <= cursor < end:
        raise ValueError('continuous resume cut outside account')
    durable = None if checkpoint is None else checkpoint['cut_ts_ms']
    durable_prefix = None if checkpoint is None else checkpoint['l2_prefix']['manifest']
    previous_callback = actual.get('_replay_progress_callback')
    last_observed_hour = None
    def in_day_progress(value):
        nonlocal last_observed_hour
        if previous_callback is not None:
            previous_callback(value)
        hour = int(value['event_ts_ms']) // HOUR_MS
        if progress is not None and hour != last_observed_hour:
            last_observed_hour = hour
            progress(dict(status='IN_DAY_REPLAY', event_ts_ms=int(value['event_ts_ms']),
                durable_cut_ms=durable, execution_window_ms=window_ms,
                event_index=value['event_index'], event_count=value['event_count']))
    if window_ms == DAY_MS:
        actual['_replay_progress_callback'] = in_day_progress
    while cursor < end:
        began = time.monotonic()
        # A recovered interior cut continues to the next original UTC/account
        # boundary; it must not shift every later day by its recovery offset.
        cut = min(start + ((cursor-start)//window_ms+1)*window_ms, end)
        if before_window is not None:
            before_window(timelines, actual, checkpoint, cursor, cut)
        begin = timelines.bundle.manifest['plan']['start_ns']//10**6
        context = {}
        old_prefix = None
        if checkpoint is not None:
            old_prefix = checkpoint['l2_prefix']['manifest']
            feature_begin = runtime_input_context_start(checkpoint['runtime'], cursor-context_ms, context_ms)
            begin = runtime_input_context_start(checkpoint['runtime'], cursor-context_ms, context_ms, market_only=True)
            context['feature_begin_ns'] = min(feature_begin, begin)*10**6
        prepared, predictions = timelines.prepare(begin*10**6, min(cut+lookahead_ms, end)*10**6,
                                                   tick_size=params['tick_size'], **context)
        prepared_seconds = time.monotonic()-began
        replay_started = time.monotonic()
        result = simulate_prepared_inputs(prepared, actual, prepared_predictions=predictions,
            checkpoint_at_ts_ms=None if cut == end else cut, resume_checkpoint=checkpoint,
            consume_resume_checkpoint=checkpoint is not None)
        replay_seconds = time.monotonic()-replay_started
        save_seconds = 0.0
        if cut < end:
            checkpoint = result['_replay_checkpoint']
            if checkpoint['cut_ts_ms'] != cut:
                raise ValueError('continuous engine did not reach requested cut')
            if persist is not None and (durable is None or (cut-start) % durable_interval_ms == 0):
                save_started = time.monotonic()
                previous_durable = durable_prefix
                persist(checkpoint)
                durable, durable_prefix = cut, checkpoint['l2_prefix']['manifest']
                save_seconds = time.monotonic()-save_started
                if previous_durable != durable_prefix:
                    _retire_prefix(previous_durable, journal)
            if old_prefix != durable_prefix:
                _retire_prefix(old_prefix, journal)
        cursor = cut
        if progress:
            progress(dict(status='PARTIAL' if cut < end else 'SETTLING', memory_cut_ms=cut,
                durable_cut_ms=durable, prepared_seconds=prepared_seconds,
                replay_seconds=replay_seconds, save_seconds=save_seconds,
                execution_window_ms=window_ms,
                peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
        del prepared, predictions
    return result


def _retire_prefix(value, journal):
    """Only unused output-manifest snapshots, never payloads or a recovery root."""
    if value is not None:
        path = Path(value)
        if path.parent == journal.writer.output_dir and path.name.startswith('prefix-'):
            path.unlink(missing_ok=True)


def execute_continuous(config, *, progress=None):
    ident = identity(config['spec'])
    if ident['account_contract'] != 'MECHANISM407_FINAL107_CONTINUOUS' or ident['arm_id'] == 'B0':
        raise ValueError('new execution is limited to the two candidate continuous paths')
    return execute_bound_continuous(config, ident=ident,
        params=arm_parameters(config['common_params'], ident['arm_id']),
        validate_output=lambda path: validate_cell(path, ident), progress=progress)


def execute_bound_continuous(config, *, ident, params, validate_output,
                             progress=None, checkpoint_key='mechanism_cell',
                             diagnostic_builder=diagnostics, before_window=None,
                             durable_interval_ms=DAY_MS, parameter_record=None,
                             window_ms=HOUR_MS):
    """Account-neutral window executor after the owning contract is checked."""
    from models.replay.continuous_l2_journal import ContinuousReplayL2Journal
    from models.replay.public_input_window import GlobalPublicTimelines
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from models.replay.public_accounting import public_equity_ledger

    if (params['account_start_ns'] != ident['start_ns'] or config['initial_capital'] != 10000
            or params.get('initial_live_state')
            or params.get('replay_initial_state_mode', 'fresh_start') != 'fresh_start'):
        raise ValueError('continuous account initialization differs')
    timelines = GlobalPublicTimelines(config['bundle'], config['timelines'])
    if (timelines.bundle.input_manifest_id != ident['input_manifest_id']
            or timelines.identity != config['timeline_identity']
            or timelines.manifest['model_identity'] != config['timeline_model_identity']):
        raise ValueError('continuous global input/model binding differs')
    output = Path(config['output'])
    recorded_params = params if parameter_record is None else parameter_record(params)
    with cell_lock(output):
        if (output/'identity.json').exists() and _load(output/'identity.json') != ident:
            raise ValueError('foreign continuous output')
        _atomic(output/'identity.json', ident)
        if (output/'completion.json').exists():
            validate_output(output)
            if _load(output/'effective_params.json') != json.loads(json.dumps(recorded_params, default=_numeric)):
                raise ValueError('completed continuous parameters changed')
            return {'status': 'ALREADY_COMPLETE', 'identity': ident}
        checkpoint_path = output/'latest.checkpoint'
        checkpoint = load_trusted_runtime_checkpoint(checkpoint_path) if checkpoint_path.exists() else None
        if checkpoint is not None and checkpoint.get(checkpoint_key) != ident:
            raise ValueError('foreign continuous checkpoint')
        journal = ContinuousReplayL2Journal(output/'journal'/uuid.uuid4().hex, identity=ident)
        def saved(state):
            state[checkpoint_key] = ident
            save_runtime_checkpoint(checkpoint_path, state)
        def observed(value):
            value = dict(value, identity=ident)
            _atomic(output/'progress.json', value)
            if progress:
                progress(value)
        result = replay_windows(timelines, params, start_ns=ident['start_ns'], end_ns=ident['end_ns'],
            journal=journal, checkpoint=checkpoint, persist=saved, progress=observed,
            before_window=before_window, durable_interval_ms=durable_interval_ms,
            window_ms=window_ms)
        ledger = public_equity_ledger(timelines.bundle.root, result,
            range(ident['start_ns']+DAY_NS, ident['end_ns']+1, DAY_NS),
            initial_capital=config['initial_capital'], max_mark_age_ns=config['max_mark_age_ns'],
            funding=deepcopy(config['funding']))
        accounting = ledger['accounting']
        fills = result['_economic_fill_trace'] if result.get('private_fill_visibility_enabled') else result['_fill_trace']
        metrics = dict(net_pnl=accounting['all_in_net_pnl'], volume_btc=math.fsum(x['fill_qty'] for x in fills),
                       notional_usdc=math.fsum(x['fill_qty']*x['quote_px'] for x in fills))
        values = {'result': result, 'accounting': accounting, 'daily-ledger': ledger,
                  'effective_params': recorded_params, 'metrics': metrics,
                  'diagnostics': diagnostic_builder(result, result['_l2_journal'], full_calendar=True)}
        values['diagnostics']['account_scope'] = 'one continuous Final107 path; daily values are not independent accounts'
        for name, value in values.items():
            _atomic(output/(name+'.json'), value)
        files = [output/(name+'.json') for name in values]
        files += [p for p in journal.writer.output_dir.rglob('*') if p.is_file()]
        _atomic(output/'completion.json', dict(identity=ident, economic_complete=True,
            durable_cut_ms=ident['end_ns']//10**6,
            payload_sha256={str(p.relative_to(output)): sha(p) for p in files}))
        validate_output(output)
        return {'status': 'COMPLETE', 'identity': ident}
