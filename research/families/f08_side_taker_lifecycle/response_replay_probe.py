"""Bounded B0 opportunity diagnostic using the existing prepared replay.

No complete-account settlement, fitting, candidate selection, or automatic retry.
The private plan supplies original parent boundaries, models and timing samples.
"""
import argparse
import json
from pathlib import Path
import resource
import sys
import time
from contextlib import nullcontext

import numpy as np


def numeric(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(type(value).__name__)


def validate_plan(plan):
    from data.runtime import ConsumerBundle
    bundle = ConsumerBundle(plan['bundle'])
    parent = bundle.manifest['plan']
    start, end = plan['account_start_ns'], plan['account_end_ns']
    if (type(start) is not int or type(end) is not int or
            not parent['start_ns'] <= start < end or end != parent['end_ns'] or
            end-start != 172800_000_000_000 or not parent.get('include_response_observations')):
        raise ValueError('complete original warmed parent and retained observations required')
    from datetime import datetime, timezone
    from research.families.f03_causal_13_head.time_weighted_evaluation import CALENDAR
    if any(datetime.fromtimestamp(t//1_000_000_000, timezone.utc).date().isoformat() not in CALENDAR[:300]
           for t in (start, end-1)):
        raise ValueError('probe is restricted to non-Final development parents')
    params = plan['params']
    if (params.get('risk_selection_mode', 'B') != 'B' or
            params.get('quote_schedule_mode', 'existing') != 'existing' or
            params.get('replace_price_threshold_mode', 'fixed') != 'fixed'):
        raise ValueError('probe must retain B0 without E/C, U1, or dynamic thresholds')
    return bundle


def run_probe(plan, output):
    validate_plan(plan)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    status = dict(visibility='local_only_do_not_publish', status='preparing',
                  path_id='B0', economic_complete=False, simulated_seconds=900,
                  account_start_ns=plan['account_start_ns'],
                  parent_account_end_ns=plan['account_end_ns'])
    wall, cpu = time.monotonic(), time.process_time()

    def save_status():
        status['wall_seconds'] = time.monotonic()-wall
        status['cpu_seconds'] = time.process_time()-cpu
        (output/'run.json').write_text(json.dumps(status, indent=2, default=numeric)+'\n')

    save_status()
    try:
        from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs, XMARKET_REPLAY_FEATURE_COLUMNS
        from models.replay.public_input import PreparedPublicPredictions
        from strategy.signal import SignalEngine
        from research.system_engineering.audit.rest_latency_calibration import load_runtime_timing_samples
        from research.families.f01_fixed_parameter_racing.inventory_lifecycle_outcome_replay_audit import _runtime_compute_for_window
        params = dict(plan['params'], account_start_ns=plan['account_start_ns'], response_observe_only=True)
        prepared = prepare_public_inputs(plan['bundle'], tick_size=params['tick_size'], cache_dir=plan['cache'])
        signal = SignalEngine.from_public_models(plan['model'], symbol=params['symbol'],
                                                ret_demean_halflife=params['ret_demean_halflife'])
        predictions = PreparedPublicPredictions.create(prepared, signal, XMARKET_REPLAY_FEATURE_COLUMNS)
        calibration = load_runtime_timing_samples(plan['runtime_timing_samples'],
            effective_time_assumption='exchange_event_proxy')['calibration']
        params.update(_runtime_compute_for_window({'ml_data': predictions.for_inputs(prepared)},
            params, calibration, clock='source_time_assumption', start_ms=plan['account_start_ns']//1_000_000))
        (output/'effective_params.json').write_text(json.dumps(params, default=numeric, allow_nan=False)+'\n')
        status.update(status='bounded_replay_running', preparation_seconds=time.monotonic()-wall)
        save_status()
        def progress(value):
            status['progress'] = value
            save_status()
        params['_replay_progress_callback'] = progress
        timing = None
        measurement = nullcontext()
        if plan.get('phase_timing', False):
            from models.replay.phase_timing import ReplayPhaseTiming
            from data.response_cursor import ResponseObservationCursor, _Rows
            from features.trade_book_response import ResponseState
            from models.replay.public_input import PreparedReplayInputs
            from strategy.update_opportunity import UpdateOpportunityObserver
            from models.exchange_book_replay import HistoricalExchangeBookScheduler
            timing = ReplayPhaseTiming()
            params['_replay_phase_callback'] = timing.mark
            methods = [(PreparedReplayInputs, name, name) for name in
                       ('account_trades', 'event_clock', 'book_inputs', 'historical_book_coverage')]
            methods += [(ResponseObservationCursor, 'advance', 'observation_advance'),
                        (HistoricalExchangeBookScheduler, 'advance_to', 'exchange_book_advance'),
                        (_Rows, 'peek', 'observation_read'),
                        (ResponseState, 'observe_book', 'response_book_update'),
                        (ResponseState, 'observe_trade', 'response_trade_update'),
                        (ResponseState, 'order_frame', 'order_feature_materialization'),
                        (UpdateOpportunityObserver, 'observe', 'eligibility_and_observation')]
            measurement = timing.measure_methods(methods)
            timing.mark('input_admission_and_validation')
        replay_wall, replay_cpu = time.monotonic(), time.process_time()
        with measurement:
            result = simulate_prepared_inputs(prepared, params, prepared_predictions=predictions,
                                              execution_end_ns=plan['account_start_ns']+900_000_000_000)
        elapsed = time.monotonic()-replay_wall
        if timing is not None:
            timing.mark(None)
            (output/'replay_phase_timing.json').write_text(json.dumps(timing.report(), indent=2)+'\n')
        status.update(replay_wall_seconds=elapsed, replay_cpu_seconds=time.process_time()-replay_cpu,
                      actual_strategy_speed_multiple=900/elapsed,
                      status='bounded_observation_complete_not_economic_complete',
                      candidate_speed_admission=False)
        (output/'replay.json').write_text(json.dumps(result, default=numeric, allow_nan=True)+'\n')
        (output/'opportunities.json').write_text(json.dumps(result['response_update_observation'],
                                                            default=numeric, allow_nan=False)+'\n')
    except BaseException as exc:
        status.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        status['peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        status['output_bytes'] = sum(p.stat().st_size for p in output.rglob('*') if p.is_file())
        save_status()
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_probe(json.loads(args.plan.read_text()), args.output)))


if __name__ == '__main__':
    main()
