"""One continuous D/U Final account, never a stitched independent panel.

The input owner must represent the full chronological account. This entry does
not turn existing per-parent caches into global inputs or admit their resource
footprint; bounded production staging remains an explicit caller responsibility.
"""
from datetime import datetime

from .defense_urgency import BASELINE
from .defense_urgency_results import load_frozen_selection
from .public_input import iter_defense_urgency_candidates


def continuous_bounds(calendar):
    contract = calendar['continuous_final']
    def timestamp(value):
        date = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if date.utcoffset() is None:
            raise ValueError('continuous calendar requires explicit UTC timestamps')
        return int(date.timestamp())*10**9
    start, end = timestamp(contract['start_utc']), timestamp(contract['end_utc_exclusive'])
    if (contract['account_contract'] != 'FINAL107_CONTINUOUS'
            or contract['baseline_id'] != BASELINE
            or contract['initialize_economic_state_once'] is not True
            or contract['reset_at_48h'] is not False
            or contract['preserve_response_state_and_compute_watermarks'] is not True
            or end-start != 107*86400*10**9
            or contract['scored_days'] != 107):
        raise ValueError('not the authorized continuous Final107 contract')
    return start, end


def admit_continuous_inputs(prepared, common_params, *, calendar, arm,
                            selection_path, run_identity):
    selection = load_frozen_selection(selection_path, calendar=calendar, run_identity=run_identity)
    if arm not in ('B0', selection['frozen_challenger']):
        raise ValueError('continuous pair is B0 and the B-frozen challenger only')
    start, end = continuous_bounds(calendar)
    plan = prepared.inputs['bundle'].manifest['plan']
    if (common_params['account_start_ns'] != start or plan['end_ns'] != end
            or plan['start_ns'] > start):
        raise ValueError('continuous input must bind the global interval, not a 48h parent')
    return start, end


def replay_continuous_final(prepared, arm, *, calendar, selection_path, run_identity,
                             common_params, prepared_predictions, initial_capital,
                             max_mark_age_ns, funding, checkpoint_cuts=(),
                             on_checkpoint=None, resume_checkpoint=None):
    """Engineering checkpoints continue state; settlement happens only once.

    The same immutable input/prediction binding and global account endpoint are
    used at every cut. The existing runtime checkpoint retains actual orders,
    pending notifications, risk/response history, RNG and compute watermarks.
    No state is reconstructed from account summaries or daily ledger rows.
    """
    from models.replay.public_accounting import public_equity_ledger
    start, end = admit_continuous_inputs(prepared, common_params, calendar=calendar,
        arm=arm, selection_path=selection_path, run_identity=run_identity)
    _, cell = next(iter_defense_urgency_candidates(prepared, [arm],
        common_params=common_params, prepared_predictions=prepared_predictions,
        initial_capital=initial_capital, max_mark_age_ns=max_mark_age_ns, funding=funding,
        checkpoint_cuts=checkpoint_cuts, on_checkpoint=on_checkpoint,
        resume_checkpoint=resume_checkpoint))
    cell['daily_ledger'] = public_equity_ledger(prepared.inputs['bundle'].root,
        cell['replay'], range(start+86400*10**9, end+1, 86400*10**9),
        initial_capital=initial_capital, max_mark_age_ns=max_mark_age_ns, funding=funding)
    cell['account_contract'] = 'FINAL107_CONTINUOUS'
    return cell


def replay_continuous_final_windows(timelines, arm, *, calendar, selection_path, run_identity,
        common_params, initial_capital, max_mark_age_ns, funding, on_checkpoint=None,
        resume_checkpoint=None, window_ms=3_600_000, context_ms=60_000,
        lookahead_ms=60_000, separate_feature_context=False,
        durable_interval_ms=None):
    """Bounded in-memory windows; optional less frequent durable callbacks.

    Default behavior is unchanged. A larger durable interval never enlarges
    the market window or resets economic state. Always publish the first
    completed window after startup/recovery, then account-aligned intervals.
    The callback remains responsible for atomic, verified persistence.
    """
    from models.backtest_tick import simulate_prepared_inputs
    from models.replay.runtime_input_window import runtime_input_context_start
    from models.replay.public_accounting import public_equity_ledger
    from .defense_urgency import arm_parameters
    start, end = continuous_bounds(calendar)
    if (min(window_ms, context_ms, lookahead_ms) <= 0
            or lookahead_ms < 30_000):
        raise ValueError('invalid bounded replay resource/lookahead contract')
    durable_interval_ms = window_ms if durable_interval_ms is None else durable_interval_ms
    if (type(durable_interval_ms) is not int or durable_interval_ms < window_ms
            or durable_interval_ms % window_ms or durable_interval_ms > 86_400_000):
        raise ValueError('durable interval must be whole windows and at most one day')
    params, _ = arm_parameters(common_params, arm)
    checkpoint = resume_checkpoint
    cursor = start//10**6 if checkpoint is None else checkpoint['cut_ts_ms']
    if not start//10**6 <= cursor < end//10**6:
        raise ValueError('continuous checkpoint lies outside global account')
    first_window = True
    while cursor < end//10**6:
        cut = min(cursor+window_ms, end//10**6)
        begin = timelines.bundle.manifest['plan']['start_ns']//10**6
        feature_context = {}
        if checkpoint is not None:
            begin = runtime_input_context_start(checkpoint['runtime'], cursor-context_ms, context_ms)
            if separate_feature_context:
                feature_context['feature_begin_ns'] = begin*10**6
                begin = runtime_input_context_start(checkpoint['runtime'], cursor-context_ms,
                                                    context_ms, market_only=True)
                feature_context['feature_begin_ns'] = min(feature_context['feature_begin_ns'],
                                                          begin*10**6)
        loaded_end = min(cut+lookahead_ms, end//10**6)
        prepared, predictions = timelines.prepare(begin*10**6, loaded_end*10**6,
                                                   tick_size=params['tick_size'], **feature_context)
        admit_continuous_inputs(prepared, common_params, calendar=calendar, arm=arm,
            selection_path=selection_path, run_identity=run_identity)
        result = simulate_prepared_inputs(prepared, params, prepared_predictions=predictions,
            checkpoint_at_ts_ms=None if cut == end//10**6 else cut,
            resume_checkpoint=checkpoint, consume_resume_checkpoint=checkpoint is not None)
        if cut < end//10**6:
            checkpoint = result['_replay_checkpoint']
            if checkpoint['cut_ts_ms'] != cut:
                raise ValueError('continuous execution did not reach requested durable cut')
            if on_checkpoint is not None and (first_window
                    or (cut-start//10**6) % durable_interval_ms == 0):
                on_checkpoint(arm, checkpoint)
        first_window = False
        cursor = cut
        del prepared, predictions
    ledger = public_equity_ledger(timelines.bundle.root, result,
        range(start+86400*10**9, end+1, 86400*10**9), initial_capital=initial_capital,
        max_mark_age_ns=max_mark_age_ns, funding=funding)
    return dict(replay=result, accounting=ledger['accounting'], daily_ledger=ledger,
                effective_params=params, account_contract='FINAL107_CONTINUOUS')
