"""Bounded optional-update recovery validation and paired labels on declared parents.

No fitting, new account dates, automatic retry, or task dispatch is implied.
Parent checkpoints are retained to continue the same reference traversal.
"""
import argparse
from dataclasses import asdict, replace
import json
from pathlib import Path
import resource
import sys
import time
from contextlib import nullcontext

from .response_replay_probe import numeric, validate_plan


def same_result(left, right):
    """Compare complete output, including NumPy curves and matching NaNs."""
    from models.replay.risk_selection import _same_trace_value
    return _same_trace_value(json.loads(json.dumps(left, default=numeric)),
                             json.loads(json.dumps(right, default=numeric)))


def write_branch_json(path, branch):
    """Keep the existing byte/schema contract while timing output separately."""
    path.write_text(json.dumps(branch, default=numeric)+'\n')


def advance_to_root(simulate, prepared, params, predictions, checkpoint, decision_ms,
                    *, consume_resume_checkpoint=False):
    """Both sides of one publication can share the same immutable root."""
    if decision_ms == checkpoint['cut_ts_ms']:
        return checkpoint
    if decision_ms < checkpoint['cut_ts_ms']:
        raise ValueError('root selection moved backwards')
    return simulate(prepared, params, prepared_predictions=predictions,
                    resume_checkpoint=checkpoint, checkpoint_at_ts_ms=decision_ms,
                    consume_resume_checkpoint=consume_resume_checkpoint)['_replay_checkpoint']


def prepare_parent(plan):
    validate_plan(plan)
    from models.backtest_tick import prepare_public_inputs, XMARKET_REPLAY_FEATURE_COLUMNS
    from models.replay.public_input import PreparedPublicPredictions
    from strategy.signal import SignalEngine
    from research.system_engineering.audit.rest_latency_calibration import load_runtime_timing_samples
    from research.families.f01_fixed_parameter_racing.inventory_lifecycle_outcome_replay_audit import _runtime_compute_for_window
    params = dict(plan['params'], account_start_ns=plan['account_start_ns'],
                  response_observe_only=True, response_update_branch_enabled=True)
    prepared = prepare_public_inputs(plan['bundle'], tick_size=params['tick_size'], cache_dir=plan['cache'])
    signal = SignalEngine.from_public_models(plan['model'], symbol=params['symbol'],
                                             ret_demean_halflife=params['ret_demean_halflife'])
    predictions = PreparedPublicPredictions.create(prepared, signal, XMARKET_REPLAY_FEATURE_COLUMNS)
    calibration = load_runtime_timing_samples(plan['runtime_timing_samples'],
        effective_time_assumption='exchange_event_proxy')['calibration']
    params.update(_runtime_compute_for_window({'ml_data': predictions.for_inputs(prepared)},
        params, calibration, clock='source_time_assumption', start_ms=plan['account_start_ns']//1_000_000))
    return prepared, predictions, params


def validate_roots(plans, output, *, resume_failed=None):
    from models.backtest_tick import simulate_prepared_inputs, simulate_optional_update_branch
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint
    from models.replay.public_accounting import funding_window, settle_public_replay, terminal_valuations
    from strategy.optional_update import OptionalUpdateFork
    if len(plans) != 2 or len({p['account_start_ns'] for p in plans}) != 2:
        raise ValueError('exactly two distinct declared diagnostic parents required')
    previous = None
    if resume_failed is not None:
        previous = json.loads((Path(resume_failed)/'receipt.json').read_text())
        known = ('truth value of an array', 'the next checkpoint must advance beyond the saved cut')
        if previous['status'] != 'failed' or not any(s in previous.get('error', '') for s in known):
            raise ValueError('recovery requires a diagnosed runner failure')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    start_wall, start_cpu = time.monotonic(), time.process_time()
    receipt = dict(visibility='local_only_do_not_publish', status='running',
                   selection='first eight decision-admissible roots per parent in original publication order',
                   root_limit=16, roots=[], parent_prefix_seconds=0, branch_calls=0,
                   action_labels=0, model_fits=0, complete_accounts=0)
    if previous is not None:
        receipt['roots'] = previous['roots'][:]
        receipt['reused_roots_from'] = str(resume_failed)
        receipt['previous_failed_attempt'] = dict(path=str(resume_failed),
            branch_calls=previous['branch_calls'], wall_seconds=previous['wall_seconds'],
            cpu_seconds=previous['cpu_seconds'], parent_prefix_seconds=previous['parent_prefix_seconds'])

    def save():
        receipt.update(wall_seconds=time.monotonic()-start_wall,
                       cpu_seconds=time.process_time()-start_cpu,
                       peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))
        (output/'receipt.json').write_text(json.dumps(receipt, indent=2, default=numeric)+'\n')

    save()
    try:
        for plan in plans:
            parent = output/str(plan['account_start_ns'])
            parent.mkdir()
            prepared, predictions, params = prepare_parent(plan)
            (parent/'effective_params.json').write_text(json.dumps(params, default=numeric)+'\n')
            start_ms = plan['account_start_ns']//1_000_000
            checkpoint = simulate_prepared_inputs(prepared, params, prepared_predictions=predictions,
                checkpoint_at_ts_ms=start_ms)['_replay_checkpoint']
            # This is the beginning of the budgeted reference traversal, not
            # an additional short-test path or a completed economic account.
            retained = Path(resume_failed)/parent.name if previous is not None else None
            if retained is not None and (retained/'selected-roots.json').exists():
                requests = [OptionalUpdateFork(**r) for r in json.loads((retained/'selected-roots.json').read_text())]
                saved = retained/'parent-at-120s.checkpoint'
                if not saved.exists():
                    saved = next((Path(p) for p in previous.get('retained_parent_checkpoints', [])
                                  if Path(p).parent.name == parent.name and Path(p).exists()), None)
                if saved is None:
                    raise ValueError('retained parent checkpoint missing')
                receipt.setdefault('retained_parent_checkpoints', []).append(str(saved))
            else:
                following = simulate_prepared_inputs(prepared, params, prepared_predictions=predictions,
                    resume_checkpoint=checkpoint, checkpoint_at_ts_ms=start_ms+120_000)['_replay_checkpoint']
                save_runtime_checkpoint(parent/'parent-at-120s.checkpoint', following)
                rows = following['runtime'].response_update_observer.samples
                selected = [r for r in rows if r.get('optional_update_admission', {}).get('decision_admissible')][:8]
                if len(selected) != 8:
                    raise ValueError('diagnostic prefix lacks eight admissible roots; do not extend dates or retry automatically')
                requests = [OptionalUpdateFork(decision_ns=r['read_ns'], decision_sequence=r['call_sequence'],
                    side=r['side'], order_id=r['order_id'], old_price=r['old_price'], target_price=r['target_price'],
                    quantity=r['old_quantity'], action='BASELINE', execution_end_ns=r['read_ns']+30_000_000_000)
                    for r in selected]
                del following
                receipt['parent_prefix_seconds'] += 120
            (parent/'selected-roots.json').write_text(json.dumps([asdict(r) for r in requests], indent=2)+'\n')
            funding = json.loads(Path(plan['funding']).read_text())
            age = int(plan.get('max_mark_age_ns', 1_000_000_000))
            marks = terminal_valuations(prepared.inputs['bundle'], [r.execution_end_ns for r in requests],
                                        max_mark_age_ns=age)
            for request in requests:
                if any(r['request'] == asdict(request) for r in receipt['roots']):
                    continue
                receipt['current_root'] = asdict(request)
                save()
                root = advance_to_root(simulate_prepared_inputs, prepared, params, predictions,
                                       checkpoint, request.decision_ns//1_000_000)
                checkpoint = root
                results, accounts = [], []
                for branch_index in range(2):
                    receipt['branch_calls'] += 1
                    save()
                    result = simulate_optional_update_branch(prepared, params, checkpoint=root,
                        request=request, prepared_predictions=predictions)
                    account = settle_public_replay(plan['bundle'], result,
                        initial_capital=float(plan.get('initial_capital', 10000)), max_mark_age_ns=age,
                        funding=funding_window(funding, start_ns=plan['account_start_ns'], end_ns=request.execution_end_ns),
                        terminal_valuation=marks[request.execution_end_ns])
                    if not account['economic_complete']:
                        raise ValueError('branch settlement incomplete')
                    (parent/f'root-{request.decision_sequence}-{request.side}-{branch_index}.json').write_text(
                        json.dumps(dict(result=result, accounting=account), default=numeric)+'\n')
                    results.append(result)
                    accounts.append(account)
                if not same_result(results[0], results[1]) or not same_result(accounts[0], accounts[1]):
                    raise ValueError('same-state no-intervention branches differ')
                if root['runtime'].response_update_branch_evidence is not None:
                    raise ValueError('branch mutated its parent')
                receipt['roots'].append(dict(request=asdict(request), identical=True,
                    accounting=accounts[0], branch_calls=2, scope='no_intervention_recovery_not_action_value'))
                save()
                del results, accounts, result, root
            del checkpoint, prepared, predictions
        receipt['status'] = 'sixteen_no_intervention_roots_verified'
    except BaseException as exc:
        receipt.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        save()
    return receipt


def parent_step(simulate, prepared, params, predictions, checkpoint, cut, end_ms):
    # The full account end is already bound. Adding execution_end_ns, even
    # with the same value, changes the bounded-execution parameter contract.
    return simulate(prepared, params, prepared_predictions=predictions,
                    resume_checkpoint=checkpoint,
                    **({} if cut == end_ms else {'checkpoint_at_ts_ms': cut}))


def generate_parent_labels(plan, output, validation, *, resume_failed=None, timing=None):
    """One budgeted reference traversal, bounded roots, two one-shot suffixes.

    Branch outputs are settled together after traversal, so all required marks
    use a single depth scan. A failed attempt is retained, never auto-retried.
    """
    from models.backtest_tick import simulate_prepared_inputs, simulate_optional_update_branch
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from models.replay.public_accounting import terminal_valuations, funding_window, settle_public_replay
    from strategy.optional_update import OptionalUpdateFork
    from strategy.response_action_features import FEATURES, ABLATION
    from .response_sampling import slots
    from .response_label_accounting import paired_labels
    def timed(name, function, *args, **kwargs):
        with timing.phase(name) if timing is not None else nullcontext():
            return function(*args, **kwargs)
    check = json.loads(Path(validation).read_text())
    if check['status'] != 'sixteen_no_intervention_roots_verified' or len(check['roots']) != 16:
        raise ValueError('sixteen actual no-intervention roots must pass first')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    wall, cpu = time.monotonic(), time.process_time()
    start, end = plan['account_start_ns'], plan['account_end_ns']
    selected_slots = [s for s in slots() if start <= s['start_ns'] < end]
    if not selected_slots or len(selected_slots) > 128:
        raise ValueError('parent slots outside bounded engineering allocation')
    receipt = dict(status='preparing', visibility='local_only_do_not_publish',
                   account_start_ns=start, account_end_ns=end, root_budget=len(selected_slots),
                   features=list(FEATURES), ablation=list(ABLATION),
                   slots=selected_slots, roots=[], branch_calls=0, labels=0, model_fits=0)
    previous = None
    if resume_failed is not None:
        prior = Path(resume_failed).resolve()
        previous = json.loads((prior/'receipt.json').read_text())
        if (previous['status'] != 'failed'
                or previous.get('error') != 'ValueError: public checkpoint input, parameters, predictions or implementation changed'
                or previous.get('parent_cut_ms') != end//1_000_000-60_000
                or previous['account_start_ns'] != start or previous['account_end_ns'] != end
                or previous['slots'] != selected_slots):
            raise ValueError('resume is restricted to the diagnosed final-call failure on this parent')
        receipt['previous_attempt'] = dict(path=str(prior), wall_seconds=previous['wall_seconds'],
            cpu_seconds=previous['cpu_seconds'], branch_calls=previous['branch_calls'], error=previous['error'])
        receipt['roots'] = previous['roots']
        for row in receipt['roots']:
            row['branches'] = [str((prior/p).resolve()) for p in row['branches']]
            if any(not Path(p).is_file() for p in row['branches']):
                raise ValueError('completed branch file missing; do not silently rerun')
    def save():
        receipt.update(wall_seconds=time.monotonic()-wall, cpu_seconds=time.process_time()-cpu,
            peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))
        with timing.phase('receipt_output') if timing is not None else nullcontext():
            (output/'receipt.json').write_text(json.dumps(receipt, indent=2, default=numeric)+'\n')
    save()
    try:
        prepared, predictions, params = timed('prepare', prepare_parent, plan)
        params.update(response_action_sampling_slots=selected_slots, response_action_account_end_ns=end)
        (output/'effective_params.json').write_text(json.dumps(params, default=numeric)+'\n')
        if previous is None:
            checkpoint = timed('parent_initial', simulate_prepared_inputs, prepared, params, prepared_predictions=predictions,
                                                  checkpoint_at_ts_ms=start//1_000_000)['_replay_checkpoint']
        else:
            if not same_result(params, json.loads((prior/'effective_params.json').read_text())):
                raise ValueError('original effective parameters changed')
            checkpoint = load_trusted_runtime_checkpoint(prior/'parent.checkpoint')
            if not start//1_000_000 <= checkpoint['cut_ts_ms'] <= previous['parent_cut_ms']:
                raise ValueError('saved checkpoint outside original parent progress')
            receipt['resumed_cut_ms'] = checkpoint['cut_ts_ms']
        seen = {r['key'] for r in receipt['roots']}
        receipt['status'] = 'reference_and_paired_suffixes_running'
        for cut in range(checkpoint['cut_ts_ms']+60_000, end//1_000_000+1, 60_000):
            final = cut == end//1_000_000
            following = timed('parent', parent_step, simulate_prepared_inputs, prepared, params, predictions,
                                    checkpoint, cut, end//1_000_000)
            if final:
                result = following
                selected = result['response_action_sampling']['selected']
                eligible = result['response_action_sampling']['eligible']
                qualified = result['response_action_sampling']['qualified']
                unsupported = result['response_action_sampling']['unsupported']
            else:
                following = following['_replay_checkpoint']
                current = following['runtime'].response_action_sampler
                selected, eligible = current.selected, current.eligible
                qualified, unsupported = current.qualified, current.unsupported
            new = [(key, row) for key, row in selected.items() if key not in seen]
            root = checkpoint
            for key, row in sorted(new, key=lambda item: (item[1]['read_ns'], item[1]['sequence'])):
                legal_end = min(end, row['sampling']['label_day_end_ns'])
                horizon = 120 if row['read_ns']+120_000_000_000 < legal_end else 30
                request = OptionalUpdateFork(decision_ns=row['read_ns'], decision_sequence=row['call_sequence'],
                    side=row['side'], order_id=row['order_id'], old_price=row['old_price'],
                    target_price=row['target_price'], quantity=row['old_quantity'], action='KEEP_EXISTING',
                    execution_end_ns=row['read_ns']+horizon*1_000_000_000)
                root = timed('root_advance', advance_to_root, simulate_prepared_inputs, prepared, params, predictions,
                                       root, request.decision_ns//1_000_000,
                                       consume_resume_checkpoint=True)
                paths = []
                for action in ('KEEP_EXISTING', 'UPDATE_TO_B0_TARGET'):
                    receipt['branch_calls'] += 1
                    receipt['current_root'] = key
                    save()
                    branch = timed(action, simulate_optional_update_branch, prepared, params, checkpoint=root,
                        request=replace(request, action=action), prepared_predictions=predictions)
                    path = output/f'{key.replace(":", "-")}-{action}.json'
                    timed('branch_output', write_branch_json, path, branch)
                    paths.append(path.name)
                    del branch
                receipt['roots'].append(dict(key=key, request=asdict(request), branches=paths, observation=row))
                seen.add(key)
                save()
            del root
            receipt.update(parent_cut_ms=cut, eligible=dict(eligible), selected=len(selected),
                           decision_qualified=qualified, unsupported_features=unsupported)
            if final:
                break
            checkpoint = following
            if (cut-start//1_000_000) % 3_600_000 == 0:
                timed('checkpoint_output', save_runtime_checkpoint, output/'parent.checkpoint', checkpoint)
            save()
        receipt['status'] = 'settling_pairs'
        save()
        points = {end}
        for row in receipt['roots']:
            r = row['request']
            points.add(r['decision_ns'])
            points.update(r['decision_ns']+h*1_000_000_000 for h in (5, 30, 120)
                          if r['decision_ns']+h*1_000_000_000 <= r['execution_end_ns'])
        age = int(plan.get('max_mark_age_ns', 1_000_000_000))
        marks = timed('terminal_marks', terminal_valuations, prepared.inputs['bundle'], points, max_mark_age_ns=age)
        funding = json.loads(Path(plan['funding']).read_text())
        labels = []
        for row in receipt['roots']:
            request = OptionalUpdateFork(**row['request'])
            branches = [timed('branch_read', lambda p=p: json.loads((output/p).read_text()))
                        for p in row['branches']]
            schedule = funding_window(funding, start_ns=start, end_ns=request.execution_end_ns)
            accounts = [timed('branch_settlement', settle_public_replay, plan['bundle'], b, initial_capital=plan.get('initial_capital', 10000),
                max_mark_age_ns=age, funding=schedule, terminal_valuation=marks[request.execution_end_ns]) for b in branches]
            label = timed('label_wealth', paired_labels, *branches, accounts, schedule, request,
                {k: v.price for k, v in marks.items()}, day_end_ns=row['observation']['sampling']['label_day_end_ns'],
                account_end_ns=end)
            slot = row['observation']['sampling']
            labels.append(dict(key=row['key'], day=slot['day'], split=slot['split'], slot=slot['index'],
                role=row['observation']['role'], decision_ns=request.decision_ns,
                outcome_end_ns=request.decision_ns+30_000_000_000,
                features=row['observation']['model_features'], **label))
        (output/'labels.json').write_text(json.dumps(labels, indent=2)+'\n')
        (output/'reference-result.json').write_text(json.dumps(result, default=numeric)+'\n')
        account = timed('reference_settlement', settle_public_replay, plan['bundle'], result, initial_capital=plan.get('initial_capital', 10000),
            max_mark_age_ns=age, funding=funding_window(funding, start_ns=start, end_ns=end), terminal_valuation=marks[end])
        receipt.update(status='parent_and_labels_complete', labels=len(labels), accounting=account,
                       empty_slots=len(selected_slots)-len(seen))
    except BaseException as exc:
        receipt.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        save()
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plans', type=Path, nargs=2)
    parser.add_argument('--label-plan', type=Path)
    parser.add_argument('--validation', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--resume-failed', type=Path)
    parser.add_argument('--timing-output', type=Path,
                        help='isolated-process engineering timing; never a strategy parameter')
    args = parser.parse_args()
    if args.label_plan is not None:
        if args.plans is not None or args.validation is None:
            parser.error('label generation requires one plan and completed root validation')
        if args.timing_output is None:
            result = generate_parent_labels(json.loads(args.label_plan.read_text()), args.output,
                                             args.validation, resume_failed=args.resume_failed)
        else:
            from models.replay.phase_timing import ReplayPhaseTiming
            from models.replay import public_input, runtime_checkpoint_io
            timing = ReplayPhaseTiming()
            methods = [(public_input._AccountTrades, 'copy', 'account_deserialize'),
                       (public_input._AccountTrades, 'execution_frame', 'account_frame'),
                       (public_input.PreparedReplayInputs, 'event_clock', 'event_clock'),
                       (runtime_checkpoint_io, 'clone_runtime_state', 'runtime_clone'),
                       (runtime_checkpoint_io, 'public_checkpoint_binding', 'binding')]
            try:
                with timing.measure_methods(methods):
                    result = generate_parent_labels(json.loads(args.label_plan.read_text()), args.output,
                        args.validation, resume_failed=args.resume_failed, timing=timing)
            finally:
                args.timing_output.write_text(json.dumps(timing.report(), indent=2)+'\n')
        print(json.dumps(result))
        return
    if args.plans is None:
        parser.error('two root-validation plans or one label plan required')
    print(json.dumps(validate_roots([json.loads(p.read_text()) for p in args.plans], args.output,
                                   resume_failed=args.resume_failed)))


if __name__ == '__main__':
    main()
