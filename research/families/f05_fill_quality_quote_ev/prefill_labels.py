"""Stream a declared B0 parent and its bounded E/C suffix label pairs.

This entry owns orchestration only. Execution, restoration, valuation and
settlement remain in their maintained shared implementations.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

from data.facts import save_private_json
from models.backtest_tick import prepare_public_inputs, XMARKET_REPLAY_FEATURE_COLUMNS
from models.replay.public_input import PreparedPublicPredictions
from research.families.f05_fill_quality_quote_ev.public_input import (
    iter_prefill_parent_segments, iter_prefill_suffix_labels,
)
from strategy.signal import SignalEngine


class OutcomeBlindSelection:
    """First eligible identity per surface/UTC time bin, no repeated C order."""

    def __init__(self, *, per_surface, spacing_ns, end_ns, feature_columns, sample_start_ns=None,
                 calendar_revision=None, account_id=None):
        if type(per_surface) is not int or per_surface <= 0 or spacing_ns < 30_000_000_000:
            raise ValueError('positive quota and nonoverlapping label spacing required')
        self.limit, self.spacing, self.end = per_surface, spacing_ns, end_ns
        self.columns = tuple(feature_columns)
        self.counts = {f'{k}:{s}': 0 for k in ('E', 'C') for s in ('BUY', 'SELL')}
        self.last = {}
        self.orders = set()
        self.sample_start = sample_start_ns
        self.selected = []
        self.calendar_slots = None
        self.used_slots = set()
        self.account_id = account_id
        if calendar_revision is not None:
            from .prefill_calendar import REVISION, account_slots
            if calendar_revision != REVISION:
                raise ValueError('unknown sampling calendar revision')
            self.calendar_slots = account_slots(account_id, end_ns)

    def restore_selected(self, rows):
        """Reuse already frozen identities, never their outcomes."""
        for row in rows:
            if self.calendar_slots is not None:
                accepted = self.select([row])
                if len(accepted) != 1 or accepted[0].get('sampling') != row.get('sampling'):
                    raise ValueError('retained selection has incompatible calendar slot identity')
                continue
            surface = f"{row['kind']}:{row['side']}"
            self.counts[surface] += 1
            self.last[surface] = max(self.last.get(surface, 0), row['decision_ts_ns'])
            if row['kind'] == 'C':
                self.orders.add((surface, row['order_id']))
            self.selected.append(row)
        if self.calendar_slots is None and any(n > self.limit for n in self.counts.values()):
            raise ValueError('retained selection exceeds declared quota')

    def select(self, rows):
        import math

        selected = []
        for row in rows:
            surface = f"{row['kind']}:{row['side']}"
            ts = row['decision_ts_ns']
            order = (surface, row['order_id'])
            if self.calendar_slots is not None:
                from .prefill_calendar import DAY_NS, day_start, sampling_identity
                slot = next((s for s in self.calendar_slots
                             if s[2] == surface and s[4] <= ts < s[5]), None)
                if (slot is None or slot in self.used_slots
                        or ts+30_000_000_000 >= min(day_start(slot[0])+DAY_NS, self.end)
                        or row.get('feature_ready_ts_ns', ts) > ts
                        or ts-self.last.get(surface, -self.spacing) < self.spacing
                        or (row['kind'] == 'C' and order in self.orders)
                        or any(row['features'].get(k) is None
                               or not math.isfinite(row['features'][k]) for k in self.columns)):
                    continue
                row = dict(row, sampling=sampling_identity(slot, self.account_id))
                self.used_slots.add(slot)
                selected.append(row)
                self.selected.append(row)
                self.counts[surface] += 1
                self.last[surface] = ts
                if row['kind'] == 'C':
                    self.orders.add(order)
                continue
            if self.sample_start is not None:
                earliest = self.sample_start + (self.end-self.sample_start)*self.counts[surface]//self.limit
                if ts < earliest:
                    continue
            if (self.counts[surface] >= self.limit or ts+30_000_000_000 > self.end
                    or ts-self.last.get(surface, -self.spacing) < self.spacing
                    or (row['kind'] == 'C' and order in self.orders)
                    or any(row['features'].get(k) is None
                           or not math.isfinite(row['features'][k]) for k in self.columns)):
                continue
            selected.append(row)
            self.selected.append(row)
            self.counts[surface] += 1
            self.last[surface] = ts
            if row['kind'] == 'C':
                self.orders.add(order)
        return selected


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task', type=Path, required=True,
                        help='Explicit account, baseline, model, funding and sampling binding')
    parser.add_argument('--output', type=Path, help='Distinct output for an explicitly diagnosed retry')
    parser.add_argument('--resume-from', type=Path,
                        help='Completed label stage with its trusted parent checkpoint; writes a new stage')
    args = parser.parse_args(argv)
    task = json.loads(args.task.read_text())
    if task.get('calendar_revision'):
        from .prefill_calendar import REVISION, account_specs
        spec = next((s for s in account_specs() if s.shard_id == task['account_id']), None)
        if (task['calendar_revision'] != REVISION or spec is None
                or task['account_start_ns'] != spec.start_ts_ms*1_000_000
                or task['account_end_ns'] != spec.end_ts_ms_exclusive*1_000_000
                or task['segment_ms'] != 60_000):
            raise ValueError('calendar task account boundaries/segment differ')
    out = args.output or Path(task['output'])
    out.mkdir(parents=True, exist_ok=False)
    save_private_json(out/'task.json', task)
    params = json.loads(Path(task['params']).read_text())
    params['account_start_ns'] = task['account_start_ns']
    funding = json.loads(Path(task['funding']).read_text())
    if 'source_commit' in task:
        source = dict(source_commit=task['source_commit'],
                      source_identity_basis='explicitly_bound_staged_source', source_diff=None)
    else:
        source = dict(source_commit=subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], text=True).strip(),
            source_diff=subprocess.check_output(['git', 'diff', 'HEAD'], text=True),
            source_identity_basis='checkout')
    status = dict(status='preparing', pairs_completed=0, parent_segments=0,
                  python=sys.executable, **source)
    save_private_json(out/'run.json', status)
    try:
        prepared = prepare_public_inputs(task['bundle'], tick_size=params['tick_size'],
                                         cache_dir=task.get('cache'))
        end = prepared.inputs['bundle'].manifest['plan']['end_ns']
        if end != task['account_end_ns']:
            raise ValueError('declared parent end differs from bound input')
        signal = SignalEngine.from_public_models(task['model'], symbol=params['symbol'],
            ret_demean_halflife=params['ret_demean_halflife'])
        predictions = PreparedPublicPredictions.create(prepared, signal,
                                                       XMARKET_REPLAY_FEATURE_COLUMNS)
        selector = OutcomeBlindSelection(per_surface=task['per_surface'],
            spacing_ns=task['spacing_ms']*1_000_000, end_ns=end,
            feature_columns=task['feature_columns'],
            sample_start_ns=task.get('sample_start_ns'),
            calendar_revision=task.get('calendar_revision'), account_id=task.get('account_id'))
        checkpoint = None
        start_ms = task['account_start_ns']//1_000_000
        if args.resume_from:
            from models.replay.runtime_checkpoint_io import load_trusted_runtime_checkpoint

            previous = json.loads((args.resume_from/'run.json').read_text())
            previous_task = json.loads((args.resume_from/'task.json').read_text())
            if previous_task.get('calendar_revision') != task.get('calendar_revision'):
                raise ValueError('resume sampling calendar revision differs')
            if previous['status'] not in {'pair_quota_complete_parent_not_complete', 'paused_at_parent_boundary'}:
                raise ValueError('only a completed label stage may continue its parent')
            state_path = args.resume_from/'selected-identities.json'
            retained = (json.loads(state_path.read_text()) if state_path.exists() else
                [row for path in sorted(args.resume_from.glob('selection-*.json'))
                 for row in json.loads(path.read_text())])
            if len(retained) != previous['pairs_completed']+previous.get('reused_pairs', 0):
                raise ValueError('retained selected identities differ from completed pairs')
            selector.restore_selected(retained)
            checkpoint = load_trusted_runtime_checkpoint(args.resume_from/'parent.checkpoint')
            start_ms = checkpoint['cut_ts_ms']
            status['reused_pairs'] = len(retained)
            status['reused_stage'] = str(args.resume_from)
        cuts = range(start_ms, end//1_000_000,
                     task['segment_ms'])
        save_private_json(out/'effective_params.json', params)
        from contextlib import closing

        with (out/'opportunities.jsonl').open('x') as tape, closing(
                iter_prefill_parent_segments(prepared, params,
                    cut_times_ms=cuts, prepared_predictions=predictions,
                    resume_checkpoint=checkpoint)) as segments:
            # The generator now owns the initial resume reference.
            del checkpoint
            final_result = None
            for segment in segments:
                rows = segment['opportunities']
                for row in rows:
                    tape.write(json.dumps(row, allow_nan=False)+'\n')
                tape.flush()
                selected = selector.select(rows)
                # Freeze the selection before reading any branch outcome.
                index = status['parent_segments']
                save_private_json(out/f'selection-{index:05d}.json', selected)
                status.update(status='parent_running', parent_segments=index+1,
                    last_decision_ns=rows[-1]['decision_ts_ns'] if rows else None)
                if segment['next_checkpoint'] is not None:
                    status['parent_cut_ts_ms'] = segment['next_checkpoint']['cut_ts_ms']
                save_private_json(out/'run.json', status)
                print('PARENT_PROGRESS', json.dumps({k: status[k] for k in (
                    'parent_segments', 'pairs_completed', 'last_decision_ns')}), flush=True)
                with closing(iter_prefill_suffix_labels(prepared, params,
                        checkpoint=segment['checkpoint'], opportunities=selected,
                        prepared_predictions=predictions, funding=funding,
                        initial_capital=task['initial_capital'],
                        max_mark_age_ns=task['max_mark_age_ns'],
                        verify_first_control=status['pairs_completed'] == 0)) as suffixes:
                    for receipt in suffixes:
                        if task.get('calendar_revision'):
                            row = next(r for r in selected if r['opportunity_id'] == receipt['label']['opportunity_id'])
                            receipt['label']['sampling'] = row['sampling']
                        path = out/f"pair-{status['pairs_completed']:04d}.json"
                        save_private_json(path, receipt)
                        if json.loads(path.read_text()) != receipt:
                            raise ValueError('paired receipt readback differs')
                        status['pairs_completed'] += 1
                        status['completed_at_unix'] = time.time()
                        save_private_json(out/'run.json', status)
                        print('PAIR_COMPLETE', status['pairs_completed'], flush=True)
                pause = time.time() >= task.get('stop_before_unix', float('inf'))
                if (segment['next_checkpoint'] is not None and (pause or
                        (selector.calendar_slots is None and not task.get('complete_parent', False)
                         and all(n == selector.limit for n in selector.counts.values())))):
                    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint

                    if segment['next_checkpoint'] is not None:
                        save_runtime_checkpoint(out/'parent.checkpoint', segment['next_checkpoint'])
                    save_private_json(out/'selected-identities.json', selector.selected)
                    status.update(status=('paused_at_parent_boundary' if pause else
                                          'pair_quota_complete_parent_not_complete'),
                                  surface_counts=selector.counts)
                    break
                final_result = segment['result']
                del segment, rows, selected
            else:
                from models.replay.public_accounting import settle_public_replay
                import numpy as np

                result = final_result
                # Persist producer evidence before accounting; failures retain the raw replay.
                def numeric(value):
                    if isinstance(value, np.ndarray):
                        return value.tolist()
                    if isinstance(value, np.generic):
                        return value.item()
                    raise TypeError(type(value).__name__)

                with (out/'replay.json').open('x') as stream:
                    json.dump(result, stream, default=numeric, allow_nan=True)
                account = settle_public_replay(task['bundle'], result,
                    initial_capital=task['initial_capital'], funding=funding,
                    max_mark_age_ns=task['max_mark_age_ns'])
                save_private_json(out/'accounting.json', account)
                if json.loads((out/'accounting.json').read_text()) != account:
                    raise ValueError('parent accounting readback differs')
                status.update(status='parent_complete', surface_counts=selector.counts,
                              economic_complete=account['economic_complete'])
        save_private_json(out/'run.json', status)
        if selector.calendar_slots is not None:
            from .prefill_calendar import sampling_identity
            save_private_json(out/'sampling-support.json', {
                'calendar_revision': task['calendar_revision'],
                'selected': selector.selected,
                'unfilled_slots': [sampling_identity(s, task['account_id'])
                                   for s in selector.calendar_slots if s not in selector.used_slots],
                'unfilled_scope': ('no_eligible_opportunity' if status['status'] == 'parent_complete'
                                   else 'not_yet_exhaustively_observed'),
            })
    except Exception as exc:
        status.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        save_private_json(out/'run.json', status)
        raise


if __name__ == '__main__':
    main()
