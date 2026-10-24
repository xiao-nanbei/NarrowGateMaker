"""One equal-valid-label joint direction candidate; no scheduling or live action.

Reuse the accepted train100 panel and original eligibility/endpoint masks. Only
loss weights and class rebalance change; this is not a recency-decay experiment.
"""
from copy import deepcopy
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from data.facts import save_private_json as save
from research.families.f04_external_market_alpha import full_support_direction as base

CANDIDATE = 'F03_TRAIN100_BTCUSDC_BTCUSDT_DIRECTION_EQUAL_LABEL_20261015'
POLICY = 'narrowgate.f04.valid_label_equal_direction.v1'
PREDECESSOR_PROJECT = 'f03-equal-label-final107-20261015'


def parameters(original, threads=2):
    if type(threads) is not int or not 1 <= threads <= 2:
        raise ValueError('bounded training concurrency required')
    result = deepcopy(original)
    for key in ('unbalance', 'unbalanced_sets', 'scale_pos_weight', 'class_weight'):
        if key in result:
            raise ValueError('unexpected class-weight alias: ' + key)
    if result.get('objective') != 'binary' or result.get('metric') != 'auc':
        raise ValueError('original binary/AUC direction contract required')
    result.update(is_unbalance=False, n_jobs=threads)
    return result


def uniform_phases(frame, metadata, columns):
    """Preserve the original causal split and masks; never multiply old weights."""
    old = base.prepared_phases(frame, metadata, columns)
    result = {}
    for phase, (x, y, _weights, cols, previous) in old.items():
        if list(cols) != list(columns) or not x.index.equals(y.index):
            raise ValueError('feature/label alignment differs')
        if not np.isfinite(y.to_numpy()).all() or set(y.unique()) != {0., 1.}:
            raise ValueError('binary label support missing')
        if np.isinf(x.to_numpy()).any():
            raise ValueError('infinite features are not missing values')
        weights = None if phase == 'selection' else pd.Series(1., index=y.index)
        counts = y.groupby(y.index.strftime('%Y-%m-%d')).size()
        report = dict(policy=POLICY, effective_rows=len(y), effective_day_count=len(counts),
            weight_min=1., weight_max=1., weight_sum=float(len(y)),
            class_counts={str(k): int(v) for k, v in y.value_counts().items()},
            daily=[dict(day=day, effective_rows=int(n), weight_sum=float(n))
                   for day, n in counts.items()],
            date_normalization='none', age_decay='none', class_adjustment='none',
            unweighted_early_stopping=phase == 'selection',
            original_eligibility_and_endpoint_masks_preserved=True,
            statistical_independence_claim=False,
            inherited_eligibility_report=previous)
        result[phase] = (x, y, weights, cols, report)
    if not np.isfinite(result['refit'][0].to_numpy()).any(axis=0).all():
        raise ValueError('zero supported feature')
    return result


def fit_direction(frame, metadata, columns, output, *, provenance, threads=2):
    output = Path(output)
    if output.exists():
        raise FileExistsError('existing model must not be refitted implicitly')
    phases = uniform_phases(frame, metadata, columns)
    params = parameters(metadata['selection_fit_params'], threads)
    train, selection, refit = (phases[k] for k in ('fit', 'selection', 'refit'))
    selected = lgb.LGBMClassifier(**params)
    selected.fit(train[0], train[1], sample_weight=train[2],
        eval_set=[(selection[0], selection[1])], eval_sample_weight=[None],
        callbacks=[lgb.log_evaluation(0), lgb.early_stopping(100, verbose=False)])
    iteration = int(selected.best_iteration_ or params['n_estimators'])
    final_params = dict(params, n_estimators=iteration)
    fitted = lgb.LGBMClassifier(**final_params)
    fitted.fit(refit[0], refit[1], sample_weight=refit[2])
    output.mkdir(parents=True, exist_ok=False)
    model_path = output / (base.HEAD + '.txt')
    fitted.booster_.save_model(str(model_path))
    loaded = lgb.Booster(model_file=str(model_path))
    if loaded.feature_name() != list(columns):
        raise ValueError('serialized model feature order differs')
    probes = {}
    for phase, (x, _y, _w, _cols, _report) in phases.items():
        # Every eligible training/selection row, not a favorable subset.
        before = fitted.booster_.predict(x, num_threads=threads)
        after = loaded.predict(x, num_threads=threads)
        if not np.array_equal(before, after) or not np.isfinite(after).all():
            raise ValueError('saved-model predictions differ: ' + phase)
        if ((after < 0) | (after > 1)).any():
            raise ValueError('probability out of range')
        probes[phase] = dict(rows=len(x), exact_reload=True)
    manifest = dict(schema=POLICY, visibility='local_only_do_not_publish',
        candidate=CANDIDATE, name=base.HEAD, feature_cols=list(columns),
        best_iteration=iteration, model_sha256=base.sha(model_path),
        selection_fit_params=params, refit_fit_params=final_params,
        baseline_method_metadata=metadata, provenance=provenance,
        support={k: v[4] for k, v in phases.items()},
        selection_auc=float(selected.best_score_['valid_0']['auc']),
        feature_availability={k: float(np.isfinite(refit[0][k]).mean()) for k in columns},
        saved_model_prediction_checks=probes, new_fits=1, refits=1,
        weighting=dict(per_valid_label=1., date_normalization='none', age_decay='none',
                       class_adjustment='none', is_unbalance=False),
        isolated_date_weight_effect=False, external_panel_read=False, live_authority=False)
    save(output / 'manifest.json', manifest)
    return manifest


def predecessor_gate(plan, accepted, state):
    """An all-calendar completion gate, not merely a lower dispatch priority."""
    parents = {r['parent_id'] for r in plan['parents']}
    if len(parents) != 54 or len(plan['parents']) != 54:
        raise ValueError('full original 54-account predecessor calendar required')
    records = accepted.get('parents', {})
    tasks = [t for t in state['tasks'].values()
             if t['spec']['project'] == PREDECESSOR_PROJECT]
    by_parent = {}
    for task in tasks:
        parent = task['spec']['parameters']['cell']['parent_id']
        if parent not in parents or parent in by_parent:
            raise ValueError('unexpected or duplicate predecessor task')
        by_parent[parent] = task
    waiting = []
    for parent in sorted(parents):
        task, record = by_parent.get(parent), records.get(parent)
        if not task or task['status'] != 'COMPLETE' or not record:
            waiting.append(parent)
            continue
        attempt = state['attempts'].get(task['attempt_id'])
        flags = ('process_exited', 'all_receipt_payload_sha_and_bytes_verified',
                 'full_endpoint_accounting_daily_ledger_verified')
        if (not attempt or attempt['status'] != 'COMPLETE'
                or record['attempt'] != task['attempt_id']
                or record['spec_hash'] != attempt['spec_hash']
                or not all(record.get(k) is True for k in flags)):
            waiting.append(parent)
    return dict(status='PREDECESSOR_ACCEPTED' if not waiting else 'WAITING_FOR_CURRENT_MODEL',
        accepted_predecessor_accounts=54-len(waiting), target=54, waiting=waiting,
        economic_dispatch_allowed=not waiting)


def load(path):
    return json.loads(Path(path).read_text())
