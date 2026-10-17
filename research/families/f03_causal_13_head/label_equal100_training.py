"""Original-B0 100-day, single-market, all-head unit-label comparator.

Reuse the accepted twelve single-market heads from the matched 100-day
composite, preserving their original bytes/provenance. Fit only the missing
single-market 10s direction head. Never reuse the composite's joint head.
"""
from copy import deepcopy
from pathlib import Path
import shutil

import lightgbm as lgb
import numpy as np
import pandas as pd

from data.facts import save_private_json as save
from research.families.f03_causal_13_head import development300 as original
from research.families.f03_causal_13_head import ml_model as ml
from research.families.f03_causal_13_head.public_input_panel import publish_model_contract
from research.families.f04_external_market_alpha import equal_label_joint_bundle as shared
from strategy.public_model_contract import training_selection_contract, validate_public_bundle

CANDIDATE = 'F03_BTCUSDC_TRAIN100_13HEAD_EQUAL_LABEL_20261015'
HEAD = shared.joint.HEAD
REUSED = tuple(h for h in ml.MODEL_SPECS if h != HEAD)


def validate_plan(plan, baseline):
    if (plan.get('candidate') != CANDIDATE or plan.get('economic_accounts') != 54
            or plan.get('new_control_accounts') != 0 or plan.get('continuous_accounts') != 0
            or plan.get('training_threads') != 2 or plan.get('per_valid_label_weight') != 1.
            or plan.get('date_normalization') != 'none' or plan.get('class_adjustment') != 'none'
            or plan.get('age_decay') != 'none' or plan.get('reference_market') is not None
            or plan.get('newly_fitted_heads') != [HEAD] or plan.get('reused_heads') != list(REUSED)):
        raise ValueError('fixed single-market original100 equal-label scope mismatch')
    metadata = baseline['baseline_head_metadata']
    prior, equal = shared.contracts(metadata[HEAD])
    if (list(equal.refit_days) != plan['training_days'] or len(equal.refit_days) != 100
            or [len(equal.fit_days), len(equal.embargo_days), len(equal.selection_days)] != [79, 1, 20]
            or len(equal.feature_cols) != 29):
        raise ValueError('must inherit the actual B0 100-day calendar and 79/1/20 split')
    for meta in metadata.values():
        if shared.contracts(meta) != (prior, equal):
            raise ValueError('baseline head training contracts differ')
    return prior, equal


def validate_reused(head, item, meta, baseline_meta, equal, support):
    if head not in REUSED:
        raise ValueError('joint direction cannot be reused in the single-market candidate')
    if (item['feature_cols'] != list(equal.feature_cols)
            or item['training_days'] != list(equal.refit_days)
            or item['per_valid_label_weight'] != 1. or item['date_normalization'] != 'none'
            or item['class_adjustment'] != 'none' or item['age_decay'] != 'none'
            or item['is_unbalance'] is not False):
        raise ValueError('reused head input/weight contract mismatch')
    expected = shared.parameters(baseline_meta, head, 2)
    if (meta['selection_fit_params'] != expected
            or meta['refit_fit_params'] != dict(expected, n_estimators=meta['best_iteration'])
            or training_selection_contract(meta['train_only_selection']) !=
               training_selection_contract(equal.to_metadata())
            or meta['feature_cols'] != list(equal.feature_cols)):
        raise ValueError('reused head actual training method mismatch')
    for phase, row in support.items():
        if (meta['per_head_time_weighting'][phase] != row['report']
                or meta['saved_model_prediction_checks'][phase] !=
                   dict(rows=row['rows'], exact_saved_reload=True)):
            raise ValueError('reused head support or saved-prediction acceptance mismatch')


def fit(plan, output, evidence, progress):
    output, evidence = Path(output), Path(evidence)
    if output.exists():
        raise FileExistsError('existing model output: inspect, never refit implicitly')
    for name in ('baseline_plan', 'original_panel', 'original_manifest', 'reused_plan',
                 'reused_manifest', 'reused_admission', 'reused_support', 'p3'):
        item = plan[name]
        if shared.joint.sha(item['path']) != item['sha256']:
            raise ValueError('bound source differs: ' + name)
    baseline = shared.load(plan['baseline_plan']['path'])
    metadata = baseline['baseline_head_metadata']
    prior, equal = validate_plan(plan, baseline)
    root = Path(baseline['baseline_bundle']['path']).parent
    bundle = shared.load(root / 'public_input_model.json')
    if shared.joint.sha(root / 'public_input_model.json') != baseline['baseline_bundle']['sha256']:
        raise ValueError('actual B0 bundle differs')
    for head, item in bundle['heads'].items():
        if (shared.joint.sha(root / (head + '.txt')) != item['sha256']
                or shared.joint.sha(root / (head + '_meta.json')) != item['metadata_sha256']
                or shared.load(root / (head + '_meta.json')) != metadata[head]):
            raise ValueError('actual B0 model/metadata differs')
    reused = shared.load(plan['reused_manifest']['path'])
    accepted = shared.load(plan['reused_admission']['path'])
    if (accepted['status'] != 'ALL13_EQUAL_LABEL_TRAIN100_MODELS_VERIFIED'
            or accepted['model_manifest']['sha256'] != plan['reused_manifest']['sha256']
            or reused['candidate'] != shared.CANDIDATE
            or reused['training_days'] != plan['training_days']):
        raise ValueError('accepted matched100 source required')
    frame = original.convert_original_labels(pd.read_parquet(plan['original_panel']['path']), metadata)
    if len(frame) != 864000 or frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
        raise ValueError('original100 panel rows or clock differ')
    parts = dict(zip(('fit', 'selection', 'refit'), ml.split_train_only_selection(frame, equal), strict=True))
    support = shared.support_check(parts, prior, equal)
    if support != shared.load(plan['reused_support']['path'])['heads']:
        raise ValueError('original100 support differs from accepted matched experiment')
    for head in REUSED:
        item = reused['heads'][head]
        for kind in ('model', 'metadata'):
            if shared.joint.sha(item[kind]['path']) != item[kind]['sha256']:
                raise ValueError('reused model bytes changed: ' + head)
        validate_reused(head, item, shared.load(item['metadata']['path']), metadata[head], equal, support[head])
    save(evidence / 'support-admission.json', dict(status='ALL13_ORIGINAL100_SUPPORT_VERIFIED',
        candidate=CANDIDATE, heads=support, original_dates=plan['training_days'],
        no_label_regeneration=True, final_read=False, reference_market=None))
    progress('SUPPORT_ACCEPTED_FITTING_SINGLE_MARKET_DIRECTION', HEAD, 12)
    output.mkdir(parents=True, exist_ok=False)
    heads = {}
    for head in REUSED:
        item = reused['heads'][head]
        for kind, suffix in (('model', '.txt'), ('metadata', '_meta.json')):
            target = output / (head + suffix)
            shutil.copy2(item[kind]['path'], target)
            if shared.joint.sha(target) != item[kind]['sha256']:
                raise ValueError('execution-layout copy differs')
        heads[head] = dict(source='accepted_D_single_market_equal_label_head',
            original_model=item['model'], original_metadata=item['metadata'], newly_fitted=False)
    keys = ('feature_manifest_path', 'feature_manifest_sha256', 'feature_dag_id',
        'feature_dag_sha256', 'feature_semantics_version', 'feature_timestamp_semantics',
        'feature_cutoff_semantics', 'feature_sampling_interval_ms', 'input_contract_id',
        'observation_contract_id', 'feature_contract_id', 'label_contract_id', 'split_manifest_id',
        'public_input_contract', 'volatility_unit_contract', 'feature_label_quote_calibration',
        'execution_trade_count_unit', 'reference_trade_count_unit', 'reference_trade_symbol')
    identity = {k: deepcopy(metadata[HEAD][k]) for k in keys}
    params = shared.parameters(metadata[HEAD], HEAD, plan['training_threads'])
    model, meta = ml.train_one(HEAD, parts['fit'], parts['selection'], params_override=params,
        refit_df=parts['refit'], selection_contract=equal, feature_identity=identity)
    meta.update(training_experiment_id=CANDIDATE, promotion_authority='research_only',
        parent_metadata_sha256=bundle['heads'][HEAD]['metadata_sha256'])
    path = output / (HEAD + '.txt')
    model.booster_.save_model(str(path))
    loaded = lgb.Booster(model_file=str(path))
    probes = {}
    for phase, part in parts.items():
        x, _, _, cols, report = ml.prepare_time_weighted_xy(part, HEAD, equal, phase=phase)
        if report != meta['per_head_time_weighting'][phase] or cols != loaded.feature_name():
            raise ValueError('saved direction schema/support differs')
        before = model.booster_.predict(x, num_threads=2)
        after = loaded.predict(x, num_threads=2)
        if not np.array_equal(before, after) or not np.isfinite(after).all():
            raise ValueError('saved direction predictions differ')
        probes[phase] = dict(rows=len(x), exact_saved_reload=True)
    meta['saved_model_prediction_checks'] = probes
    save(output / (HEAD + '_meta.json'), meta)
    heads[HEAD] = dict(source='new_BTCUSDC29_selection_and_full100_refit', newly_fitted=True,
        model=shared.bound(path), metadata=shared.bound(output / (HEAD + '_meta.json')))
    publish_model_contract(output, identity, list(ml.MODEL_SPECS), selection_contract=equal.to_metadata())
    validated = validate_public_bundle(output, live=False)
    if set(validated) != set(ml.MODEL_SPECS):
        raise ValueError('incomplete thirteen-head model')
    for head, item in validated.items():
        expected = shared.parameters(metadata[head], head, 2)
        if item['selection_fit_params'] != expected or item['feature_cols'] != list(equal.feature_cols):
            raise ValueError('final bundle actual training/input mismatch')
        for phase, row in support[head].items():
            if item['per_head_time_weighting'][phase] != row['report']:
                raise ValueError('final bundle support mismatch')
    save(evidence / 'model-admission.json', dict(status='ALL13_EQUAL_LABEL_ORIGINAL100_BTCUSDC_VERIFIED',
        candidate=CANDIDATE, training_days=100, heads=heads, accepted_heads=13,
        new_selection_fits=1, new_refits=1, reused_equal_label_heads=12,
        original_B0_weighted_heads_used=0, reference_market=None,
        bundle_sha256=shared.joint.sha(output / 'public_input_model.json'),
        source_plan=plan, final_read_during_fit=False, economic_dispatch=False,
        remaining_gates=['immutable economic runtime', 'real saved-account recovery',
                         'per-account input/prediction and cache lifecycle', 'unique PoolRun admission']))
    progress('ALL13_MODELS_ACCEPTED_ECONOMIC_ADMISSION_PENDING', None, 13)
