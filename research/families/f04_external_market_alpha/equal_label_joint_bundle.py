"""All thirteen heads use unit-label loss; only the 10s direction uses USDT.

Training-only composite, not a live/legacy homogeneous bundle. Reuse the
already accepted equal-label joint head without refitting or relabelling it.
"""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from data.facts import save_private_json as save
from research.families.f03_causal_13_head import development300 as original
from research.families.f03_causal_13_head import ml_model as ml
from research.families.f04_external_market_alpha import full_support_direction as joint

CANDIDATE = 'F03_TRAIN100_13HEAD_EQUAL_LABEL_JOINT_DIR10_20261015'
POLICY = 'narrowgate.f03.valid_label_equal.v1'
SCHEMA = 'narrowgate.f04.train100_equal_label_13head_composite.v1'
REMAINING = tuple(h for h in ml.MODEL_SPECS if h != joint.HEAD)


def load(path):
    return json.loads(Path(path).read_text())


def bound(path):
    path = Path(path).resolve(strict=True)
    return dict(path=str(path), sha256=joint.sha(path), size=path.stat().st_size)


def parameters(meta, head, threads=2):
    if type(threads) is not int or not 1 <= threads <= 2:
        raise ValueError('training is bounded to two threads')
    result = deepcopy(meta['selection_fit_params'])
    for key in ('unbalance', 'unbalanced_sets', 'scale_pos_weight', 'class_weight'):
        if key in result:
            raise ValueError('unexpected class weighting alias: ' + key)
    if ml.MODEL_SPECS[head][3]:
        result['is_unbalance'] = False
    result['n_jobs'] = threads
    return result


def contracts(metadata):
    prior = joint.training_contract(metadata, metadata['feature_cols'])
    policy = dict(prior.sample_weight_policy, schema_version=POLICY,
                  half_life_days='inf', date_normalization='none', class_adjustment='none')
    equal = replace(prior, sample_weight_policy=policy)
    ml._validate_sample_weight_policy(policy, equal.refit_days)
    return prior, equal


def support_check(parts, prior, equal):
    result = {}
    for head, spec in ml.MODEL_SPECS.items():
        result[head] = {}
        for phase, frame in parts.items():
            x, y, w, cols, report = ml.prepare_time_weighted_xy(frame, head, equal, phase=phase)
            ox, oy, _, old_cols, old_report = ml.prepare_time_weighted_xy(frame, head, prior, phase=phase)
            if not x.equals(ox) or not y.equals(oy) or cols != old_cols:
                raise ValueError('changed masks or labels: ' + head)
            if w is not None and not (w == 1.).all():
                raise ValueError('non-unit label weights')
            if (not np.isfinite(y.to_numpy()).all() or np.isinf(x.to_numpy()).any()
                    or not np.isfinite(x.to_numpy()).any(axis=0).all()
                    or (spec[3] and set(y.unique()) != {0., 1.})):
                raise ValueError('unsupported head: ' + head)
            result[head][phase] = dict(rows=len(y), days=report['effective_day_count'],
                weight_min=1., weight_max=1., identical_original_masks=True,
                class_counts={str(k): int(v) for k, v in y.value_counts().items()} if spec[3] else None,
                old_rows=old_report['effective_rows'], report=report)
    return result


def validate_composite(manifest, *, artifact_resolver=None):
    if manifest.get('schema') != SCHEMA or manifest.get('candidate') != CANDIDATE:
        raise ValueError('wrong composite identity')
    if set(manifest.get('heads', {})) != set(ml.MODEL_SPECS):
        raise ValueError('all thirteen heads are required')
    dates = manifest['training_days']
    if len(dates) != 100 or dates != sorted(set(dates)):
        raise ValueError('exact original 100 training dates required')
    for head, item in manifest['heads'].items():
        expected = 41 if head == joint.HEAD else 29
        if (len(item['feature_cols']) != expected or item['training_days'] != dates
                or item['per_valid_label_weight'] != 1.
                or item['date_normalization'] != 'none' or item['class_adjustment'] != 'none'
                or item['age_decay'] != 'none' or item.get('is_unbalance', False)):
            raise ValueError('mixed input or weight scope: ' + head)
        for name in ('model', 'metadata'):
            b = item[name]
            path = artifact_resolver(b) if artifact_resolver is not None else b['path']
            if joint.sha(path) != b['sha256']:
                raise ValueError('changed model artifact: ' + head)
    return manifest


def fit(plan, output, evidence, progress):
    output, evidence = Path(output), Path(evidence)
    if output.exists():
        raise FileExistsError('existing training must not be repeated')
    for name in ('baseline_plan', 'original_panel', 'original_manifest', 'joint_plan',
                 'joint_admission', 'joint_manifest', 'joint_model', 'p3'):
        item = plan[name]
        if joint.sha(item['path']) != item['sha256']:
            raise ValueError('changed inherited artifact: ' + name)
    baseline = load(plan['baseline_plan']['path'])
    metadata = baseline['baseline_head_metadata']
    root = Path(baseline['baseline_bundle']['path']).parent
    bundle = load(root/'public_input_model.json')
    for head, item in bundle['heads'].items():
        if (joint.sha(root/(head+'.txt')) != item['sha256']
                or joint.sha(root/(head+'_meta.json')) != item['metadata_sha256']
                or load(root/(head+'_meta.json')) != metadata[head]):
            raise ValueError('original B0 metadata mismatch')
    raw = pd.read_parquet(plan['original_panel']['path'])
    frame = original.convert_original_labels(raw, metadata)
    del raw
    prior, equal = contracts(metadata[joint.HEAD])
    if (len(frame) != 864000 or list(equal.refit_days) != plan['training_days']
            or [len(equal.fit_days), len(equal.embargo_days), len(equal.selection_days)] != [79, 1, 20]):
        raise ValueError('training calendar differs')
    for meta in metadata.values():
        if contracts(meta)[1] != equal:
            raise ValueError('baseline head training contracts differ')
    parts = dict(zip(('fit', 'selection', 'refit'), ml.split_train_only_selection(frame, equal), strict=True))
    support = support_check(parts, prior, equal)
    reused = load(plan['joint_manifest']['path'])
    admission = load(plan['joint_admission']['path'])
    if (admission['status'] != 'TRAIN100_EQUAL_LABEL_MODEL_VERIFIED'
            or reused['model_sha256'] != plan['joint_model']['sha256']
            or reused['selection_fit_params']['is_unbalance'] is not False
            or reused['refit_fit_params']['is_unbalance'] is not False
            or reused['weighting'] != dict(per_valid_label=1., date_normalization='none',
                age_decay='none', class_adjustment='none', is_unbalance=False)):
        raise ValueError('joint direction is not an accepted equal-label model')
    # Exact original 29-feature rows and labels must agree with the joint panel.
    jp = load(plan['joint_plan']['path'])
    jframe = pd.read_parquet(jp['panel']['path'])
    common = list(equal.feature_cols) + [joint.LABEL, joint.END, ml.WEIGHT_COL]
    if not frame[common].equals(jframe[common]):
        raise ValueError('joint and other-head execution rows or labels differ')
    del jframe
    for phase, row in support[joint.HEAD].items():
        if row['rows'] != reused['support'][phase]['effective_rows']:
            raise ValueError('joint model has a different eligibility mask')
    save(evidence/'support-admission.json', dict(status='ALL_13_SUPPORT_VERIFIED',
        heads=support, original_dates=plan['training_days'], joint_panel_common_rows_exact=True,
        new_label_generation=False, final_read=False))
    output.mkdir(parents=True, exist_ok=False)
    heads = {}
    def entry(model, meta, cols, source):
        return dict(model=bound(model), metadata=bound(meta), feature_cols=cols,
            training_days=plan['training_days'], per_valid_label_weight=1.,
            date_normalization='none', age_decay='none', class_adjustment='none',
            is_unbalance=False, source=source)
    heads[joint.HEAD] = entry(plan['joint_model']['path'], plan['joint_manifest']['path'],
                             reused['feature_cols'], 'accepted_joint_model_reused_without_refit')
    identity_keys = ('feature_manifest_path', 'feature_manifest_sha256', 'feature_dag_id',
        'feature_dag_sha256', 'feature_semantics_version', 'feature_timestamp_semantics',
        'feature_cutoff_semantics', 'feature_sampling_interval_ms', 'input_contract_id',
        'observation_contract_id', 'feature_contract_id', 'label_contract_id', 'split_manifest_id',
        'public_input_contract', 'volatility_unit_contract', 'feature_label_quote_calibration',
        'execution_trade_count_unit', 'reference_trade_count_unit', 'reference_trade_symbol')
    identity = {k: deepcopy(metadata[joint.HEAD][k]) for k in identity_keys}
    for head in REMAINING:
        progress('FITTING_REMAINING_12_HEADS', head, len(heads))
        params = parameters(metadata[head], head, plan['training_threads'])
        model, meta = ml.train_one(head, parts['fit'], parts['selection'], params_override=params,
            refit_df=parts['refit'], selection_contract=equal, feature_identity=identity)
        meta.update(training_experiment_id=CANDIDATE, promotion_authority='research_only',
                    parent_metadata_sha256=bundle['heads'][head]['metadata_sha256'])
        model_path, meta_path = output/(head+'.txt'), output/(head+'_meta.json')
        model.booster_.save_model(str(model_path))
        loaded = lgb.Booster(model_file=str(model_path))
        probes = {}
        for phase, part in parts.items():
            x, _, _, cols, report = ml.prepare_time_weighted_xy(part, head, equal, phase=phase)
            if report != meta['per_head_time_weighting'][phase] or cols != loaded.feature_name():
                raise ValueError('saved model schema/support differs')
            before = model.booster_.predict(x, num_threads=plan['training_threads'])
            after = loaded.predict(x, num_threads=plan['training_threads'])
            if not np.array_equal(before, after) or not np.isfinite(after).all():
                raise ValueError('saved model prediction mismatch')
            probes[phase] = dict(rows=len(x), exact_saved_reload=True)
        meta['saved_model_prediction_checks'] = probes
        save(meta_path, meta)
        heads[head] = entry(model_path, meta_path, meta['feature_cols'], 'new_fit_and_full100_refit')
    result = dict(schema=SCHEMA, candidate=CANDIDATE, heads=heads,
        training_days=plan['training_days'], p3=plan['p3'], new_selection_fits=12, new_refits=12,
        reused_equal_label_joint_heads=1, total_equal_label_heads=13,
        homogeneous_public_bundle=False, requires_dedicated_composite_runtime=True,
        original_B0_heads_used_at_inference=0, final_read_during_fit=False,
        economic_admission=False, control_new_runs=0, continuous_accounts=0)
    validate_composite(result)
    save(output/'manifest.json', result)
    save(evidence/'model-admission.json', dict(status='ALL13_EQUAL_LABEL_TRAIN100_MODELS_VERIFIED',
        candidate=CANDIDATE, model_manifest=bound(output/'manifest.json'),
        support=bound(evidence/'support-admission.json'), newly_fitted_heads=12,
        reused_joint_head=plan['joint_model'], total_equal_label_heads=13,
        economic_dispatch=False, remaining_gates=plan['economic_gates']))
    progress('ALL13_MODELS_ACCEPTED_WAITING_PREDECESSOR_AND_ECONOMIC_GATES', None, 13)
