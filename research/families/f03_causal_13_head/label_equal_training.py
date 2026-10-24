"""One fixed valid-label-uniform Development300 candidate; no dispatch.

The accepted panel and label support are inherited, not regenerated. This
candidate changes both daily normalization and classification reweighting;
it is not an isolated estimate of either component's economic contribution.
"""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import numpy as np
import pandas as pd

from data.facts import save_private_json as save
from data.observation import CONTRACT, FEATURE_CONTRACT
from data.tardis_input import CONTRACT as INPUT_CONTRACT
from research.families.f03_causal_13_head import development300 as base
from research.families.f03_causal_13_head import ml_model as ml
from research.families.f03_causal_13_head.public_input_panel import publish_model_contract
from strategy.model_contract import absolute_price_variance_unit_contract
from strategy.public_model_contract import validate_public_bundle

CANDIDATE = 'F03_BTCUSDC_TRAIN300_13HEAD_EQUAL_LABEL_20261015'
POLICY = 'narrowgate.f03.valid_label_equal.v1'


def load(path):
    return json.loads(Path(path).read_text())


def equal_policy(original):
    result = deepcopy(original)
    result.update(schema_version=POLICY, half_life_days='inf',
                  date_normalization='none', class_adjustment='none')
    return result


def head_params(original, is_classifier):
    params = deepcopy(original)
    params['n_jobs'] = 4
    for name in ('unbalance', 'unbalanced_sets', 'scale_pos_weight', 'class_weight'):
        if name in params:
            raise ValueError('unexpected inherited class-weight alias: ' + name)
    if is_classifier:
        params['is_unbalance'] = False
    return params


def bindings(plan):
    if (plan['candidate'] != CANDIDATE or plan['economic_accounts'] != 54
            or plan['continuous_accounts'] != 0 or plan['training_threads'] != 4
            or plan['policy_schema'] != POLICY):
        raise ValueError('fixed equal-label candidate contract mismatch')
    for name in ('base_plan', 'panel', 'panel_manifest', 'base_model_admission'):
        item = plan[name]
        if base.sha(item['path']) != item['sha256']:
            raise ValueError('inherited artifact changed: ' + name)
    old = load(plan['base_plan']['path'])
    base.validate_plan(old)
    manifest = load(plan['panel_manifest']['path'])
    if manifest['training_days'] != list(base.DAYS) or manifest['panel_sha256'] != plan['panel']['sha256']:
        raise ValueError('complete accepted 300-day panel required')
    baseline = load(old['baseline_bundle']['path'])
    for head, item in baseline['heads'].items():
        root = Path(old['baseline_bundle']['path']).parent
        if (base.sha(root / (head + '.txt')) != item['sha256']
                or base.sha(root / (head + '_meta.json')) != item['metadata_sha256']
                or load(root / (head + '_meta.json')) != old['baseline_head_metadata'][head]):
            raise ValueError('actual B0 model/metadata differs from inherited plan')
    prior = base.selection_contract(old, manifest, plan['panel_manifest']['path'])
    contract = replace(prior, sample_weight_policy=equal_policy(prior.sample_weight_policy))
    ml._validate_sample_weight_policy(contract.sample_weight_policy, contract.refit_days)
    return old, manifest, prior, contract


def frame_and_support(plan):
    old, manifest, prior, contract = bindings(plan)
    full = pd.read_parquet(plan['panel']['path'])
    if len(full) != 300 * 8640 or not full.index.is_unique or not full.index.is_monotonic_increasing:
        raise ValueError('panel row/clock identity mismatch')
    parts = dict(zip(('fit', 'selection', 'refit'), ml.split_train_only_selection(full, contract), strict=True))
    support = {}
    for head, spec in ml.MODEL_SPECS.items():
        support[head] = {}
        for phase, frame in parts.items():
            x, y, w, columns, report = ml.prepare_time_weighted_xy(frame, head, contract, phase=phase)
            ox, oy, ow, ocolumns, old_report = ml.prepare_time_weighted_xy(frame, head, prior, phase=phase)
            if not x.equals(ox) or not y.equals(oy) or columns != ocolumns:
                raise ValueError('label mask/features changed: ' + head + '/' + phase)
            if w is not None and not (w == 1.0).all():
                raise ValueError('non-uniform valid-label weights')
            if (not np.isfinite(y.to_numpy()).all()
                    or not np.isfinite(x.to_numpy()).any(axis=0).all()
                    or (spec[3] and set(y.unique()) != {0., 1.})):
                raise ValueError('insufficient label/feature support: ' + head + '/' + phase)
            support[head][phase] = dict(rows=len(y), supported_days=report['effective_day_count'],
                unsupported_days=report['unsupported_days'], new_weight_min=1.0, new_weight_max=1.0,
                new_weight_concentration_rows=float(len(y)), statistical_independence_claim=False,
                old_weight_min=float(ow.min()) if ow is not None else 1.0,
                old_weight_max=float(ow.max()) if ow is not None else 1.0,
                old_weight_concentration_rows=old_report['weighted_effective_rows_concentration_only'],
                class_counts={str(k): int(v) for k, v in y.value_counts().items()} if spec[3] else None,
                masks_features_and_labels_exactly_equal=True)
            del x, y, w, ox, oy, ow
    return old, manifest, contract, parts, support


def fit(plan, output, evidence, progress):
    output, evidence = Path(output), Path(evidence)
    if output.exists():
        raise FileExistsError('inspect existing model; no implicit fit retry')
    old, manifest, contract, parts, support = frame_and_support(plan)
    save(evidence / 'support-admission.json', dict(status='PASS', heads=support,
        panel_sha256=plan['panel']['sha256'], policy=contract.sample_weight_policy,
        final_read=False, same_label_masks=True, economic_dispatch=False))
    progress('SUPPORT_ACCEPTED_FITTING', None, 0)
    identity = dict(feature_manifest_path=plan['panel_manifest']['path'],
        feature_manifest_sha256=plan['panel_manifest']['sha256'], feature_dag_id=FEATURE_CONTRACT,
        feature_dag_sha256=manifest['feature_dag_sha256'], feature_semantics_version=1,
        feature_panel_split={'train': list(base.DAYS)}, feature_timestamp_semantics='feature_ready_index',
        feature_cutoff_semantics='feature_ready_index', feature_sampling_interval_ms=10000,
        input_contract_id=INPUT_CONTRACT, observation_contract_id=CONTRACT, feature_contract_id=FEATURE_CONTRACT,
        label_contract_id=manifest['label_contract_id'], split_manifest_id=base.SPLIT_ID,
        public_input_contract={'decision_time_semantics': 'feature_ready_index'},
        volatility_unit_contract=absolute_price_variance_unit_contract('BTCUSDC'),
        feature_label_quote_calibration=old['p3'], execution_trade_count_unit='individual_execution',
        reference_trade_count_unit='unavailable', reference_trade_symbol=None)
    output.mkdir(parents=True, exist_ok=False)
    heads = {}
    for head, spec in ml.MODEL_SPECS.items():
        progress('FITTING_AND_REFITTING', head, len(heads))
        params = head_params(old['baseline_head_metadata'][head]['selection_fit_params'], spec[3])
        model, meta = ml.train_one(head, parts['fit'], parts['selection'], params_override=params,
            refit_df=parts['refit'], selection_contract=contract, feature_identity=identity)
        meta.update(training_experiment_id=CANDIDATE, promotion_authority='research_only',
                    parent_model=old['baseline_bundle'], previous_use=plan['previous_use'])
        model.booster_.save_model(str(output / (head + '.txt')))
        save(output / (head + '_meta.json'), meta)
        for phase, part in parts.items():
            x, y, w, cols, report = ml.prepare_time_weighted_xy(part, head, contract, phase=phase)
            if report != meta['per_head_time_weighting'][phase]:
                raise ValueError('model metadata/support mismatch')
            prediction = model.predict_proba(x.iloc[::max(1, len(x)//1000)]) if spec[3] else model.predict(x.iloc[::max(1, len(x)//1000)])
            if not np.isfinite(prediction).all():
                raise ValueError('non-finite model prediction')
            del x, y, w
        if (meta['selection_fit_params'] != params
                or meta['refit_fit_params'] != dict(params, n_estimators=meta['best_iteration'])):
            raise ValueError('unexpected fitted parameters')
        heads[head] = dict(best_iteration=meta['best_iteration'],
            model_sha256=base.sha(output / (head + '.txt')),
            metadata_sha256=base.sha(output / (head + '_meta.json')),
            selection_rows=meta['selection_training_rows'], refit_rows=meta['final_training_rows'])
        del model
    publish_model_contract(output, identity, list(ml.MODEL_SPECS), selection_contract=contract.to_metadata())
    validated = validate_public_bundle(output, live=False)
    if set(validated) != set(ml.MODEL_SPECS):
        raise ValueError('incomplete public bundle')
    save(evidence / 'model-admission.json', dict(status='ALL_13_MODELS_AND_ACTUAL_300_DAY_SUPPORT_VERIFIED',
        candidate=CANDIDATE, heads=heads, selection_fits=13, refits=13, training_days=300,
        bundle_sha256=base.sha(output / 'public_input_model.json'), plan=plan,
        final_read_during_fit=False, economic_dispatch=False,
        remaining_gates=['immutable economic runtime', 'input and prediction identity',
                         'real checkpoint recovery', 'control equivalence', 'PoolRun admission']))
    progress('MODEL_ACCEPTED_ECONOMIC_ADMISSION_PENDING', None, 13)
