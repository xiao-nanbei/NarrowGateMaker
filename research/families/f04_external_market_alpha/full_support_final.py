"""Admit the train100 joint direction model and reuse the Final economic engine.

No scheduling, training, implicit model fallback, or live activation occurs here.
The eight-day M0/M1 models and their frozen execution identities are unchanged.
"""
from copy import deepcopy
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from data.observation import model_row
from data.runtime import ConsumerBundle, ObservationProfile
from research.families.f03_causal_13_head.public_input_panel import FEATURE_COLUMNS
from research.families.f04_external_market_alpha import final_b0 as final
from research.families.f04_external_market_alpha import full_support_direction as training
from research.families.f04_external_market_alpha.tardis_direction import (
    EXECUTION_MARKET, REFERENCE_FEATURE_PREFIX, REFERENCE_MARKET, _feature_metadata,
)

CANDIDATE = 'F03_TRAIN100_BTCUSDC_BTCUSDT_DIRECTION_20261014'
EXPERIMENT = 'B0_DEFENSE_URGENCY_OFF_20261011_F04_TRAIN100_FINAL107_20261014'
ARMS = (CANDIDATE,)


def load(path):
    return json.loads(Path(path).read_text())


def feature_columns(plan):
    reference = plan['reference_columns']
    if (plan['candidate'] != CANDIDATE or plan['baseline'] != final.BASELINE
            or plan['base_features'] != list(FEATURE_COLUMNS)
            or len(reference) != 12 or len(set(reference)) != 12
            or any(x in {'mid', 'close', 'vwap_10s', 'weighted_mid_proxy'} for x in reference)
            or type(plan['reference_asof_max_age_ns']) is not int
            or plan['reference_asof_max_age_ns'] < 0):
        raise ValueError('joint-model feature or baseline contract differs')
    return [*plan['base_features'], *(REFERENCE_FEATURE_PREFIX+x for x in reference)]


def reference_profile(plan):
    profiles = [row['reference_plan']['observation_profile'] for row in plan['days']]
    if not profiles or any(ObservationProfile(**p) != ObservationProfile(**profiles[0]) for p in profiles):
        raise ValueError('training reference profiles differ')
    ObservationProfile(**profiles[0]).require_market(REFERENCE_MARKET)
    return deepcopy(profiles[0])


def reference_plan(execution_root, facts_root, plan, *, contract_sha, parent_id):
    """Final reference uses the admitted training observation rule, not old caches."""
    profile = reference_profile(plan)
    declared = dict(profile, origin='same simulated reference observation contract as train100')
    result = final.reference_plan(execution_root, facts_root,
        {'observation': {'reference': declared}}, contract_sha=contract_sha, parent_id=parent_id)
    result['observation_profile'] = profile
    return result


def admit_training(root):
    """Read complete real training evidence; return a receipt, never dispatch."""
    root = Path(root)
    plan, progress = load(root/'plan.json'), load(root/'progress.json')
    columns = feature_columns(plan)
    reference_profile(plan)
    days = [x['day'] for x in plan['days']]
    if (len(days) != 100 or days != sorted(set(days))
            or days != plan['baseline_metadata']['train_only_selection']['refit_days']
            or progress['phase'] != 'MODEL_TRAINED_FINAL_ADMISSION_PENDING'
            or sorted(progress['completed_days']) != days):
        raise ValueError('full training is not complete')
    if training.sha(root/'source-identity.json') != plan['source_identity_sha256']:
        raise ValueError('training source identity differs')
    for name, digest in load(root/'source-identity.json')['files'].items():
        path = root/'source'/name
        if (path.is_symlink() or not path.resolve().is_relative_to((root/'source').resolve())
                or training.sha(path) != digest):
            raise ValueError('frozen training source changed')
    for row in plan['days']:
        receipt = load(root/'reference-features'/(row['day']+'.json'))
        path = root/'reference-features'/('BTCUSDT-features-'+row['day']+'.parquet')
        expected = row['reference_plan']
        if (receipt['plan'] != expected or receipt['file'] != path.name
                or receipt['size'] != path.stat().st_size or receipt['sha256'] != training.sha(path)
                or receipt['rows'] != (expected['end_ns']-expected['start_ns'])//expected['observation_profile']['feature_period_ns']):
            raise ValueError('training reference day is incomplete or changed')
    meta = load(root/'model/manifest.json')
    provenance = dict(plan_sha256=training.sha(root/'plan.json'),
        source_identity_sha256=plan['source_identity_sha256'],
        panel_sha256=training.sha(root/'training-panel.parquet'),
        reference_coverage_sha256=training.sha(root/'reference-coverage.json'))
    if (meta['schema'] != 'narrowgate.f04.f03_method_direction.v1'
            or meta['name'] != training.HEAD or meta['feature_cols'] != columns
            or meta['baseline_method_metadata'] != plan['baseline_metadata']
            or meta['provenance'] != provenance
            or meta['selection_fit_params'] != plan['baseline_metadata']['selection_fit_params']
            or meta['refit_fit_params'] != dict(meta['selection_fit_params'], n_estimators=meta['best_iteration'])
            or meta['new_fits'] != 1 or meta['refits'] != 1
            or meta['external_panel_read'] is not False or meta['live_authority'] is not False
            or training.sha(root/'model'/(training.HEAD+'.txt')) != meta['model_sha256']
            or progress['model_sha256'] != meta['model_sha256']):
        raise ValueError('model is not the one authorized F03-method fit/refit')
    frame = pd.read_parquet(root/'training-panel.parquet')
    phases = training.prepared_phases(frame, plan['baseline_metadata'], columns)
    if {k: v[4] for k, v in phases.items()} != meta['support']:
        raise ValueError('model support differs from the actual panel')
    coverage = load(root/'reference-coverage.json')['days']
    if sorted(coverage) != days or sum(x['decisions'] for x in coverage.values()) != len(frame):
        raise ValueError('reference coverage differs from the full panel')
    return dict(schema='narrowgate.f04.train100_model_admission.v1',
        visibility='local_only_do_not_publish', status='TRAINING_MODEL_VERIFIED',
        candidate=CANDIDATE, plan_sha256=provenance['plan_sha256'],
        model_manifest_sha256=training.sha(root/'model/manifest.json'),
        model_sha256=meta['model_sha256'], source_identity_sha256=plan['source_identity_sha256'],
        reference_days=100, feature_cols=columns, support=meta['support'],
        final_input_and_executor_admission=False, economic_complete=False)


class DirectionModel:
    """One admitted 41-feature booster, never the historical M0/M1 pair."""

    def __init__(self, root, *, plan_path, admission_path, expected_admission_sha256):
        root = Path(root)
        admission = load(admission_path)
        self.training_plan, self.manifest = load(plan_path), load(root/'manifest.json')
        self.feature_cols = tuple(feature_columns(self.training_plan))
        if (training.sha(admission_path) != expected_admission_sha256
                or admission['status'] != 'TRAINING_MODEL_VERIFIED'
                or admission['candidate'] != CANDIDATE
                or admission['plan_sha256'] != training.sha(plan_path)
                or admission['model_manifest_sha256'] != training.sha(root/'manifest.json')
                or admission['feature_cols'] != list(self.feature_cols)
                or self.manifest['feature_cols'] != list(self.feature_cols)
                or admission['model_sha256'] != self.manifest['model_sha256']
                or training.sha(root/(training.HEAD+'.txt')) != admission['model_sha256']):
            raise ValueError('unadmitted or changed joint model')
        self.booster = lgb.Booster(model_file=str(root/(training.HEAD+'.txt')))
        if self.booster.feature_name() != list(self.feature_cols):
            raise ValueError('joint booster internal feature order differs')
        self.arm = CANDIDATE

    def predict(self, matrix):
        if (matrix.ndim != 2 or matrix.shape[1] != len(self.feature_cols)
                or np.isinf(matrix).any()):
            raise ValueError('joint prediction schema differs')
        values = np.asarray(self.booster.predict(matrix, num_threads=1), dtype=float)
        if (values.shape != (len(matrix),) or not np.isfinite(values).all()
                or ((values < 0) | (values > 1)).any()):
            raise ValueError('invalid joint direction probability')
        return values


class JointDirectionSignalEngine:
    """Keep 12 base outputs; feed causal USDC+USDT features to one direction head."""

    def __init__(self, execution_root, reference_file, reference_receipt,
                 frozen_base_model_root, direction_model, *, expected_input_id,
                 expected_reference_plan, expected_base_manifest_sha256):
        self.execution = ConsumerBundle(execution_root)
        ep = self.execution.manifest['plan']
        receipt = load(reference_receipt)
        rp = receipt['plan']
        if (self.execution.input_manifest_id != expected_input_id
                or ep['market_id'] != EXECUTION_MARKET or rp != expected_reference_plan
                or rp['market_id'] != REFERENCE_MARKET
                or rp['execution_parent_input_manifest_id'] != expected_input_id
                or ObservationProfile(**rp['observation_profile']) != ObservationProfile(**reference_profile(direction_model.training_plan))
                or (rp['start_ns'], rp['end_ns']) != (ep['start_ns'], ep['end_ns'])
                or training.sha(reference_file) != receipt['sha256']
                or Path(reference_file).stat().st_size != receipt['size']
                or training.sha(Path(frozen_base_model_root)/'public_input_model.json') != expected_base_manifest_sha256):
            raise ValueError('joint Final market/reference/base binding differs')
        self.model = direction_model
        self.reference = pd.read_parquet(reference_file)
        if len(self.reference) != receipt['rows']:
            raise ValueError('reference projection row coverage differs')
        from strategy.signal import SignalEngine
        self.base = SignalEngine.from_public_models(frozen_base_model_root,
            symbol='BTCUSDC', ret_demean_halflife=0)
        self.decisions = self.reference_context_missing = self.reference_values_missing = 0

    def compute_feature_frames(self, frames):
        frames = tuple(frames)
        if not frames:
            return []
        base = self.base.compute_feature_frames(frames)
        if len(base) != len(frames):
            raise ValueError('base predictions do not cover all joint decisions')
        plan = self.model.training_plan
        extra, report = training.causal_reference_rows(self.reference,
            [frame.cutoff_ns for frame in frames], plan['reference_columns'],
            plan['reference_asof_max_age_ns'])
        local_meta = _feature_metadata(self.execution, FEATURE_COLUMNS)
        local = np.asarray([model_row(frame, local_meta, decision_ns=frame.cutoff_ns)
                            for frame in frames], dtype=float)
        matrix = np.column_stack((local, extra.to_numpy(dtype=float)))
        predicted = self.model.predict(matrix)
        for original, direction in zip(base, predicted, strict=True):
            original.touch_conditioned_up_probability_10000ms = float(direction)
        self.decisions += len(frames)
        self.reference_context_missing += report['missing_context']
        self.reference_values_missing += int(extra.isna().any(axis=1).sum())
        return base

    def compute_signal(self, *, feature_frame, decision_ns):
        if decision_ns != feature_frame.cutoff_ns:
            raise ValueError('joint decision must use its ready feature cutoff')
        return self.compute_feature_frames((feature_frame,))[0]

    def report(self):
        return dict(arm=CANDIDATE, decisions=self.decisions,
            reference_context_missing=self.reference_context_missing,
            reference_values_missing=self.reference_values_missing,
            reference_market=REFERENCE_MARKET, reference_observation_parity='simulated_not_measured')


def identity(spec):
    return final.identity(spec, experiment=EXPERIMENT, arms=ARMS)


def parameters(original_parent):
    return final.parameters(deepcopy(original_parent), CANDIDATE, arms=ARMS)


def validate_cell(output, spec):
    return final.validate_cell(output, spec, experiment=EXPERIMENT, arms=ARMS)


def execute_cell(prepared, **kwargs):
    return final.execute_cell(prepared, experiment=EXPERIMENT, arms=ARMS, **kwargs)
