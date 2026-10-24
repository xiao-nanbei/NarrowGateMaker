"""Research-only mixed-schema inference for the all-13-head train100 candidate.

This is not a homogeneous public bundle and never loads original B0 heads.
The completed study required immutable binding, real recovery and per-account
admission. The owner removed the historical cross-model completion wait;
importing this module grants no new execution authority.
"""
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from data.observation import model_row
from data.runtime import ConsumerBundle, ObservationProfile
from research.families.f03_causal_13_head.public_input_panel import FEATURE_COLUMNS
from research.families.f04_external_market_alpha import equal_label_joint_bundle as bundle
from research.families.f04_external_market_alpha import full_support_final as joint
from strategy.signal import Prediction, REQUIRED_MODEL_HEADS

CANDIDATE = bundle.CANDIDATE
EXPERIMENT = 'B0_DEFENSE_URGENCY_OFF_20261011_TRAIN100_ALL13_EQUAL_FINAL107_20261015'
ARMS = (CANDIDATE,)


class ExactArtifactMap:
    """Explicit byte-bound host locations; never rewrite training manifests.

    Every reference must be listed. Missing entries cannot fall back to the
    original workstation path, and no prefix substitution is performed.
    """

    def __init__(self, root, files):
        self.root = Path(root).resolve(strict=True)
        self.files = files

    def __call__(self, item):
        entry = self.files.get(item['path'])
        if (entry is None or entry['sha256'] != item['sha256']
                or ('size' in item and entry['size'] != item['size'])):
            raise ValueError('unmapped or changed composite artifact')
        relative = Path(entry['relative_path'])
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('composite artifact escapes bound root')
        path = self.root / relative
        if (any(p.is_symlink() for p in (path, *path.parents) if p != self.root)
                or not path.resolve().is_relative_to(self.root)
                or not path.is_file() or path.stat().st_size != entry['size']):
            raise ValueError('unsafe or missing composite artifact')
        return path


def _bound(item, artifact_resolver=None):
    path = Path(artifact_resolver(item) if artifact_resolver is not None else item['path'])
    if (path.is_symlink() or not path.is_file()
            or ('size' in item and path.stat().st_size != item['size'])
            or joint.training.sha(path) != item['sha256']):
        raise ValueError('changed composite dependency: ' + str(path))
    return path


def runtime_metadata(metadata, model_text, inherited_missing_policy):
    """Keep the original explicit NaN policy, absent from training-only JSON.

    The homogeneous runtime also obtains this field from its public manifest.
    Admit it here only with the bound original policy and actual booster flags;
    do not modify training metadata or substitute zeros for unavailable inputs.
    """
    if (inherited_missing_policy != 'native_nan'
            or metadata.get('missing_policy', 'native_nan') != 'native_nan'
            or '[use_missing: 1]' not in model_text
            or '[zero_as_missing: 0]' not in model_text):
        raise ValueError('composite booster missing-value semantics differ')
    return dict(metadata, missing_policy='native_nan')


def validate_head_origin(head, item, metadata):
    if head == joint.training.HEAD:
        if (item['source'] != 'accepted_joint_model_reused_without_refit'
                or metadata.get('schema') != 'narrowgate.f04.valid_label_equal_direction.v1'
                or metadata.get('model_sha256') != item['model']['sha256']
                or metadata.get('weighting') != dict(per_valid_label=1.,
                    date_normalization='none', age_decay='none',
                    class_adjustment='none', is_unbalance=False)):
            raise ValueError('joint head is not the accepted equal-label model')
    elif (item['source'] != 'new_fit_and_full100_refit'
            or metadata.get('training_experiment_id') != CANDIDATE):
        raise ValueError('old B0 or other experiment head cannot be substituted')


class CompositeModels:
    """Thirteen exact accepted boosters, with a separate 41-column direction."""

    def __init__(self, manifest_path, *, plan_path, admission_path,
                 expected_admission_sha256, artifact_resolver=None):
        def bound(item):
            return _bound(item, artifact_resolver)

        if joint.training.sha(admission_path) != expected_admission_sha256:
            raise ValueError('changed model admission')
        admission, plan = joint.load(admission_path), joint.load(plan_path)
        if (admission['status'] != 'ALL13_EQUAL_LABEL_TRAIN100_MODELS_VERIFIED'
                or admission['candidate'] != CANDIDATE or plan['candidate'] != CANDIDATE
                or bound(admission['model_manifest']).resolve() != Path(manifest_path).resolve()):
            raise ValueError('unadmitted all-head composite')
        self.manifest = bundle.validate_composite(joint.load(manifest_path), artifact_resolver=bound)
        joint_plan = joint.load(bound(plan['joint_plan']))
        self.training_plan = joint.load(bound(joint_plan['original_plan']))
        base_plan = joint.load(bound(plan['baseline_plan']))
        inherited = joint.load(bound(base_plan['baseline_bundle']))
        columns = joint.feature_columns(self.training_plan)
        joint.reference_profile(self.training_plan)
        if (self.manifest['training_days'] != plan['training_days']
                or self.manifest['p3'] != plan['p3']
                or self.manifest['original_B0_heads_used_at_inference'] != 0):
            raise ValueError('composite training or P3 scope differs')
        bound(plan['p3'])
        self.models, self.metadata, self.columns = {}, {}, {}
        for head in REQUIRED_MODEL_HEADS:
            item = self.manifest['heads'][head]
            expected = columns if head == joint.training.HEAD else list(FEATURE_COLUMNS)
            if item['feature_cols'] != expected:
                raise ValueError('mixed-schema feature order differs: ' + head)
            model_path = bound(item['model'])
            metadata = joint.load(bound(item['metadata']))
            validate_head_origin(head, item, metadata)
            model = lgb.Booster(model_file=str(model_path))
            if model.feature_name() != expected or metadata['feature_cols'] != expected:
                raise ValueError('booster or metadata feature order differs: ' + head)
            self.models[head] = model
            self.metadata[head] = runtime_metadata(metadata, model_path.read_text(),
                inherited['heads'][head]['missing_policy'])
            self.columns[head] = tuple(expected)

    def predict(self, local, reference):
        local, reference = np.asarray(local, dtype=float), np.asarray(reference, dtype=float)
        if (local.ndim != 2 or local.shape[1] != 29
                or reference.shape != (len(local), 12)
                or np.isinf(local).any() or np.isinf(reference).any()
                or set(self.models) != set(REQUIRED_MODEL_HEADS)):
            raise ValueError('complete 29/12-column composite input required')
        combined = np.column_stack((local, reference))
        values = {}
        for head in REQUIRED_MODEL_HEADS:
            matrix = combined if head == joint.training.HEAD else local
            output = np.asarray(self.models[head].predict(matrix, num_threads=1), dtype=float)
            if output.shape != (len(local),) or not np.isfinite(output).all():
                raise ValueError('invalid composite prediction: ' + head)
            values[head] = output
        return values


def prediction_from_raw(raw, execution_row, cutoff_ns):
    """Original no-demeaning Prediction mapping, without a fake 29-head bundle.

    Only variance flooring and adverse-probability clipping occur in the
    original raw-output consumer. Tests compare every field with that path.
    The diagnostic features remain the 29 execution features, as in A/B/C.
    """
    row = np.asarray(execution_row, dtype=float)
    if (set(raw) != set(REQUIRED_MODEL_HEADS) or row.shape != (29,)
            or np.isinf(row).any() or not all(np.isfinite(v) for v in raw.values())):
        raise ValueError('incomplete raw heads or execution diagnostics')
    pred = Prediction(ts=cutoff_ns / 1_000_000_000,
        features=row.copy(), feature_dict=dict(zip(FEATURE_COLUMNS, row, strict=True)))
    for head in REQUIRED_MODEL_HEADS:
        value = float(raw[head])
        if head.startswith('absolute_price_variance_rate_'):
            value = max(value, 0.)
        elif head.startswith('touch_side_adverse_probability_'):
            value = float(np.clip(value, 0., 1.))
        setattr(pred, head, value)
    return pred


class CompositeSignalEngine:
    """Causal reference projection plus all newly trained head outputs."""

    def __init__(self, execution_root, reference_file, reference_receipt, models,
                 *, expected_input_id, expected_reference_plan):
        self.execution = ConsumerBundle(execution_root)
        ep = self.execution.manifest['plan']
        receipt = joint.load(reference_receipt)
        rp = receipt['plan']
        if (self.execution.input_manifest_id != expected_input_id
                or ep['market_id'] != joint.EXECUTION_MARKET
                or rp != expected_reference_plan or rp['market_id'] != joint.REFERENCE_MARKET
                or rp['execution_parent_input_manifest_id'] != expected_input_id
                or ObservationProfile(**rp['observation_profile']) != ObservationProfile(
                    **joint.reference_profile(models.training_plan))
                or (rp['start_ns'], rp['end_ns']) != (ep['start_ns'], ep['end_ns'])
                or joint.training.sha(reference_file) != receipt['sha256']
                or Path(reference_file).stat().st_size != receipt['size']):
            raise ValueError('composite execution/reference binding differs')
        self.model = models
        self.reference = pd.read_parquet(reference_file)
        if len(self.reference) != receipt['rows']:
            raise ValueError('reference projection coverage differs')
        self.decisions = self.reference_context_missing = self.reference_values_missing = 0

    def compute_feature_frames(self, frames):
        frames = tuple(frames)
        if not frames:
            return []
        rows = []
        local_metadata = joint._feature_metadata(self.execution, FEATURE_COLUMNS)
        for frame in frames:
            row = model_row(frame, local_metadata, decision_ns=frame.cutoff_ns)
            for head in bundle.REMAINING:
                other = model_row(frame, self.model.metadata[head], decision_ns=frame.cutoff_ns)
                if not np.array_equal(row, other, equal_nan=True):
                    raise ValueError('execution feature schema differs across heads')
            rows.append(row)
        local = np.asarray(rows, dtype=float)
        plan = self.model.training_plan
        reference, report = joint.training.causal_reference_rows(self.reference,
            [f.cutoff_ns for f in frames], plan['reference_columns'],
            plan['reference_asof_max_age_ns'])
        raw = self.model.predict(local, reference.to_numpy(dtype=float))
        result = [prediction_from_raw({h: raw[h][i] for h in REQUIRED_MODEL_HEADS},
            local[i], frame.cutoff_ns) for i, frame in enumerate(frames)]
        self.decisions += len(frames)
        self.reference_context_missing += report['missing_context']
        self.reference_values_missing += int(reference.isna().any(axis=1).sum())
        return result

    def compute_signal(self, *, feature_frame, decision_ns):
        if decision_ns != feature_frame.cutoff_ns:
            raise ValueError('composite decision must use its ready feature cutoff')
        return self.compute_feature_frames((feature_frame,))[0]

    def report(self):
        return dict(arm=CANDIDATE, decisions=self.decisions,
            reference_context_missing=self.reference_context_missing,
            reference_values_missing=self.reference_values_missing,
            reference_market=joint.REFERENCE_MARKET,
            joint_heads=[joint.training.HEAD], retrained_execution_only_heads=list(bundle.REMAINING),
            original_B0_heads_used_at_inference=0,
            reference_observation_parity='simulated_not_measured')


def identity(spec):
    return joint.final.identity(spec, experiment=EXPERIMENT, arms=ARMS)


def parameters(original_parent):
    return joint.final.parameters(original_parent, CANDIDATE, arms=ARMS)


def validate_cell(directory, spec):
    return joint.final.validate_cell(directory, spec, experiment=EXPERIMENT, arms=ARMS)


def execute_cell(prepared, **kwargs):
    return joint.final.execute_cell(prepared, experiment=EXPERIMENT, arms=ARMS, **kwargs)
