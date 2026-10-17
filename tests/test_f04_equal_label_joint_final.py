"""Mixed-schema inference is not a partial replacement of old B0 heads."""
from dataclasses import asdict
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from data.observation import EXECUTION_FEATURE_NAMES
from research.families.f04_external_market_alpha import equal_label_joint_final as m
from strategy.signal import SignalEngine


class Booster:
    def __init__(self, value):
        self.value, self.matrices = value, []

    def predict(self, matrix, **kwargs):
        assert kwargs == {'num_threads': 1}
        self.matrices.append(matrix.copy())
        return np.full(len(matrix), self.value)


def models():
    value = object.__new__(m.CompositeModels)
    value.models = {h: Booster((i + 1) / 20) for i, h in enumerate(m.REQUIRED_MODEL_HEADS)}
    value.metadata = {h: {} for h in m.bundle.REMAINING}
    return value


def test_all_thirteen_models_used_and_only_direction_has_reference_columns():
    value = models()
    local, reference = np.ones((3, 29)), np.full((3, 12), np.nan)
    result = value.predict(local, reference)
    assert set(result) == set(m.REQUIRED_MODEL_HEADS)
    for head, booster in value.models.items():
        expected = 41 if head == m.joint.training.HEAD else 29
        assert booster.matrices[0].shape == (3, expected)
        assert (booster.matrices[0][:, :29] == 1).all()
        assert np.all(result[head] == booster.value)
        if expected == 41:
            assert np.isnan(booster.matrices[0][:, 29:]).all()


def test_explicit_runtime_nan_policy_preserves_training_metadata():
    original = {'feature_cols': list(m.FEATURE_COLUMNS)}
    flags = '[use_missing: 1]\n[zero_as_missing: 0]'
    assert m.runtime_metadata(original, flags, 'native_nan') == dict(original, missing_policy='native_nan')
    assert 'missing_policy' not in original
    for text, policy in [(flags, 'zero'), ('[use_missing: 0]', 'native_nan'),
                         ('[use_missing: 1]\n[zero_as_missing: 1]', 'native_nan')]:
        with pytest.raises(ValueError, match='missing-value'):
            m.runtime_metadata(original, text, policy)


def test_original_b0_other_twelve_heads_cannot_be_substituted():
    head = m.bundle.REMAINING[0]
    item = {'source': 'new_fit_and_full100_refit'}
    m.validate_head_origin(head, item, {'training_experiment_id': m.CANDIDATE})
    with pytest.raises(ValueError, match='old B0'):
        m.validate_head_origin(head, item, {'training_experiment_id': 'B0'})


def test_old_weighted_joint_direction_cannot_be_substituted():
    item = {'source': 'accepted_joint_model_reused_without_refit', 'model': {'sha256': 'a'}}
    meta = dict(schema='narrowgate.f04.valid_label_equal_direction.v1', model_sha256='a',
        weighting=dict(per_valid_label=1., date_normalization='none', age_decay='none',
            class_adjustment='none', is_unbalance=False))
    m.validate_head_origin(m.joint.training.HEAD, item, meta)
    meta['weighting']['is_unbalance'] = True
    with pytest.raises(ValueError, match='equal-label'):
        m.validate_head_origin(m.joint.training.HEAD, item, meta)


@pytest.mark.parametrize('bad', ['head', 'local', 'reference', 'inf', 'output'])
def test_missing_head_wrong_shape_or_invalid_output_fails(bad):
    value = models()
    local, reference = np.ones((3, 29)), np.ones((3, 12))
    if bad == 'head':
        value.models.pop(m.REQUIRED_MODEL_HEADS[-1])
    elif bad == 'local':
        local = local[:, :-1]
    elif bad == 'reference':
        reference = reference[:, :-1]
    elif bad == 'inf':
        reference[0, 0] = np.inf
    else:
        value.models[m.REQUIRED_MODEL_HEADS[0]].value = np.nan
    with pytest.raises(ValueError):
        value.predict(local, reference)


def test_raw_mapping_matches_original_no_demeaning_consumer_every_field():
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    engine._enable_ml = True
    engine._models = models().models
    engine._model_feature_schema = tuple(m.FEATURE_COLUMNS)
    row = np.linspace(-1, 1, 29)
    row[5] = np.nan
    raw = {h: (i - 5) / 3 for i, h in enumerate(m.REQUIRED_MODEL_HEADS)}
    old = engine._predict(dict(zip(m.FEATURE_COLUMNS, row, strict=True)), raw_predictions=raw)
    old.ts, old.features = 17., row
    new = m.prediction_from_raw(raw, row, 17_000_000_000)
    for field, expected in asdict(old).items():
        actual = asdict(new)[field]
        if isinstance(expected, np.ndarray):
            assert np.array_equal(actual, expected, equal_nan=True)
        elif isinstance(expected, dict):
            assert actual.keys() == expected.keys()
            assert np.array_equal(list(actual.values()), list(expected.values()), equal_nan=True)
        else:
            assert actual == expected


def test_causal_reference_batch_row_parity_and_all_new_heads(tmp_path, monkeypatch):
    plan = dict(reference_asof_max_age_ns=10, reference_columns=[
        'spread_bps', 'depth_imbalance_5', 'depth_imbalance_20',
        'clock_volume_imbalance_10s', 'observed_trade_count_10s', 'return_10s',
        'return_30s', 'return_60s', 'realized_vol_60s', 'book_age_s',
        'last_trade_age_s', 'trade_intensity_burst_guard'])
    reference = pd.DataFrame(dict(cutoff_ns=[10, 20], max_dependency_ready_ns=[9, 20],
        validity_mask=[[True] * len(EXECUTION_FEATURE_NAMES)] * 2,
        **{x: [2., 3.] for x in plan['reference_columns']}))
    def engine():
        e = object.__new__(m.CompositeSignalEngine)
        e.model, e.reference = models(), reference
        e.model.training_plan = plan
        e.execution = object()
        e.decisions = e.reference_context_missing = e.reference_values_missing = 0
        return e
    monkeypatch.setattr(m.joint, '_feature_metadata', lambda *a: {})
    monkeypatch.setattr(m, 'model_row', lambda *a, **k: [1.] * 29)
    frames = [SimpleNamespace(cutoff_ns=t) for t in [9, 10, 19, 20, 31]]
    e = engine()
    actual = e.compute_feature_frames(frames)
    other = engine()
    expected = [other.compute_signal(feature_frame=f, decision_ns=f.cutoff_ns) for f in frames]
    for a, b in zip(actual, expected, strict=True):
        assert a.ts == b.ts and np.array_equal(a.features, b.features)
        for head in m.REQUIRED_MODEL_HEADS:
            assert getattr(a, head) == getattr(b, head) == e.model.models[head].value
    matrix = e.model.models[m.joint.training.HEAD].matrices[0]
    assert np.isnan(matrix[[0, 4], 29:]).all()
    assert (matrix[1:3, 29:] == 2).all() and (matrix[3, 29:] == 3).all()
    assert e.report()['reference_context_missing'] == 2
    assert e.report()['original_B0_heads_used_at_inference'] == 0
    with pytest.raises(ValueError, match='cutoff'):
        e.compute_signal(feature_frame=frames[0], decision_ns=11)


def test_independent_identity_rejects_previous_single_joint_arm():
    spec = dict(experiment_id=m.EXPERIMENT, baseline_id=m.joint.final.BASELINE,
        parent_id='final-000-2026-05-28', arm_id=m.CANDIDATE,
        start_ns=m.joint.final.START_NS, end_ns=m.joint.final.START_NS + 2*m.joint.final.DAY_NS,
        input_manifest_id='a'*64, model_identity='b'*64, release_identity='c'*64,
        seed_pair={'rng_seed': 42, 'latency_seed': 59})
    assert m.identity(spec) == spec
    spec['arm_id'] = m.joint.CANDIDATE
    with pytest.raises(ValueError):
        m.identity(spec)


def test_exact_artifact_map_preserves_original_reference_and_rejects_fallback(tmp_path):
    import hashlib
    path = tmp_path / 'model.txt'
    path.write_bytes(b'exact retained model')
    original = dict(path='/original/private/model.txt', size=path.stat().st_size,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    entry = dict(relative_path=path.name, size=original['size'], sha256=original['sha256'])
    resolver = m.ExactArtifactMap(tmp_path, {original['path']: entry})
    assert m._bound(original, resolver) == path
    assert original['path'] == '/original/private/model.txt'
    for bad in (dict(original, path=str(path)), dict(original, size=1),
                dict(original, sha256='0' * 64)):
        with pytest.raises(ValueError, match='unmapped or changed'):
            m._bound(bad, resolver)
    path.write_bytes(b'wrong retained model')
    with pytest.raises(ValueError, match='changed composite dependency'):
        m._bound(original, resolver)


@pytest.mark.parametrize('relative', ['../outside', '/absolute/path'])
def test_exact_artifact_map_rejects_escaped_paths(tmp_path, relative):
    item = dict(path='/original/model', sha256='a' * 64, size=1)
    resolver = m.ExactArtifactMap(tmp_path, {item['path']: dict(
        relative_path=relative, sha256=item['sha256'], size=1)})
    with pytest.raises(ValueError, match='escapes'):
        resolver(item)


def test_exact_artifact_map_rejects_symlink_and_missing_files(tmp_path):
    item = dict(path='/original/model', sha256='a' * 64, size=1)
    resolver = m.ExactArtifactMap(tmp_path, {item['path']: dict(
        relative_path='missing', sha256=item['sha256'], size=1)})
    with pytest.raises(ValueError, match='unsafe or missing'):
        resolver(item)
    (tmp_path / 'target').write_bytes(b'x')
    (tmp_path / 'missing').symlink_to(tmp_path / 'target')
    with pytest.raises(ValueError, match='unsafe or missing'):
        resolver(item)
