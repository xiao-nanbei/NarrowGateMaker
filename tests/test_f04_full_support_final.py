"""Joint-head admission and actual feature/prediction wiring, no real refit."""
from copy import deepcopy
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from data.observation import EXECUTION_FEATURE_NAMES
from research.families.f04_external_market_alpha import final_b0 as old
from research.families.f04_external_market_alpha import full_support_final as f


def plan():
    return dict(candidate=f.CANDIDATE, baseline=old.BASELINE,
        base_features=list(f.FEATURE_COLUMNS), reference_asof_max_age_ns=10,
        days=[{'reference_plan': {'observation_profile': {
            'profile_id': 'joint-reference-test', 'clock_policy': 'source_timestamp_proxy',
            'market_delay_ns': 250000000, 'processing_ns': 1000000,
            'allowed_lateness_ns': 100000000, 'max_book_age_ns': 1000000000}}}],
        reference_columns=['spread_bps', 'depth_imbalance_5', 'depth_imbalance_20',
            'clock_volume_imbalance_10s', 'observed_trade_count_10s', 'return_10s',
            'return_30s', 'return_60s', 'realized_vol_60s', 'book_age_s',
            'last_trade_age_s', 'trade_intensity_burst_guard'])


def cell():
    return dict(experiment_id=f.EXPERIMENT, baseline_id=old.BASELINE,
        parent_id='final-000-2026-05-28', arm_id=f.CANDIDATE, start_ns=old.START_NS,
        end_ns=old.START_NS+2*old.DAY_NS, input_manifest_id='a'*64,
        model_identity='b'*64, release_identity='c'*64,
        seed_pair={'rng_seed': 42, 'latency_seed': 59})


def write(path, value):
    path.write_text(json.dumps(value))
    return path


def test_old_and_new_cell_identities_are_disjoint():
    value = cell()
    assert f.identity(value) == value
    with pytest.raises(ValueError):
        old.identity(value)
    value.update(experiment_id=old.EXPERIMENT, arm_id='M1')
    assert old.identity(value) == value
    with pytest.raises(ValueError):
        f.identity(value)
    value = cell()
    value.update(parent_id='final-053-2026-09-11', start_ns=old.END_NS-old.DAY_NS, end_ns=old.END_NS)
    assert f.identity(value) == value


@pytest.mark.parametrize('key,value', [('arm_id', 'M1'), ('arm_id', 'B0'),
    ('experiment_id', old.EXPERIMENT), ('seed_pair', {'rng_seed': 42, 'latency_seed': 42}),
    ('end_ns', old.END_NS), ('input_manifest_id', 'unknown')])
def test_wrong_cell_refused(key, value):
    item = cell()
    item[key] = value
    with pytest.raises(ValueError):
        f.identity(item)


def test_full_effective_inheritance_is_same_control(monkeypatch):
    parent = dict(exit_urgency_strength=.5, nested={'retained': True})
    calls = []
    def inherit(value, arm):
        calls.append(arm)
        value = deepcopy(value)
        value['exit_urgency_strength'] = 0.
        return value
    monkeypatch.setattr(old, 'arm_parameters', inherit)
    assert f.parameters(parent) == old.parameters(parent, 'M1')
    assert calls == ['B0', 'B0'] and parent['exit_urgency_strength'] == .5


def test_economic_engine_receives_separate_contract(monkeypatch):
    seen = {}
    def execute(prepared, **kwargs):
        seen.update(kwargs)
        return prepared
    monkeypatch.setattr(old, 'execute_cell', execute)
    assert f.execute_cell('prepared', spec=cell()) == 'prepared'
    assert seen['experiment'] == f.EXPERIMENT and seen['arms'] == (f.CANDIDATE,)


def model_files(tmp_path):
    p = plan()
    plan_path = write(tmp_path/'plan.json', p)
    root = tmp_path/'model'
    root.mkdir()
    (root/(f.training.HEAD+'.txt')).write_text('synthetic loader fixture, not a trained research model')
    meta = dict(feature_cols=f.feature_columns(p), model_sha256=f.training.sha(root/(f.training.HEAD+'.txt')))
    write(root/'manifest.json', meta)
    admission = dict(status='TRAINING_MODEL_VERIFIED', candidate=f.CANDIDATE,
        plan_sha256=f.training.sha(plan_path), model_manifest_sha256=f.training.sha(root/'manifest.json'),
        feature_cols=f.feature_columns(p), model_sha256=meta['model_sha256'])
    ap = write(tmp_path/'admission.json', admission)
    return p, root, dict(plan_path=plan_path, admission_path=ap, expected_admission_sha256=f.training.sha(ap))


def test_loader_requires_one_bound_booster_and_feature_order(tmp_path, monkeypatch):
    p, root, kwargs = model_files(tmp_path)
    calls = []
    def predict(matrix, **kw):
        calls.append(kw)
        return np.full(len(matrix), .65)
    monkeypatch.setattr(f.lgb, 'Booster', lambda **kw: SimpleNamespace(
        feature_name=lambda: f.feature_columns(p), predict=predict))
    model = f.DirectionModel(root, **kwargs)
    assert model.arm == f.CANDIDATE and len(model.feature_cols) == 41
    assert model.predict(np.full((2, 41), np.nan)).tolist() == [.65, .65]
    assert calls == [{'num_threads': 1}]
    with pytest.raises(ValueError):
        model.predict(np.full((1, 41), np.inf))
    with pytest.raises(ValueError):
        model.predict(np.zeros((1, 29)))
    monkeypatch.setattr(f.lgb, 'Booster', lambda **kw: SimpleNamespace(feature_name=lambda: list(reversed(f.feature_columns(p)))))
    with pytest.raises(ValueError, match='order'):
        f.DirectionModel(root, **kwargs)


@pytest.mark.parametrize('changed', ['plan', 'admission', 'manifest', 'weights'])
def test_changed_model_bytes_refused_before_loading(tmp_path, monkeypatch, changed):
    _, root, kwargs = model_files(tmp_path)
    paths = dict(plan=kwargs['plan_path'], admission=kwargs['admission_path'],
        manifest=root/'manifest.json', weights=root/(f.training.HEAD+'.txt'))
    target = paths[changed]
    target.write_text(target.read_text()+'\n')
    def forbidden(**kw):
        pytest.fail('unadmitted model reached LightGBM')
    monkeypatch.setattr(f.lgb, 'Booster', forbidden)
    with pytest.raises(ValueError, match='unadmitted'):
        f.DirectionModel(root, **kwargs)


def signal_fixture(tmp_path, monkeypatch):
    p = plan()
    ep = dict(market_id=f.EXECUTION_MARKET, start_ns=0, end_ns=100)
    rp = dict(market_id=f.REFERENCE_MARKET, start_ns=0, end_ns=100, execution_parent_input_manifest_id='a'*64,
        observation_profile=f.reference_profile(p))
    reference = pd.DataFrame(dict(cutoff_ns=[10, 20], max_dependency_ready_ns=[9, 20],
        validity_mask=[[True]*len(EXECUTION_FEATURE_NAMES)]*2,
        **{x: [2., 3.] for x in p['reference_columns']}))
    path = tmp_path/'features.parquet'
    reference.to_parquet(path)
    receipt = write(tmp_path/'receipt.json', dict(plan=rp, sha256=f.training.sha(path), size=path.stat().st_size, rows=2))
    base_path = tmp_path/'base'
    base_path.mkdir()
    write(base_path/'public_input_model.json', {'fixture': True})
    outputs = []
    def base_compute(frames):
        result = [SimpleNamespace(touch_conditioned_up_probability_10000ms=.1,
            absolute_price_variance_rate_10000ms=7., other=('original', frame.cutoff_ns)) for frame in frames]
        outputs.extend(deepcopy(result))
        return result
    from strategy.signal import SignalEngine
    monkeypatch.setattr(SignalEngine, 'from_public_models', lambda *a, **kw: SimpleNamespace(compute_feature_frames=base_compute))
    monkeypatch.setattr(f, 'ConsumerBundle', lambda _: SimpleNamespace(
        input_manifest_id='a'*64, manifest=dict(plan=ep, input_contract_id='input', observation_contract_id='observation', feature_contract_id='features')))
    monkeypatch.setattr(f, 'model_row', lambda *a, **kw: [1.]*29)
    seen = []
    model = SimpleNamespace(training_plan=p, predict=lambda matrix: (seen.append(matrix.copy()) or np.full(len(matrix), .7)))
    engine = f.JointDirectionSignalEngine('execution', path, receipt, base_path, model,
        expected_input_id='a'*64, expected_reference_plan=rp,
        expected_base_manifest_sha256=f.training.sha(base_path/'public_input_model.json'))
    return engine, seen, outputs


def test_joint_consumer_preserves_other_heads_and_causal_missingness(tmp_path, monkeypatch):
    engine, seen, originals = signal_fixture(tmp_path, monkeypatch)
    frames = [SimpleNamespace(cutoff_ns=t) for t in [9, 10, 19, 20, 31]]
    result = engine.compute_feature_frames(frames)
    assert len(seen) == 1 and seen[0].shape == (5, 41)
    assert (seen[0][:, :29] == 1).all()
    assert np.isnan(seen[0][[0, 4], 29:]).all()
    assert (seen[0][1:3, 29:] == 2).all() and (seen[0][3, 29:] == 3).all()
    for actual, old_output in zip(result, originals, strict=True):
        assert actual.touch_conditioned_up_probability_10000ms == .7
        assert actual.absolute_price_variance_rate_10000ms == old_output.absolute_price_variance_rate_10000ms
        assert actual.other == old_output.other
    assert engine.report()['reference_context_missing'] == 2
    assert engine.report()['decisions'] == 5
    with pytest.raises(ValueError):
        engine.compute_signal(feature_frame=frames[-1], decision_ns=32)


def test_unfinished_training_cannot_be_admitted(tmp_path):
    p = plan()
    p.update(baseline_metadata={'train_only_selection': {'refit_days': []}})
    p['days'][0]['day'] = '2025-08-01'
    write(tmp_path/'plan.json', p)
    write(tmp_path/'progress.json', dict(phase='REFERENCE_FEATURE_PREPARATION', completed_days=[]))
    with pytest.raises(ValueError, match='not complete'):
        f.admit_training(tmp_path)


def test_different_reference_clock_is_not_a_reusable_training_profile():
    p = plan()
    changed = deepcopy(p['days'][0])
    changed['reference_plan']['observation_profile']['depth_connection_id'] = 'depth:epoch0'
    changed['reference_plan']['observation_profile']['trade_connection_id'] = 'trade:epoch0'
    p['days'].append(changed)
    with pytest.raises(ValueError, match='profiles differ'):
        f.reference_profile(p)


def test_reference_plan_uses_training_clock_not_execution_clock(monkeypatch):
    p = plan()
    def inherited(*a, **kw):
        return dict(start_ns=10, end_ns=100, execution_parent_input_manifest_id='a'*64,
            observation_profile={'different': 'old execution clock'})
    monkeypatch.setattr(old, 'reference_plan', inherited)
    actual = f.reference_plan('execution', 'facts', p, contract_sha='c'*64, parent_id='parent')
    assert actual['observation_profile'] == f.reference_profile(p)
    assert actual['start_ns'] == 10 and actual['execution_parent_input_manifest_id'] == 'a'*64
