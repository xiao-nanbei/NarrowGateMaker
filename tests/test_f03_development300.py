from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from research.families.f03_causal_13_head import development300 as d
from research.families.f03_causal_13_head import ml_model as m


def test_calendar_stops_before_final():
    assert len(d.DAYS) == 300
    assert d.DAYS[0] == '2025-08-01'
    assert d.DAYS[-1] == '2026-05-27'
    assert len(d.DAYS[:239]) == 239
    assert len(d.DAYS[239:240]) == 1
    assert len(d.DAYS[240:]) == 60


def test_causal_context_and_final_rejection():
    plan = {'observation_profile': {}}
    first = d.day_plan(plan, {'day': d.DAYS[0], 'source_bundles': []})
    second = d.day_plan(plan, {'day': d.DAYS[1], 'source_bundles': []})
    assert first['end_ns'] - first['start_ns'] == 86400 * 10**9
    assert second['end_ns'] - second['start_ns'] == 86520 * 10**9
    with pytest.raises(ValueError, match='Final'):
        d.day_plan(plan, {'day': '2026-05-28', 'source_bundles': []})


def test_daily_purge_rejects_boundary_and_unknown_but_preserves_endpoints():
    day = d.DAYS[-1]
    index = pd.date_range(day, periods=3, freq='10s', tz='UTC')
    frame = pd.DataFrame(index=index)
    ends = pd.to_datetime([day + 'T00:00:02Z', '2026-05-28T00:00:00Z', None])
    for head, spec in m.MODEL_SPECS.items():
        frame[spec[0]] = [1., 1., 1.]
        frame['label_outcome_end_' + head] = ends
    result = d.purge_day(frame, day)
    for head, spec in m.MODEL_SPECS.items():
        assert result[spec[0]].iloc[0] == 1
        assert result[spec[0]].iloc[1:].isna().all()
        pd.testing.assert_series_equal(result['label_outcome_end_' + head], frame['label_outcome_end_' + head])
    assert frame[m.MODEL_SPECS[next(iter(m.MODEL_SPECS))][0]].notna().all()


def test_explicit_original_rename_does_not_change_values():
    meta = {'new': {'migration_source': {'target_name': 'old'}, 'representation_source':
                   {'index_values_changed': False, 'sample_weights_changed': False}}}
    frame = pd.DataFrame({'label_old': [0., np.nan], 'label_outcome_end_old': [1, 2], 'sample_weight': [1., 1.]})
    got = d.convert_original_labels(frame, meta)
    pd.testing.assert_series_equal(got.label_new, frame.label_old, check_names=False)
    assert got.index.equals(frame.index)
    with pytest.raises(ValueError, match='exactly once'):
        d.convert_original_labels(got, meta)
    bad = deepcopy(meta)
    bad['new']['representation_source']['sample_weights_changed'] = True
    with pytest.raises(ValueError, match='representation'):
        d.convert_original_labels(frame, bad)


def test_inherited_weights_are_per_day_not_per_row_or_age():
    head = next(iter(m.MODEL_SPECS))
    index = pd.to_datetime(['2025-08-01T00:00:00Z', '2025-08-01T00:00:10Z', '2025-08-02T00:00:00Z'])
    frame = pd.DataFrame({'mid': [1., 2., 3.], 'sample_weight': [1., 1., 1.],
        m.MODEL_SPECS[head][0]: [0., 1., 1.], 'label_outcome_end_' + head: index + pd.Timedelta(seconds=1)}, index=index)
    policy = {'schema_version': 'narrowgate.f03.time_half_life_daily.v2', 'decision_clock': 'feature_ready_index',
        'half_life_days': 'inf', 'reference_date': '2026-05-27',
        'previous_time_weight': {'reference_date': '2026-05-27', 'lambda': 0},
        'outcome_end_columns': {head: 'label_outcome_end_' + head}}
    contract = m.TrainOnlySelectionContract('test', '', '', '', d.DAYS[:2], (), d.DAYS[:2], d.DAYS[:2],
        '', '', '', '', policy, ('mid',))
    _, _, weights, _, report = m.prepare_time_weighted_xy(frame, head, contract, phase='fit')
    np.testing.assert_array_equal(weights, [.75, .75, 1.5])
    assert report['daily'][0]['weight_sum'] == report['daily'][1]['weight_sum']
    assert m.prepare_time_weighted_xy(frame, head, contract, phase='selection')[2] is None
    with pytest.raises(ValueError, match='undeclared'):
        m.prepare_time_weighted_xy(frame, head, replace(contract, fit_days=d.DAYS[:1]), phase='fit')


def test_plan_rejects_scope_changes_before_file_access():
    with pytest.raises(ValueError, match='contract mismatch'):
        d.validate_plan({'schema': 'narrowgate.f03.development300.v1', 'training_days': list(d.DAYS) + ['2026-05-28']})


def test_explicit_panel_identity_uses_real_fitter_without_original_panel_lookup(monkeypatch):
    def forbidden():
        raise AssertionError('original 100-day identity must not be consulted')
    monkeypatch.setattr(m, '_feature_panel_identity', forbidden)
    identity = dict(feature_timestamp_semantics='feature_ready_index',
                    feature_cutoff_semantics='feature_ready_index',
                    public_input_contract={'decision_time_semantics': 'feature_ready_index'},
                    split_manifest_id=d.SPLIT_ID)
    head = 'touch_conditioned_price_change_fraction_10000ms'
    frame = pd.DataFrame({'close': np.arange(80, dtype=float),
                          m.MODEL_SPECS[head][0]: np.arange(80, dtype=float) / 1000})
    model, meta = m.train_one(head, frame.iloc[:60], frame.iloc[60:],
        params_override={'n_estimators': 2, 'num_threads': 1, 'verbosity': -1,
                         'min_child_samples': 2}, feature_identity=identity)
    assert model.booster_.num_feature() == 1
    assert meta['split_manifest_id'] == d.SPLIT_ID
    assert meta['feature_timestamp_semantics'] == 'feature_ready_index'


def test_bundle_publisher_receives_metadata_mapping_not_training_dataclass():
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(d.fit_bundle))
    call = next(node for node in ast.walk(tree) if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name) and node.func.id == 'publish_model_contract')
    value = next(item.value for item in call.keywords if item.arg == 'selection_contract')
    assert ast.dump(value) == ast.dump(ast.parse('contract.to_metadata()', mode='eval').body)
    from strategy.public_model_contract import training_selection_contract
    contract = m.TrainOnlySelectionContract('test', '', '', '', d.DAYS[:239], d.DAYS[239:240],
        d.DAYS[240:], d.DAYS, '', '', '', '', {'half_life_days':'inf'}, ('mid',))
    assert training_selection_contract(contract.to_metadata())['refit_days'] == list(d.DAYS)
