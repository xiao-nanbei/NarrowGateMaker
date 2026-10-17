"""Support, clock and offline conversion contracts without purchased data."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from data.observation import EXECUTION_FEATURE_NAMES
from research.families.f04_external_market_alpha import full_support_direction as f


def reference():
    return pd.DataFrame(dict(cutoff_ns=[10, 20], max_dependency_ready_ns=[9, 20],
        spread_bps=[2., 3.], validity_mask=[[True]*len(EXECUTION_FEATURE_NAMES)]*2))


def test_asof_never_borrows_future_and_keeps_missing_rows():
    rows, report = f.causal_reference_rows(reference(), [9, 10, 19, 20, 31], ['spread_bps'], 10)
    assert np.allclose(rows.iloc[:, 0], [np.nan, 2, 2, 3, np.nan], equal_nan=True)
    assert report['missing_context'] == 2 and len(rows) == 5


def test_invalid_value_is_nan_not_zero():
    ref = reference()
    masks = deepcopy(ref.validity_mask.tolist())
    masks[1][EXECUTION_FEATURE_NAMES.index('spread_bps')] = False
    ref['validity_mask'] = masks
    rows, report = f.causal_reference_rows(ref, [20], ['spread_bps'], 10)
    assert np.isnan(rows.iloc[0, 0]) and report['missing_context'] == 0


@pytest.mark.parametrize('change', ['future', 'unordered', 'empty', 'bad_mask'])
def test_corrupt_reference_rejected(change):
    ref = reference()
    if change == 'future':
        ref.loc[0, 'max_dependency_ready_ns'] = 11
    if change == 'unordered':
        ref['cutoff_ns'] = [20, 10]
    if change == 'empty':
        ref = ref.iloc[:0]
    if change == 'bad_mask':
        ref['validity_mask'] = [[True], [True]]
    with pytest.raises(ValueError):
        f.causal_reference_rows(ref, [10, 20], ['spread_bps'], 10)


def test_original_conversion_explicit_and_value_preserving():
    meta = dict(migration_source=dict(target_name='dir_10s'), name=f.HEAD,
        representation_source=dict(index_values_changed=False, sample_weights_changed=False))
    original = pd.DataFrame(dict(label_dir_10s=[1., np.nan],
        label_outcome_end_dir_10s=pd.to_datetime(['2025-01-01', None], utc=True), sample_weight=[1., 1.]))
    converted = f.convert_original_direction(original, meta)
    pd.testing.assert_series_equal(converted[f.LABEL], original.label_dir_10s, check_names=False)
    pd.testing.assert_series_equal(converted[f.END], original.label_outcome_end_dir_10s, check_names=False)
    assert 'label_dir_10s' in original
    with pytest.raises(ValueError):
        f.convert_original_direction(converted, meta)
    meta['representation_source']['sample_weights_changed'] = True
    with pytest.raises(ValueError):
        f.convert_original_direction(original, meta)
