"""All-head equal weights and one-head reference input are separate contracts."""
from copy import deepcopy

import pytest

from research.families.f04_external_market_alpha import equal_label_joint_bundle as m


@pytest.mark.parametrize('head', m.ml.MODEL_SPECS)
def test_all_heads_disable_extra_weights_without_changing_other_parameters(head):
    old = dict(n_jobs=6, seed=42, n_estimators=2000, max_depth=8)
    if m.ml.MODEL_SPECS[head][3]:
        old['is_unbalance'] = True
    actual = m.parameters({'selection_fit_params': old}, head)
    expected = dict(old, n_jobs=2)
    if m.ml.MODEL_SPECS[head][3]:
        expected['is_unbalance'] = False
    assert actual == expected and old['n_jobs'] == 6


def test_no_duplicate_joint_fit_and_exact_other_twelve():
    assert len(m.REMAINING) == 12
    assert set(m.REMAINING) | {m.joint.HEAD} == set(m.ml.MODEL_SPECS)
    assert m.joint.HEAD not in m.REMAINING


def composite(tmp_path):
    payload = tmp_path/'artifact';payload.write_text('retained model or metadata')
    days = [str(x.date()) for x in m.pd.date_range('2025-08-01', periods=100)]
    return dict(schema=m.SCHEMA,candidate=m.CANDIDATE,training_days=days,
        heads={head:dict(model=m.bound(payload),metadata=m.bound(payload),
            feature_cols=list(range(41 if head==m.joint.HEAD else 29)),training_days=days,
            per_valid_label_weight=1.,date_normalization='none',age_decay='none',
            class_adjustment='none',is_unbalance=False) for head in m.ml.MODEL_SPECS})


def test_complete_composite_is_not_the_old_single_head_candidate(tmp_path):
    value=composite(tmp_path)
    assert m.validate_composite(value)==value
    value['candidate']='F03_TRAIN100_BTCUSDC_BTCUSDT_DIRECTION_EQUAL_LABEL_20261015'
    with pytest.raises(ValueError,match='identity'):m.validate_composite(value)


@pytest.mark.parametrize('change', [dict(per_valid_label_weight=2),
    dict(date_normalization='effective_head_rows'),dict(class_adjustment='balanced'),
    dict(is_unbalance=True),dict(age_decay='time'),dict(feature_cols=list(range(41)))])
def test_any_old_weighted_or_wrong_input_head_rejects_whole_bundle(tmp_path,change):
    value=composite(tmp_path);head=m.REMAINING[0]
    value['heads'][head].update(change)
    with pytest.raises(ValueError,match='scope'):m.validate_composite(value)


def test_missing_head_and_changed_payload_fail_closed(tmp_path):
    value=composite(tmp_path);bad=deepcopy(value);bad['heads'].pop(m.REMAINING[0])
    with pytest.raises(ValueError,match='thirteen'):m.validate_composite(bad)
    (tmp_path/'artifact').write_text('changed')
    with pytest.raises(ValueError,match='artifact'):m.validate_composite(value)
