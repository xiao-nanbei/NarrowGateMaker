import pytest

from research.families.f08_side_taker_lifecycle.response_action_training import train_pair, weights, day_ns
from research.families.f03_causal_13_head.time_weighted_evaluation import SPLITS
from strategy.response_action_features import FEATURES


def rows():
    return [dict(day=day, slot=0, decision_ns=day_ns(day)+1_000_000_000,
                 outcome_end_ns=day_ns(day)+31_000_000_000, valid=True, role='opener',
                 features=dict.fromkeys(FEATURES, 1.), delta_usdc=0.) for day in SPLITS['T']]


def test_same_rows_strong_ablation_zero_labels_tie_and_day_weights():
    records = rows()
    model = train_pair(records)
    assert all(m['alpha'] == 10. for m in model['models'].values())
    assert all(m['intercept'] == 0. for m in model['models'].values())
    w = weights(records+[dict(records[0], slot=1)])
    assert w[0]+w[-1] == pytest.approx(.01)
    assert w[-2] == 1.
    a = model['models']['RESPONSE_HISTORY_ABLATION']['features']
    assert 'pressure_x_depth_change' in a and 'order_recovery_age_s' not in a


def test_no_diagnostic_final_or_sparse_root_fit():
    data = rows()
    with pytest.raises(ValueError, match='support'):
        train_pair(data[:2])
    with pytest.raises(ValueError, match='T dates only'):
        train_pair([dict(data[0], day=SPLITS['F'][0])])
    with pytest.raises(ValueError, match='duplicate'):
        train_pair(data+data[:1])
    data[0]['outcome_end_ns'] += 1
    with pytest.raises(ValueError, match='boundary'):
        train_pair(data)
