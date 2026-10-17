from collections import Counter
import pickle

import pytest

from research.families.f08_side_taker_lifecycle.response_sampling import slots, OptionalUpdateSampler
from strategy.response_action_features import BASE, CONTEXT, FEATURES, ABLATION


def test_fixed_quota_no_final_and_balanced_surfaces():
    rows = slots()
    assert Counter(r['split'] for r in rows) == dict(T=4096, A=64, B=128, C=64)
    assert max(r['day'] for r in rows) < '2026-05-28'
    for group in ('T', 'A', 'B', 'C'):
        counts = Counter((r['side'], r['role'], r['baseline_action']) for r in rows if r['split'] == group)
        assert max(counts.values())-min(counts.values()) <= 1


def test_first_eligible_preserves_missing_and_checkpoint_identity():
    s = slots()[0]
    sampler = OptionalUpdateSampler([s], s['label_day_end_ns'])
    row = dict(read_ns=s['start_ns']+1_000_000_000, side=s['side'], role=s['role'],
               baseline_price_gate_action=s['baseline_action'], micro_available=True,
               features=dict.fromkeys(BASE, 0.) | {'order_price_delta_qty': None},
               action_context=dict.fromkeys(CONTEXT, 0.),
               optional_update_admission={'decision_admissible': True})
    sampler.consider(row)
    assert not sampler.selected
    row['features']['order_price_delta_qty'] = 0.
    sampler.consider(row)
    clone = pickle.loads(pickle.dumps(sampler))
    clone.consider(dict(row, read_ns=row['read_ns']+1))
    assert clone.selected == sampler.selected
    assert clone.eligible != sampler.eligible
    assert len(FEATURES) <= 64
    assert 'pressure_x_depth_change' in ABLATION
    assert clone.selected[next(iter(clone.selected))]['model_features']['recovery_age_s_missing'] == 1.
    with pytest.raises(ValueError):
        OptionalUpdateSampler([dict(s, side='unknown')], s['label_day_end_ns'])


def test_index_preserves_duplicate_order_and_history_errors(monkeypatch):
    import research.families.f08_side_taker_lifecycle.response_sampling as module
    from strategy.response_action_features import HISTORY
    first = dict(slots()[0])
    second = dict(first, index=999)
    monkeypatch.setattr(module, 'slots', lambda: (first, second))
    sampler = OptionalUpdateSampler([second, first, second], first['label_day_end_ns'])
    row = dict(read_ns=first['start_ns']+1, side=first['side'], role=first['role'],
        baseline_price_gate_action=first['baseline_action'], micro_available=True,
        features=dict.fromkeys(BASE, 0.), action_context=dict.fromkeys(CONTEXT, 0.),
        optional_update_admission={'decision_admissible': True})
    sampler.consider(row)
    assert list(sampler.selected) == [f"{first['day']}:999", f"{first['day']}:{first['index']}"]
    assert list(sampler.eligible.values()) == [2, 1]
    row['read_ns'] = first['end_ns']
    row['features'][HISTORY[0]] = float('inf')
    with pytest.raises(ValueError, match='nonfinite'):
        sampler.consider(row)  # Even outside every slot: do not skip validation.
