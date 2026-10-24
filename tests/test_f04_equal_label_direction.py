"""Preserve joint-panel support while removing both kinds of label reweighting."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from research.families.f04_external_market_alpha import equal_label_direction as m


def test_parameters_keep_baseline_except_authorized_weights_and_resource_threads():
    old = dict(objective='binary', metric='auc', is_unbalance=True, n_jobs=6, seed=42,
               max_depth=8, learning_rate=.05, n_estimators=2000)
    assert m.parameters(old) == dict(old, is_unbalance=False, n_jobs=2)
    assert old['is_unbalance'] is True and old['n_jobs'] == 6


@pytest.mark.parametrize('key', ['class_weight', 'unbalance', 'unbalanced_sets', 'scale_pos_weight'])
def test_hidden_class_adjustment_rejected(key):
    with pytest.raises(ValueError, match='alias'):
        m.parameters(dict(objective='binary', metric='auc', **{key: 2}))


def fake_phases():
    index = pd.DatetimeIndex([pd.Timestamp('2025-08-01', tz='UTC')] +
        list(pd.date_range('2025-08-02', periods=100, freq='10s', tz='UTC')))
    x = pd.DataFrame({'mid': np.arange(101.)}, index=index)
    y = pd.Series(np.arange(101) % 2, index=index, dtype=float)
    weights = pd.Series([50.5] + [.505]*100, index=index)
    return {phase: (x.copy(), y.copy(), None if phase == 'selection' else weights.copy(), ['mid'], {'old': True})
            for phase in ('fit', 'selection', 'refit')}


def test_sparse_day_no_amplification_and_no_mask_change(monkeypatch):
    old = fake_phases()
    monkeypatch.setattr(m.base, 'prepared_phases', lambda *a: old)
    actual = m.uniform_phases(None, {}, ['mid'])
    for phase, (x, y, w, cols, report) in actual.items():
        assert x is old[phase][0] and y is old[phase][1] and cols == ['mid']
        assert [d['weight_sum'] for d in report['daily']] == [1., 100.]
        assert w is None if phase == 'selection' else (w == 1).all()
    assert old['fit'][2].iloc[0] == 50.5


def test_real_tiny_fit_serialized_predictions_and_duplicate_refusal(monkeypatch, tmp_path):
    monkeypatch.setattr(m.base, 'prepared_phases', lambda *a: fake_phases())
    meta = {'selection_fit_params': dict(objective='binary', metric='auc', is_unbalance=True,
        n_jobs=6, n_estimators=3, min_child_samples=2, verbosity=-1, seed=42)}
    out = tmp_path/'model'
    result = m.fit_direction(None, meta, ['mid'], out, provenance={'test': True}, threads=1)
    assert result['selection_fit_params']['is_unbalance'] is False
    assert result['weighting']['per_valid_label'] == 1
    assert all(p['exact_reload'] for p in result['saved_model_prediction_checks'].values())
    with pytest.raises(FileExistsError):
        m.fit_direction(None, meta, ['mid'], out, provenance={})


def complete_predecessor():
    plan = {'parents': [{'parent_id': str(i)} for i in range(54)]}
    accepted, state = {'parents': {}}, {'tasks': {}, 'attempts': {}}
    for row in plan['parents']:
        p = row['parent_id']
        state['tasks'][p] = dict(status='COMPLETE', attempt_id=p,
            spec=dict(project=m.PREDECESSOR_PROJECT, parameters=dict(cell=dict(parent_id=p))))
        state['attempts'][p] = dict(status='COMPLETE', spec_hash='hash-'+p)
        accepted['parents'][p] = dict(attempt=p, spec_hash='hash-'+p, process_exited=True,
            all_receipt_payload_sha_and_bytes_verified=True,
            full_endpoint_accounting_daily_ledger_verified=True)
    return plan, accepted, state


def test_gate_requires_all_original_accounts_and_acceptance_not_only_scheduler():
    plan, accepted, state = complete_predecessor()
    assert m.predecessor_gate(plan, accepted, state)['economic_dispatch_allowed']
    for field in ('process_exited', 'all_receipt_payload_sha_and_bytes_verified',
                  'full_endpoint_accounting_daily_ledger_verified'):
        bad = deepcopy(accepted);bad['parents']['0'][field] = False
        assert not m.predecessor_gate(plan, bad, state)['economic_dispatch_allowed']
    del accepted['parents']['0']
    assert not m.predecessor_gate(plan, accepted, state)['economic_dispatch_allowed']


@pytest.mark.parametrize('status', ['RUNNING', 'PENDING', 'UNKNOWN', 'FAILED', 'RESULT_PENDING'])
def test_gate_waits_for_current_candidate_even_if_new_candidate_priority_lower(status):
    plan, accepted, state = complete_predecessor()
    state['tasks']['0']['status'] = status
    assert m.predecessor_gate(plan, accepted, state)['waiting'] == ['0']
