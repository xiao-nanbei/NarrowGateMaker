from collections import Counter
from copy import deepcopy

import pytest

from research.families.f05_fill_quality_quote_ev.prefill_calendar import (
    DAY_NS, REVISION, account_specs, day_start, label_group, plan, sampling_identity, slots,
)
from research.families.f05_fill_quality_quote_ev.prefill_labels import OutcomeBlindSelection
from research.families.f05_fill_quality_quote_ev.risk_selection_training import train_chronological_ridge
from models.replay.risk_selection import PREFILL_LABEL_CONTRACT


def test_calendar_budget_and_f03_accounts():
    p = plan()
    assert {k: len(v) for k, v in p['splits'].items()} == dict(T=100, A=50, B=100, C=50, F=107)
    assert len(p['shards']) == 204
    assert len(p['train_parent_ids']) == 100
    assert len(p['diagnostic_parent_ids']) == 64
    assert len(p['unique_label_parent_ids']) == 132
    assert Counter(s[1] for s in slots()) == dict(T=2048, A=64, B=128, C=64)
    assert Counter(Counter(s[0] for s in slots() if s[1] == 'T' and s[2] == 'E:BUY').values()) == {5: 88, 6: 12}


def selector():
    spec = account_specs()[0]
    return OutcomeBlindSelection(per_surface=1, spacing_ns=30_000_000_000,
        end_ns=spec.end_ts_ms_exclusive*1_000_000, feature_columns=['x'],
        calendar_revision=REVISION, account_id=spec.shard_id)


def test_first_legal_slot_batch_invariance_and_day_boundary():
    start = day_start('2025-08-01')
    def row(offset, **extra):
        return dict(kind='E', side='BUY', order_id='', decision_ts_ns=start+offset,
                    feature_ready_ts_ns=start, features={'x': 1.}, **extra)
    rows = [row(1, future_pnl=999), row(31_000_000_000), row(DAY_NS//5),
            row(DAY_NS-30_000_000_000), row(DAY_NS)]
    original = deepcopy(rows)
    all_rows = selector().select(rows)
    s = selector()
    assert s.select(rows[:2])+s.select(rows[2:]) == all_rows
    assert [r['decision_ts_ns'] for r in all_rows] == [start+1, start+DAY_NS//5]
    assert rows == original
    restored = selector()
    restored.restore_selected(all_rows)
    assert restored.select(rows) == []
    broken = deepcopy(all_rows)
    broken[0]['sampling']['split'] = 'B'
    with pytest.raises(ValueError):
        selector().restore_selected(broken)


def make_label(slot, i):
    spec = next(s for s in account_specs() if slot[0] in s.calendar_days)
    ts = slot[4]+1_000_000_000
    kind, side = slot[2].split(':')
    return dict(opportunity_id=f'op-{i}', kind=kind, side=side, order_id='',
        value_scope=PREFILL_LABEL_CONTRACT, label_contract=PREFILL_LABEL_CONTRACT,
        horizon_ns=30_000_000_000, continuation_policy='frozen_B0',
        selection_scope='visible_inventory', fork={'phase': 'baseline_intent_before_order_budget'},
        additive_portfolio_return=False, baseline_action='POST', alternative_action='WAIT',
        replay_start_ts_ms=spec.start_ts_ms, terminal_mark_ts_ms=(ts+30_000_000_000)//1_000_000,
        decision_ts_ns=ts, feature_ready_ts_ns=ts, baseline_value_usdc=float(i),
        alternative_value_usdc=0., value_difference_usdc=float(i), matched_opportunity_prefix_count=1,
        features={'x': float(i % 7)}, sampling=sampling_identity(slot, spec.shard_id))


def test_training_uses_t_only_and_diagnostics_cannot_change_model():
    selected = [s for s in slots() if s[2] == 'E:BUY']
    rows = [make_label(s, i) for i, s in enumerate(selected)]
    kwargs = dict(feature_units={'x': '1'}, calendar_revision=REVISION,
                  min_train_rows=128, required_label_contract=PREFILL_LABEL_CONTRACT)
    policy, report = train_chronological_ridge(rows, **kwargs)
    assert report['train_rows'] == 512
    assert report['validation_rows'] == 64
    assert {k: v['rows'] for k, v in report['surfaces']['E:BUY']['diagnostic_groups'].items()} == dict(A=16, B=32, C=16)
    changed = deepcopy(rows)
    for row in changed:
        if label_group(row) != 'T':
            row['features']['x'] = -1e6
            row['baseline_value_usdc'] = row['value_difference_usdc'] = -999.
    assert train_chronological_ridge(changed, **kwargs)[0] == policy
    changed[0]['sampling']['day'] = '2026-09-01'
    with pytest.raises(ValueError):
        train_chronological_ridge(changed, **kwargs)
    duplicate = dict(rows[0], opportunity_id='another-id-in-same-slot')
    with pytest.raises(ValueError, match='duplicate calendar slot'):
        train_chronological_ridge(rows+[duplicate], **kwargs)


def test_calendar_rejects_wrong_account_end_and_future_features():
    with pytest.raises(ValueError, match='account/end'):
        OutcomeBlindSelection(per_surface=1, spacing_ns=30_000_000_000,
            end_ns=1, feature_columns=['x'], calendar_revision=REVISION,
            account_id=account_specs()[0].shard_id)
    ts = day_start('2025-08-01')
    row = dict(kind='E', side='BUY', order_id='', decision_ts_ns=ts,
               feature_ready_ts_ns=ts+1, features={'x': 1.})
    s = selector()
    assert not s.select([row])
    assert s.select([dict(row, feature_ready_ts_ns=ts)])
