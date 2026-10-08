"""Synthetic one-decision branches, not economic or live acceptance."""
from dataclasses import asdict, replace
import pickle

import pytest

from strategy.optional_update import OptionalUpdateFork, admission_reasons


def request(**changes):
    return OptionalUpdateFork(**(dict(decision_ns=2_000_000_000, decision_sequence=1,
        side='BUY', order_id='o', old_price=100., target_price=101., quantity=.001,
        action='KEEP_EXISTING', execution_end_ns=7_000_000_000) | changes))


@pytest.mark.parametrize('action,baseline,expected', [
    ('BASELINE', False, False), ('BASELINE', True, True),
    ('KEEP_EXISTING', True, False), ('UPDATE_TO_B0_TARGET', False, True)])
def test_action_changes_only_optional_price_choice(action, baseline, expected):
    r = request(action=action)
    assert r.select(old_price=100., target_price=101., quantity=.001,
                    baseline=baseline, reasons=[]) is expected
    assert r.matches(now_ms=2000, sequence=1, side='BUY', order_id='o')
    assert not r.matches(now_ms=2000, sequence=2, side='BUY', order_id='o')
    assert not r.matches(now_ms=2000, sequence=1, side='SELL', order_id='o')
    assert pickle.loads(pickle.dumps(r)) == r


@pytest.mark.parametrize('reason', ['pending', 'time', 'force', 'quantity_changed',
                                   'inventory', 'queue_keep', 'sub_tick'])
def test_no_override_of_other_conditions(reason):
    with pytest.raises(ValueError, match='not admissible'):
        request().select(old_price=100., target_price=101., quantity=.001,
                         baseline=True, reasons=[reason])


def test_identity_and_missing_fields_fail_closed():
    with pytest.raises(ValueError, match='differs'):
        request().select(old_price=100., target_price=101., quantity=.002,
                         baseline=True, reasons=[])
    for changes in ({'side': 'bid'}, {'decision_ns': 1}, {'quantity': float('nan')},
                    {'action': 'CANCEL'}, {'execution_end_ns': 6_000_000_000}):
        with pytest.raises(ValueError):
            request(**changes)
    reasons = admission_reasons(local_reasons=['time'], order_count=2, pending=True,
        inventory_allowed=False, forced_cancel=True, quantity=.001, lot_size=.001,
        capped_quantity=0., budget_allowed=False)
    assert reasons == ['time', 'multiple_or_absent_orders', 'pending', 'inventory',
                       'forced_cancel', 'notional_quantity_change', 'inventory_budget']


@pytest.fixture
def synthetic_parent(tmp_path):
    from data.facts import materialize
    from data.runtime import ObservationProfile, derive_inputs
    from models.backtest_tick import prepare_public_inputs
    from tests.test_research_public_inputs import public_replay_params, MARKET
    book, trade = tmp_path/'book.csv', tmp_path/'trade.csv'
    rows = ['exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount']
    for second in range(1, 21):
        # New full snapshot: no half-message or time-order repair.
        for side, price in [('bid', 100+second), ('ask', 102+second)]:
            rows.append(f'binance-futures,BTCUSDC,{second*1000000},{second*1000000},true,{side},{price},1')
    book.write_text('\n'.join(rows)+'\n')
    trade.write_text('exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n'
                     'binance-futures,BTCUSDC,1300000,1300000,1,buy,102,0.0001\n')
    materialize({'source_profile': 'tardis_only', 'files': [
        {'path': str(book), 'symbol': 'BTCUSDC', 'channel': 'incremental_book_L2'},
        {'path': str(trade), 'symbol': 'BTCUSDC', 'channel': 'trades'}]}, tmp_path/'facts')
    profile = ObservationProfile('synthetic', 'source_timestamp_proxy', 0, 0, 0,
                                  1_000_000_000, trade_coverage='observed')
    derive_inputs(dict(facts_root=str(tmp_path/'facts'), observation_profile=asdict(profile),
        start_ns=1_000_000_000, end_ns=21_000_000_000, market_id=MARKET,
        include_response_observations=True), tmp_path/'inputs')
    params = dict(public_replay_params(), response_observe_only=True,
                  response_update_branch_enabled=True, replace_min_price_change_ticks=15,
                  replace_min_interval_ms=125, replace_min_price_change_ticks_reducing=15,
                  replace_min_interval_ms_reducing=125)
    return prepare_public_inputs(tmp_path/'inputs', tick_size=.1), params


def test_real_consumer_checkpoint_branches_and_default_parity(synthetic_parent):
    from models.backtest_tick import simulate_prepared_inputs, simulate_optional_update_branch
    from tests.test_target_observation import exact
    prepared, params = synthetic_parent
    full = simulate_prepared_inputs(prepared, params)
    plain = simulate_prepared_inputs(prepared, dict(params, response_update_branch_enabled=False))
    observed = full.pop('response_update_observation')
    plain.pop('response_update_observation')
    exact(full, plain)
    roots = [r for r in observed['samples'] if r.get('optional_update_admission', {}).get('decision_admissible')
             and r['read_ns']+5_000_000_000 < 21_000_000_000]
    assert roots, observed['samples']
    row = roots[0]
    checkpoint = simulate_prepared_inputs(prepared, params,
        checkpoint_at_ts_ms=row['read_ns']//1_000_000)['_replay_checkpoint']
    r = OptionalUpdateFork(decision_ns=row['read_ns'], decision_sequence=row['call_sequence'],
        side=row['side'], order_id=row['order_id'], old_price=row['old_price'],
        target_price=row['target_price'], quantity=.001, action='BASELINE',
        execution_end_ns=row['read_ns']+5_000_000_000)
    first = simulate_optional_update_branch(prepared, params, checkpoint=checkpoint, request=r)
    second = simulate_optional_update_branch(prepared, params, checkpoint=checkpoint, request=r)
    exact(first, second)
    assert checkpoint['runtime'].response_update_branch_evidence is None
    for action, expected in [('KEEP_EXISTING', False), ('UPDATE_TO_B0_TARGET', True)]:
        result = simulate_optional_update_branch(prepared, params, checkpoint=checkpoint,
                                                request=replace(r, action=action))
        evidence = result['response_update_fork']
        assert evidence['selected_update'] is expected
        assert evidence['future_effective_ns'] is None
        assert result['execution_window']['exclusive_end_ns'] == r.execution_end_ns
        assert not result['economic_complete']
    with pytest.raises(ValueError, match='changed'):
        simulate_optional_update_branch(prepared, dict(params, replace_min_price_change_ticks=10),
                                        checkpoint=checkpoint, request=r)


def test_sampler_namespace_does_not_change_business_and_restores(synthetic_parent, monkeypatch):
    from models.backtest_tick import simulate_prepared_inputs
    from tests.test_target_observation import exact
    from research.families.f08_side_taker_lifecycle import response_sampling
    prepared, params = synthetic_parent
    slot = dict(day='1970-01-01', split='T', index=0, side='BUY', role='opener',
                baseline_action='KEEP', start_ns=1_000_000_000, end_ns=20_000_000_000,
                label_day_end_ns=200_000_000_000)
    monkeypatch.setattr(response_sampling, 'slots', lambda: (slot,))
    sampled = dict(params, response_action_sampling_slots=[slot], response_action_account_end_ns=200_000_000_000)
    baseline = simulate_prepared_inputs(prepared, params)
    full = simulate_prepared_inputs(prepared, sampled)
    checkpoint = simulate_prepared_inputs(prepared, sampled, checkpoint_at_ts_ms=10000)['_replay_checkpoint']
    restored = simulate_prepared_inputs(prepared, sampled, resume_checkpoint=checkpoint)
    exact(full, restored)
    full.pop('response_action_sampling')
    full.pop('response_update_observation')
    baseline.pop('response_update_observation')
    exact(full, baseline)


def test_parent_finalization_keeps_original_binding_after_disk_restore(synthetic_parent, tmp_path):
    from models.backtest_tick import simulate_prepared_inputs
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from research.families.f08_side_taker_lifecycle.response_action_labels import parent_step
    from tests.test_target_observation import exact
    prepared, params = synthetic_parent
    full = simulate_prepared_inputs(prepared, params)
    checkpoint = simulate_prepared_inputs(prepared, params, checkpoint_at_ts_ms=20000)['_replay_checkpoint']
    save_runtime_checkpoint(tmp_path/'parent.checkpoint', checkpoint)
    restored = load_trusted_runtime_checkpoint(tmp_path/'parent.checkpoint')
    with pytest.raises(ValueError, match='public checkpoint'):
        simulate_prepared_inputs(prepared, params, resume_checkpoint=restored, execution_end_ns=21_000_000_000)
    result = parent_step(simulate_prepared_inputs, prepared, params, None, restored, 21000, 21000)
    exact(full, result)
    assert restored['public_binding'] == checkpoint['public_binding']
