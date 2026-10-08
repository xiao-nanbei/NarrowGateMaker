"""Synthetic shared-producer export; no market records or strategy results."""
from dataclasses import asdict
import pytest

from data.observation import TradeContribution
from data.runtime import ConsumerBundle, derive_inputs
from tests import test_research_public_inputs as fixtures

bundle = fixtures.bundle


def test_export_preserves_old_tables_clocks_and_exact_payloads(bundle, tmp_path):
    original = ConsumerBundle(bundle)
    output = tmp_path / 'with_observations'
    derive_inputs({**original.manifest['plan'], 'include_response_observations': True}, output)
    recorded = ConsumerBundle(output)
    for name in ('bars', 'features', 'depth'):
        before = original.table(name)
        assert recorded.table(name).select(before.column_names).equals(before)
    assert recorded.manifest['stats'] == original.manifest['stats']
    trades = iter(recorded.table('trade_observations').to_pylist())
    books = iter(recorded.table('depth').to_pylist())
    count = 0
    for tick in original.stream():
        for observation in tick.observations:
            count += 1
            row = next(trades if isinstance(observation.payload, TradeContribution) else books)
            assert row['observation_sequence'] == count
            for name in ('source_asof_ns', 'publish_ns', 'receive_ns', 'ready_ns', 'event_id'):
                assert row[name] == getattr(observation, name)
            if isinstance(observation.payload, TradeContribution):
                for name, value in asdict(observation.payload).items():
                    assert row[name] == (str(value) if name in ('price', 'quantity') else value)
            else:
                for side in ('bid', 'ask'):
                    levels = getattr(observation.payload, side + 's')
                    assert row[side + '_px_exact'] == [str(p) for p, _ in levels]
                    assert row[side + '_qty_exact'] == [str(q) for _, q in levels]
    assert next(trades, None) is next(books, None) is None
    assert count > 0
    assert not list(tmp_path.glob('*.part'))


def test_default_does_not_claim_ready_trade_support(bundle):
    source = ConsumerBundle(bundle)
    assert 'trade_observations' not in source.manifest['files']
    assert 'observation_sequence' not in source.table('depth').column_names


def test_cursor_same_visible_events_restore_and_no_future(bundle, tmp_path):
    import pickle
    import pytest
    from data.response_cursor import ResponseObservationCursor
    from features.trade_book_response import ResponseState
    original = ConsumerBundle(bundle)
    output = tmp_path / 'ready'
    derive_inputs({**original.manifest['plan'], 'include_response_observations': True}, output)
    with pytest.raises(ValueError, match='no bound ready'):
        ResponseObservationCursor(original)
    cursor = ResponseObservationCursor(output)
    cursor.advance(1_100_000_000)
    assert cursor.sequence == 0  # source exists, but not ready yet
    cursor.advance(2_000_000_000)
    sequence = cursor.sequence
    cursor.advance(2_000_000_000)
    assert cursor.sequence == sequence
    restored = pickle.loads(pickle.dumps(cursor))
    restored.advance(3_000_000_000)
    assert cursor.sequence == sequence
    cursor.advance(3_000_000_000)
    direct = ResponseState(max_book_age_ns=1_000_000_000, trade_coverage='observed',
                           fast_response_state=True, range_block_size=64)
    for tick in original.stream():
        for observation in tick.observations:
            if observation.ready_ns > 3_000_000_000:
                continue
            if isinstance(observation.payload, TradeContribution):
                direct.observe_trade(observation.ready_ns, observation.payload)
            else:
                direct.observe_book(observation.ready_ns, observation.source_asof_ns, observation.payload)
    direct.advance(3_000_000_000)
    for side, price in (('bid', 101), ('ask', 102)):
        assert cursor.frame(side, order_price=price) == restored.frame(side, order_price=price)
        assert cursor.frame(side, order_price=price) == direct.order_frame(side, price)
    assert cursor.state.stats == direct.stats
    assert cursor.frame('bid', order_price=1) is None
    with pytest.raises(ValueError, match='regressed'):
        cursor.advance(2_000_000_000)


@pytest.mark.parametrize('cut', [1200, 1600, 2100, 3000])
@pytest.mark.parametrize('async_fifo', [False, True])
def test_business_observation_and_checkpoint_do_not_change_baseline(bundle, tmp_path, cut, async_fifo):
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from tests.test_research_public_inputs import public_replay_params
    from tests.test_target_observation import exact
    original = ConsumerBundle(bundle)
    output = tmp_path / 'prepared_source'
    derive_inputs({**original.manifest['plan'], 'include_response_observations': True}, output)
    prepared = prepare_public_inputs(output, tick_size=.1)
    params = {**public_replay_params(), 'tick_size': .1}
    if async_fifo:
        from tests.test_python_planned_maintenance_replay import _async_fifo_params
        params.update(_async_fifo_params())
        params.update(_bulk_cancel_timing_samples_ms=[[2., 11., 400.]],
                      _bulk_cancel_timing_sample_semantics='synthetic coupled batch phases')
    expected = simulate_prepared_inputs(prepared, params)
    observed_params = {**params, 'response_observe_only': True}
    observed = simulate_prepared_inputs(prepared, observed_params)
    receipt = observed.pop('response_update_observation')
    assert receipt['sequence'] > 0
    assert any(':final_intent:' in key for key in receipt['downstream'])
    if async_fifo:
        assert sum(value for key, value in receipt['downstream'].items()
                   if ':request_submitted:new' in key) > 0
        requests = [request for row in receipt['samples']
                    for request in row.get('submitted_requests', [])]
        assert requests and all(request['gateway_request_sequence'] is not None for request in requests)
    for row in receipt['samples']:
        assert row['latest_observation_ready_ns'] <= row['read_ns']
        assert not row['complete_action_eligibility_verified']
    exact(observed, expected)
    checkpoint = simulate_prepared_inputs(prepared, observed_params, checkpoint_at_ts_ms=cut)['_replay_checkpoint']
    saved_sequence = checkpoint['runtime'].response_update_observer.sequence
    checkpoint_path = tmp_path / 'observation.checkpoint'
    save_runtime_checkpoint(checkpoint_path, checkpoint)
    for _ in range(2):
        restored = load_trusted_runtime_checkpoint(checkpoint_path)
        completed = simulate_prepared_inputs(prepared, observed_params, resume_checkpoint=restored)
        assert completed.pop('response_update_observation') == receipt
        exact(completed, expected)
    assert checkpoint['runtime'].response_update_observer.sequence == saved_sequence
