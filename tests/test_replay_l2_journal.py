"""Exercise the production callbacks and existing bounded writer together."""
import pytest

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from models.backtest_tick import simulate_tick
from models.replay.l2_journal import ReplayL2Journal
from tests.test_tick_runtime_checkpoint import assert_same, scenario


@pytest.mark.parametrize("mode", ["ordinary", "async", "compute", "timeout", "emergency", "close_replace"])
def test_l2_output_does_not_change_execution(mode, tmp_path):
    args, kwargs = scenario(mode)
    expected = simulate_tick(*args, **kwargs)
    params = {**args[3], "_l2_journal": ReplayL2Journal(tmp_path / mode, identity={"synthetic": True}, chunk_rows=3)}
    actual = simulate_tick(*args[:3], params, **kwargs)
    receipt = actual.pop("_l2_journal")
    assert_same(actual, expected)
    rows = list(iter_chunked_parquet_journal(receipt["manifest"]))
    assert len(rows) == receipt["rows"]
    assert receipt["dropped"] == 0
    assert receipt['independent_delivery_verified']
    assert receipt['production']['total'] == len(rows)
    assert rows[-1]["event_type"] == "account_end"
    assert receipt["counts"].get("decision", 0) == actual["_trace_coverage"]["routed_decisions"]["attempted"]


def test_unknown_values_survive_disk_and_output_is_create_only(tmp_path):
    journal = ReplayL2Journal(tmp_path / "out", identity={}, chunk_rows=1)
    journal.emit("fill", 42, payload={"ev": float("nan"), "toxic": None})
    receipt = journal.close()
    row = next(iter_chunked_parquet_journal(receipt["manifest"]))
    assert row["record"] == {"ev": None, "toxic": None}
    with pytest.raises(FileExistsError):
        ReplayL2Journal(tmp_path / "out", identity={})


def test_capped_memory_views_do_not_cap_research_journal(tmp_path):
    args, kwargs = scenario("ordinary")
    params = {**args[3], "trace_fills_max": 0, "trace_quotes_max": 0,
              "trace_decisions_max": 0,
              "_l2_journal": ReplayL2Journal(tmp_path / "full", identity={}, chunk_rows=2)}
    actual = simulate_tick(*args[:3], params, **kwargs)
    counts = actual["_l2_journal"]["counts"]
    for event, view in (("fill", "fills"), ("decision", "routed_decisions"),
                        ("order_outcome", "order_outcomes")):
        assert counts.get(event, 0) == actual["_trace_coverage"][view]["attempted"]


def test_unvalidated_resume_and_deferred_mutation_are_rejected(tmp_path):
    args, kwargs = scenario("ordinary")
    params = {**args[3], "_l2_journal": ReplayL2Journal(tmp_path / "guard", identity={})}
    with pytest.raises(ValueError, match="checkpoint unsupported"):
        simulate_tick(*args[:3], params, **kwargs, checkpoint_at_ts_ms=1000)
    params["trace_external_market_release"] = True
    with pytest.raises(ValueError, match="not validated"):
        simulate_tick(*args[:3], params, **kwargs)


@pytest.mark.parametrize('fault', ['omit', 'duplicate', 'disk_failure'])
def test_production_entry_detects_delivery_faults(tmp_path, fault):
    args, kwargs = scenario('ordinary')
    journal = ReplayL2Journal(tmp_path / fault, identity={}, chunk_rows=2)
    emit = journal.emit
    injected = False

    def faulty(*args, **kwargs):
        nonlocal injected
        if not injected:
            injected = True
            if fault == 'omit':
                return
            if fault == 'disk_failure':
                raise OSError('injected write failure')
            emit(*args, **kwargs)
        emit(*args, **kwargs)

    journal.emit = faulty
    params = {**args[3], '_l2_journal': journal}
    with pytest.raises((ValueError, OSError), match='production event|write failure'):
        simulate_tick(*args[:3], params, **kwargs)
    assert not journal.writer.closed


def test_closed_delivery_rejects_truncated_bytes(tmp_path):
    from models.replay.l2_journal import audit_l2_delivery
    args, kwargs = scenario('ordinary')
    journal = ReplayL2Journal(tmp_path / 'truncate', identity={})
    params = {**args[3], '_l2_journal': journal}
    actual = simulate_tick(*args[:3], params, **kwargs)
    receipt = actual['_l2_journal']
    part = next(journal.writer.output_dir.glob('part-*.parquet'))
    with part.open('r+b') as handle:
        handle.truncate(16)
    with pytest.raises(ValueError, match='hash mismatch'):
        audit_l2_delivery(receipt['manifest'], receipt['production'])


def test_actual_writer_failure_never_publishes_closed_receipt(tmp_path, monkeypatch):
    import execution.chunked_parquet_journal as writer_module
    args, kwargs = scenario('ordinary')
    journal = ReplayL2Journal(tmp_path/'disk-failure', identity={}, chunk_rows=1)
    def fail(*args, **kwargs):
        raise OSError('injected parquet disk failure')
    monkeypatch.setattr(writer_module.pq, 'write_table', fail)
    with pytest.raises(OSError, match='parquet disk failure'):
        simulate_tick(*args[:3], {**args[3], '_l2_journal': journal}, **kwargs)
    assert not journal.writer.closed and not journal.writer.manifest_path.exists()


def test_omitted_final_event_detected_at_close(tmp_path):
    args, kwargs = scenario('ordinary')
    journal = ReplayL2Journal(tmp_path/'missing-last', identity={}, chunk_rows=1)
    emit = journal.emit
    journal.emit = lambda kind, *args, **kwargs: None if kind == 'account_end' else emit(kind, *args, **kwargs)
    with pytest.raises(ValueError, match='submitted event coverage'):
        simulate_tick(*args[:3], {**args[3], '_l2_journal': journal}, **kwargs)
    assert not journal.writer.closed


@pytest.mark.parametrize('mode', ['ordinary', 'async', 'compute', 'timeout', 'emergency', 'close_replace'])
def test_creation_and_cutoff_are_output_only_and_not_fake_terminals(tmp_path, mode):
    args, kwargs = scenario(mode)
    journal = ReplayL2Journal(tmp_path / 'causes', identity={})
    params = {**args[3], '_l2_journal': journal}
    simulate_tick(*args[:3], params, **kwargs)
    rows = list(iter_chunked_parquet_journal(journal.writer.manifest_path))
    decisions = {r['production_event']['sequence']: r for r in rows if r['event_type'] == 'decision'}
    created = [r['record'] for r in rows if r['event_type'] == 'order_created']
    assert created
    for row in created:
        if row['circuit_breaker_close']:
            assert row['creation_opportunity_event_id'] is None
            assert row['creation_source'] in {'resume_maker_close', 'requote_circuit_breaker_close'}
        else:
            assert row['creation_opportunity_event_id'] in decisions
            assert row['creation_source'] == 'quote_decision'
    cutoff = [r['record'] for r in rows if r['event_type'] == 'order_observation_cutoff']
    for row in cutoff:
        assert row['administrative_right_censored'] is True
        assert row['reason'] == 'account_observation_ended_not_exchange_terminal'
        assert 'outcome' not in row


def test_active_tail_is_censored_without_manufacturing_cancel(tmp_path):
    args, kwargs = scenario('ordinary')
    params = {**args[3], 'planned_quote_stop_ts_ms': 20_000,
              'replay_event_clock_end_ts_ms': 500}
    inputs = (args[0].loc[args[0].transact_time <= 500], *args[1:3])
    expected = simulate_tick(*inputs, params, **kwargs)
    journal = ReplayL2Journal(tmp_path / 'tail', identity={})
    actual = simulate_tick(*inputs, {**params, '_l2_journal': journal}, **kwargs)
    actual.pop('_l2_journal')
    assert_same(actual, expected)
    rows = list(iter_chunked_parquet_journal(journal.writer.manifest_path))
    tails = [r['record'] for r in rows if r['event_type'] == 'order_observation_cutoff']
    assert len(tails) == 2
    for row in tails:
        assert row['administrative_right_censored']
        assert row['local_remaining_btc'] > 0
        assert row['pending_request_clocks']['cancel_request_ts'] == -1
        assert not any(r['event_type'] == 'order_outcome' and r['record']['outcome'] != 'open_end' and
                       r['record']['order_id'] == row['order_id'] for r in rows)


@pytest.mark.parametrize('reset_ms', [500, 1100, 2100])
def test_resnapshot_uses_production_entry_without_changing_own_orders(tmp_path, reset_ms):
    from models.tick_data_types import HistoricalExchangeBookEvent
    args, kwargs = scenario('ordinary')
    levels = (('bid', 999, 1.), ('ask', 1001, 1.))
    def tape():
        return [HistoricalExchangeBookEvent(
            market_id='binance_futures:perpetual:BTCUSDC', event_type='snapshot',
            exchange_ts_ns=ms*1_000_000, levels=levels, last_update_id=number,
            source_ordinal=number) for number, ms in enumerate([1, reset_ms], 1)]
    params = {**args[3], 'exchange_book_queue_mode': 'diagnostic'}
    expected = simulate_tick(*args[:3], params, **kwargs, exchange_book_event_tape=tape())
    journal = ReplayL2Journal(tmp_path / 'reset', identity={})
    actual = simulate_tick(*args[:3], {**params, '_l2_journal': journal}, **kwargs,
                           exchange_book_event_tape=tape())
    receipt = actual.pop('_l2_journal')
    assert_same(actual, expected)
    rows = list(iter_chunked_parquet_journal(receipt['manifest']))
    resets = [r['record'] for r in rows if r['event_type'] == 'book_snapshot_boundary']
    assert len(resets) == receipt['exchange_book_stats']['snapshot_events'] == 2
    assert resets[-1]['source_ordinal'] == 2
    assert resets[-1]['message_levels'] == 2
    # Reset invalidates queue evidence, not ownership or a cancel ACK.
    for before, order in zip(resets[-1]['own_orders_before_queue_invalidation'],
                             resets[-1]['own_orders_after_queue_invalidation'], strict=True):
        assert order['exchange_book_queue_path_valid'] is False
        for key in ('trace_id', 'state', 'remaining', 'exchange_remaining', 'queue_left',
                    'cancel_request_ts', 'cancel_effective_ts', 'cancel_ts'):
            assert before[key] == order[key]
        if before['exchange_book_queue_path_valid']:
            assert order['exchange_book_queue_invalidated_reason'] == 'native_snapshot_reset'


@pytest.mark.parametrize('status', ['OPEN', 'PENDING_CANCEL'])
def test_resnapshot_preserves_partial_order_and_inflight_cancel(tmp_path, status):
    from models.tick_data_types import HistoricalExchangeBookEvent
    args, kwargs = scenario('ordinary')
    trades = args[0].copy()
    trades['quantity'] = 0.
    params = {**args[3], 'exchange_book_queue_mode': 'diagnostic',
        'requote_interval': 100., 'rq_min': 100., 'rq_max': 100.,
        'replace_min_interval_ms': 100_000,
        'initial_live_state': {'active_orders': [dict(side='BUY', price=99.,
            quantity=.002, remaining=.001, submit_ts_ms=0, event_ts_ms=0,
            status=status, queue_left=.75, cancel_request_ts_ms=0,
            cancel_effective_ts_ms=900, cancel_ts_ms=950)]}}
    def tape():
        return [HistoricalExchangeBookEvent(market_id='binance_futures:perpetual:BTCUSDC',
            event_type='snapshot', exchange_ts_ns=ms*1_000_000,
            levels=(('bid', 990, 8.), ('ask', 1010, 9.)), last_update_id=n,
            source_ordinal=n) for n, ms in enumerate([1, 500], 1)]
    journal = ReplayL2Journal(tmp_path/'partial', identity={})
    params['_l2_journal'] = journal
    simulate_tick(trades, *args[1:3], params, **kwargs, exchange_book_event_tape=tape())
    resets = [row['record'] for row in iter_chunked_parquet_journal(journal.writer.manifest_path)
              if row['event_type'] == 'book_snapshot_boundary']
    before = next(x for x in resets[1]['own_orders_before_queue_invalidation'] if x['trace_id'] == 0)
    after = next(x for x in resets[1]['own_orders_after_queue_invalidation'] if x['trace_id'] == 0)
    assert before == after  # inherited queue history was already explicitly unknown
    assert after['remaining'] == after['exchange_remaining'] == .001
    assert after['queue_left'] == .75
    assert after['exchange_book_queue_path_valid'] is False
    if status == 'PENDING_CANCEL':
        assert after['cancel_effective_ts'] == 900 and after['cancel_ts'] == 950
