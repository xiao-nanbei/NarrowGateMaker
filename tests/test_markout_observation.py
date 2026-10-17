"""Output-only markout contracts exercised through the original consumers."""
import json
import logging
from types import SimpleNamespace

import pytest

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from models.backtest_tick import simulate_tick
from models.replay.l2_journal import ReplayL2Journal
from models.replay.runtime_checkpoint_io import load_trusted_runtime_checkpoint, save_runtime_checkpoint
from strategy.maker_engine import MakerEngine
from strategy.markout_observation import pause_transition
from tests.test_tick_runtime_checkpoint import assert_same, scenario


def params(args, **overrides):
    return {**args[3], "markout_ema_span_fills": 50, "markout_spread_scale": .2,
            "markout_horizon_s": 10., "adverse_markout_max_resolve_gap_s": 30., **overrides}


@pytest.mark.parametrize("age,status,process", [
    (9999, "RESOLVED", 100), (10000, "RESOLVED", 0),
    (30000, "RESOLVED", 0), (30001, "STALE_DROP", 0),
    (0, "END_NOT_MATURE", None),
])
def test_original_sampled_age_boundaries_and_terminal_pending(tmp_path, age, status, process):
    args, kw = scenario("ordinary")
    p = params(args)
    p['_l2_journal'] = ReplayL2Journal(tmp_path/'prefix', identity={})
    checkpoint = simulate_tick(*args[:3], p, **kw, checkpoint_at_ts_ms=0)['_replay_checkpoint']
    # Synthetic inherited pending observation; no new matching assumptions.
    checkpoint['runtime'].mo_pending = [(-age, 100., True,
        {'markout_observation_id': 'inherited-test', 'markout_order_id': None}, .001)]
    p['_l2_journal'] = ReplayL2Journal(tmp_path/'tail', identity={})
    out = simulate_tick(*args[:3], p, **kw, resume_checkpoint=checkpoint)
    rows = [r['record'] for r in iter_chunked_parquet_journal(out['_l2_journal']['manifest'])
            if r['event_type'] == 'markout_observation' and r['record']['observation_id'] == 'inherited-test']
    assert len(rows) == 1 and rows[0]['status'] == status
    if process is not None:
        assert rows[0]['process_ts_ms'] == process
    if status == 'RESOLVED':
        assert rows[0]['price_provenance']['price_age_known'] is False
        assert rows[0]['price_provenance']['price_source_ts_ms'] is None
    assert out['_l2_journal']['independent_delivery_verified']


def test_effective_limit_is_not_the_raw_configuration():
    args, kw = scenario('ordinary')
    result = simulate_tick(*args[:3], params(args, markout_horizon_s=110.), **kw)
    semantics = result['markout_semantics']
    assert semantics['minimum_fill_age_s'] == 110.
    assert semantics['configured_max_fill_age_s'] == 30.
    assert semantics['effective_max_fill_age_s'] == 110.
    assert semantics['max_fill_age_rule_applied']
    assert not semantics['exact_target_time_query']


def test_message_schedule_31_seconds_is_not_sampled_stale_drop(tmp_path):
    from tests.test_exec_book_visibility_delay import _message_schedule_replay_inputs
    inputs = _message_schedule_replay_inputs()
    inputs['params'].update(markout_ema_span_fills=50, markout_spread_scale=.2,
                            markout_horizon_s=10., adverse_markout_max_resolve_gap_s=30.)
    inputs['params']['_l2_journal'] = ReplayL2Journal(tmp_path/'prefix', identity={})
    checkpoint = simulate_tick(**inputs, checkpoint_at_ts_ms=0)['_replay_checkpoint']
    checkpoint['runtime'].mo_pending = [(-31000, 100., True,
        {'markout_observation_id': 'late-message'}, .001)]
    inputs['params']['_l2_journal'] = ReplayL2Journal(tmp_path/'tail', identity={})
    out = simulate_tick(**inputs, resume_checkpoint=checkpoint)
    rows = [r['record'] for r in iter_chunked_parquet_journal(out['_l2_journal']['manifest'])
            if r['event_type'] == 'markout_observation' and r['record']['observation_id'] == 'late-message']
    assert any(r['status'] == 'RESOLVED' for r in rows)
    assert not any(r['status'] == 'STALE_DROP' for r in rows)
    assert out['markout_semantics']['max_fill_age_rule_applied'] is False


@pytest.mark.parametrize('cut', [0, 500, 1100, 2001])
def test_enabled_markout_delivery_and_disk_restore_preserve_all_outputs(tmp_path, cut):
    args, kw = scenario('ordinary')
    p = params(args, markout_horizon_s=.1, adverse_markout_pause_hybrid=True,
               adverse_markout_pause_threshold=.001, adverse_markout_decay_tau_s=.5)
    no_sink = simulate_tick(*args[:3], p, **kw)

    def run(name, **options):
        writer = ReplayL2Journal(tmp_path/name, identity={'synthetic': True}, chunk_rows=2)
        return simulate_tick(*args[:3], {**p, '_l2_journal': writer}, **kw, **options)

    whole = run('whole')
    receipt = whole.pop('_l2_journal')
    assert_same(whole, no_sink)
    checkpoint = run('prefix', checkpoint_at_ts_ms=cut)['_replay_checkpoint']
    path = tmp_path/'checkpoint'
    save_runtime_checkpoint(path, checkpoint)
    restored = run('restored', resume_checkpoint=load_trusted_runtime_checkpoint(path))
    restored_receipt = restored.pop('_l2_journal')
    assert_same(restored, whole)
    expected_rows = list(iter_chunked_parquet_journal(receipt['manifest']))
    assert list(iter_chunked_parquet_journal(restored_receipt['manifest'])) == expected_rows
    assert any(r['event_type'] == 'markout_enqueued' for r in expected_rows)
    assert any(r['event_type'] == 'markout_observation' for r in expected_rows)
    decisions = [r['record'] for r in expected_rows if r['event_type'] == 'decision']
    assert decisions and all(r['markout_action_observation']['independent_veto'] == 'NOT_EVALUATED' for r in decisions)
    assert receipt['independent_delivery_verified'] and receipt['dropped'] == 0


def engine():
    obj = MakerEngine.__new__(MakerEngine)
    obj.cfg = SimpleNamespace(strategy=SimpleNamespace(markout_ema_span_fills=50,
        markout_spread_scale=.2, markout_horizon_s=10., adverse_markout_pause_hybrid=True,
        adverse_pause=True, adverse_markout_pause_threshold=5., adverse_markout_decay_tau_s=900.))
    obj._mo_pending = [(0., 100., 'BUY')]
    obj._mo_ema_all = obj._mo_ema_bid = obj._mo_ema_ask = 0.
    obj._mo_last_decay_time = 0.
    obj._mo_pause_until = {'BUY': 0., 'SELL': 0.}
    obj._mo_last_pause_log = {'BUY': 0., 'SELL': 0.}
    return obj


def test_live_31s_has_no_total_age_discard_and_debug_logging_is_output_only(caplog):
    a, b = engine(), engine()
    with caplog.at_level(logging.INFO, logger='maker_engine'):
        a._resolve_pending_markouts(9., 101.)
        a._resolve_pending_markouts(31., 101.)
    with caplog.at_level(logging.DEBUG, logger='maker_engine'):
        b._resolve_pending_markouts(9., 101.)
        b._resolve_pending_markouts(31., 101.)
    for key in ('_mo_pending', '_mo_ema_all', '_mo_ema_bid', '_mo_ema_ask', '_mo_pause_until'):
        assert getattr(a, key) == getattr(b, key)
    assert not a._mo_pending and a._mo_ema_bid == 2 / 51
    rows = [json.loads(r.message.split(' ', 1)[1]) for r in caplog.records if r.message.startswith('MARKOUT_RESOLVED ')]
    assert len(rows) == 1 and rows[0]['fill_age_s'] == 31.
    assert rows[0]['price_source_time_s'] is None and rows[0]['fill_id'] is None
    assert a.markout_observation_semantics()['max_fill_age_rule_applied'] is False


def test_ema_recovery_exits_before_min_ttl_and_extensions_can_exceed_max(caplog):
    a = engine()
    a._mo_pending = []
    a._mo_ema_bid = -5.01
    a._mo_last_decay_time = 1.
    with caplog.at_level(logging.DEBUG, logger='maker_engine'):
        a._observe_markout_pause(1., 'test_initial')
        a._extend_markout_pause_until('BUY', 1.)
        a._observe_markout_pause(1., 'test_extend')
        assert a._markout_pause_latch_active('BUY', 1.)
        a._resolve_pending_markouts(3., 100.)
        assert not a._markout_pause_latch_active('BUY', 3.)
    transitions = [json.loads(r.message.split(' ', 1)[1])['transition'] for r in caplog.records
                   if r.message.startswith('MARKOUT_PAUSE_OBSERVATION ')]
    assert 'ENTER' in transitions and 'EXIT' in transitions
    a._mo_ema_bid = -50.
    a._extend_markout_pause_until('BUY', 10.)
    previous = {'active': True, 'until': a._mo_pause_until['BUY']}
    a._extend_markout_pause_until('BUY', 800.)
    assert a._mo_pause_until['BUY'] - 10. > 900.
    assert pause_transition(previous, {'active': True, 'until': a._mo_pause_until['BUY']}) == 'EXTEND'


def test_same_timestamp_resolves_keep_original_global_and_side_order(caplog):
    a = engine()
    a._mo_pending = [(1., 100., 'BUY'), (1., 110., 'SELL'), (1., 90., 'BUY')]
    with caplog.at_level(logging.DEBUG, logger='maker_engine'):
        a._resolve_pending_markouts(11., 105.)
    alpha = 2 / 51
    assert a._mo_ema_all == alpha * 15 + (1-alpha) * (alpha * 5 + (1-alpha) * (alpha * 5))
    assert a._mo_ema_bid == alpha * 15 + (1-alpha) * (alpha * 5)
    rows = [json.loads(r.message.split(' ', 1)[1]) for r in caplog.records if r.message.startswith('MARKOUT_RESOLVED ')]
    assert [r['side'] for r in rows] == ['BUY', 'SELL', 'BUY']


def test_missing_markout_delivery_is_not_reported_as_complete(tmp_path):
    args, kw = scenario('ordinary')
    writer = ReplayL2Journal(tmp_path/'missing', identity={})
    emit = writer.emit
    writer.emit = lambda kind, *a, **kw: None if kind == 'markout_observation' else emit(kind, *a, **kw)
    with pytest.raises(ValueError, match='production event|coverage'):
        simulate_tick(*args[:3], params(args, markout_horizon_s=.1, _l2_journal=writer), **kw)
    assert not writer.writer.closed
