"""Network-free integration; synthetic messages/models, real consumers.

These tests do not assert exchange delivery or production model equivalence.
"""
import hashlib
import json
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pytest

from data.observation import TradeContribution
from data.tardis_input import BookView
from features.trade_book_response import ResponseState
from live.config import Config
from strategy.response_action_features import FEATURES, extract
from strategy.maker_engine import MakerEngine
from strategy.order_manager import Side, OrderState
from tests.test_live_public_signal import model_bundle  # noqa: F401
from tests.test_live_maker_close_ioc import _ControllableAsyncGateway, _result_for
from tests.test_response_action_value import artifact


@pytest.fixture
def assembled(model_bundle, monkeypatch, tmp_path):  # noqa: F811
    for flag in ('NARROWGATE_CPP_LIVE_ROUTING', 'NARROWGATE_CPP_ORDER_ACTION_PLAN',
                 'NARROWGATE_CPP_FINAL_ORDER_PLAN', 'NARROWGATE_CPP_QUOTE_CORE'):
        monkeypatch.setenv(flag, '0')
    now = [1_800_000_000_000_000_000]
    monkeypatch.setattr(time, 'time_ns', lambda: now[0])
    monkeypatch.setattr(time, 'time', lambda: now[0] / 1e9)
    monkeypatch.chdir(tmp_path)
    cfg = Config()
    cfg.ml.model_dir = str(model_bundle)
    cfg.ml.feature_protocol = 'execution_v1'
    cfg.strategy.use_bar_pricing = False
    # The synthetic 13-head fixture has no admitted return-action horizon.
    cfg.ml.ret_skew = 0.
    cfg.strategy.order_size = .001
    cfg.strategy.requote_threshold_bps = 0.
    cfg.strategy.replace_min_price_change_ticks = 15
    cfg.strategy.replace_min_interval_ms = 500
    cfg.strategy.replace_terminal_continuation = True
    cfg.api.async_order_lanes_enabled = True
    for name, value in vars(cfg.logging).items():
        if isinstance(value, str) and value and ('/' in value or '.' in value):
            setattr(cfg.logging, name, str(tmp_path / name))
    path = tmp_path / 'response.json'
    path.write_text(json.dumps(artifact(1)))
    cfg.strategy.response_action_policy_path = str(path)
    cfg.strategy.response_action_policy_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    gateway = _ControllableAsyncGateway()
    engine = MakerEngine(cfg, gateway)
    engine.restore_fill_cooldown_checkpoint(now_ms=now[0]//1_000_000)
    engine.set_admitted_user_stream_generation(1)
    engine.set_event_source(SimpleNamespace(user_event_safety_snapshot=lambda: {
        'user_stream_connected': True, 'user_stream_generation': 1}))
    reference = ResponseState(trade_coverage='observed', fast_response_state=False)
    for i in range(1, 75):
        now[0] += 1_000_000_000
        ts = now[0] // 1_000_000
        engine.signal.on_trade(dict(e='trade', s='BTCUSDC', t=i, T=ts, E=ts,
                                    m=bool(i % 2), p=str(100000 + i % 3), q='.01'),
                               receive_ts_ns=now[0])
        engine.signal.on_depth(dict(e='depthUpdate', s='BTCUSDC', u=i, T=ts, E=ts,
            b=[['99999.9', '2'], ['99999.8', '3']],
            a=[['100000.1', '2'], ['100000.2', '3']]), receive_ts_ns=now[0])
        reference.observe_trade(now[0], TradeContribution(str(i), now[0],
            Decimal(100000 + i % 3), Decimal('.01'), 'sell' if i % 2 else 'buy',
            1, 1, 'native_individual_trade', i))
        reference.observe_book(now[0], now[0], BookView(i,
            ((Decimal('99999.9'), Decimal(2)), (Decimal('99999.8'), Decimal(3))),
            ((Decimal('100000.1'), Decimal(2)), (Decimal('100000.2'), Decimal(3))),
            None, None, 'same_visible_record', True), emit_frames=False)
    engine._test_reference_response = reference
    yield engine, gateway, now
    engine.close_fill_cooldown_checkpoint_store()


def test_same_visible_record_all_47_features_normalization_score_and_intent(assembled):
    engine, _, now = assembled
    prediction = engine.signal.compute_signal()
    snapshot = engine.signal.quote_decision_snapshot()
    engine._compute_quotes(snapshot, 0., prediction)
    cid = engine.orders.create_order('BTCUSDC', Side.BUY, 99999.9, .001)
    engine.orders.confirm_new(cid, 123)
    policy = engine._response_action_policy
    args = dict(side='BUY', inventory=0., order=engine.orders.get_order(cid),
        target_price=99999.8, tick_size=.1, age_ms=2000.,
        context=engine._last_quote_context['BUY'], reasons=[])
    live_frame = engine.signal.response_order_frame(snapshot, 'BUY', 99999.9)
    engine._test_reference_response.advance(snapshot.capture_ts_ns)
    reference = engine._test_reference_response.order_frame('bid', 99999.9)
    assert live_frame.book_version == reference.book_version == snapshot.depth_generation
    assert live_frame.valid and reference.valid
    actual = policy.action_observation(frame=live_frame, **args)
    expected = policy.action_observation(frame=reference, **args)
    a, b = extract(actual), extract(expected)
    assert len(a) == len(b) == 47
    assert a == b
    model = policy.model
    # Check the actual model order/scales, not a sorted feature dictionary.
    np.testing.assert_array_equal(
        (np.array([a[n] for n in FEATURES])-model.means)/model.scales,
        (np.array([b[n] for n in FEATURES])-model.means)/model.scales)
    assert model.choose(actual, baseline=False) == model.choose(expected, baseline=False)
    assert any(a[n+'_missing'] == 1 for n in ('recovery_age_s', 'order_recovery_age_s'))


def test_changed_order_identity_cannot_score_captured_old_order(assembled):
    engine, _, now = assembled
    prediction = engine.signal.compute_signal()
    snapshot = engine.signal.quote_decision_snapshot()
    engine._compute_quotes(snapshot, 0., prediction)
    old = engine.orders.create_order('BTCUSDC', Side.BUY, 99999.9, .001)
    engine.orders.confirm_new(old, 123)
    order = engine.orders.get_order(old)
    order.create_time -= 2
    # A stale captured object must not borrow another current order's admission.
    order = replace(order)
    assert not engine._apply_replace_throttle(side=Side.BUY, now_ts=now[0]/1e9,
        q=0., target_price=99999.8, order=order, needs_update=True, force_update=False,
        age_wake_eligible=True, response_snapshot=snapshot, desired_quantity=.001)
    assert engine._response_action_policy.counts['scored'] == 0


def test_real_input_quote_and_order_assembly(assembled):
    engine, gateway, now = assembled
    prediction = engine.signal.compute_signal()
    snapshot = engine.signal.quote_decision_snapshot()
    guard = engine._post_only_guard_for_snapshot(snapshot)
    prices = engine._compute_quotes(snapshot, 0., prediction, post_only_guard=guard)
    assert prices
    assert engine._last_quote_context['BUY']['raw_half_spread'] is not None
    cid = engine.orders.create_order('BTCUSDC', Side.BUY, 99999.9, .001)
    engine.orders.confirm_new(cid, 123)
    engine.orders.get_order(cid).create_time -= 2
    engine._bid_cid = cid
    assert engine.signal.response_order_frame(snapshot, 'BUY', 99999.9).valid
    assert engine._apply_replace_throttle(side=Side.BUY, now_ts=now[0]/1e9,
        q=0., target_price=99999.8, order=engine.orders.get_order(cid),
        needs_update=True, force_update=False, age_wake_eligible=True,
        response_snapshot=snapshot, desired_quantity=.001), engine._response_action_policy.counts
    assert engine._response_action_policy.counts['scored'] == 1
    assert not engine._cancel_order(cid)
    assert len(gateway.cancel_calls) == 1
    assert engine.orders.get_order(cid).state is OrderState.PENDING_CANCEL
    gateway.cancel_future.set_result(_result_for(gateway.cancel_calls[0], status='CANCELED'))
    assert engine.orders.get_order(cid).state is OrderState.CANCELED


@pytest.mark.parametrize('completion', ['http', 'private_before_http', 'unknown', 'partial_then_cancel', 'reject',
                                        'new_private_before_http', 'new_unknown', 'new_rejected'])
def test_unbroken_quote_route_uses_real_gateway_and_manager(assembled, completion):
    engine, gateway, now = assembled
    prediction = engine.signal.compute_signal()
    snapshot = engine.signal.quote_decision_snapshot()
    guard = engine._post_only_guard_for_snapshot(snapshot)
    bid, ask, _ = engine._compute_quotes(snapshot, 0., prediction, post_only_guard=guard)
    cid = engine.orders.create_order('BTCUSDC', Side.BUY, 99999.9, .001)
    engine.orders.confirm_new(cid, 123)
    engine.orders.get_order(cid).create_time -= 2
    engine._bid_cid = cid
    engine._update_orders(mid=snapshot.mid, bid_price=bid, ask_price=ask, q=0.,
        pred=prediction, quote_snapshot=snapshot, post_only_guard=guard,
        route_sides=frozenset({Side.BUY}))
    assert engine._response_action_policy.counts['scored'] == 1, (
        engine._response_action_policy.counts, engine._last_bid_action, bid, ask,
        engine._last_quote_context['BUY'], gateway.cancel_calls, gateway.new_calls)
    assert len(gateway.cancel_calls) == 1
    assert engine.orders.get_order(cid).state is OrderState.PENDING_CANCEL
    event = dict(c=cid, i=123, s='BTCUSDC', S='BUY', X='CANCELED', o='LIMIT',
                 p='99999.9', q='.001', z='0', l='0', L='0',
                 T=now[0]//1_000_000, _local_receive_ts_ns=now[0])
    if completion == 'unknown':
        gateway.cancel_future.set_exception(TimeoutError('response not observed'))
        assert engine.orders.get_order(cid).state is OrderState.PENDING_CANCEL
    elif completion == 'reject':
        gateway.cancel_future.set_result(_result_for(gateway.cancel_calls[0], status='NEW'))
        assert engine.orders.get_order(cid).state is OrderState.OPEN
    else:
        if completion == 'partial_then_cancel':
            engine.orders.on_order_update(event | dict(X='PARTIALLY_FILLED',
                l='.0004', z='.0004', L='99999.9', ap='99999.9', n='0', N='USDC', t=991))
            assert engine.orders.get_order(cid).filled_qty == .0004
            event['z'] = '.0004'
        if completion in ('private_before_http', 'partial_then_cancel'):
            engine.orders.on_order_update(event)
        result = _result_for(gateway.cancel_calls[0], status='CANCELED')
        if completion == 'partial_then_cancel':
            result['executedQty'] = '.0004'
        gateway.cancel_future.set_result(result)
        assert engine.orders.get_order(cid).state is OrderState.CANCELED
        assert engine.orders.get_order(cid).filled_qty == (.0004 if completion == 'partial_then_cancel' else 0)
    if completion.startswith('new_'):
        intents = engine._take_ready_replace_terminal_continuations()
        assert set(intents) == {Side.BUY}
        assert engine._take_ready_replace_terminal_continuations() == {}
        engine._requote(route_sides=frozenset(intents), advance_requote_clock=False,
                        replace_terminal_continuations=intents)
        assert len(gateway.new_calls) == 1
        request = gateway.new_calls[0]
        new_cid = request['newClientOrderId']
        assert engine.orders.get_order(new_cid).state is OrderState.PENDING_NEW
        if completion == 'new_private_before_http':
            engine.orders.on_order_update(event | dict(c=new_cid, i=124, X='NEW', p=str(request['price'])))
            assert engine.orders.get_order(new_cid).state is OrderState.OPEN
            gateway.new_future.set_result(_result_for(request, order_id=124))
            assert engine.orders.get_order(new_cid).state is OrderState.OPEN
        elif completion == 'new_unknown':
            gateway.new_future.set_exception(TimeoutError('new response unknown'))
            assert engine.orders.get_order(new_cid).state is OrderState.PENDING_NEW
            assert engine.orders.get_order(new_cid).lifecycle.submit_ack_unknown_observed
        else:
            gateway.new_future.set_result(_result_for(request, status='REJECTED', order_id=124))
            assert engine.orders.get_order(new_cid).state is OrderState.REJECTED


def test_snapshot_change_and_disconnect_do_not_borrow_future_history(assembled):
    engine, _, now = assembled
    snapshot = engine.signal.quote_decision_snapshot()
    assert engine.signal.response_order_frame(snapshot, 'BUY', 99999.9).valid
    now[0] += 1_000_000
    engine.signal.on_trade(dict(e='trade', s='BTCUSDC', t=75, T=now[0]//1_000_000,
        E=now[0]//1_000_000, m=False, p='100000', q='.01'), receive_ts_ns=now[0])
    assert engine.signal.response_order_frame(snapshot, 'BUY', 99999.9) is None
    engine.signal.market_disconnected()
    assert engine.signal.response_order_frame(snapshot, 'BUY', 99999.9) is None
    now[0] += 1_000_000_000
    ts = now[0]//1_000_000
    engine.signal.on_trade(dict(e='trade', s='BTCUSDC', t=76, T=ts, E=ts,
                               m=False, p='100000', q='.01'), receive_ts_ns=now[0])
    engine.signal.on_depth(dict(e='depthUpdate', s='BTCUSDC', u=75, T=ts, E=ts,
        b=[['99999.9','2']], a=[['100000.1','2']]), receive_ts_ns=now[0])
    fresh = engine.signal.response_order_frame(engine.signal.quote_decision_snapshot(), 'BUY', 99999.9)
    assert fresh is None or not fresh.valid
