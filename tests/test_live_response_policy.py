from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace

import pytest

from features.trade_book_response import ResponseFrame
from live.response_policy import LiveResponseHistory, LiveResponsePolicy
from strategy.response_action_features import BASE, CONTEXT, HISTORY
from strategy.order_manager import Side, OrderState
from tests.test_live_replace_throttle import _engine, _order, _routable_update_orders_engine
from tests.test_live_public_signal import model_bundle  # noqa: F401
from tests.test_response_action_value import artifact


def frame():
    return ResponseFrame(12_000_000_000, 12_000_000_000, 12, 'bid', 100., 1., 101.,
        tuple(dict.fromkeys(BASE + HISTORY, 1.).items()), True, None)


@pytest.mark.parametrize('intercept,expected', [(1, True), (-1, False), (0, False)])
def test_actual_live_price_gate(intercept, expected):
    engine = _engine()
    engine.cfg.strategy.replace_min_price_change_ticks = 10
    order = _order(Side.BUY, 100., 1000.)
    engine.orders = SimpleNamespace(get_active_orders=lambda: [order])
    engine.signal = SimpleNamespace(response_order_frame=lambda *args: frame())
    engine._last_quote_context = {'BUY': dict.fromkeys(CONTEXT, 1.)}
    engine._response_action_policy = LiveResponsePolicy(artifact(intercept))
    args = dict(side=Side.BUY, now_ts=order.create_time+1., q=0., target_price=100.2,
        order=order, needs_update=True, force_update=False, age_wake_eligible=True,
        response_snapshot=object(), desired_quantity=.001)
    assert engine._apply_replace_throttle(**args) is expected
    assert engine._apply_replace_throttle(**(args | {'force_update': True})) is True
    assert engine._apply_replace_throttle(**(args | {'needs_update': False})) is False


@pytest.mark.parametrize('change', ['age', 'quantity', 'pending', 'risk', 'multiple', 'stale', 'missing'])
def test_model_cannot_relax_admission(change):
    engine = _engine()
    engine.cfg.strategy.replace_min_price_change_ticks = 10
    order = _order(Side.BUY, 100., 1000.)
    engine.orders = SimpleNamespace(get_active_orders=lambda: [order, order] if change == 'multiple' else [order])
    f = None if change == 'missing' else replace(frame(), valid=change != 'stale')
    engine.signal = SimpleNamespace(response_order_frame=lambda *args: f)
    engine._last_quote_context = {'BUY': dict.fromkeys(CONTEXT, 1.)}
    engine._response_action_policy = LiveResponsePolicy(artifact(1))
    if change == 'pending':
        order.state = OrderState.PENDING_NEW
    args = dict(side=Side.BUY, now_ts=order.create_time+(0.1 if change == 'age' else 1.),
        q=0., target_price=100.2, order=order, needs_update=True, force_update=False,
        age_wake_eligible=change != 'risk', response_snapshot=object(),
        desired_quantity=.002 if change == 'quantity' else .001)
    assert engine._apply_replace_throttle(**args) is False
    assert engine._response_action_policy.counts['scored'] == 0


def test_history_warmup_gap_and_snapshot_binding():
    history = LiveResponseHistory()
    for i in range(1, 13):
        now = i * 1_000_000_000
        history.trade(dict(e='trade', t=i, T=i*1000, m=False, p='100', q='1'), now_ns=now)
        history.book(dict(T=i*1000, b=[['100','2'],['99','2']], a=[['102','2'],['103','2']]),
                     now_ns=now, version=i)
    args = dict(sequence=history.sequence, capture_ns=now, side='BUY', price=100.)
    assert history.frame(**args).valid
    history.trade(dict(e='trade', t=13, T=12000, m=False, p='100', q='1'), now_ns=now+1)
    assert history.frame(**args) is None
    history.trade(dict(e='trade', t=15, T=12000, m=False, p='100', q='1'), now_ns=now+2)
    assert history.state.book is None
    assert history.state.stats['invalid:trade_gap_or_duplicate'] == 1


def test_artifact_bytes_bound(tmp_path):
    path = tmp_path/'model.json'
    path.write_text(json.dumps(artifact(1)))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert LiveResponsePolicy.load(path, digest).model.path_id == 'TRADE_BOOK_RESPONSE_VALUE'
    with pytest.raises(ValueError, match='identity'):
        LiveResponsePolicy.load(path, '0'*64)


@pytest.mark.parametrize('score,expected_request', [(1, True), (-1, False)])
def test_real_order_routing_receives_model_price_intent(monkeypatch, score, expected_request):
    from strategy import maker_engine as module
    monkeypatch.setenv('NARROWGATE_CPP_ORDER_ACTION_PLAN', '0')
    monkeypatch.setenv('NARROWGATE_CPP_FINAL_ORDER_PLAN', '0')
    monkeypatch.setattr(module, '_get_live_routing_cpp', lambda: None)
    engine = _routable_update_orders_engine()
    engine._native_order_action_planner = None
    engine.cfg.strategy.order_size = .001
    engine.cfg.strategy.requote_threshold_bps = 0.
    engine.cfg.strategy.replace_min_price_change_ticks = 10.
    engine.cfg.risk.max_position_value = 3000.
    cid = engine.orders.create_order('BTCUSDC', Side.BUY, price=98999.8, quantity=.001)
    engine.orders.confirm_new(cid, 502)
    engine.orders.get_order(cid).create_time -= 2
    engine._bid_cid = cid
    engine.signal = SimpleNamespace(response_order_frame=lambda *args: replace(frame(), price=98999.8))
    engine._last_quote_context = {'BUY': dict.fromkeys(CONTEXT, 1.), 'SELL': {}}
    engine._response_action_policy = LiveResponsePolicy(artifact(score))
    requests = []
    engine._cancel_order = lambda *args, **kw: requests.append(('cancel', args)) or False
    engine._place_order = lambda *args, **kw: requests.append(('new', args)) or 'new-id'
    engine._update_orders(mid=100000., bid_price=99000., ask_price=101000., q=0.,
        pred=SimpleNamespace(), quote_snapshot=SimpleNamespace(),
        post_only_guard=SimpleNamespace(best_bid=99000., best_ask=101000., source='test'),
        route_sides=frozenset({Side.BUY}))
    assert engine._response_action_policy.counts['scored'] == 1, engine._response_action_policy.counts
    assert bool(requests) is expected_request


def test_actual_individual_live_input_history(model_bundle, monkeypatch):  # noqa: F811
    from strategy.live_public_signal import LivePublicSignalEngine
    import time
    base = 1_800_000_000_000_000_000
    now = [base]
    monkeypatch.setattr(time, 'time_ns', lambda: now[0])
    engine = LivePublicSignalEngine(model_dir=model_bundle, symbol='BTCUSDC')
    engine.enable_response_history()
    for i in range(1, 13):
        now[0] = base+i*1_000_000_000
        ts = now[0]//1_000_000
        event = dict(e='trade', s='BTCUSDC', t=i, T=ts, E=ts, m=False, p='100', q='1')
        engine.on_trade(event, receive_ts_ns=now[0])
        book = dict(e='depthUpdate', s='BTCUSDC', u=i, T=ts, E=ts,
                    b=[['100','2'],['99','2']], a=[['102','2'],['103','2']])
        engine.on_depth(book, receive_ts_ns=now[0])
    snapshot = engine.quote_decision_snapshot()
    assert engine.response_order_frame(snapshot, 'BUY', 100.).valid
    sequence = engine._response_history.sequence
    engine.on_trade(event, receive_ts_ns=now[0])
    engine.on_depth(book, receive_ts_ns=now[0])
    assert engine._response_history.sequence == sequence
    engine.market_disconnected()
    assert engine.response_order_frame(snapshot, 'BUY', 100.) is None


@pytest.mark.parametrize('flag', ['NARROWGATE_CPP_LIVE_ROUTING',
    'NARROWGATE_CPP_ORDER_ACTION_PLAN', 'NARROWGATE_CPP_FINAL_ORDER_PLAN'])
def test_native_planner_cannot_silently_bypass_policy(monkeypatch, flag):
    from live.config import Config
    from strategy.maker_engine import validate_native_live_routing_policy_compatibility
    cfg = Config()
    cfg.strategy.response_action_policy_path = 'local-candidate'
    monkeypatch.setenv(flag, '1')
    with pytest.raises(RuntimeError, match='Python order planner'):
        validate_native_live_routing_policy_compatibility(cfg)


def test_candidate_config_validation_and_restart_only(tmp_path):
    from live.config import Config, _validate_config
    from strategy.maker_engine import MakerEngine
    from copy import deepcopy
    cfg = Config()
    path = tmp_path/'model.json'
    path.write_text(json.dumps(artifact(1)))
    cfg.strategy.response_action_policy_path = str(path)
    with pytest.raises(ValueError, match='both path'):
        _validate_config(cfg, validate_live_storage=False)
    cfg.strategy.response_action_policy_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match='execution_v1'):
        _validate_config(cfg, validate_live_storage=False)
    cfg.ml.feature_protocol = 'execution_v1'
    _validate_config(cfg, validate_live_storage=False)
    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    changed = deepcopy(cfg)
    changed.strategy.response_action_policy_sha256 = '0'*64
    with pytest.raises(ValueError, match='require restart'):
        engine.on_config_reload(changed)
