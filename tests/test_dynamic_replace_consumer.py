import copy
import pickle

import pytest

from strategy.replace_threshold import ReplacePriceThreshold
from strategy.target_observation import TargetObservation
from strategy.target_variance import dynamic_outward_ticks
from tests.test_target_observation import exact
from tests.test_directional_replace_threshold import replay, clean
from tests.test_python_planned_maintenance_replay import _params, _async_fifo_params
from tests.test_live_replace_throttle import _engine, _order
from strategy.order_manager import Side


def observer():
    o = TargetObservation()
    for side in ('BUY', 'SELL'):
        for second in range(302):
            o.sides[side].observe(price=100 + (second % 2), ready_ns=second*10**9, version=second)
    o.sequence = o.applied_sequence = 301
    return o


@pytest.mark.parametrize('mode', ['dynamic_outward', 'static_outward'])
@pytest.mark.parametrize('side,sign', [('BUY', 1), ('SELL', -1)])
def test_ready_only_outward_and_future_not_consumed(mode, side, sign):
    p = ReplacePriceThreshold(mode, outward_r=2., static_ticks=37)
    o = TargetObservation()
    args = dict(ordinary=True, side=side, old_price=100., target_price=100.-sign,
                fixed=15, tick_size=.1, observation=o)
    assert p.select(**args) == 15
    o = observer(); args['observation'] = o
    expected = 37 if mode == 'static_outward' else dynamic_outward_ticks(
        variance_rate=o.sides[side].variance_rate, r=2., tick_size=.1)
    assert p.select(**args) == expected
    before = copy.deepcopy(o.sides[side].snapshot())
    o.publish(side=side, call_id='future', price=200, observation_ns=400*10**9,
              logical_ns=302*10**9, trigger_kind='ordinary', stages={'compute_mode':'test'})
    for _ in range(3):
        assert p.select(**args) == expected
    assert o.sides[side].snapshot() == before
    assert len(o.pending) == 1
    assert p.select(**{**args, 'ordinary':False}) == 15
    assert p.select(**{**args, 'target_price':100.+sign}) == 15
    assert p.select(**{**args, 'target_price':100.}) == 15
    restored = pickle.loads(pickle.dumps((p,o)))
    assert restored[0].select(**{**args,'observation':restored[1]}) == expected
    restored[1].advance(400*10**9)
    assert not restored[1].sides[side].ready  # gap resets, not a zero variance
    assert restored[0].select(**{**args,'observation':restored[1]}) == 15
    assert o.sides[side].ready


@pytest.mark.parametrize('mode', ['dynamic_outward', 'static_outward'])
def test_real_gate_time_force_quantity_and_roles(mode):
    e = _engine()
    e.cfg.strategy.replace_price_threshold_mode = mode
    e.cfg.strategy.replace_outward_r = 2.
    e.cfg.strategy.replace_outward_static_ticks = 37
    e.cfg.strategy.replace_min_price_change_ticks = 15
    e.cfg.strategy.replace_min_price_change_ticks_reducing = 15
    e.cfg.strategy.replace_min_interval_ms = 1000
    e._target_observation = observer()
    order = _order(Side.BUY,100,2000)
    args = dict(side=Side.BUY, now_ts=order.create_time+2, q=0,
                target_price=90, order=order, needs_update=True, force_update=False)
    assert e._apply_replace_throttle(**args)
    assert not e._apply_replace_throttle(**{**args,'now_ts':order.create_time+.1})
    assert e._apply_replace_throttle(**{**args,'now_ts':order.create_time,'force_update':True})
    assert not e._apply_replace_throttle(**{**args,'needs_update':False,'force_update':True})
    assert e._apply_replace_throttle(**{**args,'order':None})
    assert not e._apply_replace_throttle(**{**args,'target_price':100})
    assert not e._apply_replace_throttle(**{**args,'target_price':101.2})
    assert not e._apply_replace_throttle(**{**args,'q':-1,'target_price':98.8})


@pytest.mark.parametrize('mode', ['dynamic_outward','static_outward'])
@pytest.mark.parametrize('cut', [20,40,1500])
def test_replay_warmup_business_rng_and_checkpoint(mode, cut):
    params = {**_params(), **_async_fifo_params(), 'planned_quote_stop_ts_ms':0,
        'replace_min_price_change_ticks':15, 'replace_min_price_change_ticks_reducing':15,
        'replace_terminal_continuation':True,
        '_pre_snapshot_compute_latency_samples_ms':[20.],
        '_decision_to_gateway_latency_samples_ms':[60.]}
    baseline = replay(params)
    candidate = {**params, 'replace_price_threshold_mode':mode,
                 'replace_outward_r':2., 'replace_outward_static_ticks':37}
    full = replay(candidate)
    business = {k:v for k,v in full.items() if k not in (
        'target_observation','replace_outward_r','replace_outward_static_ticks')}
    exact(clean(business),clean(baseline))
    cp = replay(candidate, cut=cut)['_replay_checkpoint']
    restored = pickle.loads(pickle.dumps(cp))
    exact(replay(candidate, checkpoint=restored), full)
    assert restored['runtime'].replace_price_threshold.mode == mode


@pytest.mark.parametrize('kwargs', [dict(outward_r=0),dict(outward_r=float('nan')),
                                  dict(outward_r=float('inf'))])
def test_invalid_dynamic(kwargs):
    with pytest.raises(ValueError):
        ReplacePriceThreshold('dynamic_outward', **kwargs)


@pytest.mark.parametrize('ticks', [0, -1, 1.5, True])
def test_invalid_static(ticks):
    with pytest.raises(ValueError):
        ReplacePriceThreshold('static_outward', static_ticks=ticks)


@pytest.mark.parametrize('mode', ['dynamic_outward', 'static_outward'])
def test_ready_runtime_consumer_survives_checkpoint_and_single_side_continuation(mode):
    params = {**_params(), **_async_fifo_params(), 'planned_quote_stop_ts_ms':0,
        'replace_min_price_change_ticks':15, 'replace_min_price_change_ticks_reducing':15,
        'replace_terminal_continuation':True, 'replace_price_threshold_mode':mode,
        'replace_outward_r':.000002, 'replace_outward_static_ticks':3}
    cp = replay(params, cut=40)['_replay_checkpoint']
    # Synthetic pre-warmed state, not a production restart from another policy.
    o = cp['runtime'].target_observation
    for state in o.sides.values():
        state.initialized = True
        state.last_target_price = 100.
        state.last_target_ready_time = 0
        state.last_target_version = 0
        state.valid_span = 300.
        state.positive_dt_count = 2
        state.variance_rate = 1.
    a = replay(params, checkpoint=cp)
    b = replay(params, checkpoint=pickle.loads(pickle.dumps(cp)))
    exact(a,b)
    groups = [v for k,v in a['directional_replace_counts'].items() if k.endswith(':ordinary:outward')]
    assert sum(v.get('estimator_ready',0) for v in groups) > 0
    if mode == 'static_outward':
        assert all(v['threshold_min'] == v['threshold_max'] == 3 for v in groups)
    assert a['replace_terminal_continuation_decision_count'] > 0
