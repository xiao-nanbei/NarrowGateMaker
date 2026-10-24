import copy
import pickle

import numpy as np
import pytest

from models.backtest_tick import simulate_tick
from strategy.target_observation import TargetObservation
from strategy.target_variance import TargetVarianceState
from tests.exact_replay_assertions import assert_exact_replay_value
from tests.test_python_planned_maintenance_replay import _inputs, _params, _run, _async_fifo_params


def plain(x):
    if isinstance(x, np.ndarray):
        return x.tolist()
    if isinstance(x, dict):
        return {k: plain(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [plain(v) for v in x]
    return x


def exact(a, b):
    assert_exact_replay_value(plain(a), plain(b))


def publish(observer, version, price=100., ready=60, logical=20, side='BUY'):
    return observer.publish(side=side, call_id=str(version), price=price,
        observation_ns=ready * 1_000_000, logical_ns=logical * 1_000_000,
        trigger_kind='ordinary', stages=dict(entry_ts=0, capture_ts=20,
                                             request_ready_ts=ready, compute_mode='test'))


def test_future_watermarks_dedup_and_independent_pickle_restore():
    observer = TargetObservation()
    assert publish(observer, 1)
    assert publish(observer, 1) is None
    observer.advance(20_000_000)
    assert observer.applied_sequence == 0
    resumed = pickle.loads(pickle.dumps(observer))
    resumed.advance(60_000_000)
    assert resumed.applied_sequence == 1
    assert observer.applied_sequence == 0
    publish(resumed, 2, price=100., ready=61, logical=61)
    resumed.advance(61_000_000)
    assert resumed.sides['BUY'].positive_dt_count == 1
    assert resumed.sides['BUY'].variance_rate == 0
    assert not resumed.sides['SELL'].initialized


@pytest.mark.parametrize('split', [False, True])
@pytest.mark.parametrize('fill', [None, 500])
def test_observe_only_exact_business_and_timing(split, fill):
    params = dict(**_async_fifo_params(),
                  _decision_to_gateway_latency_samples_ms=[60.])
    if split:
        params['_pre_snapshot_compute_latency_samples_ms'] = [20.]
    fixed = _run(crossing_fill_ts_ms=fill, param_overrides=params)
    observed = _run(crossing_fill_ts_ms=fill,
                    param_overrides={**params, 'target_observe_only': True})
    observation = observed.pop('target_observation')
    exact(observed, fixed)
    assert observation['publication_seq'] > 0
    for row in observation['samples']:
        assert row['request_ready_ts'] - row['entry_ts'] == 60
        assert row['capture_ts'] - row['entry_ts'] == (20 if split else 0)
        assert row['observation_ns'] == row['request_ready_ts'] * 1_000_000


def test_business_checkpoint_preserves_pending_and_sequence(tmp_path):
    trades, bbo = _inputs()
    params = {**_params(), **_async_fifo_params(),
              '_decision_to_gateway_latency_samples_ms': [60.], 'target_observe_only': True}
    args = (trades, np.array([0], dtype=np.int64), np.array([1.]), params)
    complete = simulate_tick(*args, bbo_data=bbo)
    partial = simulate_tick(*args, bbo_data=bbo, checkpoint_at_ts_ms=40)
    checkpoint = pickle.loads(pickle.dumps(partial['_replay_checkpoint']))
    observer = checkpoint['runtime'].target_observation
    assert observer.sequence > observer.applied_sequence
    before = copy.deepcopy(observer.snapshot())
    resumed = simulate_tick(*args, bbo_data=bbo, resume_checkpoint=checkpoint)
    exact(resumed, complete)
    assert observer.snapshot() == before
    exact(simulate_tick(*args, bbo_data=bbo, resume_checkpoint=checkpoint), complete)


def test_same_time_sensitivity_is_not_epsilon_fixed():
    def run(delta):
        state = TargetVarianceState()
        for second in range(302):
            state.observe(price=100., ready_ns=second * 10**9, version=second)
        assert state.ready and state.mean_rate == state.variance_rate == 0
        state.observe(price=101., ready_ns=301*10**9+delta, version=302)
        state.observe(price=101., ready_ns=302*10**9, version=303)
        return state.variance_rate
    assert run(0) == 0
    assert run(1_000_000) > 0


@pytest.mark.parametrize('cut', [10, 20, 40, 999, 2001])
def test_stratified_compute_pause_resume_exact(cut):
    from tests.test_tick_runtime_checkpoint import scenario
    args, kwargs = scenario('compute')
    baseline = simulate_tick(*args, **kwargs)
    args[3]['target_observe_only'] = True
    full = simulate_tick(*args, **kwargs)
    observed = full['target_observation']
    exact({k:v for k,v in full.items() if k != 'target_observation'}, baseline)
    cp = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=cut)['_replay_checkpoint']
    restored = pickle.loads(pickle.dumps(cp))
    exact(simulate_tick(*args, **kwargs, resume_checkpoint=restored), full)
    assert all(x['capture_ts'] - x['entry_ts'] == 20 for x in observed['samples'])
    assert all(x['request_ready_ts'] - x['entry_ts'] == 40 for x in observed['samples'])


def test_live_synthetic_adapter_monotonic_and_single_side(monkeypatch):
    from types import SimpleNamespace
    from strategy.maker_engine import MakerEngine
    observer = TargetObservation(contract='target_pre_throttle_local_monotonic.v1')
    owner = SimpleNamespace(_target_observation=observer)
    monkeypatch.setattr('strategy.maker_engine.time.monotonic_ns', lambda: 1000000000)
    monkeypatch.setattr('strategy.maker_engine.time.time_ns', lambda: 987000000000)
    MakerEngine._observe_target_publications(owner, decision_group_id='one',
        targets=(('BUY',100.,True),('SELL',101.,False)), trigger_kind='terminal')
    assert observer.sequence == observer.applied_sequence == 1
    assert observer.samples[0]['observation_ns'] == 1000000000
    assert observer.samples[0]['utc_association_ns'] == 987000000000
    assert not observer.sides['SELL'].initialized
    # Re-reading the same frozen intent later is not a new publication or time.
    before = observer.snapshot()
    monkeypatch.setattr('strategy.maker_engine.time.monotonic_ns', lambda: 2000000000)
    MakerEngine._observe_target_publications(owner, decision_group_id='one',
        targets=(('BUY',100.,True),('SELL',101.,False)), trigger_kind='terminal')
    assert observer.snapshot() == before


def test_target_only_journal_checkpoint_delivery(tmp_path):
    from models.replay.l2_journal import ReplayL2Journal, audit_l2_delivery
    from tests.test_tick_runtime_checkpoint import scenario
    args, kwargs = scenario('compute')
    first = ReplayL2Journal(tmp_path/'first', identity={'test':'target_only'})
    args[3].update(target_observe_only=True, _target_journal=first)
    cp = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=30)['_replay_checkpoint']
    cp = pickle.loads(pickle.dumps(cp))
    second = ReplayL2Journal(tmp_path/'second', identity={'test':'target_only'})
    args[3]['_target_journal'] = second
    result = simulate_tick(*args, **kwargs, resume_checkpoint=cp)
    second.writer.close()
    receipt = audit_l2_delivery(second.writer.manifest_path, result['target_observation_delivery'])
    assert receipt['total'] == result['target_observation']['publication_seq']


@pytest.mark.parametrize('cut', [999, 2000, 2001])
def test_midnight_state_does_not_reset(cut):
    from dataclasses import replace
    from tests.test_tick_runtime_checkpoint import scenario
    args, kwargs = scenario('async')
    offset = 86400000 - 2000
    trades = args[0].copy()
    trades['transact_time'] += offset
    bbo = replace(kwargs['bbo_data'],ts_ms=kwargs['bbo_data'].ts_ms+offset)
    params = {**args[3], 'target_observe_only': True,
              'replay_event_clock_end_ts_ms': offset+4000}
    args = (trades,args[1]+offset,args[2],params)
    whole = simulate_tick(*args,bbo_data=bbo)
    cp = simulate_tick(*args,bbo_data=bbo,checkpoint_at_ts_ms=offset+cut)['_replay_checkpoint']
    exact(simulate_tick(*args,bbo_data=bbo,resume_checkpoint=cp),whole)


def test_real_continuation_routes_only_generated_side_and_keeps_business():
    from tests.test_directional_replace_threshold import replay
    params = {**_params(), **_async_fifo_params(), 'replace_terminal_continuation': True,
              'replace_cancel_first_exposure_increasing': True,
              'replace_min_interval_ms': 0, 'replace_min_price_change_ticks': 1}
    fixed = replay(params)
    observed = replay({**params,'target_observe_only':True})
    observation = observed.pop('target_observation')
    exact(observed,fixed)
    terminal = [r for r in observation['samples'] if r['trigger_kind']=='terminal']
    assert terminal
    # Each single-side continuation is one publication, not a synthetic pair.
    from collections import Counter
    counts = Counter(r['call_id'] for r in terminal)
    assert 1 in counts.values()


def test_fixed_grid_only_uses_visible_last_target_and_preserves_age():
    from strategy.target_observation_diagnostics import fixed_grid
    records=[dict(observation_ns=t,target_price=p,publication_seq=i)
             for i,(t,p) in enumerate([(0,100),(50,101),(100,102),(100,103),(250,104)])]
    rows=list(fixed_grid(records,period_ns=100))
    assert [(r['observation_ns'],r['target_price']) for r in rows]==[(0,100),(100,103),(200,103)]
    assert rows[-1]['target_age_ns']==100
    assert rows[-1]['source_publication_ns']==100
    assert rows[-1]['publication_seq']==3
    assert rows[-1]['planned_observation_seq']==3
    shifted=list(fixed_grid(records,period_ns=100,phase_ns=25))
    assert [(r['observation_ns'],r['target_price']) for r in shifted]==[(25,100),(125,103),(225,103)]


def test_observer_does_not_consume_rng():
    from tests.test_tick_runtime_checkpoint import scenario
    args,kwargs=scenario('compute')
    fixed=simulate_tick(*args,**kwargs,checkpoint_at_ts_ms=2001)['_replay_checkpoint']['runtime']
    args[3]['target_observe_only']=True
    observed=simulate_tick(*args,**kwargs,checkpoint_at_ts_ms=2001)['_replay_checkpoint']['runtime']
    names=[k for k,v in vars(fixed).items() if isinstance(v,np.random.Generator)]
    assert names
    for name in names:
        assert getattr(fixed,name).bit_generator.state==getattr(observed,name).bit_generator.state


@pytest.mark.parametrize('scenario',['pending','throttle','cancel_first','position_cap','min_notional','cross_zero'])
def test_live_actual_update_orders_observation_parity(monkeypatch,scenario):
    from strategy.maker_engine import MakerEngine
    from tests.test_live_replace_throttle import _run_order_action_planner_arm
    fixed=_run_order_action_planner_arm(monkeypatch,native=False,scenario=scenario)
    original=MakerEngine._update_orders
    observers=[]
    def observed(self,*args,**kwargs):
        self._target_observation=TargetObservation(contract='synthetic_live_monotonic.v1')
        snapshot=kwargs['quote_snapshot']
        snapshot.market_generation=1
        snapshot.depth_generation=1
        snapshot.book_ticker_generation=1
        result=original(self,*args,**kwargs)
        observers.append(self._target_observation)
        return result
    monkeypatch.setattr(MakerEngine,'_update_orders',observed)
    actual=_run_order_action_planner_arm(monkeypatch,native=False,scenario=scenario)
    assert actual==fixed
    assert len(observers)==1 and observers[0].sequence==1
    assert observers[0].samples[0]['side']=='BUY'
