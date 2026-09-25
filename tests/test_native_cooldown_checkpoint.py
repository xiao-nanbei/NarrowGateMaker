"""Native callback state persistence; synthetic mechanics, not economic proof."""
import copy
import json
from pathlib import Path
import subprocess
import sys

import narrowgate_cpp as cpp
import pytest

from models.replay.runtime_checkpoint_io import (
    clone_runtime_state, save_runtime_checkpoint, load_trusted_runtime_checkpoint,
)
from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy
from test_cpp_signal_features import _sell_cooldown_evaluator
from test_boolean_cooldown_buy_e3 import _artifact


def observe(policy, ns, mid=100.):
    policy.observe_depth(receive_ts_ns=ns, bids=((mid-.1, 1.),),
                         asks=((mid+.1, 1.),), market_generation=1, depth_generation=1)


@pytest.fixture(params=['SELL', 'BUY'])
def policy(request, tmp_path, monkeypatch):
    monkeypatch.setenv('NARROWGATE_CPP_COOLDOWN', '1')
    monkeypatch.setenv('NARROWGATE_CPP_STRICT', '1')
    if request.param == 'BUY':
        value, _ = _artifact(tmp_path)
    else:
        value = LiveBooleanCooldownPolicy(evaluator=_sell_cooldown_evaluator(),
                    warmup_s=.2, max_feature_age_s=1., native_runtime=True)
    assert value._native_hot_path is not None
    return value


def decision(policy, ns):
    side = 'SELL' if type(policy) is LiveBooleanCooldownPolicy else 'BUY'
    value = policy.evaluate(side=side, baseline_duration_ms=170000,
                           campaign_age_s=300., decision_ts_ns=ns, snapshot_id='checkpoint-test')
    return (value.duration_ms, value.action_id, value.fallback_reason)


@pytest.mark.parametrize('cut', [1, 4, 8])
def test_native_complete_state_persisted_fork_and_continuation(policy, tmp_path, cut):
    events = [(100000000*i+7, 100.+(-1)**i*i) for i in range(1, 10)]
    for ns, mid in events[:cut]:
        observe(policy, ns, mid)
        decision(policy, ns+1)
    snapshot = policy._native_hot_path.export_state()
    path = tmp_path/'state.pickle'
    save_runtime_checkpoint(path, {'schema':'tick_replay_runtime.v1', 'policy':policy})
    left = load_trusted_runtime_checkpoint(path)['policy']
    right = load_trusted_runtime_checkpoint(path)['policy']
    assert left._lock is not right._lock
    assert left._native_hot_path.export_state() == snapshot
    assert left._evaluations == policy._evaluations
    for ns, mid in events[cut:]:
        for value in (policy, left):
            observe(value, ns, mid)
        assert decision(policy, ns+1) == decision(left, ns+1)
        assert left._native_hot_path.export_state() == policy._native_hot_path.export_state()
    assert right._native_hot_path.export_state() == snapshot
    assert right._evaluations == cut


def test_native_cross_process_restore_and_callback(policy, tmp_path):
    for i in range(1, 8):
        observe(policy, i*100000000+7, 100.+(-1)**i)
    path = tmp_path/'process.pickle'
    save_runtime_checkpoint(path, {'schema':'tick_replay_runtime.v1', 'policy':policy})
    code = '''
import importlib.util, json, sys
spec=importlib.util.spec_from_file_location('narrowgate_cpp', sys.argv[1])
cpp=importlib.util.module_from_spec(spec); spec.loader.exec_module(cpp)
sys.modules['narrowgate_cpp']=cpp
from models.replay.runtime_checkpoint_io import load_trusted_runtime_checkpoint
p=load_trusted_runtime_checkpoint(sys.argv[2])['policy']
p.observe_depth(receive_ts_ns=800000007,bids=((101.9,1.),),asks=((102.1,1.),),market_generation=1,depth_generation=1)
print(json.dumps(p._native_hot_path.export_state(),sort_keys=True))
'''
    output = subprocess.check_output([sys.executable, '-c', code, cpp.__file__, str(path)],
                                     cwd=Path(__file__).resolve().parents[1], text=True)
    observe(policy, 800000007, 102.)
    assert json.loads(output) == policy._native_hot_path.export_state()


def test_native_gap_invalid_out_of_order_and_reset_state(policy):
    for ns in (100000007, 200000007, 600000007, 500000007):
        observe(policy, ns)
    observe(policy, 700000007, float('nan'))
    snapshot = policy._native_hot_path.export_state()
    assert snapshot['audit']['gap_windows'] > 0
    assert snapshot['audit']['out_of_order_updates'] == 1
    assert snapshot['audit']['invalid_updates'] == 1
    restored = clone_runtime_state(policy)
    assert restored._native_hot_path.export_state() == snapshot
    for value in (policy, restored):
        observe(value, 3000000007)
    assert restored._native_hot_path.export_state() == policy._native_hot_path.export_state()
    assert policy._native_hot_path.export_state()['audit']['gap_resets'] == 1


@pytest.mark.parametrize('field,value', [('version',2),('configuration','wrong'),
    ('ema',[1.]),('effective_sign',[3]*45),('pending_mid',float('nan'))])
def test_native_rejects_invalid_state_without_mutating(policy, field, value):
    observe(policy, 100000007)
    before = policy._native_hot_path.export_state()
    damaged = copy.deepcopy(before); damaged[field] = value
    with pytest.raises((ValueError, TypeError, RuntimeError)):
        policy._native_hot_path.restore_state(damaged)
    assert policy._native_hot_path.export_state() == before


def test_policy_checkpoint_rejects_in_progress_callback(policy):
    with policy._lock:
        with pytest.raises(RuntimeError, match='callback owns'):
            clone_runtime_state(policy)


def test_admitted_ema_cross_and_derivatives_survive_restore(policy):
    # BUY's real 2,048,000 ms warmup is not shortened for this mechanics test.
    import math
    for i in range(1, 20520):
        observe(policy, i*100000000+7, 100.+math.sin(i/37.))
    assert policy._native_hot_path.export_state()['audit']['warmup_admitted']
    restored = clone_runtime_state(policy)
    for i in range(20520, 20530):
        for p in (policy, restored):
            observe(p, i*100000000+7, 100.+math.sin(i/37.))
        assert decision(policy, i*100000000+8) == decision(restored, i*100000000+8)
        assert restored._native_hot_path.export_state() == policy._native_hot_path.export_state()


def test_actual_tick_loop_restores_native_adapter_account_outputs(policy, tmp_path):
    import numpy as np
    from models.backtest_tick import simulate_tick
    from models.exchange_book_replay import ReceiveTimeCooldownReplayAdapter, HistoricalMessageDeliverySchedule
    from models.tick_data_types import HistoricalL2Data
    from test_tick_runtime_checkpoint import scenario, assert_same
    ts = np.arange(50, 4050, 100, dtype=np.int64)
    bid = np.full((len(ts), 1), 99.9)
    ask = np.full((len(ts), 1), 100.1)
    depth = HistoricalL2Data(ts, bid, np.ones_like(bid), ask, np.ones_like(ask))
    schedule = HistoricalMessageDeliverySchedule(ts*1000000, ts*1000000, ts*1000000)
    side = 'SELL' if type(policy) is LiveBooleanCooldownPolicy else 'BUY'
    adapter = ReceiveTimeCooldownReplayAdapter(depth, schedule, policies={side:policy})
    args, kwargs = scenario('ordinary')
    args[3]['cooldown_duration_policy_evaluator'] = clone_runtime_state(adapter)
    args[3]['cooldown_v2_snapshot_emitter'] = args[3]['cooldown_duration_policy_evaluator']
    expected = simulate_tick(*args, **kwargs)
    args[3]['cooldown_duration_policy_evaluator'] = clone_runtime_state(adapter)
    args[3]['cooldown_v2_snapshot_emitter'] = args[3]['cooldown_duration_policy_evaluator']
    checkpoint = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=1005)['_replay_checkpoint']
    path = tmp_path/'account.pickle'
    save_runtime_checkpoint(path, checkpoint)
    actual = simulate_tick(*args, **kwargs, resume_checkpoint=load_trusted_runtime_checkpoint(path))
    assert_same(actual, expected)


def test_public_checkpoint_binds_native_cooldown_inputs_not_progress(policy):
    import numpy as np
    from models.exchange_book_replay import ReceiveTimeCooldownReplayAdapter, HistoricalMessageDeliverySchedule
    from models.tick_data_types import HistoricalL2Data
    from models.replay.runtime_checkpoint_io import public_checkpoint_binding

    ts = np.array([100, 200], dtype=np.int64)
    bids = np.array([[99.], [100.]])
    depth = HistoricalL2Data(ts, bids, np.ones_like(bids), bids + 2, np.ones_like(bids))
    clocks = ts * 1_000_000
    side = 'SELL' if type(policy) is LiveBooleanCooldownPolicy else 'BUY'
    adapter = ReceiveTimeCooldownReplayAdapter(
        depth, HistoricalMessageDeliverySchedule(clocks, clocks, clocks), policies={side: policy})
    params = {'cooldown_duration_policy_evaluator': adapter, 'cooldown_v2_snapshot_emitter': adapter}
    before = public_checkpoint_binding('input', params, None)
    adapter._cursor = 1
    observe(policy, 100_000_007)
    assert public_checkpoint_binding('input', params, None) == before
    policy.windows.max_feature_age_s += 1
    assert public_checkpoint_binding('input', params, None) != before
    policy.windows.max_feature_age_s -= 1
    bids[0, 0] += .5
    assert public_checkpoint_binding('input', params, None) != before


def test_preflight_rejects_incapable_native_before_execution(policy, monkeypatch):
    from types import SimpleNamespace
    from models.replay.runtime_checkpoint_io import validate_native_cooldown_checkpoint
    monkeypatch.setattr(policy, '_native_hot_path', SimpleNamespace())
    with pytest.raises(TypeError, match='complete checkpoint capability'):
        validate_native_cooldown_checkpoint(policy)
