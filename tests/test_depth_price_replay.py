import pickle
import os
import subprocess
import sys

import pytest

from models.backtest_tick import simulate_prepared_inputs
from models.replay.runtime_checkpoint_io import load_trusted_runtime_checkpoint, save_runtime_checkpoint
from research.families.f08_side_taker_lifecycle.depth_price_replay import (
    DeliveredDepthCursor, DepthPriceReplayRuntime,
)
from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_python_planned_maintenance_replay import _async_fifo_params
from tests.test_target_observation import exact


def runtime(u=0., v=0., cost=0):
    return DepthPriceReplayRuntime(arm='PRICE_ONLY', tick_size=.1, constant=(u, v),
        cost_profile=dict(measurement_id='synthetic-test-only', publication_ns=0, query_ns=cost))


def clocks():
    return dict(_async_fifo_params(), _pre_snapshot_compute_latency_samples_ms=[1.],
                _decision_to_gateway_latency_samples_ms=[2.])


def test_actual_executor_zero_policy_exact_and_default_off(synthetic_parent):  # noqa: F811
    prepared, base = synthetic_parent
    params = dict(base, **clocks())
    old = simulate_prepared_inputs(prepared, params)
    exact(old, simulate_prepared_inputs(prepared, dict(params, _quote_price_runtime=None)))
    zero = simulate_prepared_inputs(prepared, dict(params, _quote_price_runtime=runtime()))
    audit = zero.pop('quote_price_policy')
    # Cost disclosure is attached to the compute clock, not an altered quote.
    for row in zero.get('response_update_observation', {}).get('samples', []):
        row.get('compute', {}).pop('quote_price_extra_ms', None)
    exact(old, zero)
    assert audit['counts']['queries'] > 0
    assert audit['counts']['charged_ms'] == 0


def test_nonzero_policy_changes_orders_cost_delays_requests_and_cold_restore(synthetic_parent, tmp_path):  # noqa: F811
    prepared, base = synthetic_parent
    params = dict(base, **clocks(), trace_quotes_max=10000)
    zero = simulate_prepared_inputs(prepared, params)
    policy = runtime(.1, .1, 2_100_000)
    candidate = dict(params, _quote_price_runtime=policy)
    full = simulate_prepared_inputs(prepared, candidate)
    assert [(r['side'], r['price']) for r in full['_quote_trace']] != [
        (r['side'], r['price']) for r in zero['_quote_trace']]
    audit = full['quote_price_policy']['counts']
    assert audit['nonzero_raw_corrections'] > 0
    assert audit['charged_ms'] >= 3*audit['captures']
    assert not policy.counts  # No mutable state is shared with the caller.
    rows = full['response_update_observation']['samples']
    for row in rows:
        assert row['compute']['quote_price_extra_ms'] >= 3
        for request in row.get('submitted_requests', []):
            assert request['request_ns'] >= row['compute']['request_ready_ts']*1_000_000
    cp = simulate_prepared_inputs(prepared, candidate, checkpoint_at_ts_ms=10000)['_replay_checkpoint']
    save_runtime_checkpoint(tmp_path/'policy', cp)
    restored = load_trusted_runtime_checkpoint(tmp_path/'policy')
    exact(full, simulate_prepared_inputs(prepared, candidate, resume_checkpoint=restored))
    with pytest.raises(ValueError, match='changed|identity'):
        simulate_prepared_inputs(prepared, dict(candidate, _quote_price_runtime=runtime(-.1, .1)),
                                 resume_checkpoint=restored)


def test_wrong_clock_cost_and_double_charge_fail_closed(synthetic_parent):  # noqa: F811
    prepared, params = synthetic_parent
    with pytest.raises(ValueError, match='split-compute'):
        simulate_prepared_inputs(prepared, dict(params, _quote_price_runtime=runtime()))
    with pytest.raises(ValueError, match='measured'):
        DepthPriceReplayRuntime(arm='PRICE_ONLY', tick_size=.1, constant=(0, 0), cost_profile={})
    p = runtime(cost=1)
    p.begin_capture(1)
    p.correction()
    assert p.finish_capture() == 1
    with pytest.raises(ValueError, match='already charged'):
        p.finish_capture()


def test_delivered_cursor_future_exclusion_and_restore_without_duplicate(monkeypatch):
    from tests.test_depth_observation import stream
    import research.families.f08_side_taker_lifecycle.depth_price_replay as module
    monkeypatch.setattr(module, 'streams', lambda config, tap: stream(tap))
    first = DeliveredDepthCursor({})
    rows = list(first.advance_to(1_000_000_000))
    assert all(o.original.ready_ns <= first.clock for o in rows)
    restored = pickle.loads(pickle.dumps(first))
    left = list(first.advance_to(5_000_000_000))
    right = list(restored.advance_to(5_000_000_000))
    assert left == right
    assert not {o.original.event_id for o in rows} & {o.original.event_id for o in left}
    with pytest.raises(ValueError, match='regressed'):
        first.advance_to(0).__next__()


def test_depth_state_cold_process_hash_seed_independent(tmp_path):
    from tests.test_depth_observation import stream
    from data.depth_observation import DepthStateTap
    snapshot = next(o.snapshot for t in stream(DepthStateTap()) for o in t.research_depth_observations)
    path = tmp_path/'snapshot'
    path.write_bytes(pickle.dumps(snapshot))
    code = '''
import pickle, sys
from pathlib import Path
s = pickle.loads(Path(sys.argv[1]).read_bytes())
for side in ('bid', 'ask'):
    for price, qty in s.points(side):
        assert s.quantity(side, price) == qty, (side, price)
'''
    for seed in ('1', '17'):
        subprocess.run([sys.executable, '-c', code, str(path)], check=True,
                       env=dict(os.environ, PYTHONHASHSEED=seed))
