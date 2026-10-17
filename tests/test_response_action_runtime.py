import pickle

import pytest

from strategy.response_action_runtime import ResponseActionRuntime
from tests.test_response_action_value import artifact, observation
from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_python_planned_maintenance_replay import _async_fifo_params
from tests.test_target_observation import exact


def config(path='TRADE_BOOK_RESPONSE_VALUE'):
    return dict(artifact=artifact(1), path_id=path, compute=dict(
        schema='response_compute.v1', measurement_receipt='synthetic-test-only',
        common_to_both_arms=True, event_ns=1000, call_ns=2_000_000))


def test_binding_single_consumption_and_common_budget():
    row = observation() | dict(call_sequence=3, side='BUY', order_id='o',
        old_price=100., target_price=100.1, old_quantity=.001)
    args = dict(sequence=3, side='BUY', order_id='o', old_price=100.,
                target_price=100.1, quantity=.001, baseline=False)
    first = ResponseActionRuntime(config())
    second = ResponseActionRuntime(config('RESPONSE_HISTORY_ABLATION'))
    assert first.select(row, **args)
    assert first.charge_ms(17) == second.charge_ms(17) == 3
    restored = pickle.loads(pickle.dumps(first))
    with pytest.raises(ValueError, match='twice'):
        restored.select(row, **args)
    with pytest.raises(ValueError, match='binding'):
        second.select(row, **(args | dict(quantity=.002)))


@pytest.mark.parametrize('stratified', [False, True])
def test_real_continuous_consumer_clock_restore_and_off_parity(synthetic_parent, tmp_path, stratified):  # noqa: F811
    from models.backtest_tick import simulate_prepared_inputs
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    prepared, base = synthetic_parent
    params = dict(base, **_async_fifo_params(), replace_terminal_continuation=True)
    if stratified:
        params.update(_runtime_compute_samples_by_path={k:[[1., 2., 1.]] for k in (
            'cached_no_new_bucket', 'new_bucket', 'catch_up')},
            runtime_compute_bucket_ms=1000, runtime_compute_initial_bucket_end_ms=0,
            runtime_compute_clock='source_time_assumption',
            _runtime_compute_sample_semantics='synthetic paired phases')
    else:
        params.update(_pre_snapshot_compute_latency_samples_ms=[1.],
                      _decision_to_gateway_latency_samples_ms=[2.])
    baseline = simulate_prepared_inputs(prepared, params)
    exact(baseline, simulate_prepared_inputs(prepared, dict(params, response_action_policy=None)))
    candidate = dict(params, response_action_policy=config())
    full = simulate_prepared_inputs(prepared, candidate)
    cp = simulate_prepared_inputs(prepared, candidate, checkpoint_at_ts_ms=10000)['_replay_checkpoint']
    save_runtime_checkpoint(tmp_path/'policy.checkpoint', cp)
    for consume in (False, True):
        exact(full, simulate_prepared_inputs(prepared, candidate,
            resume_checkpoint=load_trusted_runtime_checkpoint(tmp_path/'policy.checkpoint'),
            consume_resume_checkpoint=consume))
    counts = full['response_action_policy']['counts']
    assert counts['compute_calls'] > 0 and counts['charged_ms'] > 0
    assert counts['price_calls'] > 0
    rows = full['response_update_observation']['samples']
    for row in rows:
        assert row['compute']['response_extra_ms'] >= 2
        intent = row.get('response_model_intent')
        checked = row.get('actual_checks', {}).get('replace_throttle')
        if intent is not None and intent['value'] is None and checked is not None:
            assert checked['price_blocked'] == (
                checked['min_ticks'] > 0 and checked['price_delta_ticks']+1e-9 < checked['min_ticks'])
        for request in row.get('submitted_requests', []):
            assert request['request_ns'] >= row['compute']['request_ready_ts'] * 1_000_000


def test_missing_cost_or_wrong_execution_clock_rejected(synthetic_parent):  # noqa: F811
    from models.backtest_tick import simulate_prepared_inputs
    with pytest.raises(ValueError):
        ResponseActionRuntime(config() | dict(compute={}))
    prepared, params = synthetic_parent
    with pytest.raises(ValueError, match='split-compute'):
        simulate_prepared_inputs(prepared, dict(params, response_action_policy=config()))
