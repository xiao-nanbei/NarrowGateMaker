"""Full-event-loop persistence, distinct from drained daily accounting state."""

import math

import numpy as np
import pytest

from models.backtest_tick import simulate_tick
from models.replay.runtime_checkpoint_io import (
    load_trusted_runtime_checkpoint,
    save_runtime_checkpoint,
)
from models.tick_data_types import HistoricalExchangeBookEvent
from tests.test_python_planned_maintenance_replay import (
    _async_close_params,
    _async_fifo_params,
    _inputs,
    _params,
    _risk_policy_payload,
)


def assert_same(actual, expected):
    if isinstance(expected, np.ndarray):
        np.testing.assert_array_equal(actual, expected)
    elif isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            try:
                assert_same(actual[key], expected[key])
            except AssertionError as error:
                raise AssertionError(f"checkpoint result field {key}: {error}") from error
    elif isinstance(expected, (list, tuple)):
        assert type(actual) is type(expected)
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected, strict=True):
            assert_same(a, e)
    elif isinstance(expected, float) and math.isnan(expected):
        assert math.isnan(actual)
    else:
        assert actual == expected


def scenario(mode):
    trades, bbo = _inputs(crossing_fill_ts_ms=1_100)
    params = _params()
    if mode in {"async", "compute"}:
        params.update(_async_fifo_params())
        params["_private_fill_visibility_latency_samples_ms"] = [20.0]
    if mode == "compute":
        params.update({
            "_runtime_compute_samples_by_path": {
                key: [[20.0, 40.0, 5.0]]
                for key in ("cached_no_new_bucket", "new_bucket", "catch_up")
            },
            "runtime_compute_bucket_ms": 1_000,
            "runtime_compute_initial_bucket_end_ms": 0,
            "runtime_compute_clock": "source_time_assumption",
            "_runtime_compute_sample_semantics": "synthetic paired local phases",
        })
    if mode in {"timeout", "emergency", "close_replace"}:
        params.update(_async_close_params(cancel=(2.0, 11.0, 400.0)))
        params.update(requote_interval=0.1, rq_min=0.1, rq_max=0.1)
        if mode == "close_replace":
            bbo.best_bid[bbo.ts_ms >= 600] = 98.9
            bbo.best_ask[bbo.ts_ms >= 600] = 99.1
        else:
            params.update(circuit_breaker_sigma=0.0, initial_entry_price=100.0)
            params["_bulk_cancel_timing_samples_ms"] = [[4.0, 5.0, 6.0]]
            params["_bulk_cancel_timing_sample_semantics"] = "synthetic coupled batch phases"
            if mode == "timeout":
                params.update(position_timeout=0.5, requote_interval=1.0, rq_min=1.0, rq_max=1.0)
            else:
                params["emergency_close_dd"] = 0.005
                trades.loc[trades.transact_time >= 1_000, "price"] = 90.0
                bbo.best_bid[bbo.ts_ms >= 1_000] = 89.9
                bbo.best_ask[bbo.ts_ms >= 1_000] = 90.1
    return (trades, np.asarray([0]), np.asarray([1.0]), params), {"bbo_data": bbo}


@pytest.mark.parametrize("mode", ["ordinary", "async", "compute", "timeout", "emergency", "close_replace"])
@pytest.mark.parametrize("cut", [0, 1, 4, 20, 100, 299, 601, 1_005, 1_110, 1_121, 1_399, 2_001, 2_201, 3_999])
def test_saved_runtime_resumes_exact_order_fill_decision_and_accounting(mode, cut, tmp_path):
    args, kwargs = scenario(mode)
    expected = simulate_tick(*args, **kwargs)
    partial = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=cut)
    if partial.get("completed") is not False:
        # Some emergency-stop scenarios have already reached their real terminal.
        assert_same(partial, expected)
        return
    checkpoint = partial["_replay_checkpoint"]
    path = tmp_path / "runtime.pickle"
    save_runtime_checkpoint(path, checkpoint)
    loaded = load_trusted_runtime_checkpoint(path)
    actual = simulate_tick(*args, **kwargs, resume_checkpoint=loaded)
    assert_same(actual, expected)
    # Loading/resuming must not mutate the saved object or depend on a live frame.
    assert_same(simulate_tick(*args, **kwargs, resume_checkpoint=loaded), expected)


@pytest.mark.parametrize("mode", ["ordinary", "async", "compute", "timeout"])
def test_multiple_saved_cuts_do_not_force_cancel_or_flatten(mode, tmp_path):
    args, kwargs = scenario(mode)
    expected = simulate_tick(*args, **kwargs)
    checkpoint = None
    for cut in (1, 299, 601, 1_005, 1_110, 1_121, 1_399, 2_001, 2_201):
        partial = simulate_tick(*args, **kwargs, resume_checkpoint=checkpoint, checkpoint_at_ts_ms=cut)
        assert partial["completed"] is False
        path = tmp_path / "latest.pickle"
        save_runtime_checkpoint(path, partial["_replay_checkpoint"])
        checkpoint = load_trusted_runtime_checkpoint(path)
    assert_same(simulate_tick(*args, **kwargs, resume_checkpoint=checkpoint), expected)


@pytest.mark.parametrize("mode", ["E", "C", "EC"])
@pytest.mark.parametrize("cut", [1, 299, 1_005, 1_121, 2_201])
def test_bilateral_selector_and_diagnostics_survive_saved_runtime(mode, cut, tmp_path):
    args, kwargs = scenario("async")
    policy = {**_risk_policy_payload(), "selection_scope": "visible_inventory"}
    policy["models"]["E:SELL"]["intercept_usdc"] = .01
    args[3].update(risk_selection_scope="visible_inventory", risk_selection_mode=mode,
                   risk_selection_policy=policy, planned_quote_stop_ts_ms=0,
                   requote_threshold_bps=1.)
    expected = simulate_tick(*args, **kwargs)
    partial = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=cut)
    path = tmp_path / "selector.pickle"
    save_runtime_checkpoint(path, partial["_replay_checkpoint"])
    actual = simulate_tick(*args, **kwargs, resume_checkpoint=load_trusted_runtime_checkpoint(path))
    assert_same(actual, expected)


@pytest.mark.parametrize("cut", [0, 100, 250_100, 400_000, 450_050, 451_500])
def test_cold_signal_bar_completion_and_compute_survive_saved_state(cut, tmp_path):
    from tests.test_exec_book_visibility_delay import _message_schedule_replay_inputs

    inputs = _message_schedule_replay_inputs()
    origin = 1_000_000
    inputs["trades_df"]["transact_time"] = origin + inputs["trades_df"]["transact_time"] * 100
    # The fixture shares timestamp arrays; derive the new clock only once.
    source = origin + np.arange(5, dtype=np.int64) * 100_000
    for name in ("bbo_data", "l2_data"):
        inputs[name].ts_ms[:] = source
    inputs["var_ts_ms"] = source.copy()
    inputs["ml_data"] = (source.copy(), *inputs["ml_data"][1:])
    params = inputs["params"]
    for feed in params["_exec_message_delivery"].values():
        for clock in ("exchange_ts_ns", "receive_ts_ns", "feature_ready_ts_ns"):
            feed[clock] = origin * 1_000_000 + feed[clock] * 100
    params.update(_async_fifo_params())
    params.pop("planned_quote_stop_ts_ms", None)
    params.update(
        signal_cold_start=True, runtime_compute_initial_bucket_end_ms=None,
        runtime_compute_clock="prediction_delivery", runtime_compute_bucket_ms=10_000,
        _runtime_compute_samples_by_path={
            path: [[2., 4., 1.]] for path in ("cached_no_new_bucket", "new_bucket", "catch_up")
        },
        _runtime_compute_sample_semantics="synthetic paired compute phases",
        # This sparse clock fixture isolates signal readiness, not stale policy.
        max_exec_book_visible_age_s=1_000., max_exec_book_source_lag_s=1_000.,
        replay_event_clock_end_ts_ms=origin + 460_000,
    )
    expected = simulate_tick(**inputs)
    # Four parent callbacks are sufficient: live emits intervening zero-volume
    # bars. But 300 elapsed wall seconds alone cannot complete a received bar.
    assert expected["signal_first_bucket_ms"] == origin
    assert expected["signal_completed_bars_at_warmup"] == 400
    assert expected["signal_warmup_observed_ts_ms"] == origin + 450_100
    assert expected["_decision_trace"] and expected["_quote_trace"]
    assert min(row["ts_ms"] for row in expected["_decision_trace"]) >= origin + 451_000
    partial = simulate_tick(**inputs, checkpoint_at_ts_ms=origin + cut)
    checkpoint = partial["_replay_checkpoint"]
    if cut < 450_100:
        assert checkpoint["runtime"].runtime_compute_last_bucket_end_ms is None
        assert checkpoint["runtime"].signal_warmup_observed_ts_ms is None
        assert checkpoint["runtime"].ml_idx == -1
    path = tmp_path / "cold-start.pickle"
    save_runtime_checkpoint(path, checkpoint)
    assert_same(simulate_tick(**inputs, resume_checkpoint=load_trusted_runtime_checkpoint(path)), expected)
    if cut == 400_000:
        from copy import deepcopy
        cropped = deepcopy(inputs)
        cropped["ml_data"] = tuple(array[1:] for array in cropped["ml_data"])
        prediction = cropped["params"]["_exec_message_delivery"]["prediction"]
        for clock in prediction:
            prediction[clock] = prediction[clock][1:]
        actual = simulate_tick(**cropped, resume_checkpoint=load_trusted_runtime_checkpoint(path),
                               resume_input_batch=True)
        for output in (actual, expected):
            output.pop("exec_message_delivery_sources", None)
            output.pop("exec_message_delivery_input_semantics", None)
        assert_same(actual, expected)


def test_failed_checkpoint_write_keeps_last_complete_state(tmp_path, monkeypatch):
    import models.replay.runtime_checkpoint_io as module

    path = tmp_path / "latest.pickle"
    original = {"schema": "tick_replay_runtime.v1", "cut_ts_ms": 100}
    save_runtime_checkpoint(path, original)

    def fail(*_args, **_kwargs):
        raise OSError("injected full disk")

    monkeypatch.setattr(module, "_RuntimePickler", fail)
    with pytest.raises(OSError, match="full disk"):
        save_runtime_checkpoint(path, {**original, "cut_ts_ms": 200})
    assert load_trusted_runtime_checkpoint(path) == original
    assert list(tmp_path.iterdir()) == [path]


def test_native_book_iterator_is_reopened_without_losing_prefetched_events(tmp_path):
    args, kwargs = scenario("async")
    args[3]["exchange_book_queue_mode"] = "diagnostic"
    base = 1_700_000_000_000
    args[0]["transact_time"] += base
    args[1][:] += base
    kwargs["bbo_data"].ts_ms[:] += base
    args[3]["replay_event_clock_end_ts_ms"] += base
    events = [HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC", event_type="snapshot",
        exchange_ts_ns=(base - 100) * 1_000_000,
        local_receive_ts_ns=(base - 99) * 1_000_000,
        last_update_id=1,
        levels=(("bid", 960, 1.0), ("bid", 999, 1.0),
                ("ask", 1001, 1.0), ("ask", 1040, 1.0)),
    )]
    for number, offset in enumerate((300, 1_105, 1_105, 1_200, 2_000), start=2):
        events.append(HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC", event_type="snapshot",
            exchange_ts_ns=(base + offset) * 1_000_000,
            local_receive_ts_ns=(base + offset + 1) * 1_000_000,
            last_update_id=number,
            levels=(("bid", 960, number / 10), ("bid", 999, 1.0),
                    ("ask", 1001, 1.0), ("ask", 1040, number / 10)),
        ))
    class Tape:
        def __iter__(self):
            yield from events

    expected = simulate_tick(*args, **kwargs, exchange_book_event_tape=Tape())
    checkpoint = None
    for cut in (1, 299, 601, 1_005, 1_110, 1_399, 2_201):
        partial = simulate_tick(
            *args, **kwargs, exchange_book_event_tape=Tape(),
            resume_checkpoint=checkpoint, checkpoint_at_ts_ms=base + cut,
        )
        path = tmp_path / "with-book.pickle"
        save_runtime_checkpoint(path, partial["_replay_checkpoint"])
        checkpoint = load_trusted_runtime_checkpoint(path)
        scheduler = checkpoint["runtime"].exchange_book_scheduler
        assert scheduler._iterator is None
        assert scheduler.sequence.book is scheduler.book
    actual = simulate_tick(
        *args, **kwargs, exchange_book_event_tape=Tape(),
        resume_checkpoint=checkpoint,
    )
    assert_same(actual, expected)


def test_source_delivery_compute_and_private_callbacks_survive_saved_state(tmp_path):
    from tests.test_exec_book_visibility_delay import _profile_execution_message_fixture

    inputs, *_ = _profile_execution_message_fixture()
    inputs["params"].update(_async_fifo_params(new=(2.0, 5.0, 30.0), cancel=(2.0, 11.0, 40.0)))
    inputs["params"]["_private_fill_visibility_latency_samples_ms"] = [35.0]
    expected = simulate_tick(**inputs)
    start = int(inputs["trades_df"].transact_time.iloc[0])
    checkpoint = None
    for offset in (1, 35, 101, 301):
        partial = simulate_tick(**inputs, resume_checkpoint=checkpoint, checkpoint_at_ts_ms=start + offset)
        if partial.get("completed") is not False:
            assert_same(partial, expected)
            break
        path = tmp_path / "source-delivery.pickle"
        save_runtime_checkpoint(path, partial["_replay_checkpoint"])
        checkpoint = load_trusted_runtime_checkpoint(path)
        assert_same(simulate_tick(**inputs, resume_checkpoint=checkpoint), expected)


def test_progress_hook_is_rebound_instead_of_persisting_a_process_closure(tmp_path):
    args, kwargs = scenario("async")
    observed = []
    args[3]["_replay_progress_callback"] = lambda *a, **kw: observed.append((a, kw))
    args[3]["_replay_progress_interval_events"] = 1
    expected = simulate_tick(*args, **kwargs)
    partial = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=601)
    path = tmp_path / "progress.pickle"
    save_runtime_checkpoint(path, partial["_replay_checkpoint"])
    checkpoint = load_trusted_runtime_checkpoint(path)
    assert checkpoint["runtime"].replay_progress_callback is None
    observed.clear()
    actual = simulate_tick(*args, **kwargs, resume_checkpoint=checkpoint)
    assert_same(actual, expected)
    assert observed


def test_resume_rejects_a_different_window_or_nonadvancing_cut():
    args, kwargs = scenario("async")
    checkpoint = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=601)["_replay_checkpoint"]
    with pytest.raises(ValueError, match="advance"):
        simulate_tick(*args, **kwargs, resume_checkpoint=checkpoint, checkpoint_at_ts_ms=601)
    args[3]["replay_event_clock_end_ts_ms"] += 100
    with pytest.raises(ValueError, match="same loaded window"):
        simulate_tick(*args, **kwargs, resume_checkpoint=checkpoint)
