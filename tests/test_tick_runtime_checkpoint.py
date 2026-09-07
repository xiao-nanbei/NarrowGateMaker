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
