from __future__ import annotations

from typing import Any

import pytest

from strategy.signal import (
    Bar1s,
    Prediction,
    SIGNAL_COMPUTE_PHASE_FIELDS,
    SignalEngine,
)


def _engine_ready_for_signal() -> SignalEngine:
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    engine._bar_buffer.extend(
        Bar1s(ts=second * 1_000, close=100.0)
        for second in range(30)
    )
    return engine


def _assert_accounted(timings: dict[str, Any]) -> None:
    assert all(float(timings[name]) >= 0.0 for name in SIGNAL_COMPUTE_PHASE_FIELDS)
    assert timings["signal_compute_accounted_us"] == pytest.approx(
        sum(float(timings[name]) for name in SIGNAL_COMPUTE_PHASE_FIELDS)
    )


def test_compute_signal_cached_path_returns_same_prediction_without_prediction_or_commit() -> None:
    engine = _engine_ready_for_signal()
    cached = Prediction(ts=123.0, dir_10s=0.61)
    engine._last_prediction = cached
    engine._process_completed_feature_buckets_locked = lambda _bars: []
    timings: dict[str, Any] = {}

    actual = engine.compute_signal(perf_timings=timings)

    assert actual is cached
    assert engine._last_prediction is cached
    assert timings["signal_compute_path"] == "cached_no_new_bucket"
    assert timings["signal_compute_bucket_count"] == 0
    assert timings["signal_compute_prediction_us"] == 0.0
    assert timings["signal_compute_commit_lock_wait_us"] == 0.0
    assert timings["signal_compute_commit_us"] == 0.0
    _assert_accounted(timings)


@pytest.mark.parametrize(
    ("bucket_count", "expected_path"),
    [(1, "new_bucket"), (3, "catch_up")],
)
def test_compute_signal_new_and_catch_up_paths_preserve_order_and_commit_once(
    bucket_count: int,
    expected_path: str,
) -> None:
    engine = _engine_ready_for_signal()
    feature_batches = [{"sequence": index} for index in range(bucket_count)]
    seen: list[dict[str, int]] = []
    predictions = [
        Prediction(ts=float(index), dir_10s=0.5 + index * 0.01)
        for index in range(bucket_count)
    ]
    engine._process_completed_feature_buckets_locked = lambda _bars: feature_batches

    def predict(features: dict[str, int]) -> Prediction:
        seen.append(features)
        return predictions[len(seen) - 1]

    engine._predict = predict
    timings: dict[str, Any] = {}

    actual = engine.compute_signal(perf_timings=timings)

    assert seen == feature_batches
    assert actual is predictions[-1]
    assert engine._last_prediction is predictions[-1]
    assert timings["signal_compute_path"] == expected_path
    assert timings["signal_compute_bucket_count"] == bucket_count
    _assert_accounted(timings)


def test_compute_signal_snapshot_feature_exception_propagates_and_closes_partial_spans() -> None:
    engine = _engine_ready_for_signal()

    def fail(_bars):
        raise RuntimeError("feature failure")

    engine._process_completed_feature_buckets_locked = fail
    timings: dict[str, Any] = {}

    with pytest.raises(RuntimeError, match="feature failure"):
        engine.compute_signal(perf_timings=timings)

    assert timings["signal_compute_path"] == "unknown"
    assert timings["signal_compute_bucket_count"] == 0
    assert timings["signal_compute_prediction_us"] == 0.0
    assert timings["signal_compute_commit_us"] == 0.0
    _assert_accounted(timings)


def test_compute_signal_prediction_exception_propagates_without_committing() -> None:
    engine = _engine_ready_for_signal()
    cached = Prediction(ts=123.0, dir_10s=0.61)
    engine._last_prediction = cached
    engine._process_completed_feature_buckets_locked = lambda _bars: [{"sequence": 0}]

    def fail(_features):
        raise RuntimeError("model failure")

    engine._predict = fail
    timings: dict[str, Any] = {}

    with pytest.raises(RuntimeError, match="model failure"):
        engine.compute_signal(perf_timings=timings)

    assert engine._last_prediction is cached
    assert timings["signal_compute_path"] == "new_bucket"
    assert timings["signal_compute_bucket_count"] == 1
    assert timings["signal_compute_commit_us"] == 0.0
    _assert_accounted(timings)


def test_compute_signal_without_timing_sink_keeps_clock_free_cached_fast_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    cached = Prediction(ts=123.0, dir_10s=0.61)
    engine._last_prediction = cached

    def unexpected_clock() -> int:
        raise AssertionError("uninstrumented compute_signal must not read perf clock")

    monkeypatch.setattr("strategy.signal.time.perf_counter_ns", unexpected_clock)

    assert engine.compute_signal() is cached
