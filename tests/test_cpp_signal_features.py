from types import SimpleNamespace

import numpy as np
import pytest

import strategy.signal as signal_module
from live.config import Config
from strategy.maker_engine import MakerEngine
from strategy.signal import (
    EXECUTION_L2_FEATURE_COLS,
    EXECUTION_L2_POLICY_METRIC_COLS,
    Bar1s,
    DepthSnapshot,
    QuoteDepthObservation,
    SignalEngine,
)

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")


def _depth_snapshot(ts: float, index: int, *, depth: int = 10) -> DepthSnapshot:
    mid = 60_000.0 + (index % 7) * 0.1
    return DepthSnapshot(
        ts=ts,
        bids=[
            (mid - 0.1 * level, 0.1 + level * 0.01 + (index % 5) * 0.001)
            for level in range(1, depth + 1)
        ],
        asks=[
            (mid + 0.1 * level, 0.11 + level * 0.012 + (index % 3) * 0.001)
            for level in range(1, depth + 1)
        ],
    )


def _python_execution_l2_values(
    snapshots: list[DepthSnapshot],
    bucket_end_ms: int,
) -> np.ndarray:
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal_features_enabled = False
    engine._depth_history.extend(snapshots)
    features: dict[str, float] = {}
    engine._compute_execution_l2_features(features, bucket_end_ms)
    return np.asarray(
        [features[name] for name in EXECUTION_L2_FEATURE_COLS],
        dtype=np.float64,
    )


def _native_execution_l2_values(
    snapshots: list[DepthSnapshot],
    bucket_end_ms: int,
) -> np.ndarray:
    return np.asarray(
        narrowgate_cpp.compute_signal_execution_l2_feature_values(
            snapshots,
            bucket_end_ms,
        ),
        dtype=np.float64,
    )


def _python_l2_policy_values(
    snapshots: list[DepthSnapshot],
    end_exchange_ms: float,
) -> np.ndarray:
    quote_snapshot = _policy_quote_snapshot(snapshots, end_exchange_ms)
    signal = SignalEngine(enable_ml=False)
    signal._cpp_signal_features_enabled = False
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.signal = signal
    metrics = engine._current_l2_policy_metrics(60_000.0, quote_snapshot)
    return np.asarray(
        [metrics[name] for name in EXECUTION_L2_POLICY_METRIC_COLS],
        dtype=np.float64,
    )


def _policy_quote_snapshot(
    snapshots: list[DepthSnapshot],
    end_exchange_ms: float,
):
    history = tuple(
        QuoteDepthObservation(
            exchange_ts_ms=int(item.ts),
            receive_ts_ns=int(item.ts * 1_000_000),
            bids=tuple(item.bids),
            asks=tuple(item.asks),
        )
        for item in snapshots
    )
    latest = snapshots[-1] if snapshots else DepthSnapshot()
    return SimpleNamespace(
        depth_visible_age_s=0.0,
        bids=tuple(latest.bids),
        asks=tuple(latest.asks),
        depth_history=history,
        depth_exchange_ts_ms=int(end_exchange_ms),
    )


def _native_l2_policy_values(
    snapshots: list[DepthSnapshot],
    end_exchange_ms: float,
) -> np.ndarray:
    return np.asarray(
        narrowgate_cpp.compute_signal_execution_l2_policy_metric_values(
            snapshots,
            end_exchange_ms,
        ),
        dtype=np.float64,
    )


def test_cpp_execution_l2_batch_matches_python_exactly_at_102_snapshots() -> None:
    bucket_end_ms = 1_000_000
    bucket_start_ms = bucket_end_ms - 10_000
    snapshots = [_depth_snapshot(bucket_start_ms - 1, -1)]
    snapshots.extend(
        _depth_snapshot(bucket_start_ms + index * 97, index)
        for index in range(102)
    )

    assert tuple(narrowgate_cpp.SIGNAL_EXECUTION_L2_FEATURE_NAMES) == tuple(
        EXECUTION_L2_FEATURE_COLS
    )
    expected = _python_execution_l2_values(snapshots, bucket_end_ms)
    actual = _native_execution_l2_values(snapshots, bucket_end_ms)
    assert actual.flags.c_contiguous
    assert np.array_equal(actual, expected)

    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    engine._depth_history.extend(snapshots)
    integrated: dict[str, float] = {}
    engine._compute_execution_l2_features(integrated, bucket_end_ms)
    assert np.array_equal(
        np.asarray(
            [integrated[name] for name in EXECUTION_L2_FEATURE_COLS],
            dtype=np.float64,
        ),
        expected,
    )


@pytest.mark.parametrize(
    "snapshots",
    [
        [],
        [
            DepthSnapshot(
                ts=999_500,
                bids=[(100.0, -1.0)],
                asks=[(100.2, 2.0)],
            )
        ],
        [
            _depth_snapshot(989_999, 0, depth=3),
            _depth_snapshot(999_999, 1, depth=1),
        ],
    ],
    ids=("empty", "single_shallow", "sparse_with_previous"),
)
def test_cpp_execution_l2_batch_matches_python_empty_and_sparse_histories(
    snapshots: list[DepthSnapshot],
) -> None:
    expected = _python_execution_l2_values(snapshots, 1_000_000)
    actual = _native_execution_l2_values(snapshots, 1_000_000)
    assert np.array_equal(actual, expected)


def test_cpp_execution_l2_batch_preserves_cutoff_and_invalid_latest_state() -> None:
    bucket_end_ms = 1_000_000
    visible = [
        _depth_snapshot(989_999, 0),
        _depth_snapshot(990_000, 1),
        _depth_snapshot(999_999, 2),
    ]
    expected = _native_execution_l2_values(visible, bucket_end_ms)
    future = _depth_snapshot(bucket_end_ms, 100)
    future.bids[0] = (1.0, 1_000_000.0)
    future.asks[0] = (1_000_000.0, 1_000_000.0)
    assert np.array_equal(
        _native_execution_l2_values([*visible, future], bucket_end_ms),
        expected,
    )

    invalid_latest = DepthSnapshot(ts=bucket_end_ms - 1, bids=[], asks=[])
    with_invalid = [*visible, invalid_latest]
    assert np.array_equal(
        _native_execution_l2_values(with_invalid, bucket_end_ms),
        _python_execution_l2_values(with_invalid, bucket_end_ms),
    )
    assert np.array_equal(
        _native_execution_l2_values(with_invalid, bucket_end_ms),
        np.zeros(len(EXECUTION_L2_FEATURE_COLS), dtype=np.float64),
    )


def test_cpp_l2_policy_batch_matches_python_inclusive_exchange_window() -> None:
    end_exchange_ms = 1_000_000.0
    snapshots = [
        _depth_snapshot(989_999.0, 0),
        _depth_snapshot(990_000.0, 1, depth=1),
        DepthSnapshot(ts=995_000.0, bids=[], asks=[]),
        _depth_snapshot(999_999.0, 2, depth=3),
        _depth_snapshot(1_000_000.0, 3, depth=10),
        _depth_snapshot(1_000_001.0, 4),
    ]

    assert tuple(narrowgate_cpp.SIGNAL_EXECUTION_L2_POLICY_METRIC_NAMES) == (
        EXECUTION_L2_POLICY_METRIC_COLS
    )
    expected = _python_l2_policy_values(snapshots, end_exchange_ms)
    actual = _native_l2_policy_values(snapshots, end_exchange_ms)
    assert actual.flags.c_contiguous
    assert np.array_equal(actual, expected)

    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    integrated = engine._compute_cpp_l2_policy_values(
        snapshots,
        end_exchange_ms,
    )
    assert integrated is not None
    assert np.array_equal(integrated, expected)


@pytest.mark.parametrize(
    "snapshots",
    [
        [],
        [DepthSnapshot(ts=1_000_000.0, bids=[], asks=[])],
        [_depth_snapshot(989_999.0, 0)],
        [_depth_snapshot(1_000_000.0, 1, depth=1)],
    ],
    ids=("empty", "invalid", "outside_window", "single_shallow_at_end"),
)
def test_cpp_l2_policy_batch_matches_python_empty_sparse_and_boundary(
    snapshots: list[DepthSnapshot],
) -> None:
    expected = _python_l2_policy_values(snapshots, 1_000_000.0)
    actual = _native_l2_policy_values(snapshots, 1_000_000.0)
    assert np.array_equal(actual, expected)


def test_cpp_l2_policy_missing_abi_falls_back_or_fails_strict(monkeypatch) -> None:
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = object()
    engine._cpp_signal_features_enabled = True
    monkeypatch.setattr(signal_module, "_cpp_signal_strict", lambda: False)
    assert engine._compute_cpp_l2_policy_values([], 1_000_000.0) is None
    assert engine._cpp_l2_policy_disabled_after_error is True

    strict_engine = SignalEngine(enable_ml=False)
    strict_engine._cpp_signal = object()
    strict_engine._cpp_signal_features_enabled = True
    monkeypatch.setattr(signal_module, "_cpp_signal_strict", lambda: True)
    with pytest.raises(
        RuntimeError,
        match="missing execution L2 policy metric batch ABI",
    ):
        strict_engine._compute_cpp_l2_policy_values([], 1_000_000.0)


def test_maker_l2_policy_metrics_native_path_matches_python_fallback() -> None:
    depth_snapshots = [
        _depth_snapshot(990_000.0 + index * 100.0, index)
        for index in range(101)
    ]
    quote_snapshot = _policy_quote_snapshot(depth_snapshots, 1_000_000.0)
    signal = SignalEngine(enable_ml=False)
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.signal = signal

    signal._cpp_signal_features_enabled = False
    expected = engine._current_l2_policy_metrics(
        60_000.0,
        quote_snapshot,
    )
    signal._cpp_signal = narrowgate_cpp
    signal._cpp_signal_features_enabled = True
    actual = engine._current_l2_policy_metrics(
        60_000.0,
        quote_snapshot,
    )

    assert actual == expected


def test_cpp_signal_feature_overlay_matches_python_core_features():
    engine = SignalEngine(enable_ml=False)
    for i in range(80):
        price = 100.0 + (i % 7 - 3) * 0.1 + i * 0.005
        bar = Bar1s(
            ts=1_000 * (i + 1),
            open=price - 0.05,
            high=price + 0.1,
            low=price - 0.1,
            close=price,
            volume=1.0 + (i % 5) * 0.1,
            buy_volume=0.55 + (i % 3) * 0.03,
            sell_volume=0.45 + (i % 2) * 0.04,
            trade_count=4 + (i % 4),
            buy_count=2 + (i % 2),
            sell_count=2 + (i % 3),
            quote_qty=(1.0 + (i % 5) * 0.1) * price,
            buy_quote_qty=(0.55 + (i % 3) * 0.03) * price,
            sell_quote_qty=(0.45 + (i % 2) * 0.04) * price,
            max_same_side_run=1 + (i % 4),
            buy_price_high=price + 0.05,
            buy_price_low=price - 0.02,
            sell_price_high=price + 0.04,
            sell_price_low=price - 0.06,
        )
        engine._finalize_bar(bar)

    all_bars = list(engine._bar_buffer)
    bars_10 = all_bars[-10:]
    bar_10s = engine._aggregate_bars(bars_10)

    engine._cpp_signal_features_enabled = False
    py_features = engine._compute_features(bar_10s, all_bars)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    cpp_features = engine._compute_features(bar_10s, all_bars)
    cpp_overlay = engine._compute_cpp_feature_overlay(bar_10s, all_bars)
    cpp_values = engine._compute_cpp_feature_values(bar_10s, all_bars)

    assert cpp_overlay is not None
    assert len(cpp_overlay) == 80
    assert cpp_values is not None
    assert cpp_values.shape == (80,)
    assert cpp_values.flags.c_contiguous
    for key, value in cpp_overlay.items():
        assert value == pytest.approx(py_features[key], abs=1e-12), key

    keys = [
        "tick_streak",
        "tick_mom_3s",
        "tick_mom_5s",
        "tick_mom_10s",
        "tick_ewm_3s",
        "tick_ewm_10s",
        "micro_ret_std",
        "micro_ret_skew",
        "micro_ret_kurt",
        "tick_reversal_freq",
        "flow_velocity",
        "flow_acceleration",
        "tick_streak_max",
        "tick_mom_range",
        "volatility_30s",
        "volatility_60s",
        "volume_imbalance",
        "volume_imbalance_60s",
        "trade_intensity_60s",
        "vpin_60s",
        "taker_quote_imbalance_30s",
        "taker_signed_quote_sum_60s",
        "taker_trade_count_sum_60s",
        "taker_max_same_side_run_60s",
        "taker_buy_sweep_score_60s",
        "taker_sell_iceberg_pressure_sum_60s",
        "price_velocity",
        "price_acceleration",
        "price_change_60s",
        "avg_trade_size",
        "avg_trade_size_60s",
        "large_trade_ratio",
        "volume_zscore",
        "bar_spread_bps",
        "return_1",
        "return_abs",
        "vol_regime_6h",
        "vol_regime_24h",
        "vol_regime_zscore",
    ]
    for key in keys:
        assert cpp_features[key] == pytest.approx(py_features[key], abs=1e-12)


def test_cpp_signal_feature_ring_buffer_wrap_matches_stateless_tail():
    max_bars = 32
    max_history = 64
    engine = narrowgate_cpp.SignalFeatureEngine(max_bars, max_history)
    bars = []
    history = []

    for i in range(96):
        price = 100.0 + i * 0.01 + (i % 5 - 2) * 0.03
        bar = narrowgate_cpp.Bar1s()
        bar.ts_ms = (i + 1) * 1_000
        bar.open = price - 0.01
        bar.high = price + 0.04
        bar.low = price - 0.05
        bar.close = price
        bar.volume = 1.0 + (i % 7) * 0.1
        bar.buy_volume = 0.6 + (i % 3) * 0.02
        bar.sell_volume = 0.4 + (i % 4) * 0.02
        bar.trade_count = 3 + i % 5
        bar.buy_count = 2 + i % 3
        bar.sell_count = 1 + i % 4
        bar.buy_quote_qty = bar.buy_volume * price
        bar.sell_quote_qty = bar.sell_volume * price
        bar.max_same_side_run = 1 + i % 4
        bar.buy_price_high = price + 0.02
        bar.buy_price_low = price - 0.01
        bar.sell_price_high = price + 0.01
        bar.sell_price_low = price - 0.03
        bars.append(bar)
        engine.push_bar(bar)

        row = narrowgate_cpp.FeatureHistoryRow()
        row.close = price
        row.volume = bar.volume
        row.buy_volume = bar.buy_volume
        row.sell_volume = bar.sell_volume
        row.trade_count = bar.trade_count
        row.flow_velocity = bar.buy_volume - bar.sell_volume
        row.avg_trade_size = bar.volume / bar.trade_count
        row.price_velocity = 0.01
        row.return_abs = abs(0.0001 * (i % 5 - 2))
        row.vol_regime_6h = 0.001 + i * 1e-6
        history.append(row)
        engine.push_history(row)

    bar_10s = narrowgate_cpp.Bar1s()
    bar_10s.ts_ms = 97_000
    bar_10s.open = 100.8
    bar_10s.high = 101.1
    bar_10s.low = 100.7
    bar_10s.close = 101.0
    bar_10s.volume = 8.0
    bar_10s.buy_volume = 4.5
    bar_10s.sell_volume = 3.5
    bar_10s.trade_count = 24

    persistent = engine.compute(bar_10s)
    persistent_values = engine.compute_values(bar_10s)
    stateless = narrowgate_cpp.compute_signal_feature_overlay(
        bars[-max_bars:], history[-max_history:], bar_10s
    )

    assert engine.bar_count() == max_bars
    assert engine.history_count() == max_history
    assert len(persistent) == 80
    assert tuple(narrowgate_cpp.SIGNAL_FEATURE_NAMES) == tuple(persistent.keys())
    assert list(persistent_values) == pytest.approx(list(persistent.values()), abs=1e-12)
    assert persistent.keys() == stateless.keys()
    for key in persistent:
        assert persistent[key] == pytest.approx(stateless[key], abs=1e-12)


def test_cpp_signal_feature_incremental_vol_regime_matches_stateless_history():
    engine = narrowgate_cpp.SignalFeatureEngine(8, 10_000)
    history = []
    for i in range(9_000):
        row = narrowgate_cpp.FeatureHistoryRow()
        row.close = 100.0 + i * 0.001
        row.return_abs = 0.0001 + (i % 31) * 1e-7
        row.vol_regime_6h = 0.0002 + (i % 101) * 1e-8
        history.append(row)
        engine.push_history(row)

    bar_10s = narrowgate_cpp.Bar1s()
    bar_10s.close = 109.01
    bar_10s.high = 109.02
    bar_10s.low = 109.00
    persistent = engine.compute(bar_10s)
    stateless = narrowgate_cpp.compute_signal_feature_overlay([], history, bar_10s)

    for key in ("vol_regime_6h", "vol_regime_24h", "vol_regime_zscore"):
        assert persistent[key] == pytest.approx(stateless[key], abs=1e-10), key
