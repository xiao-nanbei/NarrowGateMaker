import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

from strategy.signal import Bar1s, SignalEngine


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
