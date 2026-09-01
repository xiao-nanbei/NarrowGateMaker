import math
from collections import deque

import numpy as np
import pandas as pd
import pytest

from features.feature_engineer import (
    add_microstructure_features,
    compute_tick_momentum,
    resample_to_10s,
)
from strategy.signal import (
    FEATURE_NAMES_BASE,
    Bar1s,
    FeatureCutoff,
    SignalEngine,
)

BASE_MS = 1_000_000


def _bar(second: int, *, future_shock: float = 0.0) -> Bar1s:
    price = 100.0 + second * 0.01 + ((second % 5) - 2) * 0.02 + future_shock
    buy_volume = 0.6 + (second % 3) * 0.03 + max(future_shock, 0.0)
    sell_volume = 0.4 + (second % 4) * 0.02 + max(-future_shock, 0.0)
    trade_count = 3 + second % 5
    return Bar1s(
        ts=BASE_MS + second * 1_000,
        open=price - 0.01,
        high=price + 0.04,
        low=price - 0.05,
        close=price,
        volume=buy_volume + sell_volume,
        buy_volume=buy_volume,
        sell_volume=sell_volume,
        trade_count=trade_count,
        buy_count=2 + second % 3,
        sell_count=1 + second % 4,
        quote_qty=(buy_volume + sell_volume) * price,
        buy_quote_qty=buy_volume * price,
        sell_quote_qty=sell_volume * price,
        max_same_side_run=1 + second % 4,
        max_buy_run=1 + second % 3,
        max_sell_run=1 + second % 4,
        buy_price_high=price + 0.02,
        buy_price_low=price - 0.01,
        sell_price_high=price + 0.01,
        sell_price_low=price - 0.03,
    )


def _engine_with_bars(last_second: int, *, future_shock: float = 0.0) -> SignalEngine:
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal_features_enabled = False
    for second in range(last_second + 1):
        shock = future_shock if second == last_second else 0.0
        engine._finalize_bar(_bar(second, future_shock=shock))
    return engine


def _capture_features(engine: SignalEngine):
    captured = []
    original = engine._compute_features

    def wrapped(bar_10s, all_bars, *, cutoff=None):
        features = original(bar_10s, all_bars, cutoff=cutoff)
        captured.append(dict(features))
        return features

    engine._compute_features = wrapped
    return captured


def _assert_feature_dicts_equal(left: dict, right: dict) -> None:
    assert left.keys() == right.keys()
    for key in left:
        assert float(left[key]) == pytest.approx(float(right[key]), abs=1e-12), key


class _NonIterableFeatureHistory(deque):
    """Prove metrics can address its one required history row without a full scan."""

    def __iter__(self):
        raise AssertionError("metrics must not iterate the complete feature history")


def _append_metrics_history(engine: SignalEngine, target_ts_ms: int) -> None:
    for index in range(72):
        engine._metrics_history.append(
            {
                "ts_ms": target_ts_ms - (71 - index) * 300_000,
                "oi": 1_000_000.0 + index * 317.0,
                "top_ls": 1.02 + math.sin(index / 9.0) * 0.03,
                "crowd_ls": 0.98 + math.cos(index / 8.0) * 0.02,
                "taker_ls": 1.00 + math.sin(index / 7.0) * 0.025,
            }
        )


def test_metrics_full_feature_history_matches_legacy_row_selection_without_iteration() -> None:
    target_ts_ms = 1_780_000_000_000
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    _append_metrics_history(engine, target_ts_ms)
    for index in range(60_480):
        engine._feat_history.append(
            {
                "close": 99_000.0 + index * 0.017,
                "return_abs": 0.00002 + (index % 31) * 0.0000002,
                "vol_regime_6h": 0.000022 + (index % 101) * 0.00000001,
            }
        )

    seed = {"close": 100_123.45}
    expected = dict(seed)
    engine._compute_metrics_features(expected, target_ts_ms)
    full_history = list(engine._feat_history)
    previous_oi = engine._metrics_history[-2]["oi"]
    current_oi = engine._metrics_history[-1]["oi"]
    legacy_old_close = full_history[-30]["close"]
    legacy_divergence = (
        (current_oi - previous_oi) / previous_oi
        - (seed["close"] - legacy_old_close) / legacy_old_close
    )
    assert expected["oi_price_divergence"] == pytest.approx(legacy_divergence, abs=0.0)

    engine._feat_history = _NonIterableFeatureHistory(full_history, maxlen=60_480)
    actual = dict(seed)
    engine._compute_metrics_features(actual, target_ts_ms)

    assert actual.keys() == expected.keys()
    for name, value in expected.items():
        assert actual[name] == pytest.approx(value, abs=0.0), name


@pytest.mark.parametrize(
    ("history_rows", "old_close"),
    [(0, None), (29, None), (30, 99_000.0)],
)
def test_metrics_oi_price_divergence_thirty_row_boundary(
    history_rows: int,
    old_close: float | None,
) -> None:
    target_ts_ms = 1_780_000_000_000
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    _append_metrics_history(engine, target_ts_ms)
    for index in range(history_rows):
        engine._feat_history.append({"close": 99_000.0 + index})

    features = {"close": 100_123.45}
    engine._compute_metrics_features(features, target_ts_ms)

    if old_close is None:
        assert features["oi_price_divergence"] == 0.0
        return
    previous_oi = engine._metrics_history[-2]["oi"]
    current_oi = engine._metrics_history[-1]["oi"]
    expected = (current_oi - previous_oi) / previous_oi - (
        features["close"] - old_close
    ) / old_close
    assert features["oi_price_divergence"] == pytest.approx(expected, abs=0.0)


def test_feature_cutoff_is_strictly_exclusive() -> None:
    cutoff = FeatureCutoff(BASE_MS + 10_000)
    visible = cutoff.visible_bars(
        [
            Bar1s(ts=BASE_MS + 9_999, close=1.0),
            Bar1s(ts=BASE_MS + 10_000, close=2.0),
        ]
    )
    assert [bar.ts for bar in visible] == [BASE_MS + 9_999]
    assert cutoff.source_clock == "exchange_time_ms"
    assert cutoff.availability_clock == "finalized_bar_time"


def test_next_bucket_first_bar_cannot_change_previous_bucket_features() -> None:
    control = _engine_with_bars(30, future_shock=0.0)
    shocked = _engine_with_bars(30, future_shock=500.0)

    control_features = control.compute_signal().feature_dict
    shocked_features = shocked.compute_signal().feature_dict

    assert control._last_processed_bucket == BASE_MS + 20_000
    assert shocked._last_processed_bucket == BASE_MS + 20_000
    assert control_features is not None and shocked_features is not None
    _assert_feature_dicts_equal(control_features, shocked_features)
    assert control_features["_feature_cutoff_exclusive_ms"] == BASE_MS + 30_000


def test_compute_signal_catches_up_each_completed_bucket_exactly_once() -> None:
    engine = _engine_with_bars(60)

    engine.compute_signal()
    assert len(engine._feat_history) == 6
    assert engine._last_processed_bucket == BASE_MS + 50_000

    engine.compute_signal()
    assert len(engine._feat_history) == 6
    assert engine._last_processed_bucket == BASE_MS + 50_000


def test_multi_second_trade_gap_generates_each_feature_bucket() -> None:
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal_features_enabled = False
    engine.on_agg_trade({"T": BASE_MS + 500, "p": "100.0", "q": "0.1", "m": False})
    engine.on_agg_trade({"T": BASE_MS + 60_500, "p": "101.0", "q": "0.1", "m": True})

    engine.compute_signal()

    assert len(engine._bar_buffer) == 60
    assert len(engine._feat_history) == 6
    assert engine._last_processed_bucket == BASE_MS + 50_000


def test_prefill_and_live_ingestion_have_identical_feature_fingerprints() -> None:
    trades = []
    for second in range(61):
        price = 100.0 + second * 0.01 + ((second % 5) - 2) * 0.02
        trades.append(
            {
                "T": BASE_MS + second * 1_000 + 500,
                "p": str(price),
                "q": str(0.1 + (second % 3) * 0.01),
                "m": bool(second % 2),
            }
        )

    prefill = SignalEngine(enable_ml=False)
    prefill._cpp_signal_features_enabled = False
    prefill_features = _capture_features(prefill)
    prefill.prefill_from_agg_trades(trades)

    live = SignalEngine(enable_ml=False)
    live._cpp_signal_features_enabled = False
    live_features = _capture_features(live)
    for trade in trades:
        live.on_agg_trade(trade)
    live.compute_signal()

    assert len(prefill_features) == len(live_features) == 6
    for prefill_row, live_row in zip(prefill_features, live_features, strict=True):
        _assert_feature_dicts_equal(prefill_row, live_row)


def test_prefill_partial_bucket_bridges_to_delayed_first_live_trade() -> None:
    prefill_trades = [
        {
            "T": BASE_MS + second * 1_000 + 500,
            "p": str(100.0 + second * 0.01),
            "q": "0.1",
            "m": bool(second % 2),
        }
        for second in range(14)
    ]
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal_features_enabled = False
    engine.prefill_from_agg_trades(prefill_trades)

    assert engine._last_processed_bucket == BASE_MS
    assert [bar.ts for bar in engine._bar_buffer][-4:] == [
        BASE_MS + second * 1_000 for second in range(10, 14)
    ]

    engine.on_agg_trade(
        {
            "T": BASE_MS + 50_500,
            "p": "101.0",
            "q": "0.1",
            "m": False,
        }
    )
    engine.compute_signal()

    assert engine._last_processed_bucket == BASE_MS + 40_000
    assert len(engine._feat_history) == 5
    assert [bar.ts for bar in engine._bar_buffer][-3:] == [
        BASE_MS + 47_000,
        BASE_MS + 48_000,
        BASE_MS + 49_000,
    ]


def test_prefill_overlap_trade_does_not_append_out_of_order_bar() -> None:
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal_features_enabled = False
    engine.prefill_from_agg_trades(
        [
            {
                "T": BASE_MS + second * 1_000 + 500,
                "p": str(100.0 + second * 0.01),
                "q": "0.1",
                "m": False,
            }
            for second in range(14)
        ]
    )
    before = [bar.ts for bar in engine._bar_buffer]

    engine.on_agg_trade(
        {
            "T": BASE_MS + 13_750,
            "p": "100.13",
            "q": "0.1",
            "m": False,
        }
    )

    assert [bar.ts for bar in engine._bar_buffer] == before
    assert engine._current_bar is None


def test_live_core_features_match_offline_completed_bucket() -> None:
    engine = _engine_with_bars(60)
    live = engine.compute_signal().feature_dict
    assert live is not None

    bars = [_bar(second) for second in range(60)]
    index = pd.to_datetime([bar.ts for bar in bars], unit="ms", utc=True)
    frame = pd.DataFrame(
        {
            "open": [bar.open for bar in bars],
            "high": [bar.high for bar in bars],
            "low": [bar.low for bar in bars],
            "close": [bar.close for bar in bars],
            "vwap": [bar.close for bar in bars],
            "volume": [bar.volume for bar in bars],
            "buy_volume": [bar.buy_volume for bar in bars],
            "sell_volume": [bar.sell_volume for bar in bars],
            "trade_count": [bar.trade_count for bar in bars],
            "buy_count": [bar.buy_count for bar in bars],
            "sell_count": [bar.sell_count for bar in bars],
        },
        index=index,
    )
    offline = resample_to_10s(frame)
    offline = offline.join(compute_tick_momentum(frame), how="left")
    offline = add_microstructure_features(offline, frame)
    row = offline.loc[pd.to_datetime(BASE_MS + 50_000, unit="ms", utc=True)]

    keys = (
        "tick_streak",
        "tick_mom_3s",
        "tick_mom_5s",
        "tick_mom_10s",
        "tick_ewm_3s",
        "tick_ewm_10s",
        "micro_ret_std",
        "micro_ret_skew",
        "micro_ret_kurt",
        "flow_velocity",
        "volatility_5s",
        "volume_imbalance_5s",
        "trade_intensity_5s",
        "vpin_5s",
        "price_change_5s",
        "volume_imbalance",
        "volume_imbalance_30s",
        "trade_intensity_30s",
        "vpin_30s",
        "price_velocity",
        "price_change_30s",
        "avg_trade_size",
        "avg_trade_size_60s",
        "large_trade_ratio",
        "volume_zscore",
        "bar_spread_bps",
        "return_1",
        "return_abs",
    )
    for key in keys:
        assert math.isfinite(float(row[key]))
        assert float(live[key]) == pytest.approx(float(row[key]), abs=1e-12), key


@pytest.mark.parametrize("constant", [False, True])
def test_offline_return_moments_match_population_definition(constant):
    changes = np.ones(29) if constant else np.asarray([0, 1, -2, 4, -1, 0, 3] * 5)[:29]
    close = np.r_[100.0, 100.0 + np.cumsum(changes)]
    bars = pd.DataFrame(
        {"close": close, "buy_volume": 1.0, "sell_volume": 0.5},
        index=pd.date_range("2026-01-01", periods=30, freq="s", tz="UTC"),
    )
    result = compute_tick_momentum(bars)
    for bucket, end in enumerate((9, 19, 29)):
        sample = np.diff(close)[max(0, end - 10):end]
        sd = np.std(sample, ddof=0)
        z = (sample - sample.mean()) / sd if sd > 1e-12 else np.zeros_like(sample)
        row = result.iloc[bucket]
        assert row.micro_ret_std == pytest.approx(sd, abs=1e-12)
        assert row.micro_ret_skew == pytest.approx(np.mean(z ** 3), abs=1e-12)
        assert row.micro_ret_kurt == pytest.approx(
            np.mean(z ** 4) - 3 if sd > 1e-12 else 0.0, abs=1e-12,
        )


def test_near_constant_return_moments_keep_live_population_values():
    close = 78000.0 + 0.1 * np.arange(30)
    bars = pd.DataFrame(
        {"close": close, "buy_volume": 1.0, "sell_volume": 0.5},
        index=pd.date_range("2026-01-01", periods=30, freq="s", tz="UTC"),
    )
    result = compute_tick_momentum(bars)
    for bucket, end in enumerate((9, 19, 29)):
        sample = np.diff(close)[max(0, end - 10):end]
        sd = np.std(sample)
        assert sd > 1e-12
        z = (sample - sample.mean()) / sd
        row = result.iloc[bucket]
        assert row.micro_ret_std == pytest.approx(sd, rel=1e-12, abs=0)
        assert row.micro_ret_skew == pytest.approx(np.mean(z ** 3), abs=1e-12)
        assert row.micro_ret_kurt == pytest.approx(np.mean(z ** 4) - 3, abs=1e-12)


def test_empty_buckets_contribute_zero_size_and_population_volume_zscore():
    count = np.repeat([1, 0, 2, 0, 3, 0], 10)
    bars = pd.DataFrame(
        {
            "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0,
            "vwap": 100.0, "trade_count": count, "volume": count * 2.0,
            "buy_volume": count * 2.0, "sell_volume": 0.0,
            "buy_count": count, "sell_count": 0,
        },
        index=pd.date_range("2026-01-01", periods=60, freq="s", tz="UTC"),
    )
    result = add_microstructure_features(resample_to_10s(bars), bars)
    assert result.avg_trade_size.tolist() == [2, 0, 2, 0, 2, 0]
    assert result.avg_trade_size_60s.iloc[-1] == 1.0
    assert result.large_trade_ratio.iloc[-1] == 0.0
    volumes = result.volume.to_numpy()
    assert result.volume_zscore.iloc[-1] == pytest.approx(
        (volumes[-1] - volumes.mean()) / volumes.std(ddof=0),
    )
    assert result.volume_zscore.iloc[:2].tolist() == [0.0, 0.0]
    empty = bars.copy()
    for name in ("trade_count", "volume", "buy_volume", "buy_count"):
        empty[name] = 0
    flat = add_microstructure_features(resample_to_10s(empty), empty)
    assert flat.avg_trade_size_60s.eq(0).all()
    assert flat.large_trade_ratio.eq(1).all()
    assert flat.volume_zscore.eq(0).all()


def test_feature_name_contract_is_88_unique_fields() -> None:
    assert len(FEATURE_NAMES_BASE) == 88
    assert len(set(FEATURE_NAMES_BASE)) == 88


def test_cpp_persistent_engine_excludes_future_bar_at_cutoff() -> None:
    narrowgate_cpp = pytest.importorskip("narrowgate_cpp")
    assert narrowgate_cpp.SIGNAL_FEATURE_ABI_VERSION == "signal_feature_cutoff.v1"
    if not hasattr(narrowgate_cpp.SignalFeatureEngine, "compute_values_at_cutoff"):
        pytest.fail("installed narrowgate_cpp lacks the cutoff-aware signal ABI")

    engine = narrowgate_cpp.SignalFeatureEngine(128, 128)
    for second in range(30):
        engine.push_bar(SignalEngine._bar_to_cpp(_CppAdapter(narrowgate_cpp), _bar(second)))

    target = SignalEngine(enable_ml=False)._aggregate_bars([_bar(i) for i in range(20, 30)])
    cpp_target = narrowgate_cpp.Bar1s()
    for name, source in (
        ("ts_ms", "ts"),
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
        ("buy_volume", "buy_volume"),
        ("sell_volume", "sell_volume"),
        ("trade_count", "trade_count"),
        ("buy_count", "buy_count"),
        ("sell_count", "sell_count"),
        ("quote_qty", "quote_qty"),
        ("buy_quote_qty", "buy_quote_qty"),
        ("sell_quote_qty", "sell_quote_qty"),
        ("max_same_side_run", "max_same_side_run"),
    ):
        setattr(cpp_target, name, target[source])

    cutoff_ms = BASE_MS + 30_000
    before = np.asarray(engine.compute_values_at_cutoff(cpp_target, cutoff_ms))
    engine.push_bar(SignalEngine._bar_to_cpp(_CppAdapter(narrowgate_cpp), _bar(30, future_shock=500.0)))
    after = np.asarray(engine.compute_values_at_cutoff(cpp_target, cutoff_ms))
    assert after == pytest.approx(before, abs=1e-12)


class _CppAdapter:
    def __init__(self, module):
        self._cpp_signal = module
