import math
import sys
import threading
from collections import deque
from dataclasses import asdict
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from features.feature_engineer import (
    add_microstructure_features,
    compute_tick_momentum,
    resample_to_10s,
)
from strategy import signal as signal_module
from strategy.signal import (
    FEATURE_NAMES_BASE,
    Bar1s,
    FeatureCutoff,
    SignalEngine,
)

BASE_MS = 1_000_000


def _count_packet(first=10, last=12, *, ts=BASE_MS + 100, qty="0.6", maker=False):
    return {"e": "aggTrade", "s": "BTCUSDC", "T": ts, "p": "100", "q": qty,
            "m": maker, "f": first, "l": last}


@pytest.fixture
def count_engine(monkeypatch):
    for flag in ("NARROWGATE_CPP_SIGNAL_FEATURES", "NARROWGATE_CPP_GLOBAL_FLOW",
                 "NARROWGATE_CPP_LIGHTGBM_INFERENCE"):
        monkeypatch.setenv(flag, "0")
    return SignalEngine(enable_ml=False, execution_trade_count_unit="individual_execution")


def test_execution_individual_split_and_combined_bar_and_tempo_equivalence(count_engine):
    combined = count_engine
    split = SignalEngine(enable_ml=False, execution_trade_count_unit="individual_execution")
    combined.on_agg_trade(_count_packet(), receive_ts_ns=123_456_789)
    for i, qty in enumerate(("0.1", "0.2", "0.3")):
        split.on_agg_trade(_count_packet(10 + i, 10 + i, qty=qty), receive_ts_ns=123_456_789)
    for engine in (combined, split):
        engine.on_agg_trade(_count_packet(13, 14, ts=BASE_MS + 1_100, qty="0.4", maker=True))
        engine.on_agg_trade(_count_packet(15, 15, ts=BASE_MS + 2_100, qty="0.1", maker=True))
    for left, right in zip(combined._bar_buffer, split._bar_buffer, strict=True):
        assert asdict(left) == pytest.approx(asdict(right))
    assert combined._bar_buffer[0].trade_count == 3
    assert combined._bar_buffer[0].buy_count == 3
    assert combined._bar_buffer[0].max_buy_run == 3
    assert combined._current_bar.max_sell_run == 3
    left = combined.compute_taker_tempo_features_from_bars(list(combined._bar_buffer))
    right = split.compute_taker_tempo_features_from_bars(list(split._bar_buffer))
    assert left == pytest.approx(right, nan_ok=True)


def test_execution_count_opt_in_preserves_packet_clock(count_engine):
    packet = _count_packet()
    count_engine.on_agg_trade(packet, receive_ts_ns=123_456_789)
    assert count_engine._current_bar.ts == BASE_MS
    assert count_engine._execution_trade_last_ts_ms == packet["T"]
    state = next(iter(count_engine._market_source_state.values()))
    assert state["last_trade_receive_ts_ns"] == 123_456_789
    assert count_engine._execution_trade_last_id == 12


def test_default_engine_retains_native_packet_count_and_missing_id_behavior(count_engine):
    engine = SignalEngine(enable_ml=False)
    engine.on_agg_trade(_count_packet())
    packet = _count_packet(None, None, qty="0.4")
    engine.on_agg_trade(packet)
    assert engine._execution_trade_count_unit == "native_aggregate_packet"
    assert engine._current_bar.trade_count == 2
    assert engine._current_bar.max_buy_run == 2
    assert engine._current_bar.volume == pytest.approx(1.0)


@pytest.mark.parametrize("first,last", [(None, 2), (1, None), (-1, 2), (3, 2),
                                      (True, 2), (1, False), (1.0, 2), ("1", 2),
                                      (1, 2**63)])
def test_individual_invalid_ids_do_not_update_any_market_or_bar_state(count_engine, first, last):
    with pytest.raises(ValueError, match="valid integer"):
        count_engine.on_agg_trade(_count_packet(first, last))
    assert count_engine._current_bar is None and not count_engine._bar_buffer
    assert not count_engine._market_source_state
    assert not count_engine._execution_trade_state_started
    assert count_engine._execution_trade_last_id is None


@pytest.mark.parametrize("first,last", [(10, 12), (11, 15), (2, 9)])
def test_individual_duplicate_overlap_and_reordering_are_atomic(count_engine, first, last):
    count_engine.on_agg_trade(_count_packet())
    before = asdict(count_engine._current_bar)
    market = {key: dict(value) for key, value in count_engine._market_source_state.items()}
    with pytest.raises(ValueError, match="duplicate/overlapping/out-of-order"):
        count_engine.on_agg_trade(_count_packet(first, last))
    assert asdict(count_engine._current_bar) == before
    assert count_engine._market_source_state == market
    assert count_engine._execution_trade_last_id == 12


def test_individual_gap_counts_only_observed_range_and_breaks_run(count_engine):
    count_engine.on_agg_trade(_count_packet())
    count_engine.on_agg_trade(_count_packet(20, 21, ts=BASE_MS + 1_100))
    assert count_engine._current_bar.trade_count == 2
    assert count_engine._current_bar.max_buy_run == 2
    assert count_engine._last_trade_run_len == 2
    assert count_engine._execution_trade_last_id == 21


@pytest.mark.parametrize("updates", [{"s": "BTCUSDT"}, {"T": BASE_MS - 100}])
def test_individual_wrong_market_or_clock_regression_is_atomic(count_engine, updates):
    count_engine.on_agg_trade(_count_packet())
    before = asdict(count_engine._current_bar)
    packet = {**_count_packet(13, 14), **updates}
    with pytest.raises(ValueError, match="execution-symbol chronological"):
        count_engine.on_agg_trade(packet)
    assert asdict(count_engine._current_bar) == before


def test_individual_prefill_and_live_share_weight_and_watermark(count_engine):
    count_engine.prefill_from_agg_trades([_count_packet(), _count_packet(13, 14, ts=BASE_MS + 1_100)])
    assert [bar.trade_count for bar in count_engine._bar_buffer] == [3, 2]
    with pytest.raises(ValueError, match="duplicate"):
        count_engine.on_agg_trade(_count_packet(13, 14, ts=BASE_MS + 1_100))
    count_engine.on_agg_trade(_count_packet(15, 16, ts=BASE_MS + 2_100))
    assert count_engine._current_bar.trade_count == 2
    assert count_engine._current_bar.max_buy_run == 7


def test_individual_prefill_invalid_suffix_commits_no_prefix(count_engine):
    with pytest.raises(ValueError, match="valid integer"):
        count_engine.prefill_from_agg_trades([_count_packet(), _count_packet(None, None)])
    assert not count_engine._bar_buffer and not count_engine._execution_trade_state_started
    assert count_engine._execution_trade_last_id is None


def test_individual_new_packet_cannot_silently_disappear_inside_prefill_bar(count_engine):
    count_engine.prefill_from_agg_trades([_count_packet()])
    market = {key: dict(value) for key, value in count_engine._market_source_state.items()}
    with pytest.raises(ValueError, match="already committed prefill bar"):
        count_engine.on_agg_trade(_count_packet(13, 14, ts=BASE_MS + 200))
    assert count_engine._market_source_state == market
    assert count_engine._execution_trade_last_id == 12
    assert count_engine._bar_buffer[-1].trade_count == 3


def _count_model_metadata(unit=None):
    return {head: {"feature_cols": ["close"], **(
        {} if unit is None else {"execution_trade_count_unit": unit})}
        for head in signal_module.REQUIRED_MODEL_HEADS}


@pytest.fixture
def count_model_loader(monkeypatch, count_engine):
    holder = {"metadata": _count_model_metadata(), "on_load": lambda: None}

    class Booster:
        def __init__(self, **kwargs):
            holder["on_load"]()

        def num_feature(self):
            return 1

    monkeypatch.setattr(signal_module, "validate_model_bundle", lambda *a, **kw: holder["metadata"])
    monkeypatch.setitem(sys.modules, "lightgbm", SimpleNamespace(Booster=Booster))
    return holder


@pytest.mark.parametrize("bad", [None, "individual", "", 1, []])
def test_model_metadata_rejects_explicit_invalid_count_unit(bad):
    metadata = _count_model_metadata()
    metadata[signal_module.REQUIRED_MODEL_HEADS[0]]["execution_trade_count_unit"] = bad
    with pytest.raises(ValueError, match="execution_trade_count_unit"):
        signal_module.execution_trade_count_unit(metadata)


def test_model_metadata_requires_every_head_to_opt_in():
    metadata = _count_model_metadata("individual_execution")
    metadata[signal_module.REQUIRED_MODEL_HEADS[0]].pop("execution_trade_count_unit")
    with pytest.raises(ValueError, match="share one"):
        signal_module.execution_trade_count_unit(metadata)


def test_metadata_selects_unit_and_same_unit_reload_preserves_counts(count_model_loader):
    count_model_loader["metadata"] = _count_model_metadata("individual_execution")
    engine = SignalEngine(enable_ml=True)
    engine.on_agg_trade(_count_packet())
    old_models = engine._models
    engine.reload_models()
    assert engine._models is not old_models
    assert engine._execution_trade_count_unit == "individual_execution"
    assert engine._current_bar.trade_count == 3 and engine._execution_trade_last_id == 12


def test_constructor_cannot_override_legacy_model_count_contract(count_model_loader):
    with pytest.raises(RuntimeError, match="declared by every model head"):
        SignalEngine(enable_ml=True, execution_trade_count_unit="individual_execution")


def test_count_semantics_reload_rejects_used_engine_without_changing_model(count_model_loader):
    engine = SignalEngine(enable_ml=True)
    old_models, old_metadata = engine._models, engine._model_metadata
    engine.on_agg_trade(_count_packet())
    engine._current_bar = None  # Empty current buffers do not erase prior use.
    count_model_loader["metadata"] = _count_model_metadata("individual_execution")
    with pytest.raises(RuntimeError, match="fresh SignalEngine"):
        engine.reload_models()
    assert engine._models is old_models and engine._model_metadata is old_metadata
    assert engine._execution_trade_count_unit == "native_aggregate_packet"


def test_count_semantics_commit_rechecks_event_arriving_during_model_load(count_model_loader):
    engine = SignalEngine(enable_ml=True)
    count_model_loader["metadata"] = _count_model_metadata("individual_execution")
    count_model_loader["on_load"] = lambda: engine.on_agg_trade(_count_packet())
    with pytest.raises(RuntimeError, match="fresh SignalEngine"):
        engine.reload_models()
    assert engine._execution_trade_count_unit == "native_aggregate_packet"
    assert engine._current_bar.trade_count == len(signal_module.REQUIRED_MODEL_HEADS)


def test_count_semantics_concurrent_reload_and_trade_are_serialized(count_model_loader):
    engine = SignalEngine(enable_ml=True)
    old_models = engine._models
    count_model_loader["metadata"] = _count_model_metadata("individual_execution")
    loading, continue_load = threading.Event(), threading.Event()
    failures = []

    def loading_pause():
        loading.set()
        assert continue_load.wait(5)

    def reload():
        try:
            engine.reload_models()
        except Exception as exc:
            failures.append(exc)

    count_model_loader["on_load"] = loading_pause
    worker = threading.Thread(target=reload)
    worker.start()
    try:
        assert loading.wait(5)
        engine.on_agg_trade(_count_packet())
    finally:
        continue_load.set()
        worker.join(5)
    assert not worker.is_alive()
    assert len(failures) == 1 and "fresh SignalEngine" in str(failures[0])
    assert engine._models is old_models and engine._current_bar.trade_count == 1


def test_prefill_rechecks_semantics_if_reload_occurs_during_bar_construction(count_model_loader, monkeypatch):
    engine = SignalEngine(enable_ml=True)
    original = engine._apply_trade_to_bar

    def with_reload(*args, **kwargs):
        count_model_loader["metadata"] = _count_model_metadata("individual_execution")
        engine.reload_models()
        original(*args, **kwargs)

    monkeypatch.setattr(engine, "_apply_trade_to_bar", with_reload)
    with pytest.raises(RuntimeError, match="changed during prefill"):
        engine.prefill_from_agg_trades([_count_packet()])
    assert not engine._bar_buffer and not engine._execution_trade_state_started


def test_execution_opt_in_does_not_change_reference_packet_count(count_engine):
    packet = {**_count_packet(), "s": "BTCUSDT"}
    count_engine.on_cross_agg_trade(packet)
    assert next(iter(count_engine._cross_current_bars.values())).trade_count == 1


@pytest.mark.parametrize("missing", [None, *Bar1s.__dataclass_fields__])
def test_native_bar_requires_every_declared_field(missing):
    values = asdict(_bar(1))
    values["ts_ms"] = values.pop("ts")
    if missing is None:
        values.clear()
    else:
        values.pop("ts_ms" if missing == "ts" else missing)
    with pytest.raises(AttributeError):
        SignalEngine._bar_from_cpp(SimpleNamespace(**values))


def test_native_bar_preserves_complete_values():
    expected = _bar(1)
    values = asdict(expected)
    values["ts_ms"] = values.pop("ts")
    assert SignalEngine._bar_from_cpp(SimpleNamespace(**values)) == expected


def test_native_signal_loader_rejects_incomplete_module_without_fallback(monkeypatch):
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "0")
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "1")
    monkeypatch.setattr(signal_module, "_CPP_SIGNAL_MODULE", None)
    monkeypatch.setattr(signal_module, "_CPP_SIGNAL_IMPORT_FAILED", False)
    monkeypatch.setattr(signal_module, "load_native_module", lambda **_kwargs: SimpleNamespace())
    with pytest.raises(RuntimeError, match="ABI missing"):
        signal_module._load_cpp_signal_module()
    assert signal_module._CPP_SIGNAL_MODULE is None
    assert signal_module._CPP_SIGNAL_IMPORT_FAILED is False


@pytest.mark.parametrize("strict", ["0", "1"])
def test_native_feature_exception_does_not_select_another_backend(monkeypatch, strict):
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", strict)
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "0")
    engine = SignalEngine(enable_ml=False)
    failure = RuntimeError("native feature calculation failed")

    def broken(*_args):
        raise failure

    engine._cpp_signal = SimpleNamespace(Bar1s=SimpleNamespace)
    engine._cpp_signal_features_enabled = True
    engine._cpp_signal_feature_names = ("close",)
    monkeypatch.setattr(
        engine, "_ensure_cpp_feature_engine",
        lambda _bars: SimpleNamespace(compute_values_at_cutoff=broken),
    )
    with pytest.raises(RuntimeError) as raised:
        engine._compute_cpp_feature_values({"ts": BASE_MS}, [])
    assert raised.value is failure
    assert engine._cpp_signal_features_enabled is True


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
