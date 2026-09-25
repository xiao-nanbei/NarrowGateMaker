"""Reference opt-in counts real IDs without multiplying packet price/volume."""
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from strategy import signal as module
from strategy.signal import SignalEngine

T = 1_800_000_000_000


def packet(first=10, last=12, *, ts=T + 100, qty="0.6", **extra):
    return {"s": "BTCUSDT", "T": ts, "p": "100", "q": qty, "m": False,
            "f": first, "l": last, **extra}


@pytest.fixture
def engine(monkeypatch):
    for name in ("NARROWGATE_CPP_SIGNAL_FEATURES", "NARROWGATE_CPP_GLOBAL_FLOW",
                 "NARROWGATE_CPP_LIGHTGBM_INFERENCE"):
        monkeypatch.setenv(name, "0")
    return SignalEngine(enable_ml=False, reference_trade_count_unit="individual_execution")


def snapshot(engine):
    return deepcopy((engine._cross_current_bars, engine._cross_bar_buffers,
                     engine._market_source_state, engine._reference_trade_last_id,
                     engine._reference_trade_last_ts_ms, engine._reference_trade_gap_count))


def test_reference_packet_and_real_individual_rows_have_same_counts_and_volume(engine):
    other = SignalEngine(enable_ml=False, reference_trade_count_unit="individual_execution")
    engine.on_cross_agg_trade(packet(), receive_ts_ns=123456)
    other.on_cross_trade_arrays("BTCUSDT", [T + 100] * 3, [100.] * 3, [.1, .2, .3], [False] * 3,
                               trade_count_unit="individual_execution", trade_ids=[10, 11, 12],
                               receive_ts_ns=123456)
    for instance in (engine, other):
        instance.on_cross_agg_trade(packet(13, 14, ts=T + 10_100, qty="0.4"))
        instance.on_cross_agg_trade(packet(15, 15, ts=T + 20_100, qty="0.1"))
    key = next(iter(engine._cross_current_bars))
    for left, right in zip(engine._cross_bar_buffers[key], other._cross_bar_buffers[key], strict=True):
        assert asdict(left) == pytest.approx(asdict(right))
    assert engine._cross_bar_buffers[key][0].trade_count == 3
    assert engine._cross_bar_buffers[key][0].volume == pytest.approx(.6)
    assert engine._reference_trade_last_ts_ms == T + 20_100
    values = {}
    engine._fill_cross_market_trade_features(values, "cv_ref_perp", "perp", "BTCUSDT", T + 19_999, 100.)
    assert values["cv_ref_perp_trade_intensity_60s"] == 2.5


@pytest.mark.parametrize("first,last", [(None, 12), (10, None), (True, 12), (10., 12),
                                      (12, 10), (-1, 10), (10, 2**63)])
def test_bad_reference_suffix_does_not_commit_valid_prefix(engine, first, last):
    before = snapshot(engine)
    with pytest.raises(ValueError):
        engine.on_cross_agg_trade_batch([packet(), packet(first, last, ts=T + 1100)])
    assert snapshot(engine) == before
    assert not engine._reference_trade_state_started


@pytest.mark.parametrize("first,last,ts", [(10, 12, T+200), (11, 14, T+200), (1, 9, T+200), (13, 14, T)])
def test_reference_overlap_or_time_regression_is_atomic(engine, first, last, ts):
    engine.on_cross_agg_trade(packet())
    before = snapshot(engine)
    with pytest.raises(ValueError):
        engine.on_cross_agg_trade(packet(first, last, ts=ts))
    assert snapshot(engine) == before


@pytest.mark.parametrize("change", [{"T": T + .5}, {"T": True}, {"m": "unknown"},
                                   {"p": "nan"}, {"q": "inf"}])
def test_reference_does_not_truncate_clock_or_accept_invalid_side_values(engine, change):
    with pytest.raises(ValueError):
        engine.on_cross_agg_trade(packet(**change))
    assert not engine._market_source_state and not engine._cross_current_bars


def test_reference_gaps_are_unknown_not_extra_executions_and_same_timestamp_ids_survive(engine):
    engine.on_cross_agg_trade_batch([packet(), packet(20, 21)])
    bar = next(iter(engine._cross_current_bars.values()))
    assert bar.trade_count == 5 and bar.volume == pytest.approx(1.2)
    assert engine._reference_trade_gap_count == 1
    assert engine._reference_trade_last_id == 21


def test_reference_arrays_require_explicit_real_ids_or_packet_ranges(engine):
    args = ("BTCUSDT", [T + 100], [100.], [.6], [False])
    with pytest.raises(ValueError, match="f/l"):
        engine.on_cross_trade_arrays(*args)
    with pytest.raises(ValueError, match="trade_ids"):
        engine.on_cross_trade_arrays(*args, trade_count_unit="individual_execution")
    engine.on_cross_trade_arrays(*args, first_trade_ids=[10], last_trade_ids=[12])
    assert next(iter(engine._cross_current_bars.values())).trade_count == 3


def test_legacy_and_other_venue_defaults_remain_one_packet(engine):
    legacy = SignalEngine(enable_ml=False)
    legacy.on_cross_agg_trade(packet(None, None))
    assert next(iter(legacy._cross_current_bars.values())).trade_count == 1
    engine.on_cross_agg_trade(packet(None, None), venue="bybit")
    assert next(iter(engine._cross_current_bars.values())).trade_count == 1
    assert not engine._reference_trade_state_started
    with pytest.raises(ValueError, match="mismatch"):
        legacy.on_cross_trade_arrays("BTCUSDT", [T+200], [100.], [.1], [False],
                                     trade_count_unit="individual_execution", trade_ids=[20])


def test_reference_units_are_uniform_and_opt_in_does_not_change_execution(engine):
    metadata = {head: {"feature_cols": ["close"]} for head in module.REQUIRED_MODEL_HEADS}
    assert module.reference_trade_count_unit(metadata) == "native_aggregate_packet"
    metadata[module.REQUIRED_MODEL_HEADS[0]]["reference_trade_count_unit"] = "individual_execution"
    with pytest.raises(ValueError, match="share"):
        module.reference_trade_count_unit(metadata)
    for row in metadata.values():
        row.update(reference_trade_count_unit="individual_execution", reference_trade_symbol="BTCUSDT")
    loaded = engine
    assert loaded._reference_trade_count_unit == "individual_execution"
    assert loaded._execution_trade_count_unit == "native_aggregate_packet"
    loaded.on_cross_agg_trade(packet())
    old_models = loaded._models
    for row in metadata.values():
        row.pop("reference_trade_count_unit")
    with pytest.raises(AttributeError):
        loaded.reload_models()
    assert loaded._models is old_models


def test_reference_individual_input_cannot_bind_wrong_symbol(engine):
    before = snapshot(engine)
    with pytest.raises(ValueError, match="limited to the configured"):
        engine.on_cross_trade_arrays("ETHUSDT", [T+100], [100.], [.6], [False],
                                     trade_count_unit="individual_execution", trade_ids=[10])
    assert snapshot(engine) == before


@pytest.mark.parametrize("flag,missing", [
    ("reference", "native reference engine lacks individual counts"),
    ("aggregator", "native reference aggregator lacks individual counts"),
])
def test_reference_rejects_old_native_before_state_publication(engine, flag, missing):
    if flag == "reference":
        engine._cpp_ref_perp_engine = SimpleNamespace()
    else:
        engine._cpp_cross_batch_enabled = True
        engine._cpp_signal = SimpleNamespace(TradeBarAggregator=SimpleNamespace())
    before = engine._models
    before_state = snapshot(engine)
    with pytest.raises(RuntimeError, match=missing):
        engine.on_cross_agg_trade(packet())
    assert engine._models is before and not engine._models
    assert snapshot(engine) == before_state


def test_cumulative_reference_count_overflow_is_atomic(engine):
    engine.on_cross_agg_trade(packet(0, 2**53 - 1))
    before = snapshot(engine)
    with pytest.raises(ValueError, match="exact"):
        engine.on_cross_agg_trade(packet(2**53, 2**53))
    assert snapshot(engine) == before


def test_native_weighted_reference_matches_python(engine):
    import narrowgate_cpp as native
    if not hasattr(native.TradeBarAggregator, "update_weighted_batch"):
        pytest.skip("isolated new native build required")
    other = SignalEngine(enable_ml=False, reference_trade_count_unit="individual_execution")
    other._cpp_signal = native
    other._cpp_cross_batch_enabled = True
    other._cpp_ref_perp_engine = native.SignalRefPerpFeatureEngine()
    events = [packet(), packet(13, 14, ts=T+1100), packet(15, 15, ts=T+2100)]
    for instance in (engine, other):
        instance.on_cross_agg_trade_batch(events)
    key = next(iter(engine._cross_current_bars))
    other._sync_cpp_cross_current_bar_locked(key)
    assert asdict(engine._cross_current_bars[key]) == pytest.approx(asdict(other._cross_current_bars[key]))
    for left, right in zip(engine._cross_bar_buffers[key], other._cross_bar_buffers[key], strict=True):
        assert asdict(left) == pytest.approx(asdict(right))
    before = snapshot(other)
    with pytest.raises(ValueError):
        other.on_cross_agg_trade_batch([packet(16, 17, ts=T+3000), packet(16, 18, ts=T+4000)])
    assert snapshot(other) == before
