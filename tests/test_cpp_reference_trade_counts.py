"""Native reference packets carry child counts, never replicated quantity."""

import numpy as np
import pytest


native = pytest.importorskip("narrowgate_cpp")

BAR_FIELDS = (
    "ts_ms", "open", "high", "low", "close", "volume", "trade_count",
    "buy_volume", "sell_volume", "buy_count", "sell_count", "quote_qty",
    "buy_quote_qty", "sell_quote_qty", "max_same_side_run", "max_buy_run",
    "max_sell_run", "buy_price_high", "buy_price_low", "sell_price_high",
    "sell_price_low",
)


def arrays(ts=(100, 300, 1100), prices=(100, 101, 102),
           quantities=(6, 8, 2), sides=(0, 1, 0), counts=(3, 4, 2)):
    return (
        np.asarray(ts, dtype=np.int64),
        np.asarray(prices, dtype=np.float64),
        np.asarray(quantities, dtype=np.float64),
        np.asarray(sides, dtype=np.uint8),
        np.asarray(counts, dtype=np.int64),
    )


def bar_values(bar):
    return tuple(getattr(bar, name) for name in BAR_FIELDS)


def test_weighted_counts_preserve_price_quantity_and_side_runs():
    aggregator = native.TradeBarAggregator(True)
    completed = aggregator.update_weighted_batch(*arrays())
    bar = completed[0]
    assert (bar.trade_count, bar.buy_count, bar.sell_count) == (7, 3, 4)
    assert (bar.volume, bar.buy_volume, bar.sell_volume) == (14, 6, 8)
    assert (bar.quote_qty, bar.buy_quote_qty, bar.sell_quote_qty) == (1408, 600, 808)
    assert (bar.open, bar.high, bar.low, bar.close) == (100, 101, 100, 101)
    assert (bar.max_same_side_run, bar.max_buy_run, bar.max_sell_run) == (4, 3, 4)
    assert aggregator.current_bar().trade_count == 2


@pytest.mark.parametrize("track_runs", [False, True])
def test_weighted_batch_splits_and_same_side_run_cross_bucket(track_runs):
    batch = arrays(ts=(100, 1200, 1300, 3000), prices=(100,) * 4,
                   quantities=(2, 3, 4, 5), sides=(0, 0, 0, 1), counts=(2, 3, 4, 5))
    whole = native.TradeBarAggregator(track_runs)
    split = native.TradeBarAggregator(track_runs)
    expected = whole.update_weighted_batch(*batch)
    actual = []
    for index in range(4):
        actual.extend(split.update_weighted_batch(*(a[index:index + 1] for a in batch)))
    assert [bar_values(b) for b in actual] == [bar_values(b) for b in expected]
    assert bar_values(split.current_bar()) == bar_values(whole.current_bar())
    assert expected[1].max_buy_run == (9 if track_runs else 1)
    assert expected[2].volume == expected[2].trade_count == 0  # Legacy empty-second policy.


def test_old_unweighted_interface_still_counts_one_packet():
    weighted = native.TradeBarAggregator()
    legacy = native.TradeBarAggregator()
    batch = arrays(counts=(1, 1, 1))
    assert [bar_values(b) for b in weighted.update_weighted_batch(*batch)] == [
        bar_values(b) for b in legacy.update_batch(*batch[:4])
    ]
    assert bar_values(weighted.current_bar()) == bar_values(legacy.current_bar())
    assert legacy.current_bar().trade_count == 1


@pytest.mark.parametrize("kind", ["aggregator", "reference"])
@pytest.mark.parametrize("bad", [
    "zero", "negative", "float", "bool", "uint64", "count_overflow", "short",
    "matrix", "nan_price", "inf_quantity", "bad_side", "side_overflow",
    "time_reversal", "old_time", "zero_time",
])
def test_invalid_weighted_batch_is_rejected_before_any_mutation(kind, bad):
    engine = (native.TradeBarAggregator() if kind == "aggregator"
              else native.SignalRefPerpFeatureEngine())
    update = (engine.update_weighted_batch if kind == "aggregator"
              else engine.update_trade_weighted_batch)
    initial = arrays(ts=(1000,), prices=(99,), quantities=(2,), sides=(0,), counts=(2,))
    update(*initial)
    before = (bar_values(engine.current_bar()) if kind == "aggregator"
              else (engine.bar_count(), engine.prepare(2000, 100).revision,
                    tuple(engine.prepare(2000, 100).values)))
    batch = list(arrays(ts=(1100, 1200, 2300)))
    if bad == "zero":
        batch[4][1] = 0
    elif bad == "negative":
        batch[4][1] = -1
    elif bad in {"float", "bool", "uint64"}:
        batch[4] = batch[4].astype({"float": np.float64, "bool": bool, "uint64": np.uint64}[bad])
        if bad == "float":
            batch[4][1] = 1.5
    elif bad == "count_overflow":
        batch[4][1] = 2**53 + 1
    elif bad == "short":
        batch[4] = batch[4][:2]
    elif bad == "matrix":
        batch[1] = batch[1].reshape(1, 3)
    elif bad == "nan_price":
        batch[1][1] = np.nan
    elif bad == "inf_quantity":
        batch[2][1] = np.inf
    elif bad == "bad_side":
        batch[3][1] = 2
    elif bad == "side_overflow":
        batch[3] = np.array([0, 256, 0], dtype=np.int64)
    elif bad == "time_reversal":
        batch[0][1] = 1050
    elif bad == "old_time":
        batch[0][0] = 999
    else:
        batch[0][1] = 0
    with pytest.raises((ValueError, TypeError)):
        update(*batch)
    after = (bar_values(engine.current_bar()) if kind == "aggregator"
             else (engine.bar_count(), engine.prepare(2000, 100).revision,
                   tuple(engine.prepare(2000, 100).values)))
    assert after == before


@pytest.mark.parametrize("track_runs,second_ts", [(False, 200), (True, 1200)])
def test_exact_count_limit_includes_existing_bar_or_cross_bar_run(track_runs, second_ts):
    engine = native.TradeBarAggregator(track_runs)
    engine.update_weighted_batch(*arrays(ts=(100,), prices=(100,), quantities=(1,),
                                        sides=(0,), counts=(2**53,)))
    before = bar_values(engine.current_bar())
    with pytest.raises(ValueError, match="exact double range"):
        engine.update_weighted_batch(*arrays(ts=(second_ts,), prices=(100,),
                                            quantities=(1,), sides=(0,), counts=(1,)))
    assert bar_values(engine.current_bar()) == before


def test_reference_weighted_features_equal_individual_count_semantics():
    weighted = native.SignalRefPerpFeatureEngine()
    individual = native.SignalRefPerpFeatureEngine()
    packets = arrays(ts=tuple(range(100, 80100, 1000)),
                     prices=tuple(100 + i % 3 for i in range(80)),
                     quantities=(6,) * 80, sides=tuple(i % 2 for i in range(80)),
                     counts=(3,) * 80)
    weighted.update_trade_weighted_batch(*packets)
    individual.update_trade_batch(
        np.repeat(packets[0], 3), np.repeat(packets[1], 3),
        np.repeat(packets[2] / 3, 3), np.repeat(packets[3], 3),
    )
    assert weighted.bar_count() == individual.bar_count()
    np.testing.assert_allclose(weighted.prepare(79000, 100).values,
                               individual.prepare(79000, 100).values, rtol=0, atol=1e-12)
    names = list(native.SIGNAL_REF_PERP_FEATURE_NAMES)
    values = dict(zip(names, weighted.prepare(79000, 100).values, strict=True))
    assert any("intensity" in name and value > 0 for name, value in values.items())


def test_empty_and_noncontiguous_int64_counts_are_supported():
    engine = native.TradeBarAggregator()
    batch = arrays()
    assert engine.update_weighted_batch(*(a[:0] for a in batch)) == []
    assert engine.current_bar() is None
    counts = np.array([3, -1, 4, -1, 2, -1], dtype=np.int64)[::2]
    assert not counts.flags.c_contiguous
    engine.update_weighted_batch(*batch[:4], counts)
    assert engine.current_bar().trade_count == 2
