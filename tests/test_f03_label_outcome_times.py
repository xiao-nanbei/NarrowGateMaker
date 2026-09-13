"""The label dependency clock is not the name of its nominal horizon."""
import numpy as np
import pandas as pd

from features import feature_engineer as fe


NAT = np.iinfo(np.int64).min
SECOND = 1_000_000_000


def test_triplet_records_actual_bar_close_without_changing_label_values():
    ts = np.arange(60, dtype=np.int64) * SECOND
    close = np.linspace(100.0, 106.0, 60)
    high = np.full(60, 100.0)
    low = np.full(60, 100.0)
    low[13] = 98.0
    args = (ts, close, high, low, np.r_[0.0, np.diff(close)],
            np.array([10 * SECOND]), np.array([10]), np.array([99.0]),
            np.array([101.0]), np.array([100.0]), 10 * SECOND)
    old = fe._compute_label_triplet(*args)
    ret_end = np.full(1, NAT, dtype=np.int64)
    vol_end = np.full(1, NAT, dtype=np.int64)
    actual = fe._compute_label_triplet(*args, ret_end, vol_end)
    for expected, observed in zip(old, actual, strict=True):
        np.testing.assert_array_equal(expected, observed)
    assert ret_end.tolist() == [24 * SECOND]  # fill at13, close at23 visible at24
    assert vol_end.tolist() == [20 * SECOND]  # variance [10,20)


def test_toxicity_has_separate_side_end_times_and_missing_outcome_stays_unknown():
    ts = np.arange(30, dtype=np.int64) * SECOND
    high, low = np.full(30, 100.0), np.full(30, 100.0)
    low[11], high[14] = 98.0, 102.0
    args = (ts, np.full(30, 100.0), high, low, np.array([10 * SECOND, 29 * SECOND]),
            np.array([10, 29]), np.array([99.0, 99.0]), np.array([101.0, 101.0]),
            5 * SECOND)
    old = fe._compute_toxicity_pair(*args)
    bid_end, ask_end = np.full(2, NAT, dtype=np.int64), np.full(2, NAT, dtype=np.int64)
    actual = fe._compute_toxicity_pair(*args, bid_end, ask_end)
    for expected, observed in zip(old, actual, strict=True):
        np.testing.assert_array_equal(expected, observed)
    assert bid_end.tolist() == [17 * SECOND, NAT]
    assert ask_end.tolist() == [20 * SECOND, NAT]


def test_add_labels_outcome_helpers_are_opt_in_and_masked_with_labels(monkeypatch):
    seconds = pd.date_range("2026-01-01", periods=400, freq="s", tz="UTC")
    bars = pd.DataFrame({"close": 100.0, "high": 100.0, "low": 98.0}, index=seconds)
    features = pd.DataFrame({"close": 100.0}, index=seconds[::10])
    monkeypatch.setattr(fe, "_load_label_quote_params", lambda *a, **kw: {})
    monkeypatch.setattr(fe, "_quote_half_spread", lambda frame, *a: np.ones(len(frame)))
    old = fe.add_labels(features.copy(), bars)
    actual = fe.add_labels(features.copy(), bars, include_outcome_times=True)
    pd.testing.assert_frame_equal(actual[old.columns], old)
    helpers = [c for c in actual if c.startswith("label_outcome_end_")]
    assert len(helpers) == 13
    assert not any(c.startswith("label_outcome_end_") for c in old)
    for col in helpers:
        label = "label_" + col.removeprefix("label_outcome_end_")
        assert actual[col].isna().equals(actual[label].isna())
        valid = actual[col].notna()
        assert (actual.loc[valid, col] >= actual.index[valid] + pd.Timedelta(seconds=10)).all()
    assert actual.iloc[0]["label_outcome_end_ret_10s"] == seconds[21]
    assert actual.iloc[0]["label_outcome_end_vol_10s"] == seconds[20]


def test_sparse_traded_seconds_use_same_dense_label_grid_as_feature_pipeline(monkeypatch):
    seconds = pd.date_range("2026-01-01", periods=400, freq="s", tz="UTC")
    # Only every other second trades. Silence in this complete event-stream
    # fixture is not an unavailable observation and must not shorten horizons.
    sparse = pd.DataFrame({"close": 100.0, "high": 100.0, "low": 98.0,
                           "volume": 2.0, "trade_count": 1}, index=seconds[1::2])
    dense = fe.densify_bars_1s(sparse, calendar_tag="2026-01-01")
    assert pd.isna(dense.iloc[0]["close"])  # No future backfill at day start.
    assert dense.loc[seconds[2], "close"] == 100.0
    assert dense.loc[seconds[2], "low"] == 100.0  # Do not copy traded low.
    assert dense.loc[seconds[2], "volume"] == 0.0
    assert dense.loc[seconds[2], "trade_count"] == 0
    expected = pd.DataFrame({"close": 100.0, "high": 100.0, "low": 100.0},
                            index=seconds)
    expected.loc[seconds[1::2], "low"] = 98.0
    expected.iloc[0] = np.nan
    features = pd.DataFrame({"close": 100.0}, index=seconds[2:240:10])
    monkeypatch.setattr(fe, "_load_label_quote_params", lambda *a, **kw: {})
    monkeypatch.setattr(fe, "_quote_half_spread", lambda frame, *a: np.ones(len(frame)))
    actual = fe.add_labels(features.copy(), dense, include_outcome_times=True)
    reference = fe.add_labels(features.copy(), expected, include_outcome_times=True)
    pd.testing.assert_frame_equal(actual, reference)
    assert actual.iloc[0]["label_outcome_end_vol_10s"] == seconds[22]
    assert actual.iloc[0]["label_outcome_end_ret_10s"] == seconds[24]
