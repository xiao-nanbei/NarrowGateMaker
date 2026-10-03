"""Synthetic auxiliary input tests; no live polling or economic results."""

from dataclasses import replace

import pandas as pd
import pytest

from data.auxiliary import METRIC_FIELDS, metrics_observations, reconcile_metrics


def raw():
    frame = pd.DataFrame({"create_time": pd.date_range("2025-08-01", periods=288, freq="5min", tz="UTC"),
                          "symbol": "BTCUSDC"})
    for column in METRIC_FIELDS.values():
        frame[column] = 1.0
    return frame


def load(frame):
    return metrics_observations(frame, day="2025-08-01", symbol="BTCUSDC",
                               source_id="synthetic", availability_delay_ns=2_000_000_000)


def test_start_stamped_period_not_visible_early():
    first = load(raw())[0]
    assert first.period_end_ns == pd.Timestamp("2025-08-01T00:05Z").value
    with pytest.raises(ValueError, match="not yet visible"):
        first.signal_values(decision_ns=first.ready_ns - 1)
    assert first.signal_values(decision_ns=first.ready_ns)["ts_ms"] == first.ready_ns // 1_000_000


def test_real_gaps_not_filled():
    rows = load(raw().drop(index=[5, 6, 7]))
    assert len(rows) == 285
    assert rows[5].period_end_ns - rows[4].period_end_ns == 1_200_000_000_000


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1])
def test_invalid_values_fail_not_neutralized(bad):
    frame = raw()
    frame.loc[5, "sum_open_interest"] = bad
    with pytest.raises(ValueError, match="invalid metric"):
        load(frame)


def test_symbol_and_period_conflicts_rejected():
    frame = raw()
    frame.loc[0, "symbol"] = "BTCUSDT"
    with pytest.raises(ValueError, match="market"):
        load(frame)
    row = load(raw())[0]
    assert reconcile_metrics([row, row]) == (row,)
    altered = replace(row, values=tuple((key, value+1) for key, value in row.values))
    with pytest.raises(ValueError, match="conflicting"):
        reconcile_metrics([row, altered])


def test_end_stamped_period_not_shifted_twice():
    frame = raw()
    frame["create_time"] += pd.Timedelta(minutes=5)
    assert load(frame) == load(raw())
