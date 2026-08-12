from __future__ import annotations

import pandas as pd

from features.preprocess_metrics import normalize_feature_ready_time


def _frame(times: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "create_time": times,
            "sum_open_interest": range(len(times)),
        }
    )


def test_start_stamped_metrics_become_causal_at_interval_end() -> None:
    times = pd.date_range(
        "2026-07-12 00:00:00",
        periods=288,
        freq="5min",
    ).astype(str).tolist()
    result = normalize_feature_ready_time(
        _frame(times),
        day="2026-07-12",
    )

    assert result["create_time"].iloc[0] == pd.Timestamp(
        "2026-07-12 00:05:00",
        tz="UTC",
    )
    assert result["create_time"].iloc[-1] == pd.Timestamp(
        "2026-07-13 00:00:00",
        tz="UTC",
    )


def test_end_stamped_metrics_are_not_shifted_again() -> None:
    times = pd.date_range(
        "2026-01-01 00:05:00",
        periods=288,
        freq="5min",
    ).astype(str).tolist()
    result = normalize_feature_ready_time(
        _frame(times),
        day="2026-01-01",
    )

    assert result["create_time"].iloc[0] == pd.Timestamp(
        "2026-01-01 00:05:00",
        tz="UTC",
    )
    assert result["create_time"].iloc[-1] == pd.Timestamp(
        "2026-01-02 00:00:00",
        tz="UTC",
    )


def test_unknown_metrics_timestamp_bounds_fail_fast() -> None:
    times = pd.date_range(
        "2026-07-12 00:10:00",
        periods=288,
        freq="5min",
    ).astype(str).tolist()
    frame = _frame(times)

    try:
        normalize_feature_ready_time(frame, day="2026-07-12")
    except ValueError as exc:
        assert "unrecognized metrics timestamp bounds" in str(exc)
    else:
        raise AssertionError("expected invalid timestamp bounds to fail")


def test_incomplete_metrics_day_fails_fast() -> None:
    times = pd.date_range(
        "2026-07-12 00:00:00",
        periods=287,
        freq="5min",
    ).astype(str).tolist()

    try:
        normalize_feature_ready_time(_frame(times), day="2026-07-12")
    except ValueError as exc:
        assert "expected 288 unique metrics rows" in str(exc)
    else:
        raise AssertionError("expected incomplete metrics day to fail")


def test_small_source_timestamp_jitter_is_preserved() -> None:
    times = pd.date_range(
        "2026-07-12 00:05:00",
        periods=288,
        freq="5min",
    ).to_series(index=None)
    times.iloc[32] += pd.Timedelta(seconds=2)

    result = normalize_feature_ready_time(
        _frame(times.astype(str).tolist()),
        day="2026-07-12",
    )

    assert result["create_time"].iloc[32].second == 2
