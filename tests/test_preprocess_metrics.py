from __future__ import annotations

import sys

import pandas as pd
import pytest

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


@pytest.mark.parametrize("start", ["00:00:00", "00:05:00"])
def test_explicit_sparse_metrics_keep_observed_values_and_causal_gap(tmp_path, start):
    times = pd.date_range(f"2026-07-12 {start}", periods=288, freq="5min")
    frame = _frame(times.astype(str).tolist()).drop(index=[20, 21, 22])
    result = normalize_feature_ready_time(frame, day="2026-07-12", allow_missing_observations=True)
    assert len(result) == 285
    assert result.sum_open_interest.tolist() == frame.sum_open_interest.tolist()
    assert result.create_time.diff().max() == pd.Timedelta(minutes=20)
    assert result.create_time.iloc[0] == pd.Timestamp("2026-07-12T00:05:00Z")
    assert result.attrs['missing_observation_count'] == 3
    assert result.attrs['missing_observations_filled'] is False
    path = tmp_path / 'metrics.parquet'
    result.to_parquet(path)
    assert pd.read_parquet(path).attrs == result.attrs
    duplicate = pd.concat([frame, frame.iloc[[0]]])
    with pytest.raises(ValueError):
        normalize_feature_ready_time(duplicate, day="2026-07-12", allow_missing_observations=True)


@pytest.mark.parametrize("start", ["00:00:00", "00:05:00"])
def test_metrics_canonical_strings_match_explicit_csv(tmp_path, monkeypatch, start):
    from features import preprocess_metrics as module

    day = "2026-07-12"
    frame = pd.DataFrame({"create_time": pd.date_range(f"{day} {start}", periods=288, freq="5min").astype(str)})
    for column in module.KEEP_COLS:
        frame[column] = [str(10.5 + index / 10) for index in range(288)]
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    frame.to_csv(legacy / f"BTCUSDC-metrics-{day}.csv", index=False)
    raw = tmp_path / "raw/history/binance_futures/BTCUSDC" / day / "metrics.parquet"
    raw.parent.mkdir(parents=True)
    frame.to_parquet(raw)
    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    outputs = []
    for mode in ("explicit", "canonical"):
        out = tmp_path / mode
        argv = ["preprocess_metrics", "--symbol", "BTCUSDC", "--file", day, "--output-dir", str(out)]
        if mode == "explicit":
            argv += ["--input-dir", str(legacy)]
        monkeypatch.setattr(sys, "argv", argv)
        module.main()
        outputs.append(pd.read_parquet(out / f"BTCUSDC-metrics-{day}.parquet"))
    pd.testing.assert_frame_equal(*outputs)
    assert outputs[1].index[0] == pd.Timestamp(f"{day}T00:05:00Z")
    assert outputs[1].index.unit == "us"
    assert all(dtype == "float64" for dtype in outputs[1].dtypes)
