from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from calendar_features import (
    CALENDAR_DATA_VERSION,
    CN_ADJUSTED_WORKDAYS_INT,
    CN_HOLIDAYS_INT,
    NYSE_HOLIDAYS_INT,
    SUPPORTED_CALENDAR_YEARS,
    US_FEDERAL_HOLIDAYS_INT,
    UnsupportedCalendarYearError,
    calendar_feature_frame,
    calendar_feature_names,
    calendar_scalar_features,
    quote_calendar_feature_values,
)


def test_calendar_data_identity_is_explicit() -> None:
    assert CALENDAR_DATA_VERSION == "2025-2026.v2"
    assert SUPPORTED_CALENDAR_YEARS == frozenset({2025, 2026})


def test_2025_calendar_metadata_is_retained() -> None:
    assert {value for value in NYSE_HOLIDAYS_INT if value // 10000 == 2025} == {
        20250101, 20250109, 20250120, 20250217, 20250418, 20250526,
        20250619, 20250704, 20250901, 20251127, 20251225,
    }
    assert {value for value in US_FEDERAL_HOLIDAYS_INT if value // 10000 == 2025} == {
        20250101, 20250109, 20250120, 20250217, 20250526, 20250619,
        20250704, 20250901, 20251013, 20251111, 20251127, 20251225,
    }
    assert {value for value in CN_HOLIDAYS_INT if value // 10000 == 2025} == {
        20250101,
        20250128, 20250129, 20250130, 20250131,
        20250201, 20250202, 20250203, 20250204,
        20250404, 20250405, 20250406,
        20250501, 20250502, 20250503, 20250504, 20250505,
        20250531, 20250601, 20250602,
        20251001, 20251002, 20251003, 20251004,
        20251005, 20251006, 20251007, 20251008,
    }
    assert {value for value in CN_ADJUSTED_WORKDAYS_INT if value // 10000 == 2025} == {
        20250126, 20250208, 20250427, 20250928, 20251011,
    }


@pytest.mark.parametrize(
    "local_date",
    [
        "2026-09-25", "2026-09-26", "2026-09-27",
        "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
        "2026-10-05", "2026-10-06", "2026-10-07",
    ],
)
def test_2026_h2_cn_holidays(local_date: str) -> None:
    features = calendar_scalar_features(f"{local_date}T12:00:00+08:00")
    assert features["cal_cn_is_holiday"] == 1.0
    assert features["cal_cn_is_adjusted_workday"] == 0.0
    assert features["cal_cn_is_workday"] == 0.0


@pytest.mark.parametrize("local_date", ["2026-09-20", "2026-10-10"])
def test_2026_h2_cn_adjusted_workdays(local_date: str) -> None:
    features = calendar_scalar_features(f"{local_date}T12:00:00+08:00")
    assert features["cal_cn_is_weekend"] == 1.0
    assert features["cal_cn_is_holiday"] == 0.0
    assert features["cal_cn_is_adjusted_workday"] == 1.0
    assert features["cal_cn_is_workday"] == 1.0


def test_vector_and_scalar_calendar_features_match() -> None:
    timestamps = pd.Series(
        pd.to_datetime(
            [
                "2025-10-01T00:30:00Z",
                "2026-03-08T07:30:00Z",
                "2026-07-03T15:00:00Z",
                "2026-09-24T16:00:00Z",
                "2026-11-01T06:30:00Z",
            ],
            utc=True,
        )
    )
    vector = calendar_feature_frame(timestamps)

    for row_index, timestamp in timestamps.items():
        scalar = calendar_scalar_features(timestamp.to_pydatetime())
        for name in calendar_feature_names():
            assert float(vector.loc[row_index, name]) == pytest.approx(scalar[name])


def test_utc_timestamp_uses_cn_local_date() -> None:
    before_midnight = calendar_scalar_features("2026-09-24T15:59:59Z")
    after_midnight = calendar_scalar_features("2026-09-24T16:00:00Z")

    assert before_midnight["cal_cn_is_holiday"] == 0.0
    assert before_midnight["cal_cn_is_holiday_eve"] == 1.0
    assert after_midnight["cal_cn_is_holiday"] == 1.0


def test_utc_timestamp_uses_us_local_date() -> None:
    before_midnight = calendar_scalar_features("2026-07-03T03:59:59Z")
    after_midnight = calendar_scalar_features("2026-07-03T04:00:00Z")

    assert before_midnight["cal_us_is_holiday_eve"] == 1.0
    assert before_midnight["cal_us_is_nyse_trading_day"] == 1.0
    assert after_midnight["cal_us_is_federal_holiday"] == 1.0
    assert after_midnight["cal_us_is_nyse_trading_day"] == 0.0


def test_us_dst_spring_forward_conversion() -> None:
    features = calendar_feature_frame(
        ["2026-03-08T06:30:00Z", "2026-03-08T07:30:00Z"]
    )
    assert features["cal_us_hour"].tolist() == [1, 3]


def test_us_dst_fall_back_conversion() -> None:
    features = calendar_feature_frame(
        ["2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"]
    )
    assert features["cal_us_hour"].tolist() == [1, 1]
    assert features["cal_us_is_sunday"].tolist() == [1, 1]


def test_holiday_eve_and_post_holiday_use_local_calendar_days() -> None:
    eve = calendar_scalar_features("2026-09-24T12:00:00+08:00")
    holiday = calendar_scalar_features("2026-09-25T12:00:00+08:00")
    post = calendar_scalar_features("2026-09-28T12:00:00+08:00")

    assert eve["cal_cn_is_holiday_eve"] == 1.0
    assert holiday["cal_cn_is_holiday"] == 1.0
    assert post["cal_cn_is_post_holiday"] == 1.0


@pytest.mark.parametrize("year", [2024, 2027])
def test_scalar_rejects_unsupported_local_calendar_year(year: int) -> None:
    with pytest.raises(
        UnsupportedCalendarYearError,
        match=rf"CN=\[{year}\].*US=\[{year}\].*supported=\[2025, 2026\]",
    ):
        calendar_scalar_features(f"{year}-06-01T12:00:00Z")


@pytest.mark.parametrize("year", [2024, 2027])
def test_vector_rejects_unsupported_local_calendar_year(year: int) -> None:
    with pytest.raises(
        UnsupportedCalendarYearError,
        match=rf"CN=\[{year}\].*US=\[{year}\].*supported=\[2025, 2026\]",
    ):
        calendar_feature_frame([f"{year}-06-01T12:00:00Z"])


def test_local_year_boundary_rejects_cn_2027_while_utc_and_us_are_2026() -> None:
    supported = calendar_scalar_features("2026-12-31T15:59:59Z")
    assert supported["cal_cn_hour"] == 23.0

    with pytest.raises(UnsupportedCalendarYearError, match=r"CN=\[2027\]") as exc:
        calendar_scalar_features("2026-12-31T16:00:00Z")
    assert "US=[2027]" not in str(exc.value)


def test_local_year_boundary_rejects_us_2024_while_utc_and_cn_are_2025() -> None:
    with pytest.raises(UnsupportedCalendarYearError, match=r"US=\[2024\]") as exc:
        calendar_feature_frame(["2025-01-01T00:00:00Z"])
    assert "CN=[2024]" not in str(exc.value)


@pytest.mark.parametrize("value", [None, pd.NaT, np.datetime64("NaT")])
def test_scalar_missing_timestamp_returns_zero_features(value: object) -> None:
    assert set(calendar_scalar_features(value).values()) == {0.0}


def test_vector_nat_returns_zero_features() -> None:
    features = calendar_feature_frame(pd.Series([pd.NaT]))
    assert np.count_nonzero(features.to_numpy()) == 0


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
def test_vector_preserves_datetime_index_physical_unit(unit: str) -> None:
    expected = pd.DatetimeIndex(
        ["2026-01-01T00:00:20Z", "2026-07-20T23:59:50Z"]
    ).as_unit(unit)

    features = calendar_feature_frame(expected)

    assert features["cal_utc_hour"].tolist() == [0, 23]
    assert features["cal_utc_weekday"].tolist() == [3, 0]
    assert features["cal_cn_hour"].tolist() == [8, 7]


@pytest.mark.parametrize(
    ("unit", "values"),
    [
        ("s", [1767225620, 1784591990]),
        ("ms", [1767225620000, 1784591990000]),
        ("us", [1767225620000000, 1784591990000000]),
        ("ns", [1767225620000000000, 1784591990000000000]),
    ],
)
def test_vector_infers_numeric_epoch_unit(unit: str, values: list[int]) -> None:
    del unit
    features = calendar_feature_frame(values)
    assert features["cal_utc_hour"].tolist() == [0, 23]
    assert features["cal_utc_weekday"].tolist() == [3, 0]


@pytest.mark.parametrize(
    "value",
    [
        1767225620,
        1767225620000,
        1767225620000000,
        1767225620000000000,
    ],
)
def test_scalar_and_vector_infer_the_same_numeric_epoch_unit(value: int) -> None:
    scalar = calendar_scalar_features(value)
    vector = calendar_feature_frame([value]).iloc[0]

    assert scalar["cal_utc_hour"] == 0.0
    assert scalar["cal_cn_hour"] == 8.0
    for name in calendar_feature_names():
        assert float(vector[name]) == pytest.approx(scalar[name])


def test_direct_scalar_1970_clock_remains_strictly_unsupported() -> None:
    with pytest.raises(
        UnsupportedCalendarYearError,
        match=r"CN=\[1970\].*US=\[1969\]",
    ):
        calendar_scalar_features(0)


def test_synthetic_relative_quote_clock_materializes_zero_calendar() -> None:
    features = {"quote_ts_ms": 7_000, "side": "BUY"}

    result = quote_calendar_feature_values(features)

    assert result["quote_ts_ms"] == 7_000
    assert result["side"] == "BUY"
    assert {result[name] for name in calendar_feature_names("quote_cal_")} == {0.0}


def test_live_epoch_quote_clock_materializes_real_calendar() -> None:
    result = quote_calendar_feature_values({"quote_ts_ms": 1767225620000})

    assert result["quote_cal_cn_is_holiday"] == 1.0
    assert result["quote_cal_cn_is_workday"] == 0.0


def test_relative_quote_clock_fallback_requires_a_numeric_value() -> None:
    with pytest.raises(UnsupportedCalendarYearError):
        quote_calendar_feature_values({"quote_ts_ms": "7000"})
