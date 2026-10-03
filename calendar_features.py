"""Unified UTC/session/calendar feature helpers for NarrowGate research.

All training, replay, shadow-label, and bucket-audit code should derive
calendar/session flags through this module instead of hard-coding holidays in
individual scripts.  The project currently studies daily UTC market data, while
some features intentionally project those UTC timestamps into US/Eastern or
Asia/Shanghai to test whether regime effects are really calendar/session
effects.

The holiday tables below are centralized 2025-2026 fallbacks.  Timestamps whose
China or US local year falls outside that explicit support window fail fast so
an incomplete table cannot silently label a future holiday as a normal day.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

UTC = timezone.utc
US_EASTERN = ZoneInfo("America/New_York")
CN_TZ = ZoneInfo("Asia/Shanghai")

CALENDAR_DATA_VERSION = "2025-2026.v2"
SUPPORTED_CALENDAR_YEARS = frozenset({2025, 2026})
_MAX_RELATIVE_QUOTE_CLOCK_SECONDS = 366 * 24 * 60 * 60


class UnsupportedCalendarYearError(ValueError):
    """Raised when a valid local timestamp is outside the frozen calendar."""


def _validate_supported_local_years(*, cn_years: Any, us_years: Any) -> None:
    """Apply one unsupported-year contract to vector and scalar call paths."""

    def normalized(values: Any) -> set[int]:
        if isinstance(values, (int, np.integer)):
            return {int(values)}
        return {int(value) for value in values if not pd.isna(value)}

    unsupported = {
        "CN": sorted(normalized(cn_years) - SUPPORTED_CALENDAR_YEARS),
        "US": sorted(normalized(us_years) - SUPPORTED_CALENDAR_YEARS),
    }
    details = [f"{zone}={years}" for zone, years in unsupported.items() if years]
    if details:
        supported = sorted(SUPPORTED_CALENDAR_YEARS)
        raise UnsupportedCalendarYearError(
            "calendar year outside supported local-year range: "
            f"{', '.join(details)}; supported={supported}; "
            f"calendar_data_version={CALENDAR_DATA_VERSION}"
        )

# NYSE observed holidays.  This preserves the old feature_engineer.py behavior
# and also includes the 2025 Carter National Day of Mourning closure.
NYSE_HOLIDAYS_INT = frozenset({
    # 2025
    20250101, 20250109, 20250120, 20250217, 20250418, 20250526,
    20250619, 20250704, 20250901, 20251127, 20251225,
    # 2026
    20260101, 20260119, 20260216, 20260403, 20260525,
    20260619, 20260703, 20260907, 20261126, 20261225,
})

US_FEDERAL_HOLIDAYS_INT = frozenset({
    # 2025
    20250101, 20250109, 20250120, 20250217, 20250526,
    20250619, 20250704, 20250901, 20251013, 20251111, 20251127, 20251225,
    # 2026
    20260101, 20260119, 20260216, 20260525,
    20260619, 20260703, 20260907, 20261012, 20261111, 20261126, 20261225,
})

# China mainland holiday/workday fallback.  These flags are for market-regime
# evidence only; crypto trades 24/7, so they should not be treated as hard data
# availability filters.
CN_HOLIDAYS_INT = frozenset({
    # 2025
    20250101,
    20250128, 20250129, 20250130, 20250131, 20250201, 20250202, 20250203, 20250204,
    20250404, 20250405, 20250406,
    20250501, 20250502, 20250503, 20250504, 20250505,
    20250531, 20250601, 20250602,
    20251001, 20251002, 20251003, 20251004, 20251005, 20251006, 20251007, 20251008,
    # 2026 official full-year schedule: 国办发明电〔2025〕7号.
    20260101, 20260102, 20260103,
    20260215, 20260216, 20260217, 20260218, 20260219, 20260220, 20260221, 20260222, 20260223,
    20260404, 20260405, 20260406,
    20260501, 20260502, 20260503, 20260504, 20260505,
    20260619, 20260620, 20260621,
    20260925, 20260926, 20260927,
    20261001, 20261002, 20261003, 20261004, 20261005, 20261006, 20261007,
})

CN_ADJUSTED_WORKDAYS_INT = frozenset({
    # 2025
    20250126, 20250208, 20250427, 20250928, 20251011,
    # 2026 official full-year schedule: 国办发明电〔2025〕7号.
    20260104, 20260214, 20260228, 20260426, 20260509, 20260920, 20261010,
})

_BASE_CALENDAR_FEATURES = [
    "utc_hour",
    "utc_weekday",
    "utc_is_weekend",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "session_asia",
    "session_tokyo",
    "session_singapore_hk",
    "session_europe",
    "session_london",
    "session_america",
    "session_us_extended",
    "session_asia_europe_overlap",
    "session_europe_america_overlap",
    "session_tokyo_singapore_overlap",
    "session_london_us_overlap",
    "session_active_count",
    "cn_hour",
    "cn_weekday",
    "cn_is_weekend",
    "cn_is_holiday",
    "cn_is_adjusted_workday",
    "cn_is_workday",
    "cn_is_holiday_eve",
    "cn_is_post_holiday",
    "us_hour",
    "us_weekday",
    "us_is_weekend",
    "us_is_sunday",
    "us_is_sunday_evening",
    "us_is_federal_holiday",
    "us_is_nyse_trading_day",
    "us_is_regular_hours",
    "us_is_premarket",
    "us_is_afterhours",
    "us_is_holiday_eve",
    "us_is_post_holiday",
    "minutes_to_us_open",
    "minutes_to_us_close",
    "is_weekday_us_rth",
    "is_weekend_core",
]

_LEGACY_MAP = {
    "hour_sin": "hour_sin",
    "hour_cos": "hour_cos",
    "dow_sin": "dow_sin",
    "dow_cos": "dow_cos",
    "session_asia": "session_asia",
    "session_tokyo": "session_tokyo",
    "session_singapore_hk": "session_singapore_hk",
    "session_europe": "session_europe",
    "session_london": "session_london",
    "session_america": "session_america",
    "session_us_extended": "session_us_extended",
    "session_asia_europe_overlap": "session_asia_europe_overlap",
    "session_europe_america_overlap": "session_europe_america_overlap",
    "session_tokyo_singapore_overlap": "session_tokyo_singapore_overlap",
    "session_london_us_overlap": "session_london_us_overlap",
    "session_active_count": "session_active_count",
    "us_is_nyse_trading_day": "is_us_trading_day",
    "us_is_regular_hours": "is_us_regular_hours",
    "us_is_premarket": "is_us_premarket",
    "minutes_to_us_open": "minutes_to_us_open",
    "minutes_to_us_close": "minutes_to_us_close",
}


def calendar_feature_names(prefix: str = "cal_") -> list[str]:
    return [f"{prefix}{name}" for name in _BASE_CALENDAR_FEATURES]


def legacy_calendar_feature_names() -> list[str]:
    """Return deprecated unprefixed aliases excluded from new model bundles."""

    return sorted(set(_LEGACY_MAP.values()))


def _numeric_timestamp_unit(magnitude: float) -> str:
    """Infer s/ms/us/ns from a finite absolute Unix timestamp magnitude."""

    if magnitude >= 1e17:
        return "ns"
    if magnitude >= 1e14:
        return "us"
    if magnitude >= 1e11:
        return "ms"
    return "s"


def _to_datetime_series(values: Any) -> pd.Series:
    if isinstance(values, pd.Series):
        raw = values
    elif isinstance(values, pd.Index):
        raw = pd.Series(values, index=values)
    else:
        raw = pd.Series(values)

    # Pandas 3 may preserve parquet indexes as datetime64[ms] or
    # datetime64[us].  Converting those values to integers first loses the
    # physical unit and can turn a 2026 microsecond timestamp into a 1970
    # nanosecond timestamp.  Datetime-like inputs must retain their dtype.
    if isinstance(raw.dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_any_dtype(raw.dtype):
        parsed = pd.to_datetime(raw, utc=True, errors="coerce")
        return pd.Series(parsed, index=raw.index)

    numeric = pd.to_numeric(raw, errors="coerce")
    if numeric.notna().any():
        arr = numeric.to_numpy(dtype=np.float64, copy=False)
        finite = np.isfinite(arr)
        magnitude = np.nanmax(np.abs(arr[finite])) if finite.any() else 0.0
        unit = _numeric_timestamp_unit(float(magnitude))
        parsed = pd.to_datetime(arr, unit=unit, utc=True, errors="coerce")
        return pd.Series(parsed, index=raw.index)
    parsed = pd.to_datetime(raw, utc=True, errors="coerce")
    if isinstance(parsed, pd.Series):
        return parsed
    return pd.Series(parsed, index=raw.index)


def _date_int(local_ts: pd.Series) -> pd.Series:
    return (
        local_ts.dt.year.fillna(0).astype(np.int32) * 10000
        + local_ts.dt.month.fillna(0).astype(np.int32) * 100
        + local_ts.dt.day.fillna(0).astype(np.int32)
    )


def _shifted_date_int(local_ts: pd.Series, days: int) -> pd.Series:
    shifted = local_ts.dt.normalize() + pd.Timedelta(days=days)
    return _date_int(shifted)


def calendar_feature_frame(values: Any, *, prefix: str = "cal_") -> pd.DataFrame:
    """Return numeric calendar/session features for UTC-like timestamps."""

    ts = _to_datetime_series(values)
    idx = ts.index
    valid = ts.notna()

    utc_hour = ts.dt.hour.fillna(-1).astype(np.int16)
    utc_weekday = ts.dt.dayofweek.fillna(-1).astype(np.int16)
    minute = ts.dt.minute.fillna(0).astype(np.float64)
    second = ts.dt.second.fillna(0).astype(np.float64)
    hour_frac = utc_hour.clip(lower=0).astype(np.float64) + minute / 60.0 + second / 3600.0

    cn = ts.dt.tz_convert(CN_TZ)
    us = ts.dt.tz_convert(US_EASTERN)
    _validate_supported_local_years(
        cn_years=cn.loc[valid].dt.year.unique(),
        us_years=us.loc[valid].dt.year.unique(),
    )
    cn_date = _date_int(cn)
    cn_next = _shifted_date_int(cn, 1)
    cn_prev = _shifted_date_int(cn, -1)
    cn_weekday = cn.dt.dayofweek.fillna(-1).astype(np.int16)
    cn_is_weekend = cn_weekday >= 5
    cn_is_holiday = np.isin(cn_date.to_numpy(), list(CN_HOLIDAYS_INT))
    cn_is_adjusted = np.isin(cn_date.to_numpy(), list(CN_ADJUSTED_WORKDAYS_INT))
    cn_is_workday = ((cn_weekday < 5) & ~cn_is_holiday) | cn_is_adjusted

    us_date = _date_int(us)
    us_next = _shifted_date_int(us, 1)
    us_prev = _shifted_date_int(us, -1)
    us_weekday = us.dt.dayofweek.fillna(-1).astype(np.int16)
    us_minutes = (
        us.dt.hour.fillna(-1).astype(np.float64) * 60.0
        + us.dt.minute.fillna(0).astype(np.float64)
        + us.dt.second.fillna(0).astype(np.float64) / 60.0
    )
    us_is_weekday = us_weekday < 5
    us_is_nyse_holiday = np.isin(us_date.to_numpy(), list(NYSE_HOLIDAYS_INT))
    us_is_federal_holiday = np.isin(us_date.to_numpy(), list(US_FEDERAL_HOLIDAYS_INT))
    us_is_trading = us_is_weekday & ~us_is_nyse_holiday
    us_is_rth = us_is_trading & (us_minutes >= 570.0) & (us_minutes < 960.0)
    us_is_pre = us_is_trading & (us_minutes >= 240.0) & (us_minutes < 570.0)
    us_is_after = us_is_trading & (us_minutes >= 960.0) & (us_minutes < 1200.0)

    out = pd.DataFrame(index=idx)
    out[f"{prefix}utc_hour"] = utc_hour.where(valid, -1).astype(np.int16)
    out[f"{prefix}utc_weekday"] = utc_weekday.where(valid, -1).astype(np.int16)
    out[f"{prefix}utc_is_weekend"] = ((utc_weekday >= 5) & valid).astype(np.int8)
    out[f"{prefix}hour_sin"] = np.sin(2 * math.pi * hour_frac / 24.0).where(valid, 0.0)
    out[f"{prefix}hour_cos"] = np.cos(2 * math.pi * hour_frac / 24.0).where(valid, 0.0)
    out[f"{prefix}dow_sin"] = np.sin(2 * math.pi * utc_weekday.clip(lower=0) / 7.0).where(valid, 0.0)
    out[f"{prefix}dow_cos"] = np.cos(2 * math.pi * utc_weekday.clip(lower=0) / 7.0).where(valid, 0.0)
    # Session flags are intentionally multi-label.  Crypto is 24/7, so these
    # are regime proxies, not market-open filters.  UTC day remains the only
    # training/backtest day boundary.
    session_tokyo = (utc_hour >= 0) & (utc_hour < 6)
    session_singapore_hk = (utc_hour >= 1) & (utc_hour < 9)
    session_london = (utc_hour >= 8) & (utc_hour < 16)
    session_america = (utc_hour >= 13) & (utc_hour < 21)
    session_us_extended = us_is_pre | us_is_after
    session_asia = session_tokyo | session_singapore_hk
    session_europe = session_london
    session_asia_europe_overlap = session_singapore_hk & session_london
    session_europe_america_overlap = session_london & session_america
    session_tokyo_singapore_overlap = session_tokyo & session_singapore_hk
    session_london_us_overlap = session_europe_america_overlap
    active_count = (
        session_tokyo.astype(np.int16)
        + session_singapore_hk.astype(np.int16)
        + session_london.astype(np.int16)
        + session_america.astype(np.int16)
    )
    out[f"{prefix}session_asia"] = (session_asia & valid).astype(np.int8)
    out[f"{prefix}session_tokyo"] = (session_tokyo & valid).astype(np.int8)
    out[f"{prefix}session_singapore_hk"] = (session_singapore_hk & valid).astype(np.int8)
    out[f"{prefix}session_europe"] = (session_europe & valid).astype(np.int8)
    out[f"{prefix}session_london"] = (session_london & valid).astype(np.int8)
    out[f"{prefix}session_america"] = (session_america & valid).astype(np.int8)
    out[f"{prefix}session_us_extended"] = (session_us_extended & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}session_asia_europe_overlap"] = (session_asia_europe_overlap & valid).astype(np.int8)
    out[f"{prefix}session_europe_america_overlap"] = (session_europe_america_overlap & valid).astype(np.int8)
    out[f"{prefix}session_tokyo_singapore_overlap"] = (session_tokyo_singapore_overlap & valid).astype(np.int8)
    out[f"{prefix}session_london_us_overlap"] = (session_london_us_overlap & valid).astype(np.int8)
    out[f"{prefix}session_active_count"] = np.where(valid, active_count, 0).astype(np.int8)

    out[f"{prefix}cn_hour"] = cn.dt.hour.fillna(-1).astype(np.int16)
    out[f"{prefix}cn_weekday"] = cn_weekday.where(valid, -1).astype(np.int16)
    out[f"{prefix}cn_is_weekend"] = (cn_is_weekend & valid).astype(np.int8)
    out[f"{prefix}cn_is_holiday"] = (cn_is_holiday & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}cn_is_adjusted_workday"] = (cn_is_adjusted & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}cn_is_workday"] = (cn_is_workday & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}cn_is_holiday_eve"] = (np.isin(cn_next.to_numpy(), list(CN_HOLIDAYS_INT)) & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}cn_is_post_holiday"] = (np.isin(cn_prev.to_numpy(), list(CN_HOLIDAYS_INT)) & valid.to_numpy()).astype(np.int8)

    out[f"{prefix}us_hour"] = us.dt.hour.fillna(-1).astype(np.int16)
    out[f"{prefix}us_weekday"] = us_weekday.where(valid, -1).astype(np.int16)
    out[f"{prefix}us_is_weekend"] = ((us_weekday >= 5) & valid).astype(np.int8)
    out[f"{prefix}us_is_sunday"] = ((us_weekday == 6) & valid).astype(np.int8)
    out[f"{prefix}us_is_sunday_evening"] = ((us_weekday == 6) & (us_minutes >= 18 * 60) & valid).astype(np.int8)
    out[f"{prefix}us_is_federal_holiday"] = (us_is_federal_holiday & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}us_is_nyse_trading_day"] = (us_is_trading & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}us_is_regular_hours"] = (us_is_rth & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}us_is_premarket"] = (us_is_pre & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}us_is_afterhours"] = (us_is_after & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}us_is_holiday_eve"] = (np.isin(us_next.to_numpy(), list(NYSE_HOLIDAYS_INT)) & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}us_is_post_holiday"] = (np.isin(us_prev.to_numpy(), list(NYSE_HOLIDAYS_INT)) & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}minutes_to_us_open"] = np.where(us_is_trading & (us_minutes < 570.0), (570.0 - us_minutes) / 570.0, 0.0)
    out[f"{prefix}minutes_to_us_close"] = np.where(us_is_rth, (960.0 - us_minutes) / 390.0, 0.0)
    out[f"{prefix}is_weekday_us_rth"] = (us_is_rth & valid.to_numpy()).astype(np.int8)
    out[f"{prefix}is_weekend_core"] = ((utc_weekday >= 5) & valid).astype(np.int8)
    if (~valid).any():
        out.loc[~valid, :] = 0
    return out


def add_calendar_features(
    frame: pd.DataFrame,
    *,
    ts_col: str | None = None,
    prefix: str = "cal_",
    include_legacy: bool = False,
) -> pd.DataFrame:
    """Attach calendar features to a DataFrame.

    `include_legacy=True` writes the old model feature names as aliases so
    existing main-model bundles keep their feature schema unchanged.
    """

    out = frame.copy()
    values = out[ts_col] if ts_col and ts_col in out.columns else out.index
    cal = calendar_feature_frame(values, prefix=prefix)
    cal.index = out.index
    for col in cal.columns:
        out[col] = cal[col].to_numpy()
    if include_legacy:
        for base, legacy in _LEGACY_MAP.items():
            out[legacy] = out[f"{prefix}{base}"]
    return out


def _coerce_scalar_datetime(value: Any | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (datetime, np.datetime64)) and pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        parsed = pd.to_datetime([value], utc=True, errors="coerce")[0]
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()
    if not math.isfinite(numeric):
        return None
    scale = {
        "s": 1.0,
        "ms": 1_000.0,
        "us": 1_000_000.0,
        "ns": 1_000_000_000.0,
    }[_numeric_timestamp_unit(abs(numeric))]
    return datetime.fromtimestamp(numeric / scale, tz=UTC)


def calendar_scalar_features(
    value: Any | None = None,
    *,
    prefix: str = "cal_",
    include_legacy: bool = False,
) -> dict[str, float]:
    """Scalar calendar features for live paths without per-call pandas code."""

    ts = _coerce_scalar_datetime(value)
    if ts is None:
        out = {name: 0.0 for name in calendar_feature_names(prefix)}
        if include_legacy:
            out.update({legacy: 0.0 for legacy in _LEGACY_MAP.values()})
        return out

    utc_hour = ts.hour
    utc_weekday = ts.weekday()
    hour_frac = utc_hour + ts.minute / 60.0 + ts.second / 3600.0
    cn = ts.astimezone(CN_TZ)
    us = ts.astimezone(US_EASTERN)
    _validate_supported_local_years(cn_years={cn.year}, us_years={us.year})

    def dint(dt: datetime) -> int:
        return dt.year * 10000 + dt.month * 100 + dt.day

    cn_date = dint(cn)
    cn_prev = dint(cn - pd.Timedelta(days=1).to_pytimedelta())
    cn_next = dint(cn + pd.Timedelta(days=1).to_pytimedelta())
    us_date = dint(us)
    us_prev = dint(us - pd.Timedelta(days=1).to_pytimedelta())
    us_next = dint(us + pd.Timedelta(days=1).to_pytimedelta())
    cn_weekday = cn.weekday()
    us_weekday = us.weekday()
    cn_holiday = cn_date in CN_HOLIDAYS_INT
    cn_adjusted = cn_date in CN_ADJUSTED_WORKDAYS_INT
    cn_workday = ((cn_weekday < 5) and not cn_holiday) or cn_adjusted
    us_minutes = us.hour * 60.0 + us.minute + us.second / 60.0
    us_trading = us_weekday < 5 and us_date not in NYSE_HOLIDAYS_INT
    us_rth = us_trading and 570.0 <= us_minutes < 960.0
    us_pre = us_trading and 240.0 <= us_minutes < 570.0
    us_after = us_trading and 960.0 <= us_minutes < 1200.0
    session_tokyo = 0 <= utc_hour < 6
    session_singapore_hk = 1 <= utc_hour < 9
    session_london = 8 <= utc_hour < 16
    session_america = 13 <= utc_hour < 21
    session_us_extended = us_pre or us_after
    session_asia = session_tokyo or session_singapore_hk
    session_europe = session_london
    session_asia_europe_overlap = session_singapore_hk and session_london
    session_europe_america_overlap = session_london and session_america
    session_tokyo_singapore_overlap = session_tokyo and session_singapore_hk
    session_london_us_overlap = session_europe_america_overlap
    session_active_count = int(session_tokyo) + int(session_singapore_hk) + int(session_london) + int(session_america)

    base = {
        "utc_hour": float(utc_hour),
        "utc_weekday": float(utc_weekday),
        "utc_is_weekend": float(utc_weekday >= 5),
        "hour_sin": math.sin(2 * math.pi * hour_frac / 24.0),
        "hour_cos": math.cos(2 * math.pi * hour_frac / 24.0),
        "dow_sin": math.sin(2 * math.pi * utc_weekday / 7.0),
        "dow_cos": math.cos(2 * math.pi * utc_weekday / 7.0),
        "session_asia": float(session_asia),
        "session_tokyo": float(session_tokyo),
        "session_singapore_hk": float(session_singapore_hk),
        "session_europe": float(session_europe),
        "session_london": float(session_london),
        "session_america": float(session_america),
        "session_us_extended": float(session_us_extended),
        "session_asia_europe_overlap": float(session_asia_europe_overlap),
        "session_europe_america_overlap": float(session_europe_america_overlap),
        "session_tokyo_singapore_overlap": float(session_tokyo_singapore_overlap),
        "session_london_us_overlap": float(session_london_us_overlap),
        "session_active_count": float(session_active_count),
        "cn_hour": float(cn.hour),
        "cn_weekday": float(cn_weekday),
        "cn_is_weekend": float(cn_weekday >= 5),
        "cn_is_holiday": float(cn_holiday),
        "cn_is_adjusted_workday": float(cn_adjusted),
        "cn_is_workday": float(cn_workday),
        "cn_is_holiday_eve": float(cn_next in CN_HOLIDAYS_INT),
        "cn_is_post_holiday": float(cn_prev in CN_HOLIDAYS_INT),
        "us_hour": float(us.hour),
        "us_weekday": float(us_weekday),
        "us_is_weekend": float(us_weekday >= 5),
        "us_is_sunday": float(us_weekday == 6),
        "us_is_sunday_evening": float(us_weekday == 6 and us_minutes >= 18 * 60),
        "us_is_federal_holiday": float(us_date in US_FEDERAL_HOLIDAYS_INT),
        "us_is_nyse_trading_day": float(us_trading),
        "us_is_regular_hours": float(us_rth),
        "us_is_premarket": float(us_pre),
        "us_is_afterhours": float(us_after),
        "us_is_holiday_eve": float(us_next in NYSE_HOLIDAYS_INT),
        "us_is_post_holiday": float(us_prev in NYSE_HOLIDAYS_INT),
        "minutes_to_us_open": (570.0 - us_minutes) / 570.0 if us_trading and us_minutes < 570.0 else 0.0,
        "minutes_to_us_close": (960.0 - us_minutes) / 390.0 if us_rth else 0.0,
        "is_weekday_us_rth": float(us_rth),
        "is_weekend_core": float(utc_weekday >= 5),
    }
    out = {f"{prefix}{key}": float(value) for key, value in base.items()}
    if include_legacy:
        for base_name, legacy in _LEGACY_MAP.items():
            out[legacy] = float(base.get(base_name, 0.0))
    return out


def _is_relative_quote_clock(name: str, value: Any) -> bool:
    """Recognize the bounded, zero-based millisecond clock used by replays."""

    if isinstance(value, (bool, np.bool_)) or not isinstance(
        value, (int, float, np.integer, np.floating)
    ):
        return False
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0.0:
        return False

    # Formal replay is day-scoped.  Its quote/order timestamps are elapsed
    # milliseconds from zero, while live timestamps are Unix epoch values.
    # Some historical order fields omit the `_ms` suffix but use the same ABI.
    if name.endswith("_ms") or name in {"quote_ts", "submit_ts", "activate_ts"}:
        elapsed_seconds = numeric / 1_000.0
    else:
        scale = {
            "s": 1.0,
            "ms": 1_000.0,
            "us": 1_000_000.0,
            "ns": 1_000_000_000.0,
        }[_numeric_timestamp_unit(abs(numeric))]
        elapsed_seconds = numeric / scale
    return elapsed_seconds <= _MAX_RELATIVE_QUOTE_CLOCK_SECONDS


def is_relative_millisecond_clock(value: Any) -> bool:
    """Return whether ``value`` is the bounded day-relative replay clock."""

    return _is_relative_quote_clock("timestamp_ms", value)


def quote_calendar_feature_values(features: dict[str, Any], *, prefix: str = "quote_cal_") -> dict[str, Any]:
    """Materialize quote-time features; zero only a bounded relative clock."""

    ts_value = None
    ts_name = ""
    for name in ("quote_ts_ms", "quote_ts", "submit_ts", "activate_ts", "timestamp_ms", "ts_ms", "timestamp", "quote_dt"):
        if name in features:
            ts_name = name
            ts_value = features.get(name)
            break
    if ts_name and _is_relative_quote_clock(ts_name, ts_value):
        features.update(calendar_scalar_features(None, prefix=prefix, include_legacy=False))
        return features
    features.update(calendar_scalar_features(ts_value, prefix=prefix, include_legacy=False))
    return features
