"""Known market-data quality exclusions for training and replay workflows."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import numpy as np
import pandas as pd


SOURCE_UNRESOLVED_MISSING_OBJECT_DAYS = frozenset(
    {
        ("BTCUSDT", "2026-03-30"),
        ("BTCUSDT", "2026-03-31"),
        ("BTCUSDT", "2026-04-01"),
        ("BTCUSDT", "2026-04-02"),
        ("BTCUSDT", "2026-04-03"),
        ("BTCUSDT", "2026-04-04"),
        ("BTCUSDT", "2026-04-05"),
        ("BTCUSDT", "2026-04-06"),
        ("BTCUSDT", "2026-04-07"),
        ("BTCUSDT", "2026-04-08"),
        ("BTCUSDT", "2026-04-09"),
    }
)

RAW_ZERO_NORMALIZED_MISSING_DAYS = frozenset(
    {
        ("BTCUSDT", "2026-03-31"),
        ("BTCUSDT", "2026-04-03"),
        ("BTCUSDT", "2026-04-04"),
        ("BTCUSDT", "2026-04-06"),
        ("BTCUSDT", "2026-04-07"),
    }
)

PARTIAL_ORDERBOOK_DAYS = SOURCE_UNRESOLVED_MISSING_OBJECT_DAYS - RAW_ZERO_NORMALIZED_MISSING_DAYS

# Keep this policy boundary for future source failures.  The former
# 2026-07-13 entry was removed after replaying the native 2026-07-12 22:26 UTC
# snapshot through an exact U/u/pu chain across midnight.  Its remaining
# 06:36:54-06:58:00 source gap is recorded in the data manifest and passes the
# existing >=90% minimal-day gate, but it is not gap-free event-L2 evidence.
STRICT_RECONSTRUCTION_BAD_DAYS: frozenset[tuple[str, str]] = frozenset()

_DAY_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_DEFAULT_AUDIT_CSV = Path(__file__).resolve().parent / "logs" / "data_audit" / "cryptohft_bad_days_20250801_20260624.csv"


def _audit_csv_path() -> Path:
    override = os.environ.get("MM_CRYPTOHFT_BAD_DAYS_CSV")
    return Path(override).expanduser().resolve() if override else _DEFAULT_AUDIT_CSV


def _load_audit_bad_orderbook_days() -> tuple[frozenset[tuple[str, str]], dict[tuple[str, str], tuple[str, ...]]]:
    path = _audit_csv_path()
    if not path.exists():
        return frozenset(), {}

    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        print(f"  [WARN] Failed to read CryptoHFT bad-day audit {path}: {exc}")
        return frozenset(), {}

    required = {"symbol", "date"}
    if not required.issubset(frame.columns):
        print(f"  [WARN] CryptoHFT bad-day audit missing columns {sorted(required - set(frame.columns))}: {path}")
        return frozenset(), {}

    entries: set[tuple[str, str]] = set()
    reasons: dict[tuple[str, str], set[str]] = {}
    for row in frame.to_dict("records"):
        symbol = str(row.get("symbol", "") or "").strip().upper().replace("/", "").replace("-", "")
        day = str(row.get("date", ""))[:10]
        if not symbol or not _DAY_RE.fullmatch(day):
            continue

        cause = str(row.get("cause", "") or "").strip()
        reason = f"cryptohft_audit_{cause}" if cause else "cryptohft_audit_bad_day"
        key = (symbol, day)
        entries.add(key)
        reasons.setdefault(key, set()).add(reason)

    frozen_reasons = {key: tuple(sorted(value)) for key, value in reasons.items()}
    return frozenset(entries), frozen_reasons


AUDIT_BAD_ORDERBOOK_DAYS, AUDIT_BAD_ORDERBOOK_DAY_REASONS = _load_audit_bad_orderbook_days()


@dataclass(frozen=True)
class DataCompletenessPolicy:
    exclude_source_unresolved_missing_objects: bool = True
    exclude_raw_zero_normalized_missing: bool = True
    exclude_partial_orderbook_days: bool = True
    exclude_strict_reconstruction_bad_days: bool = True
    exclude_audit_bad_orderbook_days: bool = True
    cross_symbol_orderbook_exclusions: bool = False

    def _matching_days(self, entries: frozenset[tuple[str, str]], symbol: Optional[str]) -> set[str]:
        normalized = normalize_quality_symbol(symbol)
        # The current BTCUSDT historical bridge uses official trades, not the
        # retired CryptoHFT BTCUSDT book. Cross-symbol exclusions remain an
        # explicit legacy-reproduction option, never the default live lineage.
        if self.cross_symbol_orderbook_exclusions or not normalized:
            return {day for _, day in entries}
        return {day for entry_symbol, day in entries if entry_symbol == normalized}

    def excluded_orderbook_days(self, symbol: Optional[str]) -> frozenset[str]:
        excluded: set[str] = set()
        if self.exclude_source_unresolved_missing_objects:
            excluded.update(self._matching_days(SOURCE_UNRESOLVED_MISSING_OBJECT_DAYS, symbol))
        if self.exclude_raw_zero_normalized_missing:
            excluded.update(self._matching_days(RAW_ZERO_NORMALIZED_MISSING_DAYS, symbol))
        if self.exclude_partial_orderbook_days:
            excluded.update(self._matching_days(PARTIAL_ORDERBOOK_DAYS, symbol))
        if self.exclude_strict_reconstruction_bad_days:
            excluded.update(self._matching_days(STRICT_RECONSTRUCTION_BAD_DAYS, symbol))
        if self.exclude_audit_bad_orderbook_days:
            excluded.update(self._matching_days(AUDIT_BAD_ORDERBOOK_DAYS, symbol))
        return frozenset(excluded)

    def reasons_for_day(self, symbol: Optional[str], day: str) -> tuple[str, ...]:
        normalized = normalize_quality_symbol(symbol)
        day = str(day)[:10]
        keys = {(normalized, day)}
        if self.cross_symbol_orderbook_exclusions or not normalized:
            keys.update((entry_symbol, day) for entry_symbol, _ in SOURCE_UNRESOLVED_MISSING_OBJECT_DAYS)
            keys.update((entry_symbol, day) for entry_symbol, _ in RAW_ZERO_NORMALIZED_MISSING_DAYS)
            keys.update((entry_symbol, day) for entry_symbol, _ in PARTIAL_ORDERBOOK_DAYS)
            keys.update((entry_symbol, day) for entry_symbol, _ in STRICT_RECONSTRUCTION_BAD_DAYS)
            keys.update((entry_symbol, day) for entry_symbol, _ in AUDIT_BAD_ORDERBOOK_DAYS)

        reasons: list[str] = []
        if any(key in SOURCE_UNRESOLVED_MISSING_OBJECT_DAYS for key in keys):
            reasons.append("source_unresolved_missing_objects")
        if any(key in RAW_ZERO_NORMALIZED_MISSING_DAYS for key in keys):
            reasons.append("raw_zero_normalized_missing")
        if any(key in PARTIAL_ORDERBOOK_DAYS for key in keys):
            reasons.append("partial_orderbook_day")
        if any(key in STRICT_RECONSTRUCTION_BAD_DAYS for key in keys):
            reasons.append("strict_reconstruction_missing_opening_snapshot")
        for key in keys:
            for reason in AUDIT_BAD_ORDERBOOK_DAY_REASONS.get(key, ()):
                if reason not in reasons:
                    reasons.append(reason)
        return tuple(reasons)

    def is_allowed_day(self, symbol: Optional[str], day: str) -> bool:
        return str(day)[:10] not in self.excluded_orderbook_days(symbol)


COMPLETE_DATA_POLICY = DataCompletenessPolicy()

def normalize_quality_symbol(symbol: Optional[str]) -> str:
    return (symbol or "").strip().upper().replace("/", "").replace("-", "")


def excluded_orderbook_days(symbol: Optional[str]) -> frozenset[str]:
    return COMPLETE_DATA_POLICY.excluded_orderbook_days(symbol)


def audit_bad_orderbook_days(symbol: Optional[str]) -> frozenset[str]:
    return frozenset(COMPLETE_DATA_POLICY._matching_days(AUDIT_BAD_ORDERBOOK_DAYS, symbol))


def is_source_unresolved_missing_object_day(symbol: Optional[str], day: str) -> bool:
    return "source_unresolved_missing_objects" in COMPLETE_DATA_POLICY.reasons_for_day(symbol, day)


def day_tag_from_path(path: Path | str) -> Optional[str]:
    match = _DAY_RE.search(Path(path).name)
    return match.group(0) if match else None


def filter_paths_for_orderbook_quality(
    paths: Sequence[Path],
    symbol: Optional[str],
    *,
    label: str = "data",
    explicitly_allowed_days: Iterable[str] = (),
) -> list[Path]:
    allowed = frozenset(str(day)[:10] for day in explicitly_allowed_days)
    excluded = excluded_orderbook_days(symbol) - allowed
    if not excluded:
        return list(paths)

    kept = []
    skipped = []
    for path in paths:
        day = day_tag_from_path(path)
        if day and day in excluded:
            skipped.append(Path(path).name)
        else:
            kept.append(path)

    if skipped:
        preview = ", ".join(skipped[:4])
        suffix = " ..." if len(skipped) > 4 else ""
        print(f"  Excluding {len(skipped)} {label} files with orderbook-quality excluded days: {preview}{suffix}")
    return kept


def _day_tags_from_index(index: pd.Index) -> Optional[pd.Index]:
    if len(index) == 0:
        return pd.Index([], dtype=object)

    if isinstance(index, pd.DatetimeIndex):
        timestamps = pd.to_datetime(index, utc=True, errors="coerce")
    else:
        numeric = pd.to_numeric(pd.Index(index), errors="coerce")
        if numeric.notna().any():
            max_abs = float(np.nanmax(np.abs(numeric.to_numpy(dtype=np.float64))))
            unit = "ns" if max_abs >= 1e15 else "ms"
            timestamps = pd.to_datetime(numeric, unit=unit, utc=True, errors="coerce")
        else:
            timestamps = pd.to_datetime(index, utc=True, errors="coerce")

    if timestamps.isna().all():
        return None
    return pd.Index(timestamps.strftime("%Y-%m-%d"))


def _coerce_utc_timestamps(index_like: Iterable) -> pd.DatetimeIndex:
    if isinstance(index_like, pd.DatetimeIndex):
        return pd.to_datetime(index_like, utc=True, errors="coerce")

    values = index_like
    if isinstance(values, pd.Series):
        values = values.reset_index(drop=True)
    elif not isinstance(values, pd.Index):
        values = pd.Index(values)

    numeric = pd.to_numeric(values, errors="coerce")
    if pd.notna(numeric).any():
        numeric_values = np.asarray(numeric, dtype=np.float64)
        finite = np.isfinite(numeric_values)
        if finite.any():
            max_abs = float(np.nanmax(np.abs(numeric_values[finite])))
            unit = "ns" if max_abs >= 1e15 else "ms"
            return pd.to_datetime(numeric_values, unit=unit, utc=True, errors="coerce")

    return pd.to_datetime(values, utc=True, errors="coerce")


def _timestamps_ns(timestamps: pd.DatetimeIndex) -> np.ndarray:
    """Return epoch nanoseconds independent of pandas' internal index unit."""
    return timestamps.as_unit("ns").asi8


def continuous_segment_ids(index_like: Iterable, max_gap_s: float = 5.0) -> np.ndarray:
    """Return monotonically increasing segment ids split at invalid/order/gap breaks.

    The input is interpreted in its existing order.  Use a `max_gap_s` appropriate
    for the timeline resolution: e.g. 5s for dense 1s data, 15s for a 10s grid.

    中文说明：segment 是训练和 label 的硬边界。rolling feature 或 horizon label
    不允许穿过坏日、乱序时间戳或长 gap；否则会把缺失数据两侧的行情错误拼接。
    """
    timestamps = _coerce_utc_timestamps(index_like)
    n = len(timestamps)
    if n == 0:
        return np.empty(0, dtype=np.int64)

    invalid = np.asarray(timestamps.isna(), dtype=bool)
    max_gap_ns = max(float(max_gap_s), 0.0) * 1_000_000_000.0
    ns = _timestamps_ns(timestamps)

    breaks = np.zeros(n, dtype=bool)
    breaks[0] = True
    if n > 1:
        prev_invalid = invalid[:-1]
        cur_invalid = invalid[1:]
        delta = ns[1:].astype(np.float64) - ns[:-1].astype(np.float64)
        breaks[1:] = prev_invalid | cur_invalid | (delta < 0.0) | (delta > max_gap_ns)
    return np.cumsum(breaks, dtype=np.int64) - 1


def mask_valid_horizon(index_like: Iterable, horizon_s: float, max_gap_s: float = 5.0) -> np.ndarray:
    """Mask rows whose future horizon remains inside the same continuous segment.

    For each timestamp t, the mask is true only if there is an observation at or
    after t + horizon_s, the observation is no later than max_gap_s after the
    target, and both timestamps share the same segment id.

    中文说明：这是 label 口径，不是数据清洗建议。false 表示这行不能参与
    对应 horizon 的监督学习/markout 评估，而不是简单向前填充。
    """
    timestamps = _coerce_utc_timestamps(index_like)
    n = len(timestamps)
    if n == 0:
        return np.ones(0, dtype=bool)

    invalid = np.asarray(timestamps.isna(), dtype=bool)
    ns = _timestamps_ns(timestamps)
    monotonic = bool(n <= 1 or np.all(np.diff(ns.astype(np.int64, copy=False)) >= 0))
    horizon_ns = max(float(horizon_s), 0.0) * 1_000_000_000.0
    max_gap_ns = max(float(max_gap_s), 0.0) * 1_000_000_000.0
    segments = continuous_segment_ids(timestamps, max_gap_s=max_gap_s)

    valid = ~invalid
    target = ns.astype(np.float64) + horizon_ns

    if monotonic:
        future_idx = np.searchsorted(ns, target, side="left")
        valid &= future_idx < n
        if valid.any():
            rows = np.flatnonzero(valid)
            future = future_idx[rows]
            valid_rows = (
                (~invalid[future])
                & (segments[future] == segments[rows])
                & ((ns[future].astype(np.float64) - target[rows]) <= max_gap_ns)
            )
            valid[rows] = valid_rows
    else:
        # 中文说明：乱序输入不能对全局 ns 做 searchsorted。按 segment 分片后，
        # 每个片段内部由 continuous_segment_ids 保证非递减，mask 仍保持原行顺序。
        valid[:] = False
        for segment in np.unique(segments):
            rows = np.flatnonzero((segments == segment) & (~invalid))
            if rows.size == 0:
                continue
            seg_ns = ns[rows]
            seg_target = target[rows]
            future_local = np.searchsorted(seg_ns, seg_target, side="left")
            ok = future_local < rows.size
            if ok.any():
                ok_rows = rows[np.flatnonzero(ok)]
                future_rows = rows[future_local[ok]]
                valid[ok_rows] = (
                    (~invalid[future_rows])
                    & ((ns[future_rows].astype(np.float64) - target[ok_rows]) <= max_gap_ns)
                )
    return np.asarray(valid, dtype=bool)


def filter_frame_for_orderbook_quality(
    frame: pd.DataFrame,
    symbol: Optional[str],
    *,
    label: str = "data",
    explicitly_allowed_days: Iterable[str] = (),
) -> pd.DataFrame:
    allowed = frozenset(str(day)[:10] for day in explicitly_allowed_days)
    excluded = excluded_orderbook_days(symbol) - allowed
    if frame is None or frame.empty or not excluded:
        return frame

    day_tags = _day_tags_from_index(frame.index)
    if day_tags is None:
        return frame

    keep = ~day_tags.isin(excluded)
    removed = int((~keep).sum())
    if removed:
        print(f"  Excluding {removed:,} {label} rows from orderbook-quality excluded days")
        return frame.loc[keep]
    return frame


def allowed_timestamp_mask(
    timestamps: Iterable[int | float],
    symbol: Optional[str],
    *,
    label: str = "data",
    explicitly_allowed_days: Iterable[str] = (),
) -> np.ndarray:
    values = np.asarray(timestamps, dtype=np.float64)
    if values.size == 0:
        return np.ones(0, dtype=bool)

    allowed = frozenset(str(day)[:10] for day in explicitly_allowed_days)
    excluded = excluded_orderbook_days(symbol) - allowed
    if not excluded:
        return np.ones(values.size, dtype=bool)

    max_abs = float(np.nanmax(np.abs(values)))
    unit = "ns" if max_abs >= 1e15 else "ms"
    day_tags = pd.Index(pd.to_datetime(values, unit=unit, utc=True, errors="coerce").strftime("%Y-%m-%d"))
    keep = ~day_tags.isin(excluded)
    removed = int((~keep).sum())
    if removed:
        print(f"  Excluding {removed:,} {label} rows from orderbook-quality excluded days")
    return np.asarray(keep, dtype=bool)
