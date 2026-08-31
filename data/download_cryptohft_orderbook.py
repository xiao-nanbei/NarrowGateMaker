#!/usr/bin/env python3
"""Download CryptoHFTData Binance Futures orderbook files and rebuild daily BBO/L2.

CryptoHFTData is a third-party, personally maintained collection rather than a
Binance official archive. Missing days/hours/segments are expected source risks;
formal replay eligibility is determined only by the downstream coverage/gap
audit, never by file existence alone.

The source dataset is only available from 2025-08-01 onward. Hourly raw files are
stored once under ${NARROWGATE_MARKETDATA_ROOT}/cryptohftdata/ and normalized
daily parquet files are written only to the active BTCUSDC data root. The
historical BTCUSDT bridge uses official individual-trade bars, so BTCUSDT
CryptoHFT order books are no longer downloaded by default.

Outputs:
  ${NARROWGATE_MARKETDATA_ROOT}/cryptohftdata/binance_futures/YYYY-MM-DD/HH/SYMBOL_orderbook.parquet.zst
  ${NARROWGATE_DATA_ROOT}/bbo/SYMBOL-bbo-YYYY-MM-DD.parquet
  ${NARROWGATE_DATA_ROOT}/l2/SYMBOL-l2-YYYY-MM-DD.parquet

Examples:
  export CRYPTOHFTDATA_API_KEY=...
  python data/download_cryptohft_orderbook.py --start 2025-08-01 --end 2025-08-31
  python data/download_cryptohft_orderbook.py --symbols BTCUSDC --start 2025-09-01 --end 2025-09-30
  python data/download_cryptohft_orderbook.py --repair-audit-csv logs/data_audit/cryptohft_bad_days.csv --dry-run
"""

from __future__ import annotations

import argparse
import concurrent.futures
import heapq
import json
import os
import re
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import zstandard as zstd

try:
    from cryptohftdata import CryptoHFTDataClient as SDKCryptoHFTDataClient
except ImportError:
    SDKCryptoHFTDataClient = None


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import marketdata_root  # noqa: E402
from market_fusion import normalize_symbol  # noqa: E402

API_BASE_URL = "https://api.cryptohftdata.com"
DOWNLOAD_ENDPOINT = f"{API_BASE_URL}/download"
JWT_ENDPOINT = f"{API_BASE_URL}/jwt-token"
DEFAULT_EXCHANGE = "binance_futures"
DEFAULT_SYMBOLS = ("BTCUSDC",)
# Match the live Binance partial-depth contract by default.  The source files
# remain price-level event tapes; these settings only control normalized output.
DEFAULT_LEVELS = 20
DEFAULT_SNAPSHOT_MS = 100
# A UTC-day file can begin with deltas whose native snapshot was recorded late
# in the preceding day. Keep one full day of raw history available so strict
# snapshot bootstrap does not silently discard an otherwise recoverable day.
DEFAULT_WARMUP_HOURS = 24
DEFAULT_DOWNLOAD_ATTEMPTS = 20
DEFAULT_DOWNLOAD_TIMEOUT = (30, 30)
DEFAULT_DOWNLOAD_TRANSPORT = "auto"
DEFAULT_MIN_DAY_COVERAGE = 0.90
DEFAULT_COVERAGE_FRESHNESS_S = 5.0
DEFAULT_TIMESTAMP_SOURCE = "transaction"
DEFAULT_SEQUENCE_BOOTSTRAP = "snapshot"
DEFAULT_DELTA_CONVERGENCE_MS = 120_000
MIN_AVAILABLE_UTC = datetime(2025, 8, 1, 0, 0, tzinfo=timezone.utc)
API_KEY_ENV = "CRYPTOHFTDATA_API_KEY"
JWT_ENV = "CRYPTOHFTDATA_JWT"


@dataclass(frozen=True)
class BadDayRepair:
    """One symbol/day repair decision from a classified coverage audit."""

    symbol: str
    date: str
    cause: str
    suggested_fix: str
    redownload_can_fix: bool
    missing_raw_hours: tuple[str, ...]


_BAD_DAY_REPAIR_COLUMNS = {
    "symbol",
    "date",
    "cause",
    "suggested_fix",
    "redownload_can_fix",
    "missing_raw_hours",
}
_REPAIR_CAUSES = {
    "missing_raw_hours",
    "normalized_gap_with_snapshots",
    "raw_coverage_below_threshold",
    "raw_decode_errors",
    "raw_has_no_snapshots",
    "source_unresolved_missing_objects",
}
_REPAIR_SUGGESTED_FIXES = {
    "exclude_day",
    "exclude_from_training_backtest_parity",
    "force_rebuild_from_raw",
    "not_fixable_by_redownload",
    "refresh_raw_and_rebuild",
    "retry_download",
}
_REPAIR_DOWNLOAD_FIXES = {"retry_download", "refresh_raw_and_rebuild"}


def _parse_strict_bool(value: object, *, field: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n", ""}:
        return False
    raise ValueError(f"invalid {field} value: {value!r}")


def _load_bad_day_repairs(path: Path) -> list[BadDayRepair]:
    """Load and validate a classified CryptoHFT bad-day CSV."""

    path = Path(path).expanduser().resolve()
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = sorted(_BAD_DAY_REPAIR_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"bad-day CSV missing required columns {missing}: {path}")

    repairs: list[BadDayRepair] = []
    identities: set[tuple[str, str]] = set()
    for row_number, row in enumerate(frame.to_dict("records"), start=2):
        raw_symbol = str(row["symbol"]).strip()
        if not raw_symbol:
            raise ValueError(f"empty symbol at {path}:{row_number}")
        symbol = normalize_symbol(raw_symbol)
        if not re.fullmatch(r"[A-Z0-9]{5,32}", symbol):
            raise ValueError(
                f"invalid symbol at {path}:{row_number}: {raw_symbol!r}"
            )
        cause = str(row["cause"]).strip()
        if cause not in _REPAIR_CAUSES:
            raise ValueError(
                f"invalid cause at {path}:{row_number}: {cause!r}"
            )
        suggested_fix = str(row["suggested_fix"]).strip()
        if suggested_fix not in _REPAIR_SUGGESTED_FIXES:
            raise ValueError(
                f"invalid suggested_fix at {path}:{row_number}: "
                f"{suggested_fix!r}"
            )
        try:
            date = datetime.strptime(str(row["date"]).strip(), "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )
        except ValueError as exc:
            raise ValueError(
                f"invalid UTC date at {path}:{row_number}: {row['date']!r}"
            ) from exc
        identity = (symbol, date)
        if identity in identities:
            raise ValueError(f"duplicate bad-day repair {symbol} {date} in {path}")
        identities.add(identity)

        try:
            hours = sorted(
                {
                    int(value.strip())
                    for value in str(row["missing_raw_hours"]).split(",")
                    if value.strip()
                }
            )
        except ValueError as exc:
            raise ValueError(
                f"invalid missing_raw_hours at {path}:{row_number}"
            ) from exc
        if any(hour < 0 or hour > 23 for hour in hours):
            raise ValueError(f"missing raw hour outside 00..23 at {path}:{row_number}")
        repairs.append(
            BadDayRepair(
                symbol=symbol,
                date=date,
                cause=cause,
                suggested_fix=suggested_fix,
                redownload_can_fix=_parse_strict_bool(
                    row["redownload_can_fix"], field="redownload_can_fix"
                ),
                missing_raw_hours=tuple(f"{hour:02d}" for hour in hours),
            )
        )
    return repairs


def _select_bad_day_repairs(
    repairs: list[BadDayRepair],
    *,
    symbols: set[str] | None = None,
    causes: set[str] | None = None,
    include_nonfixable: bool = False,
    limit: int | None = None,
) -> list[BadDayRepair]:
    """Apply repair-mode filters while preserving classified CSV order."""

    normalized_symbols = (
        {normalize_symbol(symbol) for symbol in symbols}
        if symbols
        else set()
    )
    normalized_causes = {str(cause).strip() for cause in causes or set()}
    selected = [
        repair
        for repair in repairs
        if (not normalized_symbols or repair.symbol in normalized_symbols)
        and (not normalized_causes or repair.cause in normalized_causes)
        and (include_nonfixable or repair.redownload_can_fix)
    ]
    if limit is not None:
        if limit < 0:
            raise ValueError("repair limit must be >= 0")
        selected = selected[:limit]
    return selected


def _raw_paths_for_repair(
    raw_root: Path,
    exchange: str,
    repair: BadDayRepair,
    *,
    refresh_entire_day: bool = False,
) -> list[Path]:
    """Return canonical exchange-aware raw paths selected for forced refresh."""

    day_start = datetime.strptime(repair.date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    hours = (
        [f"{hour:02d}" for hour in range(24)]
        if refresh_entire_day
        else list(repair.missing_raw_hours)
    )
    return [
        Path(raw_root)
        / _object_rel_path(
            exchange,
            repair.symbol,
            day_start.replace(hour=int(hour)),
        )
        for hour in hours
    ]


def _repo_data_root(repo_root: Path, honor_env: bool) -> Path:
    if honor_env:
        env_root = os.environ.get("NARROWGATE_DATA_ROOT") or os.environ.get(
            "MM_DATA_ROOT"
        )
        if env_root:
            return Path(env_root).expanduser().resolve()

    external_root = marketdata_root() / repo_root.name
    if external_root.exists():
        return external_root
    return repo_root / "data"


def _default_target_roots() -> list[Path]:
    # Rebuild into an explicit staging root. The top-level bbo/l2 identity is
    # retired, while normalized_l2_100ms_v2 is an immutable registry view.
    return [
        (
            _repo_data_root(ROOT, honor_env=True)
            / "replay_l2_retained100ms_staging"
        ).resolve()
    ]


def _default_raw_root() -> Path:
    return marketdata_root() / "cryptohftdata"


def _parse_datetime_arg(value: str, *, end: bool) -> datetime:
    value = value.strip()
    formats = (
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H",
        "%Y-%m-%d %H",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            if fmt == "%Y-%m-%d" and end:
                dt += timedelta(hours=23)
            return dt
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid datetime '{value}'. Use YYYY-MM-DD or YYYY-MM-DDTHH"
    )


def _floor_hour(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _daily_write_start(dt: datetime) -> datetime:
    """Return the UTC day boundary for daily normalized output.

    Raw downloads may be requested for a single new hour, but BBO/L2 artifacts
    are whole-day files.  Rebuilding from the requested hour would atomically
    replace the existing day with a partial file.
    """
    return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _load_retained_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame.columns:
        raise ValueError(f"retained manifest must contain a day column: {path}")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        bad = frame.loc[parsed.isna(), "day"].astype(str).head(3).tolist()
        raise ValueError(f"retained manifest contains invalid UTC days: {bad}")
    days = sorted(set(parsed.dt.strftime("%Y-%m-%d")))
    if not days:
        raise ValueError(f"retained manifest is empty: {path}")
    return days


def _contiguous_day_ranges(
    days: list[str],
) -> list[tuple[datetime, datetime]]:
    parsed = [
        datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        for day in sorted(set(days))
    ]
    if not parsed:
        return []
    ranges: list[list[datetime]] = [[parsed[0], parsed[0]]]
    for current in parsed[1:]:
        if current == ranges[-1][1] + timedelta(days=1):
            ranges[-1][1] = current
        else:
            ranges.append([current, current])
    return [
        (start, end.replace(hour=23))
        for start, end in ranges
    ]


def _retained_process_ranges(
    days: list[str],
    *,
    independent_days: bool,
    sequence_bootstrap: str,
    max_days: int = 0,
) -> list[tuple[datetime, datetime]]:
    if independent_days or sequence_bootstrap == "delta-converged":
        ranges = [
            (
                datetime.strptime(day, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                ),
                datetime.strptime(day, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc,
                    hour=23,
                ),
            )
            for day in days
        ]
    else:
        ranges = _contiguous_day_ranges(days)
    if max_days <= 0:
        return ranges

    chunked: list[tuple[datetime, datetime]] = []
    for start, end in ranges:
        current = start
        while current <= end:
            chunk_end = min(
                end,
                current + timedelta(days=int(max_days), hours=-1),
            )
            chunked.append((current, chunk_end))
            current = chunk_end.replace(hour=0) + timedelta(days=1)
    return chunked


def _prefetch_raw_hours(
    *,
    raw_root: Path,
    exchange: str,
    symbols: list[str],
    process_ranges: list[tuple[datetime, datetime]],
    warmup_hours: int,
    api_key: Optional[str],
    jwt: Optional[str],
    transport: str,
    workers: int,
) -> dict[str, int]:
    """Download missing source hours before CPU-bound reconstruction."""

    jobs: list[tuple[str, datetime, Path, Path]] = []
    seen: set[tuple[str, datetime]] = set()
    for symbol in symbols:
        for range_start, range_end in process_ranges:
            process_start = max(
                range_start - timedelta(hours=warmup_hours),
                MIN_AVAILABLE_UTC,
            )
            for hour_dt in _iter_hours(process_start, range_end):
                key = (symbol, hour_dt)
                if key in seen:
                    continue
                seen.add(key)
                rel_path = _object_rel_path(exchange, symbol, hour_dt)
                raw_path = raw_root / rel_path
                if raw_path.exists() and raw_path.stat().st_size > 0:
                    continue
                jobs.append((symbol, hour_dt, rel_path, raw_path))
    if not jobs:
        return {"downloaded": 0, "exists": 0, "404": 0}

    thread_state = threading.local()

    def fetch(
        job: tuple[str, datetime, Path, Path],
    ) -> tuple[str, str]:
        symbol, hour_dt, rel_path, raw_path = job
        client = getattr(thread_state, "client", None)
        if client is None:
            client = CryptoHFTClient(
                api_key=api_key,
                jwt=jwt,
                transport=transport,
            )
            thread_state.client = client
        status = client.download_file(rel_path, raw_path)
        return (
            status,
            f"{symbol} {hour_dt:%Y-%m-%d %H}:00",
        )

    counts = {"downloaded": 0, "exists": 0, "404": 0}
    print(
        f"Prefetching {len(jobs)} missing raw hours with "
        f"{max(1, int(workers))} workers"
    )
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, int(workers))
    ) as executor:
        futures = [executor.submit(fetch, job) for job in jobs]
        for future in concurrent.futures.as_completed(futures):
            status, label = future.result()
            counts[status] = counts.get(status, 0) + 1
            if status == "404":
                print(f"[404]  {label}")
    print("Raw prefetch: " + json.dumps(counts, sort_keys=True))
    return counts


def _iter_hours(start_dt: datetime, end_dt: datetime):
    current = _floor_hour(start_dt)
    end_hour = _floor_hour(end_dt)
    while current <= end_hour:
        yield current
        current += timedelta(hours=1)


def _object_rel_path(exchange: str, symbol: str, dt: datetime) -> Path:
    return Path(exchange) / dt.strftime("%Y-%m-%d") / dt.strftime("%H") / f"{symbol}_orderbook.parquet.zst"


def _daily_output_paths(root: Path, symbol: str, day_tag: str) -> tuple[Path, Path, Path, Path]:
    bbo_final = root / "bbo" / f"{symbol}-bbo-{day_tag}.parquet"
    l2_final = root / "l2" / f"{symbol}-l2-{day_tag}.parquet"
    bbo_tmp = bbo_final.with_suffix(bbo_final.suffix + ".tmp")
    l2_tmp = l2_final.with_suffix(l2_final.suffix + ".tmp")
    return bbo_final, l2_final, bbo_tmp, l2_tmp


def _is_readable_parquet(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        pq.ParquetFile(path)
        return True
    except Exception:
        return False


def _read_parquet_timestamps(path: Path) -> Optional[np.ndarray]:
    if not _is_readable_parquet(path):
        return None
    try:
        parquet_file = pq.ParquetFile(path)
        names = set(parquet_file.schema.names)
        ts_col = next((col for col in ("timestamp", "ts_ms", "event_time", "transaction_time", "received_time", "time") if col in names), None)
        if ts_col is None:
            return None
        table = pq.read_table(path, columns=[ts_col])
        values = pd.to_numeric(table.column(ts_col).to_pandas(), errors="coerce").dropna().astype("int64")
        if values.empty:
            return None
        arr = values.to_numpy(copy=False)
        if arr.max(initial=0) >= 10**15:
            arr = arr // 10**6
        return np.sort(arr)
    except Exception:
        return None


def _read_raw_parquet_zst_summary(path: Path) -> tuple[Optional[np.ndarray], dict[str, int]]:
    temp_parquet: Optional[Path] = None
    try:
        temp_parquet = _decompress_parquet_zst(path)
        ts_ms = _read_parquet_timestamps(temp_parquet)
        event_counts: dict[str, int] = {}
        try:
            table = pq.read_table(temp_parquet, columns=["event_type"])
            counts = table.column("event_type").to_pandas().astype(str).str.lower().value_counts()
            event_counts = {str(key): int(value) for key, value in counts.items()}
        except Exception:
            event_counts = {}
        return ts_ms, event_counts
    except Exception:
        return None, {}
    finally:
        if temp_parquet is not None and temp_parquet.exists():
            temp_parquet.unlink()


def _coverage_union_seconds(starts: np.ndarray, ends: np.ndarray) -> float:
    if len(starts) == 0:
        return 0.0
    order = np.argsort(starts, kind="stable")
    starts = starts[order]
    ends = ends[order]
    total_ms = 0.0
    cur_start = float(starts[0])
    cur_end = float(ends[0])
    for start, end in zip(starts[1:], ends[1:], strict=True):
        start = float(start)
        end = float(end)
        if start <= cur_end:
            cur_end = max(cur_end, end)
        else:
            total_ms += cur_end - cur_start
            cur_start = start
            cur_end = end
    total_ms += cur_end - cur_start
    return max(total_ms, 0.0) / 1000.0


def _timestamp_coverage_ratio(
    ts_ms: Optional[np.ndarray],
    day_start_dt: datetime,
    freshness_ms: int,
) -> tuple[float, int, Optional[int], Optional[int], float]:
    if ts_ms is None or len(ts_ms) == 0 or freshness_ms <= 0:
        return 0.0, 0, None, None, 0.0
    start_ms = int(day_start_dt.timestamp() * 1000)
    end_ms = int((day_start_dt + timedelta(days=1)).timestamp() * 1000) - 1
    mask = (ts_ms <= end_ms) & ((ts_ms + freshness_ms) >= start_ms)
    active_ts = ts_ms[mask]
    if len(active_ts) == 0:
        return 0.0, 0, None, None, 0.0
    starts = np.maximum(active_ts, start_ms)
    ends = np.minimum(active_ts + freshness_ms, end_ms)
    valid = ends > starts
    if not np.any(valid):
        return 0.0, int(len(active_ts)), int(active_ts[0]), int(active_ts[-1]), 0.0
    coverage_s = _coverage_union_seconds(starts[valid], ends[valid])
    span_s = max((end_ms - start_ms) / 1000.0, 1.0)
    gaps_s = np.diff(active_ts).astype(np.float64) / 1000.0
    p99_gap = float(np.quantile(gaps_s, 0.99)) if len(gaps_s) else 0.0
    return min(1.0, coverage_s / span_s), int(len(active_ts)), int(active_ts[0]), int(active_ts[-1]), p99_gap


def _raw_hour_counts(raw_root: Path, exchange: str, symbol: str, day_start_dt: datetime) -> tuple[int, list[str]]:
    missing = []
    present = 0
    for hour in range(24):
        hour_dt = day_start_dt + timedelta(hours=hour)
        raw_path = raw_root / _object_rel_path(exchange, symbol, hour_dt)
        if raw_path.exists() and raw_path.stat().st_size > 0:
            present += 1
        else:
            missing.append(hour_dt.strftime("%H"))
    return present, missing


def _raw_day_summary(
    raw_root: Path,
    exchange: str,
    symbol: str,
    day_start_dt: datetime,
    freshness_ms: int,
    read_timestamps: bool,
) -> dict[str, object]:
    missing_hours = []
    decode_error_hours = []
    snapshot_hours = []
    present = 0
    snapshot_rows = 0
    update_rows = 0
    ts_arrays = []
    for hour in range(24):
        hour_dt = day_start_dt + timedelta(hours=hour)
        raw_path = raw_root / _object_rel_path(exchange, symbol, hour_dt)
        if not raw_path.exists() or raw_path.stat().st_size <= 0:
            missing_hours.append(hour_dt.strftime("%H"))
            continue

        present += 1
        if not read_timestamps:
            continue

        ts_ms, event_counts = _read_raw_parquet_zst_summary(raw_path)
        if ts_ms is None:
            decode_error_hours.append(hour_dt.strftime("%H"))
            continue
        hour_snapshot_rows = int(event_counts.get("snapshot", 0))
        snapshot_rows += hour_snapshot_rows
        update_rows += int(event_counts.get("update", 0))
        if hour_snapshot_rows > 0:
            snapshot_hours.append(hour_dt.strftime("%H"))
        ts_arrays.append(ts_ms)

    if read_timestamps and ts_arrays:
        raw_ts = np.sort(np.concatenate(ts_arrays))
        raw_cov, raw_rows, raw_first, raw_last, raw_p99_gap = _timestamp_coverage_ratio(
            raw_ts, day_start_dt, freshness_ms
        )
    else:
        raw_cov, raw_rows, raw_first, raw_last, raw_p99_gap = None, 0, None, None, None

    return {
        "raw_hours": present,
        "missing_hours": missing_hours,
        "raw_coverage": raw_cov,
        "raw_rows": raw_rows,
        "raw_snapshot_rows": snapshot_rows,
        "raw_update_rows": update_rows,
        "raw_snapshot_hours": snapshot_hours,
        "raw_first_ts": raw_first,
        "raw_last_ts": raw_last,
        "raw_p99_gap_s": raw_p99_gap,
        "raw_decode_error_hours": decode_error_hours,
    }


def _normalized_day_summary(
    root: Path,
    symbol: str,
    day_start_dt: datetime,
    freshness_ms: int,
    levels: int = DEFAULT_LEVELS,
) -> dict[str, object]:
    day_tag = day_start_dt.strftime("%Y-%m-%d")
    bbo_final, l2_final, bbo_tmp, l2_tmp = _daily_output_paths(root, symbol, day_tag)
    bbo_ts = _read_parquet_timestamps(bbo_final)
    l2_ts = _read_parquet_timestamps(l2_final)
    bbo_cov, bbo_rows, bbo_first, bbo_last, bbo_p99_gap = _timestamp_coverage_ratio(bbo_ts, day_start_dt, freshness_ms)
    l2_cov, l2_rows, l2_first, l2_last, l2_p99_gap = _timestamp_coverage_ratio(l2_ts, day_start_dt, freshness_ms)
    required_l2_columns = {
        field
        for level in range(1, levels + 1)
        for field in (
            f"bid_px_{level}",
            f"bid_qty_{level}",
            f"ask_px_{level}",
            f"ask_qty_{level}",
        )
    }
    l2_schema_complete = False
    l2_valid_spread_ratio = 0.0
    if _is_readable_parquet(l2_final):
        parquet_file = pq.ParquetFile(l2_final)
        names = set(parquet_file.schema.names)
        l2_schema_complete = required_l2_columns.issubset(names)
        if {"bid_px_1", "ask_px_1"}.issubset(names):
            touch = pq.read_table(
                l2_final,
                columns=["bid_px_1", "ask_px_1"],
            )
            bid = touch.column("bid_px_1").to_numpy()
            ask = touch.column("ask_px_1").to_numpy()
            if len(bid):
                valid_spread = (
                    np.isfinite(bid)
                    & np.isfinite(ask)
                    & (bid > 0.0)
                    & (ask > bid)
                )
                l2_valid_spread_ratio = float(np.mean(valid_spread))
    return {
        "root": str(root),
        "bbo_path": str(bbo_final),
        "l2_path": str(l2_final),
        "bbo_readable": _is_readable_parquet(bbo_final),
        "l2_readable": _is_readable_parquet(l2_final),
        "tmp_exists": bbo_tmp.exists() or l2_tmp.exists(),
        "bbo_rows": bbo_rows,
        "l2_rows": l2_rows,
        "bbo_coverage": bbo_cov,
        "l2_coverage": l2_cov,
        "bbo_first_ts": bbo_first,
        "bbo_last_ts": bbo_last,
        "l2_first_ts": l2_first,
        "l2_last_ts": l2_last,
        "bbo_p99_gap_s": bbo_p99_gap,
        "l2_p99_gap_s": l2_p99_gap,
        "l2_schema_complete": l2_schema_complete,
        "l2_valid_spread_ratio": l2_valid_spread_ratio,
    }


def _day_is_complete(
    raw_root: Path,
    target_roots: list[Path],
    exchange: str,
    symbol: str,
    day_start_dt: datetime,
    min_coverage: float,
    freshness_ms: int,
) -> bool:
    day_tag = day_start_dt.strftime("%Y-%m-%d")

    raw_hours, _ = _raw_hour_counts(raw_root, exchange, symbol, day_start_dt)
    if raw_hours < 24:
        return False

    for root in target_roots:
        bbo_final, l2_final, bbo_tmp, l2_tmp = _daily_output_paths(root, symbol, day_tag)
        if not _is_readable_parquet(bbo_final) or not _is_readable_parquet(l2_final):
            return False
        if bbo_tmp.exists() or l2_tmp.exists():
            return False
        summary = _normalized_day_summary(root, symbol, day_start_dt, freshness_ms)
        if summary["bbo_coverage"] < min_coverage or summary["l2_coverage"] < min_coverage:
            return False

    return True


def _iter_days(start_dt: datetime, end_dt: datetime):
    current = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def _sequence_audit_status(
    sequence_audit: dict[str, object],
) -> tuple[bool, int, int]:
    """Prefer target-day counters when an independent warmup was replayed."""

    if "target_initialized_at_start" in sequence_audit:
        sequence_ok = (
            bool(
                sequence_audit.get(
                    "target_initialized_at_start",
                    False,
                )
            )
            and str(
                sequence_audit.get(
                    "target_initialization_source_at_start",
                    "",
                )
            )
            == "snapshot"
            and int(sequence_audit.get("target_accepted_updates", 0)) > 0
            and int(sequence_audit.get("target_sequence_gaps", 0)) == 0
            and int(
                sequence_audit.get(
                    "target_invalid_sequence_messages",
                    0,
                )
            )
            == 0
            and int(
                sequence_audit.get(
                    "target_message_time_reversals",
                    0,
                )
            )
            == 0
        )
        return (
            sequence_ok,
            int(sequence_audit.get("target_sequence_gaps", 0)),
            int(
                sequence_audit.get(
                    "target_delta_bootstrap_messages",
                    0,
                )
            ),
        )

    sequence_ok = (
        int(sequence_audit.get("accepted_updates", 0)) > 0
        and (
            int(sequence_audit.get("delta_bootstrap_messages", 0))
            + int(sequence_audit.get("snapshot_messages", 0))
            > 0
        )
        and int(sequence_audit.get("sequence_gaps", 0)) == 0
        and int(
            sequence_audit.get(
                "invalid_sequence_messages",
                0,
            )
        )
        == 0
        and int(sequence_audit.get("message_time_reversals", 0)) == 0
    )
    return (
        sequence_ok,
        int(sequence_audit.get("sequence_gaps", 0)),
        int(sequence_audit.get("delta_bootstrap_messages", 0)),
    )


def _audit_days(
    raw_root: Path,
    target_roots: list[Path],
    exchange: str,
    symbols: list[str],
    start_dt: datetime,
    end_dt: datetime,
    min_coverage: float,
    freshness_ms: int,
    audit_csv: Optional[str],
    audit_raw_timestamps: bool,
    allowed_days: Optional[set[str]] = None,
    sequence_day_audits: Optional[
        dict[str, dict[str, object]]
    ] = None,
    levels: int = DEFAULT_LEVELS,
) -> pd.DataFrame:
    rows = []
    for symbol in symbols:
        for day_dt in _iter_days(start_dt, end_dt):
            day_tag = day_dt.strftime("%Y-%m-%d")
            if allowed_days is not None and day_tag not in allowed_days:
                continue
            raw_summary = _raw_day_summary(
                raw_root,
                exchange,
                symbol,
                day_dt,
                freshness_ms,
                read_timestamps=audit_raw_timestamps,
            )
            raw_hours = int(raw_summary["raw_hours"])
            missing_hours = list(raw_summary["missing_hours"])
            raw_coverage = raw_summary["raw_coverage"]
            for root in target_roots:
                summary = _normalized_day_summary(
                    root,
                    symbol,
                    day_dt,
                    freshness_ms,
                    levels,
                )
                complete = (
                    raw_hours == 24
                    and (not audit_raw_timestamps or float(raw_coverage or 0.0) >= min_coverage)
                    and bool(summary["bbo_readable"])
                    and bool(summary["l2_readable"])
                    and not bool(summary["tmp_exists"])
                    and float(summary["bbo_coverage"]) >= min_coverage
                    and float(summary["l2_coverage"]) >= min_coverage
                    and bool(summary["l2_schema_complete"])
                    and float(summary["l2_valid_spread_ratio"]) >= 1.0
                )
                sequence_audit = (
                    sequence_day_audits.get(
                        f"{symbol}:{day_tag}",
                        sequence_day_audits.get(day_tag),
                    )
                    if sequence_day_audits is not None
                    else None
                )
                sequence_ok = (
                    False if sequence_day_audits is not None else None
                )
                if sequence_audit is not None:
                    (
                        sequence_ok,
                        scoped_sequence_gaps,
                        scoped_delta_bootstrap,
                    ) = _sequence_audit_status(
                        sequence_audit
                    )
                else:
                    scoped_sequence_gaps = 0
                    scoped_delta_bootstrap = 0
                row = {
                    "symbol": symbol,
                    "date": day_tag,
                    "root": str(root),
                    "raw_hours": raw_hours,
                    "missing_hours": ",".join(missing_hours),
                    "raw_rows": int(raw_summary["raw_rows"]),
                    "raw_snapshot_rows": int(raw_summary["raw_snapshot_rows"]),
                    "raw_update_rows": int(raw_summary["raw_update_rows"]),
                    "raw_snapshot_hours": ",".join(raw_summary["raw_snapshot_hours"]),
                    "raw_coverage": "" if raw_coverage is None else float(raw_coverage),
                    "raw_p99_gap_s": "" if raw_summary["raw_p99_gap_s"] is None else float(raw_summary["raw_p99_gap_s"]),
                    "raw_decode_error_hours": ",".join(raw_summary["raw_decode_error_hours"]),
                    "bbo_rows": summary["bbo_rows"],
                    "l2_rows": summary["l2_rows"],
                    "bbo_coverage": float(summary["bbo_coverage"]),
                    "l2_coverage": float(summary["l2_coverage"]),
                    "bbo_p99_gap_s": float(summary["bbo_p99_gap_s"]),
                    "l2_p99_gap_s": float(summary["l2_p99_gap_s"]),
                    "l2_schema_complete": bool(
                        summary["l2_schema_complete"]
                    ),
                    "l2_valid_spread_ratio": float(
                        summary["l2_valid_spread_ratio"]
                    ),
                    "sequence_ok": (
                        "" if sequence_ok is None else bool(sequence_ok)
                    ),
                    "sequence_gaps": (
                        ""
                        if sequence_audit is None
                        else scoped_sequence_gaps
                    ),
                    "delta_bootstrap_messages": (
                        ""
                        if sequence_audit is None
                        else scoped_delta_bootstrap
                    ),
                    "complete": complete,
                    "eligible": complete and (
                        bool(sequence_ok)
                        if sequence_day_audits is not None
                        else True
                    ),
                }
                rows.append(row)
                status = "OK" if row["eligible"] else "BAD"
                raw_cov_desc = ""
                if raw_coverage is not None:
                    raw_cov_desc = (
                        f" raw_cov={float(raw_coverage):.1%} raw_rows={row['raw_rows']}"
                        f" raw_snap={row['raw_snapshot_rows']}"
                    )
                print(
                    f"[{status}] {symbol} {row['date']} root={Path(root).name} "
                    f"raw={raw_hours}/24{raw_cov_desc} bbo_cov={row['bbo_coverage']:.1%} "
                    f"l2_cov={row['l2_coverage']:.1%} bbo_rows={row['bbo_rows']} "
                    f"l2_rows={row['l2_rows']} seq_gap={row['sequence_gaps']}"
                )
                if missing_hours:
                    print(f"      missing raw hours: {','.join(missing_hours)}")
                if raw_summary["raw_decode_error_hours"]:
                    print(f"      raw decode errors: {','.join(raw_summary['raw_decode_error_hours'])}")
    frame = pd.DataFrame(rows)
    if audit_csv:
        out_path = Path(audit_csv).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(out_path, index=False)
        print(f"Audit CSV saved -> {out_path}")
    return frame


def _load_per_day_sequence_audits(
    path: Path,
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, dict[str, object]] = {}
    for item in payload.get("range_audits", []):
        symbol = str(item.get("symbol", "")).strip()
        sequence_audit = item.get("sequence_audit", {})
        day_audits = (
            sequence_audit.get("day_sequence_audits", {})
            if isinstance(sequence_audit, dict)
            else {}
        )
        if isinstance(day_audits, dict) and day_audits:
            for day, audit in sorted(day_audits.items()):
                normalized_day = str(day)[:10]
                key = (
                    f"{symbol}:{normalized_day}"
                    if symbol
                    else normalized_day
                )
                if key in out:
                    raise ValueError(
                        f"duplicate sequence audit key: {key}"
                    )
                if not isinstance(audit, dict):
                    raise ValueError(
                        f"invalid sequence audit payload for {normalized_day}"
                    )
                out[key] = dict(audit)
            continue
        start = datetime.fromisoformat(str(item["range_start_utc"]))
        end = datetime.fromisoformat(str(item["range_end_utc"]))
        if start.date() != end.date():
            raise ValueError(
                "eligible manifest requires one sequence audit range per UTC "
                f"day; found {start.isoformat()} -> {end.isoformat()}"
            )
        day = start.strftime("%Y-%m-%d")
        key = f"{symbol}:{day}" if symbol else day
        if key in out:
            raise ValueError(f"duplicate sequence audit key: {key}")
        out[key] = dict(sequence_audit)
    return payload, out


def _fast_forward_completed_days(
    raw_root: Path,
    target_roots: list[Path],
    exchange: str,
    symbol: str,
    write_start_dt: datetime,
    end_dt: datetime,
    warmup_hours: int,
    min_coverage: float,
    freshness_ms: int,
) -> tuple[datetime, datetime]:
    if write_start_dt.hour != 0:
        process_start_dt = max(write_start_dt - timedelta(hours=warmup_hours), MIN_AVAILABLE_UTC)
        return write_start_dt, process_start_dt

    resume_write_start_dt = write_start_dt
    while resume_write_start_dt <= end_dt:
        if not _day_is_complete(raw_root, target_roots, exchange, symbol, resume_write_start_dt, min_coverage, freshness_ms):
            break
        resume_write_start_dt += timedelta(days=1)

    process_start_dt = max(resume_write_start_dt - timedelta(hours=warmup_hours), MIN_AVAILABLE_UTC)
    return resume_write_start_dt, process_start_dt


def _extract_ts_ms(frame: pd.DataFrame, ts_col: str) -> list[int]:
    raw = pd.to_numeric(frame[ts_col], errors="coerce").fillna(0).astype("int64")
    arr = raw.to_numpy(copy=False)
    if len(arr) and arr.max(initial=0) >= 10**15:
        arr = arr // 10**6
    return arr.tolist()


def _select_ts_ms(frame: pd.DataFrame, timestamp_source: str) -> np.ndarray:
    """Select one causal exchange timestamp per source row.

    Live partial-depth state is stamped with Binance ``T`` (transaction time),
    while legacy normalized files used ``E`` (event time).  Keep the choice
    explicit so a replay artifact cannot silently change clocks.
    """

    fallback_order = {
        "event": ("event_time", "transaction_time", "received_time"),
        "transaction": ("transaction_time", "event_time", "received_time"),
        "received": ("received_time", "transaction_time", "event_time"),
    }
    try:
        columns = fallback_order[timestamp_source]
    except KeyError as exc:
        raise ValueError(f"unsupported timestamp source: {timestamp_source}") from exc

    selected = np.zeros(len(frame), dtype=np.int64)
    for column in columns:
        if column not in frame.columns:
            continue
        values = np.asarray(_extract_ts_ms(frame, column), dtype=np.int64)
        missing = selected <= 0
        selected[missing] = values[missing]
    return selected


def _resolve_jwt_payload(resp: requests.Response) -> str:
    candidates = []
    text = resp.text.strip()

    try:
        payload = resp.json()
    except Exception:
        payload = None

    if isinstance(payload, str):
        candidates.append(payload)
    elif isinstance(payload, dict):
        for key in ("token", "jwt", "jwt_token", "access_token", "accessToken"):
            value = payload.get(key)
            if isinstance(value, str):
                candidates.append(value)
        nested = payload.get("data")
        if isinstance(nested, str):
            candidates.append(nested)
        elif isinstance(nested, dict):
            for key in ("token", "jwt", "jwt_token", "access_token", "accessToken"):
                value = nested.get(key)
                if isinstance(value, str):
                    candidates.append(value)

    if text:
        candidates.append(text)

    token_pattern = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")

    for candidate in candidates:
        token = str(candidate).strip().strip('"')
        if token_pattern.fullmatch(token):
            return token
    raise RuntimeError(f"Unexpected JWT response payload: {text[:200]}")


class CryptoHFTClient:
    def __init__(
        self,
        api_key: Optional[str],
        jwt: Optional[str] = None,
        *,
        transport: str = DEFAULT_DOWNLOAD_TRANSPORT,
    ):
        self.api_key = api_key or None
        self.jwt = jwt or None
        self.session = requests.Session()
        self.sdk_client = None
        self.sdk_jwt_manager = None
        self.transport = self._configure_transport(transport)

    def _build_sdk_client(self):
        if SDKCryptoHFTDataClient is None:
            return None
        return SDKCryptoHFTDataClient(
            api_key=self.api_key,
            timeout=max(DEFAULT_DOWNLOAD_TIMEOUT),
            max_retries=5,
            use_jwt=not bool(self.jwt),
        )

    def _configure_transport(self, transport: str) -> str:
        if transport not in {"auto", "sdk", "rest"}:
            raise RuntimeError("--transport must be one of: auto, sdk, rest")
        if transport == "rest":
            return "rest"

        if SDKCryptoHFTDataClient is None:
            if transport == "sdk":
                raise RuntimeError(
                    "cryptohftdata SDK is not installed. Install it with 'pip install cryptohftdata'"
                )
            return "rest"

        if not self.api_key:
            if transport == "sdk":
                raise RuntimeError(
                    f"SDK transport requires {API_KEY_ENV} or --api-key"
                )
            return "rest"

        sdk_client = self._build_sdk_client()
        sdk_http_client = getattr(sdk_client, "_http_client", None)
        sdk_session = getattr(sdk_http_client, "session", None)
        if sdk_session is None:
            if transport == "sdk":
                raise RuntimeError("cryptohftdata SDK did not expose an HTTP session")
            return "rest"

        self.session.close()
        self.session = sdk_session
        self.sdk_client = sdk_client
        self.sdk_jwt_manager = getattr(sdk_client, "_jwt_manager", None)
        return "sdk"

    def _reset_session(self) -> None:
        self.session.close()
        if self.transport == "sdk":
            sdk_client = self._build_sdk_client()
            sdk_http_client = getattr(sdk_client, "_http_client", None)
            sdk_session = getattr(sdk_http_client, "session", None)
            if sdk_session is None:
                raise RuntimeError("cryptohftdata SDK did not expose an HTTP session")
            self.session = sdk_session
            self.sdk_client = sdk_client
            self.sdk_jwt_manager = getattr(sdk_client, "_jwt_manager", None)
            return

        self.session = requests.Session()

    def _refresh_jwt(self) -> str:
        if self.sdk_jwt_manager is not None:
            try:
                self.jwt = self.sdk_jwt_manager.get_jwt_token()
                return self.jwt
            except Exception:
                pass

        if not self.api_key:
            raise RuntimeError(
                f"Set {API_KEY_ENV} or pass --api-key/--jwt before downloading CryptoHFTData files"
            )

        last_error = None
        for method in ("post", "get"):
            request = getattr(self.session, method)
            try:
                resp = request(JWT_ENDPOINT, params={"api_key": self.api_key}, timeout=30)
            except requests.RequestException as exc:
                last_error = exc
                continue

            if resp.status_code in (404, 405):
                last_error = RuntimeError(f"JWT endpoint returned {resp.status_code}")
                continue
            resp.raise_for_status()
            self.jwt = _resolve_jwt_payload(resp)
            return self.jwt

        raise RuntimeError(f"Failed to refresh CryptoHFTData JWT: {last_error}")

    def ensure_jwt(self) -> str:
        return self.jwt or self._refresh_jwt()

    def _expected_download_size(self, resp: requests.Response, resume_bytes: int) -> Optional[int]:
        content_range = resp.headers.get("Content-Range", "").strip()
        if content_range:
            match = re.match(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", content_range)
            if match and match.group(3) != "*":
                return int(match.group(3))

        content_length = resp.headers.get("Content-Length")
        if not content_length:
            return None

        try:
            length = int(content_length)
        except ValueError:
            return None

        if resp.status_code == 206 and resume_bytes > 0:
            return resume_bytes + length
        return length

    def download_file(self, relative_path: Path, out_path: Path) -> str:
        if out_path.exists() and out_path.stat().st_size > 0:
            return "exists"

        out_path.parent.mkdir(parents=True, exist_ok=True)
        params = {"file": relative_path.as_posix()}
        last_error = None

        for attempt in range(1, DEFAULT_DOWNLOAD_ATTEMPTS + 1):
            part_path = out_path.with_suffix(out_path.suffix + ".part")
            resume_bytes = part_path.stat().st_size if part_path.exists() else 0
            headers = {
                "Authorization": f"Bearer {self.ensure_jwt()}",
                "Accept-Encoding": "identity",
                "Connection": "close",
            }
            if resume_bytes > 0:
                headers["Range"] = f"bytes={resume_bytes}-"

            try:
                with self.session.get(
                    DOWNLOAD_ENDPOINT,
                    params=params,
                    headers=headers,
                    stream=True,
                    timeout=DEFAULT_DOWNLOAD_TIMEOUT,
                ) as resp:
                    if resp.status_code == 404:
                        return "404"
                    if resp.status_code == 416:
                        expected_size = self._expected_download_size(resp, resume_bytes)
                        if expected_size is not None and resume_bytes >= expected_size and part_path.exists():
                            part_path.replace(out_path)
                            return "downloaded"

                        if part_path.exists():
                            part_path.unlink()
                        last_error = RuntimeError(
                            f"Download range failed with 416 for {relative_path.as_posix()}"
                        )
                        time.sleep(min(attempt * 2, 20))
                        continue
                    if resp.status_code in (401, 403):
                        self.jwt = None
                        last_error = RuntimeError(f"Download auth failed with {resp.status_code}")
                        continue
                    if resp.status_code >= 500:
                        last_error = RuntimeError(f"Server error {resp.status_code}")
                        time.sleep(min(attempt * 2, 20))
                        continue

                    resp.raise_for_status()
                    expected_size = self._expected_download_size(resp, resume_bytes)
                    can_resume = resume_bytes > 0 and resp.status_code == 206
                    if resume_bytes > 0 and not can_resume and part_path.exists():
                        part_path.unlink()
                        resume_bytes = 0
                        expected_size = self._expected_download_size(resp, resume_bytes)

                    write_mode = "ab" if can_resume else "wb"
                    with open(part_path, write_mode) as f:
                        for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                            if chunk:
                                f.write(chunk)

                    if expected_size is not None and part_path.stat().st_size < expected_size:
                        raise RuntimeError(
                            f"Short download for {relative_path.as_posix()}: "
                            f"got {part_path.stat().st_size} of {expected_size} bytes"
                        )
                part_path.replace(out_path)
                return "downloaded"
            except (requests.RequestException, RuntimeError) as exc:
                last_error = exc
            except OSError as exc:
                last_error = exc

            self._reset_session()
            print(
                f"[WARN] download attempt {attempt}/{DEFAULT_DOWNLOAD_ATTEMPTS} failed for "
                f"{relative_path.as_posix()} ({type(last_error).__name__}: {last_error})"
            )
            time.sleep(min(attempt * 2, 20))

        raise RuntimeError(f"Failed to download {relative_path}: {last_error}")


class OrderBookState:
    def __init__(self):
        self.bid_levels: dict[float, float] = {}
        self.ask_levels: dict[float, float] = {}
        self.bid_heap: list[float] = []
        self.ask_heap: list[float] = []

    def reset(self) -> None:
        self.bid_levels.clear()
        self.ask_levels.clear()
        self.bid_heap.clear()
        self.ask_heap.clear()

    def apply(self, side: str, price: float, quantity: float) -> None:
        if side == "bid":
            levels = self.bid_levels
            heap = self.bid_heap
            heap_key = -price
        elif side == "ask":
            levels = self.ask_levels
            heap = self.ask_heap
            heap_key = price
        else:
            return

        was_active = price in levels
        if quantity <= 0.0:
            levels.pop(price, None)
        else:
            levels[price] = quantity
            if not was_active:
                heapq.heappush(heap, heap_key)

        if len(heap) > max(4 * len(levels) + 1024, 4096):
            self._rebuild(side)

    def _rebuild(self, side: str) -> None:
        if side == "bid":
            self.bid_heap = [-price for price in self.bid_levels]
            heapq.heapify(self.bid_heap)
        else:
            self.ask_heap = [price for price in self.ask_levels]
            heapq.heapify(self.ask_heap)

    def _peek_levels(self, side: str, count: int) -> list[tuple[float, float]]:
        if count <= 0:
            return []

        if side == "bid":
            levels = self.bid_levels
            heap = self.bid_heap
            decode = lambda item: -item
        else:
            levels = self.ask_levels
            heap = self.ask_heap
            decode = lambda item: item

        taken = []
        seen_prices = set()
        out = []
        while heap and len(out) < count:
            item = heapq.heappop(heap)
            price = decode(item)
            qty = levels.get(price)
            if qty is None or qty <= 0.0:
                continue
            # A remove/re-add can leave an older lazy heap entry behind.  A
            # snapshot must still expose each exchange price level once.
            if price in seen_prices:
                continue
            seen_prices.add(price)
            out.append((price, qty))
            taken.append(item)

        for item in taken:
            heapq.heappush(heap, item)
        return out

    def top_levels(self, count: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        return self._peek_levels("bid", count), self._peek_levels("ask", count)


@dataclass
class OrderBookSequenceStats:
    logical_messages: int = 0
    snapshot_messages: int = 0
    update_messages: int = 0
    duplicate_messages: int = 0
    duplicate_snapshots: int = 0
    stale_updates: int = 0
    ignored_before_snapshot: int = 0
    sequence_gaps: int = 0
    invalid_sequence_messages: int = 0
    accepted_updates: int = 0
    delta_bootstrap_messages: int = 0
    message_intervals: int = 0
    message_interval_sum_ms: int = 0
    message_interval_le_10ms: int = 0
    message_interval_le_25ms: int = 0
    message_interval_le_50ms: int = 0
    message_interval_le_100ms: int = 0
    message_interval_le_250ms: int = 0
    message_interval_le_500ms: int = 0
    message_interval_le_1000ms: int = 0
    message_time_reversals: int = 0


class OrderBookSequenceState:
    """Validate Binance snapshot/delta continuity across batches and hours."""

    def __init__(
        self,
        book: OrderBookState,
        *,
        allow_delta_bootstrap: bool = False,
    ):
        self.book = book
        self.stats = OrderBookSequenceStats()
        self.allow_delta_bootstrap = bool(allow_delta_bootstrap)
        self.initialized = False
        self.bridge_pending = False
        self.last_update_id: Optional[int] = None
        self.last_snapshot_key: Optional[tuple[int, int]] = None
        self.current_message_key: Optional[tuple] = None
        self.current_message_apply = False
        self.previous_message_ts_ms: Optional[int] = None
        self.initialization_source: Optional[str] = None
        self.initialization_ts_ms: Optional[int] = None

    def _invalidate(self) -> bool:
        self.book.reset()
        self.initialized = False
        self.bridge_pending = False
        self.last_update_id = None
        self.initialization_source = None
        self.initialization_ts_ms = None
        self.stats.sequence_gaps += 1
        return False

    def invalidate_source_gap(self) -> None:
        """Invalidate continuity after a missing/non-adjacent raw hour."""

        self.current_message_key = None
        self.current_message_apply = False
        self.previous_message_ts_ms = None
        self._invalidate()

    def output_ready(self, ts_ms: int, delta_convergence_ms: int) -> bool:
        """Return whether the current book may be emitted as normalized L2.

        Native source snapshots are usable immediately. A delta-bootstrap
        starts from an empty book and therefore needs an explicit burn-in
        before active top levels are expected to have converged.
        """

        if not self.initialized:
            return False
        if self.initialization_source == "snapshot":
            return True
        if (
            self.initialization_source != "delta"
            or self.initialization_ts_ms is None
        ):
            return False
        return int(ts_ms) >= (
            int(self.initialization_ts_ms) + max(0, int(delta_convergence_ms))
        )

    def begin_message(
        self,
        *,
        event_type: str,
        receive_time_ms: int,
        event_time_ms: int,
        transaction_time_ms: int,
        first_update_id: Optional[int],
        final_update_id: Optional[int],
        previous_final_update_id: Optional[int],
        last_update_id: Optional[int],
    ) -> bool:
        """Return whether all price-level rows in this logical message apply."""

        event_type = str(event_type).lower()
        if event_type == "snapshot":
            # One native snapshot contains thousands of price-level rows.
            # CryptoHFTData can stamp those rows with more than one local
            # receive timestamp even though event_time/lastUpdateId identify
            # one exchange snapshot. Do not reset or reject the remaining
            # levels merely because their recorder timestamp differs.
            message_key = (
                event_type,
                int(event_time_ms),
                last_update_id,
                final_update_id,
            )
        else:
            message_key = (
                event_type,
                int(receive_time_ms),
                int(event_time_ms),
                int(transaction_time_ms),
                first_update_id,
                final_update_id,
                previous_final_update_id,
                last_update_id,
            )
        if message_key == self.current_message_key:
            return self.current_message_apply

        self.current_message_key = message_key
        self.stats.logical_messages += 1
        message_ts_ms = next(
            (
                int(value)
                for value in (
                    transaction_time_ms,
                    event_time_ms,
                    receive_time_ms,
                )
                if int(value) > 0
            ),
            0,
        )
        if message_ts_ms > 0 and self.previous_message_ts_ms is not None:
            interval_ms = message_ts_ms - self.previous_message_ts_ms
            if interval_ms < 0:
                self.stats.message_time_reversals += 1
            else:
                self.stats.message_intervals += 1
                self.stats.message_interval_sum_ms += int(interval_ms)
                for threshold, field in (
                    (10, "message_interval_le_10ms"),
                    (25, "message_interval_le_25ms"),
                    (50, "message_interval_le_50ms"),
                    (100, "message_interval_le_100ms"),
                    (250, "message_interval_le_250ms"),
                    (500, "message_interval_le_500ms"),
                    (1000, "message_interval_le_1000ms"),
                ):
                    if interval_ms <= threshold:
                        setattr(
                            self.stats,
                            field,
                            int(getattr(self.stats, field)) + 1,
                        )
        if message_ts_ms > 0:
            self.previous_message_ts_ms = message_ts_ms

        if event_type == "snapshot":
            snapshot_update_id = (
                last_update_id if last_update_id is not None else final_update_id
            )
            if snapshot_update_id is None:
                self.stats.invalid_sequence_messages += 1
                self.current_message_apply = self._invalidate()
                return self.current_message_apply

            snapshot_key = (int(event_time_ms), int(snapshot_update_id))
            if snapshot_key == self.last_snapshot_key:
                self.stats.duplicate_messages += 1
                self.stats.duplicate_snapshots += 1
                self.current_message_apply = False
                return False

            self.book.reset()
            self.initialized = True
            self.bridge_pending = True
            self.last_update_id = int(snapshot_update_id)
            self.last_snapshot_key = snapshot_key
            self.initialization_source = "snapshot"
            self.initialization_ts_ms = message_ts_ms or None
            self.stats.snapshot_messages += 1
            self.current_message_apply = True
            return True

        self.stats.update_messages += 1
        if final_update_id is None:
            self.stats.invalid_sequence_messages += 1
            self.current_message_apply = self._invalidate()
            return self.current_message_apply
        if not self.initialized or self.last_update_id is None:
            if (
                self.allow_delta_bootstrap
                and previous_final_update_id is not None
            ):
                # This does not invent a full snapshot. It anchors the update
                # chain at the first observed ``pu`` and starts from an empty
                # book. Output remains blocked until the configured
                # convergence burn-in has elapsed.
                self.book.reset()
                self.initialized = True
                self.bridge_pending = False
                self.last_update_id = int(previous_final_update_id)
                self.initialization_source = "delta"
                self.initialization_ts_ms = message_ts_ms or None
                self.stats.delta_bootstrap_messages += 1
            else:
                self.stats.ignored_before_snapshot += 1
                self.current_message_apply = False
                return False

        final_id = int(final_update_id)
        current_id = int(self.last_update_id)
        if final_id <= current_id:
            self.stats.duplicate_messages += 1
            self.stats.stale_updates += 1
            self.current_message_apply = False
            return False

        if self.bridge_pending:
            # Binance requires the first event after a REST snapshot to span
            # lastUpdateId.  A recorder that snapshots before subscribing can
            # instead expose the immediately following event, in which case
            # ``pu == lastUpdateId`` is the stronger continuity proof.
            spans_snapshot = (
                first_update_id is not None
                and int(first_update_id) <= current_id <= final_id
            )
            follows_snapshot = (
                previous_final_update_id is not None
                and int(previous_final_update_id) == current_id
            )
            if not (spans_snapshot or follows_snapshot):
                self.current_message_apply = self._invalidate()
                return self.current_message_apply
            self.bridge_pending = False
        elif previous_final_update_id is not None:
            if int(previous_final_update_id) != current_id:
                self.current_message_apply = self._invalidate()
                return self.current_message_apply
        elif first_update_id is not None and int(first_update_id) > current_id + 1:
            self.current_message_apply = self._invalidate()
            return self.current_message_apply

        self.last_update_id = final_id
        self.stats.accepted_updates += 1
        self.current_message_apply = True
        return True


def _bbo_schema() -> pa.schema:
    return pa.schema([
        ("timestamp", pa.int64()),
        ("best_bid", pa.float64()),
        ("best_bid_qty", pa.float64()),
        ("best_ask", pa.float64()),
        ("best_ask_qty", pa.float64()),
    ])


def _l2_schema(levels: int) -> pa.schema:
    fields = [("timestamp", pa.int64())]
    for level in range(1, levels + 1):
        fields.extend([
            (f"bid_px_{level}", pa.float64()),
            (f"bid_qty_{level}", pa.float64()),
            (f"ask_px_{level}", pa.float64()),
            (f"ask_qty_{level}", pa.float64()),
        ])
    return pa.schema(fields)


@dataclass
class _TargetDayWriter:
    bbo_tmp_path: Path
    bbo_final_path: Path
    l2_tmp_path: Path
    l2_final_path: Path
    bbo_writer: pq.ParquetWriter
    l2_writer: pq.ParquetWriter


class DailyOutputWriter:
    def __init__(
        self,
        target_roots: list[Path],
        symbol: str,
        levels: int,
        *,
        allowed_days: Optional[set[str]] = None,
    ):
        self.target_roots = target_roots
        self.symbol = symbol
        self.levels = levels
        self.allowed_days = set(allowed_days) if allowed_days is not None else None
        self.day_tag: Optional[str] = None
        self.day_rows = 0
        self.total_rows = 0
        self.targets: list[_TargetDayWriter] = []
        self.bbo_buffer = self._new_bbo_buffer()
        self.l2_buffer = self._new_l2_buffer()

    def _new_bbo_buffer(self) -> dict[str, list[float]]:
        return {
            "timestamp": [],
            "best_bid": [],
            "best_bid_qty": [],
            "best_ask": [],
            "best_ask_qty": [],
        }

    def _new_l2_buffer(self) -> dict[str, list[float]]:
        out = {"timestamp": []}
        for level in range(1, self.levels + 1):
            out[f"bid_px_{level}"] = []
            out[f"bid_qty_{level}"] = []
            out[f"ask_px_{level}"] = []
            out[f"ask_qty_{level}"] = []
        return out

    def _open_day(self, day_tag: str) -> None:
        self.close()
        self.day_tag = day_tag
        self.day_rows = 0
        self.targets = []

        bbo_schema = _bbo_schema()
        l2_schema = _l2_schema(self.levels)
        for root in self.target_roots:
            bbo_dir = root / "bbo"
            l2_dir = root / "l2"
            bbo_dir.mkdir(parents=True, exist_ok=True)
            l2_dir.mkdir(parents=True, exist_ok=True)

            bbo_final = bbo_dir / f"{self.symbol}-bbo-{day_tag}.parquet"
            l2_final = l2_dir / f"{self.symbol}-l2-{day_tag}.parquet"
            bbo_tmp = bbo_final.with_suffix(bbo_final.suffix + ".tmp")
            l2_tmp = l2_final.with_suffix(l2_final.suffix + ".tmp")
            if bbo_tmp.exists():
                bbo_tmp.unlink()
            if l2_tmp.exists():
                l2_tmp.unlink()

            self.targets.append(_TargetDayWriter(
                bbo_tmp_path=bbo_tmp,
                bbo_final_path=bbo_final,
                l2_tmp_path=l2_tmp,
                l2_final_path=l2_final,
                bbo_writer=pq.ParquetWriter(bbo_tmp, bbo_schema, compression="zstd"),
                l2_writer=pq.ParquetWriter(l2_tmp, l2_schema, compression="zstd"),
            ))

    def append(self, ts_ms: int, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> None:
        day_tag = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if self.allowed_days is not None and day_tag not in self.allowed_days:
            return
        if day_tag != self.day_tag:
            self._open_day(day_tag)

        best_bid, best_bid_qty = bids[0]
        best_ask, best_ask_qty = asks[0]
        self.bbo_buffer["timestamp"].append(ts_ms)
        self.bbo_buffer["best_bid"].append(best_bid)
        self.bbo_buffer["best_bid_qty"].append(best_bid_qty)
        self.bbo_buffer["best_ask"].append(best_ask)
        self.bbo_buffer["best_ask_qty"].append(best_ask_qty)

        self.l2_buffer["timestamp"].append(ts_ms)
        for idx in range(self.levels):
            bid_px, bid_qty = bids[idx] if idx < len(bids) else (0.0, 0.0)
            ask_px, ask_qty = asks[idx] if idx < len(asks) else (0.0, 0.0)
            level = idx + 1
            self.l2_buffer[f"bid_px_{level}"].append(bid_px)
            self.l2_buffer[f"bid_qty_{level}"].append(bid_qty)
            self.l2_buffer[f"ask_px_{level}"].append(ask_px)
            self.l2_buffer[f"ask_qty_{level}"].append(ask_qty)

        if len(self.bbo_buffer["timestamp"]) >= 10_000:
            self._flush_buffers()

    def _flush_buffers(self) -> None:
        if not self.targets or not self.bbo_buffer["timestamp"]:
            return

        bbo_table = pa.Table.from_pydict(self.bbo_buffer, schema=_bbo_schema())
        l2_table = pa.Table.from_pydict(self.l2_buffer, schema=_l2_schema(self.levels))
        for target in self.targets:
            target.bbo_writer.write_table(bbo_table)
            target.l2_writer.write_table(l2_table)

        row_count = len(self.bbo_buffer["timestamp"])
        self.day_rows += row_count
        self.total_rows += row_count
        self.bbo_buffer = self._new_bbo_buffer()
        self.l2_buffer = self._new_l2_buffer()

    def close(self) -> None:
        if not self.targets:
            self.day_tag = None
            self.bbo_buffer = self._new_bbo_buffer()
            self.l2_buffer = self._new_l2_buffer()
            return

        self._flush_buffers()
        for target in self.targets:
            target.bbo_writer.close()
            target.l2_writer.close()
            if self.day_rows > 0:
                target.bbo_tmp_path.replace(target.bbo_final_path)
                target.l2_tmp_path.replace(target.l2_final_path)
            else:
                if target.bbo_tmp_path.exists():
                    target.bbo_tmp_path.unlink()
                if target.l2_tmp_path.exists():
                    target.l2_tmp_path.unlink()

        self.targets = []
        self.day_tag = None
        self.day_rows = 0
        self.bbo_buffer = self._new_bbo_buffer()
        self.l2_buffer = self._new_l2_buffer()


def _decompress_parquet_zst(path: Path) -> Path:
    with tempfile.NamedTemporaryFile(prefix="cryptohftdata-", suffix=".parquet", delete=False) as tmp:
        with open(path, "rb") as src:
            # CryptoHFT's SDK may transparently return an already decompressed
            # Parquet object even though the remote key retains its .zst
            # suffix.  Preserve the raw cache key, but accept either wire
            # representation at this boundary.
            magic = src.read(4)
            src.seek(0)
            reader = src if magic == b"PAR1" else zstd.ZstdDecompressor().stream_reader(src)
            while True:
                chunk = reader.read(8 * 1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)
        return Path(tmp.name)


def _replay_hour_with_retry(
    client: Optional[CryptoHFTClient],
    rel_path: Path,
    raw_path: Path,
    book: OrderBookState,
    writer: DailyOutputWriter,
    levels: int,
    snapshot_ms: int,
    write_start_ms: int,
    current_bucket_id: Optional[int],
    current_bucket_ts_ms: Optional[int],
    sequence_state: OrderBookSequenceState,
    timestamp_source: str,
    delta_convergence_ms: int,
    *,
    max_attempts: int = 3,
    allow_redownload: bool = True,
    verbose: bool = False,
) -> tuple[Optional[int], Optional[int], int]:
    last_error: Optional[Exception] = None
    redownloaded = 0

    for attempt in range(1, max_attempts + 1):
        temp_parquet: Optional[Path] = None
        try:
            temp_parquet = _decompress_parquet_zst(raw_path)
            next_bucket_id, next_bucket_ts_ms = _replay_orderbook_file(
                temp_parquet,
                book,
                writer,
                levels,
                snapshot_ms,
                write_start_ms,
                current_bucket_id,
                current_bucket_ts_ms,
                sequence_state,
                timestamp_source,
                delta_convergence_ms,
            )
            return next_bucket_id, next_bucket_ts_ms, redownloaded
        except Exception as exc:
            last_error = exc
            if not allow_redownload or client is None or attempt >= max_attempts:
                break

            try:
                if raw_path.exists():
                    raw_path.unlink()
            except OSError:
                pass

            print(
                f"[WARN] invalid raw hour {rel_path.as_posix()} "
                f"({type(exc).__name__}: {exc}); re-downloading"
            )
            status = client.download_file(rel_path, raw_path)
            if status == "404":
                raise RuntimeError(
                    f"Failed to recover {rel_path.as_posix()}: file returned 404 after decode failure"
                ) from exc
            if status == "downloaded":
                redownloaded += 1
                if verbose:
                    print(f"[OK]   re-downloaded {rel_path.as_posix()}")
        finally:
            if temp_parquet is not None and temp_parquet.exists():
                temp_parquet.unlink()

    raise RuntimeError(
        f"Failed to decode {rel_path.as_posix()} after {max_attempts} attempts: {last_error}"
    ) from last_error


def _emit_snapshot(
    book: OrderBookState,
    last_ts_ms: Optional[int],
    writer: DailyOutputWriter,
    levels: int,
    write_start_ms: int,
    sequence_state: OrderBookSequenceState,
    delta_convergence_ms: int,
) -> None:
    if last_ts_ms is None or last_ts_ms < write_start_ms:
        return
    if not sequence_state.output_ready(last_ts_ms, delta_convergence_ms):
        return

    bids, asks = book.top_levels(levels)
    if len(bids) < levels or len(asks) < levels:
        return
    if bids[0][0] <= 0.0 or asks[0][0] <= bids[0][0]:
        return
    writer.append(last_ts_ms, bids, asks)


def _replay_orderbook_file(
    parquet_path: Path,
    book: OrderBookState,
    writer: DailyOutputWriter,
    levels: int,
    snapshot_ms: int,
    write_start_ms: int,
    current_bucket_id: Optional[int],
    current_bucket_ts_ms: Optional[int],
    sequence_state: OrderBookSequenceState,
    timestamp_source: str,
    delta_convergence_ms: int,
) -> tuple[Optional[int], Optional[int]]:
    parquet_file = pq.ParquetFile(parquet_path)
    available_columns = set(parquet_file.schema.names)
    columns = [
        col for col in (
            "event_time",
            "transaction_time",
            "received_time",
            "event_type",
            "first_update_id",
            "final_update_id",
            "prev_final_update_id",
            "last_update_id",
            "side",
            "price",
            "quantity",
        )
        if col in available_columns
    ]
    for batch in parquet_file.iter_batches(batch_size=500_000, columns=columns):
        frame = batch.to_pandas()
        if frame.empty:
            continue

        ts_ms_arr = _select_ts_ms(frame, timestamp_source)
        def _timestamp_array(
            column: str,
            source_frame: pd.DataFrame = frame,
        ) -> np.ndarray:
            if column not in source_frame.columns:
                return np.zeros(len(source_frame), dtype=np.int64)
            return np.asarray(_extract_ts_ms(source_frame, column), dtype=np.int64)

        event_ts_ms_arr = _timestamp_array("event_time")
        transaction_ts_ms_arr = _timestamp_array("transaction_time")
        receive_ts_ms_arr = _timestamp_array("received_time")

        side_arr = frame["side"].astype(str).str.lower().to_numpy(copy=False)
        event_type_arr = (
            frame["event_type"].astype(str).str.lower().to_numpy(copy=False)
            if "event_type" in frame.columns
            else np.full(len(frame), "update", dtype=object)
        )
        last_update_arr = (
            pd.to_numeric(frame["last_update_id"], errors="coerce").to_numpy(dtype=np.float64, copy=False)
            if "last_update_id" in frame.columns
            else np.full(len(frame), np.nan, dtype=np.float64)
        )
        first_update_arr = (
            pd.to_numeric(frame["first_update_id"], errors="coerce").to_numpy(dtype=np.float64, copy=False)
            if "first_update_id" in frame.columns
            else np.full(len(frame), np.nan, dtype=np.float64)
        )
        final_update_arr = (
            pd.to_numeric(frame["final_update_id"], errors="coerce").to_numpy(dtype=np.float64, copy=False)
            if "final_update_id" in frame.columns
            else np.full(len(frame), np.nan, dtype=np.float64)
        )
        previous_final_arr = (
            pd.to_numeric(frame["prev_final_update_id"], errors="coerce").to_numpy(dtype=np.float64, copy=False)
            if "prev_final_update_id" in frame.columns
            else np.full(len(frame), np.nan, dtype=np.float64)
        )
        price_arr = pd.to_numeric(frame["price"], errors="coerce").to_numpy(dtype=np.float64, copy=False)
        qty_arr = pd.to_numeric(frame["quantity"], errors="coerce").to_numpy(dtype=np.float64, copy=False)

        for (
            ts_ms,
            receive_ts_ms,
            event_ts_ms,
            transaction_ts_ms,
            event_type,
            first_update_id,
            final_update_id,
            previous_final_update_id,
            last_update_id,
            side,
            price,
            qty,
        ) in zip(
            ts_ms_arr,
            receive_ts_ms_arr,
            event_ts_ms_arr,
            transaction_ts_ms_arr,
            event_type_arr,
            first_update_arr,
            final_update_arr,
            previous_final_arr,
            last_update_arr,
            side_arr,
            price_arr,
            qty_arr,
            strict=True,
        ):
            if ts_ms <= 0 or side not in {"bid", "ask"}:
                continue
            if not pd.notna(price) or price <= 0.0 or not pd.notna(qty):
                continue

            bucket_id = (ts_ms // snapshot_ms) * snapshot_ms if snapshot_ms > 0 else ts_ms
            if current_bucket_id is None:
                current_bucket_id = bucket_id
            elif bucket_id != current_bucket_id:
                _emit_snapshot(
                    book,
                    current_bucket_ts_ms,
                    writer,
                    levels,
                    write_start_ms,
                    sequence_state,
                    delta_convergence_ms,
                )
                current_bucket_id = bucket_id

            apply_message = sequence_state.begin_message(
                event_type=event_type,
                receive_time_ms=int(receive_ts_ms),
                event_time_ms=int(event_ts_ms),
                transaction_time_ms=int(transaction_ts_ms),
                first_update_id=int(first_update_id) if pd.notna(first_update_id) else None,
                final_update_id=int(final_update_id) if pd.notna(final_update_id) else None,
                previous_final_update_id=(
                    int(previous_final_update_id)
                    if pd.notna(previous_final_update_id)
                    else None
                ),
                last_update_id=int(last_update_id) if pd.notna(last_update_id) else None,
            )
            if not apply_message:
                continue

            book.apply(side, float(price), max(float(qty), 0.0))
            current_bucket_ts_ms = ts_ms

    return current_bucket_id, current_bucket_ts_ms


def _process_symbol(
    client: Optional[CryptoHFTClient],
    raw_root: Path,
    target_roots: list[Path],
    symbol: str,
    exchange: str,
    start_dt: datetime,
    end_dt: datetime,
    levels: int,
    snapshot_ms: int,
    warmup_hours: int,
    min_coverage: float,
    freshness_ms: int,
    force_rebuild: bool,
    timestamp_source: str,
    sequence_bootstrap: str = DEFAULT_SEQUENCE_BOOTSTRAP,
    delta_convergence_ms: int = DEFAULT_DELTA_CONVERGENCE_MS,
    allowed_days: Optional[set[str]] = None,
    download_missing: bool = True,
    verbose: bool = False,
) -> tuple[int, int, int, dict[str, object]]:
    requested_start_dt = max(_floor_hour(start_dt), MIN_AVAILABLE_UTC)
    write_start_dt = max(_daily_write_start(requested_start_dt), MIN_AVAILABLE_UTC)
    if write_start_dt < requested_start_dt:
        print(
            f"[NORMALIZE] {symbol}: expanding daily rebuild start from "
            f"{requested_start_dt:%Y-%m-%d %H:%M} to {write_start_dt:%Y-%m-%d %H:%M} UTC"
        )
    if force_rebuild:
        process_start_dt = max(write_start_dt - timedelta(hours=warmup_hours), MIN_AVAILABLE_UTC)
    else:
        write_start_dt, process_start_dt = _fast_forward_completed_days(
            raw_root,
            target_roots,
            exchange,
            symbol,
            write_start_dt,
            end_dt,
            warmup_hours,
            min_coverage,
            freshness_ms,
        )
    write_start_ms = int(write_start_dt.timestamp() * 1000)

    if write_start_dt > end_dt:
        if verbose:
            print(f"[SKIP] {symbol}: requested range already finalized")
        return 0, 0, 0, {}

    if write_start_dt > requested_start_dt:
        print(
            f"[RESUME] {symbol}: skipping completed days before "
            f"{write_start_dt:%Y-%m-%d %H:%M} UTC"
        )

    book = OrderBookState()
    sequence_state = OrderBookSequenceState(
        book,
        allow_delta_bootstrap=sequence_bootstrap == "delta-converged",
    )
    writer = DailyOutputWriter(
        target_roots,
        symbol,
        levels,
        allowed_days=allowed_days,
    )
    current_bucket_id = None
    current_bucket_ts_ms = None
    downloaded = 0
    reused = 0
    missing = 0
    last_hour: Optional[datetime] = None
    current_target_day = ""
    target_stats_start: Optional[dict[str, int]] = None
    target_initialized_at_start = False
    target_initialization_source_at_start = ""
    day_sequence_audits: dict[str, dict[str, object]] = {}

    def finalize_target_day() -> None:
        nonlocal current_target_day
        nonlocal target_stats_start
        if not current_target_day or target_stats_start is None:
            return
        current_stats = asdict(sequence_state.stats)
        audit: dict[str, object] = {
            f"target_{key}": int(value)
            - int(target_stats_start.get(key, 0))
            for key, value in current_stats.items()
        }
        audit["target_initialized_at_start"] = bool(
            target_initialized_at_start
        )
        audit["target_initialization_source_at_start"] = (
            target_initialization_source_at_start
        )
        day_sequence_audits[current_target_day] = audit

    try:
        for hour_dt in _iter_hours(process_start_dt, end_dt):
            hour_day = hour_dt.strftime("%Y-%m-%d")
            target_hour = (
                hour_dt >= write_start_dt
                and (
                    allowed_days is None
                    or hour_day in allowed_days
                )
            )
            if target_hour and hour_day != current_target_day:
                finalize_target_day()
                current_target_day = hour_day
                target_stats_start = asdict(sequence_state.stats)
                target_initialized_at_start = bool(
                    sequence_state.initialized
                )
                target_initialization_source_at_start = str(
                    sequence_state.initialization_source or ""
                )
            if last_hour is not None and hour_dt - last_hour > timedelta(hours=1):
                _emit_snapshot(
                    book,
                    current_bucket_ts_ms,
                    writer,
                    levels,
                    write_start_ms,
                    sequence_state,
                    delta_convergence_ms,
                )
                sequence_state.invalidate_source_gap()
                current_bucket_id = None
                current_bucket_ts_ms = None

            rel_path = _object_rel_path(exchange, symbol, hour_dt)
            raw_path = raw_root / rel_path
            if raw_path.exists() and raw_path.stat().st_size > 0:
                status = "exists"
            elif download_missing:
                if client is None:
                    raise RuntimeError(
                        "download_missing=True requires a CryptoHFT client"
                    )
                status = client.download_file(rel_path, raw_path)
            else:
                status = "404"
            if status == "404":
                prefix = "[MISS]" if not download_missing else "[404] "
                if verbose:
                    print(f"{prefix} {rel_path.as_posix()}")
                _emit_snapshot(
                    book,
                    current_bucket_ts_ms,
                    writer,
                    levels,
                    write_start_ms,
                    sequence_state,
                    delta_convergence_ms,
                )
                sequence_state.invalidate_source_gap()
                current_bucket_id = None
                current_bucket_ts_ms = None
                missing += 1
                last_hour = None
                continue
            if status == "downloaded":
                downloaded += 1
                if verbose:
                    print(f"[OK]   downloaded {rel_path.as_posix()}")
            else:
                reused += 1
                if verbose:
                    print(f"[SKIP] {rel_path.as_posix()} already exists")

            current_bucket_id, current_bucket_ts_ms, recovered_downloads = _replay_hour_with_retry(
                client,
                rel_path,
                raw_path,
                book,
                writer,
                levels,
                snapshot_ms,
                write_start_ms,
                current_bucket_id,
                current_bucket_ts_ms,
                sequence_state,
                timestamp_source,
                delta_convergence_ms,
                allow_redownload=download_missing,
                verbose=verbose,
            )
            downloaded += recovered_downloads

            last_hour = hour_dt

        finalize_target_day()
        _emit_snapshot(
            book,
            current_bucket_ts_ms,
            writer,
            levels,
            write_start_ms,
            sequence_state,
            delta_convergence_ms,
        )
    finally:
        writer.close()

    print(
        "  Sequence audit: "
        + json.dumps(asdict(sequence_state.stats), sort_keys=True)
    )

    sequence_audit: dict[str, object] = asdict(sequence_state.stats)
    sequence_audit["day_sequence_audits"] = day_sequence_audits
    if len(day_sequence_audits) == 1:
        sequence_audit.update(next(iter(day_sequence_audits.values())))

    return downloaded, reused, writer.total_rows, sequence_audit


def _process_symbol_task(payload: dict[str, object]) -> dict[str, object]:
    """Picklable retained-range worker used after raw prefetch."""

    downloaded, reused, rows, sequence_audit = _process_symbol(
        client=None,
        raw_root=Path(str(payload["raw_root"])),
        target_roots=[
            Path(str(path)) for path in payload["target_roots"]
        ],
        symbol=str(payload["symbol"]),
        exchange=str(payload["exchange"]),
        start_dt=payload["start_dt"],
        end_dt=payload["end_dt"],
        levels=int(payload["levels"]),
        snapshot_ms=int(payload["snapshot_ms"]),
        warmup_hours=int(payload["warmup_hours"]),
        min_coverage=float(payload["min_coverage"]),
        freshness_ms=int(payload["freshness_ms"]),
        force_rebuild=bool(payload["force_rebuild"]),
        timestamp_source=str(payload["timestamp_source"]),
        sequence_bootstrap=str(payload["sequence_bootstrap"]),
        delta_convergence_ms=int(payload["delta_convergence_ms"]),
        allowed_days=set(payload["allowed_days"]),
        download_missing=False,
        verbose=bool(payload["verbose"]),
    )
    return {
        "symbol": str(payload["symbol"]),
        "range_index": int(payload["range_index"]),
        "range_start_utc": payload["start_dt"].isoformat(),
        "range_end_utc": payload["end_dt"].isoformat(),
        "downloaded": downloaded,
        "reused": reused,
        "rows": rows,
        "sequence_audit": sequence_audit,
    }


def _effective_repair_refresh_scope(
    repair: BadDayRepair,
    requested_scope: str,
) -> str:
    if requested_scope != "none":
        return requested_scope
    if repair.suggested_fix == "refresh_raw_and_rebuild":
        return "listed" if repair.missing_raw_hours else "day"
    return "none"


def _repair_action(repair: BadDayRepair, effective_refresh_scope: str) -> str:
    if effective_refresh_scope != "none":
        return "refresh_raw_and_rebuild"
    if repair.suggested_fix == "retry_download" or repair.cause == "missing_raw_hours":
        return "retry_download"
    return "force_rebuild_from_raw"


def _refresh_repair_raw_files(
    client: CryptoHFTClient,
    raw_root: Path,
    paths: list[Path],
) -> int:
    """Validate refreshed raw hours before atomically replacing cached files."""

    refreshed = 0
    for raw_path in paths:
        try:
            relative_path = raw_path.relative_to(raw_root)
        except ValueError as exc:
            raise ValueError(
                f"repair raw path is outside configured root: {raw_path}"
            ) from exc
        refresh_path = raw_path.with_suffix(raw_path.suffix + ".refresh")
        refresh_part_path = refresh_path.with_suffix(refresh_path.suffix + ".part")
        for transient_path in (refresh_path, refresh_part_path):
            if transient_path.exists():
                transient_path.unlink()
        try:
            status = client.download_file(relative_path, refresh_path)
            if status == "404":
                raise RuntimeError(
                    f"refresh source returned 404: {relative_path.as_posix()}"
                )
            if not refresh_path.exists() or refresh_path.stat().st_size <= 0:
                raise RuntimeError(
                    f"refresh produced no data: {relative_path.as_posix()}"
                )
            timestamps, _ = _read_raw_parquet_zst_summary(refresh_path)
            if timestamps is None or len(timestamps) == 0:
                raise RuntimeError(
                    f"refreshed raw file failed decode validation: "
                    f"{relative_path.as_posix()}"
                )
            refresh_path.replace(raw_path)
            refreshed += 1
        finally:
            for transient_path in (refresh_path, refresh_part_path):
                if transient_path.exists():
                    transient_path.unlink()
    return refreshed


def _run_bad_day_repairs(
    *,
    args: argparse.Namespace,
    raw_root: Path,
    target_roots: list[Path],
    symbols: list[str],
    start_dt: datetime,
    end_dt: datetime,
    freshness_ms: int,
) -> list[dict[str, object]]:
    repair_csv = Path(args.repair_audit_csv).expanduser().resolve()
    if not repair_csv.exists():
        raise SystemExit(f"repair audit CSV does not exist: {repair_csv}")

    selected = _select_bad_day_repairs(
        _load_bad_day_repairs(repair_csv),
        symbols=set(symbols),
        causes=set(args.repair_causes or []),
        include_nonfixable=bool(args.include_nonfixable),
        limit=None,
    )
    selected = [
        repair
        for repair in selected
        if start_dt
        <= datetime.strptime(repair.date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        and datetime.strptime(repair.date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc,
            hour=23,
        )
        <= end_dt
    ]
    if args.repair_limit is not None:
        selected = selected[: args.repair_limit]
    if not selected:
        raise SystemExit(f"No matching bad days selected from {repair_csv}")

    requested_refresh_scope = str(args.repair_refresh_raw)
    refresh_scopes = {
        (repair.symbol, repair.date): _effective_repair_refresh_scope(
            repair,
            requested_refresh_scope,
        )
        for repair in selected
    }
    actions = {
        (repair.symbol, repair.date): _repair_action(
            repair,
            refresh_scopes[(repair.symbol, repair.date)],
        )
        for repair in selected
    }
    needs_download = any(
        action in _REPAIR_DOWNLOAD_FIXES for action in actions.values()
    )
    if needs_download and not args.dry_run and not (args.api_key or args.jwt):
        raise SystemExit(
            f"Repair plan requires raw downloads; set {API_KEY_ENV} or "
            f"{JWT_ENV}, or pass --api-key/--jwt"
        )
    if int(args.process_workers) != 1:
        print(
            "[WARN] repair mode intentionally processes independent days "
            "serially; --process-workers is ignored"
        )

    print(f"Selected {len(selected)} bad-day repairs from {repair_csv}")
    repair_client = (
        CryptoHFTClient(
            api_key=args.api_key,
            jwt=args.jwt,
            transport=args.transport,
        )
        if needs_download and not args.dry_run
        else None
    )
    results: list[dict[str, object]] = []
    audit_frames: list[pd.DataFrame] = []
    for index, repair in enumerate(selected, start=1):
        action = actions[(repair.symbol, repair.date)]
        refresh_scope = refresh_scopes[(repair.symbol, repair.date)]
        download_missing = action in _REPAIR_DOWNLOAD_FIXES
        day_start = datetime.strptime(repair.date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        day_end = day_start.replace(hour=23)
        refresh_paths: list[Path] = []
        if refresh_scope != "none":
            refresh_paths = _raw_paths_for_repair(
                raw_root,
                args.exchange,
                repair,
                refresh_entire_day=refresh_scope == "day",
            )

        print(
            f"[{index}/{len(selected)}] {repair.symbol} {repair.date} "
            f"cause={repair.cause} action={action}"
        )
        if refresh_scope == "listed" and not refresh_paths:
            print("  raw refresh: no hours listed in the audit CSV")
        elif refresh_paths:
            print(
                f"  raw refresh: {len(refresh_paths)} canonical hourly paths "
                f"({refresh_scope})"
            )

        if args.dry_run:
            print(
                "  dry-run: "
                + ("download missing raw + " if download_missing else "reuse raw + ")
                + "force normalized rebuild"
            )
            results.append(
                {
                    "symbol": repair.symbol,
                    "repair_date": repair.date,
                    "repair_cause": repair.cause,
                    "repair_action": action,
                    "dry_run": True,
                }
            )
            continue

        refreshed = 0
        if refresh_paths:
            if repair_client is None:
                raise RuntimeError("raw refresh requires a CryptoHFT client")
            refreshed = _refresh_repair_raw_files(
                repair_client,
                raw_root,
                refresh_paths,
            )
            print(f"  atomically_refreshed_raw_files={refreshed}")

        prefetch_counts = {"downloaded": 0, "exists": 0, "404": 0}
        if download_missing:
            prefetch_counts = _prefetch_raw_hours(
                raw_root=raw_root,
                exchange=args.exchange,
                symbols=[repair.symbol],
                process_ranges=[(day_start, day_end)],
                warmup_hours=int(args.warmup_hours),
                api_key=args.api_key,
                jwt=args.jwt,
                transport=args.transport,
                workers=int(args.download_workers),
            )

        downloaded, reused, rows, sequence_audit = _process_symbol(
            client=repair_client if download_missing else None,
            raw_root=raw_root,
            target_roots=target_roots,
            symbol=repair.symbol,
            exchange=args.exchange,
            start_dt=day_start,
            end_dt=day_end,
            levels=args.levels,
            snapshot_ms=args.snapshot_ms,
            warmup_hours=args.warmup_hours,
            min_coverage=args.min_day_coverage,
            freshness_ms=freshness_ms,
            force_rebuild=True,
            timestamp_source=args.timestamp_source,
            sequence_bootstrap=args.sequence_bootstrap,
            delta_convergence_ms=args.delta_convergence_ms,
            allowed_days={repair.date},
            download_missing=download_missing,
            verbose=args.verbose,
        )
        results.append(
            {
                "symbol": repair.symbol,
                "range_index": index,
                "range_start_utc": day_start.isoformat(),
                "range_end_utc": day_end.isoformat(),
                "repair_date": repair.date,
                "repair_cause": repair.cause,
                "repair_suggested_fix": repair.suggested_fix,
                "repair_action": action,
                "refresh_scope": refresh_scope,
                "refreshed_raw_files": refreshed,
                "raw_prefetch": prefetch_counts,
                "downloaded": downloaded,
                "reused": reused,
                "rows": rows,
                "sequence_audit": sequence_audit,
            }
        )
        day_audits = sequence_audit.get("day_sequence_audits", {})
        day_sequence = (
            day_audits.get(repair.date, sequence_audit)
            if isinstance(day_audits, dict)
            else sequence_audit
        )
        audit_frames.append(
            _audit_days(
                raw_root,
                target_roots,
                args.exchange,
                [repair.symbol],
                day_start,
                day_end,
                args.min_day_coverage,
                freshness_ms,
                None,
                args.audit_raw_timestamps,
                {repair.date},
                {repair.date: day_sequence},
                args.levels,
            )
        )

    if args.dry_run:
        print("Dry run complete; no raw or normalized files were changed")
        return results

    if args.sequence_audit_json:
        audit_path = Path(args.sequence_audit_json).expanduser().resolve()
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(
            json.dumps(
                {
                    "schema_version": "cryptohft_sequence_audit.v1",
                    "mode": "repair_bad_days",
                    "repair_audit_csv": str(repair_csv),
                    "exchange": args.exchange,
                    "levels": int(args.levels),
                    "snapshot_ms": int(args.snapshot_ms),
                    "timestamp_source": args.timestamp_source,
                    "sequence_bootstrap": args.sequence_bootstrap,
                    "delta_convergence_ms": int(args.delta_convergence_ms),
                    "independent_retained_days": True,
                    "range_audits": results,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        print(f"Sequence audit JSON: {audit_path}")
    audit = (
        pd.concat(audit_frames, ignore_index=True)
        if audit_frames
        else pd.DataFrame()
    )
    if args.audit_csv:
        audit_path = Path(args.audit_csv).expanduser().resolve()
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit.to_csv(audit_path, index=False)
        print(f"Audit CSV saved -> {audit_path}")
    eligible = int(audit["eligible"].astype(bool).sum()) if not audit.empty else 0
    print(
        f"Repair complete: tasks={len(results)} "
        f"eligible_outputs={eligible}/{len(audit)}"
    )
    if audit.empty or eligible != len(audit):
        raise SystemExit(
            f"Repair post-audit failed: eligible_outputs={eligible}/{len(audit)}"
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download CryptoHFTData orderbook files and normalize them into daily bbo/l2 parquet files"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_SYMBOLS),
        help=f"Symbols to download (default: {' '.join(DEFAULT_SYMBOLS)})",
    )
    parser.add_argument(
        "--start",
        type=lambda value: _parse_datetime_arg(value, end=False),
        default=MIN_AVAILABLE_UTC,
        help="Start date/hour (YYYY-MM-DD or YYYY-MM-DDTHH). Default: 2025-08-01",
    )
    parser.add_argument(
        "--end",
        type=lambda value: _parse_datetime_arg(value, end=True),
        default=_floor_hour(datetime.now(timezone.utc) - timedelta(hours=1)),
        help="End date/hour inclusive (YYYY-MM-DD or YYYY-MM-DDTHH). Default: last complete UTC hour",
    )
    parser.add_argument(
        "--levels",
        type=int,
        default=DEFAULT_LEVELS,
        help=f"Number of L2 levels to store (default {DEFAULT_LEVELS}, matching live partial depth)",
    )
    parser.add_argument(
        "--snapshot-ms",
        type=int,
        default=DEFAULT_SNAPSHOT_MS,
        help=(
            "Snapshot interval in ms for normalized bbo/l2 output "
            f"(default {DEFAULT_SNAPSHOT_MS}, matching live partial depth)"
        ),
    )
    parser.add_argument(
        "--timestamp-source",
        choices=("event", "transaction", "received"),
        default=DEFAULT_TIMESTAMP_SOURCE,
        help=(
            "Clock used for normalized visibility. Use transaction to match "
            f"live partial-depth T timestamps (default: {DEFAULT_TIMESTAMP_SOURCE})."
        ),
    )
    parser.add_argument(
        "--sequence-bootstrap",
        choices=("snapshot", "delta-converged"),
        default=DEFAULT_SEQUENCE_BOOTSTRAP,
        help=(
            "Book initialization policy. 'snapshot' requires a native source "
            "snapshot. 'delta-converged' anchors the first contiguous update "
            "at pu, starts from an empty book, and emits only after "
            "--delta-convergence-ms (default: snapshot)."
        ),
    )
    parser.add_argument(
        "--delta-convergence-ms",
        type=int,
        default=DEFAULT_DELTA_CONVERGENCE_MS,
        help=(
            "Burn-in before emitting a delta-bootstrapped book "
            f"(default {DEFAULT_DELTA_CONVERGENCE_MS}ms). Ignored after a "
            "native source snapshot."
        ),
    )
    parser.add_argument(
        "--warmup-hours",
        type=int,
        default=DEFAULT_WARMUP_HOURS,
        help=(
            "Extra hours to preload before --start so a native snapshot from "
            f"the preceding UTC day can seed the book (default "
            f"{DEFAULT_WARMUP_HOURS})"
        ),
    )
    parser.add_argument(
        "--min-day-coverage",
        type=float,
        default=DEFAULT_MIN_DAY_COVERAGE,
        help="Minimum daily fresh normalized BBO/L2 coverage to treat a day as complete (default 0.90)",
    )
    parser.add_argument(
        "--coverage-freshness-s",
        type=float,
        default=DEFAULT_COVERAGE_FRESHNESS_S,
        help="Seconds each normalized snapshot counts as fresh during coverage audit (default 5.0)",
    )
    parser.add_argument("--force-rebuild", action="store_true", help="Rewrite normalized BBO/L2 outputs from --start even if they look complete")
    parser.add_argument(
        "--repair-audit-csv",
        type=Path,
        default=None,
        help=(
            "Repair only symbol/day rows selected from a classified bad-day "
            "CSV. Each day is rebuilt independently with its own warmup."
        ),
    )
    parser.add_argument(
        "--repair-causes",
        nargs="+",
        default=None,
        help="Optional cause filter used with --repair-audit-csv",
    )
    parser.add_argument(
        "--include-nonfixable",
        action="store_true",
        help=(
            "Include bad-day rows marked redownload_can_fix=false. Disabled "
            "by default because source-incomplete days normally remain bad."
        ),
    )
    parser.add_argument(
        "--repair-refresh-raw",
        choices=("none", "listed", "day"),
        default="none",
        help=(
            "Raw refresh override in repair mode: none follows suggested_fix; "
            "listed refreshes only missing_raw_hours; day refreshes all 24 "
            "target-day hours."
        ),
    )
    parser.add_argument(
        "--repair-limit",
        type=int,
        default=None,
        help="Optional maximum number of selected bad-day rows",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print a repair plan without deleting, downloading, or rebuilding files",
    )
    parser.add_argument("--audit-only", action="store_true", help="Only report raw-hour and normalized BBO/L2 coverage; do not download")
    parser.add_argument(
        "--audit-raw-timestamps",
        action="store_true",
        help="During --audit-only, decompress raw hourly files and calculate source timestamp coverage",
    )
    parser.add_argument("--audit-csv", default=None, help="Optional path to write audit CSV")
    parser.add_argument(
        "--eligible-manifest",
        default=None,
        help=(
            "Optional UTC-day CSV written from audit rows that pass coverage, "
            "schema, spread, and per-day sequence gates. Requires an existing "
            "--sequence-audit-json generated by a per-day rebuild."
        ),
    )
    parser.add_argument(
        "--sequence-audit-json",
        default=None,
        help="Optional JSON output for strict snapshot/delta sequence counters",
    )
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE, help="Exchange dataset name (default binance_futures)")
    parser.add_argument("--api-key", default=os.environ.get(API_KEY_ENV), help=f"CryptoHFTData API key (or set {API_KEY_ENV})")
    parser.add_argument("--jwt", default=os.environ.get(JWT_ENV), help=f"Pre-minted download JWT (or set {JWT_ENV})")
    parser.add_argument(
        "--transport",
        choices=["auto", "sdk", "rest"],
        default=DEFAULT_DOWNLOAD_TRANSPORT,
        help="Download transport: auto prefers the official cryptohftdata SDK when installed",
    )
    parser.add_argument(
        "--raw-root",
        default=str(_default_raw_root()),
        help="Root directory for raw hourly .parquet.zst files (default: external market-data root / cryptohftdata)",
    )
    parser.add_argument(
        "--retained-manifest",
        type=Path,
        default=None,
        help=(
            "Optional CSV with a UTC day column. Normalization still replays "
            "intervening raw hours for sequence continuity, but writes only "
            "the selected days."
        ),
    )
    parser.add_argument(
        "--independent-retained-days",
        action="store_true",
        help=(
            "Replay every retained UTC day as an independent task with its "
            "own --warmup-hours prefix. This is the formal mode for per-day "
            "sequence audits and prevents continuity from crossing omitted "
            "days."
        ),
    )
    parser.add_argument(
        "--retained-range-max-days",
        type=int,
        default=0,
        help=(
            "Optionally split contiguous retained ranges into bounded UTC-day "
            "chunks for balanced multiprocessing. Each chunk receives its own "
            "warmup prefix; 0 keeps whole contiguous ranges."
        ),
    )
    parser.add_argument(
        "--reuse-raw-only",
        action="store_true",
        help=(
            "Never download or replace raw source files. Missing/corrupt hours "
            "invalidate continuity until a later source snapshot."
        ),
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=4,
        help="Concurrent workers used to prefetch missing raw hours (default 4)",
    )
    parser.add_argument(
        "--process-workers",
        type=int,
        default=1,
        help=(
            "Parallel retained-range reconstruction workers. Ranges write "
            "disjoint UTC-day files (default 1)."
        ),
    )
    parser.add_argument(
        "--target-root",
        action="append",
        default=None,
        help=(
            "Normalized output root. Repeat for multiple roots; omitted uses "
            "the versioned retained-100ms staging root and never the retired "
            "top-level bbo/l2 directories."
        ),
    )
    parser.add_argument("--verbose", action="store_true",
                        help="Print one line per raw hourly file")
    args = parser.parse_args()

    if args.levels <= 0:
        raise SystemExit("--levels must be > 0")
    if args.snapshot_ms <= 0:
        raise SystemExit("--snapshot-ms must be > 0")
    if args.warmup_hours < 0:
        raise SystemExit("--warmup-hours must be >= 0")
    if args.delta_convergence_ms < 0:
        raise SystemExit("--delta-convergence-ms must be >= 0")
    if args.download_workers <= 0:
        raise SystemExit("--download-workers must be > 0")
    if args.process_workers <= 0:
        raise SystemExit("--process-workers must be > 0")
    if args.retained_range_max_days < 0:
        raise SystemExit("--retained-range-max-days must be >= 0")
    if args.repair_limit is not None and args.repair_limit < 0:
        raise SystemExit("--repair-limit must be >= 0")
    if not 0.0 <= args.min_day_coverage <= 1.0:
        raise SystemExit("--min-day-coverage must be between 0 and 1")
    if args.coverage_freshness_s <= 0:
        raise SystemExit("--coverage-freshness-s must be > 0")
    repair_only_option_used = bool(
        args.repair_causes
        or args.include_nonfixable
        or args.repair_refresh_raw != "none"
        or args.repair_limit is not None
        or args.dry_run
    )
    if args.repair_audit_csv is None and repair_only_option_used:
        raise SystemExit(
            "repair filters, raw refresh, and --dry-run require "
            "--repair-audit-csv"
        )
    if args.repair_audit_csv is not None:
        incompatible = []
        if args.audit_only:
            incompatible.append("--audit-only")
        if args.retained_manifest is not None:
            incompatible.append("--retained-manifest")
        if args.eligible_manifest:
            incompatible.append("--eligible-manifest")
        if args.reuse_raw_only:
            incompatible.append("--reuse-raw-only")
        if incompatible:
            raise SystemExit(
                "--repair-audit-csv cannot be combined with "
                + ", ".join(incompatible)
            )

    start_dt = _floor_hour(args.start)
    end_dt = _floor_hour(args.end)
    retained_days: Optional[list[str]] = None
    if args.retained_manifest is not None:
        retained_path = args.retained_manifest.expanduser().resolve()
        retained_days = _load_retained_days(retained_path)
        retained_days = [
            day
            for day in retained_days
            if start_dt.date()
            <= datetime.strptime(day, "%Y-%m-%d").date()
            <= end_dt.date()
        ]
        if not retained_days:
            raise SystemExit("retained manifest has no days in the requested range")
        start_dt = datetime.strptime(
            retained_days[0], "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(
            retained_days[-1], "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc, hour=23)
    if end_dt < MIN_AVAILABLE_UTC:
        raise SystemExit("CryptoHFTData orderbook files are only available from 2025-08-01 onward")
    if start_dt < MIN_AVAILABLE_UTC:
        print(f"[WARN] requested start {start_dt:%Y-%m-%d %H:%M} < 2025-08-01; clamping to 2025-08-01")
        start_dt = MIN_AVAILABLE_UTC
    if end_dt < start_dt:
        raise SystemExit("--end must be >= --start")

    target_roots = (
        [Path(value).expanduser().resolve() for value in args.target_root]
        if args.target_root
        else _default_target_roots()
    )
    raw_root = Path(args.raw_root).expanduser().resolve()
    symbols = [normalize_symbol(symbol) for symbol in args.symbols]
    freshness_ms = int(args.coverage_freshness_s * 1000.0)

    print(f"Raw orderbook root: {raw_root}")
    print("Mirrored normalized roots:")
    for root in target_roots:
        print(f"  {root}")
    print(f"Range: {start_dt:%Y-%m-%d %H:%M} -> {end_dt:%Y-%m-%d %H:%M} UTC")
    print(f"Snapshot interval: {args.snapshot_ms} ms  Levels: {args.levels}\n")
    print(f"Timestamp source: {args.timestamp_source}\n")
    print(
        f"Sequence bootstrap: {args.sequence_bootstrap} "
        f"(delta convergence {args.delta_convergence_ms}ms)\n"
    )

    if args.repair_audit_csv is not None:
        _run_bad_day_repairs(
            args=args,
            raw_root=raw_root,
            target_roots=target_roots,
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            freshness_ms=freshness_ms,
        )
        return

    if args.audit_only:
        print(f"Coverage threshold: {args.min_day_coverage:.1%} fresh<= {args.coverage_freshness_s:.3f}s")
        sequence_day_audits = None
        sequence_payload = None
        if args.sequence_audit_json:
            sequence_path = Path(
                args.sequence_audit_json
            ).expanduser().resolve()
            if sequence_path.exists():
                sequence_payload, sequence_day_audits = (
                    _load_per_day_sequence_audits(sequence_path)
                )
            elif args.eligible_manifest:
                raise SystemExit(
                    f"sequence audit JSON does not exist: {sequence_path}"
                )
        audit_frame = _audit_days(
            raw_root,
            target_roots,
            args.exchange,
            symbols,
            start_dt,
            end_dt,
            args.min_day_coverage,
            freshness_ms,
            args.audit_csv,
            args.audit_raw_timestamps,
            set(retained_days) if retained_days is not None else None,
            sequence_day_audits,
            args.levels,
        )
        if args.eligible_manifest:
            if sequence_payload is None or sequence_day_audits is None:
                raise SystemExit(
                    "--eligible-manifest requires an existing "
                    "--sequence-audit-json"
                )
            eligible_path = Path(
                args.eligible_manifest
            ).expanduser().resolve()
            eligible_path.parent.mkdir(parents=True, exist_ok=True)
            eligible = (
                audit_frame.loc[
                    audit_frame["eligible"].astype(bool),
                    ["date"],
                ]
                .drop_duplicates()
                .sort_values("date")
                .rename(columns={"date": "day"})
            )
            eligible.to_csv(eligible_path, index=False)
            print(
                f"Eligible manifest saved -> {eligible_path} "
                f"({len(eligible)}/{len(audit_frame)} days)"
            )
        return

    process_ranges = (
        _retained_process_ranges(
            retained_days,
            independent_days=bool(args.independent_retained_days),
            sequence_bootstrap=str(args.sequence_bootstrap),
            max_days=int(args.retained_range_max_days),
        )
        if retained_days is not None
        else [(start_dt, end_dt)]
    )
    prefetch_counts = {"downloaded": 0, "exists": 0, "404": 0}
    if not args.reuse_raw_only:
        prefetch_counts = _prefetch_raw_hours(
            raw_root=raw_root,
            exchange=args.exchange,
            symbols=symbols,
            process_ranges=process_ranges,
            warmup_hours=int(args.warmup_hours),
            api_key=args.api_key,
            jwt=args.jwt,
            transport=args.transport,
            workers=int(args.download_workers),
        )

    client: Optional[CryptoHFTClient]
    if args.reuse_raw_only:
        client = None
        print("Download transport: disabled (reuse-raw-only)")
    else:
        client = CryptoHFTClient(
            api_key=args.api_key,
            jwt=args.jwt,
            transport=args.transport,
        )
        print(f"Download transport: {client.transport}")

    total_downloaded = 0
    total_reused = 0
    total_rows = 0
    sequence_audits: dict[str, dict[str, object]] = {}
    if retained_days is not None:
        print(
            f"Retained output: {len(retained_days)} UTC days in "
            f"{len(process_ranges)} contiguous ranges"
        )
    results: list[dict[str, object]] = []
    if int(args.process_workers) > 1 and len(process_ranges) > 1:
        payloads = []
        for symbol in symbols:
            for range_index, (range_start, range_end) in enumerate(
                process_ranges,
                start=1,
            ):
                payloads.append(
                    {
                        "raw_root": str(raw_root),
                        "target_roots": [str(path) for path in target_roots],
                        "symbol": symbol,
                        "exchange": args.exchange,
                        "start_dt": range_start,
                        "end_dt": range_end,
                        "levels": int(args.levels),
                        "snapshot_ms": int(args.snapshot_ms),
                        "warmup_hours": int(args.warmup_hours),
                        "min_coverage": float(args.min_day_coverage),
                        "freshness_ms": int(freshness_ms),
                        "force_rebuild": bool(args.force_rebuild),
                        "timestamp_source": args.timestamp_source,
                        "sequence_bootstrap": args.sequence_bootstrap,
                        "delta_convergence_ms": int(args.delta_convergence_ms),
                        "allowed_days": (
                            [range_start.strftime("%Y-%m-%d")]
                            if args.independent_retained_days
                            else retained_days or []
                        ),
                        "verbose": bool(args.verbose),
                        "range_index": range_index,
                    }
                )
        workers = min(int(args.process_workers), len(payloads))
        print(
            f"Reconstructing {len(payloads)} symbol/range tasks with "
            f"{workers} processes"
        )
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers
        ) as executor:
            futures = [
                executor.submit(_process_symbol_task, payload)
                for payload in payloads
            ]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)
                print(
                    f"  Complete {result['symbol']} range "
                    f"{result['range_index']}/{len(process_ranges)}: "
                    f"rows={result['rows']}"
                )
    else:
        for symbol in symbols:
            print(f"=== {symbol} ===")
            for range_index, (range_start, range_end) in enumerate(
                process_ranges,
                start=1,
            ):
                if len(process_ranges) > 1:
                    print(
                        f"  Range {range_index}/{len(process_ranges)}: "
                        f"{range_start:%Y-%m-%d} -> {range_end:%Y-%m-%d}"
                    )
                downloaded, reused, rows, sequence_audit = _process_symbol(
                    client=client,
                    raw_root=raw_root,
                    target_roots=target_roots,
                    symbol=symbol,
                    exchange=args.exchange,
                    start_dt=range_start,
                    end_dt=range_end,
                    levels=args.levels,
                    snapshot_ms=args.snapshot_ms,
                    warmup_hours=args.warmup_hours,
                    min_coverage=args.min_day_coverage,
                    freshness_ms=freshness_ms,
                    force_rebuild=args.force_rebuild,
                    timestamp_source=args.timestamp_source,
                    sequence_bootstrap=args.sequence_bootstrap,
                    delta_convergence_ms=args.delta_convergence_ms,
                    allowed_days=(
                        {range_start.strftime("%Y-%m-%d")}
                        if args.independent_retained_days
                        else (
                            set(retained_days)
                            if retained_days is not None
                            else None
                        )
                    ),
                    download_missing=not args.reuse_raw_only,
                    verbose=args.verbose,
                )
                results.append(
                    {
                        "symbol": symbol,
                        "range_index": range_index,
                        "range_start_utc": range_start.isoformat(),
                        "range_end_utc": range_end.isoformat(),
                        "downloaded": downloaded,
                        "reused": reused,
                        "rows": rows,
                        "sequence_audit": sequence_audit,
                    }
                )

    for symbol in symbols:
        symbol_results = [
            result for result in results if result["symbol"] == symbol
        ]
        symbol_audit: dict[str, int] = {}
        symbol_downloaded = sum(
            int(result["downloaded"]) for result in symbol_results
        )
        symbol_reused = sum(
            int(result["reused"]) for result in symbol_results
        )
        symbol_rows = sum(int(result["rows"]) for result in symbol_results)
        for result in symbol_results:
            for key, value in result["sequence_audit"].items():
                if not isinstance(value, (bool, int, float)):
                    continue
                symbol_audit[key] = symbol_audit.get(key, 0) + int(value)
        total_downloaded += symbol_downloaded
        total_reused += symbol_reused
        total_rows += symbol_rows
        sequence_audits[symbol] = symbol_audit
        print(
            f"{symbol}: downloaded={symbol_downloaded} "
            f"reused={symbol_reused} normalized_rows={symbol_rows}\n"
        )

    if args.sequence_audit_json:
        audit_path = Path(args.sequence_audit_json).expanduser().resolve()
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(
            json.dumps(
                {
                    "schema_version": "cryptohft_sequence_audit.v1",
                    "exchange": args.exchange,
                    "range_start_utc": start_dt.isoformat(),
                    "range_end_utc": end_dt.isoformat(),
                    "levels": int(args.levels),
                    "snapshot_ms": int(args.snapshot_ms),
                    "timestamp_source": args.timestamp_source,
                    "sequence_bootstrap": args.sequence_bootstrap,
                    "delta_convergence_ms": int(args.delta_convergence_ms),
                    "retained_manifest": (
                        str(args.retained_manifest.expanduser().resolve())
                        if args.retained_manifest is not None
                        else ""
                    ),
                    "retained_days": retained_days or [],
                    "independent_retained_days": bool(
                        args.independent_retained_days
                    ),
                    "retained_range_max_days": int(
                        args.retained_range_max_days
                    ),
                    "reuse_raw_only": bool(args.reuse_raw_only),
                    "raw_prefetch": prefetch_counts,
                    "process_workers": int(args.process_workers),
                    "symbols": sequence_audits,
                    "range_audits": sorted(
                        results,
                        key=lambda item: (
                            str(item["symbol"]),
                            int(item["range_index"]),
                        ),
                    ),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        print(f"Sequence audit JSON: {audit_path}")

    print(
        f"Done. raw_prefetched={prefetch_counts.get('downloaded', 0)} "
        f"raw_downloaded={total_downloaded} raw_reused={total_reused} "
        f"normalized_rows={total_rows}"
    )


if __name__ == "__main__":
    main()
