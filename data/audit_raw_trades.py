"""Audit raw Binance futures trades and align them with BBO/L2/bars days.

Outputs are written under ``${NARROWGATE_DATA_ROOT}/reports/raw_trades_integrity`` by
default. This script is read-only with respect to market data.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root
from data_quality import excluded_orderbook_days
from market_fusion import normalize_symbol


DAY_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
CHUNK_SIZE = int(os.environ.get("MM_RAW_TRADES_AUDIT_CHUNK_SIZE", "3000000"))


@dataclass(frozen=True)
class RawTradeStats:
    symbol: str
    day: str
    path: Path
    file_size_bytes: int
    row_count: int
    first_trade_id: int | None
    last_trade_id: int | None
    first_time_ms: int | None
    last_time_ms: int | None
    min_time_ms: int | None
    max_time_ms: int | None
    repeated_adjacent_id_count: int
    non_monotonic_id_count: int
    buyer_maker_true_count: int
    buyer_maker_false_count: int
    buyer_maker_invalid_count: int

    @property
    def empty(self) -> bool:
        return self.row_count == 0

    @property
    def side_complete(self) -> bool:
        return (
            self.buyer_maker_true_count > 0
            and self.buyer_maker_false_count > 0
            and self.buyer_maker_invalid_count == 0
        )


def day_tag_from_path(path: Path) -> str | None:
    match = DAY_RE.search(path.name)
    return match.group(0) if match else None


def normalize_time_ms_array(values: np.ndarray) -> np.ndarray:
    timestamps = values.astype(np.int64, copy=True)
    abs_timestamps = np.abs(timestamps)
    ns_mask = abs_timestamps >= 100_000_000_000_000_000
    us_mask = (abs_timestamps >= 100_000_000_000_000) & ~ns_mask
    timestamps[ns_mask] //= 1_000_000
    timestamps[us_mask] //= 1_000
    return timestamps


def buyer_maker_counts(values: pd.Series) -> tuple[int, int, int]:
    if pd.api.types.is_bool_dtype(values):
        true_count = int(values.sum())
        return true_count, int(len(values) - true_count), 0
    normalized = values.astype(str).str.strip().str.lower()
    true_mask = normalized.isin({"true", "1", "t", "yes"})
    false_mask = normalized.isin({"false", "0", "f", "no"})
    return (
        int(true_mask.sum()),
        int(false_mask.sum()),
        int((~true_mask & ~false_mask).sum()),
    )


def utc_iso_from_ms(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def scan_raw_trade_file(csv_path: Path, symbol: str, *, chunk_size: int) -> RawTradeStats:
    day = day_tag_from_path(csv_path)
    if day is None:
        raise ValueError(f"Cannot parse day from {csv_path.name}")

    row_count = 0
    first_trade_id = last_trade_id = None
    first_time_ms = last_time_ms = None
    min_time_ms = max_time_ms = None
    repeated_adjacent_id_count = 0
    non_monotonic_id_count = 0
    buyer_maker_true_count = 0
    buyer_maker_false_count = 0
    buyer_maker_invalid_count = 0
    previous_id = None

    try:
        reader = pd.read_csv(
            csv_path,
            usecols=["id", "time", "is_buyer_maker"],
            dtype={"id": np.int64, "time": np.int64},
            chunksize=chunk_size,
        )
    except pd.errors.EmptyDataError:
        reader = []

    for chunk in reader:
        if chunk.empty:
            continue

        ids = chunk["id"].to_numpy(dtype=np.int64)
        times = chunk["time"].to_numpy(dtype=np.int64)
        normalized_times = normalize_time_ms_array(times)
        true_count, false_count, invalid_count = buyer_maker_counts(
            chunk["is_buyer_maker"]
        )
        buyer_maker_true_count += true_count
        buyer_maker_false_count += false_count
        buyer_maker_invalid_count += invalid_count

        if row_count == 0:
            first_trade_id = int(ids[0])
            first_time_ms = int(normalized_times[0])

        if previous_id is not None:
            repeated_adjacent_id_count += int(ids[0] == previous_id)
            non_monotonic_id_count += int(ids[0] <= previous_id)

        if len(ids) > 1:
            repeated_adjacent_id_count += int(np.sum(ids[1:] == ids[:-1]))
            non_monotonic_id_count += int(np.sum(ids[1:] <= ids[:-1]))

        row_count += len(chunk)
        previous_id = int(ids[-1])
        last_trade_id = int(ids[-1])
        last_time_ms = int(normalized_times[-1])
        chunk_min_time = int(np.min(normalized_times))
        chunk_max_time = int(np.max(normalized_times))
        min_time_ms = chunk_min_time if min_time_ms is None else min(min_time_ms, chunk_min_time)
        max_time_ms = chunk_max_time if max_time_ms is None else max(max_time_ms, chunk_max_time)

    return RawTradeStats(
        symbol=symbol,
        day=day,
        path=csv_path,
        file_size_bytes=csv_path.stat().st_size,
        row_count=row_count,
        first_trade_id=first_trade_id,
        last_trade_id=last_trade_id,
        first_time_ms=first_time_ms,
        last_time_ms=last_time_ms,
        min_time_ms=min_time_ms,
        max_time_ms=max_time_ms,
        repeated_adjacent_id_count=repeated_adjacent_id_count,
        non_monotonic_id_count=non_monotonic_id_count,
        buyer_maker_true_count=buyer_maker_true_count,
        buyer_maker_false_count=buyer_maker_false_count,
        buyer_maker_invalid_count=buyer_maker_invalid_count,
    )


def available_daily_file_days(directory: Path, symbol: str, token: str) -> set[str]:
    days = set()
    for path in directory.glob(f"{symbol}-{token}-*.parquet"):
        day = day_tag_from_path(path)
        if day:
            days.add(day)
    return days


def available_bar_days(directory: Path, symbol: str) -> set[str]:
    days = set()
    for path in sorted(directory.glob(f"{symbol}-1s-*.parquet")):
        # 只承认日度容器文件名。旧的 YYYY-MM 月度 bars 即使 index 里有日期，也不能
        # 在 audit 里被展开成“可用日”，否则会掩盖日度容器缺失。
        day = day_tag_from_path(path)
        if day:
            days.add(day)
    return days


def consecutive_days(start_day: str, end_day: str) -> list[str]:
    current = datetime.strptime(start_day, "%Y-%m-%d")
    end = datetime.strptime(end_day, "%Y-%m-%d")
    days = []
    while current <= end:
        days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def raw_stats_to_frame(stats: list[RawTradeStats], duplicate_days: set[str]) -> pd.DataFrame:
    rows = []
    for item in stats:
        rows.append(
            {
                "symbol": item.symbol,
                "day": item.day,
                "file": item.path.name,
                "path": str(item.path),
                "file_size_bytes": item.file_size_bytes,
                "row_count": item.row_count,
                "first_trade_id": item.first_trade_id,
                "last_trade_id": item.last_trade_id,
                "first_time_ms": item.first_time_ms,
                "last_time_ms": item.last_time_ms,
                "min_time_ms": item.min_time_ms,
                "max_time_ms": item.max_time_ms,
                "first_time_utc": utc_iso_from_ms(item.first_time_ms),
                "last_time_utc": utc_iso_from_ms(item.last_time_ms),
                "min_time_utc": utc_iso_from_ms(item.min_time_ms),
                "max_time_utc": utc_iso_from_ms(item.max_time_ms),
                "empty": item.empty,
                "duplicate_file_for_day": item.day in duplicate_days,
                "repeated_adjacent_id_count": item.repeated_adjacent_id_count,
                "non_monotonic_id_count": item.non_monotonic_id_count,
                "buyer_maker_true_count": item.buyer_maker_true_count,
                "buyer_maker_false_count": item.buyer_maker_false_count,
                "buyer_maker_invalid_count": item.buyer_maker_invalid_count,
                "side_complete": item.side_complete,
            }
        )
    return pd.DataFrame(rows)


def audit_symbol(symbol: str, data_dir: Path, out_dir: Path, *, chunk_size: int, file_filter: str | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_dir = data_dir / "raw_trades" / symbol
    raw_files = sorted(raw_dir.glob(f"{symbol}-trades-*.csv"))
    if file_filter:
        raw_files = [path for path in raw_files if file_filter in path.name]
    if not raw_files:
        raise FileNotFoundError(f"No raw trades found for {symbol} in {raw_dir}")

    day_counts: dict[str, int] = {}
    for path in raw_files:
        day = day_tag_from_path(path)
        if day:
            day_counts[day] = day_counts.get(day, 0) + 1
    duplicate_days = {day for day, count in day_counts.items() if count > 1}

    print(f"{symbol}: scanning {len(raw_files)} raw trade files", flush=True)
    stats = []
    for index, path in enumerate(raw_files, start=1):
        stats.append(scan_raw_trade_file(path, symbol, chunk_size=chunk_size))
        if index == len(raw_files) or index % 25 == 0:
            print(f"  {symbol}: scanned {index}/{len(raw_files)} files", flush=True)
    raw_frame = raw_stats_to_frame(stats, duplicate_days)

    book_root = (
        data_dir / "normalized_l2_100ms_v2"
        if normalize_symbol(symbol) == "BTCUSDC"
        else data_dir
    )
    bbo_days = available_daily_file_days(book_root / "bbo", symbol, "bbo")
    l2_days = available_daily_file_days(book_root / "l2", symbol, "l2")
    bar_days = available_bar_days(data_dir / "bars_1s", symbol)
    excluded_days = excluded_orderbook_days(symbol)
    raw_days = set(raw_frame.loc[~raw_frame["empty"], "day"])
    expected_days = consecutive_days(min(raw_days), max(raw_days)) if raw_days else []

    eligible_rows = []
    for day in expected_days:
        day_rows = raw_frame[raw_frame["day"] == day]
        raw_ok = bool(
            len(day_rows) == 1
            and not bool(day_rows.iloc[0]["empty"])
            and int(day_rows.iloc[0]["non_monotonic_id_count"]) == 0
            and bool(day_rows.iloc[0]["side_complete"])
        )
        has_bbo = day in bbo_days
        has_l2 = day in l2_days
        has_bars = day in bar_days
        orderbook_excluded = day in excluded_days
        eligible = raw_ok and has_bbo and has_l2 and has_bars and not orderbook_excluded
        reasons = []
        if not raw_ok:
            reasons.append("raw_missing_empty_duplicate_or_nonmonotonic")
        if len(day_rows) == 1 and not bool(day_rows.iloc[0]["side_complete"]):
            reasons.append("raw_trade_side_incomplete_or_invalid")
        if not has_bbo:
            reasons.append("missing_bbo")
        if not has_l2:
            reasons.append("missing_l2")
        if not has_bars:
            reasons.append("missing_bars")
        if orderbook_excluded:
            reasons.append("orderbook_quality_excluded")

        eligible_rows.append(
            {
                "symbol": symbol,
                "day": day,
                "raw_ok": raw_ok,
                "has_bbo": has_bbo,
                "has_l2": has_l2,
                "has_bars": has_bars,
                "orderbook_excluded": orderbook_excluded,
                "eligible": eligible,
                "exclude_reason": ";".join(reasons),
            }
        )

    eligible_frame = pd.DataFrame(eligible_rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_frame.to_csv(out_dir / f"{symbol}-raw-trades-audit.csv", index=False)
    eligible_frame.to_csv(out_dir / f"{symbol}-eligible-days.csv", index=False)
    return raw_frame, eligible_frame


def main() -> None:
    default_symbols = [normalize_symbol(os.environ.get("MM_SYMBOL"), "BTCUSDC"), "BTCUSDT"]
    parser = argparse.ArgumentParser(description="Audit raw trades coverage and BBO/L2/bars alignment")
    parser.add_argument("--symbols", nargs="+", default=default_symbols, help="Symbols to audit")
    parser.add_argument("--data-dir", type=Path, default=None, help="Override data root")
    parser.add_argument("--out-dir", type=Path, default=None, help="Override report output directory")
    parser.add_argument("--file", default=None, help="Only audit files whose name contains this string")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="CSV rows per read chunk")
    args = parser.parse_args()

    data_dir = args.data_dir.expanduser().resolve() if args.data_dir else data_root(ROOT)
    out_dir = args.out_dir.expanduser().resolve() if args.out_dir else data_dir / "reports" / "raw_trades_integrity"
    symbols = sorted({normalize_symbol(symbol) for symbol in args.symbols})

    print(f"Data root: {data_dir}")
    print(f"Reports: {out_dir}")
    for symbol in symbols:
        raw_frame, eligible_frame = audit_symbol(
            symbol,
            data_dir,
            out_dir,
            chunk_size=args.chunk_size,
            file_filter=args.file,
        )
        eligible_count = int(eligible_frame["eligible"].sum()) if not eligible_frame.empty else 0
        print(
            f"{symbol}: raw_files={len(raw_frame)} rows={int(raw_frame['row_count'].sum()):,} "
            f"empty={int(raw_frame['empty'].sum())} duplicate_days={int(raw_frame['duplicate_file_for_day'].sum())} "
            f"eligible_days={eligible_count}/{len(eligible_frame)}"
        )


if __name__ == "__main__":
    main()
