"""
Step 2d: 将 raw metrics CSV 转为日度 parquet。

输入: data/raw_metrics/BTCUSDT-metrics-YYYY-MM-DD.csv  (5分钟间隔)
输出: data/metrics_5m/BTCUSDT-metrics-YYYY-MM-DD.parquet

字段:
  create_time, sum_open_interest, sum_open_interest_value,
  count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
  count_long_short_ratio, sum_taker_long_short_vol_ratio

用法:
    python features/preprocess_metrics.py
    python features/preprocess_metrics.py --file 2025-03-01
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402

DATA_DIR = data_root(ROOT)
RAW_DIR = DATA_DIR / "raw_metrics"
OUT_DIR = DATA_DIR / "metrics_5m"
DEFAULT_SYMBOL = os.environ.get("MM_SYMBOL", "BTCUSDC").upper()

KEEP_COLS = [
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
]


def normalize_feature_ready_time(
    frame: pd.DataFrame,
    *,
    day: str,
) -> pd.DataFrame:
    """Normalize Binance's two daily-metrics timestamp conventions.

    Older archives timestamp each 5-minute observation at interval end
    (00:05 through next-day 00:00). Some corrected/newer archives timestamp
    the same observations at interval start (00:00 through 23:55). Metrics
    that summarize a period are not causal until that period completes, so
    start-stamped files are shifted forward by five minutes.
    """
    result = frame.copy()
    timestamps = pd.to_datetime(result["create_time"], utc=True, errors="coerce")
    result["create_time"] = timestamps
    result.dropna(subset=["create_time"], inplace=True)
    if result.empty:
        raise ValueError(f"{day}: metrics file has no valid create_time rows")

    valid_times = result["create_time"].sort_values()
    cadence = valid_times.diff().dropna()
    if (
        len(valid_times) != 288
        or valid_times.duplicated().any()
        or not cadence.between(
            pd.Timedelta(minutes=4, seconds=55),
            pd.Timedelta(minutes=5, seconds=5),
        ).all()
    ):
        raise ValueError(
            f"{day}: expected 288 unique metrics rows at nominal five-minute cadence"
        )

    day_start = pd.Timestamp(day, tz="UTC")
    start_stamped = (
        valid_times.min() == day_start
        and valid_times.max() == day_start + pd.Timedelta(hours=23, minutes=55)
    )
    end_stamped = (
        valid_times.min() == day_start + pd.Timedelta(minutes=5)
        and valid_times.max() == day_start + pd.Timedelta(days=1)
    )
    if start_stamped:
        result["create_time"] += pd.Timedelta(minutes=5)
    elif not end_stamped:
        raise ValueError(
            f"{day}: unrecognized metrics timestamp bounds "
            f"{valid_times.min()}..{valid_times.max()}"
        )
    return result


def main():
    parser = argparse.ArgumentParser(description="Preprocess metrics CSV to daily parquet")
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL,
                        help=f"Symbol (default {DEFAULT_SYMBOL}; MM_SYMBOL also supported)")
    parser.add_argument("--file", type=str, default=None)
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing daily parquet outputs")
    parser.add_argument("--verbose", action="store_true",
                        help="Print one line per daily output")
    parser.add_argument(
        "--skip-invalid",
        action="store_true",
        help=(
            "Continue a batch after a strict 288-row/cadence failure. "
            "Invalid days remain without parquet and must fail downstream "
            "training eligibility."
        ),
    )
    args = parser.parse_args()
    symbol = args.symbol.upper()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(RAW_DIR.glob(f"{symbol}-metrics-*.csv"))
    if not csv_files:
        print("No metrics CSV files found")
        sys.exit(1)

    if args.file:
        csv_files = [f for f in csv_files if args.file in f.name]

    ok = skip = rows_written = 0
    invalid: list[tuple[str, str]] = []
    for f in csv_files:
        # filename: BTCUSDT-metrics-2025-01-01.csv
        date_str = f.stem.replace(f"{symbol}-metrics-", "")
        if len(date_str) != 10:
            print(f"[SKIP] non-daily metrics CSV: {f.name}")
            skip += 1
            continue

        out_path = OUT_DIR / f"{symbol}-metrics-{date_str}.parquet"
        if out_path.exists() and not args.overwrite:
            skip += 1
            if args.verbose:
                print(f"[SKIP] {out_path.name}")
            continue

        if out_path.exists() and args.overwrite and args.verbose:
            print(f"[OVERWRITE] {out_path.name}")

        combined = pd.read_csv(f, parse_dates=["create_time"])
        combined = combined[["create_time"] + KEEP_COLS]
        try:
            combined = normalize_feature_ready_time(combined, day=date_str)
        except ValueError as exc:
            if not args.skip_invalid:
                raise
            invalid.append((date_str, str(exc)))
            print(f"[INVALID] {symbol} {date_str}: {exc}")
            continue
        combined.sort_values("create_time", inplace=True)
        combined.drop_duplicates(subset=["create_time"], keep="first", inplace=True)

        # 输出严格按日度文件分桶；后续 feature join 再负责和 bars/orderbook 的 UTC index 对齐。
        combined.set_index("create_time", inplace=True)

        combined.to_parquet(out_path, engine="pyarrow")
        size_kb = out_path.stat().st_size / 1024
        ok += 1
        rows_written += len(combined)
        if args.verbose:
            print(f"[OK]  {out_path.name}: {len(combined):,} rows, {size_kb:.0f} KB")

    parquet_files = sorted(OUT_DIR.glob(f"{symbol}-metrics-*.parquet"))
    print(
        f"\nDone: wrote {ok}, skipped {skip}, "
        f"invalid {len(invalid)}, rows this run {rows_written:,}, "
        f"total parquet {len(parquet_files)}"
    )
    if invalid:
        print(
            "Invalid days were not materialized: "
            + ", ".join(day for day, _ in invalid)
        )


if __name__ == "__main__":
    main()
