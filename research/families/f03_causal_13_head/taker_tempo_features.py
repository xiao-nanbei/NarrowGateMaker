"""Build taker-tempo features from Binance USD-M raw trades.

The output is a derived daily input to ``features.feature_engineer``. Missing
daily sidecars are filled with zeros by that builder, so formal feature
generation must select an explicit retained-day manifest and audit this
directory against it before training.

Input:
    ${NARROWGATE_DATA_ROOT}/raw_trades/<SYMBOL>/<SYMBOL>-trades-YYYY-MM-DD.csv

Output:
    ${NARROWGATE_DATA_ROOT}/trade_features/<SYMBOL>/<SYMBOL>-trade-tempo-YYYY-MM-DD.parquet
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root
from market_fusion import normalize_symbol


COLUMNS = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]
DTYPES = {
    "id": np.int64,
    "price": np.float64,
    "qty": np.float64,
    "quote_qty": np.float64,
    "time": np.int64,
    "is_buyer_maker": str,
}
CHUNK_SIZE = int(os.environ.get("MM_TAKER_TEMPO_CHUNK_SIZE", "3000000"))
ROLLING_WINDOWS = (1, 5, 10, 30, 60)
EVENT_BUCKET_NOTIONAL = float(os.environ.get("MM_TAKER_TEMPO_BUCKET_NOTIONAL", "5000000"))
VPIN_BUCKET_WINDOW = int(os.environ.get("MM_TAKER_TEMPO_VPIN_BUCKETS", "50"))
HAWKES_SHORT_HALFLIFE_S = float(os.environ.get("MM_TAKER_TEMPO_HAWKES_SHORT_HALFLIFE", "5"))
HAWKES_LONG_HALFLIFE_S = float(os.environ.get("MM_TAKER_TEMPO_HAWKES_LONG_HALFLIFE", "60"))

ZERO_ON_EMPTY_SECOND_COLS = [
    "volume",
    "quote_qty",
    "buy_qty",
    "sell_qty",
    "buy_quote_qty",
    "sell_quote_qty",
    "signed_qty",
    "signed_quote_qty",
    "trade_count",
    "buy_trade_count",
    "sell_trade_count",
    "max_same_side_run",
    "max_buy_run",
    "max_sell_run",
    "interarrival_ms_sum",
    "interarrival_ms_p90",
    "interarrival_ms_max",
]


@dataclass
class ChunkCarry:
    last_side: int | None = None
    last_run_len: int = 0
    last_time_ms: int | None = None


def floor_trade_time_to_second_ms(transact_time: pd.Series) -> pd.Series:
    trade_time = transact_time.astype(np.int64)
    seconds = trade_time // 1_000
    seconds = seconds.where(trade_time < 100_000_000_000_000, trade_time // 1_000_000)
    seconds = seconds.where(trade_time < 100_000_000_000_000_000, trade_time // 1_000_000_000)
    return seconds * 1000


def parse_bool_series(values: pd.Series) -> pd.Series:
    return values.astype(str).str.strip().str.lower().isin({"true", "1", "t"})


def run_lengths_with_carry(side: np.ndarray, carry: ChunkCarry) -> np.ndarray:
    previous_side = np.empty_like(side)
    previous_side[0] = carry.last_side if carry.last_side is not None else 0
    previous_side[1:] = side[:-1]

    starts = side != previous_side
    if carry.last_side is None:
        starts[0] = True

    run_groups = starts.cumsum()
    run_lengths = pd.Series(run_groups).groupby(run_groups, sort=False).cumcount().to_numpy() + 1
    if carry.last_side is not None and side[0] == carry.last_side:
        run_lengths[run_groups == 0] += carry.last_run_len
    return run_lengths.astype(np.int32)


def enrich_trades(chunk: pd.DataFrame, carry: ChunkCarry) -> pd.DataFrame:
    chunk = chunk.copy()
    is_buyer_maker = parse_bool_series(chunk["is_buyer_maker"])
    is_buy_taker = ~is_buyer_maker
    side = np.where(is_buy_taker.to_numpy(), 1, -1).astype(np.int8)
    trade_time_ms = chunk["time"].astype(np.int64).to_numpy()

    previous_time_ms = np.empty_like(trade_time_ms)
    previous_time_ms[0] = carry.last_time_ms if carry.last_time_ms is not None else trade_time_ms[0]
    previous_time_ms[1:] = trade_time_ms[:-1]
    interarrival_ms = np.maximum(trade_time_ms - previous_time_ms, 0)
    if carry.last_time_ms is None:
        interarrival_ms[0] = 0

    run_lengths = run_lengths_with_carry(side, carry)
    carry.last_side = int(side[-1])
    carry.last_run_len = int(run_lengths[-1])
    carry.last_time_ms = int(trade_time_ms[-1])

    chunk["ts_sec"] = floor_trade_time_to_second_ms(chunk["time"])
    chunk["side"] = side
    chunk["is_buy_taker"] = is_buy_taker
    chunk["interarrival_ms"] = interarrival_ms.astype(np.int64)
    chunk["same_side_run_len"] = run_lengths
    chunk["signed_qty"] = chunk["qty"] * chunk["side"]
    chunk["signed_quote_qty"] = chunk["quote_qty"] * chunk["side"]
    chunk["buy_qty"] = chunk["qty"].where(chunk["is_buy_taker"], 0.0)
    chunk["sell_qty"] = chunk["qty"].where(~chunk["is_buy_taker"], 0.0)
    chunk["buy_quote_qty"] = chunk["quote_qty"].where(chunk["is_buy_taker"], 0.0)
    chunk["sell_quote_qty"] = chunk["quote_qty"].where(~chunk["is_buy_taker"], 0.0)
    chunk["buy_price"] = chunk["price"].where(chunk["is_buy_taker"])
    chunk["sell_price"] = chunk["price"].where(~chunk["is_buy_taker"])
    chunk["buy_run_len"] = np.where(chunk["is_buy_taker"], chunk["same_side_run_len"], 0)
    chunk["sell_run_len"] = np.where(~chunk["is_buy_taker"], chunk["same_side_run_len"], 0)
    return chunk


def aggregate_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    grouped = chunk.groupby("ts_sec", sort=True)
    features = pd.DataFrame(
        {
            "open": grouped["price"].first(),
            "high": grouped["price"].max(),
            "low": grouped["price"].min(),
            "close": grouped["price"].last(),
            "volume": grouped["qty"].sum(),
            "quote_qty": grouped["quote_qty"].sum(),
            "buy_qty": grouped["buy_qty"].sum(),
            "sell_qty": grouped["sell_qty"].sum(),
            "buy_quote_qty": grouped["buy_quote_qty"].sum(),
            "sell_quote_qty": grouped["sell_quote_qty"].sum(),
            "signed_qty": grouped["signed_qty"].sum(),
            "signed_quote_qty": grouped["signed_quote_qty"].sum(),
            "trade_count": grouped["id"].count(),
            "buy_trade_count": grouped["is_buy_taker"].sum(),
            "sell_trade_count": grouped["is_buyer_maker"].count() - grouped["is_buy_taker"].sum(),
            "max_same_side_run": grouped["same_side_run_len"].max(),
            "max_buy_run": grouped["buy_run_len"].max(),
            "max_sell_run": grouped["sell_run_len"].max(),
            "interarrival_ms_sum": grouped["interarrival_ms"].sum(),
            "interarrival_ms_p90": grouped["interarrival_ms"].quantile(0.9),
            "interarrival_ms_max": grouped["interarrival_ms"].max(),
            "buy_price_high": grouped["buy_price"].max(),
            "buy_price_low": grouped["buy_price"].min(),
            "sell_price_high": grouped["sell_price"].max(),
            "sell_price_low": grouped["sell_price"].min(),
        }
    )
    features.index.name = "timestamp"
    return features


def safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0.0, np.nan)


def densify_seconds(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return features

    index_values = features.index.to_numpy(dtype=np.int64)
    full_index = pd.Index(
        np.arange(index_values.min(), index_values.max() + 1000, 1000, dtype=np.int64),
        name=features.index.name,
    )
    dense = features.reindex(full_index)
    for column in ZERO_ON_EMPTY_SECOND_COLS:
        dense[column] = dense[column].fillna(0.0)

    close = dense["close"].ffill().bfill()
    for column in ["open", "high", "low", "close"]:
        dense[column] = dense[column].fillna(close)
    return dense


def finalize_features(features: pd.DataFrame) -> pd.DataFrame:
    features = features.copy()
    volume = features["volume"].replace(0.0, np.nan)
    quote_qty = features["quote_qty"].replace(0.0, np.nan)
    trade_count = features["trade_count"].replace(0, np.nan)
    vwap = (quote_qty / volume).fillna(features["close"])

    features["vwap"] = vwap
    features["interarrival_ms_mean"] = features["interarrival_ms_sum"] / trade_count
    features["taker_buy_ratio"] = features["buy_trade_count"] / trade_count
    features["signed_qty_ratio"] = features["signed_qty"] / volume
    features["signed_quote_ratio"] = features["signed_quote_qty"] / quote_qty
    features["price_change_bps"] = (features["close"] / features["open"] - 1.0) * 10_000
    features["trade_range_bps"] = (features["high"] - features["low"]) / vwap * 10_000
    features["buy_sweep_bps"] = (features["buy_price_high"] - features["buy_price_low"]) / vwap * 10_000
    features["sell_sweep_bps"] = (features["sell_price_high"] - features["sell_price_low"]) / vwap * 10_000
    features["buy_iceberg_pressure"] = features["buy_trade_count"] / (1.0 + features["buy_sweep_bps"].fillna(0.0))
    features["sell_iceberg_pressure"] = features["sell_trade_count"] / (1.0 + features["sell_sweep_bps"].fillna(0.0))

    fill_zero_cols = [
        "taker_buy_ratio",
        "signed_qty_ratio",
        "signed_quote_ratio",
        "price_change_bps",
        "trade_range_bps",
        "buy_sweep_bps",
        "sell_sweep_bps",
        "buy_iceberg_pressure",
        "sell_iceberg_pressure",
        "interarrival_ms_mean",
    ]
    features[fill_zero_cols] = features[fill_zero_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return features


def add_rolling_tempo_features(features: pd.DataFrame) -> pd.DataFrame:
    features = features.copy()
    for window in ROLLING_WINDOWS:
        buy_quote_sum = features["buy_quote_qty"].rolling(window, min_periods=1).sum()
        sell_quote_sum = features["sell_quote_qty"].rolling(window, min_periods=1).sum()
        quote_sum = buy_quote_sum + sell_quote_sum

        features[f"signed_qty_sum_{window}s"] = features["signed_qty"].rolling(window, min_periods=1).sum()
        features[f"signed_quote_sum_{window}s"] = features["signed_quote_qty"].rolling(window, min_periods=1).sum()
        features[f"trade_count_sum_{window}s"] = features["trade_count"].rolling(window, min_periods=1).sum()
        features[f"buy_quote_sum_{window}s"] = buy_quote_sum
        features[f"sell_quote_sum_{window}s"] = sell_quote_sum
        features[f"quote_imbalance_{window}s"] = safe_ratio(buy_quote_sum - sell_quote_sum, quote_sum).fillna(0.0)
        features[f"max_same_side_run_max_{window}s"] = features["max_same_side_run"].rolling(window, min_periods=1).max()
        features[f"buy_sweep_bps_max_{window}s"] = features["buy_sweep_bps"].rolling(window, min_periods=1).max()
        features[f"sell_sweep_bps_max_{window}s"] = features["sell_sweep_bps"].rolling(window, min_periods=1).max()
        features[f"buy_iceberg_pressure_sum_{window}s"] = features["buy_iceberg_pressure"].rolling(window, min_periods=1).sum()
        features[f"sell_iceberg_pressure_sum_{window}s"] = features["sell_iceberg_pressure"].rolling(window, min_periods=1).sum()
        features[f"buy_sweep_score_{window}s"] = features[f"buy_sweep_bps_max_{window}s"] * np.log1p(buy_quote_sum)
        features[f"sell_sweep_score_{window}s"] = features[f"sell_sweep_bps_max_{window}s"] * np.log1p(sell_quote_sum)
    return features.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def sign_streak(values: pd.Series) -> pd.Series:
    signs = np.sign(values.to_numpy(dtype=np.float64)).astype(np.int8)
    streaks = np.zeros(len(signs), dtype=np.int32)
    previous = 0
    streak = 0
    for index, sign in enumerate(signs):
        if sign == 0:
            streak = 0
        elif sign == previous:
            streak += 1
        else:
            streak = 1
        streaks[index] = streak * int(sign)
        previous = int(sign)
    return pd.Series(streaks, index=values.index)


def add_event_bucket_features(features: pd.DataFrame, bucket_notional: float = EVENT_BUCKET_NOTIONAL) -> pd.DataFrame:
    features = features.copy()
    if features.empty or bucket_notional <= 0:
        return features

    cumulative_quote = features["quote_qty"].cumsum()
    bucket_ids = np.floor((cumulative_quote.clip(lower=0.0) / bucket_notional)).astype(np.int64)
    bucket_frame = pd.DataFrame(
        {
            "bucket_quote_qty": features.groupby(bucket_ids)["quote_qty"].sum(),
            "bucket_signed_quote_qty": features.groupby(bucket_ids)["signed_quote_qty"].sum(),
        }
    )
    bucket_frame["bucket_imbalance"] = safe_ratio(
        bucket_frame["bucket_signed_quote_qty"].abs(),
        bucket_frame["bucket_quote_qty"],
    ).fillna(0.0)
    bucket_frame["vpin_50b"] = bucket_frame["bucket_imbalance"].rolling(VPIN_BUCKET_WINDOW, min_periods=1).mean()
    bucket_frame["bucket_side_streak"] = sign_streak(bucket_frame["bucket_signed_quote_qty"])

    mapped = bucket_frame.reindex(bucket_ids.to_numpy()).reset_index(drop=True)
    features["event_bucket_id"] = bucket_ids.to_numpy()
    features["event_bucket_notional"] = bucket_notional
    features["bucket_imbalance"] = mapped["bucket_imbalance"].to_numpy()
    features["vpin_50b"] = mapped["vpin_50b"].to_numpy()
    features["bucket_side_streak"] = mapped["bucket_side_streak"].to_numpy()
    return features.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def add_hawkes_proxy_features(features: pd.DataFrame) -> pd.DataFrame:
    features = features.copy()
    short = HAWKES_SHORT_HALFLIFE_S
    long = HAWKES_LONG_HALFLIFE_S
    for prefix, column in [
        ("buy_trade", "buy_trade_count"),
        ("sell_trade", "sell_trade_count"),
        ("buy_quote", "buy_quote_qty"),
        ("sell_quote", "sell_quote_qty"),
    ]:
        features[f"{prefix}_intensity_h{int(short)}s"] = features[column].ewm(halflife=short, adjust=False).mean()
        features[f"{prefix}_intensity_h{int(long)}s"] = features[column].ewm(halflife=long, adjust=False).mean()
        features[f"{prefix}_excitation_ratio"] = safe_ratio(
            features[f"{prefix}_intensity_h{int(short)}s"],
            features[f"{prefix}_intensity_h{int(long)}s"],
        ).fillna(0.0)

    buy_trade_short = features[f"buy_trade_intensity_h{int(short)}s"]
    sell_trade_short = features[f"sell_trade_intensity_h{int(short)}s"]
    buy_quote_short = features[f"buy_quote_intensity_h{int(short)}s"]
    sell_quote_short = features[f"sell_quote_intensity_h{int(short)}s"]
    features["trade_intensity_imbalance"] = safe_ratio(buy_trade_short - sell_trade_short, buy_trade_short + sell_trade_short).fillna(0.0)
    features["quote_intensity_imbalance"] = safe_ratio(buy_quote_short - sell_quote_short, buy_quote_short + sell_quote_short).fillna(0.0)
    features["hawkes_burst_score"] = (
        features["trade_count"]
        * (1.0 + features["max_same_side_run"])
        / (1.0 + features["interarrival_ms_mean"])
    )
    return features.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def complete_features(features: pd.DataFrame, *, dense: bool) -> pd.DataFrame:
    if dense:
        features = densify_seconds(features)
    features = finalize_features(features)
    features = add_rolling_tempo_features(features)
    features = add_event_bucket_features(features)
    features = add_hawkes_proxy_features(features)
    return features


def combine_partials(partials: list[pd.DataFrame], *, dense: bool) -> pd.DataFrame:
    combined = pd.concat(partials).sort_index()
    if not combined.index.duplicated().any():
        return complete_features(combined, dense=dense)

    grouped = combined.groupby(level=0, sort=True)
    merged = pd.DataFrame(
        {
            "open": grouped["open"].first(),
            "high": grouped["high"].max(),
            "low": grouped["low"].min(),
            "close": grouped["close"].last(),
            "volume": grouped["volume"].sum(),
            "quote_qty": grouped["quote_qty"].sum(),
            "buy_qty": grouped["buy_qty"].sum(),
            "sell_qty": grouped["sell_qty"].sum(),
            "buy_quote_qty": grouped["buy_quote_qty"].sum(),
            "sell_quote_qty": grouped["sell_quote_qty"].sum(),
            "signed_qty": grouped["signed_qty"].sum(),
            "signed_quote_qty": grouped["signed_quote_qty"].sum(),
            "trade_count": grouped["trade_count"].sum(),
            "buy_trade_count": grouped["buy_trade_count"].sum(),
            "sell_trade_count": grouped["sell_trade_count"].sum(),
            "max_same_side_run": grouped["max_same_side_run"].max(),
            "max_buy_run": grouped["max_buy_run"].max(),
            "max_sell_run": grouped["max_sell_run"].max(),
            "interarrival_ms_sum": grouped["interarrival_ms_sum"].sum(),
            "interarrival_ms_p90": grouped["interarrival_ms_p90"].max(),
            "interarrival_ms_max": grouped["interarrival_ms_max"].max(),
            "buy_price_high": grouped["buy_price_high"].max(),
            "buy_price_low": grouped["buy_price_low"].min(),
            "sell_price_high": grouped["sell_price_high"].max(),
            "sell_price_low": grouped["sell_price_low"].min(),
        }
    )
    merged.index.name = "timestamp"
    return complete_features(merged, dense=dense)


def date_tag_from_path(csv_path: Path) -> str:
    parts = csv_path.stem.split("-")
    if len(parts) < 5:
        raise ValueError(f"Cannot parse date tag from {csv_path.name}")
    tag = "-".join(parts[2:5])
    if len(tag) != 10:
        raise ValueError(f"{csv_path.name}: expected daily raw-trade filename with YYYY-MM-DD tag")
    return tag


def process_file(
    csv_path: Path,
    symbol: str,
    out_dir: Path,
    *,
    chunk_size: int,
    verbose: bool,
    overwrite: bool,
    dense: bool,
) -> tuple[str, int, Path]:
    date_tag = date_tag_from_path(csv_path)
    symbol_out_dir = out_dir / symbol
    symbol_out_dir.mkdir(parents=True, exist_ok=True)
    out_path = symbol_out_dir / f"{symbol}-trade-tempo-{date_tag}.parquet"
    if out_path.exists() and not overwrite:
        return "skip", 0, out_path

    carry = ChunkCarry()
    partials = []
    rows_read = 0
    for chunk in pd.read_csv(csv_path, usecols=COLUMNS, dtype=DTYPES, header=0, chunksize=chunk_size):
        rows_read += len(chunk)
        partials.append(aggregate_chunk(enrich_trades(chunk, carry)))
        if verbose and rows_read % (chunk_size * 5) == 0:
            print(f"  {csv_path.name}: {rows_read:,} rows")

    features = combine_partials(partials, dense=dense)
    features.to_parquet(out_path, engine="pyarrow")
    return "ok", rows_read, out_path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(
    csv_files: list[Path],
    symbol: str,
    out_dir: Path,
    manifest_path: Path,
) -> Path:
    daily_files = []
    digest = hashlib.sha256()
    for raw_path in csv_files:
        day = date_tag_from_path(raw_path)
        sidecar_path = out_dir / symbol / f"{symbol}-trade-tempo-{day}.parquet"
        if not sidecar_path.is_file():
            raise FileNotFoundError(f"missing taker-tempo sidecar: {sidecar_path}")
        counts = pd.read_parquet(
            sidecar_path,
            columns=["buy_trade_count", "sell_trade_count"],
        )
        raw_sha256 = _sha256_file(raw_path)
        sidecar_sha256 = _sha256_file(sidecar_path)
        row = {
            "day": day,
            "raw_file": str(raw_path.resolve()),
            "raw_size_bytes": int(raw_path.stat().st_size),
            "raw_sha256": raw_sha256,
            "sidecar_file": str(sidecar_path.resolve()),
            "sidecar_size_bytes": int(sidecar_path.stat().st_size),
            "sidecar_sha256": sidecar_sha256,
            "sidecar_rows": int(len(counts)),
            "buy_taker_trades": int(counts["buy_trade_count"].sum()),
            "sell_taker_trades": int(counts["sell_trade_count"].sum()),
        }
        if row["buy_taker_trades"] <= 0 or row["sell_taker_trades"] <= 0:
            raise ValueError(f"{day}: taker-tempo sidecar is missing one trade side")
        daily_files.append(row)
        digest.update(
            (
                f"{day}\0{raw_sha256}\0{sidecar_sha256}\0"
                f"{row['sidecar_rows']}\0{row['buy_taker_trades']}\0"
                f"{row['sell_taker_trades']}\n"
            ).encode()
        )

    payload = {
        "schema": "narrowgate.taker_tempo_manifest.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "raw_root": str(csv_files[0].parent.resolve()) if csv_files else "",
        "sidecar_root": str((out_dir / symbol).resolve()),
        "daily_file_count": len(daily_files),
        "first_day": daily_files[0]["day"] if daily_files else "",
        "last_day": daily_files[-1]["day"] if daily_files else "",
        "daily_manifest_sha256": digest.hexdigest(),
        "generator": str(Path(__file__).resolve()),
        "generator_sha256": _sha256_file(Path(__file__).resolve()),
        "daily_files": daily_files,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def main() -> None:
    default_symbol = normalize_symbol(os.environ.get("MM_SYMBOL"), "BTCUSDC")
    parser = argparse.ArgumentParser(description="Build taker-tempo features from raw Binance futures trades")
    parser.add_argument("--symbol", default=default_symbol, help=f"Symbol, default {default_symbol}")
    parser.add_argument("--file", default=None, help="Only process files whose name contains this string")
    parser.add_argument(
        "--days-file",
        type=Path,
        default=None,
        help="Optional CSV day whitelist; mutually exclusive with --file",
    )
    parser.add_argument("--raw-dir", type=Path, default=None, help="Override raw_trades directory")
    parser.add_argument("--out-dir", type=Path, default=None, help="Override trade_features directory")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="CSV rows per chunk")
    parser.add_argument("--overwrite", action="store_true", help="Regenerate existing parquet outputs")
    parser.add_argument("--sparse", action="store_true", help="Keep only seconds with trades instead of dense 1s output")
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=None,
        help="Write a SHA-bound raw/sidecar manifest after all selected files pass.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print progress")
    parser.add_argument("--workers", type=int, default=1, help="Parallel UTC-day workers, maximum 4")
    args = parser.parse_args()
    if args.file and args.days_file is not None:
        raise SystemExit("--file and --days-file are mutually exclusive")
    if args.workers < 1 or args.workers > 4:
        raise SystemExit("--workers must be in [1, 4]")

    symbol = normalize_symbol(args.symbol)
    raw_dir = args.raw_dir.expanduser().resolve() if args.raw_dir else data_root(ROOT) / "raw_trades"
    out_dir = args.out_dir.expanduser().resolve() if args.out_dir else data_root(ROOT) / "trade_features"
    csv_files = sorted((raw_dir / symbol).glob(f"{symbol}-trades-*.csv"))
    if args.file:
        if len(args.file) != 10 or "/" in args.file:
            raise SystemExit(f"--file must be an explicit UTC daily tag YYYY-MM-DD: {args.file}")
        csv_files = [csv_path for csv_path in csv_files if args.file in csv_path.name]
    elif args.days_file is not None:
        with args.days_file.expanduser().resolve().open(
            newline="", encoding="utf-8-sig"
        ) as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None or "day" not in reader.fieldnames:
                raise SystemExit("--days-file must contain a day column")
            requested_days = {
                str(row["day"]).strip()
                for row in reader
                if str(row.get("day", "")).strip()
            }
        invalid_days = sorted(
            day
            for day in requested_days
            if len(day) != 10 or day[4] != "-" or day[7] != "-"
        )
        if invalid_days:
            raise SystemExit(
                f"--days-file contains invalid UTC days: {invalid_days[:5]}"
            )
        csv_files = [
            csv_path
            for csv_path in csv_files
            if csv_path.name.removesuffix(".csv").removeprefix(
                f"{symbol}-trades-"
            )
            in requested_days
        ]
        observed_days = {
            csv_path.name.removesuffix(".csv").removeprefix(
                f"{symbol}-trades-"
            )
            for csv_path in csv_files
        }
        missing_days = sorted(requested_days - observed_days)
        if missing_days:
            raise SystemExit(
                "raw trade files missing for frozen days: "
                + ", ".join(missing_days[:10])
            )

    if not csv_files:
        print(f"No raw trade CSV files found for {symbol} in {raw_dir / symbol}")
        sys.exit(1)

    print(f"Symbol: {symbol}")
    print(f"Input: {raw_dir / symbol}")
    print(f"Output: {out_dir / symbol}")
    print(f"Files: {len(csv_files)}")

    ok_count = skip_count = rows_read = 0

    def record(csv_path: Path, result: tuple[str, int, Path]) -> None:
        nonlocal ok_count, skip_count, rows_read
        status, file_rows, out_path = result
        ok_count += status == "ok"
        skip_count += status == "skip"
        rows_read += file_rows
        if args.verbose or status == "ok":
            print(f"[{status.upper()}] {csv_path.name} -> {out_path.name} ({file_rows:,} rows)")

    if args.workers == 1:
        for csv_path in csv_files:
            record(
                csv_path,
                process_file(
                    csv_path,
                    symbol,
                    out_dir,
                    chunk_size=args.chunk_size,
                    verbose=args.verbose,
                    overwrite=args.overwrite,
                    dense=not args.sparse,
                ),
            )
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers
        ) as executor:
            pending = {
                executor.submit(
                    process_file,
                    csv_path,
                    symbol,
                    out_dir,
                    chunk_size=args.chunk_size,
                    verbose=args.verbose,
                    overwrite=args.overwrite,
                    dense=not args.sparse,
                ): csv_path
                for csv_path in csv_files
            }
            for future in concurrent.futures.as_completed(pending):
                record(pending[future], future.result())

    print(f"Done: ok {ok_count}, skip {skip_count}, rows {rows_read:,}")
    if args.manifest_path is not None:
        manifest_path = write_manifest(
            csv_files,
            symbol,
            out_dir,
            args.manifest_path.expanduser().resolve(),
        )
        print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
