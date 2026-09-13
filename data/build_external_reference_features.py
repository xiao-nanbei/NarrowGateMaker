#!/usr/bin/env python3
"""Build causal daily 1s external-venue reference features from normalized trades.

The bar containing events in ``[t, t+1s)`` is timestamped at ``t+1s``.  This
right-edge convention guarantees that an order decision at time ``t`` never
sees trades that arrive later in the same second.

Bitget, Bybit, and OKX all use this neutral implementation. Venue-specific
download/import modules only normalize their source data into the shared trade
schema consumed here.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.download_bitget_reference import load_manifest  # noqa: E402

DAY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
INPUT_COLUMNS = (
    "trade_id",
    "exchange_event_ts_ms",
    "price",
    "size",
    "taker_side",
)


@dataclass
class BuildResult:
    day: str
    status: str
    input_rows: int = 0
    bars: int = 0
    min_event_ts_ms: int = 0
    max_event_ts_ms: int = 0
    output_path: str = ""
    message: str = ""

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


def _input_path(trades_dir: Path, symbol: str, day: date, venue: str = "bitget") -> Path:
    return trades_dir / f"{venue}_{symbol}_trades_{day.isoformat()}.csv.gz"


def _output_path(out_dir: Path, symbol: str, day: date, venue: str = "bitget") -> Path:
    return out_dir / f"{symbol}-{venue}-trades-1s-{day.isoformat()}.parquet"


def _meta_path(path: Path) -> Path:
    return Path(str(path) + ".meta.json")


def _chunk_seconds(chunk: pd.DataFrame, *, source_offset: int = 0) -> pd.DataFrame:
    for column in ("exchange_event_ts_ms", "price", "size"):
        chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
    chunk["trade_id"] = chunk["trade_id"].astype("string").str.strip()
    chunk = chunk.dropna(subset=["trade_id", "exchange_event_ts_ms", "price", "size"])
    chunk = chunk[chunk["trade_id"].ne("")]
    chunk = chunk[(chunk["price"] > 0.0) & (chunk["size"] >= 0.0)]
    if chunk.empty:
        return pd.DataFrame()
    chunk["exchange_event_ts_ms"] = chunk["exchange_event_ts_ms"].astype("int64")
    source_order = pd.Series(
        np.arange(source_offset, source_offset + len(chunk), dtype="int64"),
        index=chunk.index,
        dtype="int64",
    )
    numeric_id = chunk["trade_id"].str.fullmatch(r"\d+")
    # Numeric venue IDs sort exactly after zero-padding; UUID-style IDs retain
    # the archive's stable source order.  This avoids float precision loss and
    # lets Bitget numeric IDs and Bybit UUIDs share one causal builder.
    chunk["trade_order_key"] = chunk["trade_id"].str.zfill(32)
    chunk.loc[~numeric_id, "trade_order_key"] = "~" + source_order.loc[~numeric_id].astype(
        "string"
    ).str.zfill(20)
    chunk["second_ts_ms"] = (chunk["exchange_event_ts_ms"] // 1000) * 1000
    chunk["buy_volume"] = np.where(chunk["taker_side"].str.lower().eq("buy"), chunk["size"], 0.0)
    chunk["sell_volume"] = np.where(chunk["taker_side"].str.lower().eq("sell"), chunk["size"], 0.0)
    ordered = chunk.sort_values(["exchange_event_ts_ms", "trade_order_key"], kind="mergesort")
    grouped = ordered.groupby("second_ts_ms", sort=True, observed=True)
    out = grouped.agg(
        first_event_ts_ms=("exchange_event_ts_ms", "first"),
        first_trade_id=("trade_order_key", "first"),
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        last_event_ts_ms=("exchange_event_ts_ms", "last"),
        last_trade_id=("trade_order_key", "last"),
        buy_volume=("buy_volume", "sum"),
        sell_volume=("sell_volume", "sum"),
        trade_count=("trade_id", "size"),
    ).reset_index()
    return out


def _merge_chunk_seconds(frames: list[pd.DataFrame]) -> pd.DataFrame:
    work = pd.concat(frames, ignore_index=True)
    if work.empty:
        return work
    work = work.sort_values(
        ["second_ts_ms", "first_event_ts_ms", "first_trade_id"], kind="mergesort"
    )
    first = work.groupby("second_ts_ms", sort=True, observed=True).first()
    last = (
        work.sort_values(["second_ts_ms", "last_event_ts_ms", "last_trade_id"], kind="mergesort")
        .groupby("second_ts_ms", sort=True, observed=True)
        .last()
    )
    sums = work.groupby("second_ts_ms", sort=True, observed=True).agg(
        high=("high", "max"),
        low=("low", "min"),
        buy_volume=("buy_volume", "sum"),
        sell_volume=("sell_volume", "sum"),
        trade_count=("trade_count", "sum"),
    )
    out = pd.DataFrame(index=sums.index)
    out["event_second_ts_ms"] = out.index.astype("int64")
    out["timestamp"] = out["event_second_ts_ms"] + 1000
    out["first_event_ts_ms"] = first["first_event_ts_ms"].astype("int64")
    out["last_event_ts_ms"] = last["last_event_ts_ms"].astype("int64")
    out["open"] = first["open"].astype("float64")
    out["high"] = sums["high"].astype("float64")
    out["low"] = sums["low"].astype("float64")
    out["close"] = last["close"].astype("float64")
    out["buy_volume"] = sums["buy_volume"].astype("float64")
    out["sell_volume"] = sums["sell_volume"].astype("float64")
    out["volume"] = out["buy_volume"] + out["sell_volume"]
    out["flow_imbalance"] = (
        (out["buy_volume"] - out["sell_volume"]) / out["volume"].replace(0.0, np.nan)
    ).fillna(0.0)
    out["trade_count"] = sums["trade_count"].astype("int64")
    out["source_age_ms"] = out["timestamp"] - out["last_event_ts_ms"]
    return out.reset_index(drop=True)


def build_day(
    *,
    day: date,
    symbol: str,
    trades_dir: Path,
    out_dir: Path,
    chunksize: int,
    overwrite: bool,
    venue: str = "bitget",
    instrument_type: str = "perp",
) -> BuildResult:
    venue = str(venue).strip().lower()
    instrument_type = str(instrument_type).strip().lower()
    if instrument_type not in {"perp", "spot"}:
        raise ValueError("instrument_type must be perp or spot")
    source = _input_path(trades_dir, symbol, day, venue)
    target = _output_path(out_dir, symbol, day, venue)
    meta_path = _meta_path(target)
    if target.exists() and meta_path.exists() and not overwrite:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("complete"):
            return BuildResult(
                day=day.isoformat(),
                status="present",
                input_rows=int(meta.get("input_rows", 0)),
                bars=int(meta.get("bars", 0)),
                min_event_ts_ms=int(meta.get("min_event_ts_ms", 0)),
                max_event_ts_ms=int(meta.get("max_event_ts_ms", 0)),
                output_path=str(target),
            )
    if not source.exists():
        return BuildResult(day=day.isoformat(), status="missing_input", message=str(source))

    frames: list[pd.DataFrame] = []
    input_rows = 0
    for chunk in pd.read_csv(
        source,
        compression="gzip",
        usecols=list(INPUT_COLUMNS),
        dtype={"taker_side": "string"},
        chunksize=max(10_000, int(chunksize)),
    ):
        source_offset = input_rows
        input_rows += len(chunk)
        seconds = _chunk_seconds(chunk, source_offset=source_offset)
        if not seconds.empty:
            frames.append(seconds)
    if not frames:
        return BuildResult(day=day.isoformat(), status="empty", input_rows=input_rows)
    bars = _merge_chunk_seconds(frames)

    day_start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    event_min = int(bars["first_event_ts_ms"].min())
    event_max = int(bars["last_event_ts_ms"].max())
    if event_min < day_start_ms or event_max >= day_start_ms + 86_400_000:
        raise ValueError(f"{day}: event timestamps cross UTC day: {event_min}..{event_max}")
    if not bars["timestamp"].is_monotonic_increasing or bars["timestamp"].duplicated().any():
        raise ValueError(f"{day}: output timestamps are not unique monotonic seconds")

    out_dir.mkdir(parents=True, exist_ok=True)
    temp = Path(str(target) + ".part")
    bars.to_parquet(temp, index=False)
    os.replace(temp, target)
    meta = {
        "complete": True,
        "source": str(source),
        "venue": venue,
        "market_id": f"{venue}:{instrument_type}:{symbol}",
        "instrument_type": instrument_type,
        "symbol": symbol,
        "utc_day": day.isoformat(),
        "causal_timestamp_rule": "events_[t,t+1s)_available_at_t+1s",
        "input_rows": input_rows,
        "bars": len(bars),
        "min_event_ts_ms": event_min,
        "max_event_ts_ms": event_max,
        "built_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return BuildResult(
        day=day.isoformat(),
        status="built",
        input_rows=input_rows,
        bars=len(bars),
        min_event_ts_ms=event_min,
        max_event_ts_ms=event_max,
        output_path=str(target),
    )


def _write_manifest(path: Path, rows: list[BuildResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(str(path) + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(BuildResult("", "").as_dict()))
        writer.writeheader()
        for row in sorted(rows, key=lambda item: item.day):
            writer.writerow(row.as_dict())
    os.replace(temp, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--trades-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--venue", default="bitget", choices=("bitget", "bybit", "okx"))
    parser.add_argument("--instrument-type", default="perp", choices=("perp", "spot"))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--chunksize", type=int, default=500_000)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--status-out", type=Path)
    args = parser.parse_args()

    days = load_manifest(args.manifest)
    if args.max_days > 0:
        days = days[: args.max_days]
    results: list[BuildResult] = []
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                build_day,
                day=day,
                symbol=args.symbol.upper(),
                trades_dir=args.trades_dir.expanduser(),
                out_dir=args.out_dir.expanduser(),
                chunksize=args.chunksize,
                    overwrite=args.overwrite,
                    venue=args.venue,
                    instrument_type=args.instrument_type,
            ): day
            for day in days
        }
        for index, future in enumerate(as_completed(futures), 1):
            day = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = BuildResult(day=day.isoformat(), status="error", message=str(exc))
            results.append(result)
            print(
                f"[{index:03d}/{len(days):03d}] {result.day} {result.status} "
                f"rows={result.input_rows:,} bars={result.bars:,} {result.message}",
                flush=True,
            )
            status_out = args.status_out or (
                args.out_dir / f"{args.symbol}_{args.venue}_1s_manifest.csv"
            )
            _write_manifest(status_out, results)

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    print(json.dumps({"days": len(results), "status_counts": counts}, sort_keys=True))


if __name__ == "__main__":
    main()
