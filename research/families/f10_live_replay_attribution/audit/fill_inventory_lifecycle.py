#!/usr/bin/env python3
"""Reconstruct the inventory lifetime created by each exposure-increasing fill."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

SCHEMA_VERSION = "fill_inventory_lifecycle.v1"


@dataclass
class Lot:
    lot_id: str
    day: str
    inventory_side: str
    opened_ts: float
    opened_price: float
    quantity: float
    remaining: float
    campaign_key: str


def _day_end(day: str) -> float:
    start = pd.Timestamp(day, tz="UTC")
    return float((start + pd.Timedelta(days=1)).timestamp())


def reconstruct_lifetimes(fills: pd.DataFrame, *, matching: str) -> pd.DataFrame:
    if matching not in {"fifo", "lifo"}:
        raise ValueError("matching must be fifo or lifo")
    source = fills.copy()
    source["day"] = source["day"].astype(str).str.slice(0, 10)
    source["side"] = source["side"].astype(str).str.upper()
    source["fill_ts"] = pd.to_numeric(source["fill_ts"], errors="coerce")
    source["filled_qty"] = pd.to_numeric(source["filled_qty"], errors="coerce")
    source = source[
        source["side"].isin({"BUY", "SELL"})
        & source["fill_ts"].notna()
        & source["filled_qty"].gt(0.0)
    ].copy()
    dedup = [name for name in ("day", "client_order_id") if name in source]
    if len(dedup) == 2:
        source = source.drop_duplicates(dedup, keep="last")

    output: list[dict[str, object]] = []
    for day, group in source.groupby("day", sort=True):
        long_lots: list[Lot] = []
        short_lots: list[Lot] = []
        campaign_lots: list[dict[str, object]] = []
        campaign_number = 0

        def close_lots(
            lots: list[Lot],
            quantity: float,
            close_ts: float,
            close_price: float,
            records: list[dict[str, object]],
        ) -> float:
            left = quantity
            while left > 1e-12 and lots:
                index = 0 if matching == "fifo" else -1
                lot = lots[index]
                matched = min(left, lot.remaining)
                lot_pnl = (
                    (close_price - lot.opened_price) * matched
                    if lot.inventory_side == "LONG"
                    else (lot.opened_price - close_price) * matched
                )
                records.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "matching": matching,
                        "day": lot.day,
                        "lot_id": lot.lot_id,
                        "campaign_key": lot.campaign_key,
                        "inventory_side": lot.inventory_side,
                        "opened_ts": lot.opened_ts,
                        "opened_price": lot.opened_price,
                        "closed_ts": close_ts,
                        "closed_price": close_price,
                        "matched_qty": matched,
                        "lot_pnl": lot_pnl,
                        "duration_s": max(0.0, close_ts - lot.opened_ts),
                        "observed_close": 1,
                    }
                )
                lot.remaining -= matched
                left -= matched
                if lot.remaining <= 1e-12:
                    lots.pop(index)
            return left

        ordered = group.sort_values(["fill_ts", "client_order_id"], kind="stable")
        lot_sequence = 0
        for row in ordered.itertuples(index=False):
            timestamp = float(row.fill_ts)
            quantity = float(row.filled_qty)
            price = float(row.avg_fill_price)
            side = str(row.side)
            if not long_lots and not short_lots:
                campaign_number += 1
            campaign_key = f"{day}:{campaign_number}"
            if side == "BUY":
                remaining = close_lots(
                    short_lots, quantity, timestamp, price, campaign_lots
                )
                inventory_side = "LONG"
                destination = long_lots
            else:
                remaining = close_lots(
                    long_lots, quantity, timestamp, price, campaign_lots
                )
                inventory_side = "SHORT"
                destination = short_lots
            if remaining > 1e-12:
                lot_sequence += 1
                destination.append(
                    Lot(
                        lot_id=f"{day}:{lot_sequence}",
                        day=day,
                        inventory_side=inventory_side,
                        opened_ts=timestamp,
                        opened_price=price,
                        quantity=remaining,
                        remaining=remaining,
                        campaign_key=campaign_key,
                    )
                )
            if not long_lots and not short_lots and campaign_lots:
                for item in campaign_lots:
                    item["campaign_flat_ts"] = timestamp
                    item["campaign_flat_duration_s"] = max(
                        0.0, timestamp - float(item["opened_ts"])
                    )
                output.extend(campaign_lots)
                campaign_lots = []

        censor_ts = _day_end(day)
        for lot in [*long_lots, *short_lots]:
            campaign_lots.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "matching": matching,
                    "day": lot.day,
                    "lot_id": lot.lot_id,
                    "campaign_key": lot.campaign_key,
                    "inventory_side": lot.inventory_side,
                    "opened_ts": lot.opened_ts,
                    "opened_price": lot.opened_price,
                    "closed_ts": np.nan,
                    "closed_price": np.nan,
                    "matched_qty": lot.remaining,
                    "lot_pnl": np.nan,
                    "duration_s": max(0.0, censor_ts - lot.opened_ts),
                    "observed_close": 0,
                    "campaign_flat_ts": np.nan,
                    "campaign_flat_duration_s": np.nan,
                }
            )
        output.extend(campaign_lots)
    return pd.DataFrame(output)


def _km_stats(frame: pd.DataFrame, *, tau_s: float = 86_400.0) -> dict[str, float]:
    if frame.empty:
        return {}
    duration = pd.to_numeric(frame["duration_s"], errors="coerce").to_numpy(float)
    observed = pd.to_numeric(frame["observed_close"], errors="coerce").fillna(0).to_numpy(int)
    quantity = pd.to_numeric(frame["matched_qty"], errors="coerce").fillna(0).to_numpy(float)
    valid = np.isfinite(duration) & (duration >= 0.0) & (quantity > 0.0)
    duration, observed, quantity = duration[valid], observed[valid], quantity[valid]
    event_times = np.unique(duration[observed == 1])
    survival = 1.0
    previous = 0.0
    rmst = 0.0
    median = math.nan
    survival_points: dict[int, float] = {}
    horizons = (30, 60, 300, 900, 1800, 3600, 7200, 21600)
    for timestamp in sorted(set(event_times.tolist()) | set(horizons) | {tau_s}):
        if timestamp > tau_s:
            break
        rmst += survival * max(0.0, timestamp - previous)
        risk = quantity[duration >= timestamp - 1e-12].sum()
        events = quantity[(duration == timestamp) & (observed == 1)].sum()
        if risk > 0.0 and events > 0.0:
            survival *= max(0.0, 1.0 - events / risk)
            if math.isnan(median) and survival <= 0.5:
                median = float(timestamp)
        if timestamp in horizons:
            survival_points[int(timestamp)] = survival
        previous = timestamp
    closed = duration[observed == 1]
    result = {
        "lots": float(len(duration)),
        "quantity": float(quantity.sum()),
        "closed_rate": float(np.average(observed, weights=quantity)),
        "closed_mean_s": float(np.mean(closed)) if len(closed) else math.nan,
        "closed_median_s": float(np.median(closed)) if len(closed) else math.nan,
        "closed_p75_s": float(np.quantile(closed, 0.75)) if len(closed) else math.nan,
        "closed_p90_s": float(np.quantile(closed, 0.90)) if len(closed) else math.nan,
        "closed_p95_s": float(np.quantile(closed, 0.95)) if len(closed) else math.nan,
        "km_median_s": median,
        "km_rmst_24h_s": float(rmst),
    }
    result.update({f"km_survival_{horizon}s": value for horizon, value in survival_points.items()})
    return result


def summarize_lifetimes(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(["matching", "inventory_side"], sort=True):
        matching, side = keys
        rows.append(
            {
                "matching": matching,
                "inventory_side": side,
                **_km_stats(group),
                **_campaign_flat_stats(group),
                **_lot_pnl_stats(group),
            }
        )
    for matching, group in frame.groupby("matching", sort=True):
        rows.append(
            {
                "matching": matching,
                "inventory_side": "ALL",
                **_km_stats(group),
                **_campaign_flat_stats(group),
                **_lot_pnl_stats(group),
            }
        )
    return pd.DataFrame(rows)


def _campaign_flat_stats(frame: pd.DataFrame) -> dict[str, float]:
    duration = pd.to_numeric(frame["campaign_flat_duration_s"], errors="coerce").dropna()
    if duration.empty:
        return {
            "campaign_flat_mean_s": math.nan,
            "campaign_flat_median_s": math.nan,
            "campaign_flat_p90_s": math.nan,
        }
    return {
        "campaign_flat_mean_s": float(duration.mean()),
        "campaign_flat_median_s": float(duration.median()),
        "campaign_flat_p90_s": float(duration.quantile(0.90)),
    }


def _lot_pnl_stats(frame: pd.DataFrame) -> dict[str, float]:
    pnl = pd.to_numeric(frame["lot_pnl"], errors="coerce").dropna()
    if pnl.empty:
        return {"closed_lot_pnl_sum": math.nan, "closed_lot_pnl_mean": math.nan}
    return {
        "closed_lot_pnl_sum": float(pnl.sum()),
        "closed_lot_pnl_mean": float(pnl.mean()),
    }


def _load_input_paths(
    *,
    data_dir: Path | None,
    input_glob: str | None,
    input_filelist: Path | None,
) -> list[Path]:
    if input_filelist is not None:
        text = input_filelist.read_text(encoding="utf-8").strip()
        if not text:
            raise SystemExit("input filelist is empty")
        paths: list[Path] = []
        if input_filelist.suffix.lower() == ".csv":
            with input_filelist.open(newline="") as stream:
                for row in csv.DictReader(stream):
                    value = (
                        row.get("order_level_csv")
                        or row.get("path")
                        or row.get("file")
                        or ""
                    ).strip()
                    if value:
                        paths.append(Path(value))
        else:
            paths = [
                Path(line.strip())
                for line in text.splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
    else:
        assert data_dir is not None and input_glob is not None
        paths = sorted(data_dir.glob(input_glob))
    if not paths:
        raise SystemExit("no input order-level files")
    resolved = [path.expanduser().resolve() for path in paths]
    if len(set(resolved)) != len(resolved):
        raise SystemExit("duplicate order-level files in input")
    missing = [path for path in resolved if not path.is_file()]
    if missing:
        raise SystemExit(f"missing order-level input: {missing[0]}")
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input-filelist", type=Path)
    source.add_argument("--input-glob")
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--output-prefix", type=Path, required=True)
    args = parser.parse_args()
    if args.input_glob and args.data_dir is None:
        parser.error("--data-dir is required with --input-glob")
    paths = _load_input_paths(
        data_dir=args.data_dir,
        input_glob=args.input_glob,
        input_filelist=args.input_filelist,
    )
    columns = {
        "day",
        "client_order_id",
        "side",
        "filled",
        "fill_ts",
        "filled_qty",
        "avg_fill_price",
    }
    frames = []
    for path in paths:
        frame = pd.read_csv(path, usecols=lambda name: name in columns, low_memory=False)
        frame = frame[pd.to_numeric(frame["filled"], errors="coerce").fillna(0).gt(0)]
        frames.append(frame)
    fills = pd.concat(frames, ignore_index=True, sort=False)
    lots = pd.concat(
        [reconstruct_lifetimes(fills, matching=matching) for matching in ("fifo", "lifo")],
        ignore_index=True,
    )
    summary = summarize_lifetimes(lots)
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    lots.to_csv(args.output_prefix.with_suffix(".lots.csv"), index=False)
    summary.to_csv(args.output_prefix.with_suffix(".summary.csv"), index=False)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "input_files": len(paths),
        "input_filelist": str(args.input_filelist) if args.input_filelist else "",
        "input_filelist_sha256": (
            hashlib.sha256(args.input_filelist.read_bytes()).hexdigest()
            if args.input_filelist
            else ""
        ),
        "unique_fill_events": int(fills.drop_duplicates(["day", "client_order_id"]).shape[0]),
        "warning": "Inventory is fungible; FIFO/LIFO are attribution conventions, not observed exchange truth.",
    }
    args.output_prefix.with_suffix(".json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
