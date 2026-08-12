#!/usr/bin/env python3
"""Calibrate causal P3 touch probability from exact trade/BBO windows.

P3 answers whether aggressive flow reaches a quote at distance ``delta``
within a fixed horizon. It intentionally does not model queue-ahead depletion;
that second stage belongs to queue calibration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f02_empirical_p3_touch.fill_probability import FillProbabilityModel

SCHEMA_VERSION = "narrowgate_p3_touch_calibration.v2"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_input_roots(args: argparse.Namespace) -> tuple[Path, Path]:
    data_root = args.data_root.expanduser().resolve()
    bbo_root = getattr(args, "bbo_root", None)
    trade_root = getattr(args, "trade_root", None)
    return (
        (
            bbo_root
            or data_root / "normalized_l2_100ms_v2" / "bbo"
        ).expanduser().resolve(),
        (trade_root or data_root / "raw").expanduser().resolve(),
    )


def _input_identity(rows: list[dict[str, Any]]) -> str:
    payload = sorted(
        (
            {
                "kind": str(row["kind"]),
                "path": str(row["path"]),
                "sha256": str(row["sha256"]),
            }
            for row in rows
        ),
        key=lambda row: (row["kind"], row["path"], row["sha256"]),
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _timestamp_ms(values: pd.Series) -> np.ndarray:
    out = pd.to_numeric(values, errors="coerce").to_numpy(dtype=np.float64)
    finite = out[np.isfinite(out)]
    if finite.size and float(np.median(finite)) < 1e11:
        out *= 1000.0
    return out.astype(np.int64)


def _buyer_maker(values: pd.Series) -> np.ndarray:
    if pd.api.types.is_bool_dtype(values):
        return values.to_numpy(dtype=bool)
    return values.astype(str).str.lower().isin({"true", "1", "t", "yes"}).to_numpy()


def _day_start_ms(day: str) -> int:
    return int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)


def window_reaches(
    *,
    day: str,
    bbo_path: Path,
    trade_path: Path,
    horizon_s: float,
    max_bbo_age_ms: int,
) -> dict[str, np.ndarray]:
    """Return BUY/SELL reachable distances for non-overlapping causal windows."""
    horizon_ms = int(round(float(horizon_s) * 1000.0))
    if horizon_ms <= 0 or 86_400_000 % horizon_ms:
        raise ValueError("horizon must be positive and divide one UTC day exactly")

    bbo = pd.read_parquet(
        bbo_path,
        columns=["timestamp", "best_bid", "best_ask"],
    ).dropna()
    trades = pd.read_csv(
        trade_path,
        usecols=["price", "transact_time", "is_buyer_maker"],
    ).dropna(subset=["price", "transact_time"])
    bbo_ts = _timestamp_ms(bbo["timestamp"])
    order = np.argsort(bbo_ts, kind="stable")
    bbo_ts = bbo_ts[order]
    bids = pd.to_numeric(bbo["best_bid"], errors="coerce").to_numpy(dtype=np.float64)[order]
    asks = pd.to_numeric(bbo["best_ask"], errors="coerce").to_numpy(dtype=np.float64)[order]

    n_windows = 86_400_000 // horizon_ms
    day_start = _day_start_ms(day)
    starts = day_start + np.arange(n_windows, dtype=np.int64) * horizon_ms
    bbo_idx = np.searchsorted(bbo_ts, starts, side="right") - 1
    safe_idx = np.clip(bbo_idx, 0, max(len(bbo_ts) - 1, 0))
    valid_book = (
        (bbo_idx >= 0)
        & np.isfinite(bids[safe_idx])
        & np.isfinite(asks[safe_idx])
        & ((starts - bbo_ts[safe_idx]) >= 0)
        & ((starts - bbo_ts[safe_idx]) <= int(max_bbo_age_ms))
    )

    trade_ts = _timestamp_ms(trades["transact_time"])
    prices = pd.to_numeric(trades["price"], errors="coerce").to_numpy(dtype=np.float64)
    maker = _buyer_maker(trades["is_buyer_maker"])
    trade_bins = (trade_ts - day_start) // horizon_ms
    in_day = (
        (trade_bins >= 0)
        & (trade_bins < n_windows)
        & np.isfinite(prices)
    )

    min_sell = np.full(n_windows, np.inf, dtype=np.float64)
    max_buy = np.full(n_windows, -np.inf, dtype=np.float64)
    sell_rows = in_day & maker
    buy_rows = in_day & ~maker
    np.minimum.at(min_sell, trade_bins[sell_rows].astype(np.int64), prices[sell_rows])
    np.maximum.at(max_buy, trade_bins[buy_rows].astype(np.int64), prices[buy_rows])

    buy_reach = np.full(n_windows, -np.inf, dtype=np.float64)
    sell_reach = np.full(n_windows, -np.inf, dtype=np.float64)
    buy_touch = valid_book & np.isfinite(min_sell)
    sell_touch = valid_book & np.isfinite(max_buy)
    buy_reach[buy_touch] = bids[safe_idx[buy_touch]] - min_sell[buy_touch]
    sell_reach[sell_touch] = max_buy[sell_touch] - asks[safe_idx[sell_touch]]
    return {
        "BUY": buy_reach[valid_book],
        "SELL": sell_reach[valid_book],
        "book_age_ms": (starts[valid_book] - bbo_ts[safe_idx[valid_book]]).astype(np.float64),
    }


def survival_curve(reaches: np.ndarray, delta_grid: np.ndarray) -> np.ndarray:
    ordered = np.sort(np.asarray(reaches, dtype=np.float64))
    if ordered.size == 0:
        return np.zeros_like(delta_grid, dtype=np.float64)
    first = np.searchsorted(ordered, delta_grid, side="left")
    return (ordered.size - first).astype(np.float64) / float(ordered.size)


def _curve_summary(reaches: dict[str, list[np.ndarray]], grid: np.ndarray) -> dict[str, Any]:
    by_side: dict[str, Any] = {}
    pooled_parts: list[np.ndarray] = []
    for side in ("BUY", "SELL"):
        values = np.concatenate(reaches.get(side) or [np.array([], dtype=np.float64)])
        pooled_parts.append(values)
        curve = survival_curve(values, grid)
        by_side[side] = {
            "windows": int(values.size),
            "touch_at_best_rate": float(np.mean(values >= 0.0)) if values.size else 0.0,
            "probability_grid": curve.tolist(),
        }
    pooled = np.concatenate(pooled_parts)
    return {
        "windows": int(pooled.size),
        "probability_grid": survival_curve(pooled, grid).tolist(),
        "by_side": by_side,
    }


def calibrate(args: argparse.Namespace) -> dict[str, Any]:
    if not np.isclose(float(args.horizon_s), 10.0, rtol=0.0, atol=1e-12):
        raise ValueError("formal F02 P3 calibration requires horizon_s=10")
    meta_path = args.model_meta.expanduser().resolve()
    model_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    split_days = model_meta.get("feature_panel_split") or {}
    required = ("train", "validation", "test")
    if any(not split_days.get(name) for name in required):
        raise ValueError(f"model metadata lacks chronological split days: {meta_path}")

    bbo_root, trade_root = _resolve_input_roots(args)
    grid = np.arange(
        float(args.distance_min),
        float(args.distance_max) + float(args.distance_step) * 0.5,
        float(args.distance_step),
        dtype=np.float64,
    )
    split_reaches: dict[str, dict[str, list[np.ndarray]]] = {}
    input_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    for split in required:
        split_reaches[split] = {"BUY": [], "SELL": []}
        for day in split_days[split]:
            bbo_path = bbo_root / f"{args.symbol}-bbo-{day}.parquet"
            trade_path = trade_root / f"{args.symbol}-aggTrades-{day}.csv"
            if not bbo_path.is_file() or not trade_path.is_file():
                raise FileNotFoundError(f"missing exact P3 input for {day}: {bbo_path} / {trade_path}")
            reach = window_reaches(
                day=day,
                bbo_path=bbo_path,
                trade_path=trade_path,
                horizon_s=args.horizon_s,
                max_bbo_age_ms=args.max_bbo_age_ms,
            )
            for side in ("BUY", "SELL"):
                split_reaches[split][side].append(reach[side])
                daily_rows.append({
                    "split": split,
                    "day": day,
                    "side": side,
                    "windows": int(reach[side].size),
                    "touch_at_best_rate": float(np.mean(reach[side] >= 0.0)),
                    "touch_10usd_rate": float(np.mean(reach[side] >= 10.0)),
                    "touch_20usd_rate": float(np.mean(reach[side] >= 20.0)),
                })
            input_rows.extend([
                {"kind": "bbo", "path": str(bbo_path), "sha256": _sha256(bbo_path)},
                {"kind": "trade", "path": str(trade_path), "sha256": _sha256(trade_path)},
            ])

    summaries = {
        split: _curve_summary(reaches, grid)
        for split, reaches in split_reaches.items()
    }
    train_curve = np.asarray(summaries["train"]["probability_grid"], dtype=np.float64)
    train_curve = np.minimum.accumulate(np.clip(train_curve, 0.0, 1.0))
    bbo_input_rows = [row for row in input_rows if row["kind"] == "bbo"]
    trade_input_rows = [row for row in input_rows if row["kind"] == "trade"]
    input_identity = _input_identity(input_rows)
    input_manifest = {
        "roots": {
            "bbo": str(bbo_root),
            "trade": str(trade_root),
        },
        "file_counts": {
            "bbo": len(bbo_input_rows),
            "trade": len(trade_input_rows),
            "total": len(input_rows),
        },
        "hashes": {
            "bbo_input_identity_sha256": _input_identity(bbo_input_rows),
            "trade_input_identity_sha256": _input_identity(trade_input_rows),
            "combined_input_identity_sha256": input_identity,
        },
    }
    metadata = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": args.symbol,
        "event_type": "touch",
        "horizon_s": float(args.horizon_s),
        "distance_unit": "USDC_per_BTC",
        "distance_origin": "same_side_best_bid_or_ask_at_window_start",
        "touch_source": "side-correct Binance aggTrades against causal last-known BBO",
        "queue_included": False,
        "windowing": "non_overlapping_utc",
        "max_bbo_age_ms": int(args.max_bbo_age_ms),
        "fit_days": list(split_days["train"]),
        "validation_days": list(split_days["validation"]),
        "test_days": list(split_days["test"]),
        "model_meta_path": str(meta_path),
        "model_meta_sha256": _sha256(meta_path),
        "input_identity_sha256": input_identity,
        "input_manifest": input_manifest,
        "split_summaries": summaries,
    }
    model = FillProbabilityModel(
        model_type="empirical_survival",
        delta_grid=grid.tolist(),
        probability_grid=train_curve.tolist(),
        schema_version=SCHEMA_VERSION,
        metadata=metadata,
    )
    delta_star = model.optimal_delta(delta_max=float(args.distance_max))
    kappa_eff = model.effective_kappa(delta_star)
    metadata["delta_star"] = delta_star
    metadata["kappa_eff"] = kappa_eff
    metadata["probability_at_delta_star"] = float(model.prob(delta_star))
    metadata["validation_probability_at_delta_star"] = float(np.interp(
        delta_star, grid, summaries["validation"]["probability_grid"]
    ))
    metadata["test_probability_at_delta_star"] = float(np.interp(
        delta_star, grid, summaries["test"]["probability_grid"]
    ))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    model.save(args.output)
    artifact_sha256 = _sha256(args.output)
    report = {
        "schema_version": SCHEMA_VERSION,
        "model_path": str(args.output.resolve()),
        "artifact_sha256": artifact_sha256,
        "event_type": "touch",
        "horizon_s": 10.0,
        "distance_unit": "USDC_per_BTC",
        "delta_star": delta_star,
        "kappa_eff": kappa_eff,
        "metadata": metadata,
        "input_manifest": input_manifest,
        "inputs": input_rows,
    }
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    args.daily_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(daily_rows).to_csv(args.daily_csv, index=False)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--bbo-root",
        type=Path,
        help="Explicit BBO directory; defaults to <data-root>/bbo.",
    )
    parser.add_argument(
        "--trade-root",
        type=Path,
        help="Explicit aggTrades directory; defaults to <data-root>/raw.",
    )
    parser.add_argument("--model-meta", type=Path, required=True)
    parser.add_argument("--horizon-s", type=float, default=10.0)
    parser.add_argument("--max-bbo-age-ms", type=int, default=5_000)
    parser.add_argument("--distance-min", type=float, default=0.1)
    parser.add_argument("--distance-max", type=float, default=120.0)
    parser.add_argument("--distance-step", type=float, default=0.1)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--daily-csv", type=Path, required=True)
    args = parser.parse_args()
    report = calibrate(args)
    print(json.dumps({
        "schema_version": report["schema_version"],
        "delta_star": report["delta_star"],
        "kappa_eff": report["kappa_eff"],
        "model_path": report["model_path"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
