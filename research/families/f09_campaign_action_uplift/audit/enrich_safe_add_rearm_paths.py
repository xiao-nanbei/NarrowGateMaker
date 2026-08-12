#!/usr/bin/env python3
"""Attach causal shock/refill/recovery paths to a frozen rearm panel."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from models import backtest_tick as bt
from research.families.f09_campaign_action_uplift.causal_path_features import (
    CAUSAL_PATH_FEATURE_COLUMNS,
    CAUSAL_PATH_FEATURE_VERSION,
    compute_causal_path_features,
    validate_causal_path_mapping,
)

SCHEMA_VERSION = "safe_add_rearm_path_enrichment.v2"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _enrich_day(task: tuple[str, str, list[dict[str, Any]]]) -> pd.DataFrame:
    day, symbol, records = task
    bt.configure_symbol(symbol)
    trades = bt.load_aggtrades(days=[day])
    l2 = bt.load_l2_data(days=[day])
    if l2 is None:
        raise RuntimeError(f"{day}: exact L2 is required for path enrichment")
    trade_ts = trades["transact_time"].to_numpy(dtype="int64", copy=False)
    trade_qty = trades["quantity"].to_numpy(dtype="float64", copy=False)
    seller = trades["is_buyer_maker"].to_numpy(dtype=bool, copy=False)
    output_rows: list[dict[str, Any]] = []
    for row in records:
        decision_ts_ms = int(row["decision_ts_ms"])
        elapsed_ms = int(round(float(row["fill_cooldown_elapsed_ms"])))
        start_ts_ms = decision_ts_ms - elapsed_ms
        features = compute_causal_path_features(
            side=str(row["side"]),
            start_ts_ms=start_ts_ms,
            decision_ts_ms=decision_ts_ms,
            trade_ts_ms=trade_ts,
            trade_qty=trade_qty,
            is_buyer_maker=seller,
            l2_ts_ms=l2.ts_ms,
            l2_bid_px=l2.bid_px,
            l2_bid_qty=l2.bid_qty,
            l2_ask_px=l2.ask_px,
            l2_ask_qty=l2.ask_qty,
            near_levels=min(5, l2.bid_px.shape[1]),
        )
        validate_causal_path_mapping(features)
        output_rows.append(
            {
                "_row_order": int(row["_row_order"]),
                "decision_id": str(row["decision_id"]),
                **features,
            }
        )
    return pd.DataFrame(output_rows)


def enrich_panel(
    *,
    input_path: Path,
    output_path: Path,
    metadata_path: Path,
    partial_dir: Path,
    symbol: str,
    workers: int,
    refresh: bool,
) -> dict[str, Any]:
    panel = pd.read_csv(input_path)
    required = {
        "day",
        "decision_id",
        "decision_ts_ms",
        "fill_cooldown_elapsed_ms",
        "side",
    }
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ValueError(f"source panel lacks enrichment fields: {missing}")
    if panel["decision_id"].astype(str).duplicated().any():
        raise ValueError("source decision_id must be unique")
    panel["_row_order"] = range(len(panel))
    days = sorted(panel["day"].astype(str).str.slice(0, 10).unique())
    identity = {
        "schema_version": SCHEMA_VERSION,
        "feature_version": CAUSAL_PATH_FEATURE_VERSION,
        "input_panel_sha256": _sha256(input_path),
        "symbol": symbol.upper(),
        "days": days,
    }
    partial_dir.mkdir(parents=True, exist_ok=True)
    identity_path = partial_dir / "run_identity.json"
    if identity_path.exists() and not refresh:
        existing = json.loads(identity_path.read_text(encoding="utf-8"))
        if existing != identity:
            raise RuntimeError("path-enrichment partial identity differs")
    identity_path.write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    day_frames: list[pd.DataFrame] = []
    tasks: list[tuple[str, str, list[dict[str, Any]]]] = []
    for day in days:
        partial = partial_dir / f"{day}.parquet"
        if partial.exists() and not refresh:
            day_frames.append(pd.read_parquet(partial))
            continue
        records = panel[panel["day"].astype(str).str.slice(0, 10) == day][
            [
                "_row_order",
                "decision_id",
                "decision_ts_ms",
                "fill_cooldown_elapsed_ms",
                "side",
            ]
        ].to_dict("records")
        tasks.append((day, symbol.upper(), records))

    def persist(day: str, frame: pd.DataFrame) -> None:
        frame.to_parquet(partial_dir / f"{day}.parquet", index=False)
        day_frames.append(frame)

    if workers <= 1:
        for task in tasks:
            persist(task[0], _enrich_day(task))
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_enrich_day, task): task[0] for task in tasks
            }
            for future in concurrent.futures.as_completed(futures):
                persist(futures[future], future.result())

    path_frame = pd.concat(day_frames, ignore_index=True).sort_values("_row_order")
    if len(path_frame) != len(panel) or path_frame["decision_id"].duplicated().any():
        raise RuntimeError("path enrichment did not preserve one row per decision")
    stale_columns = [name for name in CAUSAL_PATH_FEATURE_COLUMNS if name in panel]
    if stale_columns:
        panel = panel.drop(columns=stale_columns)
    enriched = panel.merge(
        path_frame.drop(columns="_row_order"),
        on="decision_id",
        how="left",
        validate="one_to_one",
    ).sort_values("_row_order")
    enriched = enriched.drop(columns="_row_order")
    if enriched[list(CAUSAL_PATH_FEATURE_COLUMNS)].isna().any().any():
        raise RuntimeError("path enrichment produced missing feature values")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(output_path, index=False)
    valid = pd.to_numeric(enriched["path_feature_valid"], errors="coerce")
    summary = {
        **identity,
        "rows": int(len(enriched)),
        "valid_rows": int((valid == 1.0).sum()),
        "valid_rate": float((valid == 1.0).mean()),
        "output_panel_sha256": _sha256(output_path),
        "feature_columns": list(CAUSAL_PATH_FEATURE_COLUMNS),
        "causal_boundary": "all trade/L2 events <= decision_ts_ms",
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-panel", type=Path, required=True)
    parser.add_argument("--output-panel", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--partial-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = enrich_panel(
        input_path=args.input_panel.expanduser().resolve(),
        output_path=args.output_panel.expanduser().resolve(),
        metadata_path=args.metadata.expanduser().resolve(),
        partial_dir=args.partial_dir.expanduser().resolve(),
        symbol=args.symbol,
        workers=max(1, int(args.workers)),
        refresh=bool(args.refresh),
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
