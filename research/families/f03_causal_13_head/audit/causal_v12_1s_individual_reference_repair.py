#!/usr/bin/env python3
"""Repair exact BTCUSDT individual-trade 1s reference artifacts.

This runner is outcome blind. It accepts only the named ORICO raw individual
trade CSV authority and publishes the parquet first, followed by its sidecar as
the admission marker. Existing aggregate-trade bars are never consulted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from data_paths import data_root
from features import preprocess

SCHEMA_VERSION = "f03_causal_v12_1s_individual_reference_repair.v1"
BAR_SCHEMA_VERSION = "binance_individual_trade_bar_1s.v1"
SOURCE_AUTHORITY = "binance_futures_official_daily_individual_trades"
SOURCE_CLOCK = "binance_exchange_trade_time_ms"
REFERENCE_SOURCE_IDENTITY = "binance_futures_reference_individual_trades_1s.v1"
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_ROOT = data_root(ROOT)
RAW_RELATIVE_ROOT = Path("raw_trades/BTCUSDT")
OUTPUT_RELATIVE_ROOT = Path("reference_bars_1s_trades_v1")
DEFAULT_MANIFEST_NAME = "causal_v12_1s_2026_native_reference_repair_v1_20260805.json"
REPAIR_DAYS = (
    "2026-04-12",
    "2026-05-07",
    "2026-05-08",
    "2026-05-10",
    "2026-05-11",
    "2026-05-14",
    "2026-05-16",
)
RAW_HEADER = "id,price,qty,quote_qty,time,is_buyer_maker"
OUTPUT_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "buy_volume",
    "sell_volume",
    "trade_count",
    "buy_count",
    "sell_count",
    "last_event_ts_ms",
    "vwap",
)


class ReferenceRepairError(ValueError):
    """Raised when raw authority or admission identity is invalid."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_day(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ReferenceRepairError(f"invalid UTC day: {value!r}") from exc
    if parsed.strftime("%Y-%m-%d") != value:
        raise ReferenceRepairError(f"non-canonical UTC day: {value!r}")
    return value


def _day_bounds_ms(day: str) -> tuple[int, int]:
    start = int(
        datetime.strptime(_canonical_day(day), "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000
    )
    return start, start + 86_400_000


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial-{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _fsync_file(temporary)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _require_exact_source(raw_root: Path, day: str) -> Path:
    expected = (raw_root / f"BTCUSDT-trades-{day}.csv").resolve()
    if not expected.is_file():
        raise ReferenceRepairError(f"exact raw individual-trade authority missing: {expected}")
    header = expected.open("r", encoding="utf-8").readline().strip()
    if header != RAW_HEADER:
        raise ReferenceRepairError(
            f"{expected.name}: exact individual-trade header mismatch: {header!r}"
        )
    return expected


def _validate_chunk(
    chunk: pd.DataFrame,
    *,
    day: str,
    previous_id: int | None,
    previous_ts: int | None,
) -> tuple[int, int]:
    if chunk.empty:
        raise ReferenceRepairError(f"{day}: empty source chunk")
    for column in ("agg_trade_id", "price", "quantity", "quote_quantity", "transact_time"):
        values = chunk[column].to_numpy()
        if not np.isfinite(values).all():
            raise ReferenceRepairError(f"{day}: non-finite raw {column}")
    if (chunk["price"] <= 0).any() or (chunk["quantity"] <= 0).any():
        raise ReferenceRepairError(f"{day}: non-positive price or quantity")
    if (chunk["quote_quantity"] < 0).any():
        raise ReferenceRepairError(f"{day}: negative quote quantity")

    ids = chunk["agg_trade_id"].to_numpy(dtype=np.int64, copy=False)
    timestamps = chunk["transact_time"].to_numpy(dtype=np.int64, copy=False)
    if previous_id is not None and ids[0] <= previous_id:
        raise ReferenceRepairError(f"{day}: trade id is not strictly increasing across chunks")
    if len(ids) > 1 and not np.all(ids[1:] > ids[:-1]):
        raise ReferenceRepairError(f"{day}: trade id is not strictly increasing")
    if previous_ts is not None and timestamps[0] < previous_ts:
        raise ReferenceRepairError(f"{day}: exchange trade time decreases across chunks")
    if len(timestamps) > 1 and not np.all(timestamps[1:] >= timestamps[:-1]):
        raise ReferenceRepairError(f"{day}: exchange trade time decreases")

    start_ms, end_ms = _day_bounds_ms(day)
    if timestamps[0] < start_ms or timestamps[-1] >= end_ms:
        raise ReferenceRepairError(f"{day}: exchange trade time falls outside UTC day")
    flags = chunk["is_buyer_maker"].astype("string").str.strip().str.lower()
    if flags.isna().any() or not flags.isin(("true", "false")).all():
        raise ReferenceRepairError(f"{day}: non-canonical is_buyer_maker value")
    return int(ids[-1]), int(timestamps[-1])


def _build_bars(raw_path: Path, *, day: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    bars_by_chunk: list[pd.DataFrame] = []
    source_rows = 0
    previous_id: int | None = None
    previous_ts: int | None = None
    first_id: int | None = None
    first_ts: int | None = None
    columns = preprocess.TRADE_COLUMNS
    dtypes = preprocess.TRADE_DTYPES
    for chunk in pd.read_csv(
        raw_path,
        names=columns,
        dtype=dtypes,
        header=0,
        chunksize=preprocess.CHUNK_SIZE,
    ):
        previous_id, previous_ts = _validate_chunk(
            chunk,
            day=day,
            previous_id=previous_id,
            previous_ts=previous_ts,
        )
        if first_id is None:
            first_id = int(chunk["agg_trade_id"].iloc[0])
            first_ts = int(chunk["transact_time"].iloc[0])
        source_rows += len(chunk)
        bars_by_chunk.append(preprocess.aggregate_to_1s_bars(chunk))
    if not bars_by_chunk or first_id is None or first_ts is None:
        raise ReferenceRepairError(f"{day}: raw authority has no trades")

    combined = pd.concat(bars_by_chunk)
    if combined.index.duplicated().any():
        combined = combined.groupby(level=0).agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "turnover": "sum",
                "buy_volume": "sum",
                "sell_volume": "sum",
                "trade_count": "sum",
                "buy_count": "sum",
                "sell_count": "sum",
                "last_event_ts_ms": "max",
                "vwap": "first",
            }
        )
        combined["vwap"] = combined["turnover"] / combined["volume"]
    combined.drop(columns=["turnover"], inplace=True)
    combined.sort_index(inplace=True)

    start_ms, end_ms = _day_bounds_ms(day)
    if tuple(combined.columns) != OUTPUT_COLUMNS:
        raise ReferenceRepairError(f"{day}: output schema drift")
    timestamps = combined.index.to_numpy(dtype=np.int64, copy=False)
    if timestamps[0] < start_ms or timestamps[-1] >= end_ms:
        raise ReferenceRepairError(f"{day}: output bar timestamp outside UTC day")
    if len(timestamps) > 1 and not np.all(timestamps[1:] > timestamps[:-1]):
        raise ReferenceRepairError(f"{day}: output timestamp is not strictly increasing")
    if not np.all(timestamps % 1_000 == 0):
        raise ReferenceRepairError(f"{day}: output timestamp is not a 1s grid")
    last_event = combined["last_event_ts_ms"].to_numpy(dtype=np.int64, copy=False)
    if not np.all((last_event >= timestamps) & (last_event < timestamps + 1_000)):
        raise ReferenceRepairError(f"{day}: last event violates [t,t+1s) bar interval")
    if int(combined["trade_count"].sum()) != source_rows:
        raise ReferenceRepairError(f"{day}: source/output trade count mismatch")
    if not np.array_equal(
        combined["buy_count"].to_numpy() + combined["sell_count"].to_numpy(),
        combined["trade_count"].to_numpy(),
    ):
        raise ReferenceRepairError(f"{day}: buy/sell count accounting mismatch")
    numeric = combined.to_numpy()
    if not all(math.isfinite(float(value)) for value in numeric.ravel()):
        raise ReferenceRepairError(f"{day}: output contains non-finite values")
    return combined, {
        "source_rows": source_rows,
        "source_first_trade_id": first_id,
        "source_last_trade_id": previous_id,
        "source_first_event_ts_ms": first_ts,
        "source_last_event_ts_ms": previous_ts,
    }


def _artifact_paths(output_root: Path, day: str) -> tuple[Path, Path]:
    parquet = output_root / f"BTCUSDT-1s-{day}.parquet"
    return parquet, parquet.with_suffix(parquet.suffix + ".meta.json")


def validate_admitted_artifact(
    *,
    parquet_path: Path,
    meta_path: Path,
    raw_path: Path,
    day: str,
) -> dict[str, Any]:
    if not parquet_path.is_file() or not meta_path.is_file():
        raise ReferenceRepairError(f"{day}: admitted parquet/sidecar pair is incomplete")
    payload = json.loads(meta_path.read_text(encoding="utf-8"))
    expected = {
        "schema_version": BAR_SCHEMA_VERSION,
        "admission_schema_version": SCHEMA_VERSION,
        "complete": True,
        "atomic_admission": True,
        "utc_day": day,
        "symbol": "BTCUSDT",
        "source_data_type": "trades",
        "source_authority": SOURCE_AUTHORITY,
        "source_clock": SOURCE_CLOCK,
        "reference_source_identity": REFERENCE_SOURCE_IDENTITY,
        "bar_interval": "[t,t+1s)",
        "causal_visible_at": "t+1s",
        "source_path": str(raw_path.resolve()),
        "source_size_bytes": raw_path.stat().st_size,
        "source_sha256": sha256_file(raw_path),
        "output_sha256": sha256_file(parquet_path),
        "rows": pq.ParquetFile(parquet_path).metadata.num_rows,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ReferenceRepairError(f"{day}: admitted metadata mismatch for {key}")
    return dict(payload)


def materialize_day(
    *,
    market_data_root: Path,
    day: str,
    rebuild_admitted: bool = False,
) -> dict[str, Any]:
    day = _canonical_day(day)
    root = market_data_root.expanduser().resolve()
    raw_root = (root / RAW_RELATIVE_ROOT).resolve()
    output_root = (root / OUTPUT_RELATIVE_ROOT).resolve()
    raw_path = _require_exact_source(raw_root, day)
    output_root.mkdir(parents=True, exist_ok=True)
    parquet_path, meta_path = _artifact_paths(output_root, day)
    if parquet_path.exists() or meta_path.exists():
        payload = validate_admitted_artifact(
            parquet_path=parquet_path,
            meta_path=meta_path,
            raw_path=raw_path,
            day=day,
        )
        if not rebuild_admitted:
            return {"day": day, "status": "reused", "metadata": payload}

    staging = output_root / f".reference-repair-{day}-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        frame, source_audit = _build_bars(raw_path, day=day)
        staged_parquet = staging / parquet_path.name
        frame.to_parquet(staged_parquet, engine="pyarrow")
        _fsync_file(staged_parquet)
        output_sha = sha256_file(staged_parquet)
        generator_path = Path(__file__).resolve()
        metadata = {
            "schema_version": BAR_SCHEMA_VERSION,
            "admission_schema_version": SCHEMA_VERSION,
            "complete": True,
            "atomic_admission": True,
            "utc_day": day,
            "symbol": "BTCUSDT",
            "source_data_type": "trades",
            "source_authority": SOURCE_AUTHORITY,
            "source_clock": SOURCE_CLOCK,
            "reference_source_identity": REFERENCE_SOURCE_IDENTITY,
            "source_path": str(raw_path),
            "source_size_bytes": raw_path.stat().st_size,
            "source_sha256": sha256_file(raw_path),
            **source_audit,
            "rows": len(frame),
            "first_bar_start_ts_ms": int(frame.index[0]),
            "last_bar_start_ts_ms": int(frame.index[-1]),
            "output_columns": list(OUTPUT_COLUMNS),
            "output_sha256": output_sha,
            "bar_interval": "[t,t+1s)",
            "causal_visible_at": "t+1s",
            "generator_path": str(generator_path),
            "generator_sha256": sha256_file(generator_path),
            "built_at": datetime.now(tz=UTC).isoformat(),
            "alternate_aggtrade_artifact_used": False,
            "economic_outcomes_read": False,
            "predictions_read": False,
        }
        staged_meta = staging / meta_path.name
        staged_meta.write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _fsync_file(staged_meta)

        os.replace(staged_parquet, parquet_path)
        _fsync_directory(output_root)
        os.replace(staged_meta, meta_path)
        _fsync_directory(output_root)
        admitted = validate_admitted_artifact(
            parquet_path=parquet_path,
            meta_path=meta_path,
            raw_path=raw_path,
            day=day,
        )
        return {"day": day, "status": "admitted", "metadata": admitted}
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def build_batch_manifest(
    *,
    market_data_root: Path,
    results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    root = market_data_root.expanduser().resolve()
    rows = []
    for result in results:
        day = str(result["day"])
        parquet, meta = _artifact_paths(root / OUTPUT_RELATIVE_ROOT, day)
        metadata = dict(result["metadata"])
        rows.append(
            {
                "day": day,
                "status": result["status"],
                "raw_path": metadata["source_path"],
                "raw_sha256": metadata["source_sha256"],
                "raw_rows": metadata["source_rows"],
                "parquet_path": str(parquet.resolve()),
                "parquet_sha256": metadata["output_sha256"],
                "parquet_rows": metadata["rows"],
                "sidecar_path": str(meta.resolve()),
                "sidecar_sha256": sha256_file(meta),
            }
        )
    identity = {
        "schema_version": SCHEMA_VERSION,
        "repair_days": list(REPAIR_DAYS),
        "source_authority": SOURCE_AUTHORITY,
        "source_clock": SOURCE_CLOCK,
        "reference_source_identity": REFERENCE_SOURCE_IDENTITY,
        "raw_root": str((root / RAW_RELATIVE_ROOT).resolve()),
        "output_root": str((root / OUTPUT_RELATIVE_ROOT).resolve()),
        "artifacts": rows,
    }
    return {
        **identity,
        "identity_sha256": canonical_sha256(identity),
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "atomic_admission": True,
        "admission_marker": "per-day .parquet.meta.json published after parquet",
        "alternate_aggtrade_artifacts_used": False,
        "frozen_panels_modified": False,
        "metrics_repair_included": False,
        "predictions_read": False,
        "economic_outcomes_read": False,
        "pnl_read": False,
        "permissions": {
            "training_authorized": False,
            "transport_scoring_authorized": False,
            "economic_replay_authorized": False,
            "live_authorized": False,
        },
    }


def run_repair(
    *,
    market_data_root: Path,
    days: Sequence[str] = REPAIR_DAYS,
    manifest_path: Path | None = None,
    rebuild_admitted: bool = False,
) -> dict[str, Any]:
    canonical_days = tuple(_canonical_day(day) for day in days)
    if canonical_days != REPAIR_DAYS:
        raise ReferenceRepairError("repair day set/order must match the frozen seven-day contract")
    root = market_data_root.expanduser().resolve()
    results = [
        materialize_day(
            market_data_root=root,
            day=day,
            rebuild_admitted=rebuild_admitted,
        )
        for day in canonical_days
    ]
    manifest = build_batch_manifest(market_data_root=root, results=results)
    output = (
        manifest_path.expanduser().resolve()
        if manifest_path is not None
        else (root / OUTPUT_RELATIVE_ROOT / "admission_manifests" / DEFAULT_MANIFEST_NAME).resolve()
    )
    _atomic_write_json(output, manifest)
    if sha256_file(output) == "":  # pragma: no cover - defensive impossibility
        raise ReferenceRepairError("empty batch manifest hash")
    return {
        "manifest_path": str(output),
        "manifest_sha256": sha256_file(output),
        "manifest": manifest,
    }


def build_plan(*, market_data_root: Path) -> dict[str, Any]:
    root = market_data_root.expanduser().resolve()
    rows = []
    for day in REPAIR_DAYS:
        raw = root / RAW_RELATIVE_ROOT / f"BTCUSDT-trades-{day}.csv"
        parquet, meta = _artifact_paths(root / OUTPUT_RELATIVE_ROOT, day)
        rows.append(
            {
                "day": day,
                "raw_path": str(raw.resolve()),
                "raw_exists": raw.is_file(),
                "raw_size_bytes": raw.stat().st_size if raw.is_file() else None,
                "parquet_path": str(parquet.resolve()),
                "parquet_exists": parquet.is_file(),
                "sidecar_path": str(meta.resolve()),
                "sidecar_exists": meta.is_file(),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "outcome_blind_plan",
        "days": rows,
        "alternate_aggtrade_artifacts_used": False,
        "predictions_read": False,
        "economic_outcomes_read": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-data-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--mode", choices=("plan", "repair"), default="plan")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument(
        "--rebuild-admitted",
        action="store_true",
        help="atomically replace only a pair that first passes exact admission validation",
    )
    args = parser.parse_args(argv)
    if args.mode == "plan":
        print(json.dumps(build_plan(market_data_root=args.market_data_root), indent=2))
        return 0
    result = run_repair(
        market_data_root=args.market_data_root,
        manifest_path=args.manifest,
        rebuild_admitted=args.rebuild_admitted,
    )
    print(json.dumps({key: value for key, value in result.items() if key != "manifest"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
