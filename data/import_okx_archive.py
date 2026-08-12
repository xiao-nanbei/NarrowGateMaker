#!/usr/bin/env python3
"""Normalize retained OKX trade ZIPs into the external-venue daily schema.

The importer never treats an archive filename as a good-day decision.  Source
days are intersected with the supplied retained manifest, every row is checked
against its UTC boundary, and swap contract sizes are converted to base BTC.
Validated source ZIPs may be removed with ``--cleanup-source``.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import sys
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.download_bitget_reference import load_manifest
from market_fusion import OKX_VENUE, PERP_MARKET, SPOT_MARKET, market_key, normalize_symbol


DAY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
SOURCE_COLUMNS = {
    "instrument_name", "trade_id", "side", "price", "size", "created_time"
}
OUTPUT_COLUMNS = (
    "venue", "market_id", "symbol", "product_type", "trade_id",
    "exchange_event_ts_ms", "price", "size", "taker_side",
)


@dataclass
class ImportResult:
    day: str
    status: str
    rows: int = 0
    min_ts_ms: int = 0
    max_ts_ms: int = 0
    output_path: str = ""
    source_path: str = ""
    source_sha256: str = ""
    message: str = ""

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_path(archive_dir: Path, instrument_id: str, day: date) -> Path:
    return archive_dir / f"{instrument_id}-trades-{day.isoformat()}.zip"


def _source_paths(archive_dir: Path, instrument_id: str, day: date) -> tuple[Path, Path]:
    # OKX daily download files are cut at UTC+8 midnight.  A complete UTC day
    # therefore needs source day D (00:00-16:00Z) and D+1 (16:00-24:00Z).
    return (
        _source_path(archive_dir, instrument_id, day),
        _source_path(archive_dir, instrument_id, day + timedelta(days=1)),
    )


def _target_path(out_dir: Path, symbol: str, day: date) -> Path:
    return out_dir / f"okx_{symbol}_trades_{day.isoformat()}.csv.gz"


def _meta_path(target: Path) -> Path:
    return Path(str(target) + ".meta.json")


def _audit_existing(target: Path, day: date) -> ImportResult | None:
    meta_path = _meta_path(target)
    if not target.exists() or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not meta.get("complete") or str(meta.get("utc_day", "")) != day.isoformat():
        return None
    source_paths = meta.get("source_paths", [])
    source_hashes = meta.get("source_sha256", {})
    if not isinstance(source_paths, list):
        source_paths = [meta.get("source_path", "")]
    if isinstance(source_hashes, dict):
        source_hash_values = list(source_hashes.values())
    else:
        source_hash_values = [source_hashes or meta.get("source_sha256", "")]
    return ImportResult(
        day=day.isoformat(),
        status="present",
        rows=int(meta.get("rows", 0)),
        min_ts_ms=int(meta.get("min_ts_ms", 0)),
        max_ts_ms=int(meta.get("max_ts_ms", 0)),
        output_path=str(target),
        source_path="|".join(str(path) for path in source_paths if path),
        source_sha256="|".join(str(value) for value in source_hash_values if value),
    )


def import_day(
    *,
    day: date,
    archive_dir: Path,
    out_dir: Path,
    symbol: str,
    instrument_id: str,
    instrument_type: str,
    product_type: str,
    contract_multiplier: Decimal,
    source_size_unit: str,
    overwrite: bool,
    cleanup_source: bool,
) -> ImportResult:
    target = _target_path(out_dir, symbol, day)
    existing = _audit_existing(target, day)
    sources = _source_paths(archive_dir, instrument_id, day)
    if existing is not None and not overwrite:
        if cleanup_source:
            for source in sources:
                source.unlink(missing_ok=True)
        return existing
    missing_sources = [source for source in sources if not source.exists()]
    if missing_sources:
        return ImportResult(
            day=day.isoformat(), status="missing_source",
            source_path="|".join(str(path) for path in missing_sources),
        )

    source_hashes = {str(source): _sha256(source) for source in sources}
    day_start_ms = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
    day_end_ms = day_start_ms + 86_400_000
    out_dir.mkdir(parents=True, exist_ok=True)
    temp = Path(str(target) + ".part")
    rows = 0
    min_ts = 0
    max_ts = 0
    last_ts = 0
    last_trade_id = -1
    try:
        with gzip.open(temp, "wt", newline="", encoding="utf-8") as dst:
            writer = csv.DictWriter(dst, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()
            for source in sources:
                with zipfile.ZipFile(source) as archive:
                    members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                    if len(members) != 1:
                        raise ValueError(f"{source}: expected one CSV member, got {len(members)}")
                    raw = archive.open(members[0], "r")
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                    reader = csv.DictReader(text)
                    missing = SOURCE_COLUMNS.difference(reader.fieldnames or [])
                    if missing:
                        raise ValueError(f"{source}: missing OKX columns: {sorted(missing)}")
                    for source_row in reader:
                        if str(source_row.get("instrument_name", "")).upper() != instrument_id:
                            raise ValueError("archive contains unexpected instrument")
                        side = str(source_row.get("side", "")).lower()
                        if side not in {"buy", "sell"}:
                            raise ValueError(f"invalid taker side: {side!r}")
                        try:
                            ts_ms = int(source_row["created_time"])
                            trade_id = int(source_row["trade_id"])
                            price = Decimal(source_row["price"])
                            size = Decimal(source_row["size"]) * contract_multiplier
                        except (InvalidOperation, TypeError, ValueError) as exc:
                            raise ValueError(f"invalid OKX trade row near selected row {rows + 2}") from exc
                        if not day_start_ms <= ts_ms < day_end_ms:
                            continue
                        if price <= 0 or size <= 0:
                            raise ValueError("non-positive price/size")
                        if ts_ms < last_ts or (ts_ms == last_ts and trade_id < last_trade_id):
                            raise ValueError("source trades are not monotonic by timestamp/trade_id")
                        last_ts = ts_ms
                        last_trade_id = trade_id
                        writer.writerow({
                            "venue": OKX_VENUE,
                            "market_id": market_key(OKX_VENUE, instrument_type, symbol),
                            "symbol": symbol,
                            "product_type": product_type,
                            "trade_id": trade_id,
                            "exchange_event_ts_ms": ts_ms,
                            "price": str(price),
                            "size": str(size.normalize()),
                            "taker_side": side,
                        })
                        rows += 1
                        min_ts = ts_ms if not min_ts else min(min_ts, ts_ms)
                        max_ts = max(max_ts, ts_ms)
                    text.close()
        if rows == 0:
            raise ValueError("empty OKX archive")
        if min_ts > day_start_ms + 60_000 or max_ts < day_end_ms - 60_000:
            raise ValueError(
                f"incomplete UTC coverage: selected {min_ts}..{max_ts}, "
                f"expected near {day_start_ms}..{day_end_ms - 1}"
            )
        os.replace(temp, target)
        metadata = {
            "complete": True,
            "venue": OKX_VENUE,
            "market_id": market_key(OKX_VENUE, instrument_type, symbol),
            "symbol": symbol,
            "instrument_type": instrument_type,
            "instrument_id": instrument_id,
            "product_type": product_type,
            "utc_day": day.isoformat(),
            "source_paths": [str(source) for source in sources],
            "source_sha256": source_hashes,
            "source_day_timezone": "Asia/Shanghai (UTC+8)",
            "source_size_unit": source_size_unit,
            "contract_multiplier_base_btc": str(contract_multiplier),
            "normalized_size_unit": "BTC",
            "rows": rows,
            "min_ts_ms": min_ts,
            "max_ts_ms": max_ts,
            "normalized_at": datetime.now(timezone.utc).isoformat(),
        }
        meta_temp = Path(str(_meta_path(target)) + ".tmp")
        meta_temp.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(meta_temp, _meta_path(target))
        if cleanup_source:
            for source in sources:
                source.unlink(missing_ok=True)
        return ImportResult(
            day=day.isoformat(), status="imported", rows=rows,
            min_ts_ms=min_ts, max_ts_ms=max_ts, output_path=str(target),
            source_path="|".join(str(source) for source in sources),
            source_sha256="|".join(source_hashes.values()),
        )
    except Exception as exc:
        temp.unlink(missing_ok=True)
        return ImportResult(
            day=day.isoformat(), status="error", rows=rows,
            output_path=str(target),
            source_path="|".join(str(source) for source in sources), message=str(exc),
        )


def _selected_retained_days(
    retained: set[date],
    *,
    archive_dir: Path,
    out_dir: Path,
    symbol: str,
    instrument_id: str,
) -> list[date]:
    return sorted(
        day
        for day in retained
        if _target_path(out_dir, symbol, day).exists()
        or all(path.exists() for path in _source_paths(archive_dir, instrument_id, day))
    )


def _write_status(path: Path, rows: list[ImportResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(str(path) + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(ImportResult("", "").as_dict()))
        writer.writeheader()
        for row in sorted(rows, key=lambda item: item.day):
            writer.writerow(row.as_dict())
    os.replace(temp, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--instrument-type", choices=(PERP_MARKET, SPOT_MARKET), default=PERP_MARKET)
    parser.add_argument("--instrument-id", default="")
    parser.add_argument("--contract-multiplier", default="")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--cleanup-source", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--status-out", type=Path)
    args = parser.parse_args()

    archive_dir = args.archive_dir.expanduser().resolve()
    out_dir = args.out_dir.expanduser().resolve()
    symbol = normalize_symbol(args.symbol, "BTCUSDT")
    instrument_type = str(args.instrument_type).lower()
    instrument_id = str(args.instrument_id).strip().upper() or (
        "BTC-USDT-SWAP" if instrument_type == PERP_MARKET else "BTC-USDT"
    )
    product_type = "SWAP" if instrument_type == PERP_MARKET else "SPOT"
    source_size_unit = "contracts" if instrument_type == PERP_MARKET else "base_asset"
    retained = set(load_manifest(args.manifest))
    days = _selected_retained_days(
        retained,
        archive_dir=archive_dir,
        out_dir=out_dir,
        symbol=symbol,
        instrument_id=instrument_id,
    )
    if not days:
        raise SystemExit("no retained OKX source or normalized days found")
    multiplier = Decimal(str(args.contract_multiplier or ("0.01" if instrument_type == PERP_MARKET else "1.0")))
    if multiplier <= 0:
        raise SystemExit("contract multiplier must be positive")

    status_path = args.status_out or (
        out_dir.parent / "manifests" / f"okx_{symbol}_{instrument_type}_retained_available.csv"
    )
    job_kwargs = {
        "archive_dir": archive_dir,
        "out_dir": out_dir,
        "symbol": symbol,
        "instrument_id": instrument_id,
        "instrument_type": instrument_type,
        "product_type": product_type,
        "contract_multiplier": multiplier,
        "source_size_unit": source_size_unit,
        "overwrite": args.overwrite,
        "cleanup_source": False,
    }
    results: list[ImportResult] = []
    workers = max(1, int(args.workers))
    if workers == 1:
        for day in days:
            row = import_day(day=day, **job_kwargs)
            results.append(row)
            _write_status(status_path, results)
            print(f"{row.day} {row.status} rows={row.rows:,} {row.message}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(import_day, day=day, **job_kwargs): day for day in days
            }
            for future in as_completed(futures):
                row = future.result()
                results.append(row)
                _write_status(status_path, results)
                print(f"{row.day} {row.status} rows={row.rows:,} {row.message}", flush=True)
    failures = [row for row in results if row.status not in {"imported", "present"}]
    if args.cleanup_source and not failures:
        for day in days:
            for source in _source_paths(archive_dir, instrument_id, day):
                source.unlink(missing_ok=True)
    print(json.dumps({
        "retained_source_days": len(days),
        "complete": len(days) - len(failures),
        "failures": len(failures),
        "status_out": str(status_path),
    }, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
