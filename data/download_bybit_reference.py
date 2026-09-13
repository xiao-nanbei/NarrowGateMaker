#!/usr/bin/env python3
"""Download and normalize Bybit BTCUSDT public trades for retained UTC days.

Bybit exposes daily gzip archives at ``public.bybit.com``.  The downloader is
manifest-bound, resumable, and converts each archive into NarrowGate's common
external-trade schema before marking the day complete.  A filename is never
accepted as proof of its UTC boundary; every row timestamp is checked.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.download_bitget_reference import load_manifest
from market_fusion import BYBIT_VENUE, PERP_MARKET, SPOT_MARKET, market_key, normalize_symbol


PERP_PUBLIC_ROOT = "https://public.bybit.com/trading"
SPOT_PUBLIC_ROOT = "https://public.bybit.com/spot"
PERP_SOURCE_COLUMNS = {
    "timestamp",
    "symbol",
    "side",
    "size",
    "price",
    "trdMatchID",
}
SPOT_SOURCE_COLUMNS = {"id", "timestamp", "price", "volume", "side", "rpi"}
OUTPUT_COLUMNS = (
    "venue",
    "market_id",
    "symbol",
    "product_type",
    "trade_id",
    "exchange_event_ts_ms",
    "price",
    "size",
    "taker_side",
    "is_rpi_trade",
)


@dataclass
class DownloadResult:
    day: str
    status: str
    rows: int = 0
    bytes_downloaded: int = 0
    min_ts_ms: int = 0
    max_ts_ms: int = 0
    path: str = ""
    source_sha256: str = ""
    message: str = ""

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


def _timestamp_ms(value: str) -> int:
    try:
        return int(Decimal(str(value)) * 1000)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid Bybit timestamp: {value!r}") from exc


class BybitArchiveDownloader:
    def __init__(
        self,
        *,
        symbol: str,
        out_dir: Path,
        instrument_type: str = PERP_MARKET,
        public_root: Optional[str] = None,
        timeout_s: float = 60.0,
        retries: int = 5,
        session: Optional[requests.Session] = None,
    ):
        self.symbol = normalize_symbol(symbol, "BTCUSDT")
        self.instrument_type = str(instrument_type).strip().lower()
        if self.instrument_type not in {PERP_MARKET, SPOT_MARKET}:
            raise ValueError("instrument_type must be perp or spot")
        self.product_type = "spot" if self.instrument_type == SPOT_MARKET else "linear"
        self.out_dir = Path(out_dir).expanduser()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        default_root = SPOT_PUBLIC_ROOT if self.instrument_type == SPOT_MARKET else PERP_PUBLIC_ROOT
        self.public_root = str(public_root or default_root).rstrip("/")
        self.timeout_s = max(5.0, float(timeout_s))
        self.retries = max(1, int(retries))
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "NarrowGateMaker/bybit-reference-downloader"})

    @property
    def market_id(self) -> str:
        return market_key(BYBIT_VENUE, self.instrument_type, self.symbol)

    def source_url(self, day: date) -> str:
        separator = "_" if self.instrument_type == SPOT_MARKET else ""
        return f"{self.public_root}/{self.symbol}/{self.symbol}{separator}{day.isoformat()}.csv.gz"

    def output_path(self, day: date) -> Path:
        return self.out_dir / f"bybit_{self.symbol}_trades_{day.isoformat()}.csv.gz"

    def meta_path(self, day: date) -> Path:
        return Path(str(self.output_path(day)) + ".meta.json")

    def audit_day(self, day: date) -> DownloadResult:
        target = self.output_path(day)
        meta_path = self.meta_path(day)
        if not target.exists() or not meta_path.exists():
            return DownloadResult(day.isoformat(), "missing", path=str(target))
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception as exc:
            return DownloadResult(
                day.isoformat(), "invalid_meta", path=str(target), message=str(exc)
            )
        if not bool(meta.get("complete")):
            return DownloadResult(day.isoformat(), "incomplete", path=str(target))
        return DownloadResult(
            day=day.isoformat(),
            status="present",
            rows=int(meta.get("rows", 0)),
            bytes_downloaded=int(meta.get("bytes_downloaded", 0)),
            min_ts_ms=int(meta.get("min_ts_ms", 0)),
            max_ts_ms=int(meta.get("max_ts_ms", 0)),
            path=str(target),
            source_sha256=str(meta.get("source_sha256", "")),
        )

    def download_day(self, day: date, *, overwrite: bool = False) -> DownloadResult:
        audit = self.audit_day(day)
        if audit.status == "present" and not overwrite:
            return audit

        target = self.output_path(day)
        source_part = self.out_dir / f".{self.symbol}{day.isoformat()}.csv.gz.download"
        normalized_part = Path(str(target) + ".part")
        if overwrite:
            source_part.unlink(missing_ok=True)
            normalized_part.unlink(missing_ok=True)
            target.unlink(missing_ok=True)
            self.meta_path(day).unlink(missing_ok=True)

        bytes_downloaded, source_sha256 = self._download_archive(day, source_part)
        result = self._normalize_archive(day, source_part, normalized_part)
        result.bytes_downloaded = bytes_downloaded
        result.source_sha256 = source_sha256
        result.path = str(target)
        if result.status != "normalized":
            return result

        os.replace(normalized_part, target)
        source_part.unlink(missing_ok=True)
        meta = {
            "complete": True,
            "venue": BYBIT_VENUE,
            "market_id": self.market_id,
            "symbol": self.symbol,
            "instrument_type": self.instrument_type,
            "product_type": self.product_type,
            "utc_day": day.isoformat(),
            "source_url": self.source_url(day),
            "source_sha256": source_sha256,
            "bytes_downloaded": bytes_downloaded,
            "rows": result.rows,
            "min_ts_ms": result.min_ts_ms,
            "max_ts_ms": result.max_ts_ms,
            "normalized_at": datetime.now(timezone.utc).isoformat(),
        }
        meta_temp = Path(str(self.meta_path(day)) + ".tmp")
        meta_temp.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(meta_temp, self.meta_path(day))
        result.status = "downloaded"
        return result

    def _download_archive(self, day: date, target: Path) -> tuple[int, str]:
        url = self.source_url(day)
        last_error: Optional[Exception] = None
        for attempt in range(self.retries):
            existing = target.stat().st_size if target.exists() else 0
            headers = {"Range": f"bytes={existing}-"} if existing else {}
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    stream=True,
                    timeout=(10.0, self.timeout_s),
                )
                if response.status_code == 404:
                    raise FileNotFoundError(url)
                response.raise_for_status()
                append = existing > 0 and response.status_code == 206
                if existing > 0 and not append:
                    existing = 0
                mode = "ab" if append else "wb"
                with target.open(mode) as fh:
                    for chunk in response.iter_content(chunk_size=1 << 20):
                        if chunk:
                            fh.write(chunk)
                break
            except Exception as exc:
                last_error = exc
                if attempt + 1 >= self.retries:
                    raise
                time.sleep(min(30.0, 2.0**attempt))
        else:
            raise RuntimeError(f"failed to download {url}: {last_error}")

        digest = hashlib.sha256()
        size = 0
        with target.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                size += len(chunk)
                digest.update(chunk)
        return size, digest.hexdigest()

    def _normalize_archive(self, day: date, source: Path, target: Path) -> DownloadResult:
        day_start_ms = int(
            datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000
        )
        day_end_ms = day_start_ms + 86_400_000
        rows = 0
        min_ts = 0
        max_ts = 0
        last_trade_id = ""
        last_ts_ms = 0
        with (
            gzip.open(source, "rt", newline="", encoding="utf-8-sig") as src,
            gzip.open(target, "wt", newline="", encoding="utf-8") as dst,
        ):
            reader = csv.DictReader(src)
            required_columns = (
                SPOT_SOURCE_COLUMNS if self.instrument_type == SPOT_MARKET else PERP_SOURCE_COLUMNS
            )
            missing = required_columns - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"{source}: missing columns {sorted(missing)}")
            writer = csv.DictWriter(dst, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()
            for row in reader:
                ts_ms = (
                    int(row["timestamp"])
                    if self.instrument_type == SPOT_MARKET
                    else _timestamp_ms(row["timestamp"])
                )
                if not day_start_ms <= ts_ms < day_end_ms:
                    raise ValueError(
                        f"{source}: event {ts_ms} crosses retained UTC day {day.isoformat()}"
                    )
                trade_id = str(
                    row.get("id", "")
                    if self.instrument_type == SPOT_MARKET
                    else row.get("trdMatchID", "")
                ).strip()
                if not trade_id or trade_id == last_trade_id:
                    continue
                if ts_ms < last_ts_ms:
                    raise ValueError(
                        f"{source}: non-monotonic event timestamp {ts_ms} after {last_ts_ms}"
                    )
                side = str(row.get("side", "")).strip().lower()
                if side not in {"buy", "sell"}:
                    raise ValueError(f"{source}: invalid taker side {side!r}")
                price = float(row.get("price", 0) or 0)
                size = float(
                    row.get("volume", 0)
                    if self.instrument_type == SPOT_MARKET
                    else row.get("size", 0)
                    or 0
                )
                if price <= 0 or size <= 0:
                    continue
                writer.writerow(
                    {
                        "venue": BYBIT_VENUE,
                        "market_id": self.market_id,
                        "symbol": self.symbol,
                        "product_type": self.product_type,
                        "trade_id": trade_id,
                        "exchange_event_ts_ms": ts_ms,
                        "price": row["price"],
                        "size": str(size),
                        "taker_side": side,
                        "is_rpi_trade": int(
                            str(row.get("rpi", row.get("RPI", "0"))).strip()
                            in {"1", "true", "True"}
                        ),
                    }
                )
                rows += 1
                last_trade_id = trade_id
                last_ts_ms = ts_ms
                min_ts = ts_ms if min_ts == 0 else min(min_ts, ts_ms)
                max_ts = max(max_ts, ts_ms)
        if rows == 0:
            target.unlink(missing_ok=True)
            return DownloadResult(day.isoformat(), "empty")
        return DownloadResult(
            day=day.isoformat(), status="normalized", rows=rows, min_ts_ms=min_ts, max_ts_ms=max_ts
        )


def _write_status(path: Path, rows: list[DownloadResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(str(path) + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(DownloadResult("", "").as_dict()))
        writer.writeheader()
        for row in sorted(rows, key=lambda item: item.day):
            writer.writerow(row.as_dict())
    os.replace(temp, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--instrument-type", choices=("perp", "spot"), default="perp")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--timeout-s", type=float, default=120.0)
    parser.add_argument("--retries", type=int, default=6)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--status-out", type=Path)
    args = parser.parse_args()

    days = load_manifest(args.manifest)
    if args.max_days > 0:
        days = days[: args.max_days]
    status_out = args.status_out or args.out_dir / "bybit_BTCUSDT_retained_download_status.csv"

    def run(day: date) -> DownloadResult:
        return BybitArchiveDownloader(
            symbol=args.symbol,
            out_dir=args.out_dir,
            instrument_type=args.instrument_type,
            timeout_s=args.timeout_s,
            retries=args.retries,
        ).download_day(day, overwrite=args.overwrite)

    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
        futures = {pool.submit(run, day): day for day in days}
        for future in as_completed(futures):
            day = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = DownloadResult(day.isoformat(), "error", message=str(exc))
            results.append(result)
            _write_status(status_out, results)
            print(json.dumps(result.as_dict(), sort_keys=True), flush=True)

    failures = [row for row in results if row.status not in {"downloaded", "present"}]
    print(
        json.dumps(
            {
                "days": len(results),
                "complete": len(results) - len(failures),
                "failures": len(failures),
                "status_out": str(status_out),
            },
            sort_keys=True,
        )
    )
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
