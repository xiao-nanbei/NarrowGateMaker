#!/usr/bin/env python3
"""Download and normalize Bitget trade archives for retained UTC good days.

Bitget's History Data Download page emits one UTC+8 day as multiple numbered
ZIP parts. Each part contains one UMCBL (perp) or SPBL (spot) CSV. This
importer deliberately consumes only days listed in ``--manifest`` and writes
the same daily gzip schema as ``download_bitget_reference.py``.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import sys
import time
import zipfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.download_bitget_reference import (  # noqa: E402
    CSV_COLUMNS,
    DownloadResult,
    load_manifest,
    write_manifest,
)
from market_fusion import (  # noqa: E402
    BITGET_VENUE,
    PERP_MARKET,
    SPOT_MARKET,
    market_key,
    normalize_symbol,
)

ARCHIVE_URL = "https://www.bitget.com/data-download"
CATALOG_URL = "https://www.bitget.com/v1/statistics/public/download/getPublicDataV2"
ZIP_NAME_RE = re.compile(r"^(?P<day>\d{8})_(?P<part>\d+)(?: \(\d+\))?\.zip$")
ARCHIVE_COLUMNS = ("trade_id", "timestamp", "price", "side", "volume(quote)", "size(base)")


@dataclass(frozen=True)
class ArchivePart:
    day: date
    part: int
    path: Path


def _source_days(days: Iterable[date]) -> list[date]:
    """Bitget archive names use UTC+8 days, so each UTC day needs D and D+1."""
    return sorted({item for day in days for item in (day, date.fromordinal(day.toordinal() + 1))})


def _normalized_target_complete(out_dir: Path, symbol: str, day: date) -> bool:
    target = Path(out_dir).expanduser() / f"bitget_{symbol}_trades_{day.isoformat()}.csv.gz"
    meta_path = Path(str(target) + ".meta.json")
    if not target.exists() or not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return bool(meta.get("complete")) and str(meta.get("utc_day", "")) == day.isoformat()


def required_source_days(
    retained_days: Iterable[date], *, out_dir: Path, symbol: str
) -> list[date]:
    """Return only UTC+8 archives needed by missing normalized target days."""

    normalized_symbol = normalize_symbol(symbol, "BTCUSDT")
    missing_targets = {
        day
        for day in retained_days
        if not _normalized_target_complete(out_dir, normalized_symbol, day)
    }
    return _source_days(missing_targets)


def _catalog_parts(
    day: date,
    *,
    instrument_type: str,
    symbol: str,
    session: requests.Session,
    timeout_s: float,
) -> list[dict]:
    business_line = 1 if instrument_type == SPOT_MARKET else 2
    payload = {
        "displaySymbol": [symbol],
        "businessLine": business_line,
        "businessType": 2,
        "dateType": 1,
        "beginTimeStr": day.isoformat(),
        "endTimeStr": day.isoformat(),
    }
    response: requests.Response | None = None
    last_error: Exception | None = None
    for attempt in range(8):
        try:
            response = session.post(CATALOG_URL, json=payload, timeout=timeout_s)
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(min(30.0, 1.5 * (2**attempt)))
            continue
        if response.status_code != 429:
            response.raise_for_status()
            break
        retry_after = float(response.headers.get("Retry-After", 0) or 0)
        time.sleep(max(retry_after, min(30.0, 1.5 * (2**attempt))))
    else:
        raise RuntimeError(
            f"Bitget archive catalog failed after retries for {day}: {last_error or 'rate limited'}"
        )
    assert response is not None
    body = response.json()
    if str(body.get("code")) != "200":
        raise RuntimeError(f"Bitget archive catalog failed: {body}")
    rows = body.get("data") or []
    if not rows:
        raise FileNotFoundError(f"Bitget archive catalog has no {instrument_type} data for {day}")
    return sorted(rows, key=lambda row: str(row.get("fileName", "")))


def download_missing_archives(
    *,
    source_days: Iterable[date],
    archive_dir: Path,
    instrument_type: str,
    symbol: str,
    workers: int,
    timeout_s: float,
) -> list[Path]:
    """Fetch official ZIP parts atomically; existing valid files are reused."""
    archive_dir = Path(archive_dir).expanduser()
    archive_dir.mkdir(parents=True, exist_ok=True)

    def run(day: date) -> list[Path]:
        session = requests.Session()
        session.headers.update({"User-Agent": "NarrowGateMaker/bitget-archive-downloader"})
        rows = _catalog_parts(
            day,
            instrument_type=instrument_type,
            symbol=symbol,
            session=session,
            timeout_s=timeout_s,
        )
        outputs: list[Path] = []
        for index, row in enumerate(rows, 1):
            file_url = str(row.get("fileUrl", ""))
            if not file_url.startswith("https://"):
                raise ValueError(f"Bitget archive returned invalid URL: {file_url!r}")
            target = archive_dir / f"{day:%Y%m%d}_{index:03d}.zip"
            if target.exists():
                with zipfile.ZipFile(target) as zf:
                    if zf.testzip() is None:
                        outputs.append(target)
                        continue
            temp = Path(str(target) + ".download")
            for attempt in range(6):
                try:
                    response = session.get(file_url, stream=True, timeout=timeout_s)
                    response.raise_for_status()
                    with temp.open("wb") as fh:
                        for chunk in response.iter_content(chunk_size=1 << 20):
                            if chunk:
                                fh.write(chunk)
                    break
                except requests.RequestException:
                    temp.unlink(missing_ok=True)
                    if attempt == 5:
                        raise
                    time.sleep(min(30.0, 1.5 * (2**attempt)))
            with zipfile.ZipFile(temp) as zf:
                bad_member = zf.testzip()
                if bad_member is not None:
                    raise ValueError(f"{file_url}: corrupt member {bad_member}")
            os.replace(temp, target)
            outputs.append(target)
        return outputs

    downloaded: list[Path] = []
    source_days = sorted(set(source_days))
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = {pool.submit(run, day): day for day in source_days}
        for index, future in enumerate(as_completed(futures), 1):
            day = futures[future]
            paths = future.result()
            downloaded.extend(paths)
            print(
                f"[archive {index:03d}/{len(source_days):03d}] {day} parts={len(paths)}",
                flush=True,
            )
    return downloaded


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def discover_parts(archive_dir: Path) -> dict[date, list[ArchivePart]]:
    by_day: dict[date, dict[int, ArchivePart]] = {}
    for path in sorted(Path(archive_dir).expanduser().glob("*.zip")):
        match = ZIP_NAME_RE.match(path.name)
        if not match:
            continue
        day = datetime.strptime(match.group("day"), "%Y%m%d").date()
        part = int(match.group("part"))
        candidate = ArchivePart(day=day, part=part, path=path)
        existing = by_day.setdefault(day, {}).get(part)
        if existing is not None:
            if existing.path.stat().st_size != path.stat().st_size or _sha256(existing.path) != _sha256(path):
                raise ValueError(f"conflicting duplicate archive part: {existing.path} vs {path}")
            continue
        by_day[day][part] = candidate
    return {day: [parts[key] for key in sorted(parts)] for day, parts in by_day.items()}


class BitgetArchiveImporter:
    def __init__(
        self,
        *,
        archive_dir: Path,
        out_dir: Path,
        symbol: str,
        product_type: str,
        instrument_type: str = PERP_MARKET,
    ):
        self.archive_dir = Path(archive_dir).expanduser()
        self.out_dir = Path(out_dir).expanduser()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.symbol = normalize_symbol(symbol, "BTCUSDT")
        self.instrument_type = str(instrument_type).strip().lower()
        if self.instrument_type not in {PERP_MARKET, SPOT_MARKET}:
            raise ValueError("instrument_type must be perp or spot")
        self.product_type = str(product_type or "USDT-FUTURES").upper()
        self.parts_by_day = discover_parts(self.archive_dir)

    @property
    def market_id(self) -> str:
        return market_key(BITGET_VENUE, self.instrument_type, self.symbol)

    def output_path(self, day: date) -> Path:
        return self.out_dir / f"bitget_{self.symbol}_trades_{day.isoformat()}.csv.gz"

    def meta_path(self, day: date) -> Path:
        return Path(str(self.output_path(day)) + ".meta.json")

    def import_day(self, day: date, *, overwrite: bool = False) -> DownloadResult:
        final_path = self.output_path(day)
        meta_path = self.meta_path(day)
        if final_path.exists() and meta_path.exists() and not overwrite:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if bool(meta.get("complete")):
                return DownloadResult(
                    day=day.isoformat(), status="present", rows=int(meta.get("rows", 0)),
                    pages=int(meta.get("archive_parts", meta.get("pages", 0))),
                    min_ts_ms=int(meta.get("min_ts_ms", 0)), max_ts_ms=int(meta.get("max_ts_ms", 0)),
                    path=str(final_path),
                )

        # Bitget archive filenames use UTC+8 calendar days.  A NarrowGate UTC
        # day therefore spans the tail of archive day D and the head of D+1.
        source_days = (day, date.fromordinal(day.toordinal() + 1))
        parts_by_source_day = [(source_day, self.parts_by_day.get(source_day, [])) for source_day in source_days]
        if not all(parts for _, parts in parts_by_source_day):
            missing_days = [source_day.isoformat() for source_day, source_parts in parts_by_source_day if not source_parts]
            return DownloadResult(
                day=day.isoformat(), status="archive_missing", path=str(final_path),
                message=f"missing UTC+8 archive days: {missing_days}",
            )
        for source_day, source_parts in parts_by_source_day:
            expected = list(range(1, source_parts[-1].part + 1))
            actual = [item.part for item in source_parts]
            if actual != expected:
                missing = sorted(set(expected) - set(actual))
                return DownloadResult(
                    day=day.isoformat(), status="archive_incomplete", path=str(final_path),
                    message=f"archive day {source_day}: missing parts {missing}",
                )
        parts = [part for _, source_parts in parts_by_source_day for part in source_parts]

        temp_path = Path(str(final_path) + ".archive.part")
        temp_path.unlink(missing_ok=True)
        day_start_ms = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        day_end_ms = day_start_ms + 86_400_000 - 1
        rows = 0
        duplicate_rows = 0
        min_ts = 0
        max_ts = 0
        previous_key: tuple[int, int] | None = None

        try:
            with gzip.open(temp_path, "wt", newline="", encoding="utf-8") as out_fh:
                writer = csv.DictWriter(out_fh, fieldnames=CSV_COLUMNS)
                writer.writeheader()
                for archive_part in parts:
                    with zipfile.ZipFile(archive_part.path) as zf:
                        members = [name for name in zf.namelist() if not name.endswith("/")]
                        if len(members) != 1:
                            raise ValueError(f"{archive_part.path}: expected one CSV member, got {members}")
                        with zf.open(members[0]) as raw_fh:
                            text_fh = (line.decode("utf-8-sig") for line in raw_fh)
                            reader = csv.DictReader(text_fh)
                            if tuple(reader.fieldnames or ()) != ARCHIVE_COLUMNS:
                                raise ValueError(f"{archive_part.path}: unexpected columns {reader.fieldnames}")
                            for row in reader:
                                trade_id = int(row["trade_id"])
                                ts_ms = int(row["timestamp"])
                                if not day_start_ms <= ts_ms <= day_end_ms:
                                    continue
                                key = (ts_ms, trade_id)
                                if previous_key is not None and key < previous_key:
                                    raise ValueError(
                                        f"{archive_part.path}: non-monotonic trade sequence {key} < {previous_key}"
                                    )
                                if key == previous_key:
                                    duplicate_rows += 1
                                    continue
                                writer.writerow(
                                    {
                                        "venue": BITGET_VENUE,
                                        "market_id": self.market_id,
                                        "symbol": self.symbol,
                                        "product_type": self.product_type,
                                        "trade_id": trade_id,
                                        "exchange_event_ts_ms": ts_ms,
                                        "price": row["price"],
                                        "size": row["size(base)"],
                                        "taker_side": row["side"].lower(),
                                    }
                                )
                                previous_key = key
                                rows += 1
                                min_ts = ts_ms if min_ts == 0 else min(min_ts, ts_ms)
                                max_ts = max(max_ts, ts_ms)
            if rows == 0:
                raise ValueError(f"{day}: archive contains no trades")
            os.replace(temp_path, final_path)
            meta = {
                "complete": True,
                "source": ARCHIVE_URL,
                "source_kind": "bitget_history_data_download",
                "venue": BITGET_VENUE,
                "market_id": self.market_id,
                "symbol": self.symbol,
                "product_type": self.product_type,
                "instrument_type": self.instrument_type,
                "utc_day": day.isoformat(),
                "rows": rows,
                "archive_parts": len(parts),
                "duplicate_rows_dropped": duplicate_rows,
                "min_ts_ms": min_ts,
                "max_ts_ms": max_ts,
                "imported_at": datetime.now(timezone.utc).isoformat(),
            }
            meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return DownloadResult(
                day=day.isoformat(), status="imported", rows=rows, pages=len(parts),
                min_ts_ms=min_ts, max_ts_ms=max_ts, path=str(final_path),
                message=f"duplicate_rows_dropped={duplicate_rows}",
            )
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise


def _write_results(path: Path, results: Iterable[DownloadResult]) -> None:
    write_manifest(path, sorted(results, key=lambda item: item.day))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--product-type", default="USDT-FUTURES")
    parser.add_argument("--instrument-type", choices=("perp", "spot"), default="perp")
    parser.add_argument("--download-missing", action="store_true")
    parser.add_argument("--download-workers", type=int, default=4)
    parser.add_argument("--timeout-s", type=float, default=120.0)
    parser.add_argument("--cleanup-archives", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--status-out", type=Path)
    args = parser.parse_args()

    days = load_manifest(args.manifest)
    if args.max_days > 0:
        days = days[: args.max_days]
    if args.download_missing:
        source_days = required_source_days(
            days,
            out_dir=args.out_dir,
            symbol=args.symbol,
        )
        download_missing_archives(
            source_days=source_days,
            archive_dir=args.archive_dir,
            instrument_type=args.instrument_type,
            symbol=normalize_symbol(args.symbol, "BTCUSDT"),
            workers=args.download_workers,
            timeout_s=args.timeout_s,
        )
    importer = BitgetArchiveImporter(
        archive_dir=args.archive_dir,
        out_dir=args.out_dir,
        symbol=args.symbol,
        product_type=args.product_type,
        instrument_type=args.instrument_type,
    )
    results: list[DownloadResult] = []
    for index, day in enumerate(days, 1):
        try:
            result = importer.import_day(day, overwrite=args.overwrite)
        except Exception as exc:
            result = DownloadResult(day=day.isoformat(), status="error", message=str(exc))
        results.append(result)
        print(
            f"[{index:03d}/{len(days):03d}] {result.day} {result.status} "
            f"rows={result.rows:,} parts={result.pages} {result.message}",
            flush=True,
        )
        status_out = args.status_out or (args.out_dir / "bitget_BTCUSDT_archive_import_manifest.csv")
        _write_results(status_out, results)

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    failures = [result for result in results if result.status not in {"imported", "present"}]
    if args.cleanup_archives:
        if failures:
            print("Archive cleanup skipped because retained-day imports are incomplete", flush=True)
        else:
            for path in args.archive_dir.glob("*.zip"):
                path.unlink()
            for path in args.archive_dir.glob("*.download"):
                path.unlink()
    print(json.dumps({"days": len(results), "status_counts": counts}, sort_keys=True))
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
