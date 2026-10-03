#!/usr/bin/env python3
"""Download retained daily Binance Vision datasets through one implementation.

Supported datasets:

* ``aggTrades`` for reference markets only (not BTCUSDC USD-M execution);
* raw ``trades`` for one or more USD-M futures symbols;
* USD-M futures ``metrics``.

Default downloads publish daily Parquet under the raw root after verified
conversion. Explicit CSV output overrides remain available for ingestion
compatibility. Transport, checksums and acquisition concurrency are shared.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import daily_market_path, data_root, raw_data_root  # noqa: E402
from market_fusion import (  # noqa: E402
    PERP_MARKET,
    SPOT_MARKET,
    market_raw_dir,
    normalize_symbol,
)

DATASETS = ("aggTrades", "trades", "metrics")


@dataclass(frozen=True)
class DownloadJob:
    symbol: str
    day: str
    dataset: str
    url: str
    filename: str
    target_dir: Path
    canonical_output: Path | None = None
    auxiliary_exchange: str | None = None


def parse_day(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def retained_manifest_days(path: Path) -> list[str]:
    with path.expanduser().open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    days = sorted(
        {
            str(row.get("day") or row.get("date") or "").strip()[:10]
            for row in rows
        }
    )
    days = [day for day in days if day]
    if not days:
        raise ValueError(f"{path}: no day/date values")
    for day in days:
        parse_day(day)
    return days


def date_tags(
    *,
    days: int,
    start: str | None,
    end: str | None,
    retained_manifest: Path | None,
) -> list[str]:
    if retained_manifest is not None:
        if start or end:
            raise argparse.ArgumentTypeError(
                "--retained-manifest cannot be combined with a date range"
            )
        return retained_manifest_days(retained_manifest)
    if end and not start:
        raise argparse.ArgumentTypeError("--day-end/--end requires --day-start/--start")
    if start:
        current = parse_day(start)
        final = parse_day(end) if end else current + timedelta(days=max(1, days) - 1)
        if current > final:
            raise argparse.ArgumentTypeError(f"start {start} is after end {end}")
        values: list[str] = []
        while current <= final:
            values.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return values
    today = datetime.utcnow()
    return [
        (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(1, max(1, days) + 1)
    ]


def base_url(*, dataset: str, market_type: str) -> str:
    if dataset == "metrics":
        return "https://data.binance.vision/data/futures/um/daily/metrics"
    market_root = (
        "https://data.binance.vision/data/spot"
        if market_type == SPOT_MARKET
        else "https://data.binance.vision/data/futures/um"
    )
    return f"{market_root}/daily/{dataset}"


def _reject_retired_aggregate(symbol: str, dataset: str, market_type: str) -> None:
    if dataset == "aggTrades" and market_type == PERP_MARKET and normalize_symbol(symbol) == "BTCUSDC":
        raise ValueError(
            "BTCUSDC perpetual native aggTrades acquisition is retired; "
            "use the current data acquisition and fact-normalization contract instead"
        )


def build_jobs(
    *,
    symbols: list[str],
    days: list[str],
    dataset: str,
    market_type: str,
    output_dir: Path | None,
) -> list[DownloadJob]:
    for symbol in symbols:
        _reject_retired_aggregate(symbol, dataset, market_type)
    root = data_root(ROOT)
    jobs: list[DownloadJob] = []
    for symbol in symbols:
        canonical = output_dir is None and market_type == PERP_MARKET and dataset in {"trades", "aggTrades"}
        auxiliary = output_dir is None and (dataset == "metrics" or (market_type == SPOT_MARKET and dataset == "aggTrades"))
        auxiliary_exchange = "binance_spot" if market_type == SPOT_MARKET else "binance_futures"
        if output_dir is not None:
            target_dir = output_dir / symbol if dataset == "trades" else output_dir
        elif canonical:
            target_dir = raw_data_root() / ".incoming" / dataset / symbol
        elif auxiliary:
            target_dir = raw_data_root() / ".incoming" / auxiliary_exchange / dataset / symbol
        elif dataset == "trades":
            target_dir = root / "raw_trades" / symbol
        elif dataset == "metrics":
            target_dir = root / "raw_metrics"
        else:
            target_dir = market_raw_dir(ROOT, market_type)
        for day in days:
            filename = f"{symbol}-{dataset}-{day}.zip"
            url = f"{base_url(dataset=dataset, market_type=market_type)}/{symbol}/{filename}"
            canonical_output = None
            if canonical:
                canonical_output = daily_market_path(day, symbol, dataset)
            elif auxiliary:
                auxiliary_root = raw_data_root() / ("history" if dataset == "metrics" else "")
                canonical_output = auxiliary_root / auxiliary_exchange / symbol / day / f"{dataset}.parquet"
            jobs.append(
                DownloadJob(
                    symbol=symbol,
                    day=day,
                    dataset=dataset,
                    url=url,
                    filename=filename,
                    target_dir=target_dir,
                    canonical_output=canonical_output,
                    auxiliary_exchange=auxiliary_exchange if auxiliary else None,
                )
            )
    return jobs


def fetch_zip(url: str, target: Path, *, verbose: bool = False) -> str:
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix(target.suffix + ".part")
    curl = shutil.which("curl")
    if curl:
        last_error = ""
        for attempt in range(1, 9):
            command = [
                curl,
                "-L",
                "--fail",
                "--retry",
                "3",
                "--retry-delay",
                "2",
                "--connect-timeout",
                "30",
                "--continue-at",
                "-",
                "--output",
                str(part),
                "-sS",
                url,
            ]
            result = subprocess.run(command, stderr=subprocess.PIPE, text=True)
            if result.returncode == 0:
                os.replace(part, target)
                return "OK"
            last_error = result.stderr.strip() or f"curl exit {result.returncode}"
            if part.exists() and part.stat().st_size == 0:
                part.unlink()
            try:
                response = requests.head(url, timeout=30, allow_redirects=True)
                if response.status_code == 404:
                    return "404"
            except requests.RequestException:
                pass
            if verbose and attempt < 8:
                print(f"[WARN] {target.name}: retry {attempt + 1}/8")
            time.sleep(min(2 * attempt, 10))
        raise RuntimeError(last_error)

    response = requests.get(url, stream=True, timeout=120)
    if response.status_code == 404:
        return "404"
    response.raise_for_status()
    with part.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1 << 20):
            if chunk:
                handle.write(chunk)
    os.replace(part, target)
    return "OK"


def maybe_download_checksum(url: str, path: Path) -> None:
    try:
        response = requests.get(url + ".CHECKSUM", timeout=30)
    except requests.RequestException:
        return
    if response.status_code == 200:
        path.write_text(response.text, encoding="utf-8")


def verify_checksum(archive: Path, checksum: Path) -> bool:
    if not checksum.exists():
        return True
    expected = checksum.read_text(encoding="utf-8").strip().split()[0]
    digest = hashlib.sha256()
    with archive.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest() == expected


def download_one(
    job: DownloadJob,
    *,
    keep_zip: bool,
    overwrite: bool,
    verbose: bool,
) -> str:
    market_type = SPOT_MARKET if "/data/spot/" in job.url else PERP_MARKET
    _reject_retired_aggregate(job.symbol, job.dataset, market_type)

    def publish_daily(source: Path) -> None:
        from data.daily_raw import convert_auxiliary_csv, convert_day_channel

        if job.auxiliary_exchange is not None:
            convert_auxiliary_csv(source, job.canonical_output, job.day,
                                  job.dataset, job.auxiliary_exchange, job.symbol)
        else:
            convert_day_channel(source, job.canonical_output, job.day, job.dataset, job.symbol)

    job.target_dir.mkdir(parents=True, exist_ok=True)
    archive = job.target_dir / job.filename
    output = job.target_dir / job.filename.replace(".zip", ".csv")
    checksum = job.target_dir / f"{job.filename}.CHECKSUM"
    if job.canonical_output is not None and job.canonical_output.is_file() and not overwrite:
        return f"[SKIP] {job.symbol}/{job.day}/{job.dataset}.parquet"
    if output.exists() and not overwrite:
        if job.canonical_output is not None:
            publish_daily(output)
            output.unlink()
            return f"[OK]   {job.symbol}/{job.day}/{job.dataset}.parquet"
        return f"[SKIP] {output.name}"
    if archive.exists() and not zipfile.is_zipfile(archive):
        archive.unlink()
    if not archive.exists():
        status = fetch_zip(job.url, archive, verbose=verbose)
        if status == "404":
            return f"[404]  {job.filename}"
    maybe_download_checksum(job.url, checksum)
    if not verify_checksum(archive, checksum):
        archive.unlink(missing_ok=True)
        checksum.unlink(missing_ok=True)
        return f"[ERR]  {job.filename}: checksum mismatch"
    try:
        with zipfile.ZipFile(archive) as zipped:
            bad_member = zipped.testzip()
            if bad_member is not None:
                raise zipfile.BadZipFile(f"CRC failed: {bad_member}")
            zipped.extractall(job.target_dir)
    except zipfile.BadZipFile as exc:
        archive.unlink(missing_ok=True)
        checksum.unlink(missing_ok=True)
        return f"[ERR]  {job.filename}: {exc}"
    if not keep_zip:
        archive.unlink(missing_ok=True)
        checksum.unlink(missing_ok=True)
    if job.canonical_output is not None:
        publish_daily(output)
        output.unlink()
        return f"[OK]   {job.symbol}/{job.day}/{job.dataset}.parquet"
    size_mb = output.stat().st_size / (1 << 20) if output.exists() else 0.0
    return f"[OK]   {output.name} ({size_mb:.1f} MB)"


def main() -> int:
    default_symbol = normalize_symbol(os.environ.get("MM_SYMBOL"), "BTCUSDC")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        choices=DATASETS,
        default="trades",
    )
    parser.add_argument("--symbol", default=default_symbol)
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument(
        "--market-type",
        choices=(PERP_MARKET, SPOT_MARKET),
        default=PERP_MARKET,
    )
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--day-start", "--start", dest="day_start")
    parser.add_argument("--day-end", "--end", dest="day_end")
    parser.add_argument("--retained-manifest", type=Path)
    parser.add_argument("--output-dir", "--out-dir", dest="output_dir", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--keep-zip", action="store_true")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing extracted files with the current Binance archive",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    dataset = args.dataset
    if dataset == "metrics" and args.market_type != PERP_MARKET:
        parser.error("metrics exists only for USD-M futures")
    symbols = args.symbols or [args.symbol]
    if dataset == "trades" and args.symbols is None and args.symbol == default_symbol:
        symbols = [default_symbol, "BTCUSDT"]
    symbols = sorted({normalize_symbol(symbol) for symbol in symbols})
    days = date_tags(
        days=args.days,
        start=args.day_start,
        end=args.day_end,
        retained_manifest=args.retained_manifest,
    )
    jobs = build_jobs(
        symbols=symbols,
        days=days,
        dataset=dataset,
        market_type=args.market_type,
        output_dir=args.output_dir.expanduser().resolve() if args.output_dir else None,
    )
    print(
        f"dataset={dataset} market={args.market_type} symbols={','.join(symbols)} "
        f"days={days[0]}..{days[-1]} files={len(jobs)}"
    )
    if args.dry_run:
        for job in jobs:
            print(f"{job.url} -> {job.target_dir}")
        return 0

    counts = {"OK": 0, "SKIP": 0, "404": 0, "ERR": 0}
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
        futures = {
            pool.submit(
                download_one,
                job,
                keep_zip=bool(args.keep_zip),
                overwrite=bool(args.overwrite),
                verbose=bool(args.verbose),
            ): job
            for job in jobs
        }
        for future in as_completed(futures):
            job = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = f"[ERR]  {job.filename}: {exc}"
            key = result.split("]", 1)[0].lstrip("[")
            counts[key] = counts.get(key, 0) + 1
            if args.verbose or key in {"404", "ERR"}:
                print(result)
    print(
        f"done ok={counts['OK']} skip={counts['SKIP']} "
        f"404={counts['404']} errors={counts['ERR']}"
    )
    return 1 if counts["ERR"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
