#!/usr/bin/env python3
"""Download retained OKX UTC+8 daily trade archives with resume support.

OKX download-page files are cut at UTC+8 midnight.  A retained UTC day D
therefore needs source files D and D+1.  This downloader derives that source
union from the retained manifest, skips targets already normalized, validates
each ZIP before atomic rename, and can relay downloads through an SSH host when
the local route to ``static.okx.com`` is unreliable.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.download_bitget_reference import load_manifest
from market_fusion import PERP_MARKET, SPOT_MARKET, normalize_symbol


DEFAULT_BASE_URL = "https://static.okx.com/cdn/okex/traderecords/trades/daily"


@dataclass(frozen=True)
class DownloadResult:
    day: str
    status: str
    bytes: int = 0
    path: str = ""
    message: str = ""


def archive_name(instrument_id: str, day: date) -> str:
    return f"{instrument_id}-trades-{day.isoformat()}.zip"


def archive_url(base_url: str, instrument_id: str, day: date) -> str:
    name = archive_name(instrument_id, day)
    return f"{base_url.rstrip('/')}/{day:%Y%m%d}/{name}?v=999"


def normalized_target(out_dir: Path, symbol: str, day: date) -> Path:
    return out_dir / f"okx_{symbol}_trades_{day.isoformat()}.csv.gz"


def required_source_days(
    retained_days: list[date], *, out_dir: Path, symbol: str
) -> list[date]:
    missing_targets = {
        day for day in retained_days if not normalized_target(out_dir, symbol, day).exists()
    }
    return sorted(missing_targets | {day + timedelta(days=1) for day in missing_targets})


def _validate_zip(path: Path, instrument_id: str) -> None:
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected one CSV member, got {len(members)}")
        if instrument_id not in Path(members[0]).name:
            raise ValueError(f"unexpected member {members[0]!r}")
        if archive.testzip() is not None:
            raise ValueError("ZIP CRC validation failed")


def _curl_command(url: str, *, relay_host: str, timeout_s: float) -> list[str]:
    curl = [
        "curl", "-L", "--fail", "--silent", "--show-error",
        "--connect-timeout", "10", "--max-time", str(max(30, int(timeout_s))),
        "--retry", "3", "--retry-delay", "1", url,
    ]
    return ["ssh", "-o", "BatchMode=yes", relay_host, *curl] if relay_host else curl


def download_one(
    day: date,
    *,
    archive_dir: Path,
    instrument_id: str,
    base_url: str,
    relay_host: str,
    timeout_s: float,
    overwrite: bool,
) -> DownloadResult:
    target = archive_dir / archive_name(instrument_id, day)
    if target.exists() and not overwrite:
        try:
            _validate_zip(target, instrument_id)
            return DownloadResult(day.isoformat(), "present", target.stat().st_size, str(target))
        except Exception:
            target.unlink(missing_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".part")
    temp.unlink(missing_ok=True)
    url = archive_url(base_url, instrument_id, day)
    try:
        started = time.monotonic()
        with temp.open("wb") as output:
            result = subprocess.run(
                _curl_command(url, relay_host=relay_host, timeout_s=timeout_s),
                stdout=output,
                stderr=subprocess.PIPE,
                timeout=max(60.0, timeout_s + 30.0),
                check=False,
            )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip())
        _validate_zip(temp, instrument_id)
        os.replace(temp, target)
        elapsed = time.monotonic() - started
        return DownloadResult(
            day.isoformat(), "downloaded", target.stat().st_size, str(target),
            f"elapsed_s={elapsed:.3f}",
        )
    except Exception as exc:
        temp.unlink(missing_ok=True)
        return DownloadResult(day.isoformat(), "error", path=str(target), message=str(exc))


def _write_status(path: Path, rows: list[DownloadResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=DownloadResult("", "").__dict__.keys())
        writer.writeheader()
        for row in sorted(rows, key=lambda item: item.day):
            writer.writerow(row.__dict__)
    os.replace(temp, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--instrument-type", choices=(PERP_MARKET, SPOT_MARKET), default=PERP_MARKET)
    parser.add_argument("--instrument-id", default="")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--relay-host", default="")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--status-out", type=Path)
    args = parser.parse_args()

    symbol = normalize_symbol(args.symbol, "BTCUSDT")
    instrument_id = str(args.instrument_id).strip().upper() or (
        "BTC-USDT-SWAP" if args.instrument_type == PERP_MARKET else "BTC-USDT"
    )
    retained = load_manifest(args.manifest)
    days = required_source_days(retained, out_dir=args.out_dir, symbol=symbol)
    status_path = args.status_out or (
        args.archive_dir / f"okx_{symbol}_{args.instrument_type}_download_status.csv"
    )
    if not days:
        _write_status(status_path, [])
        print("all retained targets are already normalized", flush=True)
        return 0

    rows: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                download_one,
                day,
                archive_dir=args.archive_dir,
                instrument_id=instrument_id,
                base_url=args.base_url,
                relay_host=str(args.relay_host).strip(),
                timeout_s=float(args.timeout_s),
                overwrite=bool(args.overwrite),
            ): day
            for day in days
        }
        for future in as_completed(futures):
            row = future.result()
            rows.append(row)
            print(
                f"{row.day} {row.status} bytes={row.bytes:,} {row.message}",
                flush=True,
            )
    _write_status(status_path, rows)
    failures = [row for row in rows if row.status == "error"]
    print(
        f"source_days={len(rows)} complete={len(rows) - len(failures)} "
        f"failures={len(failures)} status={status_path}",
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
