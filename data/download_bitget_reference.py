#!/usr/bin/env python3
"""Download Bitget BTCUSDT public trades for retained UTC good days.

The public REST endpoint is deterministic and does not require credentials, but
Bitget limits it to the recent 90-day window.  Older retained days are reported
as ``archive_required`` instead of being silently skipped.  They can then be
filled from Bitget's History Data Download page and normalized with the same
daily schema in a later import step.

Only days present in ``--manifest`` are requested.  This preserves NarrowGate's
retained-good-day boundary and avoids rebuilding a broad, contaminated date
range merely because another venue was added.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_fusion import (
    BITGET_VENUE,
    PERP_MARKET,
    SPOT_MARKET,
    market_key,
    normalize_symbol,
)


PERP_API_URL = "https://api.bitget.com/api/v2/mix/market/fills-history"
SPOT_API_URL = "https://api.bitget.com/api/v2/spot/market/fills-history"
ARCHIVE_URL = "https://www.bitget.com/data-download"
DEFAULT_LOOKBACK_DAYS = 90
CSV_COLUMNS = (
    "venue",
    "market_id",
    "symbol",
    "product_type",
    "trade_id",
    "exchange_event_ts_ms",
    "price",
    "size",
    "taker_side",
)


def _utc_day_bounds(day: date) -> tuple[int, int]:
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    start_ms = int(start.timestamp() * 1000)
    return start_ms, start_ms + 86_400_000 - 1


def _parse_day(value: str) -> date:
    return date.fromisoformat(str(value).strip()[:10])


def load_manifest(path: Path) -> list[date]:
    with Path(path).expanduser().open(newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ValueError(f"{path}: empty retained-day manifest")
    column = "day" if "day" in rows[0] else next(iter(rows[0]))
    days = sorted({_parse_day(row[column]) for row in rows if row.get(column)})
    if not days:
        raise ValueError(f"{path}: no parseable UTC days")
    return days


def api_eligible(day: date, *, now: Optional[date] = None, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> bool:
    current = now or datetime.now(timezone.utc).date()
    return current - timedelta(days=max(1, lookback_days)) <= day <= current


@dataclass
class DownloadResult:
    day: str
    status: str
    rows: int = 0
    pages: int = 0
    min_ts_ms: int = 0
    max_ts_ms: int = 0
    path: str = ""
    message: str = ""

    def as_dict(self) -> dict:
        return self.__dict__.copy()


class BitgetApiError(RuntimeError):
    def __init__(self, code: str, message: str, status_code: int = 0):
        super().__init__(f"Bitget {code}: {message}")
        self.code = str(code)
        self.status_code = int(status_code)


class SharedRateLimiter:
    """Process-local token spacing shared by all day workers."""

    def __init__(self, requests_per_second: float):
        self._gap_s = 1.0 / max(0.1, float(requests_per_second))
        self._next_at = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            sleep_s = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self._gap_s
        if sleep_s > 0:
            time.sleep(sleep_s)


class BitgetTradeDownloader:
    def __init__(
        self,
        *,
        symbol: str,
        product_type: str,
        instrument_type: str = PERP_MARKET,
        out_dir: Path,
        requests_per_second: float = 8.0,
        timeout_s: float = 30.0,
        session: Optional[requests.Session] = None,
        rate_limiter: Optional[SharedRateLimiter] = None,
    ):
        self.symbol = normalize_symbol(symbol, "BTCUSDT")
        self.instrument_type = str(instrument_type).strip().lower()
        if self.instrument_type not in {PERP_MARKET, SPOT_MARKET}:
            raise ValueError("instrument_type must be perp or spot")
        self.product_type = str(product_type or "USDT-FUTURES").upper()
        self.out_dir = Path(out_dir).expanduser()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "NarrowGateMaker/bitget-reference-downloader"})
        self.timeout_s = max(1.0, float(timeout_s))
        self.min_request_gap_s = 1.0 / max(0.1, float(requests_per_second))
        self._last_request_at = 0.0
        self.rate_limiter = rate_limiter

    @property
    def market_id(self) -> str:
        return market_key(BITGET_VENUE, self.instrument_type, self.symbol)

    @property
    def api_url(self) -> str:
        return SPOT_API_URL if self.instrument_type == SPOT_MARKET else PERP_API_URL

    def _query_params(self, *, start_ms: int, end_ms: int, limit: int) -> dict:
        params = {
            "symbol": self.symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        if self.instrument_type == PERP_MARKET:
            params["productType"] = self.product_type
        return params

    def output_path(self, day: date) -> Path:
        return self.out_dir / f"bitget_{self.symbol}_trades_{day.isoformat()}.csv.gz"

    def meta_path(self, day: date) -> Path:
        return Path(str(self.output_path(day)) + ".meta.json")

    def audit_day(self, day: date) -> DownloadResult:
        final_path = self.output_path(day)
        meta_path = self.meta_path(day)
        if final_path.exists() and meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if bool(meta.get("complete")):
                    return DownloadResult(
                        day=day.isoformat(),
                        status="present",
                        rows=int(meta.get("rows", 0)),
                        pages=int(meta.get("pages", 0)),
                        min_ts_ms=int(meta.get("min_ts_ms", 0)),
                        max_ts_ms=int(meta.get("max_ts_ms", 0)),
                        path=str(final_path),
                    )
            except Exception:
                pass
        if not api_eligible(day):
            return DownloadResult(
                day=day.isoformat(),
                status="archive_required",
                path=str(final_path),
                message=f"outside Bitget public REST 90-day window; use {ARCHIVE_URL}",
            )

        start_ms, end_ms = _utc_day_bounds(day)
        try:
            payload = self._request(
                self._query_params(start_ms=start_ms, end_ms=end_ms, limit=1)
            )
        except BitgetApiError as exc:
            if exc.code == "40934":
                return DownloadResult(
                    day=day.isoformat(),
                    status="archive_required",
                    path=str(final_path),
                    message=f"outside Bitget public REST 90-day window; use {ARCHIVE_URL}",
                )
            raise
        rows = payload.get("data") or []
        return DownloadResult(
            day=day.isoformat(),
            status="available" if rows else "empty",
            path=str(final_path),
            message="public REST probe succeeded" if rows else "public REST returned no trades",
        )

    def download_day(self, day: date, *, max_pages: int = 0, overwrite: bool = False) -> DownloadResult:
        audit = self.audit_day(day)
        if audit.status == "present" and not overwrite:
            return audit
        if audit.status == "archive_required":
            return audit
        if audit.status == "empty":
            return audit

        final_path = self.output_path(day)
        temp_path = final_path.with_suffix(final_path.suffix + ".part")
        start_ms, end_ms = _utc_day_bounds(day)
        cursor: Optional[str] = None
        row_count = 0
        pages = 0
        min_ts = 0
        max_ts = 0
        last_written_trade_id = ""
        mode = "wt"
        if overwrite:
            temp_path.unlink(missing_ok=True)
            self.meta_path(day).unlink(missing_ok=True)

        with gzip.open(temp_path, mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            while True:
                params = self._query_params(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    limit=1000,
                )
                if cursor:
                    params["idLessThan"] = cursor
                payload = self._request(params)
                rows = payload.get("data") or []
                if not rows:
                    break
                pages += 1
                next_cursor = None
                for item in rows:
                    ts_ms = int(item.get("ts", 0) or 0)
                    trade_id = str(item.get("tradeId", ""))
                    if not (start_ms <= ts_ms <= end_ms) or not trade_id:
                        continue
                    if trade_id == last_written_trade_id:
                        continue
                    writer.writerow(
                        {
                            "venue": BITGET_VENUE,
                            "market_id": self.market_id,
                            "symbol": self.symbol,
                            "product_type": self.product_type,
                            "trade_id": trade_id,
                            "exchange_event_ts_ms": ts_ms,
                            "price": item.get("price", ""),
                            "size": item.get("size", ""),
                            "taker_side": str(item.get("side", "")).lower(),
                        }
                    )
                    last_written_trade_id = trade_id
                    row_count += 1
                    min_ts = ts_ms if min_ts == 0 else min(min_ts, ts_ms)
                    max_ts = max(max_ts, ts_ms)
                    if next_cursor is None or int(trade_id) < int(next_cursor):
                        next_cursor = trade_id
                if max_pages > 0 and pages >= max_pages:
                    return DownloadResult(
                        day=day.isoformat(),
                        status="partial",
                        rows=row_count,
                        pages=pages,
                        min_ts_ms=min_ts,
                        max_ts_ms=max_ts,
                        path=str(temp_path),
                        message=f"stopped at max_pages={max_pages}",
                    )
                if not next_cursor or next_cursor == cursor or len(rows) < 1000:
                    break
                cursor = next_cursor

        if row_count == 0:
            temp_path.unlink(missing_ok=True)
            return DownloadResult(day=day.isoformat(), status="empty", path=str(final_path))

        os.replace(temp_path, final_path)
        meta = {
            "complete": True,
            "source": self.api_url,
            "archive_fallback": ARCHIVE_URL,
            "venue": BITGET_VENUE,
            "market_id": self.market_id,
            "symbol": self.symbol,
            "product_type": self.product_type,
            "utc_day": day.isoformat(),
            "rows": row_count,
            "pages": pages,
            "min_ts_ms": min_ts,
            "max_ts_ms": max_ts,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }
        self.meta_path(day).write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return DownloadResult(
            day=day.isoformat(),
            status="downloaded",
            rows=row_count,
            pages=pages,
            min_ts_ms=min_ts,
            max_ts_ms=max_ts,
            path=str(final_path),
        )

    def _request(self, params: dict) -> dict:
        if self.rate_limiter is not None:
            self.rate_limiter.wait()
        else:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.min_request_gap_s:
                time.sleep(self.min_request_gap_s - elapsed)
        for attempt in range(5):
            try:
                response = self.session.get(
                    self.api_url,
                    params=params,
                    timeout=self.timeout_s,
                )
                self._last_request_at = time.monotonic()
                if response.status_code == 429:
                    time.sleep(min(30.0, 2.0 ** attempt))
                    continue
                payload = response.json()
                if str(payload.get("code")) != "00000":
                    raise BitgetApiError(
                        str(payload.get("code")),
                        str(payload.get("msg")),
                        response.status_code,
                    )
                response.raise_for_status()
                return payload
            except BitgetApiError:
                raise
            except (requests.RequestException, ValueError, RuntimeError):
                if attempt == 4:
                    raise
                time.sleep(min(30.0, 2.0 ** attempt))
        raise RuntimeError("unreachable")


def write_manifest(path: Path, rows: Iterable[DownloadResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = tuple(DownloadResult("", "").as_dict())
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_dict())
    os.replace(temp, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--product-type", default="USDT-FUTURES")
    parser.add_argument(
        "--instrument-type",
        choices=(PERP_MARKET, SPOT_MARKET),
        default=PERP_MARKET,
    )
    parser.add_argument("--execute", action="store_true", help="Download API-eligible days; default is availability audit")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--requests-per-second", type=float, default=8.0)
    parser.add_argument("--workers", type=int, default=1, help="Parallel UTC-day downloads; requests share one rate limiter")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--max-pages", type=int, default=0, help="Smoke-test limit per day; 0 means complete")
    parser.add_argument("--status-out", type=Path)
    args = parser.parse_args()

    days = load_manifest(args.manifest)
    if args.max_days > 0:
        days = days[-args.max_days :]
    limiter = SharedRateLimiter(args.requests_per_second)

    def run_day(day: date) -> DownloadResult:
        downloader = BitgetTradeDownloader(
            symbol=args.symbol,
            product_type=args.product_type,
            instrument_type=args.instrument_type,
            out_dir=args.out_dir,
            requests_per_second=args.requests_per_second,
            rate_limiter=limiter,
        )
        try:
            return (
                downloader.download_day(day, max_pages=args.max_pages, overwrite=args.overwrite)
                if args.execute
                else downloader.audit_day(day)
            )
        except Exception as exc:
            return DownloadResult(day=day.isoformat(), status="error", message=str(exc))

    results_by_day: dict[str, DownloadResult] = {}
    workers = max(1, int(args.workers))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(run_day, day): day for day in days}
        for index, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results_by_day[result.day] = result
            print(
                f"[{index:03d}/{len(days):03d}] {result.day} {result.status} "
                f"rows={result.rows:,} pages={result.pages} {result.message}",
                flush=True,
            )
            results = [results_by_day[key] for key in sorted(results_by_day)]
            status_out = args.status_out or (args.out_dir / "bitget_BTCUSDT_good_day_download_manifest.csv")
            write_manifest(status_out, results)

    results = [results_by_day[key] for key in sorted(results_by_day)]

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    print(json.dumps({"days": len(results), "status_counts": counts}, sort_keys=True))


if __name__ == "__main__":
    main()
