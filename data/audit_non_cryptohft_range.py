#!/usr/bin/env python3
"""Audit a UTC range of non-CryptoHFTData market-data sources.

This audit is deliberately a technical-availability ledger.  It never edits a
frozen good-day denominator and never promotes a day to research eligibility.
Binance Vision CSVs are checked for a valid schema, non-empty day-local first
and last events, and a SHA256 digest.  Normalized external-venue gzip files are
checked against their atomic metadata contract and gzip CRC.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent

from data_paths import data_root  # noqa: E402

SCHEMA_VERSION = "non_cryptohft_range_audit.v1"


@dataclass(frozen=True)
class SourceContract:
    source_id: str
    relative_dir: str
    filename: str
    kind: str
    timestamp_index: int | None = None
    expected_columns: int | None = None
    endpoint_tolerance_ms: int = 60_000
    allow_end_boundary: bool = False
    normalized_extra_columns: tuple[str, ...] = ()
    expected_header: str | None = None
    has_header: bool = True


@dataclass
class AuditRow:
    day: str
    source_id: str
    path: str
    status: str
    bytes: int = 0
    rows: int = 0
    min_ts_ms: int = 0
    max_ts_ms: int = 0
    sha256: str = ""
    failure_reason: str = ""


CONTRACTS = (
    SourceContract(
        "binance_btcusdc_perp_aggtrades",
        "raw",
        "BTCUSDC-aggTrades-{day}.csv",
        "binance_csv",
        5,
        7,
        expected_header=(
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"
        ),
    ),
    SourceContract(
        "binance_btcusdt_perp_aggtrades",
        "raw",
        "BTCUSDT-aggTrades-{day}.csv",
        "binance_csv",
        5,
        7,
        expected_header=(
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"
        ),
    ),
    SourceContract(
        "binance_btcusdc_perp_trades",
        "raw_trades/BTCUSDC",
        "BTCUSDC-trades-{day}.csv",
        "binance_csv",
        4,
        6,
        expected_header="id,price,qty,quote_qty,time,is_buyer_maker",
    ),
    SourceContract(
        "binance_btcusdt_perp_trades",
        "raw_trades/BTCUSDT",
        "BTCUSDT-trades-{day}.csv",
        "binance_csv",
        4,
        6,
        expected_header="id,price,qty,quote_qty,time,is_buyer_maker",
    ),
    SourceContract(
        "binance_btcusdc_spot_aggtrades",
        "raw_spot",
        "BTCUSDC-aggTrades-{day}.csv",
        "binance_csv",
        5,
        8,
        has_header=False,
    ),
    SourceContract(
        "binance_btcusdt_spot_aggtrades",
        "raw_spot",
        "BTCUSDT-aggTrades-{day}.csv",
        "binance_csv",
        5,
        8,
        has_header=False,
    ),
    SourceContract(
        "binance_btcusdc_perp_metrics",
        "raw_metrics",
        "BTCUSDC-metrics-{day}.csv",
        "binance_csv",
        0,
        8,
        360_000,
        True,
        expected_header=(
            "create_time,symbol,sum_open_interest,sum_open_interest_value,"
            "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
            "count_long_short_ratio,sum_taker_long_short_vol_ratio"
        ),
    ),
    SourceContract(
        "binance_btcusdt_perp_metrics",
        "raw_metrics",
        "BTCUSDT-metrics-{day}.csv",
        "binance_csv",
        0,
        8,
        360_000,
        True,
        expected_header=(
            "create_time,symbol,sum_open_interest,sum_open_interest_value,"
            "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
            "count_long_short_ratio,sum_taker_long_short_vol_ratio"
        ),
    ),
    SourceContract(
        "binance_usdcusdt_spot_aggtrades",
        "external_venues/binance/spot/USDCUSDT/raw",
        "USDCUSDT-aggTrades-{day}.csv",
        "binance_csv",
        5,
        8,
        has_header=False,
    ),
    SourceContract(
        "bitget_btcusdt_perp_trades",
        "external_venues/bitget/perp/BTCUSDT/trades",
        "bitget_BTCUSDT_trades_{day}.csv.gz",
        "normalized_gzip",
    ),
    SourceContract(
        "bitget_btcusdt_spot_trades",
        "external_venues/bitget/spot/BTCUSDT/trades",
        "bitget_BTCUSDT_trades_{day}.csv.gz",
        "normalized_gzip",
    ),
    SourceContract(
        "bybit_btcusdt_perp_trades",
        "external_venues/bybit/perp/BTCUSDT/trades",
        "bybit_BTCUSDT_trades_{day}.csv.gz",
        "normalized_gzip",
        normalized_extra_columns=("is_rpi_trade",),
    ),
    SourceContract(
        "bybit_btcusdt_spot_trades",
        "external_venues/bybit/spot/BTCUSDT/trades",
        "bybit_BTCUSDT_trades_{day}.csv.gz",
        "normalized_gzip",
        normalized_extra_columns=("is_rpi_trade",),
    ),
    SourceContract(
        "okx_btcusdt_perp_trades",
        "external_venues/okx/perp/BTCUSDT/trades",
        "okx_BTCUSDT_trades_{day}.csv.gz",
        "normalized_gzip",
    ),
    SourceContract(
        "okx_btcusdt_spot_trades",
        "external_venues/okx/spot/BTCUSDT/trades",
        "okx_BTCUSDT_trades_{day}.csv.gz",
        "normalized_gzip",
    ),
)


def _days(start: date, end: date) -> list[date]:
    if start > end:
        raise ValueError("start is after end")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timestamp_ms(value: str) -> int:
    text = str(value).strip()
    if "-" in text or ":" in text:
        parsed_datetime = datetime.fromisoformat(text)
        if parsed_datetime.tzinfo is None:
            parsed_datetime = parsed_datetime.replace(tzinfo=timezone.utc)
        return int(parsed_datetime.timestamp() * 1000)
    parsed = int(float(text))
    magnitude = abs(parsed)
    if magnitude >= 10**17:
        return parsed // 1_000_000
    if magnitude >= 10**14:
        return parsed // 1_000
    if magnitude >= 10**11:
        return parsed
    if magnitude >= 10**8:
        return parsed * 1_000
    raise ValueError(f"unsupported timestamp magnitude: {value!r}")


def _scan_binance_csv(
    path: Path,
    contract: SourceContract,
) -> tuple[list[str], list[str], int, str]:
    """Validate every row width while hashing the CSV in one binary pass."""
    expected_columns = contract.expected_columns
    if expected_columns is None:
        raise ValueError("Binance contract is missing expected_columns")
    digest = hashlib.sha256()
    pending = b""
    header_consumed = not contract.has_header
    first_raw: bytes | None = None
    last_raw: bytes | None = None
    rows = 0

    def consume(raw_line: bytes) -> None:
        nonlocal header_consumed, first_raw, last_raw, rows
        line = raw_line.removesuffix(b"\r")
        if not header_consumed:
            header = line.decode("utf-8-sig")
            if contract.expected_header is not None and header != contract.expected_header:
                raise ValueError(f"header mismatch: {header!r}")
            if len(header.split(",")) != expected_columns:
                raise ValueError("header column-count mismatch")
            header_consumed = True
            return
        if not line:
            raise ValueError(f"empty data row at row {rows + 1}")
        observed_columns = line.count(b",") + 1
        if observed_columns != expected_columns:
            raise ValueError(
                f"schema mismatch at data row {rows + 1}: "
                f"expected {expected_columns} columns, got {observed_columns}"
            )
        first_raw = line if first_raw is None else first_raw
        last_raw = line
        rows += 1

    with path.open("rb") as handle:
        while chunk := handle.read(8 << 20):
            digest.update(chunk)
            lines = (pending + chunk).split(b"\n")
            pending = lines.pop()
            for raw_line in lines:
                consume(raw_line)
    if pending:
        consume(pending)
    if first_raw is None or last_raw is None:
        raise ValueError("no data rows")
    first = next(csv.reader([first_raw.decode("utf-8")]))
    last = next(csv.reader([last_raw.decode("utf-8")]))
    return first, last, rows, digest.hexdigest()


def _audit_binance(path: Path, day: date, contract: SourceContract) -> AuditRow:
    row = AuditRow(day.isoformat(), contract.source_id, str(path), "invalid")
    if not path.is_file() or path.stat().st_size <= 0:
        row.status = "missing"
        row.failure_reason = "file_missing_or_empty"
        return row
    try:
        index = int(contract.timestamp_index or 0)
        first, last, rows, sha256 = _scan_binance_csv(path, contract)
        min_ts_ms = _timestamp_ms(first[index])
        max_ts_ms = _timestamp_ms(last[index])
        day_start_ms = int(
            datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000
        )
        day_end_ms = day_start_ms + 86_400_000
        if not day_start_ms <= min_ts_ms < day_end_ms:
            raise ValueError(f"first timestamp outside UTC day: {min_ts_ms}")
        max_in_day = (
            max_ts_ms <= day_end_ms if contract.allow_end_boundary else max_ts_ms < day_end_ms
        )
        if max_ts_ms < day_start_ms or not max_in_day:
            raise ValueError(f"last timestamp outside UTC day: {max_ts_ms}")
        if min_ts_ms > day_start_ms + contract.endpoint_tolerance_ms:
            raise ValueError(f"late day start: {min_ts_ms - day_start_ms}ms")
        if max_ts_ms < day_end_ms - contract.endpoint_tolerance_ms:
            raise ValueError(f"early day end: {day_end_ms - max_ts_ms}ms")
        row.status = "valid"
        row.bytes = path.stat().st_size
        row.rows = rows
        row.min_ts_ms = min_ts_ms
        row.max_ts_ms = max_ts_ms
        row.sha256 = sha256
    except Exception as exc:
        row.failure_reason = str(exc)
    return row


def _read_meta(path: Path) -> dict[str, Any]:
    return json.loads(Path(str(path) + ".meta.json").read_text(encoding="utf-8"))


NORMALIZED_HEADER = (
    "venue,market_id,symbol,product_type,trade_id,exchange_event_ts_ms,price,size,taker_side"
)


def _validated_gzip_row_count(path: Path, contract: SourceContract) -> int:
    """Consume the full gzip member, validate its schema, and count data rows."""
    with gzip.open(path, "rb") as handle:
        header = handle.readline().decode("utf-8").rstrip("\r\n")
        expected_header = NORMALIZED_HEADER
        if contract.normalized_extra_columns:
            expected_header += "," + ",".join(contract.normalized_extra_columns)
        if header != expected_header:
            raise ValueError(f"normalized schema mismatch: {header!r}")
        rows = 0
        saw_data = False
        last_byte = b"\n"
        while chunk := handle.read(4 << 20):
            saw_data = True
            rows += chunk.count(b"\n")
            last_byte = chunk[-1:]
        if saw_data and last_byte != b"\n":
            rows += 1
    return rows


def _audit_normalized(path: Path, day: date, contract: SourceContract) -> AuditRow:
    row = AuditRow(day.isoformat(), contract.source_id, str(path), "invalid")
    if not path.is_file() or path.stat().st_size <= 0:
        row.status = "missing"
        row.failure_reason = "file_missing_or_empty"
        return row
    meta_path = Path(str(path) + ".meta.json")
    if not meta_path.is_file():
        row.failure_reason = "metadata_missing"
        return row
    try:
        meta = _read_meta(path)
        if not bool(meta.get("complete")):
            raise ValueError("metadata complete is false")
        if str(meta.get("utc_day", "")) != day.isoformat():
            raise ValueError("metadata UTC day mismatch")
        rows = int(meta.get("rows", 0))
        min_ts_ms = int(meta.get("min_ts_ms", 0))
        max_ts_ms = int(meta.get("max_ts_ms", 0))
        if rows <= 0:
            raise ValueError("metadata rows is not positive")
        day_start_ms = int(
            datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000
        )
        day_end_ms = day_start_ms + 86_400_000
        if not day_start_ms <= min_ts_ms <= max_ts_ms < day_end_ms:
            raise ValueError("metadata timestamps outside UTC day")
        if min_ts_ms > day_start_ms + contract.endpoint_tolerance_ms:
            raise ValueError(f"late day start: {min_ts_ms - day_start_ms}ms")
        if max_ts_ms < day_end_ms - contract.endpoint_tolerance_ms:
            raise ValueError(f"early day end: {day_end_ms - max_ts_ms}ms")
        observed_rows = _validated_gzip_row_count(path, contract)
        if observed_rows != rows:
            raise ValueError(f"metadata row mismatch: expected {rows}, observed {observed_rows}")
        row.status = "valid"
        row.bytes = path.stat().st_size
        row.rows = rows
        row.min_ts_ms = min_ts_ms
        row.max_ts_ms = max_ts_ms
        row.sha256 = _sha256(path)
    except Exception as exc:
        row.failure_reason = str(exc)
    return row


def _audit_task(task: tuple[date, SourceContract, Path]) -> AuditRow:
    day, contract, root = task
    path = root / contract.relative_dir / contract.filename.format(day=day.isoformat())
    if contract.kind == "binance_csv":
        return _audit_binance(path, day, contract)
    return _audit_normalized(path, day, contract)


def audit_range(*, start: date, end: date, root: Path, workers: int = 4) -> list[AuditRow]:
    tasks = [(day, contract, root) for day in _days(start, end) for contract in CONTRACTS]
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        return list(executor.map(_audit_task, tasks))


def _write_outputs(rows: list[AuditRow], output_dir: Path, start: date, end: date) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "daily_source_coverage.csv"
    temp_csv = csv_path.with_suffix(".csv.tmp")
    fields = list(asdict(AuditRow("", "", "", "")).keys())
    with temp_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)
    os.replace(temp_csv, csv_path)
    by_source: dict[str, dict[str, Any]] = {}
    for contract in CONTRACTS:
        source_rows = [row for row in rows if row.source_id == contract.source_id]
        failures = [row for row in source_rows if row.status != "valid"]
        by_source[contract.source_id] = {
            "days": len(source_rows),
            "valid_days": len(source_rows) - len(failures),
            "failed_days": len(failures),
            "bytes": sum(row.bytes for row in source_rows),
            "rows": sum(row.rows for row in source_rows),
            "failure_reasons": [
                {"day": row.day, "status": row.status, "reason": row.failure_reason}
                for row in failures
            ],
        }
    summary = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "identity": "technical_availability_only_not_frozen_good_day",
        "cryptohftdata_read_or_modified": False,
        "frozen_good_day_denominator_modified": False,
        "start_day": start.isoformat(),
        "end_day": end.isoformat(),
        "day_count": len(_days(start, end)),
        "source_count": len(CONTRACTS),
        "check_count": len(rows),
        "failure_count": sum(row.status != "valid" for row in rows),
        "total_bytes": sum(row.bytes for row in rows),
        "total_rows": sum(row.rows for row in rows),
        "all_sources_valid_days": sum(
            all(row.status == "valid" for row in rows if row.day == day.isoformat())
            for day in _days(start, end)
        ),
        "sources": by_source,
        "daily_csv": str(csv_path),
    }
    json_path = output_dir / "summary.json"
    temp_json = json_path.with_suffix(".json.tmp")
    temp_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp_json, json_path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--data-root", type=Path, default=data_root(ROOT))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    rows = audit_range(
        start=args.start,
        end=args.end,
        root=args.data_root.expanduser().resolve(),
        workers=args.workers,
    )
    _write_outputs(rows, args.output_dir.expanduser().resolve(), args.start, args.end)
    failures = sum(row.status != "valid" for row in rows)
    print(
        json.dumps(
            {
                "days": len(_days(args.start, args.end)),
                "sources": len(CONTRACTS),
                "checks": len(rows),
                "failures": failures,
                "output_dir": str(args.output_dir),
            },
            sort_keys=True,
        )
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
