#!/usr/bin/env python3
"""Audit Tardis daily archive boundaries without changing good-day identity.

This is the raw-admission layer.  It verifies the downloader's content
identity, CSV schema, daily boundary coverage, local/exchange clock ordering,
and paired book-ticker/L2 availability.  Tardis incremental L2 does not expose
Binance ``U/u/pu`` IDs, so a raw-admissible day remains a provider-normalized
candidate and is not silently promoted to native-sequence or exact-queue data.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from data_paths import data_root

BOOK_TICKER = "book_ticker"
INCREMENTAL_L2 = "incremental_book_L2"
EXPECTED_HEADERS = {
    BOOK_TICKER: (
        "exchange",
        "symbol",
        "timestamp",
        "local_timestamp",
        "ask_amount",
        "ask_price",
        "bid_price",
        "bid_amount",
    ),
    INCREMENTAL_L2: (
        "exchange",
        "symbol",
        "timestamp",
        "local_timestamp",
        "is_snapshot",
        "side",
        "price",
        "amount",
    ),
}
DAY_US = 86_400 * 1_000_000


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _day_start_us(value: str) -> int:
    parsed = date.fromisoformat(value)
    return int(
        datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc).timestamp()
        * 1_000_000
    )


def _parse_row(value: str) -> list[str]:
    rows = list(csv.reader([value]))
    if len(rows) != 1:
        raise ValueError("expected one CSV row")
    return rows[0]


def _audit_download(row: Mapping[str, Any], *, boundary_tolerance_us: int) -> dict[str, Any]:
    dataset = str(row["dataset"])
    header = tuple(str(row.get("header", "")).split(","))
    first = _parse_row(str(row.get("first_data_row", "")))
    last = _parse_row(str(row.get("last_data_row", "")))
    expected = EXPECTED_HEADERS.get(dataset, ())
    schema_valid = header == expected and len(first) == len(expected) and len(last) == len(expected)
    day_start = _day_start_us(str(row["day"]))
    day_end = day_start + DAY_US
    first_ts = int(first[2]) if schema_valid else 0
    first_local = int(first[3]) if schema_valid else 0
    last_ts = int(last[2]) if schema_valid else 0
    last_local = int(last[3]) if schema_valid else 0
    exchange_symbol_valid = bool(
        schema_valid
        and first[0] == row["venue"]
        and last[0] == row["venue"]
        and first[1] == row["symbol"]
        and last[1] == row["symbol"]
    )
    # Tardis partitions by local receive day while preserving exchange time.
    # A first exchange timestamp a few milliseconds before UTC midnight is
    # therefore legitimate.  These endpoint envelopes validate the raw daily
    # partition; they are deliberately not a 100ms freshness/coverage gate.
    boundary_valid = bool(
        schema_valid
        and day_start - boundary_tolerance_us
        <= first_ts
        <= day_start + boundary_tolerance_us
        and day_end - boundary_tolerance_us
        <= last_ts
        < day_end + boundary_tolerance_us
        and day_start <= first_local < day_end
        and day_start <= last_local < day_end
    )
    endpoint_clock_valid = bool(
        schema_valid and first_local >= first_ts and last_local >= last_ts
    )
    snapshot_bootstrap_valid = bool(
        dataset != INCREMENTAL_L2 or (schema_valid and first[4].lower() == "true")
    )
    integrity_valid = bool(
        row.get("zstd_valid")
        and int(row.get("size_bytes", -1)) == int(row.get("content_length", -2))
        and int(row.get("csv_rows", 0)) > 0
        and re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256", "")))
    )
    raw_admissible = bool(
        integrity_valid
        and schema_valid
        and exchange_symbol_valid
        and boundary_valid
        and endpoint_clock_valid
        and snapshot_bootstrap_valid
    )
    return {
        "day": str(row["day"]),
        "dataset": dataset,
        "path": str(row["path"]),
        "sha256": str(row["sha256"]),
        "size_bytes": int(row["size_bytes"]),
        "csv_rows": int(row["csv_rows"]),
        "first_timestamp_us": first_ts,
        "last_timestamp_us": last_ts,
        "start_delay_us": first_ts - day_start,
        "end_gap_us": day_end - last_ts,
        "schema_valid": schema_valid,
        "exchange_symbol_valid": exchange_symbol_valid,
        "boundary_valid": boundary_valid,
        "endpoint_clock_valid": endpoint_clock_valid,
        "snapshot_bootstrap_valid": snapshot_bootstrap_valid,
        "integrity_valid": integrity_valid,
        "raw_admissible": raw_admissible,
    }


def _available_days(path: Path) -> set[str]:
    days: set[str] = set()
    if not path.exists():
        return days
    for candidate in path.iterdir():
        if candidate.name.endswith((".json", ".part")):
            continue
        match = re.search(r"(2026-\d{2}-\d{2})", candidate.name)
        if match:
            days.add(match.group(1))
    return days


def _source_coverage(project_data_root: Path) -> dict[str, set[str]]:
    external = project_data_root / "external_venues"
    return {
        "binance_individual_trades": _available_days(
            project_data_root / "raw_trades" / "BTCUSDC"
        ),
        "bitget_perp": _available_days(
            external / "bitget" / "perp" / "BTCUSDT" / "trades"
        ),
        "bitget_spot": _available_days(
            external / "bitget" / "spot" / "BTCUSDT" / "trades"
        ),
        "bybit_perp": _available_days(
            external / "bybit" / "perp" / "BTCUSDT" / "trades"
        ),
        "bybit_spot": _available_days(
            external / "bybit" / "spot" / "BTCUSDT" / "trades"
        ),
        "okx_perp": _available_days(
            external / "okx" / "perp" / "BTCUSDT" / "trades"
        ),
        "okx_spot": _available_days(
            external / "okx" / "spot" / "BTCUSDT" / "trades"
        ),
    }


def audit_manifest(
    manifest_path: Path,
    *,
    candidate_days: set[str],
    project_data_root: Path,
    boundary_tolerance_us: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        raise RuntimeError("Tardis download manifest is not complete")
    downloads = list(manifest.get("downloads", []))
    required_fields = {
        "header",
        "first_data_row",
        "last_data_row",
        "csv_rows",
        "sha256",
        "zstd_valid",
    }
    if any(not required_fields.issubset(row) for row in downloads):
        raise RuntimeError("manifest predates boundary-aware zstd validation; rerun admission")
    audited = [
        _audit_download(row, boundary_tolerance_us=boundary_tolerance_us)
        for row in downloads
    ]
    by_key = {(row["day"], row["dataset"]): row for row in audited}
    source_days = _source_coverage(project_data_root)
    output_rows: list[dict[str, Any]] = []
    for day in sorted(candidate_days):
        ticker = by_key.get((day, BOOK_TICKER))
        l2 = by_key.get((day, INCREMENTAL_L2))
        row: dict[str, Any] = {
            "day": day,
            "book_ticker_present": ticker is not None,
            "incremental_l2_present": l2 is not None,
            "book_ticker_raw_admissible": bool(ticker and ticker["raw_admissible"]),
            "incremental_l2_raw_admissible": bool(l2 and l2["raw_admissible"]),
            "tardis_pair_raw_admissible": bool(
                ticker
                and l2
                and ticker["raw_admissible"]
                and l2["raw_admissible"]
            ),
            "native_binance_sequence_ids_present": False,
            "exact_queue_policy_eligible": False,
        }
        for source, available in source_days.items():
            row[f"{source}_present"] = day in available
        row["all_trade_sources_present"] = all(
            row[f"{source}_present"] for source in source_days
        )
        row["raw_repair_ready"] = bool(
            row["tardis_pair_raw_admissible"] and row["all_trade_sources_present"]
        )
        if ticker:
            row.update(
                {
                    "book_ticker_sha256": ticker["sha256"],
                    "book_ticker_rows": ticker["csv_rows"],
                    "book_ticker_start_delay_us": ticker["start_delay_us"],
                    "book_ticker_end_gap_us": ticker["end_gap_us"],
                }
            )
        if l2:
            row.update(
                {
                    "incremental_l2_sha256": l2["sha256"],
                    "incremental_l2_rows": l2["csv_rows"],
                    "incremental_l2_start_delay_us": l2["start_delay_us"],
                    "incremental_l2_end_gap_us": l2["end_gap_us"],
                }
            )
        output_rows.append(row)
    frame = pd.DataFrame(output_rows)
    summary = {
        "schema_version": "narrowgate.tardis_raw_repair_audit.v1",
        "manifest": str(manifest_path.resolve()),
        "raw_partition_boundary_tolerance_us": boundary_tolerance_us,
        "raw_partition_boundary_is_coverage_gate": False,
        "candidate_days": len(frame),
        "tardis_pair_raw_admissible": int(frame["tardis_pair_raw_admissible"].sum()),
        "all_trade_sources_present": int(frame["all_trade_sources_present"].sum()),
        "raw_repair_ready": int(frame["raw_repair_ready"].sum()),
        "native_binance_sequence_ids_present": False,
        "exact_queue_policy_eligible": False,
        "interpretation": (
            "Raw repair readiness does not promote a day into the native-sequence "
            "or exact-queue denominator. Normalized 100ms reconstruction, internal "
            "gap diagnostics, cross-channel BBO checks, and the frozen multi-source "
            "good-day gate remain required."
        ),
    }
    return frame, summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--candidate-days", type=Path, required=True)
    parser.add_argument("--project-data-root", type=Path, default=data_root())
    parser.add_argument("--boundary-tolerance-ms", type=int, default=5_000)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    candidate = set(pd.read_csv(args.candidate_days)["day"].astype(str))
    frame, summary = audit_manifest(
        args.manifest,
        candidate_days=candidate,
        project_data_root=args.project_data_root.expanduser().resolve(),
        boundary_tolerance_us=int(args.boundary_tolerance_ms) * 1_000,
    )
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output_csv, index=False)
    _atomic_json(summary, args.output_json)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
