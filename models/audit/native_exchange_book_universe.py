#!/usr/bin/env python3
"""Freeze the native exchange-book universe used by action research."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from models.exchange_book_replay import CryptoHFTExchangeBookTape

SCHEMA_VERSION = "native_exchange_book_source_universe.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame:
        raise ValueError("eligible-day CSV must contain a day column")
    days = sorted(
        {
            pd.Timestamp(value, tz="UTC").strftime("%Y-%m-%d")
            for value in frame["day"].astype(str)
        }
    )
    if not days:
        raise ValueError("eligible-day CSV contains no days")
    return days


def _load_sequence_audit(
    path: Path | None,
    *,
    symbol: str,
) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("range_audits")
    if not isinstance(rows, list):
        raise ValueError("sequence audit has no range_audits list")
    return {
        str(row["range_start_utc"])[:10]: dict(row["sequence_audit"])
        for row in rows
        if str(row.get("symbol", "")).upper()
        == str(symbol).upper()
    }


def build_native_exchange_book_universe(
    *,
    raw_root: Path,
    eligible_days_path: Path,
    raw_files_manifest_path: Path,
    sequence_audit_path: Path | None,
    split: dict[str, list[str]],
    symbol: str,
    exchange: str,
    tick_size: float,
    warmup_hours: int,
) -> dict[str, Any]:
    """Inventory complete native tapes and hash every immutable raw input."""

    raw_root = raw_root.expanduser().resolve()
    eligible_days_path = eligible_days_path.expanduser().resolve()
    sequence_audit_path = (
        sequence_audit_path.expanduser().resolve()
        if sequence_audit_path is not None
        else None
    )
    raw_files_manifest_path = raw_files_manifest_path.expanduser().resolve()
    eligible_days = _read_days(eligible_days_path)
    sequence_by_day = _load_sequence_audit(
        sequence_audit_path,
        symbol=symbol,
    )

    complete_days: list[str] = []
    excluded: dict[str, list[str]] = {}
    files_by_path: dict[str, Path] = {}
    for day in eligible_days:
        tape = CryptoHFTExchangeBookTape(
            raw_root=raw_root,
            day=day,
            symbol=symbol,
            tick_size=tick_size,
            exchange=exchange,
            warmup_hours=warmup_hours,
            strict_complete=False,
        )
        if tape.missing_paths:
            excluded[day] = [
                str(path.relative_to(raw_root))
                for path in tape.missing_paths
            ]
            continue
        if sequence_by_day:
            source_days = sorted(
                {
                    path.parent.parent.name
                    for path in tape.source_paths
                }
            )
            invalid_source_days: list[str] = []
            for source_day in source_days:
                audit = sequence_by_day.get(source_day)
                if audit is None or any(
                    int(audit.get(field, 0) or 0) > 0
                    for field in (
                        "sequence_gaps",
                        "invalid_sequence_messages",
                        "message_time_reversals",
                    )
                ):
                    invalid_source_days.append(source_day)
            if invalid_source_days:
                excluded[day] = [
                    f"sequence_unusable:{source_day}"
                    for source_day in invalid_source_days
                ]
                continue
        complete_days.append(day)
        for path in tape.source_paths:
            files_by_path[str(path.relative_to(raw_root))] = path

    normalized_split = {
        str(name): sorted({str(day)[:10] for day in days})
        for name, days in split.items()
    }
    requested = {
        day for days in normalized_split.values() for day in days
    }
    unavailable = sorted(requested - set(complete_days))
    if unavailable:
        raise ValueError(
            "frozen split contains days without complete native warmup: "
            f"{unavailable}"
        )

    raw_files_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if raw_files_manifest_path.exists():
        raise FileExistsError(
            f"refusing to overwrite raw manifest: {raw_files_manifest_path}"
        )
    lines: list[str] = []
    total_bytes = 0
    for relative, path in sorted(files_by_path.items()):
        lines.append(f"{_sha256(path)}  {relative}")
        total_bytes += int(path.stat().st_size)
    raw_files_manifest_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    split_with_universe = dict(normalized_split)
    split_with_universe["eligible_native_days"] = complete_days
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_id": (
            f"{str(symbol).upper()}_native_exchange_snapshot_delta_"
            f"warmup{int(warmup_hours)}h_v1"
        ),
        "symbol": str(symbol).upper(),
        "exchange": str(exchange),
        "tick_size": float(tick_size),
        "warmup_hours": int(warmup_hours),
        "clock": "exchange_transaction_time",
        "state_scope": (
            "full native snapshot/delta reconstruction independent of "
            "strategy trajectory"
        ),
        "policy_boundary": (
            "native exact queue and raw level changes are simulator-only; "
            "policy features use delayed strategy-visible BBO/L2/flow"
        ),
        "raw_root": str(raw_root),
        "eligible_days_path": str(eligible_days_path),
        "eligible_days_sha256": _sha256(eligible_days_path),
        "sequence_audit_path": str(sequence_audit_path or ""),
        "sequence_audit_sha256": (
            _sha256(sequence_audit_path)
            if sequence_audit_path is not None
            else ""
        ),
        "eligible_days_count": int(len(eligible_days)),
        "native_complete_days_count": int(len(complete_days)),
        "excluded_days": excluded,
        "raw_files_manifest_path": str(raw_files_manifest_path),
        "raw_files_manifest_sha256": _sha256(raw_files_manifest_path),
        "raw_unique_file_count": int(len(files_by_path)),
        "raw_unique_file_bytes": int(total_bytes),
        "split": split_with_universe,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--eligible-days", type=Path, required=True)
    parser.add_argument("--raw-files-manifest", type=Path, required=True)
    parser.add_argument("--sequence-audit", type=Path, default=None)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--split-json", required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--exchange", default="binance_futures")
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--warmup-hours", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.tick_size <= 0.0 or args.warmup_hours < 0:
        raise SystemExit("--tick-size must be positive and warmup non-negative")
    raw_split = json.loads(args.split_json)
    if not isinstance(raw_split, dict):
        raise SystemExit("--split-json must decode to an object")
    payload = build_native_exchange_book_universe(
        raw_root=args.raw_root,
        eligible_days_path=args.eligible_days,
        raw_files_manifest_path=args.raw_files_manifest,
        sequence_audit_path=args.sequence_audit,
        split={
            str(name): [str(day) for day in days]
            for name, days in raw_split.items()
        },
        symbol=args.symbol,
        exchange=args.exchange,
        tick_size=float(args.tick_size),
        warmup_hours=int(args.warmup_hours),
    )
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise FileExistsError(
            f"refusing to overwrite source universe: {output}"
        )
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(output)


if __name__ == "__main__":
    main()
