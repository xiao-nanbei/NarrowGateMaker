#!/usr/bin/env python3
"""Freeze the maximal good-day universe with complete native L2 warmup."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA_VERSION = "native_exchange_book_candidate_universe.v2"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame:
        raise ValueError("good-day manifest must contain a day column")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("good-day manifest contains invalid UTC dates")
    days = sorted(set(parsed.dt.strftime("%Y-%m-%d")))
    if not days:
        raise ValueError("good-day manifest is empty")
    return days


def _hour_path(
    raw_root: Path,
    *,
    exchange: str,
    symbol: str,
    timestamp: datetime,
) -> Path:
    return (
        raw_root
        / exchange
        / timestamp.strftime("%Y-%m-%d")
        / timestamp.strftime("%H")
        / f"{symbol}_orderbook.parquet.zst"
    )


def scan_complete_warmup_universe(
    *,
    raw_root: Path,
    good_days_path: Path,
    symbol: str = "BTCUSDC",
    exchange: str = "binance_futures",
    warmup_hours: int = 24,
) -> tuple[pd.DataFrame, list[Path]]:
    """Inventory exact target/warmup hours without downloading or imputing."""

    if warmup_hours <= 0 or warmup_hours % 24 != 0:
        raise ValueError("warmup_hours must be a positive whole number of days")
    root = raw_root.expanduser().resolve()
    source = good_days_path.expanduser().resolve()
    normalized_symbol = str(symbol).strip().upper()
    days = _read_days(source)
    rows: list[dict[str, Any]] = []
    unique_paths: dict[str, Path] = {}

    for day in days:
        day_start = datetime.strptime(day, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        warmup_start = day_start - timedelta(hours=int(warmup_hours))
        target_paths = [
            _hour_path(
                root,
                exchange=exchange,
                symbol=normalized_symbol,
                timestamp=day_start + timedelta(hours=hour),
            )
            for hour in range(24)
        ]
        warmup_paths = [
            _hour_path(
                root,
                exchange=exchange,
                symbol=normalized_symbol,
                timestamp=warmup_start + timedelta(hours=hour),
            )
            for hour in range(int(warmup_hours))
        ]
        target_present = [
            path for path in target_paths if path.is_file() and path.stat().st_size > 0
        ]
        warmup_present = [
            path for path in warmup_paths if path.is_file() and path.stat().st_size > 0
        ]
        target_complete = len(target_present) == len(target_paths)
        warmup_complete = len(warmup_present) == len(warmup_paths)
        candidate = target_complete and warmup_complete
        reasons = []
        if not target_complete:
            reasons.append("missing_target_l2_hours")
        if not warmup_complete:
            reasons.append("missing_prior_day_warmup")
        rows.append(
            {
                "day": day,
                "previous_natural_day": (
                    day_start - timedelta(days=1)
                ).strftime("%Y-%m-%d"),
                "target_hours_expected": len(target_paths),
                "target_hours_present": len(target_present),
                "warmup_hours_expected": len(warmup_paths),
                "warmup_hours_present": len(warmup_present),
                "target_complete": target_complete,
                "warmup_complete": warmup_complete,
                "candidate": candidate,
                "exclusion_reason": "|".join(reasons),
            }
        )
        if candidate:
            for path in (*warmup_paths, *target_paths):
                unique_paths[str(path.relative_to(root))] = path

    return pd.DataFrame(rows), [
        unique_paths[key] for key in sorted(unique_paths)
    ]


def freeze_complete_warmup_universe(
    *,
    raw_root: Path,
    good_days_path: Path,
    output_dir: Path,
    symbol: str = "BTCUSDC",
    exchange: str = "binance_futures",
    warmup_hours: int = 24,
    hash_raw_files: bool = False,
) -> dict[str, Any]:
    output = output_dir.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"refusing to overwrite universe directory: {output}")
    output.mkdir(parents=True)
    availability, raw_paths = scan_complete_warmup_universe(
        raw_root=raw_root,
        good_days_path=good_days_path,
        symbol=symbol,
        exchange=exchange,
        warmup_hours=warmup_hours,
    )
    candidate = availability.loc[
        availability["candidate"].astype(bool), ["day"]
    ].reset_index(drop=True)
    availability_path = output / "source_availability.csv"
    candidate_path = output / "candidate_days.csv"
    raw_manifest_path = output / "raw_files.manifest"
    availability.to_csv(availability_path, index=False)
    candidate.to_csv(candidate_path, index=False)

    raw_lines: list[str] = []
    total_bytes = 0
    root = raw_root.expanduser().resolve()
    for path in raw_paths:
        total_bytes += int(path.stat().st_size)
        identity = _sha256(path) if hash_raw_files else (
            f"size={path.stat().st_size};mtime_ns={path.stat().st_mtime_ns}"
        )
        raw_lines.append(f"{identity}  {path.relative_to(root)}")
    raw_manifest_path.write_text(
        "\n".join(raw_lines) + ("\n" if raw_lines else ""),
        encoding="utf-8",
    )

    excluded = availability.loc[
        ~availability["candidate"].astype(bool),
        ["day", "exclusion_reason"],
    ]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_id": (
            f"{str(symbol).upper()}_good_days_complete_"
            f"warmup{int(warmup_hours)}h_20260724"
        ),
        "symbol": str(symbol).upper(),
        "exchange": str(exchange),
        "warmup_hours": int(warmup_hours),
        "source_policy": (
            "target day and preceding natural-day warmup must both be "
            "complete; no download, imputation, delayed snapshot entry, or "
            "delta bootstrap is used to rescue missing warmup"
        ),
        "raw_root": str(root),
        "good_days_path": str(good_days_path.expanduser().resolve()),
        "good_days_sha256": _sha256(good_days_path.expanduser().resolve()),
        "good_days_count": int(len(availability)),
        "candidate_days_count": int(len(candidate)),
        "excluded_days_count": int(len(excluded)),
        "excluded_days": excluded.to_dict("records"),
        "availability_path": str(availability_path),
        "availability_sha256": _sha256(availability_path),
        "candidate_days_path": str(candidate_path),
        "candidate_days_sha256": _sha256(candidate_path),
        "raw_files_manifest_path": str(raw_manifest_path),
        "raw_files_manifest_sha256": _sha256(raw_manifest_path),
        "raw_files_hashed_by_content": bool(hash_raw_files),
        "raw_unique_file_count": int(len(raw_paths)),
        "raw_unique_file_bytes": int(total_bytes),
        "next_gate": (
            "per-day target-scoped snapshot/delta sequence audit, followed "
            "by lifecycle causal/exact-queue integrity"
        ),
    }
    manifest_path = output / "candidate_universe.json"
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--good-days", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--exchange", default="binance_futures")
    parser.add_argument("--warmup-hours", type=int, default=24)
    parser.add_argument("--hash-raw-files", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = freeze_complete_warmup_universe(
        raw_root=args.raw_root,
        good_days_path=args.good_days,
        output_dir=args.output_dir,
        symbol=args.symbol,
        exchange=args.exchange,
        warmup_hours=args.warmup_hours,
        hash_raw_files=args.hash_raw_files,
    )
    print(
        json.dumps(
            {
                "candidate_days": payload["candidate_days_count"],
                "excluded_days": payload["excluded_days_count"],
                "manifest": str(
                    Path(args.output_dir).expanduser().resolve()
                    / "candidate_universe.json"
                ),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
