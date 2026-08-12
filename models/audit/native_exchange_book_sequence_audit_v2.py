#!/usr/bin/env python3
"""Audit native CryptoHFT snapshot/delta continuity without materializing L2."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from data.build_active_order_queue_tape import (
    iter_cryptohft_logical_messages,
)
from data.download_cryptohft_orderbook import (
    OrderBookSequenceState,
    OrderBookState,
)

SCHEMA_VERSION = "native_exchange_book_sequence_audit.v2"
DEFAULT_EXCHANGE = "binance_futures"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _read_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame:
        raise ValueError("candidate manifest must contain a day column")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("candidate manifest contains invalid UTC dates")
    days = sorted(set(parsed.dt.strftime("%Y-%m-%d")))
    if not days:
        raise ValueError("candidate manifest is empty")
    return days


def _contiguous_ranges(
    days: Iterable[str],
    *,
    max_days: int,
) -> list[list[str]]:
    ordered = sorted(set(str(day) for day in days))
    ranges: list[list[str]] = []
    current: list[str] = []
    previous: datetime | None = None
    for day in ordered:
        parsed = datetime.strptime(day, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        contiguous = (
            previous is not None
            and parsed - previous == timedelta(days=1)
            and (max_days <= 0 or len(current) < max_days)
        )
        if current and not contiguous:
            ranges.append(current)
            current = []
        current.append(day)
        previous = parsed
    if current:
        ranges.append(current)
    return ranges


def _hour_path(
    raw_root: Path,
    *,
    exchange: str,
    symbol: str,
    hour: datetime,
) -> Path:
    return (
        raw_root
        / exchange
        / hour.strftime("%Y-%m-%d")
        / hour.strftime("%H")
        / f"{symbol}_orderbook.parquet.zst"
    )


def _iter_hours(start: datetime, end_exclusive: datetime):
    current = start
    while current < end_exclusive:
        yield current
        current += timedelta(hours=1)


def evaluate_target_day_audit(
    audit: dict[str, object],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not bool(audit.get("target_initialized_at_start", False)):
        reasons.append("not_initialized_at_target_start")
    if str(audit.get("target_initialization_source_at_start", "")) != "snapshot":
        reasons.append("target_not_snapshot_seeded")
    if int(audit.get("target_accepted_updates", 0)) <= 0:
        reasons.append("no_target_updates")
    if int(audit.get("target_sequence_gaps", 0)) != 0:
        reasons.append("target_sequence_gap")
    if int(audit.get("target_invalid_sequence_messages", 0)) != 0:
        reasons.append("target_invalid_sequence")
    if int(audit.get("target_message_time_reversals", 0)) != 0:
        reasons.append("target_time_reversal")
    return not reasons, reasons


def _audit_range(payload: dict[str, object]) -> dict[str, object]:
    days = [str(day) for day in payload["days"]]
    raw_root = Path(str(payload["raw_root"]))
    exchange = str(payload["exchange"])
    symbol = str(payload["symbol"]).upper()
    tick_size = float(payload["tick_size"])
    warmup_hours = int(payload["warmup_hours"])
    first = datetime.strptime(days[0], "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    last = datetime.strptime(days[-1], "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    start = first - timedelta(hours=warmup_hours)
    end_exclusive = last + timedelta(days=1)
    target_days = set(days)

    state = OrderBookSequenceState(
        OrderBookState(),
        allow_delta_bootstrap=False,
    )
    current_day = ""
    stats_at_start: dict[str, int] | None = None
    initialized_at_start = False
    initialization_source_at_start = ""
    day_audits: dict[str, dict[str, object]] = {}
    source_paths: list[str] = []

    def finish_day() -> None:
        nonlocal current_day
        nonlocal stats_at_start
        if not current_day or stats_at_start is None:
            return
        current_stats = asdict(state.stats)
        audit: dict[str, object] = {
            f"target_{key}": int(value)
            - int(stats_at_start.get(key, 0))
            for key, value in current_stats.items()
        }
        audit["target_initialized_at_start"] = bool(
            initialized_at_start
        )
        audit["target_initialization_source_at_start"] = (
            initialization_source_at_start
        )
        eligible, reasons = evaluate_target_day_audit(audit)
        audit["eligible"] = bool(eligible)
        audit["exclusion_reasons"] = reasons
        day_audits[current_day] = audit

    for hour in _iter_hours(start, end_exclusive):
        day = hour.strftime("%Y-%m-%d")
        if day in target_days and day != current_day:
            finish_day()
            current_day = day
            stats_at_start = asdict(state.stats)
            initialized_at_start = bool(state.initialized)
            initialization_source_at_start = str(
                state.initialization_source or ""
            )

        path = _hour_path(
            raw_root,
            exchange=exchange,
            symbol=symbol,
            hour=hour,
        )
        if not path.is_file():
            state.invalidate_source_gap()
            continue
        source_paths.append(str(path))
        for message in iter_cryptohft_logical_messages(
            path,
            tick_size,
            include_levels=False,
        ):
            state.begin_message(
                event_type=message.event_type,
                receive_time_ms=message.receive_time_ms,
                event_time_ms=message.event_time_ms,
                transaction_time_ms=message.transaction_time_ms,
                first_update_id=message.first_update_id,
                final_update_id=message.final_update_id,
                previous_final_update_id=(
                    message.previous_final_update_id
                ),
                last_update_id=message.last_update_id,
            )
    finish_day()
    missing = sorted(target_days - set(day_audits))
    if missing:
        raise ValueError(f"range audit omitted target days: {missing}")
    return {
        "days": days,
        "day_audits": day_audits,
        "source_paths": source_paths,
        "range_stats": asdict(state.stats),
    }


def run_sequence_audit(
    *,
    candidate_days_path: Path,
    raw_root: Path,
    output_json: Path,
    eligible_days_path: Path,
    audit_csv_path: Path,
    symbol: str = "BTCUSDC",
    exchange: str = DEFAULT_EXCHANGE,
    tick_size: float = 0.1,
    warmup_hours: int = 24,
    max_range_days: int = 4,
    workers: int = 8,
) -> dict[str, object]:
    source = candidate_days_path.expanduser().resolve()
    raw = raw_root.expanduser().resolve()
    output = output_json.expanduser().resolve()
    eligible_output = eligible_days_path.expanduser().resolve()
    csv_output = audit_csv_path.expanduser().resolve()
    for path in (output, eligible_output, csv_output):
        if path.exists():
            raise FileExistsError(f"refusing to overwrite audit artifact: {path}")
    days = _read_days(source)
    ranges = _contiguous_ranges(days, max_days=max_range_days)
    tasks = [
        {
            "days": values,
            "raw_root": str(raw),
            "exchange": str(exchange),
            "symbol": str(symbol).upper(),
            "tick_size": float(tick_size),
            "warmup_hours": int(warmup_hours),
        }
        for values in ranges
    ]
    results: list[dict[str, object]] = []
    max_workers = max(1, min(int(workers), len(tasks)))
    if max_workers == 1:
        for index, task in enumerate(tasks, start=1):
            results.append(_audit_range(task))
            print(f"sequence range {index}/{len(tasks)} complete", flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers
        ) as pool:
            futures = {
                pool.submit(_audit_range, task): index
                for index, task in enumerate(tasks, start=1)
            }
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
                print(
                    f"sequence range {futures[future]}/{len(tasks)} complete",
                    flush=True,
                )

    by_day: dict[str, dict[str, object]] = {}
    source_paths: set[str] = set()
    for result in results:
        source_paths.update(str(path) for path in result["source_paths"])
        for day, audit in dict(result["day_audits"]).items():
            if day in by_day:
                raise ValueError(f"duplicate target-day audit: {day}")
            by_day[str(day)] = dict(audit)
    if sorted(by_day) != days:
        raise ValueError("sequence audit does not cover the candidate universe")

    rows: list[dict[str, object]] = []
    eligible_days: list[str] = []
    for day in days:
        audit = by_day[day]
        eligible = bool(audit["eligible"])
        if eligible:
            eligible_days.append(day)
        rows.append(
            {
                "day": day,
                "eligible": eligible,
                "exclusion_reasons": "|".join(
                    str(value)
                    for value in audit["exclusion_reasons"]
                ),
                **{
                    key: value
                    for key, value in audit.items()
                    if key not in {"eligible", "exclusion_reasons"}
                },
            }
        )
    frame = pd.DataFrame(rows)
    eligible_frame = pd.DataFrame({"day": eligible_days})
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    eligible_output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_output, index=False)
    eligible_frame.to_csv(eligible_output, index=False)

    identity: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "candidate_days_path": str(source),
        "candidate_days_sha256": _sha256(source),
        "candidate_days_count": len(days),
        "eligible_days_path": str(eligible_output),
        "eligible_days_sha256": _sha256(eligible_output),
        "eligible_days_count": len(eligible_days),
        "audit_csv_path": str(csv_output),
        "audit_csv_sha256": _sha256(csv_output),
        "excluded_days_count": len(days) - len(eligible_days),
        "raw_root": str(raw),
        "symbol": str(symbol).upper(),
        "exchange": str(exchange),
        "tick_size": float(tick_size),
        "warmup_hours": int(warmup_hours),
        "max_range_days": int(max_range_days),
        "workers": int(max_workers),
        "source_file_count": len(source_paths),
        "source_policy": (
            "header-only native snapshot/delta continuity; no delta "
            "bootstrap, imputation, or L2 materialization"
        ),
        "day_audits": by_day,
    }
    identity["identity_sha256"] = _canonical_sha256(identity)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return identity


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-days", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--eligible-days", type=Path, required=True)
    parser.add_argument("--audit-csv", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--warmup-hours", type=int, default=24)
    parser.add_argument("--max-range-days", type=int, default=4)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.tick_size <= 0.0:
        raise SystemExit("--tick-size must be positive")
    if args.warmup_hours < 0:
        raise SystemExit("--warmup-hours must be non-negative")
    if args.max_range_days <= 0:
        raise SystemExit("--max-range-days must be positive")
    if args.workers <= 0:
        raise SystemExit("--workers must be positive")
    identity = run_sequence_audit(
        candidate_days_path=args.candidate_days,
        raw_root=args.raw_root,
        output_json=args.output_json,
        eligible_days_path=args.eligible_days,
        audit_csv_path=args.audit_csv,
        symbol=args.symbol,
        exchange=args.exchange,
        tick_size=args.tick_size,
        warmup_hours=args.warmup_hours,
        max_range_days=args.max_range_days,
        workers=args.workers,
    )
    print(
        json.dumps(
            {
                "candidate_days": identity["candidate_days_count"],
                "eligible_days": identity["eligible_days_count"],
                "excluded_days": identity["excluded_days_count"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
