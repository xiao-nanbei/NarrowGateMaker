#!/usr/bin/env python3
"""Freeze days whose replay lifecycle used the strict native queue path."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA_VERSION = "native_lifecycle_universe.v1"
STRICT_QUEUE_SCOPE = (
    "strategy_independent_native_snapshot_delta_exchange_time_v1"
)

ZERO_REQUIRED_FIELDS = (
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_sequence_gaps",
    "exchange_book_message_time_reversals",
    "exchange_book_delta_bootstrap_events",
    "exchange_book_event_timestamp_fallback_events",
    "exchange_book_receive_timestamp_fallback_events",
    "exchange_book_unknown_timestamp_source_events",
)


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
        raise ValueError("candidate-day manifest must contain a day column")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("candidate-day manifest contains invalid UTC dates")
    days = sorted(set(parsed.dt.strftime("%Y-%m-%d")))
    if not days:
        raise ValueError("candidate-day manifest is empty")
    return days


def lifecycle_integrity_reasons(
    record: dict[str, Any],
    *,
    expected_day: str,
    max_queue_missing_ratio: float = 0.001,
) -> list[str]:
    reasons: list[str] = []
    if str(record.get("day", "")) != str(expected_day):
        reasons.append("day_identity")
    if int(record.get("lifecycle_rows", 0) or 0) <= 0:
        reasons.append("empty_lifecycle")
    if int(record.get("exchange_book_events_accepted", 0) or 0) <= 0:
        reasons.append("no_native_events")
    if int(record.get("exchange_book_snapshot_events", 0) or 0) <= 0:
        reasons.append("no_snapshot")
    if str(record.get("exchange_book_queue_mode", "")) != "strict":
        reasons.append("queue_mode")
    if str(record.get("exchange_book_queue_scope", "")) != STRICT_QUEUE_SCOPE:
        reasons.append("queue_scope")
    for field in ZERO_REQUIRED_FIELDS:
        if int(record.get(field, 0) or 0) != 0:
            reasons.append(field)

    lookups = int(record.get("exchange_book_queue_lookup_count", 0) or 0)
    supported = int(record.get("exchange_book_queue_exact_count", 0) or 0)
    supported += int(
        record.get("exchange_book_queue_known_zero_count", 0) or 0
    )
    missing = int(record.get("exchange_book_queue_missing_count", 0) or 0)
    if lookups <= 0:
        reasons.append("no_queue_lookups")
    elif supported <= 0:
        reasons.append("no_exact_queue_support")
    if lookups > 0 and missing / lookups > float(max_queue_missing_ratio):
        reasons.append("queue_missing_ratio")
    return reasons


def freeze_native_lifecycle_universe(
    *,
    candidate_days_path: Path,
    partial_dir: Path,
    audit_output_path: Path,
    strict_days_output_path: Path,
    max_queue_missing_ratio: float = 0.001,
) -> dict[str, Any]:
    source = candidate_days_path.expanduser().resolve()
    partial = partial_dir.expanduser().resolve()
    audit_output = audit_output_path.expanduser().resolve()
    strict_output = strict_days_output_path.expanduser().resolve()
    manifest_output = audit_output.with_suffix(".manifest.json")
    for output in (audit_output, strict_output, manifest_output):
        if output.exists():
            raise FileExistsError(
                f"refusing to overwrite lifecycle audit output: {output}"
            )

    rows: list[dict[str, Any]] = []
    strict_days: list[str] = []
    daily_artifacts: list[dict[str, Any]] = []
    for day in _read_days(source):
        daily_path = partial / f"{day}.daily.json"
        if not daily_path.is_file():
            rows.append(
                {
                    "day": day,
                    "eligible": False,
                    "reasons": "missing_daily_artifact",
                }
            )
            continue
        record = json.loads(daily_path.read_text(encoding="utf-8"))
        reasons = lifecycle_integrity_reasons(
            record,
            expected_day=day,
            max_queue_missing_ratio=max_queue_missing_ratio,
        )
        eligible = not reasons
        if eligible:
            strict_days.append(day)
        lookups = int(
            record.get("exchange_book_queue_lookup_count", 0) or 0
        )
        missing = int(
            record.get("exchange_book_queue_missing_count", 0) or 0
        )
        rows.append(
            {
                "day": day,
                "eligible": eligible,
                "reasons": "|".join(reasons),
                "lifecycle_rows": int(record.get("lifecycle_rows", 0) or 0),
                "exchange_book_events_accepted": int(
                    record.get("exchange_book_events_accepted", 0) or 0
                ),
                "exchange_book_snapshot_events": int(
                    record.get("exchange_book_snapshot_events", 0) or 0
                ),
                "exchange_book_delta_bootstrap_events": int(
                    record.get("exchange_book_delta_bootstrap_events", 0)
                    or 0
                ),
                "exchange_book_sequence_gaps": int(
                    record.get("exchange_book_sequence_gaps", 0) or 0
                ),
                "exchange_book_queue_lookup_count": lookups,
                "exchange_book_queue_exact_count": int(
                    record.get("exchange_book_queue_exact_count", 0) or 0
                ),
                "exchange_book_queue_known_zero_count": int(
                    record.get(
                        "exchange_book_queue_known_zero_count",
                        0,
                    )
                    or 0
                ),
                "exchange_book_queue_missing_count": missing,
                "exchange_book_queue_missing_ratio": (
                    float(missing / lookups) if lookups > 0 else None
                ),
            }
        )
        daily_artifacts.append(
            {
                "day": day,
                "path": str(daily_path),
                "sha256": _sha256(daily_path),
            }
        )

    audit_output.parent.mkdir(parents=True, exist_ok=True)
    strict_output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(audit_output, index=False)
    pd.DataFrame({"day": strict_days}).to_csv(strict_output, index=False)
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "candidate_days_path": str(source),
        "candidate_days_sha256": _sha256(source),
        "candidate_days_count": len(rows),
        "strict_days_count": len(strict_days),
        "excluded_days_count": len(rows) - len(strict_days),
        "partial_dir": str(partial),
        "max_queue_missing_ratio": float(max_queue_missing_ratio),
        "zero_required_fields": list(ZERO_REQUIRED_FIELDS),
        "audit_output_path": str(audit_output),
        "audit_output_sha256": _sha256(audit_output),
        "strict_days_output_path": str(strict_output),
        "strict_days_output_sha256": _sha256(strict_output),
        "daily_artifacts": daily_artifacts,
        "daily_artifacts_sha256": _canonical_sha256(daily_artifacts),
        "manifest_output_path": str(manifest_output),
    }
    payload["identity_sha256"] = _canonical_sha256(payload)
    manifest_output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-days", type=Path, required=True)
    parser.add_argument("--partial-dir", type=Path, required=True)
    parser.add_argument("--audit-output", type=Path, required=True)
    parser.add_argument("--strict-days-output", type=Path, required=True)
    parser.add_argument("--max-queue-missing-ratio", type=float, default=0.001)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = freeze_native_lifecycle_universe(
        candidate_days_path=args.candidate_days,
        partial_dir=args.partial_dir,
        audit_output_path=args.audit_output,
        strict_days_output_path=args.strict_days_output,
        max_queue_missing_ratio=args.max_queue_missing_ratio,
    )
    print(
        json.dumps(
            {
                "candidate_days": payload["candidate_days_count"],
                "strict_days": payload["strict_days_count"],
                "excluded_days": payload["excluded_days_count"],
                "identity_sha256": payload["identity_sha256"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
