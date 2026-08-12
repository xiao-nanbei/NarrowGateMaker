#!/usr/bin/env python3
"""Freeze Tardis batch cohorts and aggregate source-separated daily QA.

This utility never edits ``normalized_l2_100ms_v2/daily_quality.csv`` and
never promotes a Tardis day into the canonical good-day universe.  ``freeze``
creates two deliberately separate denominators: the 2025 technical rebuild
targets and the already-formal CryptoHFTData days whose Tardis raw pair is
complete.  ``aggregate`` reads the atomic per-day Tardis quality markers and
summarizes engineering QA without changing either source identity.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import tempfile
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DATASET_ID = "normalized_tardis_l2_100ms_v1"
SOURCE_ID = "tardis.0730-beinan.binance-futures.BTCUSDC.v1"
SCHEMA_VERSION = "narrowgate.tardis_batch_quality_aggregate.v1"
FREEZE_SCHEMA_VERSION = "narrowgate.tardis_batch_governance_freeze.v1"
REQUIRED_RAW_DATASETS = ("book_ticker", "incremental_book_L2")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write an empty manifest: {path}")
    fieldnames = list(rows[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _date_range(start: date, end: date) -> list[str]:
    if start > end:
        raise ValueError("technical start is after technical end")
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _raw_rows_by_day(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = {}
    for row in manifest.get("downloads", []):
        day = str(row.get("day", ""))
        dataset = str(row.get("dataset", ""))
        if day and dataset:
            by_day.setdefault(day, {})[dataset] = row
    return by_day


def _raw_row_valid(row: Mapping[str, Any] | None) -> bool:
    if not row:
        return False
    return bool(
        row.get("exists")
        and row.get("zstd_valid")
        and int(row.get("size_bytes", 0)) > 0
        and int(row.get("content_length", 0)) == int(row.get("size_bytes", -1))
        and len(str(row.get("sha256", ""))) == 64
    )


def freeze_manifests(
    *,
    canonical_quality: Path,
    tardis_manifest: Path,
    technical_start: date,
    technical_end: date,
    technical_csv: Path,
    overlap_csv: Path,
    contract_json: Path,
) -> dict[str, Any]:
    """Freeze technical and formal-overlap cohorts without changing eligibility."""

    with canonical_quality.open(newline="", encoding="utf-8-sig") as handle:
        canonical_rows = list(csv.DictReader(handle))
    raw_manifest = json.loads(tardis_manifest.read_text(encoding="utf-8"))
    if not raw_manifest.get("complete"):
        raise ValueError("the overlap Tardis download manifest must be complete")
    raw_by_day = _raw_rows_by_day(raw_manifest)

    technical_rows = [
        {
            "day": day,
            "cohort": "technical_target_2025",
            "purpose": "provider_normalization_and_engineering_qa",
            "research_eligible": "false",
            "canonical_good_day_modified": "false",
            "required_tardis_datasets": "book_ticker|incremental_book_L2",
        }
        for day in _date_range(technical_start, technical_end)
    ]

    overlap_rows: list[dict[str, Any]] = []
    for row in canonical_rows:
        if not _parse_bool(row.get("formal_eligible")):
            continue
        day = str(row["day"])
        raw = raw_by_day.get(day, {})
        ticker = raw.get("book_ticker")
        l2 = raw.get("incremental_book_L2")
        if not (_raw_row_valid(ticker) and _raw_row_valid(l2)):
            continue
        overlap_rows.append(
            {
                "day": day,
                "cohort": "formal_cryptohft_tardis_overlap_2026",
                "canonical_formal_eligible_at_freeze": "true",
                "tardis_raw_pair_complete": "true",
                "comparison_identity": "dual_source_diagnostic_only",
                "canonical_good_day_modified": "false",
                "cryptohft_bbo_sha256": str(row.get("bbo_sha256", "")),
                "cryptohft_l2_sha256": str(row.get("l2_sha256", "")),
                "tardis_book_ticker_sha256": str(ticker["sha256"]),
                "tardis_incremental_l2_sha256": str(l2["sha256"]),
            }
        )

    if not overlap_rows:
        raise ValueError("no formal CryptoHFTData/Tardis overlap days")
    _atomic_csv(technical_csv, technical_rows)
    _atomic_csv(overlap_csv, overlap_rows)
    payload: dict[str, Any] = {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "identity_boundary": {
            "technical_targets_are_research_eligible": False,
            "overlap_is_dual_source_diagnostic_only": True,
            "canonical_good_day_modified": False,
            "native_sequence_continuity_upgraded": False,
            "exact_queue_policy_eligible": False,
        },
        "inputs": {
            "canonical_quality": {
                "path": str(canonical_quality.resolve()),
                "sha256": _sha256(canonical_quality),
            },
            "tardis_download_manifest": {
                "path": str(tardis_manifest.resolve()),
                "sha256": _sha256(tardis_manifest),
            },
        },
        "technical_target": {
            "start": technical_start.isoformat(),
            "end": technical_end.isoformat(),
            "day_count": len(technical_rows),
            "path": str(technical_csv.resolve()),
            "sha256": _sha256(technical_csv),
        },
        "formal_overlap": {
            "day_count": len(overlap_rows),
            "path": str(overlap_csv.resolve()),
            "sha256": _sha256(overlap_csv),
        },
    }
    _atomic_json(contract_json, payload)
    return payload


def _read_days(path: Path, cohort: str) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "day" not in reader.fieldnames:
            raise ValueError(f"day column missing from {path}")
        return {str(row["day"]): cohort for row in reader if row.get("day")}


def _nested(payload: Mapping[str, Any], dotted: str) -> Any:
    current: Any = payload
    for key in dotted.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _finite_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric != numeric or numeric in {float("inf"), float("-inf")}:
        return None
    return numeric


def _quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return float(ordered[lower] * (1 - fraction) + ordered[upper] * fraction)


def _distribution(values: Iterable[Any]) -> dict[str, Any]:
    numeric = [number for value in values if (number := _finite_number(value)) is not None]
    return {
        "count": len(numeric),
        "min": _quantile(numeric, 0.0),
        "p25": _quantile(numeric, 0.25),
        "p50": _quantile(numeric, 0.50),
        "p75": _quantile(numeric, 0.75),
        "p95": _quantile(numeric, 0.95),
        "p99": _quantile(numeric, 0.99),
        "max": _quantile(numeric, 1.0),
    }


METRICS = (
    "freshness_union_coverage",
    "output_p99_gap_ms",
    "logical_message_gap.p99_upper_us",
    "logical_message_gap.maximum_us",
    "book_ticker_audit.book_ticker_comparable_ratio",
    "book_ticker_audit.book_ticker_price_exact_ratio",
    "book_ticker_audit.book_ticker_price_within_one_tick_ratio",
    "book_ticker_audit.book_ticker_quantity_exact_ratio",
    "book_ticker_audit.book_ticker_quantity_close_ratio",
    "cryptohft_dual_source.exchange_time_causal_asof.matched_ratio",
    "cryptohft_dual_source.exchange_time_causal_asof.top20_price_exact_ratio",
    "cryptohft_dual_source.exchange_time_causal_asof.top20_price_within_one_tick_ratio",
    "cryptohft_dual_source.exchange_time_causal_asof.top20_quantity_exact_ratio",
    "cryptohft_dual_source.exchange_time_causal_asof.top20_quantity_close_ratio",
    "cryptohft_dual_source.clock_agnostic_nearest.matched_ratio",
    "cryptohft_dual_source.clock_agnostic_nearest.top20_price_exact_ratio",
    "cryptohft_dual_source.clock_agnostic_nearest.top20_price_within_one_tick_ratio",
    "cryptohft_dual_source.clock_agnostic_nearest.top20_quantity_exact_ratio",
    "cryptohft_dual_source.clock_agnostic_nearest.top20_quantity_close_ratio",
)


def _failure_reasons(quality: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    freshness = _finite_number(quality.get("freshness_union_coverage"))
    output_gap = _finite_number(quality.get("output_p99_gap_ms"))
    internal_p99 = _finite_number(
        _nested(quality, "logical_message_gap.p99_upper_us")
    )
    internal_max = _finite_number(
        _nested(quality, "logical_message_gap.maximum_us")
    )
    checks = (
        (bool(quality.get("complete_day")), "incomplete_day"),
        (bool(quality.get("snapshot_seen_at_start")), "missing_initial_snapshot"),
        (
            _finite_number(quality.get("causal_violations")) == 0,
            "causal_violation",
        ),
        (
            _finite_number(quality.get("local_clock_reversals")) == 0,
            "provider_clock_reversal",
        ),
        (
            _finite_number(quality.get("invalid_spread_buckets")) == 0,
            "invalid_spread",
        ),
        (
            freshness is not None and freshness >= 0.99,
            "freshness_below_99pct",
        ),
        (output_gap is not None and output_gap <= 500.0, "output_p99_gap"),
        (
            internal_p99 is not None and internal_p99 <= 500_000,
            "internal_p99_gap",
        ),
        (
            internal_max is not None and internal_max <= 5_000_000,
            "internal_max_gap",
        ),
        (bool(quality.get("cross_channel_contract_valid")), "book_ticker_contract"),
        (
            str(quality.get("dataset_id")) == DATASET_ID
            and str(quality.get("source_id")) == SOURCE_ID,
            "source_identity_mismatch",
        ),
        (quality.get("policy_visible") is False, "policy_visibility_must_be_false"),
        (
            quality.get("exact_queue_policy_eligible") is False,
            "exact_queue_identity_must_be_false",
        ),
    )
    for passed, reason in checks:
        if not passed:
            reasons.append(reason)
    if not bool(quality.get("provider_normalized_replay_candidate")) and not reasons:
        reasons.append("provider_candidate_false_without_derived_reason")
    return reasons


def _group_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    valid = [row for row in rows if row.get("quality") is not None]
    qualities = [row["quality"] for row in valid]
    return {
        "requested_days": len(rows),
        "quality_days": len(valid),
        "missing_quality_days": [row["day"] for row in rows if row.get("quality") is None],
        "provider_candidate_days": [
            row["day"]
            for row in valid
            if bool(row["quality"].get("provider_normalized_replay_candidate"))
        ],
        "dual_source_available_days": [
            row["day"]
            for row in valid
            if bool(_nested(row["quality"], "cryptohft_dual_source.dual_source_available"))
        ],
        "failure_reason_counts": dict(
            sorted(
                Counter(
                    reason
                    for row in valid
                    for reason in row.get("failure_reasons", [])
                ).items()
            )
        ),
        "metrics": {
            metric: _distribution(_nested(quality, metric) for quality in qualities)
            for metric in METRICS
        },
    }


def aggregate_quality(
    *,
    quality_dir: Path,
    technical_days: Path,
    overlap_days: Path,
    output_json: Path,
    output_csv: Path,
) -> dict[str, Any]:
    """Aggregate atomic daily quality JSONs without editing any input file."""

    requested = _read_days(technical_days, "technical_target_2025")
    overlap = _read_days(overlap_days, "formal_overlap_2026")
    duplicate = set(requested).intersection(overlap)
    if duplicate:
        raise ValueError(f"technical and overlap cohorts intersect: {sorted(duplicate)}")
    requested.update(overlap)
    rows: list[dict[str, Any]] = []
    csv_rows: list[dict[str, Any]] = []
    for day, cohort in sorted(requested.items()):
        path = quality_dir / f"BTCUSDC-{day}.json"
        quality: dict[str, Any] | None = None
        error = ""
        if path.is_file():
            try:
                quality = json.loads(path.read_text(encoding="utf-8"))
                if str(quality.get("day")) != day:
                    raise ValueError("quality day does not match filename")
            except Exception as exc:  # noqa: BLE001 - report corrupt marker
                quality = None
                error = f"{type(exc).__name__}: {exc}"
        else:
            error = "quality_json_missing"
        reasons = _failure_reasons(quality) if quality is not None else [error]
        rows.append(
            {
                "day": day,
                "cohort": cohort,
                "quality": quality,
                "failure_reasons": reasons,
            }
        )
        csv_rows.append(
            {
                "day": day,
                "cohort": cohort,
                "quality_present": str(quality is not None).lower(),
                "provider_normalized_replay_candidate": str(
                    bool(quality and quality.get("provider_normalized_replay_candidate"))
                ).lower(),
                "dual_source_available": str(
                    bool(quality and _nested(quality, "cryptohft_dual_source.dual_source_available"))
                ).lower(),
                "freshness_union_coverage": "" if quality is None else quality.get("freshness_union_coverage", ""),
                "output_p99_gap_ms": "" if quality is None else quality.get("output_p99_gap_ms", ""),
                "logical_message_p99_upper_us": "" if quality is None else _nested(quality, "logical_message_gap.p99_upper_us"),
                "logical_message_maximum_us": "" if quality is None else _nested(quality, "logical_message_gap.maximum_us"),
                "book_ticker_comparable_ratio": "" if quality is None else _nested(quality, "book_ticker_audit.book_ticker_comparable_ratio"),
                "book_ticker_price_exact_ratio": "" if quality is None else _nested(quality, "book_ticker_audit.book_ticker_price_exact_ratio"),
                "book_ticker_price_within_one_tick_ratio": "" if quality is None else _nested(quality, "book_ticker_audit.book_ticker_price_within_one_tick_ratio"),
                "failure_reasons": "|".join(reason for reason in reasons if reason),
            }
        )
    groups = {
        "all": _group_summary(rows),
        "technical_target_2025": _group_summary(
            [row for row in rows if row["cohort"] == "technical_target_2025"]
        ),
        "formal_overlap_2026": _group_summary(
            [row for row in rows if row["cohort"] == "formal_overlap_2026"]
        ),
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_id": DATASET_ID,
        "source_id": SOURCE_ID,
        "quality_dir": str(quality_dir.resolve()),
        "input_manifests": {
            "technical_days": {
                "path": str(technical_days.resolve()),
                "sha256": _sha256(technical_days),
            },
            "overlap_days": {
                "path": str(overlap_days.resolve()),
                "sha256": _sha256(overlap_days),
            },
        },
        "identity_boundary": {
            "canonical_good_day_modified": False,
            "technical_targets_are_research_eligible": False,
            "dual_source_can_upgrade_native_sequence": False,
            "exact_queue_policy_eligible": False,
        },
        "groups": groups,
    }
    _atomic_csv(output_csv, csv_rows)
    payload["daily_csv"] = {
        "path": str(output_csv.resolve()),
        "sha256": _sha256(output_csv),
    }
    _atomic_json(output_json, payload)
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--canonical-quality", type=Path, required=True)
    freeze.add_argument("--tardis-manifest", type=Path, required=True)
    freeze.add_argument("--technical-start", type=date.fromisoformat, required=True)
    freeze.add_argument("--technical-end", type=date.fromisoformat, required=True)
    freeze.add_argument("--technical-csv", type=Path, required=True)
    freeze.add_argument("--overlap-csv", type=Path, required=True)
    freeze.add_argument("--contract-json", type=Path, required=True)
    aggregate = subparsers.add_parser("aggregate")
    aggregate.add_argument("--quality-dir", type=Path, required=True)
    aggregate.add_argument("--technical-days", type=Path, required=True)
    aggregate.add_argument("--overlap-days", type=Path, required=True)
    aggregate.add_argument("--output-json", type=Path, required=True)
    aggregate.add_argument("--output-csv", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "freeze":
        payload = freeze_manifests(
            canonical_quality=args.canonical_quality.expanduser().resolve(),
            tardis_manifest=args.tardis_manifest.expanduser().resolve(),
            technical_start=args.technical_start,
            technical_end=args.technical_end,
            technical_csv=args.technical_csv.expanduser().resolve(),
            overlap_csv=args.overlap_csv.expanduser().resolve(),
            contract_json=args.contract_json.expanduser().resolve(),
        )
    else:
        payload = aggregate_quality(
            quality_dir=args.quality_dir.expanduser().resolve(),
            technical_days=args.technical_days.expanduser().resolve(),
            overlap_days=args.overlap_days.expanduser().resolve(),
            output_json=args.output_json.expanduser().resolve(),
            output_csv=args.output_csv.expanduser().resolve(),
        )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
