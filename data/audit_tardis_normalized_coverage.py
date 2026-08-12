#!/usr/bin/env python3
"""Audit and materialize the missing-day plan for normalized Tardis L2."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from data.download_tardis_archive import resolve_tardis_artifact_path

SCHEMA_VERSION = "normalized_tardis_l2_calendar_coverage.v1"
REQUIRED_DATASETS = ("book_ticker", "incremental_book_L2")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _days(start_day: str, end_day: str) -> list[str]:
    start = date.fromisoformat(start_day)
    end = date.fromisoformat(end_day)
    if end < start:
        raise ValueError("end day precedes start day")
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _atomic_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        _atomic_text(path, "day\n")
        return
    fieldnames = list(rows[0])
    output = []
    from io import StringIO

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    output.append(buffer.getvalue())
    _atomic_text(path, "".join(output))


def _raw_index(manifest_paths: Sequence[Path]) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    by_day: dict[str, dict[str, Any]] = {}
    identities: list[dict[str, str]] = []
    for raw_path in manifest_paths:
        path = raw_path.expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not payload.get("complete"):
            raise ValueError(f"Tardis download manifest is incomplete: {path}")
        identities.append({"path": str(path), "sha256": _sha256(path)})
        for row in payload.get("downloads", ()):
            day = str(row.get("day", ""))
            dataset = str(row.get("dataset", ""))
            if day and dataset in REQUIRED_DATASETS:
                by_day.setdefault(day, {})[dataset] = row
    return by_day, identities


def _raw_valid(row: Mapping[str, Any] | None) -> tuple[bool, str]:
    if not row or not row.get("exists") or not row.get("zstd_valid"):
        return False, "manifest_source_absent"
    path = resolve_tardis_artifact_path(str(row.get("path", "")))
    if not path.is_file():
        return False, "relocated_source_absent"
    expected_size = int(row.get("size_bytes", 0))
    if expected_size <= 0 or path.stat().st_size != expected_size:
        return False, "source_size_mismatch"
    if len(str(row.get("sha256", ""))) != 64:
        return False, "source_sha256_identity_absent"
    return True, ""


def _admission_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "quality": root / "quality" / f"BTCUSDC-{day}.json",
        "bbo": root / "bbo" / f"BTCUSDC-bbo-{day}.parquet",
        "l2": root / "l2" / f"BTCUSDC-l2-{day}.parquet",
        "clock": root / "clock" / f"BTCUSDC-clock-{day}.parquet",
    }


def _admission_valid(
    root: Path,
    day: str,
    *,
    rehash_existing: bool,
) -> tuple[bool, str]:
    paths = _admission_paths(root, day)
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        return False, "missing:" + "|".join(missing)
    try:
        quality = json.loads(paths["quality"].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, f"invalid_quality_json:{type(exc).__name__}"
    if str(quality.get("day", "")) != day or not quality.get("complete_day"):
        return False, "quality_identity_or_completeness_invalid"
    for name in ("bbo", "l2", "clock"):
        output = quality.get(f"{name}_output") or {}
        expected = str(output.get("sha256", ""))
        expected_size = int(output.get("size_bytes", 0))
        if len(expected) != 64:
            return False, f"{name}_sha256_identity_absent"
        if expected_size <= 0 or paths[name].stat().st_size != expected_size:
            return False, f"{name}_size_mismatch"
        if rehash_existing and _sha256(paths[name]) != expected:
            return False, f"{name}_sha256_mismatch"
    return True, ""


def audit_coverage(
    *,
    start_day: str,
    end_day: str,
    normalized_root: Path,
    download_manifests: Sequence[Path],
    excluded_days: Sequence[str] = (),
    rehash_existing: bool = False,
) -> dict[str, Any]:
    normalized_root = normalized_root.expanduser().resolve()
    raw_by_day, manifest_identities = _raw_index(download_manifests)
    calendar_days = _days(start_day, end_day)
    excluded = set(excluded_days)
    unknown_exclusions = sorted(excluded.difference(calendar_days))
    if unknown_exclusions:
        raise ValueError(
            "excluded normalization days lie outside the calendar: "
            + ",".join(unknown_exclusions)
        )
    rows: list[dict[str, Any]] = []
    for day in calendar_days:
        admitted, admission_reason = _admission_valid(
            normalized_root,
            day,
            rehash_existing=rehash_existing,
        )
        raw = raw_by_day.get(day, {})
        raw_checks = {
            dataset: _raw_valid(raw.get(dataset)) for dataset in REQUIRED_DATASETS
        }
        raw_ready = all(valid for valid, _ in raw_checks.values())
        quality: Mapping[str, Any] = {}
        if admitted:
            quality = json.loads(
                _admission_paths(normalized_root, day)["quality"].read_text(
                    encoding="utf-8"
                )
            )
        rows.append(
            {
                "day": day,
                "normalization_target": day not in excluded,
                "exclusion_reason": (
                    "owner_excluded_source_unavailable" if day in excluded else ""
                ),
                "normalized_admission_complete": admitted,
                "normalized_reason": admission_reason,
                "raw_pair_ready": raw_ready,
                "provider_normalized_replay_candidate": bool(
                    quality.get("provider_normalized_replay_candidate")
                ),
                "policy_visible": bool(quality.get("policy_visible")),
                "exact_queue_policy_eligible": bool(
                    quality.get("exact_queue_policy_eligible")
                ),
                "clock_source": str(quality.get("clock_source", "")),
                "raw_blockers": "|".join(
                    f"{dataset}:{reason}"
                    for dataset, (valid, reason) in raw_checks.items()
                    if not valid
                ),
            }
        )
    missing = [row for row in rows if not row["normalized_admission_complete"]]
    targets = [row for row in rows if row["normalization_target"]]
    target_missing = [
        row for row in targets if not row["normalized_admission_complete"]
    ]
    runnable = [row for row in target_missing if row["raw_pair_ready"]]
    blocked = [row for row in target_missing if not row["raw_pair_ready"]]
    complete_targets = [
        row for row in targets if row["normalized_admission_complete"]
    ]
    clock_sources = Counter(
        row["clock_source"] for row in complete_targets if row["clock_source"]
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "calendar_start_day": start_day,
        "calendar_end_day": end_day,
        "calendar_day_count": len(rows),
        "normalization_target_day_count": len(targets),
        "excluded_day_count": len(excluded),
        "excluded_days": sorted(excluded),
        "normalized_root": str(normalized_root),
        "download_manifests": manifest_identities,
        "rehash_existing": bool(rehash_existing),
        "complete_day_count": len(rows) - len(missing),
        "missing_day_count": len(missing),
        "target_complete_day_count": len(complete_targets),
        "target_missing_day_count": len(target_missing),
        "complete_target_days": [row["day"] for row in complete_targets],
        "provider_candidate_day_count": sum(
            row["provider_normalized_replay_candidate"] for row in complete_targets
        ),
        "policy_visible_day_count": sum(
            row["policy_visible"] for row in complete_targets
        ),
        "exact_queue_policy_eligible_day_count": sum(
            row["exact_queue_policy_eligible"] for row in complete_targets
        ),
        "clock_source_counts": dict(sorted(clock_sources.items())),
        "runnable_missing_day_count": len(runnable),
        "raw_blocked_day_count": len(blocked),
        "runnable_missing_days": [row["day"] for row in runnable],
        "raw_blocked_days": [row["day"] for row in blocked],
        "rows": rows,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day", required=True)
    parser.add_argument("--normalized-root", type=Path, required=True)
    parser.add_argument("--download-manifest", type=Path, action="append", required=True)
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--runnable-days-csv", type=Path, required=True)
    parser.add_argument("--blocked-days-csv", type=Path, required=True)
    parser.add_argument("--complete-days-csv", type=Path)
    parser.add_argument("--exclude-day", action="append", default=[])
    parser.add_argument("--rehash-existing", action="store_true")
    args = parser.parse_args(argv)
    report = audit_coverage(
        start_day=args.start_day,
        end_day=args.end_day,
        normalized_root=args.normalized_root,
        download_manifests=args.download_manifest,
        excluded_days=args.exclude_day,
        rehash_existing=args.rehash_existing,
    )
    _atomic_text(
        args.report_json.expanduser().resolve(),
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    _atomic_csv(
        args.runnable_days_csv.expanduser().resolve(),
        [{"day": day} for day in report["runnable_missing_days"]],
    )
    if args.complete_days_csv:
        _atomic_csv(
            args.complete_days_csv.expanduser().resolve(),
            [{"day": day} for day in report["complete_target_days"]],
        )
    blocked = [
        row
        for row in report["rows"]
        if row["normalization_target"]
        and not row["normalized_admission_complete"]
        and not row["raw_pair_ready"]
    ]
    _atomic_csv(args.blocked_days_csv.expanduser().resolve(), blocked)
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "calendar_day_count",
                    "normalization_target_day_count",
                    "excluded_day_count",
                    "excluded_days",
                    "complete_day_count",
                    "missing_day_count",
                    "target_complete_day_count",
                    "target_missing_day_count",
                    "provider_candidate_day_count",
                    "policy_visible_day_count",
                    "exact_queue_policy_eligible_day_count",
                    "clock_source_counts",
                    "runnable_missing_day_count",
                    "raw_blocked_day_count",
                    "raw_blocked_days",
                )
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
