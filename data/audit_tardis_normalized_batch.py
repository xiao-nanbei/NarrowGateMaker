#!/usr/bin/env python3
"""Fail-closed post-batch audit for normalized Tardis top-20/100ms data.

The normalizer deliberately publishes one quality JSON last for each day.  This
auditor treats that JSON as a claim, not as proof: it re-hashes the three
Parquet outputs, checks their common 100 ms clock, re-derives the candidate
gate from primitive metrics, and binds the batch to one raw-manifest and one
normalizer-code SHA256.  It never edits a normalized artifact or a canonical
good-day registry.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from data.download_tardis_archive import resolve_tardis_artifact_path
from data.normalize_tardis_orderbook import (
    BOOK_TICKER,
    CROSS_CHANNEL_MIN_COMPARABLE_RATIO,
    CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO,
    CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO,
    DATASET_ID,
    INCREMENTAL_L2,
    SOURCE_ID,
)

SCHEMA_VERSION = "narrowgate.tardis_normalized_batch_integrity.v1"
EXPECTED_DATASETS = (BOOK_TICKER, INCREMENTAL_L2)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _read_days(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "day" not in reader.fieldnames:
            raise ValueError(f"day column missing from {path}")
        values = [str(row.get("day", "")).strip() for row in reader]
    if any(not value for value in values):
        raise ValueError(f"empty day in {path}")
    duplicates = sorted(day for day, count in Counter(values).items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate days in {path}: {duplicates}")
    return sorted(values)


def _raw_rows(manifest: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    rows: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in manifest.get("downloads", []):
        key = (str(row.get("day", "")), str(row.get("dataset", "")))
        if not all(key):
            raise ValueError(f"raw manifest row lacks day/dataset: {row}")
        if key in rows:
            raise ValueError(f"duplicate raw manifest identity: {key}")
        rows[key] = row
    return rows


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _derived_gates(quality: Mapping[str, Any]) -> dict[str, bool]:
    gap = quality.get("logical_message_gap", {})
    ticker = quality.get("book_ticker_audit", {})
    internal_p99 = _finite(gap.get("p99_upper_us"))
    internal_max = _finite(gap.get("maximum_us"))
    freshness = _finite(quality.get("freshness_union_coverage"))
    output_gap = _finite(quality.get("output_p99_gap_ms"))
    before_cross = bool(
        quality.get("complete_day")
        and quality.get("snapshot_seen_at_start")
        and _finite(quality.get("causal_violations")) == 0
        and _finite(quality.get("local_clock_reversals")) == 0
        and _finite(quality.get("invalid_spread_buckets")) == 0
        and freshness is not None
        and freshness >= 0.99
        and output_gap is not None
        and output_gap <= 500.0
        and internal_p99 is not None
        and internal_p99 <= 500_000
        and internal_max is not None
        and internal_max <= 5_000_000
    )
    comparable = _finite(ticker.get("book_ticker_comparable_ratio"))
    price_exact = _finite(ticker.get("book_ticker_price_exact_ratio"))
    within_tick = _finite(ticker.get("book_ticker_price_within_one_tick_ratio"))
    cross_channel = bool(
        comparable is not None
        and comparable >= CROSS_CHANNEL_MIN_COMPARABLE_RATIO
        and price_exact is not None
        and price_exact >= CROSS_CHANNEL_MIN_EXACT_PRICE_RATIO
        and within_tick is not None
        and within_tick >= CROSS_CHANNEL_MIN_WITHIN_TICK_RATIO
        and _finite(ticker.get("book_ticker_causal_violations")) == 0
        and _finite(ticker.get("book_ticker_local_clock_reversals")) == 0
    )
    return {
        "normalized_replay_candidate_before_cross_channel": before_cross,
        "cross_channel_contract_valid": cross_channel,
        "provider_normalized_replay_candidate": before_cross and cross_channel,
    }


def _artifact_claim(
    quality: Mapping[str, Any], name: str, errors: list[str]
) -> Path | None:
    claim = quality.get(name)
    if not isinstance(claim, Mapping):
        errors.append(f"{name}_claim_missing")
        return None
    path_text = str(claim.get("path", ""))
    path = Path(path_text) if path_text else None
    if path is None or not path.is_file():
        errors.append(f"{name}_file_missing")
        return None
    if path.stat().st_size != int(claim.get("size_bytes", -1)):
        errors.append(f"{name}_size_mismatch")
    if _sha256(path) != str(claim.get("sha256", "")):
        errors.append(f"{name}_sha256_mismatch")
    return path


def _parquet_structure(
    *,
    quality: Mapping[str, Any],
    bbo: Path,
    l2: Path,
    clock: Path,
    errors: list[str],
) -> dict[str, Any]:
    levels = int(quality.get("levels", 0))
    expected_l2_columns = {"timestamp"}
    for level in range(1, levels + 1):
        expected_l2_columns.update(
            {
                f"bid_px_{level}",
                f"bid_qty_{level}",
                f"ask_px_{level}",
                f"ask_qty_{level}",
            }
        )
    if levels != 20 or not expected_l2_columns.issubset(
        set(pq.ParquetFile(l2).schema.names)
    ):
        errors.append("l2_top20_schema_invalid")
    bbo_table = pq.read_table(
        bbo, columns=["timestamp", "best_bid", "best_ask"]
    ).to_pydict()
    l2_ts = pq.read_table(l2, columns=["timestamp"]).column(0).to_numpy()
    clock_table = pq.read_table(
        clock,
        columns=[
            "timestamp",
            "exchange_cut_timestamp_us",
            "last_provider_local_timestamp_us",
            "provider_visibility_delay_us",
        ],
    ).to_pydict()
    bbo_ts = np.asarray(bbo_table["timestamp"], dtype=np.int64)
    clock_ts = np.asarray(clock_table["timestamp"], dtype=np.int64)
    l2_ts = np.asarray(l2_ts, dtype=np.int64)
    row_count = len(bbo_ts)
    if not (row_count == len(l2_ts) == len(clock_ts)):
        errors.append("parquet_row_count_mismatch")
    if row_count != int(quality.get("emitted_rows", -1)):
        errors.append("quality_emitted_rows_mismatch")
    if not np.array_equal(bbo_ts, l2_ts) or not np.array_equal(bbo_ts, clock_ts):
        errors.append("parquet_timestamp_identity_mismatch")
    if len(bbo_ts) and (
        bool(np.any(np.diff(bbo_ts) <= 0)) or bool(np.any(bbo_ts % 100 != 0))
    ):
        errors.append("normalized_100ms_clock_invalid")
    day = date.fromisoformat(str(quality["day"]))
    day_start_ms = int(
        datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
        * 1_000
    )
    if len(bbo_ts) and bool(
        np.any(bbo_ts < day_start_ms) or np.any(bbo_ts >= day_start_ms + 86_400_000)
    ):
        errors.append("normalized_timestamp_outside_utc_day")
    if len(bbo_ts) and (
        int(quality.get("first_timestamp_ms", -1)) != int(bbo_ts[0])
        or int(quality.get("last_timestamp_ms", -1)) != int(bbo_ts[-1])
    ):
        errors.append("quality_timestamp_bounds_mismatch")
    bid = np.asarray(bbo_table["best_bid"], dtype=np.float64)
    ask = np.asarray(bbo_table["best_ask"], dtype=np.float64)
    if len(bid) and bool(np.any(~np.isfinite(bid) | ~np.isfinite(ask) | (bid <= 0) | (ask <= bid))):
        errors.append("bbo_spread_invalid")
    boundary_us = clock_ts * 1_000
    exchange_cut = np.asarray(
        clock_table["exchange_cut_timestamp_us"], dtype=np.int64
    )
    provider_local = np.asarray(
        clock_table["last_provider_local_timestamp_us"], dtype=np.int64
    )
    visibility_delay = np.asarray(
        clock_table["provider_visibility_delay_us"], dtype=np.int64
    )
    if len(clock_ts) and bool(
        np.any(exchange_cut >= boundary_us)
        or np.any(provider_local >= boundary_us)
        or np.any(visibility_delay < 0)
        or np.any(visibility_delay > 100_000)
        or np.any(visibility_delay != boundary_us - provider_local)
        or np.any(np.diff(exchange_cut) < 0)
        or np.any(np.diff(provider_local) < 0)
    ):
        errors.append("clock_sidecar_causal_contract_invalid")
    return {
        "rows": row_count,
        "first_timestamp_ms": int(bbo_ts[0]) if len(bbo_ts) else None,
        "last_timestamp_ms": int(bbo_ts[-1]) if len(bbo_ts) else None,
    }


def audit_batch(
    *,
    raw_manifest: Path,
    days_file: Path,
    quality_dir: Path,
    normalizer_code: Path,
    expected_normalizer_sha256: str | None = None,
    expected_manifest_sha256: str | None = None,
    governance_freeze: Path | None = None,
    rehash_raw: bool = False,
    allow_manifest_superset: bool = False,
) -> dict[str, Any]:
    """Return a machine-readable, read-only batch integrity audit."""

    days = _read_days(days_file)
    manifest_sha256 = _sha256(raw_manifest)
    normalizer_sha256 = _sha256(normalizer_code)
    manifest = json.loads(raw_manifest.read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        raise ValueError("raw download manifest is incomplete")
    raw = _raw_rows(manifest)
    expected_raw = {(day, dataset) for day in days for dataset in EXPECTED_DATASETS}
    missing = sorted(expected_raw.difference(raw))
    extra = sorted(set(raw).difference(expected_raw))
    if missing or (extra and not allow_manifest_superset):
        raise ValueError(f"raw/day identity mismatch: missing={missing} extra={extra}")

    batch_errors: list[str] = []
    if expected_normalizer_sha256 and normalizer_sha256 != expected_normalizer_sha256:
        batch_errors.append("normalizer_code_sha256_mismatch")
    if expected_manifest_sha256 and manifest_sha256 != expected_manifest_sha256:
        batch_errors.append("raw_manifest_sha256_mismatch")
    freeze_summary: dict[str, Any] | None = None
    if governance_freeze is not None:
        freeze = json.loads(governance_freeze.read_text(encoding="utf-8"))
        technical = freeze.get("technical_target", {})
        technical_path = Path(str(technical.get("path", "")))
        if not technical_path.is_file():
            batch_errors.append("governance_technical_manifest_missing")
        else:
            if _sha256(technical_path) != str(technical.get("sha256", "")):
                batch_errors.append("governance_technical_manifest_sha256_mismatch")
            if _read_days(technical_path) != days:
                batch_errors.append("governance_technical_day_set_mismatch")
        freeze_summary = {
            "path": str(governance_freeze.resolve()),
            "sha256": _sha256(governance_freeze),
        }

    daily: list[dict[str, Any]] = []
    for day in days:
        errors: list[str] = []
        raw_identity: dict[str, Any] = {}
        for dataset in EXPECTED_DATASETS:
            row = raw[(day, dataset)]
            manifest_path = Path(str(row.get("path", "")))
            path = resolve_tardis_artifact_path(manifest_path)
            row_valid = bool(
                row.get("exists")
                and row.get("zstd_valid")
                and int(row.get("size_bytes", 0)) > 0
                and int(row.get("size_bytes", -1))
                == int(row.get("content_length", -2))
                and len(str(row.get("sha256", ""))) == 64
                and path.is_file()
                and path.stat().st_size == int(row.get("size_bytes", -1))
            )
            if not row_valid:
                errors.append(f"raw_{dataset}_identity_invalid")
            actual_sha = _sha256(path) if rehash_raw and path.is_file() else None
            if actual_sha is not None and actual_sha != str(row.get("sha256", "")):
                errors.append(f"raw_{dataset}_sha256_mismatch")
            raw_identity[dataset] = {
                "manifest_path": str(manifest_path),
                "resolved_path": str(path),
                "size_bytes": int(row.get("size_bytes", 0)),
                "manifest_sha256": str(row.get("sha256", "")),
                "actual_sha256": actual_sha,
            }

        quality_path = quality_dir / f"BTCUSDC-{day}.json"
        if not quality_path.is_file():
            daily.append({"day": day, "errors": errors + ["quality_json_missing"]})
            continue
        quality = json.loads(quality_path.read_text(encoding="utf-8"))
        if str(quality.get("day")) != day:
            errors.append("quality_day_mismatch")
        if str(quality.get("source_id")) != SOURCE_ID:
            errors.append("quality_source_id_mismatch")
        if str(quality.get("dataset_id")) != DATASET_ID:
            errors.append("quality_dataset_id_mismatch")
        if (
            quality.get("clock_source") != "tardis_provider_local"
            or quality.get("clock_unit") != "microseconds_since_unix_epoch_utc"
            or int(quality.get("cadence_ms", 0)) != 100
            or int(quality.get("levels", 0)) != 20
        ):
            errors.append("quality_clock_or_shape_contract_mismatch")
        if not quality.get("complete_day") or quality.get("pilot_duration_s") is not None:
            errors.append("quality_not_complete_day")
        if str(quality.get("download_manifest", {}).get("sha256", "")) != manifest_sha256:
            errors.append("quality_download_manifest_sha256_mismatch")
        for dataset in EXPECTED_DATASETS:
            claim = quality.get("raw_inputs", {}).get(dataset, {})
            row = raw[(day, dataset)]
            if (
                str(claim.get("sha256", "")) != str(row.get("sha256", ""))
                or int(claim.get("size_bytes", -1)) != int(row.get("size_bytes", -2))
                or resolve_tardis_artifact_path(str(claim.get("path", "")))
                != resolve_tardis_artifact_path(str(row.get("path", "")))
            ):
                errors.append(f"quality_raw_{dataset}_identity_mismatch")

        bbo = _artifact_claim(quality, "bbo_output", errors)
        l2 = _artifact_claim(quality, "l2_output", errors)
        clock = _artifact_claim(quality, "clock_output", errors)
        structure: dict[str, Any] = {}
        if bbo is not None and l2 is not None and clock is not None:
            try:
                structure = _parquet_structure(
                    quality=quality, bbo=bbo, l2=l2, clock=clock, errors=errors
                )
            except Exception as exc:  # noqa: BLE001 - report corrupt Parquet
                errors.append(f"parquet_validation_error:{type(exc).__name__}:{exc}")

        derived = _derived_gates(quality)
        for key, value in derived.items():
            if bool(quality.get(key)) != value:
                errors.append(f"derived_gate_mismatch:{key}")
        if quality.get("policy_visible") is not False:
            errors.append("policy_visibility_identity_invalid")
        if quality.get("exact_queue_policy_eligible") is not False:
            errors.append("exact_queue_identity_invalid")
        dual = quality.get("cryptohft_dual_source", {})
        if bool(dual.get("dual_source_available")):
            dual_path = Path(str(dual.get("path", "")))
            if not dual_path.is_file() or _sha256(dual_path) != str(dual.get("sha256", "")):
                errors.append("cryptohft_dual_source_identity_mismatch")
        daily.append(
            {
                "day": day,
                "valid": not errors,
                "errors": errors,
                "quality_json": {
                    "path": str(quality_path.resolve()),
                    "sha256": _sha256(quality_path),
                },
                "raw_inputs": raw_identity,
                "outputs": {
                    name: quality.get(name)
                    for name in ("bbo_output", "l2_output", "clock_output")
                },
                "structure": structure,
                "derived_gates": derived,
            }
        )

    temporary_files = sorted(
        str(path) for path in quality_dir.parent.rglob("*.tmp") if path.is_file()
    )
    if temporary_files:
        batch_errors.append("temporary_output_files_present")
    invalid_days = [row["day"] for row in daily if not row.get("valid")]
    return {
        "schema_version": SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "dataset_id": DATASET_ID,
        "identity_boundary": {
            "canonical_good_day_modified": False,
            "policy_visible": False,
            "exact_queue_policy_eligible": False,
            "daily_quality_embeds_normalizer_code_sha256": False,
            "normalizer_code_sha256_is_batch_level_external_binding": True,
        },
        "inputs": {
            "raw_manifest": {
                "path": str(raw_manifest.resolve()),
                "sha256": manifest_sha256,
            },
            "days_file": {
                "path": str(days_file.resolve()),
                "sha256": _sha256(days_file),
            },
            "normalizer_code": {
                "path": str(normalizer_code.resolve()),
                "sha256": normalizer_sha256,
            },
            "governance_freeze": freeze_summary,
        },
        "requested_days": len(days),
        "valid_days": len(days) - len(invalid_days),
        "invalid_days": invalid_days,
        "provider_candidate_days": [
            row["day"]
            for row in daily
            if row.get("valid")
            and row.get("derived_gates", {}).get(
                "provider_normalized_replay_candidate"
            )
        ],
        "raw_payloads_rehashed": rehash_raw,
        "raw_manifest_superset_allowed": allow_manifest_superset,
        "raw_manifest_unrequested_identity_count": len(extra),
        "temporary_files": temporary_files,
        "batch_errors": batch_errors,
        "batch_integrity_valid": not batch_errors and not invalid_days,
        "daily": daily,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-manifest", type=Path, required=True)
    parser.add_argument("--days-file", type=Path, required=True)
    parser.add_argument("--quality-dir", type=Path, required=True)
    parser.add_argument(
        "--normalizer-code",
        type=Path,
        default=Path(__file__).with_name("normalize_tardis_orderbook.py"),
    )
    parser.add_argument("--expected-normalizer-sha256")
    parser.add_argument("--expected-manifest-sha256")
    parser.add_argument("--governance-freeze", type=Path)
    parser.add_argument("--rehash-raw", action="store_true")
    parser.add_argument(
        "--allow-manifest-superset",
        action="store_true",
        help=(
            "Allow a complete raw manifest to contain additional day/dataset "
            "identities while still requiring every requested identity exactly once"
        ),
    )
    parser.add_argument("--output-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload = audit_batch(
        raw_manifest=args.raw_manifest.expanduser().resolve(),
        days_file=args.days_file.expanduser().resolve(),
        quality_dir=args.quality_dir.expanduser().resolve(),
        normalizer_code=args.normalizer_code.expanduser().resolve(),
        expected_normalizer_sha256=args.expected_normalizer_sha256,
        expected_manifest_sha256=args.expected_manifest_sha256,
        governance_freeze=(
            args.governance_freeze.expanduser().resolve()
            if args.governance_freeze
            else None
        ),
        rehash_raw=args.rehash_raw,
        allow_manifest_superset=args.allow_manifest_superset,
    )
    if args.output_json:
        _atomic_json(args.output_json.expanduser().resolve(), payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["batch_integrity_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
