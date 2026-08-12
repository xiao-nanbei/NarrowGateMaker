#!/usr/bin/env python3
"""Build deterministic per-day quality authority for native normalized L2."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from data.download_cryptohft_orderbook import _normalized_day_summary
from models.audit.native_normalized_book_manifest import normalized_summary_is_strict

SCHEMA_VERSION = "narrowgate.native_l2_day_quality.v1"
SOURCE_KIND = "cryptohft_native_snapshot_delta"
CLOCK_SOURCE = "cryptohft_transaction_time_100ms_grid"
EXPECTED_REGISTRY_DATASET_VERSION = "normalized_l2_100ms_v2"
EXPECTED_REGISTRY_CONTRACT_VERSION = 1
EXPECTED_SEQUENCE_SCHEMA_VERSION = "cryptohft_sequence_audit.v1"


class NativeL2QualityError(RuntimeError):
    """Raised when native source provenance or mechanics quality is invalid."""


def sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _file_identity(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    stat = resolved.stat()
    return {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "size_bytes": int(stat.st_size),
    }


def _parse_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if value is None or bool(pd.isna(value)):
        return False
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n", "", "nan"}:
        return False
    raise NativeL2QualityError(f"invalid boolean value: {value!r}")


def _read_unique_day_row(path: Path, day: str, *, label: str) -> dict[str, Any]:
    frame = pd.read_csv(path)
    if "day" not in frame.columns:
        raise NativeL2QualityError(f"{label} lacks day column: {path}")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        raise NativeL2QualityError(f"{label} contains invalid UTC day")
    frame = frame.copy()
    frame["day"] = parsed.dt.strftime("%Y-%m-%d")
    selected = frame.loc[frame["day"] == day]
    if len(selected) != 1:
        raise NativeL2QualityError(
            f"{label} must contain exactly one row for {day}; observed={len(selected)}"
        )
    return {
        key: (None if bool(pd.isna(value)) else value)
        for key, value in selected.iloc[0].to_dict().items()
    }


def _manifest_bound_path(
    identity: Mapping[str, Any],
    *,
    expected_path: Path | None = None,
) -> Path:
    raw_path = identity.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        raise NativeL2QualityError("manifest file identity lacks path")
    path = Path(raw_path).expanduser().resolve(strict=True)
    if expected_path is not None and path != expected_path.expanduser().resolve(strict=True):
        raise NativeL2QualityError(
            f"manifest path mismatch: declared={path}, expected={expected_path}"
        )
    observed = _file_identity(path)
    if observed["sha256"] != identity.get("sha256"):
        raise NativeL2QualityError(f"manifest SHA256 mismatch: {path}")
    if observed["size_bytes"] != identity.get("size_bytes"):
        raise NativeL2QualityError(f"manifest size mismatch: {path}")
    return path


def _registry_file_record(
    manifest: Mapping[str, Any],
    *,
    day: str,
    kind: str,
) -> Mapping[str, Any]:
    records = [
        item
        for item in manifest.get("files", [])
        if isinstance(item, Mapping) and item.get("day") == day and item.get("kind") == kind
    ]
    if len(records) != 1:
        raise NativeL2QualityError(f"registry manifest must bind exactly one {kind} file for {day}")
    return records[0]


def _sequence_day_payload(payload: Mapping[str, Any], day: str) -> Mapping[str, Any]:
    matches: list[Mapping[str, Any]] = []
    for item in payload.get("range_audits", []):
        if not isinstance(item, Mapping):
            continue
        sequence = item.get("sequence_audit")
        if not isinstance(sequence, Mapping):
            continue
        days = sequence.get("day_sequence_audits")
        if not isinstance(days, Mapping):
            continue
        candidate = days.get(day)
        if isinstance(candidate, Mapping):
            matches.append(candidate)
    if len(matches) != 1:
        raise NativeL2QualityError(
            f"detailed sequence audit must contain exactly one record for {day}"
        )
    return matches[0]


def _timestamp_stats(path: Path, *, day: str) -> tuple[np.ndarray, dict[str, Any]]:
    values = pq.read_table(path, columns=["timestamp"]).column(0).to_numpy()
    timestamps = np.asarray(values, dtype=np.int64)
    if not len(timestamps):
        raise NativeL2QualityError(f"timestamp source is empty: {path}")
    deltas = np.diff(timestamps)
    if np.any(deltas <= 0):
        raise NativeL2QualityError(f"timestamps are duplicate or reversed: {path}")
    day_start_ms = int(pd.Timestamp(day, tz="UTC").value // 1_000_000)
    day_end_ms = day_start_ms + 86_400_000
    if timestamps[0] < day_start_ms or timestamps[-1] >= day_end_ms:
        raise NativeL2QualityError(f"timestamps escape UTC day {day}: {path}")
    return timestamps, {
        "rows": int(len(timestamps)),
        "first_timestamp_ms": int(timestamps[0]),
        "last_timestamp_ms": int(timestamps[-1]),
        "start_age_s": float((timestamps[0] - day_start_ms) / 1_000.0),
        "end_age_s": float((day_end_ms - timestamps[-1]) / 1_000.0),
        "p99_gap_s": float(np.quantile(deltas, 0.99) / 1_000.0) if len(deltas) else 0.0,
        "max_gap_s": float(np.max(deltas) / 1_000.0) if len(deltas) else 0.0,
        "gap_count_gt_500ms": int(np.count_nonzero(deltas > 500)),
        "gap_count_gt_5s": int(np.count_nonzero(deltas > 5_000)),
    }


def _available_depth_levels(path: Path) -> int:
    names = set(pq.ParquetFile(path).schema.names)
    level = 0
    while all(
        f"{side}_{kind}_{level + 1}" in names for side in ("bid", "ask") for kind in ("px", "qty")
    ):
        level += 1
    return level


def _cross_channel_quality(bbo_path: Path, l2_path: Path) -> dict[str, Any]:
    bbo = pq.read_table(
        bbo_path,
        columns=["timestamp", "best_bid", "best_bid_qty", "best_ask", "best_ask_qty"],
    )
    l2 = pq.read_table(
        l2_path,
        columns=["timestamp", "bid_px_1", "bid_qty_1", "ask_px_1", "ask_qty_1"],
    )
    if bbo.num_rows != l2.num_rows:
        return {
            "rows_equal": False,
            "timestamp_exact": False,
            "top_level_exact_ratio": 0.0,
            "valid": False,
        }
    bbo_arrays = [bbo.column(index).to_numpy() for index in range(5)]
    l2_arrays = [l2.column(index).to_numpy() for index in range(5)]
    timestamp_exact = bool(np.array_equal(bbo_arrays[0], l2_arrays[0]))
    exact = np.ones(bbo.num_rows, dtype=bool)
    for left, right in zip(bbo_arrays[1:], l2_arrays[1:], strict=True):
        exact &= left == right
    ratio = float(np.mean(exact)) if len(exact) else 0.0
    return {
        "rows_equal": True,
        "timestamp_exact": timestamp_exact,
        "top_level_exact_ratio": ratio,
        "valid": timestamp_exact and ratio == 1.0,
    }


def _sequence_quality(
    *,
    summary_row: Mapping[str, Any],
    detail: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    checks = {
        "summary_eligible": _parse_bool(summary_row.get("eligible")),
        "initialized_at_start": _parse_bool(detail.get("target_initialized_at_start")),
        "initialization_source": detail.get("target_initialization_source_at_start"),
        "sequence_gaps": int(detail.get("target_sequence_gaps", -1)),
        "invalid_sequence_messages": int(detail.get("target_invalid_sequence_messages", -1)),
        "message_time_reversals": int(detail.get("target_message_time_reversals", -1)),
        "stale_updates": int(detail.get("target_stale_updates", -1)),
        "duplicate_messages": int(detail.get("target_duplicate_messages", -1)),
        "accepted_updates": int(detail.get("target_accepted_updates", 0)),
        "snapshot_messages": int(detail.get("target_snapshot_messages", 0)),
    }
    if not checks["summary_eligible"]:
        reasons.append("sequence_summary_ineligible")
    if not checks["initialized_at_start"]:
        reasons.append("sequence_not_initialized_at_start")
    if checks["initialization_source"] != "snapshot":
        reasons.append("sequence_initialization_is_not_snapshot")
    for name in (
        "sequence_gaps",
        "invalid_sequence_messages",
        "message_time_reversals",
        "stale_updates",
        "duplicate_messages",
    ):
        if checks[name] != 0:
            reasons.append(name)
    if checks["accepted_updates"] <= 0:
        reasons.append("no_accepted_updates")
    if checks["snapshot_messages"] <= 0:
        reasons.append("no_snapshot_messages")
    checks["valid"] = not reasons
    return checks, reasons


def _admission_reasons(
    *,
    normalized_reasons: Sequence[str],
    sequence_reasons: Sequence[str],
    source_complete: bool,
    cross_channel_valid: bool,
    max_gap_s: float,
    max_target_gap_s: float,
    end_age_s: float,
    max_warmup_end_age_s: float,
    warmup_role: bool,
) -> list[str]:
    reasons = list(normalized_reasons) + list(sequence_reasons)
    if not source_complete:
        reasons.append("official_target_hours_incomplete")
    if not cross_channel_valid:
        reasons.append("bbo_l2_cross_channel_mismatch")
    if warmup_role:
        if end_age_s > max_warmup_end_age_s:
            reasons.append("warmup_midnight_state_stale")
    elif max_gap_s > max_target_gap_s:
        reasons.append("max_contiguous_gap_exceeds_target_limit")
    return sorted(set(reasons))


def build_native_l2_day_quality(
    *,
    registry_root: Path,
    registry_manifest_path: Path,
    detailed_sequence_audit_path: Path,
    day: str,
    symbol: str = "BTCUSDC",
    levels: int = 20,
    cadence_ms: int = 100,
    freshness_s: float = 5.0,
    min_coverage: float = 0.99,
    min_valid_spread_ratio: float = 0.999,
    max_p99_gap_s: float = 0.5,
    max_target_gap_s: float = 5.0,
    max_warmup_end_age_s: float = 0.5,
) -> dict[str, Any]:
    """Recompute and bind one native normalized day without reading economics."""

    canonical_day = pd.Timestamp(day, tz="UTC").strftime("%Y-%m-%d")
    if canonical_day != day:
        raise NativeL2QualityError("day must be canonical YYYY-MM-DD")
    if cadence_ms != 100:
        raise NativeL2QualityError("native normalized quality v1 requires 100ms cadence")
    root = registry_root.expanduser().resolve(strict=True)
    manifest_path = registry_manifest_path.expanduser().resolve(strict=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("dataset_version") != EXPECTED_REGISTRY_DATASET_VERSION:
        raise NativeL2QualityError("unsupported normalized registry dataset version")
    if manifest.get("contract_version") != EXPECTED_REGISTRY_CONTRACT_VERSION:
        raise NativeL2QualityError("unsupported normalized registry contract version")
    if manifest.get("symbol") != symbol:
        raise NativeL2QualityError("registry symbol mismatch")

    quality_identity = manifest.get("daily_quality")
    inputs = manifest.get("inputs")
    if not isinstance(quality_identity, Mapping) or not isinstance(inputs, Mapping):
        raise NativeL2QualityError("registry lacks quality/input identities")
    quality_path = _manifest_bound_path(
        quality_identity,
        expected_path=root / "daily_quality.csv",
    )
    sequence_summary_identity = inputs.get("sequence_audit")
    availability_identity = inputs.get("source_availability")
    if not isinstance(sequence_summary_identity, Mapping) or not isinstance(
        availability_identity, Mapping
    ):
        raise NativeL2QualityError("registry lacks sequence/source availability identities")
    sequence_summary_path = _manifest_bound_path(sequence_summary_identity)
    availability_path = _manifest_bound_path(availability_identity)

    quality_row = _read_unique_day_row(quality_path, day, label="registry daily quality")
    sequence_summary_row = _read_unique_day_row(
        sequence_summary_path,
        day,
        label="sequence summary",
    )
    availability_row = _read_unique_day_row(
        availability_path,
        day,
        label="source availability",
    )

    detailed_sequence_path = detailed_sequence_audit_path.expanduser().resolve(strict=True)
    detailed_sequence = json.loads(detailed_sequence_path.read_text(encoding="utf-8"))
    if detailed_sequence.get("schema_version") != EXPECTED_SEQUENCE_SCHEMA_VERSION:
        raise NativeL2QualityError("unsupported detailed sequence audit schema")
    if detailed_sequence.get("timestamp_source") != "transaction":
        raise NativeL2QualityError("native sequence audit is not transaction-time based")
    if detailed_sequence.get("snapshot_ms") != cadence_ms:
        raise NativeL2QualityError("sequence audit cadence mismatch")
    if int(detailed_sequence.get("levels", 0)) < levels:
        raise NativeL2QualityError("sequence audit depth is below requested levels")
    if symbol not in detailed_sequence.get("symbols", {}):
        raise NativeL2QualityError("sequence audit symbol is absent")
    sequence_detail = _sequence_day_payload(detailed_sequence, day)
    sequence_quality, sequence_reasons = _sequence_quality(
        summary_row=sequence_summary_row,
        detail=sequence_detail,
    )

    bbo_record = _registry_file_record(manifest, day=day, kind="bbo")
    l2_record = _registry_file_record(manifest, day=day, kind="l2")
    bbo_path = root / str(bbo_record.get("destination_relative_path", ""))
    l2_path = root / str(l2_record.get("destination_relative_path", ""))
    bbo_identity = _file_identity(bbo_path)
    l2_identity = _file_identity(l2_path)
    for record, observed, kind in (
        (bbo_record, bbo_identity, "bbo"),
        (l2_record, l2_identity, "l2"),
    ):
        source_identity = record.get("source_identity")
        if not isinstance(source_identity, Mapping):
            raise NativeL2QualityError(f"registry {kind} record lacks source identity")
        if source_identity.get("sha256") != observed["sha256"]:
            raise NativeL2QualityError(f"registry {kind} output SHA256 mismatch")
        if source_identity.get("size_bytes") != observed["size_bytes"]:
            raise NativeL2QualityError(f"registry {kind} output size mismatch")

    summary = _normalized_day_summary(
        root,
        symbol,
        pd.Timestamp(day, tz="UTC").to_pydatetime(),
        int(freshness_s * 1_000),
        levels=levels,
    )
    normalized_valid, normalized_reasons = normalized_summary_is_strict(
        summary,
        min_coverage=min_coverage,
        min_valid_spread_ratio=min_valid_spread_ratio,
        max_p99_gap_s=max_p99_gap_s,
    )
    bbo_timestamps, bbo_time = _timestamp_stats(bbo_path, day=day)
    l2_timestamps, l2_time = _timestamp_stats(l2_path, day=day)
    timestamp_exact = bool(np.array_equal(bbo_timestamps, l2_timestamps))
    cross_channel = _cross_channel_quality(bbo_path, l2_path)
    cross_channel["timestamp_exact"] = timestamp_exact
    cross_channel["valid"] = bool(cross_channel["valid"] and timestamp_exact)
    available_levels = _available_depth_levels(l2_path)
    if available_levels < levels:
        normalized_valid = False
        normalized_reasons = [*normalized_reasons, "insufficient_depth_levels"]

    source_complete = _parse_bool(availability_row.get("target_complete"))
    target_reasons = _admission_reasons(
        normalized_reasons=normalized_reasons,
        sequence_reasons=sequence_reasons,
        source_complete=source_complete,
        cross_channel_valid=bool(cross_channel["valid"]),
        max_gap_s=float(l2_time["max_gap_s"]),
        max_target_gap_s=max_target_gap_s,
        end_age_s=float(l2_time["end_age_s"]),
        max_warmup_end_age_s=max_warmup_end_age_s,
        warmup_role=False,
    )
    warmup_reasons = _admission_reasons(
        normalized_reasons=normalized_reasons,
        sequence_reasons=sequence_reasons,
        source_complete=source_complete,
        cross_channel_valid=bool(cross_channel["valid"]),
        max_gap_s=float(l2_time["max_gap_s"]),
        max_target_gap_s=max_target_gap_s,
        end_age_s=float(l2_time["end_age_s"]),
        max_warmup_end_age_s=max_warmup_end_age_s,
        warmup_role=True,
    )

    artifact: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source_kind": SOURCE_KIND,
        "dataset_id": manifest.get("dataset_version"),
        "symbol": symbol,
        "day": day,
        "clock_source": CLOCK_SOURCE,
        "clock_unit": "milliseconds_since_unix_epoch_utc",
        "timestamp_source": "transaction",
        "aws_tokyo_receive_time": False,
        "live_transport_eligible": False,
        "provider_normalized_replay_candidate": False,
        "cadence_ms": cadence_ms,
        "levels": available_levels,
        "thresholds": {
            "freshness_s": freshness_s,
            "min_coverage": min_coverage,
            "min_valid_spread_ratio": min_valid_spread_ratio,
            "max_p99_gap_s": max_p99_gap_s,
            "max_target_gap_s": max_target_gap_s,
            "max_warmup_end_age_s": max_warmup_end_age_s,
        },
        "registry": {
            "manifest": _file_identity(manifest_path),
            "daily_quality": _file_identity(quality_path),
            "daily_quality_row": quality_row,
        },
        "source_authority": {
            "source_availability": _file_identity(availability_path),
            "source_availability_row": availability_row,
            "official_target_hours_complete": source_complete,
            "official_warmup_hours_complete": _parse_bool(availability_row.get("warmup_complete")),
        },
        "sequence_authority": {
            "summary": _file_identity(sequence_summary_path),
            "detail": _file_identity(detailed_sequence_path),
            "day_evidence": sequence_quality,
        },
        "normalized_quality": {
            "bbo": {
                **bbo_time,
                "coverage": float(summary.get("bbo_coverage") or 0.0),
            },
            "l2": {
                **l2_time,
                "coverage": float(summary.get("l2_coverage") or 0.0),
                "valid_spread_ratio": float(summary.get("l2_valid_spread_ratio") or 0.0),
            },
            "tmp_file_present": bool(summary.get("tmp_exists")),
            "structural_valid": bool(normalized_valid),
            "structural_failure_reasons": sorted(set(normalized_reasons)),
        },
        "depth_quality": {
            "available_levels": available_levels,
            "required_levels": levels,
            "schema_complete": available_levels >= levels,
        },
        "cross_channel_quality": cross_channel,
        "bbo_output": bbo_identity,
        "l2_output": l2_identity,
        "native_sequence_valid": not sequence_reasons,
        "normalized_structural_valid": bool(normalized_valid),
        "target_replay_candidate": not target_reasons,
        "target_failure_reasons": target_reasons,
        "midnight_warmup_candidate": not warmup_reasons,
        "midnight_warmup_failure_reasons": warmup_reasons,
        "warmup_internal_gap_is_diagnostic_only": True,
        "labels_read": False,
        "economic_outcomes_read": False,
        "training_authorized": False,
        "live_authorized": False,
    }
    artifact["audit_code_sha256"] = sha256_file(Path(__file__).resolve())
    artifact["identity_sha256"] = _canonical_sha256(artifact)
    return artifact


def write_quality_artifact(path: Path, payload: Mapping[str, Any]) -> bool:
    """Publish atomically, reusing only byte-semantically identical output."""

    output = path.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        existing = json.loads(output.read_text(encoding="utf-8"))
        if _canonical_sha256(existing) != _canonical_sha256(payload):
            raise FileExistsError(f"refusing to replace different quality artifact: {output}")
        return True
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-root", type=Path, required=True)
    parser.add_argument("--registry-manifest", type=Path, required=True)
    parser.add_argument("--detailed-sequence-audit", type=Path, required=True)
    parser.add_argument("--day", action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    args = parser.parse_args(argv)

    results: list[dict[str, Any]] = []
    for day in args.day:
        payload = build_native_l2_day_quality(
            registry_root=args.registry_root,
            registry_manifest_path=args.registry_manifest,
            detailed_sequence_audit_path=args.detailed_sequence_audit,
            day=day,
            symbol=str(args.symbol).upper(),
        )
        output = args.output_dir / f"{str(args.symbol).upper()}-{day}.json"
        reused = write_quality_artifact(output, payload)
        results.append(
            {
                "day": day,
                "output": str(output.expanduser().resolve()),
                "reused": reused,
                "target_replay_candidate": payload["target_replay_candidate"],
                "midnight_warmup_candidate": payload["midnight_warmup_candidate"],
                "identity_sha256": payload["identity_sha256"],
            }
        )
    print(json.dumps({"schema_version": SCHEMA_VERSION, "results": results}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
