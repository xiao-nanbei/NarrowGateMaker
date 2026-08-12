#!/usr/bin/env python3
"""Build the source/day manifest for the F02 reach-time hazard successor.

The builder reads only source-quality metadata, BBO files, and official
Binance Futures aggTrades.  Provider eligibility is read from each atomic
per-day quality JSON; the stale provider aggregate CSV is deliberately not an
input.  Panel membership and source ownership are caller-supplied so an
overlap date can never be weighted once per source by accident.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

IDENTITY = "p3_aggressive_reach_time_conditioned_hazard_v1"
SCHEMA_VERSION = "narrowgate.p3_reach_time_source_day_manifest.v1"
PROVIDER_SOURCE = "provider"
NATIVE_SOURCE = "native"
SOURCES = (PROVIDER_SOURCE, NATIVE_SOURCE)

PROVIDER_DATASET_ID = "normalized_tardis_l2_100ms_v1"
NATIVE_DATASET_ID = "normalized_l2_100ms_v2"
PROVIDER_AUTHORITY = "provider_normalized_causal"
NATIVE_AUTHORITY = "normalized_100ms_v2_historical_p3_reference"
NATIVE_SOURCE_CLOCK = "exchange_event_time"
NATIVE_CLOCK_UNIT = "milliseconds_since_unix_epoch_utc"
TRADE_AUTHORITY = "binance_futures_official_aggTrades"
TRADE_SOURCE_CLOCK = "exchange_trade_time"
TRADE_CLOCK_UNIT = "milliseconds_since_unix_epoch_utc"

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class PanelSpec:
    """One explicitly assigned, chronologically ordered source panel."""

    name: str
    source: str
    dates: tuple[str, ...]


def sha256_file(path: Path) -> str:
    """Return the SHA256 of one required file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    """Hash a JSON-compatible value with deterministic serialization."""

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_manifest_sha256(manifest: Mapping[str, Any]) -> str:
    """Compute the manifest identity, excluding its self-referential field."""

    payload = dict(manifest)
    payload.pop("canonical_manifest_sha256", None)
    return canonical_sha256(payload)


def _canonical_day(value: Any, *, label: str) -> str:
    raw = str(value)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{label} is not an ISO UTC date: {raw!r}") from exc
    canonical = parsed.isoformat()
    if canonical != raw:
        raise ValueError(f"{label} must use canonical YYYY-MM-DD form: {raw!r}")
    return canonical


def _require_filename_day(path: Path, expected_day: str, *, label: str) -> None:
    days = _DATE_RE.findall(path.name)
    if days != [expected_day]:
        raise ValueError(
            f"{label} source date mismatch: file={path.name!r} expected={expected_day}"
        )


def _require_sha256(value: Any, *, label: str) -> str:
    digest = str(value or "").lower()
    if _SHA256_RE.fullmatch(digest) is None:
        raise ValueError(f"{label} is missing a valid SHA256")
    return digest


def _strict_bool(value: Any, *, label: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f"{label} is not a strict boolean: {value!r}")


def _required_file(path: Path, *, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} missing: {resolved}")
    return resolved


def _file_identity(path: Path, *, authority: str, source_clock: str) -> dict[str, Any]:
    required = _required_file(path, label=authority)
    return {
        "path": str(required),
        "sha256": sha256_file(required),
        "size_bytes": required.stat().st_size,
        "authority": authority,
        "source_clock": source_clock,
    }


def _aggtrades_path(root: Path, symbol: str, day: str) -> Path:
    candidates = [
        root / f"{symbol}-aggTrades-{day}.csv",
        root / f"{symbol}-aggTrades-{day}.csv.gz",
    ]
    existing = [path.expanduser().resolve() for path in candidates if path.is_file()]
    if not existing:
        raise FileNotFoundError(
            f"official aggTrades missing for {day}: expected one of "
            + ", ".join(str(path) for path in candidates)
        )
    if len(existing) != 1:
        raise ValueError(f"ambiguous official aggTrades files for {day}: {existing}")
    _require_filename_day(existing[0], day, label="official aggTrades")
    return existing[0]


def _record_with_hash(payload: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(payload)
    record["record_sha256"] = canonical_sha256(record)
    return record


def _validate_record_hash(record: Mapping[str, Any], *, label: str) -> None:
    payload = dict(record)
    observed = str(payload.pop("record_sha256", ""))
    expected = canonical_sha256(payload)
    if observed != expected:
        raise ValueError(f"{label} canonical record hash mismatch")


def normalize_panels(
    panels: Sequence[PanelSpec | Mapping[str, Any]] | Mapping[str, Mapping[str, Any]],
) -> tuple[PanelSpec, ...]:
    """Normalize caller-supplied panels without inferring any split."""

    raw_panels: list[PanelSpec | Mapping[str, Any]]
    if isinstance(panels, Mapping):
        raw_panels = [{"name": name, **dict(spec)} for name, spec in panels.items()]
    else:
        raw_panels = list(panels)
    if not raw_panels:
        raise ValueError("at least one explicit panel is required")

    normalized: list[PanelSpec] = []
    seen_names: set[str] = set()
    owner_by_day: dict[str, str] = {}
    for raw in raw_panels:
        if isinstance(raw, PanelSpec):
            panel = raw
        else:
            panel = PanelSpec(
                name=str(raw.get("name", "")).strip(),
                source=str(raw.get("source", "")).strip().lower(),
                dates=tuple(str(day) for day in raw.get("dates", ())),
            )
        if not panel.name:
            raise ValueError("panel name must be non-empty")
        if panel.name in seen_names:
            raise ValueError(f"duplicate panel name: {panel.name}")
        if panel.source not in SOURCES:
            raise ValueError(f"unsupported source for panel {panel.name}: {panel.source}")
        dates = tuple(_canonical_day(day, label=f"panel {panel.name} date") for day in panel.dates)
        if not dates:
            raise ValueError(f"panel {panel.name} must contain at least one date")
        if list(dates) != sorted(dates) or len(dates) != len(set(dates)):
            raise ValueError(f"panel {panel.name} dates must be chronological and unique")
        for day in dates:
            prior = owner_by_day.get(day)
            if prior is not None:
                raise ValueError(
                    f"panel dates must be disjoint: {day} appears in {prior} and {panel.name}"
                )
            owner_by_day[day] = panel.name
        seen_names.add(panel.name)
        normalized.append(PanelSpec(panel.name, panel.source, dates))
    return tuple(sorted(normalized, key=lambda item: item.name))


def normalize_overlap_dates(overlap_dates: Sequence[str]) -> tuple[str, ...]:
    """Validate the explicit source-comparison dates."""

    dates = tuple(_canonical_day(day, label="overlap date") for day in overlap_dates)
    if list(dates) != sorted(dates) or len(dates) != len(set(dates)):
        raise ValueError("overlap dates must be chronological and unique")
    return dates


def _provider_quality_index(
    quality_root: Path,
    *,
    symbol: str,
) -> dict[str, tuple[Path, dict[str, Any]]]:
    root = quality_root.expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"provider quality directory missing: {root}")
    index: dict[str, tuple[Path, dict[str, Any]]] = {}
    for quality_path in sorted(root.glob(f"{symbol}-*.json")):
        filename_days = _DATE_RE.findall(quality_path.name)
        if len(filename_days) != 1:
            raise ValueError(f"provider quality filename has no unique date: {quality_path}")
        filename_day = _canonical_day(filename_days[0], label="provider filename date")
        payload = json.loads(quality_path.read_text(encoding="utf-8"))
        payload_day = _canonical_day(payload.get("day"), label="provider payload day")
        if payload_day != filename_day:
            raise ValueError(
                f"provider quality source date mismatch: file={filename_day} payload={payload_day}"
            )
        if payload_day in index:
            raise ValueError(f"duplicate provider quality date: {payload_day}")
        if payload.get("symbol") != symbol:
            raise ValueError(f"provider quality symbol mismatch for {payload_day}")
        if payload.get("dataset_id") != PROVIDER_DATASET_ID:
            raise ValueError(f"provider dataset mismatch for {payload_day}")
        index[payload_day] = (quality_path.resolve(), payload)
    return index


def scan_provider_records(
    *,
    quality_root: Path,
    bbo_root: Path,
    official_aggtrades_root: Path,
    target_dates: Sequence[str],
    symbol: str = "BTCUSDC",
    authority: str = PROVIDER_AUTHORITY,
) -> list[dict[str, Any]]:
    """Freeze requested provider days from atomic per-day quality JSONs."""

    targets = tuple(_canonical_day(day, label="provider target date") for day in target_dates)
    if len(targets) != len(set(targets)):
        raise ValueError("provider target dates must be unique")
    index = _provider_quality_index(quality_root, symbol=symbol)
    actual_bbo_root = bbo_root.expanduser().resolve()
    trade_root = official_aggtrades_root.expanduser().resolve()
    records: list[dict[str, Any]] = []
    for day in sorted(targets):
        if day not in index:
            raise FileNotFoundError(f"provider quality JSON missing for target date {day}")
        quality_path, quality = index[day]
        eligible = _strict_bool(
            quality.get("provider_normalized_replay_candidate"),
            label=f"provider eligibility {day}",
        )
        if not eligible:
            raise ValueError(f"provider target date failed quality status: {day}")

        output = quality.get("bbo_output")
        if not isinstance(output, Mapping):
            raise ValueError(f"provider BBO identity missing for {day}")
        expected_hash = _require_sha256(output.get("sha256"), label=f"provider BBO SHA256 {day}")
        declared_bbo = Path(str(output.get("path", "")))
        _require_filename_day(declared_bbo, day, label="provider declared BBO")
        expected_name = f"{symbol}-bbo-{day}.parquet"
        if declared_bbo.name != expected_name:
            raise ValueError(f"provider BBO filename mismatch for {day}")
        bbo_path = _required_file(actual_bbo_root / expected_name, label=f"provider BBO {day}")
        observed_hash = sha256_file(bbo_path)
        if observed_hash != expected_hash:
            raise ValueError(
                f"provider BBO hash mismatch for {day}: "
                f"observed={observed_hash} expected={expected_hash}"
            )

        trade_path = _aggtrades_path(trade_root, symbol, day)
        if day.startswith("2025-") and not trade_path.is_file():
            # Kept explicit because official labels are mandatory for every
            # selected 2025 provider row, even when BBO quality is valid.
            raise FileNotFoundError(f"2025 provider row lacks official aggTrades: {day}")
        source_clock = str(quality.get("clock_source", "")).strip()
        clock_unit = str(quality.get("clock_unit", "")).strip()
        if not source_clock or not clock_unit:
            raise ValueError(f"provider source clock identity missing for {day}")

        quality_identity = _file_identity(
            quality_path,
            authority="provider_per_day_quality_json",
            source_clock="artifact_build_time",
        )
        record = _record_with_hash(
            {
                "date": day,
                "source": PROVIDER_SOURCE,
                "dataset_id": PROVIDER_DATASET_ID,
                "source_authority": authority,
                "source_clock": source_clock,
                "clock_unit": clock_unit,
                "quality_status": {
                    "field": "provider_normalized_replay_candidate",
                    "eligible": eligible,
                    "complete_day": _strict_bool(
                        quality.get("complete_day"),
                        label=f"provider complete_day {day}",
                    ),
                    "cross_channel_contract_valid": _strict_bool(
                        quality.get("cross_channel_contract_valid"),
                        label=f"provider cross-channel status {day}",
                    ),
                    "causal_violations": int(quality.get("causal_violations", 0)),
                },
                "files": {
                    "quality": quality_identity,
                    "bbo": {
                        "path": str(bbo_path),
                        "sha256": observed_hash,
                        "size_bytes": bbo_path.stat().st_size,
                        "authority": authority,
                        "source_clock": source_clock,
                    },
                    "official_aggtrades": _file_identity(
                        trade_path,
                        authority=TRADE_AUTHORITY,
                        source_clock=TRADE_SOURCE_CLOCK,
                    ),
                },
            }
        )
        records.append(record)
    return records


def _native_quality_index(
    daily_quality_csv: Path,
) -> tuple[Path, str, dict[str, dict[str, str]]]:
    path = _required_file(daily_quality_csv, label="native daily quality CSV")
    quality_hash = sha256_file(path)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "day" not in reader.fieldnames:
            raise ValueError("native daily quality CSV lacks day column")
        index: dict[str, dict[str, str]] = {}
        for row in reader:
            day = _canonical_day(row.get("day"), label="native quality date")
            if day in index:
                raise ValueError(f"duplicate native quality date: {day}")
            declared_path = Path(str(row.get("bbo_source_path", "")))
            _require_filename_day(declared_path, day, label="native declared BBO")
            index[day] = dict(row)
    return path, quality_hash, index


def scan_native_records(
    *,
    daily_quality_csv: Path,
    bbo_root: Path,
    official_aggtrades_root: Path,
    target_dates: Sequence[str],
    symbol: str = "BTCUSDC",
    authority: str = NATIVE_AUTHORITY,
    source_clock: str = NATIVE_SOURCE_CLOCK,
    clock_unit: str = NATIVE_CLOCK_UNIT,
) -> list[dict[str, Any]]:
    """Freeze requested native days from the canonical daily quality table."""

    targets = tuple(_canonical_day(day, label="native target date") for day in target_dates)
    if len(targets) != len(set(targets)):
        raise ValueError("native target dates must be unique")
    quality_path, quality_hash, index = _native_quality_index(daily_quality_csv)
    actual_bbo_root = bbo_root.expanduser().resolve()
    trade_root = official_aggtrades_root.expanduser().resolve()
    records: list[dict[str, Any]] = []
    for day in sorted(targets):
        row = index.get(day)
        if row is None:
            raise FileNotFoundError(f"native quality row missing for target date {day}")
        eligible = _strict_bool(row.get("coverage_99_valid"), label=f"native coverage status {day}")
        if not eligible:
            raise ValueError(f"native target date failed coverage_99_valid: {day}")
        expected_hash = _require_sha256(row.get("bbo_sha256"), label=f"native BBO SHA256 {day}")
        expected_name = f"{symbol}-bbo-{day}.parquet"
        declared_bbo = Path(str(row.get("bbo_source_path", "")))
        if declared_bbo.name != expected_name:
            raise ValueError(f"native BBO filename mismatch for {day}")
        bbo_path = _required_file(actual_bbo_root / expected_name, label=f"native BBO {day}")
        observed_hash = sha256_file(bbo_path)
        if observed_hash != expected_hash:
            raise ValueError(
                f"native BBO hash mismatch for {day}: "
                f"observed={observed_hash} expected={expected_hash}"
            )
        trade_path = _aggtrades_path(trade_root, symbol, day)
        quality_row_sha256 = canonical_sha256(row)
        records.append(
            _record_with_hash(
                {
                    "date": day,
                    "source": NATIVE_SOURCE,
                    "dataset_id": NATIVE_DATASET_ID,
                    "source_authority": authority,
                    "source_clock": source_clock,
                    "clock_unit": clock_unit,
                    "quality_status": {
                        "field": "coverage_99_valid",
                        "eligible": eligible,
                        "formal_eligible": _strict_bool(
                            row.get("formal_eligible", "false"),
                            label=f"native formal status {day}",
                        ),
                        "source_label": str(row.get("source_label", "")),
                        "reconstruction_mode": str(row.get("reconstruction_mode", "")),
                    },
                    "files": {
                        "quality": {
                            "path": str(quality_path),
                            "sha256": quality_hash,
                            "row_sha256": quality_row_sha256,
                            "authority": "native_daily_quality_csv_row",
                            "source_clock": "artifact_build_time",
                        },
                        "bbo": {
                            "path": str(bbo_path),
                            "sha256": observed_hash,
                            "size_bytes": bbo_path.stat().st_size,
                            "authority": authority,
                            "source_clock": source_clock,
                        },
                        "official_aggtrades": _file_identity(
                            trade_path,
                            authority=TRADE_AUTHORITY,
                            source_clock=TRADE_SOURCE_CLOCK,
                        ),
                    },
                }
            )
        )
    return records


def build_source_day_manifest(
    *,
    provider_quality_root: Path,
    provider_bbo_root: Path,
    native_daily_quality_csv: Path,
    native_bbo_root: Path,
    official_aggtrades_root: Path,
    panels: Sequence[PanelSpec | Mapping[str, Any]] | Mapping[str, Mapping[str, Any]],
    overlap_dates: Sequence[str],
    symbol: str = "BTCUSDC",
    provider_authority: str = PROVIDER_AUTHORITY,
    native_authority: str = NATIVE_AUTHORITY,
    native_source_clock: str = NATIVE_SOURCE_CLOCK,
    native_clock_unit: str = NATIVE_CLOCK_UNIT,
    panel_request_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic, source-separated manifest without outcomes."""

    normalized_panels = normalize_panels(panels)
    normalized_overlap = normalize_overlap_dates(overlap_dates)
    provider_targets = {
        day for panel in normalized_panels if panel.source == PROVIDER_SOURCE for day in panel.dates
    } | set(normalized_overlap)
    native_targets = {
        day for panel in normalized_panels if panel.source == NATIVE_SOURCE for day in panel.dates
    } | set(normalized_overlap)

    provider_records = scan_provider_records(
        quality_root=provider_quality_root,
        bbo_root=provider_bbo_root,
        official_aggtrades_root=official_aggtrades_root,
        target_dates=sorted(provider_targets),
        symbol=symbol,
        authority=provider_authority,
    )
    native_records = scan_native_records(
        daily_quality_csv=native_daily_quality_csv,
        bbo_root=native_bbo_root,
        official_aggtrades_root=official_aggtrades_root,
        target_dates=sorted(native_targets),
        symbol=symbol,
        authority=native_authority,
        source_clock=native_source_clock,
        clock_unit=native_clock_unit,
    )
    provider_by_day = {record["date"]: record for record in provider_records}
    native_by_day = {record["date"]: record for record in native_records}

    weighted_records: list[dict[str, Any]] = []
    owner_by_day: dict[str, tuple[str, str]] = {}
    panel_payloads: list[dict[str, Any]] = []
    for panel in normalized_panels:
        source_records = provider_by_day if panel.source == PROVIDER_SOURCE else native_by_day
        panel_payloads.append(
            {"name": panel.name, "source": panel.source, "dates": list(panel.dates)}
        )
        for day in panel.dates:
            record = source_records.get(day)
            if record is None:
                raise ValueError(f"panel {panel.name} lacks frozen {panel.source} record: {day}")
            owner_by_day[day] = (panel.name, panel.source)
            weighted_records.append(
                _record_with_hash(
                    {
                        "date": day,
                        "panel": panel.name,
                        "primary_source": panel.source,
                        "source_record_sha256": record["record_sha256"],
                        "weight": 1,
                    }
                )
            )
    weighted_records.sort(key=lambda row: (row["date"], row["panel"]))

    overlap_records: list[dict[str, Any]] = []
    for day in normalized_overlap:
        provider_record = provider_by_day.get(day)
        native_record = native_by_day.get(day)
        if provider_record is None or native_record is None:
            raise ValueError(f"overlap date must have both source records: {day}")
        owner = owner_by_day.get(day)
        overlap_records.append(
            _record_with_hash(
                {
                    "date": day,
                    "provider_record_sha256": provider_record["record_sha256"],
                    "native_record_sha256": native_record["record_sha256"],
                    "weighted_panel": owner[0] if owner else None,
                    "primary_source": owner[1] if owner else None,
                    "weighting_count": 1 if owner else 0,
                    "duplicate_weighting": False,
                }
            )
        )

    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "symbol": symbol,
        "economic_inputs_read": False,
        "panel_request_identity": dict(panel_request_identity or {}),
        "input_kinds": [
            "provider_per_day_quality_json",
            "native_daily_quality_csv",
            "causal_bbo",
            "binance_futures_official_aggTrades",
            "explicit_panel_assignment",
        ],
        "source_contracts": {
            PROVIDER_SOURCE: {
                "dataset_id": PROVIDER_DATASET_ID,
                "authority": provider_authority,
                "quality_status_field": "provider_normalized_replay_candidate",
                "quality_discovery": "per_day_json_only",
                "combined_provider_quality_csv_read": False,
            },
            NATIVE_SOURCE: {
                "dataset_id": NATIVE_DATASET_ID,
                "authority": native_authority,
                "source_clock": native_source_clock,
                "clock_unit": native_clock_unit,
                "quality_status_field": "coverage_99_valid",
            },
            "official_aggtrades": {
                "authority": TRADE_AUTHORITY,
                "source_clock": TRADE_SOURCE_CLOCK,
                "clock_unit": TRADE_CLOCK_UNIT,
            },
        },
        "panels": panel_payloads,
        "provider_records": provider_records,
        "native_records": native_records,
        "overlap_records": overlap_records,
        "weighted_day_records": weighted_records,
        "weighting_contract": {
            "unit": "UTC_source_day",
            "one_primary_source_per_weighted_date": True,
            "overlap_sources_are_comparison_only": True,
            "weighted_date_count": len(weighted_records),
        },
    }
    manifest["canonical_manifest_sha256"] = canonical_manifest_sha256(manifest)
    validate_source_day_manifest(manifest)
    return manifest


def validate_source_day_manifest(manifest: Mapping[str, Any]) -> None:
    """Validate manifest structure, hashes, split ownership, and permissions."""

    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported F02 source/day manifest schema")
    if manifest.get("identity") != IDENTITY:
        raise ValueError("unexpected F02 reach-time identity")
    if manifest.get("economic_inputs_read") is not False:
        raise ValueError("source/day manifest must not read economic inputs")
    if manifest.get("canonical_manifest_sha256") != canonical_manifest_sha256(manifest):
        raise ValueError("canonical manifest hash mismatch")
    request_identity = manifest.get("panel_request_identity", {})
    if not isinstance(request_identity, Mapping):
        raise ValueError("panel_request_identity must be a mapping")
    if request_identity:
        _require_sha256(
            request_identity.get("sha256"), label="panel request file SHA256"
        )
        _require_sha256(
            request_identity.get("canonical_sha256"),
            label="panel request canonical SHA256",
        )
        if not str(request_identity.get("path", "")).strip():
            raise ValueError("panel request identity path must be non-empty")

    provider_records = list(manifest.get("provider_records", ()))
    native_records = list(manifest.get("native_records", ()))
    for source, records in (
        (PROVIDER_SOURCE, provider_records),
        (NATIVE_SOURCE, native_records),
    ):
        dates = [str(record.get("date")) for record in records]
        if dates != sorted(dates) or len(dates) != len(set(dates)):
            raise ValueError(f"{source} records must be chronological and unique")
        for record in records:
            if record.get("source") != source:
                raise ValueError(f"{source} record source mismatch")
            _canonical_day(record.get("date"), label=f"{source} record date")
            _validate_record_hash(record, label=f"{source} {record.get('date')}")

    normalized_panels = normalize_panels(list(manifest.get("panels", ())))
    provider_by_day = {record["date"]: record for record in provider_records}
    native_by_day = {record["date"]: record for record in native_records}
    expected_weighted: dict[str, tuple[str, str, str]] = {}
    for panel in normalized_panels:
        records = provider_by_day if panel.source == PROVIDER_SOURCE else native_by_day
        for day in panel.dates:
            if day not in records:
                raise ValueError(f"panel {panel.name} lacks {panel.source} record: {day}")
            expected_weighted[day] = (
                panel.name,
                panel.source,
                str(records[day]["record_sha256"]),
            )

    weighted = list(manifest.get("weighted_day_records", ()))
    weighted_dates = [str(record.get("date")) for record in weighted]
    if len(weighted_dates) != len(set(weighted_dates)):
        raise ValueError("a source date is weighted more than once")
    if set(weighted_dates) != set(expected_weighted):
        raise ValueError("weighted day records differ from explicit panels")
    for record in weighted:
        _validate_record_hash(record, label=f"weighted day {record.get('date')}")
        day = str(record["date"])
        panel, source, record_hash = expected_weighted[day]
        if (
            record.get("panel") != panel
            or record.get("primary_source") != source
            or record.get("source_record_sha256") != record_hash
            or record.get("weight") != 1
        ):
            raise ValueError(f"weighted ownership mismatch for {day}")

    overlaps = list(manifest.get("overlap_records", ()))
    overlap_dates = [str(record.get("date")) for record in overlaps]
    if overlap_dates != sorted(overlap_dates) or len(overlap_dates) != len(set(overlap_dates)):
        raise ValueError("overlap records must be chronological and unique")
    for record in overlaps:
        _validate_record_hash(record, label=f"overlap {record.get('date')}")
        day = str(record["date"])
        if day not in provider_by_day or day not in native_by_day:
            raise ValueError(f"overlap record lacks both sources: {day}")
        if record.get("provider_record_sha256") != provider_by_day[day]["record_sha256"]:
            raise ValueError(f"overlap provider identity mismatch for {day}")
        if record.get("native_record_sha256") != native_by_day[day]["record_sha256"]:
            raise ValueError(f"overlap native identity mismatch for {day}")
        expected_count = 1 if day in expected_weighted else 0
        if record.get("weighting_count") != expected_count:
            raise ValueError(f"overlap weighting count mismatch for {day}")
        if record.get("duplicate_weighting") is not False:
            raise ValueError(f"overlap duplicate weighting flag set for {day}")


def load_panel_request(path: Path) -> tuple[Any, tuple[str, ...]]:
    """Load the CLI panel request; it is identity input, not an inferred split."""

    payload = json.loads(_required_file(path, label="panel request JSON").read_text())
    if not isinstance(payload, Mapping) or "panels" not in payload:
        raise ValueError("panel request must be an object containing panels")
    overlap_dates = tuple(str(day) for day in payload.get("overlap_dates", ()))
    return payload["panels"], overlap_dates


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider-quality-root", type=Path, required=True)
    parser.add_argument("--provider-bbo-root", type=Path, required=True)
    parser.add_argument("--native-daily-quality-csv", type=Path, required=True)
    parser.add_argument("--native-bbo-root", type=Path, required=True)
    parser.add_argument("--official-aggtrades-root", type=Path, required=True)
    parser.add_argument("--panels-json", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--provider-authority", default=PROVIDER_AUTHORITY)
    parser.add_argument("--native-authority", default=NATIVE_AUTHORITY)
    parser.add_argument("--native-source-clock", default=NATIVE_SOURCE_CLOCK)
    parser.add_argument("--native-clock-unit", default=NATIVE_CLOCK_UNIT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Print a validated manifest to stdout; never publish one implicitly."""

    args = _parser().parse_args(argv)
    panels, overlap_dates = load_panel_request(args.panels_json)
    manifest = build_source_day_manifest(
        provider_quality_root=args.provider_quality_root,
        provider_bbo_root=args.provider_bbo_root,
        native_daily_quality_csv=args.native_daily_quality_csv,
        native_bbo_root=args.native_bbo_root,
        official_aggtrades_root=args.official_aggtrades_root,
        panels=panels,
        overlap_dates=overlap_dates,
        symbol=args.symbol,
        provider_authority=args.provider_authority,
        native_authority=args.native_authority,
        native_source_clock=args.native_source_clock,
        native_clock_unit=args.native_clock_unit,
    )
    json.dump(manifest, sys.stdout, indent=2, sort_keys=True, allow_nan=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
