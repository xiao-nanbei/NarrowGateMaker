#!/usr/bin/env python3
"""Audit exact physical inputs for F03 1s 2026 native panels.

This auditor is deliberately read-only.  It never discovers replacement files,
materializes features, reads predictions/economic outcomes, or changes a frozen
panel.  Every candidate path is derived from a named profile and UTC day.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from data_paths import data_root
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as daily_sources,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_orico_source_spec as source_spec,
)

SCHEMA_VERSION = "causal_v12_1s_2026_native_source_coverage.v1"
AUDIT_IDENTITY = "causal_v12_1s_2026_native_source_coverage_v1"
PROPOSED_PROFILE_ID = "native_historical_minimal141_individual_reference_v1_candidate"

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MARKET_DATA_ROOT = data_root(ROOT)
DEFAULT_TRANSPORT_SPEC = Path(
    "research/families/f03_causal_13_head/docs/causal_v12_native_transport_audit_spec_20260802.json"
)
DEFAULT_ECONOMIC_SPEC = Path(
    "research/families/f03_causal_13_head/docs/causal_v12_native_full_path_ml_ab_spec_20260802.json"
)
DEFAULT_40_DAY_SPEC = Path(
    "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2_mechanics_spec_"
    "20260803.json"
)

# The proposed profile is intentionally homogeneous.  These exact dates have
# another 1s reference artifact, but it is aggTrades-derived and is not used as
# a fallback for the individual-trades authority required here.
KNOWN_ALTERNATE_REFERENCE_DAYS = frozenset(
    {
        "2026-04-12",
        "2026-05-07",
        "2026-05-08",
        "2026-05-10",
        "2026-05-11",
        "2026-05-14",
        "2026-05-16",
    }
)


class CoverageContractError(ValueError):
    """Raised when a frozen panel or source authority is malformed."""


@dataclass(frozen=True, slots=True)
class FrozenPanel:
    panel_id: str
    role: str
    days: tuple[str, ...]
    spec_path: Path
    spec_sha256: str


@dataclass(frozen=True, slots=True)
class HistoricalNativeRoots:
    market_data_root: Path
    local_tempo_dir: Path
    local_tempo_manifest: Path
    native_l2_dir: Path
    native_l2_manifest: Path
    native_l2_quality: Path
    metrics_dir: Path
    reference_dir: Path
    alternate_reference_dir: Path
    raw_reference_dir: Path

    @classmethod
    def from_market_data_root(cls, root: Path) -> HistoricalNativeRoots:
        resolved = root.expanduser().resolve()
        return cls(
            market_data_root=resolved,
            local_tempo_dir=(
                resolved / "trade_features_causal_v5_expanded_20250801_20260725" / "BTCUSDC"
            ),
            local_tempo_manifest=(
                resolved / "trade_features_causal_v5_expanded_20250801_20260725" / "manifest.json"
            ),
            native_l2_dir=(resolved / "normalized_l2_100ms_v2_minimal141_20260727" / "l2"),
            native_l2_manifest=(
                resolved / "normalized_l2_100ms_v2_minimal141_20260727" / "manifest.json"
            ),
            native_l2_quality=(
                resolved / "normalized_l2_100ms_v2_minimal141_20260727" / "daily_quality.csv"
            ),
            metrics_dir=resolved / "raw_metrics",
            reference_dir=resolved / "reference_bars_1s_trades_v1",
            alternate_reference_dir=resolved / "bars_1s",
            raw_reference_dir=resolved / "raw_trades" / "BTCUSDT",
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_day(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise CoverageContractError(f"invalid UTC day: {value!r}") from exc
    if parsed.strftime("%Y-%m-%d") != value:
        raise CoverageContractError(f"non-canonical UTC day: {value!r}")
    return value


def previous_day(day: str) -> str:
    parsed = datetime.strptime(_canonical_day(day), "%Y-%m-%d").replace(tzinfo=UTC)
    return (parsed - timedelta(days=1)).strftime("%Y-%m-%d")


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CoverageContractError(f"JSON root must be an object: {path}")
    return payload


def _ordered_days(values: Sequence[Any], *, expected_count: int, label: str) -> tuple[str, ...]:
    days = tuple(_canonical_day(str(value)) for value in values)
    if len(days) != expected_count:
        raise CoverageContractError(
            f"{label}: expected {expected_count} days, observed {len(days)}"
        )
    if len(set(days)) != len(days):
        raise CoverageContractError(f"{label}: duplicate UTC days")
    if tuple(sorted(days)) != days:
        raise CoverageContractError(f"{label}: days are not chronological")
    return days


def load_frozen_panels(
    *,
    transport_spec_path: Path,
    economic_spec_path: Path,
    forty_day_spec_path: Path,
) -> tuple[FrozenPanel, ...]:
    """Load the frozen 22+22 transport/economic and 40-day denominators."""

    transport_path = transport_spec_path.resolve()
    economic_path = economic_spec_path.resolve()
    forty_path = forty_day_spec_path.resolve()
    transport = _load_json(transport_path)
    economic = _load_json(economic_path)
    forty = _load_json(forty_path)

    transport_panels = transport.get("panels")
    economic_panels = economic.get("panels")
    if not isinstance(transport_panels, list) or len(transport_panels) != 2:
        raise CoverageContractError("transport spec must contain exactly two panels")
    if not isinstance(economic_panels, list) or len(economic_panels) != 2:
        raise CoverageContractError("economic spec must contain exactly two panels")

    transport_loaded: list[FrozenPanel] = []
    for index, raw in enumerate(transport_panels):
        if not isinstance(raw, Mapping):
            raise CoverageContractError("transport panel must be an object")
        role = str(raw.get("role", ""))
        days = _ordered_days(
            raw.get("days", ()), expected_count=22, label=f"transport panel {index}"
        )
        transport_loaded.append(
            FrozenPanel(
                panel_id=f"transport_{index + 1}",
                role=role,
                days=days,
                spec_path=transport_path,
                spec_sha256=sha256_file(transport_path),
            )
        )

    for index, (transport_panel, raw) in enumerate(
        zip(transport_loaded, economic_panels, strict=True)
    ):
        if not isinstance(raw, Mapping):
            raise CoverageContractError("economic panel must be an object")
        economic_days = _ordered_days(
            raw.get("days", ()), expected_count=22, label=f"economic panel {index}"
        )
        if str(raw.get("role", "")) != transport_panel.role:
            raise CoverageContractError("transport/economic panel role mismatch")
        if economic_days != transport_panel.days:
            raise CoverageContractError("transport/economic panel day mismatch")

    forty_panels = forty.get("panels")
    if not isinstance(forty_panels, Mapping):
        raise CoverageContractError("40-day spec lacks panels object")
    development_days = _ordered_days(
        forty_panels.get("development_days", ()),
        expected_count=40,
        label="40-day development panel",
    )
    return (
        *transport_loaded,
        FrozenPanel(
            panel_id="economic_22_plus_22",
            role="historical_native_full_path_ml_ab_same_22_plus_22",
            days=tuple(day for panel in transport_loaded for day in panel.days),
            spec_path=economic_path,
            spec_sha256=sha256_file(economic_path),
        ),
        FrozenPanel(
            panel_id="development_40",
            role="frozen_causal_v12_development_40",
            days=development_days,
            spec_path=forty_path,
            spec_sha256=sha256_file(forty_path),
        ),
    )


def required_day_roles(panels: Sequence[FrozenPanel]) -> dict[str, tuple[str, ...]]:
    roles: dict[str, set[str]] = {}
    for panel in panels:
        for target_day in panel.days:
            roles.setdefault(target_day, set()).add("target")
            roles.setdefault(previous_day(target_day), set()).add("warmup")
    return {day: tuple(sorted(values)) for day, values in sorted(roles.items())}


class FileIdentityCache:
    def __init__(self) -> None:
        self._records: dict[Path, dict[str, Any]] = {}

    def record(self, path: Path) -> dict[str, Any]:
        resolved = path.expanduser().resolve()
        cached = self._records.get(resolved)
        if cached is not None:
            return dict(cached)
        if not resolved.is_file():
            record = {
                "path": str(resolved),
                "exists": False,
                "size_bytes": None,
                "sha256": None,
            }
        else:
            record = {
                "path": str(resolved),
                "exists": True,
                "size_bytes": resolved.stat().st_size,
                "sha256": sha256_file(resolved),
            }
        self._records[resolved] = record
        return dict(record)


def _bool(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def _unique_index(
    rows: Sequence[Mapping[str, Any]], key: str, *, label: str
) -> dict[str, Mapping[str, Any]]:
    output: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        value = str(row.get(key, ""))
        if not value:
            raise CoverageContractError(f"{label}: empty {key}")
        if value in output:
            raise CoverageContractError(f"{label}: duplicate {key}={value}")
        output[value] = row
    return output


def _manifest_authorities(
    roots: HistoricalNativeRoots, cache: FileIdentityCache
) -> tuple[
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
    dict[str, Any],
]:
    tempo_manifest = _load_json(roots.local_tempo_manifest)
    if tempo_manifest.get("schema") != "narrowgate.taker_tempo_manifest.v1":
        raise CoverageContractError("unsupported local tempo manifest schema")
    tempo_rows = tempo_manifest.get("daily_files")
    if not isinstance(tempo_rows, list):
        raise CoverageContractError("local tempo manifest lacks daily_files")
    tempo_by_day = _unique_index(tempo_rows, "day", label="local tempo manifest")

    l2_manifest = _load_json(roots.native_l2_manifest)
    if l2_manifest.get("dataset_version") != "normalized_l2_100ms_v2":
        raise CoverageContractError("unsupported native L2 dataset version")
    l2_rows = [
        row
        for row in l2_manifest.get("files", ())
        if isinstance(row, Mapping) and row.get("kind") == "l2"
    ]
    l2_by_day = _unique_index(l2_rows, "day", label="native L2 manifest")

    with roots.native_l2_quality.open(newline="", encoding="utf-8") as handle:
        quality_rows = list(csv.DictReader(handle))
    quality_by_day = _unique_index(quality_rows, "day", label="native L2 quality")

    declared_quality = l2_manifest.get("daily_quality")
    if not isinstance(declared_quality, Mapping):
        raise CoverageContractError("native L2 manifest lacks daily_quality identity")
    actual_quality = cache.record(roots.native_l2_quality)
    if actual_quality["sha256"] != declared_quality.get("sha256"):
        raise CoverageContractError("native L2 daily_quality SHA256 mismatch")
    authorities = {
        "local_tempo_manifest": cache.record(roots.local_tempo_manifest),
        "native_l2_manifest": cache.record(roots.native_l2_manifest),
        "native_l2_quality": actual_quality,
        "native_l2_quality_declared_sha256": declared_quality.get("sha256"),
    }
    return tempo_by_day, l2_by_day, quality_by_day, authorities


def _component_result(
    identity: Mapping[str, Any], *, errors: Sequence[str], extra: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    result = dict(identity)
    if extra:
        result.update(extra)
    result["valid"] = not errors
    result["errors"] = list(errors)
    return result


def _audit_tempo_day(
    day: str,
    roots: HistoricalNativeRoots,
    manifest_by_day: Mapping[str, Mapping[str, Any]],
    cache: FileIdentityCache,
) -> dict[str, Any]:
    path = roots.local_tempo_dir / f"BTCUSDC-trade-tempo-{day}.parquet"
    identity = cache.record(path)
    errors: list[str] = []
    entry = manifest_by_day.get(day)
    if entry is None:
        errors.append("local_tempo_manifest_entry_missing")
    if not identity["exists"]:
        errors.append("local_tempo_file_missing")
        return _component_result(identity, errors=errors)
    rows = pq.ParquetFile(path).metadata.num_rows
    if entry is not None:
        if entry.get("sidecar_sha256") != identity["sha256"]:
            errors.append("local_tempo_sha256_mismatch")
        if int(entry.get("sidecar_size_bytes", -1)) != identity["size_bytes"]:
            errors.append("local_tempo_size_mismatch")
        if int(entry.get("sidecar_rows", -1)) != rows:
            errors.append("local_tempo_row_count_mismatch")
    return _component_result(identity, errors=errors, extra={"rows": rows})


def _audit_l2_day(
    day: str,
    roots: HistoricalNativeRoots,
    manifest_by_day: Mapping[str, Mapping[str, Any]],
    quality_by_day: Mapping[str, Mapping[str, Any]],
    cache: FileIdentityCache,
) -> dict[str, Any]:
    path = roots.native_l2_dir / f"BTCUSDC-l2-{day}.parquet"
    identity = cache.record(path)
    errors: list[str] = []
    manifest_row = manifest_by_day.get(day)
    quality = quality_by_day.get(day)
    if manifest_row is None:
        errors.append("native_l2_manifest_entry_missing")
    if quality is None:
        errors.append("native_l2_quality_row_missing")
    if not identity["exists"]:
        errors.append("native_l2_file_missing")
    if identity["exists"] and manifest_row is not None:
        expected_relative = f"l2/BTCUSDC-l2-{day}.parquet"
        if manifest_row.get("destination_relative_path") != expected_relative:
            errors.append("native_l2_destination_identity_mismatch")
        source_identity = manifest_row.get("source_identity")
        if not isinstance(source_identity, Mapping):
            errors.append("native_l2_source_identity_missing")
        else:
            if source_identity.get("sha256") != identity["sha256"]:
                errors.append("native_l2_manifest_sha256_mismatch")
            if int(source_identity.get("size_bytes", -1)) != identity["size_bytes"]:
                errors.append("native_l2_manifest_size_mismatch")
    if identity["exists"] and quality is not None:
        if quality.get("l2_sha256") != identity["sha256"]:
            errors.append("native_l2_quality_sha256_mismatch")
        if int(quality.get("l2_size_bytes", -1)) != identity["size_bytes"]:
            errors.append("native_l2_quality_size_mismatch")
        # Target eligibility and previous-natural-day warmup eligibility are
        # different contracts.  sequence/coverage can exclude a target while
        # its end-of-day state remains an admitted midnight warmup.
        for field in (
            "target_source_valid",
            "source_formal_capable",
            "cadence_schema_valid",
        ):
            if not _bool(quality.get(field)):
                errors.append(f"native_l2_quality_{field}_false")
    extra = {
        "formal_eligible": False if quality is None else _bool(quality.get("formal_eligible")),
        "warmup_valid": False if quality is None else _bool(quality.get("warmup_valid")),
        "formal_exclusion_reason": None
        if quality is None
        else str(quality.get("formal_exclusion_reason", "")),
        "source_label": None if quality is None else quality.get("source_label"),
        "reconstruction_mode": None if quality is None else quality.get("reconstruction_mode"),
    }
    return _component_result(identity, errors=errors, extra=extra)


def _audit_metrics_day(
    day: str, roots: HistoricalNativeRoots, cache: FileIdentityCache
) -> dict[str, Any]:
    path = roots.metrics_dir / f"BTCUSDC-metrics-{day}.csv"
    identity = cache.record(path)
    errors: list[str] = []
    audit_payload: dict[str, Any] = {}
    if not identity["exists"]:
        errors.append("metrics_file_missing")
    else:
        try:
            audit = daily_sources.read_metrics_with_audit((path,))
            if len(audit.files) != 1:
                errors.append("metrics_file_audit_count_mismatch")
            else:
                audit_payload = audit.files[0].audit_payload()
                audit_payload.pop("path", None)
        except Exception as exc:  # fail closed on the existing strict parser
            errors.append(f"metrics_contract_error:{type(exc).__name__}:{exc}")
    return _component_result(identity, errors=errors, extra=audit_payload)


def _audit_reference_day(
    day: str, roots: HistoricalNativeRoots, cache: FileIdentityCache
) -> dict[str, Any]:
    path = roots.reference_dir / f"BTCUSDT-1s-{day}.parquet"
    meta_path = roots.reference_dir / f"BTCUSDT-1s-{day}.parquet.meta.json"
    identity = cache.record(path)
    meta_identity = cache.record(meta_path)
    errors: list[str] = []
    payload: Mapping[str, Any] = {}
    if not identity["exists"]:
        errors.append("reference_bar_file_missing")
    if not meta_identity["exists"]:
        errors.append("reference_bar_authority_missing")
    if identity["exists"] and meta_identity["exists"]:
        try:
            payload = _load_json(meta_path)
            if payload.get("schema_version") != "binance_individual_trade_bar_1s.v1":
                errors.append("reference_bar_schema_mismatch")
            if payload.get("symbol") != "BTCUSDT" or payload.get("utc_day") != day:
                errors.append("reference_bar_symbol_day_mismatch")
            if payload.get("source_data_type") != "trades":
                errors.append("reference_bar_not_individual_trades")
            if payload.get("complete") is not True:
                errors.append("reference_bar_not_complete")
            if payload.get("bar_interval") != "[t,t+1s)":
                errors.append("reference_bar_interval_mismatch")
            if payload.get("causal_visible_at") != "t+1s":
                errors.append("reference_bar_visibility_mismatch")
            if payload.get("output_sha256") != identity["sha256"]:
                errors.append("reference_bar_output_sha256_mismatch")
            if int(payload.get("rows", -1)) != pq.ParquetFile(path).metadata.num_rows:
                errors.append("reference_bar_row_count_mismatch")
        except Exception as exc:
            errors.append(f"reference_bar_contract_error:{type(exc).__name__}:{exc}")

    alternate_path = roots.alternate_reference_dir / f"BTCUSDT-1s-{day}.parquet"
    alternate_meta = roots.alternate_reference_dir / f"BTCUSDT-1s-{day}.parquet.meta.json"
    alternate_observed = day in KNOWN_ALTERNATE_REFERENCE_DAYS
    alternate = {
        "known_exact_alternate_day": alternate_observed,
        "used": False,
        "reason": "alternate_source_is_not_a_warmup_or_authority_fallback",
        "bar": cache.record(alternate_path) if alternate_observed else None,
        "authority": cache.record(alternate_meta) if alternate_observed else None,
    }
    raw_path = roots.raw_reference_dir / f"BTCUSDT-trades-{day}.csv"
    return _component_result(
        identity,
        errors=errors,
        extra={
            "authority": meta_identity,
            "authority_source_data_type": payload.get("source_data_type"),
            "recorded_raw_source_path": payload.get("source_path"),
            "orico_raw_individual_trades_path": str(raw_path.resolve()),
            "orico_raw_individual_trades_exists": raw_path.is_file(),
            "alternate_artifact_observed_not_used": alternate,
        },
    )


def audit_historical_native_profile(
    *,
    roots: HistoricalNativeRoots,
    day_roles: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    """Audit one explicit, no-fallback historical native profile."""

    cache = FileIdentityCache()
    tempo_by_day, l2_by_day, quality_by_day, authorities = _manifest_authorities(roots, cache)
    days: list[dict[str, Any]] = []
    for day, roles in day_roles.items():
        tempo = _audit_tempo_day(day, roots, tempo_by_day, cache)
        l2 = _audit_l2_day(day, roots, l2_by_day, quality_by_day, cache)
        metrics = _audit_metrics_day(day, roots, cache)
        reference = _audit_reference_day(day, roots, cache)
        shared_valid = all(item["valid"] for item in (tempo, l2, metrics, reference))
        days.append(
            {
                "day": day,
                "roles": list(roles),
                "shared_sources_valid": shared_valid,
                "target_role_valid": shared_valid and bool(l2["formal_eligible"]),
                "warmup_role_valid": shared_valid and bool(l2["warmup_valid"]),
                "components": {
                    "local_trade_tempo": tempo,
                    "native_normalized_l2_and_quality": l2,
                    "metrics": metrics,
                    "btcusdt_reference_bars_and_authority": reference,
                },
            }
        )
    return {
        "profile_id": PROPOSED_PROFILE_ID,
        "profile_status": "coverage_audited_not_implemented_in_source_resolver",
        "fallback_discovery_allowed": False,
        "substitute_warmup_allowed": False,
        "reference_source_identity": "binance_futures_reference_individual_trades_1s.v1",
        "reference_source_mixing_allowed": False,
        "execution_l2_clock_identity": "cryptohft_transaction_time_100ms_grid",
        "component_roots": {
            "local_trade_tempo": str(roots.local_tempo_dir),
            "local_trade_tempo_manifest": str(roots.local_tempo_manifest),
            "native_l2": str(roots.native_l2_dir),
            "native_l2_manifest": str(roots.native_l2_manifest),
            "native_l2_quality": str(roots.native_l2_quality),
            "metrics": str(roots.metrics_dir),
            "reference_bars": str(roots.reference_dir),
            "reference_alternate_observed_not_used": str(roots.alternate_reference_dir),
        },
        "authority_files": authorities,
        "required_day_count": len(days),
        "days": days,
    }


def _date_range(first_day: str, last_day: str) -> tuple[str, ...]:
    start = datetime.strptime(_canonical_day(first_day), "%Y-%m-%d").replace(tzinfo=UTC)
    end = datetime.strptime(_canonical_day(last_day), "%Y-%m-%d").replace(tzinfo=UTC)
    if end < start:
        raise CoverageContractError("date range is reversed")
    output: list[str] = []
    current = start
    while current <= end:
        output.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return tuple(output)


def _current_profile_path_records(
    market_data_root: Path, target_day: str, cache: FileIdentityCache
) -> dict[str, Any]:
    profile = source_spec.PROFILES[source_spec.NATIVE_NORMALIZED_PROFILE]
    required = (previous_day(target_day), target_day)
    root = market_data_root.resolve()
    groups = {
        "local_trade_tempo": [
            root / profile.local_trade_tempo_dir / f"BTCUSDC-trade-tempo-{day}.parquet"
            for day in required
        ],
        "local_trade_tempo_manifest": [root / profile.local_manifest_path],
        "native_l2": [
            root / profile.execution_l2_dir / f"BTCUSDC-l2-{day}.parquet" for day in required
        ],
        "native_l2_quality": [
            root / profile.execution_l2_quality_dir / f"BTCUSDC-{day}.json" for day in required
        ],
        "metrics": [root / profile.metrics_dir / f"BTCUSDC-metrics-{day}.csv" for day in required],
        "reference_bars": [
            root / profile.reference_bar_dir / f"BTCUSDT-1s-{day}.parquet" for day in required
        ],
        "reference_authority": [
            root / profile.reference_bar_dir / f"BTCUSDT-1s-{day}.parquet.meta.json"
            for day in required
        ],
    }
    return {name: [cache.record(path) for path in paths] for name, paths in groups.items()}


def audit_current_native_profile(market_data_root: Path) -> dict[str, Any]:
    """Report the exact target coverage of the existing postfit profile."""

    root = market_data_root.resolve()
    profile = source_spec.PROFILES[source_spec.NATIVE_NORMALIZED_PROFILE]
    manifest_path = root / profile.local_manifest_path
    manifest = _load_json(manifest_path)
    candidate_days = _date_range(str(manifest["first_day"]), str(manifest["last_day"]))
    cache = FileIdentityCache()
    rows: list[dict[str, Any]] = []
    for day in candidate_days:
        path_records = _current_profile_path_records(root, day, cache)
        try:
            built = source_spec.build_orico_daily_source_spec(
                target_day=day,
                market_data_root=root,
                profile_id=source_spec.NATIVE_NORMALIZED_PROFILE,
            )
            valid = bool(built.probe.get("physical_materialization_eligible"))
            errors = list(built.probe.get("failure_reasons", ()))
        except Exception as exc:
            valid = False
            errors = [f"{type(exc).__name__}:{exc}"]
        rows.append(
            {
                "target_day": day,
                "required_days": [previous_day(day), day],
                "valid": valid,
                "errors": errors,
                "exact_paths": path_records,
            }
        )
    return {
        "profile_id": source_spec.NATIVE_NORMALIZED_PROFILE,
        "profile_scope": "postfit_20260726_31",
        "fallback_discovery_allowed": False,
        "candidate_target_days": list(candidate_days),
        "accepted_target_days": [row["target_day"] for row in rows if row["valid"]],
        "rejected_target_days": [row["target_day"] for row in rows if not row["valid"]],
        "source_resolver": {
            "path": str(Path(source_spec.__file__).resolve()),
            "sha256": sha256_file(Path(source_spec.__file__).resolve()),
        },
        "days": rows,
    }


def _day_index(profile: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(row["day"]): row for row in profile.get("days", ())}


def panel_coverage(
    panels: Sequence[FrozenPanel], profile: Mapping[str, Any]
) -> list[dict[str, Any]]:
    by_day = _day_index(profile)
    output: list[dict[str, Any]] = []
    for panel in panels:
        accepted: list[str] = []
        rejected: list[dict[str, Any]] = []
        for target_day in panel.days:
            warmup_day = previous_day(target_day)
            target = by_day.get(target_day)
            warmup = by_day.get(warmup_day)
            reasons: list[str] = []
            if target is None:
                reasons.append("target_day_not_audited")
            elif not target.get("target_role_valid"):
                reasons.extend(_daily_failure_reasons(target, prefix="target"))
            if warmup is None:
                reasons.append("warmup_day_not_audited")
            elif not warmup.get("warmup_role_valid"):
                reasons.extend(_daily_failure_reasons(warmup, prefix="warmup"))
            if reasons:
                rejected.append(
                    {
                        "target_day": target_day,
                        "warmup_day": warmup_day,
                        "reasons": sorted(set(reasons)),
                    }
                )
            else:
                accepted.append(target_day)
        output.append(
            {
                "panel_id": panel.panel_id,
                "role": panel.role,
                "spec_path": str(panel.spec_path),
                "spec_sha256": panel.spec_sha256,
                "target_day_count": len(panel.days),
                "accepted_count": len(accepted),
                "rejected_count": len(rejected),
                "accepted_days": accepted,
                "rejected_days": rejected,
                "complete": not rejected,
            }
        )
    return output


def _daily_failure_reasons(day: Mapping[str, Any], *, prefix: str) -> list[str]:
    reasons: list[str] = []
    for component_name, component in day.get("components", {}).items():
        if not component.get("valid"):
            component_errors = component.get("errors") or ["invalid"]
            reasons.extend(f"{prefix}:{component_name}:{error}" for error in component_errors)
    l2 = day.get("components", {}).get("native_normalized_l2_and_quality", {})
    role_field = "formal_eligible" if prefix == "target" else "warmup_valid"
    if not l2.get(role_field):
        reasons.append(f"{prefix}:native_normalized_l2_and_quality:{role_field}_false")
    return reasons


def build_report(
    *,
    market_data_root: Path,
    transport_spec_path: Path,
    economic_spec_path: Path,
    forty_day_spec_path: Path,
) -> dict[str, Any]:
    panels = load_frozen_panels(
        transport_spec_path=transport_spec_path,
        economic_spec_path=economic_spec_path,
        forty_day_spec_path=forty_day_spec_path,
    )
    roles = required_day_roles(panels)
    roots = HistoricalNativeRoots.from_market_data_root(market_data_root)
    proposed = audit_historical_native_profile(roots=roots, day_roles=roles)
    coverage = panel_coverage(panels, proposed)
    current = audit_current_native_profile(market_data_root)

    unique_target_days = sorted({day for panel in panels for day in panel.days})
    proposed_by_day = _day_index(proposed)
    accepted_unique = [
        day
        for day in unique_target_days
        if proposed_by_day[day]["target_role_valid"]
        and proposed_by_day[previous_day(day)]["warmup_role_valid"]
    ]
    rejected_unique = sorted(set(unique_target_days) - set(accepted_unique))
    invalid_component_days: dict[str, list[str]] = {}
    for day, row in proposed_by_day.items():
        for component_name, component in row["components"].items():
            if not component["valid"]:
                invalid_component_days.setdefault(component_name, []).append(day)
    identity_payload = {
        "schema_version": SCHEMA_VERSION,
        "audit_identity": AUDIT_IDENTITY,
        "market_data_root": str(market_data_root.resolve()),
        "panel_specs": [
            {"path": str(panel.spec_path), "sha256": panel.spec_sha256} for panel in panels
        ],
        "proposed_profile_id": PROPOSED_PROFILE_ID,
        "required_days": list(roles),
        "auditor_sha256": sha256_file(Path(__file__).resolve()),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "audit_identity": AUDIT_IDENTITY,
        "audit_identity_sha256": canonical_sha256(identity_payload),
        "audited_at_utc": datetime.now(tz=UTC).isoformat(),
        "scope": {
            "read_only": True,
            "glob_discovery_used": False,
            "fallback_source_used": False,
            "substitute_warmup_used": False,
            "large_data_materialized": False,
            "training_run": False,
            "predictions_read": False,
            "economic_outcomes_read": False,
            "pnl_read": False,
        },
        "input_identity": identity_payload,
        "panel_denominators": [
            {
                "panel_id": panel.panel_id,
                "role": panel.role,
                "days": list(panel.days),
                "spec_path": str(panel.spec_path),
                "spec_sha256": panel.spec_sha256,
            }
            for panel in panels
        ],
        "denominator_summary": {
            "transport_days": 44,
            "economic_22_plus_22_days": 44,
            "development_40_days": 40,
            "unique_target_days": len(unique_target_days),
            "required_D_minus_1_or_D_days": len(roles),
        },
        "existing_native_normalized_profile": {
            **current,
            "frozen_panel_unique_target_days_covered": sorted(
                set(current["accepted_target_days"]) & set(unique_target_days)
            ),
        },
        "proposed_explicit_profile": proposed,
        "proposed_profile_panel_coverage": coverage,
        "proposed_profile_unique_target_summary": {
            "accepted_count": len(accepted_unique),
            "rejected_count": len(rejected_unique),
            "accepted_days": accepted_unique,
            "rejected_days": rejected_unique,
        },
        "decision": {
            "existing_native_profile_covers_frozen_panels": False,
            "new_explicit_profile_40_day_physically_complete": next(
                row["complete"] for row in coverage if row["panel_id"] == "development_40"
            ),
            "new_explicit_profile_22_plus_22_physically_complete": all(
                row["complete"] for row in coverage if row["panel_id"].startswith("transport_")
            ),
            "local_trade_tempo_blocker": bool(invalid_component_days.get("local_trade_tempo")),
            "native_l2_quality_blocker": bool(
                invalid_component_days.get("native_normalized_l2_and_quality")
            ),
            "metrics_blocker": bool(invalid_component_days.get("metrics")),
            "reference_authority_blocker": bool(
                invalid_component_days.get("btcusdt_reference_bars_and_authority")
            ),
            "invalid_component_days": invalid_component_days,
            "remaining_blockers": [
                {
                    "id": "missing_individual_trade_reference_bar_artifacts",
                    "required_days": sorted(
                        invalid_component_days.get("btcusdt_reference_bars_and_authority", ())
                    ),
                    "alternate_source_substituted": False,
                },
                {
                    "id": "metrics_causal_clock_contract",
                    "required_days": sorted(invalid_component_days.get("metrics", ())),
                    "source_rows_reordered_or_rewritten": False,
                },
            ],
            "source_aware_reference_successor_possible": True,
            "source_aware_reference_successor_requires_new_identity": True,
            "current_source_resolver_modified": False,
        },
        "permissions": {
            "feature_materialization_authorized": False,
            "training_authorized": False,
            "transport_scoring_authorized": False,
            "economic_replay_authorized": False,
            "action_authorized": False,
            "baseline_authorized": False,
            "live_authorized": False,
        },
    }


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    output = path.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-data-root", type=Path, default=DEFAULT_MARKET_DATA_ROOT)
    parser.add_argument("--transport-spec", type=Path, default=DEFAULT_TRANSPORT_SPEC)
    parser.add_argument("--economic-spec", type=Path, default=DEFAULT_ECONOMIC_SPEC)
    parser.add_argument("--forty-day-spec", type=Path, default=DEFAULT_40_DAY_SPEC)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = build_report(
        market_data_root=args.market_data_root,
        transport_spec_path=args.transport_spec,
        economic_spec_path=args.economic_spec,
        forty_day_spec_path=args.forty_day_spec,
    )
    if args.output is None:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _atomic_write_json(args.output, report)
        print(args.output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
