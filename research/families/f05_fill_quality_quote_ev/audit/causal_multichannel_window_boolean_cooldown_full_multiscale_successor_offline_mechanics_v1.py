#!/usr/bin/env python3
"""Admit outcome-blind mechanics inputs for the offline F05 successor.

This module does not build features, labels, counterfactuals, or replay rows.  It
first publishes an immutable same-filesystem hardlink view over normalized BBO
and L2 files already admitted by the canonical source manifest.  It can then
admit five externally materialized, outcome-blind mechanics tables.  Economic
columns and precomputed action labels fail closed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import math
import os
import re
import shutil
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

MECHANICS_IDENTITY = f"{offline.IDENTITY}.offline_mechanics_v1"
BOOK_VIEW_SCHEMA_VERSION = f"{MECHANICS_IDENTITY}.normalized_book_view.v2"
LEGACY_PANEL_SCHEMA_VERSION = f"{offline.IDENTITY}.nested_oof_panel_manifest.v1"
PANEL_SCHEMA_VERSION = f"{offline.IDENTITY}.nested_oof_panel_manifest.v2"
SEQUENTIAL_PANEL_BUILDER_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_panel_builder_v1"
)
SOURCE_AUTHORITY = "native_normalized_modeled_queue"
SYMBOL = offline.SYMBOL
BOOK_KINDS = ("bbo", "l2")
PANEL_FILE_ROLES = (
    "metadata",
    "boolean_features",
    "continuous_features",
    "exact_owner_actions",
    "replay_inputs",
)
OWNER_ARTIFACT_ROLES = ("policy", "predicate_bundle", "private_config")
OWNER_ACTION_VOCABULARY = frozenset(
    {"CONTROL_85N", "FIXED_166S", "FIXED_211S", "FIXED_1748S"}
)
DAILY_QUALITY_COLUMNS = (
    "day",
    "target_day",
    "source_authority",
    "queue_identity",
    "exact_queue_policy_eligible",
    "cadence_schema_valid",
    "coverage_99_valid",
    "formal_eligible",
    "provider_sensitivity_replay_eligible",
    "same_millisecond_ambiguity_policy",
    "bbo_rows",
    "l2_rows",
    "bbo_schema_sha256",
    "l2_schema_sha256",
    "bbo_sha256",
    "l2_sha256",
    "bbo_size_bytes",
    "l2_size_bytes",
    "bbo_source_path",
    "l2_source_path",
)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FORBIDDEN_ECONOMIC_COLUMN_PARTS = (
    "pnl",
    "profit",
    "reward",
    "economic_outcome",
    "action_outcome",
    "terminal_value",
    "closed_campaign_value",
    "campaign_value_usdc",
    "markout",
    "realized_value",
    "unrealized_value",
    "value_label",
)
_FALSE_ONLY_GOVERNANCE_COLUMNS = frozenset(
    {
        "candidate_actions_generated",
        "continuation_creates_target_assignments",
        "economic_outcomes_read",
        "labels_read",
    }
)


class OfflineMechanicsError(RuntimeError):
    """Raised when a mechanics artifact violates the outcome-blind contract."""


@dataclass(frozen=True, slots=True)
class PortableRoots:
    """Filesystem roots used to encode and resolve public portable markers."""

    repository_root: Path
    project_data_root: Path
    marketdata_root: Path

    @classmethod
    def from_layout(
        cls,
        layout: offline.OfflineSourceLayout,
        *,
        repository_root: Path | None = None,
    ) -> PortableRoots:
        root = repository_root or Path(__file__).resolve().parents[4]
        return cls(
            repository_root=root.expanduser().resolve(),
            project_data_root=layout.project_data_root.expanduser().resolve(),
            marketdata_root=layout.marketdata_root.expanduser().resolve(),
        )

    def marker_roots(self) -> tuple[tuple[str, Path], ...]:
        values = (
            ("${NARROWGATE_MARKETDATA_ROOT}", self.marketdata_root),
            ("${NARROWGATE_DATA_ROOT}", self.project_data_root),
            ("${NARROWGATE_ROOT}", self.repository_root),
        )
        return tuple(sorted(values, key=lambda item: len(str(item[1])), reverse=True))


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def canonical_document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return canonical_sha256(payload)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_owner_hashes() -> dict[str, str]:
    return {
        "policy": offline.ACTIVE_OWNER_POLICY_SHA256,
        "predicate_bundle": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "private_config": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    }


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineMechanicsError(f"cannot load {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise OfflineMechanicsError(f"{label} root must be an object")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="ascii",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _portable_path(path: Path, *, roots: PortableRoots) -> str:
    resolved = path.expanduser().resolve()
    for marker, root in roots.marker_roots():
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            continue
        return marker if not relative.parts else f"{marker}/{relative.as_posix()}"
    raise OfflineMechanicsError(f"path lies outside portable roots: {resolved}")


def _resolve_portable(value: Any, *, roots: PortableRoots) -> Path:
    if not isinstance(value, str) or not value:
        raise OfflineMechanicsError("portable path binding is missing")
    for marker, root in roots.marker_roots():
        if value == marker:
            return root
        prefix = marker + "/"
        if value.startswith(prefix):
            return (root / value[len(prefix) :]).resolve()
    if "${" in value or Path(value).is_absolute():
        raise OfflineMechanicsError(f"unsupported portable path: {value}")
    raise OfflineMechanicsError(f"unmarked path is forbidden in mechanics manifests: {value}")


def _canonical_day(value: Any) -> str:
    if isinstance(value, datetime):
        raw = value.date().isoformat()
    elif isinstance(value, date):
        raw = value.isoformat()
    else:
        raw = str(value)
    if _DAY_RE.fullmatch(raw) is None:
        raise OfflineMechanicsError(f"invalid UTC day: {raw!r}")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise OfflineMechanicsError(f"invalid UTC day: {raw!r}") from exc
    if parsed.isoformat() != raw:
        raise OfflineMechanicsError(f"noncanonical UTC day: {raw!r}")
    return raw


def _validate_source_manifest(
    source_manifest_path: Path,
    *,
    layout: offline.OfflineSourceLayout,
) -> dict[str, Any]:
    try:
        manifest = offline.validate_canonical_manifest(
            source_manifest_path.expanduser().resolve(),
            rehash_sources=True,
            layout=layout,
        )
    except (offline.OfflineSourceGateError, OSError, ValueError) as exc:
        raise OfflineMechanicsError("canonical source manifest failed full validation") from exc
    selected = tuple(_canonical_day(day) for day in manifest.get("selected_days", ()))
    if len(selected) != offline.REQUIRED_DAYS or len(set(selected)) != len(selected):
        raise OfflineMechanicsError("canonical source manifest lacks the frozen admitted day count")
    if manifest.get("exact_current_owner_baseline") != {
        "policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "private_live_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "buy_policy": "CONTROL_85N",
        "owner_policy_unchanged": True,
    }:
        raise OfflineMechanicsError("canonical source manifest owner identity drifted")
    return manifest


def _binding(path: Path, *, roots: PortableRoots) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise OfflineMechanicsError(f"bound file does not exist: {resolved}")
    return {
        "path": _portable_path(resolved, roots=roots),
        "sha256": file_sha256(resolved),
        "size_bytes": int(resolved.stat().st_size),
    }


def _resolve_binding(
    binding: Mapping[str, Any],
    *,
    label: str,
    roots: PortableRoots,
    expected_sha256: str | None = None,
) -> Path:
    if not isinstance(binding, Mapping):
        raise OfflineMechanicsError(f"{label} binding is malformed")
    digest = str(binding.get("sha256", ""))
    if _SHA_RE.fullmatch(digest) is None:
        raise OfflineMechanicsError(f"{label} SHA256 is malformed")
    if expected_sha256 is not None and digest != expected_sha256:
        raise OfflineMechanicsError(f"{label} SHA256 is not the frozen owner identity")
    path = _resolve_portable(binding.get("path"), roots=roots)
    if not path.is_file():
        raise OfflineMechanicsError(f"{label} file is missing")
    if "size_bytes" in binding and path.stat().st_size != int(binding["size_bytes"]):
        raise OfflineMechanicsError(f"{label} file was resized")
    if file_sha256(path) != digest:
        raise OfflineMechanicsError(f"{label} file hash drifted")
    return path


def _source_receipt(
    source: Mapping[str, Any],
    day: str,
    *,
    roots: PortableRoots,
) -> dict[str, Any]:
    files = source.get("source_day_receipt_files")
    if not isinstance(files, Mapping) or not isinstance(files.get(day), Mapping):
        raise OfflineMechanicsError(f"source-day receipt is not bound: {day}")
    binding = files[day]
    path = _resolve_binding(binding, label=f"source-day receipt {day}", roots=roots)
    receipt = _load_json(path, label=f"source-day receipt {day}")
    if receipt.get("source_day") != day:
        raise OfflineMechanicsError(f"source-day receipt day drifted: {day}")
    canonical = receipt.get("source_day_receipt_sha256")
    if canonical != binding.get("canonical_sha256") or canonical != offline.canonical_document_sha256(
        receipt, "source_day_receipt_sha256"
    ):
        raise OfflineMechanicsError(f"source-day receipt canonical identity drifted: {day}")
    return receipt


def _selected_context_days(source: Mapping[str, Any]) -> tuple[str, ...]:
    selected = tuple(_canonical_day(day) for day in source.get("selected_days", ()))
    by_day = {
        str(row.get("utc_day")): row
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping)
    }
    context: set[str] = set()
    for day in selected:
        receipt = by_day.get(day)
        if not isinstance(receipt, Mapping) or receipt.get("source_gate_eligible") is not True:
            raise OfflineMechanicsError(f"selected target lacks an eligible receipt: {day}")
        values = receipt.get("context_days")
        if not isinstance(values, Mapping) or set(values) != {"D_minus_1", "D", "D_plus_1"}:
            raise OfflineMechanicsError(f"selected target context drifted: {day}")
        context.update(_canonical_day(value) for value in values.values())
    return tuple(sorted(context))


def _parquet_identity(path: Path) -> dict[str, Any]:
    try:
        parquet = pq.ParquetFile(path)
    except (OSError, ValueError) as exc:
        raise OfflineMechanicsError(f"cannot inspect Parquet file: {path}") from exc
    schema = parquet.schema_arrow
    columns = [str(name) for name in schema.names]
    types = [str(schema.field(index).type) for index in range(len(schema))]
    schema_payload = {"columns": columns, "types": types}
    return {
        "sha256": file_sha256(path),
        "size_bytes": int(path.stat().st_size),
        "rows": int(parquet.metadata.num_rows),
        "schema": schema_payload,
        "schema_sha256": canonical_sha256(schema_payload),
    }


def _normalized_sources_for_context(
    source: Mapping[str, Any],
    *,
    roots: PortableRoots,
) -> tuple[tuple[str, ...], dict[tuple[str, str], dict[str, Any]]]:
    days = _selected_context_days(source)
    output: dict[tuple[str, str], dict[str, Any]] = {}
    for day in days:
        receipt = _source_receipt(source, day, roots=roots)
        normalized = receipt.get("normalized")
        if not isinstance(normalized, Mapping):
            raise OfflineMechanicsError(f"normalized source identity is absent: {day}")
        clock = normalized.get("clock_audit")
        if not isinstance(clock, Mapping) or clock.get("timestamp_source") != "transaction":
            raise OfflineMechanicsError(f"normalized exchange clock drifted: {day}")
        if clock.get("snapshot_grid_ms") != 100:
            raise OfflineMechanicsError(f"normalized grid drifted: {day}")
        if clock.get("bbo_l2_timestamps_equal") is not True:
            raise OfflineMechanicsError(f"normalized BBO/L2 clock differs: {day}")
        for kind in BOOK_KINDS:
            binding = normalized.get(kind)
            path = _resolve_binding(binding, label=f"{day} normalized {kind}", roots=roots)
            identity = _parquet_identity(path)
            if identity["sha256"] != binding.get("sha256"):
                raise OfflineMechanicsError(f"normalized {kind} hash drifted: {day}")
            if identity["rows"] != int(clock.get("rows", -1)):
                raise OfflineMechanicsError(f"normalized {kind} rows drifted: {day}")
            output[(day, kind)] = {
                "path": path,
                "source_binding": dict(binding),
                **identity,
            }
        if output[(day, "bbo")]["rows"] != output[(day, "l2")]["rows"]:
            raise OfflineMechanicsError(f"normalized row parity drifted: {day}")
    return days, output


def _device_id(path: Path) -> int:
    return int(path.stat().st_dev)


def _write_daily_quality(
    path: Path,
    *,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    with path.open("w", newline="", encoding="ascii") as handle:
        writer = csv.DictWriter(handle, fieldnames=DAILY_QUALITY_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())


def build_book_view(
    source_manifest_path: Path,
    output_root: Path,
    *,
    layout: offline.OfflineSourceLayout | None = None,
    repository_root: Path | None = None,
) -> dict[str, Any]:
    """Atomically publish a same-filesystem hardlink view over admitted books."""

    active_layout = layout or offline.default_layout()
    roots = PortableRoots.from_layout(active_layout, repository_root=repository_root)
    source_path = source_manifest_path.expanduser().resolve()
    source = _validate_source_manifest(source_path, layout=active_layout)
    destination = output_root.expanduser().resolve()
    if destination.exists():
        raise OfflineMechanicsError(f"immutable book view already exists: {destination}")
    _portable_path(destination, roots=roots)
    context_days, sources = _normalized_sources_for_context(source, roots=roots)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination_device = _device_id(destination.parent)
    for record in sources.values():
        if _device_id(record["path"]) != destination_device:
            raise OfflineMechanicsError("normalized book view requires same-filesystem hardlinks")

    stage = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}.staging-",
            dir=destination.parent,
        )
    )
    try:
        for kind in BOOK_KINDS:
            (stage / kind).mkdir()
        selected = set(source["selected_days"])
        file_records: list[dict[str, Any]] = []
        quality_rows: list[dict[str, Any]] = []
        for day in context_days:
            source_receipt = _source_receipt(source, day, roots=roots)
            normalized = source_receipt.get("normalized")
            clock = normalized.get("clock_audit") if isinstance(normalized, Mapping) else None
            if not isinstance(clock, Mapping):
                raise OfflineMechanicsError(
                    f"normalized clock audit is unavailable for quality projection: {day}"
                )
            cadence_schema_valid = bool(
                clock.get("timestamp_source") == "transaction"
                and clock.get("snapshot_grid_ms") == 100
                and clock.get("bbo_l2_timestamps_equal") is True
                and clock.get("strictly_increasing") is True
            )
            expected_grid_rows = 86_400 * 10
            coverage_99_valid = bool(
                cadence_schema_valid
                and int(clock.get("rows", -1)) >= math.ceil(0.99 * expected_grid_rows)
            )
            row: dict[str, Any] = {
                "day": day,
                "target_day": day in selected,
                "source_authority": SOURCE_AUTHORITY,
                "queue_identity": offline.QUEUE_IDENTITY,
                "exact_queue_policy_eligible": False,
                "cadence_schema_valid": cadence_schema_valid,
                "coverage_99_valid": coverage_99_valid,
                "formal_eligible": False,
                "provider_sensitivity_replay_eligible": False,
                "same_millisecond_ambiguity_policy": "censor",
            }
            for kind in BOOK_KINDS:
                source_record = sources[(day, kind)]
                target = stage / kind / f"{SYMBOL}-{kind}-{day}.parquet"
                os.link(source_record["path"], target)
                if not os.path.samefile(source_record["path"], target):
                    raise OfflineMechanicsError(f"hardlink identity mismatch: {day} {kind}")
                final_target = destination / kind / target.name
                file_records.append(
                    {
                        "day": day,
                        "kind": kind,
                        "path": _portable_path(final_target, roots=roots),
                        "source_path": _portable_path(source_record["path"], roots=roots),
                        "sha256": source_record["sha256"],
                        "size_bytes": source_record["size_bytes"],
                        "rows": source_record["rows"],
                        "schema": source_record["schema"],
                        "schema_sha256": source_record["schema_sha256"],
                    }
                )
                row[f"{kind}_rows"] = source_record["rows"]
                row[f"{kind}_schema_sha256"] = source_record["schema_sha256"]
                row[f"{kind}_sha256"] = source_record["sha256"]
                row[f"{kind}_size_bytes"] = source_record["size_bytes"]
                row[f"{kind}_source_path"] = _portable_path(
                    source_record["path"], roots=roots
                )
            quality_rows.append(row)
        quality_path = stage / "daily_quality.csv"
        _write_daily_quality(quality_path, rows=quality_rows)
        source_binding = _binding(source_path, roots=roots)
        source_binding["canonical_sha256"] = source["canonical_manifest_sha256"]
        manifest: dict[str, Any] = {
            "schema_version": BOOK_VIEW_SCHEMA_VERSION,
            "identity": MECHANICS_IDENTITY,
            "status": "immutable_outcome_blind_book_view_admitted",
            "symbol": SYMBOL,
            "output_root": _portable_path(destination, roots=roots),
            "source_manifest": source_binding,
            "source_authority": SOURCE_AUTHORITY,
            "queue_identity": offline.QUEUE_IDENTITY,
            "exact_queue_policy_eligible": False,
            "timestamp_source": "transaction",
            "snapshot_grid_ms": 100,
            "same_millisecond_ambiguity_policy": "censor",
            "selected_target_days": list(source["selected_days"]),
            "context_days": list(context_days),
            "day_count": len(context_days),
            "files": file_records,
            "daily_quality": {
                "path": _portable_path(
                    destination / "daily_quality.csv", roots=roots
                ),
                "sha256": file_sha256(quality_path),
                "size_bytes": quality_path.stat().st_size,
                "rows": len(quality_rows),
                "schema": list(DAILY_QUALITY_COLUMNS),
            },
            "permissions": {
                "economic_outcomes_read": False,
                "one_shot_training_labels_generated": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }
        manifest["canonical_manifest_sha256"] = canonical_document_sha256(
            manifest, "canonical_manifest_sha256"
        )
        _atomic_json(stage / "manifest.json", manifest)
        os.replace(stage, destination)
        _fsync_directory(destination.parent)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    try:
        return validate_book_view(
            destination,
            layout=active_layout,
            repository_root=roots.repository_root,
        )
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        _fsync_directory(destination.parent)
        raise


def _parse_bool(value: Any) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise OfflineMechanicsError(f"invalid Boolean in daily quality: {value!r}")


def validate_book_view(
    book_view_root: Path,
    *,
    layout: offline.OfflineSourceLayout | None = None,
    repository_root: Path | None = None,
) -> dict[str, Any]:
    """Fully revalidate a published book view and every underlying source byte."""

    active_layout = layout or offline.default_layout()
    roots = PortableRoots.from_layout(active_layout, repository_root=repository_root)
    root = book_view_root.expanduser().resolve()
    manifest_path = root / "manifest.json"
    quality_path = root / "daily_quality.csv"
    manifest = _load_json(manifest_path, label="normalized book-view manifest")
    if manifest.get("schema_version") != BOOK_VIEW_SCHEMA_VERSION:
        raise OfflineMechanicsError("normalized book-view schema drifted")
    if manifest.get("identity") != MECHANICS_IDENTITY:
        raise OfflineMechanicsError("normalized book-view identity drifted")
    if manifest.get("canonical_manifest_sha256") != canonical_document_sha256(
        manifest, "canonical_manifest_sha256"
    ):
        raise OfflineMechanicsError("normalized book-view manifest hash drifted")
    if _resolve_portable(manifest.get("output_root"), roots=roots) != root:
        raise OfflineMechanicsError("normalized book-view root binding drifted")
    if (
        manifest.get("source_authority") != SOURCE_AUTHORITY
        or manifest.get("queue_identity") != offline.QUEUE_IDENTITY
        or manifest.get("exact_queue_policy_eligible") is not False
        or manifest.get("same_millisecond_ambiguity_policy") != "censor"
    ):
        raise OfflineMechanicsError("normalized book-view authority drifted")
    permissions = manifest.get("permissions")
    if permissions != {
        "economic_outcomes_read": False,
        "one_shot_training_labels_generated": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflineMechanicsError("normalized book-view permissions drifted")
    source_binding = manifest.get("source_manifest")
    source_path = _resolve_binding(
        source_binding,
        label="canonical source manifest",
        roots=roots,
    )
    source = _validate_source_manifest(source_path, layout=active_layout)
    if source_binding.get("canonical_sha256") != source.get("canonical_manifest_sha256"):
        raise OfflineMechanicsError("book view source canonical identity drifted")
    context_days, sources = _normalized_sources_for_context(source, roots=roots)
    if tuple(manifest.get("selected_target_days", ())) != tuple(source["selected_days"]):
        raise OfflineMechanicsError("book-view target-day order drifted")
    if tuple(manifest.get("context_days", ())) != context_days:
        raise OfflineMechanicsError("book-view context-day order drifted")
    records = manifest.get("files")
    if not isinstance(records, list) or len(records) != len(context_days) * 2:
        raise OfflineMechanicsError("book-view file census drifted")
    by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise OfflineMechanicsError("book-view file record is malformed")
        key = (_canonical_day(record.get("day")), str(record.get("kind")))
        if key in by_key or key[1] not in BOOK_KINDS:
            raise OfflineMechanicsError("book-view file key is duplicated or invalid")
        by_key[key] = record
    if tuple(by_key) != tuple((day, kind) for day in context_days for kind in BOOK_KINDS):
        raise OfflineMechanicsError("book-view file order drifted")
    expected_paths: set[Path] = set()
    for key, record in by_key.items():
        day, kind = key
        target = _resolve_portable(record.get("path"), roots=roots)
        expected = root / kind / f"{SYMBOL}-{kind}-{day}.parquet"
        if target != expected or not target.is_file():
            raise OfflineMechanicsError(f"book-view target drifted: {day} {kind}")
        expected_paths.add(target)
        source_record = sources[key]
        if _resolve_portable(record.get("source_path"), roots=roots) != source_record["path"]:
            raise OfflineMechanicsError(f"book-view source path drifted: {day} {kind}")
        if not os.path.samefile(target, source_record["path"]):
            raise OfflineMechanicsError(f"book-view file is not a hardlink: {day} {kind}")
        actual = _parquet_identity(target)
        for field in ("sha256", "size_bytes", "rows", "schema", "schema_sha256"):
            if record.get(field) != actual[field] or record.get(field) != source_record[field]:
                raise OfflineMechanicsError(f"book-view {field} drifted: {day} {kind}")
    observed_paths = {
        path.resolve()
        for kind in BOOK_KINDS
        for path in (root / kind).glob("*.parquet")
    }
    if observed_paths != expected_paths:
        raise OfflineMechanicsError("book-view contains unbound or missing Parquet files")
    quality = manifest.get("daily_quality")
    if not isinstance(quality, Mapping) or _resolve_portable(quality.get("path"), roots=roots) != quality_path:
        raise OfflineMechanicsError("daily quality path binding drifted")
    if not quality_path.is_file() or file_sha256(quality_path) != quality.get("sha256"):
        raise OfflineMechanicsError("daily quality hash drifted")
    if quality_path.stat().st_size != int(quality.get("size_bytes", -1)):
        raise OfflineMechanicsError("daily quality size drifted")
    with quality_path.open(newline="", encoding="ascii") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != DAILY_QUALITY_COLUMNS:
            raise OfflineMechanicsError("daily quality schema drifted")
        quality_rows = list(reader)
    if len(quality_rows) != len(context_days) or int(quality.get("rows", -1)) != len(
        quality_rows
    ):
        raise OfflineMechanicsError("daily quality row census drifted")
    if tuple(row["day"] for row in quality_rows) != context_days:
        raise OfflineMechanicsError("daily quality day order drifted")
    selected = set(source["selected_days"])
    for row in quality_rows:
        day = row["day"]
        if _parse_bool(row["target_day"]) != (day in selected):
            raise OfflineMechanicsError(f"daily quality target flag drifted: {day}")
        if row["source_authority"] != SOURCE_AUTHORITY:
            raise OfflineMechanicsError(f"daily quality source authority drifted: {day}")
        if row["queue_identity"] != offline.QUEUE_IDENTITY:
            raise OfflineMechanicsError(f"daily quality queue identity drifted: {day}")
        if _parse_bool(row["exact_queue_policy_eligible"]):
            raise OfflineMechanicsError(f"daily quality overclaims exact queue: {day}")
        if not _parse_bool(row["cadence_schema_valid"]):
            raise OfflineMechanicsError(f"daily quality cadence/schema drifted: {day}")
        expected_coverage = int(by_key[(day, "bbo")]["rows"]) >= math.ceil(
            0.99 * 86_400 * 10
        )
        if _parse_bool(row["coverage_99_valid"]) != expected_coverage:
            raise OfflineMechanicsError(f"daily quality coverage flag drifted: {day}")
        if _parse_bool(row["formal_eligible"]):
            raise OfflineMechanicsError(f"daily quality overclaims formal authority: {day}")
        if _parse_bool(row["provider_sensitivity_replay_eligible"]):
            raise OfflineMechanicsError(
                f"daily quality overclaims provider sensitivity authority: {day}"
            )
        if row["same_millisecond_ambiguity_policy"] != "censor":
            raise OfflineMechanicsError(f"daily quality ambiguity policy drifted: {day}")
        for kind in BOOK_KINDS:
            record = by_key[(day, kind)]
            if (
                int(row[f"{kind}_rows"]) != record["rows"]
                or row[f"{kind}_sha256"] != record["sha256"]
                or row[f"{kind}_schema_sha256"] != record["schema_sha256"]
                or int(row[f"{kind}_size_bytes"]) != record["size_bytes"]
                or _resolve_portable(row[f"{kind}_source_path"], roots=roots)
                != _resolve_portable(record["source_path"], roots=roots)
            ):
                raise OfflineMechanicsError(f"daily quality {kind} identity drifted: {day}")
    return manifest


def _economic_columns(columns: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        column
        for column in columns
        if column not in _FALSE_ONLY_GOVERNANCE_COLUMNS
        if any(part in column.lower() for part in _FORBIDDEN_ECONOMIC_COLUMN_PARTS)
    )


def _ordered_day_census(
    values: Sequence[Any],
    *,
    selected_days: Sequence[str],
    label: str,
) -> dict[str, Any]:
    days = tuple(_canonical_day(value) for value in values)
    if not days:
        raise OfflineMechanicsError(f"{label} has no rows")
    selected = tuple(selected_days)
    allowed = set(selected)
    if set(days) - allowed:
        raise OfflineMechanicsError(f"{label} contains days outside the admitted panel")
    transitions: list[str] = []
    for day in days:
        if not transitions or transitions[-1] != day:
            transitions.append(day)
    if tuple(transitions) != selected:
        raise OfflineMechanicsError(f"{label} day order drifted from source admission")
    counts = Counter(days)
    return {
        "ordered_days": list(selected),
        "rows_by_day": {day: int(counts[day]) for day in selected},
        "day_census_sha256": canonical_sha256(
            {"ordered_days": list(selected), "rows_by_day": dict(counts)}
        ),
    }


def _row_key_sha256(days: Sequence[str], opportunity_ids: Sequence[Any]) -> str:
    digest = hashlib.sha256()
    for day, opportunity_id in zip(days, opportunity_ids, strict=True):
        digest.update(day.encode("ascii"))
        digest.update(b"\0")
        digest.update(
            json.dumps(
                opportunity_id,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                default=str,
            ).encode("ascii")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _inspect_panel_file(
    role: str,
    path: Path,
    *,
    selected_days: Sequence[str],
    roots: PortableRoots,
) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if resolved.suffix != ".parquet" or not resolved.is_file():
        raise OfflineMechanicsError(f"panel {role} must be an existing Parquet file")
    identity = _parquet_identity(resolved)
    columns = tuple(identity["schema"]["columns"])
    forbidden = _economic_columns(columns)
    if forbidden:
        raise OfflineMechanicsError(
            f"panel {role} contains forbidden economic columns: {', '.join(forbidden)}"
        )
    if "utc_day" not in columns:
        raise OfflineMechanicsError(f"panel {role} lacks utc_day")
    required = {"utc_day", "opportunity_id"}
    if role == "exact_owner_actions":
        required.add("exact_owner_action")
    if not required.issubset(columns):
        raise OfflineMechanicsError(f"panel {role} lacks required mechanics columns")
    if role in {"boolean_features", "continuous_features"} and len(set(columns) - required) == 0:
        raise OfflineMechanicsError(f"panel {role} has no feature columns")
    table_columns = ["utc_day"]
    if "opportunity_id" in columns:
        table_columns.append("opportunity_id")
    if role == "exact_owner_actions":
        table_columns.append("exact_owner_action")
    table_columns.extend(
        sorted(_FALSE_ONLY_GOVERNANCE_COLUMNS.intersection(columns))
    )
    table = pq.read_table(resolved, columns=table_columns)
    for column in _FALSE_ONLY_GOVERNANCE_COLUMNS.intersection(table_columns):
        values = table[column].to_pylist()
        if any(value is not False for value in values):
            raise OfflineMechanicsError(
                f"panel {role} governance column {column} must be false"
            )
    day_values = table["utc_day"].to_pylist()
    canonical_days = tuple(_canonical_day(value) for value in day_values)
    census = _ordered_day_census(
        canonical_days,
        selected_days=selected_days,
        label=f"panel {role}",
    )
    result: dict[str, Any] = {
        "path": _portable_path(resolved, roots=roots),
        **identity,
        "day_census": census,
    }
    if "opportunity_id" in table_columns:
        identifiers = table["opportunity_id"].to_pylist()
        if any(value is None or str(value).strip() == "" for value in identifiers):
            raise OfflineMechanicsError(f"panel {role} has missing opportunity_id")
        if len({str(value) for value in identifiers}) != len(identifiers):
            raise OfflineMechanicsError(f"panel {role} has duplicate opportunity_id")
        result["row_key_sha256"] = _row_key_sha256(canonical_days, identifiers)
    if role == "exact_owner_actions":
        actions = table["exact_owner_action"].to_pylist()
        if any(
            value is None
            or (isinstance(value, float) and math.isnan(value))
            or str(value).strip() == ""
            for value in actions
        ):
            raise OfflineMechanicsError("exact owner action contains NaN or missing values")
        normalized_actions = tuple(str(value) for value in actions)
        unknown = sorted(set(normalized_actions) - OWNER_ACTION_VOCABULARY)
        if unknown:
            raise OfflineMechanicsError(f"exact owner action vocabulary drifted: {unknown}")
        result["action_counts"] = {
            action: int(count)
            for action, count in sorted(Counter(normalized_actions).items())
        }
    return result


def _owner_bindings(
    owner_artifacts: Mapping[str, Path],
    *,
    roots: PortableRoots,
) -> dict[str, dict[str, Any]]:
    if set(owner_artifacts) != set(OWNER_ARTIFACT_ROLES):
        raise OfflineMechanicsError("owner artifact census is incomplete")
    output: dict[str, dict[str, Any]] = {}
    for role, expected in _expected_owner_hashes().items():
        binding = _binding(Path(owner_artifacts[role]), roots=roots)
        if binding["sha256"] != expected:
            raise OfflineMechanicsError(f"owner {role} hash drifted")
        output[role] = binding
    return output


def _validate_sequential_panel_builder(
    builder_manifest_path: Path,
    *,
    source_path: Path,
    view_root: Path,
    panel_files: Mapping[str, Path],
    owner_artifacts: Mapping[str, Path],
    layout: offline.OfflineSourceLayout,
    roots: PortableRoots,
) -> dict[str, Any]:
    manifest_path = builder_manifest_path.expanduser().resolve()
    if manifest_path.name != "manifest.json" or not manifest_path.is_file():
        raise OfflineMechanicsError("sequential panel builder manifest is missing")
    builder_root = manifest_path.parent
    binding_path = builder_root / "_bindings" / "portable_replay_binding_v2.json"
    binding = _load_json(binding_path, label="sequential replay portable binding")
    projections = binding.get("day_projections")
    selected_days = tuple(binding.get("selected_days") or ())
    if not isinstance(projections, Mapping) or tuple(projections) != selected_days:
        raise OfflineMechanicsError("sequential replay day projections drifted")
    if not selected_days:
        raise OfflineMechanicsError("sequential replay binding has no selected days")
    first = projections.get(selected_days[0])
    if not isinstance(first, Mapping):
        raise OfflineMechanicsError("sequential replay projection is malformed")
    bound_source = _resolve_portable(first.get("source_manifest_path"), roots=roots)
    bound_view_manifest = _resolve_portable(
        first.get("book_view_manifest_path"), roots=roots
    )
    features_manifest = _resolve_portable(
        first.get("features_manifest_path"), roots=roots
    )
    native_observation_root = _resolve_portable(
        first.get("native_observation_root"), roots=roots
    )
    native_binding = binding.get("native_observation_batch_manifest")
    if not isinstance(native_binding, Mapping):
        raise OfflineMechanicsError("native observation batch binding is malformed")
    native_observation_manifest = _resolve_portable(
        native_binding.get("path"), roots=roots
    )
    if (
        bound_source != source_path
        or bound_view_manifest != view_root / "manifest.json"
        or str(first.get("private_config_sha256", ""))
        != _expected_owner_hashes()["private_config"]
    ):
        raise OfflineMechanicsError("sequential panel source or owner binding drifted")
    try:
        builder = importlib.import_module(SEQUENTIAL_PANEL_BUILDER_MODULE)
        inputs = builder.validate_inputs(
            source_manifest_path=bound_source,
            book_view_root=bound_view_manifest.parent,
            native_observation_manifest_path=native_observation_manifest,
            native_observation_root=native_observation_root,
            features_manifest_path=features_manifest,
            owner_artifacts=builder.OwnerArtifactPaths(
                policy=Path(owner_artifacts["policy"]),
                predicate_bundle=Path(owner_artifacts["predicate_bundle"]),
                private_config=Path(owner_artifacts["private_config"]),
            ),
        )
        replay_adapter = importlib.import_module(
            "research.families.f05_fill_quality_quote_ev.audit."
            "causal_multichannel_window_boolean_cooldown_full_multiscale_"
            "successor_offline_replay_adapter_v1"
        )
        replay_adapter._rebind_historical_fixed_bridge(
            binding.get("fixed_bridge"),
            context="sequential mechanics historical execution bridge",
        )
        validated = builder.validate_panel(builder_root, inputs=inputs)
    except Exception as exc:
        raise OfflineMechanicsError(
            "sequential panel builder failed full source/day/byte validation"
        ) from exc
    if validated.get("selected_days") != list(selected_days):
        raise OfflineMechanicsError("sequential panel selected-day order drifted")
    for role in PANEL_FILE_ROLES:
        expected = (builder_root / "panel" / f"{role}.parquet").resolve()
        if Path(panel_files[role]).expanduser().resolve() != expected:
            raise OfflineMechanicsError(
                f"panel {role} is not the admitted sequential-builder output"
            )
    merged_manifest_path = builder_root / "panel" / "manifest.json"
    merged = _load_json(merged_manifest_path, label="sequential merged panel manifest")
    if validated.get("merged_panel_manifest_sha256") != merged.get(
        "canonical_manifest_sha256"
    ):
        raise OfflineMechanicsError("sequential root/merged manifest binding drifted")
    manifest_binding = _binding(manifest_path, roots=roots)
    manifest_binding["canonical_sha256"] = validated["canonical_manifest_sha256"]
    merged_binding = _binding(merged_manifest_path, roots=roots)
    merged_binding["canonical_sha256"] = merged["canonical_manifest_sha256"]
    portable_binding = _binding(binding_path, roots=roots)
    return {
        "identity": validated["identity"],
        "status": validated["status"],
        "selected_days": list(selected_days),
        "selected_day_count": len(selected_days),
        "input_binding_sha256": validated["input_binding_sha256"],
        "sequential_replay_input_identity": validated[
            "sequential_replay_input_identity"
        ],
        "manifest": manifest_binding,
        "merged_panel_manifest": merged_binding,
        "portable_replay_binding": portable_binding,
        "day_manifest_sha256": dict(validated["day_manifest_sha256"]),
        "permissions": dict(validated["permissions"]),
    }


def admit_panel(
    source_manifest_path: Path,
    book_view_root: Path,
    panel_manifest_path: Path,
    *,
    panel_files: Mapping[str, Path],
    owner_artifacts: Mapping[str, Path],
    sequential_panel_builder_manifest_path: Path | None = None,
    layout: offline.OfflineSourceLayout | None = None,
    repository_root: Path | None = None,
) -> dict[str, Any]:
    """Atomically bind existing outcome-blind mechanics files; generate no rows."""

    active_layout = layout or offline.default_layout()
    roots = PortableRoots.from_layout(active_layout, repository_root=repository_root)
    source_path = source_manifest_path.expanduser().resolve()
    source = _validate_source_manifest(source_path, layout=active_layout)
    view_root = book_view_root.expanduser().resolve()
    view = validate_book_view(
        view_root,
        layout=active_layout,
        repository_root=roots.repository_root,
    )
    if view.get("source_manifest", {}).get("canonical_sha256") != source.get(
        "canonical_manifest_sha256"
    ):
        raise OfflineMechanicsError("book view and panel source manifests differ")
    if set(panel_files) != set(PANEL_FILE_ROLES):
        raise OfflineMechanicsError("canonical mechanics panel file roles drifted")
    selected_days = tuple(source["selected_days"])
    inspected = {
        role: _inspect_panel_file(
            role,
            Path(panel_files[role]),
            selected_days=selected_days,
            roots=roots,
        )
        for role in PANEL_FILE_ROLES
    }
    metadata_key = inspected["metadata"].get("row_key_sha256")
    for role in (
        "boolean_features",
        "continuous_features",
        "exact_owner_actions",
        "replay_inputs",
    ):
        if inspected[role].get("row_key_sha256") != metadata_key:
            raise OfflineMechanicsError(f"panel {role} row identity drifted from metadata")
    owners = _owner_bindings(owner_artifacts, roots=roots)
    sequential_builder = (
        None
        if sequential_panel_builder_manifest_path is None
        else _validate_sequential_panel_builder(
            sequential_panel_builder_manifest_path,
            source_path=source_path,
            view_root=view_root,
            panel_files=panel_files,
            owner_artifacts=owner_artifacts,
            layout=active_layout,
            roots=roots,
        )
    )
    source_binding = _binding(source_path, roots=roots)
    source_binding["canonical_sha256"] = source["canonical_manifest_sha256"]
    view_manifest_path = view_root / "manifest.json"
    view_binding = _binding(view_manifest_path, roots=roots)
    view_binding["canonical_sha256"] = view["canonical_manifest_sha256"]
    selected_set = set(selected_days)
    day_receipts = {
        row["utc_day"]: row["day_receipt_sha256"]
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping) and row.get("utc_day") in selected_set
    }
    if tuple(day_receipts) != selected_days:
        raise OfflineMechanicsError("selected day-receipt order drifted")
    manifest: dict[str, Any] = {
        "schema_version": (
            PANEL_SCHEMA_VERSION
            if sequential_builder is not None
            else LEGACY_PANEL_SCHEMA_VERSION
        ),
        "identity": offline.IDENTITY,
        "mechanics_identity": MECHANICS_IDENTITY,
        "status": (
            "offline_outcome_blind_sequential_mechanics_panel_admitted"
            if sequential_builder is not None
            else "offline_outcome_blind_mechanics_panel_admitted"
        ),
        "source_manifest": source_binding,
        "source_manifest_sha256": source["canonical_manifest_sha256"],
        "book_view_manifest": view_binding,
        "selected_days": list(selected_days),
        "panel_role": offline.PANEL_ROLE,
        "source_authority": SOURCE_AUTHORITY,
        "queue_identity": offline.QUEUE_IDENTITY,
        "exact_queue_policy_eligible": False,
        "same_millisecond_ambiguity_policy": "censor",
        "economic_outcomes_present": False,
        "one_shot_training_labels_precomputed": False,
        "outer_train_label_generation_required": True,
        "one_shot_effect_aggregation_used": False,
        "repeated_sequential_policy_required": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "exact_current_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_current_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "exact_current_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "owner_artifacts": owners,
        "day_receipt_sha256": day_receipts,
        "files": inspected,
        "permissions": {
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    if sequential_builder is not None:
        manifest["formal_execution_eligible"] = True
        manifest["sequential_panel_builder"] = sequential_builder
    manifest["canonical_panel_manifest_sha256"] = canonical_document_sha256(
        manifest, "canonical_panel_manifest_sha256"
    )
    destination = panel_manifest_path.expanduser().resolve()
    _portable_path(destination, roots=roots)
    if destination.exists():
        raise OfflineMechanicsError(f"immutable panel manifest already exists: {destination}")
    _atomic_json(destination, manifest)
    try:
        return validate_panel(
            destination,
            layout=active_layout,
            repository_root=roots.repository_root,
        )
    except Exception:
        destination.unlink(missing_ok=True)
        raise


def validate_panel(
    panel_manifest_path: Path,
    *,
    layout: offline.OfflineSourceLayout | None = None,
    repository_root: Path | None = None,
) -> dict[str, Any]:
    """Revalidate a canonical mechanics panel and all byte/schema/day bindings."""

    active_layout = layout or offline.default_layout()
    roots = PortableRoots.from_layout(active_layout, repository_root=repository_root)
    path = panel_manifest_path.expanduser().resolve()
    manifest = _load_json(path, label="canonical mechanics panel manifest")
    schema_version = manifest.get("schema_version")
    if schema_version not in {LEGACY_PANEL_SCHEMA_VERSION, PANEL_SCHEMA_VERSION}:
        raise OfflineMechanicsError("canonical mechanics panel schema drifted")
    if manifest.get("identity") != offline.IDENTITY or manifest.get(
        "mechanics_identity"
    ) != MECHANICS_IDENTITY:
        raise OfflineMechanicsError("canonical mechanics panel identity drifted")
    if manifest.get("canonical_panel_manifest_sha256") != canonical_document_sha256(
        manifest, "canonical_panel_manifest_sha256"
    ):
        raise OfflineMechanicsError("canonical mechanics panel hash drifted")
    required_contract = {
        "source_authority": SOURCE_AUTHORITY,
        "queue_identity": offline.QUEUE_IDENTITY,
        "exact_queue_policy_eligible": False,
        "same_millisecond_ambiguity_policy": "censor",
        "economic_outcomes_present": False,
        "one_shot_training_labels_precomputed": False,
        "outer_train_label_generation_required": True,
        "one_shot_effect_aggregation_used": False,
        "repeated_sequential_policy_required": True,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    if any(manifest.get(key) != value for key, value in required_contract.items()):
        raise OfflineMechanicsError("canonical mechanics panel contract drifted")
    if manifest.get("permissions") != {
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflineMechanicsError("canonical mechanics panel permissions drifted")
    source_path = _resolve_binding(
        manifest.get("source_manifest"),
        label="canonical source manifest",
        roots=roots,
    )
    source = _validate_source_manifest(source_path, layout=active_layout)
    if (
        manifest.get("source_manifest_sha256") != source.get("canonical_manifest_sha256")
        or manifest.get("source_manifest", {}).get("canonical_sha256")
        != source.get("canonical_manifest_sha256")
    ):
        raise OfflineMechanicsError("canonical mechanics panel source binding drifted")
    selected_days = tuple(source["selected_days"])
    if tuple(manifest.get("selected_days", ())) != selected_days:
        raise OfflineMechanicsError("canonical mechanics panel day order drifted")
    view_manifest_path = _resolve_binding(
        manifest.get("book_view_manifest"),
        label="normalized book-view manifest",
        roots=roots,
    )
    view = validate_book_view(
        view_manifest_path.parent,
        layout=active_layout,
        repository_root=roots.repository_root,
    )
    if manifest.get("book_view_manifest", {}).get("canonical_sha256") != view.get(
        "canonical_manifest_sha256"
    ):
        raise OfflineMechanicsError("canonical mechanics panel book-view binding drifted")
    owners = manifest.get("owner_artifacts")
    if not isinstance(owners, Mapping) or set(owners) != set(OWNER_ARTIFACT_ROLES):
        raise OfflineMechanicsError("canonical mechanics panel owner bindings drifted")
    expected_owner = _expected_owner_hashes()
    for role in OWNER_ARTIFACT_ROLES:
        _resolve_binding(
            owners[role],
            label=f"owner {role}",
            roots=roots,
            expected_sha256=expected_owner[role],
        )
    if (
        manifest.get("exact_current_owner_policy_sha256") != expected_owner["policy"]
        or manifest.get("exact_current_predicate_bundle_sha256")
        != expected_owner["predicate_bundle"]
        or manifest.get("exact_current_private_config_sha256")
        != expected_owner["private_config"]
    ):
        raise OfflineMechanicsError("canonical mechanics panel owner identity drifted")
    files = manifest.get("files")
    if not isinstance(files, Mapping) or set(files) != set(PANEL_FILE_ROLES):
        raise OfflineMechanicsError("canonical mechanics panel file census drifted")
    inspected: dict[str, dict[str, Any]] = {}
    for role in PANEL_FILE_ROLES:
        binding = files[role]
        if not isinstance(binding, Mapping):
            raise OfflineMechanicsError(f"panel {role} binding is malformed")
        file_path = _resolve_portable(binding.get("path"), roots=roots)
        inspected[role] = _inspect_panel_file(
            role,
            file_path,
            selected_days=selected_days,
            roots=roots,
        )
        if inspected[role] != binding:
            raise OfflineMechanicsError(f"panel {role} byte/schema/day identity drifted")
    metadata_key = inspected["metadata"].get("row_key_sha256")
    for role in (
        "boolean_features",
        "continuous_features",
        "exact_owner_actions",
        "replay_inputs",
    ):
        if inspected[role].get("row_key_sha256") != metadata_key:
            raise OfflineMechanicsError(f"panel {role} row identity drifted from metadata")
    if schema_version == PANEL_SCHEMA_VERSION:
        if (
            manifest.get("status")
            != "offline_outcome_blind_sequential_mechanics_panel_admitted"
            or manifest.get("formal_execution_eligible") is not True
        ):
            raise OfflineMechanicsError("formal sequential mechanics status drifted")
        expected_builder = _validate_sequential_panel_builder(
            _resolve_binding(
                manifest.get("sequential_panel_builder", {}).get("manifest", {}),
                label="sequential panel builder manifest",
                roots=roots,
            ),
            source_path=source_path,
            view_root=view_manifest_path.parent,
            panel_files={
                role: _resolve_portable(files[role]["path"], roots=roots)
                for role in PANEL_FILE_ROLES
            },
            owner_artifacts={
                role: _resolve_portable(owners[role]["path"], roots=roots)
                for role in OWNER_ARTIFACT_ROLES
            },
            layout=active_layout,
            roots=roots,
        )
        if manifest.get("sequential_panel_builder") != expected_builder:
            raise OfflineMechanicsError("sequential panel builder admission drifted")
    elif "sequential_panel_builder" in manifest or manifest.get(
        "formal_execution_eligible"
    ) is not None:
        raise OfflineMechanicsError("legacy mechanics panel overclaimed formal eligibility")
    selected_set = set(selected_days)
    expected_receipts = {
        row["utc_day"]: row["day_receipt_sha256"]
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping) and row.get("utc_day") in selected_set
    }
    if manifest.get("day_receipt_sha256") != expected_receipts:
        raise OfflineMechanicsError("canonical mechanics panel day receipts drifted")
    return manifest


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build-book-view")
    build.add_argument("source_manifest", type=Path)
    build.add_argument("output_root", type=Path)

    validate_view = subparsers.add_parser("validate-book-view")
    validate_view.add_argument("book_view_root", type=Path)

    admit = subparsers.add_parser("admit-panel")
    admit.add_argument("source_manifest", type=Path)
    admit.add_argument("book_view_root", type=Path)
    admit.add_argument("panel_manifest", type=Path)
    for role in PANEL_FILE_ROLES:
        admit.add_argument(f"--{role.replace('_', '-')}", type=Path, required=True)
    admit.add_argument("--owner-policy", type=Path, required=True)
    admit.add_argument("--predicate-bundle", type=Path, required=True)
    admit.add_argument("--private-config", type=Path, required=True)
    admit.add_argument(
        "--sequential-panel-builder-manifest",
        type=Path,
        required=True,
    )

    validate = subparsers.add_parser("validate-panel")
    validate.add_argument("panel_manifest", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "build-book-view":
        result = build_book_view(args.source_manifest, args.output_root)
    elif args.command == "validate-book-view":
        result = validate_book_view(args.book_view_root)
    elif args.command == "admit-panel":
        result = admit_panel(
            args.source_manifest,
            args.book_view_root,
            args.panel_manifest,
            panel_files={role: getattr(args, role) for role in PANEL_FILE_ROLES},
            owner_artifacts={
                "policy": args.owner_policy,
                "predicate_bundle": args.predicate_bundle,
                "private_config": args.private_config,
            },
            sequential_panel_builder_manifest_path=(
                args.sequential_panel_builder_manifest
            ),
        )
    else:
        result = validate_panel(args.panel_manifest)
    print(
        json.dumps(
            {
                "identity": result["identity"],
                "status": result["status"],
                "economic_outcomes_present": False,
                "canonical_sha256": result.get("canonical_manifest_sha256")
                or result.get("canonical_panel_manifest_sha256"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
