#!/usr/bin/env python3
"""Bind a trained F03 1s bundle and materialize the native 40-day overlays.

This is a prediction-only post-training preparation layer. It never reads
labels, markouts, campaign outcomes, or PnL, and it does not copy market
features into the model-overlay cache.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import struct
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from data_paths import data_root, external_cache_root
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_2026_native_execution_prep as native_prep,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_full_schema as full_schema,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as feature_panels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_prediction_overlay as prediction_overlays,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import causal_v12_1s_training as training

SCHEMA_VERSION = "causal_v12_1s_2026_native_overlay_binding.v2"
PLAN_SCHEMA_VERSION = "causal_v12_1s_2026_native_overlay_execution_plan.v2"
PANEL_SCHEMA_VERSION = "causal_v12_1s_2026_native_overlay_panel.v2"
IDENTITY = "causal_v12_1s_2026_native_40day_model_overlay_binding_v2"
EXPECTED_DAY_COUNT = 40
ROWS_PER_DAY = 86_400
EXPECTED_HEADS = tuple(training.HEAD_SPECS)
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MARKET_DATA_ROOT = data_root(ROOT)
DEFAULT_PREP_ROOT = (
    external_cache_root(ROOT)
    / "replay_dag/f03_causal_v12_1s_2026_native_40d_execution_prep_v1"
)
DEFAULT_OUTPUT_ROOT = (
    external_cache_root(ROOT)
    / "replay_dag/f03_causal_v12_1s_2026_native_40d_model_overlay_v2"
)
PLAN_FILENAME = "execution-plan.json"
PLAN_SUCCESS_FILENAME = "_PLAN_SUCCESS"
PROGRESS_FILENAME = "materialization-progress.json"
PANEL_MANIFEST_FILENAME = "overlay-panel-manifest.json"
PANEL_SUCCESS_FILENAME = "_OVERLAY_PANEL_SUCCESS"
GIB = 1 << 30
ESTIMATED_FINAL_BYTES = 2 * GIB
SAFETY_RESERVE_BYTES = 60 * GIB
LEGACY_FEATURE_ARTIFACT_SCHEMA_VERSION = (
    prediction_overlays.LEGACY_FEATURE_PANEL_ARTIFACT_SCHEMA_VERSION
)


class NativeOverlayBindingError(ValueError):
    """Raised when post-training execution inputs drift from their contract."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json_object(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise NativeOverlayBindingError(f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NativeOverlayBindingError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise NativeOverlayBindingError(f"{role} must be a JSON object")
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="ascii") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _require_cache_location(*, market_data_root: Path, prep_root: Path, output_root: Path) -> None:
    market_data_root = market_data_root.expanduser().resolve()
    cache_root = market_data_root / "cache"
    if not market_data_root.is_dir():
        raise NativeOverlayBindingError(f"market-data root is unavailable: {market_data_root}")
    for role, path in (("feature prep", prep_root), ("overlay output", output_root)):
        try:
            path.expanduser().resolve().relative_to(cache_root)
        except ValueError as exc:
            raise NativeOverlayBindingError(
                f"{role} must stay below the market-data cache root: {cache_root}"
            ) from exc
    free = shutil.disk_usage(cache_root).free
    required = SAFETY_RESERVE_BYTES + int(2.5 * ESTIMATED_FINAL_BYTES)
    if free < required:
        raise NativeOverlayBindingError(
            f"ORICO storage gate failed: free={free}, required={required}"
        )


def _source_clock_contract_payload() -> dict[str, Any]:
    contract = full_schema.full_feature_contract_payload()
    return {
        "cadence_ms": schema.CADENCE_MS,
        "feature_dag_id": schema.FEATURE_DAG_ID,
        "feature_semantics_identity": schema.FEATURE_SEMANTICS_IDENTITY,
        "feature_order_sha256": schema.feature_order_sha256(),
        "nodes": [
            {
                "name": node["name"],
                "source_clock": node["source_clock"],
                "cadence_ms": node["cadence_ms"],
                "unit": node["unit"],
            }
            for node in contract["nodes"]
        ],
        "source_manifest_sha256": contract["source_manifest_sha256"],
    }


def _validate_feature_manifest(
    manifest_path: Path,
    panel_path: Path,
    *,
    expected_day: str,
    expected_manifest_sha256: str,
    expected_panel_sha256: str,
    expected_rows: int,
) -> dict[str, Any]:
    if _sha256_file(manifest_path) != expected_manifest_sha256:
        raise NativeOverlayBindingError(f"feature manifest SHA256 drift for {expected_day}")
    if _sha256_file(panel_path) != expected_panel_sha256:
        raise NativeOverlayBindingError(f"feature panel SHA256 drift for {expected_day}")
    manifest = _load_json_object(manifest_path, role=f"{expected_day} feature manifest")
    success_path = manifest_path.parent / feature_panels.SUCCESS_FILENAME
    if not success_path.is_file() or success_path.read_text(encoding="ascii").strip() != (
        expected_manifest_sha256
    ):
        raise NativeOverlayBindingError(f"feature panel admission marker drift for {expected_day}")
    artifact_schema_version = manifest.get("schema_version")
    supported_feature_artifact_schemas = {
        feature_panels.ARTIFACT_SCHEMA_VERSION,
        LEGACY_FEATURE_ARTIFACT_SCHEMA_VERSION,
    }
    if artifact_schema_version not in supported_feature_artifact_schemas:
        raise NativeOverlayBindingError(f"feature artifact schema drift for {expected_day}")
    if manifest.get("identity") != schema.IDENTITY or manifest.get("utc_day") != expected_day:
        raise NativeOverlayBindingError(f"feature identity/day drift for {expected_day}")
    if manifest.get("atomic_admission") is not True:
        raise NativeOverlayBindingError(f"feature panel is not atomically admitted: {expected_day}")
    if (
        manifest.get("labels_read") is not False
        or manifest.get("economic_outcomes_read") is not False
    ):
        raise NativeOverlayBindingError(f"feature panel crossed outcome boundary: {expected_day}")
    try:
        expected_panel_schema = prediction_overlays.feature_panel_schema_payload_for_version(
            artifact_schema_version
        )
    except prediction_overlays.PredictionOverlayMaterializationError as exc:
        raise NativeOverlayBindingError(
            f"feature artifact schema drift for {expected_day}"
        ) from exc
    if manifest.get("panel_schema") != expected_panel_schema:
        raise NativeOverlayBindingError(f"feature schema drift for {expected_day}")
    cache_payload = manifest.get("cache_identity_payload")
    if not isinstance(cache_payload, dict):
        raise NativeOverlayBindingError(f"feature cache identity missing for {expected_day}")
    if manifest.get("cache_identity_sha256") != _canonical_sha256(cache_payload):
        raise NativeOverlayBindingError(
            f"feature cache identity is not reproducible: {expected_day}"
        )
    expected_contract = full_schema.full_feature_contract_fingerprint()
    expected_source = schema.canonical_sha256(schema.source_manifest_payload())
    if cache_payload.get("feature_contract_sha256") != expected_contract:
        raise NativeOverlayBindingError(f"Feature DAG drift for {expected_day}")
    if cache_payload.get("source_manifest_sha256") != expected_source:
        raise NativeOverlayBindingError(f"source-clock manifest drift for {expected_day}")
    if cache_payload.get("feature_order_sha256") != schema.feature_order_sha256():
        raise NativeOverlayBindingError(f"feature order drift for {expected_day}")
    panel_entry = manifest.get("panel")
    if not isinstance(panel_entry, dict) or panel_entry.get("rows") != expected_rows:
        raise NativeOverlayBindingError(f"feature row-count drift for {expected_day}")
    if panel_entry.get("sha256") != expected_panel_sha256:
        raise NativeOverlayBindingError(f"feature panel binding drift for {expected_day}")
    parquet = pq.ParquetFile(panel_path)
    if parquet.metadata.num_rows != expected_rows:
        raise NativeOverlayBindingError(f"physical feature rows drift for {expected_day}")
    if not parquet.schema_arrow.equals(feature_panels.panel_arrow_schema(), check_metadata=False):
        raise NativeOverlayBindingError(f"physical feature schema drift for {expected_day}")
    return manifest


def _row_identity_sha256(
    parquet_path: Path,
    *,
    utc_day: str,
    expected_rows: int,
) -> str:
    digest = hashlib.sha256()
    row_offset = 0
    day_start_ms = int(
        datetime.strptime(utc_day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000
    )
    columns = list(prediction_overlays.JOIN_COLUMNS)
    for batch in pq.ParquetFile(parquet_path).iter_batches(batch_size=8_192, columns=columns):
        rows = batch.num_rows
        cutoff = np.asarray(batch.column(0), dtype=np.int64)
        decision = np.asarray(batch.column(1), dtype=np.int64)
        ready = np.asarray(batch.column(2), dtype=np.int64)
        expected = day_start_ms + np.arange(row_offset, row_offset + rows, dtype=np.int64) * 1_000
        if not np.array_equal(cutoff, expected) or not np.array_equal(decision, expected):
            raise NativeOverlayBindingError(f"noncanonical row order for {utc_day}")
        if np.any(ready > decision):
            raise NativeOverlayBindingError(f"future feature-ready timestamp for {utc_day}")
        for values in (cutoff, decision, ready):
            digest.update(values.astype("<i8", copy=False).tobytes(order="C"))
        for fingerprint in batch.column(3).to_pylist():
            if not _is_sha256(fingerprint):
                raise NativeOverlayBindingError(f"invalid feature row fingerprint for {utc_day}")
            encoded = fingerprint.encode("ascii")
            digest.update(struct.pack("<I", len(encoded)))
            digest.update(encoded)
        row_offset += rows
    if row_offset != expected_rows:
        raise NativeOverlayBindingError(f"row identity count drift for {utc_day}")
    return digest.hexdigest()


def _load_execution_prep(prep_root: Path, *, test_only_rows: int | None) -> dict[str, Any]:
    prep_root = prep_root.expanduser().resolve()
    manifest_path = prep_root / native_prep.MANIFEST_FILENAME
    success_path = prep_root / native_prep.SUCCESS_FILENAME
    if not manifest_path.is_file() or not success_path.is_file():
        raise NativeOverlayBindingError("native execution prep is not atomically admitted")
    manifest_sha = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha:
        raise NativeOverlayBindingError("native execution prep admission marker mismatch")
    manifest = _load_json_object(manifest_path, role="native execution prep manifest")
    if manifest.get("schema_version") != native_prep.MANIFEST_SCHEMA_VERSION:
        raise NativeOverlayBindingError("native execution prep schema drift")
    if manifest.get("identity") != native_prep.IDENTITY:
        raise NativeOverlayBindingError("native execution prep identity drift")
    if manifest.get("completed_day_count") != EXPECTED_DAY_COUNT:
        raise NativeOverlayBindingError("native execution prep must contain exactly 40 days")
    if manifest.get("status") != "feature_panels_complete_model_bundle_unbound":
        raise NativeOverlayBindingError(
            "native execution prep is not the frozen bundle-unbound identity"
        )
    if manifest.get("model_bundle_bound") is not False:
        raise NativeOverlayBindingError("native execution prep unexpectedly binds a model bundle")
    if manifest.get("economic_outcomes_read") is not False:
        raise NativeOverlayBindingError("native execution prep crossed economic outcome boundary")
    identity_payload = manifest.get("identity_payload")
    if not isinstance(identity_payload, dict):
        raise NativeOverlayBindingError("native execution prep identity payload missing")
    if identity_payload.get("model_bundle") is not None:
        raise NativeOverlayBindingError(
            "native execution prep identity contains a stale model bundle"
        )
    if manifest.get("execution_prep_identity_sha256") != _canonical_sha256(identity_payload):
        raise NativeOverlayBindingError("native execution prep identity cannot be reproduced")
    days = identity_payload.get("days")
    rows = identity_payload.get("feature_panels")
    if not isinstance(days, list) or not isinstance(rows, list):
        raise NativeOverlayBindingError("native execution prep denominator missing")
    if days != sorted(set(days)) or len(days) != EXPECTED_DAY_COUNT:
        raise NativeOverlayBindingError("native execution prep day denominator drift")
    if len(rows) != EXPECTED_DAY_COUNT:
        raise NativeOverlayBindingError("native execution prep daily binding count drift")
    expected_rows = test_only_rows if test_only_rows is not None else ROWS_PER_DAY
    validated: list[dict[str, Any]] = []
    for ordinal, (day, row) in enumerate(zip(days, rows, strict=True), start=1):
        if not isinstance(row, dict):
            raise NativeOverlayBindingError(f"invalid daily prep row: {day}")
        if row.get("ordinal") != ordinal or row.get("utc_day") != day:
            raise NativeOverlayBindingError(f"daily prep order drift: {day}")
        if row.get("feature_rows") != expected_rows:
            raise NativeOverlayBindingError(f"daily prep row-count drift: {day}")
        feature_dir = Path(str(row["feature_panel_dir"])).expanduser().resolve()
        manifest_path_for_day = Path(str(row["feature_manifest_path"])).expanduser().resolve()
        panel_path = Path(str(row["feature_panel_path"])).expanduser().resolve()
        if manifest_path_for_day != feature_dir / feature_panels.MANIFEST_FILENAME:
            raise NativeOverlayBindingError(f"feature manifest path drift: {day}")
        if panel_path != feature_dir / feature_panels.PANEL_FILENAME:
            raise NativeOverlayBindingError(f"feature panel path drift: {day}")
        feature_manifest = _validate_feature_manifest(
            manifest_path_for_day,
            panel_path,
            expected_day=day,
            expected_manifest_sha256=str(row["feature_manifest_sha256"]),
            expected_panel_sha256=str(row["feature_panel_sha256"]),
            expected_rows=expected_rows,
        )
        row_identity = _row_identity_sha256(
            panel_path,
            utc_day=day,
            expected_rows=expected_rows,
        )
        validated.append(
            {
                "ordinal": ordinal,
                "utc_day": day,
                "feature_panel_dir": str(feature_dir),
                "feature_manifest_path": str(manifest_path_for_day),
                "feature_manifest_sha256": str(row["feature_manifest_sha256"]),
                "feature_panel_path": str(panel_path),
                "feature_panel_sha256": str(row["feature_panel_sha256"]),
                "feature_cache_identity_sha256": feature_manifest["cache_identity_sha256"],
                "row_count": expected_rows,
                "row_identity_sha256": row_identity,
            }
        )
    return {
        "root": str(prep_root),
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "execution_prep_identity_sha256": manifest["execution_prep_identity_sha256"],
        "days": validated,
    }


def _validate_training_feature_inputs(training_identity: Mapping[str, Any]) -> dict[str, Any]:
    artifacts = training_identity.get("daily_artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 66:
        raise NativeOverlayBindingError("bundle training identity must bind 66 daily artifacts")
    observed_days = [str(row.get("utc_day", "")) for row in artifacts if isinstance(row, dict)]
    if len(observed_days) != 66 or observed_days != sorted(set(observed_days)):
        raise NativeOverlayBindingError("bundle training day order drift")
    feature_bindings: list[dict[str, Any]] = []
    for row in artifacts:
        feature_dir = Path(str(row["feature_panel_dir"])).expanduser().resolve()
        manifest_path = feature_dir / feature_panels.MANIFEST_FILENAME
        panel_path = feature_dir / feature_panels.PANEL_FILENAME
        _validate_feature_manifest(
            manifest_path,
            panel_path,
            expected_day=str(row["utc_day"]),
            expected_manifest_sha256=str(row["feature_manifest_sha256"]),
            expected_panel_sha256=str(row["feature_panel_sha256"]),
            expected_rows=ROWS_PER_DAY,
        )
        feature_bindings.append(
            {
                "utc_day": row["utc_day"],
                "feature_manifest_sha256": row["feature_manifest_sha256"],
                "feature_panel_sha256": row["feature_panel_sha256"],
            }
        )
    return {
        "day_count": 66,
        "first_day": observed_days[0],
        "last_day": observed_days[-1],
        "feature_artifact_set_sha256": _canonical_sha256(feature_bindings),
    }


def _load_bundle_binding(bundle_dir: Path) -> dict[str, Any]:
    admitted = prediction_overlays.load_admitted_research_bundle(bundle_dir)
    bundle = admitted.bundle
    identity = bundle.get("training_identity")
    if not isinstance(identity, dict):
        raise NativeOverlayBindingError("research bundle lacks its full training identity")
    if bundle.get("training_identity_sha256") != _canonical_sha256(identity):
        raise NativeOverlayBindingError("bundle training identity SHA256 mismatch")
    if identity.get("identity") != schema.IDENTITY:
        raise NativeOverlayBindingError("bundle training identity differs from 1s successor")
    if identity.get("inference_cadence_ms") != schema.CADENCE_MS:
        raise NativeOverlayBindingError("bundle inference cadence must be exactly 1s")
    if identity.get("heads") != list(EXPECTED_HEADS):
        raise NativeOverlayBindingError("bundle head order differs from the frozen 13 heads")
    if identity.get("feature_order") != list(schema.TRAINABLE_FEATURE_ORDER):
        raise NativeOverlayBindingError("bundle feature order differs from the 173-column ABI")
    if identity.get("feature_order_sha256") != schema.feature_order_sha256():
        raise NativeOverlayBindingError("bundle feature-order SHA256 drift")
    if identity.get("economic_outcomes_read") is not False:
        raise NativeOverlayBindingError("bundle training crossed economic outcome boundary")
    if identity.get("external_2026_panels_read") is not False:
        raise NativeOverlayBindingError("bundle training read the external 2026 panel")
    for head in admitted.heads:
        if head.metadata.get("feature_timestamp_semantics") != (
            "canonical_1s_decision_ready_at_boundary"
        ):
            raise NativeOverlayBindingError(f"head timestamp semantics drift: {head.head}")
    training_features = _validate_training_feature_inputs(identity)
    return {
        "bundle_dir": str(admitted.output_dir),
        "bundle_meta_path": str(admitted.bundle_path),
        "bundle_meta_sha256": admitted.bundle_sha256,
        "training_identity_sha256": bundle["training_identity_sha256"],
        "head_count": len(admitted.heads),
        "heads": [
            {
                "head": head.head,
                "model_path": str(head.model_path),
                "model_sha256": head.model_sha256,
                "metadata_path": str(head.metadata_path),
                "metadata_sha256": head.metadata_sha256,
            }
            for head in admitted.heads
        ],
        "training_feature_inputs": training_features,
    }


def _code_bindings() -> dict[str, dict[str, str]]:
    modules = {
        "binding": Path(__file__).resolve(),
        "prediction_overlay": Path(prediction_overlays.__file__).resolve(),
        "feature_schema": Path(schema.__file__).resolve(),
        "full_feature_dag": Path(full_schema.__file__).resolve(),
        "native_execution_prep": Path(native_prep.__file__).resolve(),
    }
    return {
        name: {"path": str(path), "sha256": _sha256_file(path)} for name, path in modules.items()
    }


def prepare_execution_plan(
    *,
    research_bundle_dir: Path | None,
    prep_root: Path = DEFAULT_PREP_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT,
    batch_rows: int = prediction_overlays.DEFAULT_BATCH_ROWS,
    test_only_row_count: int | None = None,
) -> dict[str, Any]:
    """Freeze a result-blind, hash-bound 40-day overlay execution plan."""

    if research_bundle_dir is None:
        raise NativeOverlayBindingError("candidate 1s research bundle is required; failing closed")
    if batch_rows <= 0:
        raise NativeOverlayBindingError("batch_rows must be positive")
    prep_root = prep_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    _require_cache_location(
        market_data_root=market_data_root,
        prep_root=prep_root,
        output_root=output_root,
    )
    prepared = _load_execution_prep(prep_root, test_only_rows=test_only_row_count)
    bundle = _load_bundle_binding(research_bundle_dir)
    source_clock_contract = _source_clock_contract_payload()
    days = []
    for row in prepared["days"]:
        day = row["utc_day"]
        days.append(
            row
            | {
                "overlay_output_dir": str(output_root / "overlays" / day),
            }
        )
    identity_payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "native_execution_prep": {
            key: prepared[key]
            for key in (
                "root",
                "manifest_path",
                "manifest_sha256",
                "execution_prep_identity_sha256",
            )
        },
        "research_bundle": bundle,
        "feature_contract": {
            "feature_dag_id": schema.FEATURE_DAG_ID,
            "feature_semantics_identity": schema.FEATURE_SEMANTICS_IDENTITY,
            "feature_order_sha256": schema.feature_order_sha256(),
            "full_feature_contract_sha256": full_schema.full_feature_contract_fingerprint(),
            "source_clock_contract": source_clock_contract,
            "source_clock_contract_sha256": _canonical_sha256(source_clock_contract),
            "cadence_ms": schema.CADENCE_MS,
        },
        "overlay_schema_sha256": _canonical_sha256(
            prediction_overlays.prediction_overlay_schema_payload()
        ),
        "code": _code_bindings(),
        "output_root": str(output_root),
        "batch_rows": batch_rows,
        "test_only_row_count": test_only_row_count,
        "days": days,
    }
    plan_identity = _canonical_sha256(identity_payload)
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "prediction_overlay_execution_planned_outcomes_unread",
        "plan_identity_sha256": plan_identity,
        "identity_payload": identity_payload,
        "day_count": len(days),
        "prediction_values_materialized": False,
        "labels_read": False,
        "economic_outcomes_read": False,
        "markouts_read": False,
        "campaign_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
        "market_features_copied_to_overlay": False,
        "atomic_daily_admission": True,
        "resume_safe": True,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    plan_path = output_root / PLAN_FILENAME
    success_path = output_root / PLAN_SUCCESS_FILENAME
    if plan_path.exists() or success_path.exists():
        if not (plan_path.is_file() and success_path.is_file()):
            raise NativeOverlayBindingError("incomplete prior execution-plan admission")
        existing = validate_execution_plan(plan_path, market_data_root=market_data_root)
        if existing["plan_identity_sha256"] != plan_identity:
            raise NativeOverlayBindingError("existing execution plan has a different identity")
        return existing
    _atomic_json(plan_path, plan)
    _atomic_text(success_path, _sha256_file(plan_path) + "\n")
    return plan | {
        "execution_plan_path": str(plan_path),
        "execution_plan_sha256": _sha256_file(plan_path),
    }


def validate_execution_plan(
    plan_path: Path,
    *,
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT,
) -> dict[str, Any]:
    """Re-resolve every physical binding in a frozen execution plan."""

    plan_path = plan_path.expanduser().resolve()
    plan = _load_json_object(plan_path, role="native overlay execution plan")
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION or plan.get("identity") != IDENTITY:
        raise NativeOverlayBindingError("unsupported native overlay execution plan")
    payload = plan.get("identity_payload")
    if not isinstance(payload, dict) or plan.get("plan_identity_sha256") != _canonical_sha256(
        payload
    ):
        raise NativeOverlayBindingError("execution plan identity cannot be reproduced")
    success_path = plan_path.parent / PLAN_SUCCESS_FILENAME
    if not success_path.is_file() or success_path.read_text(encoding="ascii").strip() != (
        _sha256_file(plan_path)
    ):
        raise NativeOverlayBindingError("execution plan admission marker mismatch")
    required_false = (
        "prediction_values_materialized",
        "labels_read",
        "economic_outcomes_read",
        "markouts_read",
        "campaign_outcomes_read",
        "action_authorized",
        "live_authorized",
        "market_features_copied_to_overlay",
    )
    if any(plan.get(field) is not False for field in required_false):
        raise NativeOverlayBindingError("execution plan violates its preparation-only boundary")
    if plan.get("day_count") != EXPECTED_DAY_COUNT:
        raise NativeOverlayBindingError("execution plan denominator is not 40 days")
    output_root = Path(str(payload["output_root"])).expanduser().resolve()
    prep_binding = payload.get("native_execution_prep")
    if not isinstance(prep_binding, dict):
        raise NativeOverlayBindingError("execution plan lacks native prep binding")
    _require_cache_location(
        market_data_root=market_data_root,
        prep_root=Path(str(prep_binding["root"])),
        output_root=output_root,
    )
    test_rows = payload.get("test_only_row_count")
    prepared = _load_execution_prep(
        Path(str(prep_binding["root"])),
        test_only_rows=test_rows,
    )
    if prepared["manifest_sha256"] != prep_binding.get("manifest_sha256"):
        raise NativeOverlayBindingError("execution plan native prep hash drift")
    bundle = _load_bundle_binding(Path(str(payload["research_bundle"]["bundle_dir"])))
    if bundle != payload.get("research_bundle"):
        raise NativeOverlayBindingError("execution plan bundle binding drift")
    expected_clock = _source_clock_contract_payload()
    feature_contract = payload.get("feature_contract")
    if not isinstance(feature_contract, dict):
        raise NativeOverlayBindingError("execution plan lacks Feature DAG contract")
    if feature_contract.get("source_clock_contract") != expected_clock:
        raise NativeOverlayBindingError("execution plan source-clock contract drift")
    if feature_contract.get("source_clock_contract_sha256") != _canonical_sha256(expected_clock):
        raise NativeOverlayBindingError("execution plan source-clock hash drift")
    for binding in payload.get("code", {}).values():
        path = Path(str(binding["path"])).expanduser().resolve()
        if _sha256_file(path) != binding.get("sha256"):
            raise NativeOverlayBindingError(f"execution code hash drift: {path}")
    expected_days = prepared["days"]
    observed_days = payload.get("days")
    if not isinstance(observed_days, list) or len(observed_days) != EXPECTED_DAY_COUNT:
        raise NativeOverlayBindingError("execution plan daily denominator drift")
    for expected, observed in zip(expected_days, observed_days, strict=True):
        if not isinstance(observed, dict):
            raise NativeOverlayBindingError("invalid execution-plan daily row")
        expected_with_output = expected | {
            "overlay_output_dir": str(output_root / "overlays" / expected["utc_day"])
        }
        if observed != expected_with_output:
            raise NativeOverlayBindingError(
                f"execution-plan row identity drift: {expected['utc_day']}"
            )
    return plan | {
        "execution_plan_path": str(plan_path),
        "execution_plan_sha256": _sha256_file(plan_path),
    }


def _validate_materialized_day(
    day_plan: Mapping[str, Any],
    *,
    bundle_sha256: str,
    test_only_row_count: int | None,
) -> dict[str, Any]:
    output_dir = Path(str(day_plan["overlay_output_dir"])).expanduser().resolve()
    overlay_path = output_dir / prediction_overlays.OVERLAY_FILENAME
    manifest_path = output_dir / prediction_overlays.MANIFEST_FILENAME
    success_path = output_dir / prediction_overlays.SUCCESS_FILENAME
    for path in (overlay_path, manifest_path, success_path):
        if not path.is_file():
            raise NativeOverlayBindingError(f"daily overlay is incomplete: {path}")
    manifest_sha = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha:
        raise NativeOverlayBindingError(f"daily overlay admission drift: {day_plan['utc_day']}")
    manifest = _load_json_object(manifest_path, role="daily prediction overlay")
    if manifest.get("research_bundle_sha256") != bundle_sha256:
        raise NativeOverlayBindingError(f"daily bundle binding drift: {day_plan['utc_day']}")
    if manifest.get("feature_panel_sha256") != day_plan["feature_panel_sha256"]:
        raise NativeOverlayBindingError(f"daily feature binding drift: {day_plan['utc_day']}")
    expected_rows = test_only_row_count if test_only_row_count is not None else ROWS_PER_DAY
    if manifest.get("overlay", {}).get("rows") != expected_rows:
        raise NativeOverlayBindingError(f"daily overlay row-count drift: {day_plan['utc_day']}")
    overlay_sha = _sha256_file(overlay_path)
    if manifest.get("overlay", {}).get("sha256") != overlay_sha:
        raise NativeOverlayBindingError(f"daily overlay SHA256 drift: {day_plan['utc_day']}")
    row_identity = _row_identity_sha256(
        overlay_path,
        utc_day=str(day_plan["utc_day"]),
        expected_rows=expected_rows,
    )
    if row_identity != day_plan["row_identity_sha256"]:
        raise NativeOverlayBindingError(
            f"feature/overlay row identity mismatch: {day_plan['utc_day']}"
        )
    return {
        "ordinal": day_plan["ordinal"],
        "utc_day": day_plan["utc_day"],
        "feature_panel_sha256": day_plan["feature_panel_sha256"],
        "row_identity_sha256": row_identity,
        "overlay_dir": str(output_dir),
        "overlay_path": str(overlay_path),
        "overlay_sha256": overlay_sha,
        "overlay_manifest_path": str(manifest_path),
        "overlay_manifest_sha256": manifest_sha,
        "overlay_cache_identity_sha256": manifest["cache_identity_sha256"],
        "row_count": expected_rows,
    }


def materialize_execution_plan(
    plan_path: Path,
    *,
    days: Sequence[str] | None = None,
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT,
) -> dict[str, Any]:
    """Materialize selected days and admit the complete panel when all 40 exist."""

    plan = validate_execution_plan(plan_path, market_data_root=market_data_root)
    payload = plan["identity_payload"]
    all_days = [row["utc_day"] for row in payload["days"]]
    selected = all_days if days is None else list(days)
    if (
        not selected
        or selected != sorted(set(selected))
        or any(day not in all_days for day in selected)
    ):
        raise NativeOverlayBindingError(
            "requested days must be a sorted nonempty subset of the plan"
        )
    output_root = Path(str(payload["output_root"])).expanduser().resolve()
    bundle_dir = Path(str(payload["research_bundle"]["bundle_dir"])).expanduser().resolve()
    bundle_sha = str(payload["research_bundle"]["bundle_meta_sha256"])
    test_rows = payload.get("test_only_row_count")
    rows_by_day = {row["utc_day"]: row for row in payload["days"]}
    started = time.perf_counter()
    for index, day in enumerate(selected, start=1):
        row = rows_by_day[day]
        result = prediction_overlays.materialize_daily_prediction_overlay(
            feature_panel_dir=Path(str(row["feature_panel_dir"])),
            research_bundle_dir=bundle_dir,
            output_dir=Path(str(row["overlay_output_dir"])),
            batch_rows=int(payload["batch_rows"]),
            test_only_row_count=test_rows,
        )
        validated = _validate_materialized_day(
            row,
            bundle_sha256=bundle_sha,
            test_only_row_count=test_rows,
        )
        completed = []
        for candidate in payload["days"]:
            if Path(str(candidate["overlay_output_dir"])).is_dir():
                completed.append(
                    _validate_materialized_day(
                        candidate,
                        bundle_sha256=bundle_sha,
                        test_only_row_count=test_rows,
                    )
                )
        _atomic_json(
            output_root / PROGRESS_FILENAME,
            {
                "schema_version": SCHEMA_VERSION,
                "identity": IDENTITY,
                "status": "materializing_prediction_overlays",
                "plan_identity_sha256": plan["plan_identity_sha256"],
                "completed_day_count": len(completed),
                "total_day_count": EXPECTED_DAY_COUNT,
                "last_requested_day": day,
                "last_overlay_reused": result.reused,
                "days": sorted(completed, key=lambda item: int(item["ordinal"])),
                "labels_read": False,
                "economic_outcomes_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        )
        print(
            f"[{index:02d}/{len(selected)}] {day} reused={result.reused} "
            f"rows={validated['row_count']}",
            flush=True,
        )
    completed = [
        _validate_materialized_day(
            row,
            bundle_sha256=bundle_sha,
            test_only_row_count=test_rows,
        )
        for row in payload["days"]
        if Path(str(row["overlay_output_dir"])).is_dir()
    ]
    if len(completed) != EXPECTED_DAY_COUNT:
        return {
            "status": "partial_prediction_overlay_materialization",
            "completed_day_count": len(completed),
            "total_day_count": EXPECTED_DAY_COUNT,
            "execution_input_eligible": False,
            "economic_outcomes_read": False,
        }
    completed.sort(key=lambda item: int(item["ordinal"]))
    panel_identity = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "identity": IDENTITY,
        "plan_identity_sha256": plan["plan_identity_sha256"],
        "execution_plan_sha256": plan["execution_plan_sha256"],
        "bundle_meta_sha256": bundle_sha,
        "feature_dag_id": schema.FEATURE_DAG_ID,
        "source_clock_contract_sha256": payload["feature_contract"]["source_clock_contract_sha256"],
        "ordered_days": all_days,
        "daily_overlays": completed,
    }
    panel_manifest = {
        "schema_version": PANEL_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "prediction_overlays_complete_execution_input_eligible",
        "panel_identity_sha256": _canonical_sha256(panel_identity),
        "identity_payload": panel_identity,
        "completed_day_count": EXPECTED_DAY_COUNT,
        "total_rows": EXPECTED_DAY_COUNT * (test_rows if test_rows is not None else ROWS_PER_DAY),
        "elapsed_seconds": time.perf_counter() - started,
        "prediction_values_materialized": True,
        "execution_input_eligible": True,
        "labels_read": False,
        "economic_outcomes_read": False,
        "markouts_read": False,
        "campaign_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
        "market_features_copied_to_overlay": False,
        "atomic_admission": True,
        "resume_safe": True,
    }
    manifest_path = output_root / PANEL_MANIFEST_FILENAME
    success_path = output_root / PANEL_SUCCESS_FILENAME
    if manifest_path.exists() or success_path.exists():
        if not (manifest_path.is_file() and success_path.is_file()):
            raise NativeOverlayBindingError("incomplete prior overlay-panel admission")
        existing = _load_json_object(manifest_path, role="overlay panel manifest")
        if existing.get("panel_identity_sha256") != panel_manifest["panel_identity_sha256"]:
            raise NativeOverlayBindingError("existing overlay panel has a different identity")
        if success_path.read_text(encoding="ascii").strip() != _sha256_file(manifest_path):
            raise NativeOverlayBindingError("overlay panel admission marker mismatch")
        return existing | {
            "overlay_panel_manifest_path": str(manifest_path),
            "overlay_panel_manifest_sha256": _sha256_file(manifest_path),
        }
    _atomic_json(manifest_path, panel_manifest)
    _atomic_text(success_path, _sha256_file(manifest_path) + "\n")
    return panel_manifest | {
        "overlay_panel_manifest_path": str(manifest_path),
        "overlay_panel_manifest_sha256": _sha256_file(manifest_path),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--research-bundle-dir", type=Path, required=True)
    prepare.add_argument("--prep-root", type=Path, default=DEFAULT_PREP_ROOT)
    prepare.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    prepare.add_argument("--market-data-root", type=Path, default=DEFAULT_MARKET_DATA_ROOT)
    prepare.add_argument("--batch-rows", type=int, default=prediction_overlays.DEFAULT_BATCH_ROWS)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--plan", type=Path, required=True)
    validate.add_argument("--market-data-root", type=Path, default=DEFAULT_MARKET_DATA_ROOT)
    run = subparsers.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--days", nargs="*")
    run.add_argument("--market-data-root", type=Path, default=DEFAULT_MARKET_DATA_ROOT)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        result = prepare_execution_plan(
            research_bundle_dir=args.research_bundle_dir,
            prep_root=args.prep_root,
            output_root=args.output_root,
            market_data_root=args.market_data_root,
            batch_rows=args.batch_rows,
        )
    elif args.command == "validate":
        result = validate_execution_plan(args.plan, market_data_root=args.market_data_root)
    else:
        result = materialize_execution_plan(
            args.plan,
            days=args.days,
            market_data_root=args.market_data_root,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
