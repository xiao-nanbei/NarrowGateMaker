#!/usr/bin/env python3
"""Atomically persist one F03 daily 1s research prediction overlay."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training as training,
)

SCHEMA_VERSION = "causal_v12_1s_prediction_overlay_materializer.v2"
ARTIFACT_SCHEMA_VERSION = "causal_v12_1s_daily_prediction_overlay_artifact.v2"
LEGACY_FEATURE_PANEL_ARTIFACT_SCHEMA_VERSION = (
    "causal_v12_1s_daily_feature_panel_artifact.v3"
)
HEAD_META_SCHEMA_VERSION = "causal_v12_1s_head_meta.v1"
OVERLAY_FILENAME = "prediction_overlay.parquet"
MANIFEST_FILENAME = "manifest.json"
SUCCESS_FILENAME = "_SUCCESS"
AUTHORITATIVE_DAILY_ROWS = 86_400
DEFAULT_BATCH_ROWS = 4_096
JOIN_COLUMNS = (
    "cutoff_exclusive_ms",
    "decision_ts_ms",
    "feature_ready_ts_ms",
    "feature_row_fingerprint_sha256",
)
HEADS = tuple(training.HEAD_SPECS)
PREDICTION_COLUMN_BY_HEAD = {head: f"prediction__{head}" for head in HEADS}
CLASSIFICATION_HEADS = frozenset(head for head, spec in training.HEAD_SPECS.items() if spec[3])
VOLATILITY_HEADS = frozenset(head for head in HEADS if head.startswith("vol_"))
PREDICTION_POSTPROCESS_CONTRACT = {
    "classification_heads": "clip_raw_prediction_to_closed_interval_0_1",
    "volatility_heads": "max_raw_prediction_with_zero",
    "return_heads": "identity",
    "authority": "strategy.signal.SignalGenerator._predict_all",
}


class PredictionOverlayMaterializationError(ValueError):
    """Raised when an input or output violates the prediction-overlay contract."""


def feature_panel_schema_payload_for_version(schema_version: object) -> dict[str, Any]:
    """Return the exact accepted feature-panel projection for an artifact version."""

    current = panels.panel_schema_payload()
    if schema_version == panels.ARTIFACT_SCHEMA_VERSION:
        return current
    if schema_version == LEGACY_FEATURE_PANEL_ARTIFACT_SCHEMA_VERSION:
        return {
            **current,
            "schema_version": LEGACY_FEATURE_PANEL_ARTIFACT_SCHEMA_VERSION,
        }
    raise PredictionOverlayMaterializationError("feature panel artifact schema mismatch")


@dataclass(frozen=True, slots=True)
class AdmittedFeaturePanel:
    output_dir: Path
    panel_path: Path
    manifest_path: Path
    manifest: dict[str, Any]
    manifest_sha256: str
    panel_sha256: str
    row_count: int


@dataclass(frozen=True, slots=True)
class ResearchHeadArtifact:
    head: str
    model_path: Path
    model_sha256: str
    metadata_path: Path
    metadata_sha256: str
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AdmittedResearchBundle:
    output_dir: Path
    bundle_path: Path
    bundle_sha256: str
    bundle: dict[str, Any]
    heads: tuple[ResearchHeadArtifact, ...]


@dataclass(frozen=True, slots=True)
class MaterializedPredictionOverlay:
    output_dir: Path
    overlay_path: Path
    manifest_path: Path
    cache_identity_sha256: str
    row_count: int
    reused: bool
    test_only: bool


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_fsync(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _write_text_fsync(path: Path, value: str) -> None:
    with path.open("w", encoding="ascii") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def _load_json_object(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PredictionOverlayMaterializationError(f"invalid {role} JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise PredictionOverlayMaterializationError(f"{role} must be a JSON object")
    return payload


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def prediction_overlay_arrow_schema() -> pa.Schema:
    fields = [
        pa.field("cutoff_exclusive_ms", pa.int64(), nullable=False),
        pa.field("decision_ts_ms", pa.int64(), nullable=False),
        pa.field("feature_ready_ts_ms", pa.int64(), nullable=False),
        pa.field("feature_row_fingerprint_sha256", pa.string(), nullable=False),
    ]
    fields.extend(
        pa.field(PREDICTION_COLUMN_BY_HEAD[head], pa.float64(), nullable=False) for head in HEADS
    )
    return pa.schema(fields)


def prediction_overlay_schema_payload() -> dict[str, Any]:
    arrow_schema = prediction_overlay_arrow_schema()
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "columns": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow_schema
        ],
        "join_columns": list(JOIN_COLUMNS),
        "head_order": list(HEADS),
        "head_count": len(HEADS),
        "prediction_columns": [PREDICTION_COLUMN_BY_HEAD[head] for head in HEADS],
        "prediction_postprocess": PREDICTION_POSTPROCESS_CONTRACT,
        "feature_columns_copied": False,
        "label_columns_allowed": False,
        "economic_outcome_columns_allowed": False,
    }


def _expected_rows(test_only_row_count: int | None) -> tuple[int, bool]:
    if test_only_row_count is None:
        return AUTHORITATIVE_DAILY_ROWS, False
    if isinstance(test_only_row_count, bool) or not isinstance(test_only_row_count, int):
        raise PredictionOverlayMaterializationError(
            "test_only_row_count must be an explicit positive integer"
        )
    if test_only_row_count <= 0:
        raise PredictionOverlayMaterializationError(
            "test_only_row_count must be an explicit positive integer"
        )
    if test_only_row_count == AUTHORITATIVE_DAILY_ROWS:
        raise PredictionOverlayMaterializationError(
            "omit test_only_row_count for a formal 86,400-row artifact"
        )
    return test_only_row_count, True


def load_admitted_feature_panel(
    feature_panel_dir: Path,
    *,
    test_only_row_count: int | None = None,
) -> AdmittedFeaturePanel:
    expected_rows, _ = _expected_rows(test_only_row_count)
    output_dir = feature_panel_dir.expanduser().resolve()
    manifest_path = output_dir / panels.MANIFEST_FILENAME
    panel_path = output_dir / panels.PANEL_FILENAME
    success_path = output_dir / panels.SUCCESS_FILENAME
    for path in (manifest_path, panel_path, success_path):
        if not path.is_file():
            raise PredictionOverlayMaterializationError(
                f"feature panel is not atomically admitted; missing {path.name}"
            )

    manifest_sha256 = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha256:
        raise PredictionOverlayMaterializationError(
            "feature panel _SUCCESS does not bind its manifest SHA256"
        )
    manifest = _load_json_object(manifest_path, role="feature panel manifest")
    expected_panel_schema = feature_panel_schema_payload_for_version(
        manifest.get("schema_version")
    )
    if manifest.get("atomic_admission") is not True:
        raise PredictionOverlayMaterializationError(
            "feature panel lacks atomic admission authority"
        )
    if manifest.get("panel_schema") != expected_panel_schema:
        raise PredictionOverlayMaterializationError("feature panel schema payload mismatch")
    cache_payload = manifest.get("cache_identity_payload")
    if not isinstance(cache_payload, dict):
        raise PredictionOverlayMaterializationError(
            "feature panel manifest lacks cache identity payload"
        )
    cache_identity = manifest.get("cache_identity_sha256")
    if cache_identity != _canonical_sha256(cache_payload):
        raise PredictionOverlayMaterializationError(
            "feature panel cache identity cannot be reproduced from its payload"
        )
    panel_entry = manifest.get("panel")
    if not isinstance(panel_entry, dict):
        raise PredictionOverlayMaterializationError("feature panel manifest lacks panel identity")
    if panel_entry.get("path") != panels.PANEL_FILENAME:
        raise PredictionOverlayMaterializationError("feature panel manifest path mismatch")
    panel_sha256 = _sha256_file(panel_path)
    if panel_entry.get("sha256") != panel_sha256:
        raise PredictionOverlayMaterializationError("feature panel SHA256 mismatch")
    parquet = pq.ParquetFile(panel_path)
    if parquet.metadata.num_rows != expected_rows or panel_entry.get("rows") != expected_rows:
        qualifier = "test-only" if test_only_row_count is not None else "formal"
        raise PredictionOverlayMaterializationError(
            f"{qualifier} prediction overlay requires exactly {expected_rows:,} panel rows"
        )
    if not parquet.schema_arrow.equals(panels.panel_arrow_schema(), check_metadata=False):
        raise PredictionOverlayMaterializationError("feature panel Parquet schema mismatch")
    return AdmittedFeaturePanel(
        output_dir=output_dir,
        panel_path=panel_path,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        panel_sha256=panel_sha256,
        row_count=expected_rows,
    )


def _bundle_member(bundle_dir: Path, value: Any, *, role: str) -> Path:
    if not isinstance(value, str) or not value:
        raise PredictionOverlayMaterializationError(f"{role} path is missing")
    relative = Path(value)
    if relative.is_absolute() or len(relative.parts) != 1 or relative.name != value:
        raise PredictionOverlayMaterializationError(
            f"{role} must be a direct child of the research bundle"
        )
    path = (bundle_dir / relative).resolve()
    if path.parent != bundle_dir or not path.is_file():
        raise PredictionOverlayMaterializationError(f"{role} is missing: {path}")
    return path


def _validate_head_artifact(
    bundle_dir: Path,
    *,
    head: str,
    entry: Any,
) -> ResearchHeadArtifact:
    if not isinstance(entry, dict):
        raise PredictionOverlayMaterializationError(f"bundle head entry is invalid: {head}")
    model_entry = entry.get("model")
    metadata_entry = entry.get("metadata")
    if not isinstance(model_entry, dict) or not isinstance(metadata_entry, dict):
        raise PredictionOverlayMaterializationError(
            f"bundle head lacks model or metadata identity: {head}"
        )
    model_path = _bundle_member(
        bundle_dir,
        model_entry.get("path"),
        role=f"{head} model",
    )
    metadata_path = _bundle_member(
        bundle_dir,
        metadata_entry.get("path"),
        role=f"{head} metadata",
    )
    model_sha256 = _sha256_file(model_path)
    metadata_sha256 = _sha256_file(metadata_path)
    if model_entry.get("sha256") != model_sha256:
        raise PredictionOverlayMaterializationError(f"model SHA256 mismatch for {head}")
    if metadata_entry.get("sha256") != metadata_sha256:
        raise PredictionOverlayMaterializationError(f"metadata SHA256 mismatch for {head}")

    metadata = _load_json_object(metadata_path, role=f"{head} metadata")
    label_column, objective, metric, _ = training.HEAD_SPECS[head]
    required_values = {
        "schema_version": HEAD_META_SCHEMA_VERSION,
        "name": head,
        "label_col": label_column,
        "objective": objective,
        "metric": metric,
        "feature_count": len(schema.TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": schema.feature_order_sha256(),
        "feature_bucket_ms": schema.CADENCE_MS,
        "model_sha256": model_sha256,
        "research_only": True,
        "prediction_authority": False,
        "action_authority": False,
        "live_authority": False,
    }
    for field, expected in required_values.items():
        if metadata.get(field) != expected:
            raise PredictionOverlayMaterializationError(
                f"{head} metadata {field} differs from the 1s research contract"
            )
    if metadata.get("feature_cols") != list(schema.TRAINABLE_FEATURE_ORDER):
        raise PredictionOverlayMaterializationError(
            f"{head} metadata feature order differs from the exact 173-column schema"
        )
    return ResearchHeadArtifact(
        head=head,
        model_path=model_path,
        model_sha256=model_sha256,
        metadata_path=metadata_path,
        metadata_sha256=metadata_sha256,
        metadata=metadata,
    )


def load_admitted_research_bundle(bundle_dir: Path) -> AdmittedResearchBundle:
    output_dir = bundle_dir.expanduser().resolve()
    bundle_path = output_dir / "bundle_meta.json"
    success_path = output_dir / training.SUCCESS_FILENAME
    for path in (bundle_path, success_path):
        if not path.is_file():
            raise PredictionOverlayMaterializationError(
                f"research bundle is not atomically admitted; missing {path.name}"
            )
    bundle_sha256 = _sha256_file(bundle_path)
    if success_path.read_text(encoding="ascii").strip() != bundle_sha256:
        raise PredictionOverlayMaterializationError(
            "research bundle _SUCCESS does not bind bundle_meta.json"
        )
    bundle = _load_json_object(bundle_path, role="research bundle metadata")
    if bundle.get("schema_version") != training.BUNDLE_SCHEMA_VERSION:
        raise PredictionOverlayMaterializationError(
            "research bundle schema must be causal_v12_1s_research_bundle.v1"
        )
    if bundle.get("identity") != schema.IDENTITY:
        raise PredictionOverlayMaterializationError(
            "research bundle identity differs from the 1s successor"
        )
    if not _is_sha256(bundle.get("training_identity_sha256")):
        raise PredictionOverlayMaterializationError(
            "research bundle lacks a valid training identity SHA256"
        )
    if bundle.get("status") != "research_only_transport_and_economics_not_run":
        raise PredictionOverlayMaterializationError("bundle is not the frozen research-only status")
    if bundle.get("atomic_admission") is not True:
        raise PredictionOverlayMaterializationError("research bundle is not atomically admitted")
    if bundle.get("head_count") != len(HEADS):
        raise PredictionOverlayMaterializationError("research bundle must declare 13 heads")
    bundle_heads = bundle.get("heads")
    if not isinstance(bundle_heads, dict) or set(bundle_heads) != set(HEADS):
        raise PredictionOverlayMaterializationError(
            "research bundle must contain exactly the frozen 13 heads"
        )
    required_false = (
        "prediction_outcomes_read",
        "economic_outcomes_read",
        "native_transport_run",
        "full_path_ml_ab_run",
        "prediction_authority",
        "action_authority",
        "live_authority",
    )
    if any(bundle.get(field) is not False for field in required_false):
        raise PredictionOverlayMaterializationError(
            "research bundle violates its research-only permission boundary"
        )
    heads = tuple(
        _validate_head_artifact(output_dir, head=head, entry=bundle_heads[head]) for head in HEADS
    )
    return AdmittedResearchBundle(
        output_dir=output_dir,
        bundle_path=bundle_path,
        bundle_sha256=bundle_sha256,
        bundle=bundle,
        heads=heads,
    )


def _identity_payload(
    panel: AdmittedFeaturePanel,
    bundle: AdmittedResearchBundle,
    *,
    test_only: bool,
) -> dict[str, Any]:
    materializer_path = Path(__file__).resolve()
    schema_path = Path(schema.__file__).resolve()
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_schema_sha256": _canonical_sha256(prediction_overlay_schema_payload()),
        "utc_day": panel.manifest.get("utc_day"),
        "row_contract": {
            "rows": panel.row_count,
            "formal_daily_rows": AUTHORITATIVE_DAILY_ROWS,
            "test_only": test_only,
        },
        "feature_panel": {
            "manifest_path": str(panel.manifest_path),
            "manifest_sha256": panel.manifest_sha256,
            "panel_path": str(panel.panel_path),
            "panel_sha256": panel.panel_sha256,
            "cache_identity_sha256": panel.manifest.get("cache_identity_sha256"),
            "schema_sha256": _canonical_sha256(panel.manifest["panel_schema"]),
            "rows": panel.row_count,
        },
        "research_bundle": {
            "bundle_path": str(bundle.bundle_path),
            "bundle_sha256": bundle.bundle_sha256,
            "schema_version": training.BUNDLE_SCHEMA_VERSION,
            "training_identity_sha256": bundle.bundle.get("training_identity_sha256"),
            "heads": [
                {
                    "head": artifact.head,
                    "model_sha256": artifact.model_sha256,
                    "metadata_sha256": artifact.metadata_sha256,
                }
                for artifact in bundle.heads
            ],
        },
        "code": {
            "materializer_path": str(materializer_path),
            "materializer_sha256": _sha256_file(materializer_path),
            "schema_path": str(schema_path),
            "schema_sha256": _sha256_file(schema_path),
            "lightgbm_version": lgb.__version__,
        },
        "feature_schema": {
            "feature_count": len(schema.TRAINABLE_FEATURE_ORDER),
            "feature_order_sha256": schema.feature_order_sha256(),
            "feature_bucket_ms": schema.CADENCE_MS,
            "trainable_schema_sha256": _canonical_sha256(schema.trainable_schema_payload()),
        },
        "output_schema": prediction_overlay_schema_payload(),
    }


def cache_identity_sha256(
    panel: AdmittedFeaturePanel,
    bundle: AdmittedResearchBundle,
    *,
    test_only: bool,
) -> str:
    return _canonical_sha256(_identity_payload(panel, bundle, test_only=test_only))


def _load_booster(path: Path) -> lgb.Booster:
    return lgb.Booster(model_file=str(path))


def _postprocess_prediction(head: str, prediction: np.ndarray) -> np.ndarray:
    """Apply the canonical live 13-head output-domain projection."""

    if head in VOLATILITY_HEADS:
        return np.maximum(prediction, 0.0)
    if head in CLASSIFICATION_HEADS:
        return np.clip(prediction, 0.0, 1.0)
    return prediction


def _load_and_validate_boosters(
    bundle: AdmittedResearchBundle,
) -> dict[str, lgb.Booster]:
    boosters: dict[str, lgb.Booster] = {}
    expected_features = list(schema.TRAINABLE_FEATURE_ORDER)
    for artifact in bundle.heads:
        try:
            booster = _load_booster(artifact.model_path)
        except Exception as exc:
            raise PredictionOverlayMaterializationError(
                f"cannot load LightGBM model for {artifact.head}"
            ) from exc
        if int(booster.num_feature()) != len(expected_features):
            raise PredictionOverlayMaterializationError(
                f"LightGBM model feature count mismatch for {artifact.head}"
            )
        if list(booster.feature_name()) != expected_features:
            raise PredictionOverlayMaterializationError(
                f"LightGBM model feature order mismatch for {artifact.head}"
            )
        boosters[artifact.head] = booster
    return boosters


def _validate_join_batch(
    batch: pa.RecordBatch,
    *,
    utc_day: str,
    row_offset: int,
) -> None:
    day_start_ms = int(
        datetime.strptime(utc_day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000
    )
    rows = batch.num_rows
    expected_cutoffs = (
        day_start_ms
        + np.arange(
            row_offset,
            row_offset + rows,
            dtype=np.int64,
        )
        * schema.CADENCE_MS
    )
    cutoff = batch.column(batch.schema.get_field_index("cutoff_exclusive_ms")).to_numpy()
    decision = batch.column(batch.schema.get_field_index("decision_ts_ms")).to_numpy()
    ready = batch.column(batch.schema.get_field_index("feature_ready_ts_ms")).to_numpy()
    if not np.array_equal(cutoff, expected_cutoffs):
        raise PredictionOverlayMaterializationError(
            "feature panel rows are not the canonical ordered 1s decision grid"
        )
    if not np.array_equal(decision, cutoff):
        raise PredictionOverlayMaterializationError(
            "feature panel decision timestamps differ from canonical cutoffs"
        )
    if np.any(ready > decision):
        raise PredictionOverlayMaterializationError(
            "feature panel contains future feature-ready timestamps"
        )
    fingerprints = batch.column(
        batch.schema.get_field_index("feature_row_fingerprint_sha256")
    ).to_pylist()
    if any(
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
        for value in fingerprints
    ):
        raise PredictionOverlayMaterializationError(
            "feature panel contains invalid row fingerprints"
        )


def _predict_overlay(
    panel: AdmittedFeaturePanel,
    bundle: AdmittedResearchBundle,
    output_path: Path,
    *,
    batch_rows: int,
) -> int:
    if batch_rows <= 0:
        raise PredictionOverlayMaterializationError("batch_rows must be positive")
    boosters = _load_and_validate_boosters(bundle)
    selected_columns = [*JOIN_COLUMNS, *schema.TRAINABLE_FEATURE_ORDER]
    parquet = pq.ParquetFile(panel.panel_path)
    writer = pq.ParquetWriter(
        output_path,
        prediction_overlay_arrow_schema(),
        compression="zstd",
        write_statistics=True,
    )
    row_count = 0
    try:
        for batch in parquet.iter_batches(batch_size=batch_rows, columns=selected_columns):
            _validate_join_batch(
                batch,
                utc_day=str(panel.manifest.get("utc_day")),
                row_offset=row_count,
            )
            feature_table = pa.Table.from_batches([batch]).select(
                list(schema.TRAINABLE_FEATURE_ORDER)
            )
            matrix = feature_table.to_pandas().to_numpy(dtype=np.float32, copy=False)
            if np.isinf(matrix).any():
                raise PredictionOverlayMaterializationError(
                    "feature panel contains infinite model inputs"
                )
            output_arrays: list[pa.Array] = [
                batch.column(batch.schema.get_field_index(name)) for name in JOIN_COLUMNS
            ]
            for head in HEADS:
                prediction = np.asarray(boosters[head].predict(matrix), dtype=np.float64)
                if prediction.shape != (batch.num_rows,) or not np.isfinite(prediction).all():
                    raise PredictionOverlayMaterializationError(
                        f"model produced invalid prediction shape or values for {head}"
                    )
                prediction = _postprocess_prediction(head, prediction)
                output_arrays.append(pa.array(prediction, type=pa.float64()))
            writer.write_table(
                pa.Table.from_arrays(
                    output_arrays,
                    schema=prediction_overlay_arrow_schema(),
                )
            )
            row_count += batch.num_rows
    finally:
        writer.close()
    if row_count != panel.row_count:
        raise PredictionOverlayMaterializationError(
            "prediction overlay row count differs from the admitted feature panel"
        )
    return row_count


def _load_reusable(
    output_dir: Path,
    *,
    expected_identity: str,
    expected_payload: dict[str, Any],
    expected_rows: int,
    test_only: bool,
) -> MaterializedPredictionOverlay:
    manifest_path = output_dir / MANIFEST_FILENAME
    overlay_path = output_dir / OVERLAY_FILENAME
    success_path = output_dir / SUCCESS_FILENAME
    for path in (manifest_path, overlay_path, success_path):
        if not path.is_file():
            raise PredictionOverlayMaterializationError(
                "existing output directory is not an admitted prediction overlay"
            )
    manifest_sha256 = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha256:
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay _SUCCESS is invalid"
        )
    manifest = _load_json_object(manifest_path, role="prediction overlay manifest")
    if manifest.get("schema_version") != ARTIFACT_SCHEMA_VERSION:
        raise PredictionOverlayMaterializationError("existing prediction overlay artifact mismatch")
    if manifest.get("cache_identity_sha256") != expected_identity:
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay is not hash-compatible with requested inputs"
        )
    if manifest.get("cache_identity_payload") != expected_payload:
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay identity payload differs from requested inputs"
        )
    if _canonical_sha256(manifest["cache_identity_payload"]) != expected_identity:
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay cache identity cannot be reproduced"
        )
    if manifest.get("overlay_schema") != prediction_overlay_schema_payload():
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay schema payload mismatch"
        )
    if manifest.get("atomic_admission") is not True:
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay is not atomically admitted"
        )
    required_false = (
        "labels_read",
        "economic_outcomes_read",
        "training_performed",
        "prediction_authorized",
        "action_authorized",
        "live_authorized",
    )
    if any(manifest.get(field) is not False for field in required_false):
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay violates its permission boundary"
        )
    if manifest.get("test_only") is not test_only:
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay test-only identity mismatch"
        )
    overlay = manifest.get("overlay")
    if (
        not isinstance(overlay, dict)
        or overlay.get("path") != OVERLAY_FILENAME
        or overlay.get("compression") != "zstd"
        or overlay.get("sha256") != _sha256_file(overlay_path)
        or overlay.get("rows") != expected_rows
    ):
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay file identity mismatch"
        )
    parquet = pq.ParquetFile(overlay_path)
    if parquet.metadata.num_rows != expected_rows:
        raise PredictionOverlayMaterializationError(
            "existing prediction overlay Parquet is incomplete"
        )
    if not parquet.schema_arrow.equals(prediction_overlay_arrow_schema(), check_metadata=False):
        raise PredictionOverlayMaterializationError("existing prediction overlay schema mismatch")
    return MaterializedPredictionOverlay(
        output_dir=output_dir,
        overlay_path=overlay_path,
        manifest_path=manifest_path,
        cache_identity_sha256=expected_identity,
        row_count=expected_rows,
        reused=True,
        test_only=test_only,
    )


def materialize_daily_prediction_overlay(
    *,
    feature_panel_dir: Path,
    research_bundle_dir: Path,
    output_dir: Path,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    test_only_row_count: int | None = None,
) -> MaterializedPredictionOverlay:
    """Run all 13 research heads and atomically admit a prediction-only overlay."""

    expected_rows, test_only = _expected_rows(test_only_row_count)
    panel = load_admitted_feature_panel(
        feature_panel_dir,
        test_only_row_count=test_only_row_count,
    )
    if panel.row_count != expected_rows:
        raise PredictionOverlayMaterializationError("resolved panel row contract mismatch")
    bundle = load_admitted_research_bundle(research_bundle_dir)
    expected_payload = _identity_payload(panel, bundle, test_only=test_only)
    expected_identity = _canonical_sha256(expected_payload)
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        return _load_reusable(
            output_dir,
            expected_identity=expected_identity,
            expected_payload=expected_payload,
            expected_rows=expected_rows,
            test_only=test_only,
        )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex}"
    temporary_dir.mkdir()
    try:
        overlay_path = temporary_dir / OVERLAY_FILENAME
        row_count = _predict_overlay(
            panel,
            bundle,
            overlay_path,
            batch_rows=batch_rows,
        )
        _fsync_file(overlay_path)
        overlay_sha256 = _sha256_file(overlay_path)
        manifest = {
            "schema_version": ARTIFACT_SCHEMA_VERSION,
            "identity": schema.IDENTITY,
            "status": (
                "test_only_prediction_overlay_not_authorized"
                if test_only
                else "research_prediction_overlay_not_authorized"
            ),
            "created_at_utc": datetime.now(tz=UTC).isoformat(),
            "utc_day": panel.manifest.get("utc_day"),
            "cache_identity_sha256": expected_identity,
            "cache_identity_payload": expected_payload,
            "feature_panel_manifest_sha256": panel.manifest_sha256,
            "feature_panel_sha256": panel.panel_sha256,
            "research_bundle_sha256": bundle.bundle_sha256,
            "materializer_sha256": expected_payload["code"]["materializer_sha256"],
            "trainable_schema_sha256": expected_payload["code"]["schema_sha256"],
            "feature_order_sha256": schema.feature_order_sha256(),
            "feature_bucket_ms": schema.CADENCE_MS,
            "overlay_schema": prediction_overlay_schema_payload(),
            "prediction_postprocess": PREDICTION_POSTPROCESS_CONTRACT,
            "overlay": {
                "path": OVERLAY_FILENAME,
                "sha256": overlay_sha256,
                "size_bytes": overlay_path.stat().st_size,
                "rows": row_count,
                "compression": "zstd",
            },
            "join_contract": {
                "keys": list(JOIN_COLUMNS),
                "unique": True,
                "canonical_1s_order": True,
                "feature_columns_copied": False,
            },
            "head_count": len(HEADS),
            "test_only": test_only,
            "atomic_admission": True,
            "prediction_values_materialized": True,
            "labels_read": False,
            "economic_outcomes_read": False,
            "training_performed": False,
            "prediction_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        manifest_path = temporary_dir / MANIFEST_FILENAME
        _write_json_fsync(manifest_path, manifest)
        _write_text_fsync(
            temporary_dir / SUCCESS_FILENAME,
            _sha256_file(manifest_path) + "\n",
        )
        _fsync_directory(temporary_dir)
        os.replace(temporary_dir, output_dir)
        _fsync_directory(output_dir.parent)
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise

    return MaterializedPredictionOverlay(
        output_dir=output_dir,
        overlay_path=output_dir / OVERLAY_FILENAME,
        manifest_path=output_dir / MANIFEST_FILENAME,
        cache_identity_sha256=expected_identity,
        row_count=expected_rows,
        reused=False,
        test_only=test_only,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-panel-dir", type=Path, required=True)
    parser.add_argument("--research-bundle-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    args = parser.parse_args()
    result = materialize_daily_prediction_overlay(
        feature_panel_dir=args.feature_panel_dir,
        research_bundle_dir=args.research_bundle_dir,
        output_dir=args.output_dir,
        batch_rows=args.batch_rows,
    )
    print(
        json.dumps(
            {
                "output_dir": str(result.output_dir),
                "overlay_path": str(result.overlay_path),
                "manifest_path": str(result.manifest_path),
                "cache_identity_sha256": result.cache_identity_sha256,
                "row_count": result.row_count,
                "reused": result.reused,
                "test_only": result.test_only,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
