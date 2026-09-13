#!/usr/bin/env python3
"""Memory-bounded LightGBM training for the canonical 1s F03 successor.

The daily feature panel and label overlay remain separate immutable artifacts.
This module builds one reusable float32 matrix cache and then trains all 13
heads with the frozen 52/1/13/66 chronological contract.  It does not read
strategy PnL and its output is research-only until transport and full-path
economic gates are run by their own identities.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import shutil
import time
import uuid
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_overlay_materializer as overlays,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_parity_successor_gate as parity_successor_gate,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training_admission_v3 as admissions,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training_contract as contract,
)

MATRIX_SCHEMA_VERSION = "causal_v12_1s_training_matrix_cache.v2"
TRAINER_SCHEMA_VERSION = "causal_v12_1s_lightgbm_trainer.v2"
BUNDLE_SCHEMA_VERSION = "causal_v12_1s_research_bundle.v2"
EXECUTION_AMENDMENT_SCHEMA_VERSION = "causal_v12_1s_training_execution_amendment.v3"
LIGHTGBM_RUNTIME_ABI_SCHEMA_VERSION = "causal_v12_1s_lightgbm_runtime_abi.v1"
MATRIX_FILENAME = "features.float32.npy"
MATRIX_MANIFEST_FILENAME = "manifest.json"
SUCCESS_FILENAME = "_SUCCESS"
ROWS_PER_DAY = 86_400

AMENDMENT_DECLARED_MISSING_EXECUTION_ARTIFACTS = frozenset(
    {
        "model_output_identity",
        "one_second_feature_panel_manifest",
        "training_implementation_sha256",
    }
)
AMENDMENT_RESOLVED_DESIGN_PRECONDITIONS = (
    "one_second_feature_panel_manifest",
    "training_implementation_sha256",
)

HEAD_SPECS: dict[str, tuple[str, str, str, bool]] = {
    "dir_10s": ("label_dir_10s", "binary", "auc", True),
    "ret_10s": ("label_ret_10s", "regression", "mae", False),
    "vol_10s": ("label_vol_10s", "regression", "mae", False),
    "dir_30s": ("label_dir_30s", "binary", "auc", True),
    "ret_30s": ("label_ret_30s", "regression", "mae", False),
    "vol_30s": ("label_vol_30s", "regression", "mae", False),
    "dir_60s": ("label_dir_60s", "binary", "auc", True),
    "ret_60s": ("label_ret_60s", "regression", "mae", False),
    "vol_60s": ("label_vol_60s", "regression", "mae", False),
    "tox_bid_5s": ("label_tox_bid_5s", "binary", "auc", True),
    "tox_ask_5s": ("label_tox_ask_5s", "binary", "auc", True),
    "tox_bid_10s": ("label_tox_bid_10s", "binary", "auc", True),
    "tox_ask_10s": ("label_tox_ask_10s", "binary", "auc", True),
}


class OneSecondTrainingError(ValueError):
    """Raised when an input or output drifts from the frozen 1s identity."""


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


def _fsync_dir(path: Path) -> None:
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


@dataclass(frozen=True, slots=True)
class TrainingDayArtifact:
    utc_day: str
    feature_panel_dir: Path
    label_overlay_dir: Path
    feature_manifest_path: Path
    feature_panel_path: Path
    overlay_manifest_path: Path
    overlay_path: Path
    feature_manifest_sha256: str
    feature_panel_sha256: str
    overlay_manifest_sha256: str
    overlay_sha256: str
    admission_receipt_path: Path
    admission_receipt_sha256: str
    admission_identity_sha256: str
    pipeline_execution_identity_sha256: str
    parity_gate_identity_sha256: str
    f03_component_semantics_sha256: str

    def feature_identity_payload(self) -> dict[str, Any]:
        return {
            "utc_day": self.utc_day,
            "feature_panel_dir": str(self.feature_panel_dir),
            "feature_manifest_sha256": self.feature_manifest_sha256,
            "feature_panel_sha256": self.feature_panel_sha256,
            "admission_receipt_sha256": self.admission_receipt_sha256,
            "admission_identity_sha256": self.admission_identity_sha256,
        }

    def identity_payload(self) -> dict[str, Any]:
        return {
            "utc_day": self.utc_day,
            "feature_panel_dir": str(self.feature_panel_dir),
            "label_overlay_dir": str(self.label_overlay_dir),
            "feature_manifest_sha256": self.feature_manifest_sha256,
            "feature_panel_sha256": self.feature_panel_sha256,
            "overlay_manifest_sha256": self.overlay_manifest_sha256,
            "overlay_sha256": self.overlay_sha256,
            "admission_receipt_path": str(self.admission_receipt_path),
            "admission_receipt_sha256": self.admission_receipt_sha256,
            "admission_identity_sha256": self.admission_identity_sha256,
            "pipeline_execution_identity_sha256": self.pipeline_execution_identity_sha256,
            "parity_gate_identity_sha256": self.parity_gate_identity_sha256,
            "f03_component_semantics_sha256": self.f03_component_semantics_sha256,
        }


def _load_json_object(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OneSecondTrainingError(f"invalid {role}: {path}") from exc
    if not isinstance(payload, dict):
        raise OneSecondTrainingError(f"{role} must be a JSON object")
    return payload


def _load_admitted_day(
    utc_day: str,
    feature_panel_dir: Path,
    label_overlay_dir: Path,
    admission_receipt_path: Path,
) -> TrainingDayArtifact:
    admission_receipt_path = admission_receipt_path.expanduser().resolve(strict=True)
    admission = admissions.validate_daily_training_admission(admission_receipt_path)
    if admission.get("utc_day") != utc_day:
        raise OneSecondTrainingError(f"daily admission UTC identity mismatch for {utc_day}")
    feature_panel_dir = feature_panel_dir.expanduser().resolve()
    label_overlay_dir = label_overlay_dir.expanduser().resolve()
    if Path(str(admission["feature_manifest"]["path"])).parent != feature_panel_dir:
        raise OneSecondTrainingError(f"daily admission feature directory mismatch for {utc_day}")
    if Path(str(admission["label_manifest"]["path"])).parent != label_overlay_dir:
        raise OneSecondTrainingError(f"daily admission label directory mismatch for {utc_day}")
    feature_manifest_path = feature_panel_dir / panels.MANIFEST_FILENAME
    feature_panel_path = feature_panel_dir / panels.PANEL_FILENAME
    feature_success = feature_panel_dir / panels.SUCCESS_FILENAME
    overlay_manifest_path = label_overlay_dir / overlays.MANIFEST_FILENAME
    overlay_path = label_overlay_dir / overlays.OVERLAY_FILENAME
    overlay_success = label_overlay_dir / overlays.SUCCESS_FILENAME
    for path in (
        feature_manifest_path,
        feature_panel_path,
        feature_success,
        overlay_manifest_path,
        overlay_path,
        overlay_success,
    ):
        if not path.is_file():
            raise OneSecondTrainingError(f"daily training input is not admitted: {path}")

    feature_manifest_sha = _sha256_file(feature_manifest_path)
    overlay_manifest_sha = _sha256_file(overlay_manifest_path)
    if feature_success.read_text(encoding="ascii").strip() != feature_manifest_sha:
        raise OneSecondTrainingError(f"feature _SUCCESS mismatch for {utc_day}")
    if overlay_success.read_text(encoding="ascii").strip() != overlay_manifest_sha:
        raise OneSecondTrainingError(f"overlay _SUCCESS mismatch for {utc_day}")
    feature_manifest = _load_json_object(feature_manifest_path, role="feature manifest")
    overlay_manifest = _load_json_object(overlay_manifest_path, role="overlay manifest")
    if feature_manifest.get("utc_day") != utc_day or overlay_manifest.get("utc_day") != utc_day:
        raise OneSecondTrainingError(f"daily input UTC identity mismatch for {utc_day}")
    if feature_manifest.get("schema_version") not in {
        panels.ARTIFACT_SCHEMA_VERSION,
        "causal_v12_1s_daily_feature_panel_artifact.v3",
    }:
        raise OneSecondTrainingError(f"feature artifact schema mismatch for {utc_day}")
    if overlay_manifest.get("schema_version") not in {
        overlays.ARTIFACT_SCHEMA_VERSION,
        "causal_v12_1s_daily_label_overlay_artifact.v2",
    }:
        raise OneSecondTrainingError(f"overlay artifact schema mismatch for {utc_day}")
    if feature_manifest.get("atomic_admission") is not True:
        raise OneSecondTrainingError(f"feature artifact is not atomically admitted for {utc_day}")
    if overlay_manifest.get("atomic_admission") is not True:
        raise OneSecondTrainingError(f"overlay artifact is not atomically admitted for {utc_day}")
    feature_panel_sha = _sha256_file(feature_panel_path)
    overlay_sha = _sha256_file(overlay_path)
    if feature_manifest.get("panel", {}).get("sha256") != feature_panel_sha:
        raise OneSecondTrainingError(f"feature panel SHA256 mismatch for {utc_day}")
    if overlay_manifest.get("overlay", {}).get("sha256") != overlay_sha:
        raise OneSecondTrainingError(f"label overlay SHA256 mismatch for {utc_day}")
    if overlay_manifest.get("feature_panel_manifest_sha256") != feature_manifest_sha:
        raise OneSecondTrainingError(f"overlay does not bind feature manifest for {utc_day}")
    if overlay_manifest.get("feature_panel_sha256") != feature_panel_sha:
        raise OneSecondTrainingError(f"overlay does not bind feature panel for {utc_day}")
    feature_parquet = pq.ParquetFile(feature_panel_path)
    overlay_parquet = pq.ParquetFile(overlay_path)
    if feature_parquet.metadata.num_rows != ROWS_PER_DAY:
        raise OneSecondTrainingError(f"feature day is not 86,400 rows: {utc_day}")
    if overlay_parquet.metadata.num_rows != ROWS_PER_DAY:
        raise OneSecondTrainingError(f"overlay day is not 86,400 rows: {utc_day}")
    if not feature_parquet.schema_arrow.equals(panels.panel_arrow_schema(), check_metadata=False):
        raise OneSecondTrainingError(f"feature schema mismatch for {utc_day}")
    if not overlay_parquet.schema_arrow.equals(
        overlays.overlay_arrow_schema(), check_metadata=False
    ):
        raise OneSecondTrainingError(f"overlay schema mismatch for {utc_day}")
    feature_join = feature_parquet.read(columns=list(overlays.JOIN_COLUMNS)).to_pandas()
    overlay_join = overlay_parquet.read(columns=list(overlays.JOIN_COLUMNS)).to_pandas()
    if not feature_join.equals(overlay_join):
        raise OneSecondTrainingError(f"feature/overlay join identity mismatch for {utc_day}")
    return TrainingDayArtifact(
        utc_day=utc_day,
        feature_panel_dir=feature_panel_dir,
        label_overlay_dir=label_overlay_dir,
        feature_manifest_path=feature_manifest_path,
        feature_panel_path=feature_panel_path,
        overlay_manifest_path=overlay_manifest_path,
        overlay_path=overlay_path,
        feature_manifest_sha256=feature_manifest_sha,
        feature_panel_sha256=feature_panel_sha,
        overlay_manifest_sha256=overlay_manifest_sha,
        overlay_sha256=overlay_sha,
        admission_receipt_path=admission_receipt_path,
        admission_receipt_sha256=_sha256_file(admission_receipt_path),
        admission_identity_sha256=str(admission["admission_identity_sha256"]),
        pipeline_execution_identity_sha256=str(
            admission["pipeline_execution_receipt"]["execution_identity_sha256"]
        ),
        parity_gate_identity_sha256=str(
            admission["parity_successor_gate"]["parity_gate_identity_sha256"]
        ),
        f03_component_semantics_sha256=str(
            admission["f03_component_semantics_sha256"]
        ),
    )


def load_training_day_manifest(
    path: Path,
    *,
    expected_days: Sequence[str] | None = None,
) -> tuple[TrainingDayArtifact, ...]:
    """Load the exact ordered day-to-artifact mapping used by matrix/training."""

    manifest_path = path.expanduser().resolve()
    payload = _load_json_object(manifest_path, role="training day manifest")
    if payload.get("schema_version") != admissions.TRAINING_DAY_MANIFEST_SCHEMA_VERSION:
        raise OneSecondTrainingError("unsupported training day manifest schema")
    if payload.get("training_input_authorized") is not True:
        raise OneSecondTrainingError("training day manifest is not successor-authorized")
    if payload.get("profile_id") != execution_identity.PROVIDER_PROFILE_ID:
        raise OneSecondTrainingError("training day manifest source profile differs")
    if payload.get("source_permissions") != execution_identity.SOURCE_PERMISSION_CONTRACT:
        raise OneSecondTrainingError("training day manifest source permissions differ")
    for key in (
        "queue_authority",
        "order_lifecycle_authority",
        "fill_path_authority",
        "pnl_authority",
        "economic_outcomes_read",
        "prediction_outcomes_read",
        "training_performed",
        "action_authorized",
        "live_authorized",
    ):
        if payload.get(key) is not False:
            raise OneSecondTrainingError(f"training day manifest must bind {key}=false")
    rows = payload.get("days")
    if not isinstance(rows, list) or not rows:
        raise OneSecondTrainingError("training day manifest has no days")
    observed_days = tuple(str(row.get("utc_day", "")) for row in rows)
    if observed_days != tuple(sorted(set(observed_days))):
        raise OneSecondTrainingError("training day manifest must be sorted and unique")
    if expected_days is not None and observed_days != tuple(expected_days):
        raise OneSecondTrainingError("training day manifest differs from frozen refit days")
    artifacts = tuple(
        _load_admitted_day(
            day,
            Path(str(row["feature_panel_dir"])),
            Path(str(row["label_overlay_dir"])),
            Path(str(row["admission_receipt_path"])),
        )
        for day, row in zip(observed_days, rows, strict=True)
    )
    for row, artifact in zip(rows, artifacts, strict=True):
        if row.get("admission_receipt_sha256") != artifact.admission_receipt_sha256:
            raise OneSecondTrainingError(
                f"day manifest admission receipt hash differs for {artifact.utc_day}"
            )
        if row.get("admission_identity_sha256") != artifact.admission_identity_sha256:
            raise OneSecondTrainingError(
                f"day manifest admission identity differs for {artifact.utc_day}"
            )
    _validate_successor_admission_set(artifacts)
    manifest_pipeline = payload.get("pipeline_execution_receipt", {})
    manifest_gate = payload.get("parity_successor_gate", {})
    if manifest_pipeline.get("execution_identity_sha256") != artifacts[
        0
    ].pipeline_execution_identity_sha256:
        raise OneSecondTrainingError("day manifest pipeline receipt differs from daily admissions")
    if manifest_gate.get("parity_gate_identity_sha256") != artifacts[
        0
    ].parity_gate_identity_sha256:
        raise OneSecondTrainingError("day manifest parity gate differs from daily admissions")
    return artifacts


def _validate_successor_admission_set(
    days: Sequence[TrainingDayArtifact],
) -> dict[str, str]:
    if len(days) != 66:
        raise OneSecondTrainingError("successor training requires exactly 66 admitted days")
    if tuple(day.utc_day for day in days) != tuple(
        sorted({day.utc_day for day in days})
    ):
        raise OneSecondTrainingError("successor daily admissions are not ordered and unique")
    for day in days:
        admission = admissions.validate_daily_training_admission(
            day.admission_receipt_path
        )
        if _sha256_file(day.admission_receipt_path) != day.admission_receipt_sha256:
            raise OneSecondTrainingError(f"daily admission receipt drifted for {day.utc_day}")
        if admission["admission_identity_sha256"] != day.admission_identity_sha256:
            raise OneSecondTrainingError(f"daily admission identity drifted for {day.utc_day}")
    fields = {
        "pipeline_execution_identity_sha256": {
            day.pipeline_execution_identity_sha256 for day in days
        },
        "parity_gate_identity_sha256": {day.parity_gate_identity_sha256 for day in days},
        "f03_component_semantics_sha256": {
            day.f03_component_semantics_sha256 for day in days
        },
    }
    if any(len(values) != 1 for values in fields.values()):
        raise OneSecondTrainingError("66 daily admissions do not share one execution identity")
    return {key: next(iter(values)) for key, values in fields.items()}


def _matrix_identity(days: Sequence[TrainingDayArtifact]) -> dict[str, Any]:
    successor = _validate_successor_admission_set(days)
    return {
        "schema_version": MATRIX_SCHEMA_VERSION,
        "utc_days": [day.utc_day for day in days],
        "daily_feature_artifacts": [day.feature_identity_payload() for day in days],
        "feature_order": list(schema.TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": schema.feature_order_sha256(),
        "dtype": "float32",
        "shape": [len(days) * ROWS_PER_DAY, len(schema.TRAINABLE_FEATURE_ORDER)],
        "row_order": "utc_day_then_canonical_1s_decision",
        "source": "admitted_daily_feature_panels",
        "successor_execution_identity": successor,
        "labels_embedded": False,
    }


def _lightgbm_runtime_abi() -> dict[str, Any]:
    native_name = getattr(lgb.basic._LIB, "_name", None)
    if not native_name:
        raise OneSecondTrainingError("LightGBM runtime native library is unavailable")
    native_path = Path(str(native_name)).expanduser().resolve(strict=True)
    return {
        "schema_version": LIGHTGBM_RUNTIME_ABI_SCHEMA_VERSION,
        "lightgbm_version": str(lgb.__version__),
        "lightgbm_python_module": {
            "path": str(Path(lgb.__file__).resolve(strict=True)),
        },
        "lightgbm_native_library": {
            "path": str(native_path),
            "sha256": _sha256_file(native_path),
        },
        "numpy_version": str(np.__version__),
        "sequence_contract": {
            "class": "MemmapFeatureSequence",
            "storage_dtype": "float32",
            "lightgbm_input_dtype": "float64",
            "indexing": "row_interval_sequence_v1",
        },
    }


def _load_reusable_matrix(output_dir: Path, expected_identity: str) -> dict[str, Any]:
    manifest_path = output_dir / MATRIX_MANIFEST_FILENAME
    matrix_path = output_dir / MATRIX_FILENAME
    success_path = output_dir / SUCCESS_FILENAME
    for path in (manifest_path, matrix_path, success_path):
        if not path.is_file():
            raise OneSecondTrainingError("existing matrix cache is incomplete")
    manifest_sha = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha:
        raise OneSecondTrainingError("matrix cache _SUCCESS mismatch")
    manifest = _load_json_object(manifest_path, role="matrix cache manifest")
    if manifest.get("schema_version") != MATRIX_SCHEMA_VERSION:
        raise OneSecondTrainingError("matrix cache schema version mismatch")
    if manifest.get("atomic_admission") is not True:
        raise OneSecondTrainingError("matrix cache is not atomically admitted")
    identity_payload = manifest.get("cache_identity_payload")
    if not isinstance(identity_payload, dict):
        raise OneSecondTrainingError("matrix cache identity payload is missing")
    reproduced_identity = _canonical_sha256(identity_payload)
    if manifest.get("cache_identity_sha256") != reproduced_identity:
        raise OneSecondTrainingError("matrix cache identity cannot be reproduced")
    if manifest.get("cache_identity_sha256") != expected_identity:
        raise OneSecondTrainingError("existing matrix cache has a different identity")
    if manifest.get("matrix", {}).get("sha256") != _sha256_file(matrix_path):
        raise OneSecondTrainingError("matrix cache SHA256 mismatch")
    observed = np.load(matrix_path, mmap_mode="r")
    if list(observed.shape) != manifest.get("matrix", {}).get("shape"):
        raise OneSecondTrainingError("matrix cache shape mismatch")
    if observed.dtype != np.dtype("float32"):
        raise OneSecondTrainingError("matrix cache dtype mismatch")
    return manifest


def build_training_matrix_cache(
    days: Sequence[TrainingDayArtifact],
    *,
    output_dir: Path,
) -> dict[str, Any]:
    """Build one atomic float32 memmap cache from immutable daily panels."""

    if not days:
        raise OneSecondTrainingError("cannot build an empty training matrix")
    _validate_successor_admission_set(days)
    observed_days = tuple(day.utc_day for day in days)
    if observed_days != tuple(sorted(set(observed_days))):
        raise OneSecondTrainingError("training days must be sorted and unique")
    identity = _matrix_identity(days)
    identity_sha = _canonical_sha256(identity)
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        return _load_reusable_matrix(output_dir, identity_sha)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex}"
    temporary_dir.mkdir()
    matrix_path = temporary_dir / MATRIX_FILENAME
    shape = tuple(identity["shape"])
    try:
        matrix = np.lib.format.open_memmap(
            matrix_path,
            mode="w+",
            dtype=np.float32,
            shape=shape,
        )
        row_start = 0
        feature_columns = list(schema.TRAINABLE_FEATURE_ORDER)
        for day in days:
            if _sha256_file(day.feature_panel_path) != day.feature_panel_sha256:
                raise OneSecondTrainingError(
                    f"feature panel changed after admission: {day.utc_day}"
                )
            parquet = pq.ParquetFile(day.feature_panel_path)
            local_offset = 0
            for row_group in range(parquet.num_row_groups):
                table = parquet.read_row_group(row_group, columns=feature_columns)
                frame = table.to_pandas()
                values = frame.to_numpy(dtype=np.float32, copy=False)
                stop = row_start + local_offset + len(values)
                matrix[row_start + local_offset : stop] = values
                local_offset += len(values)
            if local_offset != ROWS_PER_DAY:
                raise OneSecondTrainingError(
                    f"feature row count changed during matrix build: {day.utc_day}"
                )
            if _sha256_file(day.feature_panel_path) != day.feature_panel_sha256:
                raise OneSecondTrainingError(
                    f"feature panel changed during matrix build: {day.utc_day}"
                )
            row_start += ROWS_PER_DAY
        matrix.flush()
        del matrix
        _fsync_file(matrix_path)
        matrix_sha = _sha256_file(matrix_path)
        manifest = {
            "schema_version": MATRIX_SCHEMA_VERSION,
            "status": "cache_only_not_prediction_or_live_authorized",
            "created_at_utc": datetime.now(tz=UTC).isoformat(),
            "cache_identity_sha256": identity_sha,
            "cache_identity_payload": identity,
            "matrix": {
                "path": MATRIX_FILENAME,
                "sha256": matrix_sha,
                "size_bytes": matrix_path.stat().st_size,
                "shape": list(shape),
                "dtype": "float32",
            },
            "economic_outcomes_read": False,
            "prediction_outcomes_read": False,
            "training_performed": False,
            "action_authorized": False,
            "live_authorized": False,
            "atomic_admission": True,
        }
        manifest_path = temporary_dir / MATRIX_MANIFEST_FILENAME
        _write_json_fsync(manifest_path, manifest)
        _write_text_fsync(
            temporary_dir / SUCCESS_FILENAME,
            _sha256_file(manifest_path) + "\n",
        )
        _fsync_dir(temporary_dir)
        os.replace(temporary_dir, output_dir)
        _fsync_dir(output_dir.parent)
        return manifest
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise


class MemmapFeatureSequence(lgb.Sequence):
    """LightGBM Sequence over one immutable matrix without loading it all."""

    batch_size = 16_384

    def __init__(
        self,
        matrix: np.ndarray,
        *,
        row_start: int,
        row_stop: int,
        batch_size: int = 16_384,
    ) -> None:
        if matrix.ndim != 2 or matrix.dtype != np.dtype("float32"):
            raise OneSecondTrainingError("training matrix must be 2D float32")
        if not 0 <= row_start < row_stop <= len(matrix):
            raise OneSecondTrainingError("invalid sequence row interval")
        self._matrix = matrix
        self._row_start = int(row_start)
        self._row_stop = int(row_stop)
        self.batch_size = int(batch_size)
        if self.batch_size <= 0:
            raise OneSecondTrainingError("sequence batch size must be positive")

    def __len__(self) -> int:
        return self._row_stop - self._row_start

    def __getitem__(self, index: int | slice | list[int]) -> np.ndarray:
        if isinstance(index, (int, np.integer)):
            relative = int(index)
            if relative < 0:
                relative += len(self)
            if not 0 <= relative < len(self):
                raise IndexError(relative)
            return np.asarray(self._matrix[self._row_start + relative], dtype=np.float64)
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            return np.asarray(
                self._matrix[self._row_start + start : self._row_start + stop : step],
                dtype=np.float64,
            )
        if isinstance(index, list):
            indices = np.asarray(index, dtype=np.int64)
            if np.any(indices < 0) or np.any(indices >= len(self)):
                raise IndexError("sequence list index outside interval")
            return np.asarray(self._matrix[self._row_start + indices], dtype=np.float64)
        raise TypeError(f"unsupported Sequence index: {type(index).__name__}")


def _load_head_targets(
    days: Sequence[TrainingDayArtifact],
    head: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if head not in HEAD_SPECS:
        raise OneSecondTrainingError(f"unknown head: {head}")
    label_column = HEAD_SPECS[head][0]
    valid_column = f"label_valid__{head}"
    weight_column = f"sample_weight__{head}"
    total_rows = len(days) * ROWS_PER_DAY
    values = np.empty(total_rows, dtype=np.float32)
    valid = np.empty(total_rows, dtype=bool)
    weights = np.empty(total_rows, dtype=np.float32)
    offset = 0
    for day in days:
        if _sha256_file(day.overlay_path) != day.overlay_sha256:
            raise OneSecondTrainingError(f"label overlay changed after admission: {day.utc_day}")
        table = pq.read_table(
            day.overlay_path,
            columns=[label_column, valid_column, weight_column],
        )
        frame = table.to_pandas()
        stop = offset + len(frame)
        values[offset:stop] = frame[label_column].to_numpy(dtype=np.float32)
        valid[offset:stop] = frame[valid_column].to_numpy(dtype=bool)
        weights[offset:stop] = frame[weight_column].to_numpy(dtype=np.float32)
        offset = stop
        if _sha256_file(day.overlay_path) != day.overlay_sha256:
            raise OneSecondTrainingError(
                f"label overlay changed during target loading: {day.utc_day}"
            )
    if offset != total_rows:
        raise OneSecondTrainingError("head target row count mismatch")
    finite = np.isfinite(values)
    if not np.array_equal(finite, valid):
        raise OneSecondTrainingError(f"label validity mismatch for {head}")
    if np.any(weights[~valid] != 0.0) or np.any(weights[valid] <= 0.0):
        raise OneSecondTrainingError(f"sample-weight validity mismatch for {head}")
    values[~valid] = 0.0
    return values, valid, weights


def _lightgbm_params(*, objective: str, is_classification: bool) -> dict[str, Any]:
    params: dict[str, Any] = {
        "objective": objective,
        "metric": "auc" if is_classification else "mae",
        "verbosity": -1,
        "seed": 42,
        "learning_rate": 0.05,
        "num_leaves": 127,
        "max_depth": 8,
        "min_data_in_leaf": 500,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
        "feature_fraction": 0.8,
        "lambda_l1": 0.1,
        "lambda_l2": 1.0,
        "max_bin": 255,
        "force_col_wise": True,
        "num_threads": max(1, int(os.environ.get("MM_LGB_THREADS", "6"))),
        "histogram_pool_size": float(os.environ.get("MM_LGB_HIST_POOL_MB", "256")),
    }
    if is_classification:
        params["is_unbalance"] = True
    return params


def _metric_value(model: lgb.Booster, metric: str) -> float:
    score = model.best_score.get("selection", {})
    if metric in score:
        return float(score[metric])
    aliases = {"mae": "l1"}
    alias = aliases.get(metric)
    if alias and alias in score:
        return float(score[alias])
    raise OneSecondTrainingError(f"LightGBM did not report selection metric {metric}")


def _execution_amendment_payload(
    days: Sequence[TrainingDayArtifact],
    *,
    day_manifest_path: Path,
    matrix_cache_dir: Path,
    training_design_path: Path,
) -> dict[str, Any]:
    successor_identity = _validate_successor_admission_set(days)
    implementation_path = Path(__file__).resolve()
    day_manifest_path = day_manifest_path.expanduser().resolve(strict=True)
    matrix_cache_dir = matrix_cache_dir.expanduser().resolve(strict=True)
    matrix_manifest_path = matrix_cache_dir / MATRIX_MANIFEST_FILENAME
    training_design_path = training_design_path.expanduser().resolve(strict=True)
    training_audit = contract.load_and_validate_training_design(training_design_path)
    missing_execution_artifacts = frozenset(
        str(name) for name in training_audit.get("missing_execution_artifacts", ())
    )
    if missing_execution_artifacts != AMENDMENT_DECLARED_MISSING_EXECUTION_ARTIFACTS:
        unexpected = sorted(
            missing_execution_artifacts - AMENDMENT_DECLARED_MISSING_EXECUTION_ARTIFACTS
        )
        absent = sorted(
            AMENDMENT_DECLARED_MISSING_EXECUTION_ARTIFACTS - missing_execution_artifacts
        )
        raise OneSecondTrainingError(
            "unresolved training design blockers do not match execution amendment "
            f"scope; unexpected={unexpected}, absent={absent}"
        )
    _validate_day_manifest_semantics(day_manifest_path, days)
    matrix_manifest = _load_json_object(
        matrix_manifest_path,
        role="matrix cache manifest",
    )
    _load_reusable_matrix(
        matrix_cache_dir,
        str(matrix_manifest.get("cache_identity_sha256", "")),
    )
    if matrix_manifest.get("cache_identity_payload") != _matrix_identity(days):
        raise OneSecondTrainingError(
            "matrix cache identity differs from the amendment daily inputs"
        )
    first_admission = admissions.validate_daily_training_admission(
        days[0].admission_receipt_path
    )
    pipeline_path = Path(
        str(first_admission["pipeline_execution_receipt"]["path"])
    ).resolve(strict=True)
    gate_path = Path(str(first_admission["parity_successor_gate"]["path"])).resolve(
        strict=True
    )
    pipeline_receipt = execution_identity.validate_pipeline_execution_receipt(
        pipeline_path,
        require_materialization_workspace_stability=False,
    )
    gate = parity_successor_gate.validate_training_parity_gate(gate_path)
    return {
        "schema_version": EXECUTION_AMENDMENT_SCHEMA_VERSION,
        "identity": contract.IDENTITY,
        "status": "training_inputs_bound_model_output_is_postcondition",
        "training_design": {
            "path": str(training_design_path),
            "sha256": _sha256_file(training_design_path),
        },
        "training_implementation": {
            "path": str(implementation_path),
            "sha256": _sha256_file(implementation_path),
        },
        "training_day_manifest": {
            "path": str(day_manifest_path),
            "sha256": _sha256_file(day_manifest_path),
        },
        "matrix_cache_manifest": {
            "path": str(matrix_manifest_path),
            "sha256": _sha256_file(matrix_manifest_path),
            "cache_identity_sha256": matrix_manifest["cache_identity_sha256"],
        },
        "successor_training_gate": {
            "daily_admission_schema_version": admissions.SCHEMA_VERSION,
            "admitted_day_count": len(days),
            "source_profile_id": execution_identity.PROVIDER_PROFILE_ID,
            "source_permissions": execution_identity.SOURCE_PERMISSION_CONTRACT,
            "execution_identities": successor_identity,
            "pipeline_execution_receipt": {
                **execution_identity.file_identity(pipeline_path),
                "execution_identity_sha256": pipeline_receipt[
                    "execution_identity_sha256"
                ],
            },
            "parity_successor_gate": {
                **execution_identity.file_identity(gate_path),
                "parity_gate_identity_sha256": gate["parity_gate_identity_sha256"],
                "training_authorized": gate["training_authorized"],
            },
            "native_build_receipt": pipeline_receipt["native_build_receipt"],
            "f03_component_semantics": pipeline_receipt[
                "f03_component_semantics"
            ],
            "quote_config": pipeline_receipt["quote_config"],
            "p3_v2_artifact": pipeline_receipt["p3_v2_artifact"],
            "old_synthetic_parity_json_training_authority": False,
        },
        "declared_missing_execution_artifacts": sorted(
            AMENDMENT_DECLARED_MISSING_EXECUTION_ARTIFACTS
        ),
        "daily_artifacts": [day.identity_payload() for day in days],
        "resolved_design_preconditions": list(AMENDMENT_RESOLVED_DESIGN_PRECONDITIONS),
        "model_output_identity_role": "atomic_training_postcondition",
        "lightgbm_runtime_abi": _lightgbm_runtime_abi(),
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
        "model_training_executed": False,
        "queue_authority": False,
        "order_lifecycle_authority": False,
        "fill_path_authority": False,
        "pnl_authority": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _validate_day_manifest_semantics(
    day_manifest_path: Path,
    days: Sequence[TrainingDayArtifact],
) -> None:
    payload = _load_json_object(day_manifest_path, role="training day manifest")
    if payload.get("schema_version") != admissions.TRAINING_DAY_MANIFEST_SCHEMA_VERSION:
        raise OneSecondTrainingError("day manifest schema differs from daily artifacts")
    rows = payload.get("days")
    if not isinstance(rows, list):
        raise OneSecondTrainingError("day manifest semantics differ from daily artifacts")
    try:
        observed = [
            {
                "utc_day": str(row["utc_day"]),
                "feature_panel_dir": str(
                    Path(str(row["feature_panel_dir"])).expanduser().resolve()
                ),
                "label_overlay_dir": str(
                    Path(str(row["label_overlay_dir"])).expanduser().resolve()
                ),
                "admission_receipt_path": str(
                    Path(str(row["admission_receipt_path"])).expanduser().resolve()
                ),
                "admission_identity_sha256": str(row["admission_identity_sha256"]),
            }
            for row in rows
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise OneSecondTrainingError("day manifest semantics differ from daily artifacts") from exc
    expected = [
        {
            "utc_day": day.utc_day,
            "feature_panel_dir": str(day.feature_panel_dir),
            "label_overlay_dir": str(day.label_overlay_dir),
            "admission_receipt_path": str(day.admission_receipt_path),
            "admission_identity_sha256": day.admission_identity_sha256,
        }
        for day in days
    ]
    if observed != expected:
        raise OneSecondTrainingError("day manifest semantics differ from daily artifacts")


def freeze_training_execution_amendment(
    days: Sequence[TrainingDayArtifact],
    *,
    day_manifest_path: Path,
    matrix_cache_dir: Path,
    training_design_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    """Freeze all training inputs before any model or prediction outcome exists."""

    payload = _execution_amendment_payload(
        days,
        day_manifest_path=day_manifest_path,
        matrix_cache_dir=matrix_cache_dir,
        training_design_path=training_design_path,
    )
    payload["execution_identity_sha256"] = _canonical_sha256(payload)
    output_path = output_path.expanduser().resolve()
    if output_path.exists():
        existing = _load_json_object(output_path, role="training execution amendment")
        if existing != payload:
            raise FileExistsError(
                f"refusing to replace a different execution amendment: {output_path}"
            )
        return existing
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.parent / f".{output_path.name}.tmp-{uuid.uuid4().hex}"
    try:
        _write_json_fsync(temporary, payload)
        os.replace(temporary, output_path)
        _fsync_dir(output_path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return payload


def validate_training_execution_amendment(
    path: Path | None,
    days: Sequence[TrainingDayArtifact],
    *,
    matrix_cache_dir: Path,
    training_design_path: Path,
) -> dict[str, Any]:
    if path is None:
        raise OneSecondTrainingError(
            "training is not eligible without a frozen execution amendment"
        )
    amendment_path = path.expanduser().resolve(strict=True)
    observed = _load_json_object(amendment_path, role="training execution amendment")
    execution_sha = observed.pop("execution_identity_sha256", None)
    observed_runtime_abi = observed.get("lightgbm_runtime_abi")
    current_runtime_abi = _lightgbm_runtime_abi()
    if observed_runtime_abi != current_runtime_abi:
        observed["execution_identity_sha256"] = execution_sha
        raise OneSecondTrainingError("LightGBM runtime ABI drifted from frozen amendment")
    try:
        expected = _execution_amendment_payload(
            days,
            day_manifest_path=Path(str(observed.get("training_day_manifest", {}).get("path", ""))),
            matrix_cache_dir=matrix_cache_dir,
            training_design_path=training_design_path,
        )
    finally:
        if execution_sha is not None:
            observed["execution_identity_sha256"] = execution_sha
    if execution_sha != _canonical_sha256(expected):
        raise OneSecondTrainingError("training execution amendment hash mismatch")
    expected_with_sha = dict(expected)
    expected_with_sha["execution_identity_sha256"] = execution_sha
    if observed != expected_with_sha:
        raise OneSecondTrainingError("training execution amendment identity drifted")
    return observed


def _trainer_identity(
    days: Sequence[TrainingDayArtifact],
    matrix_manifest: Mapping[str, Any],
    training_audit: Mapping[str, Any],
    execution_amendment: Mapping[str, Any],
) -> dict[str, Any]:
    implementation_path = Path(__file__).resolve()
    contract_path = Path(contract.__file__).resolve()
    return {
        "schema_version": TRAINER_SCHEMA_VERSION,
        "identity": contract.IDENTITY,
        "implementation": {
            "path": str(implementation_path),
            "sha256": _sha256_file(implementation_path),
        },
        "training_contract_implementation": {
            "path": str(contract_path),
            "sha256": _sha256_file(contract_path),
        },
        "training_contract_audit": dict(training_audit),
        "execution_amendment": dict(execution_amendment),
        "lightgbm_runtime_abi": _lightgbm_runtime_abi(),
        "matrix_cache_identity_sha256": matrix_manifest["cache_identity_sha256"],
        "daily_artifacts": [day.identity_payload() for day in days],
        "heads": list(HEAD_SPECS),
        "feature_order": list(schema.TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": schema.feature_order_sha256(),
        "inference_cadence_ms": 1_000,
        "fixed_hyperparameters": _lightgbm_params(objective="regression", is_classification=False)
        | {"classification_addition": {"is_unbalance": True}},
        "selection": "52_fit_1_embargo_13_selection_then_66_day_refit",
        "economic_outcomes_read": False,
        "external_2026_panels_read": False,
    }


def train_research_bundle(
    days: Sequence[TrainingDayArtifact],
    *,
    matrix_cache_dir: Path,
    output_dir: Path,
    training_design_path: Path = contract.DEFAULT_DESIGN_PATH,
    execution_amendment_path: Path | None = None,
) -> dict[str, Any]:
    """Train all 13 heads and atomically publish a research-only bundle."""

    _validate_successor_admission_set(days)
    training_audit = contract.load_and_validate_training_design(training_design_path)
    expected_days = tuple(training_audit["refit_days"])
    if tuple(day.utc_day for day in days) != expected_days:
        raise OneSecondTrainingError("training inputs differ from frozen 66 refit days")
    matrix_cache_dir = matrix_cache_dir.expanduser().resolve()
    amendment = validate_training_execution_amendment(
        execution_amendment_path,
        days,
        matrix_cache_dir=matrix_cache_dir,
        training_design_path=training_design_path,
    )
    amendment_path = execution_amendment_path.expanduser().resolve(strict=True)
    amendment_binding = {
        "path": str(amendment_path),
        "sha256": _sha256_file(amendment_path),
        "execution_identity_sha256": amendment["execution_identity_sha256"],
    }
    matrix_manifest = _load_json_object(
        matrix_cache_dir / MATRIX_MANIFEST_FILENAME,
        role="matrix cache manifest",
    )
    if matrix_manifest.get("cache_identity_payload") != _matrix_identity(days):
        raise OneSecondTrainingError("matrix cache does not bind the requested daily inputs")
    _load_reusable_matrix(
        matrix_cache_dir,
        str(matrix_manifest.get("cache_identity_sha256", "")),
    )
    matrix = np.load(matrix_cache_dir / MATRIX_FILENAME, mmap_mode="r")
    total_rows = len(days) * ROWS_PER_DAY
    if matrix.shape != (total_rows, len(schema.TRAINABLE_FEATURE_ORDER)):
        raise OneSecondTrainingError("matrix cache shape drifted")

    fit_rows = len(training_audit["fit_days"]) * ROWS_PER_DAY
    selection_start = (
        len(training_audit["fit_days"]) + len(training_audit["embargo_days"])
    ) * ROWS_PER_DAY
    selection_stop = selection_start + len(training_audit["selection_days"]) * ROWS_PER_DAY
    if amendment.get("lightgbm_runtime_abi") != _lightgbm_runtime_abi():
        raise OneSecondTrainingError("LightGBM runtime ABI drifted before training")
    identity = _trainer_identity(
        days,
        matrix_manifest,
        training_audit,
        amendment_binding,
    )
    identity_sha = _canonical_sha256(identity)
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise FileExistsError(f"research bundle output already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex}"
    temporary_dir.mkdir()
    started = time.perf_counter()
    head_metadata: dict[str, Any] = OrderedDict()
    try:
        for head, (label_column, objective, metric, is_classification) in HEAD_SPECS.items():
            targets, valid, weights = _load_head_targets(days, head)
            fit_sequence = MemmapFeatureSequence(matrix, row_start=0, row_stop=fit_rows)
            selection_sequence = MemmapFeatureSequence(
                matrix,
                row_start=selection_start,
                row_stop=selection_stop,
            )
            feature_names = list(schema.TRAINABLE_FEATURE_ORDER)
            params = _lightgbm_params(
                objective=objective,
                is_classification=is_classification,
            )
            train_data = lgb.Dataset(
                fit_sequence,
                label=targets[:fit_rows],
                weight=weights[:fit_rows],
                feature_name=feature_names,
                free_raw_data=True,
            )
            selection_data = lgb.Dataset(
                selection_sequence,
                label=targets[selection_start:selection_stop],
                weight=weights[selection_start:selection_stop],
                feature_name=feature_names,
                reference=train_data,
                free_raw_data=True,
            )
            head_started = time.perf_counter()
            selected = lgb.train(
                params,
                train_data,
                num_boost_round=2_000,
                valid_sets=[selection_data],
                valid_names=["selection"],
                callbacks=[lgb.early_stopping(100, verbose=False)],
            )
            best_iteration = max(1, int(selected.best_iteration or 2_000))
            selection_score = _metric_value(selected, metric)
            del selection_data, train_data, selected
            gc.collect()
            refit_sequence = MemmapFeatureSequence(
                matrix,
                row_start=0,
                row_stop=total_rows,
            )
            refit_data = lgb.Dataset(
                refit_sequence,
                label=targets,
                weight=weights,
                feature_name=feature_names,
                free_raw_data=True,
            )
            final = lgb.train(
                params,
                refit_data,
                num_boost_round=best_iteration,
            )
            model_path = temporary_dir / f"{head}.txt"
            final.save_model(str(model_path))
            _fsync_file(model_path)
            metadata = {
                "schema_version": "causal_v12_1s_head_meta.v1",
                "name": head,
                "label_col": label_column,
                "objective": objective,
                "metric": metric,
                "selection_metric": selection_score,
                "best_iteration": best_iteration,
                "fit_rows": fit_rows,
                "fit_valid_rows": int(valid[:fit_rows].sum()),
                "selection_rows": selection_stop - selection_start,
                "selection_valid_rows": int(valid[selection_start:selection_stop].sum()),
                "refit_rows": total_rows,
                "refit_valid_rows": int(valid.sum()),
                "feature_cols": feature_names,
                "feature_count": len(feature_names),
                "feature_order_sha256": schema.feature_order_sha256(),
                "feature_bucket_ms": 1_000,
                "feature_timestamp_semantics": "canonical_1s_decision_ready_at_boundary",
                "model_sha256": _sha256_file(model_path),
                "training_seconds": time.perf_counter() - head_started,
                "research_only": True,
                "prediction_authority": False,
                "action_authority": False,
                "live_authority": False,
            }
            metadata_path = temporary_dir / f"{head}_meta.json"
            _write_json_fsync(metadata_path, metadata)
            head_metadata[head] = {
                "model": {"path": model_path.name, "sha256": metadata["model_sha256"]},
                "metadata": {
                    "path": metadata_path.name,
                    "sha256": _sha256_file(metadata_path),
                },
                "selection_metric": selection_score,
                "best_iteration": best_iteration,
            }
            del targets, valid, weights, refit_data, final
            gc.collect()

        bundle = {
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "identity": contract.IDENTITY,
            "status": "research_only_transport_and_economics_not_run",
            "created_at_utc": datetime.now(tz=UTC).isoformat(),
            "training_identity_sha256": identity_sha,
            "training_identity": identity,
            "execution_amendment": amendment_binding,
            "matrix_cache": {
                "path": str(matrix_cache_dir),
                "manifest_sha256": _sha256_file(matrix_cache_dir / MATRIX_MANIFEST_FILENAME),
                "cache_identity_sha256": matrix_manifest["cache_identity_sha256"],
            },
            "heads": head_metadata,
            "head_count": len(head_metadata),
            "training_seconds": time.perf_counter() - started,
            "prediction_outcomes_read": False,
            "economic_outcomes_read": False,
            "native_transport_run": False,
            "full_path_ml_ab_run": False,
            "prediction_authority": False,
            "action_authority": False,
            "live_authority": False,
            "atomic_admission": True,
        }
        bundle_path = temporary_dir / "bundle_meta.json"
        _write_json_fsync(bundle_path, bundle)
        _write_text_fsync(
            temporary_dir / SUCCESS_FILENAME,
            _sha256_file(bundle_path) + "\n",
        )
        _fsync_dir(temporary_dir)
        os.replace(temporary_dir, output_dir)
        _fsync_dir(output_dir.parent)
        return bundle
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build-matrix")
    build.add_argument("--day-manifest", type=Path, required=True)
    build.add_argument("--output-dir", type=Path, required=True)
    build.add_argument("--training-design", type=Path, default=contract.DEFAULT_DESIGN_PATH)

    amendment = subparsers.add_parser("freeze-amendment")
    amendment.add_argument("--day-manifest", type=Path, required=True)
    amendment.add_argument("--matrix-cache-dir", type=Path, required=True)
    amendment.add_argument("--output", type=Path, required=True)
    amendment.add_argument("--training-design", type=Path, default=contract.DEFAULT_DESIGN_PATH)

    train = subparsers.add_parser("train")
    train.add_argument("--day-manifest", type=Path, required=True)
    train.add_argument("--matrix-cache-dir", type=Path, required=True)
    train.add_argument("--output-dir", type=Path, required=True)
    train.add_argument("--training-design", type=Path, default=contract.DEFAULT_DESIGN_PATH)
    train.add_argument("--execution-amendment", type=Path, required=True)

    args = parser.parse_args()
    audit = contract.load_and_validate_training_design(args.training_design)
    days = load_training_day_manifest(
        args.day_manifest,
        expected_days=audit["refit_days"],
    )
    if args.command == "build-matrix":
        result = build_training_matrix_cache(days, output_dir=args.output_dir)
    elif args.command == "freeze-amendment":
        result = freeze_training_execution_amendment(
            days,
            day_manifest_path=args.day_manifest,
            matrix_cache_dir=args.matrix_cache_dir,
            training_design_path=args.training_design,
            output_path=args.output,
        )
    else:
        result = train_research_bundle(
            days,
            matrix_cache_dir=args.matrix_cache_dir,
            output_dir=args.output_dir,
            training_design_path=args.training_design,
            execution_amendment_path=args.execution_amendment,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
