#!/usr/bin/env python3
"""Atomically persist one F03 daily 1s label overlay without feature copies."""

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

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as sources,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_generator as labels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)

SCHEMA_VERSION = "causal_v12_1s_label_overlay_materializer.v3"
ARTIFACT_SCHEMA_VERSION = "causal_v12_1s_daily_label_overlay_artifact.v3"
OVERLAY_FILENAME = "label_overlay.parquet"
MANIFEST_FILENAME = "manifest.json"
SUCCESS_FILENAME = "_SUCCESS"
AUTHORITATIVE_DAILY_ROWS = 86_400
JOIN_COLUMNS = (
    "cutoff_exclusive_ms",
    "decision_ts_ms",
    "feature_ready_ts_ms",
    "feature_row_fingerprint_sha256",
)
HEADS = tuple(labels.LABEL_COLUMN_BY_HEAD)


class LabelOverlayMaterializationError(ValueError):
    """Raised when an input or output violates the successor overlay contract."""


@dataclass(frozen=True, slots=True)
class AdmittedFeaturePanel:
    output_dir: Path
    panel_path: Path
    manifest_path: Path
    manifest: dict[str, Any]
    manifest_sha256: str
    panel_sha256: str


@dataclass(frozen=True, slots=True)
class MaterializedLabelOverlay:
    output_dir: Path
    overlay_path: Path
    manifest_path: Path
    cache_identity_sha256: str
    row_count: int
    reused: bool


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
        for chunk in iter(lambda: handle.read(1 << 20), b""):
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


def overlay_arrow_schema() -> pa.Schema:
    fields = [
        pa.field("cutoff_exclusive_ms", pa.int64(), nullable=False),
        pa.field("decision_ts_ms", pa.int64(), nullable=False),
        pa.field("feature_ready_ts_ms", pa.int64(), nullable=False),
        pa.field("feature_row_fingerprint_sha256", pa.string(), nullable=False),
    ]
    for head in HEADS:
        fields.extend(
            (
                pa.field(labels.LABEL_COLUMN_BY_HEAD[head], pa.float64(), nullable=True),
                pa.field(f"label_valid__{head}", pa.bool_(), nullable=False),
                pa.field(f"sample_weight__{head}", pa.float64(), nullable=False),
                pa.field(f"overlap_uniqueness__{head}", pa.float64(), nullable=False),
            )
        )
    return pa.schema(fields)


def overlay_schema_payload() -> dict[str, Any]:
    arrow_schema = overlay_arrow_schema()
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "columns": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow_schema
        ],
        "join_columns": list(JOIN_COLUMNS),
        "head_order": list(HEADS),
        "head_count": len(HEADS),
        "feature_columns_copied": False,
        "prediction_columns_allowed": False,
        "economic_outcome_columns_allowed": False,
    }


def _load_json_object(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LabelOverlayMaterializationError(f"invalid {role} JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise LabelOverlayMaterializationError(f"{role} must be a JSON object")
    return payload


def load_admitted_feature_panel(feature_panel_dir: Path) -> AdmittedFeaturePanel:
    output_dir = feature_panel_dir.expanduser().resolve()
    manifest_path = output_dir / panels.MANIFEST_FILENAME
    panel_path = output_dir / panels.PANEL_FILENAME
    success_path = output_dir / panels.SUCCESS_FILENAME
    for path in (manifest_path, panel_path, success_path):
        if not path.is_file():
            raise LabelOverlayMaterializationError(
                f"feature panel is not atomically admitted; missing {path.name}"
            )

    manifest_sha256 = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha256:
        raise LabelOverlayMaterializationError(
            "feature panel _SUCCESS does not bind its manifest SHA256"
        )
    manifest = _load_json_object(manifest_path, role="feature panel manifest")
    if manifest.get("schema_version") != panels.ARTIFACT_SCHEMA_VERSION:
        raise LabelOverlayMaterializationError("feature panel artifact schema mismatch")
    if manifest.get("atomic_admission") is not True:
        raise LabelOverlayMaterializationError("feature panel lacks atomic admission authority")
    if manifest.get("panel_schema") != panels.panel_schema_payload():
        raise LabelOverlayMaterializationError("feature panel schema payload mismatch")
    cache_payload = manifest.get("cache_identity_payload")
    if not isinstance(cache_payload, dict):
        raise LabelOverlayMaterializationError(
            "feature panel manifest lacks cache identity payload"
        )
    cache_identity = manifest.get("cache_identity_sha256")
    if cache_identity != _canonical_sha256(cache_payload):
        raise LabelOverlayMaterializationError(
            "feature panel cache identity cannot be reproduced from its payload"
        )
    panel_entry = manifest.get("panel")
    if not isinstance(panel_entry, dict):
        raise LabelOverlayMaterializationError("feature panel manifest lacks panel identity")
    if panel_entry.get("path") != panels.PANEL_FILENAME:
        raise LabelOverlayMaterializationError("feature panel manifest path mismatch")
    panel_sha256 = _sha256_file(panel_path)
    if panel_entry.get("sha256") != panel_sha256:
        raise LabelOverlayMaterializationError("feature panel SHA256 mismatch")
    metadata = pq.ParquetFile(panel_path).metadata
    if metadata.num_rows != AUTHORITATIVE_DAILY_ROWS:
        raise LabelOverlayMaterializationError(
            "formal label overlay requires an admitted 86,400-row feature panel"
        )
    if panel_entry.get("rows") != AUTHORITATIVE_DAILY_ROWS:
        raise LabelOverlayMaterializationError(
            "feature panel manifest does not declare exactly 86,400 rows"
        )
    observed_schema = pq.ParquetFile(panel_path).schema_arrow
    if not observed_schema.equals(panels.panel_arrow_schema(), check_metadata=False):
        raise LabelOverlayMaterializationError("feature panel Parquet schema mismatch")
    return AdmittedFeaturePanel(
        output_dir=output_dir,
        panel_path=panel_path,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        panel_sha256=panel_sha256,
    )


def _validate_bundle_binding(
    admitted: AdmittedFeaturePanel,
    bundle: sources.DailySourceBundle,
) -> None:
    manifest = admitted.manifest
    if manifest.get("utc_day") != bundle.utc_day:
        raise LabelOverlayMaterializationError(
            "feature panel UTC day differs from DailySourceBundle"
        )
    if manifest.get("source_bundle") != bundle.identity_payload():
        raise LabelOverlayMaterializationError(
            "feature panel source bundle identity differs from DailySourceBundle"
        )
    cache_payload = manifest["cache_identity_payload"]
    if cache_payload.get("bundle_identity_sha256") != bundle.identity_sha256():
        raise LabelOverlayMaterializationError(
            "feature panel cache identity does not bind the DailySourceBundle"
        )


def _label_bars(bundle: sources.DailySourceBundle) -> pd.DataFrame:
    audit = sources.read_local_trade_bars_with_audit(bundle.local_trade_tempo_paths)
    day_start = int(pd.Timestamp(bundle.utc_day, tz="UTC").timestamp() * 1_000)
    day_end = day_start + 86_400_000
    rows = [bar for bar in audit.bars if day_start <= int(bar.start_ts_ms) < day_end]
    if len(rows) != AUTHORITATIVE_DAILY_ROWS:
        raise LabelOverlayMaterializationError(
            "DailySourceBundle must provide exactly 86,400 dense target-day 1s label bars"
        )
    timestamps = np.fromiter(
        (int(bar.start_ts_ms) for bar in rows),
        dtype=np.int64,
        count=len(rows),
    )
    expected = np.arange(day_start, day_end, 1_000, dtype=np.int64)
    if not np.array_equal(timestamps, expected):
        raise LabelOverlayMaterializationError(
            "DailySourceBundle target-day label bars are not a complete canonical grid"
        )
    return pd.DataFrame(
        {
            "close": np.fromiter((float(bar.close) for bar in rows), dtype=np.float64),
            "high": np.fromiter((float(bar.high) for bar in rows), dtype=np.float64),
            "low": np.fromiter((float(bar.low) for bar in rows), dtype=np.float64),
        },
        index=pd.to_datetime(timestamps, unit="ms", utc=True),
    )


def _identity_payload(
    admitted: AdmittedFeaturePanel,
    bundle: sources.DailySourceBundle,
    *,
    quote_config_path: Path,
    p3_v2_artifact_path: Path,
    symbol: str,
) -> dict[str, Any]:
    generator_path = Path(labels.__file__).resolve()
    materializer_path = Path(__file__).resolve()
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_schema_sha256": _canonical_sha256(overlay_schema_payload()),
        "utc_day": bundle.utc_day,
        "symbol": symbol,
        "feature_panel": {
            "manifest_path": str(admitted.manifest_path),
            "manifest_sha256": admitted.manifest_sha256,
            "panel_path": str(admitted.panel_path),
            "panel_sha256": admitted.panel_sha256,
            "cache_identity_sha256": admitted.manifest.get("cache_identity_sha256"),
            "schema_sha256": _canonical_sha256(admitted.manifest["panel_schema"]),
            "rows": AUTHORITATIVE_DAILY_ROWS,
        },
        "source_bundle_identity_sha256": bundle.identity_sha256(),
        "source_bundle_identity": bundle.identity_payload(),
        "label_generator": {
            "path": str(generator_path),
            "sha256": _sha256_file(generator_path),
            "schema_version": labels.SCHEMA_VERSION,
            "identity": labels.IDENTITY,
        },
        "materializer": {
            "path": str(materializer_path),
            "sha256": _sha256_file(materializer_path),
        },
        "label_quote_config": {
            "path": str(quote_config_path),
            "sha256": _sha256_file(quote_config_path),
        },
        "p3_v2_artifact": {
            "path": str(p3_v2_artifact_path),
            "sha256": _sha256_file(p3_v2_artifact_path),
        },
        "output_schema": overlay_schema_payload(),
        "formal_daily_rows": AUTHORITATIVE_DAILY_ROWS,
        "in_memory_join_only_predecessor_superseded": True,
    }


def cache_identity_sha256(
    admitted: AdmittedFeaturePanel,
    bundle: sources.DailySourceBundle,
    *,
    quote_config_path: Path,
    p3_v2_artifact_path: Path,
    symbol: str,
) -> str:
    return _canonical_sha256(
        _identity_payload(
            admitted,
            bundle,
            quote_config_path=quote_config_path,
            p3_v2_artifact_path=p3_v2_artifact_path,
            symbol=symbol,
        )
    )


def _validate_overlay_frame(frame: pd.DataFrame) -> None:
    expected_columns = overlay_arrow_schema().names
    if list(frame.columns) != expected_columns:
        raise LabelOverlayMaterializationError(
            "label overlay columns differ from the exact successor schema"
        )
    if len(frame) != AUTHORITATIVE_DAILY_ROWS:
        raise LabelOverlayMaterializationError(
            "formal label overlay requires exactly 86,400 output rows"
        )
    if frame[list(JOIN_COLUMNS)].isna().any().any():
        raise LabelOverlayMaterializationError("label overlay join keys contain nulls")
    if frame.duplicated(list(JOIN_COLUMNS)).any():
        raise LabelOverlayMaterializationError("label overlay join keys are not unique")
    fingerprints = frame["feature_row_fingerprint_sha256"].astype(str)
    if not bool(fingerprints.str.fullmatch(r"[0-9a-f]{64}").all()):
        raise LabelOverlayMaterializationError("label overlay has invalid row fingerprints")
    for head in HEADS:
        label_values = frame[labels.LABEL_COLUMN_BY_HEAD[head]].to_numpy(dtype=np.float64)
        valid = frame[f"label_valid__{head}"].to_numpy(dtype=bool)
        weights = frame[f"sample_weight__{head}"].to_numpy(dtype=np.float64)
        uniqueness = frame[f"overlap_uniqueness__{head}"].to_numpy(dtype=np.float64)
        if not np.array_equal(np.isfinite(label_values), valid):
            raise LabelOverlayMaterializationError(
                f"label_valid disagrees with finite labels for {head}"
            )
        if np.any(~np.isfinite(weights)) or np.any(~np.isfinite(uniqueness)):
            raise LabelOverlayMaterializationError(f"non-finite weights for {head}")
        if np.any(weights[~valid] != 0.0) or np.any(uniqueness[~valid] != 0.0):
            raise LabelOverlayMaterializationError(
                f"invalid rows must have zero weight and uniqueness for {head}"
            )
        if np.any(weights[valid] <= 0.0):
            raise LabelOverlayMaterializationError(f"valid rows require positive weight for {head}")
        if np.any((uniqueness[valid] <= 0.0) | (uniqueness[valid] > 1.0)):
            raise LabelOverlayMaterializationError(
                f"valid overlap uniqueness is outside (0,1] for {head}"
            )


def _load_reusable(
    output_dir: Path,
    *,
    expected_identity: str,
    expected_payload: dict[str, Any],
) -> MaterializedLabelOverlay:
    manifest_path = output_dir / MANIFEST_FILENAME
    overlay_path = output_dir / OVERLAY_FILENAME
    success_path = output_dir / SUCCESS_FILENAME
    for path in (manifest_path, overlay_path, success_path):
        if not path.is_file():
            raise LabelOverlayMaterializationError(
                "existing output directory is not an admitted label overlay"
            )
    manifest_sha256 = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha256:
        raise LabelOverlayMaterializationError("existing overlay _SUCCESS is invalid")
    manifest = _load_json_object(manifest_path, role="label overlay manifest")
    if manifest.get("schema_version") != ARTIFACT_SCHEMA_VERSION:
        raise LabelOverlayMaterializationError("existing label overlay artifact mismatch")
    if manifest.get("cache_identity_sha256") != expected_identity:
        raise LabelOverlayMaterializationError(
            "existing label overlay is not hash-compatible with requested inputs"
        )
    if manifest.get("cache_identity_payload") != expected_payload:
        raise LabelOverlayMaterializationError(
            "existing label overlay identity payload differs from requested inputs"
        )
    if _canonical_sha256(manifest["cache_identity_payload"]) != expected_identity:
        raise LabelOverlayMaterializationError(
            "existing label overlay cache identity cannot be reproduced"
        )
    if manifest.get("overlay_schema") != overlay_schema_payload():
        raise LabelOverlayMaterializationError("existing label overlay schema payload mismatch")
    if manifest.get("atomic_admission") is not True:
        raise LabelOverlayMaterializationError("existing label overlay is not atomically admitted")
    required_false = (
        "predictions_read",
        "economic_outcomes_read",
        "training_performed",
        "training_authorized",
        "action_authorized",
        "live_authorized",
    )
    if any(manifest.get(field) is not False for field in required_false):
        raise LabelOverlayMaterializationError(
            "existing label overlay violates its permission boundary"
        )
    overlay = manifest.get("overlay")
    if (
        not isinstance(overlay, dict)
        or overlay.get("path") != OVERLAY_FILENAME
        or overlay.get("compression") != "zstd"
        or overlay.get("sha256") != _sha256_file(overlay_path)
    ):
        raise LabelOverlayMaterializationError("existing label overlay SHA256 mismatch")
    if overlay.get("rows") != AUTHORITATIVE_DAILY_ROWS:
        raise LabelOverlayMaterializationError("existing label overlay row count mismatch")
    parquet = pq.ParquetFile(overlay_path)
    if parquet.metadata.num_rows != AUTHORITATIVE_DAILY_ROWS:
        raise LabelOverlayMaterializationError("existing label overlay Parquet is incomplete")
    if not parquet.schema_arrow.equals(overlay_arrow_schema(), check_metadata=False):
        raise LabelOverlayMaterializationError("existing label overlay schema mismatch")
    return MaterializedLabelOverlay(
        output_dir=output_dir,
        overlay_path=overlay_path,
        manifest_path=manifest_path,
        cache_identity_sha256=expected_identity,
        row_count=AUTHORITATIVE_DAILY_ROWS,
        reused=True,
    )


def materialize_daily_label_overlay(
    bundle: sources.DailySourceBundle,
    *,
    feature_panel_dir: Path,
    output_dir: Path,
    quote_config_path: Path,
    p3_v2_artifact_path: Path,
    symbol: str = "BTCUSDC",
) -> MaterializedLabelOverlay:
    """Generate and atomically admit one complete daily label-only overlay."""

    admitted = load_admitted_feature_panel(feature_panel_dir)
    _validate_bundle_binding(admitted, bundle)
    quote_config_path = quote_config_path.expanduser().resolve()
    p3_v2_artifact_path = p3_v2_artifact_path.expanduser().resolve()
    for role, path in (
        ("label quote config", quote_config_path),
        ("P3 v2 artifact", p3_v2_artifact_path),
    ):
        if not path.is_file():
            raise LabelOverlayMaterializationError(f"{role} is missing: {path}")
    try:
        resolved_p3 = execution_identity.validate_explicit_p3_identity(
            quote_config_path,
            p3_v2_artifact_path,
        )
    except execution_identity.ExecutionIdentityError as exc:
        raise LabelOverlayMaterializationError(str(exc)) from exc

    expected_payload = _identity_payload(
        admitted,
        bundle,
        quote_config_path=quote_config_path,
        p3_v2_artifact_path=p3_v2_artifact_path,
        symbol=symbol,
    )
    expected_identity = _canonical_sha256(expected_payload)
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        return _load_reusable(
            output_dir,
            expected_identity=expected_identity,
            expected_payload=expected_payload,
        )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex}"
    temporary_dir.mkdir()
    try:
        feature_panel = pd.read_parquet(admitted.panel_path)
        overlay_frame = labels.generate_daily_1s_labels(
            feature_panel,
            _label_bars(bundle),
            target_utc_day=bundle.utc_day,
            symbol=symbol,
            config_path=quote_config_path,
        )
        _validate_overlay_frame(overlay_frame)
        overlay_path = temporary_dir / OVERLAY_FILENAME
        table = pa.Table.from_pandas(
            overlay_frame,
            schema=overlay_arrow_schema(),
            preserve_index=False,
            safe=True,
        )
        pq.write_table(table, overlay_path, compression="zstd", write_statistics=True)
        _fsync_file(overlay_path)
        overlay_sha256 = _sha256_file(overlay_path)
        manifest = {
            "schema_version": ARTIFACT_SCHEMA_VERSION,
            "identity": labels.IDENTITY,
            "status": "materialized_not_training_or_live_authorized",
            "created_at_utc": datetime.now(tz=UTC).isoformat(),
            "utc_day": bundle.utc_day,
            "cache_identity_sha256": expected_identity,
            "cache_identity_payload": expected_payload,
            "feature_panel_manifest_sha256": admitted.manifest_sha256,
            "feature_panel_sha256": admitted.panel_sha256,
            "source_bundle_identity_sha256": bundle.identity_sha256(),
            "label_generator_sha256": expected_payload["label_generator"]["sha256"],
            "label_quote_config_sha256": expected_payload["label_quote_config"]["sha256"],
            "p3_v2_artifact_sha256": expected_payload["p3_v2_artifact"]["sha256"],
            "config_resolved_p3": resolved_p3,
            "overlay_schema": overlay_schema_payload(),
            "overlay": {
                "path": OVERLAY_FILENAME,
                "sha256": overlay_sha256,
                "size_bytes": overlay_path.stat().st_size,
                "rows": len(overlay_frame),
                "compression": "zstd",
            },
            "join_contract": {
                "keys": list(JOIN_COLUMNS),
                "unique": True,
                "feature_columns_copied": False,
            },
            "atomic_admission": True,
            "predictions_read": False,
            "economic_outcomes_read": False,
            "training_performed": False,
            "training_authorized": False,
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

    return MaterializedLabelOverlay(
        output_dir=output_dir,
        overlay_path=output_dir / OVERLAY_FILENAME,
        manifest_path=output_dir / MANIFEST_FILENAME,
        cache_identity_sha256=expected_identity,
        row_count=AUTHORITATIVE_DAILY_ROWS,
        reused=False,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-spec", type=Path, required=True)
    parser.add_argument("--feature-panel-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--quote-config", type=Path, required=True)
    parser.add_argument("--p3-v2-artifact", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    args = parser.parse_args()
    result = materialize_daily_label_overlay(
        sources.DailySourceBundle.from_json(args.source_spec.expanduser().resolve()),
        feature_panel_dir=args.feature_panel_dir,
        output_dir=args.output_dir,
        quote_config_path=args.quote_config,
        p3_v2_artifact_path=args.p3_v2_artifact,
        symbol=args.symbol,
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
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
