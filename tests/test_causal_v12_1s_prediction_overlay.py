from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_prediction_overlay as overlay,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training as training,
)

DAY = "2025-08-02"
DAY_START_MS = 1_754_092_800_000
ROWS = 8


def _sha256(path: Path) -> str:
    return overlay._sha256_file(path)


def _write_panel(panel_dir: Path, *, rows: int = ROWS) -> None:
    panel_dir.mkdir()
    timestamps = DAY_START_MS + np.arange(rows, dtype=np.int64) * 1_000
    values: dict[str, pa.Array] = {
        "cutoff_exclusive_ms": pa.array(timestamps, type=pa.int64()),
        "decision_ts_ms": pa.array(timestamps, type=pa.int64()),
        "feature_ready_ts_ms": pa.array(timestamps, type=pa.int64()),
        "unsupported_feature_count": pa.array(np.zeros(rows, dtype=np.int16)),
        "feature_row_fingerprint_sha256": pa.array(
            [f"{index:064x}" for index in range(1, rows + 1)]
        ),
        "local_bar_lag_state": pa.array(["observed_completed_1s"] * rows),
        "local_synthetic_seconds_24h": pa.array(np.zeros(rows, dtype=np.int32)),
        "reference_bar_lag_state": pa.array(["source_unavailable"] * rows),
        "reference_synthetic_seconds_1h": pa.array(np.zeros(rows, dtype=np.int32)),
    }
    for index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        values[name] = pa.array(
            np.arange(rows, dtype=np.float64) + float(index),
            type=pa.float64(),
        )
    panel_path = panel_dir / panels.PANEL_FILENAME
    pq.write_table(
        pa.Table.from_pydict(values, schema=panels.panel_arrow_schema()),
        panel_path,
        compression="zstd",
    )
    cache_payload = {"fixture": DAY, "rows": rows}
    manifest = {
        "schema_version": panels.ARTIFACT_SCHEMA_VERSION,
        "identity": schema.IDENTITY,
        "utc_day": DAY,
        "cache_identity_sha256": schema.canonical_sha256(cache_payload),
        "cache_identity_payload": cache_payload,
        "panel_schema": panels.panel_schema_payload(),
        "panel": {
            "path": panels.PANEL_FILENAME,
            "sha256": _sha256(panel_path),
            "size_bytes": panel_path.stat().st_size,
            "rows": rows,
        },
        "atomic_admission": True,
    }
    manifest_path = panel_dir / panels.MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (panel_dir / panels.SUCCESS_FILENAME).write_text(
        _sha256(manifest_path) + "\n",
        encoding="ascii",
    )


def _rewrite_panel_as_legacy_v3(panel_dir: Path) -> None:
    manifest_path = panel_dir / panels.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    legacy = overlay.LEGACY_FEATURE_PANEL_ARTIFACT_SCHEMA_VERSION
    manifest["schema_version"] = legacy
    manifest["panel_schema"] = {
        **manifest["panel_schema"],
        "schema_version": legacy,
    }
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (panel_dir / panels.SUCCESS_FILENAME).write_text(
        _sha256(manifest_path) + "\n",
        encoding="ascii",
    )


def _head_metadata(head: str, model_sha256: str) -> dict[str, object]:
    label, objective, metric, _ = training.HEAD_SPECS[head]
    return {
        "schema_version": overlay.HEAD_META_SCHEMA_VERSION,
        "name": head,
        "label_col": label,
        "objective": objective,
        "metric": metric,
        "feature_cols": list(schema.TRAINABLE_FEATURE_ORDER),
        "feature_count": len(schema.TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": schema.feature_order_sha256(),
        "feature_bucket_ms": 1_000,
        "model_sha256": model_sha256,
        "research_only": True,
        "prediction_authority": False,
        "action_authority": False,
        "live_authority": False,
    }


def _write_bundle(bundle_dir: Path) -> None:
    bundle_dir.mkdir()
    heads: dict[str, object] = {}
    for head in overlay.HEADS:
        model_path = bundle_dir / f"{head}.txt"
        model_path.write_text(f"fixture model for {head}\n", encoding="ascii")
        metadata_path = bundle_dir / f"{head}_meta.json"
        metadata_path.write_text(
            json.dumps(_head_metadata(head, _sha256(model_path)), sort_keys=True),
            encoding="utf-8",
        )
        heads[head] = {
            "model": {"path": model_path.name, "sha256": _sha256(model_path)},
            "metadata": {
                "path": metadata_path.name,
                "sha256": _sha256(metadata_path),
            },
        }
    bundle = {
        "schema_version": training.BUNDLE_SCHEMA_VERSION,
        "identity": schema.IDENTITY,
        "status": "research_only_transport_and_economics_not_run",
        "training_identity_sha256": "a" * 64,
        "heads": heads,
        "head_count": len(heads),
        "prediction_outcomes_read": False,
        "economic_outcomes_read": False,
        "native_transport_run": False,
        "full_path_ml_ab_run": False,
        "prediction_authority": False,
        "action_authority": False,
        "live_authority": False,
        "atomic_admission": True,
    }
    bundle_path = bundle_dir / "bundle_meta.json"
    bundle_path.write_text(json.dumps(bundle, sort_keys=True), encoding="utf-8")
    (bundle_dir / training.SUCCESS_FILENAME).write_text(
        _sha256(bundle_path) + "\n",
        encoding="ascii",
    )


def _rewrite_bundle_head_metadata(
    bundle_dir: Path,
    head: str,
    mutate,
) -> None:
    bundle_path = bundle_dir / "bundle_meta.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    metadata_path = bundle_dir / bundle["heads"][head]["metadata"]["path"]
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    mutate(metadata)
    metadata_path.write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
    bundle["heads"][head]["metadata"]["sha256"] = _sha256(metadata_path)
    bundle_path.write_text(json.dumps(bundle, sort_keys=True), encoding="utf-8")
    (bundle_dir / training.SUCCESS_FILENAME).write_text(
        _sha256(bundle_path) + "\n",
        encoding="ascii",
    )


class _FakeBooster:
    def __init__(self, head: str, *, invalid_prediction: bool = False) -> None:
        self.head = head
        self.invalid_prediction = invalid_prediction

    def num_feature(self) -> int:
        return len(schema.TRAINABLE_FEATURE_ORDER)

    def feature_name(self) -> list[str]:
        return list(schema.TRAINABLE_FEATURE_ORDER)

    def predict(self, matrix: np.ndarray) -> np.ndarray:
        if self.invalid_prediction and self.head == overlay.HEADS[0]:
            return np.full(len(matrix), np.nan)
        index = overlay.HEADS.index(self.head)
        if training.HEAD_SPECS[self.head][3]:
            return np.full(len(matrix), 0.05 + index * 0.01)
        return np.full(len(matrix), -0.5 + index * 0.01)


@pytest.fixture
def admitted_inputs(tmp_path: Path) -> tuple[Path, Path]:
    panel_dir = tmp_path / "panel"
    bundle_dir = tmp_path / "bundle"
    _write_panel(panel_dir)
    _write_bundle(bundle_dir)
    return panel_dir, bundle_dir


def test_exact_legacy_v3_feature_panel_is_accepted(
    admitted_inputs: tuple[Path, Path],
) -> None:
    panel_dir, _ = admitted_inputs
    _rewrite_panel_as_legacy_v3(panel_dir)

    admitted = overlay.load_admitted_feature_panel(
        panel_dir,
        test_only_row_count=ROWS,
    )

    assert admitted.manifest["schema_version"] == (
        overlay.LEGACY_FEATURE_PANEL_ARTIFACT_SCHEMA_VERSION
    )


def test_legacy_v3_feature_panel_with_projection_drift_fails_closed(
    admitted_inputs: tuple[Path, Path],
) -> None:
    panel_dir, _ = admitted_inputs
    _rewrite_panel_as_legacy_v3(panel_dir)
    manifest_path = panel_dir / panels.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["panel_schema"]["feature_count"] -= 1
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (panel_dir / panels.SUCCESS_FILENAME).write_text(
        _sha256(manifest_path) + "\n",
        encoding="ascii",
    )

    with pytest.raises(
        overlay.PredictionOverlayMaterializationError,
        match="feature panel schema payload mismatch",
    ):
        overlay.load_admitted_feature_panel(
            panel_dir,
            test_only_row_count=ROWS,
        )


def _patch_boosters(
    monkeypatch: pytest.MonkeyPatch,
    *,
    invalid_prediction: bool = False,
) -> None:
    monkeypatch.setattr(
        overlay,
        "_load_booster",
        lambda path: _FakeBooster(
            path.stem,
            invalid_prediction=invalid_prediction,
        ),
    )


def test_atomic_prediction_only_overlay_and_hash_reuse(
    admitted_inputs: tuple[Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel_dir, bundle_dir = admitted_inputs
    output_dir = tmp_path / "prediction-overlay"
    _patch_boosters(monkeypatch)

    result = overlay.materialize_daily_prediction_overlay(
        feature_panel_dir=panel_dir,
        research_bundle_dir=bundle_dir,
        output_dir=output_dir,
        batch_rows=3,
        test_only_row_count=ROWS,
    )

    assert not result.reused
    assert result.test_only
    assert result.row_count == ROWS
    assert (output_dir / overlay.SUCCESS_FILENAME).read_text().strip() == _sha256(
        output_dir / overlay.MANIFEST_FILENAME
    )
    parquet = pq.ParquetFile(result.overlay_path)
    assert parquet.metadata.num_rows == ROWS
    assert parquet.schema_arrow.equals(overlay.prediction_overlay_arrow_schema())
    assert parquet.schema_arrow.names == [
        *overlay.JOIN_COLUMNS,
        *(overlay.PREDICTION_COLUMN_BY_HEAD[head] for head in overlay.HEADS),
    ]
    assert not set(schema.TRAINABLE_FEATURE_ORDER) & set(parquet.schema_arrow.names)
    assert not any(name.startswith("label") for name in parquet.schema_arrow.names)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["feature_panel_sha256"] == _sha256(panel_dir / panels.PANEL_FILENAME)
    assert manifest["research_bundle_sha256"] == _sha256(bundle_dir / "bundle_meta.json")
    assert manifest["feature_order_sha256"] == schema.feature_order_sha256()
    assert manifest["labels_read"] is False
    assert manifest["economic_outcomes_read"] is False
    assert manifest["prediction_authorized"] is False
    assert manifest["action_authorized"] is False
    assert manifest["live_authorized"] is False
    assert manifest["prediction_postprocess"] == overlay.PREDICTION_POSTPROCESS_CONTRACT
    table = pq.read_table(result.overlay_path)
    for head in overlay.VOLATILITY_HEADS:
        values = table[overlay.PREDICTION_COLUMN_BY_HEAD[head]].to_numpy()
        assert np.all(values >= 0.0)

    reused = overlay.materialize_daily_prediction_overlay(
        feature_panel_dir=panel_dir,
        research_bundle_dir=bundle_dir,
        output_dir=output_dir,
        batch_rows=2,
        test_only_row_count=ROWS,
    )
    assert reused.reused
    assert reused.cache_identity_sha256 == result.cache_identity_sha256


def test_formal_default_rejects_small_panel_without_test_only_override(
    admitted_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    panel_dir, bundle_dir = admitted_inputs
    with pytest.raises(
        overlay.PredictionOverlayMaterializationError,
        match="formal prediction overlay requires exactly 86,400 panel rows",
    ):
        overlay.materialize_daily_prediction_overlay(
            feature_panel_dir=panel_dir,
            research_bundle_dir=bundle_dir,
            output_dir=tmp_path / "must-not-exist",
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("feature_cols", [], "feature order differs"),
        ("feature_order_sha256", "0" * 64, "feature_order_sha256 differs"),
        ("feature_bucket_ms", 10_000, "feature_bucket_ms differs"),
        ("research_only", False, "research_only differs"),
    ),
)
def test_rejects_drifted_head_metadata(
    admitted_inputs: tuple[Path, Path],
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    panel_dir, original_bundle = admitted_inputs
    bundle_dir = tmp_path / f"bundle-{field}"
    shutil.copytree(original_bundle, bundle_dir)
    _rewrite_bundle_head_metadata(
        bundle_dir,
        overlay.HEADS[0],
        lambda metadata: metadata.__setitem__(field, value),
    )
    with pytest.raises(overlay.PredictionOverlayMaterializationError, match=message):
        overlay.load_admitted_research_bundle(bundle_dir)
    assert panel_dir.is_dir()


def test_rejects_bundle_or_model_hash_drift(
    admitted_inputs: tuple[Path, Path],
) -> None:
    _, bundle_dir = admitted_inputs
    model_path = bundle_dir / f"{overlay.HEADS[0]}.txt"
    model_path.write_text("changed after bundle admission\n", encoding="ascii")
    with pytest.raises(
        overlay.PredictionOverlayMaterializationError,
        match="model SHA256 mismatch",
    ):
        overlay.load_admitted_research_bundle(bundle_dir)


def test_prediction_failure_never_admits_partial_output(
    admitted_inputs: tuple[Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel_dir, bundle_dir = admitted_inputs
    output_dir = tmp_path / "prediction-failure"
    _patch_boosters(monkeypatch, invalid_prediction=True)
    with pytest.raises(
        overlay.PredictionOverlayMaterializationError,
        match="invalid prediction shape or values",
    ):
        overlay.materialize_daily_prediction_overlay(
            feature_panel_dir=panel_dir,
            research_bundle_dir=bundle_dir,
            output_dir=output_dir,
            test_only_row_count=ROWS,
        )
    assert not output_dir.exists()
    assert not list(tmp_path.glob(f".{output_dir.name}.tmp-*"))


def test_existing_output_must_preserve_permission_boundary(
    admitted_inputs: tuple[Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel_dir, bundle_dir = admitted_inputs
    output_dir = tmp_path / "prediction-overlay"
    _patch_boosters(monkeypatch)
    overlay.materialize_daily_prediction_overlay(
        feature_panel_dir=panel_dir,
        research_bundle_dir=bundle_dir,
        output_dir=output_dir,
        test_only_row_count=ROWS,
    )
    manifest_path = output_dir / overlay.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["live_authorized"] = True
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (output_dir / overlay.SUCCESS_FILENAME).write_text(
        _sha256(manifest_path) + "\n",
        encoding="ascii",
    )
    with pytest.raises(
        overlay.PredictionOverlayMaterializationError,
        match="permission boundary",
    ):
        overlay.materialize_daily_prediction_overlay(
            feature_panel_dir=panel_dir,
            research_bundle_dir=bundle_dir,
            output_dir=output_dir,
            test_only_row_count=ROWS,
        )
