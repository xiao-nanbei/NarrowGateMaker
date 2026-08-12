from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_overlay_materializer as overlays,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import causal_v12_1s_training as training
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training_admission_v3 as admissions,
)

SMALL_ROWS_PER_DAY = 8
DAYS = ("2025-08-02", "2025-08-03")


@dataclass(frozen=True, slots=True)
class _DayFixture:
    utc_day: str
    panel_dir: Path
    overlay_dir: Path
    panel_path: Path
    overlay_path: Path


@dataclass(frozen=True, slots=True)
class _ExecutionInputs:
    days: tuple[training.TrainingDayArtifact, ...]
    day_manifest_path: Path
    matrix_cache_dir: Path
    training_design_path: Path
    amendment_path: Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _admit_manifest(directory: Path, manifest_name: str, success_name: str) -> None:
    manifest_path = directory / manifest_name
    (directory / success_name).write_text(_sha256(manifest_path) + "\n", encoding="ascii")


def _day_start_ms(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000)


def _fingerprints(day: str, rows: int) -> list[str]:
    return [hashlib.sha256(f"{day}:{row}".encode()).hexdigest() for row in range(rows)]


def _panel_table(
    day: str,
    rows: int,
    *,
    feature_bias: float = 0.0,
) -> pa.Table:
    decision = _day_start_ms(day) + np.arange(rows, dtype=np.int64) * 1_000
    values: dict[str, pa.Array] = {
        "cutoff_exclusive_ms": pa.array(decision, type=pa.int64()),
        "decision_ts_ms": pa.array(decision, type=pa.int64()),
        "feature_ready_ts_ms": pa.array(decision, type=pa.int64()),
        "unsupported_feature_count": pa.array(np.zeros(rows, dtype=np.int16)),
        "feature_row_fingerprint_sha256": pa.array(_fingerprints(day, rows)),
        "local_bar_lag_state": pa.array(["observed_completed_1s"] * rows),
        "local_synthetic_seconds_24h": pa.array(np.zeros(rows, dtype=np.int32)),
        "reference_bar_lag_state": pa.array(["source_unavailable"] * rows),
        "reference_synthetic_seconds_1h": pa.array(np.zeros(rows, dtype=np.int32)),
    }
    day_bias = float(int(day[-2:]) * 1_000)
    row_values = np.arange(rows, dtype=np.float64) / 10.0
    for column_index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        values[name] = pa.array(
            feature_bias + day_bias + float(column_index) + row_values,
            type=pa.float64(),
        )
    return pa.Table.from_pydict(values, schema=panels.panel_arrow_schema())


def _overlay_table(
    day: str,
    rows: int,
    *,
    join_fingerprint_drift: bool = False,
    regression_shift: float = 0.0,
) -> pa.Table:
    decision = _day_start_ms(day) + np.arange(rows, dtype=np.int64) * 1_000
    fingerprints = _fingerprints(day, rows)
    if join_fingerprint_drift:
        fingerprints[0] = "f" * 64
    values: dict[str, pa.Array] = {
        "cutoff_exclusive_ms": pa.array(decision, type=pa.int64()),
        "decision_ts_ms": pa.array(decision, type=pa.int64()),
        "feature_ready_ts_ms": pa.array(decision, type=pa.int64()),
        "feature_row_fingerprint_sha256": pa.array(fingerprints),
    }
    binary_labels = (np.arange(rows) % 2).astype(np.float64)
    regression_labels = np.linspace(-0.2, 0.2, rows, dtype=np.float64) + regression_shift
    for head, (label_column, _, _, is_classification) in training.HEAD_SPECS.items():
        labels = binary_labels if is_classification else regression_labels
        values[label_column] = pa.array(labels, type=pa.float64())
        values[f"label_valid__{head}"] = pa.array(np.ones(rows, dtype=bool))
        values[f"sample_weight__{head}"] = pa.array(
            np.ones(rows, dtype=np.float64), type=pa.float64()
        )
        values[f"overlap_uniqueness__{head}"] = pa.array(
            np.full(rows, 0.5, dtype=np.float64), type=pa.float64()
        )
    return pa.Table.from_pydict(values, schema=overlays.overlay_arrow_schema())


def _create_admitted_day(
    root: Path,
    day: str,
    *,
    rows: int = SMALL_ROWS_PER_DAY,
    join_fingerprint_drift: bool = False,
) -> _DayFixture:
    panel_dir = root / day / "feature-panel"
    overlay_dir = root / day / "label-overlay"
    panel_dir.mkdir(parents=True)
    overlay_dir.mkdir(parents=True)

    panel_path = panel_dir / panels.PANEL_FILENAME
    pq.write_table(
        _panel_table(day, rows),
        panel_path,
        compression="zstd",
        row_group_size=3,
    )
    panel_identity = {"fixture": "small_training_day", "utc_day": day}
    panel_manifest = {
        "schema_version": panels.ARTIFACT_SCHEMA_VERSION,
        "identity": schema.IDENTITY,
        "utc_day": day,
        "cache_identity_sha256": _canonical_sha256(panel_identity),
        "cache_identity_payload": panel_identity,
        "panel_schema": panels.panel_schema_payload(),
        "panel": {
            "path": panels.PANEL_FILENAME,
            "sha256": _sha256(panel_path),
            "rows": rows,
        },
        "atomic_admission": True,
    }
    _write_json(panel_dir / panels.MANIFEST_FILENAME, panel_manifest)
    _admit_manifest(panel_dir, panels.MANIFEST_FILENAME, panels.SUCCESS_FILENAME)

    overlay_path = overlay_dir / overlays.OVERLAY_FILENAME
    pq.write_table(
        _overlay_table(day, rows, join_fingerprint_drift=join_fingerprint_drift),
        overlay_path,
        compression="zstd",
        row_group_size=3,
    )
    overlay_identity = {"fixture": "small_label_overlay", "utc_day": day}
    overlay_manifest = {
        "schema_version": overlays.ARTIFACT_SCHEMA_VERSION,
        "identity": "causal_v12_1s_label_overlay_fixture",
        "utc_day": day,
        "cache_identity_sha256": _canonical_sha256(overlay_identity),
        "cache_identity_payload": overlay_identity,
        "feature_panel_manifest_sha256": _sha256(panel_dir / panels.MANIFEST_FILENAME),
        "feature_panel_sha256": _sha256(panel_path),
        "overlay_schema": overlays.overlay_schema_payload(),
        "overlay": {
            "path": overlays.OVERLAY_FILENAME,
            "sha256": _sha256(overlay_path),
            "rows": rows,
        },
        "join_contract": {
            "keys": list(overlays.JOIN_COLUMNS),
            "unique": True,
            "feature_columns_copied": False,
        },
        "atomic_admission": True,
    }
    _write_json(overlay_dir / overlays.MANIFEST_FILENAME, overlay_manifest)
    _admit_manifest(overlay_dir, overlays.MANIFEST_FILENAME, overlays.SUCCESS_FILENAME)
    return _DayFixture(day, panel_dir, overlay_dir, panel_path, overlay_path)


def _write_day_manifest(path: Path, fixtures: list[_DayFixture]) -> Path:
    pipeline_path = path.parent / "fixture-pipeline-receipt.json"
    gate_path = path.parent / "fixture-parity-gate.json"
    if not pipeline_path.exists():
        _write_json(pipeline_path, {"fixture": "pipeline"})
    if not gate_path.exists():
        _write_json(gate_path, {"fixture": "parity"})
    rows = []
    for fixture in fixtures:
        admission_path = path.parent / f"{fixture.utc_day}.admission.json"
        admission = {
            "utc_day": fixture.utc_day,
            "feature_manifest": {
                "path": str(fixture.panel_dir / panels.MANIFEST_FILENAME)
            },
            "label_manifest": {
                "path": str(fixture.overlay_dir / overlays.MANIFEST_FILENAME)
            },
            "admission_identity_sha256": hashlib.sha256(
                f"admission:{fixture.utc_day}".encode()
            ).hexdigest(),
            "pipeline_execution_receipt": {
                "path": str(pipeline_path),
                "execution_identity_sha256": "1" * 64,
            },
            "parity_successor_gate": {
                "path": str(gate_path),
                "parity_gate_identity_sha256": "2" * 64,
            },
            "f03_component_semantics_sha256": "3" * 64,
        }
        _write_json(admission_path, admission)
        rows.append(
            {
                "utc_day": fixture.utc_day,
                "feature_panel_dir": str(fixture.panel_dir),
                "label_overlay_dir": str(fixture.overlay_dir),
                "admission_receipt_path": str(admission_path),
                "admission_receipt_sha256": _sha256(admission_path),
                "admission_identity_sha256": admission[
                    "admission_identity_sha256"
                ],
            }
        )
    payload = {
        "schema_version": admissions.TRAINING_DAY_MANIFEST_SCHEMA_VERSION,
        "profile_id": "provider_normalized_v1",
        "source_permissions": training.execution_identity.SOURCE_PERMISSION_CONTRACT,
        "pipeline_execution_receipt": {"execution_identity_sha256": "1" * 64},
        "parity_successor_gate": {"parity_gate_identity_sha256": "2" * 64},
        "days": rows,
        "training_input_authorized": True,
        "queue_authority": False,
        "order_lifecycle_authority": False,
        "fill_path_authority": False,
        "pnl_authority": False,
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
        "training_performed": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _write_json(path, payload)
    return path


@pytest.fixture(autouse=True)
def _unit_fixture_successor_gate(monkeypatch: pytest.MonkeyPatch):
    def load_admission(path: Path) -> dict[str, Any]:
        return json.loads(Path(path).read_text(encoding="utf-8"))

    successor = {
        "pipeline_execution_identity_sha256": "1" * 64,
        "parity_gate_identity_sha256": "2" * 64,
        "f03_component_semantics_sha256": "3" * 64,
    }
    monkeypatch.setattr(admissions, "validate_daily_training_admission", load_admission)
    monkeypatch.setattr(training, "_validate_successor_admission_set", lambda _: successor)
    monkeypatch.setattr(
        training.execution_identity,
        "validate_pipeline_execution_receipt",
        lambda *_args, **_kwargs: {
            "execution_identity_sha256": "1" * 64,
            "native_build_receipt": {"receipt_sha256": "4" * 64},
            "f03_component_semantics": {"identity_sha256": "3" * 64},
            "quote_config": {"path": "fixture", "sha256": "5" * 64},
            "p3_v2_artifact": {"path": "fixture", "sha256": "6" * 64},
        },
    )
    monkeypatch.setattr(
        training.parity_successor_gate,
        "validate_training_parity_gate",
        lambda *_args, **_kwargs: {
            "parity_gate_identity_sha256": "2" * 64,
            "training_authorized": True,
        },
    )


def _load_small_days(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    day_names: tuple[str, ...] = DAYS,
) -> tuple[training.TrainingDayArtifact, ...]:
    monkeypatch.setattr(training, "ROWS_PER_DAY", SMALL_ROWS_PER_DAY)
    fixtures = [_create_admitted_day(tmp_path, day) for day in day_names]
    manifest_path = _write_day_manifest(tmp_path / "training-days.json", fixtures)
    return training.load_training_day_manifest(manifest_path, expected_days=day_names)


def _small_training_audit(
    day: str,
    *,
    missing: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "schema_version": "causal_v12_1s_training_contract_audit.v1",
        "identity": training.contract.IDENTITY,
        "fit_days": [day],
        "embargo_days": [],
        "selection_days": [],
        "refit_days": [day],
        "head_count": len(training.HEAD_SPECS),
        "training_execution_eligible": not missing,
        "missing_execution_artifacts": list(missing),
        "bound_execution_artifacts": [],
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
    }


def _create_execution_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> _ExecutionInputs:
    allowed_missing = tuple(sorted(training.AMENDMENT_DECLARED_MISSING_EXECUTION_ARTIFACTS))
    monkeypatch.setattr(
        training.contract,
        "load_and_validate_training_design",
        lambda _: _small_training_audit(DAYS[0], missing=allowed_missing),
    )
    days = _load_small_days(tmp_path, monkeypatch, day_names=(DAYS[0],))
    matrix_cache_dir = tmp_path / "matrix-cache"
    training.build_training_matrix_cache(days, output_dir=matrix_cache_dir)
    training_design_path = tmp_path / "training-design.json"
    _write_json(
        training_design_path,
        {
            "schema_version": training.contract.SCHEMA_VERSION,
            "identity": training.contract.IDENTITY,
            "fixture": "small_execution_amendment",
        },
    )
    return _ExecutionInputs(
        days=days,
        day_manifest_path=tmp_path / "training-days.json",
        matrix_cache_dir=matrix_cache_dir,
        training_design_path=training_design_path,
        amendment_path=tmp_path / "training-execution-amendment.json",
    )


def _freeze_execution_amendment(inputs: _ExecutionInputs) -> dict[str, Any]:
    return training.freeze_training_execution_amendment(
        inputs.days,
        day_manifest_path=inputs.day_manifest_path,
        matrix_cache_dir=inputs.matrix_cache_dir,
        training_design_path=inputs.training_design_path,
        output_path=inputs.amendment_path,
    )


def _rewrite_amendment_with_recomputed_identity(
    path: Path,
    mutate: Any,
) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("execution_identity_sha256", None)
    mutate(payload)
    payload["execution_identity_sha256"] = _canonical_sha256(payload)
    _write_json(path, payload)
    return payload


def _rewrite_manifest(directory: Path, filename: str, success_name: str, **updates: Any) -> None:
    path = directory / filename
    payload = json.loads(path.read_text(encoding="utf-8"))
    for key, value in updates.items():
        payload[key] = value
    _write_json(path, payload)
    _admit_manifest(directory, filename, success_name)


def test_load_training_manifest_accepts_exact_admitted_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _load_small_days(tmp_path, monkeypatch)

    assert tuple(day.utc_day for day in days) == DAYS
    assert all(day.feature_panel_sha256 == _sha256(day.feature_panel_path) for day in days)
    assert all(day.overlay_sha256 == _sha256(day.overlay_path) for day in days)


@pytest.mark.parametrize("drift", ["feature_success", "panel_hash", "overlay_binding"])
def test_daily_admission_hashes_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    monkeypatch.setattr(training, "ROWS_PER_DAY", SMALL_ROWS_PER_DAY)
    fixture = _create_admitted_day(tmp_path, DAYS[0])
    if drift == "feature_success":
        (fixture.panel_dir / panels.SUCCESS_FILENAME).write_text("0" * 64, encoding="ascii")
        expected = "feature _SUCCESS mismatch"
    elif drift == "panel_hash":
        manifest_path = fixture.panel_dir / panels.MANIFEST_FILENAME
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload["panel"]["sha256"] = "0" * 64
        _write_json(manifest_path, payload)
        _admit_manifest(fixture.panel_dir, panels.MANIFEST_FILENAME, panels.SUCCESS_FILENAME)
        expected = "feature panel SHA256 mismatch"
    else:
        _rewrite_manifest(
            fixture.overlay_dir,
            overlays.MANIFEST_FILENAME,
            overlays.SUCCESS_FILENAME,
            feature_panel_sha256="0" * 64,
        )
        expected = "overlay does not bind feature panel"
    manifest_path = _write_day_manifest(tmp_path / "training-days.json", [fixture])

    with pytest.raises(training.OneSecondTrainingError, match=expected):
        training.load_training_day_manifest(manifest_path, expected_days=(DAYS[0],))


def test_daily_join_identity_drift_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(training, "ROWS_PER_DAY", SMALL_ROWS_PER_DAY)
    fixture = _create_admitted_day(tmp_path, DAYS[0], join_fingerprint_drift=True)
    manifest_path = _write_day_manifest(tmp_path / "training-days.json", [fixture])

    with pytest.raises(training.OneSecondTrainingError, match="join identity mismatch"):
        training.load_training_day_manifest(manifest_path, expected_days=(DAYS[0],))


def test_training_day_order_and_frozen_membership_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(training, "ROWS_PER_DAY", SMALL_ROWS_PER_DAY)
    fixtures = [_create_admitted_day(tmp_path, day) for day in DAYS]
    reversed_manifest = _write_day_manifest(
        tmp_path / "reversed.json",
        list(reversed(fixtures)),
    )
    with pytest.raises(training.OneSecondTrainingError, match="sorted and unique"):
        training.load_training_day_manifest(reversed_manifest)

    ordered_manifest = _write_day_manifest(tmp_path / "ordered.json", fixtures)
    with pytest.raises(training.OneSecondTrainingError, match="frozen refit days"):
        training.load_training_day_manifest(
            ordered_manifest,
            expected_days=(DAYS[0],),
        )


def test_matrix_cache_is_atomic_exact_and_reusable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _load_small_days(tmp_path, monkeypatch)
    output_dir = tmp_path / "matrix-cache"

    first = training.build_training_matrix_cache(days, output_dir=output_dir)
    manifest_path = output_dir / training.MATRIX_MANIFEST_FILENAME
    matrix_path = output_dir / training.MATRIX_FILENAME
    manifest_sha = _sha256(manifest_path)
    matrix_sha = _sha256(matrix_path)
    matrix = np.load(matrix_path, mmap_mode="r")

    assert matrix.shape == (
        len(days) * SMALL_ROWS_PER_DAY,
        len(schema.TRAINABLE_FEATURE_ORDER),
    )
    assert matrix.dtype == np.dtype("float32")
    assert matrix[0, 0] == pytest.approx(2_000.0)
    assert matrix[SMALL_ROWS_PER_DAY, 0] == pytest.approx(3_000.0)
    assert first["matrix"]["sha256"] == matrix_sha
    assert (output_dir / training.SUCCESS_FILENAME).read_text().strip() == manifest_sha
    assert not list(tmp_path.glob(".matrix-cache.tmp-*"))

    second = training.build_training_matrix_cache(days, output_dir=output_dir)
    assert second == first
    assert _sha256(matrix_path) == matrix_sha
    assert not list(tmp_path.glob(".matrix-cache.tmp-*"))

    with pytest.raises(training.OneSecondTrainingError, match="different identity"):
        training.build_training_matrix_cache(days[:1], output_dir=output_dir)


def test_matrix_build_failure_leaves_no_admitted_or_temporary_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _load_small_days(tmp_path, monkeypatch, day_names=(DAYS[0],))
    output_dir = tmp_path / "failed-cache"
    original = training.pq.ParquetFile

    def _raise_on_panel(path: Path, *args: Any, **kwargs: Any) -> pq.ParquetFile:
        if Path(path) == days[0].feature_panel_path:
            raise RuntimeError("injected row-group read failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(training.pq, "ParquetFile", _raise_on_panel)
    with pytest.raises(RuntimeError, match="injected row-group read failure"):
        training.build_training_matrix_cache(days, output_dir=output_dir)

    assert not output_dir.exists()
    assert not list(tmp_path.glob(".failed-cache.tmp-*"))


def test_memmap_feature_sequence_indexes_only_its_declared_interval(tmp_path: Path) -> None:
    values = np.arange(60, dtype=np.float32).reshape(12, 5)
    matrix_path = tmp_path / "sequence.npy"
    np.save(matrix_path, values)
    matrix = np.load(matrix_path, mmap_mode="r")
    sequence = training.MemmapFeatureSequence(
        matrix,
        row_start=2,
        row_stop=10,
        batch_size=3,
    )

    assert len(sequence) == 8
    assert sequence.batch_size == 3
    np.testing.assert_array_equal(sequence[0], values[2])
    np.testing.assert_array_equal(sequence[-1], values[9])
    np.testing.assert_array_equal(sequence[1:6:2], values[3:8:2])
    np.testing.assert_array_equal(sequence[[0, 3, 7]], values[[2, 5, 9]])
    assert sequence[0].dtype == np.dtype("float64")

    with pytest.raises(IndexError):
        _ = sequence[8]
    with pytest.raises(IndexError, match="outside interval"):
        _ = sequence[[0, 8]]
    with pytest.raises(TypeError, match="unsupported Sequence index"):
        _ = sequence[(0, 1)]


def test_lightgbm_can_train_from_small_memmap_feature_sequence(tmp_path: Path) -> None:
    rng = np.random.default_rng(17)
    values = rng.normal(size=(128, 4)).astype(np.float32)
    labels = (values[:, 0] + values[:, 1] > 0.0).astype(np.float32)
    matrix_path = tmp_path / "lightgbm-sequence.npy"
    np.save(matrix_path, values)
    matrix = np.load(matrix_path, mmap_mode="r")
    sequence = training.MemmapFeatureSequence(
        matrix,
        row_start=0,
        row_stop=len(matrix),
        batch_size=17,
    )
    dataset = lgb.Dataset(sequence, label=labels, free_raw_data=False)

    model = lgb.train(
        {
            "objective": "binary",
            "metric": "auc",
            "verbosity": -1,
            "num_threads": 1,
            "min_data_in_leaf": 4,
            "num_leaves": 7,
        },
        dataset,
        num_boost_round=4,
    )

    assert model.current_iteration() == 4
    assert np.isfinite(model.predict(values[:8])).all()


def test_training_requires_execution_amendment_before_matrix_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit = _small_training_audit(
        DAYS[0],
        missing=("training_implementation_sha256",),
    )
    audit["fit_days"] = []
    audit["refit_days"] = []
    monkeypatch.setattr(
        training.contract,
        "load_and_validate_training_design",
        lambda _: audit,
    )

    with pytest.raises(training.OneSecondTrainingError, match="not eligible"):
        training.train_research_bundle(
            (),
            matrix_cache_dir=tmp_path / "must-not-be-read",
            output_dir=tmp_path / "must-not-be-created",
            training_design_path=tmp_path / "design.json",
        )


def test_reusable_matrix_recomputes_manifest_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _load_small_days(tmp_path, monkeypatch, day_names=(DAYS[0],))
    output_dir = tmp_path / "matrix-cache"
    training.build_training_matrix_cache(days, output_dir=output_dir)
    manifest_path = output_dir / training.MATRIX_MANIFEST_FILENAME
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    forged_identity = "f" * 64
    payload["cache_identity_sha256"] = forged_identity
    _write_json(manifest_path, payload)
    _admit_manifest(
        output_dir,
        training.MATRIX_MANIFEST_FILENAME,
        training.SUCCESS_FILENAME,
    )

    with pytest.raises(training.OneSecondTrainingError, match="cannot be reproduced"):
        training._load_reusable_matrix(output_dir, forged_identity)


def test_matrix_build_rechecks_feature_hash_after_day_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _load_small_days(tmp_path, monkeypatch, day_names=(DAYS[0],))
    pq.write_table(
        _panel_table(DAYS[0], SMALL_ROWS_PER_DAY, feature_bias=50_000.0),
        days[0].feature_panel_path,
        compression="zstd",
        row_group_size=3,
    )

    with pytest.raises(training.OneSecondTrainingError, match="changed after admission"):
        training.build_training_matrix_cache(days, output_dir=tmp_path / "matrix-cache")


def test_head_target_load_rechecks_overlay_hash_after_day_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _load_small_days(tmp_path, monkeypatch, day_names=(DAYS[0],))
    pq.write_table(
        _overlay_table(DAYS[0], SMALL_ROWS_PER_DAY, regression_shift=100.0),
        days[0].overlay_path,
        compression="zstd",
        row_group_size=3,
    )

    with pytest.raises(training.OneSecondTrainingError, match="changed after admission"):
        training._load_head_targets(days, "ret_10s")


def test_feature_matrix_reuse_is_independent_of_label_overlay_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _load_small_days(tmp_path, monkeypatch, day_names=(DAYS[0],))
    output_dir = tmp_path / "matrix-cache"
    first = training.build_training_matrix_cache(days, output_dir=output_dir)
    label_only_change = replace(
        days[0],
        overlay_manifest_sha256="a" * 64,
        overlay_sha256="b" * 64,
    )

    reused = training.build_training_matrix_cache((label_only_change,), output_dir=output_dir)

    assert reused == first


def test_execution_amendment_freeze_is_deterministic_reusable_and_validated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)

    frozen = _freeze_execution_amendment(inputs)
    unsigned = dict(frozen)
    execution_identity = unsigned.pop("execution_identity_sha256")

    assert execution_identity == _canonical_sha256(unsigned)
    assert frozen["schema_version"] == training.EXECUTION_AMENDMENT_SCHEMA_VERSION
    assert frozen["training_implementation"]["sha256"] == _sha256(Path(training.__file__).resolve())
    assert frozen["training_day_manifest"]["sha256"] == _sha256(inputs.day_manifest_path)
    assert frozen["matrix_cache_manifest"]["sha256"] == _sha256(
        inputs.matrix_cache_dir / training.MATRIX_MANIFEST_FILENAME
    )
    assert frozen["resolved_design_preconditions"] == [
        "one_second_feature_panel_manifest",
        "training_implementation_sha256",
    ]
    assert frozen["declared_missing_execution_artifacts"] == sorted(
        training.AMENDMENT_DECLARED_MISSING_EXECUTION_ARTIFACTS
    )
    assert frozen["lightgbm_runtime_abi"]["lightgbm_version"] == lgb.__version__
    assert frozen["lightgbm_runtime_abi"]["sequence_contract"]["lightgbm_input_dtype"] == "float64"
    assert frozen["model_output_identity_role"] == "atomic_training_postcondition"
    assert frozen["economic_outcomes_read"] is False
    assert frozen["prediction_outcomes_read"] is False
    assert frozen["model_training_executed"] is False
    assert frozen["action_authorized"] is False
    assert frozen["live_authorized"] is False
    assert not list(tmp_path.glob(".training-execution-amendment.json.tmp-*"))

    validated = training.validate_training_execution_amendment(
        inputs.amendment_path,
        inputs.days,
        matrix_cache_dir=inputs.matrix_cache_dir,
        training_design_path=inputs.training_design_path,
    )
    assert validated == frozen
    assert _freeze_execution_amendment(inputs) == frozen


def test_execution_amendment_is_required_even_before_matrix_access(
    tmp_path: Path,
) -> None:
    with pytest.raises(training.OneSecondTrainingError, match="without a frozen"):
        training.validate_training_execution_amendment(
            None,
            (),
            matrix_cache_dir=tmp_path / "must-not-be-read",
            training_design_path=tmp_path / "must-not-be-read.json",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("economic_outcomes_read", True),
        ("model_training_executed", True),
        ("action_authorized", True),
        ("live_authorized", True),
        ("status", "training_authorized"),
    ],
)
def test_execution_amendment_rejects_rehashed_authority_or_status_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: Any,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    _freeze_execution_amendment(inputs)
    _rewrite_amendment_with_recomputed_identity(
        inputs.amendment_path,
        lambda payload: payload.__setitem__(field, value),
    )

    with pytest.raises(training.OneSecondTrainingError, match="amendment hash mismatch"):
        training.validate_training_execution_amendment(
            inputs.amendment_path,
            inputs.days,
            matrix_cache_dir=inputs.matrix_cache_dir,
            training_design_path=inputs.training_design_path,
        )


def test_execution_amendment_rejects_unreproducible_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    payload = _freeze_execution_amendment(inputs)
    payload["execution_identity_sha256"] = "0" * 64
    _write_json(inputs.amendment_path, payload)

    with pytest.raises(training.OneSecondTrainingError, match="amendment hash mismatch"):
        training.validate_training_execution_amendment(
            inputs.amendment_path,
            inputs.days,
            matrix_cache_dir=inputs.matrix_cache_dir,
            training_design_path=inputs.training_design_path,
        )


@pytest.mark.parametrize("drift", ["training_design", "day_manifest", "matrix_manifest"])
def test_execution_amendment_rejects_bound_input_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    _freeze_execution_amendment(inputs)
    if drift == "training_design":
        inputs.training_design_path.write_text(
            inputs.training_design_path.read_text(encoding="utf-8") + "\n",
            encoding="utf-8",
        )
    elif drift == "day_manifest":
        payload = json.loads(inputs.day_manifest_path.read_text(encoding="utf-8"))
        payload["post_freeze_drift"] = True
        _write_json(inputs.day_manifest_path, payload)
    else:
        manifest_path = inputs.matrix_cache_dir / training.MATRIX_MANIFEST_FILENAME
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload["post_freeze_drift"] = True
        _write_json(manifest_path, payload)
        _admit_manifest(
            inputs.matrix_cache_dir,
            training.MATRIX_MANIFEST_FILENAME,
            training.SUCCESS_FILENAME,
        )

    with pytest.raises(training.OneSecondTrainingError, match="amendment hash mismatch"):
        training.validate_training_execution_amendment(
            inputs.amendment_path,
            inputs.days,
            matrix_cache_dir=inputs.matrix_cache_dir,
            training_design_path=inputs.training_design_path,
        )


def test_execution_amendment_rejects_training_implementation_hash_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    _freeze_execution_amendment(inputs)
    implementation_path = Path(training.__file__).resolve()
    original_sha256_file = training._sha256_file

    def _drift_implementation(path: Path) -> str:
        if Path(path).resolve() == implementation_path:
            return "0" * 64
        return original_sha256_file(path)

    monkeypatch.setattr(training, "_sha256_file", _drift_implementation)
    with pytest.raises(training.OneSecondTrainingError, match="amendment hash mismatch"):
        training.validate_training_execution_amendment(
            inputs.amendment_path,
            inputs.days,
            matrix_cache_dir=inputs.matrix_cache_dir,
            training_design_path=inputs.training_design_path,
        )


def test_execution_amendment_freeze_refuses_existing_different_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    payload = _freeze_execution_amendment(inputs)
    payload["live_authorized"] = True
    _write_json(inputs.amendment_path, payload)

    with pytest.raises(FileExistsError, match="refusing to replace"):
        _freeze_execution_amendment(inputs)


def test_execution_amendment_freeze_failure_leaves_no_partial_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)

    def _raise_during_write(path: Path, payload: Any) -> None:
        del path, payload
        raise RuntimeError("injected amendment write failure")

    monkeypatch.setattr(training, "_write_json_fsync", _raise_during_write)
    with pytest.raises(RuntimeError, match="injected amendment write failure"):
        _freeze_execution_amendment(inputs)

    assert not inputs.amendment_path.exists()
    assert not list(tmp_path.glob(".training-execution-amendment.json.tmp-*"))


def test_execution_amendment_freeze_rejects_day_manifest_semantic_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    wrong_manifest = {
        "schema_version": "causal_v12_1s_training_day_manifest.v1",
        "days": [
            {
                "utc_day": DAYS[0],
                "feature_panel_dir": str(tmp_path / "different-feature-panel"),
                "label_overlay_dir": str(tmp_path / "different-label-overlay"),
            }
        ],
    }
    _write_json(inputs.day_manifest_path, wrong_manifest)

    with pytest.raises(training.OneSecondTrainingError, match="day manifest.*daily artifacts"):
        _freeze_execution_amendment(inputs)


def test_training_amendment_rejects_unexpected_unresolved_design_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    _freeze_execution_amendment(inputs)
    unexpected = "one_second_python_cpp_parity_contract"
    audit = _small_training_audit(DAYS[0], missing=(unexpected,))
    monkeypatch.setattr(
        training.contract,
        "load_and_validate_training_design",
        lambda _: audit,
    )
    monkeypatch.setattr(training, "HEAD_SPECS", {})

    with pytest.raises(training.OneSecondTrainingError, match="unresolved.*parity"):
        training.train_research_bundle(
            inputs.days,
            matrix_cache_dir=inputs.matrix_cache_dir,
            output_dir=tmp_path / "must-not-be-created",
            training_design_path=inputs.training_design_path,
            execution_amendment_path=inputs.amendment_path,
        )


def test_training_bundle_identity_binds_execution_amendment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    amendment = _freeze_execution_amendment(inputs)
    allowed_missing = (
        "model_output_identity",
        "one_second_feature_panel_manifest",
        "training_implementation_sha256",
    )
    audit = _small_training_audit(DAYS[0], missing=allowed_missing)
    monkeypatch.setattr(
        training.contract,
        "load_and_validate_training_design",
        lambda _: audit,
    )
    monkeypatch.setattr(training, "HEAD_SPECS", {})

    bundle = training.train_research_bundle(
        inputs.days,
        matrix_cache_dir=inputs.matrix_cache_dir,
        output_dir=tmp_path / "research-bundle",
        training_design_path=inputs.training_design_path,
        execution_amendment_path=inputs.amendment_path,
    )

    assert (
        bundle["execution_amendment"]["execution_identity_sha256"]
        == amendment["execution_identity_sha256"]
    )
    assert bundle["execution_amendment"]["path"] == str(inputs.amendment_path.resolve())
    assert bundle["execution_amendment"]["sha256"] == _sha256(inputs.amendment_path)
    assert bundle["training_identity"]["execution_amendment"] == bundle["execution_amendment"]


def test_execution_amendment_validation_rejects_lightgbm_runtime_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    _freeze_execution_amendment(inputs)
    monkeypatch.setattr(training.lgb, "__version__", "999.0-test-drift")

    with pytest.raises(training.OneSecondTrainingError, match="runtime.*drift"):
        training.validate_training_execution_amendment(
            inputs.amendment_path,
            inputs.days,
            matrix_cache_dir=inputs.matrix_cache_dir,
            training_design_path=inputs.training_design_path,
        )


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("atomic_admission", False, "not atomically admitted"),
        ("schema_version", "wrong.matrix.schema", "schema version mismatch"),
    ],
)
def test_execution_amendment_freeze_rejects_invalid_matrix_manifest_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: Any,
    expected: str,
) -> None:
    inputs = _create_execution_inputs(tmp_path, monkeypatch)
    manifest_path = inputs.matrix_cache_dir / training.MATRIX_MANIFEST_FILENAME
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload[field] = value
    _write_json(manifest_path, payload)
    _admit_manifest(
        inputs.matrix_cache_dir,
        training.MATRIX_MANIFEST_FILENAME,
        training.SUCCESS_FILENAME,
    )

    with pytest.raises(training.OneSecondTrainingError, match=expected):
        _freeze_execution_amendment(inputs)
