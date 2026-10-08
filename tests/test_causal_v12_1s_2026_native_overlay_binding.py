from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_2026_native_execution_prep as prep,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_2026_native_overlay_binding as binding,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_full_schema as full_schema,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_prediction_overlay as overlays,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import causal_v12_1s_training as training

ROWS = 2


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _days(count: int) -> list[str]:
    start = datetime(2026, 4, 1, tzinfo=UTC)
    return [(start + timedelta(days=index)).date().isoformat() for index in range(count)]


def _write_panel(
    panel_dir: Path,
    day: str,
    *,
    artifact_schema_version: str = panels.ARTIFACT_SCHEMA_VERSION,
) -> dict[str, object]:
    panel_dir.mkdir(parents=True)
    day_start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000)
    timestamps = day_start + np.arange(ROWS, dtype=np.int64) * 1_000
    values: dict[str, pa.Array] = {
        "cutoff_exclusive_ms": pa.array(timestamps),
        "decision_ts_ms": pa.array(timestamps),
        "feature_ready_ts_ms": pa.array(timestamps - 1),
        "unsupported_feature_count": pa.array(np.zeros(ROWS, dtype=np.int16)),
        "feature_row_fingerprint_sha256": pa.array(
            [hashlib.sha256(f"{day}:{index}".encode()).hexdigest() for index in range(ROWS)]
        ),
        "local_bar_lag_state": pa.array(["observed_completed_1s"] * ROWS),
        "local_synthetic_seconds_24h": pa.array(np.zeros(ROWS, dtype=np.int32)),
        "reference_bar_lag_state": pa.array(["source_unavailable"] * ROWS),
        "reference_synthetic_seconds_1h": pa.array(np.zeros(ROWS, dtype=np.int32)),
    }
    for index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        values[name] = pa.array(np.full(ROWS, float(index)), type=pa.float64())
    panel_path = panel_dir / panels.PANEL_FILENAME
    pq.write_table(pa.Table.from_pydict(values, schema=panels.panel_arrow_schema()), panel_path)
    cache_payload = {
        "feature_contract_sha256": full_schema.full_feature_contract_fingerprint(),
        "source_manifest_sha256": schema.canonical_sha256(schema.source_manifest_payload()),
        "feature_order_sha256": schema.feature_order_sha256(),
        "fixture": day,
    }
    panel_schema = panels.panel_schema_payload()
    if artifact_schema_version == binding.LEGACY_FEATURE_ARTIFACT_SCHEMA_VERSION:
        panel_schema = {
            **panel_schema,
            "schema_version": binding.LEGACY_FEATURE_ARTIFACT_SCHEMA_VERSION,
        }
    manifest = {
        "schema_version": artifact_schema_version,
        "identity": schema.IDENTITY,
        "utc_day": day,
        "cache_identity_payload": cache_payload,
        "cache_identity_sha256": binding._canonical_sha256(cache_payload),
        "panel_schema": panel_schema,
        "panel": {
            "path": panels.PANEL_FILENAME,
            "sha256": _sha256(panel_path),
            "rows": ROWS,
        },
        "atomic_admission": True,
        "labels_read": False,
        "economic_outcomes_read": False,
    }
    manifest_path = panel_dir / panels.MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (panel_dir / panels.SUCCESS_FILENAME).write_text(_sha256(manifest_path) + "\n")
    return {
        "feature_panel_dir": str(panel_dir),
        "feature_manifest_path": str(manifest_path),
        "feature_manifest_sha256": _sha256(manifest_path),
        "feature_panel_path": str(panel_path),
        "feature_panel_sha256": _sha256(panel_path),
        "feature_cache_identity_sha256": manifest["cache_identity_sha256"],
    }


def test_exact_legacy_v3_panel_projection_is_accepted(tmp_path: Path) -> None:
    day = "2026-04-17"
    panel_dir = tmp_path / day
    row = _write_panel(
        panel_dir,
        day,
        artifact_schema_version=binding.LEGACY_FEATURE_ARTIFACT_SCHEMA_VERSION,
    )
    manifest = binding._validate_feature_manifest(
        Path(str(row["feature_manifest_path"])),
        Path(str(row["feature_panel_path"])),
        expected_day=day,
        expected_manifest_sha256=str(row["feature_manifest_sha256"]),
        expected_panel_sha256=str(row["feature_panel_sha256"]),
        expected_rows=ROWS,
    )
    assert manifest["schema_version"] == binding.LEGACY_FEATURE_ARTIFACT_SCHEMA_VERSION


def test_legacy_v3_panel_with_schema_drift_fails_closed(tmp_path: Path) -> None:
    day = "2026-04-17"
    panel_dir = tmp_path / day
    row = _write_panel(
        panel_dir,
        day,
        artifact_schema_version=binding.LEGACY_FEATURE_ARTIFACT_SCHEMA_VERSION,
    )
    manifest_path = Path(str(row["feature_manifest_path"]))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["panel_schema"]["feature_count"] = 172
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (panel_dir / panels.SUCCESS_FILENAME).write_text(_sha256(manifest_path) + "\n")
    with pytest.raises(binding.NativeOverlayBindingError, match="feature schema drift"):
        binding._validate_feature_manifest(
            manifest_path,
            Path(str(row["feature_panel_path"])),
            expected_day=day,
            expected_manifest_sha256=_sha256(manifest_path),
            expected_panel_sha256=str(row["feature_panel_sha256"]),
            expected_rows=ROWS,
        )


def _write_prep(market_root: Path) -> Path:
    prep_root = market_root / "cache" / "native-prep"
    rows = []
    days = _days(binding.EXPECTED_DAY_COUNT)
    for ordinal, day in enumerate(days, start=1):
        panel = _write_panel(prep_root / "feature-panels" / day, day)
        rows.append(
            {
                "ordinal": ordinal,
                "utc_day": day,
                **panel,
                "feature_rows": ROWS,
            }
        )
    identity_payload = {
        "cache_root": str(prep_root),
        "days": days,
        "feature_panels": rows,
        "model_bundle": None,
    }
    manifest = {
        "schema_version": prep.MANIFEST_SCHEMA_VERSION,
        "identity": prep.IDENTITY,
        "status": "feature_panels_complete_model_bundle_unbound",
        "completed_day_count": binding.EXPECTED_DAY_COUNT,
        "model_bundle_bound": False,
        "execution_prep_identity_sha256": binding._canonical_sha256(identity_payload),
        "identity_payload": identity_payload,
        "economic_outcomes_read": False,
    }
    manifest_path = prep_root / prep.MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (prep_root / prep.SUCCESS_FILENAME).write_text(_sha256(manifest_path) + "\n")
    return prep_root


def _head_metadata(head: str, model_sha: str) -> dict[str, object]:
    label, objective, metric, _ = training.HEAD_SPECS[head]
    return {
        "schema_version": overlays.HEAD_META_SCHEMA_VERSION,
        "name": head,
        "label_col": label,
        "objective": objective,
        "metric": metric,
        "feature_cols": list(schema.TRAINABLE_FEATURE_ORDER),
        "feature_count": len(schema.TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": schema.feature_order_sha256(),
        "feature_bucket_ms": 1_000,
        "feature_timestamp_semantics": "canonical_1s_decision_ready_at_boundary",
        "model_sha256": model_sha,
        "research_only": True,
        "prediction_authority": False,
        "action_authority": False,
        "live_authority": False,
    }


def _write_bundle(root: Path, *, cadence_ms: int = 1_000) -> Path:
    bundle_dir = root / "bundle"
    bundle_dir.mkdir()
    heads: dict[str, object] = {}
    for head in training.HEAD_SPECS:
        model_path = bundle_dir / f"{head}.txt"
        model_path.write_text(f"fixture:{head}\n", encoding="ascii")
        metadata_path = bundle_dir / f"{head}_meta.json"
        metadata_path.write_text(
            json.dumps(_head_metadata(head, _sha256(model_path)), sort_keys=True),
            encoding="utf-8",
        )
        heads[head] = {
            "model": {"path": model_path.name, "sha256": _sha256(model_path)},
            "metadata": {"path": metadata_path.name, "sha256": _sha256(metadata_path)},
        }
    training_days = _days(66)
    identity = {
        "identity": schema.IDENTITY,
        "inference_cadence_ms": cadence_ms,
        "heads": list(training.HEAD_SPECS),
        "feature_order": list(schema.TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": schema.feature_order_sha256(),
        "daily_artifacts": [
            {
                "utc_day": day,
                "feature_panel_dir": f"/fixture/{day}",
                "feature_manifest_sha256": "1" * 64,
                "feature_panel_sha256": "2" * 64,
                "label_overlay_dir": f"/fixture-label/{day}",
                "overlay_manifest_sha256": "3" * 64,
                "overlay_sha256": "4" * 64,
            }
            for day in training_days
        ],
        "economic_outcomes_read": False,
        "external_2026_panels_read": False,
    }
    bundle = {
        "schema_version": training.BUNDLE_SCHEMA_VERSION,
        "identity": schema.IDENTITY,
        "status": "research_only_transport_and_economics_not_run",
        "training_identity": identity,
        "training_identity_sha256": binding._canonical_sha256(identity),
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
    (bundle_dir / training.SUCCESS_FILENAME).write_text(_sha256(bundle_path) + "\n")
    return bundle_dir


@pytest.fixture
def prepared_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    market_root = tmp_path / "market"
    market_root.mkdir()
    prep_root = _write_prep(market_root)
    bundle_dir = _write_bundle(tmp_path)
    monkeypatch.setattr(
        binding,
        "_validate_training_feature_inputs",
        lambda _: {
            "day_count": 66,
            "first_day": "2025-08-02",
            "last_day": "2025-12-28",
            "feature_artifact_set_sha256": "5" * 64,
        },
    )
    monkeypatch.setattr(
        binding.shutil,
        "disk_usage",
        lambda _: SimpleNamespace(free=1_000 * binding.GIB),
    )
    return market_root, prep_root, bundle_dir


def test_missing_candidate_bundle_fails_before_output(tmp_path: Path) -> None:
    market_root = tmp_path / "market"
    market_root.mkdir()
    output_root = market_root / "cache" / "output"
    with pytest.raises(binding.NativeOverlayBindingError, match="bundle is required"):
        binding.prepare_execution_plan(
            research_bundle_dir=None,
            prep_root=market_root / "cache" / "prep",
            output_root=output_root,
            market_data_root=market_root,
        )
    assert not output_root.exists()


def test_plan_strictly_binds_bundle_dag_clocks_and_ordered_40_days(
    prepared_inputs: tuple[Path, Path, Path],
) -> None:
    market_root, prep_root, bundle_dir = prepared_inputs
    output_root = market_root / "cache" / "overlays"
    plan = binding.prepare_execution_plan(
        research_bundle_dir=bundle_dir,
        prep_root=prep_root,
        output_root=output_root,
        market_data_root=market_root,
        test_only_row_count=ROWS,
    )
    payload = plan["identity_payload"]
    assert plan["day_count"] == 40
    assert [row["utc_day"] for row in payload["days"]] == _days(40)
    assert payload["research_bundle"]["head_count"] == 13
    assert payload["feature_contract"]["cadence_ms"] == 1_000
    assert payload["feature_contract"]["feature_dag_id"] == schema.FEATURE_DAG_ID
    assert len(payload["feature_contract"]["source_clock_contract"]["nodes"]) == 173
    assert all(binding._is_sha256(row["row_identity_sha256"]) for row in payload["days"])
    assert plan["economic_outcomes_read"] is False
    assert plan["market_features_copied_to_overlay"] is False
    validated = binding.validate_execution_plan(
        output_root / binding.PLAN_FILENAME,
        market_data_root=market_root,
    )
    assert validated["plan_identity_sha256"] == plan["plan_identity_sha256"]


def test_bundle_cadence_drift_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    market_root = tmp_path / "market"
    market_root.mkdir()
    prep_root = _write_prep(market_root)
    bundle_dir = _write_bundle(tmp_path, cadence_ms=10_000)
    monkeypatch.setattr(binding.shutil, "disk_usage", lambda _: SimpleNamespace(free=1000 << 30))
    with pytest.raises(binding.NativeOverlayBindingError, match="cadence"):
        binding.prepare_execution_plan(
            research_bundle_dir=bundle_dir,
            prep_root=prep_root,
            output_root=market_root / "cache" / "output",
            market_data_root=market_root,
            test_only_row_count=ROWS,
        )


def _write_fake_overlay(
    *, feature_panel_dir: Path, research_bundle_dir: Path, output_dir: Path, **_: object
) -> SimpleNamespace:
    output_dir.mkdir(parents=True, exist_ok=True)
    feature = pq.read_table(
        feature_panel_dir / panels.PANEL_FILENAME,
        columns=list(overlays.JOIN_COLUMNS),
    )
    arrays = [feature[name] for name in overlays.JOIN_COLUMNS]
    for index, _head in enumerate(overlays.HEADS):
        arrays.append(pa.array(np.full(ROWS, index / 100.0), type=pa.float64()))
    table = pa.Table.from_arrays(arrays, schema=overlays.prediction_overlay_arrow_schema())
    overlay_path = output_dir / overlays.OVERLAY_FILENAME
    pq.write_table(table, overlay_path)
    bundle_sha = _sha256(research_bundle_dir / "bundle_meta.json")
    manifest = {
        "schema_version": overlays.ARTIFACT_SCHEMA_VERSION,
        "utc_day": feature_panel_dir.name,
        "research_bundle_sha256": bundle_sha,
        "feature_panel_sha256": _sha256(feature_panel_dir / panels.PANEL_FILENAME),
        "cache_identity_sha256": hashlib.sha256(feature_panel_dir.name.encode()).hexdigest(),
        "overlay": {
            "path": overlays.OVERLAY_FILENAME,
            "sha256": _sha256(overlay_path),
            "rows": ROWS,
        },
    }
    manifest_path = output_dir / overlays.MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (output_dir / overlays.SUCCESS_FILENAME).write_text(_sha256(manifest_path) + "\n")
    return SimpleNamespace(reused=False, row_count=ROWS)


def test_materializer_is_atomic_resume_safe_and_never_copies_features(
    prepared_inputs: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_root, prep_root, bundle_dir = prepared_inputs
    output_root = market_root / "cache" / "overlays"
    plan = binding.prepare_execution_plan(
        research_bundle_dir=bundle_dir,
        prep_root=prep_root,
        output_root=output_root,
        market_data_root=market_root,
        test_only_row_count=ROWS,
    )
    monkeypatch.setattr(overlays, "materialize_daily_prediction_overlay", _write_fake_overlay)
    partial = binding.materialize_execution_plan(
        Path(plan["execution_plan_path"]),
        days=_days(1),
        market_data_root=market_root,
    )
    assert partial["completed_day_count"] == 1
    assert partial["execution_input_eligible"] is False
    complete = binding.materialize_execution_plan(
        Path(plan["execution_plan_path"]),
        market_data_root=market_root,
    )
    assert complete["completed_day_count"] == 40
    assert complete["total_rows"] == 80
    assert complete["execution_input_eligible"] is True
    assert complete["economic_outcomes_read"] is False
    assert complete["market_features_copied_to_overlay"] is False
    overlay_schema = pq.ParquetFile(
        output_root / "overlays" / _days(1)[0] / overlays.OVERLAY_FILENAME
    ).schema_arrow
    assert set(overlay_schema.names) == {
        *overlays.JOIN_COLUMNS,
        *overlays.PREDICTION_COLUMN_BY_HEAD.values(),
    }
    assert not set(schema.TRAINABLE_FEATURE_ORDER) & set(overlay_schema.names)


def test_source_has_no_economic_execution_mode() -> None:
    source = Path(binding.__file__).read_text(encoding="utf-8")
    assert "read-pnl" not in source
    assert "run-economic" not in source
    assert '"economic_outcomes_read": False' in source
