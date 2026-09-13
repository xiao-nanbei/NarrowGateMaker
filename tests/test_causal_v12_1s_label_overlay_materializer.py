from __future__ import annotations

import json
import shutil
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as sources,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_generator as labels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_overlay_materializer as materializer,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

DAY = "2025-08-02"
DAY_START_MS = int(pd.Timestamp(DAY, tz="UTC").timestamp() * 1_000)


def _sha256(path: Path) -> str:
    return sources.sha256_file(path)


def _write_local_bars(path: Path) -> None:
    offset = np.arange(materializer.AUTHORITATIVE_DAILY_ROWS, dtype=np.int64)
    close = 65_000.0 + (offset % 100) * 0.001
    frame = pd.DataFrame(
        {
            "timestamp": DAY_START_MS + offset * 1_000,
            "open": close,
            "high": close + 0.1,
            "low": close - 0.1,
            "close": close,
            "volume": np.ones(len(offset)),
            "buy_qty": np.full(len(offset), 0.55),
            "sell_qty": np.full(len(offset), 0.45),
            "trade_count": np.ones(len(offset), dtype=np.int64),
            "buy_trade_count": np.ones(len(offset), dtype=np.int64),
            "sell_trade_count": np.zeros(len(offset), dtype=np.int64),
            "buy_quote_qty": close * 0.55,
            "sell_quote_qty": close * 0.45,
            "max_same_side_run": np.ones(len(offset), dtype=np.int64),
            "buy_price_high": close,
            "buy_price_low": close,
            "sell_price_high": close,
            "sell_price_low": close,
        }
    )
    frame.to_parquet(path, index=False, compression="zstd")


def _panel_batch(start: int, count: int) -> pa.Table:
    decision = DAY_START_MS + np.arange(start, start + count, dtype=np.int64) * 1_000
    values: dict[str, pa.Array] = {
        "cutoff_exclusive_ms": pa.array(decision, type=pa.int64()),
        "decision_ts_ms": pa.array(decision, type=pa.int64()),
        "feature_ready_ts_ms": pa.array(decision, type=pa.int64()),
        "unsupported_feature_count": pa.array(np.zeros(count, dtype=np.int16)),
        "feature_row_fingerprint_sha256": pa.array(["a" * 64] * count),
        "local_bar_lag_state": pa.array(["observed_completed_1s"] * count),
        "local_synthetic_seconds_24h": pa.array(np.zeros(count, dtype=np.int32)),
        "reference_bar_lag_state": pa.array(["source_unavailable"] * count),
        "reference_synthetic_seconds_1h": pa.array(np.zeros(count, dtype=np.int32)),
    }
    for name in schema.TRAINABLE_FEATURE_ORDER:
        value = 65_000.0 if name == "close" else 0.0
        values[name] = pa.array(np.full(count, value, dtype=np.float64))
    return pa.Table.from_pydict(values, schema=panels.panel_arrow_schema())


def _write_panel(path: Path) -> None:
    writer = pq.ParquetWriter(path, panels.panel_arrow_schema(), compression="zstd")
    try:
        for start in range(0, materializer.AUTHORITATIVE_DAILY_ROWS, 7_200):
            count = min(7_200, materializer.AUTHORITATIVE_DAILY_ROWS - start)
            writer.write_table(_panel_batch(start, count))
    finally:
        writer.close()


def _fake_overlay(feature_panel: pd.DataFrame, bars: pd.DataFrame, **kwargs) -> pd.DataFrame:
    assert len(feature_panel) == materializer.AUTHORITATIVE_DAILY_ROWS
    assert len(bars) == materializer.AUTHORITATIVE_DAILY_ROWS
    assert kwargs["target_utc_day"] == DAY
    output = feature_panel[list(materializer.JOIN_COLUMNS)].copy()
    for head in materializer.HEADS:
        output[labels.LABEL_COLUMN_BY_HEAD[head]] = 0.25
        output[f"label_valid__{head}"] = True
        output[f"sample_weight__{head}"] = 1.0
        output[f"overlap_uniqueness__{head}"] = 0.5
    return output


@pytest.fixture(scope="module")
def admitted_inputs(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("f03-label-overlay")
    local_path = root / f"BTCUSDC-trade-tempo-{DAY}.parquet"
    _write_local_bars(local_path)
    bundle = sources.DailySourceBundle(
        utc_day=DAY,
        local_trade_tempo_paths=(local_path,),
        execution_l2_clock_identity="fixture_no_l2",
    )

    panel_dir = root / "feature-panel"
    panel_dir.mkdir()
    panel_path = panel_dir / panels.PANEL_FILENAME
    _write_panel(panel_path)
    cache_payload = {
        "bundle_identity_sha256": bundle.identity_sha256(),
    }
    manifest = {
        "schema_version": panels.ARTIFACT_SCHEMA_VERSION,
        "identity": schema.IDENTITY,
        "status": "materialized_not_training_or_live_authorized",
        "utc_day": DAY,
        "cache_identity_sha256": schema.canonical_sha256(cache_payload),
        "cache_identity_payload": cache_payload,
        "source_bundle": bundle.identity_payload(),
        "panel_schema": panels.panel_schema_payload(),
        "panel": {
            "path": panels.PANEL_FILENAME,
            "sha256": _sha256(panel_path),
            "size_bytes": panel_path.stat().st_size,
            "rows": materializer.AUTHORITATIVE_DAILY_ROWS,
        },
        "atomic_admission": True,
    }
    manifest_path = panel_dir / panels.MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (panel_dir / panels.SUCCESS_FILENAME).write_text(
        _sha256(manifest_path) + "\n", encoding="ascii"
    )
    model_dir = root / "model"
    model_dir.mkdir()
    p3 = model_dir / "fill_prob_params.json"
    p3.write_text('{"identity":"empirical_p3_v2"}\n', encoding="utf-8")
    quote_config = root / "label_quote_config.yaml"
    quote_config.write_text(
        f"symbol: BTCUSDC\nml:\n  model_dir: {model_dir}\n",
        encoding="utf-8",
    )
    return root, bundle, panel_dir, quote_config, p3


def test_atomic_overlay_is_label_only_and_hash_reusable(
    admitted_inputs,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, bundle, panel_dir, quote_config, p3 = admitted_inputs
    output_dir = root / "overlay-success"
    monkeypatch.setattr(labels, "generate_daily_1s_labels", _fake_overlay)

    result = materializer.materialize_daily_label_overlay(
        bundle,
        feature_panel_dir=panel_dir,
        output_dir=output_dir,
        quote_config_path=quote_config,
        p3_v2_artifact_path=p3,
    )

    assert not result.reused
    assert result.row_count == 86_400
    assert (output_dir / materializer.SUCCESS_FILENAME).read_text().strip() == _sha256(
        output_dir / materializer.MANIFEST_FILENAME
    )
    parquet = pq.ParquetFile(result.overlay_path)
    assert parquet.metadata.num_rows == 86_400
    assert parquet.schema_arrow.equals(materializer.overlay_arrow_schema())
    assert not (set(schema.TRAINABLE_FEATURE_ORDER) & set(parquet.schema_arrow.names))
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["feature_panel_manifest_sha256"] == _sha256(
        panel_dir / panels.MANIFEST_FILENAME
    )
    assert manifest["feature_panel_sha256"] == _sha256(panel_dir / panels.PANEL_FILENAME)
    assert manifest["source_bundle_identity_sha256"] == bundle.identity_sha256()
    assert manifest["label_generator_sha256"] == _sha256(Path(labels.__file__).resolve())
    assert manifest["label_quote_config_sha256"] == _sha256(quote_config)
    assert manifest["p3_v2_artifact_sha256"] == _sha256(p3)
    assert manifest["predictions_read"] is False
    assert manifest["economic_outcomes_read"] is False
    assert manifest["training_authorized"] is False
    assert manifest["live_authorized"] is False

    reused = materializer.materialize_daily_label_overlay(
        bundle,
        feature_panel_dir=panel_dir,
        output_dir=output_dir,
        quote_config_path=quote_config,
        p3_v2_artifact_path=p3,
    )
    assert reused.reused
    assert reused.cache_identity_sha256 == result.cache_identity_sha256


def test_rejects_source_bundle_identity_drift(admitted_inputs) -> None:
    root, bundle, panel_dir, quote_config, p3 = admitted_inputs
    drifted = replace(bundle, local_source_identity="different_source_identity")
    with pytest.raises(
        materializer.LabelOverlayMaterializationError,
        match="source bundle identity differs",
    ):
        materializer.materialize_daily_label_overlay(
            drifted,
            feature_panel_dir=panel_dir,
            output_dir=root / "overlay-source-drift",
            quote_config_path=quote_config,
            p3_v2_artifact_path=p3,
        )


def test_rejects_incomplete_feature_panel_manifest(admitted_inputs, tmp_path: Path) -> None:
    _, _, panel_dir, _, _ = admitted_inputs
    copied = tmp_path / "incomplete-panel"
    shutil.copytree(panel_dir, copied)
    manifest_path = copied / panels.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["panel"]["rows"] = 86_399
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (copied / panels.SUCCESS_FILENAME).write_text(_sha256(manifest_path) + "\n", encoding="ascii")
    with pytest.raises(
        materializer.LabelOverlayMaterializationError,
        match="does not declare exactly 86,400 rows",
    ):
        materializer.load_admitted_feature_panel(copied)


def test_failure_never_admits_partial_output(
    admitted_inputs,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, bundle, panel_dir, quote_config, p3 = admitted_inputs
    output_dir = root / "overlay-failure"

    def malformed(*args, **kwargs):
        frame = _fake_overlay(*args, **kwargs)
        return frame.drop(columns=["sample_weight__dir_10s"])

    monkeypatch.setattr(labels, "generate_daily_1s_labels", malformed)
    with pytest.raises(
        materializer.LabelOverlayMaterializationError,
        match="exact successor schema",
    ):
        materializer.materialize_daily_label_overlay(
            bundle,
            feature_panel_dir=panel_dir,
            output_dir=output_dir,
            quote_config_path=quote_config,
            p3_v2_artifact_path=p3,
        )
    assert not output_dir.exists()
    assert not list(root.glob(f".{output_dir.name}.tmp-*"))


def test_existing_output_must_match_all_bound_hashes(
    admitted_inputs,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _, bundle, panel_dir, quote_config, p3 = admitted_inputs
    output_dir = tmp_path / "overlay"
    monkeypatch.setattr(labels, "generate_daily_1s_labels", _fake_overlay)
    materializer.materialize_daily_label_overlay(
        bundle,
        feature_panel_dir=panel_dir,
        output_dir=output_dir,
        quote_config_path=quote_config,
        p3_v2_artifact_path=p3,
    )
    changed_p3 = tmp_path / "changed_p3.json"
    changed_p3.write_text('{"identity":"different"}\n', encoding="utf-8")
    with pytest.raises(
        materializer.LabelOverlayMaterializationError,
        match="config-resolved",
    ):
        materializer.materialize_daily_label_overlay(
            bundle,
            feature_panel_dir=panel_dir,
            output_dir=output_dir,
            quote_config_path=quote_config,
            p3_v2_artifact_path=changed_p3,
        )

    manifest_path = output_dir / materializer.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["action_authorized"] = True
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (output_dir / materializer.SUCCESS_FILENAME).write_text(
        _sha256(manifest_path) + "\n", encoding="ascii"
    )
    with pytest.raises(
        materializer.LabelOverlayMaterializationError,
        match="permission boundary",
    ):
        materializer.materialize_daily_label_overlay(
            bundle,
            feature_panel_dir=panel_dir,
            output_dir=output_dir,
            quote_config_path=quote_config,
            p3_v2_artifact_path=p3,
        )
