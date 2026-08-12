import hashlib
import json

import numpy as np
import pandas as pd
import pytest

import models.backtest_tick as backtest_tick
import research.families.f03_causal_13_head.ml_model as ml_model
from research.families.f03_causal_13_head.ml_model import drop_all_missing_training_features
from strategy.model_contract import (
    REQUIRED_FEATURE_DAG_ID,
    REQUIRED_FEATURE_DAG_SHA256,
    REQUIRED_FEATURE_SEMANTICS_VERSION,
    REQUIRED_MODEL_HEADS,
)
from strategy.signal import FEATURE_NAMES_BASE, SignalEngine


class _ConstantModel:
    def __init__(self, value: float):
        self.value = value

    def predict(self, values):
        assert values.shape == (1, len(FEATURE_NAMES_BASE))
        return np.array([self.value], dtype=np.float64)


def test_all_missing_training_feature_is_removed_from_every_split() -> None:
    train = pd.DataFrame(
        {
            "usable": [1.0, np.nan],
            "offline_missing": [np.nan, np.nan],
            "label_dir_10s": [0.0, 1.0],
            "sample_weight": [1.0, 1.0],
        }
    )
    valid = train.copy()
    valid["offline_missing"] = [2.0, 3.0]
    dropped = drop_all_missing_training_features(train, valid)
    assert dropped == ["offline_missing"]
    assert "offline_missing" not in train
    assert "offline_missing" not in valid
    assert "usable" in train


def test_training_identity_propagates_feature_manifest_semantics(
    tmp_path, monkeypatch
) -> None:
    manifest = tmp_path / "causal_feature_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "feature_timestamp_semantics": "left_label_bucket_end",
                "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
                "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
                "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
                "feature_cutoff_semantics": (
                    "strict_exclusive_completed_bucket_end"
                ),
                "calendar_timestamp_semantics": "preserve_datetime_physical_unit",
                "microstructure_5s_semantics": "trailing_five_seconds",
                "label_semantics_version": 3,
                "label_window_semantics": "left_closed_right_open_[t,t+h)",
                "label_quote_calibration": {
                    "schema_version": "narrowgate_p3_touch_calibration.v2",
                    "model_type": "empirical_survival",
                    "sha256": "p3-sha",
                    "p3_delta_star": 14.0,
                    "p3_kappa_eff": 0.067,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(ml_model, "DATA_DIR", tmp_path)

    identity = ml_model._feature_panel_identity()

    assert identity["feature_semantics_version"] == REQUIRED_FEATURE_SEMANTICS_VERSION
    assert identity["feature_dag_id"] == REQUIRED_FEATURE_DAG_ID
    assert identity["feature_dag_sha256"] == REQUIRED_FEATURE_DAG_SHA256
    assert identity["calendar_timestamp_semantics"] == (
        "preserve_datetime_physical_unit"
    )
    assert identity["microstructure_5s_semantics"] == "trailing_five_seconds"
    assert identity["label_semantics_version"] == 3
    assert identity["label_window_semantics"] == "left_closed_right_open_[t,t+h)"


def test_training_rejects_pre_cutoff_feature_identity(tmp_path, monkeypatch) -> None:
    manifest = tmp_path / "causal_feature_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "feature_timestamp_semantics": "left_label_bucket_end",
                "feature_semantics_version": 5,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(ml_model, "DATA_DIR", tmp_path)

    with pytest.raises(RuntimeError, match="feature semantics v6"):
        ml_model._feature_panel_identity()


def test_causal_model_bundle_resolves_matching_feature_manifest(tmp_path, monkeypatch) -> None:
    feature_dir = tmp_path / "features"
    model_dir = tmp_path / "models"
    feature_dir.mkdir()
    model_dir.mkdir()
    manifest = feature_dir / "causal_feature_manifest.json"
    manifest.write_text('{"schema_version": 2}', encoding="utf-8")
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    (model_dir / "dir_10s_meta.json").write_text(
        json.dumps(
            {
                "feature_manifest_path": str(manifest),
                "feature_manifest_sha256": digest,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(backtest_tick, "MODEL_DIR", model_dir)
    monkeypatch.setattr(backtest_tick, "FEATURES_DIR", tmp_path / "legacy")
    monkeypatch.delenv("MM_FEATURE_DIR", raising=False)
    assert backtest_tick.resolve_ml_feature_dir() == feature_dir.resolve()


def test_causal_model_bundle_rejects_wrong_feature_manifest(tmp_path, monkeypatch) -> None:
    feature_dir = tmp_path / "features"
    model_dir = tmp_path / "models"
    feature_dir.mkdir()
    model_dir.mkdir()
    (feature_dir / "causal_feature_manifest.json").write_text("{}", encoding="utf-8")
    (model_dir / "dir_10s_meta.json").write_text(
        json.dumps(
            {
                "feature_manifest_path": str(feature_dir / "causal_feature_manifest.json"),
                "feature_manifest_sha256": "not-the-real-hash",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(backtest_tick, "MODEL_DIR", model_dir)
    monkeypatch.setenv("MM_FEATURE_DIR", str(feature_dir))
    with np.testing.assert_raises_regex(RuntimeError, "Feature manifest hash mismatch"):
        backtest_tick.resolve_ml_feature_dir()


def test_live_prediction_uses_canonical_features_without_ret_stacking() -> None:
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    engine._enable_ml = True
    engine._models = {
        name: _ConstantModel(
            0.001 if name.startswith("ret_") else
            3.0 if name.startswith("vol_") else
            0.5
        )
        for name in REQUIRED_MODEL_HEADS
    }
    engine._models["dir_10s"] = _ConstantModel(0.6)
    engine._model_feature_cols = {
        name: list(FEATURE_NAMES_BASE) for name in REQUIRED_MODEL_HEADS
    }
    features = {name: float(index) for index, name in enumerate(FEATURE_NAMES_BASE)}

    prediction = engine._predict(features)

    assert prediction.ret_10s == 0.001
    assert prediction.dir_10s == 0.6
    assert prediction.features.shape == (len(FEATURE_NAMES_BASE),)
    assert prediction.feature_dict == features
    assert not any(name.startswith("stacked_ret_") for name in prediction.feature_dict)


def test_live_prediction_fails_closed_when_runtime_feature_is_missing() -> None:
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    engine._enable_ml = True
    engine._models = {name: _ConstantModel(0.5) for name in REQUIRED_MODEL_HEADS}
    engine._model_feature_cols = {
        name: list(FEATURE_NAMES_BASE) for name in REQUIRED_MODEL_HEADS
    }
    features = {name: 0.0 for name in FEATURE_NAMES_BASE}
    features.pop(FEATURE_NAMES_BASE[-1])

    with pytest.raises(RuntimeError, match="runtime model feature contract missing"):
        engine._predict(features)
