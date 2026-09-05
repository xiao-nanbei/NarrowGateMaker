import hashlib
import json

import numpy as np
import pandas as pd
import pytest

import models.backtest_tick as backtest_tick
import research.families.f03_causal_13_head.ml_model as ml_model
from research.families.f03_causal_13_head.ml_model import drop_all_missing_training_features
from strategy.model_contract import (
    LEGACY_OWNER_AUTHORIZED_LIVE_CANARY,
    PRIVATE_DEPLOYMENT_AUTHORITY,
    REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
    REQUIRED_FEATURE_DAG_ID,
    REQUIRED_FEATURE_DAG_SHA256,
    REQUIRED_FEATURE_SEMANTICS_VERSION,
    REQUIRED_MODEL_HEADS,
    absolute_price_variance_unit_contract,
    canonicalize_model_variance_unit_contract,
    f03_direct_quote_action_contract,
)
from strategy.quote_core import (
    QuoteCoreConfig,
    QuotePrediction,
    QuoteState,
    compute_quote_core,
)
from strategy.signal import FEATURE_NAMES_BASE, SignalEngine


class _ConstantModel:
    def __init__(self, value: float):
        self.value = value

    def predict(self, values):
        assert values.shape == (1, len(FEATURE_NAMES_BASE))
        return np.array([self.value], dtype=np.float64)


class _RecordingFeatureModel:
    def __init__(self, value: float, seen: list[np.ndarray]):
        self.value = value
        self.seen = seen

    def predict(self, values):
        self.seen.append(values)
        return np.array([self.value], dtype=np.float64)


_BTCUSDC_VARIANCE_UNITS = absolute_price_variance_unit_contract("BTCUSDC")


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
                "symbol": "BTCUSDC",
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
                "label_volatility_units": _BTCUSDC_VARIANCE_UNITS["variance_units"],
                "volatility_unit_contract": _BTCUSDC_VARIANCE_UNITS,
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
    assert identity["volatility_unit_contract"] == _BTCUSDC_VARIANCE_UNITS


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


def test_authorization_bound_legacy_metadata_canonicalizes_without_owner_hash() -> None:
    source = {
        "symbol": "BTCUSDC",
        "feature_manifest_sha256": "legacy-fixture-manifest",
        "training_experiment_id": "causal_v12_expanded_source_aware_semantics_v6",
        "promotion_authority": LEGACY_OWNER_AUTHORIZED_LIVE_CANARY,
        "source_profile": "all",
        "feature_variant": "base",
    }
    metadata = canonicalize_model_variance_unit_contract(
        source,
        legacy_authorization_contract=_BTCUSDC_VARIANCE_UNITS,
    )

    assert "volatility_unit_contract" not in source
    assert metadata["volatility_unit_contract"] == _BTCUSDC_VARIANCE_UNITS
    assert metadata["volatility_unit_contract_origin"] == (
        "legacy_authorization_manifest"
    )
    assert metadata["promotion_authority"] == PRIVATE_DEPLOYMENT_AUTHORITY
    assert (
        metadata["promotion_authority_origin"]
        == LEGACY_OWNER_AUTHORIZED_LIVE_CANARY
    )

    unregistered = dict(source)
    unregistered["feature_manifest_sha256"] = "unregistered"
    with pytest.raises(ValueError, match="unregistered legacy metadata"):
        canonicalize_model_variance_unit_contract(unregistered)


def test_legacy_f03_ret_name_does_not_imply_direct_quote_compatibility() -> None:
    legacy = {
        "name": "ret_10s",
        "label_semantics": (
            "fill_within_h_then_markout_h_after_fill; "
            "decision outcome spans h_to_2h"
        ),
    }

    assert f03_direct_quote_action_contract(legacy) == {
        "compatible": False,
        "horizon_s": 0.0,
    }


def test_f03_direct_quote_action_requires_complete_point_horizon_identity() -> None:
    metadata = {
        "direct_quote_action": {
            "schema_version": "narrowgate.f03.direct_quote_action.v1",
            "compatible": True,
            "event_type": "decision_to_fixed_horizon_return",
            "horizon_s": 10.0,
            "price_origin": "decision_mid",
            "return_unit": "fraction",
            "consumer": "quote_center_shift",
        }
    }

    assert f03_direct_quote_action_contract(metadata)["horizon_s"] == 10.0
    metadata["direct_quote_action"]["event_type"] = "fill_conditioned_markout"
    with pytest.raises(ValueError, match="event_type"):
        f03_direct_quote_action_contract(metadata)


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
    assert (
        backtest_tick.resolve_ml_feature_dir(require_training_panel=True) == feature_dir.resolve()
    )


def test_new_inference_panel_does_not_have_to_equal_training_manifest(
    tmp_path, monkeypatch,
) -> None:
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
    assert backtest_tick.resolve_ml_feature_dir() == feature_dir.resolve()
    with np.testing.assert_raises_regex(RuntimeError, "Feature manifest hash mismatch"):
        backtest_tick.resolve_ml_feature_dir(require_training_panel=True)


def _inference_panel_fixture(tmp_path, monkeypatch):
    import lightgbm as lgb

    panel = tmp_path / "new-dates"
    model = tmp_path / "models"
    panel.mkdir()
    model.mkdir()
    manifest = {
        "schema_version": 3,
        "symbol": "BTCUSDC",
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
        "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
        "feature_bucket_ms": 10000,
        "feature_ready_offset_ms": 10000,
        "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
        "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
        "microstructure_5s_semantics": (
            "trailing_five_seconds_from_causal_left_labelled_1s_bars"
        ),
        "labels_materialized": False,
        "market_stage": "minimal",
        "reference_symbol": "BTCUSDT",
        "daily_files": [{"day": "2099-01-02"}],
    }
    (panel / "causal_feature_manifest.json").write_text(json.dumps(manifest))
    frame = pd.DataFrame(
        {"a": [0.0, 1.0, 2.0], "b": [2.0, 1.0, 0.0]},
        index=pd.date_range("2099-01-02", periods=3, freq="10s", tz="UTC"),
    )
    frame.to_parquet(panel / "features_2099-01-02.parquet")
    booster = lgb.train(
        {"objective": "regression", "verbose": -1, "num_threads": 1,
         "min_data_in_leaf": 1, "num_leaves": 2},
        lgb.Dataset(frame, label=[0.1, 0.5, 0.9]), num_boost_round=1,
    )
    booster.save_model(str(model / "dir_10s.txt"))
    metadata = {
        **{k: v for k, v in manifest.items() if k not in {"daily_files", "labels_materialized"}},
        "feature_cols": ["a", "b"],
        "feature_manifest_path": str(tmp_path / "training" / "causal_feature_manifest.json"),
        "feature_manifest_sha256": "f" * 64,
    }
    meta_path = model / "dir_10s_meta.json"
    meta_path.write_text(json.dumps(metadata))
    monkeypatch.setattr(backtest_tick, "MODEL_DIR", model)
    monkeypatch.setattr(backtest_tick, "SYMBOL", "BTCUSDC")
    monkeypatch.delenv("MM_FEATURE_DIR", raising=False)
    monkeypatch.delenv("MM_FEATURE_WARMUP_DIR", raising=False)
    start = pd.Timestamp("2099-01-02", tz="UTC").value // 1_000_000
    trades = pd.DataFrame({"transact_time": [start, start + 30_000]})
    return panel, manifest, meta_path, metadata, trades


def test_new_dates_inference_uses_frozen_model_with_compatible_actual_panel(tmp_path, monkeypatch):
    panel, _, _, _, trades = _inference_panel_fixture(tmp_path, monkeypatch)
    result = backtest_tick.load_ml_predictions(trades, feature_dir=panel)
    assert result is not None
    assert len(result[0]) == 3
    assert np.all(np.isfinite(result[1]))


@pytest.mark.parametrize(("field", "value"), [
    ("symbol", "ETHUSDC"), ("feature_semantics_version", 5),
    ("feature_dag_id", "another-graph"), ("feature_dag_sha256", "0" * 64),
    ("feature_bucket_ms", 1000), ("feature_ready_offset_ms", 0),
    ("feature_cutoff_semantics", "inclusive"),
    ("calendar_timestamp_semantics", "raw-integer-guessed-unit"),
    ("market_stage", "another-stage"), ("reference_symbol", "ETHUSDT"),
])
def test_explicit_inference_panel_cannot_bypass_semantic_checks(
    tmp_path, monkeypatch, field, value,
):
    panel, manifest, _, _, trades = _inference_panel_fixture(tmp_path, monkeypatch)
    manifest[field] = value
    (panel / "causal_feature_manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match=f"incompatible (?:inference )?{field}"):
        backtest_tick.load_ml_predictions(trades, feature_dir=panel)


def test_inference_warmup_panel_cannot_bypass_semantic_checks(tmp_path, monkeypatch):
    panel, manifest, _, _, trades = _inference_panel_fixture(tmp_path, monkeypatch)
    warmup = tmp_path / "warmup"
    warmup.mkdir()
    manifest["feature_cutoff_semantics"] = "inclusive"
    (warmup / "causal_feature_manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("MM_FEATURE_WARMUP_DIR", str(warmup))
    with pytest.raises(RuntimeError, match="incompatible feature_cutoff_semantics"):
        backtest_tick.load_ml_predictions(trades, feature_dir=panel)


@pytest.mark.parametrize("columns", [["a"], ["b", "a"], ["a", "renamed"], ["a", "a"], []])
def test_inference_rejects_model_width_or_feature_name_mismatch(tmp_path, monkeypatch, columns):
    panel, _, meta_path, metadata, trades = _inference_panel_fixture(tmp_path, monkeypatch)
    metadata["feature_cols"] = columns
    meta_path.write_text(json.dumps(metadata))
    with pytest.raises(RuntimeError, match="model feature width/order differs from metadata"):
        backtest_tick.load_ml_predictions(trades, feature_dir=panel)


def test_inference_allows_extra_and_reordered_stored_feature_columns(tmp_path, monkeypatch):
    panel, _, _, _, trades = _inference_panel_fixture(tmp_path, monkeypatch)
    before = backtest_tick.load_ml_predictions(trades, feature_dir=panel)
    path = panel / "features_2099-01-02.parquet"
    frame = pd.read_parquet(path)
    frame["unconsumed"] = 99.0
    frame.loc[:, ["unconsumed", "b", "a"]].to_parquet(path)
    after = backtest_tick.load_ml_predictions(trades, feature_dir=panel)
    np.testing.assert_array_equal(after[1], before[1])


def test_inference_rejects_duplicate_actual_feature_columns(tmp_path, monkeypatch):
    panel, _, _, _, trades = _inference_panel_fixture(tmp_path, monkeypatch)
    path = panel / "features_2099-01-02.parquet"
    duplicate_frame = pd.read_parquet(path)
    duplicate_frame.columns = ["a", "a"]
    original_read = pd.read_parquet

    def read(source, *args, **kwargs):
        return duplicate_frame.copy() if source == path else original_read(source, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", read)
    with pytest.raises(RuntimeError, match="duplicate feature columns"):
        backtest_tick.load_ml_predictions(trades, feature_dir=panel)


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


def test_live_prediction_shares_one_model_matrix_and_preserves_quote_action() -> None:
    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    engine._enable_ml = True
    seen: list[np.ndarray] = []
    values = {
        "dir_10s": 0.61,
        "dir_30s": 0.57,
        "dir_60s": 0.54,
        "vol_10s": 3.25,
        "vol_30s": 4.5,
        "vol_60s": 6.75,
        "ret_10s": 0.0002,
        "ret_30s": -0.0001,
        "ret_60s": 0.0003,
        "tox_bid_5s": 0.72,
        "tox_ask_5s": 0.31,
        "tox_bid_10s": 0.68,
        "tox_ask_10s": 0.36,
    }
    engine._models = {
        name: _RecordingFeatureModel(values[name], seen)
        for name in REQUIRED_MODEL_HEADS
    }
    engine._model_feature_cols = {
        name: list(FEATURE_NAMES_BASE) for name in REQUIRED_MODEL_HEADS
    }
    features = {
        name: float(index + 1)
        for index, name in enumerate(FEATURE_NAMES_BASE)
    }

    prediction = engine._predict(features)

    assert len(seen) == len(REQUIRED_MODEL_HEADS)
    assert len({id(matrix) for matrix in seen}) == 1
    expected_row = np.asarray(
        [[features[name] for name in FEATURE_NAMES_BASE]], dtype=np.float64
    )
    for row in seen:
        assert row == pytest.approx(expected_row, abs=0.0)
    assert prediction.dir_10s == values["dir_10s"]
    assert prediction.vol_10s == values["vol_10s"]
    assert prediction.ret_10s == values["ret_10s"]
    assert prediction.tox_bid_10s == values["tox_bid_10s"]
    assert prediction.tox_ask_10s == values["tox_ask_10s"]

    state = QuoteState(
        mid=100.0,
        inventory=0.001,
        sigma_sq=4.0,
        best_bid=99.9,
        best_ask=100.1,
    )
    cfg = QuoteCoreConfig(
        gamma=0.046,
        kappa=0.01,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.026,
        ml_enabled=True,
        vol_blend=0.5,
        dir_threshold=0.05,
        skew_strength=0.1,
    )
    optimized_action = compute_quote_core(
        state,
        cfg,
        QuotePrediction(
            dir_10s=prediction.dir_10s,
            vol_10s=prediction.vol_10s,
            ret_10s=prediction.ret_10s,
            tox_bid=prediction.tox_bid_10s,
            tox_ask=prediction.tox_ask_10s,
        ),
    )
    reference_action = compute_quote_core(
        state,
        cfg,
        QuotePrediction(
            dir_10s=values["dir_10s"],
            vol_10s=values["vol_10s"],
            ret_10s=values["ret_10s"],
            tox_bid=values["tox_bid_10s"],
            tox_ask=values["tox_ask_10s"],
        ),
    )
    assert optimized_action == reference_action


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
