import hashlib
import json
from pathlib import Path
from dataclasses import FrozenInstanceError, replace

import pandas as pd
import numpy as np
import pytest

import research.families.f03_causal_13_head.ml_model as ml_model
from research.families.f03_causal_13_head import time_weighted_evaluation as tw_eval
from research.families.f03_causal_13_head.feature_variants import TAKER_FEATURE_ABLATION_VARIANTS
from research.families.f03_causal_13_head.ml_model import (
    SOURCE_PROFILE_ABLATION_PROFILES,
    load_train_only_selection_contract,
    split_train_only_selection,
    training_experiment_contract,
    training_experiment_contract_sha256,
    validate_training_request,
)
from strategy.model_contract import (
    ABSOLUTE_PRICE_VARIANCE_SEMANTICS,
    DEPLOYMENT_AUTHORIZATION_SCHEMA,
    LEGACY_LIVE_CANARY_AUTHORIZATION_SCHEMA,
    LEGACY_OWNER_AUTHORIZED_LIVE_CANARY,
    PRIVATE_DEPLOYMENT_AUTHORITY,
    REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
    REQUIRED_FEATURE_DAG_ID,
    REQUIRED_FEATURE_DAG_SHA256,
    REQUIRED_FEATURE_SEMANTICS_VERSION,
    REQUIRED_LABEL_SEMANTICS_VERSION,
    REQUIRED_LABEL_WINDOW_SEMANTICS,
    REQUIRED_MODEL_HEADS,
    absolute_price_variance_unit_contract,
    resolve_model_authorization_manifest,
    validate_model_bundle,
)


@pytest.mark.parametrize("unit", ["individual_execution", "native_aggregate_packet"])
def test_training_metadata_keeps_execution_count_unit_from_actual_bar_inputs(unit):
    payload = {"bar_source": {"daily_files": [{"trade_count_unit": unit}, {"trade_count_unit": unit}]}}
    assert ml_model._execution_count_unit_from_panel(payload) == unit
    assert ml_model._execution_count_unit_from_panel({}) == "native_aggregate_packet"


@pytest.mark.parametrize("units", [["individual_execution", "native_aggregate_packet"], ["UNKNOWN"], [None]])
def test_training_does_not_mislabel_mixed_or_unknown_execution_counts(units):
    with pytest.raises(ValueError, match="mixed or unknown"):
        ml_model._execution_count_unit_from_panel({"bar_source": {
            "daily_files": [{"trade_count_unit": unit} for unit in units]}})


@pytest.mark.parametrize("unit", ["individual_execution", "native_aggregate_packet"])
def test_reference_count_unit_is_independent_and_preserved(unit):
    source = {"symbol": "BTCUSDT", "daily_files": [
        {"symbol": "BTCUSDT", "trade_count_unit": unit}]}
    assert ml_model._reference_count_unit_from_panel({"reference_bar_source": source}) == unit
    assert ml_model._reference_count_unit_from_panel({}) == "native_aggregate_packet"


@pytest.mark.parametrize("rows", [[], [{"symbol": "BTCUSDT", "trade_count_unit": "UNKNOWN"}],
    [{"symbol": "BTCUSDT", "trade_count_unit": "individual_execution"},
     {"symbol": "BTCUSDT", "trade_count_unit": "native_aggregate_packet"}],
    [{"symbol": "BTCUSDC", "trade_count_unit": "individual_execution"}]])
def test_reference_count_unknown_mixed_or_wrong_symbol_cannot_be_fitted_as_individual(rows):
    with pytest.raises(ValueError):
        ml_model._reference_count_unit_from_panel({"reference_bar_source": {"symbol": "BTCUSDT", "daily_files": rows}})


def test_model_identity_keeps_reference_observation_limits_without_fitting(tmp_path, monkeypatch):
    units = absolute_price_variance_unit_contract("BTCUSDC")
    payload = {
        "schema_version": 3, "symbol": "BTCUSDC", "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID, "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
        "volatility_unit_contract": units, "label_volatility_units": units["variance_units"],
        "label_quote_calibration": {"schema_version": "narrowgate_p3_touch_calibration.v3",
            "model_type": "empirical_survival", "sha256": "synthetic", "p3_delta_star": 1, "p3_kappa_eff": 1},
        "reference_bar_source": {"symbol": "BTCUSDT", "daily_files": [
            {"symbol": "BTCUSDT", "trade_count_unit": "individual_execution"}]},
    }
    (tmp_path / "causal_feature_manifest.json").write_text(json.dumps(payload))
    monkeypatch.setattr(ml_model, "DATA_DIR", tmp_path)
    monkeypatch.setattr(ml_model, "SYMBOL", "BTCUSDC")
    identity = ml_model._feature_panel_identity()
    assert identity["reference_trade_count_unit"] == "individual_execution"
    assert identity["reference_trade_symbol"] == "BTCUSDT"
    limits = identity["reference_observation_limits"]
    assert limits["live_f_l_counts"] == "observed_id_range_not_proof_of_internal_id_completeness"
    assert limits["live_child_execution_timestamps_reconstructed"] is False
    assert limits["live_173_feature_message_exactness_proven"] is False
    assert limits["legacy_business_baseline_requalified"] is False


def test_predictive_ablation_contract_preserves_source_and_taker_definitions() -> None:
    contract = training_experiment_contract()

    assert contract["required_heads"] == list(REQUIRED_MODEL_HEADS)
    assert set(SOURCE_PROFILE_ABLATION_PROFILES).issubset(contract["source_profiles"])
    assert tuple(contract["taker_feature_contract"]["variants"]) == (
        TAKER_FEATURE_ABLATION_VARIANTS
    )
    assert contract["invariants"]["complete_13_head_bundle_required"] is True
    assert contract["invariants"]["promotion_authority"] == "research_only"
    assert len(training_experiment_contract_sha256()) == 64


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {
                "source_profile": "local_only",
                "feature_variant": "base",
                "experiment_id": "source-local-v1",
                "model_dir": None,
                "target": None,
                "predict": False,
            },
            "versioned --model-dir",
        ),
        (
            {
                "source_profile": "all",
                "feature_variant": "add_l2_interactions",
                "experiment_id": None,
                "model_dir": Path("models/saved_ablation"),
                "target": None,
                "predict": False,
            },
            "--experiment-id",
        ),
        (
            {
                "source_profile": "local_ref_perp",
                "feature_variant": "base",
                "experiment_id": "source-ref-v1",
                "model_dir": Path("models/saved_ablation"),
                "target": "touch_conditioned_up_probability_10000ms",
                "predict": False,
            },
            "complete strict 13-head bundle",
        ),
    ],
)
def test_predictive_ablation_training_fails_closed(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        validate_training_request(**kwargs)


def test_base_training_keeps_diagnostic_single_head_available() -> None:
    validate_training_request(
        source_profile="all",
        feature_variant="base",
        experiment_id=None,
        model_dir=None,
        target="touch_conditioned_up_probability_10000ms",
        predict=False,
    )


def test_train_only_selection_is_hash_bound_and_chronological(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fit_days = ("2025-08-02", "2025-08-03")
    embargo_days = ("2025-08-04",)
    selection_days = ("2025-08-05", "2025-08-06")
    refit_days = fit_days + embargo_days + selection_days
    spec = {
        "schema_version": "narrowgate_13_head_train_only_selection.v1",
        "source_authority": "provider_normalized_causal",
        "fit_days": list(fit_days),
        "embargo_days": list(embargo_days),
        "selection_days": list(selection_days),
        "refit_days": list(refit_days),
        "feature_manifest_sha256": "feature-manifest",
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
        "feature_dag_sha256": "feature-dag",
        "source_manifest_sha256": "source-manifest",
        "train_source_identity_sha256": "train-source-identity",
        "training_implementation_sha256": hashlib.sha256(
            Path(ml_model.__file__).read_bytes()
        ).hexdigest(),
        "training_experiment_contract_sha256": (
            training_experiment_contract_sha256()
        ),
        "head_names": list(ml_model.MODEL_SPECS),
        "external_panel_read_during_fit": False,
    }
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    monkeypatch.setattr(
        ml_model,
        "_feature_panel_identity",
        lambda: {
            "feature_manifest_sha256": "feature-manifest",
            "feature_manifest_path": str(tmp_path / "feature-manifest.json"),
            "feature_dag_sha256": "feature-dag",
            "feature_panel_split": {"train": list(refit_days)},
        },
    )
    monkeypatch.setattr(
        ml_model,
        "_load_train_source_identity",
        lambda path, days: {
            "source_manifest_sha256": "source-manifest",
            "train_source_identity_sha256": "train-source-identity",
        },
    )

    contract = load_train_only_selection_contract(path)
    index = pd.to_datetime(
        [f"{day} 00:00:00+00:00" for day in refit_days]
    )
    frame = pd.DataFrame({"x": range(len(index))}, index=index)
    fit, selection, refit = split_train_only_selection(frame, contract)

    assert len(fit) == 2
    assert len(selection) == 2
    assert len(refit) == 5
    assert contract.to_metadata()["external_panel_read_during_fit"] is False


def test_train_only_selection_rejects_nonchronological_or_unbound_spec(
    tmp_path: Path,
    monkeypatch,
) -> None:
    payload = {
        "schema_version": "narrowgate_13_head_train_only_selection.v1",
        "source_authority": "provider_normalized_causal",
        "fit_days": ["2025-08-05"],
        "embargo_days": ["2025-08-04"],
        "selection_days": ["2025-08-06"],
        "refit_days": ["2025-08-04", "2025-08-05", "2025-08-06"],
        "feature_manifest_sha256": "wrong",
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
        "feature_dag_sha256": "feature-dag",
        "source_manifest_sha256": "source-manifest",
        "train_source_identity_sha256": "train-source-identity",
        "training_implementation_sha256": hashlib.sha256(
            Path(ml_model.__file__).read_bytes()
        ).hexdigest(),
        "training_experiment_contract_sha256": (
            training_experiment_contract_sha256()
        ),
        "head_names": list(ml_model.MODEL_SPECS),
        "external_panel_read_during_fit": False,
    }
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        ml_model,
        "_feature_panel_identity",
        lambda: {
            "feature_manifest_sha256": "feature-manifest",
            "feature_manifest_path": str(tmp_path / "feature-manifest.json"),
            "feature_dag_sha256": "feature-dag",
            "feature_panel_split": {"train": payload["refit_days"]},
        },
    )

    with pytest.raises(ValueError, match="not chronological"):
        load_train_only_selection_contract(path)


def test_train_source_identity_rejects_non_provider_refit_day(tmp_path: Path) -> None:
    source_manifest = tmp_path / "source-manifest.json"
    source_manifest.write_text(
        json.dumps(
            {
                "source_files": [
                    {
                        "day": "2025-08-02",
                        "source_authority": "native_formal_lifecycle",
                        "bbo_sha256": "bbo",
                        "l2_sha256": "l2",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    feature_manifest = tmp_path / "feature-manifest.json"
    feature_manifest.write_text(
        json.dumps(
            {
                "execution_l2_source": {
                    "manifest_path": str(source_manifest),
                    "manifest_sha256": hashlib.sha256(
                        source_manifest.read_bytes()
                    ).hexdigest(),
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="not provider-normalized"):
        ml_model._load_train_source_identity(
            feature_manifest,
            ("2025-08-02",),
        )


def test_experiment_split_applies_variant_before_enforcing_model_schema(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "close": [100.0],
            "l2_imbalance_l3": [0.5],
            "l2_near_depth_total": [10.0],
            "taker_buy_sweep_score_5s": [2.0],
            "sample_weight": [1.0],
        }
    )
    base_cols = [column for column in frame if column != "sample_weight"]
    expected = base_cols + ["x_taker_buy_sweep_l2imb_l3_5s"]

    monkeypatch.setattr(
        ml_model,
        "feature_columns_for_profile",
        lambda name, profile: base_cols,
    )
    monkeypatch.setattr(
        ml_model,
        "load_split",
        lambda name, columns=None: frame[columns].copy(),
    )

    result = ml_model.load_experiment_split(
        "train",
        source_profile="all",
        feature_variant="add_l2_interactions",
        expected_feature_cols=expected,
    )

    assert list(result.columns) == expected + ["sample_weight"]
    assert result.loc[0, "x_taker_buy_sweep_l2imb_l3_5s"] == pytest.approx(1.0)


def _weighted_contract(half_life="inf"):
    days = ("2025-08-01", "2025-08-04", "2025-08-07", "2025-08-10")
    return ml_model.TrainOnlySelectionContract(
        schema_version="narrowgate_13_head_train_only_selection.v1",
        spec_path="synthetic-selection.json", spec_sha256="spec",
        source_authority="canonical_exchange_clock",
        fit_days=days[:2], embargo_days=days[2:3], selection_days=days[3:],
        refit_days=days, feature_manifest_sha256="feature",
        feature_dag_sha256="dag", source_manifest_sha256="source",
        train_source_identity_sha256="identity",
        feature_cols=("x",),
        sample_weight_policy={
            "schema_version": "narrowgate.f03.time_half_life_daily.v1",
            "half_life_days": half_life,
            "reference_date": "2025-08-12",
            "date_normalization": "effective_head_rows",
            "previous_time_weight": {
                "formula": "exp(-lambda * days_ago / 30.44)",
                "lambda": 0.1,
                "reference_date": "2025-08-12",
            },
            "outcome_end_columns": {
                head: f"label_outcome_end_{head}" for head in ml_model.MODEL_SPECS
            },
        },
    )


def _weighted_frame(contract):
    index = pd.to_datetime([
        "2025-08-01 00:00:00Z", "2025-08-01 00:00:10Z",
        "2025-08-01 23:59:40Z",  # This valid outcome crosses into a held-out day.
        "2025-08-04 00:00:00Z", "2025-08-04 00:00:10Z",
        "2025-08-04 00:00:20Z", "2025-08-07 00:00:00Z",
        "2025-08-10 00:00:00Z", "2025-08-10 00:00:10Z",
    ])
    frame = pd.DataFrame({"x": np.arange(len(index), dtype=float)}, index=index)
    for name, (label, *_) in ml_model.MODEL_SPECS.items():
        frame[label] = np.arange(len(index)) % 2
        frame[contract.sample_weight_policy["outcome_end_columns"][name]] = index + pd.Timedelta(seconds=30)
    frame.loc[index[1], "label_touch_conditioned_price_change_fraction_10000ms"] = np.nan
    days_ago = (pd.Timestamp("2025-08-12", tz="UTC") - index).total_seconds() / 86400
    frame["sample_weight"] = np.exp(-0.1 * days_ago / 30.44)
    return frame


@pytest.mark.parametrize("half_life", ["inf", 240, 120, 60])
def test_time_weights_replace_old_decay_after_per_head_purge(half_life):
    contract = _weighted_contract(half_life)
    fit, selection, refit = split_train_only_selection(_weighted_frame(contract), contract)
    X, _, weights, columns, report = ml_model.prepare_time_weighted_xy(
        fit, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit",
    )
    assert columns == ["x"]
    assert len(X) == 4  # one NaN label and one cross-day endpoint are excluded
    assert report["outcome_outside_phase_days_rows"] == 1
    assert report["weight_mean"] == pytest.approx(1)
    day_totals = weights.groupby(weights.index.date).sum().to_numpy()
    expected_ratio = 1 if half_life == "inf" else 2 ** (3 / half_life)
    assert day_totals[1] / day_totals[0] == pytest.approx(expected_ratio)
    assert weights.iloc[1] == pytest.approx(weights.iloc[2])
    _, _, refit_weights, _, refit_report = ml_model.prepare_time_weighted_xy(
        refit, "touch_conditioned_price_change_fraction_10000ms", contract, phase="refit",
    )
    assert refit_weights.mean() == pytest.approx(1)
    assert refit_report["effective_day_count"] == 4
    assert weights.iloc[0] != pytest.approx(refit_weights.iloc[0])
    _, _, eval_weights, _, eval_report = ml_model.prepare_time_weighted_xy(
        selection, "touch_conditioned_price_change_fraction_10000ms", contract, phase="selection",
    )
    assert eval_weights is None
    assert eval_report["unweighted_early_stopping"] is True


def test_time_weighting_reports_zero_support_and_rejects_unknown_weight():
    contract = _weighted_contract()
    frame = _weighted_frame(contract)
    frame.loc[frame.index.strftime("%Y-%m-%d") == "2025-08-04", "label_touch_conditioned_price_change_fraction_10000ms"] = np.nan
    fit, _, _ = split_train_only_selection(frame, contract)
    _, _, _, _, report = ml_model.prepare_time_weighted_xy(fit, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit")
    assert report["unsupported_days"] == ["2025-08-04"]
    fit["sample_weight"] *= 0.5
    with pytest.raises(ValueError, match="not the declared time-only weight"):
        ml_model.prepare_time_weighted_xy(fit, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit")
    fit = _weighted_frame(contract).iloc[:2].copy()
    fit["label_touch_conditioned_price_change_fraction_10000ms"] = np.nan
    with pytest.raises(ValueError, match="no effective labels"):
        ml_model.prepare_time_weighted_xy(fit, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit")


def test_weighted_helpers_never_enter_features_or_allow_ambiguous_clock():
    contract = _weighted_contract()
    frame = _weighted_frame(contract).iloc[:2].copy()
    for column in ml_model.TRAINING_AUXILIARY_COLS:
        frame[column] = 1
    assert ml_model.prepare_xy(frame, "label_touch_conditioned_price_change_fraction_10000ms")[3] == ["x"]
    endpoint = "label_outcome_end_touch_conditioned_price_change_fraction_10000ms"
    frame[endpoint] = frame.index.astype("int64")
    with pytest.raises(ValueError, match="explicit datetime UTC units"):
        ml_model.prepare_time_weighted_xy(frame, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit")
    frame[endpoint] = frame.index
    with pytest.raises(ValueError, match="before its decision is visible"):
        ml_model.prepare_time_weighted_xy(frame, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit")


def test_ready_frame_weighting_does_not_add_legacy_ten_seconds():
    from dataclasses import replace
    contract = _weighted_contract()
    policy = {**contract.sample_weight_policy,
              "schema_version": "narrowgate.f03.time_half_life_daily.v2",
              "decision_clock": "feature_ready_index"}
    contract = replace(contract, sample_weight_policy=policy)
    frame = _weighted_frame(contract).iloc[[0]].copy()
    frame["label_outcome_end_touch_conditioned_price_change_fraction_10000ms"] = frame.index + pd.Timedelta(seconds=5)
    X, _, _, _, report = ml_model.prepare_time_weighted_xy(frame, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit")
    assert len(X) == 1
    assert report["first_decision_utc"] == frame.index[0].isoformat()
    assert report["decision_clock"] == "feature_ready_index"
    frame["label_outcome_end_touch_conditioned_price_change_fraction_10000ms"] = frame.index - pd.Timedelta(nanoseconds=1)
    with pytest.raises(ValueError, match="before its decision"):
        ml_model.prepare_time_weighted_xy(frame, "touch_conditioned_price_change_fraction_10000ms", contract, phase="fit")
    policy.pop("decision_clock")
    with pytest.raises(ValueError, match="explicit feature_ready_index"):
        ml_model._validate_sample_weight_policy(policy, contract.refit_days)


def test_time_weighted_main_only_reads_train_and_refits_all_heads(tmp_path, monkeypatch):
    contract = _weighted_contract(120)
    frame = _weighted_frame(contract)
    frame["x"] = np.nan  # The frozen feature schema must not be silently pruned.
    reads, fits, saved = [], [], []

    class FakeModel:
        def __init__(self, **params):
            self.params = params
            self.best_iteration_ = 7
            self.best_score_ = {"valid_0": {"auc": 0.6, "mae": 0.1}}

        def fit(self, X, y, sample_weight=None, **kwargs):
            self.feature_importances_ = np.ones(X.shape[1], dtype=int)
            fits.append((self.params, sample_weight, kwargs))
            return self

    def load(name, **kwargs):
        reads.append(name)
        assert name == "train"
        assert kwargs["preserve_calendar"] is True
        assert kwargs["expected_feature_cols"] == ("x",)
        assert set(kwargs["auxiliary_cols"]) == set(contract.sample_weight_policy["outcome_end_columns"].values())
        return frame.copy()

    monkeypatch.setattr(ml_model.sys, "argv", [
        "ml_model", "--train-only-selection-spec", "synthetic.json",
        "--model-dir", str(tmp_path), "--experiment-id", "synthetic-time-decay",
    ])
    monkeypatch.setattr(ml_model, "configure_symbol", lambda *args, **kwargs: None)
    monkeypatch.setattr(ml_model, "load_train_only_selection_contract", lambda path: contract)
    monkeypatch.setattr(ml_model, "load_experiment_split", load)
    monkeypatch.setattr(ml_model.lgb, "LGBMClassifier", FakeModel)
    monkeypatch.setattr(ml_model.lgb, "LGBMRegressor", FakeModel)
    reference_identity = {"reference_trade_count_unit": "individual_execution", "reference_trade_symbol": "BTCUSDT",
        "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end"}
    monkeypatch.setattr(ml_model, "_feature_panel_identity", lambda: reference_identity)
    monkeypatch.setattr(ml_model, "release_memory", lambda: None)
    monkeypatch.setattr(ml_model, "save_model", lambda model, name, meta: saved.append((name, meta)))
    monkeypatch.setattr(ml_model, "write_bundle_meta", lambda *args, **kwargs: None)
    monkeypatch.setattr(ml_model, "write_training_summary", lambda *args, **kwargs: tmp_path / "summary.json")
    monkeypatch.setattr(ml_model, "evaluate_test_from_disk", lambda *args, **kwargs: pytest.fail("external evaluation attempted"))
    monkeypatch.setattr(ml_model, "generate_predictions_from_disk", lambda *args, **kwargs: pytest.fail("external prediction attempted"))
    for name in ("ACTIVE_SOURCE_PROFILE", "ACTIVE_FEATURE_VARIANT", "ACTIVE_EXPERIMENT_ID", "ACTIVE_ARTIFACT_AUTHORITY"):
        monkeypatch.setattr(ml_model, name, getattr(ml_model, name))
    ml_model.main()
    assert reads == ["train"]
    assert [name for name, _ in saved] == list(ml_model.MODEL_SPECS)
    assert len(fits) == 26
    for index, (params, weights, kwargs) in enumerate(fits):
        assert weights.mean() == pytest.approx(1)
        if index % 2 == 0:
            assert kwargs["eval_sample_weight"] == [None]
        else:
            assert params["n_estimators"] == 7
            assert "eval_set" not in kwargs
    assert saved[0][1]["class_imbalance_adjustment"]["is_unbalance"] is True
    assert saved[1][1]["class_imbalance_adjustment"]["is_unbalance"] is False
    assert saved[0][1]["feature_cols"] == ["x"]
    assert saved[0][1]["feature_availability_train"] == {"x": 0.0}
    assert all(all(meta[key] == value for key, value in reference_identity.items()) for _, meta in saved)


def test_canonical_source_identity_binds_book_clock_without_provider_claim(tmp_path):
    source_manifest = tmp_path / "source.json"
    row = {
        "day": "2025-08-01", "source_kind": "narrowgate.daily_book.v1",
        "source_clock": "fused_exchange", "raw_sha256": "a" * 64,
        "quality_sha256": "b" * 64,
        "processed_files": [{"kind": kind, "sha256": "c" * 64, "size_bytes": 1} for kind in ("bbo", "l2", "clock")],
    }
    feature_manifest = tmp_path / "feature.json"

    def write_sources(rows):
        source_manifest.write_text(json.dumps({"sources": rows}))
        feature_manifest.write_text(json.dumps({"execution_l2_source": {
            "manifest_path": str(source_manifest),
            "manifest_sha256": hashlib.sha256(source_manifest.read_bytes()).hexdigest(),
        }}))

    write_sources([row])
    result = ml_model._load_train_source_identity(feature_manifest, ("2025-08-01",), source_authority="canonical_exchange_clock")
    assert len(result["train_source_identity_sha256"]) == 64
    write_sources([row, row])
    with pytest.raises(ValueError, match="duplicate L2 source day"):
        ml_model._load_train_source_identity(feature_manifest, ("2025-08-01",), source_authority="canonical_exchange_clock")
    write_sources([{**row, "source_clock": "provider_local"}])
    with pytest.raises(ValueError, match="exchange-clock contract"):
        ml_model._load_train_source_identity(feature_manifest, ("2025-08-01",), source_authority="canonical_exchange_clock")


def test_weight_policy_is_bound_to_actual_feature_weight_metadata(tmp_path, monkeypatch):
    contract = _weighted_contract(60)
    feature_manifest = tmp_path / "features.json"
    feature_manifest.write_text(json.dumps({"sample_weight": contract.sample_weight_policy["previous_time_weight"]}))
    payload = {
        **contract.to_metadata(),
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
        "head_names": list(ml_model.MODEL_SPECS),
        "training_implementation_sha256": hashlib.sha256(Path(ml_model.__file__).read_bytes()).hexdigest(),
        "training_experiment_contract_sha256": training_experiment_contract_sha256(),
    }
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(ml_model, "_feature_panel_identity", lambda: {
        "feature_manifest_sha256": "feature", "feature_dag_sha256": "dag",
        "feature_manifest_path": str(feature_manifest),
        "feature_panel_split": {"train": list(contract.refit_days)},
    })
    monkeypatch.setattr(ml_model, "_load_train_source_identity", lambda *args, **kwargs: {
        "source_manifest_sha256": "source", "train_source_identity_sha256": "identity",
    })
    loaded = load_train_only_selection_contract(path)
    assert loaded.source_authority == "canonical_exchange_clock"
    assert loaded.sample_weight_policy["half_life_days"] == 60
    feature_manifest.write_text(json.dumps({"sample_weight": {"lambda": 0.2}}))
    with pytest.raises(ValueError, match="does not match feature manifest"):
        load_train_only_selection_contract(path)


def test_weighted_train_loader_preserves_calendar_but_legacy_still_filters(tmp_path, monkeypatch):
    frame = pd.DataFrame({"x": [1.0, 2.0]}, index=pd.to_datetime(["2025-08-01", "2025-08-04"], utc=True))
    frame.to_parquet(tmp_path / "dataset_train.parquet")
    monkeypatch.setattr(ml_model, "DATA_DIR", tmp_path)
    calls = []

    def old_filter(df, *args, **kwargs):
        calls.append(True)
        return df.iloc[:1]

    monkeypatch.setattr(ml_model, "filter_frame_for_orderbook_quality", old_filter)
    assert len(ml_model.load_split("train", preserve_calendar=True)) == 2
    assert calls == []
    assert len(ml_model.load_split("train")) == 1
    assert calls == [True]


def test_research_only_predictive_bundle_cannot_enter_live(tmp_path: Path) -> None:
    for name in REQUIRED_MODEL_HEADS:
        (tmp_path / f"{name}.txt").write_text("model", encoding="utf-8")
        metadata = {
            "symbol": "BTCUSDC",
            "feature_cols": ["close"],
            "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
            "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
            "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
            "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
            "label_semantics_version": REQUIRED_LABEL_SEMANTICS_VERSION,
            "label_window_semantics": REQUIRED_LABEL_WINDOW_SEMANTICS,
            "feature_manifest_sha256": "manifest-sha",
            "source_profile": "local_only",
            "feature_variant": "base",
            "training_experiment_id": "source-local-v1",
            "promotion_authority": "research_only",
            "volatility_unit_contract": absolute_price_variance_unit_contract(
                "BTCUSDC"
            ),
        }
        if name.startswith("absolute_price_variance_rate_"):
            metadata["label_semantics"] = ABSOLUTE_PRICE_VARIANCE_SEMANTICS
        (tmp_path / f"{name}_meta.json").write_text(
            json.dumps(metadata),
            encoding="utf-8",
        )

    with pytest.raises(ValueError, match="research_only"):
        validate_model_bundle(tmp_path)

    assert len(validate_model_bundle(tmp_path, allow_research_only=True)) == 13


@pytest.mark.parametrize("legacy", [False, True], ids=["current", "legacy-canary"])
def test_private_deployment_authorization_binds_every_head_hash(
    tmp_path: Path,
    legacy: bool,
) -> None:
    training_experiment_id = (
        "causal_v12_expanded_source_aware_semantics_v6"
        if legacy
        else "canary-v1"
    )
    feature_manifest_sha256 = (
        "legacy-fixture-manifest"
        if legacy
        else "manifest-sha"
    )
    promotion_authority = (
        LEGACY_OWNER_AUTHORIZED_LIVE_CANARY
        if legacy
        else PRIVATE_DEPLOYMENT_AUTHORITY
    )
    tree_hashes = {}
    metadata_hashes = {}
    for name in REQUIRED_MODEL_HEADS:
        model_path = tmp_path / f"{name}.txt"
        model_path.write_text("model", encoding="utf-8")
        tree_hashes[name] = hashlib.sha256(model_path.read_bytes()).hexdigest()
        metadata = {
            "symbol": "BTCUSDC",
            "feature_cols": ["close"],
            "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
            "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
            "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
            "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
            "label_semantics_version": REQUIRED_LABEL_SEMANTICS_VERSION,
            "label_window_semantics": REQUIRED_LABEL_WINDOW_SEMANTICS,
            "feature_manifest_sha256": feature_manifest_sha256,
            "source_profile": "all",
            "feature_variant": "base",
            "training_experiment_id": training_experiment_id,
            "promotion_authority": promotion_authority,
        }
        if not legacy:
            metadata["volatility_unit_contract"] = (
                absolute_price_variance_unit_contract("BTCUSDC")
            )
        if name.startswith("absolute_price_variance_rate_"):
            metadata["label_semantics"] = ABSOLUTE_PRICE_VARIANCE_SEMANTICS
        metadata_path = tmp_path / f"{name}_meta.json"
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        metadata_hashes[name] = hashlib.sha256(metadata_path.read_bytes()).hexdigest()

    authorization_file = (
        "live_canary_authorization.json"
        if legacy
        else "deployment_authorization.json"
    )
    initial_error = (
        "volatility_unit_contract"
        if legacy
        else authorization_file
    )
    with pytest.raises(ValueError, match=initial_error):
        validate_model_bundle(tmp_path)

    authorization = {
        "schema_version": (
            LEGACY_LIVE_CANARY_AUTHORIZATION_SCHEMA
            if legacy
            else DEPLOYMENT_AUTHORIZATION_SCHEMA
        ),
        "training_experiment_id": training_experiment_id,
        "baseline_promotion_authorized": False,
        "derived_bundle": {
            "model_tree_sha256": tree_hashes,
            "head_metadata_sha256": metadata_hashes,
        },
    }
    if legacy:
        authorization["owner_authorized"] = True
        authorization["active_live_inference_authorized"] = True
        authorization["volatility_unit_contract"] = (
            absolute_price_variance_unit_contract("BTCUSDC")
        )
    else:
        authorization["private_deployment_authorized"] = True
        authorization["active_runtime_inference_authorized"] = True
    (tmp_path / authorization_file).write_text(
        json.dumps(authorization), encoding="utf-8"
    )
    metadata = validate_model_bundle(
        tmp_path,
        require_live_authorization=True,
        expected_symbol="BTCUSDC",
    )
    assert len(metadata) == 13
    assert {head["promotion_authority"] for head in metadata.values()} == {
        PRIVATE_DEPLOYMENT_AUTHORITY
    }
    expected_origin = (
        {LEGACY_OWNER_AUTHORIZED_LIVE_CANARY}
        if legacy
        else {None}
    )
    assert {
        head.get("promotion_authority_origin") for head in metadata.values()
    } == expected_origin
    assert resolve_model_authorization_manifest(tmp_path, metadata) == (
        tmp_path / authorization_file
    )

    (tmp_path / "touch_conditioned_up_probability_10000ms.txt").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="model hash mismatch"):
        validate_model_bundle(tmp_path)


def _utc_results(b_changes=None, *, independent=True, phase="development", capital=1000.0):
    """Synthetic marked-equity paths, not market replay or real economics."""
    b_changes = b_changes or {"inf": 1, "240": 2, "120": 2, "60": 2}
    results = {}
    for number, half_life in enumerate(tw_eval.HALF_LIVES, 1):
        rows = []
        cash = 1000.0
        specs = tw_eval.build_shard_specs(initial_capital_usdc=capital, phase=phase)
        days = tw_eval.CALENDAR[:300] if phase == "development" else tw_eval.SPLITS["F"]
        for index, day in enumerate(days):
            if independent and index % 2 == 0:
                cash = 0.0
            start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
            pnl = b_changes[half_life] if day in tw_eval.SPLITS["B"] else 1.0
            rows.append({
                "arm": f"H{half_life}", "schema_version": "f01_utc_equity_slices.v1",
                "calendar_date": day, "start_ts_ms": start,
                "end_ts_ms_exclusive": start + tw_eval.DAY_MS, "complete_utc_day": True,
                "start_cash_usdc": cash, "end_cash_usdc": cash + pnl,
                "start_equity_usdc": cash, "end_equity_usdc": cash + pnl,
                "start_inventory_btc": 0.0, "end_inventory_btc": 0.0,
                "start_mark_price": 100.0, "end_mark_price": 100.0,
                "start_mark_clock_ts_ms": start - 1,
                "end_mark_clock_ts_ms": start + tw_eval.DAY_MS - 1,
                "net_equity_change_usdc": pnl, "fees_usdc": -0.25,
                "funding_cashflow_usdc": 0.125, "funding_mode": "frozen_settlement_tape",
                "accounting_window": "utc_slice_of_continuous_state",
                "terminal_liquidation_applied": False,
                "shard_id": specs[index // 2].shard_id,
            })
            cash += pnl
        shards = []
        for index, spec in enumerate(specs):
            shard_rows = rows[index * 2:index * 2 + len(spec.calendar_days)]
            last = shard_rows[-1]
            shards.append(tw_eval.ShardResult(spec, {
                "account_state_source": "fresh_flat_account", "account_checkpoint_parent": None,
                "history_scope": "market_and_features_only", "trading_start_ts_ms": spec.start_ts_ms,
                "initial_capital_usdc": capital, "initial_open_order_count": 0,
            }, {
                "end_ts_ms_exclusive": spec.end_ts_ms_exclusive,
                "accounting_complete": True, "terminal_liquidation_applied": False,
                "cash_usdc": last["end_cash_usdc"], "inventory_btc": last["end_inventory_btc"],
                "equity_usdc": last["end_equity_usdc"],
                "net_pnl_usdc": sum(row["net_equity_change_usdc"] for row in shard_rows),
                "fees_usdc": sum(row["fees_usdc"] for row in shard_rows),
                "funding_cashflow_usdc": sum(row["funding_cashflow_usdc"] for row in shard_rows),
            }))
        results[half_life] = tw_eval.UtcArmResult(
            rows, str(number) * 64, str(number + 4) * 64, tuple(REQUIRED_MODEL_HEADS),
            tuple(shards),
        )
    return results


def test_f03_fixed_407_split_and_b_exact_tie_prefers_long_half_life():
    scope = json.loads((Path(__file__).parents[1] / "data/dataset_scope.json").read_text())
    assert len(tw_eval.CALENDAR) == scope["calendar_days"]
    assert (tw_eval.CALENDAR[0], tw_eval.CALENDAR[-1]) == (
        scope["start_day"], scope["end_day_inclusive"],
    )
    assert {name: len(days) for name, days in tw_eval.SPLITS.items()} == {
        "T": 100, "A": 50, "B": 100, "C": 50, "F": 107,
    }
    assert tw_eval.CALENDAR[299] == "2026-05-27"
    assert tw_eval.SPLITS["F"] == tw_eval.CALENDAR[300:]
    assert tw_eval.CALENDAR[-1] == "2026-09-11"
    choice = tw_eval.select_b_half_life(_utc_results(), common_contract_sha256="a" * 64)
    assert choice.selected_half_life == 240
    assert choice.positive_b_point_increment is True
    assert choice.to_metadata()["weighted_hypothesis_status"] == "positive_B_point_increment_only"
    assert choice.scores[1].delta_vs_uniform_usdc == 100
    assert choice.to_metadata()["a_c_economics_read"] is False
    with pytest.raises(FrozenInstanceError):
        choice.selected_half_life = 60


def test_f03_uniform_beats_all_finite_is_reported_not_called_supported():
    results = _utc_results({"inf": 2, "240": 1, "120": 0, "60": -1})
    choice = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    assert choice.selected_half_life == "inf"
    assert choice.positive_b_point_increment is False
    assert choice.to_metadata()["weighted_hypothesis_status"] == "not_supported_in_B"
    assert [score.delta_vs_uniform_usdc for score in choice.scores] == [0, -100, -200, -300]


@pytest.mark.parametrize("pnl", [2, 0, -2])
def test_f03_all_new_models_tie_selects_uniform_even_when_all_lose(pnl):
    results = _utc_results(dict.fromkeys(tw_eval.HALF_LIVES, pnl))
    choice = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    assert choice.selected_half_life == "inf"
    assert choice.to_metadata()["schema_version"] == "f03_time_weighted_b_selection.v3"
    assert choice.to_metadata()["uniform_role"] == "new_equal_day_weight_model_not_old_baseline"


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unexpected", "reorder", "partial", "bad_bounds"])
def test_f03_b_selection_rejects_calendar_errors_even_outside_b(mutation):
    results = _utc_results()
    rows = results["60"].rows
    if mutation == "missing":
        rows.pop(0)
    elif mutation == "duplicate":
        rows[0] = dict(rows[1])
    elif mutation == "unexpected":
        rows[0]["calendar_date"] = "2025-07-31"
    elif mutation == "reorder":
        rows[0], rows[1] = rows[1], rows[0]
    elif mutation == "partial":
        rows[0]["complete_utc_day"] = False
    else:
        rows[0]["end_ts_ms_exclusive"] -= 1
    with pytest.raises(ValueError, match="calendar|UTC day"):
        tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)


def test_f03_b_selector_does_not_access_poisoned_a_c_or_t_amounts():
    class UnreadableAmount:
        def __float__(self):
            raise AssertionError("selection opened an A/C/T amount")

    results = _utc_results()
    expected = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    for result in results.values():
        for row in result.rows:
            if row["calendar_date"] not in tw_eval.SPLITS["B"]:
                for key in row:
                    if key.endswith("_usdc") or key.endswith("_btc") or key.endswith("_mark_price"):
                        row[key] = UnreadableAmount()
        # Shard receipts may include T/A/C or cross-label totals. Never open them
        # as a shortcut to checking a B day's containing shard before selection.
        for shard in result.shards:
            for mapping in (shard.initialization, shard.terminal):
                for key in mapping:
                    if key.endswith(("_usdc", "_btc")) or key == "initial_open_order_count":
                        mapping[key] = UnreadableAmount()
    assert tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64) == expected


def test_f03_post_choice_reports_full_path_signed_fees_without_rededuction():
    results = _utc_results()
    choice = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    report = tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64)
    assert report["arms"]["240"]["split_net_pnl_usdc"] == {"T": 100, "A": 50, "B": 200, "C": 50}
    full = report["arms"]["240"]["full_development"]
    assert full["net_pnl_usdc"] == 400
    assert full["shard_count"] == 150
    assert full["net_pnl_usdc"] != full["shards"][-1]["end_equity_usdc"]
    assert full["fees_usdc"] == -75
    assert full["funding_cashflow_usdc"] == 37.5
    assert full["drawdown_scope"] == "maximum_within_shard_daily_boundary_drawdown_not_continuous_or_intraday"
    results["240"] = replace(results["240"], model_bundle_sha256="b" * 64)
    with pytest.raises(ValueError, match="changed after"):
        tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64)


def test_f03_full_path_continuity_is_checked_only_after_b_choice():
    results = _utc_results()
    choice = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    # A coherent daily row with a hidden account reset still fails full-path reporting.
    row = results["60"].rows[1]
    assert row["calendar_date"] in tw_eval.SPLITS["A"]
    for key in ("start_cash_usdc", "end_cash_usdc", "start_equity_usdc", "end_equity_usdc"):
        row[key] += 10
    assert tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64) == choice
    with pytest.raises(ValueError, match="resets or changes state"):
        tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64)


@pytest.mark.parametrize("mutation", ["pnl", "funding", "future_mark", "missing_head", "extra_arm"])
def test_f03_selection_rejects_invalid_b_accounting_or_bundle(mutation):
    results = _utc_results()
    row = next(row for row in results["60"].rows if row["calendar_date"] in tw_eval.SPLITS["B"])
    if mutation == "pnl":
        row["net_equity_change_usdc"] = 100
    elif mutation == "funding":
        row["funding_cashflow_usdc"] = None
    elif mutation == "future_mark":
        row["end_mark_clock_ts_ms"] = row["end_ts_ms_exclusive"]
    elif mutation == "missing_head":
        results["60"] = replace(results["60"], head_names=tuple(REQUIRED_MODEL_HEADS[:-1]))
    else:
        results["30"] = results["60"]
    with pytest.raises(ValueError):
        tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)


def test_f03_utc_accounting_accepts_csv_scalars_and_requires_full_final_calendar():
    rows = _utc_results(independent=False)["inf"].rows
    for row in rows:
        for key, value in row.items():
            row[key] = str(value)
    assert tw_eval.validate_continuous_account(rows)["net_pnl_usdc"] == 300
    with pytest.raises(ValueError, match="calendar"):
        tw_eval.validate_continuous_account(rows, phase="final")


def test_f03_zero_activity_and_cold_flat_unknown_mark_do_not_drop_dates():
    results = _utc_results({half_life: 0 for half_life in tw_eval.HALF_LIVES}, independent=False)
    rows = results["inf"].rows
    rows[0]["start_mark_price"] = None
    rows[0]["start_mark_clock_ts_ms"] = None
    report = tw_eval.validate_continuous_account(rows)
    assert report["days"] == 300
    assert report["net_pnl_usdc"] == 200  # 100 B days all remain, contributing zero.
    rows[0]["start_inventory_btc"] = 1
    with pytest.raises(ValueError, match="lacks a valuation"):
        tw_eval.validate_continuous_account(rows)


def test_f03_final_requires_all_107_days_and_preserves_end_inventory_mtm():
    rows = _utc_results(independent=False)["inf"].rows[:107]
    for row, day in zip(rows, tw_eval.SPLITS["F"], strict=True):
        start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
        row.update(calendar_date=day, start_ts_ms=start,
                   end_ts_ms_exclusive=start + tw_eval.DAY_MS,
                   start_mark_clock_ts_ms=start - 1,
                   end_mark_clock_ts_ms=start + tw_eval.DAY_MS - 1)
    # Same final equity with nonzero inventory: never silently liquidate it.
    rows[-1]["end_inventory_btc"] = 0.5
    rows[-1]["end_cash_usdc"] -= 50
    report = tw_eval.validate_continuous_account(rows, phase="final")
    assert report["days"] == 107
    assert report["net_pnl_usdc"] == 107
    assert report["end_inventory_btc"] == 0.5
    with pytest.raises(ValueError, match="calendar"):
        tw_eval.validate_continuous_account(rows[:-1], phase="final")


def test_f03_independent_shards_preserve_full_calendar_and_original_labels():
    dev = tw_eval.build_shard_specs(initial_capital_usdc=1000)
    final = tw_eval.build_shard_specs(initial_capital_usdc=1000, phase="final")
    assert len(dev) == 150 and {len(spec.calendar_days) for spec in dev} == {2}
    assert len(final) == 54 and [len(spec.calendar_days) for spec in final] == [2] * 53 + [1]
    assert tuple(day for spec in (*dev, *final) for day in spec.calendar_days) == tw_eval.CALENDAR
    assert any(any(day in tw_eval.SPLITS["B"] for day in spec.calendar_days)
               and any(day not in tw_eval.SPLITS["B"] for day in spec.calendar_days) for spec in dev)
    for phase, specs in (("development", dev), ("final", final)):
        tw_eval.validate_shard_specs(specs, phase=phase)
        for spec in specs:
            assert spec.start_ts_ms % tw_eval.DAY_MS == spec.end_ts_ms_exclusive % tw_eval.DAY_MS == 0
            assert spec.warmup_start_ts_ms == spec.start_ts_ms - tw_eval.DAY_MS
            assert spec.to_metadata()["checkpoint_scope"] == "same_shard_only"
    assert final[-1].calendar_days == ("2026-09-11",)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "reorder", "one_day", "capital", "warmup", "phase"])
def test_f03_shard_plan_rejects_noncanonical_or_inconsistent_accounts(mutation):
    specs = list(tw_eval.build_shard_specs(initial_capital_usdc=1000))
    if mutation == "missing":
        specs.pop()
    elif mutation == "duplicate":
        specs[-1] = specs[-2]
    elif mutation == "reorder":
        specs[0], specs[1] = specs[1], specs[0]
    elif mutation == "one_day":
        specs[0] = replace(specs[0], calendar_days=specs[0].calendar_days[:1])
    elif mutation == "capital":
        specs[-1] = replace(specs[-1], initial_capital_usdc=2000)
    elif mutation == "warmup":
        specs[-1] = replace(specs[-1], warmup_start_ts_ms=specs[-1].start_ts_ms)
    else:
        specs = list(tw_eval.build_shard_specs(initial_capital_usdc=1000, phase="final"))
    with pytest.raises(ValueError, match="shards must cover"):
        tw_eval.validate_shard_specs(specs)


def _shard_checkpoint(spec):
    return {"schema_version": "f03_independent_shard_checkpoint.v1", "shard_spec": spec.to_metadata(),
            "arm": "H120", "model_bundle_sha256": "b" * 64, "common_contract_sha256": "c" * 64,
            "cut_ts_ms": spec.start_ts_ms + tw_eval.DAY_MS}


@pytest.mark.parametrize("mutation", [None, "next_shard", "arm", "model", "contract", "start", "end", "warmup"])
def test_f03_resume_is_only_within_exact_same_shard_and_frozen_identity(mutation):
    specs = tw_eval.build_shard_specs(initial_capital_usdc=1000)
    target = specs[0]
    checkpoint = _shard_checkpoint(target)
    if mutation == "next_shard":
        target = specs[1]
        checkpoint["cut_ts_ms"] = target.start_ts_ms  # Old endpoint is not a fresh initializer.
    elif mutation == "arm":
        checkpoint["arm"] = "H60"
    elif mutation == "model":
        checkpoint["model_bundle_sha256"] = "d" * 64
    elif mutation == "contract":
        checkpoint["common_contract_sha256"] = "d" * 64
    elif mutation == "start":
        checkpoint["cut_ts_ms"] = target.start_ts_ms
    elif mutation == "end":
        checkpoint["cut_ts_ms"] = target.end_ts_ms_exclusive
    elif mutation == "warmup":
        checkpoint["cut_ts_ms"] = target.warmup_start_ts_ms
    kwargs = {"arm": "H120", "model_bundle_sha256": "b" * 64, "common_contract_sha256": "c" * 64}
    tw_eval.validate_shard_resume(target, None, **kwargs)
    if mutation is None:
        tw_eval.validate_shard_resume(target, checkpoint, **kwargs)
    else:
        with pytest.raises(ValueError, match="checkpoint"):
            tw_eval.validate_shard_resume(target, checkpoint, **kwargs)


@pytest.mark.parametrize("mutation", ["no_receipts", "wrong_row", "checkpoint_parent", "warmup_account", "incomplete", "liquidation"])
def test_f03_b_choice_rejects_invalid_shard_metadata_without_amount_reads(mutation):
    results = _utc_results()
    result = results["60"]
    if mutation == "no_receipts":
        results["60"] = replace(result, shards=())
    elif mutation == "wrong_row":
        result.rows[0]["shard_id"] = result.shards[1].spec.shard_id
    elif mutation == "checkpoint_parent":
        result.shards[1].initialization["account_checkpoint_parent"] = "old-checkpoint"
    elif mutation == "warmup_account":
        result.shards[0].initialization["history_scope"] = "market_and_account"
    elif mutation == "incomplete":
        result.shards[0].terminal["accounting_complete"] = False
    else:
        result.shards[0].terminal["terminal_liquidation_applied"] = True
    with pytest.raises(ValueError, match="shard|account|warmup|terminal"):
        tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)


@pytest.mark.parametrize("mutation", ["carry_cash", "carry_inventory", "open_order", "capital", "fees", "funding", "mtm", "net"])
def test_f03_post_choice_rejects_cross_shard_state_and_terminal_mismatch(mutation):
    results = _utc_results()
    choice = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    shard = results["60"].shards[1]
    first = results["60"].rows[2]
    if mutation == "carry_cash":
        first["start_cash_usdc"] = 2
    elif mutation == "carry_inventory":
        first["start_inventory_btc"] = 0.01
    elif mutation == "open_order":
        shard.initialization["initial_open_order_count"] = 1
    elif mutation == "capital":
        shard.initialization["initial_capital_usdc"] = 2000
    else:
        field = {"fees": "fees_usdc", "funding": "funding_cashflow_usdc",
                 "mtm": "inventory_btc", "net": "net_pnl_usdc"}[mutation]
        shard.terminal[field] += 1
    # These amounts are outside B or belong to whole-shard receipts.
    assert tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64) == choice
    with pytest.raises(ValueError, match="each shard must|terminal accounting"):
        tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64)


@pytest.mark.parametrize("phase,days,shards", [("development", 300, 150), ("final", 107, 54)])
def test_f03_each_shard_residual_inventory_is_marked_not_liquidated(phase, days, shards):
    result = _utc_results(phase=phase)["inf"]
    for receipt in result.shards:
        row = next(row for row in result.rows if row["end_ts_ms_exclusive"] == receipt.spec.end_ts_ms_exclusive)
        row["end_inventory_btc"] = 0.5
        row["end_cash_usdc"] -= 50
        receipt.terminal["inventory_btc"] = 0.5
        receipt.terminal["cash_usdc"] -= 50
    report = tw_eval.validate_independent_shards(result, phase=phase)
    assert report["days"] == report["net_pnl_usdc"] == days
    assert report["shard_count"] == shards
    assert all(row["end_inventory_btc"] == 0.5 for row in report["shards"])
    assert report["fees_usdc"] == -0.25 * days  # No fabricated closing fee or second deduction.
    assert report["funding_cashflow_usdc"] == 0.125 * days
    assert "end_inventory_btc" not in report  # Independent residuals cannot be one running position.
    assert report["max_shard_utc_close_drawdown_usdc"] == 0


def test_f03_business_baseline_is_paired_control_not_a_half_life_candidate():
    results = _utc_results()
    baseline = _utc_results()["inf"]
    for row in baseline.rows:
        row["arm"] = "business_b0"
    choice = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    report = tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64,
                                                        business_b0=baseline,
                                                        expected_business_b0_bundle_sha256=baseline.model_bundle_sha256)
    assert report["business_b0_role"] == "fixed_control_not_a_half_life_candidate"
    assert "business_b0" in report["arms"]
    assert len(choice.scores) == 4 and len(choice.input_identities) == 4
    assert report["business_b0_identity"] == {
        "result_sha256": baseline.result_sha256, "model_bundle_sha256": baseline.model_bundle_sha256,
        "head_names": list(baseline.head_names),
    }
    other = _utc_results(capital=2000)["inf"]
    with pytest.raises(ValueError, match="business_b0 must use the same"):
        tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64,
                                                    business_b0=other,
                                                    expected_business_b0_bundle_sha256=other.model_bundle_sha256)
    with pytest.raises(ValueError, match="predeclared fixed baseline"):
        tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64,
                                                    business_b0=baseline,
                                                    expected_business_b0_bundle_sha256="d" * 64)
    with pytest.raises(ValueError, match="SHA256"):
        tw_eval.report_development_after_selection(results, choice, common_contract_sha256="a" * 64,
                                                    business_b0=baseline)


def test_f03_frozen_choice_binds_shard_plan_not_just_opaque_result_identity():
    results = _utc_results()
    choice = tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
    changed = _utc_results(capital=2000)
    with pytest.raises(ValueError, match="shard contract changed"):
        tw_eval.report_development_after_selection(changed, choice, common_contract_sha256="a" * 64)


def test_f03_independent_zero_activity_shards_remain_in_denominator():
    result = _utc_results()["inf"]
    for row in result.rows:
        for key in row:
            if key.endswith(("_usdc", "_btc")):
                row[key] = 0
        row.update(start_mark_price=None, end_mark_price=None,
                   start_mark_clock_ts_ms=None, end_mark_clock_ts_ms=None)
    for shard in result.shards:
        for key in shard.terminal:
            if key.endswith(("_usdc", "_btc")):
                shard.terminal[key] = 0
    report = tw_eval.validate_independent_shards(result)
    assert report["days"] == 300 and report["shard_count"] == 150
    assert report["net_pnl_usdc"] == report["fees_usdc"] == report["funding_cashflow_usdc"] == 0


def test_f03_resets_do_not_create_artificial_drawdown():
    result = _utc_results()["inf"]
    # First independent account earns 100 then loses 10. The next account's
    # mandated zero-PnL start must not become an additional 90-unit drawdown.
    first, second = result.rows[:2]
    first.update(end_cash_usdc=100, end_equity_usdc=100, net_equity_change_usdc=100)
    second.update(start_cash_usdc=100, start_equity_usdc=100, end_cash_usdc=90,
                  end_equity_usdc=90, net_equity_change_usdc=-10)
    result.shards[0].terminal.update(cash_usdc=90, equity_usdc=90, net_pnl_usdc=90)
    report = tw_eval.validate_independent_shards(result)
    assert report["max_shard_utc_close_drawdown_usdc"] == 10
    assert report["net_pnl_usdc"] == 90 + 149 * 2


def test_f03_resume_rejects_noncanonical_target_even_without_checkpoint():
    spec = tw_eval.build_shard_specs(initial_capital_usdc=1000)[0]
    invalid = replace(spec, end_ts_ms_exclusive=spec.end_ts_ms_exclusive + tw_eval.DAY_MS)
    with pytest.raises(ValueError, match="exact planned shard"):
        tw_eval.validate_shard_resume(invalid, None, arm="H120",
                                      model_bundle_sha256="b" * 64, common_contract_sha256="c" * 64)


def test_f03_final_pairing_accepts_only_same_full_54_shards_without_selection():
    results = _utc_results(phase="final")
    pair = {name: results[name] for name in ("inf", "120")}
    assert len(tw_eval.validate_paired_shards(pair, phase="final")) == 54
    with pytest.raises(ValueError, match="calendar"):
        tw_eval.validate_paired_shards(pair)
    # Passing Final rows to the B selector cannot silently repurpose them.
    with pytest.raises(ValueError, match="calendar"):
        tw_eval.select_b_half_life(results, common_contract_sha256="a" * 64)
