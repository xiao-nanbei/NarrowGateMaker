import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

import research.families.f03_causal_13_head.ml_model as ml_model
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
    validate_model_bundle,
)


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
                "target": "dir_10s",
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
        target="dir_10s",
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
        if name.startswith("vol_"):
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
        "5409a398d845eaf9a990dbf4f390cfa3aeff2b7dd014fd02d70b303a2f8a557f"
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
        if name.startswith("vol_"):
            metadata["label_semantics"] = ABSOLUTE_PRICE_VARIANCE_SEMANTICS
        metadata_path = tmp_path / f"{name}_meta.json"
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        metadata_hashes[name] = hashlib.sha256(metadata_path.read_bytes()).hexdigest()

    authorization_file = (
        "live_canary_authorization.json"
        if legacy
        else "deployment_authorization.json"
    )
    with pytest.raises(ValueError, match=authorization_file):
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

    (tmp_path / "dir_10s.txt").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="model hash mismatch"):
        validate_model_bundle(tmp_path)
