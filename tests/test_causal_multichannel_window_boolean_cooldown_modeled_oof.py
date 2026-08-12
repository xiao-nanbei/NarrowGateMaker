from __future__ import annotations

import hashlib
import json
import time
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as modeled,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    freeze_causal_multichannel_cooldown_modeled_oof_execution as freeze_oof,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    ChronologicalFold,
    SearchConfig,
    TriLiteral,
    duration_vocabulary,
)
from research.governance.public_machine_projection import (
    projection_for,
    source_document_path,
    source_identity_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
OWNER_DOCS = ROOT / "research/families/f05_fill_quality_quote_ev/docs"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _write_canonical_json(path: Path, payload: dict[str, object]) -> str:
    canonical = modeled._canonical_sha256(payload)
    body = {**payload, "canonical_sha256": canonical}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, sort_keys=True), encoding="ascii")
    return _sha256(path)


def _quantile_definition(
    *,
    block: str,
    clock: str,
    source: str,
    threshold: float,
    quantile: float = 0.5,
) -> dict[str, object]:
    quantile_code = int(round(quantile * 10_000.0))
    return {
        "block": block,
        "category": None,
        "clock_group": clock,
        "kind": "quantile_ge",
        "name": f"tri::quantile::{source}::ge::q{quantile_code:04d}",
        "quantile": quantile,
        "source_field": source,
        "threshold": threshold,
    }


def _preserved_definition(*, block: str, clock: str, source: str) -> dict[str, object]:
    return {
        "block": block,
        "category": None,
        "clock_group": clock,
        "kind": "preserved_tri",
        "name": source,
        "quantile": None,
        "source_field": source,
        "threshold": None,
    }


def _predicate_bundle_fixture(
    tmp_path: Path,
) -> tuple[dict[str, object], Path, dict[str, str]]:
    root = tmp_path / "outcome_blind_predicates"
    book_name = "tri::quantile::x2::ge::q5000"
    book_name_q7500 = "tri::quantile::x2::ge::q7500"
    trade_name = "tri::quantile::x3::ge::q5000"
    trade_name_q7500 = "tri::quantile::x3::ge::q7500"
    artifact_rows: dict[str, dict[str, dict[str, str]]] = {"book": {}, "trade": {}}
    for clock in modeled.PREDICATE_CLOCKS:
        for side in modeled.SIDES:
            if clock == "book":
                definitions = [
                    _preserved_definition(block="R0", clock=clock, source="p0"),
                    _quantile_definition(
                        block="R0",
                        clock=clock,
                        source="x0",
                        threshold=-10.0,
                    ),
                    _preserved_definition(block="M1", clock=clock, source="p2"),
                    _quantile_definition(
                        block="M1",
                        clock=clock,
                        source="x2",
                        threshold=1.0 if side == "BUY" else 2.0,
                    ),
                    _quantile_definition(
                        block="M1",
                        clock=clock,
                        source="x2",
                        threshold=2.5 if side == "BUY" else 3.5,
                        quantile=0.75,
                    ),
                ]
                clock_identity = "provider_local_receive_time_right_boundary_100ms"
            else:
                definitions = [
                    _preserved_definition(block="M2", clock=clock, source="p3"),
                    _quantile_definition(
                        block="M2",
                        clock=clock,
                        source="x3",
                        threshold=3.0 if side == "BUY" else 4.0,
                    ),
                    _quantile_definition(
                        block="M2",
                        clock=clock,
                        source="x3",
                        threshold=5.0 if side == "BUY" else 6.0,
                        quantile=0.75,
                    ),
                ]
                clock_identity = "binance_exchange_trade_time"
            input_fields = sorted(
                {str(definition["source_field"]) for definition in definitions}
            )
            artifact_payload: dict[str, object] = {
                "schema_version": modeled.PREDICATE_ARTIFACT_SCHEMA,
                "identity": modeled.PREDICATE_ARTIFACT_IDENTITY,
                "side": side,
                "source_role": "outcome_blind_2025_single_channel",
                "source_clock_identity": {"shared": clock_identity},
                "clock_separated_2025": True,
                "clause_clock_policy": "single_book_or_trade_clock_group",
                "cross_channel_threshold_fitting": False,
                "input_schema": [["side", "text"]]
                + [[name, "numeric"] for name in input_fields],
                "quantiles": [0.5, 0.75],
                "reference_days": ["2025-08-02", "2025-08-03"],
                "reference_identity_sha256": modeled._canonical_sha256(
                    {"clock": clock, "side": side, "outcomes_read": False}
                ),
                "definitions": definitions,
            }
            relative = Path("artifacts") / f"{clock}_{side.lower()}.json"
            artifact_hash = _write_canonical_json(root / relative, artifact_payload)
            artifact_rows[clock][side] = {
                "path": relative.as_posix(),
                "sha256": artifact_hash,
            }
    bundle_payload: dict[str, object] = {
        "schema_version": modeled.PREDICATE_BUNDLE_SCHEMA,
        "identity": modeled.PREDICATE_ARTIFACT_IDENTITY,
        "book": artifact_rows["book"],
        "trade": artifact_rows["trade"],
        "m0_artifacts": [],
        "cross_clock_clause_authorized": False,
        "cross_clock_clause_scope": "2025_reference_rows_only",
        "strict_2026_target_snapshot": {
            "authority_owner": "2026_strict_denominator_study",
            "book_trade_predicates_may_be_combined_by_study": True,
            "required_condition": (
                "book and trade predicates are evaluated on the same admitted strict "
                "target snapshot and causal feature-ready cutoff"
            ),
        },
    }
    bundle_path = root / "predicate_bundle.json"
    bundle_hash = _write_canonical_json(bundle_path, bundle_payload)
    contract: dict[str, object] = {
        "role": "predicate_threshold_scale_support_and_missingness_only",
        "predicate_bundle_path": str(bundle_path),
        "predicate_bundle_file_sha256": bundle_hash,
        "economic_outcomes_read": False,
        "cooldown_labels_generated": False,
        "queue_or_lifecycle_authority": False,
        "source_identity_is_model_input": False,
    }
    return contract, bundle_path, {
        "book": book_name,
        "book_q7500": book_name_q7500,
        "trade": trade_name,
        "trade_q7500": trade_name_q7500,
    }


def _days(count: int = 8) -> tuple[str, ...]:
    return tuple(
        (pd.Timestamp("2026-01-01") + pd.Timedelta(days=index)).strftime("%Y-%m-%d")
        for index in range(count)
    )


def _feature_blocks() -> dict[str, modeled.FeatureBlockSpec]:
    return {
        "R0": modeled.FeatureBlockSpec(("p0",), ("x0",)),
        "M0": modeled.FeatureBlockSpec(("p0", "p1"), ("x0", "x1")),
        "M1": modeled.FeatureBlockSpec(
            ("p0", "p1", "p2"),
            ("x0", "x1", "x2"),
        ),
        "M2": modeled.FeatureBlockSpec(
            ("p0", "p1", "p2", "p3"),
            ("x0", "x1", "x2", "x3"),
        ),
    }


def _manual_config(
    tmp_path: Path,
    *,
    days: tuple[str, ...],
    opportunities: int,
    censored_opportunities: int,
) -> modeled.FrozenConfig:
    config_path = tmp_path / "config.json"
    spec_path = tmp_path / "spec.json"
    config_path.write_text("{}\n", encoding="ascii")
    spec_path.write_text("{}\n", encoding="ascii")
    common_days = tuple(day for index, day in enumerate(days) if index not in {1, 5})
    outer40 = ChronologicalFold("prefix40.outer1", days[:5], days[5:])
    outer33 = ChronologicalFold("prefix33.outer1", common_days[:4], common_days[4:])
    dummy = modeled.ArtifactSpec(tmp_path / "missing.json", "0" * 64, ("*.parquet",), None)
    return modeled.FrozenConfig(
        path=config_path,
        sha256=_sha256(config_path),
        payload={},
        spec_path=spec_path,
        spec_sha256=_sha256(spec_path),
        spec_payload={
            "modeled_label_source": {
                "opportunity_rows": opportunities,
                "arm_rows": opportunities * 8,
                "joint_censored_opportunities": censored_opportunities,
                "point_label_eligible_opportunities": opportunities - censored_opportunities,
            }
        },
        labels=dummy,
        features=dummy,
        columns=modeled.ColumnContract(),
        prefix40_days=days,
        added10_days=(),
        report_scopes=("prefix40_modeled_label_development",),
        panel_days={
            "prefix40_modeled_label_development": days,
            "prefix33_raw_m2_common_support": common_days,
        },
        panel_feature_blocks={
            "prefix40_modeled_label_development": ("R0", "M0", "M1"),
            "prefix33_raw_m2_common_support": ("R0", "M0", "M1", "M2"),
        },
        not_run_panels={
            "added10": {"status": "not_run_not_imputed"},
            "pooled50": {"status": "not_run_not_imputed"},
        },
        outer_folds={
            "prefix40_modeled_label_development": (outer40,),
            "prefix33_raw_m2_common_support": (outer33,),
        },
        feature_blocks=_feature_blocks(),
        search=SearchConfig(
            max_literals_per_clause=1,
            max_clauses_per_rule=1,
            max_rules_per_policy=1,
            max_clause_candidates=8,
            max_rule_candidates=8,
            max_policy_candidates=8,
            inner_folds=2,
            inner_minimum_train_days=2,
            minimum_action_opportunities=1,
            minimum_action_campaigns=1,
            minimum_action_days=1,
            confidence=0.95,
        ),
        continuous=modeled.ContinuousConfig(
            max_depth_candidates=(1, 2),
            min_samples_leaf=1,
            minimum_train_rows_per_action=1,
            random_state=7,
        ),
        deployment=modeled.DeploymentConfig(
            economic_epsilon_usdc=0.0,
            minimum_action_rate=0.0,
            minimum_action_campaigns=1,
            minimum_action_days=1,
            require_full_identification=False,
            require_outer_fold_nonbaseline_action=True,
            require_opener_and_add_reporting=True,
        ),
        minimum_inner_identified_weight_fraction=0.0,
        uplift_bounds_usdc=None,
        predicate_channel_groups={},
        predicate_semantic_groups={},
        predicate_clock_groups={},
        code_bindings=(),
        expected_library_versions=modeled.runtime_library_versions(),
    )


def _label_and_feature_rows(
    days: tuple[str, ...],
    *,
    sides: tuple[str, ...] = ("BUY",),
    censored_ids: frozenset[str] = frozenset(),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    labels: list[dict[str, object]] = []
    features: list[dict[str, object]] = []
    for day_index, day in enumerate(days):
        day_start = pd.Timestamp(day, tz="UTC").value
        for side_index, side in enumerate(sides):
            for role_index, role in enumerate(("opener", "add")):
                opportunity = f"{day}-{side}-{role}"
                assignment = day_start + (side_index * 10 + role_index + 1) * 1_000_000_000
                censored = opportunity in censored_ids
                features.append(
                    {
                        "opportunity_id": opportunity,
                        "p0": 1,
                        "p1": int((day_index + role_index) % 2 == 0),
                        "p2": -1 if day_index == 0 else int(day_index % 2 == 0),
                        "p3": int((day_index + side_index) % 3 == 0),
                        "x0": float(day_index),
                        "x1": float(role_index),
                        "x2": float(side_index),
                        "x3": float(day_index + role_index + side_index),
                    }
                )
                for action_index, action in enumerate(duration_vocabulary(side)):
                    labels.append(
                        {
                            "opportunity_id": opportunity,
                            "utc_day": day,
                            "side": side,
                            "role_at_fill": role,
                            "campaign_id": opportunity,
                            "duration_policy_id": action,
                            "assignment_to_washout_value_usdc": (
                                0.0 if action == CONTROL_ACTION else -0.1 - action_index / 100.0
                            ),
                            "assignment_ts_ns": assignment,
                            "washout_ts_ns": assignment + (action_index + 1) * 1_000_000,
                            "training_label_eligible": not censored,
                            "right_censored": False,
                            "joint_censored": censored,
                            "exact_queue_policy_eligible": False,
                        }
                    )
    return pd.DataFrame(labels), pd.DataFrame(features)


def _prepared_panel(
    tmp_path: Path,
    *,
    sides: tuple[str, ...] = ("BUY",),
    censored_ids: frozenset[str] = frozenset(),
) -> tuple[modeled.PreparedPanel, modeled.FrozenConfig, pd.DataFrame]:
    days = _days()
    labels, features = _label_and_feature_rows(days, sides=sides, censored_ids=censored_ids)
    excluded = {days[1], days[5]}
    missing_m2 = labels.loc[labels["utc_day"].isin(excluded), "opportunity_id"].unique()
    features.loc[features["opportunity_id"].isin(missing_m2), ["p3", "x3"]] = np.nan
    config = _manual_config(
        tmp_path,
        days=days,
        opportunities=features["opportunity_id"].nunique(),
        censored_opportunities=len(censored_ids),
    )
    return modeled.prepare_modeled_panel(labels, features, config=config), config, labels


def _execution_amendment_fixture(
    tmp_path: Path,
) -> tuple[modeled.FrozenConfig, Path, dict[str, object]]:
    config = _manual_config(
        tmp_path,
        days=_days(),
        opportunities=16,
        censored_opportunities=0,
    )
    config.path.write_text(json.dumps({"identity": modeled.IDENTITY}), encoding="ascii")
    config.spec_path.write_text(json.dumps({"identity": modeled.IDENTITY}), encoding="ascii")
    label_manifest = tmp_path / "label_manifest.json"
    label_manifest.write_text(json.dumps({"identity": "synthetic_label_identity"}), encoding="ascii")
    feature_root = tmp_path / "feature_panel"
    feature_root.mkdir()
    feature_manifest = feature_root / "panel_manifest.json"
    feature_manifest.write_text(
        json.dumps({"identity": modeled.EXPECTED_FEATURE_MANIFEST_IDENTITY}),
        encoding="ascii",
    )
    (feature_root / modeled.FEATURE_PANEL_SUCCESS_NAME).write_text(
        _sha256(feature_manifest) + "\n",
        encoding="ascii",
    )
    config = replace(
        config,
        sha256=_sha256(config.path),
        spec_sha256=_sha256(config.spec_path),
        labels=modeled.ArtifactSpec(
            label_manifest,
            _sha256(label_manifest),
            ("*.parquet",),
            "synthetic_label_identity",
        ),
        features=modeled.ArtifactSpec(
            feature_manifest,
            _sha256(feature_manifest),
            ("*.parquet",),
            modeled.EXPECTED_FEATURE_MANIFEST_IDENTITY,
        ),
    )
    artifacts = {
        "frozen_config": {
            "path": str(config.path.resolve()),
            "sha256": config.sha256,
            "identity": modeled.IDENTITY,
        },
        "frozen_owner_spec": {
            "path": str(config.spec_path.resolve()),
            "sha256": config.spec_sha256,
            "identity": modeled.IDENTITY,
        },
        "modeled_label_manifest": {
            "path": str(config.labels.manifest_path.resolve()),
            "sha256": config.labels.manifest_sha256,
            "identity": config.labels.expected_identity,
        },
        "feature_panel_manifest": {
            "path": str(config.features.manifest_path.resolve()),
            "sha256": config.features.manifest_sha256,
            "identity": modeled.EXPECTED_FEATURE_MANIFEST_IDENTITY,
        },
    }
    payload: dict[str, object] = {
        "schema_version": modeled.EXECUTION_AMENDMENT_SCHEMA,
        "identity": modeled.IDENTITY,
        "status": "frozen_before_owner_oof_economic_read",
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "artifact_bindings": artifacts,
        "code_bindings": [
            {"path": str(path), "sha256": _sha256(path)}
            for path in modeled._required_oof_code_paths()
        ],
        "library_versions": modeled.runtime_library_versions(),
    }
    payload["execution_identity_sha256"] = modeled._canonical_sha256(payload)
    amendment = tmp_path / "oof_execution_amendment.json"
    amendment.write_text(json.dumps(payload), encoding="ascii")
    return config, amendment, payload


def test_real_frozen_config_binds_prefix40_and_prefix33(tmp_path: Path) -> None:
    feature_manifest = tmp_path / "feature_manifest.json"
    blocks = {
        name: {
            "boolean_predicates": list(block.boolean_predicates),
            "continuous_features": list(block.continuous_features),
        }
        for name, block in _feature_blocks().items()
    }
    public_spec_path = OWNER_DOCS / (
        "causal_multichannel_window_boolean_cooldown_duration_v2_"
        "owner_modeled_queue_v1_spec_20260811.json"
    )
    projection = projection_for(public_spec_path)
    assert projection is not None
    assert _sha256(public_spec_path) == projection.public_projection_sha256
    assert source_identity_sha256(public_spec_path) == modeled.DEFAULT_SPEC_SHA256
    source_spec_path = public_spec_path
    if projection.private_source_available:
        source_spec_path = source_document_path(public_spec_path, require_private=True)
        assert source_spec_path == projection.private_source_path
        assert _sha256(source_spec_path) == modeled.DEFAULT_SPEC_SHA256
        assert (
            modeled._require_exact_source_document(
                public_spec_path,
                modeled.DEFAULT_SPEC_SHA256,
                role="frozen owner spec",
            )
            == source_spec_path
        )
    owner_spec = json.loads(source_spec_path.read_text(encoding="utf-8"))
    predicate_contract, _bundle_path, predicate_names = _predicate_bundle_fixture(tmp_path)
    owner_spec["outcome_blind_2025_input"] = predicate_contract
    spec_path = tmp_path / "owner_spec.json"
    spec_path.write_text(json.dumps(owner_spec), encoding="ascii")
    spec_sha256 = _sha256(spec_path)
    prefix40 = owner_spec["development_days"]
    excluded = set(owner_spec["analysis_panels"]["prefix33_raw_m2_common_support"]["excluded_days"])
    prefix33 = [day for day in prefix40 if day not in excluded]
    feature_manifest.write_text(
        json.dumps(
            {
                "identity": modeled.EXPECTED_FEATURE_MANIFEST_IDENTITY,
                "files": [],
                "feature_schema": {"feature_blocks": blocks, "predicate_groups": {}},
                "label_join_key": "opportunity_id",
                "economic_outcomes_read": False,
                "arm_economic_labels_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "frozen_support_split": {
                    "prefix40_days": prefix40,
                    "m2_common_support_days": prefix33,
                },
            }
        ),
        encoding="ascii",
    )
    (tmp_path / modeled.FEATURE_PANEL_SUCCESS_NAME).write_text(
        _sha256(feature_manifest) + "\n",
        encoding="ascii",
    )
    config_path = OWNER_DOCS / (
        "causal_multichannel_window_boolean_cooldown_duration_v2_"
        "owner_modeled_queue_v1_study_config_20260811.json"
    )
    config = modeled.load_frozen_config(
        config_path,
        expected_sha256=modeled.DEFAULT_CONFIG_SHA256,
        spec_path=spec_path,
        expected_spec_sha256=spec_sha256,
        feature_manifest_path=feature_manifest,
        feature_manifest_sha256=_sha256(feature_manifest),
        feature_table_globs=("features.parquet",),
    )
    assert len(config.panel_days["prefix40_modeled_label_development"]) == 40
    assert len(config.panel_days["prefix33_raw_m2_common_support"]) == 33
    assert config.panel_feature_blocks["prefix40_modeled_label_development"] == (
        "R0",
        "M0",
        "M1",
    )
    assert config.panel_feature_blocks["prefix33_raw_m2_common_support"] == (
        "R0",
        "M0",
        "M1",
        "M2",
    )
    assert set(config.not_run_panels) == {"added10", "pooled50"}
    assert config.feature_blocks["R0"] == _feature_blocks()["R0"]
    assert predicate_names["book"] in config.feature_blocks["M1"].boolean_predicates
    assert predicate_names["book"] in config.feature_blocks["M2"].boolean_predicates
    assert predicate_names["trade"] not in config.feature_blocks["M1"].boolean_predicates
    assert predicate_names["trade"] in config.feature_blocks["M2"].boolean_predicates
    assert config.predicate_clock_groups[predicate_names["book"]] == "book"
    assert config.predicate_clock_groups[predicate_names["trade"]] == "trade"
    success_path = tmp_path / modeled.FEATURE_PANEL_SUCCESS_NAME
    success_path.unlink()
    with pytest.raises(modeled.ModeledOofError, match="lacks _PANEL_SUCCESS"):
        modeled.load_frozen_config(
            config_path,
            expected_sha256=modeled.DEFAULT_CONFIG_SHA256,
            spec_path=spec_path,
            expected_spec_sha256=spec_sha256,
            feature_manifest_path=feature_manifest,
            feature_manifest_sha256=_sha256(feature_manifest),
        )
    success_path.write_text("0" * 64 + "\n", encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="marker SHA256 mismatch"):
        modeled.load_frozen_config(
            config_path,
            expected_sha256=modeled.DEFAULT_CONFIG_SHA256,
            spec_path=spec_path,
            expected_spec_sha256=spec_sha256,
            feature_manifest_path=feature_manifest,
            feature_manifest_sha256=_sha256(feature_manifest),
        )
    success_path.write_text(_sha256(feature_manifest) + "\n", encoding="ascii")
    wrong_identity = json.loads(feature_manifest.read_text(encoding="ascii"))
    wrong_identity["identity"] = "self_reported_but_untrusted_identity"
    feature_manifest.write_text(json.dumps(wrong_identity), encoding="ascii")
    success_path.write_text(_sha256(feature_manifest) + "\n", encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="feature manifest identity"):
        modeled.load_frozen_config(
            config_path,
            expected_sha256=modeled.DEFAULT_CONFIG_SHA256,
            spec_path=spec_path,
            expected_spec_sha256=spec_sha256,
            feature_manifest_path=feature_manifest,
            feature_manifest_sha256=_sha256(feature_manifest),
        )
    wrong_identity["identity"] = modeled.EXPECTED_FEATURE_MANIFEST_IDENTITY
    feature_manifest.write_text(json.dumps(wrong_identity), encoding="ascii")
    success_path.write_text(_sha256(feature_manifest) + "\n", encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="config SHA256"):
        modeled.load_frozen_config(
            config_path,
            expected_sha256="0" * 64,
            spec_path=spec_path,
            expected_spec_sha256=spec_sha256,
            feature_manifest_path=feature_manifest,
            feature_manifest_sha256=_sha256(feature_manifest),
        )
    invalid_manifest = json.loads(feature_manifest.read_text(encoding="ascii"))
    invalid_manifest["economic_outcomes_read"] = True
    feature_manifest.write_text(json.dumps(invalid_manifest), encoding="ascii")
    success_path.write_text(_sha256(feature_manifest) + "\n", encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="outcome-blind provenance"):
        modeled.load_frozen_config(
            config_path,
            expected_sha256=modeled.DEFAULT_CONFIG_SHA256,
            spec_path=spec_path,
            expected_spec_sha256=spec_sha256,
            feature_manifest_path=feature_manifest,
            feature_manifest_sha256=_sha256(feature_manifest),
        )


def test_2025_thresholds_enter_candidates_by_side_without_mutating_r0(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract, _bundle_path, names = _predicate_bundle_fixture(tmp_path)
    bundle = modeled.load_2025_predicate_bundle(contract)
    original_blocks = _feature_blocks()
    (
        binding,
        feature_blocks,
        channel_groups,
        semantic_groups,
        clock_groups,
    ) = modeled.bind_2025_predicate_materialization(
        bundle,
        feature_blocks=original_blocks,
        predicate_channel_groups={},
        predicate_semantic_groups={},
        predicate_clock_groups={},
    )
    assert feature_blocks["R0"] == original_blocks["R0"]
    assert binding.predicate_names_by_block["R0"] == ()
    assert names["book"] in feature_blocks["M1"].boolean_predicates
    assert names["book"] in feature_blocks["M2"].boolean_predicates
    assert names["trade"] not in feature_blocks["M1"].boolean_predicates
    assert names["trade"] in feature_blocks["M2"].boolean_predicates
    assert len(binding.predicate_names_by_block["M1"]) == 2
    assert len(binding.predicate_names_by_block["M2"]) == 4
    assert channel_groups[names["book"]] == "outcome_blind_2025_threshold"
    assert semantic_groups[names["book"]] == "quantile_ge_q5000"
    assert clock_groups[names["book"]] == "book"
    assert clock_groups[names["trade"]] == "trade"
    assert clock_groups["p2"] == "book"
    assert clock_groups["p3"] == "trade"

    index = pd.Index(["buy", "sell", "missing", "infinite"], name="opportunity_id")
    inputs = pd.DataFrame(
        {
            "p0": [1, 0, 1, 0],
            "p2": [1, 1, -1, 0],
            "p3": [0, 1, 0, -1],
            "x2": [1.5, 1.5, np.nan, np.inf],
            "x3": [3.5, 3.5, np.nan, -np.inf],
        },
        index=index,
    )
    metadata = pd.DataFrame(
        {"side": ["BUY", "SELL", "BUY", "SELL"]},
        index=index,
    )
    immutable_r0 = inputs["p0"].copy()
    numeric_conversion_count: dict[str, int] = {}
    original_to_numeric = modeled.pd.to_numeric

    def counted_to_numeric(values, *args, **kwargs):
        if isinstance(values, pd.Series) and values.name in {"x2", "x3"}:
            numeric_conversion_count[str(values.name)] = (
                numeric_conversion_count.get(str(values.name), 0) + 1
            )
        return original_to_numeric(values, *args, **kwargs)

    monkeypatch.setattr(modeled.pd, "to_numeric", counted_to_numeric)
    materialized = modeled.materialize_2025_predicates(
        inputs,
        metadata,
        binding=binding,
    )
    pd.testing.assert_series_equal(materialized["p0"], immutable_r0)
    assert materialized[names["book"]].tolist() == [1, 0, -1, -1]
    assert materialized[names["trade"]].tolist() == [1, 0, -1, -1]
    assert numeric_conversion_count == {"x2": 1, "x3": 1}

    action = duration_vocabulary("BUY")[1]
    positive_policy = BooleanCooldownPolicy(
        side="BUY",
        rules=(
            BooleanRule(
                action=action,
                clauses=(AndClause((TriLiteral(names["book"]),)),),
            ),
        ),
    )
    selected = positive_policy.choose(materialized[[names["book"]]])
    assert selected.tolist() == [action, CONTROL_ACTION, CONTROL_ACTION, CONTROL_ACTION]
    negated = TriLiteral(names["book"], negated=True).evaluate(
        materialized[[names["book"]]]
    )
    assert negated.tolist() == [0, 1, -1, -1]

    preflight_binding = modeled.predicate_materialization_binding_payload(binding)
    assert len(preflight_binding["artifacts"]) == 4
    assert preflight_binding["bundle"]["artifact_binding_sha256"] == (
        bundle.artifact_binding_sha256
    )
    assert preflight_binding["materialization_identity_sha256"] == (
        binding.materialization_identity_sha256
    )
    assert preflight_binding["materialized_predicate_counts"] == {
        "R0": 0,
        "M0": 0,
        "M1": 2,
        "M2": 4,
    }
    assert preflight_binding["materialized_source_counts"] == {
        "R0": 0,
        "M0": 0,
        "M1": 1,
        "M2": 2,
    }


def test_2025_predicate_bundle_missing_artifact_fails_fast(tmp_path: Path) -> None:
    contract, bundle_path, _names = _predicate_bundle_fixture(tmp_path)
    bundle_payload = json.loads(bundle_path.read_text(encoding="ascii"))
    missing = bundle_path.parent / bundle_payload["trade"]["SELL"]["path"]
    missing.unlink()
    with pytest.raises(modeled.ModeledOofError, match="predicate artifact is missing"):
        modeled.load_2025_predicate_bundle(contract)


def test_execution_amendment_binds_artifacts_code_and_libraries_before_read(
    tmp_path: Path,
) -> None:
    config, amendment, payload = _execution_amendment_fixture(tmp_path)
    bound_config, binding = modeled.load_execution_amendment(
        amendment,
        expected_sha256=_sha256(amendment),
        config=config,
    )
    assert binding.path == amendment.resolve()
    assert binding.execution_identity_sha256 == payload["execution_identity_sha256"]
    assert dict(binding.library_versions) == modeled.runtime_library_versions()
    assert tuple(bound_config.code_bindings) == binding.code_bindings
    assert set(binding.artifact_bindings) == {
        "frozen_config",
        "frozen_owner_spec",
        "modeled_label_manifest",
        "feature_panel_manifest",
    }

    tampered = dict(payload)
    tampered["code_bindings"] = [dict(row) for row in payload["code_bindings"]]  # type: ignore[arg-type]
    tampered["code_bindings"][0]["sha256"] = "0" * 64  # type: ignore[index]
    tampered.pop("execution_identity_sha256")
    tampered["execution_identity_sha256"] = modeled._canonical_sha256(tampered)
    amendment.write_text(json.dumps(tampered), encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="code binding drifted"):
        modeled.load_execution_amendment(
            amendment,
            expected_sha256=_sha256(amendment),
            config=config,
        )


def test_execution_amendment_explicitly_binds_2025_predicate_artifacts(
    tmp_path: Path,
) -> None:
    config, amendment, payload = _execution_amendment_fixture(tmp_path)
    contract, _bundle_path, _names = _predicate_bundle_fixture(tmp_path)
    bundle = modeled.load_2025_predicate_bundle(contract)
    predicate_binding = modeled.PredicateMaterializationBinding(
        bundle=bundle,
        definitions_by_block_side={},
        predicate_names_by_block={},
        source_fields_by_block={},
        materialization_identity_sha256="1" * 64,
    )
    config = replace(config, predicate_materialization=predicate_binding)
    artifact_rows = freeze_oof._predicate_artifact_bindings(
        {"outcome_blind_2025_input": contract}
    )
    payload["artifact_bindings"].update(artifact_rows)  # type: ignore[union-attr]
    payload.pop("execution_identity_sha256")
    payload["execution_identity_sha256"] = modeled._canonical_sha256(payload)
    amendment.write_text(json.dumps(payload), encoding="ascii")

    _bound_config, binding = modeled.load_execution_amendment(
        amendment,
        expected_sha256=_sha256(amendment),
        config=config,
    )
    expected_names = {
        "outcome_blind_2025_predicate_bundle",
        "outcome_blind_2025_predicate_book.BUY",
        "outcome_blind_2025_predicate_book.SELL",
        "outcome_blind_2025_predicate_trade.BUY",
        "outcome_blind_2025_predicate_trade.SELL",
    }
    assert expected_names <= set(binding.artifact_bindings)

    tampered = json.loads(json.dumps(payload))
    tampered["artifact_bindings"][
        "outcome_blind_2025_predicate_trade.SELL"
    ]["canonical_sha256"] = "0" * 64
    tampered.pop("execution_identity_sha256")
    tampered["execution_identity_sha256"] = modeled._canonical_sha256(tampered)
    amendment.write_text(json.dumps(tampered), encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="canonical identity drifted"):
        modeled.load_execution_amendment(
            amendment,
            expected_sha256=_sha256(amendment),
            config=config,
        )

    library_tampered = json.loads(json.dumps(payload))
    library_tampered["library_versions"]["numpy"] = "drifted"
    library_tampered.pop("execution_identity_sha256")
    library_tampered["execution_identity_sha256"] = modeled._canonical_sha256(
        library_tampered
    )
    amendment.write_text(json.dumps(library_tampered), encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="library versions drifted"):
        modeled.load_execution_amendment(
            amendment,
            expected_sha256=_sha256(amendment),
            config=config,
        )

    amendment.write_text(json.dumps(payload), encoding="ascii")
    config.labels.manifest_path.write_text('{"identity":"tampered"}', encoding="ascii")
    with pytest.raises(modeled.ModeledOofError, match="modeled_label_manifest bytes drifted"):
        modeled.load_execution_amendment(
            amendment,
            expected_sha256=_sha256(amendment),
            config=config,
        )


def test_oof_cli_requires_execution_amendment_binding() -> None:
    with pytest.raises(SystemExit):
        modeled._parser().parse_args(
            [
                "preflight",
                "--feature-manifest",
                "panel_manifest.json",
                "--feature-manifest-sha256",
                "0" * 64,
            ]
        )


def test_run_cli_workers_default_and_validation() -> None:
    required = [
        "--feature-manifest",
        "panel_manifest.json",
        "--feature-manifest-sha256",
        "0" * 64,
        "--execution-amendment",
        "execution_amendment.json",
        "--execution-amendment-sha256",
        "1" * 64,
        "--output",
        "output",
    ]
    default = modeled._parser().parse_args(["run", *required])
    explicit = modeled._parser().parse_args(["run", *required, "--workers", "3"])
    assert default.workers == 1
    assert explicit.workers == 3
    with pytest.raises(SystemExit):
        modeled._parser().parse_args(["run", *required, "--workers", "0"])


def test_censored_arms_stay_null_and_m2_missing_is_panel_scoped(tmp_path: Path) -> None:
    censored = frozenset({_days()[2] + "-BUY-opener"})
    panel, config, _ = _prepared_panel(tmp_path, censored_ids=censored)
    opportunity = next(iter(censored))
    assert panel.outcomes.loc[opportunity].isna().all()
    assert not panel.supported.loc[opportunity].any()
    assert panel.redacted_finite_outcomes == 8
    assert panel.features.loc[opportunity, "p3"] in {-1, 0, 1}
    excluded_opportunity = _days()[1] + "-BUY-opener"
    assert pd.isna(panel.features.loc[excluded_opportunity, "p3"])
    rows = modeled._evaluate_actions(
        panel,
        side="BUY",
        opportunity_index=panel.metadata.index,
        actions=[duration_vocabulary("BUY")[1]] * len(panel.metadata),
        fold_id="diagnostic",
        stage="test",
        candidate_id="candidate",
    )
    partial = modeled.partial_identification(
        rows,
        confidence=config.search.confidence,
        uplift_bounds_usdc=None,
    )
    assert partial.unidentified_opportunities == 1
    assert not partial.point_identified
    assert partial.population_lower_bound_usdc is None
    assert partial.population_upper_bound_usdc is None

    control_rows = modeled._evaluate_actions(
        panel,
        side="BUY",
        opportunity_index=panel.metadata.index,
        actions=[CONTROL_ACTION] * len(panel.metadata),
        fold_id="diagnostic-control",
        stage="test",
        candidate_id="control-consistency",
    )
    censored_control = control_rows.loc[
        control_rows["opportunity_id"] == opportunity
    ].iloc[0]
    assert bool(censored_control["point_identified"])
    assert bool(censored_control["contrast_identified_by_same_action_consistency"])
    assert float(censored_control["uplift_usdc"]) == 0.0
    assert pd.isna(censored_control["selected_value_usdc"])
    assert pd.isna(censored_control["control_value_usdc"])
    control_partial = modeled.partial_identification(
        control_rows,
        confidence=config.search.confidence,
        uplift_bounds_usdc=None,
    )
    assert control_partial.point_identified
    assert control_partial.unidentified_opportunities == 0


def test_campaign_cluster_identity_is_day_side_source_scoped(tmp_path: Path) -> None:
    days = _days()
    labels, features = _label_and_feature_rows(days, sides=("BUY", "SELL"))
    labels["campaign_id"] = 1
    config = _manual_config(
        tmp_path,
        days=days,
        opportunities=features["opportunity_id"].nunique(),
        censored_opportunities=0,
    )
    panel = modeled.prepare_modeled_panel(labels, features, config=config)
    assert panel.metadata["source_campaign_id"].nunique() == 1
    assert panel.metadata["campaign_cluster_id"].nunique() == len(days) * 2
    assert panel.metadata["campaign_cluster_id"].str.count("::").eq(2).all()
    grouped = panel.metadata.groupby("campaign_cluster_id", observed=True)
    cluster_days = grouped["utc_day"].nunique()
    cluster_sides = grouped["side"].nunique()
    assert cluster_days.eq(1).all()
    assert cluster_sides.eq(1).all()


def test_purge_uses_max_arm_washout_and_strict_boundary(tmp_path: Path) -> None:
    days = _days()
    labels, features = _label_and_feature_rows(days)
    crossing_opportunity = days[0] + "-BUY-opener"
    test_boundary = int(labels.loc[labels["utc_day"] == days[2], "assignment_ts_ns"].min())
    target = (labels["opportunity_id"] == crossing_opportunity) & (
        labels["duration_policy_id"] == duration_vocabulary("BUY")[-1]
    )
    labels.loc[target, "washout_ts_ns"] = test_boundary
    config = _manual_config(
        tmp_path,
        days=days,
        opportunities=features["opportunity_id"].nunique(),
        censored_opportunities=0,
    )
    panel = modeled.prepare_modeled_panel(labels, features, config=config)
    kept, audit = modeled.observation_end_aware_purge(
        panel,
        side="BUY",
        train_days=days[:2],
        test_days=(days[2],),
        fold_id="purge-test",
        stage="outer_boolean",
    )
    assert crossing_opportunity not in kept
    assert audit.purged_cross_boundary == 1
    assert audit.test_boundary_ts_ns == test_boundary


def test_negative_boolean_candidate_enters_outer_oof_before_gate(tmp_path: Path) -> None:
    panel, config, _ = _prepared_panel(tmp_path)
    crossing_opportunity = _days()[2] + "-BUY-opener"
    outer = config.outer_folds["prefix40_modeled_label_development"][0]
    outer_test = panel.metadata.index[
        (panel.metadata["side"] == "BUY")
        & panel.metadata["utc_day"].isin(outer.test_days)
    ]
    outer_boundary = int(panel.metadata.loc[outer_test, "assignment_ts_ns"].min())
    observation_end = panel.observation_end_ts_ns.copy()
    observation_end.loc[crossing_opportunity] = outer_boundary
    panel = replace(panel, observation_end_ts_ns=observation_end)
    action = duration_vocabulary("BUY")[1]
    candidate = BooleanCooldownPolicy(
        side="BUY",
        rules=(
            BooleanRule(
                action=action,
                clauses=(AndClause((TriLiteral("p0"),)),),
            ),
        ),
    )
    result = modeled.run_boolean_nested_oof(
        panel,
        config=config,
        side="BUY",
        feature_block_name="R0",
        scope="prefix40_modeled_label_development",
        candidate_policies=(candidate,),
    )
    assert result.oof_rows["selected_nonbaseline"].all()
    assert set(result.oof_rows["selected_action"]) == {action}
    assert result.partial_identification.identified_mean_usdc < 0
    assert result.deployment_gate["evaluated_after_outer_oof"] is True
    assert result.deployment_gate["decision"] == "abstain"
    assert len(result.purge_audits) == 1
    assert result.purge_audits[0].stage == "outer_boolean_candidate_selection_guard"
    fold_report = result.fold_reports[0]
    assert fold_report["candidate_replaced_by_baseline_before_outer_oof"] is False
    assert fold_report["candidate_selection_training_rows_used"] is False
    kept = set(fold_report["outer_purge_kept_opportunity_ids"])
    assert crossing_opportunity not in kept
    assert fold_report["outer_purge_kept_opportunity_count"] == len(kept)
    for evaluation in fold_report["inner_candidate_selection_evaluations"]:
        evaluated = set(evaluation["evaluation_opportunity_ids"])
        assert evaluated <= kept
        assert crossing_opportunity not in evaluated
        assert evaluation["evaluation_opportunity_count"] == len(evaluated)
        assert evaluation["candidate_training_rows_used"] is False


def test_continuous_comparator_is_nonbaseline_and_side_specific(tmp_path: Path) -> None:
    panel, config, _ = _prepared_panel(tmp_path, sides=("BUY", "SELL"))
    buy = modeled.run_continuous_nested_oof(
        panel,
        config=config,
        side="BUY",
        feature_block_name="R0",
        scope="prefix40_modeled_label_development",
    )
    sell = modeled.run_continuous_nested_oof(
        panel,
        config=config,
        side="SELL",
        feature_block_name="R0",
        scope="prefix40_modeled_label_development",
    )
    assert set(buy.oof_rows["side"]) == {"BUY"}
    assert set(sell.oof_rows["side"]) == {"SELL"}
    assert (buy.oof_rows["selected_action"] != CONTROL_ACTION).all()
    assert (sell.oof_rows["selected_action"] != CONTROL_ACTION).all()
    assert buy.method == "continuous_multioutput_decision_tree"
    assert sell.method == "continuous_multioutput_decision_tree"
    for result in (buy, sell):
        assert result.deployment_gate["decision"] == "diagnostic_only"
        assert result.deployment_gate["passed_for_owner_repeated_policy_successor"] is False
        assert result.deployment_gate["action_authorized"] is False
        assert result.deployment_gate["live_authorized"] is False
    assert len(buy.purge_audits) == config.search.inner_folds + 1
    assert len(sell.purge_audits) == config.search.inner_folds + 1


def test_full_dispatch_uses_only_frozen_blocks_on_common_denominators(
    tmp_path: Path,
) -> None:
    panel, config, _ = _prepared_panel(tmp_path, sides=("BUY", "SELL"))
    config = replace(
        config,
        report_scopes=(
            "prefix40_modeled_label_development",
            "prefix33_raw_m2_common_support",
        ),
    )
    report, rows, policies, purges = modeled.run_all_comparisons(
        panel,
        config=config,
    )
    assert set(report["results"]["prefix40_modeled_label_development"]["BUY"]) == {
        "R0",
        "M0",
        "M1",
    }
    assert set(report["results"]["prefix33_raw_m2_common_support"]["SELL"]) == {
        "R0",
        "M0",
        "M1",
        "M2",
    }
    assert report["not_run_panels"]["added10"]["economic_oof_run"] is False
    assert report["not_run_panels"]["pooled50"]["modeled_labels_imputed"] is False
    assert set(policies) == set(config.report_scopes)
    assert set(rows["panel_scope"]) == set(config.report_scopes)
    assert purges


def test_spawn_multicore_dispatch_matches_serial_artifacts_and_progress(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    panel, config, _ = _prepared_panel(tmp_path, sides=("BUY", "SELL"))
    config = replace(
        config,
        report_scopes=(
            "prefix40_modeled_label_development",
            "prefix33_raw_m2_common_support",
        ),
    )
    assert len(modeled._comparison_cells(config)) == 14

    serial_started = time.perf_counter()
    serial = modeled.run_all_comparisons(
        panel,
        config=config,
        workers=1,
    )
    serial_seconds = time.perf_counter() - serial_started
    parallel_started = time.perf_counter()
    parallel = modeled.run_all_comparisons(
        panel,
        config=config,
        workers=2,
        emit_progress=True,
    )
    parallel_seconds = time.perf_counter() - parallel_started

    serial_report, serial_rows, serial_policies, serial_purges = serial
    parallel_report, parallel_rows, parallel_policies, parallel_purges = parallel
    assert modeled._canonical_json(serial_report) == modeled._canonical_json(parallel_report)
    assert modeled._canonical_json(serial_policies) == modeled._canonical_json(
        parallel_policies
    )
    assert modeled._canonical_json({"rows": serial_purges}) == modeled._canonical_json(
        {"rows": parallel_purges}
    )
    pd.testing.assert_frame_equal(serial_rows, parallel_rows, check_exact=True)

    expected_order = [
        (cell.panel_scope, cell.side, cell.feature_block, method)
        for cell in modeled._comparison_cells(config)
        for method in (
            "bounded_sparse_boolean_dnf",
            "continuous_multioutput_decision_tree",
        )
    ]
    observed_order = list(
        serial_rows[
            ["panel_scope", "side", "feature_block", "method"]
        ].drop_duplicates().itertuples(index=False, name=None)
    )
    assert observed_order == expected_order

    bindings = {"binding_sha256": "b" * 64}
    serial_manifest = modeled.publish_atomic_output(
        tmp_path / "serial-output",
        config=config,
        bindings=bindings,
        report=serial_report,
        oof_rows=serial_rows,
        policies=serial_policies,
        purge_audits=serial_purges,
    )
    parallel_manifest = modeled.publish_atomic_output(
        tmp_path / "parallel-output",
        config=config,
        bindings=bindings,
        report=parallel_report,
        oof_rows=parallel_rows,
        policies=parallel_policies,
        purge_audits=parallel_purges,
    )
    assert serial_manifest["manifest_sha256"] == parallel_manifest["manifest_sha256"]
    assert serial_manifest["files"] == parallel_manifest["files"]

    progress = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.strip()
    ]
    assert all(
        {"completed", "total", "cell"} <= set(event) and event["total"] == 14
        for event in progress
    )
    fallback = [event for event in progress if event["status"] == "parallel_fallback"]
    submitted = [event for event in progress if event["status"] == "cell_submitted"]
    started = [event for event in progress if event["status"] == "cell_started"]
    completed = [event for event in progress if event["status"] == "cell_complete"]
    expected_cells = {cell.identity for cell in modeled._comparison_cells(config)}
    if fallback:
        assert len(progress) == 29
        assert len(fallback) == 1
        assert fallback[0]["cell"] == "all"
        assert fallback[0]["parallel_fallback"] is True
        assert not submitted
        assert {event["cell"] for event in started} == expected_cells
        assert all(event["parallel_fallback"] is True for event in completed)
        execution = "deterministic serial fallback"
    else:
        assert len(progress) == 28
        assert not started
        assert {event["cell"] for event in submitted} == expected_cells
        assert all(event["parallel_fallback"] is False for event in completed)
        execution = "spawn"
    assert {event["cell"] for event in completed} == expected_cells
    assert [event["completed"] for event in completed] == list(range(1, 15))
    with capsys.disabled():
        print(
            "modeled-oof synthetic 14-cell benchmark: "
            f"workers=1 {serial_seconds:.3f}s; requested workers=2 "
            f"{parallel_seconds:.3f}s ({execution})"
        )


def test_dispatch_rejects_bad_workers_and_fallback_does_not_hide_business_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    panel, config, _ = _prepared_panel(tmp_path, sides=("BUY", "SELL"))
    with pytest.raises(modeled.ModeledOofError, match="workers must be a positive integer"):
        modeled.run_all_comparisons(panel, config=config, workers=0)

    serial = modeled.run_all_comparisons(panel, config=config, workers=1)

    class UnsupportedProcessPool:
        def __init__(self, *args, **kwargs) -> None:
            raise PermissionError(1, "sandbox denied semaphore query")

    monkeypatch.setattr(
        modeled.concurrent.futures,
        "ProcessPoolExecutor",
        UnsupportedProcessPool,
    )
    fallback = modeled.run_all_comparisons(
        panel,
        config=config,
        workers=2,
        emit_progress=True,
    )
    assert modeled._canonical_json(serial[0]) == modeled._canonical_json(fallback[0])
    pd.testing.assert_frame_equal(serial[1], fallback[1], check_exact=True)
    assert modeled._canonical_json(serial[2]) == modeled._canonical_json(fallback[2])
    assert modeled._canonical_json({"rows": serial[3]}) == modeled._canonical_json(
        {"rows": fallback[3]}
    )
    fallback_progress = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.strip()
    ]
    assert fallback_progress[0]["status"] == "parallel_fallback"
    assert fallback_progress[0]["fallback_reason"]["exception_type"] == "PermissionError"
    assert all(event["parallel_fallback"] is True for event in fallback_progress)

    class BusinessFailureProcessPool:
        def __init__(self, *args, **kwargs) -> None:
            self.futures: list[modeled.concurrent.futures.Future] = []

        def submit(self, *args, **kwargs):
            future = modeled.concurrent.futures.Future()
            future.set_exception(modeled.ModeledOofError("worker business failure"))
            self.futures.append(future)
            return future

        def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
            return None

    monkeypatch.setattr(
        modeled.concurrent.futures,
        "ProcessPoolExecutor",
        BusinessFailureProcessPool,
    )
    with pytest.raises(modeled.ModeledOofError, match="worker business failure"):
        modeled.run_all_comparisons(panel, config=config, workers=2)


def test_atomic_output_binds_spec_and_refuses_overwrite(tmp_path: Path) -> None:
    labels, features = _label_and_feature_rows(_days())
    config = _manual_config(
        tmp_path,
        days=_days(),
        opportunities=features["opportunity_id"].nunique(),
        censored_opportunities=0,
    )
    destination = tmp_path / "atomic-result"
    bindings = {"binding_sha256": "b" * 64}
    manifest = modeled.publish_atomic_output(
        destination,
        config=config,
        bindings=bindings,
        report={"status": "test"},
        oof_rows=labels.head(1),
        policies={"BUY": {}},
        purge_audits=[],
    )
    assert (destination / "frozen_owner_spec.json").read_bytes() == config.spec_path.read_bytes()
    assert (destination / "frozen_config.json").read_bytes() == config.path.read_bytes()
    assert (destination / "_SUCCESS").read_text(encoding="ascii").strip() == manifest[
        "manifest_sha256"
    ]
    assert manifest["owner_spec_sha256"] == config.spec_sha256
    with pytest.raises(modeled.ModeledOofError, match="refusing to replace"):
        modeled.publish_atomic_output(
            destination,
            config=config,
            bindings=bindings,
            report={"status": "test"},
            oof_rows=labels.head(1),
            policies={},
            purge_audits=[],
        )
