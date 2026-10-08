from __future__ import annotations

import json
from pathlib import Path

import pytest

from models.audit.evidence_split import (
    DEFAULT_ACTION_PROBABILITIES,
    build_explicit_evidence_split,
    build_from_feature_manifest,
    build_panel_access_decision,
    load_evidence_panel,
    sha256_file,
    validate_evidence_split,
)


def _source_manifest(tmp_path: Path) -> Path:
    path = tmp_path / "features.json"
    path.write_text(
        json.dumps(
            {
                "daily_manifest_sha256": "daily-hash",
                "split": {
                    "train": ["2026-01-01", "2026-01-02"],
                    "embargo_1": ["2026-01-03"],
                    "validation": ["2026-01-04"],
                    "embargo_2": ["2026-01-05"],
                    "test": ["2026-01-06"],
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _frozen_manifest(tmp_path: Path) -> tuple[Path, dict]:
    payload = build_from_feature_manifest(
        _source_manifest(tmp_path),
        family_id="side-specific-local-v1",
        behavior_probabilities=DEFAULT_ACTION_PROBABILITIES,
    )
    path = tmp_path / "split.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def _access_decision(
    tmp_path: Path,
    split_path: Path,
    *,
    target_panel: str,
) -> tuple[Path, Path]:
    bundle = tmp_path / f"{target_panel}.bundle.json"
    bundle.write_text('{"bundle": true}', encoding="utf-8")
    prior_panel = (
        "development" if target_panel == "validation" else "validation"
    )
    metadata = tmp_path / f"{target_panel}.metadata.json"
    metadata.write_text(
        json.dumps(
            {
                "panel_role": prior_panel,
                "evidence_split": {
                    "manifest_sha256": sha256_file(split_path),
                },
                "ope_block_reason": "",
                "native_source_integrity": {"passed": True},
                "native_action_support": {
                    "rows": 10,
                    "outcome_supported_rows": 10,
                    "ambiguous_rows": 0,
                    "invalid_path_rows": 0,
                },
                "queue_model_bundle_sha256": sha256_file(bundle),
            }
        ),
        encoding="utf-8",
    )
    summary = tmp_path / f"{target_panel}.ope_summary.json"
    summary.write_text(
        json.dumps(
            {
                "numerical_ope_gate_passed": True,
                "day_cluster_bootstrap": {"uplift_p025": 0.01},
            }
        ),
        encoding="utf-8",
    )
    payload = build_panel_access_decision(
        evidence_split_path=split_path,
        target_panel=target_panel,
        prior_metadata_path=metadata,
        prior_ope_summary_paths=[summary],
        queue_model_bundle_path=bundle,
    )
    decision = tmp_path / f"{target_panel}.access.json"
    decision.write_text(json.dumps(payload), encoding="utf-8")
    return decision, bundle


def test_existing_good_days_are_frozen_into_chronological_panels(
    tmp_path: Path,
) -> None:
    path, payload = _frozen_manifest(tmp_path)

    normalized = validate_evidence_split(payload)
    days, identity = load_evidence_panel(path, "development")

    assert normalized["development"] == ["2026-01-01", "2026-01-02"]
    assert days == ["2026-01-01", "2026-01-02"]
    assert identity["family_id"] == "side-specific-local-v1"
    assert identity["sealed_access"] is False


def test_sealed_holdout_requires_explicit_one_shot_access(tmp_path: Path) -> None:
    path, _ = _frozen_manifest(tmp_path)
    decision, bundle = _access_decision(
        tmp_path,
        path,
        target_panel="sealed_holdout",
    )

    with pytest.raises(PermissionError, match="hash-bound"):
        load_evidence_panel(path, "sealed_holdout")

    days, identity = load_evidence_panel(
        path,
        "sealed_holdout",
        allow_sealed_holdout=True,
        access_decision_path=decision,
        queue_model_bundle_path=bundle,
    )
    assert days == ["2026-01-06"]
    assert identity["sealed_access"] is True


def test_validation_requires_positive_hash_bound_prior_gate(
    tmp_path: Path,
) -> None:
    path, _ = _frozen_manifest(tmp_path)
    with pytest.raises(PermissionError, match="hash-bound"):
        load_evidence_panel(path, "validation")

    decision, bundle = _access_decision(
        tmp_path,
        path,
        target_panel="validation",
    )
    days, identity = load_evidence_panel(
        path,
        "validation",
        access_decision_path=decision,
        queue_model_bundle_path=bundle,
    )
    assert days == ["2026-01-04"]
    assert identity["access_decision_sha256"] == sha256_file(decision)

    bundle.write_text('{"bundle": "changed"}', encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_evidence_panel(
            path,
            "validation",
            access_decision_path=decision,
            queue_model_bundle_path=bundle,
        )


def test_prediction_admission_can_open_validation_only(tmp_path: Path) -> None:
    split_path, split = _frozen_manifest(tmp_path)
    family_id = split["family_id"]
    summary = tmp_path / "development.summary.json"
    summary.write_text(
        json.dumps(
            {
                "family_id": family_id,
                "sealed_holdout_access_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    bundle = tmp_path / "development.bundle.json"
    bundle.write_text(
        json.dumps(
            {
                "family_id": family_id,
                "action_family_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    spec = tmp_path / "family_spec.json"
    spec.write_text(
        json.dumps(
            {
                "family_id": family_id,
                "live_change_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    oof = tmp_path / "development.oof"
    oof.write_text("frozen", encoding="utf-8")
    admission = tmp_path / "validation.admission.json"
    admission.write_text(
        json.dumps(
            {
                "schema_version": (
                    "dynamic_fill_hazard_validation_admission.v1"
                ),
                "family_id": family_id,
                "decision": "admit_buy_to_validation",
                "decision_scope": "prediction_validation_only",
                "prior_panel": "development",
                "target_panel": "validation",
                "validation_status": "admitted_not_yet_read",
                "gate_passed": True,
                "admitted_sides": ["BUY"],
                "live_change_allowed": False,
                "sealed_holdout_access_allowed": False,
                "sell_validation_access_allowed": False,
                "evidence_split_sha256": sha256_file(split_path),
                "development_summary_path": str(summary),
                "development_summary_sha256": sha256_file(summary),
                "model_bundle_path": str(bundle),
                "model_bundle_sha256": sha256_file(bundle),
                "family_spec_path": str(spec),
                "family_spec_sha256": sha256_file(spec),
                "development_oof_predictions_path": str(oof),
                "development_oof_predictions_sha256": sha256_file(oof),
                "admission_evidence": {
                    "favorable_fill_absolute_brier_improvement": 1e-6,
                    "favorable_fill_bootstrap_probability_positive": 0.77,
                    "adverse_fill_original_strict_gate_passed": True,
                    "repair_original_strict_gate_passed": True,
                },
                "admission_rule": {
                    "minimum_day_cluster_probability_improvement_positive": 0.75
                },
            }
        ),
        encoding="utf-8",
    )

    days, identity = load_evidence_panel(
        split_path,
        "validation",
        access_decision_path=admission,
    )

    assert days == ["2026-01-04"]
    assert identity["access_decision_sha256"] == sha256_file(admission)
    with pytest.raises(PermissionError, match="Validation only"):
        load_evidence_panel(
            split_path,
            "sealed_holdout",
            allow_sealed_holdout=True,
            access_decision_path=admission,
        )


def test_split_rejects_cross_panel_overlap(tmp_path: Path) -> None:
    _, payload = _frozen_manifest(tmp_path)
    payload["panels"]["validation"]["days"] = ["2026-01-02"]

    with pytest.raises(ValueError, match="overlap"):
        validate_evidence_split(payload)


def test_split_rejects_source_manifest_drift(tmp_path: Path) -> None:
    _, payload = _frozen_manifest(tmp_path)
    source = Path(payload["source_manifest_path"])
    source.write_text('{"changed": true}', encoding="utf-8")

    with pytest.raises(ValueError, match="hash has changed"):
        validate_evidence_split(payload)


def test_new_buy_family_can_reassign_existing_days_without_future_wait(
    tmp_path: Path,
) -> None:
    source = _source_manifest(tmp_path)
    payload = build_explicit_evidence_split(
        source,
        family_id="buy_add_conditional_widen_causal_v4_v1",
        panels={
            "development": ["2026-01-01", "2026-01-02"],
            "embargo_1": ["2026-01-03"],
            "validation": ["2026-01-04"],
            "embargo_2": ["2026-01-05"],
            "sealed_holdout": ["2026-01-06"],
        },
        behavior_probabilities={"baseline": 0.5, "widen_1tick": 0.5},
        sides=["BUY"],
    )

    assert validate_evidence_split(payload)["validation"] == ["2026-01-04"]
    assert payload["action_family"]["sides"] == ["BUY"]
    assert payload["action_family"]["actions"] == ["baseline", "widen_1tick"]
