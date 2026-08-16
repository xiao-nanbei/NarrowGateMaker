from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from models.audit.dataset_governance import (
    SCHEMA_VERSION,
    canonical_full_path_days,
    load_dataset_binding,
    validate_dataset_binding,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _binding(tmp_path: Path, *, experiment_class: str) -> dict[str, object]:
    identity, canonical_days = canonical_full_path_days()
    universe_path = tmp_path / "universe.json"
    universe_path.write_text(json.dumps({"days": canonical_days}), encoding="utf-8")
    development = canonical_days[:30]
    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": "test_dataset_binding",
        "experiment_class": experiment_class,
        "universe_manifest": {
            "path": str(universe_path),
            "sha256": _sha256(universe_path),
        },
        "required_capabilities": ["individual_trades", "native_l2"],
        "eligible_days": canonical_days,
        "evidence": {
            "panels": {
                "development": {"days": development, "status": "open"},
                "embargo_1": {
                    "days": canonical_days[30:31],
                    "status": "locked",
                },
                "validation": {
                    "days": canonical_days[31:36],
                    "status": "locked",
                },
                "embargo_2": {
                    "days": canonical_days[36:37],
                    "status": "locked",
                },
                "sealed_holdout": {
                    "days": canonical_days[37:42],
                    "status": "sealed",
                },
            }
        },
        "training_window": {
            "mode": "expanding_all_eligible_pre_cutoff",
            "cutoff_basis": "evidence_panel_boundary",
            "source_authorities": ["native_exchange"],
            "source_pooling": "single_authority",
        },
        "oof": {
            "enabled": True,
            "scope": "development_only",
            "test_day_count": 10,
            "folds": [
                {
                    "train_days": development[:10],
                    "test_days": development[10:15],
                },
                {
                    "train_days": development[:20],
                    "test_days": development[20:25],
                },
            ],
        },
        "execution_denominator": {
            "identity": identity,
            "days": canonical_days,
            "reduced_support": False,
            "claims_current_50_day_baseline": True,
            "report_prefix40_added10_pooled50": True,
        },
    }


def test_canonical_50_day_full_path_binding_passes(tmp_path: Path) -> None:
    payload = _binding(tmp_path, experiment_class="daily_fresh_start_full_path_action")
    result = validate_dataset_binding(payload)
    assert result["valid"] is True
    assert result["oof_test_day_count"] == 10


def test_silent_40_day_full_path_fallback_fails(tmp_path: Path) -> None:
    payload = _binding(tmp_path, experiment_class="daily_fresh_start_full_path_action")
    payload["execution_denominator"]["days"] = payload["execution_denominator"][
        "days"
    ][:40]
    with pytest.raises(ValueError, match="canonical 50-day panel"):
        validate_dataset_binding(payload)


def test_explicit_reduced_support_is_not_mislabeled_50_day(tmp_path: Path) -> None:
    payload = _binding(tmp_path, experiment_class="strict_native_queue_action")
    removed = payload["execution_denominator"]["days"][-1]
    payload["execution_denominator"].update(
        {
            "days": payload["execution_denominator"]["days"][:-1],
            "reduced_support": True,
            "claims_current_50_day_baseline": False,
            "excluded_days": [
                {"day": removed, "reasons": ["missing_previous_natural_day_warmup"]}
            ],
        }
    )
    result = validate_dataset_binding(payload)
    assert result["valid"] is True


def test_mixed_sources_cannot_claim_single_authority(tmp_path: Path) -> None:
    payload = _binding(tmp_path, experiment_class="prediction")
    payload["training_window"]["source_authorities"] = [
        "provider_normalized",
        "native_exchange",
    ]
    with pytest.raises(ValueError, match="multiple source authorities"):
        validate_dataset_binding(payload)


def test_oof_cannot_read_validation(tmp_path: Path) -> None:
    payload = _binding(tmp_path, experiment_class="prediction")
    validation_day = payload["evidence"]["panels"]["validation"]["days"][0]
    payload["oof"]["folds"][0]["test_days"] = [validation_day]
    payload["oof"]["test_day_count"] = 6
    with pytest.raises(ValueError, match="Development days only"):
        validate_dataset_binding(payload)


def test_universe_manifest_days_are_authoritative(tmp_path: Path) -> None:
    payload = _binding(tmp_path, experiment_class="prediction")
    payload["eligible_days"] = [*payload["eligible_days"], "2027-01-01"]
    with pytest.raises(ValueError, match="absent from the universe manifest"):
        validate_dataset_binding(payload)


def test_hash_bound_loader_rejects_binding_drift(tmp_path: Path) -> None:
    payload = _binding(tmp_path, experiment_class="prediction")
    binding_path = tmp_path / "binding.json"
    binding_path.write_text(json.dumps(payload), encoding="utf-8")
    expected = _sha256(binding_path)
    loaded, result = load_dataset_binding(
        binding_path,
        expected_file_sha256=expected,
        expected_experiment_id="test_dataset_binding",
    )
    assert loaded["schema_version"] == SCHEMA_VERSION
    assert result["valid"] is True
    binding_path.write_text(json.dumps({**payload, "experiment_id": "drifted"}), encoding="utf-8")
    with pytest.raises(ValueError, match="file hash"):
        load_dataset_binding(binding_path, expected_file_sha256=expected)


def test_chronological_policy_learning_binds_outer_test_execution(
    tmp_path: Path,
) -> None:
    payload = _binding(tmp_path, experiment_class="chronological_policy_learning")
    test_days = sorted(
        day for fold in payload["oof"]["folds"] for day in fold["test_days"]
    )
    payload["binding_timing"] = {
        "created_before_economic_execution": True,
        "post_execution_remediation": False,
    }
    payload["permissions"] = {
        "action_authorized": False,
        "live_authorized": False,
    }
    payload["execution_denominator"] = {
        "role": "outer_oof_learning_algorithm",
        "days": test_days,
        "claims_current_50_day_baseline": False,
        "future_canonical_50_day_confirmation_required": True,
        "one_shot_effect_aggregation_used": False,
    }
    assert validate_dataset_binding(payload)["valid"] is True
    payload["execution_denominator"]["days"] = test_days[:-1]
    with pytest.raises(ValueError, match="outer OOF test days"):
        validate_dataset_binding(payload)
