from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_full_path_adapter_contract_v1_3 import (
    canonical_spec_sha256,
    validate_execution_amendment_v1_3,
)

ROOT = Path(__file__).resolve().parents[1]
AMENDMENT = (
    ROOT
    / "research"
    / "families"
    / "f09_campaign_action_uplift"
    / "docs"
    / "causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_3_"
    "execution_amendment_20260802.json"
)


def _write_mutation(tmp_path: Path, mutate) -> Path:
    payload = json.loads(AMENDMENT.read_text(encoding="utf-8"))
    mutate(payload)
    payload["canonical_spec_identity_sha256"] = canonical_spec_sha256(
        payload,
        identity_field="canonical_spec_identity_sha256",
    )
    path = tmp_path / "mutated_v1_3.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def test_v1_3_preserves_action_and_binds_continuous_path_scorecard() -> None:
    audit = validate_execution_amendment_v1_3(AMENDMENT)

    assert audit["historical_v1_2_bytes_valid"]
    assert audit["frozen_v1_action_contract_valid"]
    assert audit["historical_scorecard_profiles_preserved"]
    assert audit["continuous_path_outcome_contract_valid"]
    assert audit["continuous_path_profile_contract"] == {
        "schema_version": "narrowgate_score_profile.v2",
        "profile_id": "action_execution_selective_v3",
        "profile_sha256": (
            "1024e254b9e2b301e7621d2b1fa1986f210cdf5072da1e7cdc1e722f1efef13a"
        ),
    }
    assert audit["economic_outcome_columns_read"] == []
    assert not audit["permissions"]["mechanics_read"]
    assert not audit["permissions"]["action_experiment_authorized"]
    assert not audit["permissions"]["live_deployment_authorized"]


def test_v1_3_rejects_action_contract_drift(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["unchanged_action_contract_projection"]["BUY"].__setitem__(
            "quantile", 0.8
        ),
    )

    with pytest.raises(ValueError, match="action, threshold, assignment, or baseline"):
        validate_execution_amendment_v1_3(path)


def test_v1_3_rejects_scorecard_contract_drift(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["scorecard_successor"]["profile"].__setitem__(
            "profile_sha256", "0" * 64
        ),
    )

    with pytest.raises(ValueError, match="profile contract drifted"):
        validate_execution_amendment_v1_3(path)


def test_v1_3_cannot_grant_economic_or_live_permission(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["permissions"].__setitem__(
            "development_economic_outcome_read", True
        ),
    )

    with pytest.raises(ValueError, match="cannot grant"):
        validate_execution_amendment_v1_3(path)
