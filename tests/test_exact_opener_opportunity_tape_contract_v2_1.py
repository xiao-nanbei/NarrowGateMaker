from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.families.f04_external_market_alpha.audit.exact_opener_opportunity_tape_contract_v2_1 import (
    canonical_spec_sha256,
    validate_execution_amendment_v2_1,
)

ROOT = Path(__file__).resolve().parents[1]
AMENDMENT = (
    ROOT
    / "research"
    / "families"
    / "f04_external_market_alpha"
    / "docs"
    / "external_adverse_quote_edge_guard_exact_opener_mechanics_v2_1_"
    "execution_amendment_20260802.json"
)


def _write_mutation(tmp_path: Path, mutate) -> Path:
    payload = json.loads(AMENDMENT.read_text(encoding="utf-8"))
    mutate(payload)
    payload["canonical_spec_identity_sha256"] = canonical_spec_sha256(payload)
    path = tmp_path / "mutated_v2_1.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def test_v2_1_binds_exact_schema_side_gates_and_lifecycle_disclosure() -> None:
    audit = validate_execution_amendment_v2_1(AMENDMENT)

    assert audit["frozen_v2_bytes_valid"]
    assert audit["exact_schema_allowlist_valid"]
    assert audit["exact_opener_denominator_contract_valid"]
    assert audit["side_specific_candidate_rate_gate_valid"]
    assert audit["lifecycle_identity_and_terminal_contract_valid"]
    assert audit["economic_outcomes_read"] is False
    assert audit["operational_lifecycle_outcomes_read"] is True
    assert audit["permissions"]["prospective_collection_eligible"] is True
    assert audit["permissions"]["prospective_tape_read"] is False
    assert audit["permissions"]["action_experiment_authorized"] is False


def test_v2_1_rejects_pooled_candidate_rate_gate(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["validator_contract"][
            "candidate_rate_gate"
        ].__setitem__("scope", "pooled_opener"),
    )

    with pytest.raises(ValueError, match="validator contract drifted"):
        validate_execution_amendment_v2_1(path)


def test_v2_1_rejects_hidden_extra_tape_column(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["validator_contract"][
            "allowed_internal_columns"
        ].append("opaque_label"),
    )

    with pytest.raises(ValueError, match="validator contract drifted"):
        validate_execution_amendment_v2_1(path)


def test_v2_1_must_disclose_operational_lifecycle_outcomes(
    tmp_path: Path,
) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["outcome_boundary"].__setitem__(
            "operational_lifecycle_outcomes_read", False
        ),
    )

    with pytest.raises(ValueError, match="must disclose lifecycle outcomes"):
        validate_execution_amendment_v2_1(path)


def test_v2_1_cannot_grant_action_or_live_authority(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["permissions"].__setitem__(
            "action_experiment_authorized", True
        ),
    )

    with pytest.raises(ValueError, match="cannot grant"):
        validate_execution_amendment_v2_1(path)
