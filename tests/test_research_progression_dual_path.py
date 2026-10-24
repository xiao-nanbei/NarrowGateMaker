from __future__ import annotations

import hashlib
from copy import deepcopy

import pytest

from models.audit.research_progression import (
    OWNER_PROMOTION,
    SCHEMA_VERSION,
    STANDARD_PROMOTION,
    progression_contract_sha256,
    validate_progression_contract,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _contract() -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": "dual_path_test_v1",
        "hard_gate_path": {
            "predecessor_spec_sha256": _sha("spec"),
            "predecessor_report_sha256": _sha("report"),
            "immutable": True,
            "passed": False,
            "failed_gates": ["minimum_support"],
        },
        "owner_progression_path": {
            "owner_requested": True,
            "risk_accepted": True,
            "outcome_informed": True,
            "accepted_risks": ["sparse support"],
            "rewrites_hard_gate": False,
        },
        "current_permissions": {
            "development_continuation_authorized": True,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "promotion_routes": {
            "hard_gate_path": STANDARD_PROMOTION,
            "owner_progression_path": OWNER_PROMOTION,
            "shared_downstream_requirements": [
                "positive_full_path_economic_evidence",
                "execution_and_shadow_parity",
                "tail_and_safety_gates",
                "promotion_controller_decision",
            ],
        },
    }


def test_both_paths_can_reach_distinct_promotion_routes() -> None:
    contract = _contract()
    validate_progression_contract(contract)
    assert contract["promotion_routes"]["hard_gate_path"] == STANDARD_PROMOTION
    assert contract["promotion_routes"]["owner_progression_path"] == OWNER_PROMOTION
    assert len(progression_contract_sha256(contract)) == 64


@pytest.mark.parametrize(
    ("path", "value", "match"),
    [
        (("hard_gate_path", "immutable"), False, "immutable"),
        (("owner_progression_path", "rewrites_hard_gate"), True, "rewrite"),
        (("current_permissions", "live_authorized"), True, "live_authorized"),
    ],
)
def test_registration_cannot_rewrite_gate_or_grant_live(
    path: tuple[str, str],
    value: object,
    match: str,
) -> None:
    contract = deepcopy(_contract())
    contract[path[0]][path[1]] = value  # type: ignore[index]
    with pytest.raises(ValueError, match=match):
        validate_progression_contract(contract)
