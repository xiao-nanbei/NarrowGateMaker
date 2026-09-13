from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.families.f09_campaign_action_uplift.audit import (
    ranked_toxicity_guard_carryover_safe_execution_v2_2 as successor,
)


ROOT = Path(__file__).resolve().parents[1]
SPEC = (
    ROOT
    / "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2_2_execution_spec_20260803.json"
)


def _audit() -> dict:
    side = {
        "zero_tolerance_counts": {
            "control_candidate_baseline_shadow_mismatch": 0,
        },
        "cross_arm_order_ownership_count": 0,
        "forced_washout_cancel_count": 0,
        "order_owner_mismatch_count": 0,
        "active_order_role_transition_to_exposure_count": 1,
        "carryover_transition_count": 2,
        "zero_tolerance_passed": True,
        "execution_complete": True,
        "carryover_contract_valid": True,
    }
    return {
        "baseline_shadow": {
            "rows": 10,
            "consumed": 10,
            "unconsumed": 0,
            "complete": True,
        },
        "adapters": {"BUY": dict(side), "SELL": dict(side)},
    }


def test_v2_2_spec_is_frozen_without_old_episode_count_gate() -> None:
    with pytest.raises(ValueError, match="authoritative_tick_replay SHA256 mismatch"):
        successor.load_spec(SPEC)
    spec = json.loads(SPEC.read_text())
    predecessor = successor.v2_1._load_predecessor(spec)
    assert predecessor["family_id"].endswith("carryover_safe_v2")
    assert "expected_v2_counts" not in spec["smoke_contract"]
    assert spec["plumbing_threshold"] == {
        "value": 0.8,
        "source_sha256": "b" * 64,
        "authority": "plumbing_only",
    }


def test_contract_counter_projection_is_narrow() -> None:
    counters = successor.extract_contract_counters(_audit())
    assert set(counters) == {"baseline", "BUY", "SELL"}
    assert counters["BUY"]["active_order_role_transition_to_exposure_count"] == 1
    assert counters["SELL"]["carryover_transition_count"] == 2
    assert "assignment_count" not in counters["BUY"]


def test_all_frozen_plumbing_gates_pass() -> None:
    gates = successor.evaluate_contract_gates(
        successor.extract_contract_counters(_audit())
    )
    assert gates
    assert all(gates.values())


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("baseline", "complete"),
        ("BUY", "baseline_shadow_mismatch_count"),
        ("BUY", "cross_arm_order_ownership_count"),
        ("SELL", "forced_washout_cancel_count"),
        ("SELL", "order_owner_mismatch_count"),
        ("BUY", "role_lifecycle_valid"),
        ("SELL", "carryover_lifecycle_valid"),
    ],
)
def test_each_plumbing_gate_fails_closed(section: str, field: str) -> None:
    counters = successor.extract_contract_counters(_audit())
    current = counters[section][field]
    counters[section][field] = not current if isinstance(current, bool) else 1
    gates = successor.evaluate_contract_gates(counters)
    assert not all(gates.values())


def test_spec_permissions_do_not_authorize_results() -> None:
    spec = json.loads(SPEC.read_text())
    permissions = spec["permissions"]
    assert permissions["one_day_plumbing_smoke_allowed"] is True
    for key, value in permissions.items():
        if key != "one_day_plumbing_smoke_allowed":
            assert value is False
