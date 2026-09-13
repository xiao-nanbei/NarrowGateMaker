from __future__ import annotations

import json
from pathlib import Path

from strategy.dynamic_fill_hazard_model import (
    CPP_DYNAMIC_FILL_HAZARD_ABI_VERSION,
)

ROOT = Path(__file__).resolve().parents[1]
IDENTITY_PATH = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "buy_q90_fresh_prospective_placement_recovery_v4_"
    "implementation_20260802.json"
)

def _identity() -> dict[str, object]:
    return json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))


def test_v4_contract_is_fresh_cancel_ack_only_and_fail_closed() -> None:
    identity = _identity()
    contract = identity["fresh_prospective_placement_contract"]

    assert CPP_DYNAMIC_FILL_HAZARD_ABI_VERSION == (
        "dynamic_fill_hazard_native_book_q90.v4"
    )
    assert contract["accepted_terminal_route"] == "PROSPECTIVE_CANCEL_REENTRY"
    assert contract["accepted_terminal_reasons"] == [
        "cancel_ack",
        "cancel_ack_reconciled",
    ]
    assert contract["remaining_quantity_rule"] == "strictly_positive"
    assert contract["candidate_price_source"] == "current_quote_decision"
    assert contract["order_age_ms"] == 0
    assert contract["queue_seed"] == "current_candidate_level_queue_at_tail"
    assert contract["old_cursor_or_path_read_allowed"] is False
    assert contract["unknown_terminal_reason"] == "fail_fast"


def test_v4_preserves_shadow_only_authority_boundary() -> None:
    identity = _identity()

    assert identity["q90_runtime_state"] == {
        "shadow_enabled": True,
        "action_enabled": False,
        "fresh_recovery_deployed": False,
    }
    assert identity["economic_outcome_read"] is False
    assert identity["validation_read"] is False
    assert identity["sealed_holdout_read"] is False
    assert identity["prediction_supported"] is False
    assert identity["transport_supported"] is False
    assert identity["action_experiment_authorized"] is False
    assert identity["live_deployment_authorized"] is False
