from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IDENTITY_PATH = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "buy_q90_dual_clock_terminal_routing_contract_v2_implementation_20260802.json"
)

def test_v2_parity_scope_does_not_claim_cpp_exposure_authority() -> None:
    identity = json.loads(IDENTITY_PATH.read_text())
    journal = identity["lifecycle_journal_contract"]
    scope = identity["parity_scope"]

    assert journal["schema_version"] == "order_lifecycle_journal.v1"
    assert journal["live_persisted"] is True
    assert journal["python_replay_emitted"] is True
    assert journal["cpp_exposure_authority"] is False
    assert journal["three_runtime_exposure_parity_claimed"] is False
    assert scope["python_live_replay_dual_exposure_journal"] is True
    assert scope["python_cpp_terminal_route_path_score_cancel_recovery"] is True
    assert scope["python_cpp_quantity_time_exposure"] is False


def test_v2_keeps_mechanics_and_economic_permissions_closed() -> None:
    identity = json.loads(IDENTITY_PATH.read_text())
    verification = identity["verification"]

    assert verification["forty_day_lockstep_rerun"] is False
    assert verification["same_date_aws_transport_rerun"] is False
    assert identity["economic_outcome_read"] is False
    assert identity["validation_read"] is False
    assert identity["sealed_holdout_read"] is False
    assert identity["prediction_supported"] is False
    assert identity["transport_supported"] is False
    assert identity["action_experiment_authorized"] is False
    assert identity["live_deployment_authorized"] is False
