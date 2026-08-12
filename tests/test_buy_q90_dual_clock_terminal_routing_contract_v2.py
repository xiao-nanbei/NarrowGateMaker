from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IDENTITY_PATH = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "buy_q90_dual_clock_terminal_routing_contract_v2_implementation_20260802.json"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_v2_identity_is_preserved_as_a_historical_implementation() -> None:
    assert _sha256(IDENTITY_PATH) == (
        "37b3feb98f11677a68d39773dc242d77798ee8a257df4b8976dadb7d3d256b35"
    )
    identity = json.loads(IDENTITY_PATH.read_text())
    mismatches = set()
    for relative_path, expected in identity["implementation_sha256"].items():
        path = ROOT / relative_path
        assert path.is_file(), relative_path
        if _sha256(path) != expected:
            mismatches.add(relative_path)

    assert mismatches == {
        "cpp/narrowgate_cpp/bindings.cpp",
        "cpp/narrowgate_cpp/dynamic_fill_hazard.cpp",
        "cpp/narrowgate_cpp/dynamic_fill_hazard.hpp",
        "execution/order_lifecycle.py",
        "features/feature_dag.py",
        "live/config.py",
        "live/config.yaml",
        "models/backtest_tick.py",
        "strategy/dynamic_fill_hazard_model.py",
        "strategy/maker_engine.py",
        "strategy/order_manager.py",
        "tests/test_buy_q90_dual_clock_terminal_routing_contract_v2.py",
        "tests/test_dynamic_fill_hazard_cpp_parity.py",
        "tests/test_dynamic_fill_hazard_live_action.py",
        "tests/test_execution_order_lifecycle.py",
    }


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
