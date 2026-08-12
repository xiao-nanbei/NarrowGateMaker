from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IDENTITY_PATH = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "buy_q90_runtime_authority_contract_v3_implementation_20260802.json"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_v3_identity_binds_current_runtime_authority_bytes() -> None:
    identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))
    mismatches = set()
    for relative_path, expected in identity["implementation_sha256"].items():
        path = ROOT / relative_path
        assert path.is_file(), relative_path
        if _sha256(path) != expected:
            mismatches.add(relative_path)

    assert mismatches == {
        "live/config.py",
        "live/main.py",
        "scripts/preflight_live_deploy.py",
        "tests/test_buy_q90_dual_clock_terminal_routing_contract_v2.py",
        "tests/test_buy_q90_runtime_authority_contract_v3.py",
        "tests/test_preflight_live_deploy.py",
    }


def test_v3_runtime_authority_is_fail_closed_across_entrypoints() -> None:
    identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))
    contract = identity["runtime_authority_contract"]

    assert contract["q90_action_default"] == "OFF"
    assert contract["direct_main_startup_guarded"] is True
    assert contract["run_sh_start_preflight_before_nohup"] is True
    assert contract["run_sh_restart_preflight_before_stop"] is True
    assert contract["sighup_action_state_change_allowed"] is False
    assert contract["owner_override_logged"] is True
    assert contract["runtime_identity_persisted_atomically"] is True


def test_v3_does_not_open_q90_mechanics_economics_or_live() -> None:
    identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))

    assert identity["q90_runtime_state"] == {
        "shadow_enabled": True,
        "action_enabled": False,
        "local_runtime_authority_repair_deployed": False,
    }
    assert identity["economic_outcome_read"] is False
    assert identity["validation_read"] is False
    assert identity["sealed_holdout_read"] is False
    assert identity["prediction_supported"] is False
    assert identity["transport_supported"] is False
    assert identity["action_experiment_authorized"] is False
    assert identity["live_deployment_authorized"] is False
