from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = (
    ROOT
    / "research/shared/replay_lifecycle/docs/"
    "order_lifecycle_journal_v2_storage_profiles_v2_implementation_20260805.json"
)


def test_storage_profile_implementation_contract_binds_files_and_permissions() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    assert payload["status"] == "implemented_local_not_deployed"
    assert payload["default"] == {
        "enabled": False,
        "storage_profile": "local_orico_replay_admission",
    }
    authority = payload["authority"]
    assert authority["seven_tape_heartbeat_contract_modified"] is False
    assert authority["ec2_deployed"] is False
    assert authority["ec2_restarted"] is False
    assert authority["transfer_executed"] is False
    assert authority["formal_collection_authorized_from_remote_spool_alone"] is False
    assert authority["economic_outcomes_read"] is False
    assert authority["action_authorized"] is False
    assert authority["live_policy_authorized"] is False
    assert set(payload["implementation_files"]) == set(payload["implementation_sha256"])
    for relative, expected in payload["implementation_sha256"].items():
        actual = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
        assert actual == expected, relative
