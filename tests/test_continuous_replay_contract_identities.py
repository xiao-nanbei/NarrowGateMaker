import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CURRENT_CONTRACTS = (
    "calendar_continuity_manifest_v1_contract.json",
    "restart_boundary_contract_v1.json",
    "continuous_replay_state_v2_contract.json",
    "continuous_accounting_contract_v2.json",
)
SUPERSEDED_CONTRACTS = (
    "continuous_replay_state_v1_contract.json",
    "continuous_accounting_contract_v1.json",
)


def test_continuous_replay_contracts_bind_current_implementation_bytes() -> None:
    contract_root = ROOT / "research/shared/replay_lifecycle/docs"
    for name in CURRENT_CONTRACTS:
        payload = json.loads((contract_root / name).read_text(encoding="utf-8"))
        implementation = payload["implementation"]
        implementation_path = ROOT / implementation["path"]
        actual = hashlib.sha256(implementation_path.read_bytes()).hexdigest()
        assert actual == implementation["sha256"], name
        assert payload["authority"]["action_or_live_authority"] is False


def test_v2_contracts_preserve_v1_as_immutable_superseded_predecessors() -> None:
    contract_root = ROOT / "research/shared/replay_lifecycle/docs"
    current_by_predecessor = {}
    for name in CURRENT_CONTRACTS:
        payload = json.loads((contract_root / name).read_text(encoding="utf-8"))
        predecessor = payload.get("predecessor")
        if predecessor:
            current_by_predecessor[predecessor["contract_id"]] = predecessor

    for name in SUPERSEDED_CONTRACTS:
        predecessor = json.loads((contract_root / name).read_text(encoding="utf-8"))
        contract_id = predecessor["contract_id"]
        assert current_by_predecessor[contract_id]["status"] == (
            "immutable_superseded_semantics"
        )
        assert predecessor["authority"]["action_or_live_authority"] is False
