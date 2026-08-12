import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = (
    "calendar_continuity_manifest_v1_contract.json",
    "restart_boundary_contract_v1.json",
    "continuous_replay_state_v1_contract.json",
    "continuous_accounting_contract_v1.json",
)


def test_continuous_replay_contracts_bind_current_implementation_bytes() -> None:
    contract_root = ROOT / "research/shared/replay_lifecycle/docs"
    for name in CONTRACTS:
        payload = json.loads((contract_root / name).read_text(encoding="utf-8"))
        implementation = payload["implementation"]
        implementation_path = ROOT / implementation["path"]
        actual = hashlib.sha256(implementation_path.read_bytes()).hexdigest()
        assert actual == implementation["sha256"], name
        assert payload["authority"]["action_or_live_authority"] is False
