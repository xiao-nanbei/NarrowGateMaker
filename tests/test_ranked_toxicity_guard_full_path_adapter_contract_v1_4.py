from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_full_path_adapter_contract_v1_4 import (
    canonical_spec_sha256,
    validate_execution_amendment_v1_4,
)

ROOT = Path(__file__).resolve().parents[1]
AMENDMENT = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4_"
    "execution_amendment_20260802.json"
)


def _write_mutation(tmp_path: Path, mutate) -> Path:
    payload = json.loads(AMENDMENT.read_text(encoding="utf-8"))
    mutate(payload)
    for section in ("implementation_identity", "documentation_identity"):
        for identity in (payload.get(section) or {}).values():
            source = Path(str(identity["path"]))
            if not source.is_absolute():
                source = ROOT / source
            identity["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    payload["canonical_spec_identity_sha256"] = canonical_spec_sha256(
        payload,
        identity_field="canonical_spec_identity_sha256",
    )
    path = tmp_path / "mutated_v1_4.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def test_v1_4_is_preserved_but_rejects_current_replay_bytes() -> None:
    with pytest.raises(ValueError, match="authoritative_tick_replay SHA256 mismatch"):
        validate_execution_amendment_v1_4(AMENDMENT)


def test_v1_4_rejects_action_contract_drift(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["unchanged_action_contract_projection"][
            "BUY"
        ].__setitem__("quantile", 0.8),
    )

    with pytest.raises(ValueError, match="action, threshold, or behavior policy"):
        validate_execution_amendment_v1_4(path)


def test_v1_4_rejects_execution_invariant_drift(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["required_execution_invariants"].__setitem__(
            "candidate_campaign_terminal_never_rerandomizes", False
        ),
    )

    with pytest.raises(ValueError, match="required execution invariants"):
        validate_execution_amendment_v1_4(path)


def test_v1_4_cannot_grant_mechanics_results_or_live_permission(
    tmp_path: Path,
) -> None:
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["permissions"].__setitem__(
            "mechanics_read", True
        ),
    )

    with pytest.raises(ValueError, match="cannot grant mechanics_read"):
        validate_execution_amendment_v1_4(path)
