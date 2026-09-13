from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.families.f09_campaign_action_uplift.audit import (
    ranked_toxicity_guard_full_path_adapter_contract_v1_4 as contract,
)
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
    # Preserve historical identities; semantic mutations do not rebind evidence.
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
        lambda payload: payload["unchanged_action_contract_projection"]["BUY"].__setitem__(
            "quantile", 0.8
        ),
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


@pytest.mark.parametrize(
    "permission",
    [
        "mechanics_read",
        "development_economic_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "prediction_authority",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ],
)
def test_v1_4_cannot_grant_mechanics_results_or_live_permission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    permission: str,
) -> None:
    require_identity = contract._require_identity

    def semantic_identity_boundary(identity, label):
        if label == "historical v1.3 predecessor":
            return require_identity(identity, label)
        # This test isolates permission semantics, not historical file survival.
        return tmp_path

    monkeypatch.setattr(contract, "_require_identity", semantic_identity_boundary)
    path = _write_mutation(
        tmp_path,
        lambda payload: payload["permissions"].__setitem__(permission, True),
    )

    with pytest.raises(ValueError, match=f"cannot grant {permission}"):
        validate_execution_amendment_v1_4(path)


def test_identity_checks_actual_bytes_and_missing_files(tmp_path: Path) -> None:
    source = tmp_path / "controlled.txt"
    identity = {"path": str(source), "sha256": hashlib.sha256(b"original").hexdigest()}
    with pytest.raises(ValueError, match="file missing"):
        contract._require_identity(identity, "controlled")
    source.write_bytes(b"original")
    assert contract._require_identity(identity, "controlled") == source
    source.write_bytes(b"changed")
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        contract._require_identity(identity, "controlled")
