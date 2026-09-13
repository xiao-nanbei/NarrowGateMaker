from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.families.f04_external_market_alpha.audit.exact_opener_opportunity_tape_contract_v2_2 import (
    canonical_spec_sha256,
    validate_execution_contract_v2_2,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = (
    ROOT
    / "research/families/f04_external_market_alpha/docs/"
    "external_adverse_quote_edge_guard_exact_opener_mechanics_v2_2_"
    "execution_contract_20260803.json"
)


def _mutated(tmp_path: Path, mutate) -> Path:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    for identity in payload["implementation_identity"].values():
        implementation_path = ROOT / identity["path"]
        identity["sha256"] = hashlib.sha256(
            implementation_path.read_bytes()
        ).hexdigest()
    mutate(payload)
    payload["canonical_spec_identity_sha256"] = canonical_spec_sha256(payload)
    path = tmp_path / "mutated.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def test_v2_2_execution_contract_is_frozen_disabled_and_runtime_stale() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    assert canonical_spec_sha256(payload) == payload["canonical_spec_identity_sha256"]
    assert payload["permissions"]["prospective_collection_enabled"] is False
    assert payload["outcome_boundary"]["economic_outcomes_read"] is False
    assert payload["permissions"]["action_experiment_authorized"] is False
    assert payload["permissions"]["live_deployment_authorized"] is False

    # The frozen v2.2 identity must remain stale after any shared runtime
    # dependency changes.  The validator reports the first mismatch in its
    # deterministic implementation-identity order.
    with pytest.raises(
        ValueError,
        match="(?:runtime_writer|order_lifecycle|feature_dag) SHA256 mismatch",
    ):
        validate_execution_contract_v2_2(CONTRACT)


def test_v2_2_rejects_half_window_splicing_permission(tmp_path: Path) -> None:
    path = _mutated(
        tmp_path,
        lambda payload: payload["admission_contract"].__setitem__(
            "overlap_or_half_window_splicing_forbidden", False
        ),
    )
    with pytest.raises(ValueError, match="admission contract is incomplete"):
        validate_execution_contract_v2_2(path)


def test_v2_2_cannot_grant_collection_or_action_authority(
    tmp_path: Path,
) -> None:
    path = _mutated(
        tmp_path,
        lambda payload: payload["permissions"].__setitem__(
            "prospective_collection_enabled", True
        ),
    )
    with pytest.raises(ValueError, match="cannot grant"):
        validate_execution_contract_v2_2(path)


def test_v2_2_requires_runtime_component_hashes(tmp_path: Path) -> None:
    path = _mutated(
        tmp_path,
        lambda payload: payload["implementation_identity"]["runtime_writer"].__setitem__(
            "sha256", "0" * 64
        ),
    )
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        validate_execution_contract_v2_2(path)
