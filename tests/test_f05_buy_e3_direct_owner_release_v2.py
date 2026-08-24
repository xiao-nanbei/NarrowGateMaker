from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_active_release as legacy_release
from scripts import f05_buy_e3_direct_owner_release as parent_release
from scripts import f05_buy_e3_direct_owner_release_v2 as subject
from strategy import boolean_cooldown_buy_e3 as runtime

MANIFEST_FILE_SHA256 = "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929"
POLICY_FILE_SHA256 = "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b"
PREDICATE_FILE_SHA256 = "4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915"
EXECUTION = {
    "execution_commit": "a" * 40,
    "execution_tree": "b" * 40,
    "annotated_operational_tag": "f05-buy-e3-direct-owner-live-v4",
    "annotated_operational_tag_object": "c" * 40,
    "tag_peeled_commit": "a" * 40,
}
SUPPLEMENT_BINDING = {
    "schema_version": "f05_buy_e3_lifecycle_reject_fix_supplement.v1",
    "status": "lifecycle_only_runtime_fix_verified_no_economic_change",
    "file_sha256": "1" * 64,
    "canonical_field": "canonical_supplement_sha256",
    "canonical_sha256": "2" * 64,
    "size_bytes": 1234,
    "mode": "0600",
}


def _binding(role: str, file_sha256: str, canonical_sha256: str) -> dict[str, Any]:
    contract = legacy_release._CONTRACTS[role]  # noqa: SLF001
    return {
        "role": role,
        "path": f"artifact/{role}-{file_sha256}.json",
        "file_sha256": file_sha256,
        "size_bytes": 100,
        "mode": "0600",
        "device": None,
        "inode": None,
        "schema_version": contract.schema,
        "identity": contract.identity,
        "status": contract.status,
        "canonical_field": contract.canonical_field,
        "canonical_sha256": canonical_sha256,
    }


def _roles() -> dict[str, dict[str, Any]]:
    return {
        "manifest": _binding(
            "manifest",
            MANIFEST_FILE_SHA256,
            subject.EXACT_ARTIFACT_SHA256,
        ),
        "policy": _binding("policy", POLICY_FILE_SHA256, "d" * 64),
        "predicate_bundle": _binding(
            "predicate_bundle",
            PREDICATE_FILE_SHA256,
            "e" * 64,
        ),
    }


def _payload() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": subject.SCHEMA_VERSION,
        "identity": subject.IDENTITY,
        "status": subject.STATUS,
        "generated_utc": "2026-08-24T00:00:00Z",
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": deepcopy(subject.AUTHORIZATION_BASIS),
        "scope": deepcopy(subject.SCOPE),
        "execution": deepcopy(EXECUTION),
        "exact_artifact": {
            "artifact_sha256": subject.EXACT_ARTIFACT_SHA256,
            "roles": _roles(),
        },
        "parent_direct_owner_release": deepcopy(subject.PARENT_DIRECT_OWNER_RELEASE),
        "historical_evidence_state": deepcopy(subject.HISTORICAL_EVIDENCE_STATE),
        "historical_attempt4_anchor": deepcopy(subject.HISTORICAL_ATTEMPT4_ANCHOR),
        "exact_v5_recovery": deepcopy(subject.EXACT_V5_RECOVERY),
        "pending_current_runtime_evidence": deepcopy(subject.PENDING_CURRENT_RUNTIME_EVIDENCE),
        "lifecycle_fix_contract": deepcopy(subject.LIFECYCLE_FIX_CONTRACT),
        "lifecycle_fix_supplement": deepcopy(SUPPLEMENT_BINDING),
        "rollback": deepcopy(subject.ROLLBACK),
        "evidence_boundary": deepcopy(subject.EVIDENCE_BOUNDARY),
    }
    payload["canonical_active_release_sha256"] = legacy_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )
    return payload


def _runtime_validate(payload: dict[str, Any]) -> dict[str, str]:
    return runtime._validate_active_release(  # noqa: SLF001
        payload,
        expected_canonical_sha256=payload["canonical_active_release_sha256"],
        expected_artifact_sha256=subject.EXACT_ARTIFACT_SHA256,
        expected_manifest_file_sha256=MANIFEST_FILE_SHA256,
        expected_policy_file_sha256=POLICY_FILE_SHA256,
        expected_predicate_bundle_file_sha256=PREDICATE_FILE_SHA256,
    )


def _rewrite_canonical(payload: dict[str, Any]) -> None:
    payload["canonical_active_release_sha256"] = legacy_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )


def test_runtime_accepts_exact_direct_owner_v2_release() -> None:
    validated = _runtime_validate(_payload())
    assert validated["execution_commit"] == EXECUTION["execution_commit"]
    assert validated["execution_tree"] == EXECUTION["execution_tree"]


@pytest.mark.parametrize(
    ("field", "mutation"),
    [
        ("historical_evidence_state", {"research_supported": True}),
        ("historical_attempt4_anchor", {"file_sha256": "9" * 64}),
        ("exact_v5_recovery", {"canonical_sha256": "8" * 64}),
        ("pending_current_runtime_evidence", {"v4_resource_gate_complete": True}),
        ("lifecycle_fix_contract", {"e3_decision_semantics_unchanged": False}),
        ("lifecycle_fix_supplement", {"mode": "0644"}),
    ],
)
def test_runtime_rejects_direct_owner_v2_evidence_drift(
    field: str,
    mutation: dict[str, Any],
) -> None:
    payload = _payload()
    payload[field].update(mutation)
    _rewrite_canonical(payload)
    with pytest.raises(ValueError):
        _runtime_validate(payload)


def test_builder_separates_completed_and_pending_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        legacy_release,
        "_operational_git_identity",
        lambda *_: EXECUTION,
    )
    monkeypatch.setattr(
        parent_release,
        "_artifact_release",
        lambda _paths: (
            {"artifact_sha256": subject.EXACT_ARTIFACT_SHA256},
            _roles(),
        ),
    )
    monkeypatch.setattr(
        subject,
        "_parent_binding",
        lambda _path: deepcopy(subject.PARENT_DIRECT_OWNER_RELEASE),
    )
    monkeypatch.setattr(
        subject,
        "_historical_binding",
        lambda _path, expected, _label: deepcopy(expected),
    )
    monkeypatch.setattr(
        subject.supplement,
        "supplement_binding",
        lambda *_args, **_kwargs: deepcopy(SUPPLEMENT_BINDING),
    )
    payload = subject.build_direct_owner_release_v2(
        repository_root=Path("/repo"),
        annotated_operational_tag="tag-v4",
        artifact_paths={},
        parent_direct_release_path=Path("parent.json"),
        exact_v5_verify_path=Path("v5.json"),
        attempt4_successor_path=Path("attempt4.json"),
        lifecycle_fix_supplement_path=Path("supplement.json"),
        generated_utc="2026-08-24T00:00:00Z",
    )
    assert payload["historical_evidence_state"] == {
        "attempt4_mechanics_and_stability_complete": True,
        "exact_v5_mechanics_recovery_complete": True,
        "attempt4_resource_or_activation_claimed": False,
        "research_supported": False,
    }
    assert all(value is False for value in payload["pending_current_runtime_evidence"].values())
    assert "incomplete_evidence" not in payload
    assert "panel_rebuild_continues" not in str(payload)
