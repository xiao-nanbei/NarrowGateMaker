from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_active_release as legacy_release
from scripts import f05_buy_e3_direct_owner_release as subject
from strategy import boolean_cooldown_buy_e3 as runtime

MANIFEST_FILE_SHA256 = (
    "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929"
)
POLICY_FILE_SHA256 = (
    "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b"
)
PREDICATE_FILE_SHA256 = (
    "4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915"
)
EXECUTION = {
    "execution_commit": "a" * 40,
    "execution_tree": "b" * 40,
    "annotated_operational_tag": "f05-buy-e3-direct-owner-live-v1",
    "annotated_operational_tag_object": "c" * 40,
    "tag_peeled_commit": "a" * 40,
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
        "incomplete_evidence": deepcopy(subject.INCOMPLETE_EVIDENCE),
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


def test_builder_freezes_direct_owner_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(legacy_release, "_operational_git_identity", lambda *_: EXECUTION)
    monkeypatch.setattr(
        subject,
        "_artifact_release",
        lambda _paths: (
            {"artifact_sha256": subject.EXACT_ARTIFACT_SHA256},
            _roles(),
        ),
    )

    payload = subject.build_direct_owner_release(
        repository_root=Path("/unused"),
        annotated_operational_tag=EXECUTION["annotated_operational_tag"],
        artifact_paths={
            "manifest": Path("manifest"),
            "policy": Path("policy"),
            "predicate_bundle": Path("predicates"),
        },
        generated_utc="2026-08-24T00:00:00Z",
    )

    assert payload["research_supported"] is False
    assert payload["formal_hierarchy_passed"] is False
    assert payload["formal_hard_gates_passed"] is False
    assert payload["owner_risk_accepted"] is True
    assert payload["outcome_informed_owner_override"] is True
    assert payload["scope"]["side"] == "BUY"
    assert payload["incomplete_evidence"]["v5_exact_panel_rebuild_complete"] is False
    assert payload["incomplete_evidence"]["panel_rebuild_continues"] is True
    assert payload["evidence_boundary"]["shadow_created"] is False
    assert payload["evidence_boundary"]["companion_created"] is False
    assert payload["evidence_boundary"]["new_economic_arm_run"] is False


def test_runtime_and_builder_share_direct_release_identity() -> None:
    assert runtime.DIRECT_OWNER_ACTIVE_RELEASE_SCHEMA == subject.SCHEMA_VERSION
    assert runtime.DIRECT_OWNER_ACTIVE_RELEASE_IDENTITY == subject.IDENTITY
    assert runtime.DIRECT_OWNER_ACTIVE_RELEASE_STATUS == subject.STATUS
    assert runtime.DIRECT_OWNER_EXACT_ARTIFACT_SHA256 == subject.EXACT_ARTIFACT_SHA256


def test_runtime_accepts_exact_direct_owner_release() -> None:
    identity = _runtime_validate(_payload())

    assert identity == {
        "file_canonical_sha256": _payload()["canonical_active_release_sha256"],
        "execution_commit": "a" * 40,
        "execution_tree": "b" * 40,
        "annotated_operational_tag": "f05-buy-e3-direct-owner-live-v1",
        "annotated_operational_tag_object": "c" * 40,
    }


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        (None, "research_supported", True),
        (None, "owner_risk_accepted", False),
        (None, "outcome_informed_owner_override", False),
        ("incomplete_evidence", "v5_exact_panel_rebuild_complete", True),
        ("incomplete_evidence", "panel_rebuild_continues", False),
        ("evidence_boundary", "shadow_created", True),
        ("evidence_boundary", "companion_created", True),
        ("evidence_boundary", "new_economic_arm_run", True),
    ],
)
def test_runtime_rejects_direct_owner_boundary_drift(
    section: str | None,
    field: str,
    value: Any,
) -> None:
    payload = _payload()
    target = payload if section is None else payload[section]
    target[field] = value
    _rewrite_canonical(payload)

    with pytest.raises(ValueError, match="authority|evidence_boundary"):
        _runtime_validate(payload)


def test_runtime_rejects_another_artifact() -> None:
    payload = _payload()
    payload["exact_artifact"]["artifact_sha256"] = "f" * 64
    _rewrite_canonical(payload)

    with pytest.raises(ValueError, match="artifact_sha256"):
        _runtime_validate(payload)


def test_standalone_validator_accepts_exact_direct_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(legacy_release, "_operational_git_identity", lambda *_: EXECUTION)
    path = tmp_path / "direct-release.json"
    path.write_text(json.dumps(_payload(), sort_keys=True, indent=2) + "\n", encoding="ascii")
    path.chmod(0o600)

    validated = subject.validate_direct_owner_release(
        path,
        repository_root=tmp_path,
    )

    assert validated == _payload()


def test_standalone_validator_does_not_accept_legacy_v1(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(legacy_release, "_operational_git_identity", lambda *_: EXECUTION)
    payload = _payload()
    payload["schema_version"] = legacy_release.ACTIVE_RELEASE_SCHEMA
    _rewrite_canonical(payload)
    path = tmp_path / "legacy-shaped.json"
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="ascii")
    path.chmod(0o600)

    with pytest.raises(subject.DirectOwnerReleaseError, match="authority"):
        subject.validate_direct_owner_release(path, repository_root=tmp_path)
