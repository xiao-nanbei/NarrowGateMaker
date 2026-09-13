from __future__ import annotations

from copy import deepcopy

import pytest

from models.audit import formal_evidence_governance as governance


def _manifest(*, attempt_id: str = "attempt-20260821-a") -> dict[str, object]:
    contract = {
        "sample_sha256": "1" * 64,
        "baseline_sha256": "2" * 64,
        "candidate_ladder_sha256": "3" * 64,
        "folds_sha256": "4" * 64,
        "estimand_sha256": "5" * 64,
        "statistics_sha256": "6" * 64,
    }
    payload: dict[str, object] = {
        "schema_version": governance.ATTEMPT_MANIFEST_SCHEMA,
        "research_identity": "research-question-v1",
        "attempt_id": attempt_id,
        "research_contract": contract,
        "research_contract_sha256": governance.research_contract_sha256(contract),
        "source": {
            "public_base_commit": "a" * 40,
            "annotated_tag": "research/example/execution-attempt-20260821-a",
            "clean_worktree": True,
        },
        "preflight": {gate: "passed" for gate in governance.PREFLIGHT_GATES},
        "economic_values_read": False,
        "permissions": {"action": False, "live": False},
    }
    payload["manifest_canonical_sha256"] = governance.document_sha256(
        payload, "manifest_canonical_sha256"
    )
    return payload


def _rehash_manifest(payload: dict[str, object]) -> None:
    contract = payload["research_contract"]
    assert isinstance(contract, dict)
    payload["research_contract_sha256"] = governance.research_contract_sha256(
        contract
    )
    payload["manifest_canonical_sha256"] = governance.document_sha256(
        payload, "manifest_canonical_sha256"
    )


def test_attempt_manifest_requires_all_stability_gates() -> None:
    payload = _manifest()
    assert governance.validate_attempt_manifest(payload)["attempt_id"] == (
        "attempt-20260821-a"
    )

    failed = deepcopy(payload)
    preflight = failed["preflight"]
    assert isinstance(preflight, dict)
    preflight["concurrency_cache_durability"] = "failed"
    failed["manifest_canonical_sha256"] = governance.document_sha256(
        failed, "manifest_canonical_sha256"
    )
    with pytest.raises(governance.FormalEvidenceGovernanceError, match="preflight"):
        governance.validate_attempt_manifest(failed)


def test_ordinary_bug_creates_attempt_not_research_version() -> None:
    previous = _manifest()
    current = _manifest(attempt_id="attempt-20260821-b")
    source = current["source"]
    assert isinstance(source, dict)
    source["public_base_commit"] = "b" * 40
    source["annotated_tag"] = "research/example/execution-attempt-20260821-b"
    current["manifest_canonical_sha256"] = governance.document_sha256(
        current, "manifest_canonical_sha256"
    )
    assert governance.classify_attempt_transition(previous, current) == (
        "same_research_identity_new_execution_attempt"
    )

    bad_name = _manifest(attempt_id="attempt-formal-v25")
    with pytest.raises(governance.FormalEvidenceGovernanceError, match="vXX"):
        governance.validate_attempt_manifest(bad_name)


def test_contract_change_requires_new_research_identity() -> None:
    previous = _manifest()
    current = _manifest(attempt_id="attempt-20260821-b")
    contract = current["research_contract"]
    assert isinstance(contract, dict)
    contract["sample_sha256"] = "7" * 64
    _rehash_manifest(current)
    with pytest.raises(
        governance.FormalEvidenceGovernanceError, match="new research identity"
    ):
        governance.classify_attempt_transition(previous, current)

    current["research_identity"] = "research-question-v2"
    current["manifest_canonical_sha256"] = governance.document_sha256(
        current, "manifest_canonical_sha256"
    )
    assert governance.classify_attempt_transition(previous, current) == (
        "new_research_identity_for_changed_contract"
    )


def test_failure_receipt_preserves_same_research_identity() -> None:
    manifest = _manifest()
    receipt: dict[str, object] = {
        "schema_version": governance.FAILURE_RECEIPT_SCHEMA,
        "status": "failed_implementation_attempt",
        "research_identity": manifest["research_identity"],
        "attempt_id": manifest["attempt_id"],
        "run_manifest_canonical_sha256": manifest["manifest_canonical_sha256"],
        "economic_inference_allowed": False,
        "new_research_identity_created": False,
        "failure_class": "implementation_bug",
    }
    receipt["receipt_canonical_sha256"] = governance.document_sha256(
        receipt, "receipt_canonical_sha256"
    )
    assert governance.validate_failure_receipt(receipt, manifest)["status"] == (
        "failed_implementation_attempt"
    )


def test_final_receipt_binds_results_to_pre_run_manifest() -> None:
    manifest = _manifest()
    receipt: dict[str, object] = {
        "schema_version": governance.FINAL_RECEIPT_SCHEMA,
        "status": "complete",
        "research_identity": manifest["research_identity"],
        "attempt_id": manifest["attempt_id"],
        "run_manifest_canonical_sha256": manifest["manifest_canonical_sha256"],
        "research_contract_sha256": manifest["research_contract_sha256"],
        "results": [{"artifact_id": "formal-result", "sha256": "8" * 64}],
    }
    receipt["receipt_canonical_sha256"] = governance.document_sha256(
        receipt, "receipt_canonical_sha256"
    )
    assert governance.validate_final_receipt(receipt, manifest)["result_count"] == 1

    drifted = deepcopy(receipt)
    results = drifted["results"]
    assert isinstance(results, list)
    results[0]["sha256"] = "9" * 64
    with pytest.raises(governance.FormalEvidenceGovernanceError, match="hash drifted"):
        governance.validate_final_receipt(drifted, manifest)
