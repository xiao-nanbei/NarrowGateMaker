#!/usr/bin/env python3
"""Validate formal research contracts, execution attempts, and result receipts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

ATTEMPT_MANIFEST_SCHEMA = "narrowgate_formal_execution_attempt_manifest.v1"
FAILURE_RECEIPT_SCHEMA = "narrowgate_formal_execution_attempt_failure.v1"
FINAL_RECEIPT_SCHEMA = "narrowgate_formal_execution_final_receipt.v1"
RESEARCH_CONTRACT_FIELDS = (
    "sample_sha256",
    "baseline_sha256",
    "candidate_ladder_sha256",
    "folds_sha256",
    "estimand_sha256",
    "statistics_sha256",
)
PREFLIGHT_GATES = (
    "single_day_end_to_end",
    "all_fold_zero_economic",
    "concurrency_cache_durability",
    "regression_and_parity",
    "complete_output_smoke",
    "clean_worktree",
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40,64}$")
_ATTEMPT_ID = re.compile(r"^attempt-[a-z0-9][a-z0-9._-]*$")
_FORMAL_VERSION_ALIAS = re.compile(r"formal[-_]v\d+", re.IGNORECASE)


class FormalEvidenceGovernanceError(ValueError):
    """Raised when formal evidence identity layers are mixed or incomplete."""


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def document_sha256(payload: Mapping[str, Any], digest_field: str) -> str:
    body = dict(payload)
    body.pop(digest_field, None)
    return canonical_sha256(body)


def _mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FormalEvidenceGovernanceError(f"{name} must be a mapping")
    return value


def _nonempty(value: object, *, name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise FormalEvidenceGovernanceError(f"{name} is empty")
    return normalized


def _sha(value: object, *, name: str) -> str:
    normalized = str(value or "").strip()
    if _SHA256.fullmatch(normalized) is None:
        raise FormalEvidenceGovernanceError(f"{name} is not a SHA256")
    return normalized


def research_contract_snapshot(contract: Mapping[str, Any]) -> dict[str, str]:
    source = _mapping(contract, name="research_contract")
    missing = [field for field in RESEARCH_CONTRACT_FIELDS if field not in source]
    if missing:
        raise FormalEvidenceGovernanceError(
            f"research_contract is missing fields: {missing}"
        )
    return {
        field: _sha(source[field], name=f"research_contract.{field}")
        for field in RESEARCH_CONTRACT_FIELDS
    }


def research_contract_sha256(contract: Mapping[str, Any]) -> str:
    return canonical_sha256(research_contract_snapshot(contract))


def validate_attempt_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    manifest = dict(_mapping(payload, name="attempt_manifest"))
    if manifest.get("schema_version") != ATTEMPT_MANIFEST_SCHEMA:
        raise FormalEvidenceGovernanceError("attempt manifest schema is invalid")

    research_identity = _nonempty(
        manifest.get("research_identity"), name="research_identity"
    )
    attempt_id = _nonempty(manifest.get("attempt_id"), name="attempt_id")
    if _ATTEMPT_ID.fullmatch(attempt_id) is None:
        raise FormalEvidenceGovernanceError("attempt_id must use the attempt-* namespace")
    if _FORMAL_VERSION_ALIAS.search(attempt_id):
        raise FormalEvidenceGovernanceError(
            "attempt_id must not manufacture a formal research vXX"
        )

    contract = research_contract_snapshot(
        _mapping(manifest.get("research_contract"), name="research_contract")
    )
    contract_sha = research_contract_sha256(contract)
    if manifest.get("research_contract_sha256") != contract_sha:
        raise FormalEvidenceGovernanceError("research contract hash drifted")

    source = _mapping(manifest.get("source"), name="source")
    commit = _nonempty(source.get("public_base_commit"), name="public_base_commit")
    if _COMMIT.fullmatch(commit) is None:
        raise FormalEvidenceGovernanceError("public_base_commit is invalid")
    _nonempty(source.get("annotated_tag"), name="annotated_tag")
    if source.get("clean_worktree") is not True:
        raise FormalEvidenceGovernanceError("formal source worktree was not clean")

    preflight = _mapping(manifest.get("preflight"), name="preflight")
    failed = [gate for gate in PREFLIGHT_GATES if preflight.get(gate) != "passed"]
    if failed:
        raise FormalEvidenceGovernanceError(f"formal preflight gates failed: {failed}")
    if manifest.get("economic_values_read") is not False:
        raise FormalEvidenceGovernanceError(
            "formal attempt manifest must be frozen before economics are read"
        )

    permissions = _mapping(manifest.get("permissions"), name="permissions")
    if permissions.get("action") is not False or permissions.get("live") is not False:
        raise FormalEvidenceGovernanceError(
            "formal research attempt cannot grant action or live authority"
        )

    expected_manifest_sha = document_sha256(manifest, "manifest_canonical_sha256")
    if manifest.get("manifest_canonical_sha256") != expected_manifest_sha:
        raise FormalEvidenceGovernanceError("attempt manifest canonical hash drifted")

    return {
        "research_identity": research_identity,
        "attempt_id": attempt_id,
        "research_contract": contract,
        "research_contract_sha256": contract_sha,
        "manifest_canonical_sha256": expected_manifest_sha,
        "public_base_commit": commit,
    }


def classify_attempt_transition(
    previous: Mapping[str, Any], current: Mapping[str, Any]
) -> str:
    prior = validate_attempt_manifest(previous)
    next_attempt = validate_attempt_manifest(current)
    if prior["attempt_id"] == next_attempt["attempt_id"]:
        raise FormalEvidenceGovernanceError("execution attempt id was reused")

    contract_unchanged = (
        prior["research_contract_sha256"]
        == next_attempt["research_contract_sha256"]
    )
    identity_unchanged = (
        prior["research_identity"] == next_attempt["research_identity"]
    )
    if contract_unchanged and not identity_unchanged:
        raise FormalEvidenceGovernanceError(
            "unchanged research contract must retain its research identity"
        )
    if not contract_unchanged and identity_unchanged:
        raise FormalEvidenceGovernanceError(
            "changed research contract requires a new research identity"
        )
    return (
        "same_research_identity_new_execution_attempt"
        if contract_unchanged
        else "new_research_identity_for_changed_contract"
    )


def validate_failure_receipt(
    payload: Mapping[str, Any], manifest_payload: Mapping[str, Any]
) -> dict[str, Any]:
    receipt = dict(_mapping(payload, name="failure_receipt"))
    manifest = validate_attempt_manifest(manifest_payload)
    if receipt.get("schema_version") != FAILURE_RECEIPT_SCHEMA:
        raise FormalEvidenceGovernanceError("failure receipt schema is invalid")
    if receipt.get("status") != "failed_implementation_attempt":
        raise FormalEvidenceGovernanceError("failure receipt status is invalid")
    if receipt.get("research_identity") != manifest["research_identity"]:
        raise FormalEvidenceGovernanceError("failure research identity drifted")
    if receipt.get("attempt_id") != manifest["attempt_id"]:
        raise FormalEvidenceGovernanceError("failure attempt identity drifted")
    if (
        receipt.get("run_manifest_canonical_sha256")
        != manifest["manifest_canonical_sha256"]
    ):
        raise FormalEvidenceGovernanceError("failure manifest binding drifted")
    if receipt.get("economic_inference_allowed") is not False:
        raise FormalEvidenceGovernanceError("failed attempt cannot support economics")
    if receipt.get("new_research_identity_created") is not False:
        raise FormalEvidenceGovernanceError(
            "ordinary implementation failure cannot create a research identity"
        )
    expected = document_sha256(receipt, "receipt_canonical_sha256")
    if receipt.get("receipt_canonical_sha256") != expected:
        raise FormalEvidenceGovernanceError("failure receipt canonical hash drifted")
    return {"status": receipt["status"], "receipt_canonical_sha256": expected}


def validate_final_receipt(
    payload: Mapping[str, Any], manifest_payload: Mapping[str, Any]
) -> dict[str, Any]:
    receipt = dict(_mapping(payload, name="final_receipt"))
    manifest = validate_attempt_manifest(manifest_payload)
    if receipt.get("schema_version") != FINAL_RECEIPT_SCHEMA:
        raise FormalEvidenceGovernanceError("final receipt schema is invalid")
    if receipt.get("status") != "complete":
        raise FormalEvidenceGovernanceError("final receipt status is invalid")
    if receipt.get("research_identity") != manifest["research_identity"]:
        raise FormalEvidenceGovernanceError("final research identity drifted")
    if receipt.get("attempt_id") != manifest["attempt_id"]:
        raise FormalEvidenceGovernanceError("final attempt identity drifted")
    if (
        receipt.get("run_manifest_canonical_sha256")
        != manifest["manifest_canonical_sha256"]
    ):
        raise FormalEvidenceGovernanceError("final manifest binding drifted")
    if (
        receipt.get("research_contract_sha256")
        != manifest["research_contract_sha256"]
    ):
        raise FormalEvidenceGovernanceError("final research contract drifted")

    results = receipt.get("results")
    if not isinstance(results, list) or not results:
        raise FormalEvidenceGovernanceError("final receipt has no result artifacts")
    for index, row in enumerate(results):
        result = _mapping(row, name=f"results[{index}]")
        _nonempty(result.get("artifact_id"), name=f"results[{index}].artifact_id")
        _sha(result.get("sha256"), name=f"results[{index}].sha256")

    expected = document_sha256(receipt, "receipt_canonical_sha256")
    if receipt.get("receipt_canonical_sha256") != expected:
        raise FormalEvidenceGovernanceError("final receipt canonical hash drifted")
    return {
        "status": receipt["status"],
        "result_count": len(results),
        "receipt_canonical_sha256": expected,
    }


__all__ = [
    "ATTEMPT_MANIFEST_SCHEMA",
    "FAILURE_RECEIPT_SCHEMA",
    "FINAL_RECEIPT_SCHEMA",
    "FormalEvidenceGovernanceError",
    "PREFLIGHT_GATES",
    "RESEARCH_CONTRACT_FIELDS",
    "canonical_sha256",
    "classify_attempt_transition",
    "document_sha256",
    "research_contract_sha256",
    "research_contract_snapshot",
    "validate_attempt_manifest",
    "validate_failure_receipt",
    "validate_final_receipt",
]
