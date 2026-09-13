#!/usr/bin/env python3
"""Validate the P2 exact-opener v2.1 execution amendment."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from research.families.f04_external_market_alpha.audit.exact_opener_opportunity_tape import (
    exact_opener_validator_contract,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = (
    "external_adverse_quote_edge_guard_exact_opener_execution_amendment.v2.1"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_identity_sha256", None)
    return canonical_sha256(normalized)


def _resolve(path_value: str) -> Path:
    path = Path(str(path_value)).expanduser()
    return path if path.is_absolute() else ROOT / path


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = _resolve(str(identity.get("path", "")))
    if not path.is_file():
        raise ValueError(f"{label} file missing: {path}")
    actual = sha256_file(path)
    expected = str(identity.get("sha256", ""))
    if actual != expected:
        raise ValueError(f"{label} SHA256 mismatch: {actual} != {expected}")
    return path


def validate_execution_amendment_v2_1(amendment_path: Path) -> dict[str, Any]:
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    if amendment.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected exact-opener execution amendment schema")
    expected_canonical = str(
        amendment.get("canonical_spec_identity_sha256", "")
    )
    if canonical_spec_sha256(amendment) != expected_canonical:
        raise ValueError("exact-opener execution amendment canonical hash mismatch")

    predecessor = amendment.get("frozen_v2_predecessor") or {}
    _require_identity(predecessor.get("spec") or {}, "frozen v2 spec")
    _require_identity(
        predecessor.get("registration") or {},
        "frozen v2 registration",
    )
    if predecessor.get("identity") != (
        "external_adverse_quote_edge_guard_exact_opener_mechanics_v2"
    ):
        raise ValueError("v2.1 predecessor identity drifted")
    if predecessor.get("v2_files_modified") is not False:
        raise ValueError("v2.1 cannot modify the frozen v2 registration")

    expected_contract = exact_opener_validator_contract()
    if amendment.get("validator_contract") != expected_contract:
        raise ValueError("exact-opener validator contract drifted")
    gate = expected_contract["candidate_rate_gate"]
    if gate != {
        "minimum": 0.05,
        "scope": "BUY_and_SELL_each_independently",
        "pooled_rate": "diagnostic_only",
    }:
        raise ValueError("side-specific five-percent support gate drifted")

    provenance = amendment.get("research_provenance") or {}
    if provenance.get("successor_class") != "mechanics_informed_successor":
        raise ValueError("v2.1 successor provenance is not explicit")
    if provenance.get("same_as_v1_estimand") is not False:
        raise ValueError("v2.1 cannot be presented as the v1 estimand")
    if provenance.get("first_add_or_P1_rows_in_scope") is not False:
        raise ValueError("v2.1 cannot absorb first-add or P1 rows")

    outcome_boundary = amendment.get("outcome_boundary") or {}
    if outcome_boundary.get("economic_outcomes_read") is not False:
        raise ValueError("v2.1 cannot read economic outcomes")
    if outcome_boundary.get("operational_lifecycle_outcomes_read") is not True:
        raise ValueError("v2.1 must disclose lifecycle outcomes")
    if outcome_boundary.get("external_outcome_tables_allowed") is not False:
        raise ValueError("v2.1 cannot join an external outcome table")

    for label, identity in amendment.get("implementation_identity", {}).items():
        _require_identity(identity, label)

    test_contract = amendment.get("contract_tests") or {}
    if test_contract.get("passed") is not True:
        raise ValueError("v2.1 targeted contract tests are not frozen as passed")
    if test_contract.get("full_repository_suite_passed") is not True:
        raise ValueError("v2.1 full repository suite is not frozen as passed")

    permissions = amendment.get("permissions") or {}
    if permissions.get("prospective_collection_eligible") is not True:
        raise ValueError("v2.1 did not restore prospective collection eligibility")
    for forbidden in (
        "prospective_tape_read",
        "economic_outcome_read",
        "prediction_authority",
        "transport_supported",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"collection-only v2.1 cannot grant {forbidden}")

    return {
        "schema_version": f"{SCHEMA_VERSION}.audit",
        "canonical_spec_identity_sha256": expected_canonical,
        "frozen_v2_bytes_valid": True,
        "exact_schema_allowlist_valid": True,
        "exact_opener_denominator_contract_valid": True,
        "side_specific_candidate_rate_gate_valid": True,
        "lifecycle_identity_and_terminal_contract_valid": True,
        "economic_outcomes_read": False,
        "operational_lifecycle_outcomes_read": True,
        "implementation_hashes_valid": True,
        "stage": "v2_1_hash_bound_waiting_for_prospective_exact_tape",
        "permissions": permissions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--amendment", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_execution_amendment_v2_1(args.amendment.resolve())
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(text, end="")
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(args.output)


if __name__ == "__main__":
    main()
