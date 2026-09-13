#!/usr/bin/env python3
"""Validate the execution-only v1.2 ranked-toxicity adapter amendment."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from execution.order_lifecycle_journal import (
    ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    SCHEMA_VERSION as ADAPTER_SCHEMA_VERSION,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    ZERO_TOLERANCE_KEYS,
    lifecycle_branch_contract,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "ranked_toxicity_guard_full_path_execution_amendment.v1.2"
PREDECESSOR_SCHEMA_VERSION = (
    "ranked_toxicity_guard_full_path_execution_amendment.v1.1"
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
    path = _resolve(str(identity["path"]))
    actual = sha256_file(path)
    expected = str(identity["sha256"])
    if actual != expected:
        raise ValueError(f"{label} SHA256 mismatch: {actual} != {expected}")
    return path


def _critical_projection(spec: Mapping[str, Any]) -> dict[str, Any]:
    behavior = spec["behavior_policy"]
    threshold = spec["threshold_contract"]
    baseline = spec["baseline_contract"]
    return {
        "family_id": spec["family_id"],
        "side": spec["side"],
        "prediction_head": spec["prediction_contract"]["head"],
        "rank": threshold["rank"],
        "quantile": threshold["quantile"],
        "history": threshold["history"],
        "random_seed": behavior["random_seed"],
        "probabilities": behavior["probabilities"],
        "assignment_unit": behavior["assignment_unit"],
        "q90_action_enabled": baseline["q90_action_enabled"],
        "buy_fill_selection_same_in_both_arms": baseline[
            "buy_fill_selection_same_in_both_arms"
        ],
        "reducing_quotes_unchanged": baseline["reducing_quotes_unchanged"],
        "scorecard_profile": spec["scorecard_profile"],
    }


def _validate_predecessor(amendment: Mapping[str, Any]) -> Mapping[str, Any]:
    identity = amendment.get("historical_v1_1_predecessor") or {}
    path = _require_identity(identity, "historical v1.1 predecessor")
    predecessor = json.loads(path.read_text(encoding="utf-8"))
    if predecessor.get("schema_version") != PREDECESSOR_SCHEMA_VERSION:
        raise ValueError("unexpected historical v1.1 predecessor schema")
    expected_canonical = str(identity.get("canonical_spec_identity_sha256", ""))
    if str(predecessor.get("canonical_spec_identity_sha256", "")) != expected_canonical:
        raise ValueError("historical v1.1 canonical identity drifted")
    if canonical_spec_sha256(predecessor) != expected_canonical:
        raise ValueError("historical v1.1 canonical content drifted")
    return predecessor


def validate_execution_amendment_v1_2(amendment_path: Path) -> dict[str, Any]:
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    if amendment.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected execution-amendment schema")
    if amendment.get("adapter_schema_version") != ADAPTER_SCHEMA_VERSION:
        raise ValueError("adapter schema identity drifted")
    if canonical_spec_sha256(amendment) != str(
        amendment.get("canonical_spec_identity_sha256", "")
    ):
        raise ValueError("execution amendment canonical hash mismatch")

    predecessor = _validate_predecessor(amendment)
    source_specs = amendment.get("frozen_v1_registrations") or {}
    if set(source_specs) != {"BUY", "SELL"}:
        raise ValueError("execution amendment must bind BUY and SELL v1 specs")
    projections: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        identity = source_specs[side]
        path = _require_identity(identity, f"{side} frozen v1 registration")
        spec = json.loads(path.read_text(encoding="utf-8"))
        if str(spec.get("canonical_spec_identity_sha256", "")) != str(
            identity.get("canonical_spec_identity_sha256", "")
        ):
            raise ValueError(f"{side} frozen canonical identity drifted")
        if spec["side"] != side:
            raise ValueError(f"{side} source registration side drifted")
        if spec["execution_boundary"]["mechanics_execution_allowed_now"] is not False:
            raise ValueError("frozen v1 registration unexpectedly opened mechanics")
        projections[side] = _critical_projection(spec)
    if projections != amendment.get("unchanged_v1_contract_projection"):
        raise ValueError("v1 action/threshold/seed/baseline projection changed")
    if projections != predecessor.get("unchanged_v1_contract_projection"):
        raise ValueError("v1.2 projection differs from historical v1.1")

    expected_zero = {key: 0 for key in ZERO_TOLERANCE_KEYS}
    if amendment.get("zero_tolerance_gates") != expected_zero:
        raise ValueError("execution amendment changed zero-tolerance gates")
    if amendment.get("lifecycle_branch_contract") != lifecycle_branch_contract():
        raise ValueError("lifecycle routing matrix drifted from implementation")

    refresh = amendment.get("shared_lifecycle_refresh_contract") or {}
    old_lifecycle = predecessor["implementation_identity"][
        "shared_order_lifecycle"
    ]["sha256"]
    if refresh.get("predecessor_shared_order_lifecycle_sha256") != old_lifecycle:
        raise ValueError("v1.2 does not bind the predecessor lifecycle identity")
    for key in (
        "action_semantics_changed",
        "threshold_changed",
        "randomization_changed",
        "baseline_changed",
        "economic_gate_changed",
    ):
        if refresh.get(key) is not False:
            raise ValueError(f"shared lifecycle refresh changed {key}")
    expected_missing = {
        "exchange_exposure_valid": False,
        "exchange_exposure_complete": False,
        "exchange_exposure_value": None,
        "exchange_exposure_invalid_reason": (
            "missing_exchange_timestamp:activate"
        ),
    }
    if refresh.get("missing_activation_timestamp_state") != expected_missing:
        raise ValueError("missing activation timestamp contract drifted")
    if refresh.get("authoritative_journal_schema") != (
        ORDER_LIFECYCLE_JOURNAL_SCHEMA_VERSION
    ):
        raise ValueError("shared lifecycle journal schema drifted")
    if refresh.get("cpp_quantity_time_exposure_authority") is not False:
        raise ValueError("v1.2 cannot claim C++ quantity-time exposure authority")
    if refresh.get("historical_v1_1_current_execution_eligible") is not False:
        raise ValueError("historical v1.1 cannot remain current after dependency drift")

    for label, identity in amendment.get("implementation_identity", {}).items():
        _require_identity(identity, label)
    for label, identity in amendment.get("documentation_identity", {}).items():
        _require_identity(identity, label)

    test_contract = amendment.get("contract_tests") or {}
    if test_contract.get("passed") is not True:
        raise ValueError("execution amendment contract tests are not frozen as passed")
    if test_contract.get("full_repository_suite_passed") is not True:
        raise ValueError("full repository regression suite is not frozen as passed")
    required_tests = {
        "test_v1_1_is_preserved_as_historical_dependency_identity",
        "test_v1_2_preserves_v1_action_and_grants_mechanics_only",
        "test_missing_exchange_activation_invalidates_only_physical_exposure",
        "test_journal_audit_reports_missing_activation_clock_without_cpp_authority",
    }
    if not required_tests.issubset(set(test_contract.get("required_tests") or [])):
        raise ValueError("execution amendment omits dependency-refresh tests")

    permissions = amendment.get("permissions") or {}
    if permissions.get("mechanics_execution_eligible") is not True:
        raise ValueError("v1.2 did not restore mechanics eligibility")
    for forbidden in (
        "mechanics_read",
        "development_economic_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"execution-only amendment cannot grant {forbidden}")
    if amendment.get("economic_outcome_columns_read") != []:
        raise ValueError("execution-only amendment read economic outcomes")

    return {
        "schema_version": f"{SCHEMA_VERSION}.audit",
        "canonical_spec_identity_sha256": amendment[
            "canonical_spec_identity_sha256"
        ],
        "historical_v1_1_bytes_valid": True,
        "historical_v1_1_current_execution_eligible": False,
        "frozen_v1_hashes_valid": True,
        "unchanged_v1_contract_projection": projections,
        "implementation_hashes_valid": True,
        "lifecycle_branch_contract_valid": True,
        "shared_lifecycle_refresh_contract_valid": True,
        "cpp_quantity_time_exposure_authority": False,
        "zero_tolerance_gates": expected_zero,
        "contract_tests_passed": True,
        "stage": "v1_2_dependency_refresh_hash_bound_mechanics_not_run",
        "economic_outcome_columns_read": [],
        "permissions": permissions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--amendment", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_execution_amendment_v1_2(args.amendment.resolve())
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
