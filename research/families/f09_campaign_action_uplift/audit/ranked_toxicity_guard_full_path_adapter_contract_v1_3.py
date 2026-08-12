#!/usr/bin/env python3
"""Validate the continuous-path scorecard successor for ranked toxicity."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from models.audit.experiment_scorecard_v2 import score_profile_contract
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    SCHEMA_VERSION as ADAPTER_SCHEMA_VERSION,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "ranked_toxicity_guard_full_path_execution_amendment.v1.3"
PREDECESSOR_SCHEMA_VERSION = (
    "ranked_toxicity_guard_full_path_execution_amendment.v1.2"
)
OUTCOME_SCHEMA_VERSION = "ranked_toxicity_exposure_guard_outcome_contract.v2"
PROFILE_ID = "action_execution_selective_v3"


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


def canonical_spec_sha256(
    payload: Mapping[str, Any], *, identity_field: str
) -> str:
    normalized = dict(payload)
    normalized.pop(identity_field, None)
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


def _action_projection(spec: Mapping[str, Any]) -> dict[str, Any]:
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
    }


def _load_canonical(
    identity: Mapping[str, Any],
    *,
    label: str,
    identity_field: str,
) -> Mapping[str, Any]:
    path = _require_identity(identity, label)
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(identity.get(identity_field, ""))
    if str(payload.get(identity_field, "")) != expected:
        raise ValueError(f"{label} embedded canonical identity drifted")
    actual = canonical_spec_sha256(payload, identity_field=identity_field)
    if actual != expected:
        raise ValueError(f"{label} canonical content drifted")
    return payload


def validate_execution_amendment_v1_3(amendment_path: Path) -> dict[str, Any]:
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    if amendment.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected execution-amendment schema")
    if amendment.get("adapter_schema_version") != ADAPTER_SCHEMA_VERSION:
        raise ValueError("adapter schema identity drifted")
    expected_canonical = str(
        amendment.get("canonical_spec_identity_sha256", "")
    )
    if canonical_spec_sha256(
        amendment,
        identity_field="canonical_spec_identity_sha256",
    ) != expected_canonical:
        raise ValueError("v1.3 execution amendment canonical hash mismatch")

    predecessor_identity = amendment.get("historical_v1_2_predecessor") or {}
    predecessor = _load_canonical(
        predecessor_identity,
        label="historical v1.2 predecessor",
        identity_field="canonical_spec_identity_sha256",
    )
    if predecessor.get("schema_version") != PREDECESSOR_SCHEMA_VERSION:
        raise ValueError("unexpected historical v1.2 predecessor schema")

    source_specs = amendment.get("frozen_v1_registrations") or {}
    if set(source_specs) != {"BUY", "SELL"}:
        raise ValueError("v1.3 must bind both frozen side registrations")
    action_projection: dict[str, Any] = {}
    historical_profiles: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        identity = source_specs[side]
        spec = _load_canonical(
            identity,
            label=f"{side} frozen v1 registration",
            identity_field="canonical_spec_identity_sha256",
        )
        if spec.get("side") != side:
            raise ValueError(f"{side} source registration side drifted")
        action_projection[side] = _action_projection(spec)
        historical_profiles[side] = spec["scorecard_profile"]
    if action_projection != amendment.get("unchanged_action_contract_projection"):
        raise ValueError("v1.3 action, threshold, assignment, or baseline changed")
    predecessor_projection = predecessor.get("unchanged_v1_contract_projection") or {}
    for side in ("BUY", "SELL"):
        predecessor_action = dict(predecessor_projection[side])
        predecessor_action.pop("scorecard_profile", None)
        if predecessor_action != action_projection[side]:
            raise ValueError(f"{side} v1.3 action differs from v1.2")
        if predecessor_projection[side]["scorecard_profile"] != historical_profiles[side]:
            raise ValueError(f"{side} historical scorecard projection drifted")
    if historical_profiles != amendment.get("historical_scorecard_profiles"):
        raise ValueError("v1.3 does not preserve historical side profile identity")

    successor = amendment.get("scorecard_successor") or {}
    expected_profile = score_profile_contract(PROFILE_ID)
    if successor.get("profile") != expected_profile:
        raise ValueError("v1.3 continuous-path profile contract drifted")
    registry = _load_canonical(
        successor.get("profile_registry") or {},
        label="continuous-path profile registry",
        identity_field="canonical_spec_identity_sha256",
    )
    if registry.get("status") != (
        "frozen_before_successor_mechanics_and_economic_outcome_read"
    ):
        raise ValueError("continuous-path profiles were not frozen before outcomes")
    if registry["profiles"][PROFILE_ID]["profile_sha256"] != (
        expected_profile["profile_sha256"]
    ):
        raise ValueError("profile registry and scorer contract disagree")

    outcome = _load_canonical(
        successor.get("outcome_contract") or {},
        label="continuous-path outcome contract",
        identity_field="canonical_contract_identity_sha256",
    )
    if outcome.get("schema_version") != OUTCOME_SCHEMA_VERSION:
        raise ValueError("unexpected successor outcome contract schema")
    if outcome.get("scorecard_profile") != expected_profile:
        raise ValueError("outcome contract does not bind selective v3")
    if outcome["continuous_panel_value"]["UTC_day_role"] != (
        "bootstrap_and_cluster_unit_only"
    ):
        raise ValueError("outcome contract gives UTC midnight strategy meaning")
    for key in (
        "forced_day_end_liquidations",
        "day_end_state_resets",
        "day_end_campaign_terminals",
    ):
        if outcome["continuous_panel_value"].get(key) != 0:
            raise ValueError(f"outcome contract permits {key}")
    if outcome["continuous_panel_value"].get(
        "panel_final_inventory_mtm_required"
    ) is not True:
        raise ValueError("successor omits final panel inventory MTM")

    change = amendment.get("change_boundary") or {}
    for key in (
        "action_semantics_changed",
        "threshold_changed",
        "randomization_changed",
        "baseline_changed",
        "mechanics_gate_changed",
    ):
        if change.get(key) is not False:
            raise ValueError(f"v1.3 unexpectedly changed {key}")
    if change.get("future_scorecard_changed") is not True:
        raise ValueError("v1.3 did not declare the scorecard successor")
    if change.get("historical_results_reinterpreted") is not False:
        raise ValueError("v1.3 cannot reinterpret frozen historical results")

    for label, identity in amendment.get("implementation_identity", {}).items():
        _require_identity(identity, label)
    for label, identity in amendment.get("documentation_identity", {}).items():
        _require_identity(identity, label)

    permissions = amendment.get("permissions") or {}
    if permissions.get("mechanics_execution_eligible") is not True:
        raise ValueError("v1.3 did not retain mechanics eligibility")
    for forbidden in (
        "mechanics_read",
        "development_economic_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"execution-only v1.3 cannot grant {forbidden}")
    if amendment.get("economic_outcome_columns_read") != []:
        raise ValueError("execution-only v1.3 read economic outcomes")

    test_contract = amendment.get("contract_tests") or {}
    if test_contract.get("passed") is not True:
        raise ValueError("v1.3 contract tests are not frozen as passed")
    if test_contract.get("full_repository_suite_passed") is not True:
        raise ValueError("v1.3 full repository suite is not frozen as passed")

    return {
        "schema_version": f"{SCHEMA_VERSION}.audit",
        "canonical_spec_identity_sha256": expected_canonical,
        "historical_v1_2_bytes_valid": True,
        "frozen_v1_action_contract_valid": True,
        "historical_scorecard_profiles_preserved": True,
        "continuous_path_profile_contract": expected_profile,
        "continuous_path_outcome_contract_valid": True,
        "implementation_hashes_valid": True,
        "stage": "v1_3_continuous_path_scorecard_hash_bound_mechanics_not_run",
        "economic_outcome_columns_read": [],
        "permissions": permissions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--amendment", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_execution_amendment_v1_3(args.amendment.resolve())
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
