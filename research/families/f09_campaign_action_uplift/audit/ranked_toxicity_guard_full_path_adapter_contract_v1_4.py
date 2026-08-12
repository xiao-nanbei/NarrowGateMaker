#!/usr/bin/env python3
"""Validate the authoritative replay successor for ranked toxicity v1.4."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from execution.chunked_parquet_journal import (
    CHUNKED_PARQUET_JOURNAL_SCHEMA_VERSION,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4 import (
    SCHEMA_VERSION as ADAPTER_SCHEMA_VERSION,
)
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4 import (
    execution_binding_contract_v1_4,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay import (
    SCHEMA_VERSION as REPLAY_BINDING_SCHEMA_VERSION,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay import (
    authoritative_replay_binding_contract_v1_4,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "ranked_toxicity_guard_full_path_execution_amendment.v1.4"
PREDECESSOR_SCHEMA_VERSION = (
    "ranked_toxicity_guard_full_path_execution_amendment.v1.3"
)
FROZEN_V1_1_ADAPTER_SHA256 = (
    "5cc1ded739ea6a186026a5759537f8ba306235bb96c008d9b48abc62659ae044"
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
    ).encode()
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
    if canonical_spec_sha256(payload, identity_field=identity_field) != expected:
        raise ValueError(f"{label} canonical content drifted")
    return payload


def validate_execution_amendment_v1_4(amendment_path: Path) -> dict[str, Any]:
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    if amendment.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected v1.4 execution-amendment schema")
    if amendment.get("adapter_schema_version") != ADAPTER_SCHEMA_VERSION:
        raise ValueError("v1.4 adapter schema identity drifted")
    if amendment.get("authoritative_replay_binding_schema_version") != (
        REPLAY_BINDING_SCHEMA_VERSION
    ):
        raise ValueError("v1.4 authoritative replay binding identity drifted")
    expected_canonical = str(
        amendment.get("canonical_spec_identity_sha256", "")
    )
    if canonical_spec_sha256(
        amendment,
        identity_field="canonical_spec_identity_sha256",
    ) != expected_canonical:
        raise ValueError("v1.4 execution amendment canonical hash mismatch")

    predecessor = _load_canonical(
        amendment.get("historical_v1_3_predecessor") or {},
        label="historical v1.3 predecessor",
        identity_field="canonical_spec_identity_sha256",
    )
    if predecessor.get("schema_version") != PREDECESSOR_SCHEMA_VERSION:
        raise ValueError("unexpected historical v1.3 predecessor schema")
    if amendment.get("unchanged_action_contract_projection") != predecessor.get(
        "unchanged_action_contract_projection"
    ):
        raise ValueError("v1.4 action, threshold, or behavior policy drifted")
    if amendment.get("scorecard_successor") != predecessor.get(
        "scorecard_successor"
    ):
        raise ValueError("v1.4 scorecard or outcome accounting drifted")

    contracts = amendment.get("execution_contracts") or {}
    if contracts.get("adapter") != execution_binding_contract_v1_4():
        raise ValueError("v1.4 prediction/decision adapter contract drifted")
    if contracts.get("authoritative_replay") != (
        authoritative_replay_binding_contract_v1_4()
    ):
        raise ValueError("v1.4 authoritative replay contract drifted")
    if contracts.get("journal_schema_version") != (
        CHUNKED_PARQUET_JOURNAL_SCHEMA_VERSION
    ):
        raise ValueError("v1.4 chunked journal schema drifted")

    required_execution = amendment.get("required_execution_invariants") or {}
    expected_execution = {
        "prediction_bucket_once_per_completed_10s_bucket": True,
        "quote_decision_for_every_authoritative_loop": True,
        "held_score_reuse_within_bucket_legal": True,
        "candidate_path_role_controls_permission": True,
        "duplicate_prediction_bucket_fail_fast": True,
        "assignment_prf_checkpoint_stable": True,
        "assignment_boundary_uses_untreated_lineage_only": True,
        "candidate_campaign_terminal_never_rerandomizes": True,
        "unknown_terminal_reason_fail_fast": True,
        "baseline_shadow_exact_denominator_two_pass": True,
        "baseline_index_exactly_one_UTC_day": True,
        "journal_streamed_to_local_atomic_parquet": True,
        "formal_adapter_journal_retained_in_memory": False,
        "cpp_full_path_authority": False,
    }
    if required_execution != expected_execution:
        raise ValueError("v1.4 required execution invariants drifted")

    change = amendment.get("change_boundary") or {}
    for key in (
        "action_semantics_changed",
        "threshold_changed",
        "behavior_probability_changed",
        "random_seed_changed",
        "assignment_unit_changed",
        "baseline_changed",
        "mechanics_gate_changed",
        "scorecard_changed",
        "outcome_accounting_changed",
        "historical_results_reinterpreted",
        "outcome_informed",
    ):
        if change.get(key) is not False:
            raise ValueError(f"v1.4 unexpectedly changed {key}")
    for key in (
        "assignment_prf_input_corrected",
        "prediction_decision_interfaces_separated",
        "authoritative_replay_binding_added",
        "unknown_terminal_fail_fast_added",
        "bounded_journal_streaming_added",
    ):
        if change.get(key) is not True:
            raise ValueError(f"v1.4 did not declare {key}")

    identities = amendment.get("implementation_identity") or {}
    frozen_adapter = identities.get("frozen_v1_1_adapter") or {}
    if frozen_adapter.get("sha256") != FROZEN_V1_1_ADAPTER_SHA256:
        raise ValueError("frozen v1.1 adapter identity was rewritten")
    for label, identity in identities.items():
        _require_identity(identity, label)
    for label, identity in (amendment.get("documentation_identity") or {}).items():
        _require_identity(identity, label)

    permissions = amendment.get("permissions") or {}
    if permissions.get("mechanics_execution_eligible") is not True:
        raise ValueError("v1.4 mechanics execution was not enabled")
    for forbidden in (
        "mechanics_read",
        "development_economic_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "prediction_authority",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"execution-only v1.4 cannot grant {forbidden}")
    if amendment.get("economic_outcome_columns_read") != []:
        raise ValueError("execution-only v1.4 read economic outcomes")

    read_boundary = amendment.get("read_boundary") or {}
    if read_boundary.get("full_path_mechanics_results_read") is not False:
        raise ValueError("v1.4 mechanics results were read before execution")
    if read_boundary.get("forty_day_development_run_executed") is not False:
        raise ValueError("v1.4 falsely claims a 40-day mechanics run")
    if read_boundary.get("economics_read") is not False:
        raise ValueError("v1.4 read economics")

    tests = amendment.get("contract_tests") or {}
    if tests.get("passed") is not True:
        raise ValueError("v1.4 contract tests are not frozen as passed")
    if tests.get("full_repository_suite_passed") is not True:
        raise ValueError("v1.4 full repository suite is not frozen as passed")

    return {
        "schema_version": f"{SCHEMA_VERSION}.audit",
        "canonical_spec_identity_sha256": expected_canonical,
        "historical_v1_3_bytes_valid": True,
        "frozen_v1_1_adapter_bytes_valid": True,
        "unchanged_action_contract_valid": True,
        "unchanged_scorecard_contract_valid": True,
        "prediction_decision_interfaces_separated": True,
        "candidate_role_permission_separation_valid": True,
        "stable_untreated_lineage_assignment_valid": True,
        "authoritative_two_pass_replay_bound": True,
        "single_day_bounded_journal_contract_valid": True,
        "implementation_hashes_valid": True,
        "stage": "v1_4_authoritative_replay_hash_bound_mechanics_not_run",
        "economic_outcome_columns_read": [],
        "permissions": permissions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--amendment", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate_execution_amendment_v1_4(args.amendment.resolve())
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
