#!/usr/bin/env python3
"""Regenerate baseline and candidate under one read-only execution identity."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from research.families.f09_campaign_action_uplift.audit import (
    ranked_toxicity_guard_carryover_safe_execution_v2_1 as v2_1,
)
from research.families.f09_campaign_action_uplift.audit import (
    ranked_toxicity_guard_full_path_mechanics as legacy,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v1_5 import (
    RankedToxicityBaselineShadowCaptureV15,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v2 import (
    RankedToxicityGuardAuthoritativeReplayV2,
)

SCHEMA_VERSION = "ranked_toxicity_guard_carryover_safe_execution.v2.2"
SIDES = ("BUY", "SELL")


class PlumbingGateViolation(RuntimeError):
    """Raised when the bounded baseline/candidate plumbing contract fails."""


def load_spec(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported carryover-safe v2.2 execution spec")
    if legacy.canonical_spec_sha256(spec) != spec.get(
        "canonical_spec_identity_sha256"
    ):
        raise ValueError("carryover-safe v2.2 canonical spec hash mismatch")
    if legacy.sha256_file(Path(__file__).resolve()) != spec.get(
        "implementation_sha256"
    ):
        raise ValueError("carryover-safe v2.2 implementation hash mismatch")
    for label, identity in spec.get("artifact_identities", {}).items():
        legacy._require_identity(identity, label)
    for label, identity in spec.get("implementation_identities", {}).items():
        legacy._require_identity(identity, label)
    predecessor = v2_1._load_predecessor(spec)
    v2_1._validate_cache_contract(spec)
    if float(spec["plumbing_threshold"]["value"]) != 0.8:
        raise ValueError("v2.2 plumbing smoke must reuse the frozen 0.8 threshold")
    if spec["plumbing_threshold"]["authority"] != "plumbing_only":
        raise ValueError("v2.2 threshold cannot carry prediction authority")
    if "expected_v2_counts" in spec.get("smoke_contract", {}):
        raise ValueError("v2.2 must not gate on old v2 episode counts")
    permissions = spec.get("permissions") or {}
    if permissions.get("one_day_plumbing_smoke_allowed") is not True:
        raise ValueError("v2.2 one-day plumbing smoke is not authorized")
    for forbidden in (
        "formal_40_day_mechanics_run",
        "mechanics_results_read_beyond_contract_counters",
        "economic_outcomes_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden)):
            raise ValueError(f"v2.2 cannot grant {forbidden}")
    return spec, predecessor


def _identity_snapshot(spec: Mapping[str, Any]) -> dict[str, str]:
    return {
        label: legacy.sha256_file(legacy._repo_path(identity["path"]))
        for label, identity in sorted(spec["implementation_identities"].items())
    }


def extract_contract_counters(audit: Mapping[str, Any]) -> dict[str, Any]:
    baseline = audit["baseline_shadow"]
    counters: dict[str, Any] = {
        "baseline": {
            "rows": int(baseline["rows"]),
            "consumed": int(baseline["consumed"]),
            "unconsumed": int(baseline["unconsumed"]),
            "complete": bool(baseline["complete"]),
        }
    }
    for side in SIDES:
        side_audit = audit["adapters"][side]
        zero_counts = side_audit["zero_tolerance_counts"]
        counters[side] = {
            "baseline_shadow_mismatch_count": int(
                zero_counts["control_candidate_baseline_shadow_mismatch"]
            ),
            "cross_arm_order_ownership_count": int(
                side_audit["cross_arm_order_ownership_count"]
            ),
            "forced_washout_cancel_count": int(
                side_audit["forced_washout_cancel_count"]
            ),
            "order_owner_mismatch_count": int(
                side_audit["order_owner_mismatch_count"]
            ),
            "active_order_role_transition_to_exposure_count": int(
                side_audit["active_order_role_transition_to_exposure_count"]
            ),
            "carryover_transition_count": int(
                side_audit["carryover_transition_count"]
            ),
            "role_lifecycle_valid": bool(
                side_audit["zero_tolerance_passed"]
                and side_audit["execution_complete"]
            ),
            "carryover_lifecycle_valid": bool(
                side_audit["carryover_contract_valid"]
            ),
        }
    return counters


def evaluate_contract_gates(counters: Mapping[str, Any]) -> dict[str, bool]:
    baseline = counters["baseline"]
    gates: dict[str, bool] = {
        "complete_current_baseline_consumption": bool(
            baseline["complete"]
            and int(baseline["unconsumed"]) == 0
            and int(baseline["rows"]) == int(baseline["consumed"])
        )
    }
    for side in SIDES:
        values = counters[side]
        gates[f"{side}_baseline_shadow_mismatch_zero"] = (
            int(values["baseline_shadow_mismatch_count"]) == 0
        )
        gates[f"{side}_cross_arm_ownership_zero"] = (
            int(values["cross_arm_order_ownership_count"]) == 0
        )
        gates[f"{side}_forced_washout_zero"] = (
            int(values["forced_washout_cancel_count"]) == 0
        )
        gates[f"{side}_owner_mismatch_zero"] = (
            int(values["order_owner_mismatch_count"]) == 0
        )
        gates[f"{side}_role_lifecycle_valid"] = bool(
            values["role_lifecycle_valid"]
        )
        gates[f"{side}_carryover_lifecycle_valid"] = bool(
            values["carryover_lifecycle_valid"]
        )
    return gates


def _manifest_identity(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not bool(payload.get("closed")):
        raise PlumbingGateViolation(f"journal did not close atomically: {path}")
    return {
        "path": str(path),
        "sha256": legacy.sha256_file(path),
        "closed": True,
    }


def _write_failure(
    report_path: Path,
    *,
    spec: Mapping[str, Any],
    stage: str,
    exc: BaseException,
) -> None:
    payload = {
        "schema_version": f"{SCHEMA_VERSION}.smoke_failure",
        "family_id": spec["family_id"],
        "status": "fail_closed",
        "failure_stage": str(stage),
        "exception_type": type(exc).__name__,
        "exception_message": str(exc),
        "contract_counters_read": [],
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "formal_40_day_mechanics_run": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    payload["report_identity_sha256"] = legacy.canonical_sha256(payload)
    legacy._atomic_json(report_path, payload)


def run_smoke(
    spec_path: Path, *, output_root: Path, report_path: Path
) -> dict[str, Any]:
    spec, predecessor = load_spec(spec_path.resolve())
    day = str(spec["smoke_contract"]["utc_day"])
    output_root = output_root.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    if output_root.exists():
        raise FileExistsError(f"v2.2 output already exists: {output_root}")
    expected_output = Path(spec["smoke_contract"]["output_root"]).resolve()
    expected_report = Path(spec["smoke_contract"]["report_path"]).resolve()
    if output_root != expected_output or report_path != expected_report:
        raise ValueError("v2.2 output identity differs from the frozen spec")

    stage = "baseline_generation"
    try:
        initial_identity = _identity_snapshot(spec)
        baseline_root = output_root / "baseline_current_identity"
        baseline_binding = RankedToxicityBaselineShadowCaptureV15(
            output_dir=baseline_root,
            lineage_namespace=f"{spec['family_id']}|{day}",
            sides=SIDES,
            chunk_rows=int(predecessor["replay_contract"]["journal_chunk_rows"]),
        )
        baseline_audit, baseline_cache_audit = v2_1._simulate_read_only(
            spec, predecessor, day, baseline_binding
        )
        baseline_manifest = baseline_root / "manifest.json"
        baseline_manifest_identity = _manifest_identity(baseline_manifest)
        if int(baseline_audit["baseline_shadow_rows"]) <= 0:
            raise PlumbingGateViolation("current baseline shadow is empty")

        stage = "between_pass_identity_check"
        if _identity_snapshot(spec) != initial_identity:
            raise PlumbingGateViolation(
                "implementation identity changed between baseline and candidate"
            )

        stage = "candidate_generation"
        threshold = float(spec["plumbing_threshold"]["value"])
        threshold_source = str(spec["plumbing_threshold"]["source_sha256"])
        candidate_root = output_root / "candidate_current_identity"
        candidate_binding = RankedToxicityGuardAuthoritativeReplayV2(
            baseline_manifest_path=baseline_manifest,
            output_root=candidate_root,
            frozen_model_sha256=str(
                predecessor["model_identity"]["bundle_meta_sha256"]
            ),
            threshold_schedule={
                side: {day: (threshold, threshold_source)} for side in SIDES
            },
            sides=SIDES,
            chunk_rows=int(predecessor["replay_contract"]["journal_chunk_rows"]),
        )
        candidate_audit, candidate_cache_audit = v2_1._simulate_read_only(
            spec, predecessor, day, candidate_binding
        )

        stage = "post_pass_identity_and_cache_check"
        if _identity_snapshot(spec) != initial_identity:
            raise PlumbingGateViolation(
                "implementation identity changed during candidate replay"
            )
        cache_read_identity_equal = bool(
            baseline_cache_audit["legacy_v13_read_paths"]
            == candidate_cache_audit["legacy_v13_read_paths"]
            and baseline_cache_audit["legacy_component_v1_read_paths"]
            == candidate_cache_audit["legacy_component_v1_read_paths"]
            and baseline_cache_audit["component_v2_read_hits"]
            == candidate_cache_audit["component_v2_read_hits"]
            and baseline_cache_audit["native_book_cache_stats"]
            == candidate_cache_audit["native_book_cache_stats"]
        )
        if not cache_read_identity_equal:
            raise PlumbingGateViolation(
                "baseline and candidate used different cache-read identities"
            )

        stage = "contract_counter_gate"
        counters = extract_contract_counters(candidate_audit)
        gates = evaluate_contract_gates(counters)
        gates["same_current_implementation_identity"] = True
        gates["same_read_only_cache_identity"] = True
        if not all(gates.values()):
            raise PlumbingGateViolation(
                "v2.2 contract counters failed: "
                + json.dumps(gates, sort_keys=True, separators=(",", ":"))
            )

        manifests = {
            "baseline": baseline_manifest_identity,
            "candidate": {
                side: _manifest_identity(
                    candidate_root / side.lower() / "manifest.json"
                )
                for side in SIDES
            },
        }
        report = {
            "schema_version": f"{SCHEMA_VERSION}.smoke_report",
            "family_id": spec["family_id"],
            "status": "one_day_plumbing_smoke_passed",
            "utc_day": day,
            "spec_path": str(spec_path.resolve()),
            "spec_canonical_sha256": spec["canonical_spec_identity_sha256"],
            "threshold": {
                "value": threshold,
                "authority": "plumbing_only",
            },
            "contract_counters": counters,
            "hard_gates": gates,
            "all_hard_gates_passed": True,
            "cache_contract": {
                "writes_enabled": False,
                "write_attempt_count": 0,
                "baseline_cache_tree_unchanged": bool(
                    baseline_cache_audit["window_cache_tree_unchanged"]
                    and baseline_cache_audit["native_book_cache_tree_unchanged"]
                ),
                "candidate_cache_tree_unchanged": bool(
                    candidate_cache_audit["window_cache_tree_unchanged"]
                    and candidate_cache_audit["native_book_cache_tree_unchanged"]
                ),
                "same_read_identity": cache_read_identity_equal,
                "legacy_v13_read_compatible": True,
                "WindowData_materialization": "ephemeral",
            },
            "journal_manifests": manifests,
            "old_v2_episode_counts_used_as_gate": False,
            "mechanics_results_read_beyond_contract_counters": False,
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "formal_40_day_mechanics_run": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        }
        report["report_identity_sha256"] = legacy.canonical_sha256(report)
        legacy._atomic_json(report_path, report)
        return report
    except Exception as exc:
        _write_failure(report_path, spec=spec, stage=stage, exc=exc)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = run_smoke(
        args.spec,
        output_root=args.output_root,
        report_path=args.report,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
