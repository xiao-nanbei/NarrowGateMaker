#!/usr/bin/env python3
"""Run carryover-safe ranked-toxicity mechanics on the frozen 40-day panel."""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from research.families.f09_campaign_action_uplift.audit import (
    ranked_toxicity_guard_full_path_mechanics as legacy,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v1_5 import (
    RankedToxicityThresholdUnreadyReplayV15,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v2 import (
    RankedToxicityGuardAuthoritativeReplayV2,
)

SCHEMA_VERSION = "ranked_toxicity_guard_full_path_mechanics.v2"
SIDES = ("BUY", "SELL")


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported carryover-safe mechanics spec")
    if legacy.canonical_spec_sha256(spec) != spec.get(
        "canonical_spec_identity_sha256"
    ):
        raise ValueError("carryover-safe mechanics canonical spec hash mismatch")
    if legacy.sha256_file(Path(__file__).resolve()) != spec.get(
        "implementation_sha256"
    ):
        raise ValueError("carryover-safe mechanics implementation hash mismatch")
    days = [str(day) for day in spec["panels"]["development_days"]]
    if days != sorted(set(days)) or len(days) != 40:
        raise ValueError("Development panel must contain 40 chronological days")
    grade_a = set(map(str, spec["panels"]["grade_a_days"]))
    grade_b = set(map(str, spec["panels"]["grade_b_days"]))
    if grade_a & grade_b or grade_a | grade_b != set(days):
        raise ValueError("Grade partition does not cover Development")
    permissions = dict(spec.get("permissions") or {})
    if permissions.get("mechanics_execution_allowed") is not True:
        raise ValueError("mechanics execution is not authorized by the spec")
    for forbidden in (
        "economic_outcomes_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"mechanics spec cannot grant {forbidden}")
    if spec.get("economic_outcome_columns_read") != []:
        raise ValueError("mechanics spec names economic outcome columns")
    if spec["assignment_contract"]["forced_cancel_for_washout"] != "forbidden":
        raise ValueError("assignment washout cannot force order cancellation")
    for label, identity in spec.get("artifact_identities", {}).items():
        legacy._require_identity(identity, label)
    for label, identity in spec.get("implementation_identities", {}).items():
        legacy._require_identity(identity, label)
    feature_dir = legacy._data_path(spec["data_identity"]["feature_dir"])
    manifest = feature_dir / "causal_feature_manifest.json"
    if legacy.sha256_file(manifest) != str(
        spec["data_identity"]["feature_manifest_sha256"]
    ):
        raise ValueError("40-day feature manifest SHA256 mismatch")
    for day in days:
        if not (feature_dir / f"features_{day}.parquet").is_file():
            raise FileNotFoundError(f"missing Development feature day {day}")
    return spec


def _day_cache_root(spec: Mapping[str, Any], day: str) -> Path:
    return (
        Path(str(spec["storage"]["cache_root"])).expanduser().resolve()
        / str(spec["canonical_spec_identity_sha256"])
        / str(day)
    )


def _run_candidate_day(payload: Mapping[str, Any]) -> dict[str, Any]:
    spec = dict(payload["spec"])
    day = str(payload["day"])
    schedule = payload["schedule"]
    baseline_manifest = Path(str(payload["baseline_manifest"])).resolve()
    root = _day_cache_root(spec, day)
    descriptor_path = root / "candidate_v2_descriptor.json"
    if descriptor_path.is_file():
        descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        identities = descriptor.get("journal_manifest_identities") or {}
        expected_ready = bool(all(day in schedule[side] for side in SIDES))
        manifests_complete = (
            set(identities) == set(SIDES) if expected_ready else not identities
        )
        manifests_valid = all(
            Path(identity["path"]).is_file()
            and legacy.sha256_file(Path(identity["path"])) == identity["sha256"]
            for identity in identities.values()
        )
        if (
            descriptor.get("spec_sha256")
            == spec["canonical_spec_identity_sha256"]
            and bool(descriptor.get("threshold_ready")) == expected_ready
            and manifests_complete
            and manifests_valid
        ):
            return descriptor
        raise RuntimeError(f"stale carryover candidate descriptor for {day}")

    started = time.monotonic()
    ready = all(day in schedule[side] for side in SIDES)
    if ready:
        output_root = root / "candidate_v2"
        if output_root.exists():
            raise RuntimeError(f"unadmitted candidate output exists: {output_root}")
        binding = RankedToxicityGuardAuthoritativeReplayV2(
            baseline_manifest_path=baseline_manifest,
            output_root=output_root,
            frozen_model_sha256=str(spec["model_identity"]["bundle_meta_sha256"]),
            threshold_schedule={
                side: {
                    key: (float(value[0]), str(value[1]))
                    for key, value in schedule[side].items()
                }
                for side in SIDES
            },
            sides=SIDES,
            chunk_rows=int(spec["replay_contract"]["journal_chunk_rows"]),
        )
    else:
        output_root = None
        binding = RankedToxicityThresholdUnreadyReplayV15(
            baseline_manifest_path=baseline_manifest
        )
    audit = legacy._simulate_with_binding(spec, day, binding)
    identities: dict[str, dict[str, str]] = {}
    if ready:
        assert output_root is not None
        for side in SIDES:
            manifest = output_root / side.lower() / "manifest.json"
            payload = (audit.get("journal_manifests") or {}).get(side)
            if (
                not manifest.is_file()
                or not isinstance(payload, Mapping)
                or not bool(payload.get("closed"))
            ):
                raise RuntimeError(
                    f"candidate journal was not atomically closed for {day} {side}"
                )
            identities[side] = {
                "path": str(manifest),
                "sha256": legacy.sha256_file(manifest),
            }
    descriptor = {
        "schema_version": f"{SCHEMA_VERSION}.candidate_day_descriptor",
        "day": day,
        "spec_sha256": spec["canonical_spec_identity_sha256"],
        "threshold_ready": bool(ready),
        "binding_audit": audit,
        "journal_manifest_identities": identities,
        "runtime_s": float(time.monotonic() - started),
        "economic_outcome_columns_read": [],
    }
    legacy._atomic_json(descriptor_path, descriptor)
    return descriptor


def _candidate_rows(descriptor: Mapping[str, Any], side: str):
    identity = (descriptor.get("journal_manifest_identities") or {}).get(side)
    if not identity:
        return iter(())
    manifest = Path(str(identity["path"]))
    if legacy.sha256_file(manifest) != str(identity["sha256"]):
        raise RuntimeError(f"candidate journal manifest drifted: {manifest}")
    return iter_chunked_parquet_journal(manifest)


def _day_side_mechanics(
    descriptor: Mapping[str, Any], side: str, grade: str
) -> dict[str, Any]:
    day = str(descriptor["day"])
    counts: defaultdict[str, int] = defaultdict(int)
    assignments: dict[str, tuple[str, float]] = {}
    activated: set[str] = set()
    changed: set[str] = set()
    changed_roles: set[str] = set()
    completed: set[str] = set()
    censored: set[str] = set()
    carryover: set[str] = set()
    for row in _candidate_rows(descriptor, side):
        event = str(row.get("event_type", ""))
        counts[event] += 1
        episode_id = str(
            row.get("assignment_episode_id", "")
            or row.get("prospective_campaign_side_id", "")
            or ""
        )
        if event == "prediction_bucket":
            counts["prediction_bucket_observed"] += 1
            if float(row["toxicity_score"]) >= float(row["threshold"]):
                counts["prediction_bucket_exceeded"] += 1
            if int(row["feature_ready_ts_ms"]) > int(
                row["prediction_observed_ts_ms"]
            ):
                counts["feature_clock_violation"] += 1
        elif event == "quote_decision":
            eligible = bool(row.get("baseline_shadow_eligible", False)) and bool(
                row.get("baseline_shadow_exposure_increasing", False)
            )
            if eligible:
                counts["eligible_decision"] += 1
                if float(row["toxicity_score"]) >= float(row["threshold"]):
                    counts["eligible_decision_exceeded"] += 1
            if int(row["feature_ready_ts_ms"]) * 1_000_000 > int(
                row["event_ts_ns"]
            ):
                counts["feature_clock_violation"] += 1
            if (
                episode_id
                and str(row.get("action", "")) == "ranked_toxicity_guard"
                and not bool(row.get("allow_exposure_submission", True))
            ):
                activated.add(episode_id)
        elif event == "assignment_episode_started":
            if not episode_id or episode_id in assignments:
                raise RuntimeError(f"duplicate/empty assignment episode {day} {side}")
            assignments[episode_id] = (
                str(row["action"]),
                float(row["behavior_propensity"]),
            )
        elif event == "assignment_episode_completed":
            completed.add(episode_id)
        elif event == "assignment_episode_censored":
            censored.add(episode_id)
        elif event == "assignment_episode_carried_over":
            carryover.add(episode_id)
        if event == "final_quote_action" and bool(
            row.get("final_quote_action_changed", False)
        ):
            if episode_id:
                changed.add(episode_id)
            role = str(row.get("role", "")).lower()
            if role in {"opener", "add"}:
                changed_roles.add(role)
        if event == "cancel_requested" and bool(row.get("guard_initiated", False)):
            counts["guard_cancel_requested"] += 1
        if (
            event == "exchange_terminal"
            and str(row.get("terminal_reason", "")) == "cancel_ack"
        ):
            counts["cancel_ack"] += 1

    audit = (descriptor["binding_audit"].get("adapters") or {}).get(side) or {}
    zero = audit.get("zero_tolerance_counts") or {}
    propensities = [value[1] for value in assignments.values()]
    weights = np.asarray([1.0 / value for value in propensities], dtype=float)
    ess = (
        float(weights.sum() ** 2 / np.square(weights).sum())
        if weights.size
        else 0.0
    )
    return {
        "day": day,
        "grade": grade,
        "side": side,
        "threshold_ready": bool(descriptor["threshold_ready"]),
        "prediction_bucket_observed": counts["prediction_bucket_observed"],
        "prediction_bucket_exceeded": counts["prediction_bucket_exceeded"],
        "eligible_decision": counts["eligible_decision"],
        "eligible_decision_exceeded": counts["eligible_decision_exceeded"],
        "assignment_episode_count": len(assignments),
        "candidate_assignment_episode_count": sum(
            action == "ranked_toxicity_guard" for action, _ in assignments.values()
        ),
        "activated_assignment_episode_count": len(activated),
        "changed_assignment_episode_count": len(changed),
        "completed_assignment_episode_count": len(completed),
        "censored_assignment_episode_count": len(censored),
        "carryover_assignment_episode_count": len(carryover),
        "opener_action_change_supported": "opener" in changed_roles,
        "add_action_change_supported": "add" in changed_roles,
        "minimum_behavior_propensity": min(propensities) if propensities else math.nan,
        "effective_sample_size": ess,
        "guard_cancel_requested": counts["guard_cancel_requested"],
        "cancel_ack": counts["cancel_ack"],
        "feature_clock_violations": counts["feature_clock_violation"],
        "post_terminal_hazard_or_cursor_reuse": int(
            zero.get("post_terminal_hazard_or_cursor_reuse", 0)
        ),
        "reducing_quote_changes": int(zero.get("reducing_quote_changes", 0)),
        "baseline_shadow_mismatches": int(
            zero.get("control_candidate_baseline_shadow_mismatch", 0)
        ),
        "cross_arm_order_ownership_count": int(
            audit.get("cross_arm_order_ownership_count", 0)
        ),
        "forced_washout_cancel_count": int(
            audit.get("forced_washout_cancel_count", 0)
        ),
        "order_owner_mismatch_count": int(
            audit.get("order_owner_mismatch_count", 0)
        ),
        "execution_complete": bool(audit.get("execution_complete", False)),
        "zero_tolerance_passed": bool(audit.get("zero_tolerance_passed", False)),
        "carryover_contract_valid": bool(
            audit.get("carryover_contract_valid", False)
        ),
    }


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else math.nan


def summarize_mechanics(
    spec: Mapping[str, Any], candidate_rows: Sequence[Mapping[str, Any]]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    grade_a = set(map(str, spec["panels"]["grade_a_days"]))
    rows: list[dict[str, Any]] = []
    for descriptor in candidate_rows:
        if not descriptor["threshold_ready"]:
            continue
        day = str(descriptor["day"])
        for side in SIDES:
            rows.append(
                _day_side_mechanics(
                    descriptor,
                    side,
                    "A" if day in grade_a else "B",
                )
            )
    daily = pd.DataFrame(rows)
    if daily.empty:
        raise RuntimeError("no threshold-ready carryover mechanics rows")
    gates_spec = spec["mechanics_hard_gates"]
    side_results: dict[str, Any] = {}
    for side in SIDES:
        frame = daily[daily["side"].eq(side)]
        sums = frame.select_dtypes(include=["number", "bool"]).sum()
        assignments = int(frame["assignment_episode_count"].sum())
        completed = int(frame["completed_assignment_episode_count"].sum())
        censored = int(frame["censored_assignment_episode_count"].sum())
        ess = float(frame["effective_sample_size"].sum())
        minimum_propensity = float(frame["minimum_behavior_propensity"].min())
        result = {
            "threshold_ready_days": int(frame["day"].nunique()),
            "grade_a_ready_days": int(frame.loc[frame["grade"].eq("A"), "day"].nunique()),
            "grade_b_ready_days": int(frame.loc[frame["grade"].eq("B"), "day"].nunique()),
            "prediction_bucket_exceedance_rate": _ratio(
                sums["prediction_bucket_exceeded"], sums["prediction_bucket_observed"]
            ),
            "eligible_decision_exceedance_rate": _ratio(
                sums["eligible_decision_exceeded"], sums["eligible_decision"]
            ),
            "assignment_episode_activation_rate": _ratio(
                sums["activated_assignment_episode_count"], assignments
            ),
            "final_quote_action_change_rate": _ratio(
                sums["changed_assignment_episode_count"], assignments
            ),
            "assignments": assignments,
            "candidate_assignments": int(
                frame["candidate_assignment_episode_count"].sum()
            ),
            "completed_assignments": completed,
            "panel_end_censored_assignments": censored,
            "completed_episode_fraction": _ratio(completed, assignments),
            "panel_end_censor_fraction": _ratio(censored, assignments),
            "carryover_assignments": int(
                frame["carryover_assignment_episode_count"].sum()
            ),
            "minimum_behavior_propensity": minimum_propensity,
            "effective_sample_size": ess,
            "ess_fraction": _ratio(ess, assignments),
            "opener_support_days": int(
                frame.loc[frame["opener_action_change_supported"], "day"].nunique()
            ),
            "add_support_days": int(
                frame.loc[frame["add_action_change_supported"], "day"].nunique()
            ),
            "guard_cancel_requests": int(frame["guard_cancel_requested"].sum()),
            "cancel_ACKs": int(frame["cancel_ack"].sum()),
            "cancel_ACK_coverage": _ratio(
                frame["cancel_ack"].sum(), frame["guard_cancel_requested"].sum()
            ),
            "feature_ready_clock_violations": int(
                frame["feature_clock_violations"].sum()
            ),
            "post_terminal_hazard_or_cursor_reuse": int(
                frame["post_terminal_hazard_or_cursor_reuse"].sum()
            ),
            "reducing_quote_changes": int(frame["reducing_quote_changes"].sum()),
            "baseline_shadow_mismatches": int(
                frame["baseline_shadow_mismatches"].sum()
            ),
            "cross_arm_order_ownership_count": int(
                frame["cross_arm_order_ownership_count"].sum()
            ),
            "forced_washout_cancel_count": int(
                frame["forced_washout_cancel_count"].sum()
            ),
            "order_owner_mismatch_count": int(
                frame["order_owner_mismatch_count"].sum()
            ),
            "execution_complete_all_days": bool(frame["execution_complete"].all()),
            "zero_tolerance_passed_all_days": bool(
                frame["zero_tolerance_passed"].all()
            ),
            "carryover_contract_valid_all_days": bool(
                frame["carryover_contract_valid"].all()
            ),
        }
        prediction_range = gates_spec["prediction_bucket_exceedance_rate_range"]
        eligible_range = gates_spec["eligible_decision_exceedance_rate_range"]
        activation_range = gates_spec["assignment_episode_activation_rate_range"]
        gates = {
            "threshold_ready_days": result["threshold_ready_days"]
            >= int(gates_spec["minimum_threshold_ready_days"]),
            "assignments": assignments >= int(gates_spec["minimum_assignments"]),
            "behavior_propensity": minimum_propensity
            >= float(gates_spec["minimum_behavior_propensity"]),
            "ESS": result["ess_fraction"]
            >= float(gates_spec["minimum_ESS_fraction_of_assignments"]),
            "prediction_bucket_exceedance": float(prediction_range[0])
            <= result["prediction_bucket_exceedance_rate"]
            <= float(prediction_range[1]),
            "eligible_decision_exceedance": float(eligible_range[0])
            <= result["eligible_decision_exceedance_rate"]
            <= float(eligible_range[1]),
            "assignment_episode_activation": float(activation_range[0])
            <= result["assignment_episode_activation_rate"]
            <= float(activation_range[1]),
            "final_quote_action_change": result["final_quote_action_change_rate"]
            >= float(gates_spec["minimum_final_quote_action_change_rate"]),
            "opener_support": result["opener_support_days"]
            >= int(gates_spec["minimum_opener_support_days"]),
            "add_support": result["add_support_days"]
            >= int(gates_spec["minimum_add_support_days"]),
            "cancel_ACK_coverage": math.isclose(
                result["cancel_ACK_coverage"],
                float(gates_spec["cancel_ACK_coverage"]),
                rel_tol=0.0,
                abs_tol=1e-12,
            ),
            "completed_episode_fraction": result["completed_episode_fraction"]
            >= float(gates_spec["minimum_completed_episode_fraction"]),
            "panel_end_censor_fraction": result["panel_end_censor_fraction"]
            <= float(gates_spec["maximum_panel_end_censor_fraction"]),
            "feature_clock": result["feature_ready_clock_violations"] == 0,
            "post_terminal_risk_set": result[
                "post_terminal_hazard_or_cursor_reuse"
            ] == 0,
            "reducing_quote_unchanged": result["reducing_quote_changes"] == 0,
            "baseline_shadow_parity": result["baseline_shadow_mismatches"] == 0,
            "cross_arm_order_ownership": result[
                "cross_arm_order_ownership_count"
            ] == 0,
            "forced_washout_cancel": result["forced_washout_cancel_count"] == 0,
            "order_owner_complete": result["order_owner_mismatch_count"] == 0,
            "execution_complete": result["execution_complete_all_days"],
            "zero_tolerance": result["zero_tolerance_passed_all_days"],
            "carryover_contract": result["carryover_contract_valid_all_days"],
        }
        result["hard_gates"] = gates
        result["mechanics_supported"] = all(gates.values())
        side_results[side] = result
    report = {
        "schema_version": f"{SCHEMA_VERSION}.development_report",
        "family_id": spec["family_id"],
        "spec_sha256": spec["canonical_spec_identity_sha256"],
        "development_days": 40,
        "threshold_unready_days": sorted(
            set(spec["panels"]["development_days"]) - set(daily["day"])
        ),
        "sides": side_results,
        "mechanics_supported": all(
            side_results[side]["mechanics_supported"] for side in SIDES
        ),
        "mechanics_read": True,
        "economic_outcome_columns_read": [],
        "development_economic_outcome_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    report["report_identity_sha256"] = legacy.canonical_sha256(report)
    return daily, report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument(
        "--stage",
        choices=("baseline", "candidate", "summarize", "all"),
        default="all",
    )
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    spec = load_spec(args.spec.expanduser().resolve())
    gate = legacy.storage_gate(spec)
    days = [str(day) for day in spec["panels"]["development_days"]]
    baseline_rows = legacy._run_parallel(
        legacy._run_baseline_day,
        [{"spec": spec, "day": day} for day in days],
        workers=max(1, int(args.workers)),
    )
    schedule, support, schedule_payload = legacy.build_and_freeze_thresholds(
        spec, baseline_rows
    )
    evidence = legacy._data_path(spec["storage"]["evidence_root"])
    legacy._atomic_json(evidence / "storage_gate.json", gate)
    legacy._atomic_json(
        evidence / "baseline_index.json",
        {
            "schema_version": f"{SCHEMA_VERSION}.baseline_index",
            "spec_sha256": spec["canonical_spec_identity_sha256"],
            "days": baseline_rows,
            "threshold_schedule_sha256": schedule_payload[
                "canonical_schedule_sha256"
            ],
            "economic_outcome_columns_read": [],
        },
    )
    if args.stage == "baseline":
        print(json.dumps({"baseline_days": len(baseline_rows)}, indent=2))
        return
    candidate_rows = legacy._run_parallel(
        _run_candidate_day,
        [
            {
                "spec": spec,
                "day": day,
                "schedule": schedule,
                "baseline_manifest": next(
                    row["manifest_path"] for row in baseline_rows if row["day"] == day
                ),
            }
            for day in days
        ],
        workers=max(1, int(args.workers)),
    )
    legacy._atomic_json(
        evidence / "candidate_index.json",
        {
            "schema_version": f"{SCHEMA_VERSION}.candidate_index",
            "spec_sha256": spec["canonical_spec_identity_sha256"],
            "days": candidate_rows,
            "economic_outcome_columns_read": [],
        },
    )
    if args.stage == "candidate":
        print(json.dumps({"candidate_days": len(candidate_rows)}, indent=2))
        return
    daily, report = summarize_mechanics(spec, candidate_rows)
    legacy._atomic_csv(evidence / "daily_mechanics.csv", daily)
    legacy._atomic_json(evidence / "report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
