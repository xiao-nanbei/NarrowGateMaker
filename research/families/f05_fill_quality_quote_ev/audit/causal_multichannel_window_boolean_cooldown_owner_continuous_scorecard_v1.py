#!/usr/bin/env python3
"""Build an isolated, fail-closed audit of F05 owner continuous evidence.

The builder consumes only admitted post-run and aggregate evidence. It never
reads execution plans, receipts, checkpoints, market data, or policy outcomes
outside the five explicitly supplied JSON files. Partial continuous evidence is
reported as historical economics; it is never promoted into a fabricated
``action_defense_v2`` scorecard pass.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from models.audit import experiment_scorecard_v2

IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_continuous_scorecard_audit_v1"
SCHEMA_VERSION = f"{IDENTITY}.v1"
PROFILE_ID = "action_defense_v2"

POSTRUN_IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_owner_restart_aware_postrun_audit_v1"
)
POSTRUN_SCHEMA_VERSION = f"{POSTRUN_IDENTITY}.v1"
CONTINUOUS_IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_restart_aware_execution_v1"
CONTINUOUS_REPORT_SCHEMA_VERSION = f"{CONTINUOUS_IDENTITY}.v1.report"
PAIRED_DAILY_SCHEMA_VERSION = f"{CONTINUOUS_IDENTITY}.v1.paired_daily"
OWNER_DAILY_IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_daily_50d_decision_v1"
OWNER_DAILY_SCHEMA_VERSION = OWNER_DAILY_IDENTITY
OOF_RESEARCH_IDENTITY = "causal_multichannel_window_boolean_cooldown_persistent_policy_v3"

PERMISSION_FIELDS = (
    "strict_queue_authority",
    "receive_time_transport_authority",
    "research_supported",
    "action_authorized",
    "live_authorized",
)
ACCOUNTING_TOLERANCE_USDC = 1e-6


class OwnerContinuousScorecardError(RuntimeError):
    """Raised when aggregate evidence is malformed or internally inconsistent."""


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise OwnerContinuousScorecardError(message)


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    _require(resolved.is_file(), f"missing {role}: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OwnerContinuousScorecardError(f"invalid {role}: {resolved}") from exc
    _require(isinstance(payload, dict), f"{role} must be a JSON object")
    return payload


def _artifact(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    _require(resolved.is_file(), f"missing {role}: {resolved}")
    return {
        "role": role,
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _finite(value: Any, *, role: str) -> float:
    _require(
        isinstance(value, (int, float)) and not isinstance(value, bool),
        f"{role} must be numeric",
    )
    number = float(value)
    _require(math.isfinite(number), f"{role} must be finite")
    return number


def _integer(value: Any, *, role: str) -> int:
    number = _finite(value, role=role)
    _require(number.is_integer(), f"{role} must be an integer")
    return int(number)


def _mapping(value: Any, *, role: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{role} must be an object")
    return value


def _require_false_permissions(payload: Mapping[str, Any], *, role: str) -> None:
    for field in PERMISSION_FIELDS:
        _require(payload.get(field) is False, f"{role} granted {field}")


def _require_owner_daily_false_permissions(payload: Mapping[str, Any]) -> None:
    """Accept the frozen owner's legacy strict-queue field without weakening it."""
    canonical = payload.get("strict_queue_authority")
    legacy = payload.get("strict_native_queue_authority")
    if canonical is None:
        _require(
            legacy is False,
            "owner 50-day decision did not explicitly deny strict queue authority",
        )
    else:
        _require(canonical is False, "owner 50-day decision granted strict_queue_authority")
        if legacy is not None:
            _require(
                legacy is False,
                "owner 50-day decision granted strict_native_queue_authority",
            )

    for field in PERMISSION_FIELDS[1:]:
        _require(payload.get(field) is False, f"owner 50-day decision granted {field}")
    if "continuous_replay_authority" in payload:
        _require(
            payload.get("continuous_replay_authority") is False,
            "owner 50-day decision granted continuous_replay_authority",
        )


def _ci95(value: Any, *, role: str) -> list[float]:
    _require(
        isinstance(value, Sequence) and not isinstance(value, (str, bytes)),
        f"{role} must be a two-value interval",
    )
    _require(len(value) == 2, f"{role} must contain two values")
    lower = _finite(value[0], role=f"{role} lower")
    upper = _finite(value[1], role=f"{role} upper")
    _require(lower <= upper, f"{role} is reversed")
    return [lower, upper]


def _close(left: float, right: float, *, role: str) -> None:
    _require(
        math.isclose(
            left,
            right,
            rel_tol=0.0,
            abs_tol=ACCOUNTING_TOLERANCE_USDC,
        ),
        f"{role} drifted: {left} != {right}",
    )


def _validate_postrun(payload: Mapping[str, Any]) -> dict[str, Any]:
    _require(
        payload.get("schema_version") == POSTRUN_SCHEMA_VERSION,
        "postrun audit schema drifted",
    )
    _require(payload.get("identity") == POSTRUN_IDENTITY, "postrun audit identity drifted")
    _require(payload.get("audit_passed") is True, "postrun strong audit did not pass")
    _require(payload.get("read_only") is True, "postrun audit was not read-only")
    _require(
        payload.get("economic_effect_estimate_computed") is False,
        "postrun audit unexpectedly computed economics",
    )
    permissions = _mapping(payload.get("permissions"), role="postrun permissions")
    _require_false_permissions(permissions, role="postrun audit")
    execution = _mapping(payload.get("execution"), role="postrun execution")
    _require(
        execution.get("same_random_path_all_epochs") is True,
        "postrun audit lost the paired random path",
    )
    _require(
        execution.get("quote_stop_and_zero_remaining_orders_all_epochs") is True,
        "postrun audit did not prove quote-stop drain",
    )
    _require(
        execution.get("warmup_past_only_all_epochs") is True,
        "postrun audit did not prove past-only warmup",
    )
    _require(
        _integer(
            execution.get("strict_queue_authority_claim_count"),
            role="strict queue claim count",
        )
        == 0,
        "postrun audit contains strict queue authority claims",
    )
    _require(
        _integer(
            execution.get("receive_time_transport_authority_claim_count"),
            role="receive-time claim count",
        )
        == 0,
        "postrun audit contains receive-time authority claims",
    )
    accounting = _mapping(payload.get("accounting"), role="postrun accounting")
    _require(accounting.get("paired_dates_identical") is True, "paired dates differ")
    _require(accounting.get("calendar_continuous") is True, "calendar is discontinuous")
    tolerance = _finite(accounting.get("tolerance_usdc"), role="accounting tolerance")
    residual = _finite(
        accounting.get("maximum_abs_reconciliation_error_usdc"),
        role="maximum accounting residual",
    )
    _require(residual <= tolerance, "postrun accounting residual exceeds tolerance")
    plan = _mapping(payload.get("plan"), role="postrun plan binding")
    adapter_identity = str(plan.get("adapter_plan_identity_sha256", ""))
    _require(len(adapter_identity) == 64, "postrun audit lacks adapter identity")
    return {
        "epoch_count": _integer(execution.get("receipt_count"), role="receipt count"),
        "utc_day_count": _integer(accounting.get("utc_day_count"), role="UTC day count"),
        "adapter_plan_identity_sha256": adapter_identity,
        "maximum_abs_reconciliation_error_usdc": residual,
        "candidate_policy_audit": dict(
            _mapping(
                payload.get("candidate_policy_audit"),
                role="candidate policy audit",
            )
        ),
        "runtime_modes": dict(_mapping(payload.get("runtime_modes"), role="runtime modes")),
    }


def _validate_owner_decision(payload: Mapping[str, Any]) -> dict[str, Any]:
    _require(
        payload.get("schema_version") == OWNER_DAILY_SCHEMA_VERSION,
        "owner 50-day decision schema drifted",
    )
    _require(
        payload.get("identity") == OWNER_DAILY_IDENTITY,
        "owner 50-day decision identity drifted",
    )
    _require(
        payload.get("evidence_route") == "owner_risk_accepted_outcome_informed_successor",
        "owner 50-day evidence route drifted",
    )
    panel = _mapping(payload.get("panel"), role="owner 50-day panel")
    _require(
        _integer(panel.get("days"), role="owner panel days") == 50, "owner panel is not 50 days"
    )
    hard_gate = _mapping(payload.get("hard_gate"), role="owner daily hard gate")
    _require(hard_gate.get("passed") is False, "owner daily hard gate was not a failure")
    owner = _mapping(payload.get("owner_decision"), role="owner continuation decision")
    _require(owner.get("daily_hard_gate_passed") is False, "owner decision rewrote daily failure")
    _require(
        owner.get("advance_to_restart_aware_continuous_confirmation") is True,
        "owner decision did not authorize continuous confirmation",
    )
    _require(owner.get("outcome_informed") is True, "owner route lost its outcome-informed label")
    permissions = _mapping(payload.get("permissions"), role="owner 50-day permissions")
    _require_owner_daily_false_permissions(permissions)
    return {
        "days": 50,
        "hard_gate_passed": False,
        "continuous_confirmation_authorized": True,
        "outcome_informed": True,
        "research_supported": False,
    }


def _validate_continuous_report(
    payload: Mapping[str, Any], *, postrun: Mapping[str, Any]
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    _require(
        payload.get("schema_version") == CONTINUOUS_REPORT_SCHEMA_VERSION,
        "continuous final report schema drifted",
    )
    _require(
        payload.get("identity") == CONTINUOUS_IDENTITY,
        "continuous final report identity drifted",
    )
    _require(
        payload.get("status") == "owner_restart_aware_continuous_historical_economics_complete",
        "continuous final report is not complete historical economics",
    )
    _require(
        _integer(payload.get("epoch_count"), role="continuous epoch count")
        == postrun["epoch_count"],
        "continuous epoch count differs from strong audit",
    )
    _require(
        payload.get("adapter_plan_identity_sha256") == postrun["adapter_plan_identity_sha256"],
        "continuous final report adapter identity differs from strong audit",
    )
    permissions = _mapping(payload.get("permissions"), role="continuous permissions")
    _require_false_permissions(permissions, role="continuous final report")
    scope = _mapping(payload.get("evidence_scope"), role="continuous evidence scope")
    expected_scope = {
        "exchange_time": True,
        "modeled_queue": True,
        "restart_aware_continuous": True,
        "daily_fresh_start": False,
        "strict_queue": False,
        "receive_time_transport": False,
    }
    for field, expected in expected_scope.items():
        _require(scope.get(field) is expected, f"continuous evidence scope drifted: {field}")
    economics = _mapping(payload.get("economics"), role="continuous economics")
    arms = _mapping(economics.get("arms"), role="continuous arm economics")
    _require(set(arms) == {"control", "candidate"}, "continuous report lost an arm")
    paired = _mapping(economics.get("paired"), role="continuous paired economics")
    for arm in ("control", "candidate"):
        arm_metrics = _mapping(arms[arm], role=f"{arm} continuous economics")
        _require(
            _integer(arm_metrics.get("utc_day_count"), role=f"{arm} UTC days")
            == postrun["utc_day_count"],
            f"{arm} UTC day count differs from strong audit",
        )
    return arms, paired


def _validate_daily_rows(
    payload: Mapping[str, Any], *, expected_days: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _require(
        payload.get("schema_version") == PAIRED_DAILY_SCHEMA_VERSION,
        "paired-daily schema drifted",
    )
    _require(payload.get("identity") == CONTINUOUS_IDENTITY, "paired-daily identity drifted")
    raw_rows = payload.get("rows")
    _require(isinstance(raw_rows, list), "paired-daily rows must be a list")
    _require(len(raw_rows) == expected_days, "paired-daily denominator differs from strong audit")
    rows: list[dict[str, Any]] = []
    previous_day = ""
    terminal_deltas: list[float] = []
    closed_deltas: list[float] = []
    for ordinal, raw in enumerate(raw_rows, start=1):
        row = _mapping(raw, role=f"paired-daily row {ordinal}")
        day = str(row.get("day", ""))
        _require(day and day > previous_day, "paired-daily dates are duplicated or unordered")
        previous_day = day
        control = _finite(row.get("control_pnl_usdc"), role=f"{day} control PnL")
        candidate = _finite(row.get("candidate_pnl_usdc"), role=f"{day} candidate PnL")
        delta = _finite(row.get("delta_pnl_usdc"), role=f"{day} PnL delta")
        control_closed = _finite(
            row.get("control_closed_campaign_value_usdc"),
            role=f"{day} control closed value",
        )
        candidate_closed = _finite(
            row.get("candidate_closed_campaign_value_usdc"),
            role=f"{day} candidate closed value",
        )
        delta_closed = _finite(
            row.get("delta_closed_campaign_value_usdc"),
            role=f"{day} closed-value delta",
        )
        _close(candidate - control, delta, role=f"{day} paired PnL identity")
        _close(
            candidate_closed - control_closed,
            delta_closed,
            role=f"{day} paired closed-value identity",
        )
        terminal_deltas.append(delta)
        closed_deltas.append(delta_closed)
        rows.append(dict(row))
    positive_days = sum(value > 0.0 for value in terminal_deltas)
    summary = {
        "day_count": len(rows),
        "terminal_total_delta_usdc": math.fsum(terminal_deltas),
        "terminal_mean_delta_usdc_per_day": math.fsum(terminal_deltas) / len(rows),
        "closed_total_delta_usdc": math.fsum(closed_deltas),
        "closed_mean_delta_usdc_per_day": math.fsum(closed_deltas) / len(rows),
        "positive_terminal_delta_days": positive_days,
        "positive_terminal_delta_day_rate": positive_days / len(rows),
    }
    return rows, summary


def _validate_bootstrap(
    value: Any,
    *,
    role: str,
    daily_total: float,
    daily_mean: float,
    day_count: int,
) -> dict[str, Any]:
    bootstrap = _mapping(value, role=role)
    _require(
        _integer(bootstrap.get("day_count"), role=f"{role} day count") == day_count,
        f"{role} day count drifted",
    )
    total = _finite(bootstrap.get("total_delta_usdc"), role=f"{role} total")
    mean = _finite(bootstrap.get("mean_delta_usdc_per_day"), role=f"{role} mean")
    _close(total, daily_total, role=f"{role} total versus paired daily")
    _close(mean, daily_mean, role=f"{role} mean versus paired daily")
    return {
        "day_count": day_count,
        "total_delta_usdc": total,
        "mean_delta_usdc_per_day": mean,
        "ci95_mean_delta_usdc_per_day": _ci95(
            bootstrap.get("ci95_mean_delta_usdc_per_day"),
            role=f"{role} CI95",
        ),
        "bootstrap_draws": _integer(
            bootstrap.get("bootstrap_draws"), role=f"{role} bootstrap draws"
        ),
        "bootstrap_seed": _integer(bootstrap.get("bootstrap_seed"), role=f"{role} bootstrap seed"),
    }


def _extract_profile_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("score_profile_contract"), Mapping):
        source = payload["score_profile_contract"]
    elif isinstance(payload.get("scorecard"), Mapping):
        scorecard = payload["scorecard"]
        source = {
            "schema_version": experiment_scorecard_v2.PROFILE_SCHEMA_VERSION,
            "profile_id": scorecard.get("profile_id"),
            "profile_sha256": scorecard.get("profile_sha256"),
        }
    else:
        source = payload
    return {
        "schema_version": source.get("schema_version"),
        "profile_id": source.get("profile_id"),
        "profile_sha256": source.get("profile_sha256"),
    }


def _policy_rate(policy_audit: Mapping[str, Any]) -> dict[str, Any]:
    evaluations_raw = policy_audit.get("evaluations")
    nonbaseline_raw = policy_audit.get("nonbaseline")
    if evaluations_raw is None or nonbaseline_raw is None:
        return {
            "evaluations": None,
            "nonbaseline": None,
            "nonbaseline_rate": None,
            "available": False,
        }
    evaluations = _integer(evaluations_raw, role="continuous policy evaluations")
    nonbaseline = _integer(nonbaseline_raw, role="continuous nonbaseline decisions")
    _require(0 <= nonbaseline <= evaluations, "continuous policy decision counts drifted")
    return {
        "evaluations": evaluations,
        "nonbaseline": nonbaseline,
        "nonbaseline_rate": nonbaseline / evaluations if evaluations else None,
        "available": evaluations > 0,
    }


def _profile_compatibility(
    supplied_contract: Mapping[str, Any],
    *,
    economics: Mapping[str, Any],
    postrun: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    expected_contract = experiment_scorecard_v2.score_profile_contract(PROFILE_ID)
    profile_payload = experiment_scorecard_v2.score_profile_payload(PROFILE_ID)
    exact_contract_match = dict(supplied_contract) == expected_contract
    required_metrics = [
        str(row["name"]) for row in profile_payload["metrics"] if row.get("required") is True
    ]
    score_ready_metrics = ["closed_campaign_value", "fills_retention"]
    point_only_metrics = [
        "q10_shortfall_protection",
        "campaign_cvar10_protection",
        "maximum_inventory_avoidance",
    ]
    missing_metrics = sorted(
        set(required_metrics) - set(score_ready_metrics) - set(point_only_metrics)
    )
    blockers = []
    if not exact_contract_match:
        blockers.append("action_defense_v2_profile_contract_mismatch")
    blockers.extend(f"required_profile_metric_missing:{name}" for name in missing_metrics)
    blockers.extend(
        f"required_profile_metric_lacks_day_clustered_bounds:{name}" for name in point_only_metrics
    )
    blockers.extend(
        (
            "full_abs_inventory_time_btc_s_missing",
            "campaign_mae_and_negative_terminal_protection_missing",
            "activity_retention_missing",
            "side_specific_continuous_economics_missing",
            "canonical_support_ESS_propensity_overlap_missing",
            "continuous_accounting_final_mark_and_inventory_mtm_payload_missing",
            "strict_queue_authority_missing",
            "receive_time_transport_authority_missing",
            "prior_OOF_research_gate_failed",
            "prior_owner_daily_50d_hard_gate_failed",
        )
    )
    direct_api_compatible = not blockers
    compatibility = {
        "profile_id": PROFILE_ID,
        "expected_contract": expected_contract,
        "supplied_contract": dict(supplied_contract),
        "exact_contract_match": exact_contract_match,
        "profile_payload_sha256": canonical_sha256(profile_payload),
        "required_metric_names": required_metrics,
        "score_ready_metric_names": score_ready_metrics,
        "point_only_metric_names": point_only_metrics,
        "missing_metric_names": missing_metrics,
        "models_audit_score_canonical_evidence_compatible": direct_api_compatible,
        "models_audit_score_canonical_evidence_invoked": False,
        "compatibility_reason": (
            "input aggregates do not contain the complete noncompensable metric, "
            "support, and continuous-accounting payload required by action_defense_v2"
        ),
        "profile_modified": False,
        "profile_contract_fields_confirmed": [
            "schema_version",
            "profile_id",
            "profile_sha256",
        ],
        "available_continuous_economics": sorted(economics),
        "postrun_accounting_residual_usdc": postrun["maximum_abs_reconciliation_error_usdc"],
    }
    return compatibility, blockers


def build_report(
    *,
    postrun_audit_path: Path,
    final_report_path: Path,
    paired_daily_path: Path,
    owner_daily_50d_decision_path: Path,
    score_profile_contract_path: Path,
) -> dict[str, Any]:
    """Build the isolated report from exactly five aggregate JSON inputs."""

    input_paths = {
        "postrun_strong_audit": Path(postrun_audit_path).expanduser().resolve(),
        "continuous_final_report": Path(final_report_path).expanduser().resolve(),
        "continuous_paired_daily": Path(paired_daily_path).expanduser().resolve(),
        "owner_daily_50d_decision": Path(owner_daily_50d_decision_path).expanduser().resolve(),
        "score_profile_contract": Path(score_profile_contract_path).expanduser().resolve(),
    }
    _require(
        len(set(input_paths.values())) == len(input_paths), "input JSON paths are not distinct"
    )

    postrun_payload = _load_json(input_paths["postrun_strong_audit"], role="postrun strong audit")
    final_payload = _load_json(
        input_paths["continuous_final_report"], role="continuous final report"
    )
    daily_payload = _load_json(
        input_paths["continuous_paired_daily"], role="continuous paired daily"
    )
    owner_payload = _load_json(
        input_paths["owner_daily_50d_decision"], role="owner daily 50-day decision"
    )
    contract_payload = _load_json(
        input_paths["score_profile_contract"], role="score profile contract"
    )

    postrun = _validate_postrun(postrun_payload)
    owner_daily = _validate_owner_decision(owner_payload)
    arms, paired = _validate_continuous_report(final_payload, postrun=postrun)
    _, daily = _validate_daily_rows(daily_payload, expected_days=postrun["utc_day_count"])

    terminal = _validate_bootstrap(
        paired.get("daily_terminal_pnl"),
        role="terminal PnL bootstrap",
        daily_total=daily["terminal_total_delta_usdc"],
        daily_mean=daily["terminal_mean_delta_usdc_per_day"],
        day_count=daily["day_count"],
    )
    closed = _validate_bootstrap(
        paired.get("daily_closed_campaign_value"),
        role="closed-campaign bootstrap",
        daily_total=daily["closed_total_delta_usdc"],
        daily_mean=daily["closed_mean_delta_usdc_per_day"],
        day_count=daily["day_count"],
    )
    paired_terminal = _finite(
        paired.get("terminal_mtm_pnl_delta_usdc"), role="paired terminal delta"
    )
    paired_closed = _finite(
        paired.get("closed_campaign_value_delta_usdc"),
        role="paired closed-campaign delta",
    )
    _close(paired_terminal, terminal["total_delta_usdc"], role="terminal delta reconciliation")
    _close(paired_closed, closed["total_delta_usdc"], role="closed delta reconciliation")

    control = _mapping(arms["control"], role="control economics")
    candidate = _mapping(arms["candidate"], role="candidate economics")
    q10_delta = _finite(paired.get("campaign_q10_delta_usdc"), role="campaign q10 delta")
    cvar_delta = _finite(paired.get("campaign_cvar10_delta_usdc"), role="campaign CVaR10 delta")
    fill_retention = _finite(paired.get("fill_retention"), role="fill retention")
    max_inventory_delta = _finite(
        paired.get("max_abs_inventory_delta_btc"),
        role="maximum absolute inventory delta",
    )
    economics = {
        "terminal_mtm": terminal,
        "closed_campaign_value": closed,
        "positive_terminal_day": {
            "positive_days": daily["positive_terminal_delta_days"],
            "day_count": daily["day_count"],
            "rate": daily["positive_terminal_delta_day_rate"],
        },
        "campaign_q10": {
            "control_usdc": _finite(control.get("campaign_q10_usdc"), role="control campaign q10"),
            "candidate_usdc": _finite(
                candidate.get("campaign_q10_usdc"), role="candidate campaign q10"
            ),
            "candidate_minus_control_usdc": q10_delta,
            "day_clustered_interval_available": False,
        },
        "campaign_cvar10": {
            "control_usdc": _finite(
                control.get("campaign_cvar10_usdc"), role="control campaign CVaR10"
            ),
            "candidate_usdc": _finite(
                candidate.get("campaign_cvar10_usdc"), role="candidate campaign CVaR10"
            ),
            "candidate_minus_control_usdc": cvar_delta,
            "day_clustered_interval_available": False,
        },
        "fills": {
            "control_count": _integer(control.get("fill_count"), role="control fill count"),
            "candidate_count": _integer(candidate.get("fill_count"), role="candidate fill count"),
            "retention": fill_retention,
        },
        "maximum_absolute_inventory": {
            "control_btc": _finite(
                control.get("max_abs_inventory_btc"), role="control max inventory"
            ),
            "candidate_btc": _finite(
                candidate.get("max_abs_inventory_btc"),
                role="candidate max inventory",
            ),
            "candidate_minus_control_btc": max_inventory_delta,
            "control_minus_candidate_avoidance_btc": -max_inventory_delta,
            "day_clustered_interval_available": False,
        },
        "policy_mechanics": _policy_rate(postrun["candidate_policy_audit"]),
    }

    supplied_contract = _extract_profile_contract(contract_payload)
    compatibility, blockers = _profile_compatibility(
        supplied_contract,
        economics=economics,
        postrun=postrun,
    )
    profile_payload = experiment_scorecard_v2.score_profile_payload(PROFILE_ID)
    minimum_positive_rate = float(
        profile_payload["hard_gates"]["minimum_reward_daily_positive_rate"]
    )
    minimum_fill_retention = float(profile_payload["hard_gates"]["minimum_fills_retention"])
    calculable_gates = {
        "terminal_delta_lcb_strictly_positive": terminal["ci95_mean_delta_usdc_per_day"][0] > 0.0,
        "closed_campaign_delta_lcb_strictly_positive": closed["ci95_mean_delta_usdc_per_day"][0]
        > 0.0,
        "positive_day_rate_at_least_profile_minimum": daily["positive_terminal_delta_day_rate"]
        >= minimum_positive_rate,
        "campaign_q10_point_not_worse": q10_delta >= 0.0,
        "campaign_cvar10_point_not_worse": cvar_delta >= 0.0,
        "fill_retention_at_least_profile_minimum": fill_retention >= minimum_fill_retention,
        "maximum_inventory_point_not_worse": max_inventory_delta <= 0.0,
    }
    blockers.extend(
        f"calculable_hard_gate_failed:{name}"
        for name, passed in calculable_gates.items()
        if not passed
    )
    blockers = list(dict.fromkeys(blockers))

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "historical_economics_audited_not_scoreable_no_authority",
        "input_contract": {
            "allowed_inputs_only": [
                "postrun_strong_audit_json",
                "continuous_final_report_json",
                "continuous_paired_daily_json",
                "frozen_owner_daily_50d_decision_json",
                "frozen_score_profile_contract_json",
            ],
            "execution_plans_receipts_checkpoints_or_market_data_read": False,
        },
        "input_bindings": {name: _artifact(path, role=name) for name, path in input_paths.items()},
        "evidence_layers": {
            "oof_research": {
                "identity": OOF_RESEARCH_IDENTITY,
                "status": "research_gate_failed",
                "research_supported": False,
                "result_recomputed_by_this_builder": False,
                "corroborating_frozen_field": (
                    "owner_daily_50d_decision.permissions.research_supported=false"
                ),
            },
            "owner_daily_50d": {
                "identity": OWNER_DAILY_IDENTITY,
                "status": "hard_gate_failed_owner_continuation_only",
                **owner_daily,
            },
            "owner_restart_aware_continuous": {
                "identity": CONTINUOUS_IDENTITY,
                "status": "historical_economics_complete",
                "strong_postrun_audit_passed": True,
                "epoch_count": postrun["epoch_count"],
                "utc_day_count": postrun["utc_day_count"],
                "exchange_time": True,
                "modeled_queue": True,
                "strict_queue": False,
                "receive_time_transport": False,
            },
        },
        "economics": economics,
        "calculable_hard_gate_audit": {
            "profile_thresholds": {
                "minimum_reward_daily_positive_rate": minimum_positive_rate,
                "minimum_fills_retention": minimum_fill_retention,
            },
            "results": calculable_gates,
            "all_calculable_gates_passed": all(calculable_gates.values()),
            "not_a_complete_action_defense_v2_gate": True,
        },
        "score_profile_compatibility": compatibility,
        "hard_blockers": blockers,
        "scorecard": {
            "profile_id": PROFILE_ID,
            "formal_scorecard_generated": False,
            "formal_scorecard_passed": False,
            "ranking_eligible": False,
            "ranking_score": None,
            "candidate_class": "owner_historical_diagnostic",
            "promotion_status": "blocked_incomplete_noncompensable_evidence",
            "profile_modified": False,
        },
        "permissions": {field: False for field in PERMISSION_FIELDS},
    }
    report["report_sha256"] = canonical_sha256(report)
    return report


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_report(path: Path, payload: Mapping[str, Any], *, replace: bool = False) -> None:
    destination = path.expanduser().resolve()
    if destination.exists() and not replace:
        raise OwnerContinuousScorecardError(
            f"output already exists; pass --replace explicitly: {destination}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postrun-audit", type=Path, required=True)
    parser.add_argument("--final-report", type=Path, required=True)
    parser.add_argument("--paired-daily", type=Path, required=True)
    parser.add_argument("--owner-daily-50d-decision", type=Path, required=True)
    parser.add_argument("--score-profile-contract", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--replace", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    inputs = {
        Path(args.postrun_audit).expanduser().resolve(),
        Path(args.final_report).expanduser().resolve(),
        Path(args.paired_daily).expanduser().resolve(),
        Path(args.owner_daily_50d_decision).expanduser().resolve(),
        Path(args.score_profile_contract).expanduser().resolve(),
    }
    output = Path(args.output).expanduser().resolve()
    if output in inputs:
        raise OwnerContinuousScorecardError("output path aliases an input artifact")
    report = build_report(
        postrun_audit_path=args.postrun_audit,
        final_report_path=args.final_report,
        paired_daily_path=args.paired_daily,
        owner_daily_50d_decision_path=args.owner_daily_50d_decision,
        score_profile_contract_path=args.score_profile_contract,
    )
    write_report(output, report, replace=bool(args.replace))
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
