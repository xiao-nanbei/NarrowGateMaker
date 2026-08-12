#!/usr/bin/env python3
"""Continuous-path action scorecards with UTC day-end diagnostics only.

The original scorecard module is frozen by historical experiment identities.
This module defines successor profiles without changing those bytes.  UTC days
remain inference clusters, while cash, inventory, and campaign state must flow
continuously across midnight.  Day-end inventory, open-campaign MTM, and
censoring are retained as diagnostics and cannot affect ranking or hard gates.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import threading
from collections.abc import Mapping
from typing import Any

from models.audit import experiment_scorecard as _v1
from models.audit.experiment_scorecard import MetricRule, ScoreProfile

SCHEMA_VERSION = "narrowgate_experiment_scorecard.v2"
PROFILE_SCHEMA_VERSION = "narrowgate_score_profile.v2"
CANONICAL_EVIDENCE_SCHEMA_VERSION = _v1.CANONICAL_EVIDENCE_SCHEMA_VERSION
CONTINUOUS_PATH_SCHEMA_VERSION = "narrowgate_continuous_path_accounting.v1"
ACCOUNTING_TOLERANCE_USDC = 1e-6

DIAGNOSTIC_ONLY_METRICS = (
    "day_end_inventory_btc",
    "day_end_open_campaign_mtm",
    "day_end_open_campaign_mtm_usdc",
    "censoring_avoidance",
    "day_end_censoring_avoidance",
)

CONTINUOUS_RISK_HARD_GATES = (
    "campaign_cvar10_protection",
    "maximum_inventory_avoidance",
    "inventory_time_avoidance",
)

_V1_PROFILE_ADAPTER_LOCK = threading.RLock()


def _continuous_action_metrics(
    *,
    value_weight: float,
    tail_weight: float,
    lifecycle_weight: float,
    mechanism_weight: float,
    execution_weight: float = 0.0,
) -> tuple[MetricRule, ...]:
    """Preserve v1 weights while moving day-end censoring to campaign value."""

    rules = [
        MetricRule(
            "closed_campaign_value",
            "value",
            value_weight,
            "lcb_scale",
            scale=0.02,
        ),
        MetricRule(
            "conditional_net_value",
            "value",
            lifecycle_weight * (4.0 / 15.0),
            "lcb_scale",
            scale=0.02,
        ),
        MetricRule(
            "negative_terminal_protection",
            "tail",
            tail_weight * 0.44,
            "lcb_scale",
            scale=0.02,
        ),
        MetricRule(
            "q10_shortfall_protection",
            "tail",
            tail_weight * 0.36,
            "lcb_scale",
            scale=0.02,
        ),
        MetricRule(
            "campaign_mae_avoidance",
            "tail",
            tail_weight * 0.20,
            "lcb_scale",
            scale=0.05,
        ),
        MetricRule(
            "repair_event",
            "lifecycle",
            lifecycle_weight * (1.0 / 3.0),
            "lcb_scale",
            scale=0.01,
        ),
        MetricRule(
            "repair_time_avoidance_s",
            "lifecycle",
            lifecycle_weight * 0.40,
            "lcb_scale",
            scale=300.0,
        ),
        MetricRule(
            "fills_retention",
            "mechanism",
            mechanism_weight,
            "floor_to_target",
            floor=0.85,
            target=1.0,
        ),
    ]
    if execution_weight > 0.0:
        rules.extend(
            [
                MetricRule(
                    "queue_reset_value",
                    "execution",
                    execution_weight * (8.0 / 15.0),
                    "lcb_scale",
                    scale=0.01,
                ),
                MetricRule(
                    "latency_adjusted_value",
                    "execution",
                    execution_weight * (7.0 / 15.0),
                    "lcb_scale",
                    scale=0.01,
                ),
            ]
        )
    return tuple(rules)


ACTION_ALPHA_V2 = ScoreProfile(
    profile_id="action_alpha_v2",
    research_class="alpha",
    metrics=_continuous_action_metrics(
        value_weight=0.50,
        tail_weight=0.25,
        lifecycle_weight=0.15,
        mechanism_weight=0.10,
    ),
    minimum_rows=200,
    minimum_days=10,
    minimum_effective_sample_size=100.0,
    minimum_behavior_propensity=0.05,
    maximum_unsupported_mass=0.05,
    maximum_overlap_violations=0,
    minimum_reward_daily_positive_rate=0.55,
    minimum_fills_retention=0.85,
    minimum_candidate_rate=0.05,
    maximum_candidate_rate=0.75,
)

ACTION_DEFENSE_V2 = ScoreProfile(
    profile_id="action_defense_v2",
    research_class="defense",
    metrics=_continuous_action_metrics(
        value_weight=0.35,
        tail_weight=0.35,
        lifecycle_weight=0.15,
        mechanism_weight=0.15,
    ),
    minimum_rows=200,
    minimum_days=10,
    minimum_effective_sample_size=100.0,
    minimum_behavior_propensity=0.05,
    maximum_unsupported_mass=0.05,
    maximum_overlap_violations=0,
    minimum_reward_daily_positive_rate=0.55,
    minimum_fills_retention=0.85,
    minimum_candidate_rate=0.05,
    maximum_candidate_rate=0.50,
)

ACTION_EXECUTION_V2 = ScoreProfile(
    profile_id="action_execution_v2",
    research_class="execution",
    metrics=_continuous_action_metrics(
        value_weight=0.40,
        tail_weight=0.20,
        lifecycle_weight=0.10,
        mechanism_weight=0.15,
        execution_weight=0.15,
    ),
    minimum_rows=200,
    minimum_days=10,
    minimum_effective_sample_size=100.0,
    minimum_behavior_propensity=0.05,
    maximum_unsupported_mass=0.05,
    maximum_overlap_violations=0,
    minimum_reward_daily_positive_rate=0.55,
    minimum_fills_retention=0.90,
    minimum_candidate_rate=0.05,
    maximum_candidate_rate=0.75,
)

ACTION_EXECUTION_SELECTIVE_V3 = ScoreProfile(
    profile_id="action_execution_selective_v3",
    research_class="selective_execution",
    metrics=(
        MetricRule(
            "closed_campaign_value", "value", 0.35, "lcb_scale", scale=0.02
        ),
        MetricRule(
            "conditional_net_value", "value", 0.03, "lcb_scale", scale=0.02
        ),
        MetricRule(
            "negative_terminal_protection",
            "tail",
            0.07,
            "lcb_scale",
            scale=0.02,
        ),
        MetricRule(
            "q10_shortfall_protection",
            "tail",
            0.05,
            "lcb_scale",
            scale=0.02,
        ),
        MetricRule(
            "campaign_mae_avoidance",
            "tail",
            0.03,
            "lcb_scale",
            scale=0.05,
        ),
        MetricRule(
            "repair_event", "lifecycle", 0.03, "lcb_scale", scale=0.01
        ),
        MetricRule(
            "repair_time_avoidance_s",
            "lifecycle",
            0.04,
            "lcb_scale",
            scale=300.0,
        ),
        MetricRule(
            "fills_retention",
            "mechanism",
            0.05,
            "floor_to_target",
            floor=0.50,
            target=1.0,
        ),
        MetricRule(
            "queue_reset_value",
            "execution",
            0.05,
            "lcb_scale",
            scale=0.01,
        ),
        MetricRule(
            "latency_adjusted_value",
            "execution",
            0.05,
            "lcb_scale",
            scale=0.01,
        ),
        MetricRule(
            "toxic_fill_selectivity_log_ratio",
            "selectivity",
            0.15,
            "lcb_scale",
            scale=math.log(2.0),
        ),
        MetricRule(
            "toxic_reduction_surplus",
            "selectivity",
            0.10,
            "lcb_scale",
            scale=0.10,
        ),
    ),
    minimum_rows=200,
    minimum_days=10,
    minimum_effective_sample_size=100.0,
    minimum_behavior_propensity=0.05,
    maximum_unsupported_mass=0.05,
    maximum_overlap_violations=0,
    minimum_reward_daily_positive_rate=0.55,
    minimum_fills_retention=0.0,
    minimum_candidate_rate=0.05,
    maximum_candidate_rate=0.30,
    require_positive_toxic_selectivity=True,
)

PROFILES: dict[str, ScoreProfile] = {
    profile.profile_id: profile
    for profile in (
        ACTION_ALPHA_V2,
        ACTION_DEFENSE_V2,
        ACTION_EXECUTION_V2,
        ACTION_EXECUTION_SELECTIVE_V3,
    )
}


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def score_profile(profile_id: str) -> ScoreProfile:
    try:
        profile = PROFILES[str(profile_id)]
    except KeyError as exc:
        raise ValueError(
            f"unknown continuous-path score profile {profile_id!r}; "
            f"choose from {sorted(PROFILES)}"
        ) from exc
    total = sum(metric.weight for metric in profile.metrics)
    if not math.isclose(total, 1.0, abs_tol=1e-12):
        raise AssertionError(f"score profile weights sum to {total}, not 1")
    return profile


def score_profile_payload(profile_id: str) -> dict[str, Any]:
    profile = score_profile(profile_id)
    payload = profile.payload()
    payload["schema_version"] = PROFILE_SCHEMA_VERSION
    payload["value_hierarchy"] = {
        "primary": "closed_campaign_value",
        "supporting_value_gate": "conditional_net_value",
        "secondary": "full_panel_continuous_mtm",
        "utc_day_role": "bootstrap_cluster_only",
    }
    payload["continuous_path_accounting"] = {
        "schema_version": CONTINUOUS_PATH_SCHEMA_VERSION,
        "cash_inventory_campaign_state_cross_UTC_midnight": "required",
        "forced_day_end_liquidation_reset_or_campaign_terminal": "forbidden",
        "daily_identity": (
            "realized_d + q_end * mid_end - q_start * mid_start"
        ),
        "daily_sum_equals_continuous_panel_pnl": "required",
        "panel_final_inventory_mtm": "required_even_when_nonzero",
        "absolute_tolerance_usdc": ACCOUNTING_TOLERANCE_USDC,
    }
    payload["diagnostic_only_metrics"] = list(DIAGNOSTIC_ONLY_METRICS)
    payload["additional_noncompensable_risk_gates"] = {
        name: "day_clustered_lower_bound_nonnegative"
        for name in CONTINUOUS_RISK_HARD_GATES
    }
    payload["closed_campaign_value_gate"] = (
        "day_clustered_lower_bound_strictly_positive"
    )
    return payload


def score_profile_contract(profile_id: str) -> dict[str, Any]:
    payload = score_profile_payload(profile_id)
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "profile_id": str(profile_id),
        "profile_sha256": _canonical_sha256(payload),
    }


def _validate_continuous_path(
    evidence: Mapping[str, Any],
) -> tuple[list[str], list[str], dict[str, Any], dict[str, Any]]:
    validity_failures: list[str] = []
    hard_failures: list[str] = []
    metrics = evidence.get("metrics") or {}

    diagnostics = {
        name: copy.deepcopy(metrics[name])
        for name in DIAGNOSTIC_ONLY_METRICS
        if name in metrics
    }

    continuous_mtm = metrics.get("full_panel_continuous_mtm") or {}
    if not isinstance(continuous_mtm, Mapping) or _finite(
        continuous_mtm.get("estimate")
    ) is None:
        validity_failures.append("full_panel_continuous_mtm_missing_or_nonfinite")

    closed_campaign = metrics.get("closed_campaign_value") or {}
    closed_lower = (
        _finite(closed_campaign.get("lower_bound"))
        if isinstance(closed_campaign, Mapping)
        else None
    )
    if closed_lower is None:
        validity_failures.append("closed_campaign_value_lower_bound_missing")
    elif closed_lower <= 0.0:
        hard_failures.append("closed_campaign_value_lower_bound_not_positive")

    for name in CONTINUOUS_RISK_HARD_GATES:
        raw = metrics.get(name) or {}
        lower = _finite(raw.get("lower_bound")) if isinstance(raw, Mapping) else None
        if lower is None:
            validity_failures.append(f"{name}_lower_bound_missing")
        elif lower < 0.0:
            hard_failures.append(f"{name}_lower_bound_negative")

    accounting = evidence.get("continuous_path_accounting") or {}
    if not isinstance(accounting, Mapping):
        accounting = {}
    expected_exact = {
        "schema_version": CONTINUOUS_PATH_SCHEMA_VERSION,
        "utc_day_role": "bootstrap_cluster_only",
        "cash_carried_across_utc_days": True,
        "inventory_carried_across_utc_days": True,
        "campaign_state_carried_across_utc_days": True,
        "panel_final_inventory_mtm_included": True,
    }
    for key, expected in expected_exact.items():
        if accounting.get(key) != expected:
            validity_failures.append(f"continuous_path_contract_mismatch:{key}")

    for key in (
        "forced_day_end_liquidations",
        "day_end_state_resets",
        "day_end_campaign_terminals",
    ):
        value = _finite(accounting.get(key))
        if value is None:
            validity_failures.append(f"continuous_path_field_missing:{key}")
        elif value != 0.0:
            validity_failures.append(f"continuous_path_forbidden_nonzero:{key}")

    numeric_keys = (
        "daily_pnl_sum_usdc",
        "continuous_panel_pnl_usdc",
        "daily_accounting_identity_max_abs_error_usdc",
        "panel_final_inventory_btc",
        "panel_final_mark_price_usdc_per_btc",
        "panel_final_inventory_mtm_usdc",
    )
    numeric = {key: _finite(accounting.get(key)) for key in numeric_keys}
    for key, value in numeric.items():
        if value is None:
            validity_failures.append(f"continuous_path_field_missing:{key}")

    daily_sum = numeric["daily_pnl_sum_usdc"]
    panel_pnl = numeric["continuous_panel_pnl_usdc"]
    if daily_sum is not None and panel_pnl is not None:
        if abs(daily_sum - panel_pnl) > ACCOUNTING_TOLERANCE_USDC:
            validity_failures.append("daily_sum_continuous_panel_pnl_mismatch")
    daily_error = numeric["daily_accounting_identity_max_abs_error_usdc"]
    if daily_error is not None and daily_error > ACCOUNTING_TOLERANCE_USDC:
        validity_failures.append("daily_accounting_identity_tolerance_exceeded")

    inventory = numeric["panel_final_inventory_btc"]
    mark = numeric["panel_final_mark_price_usdc_per_btc"]
    final_mtm = numeric["panel_final_inventory_mtm_usdc"]
    if mark is not None and mark <= 0.0:
        validity_failures.append("panel_final_mark_price_not_positive")
    if inventory is not None and mark is not None and final_mtm is not None:
        if abs(inventory * mark - final_mtm) > ACCOUNTING_TOLERANCE_USDC:
            validity_failures.append("panel_final_inventory_mtm_mismatch")

    audit = {
        "schema_version": CONTINUOUS_PATH_SCHEMA_VERSION,
        "passed": not validity_failures,
        "failures": _dedupe(validity_failures),
        "accounting_tolerance_usdc": ACCOUNTING_TOLERANCE_USDC,
        "daily_pnl_sum_usdc": daily_sum,
        "continuous_panel_pnl_usdc": panel_pnl,
        "panel_final_inventory_btc": inventory,
        "panel_final_inventory_mtm_usdc": final_mtm,
    }
    return _dedupe(validity_failures), _dedupe(hard_failures), diagnostics, audit


def score_canonical_evidence(
    evidence: Mapping[str, Any],
    *,
    profile_id: str,
    require_frozen_profile: bool = True,
) -> dict[str, Any]:
    """Score evidence under the continuous-path successor contract."""

    if evidence.get("schema_version") != CANONICAL_EVIDENCE_SCHEMA_VERSION:
        raise ValueError("unsupported canonical score evidence schema")
    profile = score_profile(profile_id)
    expected_contract = score_profile_contract(profile_id)
    original_contract = evidence.get("score_profile_contract") or {}
    profile_locked = original_contract == expected_contract

    prepared = copy.deepcopy(dict(evidence))
    prepared_metrics = dict(prepared.get("metrics") or {})
    validity, hard, diagnostics, accounting_audit = _validate_continuous_path(
        evidence
    )
    for name in DIAGNOSTIC_ONLY_METRICS:
        prepared_metrics.pop(name, None)
    prepared["metrics"] = prepared_metrics
    prepared["validity_failures"] = _dedupe(
        list(prepared.get("validity_failures") or ())
        + validity
        + (
            ["score_profile_not_frozen_before_outcome"]
            if require_frozen_profile and not profile_locked
            else []
        )
    )
    prepared["family_gate_failures"] = _dedupe(
        list(prepared.get("family_gate_failures") or ()) + hard
    )

    # The frozen v1 engine supplies normalized scoring and support semantics.
    # A temporary private registration lets this successor reuse that engine
    # while the historical module itself remains byte-identical.
    with _V1_PROFILE_ADAPTER_LOCK:
        previous = _v1.PROFILES.get(profile_id)
        _v1.PROFILES[profile_id] = profile
        try:
            prepared["score_profile_contract"] = _v1.score_profile_contract(
                profile_id
            )
            output = _v1.score_canonical_evidence(
                prepared,
                profile_id=profile_id,
                require_frozen_profile=True,
            )
        finally:
            if previous is None:
                _v1.PROFILES.pop(profile_id, None)
            else:
                _v1.PROFILES[profile_id] = previous

    profile_payload = score_profile_payload(profile_id)
    output["schema_version"] = SCHEMA_VERSION
    output["profile"] = {
        **profile_payload,
        "profile_sha256": expected_contract["profile_sha256"],
        "frozen_before_outcome": profile_locked,
    }
    output["continuous_path_accounting"] = accounting_audit
    output["diagnostic_only_metrics"] = diagnostics

    if not profile_locked:
        output["ranking_eligible"] = False
        output["ranking_score"] = None
        if require_frozen_profile:
            output["promotion_status"] = "invalid_unfrozen_score_profile"
            output["candidate_class"] = "invalid"
        else:
            output["promotion_status"] = "retrospective_score_only"
            output["candidate_class"] = "retrospective_diagnostic"

    output["scorecard_sha256"] = ""
    output["scorecard_sha256"] = _canonical_sha256(
        {
            key: value
            for key, value in output.items()
            if key != "scorecard_sha256"
        }
    )
    return output
