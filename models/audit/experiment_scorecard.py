#!/usr/bin/env python3
"""Versioned, constraint-first scorecards for NarrowGate experiments.

The scorecard is deliberately downstream of paired replay or causal OPE.  It
does not turn raw PnL into causal evidence and it never lets a weighted score
compensate for invalid identity, poor overlap, tail harm, or mechanism drift.

New experiment families should freeze ``scorecard_profile_contract()`` in the
family specification before outcomes are read.  Profiles and economic scales
are code-versioned; changing either requires a new profile id.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "narrowgate_experiment_scorecard.v1"
PROFILE_SCHEMA_VERSION = "narrowgate_score_profile.v1"
CANONICAL_EVIDENCE_SCHEMA_VERSION = "narrowgate_score_evidence.v1"


@dataclass(frozen=True)
class MetricRule:
    """One fixed score contribution.

    ``lcb_scale`` and ``estimate_scale`` use an economic scale in the metric's
    native unit. ``t_stat`` clips a paired statistic at +/-3.  The remaining
    normalizers are bounded mechanism-quality transforms.
    """

    name: str
    component: str
    weight: float
    normalization: str
    scale: float = 1.0
    floor: float = 0.0
    target: float = 1.0
    required: bool = True


@dataclass(frozen=True)
class ScoreProfile:
    profile_id: str
    research_class: str
    metrics: tuple[MetricRule, ...]
    minimum_rows: int
    minimum_days: int
    minimum_effective_sample_size: float
    minimum_behavior_propensity: float
    maximum_unsupported_mass: float
    maximum_overlap_violations: int
    minimum_reward_daily_positive_rate: float
    minimum_fills_retention: float
    minimum_candidate_rate: float
    maximum_candidate_rate: float
    require_positive_reward_lcb: bool = True
    screening_only: bool = False
    screening_ranking_enabled: bool = False
    hard_gate_parameters: tuple[tuple[str, float], ...] = ()
    allow_selective_fill_loss_override: bool = False
    absolute_minimum_fills_retention: float = 0.0
    require_positive_toxic_selectivity: bool = False

    def payload(self) -> dict[str, Any]:
        payload = {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "profile_id": self.profile_id,
            "research_class": self.research_class,
            "metrics": [asdict(metric) for metric in self.metrics],
            "hard_gates": {
                "minimum_rows": self.minimum_rows,
                "minimum_days": self.minimum_days,
                "minimum_effective_sample_size": self.minimum_effective_sample_size,
                "minimum_behavior_propensity": self.minimum_behavior_propensity,
                "maximum_unsupported_mass": self.maximum_unsupported_mass,
                "maximum_overlap_violations": self.maximum_overlap_violations,
                "minimum_reward_daily_positive_rate": (
                    self.minimum_reward_daily_positive_rate
                ),
                "minimum_fills_retention": self.minimum_fills_retention,
                "candidate_rate": [
                    self.minimum_candidate_rate,
                    self.maximum_candidate_rate,
                ],
                "require_positive_reward_lcb": self.require_positive_reward_lcb,
            },
            "screening_only": self.screening_only,
        }
        if self.allow_selective_fill_loss_override:
            payload["selective_fill_loss_override"] = {
                "enabled": True,
                "absolute_minimum_fills_retention": (
                    self.absolute_minimum_fills_retention
                ),
                "require_positive_toxic_selectivity": (
                    self.require_positive_toxic_selectivity
                ),
                "rule": (
                    "below the standard fills-retention floor, require positive "
                    "day-clustered lower bounds for toxic selectivity and toxic "
                    "reduction surplus"
                ),
            }
        elif self.require_positive_toxic_selectivity:
            payload["toxic_selectivity_gate"] = {
                "enabled": True,
                "minimum_fills_retention": self.minimum_fills_retention,
                "required_positive_lower_bounds": [
                    "toxic_fill_selectivity_log_ratio",
                    "toxic_reduction_surplus",
                ],
                "rule": (
                    "volume loss is allowed only when toxic fills fall "
                    "disproportionately faster and conditional net value has "
                    "a positive lower bound"
                ),
            }
        if self.screening_ranking_enabled:
            payload["screening_ranking_enabled"] = True
        if self.hard_gate_parameters:
            payload["hard_gate_parameters"] = dict(self.hard_gate_parameters)
        return payload


def _action_metrics(
    *,
    value_weight: float,
    tail_weight: float,
    lifecycle_weight: float,
    mechanism_weight: float,
    execution_weight: float = 0.0,
) -> tuple[MetricRule, ...]:
    """Build fixed within-component allocations without outcome-time tuning."""

    rules = [
        MetricRule(
            "conditional_net_value",
            "value",
            value_weight,
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
            "censoring_avoidance",
            "lifecycle",
            lifecycle_weight * (4.0 / 15.0),
            "lcb_scale",
            scale=0.01,
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


ACTION_ALPHA_V1 = ScoreProfile(
    profile_id="action_alpha_v1",
    research_class="alpha",
    metrics=_action_metrics(
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

ACTION_DEFENSE_V1 = ScoreProfile(
    profile_id="action_defense_v1",
    research_class="defense",
    metrics=_action_metrics(
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

ACTION_EXECUTION_V1 = ScoreProfile(
    profile_id="action_execution_v1",
    research_class="execution",
    metrics=_action_metrics(
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

ACTION_EXECUTION_SELECTIVE_V1 = ScoreProfile(
    profile_id="action_execution_selective_v1",
    research_class="selective_execution",
    metrics=(
        MetricRule(
            "conditional_net_value",
            "value",
            0.35,
            "lcb_scale",
            scale=0.02,
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
            "repair_event",
            "lifecycle",
            0.03,
            "lcb_scale",
            scale=0.01,
        ),
        MetricRule(
            "repair_time_avoidance_s",
            "lifecycle",
            0.04,
            "lcb_scale",
            scale=300.0,
        ),
        MetricRule(
            "censoring_avoidance",
            "lifecycle",
            0.03,
            "lcb_scale",
            scale=0.01,
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
    minimum_fills_retention=0.85,
    minimum_candidate_rate=0.05,
    maximum_candidate_rate=0.30,
    allow_selective_fill_loss_override=True,
    absolute_minimum_fills_retention=0.50,
    require_positive_toxic_selectivity=True,
)

# Volume loss is not itself a failure for a genuinely selective maker action.
# This v2 contract removes the absolute retention floor while keeping three
# non-negotiable gates: positive net-value LCB, positive toxic-share log-ratio
# LCB, and positive toxic-reduction-surplus LCB. A cancel-all policy therefore
# still fails because toxic and total fills fall proportionally (surplus=0).
ACTION_EXECUTION_SELECTIVE_V2 = ScoreProfile(
    profile_id="action_execution_selective_v2",
    research_class="selective_execution",
    metrics=ACTION_EXECUTION_SELECTIVE_V1.metrics,
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

PAIRED_SCREEN_V1 = ScoreProfile(
    profile_id="paired_screen_v1",
    research_class="paired_screening",
    metrics=(
        MetricRule("raw_paired_t", "value", 0.20, "t_stat"),
        MetricRule("terminal_paired_t", "value", 0.20, "t_stat"),
        MetricRule("inventory_adjusted_paired_t", "value", 0.10, "t_stat"),
        MetricRule("tail_day_balance", "tail", 0.08, "bounded"),
        MetricRule(
            "bad_campaign_rate_avoidance",
            "tail",
            0.06,
            "estimate_scale",
            scale=0.01,
        ),
        MetricRule(
            "campaign_mae_avoidance_ratio",
            "tail",
            0.06,
            "estimate_scale",
            scale=0.10,
        ),
        MetricRule(
            "repair_rate_uplift",
            "lifecycle",
            0.05,
            "estimate_scale",
            scale=0.01,
        ),
        MetricRule(
            "duration_avoidance_ratio",
            "lifecycle",
            0.05,
            "estimate_scale",
            scale=0.10,
        ),
        MetricRule(
            "fills_retention",
            "mechanism",
            0.08,
            "floor_to_target",
            floor=0.85,
            target=1.0,
        ),
        MetricRule(
            "inventory_time_avoidance_ratio",
            "mechanism",
            0.06,
            "estimate_scale",
            scale=0.10,
        ),
        MetricRule(
            "action_mix_drift",
            "mechanism",
            0.06,
            "deviation_budget",
            scale=0.06,
        ),
    ),
    minimum_rows=0,
    minimum_days=20,
    minimum_effective_sample_size=0.0,
    minimum_behavior_propensity=0.0,
    maximum_unsupported_mass=1.0,
    maximum_overlap_violations=0,
    minimum_reward_daily_positive_rate=0.55,
    minimum_fills_retention=0.85,
    minimum_candidate_rate=0.0,
    maximum_candidate_rate=1.0,
    require_positive_reward_lcb=False,
    screening_only=True,
)

# v1 is a frozen compatibility profile whose score was emitted but never used
# for ordering. v2 is the first paired profile with one explicit ranking
# authority. It remains screening-only and therefore has no panel-promotion
# permission.
PAIRED_SCREEN_V2 = ScoreProfile(
    profile_id="paired_screen_v2",
    research_class="paired_screening_v2",
    metrics=PAIRED_SCREEN_V1.metrics,
    minimum_rows=0,
    minimum_days=20,
    minimum_effective_sample_size=0.0,
    minimum_behavior_propensity=0.0,
    maximum_unsupported_mass=1.0,
    maximum_overlap_violations=0,
    minimum_reward_daily_positive_rate=0.55,
    minimum_fills_retention=0.0,
    minimum_candidate_rate=0.0,
    maximum_candidate_rate=1.0,
    require_positive_reward_lcb=False,
    screening_only=True,
    screening_ranking_enabled=True,
    hard_gate_parameters=(
        ("minimum_coverage", 0.98),
        ("minimum_fills_ratio", 0.85),
        ("maximum_fills_ratio", 1.20),
        ("minimum_campaign_ratio", 0.65),
        ("maximum_campaign_ratio", 1.45),
        ("maximum_pause_rate_delta", 0.06),
        ("maximum_keep_rate_delta", 0.08),
        ("maximum_place_replace_rate_delta", 0.08),
        ("maximum_spread_delta", 10.0),
        ("minimum_side_fill_share", 0.30),
    ),
)

PROFILES: dict[str, ScoreProfile] = {
    profile.profile_id: profile
    for profile in (
        ACTION_ALPHA_V1,
        ACTION_DEFENSE_V1,
        ACTION_EXECUTION_V1,
        ACTION_EXECUTION_SELECTIVE_V1,
        ACTION_EXECUTION_SELECTIVE_V2,
        PAIRED_SCREEN_V1,
        PAIRED_SCREEN_V2,
    )
}


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def score_profile(profile_id: str) -> ScoreProfile:
    try:
        profile = PROFILES[str(profile_id)]
    except KeyError as exc:
        raise ValueError(
            f"unknown score profile {profile_id!r}; choose from {sorted(PROFILES)}"
        ) from exc
    total = sum(metric.weight for metric in profile.metrics)
    if not math.isclose(total, 1.0, abs_tol=1e-12):
        raise AssertionError(f"score profile weights sum to {total}, not 1")
    return profile


def score_profile_contract(profile_id: str) -> dict[str, Any]:
    profile = score_profile(profile_id)
    payload = profile.payload()
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "profile_id": profile.profile_id,
        "profile_sha256": _canonical_sha256(payload),
    }


def _finite(value: Any, default: float = math.nan) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric if math.isfinite(numeric) else default


def _clip(value: float) -> float:
    return max(-1.0, min(1.0, float(value)))


def _optional_finite(value: Any) -> float | None:
    numeric = _finite(value)
    return numeric if math.isfinite(numeric) else None


def _normalized_metric(
    evidence: Mapping[str, Any], rule: MetricRule
) -> tuple[float, float, str]:
    estimate = _finite(evidence.get("estimate"))
    lower = _finite(evidence.get("lower_bound"))
    if rule.normalization == "lcb_scale":
        if not math.isfinite(lower):
            raise ValueError(f"metric {rule.name} requires a finite lower_bound")
        return _clip(lower / rule.scale), lower, "lower_bound"
    if rule.normalization == "estimate_scale":
        if not math.isfinite(estimate):
            raise ValueError(f"metric {rule.name} requires a finite estimate")
        return _clip(estimate / rule.scale), estimate, "estimate"
    if rule.normalization == "t_stat":
        if not math.isfinite(estimate):
            raise ValueError(f"metric {rule.name} requires a finite estimate")
        return _clip(estimate / 3.0), estimate, "paired_t_stat"
    if rule.normalization == "bounded":
        if not math.isfinite(estimate):
            raise ValueError(f"metric {rule.name} requires a finite estimate")
        return _clip(estimate), estimate, "estimate"
    if rule.normalization == "floor_to_target":
        if not math.isfinite(estimate):
            raise ValueError(f"metric {rule.name} requires a finite estimate")
        width = rule.target - rule.floor
        if width <= 0.0:
            raise ValueError(f"metric {rule.name} has an invalid floor/target")
        return _clip((estimate - rule.floor) / width), estimate, "estimate"
    if rule.normalization == "deviation_budget":
        if not math.isfinite(estimate) or rule.scale <= 0.0:
            raise ValueError(f"metric {rule.name} requires finite deviation and budget")
        return _clip(1.0 - abs(estimate) / rule.scale), estimate, "estimate"
    raise ValueError(f"unsupported normalization {rule.normalization!r}")


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value)))


def _economic_classification(
    evidence: Mapping[str, Any],
    *,
    profile: ScoreProfile,
    support_failures: Sequence[str],
    hard_failures: Sequence[str],
) -> str:
    metrics = evidence.get("metrics") or {}
    if profile.research_class == "paired_screening_v2":
        paired = evidence.get("paired_screening_summary") or {}
        if support_failures:
            return "diagnostic_only_insufficient_support"
        if hard_failures:
            return "rejected_mechanism_or_activity_drift"
        raw_delta = _finite(paired.get("raw_delta_sum"), 0.0)
        terminal_delta = _finite(paired.get("terminal_delta_sum"), 0.0)
        activity_adjusted = _finite(
            paired.get("activity_adjusted_raw_delta"), 0.0
        )
        campaign_adjusted = _finite(
            paired.get("campaign_adjusted_terminal_delta"), 0.0
        )
        tail_delta = _finite(paired.get("tail_campaign_delta"), 0.0)
        if raw_delta > 0.0 and terminal_delta > 0.0:
            if (
                activity_adjusted >= 0.0
                and campaign_adjusted >= 0.0
                and tail_delta <= 0.0
            ):
                return "alpha_candidate"
            if activity_adjusted < 0.0 and campaign_adjusted < 0.0:
                return "risk_control_candidate"
            return "mixed_value_candidate"
        if raw_delta > 0.0 or terminal_delta > 0.0:
            return "mixed_paired_evidence"
        return "no_paired_value_evidence"
    reward = metrics.get("conditional_net_value") or {}
    reward_estimate = _finite(reward.get("estimate"), 0.0)
    reward_lower = _finite(reward.get("lower_bound"), -math.inf)
    retention = _finite((metrics.get("fills_retention") or {}).get("estimate"))
    tail_names = (
        "negative_terminal_protection",
        "q10_shortfall_protection",
        "campaign_mae_avoidance",
    )
    tail_positive = sum(
        _finite((metrics.get(name) or {}).get("estimate"), 0.0) > 0.0
        for name in tail_names
    )
    if support_failures:
        return "diagnostic_only_insufficient_support"
    if (
        reward_estimate > 0.0
        and reward_lower <= 0.0
        and tail_positive >= 2
        and math.isfinite(retention)
        and retention < profile.minimum_fills_retention
    ):
        return "overbroad_risk_control"
    if reward_lower > 0.0 and not hard_failures:
        return "conditional_action_value_candidate"
    if tail_positive >= 2 and reward_lower <= 0.0:
        return "risk_control_evidence_only"
    if any("tail" in failure or "protection" in failure for failure in hard_failures):
        return "tail_tradeoff"
    return "no_action_uplift_evidence"


def score_canonical_evidence(
    evidence: Mapping[str, Any],
    *,
    profile_id: str,
    require_frozen_profile: bool = True,
) -> dict[str, Any]:
    """Score canonical evidence while keeping validity and hard gates separate."""

    if evidence.get("schema_version") != CANONICAL_EVIDENCE_SCHEMA_VERSION:
        raise ValueError("unsupported canonical score evidence schema")
    profile = score_profile(profile_id)
    metrics = evidence.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ValueError("canonical evidence requires a metrics object")

    validity_failures = list(evidence.get("validity_failures") or ())
    profile_lock = evidence.get("score_profile_contract") or {}
    expected_contract = score_profile_contract(profile_id)
    profile_locked = profile_lock == expected_contract
    if require_frozen_profile and not profile_locked:
        validity_failures.append("score_profile_not_frozen_before_outcome")

    metric_rows: list[dict[str, Any]] = []
    missing_metrics: list[str] = []
    for rule in profile.metrics:
        raw = metrics.get(rule.name)
        if not isinstance(raw, Mapping):
            if rule.required:
                missing_metrics.append(rule.name)
            continue
        try:
            normalized, statistic, statistic_source = _normalized_metric(raw, rule)
        except ValueError as exc:
            validity_failures.append(str(exc))
            continue
        metric_rows.append(
            {
                "name": rule.name,
                "component": rule.component,
                "weight": rule.weight,
                "normalization": rule.normalization,
                "economic_scale": rule.scale,
                "floor": rule.floor,
                "target": rule.target,
                "estimate": _optional_finite(raw.get("estimate")),
                "lower_bound": _optional_finite(raw.get("lower_bound")),
                "upper_bound": _optional_finite(raw.get("upper_bound")),
                "daily_positive_rate": _optional_finite(
                    raw.get("daily_positive_rate")
                ),
                "scoring_statistic": statistic,
                "statistic_source": statistic_source,
                "normalized_score": normalized,
                "weighted_contribution_before_shrink": rule.weight * normalized,
                "source": str(raw.get("source", "")),
            }
        )
    validity_failures.extend(f"missing_score_metric:{name}" for name in missing_metrics)
    validity_failures = _dedupe(validity_failures)

    support = dict(evidence.get("support") or {})
    n_rows = int(_finite(support.get("n_rows"), 0.0))
    n_days = int(_finite(support.get("n_days"), 0.0))
    ess = _finite(support.get("effective_sample_size"), 0.0)
    min_propensity = _finite(support.get("minimum_behavior_propensity"), 0.0)
    unsupported_mass = _finite(support.get("unsupported_mass"), 1.0)
    overlap_violations = int(
        _finite(
            support.get("overlap_violations"),
            float(profile.maximum_overlap_violations + 1),
        )
    )
    support_failures = list(support.get("failures") or ())
    if n_rows < profile.minimum_rows:
        support_failures.append(f"rows_below_{profile.minimum_rows}")
    if n_days < profile.minimum_days:
        support_failures.append(f"days_below_{profile.minimum_days}")
    if ess < profile.minimum_effective_sample_size:
        support_failures.append(
            f"effective_sample_size_below_{profile.minimum_effective_sample_size:g}"
        )
    if min_propensity < profile.minimum_behavior_propensity:
        support_failures.append(
            f"behavior_propensity_below_{profile.minimum_behavior_propensity:g}"
        )
    if unsupported_mass > profile.maximum_unsupported_mass:
        support_failures.append(
            f"unsupported_mass_above_{profile.maximum_unsupported_mass:g}"
        )
    if overlap_violations > profile.maximum_overlap_violations:
        support_failures.append("overlap_violations")
    support_failures = _dedupe(support_failures)

    hard_failures = list(evidence.get("family_gate_failures") or ())
    reward = metrics.get("conditional_net_value") or {}
    reward_lower = _finite(reward.get("lower_bound"), -math.inf)
    reward_daily = _finite(reward.get("daily_positive_rate"), -math.inf)
    if profile.require_positive_reward_lcb and reward_lower <= 0.0:
        hard_failures.append("conditional_net_value_lower_bound_not_positive")
    if (
        profile.require_positive_reward_lcb
        and reward_daily < profile.minimum_reward_daily_positive_rate
    ):
        hard_failures.append(
            "reward_daily_positive_rate_below_"
            f"{profile.minimum_reward_daily_positive_rate:g}"
        )
    for name in (
        "negative_terminal_protection",
        "q10_shortfall_protection",
        "campaign_mae_avoidance",
    ):
        raw = metrics.get(name)
        if isinstance(raw, Mapping) and _finite(raw.get("lower_bound"), -math.inf) < 0.0:
            hard_failures.append(f"{name}_lower_bound_negative")
    for name in ("repair_event", "repair_time_avoidance_s", "censoring_avoidance"):
        raw = metrics.get(name)
        if isinstance(raw, Mapping) and _finite(raw.get("estimate"), -math.inf) < 0.0:
            hard_failures.append(f"{name}_point_estimate_negative")

    selectivity = metrics.get("toxic_fill_selectivity_log_ratio") or {}
    selectivity_lower = _finite(selectivity.get("lower_bound"), -math.inf)
    reduction_surplus = metrics.get("toxic_reduction_surplus") or {}
    reduction_surplus_lower = _finite(
        reduction_surplus.get("lower_bound"), -math.inf
    )
    if profile.require_positive_toxic_selectivity:
        if selectivity_lower <= 0.0:
            hard_failures.append(
                "toxic_fill_selectivity_lower_bound_not_positive"
            )
        if reduction_surplus_lower <= 0.0:
            hard_failures.append(
                "toxic_reduction_surplus_lower_bound_not_positive"
            )

    retention = _finite((metrics.get("fills_retention") or {}).get("estimate"))
    if not math.isfinite(retention):
        hard_failures.append("fills_retention_not_finite")
    elif retention < profile.minimum_fills_retention:
        if not profile.allow_selective_fill_loss_override:
            hard_failures.append(
                f"fills_retention_below_{profile.minimum_fills_retention:g}"
            )
        else:
            selective_override_passed = bool(
                retention >= profile.absolute_minimum_fills_retention
                and selectivity_lower > 0.0
                and reduction_surplus_lower > 0.0
                and reward_lower > 0.0
            )
            if selective_override_passed:
                pass
            elif retention < profile.absolute_minimum_fills_retention:
                hard_failures.append(
                    "fills_retention_below_absolute_"
                    f"{profile.absolute_minimum_fills_retention:g}"
                )
            else:
                hard_failures.append(
                    f"fills_retention_below_{profile.minimum_fills_retention:g}"
                    "_without_selective_toxic_removal"
                )
    candidate_rate = _finite(evidence.get("candidate_rate"))
    if not (
        math.isfinite(candidate_rate)
        and profile.minimum_candidate_rate
        <= candidate_rate
        <= profile.maximum_candidate_rate
    ):
        hard_failures.append("candidate_rate_outside_profile_budget")
    if evidence.get("invariant_violations"):
        hard_failures.extend(
            f"invariant:{value}" for value in evidence["invariant_violations"]
        )
    hard_failures = _dedupe(hard_failures)

    shrink = math.sqrt(n_days / (n_days + 8.0)) if n_days > 0 else 0.0
    component_rows: dict[str, dict[str, float]] = {}
    for row in metric_rows:
        component = component_rows.setdefault(
            str(row["component"]),
            {"weight": 0.0, "raw_contribution": 0.0},
        )
        component["weight"] += float(row["weight"])
        component["raw_contribution"] += float(
            row["weighted_contribution_before_shrink"]
        )
    for component in component_rows.values():
        component["score"] = (
            component["raw_contribution"] / component["weight"]
            if component["weight"] > 0.0
            else 0.0
        )
        component["contribution_after_shrink"] = (
            component["raw_contribution"] * shrink
        )

    raw_score = sum(
        float(row["weighted_contribution_before_shrink"]) for row in metric_rows
    )
    total_score = raw_score * shrink
    validity_passed = not validity_failures
    support_passed = not support_failures
    hard_gates_passed = not hard_failures
    ranking_eligible = bool(
        validity_passed
        and support_passed
        and hard_gates_passed
        and profile_locked
        and (not profile.screening_only or profile.screening_ranking_enabled)
    )
    economic_classification = _economic_classification(
        evidence,
        profile=profile,
        support_failures=support_failures,
        hard_failures=hard_failures,
    )
    panel_role = str(evidence.get("panel_role", "development"))
    if require_frozen_profile and not profile_locked:
        promotion_status = "invalid_unfrozen_score_profile"
        candidate_class = "invalid"
    elif not validity_passed:
        promotion_status = "invalid_evidence"
        candidate_class = "invalid"
    elif not support_passed:
        promotion_status = "diagnostic_only_support_failed"
        candidate_class = "diagnostic_only"
    elif not hard_gates_passed and profile.research_class == "paired_screening_v2":
        promotion_status = "screening_gate_failed"
        candidate_class = "hard_gate_failed"
    elif not hard_gates_passed:
        promotion_status = f"{panel_role}_failed_family_closed"
        candidate_class = "hard_gate_failed"
    elif not profile_locked:
        promotion_status = "retrospective_score_only"
        candidate_class = "retrospective_diagnostic"
    elif profile.screening_only:
        promotion_status = "screening_rank_only"
        if profile.research_class == "paired_screening_v2":
            candidate_class = {
                "alpha_candidate": "screening_alpha_candidate",
                "risk_control_candidate": "screening_risk_control_candidate",
                "mixed_value_candidate": "screening_mixed_candidate",
            }.get(economic_classification, "screening_diagnostic")
        else:
            candidate_class = "screening_candidate"
    elif panel_role == "development":
        promotion_status = "development_passed_validation_locked"
        candidate_class = "development_candidate"
    elif panel_role == "validation":
        promotion_status = "validation_passed_holdout_locked"
        candidate_class = "validation_candidate"
    elif panel_role == "sealed_holdout":
        promotion_status = "sealed_holdout_passed_shadow_candidate"
        candidate_class = "shadow_candidate"
    else:
        promotion_status = "evidence_passed_unknown_panel"
        candidate_class = "evidence_candidate"

    profile_payload = profile.payload()
    input_identity = dict(evidence.get("input_identity") or {})
    output = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": str(evidence.get("experiment_id", "")),
        "family_id": str(evidence.get("family_id", "")),
        "panel_role": panel_role,
        "profile": {
            **profile_payload,
            "profile_sha256": _canonical_sha256(profile_payload),
            "frozen_before_outcome": profile_locked,
        },
        "input_identity": input_identity,
        "input_identity_sha256": _canonical_sha256(input_identity),
        "validity": {
            "passed": validity_passed,
            "failures": validity_failures,
        },
        "support": {
            "passed": support_passed,
            "failures": support_failures,
            "n_rows": n_rows,
            "n_days": n_days,
            "effective_sample_size": ess,
            "minimum_behavior_propensity": min_propensity,
            "unsupported_mass": unsupported_mass,
            "overlap_violations": overlap_violations,
        },
        "hard_gates": {
            "passed": hard_gates_passed,
            "failures": hard_failures,
        },
        "metrics": metric_rows,
        "components": component_rows,
        "weight_coverage": sum(float(row["weight"]) for row in metric_rows),
        "shrink_factor": shrink,
        "raw_weighted_score": raw_score,
        "total_score": total_score,
        "ranking_score": total_score if ranking_eligible else None,
        "ranking_eligible": ranking_eligible,
        "candidate_class": candidate_class,
        "economic_classification": economic_classification,
        "promotion_status": promotion_status,
        "scorecard_sha256": "",
    }
    output["scorecard_sha256"] = _canonical_sha256(
        {key: value for key, value in output.items() if key != "scorecard_sha256"}
    )
    return output


def _metric_from_summary(
    panel: Mapping[str, Any], aliases: Sequence[str], *, source: str
) -> dict[str, Any] | None:
    for alias in aliases:
        raw = panel.get(alias)
        if not isinstance(raw, Mapping):
            continue
        interval = raw.get("interval") or {}
        return {
            "estimate": _finite(raw.get("uplift")),
            "lower_bound": _finite(interval.get("p025")),
            "upper_bound": _finite(interval.get("p975")),
            "daily_positive_rate": _finite(raw.get("daily_positive_rate")),
            "source": f"{source}.{alias}",
        }
    return None


def _verify_file_identity(identity: Mapping[str, Any], *, label: str) -> list[str]:
    failures: list[str] = []
    path_text = str(identity.get("path", ""))
    expected = str(identity.get("sha256", ""))
    if not path_text or not expected:
        return [f"{label}_identity_missing"]
    path = Path(path_text).expanduser().resolve()
    if not path.is_file():
        return [f"{label}_file_missing"]
    if _sha256_file(path) != expected:
        failures.append(f"{label}_hash_mismatch")
    return failures


def action_family_score_evidence(
    result: Mapping[str, Any],
    family_spec: Mapping[str, Any],
    *,
    panel_role: str,
    profile_id: str,
) -> dict[str, Any]:
    """Adapt a side-specific action-family result to canonical score evidence."""

    panel = result.get(panel_role)
    if not isinstance(panel, Mapping):
        raise ValueError(f"result has no evaluated panel {panel_role!r}")
    validity_failures: list[str] = []
    if str(result.get("family_id", "")) != str(family_spec.get("family_id", "")):
        validity_failures.append("family_id_mismatch")
    if str(family_spec.get("status", "")) != "frozen_before_outcome_replay":
        validity_failures.append("family_spec_not_frozen_before_outcome")
    validity_failures.extend(
        _verify_file_identity(result.get("family_spec") or {}, label="family_spec")
    )
    family_spec_identity = result.get("family_spec") or {}
    family_spec_path = Path(str(family_spec_identity.get("path", ""))).expanduser()
    if family_spec_path.is_file():
        try:
            persisted_spec = json.loads(family_spec_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            validity_failures.append("family_spec_file_invalid_json")
        else:
            if persisted_spec != dict(family_spec):
                validity_failures.append("supplied_family_spec_differs_from_hashed_file")
    evidence_split = family_spec.get("evidence_split") or {}
    validity_failures.extend(
        _verify_file_identity(evidence_split, label="evidence_split")
    )
    panel_identity_key = f"{panel_role}_panel"
    if panel_identity_key in result:
        validity_failures.extend(
            _verify_file_identity(result.get(panel_identity_key) or {}, label=panel_identity_key)
        )
    else:
        validity_failures.append(f"{panel_identity_key}_identity_missing")
    baseline = family_spec.get("baseline") or {}
    for key in ("config_sha256", "p3_sha256", "queue_sha256", "latency_sha256"):
        if not str(baseline.get(key, "")):
            validity_failures.append(f"baseline_identity_missing:{key}")

    probabilities = family_spec.get("behavior_probabilities") or {}
    probability_values = [_finite(value) for value in probabilities.values()]
    if (
        not probability_values
        or any(not math.isfinite(value) or value <= 0.0 for value in probability_values)
        or not math.isclose(sum(probability_values), 1.0, abs_tol=1e-10)
    ):
        validity_failures.append("invalid_behavior_probability_vector")

    invariants = family_spec.get("invariants") or {}
    invariant_violations: list[str] = []
    for key in (
        "size_modified",
        "order_size_modified",
        "reducing_side_modified",
        "inventory_limit_modified",
        "taker_order_added",
    ):
        if bool(invariants.get(key, False)):
            invariant_violations.append(key)

    score_contract = family_spec.get("scorecard_profile") or family_spec.get(
        "scorecard"
    )
    metrics: dict[str, dict[str, Any]] = {}
    aliases = {
        "conditional_net_value": ("reward",),
        "negative_terminal_protection": (
            "negative_terminal_protection",
            "negative_terminal_mtm",
        ),
        "q10_shortfall_protection": (
            "development_q10_shortfall_protection",
            "development_q10_shortfall",
        ),
        "campaign_mae_avoidance": ("campaign_mae_avoidance",),
        "repair_event": ("repair_event", "repair_first_30m"),
        "repair_time_avoidance_s": ("restricted_time_to_repair",),
        "censoring_avoidance": ("day_end_censoring_avoidance",),
        "queue_reset_value": ("queue_reset_value",),
        "latency_adjusted_value": ("latency_adjusted_value",),
        "toxic_fill_selectivity_log_ratio": (
            "toxic_fill_selectivity_log_ratio",
        ),
        "toxic_reduction_surplus": ("toxic_reduction_surplus",),
    }
    for canonical, source_aliases in aliases.items():
        converted = _metric_from_summary(
            panel,
            source_aliases,
            source=f"{panel_role}",
        )
        if converted is not None:
            metrics[canonical] = converted
    activity = panel.get("activity") or {}
    if "fills_retention" in activity:
        metrics["fills_retention"] = {
            "estimate": _finite(activity.get("fills_retention")),
            "source": f"{panel_role}.activity.fills_retention",
        }

    support = panel.get("support") or {}
    oof_rows = int(_finite(support.get("oof_rows", support.get("rows", 0)), 0.0))
    unsupported_rows = int(
        _finite(support.get("unsupported_candidate_rows", 0), 0.0)
    )
    family_failures = list(result.get(f"{panel_role}_failures") or ())
    reported_gate_passed = result.get(f"{panel_role}_gate_passed")
    if reported_gate_passed is False and not family_failures:
        family_failures.append("family_reported_gate_failure_without_details")
    if (
        panel_role == "sealed_holdout"
        and "sealed_holdout_failed" in str(result.get("status", ""))
        and not family_failures
    ):
        family_failures.append("family_reported_gate_failure_without_details")
    return {
        "schema_version": CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": str(result.get("family_id", "")),
        "family_id": str(result.get("family_id", "")),
        "panel_role": panel_role,
        "score_profile_contract": score_contract or {},
        "input_identity": {
            "result_schema_version": str(result.get("schema_version", "")),
            "family_spec": dict(result.get("family_spec") or {}),
            panel_identity_key: dict(result.get(panel_identity_key) or {}),
            "baseline": dict(baseline),
        },
        "validity_failures": validity_failures,
        "support": {
            "n_rows": oof_rows,
            "n_days": int(
                _finite(support.get("oof_days", support.get("days", 0)), 0.0)
            ),
            "effective_sample_size": _finite(support.get("policy_ess"), 0.0),
            "minimum_behavior_propensity": _finite(
                support.get("min_behavior_propensity"), 0.0
            ),
            "unsupported_mass": unsupported_rows / max(oof_rows, 1),
            "overlap_violations": int(
                _finite(support.get("overlap_violations", 0), 0.0)
            ),
            "failures": list(support.get("failures") or ()),
        },
        "candidate_rate": _finite(support.get("candidate_rate")),
        "invariant_violations": invariant_violations,
        "family_gate_failures": family_failures,
        "metrics": metrics,
        "requested_profile_id": profile_id,
    }


def score_action_family_result(
    result: Mapping[str, Any],
    family_spec: Mapping[str, Any],
    *,
    panel_role: str,
    profile_id: str,
    require_frozen_profile: bool = True,
) -> dict[str, Any]:
    evidence = action_family_score_evidence(
        result,
        family_spec,
        panel_role=panel_role,
        profile_id=profile_id,
    )
    return score_canonical_evidence(
        evidence,
        profile_id=profile_id,
        require_frozen_profile=require_frozen_profile,
    )


def paired_selection_score_evidence(
    row: Mapping[str, Any],
    *,
    score_profile_contract_value: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Adapt one ``paired_daily_selection`` row to a screening scorecard."""

    n_days = int(_finite(row.get("n_days"), 0.0))
    tail_balance = (
        (_finite(row.get("tail_better_days"), 0.0) - _finite(row.get("tail_worse_days"), 0.0))
        / max(n_days, 1)
    )
    action_drift = max(
        abs(_finite(row.get("pause_rate_delta"), 0.0)),
        abs(_finite(row.get("keep_rate_delta"), 0.0)),
        abs(_finite(row.get("place_replace_rate_delta"), 0.0)),
    )
    mechanism_notes = str(row.get("mechanism_notes", ""))
    mechanism_failures = []
    if not bool(row.get("mechanism_pass", False)):
        mechanism_failures.extend(
            item for item in mechanism_notes.split(",") if item and item != "pass"
        )
    return {
        "schema_version": CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": str(row.get("arm", "")),
        "family_id": str(row.get("group", "paired_parameter_screen")),
        "panel_role": "screening",
        "score_profile_contract": dict(score_profile_contract_value or {}),
        "input_identity": {
            "arm": str(row.get("arm", "")),
            "baseline_arm": str(row.get("baseline_arm", "")),
            "coverage": _finite(row.get("coverage")),
        },
        "validity_failures": [],
        "support": {
            "n_rows": 0,
            "n_days": n_days,
            "effective_sample_size": 0.0,
            "minimum_behavior_propensity": 0.0,
            "unsupported_mass": 0.0,
            "overlap_violations": 0,
            "failures": [],
        },
        "candidate_rate": 0.5,
        "invariant_violations": [],
        "family_gate_failures": mechanism_failures,
        "metrics": {
            "raw_paired_t": {"estimate": _finite(row.get("raw_t_stat"))},
            "terminal_paired_t": {
                "estimate": _finite(row.get("terminal_t_stat"))
            },
            "inventory_adjusted_paired_t": {
                "estimate": _finite(row.get("inv_adj_t_stat"))
            },
            "tail_day_balance": {"estimate": tail_balance},
            "bad_campaign_rate_avoidance": {
                "estimate": -_finite(row.get("bad_campaign_rate_delta"), 0.0)
            },
            "campaign_mae_avoidance_ratio": {
                "estimate": 1.0 - _finite(row.get("campaign_mae_ratio"), 1.0)
            },
            "repair_rate_uplift": {
                "estimate": _finite(row.get("repair_rate_delta"), 0.0)
            },
            "duration_avoidance_ratio": {
                "estimate": 1.0 - _finite(row.get("campaign_duration_ratio"), 1.0)
            },
            "fills_retention": {"estimate": _finite(row.get("fills_ratio"))},
            "inventory_time_avoidance_ratio": {
                "estimate": 1.0 - _finite(row.get("inventory_time_ratio"), 1.0)
            },
            "action_mix_drift": {"estimate": action_drift},
        },
    }


def paired_screen_v2_score_evidence(
    row: Mapping[str, Any],
    *,
    score_profile_contract_value: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Adapt pure paired evidence to the canonical v2 screening contract."""

    profile = score_profile("paired_screen_v2")
    gates = dict(profile.hard_gate_parameters)
    arm = str(row.get("arm", ""))
    baseline_arm = str(row.get("baseline_arm", ""))
    validity_failures: list[str] = []
    if not arm:
        validity_failures.append("arm_identity_missing")
    if not baseline_arm:
        validity_failures.append("baseline_identity_missing")

    coverage = _finite(row.get("coverage"), -math.inf)
    fills_ratio = _finite(row.get("fills_ratio"), -math.inf)
    support_failures: list[str] = []
    if coverage < gates["minimum_coverage"]:
        support_failures.append("incomplete_day_coverage")
    if fills_ratio < gates["minimum_fills_ratio"]:
        support_failures.append("fills_retention_below_support_floor")
    if fills_ratio > gates["maximum_fills_ratio"]:
        support_failures.append("fills_activity_above_support_ceiling")

    family_failures: list[str] = []
    campaign_ratio = _finite(row.get("campaign_ratio"), math.inf)
    if not (
        gates["minimum_campaign_ratio"]
        <= campaign_ratio
        <= gates["maximum_campaign_ratio"]
    ):
        family_failures.append("campaign_count_drift")
    for field, gate_name, failure in (
        ("pause_rate_delta", "maximum_pause_rate_delta", "pause_drift"),
        ("keep_rate_delta", "maximum_keep_rate_delta", "keep_drift"),
        (
            "place_replace_rate_delta",
            "maximum_place_replace_rate_delta",
            "place_replace_drift",
        ),
        ("final_spread_delta", "maximum_spread_delta", "spread_drift"),
    ):
        if abs(_finite(row.get(field), math.inf)) > gates[gate_name]:
            family_failures.append(failure)
    if _finite(row.get("side_min_fill_share"), -math.inf) < gates[
        "minimum_side_fill_share"
    ]:
        family_failures.append("side_split_drift")

    adapted = paired_selection_score_evidence(
        {
            **dict(row),
            "mechanism_pass": True,
            "mechanism_notes": "pass",
        },
        score_profile_contract_value=score_profile_contract_value,
    )
    adapted["validity_failures"] = validity_failures
    adapted["support"]["failures"] = support_failures
    adapted["family_gate_failures"] = family_failures
    adapted["input_identity"] = {
        **dict(adapted["input_identity"]),
        "evidence_contract": "paired_daily_evidence_v2",
    }
    adapted["paired_screening_summary"] = {
        name: _finite(row.get(name), 0.0)
        for name in (
            "raw_delta_sum",
            "terminal_delta_sum",
            "activity_adjusted_raw_delta",
            "campaign_adjusted_terminal_delta",
            "tail_campaign_delta",
        )
    }
    return adapted


def render_scorecard_markdown(scorecard: Mapping[str, Any]) -> str:
    lines = [
        f"# Experiment Scorecard: {scorecard.get('experiment_id', '')}",
        "",
        f"- Profile: `{scorecard['profile']['profile_id']}`",
        f"- Panel: `{scorecard.get('panel_role', '')}`",
        f"- Promotion status: **{scorecard.get('promotion_status', '')}**",
        f"- Economic classification: **{scorecard.get('economic_classification', '')}**",
        f"- Ranking eligible: **{scorecard.get('ranking_eligible', False)}**",
        f"- Total score: `{float(scorecard.get('total_score', 0.0)):+.4f}`",
        "",
        "## Gates",
        "",
        f"- Validity: `{scorecard['validity']['passed']}` {scorecard['validity']['failures']}",
        f"- Support: `{scorecard['support']['passed']}` {scorecard['support']['failures']}",
        f"- Hard gates: `{scorecard['hard_gates']['passed']}` {scorecard['hard_gates']['failures']}",
        "",
        "## Components",
        "",
        "| Component | Weight | Score | Contribution |",
        "|---|---:|---:|---:|",
    ]
    for name, row in sorted(scorecard.get("components", {}).items()):
        lines.append(
            f"| {name} | {row['weight']:.3f} | {row['score']:+.3f} | "
            f"{row['contribution_after_shrink']:+.4f} |"
        )
    lines.extend(
        [
            "",
            "The weighted score ranks only candidates that pass every validity, support, "
            "and hard-risk gate. It is not a substitute for Validation or sealed holdout.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    profiles = subparsers.add_parser("profiles", help="print built-in profile identities")
    profiles.add_argument("--output", type=Path)

    action = subparsers.add_parser(
        "action-summary", help="score a side-specific action/OPE result"
    )
    action.add_argument("--summary", type=Path, required=True)
    action.add_argument("--family-spec", type=Path, required=True)
    action.add_argument("--panel", choices=("development", "validation", "sealed_holdout"), default="development")
    action.add_argument("--profile", choices=tuple(PROFILES), required=True)
    action.add_argument("--allow-retrofit-profile", action="store_true")
    action.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "profiles":
        payload = {
            profile_id: {
                **score_profile(profile_id).payload(),
                **score_profile_contract(profile_id),
            }
            for profile_id in sorted(PROFILES)
        }
        if args.output:
            _write_json(args.output.expanduser().resolve(), payload)
        else:
            print(json.dumps(payload, indent=2, sort_keys=True))
        return

    result = json.loads(args.summary.expanduser().resolve().read_text(encoding="utf-8"))
    family_spec = json.loads(
        args.family_spec.expanduser().resolve().read_text(encoding="utf-8")
    )
    scorecard = score_action_family_result(
        result,
        family_spec,
        panel_role=args.panel,
        profile_id=args.profile,
        require_frozen_profile=not args.allow_retrofit_profile,
    )
    output = args.output.expanduser().resolve()
    _write_json(output, scorecard)
    output.with_suffix(".md").write_text(
        render_scorecard_markdown(scorecard), encoding="utf-8"
    )
    print(json.dumps(scorecard, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
