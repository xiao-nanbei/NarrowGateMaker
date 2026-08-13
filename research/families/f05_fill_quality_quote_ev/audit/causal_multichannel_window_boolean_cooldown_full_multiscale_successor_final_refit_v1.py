"""Freeze a post-OOF successor artifact without confusing its evidence identity.

The learning algorithm is evaluated by nested chronological OOF.  Only after
that report passes the preregistered ladder may this module refit one policy on
all new Development rows.  The resulting bytes have no OOF evidence of their
own and require later prospective confirmation.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 import (
    OOF_EVIDENCE_SCOPE,
    CandidateLadderEntry,
    FittedCandidate,
    NestedOofExecutionResult,
    NestedOofPanel,
    _fit_boolean_candidate,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 import (
    IDENTITY as SUCCESSOR_IDENTITY,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 import (
    SCORE_PROFILE_CONTRACT,
    ProspectiveFoldManifest,
    ResearchBooleanCooldownPolicyEvaluator,
    SuccessorSearchProfile,
    audit_policy_semantics,
)
from strategy.boolean_cooldown_successor import InactiveSuccessorCooldownEvaluator

IDENTITY = f"{SUCCESSOR_IDENTITY}.final_refit.v1"
FINAL_ARTIFACT_EVIDENCE_SCOPE = "full_development_refit_requires_later_prospective_confirmation"


class FinalRefitError(ValueError):
    """Raised when OOF evidence or refit bytes violate the frozen contract."""


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float) and not math.isfinite(value):
        raise FinalRefitError("non-finite value cannot enter a hash-bound artifact")
    return value


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        _jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class FinalRefitGateConfig:
    economic_epsilon_usdc: float = 0.0
    fill_retention_min: float = 0.85
    fill_retention_max: float = 1.2
    candidate_rate_min: float = 0.05
    candidate_rate_max: float = 0.75
    minimum_common_rows: int = 200
    minimum_effective_sample_size: float = 100.0
    minimum_daily_positive_rate: float = 0.55
    maximum_unsupported_mass: float = 0.05
    risk_noninferiority_tolerance: float = 0.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.economic_epsilon_usdc):
            raise FinalRefitError("economic epsilon must be finite")
        if not 0.0 < self.fill_retention_min <= self.fill_retention_max:
            raise FinalRefitError("fill-retention interval is invalid")
        if not 0.0 <= self.candidate_rate_min <= self.candidate_rate_max <= 1.0:
            raise FinalRefitError("candidate-rate interval is invalid")
        if self.minimum_common_rows < 1 or self.minimum_effective_sample_size <= 0.0:
            raise FinalRefitError("support minima must be positive")
        if not 0.0 <= self.minimum_daily_positive_rate <= 1.0:
            raise FinalRefitError("daily-positive-rate gate is invalid")
        if not 0.0 <= self.maximum_unsupported_mass <= 1.0:
            raise FinalRefitError("unsupported-mass gate is invalid")
        if self.risk_noninferiority_tolerance < 0.0:
            raise FinalRefitError("risk tolerance must be nonnegative")


@dataclass(frozen=True, slots=True)
class GateCheck:
    name: str
    passed: bool
    observed: Any
    required: str


@dataclass(frozen=True, slots=True)
class FinalRefitSelection:
    side: str
    selected_candidate: str | None
    advancement_path: tuple[str, ...]
    checks: tuple[GateCheck, ...]
    oof_report_sha256: str
    learning_algorithm_oof_supported: bool
    exact_final_artifact_oof_available: bool = False


@dataclass(frozen=True, slots=True)
class FinalRefitBundle:
    selection: FinalRefitSelection
    fitted_candidate: FittedCandidate
    artifact: Mapping[str, Any]
    artifact_sha256: str


@dataclass(frozen=True, slots=True)
class CompiledPolicyParityAudit:
    side: str
    policy_sha256: str
    predicate_bundle_sha256: str
    predicate_count: int
    deterministic_case_count: int
    mismatch_count: int
    warmup_fallback_valid: bool
    stale_fallback_valid: bool
    missing_column_fallback_valid: bool
    active_live_wiring_changed: bool = False


def _positive_band_checks(
    result: NestedOofExecutionResult,
    *,
    family: str,
    hypothesis: str,
    epsilon: float,
) -> tuple[GateCheck, GateCheck]:
    if family == "candidate":
        day_family = result.candidate_bands
        week_family = result.candidate_week_bands
    elif family == "confirmatory":
        day_family = result.confirmatory_bands
        week_family = result.confirmatory_week_bands
    else:  # pragma: no cover - internal contract guard.
        raise FinalRefitError(f"unknown simultaneous family {family!r}")
    try:
        day = day_family.bands[hypothesis]
        week = week_family.bands[hypothesis]
    except KeyError as exc:
        raise FinalRefitError(f"missing simultaneous hypothesis {hypothesis!r}") from exc
    return (
        GateCheck(
            name=f"{hypothesis}:day_lcb",
            passed=day.lcb_usdc > epsilon,
            observed=day.lcb_usdc,
            required=f"> {epsilon}",
        ),
        GateCheck(
            name=f"{hypothesis}:week_lcb",
            passed=week.lcb_usdc > epsilon,
            observed=week.lcb_usdc,
            required=f"> {epsilon}",
        ),
    )


def _continuous_nondominance_checks(
    result: NestedOofExecutionResult,
    *,
    side: str,
    candidate: str,
    epsilon: float,
) -> tuple[GateCheck, GateCheck]:
    hypothesis = f"successor:{side}:CONTINUOUS-{candidate}"
    try:
        day = result.confirmatory_bands.bands[hypothesis]
        week = result.confirmatory_week_bands.bands[hypothesis]
    except KeyError as exc:
        raise FinalRefitError(
            f"continuous comparator was not paired with {candidate}"
        ) from exc
    return (
        GateCheck(
            name=f"{hypothesis}:day_not_dominated",
            passed=day.lcb_usdc <= epsilon,
            observed=day.lcb_usdc,
            required=f"<= {epsilon}",
        ),
        GateCheck(
            name=f"{hypothesis}:week_not_dominated",
            passed=week.lcb_usdc <= epsilon,
            observed=week.lcb_usdc,
            required=f"<= {epsilon}",
        ),
    )


def _risk_band_checks(
    result: NestedOofExecutionResult,
    *,
    side: str,
    candidate: str,
    epsilon: float,
    tolerance: float,
) -> tuple[GateCheck, ...]:
    requirements = {
        "closed_campaign_value": (epsilon, True),
        "campaign_q10": (-tolerance, False),
        "campaign_cvar10": (-tolerance, False),
        "negative_terminal_protection": (-tolerance, False),
        "campaign_mae_avoidance": (-tolerance, False),
        "repair_event": (-tolerance, False),
        "repair_time_avoidance_s": (-tolerance, False),
        "censoring_avoidance": (-tolerance, False),
        "inventory_time_avoidance": (-tolerance, False),
        "max_abs_inventory_avoidance": (-tolerance, False),
    }
    checks: list[GateCheck] = []
    for metric, (minimum, strict) in requirements.items():
        hypothesis = f"{side}:{candidate}:{metric}"
        try:
            day = result.risk_bands.bands[hypothesis]
            week = result.risk_week_bands.bands[hypothesis]
        except KeyError as exc:
            raise FinalRefitError(
                f"missing risk simultaneous hypothesis {hypothesis!r}"
            ) from exc
        for unit, band in (("day", day), ("week", week)):
            passed = band.lcb_usdc > minimum if strict else band.lcb_usdc >= minimum
            operator = ">" if strict else ">="
            checks.append(
                GateCheck(
                    name=f"{hypothesis}:{unit}_lcb",
                    passed=passed,
                    observed=band.lcb_usdc,
                    required=f"{operator} {minimum}",
                )
            )
    return tuple(checks)


def _candidate_report_checks(
    result: NestedOofExecutionResult,
    *,
    side: str,
    candidate: str,
    config: FinalRefitGateConfig,
) -> tuple[GateCheck, ...]:
    key = f"{side}:{candidate}"
    if key not in result.candidate_reports:
        raise FinalRefitError(f"missing candidate report {key}")
    report = result.candidate_reports[key]
    def finite(value: Any) -> float:
        converted = float(value)
        if not math.isfinite(converted):
            raise FinalRefitError(f"candidate report {key} contains a non-finite gate value")
        return converted

    identified = int(report.get("identified_days", 0))
    total = int(report.get("outer_test_days", 0))
    feature_ready = int(report.get("feature_ready_active_days", 0))
    candidate_rate = finite(report.get("nonbaseline_action_rate"))
    daily_positive_rate = finite(report.get("daily_positive_rate"))
    unsupported_mass = finite(report.get("unsupported_mass"))
    effective_sample_size = finite(report.get("paired_effective_sample_size"))
    fill_retention = report.get("fill_retention")
    leave_one = report.get("leave_one_top_day", {}).get("mean_usdc")
    leave_two = report.get("leave_two_top_days", {}).get("mean_usdc")
    retention = finite(fill_retention)
    leave_one_value = finite(leave_one)
    leave_two_value = finite(leave_two)
    return (
        GateCheck("all_outer_days_point_identified", identified == total, [identified, total], "equal"),
        GateCheck("all_outer_days_feature_ready", feature_ready == total, [feature_ready, total], "equal"),
        GateCheck(
            "nonbaseline_action_support",
            int(report.get("nonbaseline_action_count", 0)) > 0,
            int(report.get("nonbaseline_action_count", 0)),
            "> 0",
        ),
        GateCheck(
            "candidate_rate",
            config.candidate_rate_min <= candidate_rate <= config.candidate_rate_max,
            candidate_rate,
            f"in [{config.candidate_rate_min}, {config.candidate_rate_max}]",
        ),
        GateCheck(
            "daily_positive_rate",
            daily_positive_rate >= config.minimum_daily_positive_rate,
            daily_positive_rate,
            f">= {config.minimum_daily_positive_rate}",
        ),
        GateCheck(
            "unsupported_mass",
            unsupported_mass <= config.maximum_unsupported_mass,
            unsupported_mass,
            f"<= {config.maximum_unsupported_mass}",
        ),
        GateCheck(
            "common_row_denominator",
            int(report.get("common_row_count", 0)) >= config.minimum_common_rows,
            int(report.get("common_row_count", 0)),
            f">= {config.minimum_common_rows}",
        ),
        GateCheck(
            "common_campaign_denominator",
            int(report.get("common_campaign_count", 0)) > 0,
            int(report.get("common_campaign_count", 0)),
            "> 0",
        ),
        GateCheck(
            "paired_effective_sample_size",
            effective_sample_size >= config.minimum_effective_sample_size,
            effective_sample_size,
            f">= {config.minimum_effective_sample_size}",
        ),
        GateCheck(
            "minimum_behavior_propensity",
            finite(report.get("minimum_behavior_propensity")) >= 0.05,
            finite(report.get("minimum_behavior_propensity")),
            ">= 0.05",
        ),
        GateCheck(
            "overlap_violations",
            int(report.get("overlap_violations", -1)) == 0,
            int(report.get("overlap_violations", -1)),
            "== 0",
        ),
        GateCheck(
            "fill_retention",
            config.fill_retention_min <= retention <= config.fill_retention_max,
            retention,
            f"in [{config.fill_retention_min}, {config.fill_retention_max}]",
        ),
        GateCheck(
            "leave_one_top_day_positive",
            leave_one_value > config.economic_epsilon_usdc,
            leave_one_value,
            f"> {config.economic_epsilon_usdc}",
        ),
        GateCheck(
            "leave_two_top_days_positive",
            leave_two_value > config.economic_epsilon_usdc,
            leave_two_value,
            f"> {config.economic_epsilon_usdc}",
        ),
    )


def _record_predicates(record: Mapping[str, Any]) -> tuple[str, ...]:
    policy = record.get("policy")
    if not isinstance(policy, Mapping):
        return ()
    decision = policy.get("decision_policy")
    if not isinstance(decision, Mapping):
        return ()
    names: set[str] = set()
    for rule in decision.get("ordered_first_match_rules", ()):  # type: ignore[union-attr]
        for clause in rule.get("clauses", ()):
            for literal in clause.get("literals", ()):
                name = literal.get("predicate")
                if isinstance(name, str):
                    names.add(name)
    return tuple(sorted(names))


def _is_true_m2_predicate(name: str) -> bool:
    lowered = name.lower()
    return any(
        token in lowered
        for token in ("trade", "flow", "taker", "depth", "refill", "depletion", "queue")
    )


def _m2_fold_semantics_check(
    result: NestedOofExecutionResult,
    *,
    side: str,
) -> GateCheck:
    relevant = [
        fold
        for fold in result.fold_records
        if str(fold.get("side", "")).upper() == side
    ]
    used = 0
    for fold in relevant:
        candidates = fold.get("candidates")
        if not isinstance(candidates, Mapping):
            raise FinalRefitError("outer fold candidate records are malformed")
        record = candidates.get("M2_TRUE_INCREMENTAL")
        if not isinstance(record, Mapping):
            raise FinalRefitError("outer fold lacks M2 candidate semantics")
        if any(_is_true_m2_predicate(name) for name in _record_predicates(record)):
            used += 1
    return GateCheck(
        "M2_outer_policies_use_true_incremental_features",
        bool(relevant) and used == len(relevant),
        {"using_true_m2": used, "outer_folds": len(relevant)},
        "every outer-fold M2 policy uses trade/flow/depth predicates",
    )


def _all_pass(checks: Sequence[GateCheck]) -> bool:
    return bool(checks) and all(check.passed for check in checks)


def select_final_refit_candidate(
    result: NestedOofExecutionResult,
    *,
    side: str,
    config: FinalRefitGateConfig | None = None,
) -> FinalRefitSelection:
    """Select the highest preregistered rung supported by learning-algorithm OOF."""

    config = FinalRefitGateConfig() if config is None else config
    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise FinalRefitError("side must be BUY or SELL")
    if (
        result.evidence_scope != OOF_EVIDENCE_SCOPE
        or result.exact_final_artifact_oof_available
        or result.final_refit_performed
    ):
        raise FinalRefitError("nested OOF evidence identity drifted")

    all_checks: list[GateCheck] = []
    advancement: list[str] = []

    def common_candidate_checks(candidate: str) -> list[GateCheck]:
        checks = list(
            _positive_band_checks(
                result,
                family="candidate",
                hypothesis=f"{normalized_side}:{candidate}",
                epsilon=config.economic_epsilon_usdc,
            )
        )
        checks.extend(
            _positive_band_checks(
                result,
                family="confirmatory",
                hypothesis=f"successor:{normalized_side}:{candidate}-ACTION_MATCHED",
                epsilon=config.economic_epsilon_usdc,
            )
        )
        checks.extend(
            _continuous_nondominance_checks(
                result,
                side=normalized_side,
                candidate=candidate,
                epsilon=config.economic_epsilon_usdc,
            )
        )
        checks.extend(
            _candidate_report_checks(
                result,
                side=normalized_side,
                candidate=candidate,
                config=config,
            )
        )
        checks.extend(
            _risk_band_checks(
                result,
                side=normalized_side,
                candidate=candidate,
                epsilon=config.economic_epsilon_usdc,
                tolerance=config.risk_noninferiority_tolerance,
            )
        )
        scorecard = result.scorecards.get(f"{normalized_side}:{candidate}")
        if not isinstance(scorecard, Mapping):
            raise FinalRefitError(f"missing canonical scorecard for {candidate}")
        checks.extend(
            (
                GateCheck(
                    name=f"{candidate}:canonical_scorecard_hard_gates",
                    passed=bool(scorecard.get("hard_gates", {}).get("passed", False)),
                    observed=scorecard.get("hard_gates", {}).get("failures", []),
                    required="all action_alpha_v1 hard gates pass",
                ),
                GateCheck(
                    name=f"{candidate}:canonical_scorecard_ranking",
                    passed=bool(scorecard.get("ranking_eligible", False))
                    and scorecard.get("ranking_score") is not None,
                    observed=scorecard.get("ranking_score"),
                    required="finite non-null canonical ranking score",
                ),
            )
        )
        return checks

    e1_checks = common_candidate_checks("E1_FULL_EMA_BANK")
    for comparison in ("E1-B0", "E1-B1", "E1-B2", "E1-B3"):
        e1_checks.extend(
            _positive_band_checks(
                result,
                family="confirmatory",
                hypothesis=f"successor:{normalized_side}:{comparison}",
                epsilon=config.economic_epsilon_usdc,
            )
        )
    all_checks.extend(e1_checks)
    if not _all_pass(e1_checks):
        return FinalRefitSelection(
            side=normalized_side,
            selected_candidate=None,
            advancement_path=(),
            checks=tuple(all_checks),
            oof_report_sha256=_canonical_sha256(result.report()),
            learning_algorithm_oof_supported=False,
        )
    advancement.append("E1_FULL_EMA_BANK")

    incremental = (
        ("E2_DIRECTIONAL_EMA", "E2-E1"),
        ("E3_HIGHER_ORDER_BOOLEAN", "E3-E2"),
        ("M2_TRUE_INCREMENTAL", "M2-E3"),
    )
    for candidate, comparison in incremental:
        checks = common_candidate_checks(candidate)
        checks.extend(
            _positive_band_checks(
                result,
                family="confirmatory",
                hypothesis=f"successor:{normalized_side}:{comparison}",
                epsilon=config.economic_epsilon_usdc,
            )
        )
        if candidate == "M2_TRUE_INCREMENTAL":
            checks.append(_m2_fold_semantics_check(result, side=normalized_side))
        all_checks.extend(checks)
        if not _all_pass(checks):
            break
        advancement.append(candidate)

    return FinalRefitSelection(
        side=normalized_side,
        selected_candidate=advancement[-1],
        advancement_path=tuple(advancement),
        checks=tuple(all_checks),
        oof_report_sha256=_canonical_sha256(result.report()),
        learning_algorithm_oof_supported=True,
    )


def _profile_complexity(profile: SuccessorSearchProfile) -> tuple[int, ...]:
    return (
        profile.feature_budget,
        profile.max_depth,
        profile.max_leaf_nodes,
        profile.max_rules,
        profile.max_clauses_per_rule,
        profile.max_literals_per_clause,
        profile.min_samples_leaf,
    )


def _select_refit_profile(
    result: NestedOofExecutionResult,
    *,
    side: str,
    entry: CandidateLadderEntry,
) -> tuple[SuccessorSearchProfile, Mapping[str, int]]:
    names: list[str] = []
    for fold in result.fold_records:
        if str(fold.get("side", "")).upper() != side:
            continue
        candidates = fold.get("candidates")
        if not isinstance(candidates, Mapping):
            raise FinalRefitError("outer fold candidate records are malformed")
        record = candidates.get(entry.name)
        if not isinstance(record, Mapping):
            raise FinalRefitError(f"outer fold lacks {entry.name}")
        names.append(str(record.get("selected_profile", "")))
    if not names:
        raise FinalRefitError("no fold-specific profile identity is available")
    counts = Counter(names)
    profiles = {profile.name: profile for profile in entry.profiles}
    unknown = set(counts) - set(profiles)
    if unknown:
        raise FinalRefitError(f"outer folds selected unknown profiles: {sorted(unknown)}")
    chosen = min(
        profiles.values(),
        key=lambda profile: (-counts[profile.name], _profile_complexity(profile), profile.name),
    )
    return chosen, dict(sorted(counts.items()))


def _lock_timestamp(value: str, active_days: Sequence[str]) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise FinalRefitError("final artifact lock timestamp is invalid") from exc
    if timestamp.tzinfo is None:
        raise FinalRefitError("final artifact lock timestamp must include UTC timezone")
    timestamp = timestamp.tz_convert("UTC")
    last_day_end = pd.Timestamp(max(active_days), tz="UTC") + pd.Timedelta(days=1)
    if timestamp < last_day_end:
        raise FinalRefitError("final artifact was locked before Development closed")
    return timestamp.isoformat().replace("+00:00", "Z")


def _parity_cases(policy: Any) -> tuple[Mapping[str, int], ...]:
    names = tuple(policy.predicate_columns)
    cases: list[dict[str, int]] = [
        {name: state for name in names} for state in (-1, 0, 1)
    ]
    for name in names:
        for state in (-1, 0, 1):
            row = {candidate: 0 for candidate in names}
            row[name] = state
            cases.append(row)
    for rule in policy.rules:
        for clause in rule.clauses:
            witness = {name: 0 for name in names}
            for literal in clause.literals:
                witness[literal.predicate] = 0 if literal.negated else 1
            cases.append(witness)
            for literal in clause.literals:
                unobserved = dict(witness)
                unobserved[literal.predicate] = -1
                cases.append(unobserved)
    unique: dict[str, Mapping[str, int]] = {}
    for row in cases:
        unique[_canonical_sha256(row)] = row
    return tuple(unique[key] for key in sorted(unique))


def audit_compiled_policy_research_runtime_parity(
    fitted: FittedCandidate,
    *,
    predicate_bundle_sha256: str,
    baseline_duration_ms: int = 170_000,
) -> CompiledPolicyParityAudit:
    """Compare a frozen research policy with an inactive runtime compiler."""

    if fitted.policy is None:
        raise FinalRefitError("exact-owner control has no successor policy to compile")
    policy = fitted.policy
    policies = {"BUY": None, "SELL": None}
    policies[fitted.side] = policy
    research = ResearchBooleanCooldownPolicyEvaluator(
        policies=policies,
        policy_identity=IDENTITY,
        policy_sha256=fitted.policy_sha256,
        predicate_bundle_sha256=predicate_bundle_sha256,
    )
    runtime = InactiveSuccessorCooldownEvaluator.from_policy_payload(
        policy.payload(),
        policy_sha256=fitted.policy_sha256,
        predicate_bundle_sha256=predicate_bundle_sha256,
    )
    cases = _parity_cases(policy)
    mismatches = 0
    for offset, row in enumerate(cases):
        research_decision = research.evaluate_predicates(
            side=fitted.side,
            predicate_values=row,
            baseline_duration_ms=baseline_duration_ms,
            snapshot_id=f"parity-{offset}",
        )
        runtime_decision = runtime.evaluate_predicates(
            side=fitted.side,
            predicate_values=row,
            baseline_duration_ms=baseline_duration_ms,
        )
        if (
            research_decision.action_id,
            research_decision.duration_ms,
            research_decision.fallback_reason,
            research_decision.matched_rule_index,
            research_decision.support_valid,
        ) != (
            runtime_decision.action_id,
            runtime_decision.duration_ms,
            runtime_decision.fallback_reason,
            runtime_decision.matched_rule_index,
            runtime_decision.support_valid,
        ):
            mismatches += 1
    supported = {name: 0 for name in policy.predicate_columns}
    warmup = runtime.evaluate_predicates(
        side=fitted.side,
        predicate_values=supported,
        baseline_duration_ms=baseline_duration_ms,
        feature_ready=False,
    )
    stale = runtime.evaluate_predicates(
        side=fitted.side,
        predicate_values=supported,
        baseline_duration_ms=baseline_duration_ms,
        stale=True,
    )
    missing = dict(supported)
    missing.pop(next(iter(policy.predicate_columns)))
    missing_decision = runtime.evaluate_predicates(
        side=fitted.side,
        predicate_values=missing,
        baseline_duration_ms=baseline_duration_ms,
    )
    return CompiledPolicyParityAudit(
        side=fitted.side,
        policy_sha256=fitted.policy_sha256,
        predicate_bundle_sha256=predicate_bundle_sha256,
        predicate_count=len(policy.predicate_columns),
        deterministic_case_count=len(cases),
        mismatch_count=mismatches,
        warmup_fallback_valid=(
            warmup.action_id == "CONTROL_85N"
            and warmup.fallback_reason == "warmup_incomplete"
            and not warmup.support_valid
        ),
        stale_fallback_valid=(
            stale.action_id == "CONTROL_85N"
            and stale.fallback_reason == "feature_stale"
            and not stale.support_valid
        ),
        missing_column_fallback_valid=(
            missing_decision.action_id == "CONTROL_85N"
            and missing_decision.fallback_reason == "runtime_predicate_columns_drifted"
            and not missing_decision.support_valid
        ),
    )


def refit_and_freeze_final_artifact(
    panel: NestedOofPanel,
    *,
    fold_manifest: ProspectiveFoldManifest,
    ladder: Sequence[CandidateLadderEntry],
    result: NestedOofExecutionResult,
    side: str,
    locked_at_utc: str,
    config: FinalRefitGateConfig | None = None,
) -> FinalRefitBundle:
    """Refit the selected algorithm on all new Development rows and freeze bytes."""

    config = FinalRefitGateConfig() if config is None else config
    selection = select_final_refit_candidate(result, side=side, config=config)
    if selection.selected_candidate is None:
        raise FinalRefitError("OOF did not support any successor final refit")
    panel.validate(active_days=fold_manifest.active_days, sides=(selection.side,))
    entries = {entry.name: entry for entry in ladder}
    if selection.selected_candidate not in entries:
        raise FinalRefitError("selected candidate is absent from the frozen ladder")
    entry = entries[selection.selected_candidate]
    if entry.kind != "boolean":
        raise FinalRefitError("only learned Boolean candidates may be final-refit")
    profile, profile_counts = _select_refit_profile(
        result,
        side=selection.side,
        entry=entry,
    )
    train_index = panel.metadata.index[panel.metadata["side"].astype(str).str.upper() == selection.side]
    fitted = _fit_boolean_candidate(
        panel,
        entry=entry,
        side=selection.side,
        train_index=train_index,
        fold_id="final_full_development_refit",
        profile=profile,
        random_seed=20260813,
    )
    fitted = replace(fitted, learning_algorithm_fold_specific=False)
    if fitted.policy is None:
        raise FinalRefitError("final refit unexpectedly produced an exact-owner policy")
    semantic = audit_policy_semantics(
        fitted.policy,
        candidate_source_block=selection.selected_candidate,
    )
    if selection.selected_candidate == "M2_TRUE_INCREMENTAL" and not semantic.uses_m2_incremental_features:
        raise FinalRefitError(
            "final M2 refit did not compile a true trade/flow/depth predicate"
        )
    parity = audit_compiled_policy_research_runtime_parity(
        fitted,
        predicate_bundle_sha256=_canonical_sha256(
            {
                "identity": SUCCESSOR_IDENTITY,
                "side": selection.side,
                "predicate_columns": list(fitted.policy.predicate_columns),
            }
        ),
    )
    if (
        parity.mismatch_count != 0
        or not parity.warmup_fallback_valid
        or not parity.stale_fallback_valid
        or not parity.missing_column_fallback_valid
    ):
        raise FinalRefitError("compiled research/runtime policy parity failed")
    locked = _lock_timestamp(locked_at_utc, fold_manifest.active_days)
    module_path = Path(__file__).resolve()
    nested_path = Path(
        __import__(
            "research.families.f05_fill_quality_quote_ev.audit."
            "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1",
            fromlist=["__file__"],
        ).__file__
    ).resolve()
    artifact = {
        "schema_version": IDENTITY,
        "identity": SUCCESSOR_IDENTITY,
        "side": selection.side,
        "selected_candidate": selection.selected_candidate,
        "advancement_path": list(selection.advancement_path),
        "selected_profile": profile.name,
        "fold_selected_profile_counts": profile_counts,
        "policy_sha256": fitted.policy_sha256,
        "policy": fitted.policy_payload,
        "training_days": list(fitted.training_days),
        "training_row_sha256": fitted.training_row_sha256,
        "prospective_fold_manifest_sha256": fold_manifest.manifest_sha256,
        "learning_algorithm_oof_report_sha256": selection.oof_report_sha256,
        "learning_algorithm_oof_supported": True,
        "score_profile_contract": SCORE_PROFILE_CONTRACT,
        "oof_evidence_scope": OOF_EVIDENCE_SCOPE,
        "exact_final_artifact_oof_available": False,
        "final_artifact_evidence_scope": FINAL_ARTIFACT_EVIDENCE_SCOPE,
        "final_artifact_locked_at_utc": locked,
        "semantic_audit": asdict(semantic),
        "compiled_research_runtime_parity": asdict(parity),
        "implementation": {
            "final_refit_module_sha256": _sha256_file(module_path),
            "nested_oof_module_sha256": _sha256_file(nested_path),
        },
        "permissions": {
            "prospective_confirmation_required": True,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "active_owner_policy_modified": False,
        },
    }
    artifact_sha256 = _canonical_sha256(artifact)
    artifact = {**artifact, "artifact_sha256": artifact_sha256}
    return FinalRefitBundle(
        selection=selection,
        fitted_candidate=fitted,
        artifact=artifact,
        artifact_sha256=artifact_sha256,
    )


def write_final_refit_artifact(bundle: FinalRefitBundle, output_path: str | Path) -> str:
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        _jsonable(bundle.artifact),
        sort_keys=True,
        indent=2,
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii") + b"\n"
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "FINAL_ARTIFACT_EVIDENCE_SCOPE",
    "IDENTITY",
    "CompiledPolicyParityAudit",
    "FinalRefitBundle",
    "FinalRefitError",
    "FinalRefitGateConfig",
    "FinalRefitSelection",
    "GateCheck",
    "audit_compiled_policy_research_runtime_parity",
    "refit_and_freeze_final_artifact",
    "select_final_refit_candidate",
    "write_final_refit_artifact",
]
