"""Mechanics contract for the post-v3 full-multiscale cooldown successor.

This module is intentionally outcome-storage free and runtime inactive.  It
implements the corrected discovery primitives needed by the successor study:

* inner-train-only feature-pool construction;
* per-action identified targets without neutral-zero imputation;
* bounded, interaction-capable ordered Boolean policy compilation;
* the complete M0/M1/M2/continuous hierarchy;
* exact-owner-policy semantic and coverage audits; and
* conservative pre-activation GTX exposure classification.

It does not read Development, Validation, or holdout outcomes and it does not
modify or authorize the active owner policy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from itertools import combinations, product
from pathlib import Path
from threading import Lock
from typing import Any

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeRegressor

from models.audit.experiment_scorecard import score_profile_contract
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_successor_transport_adapter_v1 as transport_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    NestedOofContractError,
    TriLiteral,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference import (
    CensoringSensitivity,
    HierarchyDecision,
    HierarchyStepDecision,
    SimultaneousBand,
    SimultaneousBandFamily,
    apply_feature_hierarchy,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_runtime_policy import (
    CooldownDurationDecision,
    load_runtime_policy,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    CooldownAssignmentSnapshotV2,
)
from strategy.boolean_cooldown_coverage import (
    BooleanCooldownCoverageReason as CoverageReason,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1"
PREREGISTRATION_CUTOFF_UTC = "2026-08-12T17:45:29Z"
FIRST_ELIGIBLE_FULL_UTC_DAY = "2026-08-13"
MINIMUM_ACTIVE_TREATMENT_DAYS = 30
SCORE_PROFILE_ID = "action_alpha_v1"
SCORE_PROFILE_CONTRACT = score_profile_contract(SCORE_PROFILE_ID)

ACTIVE_OWNER_POLICY_IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_policy_v1"
ACTIVE_OWNER_POLICY_SHA256 = (
    "877a20033ff678bd7aa9b58069f37c3dc459b18db78c316b7e50023248f15a29"
)
ACTIVE_PREDICATE_BUNDLE_SHA256 = (
    "ba4c1bac2380564aa24d47d12796f3be5c0312cc88d28218ce84bd20e4170f37"
)

EMA_HALF_LIVES_S = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0)
EMA_PAIRS_S = tuple(combinations(EMA_HALF_LIVES_S, 2))
if len(EMA_PAIRS_S) != 45:  # pragma: no cover - immutable contract guard.
    raise RuntimeError("full EMA bank must contain exactly 45 fast/slow pairs")

CURRENT_SHORT_CROSS = "predicate::ema_pair_h4s_h16s:cross_age_le_slow"
CURRENT_LONG_CROSS = "predicate::ema_pair_h16s_h256s:cross_age_le_fast"
CURRENT_CAMPAIGN_AGE = "predicate::m0::campaign_age_gt_control_duration"

SUCCESSOR_CANDIDATE_LADDER = (
    "B0_CURRENT_EXACT",
    "B1_CAMPAIGN_AGE_ONLY",
    "B2_CAMPAIGN_PLUS_H16_H256",
    "B3_CURRENT_SEMANTIC_EQUIVALENT",
    "E1_FULL_EMA_BANK",
    "E2_DIRECTIONAL_EMA",
    "E3_HIGHER_ORDER_BOOLEAN",
    "M2_TRUE_INCREMENTAL",
    "ACTION_MATCHED_CONTROLS",
)

COMPLETE_HIERARCHY_SUFFIXES = (
    "E1-B0",
    "E2-E1",
    "E3-E2",
    "M2-E3",
    "CONTINUOUS-BOOLEAN",
)

PROSPECTIVE_DAY_SOURCE_BUNDLE_SCHEMA = f"{IDENTITY}.prospective_day_source_bundle.v2"
LIFECYCLE_DAY_ADMISSION_SCHEMA = f"{IDENTITY}.lifecycle_day_admission.v2"
MARKET_DAY_ADMISSION_SCHEMA = f"{IDENTITY}.market_day_admission.v2"
DECISION_DAY_ADMISSION_SCHEMA = f"{IDENTITY}.decision_day_admission.v2"

_EMA_PAIR_RE = re.compile(
    r"ema_pair_h(?P<fast>[0-9]+(?:p[0-9]+)?)s_h"
    r"(?P<slow>[0-9]+(?:p[0-9]+)?)s"
)
_CANONICAL_EMA_PAIR_RE = re.compile(
    r"__h(?P<fast>[0-9]+(?:p[0-9]+)?)s__h"
    r"(?P<slow>[0-9]+(?:p[0-9]+)?)s(?:::|$)"
)
_FIXED_DURATION_ACTION_RE = re.compile(r"^FIXED_([1-9][0-9]*)S$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SuccessorContractError(ValueError):
    """Raised when a successor mechanics or evidence contract drifts."""


class ExposureEncoding(StrEnum):
    """Exposure status used by future lifecycle admission."""

    ACTIVATED_OR_EXPOSED = "activated_or_exposed"
    EXACT_ZERO_EXPOSURE = "exact_zero_exposure"
    CENSORED_UNKNOWN_EXPOSURE = "censored_unknown_exposure"


@dataclass(frozen=True, slots=True)
class CoverageClassification:
    reason: CoverageReason
    eligible: bool
    feature_ready: bool
    support_valid: bool
    nonbaseline_action: bool
    raw_fallback_reason: str | None


@dataclass(frozen=True, slots=True)
class GtxExposureClassification:
    encoding: ExposureEncoding
    coverage_reason: CoverageReason
    exact_zero_exposure: bool
    point_identified: bool
    reason: str


@dataclass(frozen=True, slots=True)
class FeaturePoolAudit:
    fold_id: str
    train_days: tuple[str, ...]
    train_rows: int
    identified_screen_rows: int
    candidate_count: int
    selected_count: int
    required_count: int
    selection_method: str
    candidate_sha256: str
    selected_sha256: str
    all_45_ema_pairs_eligible: bool
    eligible_ema_pair_count: int
    selected_ema_pair_count: int


@dataclass(frozen=True, slots=True)
class IdentifiedActionTargets:
    effects: pd.DataFrame
    identified: pd.DataFrame
    control_action: str
    actions: tuple[str, ...]
    unknown_target_policy: str = "missing_mask_never_neutral_zero"

    def __post_init__(self) -> None:
        if not self.effects.index.equals(self.identified.index):
            raise SuccessorContractError("effect and identification rows drifted")
        if tuple(self.effects.columns) != self.actions:
            raise SuccessorContractError("effect columns drifted from action vocabulary")
        if tuple(self.identified.columns) != self.actions:
            raise SuccessorContractError("identification columns drifted from action vocabulary")
        mask = self.identified.to_numpy(dtype=bool)
        values = self.effects.to_numpy(dtype=float)
        if not np.isfinite(values[mask]).all():
            raise SuccessorContractError("identified targets must be finite")
        if not np.isnan(values[~mask]).all():
            raise SuccessorContractError("unidentified targets must remain missing")


@dataclass(frozen=True, slots=True)
class SuccessorSearchProfile:
    """Pre-registered bounded search capacity, not an exhaustive closure."""

    name: str = "bounded_full_universe_v1"
    feature_budget: int = 1024
    max_depth: int = 6
    max_leaf_nodes: int = 32
    min_samples_leaf: int = 30
    max_rules: int = 7
    max_clauses_per_rule: int = 16
    max_literals_per_clause: int = 6

    def __post_init__(self) -> None:
        values = (
            self.feature_budget,
            self.max_depth,
            self.max_leaf_nodes,
            self.min_samples_leaf,
            self.max_rules,
            self.max_clauses_per_rule,
            self.max_literals_per_clause,
        )
        if any(value <= 0 for value in values):
            raise SuccessorContractError("search profile values must be positive")
        if self.max_literals_per_clause < 3 or self.max_rules < 2:
            raise SuccessorContractError("successor must make higher-order multi-rule policy reachable")


DEFAULT_SEARCH_PROFILE = SuccessorSearchProfile()


@dataclass(frozen=True, slots=True)
class ActionTreeAudit:
    action: str
    identified_rows: int
    unidentified_rows: int
    identified_campaigns: int
    identified_days: int
    target_scale_usdc: float
    positive_leaf_count: int


@dataclass(frozen=True, slots=True)
class PolicyFitAudit:
    side: str
    profile: str
    feature_count: int
    compiled_rule_count: int
    compiled_clause_count: int
    compiled_literal_count: int
    maximum_clause_literals: int
    uses_neutral_zero_targets: bool
    action_audits: tuple[ActionTreeAudit, ...]
    candidate_id: str


@dataclass(frozen=True, slots=True)
class SemanticRedundancy:
    rule_index: int
    action: str
    readiness_guard_predicate: str
    common_literals: tuple[tuple[str, bool], ...]
    observed_state_equivalence: str


@dataclass(frozen=True, slots=True)
class PolicySemanticAudit:
    candidate_source_block: str
    compiled_feature_families: tuple[str, ...]
    uses_m2_incremental_features: bool
    readiness_guard_predicates: tuple[str, ...]
    economic_branch_features: tuple[str, ...]
    redundancies: tuple[SemanticRedundancy, ...]
    simplified_semantics: tuple[Mapping[str, Any], ...]
    live_artifact_rewritten: bool = False


@dataclass(frozen=True, slots=True)
class ProspectiveDayAdmission:
    utc_day: str
    epoch_identity_sha256: str
    session_manifest_sha256: str
    utc_day_closed: bool
    registered_treatment_interval_coverage_complete: bool
    strategy_identity_valid: bool
    source_complete: bool
    receive_clock_valid: bool
    feature_ready_clock_valid: bool
    policy_decision_clock_valid: bool
    lifecycle_valid: bool
    callbacks_converged: bool
    remote_local_admission_valid: bool
    recorder_drops: int
    severe_errors: int
    eligible_events: int
    feature_ready_active_treatment_events: int

    def __post_init__(self) -> None:
        day = pd.Timestamp(self.utc_day)
        if day.tzinfo is not None:
            day = day.tz_convert("UTC").tz_localize(None)
        if day != day.normalize():
            raise SuccessorContractError("prospective admission day is not a UTC date")
        object.__setattr__(self, "utc_day", day.strftime("%Y-%m-%d"))
        for name in ("epoch_identity_sha256", "session_manifest_sha256"):
            value = str(getattr(self, name))
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise SuccessorContractError(f"{name} is not a lowercase SHA256")
        for name in (
            "utc_day_closed",
            "registered_treatment_interval_coverage_complete",
            "strategy_identity_valid",
            "source_complete",
            "receive_clock_valid",
            "feature_ready_clock_valid",
            "policy_decision_clock_valid",
            "lifecycle_valid",
            "callbacks_converged",
            "remote_local_admission_valid",
        ):
            if not isinstance(getattr(self, name), bool):
                raise SuccessorContractError(f"{name} must be Boolean")
        for name in (
            "recorder_drops",
            "severe_errors",
            "eligible_events",
            "feature_ready_active_treatment_events",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise SuccessorContractError(f"{name} must be a nonnegative integer")
        if self.feature_ready_active_treatment_events > self.eligible_events:
            raise SuccessorContractError(
                "feature-ready active treatment events exceed eligible events"
            )

    @property
    def rejection_reasons(self) -> tuple[str, ...]:
        checks = (
            (self.utc_day < FIRST_ELIGIBLE_FULL_UTC_DAY, "before_preregistration_cutoff"),
            (not self.utc_day_closed, "utc_day_not_closed"),
            (
                not self.registered_treatment_interval_coverage_complete,
                "registered_treatment_interval_coverage_incomplete",
            ),
            (not self.strategy_identity_valid, "strategy_identity_invalid"),
            (not self.source_complete, "source_incomplete"),
            (not self.receive_clock_valid, "receive_clock_invalid"),
            (not self.feature_ready_clock_valid, "feature_ready_clock_invalid"),
            (not self.policy_decision_clock_valid, "policy_decision_clock_invalid"),
            (not self.lifecycle_valid, "lifecycle_invalid"),
            (not self.callbacks_converged, "callbacks_not_converged"),
            (not self.remote_local_admission_valid, "remote_local_admission_invalid"),
            (self.recorder_drops != 0, "recorder_drops_nonzero"),
            (self.severe_errors != 0, "severe_errors_nonzero"),
            (self.eligible_events == 0, "zero_eligible_events"),
            (
                self.feature_ready_active_treatment_events == 0,
                "zero_feature_ready_active_treatment_events",
            ),
        )
        return tuple(reason for failed, reason in checks if failed)

    @property
    def eligible(self) -> bool:
        return not self.rejection_reasons


@dataclass(frozen=True, slots=True)
class ProspectivePanelAdmission:
    selected_active_days: tuple[str, ...]
    all_eligible_days: tuple[str, ...]
    rejected_days: Mapping[str, tuple[str, ...]]
    required_active_days: int
    ready_for_new_economic_panel: bool
    selection_sha256: str


@dataclass(frozen=True, slots=True)
class ProspectiveFoldManifest:
    active_days: tuple[str, ...]
    outer_folds: tuple[Mapping[str, Any], ...]
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class ExactOwnerArtifactParity:
    policy_sha256: str
    predicate_bundle_sha256: str
    predicate_columns: tuple[str, ...]
    sell_tri_state_cases: int
    buy_tri_state_cases: int
    mismatch_count: int
    documented_semantics_equal: bool
    runtime_binding_valid: bool


@dataclass(frozen=True, slots=True)
class PairedRepeatedPolicyAudit:
    """One paired sequential-policy execution admission decision."""

    utc_day: str
    common_input_identity_sha256: str
    control_policy_sha256: str
    candidate_policy_sha256: str
    control_repeated_policy_evaluations: int
    candidate_repeated_policy_evaluations: int
    control_support_valid: bool
    candidate_support_valid: bool
    formal_economic_mode: bool
    formal_denominator_eligible: bool
    exclusion_reasons: tuple[str, ...]
    terminal_value_delta_usdc: float | None
    transport_common_market_source_sha256: str | None
    control_transport_receipt_sha256: str | None
    candidate_transport_receipt_sha256: str | None
    control_private_fill_visibility_authority: str | None
    candidate_private_fill_visibility_authority: str | None
    transport_live_equivalent: bool
    both_arms_executed_policy_function: bool
    execution_copied_between_arms: bool
    one_shot_effect_aggregation_used: bool

    def __post_init__(self) -> None:
        for name in (
            "common_input_identity_sha256",
            "control_policy_sha256",
            "candidate_policy_sha256",
        ):
            if _SHA256_RE.fullmatch(str(getattr(self, name))) is None:
                raise SuccessorContractError(f"{name} is not a lowercase SHA256")
        if self.formal_denominator_eligible:
            if self.exclusion_reasons:
                raise SuccessorContractError(
                    "eligible repeated-policy pair cannot carry exclusion reasons"
                )
            if self.terminal_value_delta_usdc is None:
                raise SuccessorContractError(
                    "eligible repeated-policy pair lacks a terminal-value delta"
                )
        elif self.terminal_value_delta_usdc is not None:
            raise SuccessorContractError(
                "ineligible repeated-policy pair cannot expose an economic delta"
            )
        if self.execution_copied_between_arms or self.one_shot_effect_aggregation_used:
            raise SuccessorContractError(
                "paired repeated-policy audit cannot admit copied or one-shot economics"
            )
        transport_hashes = (
            self.transport_common_market_source_sha256,
            self.control_transport_receipt_sha256,
            self.candidate_transport_receipt_sha256,
        )
        if self.formal_economic_mode:
            if any(value is None for value in transport_hashes):
                raise SuccessorContractError(
                    "formal repeated-policy audit requires paired transport receipts"
                )
            for value in transport_hashes:
                if _SHA256_RE.fullmatch(str(value)) is None:
                    raise SuccessorContractError(
                        "formal repeated-policy transport SHA256 is invalid"
                    )
            if (
                self.control_private_fill_visibility_authority is None
                or self.candidate_private_fill_visibility_authority is None
            ):
                raise SuccessorContractError(
                    "formal repeated-policy audit lacks private-fill authority labels"
                )
        if self.transport_live_equivalent:
            raise SuccessorContractError(
                "historical repeated-policy transport cannot claim live equivalence"
            )


_FORBIDDEN_DAY_ADMISSION_KEY_PARTS = (
    "outcome",
    "terminal_value",
    "pnl",
    "profit",
    "reward",
)
_ALLOWED_FALSE_EVIDENCE_FLAGS = {
    "economic_outcomes_read",
    "campaign_terminal_value_read",
}


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def parse_prospective_day_admission(
    payload: Mapping[str, Any],
) -> ProspectiveDayAdmission:
    """Parse an outcome-blind day record and reject economic selection fields."""

    lowered = {str(key).lower() for key in payload}
    forbidden = sorted(
        key
        for key in lowered
        if any(part in key for part in _FORBIDDEN_DAY_ADMISSION_KEY_PARTS)
    )
    if forbidden:
        raise SuccessorContractError(
            f"day admission may not inspect economic fields: {forbidden}"
        )
    expected = {field.name for field in ProspectiveDayAdmission.__dataclass_fields__.values()}
    missing = expected - set(payload)
    unknown = set(payload) - expected
    if missing or unknown:
        raise SuccessorContractError(
            f"day admission schema drifted: missing={sorted(missing)}, unknown={sorted(unknown)}"
        )
    return ProspectiveDayAdmission(**{name: payload[name] for name in expected})


def select_prospective_development_days(
    records: Sequence[ProspectiveDayAdmission | Mapping[str, Any]],
    *,
    required_active_days: int = MINIMUM_ACTIVE_TREATMENT_DAYS,
) -> ProspectivePanelAdmission:
    """Select the first chronological eligible days without consulting outcomes."""

    if required_active_days <= 0:
        raise SuccessorContractError("required active day count must be positive")
    parsed = tuple(
        row if isinstance(row, ProspectiveDayAdmission) else parse_prospective_day_admission(row)
        for row in records
    )
    days = [row.utc_day for row in parsed]
    if len(days) != len(set(days)):
        raise SuccessorContractError("prospective day admission repeats a UTC day")
    ordered = tuple(sorted(parsed, key=lambda row: row.utc_day))
    eligible_records = tuple(row for row in ordered if row.eligible)
    eligible = tuple(row.utc_day for row in eligible_records)
    selected = eligible[:required_active_days]
    selected_records = eligible_records[:required_active_days]
    rejected = {
        row.utc_day: row.rejection_reasons for row in ordered if not row.eligible
    }
    return ProspectivePanelAdmission(
        selected_active_days=selected,
        all_eligible_days=eligible,
        rejected_days=rejected,
        required_active_days=required_active_days,
        ready_for_new_economic_panel=len(selected) == required_active_days,
        selection_sha256=_canonical_sha256(
            {
                "identity": IDENTITY,
                "cutoff": PREREGISTRATION_CUTOFF_UTC,
                "required_active_days": required_active_days,
                "selected_active_day_admissions": [
                    asdict(row) for row in selected_records
                ],
            }
        ),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _economic_keys(payload: Any, *, prefix: str = "") -> tuple[str, ...]:
    found: list[str] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            name = str(key)
            path = f"{prefix}.{name}" if prefix else name
            if (
                any(part in name.lower() for part in _FORBIDDEN_DAY_ADMISSION_KEY_PARTS)
                and not (name.lower() in _ALLOWED_FALSE_EVIDENCE_FLAGS and value is False)
            ):
                found.append(path)
            found.extend(_economic_keys(value, prefix=path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            found.extend(_economic_keys(value, prefix=f"{prefix}[{index}]"))
    return tuple(found)


def _load_bound_outcome_blind_json(
    *,
    base: Path,
    binding: Mapping[str, Any],
    role: str,
) -> tuple[Path, Mapping[str, Any]]:
    if set(binding) != {"path", "sha256"}:
        raise SuccessorContractError(f"{role} binding schema drifted")
    raw_path = Path(str(binding["path"])).expanduser()
    path = raw_path if raw_path.is_absolute() else base / raw_path
    path = path.resolve()
    expected = str(binding["sha256"])
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise SuccessorContractError(f"{role} binding SHA256 is invalid")
    if not path.is_file() or _sha256_file(path) != expected:
        raise SuccessorContractError(f"{role} binding bytes drifted")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SuccessorContractError(f"{role} manifest is unreadable") from exc
    if not isinstance(payload, Mapping):
        raise SuccessorContractError(f"{role} manifest must be an object")
    forbidden = _economic_keys(payload)
    if forbidden:
        raise SuccessorContractError(
            f"{role} admission contains economic fields: {sorted(forbidden)}"
        )
    return path, payload


def _validate_registered_treatment_intervals(
    day: str,
    intervals: Any,
) -> tuple[tuple[str, str], ...]:
    if not isinstance(intervals, list) or not intervals:
        raise SuccessorContractError("registered treatment intervals are missing")
    normalized: list[tuple[str, str]] = []
    prior_end: pd.Timestamp | None = None
    expected_day = pd.Timestamp(day)
    for row in intervals:
        if not isinstance(row, list) or len(row) != 2:
            raise SuccessorContractError("registered treatment interval schema drifted")
        start = pd.Timestamp(row[0])
        end = pd.Timestamp(row[1])
        if start.tzinfo is None or end.tzinfo is None:
            raise SuccessorContractError(
                "registered treatment interval must use UTC clocks"
            )
        start = start.tz_convert("UTC")
        end = end.tz_convert("UTC")
        if start >= end or start.tz_localize(None).normalize() != expected_day:
            raise SuccessorContractError(
                "registered treatment interval is outside its UTC day"
            )
        day_end = expected_day.tz_localize("UTC") + pd.Timedelta(days=1)
        if end > day_end:
            raise SuccessorContractError(
                "registered treatment interval crosses a UTC boundary"
            )
        if prior_end is not None and start < prior_end:
            raise SuccessorContractError(
                "registered treatment intervals overlap or are unsorted"
            )
        normalized.append(
            (
                start.isoformat().replace("+00:00", "Z"),
                end.isoformat().replace("+00:00", "Z"),
            )
        )
        prior_end = end
    return tuple(normalized)


def _required_nonnegative_int(
    payload: Mapping[str, Any],
    name: str,
    *,
    role: str,
) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SuccessorContractError(f"{role}.{name} must be a nonnegative integer")
    return value


def produce_prospective_day_admission(
    source_bundle_path: str | Path,
) -> ProspectiveDayAdmission:
    """Derive one day admission from three hash-bound non-economic manifests."""

    bundle_path = Path(source_bundle_path).expanduser().resolve()
    if not bundle_path.is_file():
        raise SuccessorContractError("prospective day source bundle is missing")
    try:
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SuccessorContractError("prospective day source bundle is unreadable") from exc
    if not isinstance(bundle, Mapping) or _economic_keys(bundle):
        raise SuccessorContractError("prospective day source bundle is not outcome-blind")
    if (
        bundle.get("schema_version") != PROSPECTIVE_DAY_SOURCE_BUNDLE_SCHEMA
        or bundle.get("identity") != IDENTITY
    ):
        raise SuccessorContractError("prospective day source bundle identity drifted")
    permissions = bundle.get("permissions")
    required_false = ("economic_outcomes_read", "validation_read", "sealed_holdout_read")
    if not isinstance(permissions, Mapping) or any(
        permissions.get(name) is not False for name in required_false
    ):
        raise SuccessorContractError("prospective source permissions drifted")
    try:
        raw_day = pd.Timestamp(bundle.get("utc_day"))
    except (TypeError, ValueError) as exc:
        raise SuccessorContractError("prospective source UTC day is invalid") from exc
    if pd.isna(raw_day):
        raise SuccessorContractError("prospective source UTC day is invalid")
    if raw_day.tzinfo is not None:
        raw_day = raw_day.tz_convert("UTC").tz_localize(None)
    if raw_day != raw_day.normalize():
        raise SuccessorContractError("prospective source UTC day is invalid")
    day = raw_day.strftime("%Y-%m-%d")
    intervals = _validate_registered_treatment_intervals(
        day, bundle.get("registered_treatment_intervals_utc")
    )
    interval_sha256 = _canonical_sha256(
        {
            "utc_day": day,
            "registered_treatment_intervals_utc": [list(row) for row in intervals],
        }
    )
    bindings = bundle.get("bindings")
    expected_roles = {"lifecycle", "market", "decision"}
    if not isinstance(bindings, Mapping) or set(bindings) != expected_roles:
        raise SuccessorContractError("prospective source bindings drifted")
    loaded: dict[str, Mapping[str, Any]] = {}
    for role in sorted(expected_roles):
        _, payload = _load_bound_outcome_blind_json(
            base=bundle_path.parent,
            binding=bindings[role],
            role=role,
        )
        loaded[role] = payload

    expected_schemas = {
        "lifecycle": LIFECYCLE_DAY_ADMISSION_SCHEMA,
        "market": MARKET_DAY_ADMISSION_SCHEMA,
        "decision": DECISION_DAY_ADMISSION_SCHEMA,
    }
    strategy_sha = str(bundle.get("strategy_identity_sha256", ""))
    epoch_sha = str(bundle.get("epoch_identity_sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", strategy_sha):
        raise SuccessorContractError("strategy identity SHA256 is invalid")
    for role, payload in loaded.items():
        interval_count = _required_nonnegative_int(
            payload, "registered_treatment_interval_count", role=role
        )
        if payload.get("schema_version") != expected_schemas[role]:
            raise SuccessorContractError(f"{role} day admission schema drifted")
        if (
            payload.get("identity") != IDENTITY
            or payload.get("utc_day") != day
            or payload.get("epoch_identity_sha256") != epoch_sha
            or payload.get("strategy_identity_sha256") != strategy_sha
            or interval_count != len(intervals)
            or payload.get("registered_treatment_intervals_sha256") != interval_sha256
        ):
            raise SuccessorContractError(f"{role} day admission identity drifted")

    lifecycle = loaded["lifecycle"]
    market = loaded["market"]
    decision = loaded["decision"]
    coverage_counts = decision.get("coverage_reason_counts")
    if not isinstance(coverage_counts, Mapping):
        raise SuccessorContractError("decision coverage reason census is missing")
    unknown_reasons = set(coverage_counts) - {reason.value for reason in CoverageReason}
    if unknown_reasons:
        raise SuccessorContractError(
            f"decision coverage reason vocabulary drifted: {sorted(unknown_reasons)}"
        )
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in coverage_counts.values()
    ):
        raise SuccessorContractError("decision coverage counts must be nonnegative integers")
    eligible_events = _required_nonnegative_int(
        decision, "eligible_events", role="decision"
    )
    active_events = _required_nonnegative_int(
        decision,
        "feature_ready_active_treatment_events",
        role="decision",
    )
    if sum(coverage_counts.values()) != eligible_events:
        raise SuccessorContractError("decision coverage census does not equal eligible events")

    coverage_complete = all(
        payload.get("registered_treatment_interval_coverage_complete") is True
        for payload in loaded.values()
    )
    return ProspectiveDayAdmission(
        utc_day=day,
        epoch_identity_sha256=epoch_sha,
        session_manifest_sha256=_sha256_file(bundle_path),
        utc_day_closed=bundle.get("utc_day_closed") is True,
        registered_treatment_interval_coverage_complete=coverage_complete,
        strategy_identity_valid=True,
        source_complete=market.get("source_complete") is True,
        receive_clock_valid=market.get("receive_clock_valid") is True,
        feature_ready_clock_valid=market.get("feature_ready_clock_valid") is True,
        policy_decision_clock_valid=decision.get("policy_decision_clock_valid") is True,
        lifecycle_valid=lifecycle.get("lifecycle_valid") is True,
        callbacks_converged=lifecycle.get("callbacks_converged") is True,
        remote_local_admission_valid=all(
            payload.get("remote_local_admission_valid") is True
            for payload in loaded.values()
        ),
        recorder_drops=sum(
            _required_nonnegative_int(payload, "drop_count", role=role)
            for role, payload in loaded.items()
        ),
        severe_errors=sum(
            _required_nonnegative_int(payload, "error_count", role=role)
            for role, payload in loaded.items()
        ),
        eligible_events=eligible_events,
        feature_ready_active_treatment_events=active_events,
    )


def write_prospective_day_admission(
    admission: ProspectiveDayAdmission,
    output_path: str | Path,
) -> str:
    """Atomically persist an immutable outcome-blind day admission record."""

    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": f"{IDENTITY}.prospective_day_admission.v1",
        "identity": IDENTITY,
        "record": asdict(admission),
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    encoded = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise SuccessorContractError("immutable day admission already exists with other bytes")
        return hashlib.sha256(encoded).hexdigest()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return hashlib.sha256(encoded).hexdigest()


def load_prospective_day_admission(
    path: str | Path,
) -> ProspectiveDayAdmission:
    resolved = Path(path).expanduser().resolve()
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SuccessorContractError("prospective day admission is unreadable") from exc
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version")
        != f"{IDENTITY}.prospective_day_admission.v1"
        or payload.get("identity") != IDENTITY
        or payload.get("economic_outcomes_read") is not False
        or payload.get("validation_read") is not False
        or payload.get("sealed_holdout_read") is not False
        or not isinstance(payload.get("record"), Mapping)
    ):
        raise SuccessorContractError("prospective day admission identity drifted")
    return parse_prospective_day_admission(payload["record"])


def build_prospective_fold_manifest(
    admission: ProspectivePanelAdmission,
) -> ProspectiveFoldManifest:
    """Freeze the preregistered 4x3 folds after all 30 active days exist."""

    if (
        not admission.ready_for_new_economic_panel
        or admission.required_active_days != MINIMUM_ACTIVE_TREATMENT_DAYS
        or len(admission.selected_active_days) != MINIMUM_ACTIVE_TREATMENT_DAYS
    ):
        raise SuccessorContractError("30 admitted active days are not available")
    days = admission.selected_active_days
    outer_ranges = ((10, 15), (15, 20), (20, 25), (25, 30))
    outer_rows: list[Mapping[str, Any]] = []
    for outer_index, (train_end, test_end) in enumerate(outer_ranges, start=1):
        train_days = days[:train_end]
        test_days = days[train_end:test_end]
        inner_test_positions = np.array_split(
            np.arange(5, len(train_days), dtype=np.int64),
            3,
        )
        inner_rows: list[Mapping[str, Any]] = []
        for inner_index, positions in enumerate(inner_test_positions, start=1):
            if len(positions) == 0:
                raise SuccessorContractError("preregistered inner fold is empty")
            start = int(positions[0])
            inner_rows.append(
                {
                    "fold_id": f"outer{outer_index}.inner{inner_index}",
                    "train_days": list(train_days[:start]),
                    "test_days": [train_days[int(position)] for position in positions],
                }
            )
        outer_rows.append(
            {
                "fold_id": f"outer{outer_index}",
                "train_days": list(train_days),
                "test_days": list(test_days),
                "inner_folds": inner_rows,
            }
        )
    body = {
        "identity": IDENTITY,
        "prospective_day_selection_sha256": admission.selection_sha256,
        "active_days": list(days),
        "outer_folds": outer_rows,
    }
    return ProspectiveFoldManifest(
        active_days=days,
        outer_folds=tuple(outer_rows),
        manifest_sha256=_canonical_sha256(body),
    )


def write_prospective_fold_manifest(
    folds: ProspectiveFoldManifest,
    output_path: str | Path,
) -> str:
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": f"{IDENTITY}.prospective_fold_manifest.v1",
        "identity": IDENTITY,
        "active_days": list(folds.active_days),
        "outer_folds": list(folds.outer_folds),
        "manifest_sha256": folds.manifest_sha256,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    encoded = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise SuccessorContractError("immutable fold manifest already has other bytes")
        return hashlib.sha256(encoded).hexdigest()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return hashlib.sha256(encoded).hexdigest()


def prospective_status_payload(
    admissions: Sequence[ProspectiveDayAdmission],
) -> dict[str, Any]:
    panel = select_prospective_development_days(admissions)
    return {
        "schema_version": f"{IDENTITY}.prospective_status.v1",
        "identity": IDENTITY,
        "preregistration_cutoff_utc": PREREGISTRATION_CUTOFF_UTC,
        "required_active_days": panel.required_active_days,
        "eligible_active_day_count": len(panel.all_eligible_days),
        "selected_active_days": list(panel.selected_active_days),
        "rejected_days": {
            day: list(reasons) for day, reasons in panel.rejected_days.items()
        },
        "ready_for_new_economic_panel": panel.ready_for_new_economic_panel,
        "selection_sha256": panel.selection_sha256,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    produce = commands.add_parser("produce-day")
    produce.add_argument("--source-bundle", type=Path, required=True)
    produce.add_argument("--output", type=Path, required=True)
    for name in ("status", "freeze-folds"):
        command = commands.add_parser(name)
        command.add_argument("--day-admission", type=Path, action="append", default=[])
        command.add_argument("--day-admission-dir", type=Path)
        if name == "freeze-folds":
            command.add_argument("--output", type=Path, required=True)
    exact_owner = commands.add_parser("audit-owner-artifact")
    exact_owner.add_argument("--policy", type=Path, required=True)
    exact_owner.add_argument("--predicate-bundle", type=Path, required=True)
    exact_owner.add_argument("--output", type=Path, required=True)
    return parser


def _admission_paths(
    args: argparse.Namespace,
    *,
    allow_empty: bool,
) -> tuple[Path, ...]:
    paths = list(args.day_admission)
    if args.day_admission_dir is not None:
        root = args.day_admission_dir.expanduser().resolve()
        paths.extend(sorted(root.glob("*.json")))
    resolved = tuple(dict.fromkeys(path.expanduser().resolve() for path in paths))
    if not resolved and not allow_empty:
        raise SuccessorContractError("no prospective day admissions were supplied")
    return resolved


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "produce-day":
        admission = produce_prospective_day_admission(args.source_bundle)
        output_sha = write_prospective_day_admission(admission, args.output)
        payload = {
            "identity": IDENTITY,
            "utc_day": admission.utc_day,
            "eligible": admission.eligible,
            "rejection_reasons": list(admission.rejection_reasons),
            "output_sha256": output_sha,
            "economic_outcomes_read": False,
        }
    elif args.command == "audit-owner-artifact":
        parity = audit_exact_owner_artifact_parity(
            policy_path=args.policy,
            predicate_bundle_path=args.predicate_bundle,
        )
        output_sha = write_exact_owner_artifact_parity(parity, args.output)
        payload = {
            "identity": IDENTITY,
            "artifact_id": ACTIVE_OWNER_POLICY_IDENTITY,
            "policy_sha256": parity.policy_sha256,
            "predicate_bundle_sha256": parity.predicate_bundle_sha256,
            "sell_tri_state_cases": parity.sell_tri_state_cases,
            "buy_tri_state_cases": parity.buy_tri_state_cases,
            "mismatch_count": parity.mismatch_count,
            "documented_semantics_equal": parity.documented_semantics_equal,
            "runtime_binding_valid": parity.runtime_binding_valid,
            "output_sha256": output_sha,
            "economic_outcomes_read": False,
            "active_owner_policy_modified": False,
        }
    else:
        admissions = tuple(
            load_prospective_day_admission(path)
            for path in _admission_paths(
                args,
                allow_empty=args.command == "status",
            )
        )
        payload = prospective_status_payload(admissions)
        if args.command == "freeze-folds":
            panel = select_prospective_development_days(admissions)
            folds = build_prospective_fold_manifest(panel)
            payload["fold_manifest_file_sha256"] = write_prospective_fold_manifest(
                folds, args.output
            )
            payload["fold_manifest_identity_sha256"] = folds.manifest_sha256
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


def _pair_token(value: float) -> str:
    return f"{value:g}".replace(".", "p")


def _parse_ema_pair(name: str) -> tuple[float, float] | None:
    text = str(name)
    match = _EMA_PAIR_RE.search(text) or _CANONICAL_EMA_PAIR_RE.search(text)
    if match is None:
        return None
    return (
        float(match.group("fast").replace("p", ".")),
        float(match.group("slow").replace("p", ".")),
    )


def full_ema_pair_prefixes() -> tuple[str, ...]:
    """Return the exact 45 pair name fragments used by the feature schema."""

    return tuple(
        f"ema_pair_h{_pair_token(fast)}s_h{_pair_token(slow)}s"
        for fast, slow in EMA_PAIRS_S
    )


def audit_full_ema_universe(predicate_names: Sequence[str]) -> dict[str, Any]:
    present = {
        pair for name in predicate_names if (pair := _parse_ema_pair(str(name))) is not None
    }
    expected = set(EMA_PAIRS_S)
    return {
        "expected_pair_count": len(expected),
        "present_pair_count": len(present & expected),
        "all_45_pairs_present": expected <= present,
        "missing_pairs_s": [list(pair) for pair in sorted(expected - present)],
        "unexpected_pairs_s": [list(pair) for pair in sorted(present - expected)],
    }


def classify_coverage(
    *,
    eligible: bool,
    feature_ready: bool,
    support_valid: bool,
    action_id: str,
    fallback_reason: str | None,
) -> CoverageClassification:
    """Normalize existing replay/live reasons without changing policy behavior."""

    if any(type(value) is not bool for value in (eligible, feature_ready, support_valid)):
        raise SuccessorContractError("coverage state flags must be Boolean")
    raw = None if fallback_reason is None else str(fallback_reason)
    normalized = (raw or "").lower()
    if not eligible:
        reason = CoverageReason.INELIGIBLE_EVENT
    elif "cache" in normalized:
        reason = CoverageReason.CACHE_UNAVAILABLE
    elif "source" in normalized or "no_completed_receive_time_window" in normalized:
        reason = CoverageReason.SOURCE_UNAVAILABLE
    elif "warmup" in normalized:
        reason = CoverageReason.WARMUP_INCOMPLETE
    elif "stale" in normalized or "gap_exceeded" in normalized:
        reason = CoverageReason.FEATURE_STALE
    elif "unobserved" in normalized or normalized.startswith("rule_unobserved"):
        reason = CoverageReason.PREDICATE_UNOBSERVED
    elif "binding" in normalized or "sha256" in normalized or "drift" in normalized:
        reason = CoverageReason.BINDING_INVALID
    elif not feature_ready:
        reason = CoverageReason.PREDICATE_UNOBSERVED
    elif not support_valid:
        reason = CoverageReason.LIFECYCLE_UNIDENTIFIED
    elif raw is not None and raw in {"buy_control_by_contract", "no_rule_matched"}:
        reason = CoverageReason.POLICY_CONTROL
    elif raw is not None:
        reason = CoverageReason.SAFETY_FALLBACK
    elif action_id == CONTROL_ACTION:
        reason = CoverageReason.POLICY_CONTROL
    else:
        reason = CoverageReason.ELIGIBLE_FEATURE_READY
    nonbaseline = str(action_id) != CONTROL_ACTION
    if nonbaseline and reason is not CoverageReason.ELIGIBLE_FEATURE_READY:
        raise SuccessorContractError(
            "nonbaseline action cannot carry an ineligible or fallback coverage reason"
        )
    return CoverageClassification(
        reason=reason,
        eligible=bool(eligible),
        feature_ready=bool(feature_ready),
        support_valid=bool(support_valid),
        nonbaseline_action=nonbaseline,
        raw_fallback_reason=raw,
    )


def classify_gtx_exposure(
    *,
    exchange_error_code: int | None,
    exchange_reject_confirmed: bool,
    activation_observed: bool,
    transport_timeout: bool,
    response_lost: bool,
    ack_state_known: bool,
) -> GtxExposureClassification:
    """Encode ``-5022`` as zero only when pre-activation rejection is exact."""

    if activation_observed:
        return GtxExposureClassification(
            encoding=ExposureEncoding.ACTIVATED_OR_EXPOSED,
            coverage_reason=CoverageReason.ELIGIBLE_FEATURE_READY,
            exact_zero_exposure=False,
            point_identified=True,
            reason="activation_or_exchange_exposure_observed",
        )
    exact_reject = (
        exchange_error_code == -5022
        and exchange_reject_confirmed
        and ack_state_known
        and not transport_timeout
        and not response_lost
    )
    if exact_reject:
        return GtxExposureClassification(
            encoding=ExposureEncoding.EXACT_ZERO_EXPOSURE,
            coverage_reason=CoverageReason.GTX_PREACTIVATION_EXACT_ZERO,
            exact_zero_exposure=True,
            point_identified=True,
            reason="exchange_confirmed_never_activated_gtx_reject_-5022",
        )
    return GtxExposureClassification(
        encoding=ExposureEncoding.CENSORED_UNKNOWN_EXPOSURE,
        coverage_reason=CoverageReason.GTX_ACK_UNKNOWN_CENSORED,
        exact_zero_exposure=False,
        point_identified=False,
        reason="activation_or_ack_visibility_not_identified",
    )


def build_identified_action_targets(
    outcomes: pd.DataFrame,
    supported: pd.DataFrame,
    *,
    control_action: str = CONTROL_ACTION,
    actions: Sequence[str] | None = None,
    index: pd.Index | None = None,
) -> IdentifiedActionTargets:
    """Build candidate-minus-control targets while preserving unknowns as NaN."""

    rows = outcomes.index if index is None else index
    if not rows.isin(outcomes.index).all() or not rows.isin(supported.index).all():
        raise SuccessorContractError("target rows are absent from the arm matrices")
    if control_action not in outcomes or control_action not in supported:
        raise SuccessorContractError("control action is absent from the arm matrices")
    action_names = tuple(
        str(action)
        for action in (
            actions
            if actions is not None
            else [column for column in outcomes.columns if column != control_action]
        )
    )
    if not action_names or any(action == control_action for action in action_names):
        raise SuccessorContractError("candidate action vocabulary is invalid")
    missing = set(action_names) - set(outcomes.columns) | set(action_names) - set(
        supported.columns
    )
    if missing:
        raise SuccessorContractError(f"candidate arm columns are missing: {sorted(missing)}")
    control = pd.to_numeric(outcomes.loc[rows, control_action], errors="coerce")
    control_supported = supported.loc[rows, control_action].astype(bool)
    effects: dict[str, pd.Series] = {}
    masks: dict[str, pd.Series] = {}
    for action in action_names:
        candidate = pd.to_numeric(outcomes.loc[rows, action], errors="coerce")
        candidate_supported = supported.loc[rows, action].astype(bool)
        known = (
            control_supported
            & candidate_supported
            & np.isfinite(control.to_numpy(dtype=float))
            & np.isfinite(candidate.to_numpy(dtype=float))
        )
        effect = pd.Series(np.nan, index=rows, dtype=float)
        effect.loc[known] = candidate.loc[known] - control.loc[known]
        effects[action] = effect
        masks[action] = pd.Series(known, index=rows, dtype=bool)
    return IdentifiedActionTargets(
        effects=pd.DataFrame(effects, index=rows),
        identified=pd.DataFrame(masks, index=rows),
        control_action=control_action,
        actions=action_names,
    )


def build_identified_action_targets_against_policy(
    outcomes: pd.DataFrame,
    supported: pd.DataFrame,
    *,
    baseline_actions: pd.Series,
    actions: Sequence[str],
    index: pd.Index | None = None,
) -> IdentifiedActionTargets:
    """Build duration effects against the exact row-wise baseline policy action."""

    rows = outcomes.index if index is None else index
    if not rows.isin(outcomes.index).all() or not rows.isin(supported.index).all():
        raise SuccessorContractError("target rows are absent from the arm matrices")
    if not rows.isin(baseline_actions.index).all():
        raise SuccessorContractError("row-wise baseline actions are missing target rows")
    baseline = baseline_actions.reindex(rows).astype(str)
    vocabulary = set(outcomes.columns) & set(supported.columns)
    unknown_baseline = sorted(set(baseline) - vocabulary)
    if unknown_baseline:
        raise SuccessorContractError(
            f"row-wise baseline actions are absent from arm matrices: {unknown_baseline}"
        )
    action_names = tuple(dict.fromkeys(str(action) for action in actions))
    if not action_names or set(action_names) - vocabulary:
        raise SuccessorContractError("candidate action vocabulary is invalid")
    row_positions = np.arange(len(rows))
    baseline_positions = np.asarray(
        [outcomes.columns.get_loc(action) for action in baseline],
        dtype=np.int64,
    )
    outcome_values = outcomes.loc[rows].to_numpy(dtype=float, copy=False)
    support_values = supported.loc[rows].to_numpy(dtype=bool, copy=False)
    baseline_value = outcome_values[row_positions, baseline_positions]
    baseline_supported = support_values[row_positions, baseline_positions]
    effects: dict[str, pd.Series] = {}
    masks: dict[str, pd.Series] = {}
    for action in action_names:
        candidate = pd.to_numeric(outcomes.loc[rows, action], errors="coerce").to_numpy(
            dtype=float
        )
        candidate_supported = supported.loc[rows, action].to_numpy(dtype=bool)
        known = (
            baseline_supported
            & candidate_supported
            & np.isfinite(baseline_value)
            & np.isfinite(candidate)
        )
        effect = np.full(len(rows), np.nan, dtype=float)
        effect[known] = candidate[known] - baseline_value[known]
        effects[action] = pd.Series(effect, index=rows)
        masks[action] = pd.Series(known, index=rows, dtype=bool)
    return IdentifiedActionTargets(
        effects=pd.DataFrame(effects, index=rows),
        identified=pd.DataFrame(masks, index=rows),
        control_action="ROW_WISE_EXACT_POLICY",
        actions=action_names,
    )


def _campaign_weights(metadata: pd.DataFrame) -> np.ndarray:
    if "campaign_cluster_id" not in metadata:
        raise SuccessorContractError("metadata is missing campaign_cluster_id")
    counts = metadata.groupby("campaign_cluster_id", observed=True)[
        "campaign_cluster_id"
    ].transform("size")
    return (1.0 / counts.astype(float)).to_numpy(dtype=float)


def _identified_economic_scores(
    matrix: np.ndarray,
    targets: IdentifiedActionTargets,
) -> np.ndarray:
    scores = np.zeros(matrix.shape[1], dtype=float)
    for column in range(matrix.shape[1]):
        states = matrix[:, column]
        observed = states != -1
        for action in targets.actions:
            known = observed & targets.identified[action].to_numpy(dtype=bool)
            if known.sum() < 4:
                continue
            values = targets.effects[action].to_numpy(dtype=float)
            false = known & (states == 0)
            true = known & (states == 1)
            if false.sum() < 2 or true.sum() < 2:
                continue
            contrast = abs(float(values[true].mean() - values[false].mean()))
            support_factor = math.sqrt(min(false.sum(), true.sum()) / known.sum())
            scores[column] = max(scores[column], contrast * support_factor)
    return scores


def build_inner_train_feature_pool(
    features: pd.DataFrame,
    metadata: pd.DataFrame,
    *,
    train_index: pd.Index,
    candidates: Sequence[str],
    feature_budget: int,
    fold_id: str,
    required_features: Sequence[str] = (),
    targets: IdentifiedActionTargets | None = None,
) -> tuple[tuple[str, ...], FeaturePoolAudit]:
    """Freeze a pool using only one purged inner fold's training rows."""

    if feature_budget <= 0 or not fold_id:
        raise SuccessorContractError("feature pool budget and fold id are required")
    if not train_index.isin(features.index).all() or not train_index.isin(metadata.index).all():
        raise SuccessorContractError("inner-train rows are absent from features or metadata")
    names = tuple(dict.fromkeys(str(name) for name in candidates))
    required = tuple(dict.fromkeys(str(name) for name in required_features))
    missing = (set(names) | set(required)) - set(features.columns)
    if missing:
        raise SuccessorContractError(f"feature pool columns are missing: {sorted(missing)}")
    if not set(required) <= set(names):
        raise SuccessorContractError("required features must belong to the candidate universe")
    if len(required) > feature_budget:
        raise SuccessorContractError("required features exceed the feature budget")
    full_matrix = features.loc[train_index, list(names)].to_numpy(dtype=np.int8)
    if not np.isin(full_matrix, (-1, 0, 1)).all():
        raise SuccessorContractError("Boolean feature pool is not tri-state")
    matrix = full_matrix
    identified_screen_rows = len(train_index)
    screen_targets = targets
    if targets is not None:
        if not train_index.equals(targets.effects.index):
            raise SuccessorContractError("economic screen targets must be the exact inner-train rows")
        identified_any = targets.identified.any(axis=1).to_numpy(dtype=bool)
        if not identified_any.any():
            raise SuccessorContractError("inner-train feature pool has no identified action row")
        matrix = full_matrix[identified_any]
        identified_index = train_index[identified_any]
        screen_targets = IdentifiedActionTargets(
            effects=targets.effects.loc[identified_index],
            identified=targets.identified.loc[identified_index],
            control_action=targets.control_action,
            actions=targets.actions,
        )
        identified_screen_rows = int(identified_any.sum())
    observed = matrix != -1
    observed_count = observed.sum(axis=0)
    true_count = (matrix == 1).sum(axis=0)
    false_count = (matrix == 0).sum(axis=0)
    distribution_quality = (
        observed_count.astype(float)
        / max(1, len(matrix))
        * np.minimum(true_count, false_count)
        / np.maximum(observed_count, 1)
    )
    if screen_targets is not None:
        economic_score = _identified_economic_scores(matrix, screen_targets)
        method = "inner_train_identified_rows_only_action_value_then_feature_quality"
    else:
        economic_score = np.zeros(len(names), dtype=float)
        method = "inner_train_feature_distribution_only"
    ranked = sorted(
        (
            float(economic_score[index]),
            float(distribution_quality[index]),
            _canonical_sha256(["predicate", name]),
            name,
        )
        for index, name in enumerate(names)
        if distribution_quality[index] > 0.0
    )
    ranked.reverse()
    selected = list(required)
    selected_set = set(selected)
    for _, _, _, name in ranked:
        if name in selected_set:
            continue
        selected.append(name)
        selected_set.add(name)
        if len(selected) >= feature_budget:
            break
    if not selected:
        raise SuccessorContractError("inner-train feature pool is empty")
    days = tuple(sorted(metadata.loc[train_index, "utc_day"].astype(str).unique()))
    universe = audit_full_ema_universe(names)
    selected_universe = audit_full_ema_universe(selected)
    audit = FeaturePoolAudit(
        fold_id=fold_id,
        train_days=days,
        train_rows=len(train_index),
        identified_screen_rows=identified_screen_rows,
        candidate_count=len(names),
        selected_count=len(selected),
        required_count=len(required),
        selection_method=method,
        candidate_sha256=_canonical_sha256(list(names)),
        selected_sha256=_canonical_sha256(selected),
        all_45_ema_pairs_eligible=bool(universe["all_45_pairs_present"]),
        eligible_ema_pair_count=int(universe["present_pair_count"]),
        selected_ema_pair_count=int(selected_universe["present_pair_count"]),
    )
    return tuple(selected), audit


def _leaf_constraints(
    model: DecisionTreeRegressor,
    feature_names: Sequence[str],
) -> list[tuple[int, dict[str, tuple[float, float]]]]:
    tree = model.tree_
    leaves: list[tuple[int, dict[str, tuple[float, float]]]] = []

    def walk(node: int, constraints: dict[str, tuple[float, float]]) -> None:
        left = int(tree.children_left[node])
        right = int(tree.children_right[node])
        if left == right:
            leaves.append((node, dict(constraints)))
            return
        feature = feature_names[int(tree.feature[node])]
        threshold = float(tree.threshold[node])
        lower, upper = constraints.get(feature, (-math.inf, math.inf))
        left_constraints = dict(constraints)
        left_constraints[feature] = (lower, min(upper, threshold))
        right_constraints = dict(constraints)
        right_constraints[feature] = (max(lower, threshold), upper)
        walk(left, left_constraints)
        walk(right, right_constraints)

    walk(0, {})
    return leaves


def compile_observed_leaf_clauses(
    constraints: Mapping[str, tuple[float, float]],
    *,
    max_clauses: int,
) -> tuple[AndClause, ...]:
    """Compile one tree leaf without treating UNOBSERVED as action evidence.

    A tree can split ``UNOBSERVED=-1`` from the two observed states with a
    threshold near ``-0.5``.  Omitting that path constraint would let an
    unobserved runtime row match the compiled action.  An observed-only path is
    therefore expanded into ``predicate OR NOT predicate`` clauses.  If that
    fail-closed expansion exceeds the frozen clause budget, the leaf is not
    compiled.
    """

    if max_clauses <= 0:
        raise SuccessorContractError("leaf clause budget must be positive")
    literal_choices: list[tuple[TriLiteral, ...]] = []
    for name, (lower, upper) in sorted(constraints.items()):
        allowed = tuple(state for state in (-1, 0, 1) if state > lower and state <= upper)
        observed = tuple(state for state in allowed if state in (0, 1))
        if not observed:
            return ()
        if observed == (0,):
            literal_choices.append((TriLiteral(name, True),))
        elif observed == (1,):
            literal_choices.append((TriLiteral(name, False),))
        elif observed == (0, 1):
            literal_choices.append(
                (TriLiteral(name, False), TriLiteral(name, True))
            )
        else:  # pragma: no cover - states are frozen above.
            raise SuccessorContractError("tree split produced an invalid tri-state interval")
    if not literal_choices:
        return ()
    expansion_count = math.prod(len(choices) for choices in literal_choices)
    if expansion_count > max_clauses:
        return ()
    clauses = tuple(
        sorted(
            (
                AndClause(tuple(sorted(literals)))
                for literals in product(*literal_choices)
            ),
            key=lambda clause: clause.key,
        )
    )
    return clauses


def fit_identified_action_policy(
    features: pd.DataFrame,
    metadata: pd.DataFrame,
    targets: IdentifiedActionTargets,
    *,
    side: str,
    feature_names: Sequence[str],
    profile: SuccessorSearchProfile | None = None,
    random_seed: int = 20260813,
) -> tuple[BooleanCooldownPolicy, PolicyFitAudit]:
    """Fit one tree per action only on rows where that action is identified."""

    profile = DEFAULT_SEARCH_PROFILE if profile is None else profile
    names = tuple(str(name) for name in feature_names)
    if not names or len(names) > profile.feature_budget:
        raise SuccessorContractError("fitted feature pool is empty or exceeds budget")
    if not features.index.equals(targets.effects.index) or not features.index.equals(
        metadata.index
    ):
        raise SuccessorContractError("fit rows are not exactly aligned")
    if set(names) - set(features.columns):
        raise SuccessorContractError("fitted feature columns are missing")
    matrix = features.loc[:, list(names)].to_numpy(dtype=np.float32)
    if not np.isin(matrix, (-1, 0, 1)).all():
        raise SuccessorContractError("fitted Boolean features are not tri-state")

    action_candidates: list[tuple[float, str, AndClause]] = []
    action_audits: list[ActionTreeAudit] = []
    for action_index, action in enumerate(targets.actions):
        known = targets.identified[action].to_numpy(dtype=bool)
        identified_rows = int(known.sum())
        if identified_rows < max(2, profile.min_samples_leaf * 2):
            action_audits.append(
                ActionTreeAudit(
                    action=action,
                    identified_rows=identified_rows,
                    unidentified_rows=len(known) - identified_rows,
                    identified_campaigns=int(
                        metadata.loc[known, "campaign_cluster_id"].nunique()
                    ),
                    identified_days=int(metadata.loc[known, "utc_day"].nunique()),
                    target_scale_usdc=math.nan,
                    positive_leaf_count=0,
                )
            )
            continue
        target = targets.effects.loc[known, action].to_numpy(dtype=float)
        scale = max(float(np.quantile(np.abs(target), 0.75)), 1e-9)
        weights = _campaign_weights(metadata.loc[known])
        model = DecisionTreeRegressor(
            max_depth=profile.max_depth,
            max_leaf_nodes=profile.max_leaf_nodes,
            min_samples_leaf=profile.min_samples_leaf,
            random_state=random_seed + action_index,
        )
        model.fit(matrix[known], target / scale, sample_weight=weights)
        values = np.asarray(model.tree_.value, dtype=float).reshape(-1) * scale
        positive = 0
        for node, constraints in _leaf_constraints(model, names):
            score = float(values[node])
            if score <= 0.0:
                continue
            clauses = compile_observed_leaf_clauses(
                constraints,
                max_clauses=profile.max_clauses_per_rule,
            )
            if not clauses or any(
                len(clause.literals) > profile.max_literals_per_clause
                for clause in clauses
            ):
                continue
            action_candidates.extend((score, action, clause) for clause in clauses)
            positive += 1
        action_audits.append(
            ActionTreeAudit(
                action=action,
                identified_rows=identified_rows,
                unidentified_rows=len(known) - identified_rows,
                identified_campaigns=int(metadata.loc[known, "campaign_cluster_id"].nunique()),
                identified_days=int(metadata.loc[known, "utc_day"].nunique()),
                target_scale_usdc=scale,
                positive_leaf_count=positive,
            )
        )
    if not action_candidates:
        raise SuccessorContractError("identified-only trees produced no positive Boolean region")

    by_action: dict[str, list[tuple[float, AndClause]]] = {}
    for score, action, clause in sorted(
        action_candidates,
        key=lambda row: (-row[0], row[1], row[2].key),
    ):
        bucket = by_action.setdefault(action, [])
        if any(existing.key == clause.key for _, existing in bucket):
            continue
        if len(bucket) < profile.max_clauses_per_rule:
            bucket.append((score, clause))
    ranked_actions = sorted(
        by_action,
        key=lambda action: (-max(score for score, _ in by_action[action]), action),
    )[: profile.max_rules]
    rules = tuple(
        BooleanRule(
            action=action,
            clauses=tuple(sorted((clause for _, clause in by_action[action]), key=lambda item: item.key)),
        )
        for action in ranked_actions
    )
    policy = BooleanCooldownPolicy(side=side, rules=rules)
    audit = PolicyFitAudit(
        side=str(side).upper(),
        profile=profile.name,
        feature_count=len(names),
        compiled_rule_count=len(policy.rules),
        compiled_clause_count=sum(len(rule.clauses) for rule in policy.rules),
        compiled_literal_count=sum(
            len(clause.literals) for rule in policy.rules for clause in rule.clauses
        ),
        maximum_clause_literals=max(
            len(clause.literals) for rule in policy.rules for clause in rule.clauses
        ),
        uses_neutral_zero_targets=False,
        action_audits=tuple(action_audits),
        candidate_id=policy.candidate_id,
    )
    return policy, audit


def current_exact_owner_policy() -> BooleanCooldownPolicy:
    """Return the documented active Boolean semantics without loading private bytes."""

    return BooleanCooldownPolicy(
        side="SELL",
        rules=(
            BooleanRule(
                action="FIXED_1748S",
                clauses=(
                    AndClause(
                        tuple(
                            sorted(
                                (
                                    TriLiteral(CURRENT_SHORT_CROSS),
                                    TriLiteral(CURRENT_CAMPAIGN_AGE),
                                )
                            )
                        )
                    ),
                    AndClause(
                        tuple(
                            sorted(
                                (
                                    TriLiteral(CURRENT_SHORT_CROSS, True),
                                    TriLiteral(CURRENT_CAMPAIGN_AGE),
                                )
                            )
                        )
                    ),
                ),
            ),
            BooleanRule(
                action="FIXED_166S",
                clauses=(
                    AndClause(
                        tuple(
                            sorted(
                                (
                                    TriLiteral(CURRENT_LONG_CROSS),
                                    TriLiteral(CURRENT_CAMPAIGN_AGE, True),
                                )
                            )
                        )
                    ),
                ),
            ),
            BooleanRule(
                action="FIXED_211S",
                clauses=(
                    AndClause(
                        tuple(
                            sorted(
                                (
                                    TriLiteral(CURRENT_LONG_CROSS, True),
                                    TriLiteral(CURRENT_CAMPAIGN_AGE, True),
                                )
                            )
                        )
                    ),
                ),
            ),
        ),
    )


def parse_owner_boolean_policy(payload: Mapping[str, Any]) -> BooleanCooldownPolicy:
    """Parse the executable Boolean subdocument from an exact owner artifact."""

    raw = payload.get("policy")
    if (
        payload.get("identity") != ACTIVE_OWNER_POLICY_IDENTITY
        or not isinstance(raw, Mapping)
        or raw.get("side") != "SELL"
        or raw.get("default_action") != CONTROL_ACTION
    ):
        raise SuccessorContractError("exact owner policy identity drifted")
    rules: list[BooleanRule] = []
    try:
        for raw_rule in raw["ordered_first_match_rules"]:
            clauses = tuple(
                AndClause(
                    tuple(
                        sorted(
                            TriLiteral(
                                predicate=str(literal["predicate"]),
                                negated=bool(literal.get("negated", False)),
                            )
                            for literal in clause["literals"]
                        )
                    )
                )
                for clause in raw_rule["clauses"]
            )
            rules.append(
                BooleanRule(
                    action=str(raw_rule["action"]),
                    clauses=tuple(sorted(clauses, key=lambda clause: clause.key)),
                )
            )
    except (KeyError, TypeError, ValueError, NestedOofContractError) as exc:
        raise SuccessorContractError("exact owner Boolean payload is invalid") from exc
    return BooleanCooldownPolicy(side="SELL", rules=tuple(rules))


def _policy_signature(policy: BooleanCooldownPolicy) -> tuple[Any, ...]:
    return (
        policy.side,
        policy.default_action,
        tuple(
            (
                rule.action,
                tuple(clause.key for clause in rule.clauses),
            )
            for rule in policy.rules
        ),
    )


def audit_exact_owner_artifact_parity(
    *,
    policy_path: str | Path,
    predicate_bundle_path: str | Path,
) -> ExactOwnerArtifactParity:
    """Verify exact private bytes without exposing them in the public report."""

    policy_file = Path(policy_path).expanduser().resolve()
    bundle_file = Path(predicate_bundle_path).expanduser().resolve()
    if (
        not policy_file.is_file()
        or _sha256_file(policy_file) != ACTIVE_OWNER_POLICY_SHA256
        or not bundle_file.is_file()
        or _sha256_file(bundle_file) != ACTIVE_PREDICATE_BUNDLE_SHA256
    ):
        raise SuccessorContractError("exact owner artifact SHA256 drifted")
    try:
        payload = json.loads(policy_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SuccessorContractError("exact owner policy is unreadable") from exc
    if not isinstance(payload, Mapping):
        raise SuccessorContractError("exact owner policy must be an object")
    research_policy = parse_owner_boolean_policy(payload)
    documented_equal = _policy_signature(research_policy) == _policy_signature(
        current_exact_owner_policy()
    )
    if not documented_equal:
        raise SuccessorContractError("documented owner semantics differ from exact bytes")
    runtime = load_runtime_policy(
        policy_path=policy_file,
        predicate_bundle_path=bundle_file,
        expected_policy_sha256=ACTIVE_OWNER_POLICY_SHA256,
        expected_predicate_bundle_sha256=ACTIVE_PREDICATE_BUNDLE_SHA256,
    )
    if runtime.binding_error is not None:
        raise SuccessorContractError(
            f"exact owner runtime binding failed: {runtime.binding_error}"
        )
    columns = runtime.predicate_columns
    if columns != research_policy.predicate_columns:
        raise SuccessorContractError("exact owner predicate columns drifted")
    mismatches = 0
    sell_cases = 0
    buy_cases = 0
    for values in product((-1, 0, 1), repeat=len(columns)):
        row = dict(zip(columns, values, strict=True))
        research_action = str(research_policy.choose(pd.DataFrame([row]))[0])
        sell = runtime.evaluate_predicates(
            side="SELL",
            predicate_values=row,
            baseline_duration_ms=85_000,
            snapshot_id=f"exact-sell-{sell_cases}",
        )
        buy = runtime.evaluate_predicates(
            side="BUY",
            predicate_values=row,
            baseline_duration_ms=85_000,
            snapshot_id=f"exact-buy-{buy_cases}",
        )
        mismatches += int(sell.action_id != research_action)
        mismatches += int(buy.action_id != CONTROL_ACTION)
        sell_cases += 1
        buy_cases += 1
    return ExactOwnerArtifactParity(
        policy_sha256=ACTIVE_OWNER_POLICY_SHA256,
        predicate_bundle_sha256=ACTIVE_PREDICATE_BUNDLE_SHA256,
        predicate_columns=columns,
        sell_tri_state_cases=sell_cases,
        buy_tri_state_cases=buy_cases,
        mismatch_count=mismatches,
        documented_semantics_equal=documented_equal,
        runtime_binding_valid=True,
    )


def write_exact_owner_artifact_parity(
    parity: ExactOwnerArtifactParity,
    output_path: str | Path,
) -> str:
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": f"{IDENTITY}.exact_owner_artifact_parity.v1",
        "identity": IDENTITY,
        "artifact_id": ACTIVE_OWNER_POLICY_IDENTITY,
        "availability": "private evidence store; not distributed with the public repository",
        "parity": asdict(parity),
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "active_owner_policy_modified": False,
    }
    encoded = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise SuccessorContractError("immutable exact-owner parity receipt drifted")
        return hashlib.sha256(encoded).hexdigest()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return hashlib.sha256(encoded).hexdigest()


def _normalize_duration_ms(value: Any) -> tuple[int, str | None]:
    if isinstance(value, bool):
        return 85_000, "baseline_duration_type_invalid"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 85_000, "baseline_duration_type_invalid"
    if not math.isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        return 85_000, "baseline_duration_invalid"
    return int(numeric), None


def _duration_for_action(action: str, baseline_duration_ms: int) -> int:
    if action == CONTROL_ACTION:
        return baseline_duration_ms
    match = _FIXED_DURATION_ACTION_RE.fullmatch(str(action))
    if match is None:
        raise SuccessorContractError(f"unsupported cooldown action: {action}")
    return int(match.group(1)) * 1_000


class ResearchBooleanCooldownPolicyEvaluator:
    """Replay-only evaluator for one frozen BUY/SELL successor policy bundle.

    It implements the same ordered, three-valued first-match semantics as the
    runtime evaluator while consuming the full frozen snapshot row.  This class
    is deliberately absent from ``strategy/`` and cannot grant live authority.
    """

    def __init__(
        self,
        *,
        policies: Mapping[str, BooleanCooldownPolicy | None],
        policy_identity: str,
        policy_sha256: str | None = None,
        predicate_bundle_sha256: str | None = None,
        required_feature_block: str = "M2",
        expected_identity_hashes: Mapping[str, str] | None = None,
    ) -> None:
        if set(policies) != {"BUY", "SELL"}:
            raise SuccessorContractError(
                "research policy bundle must declare BUY and SELL explicitly"
            )
        normalized: dict[str, BooleanCooldownPolicy | None] = {}
        for side in ("BUY", "SELL"):
            policy = policies[side]
            if policy is not None and policy.side != side:
                raise SuccessorContractError("research policy side drifted")
            normalized[side] = policy
        if not str(policy_identity).strip():
            raise SuccessorContractError("research policy identity is empty")
        if required_feature_block != "M2":
            raise SuccessorContractError(
                "successor arms must share the cumulative M2 snapshot contract"
            )
        payload = {
            "identity": str(policy_identity),
            "required_feature_block": required_feature_block,
            "policies": {
                side: None if policy is None else policy.payload()
                for side, policy in normalized.items()
            },
        }
        derived_policy_sha = _canonical_sha256(payload)
        resolved_policy_sha = str(policy_sha256 or derived_policy_sha)
        if _SHA256_RE.fullmatch(resolved_policy_sha) is None:
            raise SuccessorContractError("research policy SHA256 is malformed")
        predicate_payload = {
            "identity": IDENTITY,
            "feature_block": required_feature_block,
            "predicate_columns": {
                side: [] if policy is None else list(policy.predicate_columns)
                for side, policy in normalized.items()
            },
        }
        resolved_bundle_sha = str(
            predicate_bundle_sha256 or _canonical_sha256(predicate_payload)
        )
        if _SHA256_RE.fullmatch(resolved_bundle_sha) is None:
            raise SuccessorContractError("predicate bundle SHA256 is malformed")
        expected = dict(expected_identity_hashes or {})
        for name, value in expected.items():
            if _SHA256_RE.fullmatch(str(value)) is None:
                raise SuccessorContractError(
                    f"expected snapshot identity hash is malformed: {name}"
                )
        self.policy_identity = str(policy_identity)
        self.policy_sha256 = resolved_policy_sha
        self.predicate_bundle_sha256 = resolved_bundle_sha
        self.required_feature_block = required_feature_block
        self._policies = normalized
        self._expected_identity_hashes = expected
        self._lock = Lock()
        self._evaluations = 0
        self._supported = 0
        self._fallback = 0
        self._nonbaseline = 0
        self._action_counts: dict[str, int] = {}
        self._reason_counts: dict[str, int] = {}

    @property
    def binding_valid(self) -> bool:
        return True

    @property
    def binding_error(self) -> None:
        return None

    def _control(
        self,
        *,
        baseline_duration_ms: int,
        snapshot_id: str,
        reason: str,
        support_valid: bool,
    ) -> CooldownDurationDecision:
        return CooldownDurationDecision(
            action_id=CONTROL_ACTION,
            duration_ms=baseline_duration_ms,
            fallback_reason=reason,
            matched_rule_index=None,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
            snapshot_id=snapshot_id,
            support_valid=support_valid,
        )

    def _evaluate_predicates_once(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: Any,
        snapshot_id: str,
    ) -> CooldownDurationDecision:
        baseline, baseline_error = _normalize_duration_ms(baseline_duration_ms)
        normalized_snapshot_id = str(snapshot_id).strip() or "research-predicate-row"
        if baseline_error is not None:
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason=baseline_error,
                support_valid=False,
            )
        normalized_side = str(side).strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason="runtime_side_invalid",
                support_valid=False,
            )
        policy = self._policies[normalized_side]
        if policy is None:
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason=f"{normalized_side.lower()}_control_by_contract",
                support_valid=True,
            )
        try:
            missing = sorted(set(policy.predicate_columns) - set(predicate_values))
            if missing:
                raise SuccessorContractError(
                    f"policy predicate columns missing: {missing}"
                )
            projected: dict[str, pd.Series] = {}
            for name in policy.predicate_columns:
                value = predicate_values[name]
                if isinstance(value, bool):
                    raise SuccessorContractError(
                        f"predicate must use explicit tri-state integer: {name}"
                    )
                state = int(value)
                if state not in {-1, 0, 1}:
                    raise SuccessorContractError(
                        f"predicate is outside TRUE/FALSE/UNOBSERVED: {name}"
                    )
                projected[name] = pd.Series([state], dtype=np.int8)
            predicates = pd.DataFrame(projected)
            chosen = str(policy.choose(predicates)[0])
            for index, rule in enumerate(policy.rules):
                state = int(rule.evaluate(predicates)[0])
                if state == 1:
                    if chosen != rule.action:
                        raise SuccessorContractError("Boolean first-match semantics drifted")
                    return CooldownDurationDecision(
                        action_id=chosen,
                        duration_ms=_duration_for_action(chosen, baseline),
                        fallback_reason=None,
                        matched_rule_index=index,
                        policy_sha256=self.policy_sha256,
                        predicate_bundle_sha256=self.predicate_bundle_sha256,
                        snapshot_id=normalized_snapshot_id,
                        support_valid=True,
                    )
                if state == -1:
                    if chosen != CONTROL_ACTION:
                        raise SuccessorContractError("Boolean UNOBSERVED semantics drifted")
                    return self._control(
                        baseline_duration_ms=baseline,
                        snapshot_id=normalized_snapshot_id,
                        reason=f"rule_unobserved:{index}",
                        support_valid=False,
                    )
            if chosen != CONTROL_ACTION:
                raise SuccessorContractError("Boolean default action drifted")
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason="no_rule_matched",
                support_valid=True,
            )
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, SuccessorContractError)
                else f"research_policy_evaluation_error:{type(exc).__name__}"
            )
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason=reason,
                support_valid=False,
            )

    def _record(self, decision: CooldownDurationDecision) -> None:
        reason = decision.fallback_reason or "candidate_executed"
        with self._lock:
            self._evaluations += 1
            self._supported += int(decision.support_valid)
            self._fallback += int(decision.fallback_reason is not None)
            self._nonbaseline += int(decision.action_id != CONTROL_ACTION)
            self._action_counts[decision.action_id] = (
                self._action_counts.get(decision.action_id, 0) + 1
            )
            self._reason_counts[reason] = self._reason_counts.get(reason, 0) + 1

    def evaluate_predicates(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: Any,
        snapshot_id: str,
    ) -> CooldownDurationDecision:
        decision = self._evaluate_predicates_once(
            side=side,
            predicate_values=predicate_values,
            baseline_duration_ms=baseline_duration_ms,
            snapshot_id=snapshot_id,
        )
        self._record(decision)
        return decision

    def evaluate(
        self,
        snapshot: CooldownAssignmentSnapshotV2,
        baseline_duration_ms: Any,
    ) -> CooldownDurationDecision:
        snapshot_id = str(getattr(snapshot, "snapshot_id", ""))
        baseline, baseline_error = _normalize_duration_ms(baseline_duration_ms)
        if baseline_error is not None:
            decision = self._control(
                baseline_duration_ms=baseline,
                snapshot_id=snapshot_id,
                reason=baseline_error,
                support_valid=False,
            )
            self._record(decision)
            return decision
        try:
            if not isinstance(snapshot, CooldownAssignmentSnapshotV2):
                raise SuccessorContractError("snapshot type invalid")
            if not snapshot.policy_input_valid or snapshot.policy_input is None:
                raise SuccessorContractError(
                    f"snapshot invalid:{snapshot.fallback_reason or 'policy_input_invalid'}"
                )
            if snapshot.policy_input.snapshot_id != snapshot.snapshot_id:
                raise SuccessorContractError("snapshot policy-input identity drifted")
            if snapshot.feature_block != self.required_feature_block:
                raise SuccessorContractError("snapshot feature block drifted")
            if snapshot.source_bundle_sha256 != snapshot.policy_input.source_bundle_sha256:
                raise SuccessorContractError("snapshot source bundle drifted")
            feature_row = snapshot.feature_row.to_dict()
            policy_feature_row = snapshot.policy_input.feature_row.to_dict()
            if feature_row != policy_feature_row:
                raise SuccessorContractError("snapshot policy feature row drifted")
            m0 = snapshot.m0_context.to_dict()
            side = str(feature_row.get("side", "")).upper()
            if side not in {"BUY", "SELL"} or side != str(m0.get("side", "")).upper():
                raise SuccessorContractError("snapshot side is inconsistent")
            frozen_baseline, frozen_error = _normalize_duration_ms(
                feature_row.get("baseline_duration_ms")
            )
            if frozen_error is not None or frozen_baseline != baseline:
                raise SuccessorContractError("snapshot baseline duration drifted")
            if (
                feature_row.get("feature_block") != self.required_feature_block
                or feature_row.get("support_valid") is not True
                or feature_row.get("channel_support_valid") is not True
                or feature_row.get("warmup_admitted") is not True
            ):
                raise SuccessorContractError("snapshot feature support invalid")
            observed_hashes = snapshot.identity_hashes.to_dict()
            for name, expected in self._expected_identity_hashes.items():
                if observed_hashes.get(name) != expected:
                    raise SuccessorContractError(
                        f"snapshot identity hash drifted:{name}"
                    )
            decision = self._evaluate_predicates_once(
                side=side,
                predicate_values=feature_row,
                baseline_duration_ms=baseline,
                snapshot_id=snapshot.snapshot_id,
            )
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, SuccessorContractError)
                else f"research_policy_snapshot_error:{type(exc).__name__}"
            )
            decision = self._control(
                baseline_duration_ms=baseline,
                snapshot_id=snapshot_id,
                reason=reason,
                support_valid=False,
            )
        self._record(decision)
        return decision

    def audit(self) -> dict[str, Any]:
        with self._lock:
            return {
                "identity": self.policy_identity,
                "policy_sha256": self.policy_sha256,
                "predicate_bundle_sha256": self.predicate_bundle_sha256,
                "required_feature_block": self.required_feature_block,
                "evaluations": int(self._evaluations),
                "supported": int(self._supported),
                "fallback": int(self._fallback),
                "nonbaseline": int(self._nonbaseline),
                "action_counts": dict(sorted(self._action_counts.items())),
                "reason_counts": dict(sorted(self._reason_counts.items())),
                "research_only": True,
                "action_authorized": False,
                "live_authorized": False,
            }


def _validated_repeated_arm_result(
    result: Mapping[str, Any],
    *,
    arm: str,
    policy_sha256: str,
    common_input_identity_sha256: str,
    formal_economic_mode: bool,
    expected_transport_common_source_sha256: str | None,
) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise SuccessorContractError(f"{arm} simulator result is not a mapping")
    if str(result.get("arm")) != arm:
        raise SuccessorContractError(f"{arm} simulator arm identity drifted")
    if str(result.get("policy_sha256")) != policy_sha256:
        raise SuccessorContractError(f"{arm} simulator policy identity drifted")
    if str(result.get("common_input_identity_sha256")) != common_input_identity_sha256:
        raise SuccessorContractError(f"{arm} simulator common input drifted")
    if result.get("execution_copied_from_other_arm") is not False:
        raise SuccessorContractError(f"{arm} replay was copied instead of executed")
    if result.get("one_shot_effect_aggregation_used") is not False:
        raise SuccessorContractError(f"{arm} replay used one-shot effect aggregation")
    if type(result.get("formal_support_valid")) is not bool:
        raise SuccessorContractError(f"{arm} formal support flag is invalid")
    raw_count = result.get("repeated_policy_evaluation_count")
    if isinstance(raw_count, bool) or not isinstance(raw_count, (int, np.integer)):
        raise SuccessorContractError(f"{arm} policy evaluation count is invalid")
    evaluation_count = int(raw_count)
    if evaluation_count < 0:
        raise SuccessorContractError(f"{arm} policy evaluation count is negative")
    support_valid = bool(result["formal_support_valid"])
    if support_valid and evaluation_count == 0:
        raise SuccessorContractError(
            f"{arm} claims support without executing the policy function"
        )
    terminal_value: float | None = None
    if support_valid:
        try:
            terminal_value = float(result["campaign_terminal_value_usdc"])
        except (KeyError, TypeError, ValueError) as exc:
            raise SuccessorContractError(
                f"{arm} terminal campaign value is invalid"
            ) from exc
        if not math.isfinite(terminal_value):
            raise SuccessorContractError(f"{arm} terminal campaign value is non-finite")
    raw_reasons = result.get("formal_exclusion_reasons", ())
    if not isinstance(raw_reasons, Sequence) or isinstance(raw_reasons, (str, bytes)):
        raise SuccessorContractError(f"{arm} exclusion reasons are invalid")
    reasons = tuple(str(value) for value in raw_reasons if str(value))
    if support_valid and reasons:
        raise SuccessorContractError(f"{arm} supported result carries exclusion reasons")
    if not support_valid and not reasons:
        reasons = ("formal_support_invalid",)
    receipt = None
    raw_receipt = result.get("transport_receipt")
    if formal_economic_mode or raw_receipt is not None:
        if not isinstance(raw_receipt, Mapping):
            raise SuccessorContractError(
                f"{arm} formal simulator lacks a transport receipt"
            )
        if expected_transport_common_source_sha256 is None:
            raise SuccessorContractError(
                "formal common replay identity lacks a transport source binding"
            )
        try:
            receipt = transport_adapter.validate_transport_receipt(
                raw_receipt,
                expected_arm=arm,
                expected_common_market_source_sha256=(
                    expected_transport_common_source_sha256
                ),
            )
        except transport_adapter.TransportContractError as exc:
            raise SuccessorContractError(
                f"{arm} transport receipt is invalid"
            ) from exc
        if support_valid and not receipt.formal_replay_support_valid:
            raise SuccessorContractError(
                f"{arm} claims economic support on an unsupported transport"
            )
        if not receipt.formal_replay_support_valid:
            reasons = tuple(
                dict.fromkeys(
                    (
                        *reasons,
                        *(
                            f"transport:{reason}"
                            for reason in receipt.exclusion_reasons
                        ),
                    )
                )
            )
    return {
        "support_valid": support_valid,
        "evaluation_count": evaluation_count,
        "terminal_value_usdc": terminal_value,
        "exclusion_reasons": reasons,
        "transport_receipt": receipt,
    }


def execute_paired_repeated_policy(
    *,
    utc_day: str,
    common_input_identity: Mapping[str, Any],
    exact_owner_evaluator: Any,
    candidate_evaluator: Any,
    simulator: Callable[[str, Any, Mapping[str, Any]], Mapping[str, Any]],
    formal_economic_mode: bool,
    prospective_day_admission: ProspectiveDayAdmission | None = None,
) -> PairedRepeatedPolicyAudit:
    """Execute two real sequential policies; never synthesize a no-difference arm."""

    day = pd.Timestamp(utc_day)
    if day.tzinfo is not None:
        day = day.tz_convert("UTC").tz_localize(None)
    if day != day.normalize():
        raise SuccessorContractError("paired repeated-policy day is not a UTC date")
    normalized_day = day.strftime("%Y-%m-%d")
    if _economic_keys(common_input_identity):
        raise SuccessorContractError("common replay identity contains economic outcomes")
    try:
        frozen_common = json.loads(
            json.dumps(
                common_input_identity,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise SuccessorContractError("common replay identity is not canonical JSON") from exc
    if not isinstance(frozen_common, Mapping) or not frozen_common:
        raise SuccessorContractError("common replay identity is empty")
    common_sha = _canonical_sha256(frozen_common)
    expected_transport_source_sha: str | None = None
    raw_transport_source_sha = frozen_common.get(
        "transport_common_market_source_sha256"
    )
    if raw_transport_source_sha is not None:
        expected_transport_source_sha = str(raw_transport_source_sha)
        if _SHA256_RE.fullmatch(expected_transport_source_sha) is None:
            raise SuccessorContractError(
                "common replay transport source SHA256 is invalid"
            )
    control_sha = str(getattr(exact_owner_evaluator, "policy_sha256", ""))
    candidate_sha = str(getattr(candidate_evaluator, "policy_sha256", ""))
    if control_sha != ACTIVE_OWNER_POLICY_SHA256:
        raise SuccessorContractError("control arm is not the exact active owner artifact")
    if _SHA256_RE.fullmatch(candidate_sha) is None:
        raise SuccessorContractError("candidate policy identity is invalid")
    if exact_owner_evaluator is candidate_evaluator:
        raise SuccessorContractError("paired arms share one evaluator instance")
    if not bool(getattr(exact_owner_evaluator, "binding_valid", False)):
        raise SuccessorContractError("exact owner evaluator binding is invalid")
    if not bool(getattr(candidate_evaluator, "binding_valid", False)):
        raise SuccessorContractError("candidate evaluator binding is invalid")
    if formal_economic_mode:
        if candidate_sha == control_sha:
            raise SuccessorContractError(
                "formal candidate cannot reuse the exact owner policy identity"
            )
        if expected_transport_source_sha is None:
            raise SuccessorContractError(
                "formal economics require a common transport source binding"
            )
        if prospective_day_admission is None:
            raise SuccessorContractError(
                "formal economics require an outcome-blind prospective day admission"
            )
        if (
            prospective_day_admission.utc_day != normalized_day
            or not prospective_day_admission.eligible
        ):
            raise SuccessorContractError(
                "prospective day is not eligible for formal repeated-policy economics"
            )

    raw_control = simulator("control", exact_owner_evaluator, dict(frozen_common))
    raw_candidate = simulator("candidate", candidate_evaluator, dict(frozen_common))
    control = _validated_repeated_arm_result(
        raw_control,
        arm="control",
        policy_sha256=control_sha,
        common_input_identity_sha256=common_sha,
        formal_economic_mode=formal_economic_mode,
        expected_transport_common_source_sha256=expected_transport_source_sha,
    )
    candidate = _validated_repeated_arm_result(
        raw_candidate,
        arm="candidate",
        policy_sha256=candidate_sha,
        common_input_identity_sha256=common_sha,
        formal_economic_mode=formal_economic_mode,
        expected_transport_common_source_sha256=expected_transport_source_sha,
    )
    exclusion_reasons: list[str] = []
    if not formal_economic_mode:
        exclusion_reasons.append("mechanics_only_not_formal_economic_mode")
    for arm, result in (("control", control), ("candidate", candidate)):
        if not result["support_valid"]:
            exclusion_reasons.extend(
                f"{arm}:{reason}" for reason in result["exclusion_reasons"]
            )
        if result["evaluation_count"] == 0:
            exclusion_reasons.append(f"{arm}:policy_function_not_executed")
    eligible = formal_economic_mode and not exclusion_reasons
    delta = None
    if eligible:
        delta = float(candidate["terminal_value_usdc"]) - float(
            control["terminal_value_usdc"]
        )
    control_receipt = control["transport_receipt"]
    candidate_receipt = candidate["transport_receipt"]
    return PairedRepeatedPolicyAudit(
        utc_day=normalized_day,
        common_input_identity_sha256=common_sha,
        control_policy_sha256=control_sha,
        candidate_policy_sha256=candidate_sha,
        control_repeated_policy_evaluations=int(control["evaluation_count"]),
        candidate_repeated_policy_evaluations=int(candidate["evaluation_count"]),
        control_support_valid=bool(control["support_valid"]),
        candidate_support_valid=bool(candidate["support_valid"]),
        formal_economic_mode=bool(formal_economic_mode),
        formal_denominator_eligible=eligible,
        exclusion_reasons=tuple(dict.fromkeys(exclusion_reasons)),
        terminal_value_delta_usdc=delta,
        transport_common_market_source_sha256=expected_transport_source_sha,
        control_transport_receipt_sha256=(
            control_receipt.transport_receipt_sha256
            if control_receipt is not None
            else None
        ),
        candidate_transport_receipt_sha256=(
            candidate_receipt.transport_receipt_sha256
            if candidate_receipt is not None
            else None
        ),
        control_private_fill_visibility_authority=(
            control_receipt.private_fill_visibility_authority
            if control_receipt is not None
            else None
        ),
        candidate_private_fill_visibility_authority=(
            candidate_receipt.private_fill_visibility_authority
            if candidate_receipt is not None
            else None
        ),
        transport_live_equivalent=False,
        both_arms_executed_policy_function=(
            int(control["evaluation_count"]) > 0
            and int(candidate["evaluation_count"]) > 0
        ),
        execution_copied_between_arms=False,
        one_shot_effect_aggregation_used=False,
    )


def _feature_family(name: str) -> str:
    if name.startswith("predicate::m0::"):
        return "M0"
    if "ema_pair_" in name:
        return "mid_ema"
    lowered = name.lower()
    if any(token in lowered for token in ("trade", "flow", "taker")):
        return "trade_flow"
    if any(token in lowered for token in ("depth", "refill", "depletion", "queue")):
        return "depth"
    return "other"


def audit_policy_semantics(
    policy: BooleanCooldownPolicy,
    *,
    candidate_source_block: str,
) -> PolicySemanticAudit:
    """Report tri-state redundancies without rewriting executable semantics."""

    redundancies: list[SemanticRedundancy] = []
    simplified: list[Mapping[str, Any]] = []
    guards: set[str] = set()
    economic: set[str] = set(policy.predicate_columns)
    for rule_index, rule in enumerate(policy.rules):
        rule_findings: list[dict[str, Any]] = []
        for left_index, left in enumerate(rule.clauses):
            left_map = {literal.predicate: literal.negated for literal in left.literals}
            for right in rule.clauses[left_index + 1 :]:
                right_map = {literal.predicate: literal.negated for literal in right.literals}
                common = tuple(
                    sorted(
                        (name, negated)
                        for name, negated in left_map.items()
                        if right_map.get(name) == negated
                    )
                )
                left_only = {
                    name: negated
                    for name, negated in left_map.items()
                    if name not in dict(common)
                }
                right_only = {
                    name: negated
                    for name, negated in right_map.items()
                    if name not in dict(common)
                }
                if (
                    len(left_only) == 1
                    and left_only.keys() == right_only.keys()
                    and next(iter(left_only.values())) != next(iter(right_only.values()))
                ):
                    guard = next(iter(left_only))
                    guards.add(guard)
                    economic.discard(guard)
                    finding = SemanticRedundancy(
                        rule_index=rule_index,
                        action=rule.action,
                        readiness_guard_predicate=guard,
                        common_literals=common,
                        observed_state_equivalence=(
                            "when the guard is observed, complementary clauses reduce to "
                            "their common literals; UNOBSERVED still fails closed"
                        ),
                    )
                    redundancies.append(finding)
                    rule_findings.append(asdict(finding))
        simplified.append(
            {
                "rule_index": rule_index,
                "action": rule.action,
                "audit_only": True,
                "findings": rule_findings,
            }
        )
    families = tuple(sorted({_feature_family(name) for name in policy.predicate_columns}))
    uses_true_m2 = bool(set(families) & {"trade_flow", "depth"})
    return PolicySemanticAudit(
        candidate_source_block=str(candidate_source_block),
        compiled_feature_families=families,
        uses_m2_incremental_features=uses_true_m2,
        readiness_guard_predicates=tuple(sorted(guards)),
        economic_branch_features=tuple(sorted(economic)),
        redundancies=tuple(redundancies),
        simplified_semantics=tuple(simplified),
    )


def complete_feature_hierarchy(
    *,
    prefix: str,
    sides: Sequence[str] = ("BUY", "SELL"),
) -> dict[str, tuple[str, ...]]:
    return {
        str(side).upper(): tuple(
            f"{prefix}:{str(side).upper()}:{suffix}"
            for suffix in COMPLETE_HIERARCHY_SUFFIXES
        )
        for side in sides
    }


def apply_complete_feature_hierarchy(
    bands: Mapping[str, SimultaneousBand] | SimultaneousBandFamily,
    *,
    prefix: str,
    economic_epsilon_usdc: float = 0.0,
    censoring: Mapping[str, CensoringSensitivity] | None = None,
) -> HierarchyDecision:
    """Apply three value gates, then block a Boolean policy if continuous wins.

    The fourth contrast is oriented as ``continuous - Boolean``.  A positive
    lower bound therefore means the Boolean representation is significantly
    dominated; it is not another positive advancement gate.
    """

    hierarchy = complete_feature_hierarchy(prefix=prefix)
    value_hierarchy = {
        side: hypotheses[:-1] for side, hypotheses in hierarchy.items()
    }
    value_decision = apply_feature_hierarchy(
        bands,
        hierarchy=value_hierarchy,
        economic_epsilon_usdc=economic_epsilon_usdc,
        censoring=censoring,
    )
    family = bands.bands if isinstance(bands, SimultaneousBandFamily) else bands
    steps: dict[str, tuple[HierarchyStepDecision, ...]] = {}
    supported: list[str] = []
    for side, hypotheses in hierarchy.items():
        prior = list(value_decision.steps[side])
        comparator_name = hypotheses[-1]
        comparator = family[comparator_name]
        if not all(step.passed for step in prior):
            comparator_step = HierarchyStepDecision(
                hypothesis=comparator_name,
                tested=False,
                passed=False,
                simultaneous_lcb_usdc=comparator.lcb_usdc,
                reason="parent_feature_block_not_supported",
            )
        elif comparator.lcb_usdc > economic_epsilon_usdc:
            comparator_step = HierarchyStepDecision(
                hypothesis=comparator_name,
                tested=True,
                passed=False,
                simultaneous_lcb_usdc=comparator.lcb_usdc,
                reason="continuous_comparator_superior",
            )
        else:
            comparator_step = HierarchyStepDecision(
                hypothesis=comparator_name,
                tested=True,
                passed=True,
                simultaneous_lcb_usdc=comparator.lcb_usdc,
                reason="boolean_not_proven_dominated_by_continuous",
            )
        side_steps = tuple((*prior, comparator_step))
        steps[side] = side_steps
        if all(step.passed for step in side_steps):
            supported.append(side)
    return HierarchyDecision(
        steps=steps,
        supported_sides=tuple(supported),
        economic_epsilon_usdc=economic_epsilon_usdc,
    )


def summarize_fold_policy_stability(
    fold_policies: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Produce the stability fields required by the successor OOF report."""

    predicate_sets: list[set[str]] = []
    pair_counts: dict[str, int] = {}
    fold_rows: list[dict[str, Any]] = []
    for fold in fold_policies:
        policy = fold.get("policy", fold)
        rules = policy.get("ordered_first_match_rules", [])
        predicates = {
            str(literal["predicate"])
            for rule in rules
            for clause in rule.get("clauses", [])
            for literal in clause.get("literals", [])
        }
        predicate_sets.append(predicates)
        pairs = sorted(
            {
                f"{pair[0]:g}/{pair[1]:g}"
                for name in predicates
                if (pair := _parse_ema_pair(name)) is not None
            }
        )
        for pair in pairs:
            pair_counts[pair] = pair_counts.get(pair, 0) + 1
        fold_rows.append(
            {
                "fold_id": str(fold.get("fold_id", "")),
                "selected_profile": fold.get("selected_profile"),
                "selected_predicates": sorted(predicates),
                "selected_ema_pairs_s": pairs,
                "action_vocabulary": sorted(
                    {str(rule.get("action")) for rule in rules}
                ),
                "rule_count": len(rules),
                "clause_count": sum(len(rule.get("clauses", [])) for rule in rules),
                "literal_count": sum(
                    len(clause.get("literals", []))
                    for rule in rules
                    for clause in rule.get("clauses", [])
                ),
                "unsupported_count": int(fold.get("unsupported_count", 0)),
                "unobserved_count": int(fold.get("unobserved_count", 0)),
            }
        )
    adjacent_jaccard = []
    for left, right in zip(predicate_sets, predicate_sets[1:], strict=False):
        union = left | right
        adjacent_jaccard.append(len(left & right) / len(union) if union else 1.0)
    return {
        "folds": fold_rows,
        "pair_inclusion_frequency": dict(sorted(pair_counts.items())),
        "adjacent_fold_predicate_jaccard": adjacent_jaccard,
        "learning_algorithm_oof_only": True,
        "exact_final_artifact_oof_available": False,
    }


if __name__ == "__main__":  # pragma: no cover - exercised through CLI smoke.
    raise SystemExit(main())
