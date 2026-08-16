"""Fail-closed nested chronological OOF executor for the F05 successor.

This module deliberately owns no dataset loader and writes no artifact.  Its
formal path accepts an outcome-blind mechanics panel, materializes one-shot
learning labels once per side and outer fold through a hash-bound provider, fits
fold-local learning algorithms, and delegates *sequential* policy economics to
a supplied evaluator.  The evaluator contract rejects one-shot aggregation,
non-owner controls, and results from days outside the frozen fold.

Preconstructed labels remain available only as a non-formal compatibility path
for synthetic regression tests.  They are not a substitute for the fold-scoped
provider in a formal execution.

The output is evidence for the learning algorithm's fold-specific policies.
It is never labelled as OOF evidence for a later full-Development refit.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeRegressor

from models.audit.experiment_scorecard import (
    CANONICAL_EVIDENCE_SCHEMA_VERSION,
    score_canonical_evidence,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 import (
    ACTIVE_OWNER_POLICY_SHA256,
    SCORE_PROFILE_CONTRACT,
    SUCCESSOR_CANDIDATE_LADDER,
    FeaturePoolAudit,
    ProspectiveFoldManifest,
    SuccessorContractError,
    SuccessorSearchProfile,
    audit_full_ema_universe,
    build_identified_action_targets_against_policy,
    build_inner_train_feature_pool,
    fit_identified_action_policy,
    full_ema_pair_prefixes,
    summarize_fold_policy_stability,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    duration_vocabulary,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference import (
    SimultaneousBand,
    SimultaneousBandFamily,
    webb_wild_day_max_t,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1."
    "nested_chronological_oof.v1"
)
PANEL_ROLE = "new_prospective_development"
OFFLINE_HISTORICAL_PANEL_ROLE = "family_specific_unconsumed_historical_development"
OOF_EVIDENCE_SCOPE = "learning_algorithm_fold_specific_policies"
CONTINUOUS_COMPARATOR = "CONTINUOUS_COMPARATOR"
FIRST_ELIGIBLE_DAY = "2026-08-13"
REQUIRED_METADATA_COLUMNS = (
    "utc_day",
    "panel_role",
    "side",
    "campaign_cluster_id",
    "assignment_ts_ns",
    "observation_end_ts_ns",
)
REQUIRED_EVALUATION_COLUMNS = (
    "utc_day",
    "side",
    "panel_role",
    "candidate_terminal_value_usdc",
    "exact_owner_terminal_value_usdc",
    "point_identified",
    "policy_assignment_count",
    "nonbaseline_action_count",
    "feature_ready_active_treatment_events",
    "repeated_sequential_policy",
    "one_shot_effect_aggregation_used",
    "exact_current_owner_row_wise_baseline",
    "candidate_executed_policy_sha256",
    "exact_owner_executed_policy_sha256",
    "paired_replay_receipt_sha256",
    "candidate_target_side",
    "same_market_source",
    "common_random_source",
    "arm_local_state",
    "common_row_count",
    "common_campaign_count",
    "candidate_closed_campaign_value_usdc",
    "exact_owner_closed_campaign_value_usdc",
    "candidate_campaign_q10_usdc",
    "exact_owner_campaign_q10_usdc",
    "candidate_campaign_cvar10_usdc",
    "exact_owner_campaign_cvar10_usdc",
    "candidate_inventory_time_btc_s",
    "exact_owner_inventory_time_btc_s",
    "candidate_max_abs_inventory_btc",
    "exact_owner_max_abs_inventory_btc",
    "candidate_fill_count",
    "exact_owner_fill_count",
    "candidate_negative_terminal_rate",
    "exact_owner_negative_terminal_rate",
    "candidate_campaign_mae_usdc",
    "exact_owner_campaign_mae_usdc",
    "candidate_repair_event_rate",
    "exact_owner_repair_event_rate",
    "candidate_mean_repair_time_s",
    "exact_owner_mean_repair_time_s",
    "candidate_censoring_rate",
    "exact_owner_censoring_rate",
)
REQUIRED_COUNT_PREFIXES = (
    "action_count::",
    "role_count::",
    "consecutive_units_count::",
    "fallback_count::",
)
ECONOMIC_PAIR_COLUMNS = (
    ("candidate_terminal_value_usdc", "exact_owner_terminal_value_usdc"),
    (
        "candidate_closed_campaign_value_usdc",
        "exact_owner_closed_campaign_value_usdc",
    ),
    ("candidate_campaign_q10_usdc", "exact_owner_campaign_q10_usdc"),
    ("candidate_campaign_cvar10_usdc", "exact_owner_campaign_cvar10_usdc"),
    (
        "candidate_inventory_time_btc_s",
        "exact_owner_inventory_time_btc_s",
    ),
    (
        "candidate_max_abs_inventory_btc",
        "exact_owner_max_abs_inventory_btc",
    ),
)
RISK_METRIC_COLUMNS = {
    "closed_campaign_value": (
        "candidate_closed_campaign_value_usdc",
        "exact_owner_closed_campaign_value_usdc",
        1.0,
    ),
    "campaign_q10": (
        "candidate_campaign_q10_usdc",
        "exact_owner_campaign_q10_usdc",
        1.0,
    ),
    "campaign_cvar10": (
        "candidate_campaign_cvar10_usdc",
        "exact_owner_campaign_cvar10_usdc",
        1.0,
    ),
    "negative_terminal_protection": (
        "candidate_negative_terminal_rate",
        "exact_owner_negative_terminal_rate",
        -1.0,
    ),
    "campaign_mae_avoidance": (
        "candidate_campaign_mae_usdc",
        "exact_owner_campaign_mae_usdc",
        -1.0,
    ),
    "repair_event": (
        "candidate_repair_event_rate",
        "exact_owner_repair_event_rate",
        1.0,
    ),
    "repair_time_avoidance_s": (
        "candidate_mean_repair_time_s",
        "exact_owner_mean_repair_time_s",
        -1.0,
    ),
    "censoring_avoidance": (
        "candidate_censoring_rate",
        "exact_owner_censoring_rate",
        -1.0,
    ),
    "inventory_time_avoidance": (
        "candidate_inventory_time_btc_s",
        "exact_owner_inventory_time_btc_s",
        -1.0,
    ),
    "max_abs_inventory_avoidance": (
        "candidate_max_abs_inventory_btc",
        "exact_owner_max_abs_inventory_btc",
        -1.0,
    ),
}
LEARNED_BOOLEAN_ORDER = (
    "E1_FULL_EMA_BANK",
    "E2_DIRECTIONAL_EMA",
    "E3_HIGHER_ORDER_BOOLEAN",
    "M2_TRUE_INCREMENTAL",
)
MATCHED_CONTROL_NAMES = tuple(
    f"ACTION_MATCHED_CONTROLS::{source}" for source in LEARNED_BOOLEAN_ORDER
)
CONFIRMATORY_COMPARISONS = (
    ("E1-B0", "E1_FULL_EMA_BANK", "B0_CURRENT_EXACT"),
    ("E1-B1", "E1_FULL_EMA_BANK", "B1_CAMPAIGN_AGE_ONLY"),
    ("E1-B2", "E1_FULL_EMA_BANK", "B2_CAMPAIGN_PLUS_H16_H256"),
    ("E1-B3", "E1_FULL_EMA_BANK", "B3_CURRENT_SEMANTIC_EQUIVALENT"),
    ("E2-E1", "E2_DIRECTIONAL_EMA", "E1_FULL_EMA_BANK"),
    ("E3-E2", "E3_HIGHER_ORDER_BOOLEAN", "E2_DIRECTIONAL_EMA"),
    ("M2-E3", "M2_TRUE_INCREMENTAL", "E3_HIGHER_ORDER_BOOLEAN"),
    *tuple(
        (
            f"{source}-ACTION_MATCHED",
            source,
            f"ACTION_MATCHED_CONTROLS::{source}",
        )
        for source in LEARNED_BOOLEAN_ORDER
    ),
    *tuple(
        (
            f"CONTINUOUS-{source}",
            CONTINUOUS_COMPARATOR,
            source,
        )
        for source in LEARNED_BOOLEAN_ORDER
    ),
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LEARNED_BOOLEAN_NAMES = set(LEARNED_BOOLEAN_ORDER)
_FIXED_NAMES = {
    "B1_CAMPAIGN_AGE_ONLY",
    "B2_CAMPAIGN_PLUS_H16_H256",
    "B3_CURRENT_SEMANTIC_EQUIVALENT",
}


class NestedOofExecutionError(ValueError):
    """Raised when a fold, target, candidate, or evaluator violates the contract."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_side(value: Any) -> str:
    side = str(value).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise NestedOofExecutionError("side must be BUY or SELL")
    return side


def _normalize_day(value: Any) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise NestedOofExecutionError(f"invalid UTC day: {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    if parsed != parsed.normalize():
        raise NestedOofExecutionError(f"UTC day includes a time component: {value!r}")
    return parsed.strftime("%Y-%m-%d")


class DecisionPolicy(Protocol):
    @property
    def predicate_columns(self) -> tuple[str, ...]: ...

    def choose(self, features: pd.DataFrame) -> np.ndarray: ...

    def payload(self) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class FoldScopedOneShotLabelRequest:
    """Outcome-blind request for exactly one side's purged outer-train rows."""

    side: str
    outer_fold_id: str
    train_days: tuple[str, ...]
    row_ids: tuple[str, ...]
    row_sha256: str
    mechanics_sha256: str
    duration_vocabulary: tuple[str, ...]
    request_sha256: str


@dataclass(frozen=True, slots=True)
class FoldScopedOneShotLabelBatch:
    """Identified-only one-shot labels bound to one formal request receipt."""

    side: str
    outer_fold_id: str
    train_days: tuple[str, ...]
    outcomes: pd.DataFrame
    supported: pd.DataFrame
    request_sha256: str
    row_sha256: str
    label_payload_sha256: str
    provider_identity: str
    provider_artifact_sha256: str
    receipt_sha256: str


class FoldScopedOneShotLabelProvider(Protocol):
    def __call__(
        self,
        request: FoldScopedOneShotLabelRequest,
    ) -> FoldScopedOneShotLabelBatch: ...


@dataclass(frozen=True, slots=True)
class CandidateLadderEntry:
    """One preregistered candidate algorithm, before any fold is fitted."""

    name: str
    kind: Literal["exact_owner", "fixed", "boolean", "action_matched"]
    feature_names_by_side: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    fixed_policy_by_side: Mapping[str, DecisionPolicy] = field(default_factory=dict)
    profiles: tuple[SuccessorSearchProfile, ...] = ()
    required_features_by_side: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    match_sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.name not in SUCCESSOR_CANDIDATE_LADDER:
            raise NestedOofExecutionError(f"unknown ladder candidate: {self.name}")
        if self.kind == "exact_owner" and self.name != "B0_CURRENT_EXACT":
            raise NestedOofExecutionError("only B0_CURRENT_EXACT may be exact_owner")
        if self.kind == "fixed" and self.name not in _FIXED_NAMES:
            raise NestedOofExecutionError("only B1/B2/B3 may be fixed candidates")
        if self.kind == "boolean" and self.name not in _LEARNED_BOOLEAN_NAMES:
            raise NestedOofExecutionError("only E1/E2/E3/M2 may be learned Boolean candidates")
        if self.kind == "action_matched":
            if (
                self.name != "ACTION_MATCHED_CONTROLS"
                or self.match_sources != LEARNED_BOOLEAN_ORDER
            ):
                raise NestedOofExecutionError(
                    "action-matched controls require every learned Boolean source"
                )
        elif self.match_sources:
            raise NestedOofExecutionError("only action-matched controls may name sources")
        if self.kind == "boolean" and not self.profiles:
            raise NestedOofExecutionError(f"{self.name} has no bounded search profile")
        if self.kind != "boolean" and self.profiles:
            raise NestedOofExecutionError("profiles are valid only for learned Boolean candidates")


@dataclass(frozen=True, slots=True)
class ContinuousComparatorEntry:
    feature_names_by_side: Mapping[str, tuple[str, ...]]
    profiles: tuple[SuccessorSearchProfile, ...]
    name: str = CONTINUOUS_COMPARATOR

    def __post_init__(self) -> None:
        if self.name != CONTINUOUS_COMPARATOR or not self.profiles:
            raise NestedOofExecutionError("continuous comparator contract is incomplete")


@dataclass(frozen=True, slots=True)
class HierarchyCandidates:
    e1: str = "E1_FULL_EMA_BANK"
    e2: str = "E2_DIRECTIONAL_EMA"
    e3: str = "E3_HIGHER_ORDER_BOOLEAN"
    m2: str = "M2_TRUE_INCREMENTAL"
    boolean: str = "M2_TRUE_INCREMENTAL"
    continuous: str = CONTINUOUS_COMPARATOR

    def __post_init__(self) -> None:
        ladder = set(SUCCESSOR_CANDIDATE_LADDER)
        if {self.e1, self.e2, self.e3, self.m2, self.boolean} - ladder:
            raise NestedOofExecutionError("hierarchy references an unknown ladder candidate")
        expected = (
            "E1_FULL_EMA_BANK",
            "E2_DIRECTIONAL_EMA",
            "E3_HIGHER_ORDER_BOOLEAN",
            "M2_TRUE_INCREMENTAL",
        )
        if (self.e1, self.e2, self.e3, self.m2) != expected:
            raise NestedOofExecutionError("feature hierarchy drifted from E1/E2/E3/M2")
        if self.continuous != CONTINUOUS_COMPARATOR:
            raise NestedOofExecutionError("continuous hierarchy identity drifted")


@dataclass(frozen=True, slots=True)
class NestedOofConfig:
    sides: tuple[str, ...] = ("BUY", "SELL")
    hierarchy: HierarchyCandidates = field(default_factory=HierarchyCandidates)
    simultaneous_draws: int = 99_999
    simultaneous_seed: int = 20260813
    confidence: float = 0.95
    economic_epsilon_usdc: float = 0.0
    zero_difference_tolerance_usdc: float = 1e-12
    panel_role: str = PANEL_ROLE
    earliest_eligible_day: str | None = FIRST_ELIGIBLE_DAY

    def __post_init__(self) -> None:
        normalized = tuple(_normalize_side(side) for side in self.sides)
        if not normalized or len(set(normalized)) != len(normalized):
            raise NestedOofExecutionError("sides must be non-empty and unique")
        object.__setattr__(self, "sides", normalized)
        if self.simultaneous_draws < 1 or not 0.5 < self.confidence < 1.0:
            raise NestedOofExecutionError("simultaneous-band settings are invalid")
        if not math.isfinite(self.economic_epsilon_usdc):
            raise NestedOofExecutionError("economic epsilon must be finite")
        if self.zero_difference_tolerance_usdc < 0.0:
            raise NestedOofExecutionError("zero-difference tolerance must be nonnegative")
        if not str(self.panel_role).strip():
            raise NestedOofExecutionError("panel role must be non-empty")
        if self.earliest_eligible_day is not None:
            object.__setattr__(
                self,
                "earliest_eligible_day",
                _normalize_day(self.earliest_eligible_day),
            )


def _pandas_content_sha256(value: pd.DataFrame | pd.Series) -> str:
    frame = value.to_frame() if isinstance(value, pd.Series) else value
    header = {
        "columns": [str(column) for column in frame.columns],
        "dtypes": [str(dtype) for dtype in frame.dtypes],
        "index_name": None if frame.index.name is None else str(frame.index.name),
        "index_dtype": str(frame.index.dtype),
        "rows": int(len(frame)),
    }
    digest = hashlib.sha256()
    digest.update(_canonical_sha256(header).encode("ascii"))
    hashed = pd.util.hash_pandas_object(frame, index=True, categorize=False)
    digest.update(hashed.to_numpy(dtype="<u8", copy=False).tobytes())
    return digest.hexdigest()


def _validate_action_label_frames(
    outcomes: pd.DataFrame,
    supported: pd.DataFrame,
    *,
    expected_index: pd.Index,
    required_vocabulary: Sequence[str],
    exact_vocabulary: bool,
) -> None:
    if not isinstance(outcomes, pd.DataFrame) or not isinstance(supported, pd.DataFrame):
        raise NestedOofExecutionError("action outcomes and support must be DataFrames")
    if not outcomes.index.equals(expected_index) or not supported.index.equals(expected_index):
        raise NestedOofExecutionError(
            "fold-scoped labels contain rows outside the requested outer-train index"
        )
    if tuple(outcomes.columns) != tuple(supported.columns):
        raise NestedOofExecutionError("outcome and support action vocabularies drifted")
    required = tuple(str(action) for action in required_vocabulary)
    observed = tuple(str(action) for action in outcomes.columns)
    if exact_vocabulary and observed != required:
        raise NestedOofExecutionError(
            "fold-scoped labels must contain the complete ordered duration vocabulary"
        )
    if not exact_vocabulary and not set(required) <= set(observed):
        raise NestedOofExecutionError("action label vocabulary is incomplete")
    if supported.isna().to_numpy().any() or not all(
        pd.api.types.is_bool_dtype(dtype) for dtype in supported.dtypes
    ):
        raise NestedOofExecutionError("action support must be an observed Boolean mask")
    support = supported.to_numpy(dtype=bool)
    try:
        values = outcomes.to_numpy(dtype=float)
    except (TypeError, ValueError) as exc:
        raise NestedOofExecutionError("action outcomes must be numeric") from exc
    if not np.isfinite(values[support]).all():
        raise NestedOofExecutionError("supported action targets must be finite")
    if not np.isnan(values[~support]).all():
        raise NestedOofExecutionError(
            "unsupported action targets must remain NaN; neutral-zero imputation is forbidden"
        )


def _label_payload_sha256(
    *,
    side: str,
    outcomes: pd.DataFrame,
    supported: pd.DataFrame,
) -> str:
    digest = hashlib.sha256()
    digest.update(
        _canonical_sha256(
            {
                "schema": f"{IDENTITY}.fold_scoped_one_shot_labels.v1",
                "side": _normalize_side(side),
                "row_ids": [str(value) for value in outcomes.index],
                "actions": [str(value) for value in outcomes.columns],
            }
        ).encode("ascii")
    )
    support = supported.to_numpy(dtype=bool)
    values = outcomes.to_numpy(dtype="<f8", copy=True)
    values[~support] = np.nan
    digest.update(support.astype(np.uint8, copy=False).tobytes(order="C"))
    digest.update(values.tobytes(order="C"))
    return digest.hexdigest()


def _fold_label_receipt_sha256(
    *,
    request_sha256: str,
    label_payload_sha256: str,
    provider_identity: str,
    provider_artifact_sha256: str,
) -> str:
    return _canonical_sha256(
        {
            "schema": f"{IDENTITY}.fold_scoped_one_shot_label_receipt.v1",
            "request_sha256": request_sha256,
            "label_payload_sha256": label_payload_sha256,
            "provider_identity": provider_identity,
            "provider_artifact_sha256": provider_artifact_sha256,
        }
    )


def bind_fold_scoped_one_shot_labels(
    request: FoldScopedOneShotLabelRequest,
    *,
    outcomes: pd.DataFrame,
    supported: pd.DataFrame,
    provider_identity: str,
    provider_artifact_sha256: str,
) -> FoldScopedOneShotLabelBatch:
    """Build a receipt-bound provider response without reading evaluation outcomes."""

    if not str(provider_identity).strip():
        raise NestedOofExecutionError("label provider identity must be non-empty")
    if _SHA256_RE.fullmatch(str(provider_artifact_sha256)) is None:
        raise NestedOofExecutionError("label provider artifact SHA256 is invalid")
    expected_index = pd.Index(request.row_ids, name=outcomes.index.name)
    _validate_action_label_frames(
        outcomes,
        supported,
        expected_index=expected_index,
        required_vocabulary=request.duration_vocabulary,
        exact_vocabulary=True,
    )
    payload_sha256 = _label_payload_sha256(
        side=request.side,
        outcomes=outcomes,
        supported=supported,
    )
    return FoldScopedOneShotLabelBatch(
        side=request.side,
        outer_fold_id=request.outer_fold_id,
        train_days=request.train_days,
        outcomes=outcomes.copy(),
        supported=supported.copy(),
        request_sha256=request.request_sha256,
        row_sha256=request.row_sha256,
        label_payload_sha256=payload_sha256,
        provider_identity=str(provider_identity),
        provider_artifact_sha256=str(provider_artifact_sha256),
        receipt_sha256=_fold_label_receipt_sha256(
            request_sha256=request.request_sha256,
            label_payload_sha256=payload_sha256,
            provider_identity=str(provider_identity),
            provider_artifact_sha256=str(provider_artifact_sha256),
        ),
    )


@dataclass(frozen=True, slots=True)
class NestedOofPanel:
    """Outcome-blind mechanics plus optional non-formal preconstructed labels."""

    metadata: pd.DataFrame
    boolean_features: pd.DataFrame
    continuous_features: pd.DataFrame
    exact_owner_actions: pd.Series
    action_outcomes: pd.DataFrame | None = None
    action_supported: pd.DataFrame | None = None
    learning_label_request_sha256: str | None = None
    learning_label_payload_sha256: str | None = None
    learning_label_receipt_sha256: str | None = None

    @property
    def has_preconstructed_labels(self) -> bool:
        return self.action_outcomes is not None and self.action_supported is not None

    def validate(
        self,
        *,
        active_days: Sequence[str],
        sides: Sequence[str],
        panel_role: str = PANEL_ROLE,
        earliest_eligible_day: str | None = FIRST_ELIGIBLE_DAY,
    ) -> None:
        frames = (self.metadata, self.boolean_features, self.continuous_features)
        if any(not isinstance(frame, pd.DataFrame) for frame in frames):
            raise NestedOofExecutionError("panel members must be DataFrames")
        if self.metadata.empty or self.metadata.index.has_duplicates:
            raise NestedOofExecutionError("metadata must have a unique non-empty index")
        if any(not frame.index.equals(self.metadata.index) for frame in frames[1:]):
            raise NestedOofExecutionError("all panel frames must have the metadata index")
        if not isinstance(self.exact_owner_actions, pd.Series) or not self.exact_owner_actions.index.equals(
            self.metadata.index
        ):
            raise NestedOofExecutionError("row-wise exact owner actions are not aligned")
        missing = set(REQUIRED_METADATA_COLUMNS) - set(self.metadata.columns)
        if missing:
            raise NestedOofExecutionError(f"metadata columns are missing: {sorted(missing)}")
        roles = set(self.metadata["panel_role"].astype(str))
        if roles != {panel_role}:
            raise NestedOofExecutionError(
                "panel rows drifted from the explicitly bound evidence role"
            )
        observed_days = tuple(sorted({_normalize_day(day) for day in self.metadata["utc_day"]}))
        expected_days = tuple(_normalize_day(day) for day in active_days)
        if expected_days != tuple(sorted(set(expected_days))) or len(expected_days) != 30:
            raise NestedOofExecutionError("fold manifest must bind exactly 30 ordered active days")
        if observed_days != expected_days:
            raise NestedOofExecutionError("panel days drifted from the fold manifest")
        if earliest_eligible_day is not None and observed_days[0] < earliest_eligible_day:
            raise NestedOofExecutionError("panel includes a day before its evidence cutoff")
        observed_sides = {_normalize_side(value) for value in self.metadata["side"]}
        if not set(sides) <= observed_sides:
            raise NestedOofExecutionError("one or more requested sides are absent")
        for side in sides:
            side_mask = self.metadata["side"].astype(str).str.upper() == side
            allowed_actions = set(duration_vocabulary(side))
            unknown_owner = sorted(
                set(self.exact_owner_actions.loc[side_mask].astype(str)) - allowed_actions
            )
            if unknown_owner:
                raise NestedOofExecutionError(
                    f"{side} row-wise owner actions are outside its duration vocabulary: "
                    f"{unknown_owner}"
                )
        assignment = pd.to_numeric(self.metadata["assignment_ts_ns"], errors="coerce")
        observation_end = pd.to_numeric(
            self.metadata["observation_end_ts_ns"], errors="coerce"
        )
        if assignment.isna().any() or (assignment <= 0).any():
            raise NestedOofExecutionError("assignment clocks must be positive and observed")
        known_end = observation_end.notna()
        if (observation_end[known_end] < assignment[known_end]).any():
            raise NestedOofExecutionError("observation end precedes assignment")
        boolean = self.boolean_features.to_numpy(copy=False)
        if not np.isin(boolean, (-1, 0, 1)).all():
            raise NestedOofExecutionError("Boolean features must be TRUE/FALSE/UNOBSERVED")
        if (self.action_outcomes is None) != (self.action_supported is None):
            raise NestedOofExecutionError(
                "action outcomes and support must both be absent or both be present"
            )
        if self.has_preconstructed_labels:
            assert self.action_outcomes is not None
            assert self.action_supported is not None
            required_actions = tuple(
                dict.fromkeys(
                    action
                    for side in sides
                    for action in duration_vocabulary(side)
                )
            )
            _validate_action_label_frames(
                self.action_outcomes,
                self.action_supported,
                expected_index=self.metadata.index,
                required_vocabulary=required_actions,
                exact_vocabulary=False,
            )
        elif any(
            value is not None
            for value in (
                self.learning_label_request_sha256,
                self.learning_label_payload_sha256,
                self.learning_label_receipt_sha256,
            )
        ):
            raise NestedOofExecutionError("label receipt fields require materialized labels")


@dataclass(frozen=True, slots=True)
class PurgeAudit:
    fold_id: str
    stage: str
    side: str
    train_days: tuple[str, ...]
    test_days: tuple[str, ...]
    test_boundary_ts_ns: int
    rows_before: int
    rows_after: int
    cross_boundary_rows: int
    unknown_observation_end_rows: int
    shared_campaign_rows: int


@dataclass(frozen=True, slots=True)
class FittedCandidate:
    ladder_name: str
    side: str
    policy: DecisionPolicy | None
    selected_profile: str
    training_days: tuple[str, ...]
    training_row_sha256: str
    policy_payload: Mapping[str, Any]
    policy_sha256: str
    fit_audit: Mapping[str, Any]
    feature_pool_audit: Mapping[str, Any] | None
    learning_algorithm_fold_specific: bool = True

    @property
    def expected_executed_policy_sha256(self) -> str:
        if self.ladder_name == "B0_CURRENT_EXACT":
            return ACTIVE_OWNER_POLICY_SHA256
        return self.policy_sha256

    def choose(
        self,
        features: pd.DataFrame,
        exact_owner_actions: pd.Series,
    ) -> np.ndarray:
        if not features.index.equals(exact_owner_actions.index):
            raise NestedOofExecutionError("candidate inputs and owner actions are not aligned")
        owner = exact_owner_actions.astype(str).to_numpy(dtype=object)
        if self.policy is None:
            return owner.copy()
        raw = np.asarray(self.policy.choose(features), dtype=object)
        if len(raw) != len(owner):
            raise NestedOofExecutionError("candidate returned the wrong number of actions")
        result = np.where(raw == CONTROL_ACTION, owner, raw).astype(object)
        vocabulary = set(duration_vocabulary(self.side))
        if set(result) - vocabulary:
            raise NestedOofExecutionError("candidate emitted an action outside the side vocabulary")
        return result


@dataclass(frozen=True, slots=True)
class EvaluationRequest:
    candidate: FittedCandidate
    side: str
    days: tuple[str, ...]
    fold_id: str
    stage: Literal["inner_oof", "outer_oof"]
    panel_role: str = PANEL_ROLE

    @property
    def request_sha256(self) -> str:
        side = _normalize_side(self.side)
        days = tuple(_normalize_day(day) for day in self.days)
        fold_id = str(self.fold_id).strip()
        panel_role = str(self.panel_role).strip()
        return _canonical_sha256(
            {
                "schema_version": f"{IDENTITY}.evaluation_request.v1",
                "candidate_name": self.candidate.ladder_name,
                "candidate_policy_sha256": self.candidate.policy_sha256,
                "candidate_executed_policy_sha256": (
                    self.candidate.expected_executed_policy_sha256
                ),
                "candidate_training_row_sha256": self.candidate.training_row_sha256,
                "side": side,
                "days": list(days),
                "fold_id": fold_id,
                "stage": self.stage,
                "panel_role": panel_role,
            }
        )


@dataclass(frozen=True, slots=True)
class SequentialEvaluationResult:
    """One batch result bound to exactly one ``EvaluationRequest`` identity."""

    request_sha256: str
    rows: pd.DataFrame


SequentialPolicyEvaluator = Callable[[EvaluationRequest], pd.DataFrame]


class BatchSequentialPolicyEvaluator(Protocol):
    def evaluate_many(
        self,
        requests: Sequence[EvaluationRequest],
    ) -> Sequence[SequentialEvaluationResult]: ...


@dataclass(frozen=True, slots=True)
class NestedOofExecutionResult:
    oof_rows: pd.DataFrame
    fold_records: tuple[Mapping[str, Any], ...]
    candidate_reports: Mapping[str, Mapping[str, Any]]
    stability: Mapping[str, Mapping[str, Any]]
    candidate_bands: SimultaneousBandFamily
    candidate_week_bands: SimultaneousBandFamily
    hierarchy_bands: SimultaneousBandFamily
    hierarchy_week_bands: SimultaneousBandFamily
    confirmatory_bands: SimultaneousBandFamily
    confirmatory_week_bands: SimultaneousBandFamily
    risk_bands: SimultaneousBandFamily
    risk_week_bands: SimultaneousBandFamily
    scorecards: Mapping[str, Mapping[str, Any]]
    hierarchy: Mapping[str, Any]
    evidence_scope: str = OOF_EVIDENCE_SCOPE
    exact_final_artifact_oof_available: bool = False
    final_refit_performed: bool = False

    def report(self) -> dict[str, Any]:
        return {
            "schema_version": IDENTITY,
            "oof_evidence_scope": self.evidence_scope,
            "exact_final_artifact_oof_available": self.exact_final_artifact_oof_available,
            "final_refit_performed": self.final_refit_performed,
            "candidate_reports": self.candidate_reports,
            "stability": self.stability,
            "candidate_bands": _band_payload(self.candidate_bands),
            "candidate_week_bands": _band_payload(self.candidate_week_bands),
            "hierarchy_bands": _band_payload(self.hierarchy_bands),
            "hierarchy_week_bands": _band_payload(self.hierarchy_week_bands),
            "confirmatory_bands": _band_payload(self.confirmatory_bands),
            "confirmatory_week_bands": _band_payload(
                self.confirmatory_week_bands
            ),
            "risk_bands": _band_payload(self.risk_bands),
            "risk_week_bands": _band_payload(self.risk_week_bands),
            "scorecards": self.scorecards,
            "hierarchy": self.hierarchy,
            "score_profile_contract": SCORE_PROFILE_CONTRACT,
            "outer_oof_row_count": int(len(self.oof_rows)),
            "outer_fold_count": int(len(self.fold_records)),
            "permissions": {
                "final_policy_frozen": False,
                "action_authorized": False,
                "live_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        }


class _ContinuousActionPolicy:
    def __init__(
        self,
        *,
        side: str,
        feature_names: Sequence[str],
        models: Mapping[str, DecisionTreeRegressor],
    ) -> None:
        self.side = _normalize_side(side)
        self.feature_names = tuple(str(name) for name in feature_names)
        self.models = dict(models)

    @property
    def predicate_columns(self) -> tuple[str, ...]:
        return self.feature_names

    def choose(self, features: pd.DataFrame) -> np.ndarray:
        missing = set(self.feature_names) - set(features.columns)
        if missing:
            raise NestedOofExecutionError(
                f"continuous policy inputs are missing: {sorted(missing)}"
            )
        matrix = features.loc[:, list(self.feature_names)].to_numpy(dtype=float)
        complete = np.isfinite(matrix).all(axis=1)
        result = np.full(len(features), CONTROL_ACTION, dtype=object)
        if not complete.any():
            return result
        actions = tuple(sorted(self.models))
        predictions = np.column_stack(
            [self.models[action].predict(matrix[complete]) for action in actions]
        )
        best = np.argmax(predictions, axis=1)
        best_value = predictions[np.arange(len(best)), best]
        selected = np.asarray(actions, dtype=object)[best]
        result[np.flatnonzero(complete)[best_value > 0.0]] = selected[best_value > 0.0]
        return result

    def payload(self) -> Mapping[str, Any]:
        models: dict[str, Any] = {}
        for action, model in sorted(self.models.items()):
            tree = model.tree_
            models[action] = {
                "children_left": tree.children_left.tolist(),
                "children_right": tree.children_right.tolist(),
                "feature": tree.feature.tolist(),
                "threshold": tree.threshold.tolist(),
                "value": np.asarray(tree.value, dtype=float).reshape(-1).tolist(),
            }
        return {
            "identity": IDENTITY,
            "kind": "identified_only_continuous_comparator",
            "side": self.side,
            "features": list(self.feature_names),
            "models": models,
            "unknown_target_policy": "per_action_missing_mask_never_zero",
        }


class _ActionMatchedPolicy:
    def __init__(self, *, side: str, probabilities: Mapping[str, float], salt: str) -> None:
        self.side = _normalize_side(side)
        self.probabilities = dict(probabilities)
        self.salt = str(salt)
        if not self.probabilities or not math.isclose(
            sum(self.probabilities.values()), 1.0, abs_tol=1e-12
        ):
            raise NestedOofExecutionError("action-matched probabilities do not sum to one")

    @property
    def predicate_columns(self) -> tuple[str, ...]:
        return ()

    def choose(self, features: pd.DataFrame) -> np.ndarray:
        actions = tuple(sorted(self.probabilities))
        cumulative = np.cumsum([self.probabilities[action] for action in actions])
        draws = np.asarray(
            [
                int.from_bytes(
                    hashlib.sha256(f"{self.salt}|{index}".encode()).digest()[:8],
                    "big",
                )
                / float(1 << 64)
                for index in features.index
            ],
            dtype=float,
        )
        positions = np.searchsorted(cumulative, draws, side="right")
        positions = np.minimum(positions, len(actions) - 1)
        return np.asarray(actions, dtype=object)[positions]

    def payload(self) -> Mapping[str, Any]:
        return {
            "identity": IDENTITY,
            "kind": "deterministic_action_rate_and_duration_matched_control",
            "side": self.side,
            "probabilities": dict(sorted(self.probabilities.items())),
            "salt": self.salt,
            "market_state_features_used": False,
        }


def _band_payload(family: SimultaneousBandFamily) -> dict[str, Any]:
    return {
        "critical_value": family.critical_value,
        "confidence": family.confidence,
        "draws": family.draws,
        "seed": family.seed,
        "shared_days": list(family.shared_days),
        "bands": {name: asdict(band) for name, band in sorted(family.bands.items())},
    }


_E2_PAIR_SEMANTICS: Mapping[str, tuple[str, ...]] = {
    "ordering": ("ordering", "favorable"),
    "cross_direction": (
        "last_cross_direction",
        "last_cross_positive",
        "last_cross_favorable",
        "golden",
        "death",
    ),
    "cross_recency": ("cross_age", "recency"),
    "persistence": ("persistence",),
    "distance": ("signed_distance", "favorable_distance", "abs_distance", "distance_ge"),
    "normalized_distance": ("normalized_distance", "volatility_normalized", "provider_sigma"),
    "slope": ("distance_velocity", "slope"),
    "curvature": ("distance_acceleration", "curvature"),
    "convergence": ("converging",),
    "expansion": ("expanding",),
}


def _e2_pair_semantic_audit(names: Sequence[str]) -> dict[str, tuple[str, ...]]:
    normalized = tuple(str(name).lower() for name in names)
    missing: dict[str, tuple[str, ...]] = {}
    for prefix in full_ema_pair_prefixes():
        canonical = "__" + prefix.removeprefix("ema_pair_").replace("s_h", "s__h") + "::"
        pair_names = tuple(
            name for name in normalized if prefix in name or canonical in name
        )
        absent_items: list[str] = []
        for semantic, tokens in _E2_PAIR_SEMANTICS.items():
            if semantic == "normalized_distance":
                present = any(any(token in name for token in tokens) for name in pair_names) or any(
                    "tri::quantile::" in name and "distance" in name for name in pair_names
                )
            else:
                present = any(any(token in name for token in tokens) for name in pair_names)
            if not present:
                absent_items.append(semantic)
        absent = tuple(absent_items)
        if absent:
            missing[prefix] = absent
    return missing


def _validate_ladder(entries: Sequence[CandidateLadderEntry], sides: Sequence[str]) -> None:
    names = tuple(entry.name for entry in entries)
    if names != SUCCESSOR_CANDIDATE_LADDER:
        raise NestedOofExecutionError(
            "candidate ladder must contain B0/B1/B2/B3/E1/E2/E3/M2/action-matched in order"
        )
    by_name = {entry.name: entry for entry in entries}
    if by_name["B0_CURRENT_EXACT"].kind != "exact_owner":
        raise NestedOofExecutionError("B0 must be the exact row-wise owner policy")
    for entry in entries:
        if entry.kind == "fixed":
            missing = set(sides) - {str(side).upper() for side in entry.fixed_policy_by_side}
            if missing:
                raise NestedOofExecutionError(
                    f"{entry.name} lacks fixed policies for {sorted(missing)}"
                )
        if entry.kind == "boolean":
            missing = set(sides) - {str(side).upper() for side in entry.feature_names_by_side}
            if missing:
                raise NestedOofExecutionError(
                    f"{entry.name} lacks feature universes for {sorted(missing)}"
                )
    e1 = by_name["E1_FULL_EMA_BANK"]
    for side in sides:
        universe = audit_full_ema_universe(e1.feature_names_by_side[side])
        if not universe["all_45_pairs_present"]:
            raise NestedOofExecutionError(
                f"E1 {side} does not expose all 45 EMA pairs before inner-fold screening"
            )
    for side in sides:
        missing_semantics = _e2_pair_semantic_audit(
            by_name["E2_DIRECTIONAL_EMA"].feature_names_by_side[side]
        )
        if missing_semantics:
            example = dict(list(sorted(missing_semantics.items()))[:3])
            raise NestedOofExecutionError(
                f"E2 {side} lacks frozen per-pair semantics: {example}"
            )
    m2_names = " ".join(
        name.lower()
        for side in sides
        for name in by_name["M2_TRUE_INCREMENTAL"].feature_names_by_side[side]
    )
    if not any(
        token in m2_names
        for token in ("trade", "flow", "taker", "depth", "refill", "depletion", "queue")
    ):
        raise NestedOofExecutionError("M2_TRUE_INCREMENTAL lacks a real trade/depth predicate")


def _validate_fold_manifest(
    manifest: ProspectiveFoldManifest,
    *,
    earliest_eligible_day: str | None = FIRST_ELIGIBLE_DAY,
) -> None:
    if len(manifest.active_days) != 30 or len(manifest.outer_folds) != 4:
        raise NestedOofExecutionError("successor requires the frozen 30-day, four-fold manifest")
    if tuple(manifest.active_days) != tuple(sorted(set(manifest.active_days))):
        raise NestedOofExecutionError("active days are not sorted and unique")
    if earliest_eligible_day is not None and manifest.active_days[0] < earliest_eligible_day:
        raise NestedOofExecutionError("fold manifest includes consumed pre-successor evidence")
    seen_test: set[str] = set()
    for outer in manifest.outer_folds:
        train = tuple(str(day) for day in outer.get("train_days", ()))
        test = tuple(str(day) for day in outer.get("test_days", ()))
        inner = tuple(outer.get("inner_folds", ()))
        if not train or not test or len(inner) != 3 or max(train) >= min(test):
            raise NestedOofExecutionError("outer fold contract is invalid")
        if set(train) & set(test) or seen_test & set(test):
            raise NestedOofExecutionError("outer test days overlap")
        seen_test.update(test)
        for nested in inner:
            inner_train = tuple(str(day) for day in nested.get("train_days", ()))
            inner_test = tuple(str(day) for day in nested.get("test_days", ()))
            if (
                not inner_train
                or not inner_test
                or max(inner_train) >= min(inner_test)
                or set(inner_train) & set(inner_test)
                or not set(inner_train + inner_test) <= set(train)
            ):
                raise NestedOofExecutionError("inner fold contract is invalid")


def _purged_train_index(
    panel: NestedOofPanel,
    *,
    side: str,
    train_days: Sequence[str],
    test_days: Sequence[str],
    fold_id: str,
    stage: str,
) -> tuple[pd.Index, PurgeAudit]:
    metadata = panel.metadata
    train_mask = (metadata["side"].astype(str).str.upper() == side) & metadata[
        "utc_day"
    ].astype(str).isin(train_days)
    test_mask = (metadata["side"].astype(str).str.upper() == side) & metadata[
        "utc_day"
    ].astype(str).isin(test_days)
    train_index = metadata.index[train_mask]
    test_index = metadata.index[test_mask]
    if train_index.empty or test_index.empty:
        raise NestedOofExecutionError(f"{fold_id} has an empty side-specific train/test set")
    boundary = int(pd.to_numeric(metadata.loc[test_index, "assignment_ts_ns"]).min())
    ends = pd.to_numeric(metadata.loc[train_index, "observation_end_ts_ns"], errors="coerce")
    known = ends.notna()
    before = known & (ends < boundary)
    test_campaigns = set(metadata.loc[test_index, "campaign_cluster_id"].astype(str))
    shared_campaign = metadata.loc[train_index, "campaign_cluster_id"].astype(str).isin(
        test_campaigns
    )
    keep = before & ~shared_campaign
    kept = train_index[keep.to_numpy(dtype=bool)]
    if kept.empty:
        raise NestedOofExecutionError(f"washout-aware purge emptied {fold_id}")
    audit = PurgeAudit(
        fold_id=fold_id,
        stage=stage,
        side=side,
        train_days=tuple(str(day) for day in train_days),
        test_days=tuple(str(day) for day in test_days),
        test_boundary_ts_ns=boundary,
        rows_before=len(train_index),
        rows_after=len(kept),
        cross_boundary_rows=int((known & ~before).sum()),
        unknown_observation_end_rows=int((~known).sum()),
        shared_campaign_rows=int(shared_campaign.sum()),
    )
    return kept, audit


def _training_row_sha256(index: pd.Index) -> str:
    return _canonical_sha256([str(value) for value in index])


def _build_fold_scoped_label_request(
    panel: NestedOofPanel,
    *,
    side: str,
    outer_fold_id: str,
    train_days: Sequence[str],
    train_index: pd.Index,
) -> FoldScopedOneShotLabelRequest:
    if train_index.empty or not train_index.isin(panel.metadata.index).all():
        raise NestedOofExecutionError("fold-scoped label request has an invalid train index")
    if not all(isinstance(value, str) for value in train_index):
        raise NestedOofExecutionError(
            "formal fold-scoped labels require stable string opportunity identifiers"
        )
    normalized_days = tuple(_normalize_day(day) for day in train_days)
    if normalized_days != tuple(sorted(set(normalized_days))):
        raise NestedOofExecutionError("fold-scoped train days are not sorted and unique")
    metadata = panel.metadata.loc[train_index]
    if set(metadata["utc_day"].map(_normalize_day)) - set(normalized_days):
        raise NestedOofExecutionError("label request includes a row outside outer-train days")
    if set(metadata["side"].map(_normalize_side)) != {_normalize_side(side)}:
        raise NestedOofExecutionError("label request pooled sides")
    row_ids = tuple(str(value) for value in train_index)
    row_sha256 = _canonical_sha256(list(row_ids))
    mechanics_sha256 = _canonical_sha256(
        {
            "metadata_sha256": _pandas_content_sha256(metadata),
            "boolean_features_sha256": _pandas_content_sha256(
                panel.boolean_features.loc[train_index]
            ),
            "continuous_features_sha256": _pandas_content_sha256(
                panel.continuous_features.loc[train_index]
            ),
            "exact_owner_actions_sha256": _pandas_content_sha256(
                panel.exact_owner_actions.loc[train_index]
            ),
        }
    )
    vocabulary = tuple(duration_vocabulary(side))
    request_payload = {
        "schema": f"{IDENTITY}.fold_scoped_one_shot_label_request.v1",
        "side": _normalize_side(side),
        "outer_fold_id": str(outer_fold_id),
        "train_days": list(normalized_days),
        "row_ids": list(row_ids),
        "row_sha256": row_sha256,
        "mechanics_sha256": mechanics_sha256,
        "duration_vocabulary": list(vocabulary),
    }
    return FoldScopedOneShotLabelRequest(
        side=_normalize_side(side),
        outer_fold_id=str(outer_fold_id),
        train_days=normalized_days,
        row_ids=row_ids,
        row_sha256=row_sha256,
        mechanics_sha256=mechanics_sha256,
        duration_vocabulary=vocabulary,
        request_sha256=_canonical_sha256(request_payload),
    )


def _validate_fold_scoped_label_batch(
    request: FoldScopedOneShotLabelRequest,
    batch: FoldScopedOneShotLabelBatch,
) -> None:
    if not isinstance(batch, FoldScopedOneShotLabelBatch):
        raise NestedOofExecutionError(
            "formal label provider must return FoldScopedOneShotLabelBatch"
        )
    if (
        _normalize_side(batch.side) != request.side
        or str(batch.outer_fold_id) != request.outer_fold_id
        or tuple(batch.train_days) != request.train_days
    ):
        raise NestedOofExecutionError("fold-scoped label batch identity drifted")
    if batch.request_sha256 != request.request_sha256:
        raise NestedOofExecutionError("fold-scoped label request SHA256 drifted")
    if batch.row_sha256 != request.row_sha256:
        raise NestedOofExecutionError("fold-scoped label row SHA256 drifted")
    if not str(batch.provider_identity).strip() or _SHA256_RE.fullmatch(
        str(batch.provider_artifact_sha256)
    ) is None:
        raise NestedOofExecutionError("fold-scoped label provider identity is invalid")
    expected_index = pd.Index(request.row_ids, name=batch.outcomes.index.name)
    _validate_action_label_frames(
        batch.outcomes,
        batch.supported,
        expected_index=expected_index,
        required_vocabulary=request.duration_vocabulary,
        exact_vocabulary=True,
    )
    payload_sha256 = _label_payload_sha256(
        side=request.side,
        outcomes=batch.outcomes,
        supported=batch.supported,
    )
    if batch.label_payload_sha256 != payload_sha256:
        raise NestedOofExecutionError("fold-scoped label payload SHA256 drifted")
    receipt_sha256 = _fold_label_receipt_sha256(
        request_sha256=request.request_sha256,
        label_payload_sha256=payload_sha256,
        provider_identity=batch.provider_identity,
        provider_artifact_sha256=batch.provider_artifact_sha256,
    )
    if batch.receipt_sha256 != receipt_sha256:
        raise NestedOofExecutionError("fold-scoped label receipt SHA256 drifted")


def _materialize_outer_fold_learning_panel(
    panel: NestedOofPanel,
    *,
    provider: FoldScopedOneShotLabelProvider,
    side: str,
    outer_fold_id: str,
    train_days: Sequence[str],
    train_index: pd.Index,
) -> tuple[NestedOofPanel, Mapping[str, Any]]:
    request = _build_fold_scoped_label_request(
        panel,
        side=side,
        outer_fold_id=outer_fold_id,
        train_days=train_days,
        train_index=train_index,
    )
    batch = provider(request)
    _validate_fold_scoped_label_batch(request, batch)
    index = pd.Index(request.row_ids, name=panel.metadata.index.name)
    learning_panel = NestedOofPanel(
        metadata=panel.metadata.loc[index].copy(),
        boolean_features=panel.boolean_features.loc[index].copy(),
        continuous_features=panel.continuous_features.loc[index].copy(),
        exact_owner_actions=panel.exact_owner_actions.loc[index].copy(),
        action_outcomes=batch.outcomes.loc[index].copy(),
        action_supported=batch.supported.loc[index].copy(),
        learning_label_request_sha256=request.request_sha256,
        learning_label_payload_sha256=batch.label_payload_sha256,
        learning_label_receipt_sha256=batch.receipt_sha256,
    )
    return learning_panel, {
        "mode": "formal_fold_scoped_provider",
        "side": request.side,
        "outer_fold_id": request.outer_fold_id,
        "train_days": list(request.train_days),
        "row_count": len(request.row_ids),
        "row_sha256": request.row_sha256,
        "mechanics_sha256": request.mechanics_sha256,
        "request_sha256": request.request_sha256,
        "label_payload_sha256": batch.label_payload_sha256,
        "provider_identity": batch.provider_identity,
        "provider_artifact_sha256": batch.provider_artifact_sha256,
        "receipt_sha256": batch.receipt_sha256,
        "outer_test_rows_requested": 0,
        "unsupported_targets_remain_nan": True,
    }


def _freeze_candidate(
    *,
    ladder_name: str,
    side: str,
    policy: DecisionPolicy | None,
    profile: str,
    train_index: pd.Index,
    metadata: pd.DataFrame,
    fit_audit: Mapping[str, Any],
    pool_audit: FeaturePoolAudit | None,
    learning_label_request_sha256: str | None = None,
    learning_label_payload_sha256: str | None = None,
    learning_label_receipt_sha256: str | None = None,
) -> FittedCandidate:
    payload: Mapping[str, Any]
    if policy is None:
        payload = {
            "identity": IDENTITY,
            "kind": "row_wise_exact_current_owner",
            "side": side,
            "candidate": ladder_name,
        }
    else:
        payload = {
            "identity": IDENTITY,
            "candidate": ladder_name,
            "decision_policy": policy.payload(),
        }
    training_days = tuple(sorted(metadata.loc[train_index, "utc_day"].astype(str).unique()))
    full_payload = {
        "candidate": ladder_name,
        "side": side,
        "selected_profile": profile,
        "training_days": list(training_days),
        "training_row_sha256": _training_row_sha256(train_index),
        "learning_label_request_sha256": learning_label_request_sha256,
        "learning_label_payload_sha256": learning_label_payload_sha256,
        "learning_label_receipt_sha256": learning_label_receipt_sha256,
        "policy": payload,
    }
    return FittedCandidate(
        ladder_name=ladder_name,
        side=side,
        policy=policy,
        selected_profile=profile,
        training_days=training_days,
        training_row_sha256=full_payload["training_row_sha256"],
        policy_payload=payload,
        policy_sha256=_canonical_sha256(full_payload),
        fit_audit=dict(fit_audit),
        feature_pool_audit=None if pool_audit is None else asdict(pool_audit),
    )


def _fit_boolean_candidate(
    panel: NestedOofPanel,
    *,
    entry: CandidateLadderEntry,
    side: str,
    train_index: pd.Index,
    fold_id: str,
    profile: SuccessorSearchProfile,
    random_seed: int,
) -> FittedCandidate:
    if not panel.has_preconstructed_labels:
        raise NestedOofExecutionError("Boolean fit lacks fold-materialized learning labels")
    assert panel.action_outcomes is not None
    assert panel.action_supported is not None
    actions = duration_vocabulary(side)[1:]
    targets = build_identified_action_targets_against_policy(
        panel.action_outcomes,
        panel.action_supported,
        baseline_actions=panel.exact_owner_actions,
        actions=actions,
        index=train_index,
    )
    candidates = entry.feature_names_by_side[side]
    required = entry.required_features_by_side.get(side, ())
    selected, pool_audit = build_inner_train_feature_pool(
        panel.boolean_features,
        panel.metadata,
        train_index=train_index,
        candidates=candidates,
        feature_budget=profile.feature_budget,
        fold_id=fold_id,
        required_features=required,
        targets=targets,
    )
    train_features = panel.boolean_features.loc[train_index]
    train_metadata = panel.metadata.loc[train_index]
    policy, fit_audit = fit_identified_action_policy(
        train_features,
        train_metadata,
        targets,
        side=side,
        feature_names=selected,
        profile=profile,
        random_seed=random_seed,
    )
    fit_payload = asdict(fit_audit)
    fit_payload["selected_feature_names"] = list(selected)
    return _freeze_candidate(
        ladder_name=entry.name,
        side=side,
        policy=policy,
        profile=profile.name,
        train_index=train_index,
        metadata=panel.metadata,
        fit_audit=fit_payload,
        pool_audit=pool_audit,
        learning_label_request_sha256=panel.learning_label_request_sha256,
        learning_label_payload_sha256=panel.learning_label_payload_sha256,
        learning_label_receipt_sha256=panel.learning_label_receipt_sha256,
    )


def _fit_continuous_candidate(
    panel: NestedOofPanel,
    *,
    entry: ContinuousComparatorEntry,
    side: str,
    train_index: pd.Index,
    profile: SuccessorSearchProfile,
    random_seed: int,
) -> FittedCandidate:
    if not panel.has_preconstructed_labels:
        raise NestedOofExecutionError("continuous fit lacks fold-materialized learning labels")
    assert panel.action_outcomes is not None
    assert panel.action_supported is not None
    names = tuple(entry.feature_names_by_side[side])
    if not names or set(names) - set(panel.continuous_features.columns):
        raise NestedOofExecutionError("continuous comparator feature universe is invalid")
    actions = duration_vocabulary(side)[1:]
    targets = build_identified_action_targets_against_policy(
        panel.action_outcomes,
        panel.action_supported,
        baseline_actions=panel.exact_owner_actions,
        actions=actions,
        index=train_index,
    )
    matrix = panel.continuous_features.loc[train_index, list(names)].to_numpy(dtype=float)
    metadata = panel.metadata.loc[train_index]
    campaign_counts = metadata.groupby("campaign_cluster_id", observed=True)[
        "campaign_cluster_id"
    ].transform("size")
    base_weights = (1.0 / campaign_counts.astype(float)).to_numpy(dtype=float)
    models: dict[str, DecisionTreeRegressor] = {}
    action_audits: list[dict[str, Any]] = []
    complete = np.isfinite(matrix).all(axis=1)
    for offset, action in enumerate(actions):
        known = targets.identified[action].to_numpy(dtype=bool) & complete
        if int(known.sum()) < max(2, profile.min_samples_leaf * 2):
            action_audits.append(
                {"action": action, "identified_rows": int(known.sum()), "fitted": False}
            )
            continue
        model = DecisionTreeRegressor(
            max_depth=profile.max_depth,
            max_leaf_nodes=profile.max_leaf_nodes,
            min_samples_leaf=profile.min_samples_leaf,
            random_state=random_seed + offset,
        )
        model.fit(
            matrix[known],
            targets.effects.loc[known, action].to_numpy(dtype=float),
            sample_weight=base_weights[known],
        )
        models[action] = model
        action_audits.append(
            {"action": action, "identified_rows": int(known.sum()), "fitted": True}
        )
    if not models:
        raise NestedOofExecutionError("continuous comparator has no identified action model")
    policy = _ContinuousActionPolicy(side=side, feature_names=names, models=models)
    return _freeze_candidate(
        ladder_name=entry.name,
        side=side,
        policy=policy,
        profile=profile.name,
        train_index=train_index,
        metadata=panel.metadata,
        fit_audit={
            "uses_neutral_zero_targets": False,
            "identified_only_models": action_audits,
            "feature_count": len(names),
        },
        pool_audit=None,
        learning_label_request_sha256=panel.learning_label_request_sha256,
        learning_label_payload_sha256=panel.learning_label_payload_sha256,
        learning_label_receipt_sha256=panel.learning_label_receipt_sha256,
    )


def _freeze_fixed_candidate(
    panel: NestedOofPanel,
    *,
    entry: CandidateLadderEntry,
    side: str,
) -> FittedCandidate:
    empty = panel.metadata.index[:0]
    if entry.kind == "exact_owner":
        policy = None
    else:
        policy = entry.fixed_policy_by_side[side]
    return _freeze_candidate(
        ladder_name=entry.name,
        side=side,
        policy=policy,
        profile="preregistered_fixed",
        train_index=empty,
        metadata=panel.metadata,
        fit_audit={"economic_outcomes_used_for_definition": False},
        pool_audit=None,
    )


def _fit_action_matched_candidate(
    panel: NestedOofPanel,
    *,
    source: FittedCandidate,
    side: str,
    train_index: pd.Index,
    fold_id: str,
    matched_name: str,
) -> FittedCandidate:
    selected = source.choose(
        panel.boolean_features.loc[train_index],
        panel.exact_owner_actions.loc[train_index],
    )
    owner = panel.exact_owner_actions.loc[train_index].astype(str).to_numpy(dtype=object)
    overrides = np.where(selected == owner, CONTROL_ACTION, selected).astype(object)
    values, counts = np.unique(overrides, return_counts=True)
    probabilities = {
        str(value): float(count / len(overrides))
        for value, count in zip(values, counts, strict=True)
    }
    policy = _ActionMatchedPolicy(
        side=side,
        probabilities=probabilities,
        salt=_canonical_sha256([IDENTITY, side, fold_id, source.policy_sha256]),
    )
    return _freeze_candidate(
        ladder_name=matched_name,
        side=side,
        policy=policy,
        profile="action_rate_and_duration_matched",
        train_index=train_index,
        metadata=panel.metadata,
        fit_audit={
            "source_candidate": source.ladder_name,
            "source_policy_sha256": source.policy_sha256,
            "matched_probabilities": probabilities,
            "economic_outcomes_used_for_matching": False,
        },
        pool_audit=None,
    )


def _validate_evaluation(
    result: pd.DataFrame,
    request: EvaluationRequest,
) -> pd.DataFrame:
    if not isinstance(result, pd.DataFrame):
        raise NestedOofExecutionError("sequential evaluator must return a DataFrame")
    missing = set(REQUIRED_EVALUATION_COLUMNS) - set(result.columns)
    if missing:
        raise NestedOofExecutionError(f"evaluation columns are missing: {sorted(missing)}")
    rows = result.copy()
    rows["utc_day"] = rows["utc_day"].map(_normalize_day)
    if rows["utc_day"].duplicated().any() or set(rows["utc_day"]) != set(request.days):
        raise NestedOofExecutionError("evaluator did not return exactly one row per frozen day")
    rows = rows.set_index("utc_day").loc[list(request.days)].reset_index()
    if set(rows["side"].map(_normalize_side)) != {request.side}:
        raise NestedOofExecutionError("evaluator pooled or changed sides")
    if set(rows["panel_role"].astype(str)) != {request.panel_role}:
        raise NestedOofExecutionError("evaluator read a panel outside the bound evidence role")
    if not rows["repeated_sequential_policy"].astype(bool).all():
        raise NestedOofExecutionError("outer/inner economics are not repeated sequential policy")
    if rows["one_shot_effect_aggregation_used"].astype(bool).any():
        raise NestedOofExecutionError("one-shot effects cannot be aggregated as policy economics")
    if not rows["exact_current_owner_row_wise_baseline"].astype(bool).all():
        raise NestedOofExecutionError("evaluator did not use the exact current owner baseline")
    expected_candidate_sha = request.candidate.expected_executed_policy_sha256
    if set(rows["candidate_executed_policy_sha256"].astype(str)) != {
        expected_candidate_sha
    }:
        raise NestedOofExecutionError("candidate executed-policy identity drifted")
    if set(rows["exact_owner_executed_policy_sha256"].astype(str)) != {
        ACTIVE_OWNER_POLICY_SHA256
    }:
        raise NestedOofExecutionError("control executed-policy identity drifted")
    receipts = rows["paired_replay_receipt_sha256"].astype(str)
    if receipts.duplicated().any() or not receipts.map(
        lambda value: _SHA256_RE.fullmatch(value) is not None
    ).all():
        raise NestedOofExecutionError("paired replay receipt identity is invalid")
    if set(rows["candidate_target_side"].map(_normalize_side)) != {request.side}:
        raise NestedOofExecutionError("paired replay target side drifted")
    for column in ("same_market_source", "common_random_source", "arm_local_state"):
        if not rows[column].astype(bool).all():
            raise NestedOofExecutionError(f"paired replay lacks {column}")
    identified = rows["point_identified"].astype(bool).to_numpy()
    economic_values: dict[str, np.ndarray] = {}
    economic_pairs = list(ECONOMIC_PAIR_COLUMNS)
    economic_pairs.extend(
        (candidate_column, owner_column)
        for candidate_column, owner_column, _orientation in RISK_METRIC_COLUMNS.values()
        if (candidate_column, owner_column) not in economic_pairs
    )
    for candidate_column, owner_column in economic_pairs:
        candidate_value = pd.to_numeric(
            rows[candidate_column], errors="coerce"
        ).to_numpy(dtype=float)
        owner_value = pd.to_numeric(rows[owner_column], errors="coerce").to_numpy(
            dtype=float
        )
        if not (
            np.isfinite(candidate_value[identified]).all()
            and np.isfinite(owner_value[identified]).all()
        ):
            raise NestedOofExecutionError(
                f"identified sequential values must be finite: {candidate_column}"
            )
        if not (
            np.isnan(candidate_value[~identified]).all()
            and np.isnan(owner_value[~identified]).all()
        ):
            raise NestedOofExecutionError(
                f"unidentified sequential values must remain missing: {candidate_column}"
            )
        economic_values[candidate_column] = candidate_value
        economic_values[owner_column] = owner_value
    for column in (
        "candidate_negative_terminal_rate",
        "exact_owner_negative_terminal_rate",
        "candidate_repair_event_rate",
        "exact_owner_repair_event_rate",
        "candidate_censoring_rate",
        "exact_owner_censoring_rate",
    ):
        values = economic_values[column][identified]
        if ((values < 0.0) | (values > 1.0)).any():
            raise NestedOofExecutionError(f"evaluation rate {column!r} is outside [0, 1]")
    for column in (
        "candidate_campaign_mae_usdc",
        "exact_owner_campaign_mae_usdc",
        "candidate_mean_repair_time_s",
        "exact_owner_mean_repair_time_s",
    ):
        if (economic_values[column][identified] < 0.0).any():
            raise NestedOofExecutionError(f"evaluation magnitude {column!r} is negative")
    count_groups = {
        prefix: sorted(column for column in rows if column.startswith(prefix))
        for prefix in REQUIRED_COUNT_PREFIXES
    }
    missing_groups = [prefix for prefix, columns in count_groups.items() if not columns]
    if missing_groups:
        raise NestedOofExecutionError(
            f"evaluation lacks required count groups: {missing_groups}"
        )
    count_columns = sorted(
        {
            column
            for columns in count_groups.values()
            for column in columns
        }
    )
    for column in (
        "policy_assignment_count",
        "nonbaseline_action_count",
        "feature_ready_active_treatment_events",
        "common_row_count",
        "common_campaign_count",
        "candidate_fill_count",
        "exact_owner_fill_count",
        *count_columns,
    ):
        values = pd.to_numeric(rows[column], errors="coerce")
        if values.isna().any() or (values < 0).any() or not np.equal(values, np.floor(values)).all():
            raise NestedOofExecutionError(f"evaluation count {column!r} is invalid")
        rows[column] = values.astype(np.int64)
    for prefix, columns in count_groups.items():
        if not np.array_equal(
            rows[columns].sum(axis=1).to_numpy(dtype=np.int64),
            rows["policy_assignment_count"].to_numpy(dtype=np.int64),
        ):
            raise NestedOofExecutionError(
                f"{prefix} counts do not sum to policy assignments"
            )
    if (rows["common_row_count"] <= 0).any() or (
        rows["common_campaign_count"] <= 0
    ).any():
        raise NestedOofExecutionError("common row/campaign denominators must be positive")
    if (
        rows["nonbaseline_action_count"] > rows["feature_ready_active_treatment_events"]
    ).any():
        raise NestedOofExecutionError("nonbaseline actions exceed feature-ready opportunities")
    candidate = economic_values["candidate_terminal_value_usdc"]
    baseline = economic_values["exact_owner_terminal_value_usdc"]
    rows["delta_usdc"] = np.where(identified, candidate - baseline, np.nan)
    for candidate_column, owner_column in ECONOMIC_PAIR_COLUMNS[1:]:
        metric = candidate_column.removeprefix("candidate_")
        rows[f"delta::{metric}"] = np.where(
            identified,
            economic_values[candidate_column] - economic_values[owner_column],
            np.nan,
        )
    for metric, (candidate_column, owner_column, orientation) in RISK_METRIC_COLUMNS.items():
        rows[f"risk_delta::{metric}"] = np.where(
            identified,
            orientation
            * (economic_values[candidate_column] - economic_values[owner_column]),
            np.nan,
        )
    rows["candidate_name"] = request.candidate.ladder_name
    rows["policy_sha256"] = request.candidate.policy_sha256
    rows["selected_profile"] = request.candidate.selected_profile
    rows["fold_id"] = request.fold_id
    rows["stage"] = request.stage
    rows["outer_test_outcomes_used_for_fit"] = False
    rows["oof_evidence_scope"] = OOF_EVIDENCE_SCOPE
    return rows


def _build_evaluation_request(
    candidate: FittedCandidate,
    *,
    side: str,
    days: Sequence[str],
    fold_id: str,
    stage: Literal["inner_oof", "outer_oof"],
    panel_role: str = PANEL_ROLE,
) -> EvaluationRequest:
    normalized_days = tuple(_normalize_day(day) for day in days)
    if candidate.training_days and max(candidate.training_days) >= min(normalized_days):
        raise NestedOofExecutionError("candidate training is not strictly before evaluation")
    return EvaluationRequest(
        candidate=candidate,
        side=side,
        days=normalized_days,
        fold_id=fold_id,
        stage=stage,
        panel_role=panel_role,
    )


def _evaluate(
    evaluator: SequentialPolicyEvaluator,
    candidate: FittedCandidate,
    *,
    side: str,
    days: Sequence[str],
    fold_id: str,
    stage: Literal["inner_oof", "outer_oof"],
    panel_role: str = PANEL_ROLE,
) -> pd.DataFrame:
    request = _build_evaluation_request(
        candidate,
        side=side,
        days=days,
        fold_id=fold_id,
        stage=stage,
        panel_role=panel_role,
    )
    return _validate_evaluation(evaluator(request), request)


def _evaluation_request_slot(request: EvaluationRequest) -> tuple[Any, ...]:
    return (
        request.stage,
        _normalize_side(request.side),
        str(request.fold_id).strip(),
        request.candidate.ladder_name,
        tuple(_normalize_day(day) for day in request.days),
        str(request.panel_role).strip(),
    )


def _validate_evaluation_request_batch(
    requests: Sequence[EvaluationRequest],
) -> tuple[EvaluationRequest, ...]:
    normalized = tuple(requests)
    if not normalized:
        raise NestedOofExecutionError("evaluation request batch is empty")
    if not all(isinstance(request, EvaluationRequest) for request in normalized):
        raise NestedOofExecutionError("evaluation request batch contains a custom request")
    for request in normalized:
        side = _normalize_side(request.side)
        days = tuple(_normalize_day(day) for day in request.days)
        if side != request.candidate.side:
            raise NestedOofExecutionError("evaluation request candidate side drifted")
        if not days or len(days) != len(set(days)) or days != tuple(sorted(days)):
            raise NestedOofExecutionError(
                "evaluation request days are empty, duplicated, or non-chronological"
            )
        if not str(request.fold_id).strip() or request.stage not in {
            "inner_oof",
            "outer_oof",
        }:
            raise NestedOofExecutionError("evaluation request fold or stage is invalid")
        if not str(request.panel_role).strip():
            raise NestedOofExecutionError("evaluation request panel role is empty")
    request_sha256s = [request.request_sha256 for request in normalized]
    if len(request_sha256s) != len(set(request_sha256s)):
        raise NestedOofExecutionError("evaluation request batch contains a duplicate request")
    slots = [_evaluation_request_slot(request) for request in normalized]
    if len(slots) != len(set(slots)):
        raise NestedOofExecutionError("evaluation request batch contains a duplicate request slot")
    return normalized


def _evaluate_many(
    evaluator: SequentialPolicyEvaluator,
    candidates: Sequence[tuple[str, FittedCandidate]],
    *,
    side: str,
    days: Sequence[str],
    fold_id: str,
    stage: Literal["inner_oof", "outer_oof"],
    panel_role: str = PANEL_ROLE,
) -> tuple[tuple[str, pd.DataFrame], ...]:
    named_candidates = tuple(candidates)
    if not named_candidates:
        raise NestedOofExecutionError("candidate evaluation batch is empty")
    for name, candidate in named_candidates:
        if name != candidate.ladder_name:
            raise NestedOofExecutionError("candidate evaluation name drifted")
    requests = _validate_evaluation_request_batch(
        tuple(
            _build_evaluation_request(
                candidate,
                side=side,
                days=days,
                fold_id=fold_id,
                stage=stage,
                panel_role=panel_role,
            )
            for _name, candidate in named_candidates
        )
    )
    evaluate_many = getattr(evaluator, "evaluate_many", None)
    if not callable(evaluate_many):
        return tuple(
            (
                name,
                _validate_evaluation(evaluator(request), request),
            )
            for (name, _candidate), request in zip(
                named_candidates, requests, strict=True
            )
        )

    raw_results = evaluate_many(requests)
    if isinstance(raw_results, (str, bytes)) or not isinstance(raw_results, Sequence):
        raise NestedOofExecutionError("batch evaluator returned a custom result collection")
    results = tuple(raw_results)
    if not all(isinstance(result, SequentialEvaluationResult) for result in results):
        raise NestedOofExecutionError("batch evaluator returned a custom result")
    result_sha256s = [result.request_sha256 for result in results]
    if len(result_sha256s) != len(set(result_sha256s)):
        raise NestedOofExecutionError("batch evaluator returned a duplicate request")
    expected = {request.request_sha256: request for request in requests}
    unexpected = sorted(set(result_sha256s) - set(expected))
    missing = sorted(set(expected) - set(result_sha256s))
    if unexpected or missing:
        raise NestedOofExecutionError(
            "batch evaluator request census drifted: "
            f"missing={missing}, unexpected={unexpected}"
        )
    by_request = {result.request_sha256: result.rows for result in results}
    return tuple(
        (
            name,
            _validate_evaluation(by_request[request.request_sha256], request),
        )
        for (name, _candidate), request in zip(named_candidates, requests, strict=True)
    )


def _outer_evaluation_waves(
    frozen: Mapping[str, FittedCandidate],
) -> tuple[tuple[tuple[str, FittedCandidate], ...], ...]:
    ordered = tuple(frozen.items())
    if not ordered:
        raise NestedOofExecutionError("outer evaluation has no frozen candidate")
    for name, candidate in ordered:
        if name != candidate.ladder_name:
            raise NestedOofExecutionError("frozen outer candidate name drifted")

    dependencies: dict[str, frozenset[str]] = {}
    for name, _candidate in ordered:
        prefix = "ACTION_MATCHED_CONTROLS::"
        dependencies[name] = (
            frozenset({name.removeprefix(prefix)})
            if name.startswith(prefix)
            else frozenset()
        )
    names = {name for name, _candidate in ordered}
    unknown_dependencies = sorted(
        dependency
        for required in dependencies.values()
        for dependency in required
        if dependency not in names
    )
    if unknown_dependencies:
        raise NestedOofExecutionError(
            f"outer evaluation dependency is not frozen: {unknown_dependencies}"
        )

    completed: set[str] = set()
    pending = list(ordered)
    waves: list[tuple[tuple[str, FittedCandidate], ...]] = []
    while pending:
        ready = tuple(
            (name, candidate)
            for name, candidate in pending
            if dependencies[name] <= completed
        )
        if not ready:
            raise NestedOofExecutionError("outer evaluation dependency graph is cyclic")
        waves.append(ready)
        ready_names = {name for name, _candidate in ready}
        completed.update(ready_names)
        pending = [item for item in pending if item[0] not in ready_names]
    return tuple(waves)


def _evaluate_outer_candidates(
    evaluator: SequentialPolicyEvaluator,
    frozen: Mapping[str, FittedCandidate],
    *,
    side: str,
    days: Sequence[str],
    fold_id: str,
    panel_role: str,
) -> dict[str, pd.DataFrame]:
    if callable(getattr(evaluator, "evaluate_many", None)):
        evaluation_waves = _outer_evaluation_waves(frozen)
    else:
        # Preserve the pre-batch call sequence for single-request evaluators.
        evaluation_waves = (tuple(frozen.items()),)
    outer_results: dict[str, pd.DataFrame] = {}
    for wave in evaluation_waves:
        for name, evaluated in _evaluate_many(
            evaluator,
            wave,
            side=side,
            days=days,
            fold_id=fold_id,
            stage="outer_oof",
            panel_role=panel_role,
        ):
            if name in outer_results:
                raise NestedOofExecutionError(
                    f"outer candidate {name!r} was evaluated more than once"
                )
            outer_results[name] = evaluated
    if tuple(outer_results) != tuple(
        name for wave in evaluation_waves for name, _candidate in wave
    ):
        raise NestedOofExecutionError("outer batch result order drifted")
    if set(outer_results) != set(frozen):
        raise NestedOofExecutionError("outer batch candidate census drifted")
    return {name: outer_results[name] for name in frozen}


def _identified_equal_day_mean(rows: pd.DataFrame) -> float:
    identified = rows.loc[rows["point_identified"].astype(bool), "delta_usdc"]
    if identified.empty:
        raise NestedOofExecutionError("inner OOF profile has no identified sequential day")
    return float(pd.to_numeric(identified, errors="raise").mean())


def _profile_complexity(profile: SuccessorSearchProfile) -> tuple[int, ...]:
    return (
        profile.feature_budget,
        profile.max_depth,
        profile.max_leaf_nodes,
        profile.max_rules,
        profile.max_clauses_per_rule,
        profile.max_literals_per_clause,
    )


def _select_boolean_profile(
    panel: NestedOofPanel,
    *,
    entry: CandidateLadderEntry,
    side: str,
    inner_folds: Sequence[Mapping[str, Any]],
    evaluator: SequentialPolicyEvaluator,
    random_seed: int,
    panel_role: str,
) -> tuple[SuccessorSearchProfile, list[dict[str, Any]], list[PurgeAudit]]:
    profile_rows: list[dict[str, Any]] = []
    purge_audits: list[PurgeAudit] = []
    for profile_index, profile in enumerate(entry.profiles):
        evaluations: list[pd.DataFrame] = []
        fold_fits: list[dict[str, Any]] = []
        profile_purges: list[PurgeAudit] = []
        failed_reason: str | None = None
        for inner_index, inner in enumerate(inner_folds):
            fold_id = str(inner["fold_id"])
            try:
                train_index, purge = _purged_train_index(
                    panel,
                    side=side,
                    train_days=inner["train_days"],
                    test_days=inner["test_days"],
                    fold_id=fold_id,
                    stage=f"{entry.name}.inner_fit",
                )
                candidate = _fit_boolean_candidate(
                    panel,
                    entry=entry,
                    side=side,
                    train_index=train_index,
                    fold_id=fold_id,
                    profile=profile,
                    random_seed=random_seed + profile_index * 100 + inner_index,
                )
                evaluation = _evaluate(
                    evaluator,
                    candidate,
                    side=side,
                    days=inner["test_days"],
                    fold_id=fold_id,
                    stage="inner_oof",
                    panel_role=panel_role,
                )
            except SuccessorContractError as exc:
                failed_reason = str(exc)
                break
            evaluations.append(evaluation)
            profile_purges.append(purge)
            fold_fits.append(
                {
                    "fold_id": fold_id,
                    "candidate_policy_sha256": candidate.policy_sha256,
                    "policy": candidate.policy_payload,
                    "feature_pool_audit": candidate.feature_pool_audit,
                    "fit_audit": candidate.fit_audit,
                    "training_days": list(candidate.training_days),
                }
            )
        if failed_reason is None:
            combined = pd.concat(evaluations, ignore_index=True)
            score = _identified_equal_day_mean(combined)
            purge_audits.extend(profile_purges)
        else:
            score = -math.inf
        profile_rows.append(
            {
                "profile": profile.name,
                "inner_equal_day_mean_usdc": score,
                "valid": failed_reason is None,
                "failure_reason": failed_reason,
                "fold_fits": fold_fits,
            }
        )
    valid = [row for row in profile_rows if row["valid"]]
    if not valid:
        raise NestedOofExecutionError(f"all inner profiles failed for {side}/{entry.name}")
    by_name = {profile.name: profile for profile in entry.profiles}
    selected_row = min(
        valid,
        key=lambda row: (
            -float(row["inner_equal_day_mean_usdc"]),
            _profile_complexity(by_name[str(row["profile"])]),
            str(row["profile"]),
        ),
    )
    return by_name[str(selected_row["profile"])], profile_rows, purge_audits


def _select_continuous_profile(
    panel: NestedOofPanel,
    *,
    entry: ContinuousComparatorEntry,
    side: str,
    inner_folds: Sequence[Mapping[str, Any]],
    evaluator: SequentialPolicyEvaluator,
    random_seed: int,
    panel_role: str,
) -> tuple[SuccessorSearchProfile, list[dict[str, Any]], list[PurgeAudit]]:
    rows: list[dict[str, Any]] = []
    purges: list[PurgeAudit] = []
    for profile_index, profile in enumerate(entry.profiles):
        evaluations: list[pd.DataFrame] = []
        profile_purges: list[PurgeAudit] = []
        failed: str | None = None
        for inner_index, inner in enumerate(inner_folds):
            fold_id = str(inner["fold_id"])
            try:
                train_index, purge = _purged_train_index(
                    panel,
                    side=side,
                    train_days=inner["train_days"],
                    test_days=inner["test_days"],
                    fold_id=fold_id,
                    stage=f"{entry.name}.inner_fit",
                )
                candidate = _fit_continuous_candidate(
                    panel,
                    entry=entry,
                    side=side,
                    train_index=train_index,
                    profile=profile,
                    random_seed=random_seed + profile_index * 100 + inner_index,
                )
                evaluations.append(
                    _evaluate(
                        evaluator,
                        candidate,
                        side=side,
                        days=inner["test_days"],
                        fold_id=fold_id,
                        stage="inner_oof",
                        panel_role=panel_role,
                    )
                )
                profile_purges.append(purge)
            except SuccessorContractError as exc:
                failed = str(exc)
                break
        score = -math.inf if failed is not None else _identified_equal_day_mean(
            pd.concat(evaluations, ignore_index=True)
        )
        if failed is None:
            purges.extend(profile_purges)
        rows.append(
            {
                "profile": profile.name,
                "inner_equal_day_mean_usdc": score,
                "valid": failed is None,
                "failure_reason": failed,
            }
        )
    valid = [row for row in rows if row["valid"]]
    if not valid:
        raise NestedOofExecutionError(f"all continuous profiles failed for {side}")
    by_name = {profile.name: profile for profile in entry.profiles}
    selected_row = min(
        valid,
        key=lambda row: (
            -float(row["inner_equal_day_mean_usdc"]),
            _profile_complexity(by_name[str(row["profile"])]),
            str(row["profile"]),
        ),
    )
    return by_name[str(selected_row["profile"])], rows, purges


def _candidate_report(rows: pd.DataFrame, *, tolerance: float) -> dict[str, Any]:
    identified = rows["point_identified"].astype(bool)
    values = rows.loc[identified, ["utc_day", "delta_usdc"]].copy()
    values["delta_usdc"] = pd.to_numeric(values["delta_usdc"], errors="raise")
    deltas = values["delta_usdc"].to_numpy(dtype=float)
    day_mean = float(deltas.mean()) if len(deltas) else None
    day_se = (
        float(np.std(deltas, ddof=1) / math.sqrt(len(deltas))) if len(deltas) >= 2 else None
    )
    week_values: list[float] = []
    if not values.empty:
        weeks = values["utc_day"].map(
            lambda day: f"{pd.Timestamp(day).isocalendar().year}-W{pd.Timestamp(day).isocalendar().week:02d}"
        )
        week_values = values.assign(week=weeks).groupby("week")["delta_usdc"].mean().tolist()
    week_se = (
        float(np.std(week_values, ddof=1) / math.sqrt(len(week_values)))
        if len(week_values) >= 2
        else None
    )
    ranked = values.sort_values(["delta_usdc", "utc_day"], ascending=[False, True])
    top_days = ranked["utc_day"].head(2).tolist()

    def leave_mean(count: int) -> float | None:
        kept = values.loc[~values["utc_day"].isin(top_days[:count]), "delta_usdc"]
        return float(kept.mean()) if len(kept) else None

    action_columns = sorted(column for column in rows if column.startswith("action_count::"))
    action_counts = {
        column.removeprefix("action_count::"): int(rows[column].sum())
        for column in action_columns
    }
    total_actions = sum(action_counts.values())
    action_mix = {
        action: count / total_actions if total_actions else 0.0
        for action, count in action_counts.items()
    }
    grouped_counts: dict[str, dict[str, int]] = {}
    for prefix in REQUIRED_COUNT_PREFIXES[1:]:
        grouped_counts[prefix.removesuffix("::")] = {
            column.removeprefix(prefix): int(rows[column].sum())
            for column in sorted(rows)
            if column.startswith(prefix)
        }
    duration_counts: dict[str, int] = {}
    for action, count in action_counts.items():
        if action.startswith("FIXED_") and action.endswith("S"):
            duration = f"{int(action.removeprefix('FIXED_').removesuffix('S'))}s"
        elif action == CONTROL_ACTION:
            duration = "row_wise_owner_or_control_85n"
        else:
            duration = f"action:{action}"
        duration_counts[duration] = duration_counts.get(duration, 0) + count
    feature_ready = rows["feature_ready_active_treatment_events"] > 0
    active = rows["nonbaseline_action_count"] > 0
    zero = identified & (rows["delta_usdc"].abs() <= tolerance)
    identified_rows = rows.loc[identified]

    def identified_mean(column: str) -> float | None:
        return (
            float(pd.to_numeric(identified_rows[column], errors="raise").mean())
            if not identified_rows.empty
            else None
        )

    owner_fills = int(rows["exact_owner_fill_count"].sum())
    candidate_fills = int(rows["candidate_fill_count"].sum())
    common_rows = int(rows["common_row_count"].sum())
    common_campaigns = int(rows["common_campaign_count"].sum())
    identified_days = int(identified.sum())
    return {
        "outer_test_days": int(rows["utc_day"].nunique()),
        "identified_days": identified_days,
        "unidentified_days": int((~identified).sum()),
        "unsupported_mass": (
            float((~identified).sum()) / float(len(rows)) if len(rows) else 1.0
        ),
        "daily_positive_rate": (
            float((identified_rows["delta_usdc"] > 0.0).mean())
            if identified_days
            else 0.0
        ),
        "feature_ready_active_days": int(feature_ready.sum()),
        "candidate_nonbaseline_action_days": int(active.sum()),
        "zero_difference_days": int(zero.sum()),
        "zero_difference_tolerance_usdc": tolerance,
        "equal_day_mean_usdc": day_mean,
        "day_cluster_standard_error_usdc": day_se,
        "week_block_count": len(week_values),
        "week_block_standard_error_usdc": week_se,
        "leave_one_top_day": {
            "removed_days": top_days[:1],
            "mean_usdc": leave_mean(1),
        },
        "leave_two_top_days": {
            "removed_days": top_days[:2],
            "mean_usdc": leave_mean(2),
        },
        "policy_assignment_count": int(rows["policy_assignment_count"].sum()),
        "common_row_count": common_rows,
        "common_campaign_count": common_campaigns,
        "paired_effective_sample_size": float(common_campaigns),
        "minimum_behavior_propensity": 1.0,
        "overlap_violations": 0,
        "nonbaseline_action_count": int(rows["nonbaseline_action_count"].sum()),
        "nonbaseline_action_rate": (
            float(rows["nonbaseline_action_count"].sum())
            / float(rows["feature_ready_active_treatment_events"].sum())
            if int(rows["feature_ready_active_treatment_events"].sum()) > 0
            else 0.0
        ),
        "action_counts": action_counts,
        "action_mix": action_mix,
        "duration_counts": dict(sorted(duration_counts.items())),
        "role_counts": grouped_counts["role_count"],
        "consecutive_units_counts": grouped_counts["consecutive_units_count"],
        "fallback_counts": grouped_counts["fallback_count"],
        "candidate_fill_count": candidate_fills,
        "exact_owner_fill_count": owner_fills,
        "fill_retention": (
            float(candidate_fills) / float(owner_fills) if owner_fills > 0 else None
        ),
        "risk_and_accounting": {
            "closed_campaign_delta_usdc_equal_day_mean": identified_mean(
                "delta::closed_campaign_value_usdc"
            ),
            "campaign_q10_delta_usdc_equal_day_mean": identified_mean(
                "delta::campaign_q10_usdc"
            ),
            "campaign_cvar10_delta_usdc_equal_day_mean": identified_mean(
                "delta::campaign_cvar10_usdc"
            ),
            "inventory_time_delta_btc_s_equal_day_mean": identified_mean(
                "delta::inventory_time_btc_s"
            ),
            "max_abs_inventory_delta_btc_equal_day_mean": identified_mean(
                "delta::max_abs_inventory_btc"
            ),
            **{
                f"{metric}_equal_day_mean": identified_mean(
                    f"risk_delta::{metric}"
                )
                for metric in RISK_METRIC_COLUMNS
            },
        },
    }


def _candidate_day_series(oof: pd.DataFrame, *, side: str, candidate: str) -> pd.Series:
    rows = oof.loc[
        (oof["side"] == side)
        & (oof["candidate_name"] == candidate)
        & oof["point_identified"].astype(bool)
    ]
    series = rows.set_index("utc_day")["delta_usdc"].astype(float).sort_index()
    if series.index.has_duplicates:
        raise NestedOofExecutionError("candidate OOF repeats a side/day")
    return series


def _candidate_metric_day_series(
    oof: pd.DataFrame,
    *,
    side: str,
    candidate: str,
    metric: str,
) -> pd.Series:
    column = f"risk_delta::{metric}"
    if metric not in RISK_METRIC_COLUMNS or column not in oof:
        raise NestedOofExecutionError(f"unknown risk metric {metric!r}")
    rows = oof.loc[
        (oof["side"] == side)
        & (oof["candidate_name"] == candidate)
        & oof["point_identified"].astype(bool)
    ]
    series = rows.set_index("utc_day")[column].astype(float).sort_index()
    if series.index.has_duplicates:
        raise NestedOofExecutionError("candidate risk OOF repeats a side/day")
    return series


def _scorecard_metric(
    day_band: SimultaneousBand,
    week_band: SimultaneousBand,
    *,
    daily_positive_rate: float | None = None,
    source: str,
) -> dict[str, Any]:
    lower = min(day_band.lcb_usdc, week_band.lcb_usdc)
    upper = max(day_band.ucb_usdc, week_band.ucb_usdc)
    payload: dict[str, Any] = {
        "estimate": day_band.mean_usdc,
        "lower_bound": lower,
        "upper_bound": upper,
        "source": source,
    }
    if daily_positive_rate is not None:
        payload["daily_positive_rate"] = float(daily_positive_rate)
    return payload


def _build_candidate_scorecard(
    *,
    side: str,
    candidate: str,
    report: Mapping[str, Any],
    candidate_bands: SimultaneousBandFamily,
    candidate_week_bands: SimultaneousBandFamily,
    risk_bands: SimultaneousBandFamily,
    risk_week_bands: SimultaneousBandFamily,
) -> Mapping[str, Any]:
    candidate_key = f"{side}:{candidate}"
    reward = _scorecard_metric(
        candidate_bands.bands[candidate_key],
        candidate_week_bands.bands[candidate_key],
        daily_positive_rate=float(report["daily_positive_rate"]),
        source="paired_day_and_week_simultaneous_learning_algorithm_oof",
    )

    def risk(metric: str) -> dict[str, Any]:
        key = f"{side}:{candidate}:{metric}"
        return _scorecard_metric(
            risk_bands.bands[key],
            risk_week_bands.bands[key],
            source="paired_day_and_week_simultaneous_learning_algorithm_oof",
        )

    fill_retention = report.get("fill_retention")
    evidence = {
        "schema_version": CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": IDENTITY,
        "family_id": "F05",
        "panel_role": "development",
        "score_profile_contract": SCORE_PROFILE_CONTRACT,
        "input_identity": {
            "oof_evidence_scope": OOF_EVIDENCE_SCOPE,
            "side": side,
            "candidate": candidate,
            "exact_final_artifact_oof_available": False,
        },
        "validity_failures": [],
        "support": {
            "n_rows": int(report["common_row_count"]),
            "n_days": int(report["identified_days"]),
            "effective_sample_size": float(report["paired_effective_sample_size"]),
            "minimum_behavior_propensity": float(
                report["minimum_behavior_propensity"]
            ),
            "unsupported_mass": float(report["unsupported_mass"]),
            "overlap_violations": int(report["overlap_violations"]),
            "failures": [],
        },
        "candidate_rate": float(report["nonbaseline_action_rate"]),
        "invariant_violations": [],
        "family_gate_failures": [],
        "metrics": {
            "conditional_net_value": reward,
            "negative_terminal_protection": risk(
                "negative_terminal_protection"
            ),
            "q10_shortfall_protection": risk("campaign_q10"),
            "campaign_mae_avoidance": risk("campaign_mae_avoidance"),
            "repair_event": risk("repair_event"),
            "repair_time_avoidance_s": risk("repair_time_avoidance_s"),
            "censoring_avoidance": risk("censoring_avoidance"),
            "fills_retention": {
                "estimate": fill_retention,
                "source": "candidate_vs_exact_owner_common_fill_count",
            },
        },
    }
    return score_canonical_evidence(
        evidence,
        profile_id="action_alpha_v1",
        require_frozen_profile=True,
    )


def _week_block_series(series: pd.Series) -> pd.Series:
    weeks = [
        f"{stamp.isocalendar().year}-W{stamp.isocalendar().week:02d}"
        for stamp in (pd.Timestamp(day) for day in series.index)
    ]
    result = series.groupby(pd.Index(weeks, name="iso_week")).mean().sort_index()
    if len(result) < 2:
        raise NestedOofExecutionError("week-block evidence has fewer than two weeks")
    return result


def _paired_contrast(
    oof: pd.DataFrame,
    *,
    side: str,
    candidate: str,
    reference: str,
) -> tuple[pd.Series, int, int]:
    left_rows = oof.loc[
        (oof["side"] == side) & (oof["candidate_name"] == candidate), "utc_day"
    ]
    right_rows = oof.loc[
        (oof["side"] == side) & (oof["candidate_name"] == reference), "utc_day"
    ]
    all_days = sorted(set(left_rows.astype(str)) | set(right_rows.astype(str)))
    left = _candidate_day_series(oof, side=side, candidate=candidate)
    right = _candidate_day_series(oof, side=side, candidate=reference)
    common = left.index.intersection(right.index)
    contrast = (left.loc[common] - right.loc[common]).sort_index()
    return contrast, len(common), len(all_days)


def _hierarchy_report(
    day_family: SimultaneousBandFamily,
    week_family: SimultaneousBandFamily,
    *,
    hypothesis_support: Mapping[str, tuple[int, int]],
    sides: Sequence[str],
    epsilon: float,
) -> dict[str, Any]:
    steps: dict[str, list[dict[str, Any]]] = {}
    supported: list[str] = []
    for side in sides:
        names = (
            f"successor:{side}:E1-B0",
            f"successor:{side}:E2-E1",
            f"successor:{side}:E3-E2",
            f"successor:{side}:M2-E3",
            f"successor:{side}:CONTINUOUS-BOOLEAN",
        )
        parent = True
        side_steps: list[dict[str, Any]] = []
        for position, name in enumerate(names):
            day_band = day_family.bands[name]
            week_band = week_family.bands[name]
            identified, total = hypothesis_support[name]
            point_identified = identified == total
            if not parent:
                passed = False
                tested = False
                reason = "parent_feature_block_not_supported"
            elif not point_identified:
                passed = False
                tested = True
                reason = "unidentified_days_without_preregistered_bounds"
            elif position < 4:
                passed = (
                    day_band.lcb_usdc > epsilon
                    and week_band.lcb_usdc > epsilon
                )
                tested = True
                reason = (
                    "day_and_week_simultaneous_lcb_above_economic_epsilon"
                    if passed
                    else "day_or_week_simultaneous_lcb_not_above_economic_epsilon"
                )
            else:
                passed = (
                    day_band.lcb_usdc <= epsilon
                    and week_band.lcb_usdc <= epsilon
                )
                tested = True
                reason = (
                    "boolean_not_proven_dominated_by_continuous"
                    if passed
                    else "continuous_comparator_superior"
                )
            side_steps.append(
                {
                    "hypothesis": name,
                    "tested": tested,
                    "passed": passed,
                    "day_simultaneous_lcb_usdc": day_band.lcb_usdc,
                    "week_simultaneous_lcb_usdc": week_band.lcb_usdc,
                    "identified_days": identified,
                    "total_days": total,
                    "reason": reason,
                }
            )
            parent = passed
        steps[side] = side_steps
        if all(step["passed"] for step in side_steps):
            supported.append(side)
    return {
        "steps": steps,
        "supported_sides": supported,
        "economic_epsilon_usdc": epsilon,
        "continuous_minus_boolean_is_a_dominance_block_not_a_positive_value_step": True,
    }


def run_nested_chronological_oof(
    panel: NestedOofPanel,
    *,
    fold_manifest: ProspectiveFoldManifest,
    ladder: Sequence[CandidateLadderEntry],
    continuous: ContinuousComparatorEntry,
    evaluator: SequentialPolicyEvaluator,
    label_provider: FoldScopedOneShotLabelProvider | None = None,
    config: NestedOofConfig | None = None,
) -> NestedOofExecutionResult:
    """Run nested OOF without loading or writing any research dataset.

    Candidate discovery and profile selection happen inside each outer fold.
    In formal mode, the mechanics panel has no labels and ``label_provider`` is
    called once with exactly that side/fold's purged outer-train rows.  Every
    inner fit and the outer refit then use only that materialized learning
    panel.  A policy is evaluated exactly once on untouched outer days.
    """

    config = NestedOofConfig() if config is None else config
    _validate_fold_manifest(
        fold_manifest,
        earliest_eligible_day=config.earliest_eligible_day,
    )
    _validate_ladder(ladder, config.sides)
    panel.validate(
        active_days=fold_manifest.active_days,
        sides=config.sides,
        panel_role=config.panel_role,
        earliest_eligible_day=config.earliest_eligible_day,
    )
    if label_provider is None and not panel.has_preconstructed_labels:
        raise NestedOofExecutionError(
            "an outcome-blind mechanics panel requires the formal fold-scoped label provider"
        )
    if label_provider is not None and panel.has_preconstructed_labels:
        raise NestedOofExecutionError(
            "formal fold-scoped execution forbids preconstructed panel labels"
        )
    for side in config.sides:
        if side not in continuous.feature_names_by_side:
            raise NestedOofExecutionError(f"continuous comparator lacks {side} features")
    oof_parts: list[pd.DataFrame] = []
    fold_records: list[dict[str, Any]] = []
    policies_for_stability: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for side_index, side in enumerate(config.sides):
        for outer_index, outer in enumerate(fold_manifest.outer_folds):
            outer_id = str(outer["fold_id"])
            outer_train, outer_purge = _purged_train_index(
                panel,
                side=side,
                train_days=outer["train_days"],
                test_days=outer["test_days"],
                fold_id=outer_id,
                stage="outer_refit",
            )
            if label_provider is None:
                learning_panel = panel
                label_materialization: Mapping[str, Any] = {
                    "mode": "nonformal_preconstructed_label_compatibility",
                    "side": side,
                    "outer_fold_id": outer_id,
                    "train_days": list(outer["train_days"]),
                    "row_count": len(outer_train),
                    "outer_test_rows_requested": None,
                    "unsupported_targets_remain_nan": True,
                }
            else:
                learning_panel, label_materialization = (
                    _materialize_outer_fold_learning_panel(
                        panel,
                        provider=label_provider,
                        side=side,
                        outer_fold_id=outer_id,
                        train_days=outer["train_days"],
                        train_index=outer_train,
                    )
                )
            frozen: dict[str, FittedCandidate] = {}
            candidate_records: dict[str, Any] = {}
            inner_purges: list[dict[str, Any]] = []
            for ladder_index, entry in enumerate(ladder):
                if entry.kind in {"exact_owner", "fixed"}:
                    candidate = _freeze_fixed_candidate(
                        learning_panel, entry=entry, side=side
                    )
                    profile_rows: list[dict[str, Any]] = []
                elif entry.kind == "boolean":
                    selected_profile, profile_rows, purges = _select_boolean_profile(
                        learning_panel,
                        entry=entry,
                        side=side,
                        inner_folds=outer["inner_folds"],
                        evaluator=evaluator,
                        random_seed=(
                            config.simultaneous_seed
                            + side_index * 100_000
                            + outer_index * 10_000
                            + ladder_index * 1_000
                        ),
                        panel_role=config.panel_role,
                    )
                    inner_purges.extend(asdict(audit) for audit in purges)
                    candidate = _fit_boolean_candidate(
                        learning_panel,
                        entry=entry,
                        side=side,
                        train_index=outer_train,
                        fold_id=outer_id,
                        profile=selected_profile,
                        random_seed=config.simultaneous_seed + outer_index * 100 + ladder_index,
                    )
                else:
                    assert entry.kind == "action_matched"
                    for source_name in entry.match_sources:
                        if source_name not in frozen:
                            raise NestedOofExecutionError(
                                f"action-matched source {source_name!r} is not frozen first"
                            )
                        matched_name = f"{entry.name}::{source_name}"
                        candidate = _fit_action_matched_candidate(
                            learning_panel,
                            source=frozen[source_name],
                            side=side,
                            train_index=outer_train,
                            fold_id=outer_id,
                            matched_name=matched_name,
                        )
                        frozen[matched_name] = candidate
                        candidate_records[matched_name] = {
                            "selected_profile": candidate.selected_profile,
                            "policy_sha256": candidate.policy_sha256,
                            "policy": candidate.policy_payload,
                            "fit_audit": candidate.fit_audit,
                            "feature_pool_audit": None,
                            "inner_profile_evidence": [],
                            "outer_test_outcomes_used_for_fit": False,
                            "candidate_replaced_by_baseline_before_outer_oof": False,
                        }
                    continue
                frozen[entry.name] = candidate
                candidate_records[entry.name] = {
                    "selected_profile": candidate.selected_profile,
                    "policy_sha256": candidate.policy_sha256,
                    "policy": candidate.policy_payload,
                    "fit_audit": candidate.fit_audit,
                    "feature_pool_audit": candidate.feature_pool_audit,
                    "inner_profile_evidence": profile_rows,
                    "outer_test_outcomes_used_for_fit": False,
                    "candidate_replaced_by_baseline_before_outer_oof": False,
                }

            selected_continuous, continuous_profiles, continuous_purges = (
                _select_continuous_profile(
                    learning_panel,
                    entry=continuous,
                    side=side,
                    inner_folds=outer["inner_folds"],
                    evaluator=evaluator,
                    random_seed=config.simultaneous_seed + side_index * 100_000 + outer_index * 10_000 + 9_000,
                    panel_role=config.panel_role,
                )
            )
            inner_purges.extend(asdict(audit) for audit in continuous_purges)
            frozen_continuous = _fit_continuous_candidate(
                learning_panel,
                entry=continuous,
                side=side,
                train_index=outer_train,
                profile=selected_continuous,
                random_seed=config.simultaneous_seed + outer_index * 100 + 99,
            )
            frozen[continuous.name] = frozen_continuous
            candidate_records[continuous.name] = {
                "selected_profile": frozen_continuous.selected_profile,
                "policy_sha256": frozen_continuous.policy_sha256,
                "policy": frozen_continuous.policy_payload,
                "fit_audit": frozen_continuous.fit_audit,
                "feature_pool_audit": None,
                "inner_profile_evidence": continuous_profiles,
                "outer_test_outcomes_used_for_fit": False,
                "candidate_replaced_by_baseline_before_outer_oof": False,
            }

            outer_results = _evaluate_outer_candidates(
                evaluator,
                frozen,
                side=side,
                days=outer["test_days"],
                fold_id=outer_id,
                panel_role=config.panel_role,
            )

            for name, candidate in frozen.items():
                evaluated = outer_results[name]
                oof_parts.append(evaluated)
                policies_for_stability.setdefault((side, name), []).append(
                    {
                        "fold_id": outer_id,
                        "selected_profile": candidate.selected_profile,
                        "policy": candidate.policy_payload.get(
                            "decision_policy", candidate.policy_payload
                        ),
                        "unsupported_count": int((~evaluated["point_identified"].astype(bool)).sum()),
                        "unobserved_count": int(
                            evaluated.filter(like="fallback_count::predicate_unobserved").sum().sum()
                        ),
                    }
                )
            baseline_reference: pd.Series | None = None
            for _name, evaluated in outer_results.items():
                current = evaluated.set_index("utc_day")["exact_owner_terminal_value_usdc"]
                if baseline_reference is None:
                    baseline_reference = current
                elif not np.allclose(
                    baseline_reference.to_numpy(dtype=float),
                    current.loc[baseline_reference.index].to_numpy(dtype=float),
                    rtol=0.0,
                    atol=1e-12,
                    equal_nan=True,
                ):
                    raise NestedOofExecutionError(
                        f"outer fold {outer_id} candidates do not share the exact owner control"
                    )
            b0 = outer_results["B0_CURRENT_EXACT"]
            known_b0 = b0["point_identified"].astype(bool)
            if (
                (b0.loc[known_b0, "delta_usdc"].abs() > config.zero_difference_tolerance_usdc).any()
                or (b0["nonbaseline_action_count"] != 0).any()
            ):
                raise NestedOofExecutionError("B0_CURRENT_EXACT is not an exact row-wise no-op")
            fold_records.append(
                {
                    "fold_id": outer_id,
                    "side": side,
                    "train_days": list(outer["train_days"]),
                    "test_days": list(outer["test_days"]),
                    "outer_purge": asdict(outer_purge),
                    "inner_purges": inner_purges,
                    "fold_scoped_label_materialization": label_materialization,
                    "candidates": candidate_records,
                    "oof_evidence_scope": OOF_EVIDENCE_SCOPE,
                    "exact_final_artifact_oof_available": False,
                }
            )

    oof = pd.concat(oof_parts, ignore_index=True)
    expected_names = (
        set(SUCCESSOR_CANDIDATE_LADDER)
        - {"ACTION_MATCHED_CONTROLS"}
        | set(MATCHED_CONTROL_NAMES)
        | {CONTINUOUS_COMPARATOR}
    )
    if set(oof["candidate_name"]) != expected_names:
        raise NestedOofExecutionError("outer OOF candidate census is incomplete")
    reports: dict[str, dict[str, Any]] = {}
    stability: dict[str, dict[str, Any]] = {}
    candidate_day_contrasts: dict[str, pd.Series] = {}
    for side in config.sides:
        for name in sorted(expected_names):
            rows = oof.loc[(oof["side"] == side) & (oof["candidate_name"] == name)].copy()
            key = f"{side}:{name}"
            reports[key] = _candidate_report(
                rows, tolerance=config.zero_difference_tolerance_usdc
            )
            stability[key] = summarize_fold_policy_stability(
                policies_for_stability[(side, name)]
            )
            series = _candidate_day_series(oof, side=side, candidate=name)
            if len(series) < 2:
                raise NestedOofExecutionError(f"{key} has fewer than two identified OOF days")
            candidate_day_contrasts[key] = series
    candidate_bands = webb_wild_day_max_t(
        candidate_day_contrasts,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed,
        confidence=config.confidence,
    )
    candidate_week_bands = webb_wild_day_max_t(
        {
            name: _week_block_series(series)
            for name, series in candidate_day_contrasts.items()
        },
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 10,
        confidence=config.confidence,
    )
    risk_day_contrasts: dict[str, pd.Series] = {}
    for side in config.sides:
        for name in sorted(expected_names):
            for metric in RISK_METRIC_COLUMNS:
                key = f"{side}:{name}:{metric}"
                series = _candidate_metric_day_series(
                    oof,
                    side=side,
                    candidate=name,
                    metric=metric,
                )
                if len(series) < 2:
                    raise NestedOofExecutionError(
                        f"{key} has fewer than two identified OOF days"
                    )
                risk_day_contrasts[key] = series
    risk_bands = webb_wild_day_max_t(
        risk_day_contrasts,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 3,
        confidence=config.confidence,
    )
    risk_week_bands = webb_wild_day_max_t(
        {
            name: _week_block_series(series)
            for name, series in risk_day_contrasts.items()
        },
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 13,
        confidence=config.confidence,
    )

    hierarchy_series: dict[str, pd.Series] = {}
    hierarchy_support: dict[str, tuple[int, int]] = {}
    hierarchy_map = config.hierarchy
    comparisons = (
        ("E1-B0", hierarchy_map.e1, "B0_CURRENT_EXACT"),
        ("E2-E1", hierarchy_map.e2, hierarchy_map.e1),
        ("E3-E2", hierarchy_map.e3, hierarchy_map.e2),
        ("M2-E3", hierarchy_map.m2, hierarchy_map.e3),
        ("CONTINUOUS-BOOLEAN", hierarchy_map.continuous, hierarchy_map.boolean),
    )
    for side in config.sides:
        for suffix, candidate, reference in comparisons:
            hypothesis = f"successor:{side}:{suffix}"
            series, identified_days, total_days = _paired_contrast(
                oof,
                side=side,
                candidate=candidate,
                reference=reference,
            )
            if len(series) < 2:
                raise NestedOofExecutionError(f"{hypothesis} has insufficient paired OOF days")
            hierarchy_series[hypothesis] = series
            hierarchy_support[hypothesis] = (identified_days, total_days)
    hierarchy_bands = webb_wild_day_max_t(
        hierarchy_series,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 1,
        confidence=config.confidence,
    )
    hierarchy_week_bands = webb_wild_day_max_t(
        {
            name: _week_block_series(series)
            for name, series in hierarchy_series.items()
        },
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 11,
        confidence=config.confidence,
    )
    confirmatory_series: dict[str, pd.Series] = {}
    for side in config.sides:
        for suffix, candidate, reference in CONFIRMATORY_COMPARISONS:
            hypothesis = f"successor:{side}:{suffix}"
            series, _identified_days, _total_days = _paired_contrast(
                oof,
                side=side,
                candidate=candidate,
                reference=reference,
            )
            if len(series) < 2:
                raise NestedOofExecutionError(
                    f"{hypothesis} has insufficient confirmatory OOF days"
                )
            confirmatory_series[hypothesis] = series
    confirmatory_bands = webb_wild_day_max_t(
        confirmatory_series,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 2,
        confidence=config.confidence,
    )
    confirmatory_week_bands = webb_wild_day_max_t(
        {
            name: _week_block_series(series)
            for name, series in confirmatory_series.items()
        },
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 12,
        confidence=config.confidence,
    )
    hierarchy = _hierarchy_report(
        hierarchy_bands,
        hierarchy_week_bands,
        hypothesis_support=hierarchy_support,
        sides=config.sides,
        epsilon=config.economic_epsilon_usdc,
    )
    scorecards = {
        key: _build_candidate_scorecard(
            side=key.split(":", 1)[0],
            candidate=key.split(":", 1)[1],
            report=report,
            candidate_bands=candidate_bands,
            candidate_week_bands=candidate_week_bands,
            risk_bands=risk_bands,
            risk_week_bands=risk_week_bands,
        )
        for key, report in sorted(reports.items())
    }
    return NestedOofExecutionResult(
        oof_rows=oof,
        fold_records=tuple(fold_records),
        candidate_reports=reports,
        stability=stability,
        candidate_bands=candidate_bands,
        candidate_week_bands=candidate_week_bands,
        hierarchy_bands=hierarchy_bands,
        hierarchy_week_bands=hierarchy_week_bands,
        confirmatory_bands=confirmatory_bands,
        confirmatory_week_bands=confirmatory_week_bands,
        risk_bands=risk_bands,
        risk_week_bands=risk_week_bands,
        scorecards=scorecards,
        hierarchy=hierarchy,
    )


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
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


def write_nested_oof_artifacts(
    result: NestedOofExecutionResult,
    output_dir: str | Path,
) -> Mapping[str, Any]:
    """Atomically publish the OOF report and one canonical scorecard per candidate."""

    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    bindings: dict[str, dict[str, str]] = {}
    report_path = root / "nested_oof_report.json"
    bindings["nested_oof_report"] = {
        "path": report_path.name,
        "sha256": _atomic_write_json(report_path, result.report()),
    }
    for key, scorecard in sorted(result.scorecards.items()):
        safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", key)
        path = root / f"{safe_key}.scorecard.json"
        bindings[f"scorecard::{key}"] = {
            "path": path.name,
            "sha256": _atomic_write_json(path, scorecard),
        }
    manifest = {
        "schema_version": f"{IDENTITY}.artifact_manifest.v1",
        "identity": IDENTITY,
        "oof_evidence_scope": OOF_EVIDENCE_SCOPE,
        "exact_final_artifact_oof_available": False,
        "bindings": bindings,
        "permissions": {
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    manifest_path = root / "manifest.json"
    manifest_sha256 = _atomic_write_json(manifest_path, manifest)
    return {
        **manifest,
        "manifest_path": manifest_path.name,
        "manifest_sha256": manifest_sha256,
    }


__all__ = [
    "CONTINUOUS_COMPARATOR",
    "IDENTITY",
    "LEARNED_BOOLEAN_ORDER",
    "MATCHED_CONTROL_NAMES",
    "OOF_EVIDENCE_SCOPE",
    "PANEL_ROLE",
    "RISK_METRIC_COLUMNS",
    "BatchSequentialPolicyEvaluator",
    "CandidateLadderEntry",
    "ContinuousComparatorEntry",
    "EvaluationRequest",
    "FoldScopedOneShotLabelBatch",
    "FoldScopedOneShotLabelProvider",
    "FoldScopedOneShotLabelRequest",
    "FittedCandidate",
    "HierarchyCandidates",
    "NestedOofConfig",
    "NestedOofExecutionError",
    "NestedOofExecutionResult",
    "NestedOofPanel",
    "SequentialEvaluationResult",
    "SequentialPolicyEvaluator",
    "bind_fold_scoped_one_shot_labels",
    "run_nested_chronological_oof",
    "write_nested_oof_artifacts",
]
