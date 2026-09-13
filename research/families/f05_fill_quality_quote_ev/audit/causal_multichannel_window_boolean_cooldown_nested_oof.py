"""Outcome-agnostic nested chronological Boolean learner for cooldown v2.

The module consumes an already assembled long-form table.  It does not load
market data, strict-native labels, reports, model artifacts, or registries.
Candidate discovery is confined to inner chronological folds; the exact
nonbaseline candidate selected there is then executed unchanged on the outer
fold.  Deployment evidence is evaluated only by a separate post-OOF function.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from itertools import combinations, product
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BUY_DURATION_POLICY_IDS,
    SELL_DURATION_POLICY_IDS,
    TriState,
    tri_and,
    tri_not,
    tri_or,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
LEARNER_IDENTITY = f"{IDENTITY}.nested_chronological_boolean_oof.v1"
CONTROL_ACTION = "CONTROL_85N"
FEATURE_BLOCKS = ("R0", "M0", "M1", "M2")
SOURCE_PANEL_ROLES = ("prefix40", "added10")
REPORT_PANEL_SCOPES = ("prefix40", "added10", "pooled50")


class NestedOofContractError(ValueError):
    """Raised when an input, split, Boolean, or authority contract drifts."""


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _normalize_day(value: Any) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise NestedOofContractError(f"invalid UTC day: {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    if parsed != parsed.normalize():
        raise NestedOofContractError(f"UTC day includes a time component: {value!r}")
    return parsed.strftime("%Y-%m-%d")


def duration_vocabulary(side: str) -> tuple[str, ...]:
    normalized = str(side).upper()
    if normalized == "BUY":
        return tuple(BUY_DURATION_POLICY_IDS)
    if normalized == "SELL":
        return tuple(SELL_DURATION_POLICY_IDS)
    raise NestedOofContractError("side-specific learner requires BUY or SELL")


@dataclass(frozen=True, order=True, slots=True)
class TriLiteral:
    """One three-valued literal; negation never turns missing into evidence."""

    predicate: str
    negated: bool = False

    def __post_init__(self) -> None:
        if not str(self.predicate).strip():
            raise NestedOofContractError("literal predicate is empty")

    def evaluate_state(self, value: TriState | int) -> TriState:
        state = TriState(int(value))
        return tri_not(state) if self.negated else state

    def evaluate(self, predicates: pd.DataFrame) -> np.ndarray:
        if self.predicate not in predicates:
            raise NestedOofContractError(f"policy input is missing predicate {self.predicate!r}")
        raw = predicates[self.predicate].to_numpy(copy=False)
        try:
            values = raw.astype(np.int8, copy=False)
        except (TypeError, ValueError) as exc:
            raise NestedOofContractError(
                f"predicate {self.predicate!r} is not three-valued"
            ) from exc
        if not np.isin(values, (-1, 0, 1)).all():
            raise NestedOofContractError(
                f"predicate {self.predicate!r} is outside TRUE/FALSE/UNOBSERVED"
            )
        if not self.negated:
            return values.copy()
        return np.where(values == -1, -1, 1 - values).astype(np.int8)

    def payload(self) -> dict[str, Any]:
        return {"predicate": self.predicate, "negated": self.negated}


@dataclass(frozen=True, slots=True)
class AndClause:
    """Conjunction of literals using Kleene three-valued semantics."""

    literals: tuple[TriLiteral, ...]

    def __post_init__(self) -> None:
        if not self.literals:
            raise NestedOofContractError("AND clause cannot be empty")
        if tuple(sorted(self.literals)) != self.literals:
            raise NestedOofContractError("clause literals must be canonically sorted")
        names = tuple(literal.predicate for literal in self.literals)
        if len(names) != len(set(names)):
            raise NestedOofContractError("a clause cannot repeat or complement the same predicate")

    @property
    def key(self) -> tuple[tuple[str, bool], ...]:
        return tuple((literal.predicate, literal.negated) for literal in self.literals)

    def evaluate_state(self, values: Mapping[str, TriState | int]) -> TriState:
        return tri_and(
            tuple(literal.evaluate_state(values[literal.predicate]) for literal in self.literals)
        )

    def evaluate(self, predicates: pd.DataFrame) -> np.ndarray:
        matrix = np.column_stack([literal.evaluate(predicates) for literal in self.literals])
        any_false = (matrix == int(TriState.FALSE)).any(axis=1)
        any_unobserved = (matrix == int(TriState.UNOBSERVED)).any(axis=1)
        return np.where(any_false, 0, np.where(any_unobserved, -1, 1)).astype(np.int8)

    def payload(self) -> dict[str, Any]:
        return {"literals": [literal.payload() for literal in self.literals]}


@dataclass(frozen=True, slots=True)
class BooleanRule:
    """Sparse DNF rule: OR across AND clauses."""

    action: str
    clauses: tuple[AndClause, ...]

    def __post_init__(self) -> None:
        if not self.clauses:
            raise NestedOofContractError("Boolean rule requires at least one clause")
        keys = tuple(clause.key for clause in self.clauses)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise NestedOofContractError("OR clauses must be unique and canonical")

    def evaluate_state(self, values: Mapping[str, TriState | int]) -> TriState:
        return tri_or(tuple(clause.evaluate_state(values) for clause in self.clauses))

    def evaluate(self, predicates: pd.DataFrame) -> np.ndarray:
        matrix = np.column_stack([clause.evaluate(predicates) for clause in self.clauses])
        any_true = (matrix == int(TriState.TRUE)).any(axis=1)
        any_unobserved = (matrix == int(TriState.UNOBSERVED)).any(axis=1)
        return np.where(any_true, 1, np.where(any_unobserved, -1, 0)).astype(np.int8)

    def payload(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "clauses": [clause.payload() for clause in self.clauses],
        }


@dataclass(frozen=True, slots=True)
class BooleanCooldownPolicy:
    """Side-specific ordered first-match policy with baseline fallback."""

    side: str
    rules: tuple[BooleanRule, ...]
    default_action: str = CONTROL_ACTION

    def __post_init__(self) -> None:
        normalized = str(self.side).upper()
        object.__setattr__(self, "side", normalized)
        vocabulary = set(duration_vocabulary(normalized))
        if self.default_action != CONTROL_ACTION:
            raise NestedOofContractError("v2 exploratory policy must default to CONTROL_85N")
        if not self.rules:
            raise NestedOofContractError("exploratory policy cannot be all-baseline")
        if any(rule.action not in vocabulary - {CONTROL_ACTION} for rule in self.rules):
            raise NestedOofContractError("rule action is outside side-specific vocabulary")

    @property
    def predicate_columns(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    literal.predicate
                    for rule in self.rules
                    for clause in rule.clauses
                    for literal in clause.literals
                }
            )
        )

    @property
    def complexity(self) -> tuple[int, int, int]:
        clauses = sum(len(rule.clauses) for rule in self.rules)
        literals = sum(len(clause.literals) for rule in self.rules for clause in rule.clauses)
        return (len(self.rules), clauses, literals)

    @property
    def candidate_id(self) -> str:
        return _canonical_sha256(self.payload())

    def payload(self) -> dict[str, Any]:
        return {
            "identity": LEARNER_IDENTITY,
            "side": self.side,
            "ordered_first_match_rules": [rule.payload() for rule in self.rules],
            "default_action": self.default_action,
            "permissions": {
                "action_authorized": False,
                "live_authorized": False,
            },
        }

    def choose(self, predicates: pd.DataFrame) -> np.ndarray:
        missing = set(self.predicate_columns) - set(predicates)
        if missing:
            raise NestedOofContractError(f"policy input is missing predicates: {sorted(missing)}")
        result = np.full(len(predicates), self.default_action, dtype=object)
        still_decidable = np.ones(len(predicates), dtype=bool)
        for rule in self.rules:
            state = rule.evaluate(predicates)
            matched = still_decidable & (state == int(TriState.TRUE))
            result[matched] = rule.action
            # If an earlier first-match rule is unobserved, a later rule cannot
            # safely determine which action would have owned the opportunity.
            still_decidable &= state == int(TriState.FALSE)
        return result


@dataclass(frozen=True, slots=True)
class LongFormColumns:
    opportunity: str = "opportunity_id"
    day: str = "utc_day"
    panel_role: str = "panel_role"
    side: str = "side"
    role: str = "role_at_fill"
    campaign: str = "campaign_id"
    action: str = "duration_policy_id"
    outcome: str = "terminal_value_usdc"
    strict_native: str = "strict_native_label"


@dataclass(frozen=True, slots=True)
class ChronologicalFold:
    fold_id: str
    train_days: tuple[str, ...]
    test_days: tuple[str, ...]

    def __post_init__(self) -> None:
        train = tuple(_normalize_day(day) for day in self.train_days)
        test = tuple(_normalize_day(day) for day in self.test_days)
        if not self.fold_id or not train or not test:
            raise NestedOofContractError("chronological fold is incomplete")
        if train != tuple(sorted(set(train))) or test != tuple(sorted(set(test))):
            raise NestedOofContractError("fold days must be sorted and unique")
        if set(train) & set(test):
            raise NestedOofContractError("fold train/test days overlap")
        if max(train) >= min(test):
            raise NestedOofContractError("fold is not strictly chronological")
        object.__setattr__(self, "train_days", train)
        object.__setattr__(self, "test_days", test)


def expanding_chronological_folds(
    days: Sequence[str],
    *,
    fold_prefix: str,
    n_folds: int,
    minimum_train_days: int,
) -> tuple[ChronologicalFold, ...]:
    ordered = tuple(sorted({_normalize_day(day) for day in days}))
    if n_folds < 1 or minimum_train_days < 1:
        raise NestedOofContractError("fold counts must be positive")
    testable = len(ordered) - minimum_train_days
    if testable < n_folds:
        raise NestedOofContractError("not enough chronological days for folds")
    positions = np.array_split(np.arange(minimum_train_days, len(ordered), dtype=np.int64), n_folds)
    folds: list[ChronologicalFold] = []
    for index, block in enumerate(positions, start=1):
        if len(block) == 0:
            raise NestedOofContractError("chronological fold has no test days")
        start = int(block[0])
        folds.append(
            ChronologicalFold(
                fold_id=f"{fold_prefix}{index}",
                train_days=ordered[:start],
                test_days=tuple(ordered[int(position)] for position in block),
            )
        )
    return tuple(folds)


@dataclass(frozen=True, slots=True)
class SearchConfig:
    max_literals_per_clause: int = 2
    max_clauses_per_rule: int = 2
    max_rules_per_policy: int = 2
    max_clause_candidates: int = 96
    max_rule_candidates: int = 192
    max_policy_candidates: int = 384
    inner_folds: int = 3
    inner_minimum_train_days: int = 3
    minimum_action_opportunities: int = 3
    minimum_action_campaigns: int = 2
    minimum_action_days: int = 2
    confidence: float = 0.95

    def __post_init__(self) -> None:
        integers = (
            self.max_literals_per_clause,
            self.max_clauses_per_rule,
            self.max_rules_per_policy,
            self.max_clause_candidates,
            self.max_rule_candidates,
            self.max_policy_candidates,
            self.inner_folds,
            self.inner_minimum_train_days,
            self.minimum_action_opportunities,
            self.minimum_action_campaigns,
            self.minimum_action_days,
        )
        if any(value < 1 for value in integers):
            raise NestedOofContractError("bounded search parameters must be positive")
        if self.max_literals_per_clause > 3 or self.max_clauses_per_rule > 3:
            raise NestedOofContractError("v2 search bounds exceed the audited limit")
        if self.max_rules_per_policy > 2:
            raise NestedOofContractError("v2 supports at most two first-match rules")
        if not 0.5 < self.confidence < 1.0:
            raise NestedOofContractError("confidence must be between 0.5 and 1")


@dataclass(frozen=True, slots=True)
class SupportAudit:
    action_opportunities: int
    action_campaigns: int
    action_days: int
    action_rate: float
    passed: bool


@dataclass(frozen=True, slots=True)
class ClusteredEstimate:
    mean_usdc: float
    standard_error_usdc: float
    lcb_usdc: float
    ucb_usdc: float
    confidence: float
    opportunities: int
    campaigns: int
    days: int
    action_rate: float
    training_weight_contract: str = "each_campaign_total_weight_equals_one"
    interval_cluster_contract: str = "utc_day_cluster_over_campaign_weighted_rows"


@dataclass(frozen=True, slots=True)
class PreparedPanel:
    side: str
    panel_scope: str
    features: pd.DataFrame
    outcomes: pd.DataFrame
    predicate_columns: tuple[str, ...]
    vocabulary: tuple[str, ...]
    columns: LongFormColumns
    panel_role_counts: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class OuterFoldExecution:
    fold_id: str
    train_days: tuple[str, ...]
    test_days: tuple[str, ...]
    selected_policy: BooleanCooldownPolicy
    inner_estimate: ClusteredEstimate
    inner_support: SupportAudit
    outer_support: SupportAudit
    candidate_was_replaced_by_baseline: bool
    oof_rows: pd.DataFrame


@dataclass(frozen=True, slots=True)
class NestedOofResult:
    side: str
    feature_block: str
    panel_scope: str
    folds: tuple[OuterFoldExecution, ...]
    oof_rows: pd.DataFrame
    estimate: ClusteredEstimate
    combined_support: SupportAudit
    role_support: Mapping[str, Mapping[str, Any]]
    panel_role_counts: Mapping[str, int]
    permissions: Mapping[str, bool]
    evidence_role: str

    @property
    def candidate_ids(self) -> tuple[str, ...]:
        return tuple(fold.selected_policy.candidate_id for fold in self.folds)

    def summary(self) -> dict[str, Any]:
        return {
            "identity": LEARNER_IDENTITY,
            "side": self.side,
            "feature_block": self.feature_block,
            "panel_scope": self.panel_scope,
            "evidence_role": self.evidence_role,
            "candidate_ids": list(self.candidate_ids),
            "estimate": asdict(self.estimate),
            "combined_action_support": asdict(self.combined_support),
            "role_support": dict(self.role_support),
            "panel_role_counts": dict(self.panel_role_counts),
            "candidate_replaced_before_outer_oof": False,
            "permissions": dict(self.permissions),
        }


@dataclass(frozen=True, slots=True)
class DeploymentGateResult:
    passed: bool
    decision: str
    reasons: tuple[str, ...]
    estimate: ClusteredEstimate
    combined_support: SupportAudit
    outer_fold_support: Mapping[str, SupportAudit]
    zero_action_outer_folds: tuple[str, ...]
    required_roles: tuple[str, ...]
    role_gates: Mapping[str, Mapping[str, Any]]
    action_authorized: bool = False
    live_authorized: bool = False


@dataclass(frozen=True, slots=True)
class FeatureBlockComparison:
    panel_scope: str
    results: Mapping[str, Mapping[str, NestedOofResult]]
    pooled_side_policy_created: bool = False
    action_authorized: bool = False
    live_authorized: bool = False


def _validate_predicates(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    for column in columns:
        if column not in frame:
            raise NestedOofContractError(f"missing predicate column: {column}")
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if numeric.isna().any() or not numeric.isin((-1, 0, 1)).all():
            raise NestedOofContractError(
                f"predicate {column!r} must use TRUE/FALSE/UNOBSERVED integers"
            )


def _assert_group_constant(
    frame: pd.DataFrame, group_column: str, value_columns: Sequence[str]
) -> None:
    grouped = frame.groupby(group_column, sort=False, observed=True)
    for column in value_columns:
        if grouped[column].nunique(dropna=False).gt(1).any():
            raise NestedOofContractError(
                f"opportunity rows disagree on feature/metadata column {column!r}"
            )


def prepare_long_form_panel(
    table: pd.DataFrame,
    *,
    side: str,
    panel_scope: str,
    predicate_columns: Sequence[str],
    columns: LongFormColumns | None = None,
) -> PreparedPanel:
    """Validate and collapse a strict-native long-form label/feature table."""

    schema = columns or LongFormColumns()
    normalized_side = str(side).upper()
    vocabulary = duration_vocabulary(normalized_side)
    scope = str(panel_scope)
    if scope not in REPORT_PANEL_SCOPES:
        raise NestedOofContractError(
            "panel_scope must be prefix40, added10, or pooled50; "
            "Validation/holdout aliases are forbidden"
        )
    predicates = tuple(dict.fromkeys(str(value) for value in predicate_columns))
    if not predicates:
        raise NestedOofContractError("at least one Boolean predicate is required")
    required = {
        schema.opportunity,
        schema.day,
        schema.panel_role,
        schema.side,
        schema.role,
        schema.campaign,
        schema.action,
        schema.outcome,
        schema.strict_native,
        *predicates,
    }
    if schema.role not in table:
        raise NestedOofContractError(f"required role field {schema.role!r} is missing")
    missing = required - set(table)
    if missing:
        raise NestedOofContractError(f"long-form table is missing: {sorted(missing)}")
    frame = table.loc[:, sorted(required)].copy()
    frame[schema.side] = frame[schema.side].astype(str).str.upper()
    if set(frame[schema.side]) - {"BUY", "SELL"}:
        raise NestedOofContractError("long-form table contains an invalid side")
    frame = frame.loc[frame[schema.side] == normalized_side].copy()
    if frame.empty:
        raise NestedOofContractError(f"no rows exist for side {normalized_side}")
    frame[schema.day] = frame[schema.day].map(_normalize_day)
    frame[schema.panel_role] = frame[schema.panel_role].astype(str)
    invalid_roles = set(frame[schema.panel_role]) - set(SOURCE_PANEL_ROLES)
    if invalid_roles:
        raise NestedOofContractError(
            f"panel rows use forbidden evidence roles: {sorted(invalid_roles)}"
        )
    if scope != "pooled50":
        frame = frame.loc[frame[schema.panel_role] == scope].copy()
    if frame.empty:
        raise NestedOofContractError(f"panel scope {scope} has no rows")
    if not frame[schema.strict_native].map(lambda value: isinstance(value, (bool, np.bool_))).all():
        raise NestedOofContractError("strict_native_label must be explicit bool")
    if not frame[schema.strict_native].all():
        raise NestedOofContractError("non-strict labels cannot enter this learner")
    if frame[schema.role].isna().any():
        raise NestedOofContractError("role field contains a missing value")
    frame[schema.role] = frame[schema.role].astype(str).str.strip().str.lower()
    if set(frame[schema.role]) - {"opener", "add"}:
        raise NestedOofContractError("only opener/add opportunities are supported")
    for name in (schema.opportunity, schema.campaign, schema.action):
        frame[name] = frame[name].astype(str)
        if frame[name].str.strip().eq("").any() or frame[name].str.lower().eq("nan").any():
            raise NestedOofContractError(f"{name} contains an empty/NaN identity")
    if frame.duplicated([schema.opportunity, schema.action]).any():
        raise NestedOofContractError("opportunity/action rows must be unique")
    _validate_predicates(frame, predicates)
    outcomes = pd.to_numeric(frame[schema.outcome], errors="coerce")
    if outcomes.isna().any() or not np.isfinite(outcomes.to_numpy(dtype=float)).all():
        raise NestedOofContractError("terminal value labels must be finite")
    frame[schema.outcome] = outcomes.astype(float)
    metadata = (
        schema.day,
        schema.panel_role,
        schema.side,
        schema.role,
        schema.campaign,
        *predicates,
    )
    _assert_group_constant(frame, schema.opportunity, metadata)
    expected_actions = set(vocabulary)
    action_sets = frame.groupby(schema.opportunity, sort=False, observed=True)[schema.action].agg(
        lambda values: set(values)
    )
    if not action_sets.map(lambda values: values == expected_actions).all():
        raise NestedOofContractError(
            "every opportunity must contain the exact side-specific duration vocabulary"
        )
    features = (
        frame[[schema.opportunity, *metadata]]
        .drop_duplicates(schema.opportunity)
        .set_index(schema.opportunity, drop=True)
        .sort_values([schema.day, schema.campaign], kind="stable")
    )
    # A campaign is one clustering unit and may not be split across UTC day
    # folds. Cross-day campaigns must be assigned an explicit common cluster
    # day upstream rather than silently leaking across a fold boundary.
    campaign_days = features.groupby(schema.campaign, observed=True)[schema.day].nunique()
    if campaign_days.gt(1).any():
        raise NestedOofContractError("a campaign cluster spans multiple fold days")
    outcome_matrix = frame.pivot(
        index=schema.opportunity,
        columns=schema.action,
        values=schema.outcome,
    ).loc[features.index, list(vocabulary)]
    role_counts = features[schema.panel_role].value_counts().sort_index().astype(int).to_dict()
    return PreparedPanel(
        side=normalized_side,
        panel_scope=scope,
        features=features,
        outcomes=outcome_matrix,
        predicate_columns=predicates,
        vocabulary=vocabulary,
        columns=schema,
        panel_role_counts=role_counts,
    )


@dataclass(frozen=True, slots=True)
class _PredicateDescriptor:
    predicate: str
    channel_group: str
    semantic_group: str
    clock_group: str


@dataclass(frozen=True, slots=True)
class _ClauseUniverse:
    clauses: tuple[AndClause, ...]
    atomic_clauses: tuple[AndClause, ...]
    selected: tuple[_PredicateDescriptor, ...]
    structures: frozenset[str]
    descriptor_by_predicate: Mapping[str, _PredicateDescriptor]


def _stable_search_key(*parts: Any) -> str:
    return _canonical_sha256(parts)


def _infer_channel_group(predicate: str) -> str:
    body = str(predicate)
    for prefix in ("tri::quantile::value::", "tri::", "value::"):
        if body.startswith(prefix):
            body = body[len(prefix) :]
            break
    head = body.split("::", 1)[0]
    if "__h" in head:
        head = head.split("__h", 1)[0]
    return head or body


def _infer_semantic_group(predicate: str) -> str:
    parts = str(predicate).split("::")
    if "quantile" in parts:
        for semantic in (
            "cross_age_s",
            "persistence_s",
            "normalized_distance",
            "distance",
            "curvature",
            "slope",
            "ema",
        ):
            if semantic in parts:
                return f"quantile_{semantic}"
        return "quantile"
    return parts[-1] if parts else str(predicate)


def _predicate_descriptors(
    predicate_columns: Sequence[str],
    *,
    predicate_channel_groups: Mapping[str, str] | None,
    predicate_semantic_groups: Mapping[str, str] | None,
    predicate_clock_groups: Mapping[str, str] | None,
) -> tuple[_PredicateDescriptor, ...]:
    predicates = tuple(dict.fromkeys(str(value) for value in predicate_columns))
    if not predicates or any(not value.strip() for value in predicates):
        raise NestedOofContractError("candidate predicates must be nonempty")

    def resolve(
        mapping: Mapping[str, str] | None,
        predicate: str,
        *,
        label: str,
        fallback: str,
    ) -> str:
        if mapping is not None and predicate not in mapping:
            raise NestedOofContractError(f"predicate {label} mapping is missing {predicate!r}")
        value = fallback if mapping is None else str(mapping[predicate])
        if not value.strip():
            raise NestedOofContractError(f"predicate {label} is empty for {predicate!r}")
        return value

    descriptors: list[_PredicateDescriptor] = []
    for predicate in predicates:
        clock = resolve(
            predicate_clock_groups,
            predicate,
            label="clock group",
            fallback="context",
        )
        if clock not in {"book", "trade", "context"}:
            raise NestedOofContractError(
                f"invalid predicate clock group for {predicate!r}: {clock!r}"
            )
        descriptors.append(
            _PredicateDescriptor(
                predicate=predicate,
                channel_group=resolve(
                    predicate_channel_groups,
                    predicate,
                    label="channel group",
                    fallback=_infer_channel_group(predicate),
                ),
                semantic_group=resolve(
                    predicate_semantic_groups,
                    predicate,
                    label="semantic group",
                    fallback=_infer_semantic_group(predicate),
                ),
                clock_group=clock,
            )
        )
    return tuple(descriptors)


def _stratified_predicate_take(
    descriptors: Sequence[_PredicateDescriptor], limit: int
) -> tuple[_PredicateDescriptor, ...]:
    """Round-robin channels first, then semantic groups within each channel."""

    grouped: dict[str, dict[str, list[_PredicateDescriptor]]] = {}
    for descriptor in descriptors:
        grouped.setdefault(descriptor.channel_group, {}).setdefault(
            descriptor.semantic_group, []
        ).append(descriptor)
    for semantic_groups in grouped.values():
        for values in semantic_groups.values():
            values.sort(
                key=lambda value: _stable_search_key(
                    value.channel_group,
                    value.semantic_group,
                    value.predicate,
                )
            )

    channels = sorted(grouped, key=lambda value: _stable_search_key("channel", value))
    semantics = {
        channel: sorted(
            grouped[channel],
            key=lambda value: _stable_search_key("semantic", channel, value),
        )
        for channel in channels
    }
    semantic_cursor = {channel: 0 for channel in channels}
    selected: list[_PredicateDescriptor] = []
    while len(selected) < min(limit, len(descriptors)):
        progressed = False
        for channel in channels:
            names = semantics[channel]
            for offset in range(len(names)):
                index = (semantic_cursor[channel] + offset) % len(names)
                semantic = names[index]
                bucket = grouped[channel][semantic]
                if not bucket:
                    continue
                selected.append(bucket.pop(0))
                semantic_cursor[channel] = (index + 1) % len(names)
                progressed = True
                break
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return tuple(selected)


def _stratified_take(
    values: Sequence[Any],
    *,
    limit: int,
    stratum_key: Any,
    stable_key: Any,
) -> tuple[Any, ...]:
    grouped: dict[Any, list[Any]] = {}
    for value in values:
        grouped.setdefault(stratum_key(value), []).append(value)
    for group in grouped.values():
        group.sort(key=stable_key)
    strata = sorted(grouped, key=lambda value: _stable_search_key("stratum", value))
    output: list[Any] = []
    while len(output) < min(limit, len(values)):
        progressed = False
        for stratum in strata:
            bucket = grouped[stratum]
            if not bucket:
                continue
            output.append(bucket.pop(0))
            progressed = True
            if len(output) >= limit:
                break
        if not progressed:
            break
    return tuple(output)


def _structure_plan(
    *,
    predicate_count: int,
    action_count: int,
    config: SearchConfig,
) -> tuple[int, frozenset[str]]:
    if config.max_clause_candidates < 2 or config.max_rule_candidates < 2:
        raise NestedOofContractError(
            "bounded universe needs at least two clause/rule candidates for "
            "positive and NOT literal coverage"
        )
    if config.max_policy_candidates < action_count:
        raise NestedOofContractError(
            "bounded universe policy budget cannot cover every duration action"
        )

    possible: list[str] = []
    if predicate_count >= 2 and config.max_literals_per_clause >= 2:
        possible.append("and")
    if predicate_count >= 2 and config.max_clauses_per_rule >= 2:
        possible.append("or")
    if config.max_rules_per_policy >= 2:
        possible.append("ordered")

    subsets: list[frozenset[str]] = []
    for width in range(len(possible), -1, -1):
        subsets.extend(frozenset(values) for values in combinations(possible, width))
    subsets.sort(
        key=lambda values: (
            -len(values),
            tuple(name not in values for name in ("and", "or", "ordered")),
        )
    )
    for structures in subsets:
        selected_limit = predicate_count
        if "and" in structures:
            # Keep an outcome-blind third of the clause budget available for
            # cross-predicate interactions. Otherwise polarity-complete
            # single literals consume almost the entire M2 universe and AND
            # coverage is technically present but substantively starved.
            interaction_reserve = max(1, config.max_clause_candidates // 3)
            selected_limit = min(
                selected_limit,
                max(2, (config.max_clause_candidates - interaction_reserve) // 2),
            )
        for selected_count in range(selected_limit, 0, -1):
            if ("and" in structures or "or" in structures) and selected_count < 2:
                continue
            clauses = 2 * selected_count + int("and" in structures)
            bodies = clauses + int("or" in structures)
            policies = max(2 * selected_count, action_count) + len(structures)
            if (
                clauses <= config.max_clause_candidates
                and bodies <= config.max_rule_candidates
                and policies <= config.max_policy_candidates
            ):
                return selected_count, structures
    raise NestedOofContractError(
        "bounded universe budgets cannot cover literal polarity and duration actions"
    )


def _build_clause_universe(
    predicate_columns: Sequence[str],
    config: SearchConfig,
    *,
    predicate_channel_groups: Mapping[str, str] | None,
    predicate_semantic_groups: Mapping[str, str] | None,
    predicate_clock_groups: Mapping[str, str] | None,
) -> _ClauseUniverse:
    descriptors = _predicate_descriptors(
        predicate_columns,
        predicate_channel_groups=predicate_channel_groups,
        predicate_semantic_groups=predicate_semantic_groups,
        predicate_clock_groups=predicate_clock_groups,
    )
    actions_per_side = len(duration_vocabulary("BUY")) - 1
    selected_count, structures = _structure_plan(
        predicate_count=len(descriptors),
        action_count=actions_per_side,
        config=config,
    )
    selected = _stratified_predicate_take(descriptors, selected_count)
    by_predicate = {value.predicate: value for value in selected}
    atomic = tuple(
        AndClause((TriLiteral(descriptor.predicate, negated),))
        for descriptor in selected
        for negated in (False, True)
    )
    remaining = config.max_clause_candidates - len(atomic)
    interaction_candidates: list[AndClause] = []
    if remaining > 0 and config.max_literals_per_clause >= 2:
        for width in range(2, config.max_literals_per_clause + 1):
            for descriptor_group in combinations(selected, width):
                for negations in product((False, True), repeat=width):
                    interaction_candidates.append(
                        AndClause(
                            tuple(
                                sorted(
                                    TriLiteral(descriptor.predicate, negated)
                                    for descriptor, negated in zip(
                                        descriptor_group, negations, strict=True
                                    )
                                )
                            )
                        )
                    )

    interactions = _stratified_take(
        interaction_candidates,
        limit=remaining,
        stratum_key=lambda clause: (
            len(clause.literals),
            tuple(
                sorted(
                    {by_predicate[literal.predicate].channel_group for literal in clause.literals}
                )
            ),
            tuple(
                sorted(
                    {by_predicate[literal.predicate].semantic_group for literal in clause.literals}
                )
            ),
        ),
        stable_key=lambda clause: _stable_search_key("clause", clause.key),
    )
    if "and" in structures and not interactions:
        structures = frozenset(structure for structure in structures if structure != "and")
    return _ClauseUniverse(
        clauses=atomic + interactions,
        atomic_clauses=atomic,
        selected=selected,
        structures=structures,
        descriptor_by_predicate=by_predicate,
    )


def _clauses_for_search(
    predicate_columns: Sequence[str],
    config: SearchConfig,
    *,
    predicate_channel_groups: Mapping[str, str] | None = None,
    predicate_semantic_groups: Mapping[str, str] | None = None,
    predicate_clock_groups: Mapping[str, str] | None = None,
) -> tuple[AndClause, ...]:
    return _build_clause_universe(
        predicate_columns,
        config,
        predicate_channel_groups=predicate_channel_groups,
        predicate_semantic_groups=predicate_semantic_groups,
        predicate_clock_groups=predicate_clock_groups,
    ).clauses


def _body_key(body: tuple[AndClause, ...]) -> tuple[Any, ...]:
    return tuple(clause.key for clause in body)


def _body_structure(body: tuple[AndClause, ...]) -> str:
    if len(body) > 1:
        return "or"
    if len(body[0].literals) > 1:
        return "and"
    return "literal"


def _body_channels(
    body: tuple[AndClause, ...],
    descriptors: Mapping[str, _PredicateDescriptor],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                descriptors[literal.predicate].channel_group
                for clause in body
                for literal in clause.literals
            }
        )
    )


def _rule_bodies(
    universe: _ClauseUniverse, config: SearchConfig
) -> tuple[tuple[AndClause, ...], ...]:
    atomic_bodies = tuple((clause,) for clause in universe.atomic_clauses)
    interaction_bodies = tuple((clause,) for clause in universe.clauses if len(clause.literals) > 1)
    or_candidates: list[tuple[AndClause, ...]] = []
    if config.max_clauses_per_rule >= 2:
        for left, right in combinations(universe.atomic_clauses, 2):
            if left.literals[0].predicate == right.literals[0].predicate:
                continue
            or_candidates.append(tuple(sorted((left, right), key=lambda clause: clause.key)))
    or_bodies = _stratified_take(
        or_candidates,
        limit=len(or_candidates),
        stratum_key=lambda body: _body_channels(body, universe.descriptor_by_predicate),
        stable_key=lambda body: _stable_search_key("or", _body_key(body)),
    )

    bodies: list[tuple[AndClause, ...]] = list(atomic_bodies)
    if "and" in universe.structures and interaction_bodies:
        bodies.append(interaction_bodies[0])
    if "or" in universe.structures and or_bodies:
        bodies.append(or_bodies[0])
    remaining_candidates = [
        body for body in (*interaction_bodies, *or_bodies) if body not in bodies
    ]
    remaining = config.max_rule_candidates - len(bodies)
    bodies.extend(
        _stratified_take(
            remaining_candidates,
            limit=max(0, remaining),
            stratum_key=lambda body: (
                _body_structure(body),
                _body_channels(body, universe.descriptor_by_predicate),
            ),
            stable_key=lambda body: _stable_search_key("body", _body_key(body)),
        )
    )
    return tuple(bodies[: config.max_rule_candidates])


def _validate_policy_clock_groups(
    policy: BooleanCooldownPolicy,
    predicate_clock_groups: Mapping[str, str] | None,
) -> None:
    if predicate_clock_groups is None:
        return
    accepted = {"book", "trade", "context"}
    for rule in policy.rules:
        for clause in rule.clauses:
            missing = {literal.predicate for literal in clause.literals} - set(
                predicate_clock_groups
            )
            if missing:
                raise NestedOofContractError(
                    f"predicate clock group mapping is missing: {sorted(missing)}"
                )
            invalid = {
                str(predicate_clock_groups[literal.predicate])
                for literal in clause.literals
                if str(predicate_clock_groups[literal.predicate]) not in accepted
            }
            if invalid:
                raise NestedOofContractError(
                    f"predicate clock group mapping contains invalid values: {sorted(invalid)}"
                )


def generate_bounded_candidates(
    *,
    side: str,
    predicate_columns: Sequence[str],
    config: SearchConfig | None = None,
    predicate_channel_groups: Mapping[str, str] | None = None,
    predicate_semantic_groups: Mapping[str, str] | None = None,
    predicate_clock_groups: Mapping[str, str] | None = None,
) -> tuple[BooleanCooldownPolicy, ...]:
    """Build a stratified, deterministic, outcome-blind Boolean universe."""

    settings = config or SearchConfig()
    universe = _build_clause_universe(
        predicate_columns,
        settings,
        predicate_channel_groups=predicate_channel_groups,
        predicate_semantic_groups=predicate_semantic_groups,
        predicate_clock_groups=predicate_clock_groups,
    )
    if not universe.clauses:
        raise NestedOofContractError("candidate universe has no clauses")
    dnf_bodies = _rule_bodies(universe, settings)
    if not dnf_bodies:
        raise NestedOofContractError("candidate universe has no rule bodies")
    actions = duration_vocabulary(side)[1:]
    policies: list[BooleanCooldownPolicy] = []
    seen: set[str] = set()
    action_counts = {action: 0 for action in actions}
    structure_counts = {"literal": 0, "and": 0, "or": 0, "ordered": 0}
    channel_counts = {descriptor.channel_group: 0 for descriptor in universe.selected}

    def add_policy(policy: BooleanCooldownPolicy) -> bool:
        if len(policies) >= settings.max_policy_candidates:
            return False
        if policy.candidate_id in seen:
            return False
        _validate_policy_clock_groups(policy, predicate_clock_groups)
        policies.append(policy)
        seen.add(policy.candidate_id)
        if len(policy.rules) > 1:
            structure_counts["ordered"] += 1
        for rule in policy.rules:
            action_counts[rule.action] += 1
            structure_counts[_body_structure(rule.clauses)] += 1
            for channel in _body_channels(rule.clauses, universe.descriptor_by_predicate):
                channel_counts[channel] += 1
        return True

    atomic_bodies = tuple((clause,) for clause in universe.atomic_clauses)
    required_atomic = max(len(atomic_bodies), len(actions))
    for index in range(required_atomic):
        body = atomic_bodies[index % len(atomic_bodies)]
        action = actions[index % len(actions)]
        add_policy(
            BooleanCooldownPolicy(
                side=side,
                rules=(BooleanRule(action=action, clauses=body),),
            )
        )

    def least_used_action(*, exclude: str | None = None) -> str:
        eligible = [action for action in actions if action != exclude]
        return min(
            eligible,
            key=lambda action: (action_counts[action], actions.index(action)),
        )

    for structure in ("and", "or"):
        if structure not in universe.structures:
            continue
        body = next(
            (value for value in dnf_bodies if _body_structure(value) == structure),
            None,
        )
        if body is None:
            continue
        add_policy(
            BooleanCooldownPolicy(
                side=side,
                rules=(BooleanRule(action=least_used_action(), clauses=body),),
            )
        )

    if "ordered" in universe.structures and len(atomic_bodies) >= 2:
        first_action = least_used_action()
        second_action = least_used_action(exclude=first_action)
        add_policy(
            BooleanCooldownPolicy(
                side=side,
                rules=(
                    BooleanRule(action=first_action, clauses=atomic_bodies[0]),
                    BooleanRule(action=second_action, clauses=atomic_bodies[1]),
                ),
            )
        )

    single_options = [
        (body, action)
        for body in dnf_bodies
        for action in actions
        if BooleanCooldownPolicy(
            side=side,
            rules=(BooleanRule(action=action, clauses=body),),
        ).candidate_id
        not in seen
    ]
    while single_options and len(policies) < settings.max_policy_candidates:
        body, action = min(
            single_options,
            key=lambda value: (
                action_counts[value[1]],
                max(
                    channel_counts[channel]
                    for channel in _body_channels(value[0], universe.descriptor_by_predicate)
                ),
                structure_counts[_body_structure(value[0])],
                _stable_search_key("policy", _body_key(value[0]), value[1]),
            ),
        )
        single_options.remove((body, action))
        add_policy(
            BooleanCooldownPolicy(
                side=side,
                rules=(BooleanRule(action=action, clauses=body),),
            )
        )
    if not policies:
        raise NestedOofContractError("bounded search generated no nonbaseline policy")
    if set(action_counts) != {rule.action for policy in policies for rule in policy.rules}:
        raise NestedOofContractError("duration action coverage drifted")
    for descriptor in universe.selected:
        polarities = {
            literal.negated
            for policy in policies
            for rule in policy.rules
            for clause in rule.clauses
            for literal in clause.literals
            if literal.predicate == descriptor.predicate
        }
        if polarities != {False, True}:
            raise NestedOofContractError(
                f"literal polarity coverage drifted for {descriptor.predicate!r}"
            )
    if len(policies) > settings.max_policy_candidates:
        raise NestedOofContractError("policy universe exceeded its hard budget")
    return tuple(policies)


def _campaign_weights(rows: pd.DataFrame) -> pd.Series:
    counts = rows.groupby("campaign_cluster_id", observed=True)["opportunity_id"].transform("count")
    weights = 1.0 / counts.astype(float)
    check = weights.groupby(rows["campaign_cluster_id"], observed=True).sum()
    if not np.allclose(check.to_numpy(dtype=float), 1.0, atol=1e-12, rtol=0.0):
        raise NestedOofContractError("campaign total training weight drifted from one")
    return weights


def _evaluate_policy(
    panel: PreparedPanel,
    policy: BooleanCooldownPolicy,
    *,
    days: Sequence[str],
    fold_id: str,
    stage: str,
) -> pd.DataFrame:
    schema = panel.columns
    selected_days = {_normalize_day(day) for day in days}
    features = panel.features.loc[panel.features[schema.day].isin(selected_days)].copy()
    if features.empty:
        raise NestedOofContractError(f"{stage} fold {fold_id} has no opportunities")
    actions = policy.choose(features.loc[:, panel.predicate_columns])
    outcomes = panel.outcomes.loc[features.index]
    chosen = np.fromiter(
        (
            float(outcomes.at[index, action])
            for index, action in zip(features.index, actions, strict=True)
        ),
        dtype=float,
        count=len(features),
    )
    control = outcomes[CONTROL_ACTION].to_numpy(dtype=float, copy=False)
    rows = pd.DataFrame(
        {
            "opportunity_id": features.index.astype(str),
            "utc_day": features[schema.day].to_numpy(dtype=object),
            "panel_role": features[schema.panel_role].to_numpy(dtype=object),
            "side": panel.side,
            "role_at_fill": features[schema.role].to_numpy(dtype=object),
            "campaign_cluster_id": features[schema.campaign].to_numpy(dtype=object),
            "selected_action": actions,
            "control_action": CONTROL_ACTION,
            "selected_nonbaseline": actions != CONTROL_ACTION,
            "selected_value_usdc": chosen,
            "control_value_usdc": control,
            "uplift_usdc": chosen - control,
            "candidate_id": policy.candidate_id,
            "fold_id": fold_id,
            "evaluation_stage": stage,
        }
    )
    rows["campaign_weight"] = _campaign_weights(rows)
    return rows


def _support(rows: pd.DataFrame, config: SearchConfig) -> SupportAudit:
    acted = rows.loc[rows["selected_nonbaseline"]]
    opportunities = int(len(acted))
    campaigns = int(acted["campaign_cluster_id"].nunique())
    days = int(acted["utc_day"].nunique())
    return SupportAudit(
        action_opportunities=opportunities,
        action_campaigns=campaigns,
        action_days=days,
        action_rate=float(opportunities / len(rows)) if len(rows) else 0.0,
        passed=bool(
            opportunities >= config.minimum_action_opportunities
            and campaigns >= config.minimum_action_campaigns
            and days >= config.minimum_action_days
        ),
    )


def clustered_estimate(rows: pd.DataFrame, *, confidence: float = 0.95) -> ClusteredEstimate:
    required = {
        "uplift_usdc",
        "campaign_weight",
        "campaign_cluster_id",
        "utc_day",
        "selected_nonbaseline",
    }
    if required - set(rows):
        raise NestedOofContractError("OOF rows lack clustering/value metadata")
    if rows.empty:
        raise NestedOofContractError("cannot estimate an empty OOF panel")
    if not 0.5 < confidence < 1.0:
        raise NestedOofContractError("confidence must be between 0.5 and 1")
    values = rows["uplift_usdc"].to_numpy(dtype=float, copy=False)
    weights = rows["campaign_weight"].to_numpy(dtype=float, copy=False)
    if not np.isfinite(values).all() or not np.isfinite(weights).all():
        raise NestedOofContractError("OOF values/weights must be finite")
    total_weight = float(weights.sum())
    if total_weight <= 0.0:
        raise NestedOofContractError("OOF campaign weight must be positive")
    mean = float(np.dot(weights, values) / total_weight)
    influence = weights * (values - mean)
    cluster_sum = (
        pd.Series(influence).groupby(rows["utc_day"].reset_index(drop=True), observed=True).sum()
    )
    day_count = int(len(cluster_sum))
    if day_count < 2:
        standard_error = math.inf
    else:
        variance = (
            day_count
            / (day_count - 1.0)
            * float(np.square(cluster_sum.to_numpy(dtype=float)).sum())
            / (total_weight * total_weight)
        )
        standard_error = math.sqrt(max(0.0, variance))
    critical = NormalDist().inv_cdf(0.5 + confidence / 2.0)
    margin = critical * standard_error
    return ClusteredEstimate(
        mean_usdc=mean,
        standard_error_usdc=standard_error,
        lcb_usdc=mean - margin,
        ucb_usdc=mean + margin,
        confidence=confidence,
        opportunities=int(len(rows)),
        campaigns=int(rows["campaign_cluster_id"].nunique()),
        days=day_count,
        action_rate=float(rows["selected_nonbaseline"].mean()),
    )


def _reweight_combined_rows(rows: pd.DataFrame) -> pd.DataFrame:
    output = rows.copy()
    output["campaign_weight"] = _campaign_weights(output)
    return output


def _validate_folds(panel: PreparedPanel, folds: Sequence[ChronologicalFold]) -> None:
    if not folds:
        raise NestedOofContractError("outer OOF needs frozen folds")
    available = set(panel.features[panel.columns.day])
    seen_test: set[str] = set()
    fold_ids: set[str] = set()
    for fold in folds:
        if fold.fold_id in fold_ids:
            raise NestedOofContractError("outer fold identity is duplicated")
        fold_ids.add(fold.fold_id)
        if (set(fold.train_days) | set(fold.test_days)) - available:
            raise NestedOofContractError("outer fold references a day outside its panel")
        if seen_test & set(fold.test_days):
            raise NestedOofContractError("outer test days overlap")
        seen_test.update(fold.test_days)


def _inner_oof_rows(
    panel: PreparedPanel,
    *,
    policy: BooleanCooldownPolicy,
    outer_fold: ChronologicalFold,
    config: SearchConfig,
) -> pd.DataFrame:
    inner = expanding_chronological_folds(
        outer_fold.train_days,
        fold_prefix=f"{outer_fold.fold_id}.inner",
        n_folds=config.inner_folds,
        minimum_train_days=config.inner_minimum_train_days,
    )
    frames = [
        _evaluate_policy(
            panel,
            policy,
            days=fold.test_days,
            fold_id=fold.fold_id,
            stage="inner_oof",
        )
        for fold in inner
    ]
    rows = pd.concat(frames, ignore_index=True)
    if rows["opportunity_id"].duplicated().any():
        raise NestedOofContractError("inner OOF opportunity appears more than once")
    return _reweight_combined_rows(rows)


def _select_exploratory_candidate(
    panel: PreparedPanel,
    *,
    candidates: Sequence[BooleanCooldownPolicy],
    outer_fold: ChronologicalFold,
    config: SearchConfig,
) -> tuple[BooleanCooldownPolicy, ClusteredEstimate, SupportAudit]:
    ranked: list[
        tuple[
            float,
            tuple[int, int, int],
            str,
            BooleanCooldownPolicy,
            ClusteredEstimate,
            SupportAudit,
        ]
    ] = []
    for policy in candidates:
        rows = _inner_oof_rows(
            panel,
            policy=policy,
            outer_fold=outer_fold,
            config=config,
        )
        support = _support(rows, config)
        if not support.passed:
            continue
        estimate = clustered_estimate(rows, confidence=config.confidence)
        # Selection is point-estimate exploration. A negative or zero LCB is
        # deliberately not an abstention criterion at this stage.
        ranked.append(
            (
                -estimate.mean_usdc,
                policy.complexity,
                policy.candidate_id,
                policy,
                estimate,
                support,
            )
        )
    if not ranked:
        raise NestedOofContractError("no nonbaseline exploratory candidate satisfies inner support")
    ranked.sort(key=lambda row: (row[0], row[1], row[2]))
    _, _, _, policy, estimate, support = ranked[0]
    return policy, estimate, support


def _distribution_tail(values: np.ndarray) -> dict[str, Any]:
    finite = np.asarray(values, dtype=float)
    if finite.size == 0:
        return {
            "count": 0,
            "q10_usdc": None,
            "cvar10_usdc": None,
            "minimum_usdc": None,
            "negative_fraction": None,
        }
    if not np.isfinite(finite).all():
        raise NestedOofContractError("role tail values must be finite")
    q10 = float(np.quantile(finite, 0.10))
    lower_tail = finite[finite <= q10]
    return {
        "count": int(finite.size),
        "q10_usdc": q10,
        "cvar10_usdc": float(lower_tail.mean()),
        "minimum_usdc": float(finite.min()),
        "negative_fraction": float(np.mean(finite < 0.0)),
    }


def _role_tail_diagnostics(role_rows: pd.DataFrame) -> dict[str, Any]:
    required = {
        "utc_day",
        "campaign_cluster_id",
        "campaign_weight",
        "selected_value_usdc",
        "control_value_usdc",
        "uplift_usdc",
    }
    if required - set(role_rows):
        raise NestedOofContractError("role rows lack tail-diagnostic metadata")
    weighted = role_rows.assign(
        _selected_weighted=(role_rows["campaign_weight"] * role_rows["selected_value_usdc"]),
        _control_weighted=(role_rows["campaign_weight"] * role_rows["control_value_usdc"]),
        _uplift_weighted=(role_rows["campaign_weight"] * role_rows["uplift_usdc"]),
    )
    campaign = (
        weighted.groupby(
            ["utc_day", "campaign_cluster_id"],
            observed=True,
            sort=False,
        )
        .agg(
            selected_value_usdc=("_selected_weighted", "sum"),
            control_value_usdc=("_control_weighted", "sum"),
            uplift_usdc=("_uplift_weighted", "sum"),
        )
        .reset_index()
    )
    day = (
        campaign.groupby("utc_day", observed=True, sort=False)[
            ["selected_value_usdc", "control_value_usdc", "uplift_usdc"]
        ]
        .mean()
        .reset_index()
    )

    def summarize(frame: pd.DataFrame) -> dict[str, Any]:
        selected = _distribution_tail(
            frame["selected_value_usdc"].to_numpy(dtype=float, copy=False)
        )
        control = _distribution_tail(frame["control_value_usdc"].to_numpy(dtype=float, copy=False))
        uplift = _distribution_tail(frame["uplift_usdc"].to_numpy(dtype=float, copy=False))
        return {
            "selected": selected,
            "control": control,
            "uplift": uplift,
            "selected_minus_control_q10_usdc": float(selected["q10_usdc"] - control["q10_usdc"]),
            "selected_minus_control_cvar10_usdc": float(
                selected["cvar10_usdc"] - control["cvar10_usdc"]
            ),
        }

    return {
        "campaign": summarize(campaign),
        "utc_day": summarize(day),
        "campaign_value_contract": ("mean_opportunity_value_within_role_and_campaign"),
        "day_value_contract": "mean_role_campaign_value_within_utc_day",
    }


def role_support_audit(
    rows: pd.DataFrame, *, confidence: float
) -> dict[str, dict[str, Any]]:
    if "role_at_fill" not in rows:
        raise NestedOofContractError("OOF rows are missing required role_at_fill")
    observed_roles = set(rows["role_at_fill"].dropna().astype(str).str.lower())
    if observed_roles - {"opener", "add"}:
        raise NestedOofContractError("OOF rows contain a forbidden role")
    output: dict[str, dict[str, Any]] = {}
    for role in ("opener", "add"):
        subset = rows.loc[rows["role_at_fill"] == role]
        acted = subset.loc[subset["selected_nonbaseline"]]
        if subset.empty:
            output[role] = {
                "policy_scope": "one_shared_policy_per_side_roles_are_audit_only",
                "opportunities": 0,
                "campaigns": 0,
                "days": 0,
                "action_opportunities": 0,
                "action_campaigns": 0,
                "action_days": 0,
                "action_rate": 0.0,
                "campaign_weighted_mean_uplift_usdc": None,
                "campaign_day_clustered_uplift_interval": None,
                "tail_diagnostics": None,
            }
            continue
        role_rows = _reweight_combined_rows(subset)
        estimate = clustered_estimate(role_rows, confidence=confidence)
        output[role] = {
            "policy_scope": "one_shared_policy_per_side_roles_are_audit_only",
            "opportunities": int(len(subset)),
            "campaigns": int(subset["campaign_cluster_id"].nunique()),
            "days": int(subset["utc_day"].nunique()),
            "action_opportunities": int(len(acted)),
            "action_campaigns": int(acted["campaign_cluster_id"].nunique()),
            "action_days": int(acted["utc_day"].nunique()),
            "action_rate": float(len(acted) / len(subset)),
            "campaign_weighted_mean_uplift_usdc": estimate.mean_usdc,
            "campaign_day_clustered_uplift_interval": asdict(estimate),
            "tail_diagnostics": _role_tail_diagnostics(role_rows),
        }
    return output


def run_nested_chronological_oof(
    table: pd.DataFrame,
    *,
    side: str,
    feature_block: str,
    panel_scope: str,
    predicate_columns: Sequence[str],
    outer_folds: Sequence[ChronologicalFold],
    search_config: SearchConfig | None = None,
    candidate_policies: Sequence[BooleanCooldownPolicy] | None = None,
    columns: LongFormColumns | None = None,
    predicate_channel_groups: Mapping[str, str] | None = None,
    predicate_semantic_groups: Mapping[str, str] | None = None,
    predicate_clock_groups: Mapping[str, str] | None = None,
) -> NestedOofResult:
    """Select in inner folds and execute the frozen candidate in outer OOF."""

    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise NestedOofContractError("pooled-side policies are forbidden")
    if feature_block not in FEATURE_BLOCKS:
        raise NestedOofContractError("feature_block must be R0/M0/M1/M2")
    config = search_config or SearchConfig()
    panel = prepare_long_form_panel(
        table,
        side=normalized_side,
        panel_scope=panel_scope,
        predicate_columns=predicate_columns,
        columns=columns,
    )
    folds = tuple(outer_folds)
    _validate_folds(panel, folds)
    if candidate_policies is None:
        candidates = generate_bounded_candidates(
            side=normalized_side,
            predicate_columns=panel.predicate_columns,
            config=config,
            predicate_channel_groups=predicate_channel_groups,
            predicate_semantic_groups=predicate_semantic_groups,
            predicate_clock_groups=predicate_clock_groups,
        )
    else:
        candidates = tuple(candidate_policies)
    if not candidates:
        raise NestedOofContractError("candidate set cannot contain baseline only")
    if any(policy.side != normalized_side or not policy.rules for policy in candidates):
        raise NestedOofContractError("candidate set pools sides or contains baseline")
    if any(set(policy.predicate_columns) - set(panel.predicate_columns) for policy in candidates):
        raise NestedOofContractError("candidate uses a predicate outside the feature block")
    for policy in candidates:
        _validate_policy_clock_groups(policy, predicate_clock_groups)
    executions: list[OuterFoldExecution] = []
    for fold in folds:
        policy, inner_estimate, inner_support = _select_exploratory_candidate(
            panel,
            candidates=candidates,
            outer_fold=fold,
            config=config,
        )
        outer_rows = _evaluate_policy(
            panel,
            policy,
            days=fold.test_days,
            fold_id=fold.fold_id,
            stage="outer_oof",
        )
        outer_support = _support(outer_rows, config)
        if outer_rows["candidate_id"].nunique() != 1 or (
            outer_rows["candidate_id"].iloc[0] != policy.candidate_id
        ):
            raise NestedOofContractError("inner-frozen candidate changed before outer OOF")
        executions.append(
            OuterFoldExecution(
                fold_id=fold.fold_id,
                train_days=fold.train_days,
                test_days=fold.test_days,
                selected_policy=policy,
                inner_estimate=inner_estimate,
                inner_support=inner_support,
                outer_support=outer_support,
                candidate_was_replaced_by_baseline=False,
                oof_rows=outer_rows,
            )
        )
    oof_rows = pd.concat([execution.oof_rows for execution in executions], ignore_index=True)
    if oof_rows["opportunity_id"].duplicated().any():
        raise NestedOofContractError("outer OOF opportunity appears more than once")
    oof_rows = _reweight_combined_rows(oof_rows)
    combined_support = _support(oof_rows, config)
    evidence_role = {
        "prefix40": "development_prefix40",
        "added10": "late_diagnostic_added10_not_validation_or_holdout",
        "pooled50": "development_prefix40_plus_late_diagnostic_added10",
    }[panel.panel_scope]
    return NestedOofResult(
        side=normalized_side,
        feature_block=feature_block,
        panel_scope=panel.panel_scope,
        folds=tuple(executions),
        oof_rows=oof_rows,
        estimate=clustered_estimate(oof_rows, confidence=config.confidence),
        combined_support=combined_support,
        role_support=role_support_audit(oof_rows, confidence=config.confidence),
        panel_role_counts=panel.panel_role_counts,
        permissions={
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        evidence_role=evidence_role,
    )


def evaluate_post_oof_deployment_gate(
    result: NestedOofResult,
    *,
    economic_epsilon_usdc: float = 0.0,
    minimum_action_rate: float = 0.0,
    minimum_campaigns: int = 2,
    minimum_days: int = 2,
    require_both_roles: bool = True,
    required_roles: Sequence[str] | None = None,
    minimum_role_opportunities: int = 1,
    minimum_role_action_opportunities: int = 1,
    minimum_role_action_rate: float = 0.0,
    minimum_role_action_campaigns: int | None = None,
    minimum_role_action_days: int | None = None,
    role_severe_harm_tolerance_usdc: float = 0.0,
    role_tail_harm_tolerance_usdc: float = 0.0,
) -> DeploymentGateResult:
    """Evaluate evidence after OOF without modifying or authorizing a policy."""

    if result.side not in {"BUY", "SELL"}:
        raise NestedOofContractError("pooled-side deployment gates are forbidden")
    if not math.isfinite(economic_epsilon_usdc):
        raise NestedOofContractError("economic epsilon must be finite")
    if not math.isfinite(role_severe_harm_tolerance_usdc):
        raise NestedOofContractError("role harm tolerance must be finite")
    if role_severe_harm_tolerance_usdc < 0.0:
        raise NestedOofContractError("role harm tolerance cannot be negative")
    if not math.isfinite(role_tail_harm_tolerance_usdc) or role_tail_harm_tolerance_usdc < 0.0:
        raise NestedOofContractError("role tail harm tolerance must be finite and nonnegative")
    for name, value in (
        ("minimum_action_rate", minimum_action_rate),
        ("minimum_role_action_rate", minimum_role_action_rate),
    ):
        if not 0.0 <= value <= 1.0:
            raise NestedOofContractError(f"{name} must be in [0, 1]")
    role_campaign_minimum = (
        minimum_campaigns
        if minimum_role_action_campaigns is None
        else minimum_role_action_campaigns
    )
    role_day_minimum = (
        minimum_days if minimum_role_action_days is None else minimum_role_action_days
    )
    integer_minimums = {
        "minimum_campaigns": minimum_campaigns,
        "minimum_days": minimum_days,
        "minimum_role_opportunities": minimum_role_opportunities,
        "minimum_role_action_opportunities": minimum_role_action_opportunities,
        "minimum_role_action_campaigns": role_campaign_minimum,
        "minimum_role_action_days": role_day_minimum,
    }
    if any(value < 1 for value in integer_minimums.values()):
        raise NestedOofContractError("deployment support minimums must be positive")
    if required_roles is not None and not require_both_roles:
        raise NestedOofContractError("required_roles conflicts with require_both_roles=False")
    if required_roles is None:
        promotion_roles = ("opener", "add") if require_both_roles else ()
    else:
        promotion_roles = tuple(str(role).strip().lower() for role in required_roles)
        if (
            not promotion_roles
            or len(set(promotion_roles)) != len(promotion_roles)
            or set(promotion_roles) - {"opener", "add"}
        ):
            raise NestedOofContractError(
                "required_roles must be a nonempty unique subset of opener/add"
            )
    estimate = result.estimate
    combined_support = result.combined_support
    outer_fold_support = {
        execution.fold_id: execution.outer_support for execution in result.folds
    }
    zero_action_outer_folds = tuple(
        fold_id
        for fold_id, audit in outer_fold_support.items()
        if audit.action_opportunities == 0
    )
    reasons: list[str] = []
    if zero_action_outer_folds:
        reasons.append("outer_fold_without_nonbaseline_action")
    if not estimate.lcb_usdc > economic_epsilon_usdc:
        reasons.append("terminal_value_lcb_not_above_economic_epsilon")
    if combined_support.action_rate < minimum_action_rate:
        reasons.append("action_rate_below_support")
    if combined_support.action_campaigns < minimum_campaigns:
        reasons.append("campaign_support_below_minimum")
    if combined_support.action_days < minimum_days:
        reasons.append("day_support_below_minimum")
    role_gates: dict[str, dict[str, Any]] = {}
    for role in promotion_roles:
        audit = result.role_support.get(role)
        if not isinstance(audit, Mapping):
            raise NestedOofContractError(f"required role audit is missing: {role}")
        interval = audit.get("campaign_day_clustered_uplift_interval")
        interval_available = isinstance(interval, Mapping) and "ucb_usdc" in interval
        tail = audit.get("tail_diagnostics")
        campaign_tail = tail.get("campaign") if isinstance(tail, Mapping) else None
        tail_available = isinstance(campaign_tail, Mapping) and {
            "selected_minus_control_q10_usdc",
            "selected_minus_control_cvar10_usdc",
        } <= set(campaign_tail)
        checks = {
            "opportunity_support": (
                int(audit.get("opportunities", 0)) >= minimum_role_opportunities
            ),
            "action_opportunity_support": (
                int(audit.get("action_opportunities", 0)) >= minimum_role_action_opportunities
            ),
            "action_rate_support": (
                float(audit.get("action_rate", 0.0)) >= minimum_role_action_rate
            ),
            "action_campaign_support": (
                int(audit.get("action_campaigns", 0)) >= role_campaign_minimum
            ),
            "action_day_support": (int(audit.get("action_days", 0)) >= role_day_minimum),
            "uplift_interval_available": interval_available,
            "tail_diagnostics_available": tail_available,
        }
        if interval_available:
            checks["no_severe_negative_uplift"] = (
                float(interval["ucb_usdc"]) >= -role_severe_harm_tolerance_usdc
            )
        if tail_available:
            checks["campaign_q10_noninferior"] = (
                float(campaign_tail["selected_minus_control_q10_usdc"])
                >= -role_tail_harm_tolerance_usdc
            )
            checks["campaign_cvar10_noninferior"] = (
                float(campaign_tail["selected_minus_control_cvar10_usdc"])
                >= -role_tail_harm_tolerance_usdc
            )
        role_gates[role] = {
            **checks,
            "passed": bool(all(checks.values())),
            "severe_harm_definition": (
                "campaign_weighted_day_clustered_uplift_ucb_below_negative_tolerance"
            ),
        }
        reason_by_check = {
            "opportunity_support": f"{role}_opportunity_support_below_minimum",
            "action_opportunity_support": (f"{role}_action_opportunity_support_below_minimum"),
            "action_rate_support": f"{role}_action_rate_below_support",
            "action_campaign_support": (f"{role}_action_campaign_support_below_minimum"),
            "action_day_support": f"{role}_action_day_support_below_minimum",
            "uplift_interval_available": f"{role}_uplift_interval_missing",
            "tail_diagnostics_available": f"{role}_tail_diagnostics_missing",
            "no_severe_negative_uplift": (f"{role}_severe_negative_uplift_interval"),
            "campaign_q10_noninferior": f"{role}_campaign_q10_worsened",
            "campaign_cvar10_noninferior": f"{role}_campaign_cvar10_worsened",
        }
        reasons.extend(reason_by_check[name] for name, passed in checks.items() if not passed)
    passed = not reasons
    return DeploymentGateResult(
        passed=passed,
        decision="research_evidence_supported" if passed else "abstain",
        reasons=tuple(reasons),
        estimate=estimate,
        combined_support=combined_support,
        outer_fold_support=outer_fold_support,
        zero_action_outer_folds=zero_action_outer_folds,
        required_roles=promotion_roles,
        role_gates=role_gates,
        # Passing this evidence gate is intentionally not action/live authority.
        action_authorized=False,
        live_authorized=False,
    )


def run_feature_block_comparison(
    table: pd.DataFrame,
    *,
    panel_scope: str,
    predicate_columns_by_block: Mapping[str, Sequence[str]],
    outer_folds: Sequence[ChronologicalFold],
    search_config: SearchConfig | None = None,
    columns: LongFormColumns | None = None,
    predicate_channel_groups: Mapping[str, str] | None = None,
    predicate_semantic_groups: Mapping[str, str] | None = None,
    predicate_clock_groups: Mapping[str, str] | None = None,
) -> FeatureBlockComparison:
    """Run R0/M0/M1/M2 separately for BUY and SELL; never pool sides."""

    if set(predicate_columns_by_block) != set(FEATURE_BLOCKS):
        raise NestedOofContractError("feature comparison must provide exactly R0/M0/M1/M2 blocks")
    m0 = set(predicate_columns_by_block["M0"])
    m1 = set(predicate_columns_by_block["M1"])
    m2 = set(predicate_columns_by_block["M2"])
    if not m0 <= m1 <= m2:
        raise NestedOofContractError("M0/M1/M2 predicate blocks must be cumulative")
    results: dict[str, dict[str, NestedOofResult]] = {}
    for side in ("BUY", "SELL"):
        results[side] = {}
        for block in FEATURE_BLOCKS:
            results[side][block] = run_nested_chronological_oof(
                table,
                side=side,
                feature_block=block,
                panel_scope=panel_scope,
                predicate_columns=predicate_columns_by_block[block],
                outer_folds=outer_folds,
                search_config=search_config,
                columns=columns,
                predicate_channel_groups=predicate_channel_groups,
                predicate_semantic_groups=predicate_semantic_groups,
                predicate_clock_groups=predicate_clock_groups,
            )
    return FeatureBlockComparison(panel_scope=panel_scope, results=results)


__all__ = [
    "AndClause",
    "BooleanCooldownPolicy",
    "BooleanRule",
    "ChronologicalFold",
    "ClusteredEstimate",
    "CONTROL_ACTION",
    "DeploymentGateResult",
    "FEATURE_BLOCKS",
    "FeatureBlockComparison",
    "IDENTITY",
    "LEARNER_IDENTITY",
    "LongFormColumns",
    "NestedOofContractError",
    "NestedOofResult",
    "OuterFoldExecution",
    "REPORT_PANEL_SCOPES",
    "SOURCE_PANEL_ROLES",
    "SearchConfig",
    "SupportAudit",
    "TriLiteral",
    "clustered_estimate",
    "duration_vocabulary",
    "evaluate_post_oof_deployment_gate",
    "expanding_chronological_folds",
    "generate_bounded_candidates",
    "prepare_long_form_panel",
    "role_support_audit",
    "run_feature_block_comparison",
    "run_nested_chronological_oof",
]
