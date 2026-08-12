"""Pure-model Boolean cooldown-duration learner for the frozen v1 study.

This module intentionally has no replay, order-routing, registry, or live
integration.  It consumes a complete joint-washout opportunity-by-action panel
and learns side-specific, serializable sparse Boolean rule lists.  Every search
limit is read from the frozen Spec; callers cannot replace those limits with a
more convenient post-outcome grid.
"""

from __future__ import annotations

import hashlib
import heapq
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

IDENTITY = "multiscale_ema_boolean_cooldown_duration_policy_v1"
EXPLORATORY_IDENTITY = "multiscale_ema_boolean_cooldown_duration_policy_exploratory_oof_v1"
CONTROL_ACTION = "CONTROL_85N"
DEPLOYMENT_LCB_SELECTION = "deployment_lcb_screened"
EXPLORATORY_NONBASELINE_SELECTION = "exploratory_nonbaseline_outer_oof"
FORMAL_CONFIDENCE = 0.95
FORMAL_BOOTSTRAP_SAMPLES = 500
SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "multiscale_ema_boolean_cooldown_duration_policy_v1_spec_20260809.json"
)
OUTCOME_VALUE_COLUMN = "assignment_to_washout_value_usdc"
COMMON_COLUMNS = (
    "opportunity_id",
    "side",
    "utc_day",
    "campaign_side_id",
    "assignment_ts_ns",
    "washout_ts_ns",
    "joint_censored",
)
_CROSS_FAVORABLE_SUFFIX = ":last_cross_favorable"
_CROSS_OBSERVED_PREFIX = "__validity__::"


def _searchable_predicate_columns(predicates: pd.DataFrame) -> tuple[str, ...]:
    return tuple(column for column in predicates.columns if column.startswith("predicate::"))


def _cross_observed_column(predicate: str) -> str | None:
    if not predicate.endswith(_CROSS_FAVORABLE_SUFFIX):
        return None
    stem = predicate.removeprefix("predicate::").removesuffix(_CROSS_FAVORABLE_SUFFIX)
    return f"{_CROSS_OBSERVED_PREFIX}{stem}:cross_observed"


def _raw_cross_missing_column(predicate: str) -> str | None:
    observed = _cross_observed_column(predicate)
    if observed is None:
        return None
    stem = predicate.removeprefix("predicate::").removesuffix(_CROSS_FAVORABLE_SUFFIX)
    return f"{stem}_cross_missing"


def _canonical_json(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _frozen_source(path: Path, *, role: str) -> tuple[Path, str]:
    """Resolve a public projection without replacing its frozen source hash."""

    try:
        identity = source_identity_sha256(path)
        source = source_document_path(path, require_private=False)
    except (OSError, PublicMachineProjectionError) as exc:
        raise ValueError(f"{role} source identity is unavailable") from exc
    return source, identity


@dataclass(frozen=True)
class FrozenOuterFold:
    """One exact outer fold read from the frozen chronological source."""

    fold: int
    history_days: tuple[str, ...]
    embargo_day: str
    fit_day_candidates: tuple[str, ...]
    test_days: tuple[str, ...]


@dataclass(frozen=True)
class SearchContract:
    """Search and chronology limits loaded verbatim from the frozen Spec."""

    spec_path: str
    spec_sha256: str
    outcome_blind_path: str
    outcome_blind_sha256: str
    predicate_artifact_sha256: str
    predicate_columns: tuple[str, ...]
    predicate_schema_sha256: str
    ordered_development_days: tuple[str, ...]
    expected_opportunities: int
    expected_arm_rows: int
    outer_fold_source_path: str
    outer_fold_source_sha256: str
    frozen_outer_folds: tuple[FrozenOuterFold, ...]
    side_actions: Mapping[str, tuple[str, ...]]
    max_literals_per_clause: tuple[int, ...]
    max_clauses: tuple[int, ...]
    minimum_days: int
    minimum_campaigns: int
    minimum_campaign_weight_fraction: float
    beam_width: int
    seed: int
    outer_initial_history_days: int
    outer_test_days: int
    outer_fold_count: int
    outer_embargo_calendar_days: int
    inner_fold_count: int
    inner_test_days: int
    formal_confidence: float
    formal_bootstrap_samples: int


@dataclass(frozen=True)
class FormalInputIdentity:
    """Observed full-panel identity carried into each side-specific fit."""

    ordered_utc_days: tuple[str, ...]
    opportunity_count: int
    arm_row_count: int
    predicate_schema_sha256: str
    outer_fold_source_sha256: str
    spec_sha256: str
    outcome_blind_sha256: str

    def artifact(self) -> dict[str, Any]:
        body = {
            "schema_version": f"{IDENTITY}.formal_input_identity.v1",
            "ordered_utc_days": list(self.ordered_utc_days),
            "opportunity_count": self.opportunity_count,
            "arm_row_count": self.arm_row_count,
            "predicate_schema_sha256": self.predicate_schema_sha256,
            "outer_fold_source_sha256": self.outer_fold_source_sha256,
            "spec_sha256": self.spec_sha256,
            "outcome_blind_sha256": self.outcome_blind_sha256,
        }
        return {**body, "formal_input_identity_sha256": _sha256(body)}


def _resolve_frozen_path(path_text: str, *, spec_path: Path) -> Path:
    path = resolve_portable_path(path_text, root=spec_path.parents[4])
    if path.is_absolute():
        return path
    return spec_path.parents[4] / path


def load_frozen_search_contract(path: Path = SPEC_PATH) -> SearchContract:
    """Read and validate the immutable model-search section of the Spec."""

    source_spec_path, observed_spec_sha = _frozen_source(path, role="frozen Spec")
    payload = json.loads(source_spec_path.read_text(encoding="utf-8"))
    if payload.get("identity") != IDENTITY:
        raise ValueError("frozen Spec identity mismatch")
    search = payload["boolean_policy_search"]
    chronology = payload["nested_chronological_development"]
    duration = payload["outcome_blind_duration_artifact"]
    denominator = payload["development_denominator"]
    if search.get("policy_form") != "ordered sparse Boolean rule list":
        raise ValueError("frozen Spec no longer authorizes a Boolean rule list")
    if search.get("linear_additive_score_or_majority_vote") != "forbidden":
        raise ValueError("linear or vote proxy must remain forbidden")
    if denominator.get("missing_fork_policy", "").startswith("fail closed") is False:
        raise ValueError("missing-fork fail-closed contract is absent")
    actions = {side: tuple(values) for side, values in duration["candidate_actions"].items()}
    if set(actions) != {"BUY", "SELL"}:
        raise ValueError("the frozen action set must be side specific")
    if any(len(values) != 8 or CONTROL_ACTION not in values for values in actions.values()):
        raise ValueError("each side must retain all eight actions including CONTROL_85N")
    outcome_blind_path = _resolve_frozen_path(duration["path"], spec_path=path)
    outcome_blind_source_path, observed_outcome_blind_sha = _frozen_source(
        outcome_blind_path, role="outcome-blind artifact"
    )
    if observed_outcome_blind_sha != duration["sha256"]:
        raise ValueError("outcome-blind artifact SHA256 mismatch")
    outcome_blind = json.loads(outcome_blind_source_path.read_text(encoding="utf-8"))
    if outcome_blind.get("identity") != IDENTITY:
        raise ValueError("outcome-blind artifact identity mismatch")
    predicate_columns = tuple(
        f"predicate::{row['name']}" for row in outcome_blind["atomic_predicates"]
    )
    expected_predicate_count = int(
        payload["ema_state_contract"]["atomic_predicate_source"]["predicate_count"]
    )
    if (
        len(predicate_columns) != expected_predicate_count
        or len(set(predicate_columns)) != expected_predicate_count
    ):
        raise ValueError("frozen predicate names are not 360 unique ordered columns")
    ordered_days = tuple(outcome_blind["baseline_projection"]["ordered_utc_days"])
    if len(ordered_days) != 40 or tuple(sorted(ordered_days)) != ordered_days:
        raise ValueError("outcome-blind artifact must bind 40 ordered Development days")

    outer_source_path = _resolve_frozen_path(chronology["outer_fold_source_path"], spec_path=path)
    outer_source_document, observed_outer_sha = _frozen_source(
        outer_source_path, role="frozen outer-fold source"
    )
    if observed_outer_sha != chronology["outer_fold_source_sha256"]:
        raise ValueError("frozen outer-fold source SHA256 mismatch")
    outer_source = json.loads(outer_source_document.read_text(encoding="utf-8"))
    fold_payload = outer_source[chronology["outer_fold_field"]]
    frozen_outer_folds = tuple(
        FrozenOuterFold(
            fold=int(row["fold"]) - 1,
            history_days=tuple(row["history_days"]),
            embargo_day=str(row["calendar_embargo_utc_day"]),
            fit_day_candidates=tuple(row["fit_day_candidates_after_day_embargo"]),
            test_days=tuple(row["test_days"]),
        )
        for row in fold_payload["folds"]
    )
    if tuple(fold.fold for fold in frozen_outer_folds) != tuple(range(4)):
        raise ValueError("outer-fold source must contain exact folds 1 through 4")
    frozen_test_days = tuple(day for fold in frozen_outer_folds for day in fold.test_days)
    if ordered_days[:16] + frozen_test_days != ordered_days:
        raise ValueError("outer-fold source does not cover the frozen 40-day chronology")

    expected_opportunities = int(
        denominator["expected_opportunity_count_from_outcome_blind_census"]
    )
    expected_arm_rows = int(denominator["expected_single_action_fork_count"])
    if expected_opportunities != 8600 or expected_arm_rows != 68800:
        raise ValueError("formal opportunity/arm denominator drifted from 8,600/68,800")
    if expected_arm_rows != expected_opportunities * 8:
        raise ValueError("formal arm denominator is not eight arms per opportunity")
    support = search["complexity_grid"]["minimum_clause_support"]
    contract = SearchContract(
        spec_path=str(path),
        spec_sha256=observed_spec_sha,
        outcome_blind_path=str(outcome_blind_path),
        outcome_blind_sha256=observed_outcome_blind_sha,
        predicate_artifact_sha256=payload["ema_state_contract"]["atomic_predicate_source"][
            "sha256"
        ],
        predicate_columns=predicate_columns,
        predicate_schema_sha256=_sha256(list(predicate_columns)),
        ordered_development_days=ordered_days,
        expected_opportunities=expected_opportunities,
        expected_arm_rows=expected_arm_rows,
        outer_fold_source_path=str(outer_source_path),
        outer_fold_source_sha256=observed_outer_sha,
        frozen_outer_folds=frozen_outer_folds,
        side_actions=actions,
        max_literals_per_clause=tuple(
            int(value) for value in search["complexity_grid"]["max_literals_per_clause"]
        ),
        max_clauses=tuple(int(value) for value in search["complexity_grid"]["max_clauses"]),
        minimum_days=int(support["distinct_utc_days"]),
        minimum_campaigns=int(support["distinct_campaign_side_clusters"]),
        minimum_campaign_weight_fraction=float(support["campaign_weight_fraction"]),
        beam_width=int(search["complexity_grid"]["beam_width"]),
        seed=int(search["complexity_grid"]["seed"]),
        outer_initial_history_days=16,
        outer_test_days=6,
        outer_fold_count=4,
        outer_embargo_calendar_days=int(chronology["outer_calendar_embargo_days"]),
        inner_fold_count=int(chronology["inner_fold_count_per_outer_fold"]),
        inner_test_days=3,
        formal_confidence=FORMAL_CONFIDENCE,
        formal_bootstrap_samples=FORMAL_BOOTSTRAP_SAMPLES,
    )
    if contract.max_literals_per_clause != (1, 2, 3, 4, 6):
        raise ValueError("unexpected frozen literal complexity grid")
    if contract.max_clauses != (2, 4, 8) or contract.beam_width != 256:
        raise ValueError("unexpected frozen clause grid or beam width")
    return contract


@dataclass(frozen=True, order=True)
class Literal:
    predicate: str
    negated: bool = False

    def evaluate(self, predicates: pd.DataFrame) -> np.ndarray:
        if self.predicate not in predicates:
            raise ValueError(f"unknown predicate in policy: {self.predicate}")
        values = predicates[self.predicate].to_numpy(dtype=bool, copy=False)
        if not self.negated:
            return values
        observed_column = _cross_observed_column(self.predicate)
        if observed_column is None:
            return np.logical_not(values)
        if observed_column not in predicates:
            raise ValueError("negated crossover predicate lacks explicit observed-state validity")
        observed = predicates[observed_column].to_numpy(dtype=bool, copy=False)
        return np.logical_not(values) & observed

    def serialize(self) -> dict[str, Any]:
        return {"predicate": self.predicate, "negated": self.negated}


@dataclass(frozen=True)
class Clause:
    literals: tuple[Literal, ...]

    def __post_init__(self) -> None:
        if not self.literals:
            raise ValueError("a Boolean clause cannot be empty")
        ordered = tuple(sorted(self.literals))
        if ordered != self.literals:
            raise ValueError("clause literals must be canonically sorted")
        names = [literal.predicate for literal in self.literals]
        if len(names) != len(set(names)):
            raise ValueError("a clause cannot contain duplicate/complementary predicates")

    def evaluate(self, predicates: pd.DataFrame) -> np.ndarray:
        result = np.ones(len(predicates), dtype=bool)
        for literal in self.literals:
            result &= literal.evaluate(predicates)
        return result

    @property
    def key(self) -> tuple[tuple[str, bool], ...]:
        return tuple((literal.predicate, literal.negated) for literal in self.literals)

    def serialize(self) -> dict[str, Any]:
        return {"literals": [literal.serialize() for literal in self.literals]}


@dataclass(frozen=True)
class Rule:
    """One first-match rule; its clauses form a sparse DNF union."""

    action: str
    clauses: tuple[Clause, ...]
    conditional_point_uplift_usdc: float

    def evaluate(self, predicates: pd.DataFrame) -> np.ndarray:
        result = np.zeros(len(predicates), dtype=bool)
        for clause in self.clauses:
            result |= clause.evaluate(predicates)
        return result

    def serialize(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "clauses": [clause.serialize() for clause in self.clauses],
            "conditional_point_uplift_usdc": self.conditional_point_uplift_usdc,
            "conditional_rule_evidence_is_descriptive_only": True,
        }


@dataclass(frozen=True)
class BooleanCooldownPolicy:
    side: str
    rules: tuple[Rule, ...]
    default_action: str
    predicate_columns: tuple[str, ...]
    spec_sha256: str
    predicate_artifact_sha256: str
    predicate_schema_sha256: str
    outcome_blind_sha256: str
    outer_fold_source_sha256: str
    formal_input_identity_sha256: str
    economic_epsilon_usdc: float
    training_fold_identities: tuple[str, ...]
    beam_survivor_family_id: str
    beam_survivor_family_sha256: str
    beam_survivor_family_size: int
    beam_survivor_family_conditional_critical_usdc: float
    beam_survivor_family_conditional_policy_lcb_usdc: float
    confidence: float
    bootstrap_samples: int
    synthetic_test_only: bool = False
    selection_mode: str = DEPLOYMENT_LCB_SELECTION
    policy_identity: str = IDENTITY
    selected_candidate_rank: int | None = None

    def choose(self, predicates: pd.DataFrame) -> np.ndarray:
        unknown = set(self.predicate_columns) - set(predicates.columns)
        if unknown:
            raise ValueError(f"policy input is missing predicates: {sorted(unknown)}")
        result = np.full(len(predicates), self.default_action, dtype=object)
        unmatched = np.ones(len(predicates), dtype=bool)
        for rule in self.rules:
            selected = unmatched & rule.evaluate(predicates)
            result[selected] = rule.action
            unmatched[selected] = False
        return result

    def artifact(self) -> dict[str, Any]:
        implementation_sha = _file_sha256(Path(__file__))
        body = {
            "schema_version": f"{self.policy_identity}.boolean_policy.v3",
            "identity": self.policy_identity,
            "side": self.side,
            "ordered_rules": [rule.serialize() for rule in self.rules],
            "default_action": self.default_action,
            "predicate_columns": list(self.predicate_columns),
            "predicate_artifact_sha256": self.predicate_artifact_sha256,
            "predicate_schema_sha256": self.predicate_schema_sha256,
            "outcome_blind_sha256": self.outcome_blind_sha256,
            "outer_fold_source_sha256": self.outer_fold_source_sha256,
            "formal_input_identity_sha256": self.formal_input_identity_sha256,
            "duration_spec_sha256": self.spec_sha256,
            "economic_epsilon_usdc": self.economic_epsilon_usdc,
            "training_fold_identities": list(self.training_fold_identities),
            "beam_survivor_family_id": self.beam_survivor_family_id,
            "beam_survivor_family_sha256": self.beam_survivor_family_sha256,
            "beam_survivor_family_size": self.beam_survivor_family_size,
            "beam_survivor_family_conditional_critical_usdc": (
                self.beam_survivor_family_conditional_critical_usdc
            ),
            "beam_survivor_family_conditional_policy_lcb_usdc": (
                self.beam_survivor_family_conditional_policy_lcb_usdc
            ),
            "beam_survivor_family_evidence_scope": (
                "conditional_on_frozen_beam_survivors_not_full_search_selection"
            ),
            "beam_survivor_family_promotion_authority": False,
            "selection_mode": self.selection_mode,
            "selected_candidate_rank": self.selected_candidate_rank,
            "pre_outer_oof_positive_lcb_required": (
                self.selection_mode == DEPLOYMENT_LCB_SELECTION
            ),
            "candidate_economic_evidence_is_development_only": True,
            "confidence": self.confidence,
            "bootstrap_samples": self.bootstrap_samples,
            "implementation_sha256": implementation_sha,
            "synthetic_test_only": self.synthetic_test_only,
            "permissions": {
                "action_authorized": False,
                "live_authorized": False,
                "f09_registration_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        }
        return {**body, "policy_sha256": _sha256(body)}


@dataclass(frozen=True)
class PanelAudit:
    input_opportunities: int
    eligible_opportunities: int
    joint_censored_opportunities: int
    excluded_opportunity_ids: tuple[str, ...]
    campaign_weight_min: float
    campaign_weight_max: float


@dataclass(frozen=True)
class OpportunityPanel:
    frame: pd.DataFrame
    predicates: pd.DataFrame
    actions: tuple[str, ...]
    audit: PanelAudit
    synthetic_test_only: bool


@dataclass
class _SearchWorkCounters:
    """Deterministic work counters used by the equivalence/performance tests."""

    clause_evaluations: int = 0
    rule_state_evaluations: int = 0
    rule_state_exact_rescores: int = 0
    rule_state_materializations: int = 0
    bootstrap_draws_built: int = 0
    bootstrap_state_columns_built: int = 0


@dataclass
class _WorstFirst:
    """Heap entry whose smallest item is the worst exact total-order rank."""

    rank: tuple[Any, ...]
    serial: int
    value: Any

    def __lt__(self, other: _WorstFirst) -> bool:
        return (self.rank, self.serial) > (other.rank, other.serial)


class _ExactTopK:
    """Streaming equivalent of stable ``sorted(items, key=rank)[:k]``."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._heap: list[_WorstFirst] = []
        self._serial = 0

    def offer(self, value: Any, rank: tuple[Any, ...]) -> None:
        entry = _WorstFirst(rank, self._serial, value)
        self._serial += 1
        if len(self._heap) < self.limit:
            heapq.heappush(self._heap, entry)
            return
        worst = self._heap[0]
        if (entry.rank, entry.serial) < (worst.rank, worst.serial):
            heapq.heapreplace(self._heap, entry)

    def __len__(self) -> int:
        return len(self._heap)

    def worst_rank(self) -> tuple[Any, ...]:
        if not self._heap:
            raise IndexError("top-k heap is empty")
        return self._heap[0].rank

    def values(self) -> tuple[Any, ...]:
        return tuple(
            entry.value for entry in sorted(self._heap, key=lambda item: (item.rank, item.serial))
        )


def _validate_common(frame: pd.DataFrame) -> pd.DataFrame:
    missing = set(COMMON_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"panel is missing common columns: {sorted(missing)}")
    result = frame.copy(deep=True)
    for column in ("opportunity_id", "side", "utc_day", "campaign_side_id"):
        values = result[column].astype("string")
        if values.isna().any() or values.str.strip().eq("").any():
            raise ValueError(f"{column} must be non-empty")
        result[column] = values.astype(str)
    if not set(result["side"]).issubset({"BUY", "SELL"}):
        raise ValueError("side must be BUY or SELL")
    for day in result["utc_day"].unique():
        if pd.Timestamp(day).strftime("%Y-%m-%d") != day:
            raise ValueError("utc_day must use YYYY-MM-DD")
    for column in ("assignment_ts_ns", "washout_ts_ns"):
        result[column] = pd.to_numeric(result[column], errors="raise").astype("int64")
    if (result["washout_ts_ns"] < result["assignment_ts_ns"]).any():
        raise ValueError("washout cannot precede assignment")
    result["joint_censored"] = result["joint_censored"].astype(bool)
    return result


def _feature_columns(
    frame: pd.DataFrame,
    action_columns: Iterable[str],
    *,
    contract: SearchContract,
    synthetic_mode: bool,
) -> tuple[str, ...]:
    del action_columns
    observed = tuple(column for column in frame.columns if column.startswith("predicate::"))
    columns = tuple(sorted(observed)) if synthetic_mode else observed
    if not columns:
        raise ValueError("panel has no explicit predicate:: Boolean columns")
    if not synthetic_mode:
        if columns != contract.predicate_columns:
            raise ValueError(
                "formal predicate names/order differ from the frozen outcome-blind artifact"
            )
        if _sha256(list(columns)) != contract.predicate_schema_sha256:
            raise ValueError("formal predicate schema SHA256 mismatch")
    for column in columns:
        values = frame[column]
        if values.isna().any() or not values.isin([True, False, 0, 1]).all():
            raise ValueError(f"predicate {column} must be complete and Boolean")
    return columns


def attest_formal_input_panel(
    panel: pd.DataFrame,
    *,
    contract: SearchContract | None = None,
) -> FormalInputIdentity:
    """Attest the complete 68,800-row formal panel before side splitting.

    The returned identity is immutable input metadata.  Side-specific learners
    must carry it so a partial BUY or SELL view cannot masquerade as the full
    8,600-opportunity materialization denominator.
    """

    contract = contract or load_frozen_search_contract()
    raw = _validate_common(panel)
    if "candidate_policy_id" not in raw or OUTCOME_VALUE_COLUMN not in raw:
        raise ValueError("formal attestation requires the complete long-form arm panel")
    _feature_columns(
        raw,
        (),
        contract=contract,
        synthetic_mode=False,
    )
    if len(raw) != contract.expected_arm_rows:
        raise ValueError("formal arm-row denominator must be exactly 68,800")
    if raw["opportunity_id"].nunique() != contract.expected_opportunities:
        raise ValueError("formal opportunity denominator must be exactly 8,600")
    observed_days = tuple(dict.fromkeys(raw["utc_day"].astype(str)))
    if observed_days != contract.ordered_development_days:
        raise ValueError("formal panel does not preserve the exact ordered 40 days")
    if raw.duplicated(["opportunity_id", "candidate_policy_id"]).any():
        raise ValueError("formal panel contains duplicate opportunity/action rows")
    for opportunity_id, rows in raw.groupby("opportunity_id", sort=False):
        if len(rows) != 8:
            raise ValueError(f"formal opportunity {opportunity_id} does not have eight arms")
        if rows["side"].nunique() != 1:
            raise ValueError(f"formal opportunity {opportunity_id} pools BUY and SELL")
        side = str(rows["side"].iloc[0])
        if set(rows["candidate_policy_id"].astype(str)) != set(contract.side_actions[side]):
            raise ValueError(f"formal opportunity {opportunity_id} action universe drifted")
    return FormalInputIdentity(
        ordered_utc_days=observed_days,
        opportunity_count=int(raw["opportunity_id"].nunique()),
        arm_row_count=len(raw),
        predicate_schema_sha256=contract.predicate_schema_sha256,
        outer_fold_source_sha256=contract.outer_fold_source_sha256,
        spec_sha256=contract.spec_sha256,
        outcome_blind_sha256=contract.outcome_blind_sha256,
    )


def _validate_formal_input_identity(
    identity: FormalInputIdentity | None,
    contract: SearchContract,
) -> FormalInputIdentity:
    if identity is None:
        raise ValueError(
            "formal side training requires a full-panel FormalInputIdentity attestation"
        )
    expected = FormalInputIdentity(
        ordered_utc_days=contract.ordered_development_days,
        opportunity_count=contract.expected_opportunities,
        arm_row_count=contract.expected_arm_rows,
        predicate_schema_sha256=contract.predicate_schema_sha256,
        outer_fold_source_sha256=contract.outer_fold_source_sha256,
        spec_sha256=contract.spec_sha256,
        outcome_blind_sha256=contract.outcome_blind_sha256,
    )
    if identity != expected:
        raise ValueError("formal full-panel identity differs from the frozen denominator")
    return identity


def normalize_joint_panel(
    panel: pd.DataFrame,
    *,
    side: str,
    contract: SearchContract | None = None,
    synthetic_mode: bool = False,
) -> OpportunityPanel:
    """Normalize complete long or wide potential-outcome data.

    Long form uses ``candidate_policy_id`` and
    ``assignment_to_washout_value_usdc``.  Wide form uses one ``q::<action>``
    column per frozen action.  A censored opportunity must still contain all
    arms; it is then excluded as one joint unit and reported.  Missing arms are
    never repaired by complete-case selection.
    """

    contract = contract or load_frozen_search_contract()
    if side not in ("BUY", "SELL"):
        raise ValueError("fit side must be BUY or SELL")
    raw = _validate_common(panel)
    if set(raw["side"]) != {side}:
        raise ValueError("pooled BUY/SELL panels are forbidden")
    actions = tuple(contract.side_actions[side])
    is_long = "candidate_policy_id" in raw.columns or OUTCOME_VALUE_COLUMN in raw.columns
    if is_long and not {"candidate_policy_id", OUTCOME_VALUE_COLUMN}.issubset(raw.columns):
        raise ValueError("long panel requires both action and outcome columns")

    synthetic = bool(synthetic_mode)
    if is_long:
        features = _feature_columns(
            raw,
            (),
            contract=contract,
            synthetic_mode=synthetic,
        )
        raw["candidate_policy_id"] = raw["candidate_policy_id"].astype(str)
        if not set(raw["candidate_policy_id"]).issubset(actions):
            raise ValueError("long panel contains an action outside the frozen side grid")
        if raw.duplicated(["opportunity_id", "candidate_policy_id"]).any():
            raise ValueError("opportunity/action rows must be unique")
        groups: list[dict[str, Any]] = []
        predicate_rows: list[dict[str, bool]] = []
        for opportunity_id, group in raw.groupby("opportunity_id", sort=False):
            if set(group["candidate_policy_id"]) != set(actions) or len(group) != len(actions):
                raise ValueError(f"opportunity {opportunity_id} does not contain all eight arms")
            for column in COMMON_COLUMNS:
                if group[column].nunique(dropna=False) != 1:
                    raise ValueError(f"opportunity {opportunity_id} has inconsistent {column}")
            for column in features:
                if group[column].nunique(dropna=False) != 1:
                    raise ValueError(
                        f"opportunity {opportunity_id} has inconsistent predicate {column}"
                    )
            censored = bool(group["joint_censored"].any())
            record = {column: group.iloc[0][column] for column in COMMON_COLUMNS}
            record["joint_censored"] = censored
            for action in actions:
                value = group.loc[
                    group["candidate_policy_id"].eq(action), OUTCOME_VALUE_COLUMN
                ].iloc[0]
                if not censored and not np.isfinite(float(value)):
                    raise ValueError(
                        f"eligible opportunity {opportunity_id} has a missing arm value"
                    )
                record[f"q::{action}"] = float(value) if pd.notna(value) else np.nan
            groups.append(record)
            predicate_row = {column: bool(group.iloc[0][column]) for column in features}
            for predicate in features:
                observed_column = _cross_observed_column(predicate)
                missing_column = _raw_cross_missing_column(predicate)
                if observed_column is None or missing_column is None:
                    continue
                if missing_column not in group.columns:
                    if not synthetic:
                        raise ValueError(
                            f"formal panel lacks crossover validity field: {missing_column}"
                        )
                    predicate_row[observed_column] = True
                else:
                    predicate_row[observed_column] = not bool(group.iloc[0][missing_column])
            predicate_rows.append(predicate_row)
        wide = pd.DataFrame(groups)
        predicates = pd.DataFrame(predicate_rows)
        predicates = predicates.loc[
            :,
            [
                *features,
                *sorted(
                    column
                    for column in predicates.columns
                    if column.startswith(_CROSS_OBSERVED_PREFIX)
                ),
            ],
        ]
    else:
        q_columns = tuple(f"q::{action}" for action in actions)
        missing = set(q_columns) - set(raw.columns)
        if missing:
            raise ValueError(f"wide panel is missing frozen action columns: {sorted(missing)}")
        if raw["opportunity_id"].duplicated().any():
            raise ValueError("wide panel must contain one row per opportunity")
        features = _feature_columns(
            raw,
            q_columns,
            contract=contract,
            synthetic_mode=synthetic,
        )
        wide = raw.loc[:, [*COMMON_COLUMNS, *q_columns]].reset_index(drop=True)
        predicates = raw.loc[:, features].astype(bool).reset_index(drop=True)
        for predicate in features:
            observed_column = _cross_observed_column(predicate)
            missing_column = _raw_cross_missing_column(predicate)
            if observed_column is None or missing_column is None:
                continue
            if missing_column not in raw.columns:
                if not synthetic:
                    raise ValueError(
                        f"formal panel lacks crossover validity field: {missing_column}"
                    )
                predicates[observed_column] = True
            else:
                predicates[observed_column] = ~raw[missing_column].astype(bool).reset_index(
                    drop=True
                )

    if not synthetic and len(features) != len(contract.predicate_columns):
        raise ValueError("formal panel must contain all 360 frozen predicates")
    censored_mask = wide["joint_censored"].to_numpy(dtype=bool)
    for action in actions:
        values = pd.to_numeric(wide[f"q::{action}"], errors="coerce").to_numpy(dtype=float)
        if np.any(~np.isfinite(values[~censored_mask])):
            raise ValueError("an eligible opportunity has a non-finite action value")
        wide[f"q::{action}"] = values

    excluded = tuple(wide.loc[censored_mask, "opportunity_id"].astype(str))
    wide = wide.loc[~censored_mask].reset_index(drop=True)
    predicates = predicates.loc[~censored_mask].reset_index(drop=True)
    if wide.empty:
        raise ValueError("all opportunities are joint-censored")
    counts = wide.groupby("campaign_side_id")["opportunity_id"].transform("count")
    wide["campaign_weight"] = 1.0 / counts.to_numpy(dtype=float)
    totals = wide.groupby("campaign_side_id")["campaign_weight"].sum().to_numpy(dtype=float)
    if not np.allclose(totals, 1.0, atol=1e-12, rtol=0.0):
        raise AssertionError("campaign opportunity weights do not sum to one")
    audit = PanelAudit(
        input_opportunities=len(censored_mask),
        eligible_opportunities=len(wide),
        joint_censored_opportunities=int(censored_mask.sum()),
        excluded_opportunity_ids=excluded,
        campaign_weight_min=float(totals.min()),
        campaign_weight_max=float(totals.max()),
    )
    return OpportunityPanel(wide, predicates, actions, audit, synthetic)


def _weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    total = float(weights.sum())
    return float(np.dot(values, weights) / total) if total > 0.0 else float("-inf")


def _support_ok(mask: np.ndarray, panel: OpportunityPanel, contract: SearchContract) -> bool:
    if not mask.any():
        return False
    frame = panel.frame.loc[mask]
    distinct_days = frame["utc_day"].nunique()
    distinct_campaigns = frame["campaign_side_id"].nunique()
    supported_weight = float(frame["campaign_weight"].sum())
    total_campaign_weight = float(panel.frame["campaign_weight"].sum())
    return (
        distinct_days >= contract.minimum_days
        and distinct_campaigns >= contract.minimum_campaigns
        and supported_weight / total_campaign_weight >= contract.minimum_campaign_weight_fraction
    )


def _nested_cluster_bootstrap_lcbs(
    effects: Sequence[np.ndarray],
    masks: Sequence[np.ndarray],
    panel: OpportunityPanel,
    *,
    seed: int,
    samples: int,
    confidence: float,
    work_counters: _SearchWorkCounters | None = None,
) -> tuple[np.ndarray, np.ndarray, float]:
    """Conditional max band over the supplied frozen beam-survivor family.

    This function does not rerun the beam search inside each bootstrap draw.
    Its result is therefore conditional evidence for inner model filtering and
    has no research-promotion authority.
    """

    if samples < 100:
        raise ValueError("simultaneous bootstrap requires at least 100 draws")
    if work_counters is not None:
        work_counters.bootstrap_draws_built += samples
        work_counters.bootstrap_state_columns_built += len(effects)
    frame = panel.frame
    weights = frame["campaign_weight"].to_numpy(dtype=float)
    points = np.asarray(
        [
            _weighted_mean(effect[mask], weights[mask])
            for effect, mask in zip(effects, masks, strict=True)
        ],
        dtype=float,
    )
    days = tuple(sorted(frame["utc_day"].unique()))
    day_indices = {day: np.flatnonzero(frame["utc_day"].to_numpy() == day) for day in days}
    rng = np.random.default_rng(seed)
    boot = np.empty((samples, len(effects)), dtype=float)
    for draw in range(samples):
        pieces: list[np.ndarray] = []
        for sampled_day in rng.choice(days, size=len(days), replace=True):
            indices = day_indices[str(sampled_day)]
            campaigns = frame.iloc[indices]["campaign_side_id"].unique()
            sampled_campaigns = rng.choice(campaigns, size=len(campaigns), replace=True)
            for campaign in sampled_campaigns:
                pieces.append(
                    indices[frame.iloc[indices]["campaign_side_id"].to_numpy() == campaign]
                )
        sampled = np.concatenate(pieces) if pieces else np.empty(0, dtype=int)
        for index, (effect, mask) in enumerate(zip(effects, masks, strict=True)):
            selected = sampled[mask[sampled]]
            boot[draw, index] = (
                _weighted_mean(effect[selected], weights[selected])
                if len(selected)
                else points[index]
            )
    centered_max = np.max(np.abs(boot - points[None, :]), axis=1)
    critical = float(np.quantile(centered_max, confidence, method="higher"))
    return points, points - critical, critical


@dataclass(frozen=True)
class _ClauseCandidate:
    action: str
    clause: Clause
    mask: np.ndarray
    score: float

    @property
    def key(self) -> tuple[str, tuple[tuple[str, bool], ...]]:
        return self.action, self.clause.key


def _generate_clause_pool(
    panel: OpportunityPanel,
    contract: SearchContract,
    *,
    max_literals: int,
    work_counters: _SearchWorkCounters | None = None,
) -> tuple[_ClauseCandidate, ...]:
    predicates = panel.predicates
    weights = panel.frame["campaign_weight"].to_numpy(dtype=float)
    control = panel.frame[f"q::{CONTROL_ACTION}"].to_numpy(dtype=float)
    literals = tuple(
        Literal(column, negated)
        for column in sorted(_searchable_predicate_columns(predicates))
        for negated in (False, True)
    )
    beam: list[Clause] = [Clause((literal,)) for literal in literals]
    pool: dict[tuple[str, tuple[tuple[str, bool], ...]], _ClauseCandidate] = {}
    for depth in range(1, max_literals + 1):
        ranked_for_extension: list[tuple[float, Clause]] = []
        for clause in beam:
            if work_counters is not None:
                work_counters.clause_evaluations += 1
            mask = clause.evaluate(predicates)
            if not _support_ok(mask, panel, contract):
                continue
            best_score = float("-inf")
            for action in panel.actions:
                if action == CONTROL_ACTION:
                    continue
                effect = panel.frame[f"q::{action}"].to_numpy(dtype=float) - control
                score = float(np.dot(weights * mask, effect) / weights.sum())
                best_score = max(best_score, score)
                candidate = _ClauseCandidate(action, clause, mask, score)
                prior = pool.get(candidate.key)
                if prior is None or candidate.score > prior.score:
                    pool[candidate.key] = candidate
            ranked_for_extension.append((best_score, clause))
        ranked_for_extension.sort(key=lambda item: (-item[0], item[1].key))
        parents = [item[1] for item in ranked_for_extension[: contract.beam_width]]
        if depth == max_literals:
            break
        next_clauses: dict[tuple[tuple[str, bool], ...], Clause] = {}
        for parent in parents:
            used = {literal.predicate for literal in parent.literals}
            for literal in literals:
                if literal.predicate in used:
                    continue
                combined = Clause(tuple(sorted((*parent.literals, literal))))
                next_clauses[combined.key] = combined
        beam = list(next_clauses.values())
    ranked = sorted(pool.values(), key=lambda item: (-item.score, item.key))
    return tuple(ranked[: contract.beam_width])


def _generate_clause_depth_snapshots(
    panel: OpportunityPanel,
    contract: SearchContract,
    *,
    max_literals: Sequence[int],
    work_counters: _SearchWorkCounters | None = None,
) -> dict[int, tuple[_ClauseCandidate, ...]]:
    """Generate every requested literal-depth pool in one exact prefix walk."""

    snapshot_depths = tuple(sorted(set(int(value) for value in max_literals)))
    if not snapshot_depths or snapshot_depths[0] < 1:
        raise ValueError("literal snapshot depths must be positive")
    predicates = panel.predicates
    weights = panel.frame["campaign_weight"].to_numpy(dtype=float)
    weight_total = weights.sum()
    control = panel.frame[f"q::{CONTROL_ACTION}"].to_numpy(dtype=float)
    action_effects = {
        action: panel.frame[f"q::{action}"].to_numpy(dtype=float) - control
        for action in panel.actions
        if action != CONTROL_ACTION
    }
    literals = tuple(
        Literal(column, negated)
        for column in sorted(_searchable_predicate_columns(predicates))
        for negated in (False, True)
    )
    beam: list[Clause] = [Clause((literal,)) for literal in literals]
    candidate_top = _ExactTopK(contract.beam_width)
    snapshots: dict[int, tuple[_ClauseCandidate, ...]] = {}
    for depth in range(1, snapshot_depths[-1] + 1):
        parent_top = _ExactTopK(contract.beam_width)
        for clause in beam:
            if work_counters is not None:
                work_counters.clause_evaluations += 1
            mask = clause.evaluate(predicates)
            if not _support_ok(mask, panel, contract):
                continue
            best_score = float("-inf")
            weighted_mask = weights * mask
            for action in panel.actions:
                if action == CONTROL_ACTION:
                    continue
                score = float(np.dot(weighted_mask, action_effects[action]) / weight_total)
                best_score = max(best_score, score)
                candidate = _ClauseCandidate(action, clause, mask, score)
                candidate_top.offer(candidate, (-candidate.score, candidate.key))
            parent_top.offer((best_score, clause), (-best_score, clause.key))
        if depth in snapshot_depths:
            snapshots[depth] = candidate_top.values()
        if depth == snapshot_depths[-1]:
            break
        parents = [item[1] for item in parent_top.values()]
        next_clauses: dict[tuple[tuple[str, bool], ...], Clause] = {}
        for parent in parents:
            used = {literal.predicate for literal in parent.literals}
            for literal in literals:
                if literal.predicate in used:
                    continue
                combined = Clause(tuple(sorted((*parent.literals, literal))))
                next_clauses[combined.key] = combined
        beam = list(next_clauses.values())
    return snapshots


@dataclass(frozen=True)
class _PolicyState:
    entries: tuple[_ClauseCandidate, ...]
    actions: np.ndarray
    score: float

    @property
    def key(self) -> tuple[tuple[str, tuple[tuple[str, bool], ...]], ...]:
        return tuple(entry.key for entry in self.entries)


def _search_rule_list_reference(
    panel: OpportunityPanel,
    contract: SearchContract,
    *,
    max_literals: int,
    max_clauses: int,
    work_counters: _SearchWorkCounters | None = None,
) -> tuple[_PolicyState, ...]:
    """Run the original slow search for test/reference equivalence only."""

    pool = _generate_clause_pool(
        panel,
        contract,
        max_literals=max_literals,
        work_counters=work_counters,
    )
    if not pool:
        return ()
    weights = panel.frame["campaign_weight"].to_numpy(dtype=float)
    control = panel.frame[f"q::{CONTROL_ACTION}"].to_numpy(dtype=float)

    def make_state(entries: tuple[_ClauseCandidate, ...]) -> _PolicyState:
        if work_counters is not None:
            work_counters.rule_state_evaluations += 1
            work_counters.rule_state_materializations += 1
        chosen = np.full(len(panel.frame), CONTROL_ACTION, dtype=object)
        unmatched = np.ones(len(panel.frame), dtype=bool)
        for entry in entries:
            selected = unmatched & entry.mask
            chosen[selected] = entry.action
            unmatched[selected] = False
        values = control.copy()
        for action in panel.actions:
            selected = chosen == action
            if selected.any():
                values[selected] = panel.frame.loc[selected, f"q::{action}"].to_numpy(dtype=float)
        score = _weighted_mean(values - control, weights)
        return _PolicyState(entries, chosen, score)

    beam = [make_state((candidate,)) for candidate in pool]
    all_states: dict[tuple[Any, ...], _PolicyState] = {state.key: state for state in beam}
    beam = sorted(beam, key=lambda state: (-state.score, state.key))[: contract.beam_width]
    for _ in range(2, max_clauses + 1):
        expanded: dict[tuple[Any, ...], _PolicyState] = {}
        for state in beam:
            used = set(state.key)
            for candidate in pool:
                if candidate.key in used:
                    continue
                next_state = make_state((*state.entries, candidate))
                expanded[next_state.key] = next_state
        if not expanded:
            break
        ranked = sorted(expanded.values(), key=lambda state: (-state.score, state.key))
        beam = ranked[: contract.beam_width]
        all_states.update((state.key, state) for state in beam)
    return tuple(sorted(all_states.values(), key=lambda state: (-state.score, state.key)))


@dataclass(frozen=True)
class _IncrementalPolicyState:
    entries: tuple[_ClauseCandidate, ...]
    unmatched: np.ndarray
    effect: np.ndarray
    weighted_abs_sum: float
    score: float

    @property
    def key(self) -> tuple[tuple[str, tuple[tuple[str, bool], ...]], ...]:
        return tuple(entry.key for entry in self.entries)


@dataclass(frozen=True)
class _StateExpansion:
    parent: _IncrementalPolicyState
    candidate: _ClauseCandidate
    entries: tuple[_ClauseCandidate, ...]
    key: tuple[tuple[str, tuple[tuple[str, bool], ...]], ...]
    score: float
    error_bound: float


_PolicyStateLike = _PolicyState | _IncrementalPolicyState


class _IncrementalStateFactory:
    """Score children incrementally and materialize arrays only for survivors."""

    def __init__(
        self,
        panel: OpportunityPanel,
        *,
        work_counters: _SearchWorkCounters | None,
    ) -> None:
        self.panel = panel
        self.work_counters = work_counters
        self.weights = panel.frame["campaign_weight"].to_numpy(dtype=float)
        self.weight_total = float(self.weights.sum())
        self.control = panel.frame[f"q::{CONTROL_ACTION}"].to_numpy(dtype=float)
        self.action_effects = {
            action: panel.frame[f"q::{action}"].to_numpy(dtype=float) - self.control
            for action in panel.actions
        }
        self._exact_effect_scratch = np.empty_like(self.control)
        self.state_cache: dict[
            tuple[tuple[str, tuple[tuple[str, bool], ...]], ...],
            _IncrementalPolicyState,
        ] = {}
        self.candidate_abs_contribution: dict[tuple[str, tuple[tuple[str, bool], ...]], float] = {}

    def _count_evaluation(self) -> None:
        if self.work_counters is not None:
            self.work_counters.rule_state_evaluations += 1

    def _count_materialization(self) -> None:
        if self.work_counters is not None:
            self.work_counters.rule_state_materializations += 1

    def _candidate_abs_sum(self, candidate: _ClauseCandidate) -> float:
        cached = self.candidate_abs_contribution.get(candidate.key)
        if cached is not None:
            return cached
        selected_weights = self.weights[candidate.mask]
        selected_effect = self.action_effects[candidate.action][candidate.mask]
        value = float(np.dot(selected_weights, np.abs(selected_effect)))
        self.candidate_abs_contribution[candidate.key] = value
        return value

    def _score_error_bound(
        self,
        parent: _IncrementalPolicyState,
        candidate: _ClauseCandidate,
        fast_score: float,
    ) -> float:
        unit_roundoff = np.finfo(np.float64).eps / 2.0
        terms = len(self.weights) + 4
        gamma = terms * unit_roundoff / (1.0 - terms * unit_roundoff)
        absolute_scale = (
            parent.weighted_abs_sum + self._candidate_abs_sum(candidate)
        ) / self.weight_total
        bound = 64.0 * (
            gamma * absolute_scale + unit_roundoff * (abs(parent.score) + abs(fast_score) + 1.0)
        )
        return float(np.nextafter(bound, np.inf))

    def initial(self, candidate: _ClauseCandidate) -> _IncrementalPolicyState:
        key = (candidate.key,)
        cached = self.state_cache.get(key)
        if cached is not None:
            return cached
        self._count_evaluation()
        self._count_materialization()
        effect = np.zeros_like(self.control)
        effect[candidate.mask] = self.action_effects[candidate.action][candidate.mask]
        unmatched = np.ones(len(self.panel.frame), dtype=bool)
        unmatched[candidate.mask] = False
        state = _IncrementalPolicyState(
            entries=(candidate,),
            unmatched=unmatched,
            effect=effect,
            weighted_abs_sum=float(np.dot(np.abs(effect), self.weights)),
            score=_weighted_mean(effect, self.weights),
        )
        self.state_cache[key] = state
        return state

    def expansion(
        self,
        parent: _IncrementalPolicyState,
        candidate: _ClauseCandidate,
    ) -> _IncrementalPolicyState | _StateExpansion:
        entries = (*parent.entries, candidate)
        key = (*parent.key, candidate.key)
        cached = self.state_cache.get(key)
        if cached is not None:
            return cached
        self._count_evaluation()
        selected = parent.unmatched & candidate.mask
        if not selected.any():
            return _StateExpansion(
                parent=parent,
                candidate=candidate,
                entries=entries,
                key=key,
                score=parent.score,
                error_bound=0.0,
            )
        delta = float(
            np.dot(
                self.action_effects[candidate.action][selected],
                self.weights[selected],
            )
            / self.weight_total
        )
        fast_score = float(parent.score + delta)
        return _StateExpansion(
            parent=parent,
            candidate=candidate,
            entries=entries,
            key=key,
            score=fast_score,
            error_bound=self._score_error_bound(parent, candidate, fast_score),
        )

    def exactify(
        self,
        value: _IncrementalPolicyState | _StateExpansion,
    ) -> _IncrementalPolicyState | _StateExpansion:
        if isinstance(value, _IncrementalPolicyState) or value.error_bound == 0.0:
            return value
        selected = value.parent.unmatched & value.candidate.mask
        np.copyto(self._exact_effect_scratch, value.parent.effect)
        self._exact_effect_scratch[selected] = self.action_effects[value.candidate.action][selected]
        exact_score = _weighted_mean(self._exact_effect_scratch, self.weights)
        if self.work_counters is not None:
            self.work_counters.rule_state_exact_rescores += 1
        if abs(exact_score - value.score) > value.error_bound:
            raise AssertionError("incremental state score exceeded its exact ranking bound")
        return replace(value, score=exact_score, error_bound=0.0)

    def materialize(
        self,
        value: _IncrementalPolicyState | _StateExpansion,
    ) -> _IncrementalPolicyState:
        if isinstance(value, _IncrementalPolicyState):
            return value
        cached = self.state_cache.get(value.key)
        if cached is not None:
            return cached
        if value.error_bound != 0.0:
            raise AssertionError("only exactly ranked state survivors may be materialized")
        self._count_materialization()
        selected = value.parent.unmatched & value.candidate.mask
        effect = value.parent.effect.copy()
        effect[selected] = self.action_effects[value.candidate.action][selected]
        unmatched = value.parent.unmatched.copy()
        unmatched[selected] = False
        state = _IncrementalPolicyState(
            entries=value.entries,
            unmatched=unmatched,
            effect=effect,
            weighted_abs_sum=float(np.dot(np.abs(effect), self.weights)),
            score=value.score,
        )
        self.state_cache[value.key] = state
        return state


def _state_actions(state: _PolicyStateLike, panel: OpportunityPanel) -> np.ndarray:
    if isinstance(state, _PolicyState):
        return state.actions
    chosen = np.full(len(panel.frame), CONTROL_ACTION, dtype=object)
    unmatched = np.ones(len(panel.frame), dtype=bool)
    for entry in state.entries:
        selected = unmatched & entry.mask
        chosen[selected] = entry.action
        unmatched[selected] = False
    return chosen


def _state_effect(state: _PolicyStateLike, panel: OpportunityPanel) -> np.ndarray:
    if isinstance(state, _IncrementalPolicyState):
        return state.effect
    control = panel.frame[f"q::{CONTROL_ACTION}"].to_numpy(dtype=float)
    candidate_values = control.copy()
    actions = _state_actions(state, panel)
    for action in panel.actions:
        selected = actions == action
        if selected.any():
            candidate_values[selected] = panel.frame.loc[selected, f"q::{action}"].to_numpy(
                dtype=float
            )
    return candidate_values - control


def _search_rule_state_depth_snapshots(
    panel: OpportunityPanel,
    contract: SearchContract,
    pool: Sequence[_ClauseCandidate],
    *,
    max_clauses: Sequence[int],
    factory: _IncrementalStateFactory,
) -> dict[int, tuple[_IncrementalPolicyState, ...]]:
    """Run one exact rule-list prefix walk and snapshot requested depths."""

    snapshot_depths = tuple(sorted(set(int(value) for value in max_clauses)))
    if not snapshot_depths or snapshot_depths[0] < 1:
        raise ValueError("rule-state snapshot depths must be positive")
    if not pool:
        return {depth: () for depth in snapshot_depths}
    beam = [factory.initial(candidate) for candidate in pool]
    all_states = {state.key: state for state in beam}
    beam = sorted(beam, key=lambda state: (-state.score, state.key))[: contract.beam_width]
    snapshots: dict[int, tuple[_IncrementalPolicyState, ...]] = {}
    if 1 in snapshot_depths:
        snapshots[1] = tuple(
            sorted(all_states.values(), key=lambda state: (-state.score, state.key))
        )

    def error_bound(value: _IncrementalPolicyState | _StateExpansion) -> float:
        return value.error_bound if isinstance(value, _StateExpansion) else 0.0

    def lower_score(value: _IncrementalPolicyState | _StateExpansion) -> float:
        return value.score - error_bound(value)

    def upper_score(value: _IncrementalPolicyState | _StateExpansion) -> float:
        return value.score + error_bound(value)

    def guaranteed_floor(top: _ExactTopK) -> float:
        if len(top) < contract.beam_width:
            return float("-inf")
        return -float(top.worst_rank()[0])

    def nearer_excluded(
        value: _StateExpansion,
        current: _StateExpansion | None,
    ) -> _StateExpansion:
        if current is None or (-upper_score(value), value.key) < (
            -upper_score(current),
            current.key,
        ):
            return value
        return current

    def prune_uncertain(
        values: Sequence[_StateExpansion],
        *,
        floor: float,
        nearest: _StateExpansion | None,
    ) -> tuple[list[_StateExpansion], _StateExpansion | None]:
        retained: list[_StateExpansion] = []
        for value in values:
            if upper_score(value) >= floor:
                retained.append(value)
            else:
                nearest = nearer_excluded(value, nearest)
        return retained, nearest

    for depth in range(2, snapshot_depths[-1] + 1):
        lower_top = _ExactTopK(contract.beam_width)
        exact_top = _ExactTopK(contract.beam_width)
        uncertain: list[_StateExpansion] = []
        nearest_excluded: _StateExpansion | None = None
        offered = 0
        evaluations_since_prune = 0

        for state in beam:
            used = set(state.key)
            for candidate in pool:
                if candidate.key in used:
                    continue
                child = factory.expansion(state, candidate)
                lower_top.offer(child, (-lower_score(child), child.key))
                if error_bound(child) == 0.0:
                    exact_top.offer(child, (-child.score, child.key))
                elif upper_score(child) >= guaranteed_floor(lower_top):
                    uncertain.append(child)
                else:
                    nearest_excluded = nearer_excluded(child, nearest_excluded)
                offered += 1
                evaluations_since_prune += 1
                if evaluations_since_prune >= contract.beam_width * 4:
                    uncertain, nearest_excluded = prune_uncertain(
                        uncertain,
                        floor=guaranteed_floor(lower_top),
                        nearest=nearest_excluded,
                    )
                    evaluations_since_prune = 0
        if not offered:
            family = tuple(sorted(all_states.values(), key=lambda state: (-state.score, state.key)))
            for requested in snapshot_depths:
                if requested >= depth:
                    snapshots[requested] = family
            break
        uncertain, nearest_excluded = prune_uncertain(
            uncertain,
            floor=guaranteed_floor(lower_top),
            nearest=nearest_excluded,
        )
        for child in uncertain:
            exact = factory.exactify(child)
            exact_top.offer(exact, (-exact.score, exact.key))
        exact_survivors = exact_top.values()
        expected_survivors = min(contract.beam_width, offered)
        if len(exact_survivors) != expected_survivors:
            raise AssertionError("exact state ranking did not produce a full beam")
        if nearest_excluded is not None and len(exact_survivors) == contract.beam_width:
            audited = factory.exactify(nearest_excluded)
            boundary = exact_survivors[-1]
            if (-audited.score, audited.key) < (-boundary.score, boundary.key):
                raise AssertionError("incremental score interval changed the beam boundary")
        beam = [factory.materialize(value) for value in exact_survivors]
        all_states.update((state.key, state) for state in beam)
        if depth in snapshot_depths:
            snapshots[depth] = tuple(
                sorted(all_states.values(), key=lambda state: (-state.score, state.key))
            )
    family = tuple(sorted(all_states.values(), key=lambda state: (-state.score, state.key)))
    for depth in snapshot_depths:
        snapshots.setdefault(depth, family)
    return snapshots


@dataclass(frozen=True)
class _SearchDepthSnapshots:
    clause_pools: Mapping[int, tuple[_ClauseCandidate, ...]]
    candidate_families: Mapping[tuple[int, int], tuple[_IncrementalPolicyState, ...]]


def _build_search_depth_snapshots(
    panel: OpportunityPanel,
    contract: SearchContract,
    *,
    max_literals: Sequence[int],
    max_clauses: Sequence[int],
    work_counters: _SearchWorkCounters | None = None,
) -> _SearchDepthSnapshots:
    clause_pools = _generate_clause_depth_snapshots(
        panel,
        contract,
        max_literals=max_literals,
        work_counters=work_counters,
    )
    factory = _IncrementalStateFactory(panel, work_counters=work_counters)
    families: dict[tuple[int, int], tuple[_IncrementalPolicyState, ...]] = {}
    for literal_depth in max_literals:
        snapshots = _search_rule_state_depth_snapshots(
            panel,
            contract,
            clause_pools[int(literal_depth)],
            max_clauses=max_clauses,
            factory=factory,
        )
        for clause_depth in max_clauses:
            families[(int(literal_depth), int(clause_depth))] = snapshots[int(clause_depth)]
    return _SearchDepthSnapshots(clause_pools, families)


@dataclass(frozen=True)
class _BootstrapStateColumn:
    point: float
    draws: np.ndarray


class _BootstrapCache:
    """Exact per-panel bootstrap draws and per-state statistic columns."""

    def __init__(
        self,
        panel: OpportunityPanel,
        *,
        seed: int,
        samples: int,
        work_counters: _SearchWorkCounters | None = None,
    ) -> None:
        if samples < 100:
            raise ValueError("simultaneous bootstrap requires at least 100 draws")
        self.panel = panel
        self.samples = samples
        self.work_counters = work_counters
        frame = panel.frame
        self.weights = frame["campaign_weight"].to_numpy(dtype=float)
        self.full_mask = np.ones(len(frame), dtype=bool)
        days = tuple(sorted(frame["utc_day"].unique()))
        day_values = frame["utc_day"].to_numpy()
        day_indices = {day: np.flatnonzero(day_values == day) for day in days}
        campaign_groups: dict[str, tuple[np.ndarray, Mapping[Any, np.ndarray]]] = {}
        for day in days:
            indices = day_indices[day]
            campaign_values = frame.iloc[indices]["campaign_side_id"].to_numpy()
            campaigns = frame.iloc[indices]["campaign_side_id"].unique()
            campaign_groups[day] = (
                campaigns,
                {campaign: indices[campaign_values == campaign] for campaign in campaigns},
            )
        rng = np.random.default_rng(seed)
        draws: list[np.ndarray] = []
        for _ in range(samples):
            pieces: list[np.ndarray] = []
            for sampled_day in rng.choice(days, size=len(days), replace=True):
                campaigns, indices_by_campaign = campaign_groups[str(sampled_day)]
                sampled_campaigns = rng.choice(
                    campaigns,
                    size=len(campaigns),
                    replace=True,
                )
                pieces.extend(indices_by_campaign[campaign] for campaign in sampled_campaigns)
            draws.append(np.concatenate(pieces) if pieces else np.empty(0, dtype=int))
        self.draws = tuple(draws)
        self.draw_weights = tuple(self.weights[sampled] for sampled in self.draws)
        self.draw_weight_totals = tuple(
            float(sampled_weights.sum()) for sampled_weights in self.draw_weights
        )
        self.columns: dict[
            tuple[tuple[str, tuple[tuple[str, bool], ...]], ...],
            _BootstrapStateColumn,
        ] = {}
        if work_counters is not None:
            work_counters.bootstrap_draws_built += samples

    def column(self, state: _PolicyStateLike) -> _BootstrapStateColumn:
        cached = self.columns.get(state.key)
        if cached is not None:
            return cached
        effect = _state_effect(state, self.panel)
        point = _weighted_mean(
            effect[self.full_mask],
            self.weights[self.full_mask],
        )
        values = np.empty(self.samples, dtype=float)
        for draw, (sampled, sampled_weights, sampled_weight_total) in enumerate(
            zip(
                self.draws,
                self.draw_weights,
                self.draw_weight_totals,
                strict=True,
            )
        ):
            values[draw] = (
                float(np.dot(effect[sampled], sampled_weights) / sampled_weight_total)
                if len(sampled) and sampled_weight_total > 0.0
                else point
            )
        column = _BootstrapStateColumn(point, values)
        self.columns[state.key] = column
        if self.work_counters is not None:
            self.work_counters.bootstrap_state_columns_built += 1
        return column

    def band(
        self,
        candidate_family: Sequence[_PolicyStateLike],
        *,
        confidence: float,
    ) -> tuple[np.ndarray, np.ndarray, float]:
        columns = [self.column(candidate) for candidate in candidate_family]
        points = np.asarray([column.point for column in columns], dtype=float)
        boot = np.empty((self.samples, len(columns)), dtype=float)
        for index, column in enumerate(columns):
            boot[:, index] = column.draws
        centered_max = np.max(np.abs(boot - points[None, :]), axis=1)
        critical = float(np.quantile(centered_max, confidence, method="higher"))
        return points, points - critical, critical


def _state_to_policy(
    state: _PolicyStateLike | None,
    candidate_family: Sequence[_PolicyStateLike],
    panel: OpportunityPanel,
    contract: SearchContract,
    *,
    epsilon_usdc: float,
    bootstrap_samples: int,
    confidence: float,
    fold_identities: tuple[str, ...],
    formal_input_identity_sha256: str,
    bootstrap_cache: _BootstrapCache | None = None,
    work_counters: _SearchWorkCounters | None = None,
    selection_mode: str = DEPLOYMENT_LCB_SELECTION,
    policy_identity: str = IDENTITY,
) -> BooleanCooldownPolicy:
    if selection_mode not in {
        DEPLOYMENT_LCB_SELECTION,
        EXPLORATORY_NONBASELINE_SELECTION,
    }:
        raise ValueError(f"unsupported policy selection mode: {selection_mode}")
    family_payload = {
        "identity": f"{policy_identity}.beam_survivor_family_conditional.v1",
        "side": str(panel.frame["side"].iloc[0]),
        "actions": list(panel.actions),
        "candidate_rule_lists": [list(candidate.key) for candidate in candidate_family],
        "spec_sha256": contract.spec_sha256,
    }
    family_sha = _sha256(family_payload)
    family_id = f"{policy_identity}.{panel.frame['side'].iloc[0]}.{family_sha[:16]}"

    def control_policy(
        *,
        critical: float = 0.0,
        family_lcb: float = float("-inf"),
        selected_candidate_rank: int | None = None,
    ) -> BooleanCooldownPolicy:
        searchable_columns = _searchable_predicate_columns(panel.predicates)
        predicate_schema_sha = _sha256(list(searchable_columns))
        return BooleanCooldownPolicy(
            side=str(panel.frame["side"].iloc[0]),
            rules=(),
            default_action=CONTROL_ACTION,
            predicate_columns=searchable_columns,
            spec_sha256=contract.spec_sha256,
            predicate_artifact_sha256=contract.predicate_artifact_sha256,
            predicate_schema_sha256=predicate_schema_sha,
            outcome_blind_sha256=contract.outcome_blind_sha256,
            outer_fold_source_sha256=contract.outer_fold_source_sha256,
            formal_input_identity_sha256=formal_input_identity_sha256,
            economic_epsilon_usdc=epsilon_usdc,
            training_fold_identities=fold_identities,
            beam_survivor_family_id=family_id,
            beam_survivor_family_sha256=family_sha,
            beam_survivor_family_size=len(candidate_family),
            beam_survivor_family_conditional_critical_usdc=critical,
            beam_survivor_family_conditional_policy_lcb_usdc=family_lcb,
            confidence=confidence,
            bootstrap_samples=bootstrap_samples,
            synthetic_test_only=panel.synthetic_test_only,
            selection_mode=selection_mode,
            policy_identity=policy_identity,
            selected_candidate_rank=selected_candidate_rank,
        )

    if state is None:
        return control_policy()
    control = panel.frame[f"q::{CONTROL_ACTION}"].to_numpy(dtype=float)
    weights = panel.frame["campaign_weight"].to_numpy(dtype=float)
    if bootstrap_cache is None:
        family_effects = [_state_effect(candidate, panel) for candidate in candidate_family]
        family_masks = [np.ones(len(panel.frame), dtype=bool) for _ in candidate_family]
        _, family_lcbs, critical = _nested_cluster_bootstrap_lcbs(
            family_effects,
            family_masks,
            panel,
            seed=contract.seed,
            samples=bootstrap_samples,
            confidence=confidence,
            work_counters=work_counters,
        )
    else:
        _, family_lcbs, critical = bootstrap_cache.band(
            candidate_family,
            confidence=confidence,
        )
    requested_index = next(
        index for index, candidate in enumerate(candidate_family) if candidate.key == state.key
    )
    if selection_mode == DEPLOYMENT_LCB_SELECTION:
        candidate_indices = (requested_index,)
    else:
        candidate_indices = (
            requested_index,
            *(index for index in range(len(candidate_family)) if index != requested_index),
        )

    selected_index: int | None = None
    grouped: list[tuple[str, list[Clause]]] = []
    effects: list[np.ndarray] = []
    masks: list[np.ndarray] = []
    for candidate_index in candidate_indices:
        candidate = candidate_family[candidate_index]
        candidate_grouped: list[tuple[str, list[Clause]]] = []
        for entry in candidate.entries:
            if not candidate_grouped or candidate_grouped[-1][0] != entry.action:
                candidate_grouped.append((entry.action, []))
            candidate_grouped[-1][1].append(entry.clause)
        candidate_effects: list[np.ndarray] = []
        candidate_masks: list[np.ndarray] = []
        unmatched = np.ones(len(panel.frame), dtype=bool)
        for action, clauses in candidate_grouped:
            raw_mask = np.zeros(len(panel.frame), dtype=bool)
            for clause in clauses:
                raw_mask |= clause.evaluate(panel.predicates)
            mask = raw_mask & unmatched
            if not _support_ok(mask, panel, contract):
                break
            candidate_effects.append(panel.frame[f"q::{action}"].to_numpy(dtype=float) - control)
            candidate_masks.append(mask)
            unmatched[mask] = False
        else:
            selected_index = candidate_index
            grouped = candidate_grouped
            effects = candidate_effects
            masks = candidate_masks
            break

    if selected_index is None:
        return control_policy(critical=critical)
    family_lcb = float(family_lcbs[selected_index])
    if selection_mode == DEPLOYMENT_LCB_SELECTION and family_lcb <= epsilon_usdc:
        return control_policy(
            critical=critical,
            family_lcb=family_lcb,
            selected_candidate_rank=selected_index + 1,
        )
    points = np.asarray(
        [
            _weighted_mean(effect[mask], weights[mask])
            for effect, mask in zip(effects, masks, strict=True)
        ]
    )
    rules = tuple(
        Rule(
            action=action,
            clauses=tuple(clauses),
            conditional_point_uplift_usdc=float(point),
        )
        for (action, clauses), point in zip(grouped, points, strict=True)
    )
    searchable_columns = _searchable_predicate_columns(panel.predicates)
    predicate_schema_sha = _sha256(list(searchable_columns))
    return BooleanCooldownPolicy(
        side=str(panel.frame["side"].iloc[0]),
        rules=rules,
        default_action=CONTROL_ACTION,
        predicate_columns=searchable_columns,
        spec_sha256=contract.spec_sha256,
        predicate_artifact_sha256=contract.predicate_artifact_sha256,
        predicate_schema_sha256=predicate_schema_sha,
        outcome_blind_sha256=contract.outcome_blind_sha256,
        outer_fold_source_sha256=contract.outer_fold_source_sha256,
        formal_input_identity_sha256=formal_input_identity_sha256,
        economic_epsilon_usdc=epsilon_usdc,
        training_fold_identities=fold_identities,
        beam_survivor_family_id=family_id,
        beam_survivor_family_sha256=family_sha,
        beam_survivor_family_size=len(candidate_family),
        beam_survivor_family_conditional_critical_usdc=critical,
        beam_survivor_family_conditional_policy_lcb_usdc=family_lcb,
        confidence=confidence,
        bootstrap_samples=bootstrap_samples,
        synthetic_test_only=panel.synthetic_test_only,
        selection_mode=selection_mode,
        policy_identity=policy_identity,
        selected_candidate_rank=selected_index + 1,
    )


@dataclass(frozen=True)
class _OptimizedGridResult:
    snapshots: _SearchDepthSnapshots
    policy_templates: Mapping[tuple[int, int], BooleanCooldownPolicy]
    work_counters: _SearchWorkCounters


def _fit_normalized_complexity_grid(
    panel: OpportunityPanel,
    contract: SearchContract,
    *,
    economic_epsilon_usdc: float,
    bootstrap_samples: int,
    confidence: float,
    formal_input_identity_sha256: str,
    max_literals: Sequence[int] | None = None,
    max_clauses: Sequence[int] | None = None,
    work_counters: _SearchWorkCounters | None = None,
    selection_mode: str = DEPLOYMENT_LCB_SELECTION,
    policy_identity: str = IDENTITY,
) -> _OptimizedGridResult:
    """Fit the frozen grid with exact clause, state, and bootstrap reuse."""

    literal_depths = tuple(max_literals or contract.max_literals_per_clause)
    clause_depths = tuple(max_clauses or contract.max_clauses)
    counters = work_counters or _SearchWorkCounters()
    snapshots = _build_search_depth_snapshots(
        panel,
        contract,
        max_literals=literal_depths,
        max_clauses=clause_depths,
        work_counters=counters,
    )
    bootstrap_cache = _BootstrapCache(
        panel,
        seed=contract.seed,
        samples=bootstrap_samples,
        work_counters=counters,
    )
    templates: dict[tuple[int, int], BooleanCooldownPolicy] = {}
    for literal_depth in literal_depths:
        for clause_depth in clause_depths:
            key = (int(literal_depth), int(clause_depth))
            family = snapshots.candidate_families[key]
            templates[key] = _state_to_policy(
                family[0] if family else None,
                family,
                panel,
                contract,
                epsilon_usdc=economic_epsilon_usdc,
                bootstrap_samples=bootstrap_samples,
                confidence=confidence,
                fold_identities=(),
                formal_input_identity_sha256=formal_input_identity_sha256,
                bootstrap_cache=bootstrap_cache,
                selection_mode=selection_mode,
                policy_identity=policy_identity,
            )
    return _OptimizedGridResult(snapshots, templates, counters)


def _apply_normalized_policy(
    policy: BooleanCooldownPolicy,
    panel: OpportunityPanel,
    *,
    side: str,
) -> pd.DataFrame:
    if policy.side != side:
        raise ValueError("policy side does not match the execution panel")
    actions = policy.choose(panel.predicates)
    values = np.empty(len(actions), dtype=float)
    control = panel.frame[f"q::{CONTROL_ACTION}"].to_numpy(dtype=float)
    for action in panel.actions:
        selected = actions == action
        values[selected] = panel.frame.loc[selected, f"q::{action}"].to_numpy(dtype=float)
    return pd.DataFrame(
        {
            "opportunity_id": panel.frame["opportunity_id"],
            "side": side,
            "utc_day": panel.frame["utc_day"],
            "campaign_side_id": panel.frame["campaign_side_id"],
            "campaign_weight": panel.frame["campaign_weight"],
            "chosen_action": actions,
            "chosen_value_usdc": values,
            "control_value_usdc": control,
            "policy_minus_control_usdc": values - control,
            "policy_sha256": policy.artifact()["policy_sha256"],
        }
    )


def fit_side_policy(
    panel: pd.DataFrame,
    *,
    side: str,
    economic_epsilon_usdc: float = 0.0,
    max_literals_per_clause: int | None = None,
    max_clauses: int | None = None,
    bootstrap_samples: int = 500,
    confidence: float = 0.95,
    training_fold_identities: Sequence[str] = (),
    synthetic_mode: bool = False,
    formal_input_identity: FormalInputIdentity | None = None,
    selection_mode: str = DEPLOYMENT_LCB_SELECTION,
    policy_identity: str = IDENTITY,
) -> tuple[BooleanCooldownPolicy, PanelAudit]:
    """Fit one side only using a deterministic frozen-grid beam search."""

    if not np.isfinite(economic_epsilon_usdc) or economic_epsilon_usdc < 0.0:
        raise ValueError("economic epsilon must be finite and non-negative")
    synthetic = bool(synthetic_mode)
    if not synthetic and economic_epsilon_usdc != 0.0:
        raise ValueError("formal economic epsilon is frozen at zero")
    contract = load_frozen_search_contract()
    if synthetic:
        formal_identity_sha = "synthetic_test_only"
    else:
        formal_identity = _validate_formal_input_identity(
            formal_input_identity,
            contract,
        )
        formal_identity_sha = formal_identity.artifact()["formal_input_identity_sha256"]
    if not synthetic and (
        confidence != contract.formal_confidence
        or bootstrap_samples != contract.formal_bootstrap_samples
    ):
        raise ValueError("formal confidence/bootstrap are frozen at 0.95/500")
    max_literals = max_literals_per_clause or max(contract.max_literals_per_clause)
    clause_limit = max_clauses or max(contract.max_clauses)
    if max_literals not in contract.max_literals_per_clause:
        raise ValueError("max_literals_per_clause is outside the frozen grid")
    if clause_limit not in contract.max_clauses:
        raise ValueError("max_clauses is outside the frozen grid")
    normalized = normalize_joint_panel(
        panel,
        side=side,
        contract=contract,
        synthetic_mode=synthetic,
    )
    optimized = _fit_normalized_complexity_grid(
        normalized,
        contract,
        economic_epsilon_usdc=economic_epsilon_usdc,
        bootstrap_samples=bootstrap_samples,
        confidence=confidence,
        formal_input_identity_sha256=formal_identity_sha,
        max_literals=(max_literals,),
        max_clauses=(clause_limit,),
        selection_mode=selection_mode,
        policy_identity=policy_identity,
    )
    policy = replace(
        optimized.policy_templates[(max_literals, clause_limit)],
        training_fold_identities=tuple(str(value) for value in training_fold_identities),
    )
    return policy, normalized.audit


@dataclass(frozen=True)
class InnerFold:
    fold: int
    train_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    test_days: tuple[str, ...]
    first_test_assignment_ts_ns: int


@dataclass(frozen=True)
class OuterFold:
    fold: int
    train_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    test_days: tuple[str, ...]
    first_test_assignment_ts_ns: int
    inner_folds: tuple[InnerFold, ...]


@dataclass(frozen=True)
class NestedChronologicalResult:
    """Development-only outer OOF execution of inner-frozen policies."""

    oof: pd.DataFrame
    complexity_evidence: pd.DataFrame
    chronology_audit: pd.DataFrame
    outer_policy_artifacts: tuple[dict[str, Any], ...]
    panel_audit: PanelAudit
    permissions: Mapping[str, bool]


@dataclass(frozen=True)
class OuterOOFGate:
    """Untouched outer-OOF economic gate for one side."""

    side: str
    point_uplift_usdc: float
    lower_confidence_bound_usdc: float
    economic_epsilon_usdc: float
    confidence: float
    bootstrap_samples: int
    outer_folds: tuple[int, ...]
    test_days: tuple[str, ...]
    campaign_count: int
    passed: bool
    synthetic_test_only: bool

    def artifact(self) -> dict[str, Any]:
        body = {
            "schema_version": f"{IDENTITY}.outer_oof_gate.v1",
            "side": self.side,
            "point_uplift_usdc": self.point_uplift_usdc,
            "lower_confidence_bound_usdc": self.lower_confidence_bound_usdc,
            "economic_epsilon_usdc": self.economic_epsilon_usdc,
            "confidence": self.confidence,
            "bootstrap_samples": self.bootstrap_samples,
            "outer_folds": list(self.outer_folds),
            "test_days": list(self.test_days),
            "campaign_count": self.campaign_count,
            "passed": self.passed,
            "outer_outcomes_used_for_rule_discovery": False,
            "authority": "outer_oof_gate_only_no_action_or_live_authority",
            "synthetic_test_only": self.synthetic_test_only,
        }
        return {**body, "outer_oof_gate_sha256": _sha256(body)}


@dataclass(frozen=True)
class FinalComplexitySelection:
    max_literals_per_clause: int
    max_clauses: int
    campaign_weighted_inner_oof_usdc: float
    one_standard_error_cutoff_usdc: float
    evidence_sha256: str

    def artifact(self) -> dict[str, Any]:
        return {
            "max_literals_per_clause": self.max_literals_per_clause,
            "max_clauses": self.max_clauses,
            "campaign_weighted_inner_oof_usdc": (self.campaign_weighted_inner_oof_usdc),
            "one_standard_error_cutoff_usdc": self.one_standard_error_cutoff_usdc,
            "selection_source": "inner_chronological_evidence_only",
            "outer_oof_outcomes_used_for_complexity_selection": False,
            "evidence_sha256": self.evidence_sha256,
        }


@dataclass(frozen=True)
class FinalSidePolicyFreeze:
    """Exactly one post-gate full-Development policy for one side."""

    side: str
    policy: BooleanCooldownPolicy
    outer_oof_gate: OuterOOFGate
    complexity: FinalComplexitySelection

    def artifact(self) -> dict[str, Any]:
        policy_artifact = self.policy.artifact()
        gate_artifact = self.outer_oof_gate.artifact()
        body = {
            "schema_version": f"{IDENTITY}.final_side_policy_freeze.v1",
            "identity": IDENTITY,
            "side": self.side,
            "policy": policy_artifact,
            "outer_oof_gate": gate_artifact,
            "complexity": self.complexity.artifact(),
            "policy_count_for_side": 1,
            "refit_population": "all_eligible_frozen_Development_after_outer_oof_gate",
            "permissions": {
                "action_authorized": False,
                "live_authorized": False,
                "f09_registration_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        }
        return {**body, "final_side_policy_freeze_sha256": _sha256(body)}


def _calendar_embargo(days: Sequence[str], first_test_day: str, count: int) -> tuple[str, ...]:
    first = pd.Timestamp(first_test_day)
    forbidden = {str((first - pd.Timedelta(days=offset)).date()) for offset in range(1, count + 1)}
    return tuple(day for day in days if day in forbidden)


def build_nested_chronological_folds(
    panel: pd.DataFrame,
    *,
    side: str,
    synthetic_mode: bool = False,
) -> tuple[OuterFold, ...]:
    """Build frozen formal folds or an explicit synthetic 16 + 4x6 design."""

    contract = load_frozen_search_contract()
    normalized = normalize_joint_panel(
        panel,
        side=side,
        contract=contract,
        synthetic_mode=synthetic_mode,
    )
    frame = normalized.frame
    days = tuple(sorted(frame["utc_day"].unique()))
    required = contract.outer_initial_history_days + (
        contract.outer_fold_count * contract.outer_test_days
    )
    if len(days) != required:
        raise ValueError(f"chronology requires exactly {required} ordered Development days")
    if not synthetic_mode and days != contract.ordered_development_days:
        raise ValueError("formal chronology differs from the exact frozen ordered 40 days")
    folds: list[OuterFold] = []
    for fold_index in range(contract.outer_fold_count):
        if synthetic_mode:
            test_start = contract.outer_initial_history_days + fold_index * contract.outer_test_days
            test_days = days[test_start : test_start + contract.outer_test_days]
            prior_days = days[:test_start]
            embargo = _calendar_embargo(
                prior_days,
                test_days[0],
                contract.outer_embargo_calendar_days,
            )
            candidate_train = tuple(day for day in prior_days if day not in embargo)
        else:
            frozen = contract.frozen_outer_folds[fold_index]
            test_days = frozen.test_days
            embargo = (frozen.embargo_day,)
            candidate_train = frozen.fit_day_candidates
        first_test_ts = int(frame.loc[frame["utc_day"].eq(test_days[0]), "assignment_ts_ns"].min())
        purged_rows = frame.loc[
            frame["utc_day"].isin(candidate_train) & frame["washout_ts_ns"].lt(first_test_ts)
        ]
        train_days = tuple(day for day in candidate_train if day in set(purged_rows["utc_day"]))
        if len(train_days) < 9:
            raise ValueError("outer chronology has fewer than nine admissible inner days")
        inner_candidate_days = train_days[-9:]
        inner_folds: list[InnerFold] = []
        for inner_index in range(contract.inner_fold_count):
            start = inner_index * contract.inner_test_days
            inner_test = inner_candidate_days[start : start + contract.inner_test_days]
            inner_first_ts = int(
                frame.loc[frame["utc_day"].eq(inner_test[0]), "assignment_ts_ns"].min()
            )
            earlier = tuple(day for day in train_days if day < inner_test[0])
            inner_embargo = _calendar_embargo(earlier, inner_test[0], 1)
            inner_train_candidates = tuple(day for day in earlier if day not in inner_embargo)
            inner_rows = frame.loc[
                frame["utc_day"].isin(inner_train_candidates)
                & frame["washout_ts_ns"].lt(inner_first_ts)
            ]
            inner_train = tuple(
                day for day in inner_train_candidates if day in set(inner_rows["utc_day"])
            )
            if not inner_train or max(inner_train) >= min(inner_test):
                raise ValueError("inner chronology has future training leakage")
            inner_folds.append(
                InnerFold(
                    fold=inner_index,
                    train_days=inner_train,
                    embargo_days=inner_embargo,
                    test_days=inner_test,
                    first_test_assignment_ts_ns=inner_first_ts,
                )
            )
        if not train_days or max(train_days) >= min(test_days):
            raise ValueError("outer chronology has future training leakage")
        folds.append(
            OuterFold(
                fold=fold_index,
                train_days=train_days,
                embargo_days=embargo,
                test_days=test_days,
                first_test_assignment_ts_ns=first_test_ts,
                inner_folds=tuple(inner_folds),
            )
        )
    prior_test_max: str | None = None
    for fold in folds:
        if prior_test_max is not None and min(fold.test_days) <= prior_test_max:
            raise ValueError("outer test folds overlap or are out of order")
        prior_test_max = max(fold.test_days)
    return tuple(folds)


def apply_frozen_policy(
    policy: BooleanCooldownPolicy,
    panel: pd.DataFrame,
    *,
    side: str,
    synthetic_mode: bool = False,
    formal_input_identity: FormalInputIdentity | None = None,
) -> pd.DataFrame:
    """Execute an already-frozen policy without fitting or changing it."""

    contract = load_frozen_search_contract()
    if synthetic_mode:
        expected_identity_sha = "synthetic_test_only"
    else:
        identity = _validate_formal_input_identity(formal_input_identity, contract)
        expected_identity_sha = identity.artifact()["formal_input_identity_sha256"]
    if policy.formal_input_identity_sha256 != expected_identity_sha:
        raise ValueError("policy and execution formal-input identities differ")
    normalized = normalize_joint_panel(
        panel,
        side=side,
        contract=contract,
        synthetic_mode=synthetic_mode,
    )
    return _apply_normalized_policy(policy, normalized, side=side)


def _subset_panel(
    panel: pd.DataFrame,
    *,
    days: Sequence[str],
    washout_before_ns: int | None = None,
) -> pd.DataFrame:
    selected = panel.loc[panel["utc_day"].astype(str).isin(tuple(days))].copy()
    if washout_before_ns is not None:
        # Keep complete long-form opportunity groups only.  The washout value is
        # common across all arms by contract, so filtering here cannot create a
        # reduced complete-case panel.
        selected = selected.loc[
            pd.to_numeric(selected["washout_ts_ns"], errors="raise").lt(washout_before_ns)
        ].copy()
    selected.attrs.update(panel.attrs)
    return selected


def _weighted_policy_uplift(executed: pd.DataFrame) -> float:
    return _weighted_mean(
        executed["policy_minus_control_usdc"].to_numpy(dtype=float),
        executed["campaign_weight"].to_numpy(dtype=float),
    )


def run_nested_chronological_oof(
    panel: pd.DataFrame,
    *,
    side: str,
    economic_epsilon_usdc: float = 0.0,
    bootstrap_samples: int = 500,
    confidence: float = 0.95,
    synthetic_mode: bool = False,
    formal_input_identity: FormalInputIdentity | None = None,
    selection_mode: str = DEPLOYMENT_LCB_SELECTION,
    policy_identity: str = IDENTITY,
    outer_fold_indices: Sequence[int] | None = None,
) -> NestedChronologicalResult:
    """Run frozen-grid inner discovery and untouched outer policy execution.

    For each outer fold, every complexity pair is assessed on all three inner
    chronological test blocks.  The one-standard-error rule selects complexity.
    Inner OOF selects only the complexity.  A policy at that frozen complexity
    is refitted on every admissible outer-training row, then applied once to the
    outer test block; outer outcomes are never passed to fit or selection.
    """

    contract = load_frozen_search_contract()
    requested_outer_folds = (
        tuple(range(contract.outer_fold_count))
        if outer_fold_indices is None
        else tuple(int(value) for value in outer_fold_indices)
    )
    if (
        not requested_outer_folds
        or len(set(requested_outer_folds)) != len(requested_outer_folds)
        or any(value < 0 or value >= contract.outer_fold_count for value in requested_outer_folds)
    ):
        raise ValueError("requested outer folds must be unique members of the frozen fold set")
    if not np.isfinite(economic_epsilon_usdc) or economic_epsilon_usdc < 0.0:
        raise ValueError("economic epsilon must be finite and non-negative")
    if not synthetic_mode and economic_epsilon_usdc != 0.0:
        raise ValueError("formal economic epsilon is frozen at zero")
    if not synthetic_mode and (
        confidence != contract.formal_confidence
        or bootstrap_samples != contract.formal_bootstrap_samples
    ):
        raise ValueError("formal confidence/bootstrap are frozen at 0.95/500")
    if synthetic_mode:
        bound_identity = None
        formal_identity_sha = "synthetic_test_only"
    else:
        bound_identity = _validate_formal_input_identity(
            formal_input_identity,
            contract,
        )
        formal_identity_sha = bound_identity.artifact()["formal_input_identity_sha256"]
    normalized = normalize_joint_panel(
        panel,
        side=side,
        contract=contract,
        synthetic_mode=synthetic_mode,
    )
    folds = build_nested_chronological_folds(
        panel,
        side=side,
        synthetic_mode=synthetic_mode,
    )
    folds = tuple(fold for fold in folds if fold.fold in requested_outer_folds)
    if tuple(fold.fold for fold in folds) != requested_outer_folds:
        raise ValueError("requested outer folds must preserve frozen chronological order")
    evidence_rows: list[dict[str, Any]] = []
    chronology_rows: list[dict[str, Any]] = []
    oof_frames: list[pd.DataFrame] = []
    artifacts: list[dict[str, Any]] = []
    normalized_subsets: dict[tuple[tuple[str, ...], int | None], OpportunityPanel] = {}
    grid_templates: dict[
        tuple[tuple[str, ...], int | None],
        Mapping[tuple[int, int], BooleanCooldownPolicy],
    ] = {}

    def normalized_subset(
        days: Sequence[str],
        washout_before_ns: int | None = None,
    ) -> OpportunityPanel:
        key = (tuple(str(day) for day in days), washout_before_ns)
        cached = normalized_subsets.get(key)
        if cached is not None:
            return cached
        subset = _subset_panel(
            panel,
            days=days,
            washout_before_ns=washout_before_ns,
        )
        cached = normalize_joint_panel(
            subset,
            side=side,
            contract=contract,
            synthetic_mode=synthetic_mode,
        )
        normalized_subsets[key] = cached
        return cached

    for outer in folds:
        complexity_results: list[dict[str, Any]] = []
        inner_contexts: list[
            tuple[
                InnerFold,
                Mapping[tuple[int, int], BooleanCooldownPolicy],
                OpportunityPanel,
            ]
        ] = []
        for inner in outer.inner_folds:
            train_key = (
                tuple(str(day) for day in inner.train_days),
                inner.first_test_assignment_ts_ns,
            )
            templates = grid_templates.get(train_key)
            if templates is None:
                train = normalized_subset(
                    inner.train_days,
                    inner.first_test_assignment_ts_ns,
                )
                templates = _fit_normalized_complexity_grid(
                    train,
                    contract,
                    economic_epsilon_usdc=economic_epsilon_usdc,
                    bootstrap_samples=bootstrap_samples,
                    confidence=confidence,
                    formal_input_identity_sha256=formal_identity_sha,
                    selection_mode=selection_mode,
                    policy_identity=policy_identity,
                ).policy_templates
                grid_templates[train_key] = templates
            test = normalized_subset(inner.test_days)
            inner_contexts.append((inner, templates, test))
        for max_literals in contract.max_literals_per_clause:
            for max_clauses in contract.max_clauses:
                inner_executions: list[pd.DataFrame] = []
                final_inner_policy: BooleanCooldownPolicy | None = None
                for inner, templates, test in inner_contexts:
                    fold_id = f"outer{outer.fold}.inner{inner.fold}.L{max_literals}.C{max_clauses}"
                    fitted = replace(
                        templates[(max_literals, max_clauses)],
                        training_fold_identities=(fold_id,),
                    )
                    executed = _apply_normalized_policy(
                        fitted,
                        test,
                        side=side,
                    )
                    inner_executions.append(executed)
                    final_inner_policy = fitted
                    chronology_rows.append(
                        {
                            "outer_fold": outer.fold,
                            "inner_fold": inner.fold,
                            "max_literals_per_clause": max_literals,
                            "max_clauses": max_clauses,
                            "train_max_day": max(inner.train_days),
                            "test_min_day": min(inner.test_days),
                            "future_training_leakage": max(inner.train_days)
                            >= min(inner.test_days),
                            "outer_outcomes_used_for_fit": False,
                        }
                    )
                if final_inner_policy is None or not inner_executions:
                    raise ValueError("inner chronology produced no frozen policy evidence")
                inner_oof = pd.concat(inner_executions, ignore_index=True)
                mean = _weighted_policy_uplift(inner_oof)
                day_values = np.asarray(
                    [
                        _weighted_policy_uplift(day_frame)
                        for _, day_frame in inner_oof.groupby("utc_day", sort=True)
                    ],
                    dtype=float,
                )
                standard_error = float(day_values.std(ddof=1) / np.sqrt(len(day_values)))
                result = {
                    "outer_fold": outer.fold,
                    "max_literals_per_clause": max_literals,
                    "max_clauses": max_clauses,
                    "inner_oof_mean_usdc": mean,
                    "inner_oof_standard_error_usdc": standard_error,
                    "inner_oof_day_count": len(day_values),
                    "inner_oof_campaign_weight": float(inner_oof["campaign_weight"].sum()),
                    "inner_selection_objective": "global_campaign_weighted",
                    "frozen_policy_sha256": final_inner_policy.artifact()["policy_sha256"],
                    "non_control_action_rate": float(
                        inner_oof["chosen_action"].ne(CONTROL_ACTION).mean()
                    ),
                    "candidate_nonbaseline": bool(
                        inner_oof["chosen_action"].ne(CONTROL_ACTION).any()
                    ),
                }
                complexity_results.append(result)
        selection_pool = complexity_results
        if selection_mode == EXPLORATORY_NONBASELINE_SELECTION:
            selection_pool = [row for row in complexity_results if row["candidate_nonbaseline"]]
            if not selection_pool:
                raise ValueError(
                    "exploratory outer fold has no support-valid nonbaseline candidate"
                )
        best = max(selection_pool, key=lambda row: row["inner_oof_mean_usdc"])
        cutoff = best["inner_oof_mean_usdc"] - best["inner_oof_standard_error_usdc"]
        tied = [row for row in selection_pool if row["inner_oof_mean_usdc"] >= cutoff]
        selected = min(
            tied,
            key=lambda row: (
                row["max_clauses"],
                row["max_literals_per_clause"],
                row["frozen_policy_sha256"],
            ),
        )
        for row in complexity_results:
            evidence_rows.append(
                {
                    **row,
                    "one_standard_error_cutoff_usdc": cutoff,
                    "selected": row is selected,
                }
            )
        selected_key = (
            int(selected["max_literals_per_clause"]),
            int(selected["max_clauses"]),
        )
        # Inner OOF selects only the frozen complexity.  The policy itself is
        # then refitted on every admissible outer training day after the same
        # embargo and label-end purge.  Outer outcomes remain untouched.
        outer_train = _subset_panel(
            panel,
            days=outer.train_days,
            washout_before_ns=outer.first_test_assignment_ts_ns,
        )
        frozen_policy, _ = fit_side_policy(
            outer_train,
            side=side,
            economic_epsilon_usdc=economic_epsilon_usdc,
            max_literals_per_clause=selected_key[0],
            max_clauses=selected_key[1],
            bootstrap_samples=bootstrap_samples,
            confidence=confidence,
            training_fold_identities=(f"outer{outer.fold}.full_train",),
            synthetic_mode=synthetic_mode,
            formal_input_identity=bound_identity,
            selection_mode=selection_mode,
            policy_identity=policy_identity,
        )
        if selection_mode == EXPLORATORY_NONBASELINE_SELECTION and not frozen_policy.rules:
            raise ValueError("exploratory outer policy was cleared before untouched OOF execution")
        outer_test = normalized_subset(outer.test_days)
        executed = _apply_normalized_policy(
            frozen_policy,
            outer_test,
            side=side,
        )
        executed.insert(0, "outer_fold", outer.fold)
        oof_frames.append(executed)
        artifacts.append(frozen_policy.artifact())
    chronology = pd.DataFrame(chronology_rows)
    if chronology.empty or chronology["future_training_leakage"].any():
        raise ValueError("nested chronology failed closed")
    if chronology["outer_outcomes_used_for_fit"].any():
        raise AssertionError("outer outcomes entered model discovery")
    return NestedChronologicalResult(
        oof=pd.concat(oof_frames, ignore_index=True),
        complexity_evidence=pd.DataFrame(evidence_rows),
        chronology_audit=chronology,
        outer_policy_artifacts=tuple(artifacts),
        panel_audit=normalized.audit,
        permissions={
            "action_authorized": False,
            "live_authorized": False,
            "f09_registration_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    )


def _outer_oof_cluster_lcb(
    oof: pd.DataFrame,
    *,
    seed: int,
    samples: int,
    confidence: float,
) -> tuple[float, float]:
    weights = oof["campaign_weight"].to_numpy(dtype=float)
    effects = oof["policy_minus_control_usdc"].to_numpy(dtype=float)
    point = _weighted_mean(effects, weights)
    days = tuple(sorted(oof["utc_day"].astype(str).unique()))
    day_indices = {
        day: np.flatnonzero(oof["utc_day"].astype(str).to_numpy() == day) for day in days
    }
    rng = np.random.default_rng(seed)
    draws = np.empty(samples, dtype=float)
    for draw in range(samples):
        pieces: list[np.ndarray] = []
        for sampled_day in rng.choice(days, size=len(days), replace=True):
            indices = day_indices[str(sampled_day)]
            campaigns = oof.iloc[indices]["campaign_side_id"].astype(str).unique()
            for campaign in rng.choice(campaigns, size=len(campaigns), replace=True):
                selected = indices[
                    oof.iloc[indices]["campaign_side_id"].astype(str).to_numpy() == str(campaign)
                ]
                pieces.append(selected)
        sampled = np.concatenate(pieces) if pieces else np.empty(0, dtype=int)
        draws[draw] = _weighted_mean(effects[sampled], weights[sampled])
    critical = float(np.quantile(np.abs(draws - point), confidence, method="higher"))
    return point, point - critical


def evaluate_outer_oof_gate(
    result: NestedChronologicalResult,
    *,
    side: str,
    economic_epsilon_usdc: float = 0.0,
    confidence: float = FORMAL_CONFIDENCE,
    bootstrap_samples: int = FORMAL_BOOTSTRAP_SAMPLES,
    synthetic_mode: bool = False,
) -> OuterOOFGate:
    """Evaluate untouched outer OOF without fitting or loading any data."""

    if side not in {"BUY", "SELL"}:
        raise ValueError("outer OOF side must be BUY or SELL")
    if not np.isfinite(economic_epsilon_usdc) or economic_epsilon_usdc < 0.0:
        raise ValueError("outer OOF economic epsilon must be finite and non-negative")
    contract = load_frozen_search_contract()
    if not synthetic_mode and (
        confidence != contract.formal_confidence
        or bootstrap_samples != contract.formal_bootstrap_samples
        or economic_epsilon_usdc != 0.0
    ):
        raise ValueError("formal outer OOF gate is frozen at epsilon=0, 0.95/500")
    if any(
        bool(result.permissions.get(field, True))
        for field in (
            "action_authorized",
            "live_authorized",
            "f09_registration_authorized",
            "validation_read",
            "sealed_holdout_read",
        )
    ):
        raise ValueError("outer OOF result carries forbidden authority or data reads")
    oof = result.oof.copy(deep=True)
    required = {
        "outer_fold",
        "side",
        "utc_day",
        "campaign_side_id",
        "campaign_weight",
        "policy_minus_control_usdc",
    }
    missing = required - set(oof.columns)
    if missing:
        raise ValueError(f"outer OOF is missing columns: {sorted(missing)}")
    if set(oof["side"].astype(str)) != {side}:
        raise ValueError("outer OOF pools sides or differs from the requested side")
    folds = tuple(
        int(value) for value in sorted(pd.to_numeric(oof["outer_fold"], errors="raise").unique())
    )
    if folds != tuple(range(contract.outer_fold_count)):
        raise ValueError("outer OOF must contain each of the four frozen folds once")
    days = tuple(sorted(oof["utc_day"].astype(str).unique()))
    if not synthetic_mode:
        expected_days = tuple(day for fold in contract.frozen_outer_folds for day in fold.test_days)
        if days != expected_days:
            raise ValueError("outer OOF test-day union differs from the frozen folds")
    if not np.isfinite(
        oof[["campaign_weight", "policy_minus_control_usdc"]].to_numpy(dtype=float)
    ).all():
        raise ValueError("outer OOF contains non-finite weights or values")
    point, lcb = _outer_oof_cluster_lcb(
        oof,
        seed=contract.seed,
        samples=bootstrap_samples,
        confidence=confidence,
    )
    return OuterOOFGate(
        side=side,
        point_uplift_usdc=point,
        lower_confidence_bound_usdc=lcb,
        economic_epsilon_usdc=economic_epsilon_usdc,
        confidence=confidence,
        bootstrap_samples=bootstrap_samples,
        outer_folds=folds,
        test_days=days,
        campaign_count=int(oof["campaign_side_id"].nunique()),
        passed=bool(lcb > economic_epsilon_usdc),
        synthetic_test_only=synthetic_mode,
    )


def select_final_complexity_from_inner_evidence(
    complexity_evidence: pd.DataFrame,
) -> FinalComplexitySelection:
    """Select one final complexity using inner evidence and no outer outcomes."""

    required = {
        "outer_fold",
        "max_literals_per_clause",
        "max_clauses",
        "inner_oof_mean_usdc",
        "inner_oof_standard_error_usdc",
        "inner_oof_campaign_weight",
        "inner_selection_objective",
    }
    missing = required - set(complexity_evidence.columns)
    if missing:
        raise ValueError(f"complexity evidence is missing columns: {sorted(missing)}")
    evidence = complexity_evidence.copy(deep=True)
    if set(evidence["inner_selection_objective"].astype(str)) != {"global_campaign_weighted"}:
        raise ValueError("final complexity requires global campaign-weighted evidence")
    rows: list[dict[str, Any]] = []
    for (literals, clauses), group in evidence.groupby(
        ["max_literals_per_clause", "max_clauses"], sort=True
    ):
        if set(pd.to_numeric(group["outer_fold"], errors="raise").astype(int)) != set(range(4)):
            raise ValueError("each final complexity must have all four inner-evidence folds")
        weights = pd.to_numeric(group["inner_oof_campaign_weight"], errors="raise").to_numpy(
            dtype=float
        )
        means = pd.to_numeric(group["inner_oof_mean_usdc"], errors="raise").to_numpy(dtype=float)
        errors = pd.to_numeric(group["inner_oof_standard_error_usdc"], errors="raise").to_numpy(
            dtype=float
        )
        if (
            not np.isfinite(weights).all()
            or not np.isfinite(means).all()
            or not np.isfinite(errors).all()
            or np.any(weights <= 0.0)
        ):
            raise ValueError("final complexity evidence contains invalid values")
        total = float(weights.sum())
        mean = float(np.dot(weights, means) / total)
        standard_error = float(np.sqrt(np.sum((weights * errors) ** 2)) / total)
        rows.append(
            {
                "max_literals_per_clause": int(literals),
                "max_clauses": int(clauses),
                "mean": mean,
                "standard_error": standard_error,
            }
        )
    best = max(rows, key=lambda row: row["mean"])
    cutoff = float(best["mean"] - best["standard_error"])
    tied = [row for row in rows if row["mean"] >= cutoff]
    selected = min(
        tied,
        key=lambda row: (
            row["max_clauses"],
            row["max_literals_per_clause"],
        ),
    )
    evidence_records = (
        evidence.sort_values(["outer_fold", "max_literals_per_clause", "max_clauses"])
        .loc[:, sorted(required)]
        .to_dict(orient="records")
    )
    return FinalComplexitySelection(
        max_literals_per_clause=int(selected["max_literals_per_clause"]),
        max_clauses=int(selected["max_clauses"]),
        campaign_weighted_inner_oof_usdc=float(selected["mean"]),
        one_standard_error_cutoff_usdc=cutoff,
        evidence_sha256=_sha256(evidence_records),
    )


def freeze_final_side_policy(
    panel: pd.DataFrame,
    result: NestedChronologicalResult,
    *,
    side: str,
    formal_input_identity: FormalInputIdentity | None = None,
    economic_epsilon_usdc: float = 0.0,
    confidence: float = FORMAL_CONFIDENCE,
    bootstrap_samples: int = FORMAL_BOOTSTRAP_SAMPLES,
    synthetic_mode: bool = False,
) -> FinalSidePolicyFreeze:
    """Freeze one side policy after, and only after, its outer OOF gate passes."""

    gate = evaluate_outer_oof_gate(
        result,
        side=side,
        economic_epsilon_usdc=economic_epsilon_usdc,
        confidence=confidence,
        bootstrap_samples=bootstrap_samples,
        synthetic_mode=synthetic_mode,
    )
    if not gate.passed:
        raise ValueError(f"{side} outer OOF gate did not pass; no final policy may be frozen")
    complexity = select_final_complexity_from_inner_evidence(result.complexity_evidence)
    policy, _ = fit_side_policy(
        panel,
        side=side,
        economic_epsilon_usdc=economic_epsilon_usdc,
        max_literals_per_clause=complexity.max_literals_per_clause,
        max_clauses=complexity.max_clauses,
        bootstrap_samples=bootstrap_samples,
        confidence=confidence,
        training_fold_identities=(
            "full_development_refit_after_untouched_outer_oof_gate",
            complexity.evidence_sha256,
        ),
        synthetic_mode=synthetic_mode,
        formal_input_identity=formal_input_identity,
    )
    if not policy.rules:
        raise ValueError(f"{side} full-Development refit returned CONTROL_85N only")
    return FinalSidePolicyFreeze(
        side=side,
        policy=policy,
        outer_oof_gate=gate,
        complexity=complexity,
    )


def serialize_final_policy_bundle(
    freezes: Sequence[FinalSidePolicyFreeze],
) -> dict[str, Any]:
    """Serialize at most one frozen policy per side without any file reads."""

    by_side = {freeze.side: freeze for freeze in freezes}
    if len(by_side) != len(freezes) or not set(by_side).issubset({"BUY", "SELL"}):
        raise ValueError("final policy bundle must contain at most one BUY and one SELL policy")
    if any(not freeze.outer_oof_gate.passed for freeze in freezes):
        raise ValueError("a failed outer OOF side cannot enter the final policy bundle")
    body = {
        "schema_version": f"{IDENTITY}.final_policy_bundle.v1",
        "identity": IDENTITY,
        "side_policies": {side: by_side[side].artifact() for side in sorted(by_side)},
        "policy_count": len(by_side),
        "permissions": {
            "action_authorized": False,
            "live_authorized": False,
            "f09_registration_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    return {**body, "final_policy_bundle_sha256": _sha256(body)}


__all__ = [
    "BooleanCooldownPolicy",
    "Clause",
    "CONTROL_ACTION",
    "DEPLOYMENT_LCB_SELECTION",
    "EXPLORATORY_IDENTITY",
    "EXPLORATORY_NONBASELINE_SELECTION",
    "FORMAL_BOOTSTRAP_SAMPLES",
    "FORMAL_CONFIDENCE",
    "FinalComplexitySelection",
    "FinalSidePolicyFreeze",
    "FormalInputIdentity",
    "FrozenOuterFold",
    "IDENTITY",
    "InnerFold",
    "Literal",
    "OpportunityPanel",
    "OuterOOFGate",
    "OuterFold",
    "NestedChronologicalResult",
    "PanelAudit",
    "Rule",
    "SearchContract",
    "SPEC_PATH",
    "apply_frozen_policy",
    "attest_formal_input_panel",
    "build_nested_chronological_folds",
    "evaluate_outer_oof_gate",
    "fit_side_policy",
    "freeze_final_side_policy",
    "load_frozen_search_contract",
    "normalize_joint_panel",
    "run_nested_chronological_oof",
    "select_final_complexity_from_inner_evidence",
    "serialize_final_policy_bundle",
]
