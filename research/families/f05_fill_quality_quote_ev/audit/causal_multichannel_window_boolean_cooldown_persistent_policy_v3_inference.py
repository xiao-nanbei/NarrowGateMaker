"""Paired inference for the F05 persistent-policy v3 owner-route study.

The module is deliberately storage-free.  It consumes one outer-OOF table and
the exact PreparedPanel used to score those OOF policies.  It does not fit a
policy, read a new outcome panel, or grant action/live authority.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
import pandas as pd

CONTROL_BLOCK = "CONTROL"
WEBB_MULTIPLIERS = np.asarray(
    [
        -math.sqrt(1.5),
        -1.0,
        -math.sqrt(0.5),
        math.sqrt(0.5),
        1.0,
        math.sqrt(1.5),
    ],
    dtype=float,
)
DEFAULT_HIERARCHY: Mapping[str, tuple[str, ...]] = {
    "BUY": ("BUY:M0-CONTROL", "BUY:M1-M0", "BUY:M2-M1"),
    "SELL": ("SELL:M0-CONTROL", "SELL:M1-M0", "SELL:M2-M1"),
}


class PersistentPolicyV3InferenceError(RuntimeError):
    """Raised when paired OOF inference contracts are violated."""


class PreparedPanelLike(Protocol):
    """The subset of PreparedPanel required by this module."""

    metadata: pd.DataFrame
    outcomes: pd.DataFrame
    supported: pd.DataFrame


@dataclass(frozen=True, slots=True)
class PolicyRef:
    """A policy column inside one outer-OOF artifact."""

    panel_scope: str
    side: str
    feature_block: str
    method: str = "boolean"

    def __post_init__(self) -> None:
        if not self.panel_scope or not self.feature_block or not self.method:
            raise PersistentPolicyV3InferenceError("policy references must be complete")
        normalized_side = self.side.upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise PersistentPolicyV3InferenceError("policy side must be BUY or SELL")
        object.__setattr__(self, "side", normalized_side)
        object.__setattr__(self, "feature_block", self.feature_block.upper())

    @property
    def identity(self) -> str:
        return "/".join(
            (self.panel_scope, self.side, self.feature_block, self.method)
        )


@dataclass(frozen=True, slots=True)
class WeightedEstimate:
    mean_usdc: float | None
    standard_error_usdc: float | None
    identified_weight_fraction: float
    identified_units: int
    total_units: int
    point_identified: bool
    weighting_contract: str


@dataclass(frozen=True, slots=True)
class SimultaneousBand:
    hypothesis: str
    mean_usdc: float
    standard_error_usdc: float
    lcb_usdc: float
    ucb_usdc: float
    day_count: int


@dataclass(frozen=True, slots=True)
class SimultaneousBandFamily:
    bands: Mapping[str, SimultaneousBand]
    critical_value: float
    confidence: float
    draws: int
    seed: int
    shared_days: tuple[str, ...]
    multiplier_support: tuple[float, ...]
    studentization: str = "hc2_residual_observed_day_standard_error"

    def __getitem__(self, hypothesis: str) -> SimultaneousBand:
        return self.bands[hypothesis]


@dataclass(frozen=True, slots=True)
class CensoringSensitivity:
    identified_only_mean_usdc: float | None
    identified_population_contribution_usdc: float
    identified_weight_fraction: float
    unidentified_weight_fraction: float
    point_identified: bool
    bounds_available: bool
    population_lower_bound_usdc: float | None
    population_upper_bound_usdc: float | None
    tipping_unidentified_mean_usdc: float | None
    economic_epsilon_usdc: float
    promotion_allowed_by_identification: bool
    promotion_block_reason: str | None


@dataclass(frozen=True, slots=True)
class HierarchyStepDecision:
    hypothesis: str
    tested: bool
    passed: bool
    simultaneous_lcb_usdc: float | None
    reason: str


@dataclass(frozen=True, slots=True)
class HierarchyDecision:
    steps: Mapping[str, tuple[HierarchyStepDecision, ...]]
    supported_sides: tuple[str, ...]
    economic_epsilon_usdc: float


@dataclass(frozen=True, slots=True)
class TriStatePrevalence:
    true_rate: float
    false_rate: float
    unobserved_rate: float
    observed_rate: float
    total_weight: float


def _require_columns(frame: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = set(columns) - set(frame.columns)
    if missing:
        raise PersistentPolicyV3InferenceError(
            f"{label} is missing columns: {sorted(missing)}"
        )


def _opportunity_frame(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    if "opportunity_id" in frame.columns:
        result = frame.copy()
        result["opportunity_id"] = result["opportunity_id"].astype(str)
        return result
    if not frame.index.is_unique:
        raise PersistentPolicyV3InferenceError(f"{label} index is not unique")
    result = frame.reset_index()
    index_name = frame.index.name or "index"
    result = result.rename(columns={index_name: "opportunity_id"})
    result["opportunity_id"] = result["opportunity_id"].astype(str)
    return result


def _constant_by_opportunity(frame: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    for column in columns:
        if column not in frame.columns:
            continue
        counts = frame.groupby("opportunity_id", observed=True)[column].nunique(dropna=False)
        if bool((counts > 1).any()):
            example = str(counts.index[counts > 1][0])
            raise PersistentPolicyV3InferenceError(
                f"{label} disagrees within opportunity {example!r} for {column!r}"
            )


def _policy_rows(outer_oof: pd.DataFrame, policy: PolicyRef) -> pd.DataFrame:
    required = (
        "opportunity_id",
        "panel_scope",
        "side",
        "feature_block",
        "method",
        "fold_id",
        "utc_day",
        "campaign_cluster_id",
        "selected_action",
        "control_action",
    )
    _require_columns(outer_oof, required, "outer OOF")
    rows = outer_oof.loc[
        (outer_oof["panel_scope"].astype(str) == policy.panel_scope)
        & (outer_oof["side"].astype(str).str.upper() == policy.side)
        & (outer_oof["method"].astype(str) == policy.method)
    ].copy()
    if policy.feature_block != CONTROL_BLOCK:
        rows = rows.loc[
            rows["feature_block"].astype(str).str.upper() == policy.feature_block
        ].copy()
        action_column = "selected_action"
    else:
        action_column = "control_action"
    if rows.empty:
        raise PersistentPolicyV3InferenceError(
            f"outer OOF has no rows for policy {policy.identity}"
        )
    rows["opportunity_id"] = rows["opportunity_id"].astype(str)
    _constant_by_opportunity(
        rows,
        (
            "fold_id",
            "utc_day",
            "campaign_cluster_id",
            "role_at_fill",
            action_column,
        ),
        policy.identity,
    )
    keep = [
        "opportunity_id",
        "fold_id",
        "utc_day",
        "campaign_cluster_id",
        action_column,
    ]
    if "role_at_fill" in rows.columns:
        keep.append("role_at_fill")
    collapsed = rows.loc[:, keep].drop_duplicates("opportunity_id", keep="first")
    return collapsed.rename(columns={action_column: "policy_action"}).set_index(
        "opportunity_id"
    )


def _gather_action_values(
    frame: pd.DataFrame,
    opportunities: pd.Index,
    actions: np.ndarray,
    *,
    boolean: bool,
) -> np.ndarray:
    fill_value: bool | float = False if boolean else math.nan
    dtype = bool if boolean else float
    gathered = np.full(len(opportunities), fill_value, dtype=dtype)
    for action in pd.unique(actions):
        action_name = str(action)
        if action_name not in frame.columns:
            continue
        mask = actions == action
        values = frame.reindex(opportunities[mask])[action_name]
        if boolean:
            gathered[mask] = values.fillna(False).astype(bool).to_numpy()
        else:
            gathered[mask] = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    return gathered


def _attach_population_weights(rows: pd.DataFrame) -> pd.DataFrame:
    result = rows.copy()
    if result.empty:
        raise PersistentPolicyV3InferenceError("paired contrast is empty")
    _require_columns(
        result,
        ("utc_day", "campaign_cluster_id", "opportunity_id"),
        "paired contrast",
    )
    if result["opportunity_id"].duplicated().any():
        raise PersistentPolicyV3InferenceError("paired contrast repeats an opportunity")

    day_campaign_counts = result.groupby(
        ["utc_day", "campaign_cluster_id"], observed=True
    )["opportunity_id"].transform("size")
    result["campaign_day_opportunity_weight"] = 1.0 / day_campaign_counts.astype(float)

    campaigns_per_day = (
        result.loc[:, ["utc_day", "campaign_cluster_id"]]
        .drop_duplicates()
        .groupby("utc_day", observed=True)["campaign_cluster_id"]
        .size()
    )
    result["day_population_weight"] = result["campaign_day_opportunity_weight"] / result[
        "utc_day"
    ].map(campaigns_per_day).astype(float)
    day_count = int(result["utc_day"].nunique())
    result["equal_day_population_weight"] = result["day_population_weight"] / day_count

    campaign_counts = result.groupby("campaign_cluster_id", observed=True)[
        "opportunity_id"
    ].transform("size")
    campaign_count = int(result["campaign_cluster_id"].nunique())
    result["campaign_population_weight"] = 1.0 / (
        campaign_count * campaign_counts.astype(float)
    )

    campaign_day_sums = result.groupby(
        ["utc_day", "campaign_cluster_id"], observed=True
    )["campaign_day_opportunity_weight"].sum()
    day_sums = result.groupby("utc_day", observed=True)["day_population_weight"].sum()
    checks = (
        np.allclose(campaign_day_sums.to_numpy(dtype=float), 1.0, atol=1e-12, rtol=0.0),
        np.allclose(day_sums.to_numpy(dtype=float), 1.0, atol=1e-12, rtol=0.0),
        math.isclose(
            float(result["equal_day_population_weight"].sum()),
            1.0,
            abs_tol=1e-12,
        ),
        math.isclose(
            float(result["campaign_population_weight"].sum()),
            1.0,
            abs_tol=1e-12,
        ),
    )
    if not all(checks):
        raise PersistentPolicyV3InferenceError("hierarchical population weights drifted")
    return result


def build_paired_policy_contrast(
    outer_oof: pd.DataFrame,
    panel: PreparedPanelLike,
    *,
    lhs: PolicyRef,
    rhs: PolicyRef,
) -> pd.DataFrame:
    """Build a policy-vs-policy contrast on exactly the same outer-OOF rows.

    Equal actions have a structural contrast of exactly zero, even when that
    action's terminal outcome is unavailable.  Different actions are point
    identified only when both bound panel arms are supported and finite.
    """

    if lhs.panel_scope != rhs.panel_scope or lhs.side != rhs.side:
        raise PersistentPolicyV3InferenceError(
            "paired policies must share panel_scope and side"
        )
    left = _policy_rows(outer_oof, lhs)
    right = _policy_rows(outer_oof, rhs)
    if set(left.index) != set(right.index):
        raise PersistentPolicyV3InferenceError(
            "paired policies do not share the same outer-OOF opportunity universe"
        )
    right = right.reindex(left.index)
    for column in ("fold_id", "utc_day", "campaign_cluster_id", "role_at_fill"):
        if column in left.columns and column in right.columns:
            equal = left[column].astype(str).to_numpy() == right[column].astype(str).to_numpy()
            if not bool(equal.all()):
                raise PersistentPolicyV3InferenceError(
                    f"paired policy rows disagree on {column!r}"
                )

    metadata = _opportunity_frame(panel.metadata, label="PreparedPanel metadata").set_index(
        "opportunity_id"
    )
    if not left.index.isin(metadata.index).all():
        raise PersistentPolicyV3InferenceError(
            "outer OOF references opportunities absent from PreparedPanel"
        )
    bound_metadata = metadata.reindex(left.index)
    _require_columns(
        bound_metadata,
        ("utc_day", "side", "campaign_cluster_id"),
        "PreparedPanel metadata",
    )
    if not bool((bound_metadata["side"].astype(str).str.upper() == lhs.side).all()):
        raise PersistentPolicyV3InferenceError("PreparedPanel side disagrees with policy")
    for column in ("utc_day", "campaign_cluster_id"):
        if not bool(
            (
                bound_metadata[column].astype(str).to_numpy()
                == left[column].astype(str).to_numpy()
            ).all()
        ):
            raise PersistentPolicyV3InferenceError(
                f"PreparedPanel and outer OOF disagree on {column!r}"
            )

    outcomes = panel.outcomes.copy()
    supported = panel.supported.copy()
    outcomes.index = outcomes.index.astype(str)
    supported.index = supported.index.astype(str)
    if outcomes.index.has_duplicates or supported.index.has_duplicates:
        raise PersistentPolicyV3InferenceError("PreparedPanel arm matrices repeat opportunities")

    opportunities = left.index.astype(str)
    lhs_actions = left["policy_action"].astype(str).to_numpy(dtype=object)
    rhs_actions = right["policy_action"].astype(str).to_numpy(dtype=object)
    same_action = lhs_actions == rhs_actions
    lhs_values = _gather_action_values(
        outcomes, opportunities, lhs_actions, boolean=False
    )
    rhs_values = _gather_action_values(
        outcomes, opportunities, rhs_actions, boolean=False
    )
    lhs_supported = _gather_action_values(
        supported, opportunities, lhs_actions, boolean=True
    )
    rhs_supported = _gather_action_values(
        supported, opportunities, rhs_actions, boolean=True
    )
    finite_pair = np.isfinite(lhs_values) & np.isfinite(rhs_values)
    different_identified = (~same_action) & lhs_supported & rhs_supported & finite_pair
    point_identified = same_action | different_identified
    contrast = np.full(len(opportunities), math.nan, dtype=float)
    contrast[same_action] = 0.0
    contrast[different_identified] = (
        lhs_values[different_identified] - rhs_values[different_identified]
    )
    reason = np.full(len(opportunities), "different_action_unsupported", dtype=object)
    reason[same_action] = "same_action_structural_zero"
    reason[different_identified] = "different_action_both_arms_supported"
    missing_finite = (~same_action) & lhs_supported & rhs_supported & (~finite_pair)
    reason[missing_finite] = "different_action_nonfinite_outcome"

    result = pd.DataFrame(
        {
            "opportunity_id": opportunities,
            "utc_day": bound_metadata["utc_day"].astype(str).to_numpy(),
            "side": lhs.side,
            "campaign_cluster_id": bound_metadata["campaign_cluster_id"]
            .astype(str)
            .to_numpy(),
            "fold_id": left["fold_id"].astype(str).to_numpy(),
            "panel_scope": lhs.panel_scope,
            "lhs_policy": lhs.identity,
            "rhs_policy": rhs.identity,
            "lhs_action": lhs_actions,
            "rhs_action": rhs_actions,
            "same_action": same_action,
            "lhs_supported": lhs_supported,
            "rhs_supported": rhs_supported,
            "lhs_value_usdc": np.where(np.isfinite(lhs_values), lhs_values, np.nan),
            "rhs_value_usdc": np.where(np.isfinite(rhs_values), rhs_values, np.nan),
            "point_identified": point_identified,
            "contrast_usdc": contrast,
            "identification_reason": reason,
        }
    )
    if "role_at_fill" in bound_metadata.columns:
        result["role_at_fill"] = bound_metadata["role_at_fill"].astype(str).to_numpy()
    elif "role_at_fill" in left.columns:
        result["role_at_fill"] = left["role_at_fill"].astype(str).to_numpy()
    return _attach_population_weights(result)


def equal_day_contributions(contrasts: pd.DataFrame) -> pd.DataFrame:
    """Return day-level values after opportunity/campaign/day equalization."""

    rows = _attach_population_weights(
        contrasts.drop(
            columns=[
                "campaign_day_opportunity_weight",
                "day_population_weight",
                "equal_day_population_weight",
                "campaign_population_weight",
            ],
            errors="ignore",
        )
    )
    _require_columns(rows, ("point_identified", "contrast_usdc"), "paired contrast")
    identified = rows["point_identified"].astype(bool).to_numpy()
    values = pd.to_numeric(rows["contrast_usdc"], errors="coerce").to_numpy(dtype=float)
    if bool((identified & ~np.isfinite(values)).any()):
        raise PersistentPolicyV3InferenceError("identified contrasts must be finite")
    records: list[dict[str, Any]] = []
    for day, day_rows in rows.groupby("utc_day", sort=True, observed=True):
        day_identified = day_rows["point_identified"].astype(bool).to_numpy()
        day_values = pd.to_numeric(
            day_rows["contrast_usdc"], errors="coerce"
        ).to_numpy(dtype=float)
        weights = day_rows["day_population_weight"].to_numpy(dtype=float)
        identified_weight = float(weights[day_identified].sum())
        known_contribution = float(
            np.dot(weights[day_identified], day_values[day_identified])
        )
        identified_mean = (
            known_contribution / identified_weight if identified_weight > 0.0 else math.nan
        )
        records.append(
            {
                "utc_day": str(day),
                "identified_mean_usdc": identified_mean,
                "identified_population_contribution_usdc": known_contribution,
                "identified_weight_fraction": identified_weight,
                "unidentified_weight_fraction": 1.0 - identified_weight,
                "point_identified": bool(math.isclose(identified_weight, 1.0, abs_tol=1e-12)),
                "campaigns": int(day_rows["campaign_cluster_id"].nunique()),
                "opportunities": int(len(day_rows)),
            }
        )
    return pd.DataFrame.from_records(records).sort_values("utc_day", kind="stable")


def campaign_weighted_sensitivity(contrasts: pd.DataFrame) -> WeightedEstimate:
    """Summarize with every campaign equal, without equalizing UTC days."""

    rows = _attach_population_weights(
        contrasts.drop(
            columns=[
                "campaign_day_opportunity_weight",
                "day_population_weight",
                "equal_day_population_weight",
                "campaign_population_weight",
            ],
            errors="ignore",
        )
    )
    identified = rows["point_identified"].astype(bool).to_numpy()
    values = pd.to_numeric(rows["contrast_usdc"], errors="coerce").to_numpy(dtype=float)
    weights = rows["campaign_population_weight"].to_numpy(dtype=float)
    identified_weight = float(weights[identified].sum())
    mean = (
        float(np.dot(weights[identified], values[identified]) / identified_weight)
        if identified_weight > 0.0
        else None
    )
    campaign_values: list[float] = []
    for _, campaign_rows in rows.groupby("campaign_cluster_id", observed=True):
        mask = campaign_rows["point_identified"].astype(bool).to_numpy()
        if not mask.any():
            continue
        campaign_values.append(
            float(
                pd.to_numeric(
                    campaign_rows.loc[mask, "contrast_usdc"], errors="coerce"
                ).mean()
            )
        )
    if len(campaign_values) >= 2:
        standard_error = float(
            np.std(np.asarray(campaign_values), ddof=1) / math.sqrt(len(campaign_values))
        )
    elif campaign_values:
        standard_error = math.inf
    else:
        standard_error = None
    return WeightedEstimate(
        mean_usdc=mean,
        standard_error_usdc=standard_error,
        identified_weight_fraction=identified_weight,
        identified_units=len(campaign_values),
        total_units=int(rows["campaign_cluster_id"].nunique()),
        point_identified=bool(math.isclose(identified_weight, 1.0, abs_tol=1e-12)),
        weighting_contract="each_campaign_equal_each_opportunity_equal_within_campaign",
    )


def _coerce_day_series(value: pd.Series | pd.DataFrame, hypothesis: str) -> pd.Series:
    if isinstance(value, pd.DataFrame):
        _require_columns(value, ("utc_day", "identified_mean_usdc"), hypothesis)
        series = value.set_index("utc_day")["identified_mean_usdc"]
    elif isinstance(value, pd.Series):
        series = value.copy()
    else:
        raise PersistentPolicyV3InferenceError(
            f"day contrast {hypothesis!r} must be a Series or DataFrame"
        )
    series.index = series.index.astype(str)
    if series.index.has_duplicates:
        raise PersistentPolicyV3InferenceError(
            f"day contrast {hypothesis!r} repeats a UTC day"
        )
    series = pd.to_numeric(series, errors="coerce").dropna().sort_index()
    if len(series) < 2 or not np.isfinite(series.to_numpy(dtype=float)).all():
        raise PersistentPolicyV3InferenceError(
            f"day contrast {hypothesis!r} requires at least two finite UTC days"
        )
    return series.astype(float)


def webb_wild_day_max_t(
    day_contrasts: Mapping[str, pd.Series | pd.DataFrame],
    *,
    draws: int = 99_999,
    seed: int = 20260812,
    confidence: float = 0.95,
) -> SimultaneousBandFamily:
    """Compute a shared-day Webb six-point multiplier max-t band.

    The same random multiplier is used for a UTC day across every hypothesis.
    Each intercept-only day model uses HC2 residual scaling.  Bootstrap scores
    are studentized by the observed day standard error, which is fixed before
    the shared max-t draw is evaluated.
    """

    if not day_contrasts:
        raise PersistentPolicyV3InferenceError("max-t family cannot be empty")
    if draws < 1 or not 0.5 < confidence < 1.0:
        raise PersistentPolicyV3InferenceError("invalid max-t draws or confidence")
    normalized = {
        str(name): _coerce_day_series(value, str(name))
        for name, value in day_contrasts.items()
    }
    shared_days = tuple(sorted({day for value in normalized.values() for day in value.index}))
    day_positions = {day: index for index, day in enumerate(shared_days)}
    rng = np.random.default_rng(seed)
    codes = rng.integers(0, len(WEBB_MULTIPLIERS), size=(draws, len(shared_days)))
    multipliers = WEBB_MULTIPLIERS[codes]
    maximum_t = np.zeros(draws, dtype=float)
    moments: dict[str, tuple[float, float, int]] = {}
    for name, series in normalized.items():
        values = series.to_numpy(dtype=float)
        count = len(values)
        mean = float(values.mean())
        standard_error = float(np.std(values, ddof=1) / math.sqrt(count))
        moments[name] = (mean, standard_error, count)
        if standard_error <= np.finfo(float).eps:
            continue
        residuals_hc2 = (values - mean) / math.sqrt(1.0 - 1.0 / count)
        positions = np.fromiter(
            (day_positions[day] for day in series.index), dtype=np.int64, count=count
        )
        bootstrap_mean_error = (
            multipliers[:, positions] @ residuals_hc2 / float(count)
        )
        bootstrap_t = np.abs(bootstrap_mean_error / standard_error)
        maximum_t = np.maximum(maximum_t, bootstrap_t)
    try:
        critical = float(np.quantile(maximum_t, confidence, method="higher"))
    except TypeError:  # pragma: no cover - NumPy before 1.22 compatibility.
        critical = float(np.quantile(maximum_t, confidence, interpolation="higher"))
    bands = {
        name: SimultaneousBand(
            hypothesis=name,
            mean_usdc=mean,
            standard_error_usdc=standard_error,
            lcb_usdc=mean - critical * standard_error,
            ucb_usdc=mean + critical * standard_error,
            day_count=count,
        )
        for name, (mean, standard_error, count) in moments.items()
    }
    return SimultaneousBandFamily(
        bands=bands,
        critical_value=critical,
        confidence=confidence,
        draws=draws,
        seed=seed,
        shared_days=shared_days,
        multiplier_support=tuple(float(value) for value in WEBB_MULTIPLIERS),
    )


def censoring_tipping_bound(
    contrasts: pd.DataFrame,
    *,
    uplift_bounds_usdc: tuple[float, float] | None = None,
    economic_epsilon_usdc: float = 0.0,
) -> CensoringSensitivity:
    """Bound unidentified policy contrasts or report their tipping value."""

    if not math.isfinite(economic_epsilon_usdc):
        raise PersistentPolicyV3InferenceError("economic epsilon must be finite")
    rows = _attach_population_weights(
        contrasts.drop(
            columns=[
                "campaign_day_opportunity_weight",
                "day_population_weight",
                "equal_day_population_weight",
                "campaign_population_weight",
            ],
            errors="ignore",
        )
    )
    identified = rows["point_identified"].astype(bool).to_numpy()
    values = pd.to_numeric(rows["contrast_usdc"], errors="coerce").to_numpy(dtype=float)
    weights = rows["equal_day_population_weight"].to_numpy(dtype=float)
    identified_weight = float(weights[identified].sum())
    unidentified_weight = max(0.0, 1.0 - identified_weight)
    known_contribution = float(np.dot(weights[identified], values[identified]))
    identified_mean = (
        known_contribution / identified_weight if identified_weight > 0.0 else None
    )
    point_identified = math.isclose(unidentified_weight, 0.0, abs_tol=1e-12)
    bounds_available = False
    lower = upper = None
    if uplift_bounds_usdc is not None:
        bound_low, bound_high = map(float, uplift_bounds_usdc)
        if not (math.isfinite(bound_low) and math.isfinite(bound_high) and bound_low <= bound_high):
            raise PersistentPolicyV3InferenceError("uplift bounds must be finite and ordered")
        bounds_available = True
        lower = known_contribution + unidentified_weight * bound_low
        upper = known_contribution + unidentified_weight * bound_high
    elif point_identified:
        lower = upper = known_contribution
    tipping = None
    if unidentified_weight > 1e-12:
        tipping = (economic_epsilon_usdc - known_contribution) / unidentified_weight

    if point_identified:
        promotion_allowed = known_contribution > economic_epsilon_usdc
        block_reason = None if promotion_allowed else "point_estimate_not_above_economic_epsilon"
    elif not bounds_available:
        promotion_allowed = False
        block_reason = "unidentified_weight_without_legal_bounds"
    else:
        assert lower is not None
        promotion_allowed = lower > economic_epsilon_usdc
        block_reason = None if promotion_allowed else "population_lower_bound_not_positive"
    return CensoringSensitivity(
        identified_only_mean_usdc=identified_mean,
        identified_population_contribution_usdc=known_contribution,
        identified_weight_fraction=identified_weight,
        unidentified_weight_fraction=unidentified_weight,
        point_identified=point_identified,
        bounds_available=bounds_available,
        population_lower_bound_usdc=lower,
        population_upper_bound_usdc=upper,
        tipping_unidentified_mean_usdc=tipping,
        economic_epsilon_usdc=economic_epsilon_usdc,
        promotion_allowed_by_identification=promotion_allowed,
        promotion_block_reason=block_reason,
    )


def apply_feature_hierarchy(
    bands: Mapping[str, SimultaneousBand] | SimultaneousBandFamily,
    hierarchy: Mapping[str, tuple[str, ...]] = DEFAULT_HIERARCHY,
    *,
    economic_epsilon_usdc: float = 0.0,
    censoring: Mapping[str, CensoringSensitivity] | None = None,
) -> HierarchyDecision:
    """Apply M0 absolute, then paired M1-M0, then paired M2-M1 gates."""

    family = bands.bands if isinstance(bands, SimultaneousBandFamily) else bands
    decisions: dict[str, tuple[HierarchyStepDecision, ...]] = {}
    supported: list[str] = []
    for side, hypotheses in hierarchy.items():
        parent_passed = True
        side_steps: list[HierarchyStepDecision] = []
        for hypothesis in hypotheses:
            if hypothesis not in family:
                raise PersistentPolicyV3InferenceError(
                    f"hierarchy references missing hypothesis {hypothesis!r}"
                )
            band = family[hypothesis]
            if not parent_passed:
                step = HierarchyStepDecision(
                    hypothesis=hypothesis,
                    tested=False,
                    passed=False,
                    simultaneous_lcb_usdc=band.lcb_usdc,
                    reason="parent_feature_block_not_supported",
                )
            else:
                identification = None if censoring is None else censoring.get(hypothesis)
                if (
                    identification is not None
                    and not identification.promotion_allowed_by_identification
                ):
                    step = HierarchyStepDecision(
                        hypothesis=hypothesis,
                        tested=True,
                        passed=False,
                        simultaneous_lcb_usdc=band.lcb_usdc,
                        reason=identification.promotion_block_reason
                        or "identification_gate_failed",
                    )
                else:
                    passed = band.lcb_usdc > economic_epsilon_usdc
                    step = HierarchyStepDecision(
                        hypothesis=hypothesis,
                        tested=True,
                        passed=passed,
                        simultaneous_lcb_usdc=band.lcb_usdc,
                        reason=(
                            "simultaneous_lcb_above_economic_epsilon"
                            if passed
                            else "simultaneous_lcb_not_above_economic_epsilon"
                        ),
                    )
            side_steps.append(step)
            parent_passed = step.passed
        decisions[str(side).upper()] = tuple(side_steps)
        if side_steps and all(step.passed for step in side_steps):
            supported.append(str(side).upper())
    return HierarchyDecision(
        steps=decisions,
        supported_sides=tuple(supported),
        economic_epsilon_usdc=economic_epsilon_usdc,
    )


def _weighted_values(
    values: Sequence[float] | np.ndarray | pd.Series,
    weights: Sequence[float] | np.ndarray | pd.Series | None,
) -> tuple[np.ndarray, np.ndarray]:
    array = np.asarray(values, dtype=float)
    if array.ndim != 1:
        raise PersistentPolicyV3InferenceError("distribution values must be one-dimensional")
    if weights is None:
        weight_array = np.ones(len(array), dtype=float)
    else:
        weight_array = np.asarray(weights, dtype=float)
        if weight_array.shape != array.shape:
            raise PersistentPolicyV3InferenceError("distribution weights have wrong shape")
    valid = np.isfinite(array) & np.isfinite(weight_array) & (weight_array >= 0.0)
    array = array[valid]
    weight_array = weight_array[valid]
    if not len(array) or float(weight_array.sum()) <= 0.0:
        raise PersistentPolicyV3InferenceError("distribution has no positive finite weight")
    return array, weight_array


def _weighted_mean_variance(values: np.ndarray, weights: np.ndarray) -> tuple[float, float]:
    total = float(weights.sum())
    mean = float(np.dot(weights, values) / total)
    variance = float(np.dot(weights, np.square(values - mean)) / total)
    return mean, max(0.0, variance)


def _weighted_quantiles(
    values: np.ndarray, weights: np.ndarray, quantiles: np.ndarray
) -> np.ndarray:
    order = np.argsort(values, kind="stable")
    ordered_values = values[order]
    ordered_weights = weights[order]
    positive = ordered_weights > 0.0
    ordered_values = ordered_values[positive]
    ordered_weights = ordered_weights[positive]
    cumulative = np.cumsum(ordered_weights) - 0.5 * ordered_weights
    cumulative /= float(ordered_weights.sum())
    return np.interp(
        quantiles,
        cumulative,
        ordered_values,
        left=float(ordered_values[0]),
        right=float(ordered_values[-1]),
    )


def weighted_smd(
    train: Sequence[float] | np.ndarray | pd.Series,
    test: Sequence[float] | np.ndarray | pd.Series,
    train_weight: Sequence[float] | np.ndarray | pd.Series | None = None,
    test_weight: Sequence[float] | np.ndarray | pd.Series | None = None,
) -> float:
    """Return signed standardized mean difference using weighted variances."""

    train_values, train_weights = _weighted_values(train, train_weight)
    test_values, test_weights = _weighted_values(test, test_weight)
    train_mean, train_variance = _weighted_mean_variance(train_values, train_weights)
    test_mean, test_variance = _weighted_mean_variance(test_values, test_weights)
    denominator = math.sqrt((train_variance + test_variance) / 2.0)
    difference = test_mean - train_mean
    if denominator <= np.finfo(float).eps:
        if math.isclose(difference, 0.0, abs_tol=1e-15):
            return 0.0
        return math.copysign(math.inf, difference)
    return difference / denominator


def weighted_psi(
    train: Sequence[float] | np.ndarray | pd.Series,
    test: Sequence[float] | np.ndarray | pd.Series,
    train_weight: Sequence[float] | np.ndarray | pd.Series | None = None,
    test_weight: Sequence[float] | np.ndarray | pd.Series | None = None,
    *,
    bins: int = 10,
    epsilon: float = 1e-9,
) -> float:
    """Return PSI using outcome-blind train quantile bins."""

    if bins < 2 or not 0.0 < epsilon < 0.5:
        raise PersistentPolicyV3InferenceError("invalid PSI bins or epsilon")
    train_values, train_weights = _weighted_values(train, train_weight)
    test_values, test_weights = _weighted_values(test, test_weight)
    quantiles = np.linspace(0.0, 1.0, bins + 1)[1:-1]
    internal = _weighted_quantiles(train_values, train_weights, quantiles)
    internal = np.unique(internal)
    edges = np.concatenate(([-math.inf], internal, [math.inf]))
    train_counts, _ = np.histogram(train_values, bins=edges, weights=train_weights)
    test_counts, _ = np.histogram(test_values, bins=edges, weights=test_weights)
    train_rate = train_counts.astype(float) / float(train_counts.sum())
    test_rate = test_counts.astype(float) / float(test_counts.sum())
    train_rate = np.maximum(train_rate, epsilon)
    test_rate = np.maximum(test_rate, epsilon)
    train_rate /= train_rate.sum()
    test_rate /= test_rate.sum()
    return float(np.sum((test_rate - train_rate) * np.log(test_rate / train_rate)))


def tri_state_prevalence(
    values: Sequence[float] | np.ndarray | pd.Series,
    weights: Sequence[float] | np.ndarray | pd.Series | None = None,
) -> TriStatePrevalence:
    """Return weighted true/false/unobserved rates for {-1, 0, 1} predicates."""

    array = np.asarray(values, dtype=float)
    if array.ndim != 1:
        raise PersistentPolicyV3InferenceError("tri-state values must be one-dimensional")
    if weights is None:
        weight_array = np.ones(len(array), dtype=float)
    else:
        weight_array = np.asarray(weights, dtype=float)
        if weight_array.shape != array.shape:
            raise PersistentPolicyV3InferenceError("tri-state weights have wrong shape")
    if not np.isfinite(weight_array).all() or bool((weight_array < 0.0).any()):
        raise PersistentPolicyV3InferenceError("tri-state weights must be finite and nonnegative")
    finite = np.isfinite(array)
    if bool((finite & ~np.isin(array, (-1.0, 0.0, 1.0))).any()):
        raise PersistentPolicyV3InferenceError("tri-state values must be -1, 0, 1, or NaN")
    total = float(weight_array.sum())
    if total <= 0.0:
        raise PersistentPolicyV3InferenceError("tri-state total weight must be positive")
    true_rate = float(weight_array[finite & (array == 1.0)].sum() / total)
    false_rate = float(weight_array[finite & (array == 0.0)].sum() / total)
    unobserved_rate = float(weight_array[(~finite) | (array == -1.0)].sum() / total)
    return TriStatePrevalence(
        true_rate=true_rate,
        false_rate=false_rate,
        unobserved_rate=unobserved_rate,
        observed_rate=true_rate + false_rate,
        total_weight=total,
    )
