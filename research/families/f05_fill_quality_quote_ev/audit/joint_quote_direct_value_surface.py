"""Development-only direct-value evaluation for finite joint quote actions.

The evaluator is deliberately in-memory.  It accepts a frozen randomized
single-action panel, performs nested chronological fitting, and returns OOF
prediction and DR evidence.  It never reads a file, writes an artifact, or
grants action/live authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

PANEL_SCHEMA_VERSION = "joint_quote_direct_value_surface.panel.v1"
REPORT_SCHEMA_VERSION = "joint_quote_direct_value_surface.report.v1"
TARGET_COLUMN = "assignment_to_washout_direct_value_usdc"
TARGET_SEMANTICS = "assignment_to_washout_direct_usdc"
TARGET_UNIT = "USDC_per_assignment_episode"
PANEL_ROLE = "development"
RANDOMIZATION_UNIT = "assignment_episode"
PROPENSITY_SEMANTICS = "known_probability_of_observed_action"

IDENTITY_COLUMNS = (
    "panel_schema_version",
    "surface_spec_sha256",
    "development_identity",
    "dataset_identity_sha256",
    "feature_dag_id",
    "feature_dag_sha256",
    "panel_role",
    "target_semantics",
    "target_unit",
    "randomization_unit",
    "propensity_semantics",
)
OBSERVATION_COLUMNS = (
    "day",
    "assignment_episode_id",
    "assignment_ts_ns",
    "feature_ready_ts_ns",
    "washout_ts_ns",
    "action",
    "behavior_propensity",
    TARGET_COLUMN,
)


@dataclass(frozen=True)
class JointQuoteActionSpec:
    """One frozen joint bid/ask action in the finite candidate set."""

    name: str
    behavior_propensity: float
    candidate_features: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class JointQuoteDirectValueSpec:
    """Frozen Development contract for nested direct-Q evaluation."""

    identity: str
    development_identity: str
    dataset_identity_sha256: str
    feature_dag_id: str
    feature_dag_sha256: str
    development_days: tuple[str, ...]
    baseline_action: str
    actions: tuple[JointQuoteActionSpec, ...]
    context_features: tuple[str, ...]
    p3_features: tuple[str, ...] = ()
    ridge_alphas: tuple[float, ...] = (0.1, 1.0, 10.0)
    min_outer_train_days: int = 12
    outer_embargo_days: int = 1
    outer_test_days: int = 2
    min_inner_train_days: int = 5
    inner_embargo_days: int = 1
    inner_test_days: int = 2
    economic_epsilon_usdc: float = 0.0
    confidence: float = 0.95
    bootstrap_samples: int = 2_000
    random_seed: int = 20260803

    @property
    def canonical_sha256(self) -> str:
        return canonical_spec_sha256(self)


@dataclass(frozen=True)
class JointQuoteDirectValueEvaluation:
    """In-memory Development evidence returned by the evaluator."""

    action_predictions: pd.DataFrame
    oof_policy: pd.DataFrame
    selection_evidence: pd.DataFrame
    chronology_audit: pd.DataFrame
    hyperparameter_evidence: pd.DataFrame
    report: dict[str, Any]


@dataclass(frozen=True)
class _Fold:
    fold: int
    train_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    test_days: tuple[str, ...]


@dataclass(frozen=True)
class _DirectQModel:
    scaler: StandardScaler
    regression: Ridge


@dataclass(frozen=True)
class _InnerOOF:
    row_ids: np.ndarray
    days: np.ndarray
    observed_actions: np.ndarray
    outcomes: np.ndarray
    propensities: np.ndarray
    q_values: np.ndarray
    chronology: tuple[dict[str, Any], ...]


def _canonical_json(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def canonical_spec_sha256(spec: JointQuoteDirectValueSpec) -> str:
    """Return the deterministic identity of every frozen evaluator input."""

    return hashlib.sha256(_canonical_json(asdict(spec))).hexdigest()


def _require_sha256(value: object, *, name: str) -> str:
    text = str(value).lower()
    if (
        len(text) != 64
        or any(char not in "0123456789abcdef" for char in text)
        or len(set(text)) == 1
    ):
        raise ValueError(f"{name} must be a non-degenerate SHA256")
    return text


def _validate_days(values: tuple[str, ...], *, name: str) -> tuple[str, ...]:
    days = tuple(str(value) for value in values)
    if not days or len(days) != len(set(days)) or days != tuple(sorted(days)):
        raise ValueError(f"{name} must be non-empty, unique, and chronological")
    for day in days:
        try:
            parsed = pd.Timestamp(day)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} contains an invalid UTC day") from exc
        if parsed.strftime("%Y-%m-%d") != day:
            raise ValueError(f"{name} must use YYYY-MM-DD")
    return days


def validate_spec(spec: JointQuoteDirectValueSpec) -> None:
    """Fail closed on an incomplete or internally inconsistent frozen spec."""

    if not spec.identity or not spec.development_identity:
        raise ValueError("spec identities must be non-empty")
    _require_sha256(spec.dataset_identity_sha256, name="dataset identity")
    if not spec.feature_dag_id:
        raise ValueError("feature_dag_id must be non-empty")
    _require_sha256(spec.feature_dag_sha256, name="Feature DAG identity")
    days = _validate_days(spec.development_days, name="development_days")

    if len(spec.actions) < 2:
        raise ValueError("the finite action set must contain at least two actions")
    action_names = tuple(action.name for action in spec.actions)
    if any(not name for name in action_names) or len(action_names) != len(set(action_names)):
        raise ValueError("action names must be non-empty and unique")
    if spec.baseline_action not in action_names:
        raise ValueError("baseline_action is not in the frozen action set")

    propensities = np.asarray([action.behavior_propensity for action in spec.actions], dtype=float)
    if not np.isfinite(propensities).all() or np.any(propensities <= 0.0):
        raise ValueError("every frozen action propensity must be positive")
    if not np.isclose(propensities.sum(), 1.0, atol=1e-12, rtol=0.0):
        raise ValueError("frozen action propensities must sum to one")

    if not spec.context_features or len(spec.context_features) != len(set(spec.context_features)):
        raise ValueError("context_features must be non-empty and unique")
    reserved = set(IDENTITY_COLUMNS) | set(OBSERVATION_COLUMNS)
    if reserved.intersection(spec.context_features):
        raise ValueError("context_features collide with reserved panel columns")
    if not set(spec.p3_features).issubset(spec.context_features):
        raise ValueError("P3 features must be ordinary context features")

    candidate_names: tuple[str, ...] | None = None
    for action in spec.actions:
        names = tuple(name for name, _ in action.candidate_features)
        values = np.asarray([value for _, value in action.candidate_features], dtype=float)
        if not names or len(names) != len(set(names)):
            raise ValueError("candidate feature names must be non-empty and unique")
        if candidate_names is None:
            candidate_names = names
        elif names != candidate_names:
            raise ValueError("all actions must use the same ordered candidate feature schema")
        if not np.isfinite(values).all():
            raise ValueError("candidate feature values must be finite")
        if reserved.intersection(names) or set(spec.context_features).intersection(names):
            raise ValueError("candidate features collide with panel/context columns")

    alphas = np.asarray(spec.ridge_alphas, dtype=float)
    if (
        not len(alphas)
        or not np.isfinite(alphas).all()
        or np.any(alphas <= 0.0)
        or len(set(float(value) for value in alphas)) != len(alphas)
    ):
        raise ValueError("ridge_alphas must be unique, finite, and positive")
    if spec.min_outer_train_days < 2 or spec.min_inner_train_days < 2:
        raise ValueError("outer and inner training windows need at least two days")
    if spec.outer_embargo_days < 0 or spec.inner_embargo_days < 0:
        raise ValueError("embargo day counts must be non-negative")
    if spec.outer_test_days < 1 or spec.inner_test_days < 1:
        raise ValueError("test windows must contain at least one day")
    minimum_outer = spec.min_inner_train_days + spec.inner_embargo_days + 2
    if spec.min_outer_train_days < minimum_outer:
        raise ValueError("min_outer_train_days cannot produce two inner OOF day clusters")
    minimum_days = spec.min_outer_train_days + spec.outer_embargo_days + 2
    if len(days) < minimum_days:
        raise ValueError("development_days cannot produce two outer OOF days")
    if not np.isfinite(spec.economic_epsilon_usdc) or (spec.economic_epsilon_usdc < 0.0):
        raise ValueError("economic_epsilon_usdc must be finite and non-negative")
    if not 0.5 < spec.confidence < 1.0:
        raise ValueError("confidence must lie strictly between 0.5 and 1")
    if spec.bootstrap_samples < 100:
        raise ValueError("bootstrap_samples must be at least 100")


def _expected_columns(spec: JointQuoteDirectValueSpec) -> set[str]:
    return set(IDENTITY_COLUMNS) | set(OBSERVATION_COLUMNS) | set(spec.context_features)


def validate_panel(
    panel: pd.DataFrame,
    spec: JointQuoteDirectValueSpec,
) -> pd.DataFrame:
    """Validate exact schema, causal clocks, actions, and frozen identities."""

    validate_spec(spec)
    if not isinstance(panel, pd.DataFrame) or panel.empty:
        raise ValueError("panel must be a non-empty pandas DataFrame")
    expected = _expected_columns(spec)
    actual = set(panel.columns)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ValueError(f"panel schema mismatch; missing={missing}, extra={extra}")

    frame = panel.copy(deep=True)
    identity_values = {
        "panel_schema_version": PANEL_SCHEMA_VERSION,
        "surface_spec_sha256": spec.canonical_sha256,
        "development_identity": spec.development_identity,
        "dataset_identity_sha256": spec.dataset_identity_sha256,
        "feature_dag_id": spec.feature_dag_id,
        "feature_dag_sha256": spec.feature_dag_sha256,
        "panel_role": PANEL_ROLE,
        "target_semantics": TARGET_SEMANTICS,
        "target_unit": TARGET_UNIT,
        "randomization_unit": RANDOMIZATION_UNIT,
        "propensity_semantics": PROPENSITY_SEMANTICS,
    }
    for column, expected_value in identity_values.items():
        values = frame[column].astype(str)
        if values.isna().any() or not values.eq(str(expected_value)).all():
            raise ValueError(f"panel causal identity mismatch in {column}")

    days = frame["day"].astype(str)
    if set(days) != set(spec.development_days):
        raise ValueError("panel days do not equal the frozen Development calendar")
    for day in days.unique():
        if pd.Timestamp(day).strftime("%Y-%m-%d") != day:
            raise ValueError("panel day must use YYYY-MM-DD")

    episode_ids = frame["assignment_episode_id"].astype("string")
    if episode_ids.isna().any() or episode_ids.str.strip().eq("").any():
        raise ValueError("assignment_episode_id must be non-empty")
    if episode_ids.duplicated().any():
        raise ValueError("panel must contain one row per assignment episode")

    numeric_columns = (
        "assignment_ts_ns",
        "feature_ready_ts_ns",
        "washout_ts_ns",
        "behavior_propensity",
        TARGET_COLUMN,
        *spec.context_features,
    )
    for column in numeric_columns:
        try:
            values = pd.to_numeric(frame[column], errors="raise").to_numpy(dtype=float)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{column} must be numeric") from exc
        if not np.isfinite(values).all():
            raise ValueError(f"{column} must be finite")

    assignment_ts = pd.to_numeric(frame["assignment_ts_ns"]).to_numpy(dtype=np.int64)
    feature_ready_ts = pd.to_numeric(frame["feature_ready_ts_ns"]).to_numpy(dtype=np.int64)
    washout_ts = pd.to_numeric(frame["washout_ts_ns"]).to_numpy(dtype=np.int64)
    if np.any(feature_ready_ts > assignment_ts):
        raise ValueError("future feature_ready_ts_ns exceeds assignment time")
    if np.any(washout_ts < assignment_ts):
        raise ValueError("washout precedes assignment")
    assignment_days = pd.to_datetime(assignment_ts, unit="ns", utc=True).strftime("%Y-%m-%d")
    if not np.array_equal(assignment_days, days.to_numpy(dtype=str)):
        raise ValueError("day does not match assignment_ts_ns in UTC")

    action_names = tuple(action.name for action in spec.actions)
    action_set = set(action_names)
    observed_actions = frame["action"].astype(str)
    unknown = sorted(set(observed_actions) - action_set)
    if unknown:
        raise ValueError(f"unknown action values: {unknown}")
    expected_propensity = {
        action.name: float(action.behavior_propensity) for action in spec.actions
    }
    observed_propensity = pd.to_numeric(frame["behavior_propensity"]).to_numpy(dtype=float)
    expected_vector = observed_actions.map(expected_propensity).to_numpy(dtype=float)
    if not np.allclose(observed_propensity, expected_vector, atol=1e-12, rtol=0.0):
        raise ValueError("behavior_propensity does not match the frozen action")
    if set(observed_actions) != action_set:
        raise ValueError("every frozen action must be observed in Development")

    frame["day"] = days
    frame["action"] = observed_actions
    frame["_row_id"] = np.arange(len(frame), dtype=np.int64)
    return frame.sort_values(
        ["day", "assignment_ts_ns", "assignment_episode_id"], kind="stable"
    ).reset_index(drop=True)


def _expanding_folds(
    days: tuple[str, ...],
    *,
    min_train_days: int,
    embargo_days: int,
    test_days: int,
) -> tuple[_Fold, ...]:
    cursor = int(min_train_days)
    folds: list[_Fold] = []
    while cursor + int(embargo_days) < len(days):
        test_start = cursor + int(embargo_days)
        test_end = min(len(days), test_start + int(test_days))
        train = tuple(days[:cursor])
        embargo = tuple(days[cursor:test_start])
        test = tuple(days[test_start:test_end])
        if not test:
            break
        if max(train) >= min(test):
            raise RuntimeError("chronological fold construction failed")
        folds.append(
            _Fold(
                fold=len(folds),
                train_days=train,
                embargo_days=embargo,
                test_days=test,
            )
        )
        cursor = test_end
    if not folds:
        raise ValueError("no expanding chronological folds can be constructed")
    return tuple(folds)


def _action_index(spec: JointQuoteDirectValueSpec) -> dict[str, int]:
    return {action.name: index for index, action in enumerate(spec.actions)}


def _candidate_matrix(
    action_names: np.ndarray,
    spec: JointQuoteDirectValueSpec,
) -> np.ndarray:
    features = {
        action.name: np.asarray([value for _, value in action.candidate_features], dtype=float)
        for action in spec.actions
    }
    return np.vstack([features[str(action)] for action in action_names])


def _design_matrix(
    frame: pd.DataFrame,
    action_names: np.ndarray,
    spec: JointQuoteDirectValueSpec,
) -> np.ndarray:
    context = frame.loc[:, list(spec.context_features)].to_numpy(dtype=float)
    candidate = _candidate_matrix(action_names, spec)
    non_baseline = tuple(
        action.name for action in spec.actions if action.name != spec.baseline_action
    )
    indicators = np.column_stack([action_names == action for action in non_baseline]).astype(float)
    action_context = np.einsum("ij,ik->ijk", indicators, context).reshape(len(frame), -1)
    candidate_context = np.einsum("ij,ik->ijk", candidate, context).reshape(len(frame), -1)
    return np.column_stack(
        (
            context,
            candidate,
            candidate_context,
            indicators,
            action_context,
        )
    )


def _require_action_support(
    frame: pd.DataFrame,
    spec: JointQuoteDirectValueSpec,
    *,
    scope: str,
) -> None:
    counts = frame["action"].value_counts()
    missing = [action.name for action in spec.actions if counts.get(action.name, 0) < 2]
    if missing:
        raise ValueError(f"{scope} lacks randomized support for actions {missing}")


def _fit_direct_q(
    frame: pd.DataFrame,
    spec: JointQuoteDirectValueSpec,
    *,
    alpha: float,
) -> _DirectQModel:
    _require_action_support(frame, spec, scope="model training fold")
    actions = frame["action"].to_numpy(dtype=str)
    design = _design_matrix(frame, actions, spec)
    outcomes = frame[TARGET_COLUMN].to_numpy(dtype=float)
    propensity = frame["behavior_propensity"].to_numpy(dtype=float)
    weights = 1.0 / propensity
    weights /= float(np.mean(weights))
    scaler = StandardScaler()
    scaled = scaler.fit_transform(design, sample_weight=weights)
    regression = Ridge(alpha=float(alpha), fit_intercept=True)
    regression.fit(scaled, outcomes, sample_weight=weights)
    return _DirectQModel(scaler=scaler, regression=regression)


def _predict_q(
    model: _DirectQModel,
    frame: pd.DataFrame,
    action: str,
    spec: JointQuoteDirectValueSpec,
) -> np.ndarray:
    actions = np.repeat(str(action), len(frame))
    design = _design_matrix(frame, actions, spec)
    prediction = model.regression.predict(model.scaler.transform(design))
    if not np.isfinite(prediction).all():
        raise RuntimeError("direct-Q regression emitted non-finite predictions")
    return np.asarray(prediction, dtype=float)


def _inner_oof(
    outer_train: pd.DataFrame,
    spec: JointQuoteDirectValueSpec,
    *,
    alpha: float,
    outer_fold: int,
) -> _InnerOOF:
    days = tuple(sorted(outer_train["day"].unique()))
    folds = _expanding_folds(
        days,
        min_train_days=spec.min_inner_train_days,
        embargo_days=spec.inner_embargo_days,
        test_days=spec.inner_test_days,
    )
    parts: list[tuple[pd.DataFrame, np.ndarray]] = []
    chronology: list[dict[str, Any]] = []
    for fold in folds:
        train = outer_train.loc[outer_train["day"].isin(fold.train_days)]
        test = outer_train.loc[outer_train["day"].isin(fold.test_days)]
        model = _fit_direct_q(train, spec, alpha=float(alpha))
        q_values = np.column_stack(
            [_predict_q(model, test, action.name, spec) for action in spec.actions]
        )
        parts.append((test.copy(), q_values))
        chronology.append(
            {
                "level": "inner",
                "outer_fold": int(outer_fold),
                "inner_fold": int(fold.fold),
                "alpha": float(alpha),
                "train_min_day": min(fold.train_days),
                "train_max_day": max(fold.train_days),
                "embargo_days": list(fold.embargo_days),
                "test_min_day": min(fold.test_days),
                "test_max_day": max(fold.test_days),
                "future_training_leakage": False,
            }
        )
    if not parts:
        raise ValueError("outer train produced no inner OOF predictions")
    ordered = sorted(parts, key=lambda item: int(item[0]["_row_id"].min()))
    inner_frame = pd.concat([part[0] for part in ordered], ignore_index=True)
    q_values = np.vstack([part[1] for part in ordered])
    if inner_frame["day"].nunique() < 2:
        raise ValueError("candidate screening needs at least two inner OOF days")
    return _InnerOOF(
        row_ids=inner_frame["_row_id"].to_numpy(dtype=np.int64),
        days=inner_frame["day"].to_numpy(dtype=str),
        observed_actions=inner_frame["action"].to_numpy(dtype=str),
        outcomes=inner_frame[TARGET_COLUMN].to_numpy(dtype=float),
        propensities=inner_frame["behavior_propensity"].to_numpy(dtype=float),
        q_values=q_values,
        chronology=tuple(chronology),
    )


def _inner_weighted_mse(
    evidence: _InnerOOF,
    spec: JointQuoteDirectValueSpec,
) -> float:
    indices = _action_index(spec)
    observed_q = np.asarray(
        [
            evidence.q_values[row, indices[action]]
            for row, action in enumerate(evidence.observed_actions)
        ],
        dtype=float,
    )
    squared = np.square(evidence.outcomes - observed_q)
    weights = 1.0 / evidence.propensities
    return float(np.average(squared, weights=weights))


def doubly_robust_policy_vs_baseline(
    *,
    observed_action: np.ndarray,
    outcome: np.ndarray,
    observed_propensity: np.ndarray,
    policy_action: np.ndarray,
    baseline_action: str,
    q_policy: np.ndarray,
    q_baseline: np.ndarray,
) -> np.ndarray:
    """Return the row-level DR contrast for a deterministic target policy."""

    arrays = tuple(
        np.asarray(value)
        for value in (
            observed_action,
            outcome,
            observed_propensity,
            policy_action,
            q_policy,
            q_baseline,
        )
    )
    lengths = {len(value) for value in arrays}
    if len(lengths) != 1:
        raise ValueError("DR inputs must have equal lengths")
    observed = arrays[0].astype(str)
    values = arrays[1].astype(float)
    propensity = arrays[2].astype(float)
    policy = arrays[3].astype(str)
    q_pi = arrays[4].astype(float)
    q_base = arrays[5].astype(float)
    if (
        not np.isfinite(values).all()
        or not np.isfinite(propensity).all()
        or not np.isfinite(q_pi).all()
        or not np.isfinite(q_base).all()
        or np.any(propensity <= 0.0)
        or np.any(propensity > 1.0)
    ):
        raise ValueError("DR inputs contain invalid numeric values")
    contrast = q_pi - q_base
    policy_match = observed == policy
    baseline_match = observed == str(baseline_action)
    contrast = contrast + policy_match * (values - q_pi) / propensity
    contrast = contrast - baseline_match * (values - q_base) / propensity
    contrast[policy == str(baseline_action)] = 0.0
    return np.asarray(contrast, dtype=float)


def _candidate_dr_matrix(
    evidence: _InnerOOF,
    spec: JointQuoteDirectValueSpec,
) -> tuple[tuple[str, ...], np.ndarray]:
    indices = _action_index(spec)
    baseline_index = indices[spec.baseline_action]
    candidates = tuple(
        action.name for action in spec.actions if action.name != spec.baseline_action
    )
    columns: list[np.ndarray] = []
    for candidate in candidates:
        policy = np.repeat(candidate, len(evidence.outcomes))
        columns.append(
            doubly_robust_policy_vs_baseline(
                observed_action=evidence.observed_actions,
                outcome=evidence.outcomes,
                observed_propensity=evidence.propensities,
                policy_action=policy,
                baseline_action=spec.baseline_action,
                q_policy=evidence.q_values[:, indices[candidate]],
                q_baseline=evidence.q_values[:, baseline_index],
            )
        )
    return candidates, np.column_stack(columns)


def _cluster_bootstrap_matrix(
    values: np.ndarray,
    days: np.ndarray,
    *,
    samples: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    unique_days = tuple(sorted(set(str(day) for day in days)))
    if len(unique_days) < 2:
        raise ValueError("day-cluster inference requires at least two UTC days")
    sums = np.zeros((len(unique_days), values.shape[1]), dtype=float)
    counts = np.zeros(len(unique_days), dtype=float)
    day_text = np.asarray(days, dtype=str)
    for index, day in enumerate(unique_days):
        mask = day_text == day
        sums[index] = values[mask].sum(axis=0)
        counts[index] = float(mask.sum())
    point = sums.sum(axis=0) / counts.sum()
    rng = np.random.default_rng(int(seed))
    draw = rng.integers(0, len(unique_days), size=(int(samples), len(unique_days)))
    bootstrap = sums[draw].sum(axis=1) / counts[draw].sum(axis=1)[:, None]
    return point, bootstrap


def _simultaneous_candidate_screen(
    evidence: _InnerOOF,
    spec: JointQuoteDirectValueSpec,
    *,
    outer_fold: int,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    candidates, dr_values = _candidate_dr_matrix(evidence, spec)
    point, bootstrap = _cluster_bootstrap_matrix(
        dr_values,
        evidence.days,
        samples=spec.bootstrap_samples,
        seed=spec.random_seed + 10_000 + int(outer_fold),
    )
    max_deviation = np.max(np.abs(bootstrap - point[None, :]), axis=1)
    critical = float(np.quantile(max_deviation, spec.confidence))
    lower = point - critical
    upper = point + critical
    supported = lower > float(spec.economic_epsilon_usdc)
    rows = pd.DataFrame(
        {
            "outer_fold": int(outer_fold),
            "action": candidates,
            "dr_mean_usdc": point,
            "simultaneous_lcb_usdc": lower,
            "simultaneous_ucb_usdc": upper,
            "simultaneous_critical_usdc": critical,
            "economic_epsilon_usdc": float(spec.economic_epsilon_usdc),
            "supported": supported,
            "method": "inner_oof_day_cluster_centered_max_stat",
            "selection_days": int(len(set(evidence.days))),
            "selection_rows": int(len(evidence.days)),
        }
    )
    return rows, tuple(rows.loc[rows["supported"], "action"].astype(str))


def _select_policy(
    q_values: np.ndarray,
    supported_actions: tuple[str, ...],
    spec: JointQuoteDirectValueSpec,
) -> tuple[np.ndarray, np.ndarray]:
    indices = _action_index(spec)
    baseline_index = indices[spec.baseline_action]
    baseline_q = q_values[:, baseline_index]
    if not supported_actions:
        return (
            np.repeat(spec.baseline_action, len(q_values)),
            np.zeros(len(q_values), dtype=float),
        )
    candidate_indices = np.asarray([indices[action] for action in supported_actions], dtype=int)
    candidate_q = q_values[:, candidate_indices]
    best_position = np.argmax(candidate_q, axis=1)
    best_q = candidate_q[np.arange(len(q_values)), best_position]
    best_action = np.asarray(supported_actions, dtype=object)[best_position]
    gain = best_q - baseline_q
    use_candidate = gain > float(spec.economic_epsilon_usdc)
    policy = np.where(use_candidate, best_action, spec.baseline_action).astype(str)
    return policy, np.where(use_candidate, gain, 0.0)


def _policy_day_cluster_ci(
    oof: pd.DataFrame,
    spec: JointQuoteDirectValueSpec,
) -> dict[str, Any]:
    values = oof["dr_policy_minus_baseline_usdc"].to_numpy(dtype=float)[:, None]
    point, bootstrap = _cluster_bootstrap_matrix(
        values,
        oof["day"].to_numpy(dtype=str),
        samples=spec.bootstrap_samples,
        seed=spec.random_seed + 90_000,
    )
    alpha = 1.0 - float(spec.confidence)
    return {
        "estimand": "outer_oof_dr_policy_minus_baseline_usdc_per_assignment",
        "point_usdc": float(point[0]),
        "lower_usdc": float(np.quantile(bootstrap[:, 0], alpha / 2.0)),
        "upper_usdc": float(np.quantile(bootstrap[:, 0], 1.0 - alpha / 2.0)),
        "confidence": float(spec.confidence),
        "cluster": "assignment_UTC_day",
        "bootstrap_samples": int(spec.bootstrap_samples),
        "oof_days": int(oof["day"].nunique()),
        "oof_rows": int(len(oof)),
    }


def evaluate_joint_quote_direct_value_surface(
    panel: pd.DataFrame,
    spec: JointQuoteDirectValueSpec,
) -> JointQuoteDirectValueEvaluation:
    """Evaluate one frozen Development panel without file or permission I/O."""

    frame = validate_panel(panel, spec)
    outer_folds = _expanding_folds(
        spec.development_days,
        min_train_days=spec.min_outer_train_days,
        embargo_days=spec.outer_embargo_days,
        test_days=spec.outer_test_days,
    )
    prediction_rows: list[dict[str, Any]] = []
    oof_rows: list[dict[str, Any]] = []
    selection_parts: list[pd.DataFrame] = []
    chronology_rows: list[dict[str, Any]] = []
    hyperparameter_rows: list[dict[str, Any]] = []
    action_indices = _action_index(spec)
    baseline_index = action_indices[spec.baseline_action]

    for outer in outer_folds:
        outer_train = frame.loc[frame["day"].isin(outer.train_days)].copy()
        outer_test = frame.loc[frame["day"].isin(outer.test_days)].copy()
        _require_action_support(outer_train, spec, scope="outer training fold")
        candidates: list[tuple[float, float, int, _InnerOOF]] = []
        for alpha_order, alpha in enumerate(spec.ridge_alphas):
            evidence = _inner_oof(
                outer_train,
                spec,
                alpha=float(alpha),
                outer_fold=outer.fold,
            )
            mse = _inner_weighted_mse(evidence, spec)
            candidates.append((mse, float(alpha), alpha_order, evidence))
            hyperparameter_rows.append(
                {
                    "outer_fold": int(outer.fold),
                    "alpha": float(alpha),
                    "inner_oof_ipw_mse_usdc2": float(mse),
                    "inner_oof_rows": int(len(evidence.outcomes)),
                    "inner_oof_days": int(len(set(evidence.days))),
                    "selected": False,
                }
            )
        selected = min(candidates, key=lambda item: (item[0], item[2]))
        selected_mse, selected_alpha, _, selected_inner = selected
        for row in hyperparameter_rows:
            if row["outer_fold"] == outer.fold and row["alpha"] == selected_alpha:
                row["selected"] = True

        chronology_rows.append(
            {
                "level": "outer",
                "outer_fold": int(outer.fold),
                "inner_fold": None,
                "alpha": float(selected_alpha),
                "train_min_day": min(outer.train_days),
                "train_max_day": max(outer.train_days),
                "embargo_days": list(outer.embargo_days),
                "test_min_day": min(outer.test_days),
                "test_max_day": max(outer.test_days),
                "future_training_leakage": False,
            }
        )
        chronology_rows.extend(selected_inner.chronology)

        screen, supported_actions = _simultaneous_candidate_screen(
            selected_inner,
            spec,
            outer_fold=outer.fold,
        )
        screen["selected_alpha"] = float(selected_alpha)
        screen["selected_inner_oof_mse_usdc2"] = float(selected_mse)
        screen["outer_train_max_day"] = max(outer.train_days)
        screen["outer_test_min_day"] = min(outer.test_days)
        selection_parts.append(screen)

        outer_model = _fit_direct_q(outer_train, spec, alpha=float(selected_alpha))
        q_values = np.column_stack(
            [_predict_q(outer_model, outer_test, action.name, spec) for action in spec.actions]
        )
        policy, predicted_gain = _select_policy(q_values, supported_actions, spec)
        policy_indices = np.asarray([action_indices[action] for action in policy], dtype=int)
        q_policy = q_values[np.arange(len(outer_test)), policy_indices]
        q_baseline = q_values[:, baseline_index]
        dr = doubly_robust_policy_vs_baseline(
            observed_action=outer_test["action"].to_numpy(dtype=str),
            outcome=outer_test[TARGET_COLUMN].to_numpy(dtype=float),
            observed_propensity=outer_test["behavior_propensity"].to_numpy(dtype=float),
            policy_action=policy,
            baseline_action=spec.baseline_action,
            q_policy=q_policy,
            q_baseline=q_baseline,
        )

        for row_position, source in enumerate(outer_test.itertuples(index=False)):
            for action_position, action in enumerate(spec.actions):
                prediction_rows.append(
                    {
                        "outer_fold": int(outer.fold),
                        "day": str(source.day),
                        "assignment_episode_id": str(source.assignment_episode_id),
                        "observed_action": str(source.action),
                        "predicted_action": action.name,
                        "q_hat_usdc": float(q_values[row_position, action_position]),
                        "selected_alpha": float(selected_alpha),
                        "outer_train_max_day": max(outer.train_days),
                        "outer_test_min_day": min(outer.test_days),
                    }
                )
            oof_rows.append(
                {
                    "outer_fold": int(outer.fold),
                    "day": str(source.day),
                    "assignment_episode_id": str(source.assignment_episode_id),
                    "observed_action": str(source.action),
                    "behavior_propensity": float(source.behavior_propensity),
                    TARGET_COLUMN: float(getattr(source, TARGET_COLUMN)),
                    "policy_action": str(policy[row_position]),
                    "policy_is_baseline": bool(policy[row_position] == spec.baseline_action),
                    "q_policy_usdc": float(q_policy[row_position]),
                    "q_baseline_usdc": float(q_baseline[row_position]),
                    "predicted_gain_usdc": float(predicted_gain[row_position]),
                    "dr_policy_minus_baseline_usdc": float(dr[row_position]),
                    "selected_alpha": float(selected_alpha),
                    "supported_candidate_actions": list(supported_actions),
                    "outer_train_max_day": max(outer.train_days),
                }
            )

    action_predictions = pd.DataFrame(prediction_rows)
    oof_policy = pd.DataFrame(oof_rows)
    if oof_policy.empty or oof_policy["day"].nunique() < 2:
        raise ValueError("outer OOF policy evaluation needs at least two days")
    selection_evidence = pd.concat(selection_parts, ignore_index=True)
    chronology_audit = pd.DataFrame(chronology_rows)
    if not (chronology_audit["train_max_day"] < chronology_audit["test_min_day"]).all():
        raise RuntimeError("future training leakage detected")
    hyperparameter_evidence = pd.DataFrame(hyperparameter_rows)
    policy_ci = _policy_day_cluster_ci(oof_policy, spec)
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": spec.identity,
        "surface_spec_sha256": spec.canonical_sha256,
        "panel_role": PANEL_ROLE,
        "target": {
            "column": TARGET_COLUMN,
            "semantics": TARGET_SEMANTICS,
            "unit": TARGET_UNIT,
            "direct_quantity_weighted_accounting_required_upstream": True,
            "touch_probability_multiplier_applied": False,
            "p3_usage": "ordinary_input_feature_only",
        },
        "model": {
            "family": "pooled_direct_Q_ridge",
            "action_conditioning": (
                "candidate_features_plus_action_indicators_and_context_interactions"
            ),
            "hyperparameter_selection": "inner_past_only_OOF_IPW_MSE",
            "ridge_alphas": [float(value) for value in spec.ridge_alphas],
        },
        "candidate_selection": {
            "method": "inner_oof_DR_day_cluster_centered_max_stat",
            "confidence": float(spec.confidence),
            "economic_epsilon_usdc": float(spec.economic_epsilon_usdc),
            "fallback": spec.baseline_action,
        },
        "outer_oof_policy_vs_baseline": policy_ci,
        "support": {
            "development_days": int(frame["day"].nunique()),
            "development_rows": int(len(frame)),
            "outer_oof_days": int(oof_policy["day"].nunique()),
            "outer_oof_rows": int(len(oof_policy)),
            "policy_baseline_rate": float(oof_policy["policy_is_baseline"].mean()),
        },
        "io": {
            "files_read": 0,
            "artifacts_written": 0,
            "implicit_io": False,
        },
        "permissions": {
            "development_only": True,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    return JointQuoteDirectValueEvaluation(
        action_predictions=action_predictions,
        oof_policy=oof_policy,
        selection_evidence=selection_evidence,
        chronology_audit=chronology_audit,
        hyperparameter_evidence=hyperparameter_evidence,
        report=report,
    )
