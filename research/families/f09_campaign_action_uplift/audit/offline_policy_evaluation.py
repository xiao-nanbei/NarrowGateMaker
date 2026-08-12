#!/usr/bin/env python3
"""Cross-fitted offline policy learning and counterfactual evaluation.

The input is an *action-level* panel: one row per independent quote decision,
the action actually taken by the behavior policy, and the reward subsequently
attributed to that decision.  This is intentionally stricter than the existing
placed-order denominator.  A placed-only table cannot identify the value of
``skip``/``pause`` or an action that the behavior policy never attempted.

The evaluator implements:

* chronological walk-forward or contiguous blocked-day cross-fitting;
* a multinomial behavior-propensity model;
* action-specific outcome models ``Q(x, a)``;
* direct, clipped IPS/SNIPS, and clipped doubly robust estimates;
* action-overlap, effective-sample-size, and unsupported-mass gates;
* an optional supported-action policy learner; and
* day-clustered bootstrap intervals for candidate-minus-behavior uplift.

No result from this module changes replay or live policy.  A formal estimate is
invalid when candidate actions lack logged support, even when a regression model
can extrapolate a numerical value for them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

SCHEMA_VERSION = "offline_policy_evaluation.v1"
SplitMode = Literal["chronological", "blocked"]
FeatureKind = Literal["numeric", "categorical"]


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    kind: FeatureKind = "numeric"
    available_at: str = "decision"
    source_timestamp_col: str = ""
    max_age_ms: float | None = None
    description: str = ""


# Only state visible before the action is allowed by default.  Outcome, fill,
# future markout, and terminal campaign fields must never enter Q/propensity
# features.  A custom registry may extend this list, but formal mode still
# accepts only decision/submit-time fields.
DEFAULT_FEATURE_SPECS: tuple[FeatureSpec, ...] = (
    FeatureSpec("side", "categorical"),
    FeatureSpec("inventory_role", "categorical"),
    FeatureSpec("exposure_increasing"),
    FeatureSpec("order_exposure_increasing"),
    FeatureSpec("inventory"),
    FeatureSpec("inventory_ratio"),
    FeatureSpec("q_before"),
    FeatureSpec("campaign_active"),
    FeatureSpec("campaign_age_s"),
    FeatureSpec("campaign_max_abs_qty_so_far"),
    FeatureSpec("campaign_pnl_so_far"),
    FeatureSpec("campaign_adverse_excursion_so_far"),
    FeatureSpec("campaign_mae_so_far"),
    FeatureSpec("campaign_add_count_so_far"),
    FeatureSpec("campaign_exposure_increasing_fills_so_far"),
    FeatureSpec("campaign_reducing_fills_so_far"),
    FeatureSpec("toxicity"),
    FeatureSpec("markout_ema"),
    FeatureSpec("depth_age_s"),
    FeatureSpec("microprice_shift_bps"),
    FeatureSpec("l2_quote_flip_rate"),
    FeatureSpec("l2_book_refresh_ratio"),
    FeatureSpec("l2_book_cancel_ratio"),
    FeatureSpec("l2_near_depth_total"),
    FeatureSpec("near_depth_total"),
    FeatureSpec("exact_l2_spread_bps"),
    FeatureSpec("queue_init"),
    FeatureSpec("queue_left"),
    FeatureSpec("queue_local_rank"),
    FeatureSpec("queue_regime_mult"),
    FeatureSpec("queue_mo_mult"),
    FeatureSpec("queue_deplete_mult"),
    FeatureSpec("order_age_ms"),
    FeatureSpec("quote_distance_ticks"),
    FeatureSpec("queue_fraction_left"),
    FeatureSpec("spread_ticks"),
    FeatureSpec("book_imbalance"),
    FeatureSpec("maker_expected_ticks"),
    FeatureSpec("empirical_adverse_probability"),
    FeatureSpec("empirical_favorable_probability"),
    FeatureSpec("market_order_intensity"),
    FeatureSpec("cancel_intensity"),
    FeatureSpec("refill_intensity"),
    FeatureSpec("adverse_to_refill_ratio"),
    FeatureSpec("queue_state_key", "categorical"),
    FeatureSpec("microprice_state_key", "categorical"),
    FeatureSpec("fill_cooldown_elapsed_ms"),
    FeatureSpec("fill_cooldown_total_ms"),
    FeatureSpec("fill_cooldown_active_ms"),
    FeatureSpec("fill_cooldown_remaining_ms"),
    FeatureSpec("fill_cooldown_consecutive_units"),
    FeatureSpec("path_feature_valid"),
    FeatureSpec("path_elapsed_ms"),
    FeatureSpec("path_log_elapsed_s"),
    FeatureSpec("path_book_age_ms"),
    FeatureSpec("path_log_book_age_ms"),
    FeatureSpec("path_l2_snapshot_count"),
    FeatureSpec("path_trade_count"),
    FeatureSpec("shock_adverse_flow_imbalance_1s"),
    FeatureSpec("shock_adverse_flow_imbalance_5s"),
    FeatureSpec("shock_adverse_flow_imbalance_since_fill"),
    FeatureSpec("shock_adverse_qty_to_depth_5s"),
    FeatureSpec("shock_log1p_adverse_qty_to_depth_5s"),
    FeatureSpec("shock_adverse_qty_to_depth_since_fill"),
    FeatureSpec("shock_log1p_adverse_qty_to_depth_since_fill"),
    FeatureSpec("shock_adverse_move_bps"),
    FeatureSpec("shock_time_to_extreme_ms"),
    FeatureSpec("shock_log1p_time_to_extreme_ms"),
    FeatureSpec("refill_depletion_ratio"),
    FeatureSpec("refill_recovery_ratio"),
    FeatureSpec("refill_current_vs_start_ratio"),
    FeatureSpec("refill_log1p_current_vs_start_ratio"),
    FeatureSpec("refill_half_life_ms"),
    FeatureSpec("refill_log1p_half_life_ms"),
    FeatureSpec("refill_half_life_observed"),
    FeatureSpec("recovery_current_adverse_bps"),
    FeatureSpec("recovery_from_extreme_bps"),
    FeatureSpec("recovery_price_ratio"),
    FeatureSpec("recovery_microprice_current_adverse_bps"),
    FeatureSpec("recovery_microprice_ratio"),
    FeatureSpec(
        "bid_quote_fill_prob",
        description="Decision-time model prediction, not the realized fill label.",
    ),
    FeatureSpec(
        "ask_quote_fill_prob",
        description="Decision-time model prediction, not the realized fill label.",
    ),
    FeatureSpec("mid"),
    FeatureSpec("best_bid"),
    FeatureSpec("best_ask"),
    FeatureSpec("base_price"),
    FeatureSpec("base_size"),
    FeatureSpec("can_post_after_inventory"),
    FeatureSpec("order_active_before"),
)


@dataclass(frozen=True)
class OPEConfig:
    day_col: str = "day"
    decision_id_col: str = "decision_id"
    decision_ts_col: str = "decision_ts_ns"
    action_col: str = "action"
    reward_col: str = "reward"
    fill_value_col: str = "fill_value"
    campaign_cost_col: str = "campaign_cost"
    queue_cost_col: str = "queue_cost"
    behavior_propensity_col: str = "behavior_propensity"
    behavior_prob_prefix: str = "behavior_prob_"
    candidate_action_col: str = "candidate_action"
    candidate_prob_prefix: str = "candidate_prob_"
    split_mode: SplitMode = "chronological"
    min_train_days: int = 30
    test_days: int = 10
    embargo_days: int = 1
    blocked_folds: int = 5
    min_train_rows: int = 500
    min_action_rows: int = 100
    min_behavior_propensity: float = 0.02
    max_importance_weight: float = 20.0
    max_unsupported_mass: float = 0.05
    min_effective_sample_size: float = 100.0
    min_prediction_coverage: float = 0.98
    ridge_alpha: float = 10.0
    propensity_c: float = 1.0
    bootstrap_trials: int = 500
    random_seed: int = 20260712
    learn_supported_policy: bool = False


@dataclass(frozen=True)
class DayFold:
    fold: int
    train_days: tuple[str, ...]
    test_days: tuple[str, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _feature_registry(path: Path | None) -> dict[str, FeatureSpec]:
    registry = {spec.name: spec for spec in DEFAULT_FEATURE_SPECS}
    if path is None:
        return registry
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("features", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError("feature registry must be a list or {'features': [...]} object")
    for row in rows:
        spec = FeatureSpec(
            name=str(row["name"]),
            kind=str(row.get("kind", "numeric")),
            available_at=str(row.get("available_at", "decision")),
            source_timestamp_col=str(row.get("source_timestamp_col", "")),
            max_age_ms=(
                float(row["max_age_ms"])
                if row.get("max_age_ms") is not None
                else None
            ),
            description=str(row.get("description", "")),
        )
        if spec.kind not in {"numeric", "categorical"}:
            raise ValueError(f"unsupported feature kind for {spec.name}: {spec.kind}")
        registry[spec.name] = spec
    return registry


def resolve_feature_specs(
    frame: pd.DataFrame,
    requested: Sequence[str] | None,
    *,
    registry: dict[str, FeatureSpec],
) -> list[FeatureSpec]:
    names = list(requested or [name for name in registry if name in frame.columns])
    if not names:
        raise ValueError("no decision-time feature columns were found or requested")
    missing = [name for name in names if name not in frame.columns]
    if missing:
        raise ValueError(f"feature columns missing from panel: {missing}")
    unregistered = [name for name in names if name not in registry]
    if unregistered:
        raise ValueError(
            "formal OPE rejects unregistered features; add causal provenance to "
            f"--feature-registry first: {unregistered}"
        )
    invalid = [
        name
        for name in names
        if registry[name].available_at not in {"decision", "submit"}
    ]
    if invalid:
        raise ValueError(f"post-action/terminal features cannot be used for OPE: {invalid}")
    return [registry[name] for name in names]


def validate_feature_timing(
    frame: pd.DataFrame,
    specs: Sequence[FeatureSpec],
    *,
    config: OPEConfig,
) -> None:
    timestamped = [spec for spec in specs if spec.source_timestamp_col]
    if not timestamped:
        return
    if config.decision_ts_col not in frame:
        raise ValueError(
            f"timestamped features require decision timestamp column "
            f"{config.decision_ts_col!r}"
        )
    decision_ts = pd.to_numeric(frame[config.decision_ts_col], errors="coerce")
    if decision_ts.isna().any():
        raise ValueError(f"{config.decision_ts_col} contains missing/non-numeric values")
    for spec in timestamped:
        if spec.source_timestamp_col not in frame:
            raise ValueError(
                f"feature {spec.name!r} requires source timestamp column "
                f"{spec.source_timestamp_col!r}"
            )
        source_ts = pd.to_numeric(frame[spec.source_timestamp_col], errors="coerce")
        if source_ts.isna().any():
            raise ValueError(
                f"{spec.source_timestamp_col} contains missing/non-numeric values"
            )
        future = source_ts > decision_ts
        if future.any():
            raise ValueError(
                f"feature {spec.name!r} has {int(future.sum())} source timestamps "
                "after the action decision"
            )
        if spec.max_age_ms is not None:
            age_ms = (decision_ts - source_ts) / 1_000_000.0
            stale = age_ms > spec.max_age_ms
            if stale.any():
                raise ValueError(
                    f"feature {spec.name!r} has {int(stale.sum())} rows older than "
                    f"its {spec.max_age_ms:g}ms provenance budget"
                )


def make_day_folds(days: Sequence[str], cfg: OPEConfig) -> list[DayFold]:
    ordered = sorted({str(day) for day in days if str(day)})
    if len(ordered) < 2:
        raise ValueError("offline policy evaluation requires at least two UTC days")
    folds: list[DayFold] = []
    if cfg.split_mode == "chronological":
        cursor = cfg.min_train_days + cfg.embargo_days
        fold_id = 0
        while cursor < len(ordered):
            train_end = cursor - cfg.embargo_days
            train = ordered[:train_end]
            test = ordered[cursor : cursor + cfg.test_days]
            if train and test:
                folds.append(DayFold(fold_id, tuple(train), tuple(test)))
                fold_id += 1
            cursor += max(cfg.test_days, 1)
    elif cfg.split_mode == "blocked":
        positions = np.array_split(np.arange(len(ordered)), min(cfg.blocked_folds, len(ordered)))
        for fold_id, test_idx_raw in enumerate(positions):
            test_idx = [int(value) for value in test_idx_raw]
            if not test_idx:
                continue
            blocked = set(test_idx)
            for idx in test_idx:
                for delta in range(1, cfg.embargo_days + 1):
                    blocked.add(idx - delta)
                    blocked.add(idx + delta)
            train = [day for idx, day in enumerate(ordered) if idx not in blocked]
            test = [ordered[idx] for idx in test_idx]
            if train and test:
                folds.append(DayFold(fold_id, tuple(train), tuple(test)))
    else:
        raise ValueError(f"unsupported split mode: {cfg.split_mode}")
    if not folds:
        raise ValueError(
            "no evaluable folds; reduce min_train_days/embargo_days or provide more days"
        )
    return folds


def _prepare_panel(frame: pd.DataFrame, cfg: OPEConfig) -> pd.DataFrame:
    required = {cfg.day_col, cfg.action_col}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"action panel is missing required columns: {missing}")
    out = frame.copy()
    out[cfg.day_col] = out[cfg.day_col].astype("string").fillna("").str.slice(0, 10)
    out[cfg.action_col] = (
        out[cfg.action_col].astype("string").fillna("").str.strip().str.lower()
    )
    if cfg.decision_id_col not in out:
        out[cfg.decision_id_col] = [f"row-{idx}" for idx in range(len(out))]
    if cfg.reward_col in out:
        out["_ope_reward"] = pd.to_numeric(out[cfg.reward_col], errors="coerce")
    else:
        component_cols = (cfg.fill_value_col, cfg.campaign_cost_col, cfg.queue_cost_col)
        missing_components = [name for name in component_cols if name not in out]
        if missing_components:
            raise ValueError(
                f"provide {cfg.reward_col!r} or all reward components: {missing_components}"
            )
        fill_value = pd.to_numeric(out[cfg.fill_value_col], errors="coerce")
        campaign_cost = pd.to_numeric(out[cfg.campaign_cost_col], errors="coerce")
        queue_cost = pd.to_numeric(out[cfg.queue_cost_col], errors="coerce")
        out["_ope_reward"] = fill_value - campaign_cost - queue_cost
    keep = (
        out[cfg.day_col].ne("")
        & out[cfg.action_col].ne("")
        & np.isfinite(out["_ope_reward"].to_numpy(dtype=float))
    )
    out = out.loc[keep].reset_index(drop=True)
    if out.empty:
        raise ValueError("no finite action/reward rows remain after normalization")
    duplicated = out[cfg.decision_id_col].astype(str).duplicated(keep=False)
    if duplicated.any():
        examples = out.loc[duplicated, cfg.decision_id_col].astype(str).head(5).tolist()
        raise ValueError(f"decision_id must be unique; duplicates include {examples}")
    return out


def _make_preprocessor(specs: Sequence[FeatureSpec]):
    try:
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
    except ImportError as exc:  # pragma: no cover - CI installs research extras
        raise RuntimeError("offline policy evaluation requires scikit-learn") from exc

    numeric = [spec.name for spec in specs if spec.kind == "numeric"]
    categorical = [spec.name for spec in specs if spec.kind == "categorical"]
    transformers: list[tuple[str, Any, list[str]]] = []
    if numeric:
        transformers.append(
            (
                "numeric",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ]
                ),
                numeric,
            )
        )
    if categorical:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        (
                            "onehot",
                            OneHotEncoder(handle_unknown="ignore", sparse_output=True),
                        ),
                    ]
                ),
                categorical,
            )
        )
    return ColumnTransformer(transformers=transformers, remainder="drop")


def _feature_frame(frame: pd.DataFrame, specs: Sequence[FeatureSpec]) -> pd.DataFrame:
    output = frame[[spec.name for spec in specs]].copy()
    for spec in specs:
        if spec.kind == "numeric":
            output[spec.name] = pd.to_numeric(output[spec.name], errors="coerce")
        else:
            output[spec.name] = output[spec.name].astype("string")
    return output


def _fit_propensity(
    train: pd.DataFrame,
    test: pd.DataFrame,
    specs: Sequence[FeatureSpec],
    actions: Sequence[str],
    cfg: OPEConfig,
) -> tuple[np.ndarray, dict[str, Any]]:
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline

    y = train[cfg.action_col].astype(str).to_numpy()
    train_actions = sorted(set(y))
    probabilities = np.zeros((len(test), len(actions)), dtype=float)
    if len(train_actions) == 1:
        probabilities[:, list(actions).index(train_actions[0])] = 1.0
        return probabilities, {"train_actions": train_actions, "model": "constant"}
    model = Pipeline(
        [
            ("features", _make_preprocessor(specs)),
            (
                "model",
                LogisticRegression(
                    C=cfg.propensity_c,
                    max_iter=1_000,
                    solver="saga",
                    random_state=cfg.random_seed,
                ),
            ),
        ]
    )
    model.fit(_feature_frame(train, specs), y)
    fitted = model.predict_proba(_feature_frame(test, specs))
    classes = [str(value) for value in model.named_steps["model"].classes_]
    for class_idx, action in enumerate(classes):
        probabilities[:, list(actions).index(action)] = fitted[:, class_idx]
    return probabilities, {"train_actions": train_actions, "model": "multinomial_logit"}


def _logged_behavior_probabilities(
    frame: pd.DataFrame,
    *,
    actions: Sequence[str],
    cfg: OPEConfig,
) -> tuple[np.ndarray | None, dict[str, Any]]:
    """Read a pre-registered behavior-policy probability vector when available."""

    columns = {
        column[len(cfg.behavior_prob_prefix) :].strip().lower(): column
        for column in frame.columns
        if column.startswith(cfg.behavior_prob_prefix)
    }
    if not columns:
        return None, {}
    unknown = sorted(set(columns) - set(actions))
    if unknown:
        raise ValueError(f"behavior probability columns use unknown actions: {unknown}")
    missing = sorted(set(actions) - set(columns))
    if missing:
        raise ValueError(
            "a logged behavior probability vector must include every action; "
            f"missing: {missing}"
        )

    probabilities = np.column_stack(
        [
            pd.to_numeric(frame[columns[action]], errors="coerce").to_numpy(dtype=float)
            for action in actions
        ]
    )
    if not np.isfinite(probabilities).all():
        raise ValueError("behavior_prob_* columns must be finite")
    if (probabilities < -1e-12).any() or (probabilities > 1.0 + 1e-12).any():
        raise ValueError("behavior_prob_* columns must lie between zero and one")
    if not np.allclose(probabilities.sum(axis=1), 1.0, atol=1e-8, rtol=0.0):
        raise ValueError("behavior_prob_* columns must sum to 1 on every row")

    action_index = {action: idx for idx, action in enumerate(actions)}
    logged_indices = np.asarray(
        [action_index[action] for action in frame[cfg.action_col].astype(str)], dtype=int
    )
    logged = probabilities[np.arange(len(frame)), logged_indices]
    if (logged <= 0.0).any():
        raise ValueError("the logged action must have positive behavior probability")
    if cfg.behavior_propensity_col in frame:
        supplied = pd.to_numeric(
            frame[cfg.behavior_propensity_col], errors="coerce"
        ).to_numpy(dtype=float)
        if not np.isfinite(supplied).all():
            raise ValueError(f"{cfg.behavior_propensity_col} must be finite")
        if not np.allclose(supplied, logged, atol=1e-8, rtol=0.0):
            raise ValueError(
                f"{cfg.behavior_propensity_col} disagrees with the logged action's "
                "behavior_prob_* value"
            )
    return probabilities, {
        "train_actions": sorted(set(frame[cfg.action_col].astype(str))),
        "model": "logged_probability_vector",
    }


def _fit_outcomes(
    train: pd.DataFrame,
    test: pd.DataFrame,
    specs: Sequence[FeatureSpec],
    actions: Sequence[str],
    cfg: OPEConfig,
) -> tuple[np.ndarray, dict[str, int]]:
    from sklearn.linear_model import Ridge

    q_values = np.full((len(test), len(actions)), np.nan, dtype=float)
    support: dict[str, int] = {}
    x_test = _feature_frame(test, specs)
    for action_idx, action in enumerate(actions):
        subset = train[train[cfg.action_col].astype(str) == action]
        support[action] = len(subset)
        if len(subset) < cfg.min_action_rows:
            continue
        features = _make_preprocessor(specs)
        train_matrix = features.fit_transform(_feature_frame(subset, specs))
        test_matrix = features.transform(x_test)
        model = Ridge(alpha=cfg.ridge_alpha, solver="lsqr")
        model.fit(
            train_matrix,
            subset["_ope_reward"].to_numpy(dtype=float),
        )
        # NumPy's ``@`` path emits spurious overflow/divide warnings with the
        # Accelerate build used on the local research host even when every
        # matrix/coefficient/prediction is finite. ``ndarray.dot`` and sparse
        # ``dot`` produce the same linear prediction without those false
        # warnings. Keep an explicit finite check so genuine instability still
        # fails the fold.
        prediction = np.asarray(test_matrix.dot(model.coef_), dtype=float).reshape(-1)
        prediction += float(model.intercept_)
        if not np.isfinite(prediction).all():
            raise FloatingPointError(
                f"non-finite outcome prediction for action {action!r}"
            )
        q_values[:, action_idx] = prediction
    return q_values, support


def _candidate_probabilities(
    test: pd.DataFrame,
    *,
    actions: Sequence[str],
    behavior_probabilities: np.ndarray,
    q_values: np.ndarray,
    cfg: OPEConfig,
) -> tuple[np.ndarray, list[str], str]:
    probs = np.zeros((len(test), len(actions)), dtype=float)
    action_index = {action: idx for idx, action in enumerate(actions)}
    probability_columns = {
        col[len(cfg.candidate_prob_prefix) :].strip().lower(): col
        for col in test.columns
        if col.startswith(cfg.candidate_prob_prefix)
    }
    selected: list[str] = []
    if probability_columns:
        unknown = sorted(set(probability_columns) - set(actions))
        if unknown:
            raise ValueError(f"candidate probability columns use unknown actions: {unknown}")
        for action, column in probability_columns.items():
            probs[:, action_index[action]] = pd.to_numeric(
                test[column], errors="coerce"
            ).fillna(0.0)
        if (probs < -1e-12).any():
            raise ValueError("candidate probabilities must be non-negative")
        row_sums = probs.sum(axis=1)
        if (row_sums <= 0.0).any():
            raise ValueError("each row must assign positive candidate probability mass")
        if not np.allclose(row_sums, 1.0, atol=1e-6, rtol=0.0):
            raise ValueError("candidate_prob_* columns must sum to 1 on every row")
        selected = [actions[int(np.argmax(row))] for row in probs]
        return probs, selected, "probability_columns"
    if cfg.candidate_action_col in test:
        selected = (
            test[cfg.candidate_action_col]
            .astype("string")
            .fillna("")
            .str.strip()
            .str.lower()
            .tolist()
        )
        if any(not action for action in selected):
            raise ValueError("candidate_action contains missing/empty actions")
        unknown = sorted(set(selected) - set(actions))
        if unknown:
            raise ValueError(f"candidate_action contains actions absent from panel: {unknown}")
        for row_idx, action in enumerate(selected):
            probs[row_idx, action_index[action]] = 1.0
        return probs, selected, "candidate_action_column"
    if not cfg.learn_supported_policy:
        raise ValueError(
            "provide candidate_action/candidate_prob_* columns or enable "
            "--learn-supported-policy"
        )

    for row_idx in range(len(test)):
        supported = [
            idx
            for idx in range(len(actions))
            if math.isfinite(q_values[row_idx, idx])
            and behavior_probabilities[row_idx, idx] >= cfg.min_behavior_propensity
        ]
        if not supported:
            supported = [
                idx for idx in range(len(actions)) if math.isfinite(q_values[row_idx, idx])
            ]
        if not supported:
            selected.append("")
            continue
        best = max(supported, key=lambda idx: q_values[row_idx, idx])
        probs[row_idx, best] = 1.0
        selected.append(actions[best])
    return probs, selected, "cross_fitted_supported_q_argmax"


def _fold_predictions(
    panel: pd.DataFrame,
    fold: DayFold,
    specs: Sequence[FeatureSpec],
    actions: Sequence[str],
    cfg: OPEConfig,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    train = panel[panel[cfg.day_col].isin(fold.train_days)].copy()
    test = panel[panel[cfg.day_col].isin(fold.test_days)].copy()
    if len(train) < cfg.min_train_rows or test.empty:
        return pd.DataFrame(), {
            "fold": fold.fold,
            "status": "insufficient_rows",
            "train_rows": len(train),
            "test_rows": len(test),
        }
    behavior_probs, propensity_meta = _logged_behavior_probabilities(
        test,
        actions=actions,
        cfg=cfg,
    )
    if behavior_probs is None:
        behavior_probs, propensity_meta = _fit_propensity(
            train, test, specs, actions, cfg
        )
    q_values, action_support = _fit_outcomes(train, test, specs, actions, cfg)
    candidate_probs, candidate_actions, candidate_source = _candidate_probabilities(
        test,
        actions=actions,
        behavior_probabilities=behavior_probs,
        q_values=q_values,
        cfg=cfg,
    )

    action_index = {action: idx for idx, action in enumerate(actions)}
    logged_indices = np.asarray(
        [action_index[action] for action in test[cfg.action_col].astype(str)], dtype=int
    )
    row_indices = np.arange(len(test))
    model_logged_propensity = behavior_probs[row_indices, logged_indices]
    exact_behavior_vector = propensity_meta.get("model") == "logged_probability_vector"
    if exact_behavior_vector:
        logged_propensity = model_logged_propensity
        propensity_source = "logged_probability_vector"
    elif cfg.behavior_propensity_col in test:
        supplied = pd.to_numeric(
            test[cfg.behavior_propensity_col], errors="coerce"
        ).to_numpy(dtype=float)
        use_supplied = np.isfinite(supplied) & (supplied > 0.0) & (supplied <= 1.0)
        logged_propensity = np.where(use_supplied, supplied, model_logged_propensity)
        propensity_source = "logged_column_with_model_fallback"
    else:
        logged_propensity = model_logged_propensity
        propensity_source = "cross_fitted_model"
    q_logged = q_values[row_indices, logged_indices]
    pi_logged = candidate_probs[row_indices, logged_indices]

    q_available = np.isfinite(q_values)
    candidate_q_missing_mass = np.sum(candidate_probs * (~q_available), axis=1)
    safe_q = np.where(q_available, q_values, 0.0)
    dm = np.sum(candidate_probs * safe_q, axis=1)
    dm[candidate_q_missing_mass > 1e-12] = np.nan

    overlap_available = q_available & (
        behavior_probs >= cfg.min_behavior_propensity
    )
    supported_mass = np.sum(candidate_probs * overlap_available, axis=1)
    unsupported_mass = 1.0 - supported_mass
    raw_weight = np.divide(
        pi_logged,
        np.maximum(logged_propensity, 1e-12),
        out=np.zeros_like(pi_logged),
        where=np.isfinite(logged_propensity),
    )
    clipped_weight = np.minimum(raw_weight, cfg.max_importance_weight)
    reward = test["_ope_reward"].to_numpy(dtype=float)
    correction_q_available = np.isfinite(q_logged) | (pi_logged <= 1e-12)
    valid = np.isfinite(dm) & correction_q_available & np.isfinite(logged_propensity)
    dr = np.full(len(test), np.nan, dtype=float)
    ips = np.full(len(test), np.nan, dtype=float)
    safe_q_logged = np.where(np.isfinite(q_logged), q_logged, 0.0)
    dr[valid] = dm[valid] + clipped_weight[valid] * (
        reward[valid] - safe_q_logged[valid]
    )
    ips[valid] = clipped_weight[valid] * reward[valid]

    result = test.copy()
    result["ope_fold"] = fold.fold
    result["ope_candidate_action"] = candidate_actions
    result["ope_candidate_source"] = candidate_source
    result["ope_behavior_propensity"] = logged_propensity
    result["ope_model_behavior_propensity"] = model_logged_propensity
    result["ope_candidate_logged_probability"] = pi_logged
    result["ope_raw_importance_weight"] = raw_weight
    result["ope_clipped_importance_weight"] = clipped_weight
    result["ope_q_logged"] = q_logged
    result["ope_dm_value"] = dm
    result["ope_ips_value"] = ips
    result["ope_dr_value"] = dr
    result["ope_supported_candidate_mass"] = supported_mass
    result["ope_unsupported_candidate_mass"] = unsupported_mass
    result["ope_prediction_valid"] = valid.astype(int)
    for action_idx, action in enumerate(actions):
        result[f"ope_behavior_prob_{action}"] = behavior_probs[:, action_idx]
        result[f"ope_candidate_prob_{action}"] = candidate_probs[:, action_idx]
        result[f"ope_q_{action}"] = q_values[:, action_idx]

    valid_weights = clipped_weight[valid]
    ess = (
        float(valid_weights.sum() ** 2 / np.square(valid_weights).sum())
        if np.square(valid_weights).sum() > 0.0
        else 0.0
    )
    fold_meta = {
        "fold": fold.fold,
        "status": "ok",
        "train_days": list(fold.train_days),
        "test_days": list(fold.test_days),
        "train_rows": len(train),
        "test_rows": len(test),
        "prediction_coverage": float(valid.mean()),
        "unsupported_candidate_mass": float(np.mean(unsupported_mass)),
        "effective_sample_size": ess,
        "behavior_observed_value": (
            float(np.mean(reward[valid])) if valid.any() else math.nan
        ),
        "candidate_direct_value": (
            float(np.mean(dm[valid])) if valid.any() else math.nan
        ),
        "candidate_clipped_ips_value": (
            float(np.mean(ips[valid])) if valid.any() else math.nan
        ),
        "candidate_clipped_dr_value": (
            float(np.mean(dr[valid])) if valid.any() else math.nan
        ),
        "candidate_minus_behavior_dr_uplift": (
            float(np.mean(dr[valid]) - np.mean(reward[valid]))
            if valid.any()
            else math.nan
        ),
        "propensity_source": propensity_source,
        "candidate_source": candidate_source,
        "propensity_model": propensity_meta,
        "train_action_rows": action_support,
    }
    return result, fold_meta


def _finite_mean(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce")
    return float(numeric.mean()) if numeric.notna().any() else math.nan


def _cluster_bootstrap(
    rows: pd.DataFrame,
    *,
    day_col: str,
    trials: int,
    seed: int,
) -> dict[str, float]:
    valid = rows[np.isfinite(pd.to_numeric(rows["ope_dr_value"], errors="coerce"))].copy()
    if valid.empty or trials <= 0:
        return {"trials": 0, "uplift_p025": math.nan, "uplift_p50": math.nan, "uplift_p975": math.nan}
    valid["_uplift"] = valid["ope_dr_value"].astype(float) - valid["_ope_reward"].astype(float)
    clusters = valid.groupby(day_col, sort=True)["_uplift"].agg(["sum", "count"])
    if clusters.empty:
        return {"trials": 0, "uplift_p025": math.nan, "uplift_p50": math.nan, "uplift_p975": math.nan}
    sums = clusters["sum"].to_numpy(dtype=float)
    counts = clusters["count"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    samples = np.empty(trials, dtype=float)
    for idx in range(trials):
        chosen = rng.integers(0, len(clusters), size=len(clusters))
        samples[idx] = sums[chosen].sum() / max(counts[chosen].sum(), 1.0)
    return {
        "trials": int(trials),
        "cluster_days": int(len(clusters)),
        "uplift_p025": float(np.quantile(samples, 0.025)),
        "uplift_p50": float(np.quantile(samples, 0.50)),
        "uplift_p975": float(np.quantile(samples, 0.975)),
    }


def _summarize(
    rows: pd.DataFrame,
    fold_rows: list[dict[str, Any]],
    actions: Sequence[str],
    specs: Sequence[FeatureSpec],
    cfg: OPEConfig,
) -> tuple[dict[str, Any], pd.DataFrame]:
    valid = rows[rows["ope_prediction_valid"] == 1].copy()
    weights = valid["ope_clipped_importance_weight"].to_numpy(dtype=float)
    ess = (
        float(weights.sum() ** 2 / np.square(weights).sum())
        if np.square(weights).sum() > 0.0
        else 0.0
    )
    observed = _finite_mean(valid["_ope_reward"])
    dm = _finite_mean(valid["ope_dm_value"])
    ips = _finite_mean(valid["ope_ips_value"])
    weight_sum = float(weights.sum())
    snips = (
        float(
            np.nansum(
                valid["ope_clipped_importance_weight"].to_numpy(dtype=float)
                * valid["_ope_reward"].to_numpy(dtype=float)
            )
            / weight_sum
        )
        if weight_sum > 0.0
        else math.nan
    )
    dr = _finite_mean(valid["ope_dr_value"])
    daily_uplift = (
        valid.assign(
            _ope_uplift=(
                pd.to_numeric(valid["ope_dr_value"], errors="coerce")
                - pd.to_numeric(valid["_ope_reward"], errors="coerce")
            )
        )
        .groupby(cfg.day_col, sort=True)["_ope_uplift"]
        .mean()
    )
    daily_positive = int((daily_uplift > 0.0).sum())
    daily_negative = int((daily_uplift < 0.0).sum())
    daily_zero = int((daily_uplift == 0.0).sum())
    coverage = len(valid) / max(len(rows), 1)
    unsupported = _finite_mean(rows["ope_unsupported_candidate_mass"])
    bootstrap = _cluster_bootstrap(
        valid,
        day_col=cfg.day_col,
        trials=cfg.bootstrap_trials,
        seed=cfg.random_seed,
    )
    action_rows: list[dict[str, Any]] = []
    for action in actions:
        logged = rows[rows[cfg.action_col] == action]
        candidate_mass = pd.to_numeric(
            rows[f"ope_candidate_prob_{action}"], errors="coerce"
        ).fillna(0.0)
        action_rows.append(
            {
                "action": action,
                "logged_rows": int(len(logged)),
                "logged_reward_mean": _finite_mean(logged["_ope_reward"]),
                "candidate_probability_mass": float(candidate_mass.sum()),
                "candidate_row_equivalent": float(candidate_mass.mean()),
                "mean_behavior_probability": _finite_mean(rows[f"ope_behavior_prob_{action}"]),
                "mean_q": _finite_mean(rows[f"ope_q_{action}"]),
            }
        )
    action_frame = pd.DataFrame(action_rows)
    candidate_actions = action_frame[action_frame["candidate_probability_mass"] > 1e-12]
    unsupported_actions = candidate_actions[candidate_actions["logged_rows"] < cfg.min_action_rows][
        "action"
    ].tolist()
    overlap_pass = (
        coverage >= cfg.min_prediction_coverage
        and unsupported <= cfg.max_unsupported_mass
        and ess >= cfg.min_effective_sample_size
        and not unsupported_actions
    )
    summary = {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "overlap_gates_passed_assumptions_required"
            if overlap_pass
            else "diagnostic_only_overlap_failed"
        ),
        "numerical_ope_gate_passed": bool(overlap_pass),
        "formal_estimate_valid": bool(overlap_pass),
        "causal_identification_proven": False,
        "rows": int(len(rows)),
        "valid_prediction_rows": int(len(valid)),
        "prediction_coverage": coverage,
        "days": int(rows[cfg.day_col].nunique()),
        "actions": list(actions),
        "candidate_source": sorted(set(rows["ope_candidate_source"].astype(str))),
        "feature_specs": [asdict(spec) for spec in specs],
        "estimators": {
            "behavior_observed_value": observed,
            "candidate_direct_value": dm,
            "candidate_clipped_ips_value": ips,
            "candidate_clipped_snips_value": snips,
            "candidate_clipped_dr_value": dr,
            "candidate_minus_behavior_dr_uplift": dr - observed,
        },
        "overlap": {
            "mean_unsupported_candidate_mass": unsupported,
            "effective_sample_size": ess,
            "min_effective_sample_size": cfg.min_effective_sample_size,
            "min_behavior_propensity": cfg.min_behavior_propensity,
            "max_importance_weight": cfg.max_importance_weight,
            "raw_weight_p95": float(rows["ope_raw_importance_weight"].quantile(0.95)),
            "raw_weight_p99": float(rows["ope_raw_importance_weight"].quantile(0.99)),
            "raw_weight_max": float(rows["ope_raw_importance_weight"].max()),
            "unsupported_actions": unsupported_actions,
        },
        "day_cluster_bootstrap": bootstrap,
        "daily_uplift": {
            "days": int(len(daily_uplift)),
            "positive_days": daily_positive,
            "negative_days": daily_negative,
            "zero_days": daily_zero,
            "positive_rate": (
                float(daily_positive / len(daily_uplift))
                if len(daily_uplift)
                else math.nan
            ),
            "median": (
                float(daily_uplift.median()) if len(daily_uplift) else math.nan
            ),
        },
        "folds": fold_rows,
        "warning": (
            "DR/OPE identifies only actions with behavior-policy overlap. A deterministic "
            "baseline cannot identify never-tried keep/skip/recenter/widen actions without "
            "randomized logging or a replay counterfactual whose queue/campaign semantics pass parity."
        ),
        "identification_assumptions": [
            "consistency: the logged action has the same meaning as the evaluated action",
            "conditional exchangeability: all joint causes of action and reward are in x",
            "positivity/overlap: candidate actions have behavior-policy support at x",
            "well-defined decision unit with no unmodeled cross-row interference",
            "causal feature timestamps do not exceed the decision timestamp",
            "reward attribution does not duplicate campaign outcomes across decisions",
        ],
    }
    return summary, action_frame


def evaluate_offline_policy(
    frame: pd.DataFrame,
    *,
    feature_names: Sequence[str] | None = None,
    feature_registry_path: Path | None = None,
    config: OPEConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    cfg = config or OPEConfig()
    panel = _prepare_panel(frame, cfg)
    registry = _feature_registry(feature_registry_path)
    specs = resolve_feature_specs(panel, feature_names, registry=registry)
    validate_feature_timing(panel, specs, config=cfg)
    candidate_actions: set[str] = set()
    if cfg.candidate_action_col in panel:
        normalized_candidate_actions = (
            panel[cfg.candidate_action_col]
            .astype("string")
            .fillna("")
            .str.strip()
            .str.lower()
        )
        if normalized_candidate_actions.eq("").any():
            raise ValueError("candidate_action contains missing/empty actions")
        panel[cfg.candidate_action_col] = normalized_candidate_actions
        candidate_actions.update(normalized_candidate_actions)
    for column in panel.columns:
        if column.startswith(cfg.candidate_prob_prefix):
            candidate_actions.add(column[len(cfg.candidate_prob_prefix) :].strip().lower())
    actions = sorted(
        set(panel[cfg.action_col].astype(str)) | {action for action in candidate_actions if action}
    )
    _logged_behavior_probabilities(panel, actions=actions, cfg=cfg)
    folds = make_day_folds(panel[cfg.day_col].tolist(), cfg)
    predictions: list[pd.DataFrame] = []
    fold_rows: list[dict[str, Any]] = []
    for fold in folds:
        rows, metadata = _fold_predictions(panel, fold, specs, actions, cfg)
        fold_rows.append(metadata)
        if not rows.empty:
            predictions.append(rows)
    if not predictions:
        raise ValueError("all OPE folds were skipped because of insufficient training/test rows")
    output = pd.concat(predictions, ignore_index=True)
    summary, action_frame = _summarize(output, fold_rows, actions, specs, cfg)
    return output, pd.DataFrame(fold_rows), action_frame, summary


def evaluate_fixed_holdout_policy(
    training_frame: pd.DataFrame,
    holdout_frame: pd.DataFrame,
    *,
    feature_names: Sequence[str] | None = None,
    feature_registry_path: Path | None = None,
    config: OPEConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Fit once on frozen development days and evaluate disjoint later days."""

    cfg = config or OPEConfig()
    training = _prepare_panel(training_frame, cfg)
    holdout = _prepare_panel(holdout_frame, cfg)
    train_days = tuple(sorted(set(training[cfg.day_col].astype(str))))
    holdout_days = tuple(sorted(set(holdout[cfg.day_col].astype(str))))
    overlap_days = sorted(set(train_days) & set(holdout_days))
    if overlap_days:
        raise ValueError(f"fixed holdout overlaps training days: {overlap_days}")
    combined = pd.concat([training, holdout], ignore_index=True)
    duplicated = combined[cfg.decision_id_col].astype(str).duplicated(keep=False)
    if duplicated.any():
        examples = (
            combined.loc[duplicated, cfg.decision_id_col]
            .astype(str)
            .head(5)
            .tolist()
        )
        raise ValueError(
            f"decision_id must be unique across train/holdout; duplicates include {examples}"
        )

    registry = _feature_registry(feature_registry_path)
    specs = resolve_feature_specs(combined, feature_names, registry=registry)
    validate_feature_timing(combined, specs, config=cfg)
    candidate_actions: set[str] = set()
    if cfg.candidate_action_col in combined:
        normalized = (
            combined[cfg.candidate_action_col]
            .astype("string")
            .fillna("")
            .str.strip()
            .str.lower()
        )
        if normalized.eq("").any():
            raise ValueError("candidate_action contains missing/empty actions")
        combined[cfg.candidate_action_col] = normalized
        candidate_actions.update(normalized)
    for column in combined.columns:
        if column.startswith(cfg.candidate_prob_prefix):
            candidate_actions.add(
                column[len(cfg.candidate_prob_prefix) :].strip().lower()
            )
    actions = sorted(
        set(combined[cfg.action_col].astype(str))
        | {action for action in candidate_actions if action}
    )
    _logged_behavior_probabilities(combined, actions=actions, cfg=cfg)
    fold = DayFold(0, train_days, holdout_days)
    rows, metadata = _fold_predictions(combined, fold, specs, actions, cfg)
    if rows.empty:
        raise ValueError("fixed holdout was skipped because of insufficient rows")
    metadata["split_type"] = "fixed_development_to_later_holdout"
    summary, action_frame = _summarize(rows, [metadata], actions, specs, cfg)
    summary["evaluation_design"] = {
        "split_type": "fixed_development_to_later_holdout",
        "train_days": list(train_days),
        "holdout_days": list(holdout_days),
        "holdout_used_for_fit": False,
    }
    return rows, pd.DataFrame([metadata]), action_frame, summary


def _markdown(summary: dict[str, Any], actions: pd.DataFrame) -> str:
    estimators = summary["estimators"]
    overlap = summary["overlap"]
    bootstrap = summary["day_cluster_bootstrap"]
    lines = [
        "# Offline Policy Evaluation",
        "",
        f"- Schema: `{summary['schema_version']}`",
        f"- Status: **{summary['status']}**",
        f"- Rows / days: `{summary['rows']}` / `{summary['days']}`",
        f"- Prediction coverage: `{summary['prediction_coverage']:.4f}`",
        "",
        "## Value Estimates",
        "",
        "| estimator | value |",
        "|---|---:|",
    ]
    for key, value in estimators.items():
        lines.append(f"| `{key}` | {value:.8f} |")
    lines.extend(
        [
            "",
            "## Overlap Gate",
            "",
            f"- Numerical OPE gate passed: `{summary['numerical_ope_gate_passed']}`",
            f"- Causal identification proven by this tool: `{summary['causal_identification_proven']}`",
            f"- Unsupported candidate mass: `{overlap['mean_unsupported_candidate_mass']:.6f}`",
            f"- Effective sample size: `{overlap['effective_sample_size']:.2f}`",
            f"- Raw weight p99 / max: `{overlap['raw_weight_p99']:.3f}` / `{overlap['raw_weight_max']:.3f}`",
            f"- Unsupported actions: `{overlap['unsupported_actions']}`",
            "",
            "## Day-Clustered Uplift Interval",
            "",
            f"- p2.5 / p50 / p97.5: `{bootstrap['uplift_p025']:.8f}` / "
            f"`{bootstrap['uplift_p50']:.8f}` / `{bootstrap['uplift_p975']:.8f}`",
            f"- Daily positive / negative / zero: "
            f"`{summary['daily_uplift']['positive_days']}` / "
            f"`{summary['daily_uplift']['negative_days']}` / "
            f"`{summary['daily_uplift']['zero_days']}`",
            "",
            "## Identification Assumptions",
            "",
        ]
    )
    for assumption in summary["identification_assumptions"]:
        lines.append(f"- {assumption}")
    lines.extend(
        [
            "",
            "## Action Support",
            "",
            "| action | logged rows | candidate row equivalent | mean reward | mean behavior p |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in actions.to_dict("records"):
        lines.append(
            f"| `{row['action']}` | {row['logged_rows']} | "
            f"{row['candidate_row_equivalent']:.6f} | "
            f"{row['logged_reward_mean']:.8f} | {row['mean_behavior_probability']:.6f} |"
        )
    lines.extend(["", f"> {summary['warning']}", ""])
    return "\n".join(lines)


def write_outputs(
    prefix: Path,
    rows: pd.DataFrame,
    folds: pd.DataFrame,
    actions: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, str]:
    prefix.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "rows": str(prefix.with_suffix(".ope_rows.csv")),
        "folds": str(prefix.with_suffix(".ope_folds.csv")),
        "actions": str(prefix.with_suffix(".ope_actions.csv")),
        "summary": str(prefix.with_suffix(".ope_summary.json")),
        "markdown": str(prefix.with_suffix(".ope_report.md")),
    }
    rows.to_csv(paths["rows"], index=False)
    folds.to_csv(paths["folds"], index=False)
    actions.to_csv(paths["actions"], index=False)
    Path(paths["summary"]).write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    Path(paths["markdown"]).write_text(_markdown(summary, actions), encoding="utf-8")
    return paths


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel-csv", action="append", required=True)
    parser.add_argument("--out-prefix", type=Path, required=True)
    parser.add_argument("--feature", action="append", default=[])
    parser.add_argument("--feature-registry", type=Path, default=None)
    parser.add_argument("--day-col", default="day")
    parser.add_argument("--decision-id-col", default="decision_id")
    parser.add_argument("--decision-ts-col", default="decision_ts_ns")
    parser.add_argument("--action-col", default="action")
    parser.add_argument("--reward-col", default="reward")
    parser.add_argument("--fill-value-col", default="fill_value")
    parser.add_argument("--campaign-cost-col", default="campaign_cost")
    parser.add_argument("--queue-cost-col", default="queue_cost")
    parser.add_argument("--behavior-propensity-col", default="behavior_propensity")
    parser.add_argument("--behavior-prob-prefix", default="behavior_prob_")
    parser.add_argument("--candidate-action-col", default="candidate_action")
    parser.add_argument("--candidate-prob-prefix", default="candidate_prob_")
    parser.add_argument("--split-mode", choices=("chronological", "blocked"), default="chronological")
    parser.add_argument("--min-train-days", type=int, default=30)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--blocked-folds", type=int, default=5)
    parser.add_argument("--min-train-rows", type=int, default=500)
    parser.add_argument("--min-action-rows", type=int, default=100)
    parser.add_argument("--min-behavior-propensity", type=float, default=0.02)
    parser.add_argument("--max-importance-weight", type=float, default=20.0)
    parser.add_argument("--max-unsupported-mass", type=float, default=0.05)
    parser.add_argument("--min-effective-sample-size", type=float, default=100.0)
    parser.add_argument("--min-prediction-coverage", type=float, default=0.98)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    parser.add_argument("--propensity-c", type=float, default=1.0)
    parser.add_argument("--bootstrap-trials", type=int, default=500)
    parser.add_argument("--random-seed", type=int, default=20260712)
    parser.add_argument("--learn-supported-policy", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    panel_paths = [Path(path).expanduser().resolve() for path in args.panel_csv]
    frames = [pd.read_csv(path) for path in panel_paths]
    frame = pd.concat(frames, ignore_index=True)
    cfg = OPEConfig(
        day_col=args.day_col,
        decision_id_col=args.decision_id_col,
        decision_ts_col=args.decision_ts_col,
        action_col=args.action_col,
        reward_col=args.reward_col,
        fill_value_col=args.fill_value_col,
        campaign_cost_col=args.campaign_cost_col,
        queue_cost_col=args.queue_cost_col,
        behavior_propensity_col=args.behavior_propensity_col,
        behavior_prob_prefix=args.behavior_prob_prefix,
        candidate_action_col=args.candidate_action_col,
        candidate_prob_prefix=args.candidate_prob_prefix,
        split_mode=args.split_mode,
        min_train_days=args.min_train_days,
        test_days=args.test_days,
        embargo_days=args.embargo_days,
        blocked_folds=args.blocked_folds,
        min_train_rows=args.min_train_rows,
        min_action_rows=args.min_action_rows,
        min_behavior_propensity=args.min_behavior_propensity,
        max_importance_weight=args.max_importance_weight,
        max_unsupported_mass=args.max_unsupported_mass,
        min_effective_sample_size=args.min_effective_sample_size,
        min_prediction_coverage=args.min_prediction_coverage,
        ridge_alpha=args.ridge_alpha,
        propensity_c=args.propensity_c,
        bootstrap_trials=args.bootstrap_trials,
        random_seed=args.random_seed,
        learn_supported_policy=args.learn_supported_policy,
    )
    rows, folds, actions, summary = evaluate_offline_policy(
        frame,
        feature_names=args.feature or None,
        feature_registry_path=args.feature_registry,
        config=cfg,
    )
    summary["evaluation_config"] = asdict(cfg)
    summary["input_panels"] = [
        {"path": str(path), "sha256": _sha256(path), "bytes": path.stat().st_size}
        for path in panel_paths
    ]
    summary["feature_registry"] = (
        {
            "path": str(args.feature_registry.expanduser().resolve()),
            "sha256": _sha256(args.feature_registry.expanduser().resolve()),
        }
        if args.feature_registry is not None
        else {"path": "built_in", "schema": SCHEMA_VERSION}
    )
    paths = write_outputs(args.out_prefix.expanduser(), rows, folds, actions, summary)
    print(json.dumps({"summary": summary, "artifacts": paths}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
